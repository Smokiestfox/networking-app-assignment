import time
import unittest
from config import CLIENT_TIMEOUT
from protocol import MSG, pack_msg, parse_udp, SeqCounter
from registry import UserRegistry


class TestMessaging(unittest.TestCase):

    def test_broadcast_chat(self):
        """
        Test 1: Broadcast chat
        Client A gửi tin nhắn '*', Server chuyển tiếp tới Client B và C.
        """
        seq_counter = SeqCounter()
        seq = seq_counter.next()
        
        # Đóng gói bản tin Broadcast từ Alice
        raw_packet = pack_msg(to="*", sender="alice", text="Hello everyone!", seq=seq)
        packet, body = parse_udp(raw_packet)
        
        self.assertEqual(packet.ptype, MSG)
        self.assertEqual(body["to"], "*")
        self.assertEqual(body["from"], "alice")
        self.assertEqual(body["text"], "Hello everyone!")
        self.assertIn("time", body)

    def test_unicast_chat(self):
        """
        Test 2: Unicast chat
        Client A gửi tin nhắn riêng cho Bob ('bob'). Chỉ Bob nhận được.
        """
        seq_counter = SeqCounter()
        seq = seq_counter.next()
        
        # Đóng gói bản tin Unicast từ Alice cho Bob
        raw_packet = pack_msg(to="bob", sender="alice", text="Hello Bob secret!", seq=seq)
        packet, body = parse_udp(raw_packet)
        
        self.assertEqual(packet.ptype, MSG)
        self.assertEqual(body["to"], "bob")
        self.assertEqual(body["from"], "alice")
        self.assertEqual(body["text"], "Hello Bob secret!")

    def test_heartbeat_timeout(self):
        """
        Test 3: Heartbeat timeout
        Client A ngừng gửi heartbeat, sau 10s Server tự động loại A khỏi Active List.
        """
        registry = UserRegistry(users_file=None)
        registry.user_db = {"alice": "pass123"}
        
        # Đăng ký Alice online
        registry.register("alice", "127.0.0.1", 50001)
        self.assertTrue(registry.is_active("alice"))
        
        # Giả lập bảng theo dõi thời gian heartbeat gần nhất (quá 10s)
        last_seen = {"alice": time.time() - 11.0}
        
        # Luồng quét dọn (Cleanup loop)
        now = time.time()
        stale_users = [u for u, t in last_seen.items() if now - t > CLIENT_TIMEOUT]
        
        for user in stale_users:
            registry.unregister(username=user)
            del last_seen[user]
            
        self.assertFalse(registry.is_active("alice"))
        self.assertNotIn("alice", last_seen)


if __name__ == "__main__":
    unittest.main()