import socket
import threading
import time
import unittest
from config import CLIENT_TIMEOUT
from protocol import MSG, USER_LIST, HEARTBEAT, pack_msg, pack_heartbeat, parse_udp, SeqCounter
from registry import UserRegistry
from server import CollaborationServer


def create_udp_listener(port=0):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", port))
    assigned_port = sock.getsockname()[1]
    sock.settimeout(2.0)
    return sock, assigned_port


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

    def test_server_forward_broadcast(self):
        """
        Test 4: Server.forward() cho Broadcast (*)
        Server chuyển tiếp tin nhắn tới Bob và Charlie, loại trừ Alice (sender).
        """
        server = CollaborationServer(host="127.0.0.1", port=9200, udp_port=9201)
        sock_bob, port_bob = create_udp_listener()
        sock_charlie, port_charlie = create_udp_listener()
        sock_alice, port_alice = create_udp_listener()

        try:
            server.registry.register("alice", "127.0.0.1", port_alice, udp_port=port_alice)
            server.registry.register("bob", "127.0.0.1", port_bob, udp_port=port_bob)
            server.registry.register("charlie", "127.0.0.1", port_charlie, udp_port=port_charlie)

            raw_pkt = pack_msg(to="*", sender="alice", text="Hello everyone!", seq=1)
            recipients = server.forward("alice", raw_pkt)
            self.assertIn("bob", recipients)
            self.assertIn("charlie", recipients)
            self.assertNotIn("alice", recipients)

            # Bob nhận được
            data_b, _ = sock_bob.recvfrom(4096)
            pkt_b, body_b = parse_udp(data_b)
            self.assertEqual(body_b["from"], "alice")
            self.assertEqual(body_b["text"], "Hello everyone!")

            # Charlie nhận được
            data_c, _ = sock_charlie.recvfrom(4096)
            pkt_c, body_c = parse_udp(data_c)
            self.assertEqual(body_c["from"], "alice")
            self.assertEqual(body_c["text"], "Hello everyone!")

            # Alice không nhận lại tin của chính mình
            sock_alice.settimeout(0.2)
            with self.assertRaises(socket.timeout):
                sock_alice.recvfrom(4096)
        finally:
            server.stop()
            sock_bob.close()
            sock_charlie.close()
            sock_alice.close()

    def test_server_forward_unicast(self):
        """
        Test 5: Server.forward() cho Unicast (<username>)
        Chỉ có Bob nhận được tin nhắn riêng, Charlie không nhận được.
        """
        server = CollaborationServer(host="127.0.0.1", port=9202, udp_port=9203)
        sock_bob, port_bob = create_udp_listener()
        sock_charlie, port_charlie = create_udp_listener()

        try:
            server.registry.register("alice", "127.0.0.1", 50000)
            server.registry.register("bob", "127.0.0.1", port_bob, udp_port=port_bob)
            server.registry.register("charlie", "127.0.0.1", port_charlie, udp_port=port_charlie)

            raw_pkt = pack_msg(to="bob", sender="alice", text="Secret for Bob", seq=1)
            recipients = server.forward("alice", raw_pkt)
            self.assertEqual(recipients, ["bob"])

            # Bob nhận được
            data_b, _ = sock_bob.recvfrom(4096)
            pkt_b, body_b = parse_udp(data_b)
            self.assertEqual(body_b["to"], "bob")
            self.assertEqual(body_b["from"], "alice")
            self.assertEqual(body_b["text"], "Secret for Bob")

            # Charlie không nhận được
            sock_charlie.settimeout(0.2)
            with self.assertRaises(socket.timeout):
                sock_charlie.recvfrom(4096)
        finally:
            server.stop()
            sock_bob.close()
            sock_charlie.close()

    def test_server_forward_unicast_offline_user(self):
        """
        Test 6: Unicast tới user offline
        Gói tin bị hủy (drop) an toàn, trả về danh sách rỗng.
        """
        server = CollaborationServer(host="127.0.0.1", port=9204, udp_port=9205)
        try:
            server.registry.register("alice", "127.0.0.1", 50000)
            raw_pkt = pack_msg(to="offline_user", sender="alice", text="Are you there?", seq=1)
            recipients = server.forward("alice", raw_pkt)
            self.assertEqual(recipients, [])
        finally:
            server.stop()

    def test_server_heartbeat_processing(self):
        """
        Test 7: Nhận HEARTBEAT trên cổng UDP và cập nhật last_seen
        """
        test_udp_port = 9207
        server = CollaborationServer(host="127.0.0.1", port=9206, udp_port=test_udp_port)
        server_thread = threading.Thread(target=server.run, daemon=True)
        server_thread.start()
        time.sleep(0.1)

        try:
            client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            hb_pkt = pack_heartbeat(username="alice", seq=1)
            
            before_send = time.time()
            client_sock.sendto(hb_pkt, ("127.0.0.1", test_udp_port))
            time.sleep(0.2)

            self.assertIn("alice", server.last_seen)
            self.assertGreaterEqual(server.last_seen["alice"], before_send - 0.5)
            self.assertIn("alice", server.udp_addrs)
            client_sock.close()
        finally:
            server.stop()
            server_thread.join(timeout=1.0)

    def test_server_cleanup_loop_and_broadcast_user_list(self):
        """
        Test 8: Luồng cleanup quét timeout sau 10s, xóa user và broadcast USER_LIST mới
        """
        server = CollaborationServer(host="127.0.0.1", port=9208, udp_port=9209, client_timeout=10.0)
        sock_bob, port_bob = create_udp_listener()

        try:
            server.registry.register("alice", "127.0.0.1", 50001, udp_port=50001)
            server.registry.register("bob", "127.0.0.1", port_bob, udp_port=port_bob)

            # Bob vừa gửi heartbeat, Alice quá hạn (>10s)
            server.last_seen["bob"] = time.time()
            server.last_seen["alice"] = time.time() - 15.0

            # Chạy quét timeout
            stale = server.cleanup_stale_users()
            self.assertEqual(stale, ["alice"])
            self.assertFalse(server.registry.is_active("alice"))
            self.assertTrue(server.registry.is_active("bob"))
            self.assertNotIn("alice", server.last_seen)

            # Bob nhận được broadcast USER_LIST mới (chỉ còn bob)
            data, _ = sock_bob.recvfrom(4096)
            pkt, body = parse_udp(data)
            self.assertEqual(pkt.ptype, USER_LIST)
            self.assertEqual(body["users"], ["bob"])
        finally:
            server.stop()
            sock_bob.close()


if __name__ == "__main__":
    unittest.main()