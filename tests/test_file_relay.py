import socket
import threading
import time
import uuid

import pytest

from protocol import (
    ACK, FILE_CHUNK, FILE_META,
    pack_ack, pack_file_chunk, pack_file_meta, pack_heartbeat, parse_udp, new_id,
)
from server import CollaborationServer


class Env:
    """Server (chỉ UDP, không cần TCP login) + các client UDP giả; đăng ký user trực tiếp vào registry."""

    def __init__(self, tcp_port, udp_port, **kw):
        self.udp_port = udp_port
        self.server = CollaborationServer(host="127.0.0.1", port=tcp_port, udp_port=udp_port,
                                          log_file=None, **kw)
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.thread.start()
        time.sleep(0.15)
        self.socks = {}

    def add_client(self, name):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind(("127.0.0.1", 0))
        s.settimeout(1.0)
        self.socks[name] = s
        port = s.getsockname()[1]
        self.server.registry.register(name, "127.0.0.1", port, udp_port=port)
        s.sendto(pack_heartbeat(name, 1), ("127.0.0.1", self.udp_port))   # server học địa chỉ UDP
        time.sleep(0.1)
        return s

    def send(self, name, data):
        self.socks[name].sendto(data, ("127.0.0.1", self.udp_port))

    def recv(self, name, timeout=1.0):
        self.socks[name].settimeout(timeout)
        data, _ = self.socks[name].recvfrom(4096)
        return parse_udp(data)

    def close(self):
        self.server.stop()
        for s in self.socks.values():
            s.close()


@pytest.fixture
def env():
    e = Env(9300, 9301)
    yield e
    e.close()


def meta(sender, to, mid, seq=1, chunks=2):
    return pack_file_meta(to, sender, "a.txt", 10, chunks, "0" * 64, seq, msg_id=mid)


def test_meta_chunk_ack_relay(env):
    env.add_client("alice")
    env.add_client("bob")
    mid = new_id()

    env.send("alice", meta("spoofed", "bob", mid, seq=5))
    pkt, body = env.recv("bob")
    assert pkt.ptype == FILE_META and pkt.msg_id == mid and pkt.seq == 5
    assert body["from"] == "alice"                      # server ghi đè 'from'
    assert mid in env.server.file_sessions
    assert env.server.file_sessions[mid].receiver == "bob"

    env.send("alice", pack_file_chunk(mid, 0, 2, b"hello", seq=6))
    pkt, body = env.recv("bob")
    assert pkt.ptype == FILE_CHUNK and pkt.chunk == 0 and body == b"hello"

    env.send("bob", pack_ack(6, msg_id=mid, chunk=0))   # ACK đầu-cuối B -> A
    pkt, _ = env.recv("alice")
    assert pkt.ptype == ACK and pkt.ack == 6 and pkt.msg_id == mid


def test_meta_to_offline_user_rejected(env):
    env.add_client("alice")
    mid = new_id()
    env.send("alice", meta("alice", "ghost", mid))
    time.sleep(0.2)
    assert mid not in env.server.file_sessions


def test_chunk_without_session_not_relayed(env):
    env.add_client("alice")
    sock_bob = env.add_client("bob")
    env.send("alice", pack_file_chunk(new_id(), 0, 1, b"x", seq=2))
    with pytest.raises(socket.timeout):
        sock_bob.settimeout(0.3)
        sock_bob.recvfrom(4096)


def test_third_party_cannot_inject_into_session(env):
    env.add_client("alice")
    env.add_client("bob")
    sock_c = env.add_client("carol")
    mid = new_id()
    env.send("alice", meta("alice", "bob", mid))
    env.recv("bob")
    env.send("carol", pack_file_chunk(mid, 0, 2, b"evil", seq=2))
    with pytest.raises(socket.timeout):
        env.recv("bob", timeout=0.3)


def test_session_closed_when_user_leaves(env):
    env.add_client("alice")
    env.add_client("bob")
    mid = new_id()
    env.send("alice", meta("alice", "bob", mid))
    env.recv("bob")
    assert mid in env.server.file_sessions
    env.server.registry.unregister(username="bob")
    env.server.close_sessions_for_user("bob")
    assert mid not in env.server.file_sessions


def test_idle_session_cleanup():
    e = Env(9302, 9303, file_session_timeout=0.3)
    try:
        e.add_client("alice")
        e.add_client("bob")
        mid = new_id()
        e.send("alice", meta("alice", "bob", mid))
        e.recv("bob")
        assert mid in e.server.file_sessions
        assert e.server.cleanup_file_sessions() == []
        time.sleep(0.5)
        assert e.server.cleanup_file_sessions() == [mid]
        assert mid not in e.server.file_sessions
    finally:
        e.close()


def test_duplicate_meta_does_not_duplicate_session(env):
    env.add_client("alice")
    env.add_client("bob")
    mid = new_id()
    env.send("alice", meta("alice", "bob", mid))
    env.send("alice", meta("alice", "bob", mid))        # gửi lại do mất ACK
    env.recv("bob")
    env.recv("bob")
    assert len(env.server.file_sessions) == 1



def test_broadcast_file_meta_fans_out_and_acks_return(env):
    env.add_client("alice")
    env.add_client("bob")
    env.add_client("carol")
    mid = new_id()
    env.send("alice", meta("alice", "*", mid))
    for name in ("bob", "carol"):
        pkt, body = env.recv(name)
        assert pkt.ptype == FILE_META and body["from"] == "alice"
    with pytest.raises(socket.timeout):
        env.recv("alice", timeout=0.3)                  # người gửi không nhận lại
    env.send("alice", pack_file_chunk(mid, 0, 2, b"zz", seq=2))
    assert env.recv("bob")[1] == b"zz" and env.recv("carol")[1] == b"zz"
    env.send("carol", pack_ack(2, msg_id=mid, chunk=0))
    assert env.recv("alice")[0].ack == 2
    env.server.close_sessions_for_user("bob")           # còn carol: phiên chưa đóng
    assert mid in env.server.file_sessions
    env.server.close_sessions_for_user("carol")
    assert mid not in env.server.file_sessions


def test_session_stores_endpoints_and_relays_out_of_order_and_duplicate_packets(env):
    """Kiểm tra bảng phiên msg_id -> (địa chỉ A, địa chỉ B) và mục 8.5.1, 8.5.2, 8.5.3."""
    sock_a = env.add_client("alice")
    sock_b = env.add_client("bob")
    mid = new_id()

    env.send("alice", meta("alice", "bob", mid, seq=1, chunks=3))
    env.recv("bob")

    sess = env.server.file_sessions[mid]
    assert sess.endpoints == (sock_a.getsockname(), sock_b.getsockname())

    # 8.5.2: Gửi lệch thứ tự (chunk 2 trước chunk 0)
    env.send("alice", pack_file_chunk(mid, 2, 3, b"c2", seq=2))
    env.send("alice", pack_file_chunk(mid, 0, 3, b"c0", seq=3))
    pkt2, body2 = env.recv("bob")
    pkt0, body0 = env.recv("bob")
    assert pkt2.chunk == 2 and body2 == b"c2"
    assert pkt0.chunk == 0 and body0 == b"c0"

    # 8.5.1 & 8.5.3: Gửi lại chunk trùng và ACK trùng đều được chuyển tiếp
    env.send("alice", pack_file_chunk(mid, 0, 3, b"c0", seq=3))
    pkt_dup, body_dup = env.recv("bob")
    assert pkt_dup.chunk == 0 and body_dup == b"c0"

    env.send("bob", pack_ack(3, seq=10, msg_id=mid, chunk=0))
    env.send("bob", pack_ack(3, seq=11, msg_id=mid, chunk=0))
    ack1, _ = env.recv("alice")
    ack2, _ = env.recv("alice")
    assert ack1.ack == 3 and ack1.chunk == 0
    assert ack2.ack == 3 and ack2.chunk == 0


def test_invalid_meta_and_invalid_chunk_index_dropped(env):
    """Kiểm tra mục 8.5.4: metadata không hợp lệ hoặc chỉ số chunk vượt total bị loại bỏ."""
    env.add_client("alice")
    env.add_client("bob")
    mid_bad = new_id()

    # Checksum không đủ 64 ký tự hex -> từ chối tạo phiên
    bad_meta = pack_file_meta("bob", "alice", "a.txt", 10, 2, "short_hash", seq=1, msg_id=mid_bad)
    env.send("alice", bad_meta)
    time.sleep(0.15)
    assert mid_bad not in env.server.file_sessions

    # Tạo phiên hợp lệ (chunks=2), nhưng gửi chunk index = 5 (>= total) -> bị bỏ qua
    mid_ok = new_id()
    env.send("alice", meta("alice", "bob", mid_ok, seq=2, chunks=2))
    env.recv("bob")
    env.send("alice", pack_file_chunk(mid_ok, 5, 2, b"bad_idx", seq=3))
    with pytest.raises(socket.timeout):
        env.recv("bob", timeout=0.3)

