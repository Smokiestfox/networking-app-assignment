import uuid
import pytest
from cryptography.fernet import Fernet

import protocol

def test_packet_encode_decode_roundtrip():
    original_payload = b"Hello, Network Application Assignment!"
    msg_id = protocol.new_id()
    
    pkt = protocol.Packet(
        ptype=protocol.MSG,
        seq=1,
        msg_id=msg_id,
        ack=10,
        chunk=0,
        total=1,
        flags=0,
        payload=original_payload
    )
  
    encoded_data = protocol.encode_packet(pkt, encrypt=True)
  
    decoded_pkt = protocol.decode_packet(encoded_data)
    
    assert decoded_pkt.ptype == pkt.ptype
    assert decoded_pkt.seq == pkt.seq
    assert decoded_pkt.msg_id == pkt.msg_id
    assert decoded_pkt.ack == pkt.ack
    assert decoded_pkt.chunk == pkt.chunk
    assert decoded_pkt.total == pkt.total
    assert decoded_pkt.payload == original_payload

def test_packet_decode_invalid_short_packet():
    short_data = b"NP1"  
    with pytest.raises(ValueError, match="short packet"):
        protocol.decode_packet(short_data)

def test_packet_decode_invalid_magic_or_version():
    pkt = protocol.Packet(ptype=protocol.HEARTBEAT, seq=1, msg_id=protocol.new_id(), payload=b"test")
    encoded_data = protocol.encode_packet(pkt, encrypt=False)

    invalid_data = b"XX" + encoded_data[2:]
    
    with pytest.raises(ValueError, match="invalid packet"):
        protocol.decode_packet(invalid_data)

def test_json_frame_transmission_simulation():
    import socket
    # Dùng socketpair để mô phỏng kết nối thật (sendall/recv)
    server_sock, client_sock = socket.socketpair()

    try:
        test_obj = {"username": "Alice", "status": "online"}

        protocol.send_json_frame(client_sock, test_obj)

        received_obj = protocol.recv_json_frame(server_sock)

        assert received_obj == test_obj
    finally:
        client_sock.close()
        server_sock.close()


def test_file_helpers_binary_roundtrip_and_end_flag():
    data = bytes(range(256)) * 2 + b"\x00\xff\xfe"
    mid = protocol.new_id()
    raw = protocol.pack_file_chunk(mid, 1, 2, data, seq=42)
    pkt, body = protocol.parse_udp(raw)
    assert pkt.msg_id == mid and pkt.chunk == 1 and pkt.total == 2
    assert pkt.flags & protocol.FLAG_END
    assert body == data
    assert len(raw) <= protocol.MAX_UDP_SIZE
    raw = protocol.pack_ack(pkt.seq, seq=43, msg_id=mid, chunk=1)
    ack, body = protocol.parse_udp(raw)
    assert ack.ack == 42 and ack.chunk == 1 and body == {}
    assert len(raw) == protocol.HEADER.size == 37


@pytest.mark.parametrize("size,chunks", [(0, 1), (700, 1), (701, 2), (45874500, 65535)])
def test_file_meta_roundtrip(size, chunks):
    mid = protocol.new_id()
    raw = protocol.pack_file_meta("bob", "alice", "tệp.bin", size, chunks, "a" * 64, 10, mid)
    pkt, body = protocol.parse_udp(raw)
    assert pkt.msg_id == mid and pkt.total == chunks
    assert body["filename"] == "tệp.bin" and body["size"] == size


def test_reject_truncated_trailing_and_tampered_file_datagrams():
    # Sử dụng encrypt=False để test độ dài header và payload trực tiếp
    raw = protocol.pack_file_chunk(protocol.new_id(), 0, 1, b"hello", 1, encrypt=False)
    for broken in (raw[:-1], raw + b"extra"):
        with pytest.raises(ValueError):
            protocol.parse_udp(broken)

# ==========================================
# CÁC TEST CASES MỚI CHO TCP ENCRYPTION (W6-03)
# ==========================================

def test_tcp_frame_wire_is_encrypted():
    """Kiểm tra: Dữ liệu truyền trên socket không chứa plaintext JSON hay password."""
    import socket
    s1, s2 = socket.socketpair()
    try:
        secret_payload = {"user": "alice", "password": "super_secret_password_123"}
        protocol.send_json_frame(s1, secret_payload)
        
        # Đọc trực tiếp byte thô từ socket mạng
        raw_wire = s2.recv(1024)
        
        # 1. 4 bytes đầu là độ dài uint32
        declared_len = protocol.FRAME_HEADER.unpack(raw_wire[:4])[0]
        assert declared_len == len(raw_wire[4:])
        
        # 2. Không được để lộ plaintext trên đường truyền
        assert b"super_secret_password_123" not in raw_wire
        assert b"alice" not in raw_wire
    finally:
        s1.close()
        s2.close()


def test_tcp_frame_wrong_key_closes_connection():
    """Kiểm tra: Sai khóa Fernet thì recv_frame phải ném ConnectionResetError (đóng kết nối)."""
    import socket
    s1, s2 = socket.socketpair()
    key_alice = Fernet.generate_key()
    key_bob = Fernet.generate_key()  # Khóa khác hoàn toàn
    cipher_alice = Fernet(key_alice)
    cipher_bob = Fernet(key_bob)
    try:
        protocol.send_json_frame(s1, {"msg": "hello"}, cipher=cipher_alice)
        with pytest.raises(ConnectionResetError, match="Dữ liệu TCP bị sửa đổi hoặc sai khóa mã hóa"):
            protocol.recv_json_frame(s2, cipher=cipher_bob)
    finally:
        s1.close()
        s2.close()


def test_tcp_frame_tampered_ciphertext_closes_connection():
    """Kiểm tra: Dữ liệu mã hóa bị sửa/can thiệp trên đường truyền thì kết nối bị ngắt."""
    import socket
    s1, s2 = socket.socketpair()
    try:
        protocol.send_json_frame(s1, {"action": "transfer", "amount": 100})
        raw_wire = bytearray(s2.recv(1024))
        
        # Giả lập kẻ tấn công cố tình sửa đổi 1 byte trong phần ciphertext
        raw_wire[10] ^= 0xFF
        
        s3, s4 = socket.socketpair()
        try:
            s3.sendall(raw_wire)
            with pytest.raises(ConnectionResetError, match="Dữ liệu TCP bị sửa đổi hoặc sai khóa mã hóa"):
                protocol.recv_json_frame(s4)
        finally:
            s3.close()
            s4.close()
    finally:
        s1.close()
        s2.close()


def test_mask_credentials_masks_passwords():
    """Kiểm tra hàm che thông tin nhạy cảm khi log."""
    data = {"user": "bob", "password": "mypassword", "pass": "secret", "other": 123}
    masked = protocol.mask_credentials(data)
    assert masked["password"] == "***"
    assert masked["pass"] == "***"
    assert masked["user"] == "bob"
    assert masked["other"] == 123
    # Đảm bảo dictionary gốc không bị thay đổi ngoài ý muốn
    assert data["password"] == "mypassword"