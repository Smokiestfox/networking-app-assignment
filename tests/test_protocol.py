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
    raw = protocol.pack_file_chunk(protocol.new_id(), 0, 1, b"hello", 1)
    for broken in (raw[:-1], raw + b"extra", raw[:-1] + bytes([raw[-1] ^ 1])):
        with pytest.raises(ValueError):
            protocol.parse_udp(broken)
