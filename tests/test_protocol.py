import socket
import uuid
from protocol import (
    Packet, MSG, ACK,
    decode_packet, encode_packet,
    send_frame, recv_frame,
    send_json_frame, recv_json_frame,
    json_bytes, json_obj
)


def test_packet_roundtrip():
    packet = Packet(MSG, 42, uuid.uuid4(), payload=json_bytes({"text": "hello"}))
    restored = decode_packet(encode_packet(packet))
    assert restored.ptype == MSG
    assert restored.seq == 42
    assert restored.ack == 0
    assert restored.msg_id == packet.msg_id
    assert json_obj(restored.payload)["text"] == "hello"


def test_packet_with_ack():
    uid = uuid.uuid4()
    packet = Packet(ACK, 100, uid, ack=42, payload=b"ok")
    restored = decode_packet(encode_packet(packet))
    assert restored.ptype == ACK
    assert restored.seq == 100
    assert restored.ack == 42
    assert restored.payload == b"ok"


def test_tcp_framing_roundtrip():
    s1, s2 = socket.socketpair()
    try:
        data1 = b"Frame 1"
        data2 = b"Frame 2: Longer message payload"
        send_frame(s1, data1)
        send_frame(s1, data2)
        assert recv_frame(s2) == data1
        assert recv_frame(s2) == data2
    finally:
        s1.close()
        s2.close()


def test_tcp_json_framing():
    s1, s2 = socket.socketpair()
    try:
        obj = {"type": "AUTH", "username": "alice"}
        send_json_frame(s1, obj)
        assert recv_json_frame(s2) == obj
    finally:
        s1.close()
        s2.close()
