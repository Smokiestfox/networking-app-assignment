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
    import io
    fake_socket = io.BytesIO()
    
    test_obj = {"username": "Alice", "status": "online"}
    
    protocol.send_json_frame(fake_socket, test_obj)
    
    fake_socket.seek(0)
  
    received_obj = protocol.recv_json_frame(fake_socket)
    
    assert received_obj == test_obj
