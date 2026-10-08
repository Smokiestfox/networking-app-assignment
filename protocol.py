import json
import struct
import uuid
from dataclasses import dataclass
from cryptography.fernet import Fernet
from config import FERNET_KEY

MAGIC = b"NP"
VERSION = 1

HEARTBEAT = 1
MSG = 2
FILE_META = 3
FILE_CHUNK = 4
ACK = 5
USER_LIST = 6

FLAG_ENCRYPTED = 1
FLAG_END = 2

HEADER = struct.Struct("!2sBBB II 16s H H I") # thêm một số nguyên I cho ack
FRAME_HEADER = struct.Struct("!I")  # 4 bytes số nguyên không dấu (uint32) Big-Endian
cipher = Fernet(FERNET_KEY.encode())


@dataclass
class Packet:
    ptype: int
    seq: int
    msg_id: uuid.UUID
    ack: int = 0 # thêm trường ack mặc định = 0
    chunk: int = 0
    total: int = 1
    flags: int = 0
    payload: bytes = b""


def encode_packet(packet: Packet, encrypt=True) -> bytes:
    payload = packet.payload
    flags = packet.flags
    if encrypt and payload:
        payload = cipher.encrypt(payload)
        flags |= FLAG_ENCRYPTED
    header = HEADER.pack(
        MAGIC,
        VERSION,
        packet.ptype,
        flags,
        packet.seq,
        packet.ack,
        packet.msg_id.bytes,
        packet.chunk,
        packet.total,
        len(payload),
    )
    return header + payload


def decode_packet(data: bytes) -> Packet:
    if len(data) < HEADER.size:
        raise ValueError("short packet")
    magic, version, ptype, flags, seq, ack, mid, chunk, total, size = HEADER.unpack(
        data[:HEADER.size]
    )
    if magic != MAGIC or version != VERSION:
        raise ValueError("invalid packet")
    payload = data[HEADER.size:HEADER.size + size]
    if flags & FLAG_ENCRYPTED and payload:
        payload = cipher.decrypt(payload)
    return Packet(ptype, seq, uuid.UUID(bytes=mid), ack, chunk, total, flags, payload)


def json_bytes(obj) -> bytes:
    return json.dumps(obj, separators=(",", ":")).encode()


def json_obj(payload: bytes):
    return json.loads(payload.decode())


def new_id():
    return uuid.uuid4()

def recv_exact(sock, num_bytes: int):
    buf = bytearray()
    while len(buf) < num_bytes:
        to_read = num_bytes - len(buf)
        chunk = sock.recv(to_read) if hasattr(sock, "recv") else sock.read(to_read)
        if not chunk:
            if len(buf) == 0:
                return None  
            raise ConnectionResetError(
                f"Mất kết nối giữa chừng: cần {num_bytes} bytes nhưng mới nhận được {len(buf)} bytes"
            )
        buf.extend(chunk)
    return bytes(buf)

def send_frame(sock, data: bytes) -> None:
    header = FRAME_HEADER.pack(len(data))
    sock.sendall(header + data)

def recv_frame(sock):
    header_bytes = recv_exact(sock, FRAME_HEADER.size)
    if header_bytes is None:
        return None
    length = FRAME_HEADER.unpack(header_bytes)[0]
    if length == 0:
        return b""
    payload = recv_exact(sock, length)
    if payload is None:
        raise ConnectionResetError("Mất kết nối khi đang nhận phần payload của khung")
    return payload

def send_json_frame(sock, obj) -> None:
    send_frame(sock, json_bytes(obj))

def recv_json_frame(sock):
    raw = recv_frame(sock)
    if raw is None:
        return None
    return json_obj(raw)