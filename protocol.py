import json
import struct
import uuid
import threading
import time
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

MAX_UDP_SIZE = 1400      # max datagram size (header + encrypted payload) to avoid IP fragmentation
MAX_TEXT_LEN = 250       # max characters of a chat message (250 x 3-byte UTF-8 chars still fits MAX_UDP_SIZE after Fernet)
NIL_ID = uuid.UUID(int=0)  # msg_id for packets that are not part of a message/file session
 
# Required JSON keys of each UDP payload type
REQUIRED_KEYS = {
    MSG: ("to", "from", "text", "time"),
    HEARTBEAT: ("username",),
    USER_LIST: ("users",),
    FILE_META: ("to", "from", "filename", "size", "chunks", "checksum"), # chunks: tổng số mảnh được cắt ra để gửi, checksum: dùng để ktra tính nguyên vẹn sau khi ghép lại
}
 

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
    payload = header + data
    if hasattr(sock, "sendall"):
        sock.sendall(payload)
    else:
        sock.write(payload)

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


 
class SeqCounter:
    """Thread-safe, monotonically increasing sequence number (starts at 1)."""
 
    def __init__(self):
        self._value = 0
        self._lock = threading.Lock()
 
    def next(self) -> int:
        with self._lock:
            self._value += 1
            return self._value
 
 
def build_udp(ptype: int, body: dict, seq: int, msg_id=None, ack: int = 0,
              encrypt: bool = True) -> bytes:
    """Serialize `body` as JSON and wrap it into a full UDP datagram (37-byte header + payload)."""
    packet = Packet(
        ptype=ptype,
        seq=seq,
        msg_id=msg_id if msg_id is not None else NIL_ID,
        ack=ack,
        payload=json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),  # keep non-ASCII text as UTF-8 (shorter than \u escapes)
    )
    data = encode_packet(packet, encrypt=encrypt)
    if len(data) > MAX_UDP_SIZE:
        raise ValueError(f"datagram too large: {len(data)} > {MAX_UDP_SIZE} bytes")
    return data
 
 
def parse_udp(data: bytes):
    """Parse a datagram. Returns (Packet, body_dict). Raises ValueError for any invalid datagram."""
    if len(data) < HEADER.size:
        raise ValueError("short packet")
    declared = HEADER.unpack(data[:HEADER.size])[-1]       # payload length field
    if len(data) != HEADER.size + declared:
        raise ValueError("payload length does not match datagram size")
    try:
        packet = decode_packet(data)                       # also decrypts if FLAG_ENCRYPTED
        
        if packet.ptype == FILE_CHUNK: # nếu là FILE_CHUNK: payload là dữ liệu nhị phân (bytes), không parse JSON
            body = packet.payload
        else:
            body = json_obj(packet.payload) if packet.payload else {}
    except ValueError:
        raise
    except Exception as exc:                               # InvalidToken, UnicodeDecodeError, ...
        raise ValueError(f"invalid payload: {exc}") from exc

    if packet.ptype != FILE_CHUNK:
        if not isinstance(body, dict):
            raise ValueError("payload must be a JSON object")
        missing = [k for k in REQUIRED_KEYS.get(packet.ptype, ()) if k not in body]
        if missing:
            raise ValueError(f"missing fields {missing} in packet type {packet.ptype}")

    return packet, body
 
 
def pack_msg(to: str, sender: str, text: str, seq: int, **kwargs) -> bytes:
    """MSG: `to` is "*" (broadcast) or a username (unicast). The server overwrites `from`."""
    if not text or len(text) > MAX_TEXT_LEN:
        raise ValueError(f"text must be 1..{MAX_TEXT_LEN} characters")
    body = {"to": to, "from": sender, "text": text, "time": int(time.time())}
    return build_udp(MSG, body, seq, msg_id=new_id(), **kwargs)
 
 
def pack_heartbeat(username: str, seq: int, **kwargs) -> bytes:
    """HEARTBEAT: periodic liveness signal (client -> server)."""
    return build_udp(HEARTBEAT, {"username": username}, seq, **kwargs)
 
 
def pack_user_list(users, seq: int, **kwargs) -> bytes:
    """USER_LIST: list of online usernames (server -> clients)."""
    return build_udp(USER_LIST, {"users": list(users)}, seq, **kwargs)

def pack_file_meta(to: str, sender: str, filename: str, size: int, chunks: int, checksum: str, seq: int, msg_id: uuid.UUID = None, **kwargs) -> bytes:
    if msg_id is None:
        msg_id = new_id()

    body = {
        "to": to,
        "from": sender,
        "filename": filename,
        "size": size,
        "chunks": chunks,
        "checksum": checksum,
    }

    packet = Packet(
        ptype = FILE_META,
        seq = seq,
        msg_id = msg_id,
        chunk = 0,
        total = chunks,
        payload = json.dumps(body, separators = (",", ":"), ensure_ascii = False).encode("utf-8"),
    )
    encrypt = kwargs.get("encrypt", True)
    data = encode_packet(packet, encrypt = encrypt)
    if len(data) > MAX_UDP_SIZE:
        raise ValueError(f"datagram too large: {len(data)} > {MAX_UDP_SIZE} bytes")
    return data

def pack_file_chunk(msg_id: uuid.UUID, chunk: int, total: int, data: bytes, seq: int, flags: int = 0, **kwargs) -> bytes:
    if chunk == total - 1:
        flags = flags | FLAG_END

    packet = Packet(
        ptype = FILE_CHUNK,
        seq = seq,
        msg_id = msg_id,
        chunk = chunk,
        total = total,
        flags = flags,
        payload = data, #payload ở đây là bytes nhị phân trực tiếp của file
    )
    encrypt = kwargs.get("encrypt", True)
    data_packet = encode_packet(packet, encrypt = encrypt)
    if len(data_packet) > MAX_UDP_SIZE:
        raise ValueError(f"datagram too large: {len(data_packet)} > {MAX_UDP_SIZE} bytes")
    return data_packet

def pack_ack(ack_seq: int, seq: int = 0, msg_id: uuid.UUID = None, chunk: int = 0, **kwargs) -> bytes:
    packet = Packet(
        ptype = ACK,
        seq = seq,
        msg_id = msg_id if msg_id is not None else NIL_ID,
        ack = ack_seq, # số sequence của gói tin dc xác nhận
        chunk = chunk, # chỉ số chunk tương ứng (nếu đang truyền file)
        total = 1,
        flags = 0,
        payload = b"", # gói tin ACK chỉ cần phần header, payload để rỗng
    )
    encrypt = kwargs.get("encrypt", False)
    return encode_packet(packet, encrypt = encrypt)