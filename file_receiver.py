import hashlib
import logging
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path, PureWindowsPath
from typing import Dict, Optional

from config import CHUNK_SIZE, FILE_SESSION_TIMEOUT
from protocol import FILE_CHUNK, FILE_META, SeqCounter, pack_ack

log = logging.getLogger("file_receiver")

def valid_file_meta(body: dict) -> bool:
    if not isinstance(body, dict): # kiểm tra payload giải mã ra phải là dictionary
        return False
    if any(not isinstance(body.get(k), str) or not body[k].strip() for k in ("to", "from", "filename")): # đảm bảo ng nhận, ng gửi, tên file phải là chuỗi ký tự không rỗng
        return False
    
    size, chunks, checksum = (body.get(k) for k in ("size", "chunks", "checksum")) #lấy kích thước file, số chunks, và mã SHA-256

    if type(size) is not int or size < 0 or type(chunks) is not int: #kích thước file không được âm, số mảnh phải là số nguyên
        return False
    
    expected_chunks = max(1, (size + CHUNK_SIZE - 1) // CHUNK_SIZE) # tính toán số mảnh kỳ vọng 

    if not (1 <= chunks <= 65535) or chunks != expected_chunks: #tổng số chunks thực tế phải khớp với số chunks kỳ vọng 
        return False
    return (isinstance(checksum, str) and len(checksum) == 64 and all(c in "0123456789abcdef" for c in checksum.lower())) #check SHA-256 buộc là chuỗi hexa đúng 64 ký tự

@dataclass
class Incoming:
    meta: dict #thông tin metadata của file như tên, size, checksum
    last_seen: float #mốc tgian nhận gói tin gần nhất(tính timeout)
    chunks: dict = field(default_factory = dict) #bộ đệm lưu mảnh: {chunk_index: bytes}
    bytes_received: int = 0 #tổng số byte thật đã nhận (ko tính mảnh trùng)
    path: Optional[Path] = None #đường dẫn file sau khi ghép và lưu thành công
    complete: bool = False # đánh dầu đã nhận đủ và lưu file xong
    failed: bool = False #đánh dấu phiên thất bại

class FileReceiver:
    def __init__(
        self,
        send_fn,
        download_dir = "downloads",
        *,
        username = None,
        seq = None,
        clock = time.monotonic,
        timeout = FILE_SESSION_TIMEOUT,
        on_result = None,
        on_progress = None,
        max_sessions = 16,
    ):

        self.send_fn = send_fn
        self.download_dir = Path(download_dir)
        self.username = username
        self.seq = seq or SeqCounter()
        self.clock = clock
        self.timeout = timeout
        self.on_result = on_result
        self.on_progress = on_progress
        self.max_sessions = max_sessions
        self.sessions: Dict[object, Incoming] = {}
        self._lock = threading.RLock() 

    @property
    def is_receiving(self) -> bool:
        with self._lock: #kiểm tra xem có pheien nào đang nhận dở hay không 
            return any(not s.complete and not s.failed for s in self.sessions.values())

    def _ack(self, packet): #đóng gói và gửi bản tin ACK phản hồi cho packet vừa nhận qua send_fn
        self.send_fn(pack_ack(packet.seq, self.seq.next(), packet.msg_id, packet.chunk))

    def _notify(self, mid, session: Incoming, error = None):
        if self.on_result: #gọi hàm callback thông báo kết quả ra giao diện người dùng
            self.on_result(mid, session.path, error)

    def _notify_progress(self, mid, session: Incoming):
        if self.on_progress:
            self.on_progress(
                mid,
                session.bytes_received,
                session.meta["size"],
                len(session.chunks),
                session.meta["chunks"],
            )

    def on_packet(self, packet, body) -> bool:
        with self._lock:
            now = self.clock()

            if packet.ptype == FILE_META: #TH1: gói tin FILE_META (khởi tạo phiên nhận)
                if (not valid_file_meta(body) or packet.total != body["chunks"] or (self.username and body["to"] != self.username)):
                    return False #kiểm tra hợp lệ và file phải gửi đúng cho username của mình

                session = self.sessions.get(packet.msg_id)
                if session is None:
                    if len(self.sessions) >= self.max_sessions: #giới hạn số phiên đồng thời để chống tràn bộ nhớ
                        return False
                    session = Incoming(dict(body), now)
                    self.sessions[packet.msg_id] = session
                elif session.meta != body or session.failed:
                    return False

                session.last_seen = now

                self._ack(packet) #luôn gửi lại ACK cho FILE_META (dù nhận trùng cũng ko làm mất chunk đã có)
                return True

            if packet.ptype != FILE_CHUNK: #TH2: gói tin FILE_CHUNK (mảnh dữ liệu nhị phân)
                return False
            
            session = self.sessions.get(packet.msg_id)
            if session is None or session.failed:
                return False

            total = session.meta["chunks"]

            if packet.total != total or not (0 <= packet.chunk < total) or not isinstance(body, bytes): #kiểm tra chỉ số chunk phải nằm trong khoảng từ 0 đến total - 1
                return False 
            
            excepted_size = (CHUNK_SIZE if packet.chunk < total - 1 else session.meta["size"] - CHUNK_SIZE * (total - 1))

            if len(body) != excepted_size:
                return False

            session.last_seen = now

            if session.complete: #khôi phục nếu ACK cuối bị mất: nếu file đã xong, gửi lại ACK và ko ghi đè lại file
                self._ack(packet)
                return True

            previous = session.chunks.get(packet.chunk)
            if previous is not None:
                if previous != body: #ktra mảnh trùng lặp: nếu đã có rùi nhưng ndung bị leehcj -> từ chối
                    return False
                self._ack(packet) #mảnh trừng hợp lệ (do mất ACK): gửi lại ACK nhưng KO cộng thêm tiến độ
                return True
            
            session.chunks[packet.chunk] = body #lưu mảnh mới vào bộ đệm theo chỉ số
            session.bytes_received += len(body)

            if len(session.chunks) < total: #chỉ phát tiến độ trung gian khi chưa tới mảnh cuối
                self._notify_progress(packet.msg_id, session)

            if len(session.chunks) == total: #khi đã nhận đủ tất cả mảnh file
                data = b"".join(session.chunks[i] for i in range(total)) #ghép tất cả các mảnh lại theo đúng thứ tự 0, 1, 2,.. total - 1

                if hashlib.sha256(data).hexdigest() != session.meta["checksum"]: #ktra tính toàn vẹn: đối chiếu mã SHA-256 tính đc với checksum trong metadata
                    session.failed = True
                    session.chunks.clear()
                    self._notify(packet.msg_id, session, "SHA-256 mismatch")
                    return False #file bị hỏng -> ko gửi ACK

                try: #lưu file vào thư mục downloads
                    self.download_dir.mkdir(parents = True, exist_ok = True)
                    name = PureWindowsPath(session.meta["filename"]).name #làm sạch tên file tránh lỗi đường dẫn hoặc ký tự dbiet
                    name = Path(name).name
                    name = "".join(c if c.isalnum() or c in "._- " else "_" for c in name)
                    name = name.strip(" .")[:120] or "file"

                    destination = self.download_dir / f"{packet.msg_id.hex}_{name}" #thêm tiền tố UUID để ko bị trùng tên file giữa các lần gửi

                    with destination.open("xb") as out: #ghi file nhị phân với cờ 'xb' (chỉ tạo file mới, ko ghi đè)
                        try:
                            out.write(data)
                        except OSError:
                            out.close()
                            destination.unlink(missing_ok = True)
                            raise
                    
                    session.path = destination
                    session.complete = True
                    session.chunks.clear() #xóa dữ liệu trong RAM để tiết kiệm bộ nhớ
                except OSError as exc:
                    session.failed = True
                    session.chunks.clear()
                    self._notify(packet.msg_id, session, f"Cannot save file: {exc}")
                    return False
                    
                self._notify(packet.msg_id, session) #thông báo cho UI client rằng đã nhận file thành công

            self._ack(packet) #gửi ACK xác nhận đã nhận thành công mảnh này
            return True

    def tick(self):
        with self._lock:
            now = self.clock()
                #tìm ds các phiên bị quá hạn thời gian timeout
            expired = [mid for mid, s in self.sessions.items() if now - s.last_seen >= self.timeout]
                
            for mid in expired:
                session = self.sessions.pop(mid)
                    #nếu phiên chưa xong mà bị timeout -> tbao hủy phiên
                if not session.complete and not session.failed:
                    self._notify(mid, session, "Receiver idle timeout")
            return expired

    def cancel(self, msg_id):
        with self._lock:
            session = self.sessions.get(msg_id)
            if session is not None and not session.complete and not session.failed:
                session.failed = True
                session.chunks.clear()
                return True
            return False

    def close(self):
        with self._lock:
            self.sessions.clear()