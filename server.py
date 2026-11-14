import math
import os
import socket
import threading
import time
import uuid
from datetime import datetime
from file_rendezvous import FileRendezvous
from config import SERVER_HOST, TCP_PORT, UDP_PORT, CLIENT_TIMEOUT, FILE_SESSION_TIMEOUT
from protocol import (
    recv_json_frame,
    send_json_frame,
    mask_credentials,
    parse_udp,
    build_udp,
    pack_msg,
    pack_user_list,
    pack_file_meta,
    pack_file_chunk,
    pack_ack,
    json_obj,
    FLAG_END,
    MAX_TEXT_LEN,
    Packet,
    SeqCounter,
    HEARTBEAT,
    MSG,
    MAX_TEXT_LEN,
    USER_LIST,
    FILE_META,
    FILE_CHUNK,
    ACK,
)
from registry import UserRegistry


class FileSession:
    """Phiên truyền file: msg_id -> (địa chỉ A, địa chỉ B) kèm thông tin người gửi/nhận.
    Server chỉ định tuyến gói tin (FILE_META, FILE_CHUNK, ACK), không lưu trữ mảnh dữ liệu.
    """

    def __init__(self, msg_id, sender, sender_addr, receivers, receiver_addrs,
                 filename, size, chunks, checksum, broadcast=False):
        self.msg_id = msg_id
        self.sender = sender
        self.sender_addr = sender_addr          # địa chỉ UDP của A (ip, port)
        self.receivers = list(receivers)        # danh sách username nhận (B...)
        self.receiver_addrs = dict(receiver_addrs)  # username -> (ip, port)
        self.broadcast = broadcast
        self._label = "*" if broadcast else self.receivers[0]
        self.filename = filename
        self.size = size
        self.chunks = chunks
        self.checksum = checksum
        self.created = time.time()
        self.last_activity = self.created
        self.to_receiver = 0   # số datagram chuyển A -> B (FILE_META / FILE_CHUNK)
        self.to_sender = 0     # số datagram chuyển B -> A (ACK)

    @property
    def receiver(self):
        """Tên người nhận (unicast) hoặc '*' (broadcast)."""
        return self._label

    @property
    def receiver_addr(self):
        """Địa chỉ UDP của người nhận B (unicast) hoặc None nếu đã rời."""
        if self.broadcast:
            return list(self.receiver_addrs.values())
        return self.receiver_addrs.get(self._label)

    @property
    def endpoints(self):
        """Trả về cặp (địa chỉ A, địa chỉ B) của phiên."""
        return (self.sender_addr, self.receiver_addr)


class CollaborationServer:
    def __init__(
        self,
        host=SERVER_HOST,
        port=TCP_PORT,
        udp_port=None,
        log_file="server.log",
        registry=None,
        users_file="users.json",
        client_timeout=CLIENT_TIMEOUT,
        file_session_timeout=FILE_SESSION_TIMEOUT,
    ):
        self.host = host
        self.port = port
        self.udp_port = udp_port if udp_port is not None else (UDP_PORT if self.port == TCP_PORT else self.port + 1)
        self.log_file = log_file
        self.log_lock = threading.Lock()
        self.connection_locks = {}
        self.connection_locks_guard = threading.Lock()
        self.registry = registry if registry is not None else UserRegistry(users_file=users_file)
        self.file_rendezvous = FileRendezvous(self.registry, self._send_tcp)
        self.client_timeout = client_timeout
        self.cleanup_interval = 0.5  # Chu kỳ quét timeout (giây)
        self.running = False
        self._stop_lock = threading.Lock()
        self._stopped = False

        # Socket TCP
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Socket UDP (port 9001 mặc định)
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Bộ đếm seq cho các bản tin UDP do server phát đi
        self.seq_counter = SeqCounter()

        # Quản lý địa chỉ UDP của client: username -> (ip, port)
        self.udp_addrs = {}
        self.udp_addrs_lock = threading.Lock()

        # Theo dõi thời gian heartbeat gần nhất: username -> float timestamp
        self.last_seen = {}
        self.last_seen_lock = threading.Lock()

        # Bảng phiên truyền file: msg_id (uuid.UUID) -> FileSession
        self.file_session_timeout = file_session_timeout
        self.file_sessions = {}
        self.file_sessions_lock = threading.Lock()

    def log_file_write(self, text):
        if self.log_file:
            try:
                with self.log_lock:
                    with open(self.log_file, "a", encoding="utf-8") as f:
                        f.write(text + "\n")
            except OSError:
                pass

    def log_active_users(self):
        """In bảng Active Users (IP:Port) ra màn hình và ghi vào server.log."""
        table = self.registry.format_active_users_table()
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        banner = f"[{timestamp}] Active Users Table:\n{table}"
        print(banner, flush=True)
        self.log_file_write(banner)

    def get_user_udp_addr(self, username: str):
        """Chỉ trả về địa chỉ UDP đã ràng buộc qua AUTH/registry, không dùng cổng TCP thay thế."""
        with self.registry.lock:
            info = self.registry.active_users.get(username)
            if info is not None and info.get("udp_port") is not None:
                return (info["ip"], int(info["udp_port"]))
        return None

    def forward(self, sender: str, packet) -> list:
        """
        Định tuyến gói tin UDP:
        - Broadcast ('*'): chuyển tiếp tới tất cả active users ngoại trừ sender.
        - Unicast ('<username>'): chuyển tiếp tới chính xác user đích nếu online.
        Ghi đè trường 'from' thành sender thực tế để bảo đảm tính toàn vẹn.
        Trả về danh sách các username đã nhận gói tin.
        """
        if not sender or not isinstance(sender, str) or not self.registry.is_active(sender):
            return []

        body = {}
        pkt = None

        if isinstance(packet, Packet):
            pkt = packet
            try:
                body = json_obj(pkt.payload) if isinstance(pkt.payload, (bytes, bytearray)) else (pkt.payload if isinstance(pkt.payload, dict) else {})
            except Exception:
                body = {}
        elif isinstance(packet, (bytes, bytearray)):
            try:
                pkt, body = parse_udp(packet)
            except Exception as e:
                text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] forward error: cannot parse UDP packet: {e}"
                self.log_file_write(text)
                return []
        elif isinstance(packet, dict):
            body = dict(packet)
            target = body.get("to", "*")
            text_content = body.get("text", "")
            raw = pack_msg(to=target, sender=sender or "", text=text_content, seq=self.seq_counter.next())
            pkt, body = parse_udp(raw)
        else:
            return []

        if isinstance(body, dict):
            if sender:
                body["from"] = sender
            target = body.get("to", "*")
        else:
            target = "*"

        if pkt is not None:
            raw_data = build_udp(pkt.ptype, body if isinstance(body, dict) else {}, pkt.seq,
                                 msg_id=pkt.msg_id, ack=pkt.ack)
        elif isinstance(packet, (bytes, bytearray)):
            raw_data = bytes(packet)
        else:
            return []

        recipients = []
        if target == "*":
            active_users = self.registry.get_active_users()
            recipients = [u for u in active_users if u != sender]
            text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Routing Broadcast from '{sender}' to: {recipients}"
            self.log_file_write(text)
        else:
            if self.registry.is_active(target):
                recipients = [target]
                text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Routing Unicast from '{sender}' to '{target}'"
                self.log_file_write(text)
            else:
                text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Unicast drop: User '{target}' is offline"
                print(text, flush=True)
                self.log_file_write(text)
                return []

        sent_to = []
        for u in recipients:
            addr = self.get_user_udp_addr(u)
            if addr:
                try:
                    self.udp_sock.sendto(raw_data, addr)
                    sent_to.append(u)
                except Exception as e:
                    text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Failed to forward UDP to '{u}' at {addr}: {e}"
                    self.log_file_write(text)
            else:
                text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] No UDP address for user '{u}', skipped"
                self.log_file_write(text)

        return sent_to

    def _send_tcp(self, conn, body):
        with self.connection_locks_guard:
            lock = self.connection_locks.get(conn)
        if lock is None:
            raise ConnectionError("Connection closed")
        with lock:
            send_json_frame(conn, body)

    def forward_chat(self, sender, body):
        target, text, message_id = body.get("to"), body.get("text"), body.get("id")
        if (not isinstance(target, str) or not target.strip() or target == sender
                or not isinstance(text, str) or not 1 <= len(text) <= MAX_TEXT_LEN
                or not isinstance(message_id, str)):
            return
        try:
            message_id = str(uuid.UUID(message_id))
        except (ValueError, AttributeError):
            return
        outgoing = {"type": "MSG", "id": message_id, "from": sender,
                    "to": target, "text": text, "time": time.time()}
        with self.registry.lock:
            recipients = [(name, info.get("conn")) for name, info in self.registry.active_users.items()
                          if name != sender and (target == "*" or name == target)]
        for name, connection in recipients:
            if connection is None:
                continue
            try:
                self._send_tcp(connection, outgoing)
            except (OSError, ConnectionError):
                self.log_file_write(f"Could not send chat to {name}")

    # ------------------------------------------------------------------
    # File relay & Session Management (Week 4 - Section 8 & 8.5 of protocol.md)
    # ------------------------------------------------------------------
    def log_event(self, text):
        text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {text}"
        print(text, flush=True)
        self.log_file_write(text)

    def user_by_udp_addr(self, addr):
        """Tìm username active theo đúng endpoint UDP (IP TCP, udp_port) đã đăng ký qua AUTH."""
        if not isinstance(addr, tuple) or len(addr) < 2:
            return None
        if isinstance(addr[1], bool) or not isinstance(addr[1], int):
            return None

        src_ip, src_port = str(addr[0]), addr[1]
        with self.registry.lock:
            for user, info in self.registry.active_users.items():
                udp_port = info.get("udp_port")
                if udp_port is not None and info.get("ip") == src_ip and int(udp_port) == src_port:
                    return user
        return None

    def _resolve_sender(self, addr, claimed_sender=None, require_claimed_match=False):
        """Xác thực endpoint UDP nguồn (và tên khai báo nếu cần) trước khi cập nhật last_seen / udp_addrs."""
        sender = self.user_by_udp_addr(addr)
        if sender is None or not self.registry.is_active(sender):
            return None
        if require_claimed_match and (not isinstance(claimed_sender, str) or claimed_sender != sender):
            return None
        with self.last_seen_lock:
            self.last_seen[sender] = time.time()
        with self.udp_addrs_lock:
            self.udp_addrs[sender] = (str(addr[0]), int(addr[1]))
        return sender

    @staticmethod
    def _valid_file_meta(body):
        """Kiểm tra tính hợp lệ của metadata theo mục 8.2 & 8.5.4 (size, chunks, checksum SHA-256)."""
        filename = body.get("filename")
        size = body.get("size")
        chunks = body.get("chunks")
        checksum = body.get("checksum")
        if not isinstance(filename, str) or not filename.strip():
            return False
        if not isinstance(size, int) or size < 0:
            return False
        if not isinstance(chunks, int) or chunks < 1:
            return False
        if not isinstance(checksum, str) or len(checksum) != 64:
            return False
        return True

    def handle_file_meta(self, packet, body, addr):
        """Xử lý FILE_META (mục 8.1, 8.2, 8.5):
        - Kiểm tra người gửi & người nhận đang online.
        - Tạo / cập nhật phiên msg_id -> (địa chỉ A, địa chỉ B); hỗ trợ gửi lại FILE_META khi mất ACK (8.5.1/8.5.3).
        - Dùng pack_file_meta() có sẵn trong protocol.py để đóng gói và chuyển tiếp tới B.
        """
        sender = self._resolve_sender(addr)
        if sender is None:
            self.log_file_write(f"FILE_META from unknown/offline address {addr} dropped")
            return

        if not self._valid_file_meta(body):
            self.log_event(f"File session rejected: invalid FILE_META fields from '{sender}'")
            return

        target = body.get("to")
        broadcast = target == "*"
        if broadcast:
            receivers = []
            receiver_addrs = {}
            for u in self.registry.get_active_users():
                if u != sender:
                    u_addr = self.get_user_udp_addr(u)
                    if u_addr is not None:
                        receivers.append(u)
                        receiver_addrs[u] = u_addr
            if not receivers:
                self.log_event(f"File session rejected: nobody online to receive broadcast from '{sender}'")
                return
        else:
            if not target or target == sender:
                self.log_event(f"File session rejected: invalid target '{target}' from '{sender}'")
                return
            if not self.registry.is_active(target):
                self.log_event(f"File session rejected: receiver '{target}' is offline (sender '{sender}')")
                return
            recv_addr = self.get_user_udp_addr(target)
            if recv_addr is None:
                self.log_event(f"File session rejected: no UDP address for '{target}' (sender '{sender}')")
                return
            receivers = [target]
            receiver_addrs = {target: recv_addr}

        created = False
        with self.file_sessions_lock:
            session = self.file_sessions.get(packet.msg_id)
            if session is None:
                session = FileSession(
                    msg_id=packet.msg_id,
                    sender=sender,
                    sender_addr=addr,
                    receivers=receivers,
                    receiver_addrs=receiver_addrs,
                    filename=body["filename"],
                    size=body["size"],
                    chunks=body["chunks"],
                    checksum=body["checksum"],
                    broadcast=broadcast,
                )
                self.file_sessions[packet.msg_id] = session
                created = True
            elif session.sender != sender or session.broadcast != broadcast or (
                    not broadcast and session.receivers != receivers):
                self.log_event(f"File session {packet.msg_id} conflict: META from '{sender}' dropped")
                return
            else:
                # 8.5.1 & 8.5.3: FILE_META gửi lại (retransmit) -> cập nhật địa chỉ mới nhất và chuyển tiếp lại
                session.sender_addr = addr
                for u in list(session.receivers):
                    u_addr = self.get_user_udp_addr(u)
                    if u_addr is not None:
                        session.receiver_addrs[u] = u_addr
            session.last_activity = time.time()
            dest_items = list(session.receiver_addrs.items())

        # Dùng hàm pack_file_meta có sẵn trong protocol.py (ghi đè 'from' = sender thực tế)
        raw = pack_file_meta(
            to=target,
            sender=sender,
            filename=body["filename"],
            size=body["size"],
            chunks=body["chunks"],
            checksum=body["checksum"],
            seq=packet.seq,
            msg_id=packet.msg_id,
        )
        sent = self._send_to_targets(raw, dest_items)
        with self.file_sessions_lock:
            session.to_receiver += sent
        if created:
            self.log_event(
                f"File session {packet.msg_id} opened: {sender} ({addr[0]}:{addr[1]}) -> "
                f"{session.receiver} ({session.receiver_addr}) "
                f"file='{session.filename}' size={session.size} chunks={session.chunks}"
            )

    def _send_to_targets(self, raw, dest_items):
        """Gửi datagram tới danh sách (username, udp_addr); trả về số datagram đã gửi thành công."""
        sent = 0
        for user, dest_addr in dest_items:
            if not self.registry.is_active(user):
                continue
            current_addr = self.get_user_udp_addr(user) or dest_addr
            if current_addr is None:
                continue
            try:
                self.udp_sock.sendto(raw, current_addr)
                sent += 1
            except OSError as e:
                self.log_file_write(f"File relay to '{user}' at {current_addr} failed: {e}")
        return sent

    def relay_file_packet(self, packet, body, addr):
        """Chuyển tiếp FILE_CHUNK (A -> B) và ACK (B -> A) theo bảng phiên msg_id -> (địa chỉ A, địa chỉ B):
        - Dùng pack_file_chunk() và pack_ack() có sẵn trong protocol.py.
        - Hỗ trợ nhận lệch thứ tự (8.5.2) và gói gửi lại/trùng lặp (8.5.1, 8.5.3); không lưu mảnh trên server.
        """
        user = self._resolve_sender(addr)
        if user is None:
            return

        with self.file_sessions_lock:
            session = self.file_sessions.get(packet.msg_id)
            if session is None:
                return                               # phiên đã đóng / quá hạn 30s / không tồn tại: bỏ qua

            if user == session.sender and packet.ptype == FILE_CHUNK:
                if packet.chunk >= session.chunks or packet.total != session.chunks:
                    return                           # chỉ số chunk không khớp phiên
                session.sender_addr = addr
                dest_items = list(session.receiver_addrs.items())
                flags = FLAG_END if (packet.flags & FLAG_END) else 0
                raw = pack_file_chunk(
                    msg_id=packet.msg_id,
                    chunk=packet.chunk,
                    total=packet.total,
                    data=body,
                    seq=packet.seq,
                    flags=flags,
                )
            elif user in session.receivers and packet.ptype == ACK:
                session.receiver_addrs[user] = addr
                dest_items = [(session.sender, session.sender_addr)]
                raw = pack_ack(
                    ack_seq=packet.ack,
                    seq=packet.seq,
                    msg_id=packet.msg_id,
                    chunk=packet.chunk,
                )
            else:
                return                               # bên thứ ba hoặc sai chiều: bỏ

            session.last_activity = time.time()
            msg_id = session.msg_id

        sent = self._send_to_targets(raw, dest_items)
        with self.file_sessions_lock:
            if packet.ptype == FILE_CHUNK:
                session.to_receiver += sent
            else:
                session.to_sender += sent
        if sent == 0:
            self.close_file_session(msg_id, "peer went offline")

    def close_file_session(self, msg_id, reason):
        with self.file_sessions_lock:
            session = self.file_sessions.pop(msg_id, None)
        if session is None:
            return False
        duration = time.time() - session.created
        self.log_event(
            f"File session {msg_id} closed ({reason}): {session.sender} -> {session.receiver} "
            f"file='{session.filename}' relayed A->B={session.to_receiver} B->A={session.to_sender} "
            f"duration={duration:.1f}s"
        )
        return True

    def close_sessions_for_user(self, username):
        """Dọn phiên khi một bên rời: người gửi rời, hoặc người nhận cuối cùng rời => đóng phiên."""
        to_close = []
        with self.file_sessions_lock:
            for mid, sess in self.file_sessions.items():
                if sess.sender == username:
                    to_close.append(mid)
                elif username in sess.receivers:
                    sess.receivers.remove(username)
                    sess.receiver_addrs.pop(username, None)
                    if not sess.receivers:
                        to_close.append(mid)
        for mid in to_close:
            self.close_file_session(mid, f"'{username}' left")
        return to_close

    def cleanup_file_sessions(self):
        """8.5.5: Dọn phiên không có gói nào trong file_session_timeout giây (mặc định 30s)."""
        now = time.time()
        with self.file_sessions_lock:
            ids = [mid for mid, s in self.file_sessions.items()
                   if now - s.last_activity > self.file_session_timeout]
        for mid in ids:
            self.close_file_session(mid, f"idle > {self.file_session_timeout:g}s")
        return ids

    def broadcast_user_list(self):
        """Gửi gói tin USER_LIST (ptype=6) cập nhật danh sách user online qua UDP."""
        active_users = self.registry.get_active_users()
        seq = self.seq_counter.next()
        data = pack_user_list(active_users, seq=seq)

        for u in active_users:
            addr = self.get_user_udp_addr(u)
            if addr:
                try:
                    self.udp_sock.sendto(data, addr)
                except Exception as e:
                    self.log_file_write(f"Error sending USER_LIST to {u} at {addr}: {e}")

    def cleanup_stale_users(self) -> list:
        """
        Quét timeout sau client_timeout (10s) không thấy HEARTBEAT:
        - Xóa user khỏi Active List trong registry.
        - Xóa khỏi last_seen và udp_addrs.
        - Đóng socket TCP nếu còn kết nối.
        - Broadcast USER_LIST mới tới tất cả user còn lại.
        """
        now = time.time()
        stale_users = []
        with self.last_seen_lock:
            for u, t in list(self.last_seen.items()):
                if now - t > self.client_timeout:
                    stale_users.append(u)

        if not stale_users:
            return []

        for user in stale_users:
            with self.last_seen_lock:
                self.last_seen.pop(user, None)
            with self.udp_addrs_lock:
                self.udp_addrs.pop(user, None)

            conn = None
            with self.registry.lock:
                info = self.registry.active_users.get(user)
                if info:
                    conn = info.get("conn")

            self.registry.unregister(username=user)
            self.close_sessions_for_user(user)
            if conn:
                try:
                    conn.close()
                except OSError:
                    pass

            text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] User '{user}' timed out (no heartbeat for >{self.client_timeout}s) - removed"
            print(text, flush=True)
            self.log_file_write(text)

        self.log_active_users()
        self.broadcast_user_list()
        return stale_users

    def cleanup_loop(self):
        """Luồng quét dọn timeout chạy nền (Daemon thread)."""
        while self.running:
            try:
                self.cleanup_stale_users()
                self.cleanup_file_sessions()
                self.file_rendezvous.expire()
            except Exception as e:
                text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Error in cleanup_loop: {e}"
                self.log_file_write(text)

            # Sleep theo từng bước nhỏ 0.1s để thoát ngay khi server dừng
            steps = int(self.cleanup_interval / 0.1)
            for _ in range(max(1, steps)):
                if not self.running:
                    break
                time.sleep(0.1)

    def udp_loop(self):
        """Luồng nhận và xử lý datagram trên cổng UDP 9001."""
        while self.running:
            try:
                data, addr = self.udp_sock.recvfrom(65535)
                if not self.running:
                    break
            except ConnectionResetError as e:
                if not self.running:
                    break
                text = (
                    f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                    f"UDP ConnectionResetError: {e}; continuing recv loop"
                )
                print(text, flush=True)
                self.log_file_write(text)
                continue
            except OSError as e:
                if not self.running:
                    break
                text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] UDP recv OSError: {e}; stopping recv loop"
                print(text, flush=True)
                self.log_file_write(text)
                break
            except Exception as e:
                if not self.running:
                    break
                continue

            try:
                packet, body = parse_udp(data)
            except Exception as e:
                text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Invalid UDP packet from {addr}: {e}"
                self.log_file_write(text)
                continue

            # 1. Xử lý HEARTBEAT: xác thực endpoint UDP rồi mới cập nhật last_seen / udp_addrs
            if packet.ptype == HEARTBEAT:
                if not isinstance(body, dict):
                    self.log_file_write(
                        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] WARNING: invalid HEARTBEAT body from {addr}; dropped"
                    )
                    continue

                username = body.get("username")
                if not isinstance(username, str) or not username.strip():
                    self.log_file_write(
                        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] WARNING: invalid HEARTBEAT username from {addr}; dropped"
                    )
                    continue

                sender = self._resolve_sender(
                    addr,
                    claimed_sender=username,
                    require_claimed_match=True,
                )
                if sender is None:
                    self.log_file_write(
                        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] WARNING: rejected unauthenticated/spoofed HEARTBEAT for '{username}' from {addr}"
                    )
                    continue

                text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Received HEARTBEAT from '{sender}' ({addr[0]}:{addr[1]})"
                self.log_file_write(text)

            # 2. Xử lý MSG: kiểm tra định dạng + xác thực endpoint UDP rồi mới cập nhật last_seen và forward
            elif packet.ptype == MSG:
                if not isinstance(body, dict):
                    self.log_file_write(
                        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] WARNING: invalid MSG body from {addr}; dropped"
                    )
                    continue

                claimed_from = body.get("from")
                to = body.get("to")
                msg_text = body.get("text")
                msg_time = body.get("time")

                if not (
                    isinstance(claimed_from, str)
                    and bool(claimed_from.strip())
                    and isinstance(to, str)
                    and bool(to.strip())
                    and isinstance(msg_text, str)
                    and 1 <= len(msg_text) <= MAX_TEXT_LEN
                    and isinstance(msg_time, (int, float))
                    and not isinstance(msg_time, bool)
                    and math.isfinite(msg_time)
                ):
                    self.log_file_write(
                        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] WARNING: invalid MSG fields from {addr}; dropped"
                    )
                    continue

                sender = self._resolve_sender(
                    addr,
                    claimed_sender=claimed_from,
                    require_claimed_match=True,
                )
                if sender is None:
                    self.log_file_write(
                        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] WARNING: rejected unauthenticated/spoofed MSG from '{claimed_from}' at {addr}"
                    )
                    continue

                self.forward(sender, packet)

            # 3. Relay file: FILE_META / FILE_CHUNK / ACK (chỉ chuyển tiếp, không lưu mảnh)
            elif packet.ptype == FILE_META:
                self.handle_file_meta(packet, body, addr)
            elif packet.ptype in (FILE_CHUNK, ACK):
                self.relay_file_packet(packet, body, addr)

    def handle_client(self, conn, addr):
        client_ip, client_port = addr[0], addr[1]
        client_id = f"{client_ip}:{client_port}"
        current_username = None
        with self.connection_locks_guard:
            self.connection_locks[conn] = threading.RLock()

        try:
            while True:
                data = recv_json_frame(conn)
                if data is None:
                    text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Client {client_id} disconnected"
                    print(text, flush=True)
                    self.log_file_write(text)
                    break

                safe_data = mask_credentials(data)
                text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Received from {client_id}: {safe_data}"
                print(text, flush=True)
                self.log_file_write(text)

                if not isinstance(data, dict):
                    continue

                msg_type = str(data.get("type", "")).upper()

                # 1. Giữ nguyên tương thích với lệnh HELLO (W1 Handshake)
                if msg_type == "HELLO":
                    response = {"type": "ACK", "status": "OK"}
                    self._send_tcp(conn, response)
                    text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Sent ACK to {client_id}: {response}"
                    print(text, flush=True)
                    self.log_file_write(text)

                # 2. Xử lý Đăng nhập (AUTH / LOGIN): authenticate() -> register() atomic -> AUTH_OK / AUTH_FAIL
                elif msg_type in ("AUTH", "LOGIN"):
                    if current_username is not None:
                        self._send_tcp(conn, {
                            "type": "AUTH_FAIL",
                            "status": "ERROR",
                            "message": "Already authenticated on this connection",
                        })
                        continue

                    username = data.get("username") or data.get("client") or data.get("user")
                    password = data.get("password") or data.get("pass")
                    udp_port = data.get("udp_port")

                    if self.registry.authenticate(username, password):
                        with self.connection_locks[conn]:
                            reg_result = self.registry.register(
                                username, client_ip, client_port, conn=conn, udp_port=udp_port
                            )
                            if reg_result:
                                current_username = username
                                now_ts = time.time()
                                with self.last_seen_lock:
                                    self.last_seen[username] = now_ts
                                if udp_port is not None:
                                    with self.udp_addrs_lock:
                                        self.udp_addrs[username] = (str(client_ip), int(udp_port))

                                users_list = self.registry.get_user_list()
                                response = {
                                    "type": "AUTH_OK",
                                    "status": "OK",
                                    "user": username,
                                    "users": users_list,
                                    "user_list": users_list
                                }
                                self._send_tcp(conn, response)
                        if reg_result:
                            text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Sent AUTH_OK to {client_id}: User '{username}' logged in successfully"
                            print(text, flush=True)
                            self.log_file_write(text)
                            self.log_active_users()
                            self.broadcast_user_list()
                        else:
                            if reg_result.message == "INVALID_UDP_PORT":
                                fail_msg = "Authentication failed: invalid udp_port"
                            elif reg_result.message == "UDP_ENDPOINT_IN_USE":
                                fail_msg = "Authentication failed: UDP endpoint already in use"
                            else:
                                fail_msg = f"User '{username}' already logged in"

                            response = {
                                "type": "AUTH_FAIL",
                                "status": "ERROR",
                                "message": fail_msg
                            }
                            self._send_tcp(conn, response)
                            text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Sent AUTH_FAIL to {client_id}: {fail_msg}"
                            print(text, flush=True)
                            self.log_file_write(text)
                    else:
                        response = {
                            "type": "AUTH_FAIL",
                            "status": "ERROR",
                            "message": "Authentication failed: invalid username or password"
                        }
                        self._send_tcp(conn, response)
                        text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Sent AUTH_FAIL to {client_id}: Invalid credentials for '{username}'"
                        print(text, flush=True)
                        self.log_file_write(text)

                elif msg_type in {"FILE_OFFER", "FILE_ACCEPT", "FILE_REJECT", "FILE_CANCEL"}:
                    if current_username is not None:
                        self.file_rendezvous.handle(current_username, conn, data)

                elif msg_type == "MSG":
                    if current_username is not None:
                        self.forward_chat(current_username, data)

                # 3. Lệnh Đăng xuất / Ngắt phiên (BYE / LOGOUT): Client chủ động ngắt
                elif msg_type in ("BYE", "LOGOUT"):
                    response = {"type": "ACK", "status": "BYE"}
                    try:
                        self._send_tcp(conn, response)
                    except Exception:
                        pass
                    text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Client {client_id} sent {msg_type}"
                    print(text, flush=True)
                    self.log_file_write(text)
                    break

                # 4. Yêu cầu lấy danh sách user
                elif msg_type in ("USER_LIST", "GET_USERS"):
                    response = {
                        "type": "USER_LIST",
                        "status": "OK",
                        "users": self.registry.get_user_list()
                    }
                    self._send_tcp(conn, response)

                # 5. PING kiểm tra liveness
                elif msg_type == "PING":
                    self._send_tcp(conn, {"type": "PONG"})

        except (ConnectionResetError, ConnectionAbortedError):
            text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Connection reset by {client_id}"
            print(text, flush=True)
            self.log_file_write(text)
        except Exception as e:
            text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Error handling {client_id}: {e}"
            print(text, flush=True)
            self.log_file_write(text)
        finally:
            # Tự động gọi registry.unregister() và dọn dẹp state sạch sẽ
            was_active = False
            if current_username:
                was_active = self.registry.unregister(username=current_username)
                self.close_sessions_for_user(current_username)
                self.file_rendezvous.expire(current_username)
                with self.last_seen_lock:
                    self.last_seen.pop(current_username, None)
                with self.udp_addrs_lock:
                    self.udp_addrs.pop(current_username, None)
            else:
                was_active = self.registry.unregister(addr=addr)

            if was_active:
                text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] User '{current_username}' logged out / unregistered ({client_id})"
                print(text, flush=True)
                self.log_file_write(text)
                self.log_active_users()
                self.broadcast_user_list()

            with self.connection_locks_guard:
                self.connection_locks.pop(conn, None)
            try:
                conn.close()
            except OSError:
                pass
            text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Closed connection for {client_id}"
            print(text, flush=True)
            self.log_file_write(text)

    def stop(self):
        """Dừng server an toàn, đóng cả socket TCP và UDP."""
        with self._stop_lock:
            if self._stopped:
                return
            self.running = False
            self._stopped = True

            for sock in (self.sock, self.udp_sock):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    sock.close()
                except OSError:
                    pass

            with self.registry.lock:
                connections = [
                    info["conn"]
                    for info in self.registry.active_users.values()
                    if info.get("conn") is not None
                ]

            for conn in connections:
                try:
                    conn.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    conn.close()
                except OSError:
                    pass

    def close(self):
        self.stop()

    def run(self):
        self.running = True
        self.sock.bind((self.host, self.port))
        self.sock.listen(128)
        text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] TCP server listening on {self.host}:{self.port}"
        print(text, flush=True)
        self.log_file_write(text)

        self.udp_sock.bind((self.host, self.udp_port))
        text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] UDP server listening on {self.host}:{self.udp_port}"
        print(text, flush=True)
        self.log_file_write(text)

        # Khởi chạy luồng nhận UDP song song với TCP
        udp_thread = threading.Thread(target=self.udp_loop, daemon=True)
        udp_thread.start()

        # Khởi chạy luồng quét dọn timeout (cleanup_loop)
        cleanup_thread = threading.Thread(target=self.cleanup_loop, daemon=True)
        cleanup_thread.start()

        try:
            while self.running:
                try:
                    conn, addr = self.sock.accept()
                except (KeyboardInterrupt, OSError):
                    break
                except Exception as e:
                    text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Accept error: {e}"
                    print(text, flush=True)
                    self.log_file_write(text)
                    break

                text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Client connected from {addr[0]}:{addr[1]}"
                print(text, flush=True)
                self.log_file_write(text)

                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(conn, addr),
                    daemon=True
                )
                client_thread.start()
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()
            text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Server stopped"
            print(text, flush=True)
            self.log_file_write(text)


Server = CollaborationServer

if __name__ == "__main__":
    CollaborationServer().run()

