import json
import os
import socket
import threading
import time
from datetime import datetime
from config import SERVER_HOST, TCP_PORT, UDP_PORT, CLIENT_TIMEOUT
from protocol import (
    recv_json_frame,
    send_json_frame,
    parse_udp,
    encode_packet,
    pack_msg,
    pack_user_list,
    json_obj,
    Packet,
    SeqCounter,
    HEARTBEAT,
    MSG,
    USER_LIST,
)
from registry import UserRegistry


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
    ):
        self.host = host
        self.port = port
        self.udp_port = udp_port if udp_port is not None else (UDP_PORT if self.port == TCP_PORT else self.port + 1)
        self.log_file = log_file
        self.log_lock = threading.Lock()
        self.registry = registry if registry is not None else UserRegistry(users_file=users_file)
        self.client_timeout = client_timeout
        self.cleanup_interval = 0.5  # Chu kỳ quét timeout (giây)
        self.running = False

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
        """Lấy địa chỉ UDP (ip, port) của user từ bảng udp_addrs hoặc UserRegistry."""
        with self.udp_addrs_lock:
            if username in self.udp_addrs:
                return self.udp_addrs[username]

        with self.registry.lock:
            info = self.registry.active_users.get(username)
            if info:
                ip = info.get("ip", "127.0.0.1")
                port = info.get("udp_port") or info.get("port")
                if port:
                    return (ip, int(port))
        return None

    def forward(self, sender: str, packet) -> list:
        """
        Định tuyến gói tin UDP:
        - Broadcast ('*'): chuyển tiếp tới tất cả active users ngoại trừ sender.
        - Unicast ('<username>'): chuyển tiếp tới chính xác user đích nếu online.
        Ghi đè trường 'from' thành sender thực tế để bảo đảm tính toàn vẹn.
        Trả về danh sách các username đã nhận gói tin.
        """
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

        # Đảm bảo trường 'from' là sender thực tế và lấy đích đến
        if isinstance(body, dict):
            if sender:
                body["from"] = sender
            target = body.get("to", "*")
        else:
            target = "*"

        # Đóng gói lại payload sau khi cập nhật 'from'
        if pkt is not None:
            if isinstance(body, dict):
                pkt.payload = json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            raw_data = encode_packet(pkt, encrypt=True)
        elif isinstance(packet, (bytes, bytearray)):
            raw_data = bytes(packet)
        else:
            return []

        # Xác định danh sách nhận tin (recipients)
        recipients = []
        if target == "*":
            # Broadcast: tất cả active users ngoại trừ sender
            active_users = self.registry.get_active_users()
            recipients = [u for u in active_users if u != sender]
            text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Routing Broadcast from '{sender}' to: {recipients}"
            self.log_file_write(text)
        else:
            # Unicast: chỉ gửi cho target nếu target đang active
            if self.registry.is_active(target):
                recipients = [target]
                text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Routing Unicast from '{sender}' to '{target}'"
                self.log_file_write(text)
            else:
                text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Unicast drop: User '{target}' is offline"
                print(text, flush=True)
                self.log_file_write(text)
                return []

        # Gửi gói tin qua UDP socket
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
            except OSError:
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

            # 1. Xử lý HEARTBEAT: cập nhật last_seen và udp endpoint
            if packet.ptype == HEARTBEAT:
                username = body.get("username")
                if username:
                    with self.last_seen_lock:
                        self.last_seen[username] = time.time()
                    with self.udp_addrs_lock:
                        self.udp_addrs[username] = addr
                    text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Received HEARTBEAT from '{username}' ({addr[0]}:{addr[1]})"
                    self.log_file_write(text)

            # 2. Xử lý MSG: cập nhật last_seen cho sender và forward
            elif packet.ptype == MSG:
                sender = body.get("from")
                if sender:
                    with self.last_seen_lock:
                        self.last_seen[sender] = time.time()
                    with self.udp_addrs_lock:
                        self.udp_addrs[sender] = addr
                self.forward(sender, packet)

    def handle_client(self, conn, addr):
        client_ip, client_port = addr[0], addr[1]
        client_id = f"{client_ip}:{client_port}"
        current_username = None

        try:
            while True:
                data = recv_json_frame(conn)
                if data is None:
                    text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Client {client_id} disconnected"
                    print(text, flush=True)
                    self.log_file_write(text)
                    break

                text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Received from {client_id}: {data}"
                print(text, flush=True)
                self.log_file_write(text)

                if not isinstance(data, dict):
                    continue

                msg_type = str(data.get("type", "")).upper()

                # 1. Giữ nguyên tương thích với lệnh HELLO (W1 Handshake)
                if msg_type == "HELLO":
                    response = {"type": "ACK", "status": "OK"}
                    send_json_frame(conn, response)
                    text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Sent ACK to {client_id}: {response}"
                    print(text, flush=True)
                    self.log_file_write(text)

                # 2. Xử lý Đăng nhập (AUTH / LOGIN): authenticate() -> register() atomic -> AUTH_OK / AUTH_FAIL
                elif msg_type in ("AUTH", "LOGIN"):
                    username = data.get("username") or data.get("client") or data.get("user")
                    password = data.get("password") or data.get("pass")
                    udp_port = data.get("udp_port")

                    if self.registry.authenticate(username, password):
                        if self.registry.register(username, client_ip, client_port, conn=conn, udp_port=udp_port):
                            current_username = username
                            now_ts = time.time()
                            with self.last_seen_lock:
                                self.last_seen[username] = now_ts
                            if udp_port:
                                with self.udp_addrs_lock:
                                    self.udp_addrs[username] = (client_ip, int(udp_port))

                            users_list = self.registry.get_user_list()
                            response = {
                                "type": "AUTH_OK",
                                "status": "OK",
                                "user": username,
                                "users": users_list,
                                "user_list": users_list
                            }
                            send_json_frame(conn, response)
                            text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Sent AUTH_OK to {client_id}: User '{username}' logged in successfully"
                            print(text, flush=True)
                            self.log_file_write(text)
                            self.log_active_users()
                            self.broadcast_user_list()
                        else:
                            response = {
                                "type": "AUTH_FAIL",
                                "status": "ERROR",
                                "message": f"User '{username}' already logged in"
                            }
                            send_json_frame(conn, response)
                            text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Sent AUTH_FAIL to {client_id}: User '{username}' already online"
                            print(text, flush=True)
                            self.log_file_write(text)
                    else:
                        response = {
                            "type": "AUTH_FAIL",
                            "status": "ERROR",
                            "message": "Authentication failed: invalid username or password"
                        }
                        send_json_frame(conn, response)
                        text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Sent AUTH_FAIL to {client_id}: Invalid credentials for '{username}'"
                        print(text, flush=True)
                        self.log_file_write(text)

                # 3. Lệnh Đăng xuất / Ngắt phiên (BYE / LOGOUT): Client chủ động ngắt
                elif msg_type in ("BYE", "LOGOUT"):
                    response = {"type": "ACK", "status": "BYE"}
                    try:
                        send_json_frame(conn, response)
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
                    send_json_frame(conn, response)

                # 5. PING kiểm tra liveness
                elif msg_type == "PING":
                    send_json_frame(conn, {"type": "PONG"})

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

            try:
                conn.close()
            except OSError:
                pass
            text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Closed connection for {client_id}"
            print(text, flush=True)
            self.log_file_write(text)

    def stop(self):
        """Dừng server an toàn, đóng cả socket TCP và UDP."""
        self.running = False
        try:
            self.sock.close()
        except OSError:
            pass
        try:
            self.udp_sock.close()
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

