import json
import os
import socket
import threading
from datetime import datetime
from config import SERVER_HOST, TCP_PORT
from protocol import recv_json_frame, send_json_frame
from registry import UserRegistry


class CollaborationServer:
    def __init__(self, host=SERVER_HOST, port=TCP_PORT, log_file="server.log", registry=None, users_file="users.json"):
        self.host = host
        self.port = port
        self.log_file = log_file
        self.log_lock = threading.Lock()
        self.registry = registry if registry is not None else UserRegistry(users_file=users_file)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

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

                    if self.registry.authenticate(username, password):
                        if self.registry.register(username, client_ip, client_port, conn=conn):
                            current_username = username
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
            else:
                was_active = self.registry.unregister(addr=addr)

            if was_active:
                text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] User '{current_username}' logged out / unregistered ({client_id})"
                print(text, flush=True)
                self.log_file_write(text)
                self.log_active_users()

            try:
                conn.close()
            except OSError:
                pass
            text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Closed connection for {client_id}"
            print(text, flush=True)
            self.log_file_write(text)

    def run(self):
        self.sock.bind((self.host, self.port))
        self.sock.listen(128)
        text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] TCP server listening on {self.host}:{self.port}"
        print(text, flush=True)
        self.log_file_write(text)
        try:
            while True:
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

                # Nâng cấp server.py: Main thread loop accept(), mỗi kết nối mới phân bổ ngay threading.Thread(target=self.handle_client, daemon=True)
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(conn, addr),
                    daemon=True
                )
                client_thread.start()
        except KeyboardInterrupt:
            pass
        finally:
            try:
                self.sock.close()
            except OSError:
                pass
            text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Server stopped"
            print(text, flush=True)
            self.log_file_write(text)


Server = CollaborationServer

if __name__ == "__main__":
    CollaborationServer().run()
