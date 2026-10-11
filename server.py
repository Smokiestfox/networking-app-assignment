import socket
from datetime import datetime
from config import SERVER_HOST, TCP_PORT
from protocol import recv_json_frame, send_json_frame


class CollaborationServer:
    def __init__(self, host=SERVER_HOST, port=TCP_PORT, log_file="server.log"):
        self.host = host
        self.port = port
        self.log_file = log_file
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def log_file_write(self, text):
        if self.log_file:
            try:
                with open(self.log_file, "a", encoding="utf-8") as f:
                    f.write(text + "\n")
            except OSError:
                pass

    def handle_client(self, conn, addr):
        try:
            while True:
                data = recv_json_frame(conn)
                if data is None:
                    text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Client {addr[0]}:{addr[1]} disconnected"
                    print(text, flush=True)
                    self.log_file_write(text)
                    break
                text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Received from {addr[0]}:{addr[1]}: {data}"
                print(text, flush=True)
                self.log_file_write(text)
                if isinstance(data, dict) and str(data.get("type", "")).upper() == "HELLO":
                    response = {"type": "ACK", "status": "OK"}
                    send_json_frame(conn, response)
                    text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Sent ACK to {addr[0]}:{addr[1]}: {response}"
                    print(text, flush=True)
                    self.log_file_write(text)
        except (ConnectionResetError, ConnectionAbortedError):
            text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Connection reset by {addr[0]}:{addr[1]}"
            print(text, flush=True)
            self.log_file_write(text)
        except Exception as e:
            text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Error handling {addr[0]}:{addr[1]}: {e}"
            print(text, flush=True)
            self.log_file_write(text)
        finally:
            try:
                conn.close()
            except OSError:
                pass
            text = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Closed connection for {addr[0]}:{addr[1]}"
            print(text, flush=True)
            self.log_file_write(text)

    def run(self):
        self.sock.bind((self.host, self.port))
        self.sock.listen(5)
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
                self.handle_client(conn, addr)
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
