import socket
from datetime import datetime
from config import SERVER_HOST, TCP_PORT
from protocol import recv_json_frame, send_json_frame


class CollaborationServer:
    def __init__(self, host=SERVER_HOST, port=TCP_PORT):
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def handle_client(self, conn, addr):
        try:
            while True:
                data = recv_json_frame(conn)
                if data is None:
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Client {addr[0]}:{addr[1]} disconnected")
                    break
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Received from {addr[0]}:{addr[1]}: {data}")
                if isinstance(data, dict) and str(data.get("type", "")).upper() == "HELLO":
                    response = {"type": "ACK", "status": "OK"}
                    send_json_frame(conn, response)
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Sent ACK to {addr[0]}:{addr[1]}: {response}")
        except (ConnectionResetError, ConnectionAbortedError):
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Connection reset by {addr[0]}:{addr[1]}")
        except Exception as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Error handling {addr[0]}:{addr[1]}: {e}")
        finally:
            try:
                conn.close()
            except OSError:
                pass
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Closed connection for {addr[0]}:{addr[1]}")

    def run(self):
        self.sock.bind((self.host, self.port))
        self.sock.listen(5)
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] TCP server listening on {self.host}:{self.port}")
        try:
            while True:
                try:
                    conn, addr = self.sock.accept()
                except (KeyboardInterrupt, OSError):
                    break
                except Exception as e:
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Accept error: {e}")
                    break

                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Client connected from {addr[0]}:{addr[1]}")
                self.handle_client(conn, addr)
        except KeyboardInterrupt:
            pass
        finally:
            try:
                self.sock.close()
            except OSError:
                pass
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Server stopped")


Server = CollaborationServer

if __name__ == "__main__":
    CollaborationServer().run()
