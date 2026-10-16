# client.py – TCP client with AUTH/LOGIN lifecycle
# Luồng: Connect -> AUTH {username, password} -> AUTH_OK -> ... -> BYE -> Close

import socket
import sys
import logging
import time

from config import SERVER_HOST, TCP_PORT
from protocol import send_json_frame, recv_json_frame

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("client")


def run_client(
    username: str = "alice",
    password: str = "123456",
    host: str = None,
    port: int = TCP_PORT,
) -> None:
    if host is None:
        host = "127.0.0.1" if SERVER_HOST == "0.0.0.0" else SERVER_HOST

    log.info("[%s] Connecting to TCP server at %s:%d...", username, host, port)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.connect((host, port))
            log.info("[%s] Connected successfully to server!", username)

            # 1. Gửi thông điệp AUTH / LOGIN
            auth_msg = {
                "type": "AUTH",
                "username": username,
                "password": password,
            }
            log.info("[%s] → Sent AUTH: %s", username, auth_msg)
            send_json_frame(sock, auth_msg)

            # 2. Nhận kết quả xác thực từ Server
            response = recv_json_frame(sock)
            log.info("[%s] ← Received response: %s", username, response)

            if not response or response.get("type") != "AUTH_OK":
                log.error("[%s] Authentication failed! Reason: %s", username, response.get("message") if response else "No response")
                return

            log.info("[%s] Authentication successful! Online users: %s", username, response.get("users", []))

            # Mô phỏng thời gian giữ phiên kết nối
            time.sleep(1.0)

            # 3. Yêu cầu danh sách user cập nhật
            user_list_msg = {"type": "USER_LIST"}
            log.info("[%s] → Requesting updated user list...", username)
            send_json_frame(sock, user_list_msg)
            list_resp = recv_json_frame(sock)
            log.info("[%s] ← Active users from server: %s", username, list_resp.get("users", []))

            time.sleep(0.5)

            # 4. Gửi BYE để chủ động ngắt kết nối an toàn (Graceful Disconnect)
            bye_msg = {"type": "BYE", "username": username}
            log.info("[%s] → Sent BYE (Logout): %s", username, bye_msg)
            send_json_frame(sock, bye_msg)

            bye_ack = recv_json_frame(sock)
            log.info("[%s] ← Received ACK for BYE: %s", username, bye_ack)

        except ConnectionRefusedError:
            log.error("[%s] Connection refused. Is server.py running?", username)
        except Exception as e:
            log.error("[%s] An error occurred: %s", username, e)
        finally:
            log.info("[%s] Connection closed.", username)


if __name__ == "__main__":
    # Tham số: python client.py [username] [password] [host] [port]
    user = sys.argv[1] if len(sys.argv) > 1 else "alice"
    pwd = sys.argv[2] if len(sys.argv) > 2 else "123456"
    target_host = sys.argv[3] if len(sys.argv) > 3 else None
    target_port = int(sys.argv[4]) if len(sys.argv) > 4 else TCP_PORT

    run_client(username=user, password=pwd, host=target_host, port=target_port)
