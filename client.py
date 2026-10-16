# client.py – TCP client (Week 1)
# Task: Connect to server, send HELLO, receive ACK, send BYE.

import socket
import sys
import logging
import time

from config import SERVER_HOST, TCP_PORT
from protocol import send_json_frame, recv_json_frame

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("client")


def run_client(client_name: str = "Client1", host: str = None, port: int = TCP_PORT) -> None:
    if host is None:
        host = "127.0.0.1" if SERVER_HOST == "0.0.0.0" else SERVER_HOST

    log.info("[%s] Connecting to TCP server at %s:%d...", client_name, host, port)
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.connect((host, port))
            log.info("[%s] Connected successfully!", client_name)

            # 1. Send HELLO
            hello_msg = {"type": "HELLO", "client": client_name}
            log.info("[%s] → Sent: %s", client_name, hello_msg)
            send_json_frame(sock, hello_msg)

            # 2. Wait ACK
            response = recv_json_frame(sock)
            log.info("[%s] ← Received: %s", client_name, response)

            if response and response.get("type") == "ACK":
                log.info("[%s] Handshake successful (ACK received)!", client_name)
            else:
                log.warning("[%s] Unexpected response: %s", client_name, response)

            time.sleep(0.5)

            # 3. Send BYE to close connection
            bye_msg = {"type": "BYE", "client": client_name}
            log.info("[%s] → Sent: %s", client_name, bye_msg)
            send_json_frame(sock, bye_msg)

        except ConnectionRefusedError:
            log.error("[%s] Error: Connection refused. Is server.py running?", client_name)
        except Exception as e:
            log.error("[%s] An error occurred: %s", client_name, e)
        finally:
            log.info("[%s] Connection closed.", client_name)


if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else "Client1"
    target_host = sys.argv[2] if len(sys.argv) > 2 else None
    target_port = int(sys.argv[3]) if len(sys.argv) > 3 else TCP_PORT
    run_client(client_name=name, host=target_host, port=target_port)
