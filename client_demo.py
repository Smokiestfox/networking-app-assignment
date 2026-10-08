import socket
import sys
import time
from config import SERVER_HOST, TCP_PORT
from protocol import send_json_frame, recv_json_frame


def run_demo_client(client_name="DemoClient"):
    host = "127.0.0.1" if SERVER_HOST == "0.0.0.0" else SERVER_HOST
    port = TCP_PORT

    print(f"[{client_name}] Connecting to server at {host}:{port}...")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((host, port))
        print(f"[{client_name}] Connected successfully!")

        hello_msg = {"type": "HELLO", "client": client_name}
        print(f"[{client_name}] Sending: {hello_msg}")
        send_json_frame(sock, hello_msg)

        response = recv_json_frame(sock)
        print(f"[{client_name}] Received response: {response}")

        if response and response.get("type") == "ACK":
            print(f"[{client_name}] Handshake successful (ACK received)!")
        else:
            print(f"[{client_name}] Unexpected response: {response}")

        time.sleep(0.5)
    except ConnectionRefusedError:
        print(f"[{client_name}] Error: Could not connect. Is server.py running?")
    except Exception as e:
        print(f"[{client_name}] Error: {e}")
    finally:
        sock.close()
        print(f"[{client_name}] Disconnected.")


if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else "Client1"
    run_demo_client(name)
