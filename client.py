# client.py – TCP client (Week 2: Authentication & Session Management)
# Task: Connect to TCP server, perform HELLO handshake, authenticate with AUTH, maintain session, graceful exit with BYE.

import socket
import sys
import logging
import time

from config import SERVER_HOST, TCP_PORT
from protocol import send_json_frame, recv_json_frame

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("client")


def format_user_list(users: list) -> str:
    """Format active users list as an ASCII table."""
    if not users:
        return "  (No active users online)"
    lines = ["  +--------------------+-----------------+--------+"]
    lines.append("  | Username           | IP Address      | Port   |")
    lines.append("  +--------------------+-----------------+--------+")
    for u in users:
        uname = u.get("username", "")
        uip = u.get("ip", "")
        uport = u.get("port", "")
        lines.append(f"  | {uname:<18} | {uip:<15} | {str(uport):<6} |")
    lines.append("  +--------------------+-----------------+--------+")
    return "\n".join(lines)


def run_client(
    username: str = None,
    password: str = None,
    host: str = None,
    port: int = TCP_PORT,
    duration: float = 0.0,
    interactive: bool = True,
) -> bool:
    """
    Run TCP Client with authentication lifecycle and session management:
    1. Validate username/password (prompt interactively if omitted, or fail if empty).
    2. Connect to TCP server.
    3. Send HELLO -> Receive ACK (W1 Handshake compatibility).
    4. Send AUTH (username, password) -> Receive AUTH_OK / AUTH_FAIL.
    5. Maintain session state (LOGGED_IN) until 'quit' or Ctrl+C.
    6. Send BYE to gracefully terminate the session before closing socket.
    """
    # 0. Validate credentials: do not silently default to a dummy user
    if not username:
        if interactive:
            try:
                username = input("Enter username: ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                return False
        if not username:
            log.error("Username cannot be empty. Please specify a valid username.")
            return False

    if password is None:
        if interactive:
            try:
                password = input(f"Enter password for '{username}': ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                return False
        else:
            password = ""

    if host is None:
        host = "127.0.0.1" if SERVER_HOST == "0.0.0.0" else SERVER_HOST

    log.info("[%s] Connecting to TCP server at %s:%d...", username, host, port)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.connect((host, port))
            log.info("[%s] Connected successfully!", username)

            # 1. Initial Handshake (HELLO -> ACK)
            hello_msg = {"type": "HELLO", "client": username}
            log.info("[%s] → Sent: %s", username, hello_msg)
            send_json_frame(sock, hello_msg)

            ack_resp = recv_json_frame(sock)
            log.info("[%s] ← Received: %s", username, ack_resp)
            if not ack_resp or ack_resp.get("type") != "ACK":
                log.error("[%s] HELLO handshake failed. Closing connection.", username)
                return False

            # 2. Authentication (AUTH -> AUTH_OK / AUTH_FAIL)
            auth_msg = {"type": "AUTH", "username": username, "password": password}
            log.info("[%s] → Sending authentication request (AUTH)...", username)
            send_json_frame(sock, auth_msg)

            auth_resp = recv_json_frame(sock)
            log.info("[%s] ← Received authentication response: %s", username, auth_resp)

            if not auth_resp or auth_resp.get("type") != "AUTH_OK":
                error_msg = auth_resp.get("message", "Invalid credentials or account does not exist.") if auth_resp else "Connection dropped"
                log.error("[%s] Authentication FAILED: %s", username, error_msg)
                print("\n" + "!" * 55)
                print(f"  [X] LOGIN REJECTED FOR '{username}':")
                print(f"      Reason: {error_msg}")
                print("!" * 55 + "\n")
                return False

            # Logged in successfully
            active_users = auth_resp.get("users", [])
            log.info("[%s] Authentication SUCCESS! Session state: LOGGED_IN", username)
            print("\n" + "=" * 55)
            print(f"  WELCOME: {username.upper()}")
            print("  ACTIVE USERS ONLINE:")
            print(format_user_list(active_users))
            print("=" * 55)

            # 3. Session Maintenance Loop
            if duration > 0:
                # Timed hold mode (used for simulation / testing)
                log.info("[%s] Holding session active for %.1f seconds...", username, duration)
                time.sleep(duration)
            elif not interactive:
                # Headless background hold mode
                while True:
                    time.sleep(1)
            else:
                # Interactive CLI mode
                print("\n[Tip] Type 'list' to refresh active users.")
                print("[Tip] Type 'quit' or press Ctrl+C to log out.\n")

                while True:
                    try:
                        cmd = input(f"{username}> ").strip()
                    except (EOFError, KeyboardInterrupt):
                        print()
                        break

                    if not cmd:
                        continue
                    if cmd.lower() in ("quit", "exit", "logout"):
                        break
                    elif cmd.lower() in ("list", "users"):
                        send_json_frame(sock, {"type": "USER_LIST"})
                        list_resp = recv_json_frame(sock)
                        if list_resp and list_resp.get("type") == "USER_LIST":
                            print(format_user_list(list_resp.get("users", [])))
                        else:
                            print("No user list response received from server.")
                    else:
                        print(f"Unknown command: '{cmd}'. Type 'list' or 'quit'.")

            # 4. Graceful Disconnect (Send BYE)
            log.info("[%s] Sending graceful disconnect (BYE)...", username)
            bye_msg = {"type": "BYE", "username": username}
            try:
                send_json_frame(sock, bye_msg)
                bye_resp = recv_json_frame(sock)
                log.info("[%s] ← Received disconnect ACK: %s", username, bye_resp)
            except Exception:
                pass

            log.info("[%s] Session terminated gracefully. Goodbye!", username)
            return True

        except ConnectionRefusedError:
            log.error("[%s] Error: Connection refused at %s:%d. Is server.py running?", username, host, port)
            return False
        except KeyboardInterrupt:
            log.info("\n[%s] Received interrupt signal (Ctrl+C), closing session...", username)
            try:
                send_json_frame(sock, {"type": "BYE", "username": username})
            except Exception:
                pass
            return True
        except Exception as e:
            log.error("[%s] An error occurred during session: %s", username, e)
            return False
        finally:
            log.info("[%s] Socket closed.", username)


if __name__ == "__main__":
    # Command line: python client.py [username] [password] [host] [port]
    # If no arguments provided, run_client() will prompt interactively.
    uname = sys.argv[1].strip() if len(sys.argv) > 1 and sys.argv[1].strip() else None
    upass = sys.argv[2].strip() if len(sys.argv) > 2 else None
    target_host = sys.argv[3].strip() if len(sys.argv) > 3 else None
    target_port = int(sys.argv[4].strip()) if len(sys.argv) > 4 else TCP_PORT

    run_client(username=uname, password=upass, host=target_host, port=target_port)
