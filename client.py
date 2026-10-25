# client.py – Chat client (Week 3: UDP Messaging, Heartbeat & CLI Interface)
# Responsibilities:
#   1. TCP control channel: HELLO handshake -> AUTH -> USER_LIST -> BYE
#   2. UDP data channel:    send HEARTBEAT every HEARTBEAT_INTERVAL seconds (background thread)
#                           listen for incoming MSG / USER_LIST datagrams (background thread)
#   3. Interactive CLI:     /all <text>       – broadcast to everyone
#                           /to <user> <text> – unicast to a specific user
#                           /list             – request & display current user list
#                           /quit             – graceful logout

import socket
import sys
import logging
import threading
import time

from config import SERVER_HOST, TCP_PORT, UDP_PORT, HEARTBEAT_INTERVAL
from protocol import (
    send_json_frame,
    recv_json_frame,
    pack_msg,
    pack_heartbeat,
    parse_udp,
    SeqCounter,
    MSG,
    USER_LIST,
)

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
        if isinstance(u, dict):
            uname = u.get("username", "")
            uip   = u.get("ip", "")
            uport = str(u.get("port", ""))
        else:
            uname, uip, uport = str(u), "", ""
        lines.append(f"  | {uname:<18} | {uip:<15} | {uport:<6} |")
    lines.append("  +--------------------+-----------------+--------+")
    return "\n".join(lines)


def _names(users: list) -> set:
    """Usernames from a user list (UDP gives plain strings, TCP gives dicts)."""
    return {(u.get("username", "") if isinstance(u, dict) else str(u)) for u in users} - {""}


def _resolve_host(server_host: str) -> str:
    """Return a connectable host address (never bind-all 0.0.0.0)."""
    return "127.0.0.1" if server_host == "0.0.0.0" else server_host


# ---------------------------------------------------------------------------
# Background threads
# ---------------------------------------------------------------------------

def _heartbeat_loop(
    udp_sock: socket.socket,
    server_addr: tuple,
    username: str,
    seq: SeqCounter,
    stop_evt: threading.Event,
) -> None:
    """Send a HEARTBEAT datagram every HEARTBEAT_INTERVAL seconds until stopped."""
    log.info("[%s] Heartbeat thread started (interval=%.1fs, server=%s:%d)",
             username, HEARTBEAT_INTERVAL, *server_addr)
    while not stop_evt.is_set():
        try:
            data = pack_heartbeat(username, seq.next())
            udp_sock.sendto(data, server_addr)
        except ConnectionResetError:
            pass
        except OSError:
            break
        except Exception as exc:
            log.warning("[%s] Heartbeat error: %s", username, exc)
        stop_evt.wait(HEARTBEAT_INTERVAL)
    log.info("[%s] Heartbeat thread stopped.", username)


def _udp_listener(
    udp_sock: socket.socket,
    username: str,
    stop_evt: threading.Event,
    known: set,
    known_lock: threading.Lock,
    stats: dict = None,
) -> None:
    """Listen for incoming UDP datagrams and print them to the console."""
    log.info("[%s] UDP listener thread started.", username)
    udp_sock.settimeout(1.0)           # unblocks every second to check stop_evt
    while not stop_evt.is_set():
        try:
            raw, addr = udp_sock.recvfrom(4096)
        except socket.timeout:
            continue
        except ConnectionResetError:
            continue
        except OSError:
            break                      # socket closed externally

        try:
            pkt, body = parse_udp(raw)
        except ValueError as exc:
            log.debug("[%s] Ignored malformed datagram from %s: %s", username, addr, exc)
            continue

        if pkt.ptype == MSG:
            sender   = body.get("from", "?")
            receiver = body.get("to", "*")
            text     = body.get("text", "")
            ts       = time.strftime("%H:%M:%S", time.localtime(body.get("time", time.time())))
            if stats is not None:
                stats["received"] = stats.get("received", 0) + 1
            if receiver == "*":
                print(f"\n  [{ts}] [BROADCAST] {sender}: {text}")
            else:
                print(f"\n  [{ts}] [PRIVATE from {sender}]: {text}")
            print(f"{username}> ", end="", flush=True)

        elif pkt.ptype == USER_LIST:
            online = _names(body.get("users", []))
            with known_lock:
                joined = online - known - {username}
                left = known - online - {username}
                known.clear()
                known.update(online)
            if not (joined or left):
                continue                   # nothing changed, keep the screen quiet
            print()
            for name in sorted(joined):
                print(f"  [+] {name} is now online")
            for name in sorted(left):
                print(f"  [-] {name} went offline")
            print(f"  Online ({len(online)}): {', '.join(sorted(online))}")
            print(f"{username}> ", end="", flush=True)

    log.info("[%s] UDP listener thread stopped.", username)


# ---------------------------------------------------------------------------
# Main client function
# ---------------------------------------------------------------------------

def run_client(
    username: str = None,
    password: str = None,
    host: str = None,
    port: int = TCP_PORT,
    server_udp_port: int = UDP_PORT,   # the SERVER's UDP port (not this client's own port)
    duration: float = 0.0,
    interactive: bool = True,
    send_messages: list = None,   # list of (to, text) tuples for automated testing
    send_delay: float = 0.3,      # timed mode: seconds to wait after login before sending send_messages
    stats: dict = None,           # optional: filled with {"received": <number of MSG packets received>}
) -> bool:
    """
    Run the full chat client lifecycle:
      TCP: HELLO -> AUTH -> (session) -> BYE
      UDP: heartbeat sender + incoming MSG / USER_LIST listener
    """
    # ------------------------------------------------------------------
    # 0. Credential validation
    # ------------------------------------------------------------------
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
        host = _resolve_host(SERVER_HOST)

    log.info("[%s] Connecting to TCP server at %s:%d ...", username, host, port)

    # ------------------------------------------------------------------
    # 1. TCP connection + handshake
    # ------------------------------------------------------------------
    tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        tcp_sock.connect((host, port))
        log.info("[%s] TCP connected.", username)

        # HELLO -> ACK
        send_json_frame(tcp_sock, {"type": "HELLO", "client": username})
        ack = recv_json_frame(tcp_sock)
        if not ack or ack.get("type") != "ACK":
            log.error("[%s] HELLO handshake failed. Aborting.", username)
            tcp_sock.close()
            return False

        # AUTH -> AUTH_OK / AUTH_FAIL
        send_json_frame(tcp_sock, {"type": "AUTH", "username": username, "password": password})
        auth_resp = recv_json_frame(tcp_sock)

        if not auth_resp or auth_resp.get("type") != "AUTH_OK":
            err = (auth_resp.get("message", "invalid credentials") if auth_resp
                   else "connection dropped")
            log.error("[%s] Authentication FAILED: %s", username, err)
            print("\n" + "!" * 55)
            print(f"  [X] LOGIN REJECTED FOR '{username}':")
            print(f"      Reason: {err}")
            print("!" * 55 + "\n")
            tcp_sock.close()
            return False

        active_users = auth_resp.get("users", [])
        log.info("[%s] Authentication SUCCESS. Session: LOGGED_IN", username)
        print("\n" + "=" * 55)
        print(f"  WELCOME: {username.upper()}")
        print("  ACTIVE USERS ONLINE:")
        print(format_user_list(active_users))
        print("=" * 55)

    except ConnectionRefusedError:
        log.error("[%s] Connection refused at %s:%d. Is server.py running?", username, host, port)
        tcp_sock.close()
        return False
    except Exception as exc:
        log.error("[%s] TCP setup error: %s", username, exc)
        tcp_sock.close()
        return False

    # ------------------------------------------------------------------
    # 2. UDP socket setup
    # ------------------------------------------------------------------
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_sock.bind(("", 0))             # OS picks an ephemeral port
    udp_local_port = udp_sock.getsockname()[1]
    server_udp_addr = (host, server_udp_port)
    seq = SeqCounter()
    log.info("[%s] UDP socket bound to local port %d, server UDP at %s:%d",
             username, udp_local_port, host, server_udp_port)

    # ------------------------------------------------------------------
    # 3. Start background threads
    # ------------------------------------------------------------------
    stop_evt = threading.Event()
    known_users = _names(active_users)          # who is online right now (for join/leave notices)
    known_lock = threading.Lock()

    hb_thread = threading.Thread(
        target=_heartbeat_loop,
        args=(udp_sock, server_udp_addr, username, seq, stop_evt),
        daemon=True,
        name=f"heartbeat-{username}",
    )
    listener_thread = threading.Thread(
        target=_udp_listener,
        args=(udp_sock, username, stop_evt, known_users, known_lock, stats),
        daemon=True,
        name=f"listener-{username}",
    )
    # Register our UDP address with the server BEFORE any chat message can be sent.
    try:
        udp_sock.sendto(pack_heartbeat(username, seq.next()), server_udp_addr)
    except OSError as exc:
        log.warning("[%s] Initial heartbeat failed: %s", username, exc)
    hb_thread.start()
    listener_thread.start()

    # ------------------------------------------------------------------
    # 4. Session loop
    # ------------------------------------------------------------------
    try:
        if duration > 0:
            # Timed hold mode – simulation / automated tests
            log.info("[%s] Timed mode: holding session for %.1f seconds.", username, duration)
            if send_messages:
                # Wait so the server has registered our UDP address (and, in demos, everyone is online)
                time.sleep(send_delay)
                for to, text in send_messages:
                    try:
                        data = pack_msg(to, username, text, seq.next())
                        udp_sock.sendto(data, server_udp_addr)
                        label = "BROADCAST" if to == "*" else f"-> {to}"
                        log.info("[%s] Sent MSG (%s): %s", username, label, text)
                        time.sleep(0.1)
                    except Exception as exc:
                        log.warning("[%s] Failed to send message: %s", username, exc)
            time.sleep(duration)

        elif not interactive:
            # Headless background hold
            while not stop_evt.is_set():
                time.sleep(1)

        else:
            # ----------------------------------------------------------------
            # Interactive CLI
            # ----------------------------------------------------------------
            print("\n[Commands]")
            print("  /all <text>         - broadcast message to everyone")
            print("  /to <user> <text>   - send private message to <user>")
            print("  /list               - refresh the active user list")
            print("  /quit               - log out and exit\n")

            while True:
                try:
                    raw_cmd = input(f"{username}> ").strip()
                except (EOFError, KeyboardInterrupt):
                    print()
                    break

                if not raw_cmd:
                    continue

                if raw_cmd.lower() in ("/quit", "/exit", "/logout"):
                    break

                elif raw_cmd.lower() == "/list":
                    try:
                        send_json_frame(tcp_sock, {"type": "USER_LIST"})
                        resp = recv_json_frame(tcp_sock)
                        if resp and resp.get("type") == "USER_LIST":
                            with known_lock:
                                known_users.clear()
                                known_users.update(_names(resp.get("users", [])))
                            print(format_user_list(resp.get("users", [])))
                        else:
                            print("  No user list response from server.")
                    except Exception as exc:
                        log.warning("[%s] /list error: %s", username, exc)

                elif raw_cmd.lower() == "/all" or raw_cmd.lower().startswith("/all "):
                    # Broadcast needs the explicit /all command, so a typo can never message everyone.
                    text = raw_cmd[4:].strip()
                    if not text:
                        print("  Usage: /all <message>")
                        continue
                    try:
                        data = pack_msg("*", username, text, seq.next())
                        udp_sock.sendto(data, server_udp_addr)
                    except ValueError as exc:
                        print(f"  Error: {exc}")
                    except Exception as exc:
                        log.warning("[%s] Failed to send broadcast: %s", username, exc)

                elif raw_cmd.lower() == "/to" or raw_cmd.lower().startswith("/to "):
                    parts = raw_cmd[3:].strip().split(" ", 1)
                    if len(parts) < 2 or not parts[0].strip() or not parts[1].strip():
                        print("  Usage: /to <username> <message>")
                        continue
                    to_user, text = parts[0].strip(), parts[1].strip()
                    if to_user == username:
                        print("  You cannot send a private message to yourself.")
                        continue
                    try:
                        data = pack_msg(to_user, username, text, seq.next())
                        udp_sock.sendto(data, server_udp_addr)
                    except ValueError as exc:
                        print(f"  Error: {exc}")
                    except Exception as exc:
                        log.warning("[%s] Failed to send unicast: %s", username, exc)

                else:
                    print("  Unknown command. Use /all <text>, /to <user> <text>, /list, or /quit.")

    except KeyboardInterrupt:
        print()

    # ------------------------------------------------------------------
    # 5. Graceful shutdown
    # ------------------------------------------------------------------
    log.info("[%s] Logging out ...", username)
    stop_evt.set()
    try:
        udp_sock.close()
    except OSError:
        pass
    try:
        send_json_frame(tcp_sock, {"type": "BYE", "username": username})
        recv_json_frame(tcp_sock)
    except Exception:
        pass
    try:
        tcp_sock.close()
    except OSError:
        pass
    hb_thread.join(timeout=2.0)
    listener_thread.join(timeout=2.0)
    log.info("[%s] Session terminated. Goodbye!", username)
    return True


if __name__ == "__main__":
    # Usage: python client.py [username] [password] [host] [port]
    # If no arguments provided, run_client() will prompt interactively.
    uname       = sys.argv[1].strip() if len(sys.argv) > 1 and sys.argv[1].strip() else None
    upass       = sys.argv[2].strip() if len(sys.argv) > 2 else None
    target_host = sys.argv[3].strip() if len(sys.argv) > 3 else None
    target_port = int(sys.argv[4].strip()) if len(sys.argv) > 4 else TCP_PORT

    run_client(username=uname, password=upass, host=target_host, port=target_port)