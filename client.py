"""TCP/UDP client session API and the backwards-compatible command-line client."""

import argparse
from collections import OrderedDict, deque
from copy import deepcopy
from importlib import import_module
import logging
import os
from pathlib import Path
import queue
import socket
import threading
import time

from config import SERVER_HOST, TCP_PORT, UDP_PORT, HEARTBEAT_INTERVAL, CLIENT_TIMEOUT
from file_transfers import FileTransfers, loss_rate_from_env
from protocol import (ACK, FILE_CHUNK, FILE_META, MSG, USER_LIST, SeqCounter,
                      pack_heartbeat, pack_msg, parse_udp, recv_json_frame, send_json_frame)

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("client")
MESSAGE_LIMIT = 100
TRANSFER_LIMIT = 50


def format_user_list(users):
    if not users:
        return "  (No active users online)"
    border = "  +--------------------+-----------------+--------+"
    lines = [border, "  | Username           | IP Address      | Port   |", border]
    for user in users:
        name, ip, port = (user.get("username", ""), user.get("ip", ""), str(user.get("port", ""))) \
            if isinstance(user, dict) else (str(user), "", "")
        lines.append(f"  | {name:<18} | {ip:<15} | {port:<6} |")
    return "\n".join(lines + [border])


def _names(users):
    return {(u.get("username", "") if isinstance(u, dict) else str(u)) for u in users} - {""}


def _resolve_host(host):
    return "127.0.0.1" if host == "0.0.0.0" else host


class ClientSession:
    """A client independent of CLI/HTTP. See docs/client_api.md for schema v1.

    After start() authenticates, only the TCP reader consumes control frames.
    get_state() returns a detached JSON-safe snapshot. Event callbacks run on the
    publishing thread and should only enqueue/copy data, not perform blocking I/O.
    """

    def __init__(self, username, password, host=None, port=TCP_PORT,
                 server_udp_port=UDP_PORT, *, download_dir=None, simulate_loss=None,
                 loss_seed=None, stats=None, on_event=None, console=False):
        self.username, self._password = username, password
        self.host = _resolve_host(host if host is not None else SERVER_HOST)
        self.port, self.server_udp_port = port, server_udp_port
        self.download_dir = Path(download_dir) if download_dir is not None else Path("downloads") / str(username)
        self.loss_rate = loss_rate_from_env(os.environ.get("SIMULATE_LOSS", "0")
                                           if simulate_loss is None else simulate_loss)
        self.loss_seed, self.on_event, self.console = loss_seed, on_event, console
        self.stats = stats if stats is not None else {}
        self._state_lock = threading.RLock()
        self._lifecycle_lock = threading.RLock()
        self._send_lock = threading.Lock()
        self._request_lock = threading.Lock()
        self._users_reply = threading.Event()
        self._bye_reply = threading.Event()
        self._stop = threading.Event()
        self._stop.set()
        self._tcp = self._udp = self._transfers = None
        self._threads = []
        self._closing = False
        self._users_response = None
        self._last_control = 0.0
        self._seq = SeqCounter()
        self._server_addr = None
        self._user_details = []
        self._has_user_list = False
        self._messages = deque(maxlen=MESSAGE_LIMIT)
        self._message_ids = set()
        self._files = OrderedDict()
        self._state = {"schema_version": 1, "revision": 0, "username": username,
                       "status": {"state": "disconnected", "detail": None}, "users": []}

    @property
    def is_connected(self):
        with self._state_lock:
            return self._state["status"]["state"] == "connected" and not self._stop.is_set()

    def get_state(self):
        with self._state_lock:
            return deepcopy({**self._state, "messages": list(self._messages),
                             "transfers": list(self._files.values())})

    def _notify(self, kind, data):
        if self.on_event:
            try:
                self.on_event({"type": kind, "data": deepcopy(data)})
            except Exception:
                log.exception("[%s] Session event callback failed", self.username)

    def _set_status(self, state, detail=None):
        status = {"state": state, "detail": detail}
        with self._state_lock:
            if self._state["status"] == status:
                return
            self._state["status"] = status
            self._state["revision"] += 1
        self._notify("status", status)
        if state == "error":
            log.error("[%s] %s", self.username, detail)

    def _set_users(self, users):
        online = sorted(_names(users))
        with self._state_lock:
            old = set(self._state["users"])
            if online == self._state["users"]:
                return
            announce = self._has_user_list
            self._has_user_list = True
            self._state["users"] = online
            self._state["revision"] += 1
        left = old - set(online) - {self.username}
        transfers = self._transfers
        if transfers:
            for name in left:
                transfers.peer_left(name)
        self._notify("users", online)
        if self.console and announce:
            for name in sorted(set(online) - old - {self.username}):
                print(f"\n  [+] {name} is now online", flush=True)
            for name in sorted(left):
                print(f"\n  [-] {name} went offline", flush=True)

    def _message(self, packet, body, direction):
        message = {"id": str(packet.msg_id), "from": body["from"], "to": body["to"],
                   "text": body["text"], "time": body["time"], "direction": direction}
        with self._state_lock:
            if message["id"] in self._message_ids:
                return
            if len(self._messages) == MESSAGE_LIMIT:
                self._message_ids.discard(self._messages[0]["id"])
            self._messages.append(message)
            self._message_ids.add(message["id"])
            self._state["revision"] += 1
        self._notify("message", message)
        if direction == "incoming":
            self.stats["received"] = self.stats.get("received", 0) + 1
            if self._transfers and self._transfers.active_evt.is_set():
                self.stats["messages_during_file"] = self.stats.get("messages_during_file", 0) + 1
            if self.console:
                label = "BROADCAST" if message["to"] == "*" else "PRIVATE"
                print(f"\n  [{label}] {message['from']}: {message['text']}", flush=True)

    def _file_event(self, record):
        with self._state_lock:
            self._files[record["id"]] = deepcopy(record)
            self._files.move_to_end(record["id"])
            # Active sends/receives are bounded by FileTransfers; discard old results first.
            for mid in list(self._files):
                if len(self._files) <= TRANSFER_LIMIT:
                    break
                if self._files[mid]["status"] in ("completed", "failed"):
                    del self._files[mid]
            self._state["revision"] += 1
        self._notify("file", record)

    def _send_control(self, body):
        with self._send_lock:
            tcp = self._tcp
            if tcp is None:
                raise ConnectionError("Control connection is closed")
            send_json_frame(tcp, body)

    def start(self):
        with self._lifecycle_lock:
            if self.is_connected:
                return True
            if self._closing:
                return False
            if not isinstance(self.username, str) or not self.username.strip():
                self._set_status("error", "Username cannot be empty")
                return False
            self._stop.clear()
            self._bye_reply.clear()
            self._users_reply.clear()
            self._set_status("connecting")
            try:
                host = socket.gethostbyname(self.host)
                self._server_addr = (host, self.server_udp_port)
                # The authenticated TCP request declares the already-bound UDP endpoint.
                self._udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self._udp.bind(("", 0))
                self._udp.settimeout(0.5)
                udp_port = self._udp.getsockname()[1]
                self._tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self._tcp.settimeout(5.0)
                self._tcp.connect((host, self.port))
                self._send_control({"type": "HELLO", "client": self.username})
                hello = recv_json_frame(self._tcp)
                if not isinstance(hello, dict) or hello.get("type") != "ACK":
                    raise ConnectionError("HELLO handshake failed")
                self._send_control({"type": "AUTH", "username": self.username,
                                    "password": self._password, "udp_port": udp_port})
                response = recv_json_frame(self._tcp)
                if not isinstance(response, dict) or response.get("type") != "AUTH_OK":
                    raise ConnectionError(response.get("message", "Authentication failed")
                                          if isinstance(response, dict) else "Authentication connection closed")
                self._tcp.settimeout(None)
                self._user_details = deepcopy(response.get("users", []))
                self._last_control = time.monotonic()
                self._transfers = FileTransfers(
                    self.username, self._send_udp, self._seq, self.download_dir,
                    loss_rate=self.loss_rate, stats=self.stats, seed=self.loss_seed,
                    on_event=self._file_event, quiet=not self.console)
                self._set_users(self._user_details)
                self._set_status("connected")
                self._threads = [
                    threading.Thread(target=self._tcp_reader, name=f"control-{self.username}", daemon=True),
                    threading.Thread(target=self._udp_reader, name=f"listener-{self.username}", daemon=True),
                    threading.Thread(target=self._heartbeat, name=f"heartbeat-{self.username}", daemon=True),
                ]
                for thread in self._threads:
                    thread.start()
                return True
            except Exception as exc:
                # Keep transport failures visible to API consumers and release partial setup.
                detail = str(exc) or type(exc).__name__
        self.close(graceful=False, error=detail)
        return False

    def _send_udp(self, raw):
        udp = self._udp
        if udp is None:
            raise ConnectionError("UDP socket is closed")
        udp.sendto(raw, self._server_addr)

    def _tcp_reader(self):
        try:
            while not self._stop.is_set():
                body = recv_json_frame(self._tcp)
                if body is None:
                    raise ConnectionError("Server closed the TCP connection")
                if not isinstance(body, dict):
                    raise ValueError("Invalid control response")
                self._last_control = time.monotonic()
                if body.get("type") == "USER_LIST":
                    users = body.get("users", [])
                    self._user_details = deepcopy(users)
                    self._set_users(users)
                    self._users_response = deepcopy(users)
                    self._users_reply.set()
                elif body.get("type") == "ACK" and body.get("status") == "BYE":
                    self._bye_reply.set()
        except Exception as exc:
            if not self._stop.is_set():
                self.close(graceful=False, error=f"Control channel: {type(exc).__name__}: {exc}")
        finally:
            self._users_reply.set()

    def _udp_reader(self):
        while not self._stop.is_set():
            try:
                raw, address = self._udp.recvfrom(4096)
            except socket.timeout:
                continue
            except ConnectionResetError:
                continue  # Windows may report a previous UDP send failure here
            except OSError as exc:
                if not self._stop.is_set():
                    self.close(graceful=False, error=f"UDP channel: {exc}")
                return
            if address != self._server_addr:
                continue
            try:
                packet, body = parse_udp(raw)
                if packet.ptype == MSG:
                    if (not isinstance(body.get("from"), str) or not isinstance(body.get("to"), str)
                            or not isinstance(body.get("text"), str) or not isinstance(body.get("time"), (int, float))):
                        continue
                    self._message(packet, body, "incoming")
                elif packet.ptype == USER_LIST:
                    if isinstance(body.get("users"), list):
                        self._set_users(body["users"])
                elif packet.ptype in (FILE_META, FILE_CHUNK, ACK):
                    if packet.ptype != ACK:
                        self.stats["file_packets_received"] = self.stats.get("file_packets_received", 0) + 1
                    self._transfers.on_packet(packet, body)
            except (ValueError, TypeError, KeyError):
                log.debug("[%s] Ignored malformed UDP packet", self.username)

    def _heartbeat(self):
        while not self._stop.is_set():
            try:
                if time.monotonic() - self._last_control >= CLIENT_TIMEOUT:
                    raise ConnectionError("Server did not respond to TCP PING")
                self._send_udp(pack_heartbeat(self.username, self._seq.next()))
                # PONG/EOF is handled only by the control reader, never this thread.
                self._send_control({"type": "PING"})
            except OSError as exc:
                if not self._stop.is_set():
                    self.close(graceful=False, error=f"Server connection: {exc}")
                return
            self._stop.wait(HEARTBEAT_INTERVAL)

    def send_chat(self, to, text):
        if not self.is_connected:
            raise ConnectionError("Client is not connected")
        if not isinstance(to, str) or not to.strip() or to == self.username:
            raise ValueError("Choose '*' or another username")
        if not isinstance(text, str):
            raise ValueError("Message must be text")
        raw = pack_msg(to, self.username, text, self._seq.next())
        packet, body = parse_udp(raw)
        self._send_udp(raw)
        self._message(packet, body, "outgoing")
        return str(packet.msg_id)

    def send_file(self, to, path):
        transfers = self._transfers
        if not self.is_connected or transfers is None:
            raise ConnectionError("Client is not connected")
        return transfers.send_file(to, path)

    def refresh_users(self, timeout=5.0):
        """CLI compatibility; HTTP polling should use get_state() without TCP I/O."""
        with self._request_lock:
            if not self.is_connected:
                raise ConnectionError("Client is not connected")
            self._users_response = None
            self._users_reply.clear()
            self._send_control({"type": "USER_LIST"})
            if not self._users_reply.wait(timeout):
                raise TimeoutError("No user list response")
            if self._users_response is None:
                raise ConnectionError("Control connection closed while refreshing users")
            return deepcopy(self._users_response)

    def close(self, *, graceful=True, error=None):
        with self._lifecycle_lock:
            if self._closing:
                return
            if self._tcp is None and self._udp is None and self._transfers is None:
                self._stop.set()
                if error:
                    self._set_status("error", error)
                return
            self._closing = True
            connected = self.is_connected
            self._stop.set()
            self._set_status("error" if error else "closing", error)
            tcp, udp, transfers = self._tcp, self._udp, self._transfers
            threads = list(self._threads)
        try:
            if graceful and connected and tcp:
                try:
                    self._send_control({"type": "BYE", "username": self.username})
                    self._bye_reply.wait(0.3)
                except OSError:
                    pass
            if tcp:
                try:
                    tcp.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                tcp.close()
            if transfers:
                transfers.close()
            if udp:
                udp.close()
            for thread in threads:
                if thread is not threading.current_thread():
                    thread.join(timeout=1.0)
        finally:
            with self._lifecycle_lock:
                self._tcp = self._udp = self._transfers = None
                self._threads.clear()
                self._closing = False
            self._set_users([])
            self._users_reply.set()
            self._set_status("error" if error else "disconnected", error)

    def wait(self, timeout=None):
        """True when this session has stopped, useful for scripts and gateways."""
        return self._stop.wait(timeout)


def _credentials(username, password, interactive):
    if not username and interactive:
        username = input("Enter username: ").strip()
    if not username:
        raise ValueError("Username cannot be empty")
    if password is None:
        password = input(f"Enter password for '{username}': ").strip() if interactive else ""
    return username, password


def _interactive(session):
    print("\n[Commands] /all <text> | /to <user> <text> | /send <user> <file> | /list | /quit")
    while session.is_connected:
        try:
            command = input(f"{session.username}> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return
        if not command:
            continue
        if command.lower() in ("/quit", "/exit", "/logout"):
            return
        try:
            verb, _, rest = command.partition(" ")
            if verb.lower() == "/list":
                print(format_user_list(session.refresh_users()))
            elif verb.lower() == "/all":
                session.send_chat("*", rest.strip())
            elif verb.lower() in ("/to", "/send"):
                parts = rest.strip().split(maxsplit=1)
                if len(parts) != 2:
                    raise ValueError(f"Usage: {verb} <username> <{'message' if verb.lower() == '/to' else 'file path'}>")
                if verb.lower() == "/to":
                    session.send_chat(parts[0], parts[1])
                else:
                    session.send_file(parts[0], parts[1].strip().strip('"'))
            else:
                print("  Unknown command. Use /all, /to, /send, /list, or /quit.")
        except (OSError, ValueError, RuntimeError, queue.Full) as exc:
            print(f"  Error: {exc}")


def _run_client_impl(username=None, password=None, host=None, port=TCP_PORT,
                     server_udp_port=UDP_PORT, duration=0.0, interactive=True,
                     send_messages=None, send_delay=0.3, stats=None, send_files=None,
                     download_dir=None, simulate_loss=None, loss_seed=None, ready_evt=None,
                     session_stop=None, graceful_shutdown=True, web_port=None):
    session = gateway = None
    try:
        username, password = _credentials(username, password, interactive)
        gateway_factory = None
        if web_port is not None:
            try:
                gateway_factory = import_module("web_gateway").start_gateway
            except (ImportError, AttributeError) as exc:
                raise RuntimeError("Gateway is unavailable. Binh's web_gateway.py must expose "
                                   "start_gateway(session, host='127.0.0.1', port=...). "
                                   "Run without --web-port for the CLI.") from exc
        session = ClientSession(username, password, host, port, server_udp_port,
                                download_dir=download_dir, simulate_loss=simulate_loss,
                                loss_seed=loss_seed, stats=stats, console=True)
        if not session.start():
            return False
        if gateway_factory:
            gateway = gateway_factory(session, host="127.0.0.1", port=web_port)
            if not callable(getattr(gateway, "close", None)):
                raise RuntimeError("start_gateway() must return a handle with close()")
        print(f"\n  WELCOME: {username.upper()}\n  ACTIVE USERS ONLINE:")
        print(format_user_list(session._user_details))
        if ready_evt is not None:
            ready_evt.set()
        if duration > 0:
            session.wait(send_delay if send_messages or send_files else 0)
            for to, path in send_files or []:
                if not session.is_connected:
                    break
                session.send_file(to, path)
            for to, text in send_messages or []:
                if not session.is_connected or (session_stop and session_stop.is_set()):
                    break
                session.send_chat(to, text)
                session.wait(0.1)
            deadline = time.monotonic() + duration
            while session.is_connected and time.monotonic() < deadline:
                if session_stop and session_stop.is_set():
                    break
                session.wait(0.1)
        elif not interactive:
            while session.is_connected:
                if session_stop and session_stop.is_set():
                    break
                session.wait(0.1)
        else:
            _interactive(session)
        return session.get_state()["status"]["state"] != "error"
    except (EOFError, KeyboardInterrupt):
        print()
        return False
    except (OSError, ValueError, RuntimeError, queue.Full) as exc:
        log.error("Client: %s", exc)
        return False
    finally:
        if gateway:
            try:
                gateway.close()
            except Exception:
                log.exception("Gateway close failed")
        if session:
            session.close(graceful=graceful_shutdown)


def run_client(username: str = None, password: str = None, host: str = None, port: int = TCP_PORT,
               server_udp_port: int = UDP_PORT, duration: float = 0.0, interactive: bool = True,
               send_messages: list = None, send_delay: float = 0.3, stats: dict = None, send_files: list = None,
               download_dir=None, simulate_loss=None, loss_seed=None, ready_evt=None,
               session_stop=None, graceful_shutdown=True) -> bool:
    """Week 4 public signature retained; CLI and simulations use ClientSession."""
    return _run_client_impl(username, password, host, port, server_udp_port, duration,
                            interactive, send_messages, send_delay, stats, send_files,
                            download_dir, simulate_loss, loss_seed, ready_evt,
                            session_stop, graceful_shutdown)


def parse_cli_args(argv=None):
    def port_number(value):
        number = int(value)
        if not 1 <= number <= 65535:
            raise argparse.ArgumentTypeError("Port must be in 1..65535")
        return number

    parser = argparse.ArgumentParser(description="TCP/UDP collaboration client")
    parser.add_argument("username", nargs="?")
    parser.add_argument("password", nargs="?")
    parser.add_argument("host", nargs="?")
    parser.add_argument("port", nargs="?", type=port_number, default=TCP_PORT)
    parser.add_argument("--udp-port", type=port_number, default=UDP_PORT)
    parser.add_argument("--web-port", type=port_number, help="Start Binh's local HTTP gateway on this port")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_cli_args()
    raise SystemExit(0 if _run_client_impl(
        username=args.username, password=args.password, host=args.host, port=args.port,
        server_udp_port=args.udp_port, web_port=args.web_port) else 1)
