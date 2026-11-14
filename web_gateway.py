"""Raw-socket HTTP gateway for ClientSession with login, transfers, and SSE."""

from __future__ import annotations

import base64
import ipaddress
import json
import logging
import mimetypes
import os
from pathlib import Path
import socket
import threading
import time
from urllib.parse import parse_qs, unquote
from typing import Any, Optional, Protocol

from config import SERVER_HOST, TCP_PORT, UDP_PORT

MAX_HEADER_BYTES = 16384
MAX_URI_LEN = 2048
MAX_STATIC_BYTES = 2 * 1024 * 1024
MAX_CONNECTIONS = 64
MAX_UPLOAD_BYTES = 50 * 1024 * 1024

_REPO_ROOT = Path(__file__).resolve().parent
_UPLOADS_ROOT = _REPO_ROOT / "uploads"
_DOWNLOADS_ROOT = _REPO_ROOT / "downloads"
_ALLOWED_EXTENSIONS = {
    ".html", ".htm", ".css", ".js", ".mjs", ".json", ".png", ".jpg",
    ".jpeg", ".gif", ".svg", ".ico", ".txt", ".woff2",
}
_REASONS = {
    200: "OK",
    202: "Accepted",
    400: "Bad Request",
    401: "Unauthorized",
    404: "Not Found",
    405: "Method Not Allowed",
    413: "Payload Too Large",
    414: "URI Too Long",
    431: "Request Header Fields Too Large",
    500: "Internal Server Error",
    503: "Service Unavailable",
}
_TOKEN_CHARS = frozenset(
    "!#$%&'*+-.^_`|~0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)
_LOG = logging.getLogger(__name__)


class SessionAPI(Protocol):
    """Operations used by the gateway; the session retains transport ownership."""

    def get_state(self) -> dict[str, Any]:
        """Return a detached, JSON-compatible session snapshot."""
        ...

    def send_chat(self, to: str, text: str) -> str:
        """Send a chat message and return its identifier."""
        ...


class _HTTPError(Exception):
    """A request error that can safely be returned to the browser."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status


def _reject_json_constant(token: str) -> Any:
    raise ValueError(f"Invalid JSON constant: {token}")


def _json_bytes(value: Any) -> bytes:
    """Serialize JSON as UTF-8 without permitting nonstandard numeric values."""
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False, separators=(",", ":")
    ).encode("utf-8")


def _shutdown_socket(sock: socket.socket) -> None:
    """Release a socket, tolerating concurrent shutdown and close operations."""
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    try:
        sock.close()
    except OSError:
        pass


def _validate_host(host: str) -> str:
    """Validate a loopback hostname and return an IPv4 bind address."""
    if not isinstance(host, str) or not host:
        raise ValueError("host must be a non-empty loopback hostname or IPv4 address")
    if host == "localhost":
        return "127.0.0.1"
    try:
        address = ipaddress.IPv4Address(host)
    except ipaddress.AddressValueError as exc:
        raise ValueError("host must be localhost or a loopback IPv4 address") from exc
    if not address.is_loopback:
        raise ValueError("host must be a loopback IPv4 address")
    return str(address)


def _resolve_static_root(static_dir: Optional[str | Path]) -> Optional[Path]:
    """Resolve a static directory without allowing the repository as its root."""
    if static_dir is not None:
        root = Path(static_dir).resolve()
        if root == _REPO_ROOT or root in _REPO_ROOT.parents:
            raise ValueError("static_dir must not expose the repository root")
        return root

    for name in ("static", "ui"):
        candidate = _REPO_ROOT / name
        if candidate.is_dir():
            root = candidate.resolve()
            if root == _REPO_ROOT or root in _REPO_ROOT.parents:
                continue
            return root
    return None


def _check_uri_length(data: bytes) -> None:
    """Detect oversized targets even before the complete header block arrives."""
    first_line = data.split(b"\r\n", 1)[0]
    parts = first_line.split(b" ", 2)
    if len(parts) >= 2 and len(parts[1]) > MAX_URI_LEN:
        raise _HTTPError(414, "Request URI too long")


def _read_request(
    conn: socket.socket, max_body_bytes: int
) -> tuple[str, str, dict[str, str], bytes]:
    """Read one bounded HTTP request, retaining body bytes received with headers."""
    data = bytearray()
    while True:
        boundary = data.find(b"\r\n\r\n")
        _check_uri_length(bytes(data))
        if boundary >= 0:
            if boundary + 4 > MAX_HEADER_BYTES:
                raise _HTTPError(431, "Request headers too large")
            break
        if len(data) >= MAX_HEADER_BYTES:
            raise _HTTPError(431, "Request headers too large")
        chunk = conn.recv(min(4096, MAX_HEADER_BYTES - len(data)))
        if not chunk:
            raise _HTTPError(400, "Unexpected EOF while reading headers")
        data.extend(chunk)

    lines = bytes(data[:boundary]).split(b"\r\n")
    try:
        request_line = lines[0].decode("ascii")
    except UnicodeDecodeError as exc:
        raise _HTTPError(400, "Invalid request line") from exc

    parts = request_line.split(" ")
    if len(parts) != 3:
        raise _HTTPError(400, "Invalid request line")
    method, target, version = parts
    if (
        not method
        or any(char not in _TOKEN_CHARS for char in method)
        or not target.startswith("/")
        or target.startswith("//")
        or any(ord(char) <= 32 or ord(char) == 127 for char in target)
        or version not in ("HTTP/1.0", "HTTP/1.1")
    ):
        raise _HTTPError(400, "Invalid request line")

    path = target.split("?", 1)[0]
    headers: dict[str, str] = {"x-raw-uri": target}
    lengths: list[str] = []
    for line in lines[1:]:
        if not line or line[:1] in (b" ", b"\t") or b":" not in line:
            raise _HTTPError(400, "Malformed header")
        raw_name, raw_value = line.split(b":", 1)
        try:
            name = raw_name.decode("ascii")
        except UnicodeDecodeError as exc:
            raise _HTTPError(400, "Invalid header name") from exc
        if not name or any(char not in _TOKEN_CHARS for char in name):
            raise _HTTPError(400, "Invalid header name")
        if any(byte < 32 and byte != 9 or byte == 127 for byte in raw_value):
            raise _HTTPError(400, "Invalid header value")

        name = name.lower()
        value = raw_value.decode("iso-8859-1").strip(" \t")
        if name == "content-length":
            if not value or any(char not in "0123456789" for char in value):
                raise _HTTPError(400, "Invalid Content-Length")
            lengths.append(value.lstrip("0") or "0")
        elif name in headers:
            if name == "content-type" and headers[name] != value:
                raise _HTTPError(400, "Conflicting Content-Type headers")
            if name != "content-type":
                headers[name] += "," + value
        else:
            headers[name] = value

    if "transfer-encoding" in headers:
        raise _HTTPError(400, "Transfer-Encoding is not supported")
    if lengths and any(value != lengths[0] for value in lengths):
        raise _HTTPError(400, "Conflicting Content-Length headers")

    normalized_length = lengths[0] if lengths else "0"
    effective_max = MAX_UPLOAD_BYTES if path == "/api/file/upload" else max_body_bytes
    maximum = str(effective_max)
    if (
        len(normalized_length) > len(maximum)
        or len(normalized_length) == len(maximum)
        and normalized_length > maximum
    ):
        raise _HTTPError(413, "Request body too large")
    content_length = int(normalized_length)
    if lengths:
        headers["content-length"] = normalized_length

    body = bytearray(data[boundary + 4:boundary + 4 + content_length])
    while len(body) < content_length:
        chunk = conn.recv(min(4096, content_length - len(body)))
        if not chunk:
            raise _HTTPError(400, "Unexpected EOF while reading body")
        body.extend(chunk)

    return method, path, headers, bytes(body)


class WebGateway:
    """Background HTTP server sharing an existing client session or managing one dynamically."""

    def __init__(
        self,
        session: Optional[SessionAPI] = None,
        *,
        host: str = "127.0.0.1",
        port: int = 8080,
        static_dir: Optional[str | Path] = None,
        max_body_bytes: int = 16384,
        server_host: str = SERVER_HOST,
        server_port: int = TCP_PORT,
        server_udp_port: int = UDP_PORT,
    ) -> None:
        if session is not None:
            if not callable(getattr(session, "get_state", None)):
                raise TypeError("session must expose callable get_state()")
            if not callable(getattr(session, "send_chat", None)):
                raise TypeError("session must expose callable send_chat()")
        bind_host = _validate_host(host)
        if isinstance(port, bool) or not isinstance(port, int) or not 0 <= port <= 65535:
            raise ValueError("port must be an integer in 0..65535")
        if (
            isinstance(max_body_bytes, bool)
            or not isinstance(max_body_bytes, int)
            or max_body_bytes <= 0
        ):
            raise ValueError("max_body_bytes must be a positive integer")

        self._static_root = _resolve_static_root(static_dir)
        self._session: Optional[SessionAPI] = None
        self._internal_session_managed = False
        self.server_host = server_host
        self.server_port = server_port
        self.server_udp_port = server_udp_port
        self._max_body_bytes = max_body_bytes
        self._lock = threading.Lock()
        self._stopped = threading.Event()
        self._active_conns: set[socket.socket] = set()
        self._workers: set[threading.Thread] = set()
        self._accept_thread: Optional[threading.Thread] = None
        self._sse_clients: set[socket.socket] = set()
        self._sse_lock = threading.Lock()
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        if session is not None:
            self._attach_session(session, managed=False)

        try:
            if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
                self._listener.setsockopt(
                    socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1
                )
            else:
                self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._listener.bind((bind_host, port))
            self._listener.listen(64)
            self._listener.settimeout(0.5)
            self.host = host
            self.port = int(self._listener.getsockname()[1])
            self.url = f"http://{self.host}:{self.port}"
            self._accept_thread = threading.Thread(
                target=self._accept_loop,
                name=f"web-gateway-{self.port}",
                daemon=True,
            )
            self._accept_thread.start()
        except BaseException:
            self._stopped.set()
            _shutdown_socket(self._listener)
            raise

    def _attach_session(self, session: SessionAPI, managed: bool = False) -> None:
        """Attach a session and wrap its on_event callback to broadcast SSE."""
        self._session = session
        self._internal_session_managed = managed
        orig_on_event = getattr(session, "on_event", None)

        def _on_event(event: dict[str, Any]) -> None:
            if orig_on_event:
                try:
                    orig_on_event(event)
                except Exception:
                    pass
            self._broadcast_sse({"type": "event", "event": event, "state": self._get_safe_state()})

        try:
            session.on_event = _on_event
        except Exception:
            pass

    def _get_safe_state(self) -> dict[str, Any]:
        """Return the current session state or a disconnected default."""
        if self._session is not None:
            try:
                return self._session.get_state()
            except Exception as exc:
                return {
                    "schema_version": 1,
                    "revision": 0,
                    "username": getattr(self._session, "username", None),
                    "status": {"state": "error", "detail": str(exc)},
                    "users": [],
                    "messages": [],
                    "transfers": [],
                }
        return {
            "schema_version": 1,
            "revision": 0,
            "username": None,
            "status": {"state": "disconnected", "detail": "Not logged in"},
            "users": [],
            "messages": [],
            "transfers": [],
        }

    def _broadcast_sse(self, data: dict[str, Any]) -> None:
        """Send an SSE event payload to all currently connected SSE clients."""
        payload = b"data: " + _json_bytes(data) + b"\n\n"
        with self._sse_lock:
            dead: list[socket.socket] = []
            for client_sock in tuple(self._sse_clients):
                try:
                    client_sock.sendall(payload)
                except OSError:
                    dead.append(client_sock)
            for d in dead:
                self._sse_clients.discard(d)
                _shutdown_socket(d)

    @property
    def is_running(self) -> bool:
        """Return whether the background server has not been stopped."""
        return not self._stopped.is_set()

    def close(self) -> None:
        """Idempotently stop serving, close sockets, and briefly join workers."""
        with self._lock:
            if self._stopped.is_set():
                return
            self._stopped.set()
            connections = tuple(self._active_conns)
            workers = tuple(self._workers)
            accept_thread = self._accept_thread
            with self._sse_lock:
                sse_conns = tuple(self._sse_clients)
                self._sse_clients.clear()

        for s in sse_conns:
            _shutdown_socket(s)
        _shutdown_socket(self._listener)
        for conn in connections:
            _shutdown_socket(conn)

        current = threading.current_thread()
        if accept_thread is not None and accept_thread is not current:
            accept_thread.join(timeout=1.0)
        timeout = 1.0 / max(1, len(workers))
        for worker in workers:
            if worker is not current:
                worker.join(timeout=timeout)

        if self._internal_session_managed and self._session is not None:
            try:
                close_fn = getattr(self._session, "close", None)
                if callable(close_fn):
                    close_fn(graceful=True)
            except Exception:
                pass
            self._session = None

    def _accept_loop(self) -> None:
        """Accept connections and register handlers atomically with shutdown."""
        try:
            while not self._stopped.is_set():
                try:
                    conn, _ = self._listener.accept()
                except socket.timeout:
                    continue
                except OSError:
                    if not self._stopped.is_set():
                        _LOG.exception("Gateway accept failed")
                    break

                try:
                    conn.settimeout(5.0)
                    with self._lock:
                        if (
                            self._stopped.is_set()
                            or len(self._active_conns) >= MAX_CONNECTIONS
                        ):
                            _shutdown_socket(conn)
                            continue
                        worker = threading.Thread(
                            target=self._handle_connection,
                            args=(conn,),
                            name=f"web-request-{self.port}",
                            daemon=True,
                        )
                        self._active_conns.add(conn)
                        self._workers.add(worker)
                        try:
                            worker.start()
                        except BaseException:
                            self._active_conns.discard(conn)
                            self._workers.discard(worker)
                            raise
                except Exception:
                    _shutdown_socket(conn)
                    _LOG.exception("Gateway could not start a request handler")
        finally:
            self.close()

    def _send_response(
        self,
        conn: socket.socket,
        status: int,
        body: bytes,
        content_type: str = "application/json; charset=utf-8",
        allow: Optional[str] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> None:
        """Send a complete response with explicit framing and security headers."""
        lines = [
            f"HTTP/1.1 {status} {_REASONS.get(status, 'Status')}",
            f"Content-Length: {len(body)}",
            f"Content-Type: {content_type}",
            "Connection: close",
            "X-Content-Type-Options: nosniff",
            "Cache-Control: no-store",
        ]
        if allow is not None:
            lines.append(f"Allow: {allow}")
        if headers:
            for k, v in headers.items():
                lines.append(f"{k}: {v}")
        conn.sendall(("\r\n".join(lines) + "\r\n\r\n").encode("ascii") + body)

    def _send_json(
        self,
        conn: socket.socket,
        status: int,
        value: Any,
        allow: Optional[str] = None,
    ) -> None:
        """Encode JSON before sending any response bytes."""
        self._send_response(conn, status, _json_bytes(value), allow=allow)

    def _finish_connection(self, conn: socket.socket) -> None:
        """Half-close and non-blockingly drain buffered bytes to avoid TCP RST."""
        try:
            conn.shutdown(socket.SHUT_WR)
        except OSError:
            return
        try:
            conn.settimeout(0.0)
            remaining = 65536
            while remaining > 0:
                chunk = conn.recv(min(4096, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
        except OSError:
            pass

    def _handle_connection(self, conn: socket.socket) -> None:
        """Handle a single request and always release its socket registration."""
        is_sse = False
        try:
            try:
                method, path, headers, body = _read_request(
                    conn, self._max_body_bytes
                )
                if path == "/api/events":
                    is_sse = True
                self._dispatch(conn, method, path, headers, body)
            except _HTTPError as exc:
                self._send_json(conn, exc.status, {"error": str(exc)})
            except socket.timeout:
                self._send_json(conn, 400, {"error": "Request timed out"})
            except OSError:
                pass
            except Exception:
                _LOG.exception("Gateway request failed")
                self._send_json(conn, 500, {"error": "Internal server error"})
            if not is_sse:
                self._finish_connection(conn)
        except OSError:
            pass
        finally:
            _shutdown_socket(conn)
            with self._lock:
                self._active_conns.discard(conn)
                self._workers.discard(threading.current_thread())

    def _dispatch(
        self,
        conn: socket.socket,
        method: str,
        path: str,
        headers: dict[str, str],
        body: bytes,
    ) -> None:
        """Route API requests or serve a validated static target."""
        if path == "/api/state":
            if method != "GET":
                self._method_not_allowed(conn, "GET")
                return
            if self._session is None:
                self._send_json(conn, 200, self._get_safe_state())
                return
            try:
                state = self._session.get_state()
            except (OSError, RuntimeError) as exc:
                self._send_json(conn, 503, {"error": str(exc)})
                return
            self._send_json(conn, 200, state)
            return

        if path == "/api/login":
            if method != "POST":
                self._method_not_allowed(conn, "POST")
                return
            content_type = headers.get("content-type")
            if (
                content_type is not None
                and content_type.split(";", 1)[0].strip().lower()
                != "application/json"
            ):
                raise _HTTPError(400, "Content-Type must be application/json")
            try:
                payload = json.loads(
                    body.decode("utf-8"),
                    parse_constant=_reject_json_constant,
                )
            except (UnicodeDecodeError, ValueError, RecursionError) as exc:
                raise _HTTPError(400, "Invalid JSON body") from exc
            if not isinstance(payload, dict):
                raise _HTTPError(400, "JSON body must be an object")
            username = payload.get("username")
            password = payload.get("password")
            if not isinstance(username, str) or not username.strip():
                raise _HTTPError(400, "'username' must be a non-empty string")
            if not isinstance(password, str):
                raise _HTTPError(400, "'password' must be a string")

            if self._session is not None:
                try:
                    close_fn = getattr(self._session, "close", None)
                    if callable(close_fn):
                        close_fn(graceful=True)
                except Exception:
                    pass
                self._session = None

            from client import ClientSession
            new_session = ClientSession(
                username.strip(),
                password,
                host=self.server_host,
                port=self.server_port,
                server_udp_port=self.server_udp_port,
                console=False,
            )
            self._attach_session(new_session, managed=True)
            if not new_session.start():
                err_msg = (
                    new_session.get_state().get("status", {}).get("detail")
                    or "Authentication failed"
                )
                self._session = None
                self._send_json(conn, 401, {"error": err_msg})
                return
            state = self._get_safe_state()
            self._broadcast_sse({"type": "login", "state": state})
            self._send_json(conn, 200, {"status": "ok", "state": state})
            return

        if path == "/api/logout":
            if method != "POST":
                self._method_not_allowed(conn, "POST")
                return
            if self._session is not None:
                try:
                    close_fn = getattr(self._session, "close", None)
                    if callable(close_fn):
                        close_fn(graceful=True)
                except Exception:
                    pass
                self._session = None
            state = self._get_safe_state()
            self._broadcast_sse({"type": "logout", "state": state})
            self._send_json(conn, 200, {"status": "ok", "state": state})
            return

        if path == "/api/chat":
            if method != "POST":
                self._method_not_allowed(conn, "POST")
                return
            if self._session is None:
                self._send_json(conn, 401, {"error": "Not logged in"})
                return
            content_type = headers.get("content-type")
            if (
                content_type is not None
                and content_type.split(";", 1)[0].strip().lower()
                != "application/json"
            ):
                raise _HTTPError(400, "Content-Type must be application/json")
            try:
                payload = json.loads(
                    body.decode("utf-8"),
                    parse_constant=_reject_json_constant,
                )
            except (UnicodeDecodeError, ValueError, RecursionError) as exc:
                raise _HTTPError(400, "Invalid JSON body") from exc
            if not isinstance(payload, dict):
                raise _HTTPError(400, "JSON body must be an object")
            recipient = payload.get("to")
            text = payload.get("text")
            if not isinstance(recipient, str) or not recipient.strip():
                raise _HTTPError(400, "'to' must be a non-empty string")
            if not isinstance(text, str):
                raise _HTTPError(400, "'text' must be a string")
            try:
                message_id = self._session.send_chat(recipient, text)
            except ValueError as exc:
                self._send_json(conn, 400, {"error": str(exc)})
                return
            except (OSError, RuntimeError) as exc:
                self._send_json(conn, 503, {"error": str(exc)})
                return
            self._broadcast_sse({"type": "message", "state": self._get_safe_state()})
            self._send_json(conn, 202, {"message_id": message_id})
            return

        if path == "/api/file/send":
            if method != "POST":
                self._method_not_allowed(conn, "POST")
                return
            if self._session is None:
                self._send_json(conn, 401, {"error": "Not logged in"})
                return
            content_type = headers.get("content-type")
            if (
                content_type is not None
                and content_type.split(";", 1)[0].strip().lower()
                != "application/json"
            ):
                raise _HTTPError(400, "Content-Type must be application/json")
            try:
                payload = json.loads(
                    body.decode("utf-8"),
                    parse_constant=_reject_json_constant,
                )
            except (UnicodeDecodeError, ValueError, RecursionError) as exc:
                raise _HTTPError(400, "Invalid JSON body") from exc
            if not isinstance(payload, dict):
                raise _HTTPError(400, "JSON body must be an object")
            recipient = payload.get("to")
            file_path = payload.get("path") or payload.get("filepath")
            if not isinstance(recipient, str) or not recipient.strip():
                raise _HTTPError(400, "'to' must be a non-empty string")
            if not isinstance(file_path, str) or not file_path.strip():
                raise _HTTPError(400, "'path' must be a non-empty string")
            resolved_path = Path(file_path).resolve()
            if not resolved_path.is_file():
                raise _HTTPError(400, f"File does not exist: {file_path}")
            send_file_fn = getattr(self._session, "send_file", None)
            if not callable(send_file_fn):
                self._send_json(conn, 503, {"error": "Session does not support send_file"})
                return
            try:
                transfer_id = send_file_fn(recipient.strip(), str(resolved_path))
            except (ValueError, OSError, RuntimeError) as exc:
                self._send_json(conn, 400, {"error": str(exc)})
                return
            self._broadcast_sse({"type": "file", "state": self._get_safe_state()})
            self._send_json(conn, 202, {"transfer_id": str(transfer_id)})
            return

        if path == "/api/file/upload":
            if method != "POST":
                self._method_not_allowed(conn, "POST")
                return
            if self._session is None:
                self._send_json(conn, 401, {"error": "Not logged in"})
                return
            content_type = headers.get("content-type")
            if (
                content_type is not None
                and content_type.split(";", 1)[0].strip().lower()
                != "application/json"
            ):
                raise _HTTPError(400, "Content-Type must be application/json")
            try:
                payload = json.loads(
                    body.decode("utf-8"),
                    parse_constant=_reject_json_constant,
                )
            except (UnicodeDecodeError, ValueError, RecursionError) as exc:
                raise _HTTPError(400, "Invalid JSON body") from exc
            if not isinstance(payload, dict):
                raise _HTTPError(400, "JSON body must be an object")
            recipient = payload.get("to")
            filename = payload.get("filename")
            content_b64 = payload.get("content") or payload.get("content_base64")
            if not isinstance(recipient, str) or not recipient.strip():
                raise _HTTPError(400, "'to' must be a non-empty string")
            if not isinstance(filename, str) or not filename.strip():
                raise _HTTPError(400, "'filename' must be a non-empty string")
            if not isinstance(content_b64, str):
                raise _HTTPError(400, "'content' must be a base64 string")

            safe_name = Path(filename).name
            if not safe_name or safe_name.startswith(".") or "\\" in safe_name or "/" in safe_name or "\x00" in safe_name:
                raise _HTTPError(400, "Invalid filename")

            try:
                if "," in content_b64:
                    content_b64 = content_b64.split(",", 1)[1]
                file_data = base64.b64decode(content_b64)
            except Exception as exc:
                raise _HTTPError(400, "Invalid base64 payload") from exc

            _UPLOADS_ROOT.mkdir(parents=True, exist_ok=True)
            import uuid
            save_path = _UPLOADS_ROOT / f"{uuid.uuid4().hex[:8]}_{safe_name}"
            save_path.write_bytes(file_data)

            send_file_fn = getattr(self._session, "send_file", None)
            if not callable(send_file_fn):
                self._send_json(conn, 503, {"error": "Session does not support send_file"})
                return
            try:
                transfer_id = send_file_fn(recipient.strip(), str(save_path))
            except (ValueError, OSError, RuntimeError) as exc:
                self._send_json(conn, 400, {"error": str(exc)})
                return
            self._broadcast_sse({"type": "file", "state": self._get_safe_state()})
            self._send_json(conn, 202, {"transfer_id": str(transfer_id), "filename": safe_name})
            return

        if path == "/api/file/download" or path.startswith("/api/file/download/"):
            if method != "GET":
                self._method_not_allowed(conn, "GET")
                return
            raw_filename = ""
            if path.startswith("/api/file/download/"):
                raw_filename = path[len("/api/file/download/"):].strip()
            if not raw_filename:
                raw_uri = headers.get("x-raw-uri", "")
                if "?" in raw_uri:
                    query = parse_qs(raw_uri.split("?", 1)[1])
                    raw_filename = query.get("filename", [""])[0]

            try:
                filename = unquote(raw_filename, encoding="utf-8", errors="strict").strip()
            except Exception:
                raise _HTTPError(400, "Invalid filename encoding")

            if not filename or ".." in filename or "/" in filename or "\\" in filename or ":" in filename or "\x00" in filename:
                raise _HTTPError(400, "Invalid or unsafe filename")

            target_file: Optional[Path] = None
            candidates: list[Path] = []
            if self._session is not None:
                session_dl = getattr(self._session, "download_dir", None)
                if session_dl:
                    candidates.append(Path(session_dl) / filename)
                    for f in Path(session_dl).glob(f"*_{filename}"):
                        if f.is_file():
                            candidates.append(f)

            candidates.append(_DOWNLOADS_ROOT / filename)
            for user_dir in _DOWNLOADS_ROOT.glob("*"):
                if user_dir.is_dir():
                    candidates.append(user_dir / filename)
                    for f in user_dir.glob(f"*_{filename}"):
                        if f.is_file():
                            candidates.append(f)

            for c in candidates:
                if c.is_file():
                    target_file = c
                    break

            if target_file is None:
                raise _HTTPError(404, "File not found")

            try:
                file_bytes = target_file.read_bytes()
            except OSError:
                raise _HTTPError(500, "Could not read file")

            mime = mimetypes.guess_type(target_file.name)[0] or "application/octet-stream"
            headers_dl = {
                "Content-Disposition": f'attachment; filename="{target_file.name}"',
            }
            self._send_response(
                conn, 200, file_bytes, content_type=mime, headers=headers_dl
            )
            return

        if path == "/api/transfers":
            if method != "GET":
                self._method_not_allowed(conn, "GET")
                return
            transfers = []
            if self._session is not None:
                try:
                    transfers = self._session.get_state().get("transfers", [])
                except Exception:
                    pass
            self._send_json(conn, 200, {"transfers": transfers})
            return

        if path == "/api/events":
            if method != "GET":
                self._method_not_allowed(conn, "GET")
                return
            headers_resp = (
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/event-stream; charset=utf-8\r\n"
                "Cache-Control: no-cache, no-transform\r\n"
                "Connection: keep-alive\r\n"
                "Access-Control-Allow-Origin: *\r\n"
                "X-Accel-Buffering: no\r\n\r\n"
            )
            conn.sendall(headers_resp.encode("ascii"))
            with self._sse_lock:
                self._sse_clients.add(conn)

            initial_data = (
                b"data: "
                + _json_bytes({"type": "snapshot", "state": self._get_safe_state()})
                + b"\n\n"
            )
            conn.sendall(initial_data)

            try:
                conn.settimeout(15.0)
                while not self._stopped.is_set():
                    try:
                        time.sleep(10.0)
                    except Exception:
                        break
                    if self._stopped.is_set():
                        break
                    try:
                        conn.sendall(b": ping\n\n")
                    except OSError:
                        break
            finally:
                with self._sse_lock:
                    self._sse_clients.discard(conn)
            return

        target = self._static_target(path)
        if target is None:
            raise _HTTPError(404, "Not found")
        if method != "GET":
            self._method_not_allowed(conn, "GET")
            return
        try:
            if target.stat().st_size > MAX_STATIC_BYTES:
                raise _HTTPError(413, "Static asset too large")
            content = target.read_bytes()
        except _HTTPError:
            raise
        except (OSError, ValueError):
            raise _HTTPError(404, "Not found") from None

        content_type = {
            ".html": "text/html; charset=utf-8",
            ".htm": "text/html; charset=utf-8",
            ".css": "text/css; charset=utf-8",
            ".js": "text/javascript; charset=utf-8",
            ".mjs": "text/javascript; charset=utf-8",
            ".json": "application/json; charset=utf-8",
            ".txt": "text/plain; charset=utf-8",
            ".svg": "image/svg+xml",
        }.get(target.suffix.lower())
        if content_type is None:
            content_type = mimetypes.guess_type(target.name)[0]
        self._send_response(
            conn, 200, content, content_type or "application/octet-stream"
        )

    def _method_not_allowed(self, conn: socket.socket, allow: str) -> None:
        """Return the method error shared by API and existing static routes."""
        self._send_json(
            conn, 405, {"error": "Method not allowed"}, allow=allow
        )

    def _static_target(self, path: str) -> Optional[Path]:
        """Resolve an allowlisted file while rejecting traversal and hidden paths."""
        try:
            decoded = unquote(path, encoding="utf-8", errors="strict")
            for candidate in (path, decoded):
                if (
                    "\x00" in candidate
                    or "\\" in candidate
                    or ":" in candidate
                    or any(part.startswith(".") for part in candidate.split("/") if part)
                ):
                    return None

            if path == "/mock_state.json":
                target = (_REPO_ROOT / "mock_state.json").resolve()
                target.relative_to(_REPO_ROOT)
                return target if target.is_file() else None

            root = self._static_root
            if root is None or not root.is_dir():
                return None

            relative = "index.html" if decoded == "/" else decoded.lstrip("/")
            requested = root / relative
            if requested.suffix.lower() not in _ALLOWED_EXTENSIONS:
                return None
            target = requested.resolve()
            resolved_relative = target.relative_to(root)
            if any(part.startswith(".") for part in resolved_relative.parts):
                return None
            if target.suffix.lower() not in _ALLOWED_EXTENSIONS:
                return None
            return target if target.is_file() else None
        except (OSError, ValueError, RuntimeError):
            return None


def start_gateway(
    session: Optional[SessionAPI] = None,
    *,
    host: str = "127.0.0.1",
    port: int = 8080,
    static_dir: Optional[str | Path] = None,
    max_body_bytes: int = 16384,
    server_host: str = SERVER_HOST,
    server_port: int = TCP_PORT,
    server_udp_port: int = UDP_PORT,
) -> WebGateway:
    """Bind synchronously and return a background gateway for the given session.

    Invalid arguments fail before startup. Socket setup failures propagate after
    cleanup, and the supplied session remains owned by its caller.
    """
    return WebGateway(
        session,
        host=host,
        port=port,
        static_dir=static_dir,
        max_body_bytes=max_body_bytes,
        server_host=server_host,
        server_port=server_port,
        server_udp_port=server_udp_port,
    )
