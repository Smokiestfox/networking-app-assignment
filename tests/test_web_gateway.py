"""Contract, framing, security, and integration tests for the HTTP gateway."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from copy import deepcopy
import importlib
import inspect
import json
from pathlib import Path
import socket
import threading
import time
from typing import Any, Callable, Iterator
from uuid import UUID

import pytest

import client
import web_gateway


WAIT_TIMEOUT = 8.0
MESSAGE_ID = "5aef69e1-8242-4b12-b8ef-d6132b37ee65"


class FakeSession:
    """Provide a detached snapshot and record gateway API calls."""

    def __init__(self) -> None:
        self._password = "private-test-password"
        self._key = "private-test-key"
        self.lock = threading.Lock()
        self.calls: list[tuple[str, str]] = []
        self.state_error: Exception | None = None
        self.chat_error: Exception | None = None
        self.state: dict[str, Any] = {
            "schema_version": 1,
            "revision": 12,
            "username": "alice",
            "status": {"state": "connected", "detail": None},
            "users": ["alice", "bob"],
            "messages": [
                {
                    "id": MESSAGE_ID,
                    "from": "bob",
                    "to": "alice",
                    "text": "Xin chào Việt Nam!",
                    "time": 1793880000,
                    "direction": "incoming",
                }
            ],
            "transfers": [
                {
                    "id": "d842dfce-28d5-4328-b58e-cad5ef45b4a2",
                    "direction": "receive",
                    "peer": "bob",
                    "filename": "ghi-chú.txt",
                    "size": 4,
                    "bytes_transferred": 4,
                    "progress": 1.0,
                    "status": "completed",
                    "error": None,
                    "path": "downloads/alice/ghi-chú.txt",
                    "checksum": "b" * 64,
                    "cwnd": None,
                    "rto": None,
                }
            ],
        }

    def get_state(self) -> dict[str, Any]:
        """Return a copy without exposing credentials."""
        with self.lock:
            if self.state_error is not None:
                raise self.state_error
            return deepcopy(self.state)

    def send_chat(self, to: str, text: str) -> str:
        """Record the exact arguments or raise the configured failure."""
        with self.lock:
            if self.chat_error is not None:
                raise self.chat_error
            self.calls.append((to, text))
        return MESSAGE_ID


def wait_until(
    predicate: Callable[[], Any],
    timeout: float = WAIT_TIMEOUT,
) -> Any:
    """Poll a condition using a monotonic deadline."""
    deadline = time.monotonic() + timeout
    pause = threading.Event()
    while True:
        result = predicate()
        if result:
            return result
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            pytest.fail("Condition did not become true before the deadline")
        pause.wait(min(0.01, remaining))


@contextmanager
def running_gateway(
    session: Any,
    **kwargs: Any,
) -> Iterator[web_gateway.WebGateway]:
    """Start a gateway and always release its listener and workers."""
    gateway = web_gateway.start_gateway(session, port=0, **kwargs)
    try:
        yield gateway
    finally:
        gateway.close()


@contextmanager
def connected_socket(
    gateway: web_gateway.WebGateway,
) -> Iterator[socket.socket]:
    """Connect to the gateway with a bounded I/O timeout."""
    connection = socket.create_connection(
        (gateway.host, gateway.port), timeout=3.0
    )
    try:
        connection.settimeout(3.0)
        yield connection
    finally:
        connection.close()


def read_response(
    connection: socket.socket,
) -> tuple[int, dict[str, str], bytes]:
    """Read one framed response and require the server to close afterward."""
    raw = bytearray()
    while b"\r\n\r\n" not in raw:
        chunk = connection.recv(4096)
        assert chunk, "Connection closed before response headers"
        raw.extend(chunk)
        assert len(raw) < 128 * 1024, "Unbounded response headers"

    header_block, body = bytes(raw).split(b"\r\n\r\n", 1)
    lines = header_block.decode("iso-8859-1").split("\r\n")
    version, status, _ = lines[0].split(" ", 2)
    assert version == "HTTP/1.1"
    headers: dict[str, str] = {}
    for line in lines[1:]:
        name, value = line.split(":", 1)
        headers[name.lower()] = value.strip()

    length = int(headers["content-length"])
    assert len(body) <= length
    while len(body) < length:
        chunk = connection.recv(min(4096, length - len(body)))
        assert chunk, "Truncated response body"
        body += chunk

    assert connection.recv(1) == b"", "Response connection was not closed"
    return int(status), headers, body


def raw_request(
    gateway: web_gateway.WebGateway,
    request: bytes,
) -> tuple[int, dict[str, str], bytes]:
    """Send arbitrary HTTP bytes without normalizing the request target."""
    with connected_socket(gateway) as connection:
        connection.sendall(request)
        return read_response(connection)


def request(
    gateway: web_gateway.WebGateway,
    method: str = "GET",
    target: str = "/api/state",
    *,
    body: bytes = b"",
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, str], bytes]:
    """Send a normal request over a fresh raw TCP connection."""
    fields = {
        "Host": f"{gateway.host}:{gateway.port}",
        "Content-Length": str(len(body)),
        "Connection": "close",
    }
    fields.update(headers or {})
    head = "\r\n".join(
        [f"{method} {target} HTTP/1.1"]
        + [f"{name}: {value}" for name, value in fields.items()]
    )
    return raw_request(gateway, head.encode("ascii") + b"\r\n\r\n" + body)


def json_body(value: Any) -> bytes:
    """Encode test payloads as actual UTF-8."""
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


def assert_error(
    response: tuple[int, dict[str, str], bytes],
    expected_status: int,
) -> dict[str, Any]:
    """Check the shared JSON error contract without exposing filesystem paths."""
    status, headers, body = response
    assert status == expected_status
    assert headers["content-type"] == "application/json; charset=utf-8"
    assert headers["connection"] == "close"
    assert headers["cache-control"] == "no-store"
    assert headers["x-content-type-options"] == "nosniff"
    payload = json.loads(body)
    assert isinstance(payload, dict)
    assert set(payload) == {"error"}
    assert isinstance(payload["error"], str)
    assert payload["error"]
    return payload


@pytest.fixture
def session() -> FakeSession:
    """Return an independent fake session."""
    return FakeSession()


@pytest.fixture
def gateway(
    session: FakeSession,
    tmp_path: Path,
) -> Iterator[web_gateway.WebGateway]:
    """Use an empty isolated static root for API tests."""
    with running_gateway(session, static_dir=tmp_path) as handle:
        yield handle


@pytest.mark.parametrize(
    "invalid_session",
    [
        object(),
        type("MissingState", (), {"send_chat": lambda self, to, text: ""})(),
        type("MissingChat", (), {"get_state": lambda self: {}})(),
        type("InvalidState", (), {"get_state": None, "send_chat": lambda *a: ""})(),
        type("InvalidChat", (), {"get_state": lambda self: {}, "send_chat": None})(),
    ],
)
def test_session_validation(invalid_session: Any) -> None:
    """Both session operations must be callable."""
    with pytest.raises(TypeError):
        with running_gateway(invalid_session):
            pytest.fail("Invalid session was accepted")


@pytest.mark.parametrize("host", ["0.0.0.0", "192.168.1.10", "example.com", ""])
def test_reject_non_loopback_hosts(session: FakeSession, host: str) -> None:
    """The browser gateway must bind only a loopback address."""
    with pytest.raises(ValueError):
        with running_gateway(session, host=host):
            pytest.fail("Invalid host was accepted")


@pytest.mark.parametrize("host", ["127.0.0.1", "127.0.0.2", "localhost"])
def test_accept_loopback_hosts(session: FakeSession, host: str) -> None:
    """All documented loopback bind choices serve requests."""
    with running_gateway(session, host=host) as handle:
        assert handle.port > 0
        assert handle.url == f"http://{host}:{handle.port}"
        assert request(handle)[0] == 200


@pytest.mark.parametrize("port", [-1, 65536, True, "8080"])
def test_reject_invalid_ports(session: FakeSession, port: Any) -> None:
    """Invalid ports fail before a listener starts."""
    handle = None
    try:
        with pytest.raises(ValueError):
            handle = web_gateway.start_gateway(session, port=port)
    finally:
        if handle is not None:
            handle.close()


@pytest.mark.parametrize("limit", [0, -1, False])
def test_reject_invalid_body_limits(session: FakeSession, limit: Any) -> None:
    """The body limit must be a positive integer."""
    with pytest.raises(ValueError):
        with running_gateway(session, max_body_bytes=limit):
            pytest.fail("Invalid body limit was accepted")


@pytest.mark.parametrize("parent", [False, True])
def test_reject_repository_static_root(
    session: FakeSession,
    parent: bool,
) -> None:
    """Neither the repository nor its ancestor may be a static root."""
    root = Path(__file__).resolve().parent.parent
    with pytest.raises(ValueError):
        with running_gateway(session, static_dir=root.parent if parent else root):
            pytest.fail("Unsafe static root was accepted")


def test_collision_cleanup_and_immediate_port_reuse(
    session: FakeSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failed bind closes its socket; successful shutdown releases its port."""
    real_socket = socket.socket
    handle = web_gateway.start_gateway(session, port=0)
    attempts: list[socket.socket] = []

    def tracking_socket(*args: Any, **kwargs: Any) -> socket.socket:
        """Keep references to sockets allocated during the failing startup."""
        connection = real_socket(*args, **kwargs)
        attempts.append(connection)
        return connection

    collision = None
    port = handle.port
    try:
        accept_thread = handle._accept_thread
        with monkeypatch.context() as patch:
            patch.setattr(web_gateway.socket, "socket", tracking_socket)
            with pytest.raises(OSError):
                collision = web_gateway.start_gateway(session, port=port)
        assert attempts
        assert all(connection.fileno() == -1 for connection in attempts)
        assert handle._accept_thread is accept_thread
        assert request(handle)[0] == 200
    finally:
        if collision is not None:
            collision.close()
        handle.close()
        for connection in attempts:
            connection.close()

    handle.close()
    assert handle.is_running is False
    assert accept_thread is not None
    assert not accept_thread.is_alive()
    replacement = web_gateway.start_gateway(session, port=port)
    try:
        assert request(replacement)[0] == 200
    finally:
        replacement.close()


@pytest.mark.parametrize("target", ["/api/state", "/api/state?ts=123"])
@pytest.mark.parametrize("state", ["connected", "error", "disconnected"])
def test_state_snapshot(
    session: FakeSession,
    gateway: web_gateway.WebGateway,
    target: str,
    state: str,
) -> None:
    """HTTP success is independent of network session status."""
    session.state["status"] = {
        "state": state,
        "detail": "Mất kết nối" if state == "error" else None,
    }
    before = session.get_state()
    status, headers, body = request(gateway, target=target)
    assert status == 200
    assert headers["content-type"] == "application/json; charset=utf-8"
    assert headers["cache-control"] == "no-store"
    assert headers["x-content-type-options"] == "nosniff"
    assert headers["connection"] == "close"
    assert "access-control-allow-origin" not in headers
    assert json.loads(body) == before
    assert "Xin chào Việt Nam!".encode("utf-8") in body
    assert session._password.encode() not in body
    assert session._key.encode() not in body
    assert session.get_state() == before
    assert not session.calls


@pytest.mark.parametrize("error_type", [RuntimeError, OSError, ConnectionError])
def test_state_failure(
    session: FakeSession,
    gateway: web_gateway.WebGateway,
    error_type: type[Exception],
) -> None:
    """Session read failures use the service-unavailable contract."""
    session.state_error = error_type("state unavailable")
    assert assert_error(request(gateway), 503) == {"error": "state unavailable"}


@pytest.mark.parametrize(
    ("method", "target", "allow"),
    [
        ("POST", "/api/state", "GET"),
        ("PUT", "/api/state", "GET"),
        ("GET", "/api/chat", "POST"),
    ],
)
def test_api_methods(
    gateway: web_gateway.WebGateway,
    method: str,
    target: str,
    allow: str,
) -> None:
    """Known API routes advertise their supported method."""
    response = request(gateway, method, target)
    assert_error(response, 405)
    assert response[1]["allow"] == allow


@pytest.mark.parametrize(
    "content_type",
    ["application/json", "application/json; charset=utf-8", None],
)
def test_chat_success(
    session: FakeSession,
    gateway: web_gateway.WebGateway,
    content_type: str | None,
) -> None:
    """Chat forwards exact Unicode arguments and returns the message identifier."""
    response = request(
        gateway,
        "POST",
        "/api/chat",
        body=json_body({"to": "bob", "text": "Xin chào!"}),
        headers={} if content_type is None else {"Content-Type": content_type},
    )
    assert response[0] == 202
    assert response[1]["content-type"] == "application/json; charset=utf-8"
    assert json.loads(response[2]) == {"message_id": MESSAGE_ID}
    assert session.calls == [("bob", "Xin chào!")]


@pytest.mark.parametrize(
    "body",
    [
        b"\xff",
        b"{",
        b"",
        b"[]",
        b'"str"',
        b"null",
        b'{"to":"bob","text":NaN}',
        b'{"to":"bob","text":Infinity}',
        b'{"to":"bob","text":-Infinity}',
        b'{"to":"bob","text":"ok","extra":NaN}',
        b'{"text":"ok"}',
        b'{"to":"","text":"ok"}',
        b'{"to":" \\t ","text":"ok"}',
        b'{"to":null,"text":"ok"}',
        b'{"to":12,"text":"ok"}',
        b'{"to":[],"text":"ok"}',
        b'{"to":"bob"}',
        b'{"to":"bob","text":null}',
        b'{"to":"bob","text":1}',
        b'{"to":"bob","text":[]}',
    ],
)
def test_chat_invalid_payloads(
    session: FakeSession,
    gateway: web_gateway.WebGateway,
    body: bytes,
) -> None:
    """Malformed JSON and invalid chat fields never reach the session."""
    assert_error(request(gateway, "POST", "/api/chat", body=body), 400)
    assert session.calls == []


def test_chat_wrong_content_type(
    session: FakeSession,
    gateway: web_gateway.WebGateway,
) -> None:
    """Explicit non-JSON content types are rejected."""
    assert_error(
        request(
            gateway,
            "POST",
            "/api/chat",
            body=json_body({"to": "bob", "text": "ok"}),
            headers={"Content-Type": "text/plain"},
        ),
        400,
    )
    assert not session.calls


@pytest.mark.parametrize(
    ("error_type", "status"),
    [(ValueError, 400), (ConnectionError, 503), (OSError, 503), (RuntimeError, 503)],
)
def test_chat_session_errors(
    session: FakeSession,
    gateway: web_gateway.WebGateway,
    error_type: type[Exception],
    status: int,
) -> None:
    """Chat distinguishes validation failures from unavailable transports."""
    session.chat_error = error_type("cannot send")
    response = request(
        gateway,
        "POST",
        "/api/chat",
        body=json_body({"to": "bob", "text": "ok"}),
    )
    assert assert_error(response, status) == {"error": "cannot send"}


def test_oversized_body_rejected_without_body(session: FakeSession) -> None:
    """An excessive declared length is rejected without waiting for payload."""
    with running_gateway(session, max_body_bytes=32) as handle:
        response = raw_request(
            handle,
            b"POST /api/chat HTTP/1.1\r\nHost: localhost\r\n"
            b"Content-Length: 33\r\n\r\n",
        )
        assert_error(response, 413)


def test_split_network_request(
    session: FakeSession,
    gateway: web_gateway.WebGateway,
) -> None:
    """Header and body fragments may arrive in separate TCP writes."""
    body = json_body({"to": "bob", "text": "Xin chào!"})
    with connected_socket(gateway) as connection:
        fragments = [
            b"POST /api/",
            b"chat HTTP/1.1\r\nHost: localhost\r\nContent-Len",
            f"gth: {len(body)}\r\nContent-Type: application/json\r\n".encode(),
            b"\r\n" + body[:7],
            body[7:19],
            body[19:],
        ]
        for fragment in fragments:
            connection.sendall(fragment)
        response = read_response(connection)
    assert response[0] == 202
    assert session.calls == [("bob", "Xin chào!")]


class FragmentSocket:
    """Force deterministic recv boundaries independent of TCP coalescing."""

    def __init__(self, fragments: list[bytes]) -> None:
        self.fragments = list(fragments)

    def recv(self, size: int) -> bytes:
        """Return at most the requested number of bytes from the next fragment."""
        if not self.fragments:
            return b""
        fragment = self.fragments.pop(0)
        if len(fragment) > size:
            self.fragments.insert(0, fragment[size:])
        return fragment[:size]


def test_parser_preserves_fragmented_and_co_received_body() -> None:
    """The parser retains body bytes already received with the header ending."""
    body = json_body({"to": "bob", "text": "Chào"})
    connection = FragmentSocket(
        [
            b"POST /api/chat?x=1 HTTP/1.1\r\nContent-L",
            f"ength: {len(body)}\r\n".encode(),
            b"\r\n" + body[:5],
            body[5:11],
            body[11:],
        ]
    )
    method, path, headers, actual_body = web_gateway._read_request(
        connection, 16384  # type: ignore[arg-type]
    )
    assert method == "POST"
    assert path == "/api/chat"
    assert headers["content-length"] == str(len(body))
    assert actual_body == body
    assert connection.fragments == []


def test_oversized_headers(gateway: web_gateway.WebGateway) -> None:
    """An unterminated oversized header block receives 431."""
    prefix = b"GET /api/state HTTP/1.1\r\nX-Fill: "
    wire = prefix + b"a" * (web_gateway.MAX_HEADER_BYTES + 1 - len(prefix))
    assert_error(raw_request(gateway, wire), 431)


def test_oversized_uri_before_header_ending(
    gateway: web_gateway.WebGateway,
) -> None:
    """URI limits apply before a complete header block arrives."""
    wire = b"GET /" + b"a" * web_gateway.MAX_URI_LEN
    assert b"\r\n\r\n" not in wire
    assert_error(raw_request(gateway, wire), 414)


@pytest.mark.parametrize(
    "wire",
    [
        b"POST /api/chat HTTP/1.1\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\n",
        b"POST /api/chat HTTP/1.1\r\nContent-Length: -1\r\n\r\n",
        b"POST /api/chat HTTP/1.1\r\nContent-Length: abc\r\n\r\n",
        b"POST /api/chat HTTP/1.1\r\nContent-Length: +1\r\n\r\n",
        b"POST /api/chat HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n",
        b"POST /api/chat HTTP/1.1\r\nContent-Type: application/json\r\n"
        b"Content-Type: text/plain\r\n\r\n",
        b"BAD REQUEST LINE\r\n\r\n",
        b"GET //evil HTTP/1.1\r\n\r\n",
        b"GET /api/state HTTP/2.0\r\n\r\n",
        b"GET /api/state HTTP/1.1\r\nBroken-Header\r\n\r\n",
        b"GET /api/state HTTP/1.1\r\n folded: value\r\n\r\n",
        b"GET /api/state HTTP/1.1\r\nBad Name: value\r\n\r\n",
        b"GET /api/state HTTP/1.1\r\nX-Test: \x00\r\n\r\n",
    ],
)
def test_malformed_http(
    gateway: web_gateway.WebGateway,
    wire: bytes,
) -> None:
    """Invalid request framing returns a bounded JSON client error."""
    assert_error(raw_request(gateway, wire), 400)


def test_identical_duplicate_content_lengths(
    session: FakeSession,
    gateway: web_gateway.WebGateway,
) -> None:
    """Equivalent decimal lengths are accepted without ambiguous framing."""
    body = json_body({"to": "bob", "text": "ok"})
    wire = (
        b"POST /api/chat HTTP/1.1\r\n"
        + f"Content-Length: {len(body)}\r\n".encode()
        + f"Content-Length: 000{len(body)}\r\n\r\n".encode()
        + body
    )
    assert raw_request(gateway, wire)[0] == 202
    assert session.calls == [("bob", "ok")]


@pytest.mark.parametrize(
    "partial",
    [
        b"GET /api/state HTTP/1.1\r\nHost: local",
        b"POST /api/chat HTTP/1.1\r\nContent-Length: 20\r\n\r\n{",
    ],
)
def test_incomplete_request_timeout(
    session: FakeSession,
    monkeypatch: pytest.MonkeyPatch,
    partial: bytes,
) -> None:
    """Slow header and body readers return 400 after a real socket timeout."""
    original = web_gateway._read_request

    def bounded_read(
        connection: socket.socket,
        maximum: int,
    ) -> tuple[str, str, dict[str, str], bytes]:
        """Shorten only the accepted connection's read timeout for this test."""
        connection.settimeout(0.15)
        return original(connection, maximum)

    monkeypatch.setattr(web_gateway, "_read_request", bounded_read)
    with running_gateway(session) as handle:
        assert_error(raw_request(handle, partial), 400)


def active_count(gateway: web_gateway.WebGateway) -> int:
    """Read the tracked connection count under its owning lock."""
    with gateway._lock:
        return len(gateway._active_conns)


def test_connection_limit(
    session: FakeSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Idle clients consume only the configured number of handler slots."""
    monkeypatch.setattr(web_gateway, "MAX_CONNECTIONS", 2)
    with running_gateway(session) as handle:
        connections: list[socket.socket] = []
        try:
            for count in (1, 2):
                connection = socket.create_connection(
                    (handle.host, handle.port), timeout=3.0
                )
                connections.append(connection)
                wait_until(lambda: active_count(handle) == count)
            rejected = socket.create_connection(
                (handle.host, handle.port), timeout=3.0
            )
            connections.append(rejected)
            rejected.settimeout(3.0)
            try:
                assert rejected.recv(1) == b""
            except ConnectionResetError:
                pass
            assert active_count(handle) == 2
            for connection in connections:
                connection.close()
            wait_until(lambda: active_count(handle) == 0)
            assert request(handle)[0] == 200
        finally:
            for connection in connections:
                connection.close()


def test_concurrent_state_and_chat(
    session: FakeSession,
    gateway: web_gateway.WebGateway,
) -> None:
    """Twenty simultaneous API requests complete with independent responses."""
    barrier = threading.Barrier(20)

    def operation(index: int) -> tuple[int, dict[str, str], bytes]:
        """Synchronize clients before issuing alternating API operations."""
        barrier.wait(timeout=5.0)
        if index % 2 == 0:
            return request(gateway)
        return request(
            gateway,
            "POST",
            "/api/chat",
            body=json_body({"to": "bob", "text": f"tin {index}"}),
        )

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(operation, index) for index in range(20)]
        responses = [future.result(timeout=8.0) for future in futures]

    for index, response in enumerate(responses):
        if index % 2 == 0:
            assert response[0] == 200
            assert json.loads(response[2]) == session.get_state()
        else:
            assert response[0] == 202
            assert json.loads(response[2]) == {"message_id": MESSAGE_ID}
    assert sorted(session.calls) == sorted(
        ("bob", f"tin {index}") for index in range(1, 20, 2)
    )


def test_shutdown_closes_idle_connection(session: FakeSession) -> None:
    """Shutdown interrupts an idle recv and leaves no gateway worker alive."""
    handle = web_gateway.start_gateway(session, port=0)
    connection = None
    try:
        connection = socket.create_connection(
            (handle.host, handle.port), timeout=3.0
        )
        connection.settimeout(3.0)
        wait_until(lambda: active_count(handle) == 1)
        with handle._lock:
            workers = tuple(handle._workers)
        started = time.monotonic()
        handle.close()
        assert time.monotonic() - started < 3.0
        assert not handle.is_running
        try:
            assert connection.recv(1) == b""
        except ConnectionResetError:
            pass
        wait_until(lambda: all(not worker.is_alive() for worker in workers))
        assert active_count(handle) == 0
    finally:
        if connection is not None:
            connection.close()
        handle.close()


@pytest.mark.parametrize("target", ["/mock_state.json", "/mock_state.json?v=1"])
def test_mock_state(
    gateway: web_gateway.WebGateway,
    target: str,
) -> None:
    """The repository mock is served byte-for-byte as JSON."""
    expected = (
        Path(__file__).resolve().parent.parent / "mock_state.json"
    ).read_bytes()
    status, headers, body = request(gateway, target=target)
    assert status == 200
    assert headers["content-type"] == "application/json; charset=utf-8"
    assert body == expected


def test_mock_state_method(gateway: web_gateway.WebGateway) -> None:
    """The mock route is read-only."""
    response = request(gateway, "POST", "/mock_state.json")
    assert_error(response, 405)
    assert response[1]["allow"] == "GET"


def test_no_default_static_directory(
    session: FakeSession,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No implicit repository files are exposed when UI roots are absent."""
    empty_repository = tmp_path / "empty-repository"
    empty_repository.mkdir()
    monkeypatch.setattr(web_gateway, "_REPO_ROOT", empty_repository)
    with running_gateway(session) as handle:
        for target in ("/", "/users.json", "/config.py", "/server.py"):
            assert_error(request(handle, target=target), 404)


def test_auto_detect_static_and_ui_directories(
    session: FakeSession,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Implicit static/ or ui/ folders work without exposing repository root files."""
    repo_copy = tmp_path / "repo-copy"
    repo_copy.mkdir()
    (repo_copy / "users.json").write_text('{"alice":"pass123"}', encoding="utf-8")
    (repo_copy / "mock_state.json").write_text('{"schema_version":1}', encoding="utf-8")
    ui_dir = repo_copy / "ui"
    ui_dir.mkdir()
    (ui_dir / "index.html").write_text("<h1>UI</h1>", encoding="utf-8")

    monkeypatch.setattr(web_gateway, "_REPO_ROOT", repo_copy)
    with running_gateway(session) as handle:
        status, _, body = request(handle, target="/")
        assert status == 200
        assert body == b"<h1>UI</h1>"
        assert request(handle, target="/mock_state.json")[0] == 200
        assert_error(request(handle, target="/users.json"), 404)

    static_dir = repo_copy / "static"
    static_dir.mkdir()
    (static_dir / "index.html").write_text("<h1>STATIC</h1>", encoding="utf-8")
    with running_gateway(session) as handle:
        status, _, body = request(handle, target="/")
        assert status == 200
        assert body == b"<h1>STATIC</h1>"


def test_cli_web_port_arg_parsing() -> None:
    """CLI parses --web-port cleanly and validates bounds."""
    args = client.parse_cli_args(["alice", "pass123", "--web-port", "8080"])
    assert args.web_port == 8080
    with pytest.raises(SystemExit):
        client.parse_cli_args(["alice", "pass123", "--web-port", "0"])
    with pytest.raises(SystemExit):
        client.parse_cli_args(["alice", "pass123", "--web-port", "70000"])


@pytest.fixture
def static_gateway(
    session: FakeSession,
    tmp_path: Path,
) -> Iterator[tuple[web_gateway.WebGateway, Path]]:
    """Create representative public and forbidden assets in an isolated root."""
    root = tmp_path / "public"
    root.mkdir()
    assets = {
        "index.html": b"<!doctype html><title>Chat</title>",
        "app.js": b"console.log('chat');",
        "style.css": b"body { color: navy; }",
        "logo.svg": b'<svg xmlns="http://www.w3.org/2000/svg"/>',
        ".env": b"SECRET=private",
        "script.py": b"secret = True",
        "bin.exe": b"MZ",
        "noext": b"private",
    }
    for name, content in assets.items():
        (root / name).write_bytes(content)
    (root / ".git").mkdir()
    (root / ".git" / "config.json").write_text('{"secret":true}', encoding="utf-8")
    (root / "sub").mkdir()
    (root / "sub" / ".hidden.html").write_text("private", encoding="utf-8")
    (tmp_path / "users.json").write_text('{"password":"private"}', encoding="utf-8")
    with running_gateway(session, static_dir=root) as handle:
        yield handle, root


@pytest.mark.parametrize(
    ("target", "filename", "content_type"),
    [
        ("/", "index.html", "text/html; charset=utf-8"),
        ("/app.js", "app.js", "text/javascript; charset=utf-8"),
        ("/style.css", "style.css", "text/css; charset=utf-8"),
        ("/logo.svg", "logo.svg", "image/svg+xml"),
    ],
)
def test_static_assets(
    static_gateway: tuple[web_gateway.WebGateway, Path],
    target: str,
    filename: str,
    content_type: str,
) -> None:
    """Allowlisted assets preserve content and have browser-appropriate types."""
    handle, root = static_gateway
    status, headers, body = request(handle, target=target)
    assert status == 200
    assert headers["content-type"] == content_type
    assert body == (root / filename).read_bytes()


def test_static_post(
    static_gateway: tuple[web_gateway.WebGateway, Path],
) -> None:
    """Existing static targets report method errors."""
    handle, _ = static_gateway
    response = request(handle, "POST", "/")
    assert_error(response, 405)
    assert response[1]["allow"] == "GET"


def test_static_size_limit(
    static_gateway: tuple[web_gateway.WebGateway, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Static responses honor the configured byte bound."""
    handle, root = static_gateway
    monkeypatch.setattr(web_gateway, "MAX_STATIC_BYTES", 64)
    (root / "large.txt").write_bytes(b"x" * 65)
    assert_error(request(handle, target="/large.txt"), 413)


@pytest.mark.parametrize(
    "target",
    [
        "/../users.json",
        "/%2e%2e/users.json",
        "/%252e%252e/config.py",
        "/a/../../users.json",
        "/\\users.json",
        "/%5cusers.json",
        "/index.html%00.txt",
        "/index.html::$DATA",
        "/C:/users.json",
        "/.env",
        "/.git/config.json",
        "/sub/.hidden.html",
        "/script.py",
        "/bin.exe",
        "/noext",
        "/missing.html",
        "/unknown-api",
    ],
)
def test_static_security_boundaries(
    static_gateway: tuple[web_gateway.WebGateway, Path],
    target: str,
) -> None:
    """Unsafe targets return generic 404 JSON without filesystem disclosure."""
    handle, root = static_gateway
    payload = assert_error(request(handle, target=target), 404)
    assert payload == {"error": "Not found"}
    assert str(root) not in payload["error"]
    assert str(root.parent) not in payload["error"]


def test_symlink_cannot_escape_static_root(
    static_gateway: tuple[web_gateway.WebGateway, Path],
) -> None:
    """A public JSON symlink cannot expose repository users.json."""
    handle, root = static_gateway
    destination = Path(__file__).resolve().parent.parent / "users.json"
    try:
        (root / "linked-users.json").symlink_to(destination)
    except OSError:
        pytest.skip("Symlinks require elevated privileges on this host")
    assert_error(request(handle, target="/linked-users.json"), 404)


def registry_type(server_module: Any) -> type[Any]:
    """Find the project's registry export without depending on its module split."""
    exported = getattr(server_module, "UserRegistry", None)
    if exported is not None:
        return exported
    for module_name in ("user_registry", "registry"):
        try:
            module = importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            if exc.name != module_name:
                raise
            continue
        exported = getattr(module, "UserRegistry", None)
        if exported is not None:
            return exported
    raise AssertionError("The project must expose UserRegistry")


def server_arguments(
    server_class: type[Any],
    registry: Any,
    tcp_port: int,
    udp_port: int,
) -> dict[str, Any]:
    """Map conventional server parameter names while enforcing isolated state."""
    parameters = inspect.signature(server_class).parameters
    accepts_kwargs = any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in parameters.values()
    )
    arguments: dict[str, Any] = {}

    def assign(names: tuple[str, ...], value: Any, required: bool = True) -> None:
        """Select an explicit supported name, falling back to keyword arguments."""
        for name in names:
            if name in parameters:
                arguments[name] = value
                return
        if accepts_kwargs:
            arguments[names[0]] = value
        elif required:
            raise AssertionError(f"Missing server constructor parameter: {names}")

    assign(("host", "bind_host"), "127.0.0.1", required=False)
    assign(("tcp_port", "port"), tcp_port)
    assign(("udp_port", "server_udp_port"), udp_port)
    assign(("registry", "user_registry"), registry)
    assign(("log_file",), None)
    return arguments


@contextmanager
def live_server(tcp_port: int, udp_port: int) -> Iterator[Any]:
    """Run a real server with an in-memory registry and no persistent log."""
    server_module = importlib.import_module("server")
    registry = registry_type(server_module)(users_file=None)
    registry.user_db = {"alice": "pass123", "bob": "pass123"}
    server_class = server_module.CollaborationServer
    server = server_class(
        **server_arguments(server_class, registry, tcp_port, udp_port)
    )
    failures: list[BaseException] = []
    runner: threading.Thread | None = None
    try:
        start = next(
            (
                getattr(server, name)
                for name in ("start", "serve_forever", "run")
                if callable(getattr(server, name, None))
            ),
            None,
        )
        assert start is not None, "CollaborationServer needs a start operation"

        def run() -> None:
            """Capture background startup errors for the main test thread."""
            try:
                start()
            except BaseException as exc:
                failures.append(exc)

        runner = threading.Thread(
            target=run, name=f"test-server-{tcp_port}", daemon=True
        )
        runner.start()

        def ready() -> bool:
            """Probe TCP readiness without creating authenticated disk state."""
            if failures:
                raise AssertionError("Server startup failed") from failures[0]
            probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                probe.settimeout(0.1)
                return probe.connect_ex(("127.0.0.1", tcp_port)) == 0
            finally:
                probe.close()

        wait_until(ready)
        yield server
        if failures:
            raise AssertionError("Server failed during test") from failures[0]
    finally:
        stop = next(
            (
                getattr(server, name)
                for name in ("stop", "shutdown", "close")
                if callable(getattr(server, name, None))
            ),
            None,
        )
        try:
            assert stop is not None, "CollaborationServer needs a stop operation"
            stop()
        finally:
            if runner is not None:
                runner.join(timeout=WAIT_TIMEOUT)
                assert not runner.is_alive(), "Server runner did not terminate"


def new_session(
    username: str,
    tcp_port: int,
    udp_port: int,
    download_dir: Path,
) -> client.ClientSession:
    """Construct a real client with all downloads confined to pytest storage."""
    return client.ClientSession(
        username,
        "pass123",
        host="127.0.0.1",
        port=tcp_port,
        server_udp_port=udp_port,
        download_dir=download_dir,
    )


def assert_session_stopped(session: client.ClientSession) -> None:
    """Check public state and transport cleanup after session termination."""
    assert not session.is_connected
    assert session.wait(0)
    assert session._tcp is None
    assert session._udp is None
    assert session._transfers is None
    assert not session._threads


def test_real_server_http_chat(tmp_path: Path) -> None:
    """HTTP chat traverses the real client/server UDP transport to Bob."""
    with live_server(9460, 9461):
        alice = new_session("alice", 9460, 9461, tmp_path / "alice")
        bob = new_session("bob", 9460, 9461, tmp_path / "bob")
        handle = None
        try:
            assert alice.start(), alice.get_state()["status"]
            assert bob.start(), bob.get_state()["status"]
            handle = web_gateway.start_gateway(alice, port=0)
            wait_until(
                lambda: alice.get_state()["users"] == ["alice", "bob"]
                and bob.get_state()["users"] == ["alice", "bob"]
            )
            status, _, body = request(handle)
            snapshot = json.loads(body)
            assert status == 200
            assert snapshot["status"]["state"] == "connected"
            assert snapshot["users"] == ["alice", "bob"]

            text = "Chào Bob qua HTTP"
            status, _, body = request(
                handle,
                "POST",
                "/api/chat",
                body=json_body({"to": "bob", "text": text}),
                headers={"Content-Type": "application/json; charset=utf-8"},
            )
            assert status == 202
            message_id = json.loads(body)["message_id"]
            assert str(UUID(message_id)) == message_id

            def received() -> list[dict[str, Any]]:
                """Find the incoming datagram identified by the HTTP result."""
                return [
                    message
                    for message in bob.get_state()["messages"]
                    if message["id"] == message_id
                ]

            incoming = wait_until(received)
            assert len(incoming) == 1
            assert incoming[0]["text"] == text
            assert incoming[0]["from"] == "alice"
            assert incoming[0]["to"] == "bob"
            assert incoming[0]["direction"] == "incoming"

            outgoing = [
                message
                for message in alice.get_state()["messages"]
                if message["id"] == message_id
            ]
            assert len(outgoing) == 1
            assert outgoing[0]["text"] == text
            assert outgoing[0]["direction"] == "outgoing"

            assert_error(
                request(
                    handle,
                    "POST",
                    "/api/chat",
                    body=json_body({"to": "bob", "text": ""}),
                ),
                400,
            )

            broadcast_status, _, broadcast_body = request(
                handle,
                "POST",
                "/api/chat",
                body=json_body({"to": "*", "text": "Broadcast HTTP"}),
            )
            assert broadcast_status == 202
            broadcast_id = json.loads(broadcast_body)["message_id"]
            broadcast_incoming = wait_until(
                lambda: [
                    message
                    for message in bob.get_state()["messages"]
                    if message["id"] == broadcast_id
                ]
            )
            assert len(broadcast_incoming) == 1
            assert broadcast_incoming[0]["to"] == "*"
            assert broadcast_incoming[0]["text"] == "Broadcast HTTP"
        finally:
            if handle is not None:
                handle.close()
            try:
                bob.close()
            finally:
                alice.close()
        assert_session_stopped(alice)
        assert_session_stopped(bob)


def test_run_client_gateway_lifecycle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The CLI integration starts a real gateway and stops both owned handles."""
    real_factory = web_gateway.start_gateway
    handles: list[web_gateway.WebGateway] = []
    sessions: list[client.ClientSession] = []
    ready = threading.Event()
    stop = threading.Event()
    result: list[bool] = []
    failures: list[BaseException] = []

    def capture_factory(
        session: client.ClientSession,
        **kwargs: Any,
    ) -> web_gateway.WebGateway:
        """Capture the real gateway selected by the CLI's dynamic import."""
        sessions.append(session)
        handle = real_factory(session, **kwargs)
        handles.append(handle)
        return handle

    monkeypatch.setattr(web_gateway, "start_gateway", capture_factory)

    with live_server(9462, 9463):
        runner = None
        try:
            def run() -> None:
                """Execute the actual headless CLI lifecycle."""
                try:
                    result.append(
                        client._run_client_impl(
                            username="alice",
                            password="pass123",
                            host="127.0.0.1",
                            port=9462,
                            server_udp_port=9463,
                            interactive=False,
                            download_dir=tmp_path / "cli-alice",
                            ready_evt=ready,
                            session_stop=stop,
                            web_port=0,
                        )
                    )
                except BaseException as exc:
                    failures.append(exc)

            runner = threading.Thread(
                target=run, name="test-cli-lifecycle", daemon=True
            )
            runner.start()

            def cli_ready() -> bool:
                """Expose runner errors immediately while waiting for readiness."""
                if failures:
                    raise AssertionError("CLI runner failed") from failures[0]
                if not runner.is_alive() and not ready.is_set():
                    pytest.fail(f"CLI exited before readiness: {result!r}")
                return ready.is_set()

            wait_until(cli_ready)
            assert len(handles) == 1
            assert len(sessions) == 1
            status, _, body = request(handles[0])
            assert status == 200
            assert json.loads(body)["status"]["state"] == "connected"
            stop.set()
            runner.join(timeout=WAIT_TIMEOUT)
            assert not runner.is_alive()
            assert not failures
            assert result == [True]
            assert not handles[0].is_running
            assert_session_stopped(sessions[0])
        finally:
            stop.set()
            for handle in handles:
                handle.close()
            for session in sessions:
                session.close()
            if runner is not None:
                runner.join(timeout=WAIT_TIMEOUT)
                assert not runner.is_alive()


def test_run_client_gateway_collision_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A real HTTP bind failure returns False and disconnects the CLI client."""
    real_factory = web_gateway.start_gateway
    sessions: list[client.ClientSession] = []
    ready = threading.Event()
    handles: list[web_gateway.WebGateway] = []

    def capture_factory(
        session: client.ClientSession,
        **kwargs: Any,
    ) -> web_gateway.WebGateway:
        """Retain the authenticated session even when HTTP startup raises."""
        sessions.append(session)
        handle = real_factory(session, **kwargs)
        handles.append(handle)
        return handle

    monkeypatch.setattr(web_gateway, "start_gateway", capture_factory)

    with live_server(9464, 9465):
        observer = new_session("bob", 9464, 9465, tmp_path / "observer")
        blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            blocker.bind(("127.0.0.1", 0))
            blocker.listen(1)
            occupied_port = int(blocker.getsockname()[1])
            assert observer.start(), observer.get_state()["status"]

            result = client._run_client_impl(
                username="alice",
                password="pass123",
                host="127.0.0.1",
                port=9464,
                server_udp_port=9465,
                interactive=False,
                download_dir=tmp_path / "collision-alice",
                ready_evt=ready,
                web_port=occupied_port,
            )
            assert result is False
            assert not ready.is_set()
            assert len(sessions) == 1
            assert not handles
            assert_session_stopped(sessions[0])
            wait_until(lambda: observer.get_state()["users"] == ["bob"])
            users = observer.refresh_users()
            names = {
                user["username"] if isinstance(user, dict) else user
                for user in users
            }
            assert names == {"bob"}
        finally:
            blocker.close()
            for handle in handles:
                handle.close()
            try:
                for session in sessions:
                    session.close()
            finally:
                observer.close()
