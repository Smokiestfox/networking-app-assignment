"""Integration tests for authenticated UDP endpoint protection."""

from __future__ import annotations

import socket
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from protocol import (
    ACK,
    FILE_CHUNK,
    FILE_META,
    HEARTBEAT,
    MAX_TEXT_LEN,
    MSG,
    USER_LIST,
    Packet,
    build_udp,
    pack_ack,
    pack_file_chunk,
    pack_file_meta,
    pack_heartbeat,
    pack_msg,
    parse_udp,
    recv_json_frame,
    send_json_frame,
)
from registry import UserRegistry
from server import CollaborationServer


HOST = "127.0.0.1"
IO_TIMEOUT = 2.0
REJECTION_WINDOW = 0.2
CHECKSUM = "0" * 64
FORWARDED_TYPES = frozenset({MSG, FILE_META, FILE_CHUNK, ACK})

StateSnapshot = tuple[dict[str, float], dict[str, tuple[str, int]]]


def wait_until(
    predicate: Callable[[], bool],
    timeout: float = IO_TIMEOUT,
) -> None:
    """Wait for asynchronous server state using a bounded deadline."""
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError("Timed out waiting for server state")
        time.sleep(0.005)


class ServerHarness:
    """Own one server and every client socket created by a test."""

    def __init__(self, tcp_port: int) -> None:
        """Configure an isolated in-memory registry and dedicated server ports."""
        assert 9400 <= tcp_port < 9440
        self.registry = UserRegistry(users_file=None)
        self.registry.user_db = {
            "alice": "secret",
            "bob": "secret",
            "carol": "secret",
        }
        self.server = CollaborationServer(
            host=HOST,
            port=tcp_port,
            udp_port=tcp_port + 1,
            log_file=None,
            registry=self.registry,
            client_timeout=60.0,
        )
        self.clients: list[socket.socket] = []
        self.run_error: Exception | None = None
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        """Capture startup failures without leaking thread exceptions."""
        try:
            self.server.run()
        except Exception as exc:
            self.run_error = exc

    def start(self) -> None:
        """Start the server and wait until both listening sockets are bound."""
        self.thread.start()

        def ready() -> bool:
            if self.run_error is not None:
                raise AssertionError("Server failed to start") from self.run_error
            try:
                return (
                    self.thread.is_alive()
                    and self.server.sock.getsockname()[1] == self.server.port
                    and self.server.udp_sock.getsockname()[1]
                    == self.server.udp_port
                )
            except OSError:
                return False

        wait_until(ready)

    def close(self) -> None:
        """Close clients, stop the server, and join its main thread."""
        try:
            for client in self.clients:
                try:
                    client.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                finally:
                    client.close()
        finally:
            try:
                self.server.stop()
            finally:
                if self.thread.ident is not None:
                    self.thread.join(timeout=1.0)

    def udp(self) -> socket.socket:
        """Create and track a UDP client bound to an ephemeral local port."""
        client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.clients.append(client)
        client.bind((HOST, 0))
        client.settimeout(IO_TIMEOUT)
        return client

    def tcp(self) -> socket.socket:
        """Create a tracked TCP client and verify the HELLO handshake."""
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.clients.append(client)
        client.settimeout(IO_TIMEOUT)
        client.connect((HOST, self.server.port))
        send_json_frame(client, {"type": "HELLO"})
        assert recv_json_frame(client) == {"type": "ACK", "status": "OK"}
        return client

    def auth(
        self,
        client: socket.socket,
        username: str,
        **fields: Any,
    ) -> dict[str, Any]:
        """Send AUTH and return its response, including explicit invalid fields."""
        send_json_frame(
            client,
            {
                "type": "AUTH",
                "username": username,
                "password": "secret",
                **fields,
            },
        )
        response = recv_json_frame(client)
        assert isinstance(response, dict)
        return response

    def login(
        self,
        username: str,
        udp: socket.socket | None = None,
    ) -> socket.socket:
        """Authenticate a user with an optional bound UDP endpoint."""
        client = self.tcp()
        fields = {} if udp is None else {"udp_port": udp.getsockname()[1]}
        assert self.auth(client, username, **fields)["type"] == "AUTH_OK"
        return client

    def send(self, client: socket.socket, datagram: bytes) -> None:
        """Send one datagram to this server."""
        client.sendto(datagram, (HOST, self.server.udp_port))

    def snapshot(self) -> StateSnapshot:
        """Copy both endpoint/liveness tables under their owning locks."""
        with self.server.last_seen_lock:
            last_seen = dict(self.server.last_seen)
        with self.server.udp_addrs_lock:
            udp_addrs = dict(self.server.udp_addrs)
        return last_seen, udp_addrs

    def assert_state_unchanged(
        self,
        expected: StateSnapshot,
        duration: float = REJECTION_WINDOW,
    ) -> None:
        """Check that rejected traffic never creates or overwrites UDP state."""
        deadline = time.monotonic() + duration
        while True:
            assert self.snapshot() == expected
            if time.monotonic() >= deadline:
                return
            time.sleep(0.005)


@pytest.fixture
def server_factory() -> Iterator[Callable[[int], ServerHarness]]:
    """Clean up every server, including instances whose startup fails."""
    instances: list[ServerHarness] = []

    def create(tcp_port: int) -> ServerHarness:
        harness = ServerHarness(tcp_port)
        instances.append(harness)
        harness.start()
        return harness

    try:
        yield create
    finally:
        for harness in reversed(instances):
            harness.close()


def receive_type(
    client: socket.socket,
    expected_type: int,
) -> tuple[Packet, Any]:
    """Receive an expected packet while ignoring asynchronous USER_LIST updates."""
    previous_timeout = client.gettimeout()
    deadline = time.monotonic() + IO_TIMEOUT
    try:
        while True:
            remaining = deadline - time.monotonic()
            assert remaining > 0, f"Packet type {expected_type} was not received"
            client.settimeout(remaining)
            raw, _ = client.recvfrom(65535)
            packet, body = parse_udp(raw)
            if packet.ptype == USER_LIST:
                continue
            assert packet.ptype == expected_type
            return packet, body
    finally:
        client.settimeout(previous_timeout)


def assert_no_forwarded(
    client: socket.socket,
    duration: float = REJECTION_WINDOW,
) -> None:
    """Allow USER_LIST updates but reject any forwarded chat or file packet."""
    previous_timeout = client.gettimeout()
    deadline = time.monotonic() + duration
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            client.settimeout(remaining)
            try:
                raw, _ = client.recvfrom(65535)
            except socket.timeout:
                return
            packet, _ = parse_udp(raw)
            assert packet.ptype not in FORWARDED_TYPES
            assert packet.ptype == USER_LIST
    finally:
        client.settimeout(previous_timeout)


def file_meta(msg_id: uuid.UUID, seq: int = 1) -> bytes:
    """Build a small valid Alice-to-Bob file announcement."""
    return pack_file_meta(
        to="bob",
        sender="alice",
        filename="example.bin",
        size=1,
        chunks=1,
        checksum=CHECKSUM,
        seq=seq,
        msg_id=msg_id,
    )


def prepare_alice_source(
    harness: ServerHarness,
    scenario: str,
) -> socket.socket:
    """Create the source endpoint required by a spoofing scenario."""
    genuine = harness.udp()

    if scenario == "offline":
        return genuine

    if scenario == "tcp_only":
        harness.login("alice")
        return genuine

    if scenario == "wrong_ip":
        port = genuine.getsockname()[1]
        assert harness.registry.register(
            "alice",
            "10.0.0.5",
            50001,
            udp_port=port,
        )
        with harness.server.last_seen_lock:
            harness.server.last_seen["alice"] = time.time() - 1.0
        with harness.server.udp_addrs_lock:
            harness.server.udp_addrs["alice"] = ("10.0.0.5", port)
        return genuine

    harness.login("alice", genuine)

    if scenario == "wrong_port":
        return harness.udp()

    if scenario == "claimed_mismatch":
        carol = harness.udp()
        harness.login("carol", carol)
        return carol

    return genuine


def test_tcp_auth_and_valid_udp_traffic(
    server_factory: Callable[[int], ServerHarness],
) -> None:
    """HELLO/AUTH bind endpoints used for heartbeat, unicast, and broadcast."""
    harness = server_factory(9400)
    alice, bob, carol = harness.udp(), harness.udp(), harness.udp()
    alice_tcp = harness.tcp()
    alice_port = alice.getsockname()[1]

    response = harness.auth(alice_tcp, "alice", udp_port=alice_port)
    assert response["type"] == "AUTH_OK"
    assert response["status"] == "OK"
    assert response["user"] == "alice"

    with harness.registry.lock:
        binding = dict(harness.registry.active_users["alice"])
    assert (binding["ip"], binding["udp_port"]) == (HOST, alice_port)

    harness.login("bob", bob)
    harness.login("carol", carol)

    with harness.server.last_seen_lock:
        harness.server.last_seen["alice"] = time.time() - 1.0
    with harness.server.udp_addrs_lock:
        harness.server.udp_addrs.pop("alice", None)

    before_send = time.time()
    harness.send(alice, pack_heartbeat(username="alice", seq=1))
    wait_until(
        lambda: harness.snapshot()[0].get("alice", 0.0) >= before_send
        and harness.snapshot()[1].get("alice") == (HOST, alice_port)
    )

    harness.send(
        alice,
        pack_msg(to="bob", sender="alice", text="Private message", seq=2),
    )
    _, body = receive_type(bob, MSG)
    assert body["from"] == "alice"
    assert body["to"] == "bob"
    assert body["text"] == "Private message"
    assert_no_forwarded(carol)
    assert_no_forwarded(alice)

    harness.send(
        alice,
        pack_msg(to="*", sender="alice", text="Broadcast message", seq=3),
    )
    for receiver in (bob, carol):
        _, body = receive_type(receiver, MSG)
        assert body["from"] == "alice"
        assert body["to"] == "*"
        assert body["text"] == "Broadcast message"
    assert_no_forwarded(alice)


@pytest.mark.parametrize(
    "udp_port",
    [0, -1, 65536, "50001", True, False],
    ids=["zero", "negative", "overflow", "string", "true", "false"],
)
def test_auth_rejects_invalid_udp_port(
    server_factory: Callable[[int], ServerHarness],
    udp_port: Any,
) -> None:
    """Invalid AUTH ports never create active users or UDP state."""
    harness = server_factory(9402)
    response = harness.auth(harness.tcp(), "alice", udp_port=udp_port)

    assert response["type"] == "AUTH_FAIL"
    assert response["status"] == "ERROR"
    assert not harness.registry.is_active("alice")
    assert "alice" not in harness.snapshot()[0]
    assert "alice" not in harness.snapshot()[1]


def test_registry_registration_result_codes() -> None:
    """Direct registration validates ports and atomically reserves endpoints."""
    registry = UserRegistry(users_file=None)

    for invalid in (0, -1, 65536, "50001", True, False):
        result = registry.register(
            "alice", HOST, 50000, udp_port=invalid
        )
        assert not result
        assert result.message == "INVALID_UDP_PORT"
        assert not registry.is_active("alice")

    result = registry.register("alice", HOST, 50000, udp_port=50001)
    assert result
    assert result.message == "OK"

    duplicate = registry.register("alice", HOST, 50002, udp_port=50003)
    assert not duplicate
    assert duplicate.message == "ALREADY_LOGGED_IN"

    collision = registry.register("bob", HOST, 50004, udp_port=50001)
    assert not collision
    assert collision.message == "UDP_ENDPOINT_IN_USE"
    assert not registry.is_active("bob")

    with registry.lock:
        assert registry.active_users["alice"]["udp_port"] == 50001
        assert registry.active_users["alice"]["port"] == 50000


def test_auth_endpoint_collision_and_duplicate_auth(
    server_factory: Callable[[int], ServerHarness],
) -> None:
    """Rejected AUTH attempts cannot hijack or remove Alice's live session."""
    harness = server_factory(9404)
    alice = harness.udp()
    alice_tcp = harness.login("alice", alice)
    alice_port = alice.getsockname()[1]
    expected = harness.snapshot()

    with harness.registry.lock:
        original = dict(harness.registry.active_users["alice"])

    bob_tcp = harness.tcp()
    response = harness.auth(bob_tcp, "bob", udp_port=alice_port)
    assert response["type"] == "AUTH_FAIL"
    assert not harness.registry.is_active("bob")

    send_json_frame(bob_tcp, {"type": "BYE"})
    assert recv_json_frame(bob_tcp) == {"type": "ACK", "status": "BYE"}
    # EOF confirms the rejected connection completed its cleanup path.
    assert recv_json_frame(bob_tcp) is None

    with harness.registry.lock:
        assert harness.registry.active_users["alice"] == original
    assert harness.snapshot() == expected

    response = harness.auth(alice_tcp, "bob", udp_port=alice_port)
    assert response["type"] == "AUTH_FAIL"
    assert not harness.registry.is_active("bob")
    with harness.registry.lock:
        assert harness.registry.active_users["alice"] == original
    assert harness.snapshot() == expected

    before_send = time.time()
    harness.send(alice, pack_heartbeat(username="alice", seq=1))
    wait_until(
        lambda: harness.snapshot()[0].get("alice", 0.0) >= before_send
    )
    assert harness.server.get_user_udp_addr("alice") == (HOST, alice_port)


@pytest.mark.parametrize(
    "scenario",
    [
        "offline",
        "tcp_only",
        "wrong_port",
        "wrong_ip",
        "claimed_mismatch",
        "empty_username",
        "numeric_username",
    ],
)
def test_rejected_heartbeat_preserves_state(
    server_factory: Callable[[int], ServerHarness],
    scenario: str,
) -> None:
    """Invalid or spoofed HEARTBEAT packets cannot refresh endpoint state."""
    harness = server_factory(9406)
    source = prepare_alice_source(harness, scenario)
    username: Any = "alice"
    if scenario == "empty_username":
        username = ""
    elif scenario == "numeric_username":
        username = 123

    expected = harness.snapshot()
    harness.send(
        source,
        build_udp(HEARTBEAT, {"username": username}, seq=1),
    )
    harness.assert_state_unchanged(expected)


@pytest.mark.parametrize(
    "scenario",
    [
        "offline",
        "tcp_only",
        "wrong_port",
        "wrong_ip",
        "claimed_mismatch",
        "blank_to",
        "whitespace_to",
        "nan_time",
        "positive_infinity_time",
        "negative_infinity_time",
        "true_time",
        "false_time",
        "empty_text",
        "over_max_text",
    ],
)
def test_rejected_message_preserves_state_and_is_not_forwarded(
    server_factory: Callable[[int], ServerHarness],
    scenario: str,
) -> None:
    """Spoofed endpoints and malformed MSG fields are rejected before refresh."""
    harness = server_factory(9408)
    bob = harness.udp()
    harness.login("bob", bob)
    source = prepare_alice_source(harness, scenario)

    body: dict[str, Any] = {
        "to": "bob",
        "from": "alice",
        "text": "Must not be forwarded",
        "time": time.time(),
    }
    if scenario == "blank_to":
        body["to"] = ""
    elif scenario == "whitespace_to":
        body["to"] = "   "
    elif scenario == "empty_text":
        body["text"] = ""
    elif scenario == "over_max_text":
        body["text"] = "a" * (MAX_TEXT_LEN + 1)
    else:
        invalid_times = {
            "nan_time": float("nan"),
            "positive_infinity_time": float("inf"),
            "negative_infinity_time": float("-inf"),
            "true_time": True,
            "false_time": False,
        }
        if scenario in invalid_times:
            body["time"] = invalid_times[scenario]

    expected = harness.snapshot()
    harness.send(source, build_udp(MSG, body, seq=1))
    assert_no_forwarded(bob)
    harness.assert_state_unchanged(expected)


def test_file_relay_rejects_unregistered_endpoints(
    server_factory: Callable[[int], ServerHarness],
) -> None:
    """Wrong-port FILE_META, FILE_CHUNK, and ACK packets cannot affect relay."""
    harness = server_factory(9410)
    alice, bob = harness.udp(), harness.udp()
    harness.login("alice", alice)
    harness.login("bob", bob)
    wrong_port, outsider = harness.udp(), harness.udp()

    rejected_ids = [uuid.uuid4(), uuid.uuid4()]
    expected = harness.snapshot()
    for source, msg_id in zip((wrong_port, outsider), rejected_ids):
        harness.send(source, file_meta(msg_id))
    assert_no_forwarded(bob)
    harness.assert_state_unchanged(expected)
    with harness.server.file_sessions_lock:
        assert not harness.server.file_sessions

    msg_id = uuid.uuid4()
    harness.send(alice, file_meta(msg_id, seq=10))
    packet, body = receive_type(bob, FILE_META)
    assert packet.msg_id == msg_id
    assert body["from"] == "alice"

    harness.send(
        bob,
        pack_ack(ack_seq=10, seq=20, msg_id=msg_id),
    )
    packet, _ = receive_type(alice, ACK)
    assert packet.msg_id == msg_id
    assert packet.ack == 10

    harness.send(
        alice,
        pack_file_chunk(
            msg_id=msg_id,
            chunk=0,
            total=1,
            data=b"x",
            seq=11,
        ),
    )
    packet, body = receive_type(bob, FILE_CHUNK)
    assert packet.msg_id == msg_id
    assert body == b"x"

    # Wait for counters updated immediately after each sendto().
    def relay_counters_updated() -> bool:
        with harness.server.file_sessions_lock:
            session = harness.server.file_sessions[msg_id]
            return session.to_receiver == 2 and session.to_sender == 1

    wait_until(relay_counters_updated)

    with harness.server.file_sessions_lock:
        session = harness.server.file_sessions[msg_id]
        relay_state = (
            session.last_activity,
            session.to_receiver,
            session.to_sender,
            session.sender_addr,
            dict(session.receiver_addrs),
        )
    expected = harness.snapshot()

    for source in (wrong_port, outsider):
        harness.send(source, file_meta(msg_id, seq=30))
        harness.send(
            source,
            pack_file_chunk(
                msg_id=msg_id,
                chunk=0,
                total=1,
                data=b"!",
                seq=31,
            ),
        )
        harness.send(
            source,
            pack_ack(ack_seq=11, seq=32, msg_id=msg_id),
        )

    assert_no_forwarded(bob)
    assert_no_forwarded(alice)
    harness.assert_state_unchanged(expected)

    with harness.server.file_sessions_lock:
        session = harness.server.file_sessions[msg_id]
        assert (
            session.last_activity,
            session.to_receiver,
            session.to_sender,
            session.sender_addr,
            dict(session.receiver_addrs),
        ) == relay_state
        assert all(
            rejected_id not in harness.server.file_sessions
            for rejected_id in rejected_ids
        )


def test_udp_replay_after_bye_is_rejected(
    server_factory: Callable[[int], ServerHarness],
) -> None:
    """BYE revokes the old UDP endpoint for heartbeat, chat, and file traffic."""
    harness = server_factory(9412)
    alice, bob = harness.udp(), harness.udp()
    alice_tcp = harness.login("alice", alice)
    harness.login("bob", bob)

    msg_id = uuid.uuid4()
    harness.send(alice, file_meta(msg_id))
    receive_type(bob, FILE_META)

    send_json_frame(alice_tcp, {"type": "BYE"})
    assert recv_json_frame(alice_tcp) == {"type": "ACK", "status": "BYE"}
    assert recv_json_frame(alice_tcp) is None

    def logged_out() -> bool:
        last_seen, udp_addrs = harness.snapshot()
        with harness.server.file_sessions_lock:
            session_closed = msg_id not in harness.server.file_sessions
        return (
            not harness.registry.is_active("alice")
            and "alice" not in last_seen
            and "alice" not in udp_addrs
            and session_closed
        )

    wait_until(logged_out)
    expected = harness.snapshot()
    replay_meta_id = uuid.uuid4()

    replay_packets = (
        pack_heartbeat(username="alice", seq=40),
        pack_msg(to="bob", sender="alice", text="Replay", seq=41),
        file_meta(replay_meta_id, seq=42),
        file_meta(msg_id, seq=43),
        pack_file_chunk(
            msg_id=msg_id,
            chunk=0,
            total=1,
            data=b"x",
            seq=44,
        ),
        pack_ack(ack_seq=1, seq=45, msg_id=msg_id),
    )
    for datagram in replay_packets:
        harness.send(alice, datagram)

    assert_no_forwarded(bob)
    assert_no_forwarded(alice)
    harness.assert_state_unchanged(expected)
    assert harness.server.get_user_udp_addr("alice") is None
    with harness.server.file_sessions_lock:
        assert msg_id not in harness.server.file_sessions
        assert replay_meta_id not in harness.server.file_sessions

