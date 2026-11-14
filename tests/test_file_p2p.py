import hashlib
import threading
import time

import pytest

from client import ClientSession
from protocol import ACK, FILE_CHUNK, FILE_META, parse_udp
from server import CollaborationServer


@pytest.mark.parametrize("loss, outcome", [(0, "transfer"), (0.15, "transfer"),
                                            (0, "reject"), (0, "timeout"), (0, "disconnect")])
def test_direct_file_transfer(tmp_path, monkeypatch, loss, outcome):
    if outcome == "timeout":
        monkeypatch.setattr("file_rendezvous.FILE_SESSION_TIMEOUT", 0.1)
    server = CollaborationServer(host="127.0.0.1", port=0, udp_port=0,
                                 log_file=str(tmp_path / "server.log"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 2
    while True:
        try:
            udp_port = server.udp_sock.getsockname()[1]
            if udp_port:
                break
        except OSError:
            if time.monotonic() >= deadline:
                raise
        time.sleep(0.01)
    tcp_port = server.sock.getsockname()[1]
    alice, bob = [
        ClientSession(name, password, "127.0.0.1", tcp_port, udp_port,
                      download_dir=tmp_path / name, simulate_loss=loss, loss_seed=42)
        for name, password in (("alice", "pass123"), ("bob", "pass123"))
    ]
    endpoints = []
    try:
        for session in (alice, bob):
            send_udp = session._send_udp

            def record(raw, address=None, send=send_udp, client=session):
                ptype = parse_udp(raw)[0].ptype
                if ptype in (FILE_META, FILE_CHUNK, ACK):
                    endpoints.append((client.username, ptype, address))
                send(raw, address)

            session._send_udp = record
            assert session.start(), session.get_state()
        if outcome != "transfer":
            send_control = bob._transfers.send_control

            def decline(body):
                if body["type"] == "FILE_ACCEPT":
                    if outcome != "reject":
                        return
                    body = {**body, "type": "FILE_REJECT"}
                send_control(body)

            bob._transfers.send_control = decline
        source = tmp_path / "example.bin"
        source.write_bytes(bytes(range(256)) * 32)
        mid = alice.send_file("bob", source)
        if outcome == "disconnect":
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                with server.file_rendezvous.lock:
                    if mid in server.file_rendezvous.pending:
                        break
                time.sleep(0.01)
            else:
                pytest.fail("Offer never became pending")
            bob.close()
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            sent = next((t for t in alice.get_state()["transfers"] if t["id"] == mid), None)
            if sent and sent["status"] in ("completed", "failed"):
                break
            time.sleep(0.025)
        if outcome != "transfer":
            assert sent and sent["status"] == "failed", sent
            assert sent["error"] in {
                "reject": {"Peer declined offer"},
                "timeout": {"Offer timed out"},
                "disconnect": {"Peer disconnected", "Peer 'bob' went offline"},
            }[outcome]
            assert not endpoints
            with server.file_rendezvous.lock:
                assert not server.file_rendezvous.pending
            return
        assert sent and sent["status"] == "completed", sent
        received, = bob.download_dir.glob('*_example.bin')
        assert hashlib.sha256(received.read_bytes()).digest() == hashlib.sha256(source.read_bytes()).digest()
        assert {ptype for _, ptype, _ in endpoints} == {FILE_META, FILE_CHUNK, ACK}
        for username, _, address in endpoints:
            peer = bob if username == "alice" else alice
            assert address == ("127.0.0.1", peer._udp.getsockname()[1])
    finally:
        alice.close()
        bob.close()
        server.stop()
        thread.join(2)
