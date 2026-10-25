from server import CollaborationServer


class ResetOnceUdpSocket:
    """Simulate Windows reporting an ICMP port-unreachable on UDP receive."""

    def __init__(self, server):
        self.server = server
        self.calls = 0

    def recvfrom(self, _buffer_size):
        self.calls += 1
        if self.calls == 1:
            raise ConnectionResetError(10054, "An existing connection was forcibly closed by the remote host")

        # Prove udp_loop continued after the reset, then stop it cleanly.
        self.server.running = False
        raise OSError(10038, "Socket operation on non-socket")


def test_udp_loop_logs_connection_reset_and_continues(tmp_path, capsys):
    log_file = tmp_path / "server.log"
    server = CollaborationServer(
        host="127.0.0.1",
        port=0,
        udp_port=0,
        log_file=str(log_file),
        users_file=None,
    )
    fake_socket = ResetOnceUdpSocket(server)
    server.udp_sock.close()
    server.udp_sock = fake_socket
    server.running = True

    try:
        server.udp_loop()
    finally:
        server.running = False
        server.sock.close()

    output = capsys.readouterr().out
    logged = log_file.read_text(encoding="utf-8")
    expected = "UDP ConnectionResetError: [Errno 10054]"
    assert fake_socket.calls == 2
    assert expected in output
    assert expected in logged
    assert "continuing recv loop" in logged


def test_udp_address_does_not_fall_back_to_tcp_port():
    server = CollaborationServer(
        host="127.0.0.1",
        port=0,
        udp_port=0,
        log_file=None,
        users_file=None,
    )
    try:
        assert server.registry.register("alice", "127.0.0.1", 63554)
        assert server.get_user_udp_addr("alice") is None

        # An explicitly supplied UDP port remains usable before a heartbeat.
        assert server.registry.register("bob", "127.0.0.1", 63555, udp_port=61814)
        assert server.get_user_udp_addr("bob") == ("127.0.0.1", 61814)
    finally:
        server.sock.close()
        server.udp_sock.close()
