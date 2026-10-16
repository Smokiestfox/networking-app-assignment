import socket
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import SERVER_HOST, TCP_PORT
from protocol import send_json_frame, recv_json_frame
from server import CollaborationServer


def test_hello_ack():
    test_port = 9101
    server = CollaborationServer(host="127.0.0.1", port=test_port)
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()
    time.sleep(0.1)

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("127.0.0.1", test_port))
        
        send_json_frame(sock, {"type": "HELLO"})
        response = recv_json_frame(sock)
        
        assert response is not None
        assert response.get("type") == "ACK"
        assert response.get("status") == "OK"
        sock.close()
    finally:
        try:
            server.sock.close()
        except OSError:
            pass
        server_thread.join(timeout=1.0)


def test_sequential_clients():
    test_port = 9102
    server = CollaborationServer(host="127.0.0.1", port=test_port)
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()
    time.sleep(0.1)

    try:
        for i in range(3):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(("127.0.0.1", test_port))
            
            send_json_frame(sock, {"type": "HELLO", "client_id": f"client_{i+1}"})
            response = recv_json_frame(sock)
            
            assert response is not None
            assert response.get("type") == "ACK"
            assert response.get("status") == "OK"
            sock.close()
            time.sleep(0.05)
    finally:
        try:
            server.sock.close()
        except OSError:
            pass
        server_thread.join(timeout=1.0)


def test_abrupt_disconnect():
    test_port = 9103
    server = CollaborationServer(host="127.0.0.1", port=test_port)
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()
    time.sleep(0.1)

    try:
        sock1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock1.connect(("127.0.0.1", test_port))
        sock1.close()
        time.sleep(0.05)

        sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock2.connect(("127.0.0.1", test_port))
        send_json_frame(sock2, {"type": "HELLO"})
        response = recv_json_frame(sock2)
        
        assert response is not None
        assert response.get("type") == "ACK"
        sock2.close()
    finally:
        try:
            server.sock.close()
        except OSError:
            pass
        server_thread.join(timeout=1.0)


def test_auth_success():
    test_port = 9104
    server = CollaborationServer(host="127.0.0.1", port=test_port)
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()
    time.sleep(0.1)

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("127.0.0.1", test_port))

        send_json_frame(sock, {"type": "AUTH", "username": "alice", "password": "123456"})
        response = recv_json_frame(sock)

        assert response is not None
        assert response.get("type") == "AUTH_OK"
        assert response.get("status") == "OK"
        assert response.get("user") == "alice"
        assert any(u["username"] == "alice" for u in response.get("users", []))
        assert server.registry.is_active("alice") is True

        # Gửi BYE để logout an toàn
        send_json_frame(sock, {"type": "BYE"})
        time.sleep(0.1)
        assert server.registry.is_active("alice") is False
        sock.close()
    finally:
        try:
            server.sock.close()
        except OSError:
            pass
        server_thread.join(timeout=1.0)


def test_auth_failure():
    test_port = 9105
    server = CollaborationServer(host="127.0.0.1", port=test_port)
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()
    time.sleep(0.1)

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("127.0.0.1", test_port))

        send_json_frame(sock, {"type": "AUTH", "username": "alice", "password": "wrong_password"})
        response = recv_json_frame(sock)

        assert response is not None
        assert response.get("type") == "AUTH_FAIL"
        assert server.registry.is_active("alice") is False
        sock.close()
    finally:
        try:
            server.sock.close()
        except OSError:
            pass
        server_thread.join(timeout=1.0)


def test_auth_duplicate_login():
    test_port = 9106
    server = CollaborationServer(host="127.0.0.1", port=test_port)
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()
    time.sleep(0.1)

    try:
        sock1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock1.connect(("127.0.0.1", test_port))
        send_json_frame(sock1, {"type": "AUTH", "username": "bob", "password": "123456"})
        resp1 = recv_json_frame(sock1)
        assert resp1.get("type") == "AUTH_OK"

        sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock2.connect(("127.0.0.1", test_port))
        send_json_frame(sock2, {"type": "AUTH", "username": "bob", "password": "123456"})
        resp2 = recv_json_frame(sock2)
        assert resp2.get("type") == "AUTH_FAIL"

        sock1.close()
        sock2.close()
    finally:
        try:
            server.sock.close()
        except OSError:
            pass
        server_thread.join(timeout=1.0)


def test_abrupt_disconnect_unregisters_user():
    test_port = 9107
    server = CollaborationServer(host="127.0.0.1", port=test_port)
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()
    time.sleep(0.1)

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("127.0.0.1", test_port))
        send_json_frame(sock, {"type": "AUTH", "username": "charlie", "password": "123456"})
        resp = recv_json_frame(sock)
        assert resp.get("type") == "AUTH_OK"
        assert server.registry.is_active("charlie") is True

        # Ngắt đột ngột bằng close socket
        sock.close()
        time.sleep(0.2)
        assert server.registry.is_active("charlie") is False
    finally:
        try:
            server.sock.close()
        except OSError:
            pass
        server_thread.join(timeout=1.0)


def test_concurrent_multithreading():
    test_port = 9108
    server = CollaborationServer(host="127.0.0.1", port=test_port)
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()
    time.sleep(0.1)

    try:
        def client_worker(client_id):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(("127.0.0.1", test_port))
            send_json_frame(s, {"type": "HELLO", "client_id": f"worker_{client_id}"})
            r = recv_json_frame(s)
            assert r.get("type") == "ACK"
            s.close()

        threads = [threading.Thread(target=client_worker, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=2.0)
    finally:
        try:
            server.sock.close()
        except OSError:
            pass
        server_thread.join(timeout=1.0)


if __name__ == "__main__":
    print("========================================")
    print("TEST 1: HELLO / ACK Handshake")
    print("========================================")
    test_hello_ack()
    print("-> PASS: Client sent HELLO, Server replied ACK\n")

    print("========================================")
    print("TEST 2: Multiple Sequential Clients")
    print("========================================")
    test_sequential_clients()
    print("-> PASS: Handled 3 sequential clients\n")

    print("========================================")
    print("TEST 3: Abrupt Disconnect Recovery")
    print("========================================")
    test_abrupt_disconnect()
    print("-> PASS: Server recovered and served next client\n")

    print("========================================")
    print("TEST 4: AUTH Lifecycle & Active Users")
    print("========================================")
    test_auth_success()
    print("-> PASS: AUTH_OK, user listed in active users, unregister on BYE\n")

    print("========================================")
    print("TEST 5: AUTH Fail on Wrong Password")
    print("========================================")
    test_auth_failure()
    print("-> PASS: Rejected wrong password with AUTH_FAIL\n")

    print("========================================")
    print("TEST 6: Prevent Duplicate Login (Atomic)")
    print("========================================")
    test_auth_duplicate_login()
    print("-> PASS: Second login rejected with AUTH_FAIL\n")

    print("========================================")
    print("TEST 7: Abrupt Disconnect Unregisters User")
    print("========================================")
    test_abrupt_disconnect_unregisters_user()
    print("-> PASS: User automatically cleaned up on abrupt disconnect\n")

    print("========================================")
    print("TEST 8: Multi-threaded Concurrent Clients")
    print("========================================")
    test_concurrent_multithreading()
    print("-> PASS: Handled 5 concurrent client threads simultaneously\n")

    print("========================================")
    print("ALL TESTS COMPLETED SUCCESSFULLY!")
    print("========================================")

