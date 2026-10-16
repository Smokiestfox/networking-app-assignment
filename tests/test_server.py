import socket
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import SERVER_HOST, TCP_PORT
from protocol import send_json_frame, recv_json_frame
from server import CollaborationServer


def test_login_success():
    """Test 1: Client gửi LOGIN -> Server phản hồi AUTH_OK kèm danh sách user."""
    test_port = 9101
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


def test_login_failure():
    """Test 2: Login thất bại do sai mật khẩu -> Server trả AUTH_FAIL."""
    test_port = 9102
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


def test_duplicate_login_rejected():
    """Test 3: User đã online -> login lần 2 bị từ chối (atomic register)."""
    test_port = 9103
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


def test_sequential_logins():
    """Test 4: 3 user đăng nhập tuần tự, mỗi user login rồi logout."""
    test_port = 9104
    users = [
        ("alice", "123456"),
        ("bob", "123456"),
        ("charlie", "123456"),
    ]
    server = CollaborationServer(host="127.0.0.1", port=test_port)
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()
    time.sleep(0.1)

    try:
        for username, password in users:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(("127.0.0.1", test_port))

            send_json_frame(sock, {"type": "AUTH", "username": username, "password": password})
            response = recv_json_frame(sock)

            assert response is not None
            assert response.get("type") == "AUTH_OK"
            assert response.get("user") == username

            send_json_frame(sock, {"type": "BYE"})
            time.sleep(0.1)
            assert server.registry.is_active(username) is False
            sock.close()
            time.sleep(0.05)
    finally:
        try:
            server.sock.close()
        except OSError:
            pass
        server_thread.join(timeout=1.0)


def test_abrupt_disconnect_unregisters_user():
    """Test 5: Client ngắt đột ngột -> server tự unregister user."""
    test_port = 9105
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

        # Ngắt đột ngột
        sock.close()
        time.sleep(0.2)
        assert server.registry.is_active("charlie") is False
    finally:
        try:
            server.sock.close()
        except OSError:
            pass
        server_thread.join(timeout=1.0)


def test_abrupt_disconnect_recovery():
    """Test 6: Client ngắt đột ngột -> server vẫn phục vụ client tiếp theo."""
    test_port = 9106
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
        send_json_frame(sock2, {"type": "AUTH", "username": "alice", "password": "123456"})
        response = recv_json_frame(sock2)

        assert response is not None
        assert response.get("type") == "AUTH_OK"
        sock2.close()
    finally:
        try:
            server.sock.close()
        except OSError:
            pass
        server_thread.join(timeout=1.0)


def test_concurrent_multithreading():
    """Test 7: 5 client kết nối đồng thời, mỗi client login thành công."""
    test_port = 9107
    users = [
        ("alice", "123456"),
        ("bob", "123456"),
        ("charlie", "123456"),
        ("admin", "admin"),
    ]
    server = CollaborationServer(host="127.0.0.1", port=test_port)
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()
    time.sleep(0.1)

    results = [None] * len(users)

    try:
        def client_worker(idx, username, password):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(("127.0.0.1", test_port))
            send_json_frame(s, {"type": "AUTH", "username": username, "password": password})
            r = recv_json_frame(s)
            results[idx] = r
            send_json_frame(s, {"type": "BYE"})
            time.sleep(0.05)
            s.close()

        threads = [
            threading.Thread(target=client_worker, args=(i, u, p))
            for i, (u, p) in enumerate(users)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=3.0)

        for i, (u, _) in enumerate(users):
            assert results[i] is not None, f"Client '{u}' got no response"
            assert results[i].get("type") == "AUTH_OK", f"Client '{u}' expected AUTH_OK, got {results[i]}"
    finally:
        try:
            server.sock.close()
        except OSError:
            pass
        server_thread.join(timeout=1.0)


def test_logout_msg_type():
    """Test 8: Client gửi LOGOUT (thay vì BYE) cũng được xử lý đúng."""
    test_port = 9108
    server = CollaborationServer(host="127.0.0.1", port=test_port)
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()
    time.sleep(0.1)

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("127.0.0.1", test_port))

        send_json_frame(sock, {"type": "LOGIN", "username": "alice", "password": "123456"})
        response = recv_json_frame(sock)
        assert response.get("type") == "AUTH_OK"
        assert server.registry.is_active("alice") is True

        send_json_frame(sock, {"type": "LOGOUT"})
        bye_resp = recv_json_frame(sock)
        assert bye_resp.get("type") == "ACK"
        assert bye_resp.get("status") == "BYE"
        time.sleep(0.1)
        assert server.registry.is_active("alice") is False
        sock.close()
    finally:
        try:
            server.sock.close()
        except OSError:
            pass
        server_thread.join(timeout=1.0)


if __name__ == "__main__":
    print("========================================")
    print("TEST 1: Login Success (AUTH -> AUTH_OK)")
    print("========================================")
    test_login_success()
    print("-> PASS: AUTH_OK received, user in active list, unregister on BYE\n")

    print("========================================")
    print("TEST 2: Login Failure (Wrong Password)")
    print("========================================")
    test_login_failure()
    print("-> PASS: Rejected with AUTH_FAIL\n")

    print("========================================")
    print("TEST 3: Duplicate Login Rejected")
    print("========================================")
    test_duplicate_login_rejected()
    print("-> PASS: Second login rejected with AUTH_FAIL\n")

    print("========================================")
    print("TEST 4: Sequential Logins (3 Users)")
    print("========================================")
    test_sequential_logins()
    print("-> PASS: 3 users logged in/out sequentially\n")

    print("========================================")
    print("TEST 5: Abrupt Disconnect Unregisters User")
    print("========================================")
    test_abrupt_disconnect_unregisters_user()
    print("-> PASS: User auto-cleaned on abrupt disconnect\n")

    print("========================================")
    print("TEST 6: Abrupt Disconnect Recovery")
    print("========================================")
    test_abrupt_disconnect_recovery()
    print("-> PASS: Server recovered and served next client\n")

    print("========================================")
    print("TEST 7: Concurrent Multi-threaded Logins")
    print("========================================")
    test_concurrent_multithreading()
    print("-> PASS: 4 concurrent clients logged in simultaneously\n")

    print("========================================")
    print("TEST 8: LOGIN/LOGOUT Message Types")
    print("========================================")
    test_logout_msg_type()
    print("-> PASS: LOGIN and LOGOUT handled correctly\n")

    print("========================================")
    print("ALL TESTS COMPLETED SUCCESSFULLY!")
    print("========================================")
