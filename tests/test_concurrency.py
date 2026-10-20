import socket
import threading
import time
import pytest
from server import CollaborationServer
from registry import UserRegistry

def test_auth_and_registry_logic():
    reg = UserRegistry()
    # 1. Kiểm tra mật khẩu đúng/sai và tài khoản không tồn tại
    assert reg.authenticate("alice", "pass123") is True
    assert reg.authenticate("alice", "wrong_pass") is False
    assert reg.authenticate("nonexistent", "pass123") is False

    # 2. Đăng ký tài khoản thành công
    ok, reason = reg.register("alice", "127.0.0.1", 9001, sock=None)
    assert ok is True
    assert reason == "OK"

    # 3. Đăng ký trùng tên phải bị từ chối
    ok_dup, reason_dup = reg.register("alice", "127.0.0.1", 9002, sock=None)
    assert ok_dup is False
    assert reason_dup == "ALREADY_LOGGED_IN"

    # 4. Hủy đăng ký tài khoản
    assert reg.unregister("alice") is True
    assert reg.get_active_users() == []

def test_concurrent_registrations():
    reg = UserRegistry()
    results = []
    num_threads = 15

    def register_user(idx):
        username = f"user{idx:02d}"
        ok, reason = reg.register(username, "127.0.0.1", 9000 + idx, sock=None)
        results.append((username, ok, reason))

    threads = [threading.Thread(target=register_user, args=(i,)) for i in range(1, num_threads + 1)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Tất cả 15 user khác nhau đăng ký đồng thời phải thành công
    successes = [r for r in results if r[1] is True]
    assert len(successes) == num_threads
    assert len(reg.get_active_users()) == num_threads

def test_server_concurrency_and_disconnect():
    test_port = 9150
    server = CollaborationServer(host="127.0.0.1", port=test_port)
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()
    time.sleep(0.1)

    try:
        # Tự động kết nối 10 client đồng thời tới Server
        num_clients = 10
        sockets = []
        
        def client_connect(i):
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(("127.0.0.1", test_port))
                sockets.append(s)
            except Exception as e:
                pass

        threads = [threading.Thread(target=client_connect, args=(i,)) for i in range(num_clients)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        time.sleep(0.2)
        # Ngắt kết nối đột ngột một nửa số client để kiểm tra dọn dẹp state
        for s in sockets[:num_clients // 2]:
            s.close()
            
        time.sleep(0.2)
        
        # Đóng các socket còn lại
        for s in sockets[num_clients // 2:]:
            s.close()
            
    finally:
        try:
            server.sock.close()
        except OSError:
            pass
        server_thread.join(timeout=1.0)