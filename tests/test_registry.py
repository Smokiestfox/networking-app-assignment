import threading
from registry import UserRegistry


def test_load_credentials():
    reg = UserRegistry()
    assert len(reg._credentials) >= 20
    assert "alice" in reg._credentials
    assert "bob" in reg._credentials
    assert "user01" in reg._credentials
    assert "user20" in reg._credentials


def test_authenticate():
    reg = UserRegistry()
    # Correct password
    assert reg.authenticate("alice", "pass123") is True
    assert reg.authenticate("user01", "pass123") is True
    # Wrong password
    assert reg.authenticate("alice", "wrong_password") is False
    # Non-existent user
    assert reg.authenticate("ghost_user", "pass123") is False


def test_register_and_unregister():
    reg = UserRegistry()
    # Initial state
    assert reg.get_active_users() == []

    # Register alice
    ok, reason = reg.register("alice", "127.0.0.1", 9001, sock=None)
    assert ok is True
    assert reason == "OK"
    assert reg.get_active_users() == ["alice"]

    # Verify session details
    session = reg.get_user_session("alice")
    assert session is not None
    assert session.username == "alice"
    assert session.ip == "127.0.0.1"
    assert session.tcp_port == 9001

    # Unregister alice
    assert reg.unregister("alice") is True
    assert reg.get_active_users() == []
    assert reg.unregister("alice") is False  # Already unregistered


def test_duplicate_registration_atomic():
    reg = UserRegistry()
    # First registration succeeds
    ok1, reason1 = reg.register("bob", "127.0.0.1", 9002, sock=None)
    assert ok1 is True
    assert reason1 == "OK"

    # Second registration for same username must fail atomically
    ok2, reason2 = reg.register("bob", "192.168.1.10", 9003, sock=None)
    assert ok2 is False
    assert reason2 == "ALREADY_LOGGED_IN"


def test_concurrent_registration_thread_safety():
    """Verify that under high concurrency, exactly 1 thread succeeds in registering a username."""
    reg = UserRegistry()
    results = []
    num_threads = 10

    def attempt_register():
        res = reg.register("carol", "127.0.0.1", 9000, sock=None)
        results.append(res)

    threads = [threading.Thread(target=attempt_register) for _ in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    successes = [r for r in results if r[0] is True]
    duplicates = [r for r in results if r[1] == "ALREADY_LOGGED_IN"]

    # Exactly ONE thread must succeed
    assert len(successes) == 1
    # All other threads must receive ALREADY_LOGGED_IN
    assert len(duplicates) == num_threads - 1
