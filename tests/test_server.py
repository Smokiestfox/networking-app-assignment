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
    print("ALL TESTS COMPLETED SUCCESSFULLY!")
    print("========================================")
