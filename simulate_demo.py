"""
simulate_demo.py – Concurrent client simulation script (10-15 clients).
Purpose: Demonstrate Server Concurrency (Rubric 4) and State Tracking (Rubric 3).
Author: Chinh (Week 2)
"""

import argparse
import socket
import sys
import threading
import time
from config import SERVER_HOST, TCP_PORT
from protocol import send_json_frame, recv_json_frame


def simulate_single_client(
    username: str,
    password: str,
    host: str,
    port: int,
    hold_duration: float,
    results: dict,
    lock: threading.Lock,
):
    """Simulate an independent client: connect -> HELLO -> AUTH -> hold -> BYE."""
    start_time = time.time()
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5.0)
        sock.connect((host, port))

        # 1. HELLO -> ACK
        send_json_frame(sock, {"type": "HELLO", "client": username})
        ack = recv_json_frame(sock)
        if not ack or ack.get("type") != "ACK":
            with lock:
                results[username] = {"status": "FAIL_HELLO", "elapsed": time.time() - start_time}
            sock.close()
            return

        # 2. AUTH -> AUTH_OK
        send_json_frame(sock, {"type": "AUTH", "username": username, "password": password})
        auth_resp = recv_json_frame(sock)

        if not auth_resp or auth_resp.get("type") != "AUTH_OK":
            with lock:
                results[username] = {
                    "status": "FAIL_AUTH",
                    "reason": auth_resp.get("message") if auth_resp else "timeout",
                    "elapsed": time.time() - start_time,
                }
            sock.close()
            return

        # Authentication succeeded
        with lock:
            results[username] = {"status": "SUCCESS", "elapsed": time.time() - start_time}
            client_ip, client_port = sock.getsockname()
            print(f"  [✓] {username:<10} -> Authenticated successfully! (Local endpoint: {client_ip}:{client_port})")

        # 3. Hold connection open to allow Server to display Active Users table
        time.sleep(hold_duration)

        # 4. Graceful disconnect with BYE
        try:
            send_json_frame(sock, {"type": "BYE", "username": username})
            recv_json_frame(sock)
        except Exception:
            pass

        sock.close()

    except Exception as e:
        with lock:
            results[username] = {"status": "ERROR", "error": str(e), "elapsed": time.time() - start_time}
            print(f"  [✗] {username:<10} -> Connection error: {e}")


def run_simulation(count: int = 12, duration: float = 10.0, host: str = "127.0.0.1", port: int = TCP_PORT):
    print("=" * 68)
    print(f"  STARTING CONCURRENT CLIENT SIMULATION ({count} CLIENTS)")
    print(f"  Target Server: {host}:{port}")
    print(f"  Session Hold Duration: {duration} seconds")
    print("=" * 68)

    # Test usernames
    usernames = [f"user{i:02d}" for i in range(1, count + 1)]
    default_password = "123456"

    threads = []
    results = {}
    lock = threading.Lock()

    t_start = time.time()

    # Launch client threads concurrently
    print(f"\n[1/3] Launching {count} concurrent clients...")
    for u in usernames:
        t = threading.Thread(
            target=simulate_single_client,
            args=(u, default_password, host, port, duration, results, lock),
            daemon=True,
        )
        threads.append(t)
        t.start()
        time.sleep(0.02)  # Tiny stagger to simulate rapid incoming connection burst

    # Wait for all client sessions to finish
    print(f"\n[2/3] All clients logged in! Holding connections active for {duration}s...")
    print("      (Check server console to observe the live Active Users table!)\n")
    for t in threads:
        t.join(timeout=duration + 10.0)

    total_time = time.time() - t_start

    # Summary report
    print("\n" + "=" * 68)
    print("  SIMULATION RESULTS SUMMARY (CONCURRENCY LOAD TEST)")
    print("=" * 68)
    success_count = sum(1 for r in results.values() if r.get("status") == "SUCCESS")
    print(f"  - Total clients spawned:          {count}")
    print(f"  - Successfully authenticated:     {success_count}/{count} ({success_count/count*100:.1f}%)")
    print(f"  - Failed / Disconnected:          {count - success_count}")
    print(f"  - Total elapsed time:             {total_time:.2f} seconds")

    if success_count == count:
        print("\n  >>> RESULT: PASS - 100% SUCCESS (Rubric 3: State & Rubric 4: Concurrency) <<<")
    else:
        print("\n  >>> WARNING: Some clients failed. Please check server.py logs! <<<")
    print("=" * 68)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Concurrent client simulation for networking demo")
    parser.add_argument("--count", type=int, default=12, help="Number of concurrent clients (default: 12)")
    parser.add_argument("--duration", type=float, default=10.0, help="Session hold duration in seconds (default: 10)")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Target server host")
    parser.add_argument("--port", type=int, default=TCP_PORT, help="Target server port")

    args = parser.parse_args()
    run_simulation(count=args.count, duration=args.duration, host=args.host, port=args.port)
