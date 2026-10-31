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
import hashlib
import json
import tempfile
from pathlib import Path
from client import run_client
from file_transfers import load_receiver_class
from config import UDP_PORT
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

        # 3. Heartbeat is required since Week 3, even for the older load demo.
        from protocol import pack_heartbeat
        from config import HEARTBEAT_INTERVAL
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp:
            udp.bind(("", 0))
            end = time.monotonic() + hold_duration
            seq = 0
            while time.monotonic() < end:
                seq += 1
                udp.sendto(pack_heartbeat(username, seq), (host, UDP_PORT if port == TCP_PORT else port + 1))
                time.sleep(min(HEARTBEAT_INTERVAL, max(0, end - time.monotonic())))

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
    default_password = "pass123"

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


def run_file_demo(host="127.0.0.1", port=TCP_PORT, udp_port=UDP_PORT, *,
                  size_mb=5, loss=0, timeout=120, output="downloads/demo", abort_receiver=False,
                  demo_receiver=False):
    """Three real authenticated clients; validates hashes, isolation and concurrent chat."""
    if not 0 < size_mb <= 40 or timeout <= 0 or not 0 <= loss < 1:
        raise ValueError("Use 0 < size-mb <= 40, timeout > 0 and 0 <= loss < 1")
    if load_receiver_class(demo_receiver) is None:
        raise RuntimeError("file_receiver.py is missing; add --demo-receiver to test the reference receiver")
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    stop = threading.Event()
    bob_stop = threading.Event()
    stats = {name: {} for name in ("alice", "bob", "carol")}
    threads = []

    def launch(name, **kwargs):
        ready = threading.Event()
        client_stop = bob_stop if name == "bob" else stop
        thread = threading.Thread(target=run_client, kwargs=dict(
            username=name, password="pass123", host=host, port=port, server_udp_port=udp_port,
            interactive=False, stats=stats[name], ready_evt=ready, session_stop=client_stop,
            download_dir=output / name, simulate_loss=loss, demo_receiver=demo_receiver,
            loss_seed={"alice": 101, "bob": 102, "carol": 103}[name], **kwargs), daemon=True)
        threads.append(thread)
        thread.start()
        if not ready.wait(8):
            raise RuntimeError(f"{name} could not log in; check server and UDP port")

    try:
        with tempfile.TemporaryDirectory(prefix="network-file-demo-") as tmp:
            source = Path(tmp) / "sample file.bin"
            size = int(size_mb * 1024 * 1024)
            source.write_bytes((bytes(range(256)) * ((size + 255) // 256))[:size])
            expected = hashlib.sha256(source.read_bytes()).hexdigest()
            launch("bob", graceful_shutdown=not abort_receiver)
            launch("carol", duration=timeout, send_delay=0.3,
                   send_messages=[("*", f"file demo live chat {i}") for i in range(int(timeout * 10))])
            launch("alice", duration=timeout, send_delay=0.1, send_files=[("bob", source)])
            deadline = time.monotonic() + timeout
            abort_at = time.monotonic() + 0.4
            abort_requested = False
            while time.monotonic() < deadline:
                if abort_receiver and time.monotonic() >= abort_at:
                    bob_stop.set()
                    abort_requested = True
                if stats["alice"].get("files_sent") or stats["alice"].get("files_send_failed"):
                    break
                time.sleep(0.05)
            matched = False
            paths = stats["bob"].get("received_paths", [])
            if paths:
                matched = hashlib.sha256(Path(paths[-1]).read_bytes()).hexdigest() == expected
            chat = stats["bob"].get("messages_during_file", 0)
            isolated = not stats["carol"].get("file_packets_received")
            aborted = (abort_requested and bool(stats["alice"].get("send_results"))
                       and stats["alice"]["send_results"][-1]["error"] == "Peer 'bob' went offline")
            passed = (aborted if abort_receiver else
                      bool(stats["alice"].get("files_sent")) and matched and isolated and chat > 0)
            result = {"passed": passed, "size": size, "loss": loss,
                      "receiver_backend": "demo" if demo_receiver else "production",
                      "receiver_aborted": abort_receiver, "sha256": expected,
                      "hash_matched": matched, "carol_isolated": isolated,
                      "chat_during_transfer": chat, "clients": stats}
    finally:
        stop.set()
        bob_stop.set()
        for thread in threads:
            thread.join(timeout=8)
    result["all_clients_stopped"] = all(not t.is_alive() for t in threads)
    result["passed"] = result["passed"] and result["all_clients_stopped"]
    (output / "result.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print("\nFILE DEMO RESULT: " + json.dumps(result, indent=2), flush=True)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Concurrent client simulation for networking demo")
    parser.add_argument("--count", type=int, default=12, help="Number of concurrent clients (default: 12)")
    parser.add_argument("--duration", type=float, default=10.0, help="Session hold duration in seconds (default: 10)")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Target server host")
    parser.add_argument("--port", type=int, default=TCP_PORT, help="Target server port")
    parser.add_argument("--udp-port", type=int, default=UDP_PORT)
    parser.add_argument("--file-demo", action="store_true", help="Alice -> Bob file + Carol live chat")
    parser.add_argument("--demo-receiver", action="store_true", help="Use the reference receiver, not Hau's production module")
    parser.add_argument("--self-host", action="store_true", help="Start server in this process (file demo)")
    parser.add_argument("--size-mb", type=float, default=5)
    parser.add_argument("--loss", type=float, default=None, help="File datagram loss; default SIMULATE_LOSS or 0")
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--output", default="downloads/demo")
    parser.add_argument("--abort-receiver", action="store_true")

    args = parser.parse_args()
    if not args.file_demo:
        run_simulation(count=args.count, duration=args.duration, host=args.host, port=args.port)
    else:
        import os
        if load_receiver_class(args.demo_receiver) is None:
            parser.error("file_receiver.py is missing; use --demo-receiver for reference tests")
        server = None
        if args.self_host:
            from server import CollaborationServer
            server = CollaborationServer(host=args.host, port=args.port, udp_port=args.udp_port)
            threading.Thread(target=server.run, daemon=True).start()
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                try:
                    if server.sock.getsockname()[1] and server.udp_sock.getsockname()[1]:
                        break
                except OSError:
                    pass
                time.sleep(0.02)
            args.port = server.sock.getsockname()[1]
            args.udp_port = server.udp_sock.getsockname()[1]
        try:
            result = run_file_demo(args.host, args.port, args.udp_port, size_mb=args.size_mb,
                                   loss=float(os.environ.get("SIMULATE_LOSS", "0")) if args.loss is None else args.loss,
                                   timeout=args.timeout, output=args.output,
                                   abort_receiver=args.abort_receiver, demo_receiver=args.demo_receiver)
        finally:
            if server:
                server.stop()
        sys.exit(0 if result["passed"] else 1)
