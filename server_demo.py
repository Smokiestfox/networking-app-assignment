"""
demo_server.py - TEST-ONLY server: the real TCP server + a simple UDP engine in one process.

Use it to try client.py before the real UDP part of server.py is finished:
    python demo_server.py                 # TCP 9000 + UDP 9001, heartbeat timeout from config.py
    python demo_server.py --timeout 5     # shorter heartbeat timeout for quick tests

It does NOT modify server.py: the TCP side is CollaborationServer exactly as it is (login, registry,
Active Users table, logs). The UDP engine below only shows what the client expects from the server:
  * HEARTBEAT   -> learn the client's UDP address (source address of the datagram), refresh last_seen
  * MSG         -> sender is found from the source address; "*" goes to everyone except the sender,
                   "<user>" goes only to that user, unknown/offline recipient is dropped
  * USER_LIST   -> pushed to every client when someone appears (first heartbeat) or disappears
  * cleanup     -> TCP closed, or no heartbeat for CLIENT_TIMEOUT seconds => user is removed
Do not run it together with the real UDP server or udp_stub_server.py (they use the same UDP port).
"""

import argparse
import socket
import threading
import time
from datetime import datetime

from config import SERVER_HOST, UDP_PORT, CLIENT_TIMEOUT
from protocol import (HEARTBEAT, MSG, SeqCounter, pack_msg, pack_user_list, parse_udp)
from server import CollaborationServer


def log(text: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [UDP] {text}", flush=True)


class DemoUdpEngine:
    def __init__(self, registry, host=SERVER_HOST, port=UDP_PORT, timeout=CLIENT_TIMEOUT):
        self.registry = registry
        self.timeout = timeout
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((host, port))
        self.sock.settimeout(0.5)
        self.lock = threading.Lock()        # protects addrs and last_seen
        self.addrs = {}                     # username -> (ip, udp_port)
        self.last_seen = {}                 # username -> time of last HEARTBEAT
        self.seq = SeqCounter()
        self.stop = threading.Event()

    # ------------------------------------------------------------------ helpers
    def _send(self, data: bytes, addr: tuple) -> None:
        try:
            self.sock.sendto(data, addr)
        except OSError as exc:
            log(f"send to {addr} failed: {exc}")

    def _push_user_list(self) -> None:
        with self.lock:
            users = sorted(self.addrs)
            targets = list(self.addrs.values())
        data = pack_user_list(users, self.seq.next())
        for addr in targets:
            self._send(data, addr)
        log(f"USER_LIST pushed to {len(targets)} client(s): {users}")

    def _remove(self, username: str, reason: str) -> None:
        with self.lock:
            existed = self.addrs.pop(username, None) is not None
            self.last_seen.pop(username, None)
        if existed:
            log(f"'{username}' removed ({reason})")
            self._push_user_list()

    # ------------------------------------------------------------------ UDP receive loop
    def receive_loop(self) -> None:
        while not self.stop.is_set():
            try:
                raw, addr = self.sock.recvfrom(4096)
            except socket.timeout:
                continue
            except ConnectionResetError:    # Windows: an earlier datagram hit a closed port
                continue
            except OSError:
                break
            try:
                packet, body = parse_udp(raw)
            except ValueError as exc:
                log(f"ignored bad datagram from {addr}: {exc}")
                continue
            if packet.ptype == HEARTBEAT:
                self._on_heartbeat(body["username"], addr)
            elif packet.ptype == MSG:
                self._on_msg(body, addr)

    def _on_heartbeat(self, username: str, addr: tuple) -> None:
        if not self.registry.is_active(username):
            return                           # not logged in over TCP: ignore
        with self.lock:
            is_new = username not in self.addrs
            self.addrs[username] = addr
            self.last_seen[username] = time.time()
        if is_new:
            log(f"'{username}' UDP address learned: {addr[0]}:{addr[1]}")
            self._push_user_list()

    def _on_msg(self, body: dict, addr: tuple) -> None:
        with self.lock:
            sender = next((u for u, a in self.addrs.items() if a == addr), None)
            snapshot = dict(self.addrs)
        if sender is None:
            log(f"MSG from unknown address {addr} dropped")
            return
        to, text = body["to"], body["text"]
        data = pack_msg(to, sender, text, self.seq.next())   # "from" is set by the server
        if to == "*":
            targets = [u for u in snapshot if u != sender]
        elif to in snapshot and to != sender:
            targets = [to]
        else:
            targets = []
        for user in targets:
            self._send(data, snapshot[user])
        label = "broadcast" if to == "*" else f"unicast -> {to}"
        log(f"MSG {sender} {label}: delivered to {len(targets)} client(s)"
            + ("" if targets else " (recipient not online, dropped)"))

    # ------------------------------------------------------------------ cleanup loop
    def cleanup_loop(self) -> None:
        while not self.stop.wait(1.0):
            online = set(self.registry.get_active_users())
            with self.lock:
                known = list(self.addrs)
                now = time.time()
                stale = [u for u in known if now - self.last_seen.get(u, now) > self.timeout]
            for user in known:
                if user not in online:                      # TCP session ended (BYE / crash)
                    self._remove(user, "TCP session ended")
            for user in stale:
                if user in online:                          # alive over TCP but no heartbeat
                    self._expire(user)

    def _expire(self, username: str) -> None:
        with self.registry.lock:
            info = self.registry.active_users.get(username)
            conn = info["conn"] if info else None
        self.registry.unregister(username=username)
        if conn is not None:
            try:
                conn.close()                                # makes the TCP handler thread finish
            except OSError:
                pass
        self._remove(username, f"no heartbeat for {self.timeout:.0f}s")

    def start(self) -> None:
        threading.Thread(target=self.receive_loop, daemon=True, name="udp-recv").start()
        threading.Thread(target=self.cleanup_loop, daemon=True, name="udp-cleanup").start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test-only server: real TCP server + demo UDP engine")
    parser.add_argument("--timeout", type=float, default=CLIENT_TIMEOUT,
                        help=f"heartbeat timeout in seconds (default {CLIENT_TIMEOUT})")
    args = parser.parse_args()

    tcp_server = CollaborationServer()
    engine = DemoUdpEngine(tcp_server.registry, timeout=args.timeout)
    engine.start()
    log(f"demo UDP engine listening on port {UDP_PORT} (heartbeat timeout {args.timeout:.0f}s)")
    try:
        tcp_server.run()                                     # blocks; Ctrl+C to stop
    finally:
        engine.stop.set()