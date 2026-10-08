import json
import socket
import threading
import time
from pathlib import Path

from config import TCP_PORT, UDP_PORT, CLIENT_TIMEOUT, RETRY_TIMEOUT, MAX_RETRIES
from protocol import (
    Packet, ACK, HEARTBEAT, MSG, FILE_META, FILE_CHUNK, USER_LIST,
    decode_packet, encode_packet, json_bytes, json_obj, new_id
)

HOST = "0.0.0.0"


class CollaborationServer:
    def __init__(self):
        self.users = {}
        self.users_lock = threading.Lock()
        self.pending = {}
        self.pending_lock = threading.Lock()
        self.seq = 1
        self.seq_lock = threading.Lock()
        self.stop_event = threading.Event()
        self.credentials = json.loads(Path("users.json").read_text())

        self.tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp.bind((HOST, TCP_PORT))
        self.tcp.listen(32)

        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp.bind((HOST, UDP_PORT))

    def next_seq(self):
        with self.seq_lock:
            value = self.seq
            self.seq += 1
            return value

    def send_json_line(self, conn, obj):
        conn.sendall((json.dumps(obj) + "\n").encode())

    def recv_json_line(self, file):
        line = file.readline()
        if not line:
            return None
        return json.loads(line.decode())

    def handle_tcp(self, conn, addr):
        username = None
        file = conn.makefile("rb")
        try:
            self.send_json_line(conn, {"type": "HELLO", "server": "collab-demo"})
            req = self.recv_json_line(file)
            if not req or req.get("type") != "AUTH":
                return
            username = req.get("username", "")
            password = req.get("password", "")
            udp_port = int(req.get("udp_port", 0))
            if self.credentials.get(username) != password or udp_port <= 0:
                self.send_json_line(conn, {"type": "AUTH_FAIL"})
                return

            with self.users_lock:
                self.users[username] = {
                    "tcp": conn,
                    "udp": (addr[0], udp_port),
                    "last_seen": time.time(),
                }

            self.send_json_line(conn, {"type": "AUTH_OK", "username": username})
            self.broadcast_user_list()

            while not self.stop_event.is_set():
                req = self.recv_json_line(file)
                if req is None:
                    break
                if req.get("type") == "PING":
                    self.send_json_line(conn, {"type": "PONG"})
        except (OSError, ValueError, json.JSONDecodeError):
            pass
        finally:
            if username:
                with self.users_lock:
                    current = self.users.get(username)
                    if current and current.get("tcp") is conn:
                        self.users.pop(username, None)
                self.broadcast_user_list()
            try:
                conn.close()
            except OSError:
                pass

    def tcp_loop(self):
        while not self.stop_event.is_set():
            try:
                conn, addr = self.tcp.accept()
                threading.Thread(target=self.handle_tcp, args=(conn, addr), daemon=True).start()
            except OSError:
                break

    def username_for_udp(self, addr):
        with self.users_lock:
            for name, info in self.users.items():
                if info["udp"] == addr:
                    return name
        return None

    def ack(self, addr, packet):
        payload = json_bytes({"ack": packet.seq, "msg_id": packet.msg_id.hex})
        ack_packet = Packet(ACK, self.next_seq(), new_id(), payload=payload)
        self.udp.sendto(encode_packet(ack_packet), addr)

    def mark_ack(self, addr, packet):
        try:
            body = json_obj(packet.payload)
            key = (addr, body["msg_id"], int(body["ack"]))
        except Exception:
            return
        with self.pending_lock:
            event = self.pending.get(key)
        if event:
            event.set()

    def send_reliable(self, addr, packet):
        key = (addr, packet.msg_id.hex, packet.seq)
        event = threading.Event()
        with self.pending_lock:
            self.pending[key] = event
        try:
            raw = encode_packet(packet)
            for _ in range(MAX_RETRIES):
                self.udp.sendto(raw, addr)
                if event.wait(RETRY_TIMEOUT):
                    return True
            return False
        finally:
            with self.pending_lock:
                self.pending.pop(key, None)

    def recipients(self, target, sender):
        with self.users_lock:
            if target == "*":
                return [
                    (name, info["udp"])
                    for name, info in self.users.items()
                    if name != sender
                ]
            if target in self.users and target != sender:
                return [(target, self.users[target]["udp"])]
        return []

    def forward(self, sender, packet):
        body = json_obj(packet.payload)
        target = body.get("to", "*")
        body["from"] = sender
        payload = json_bytes(body)
        for _, addr in self.recipients(target, sender):
            forwarded = Packet(
                packet.ptype,
                self.next_seq(),
                packet.msg_id,
                packet.chunk,
                packet.total,
                packet.flags,
                payload,
            )
            threading.Thread(
                target=self.send_reliable,
                args=(addr, forwarded),
                daemon=True,
            ).start()

    def send_user_list(self, addr):
        with self.users_lock:
            names = sorted(self.users.keys())
        packet = Packet(
            USER_LIST,
            self.next_seq(),
            new_id(),
            payload=json_bytes({"users": names}),
        )
        self.udp.sendto(encode_packet(packet), addr)

    def broadcast_user_list(self):
        with self.users_lock:
            targets = [info["udp"] for info in self.users.values()]
        for addr in targets:
            try:
                self.send_user_list(addr)
            except OSError:
                pass

    def udp_loop(self):
        while not self.stop_event.is_set():
            try:
                raw, addr = self.udp.recvfrom(65535)
                packet = decode_packet(raw)
            except (OSError, ValueError):
                continue

            if packet.ptype == ACK:
                self.mark_ack(addr, packet)
                continue

            sender = self.username_for_udp(addr)
            if not sender:
                continue

            self.ack(addr, packet)
            with self.users_lock:
                if sender in self.users:
                    self.users[sender]["last_seen"] = time.time()

            if packet.ptype == HEARTBEAT:
                self.send_user_list(addr)
            elif packet.ptype in (MSG, FILE_META, FILE_CHUNK):
                try:
                    self.forward(sender, packet)
                except Exception:
                    pass

    def cleanup_loop(self):
        while not self.stop_event.wait(2):
            cutoff = time.time() - CLIENT_TIMEOUT
            removed = False
            with self.users_lock:
                stale = [u for u, i in self.users.items() if i["last_seen"] < cutoff]
                for username in stale:
                    try:
                        self.users[username]["tcp"].close()
                    except OSError:
                        pass
                    self.users.pop(username, None)
                    removed = True
            if removed:
                self.broadcast_user_list()

    def run(self):
        print(f"TCP control : {HOST}:{TCP_PORT}")
        print(f"UDP stream : {HOST}:{UDP_PORT}")
        threading.Thread(target=self.tcp_loop, daemon=True).start()
        threading.Thread(target=self.udp_loop, daemon=True).start()
        threading.Thread(target=self.cleanup_loop, daemon=True).start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop_event.set()
            self.tcp.close()
            self.udp.close()


if __name__ == "__main__":
    CollaborationServer().run()
