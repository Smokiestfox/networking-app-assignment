import argparse
import base64
import hashlib
import json
import os
import socket
import threading
import time
import uuid
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from config import (
    SERVER_HOST, TCP_PORT, UDP_PORT, HEARTBEAT_INTERVAL,
    RETRY_TIMEOUT, MAX_RETRIES, WINDOW_SIZE, CHUNK_SIZE
)
from protocol import (
    Packet, ACK, HEARTBEAT, MSG, FILE_META, FILE_CHUNK, USER_LIST,
    decode_packet, encode_packet, json_bytes, json_obj, new_id
)


class CollaborationClient:
    def __init__(self, username, password, server_host):
        self.username = username
        self.password = password
        self.server_addr = (server_host, UDP_PORT)
        self.users = []
        self.messages = []
        self.transfers = []
        self.incoming = {}
        self.pending = {}
        self.pending_lock = threading.Lock()
        self.seq = 1
        self.seq_lock = threading.Lock()
        self.stop_event = threading.Event()

        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp.bind(("0.0.0.0", 0))
        self.udp_port = self.udp.getsockname()[1]

        self.tcp = socket.create_connection((server_host, TCP_PORT), timeout=5)
        self.tcp_file = self.tcp.makefile("rb")
        hello = self.recv_json_line()
        if not hello or hello.get("type") != "HELLO":
            raise RuntimeError("bad server handshake")
        self.send_json_line({
            "type": "AUTH",
            "username": username,
            "password": password,
            "udp_port": self.udp_port,
        })
        reply = self.recv_json_line()
        if not reply or reply.get("type") != "AUTH_OK":
            raise RuntimeError("authentication failed")

        threading.Thread(target=self.udp_loop, daemon=True).start()
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()
        threading.Thread(target=self.tcp_ping_loop, daemon=True).start()

    def next_seq(self):
        with self.seq_lock:
            value = self.seq
            self.seq += 1
            return value

    def send_json_line(self, obj):
        self.tcp.sendall((json.dumps(obj) + "\n").encode())

    def recv_json_line(self):
        line = self.tcp_file.readline()
        if not line:
            return None
        return json.loads(line.decode())

    def ack(self, packet):
        body = {"ack": packet.seq, "msg_id": packet.msg_id.hex}
        ack = Packet(ACK, self.next_seq(), new_id(), payload=json_bytes(body))
        self.udp.sendto(encode_packet(ack), self.server_addr)

    def mark_ack(self, packet):
        try:
            body = json_obj(packet.payload)
            key = (body["msg_id"], int(body["ack"]))
        except Exception:
            return
        with self.pending_lock:
            event = self.pending.get(key)
        if event:
            event.set()

    def send_window(self, packets):
        index = 0
        while index < len(packets):
            batch = packets[index:index + WINDOW_SIZE]
            events = {}
            raws = {}
            for packet in batch:
                key = (packet.msg_id.hex, packet.seq)
                event = threading.Event()
                events[key] = event
                raws[key] = encode_packet(packet)
                with self.pending_lock:
                    self.pending[key] = event

            try:
                for attempt in range(MAX_RETRIES):
                    for packet in batch:
                        key = (packet.msg_id.hex, packet.seq)
                        if not events[key].is_set():
                            self.udp.sendto(raws[key], self.server_addr)
                    deadline = time.time() + RETRY_TIMEOUT
                    while time.time() < deadline:
                        if all(e.is_set() for e in events.values()):
                            break
                        time.sleep(0.01)
                    if all(e.is_set() for e in events.values()):
                        break
                else:
                    raise TimeoutError("UDP transfer timed out")
            finally:
                with self.pending_lock:
                    for key in events:
                        self.pending.pop(key, None)
            index += len(batch)

    def send_message(self, target, text):
        mid = new_id()
        packet = Packet(
            MSG,
            self.next_seq(),
            mid,
            payload=json_bytes({"to": target, "text": text}),
        )
        self.send_window([packet])
        self.messages.append({
            "from": self.username,
            "to": target,
            "text": text,
            "time": time.strftime("%H:%M:%S"),
        })

    def send_file_bytes(self, target, name, data):
        file_id = uuid.uuid4().hex
        digest = hashlib.sha256(data).hexdigest()
        chunks = [data[i:i + CHUNK_SIZE] for i in range(0, len(data), CHUNK_SIZE)] or [b""]
        total = len(chunks)

        meta = Packet(
            FILE_META,
            self.next_seq(),
            new_id(),
            payload=json_bytes({
                "to": target,
                "file_id": file_id,
                "name": os.path.basename(name),
                "size": len(data),
                "sha256": digest,
                "total": total,
            }),
        )
        self.send_window([meta])

        packets = []
        transfer_id = new_id()
        for idx, chunk in enumerate(chunks):
            packets.append(Packet(
                FILE_CHUNK,
                self.next_seq(),
                transfer_id,
                chunk=idx,
                total=total,
                payload=json_bytes({
                    "to": target,
                    "file_id": file_id,
                    "index": idx,
                    "data": base64.b64encode(chunk).decode(),
                }),
            ))

        self.transfers.append({
            "direction": "send",
            "name": name,
            "peer": target,
            "status": "sending",
            "progress": 0,
        })
        record = self.transfers[-1]

        for start in range(0, len(packets), WINDOW_SIZE):
            self.send_window(packets[start:start + WINDOW_SIZE])
            record["progress"] = min(100, int((start + WINDOW_SIZE) * 100 / len(packets)))

        record["progress"] = 100
        record["status"] = "done"

    def handle_file_meta(self, body):
        fid = body["file_id"]
        self.incoming[fid] = {
            "name": body["name"],
            "size": body["size"],
            "sha256": body["sha256"],
            "total": body["total"],
            "from": body.get("from", "?"),
            "chunks": {},
        }
        self.transfers.append({
            "direction": "receive",
            "name": body["name"],
            "peer": body.get("from", "?"),
            "status": "receiving",
            "progress": 0,
            "file_id": fid,
        })

    def handle_file_chunk(self, body):
        fid = body["file_id"]
        item = self.incoming.get(fid)
        if not item:
            return
        idx = int(body["index"])
        item["chunks"][idx] = base64.b64decode(body["data"])
        progress = int(len(item["chunks"]) * 100 / item["total"])
        for record in self.transfers:
            if record.get("file_id") == fid:
                record["progress"] = progress

        if len(item["chunks"]) == item["total"]:
            data = b"".join(item["chunks"][i] for i in range(item["total"]))
            digest = hashlib.sha256(data).hexdigest()
            status = "done" if digest == item["sha256"] else "hash mismatch"
            Path("downloads").mkdir(exist_ok=True)
            out = Path("downloads") / item["name"]
            if status == "done":
                out.write_bytes(data)
            for record in self.transfers:
                if record.get("file_id") == fid:
                    record["status"] = status
                    record["progress"] = 100
            self.incoming.pop(fid, None)

    def udp_loop(self):
        while not self.stop_event.is_set():
            try:
                raw, _ = self.udp.recvfrom(65535)
                packet = decode_packet(raw)
            except (OSError, ValueError):
                continue

            if packet.ptype == ACK:
                self.mark_ack(packet)
                continue

            self.ack(packet)
            try:
                body = json_obj(packet.payload)
            except Exception:
                continue

            if packet.ptype == USER_LIST:
                self.users = body.get("users", [])
            elif packet.ptype == MSG:
                self.messages.append({
                    "from": body.get("from", "?"),
                    "to": body.get("to", "*"),
                    "text": body.get("text", ""),
                    "time": time.strftime("%H:%M:%S"),
                })
            elif packet.ptype == FILE_META:
                self.handle_file_meta(body)
            elif packet.ptype == FILE_CHUNK:
                self.handle_file_chunk(body)

    def heartbeat_loop(self):
        while not self.stop_event.wait(HEARTBEAT_INTERVAL):
            packet = Packet(
                HEARTBEAT,
                self.next_seq(),
                new_id(),
                payload=json_bytes({"username": self.username}),
            )
            try:
                self.udp.sendto(encode_packet(packet), self.server_addr)
            except OSError:
                break

    def tcp_ping_loop(self):
        while not self.stop_event.wait(4):
            try:
                self.send_json_line({"type": "PING"})
                self.recv_json_line()
            except Exception:
                self.stop_event.set()
                break

    def state(self):
        return {
            "username": self.username,
            "users": self.users,
            "messages": self.messages[-100:],
            "transfers": self.transfers[-50:],
        }


class UIHandler(BaseHTTPRequestHandler):
    client_app = None
    index_file = Path(__file__).with_name("static") / "index.html"

    def reply(self, code, data, ctype="application/json"):
        body = data if isinstance(data, bytes) else data.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def read_json(self):
        size = int(self.headers.get("Content-Length", "0"))
        return json.loads(self.rfile.read(size) or b"{}")

    def do_GET(self):
        if self.path == "/":
            self.reply(200, self.index_file.read_bytes(), "text/html; charset=utf-8")
        elif self.path == "/api/state":
            self.reply(200, json.dumps(self.client_app.state()))
        else:
            self.reply(404, "{}")

    def do_POST(self):
        try:
            body = self.read_json()
            if self.path == "/api/send":
                self.client_app.send_message(body.get("to", "*"), body.get("text", ""))
                self.reply(200, '{"ok":true}')
            elif self.path == "/api/file":
                data = base64.b64decode(body["data"])
                threading.Thread(
                    target=self.client_app.send_file_bytes,
                    args=(body.get("to", "*"), body["name"], data),
                    daemon=True,
                ).start()
                self.reply(200, '{"ok":true}')
            else:
                self.reply(404, "{}")
        except Exception as exc:
            self.reply(500, json.dumps({"ok": False, "error": str(exc)}))

    def log_message(self, format, *args):
        pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--server", default=SERVER_HOST)
    parser.add_argument("--ui-port", type=int, default=8000)
    args = parser.parse_args()

    app = CollaborationClient(args.username, args.password, args.server)
    UIHandler.client_app = app
    ui = ThreadingHTTPServer(("127.0.0.1", args.ui_port), UIHandler)

    url = f"http://127.0.0.1:{args.ui_port}"
    print(f"Logged in as {args.username}")
    print(f"UDP port     : {app.udp_port}")
    print(f"Web UI       : {url}")
    threading.Timer(0.5, lambda: webbrowser.open(url)).start()

    try:
        ui.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        app.stop_event.set()
        ui.server_close()
        app.tcp.close()
        app.udp.close()


if __name__ == "__main__":
    main()
