"""One file worker per client; publish JSON events without owning the UI/socket."""

from collections import OrderedDict
from copy import deepcopy
from functools import partial
import logging
import queue
import random
import threading
import time
import uuid
from pathlib import Path

from file_receiver import FileReceiver
from file_sender import FileSender
from config import FILE_SESSION_TIMEOUT
from protocol import ACK, FILE_CHUNK, FILE_META, new_id, parse_udp

log = logging.getLogger("client.files")
TERMINAL = {"completed", "failed"}


def loss_rate_from_env(value):
    rate = float(value)
    if not 0 <= rate < 1:
        raise ValueError("SIMULATE_LOSS must be in [0, 1)")
    return rate


class FileTransfers:
    def __init__(self, username, send_fn, seq, download_dir, *, loss_rate=0,
                 stats=None, seed=None, on_event=None, quiet=False, send_control=None):
        self.username, self.send_fn, self.seq = username, send_fn, seq
        self.stats = stats if stats is not None else {}
        self.loss_rate = loss_rate_from_env(loss_rate)
        self.random = random.Random(seed)
        self.on_event, self.quiet = on_event, quiet
        self.send_control = send_control
        self.offers = {}
        self.peers = {}
        self.senders = {}
        self.events = queue.Queue(maxsize=4096)
        self.stop_evt = threading.Event()
        self.active_evt = threading.Event()
        self._pending = set()
        self._pending_lock = threading.RLock()
        self._incoming = OrderedDict()
        self._last_progress = {}
        self.receiver = FileReceiver(self._send_ack, download_dir, username=username,
                                     seq=seq, on_result=self._received)
        self.stats["receiver_backend"] = "production"
        self.thread = threading.Thread(target=self._run, name=f"files-{username}", daemon=True)
        self.thread.start()

    def _emit(self, data):
        if self.on_event:
            try:
                self.on_event(deepcopy(data))
            except Exception:
                log.exception("[%s] File event callback failed", self.username)

    @staticmethod
    def _record(mid, direction, peer, filename, size=None, **extra):
        return {"id": str(mid), "direction": direction, "peer": peer,
                "filename": filename, "size": size, "bytes_transferred": 0,
                "progress": 0.0, "status": "queued", "error": None,
                "path": None, "checksum": None, "cwnd": None, "rto": None, **extra}

    def _send(self, raw, endpoint=None):
        if self.random.random() < self.loss_rate:
            self.stats["file_packets_dropped"] = self.stats.get("file_packets_dropped", 0) + 1
            return
        if endpoint is None:
            self.send_fn(raw)
        else:
            self.send_fn(raw, endpoint)

    def send_file(self, to, path):
        if not isinstance(to, str) or not to.strip() or to in ("*", self.username):
            raise ValueError("Usage: /send <other username> <file path>")
        path = Path(path)
        mid = new_id()
        with self._pending_lock:
            if self.stop_evt.is_set():
                raise RuntimeError("File worker is closed")
            if len(self._pending) >= 8:
                raise ValueError("Already sending 8 files; wait for one to finish")
            self._pending.add(mid)
            # Publish queued before making the job visible; synchronize with close().
            self._emit(self._record(mid, "send", to, path.name))
            try:
                self.events.put_nowait(("send", (mid, to, path), None))
            except queue.Full:
                self._pending.discard(mid)
                self._emit(self._record(mid, "send", to, path.name,
                                        status="failed", error="File queue is full"))
                raise
        return str(mid)

    def on_control(self, body):
        self.events.put_nowait(("control", body, None))

    def on_packet(self, packet, body, address=None):
        if not self.stop_evt.is_set():
            try:
                self.events.put_nowait(("packet", packet, (body, address)))
            except queue.Full:
                pass  # selective retransmission recovers loss; never block UDP reception

    def peer_left(self, username):
        try:
            self.events.put_nowait(("left", username, None))
        except queue.Full:
            pass  # ACK inactivity still bounds outgoing transfers

    def _received(self, mid, path, error):
        entry = self._incoming.get(mid)
        if entry is None:
            return
        record = entry["record"]
        if record["status"] in TERMINAL:
            return
        record.update(status="failed" if error else "completed", error=error,
                      path=str(path) if path else None)
        if not error:
            record.update(bytes_transferred=record["size"], progress=1.0)
            self.stats.setdefault("received_paths", []).append(str(path))
            self.stats["files_received"] = self.stats.get("files_received", 0) + 1
            if not self.quiet:
                print(f"\n  [FILE RECEIVED] SHA-256 matched: {path}", flush=True)
        else:
            self.stats["files_receive_failed"] = self.stats.get("files_receive_failed", 0) + 1
            log.error("[%s] Receive %s failed: %s", self.username, mid, error)
        entry["chunks"].clear()
        entry["last_seen"] = time.monotonic()
        self._emit(record)

    def _receive_packet(self, packet, body):
        accepted = self.receiver.on_packet(packet, body)
        if not accepted:
            return
        mid, now = packet.msg_id, time.monotonic()
        if packet.ptype == FILE_META:
            if mid not in self._incoming:
                record = self._record(mid, "receive", body["from"], body["filename"],
                                      body["size"], checksum=body["checksum"], status="transferring")
                self._incoming[mid] = {"record": record, "chunks": set(), "last_seen": now}
                self._emit(record)
            self._incoming[mid]["last_seen"] = now
        else:
            entry = self._incoming.get(mid)
            if entry is None or entry["record"]["status"] in TERMINAL:
                return  # on_result may have completed this file inside on_packet()
            entry["last_seen"] = now
            if packet.chunk not in entry["chunks"]:
                entry["chunks"].add(packet.chunk)
                record = entry["record"]
                record["bytes_transferred"] += len(body)
                size = record["size"]
                record["progress"] = min(0.999, record["bytes_transferred"] / size) if size else 0.0
                self._emit_progress(mid, record)

    def _emit_progress(self, mid, record, force=False):
        now = time.monotonic()
        if force or now - self._last_progress.get(mid, 0) >= 0.2:
            self._last_progress[mid] = now
            self._emit(record)

    def _sender_record(self, sender):
        status = ("completed" if sender.success else "failed") if sender.done else "transferring"
        # Never present a rounded 100% as completed before the verification ACK.
        progress = 1.0 if sender.success else min(0.999, sender.progress)
        return self._record(sender.msg_id, "send", sender.to, sender.path.name, sender.size,
                            bytes_transferred=sender.bytes_acked, progress=progress, status=status,
                            error=sender.error, checksum=sender.checksum,
                            cwnd=round(sender.controller.cwnd, 3), rto=round(sender.controller.rto, 3))

    def _report(self, sender):
        if not self.quiet:
            filled = int(sender.progress * 20)
            label = f"[{'#' * filled}{'.' * (20 - filled)}] {sender.progress:.0%}"
            log.info("[%s -> %s] %s cwnd=%.2f RTO=%.3fs inflight=%d retries=%d",
                     self.username, sender.to, label, sender.controller.cwnd,
                     sender.controller.rto, len(sender.inflight), sender.retransmissions)

    def _finished(self, sender):
        result = {"success": sender.success, "error": sender.error,
                  "checksum": sender.checksum, "size": sender.size,
                  "retransmissions": sender.retransmissions,
                  "peak_cwnd": sender.peak_cwnd, "loss_events": sender.controller.loss_events}
        self.stats.setdefault("send_results", []).append(result)
        key = "files_sent" if sender.success else "files_send_failed"
        self.stats[key] = self.stats.get(key, 0) + 1
        with self._pending_lock:
            self._pending.discard(sender.msg_id)
        self._report(sender)
        self._emit_progress(sender.msg_id, self._sender_record(sender), force=True)
        self._last_progress.pop(sender.msg_id, None)
        if sender.success and not self.quiet:
            print(f"\n  [FILE SENT] {sender.path.name} -> {sender.to}: "
                  f"SHA-256 matched and saved by receiver ({sender.checksum})", flush=True)
        elif not sender.success:
            log.error("[%s] File %s -> %s failed: %s", self.username,
                      sender.path.name, sender.to, sender.error)

    def _start_send(self, job):
        mid, to, path = job
        try:
            sender = FileSender(path, to, self.username, self._send, seq=self.seq, msg_id=mid)
            if self.send_control is None:
                self.senders[mid] = sender
            else:
                self.send_control({"type": "FILE_OFFER", "transfer_id": str(mid), "to": to,
                                   "filename": path.name, "size": sender.size, "sha256": sender.checksum})
                self.offers[mid] = (sender, time.monotonic() + FILE_SESSION_TIMEOUT)
        except (OSError, ValueError) as exc:
            with self._pending_lock:
                self._pending.discard(mid)
            self.stats["files_send_failed"] = self.stats.get("files_send_failed", 0) + 1
            self._emit(self._record(mid, "send", to, path.name, status="failed", error=str(exc)))
            log.error("[%s] Cannot send %s: %s", self.username, path, exc)

    def _handle_control(self, body):
        try:
            mid = uuid.UUID(body.get("transfer_id"))
        except (TypeError, ValueError, AttributeError):
            return
        kind = body["type"]
        if kind == "FILE_OFFER":
            # The existing CLI accepts incoming transfers automatically.
            if len(self.peers) >= 32 or mid in self.peers or mid in self.offers:
                self.send_control({"type": "FILE_REJECT", "transfer_id": str(mid)})
                return
            self.peers[mid] = (body, None, time.monotonic())
            self.send_control({"type": "FILE_ACCEPT", "transfer_id": str(mid)})
        elif kind == "FILE_READY":
            endpoint = (body["peer_ip"], body["peer_udp_port"])
            offer = self.offers.pop(mid, None)
            if offer:
                sender = offer[0]
                sender.send_fn = partial(self._send, endpoint=endpoint)
                self.peers[mid] = (None, endpoint, time.monotonic())
                self.senders[mid] = sender
            elif mid in self.peers and self.peers[mid][1] is None:
                self.peers[mid] = (self.peers[mid][0], endpoint, time.monotonic())
        elif kind == "FILE_REJECT":
            self._cancel(mid, "Peer declined offer")
        elif kind == "FILE_CANCEL":
            self._cancel(mid, body.get("reason", "Transfer cancelled"))

    def _cancel(self, mid, reason):
        self.peers.pop(mid, None)
        offer = self.offers.pop(mid, None)
        if offer:
            sender = offer[0]
            sender.cancel(reason)
            self._finished(sender)
        if mid in self.senders:
            self.senders[mid].cancel(reason)
        self._received(mid, None, reason)

    def _send_ack(self, raw):
        if self.send_control is None:
            self._send(raw)
            return
        packet, _ = parse_udp(raw)
        peer = self.peers.get(packet.msg_id)
        if peer and peer[1]:
            self._send(raw, peer[1])

    def _accept_packet(self, packet, body, address):
        if self.send_control is None:
            return True
        peer = self.peers.get(packet.msg_id)
        if peer is None or peer[1] != address:
            return False
        offer = peer[0]
        if (packet.ptype == ACK) != (offer is None):
            return False
        if packet.ptype == FILE_META and (
            not isinstance(body, dict) or any(body.get(key) != offer.get(key)
                for key in ("filename", "size", "from", "to"))
            or body.get("checksum") != offer["sha256"]
        ):
            return False
        self.peers[packet.msg_id] = (offer, address, time.monotonic())
        return True

    def _run(self):
        next_report = time.monotonic()
        try:
            while not self.stop_evt.is_set():
                for _ in range(128):
                    try:
                        kind, first, second = self.events.get_nowait()
                    except queue.Empty:
                        break
                    if kind == "send":
                        self._start_send(first)
                    elif kind == "control":
                        try:
                            self._handle_control(first)
                        except OSError:
                            log.debug("[%s] File control connection closed", self.username)
                    elif kind == "packet":
                        second, address = second
                        if not self._accept_packet(first, second, address):
                            continue
                        if first.ptype == ACK:
                            sender = self.senders.get(first.msg_id)
                            if sender:
                                sender.on_packet(first, second)
                        elif first.ptype in (FILE_META, FILE_CHUNK):
                            self._receive_packet(first, second)
                    elif kind == "left":
                        for mid, (offer, _, _) in list(self.peers.items()):
                            if offer and offer["from"] == first:
                                self._cancel(mid, f"Peer '{first}' went offline")
                        for mid, (sender, _) in list(self.offers.items()):
                            if sender.to == first:
                                self._cancel(mid, f"Peer '{first}' went offline")
                        for sender in self.senders.values():
                            if sender.to == first:
                                sender.cancel(f"Peer '{first}' went offline")
                        for mid, entry in list(self._incoming.items()):
                            if entry["record"]["peer"] == first:
                                self._received(mid, None, f"Peer '{first}' went offline")
                now = time.monotonic()
                for mid, (_, deadline) in list(self.offers.items()):
                    if now >= deadline:
                        self._cancel(mid, "Offer timed out")
                for mid, (_, _, last_seen) in list(self.peers.items()):
                    if now - last_seen >= FILE_SESSION_TIMEOUT:
                        self._cancel(mid, "UDP transfer timed out")
                for mid, sender in list(self.senders.items()):
                    try:
                        sender.tick()
                    except (OSError, ValueError) as exc:
                        sender.cancel(str(exc))
                    if sender.done:
                        self._finished(sender)
                        del self.senders[mid]
                        self.peers.pop(mid, None)
                    else:
                        self._emit_progress(mid, self._sender_record(sender))
                        if now >= next_report:
                            self._report(sender)
                self.receiver.tick()
                for mid, entry in list(self._incoming.items()):
                    if entry["record"]["status"] in TERMINAL and now - entry["last_seen"] >= 30:
                        del self._incoming[mid]
                        self._last_progress.pop(mid, None)
                if self.senders or any(e["record"]["status"] not in TERMINAL
                                       for e in self._incoming.values()):
                    self.active_evt.set()
                else:
                    self.active_evt.clear()
                if now >= next_report:
                    next_report = now + 1.0
                self.stop_evt.wait(0.01)
        except Exception:
            log.exception("[%s] File worker stopped unexpectedly", self.username)
        finally:
            with self._pending_lock:
                self.stop_evt.set()
            for sender in self.senders.values():
                sender.cancel("Client disconnected")
                self._finished(sender)
            self.senders.clear()
            for mid in list(self.offers):
                self._cancel(mid, "Client disconnected")
            self.peers.clear()
            # Publish failures for sends accepted but not started before shutdown.
            while True:
                try:
                    kind, first, _ = self.events.get_nowait()
                except queue.Empty:
                    break
                if kind == "send":
                    mid, to, path = first
                    self._emit(self._record(mid, "send", to, path.name,
                                            status="failed", error="Client disconnected"))
            for mid in list(self._incoming):
                self._received(mid, None, "Client disconnected")
            self._incoming.clear()
            self._last_progress.clear()
            with self._pending_lock:
                self._pending.clear()
            self.receiver.close()

    def close(self):
        with self._pending_lock:
            self.stop_evt.set()
        if self.thread is not threading.current_thread():
            self.thread.join(timeout=5)
