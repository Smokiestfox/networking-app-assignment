"""Client file worker: disk/hash/retry work never blocks the CLI or UDP listener."""

import logging
import queue
import random
import threading
import time
from pathlib import Path
from importlib import import_module

from file_sender import FileSender
from protocol import ACK, FILE_CHUNK, FILE_META

log = logging.getLogger("client.files")


def load_receiver_class(demo=False):
    """Use production by default; never silently substitute the demo receiver."""
    module_name = "file_receiver_demo" if demo else "file_receiver"
    try:
        return import_module(module_name).FileReceiver
    except ModuleNotFoundError as exc:
        if not demo and exc.name == module_name:
            return None
        raise  # a missing dependency inside a receiver is an implementation error


def loss_rate_from_env(value):
    rate = float(value)
    if not 0 <= rate < 1:
        raise ValueError("SIMULATE_LOSS must be in [0, 1)")
    return rate


class FileTransfers:
    def __init__(self, username, send_fn, seq, download_dir, *, loss_rate=0,
                 stats=None, seed=None, demo_receiver=False):
        self.username = username
        self.send_fn = send_fn
        self.seq = seq
        self.stats = stats if stats is not None else {}
        self.loss_rate = loss_rate_from_env(loss_rate)
        self.random = random.Random(seed)
        self.senders = {}
        self.events = queue.Queue(maxsize=4096)
        self.stop_evt = threading.Event()
        self.active_evt = threading.Event()
        receiver_cls = load_receiver_class(demo_receiver)
        self.receiver = (receiver_cls(self._send, download_dir, username=username,
                                     seq=seq, on_result=self._received) if receiver_cls else None)
        self.stats["receiver_backend"] = "demo" if demo_receiver else (
            "production" if self.receiver is not None else "unavailable")
        if demo_receiver:
            log.warning("[%s] DEMO receiver enabled: file_receiver_demo.py (reference only)", username)
        elif self.receiver is None:
            log.warning("[%s] file_receiver.py is not available; receiving is disabled. "
                        "Use --demo-receiver for reference tests. Chat and sending remain available.", username)
        self.thread = threading.Thread(target=self._run, name=f"files-{username}", daemon=True)
        self.thread.start()

    def _send(self, raw):
        # Drop file traffic only, including outgoing receiver ACKs. Chat/HB are unchanged.
        if self.random.random() < self.loss_rate:
            self.stats["file_packets_dropped"] = self.stats.get("file_packets_dropped", 0) + 1
            return
        self.send_fn(raw)

    def send_file(self, to, path):
        if not to or to in ("*", self.username):
            raise ValueError("Usage: /send <other username> <file path>")
        self.events.put_nowait(("send", to, Path(path)))

    def on_packet(self, packet, body):
        try:
            self.events.put_nowait(("packet", packet, body))
        except queue.Full:
            pass  # sender recovers with RTO; keep the network listener responsive

    def peer_left(self, username):
        try:
            self.events.put_nowait(("left", username, None))
        except queue.Full:
            pass  # ACK inactivity still bounds the transfer

    def _received(self, mid, path, error):
        if error:
            log.error("[%s] Receive %s failed: %s", self.username, mid, error)
            self.stats["files_receive_failed"] = self.stats.get("files_receive_failed", 0) + 1
        else:
            self.stats["files_received"] = self.stats.get("files_received", 0) + 1
            self.stats.setdefault("received_paths", []).append(str(path))
            print(f"\n  [FILE RECEIVED] SHA-256 matched: {path}", flush=True)

    def _report(self, sender):
        filled = int(sender.progress * 20)
        label = f"[{'#' * filled}{'.' * (20 - filled)}] {sender.progress:.0%}"
        log.info("[%s -> %s] %s cwnd=%.2f RTO=%.3fs inflight=%d retries=%d",
                 self.username, sender.to, label, sender.controller.cwnd,
                 sender.controller.rto, len(sender.inflight), sender.retransmissions)

    def _finished(self, sender):
        key = "files_sent" if sender.success else "files_send_failed"
        self.stats[key] = self.stats.get(key, 0) + 1
        result = {"success": sender.success, "error": sender.error,
                  "checksum": sender.checksum, "size": sender.size,
                  "retransmissions": sender.retransmissions,
                  "peak_cwnd": sender.peak_cwnd,
                  "loss_events": sender.controller.loss_events}
        self.stats.setdefault("send_results", []).append(result)
        self._report(sender)
        if sender.success:
            print(f"\n  [FILE SENT] {sender.path.name} -> {sender.to}: "
                  f"SHA-256 matched and saved by receiver ({sender.checksum})", flush=True)
        else:
            log.error("[%s] File %s -> %s failed: %s", self.username,
                      sender.path.name, sender.to, sender.error)

    def _run(self):
        next_report = time.monotonic()
        try:
            while not self.stop_evt.is_set():
                # Bound queue work so a busy incoming transfer cannot starve retries.
                for _ in range(128):
                    try:
                        kind, first, second = self.events.get_nowait()
                    except queue.Empty:
                        break
                    if kind == "send":
                        try:
                            if len(self.senders) >= 8:
                                raise ValueError("Already sending 8 files; wait for one to finish")
                            sender = FileSender(second, first, self.username, self._send, seq=self.seq)
                            self.senders[sender.msg_id] = sender
                        except (OSError, ValueError) as exc:
                            self.stats["files_send_failed"] = self.stats.get("files_send_failed", 0) + 1
                            log.error("[%s] Cannot send %s: %s", self.username, second, exc)
                    elif kind == "packet":
                        if first.ptype == ACK:
                            sender = self.senders.get(first.msg_id)
                            if sender:
                                sender.on_packet(first, second)
                        elif first.ptype in (FILE_META, FILE_CHUNK) and self.receiver is not None:
                            self.receiver.on_packet(first, second)
                    elif kind == "left":
                        for sender in self.senders.values():
                            if sender.to == first:
                                sender.cancel(f"Peer '{first}' went offline")
                now = time.monotonic()
                for mid, sender in list(self.senders.items()):
                    try:
                        sender.tick()
                    except (OSError, ValueError) as exc:
                        sender.cancel(str(exc))
                    if sender.done:
                        self._finished(sender)
                        del self.senders[mid]
                    elif now >= next_report:
                        self._report(sender)
                if self.receiver is not None:
                    self.receiver.tick()
                if self.senders or (self.receiver is not None and self.receiver.is_receiving):
                    self.active_evt.set()
                else:
                    self.active_evt.clear()
                if now >= next_report:
                    next_report = now + 1.0
                self.stop_evt.wait(0.01)
        except Exception:
            log.exception("[%s] File worker stopped unexpectedly", self.username)
        finally:
            for sender in self.senders.values():
                sender.cancel("Client disconnected")
                self._finished(sender)
            self.senders.clear()
            if self.receiver is not None:
                self.receiver.close()

    def close(self):
        self.stop_evt.set()
        self.thread.join(timeout=5)
