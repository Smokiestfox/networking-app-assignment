"""Reliable unicast file sender. Network I/O is supplied by send_fn(bytes)."""

import hashlib
import logging
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from config import CHUNK_SIZE, FILE_SESSION_TIMEOUT, MAX_RETRIES
from congestion import CubicController
from protocol import ACK, SeqCounter, new_id, pack_file_chunk, pack_file_meta

MAX_CHUNKS = 65535  # total is uint16 in the existing wire header
DEFAULT_MAX_RETRIES = 20  # tolerate independent data and ACK loss on large transfers
log = logging.getLogger("client.files")


@dataclass
class Pending:
    raw: bytes
    seq: int
    chunk: int
    size: int
    sent_at: float
    retries: int = 0


class FileSender:
    def __init__(self, path, to, username, send_fn, *, seq=None, controller=None,
                 clock=time.monotonic, timeout=FILE_SESSION_TIMEOUT,
                 max_retries=DEFAULT_MAX_RETRIES, msg_id=None):
        if not to or to in ("*", username):
            raise ValueError("File transfer requires one other username")
        self.path = Path(path)
        # Check the wire limit before reading a potentially huge file into memory.
        if self.path.stat().st_size > CHUNK_SIZE * MAX_CHUNKS:
            raise ValueError(f"File exceeds the {CHUNK_SIZE * MAX_CHUNKS}-byte wire limit")
        self.data = self.path.read_bytes()
        self.size = len(self.data)
        self.total = max(1, (self.size + CHUNK_SIZE - 1) // CHUNK_SIZE)
        if self.total > MAX_CHUNKS:
            raise ValueError("File grew beyond the wire limit while being read")
        self.checksum = hashlib.sha256(self.data).hexdigest()
        self.msg_id = msg_id or new_id()
        self.to = to
        self.username = username
        self.send_fn = send_fn
        self.seq = seq or SeqCounter()
        self.controller = controller or CubicController(clock=clock)
        self.clock = clock
        self.timeout = timeout
        self.max_retries = max_retries
        self.inflight = {}
        self.next_chunk = 0
        self.meta_acked = False
        self.started = False
        self.done = False
        self.success = False
        self.error = None
        self.bytes_acked = 0
        self.chunks_acked = 0
        self.retransmissions = 0
        self.peak_cwnd = self.controller.cwnd
        self.last_progress = clock()
        self._lock = threading.RLock()

    @property
    def progress(self):
        return self.chunks_acked / self.total

    def _send(self, pending):
        self.inflight[pending.seq] = pending
        self.send_fn(pending.raw)

    def start(self):
        with self._lock:
            if self.started or self.done:
                return
            self.started = True
            self.last_progress = self.clock()
            seq = self.seq.next()
            raw = pack_file_meta(self.to, self.username, self.path.name, self.size,
                                 self.total, self.checksum, seq, msg_id=self.msg_id)
            self._send(Pending(raw, seq, 0, 0, self.clock()))

    def on_packet(self, packet, body=None):
        with self._lock:
            if self.done or packet.ptype != ACK or packet.msg_id != self.msg_id:
                return False
            pending = self.inflight.get(packet.ack)
            if pending is None or packet.chunk != pending.chunk:
                return False
            del self.inflight[packet.ack]
            now = self.clock()
            self.last_progress = now
            # Karn: the peer cannot tell us which copy of a retransmission it ACKed.
            if not self.meta_acked:
                self.meta_acked = True
            else:
                self.controller.on_ack(None if pending.retries else now - pending.sent_at)
                self.peak_cwnd = max(self.peak_cwnd, self.controller.cwnd)
                self.bytes_acked += pending.size
                self.chunks_acked += 1
                if self.chunks_acked == self.total:
                    self.done = self.success = True
                    self.data = b""
            return True

    def tick(self):
        with self._lock:
            if self.done:
                return
            if not self.started:
                self.start()
                return
            now = self.clock()
            if now - self.last_progress >= self.timeout:
                self.cancel(f"No new ACK for {self.timeout:g}s / peer unavailable")
                return
            # Snapshot the old RTO: one batch of timeouts is one congestion event.
            expired = [p for p in self.inflight.values()
                       if now - p.sent_at >= self.controller.rto]
            if expired:
                if any(p.retries >= self.max_retries for p in expired):
                    self.cancel("Retry limit reached / peer unavailable or checksum rejected")
                    return
                before = self.controller.cwnd
                # One call per batch; let the controller gate losses within one RTO.
                if self.controller.on_loss():
                    log.info("[%s -> %s] timeout batch: cwnd %.2f -> %.2f, RTO=%.3fs",
                             self.username, self.to, before, self.controller.cwnd, self.controller.rto)
                for pending in expired:
                    if pending.seq not in self.inflight:
                        continue
                    pending.retries += 1
                    pending.sent_at = now
                    self.retransmissions += 1
                    self.send_fn(pending.raw)  # keep original seq and ciphertext
            if not self.meta_acked:
                return
            while self.next_chunk < self.total and self.controller.can_send(len(self.inflight)):
                index = self.next_chunk
                self.next_chunk += 1
                data = self.data[index * CHUNK_SIZE:(index + 1) * CHUNK_SIZE]
                seq = self.seq.next()
                raw = pack_file_chunk(self.msg_id, index, self.total, data, seq)
                self._send(Pending(raw, seq, index, len(data), self.clock()))

    def cancel(self, reason="Transfer cancelled"):
        with self._lock:
            if self.done:
                return
            self.done = True
            self.error = reason
            self.inflight.clear()
            self.data = b""
