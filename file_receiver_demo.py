"""DEMO / REFERENCE ONLY: receiver for FileSender development tests.

Enable explicitly with --demo-receiver.
Keep the public class name FileReceiver so Hau can implement the same contract
in file_receiver.py. See docs/file-receiver-handoff.md for the integration API.
No direct sockets: send_fn accepts one encoded datagram.
"""

import hashlib
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path, PureWindowsPath

from config import CHUNK_SIZE, FILE_SESSION_TIMEOUT
from protocol import FILE_CHUNK, FILE_META, SeqCounter, pack_ack


def valid_file_meta(body):
    """Validation before receiver allocates a session."""
    if not isinstance(body, dict):
        return False
    if any(not isinstance(body.get(k), str) or not body[k].strip()
           for k in ("to", "from", "filename")):
        return False
    size, chunks, checksum = (body.get(k) for k in ("size", "chunks", "checksum"))
    if type(size) is not int or size < 0 or type(chunks) is not int:
        return False
    if not 1 <= chunks <= 65535 or chunks != max(1, (size + CHUNK_SIZE - 1) // CHUNK_SIZE):
        return False
    return (isinstance(checksum, str) and len(checksum) == 64
            and all(c in "0123456789abcdef" for c in checksum))


@dataclass
class Incoming:
    meta: dict
    last_seen: float
    chunks: dict = field(default_factory=dict)
    path: object = None
    complete: bool = False
    failed: bool = False


class FileReceiver:
    def __init__(self, send_fn, download_dir="downloads", *, username=None, seq=None,
                 clock=time.monotonic, timeout=FILE_SESSION_TIMEOUT, on_result=None,
                 max_sessions=16):
        self.send_fn = send_fn
        self.download_dir = Path(download_dir)
        self.username = username
        self.seq = seq or SeqCounter()
        self.clock = clock
        self.timeout = timeout
        self.on_result = on_result
        self.max_sessions = max_sessions
        self.sessions = {}
        self._lock = threading.RLock()

    @property
    def is_receiving(self):
        with self._lock:
            return any(not s.complete and not s.failed for s in self.sessions.values())

    def _ack(self, packet):
        self.send_fn(pack_ack(packet.seq, self.seq.next(), packet.msg_id, packet.chunk))

    def _notify(self, mid, session, error=None):
        if self.on_result:
            self.on_result(mid, session.path, error)

    def on_packet(self, packet, body):
        with self._lock:
            now = self.clock()
            if packet.ptype == FILE_META:
                if (not valid_file_meta(body) or packet.total != body["chunks"]
                        or (self.username and body["to"] != self.username)):
                    return False
                session = self.sessions.get(packet.msg_id)
                if session is None:
                    if len(self.sessions) >= self.max_sessions:
                        return False
                    session = Incoming(dict(body), now)
                    self.sessions[packet.msg_id] = session
                elif session.meta != body or session.failed:
                    return False
                session.last_seen = now
                self._ack(packet)  # duplicate META must not reset existing chunks
                return True
            if packet.ptype != FILE_CHUNK:
                return False
            session = self.sessions.get(packet.msg_id)
            if session is None or session.failed:
                return False
            total = session.meta["chunks"]
            if packet.total != total or not 0 <= packet.chunk < total or not isinstance(body, bytes):
                return False
            expected = (CHUNK_SIZE if packet.chunk < total - 1
                        else session.meta["size"] - CHUNK_SIZE * (total - 1))
            if len(body) != expected:
                return False
            session.last_seen = now
            if session.complete:
                # Tombstone: recover a lost final ACK without writing a second file.
                self._ack(packet)
                return True
            previous = session.chunks.get(packet.chunk)
            if previous is not None and previous != body:
                return False
            session.chunks[packet.chunk] = body
            if len(session.chunks) == total:
                data = b"".join(session.chunks[i] for i in range(total))
                if hashlib.sha256(data).hexdigest() != session.meta["checksum"]:
                    session.failed = True
                    session.chunks.clear()
                    self._notify(packet.msg_id, session, "SHA-256 mismatch")
                    return False  # do NOT ACK the chunk completing a corrupt file
                try:
                    self.download_dir.mkdir(parents=True, exist_ok=True)
                    # UUID prefix prevents collisions; basename handles Windows + POSIX paths.
                    name = PureWindowsPath(session.meta["filename"]).name
                    name = Path(name).name
                    name = "".join(c if c.isalnum() or c in "._- " else "_" for c in name)
                    name = name.strip(" .")[:120] or "file"
                    destination = self.download_dir / f"{packet.msg_id.hex}_{name}"
                    # Never overwrite an existing file, even after receiver restart.
                    with destination.open("xb") as out:
                        try:
                            out.write(data)
                        except OSError:
                            out.close()
                            destination.unlink(missing_ok=True)
                            raise
                    session.path = destination
                    session.complete = True
                    session.chunks.clear()
                except OSError as exc:
                    session.failed = True
                    session.chunks.clear()
                    self._notify(packet.msg_id, session, f"Cannot save file: {exc}")
                    return False
                self._notify(packet.msg_id, session)
            # Completion ACK is sent only after successful verification and save.
            self._ack(packet)
            return True

    def tick(self):
        with self._lock:
            now = self.clock()
            expired = [mid for mid, s in self.sessions.items()
                       if now - s.last_seen >= self.timeout]
            for mid in expired:
                session = self.sessions.pop(mid)
                if not session.complete and not session.failed:
                    self._notify(mid, session, "Receiver idle timeout")
            return expired

    def close(self):
        with self._lock:
            self.sessions.clear()
