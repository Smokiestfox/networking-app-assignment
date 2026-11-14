"""TCP rendezvous for direct UDP file transfers."""
import threading
import time
import uuid

from config import FILE_SESSION_TIMEOUT
from file_sender import CHUNK_SIZE, MAX_CHUNKS


class FileRendezvous:
    def __init__(self, registry, send):
        self.registry = registry
        self.send = send
        self.lock = threading.Lock()
        self.pending = {}

    def _notify(self, conn, body):
        try:
            self.send(conn, body)
            return True
        except OSError:
            return False

    def expire(self, departing=None):
        with self.lock:
            expired = [
                (mid, offer) for mid, offer in self.pending.items()
                if departing in (offer["from"], offer["to"]) or offer["deadline"] <= time.monotonic()
            ]
            for mid, _ in expired:
                del self.pending[mid]
        for mid, offer in expired:
            for name, conn in zip((offer["from"], offer["to"]), offer["connections"]):
                if name != departing:
                    self._notify(conn, {"type": "FILE_CANCEL", "transfer_id": mid,
                                        "reason": "Peer disconnected" if departing else "Offer timed out"})

    def handle(self, username, conn, body):
        try:
            mid = str(uuid.UUID(body.get("transfer_id")))
        except (TypeError, ValueError, AttributeError):
            return
        if body["type"] == "FILE_OFFER":
            self._offer(username, conn, mid, body)
            return
        with self.lock:
            offer = self.pending.get(mid)
            if offer is None or conn not in offer["connections"]:
                return
            if body["type"] != "FILE_CANCEL" and conn is not offer["connections"][1]:
                return
            del self.pending[mid]
        sender, receiver = offer["connections"]
        if body["type"] == "FILE_ACCEPT" and offer["deadline"] > time.monotonic():
            # Install receiver state before allowing the sender to transmit.
            for recipient, peer_index in ((receiver, 0), (sender, 1)):
                ip, port = offer["endpoints"][peer_index]
                if not self._notify(recipient, {"type": "FILE_READY", "transfer_id": mid,
                                                "peer": (offer["from"], offer["to"])[peer_index],
                                                "peer_ip": ip, "peer_udp_port": port}):
                    other = sender if recipient is receiver else receiver
                    self._notify(other, {"type": "FILE_CANCEL", "transfer_id": mid,
                                         "reason": "Peer disconnected"})
                    break
        elif body["type"] == "FILE_REJECT":
            self._notify(sender, {"type": "FILE_REJECT", "transfer_id": mid})
        else:
            reason = "Offer timed out" if body["type"] == "FILE_ACCEPT" else "Peer cancelled"
            notice = {"type": "FILE_CANCEL", "transfer_id": mid, "reason": reason}
            self._notify(sender if conn is receiver else receiver, notice)
            if body["type"] == "FILE_ACCEPT":
                self._notify(receiver, notice)

    def _offer(self, username, conn, mid, body):
        target, name, size, checksum = (body.get(key) for key in ("to", "filename", "size", "sha256"))
        if (not isinstance(target, str) or target in ("*", username)
                or not isinstance(name, str) or not name.strip()
                or type(size) is not int or not 0 <= size <= CHUNK_SIZE * MAX_CHUNKS
                or not isinstance(checksum, str) or len(checksum) != 64
                or any(char not in "0123456789abcdef" for char in checksum)):
            self._notify(conn, {"type": "FILE_CANCEL", "transfer_id": mid, "reason": "Invalid offer"})
            return
        public = {"type": "FILE_OFFER", "transfer_id": mid, "from": username, "to": target,
                  "filename": name, "size": size, "sha256": checksum}
        with self.lock, self.registry.lock:
            users = [self.registry.active_users.get(user) for user in (username, target)]
            available = all(u and u.get("conn") and u.get("udp_port") for u in users)
            if (available and users[0]["conn"] is conn and mid not in self.pending
                    and sum(conn in offer["connections"] for offer in self.pending.values()) < 8):
                offer = {**public, "deadline": time.monotonic() + FILE_SESSION_TIMEOUT,
                         "connections": tuple(u["conn"] for u in users),
                         "endpoints": tuple((u["ip"], u["udp_port"]) for u in users)}
                self.pending[mid] = offer
            else:
                offer = None
        if offer and self._notify(offer["connections"][1], public):
            return
        if offer:
            with self.lock:
                self.pending.pop(mid, None)
        self._notify(conn, {"type": "FILE_CANCEL", "transfer_id": mid,
                            "reason": "Peer unavailable or offer limit reached"})
