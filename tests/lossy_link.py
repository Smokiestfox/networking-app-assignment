import random
import time
from protocol import parse_udp
from file_sender import FileSender
from file_receiver_demo import FileReceiver
from protocol import SeqCounter


class LossyLink:
    def __init__(self, loss_rate=0.0, duplicate_rate=0.0, seed=None):
        self.loss_rate = float(loss_rate)
        self.duplicate_rate = float(duplicate_rate)
        self.random = random.Random(seed)
        
        self.sender = None
        self.receiver = None

    def sender_send_fn(self, raw_bytes: bytes):
        if self.random.random() < self.loss_rate:
            return

        try:
            packet, body = parse_udp(raw_bytes)
        except Exception:
            return

        if self.receiver:
            self.receiver.on_packet(packet, body)

        if self.random.random() < self.duplicate_rate:
            if self.receiver:
                self.receiver.on_packet(packet, body)

    def receiver_send_fn(self, raw_bytes: bytes):
        if self.random.random() < self.loss_rate:
            return

        try:
            packet, body = parse_udp(raw_bytes)
        except Exception:
            return

        if self.sender:
            self.sender.on_packet(packet, body)

        if self.random.random() < self.duplicate_rate:
            if self.sender:
                self.sender.on_packet(packet, body)

    def setup_transfer(self, file_path, to_user="bob", username="alice", download_dir="downloads/test", receiver_cls=FileReceiver):
        seq_sender = SeqCounter()
        seq_receiver = SeqCounter()

        self.receiver = receiver_cls(
            send_fn=self.receiver_send_fn,
            download_dir=download_dir,
            username=to_user,
            seq=seq_receiver
        )

        self.sender = FileSender(
            path=file_path,
            to=to_user,
            username=username,
            send_fn=self.sender_send_fn,
            seq=seq_sender
        )
        return self.sender, self.receiver

    def run_transfer(self, timeout=30.0, tick_interval=0.01):
        if not self.sender or not self.receiver:
            raise RuntimeError("Phải gọi setup_transfer() trước khi chạy vòng lặp truyền file.")

        start_time = time.monotonic()
        self.sender.start()

        while not self.sender.done:
            if time.monotonic() - start_time > timeout:
                self.sender.cancel("Test transfer timeout")
                break
            
            self.sender.tick()
            self.receiver.tick()
            
            time.sleep(tick_interval)

        self.receiver.tick()
        return self.sender.success