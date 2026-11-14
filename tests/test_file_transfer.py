import hashlib
import tempfile
import threading
import time
from pathlib import Path
import pytest
from file_transfers import FileTransfers
from protocol import SeqCounter, new_id, pack_file_chunk, pack_file_meta, parse_udp
from tests.lossy_link import LossyLink

@pytest.fixture
def temp_env():
    with tempfile.TemporaryDirectory() as tmpdir:
        d = Path(tmpdir)
        src = d / "source.bin"
        download_dir = d / "downloads"
        yield src, download_dir


def test_small_file_transfer_success(temp_env):
    src, download_dir = temp_env
    data = b"Hello, world! Reliable UDP file transfer test."
    src.write_bytes(data)

    link = LossyLink(loss_rate=0.0, seed=42)
    sender, receiver = link.setup_transfer(src, download_dir=download_dir)

    success = link.run_transfer(timeout=10.0)
    assert success is True
    assert sender.success is True

    received_files = list(download_dir.glob("*"))
    assert len(received_files) == 1
    received_data = received_files[0].read_bytes()
    assert hashlib.sha256(received_data).hexdigest() == sender.checksum


def test_one_mb_file_transfer_with_loss(temp_env):
    src, download_dir = temp_env
    size = 512 * 1024
    content = (bytes(range(256)) * ((size + 255) // 256))[:size]
    src.write_bytes(content)
    expected_hash = hashlib.sha256(content).hexdigest()

    from congestion import CubicController
    cc = CubicController(initial_rto=0.08, min_rto=0.03, max_rto=0.3)
    link = LossyLink(loss_rate=0.08, seed=42)
    sender, receiver = link.setup_transfer(src, download_dir=download_dir, controller=cc)

    success = link.run_transfer(timeout=60.0)
    assert success is True
    assert sender.success is True

    received_files = list(download_dir.glob("*"))
    assert len(received_files) == 1
    received_data = received_files[0].read_bytes()
    assert hashlib.sha256(received_data).hexdigest() == expected_hash


class SilentReceiver:
    def __init__(self, send_fn, download_dir, **kwargs):
        self.send_fn = send_fn
        self.download_dir = download_dir

    @property
    def is_receiving(self):
        return True

    def on_packet(self, packet, body):
        pass

    def tick(self):
        pass

    def close(self):
        pass


def test_receiver_silent_causes_sender_timeout(temp_env):
    src, download_dir = temp_env
    src.write_bytes(b"Timeout test content.")

    link = LossyLink(loss_rate=0.0)
    
    sender, receiver = link.setup_transfer(
        src, 
        download_dir=download_dir, 
        receiver_cls=SilentReceiver
    )
    sender.timeout = 0.5

    success = link.run_transfer(timeout=5.0, tick_interval=0.05)

    assert success is False
    assert sender.done is True
    assert sender.success is False
    assert "timeout" in sender.error.lower() or "unavailable" in sender.error.lower()



def _wait_event(events, lock, predicate, timeout=3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with lock:
            for ev in reversed(events):
                if predicate(ev):
                    return ev
        time.sleep(0.01)
    with lock:
        raise AssertionError(f"Timeout waiting for event; received={events}")


def _make_worker(download_dir):
    events = []
    lock = threading.Lock()
    sent_raw = []

    def on_event(ev):
        with lock:
            events.append(ev)

    worker = FileTransfers(
        "bob", sent_raw.append, SeqCounter(), download_dir,
        on_event=on_event, quiet=True
    )
    return worker, events, lock


def test_receive_progress_ignores_duplicate_chunks_and_completes_after_save(tmp_path):
    worker, events, lock = _make_worker(tmp_path)
    seq = SeqCounter()
    mid = new_id()
    data = b"A" * 700 + b"B" * 100
    checksum = hashlib.sha256(data).hexdigest()

    try:
        meta_pkt, meta_body = parse_udp(
            pack_file_meta("bob", "alice", "demo.bin", len(data), 2, checksum, seq.next(), msg_id=mid)
        )
        c0_pkt, c0_body = parse_udp(pack_file_chunk(mid, 0, 2, data[:700], seq.next()))
        c1_pkt, c1_body = parse_udp(pack_file_chunk(mid, 1, 2, data[700:], seq.next()))

        worker.on_packet(meta_pkt, meta_body)
        _wait_event(events, lock, lambda e: e["status"] == "transferring" and e["bytes_transferred"] == 0)

        worker.on_packet(c0_pkt, c0_body)
        ev1 = _wait_event(events, lock, lambda e: e["bytes_transferred"] == 700)
        assert ev1["status"] == "transferring"
        assert 0.0 < ev1["progress"] < 1.0

        # Chờ qua khoảng throttle 0.2s rồi gửi trùng chunk 0 nhiều lần -> không được tăng tiến độ
        time.sleep(0.25)
        with lock:
            count_before_dup = len(events)
        worker.on_packet(c0_pkt, c0_body)
        worker.on_packet(c0_pkt, c0_body)
        time.sleep(0.08)
        with lock:
            assert len(events) == count_before_dup
            assert events[-1]["bytes_transferred"] == 700
            assert events[-1]["progress"] < 1.0

        # Nhận chunk cuối -> SHA-256 khớp + lưu xong mới báo completed và 1.0
        worker.on_packet(c1_pkt, c1_body)
        done = _wait_event(events, lock, lambda e: e["status"] == "completed")
        assert done["progress"] == 1.0
        assert done["bytes_transferred"] == len(data)
        assert Path(done["path"]).read_bytes() == data

        # Chunk cuối bị gửi lại sau khi đã xong -> không đổi tiến độ hay ghi đè file
        with lock:
            count_after_done = len(events)
        worker.on_packet(c1_pkt, c1_body)
        time.sleep(0.08)
        with lock:
            assert len(events) == count_after_done
        assert len(list(tmp_path.iterdir())) == 1
    finally:
        worker.close()


def test_receive_progress_sha256_mismatch_never_completes(tmp_path):
    worker, events, lock = _make_worker(tmp_path)
    seq = SeqCounter()
    mid = new_id()
    data = b"C" * 700 + b"D" * 50
    bad_checksum = "0" * 64

    try:
        meta_pkt, meta_body = parse_udp(
            pack_file_meta("bob", "alice", "corrupt.bin", len(data), 2, bad_checksum, seq.next(), msg_id=mid)
        )
        c0_pkt, c0_body = parse_udp(pack_file_chunk(mid, 0, 2, data[:700], seq.next()))
        c1_pkt, c1_body = parse_udp(pack_file_chunk(mid, 1, 2, data[700:], seq.next()))

        worker.on_packet(meta_pkt, meta_body)
        worker.on_packet(c0_pkt, c0_body)
        _wait_event(events, lock, lambda e: e["bytes_transferred"] == 700)

        worker.on_packet(c1_pkt, c1_body)
        failed = _wait_event(events, lock, lambda e: e["status"] == "failed")
        assert "SHA-256 mismatch" in failed["error"]
        assert failed["progress"] < 1.0
        assert failed["path"] is None
        with lock:
            assert all(e["status"] != "completed" and e["progress"] < 1.0 for e in events)
        assert list(tmp_path.iterdir()) == []
    finally:
        worker.close()


def test_receive_progress_save_error_never_completes(tmp_path):
    bad_dir = tmp_path / "not_a_directory"
    bad_dir.write_text("blocking file")
    worker, events, lock = _make_worker(bad_dir)
    seq = SeqCounter()
    mid = new_id()
    data = b"hello save error"
    checksum = hashlib.sha256(data).hexdigest()

    try:
        meta_pkt, meta_body = parse_udp(
            pack_file_meta("bob", "alice", "fail.bin", len(data), 1, checksum, seq.next(), msg_id=mid)
        )
        c0_pkt, c0_body = parse_udp(pack_file_chunk(mid, 0, 1, data, seq.next()))

        worker.on_packet(meta_pkt, meta_body)
        worker.on_packet(c0_pkt, c0_body)
        failed = _wait_event(events, lock, lambda e: e["status"] == "failed")
        assert "Cannot save file" in failed["error"]
        assert failed["progress"] < 1.0
        assert failed["path"] is None
        with lock:
            assert all(e["status"] != "completed" and e["progress"] < 1.0 for e in events)
    finally:
        worker.close()


def test_receive_progress_empty_file_and_peer_left_blocks_late_chunks(tmp_path):
    # 1. File rỗng 0 byte vẫn hoàn tất 1.0 sau khi lưu thành công
    worker, events, lock = _make_worker(tmp_path)
    seq = SeqCounter()
    mid_empty = new_id()
    empty_hash = hashlib.sha256(b"").hexdigest()

    try:
        meta_pkt, meta_body = parse_udp(
            pack_file_meta("bob", "alice", "empty.txt", 0, 1, empty_hash, seq.next(), msg_id=mid_empty)
        )
        c0_pkt, c0_body = parse_udp(pack_file_chunk(mid_empty, 0, 1, b"", seq.next()))
        worker.on_packet(meta_pkt, meta_body)
        worker.on_packet(c0_pkt, c0_body)
        done = _wait_event(events, lock, lambda e: e["id"] == str(mid_empty) and e["status"] == "completed")
        assert done["progress"] == 1.0
        assert done["bytes_transferred"] == 0
        assert Path(done["path"]).read_bytes() == b""

        # 2. Peer offline (peer_left) -> FileReceiver phải khóa phiên, chặn chunk trễ tới sau
        mid_left = new_id()
        data = b"X" * 700 + b"Y" * 50
        left_hash = hashlib.sha256(data).hexdigest()
        m_pkt, m_body = parse_udp(
            pack_file_meta("bob", "alice", "aborted.bin", len(data), 2, left_hash, seq.next(), msg_id=mid_left)
        )
        p0, b0 = parse_udp(pack_file_chunk(mid_left, 0, 2, data[:700], seq.next()))
        p1, b1 = parse_udp(pack_file_chunk(mid_left, 1, 2, data[700:], seq.next()))

        worker.on_packet(m_pkt, m_body)
        worker.on_packet(p0, b0)
        _wait_event(events, lock, lambda e: e["id"] == str(mid_left) and e["bytes_transferred"] == 700)

        worker.peer_left("alice")
        aborted = _wait_event(events, lock, lambda e: e["id"] == str(mid_left) and e["status"] == "failed")
        assert "went offline" in aborted["error"]

        # Gửi tiếp mảnh cuối sau khi peer đã offline -> không được lưu file hay báo completed
        worker.on_packet(p1, b1)
        time.sleep(0.08)
        assert len(list(tmp_path.glob("*aborted.bin"))) == 0
        with lock:
            assert all(
                not (e["id"] == str(mid_left) and e["status"] == "completed")
                for e in events
            )
    finally:
        worker.close()