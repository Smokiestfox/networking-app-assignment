import hashlib
import tempfile
from pathlib import Path
import pytest
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