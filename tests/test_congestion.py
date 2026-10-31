import pytest

from congestion import CubicController, RttEstimator, MAX_CWND, MIN_CWND


class FakeClock:
    def __init__(self):
        self.t = 100.0

    def __call__(self):
        return self.t


def make():
    clock = FakeClock()
    return CubicController(clock=clock), clock


def test_initial_state():
    cc, _ = make()
    assert cc.cwnd == 2
    assert cc.in_slow_start
    assert cc.can_send(0) and cc.can_send(1)
    assert not cc.can_send(2)


def test_slow_start_grows_one_per_ack_and_caps():
    cc, _ = make()
    for _ in range(200):
        cc.on_ack(0.01)
    assert cc.cwnd == MAX_CWND


def test_rtt_estimator_rfc6298():
    est = RttEstimator(initial_rto=1.0, min_rto=0.0, max_rto=60)
    est.update(0.1)
    assert est.srtt == pytest.approx(0.1)
    assert est.rttvar == pytest.approx(0.05)
    assert est.rto == pytest.approx(0.1 + 4 * 0.05)
    est.update(0.2)
    assert est.rttvar == pytest.approx(0.75 * 0.05 + 0.25 * 0.1)
    assert est.srtt == pytest.approx(0.875 * 0.1 + 0.125 * 0.2)


def test_no_rtt_sample_for_retransmitted_chunk():
    cc, _ = make()
    cc.on_ack(None)
    assert cc.rtt.srtt is None
    cc.on_ack(0.05)
    assert cc.rtt.srtt == pytest.approx(0.05)


def test_loss_reduces_cwnd_30_percent():
    cc, _ = make()
    for _ in range(18):
        cc.on_ack(0.01)                   # cwnd = 20
    assert cc.cwnd == 20
    assert cc.on_loss() is True
    assert cc.cwnd == pytest.approx(14.0)
    assert cc.ssthresh == pytest.approx(14.0)
    assert not cc.in_slow_start


def test_cwnd_never_below_minimum():
    cc, clock = make()
    for _ in range(5):
        clock.t += 100
        cc.on_loss()
    assert cc.cwnd == MIN_CWND


def test_multiple_timeouts_in_same_round_count_once():
    cc, clock = make()
    for _ in range(18):
        cc.on_ack(0.01)
    sent = clock.t - 0.5
    assert cc.on_loss(sent) is True
    rto_after = cc.rto
    # 3 mảnh khác cùng lượt (gửi trước lần giảm) timeout tiếp: không giảm thêm, không backoff thêm
    assert cc.on_loss(sent) is False
    assert cc.on_loss(sent - 0.1) is False
    assert cc.cwnd == pytest.approx(14.0)
    assert cc.rto == rto_after
    assert cc.loss_events == 1
    # không truyền sent_time: gộp trong 1 RTO
    assert cc.on_loss() is False
    # mảnh gửi sau lần giảm mà timeout => mất gói mới
    clock.t += 1
    assert cc.on_loss(clock.t - 0.1) is True


def test_cubic_growth_after_loss_approaches_wmax():
    cc, clock = make()
    for _ in range(18):
        cc.on_ack(0.01)                   # cwnd = 20
    cc.on_loss()                          # w_max = 20, cwnd = 14
    previous = cc.cwnd
    for _ in range(400):
        clock.t += 0.01
        cc.on_ack(0.01)
        assert cc.cwnd >= previous
        previous = cc.cwnd
    assert cc.cwnd > 14.0
    assert cc.cwnd <= MAX_CWND


def test_rto_backoff_on_loss_and_bounds():
    cc, clock = make()
    cc.on_ack(0.05)
    base = cc.rto
    cc.on_loss()
    assert cc.rto == pytest.approx(min(base * 2, cc.rtt.max_rto))
    for _ in range(10):
        clock.t += 100
        cc.on_loss()
    assert cc.rto <= cc.rtt.max_rto

