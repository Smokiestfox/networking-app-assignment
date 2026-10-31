"""Congestion control cho FileSender: CUBIC (cwnd, ssthresh) + ước lượng RTT/RTO (RFC 6298).

Cách dùng trong FileSender (đơn vị cwnd = số mảnh FILE_CHUNK):

    cc = CubicController()
    while còn mảnh:
        while cc.can_send(len(inflight)):      # gửi thêm khi cửa sổ còn chỗ
            gửi mảnh, ghi lại thời điểm gửi
        khi nhận ACK:
            cc.on_ack(rtt)                     # rtt = now - thời điểm gửi; mảnh đã gửi lại -> on_ack(None)
        khi mảnh quá cc.rto chưa có ACK:
            cc.on_loss(sent_time)              # gọi cho từng mảnh timeout, nhiều mảnh cùng lượt chỉ tính 1 lần mất
"""

import time

INITIAL_CWND = 2.0     # cwnd ban đầu (mảnh)
MIN_CWND = 2.0         # không giảm dưới mức này
MAX_CWND = 64.0        # cwnd tối đa (mảnh)

CUBIC_C = 0.4          # hằng số C của hàm CUBIC (RFC 8312)
CUBIC_BETA = 0.7       # sau mất gói: cwnd = 0.7 * cwnd (giảm 30%)


class RttEstimator:
    """Ước lượng SRTT / RTTVAR / RTO theo RFC 6298."""

    ALPHA = 1 / 8
    BETA = 1 / 4
    K = 4

    def __init__(self, initial_rto=1.0, min_rto=0.2, max_rto=8.0):
        self.srtt = None
        self.rttvar = None
        self.min_rto = min_rto
        self.max_rto = max_rto
        self.rto = self._clamp(initial_rto)

    def _clamp(self, value):
        return max(self.min_rto, min(self.max_rto, value))

    def update(self, rtt):
        """Thêm một mẫu RTT (giây). Chỉ gọi với mảnh gửi lần đầu (Karn's algorithm)."""
        if rtt <= 0:
            return
        if self.srtt is None:                       # mẫu đầu tiên
            self.srtt = rtt
            self.rttvar = rtt / 2
        else:
            self.rttvar = (1 - self.BETA) * self.rttvar + self.BETA * abs(self.srtt - rtt)
            self.srtt = (1 - self.ALPHA) * self.srtt + self.ALPHA * rtt
        self.rto = self._clamp(self.srtt + self.K * self.rttvar)

    def backoff(self):
        """Timeout: nhân đôi RTO (exponential backoff), tối đa max_rto."""
        self.rto = self._clamp(self.rto * 2)


class CubicController:
    """Điều khiển cwnd/ssthresh theo CUBIC, kèm RTO thích ứng.

    Giao diện cho FileSender: can_send(inflight), on_ack(rtt), on_loss(), rto.
    """

    def __init__(self, clock=time.time, initial_rto=1.0, min_rto=0.2, max_rto=8.0,
                 initial_cwnd=INITIAL_CWND, max_cwnd=MAX_CWND):
        self._clock = clock
        self.max_cwnd = float(max_cwnd)
        self.cwnd = float(initial_cwnd)             # số mảnh được phép bay đồng thời
        self.ssthresh = self.max_cwnd               # slow start cho tới khi mất gói đầu tiên
        self.rtt = RttEstimator(initial_rto, min_rto, max_rto)
        self.w_max = 0.0                            # cwnd ngay trước lần giảm gần nhất
        self._epoch_start = None                    # mốc bắt đầu đợt tăng CUBIC
        self._k = 0.0
        self._last_loss_time = None                 # lần giảm cwnd gần nhất (để gộp mất gói cùng lượt)
        self.loss_events = 0

    # ---- giao diện cho FileSender ----
    @property
    def rto(self):
        return self.rtt.rto

    @property
    def in_slow_start(self):
        return self.cwnd < self.ssthresh

    def can_send(self, inflight):
        """True nếu số mảnh đang bay còn nhỏ hơn cwnd (làm tròn xuống, tối thiểu 1)."""
        return inflight < max(1, int(self.cwnd))

    def on_ack(self, rtt=None):
        """Một mảnh được ACK. rtt=None nếu mảnh đó đã gửi lại (không lấy mẫu RTT)."""
        if rtt is not None:
            self.rtt.update(rtt)
        if self.cwnd >= self.max_cwnd:
            self.cwnd = self.max_cwnd
            return
        if self.in_slow_start:
            self.cwnd = min(self.cwnd + 1.0, self.max_cwnd)    # +1 mảnh cho mỗi ACK
            return
        self._cubic_increase()

    def on_loss(self, sent_time=None):
        """Một mảnh timeout = mất gói. Trả True nếu tính là lần mất mới (đã giảm cwnd).

        Nhiều mảnh timeout cùng lượt chỉ giảm 1 lần: nếu truyền `sent_time` (thời điểm mảnh
        được gửi), mất gói của mảnh gửi trước lần giảm gần nhất bị bỏ qua; nếu không, bỏ qua
        các lần mất trong vòng 1 RTO kể từ lần giảm gần nhất.
        """
        now = self._clock()
        if self._last_loss_time is not None:
            if sent_time is not None:
                if sent_time <= self._last_loss_time:
                    return False
            elif now - self._last_loss_time < self.rto:
                return False
        self._last_loss_time = now
        self.loss_events += 1
        self.w_max = self.cwnd
        self.cwnd = max(MIN_CWND, self.cwnd * CUBIC_BETA)       # giảm 30%
        self.ssthresh = self.cwnd
        self._epoch_start = None
        self.rtt.backoff()
        return True

    # ---- CUBIC ----
    def _cubic_increase(self):
        now = self._clock()
        if self._epoch_start is None:
            self._epoch_start = now
            if self.cwnd < self.w_max:
                self._k = ((self.w_max - self.cwnd) / CUBIC_C) ** (1 / 3)
            else:
                self._k = 0.0
                self.w_max = self.cwnd
        t = now - self._epoch_start + (self.rtt.srtt or 0.0)
        target = CUBIC_C * (t - self._k) ** 3 + self.w_max      # W(t) = C(t-K)^3 + Wmax
        if target > self.cwnd:
            step = (target - self.cwnd) / self.cwnd             # tăng dần theo từng ACK
        else:
            step = 0.01 / self.cwnd                             # vùng chờ: tăng rất chậm
        self.cwnd = min(self.cwnd + min(step, 1.0), self.max_cwnd)

