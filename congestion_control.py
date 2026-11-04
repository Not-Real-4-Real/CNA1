from dataclasses import dataclass, field
import time
import os

# @dataclass
# class RenoController:
#     cwnd: float = 2.0
#     ssthresh: float = 16.0
#     max_cwnd: float = 64.0
#     def on_ack(self):
#         if self.cwnd < self.ssthresh:
#             self.cwnd += 1.0  
#         else:
#             self.cwnd += 1.0 / self.cwnd
#     def on_loss(self):
#         # if the traffic is on congestion and getting packet loss, we need to reduce the congestion window to avoid
#         self.ssthresh = max(self.cwnd/2.0,2.0)
#         self.cwnd = self.ssthresh
#     def on_timeout(self):
#         self.ssthresh = max(self.cwnd/2.0,2.0)
#         self.cwnd = 1.0
#     def update_rtt(self, sample:float) -> None:
#         pass

@dataclass
class CubicController:
    cwnd: float = 2.0
    ssthresh: float = 16.0
    max_cwnd: float = 64.0
    C:float = 0.4
    beta_cubic: float = 0.7
    w_max: float = 2.0
    epoch_start: float | None = None
    epoch_cwnd: float = 2.0
    w_est: float = 2.0
    srtt: float = 0.1

    def update_rtt(self, sample:float) -> None:
        if sample >0:
            self.srtt = (1-0.125) * self.srtt + 0.125 * sample
    def on_ack(self):
        if self.cwnd < self.ssthresh:
            self.cwnd = min(self.cwnd + 1.0, self.max_cwnd)
            return
        time_now = time.monotonic()
        if self.epoch_start is None:
            self.epoch_start = time_now
            self.epoch_cwnd = self.cwnd
            self.w_max = self.cwnd
        k = (max(0.0, (self.w_max - self.epoch_cwnd)/self.C))**(1/3)
        time_epoch = time_now - self.epoch_start
        #increse function
        w_cubic = self.C * (time_epoch - k)**3 + self.w_max
        increment = max(0.0, w_cubic - self.cwnd) / self.cwnd
        self.cwnd += increment

    def on_loss(self) -> None:
        self.w_max = self.cwnd
        self.cwnd = max(2.0, self.cwnd * self.beta_cubic)
        self.ssthresh = self.cwnd
        self.epoch_start = None

    def on_timeout(self) -> None:
        self.ssthresh = max(2.0, self.cwnd * self.beta_cubic)
        self.cwnd = 1.0
        self.w_max = 0.0
        self.epoch_start = None
    def window_size(self) -> int:
        return max(1, int(self.cwnd))


Controller = CubicController
# Controller = (RenoController if os.getenv("CNA_CONGESTION", "cubic").lower() == "reno"
#               else CubicController)

        


