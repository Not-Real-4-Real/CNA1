from dataclasses import dataclass, field
import threading
RTO_CAP = 60.0  # Maximum RTO value in seconds
@dataclass
class Delivery:
    done: threading.Event = field(default_factory=threading.Event)
    success: bool = False

@dataclass
class SentPacket:
    data: bytes
    sent_at: float | None = None
    retries: int = 0
    delivery: Delivery | None = None

# Create a class to track RTT and RTO values in order to control the retransmission timeout (RTO)
@dataclass
class RTTTracker:
    rtt: float | None = None
    rttvar: float | None = None
    rto: float = 1.0  

    def observe(self, sample:float) -> None:
        # if sample is less than or equal to zero, we cannot update the RTT values
        if sample <=0:
            return
        #if this is the first RTT sample, init rtt and rttvar
        if self.rtt is None:
            self.rtt = sample
            self.rttvar = sample / 2
        else: 
            self.rttvar = (1 - 0.25) * self.rttvar + 0.25 * abs(self.rtt - sample)
            self.rtt = (1 - 0.125) * self.rtt + 0.125 * sample
        self.rto = self.rtt + max(0.1, 4 * self.rttvar)  # Ensure a minimum RTO of 100ms
    def backoff(self) -> None:
        self.rto = min(self.rto * 2, RTO_CAP)

    


from dataclasses import dataclass, field
import threading
RTO_CAP = 60.0  # Maximum RTO value in seconds
@dataclass
class Delivery:
    done: threading.Event = field(default_factory=threading.Event)
    success: bool = False

@dataclass
class SentPacket:
    data: bytes
    sent_at: float
    retries: int = 0
    delivery: Delivery | None = None

# Create a class to track RTT and RTO values in order to control the retransmission timeout (RTO)
@dataclass
class RTTTracker:
    rtt: float | None = None
    rttvar: float | None = None
    rto: float = 1.0  

    def observe(self, sample:float) -> None:
        # if sample is less than or equal to zero, we cannot update the RTT values
        if sample <=0:
            return
        #if this is the first RTT sample, init rtt and rttvar
        if self.rtt is None:
            self.rtt = sample
            self.rttvar = sample / 2
        else: 
            self.rttvar = (1 - 0.25) * self.rttvar + 0.25 * abs(self.rtt - sample)
            self.rtt = (1 - 0.125) * self.rtt + 0.125 * sample
        self.rto = self.rtt + max(0.1, 4 * self.rttvar)  # Ensure a minimum RTO of 100ms
    def backoff(self) -> None:
        self.rto = min(self.rto * 2, RTO_CAP)

    


