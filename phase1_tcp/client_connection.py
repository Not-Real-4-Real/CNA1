import socket
import threading
from dataclasses import dataclass, field


@dataclass
class ClientConnection:
    sock: socket.socket
    address: tuple
    authentication: bool = False
    username: str | None = None
    send_lock: threading.Lock = field(
        default_factory=threading.Lock
    )
