import json
import struct
from dataclasses import dataclass

RDT_HEADER_FORMAT = "!16sIIBB"
RDT_HEADER_SIZE = struct.calcsize(RDT_HEADER_FORMAT)  # 26 bytes

APP_HEADER_FORMAT = "!IBIB"
APP_HEADER_SIZE = struct.calcsize(APP_HEADER_FORMAT)  # 10 bytes

MAX_DATAGRAM_SIZE = 1200

MAX_PAYLOAD_SIZE = (
    MAX_DATAGRAM_SIZE
    - RDT_HEADER_SIZE
    - APP_HEADER_SIZE
)  # 1164 bytes

class RDTType:
    OPEN = 1
    OPEN_ACK = 2
    DATA = 3
    ACK = 4
    CLOSE = 5
    CLOSE_ACK = 6
    HEARTBEAT = 7

class MessageType:
    HELLO = 1
    WELCOME = 2

    AUTH = 3
    AUTH_OK = 4

    USER_LIST = 5
    USER_OK = 6

    MSG_BROADCAST = 7
    MSG_DIRECT = 8
    MSG_DELIVER = 9
    MSG_SENT = 10

    USER_JOIN = 11
    USER_LEAVE = 12

    ERROR = 13
    PING = 14
    PONG = 15
    LOGOUT = 16

    FILE_OFFER = 17
    FILE_ACCEPT = 18
    FILE_DATA = 19
    FILE_END = 20
    FILE_RESULT = 21

    FILE_SEND = 22
    FILE_SENT = 23

# Application header flags
FLAG_NONE = 0
FLAG_ENCRYPTED = 1 << 0

@dataclass
class RDTHeader:
    session_id: bytes
    seq_num: int = 0
    ack_num: int = 0
    type: int = RDTType.OPEN
    flags: int = 0

    def encode(self) -> bytes:
        if not isinstance(self.session_id, bytes):
            raise ValueError("Session ID must be bytes")

        if len(self.session_id) != 16:
            raise ValueError(
                "Session ID must be exactly 16 bytes"
            )

        if not 0 <= self.seq_num <= 0xFFFFFFFF:
            raise ValueError(
                "Sequence number must fit in 4 bytes"
            )

        if not 0 <= self.ack_num <= 0xFFFFFFFF:
            raise ValueError(
                "ACK number must fit in 4 bytes"
            )

        # Version 1 reserves all transport flag bits.
        if self.flags != 0:
            raise ValueError(
                "RDT flags must be zero in version 1"
            )

        return struct.pack(
            RDT_HEADER_FORMAT,
            self.session_id,
            self.seq_num,
            self.ack_num,
            self.type,
            self.flags,
        )

    @classmethod
    def decode(cls, data: bytes) -> "RDTHeader":
        if len(data) != RDT_HEADER_SIZE:
            raise ValueError(
                f"Expected {RDT_HEADER_SIZE} RDT header bytes, got {len(data)}"
            )

        session_id, seq_num, ack_num, packet_type, flags = (
            struct.unpack(RDT_HEADER_FORMAT, data)
        )

        header = cls(
            session_id=session_id,
            seq_num=seq_num,
            ack_num=ack_num,
            type=packet_type,
            flags=flags,
        )

        # Reuse the same validation rules for decoded headers.
        header.encode()

        return header

@dataclass
class AppHeader:
    length: int
    type: int
    req_id: int
    flags: int = FLAG_NONE

    def encode(self) -> bytes:
        if not 0 <= self.length <= MAX_PAYLOAD_SIZE:
            raise ValueError("Invalid application payload length")

        if not 0 <= self.type <= 255:
            raise ValueError(
                "Application message type must fit in 1 byte"
            )

        if not 0 <= self.req_id <= 0xFFFFFFFF:
            raise ValueError(
                "Request ID must fit in 4 bytes"
            )

        if not 0 <= self.flags <= 255:
            raise ValueError(
                "Application flags must fit in 1 byte"
            )

        return struct.pack(
            APP_HEADER_FORMAT,
            self.length,
            self.type,
            self.req_id,
            self.flags,
        )

    @classmethod
    def decode(cls, data: bytes) -> "AppHeader":
        if len(data) != APP_HEADER_SIZE:
            raise ValueError(
                f"Expected {APP_HEADER_SIZE} application header bytes, "
                f"got {len(data)}"
            )

        length, msg_type, req_id, flags = (
            struct.unpack(APP_HEADER_FORMAT, data)
        )

        header = cls(
            length=length,
            type=msg_type,
            req_id=req_id,
            flags=flags,
        )

        # Validate the decoded values too.
        header.encode()

        return header

@dataclass
class Datagram:
    rdt: RDTHeader
    app: AppHeader | None = None
    payload: dict | None = None

def encode_datagram(
    rdt: RDTHeader,
    app: AppHeader | None = None,
    payload: dict | None = None,
) -> bytes:
    rdt_bytes = rdt.encode()

    if rdt.type == RDTType.DATA:
        if app is None or payload is None:
            raise ValueError(
                "RDT_DATA requires an application header and payload"
            )

        if not isinstance(payload, dict):
            raise ValueError(
                "Application payload must be a dictionary"
            )

        payload_bytes = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")

        if len(payload_bytes) > MAX_PAYLOAD_SIZE:
            raise ValueError(
                "Application payload exceeds maximum size"
            )

        actual_app = AppHeader(
            length=len(payload_bytes),
            type=app.type,
            req_id=app.req_id,
            flags=app.flags,
        )

        datagram = (
            rdt_bytes
            + actual_app.encode()
            + payload_bytes
        )

    else:
        # Control packets contain only the RDT header in v1.
        if app is not None or payload is not None:
            raise ValueError(
                "RDT control packets cannot contain an "
                "application header or payload in version 1"
            )

        datagram = rdt_bytes

    if len(datagram) > MAX_DATAGRAM_SIZE:
        raise ValueError("UDP datagram exceeds maximum size")

    return datagram

def decode_datagram(data: bytes) -> Datagram:
    if len(data) > MAX_DATAGRAM_SIZE:
        raise ValueError("UDP datagram exceeds maximum size")

    if len(data) < RDT_HEADER_SIZE:
        raise ValueError("Datagram is shorter than the RDT header")

    rdt = RDTHeader.decode(data[:RDT_HEADER_SIZE])

    # Control packet: exactly one RDT header.
    if rdt.type != RDTType.DATA:
        if len(data) != RDT_HEADER_SIZE:
            raise ValueError(
                "RDT control packet must contain exactly "
                "the RDT header"
            )

        return Datagram(rdt=rdt)

    # DATA packet: RDT header + application header + JSON.
    minimum_size = RDT_HEADER_SIZE + APP_HEADER_SIZE

    if len(data) < minimum_size:
        raise ValueError(
            "RDT_DATA is missing its application header"
        )

    app_start = RDT_HEADER_SIZE
    app_end = app_start + APP_HEADER_SIZE

    app = AppHeader.decode(data[app_start:app_end])
    payload_bytes = data[app_end:]

    if len(payload_bytes) != app.length:
        raise ValueError(
            f"Payload length mismatch: expected {app.length} bytes, "
            f"got {len(payload_bytes)}"
        )

    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("Invalid UTF-8 JSON payload") from error

    if not isinstance(payload, dict):
        raise ValueError(
            "Application JSON payload must be an object"
        )

    return Datagram(
        rdt=rdt,
        app=app,
        payload=payload,
    )