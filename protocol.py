import json
import socket
import struct
from dataclasses import dataclass

HEADER_FORMAT = "!IBIB"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

MAX_PAYLOAD_SIZE = 1024 * 1024  # 1 MiB

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

# Header flags
FLAG_NONE = 0
FLAG_ENCRYPTED = 1 << 0  # 0b00000001

@dataclass
class Header:
    length: int
    type: int
    req_id: int
    flags: int = FLAG_NONE

    def encode(self) -> bytes:
        "Header: 10 bytes"

        if not 0 <= self.length <= MAX_PAYLOAD_SIZE:
            raise ValueError("Invalid payload length")

        if not 0 <= self.type <= 255:
            raise ValueError("Message type must fit in 1 byte")

        if not 0 <= self.req_id <= 0xFFFFFFFF:
            raise ValueError("Request ID must fit in 4 bytes")

        if not 0 <= self.flags <= 255:
            raise ValueError("Flags must fit in 1 byte")

        return struct.pack(
            HEADER_FORMAT,
            self.length,
            self.type,
            self.req_id,
            self.flags
        )

    @classmethod
    def decode(cls, data: bytes) -> "Header":
        "Turn 10B into header"

        if len(data) != HEADER_SIZE:
            raise ValueError(
                f"Expected {HEADER_SIZE} bytes, got {len(data)}"
            )

        length, msg_type, req_id, flags = struct.unpack(HEADER_FORMAT, data)

        if length > MAX_PAYLOAD_SIZE:
            raise ValueError("Payload exceeds maximum permitted size")

        return cls(
            length=length,
            type=msg_type,
            req_id=req_id,
            flags=flags
        )

def send_frame(
    sock: socket.socket,
    msg_type: int,
    req_id: int,
    payload: dict,
    flags: int = FLAG_NONE
) -> None:
    "Payload: dict -> JSON and send 1 frame"

    payload_bytes = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":")
    ).encode("utf-8")

    if len(payload_bytes) > MAX_PAYLOAD_SIZE:
        raise ValueError("Payload exceeds maximum permitted size")

    header = Header(
        length=len(payload_bytes),
        type=msg_type,
        req_id=req_id,
        flags=flags
    )

    sock.sendall(header.encode() + payload_bytes)

def recv_exact(sock: socket.socket, size: int) -> bytes:
    "Read exactly size bytes."

    data = bytearray()

    while len(data) < size:
        chunk = sock.recv(size - len(data))

        if not chunk:
            raise ConnectionError(
                "Connection closed before the frame was complete"
            )

        data.extend(chunk)

    return bytes(data)

def recv_frame(sock: socket.socket) -> tuple[Header, dict]:
    "Receive and decode one JSON frame."

    header_bytes = recv_exact(sock, HEADER_SIZE)
    header = Header.decode(header_bytes)

    payload_bytes = recv_exact(sock, header.length)

    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("Invalid JSON payload") from error

    if not isinstance(payload, dict):
        raise ValueError("JSON payload must be an object")

    return header, payload

if __name__ == "__main__":
    header = Header(
        length=5,
        type=MessageType.HELLO,
        req_id=1,
        flags=FLAG_NONE
    )

    encoded = header.encode()
    decoded = Header.decode(encoded)

    print("Header size:", HEADER_SIZE)
    print("Encoded bytes:", encoded)
    print("Decoded header:", decoded)