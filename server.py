import socket
import time
from collections import deque
from dataclasses import dataclass, field

from udp_protocol import *

HOST = "127.0.0.1"
PORT = 5000
BUFFER_SIZE = 1201

RETRANSMISSION_TIMEOUT = 1.0   # seconds
MAX_RETRANSMISSIONS = 5
HEARTBEAT_TIMEOUT = 8.0        # seconds without valid client activity

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

@dataclass
class SenderState:
    "Layer 2: our outgoing DATA toward one client."
    seq: int = 0                                   # seq of the next DATA to send
    outbox: deque = field(default_factory=deque)   # queued (msg_type, req_id, payload)
    in_flight: bytes | None = None                 # exact bytes awaiting an ACK
    in_flight_seq: int | None = None
    sent_at: float | None = None
    retransmissions: int = 0

@dataclass
class ReceiverState:
    "Layer 2: the client's incoming DATA toward us."
    expected_seq: int = 0

@dataclass
class Session:
    "Layer 3: identity + liveness. Layer 2 state hangs off it."
    address: tuple
    session_id: bytes
    last_seen: float = field(default_factory=time.monotonic)
    sender: SenderState = field(default_factory=SenderState)
    receiver: ReceiverState = field(default_factory=ReceiverState)

# Key: (client address, session ID)
sessions: dict[tuple, Session] = {}

# Upward hooks, wired up at the bottom of layers 3 and 4.
message_handler = None
failure_handler = None

# Layer 1: Transport
def transport_send(address: tuple, packet: bytes) -> None:
    sock.sendto(packet, address)

def transport_receive():
    "Read and decode one datagram. Returns (address, datagram), or None"
    data, address = sock.recvfrom(BUFFER_SIZE)

    print(f"\n[DATAGRAM RECEIVED] {address} | bytes={len(data)}")

    try:
        return address, decode_datagram(data)
    except ValueError as error:
        print(f"[INVALID DATAGRAM] {address}: {error}")
        return None

# Layer 2: Reliable delivery (per session)
def send_control(address: tuple, session_id: bytes, packet_type: int) -> None:
    "Send a header-only RDT packet (OPEN_ACK / CLOSE_ACK). Not sequenced."
    transport_send(
        address,
        encode_datagram(RDTHeader(session_id=session_id, type=packet_type)),
    )

def send_ack(session: Session, ack_num: int) -> None:
    transport_send(
        session.address,
        encode_datagram(
            rdt=RDTHeader(
                session_id=session.session_id,
                ack_num=ack_num,
                type=RDTType.ACK,
            )
        ),
    )
    print(f"[ACK SENT] {session.address} | ack_num={ack_num}")

def send_reliable(session: Session, msg_type: int, req_id: int, payload: dict) -> None:
    """Queue a message for reliable delivery. It is sent immediately if nothing is
    in flight, otherwise as soon as the previous DATA has been ACKed. Messages
    are never dropped just because an earlier one is still unACKed."""

    session.sender.outbox.append((msg_type, req_id, payload))
    pump_outbox(session)

def pump_outbox(session: Session) -> None:
    "If the channel is idle, transmit the next queued message."
    sender = session.sender

    if sender.in_flight is not None or not sender.outbox:
        return

    msg_type, req_id, payload = sender.outbox.popleft()

    packet = encode_datagram(
        rdt=RDTHeader(
            session_id=session.session_id,
            seq_num=sender.seq,
            type=RDTType.DATA,
        ),
        app=AppHeader(length=0, type=msg_type, req_id=req_id),
        payload=payload,
    )

    # Keep the exact bytes so a retransmission is identical.
    sender.in_flight = packet
    sender.in_flight_seq = sender.seq
    sender.sent_at = time.monotonic()
    sender.retransmissions = 0

    transport_send(session.address, packet)

    print(
        f"[DATA SENT] {session.address} "
        f"| seq={sender.seq} "
        f"| type={msg_type} "
        f"| req_id={req_id} "
        f"| awaiting ACK"
    )

def handle_ack(session: Session, datagram: Datagram) -> None:
    "Process an ACK for our in-flight DATA."
    sender = session.sender
    ack_num = datagram.rdt.ack_num

    if sender.in_flight is None:
        print(
            f"[UNEXPECTED ACK] {session.address} "
            f"| ack_num={ack_num} | no outstanding DATA"
        )
        return

    # A stale or unrelated ACK must not clear the in-flight packet.
    if ack_num != sender.in_flight_seq:
        print(
            f"[IGNORED ACK] {session.address} "
            f"| ack_num={ack_num} | expected={sender.in_flight_seq}"
        )
        return

    print(
        f"[ACK ACCEPTED] {session.address} "
        f"| ack_num={ack_num} | retries={sender.retransmissions}"
    )

    sender.in_flight = None
    sender.in_flight_seq = None
    sender.sent_at = None
    sender.retransmissions = 0
    sender.seq ^= 1          # alternate 0 -> 1 -> 0 -> ...

    pump_outbox(session)     # send the next queued message, if any

def handle_data(session: Session, datagram: Datagram) -> None:
    "ACK incoming DATA, drop duplicates, pass new messages upward."
    receiver = session.receiver
    seq = datagram.rdt.seq_num

    if seq not in (0, 1):
        print(f"[REJECTED DATA] Invalid seq={seq} from {session.address}")
        return

    # Always ACK, even duplicates, in case our previous ACK was lost.
    send_ack(session, seq)

    if seq != receiver.expected_seq:
        print(f"[DUPLICATE DATA] {session.address} | seq={seq} | ACK resent")
        return

    receiver.expected_seq ^= 1

    if datagram.app is None:
        print(f"[REJECTED DATA] Missing application header from {session.address}")
        return

    print(
        f"[DATA ACCEPTED] {session.address} "
        f"| seq={seq} | req_id={datagram.app.req_id}"
    )

    if message_handler is not None:
        message_handler(session, datagram)

def check_retransmission(session: Session) -> None:
    "Retransmit this session's in-flight DATA if its timer expired."
    sender = session.sender

    if sender.in_flight is None or sender.sent_at is None:
        return

    now = time.monotonic()

    if now - sender.sent_at < RETRANSMISSION_TIMEOUT:
        return

    if sender.retransmissions >= MAX_RETRANSMISSIONS:
        print(
            f"[TRANSMISSION FAILED] {session.address} "
            f"| seq={sender.in_flight_seq} | retries={sender.retransmissions}"
        )
        if failure_handler is not None:
            failure_handler(session)
        return

    transport_send(session.address, sender.in_flight)
    sender.retransmissions += 1
    sender.sent_at = now

    print(
        f"[RETRANSMIT] {session.address} "
        f"| seq={sender.in_flight_seq} "
        f"| retry={sender.retransmissions}/{MAX_RETRANSMISSIONS}"
    )

# Layer 3: Sessions
def handle_open(address: tuple, session_id: bytes) -> None:
    "Create a session, or repeat the response to a repeated OPEN."
    key = (address, session_id)
    session = sessions.get(key)

    if session is None:
        sessions[key] = Session(address=address, session_id=session_id)
        print(f"[SESSION OPENED] {address} | session={session_id.hex()}")
    else:
        session.last_seen = time.monotonic()
        print(f"[OPEN REPEATED] {address} | session={session_id.hex()}")

    send_control(address, session_id, RDTType.OPEN_ACK)

def handle_close(address: tuple, session_id: bytes) -> None:
    key = (address, session_id)

    if sessions.pop(key, None) is None:
        print(f"[CLOSE REPEATED] Unknown session from {address} | re-ACKing")
    else:
        print(
            f"[SESSION CLOSED] {address} "
            f"| session={session_id.hex()[:8]} "
            f"| active sessions={len(sessions)}"
        )

    send_control(address, session_id, RDTType.CLOSE_ACK)

def drop_session(session: Session, reason: str) -> None:
    sessions.pop((session.address, session.session_id), None)
    print(
        f"[SESSION DROPPED] {session.address} "
        f"| session={session.session_id.hex()[:8]} "
        f"| reason={reason} "
        f"| active sessions={len(sessions)}"
    )

def on_transmission_failed(session: Session) -> None:
    "Layer 2 gave up on this client, so invalidate the session."
    drop_session(session, "transmission failed")

failure_handler = on_transmission_failed

def check_session_timeouts() -> None:
    now = time.monotonic()

    # Snapshot: sessions are removed while iterating.
    for session in list(sessions.values()):
        inactive_for = now - session.last_seen
        if inactive_for > HEARTBEAT_TIMEOUT:
            drop_session(session, f"inactive {inactive_for:.1f}s")

def check_retransmissions() -> None:
    for session in list(sessions.values()):
        check_retransmission(session)

def dispatch_datagram(address: tuple, datagram: Datagram) -> None:
    "Route a decoded datagram to the right session / reliable handler."
    rdt = datagram.rdt

    if rdt.type == RDTType.OPEN:
        handle_open(address, rdt.session_id)
        return

    if rdt.type == RDTType.CLOSE:
        handle_close(address, rdt.session_id)
        return

    if rdt.type not in (RDTType.DATA, RDTType.ACK, RDTType.HEARTBEAT):
        print(f"[UNEXPECTED DATAGRAM] type={rdt.type} from {address}")
        return

    # Everything below needs an established session. Look it up once, here.
    session = sessions.get((address, rdt.session_id))

    if session is None:
        print(f"[REJECTED] Unknown session from {address} | type={rdt.type}")
        return

    session.last_seen = time.monotonic()

    if rdt.type == RDTType.DATA:
        handle_data(session, datagram)
    elif rdt.type == RDTType.ACK:
        handle_ack(session, datagram)

# Layer 4: Application
def handle_ping(session: Session, datagram: Datagram) -> None:
    req_id = datagram.app.req_id
    payload = datagram.payload

    print(f"[PING RECEIVED] {session.address} | req_id={req_id} | payload={payload}")

    send_reliable(
        session,
        MessageType.PONG,
        req_id,
        {
            "message": "Pong from server",
            "echo": payload.get("message"),
        },
    )

# Add new message types here (broadcast, direct, file transfer, ...).
MESSAGE_HANDLERS = {
    MessageType.PING: handle_ping,
}

def dispatch_message(session: Session, datagram: Datagram) -> None:
    "Called by layer 2 for each new DATA message; routes it by app type."
    if datagram.payload is None:
        print(f"[REJECTED DATA] Missing payload from {session.address}")
        return

    handler = MESSAGE_HANDLERS.get(datagram.app.type)

    if handler is None:
        print(f"[UNSUPPORTED APP TYPE] {datagram.app.type} from {session.address}")
        return

    handler(session, datagram)

message_handler = dispatch_message

# Main loop
def tick() -> None:
    "Periodic work: retransmission timers and session expiry."
    check_retransmissions()
    check_session_timeouts()

def run_server() -> None:
    sock.bind((HOST, PORT))
    sock.settimeout(0.1)

    print(f"UDP server listening on {HOST}:{PORT}")

    try:
        while True:
            try:
                received = transport_receive()
            except socket.timeout:
                tick()
                continue

            if received is not None:
                address, datagram = received
                dispatch_datagram(address, datagram)

            tick()

    except KeyboardInterrupt:
        print("\nServer shutting down.")

    finally:
        sock.close()


if __name__ == "__main__":
    run_server()