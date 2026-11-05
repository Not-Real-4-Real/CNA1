import socket
import time
import base64
import binascii
import uuid
import queue
import threading
from pathlib import Path
from collections import deque
from dataclasses import dataclass, field

from udp_protocol import *

HOST = "127.0.0.1"
PORT = 5000
BUFFER_SIZE = 1201

RETRANSMISSION_TIMEOUT = 1.0   # seconds
MAX_RETRANSMISSIONS = 5
PERSIST_INTERVAL = 1.0
HEARTBEAT_TIMEOUT = 30.0        # seconds without valid client activity

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

@dataclass
class OutstandingPacket:
    packet: bytes
    sent_at: float
    retransmissions: int = 0


@dataclass
class SenderState:
    "Layer 2: Selective Repeat sender state for one peer."
    base: int = 0
    next_seq: int = 0
    outbox: deque = field(default_factory=deque)
    outstanding: dict[int, OutstandingPacket] = field(default_factory=dict)
    acked: set[int] = field(default_factory=set)
    peer_rwnd: int = 0
    last_window_probe: float = 0.0
    lock: threading.Lock = field(default_factory=threading.Lock)


@dataclass
class ReceiverState:
    "Layer 2: Selective Repeat receiver state for one peer."
    base: int = 0
    buffered: dict[int, Datagram] = field(default_factory=dict)

@dataclass
class Session:
    "Layer 3: identity + liveness. Layer 2 state hangs off it."
    address: tuple
    session_id: bytes
    authenticated: bool = False
    last_seen: float = field(default_factory=time.monotonic)
    sender: SenderState = field(default_factory=SenderState)
    receiver: ReceiverState = field(default_factory=ReceiverState)

    incoming_data: queue.Queue = field(
        default_factory=lambda: queue.Queue(maxsize=DATA_BUFFER_SIZE)
    )

    queue_lock: threading.Lock = field(default_factory=threading.Lock)
    handler_scheduled: bool = False

DATA_BUFFER_SIZE = 32

running = threading.Event()
running.set()

# The handler thread waits here for sessions that have buffered DATA. This queue schedules sessions.
ready_sessions = queue.Queue()

# Key: (client address, session ID)
sessions: dict[tuple, Session] = {}

# Upward hooks. A lower layer calls them; a higher layer fills them in.
message_handler = None           # layer 2 -> layer 4: new in-order DATA
failure_handler = None           # layer 2 -> layer 3: retries exhausted
session_opened_handler = None    # layer 3 -> layer 4: new session created
session_closed_handler = None    # layer 3 -> layer 4: session removed (any reason)

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
def send_control(session: Session, packet_type: int) -> None:
    "Send a header-only RDT control packet."
    transport_send(
        session.address,
        encode_datagram(
            RDTHeader(
                session_id=session.session_id,
                rwnd=receive_window(session),
                type=packet_type,
            )
        ),
    )


def send_ack(session: Session, ack_num: int) -> None:
    rwnd = receive_window(session)
    transport_send(
        session.address,
        encode_datagram(
            rdt=RDTHeader(
                session_id=session.session_id,
                ack_num=ack_num,
                rwnd=rwnd,
                type=RDTType.ACK,
            )
        ),
    )
    print(
        f"[ACK SENT] {session.address} "
        f"| ack_num={ack_num} | rwnd={rwnd}"
    )


def send_window_update(session: Session) -> None:
    """Advertise receive capacity after the application queue is drained."""
    send_control(session, RDTType.WINDOW_UPDATE)


def send_reliable(session: Session, msg_type: int, req_id: int, payload: dict) -> None:
    """Queue an application message for reliable SR delivery to one session."""
    sender = session.sender
    with sender.lock:
        sender.outbox.append((msg_type, req_id, payload))
        pump_outbox(session)
        print(
            f"[OUTBOX QUEUE] recipient={session.address} "
            f"| type={msg_type} | req_id={req_id} "
            f"| outstanding={len(sender.outstanding)} "
            f"| queued={len(sender.outbox)}"
        )


def pump_outbox(session: Session) -> None:
    """Fill the sender window while both SR and peer receive windows allow."""
    sender = session.sender

    while sender.outbox:
        sequence_span = seq_distance(sender.next_seq, sender.base)
        if sequence_span >= SR_WINDOW_SIZE:
            break

        # rwnd describes free receiver-buffer slots. Subtract packets still in
        # flight because they may not yet be reflected in the latest rwnd.
        if len(sender.outstanding) >= sender.peer_rwnd:
            break

        msg_type, req_id, payload = sender.outbox[0]
        seq = sender.next_seq
        packet = encode_datagram(
            rdt=RDTHeader(
                session_id=session.session_id,
                seq_num=seq,
                rwnd=receive_window(session),
                type=RDTType.DATA,
            ),
            app=AppHeader(length=0, type=msg_type, req_id=req_id),
            payload=payload,
        )

        sender.outbox.popleft()
        sender.outstanding[seq] = OutstandingPacket(
            packet=packet,
            sent_at=time.monotonic(),
        )
        sender.next_seq = seq_add(sender.next_seq)
        transport_send(session.address, packet)

        print(
            f"[DATA SENT] {session.address} | seq={seq} "
            f"| type={msg_type} | req_id={req_id} "
            f"| outstanding={len(sender.outstanding)}/{SR_WINDOW_SIZE}"
        )


def handle_ack(session: Session, datagram: Datagram) -> None:
    """Process an individual ACK and slide the sender base when possible."""
    sender = session.sender
    ack_num = datagram.rdt.ack_num

    with sender.lock:
        sender.peer_rwnd = max(0, datagram.rdt.rwnd)
        pending = sender.outstanding.pop(ack_num, None)

        if pending is None:
            print(
                f"[IGNORED ACK] {session.address} | ack_num={ack_num} "
                f"| no matching outstanding packet"
            )
        else:
            sender.acked.add(ack_num)
            print(
                f"[ACK ACCEPTED] {session.address} | ack_num={ack_num} "
                f"| retries={pending.retransmissions} "
                f"| rwnd={sender.peer_rwnd}"
            )

            while sender.base in sender.acked:
                sender.acked.remove(sender.base)
                sender.base = seq_add(sender.base)

        pump_outbox(session)


def handle_data(session: Session, datagram: Datagram) -> None:
    "Process an in-order DATA datagram already accepted by the SR receiver."
    if datagram.app is None:
        print(f"[REJECTED DATA] Missing application header from {session.address}")
        return

    print(
        f"[DATA ACCEPTED] {session.address} "
        f"| seq={datagram.rdt.seq_num} | req_id={datagram.app.req_id}"
    )

    if message_handler is not None:
        message_handler(session, datagram)
    else:
        print("[ERROR] message_handler is None")


def check_retransmission(session: Session) -> None:
    """Retransmit every expired packet independently (Selective Repeat)."""
    sender = session.sender
    failed = False
    now = time.monotonic()

    with sender.lock:
        for seq, pending in list(sender.outstanding.items()):
            if now - pending.sent_at < RETRANSMISSION_TIMEOUT:
                continue

            if pending.retransmissions >= MAX_RETRANSMISSIONS:
                print(
                    f"[TRANSMISSION FAILED] {session.address} "
                    f"| seq={seq} | retries={pending.retransmissions}"
                )
                failed = True
                break

            transport_send(session.address, pending.packet)
            pending.retransmissions += 1
            pending.sent_at = now
            print(
                f"[RETRANSMIT] {session.address} | seq={seq} "
                f"| retry={pending.retransmissions}/{MAX_RETRANSMISSIONS}"
            )

        if (
            not failed
            and sender.outbox
            and sender.peer_rwnd <= 0
            and now - sender.last_window_probe >= PERSIST_INTERVAL
        ):
            sender.last_window_probe = now
            try:
                transport_send(
                    session.address,
                    encode_datagram(
                        RDTHeader(
                            session_id=session.session_id,
                            rwnd=receive_window(session),
                            type=RDTType.WINDOW_PROBE,
                        )
                    ),
                )
                print(f"[WINDOW PROBE] {session.address} | peer rwnd=0")
            except OSError as error:
                print(f"[WINDOW PROBE ERROR] {session.address}: {error}")

    if failed and failure_handler is not None:
        failure_handler(session)


def receive_window(session: Session) -> int:
    """Free slots across both the in-order queue and out-of-order SR buffer."""
    with session.queue_lock:
        occupied = session.incoming_data.qsize() + len(session.receiver.buffered)
        return max(0, DATA_BUFFER_SIZE - occupied)


def _drain_receive_buffer_locked(session: Session) -> None:
    """Move contiguous receive-buffer packets to the handler queue.

    Caller must hold session.queue_lock.
    """
    receiver = session.receiver
    while receiver.base in receiver.buffered:
        try:
            session.incoming_data.put_nowait(receiver.buffered[receiver.base])
        except queue.Full:
            break
        del receiver.buffered[receiver.base]
        receiver.base = seq_add(receiver.base)


def receive_data(session: Session, datagram: Datagram) -> None:
    """Selective Repeat receiver: retain and ACK any packet inside its window."""
    seq = datagram.rdt.seq_num
    receiver = session.receiver
    accepted = False
    duplicate = False
    should_schedule = False

    with session.queue_lock:
        if seq in receiver.buffered:
            duplicate = True
        elif seq_in_window(seq, receiver.base, SR_WINDOW_SIZE):
            occupied = session.incoming_data.qsize() + len(receiver.buffered)
            if occupied >= DATA_BUFFER_SIZE:
                print(
                    f"[RECEIVE BUFFER FULL] {session.address} "
                    f"| seq={seq} | capacity={DATA_BUFFER_SIZE}"
                )
                return

            receiver.buffered[seq] = datagram
            accepted = True
            _drain_receive_buffer_locked(session)

            if not session.incoming_data.empty() and not session.handler_scheduled:
                session.handler_scheduled = True
                should_schedule = True

        elif seq_is_recent_duplicate(seq, receiver.base, SR_WINDOW_SIZE):
            duplicate = True
        else:
            print(
                f"[REJECTED DATA] {session.address} | seq={seq} "
                f"outside receive window (base={receiver.base}, size={SR_WINDOW_SIZE})"
            )
            return

    if should_schedule:
        ready_sessions.put_nowait(session)

    # Advertise the sender's receive window from this DATA header.
    with session.sender.lock:
        session.sender.peer_rwnd = max(0, datagram.rdt.rwnd)
        pump_outbox(session)

    # Selective Repeat ACKs out-of-order packets immediately after buffering.
    send_ack(session, seq)

    if duplicate:
        print(f"[DUPLICATE DATA] {session.address} | seq={seq} | ACK resent")
    elif accepted:
        print(
            f"[DATA BUFFERED] {session.address} | seq={seq} "
            f"| queued={session.incoming_data.qsize()}/{DATA_BUFFER_SIZE} "
            f"| recv_base={receiver.base}"
        )


def drain_receive_buffer(session: Session) -> None:
    with session.queue_lock:
        _drain_receive_buffer_locked(session)


def update_peer_window(session: Session, rwnd: int) -> None:
    session.sender.peer_rwnd = max(0, rwnd)


def update_window_and_pump(session: Session, rwnd: int) -> None:
    with session.sender.lock:
        update_peer_window(session, rwnd)
        pump_outbox(session)


def dispatch_datagram(address: tuple, datagram: Datagram) -> None:
    """Route a decoded datagram to the right session / reliable handler."""
    rdt = datagram.rdt

    if rdt.type == RDTType.OPEN:
        handle_open(address, datagram)
        return

    if rdt.type == RDTType.CLOSE:
        handle_close(address, rdt.session_id)
        return

    allowed_types = (
        RDTType.DATA,
        RDTType.ACK,
        RDTType.HEARTBEAT,
        RDTType.WINDOW_UPDATE,
        RDTType.WINDOW_PROBE,
    )
    if rdt.type not in allowed_types:
        print(f"[UNEXPECTED DATAGRAM] type={rdt.type} from {address}")
        return

    session = sessions.get((address, rdt.session_id))
    if session is None:
        print(f"[REJECTED] Unknown session from {address} | type={rdt.type}")
        return

    session.last_seen = time.monotonic()

    if rdt.type == RDTType.DATA:
        receive_data(session, datagram)
    elif rdt.type == RDTType.ACK:
        handle_ack(session, datagram)
    elif rdt.type == RDTType.WINDOW_UPDATE:
        update_window_and_pump(session, rdt.rwnd)
    elif rdt.type == RDTType.WINDOW_PROBE:
        send_window_update(session)
    # HEARTBEAT only refreshes last_seen.


def handler_loop() -> None:
    """Process accepted DATA sequentially across all sessions."""
    while running.is_set() or not ready_sessions.empty():
        try:
            session = ready_sessions.get(timeout=0.1)
        except queue.Empty:
            continue

        try:
            while True:
                try:
                    datagram = session.incoming_data.get_nowait()
                except queue.Empty:
                    with session.queue_lock:
                        if session.incoming_data.empty():
                            session.handler_scheduled = False
                            break
                    continue

                try:
                    handle_data(session, datagram)
                except Exception as error:
                    print(f"[HANDLER ERROR] {session.address} | {error}")
                finally:
                    session.incoming_data.task_done()
                    drain_receive_buffer(session)
                    if sessions.get(session_key(session)) is session:
                        try:
                            send_window_update(session)
                        except OSError as error:
                            print(f"[WINDOW UPDATE ERROR] {session.address}: {error}")
        finally:
            ready_sessions.task_done()

def receiver_loop() -> None:
    "Receive and dispatch datagrams; run periodic protocol maintenance."
    while running.is_set():
        try:
            received = transport_receive()

        except ConnectionResetError as error:
            # Windows may report ICMP Port Unreachable through recvfrom().
            print(f"[UDP RESET IGNORED] {error}")
            continue

        except socket.timeout:
            tick()
            continue

        except OSError as error:
            if running.is_set():
                print(f"[RECEIVER ERROR] {error}")
            break

        if received is not None:
            address, datagram = received
            dispatch_datagram(address, datagram)

        tick()

# Layer 3: Sessions
def session_key(session: Session) -> tuple:
    return (session.address, session.session_id)

def handle_open(address: tuple, datagram: Datagram) -> None:
    "Create a session, or repeat the response to a repeated OPEN."
    session_id = datagram.rdt.session_id
    key = (address, session_id)
    session = sessions.get(key)

    if session is None:
        session = Session(address=address, session_id=session_id)
        sessions[key] = session
        print(f"[SESSION OPENED] {address} | session={session_id.hex()}")

        # Only a genuinely new session is announced upward, never a repeat.
        if session_opened_handler is not None:
            session_opened_handler(session)

        send_control(session, RDTType.OPEN_ACK)
        update_window_and_pump(session, datagram.rdt.rwnd)

    else:
        session.last_seen = time.monotonic()
        print(f"[OPEN REPEATED] {address} | session={session_id.hex()}")
        send_control(session, RDTType.OPEN_ACK)
        update_window_and_pump(session, datagram.rdt.rwnd)

def remove_session(session: Session, reason: str) -> bool:
    """The single exit point for every session.
    Returns False if the session was already gone."""
    if sessions.pop(session_key(session), None) is None:
        return False

    print(
        f"[SESSION REMOVED] {session.address} "
        f"| session={session.session_id.hex()[:8]} "
        f"| reason={reason} "
        f"| active sessions={len(sessions)}"
    )

    if session_closed_handler is not None:
        session_closed_handler(session, reason)

    return True

def handle_close(address: tuple, session_id: bytes) -> None:
    session = sessions.get((address, session_id))

    if session is None:
        print(f"[CLOSE REPEATED] Unknown session from {address} | re-ACKing")
        transport_send(
            address,
            encode_datagram(
                RDTHeader(
                    session_id=session_id,
                    rwnd=0,
                    type=RDTType.CLOSE_ACK,
                )
            ),
        )
        return

    remove_session(session, "closed by client")
    send_control(session, RDTType.CLOSE_ACK)

def on_transmission_failed(session: Session) -> None:
    "Layer 2 gave up on this client, so invalidate the session."
    remove_session(session, "transmission failed")

failure_handler = on_transmission_failed

def check_session_timeouts() -> None:
    now = time.monotonic()

    # Snapshot: sessions are removed while iterating.
    for session in list(sessions.values()):
        inactive_for = now - session.last_seen
        if inactive_for > HEARTBEAT_TIMEOUT:
            remove_session(session, f"inactive {inactive_for:.1f}s")

def check_retransmissions() -> None:
    for session in list(sessions.values()):
        check_retransmission(session)

# Layer 4: Application
ACCOUNTS = {
    "alice": "alice123",
    "bob": "bob123",
    "charlie": "charlie123",
    "doug": "doug123",
    "ed": "ed123",
    "fly": "fly123",
}

# Client registry: who is online, by username. Layer 3 only knows sessions.
clients: dict[str, Session] = {}     # username -> session
client_names: dict[tuple, str] = {}  # session key -> username
guest_counter = 0

FILE_CHUNK_SIZE = 600
RECEIVED_DIR = Path("received_files")

@dataclass
class FileTransfer:
    transfer_id: int
    filename: str
    expected_size: int

    chunks: list[bytes] = field(default_factory=list)
    next_chunk_index: int = 0
    received_size: int = 0

# Key: (session_key, transfer_id)
file_transfers: dict[tuple, FileTransfer] = {}

# Key: (owner username, stored filename)
stored_files: dict[tuple[str, str], Path] = {}

def register_client(session: Session) -> str:
    "Assign a temporary username and store the connection."
    global guest_counter

    guest_counter += 1
    username = f"Guest{guest_counter}"

    clients[username] = session
    client_names[session_key(session)] = username

    print(f"[CLIENT REGISTERED] {username} | {session.address}")
    print(f"[ONLINE] {get_connected_users()}")
    return username

def unregister_client(session: Session) -> str | None:
    "Remove a connection. Returns its username, or None if it had none."
    username = client_names.pop(session_key(session), None)

    if username is None:
        return None

    clients.pop(username, None)

    print(f"[CLIENT UNREGISTERED] {username} | {session.address}")
    print(f"[ONLINE] {get_connected_users()}")
    return username

EVENT_REQ_ID = 0

def notify_users(msg_type: int, payload: dict, exclude: Session | None = None) -> None:
    "Send a one-way event to every registered client (optionally skipping one)."
    for session in list(clients.values()):
        if session is exclude:
            continue
        send_reliable(session, msg_type, EVENT_REQ_ID, payload)

def get_connected_users() -> list[str]:
    "Snapshot of registered usernames in join order. Safe to loop over."
    return list(clients)

def on_session_opened(session: Session) -> None:
    "A transport session has opened, but is not authenticated yet."
    print(
        f"[AUTH REQUIRED] {session.address} "
        f"| session={session.session_id.hex()[:8]}"
    )

def on_session_closed(session: Session, reason: str) -> None:
    # Discard incomplete file transfers belonging to this session.
    session_prefix = session_key(session)

    stale_transfers = [
        key
        for key in file_transfers
        if key[0] == session_prefix
    ]

    for key in stale_transfers:
        transfer = file_transfers.pop(key)
        print(
            f"[FILE TRANSFER ABORTED] {transfer.filename} "
            f"| reason={reason}"
        )

    username = unregister_client(session)
 
    # The leaver is already out of the registry, so only the others are told.
    if username is not None:
        notify_users(MessageType.USER_LEAVE, {"username": username})

session_opened_handler = on_session_opened
session_closed_handler = on_session_closed

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

def handle_broadcast(session: Session, datagram: Datagram) -> None:
    """Handle a client broadcast request."""
    req_id = datagram.app.req_id
    payload = datagram.payload
    message = payload.get("message")

    # 1. Validate the message.
    if not isinstance(message, str) or not message.strip():
        send_reliable(
            session,
            MessageType.ERROR,
            req_id,
            {
                "code": 400,
                "message": "Broadcast message must be a non-empty string",
            },
        )
        return

    message = message.strip()

    # 2. Identify the sender.
    sender_username = client_names.get(session_key(session))

    if sender_username is None:
        send_reliable(
            session,
            MessageType.ERROR,
            req_id,
            {
                "code": 401,
                "message": "Sender is not registered",
            },
        )
        return

    # 3. Snapshot recipients, excluding the sender.
    recipients = [
        (username, recipient_session)
        for username, recipient_session in list(clients.items())
        if recipient_session is not session
    ]

    # 4. Queue one reliable delivery per recipient.
    delivery_payload = {
        "from": sender_username,
        "message": message,
    }

    for username, recipient_session in recipients:
        send_reliable(
            recipient_session,
            MessageType.MSG_DELIVER,
            EVENT_REQ_ID,
            delivery_payload,
        )

        print(
            f"[BROADCAST QUEUED] {sender_username} -> {username}"
        )

    # 5. Acknowledge application-level acceptance to the sender.
    send_reliable(
        session,
        MessageType.MSG_SENT,
        req_id,
        {
            "message": "Broadcast accepted",
            "recipients": len(recipients),
        },
    )

    print(
        f"[BROADCAST] sender={sender_username} "
        f"| recipients={len(recipients)}"
    )

def handle_direct(session: Session, datagram: Datagram) -> None:
    """Handle a client broadcast request."""
    req_id = datagram.app.req_id
    payload = datagram.payload\

    receiver_username = payload.get("to")
    message = payload.get("message")

    if not isinstance(receiver_username, str) or not receiver_username.strip():
        send_reliable(
            session,
            MessageType.ERROR,
            req_id,
            {
                "code": 400,
                "message": "A recipient username is required",
            },
        )
        return
    
    if not isinstance(message, str) or not message.strip():
        send_reliable(
            session,
            MessageType.ERROR,
            req_id,
            {
                "code": 400,
                "message": "Broadcast message must be a non-empty string",
            },
        )
        return

    receiver_username = receiver_username.strip()

    sender_username = client_names.get(session_key(session))

    if sender_username is None:
        send_reliable(
            session,
            MessageType.ERROR,
            req_id,
            {
                "code": 401,
                "message": "Sender is not registered",
            },
        )
        return

    recipient_session = clients.get(receiver_username)

    if recipient_session is None:
        send_reliable(
            session,
            MessageType.ERROR,
            req_id,
            {
                "code": 404,
                "message": f"User '{receiver_username}' is not connected",
            },
        )
        return

    message = message.strip()
    delivery_payload = {
        "from": sender_username,
        "message": message,
    }

    send_reliable(
        recipient_session,
        MessageType.MSG_DELIVER,
        EVENT_REQ_ID,
        delivery_payload,
    )

    print(
        f"[DIRECT QUEUED] {sender_username} -> {receiver_username}"
    )

    send_reliable(
        session,
        MessageType.MSG_SENT,
        req_id,
        {
            "message": "Direct message accepted",
            "recipient": receiver_username,
        },
    )

def handle_auth(session: Session, datagram: Datagram) -> None:
    "Authenticate a provisional session and register it on success."
    req_id = datagram.app.req_id
    payload = datagram.payload

    # Authentication is allowed only once per session.
    if session.authenticated:
        send_reliable(
            session,
            MessageType.ERROR,
            req_id,
            {
                "code": 409,
                "message": "Session is already authenticated",
            },
        )
        return

    username = payload.get("username")
    password = payload.get("password")

    if (
        not isinstance(username, str)
        or not username.strip()
        or not isinstance(password, str)
        or not password
        or ACCOUNTS.get(username) != password
    ):
        print(f"[AUTH FAILED] Invalid credentials | {session.address}")

        send_reliable(
            session,
            MessageType.ERROR,
            req_id,
            {
                "code": 401,
                "message": "Invalid username or password",
            },
        )

        remove_session(session, "authentication failed")
        return

    username = username.strip()

    if username in clients:
        print(f"[AUTH FAILED] Account already connected: {username}")

        send_reliable(
            session,
            MessageType.ERROR,
            req_id,
            {
                "code": 409,
                "message": "Account already connected",
            },
        )

        remove_session(session, "account already connected")
        return

    # 3. Register the user only after credential validation.
    session.authenticated = True
    clients[username] = session
    client_names[session_key(session)] = username

    current_peers = [
        peer for peer in get_connected_users()
        if peer != username
    ]

    print(f"[AUTH OK] {username} | {session.address}")
    print(f"[ONLINE] {get_connected_users()}")

    # 4. Queue AUTH_OK first.
    send_reliable(
        session,
        MessageType.AUTH_OK,
        req_id,
        {
            "username": username,
            "message": "Authentication successful",
        },
    )

    send_reliable(
        session,
        MessageType.USER_LIST,
        EVENT_REQ_ID,
        {"users": current_peers},
    )

    notify_users(
        MessageType.USER_JOIN,
        {"username": username},
        exclude=session,
    )

def handle_file_offer(session: Session, datagram: Datagram) -> None:
    "Validate a client's file offer and accept it for transfer."
    req_id = datagram.app.req_id
    payload = datagram.payload

    transfer_id = payload.get("transfer_id")
    filename = payload.get("filename")
    file_size = payload.get("size")

    if (
        not isinstance(transfer_id, str)
        or len(transfer_id) != 32
        or any(c not in "0123456789abcdef" for c in transfer_id)
    ):
        send_reliable(
            session, MessageType.ERROR, req_id,
            {"code": 400, "message": "Invalid transfer ID"},
        )
        return

    if (
        not isinstance(filename, str)
        or not filename.strip()
        or filename in {".", ".."}
        or "/" in filename
        or "\\" in filename
    ):
        send_reliable(
            session,
            MessageType.ERROR,
            req_id,
            {
                "code": 400,
                "message": "Invalid filename",
            },
        )
        return

    if (
        not isinstance(file_size, int)
        or isinstance(file_size, bool)
        or file_size < 0
    ):
        send_reliable(
            session,
            MessageType.ERROR,
            req_id,
            {
                "code": 400,
                "message": "File size must be a non-negative integer",
            },
        )
        return

    sender_username = client_names.get(session_key(session))

    if sender_username is None:
        send_reliable(
            session,
            MessageType.ERROR,
            req_id,
            {
                "code": 401,
                "message": "Sender is not registered",
            },
        )
        return

    key = (session_key(session), transfer_id)

    if key in file_transfers:
        send_reliable(
            session, MessageType.ERROR, req_id,
            {"code": 409, "message": "Transfer ID already exists"},
        )
        return

    file_transfers[key] = FileTransfer(
        transfer_id=transfer_id,
        filename=filename,
        expected_size=file_size,
    )

    print(
        f"[FILE OFFER] sender={sender_username} "
        f"| transfer={transfer_id[:8]} "
        f"| filename={filename} | size={file_size} bytes"
    )
        
    send_reliable(
        session,
        MessageType.FILE_ACCEPT,
        req_id,
        {
            "filename": filename,
            "size": file_size,
            "message": "File offer accepted",
        },
    )

def handle_file_data(session: Session, datagram: Datagram) -> None:
    "Validate and buffer one chunk of an active file transfer."
    payload = datagram.payload

    transfer_id = payload.get("transfer_id")
    chunk_index = payload.get("chunk_index")
    encoded_data = payload.get("data")

    if not isinstance(transfer_id, str):
        print("[FILE DATA REJECTED] Missing transfer ID")
        return

    key = (session_key(session), transfer_id)
    transfer = file_transfers.get(key)

    if transfer is None:
        print(
            f"[FILE DATA REJECTED] Unknown transfer "
            f"{transfer_id[:8]}"
        )
        return

    if (
        not isinstance(chunk_index, int)
        or isinstance(chunk_index, bool)
        or chunk_index != transfer.next_chunk_index
    ):
        print(
            f"[FILE DATA REJECTED] Unexpected chunk index "
            f"| expected={transfer.next_chunk_index} "
            f"| received={chunk_index}"
        )
        file_transfers.pop(key, None)
        return

    if not isinstance(encoded_data, str):
        print("[FILE DATA REJECTED] Chunk data must be a string")
        file_transfers.pop(key, None)
        return

    # Decode the Base64 text back into the original file bytes.
    try: 
        chunk = base64.b64decode(encoded_data, validate=True)
    except (binascii.Error, ValueError):
        print("[FILE DATA REJECTED] Invalid Base64")
        file_transfers.pop(key, None)
        return

    # A chunk must not exceed the client's configured chunk size.
    if len(chunk) > FILE_CHUNK_SIZE:
        print("[FILE DATA REJECTED] Chunk exceeds maximum size")
        file_transfers.pop(key, None)
        return

    # Never accept more bytes than the original offer declared.
    if transfer.received_size + len(chunk) > transfer.expected_size:
        print("[FILE DATA REJECTED] Received size exceeds offer")
        file_transfers.pop(key, None)
        return

    transfer.chunks.append(chunk)
    transfer.received_size += len(chunk)
    transfer.next_chunk_index += 1

    print(
        f"[FILE CHUNK RECEIVED] {transfer.filename} "
        f"| chunk={chunk_index} "
        f"| received={transfer.received_size}/"
        f"{transfer.expected_size} bytes"
    )

def handle_file_end(session: Session, datagram: Datagram) -> None:
    req_id = datagram.app.req_id
    payload = datagram.payload

    transfer_id = payload.get("transfer_id")
    total_chunks = payload.get("total_chunks")
    declared_size = payload.get("size")

    if not isinstance(transfer_id, str):
        send_reliable(
            session, MessageType.ERROR, req_id,
            {"code": 400, "message": "Missing transfer ID"},
        )
        return

    key = (session_key(session), transfer_id)
    transfer = file_transfers.get(key)

    if transfer is None:
        send_reliable(
            session, MessageType.ERROR, req_id,
            {"code": 404, "message": "Transfer not found or invalid"},
        )
        return

    # Validate the final metadata.
    if (
        not isinstance(total_chunks, int)
        or isinstance(total_chunks, bool)
        or total_chunks < 0
        or not isinstance(declared_size, int)
        or isinstance(declared_size, bool)
        or declared_size < 0
    ):
        file_transfers.pop(key, None)
        send_reliable(
            session, MessageType.ERROR, req_id,
            {"code": 400, "message": "Invalid FILE_END metadata"},
        )
        return

    if (
        total_chunks != transfer.next_chunk_index
        or declared_size != transfer.expected_size
        or transfer.received_size != transfer.expected_size
    ):
        print(
            f"[FILE TRANSFER FAILED] Metadata mismatch "
            f"| filename={transfer.filename} "
            f"| chunks={transfer.next_chunk_index}/{total_chunks} "
            f"| bytes={transfer.received_size}/"
            f"{transfer.expected_size}"
        )

        file_transfers.pop(key, None)

        send_reliable(
            session, MessageType.ERROR, req_id,
            {"code": 400, "message": "Incomplete or inconsistent file"},
        )
        return

    file_bytes = b"".join(transfer.chunks)

    # Save only into the designated receiving directory.
    try:
        RECEIVED_DIR.mkdir(parents=True, exist_ok=True)
        destination = RECEIVED_DIR / transfer.filename

        # Avoid silently overwriting an existing file.
        with destination.open("xb") as output:
            output.write(file_bytes)

    except FileExistsError:
        file_transfers.pop(key, None)

        send_reliable(
            session, MessageType.ERROR, req_id,
            {
                "code": 409,
                "message": "A file with that name already exists",
            },
        )
        return

    except OSError as error:
        print(f"[FILE SAVE FAILED] {error}")
        file_transfers.pop(key, None)

        send_reliable(
            session, MessageType.FILE_RESULT, req_id,
            {
                "transfer_id": transfer_id,
                "success": False,
                "filename": transfer.filename,
                "message": "Server could not save the file",
            },
        )
        return

    file_transfers.pop(key, None)

    owner = client_names.get(session_key(session))

    stored_files[(owner, transfer.filename)] = destination

    print(
        f"[FILE SAVED] {destination} "
        f"| size={len(file_bytes)} bytes"
    )

    send_reliable(
        session,
        MessageType.FILE_RESULT,
        req_id,
        {
            "transfer_id": transfer_id,
            "success": True,
            "filename": transfer.filename,
            "size": len(file_bytes),
            "message": "File received and saved",
        },
    )

def handle_file_send(session: Session, datagram: Datagram) -> None:
    "Send a file already uploaded by this user to another online user."
    req_id = datagram.app.req_id
    payload = datagram.payload

    sender_username = client_names.get(session_key(session))
    recipient_username = payload.get("to")
    filename = payload.get("filename")

    if sender_username is None or not session.authenticated:
        send_reliable(
            session,
            MessageType.ERROR,
            req_id,
            {
                "code": 401,
                "message": "Authentication required",
            },
        )
        return

    if (
        not isinstance(recipient_username, str)
        or not recipient_username.strip()
        or not isinstance(filename, str)
        or not filename.strip()
        or filename in {".", ".."}
        or "/" in filename
        or "\\" in filename
    ):
        send_reliable(
            session,
            MessageType.ERROR,
            req_id,
            {
                "code": 400,
                "message": "Invalid receipient or filename",
            },
        )
        return

    recipient_username = recipient_username.strip()
    recipient_session = clients.get(recipient_username)

    if recipient_session is None or not recipient_session.authenticated:
        send_reliable(
            session,
            MessageType.ERROR,
            req_id,
            {
                "code": 404,
                "message": f"User '{recipient_username}' is not online",
            },
        )
        return

    file_path = stored_files.get((sender_username, filename))

    if file_path is None or not file_path.is_file():
        send_reliable(
            session,
            MessageType.ERROR,
            req_id,
            {
                "code": 404,
                "message": f"Uploaded file '{filename}' was not found",
            },
        )
        return

    transfer_id = uuid.uuid4().hex

    try:
        file_size = file_path.stat().st_size

        # Queue the unsolicited offer to B.
        send_reliable(
            recipient_session,
            MessageType.FILE_OFFER,
            EVENT_REQ_ID,
            {
                "transfer_id": transfer_id,
                "filename": filename,
                "size": file_size,
                "from": sender_username,
            },
        )

        # Queue the file chunks in order.
        total_chunks = 0

        with file_path.open("rb") as file:
            while True:
                chunk = file.read(FILE_CHUNK_SIZE)

                if not chunk:
                    break

                send_reliable(
                    recipient_session,
                    MessageType.FILE_DATA,
                    EVENT_REQ_ID,
                    {
                        "transfer_id": transfer_id,
                        "chunk_index": total_chunks,
                        "data": base64.b64encode(chunk).decode("ascii"),
                    },
                )

                total_chunks += 1

        # Queue the end marker after all chunks.
        send_reliable(
            recipient_session,
            MessageType.FILE_END,
            EVENT_REQ_ID,
            {
                "transfer_id": transfer_id,
                "total_chunks": total_chunks,
                "size": file_size,
            },
        )

    except OSError as error:
        print(f"[FILE SEND ERROR] {filename}: {error}")

        send_reliable(
            session,
            MessageType.ERROR,
            req_id,
            {
                "code": 500,
                "message": "Could not read the stored file",
            },
        )
        return

    # Tell sender that the server accepted the delivery request.
    send_reliable(
        session,
        MessageType.FILE_SENT,
        req_id,
        {
            "message": "File delivery queued",
            "recipient": recipient_username,
            "filename": filename,
            "transfer_id": transfer_id,
            "size": file_size,
        },
    )

    print(
        f"[FILE SEND QUEUED] {sender_username} -> {recipient_username} "
        f"| file={filename} | size={file_size} "
        f"| chunks={total_chunks} | transfer={transfer_id[:8]}"
    )

# Add new message types here (broadcast, direct, file transfer, ...).
MESSAGE_HANDLERS = {
    MessageType.AUTH: handle_auth,
    MessageType.PING: handle_ping,
    MessageType.MSG_BROADCAST: handle_broadcast,
    MessageType.MSG_DIRECT: handle_direct,
    MessageType.FILE_OFFER: handle_file_offer,
    MessageType.FILE_DATA: handle_file_data,
    MessageType.FILE_END: handle_file_end,
    MessageType.FILE_SEND: handle_file_send,
}

def dispatch_message(session: Session, datagram: Datagram) -> None:
    "Route application messages, enforcing authentication first."
    if datagram.app is None or datagram.payload is None:
        print(f"[REJECTED DATA] Missing application data from {session.address}")
        return

    msg_type = datagram.app.type

    # Provisional sessions may submit AUTH only.
    if not session.authenticated and msg_type != MessageType.AUTH:
        print(
            f"[REJECTED UNAUTHENTICATED] {session.address} "
            f"| type={msg_type}"
        )

        send_reliable(
            session,
            MessageType.ERROR,
            datagram.app.req_id,
            {
                "code": 401,
                "message": "Authentication required",
            },
        )

        remove_session(session, "application request before authentication")
        return

    handler = MESSAGE_HANDLERS.get(msg_type)

    if handler is None:
        print(
            f"[UNSUPPORTED APP TYPE] {msg_type} "
            f"from {session.address}"
        )
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

    receiver_thread = threading.Thread(
        target=receiver_loop,
        name="udp-receiver",
        daemon=True,
    )
    handler_thread = threading.Thread(
        target=handler_loop,
        name="data-handler",
        daemon=True,
    )

    receiver_thread.start()
    handler_thread.start()

    try:
        receiver_thread.join()
    except KeyboardInterrupt:
        print("\nServer shutting down.")

    finally:
        running.clear()
        receiver_thread.join(timeout=1.0)
        handler_thread.join() # Let the handler drain already-scheduled DATA before exiting.
        sock.close()

if __name__ == "__main__":
    run_server()