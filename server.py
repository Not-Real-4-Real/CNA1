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
HEARTBEAT_TIMEOUT = 30.0        # seconds without valid client activity

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

    peer_rwnd: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

@dataclass
class ReceiverState:
    "Layer 2: the client's incoming DATA toward us."
    expected_seq: int = 0

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
    "Send a header-only RDT packet (OPEN_ACK / CLOSE_ACK)."
    rwnd = receive_window(session)

    transport_send(
        session.address,
        encode_datagram(
            RDTHeader(
                session_id=session.session_id,
                rwnd=rwnd,
                type=packet_type,
            )
        ),
    )

    print(
        f"[SENT CONTROL] {session.address} "
        f"| type={packet_type} | rwnd={rwnd}"
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

def send_reliable(session: Session, msg_type: int, req_id: int, payload: dict) -> None:
    """Queue a message for reliable delivery. It is sent immediately if nothing is
    in flight, otherwise as soon as the previous DATA has been ACKed. Messages
    are never dropped just because an earlier one is still unACKed."""

    sender = session.sender

    with sender.lock:
        sender.outbox.append((msg_type, req_id, payload))
        pump_outbox(session)

        print(
            f"[OUTBOX QUEUE] recipient={session.address} "
            f"| type={msg_type} | req_id={req_id} "
            f"| in_flight={sender.in_flight_seq} "
            f"| queued={len(sender.outbox)}"
        )

def pump_outbox(session: Session) -> None:
    "If the channel is idle, transmit the next queued message."
    sender = session.sender

    if sender.in_flight is not None:
        return

    if not sender.outbox:
        return

    if sender.peer_rwnd <= 0:
        return

    msg_type, req_id, payload = sender.outbox.popleft()

    packet = encode_datagram(
        rdt=RDTHeader(
            session_id=session.session_id,
            seq_num=sender.seq,
            rwnd=receive_window(session=session),
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
    with sender.lock:
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

        update_peer_window(session, datagram.rdt.rwnd)

        sender.in_flight = None
        sender.in_flight_seq = None
        sender.sent_at = None
        sender.retransmissions = 0
        sender.seq ^= 1          # alternate 0 -> 1 -> 0 -> ...

        pump_outbox(session)     # send the next queued message, if any

def handle_data(session: Session, datagram: Datagram) -> None:
    "Process a DATA datagram that has already been accepted and ACKed."
    if datagram.app is None:
        print(
            f"[REJECTED DATA] Missing application header "
            f"from {session.address}"
        )
        return

    print(
        f"[DATA ACCEPTED] {session.address} "
        f"| seq={datagram.rdt.seq_num} "
        f"| req_id={datagram.app.req_id}"
    )

    if message_handler is not None:
        message_handler(session, datagram)
    else:
        print("[ERROR] message_handler is None")

def check_retransmission(session: Session) -> None:
    "Retransmit this session's in-flight DATA if its timer expired."
    sender = session.sender
    failed = False

    with sender.lock:
        if sender.in_flight is None or sender.sent_at is None:
            return

        now = time.monotonic()

        if now - sender.sent_at < RETRANSMISSION_TIMEOUT:
            return

        if sender.retransmissions >= MAX_RETRANSMISSIONS:
            print(
                f"[TRANSMISSION FAILED] {session.address} "
                f"| seq={sender.in_flight_seq} "
                f"| retries={sender.retransmissions}"
            )
            failed = True
        else:
            transport_send(session.address, sender.in_flight)
            sender.retransmissions += 1
            sender.sent_at = now

            print(
                f"[RETRANSMIT] {session.address} "
                f"| seq={sender.in_flight_seq} "
                f"| retry={sender.retransmissions}/{MAX_RETRANSMISSIONS}"
            )

    if failed and failure_handler is not None:
        failure_handler(session)

def receive_window(session: Session) -> int:
    """Return the currently available DATA buffer slots."""
    return max(0, DATA_BUFFER_SIZE - session.incoming_data.qsize())

def receive_data(session: Session, datagram: Datagram) -> None:
    "Admit in-order DATA to this session's buffer before acknowledging it."
    receiver = session.receiver
    seq = datagram.rdt.seq_num

    if seq not in (0, 1):
        print(f"[REJECTED DATA] Invalid seq={seq} from {session.address}")
        return

    # Always ACK, even duplicates, in case previous ACK was lost.
    if seq != receiver.expected_seq:
        print(f"[DUPLICATE DATA] {session.address} | seq={seq} | ACK resent")
        send_ack(session, seq)
        return

    # Queue admission and scheduling must be coordinated with the handler.
    with session.queue_lock:
        try:
            session.incoming_data.put_nowait(datagram)
        except queue.Full:
            print(
                f"[RECEIVE BUFFER FULL] {session.address} "
                f"| seq={seq} "
                f"| capacity={DATA_BUFFER_SIZE}"
            )
            return

        receiver.expected_seq ^= 1

        # Schedule this session only once until its queue is drained.
        if not session.handler_scheduled:
            session.handler_scheduled = True
            ready_sessions.put_nowait(session)

    sender = session.sender
    with sender.lock:
        update_window_and_pump(session, datagram.rdt.rwnd)

    # Acknowledge only after the DATA has been retained.
    send_ack(session, seq)

    print(
        f"[DATA BUFFERED] {session.address} "
        f"| seq={seq} "
        f"| queued={session.incoming_data.qsize()}/{DATA_BUFFER_SIZE}"
    )

def update_peer_window(session, rwnd):
    session.sender.peer_rwnd = rwnd

def update_window_and_pump(session: Session, rwnd: int) -> None:
    update_peer_window(session, rwnd)
    pump_outbox(session)

def dispatch_datagram(address: tuple, datagram: Datagram) -> None:
    "Route a decoded datagram to the right session / reliable handler."
    rdt = datagram.rdt

    if rdt.type == RDTType.OPEN:
        handle_open(address, datagram)
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
        receive_data(session, datagram)
    elif rdt.type == RDTType.ACK:
        handle_ack(session, datagram)
    # RDTType.HEARTBEAT: refreshing last_seen above is all it needs.

def handler_loop() -> None:
    "Process accepted DATA sequentially across all sessions."
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
                    # Coordinate with receive_data() before marking this session unscheduled.
                    with session.queue_lock:
                        if session.incoming_data.empty():
                            session.handler_scheduled = False
                            break

                    continue

                try:
                    handle_data(session, datagram)
                except Exception as error:
                    print(
                        f"[HANDLER ERROR] {session.address} "
                        f"| {error}"
                    )
                finally:
                    session.incoming_data.task_done()

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
        with session.sender.lock:
            update_window_and_pump(session, datagram.rdt.rwnd)

    else:
        session.last_seen = time.monotonic()
        print(f"[OPEN REPEATED] {address} | session={session_id.hex()}")
        send_control(session, RDTType.OPEN_ACK)

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