import queue
import secrets
import socket
import threading
import base64
import binascii
import uuid
import time
from pathlib import Path
from dataclasses import dataclass, field

from udp_protocol import *

SERVER_HOST = "127.0.0.1"
SERVER_PORT = 5000
SERVER_ADDRESS = (SERVER_HOST, SERVER_PORT)
 
BUFFER_SIZE = 1201
RETRANSMISSION_TIMEOUT = 1.0
MAX_RETRANSMISSIONS = 5

RESPONSE_TIMEOUT = RETRANSMISSION_TIMEOUT * (MAX_RETRANSMISSIONS + 3)

HEARTBEAT_INTERVAL = 10.0
PERSIST_INTERVAL = 1.0

FILE_CHUNK_SIZE = 600 

session_id = secrets.token_bytes(16)
 
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.settimeout(0.1)

running = threading.Event()
running.set()

# Buffer configuration
DATA_BUFFER_SIZE = 32             # Maximum queued incoming DATA datagrams

# Layer 2 (reliable) state: Selective Repeat sliding windows.
# send_base is the oldest sequence number not cumulatively passed by the window;
# send_next_seq is the next sequence number to allocate.
send_state_lock = threading.RLock()
send_condition = threading.Condition(send_state_lock)
send_base = 0
send_next_seq = 0
outstanding_packets = {}       # seq_num -> OutstandingPacket
acked_sequences = set()       # ACKed out of order, waiting for send_base to advance
peer_rwnd = 0                 # most recently advertised receiver window
last_window_probe = 0.0

# Incoming DATA buffer: application handler consumes this queue in order.
incoming_data = queue.Queue(maxsize=DATA_BUFFER_SIZE)
receive_state_lock = threading.Lock()
receive_base = 0
receive_buffer = {}            # seq_num -> Datagram (out-of-order packets)

# Layer 2 -> Layer 4 hook: called with every new (non-duplicate) DATA datagram.
message_handler = None
 
# Layer 3 (session) state
established = False
open_ack_event = threading.Event()
close_ack_event = threading.Event()
heartbeat_stop = threading.Event()   # set to stop the heartbeat thread
heartbeat_thread = None
 
# Layer 4 (request) state
pending_requests = {}             # req_id -> Queue for that response
pending_lock = threading.Lock()
next_req_id = 1
req_id_lock = threading.Lock()
EVENT_REQ_ID = 0                  # one-way events carry req_id 0
peers = set()                     # usernames of other connected users
peers_lock = threading.Lock()

# File receiving
RECEIVED_DIR = Path("client_received_files")

@dataclass
class FileTransfer:
    transfer_id: str
    filename: str
    expected_size: int
    chunks: list[bytes] = field(default_factory=list)
    next_chunk_index: int = 0
    received_size: int = 0

@dataclass
class OutstandingPacket:
    packet: bytes
    sent_at: float
    retransmissions: int = 0
    completed: threading.Event = field(default_factory=threading.Event)
    success: bool = False
    error: str | None = None

incoming_transfers: dict[str, FileTransfer] = {}
incoming_transfers_lock = threading.Lock()

# Printing
print_lock = threading.Lock()

def log(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)

# Layer 1: Transport
def transport_send(packet: bytes) -> None:
    sock.sendto(packet, SERVER_ADDRESS)

def transport_receive():
    data, address = sock.recvfrom(BUFFER_SIZE)
 
    if address != SERVER_ADDRESS:
        log(f"[IGNORED] Unexpected sender: {address}")
        return None
 
    try:
        datagram = decode_datagram(data)
    except ValueError as error:
        log(f"[INVALID DATAGRAM] {error}")
        return None
 
    if datagram.rdt.session_id != session_id:
        log("[IGNORED] Session ID mismatch")
        return None
 
    return datagram

# Layer 2: Reliable delivery (Selective Repeat sliding window)
def send_control(packet_type: int, quiet: bool = False) -> None:
    "Send a header-only RDT packet (OPEN / CLOSE / HEARTBEAT / window control)."
    transport_send(
        encode_datagram(
            RDTHeader(
                session_id=session_id,
                rwnd=receive_window(),
                type=packet_type,
            )
        )
    )
    if not quiet:
        log(f"[SENT CONTROL] type={packet_type} | session={session_id.hex()[:8]}")


def send_ack(ack_num: int) -> None:
    rwnd = receive_window()
    transport_send(
        encode_datagram(
            RDTHeader(
                session_id=session_id,
                ack_num=ack_num,
                rwnd=rwnd,
                type=RDTType.ACK,
            )
        )
    )
    log(f"[ACK SENT] ack_num={ack_num} | rwnd={rwnd}")


def send_window_update() -> None:
    """Advertise newly freed receive-buffer capacity to the server."""
    send_control(RDTType.WINDOW_UPDATE, quiet=True)


def queue_reliable(msg_type: int, req_id: int, payload: dict):
    """Allocate a sequence number and enqueue one DATA packet for transmission.

    Returns (seq_num, OutstandingPacket), or None if the sender cannot obtain
    window capacity before RESPONSE_TIMEOUT expires.
    """
    global send_next_seq, last_window_probe

    deadline = time.monotonic() + RESPONSE_TIMEOUT
    with send_condition:
        while running.is_set():
            window_span = seq_distance(send_next_seq, send_base)
            has_sequence_space = window_span < SR_WINDOW_SIZE
            has_receiver_capacity = len(outstanding_packets) < peer_rwnd

            if has_sequence_space and has_receiver_capacity:
                seq = send_next_seq
                try:
                    packet = encode_datagram(
                        RDTHeader(
                            session_id=session_id,
                            seq_num=seq,
                            rwnd=receive_window(),
                            type=RDTType.DATA,
                        ),
                        AppHeader(length=0, type=msg_type, req_id=req_id),
                        payload=payload,
                    )
                except ValueError as error:
                    # Nothing has been allocated yet, so the window is unchanged.
                    log(f"[SEND REJECTED] Cannot encode message: {error}")
                    return None
                pending = OutstandingPacket(
                    packet=packet,
                    sent_at=time.monotonic(),
                )
                outstanding_packets[seq] = pending
                send_next_seq = seq_add(send_next_seq)
                transport_send(packet)
                log(
                    f"[DATA SENT] seq={seq} | req_id={req_id} "
                    f"| in_flight={len(outstanding_packets)}/{SR_WINDOW_SIZE}"
                )
                return seq, pending

            now = time.monotonic()
            if peer_rwnd <= 0 and now - last_window_probe >= PERSIST_INTERVAL:
                last_window_probe = now
                try:
                    send_control(RDTType.WINDOW_PROBE, quiet=True)
                    log("[WINDOW PROBE] receiver advertised rwnd=0")
                except OSError as error:
                    log(f"[WINDOW PROBE ERROR] {error}")

            remaining = deadline - now
            if remaining <= 0:
                log("[SEND BLOCKED] No sender-window or receiver-window capacity")
                return None
            send_condition.wait(timeout=min(remaining, 0.2))

    return None


def wait_reliable(seq: int, pending: OutstandingPacket) -> bool:
    """Wait for the ACK (or terminal failure) of one queued DATA packet."""
    wait_time = RETRANSMISSION_TIMEOUT * (MAX_RETRANSMISSIONS + 2) + 0.5
    if not pending.completed.wait(wait_time):
        log(f"[ACK TIMEOUT] seq={seq}")
        return False
    if not pending.success:
        log(f"[TRANSMISSION FAILED] seq={seq} | reason={pending.error}")
        return False
    return True


def send_reliable(msg_type: int, req_id: int, payload: dict) -> bool:
    """Send one application message reliably and wait for its packet ACK."""
    queued = queue_reliable(msg_type, req_id, payload)
    if queued is None:
        return False
    seq, pending = queued
    return wait_reliable(seq, pending)


def send_reliable_batch(msg_type: int, req_id: int, payloads) -> bool:
    """Send payloads through the Selective Repeat window, keeping it full.

    queue_reliable() already blocks while the SR window (or the peer's rwnd)
    is full, and unblocks as individual ACKs slide the window. So there is no
    need to wait for a whole batch to be ACKed before sending more: a single
    lost packet only holds back the window, not every packet behind it.
    """
    in_flight = []
    for payload in payloads:
        queued = queue_reliable(msg_type, req_id, payload)
        if queued is None:
            return False
        in_flight.append(queued)

        # Fail fast if the channel already gave up on an earlier packet.
        if any(p.completed.is_set() and not p.success for _, p in in_flight):
            return False

        # Forget packets that are already ACKed; only unresolved ones matter.
        in_flight = [
            (seq, p) for seq, p in in_flight
            if not (p.completed.is_set() and p.success)
        ]

    results = [wait_reliable(seq, pending) for seq, pending in in_flight]
    return all(results)


def receive_window() -> int:
    with receive_state_lock:
        occupied = incoming_data.qsize() + len(receive_buffer)
        return max(0, DATA_BUFFER_SIZE - occupied)


def handle_ack(datagram) -> None:
    """Accept a packet-level ACK and slide the sender window."""
    global send_base, peer_rwnd
    ack_num = datagram.rdt.ack_num

    with send_condition:
        # The advertised window is useful even when this ACK is a duplicate.
        peer_rwnd = datagram.rdt.rwnd
        pending = outstanding_packets.pop(ack_num, None)

        if pending is None:
            log(f"[IGNORED ACK] ack_num={ack_num} | no matching outstanding packet")
        else:
            pending.success = True
            pending.completed.set()
            acked_sequences.add(ack_num)
            log(
                f"[ACK ACCEPTED] ack_num={ack_num} "
                f"| retries={pending.retransmissions} | rwnd={peer_rwnd}"
            )

            while send_base in acked_sequences:
                acked_sequences.remove(send_base)
                send_base = seq_add(send_base)

        send_condition.notify_all()


def check_retransmissions() -> None:
    """Retransmit each expired unACKed DATA packet independently."""
    failed = []
    now = time.monotonic()

    with send_condition:
        for seq, pending in list(outstanding_packets.items()):
            if now - pending.sent_at < RETRANSMISSION_TIMEOUT:
                continue

            if pending.retransmissions >= MAX_RETRANSMISSIONS:
                pending.success = False
                pending.error = "maximum retransmissions exceeded"
                pending.completed.set()
                outstanding_packets.pop(seq, None)
                failed.append(seq)
                continue

            try:
                transport_send(pending.packet)
            except OSError as error:
                pending.success = False
                pending.error = str(error)
                pending.completed.set()
                outstanding_packets.pop(seq, None)
                failed.append(seq)
                continue

            pending.retransmissions += 1
            pending.sent_at = now
            log(
                f"[RETRANSMIT] seq={seq} "
                f"| retry={pending.retransmissions}/{MAX_RETRANSMISSIONS}"
            )

        if failed:
            log(f"[RELIABLE CHANNEL FAILED] sequence(s)={failed}; session must reconnect")
            send_condition.notify_all()

    if failed:
        # Continuing after a missing sequence would leave the peer's receive
        # window blocked, so fail the session rather than silently skipping it.
        global established
        established = False
        running.clear()


def _drain_receive_buffer_locked() -> None:
    """Move contiguous buffered packets to the application queue in order.

    Caller must hold receive_state_lock.
    """
    global receive_base
    while receive_base in receive_buffer:
        try:
            incoming_data.put_nowait(receive_buffer[receive_base])
        except queue.Full:
            break
        del receive_buffer[receive_base]
        receive_base = seq_add(receive_base)


def handle_data(datagram) -> None:
    "Process an already-accepted DATA datagram at the application layer."
    if datagram.app is None:
        log("[REJECTED DATA] Missing application header")
        return

    if message_handler is not None:
        message_handler(datagram)
    else:
        log("[ERROR] message_handler is None")


def handle_datagram(datagram) -> None:
    packet_type = datagram.rdt.type

    if packet_type == RDTType.OPEN_ACK:
        handle_open_ack(datagram)
    elif packet_type == RDTType.CLOSE_ACK:
        close_ack_event.set()
    elif packet_type == RDTType.ACK:
        handle_ack(datagram)
    elif packet_type == RDTType.DATA:
        receive_data(datagram)
    elif packet_type == RDTType.WINDOW_UPDATE:
        update_peer_window(datagram.rdt.rwnd)
    elif packet_type == RDTType.WINDOW_PROBE:
        send_window_update()
    else:
        log(f"[IGNORED] Unexpected RDT type={packet_type}")


def receive_data(datagram) -> None:
    """Selective Repeat receiver: buffer in-window packets and ACK each one."""
    seq = datagram.rdt.seq_num
    accepted = False
    duplicate = False

    with receive_state_lock:
        if seq in receive_buffer:
            duplicate = True
        elif seq_in_window(seq, receive_base, SR_WINDOW_SIZE):
            occupied = incoming_data.qsize() + len(receive_buffer)
            if occupied >= DATA_BUFFER_SIZE:
                log(
                    f"[RECEIVE BUFFER FULL] seq={seq} "
                    f"| capacity={DATA_BUFFER_SIZE}"
                )
                return
            receive_buffer[seq] = datagram
            accepted = True
            _drain_receive_buffer_locked()
        elif seq_is_recent_duplicate(seq, receive_base, SR_WINDOW_SIZE):
            duplicate = True
        else:
            log(
                f"[REJECTED DATA] seq={seq} outside receive window "
                f"(base={receive_base}, size={SR_WINDOW_SIZE})"
            )
            return

    # A duplicate may be a retransmission whose header still carries the
    # rwnd from its first transmission, so only fresh packets refresh the peer
    # window. The ACK sent below carries a current rwnd anyway.
    if not duplicate:
        update_peer_window(datagram.rdt.rwnd)
    # ACK duplicates too: the original ACK may have been lost.
    send_ack(seq)

    if duplicate:
        log(f"[DUPLICATE DATA] seq={seq} | ACK resent")
    elif accepted:
        log(
            f"[DATA BUFFERED] seq={seq} | "
            f"queued={incoming_data.qsize()}/{DATA_BUFFER_SIZE} "
            f"| recv_base={receive_base}"
        )


def update_peer_window(rwnd: int) -> None:
    global peer_rwnd
    with send_condition:
        peer_rwnd = max(0, rwnd)
        send_condition.notify_all()
    log(f"[PEER WINDOW] rwnd={rwnd}")


def receiver_loop() -> None:
    while running.is_set():
        try:
            datagram = transport_receive()
        except socket.timeout:
            check_retransmissions()
            continue
        except OSError as error:
            if running.is_set():
                log(f"\n[RECEIVER ERROR] {error}")
                running.clear()
            break

        if datagram is not None:
            handle_datagram(datagram)
        check_retransmissions()


def handler_loop() -> None:
    while running.is_set() or not incoming_data.empty():
        try:
            datagram = incoming_data.get(timeout=0.1)
        except queue.Empty:
            continue

        try:
            handle_data(datagram)
        except Exception as error:
            log(f"[HANDLER ERROR] {error}")
        finally:
            incoming_data.task_done()
            with receive_state_lock:
                _drain_receive_buffer_locked()
            try:
                send_window_update()
            except OSError as error:
                log(f"[WINDOW UPDATE ERROR] {error}")


receiver_thread = threading.Thread(target=receiver_loop, name="udp-receiver", daemon=True)
handler_thread = threading.Thread(target=handler_loop, name="udp-handler", daemon=True)

def control_handshake(packet_type: int, ack_event_: threading.Event, label: str) -> bool:
    "Send a control packet, retransmitting until its ACK event is set."
    ack_event_.clear()
 
    for attempt in range(MAX_RETRANSMISSIONS + 1):
        send_control(packet_type)

        if ack_event_.wait(RETRANSMISSION_TIMEOUT):
            return True
        
        log(f"[TIMEOUT] No {label}_ACK ({attempt + 1}/{MAX_RETRANSMISSIONS + 1})")
 
    return False

def heartbeat_loop() -> None:
    """
    Tell the server we are still alive. One-way and unacknowledged: a lost
    heartbeat is covered by the next one, so nothing is retransmitted.
    """
    # wait() returns True once heartbeat_stop is set, which ends the loop.
    while not heartbeat_stop.wait(HEARTBEAT_INTERVAL):
        try:
            send_control(RDTType.HEARTBEAT, quiet=True)
        except OSError:
            break

def start_heartbeat() -> None:
    global heartbeat_thread

    heartbeat_stop.clear()
    heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
    heartbeat_thread.start()

def stop_heartbeat() -> None:
    heartbeat_stop.set()

    if heartbeat_thread is not None:
        heartbeat_thread.join(timeout=1.0)

def open_session() -> bool:
    global established
 
    log(f"[OPENING SESSION] session={session_id.hex()}")
    if control_handshake(RDTType.OPEN, open_ack_event, "OPEN"):
        established = True
        start_heartbeat()
        log("[SESSION ESTABLISHED]")
        return True
 
    log("[SESSION FAILED] Server did not answer OPEN")
    return False

def handle_open_ack(datagram) -> None:
    if established:
        log("[IGNORED] Unexpected OPEN_ACK")
        return

    update_peer_window(datagram.rdt.rwnd)
    open_ack_event.set()

def close_session() -> bool:
    global established
 
    if not established:
        return False
 
    log("[CLOSING SESSION]")
    stop_heartbeat()   # no heartbeat should trail after the CLOSE
    ok = control_handshake(RDTType.CLOSE, close_ack_event, "CLOSE")
    established = False
    log("[SESSION CLOSED]" if ok else "[SESSION CLOSE UNCONFIRMED]")
    return ok

# Layer 4: Requests (req_id <-> response matching)
def show_peer_list() -> None:
    with peers_lock:
        peer_snapshot = sorted(peers)

    log("\n========== ONLINE PEERS ==========")

    if not peer_snapshot:
        log("No other peers are currently online.")
    else:
        for index, username in enumerate(peer_snapshot, start=1):
            log(f"{index}. {username}")

    log("==================================")

def handle_server_event(datagram) -> None:
    "Handle one-way server events (req_id 0)."    
    msg_type = datagram.app.type
    payload = datagram.payload

    if msg_type == MessageType.USER_LIST:
        users = payload.get("users", [])

        if not isinstance(users, list) or not all(
            isinstance(username, str) for username in users
        ):
            log("[INVALID USER_LIST] Malformed peer list")
            return

        with peers_lock:
            peers.clear()
            peers.update(users)

        log(f"\n[PEER LIST UPDATED] {len(users)} peer(s)")

    elif msg_type == MessageType.USER_JOIN:
        username = payload["username"]
        with peers_lock:
            peers.add(username)
        log(f"\n[USER JOINED] {username}")

    elif msg_type == MessageType.USER_LEAVE:
        username = payload["username"]
        with peers_lock:
            peers.discard(username)
        log(f"\n[USER LEFT] {username}")

    elif msg_type == MessageType.MSG_DELIVER:
        sender = payload.get("from", "Unknown")
        message = payload.get("message", "")
        log(f"\n[DELIVER] {sender}: {message}")

    elif msg_type == MessageType.FILE_OFFER:
        handle_incoming_file_offer(payload)

    elif msg_type == MessageType.FILE_DATA:
        handle_incoming_file_data(payload)

    elif msg_type == MessageType.FILE_END:
        handle_incoming_file_end(payload)

    else:
        log(
            f"\n[SERVER EVENT] type={msg_type} "
            f"| payload={payload}"
        )

def handle_incoming_file_offer(payload: dict) -> None:
    transfer_id = payload.get("transfer_id")
    filename = payload.get("filename")
    size = payload.get("size")
    sender = payload.get("from", "Unknown")

    with incoming_transfers_lock:
        if transfer_id in incoming_transfers:
            log(f"[FILE OFFER IGNORED] Duplicate transfer={transfer_id[:8]}")
            return

        incoming_transfers[transfer_id] = FileTransfer(
            transfer_id=transfer_id,
            filename=filename,
            expected_size=size,
        )

    log(
        f"\n[INCOMING FILE] From={sender} | filename={filename} "
        f"| size={size} bytes | transfer={transfer_id[:8]}"
    )

def handle_incoming_file_data(payload: dict) -> None:
    transfer_id = payload.get("transfer_id")
    chunk_index = payload.get("chunk_index")
    encoded_data = payload.get("data")

    if (
        not isinstance(transfer_id, str)
        or not isinstance(chunk_index, int)
        or isinstance(chunk_index, bool)
        or not isinstance(encoded_data, str)
    ):
        log("[FILE DATA REJECTED] Invalid payload")
        return

    try:
        chunk = base64.b64decode(encoded_data, validate=True)
    except (binascii.Error, ValueError):
        log("[FILE DATA REJECTED] Invalid Base64 data")
        return

    if len(chunk) > FILE_CHUNK_SIZE:
        log("[FILE DATA REJECTED] Chunk exceeds maximum size")
        return

    with incoming_transfers_lock:
        transfer = incoming_transfers.get(transfer_id)

        if transfer is None:
            log(f"[FILE DATA REJECTED] Unknown transfer={transfer_id[:8]}")
            return

        if chunk_index != transfer.next_chunk_index:
            log(
                f"[FILE DATA REJECTED] Expected chunk "
                f"{transfer.next_chunk_index}, got {chunk_index}"
            )
            return

        if transfer.received_size + len(chunk) > transfer.expected_size:
            log("[FILE DATA REJECTED] Received size exceeds expected size")
            return

        transfer.chunks.append(chunk)
        transfer.received_size += len(chunk)
        transfer.next_chunk_index += 1

        received = transfer.received_size
        expected = transfer.expected_size

    log(
        f"[FILE CHUNK RECEIVED] {transfer.filename} "
        f"| chunk={chunk_index} | bytes={len(chunk)} "
        f"| progress={received}/{expected}"
    )

def handle_incoming_file_end(payload: dict) -> None:
    transfer_id = payload.get("transfer_id")
    total_chunks = payload.get("total_chunks")
    total_size = payload.get("size")

    if (
        not isinstance(transfer_id, str)
        or not isinstance(total_chunks, int)
        or isinstance(total_chunks, bool)
        or not isinstance(total_size, int)
        or isinstance(total_size, bool)
    ):
        log("[FILE END REJECTED] Invalid payload")
        return

    with incoming_transfers_lock:
        transfer = incoming_transfers.get(transfer_id)

        if transfer is None:    
            log(f"[FILE END REJECTED] Unknown transfer={transfer_id[:8]}")
            return

        if (
            total_chunks != transfer.next_chunk_index
            or total_size != transfer.expected_size
            or transfer.received_size != transfer.expected_size
        ):
            incoming_transfers.pop(transfer_id, None)
            success = False
            filename = transfer.filename
            received_size = transfer.received_size
            error_message = "Incomplete or inconsistent file transfer"
            file_bytes = None
        else:
            filename = transfer.filename
            received_size = transfer.received_size
            file_bytes = b"".join(transfer.chunks)
            incoming_transfers.pop(transfer_id, None)
            success = True
            error_message = ""

    if success:
        try:
            RECEIVED_DIR.mkdir(parents=True, exist_ok=True)
            destination = RECEIVED_DIR / filename

            # Do not silently overwrite an existing file.
            with destination.open("xb") as file:
                file.write(file_bytes)

        except OSError as error:
            success = False
            error_message = str(error)

    result = {
        "transfer_id": transfer_id,
        "filename": filename,
        "success": success,
        "size": received_size,
        "message": (
            "File received successfully"
            if success
            else error_message
        ),
    }

    if success:
        log(
            f"[FILE RECEIVED] {filename} | {received_size} bytes "
            f"| saved to {RECEIVED_DIR / filename}"
        )
    else:
        log(f"[FILE RECEIVE FAILED] {filename} | {error_message}")

def deliver_response(datagram) -> None:
    "Called by layer 2 for each new DATA message; routes it by req_id."
    req_id = datagram.app.req_id

    # req_id 0 is a server event, not a response to any request of ours.
    if req_id == EVENT_REQ_ID:
        handle_server_event(datagram)
        return
 
    with pending_lock:
        response_queue = pending_requests.get(req_id)
 
    if response_queue is None:
        log(f"[UNMATCHED RESPONSE] type={datagram.app.type} | req_id={req_id}")
        return
 
    try:
        response_queue.put_nowait(datagram)
    except queue.Full:
        pass  # a response for this request is already queued
 
message_handler = deliver_response
 
def send_request(msg_type: int, payload: dict, timeout: float = RESPONSE_TIMEOUT):
    "Send a request reliably and wait for the response with the same req_id."

    global next_req_id
 
    if not established:
        log("[ERROR] No established session")
        return None
 
    with req_id_lock:
        req_id = next_req_id
        next_req_id += 1
 
    # Register before sending so a fast response can't be missed.
    response_queue = queue.Queue(maxsize=1)
    with pending_lock:
        pending_requests[req_id] = response_queue
 
    try:
        if not send_reliable(msg_type, req_id, payload):
            return None
 
        try:
            return response_queue.get(timeout=timeout)
        except queue.Empty:
            log(f"[TIMEOUT] No response for req_id={req_id} within {timeout}s")
            return None
    finally:
        with pending_lock:
            pending_requests.pop(req_id, None)

def ping(message: str) -> bool:
    response = send_request(MessageType.PING, {"message": message})
 
    if response is None:
        return False
 
    if response.app.type != MessageType.PONG:
        log(f"[UNSUPPORTED APP TYPE] {response.app.type}")
        return False
 
    log(
        f"[PONG RECEIVED] req_id={response.app.req_id} "
        f"| payload={response.payload}"
    )
    return True

def broadcast(message: str) -> bool:
    message = message.strip()

    if not message:
        log("[ERROR] Broadcast message cannot be empty")
        return False

    response = send_request(
        MessageType.MSG_BROADCAST,
        {"message": message},
    )

    if response is None:
        log("[BROADCAST FAILED] No application response")
        return False

    if response.app.type == MessageType.ERROR:
        log(f"[BROADCAST ERROR] {response.payload}")
        return False

    if response.app.type != MessageType.MSG_SENT:
        log(
            f"[UNEXPECTED RESPONSE] "
            f"type={response.app.type} "
            f"| payload={response.payload}"
        )
        return False

    log(f"[BROADCAST ACCEPTED] {response.payload}")
    return True

def direct(recipient: str, message: str) -> bool:
    recipient = recipient.strip()

    if not recipient:
        log("[ERROR] Direct need a to")
        return False
    
    message = message.strip()

    if not message:
        log("[ERROR] Direct message cannot be empty")
        return False

    response = send_request(
        MessageType.MSG_DIRECT,
        {
            "to": recipient,
            "message": message,
        },
    )

    if response is None:
        log("[DIRECT FAILED] No application response")
        return False

    if response.app.type == MessageType.ERROR:
        log(f"[DIRECT ERROR] {response.payload}")
        return False

    if response.app.type != MessageType.MSG_SENT:
        log(
            f"[UNEXPECTED RESPONSE] "
            f"type={response.app.type} "
            f"| payload={response.payload}"
        )
        return False

    log(f"[DIRECT ACCEPTED] {response.payload}")
    return True

def read_file(file_path: str) -> tuple[str, bytes]:
    path = Path(file_path)

    if not path.is_file():
        raise FileNotFoundError(f"File not found: {file_path}")

    filename = path.name
    file_bytes = path.read_bytes()

    return filename, file_bytes

def prepare_file(file_path: str):
    filename, file_bytes = read_file(file_path=file_path)

    return {
        "filename": filename,
        "size": len(file_bytes),
        "data": file_bytes
    }

def offer_file(file_path: str):
    try:
        file_info = prepare_file(file_path)
    except (FileNotFoundError, OSError) as exc:
        log(f"[FILE ERROR] {exc}")
        return None

    transfer_id = uuid.uuid4().hex

    # Step 1: Offer the file.
    response = send_request(
        MessageType.FILE_OFFER,
        {
            "transfer_id": transfer_id,
            "filename": file_info["filename"],
            "size": file_info["size"],
        },
    )

    if response is None:
        log("[FILE OFFER FAILED] No application response")
        return None

    if response.app.type == MessageType.ERROR:
        log(f"[FILE OFFER REJECTED] {response.payload}")
        return None

    if response.app.type != MessageType.FILE_ACCEPT:
        log(
            f"[FILE OFFER FAILED] Unexpected response "
            f"type={response.app.type} | payload={response.payload}"
        )
        return None

    log(f"[FILE ACCEPTED] {response.payload}")

    # Step 2: Split the file into chunks.
    file_bytes = file_info["data"]

    total_chunks = (
        len(file_bytes) + FILE_CHUNK_SIZE - 1
    ) // FILE_CHUNK_SIZE

    log(
        f"[FILE TRANSFER STARTED] {file_info['filename']} "
        f"| size={len(file_bytes)} bytes "
        f"| chunks={total_chunks}"
    )

    def iter_chunk_payloads():
        for chunk_index, offset in enumerate(range(0, len(file_bytes), FILE_CHUNK_SIZE)):
            chunk = file_bytes[offset:offset + FILE_CHUNK_SIZE]
            encoded_chunk = base64.b64encode(chunk).decode("ascii")

            log(
                f"[FILE CHUNK SENDING] "
                f"{chunk_index + 1}/{total_chunks} "
                f"| raw_bytes={len(chunk)}"
            )

            yield {
                "transfer_id": transfer_id,
                "chunk_index": chunk_index,
                "data": encoded_chunk,
            }

    if not send_reliable_batch(
        MessageType.FILE_DATA,
        EVENT_REQ_ID,
        iter_chunk_payloads(),
    ):
        log("[FILE TRANSFER FAILED] Could not deliver all chunks")
        return None

    # Step 4: Tell the server that all chunks have been sent.
    response = send_request(
        MessageType.FILE_END,
        {
            "transfer_id": transfer_id,
            "total_chunks": total_chunks,
            "size": len(file_bytes),
        },
    )

    if response is None:
        log("[FILE TRANSFER FAILED] No final application response")
        return None

    if response.app.type == MessageType.ERROR:
        log(f"[FILE TRANSFER FAILED] {response.payload}")
        return None

    if response.app.type != MessageType.FILE_RESULT:
        log(
            f"[FILE TRANSFER FAILED] Unexpected response "
            f"type={response.app.type} | payload={response.payload}"
        )
        return None

    if response.payload.get("success") is not True:
        log(f"[FILE TRANSFER FAILED] {response.payload}")
        return None

    log(
        f"[FILE TRANSFER COMPLETE] "
        f"{file_info['filename']} | {len(file_bytes)} bytes"
    )

    return file_info

def file_send(recipient: str, filename: str) -> bool:
    response = send_request(
        MessageType.FILE_SEND,
        {
            "to": recipient,
            "filename": filename,
        },
    )

    if response is None:
        print("[FILESEND FAILED] No server response")
        return False

    if response.app.type == MessageType.ERROR:
        print(f"[FILESEND FAILED] {response.payload}")
        return False

    if response.app.type != MessageType.FILE_SENT:
        print(f"[FILESEND FAILED] Unexpected response: {response.payload}")
        return False

    print(f"[FILESEND ACCEPTED] {response.payload}")
    return True

def run() -> None:
    receiver_thread.start()
    handler_thread.start()
 
    try:
        if not open_session():
            return

        username = input("Username: ").strip()
        password = input("Password: ")

        response = send_request(
            MessageType.AUTH,
            {
                "username": username,
                "password": password,
            },
        )

        if response is None:
            log("[AUTH FAILED] No application response")
            return

        if response.app.type == MessageType.ERROR:
            log(f"[AUTH FAILED] {response.payload}")
            return

        if response.app.type != MessageType.AUTH_OK:
            log(
                f"[AUTH FAILED] Unexpected response "
                f"type={response.app.type} "
                f"| payload={response.payload}"
            )
            return

        log(f"[AUTH OK] {response.payload}")
 
        while True:
            command = input(
                "\nCommands: 'peers', 'ping', 'broadcast', 'direct', 'upload', 'sendfile', 'quit': "
            ).strip().lower()

            if command == "quit":
                break

            elif command == "peers":
                show_peer_list()

            elif command == "ping":
                message = input("Ping message: ").strip()
                if message:
                    ping(message)
                else:
                    log("[ERROR] Ping message cannot be empty")

            elif command == "broadcast":
                message = input("Broadcast message: ").strip()
                broadcast(message)

            elif command == "direct":
                recipient = input("Recipient username: ").strip()
                message = input("Direct message: ").strip()
                direct(recipient, message)

            elif command == 'upload':
                file_path = input("File path: ").strip()

                if not file_path:
                    log("[FILE ERROR] File path cannot be empty")
                    continue

                file_info = offer_file(file_path)

                if file_info is not None:
                    log(
                        f"[FILE READY] {file_info['filename']} "
                        f"| {file_info['size']} bytes"
                    )

            elif command == 'sendfile':
                recipient = input("Recipient username: ").strip()
                file_path = input("File path: ").strip()

                file_send(recipient=recipient, filename=file_path)

            else:
                log("Unknown command.")
 
    except KeyboardInterrupt:
        log("\n[INTERRUPTED]")

    finally:

        try:
            close_session()

        finally:
            running.clear()
            receiver_thread.join(timeout = 1.0)
            handler_thread.join()
            sock.close()
            log("[CLIENT SOCKET CLOSED]")
 
if __name__ == "__main__":
    run()