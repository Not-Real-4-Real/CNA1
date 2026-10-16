import queue
import secrets
import socket
import threading
 
from udp_protocol import *

SERVER_HOST = "127.0.0.1"
SERVER_PORT = 5000
SERVER_ADDRESS = (SERVER_HOST, SERVER_PORT)
 
BUFFER_SIZE = 1201
RETRANSMISSION_TIMEOUT = 1.0
MAX_RETRANSMISSIONS = 5

RESPONSE_TIMEOUT = RETRANSMISSION_TIMEOUT * (MAX_RETRANSMISSIONS + 3)

HEARTBEAT_INTERVAL = 10.0

session_id = secrets.token_bytes(16)
 
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.settimeout(0.1)

running = threading.Event()
running.set()

# Layer 2 (reliable) state
send_lock = threading.Lock()      # stop-and-wait: one unACKed DATA at a time
send_seq = 0                      # seq of the next DATA we send
pending_seq = None                # seq we are currently waiting an ACK for
ack_event = threading.Event()     # set by receiver when the pending DATA is ACKed
expected_recv_seq = 0             # seq we expect from the server next
 
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
EVENT_REQ_ID = 0                  # one-way server events carry req_id 0
peers = set()                     # usernames of other connected users
peers_lock = threading.Lock()

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

# Layer 2: Reliable delivery (stop-and-wait, alternating 0/1 sequence)
def send_control(packet_type: int, quiet: bool = False) -> None:
    "Send a header-only packet (OPEN / CLOSE / HEARTBEAT)."
    transport_send(
        encode_datagram(RDTHeader(session_id=session_id, type=packet_type))
    )
    if not quiet:
        log(f"[SENT CONTROL] type={packet_type} | session={session_id.hex()[:8]}")

def send_ack(ack_num: int) -> None:
    transport_send(
        encode_datagram(
            RDTHeader(session_id=session_id, ack_num=ack_num, type=RDTType.ACK)
        )
    )
    log(f"[ACK SENT] ack_num={ack_num}")

def send_reliable(msg_type: int, req_id: int, payload: dict) -> bool:
    global send_seq, pending_seq
 
    with send_lock:
        seq = send_seq
        packet = encode_datagram(
            RDTHeader(session_id=session_id, seq_num=seq, type=RDTType.DATA),
            AppHeader(length=0, type=msg_type, req_id=req_id),
            payload=payload,
        )
 
        pending_seq = seq
        ack_event.clear()
 
        try:
            for attempt in range(MAX_RETRANSMISSIONS + 1):
                transport_send(packet)
                if attempt == 0:
                    log(f"[DATA SENT] seq={seq} | req_id={req_id} | payload={payload}")
                else:
                    log(f"[RETRANSMIT] seq={seq} | retry={attempt}/{MAX_RETRANSMISSIONS}")
 
                if ack_event.wait(RETRANSMISSION_TIMEOUT):
                    send_seq ^= 1   # advance only after the correct ACK
                    return True
 
            log(f"[TRANSMISSION FAILED] seq={seq} | retries={MAX_RETRANSMISSIONS}")
            return False
        finally:
            pending_seq = None

def handle_ack(datagram) -> None:
    ack_num = datagram.rdt.ack_num
    if pending_seq is not None and ack_num == pending_seq:
        log(f"[ACK ACCEPTED] ack_num={ack_num}")
        ack_event.set()
    else:
        log(f"[IGNORED ACK] ack_num={ack_num}")

def handle_data(datagram) -> None:
    "ACK incoming DATA, drop duplicates, pass new messages upward."
    global expected_recv_seq

    seq = datagram.rdt.seq_num

    if seq not in (0, 1):
        log(f"[REJECTED DATA] Invalid seq={seq}")
        return

    # Always ACK, even duplicates, in case our previous ACK was lost.
    send_ack(seq)

    if seq != expected_recv_seq:
        log(f"[DUPLICATE DATA] seq={seq}")
        return

    expected_recv_seq ^= 1

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
        open_ack_event.set()
    elif packet_type == RDTType.CLOSE_ACK:
        close_ack_event.set()
    elif packet_type == RDTType.ACK:
        handle_ack(datagram)
    elif packet_type == RDTType.DATA:
        handle_data(datagram)
    else:
        log(f"[IGNORED] Unexpected RDT type={packet_type}")

def receiver_loop() -> None:
    while running.is_set():
        try:
            datagram = transport_receive()
        except socket.timeout:
            continue
        except OSError as error:
            if running.is_set():
                log(f"\n[RECEIVER ERROR] {error}")
                running.clear()
            break
 
        if datagram is not None:          
            handle_datagram(datagram)
 
receiver_thread = threading.Thread(target=receiver_loop, daemon=True)

# Layer 3: Session (OPEN / CLOSE handshakes, heartbeat)
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

    else:
        log(
            f"\n[SERVER EVENT] type={msg_type} "
            f"| payload={payload}"
        )

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

def run() -> None:
    receiver_thread.start()
 
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
                "\nCommands: 'peers', 'ping', 'broadcast', 'direct', 'quit': "
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

            else:
                log("Unknown command.")
 
    except KeyboardInterrupt:
        log("\n[INTERRUPTED]")

    finally:

        try:
            close_session()

        finally:
            running.clear()
            receiver_thread.join(timeout=1.0)
            sock.close()
            log("[CLIENT SOCKET CLOSED]")
 
if __name__ == "__main__":
    run()