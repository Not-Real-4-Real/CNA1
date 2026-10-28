import time
import socket
from dataclasses import dataclass, field

from udp_protocol import *

HOST = "127.0.0.1"
PORT = 5000
BUFFER_SIZE = 1201

RETRANSMISSION_TIMEOUT = 1.0  # seconds
MAX_RETRANSMISSIONS = 5

HEARTBEAT_TIMEOUT = 8.0  # seconds without valid client activity

@dataclass
class Session:
    address: tuple
    session_id: bytes

    # Heartbeat checker
    last_seen: float = field(default_factory=time.monotonic)

    # Outgoing DATA state
    send_seq_num: int = 0
    pending_seq_num: int | None = None
    pending_data: bytes | None = None
    pending_since: float | None = None
    retransmissions: int = 0

    # Incoming DATA state
    expected_recv_seq: int = 0

# Key: (client address, session ID)
sessions: dict[tuple, Session] = {}

def send_control(
    sock: socket.socket,
    address: tuple,
    session_id: bytes,
    packet_type: int,
) -> None:
    "Send an RDT control packet containing only its RDT header."

    rdt = RDTHeader(
        session_id=session_id,
        type=packet_type,
    )

    packet = encode_datagram(rdt)
    sock.sendto(packet, address)

def send_ack(
    sock: socket.socket,
    address: tuple,
    session_id: bytes,
    ack_num: int,
):
    packet = encode_datagram(
        rdt=RDTHeader(
            session_id=session_id,
            ack_num=ack_num,
            type=RDTType.ACK,
        )
    )

    sock.sendto(packet, address)

    print(
        f"[ACK SENT] {address} | ack_num={ack_num}"
    )

def send_pong(
    sock: socket.socket,
    session: Session,
    req_id: int,
    payload: dict,
) -> None:
    if session.pending_data is not None:
        print(f"[SEND BLOCKED] Session {session.session_id.hex()} already has DATA awaiting ACK")
        return
    
    rdt = RDTHeader(
        session_id=session.session_id,
        seq_num=session.send_seq_num,
        type=RDTType.DATA,
    )

    packet = encode_datagram(
        rdt=rdt,
        app=AppHeader(length=0, type=MessageType.PONG, req_id=req_id,),
        payload={
            "message": "Pong from server",
            "echo": payload.get("message"),
        },
    )

    session.pending_data = packet
    session.pending_seq_num = session.send_seq_num
    session.pending_since = time.monotonic()
    session.retransmissions = 0

    sock.sendto(packet, session.address)

    print(
        f"[PONG SENT] {session.address} "
        f"| seq={session.send_seq_num} "
        f"| req_id={req_id} "
        f"| awaiting ACK"
    )

def handle_open(
    sock: socket.socket,
    address: tuple,
    session_id: bytes,
) -> None:
    "Create a session, or repeat the response to an OPEN."

    key = (address, session_id)

    if key not in sessions:
        sessions[key] = Session(address=address, session_id=session_id,)

        print(
            f"[SESSION OPENED] {address} "
            f"| session={session_id.hex()}"
        )
    else:
        print(
            f"[OPEN REPEATED] {address} "
            f"| session={session_id.hex()}"
        )

    send_control(
        sock,
        address,
        session_id,
        RDTType.OPEN_ACK,
    )

def handle_close(
    sock: socket.socket,
    address: tuple,
    session_id: bytes,
) -> None:
    "Close a known session and ACK."

    key = (address, session_id)

    if key not in sessions:
        print(f"[CLOSE REJECTED] Unknown session from {address}")
        return

    del sessions[key]

    send_control(
        sock,
        address,
        session_id,
        RDTType.CLOSE_ACK,
    )

    print(
        f"[SESSION CLOSED] {address} "
        f"| session={session_id.hex()[:8]} "
        f"| active sessions={len(sessions)}"
    )

def handle_data(
    sock: socket.socket,
    address: tuple,
    datagram: Datagram,
) -> None:
    "Handle application data from a known session."

    key = (address, datagram.rdt.session_id)
    session = sessions.get(key)

    if session is None:
        print(
            f"[REJECTED DATA] Unknown session from {address}"
        )
        return

    if datagram.app is None or datagram.payload is None:
        print(f"[REJECTED DATA] Missing application data from {address}")
        return

    session.last_seen = time.monotonic()

    seq_num = datagram.rdt.seq_num

    # This implementation uses alternating sequence numbers: 0 and 1.
    if seq_num not in (0, 1):
        print(
            f"[REJECTED DATA] Invalid sequence number "
            f"{seq_num} from {address}"
        )
        return

    if seq_num == session.expected_recv_seq:
        # ACK the packet before processing its application content.
        send_ack(
            sock,
            address,
            session.session_id,
            ack_num=seq_num,
        )

        # Advance receive state only once.
        session.expected_recv_seq ^= 1

        app_type = datagram.app.type
        req_id = datagram.app.req_id
        payload = datagram.payload

        print(
            f"[DATA ACCEPTED] {address} "
            f"| seq={seq_num} "
            f"| req_id={req_id} "
            f"| payload={payload}"
        )

        if app_type == MessageType.PING:
            print(
                f"[PING RECEIVED] {address} "
                f"| seq={datagram.rdt.seq_num} "
                f"| req_id={req_id} "
                f"| payload={payload}"
            )

            send_pong(
                sock=sock,
                session=session,
                req_id=req_id,
                payload=payload,
            )

        else:
            print(
                f"[UNSUPPORTED APP TYPE] {app_type} from {address}"
            )

        return

    # Case 2: The packet has the previous sequence number.
    # With alternating bits, it is a duplicate of the last accepted DATA.
    print(
        f"[DUPLICATE DATA] {address} "
        f"| seq={seq_num} "
        f"| resending ACK"
    )

    send_ack(
        sock,
        address,
        session.session_id,
        ack_num=seq_num,
    )

def handle_ack(
    address: tuple,
    datagram: Datagram,       
):
    "Process an ACK for the server's DATA packet."

    key = (address, datagram.rdt.session_id)
    session = sessions.get(key)

    if session is None:
        print(f"[REJECTED ACK] Unknown session from {address}")
        return

    session.last_seen = time.monotonic()

    ack_num = datagram.rdt.ack_num

    if session.send_seq_num is None:
        print(
            f"[UNEXPECTED ACK] {address} "
            f"| ack_num={ack_num} "
            "| no outstanding DATA"
        )
        return

    # A stale or unrelated ACK must not clear the pending packet.
    if ack_num != session.pending_seq_num:
        print(
            f"[IGNORED ACK] {address} "
            f"| ack_num={ack_num} "
            f"| expected={session.pending_seq_num}"
        )
        return

    print(
        f"[ACK ACCEPTED] {address} "
        f"| ack_num={ack_num} "
        f"| retries={session.retransmissions}"
    )

    session.pending_data = None
    session.pending_seq_num = None
    session.pending_since = None
    session.retransmissions = 0

    # Alternate 0 -> 1 -> 0 -> ...
    session.send_seq_num ^= 1

def check_retransmission_timeouts(
    sock: socket.socket
):
    now = time.monotonic()

    # Use a snapshot because failed sessions may be removed.
    for key, session in list(sessions.items()):
        if session.pending_data is None:
            continue

        if session.pending_since is None:
            continue

        elapsed = now - session.pending_since

        if elapsed < RETRANSMISSION_TIMEOUT:
            continue

        # All retries sent with no matching ACK.
        if session.retransmissions >= MAX_RETRANSMISSIONS:
            print(
                f"[TRANSMISSION FAILED] {session.address} "
                f"| seq={session.pending_seq_num} "
                f"| retries={session.retransmissions} "
                "| invalidating session"
            )

            del sessions[key]
            continue

        sock.sendto(session.pending_data, session.address)

        session.retransmissions += 1
        session.pending_since = now

        print(
            f"[RETRANSMIT] {session.address} "
            f"| seq={session.pending_seq_num} "
            f"| retry={session.retransmissions}/"
            f"{MAX_RETRANSMISSIONS}"
        )

def check_session_timeouts():
    now = time.monotonic()

    for key, session in list(sessions.items()):
        inactive_for = now - session.last_seen

        if inactive_for <= HEARTBEAT_TIMEOUT:
            continue

        del sessions[key]

        print(
            f"[SESSION EXPIRED] {session.address} "
            f"| session={session.session_id.hex()[:8]} "
            f"| inactive={inactive_for:.1f}s "
            f"| active sessions={len(sessions)}"
        )

def run_server():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((HOST, PORT))
    sock.settimeout(0.1)

    print(f"UDP server listening on {HOST}:{PORT}")

    try:
        while True:
            try:
                data, address = sock.recvfrom(BUFFER_SIZE)

            except socket.timeout:
                check_retransmission_timeouts(sock)
                check_session_timeouts()
                continue

            print(
                f"\n[DATAGRAM RECEIVED] {address} "
                f"| bytes={len(data)}"
            )

            try:
                datagram = decode_datagram(data)
            except ValueError as error:
                print(f"[INVALID DATAGRAM] {address}: {error}")
                continue

            rdt = datagram.rdt

            if rdt.type == RDTType.OPEN:
                handle_open(sock, address, rdt.session_id)

            elif rdt.type == RDTType.CLOSE:
                handle_close(sock, address, rdt.session_id)

            elif rdt.type == RDTType.DATA:
                handle_data(sock, address, datagram)

            elif rdt.type == RDTType.ACK:
                handle_ack(address, datagram)

            else:
                print(f"[UNEXPECTED DATAGRAM]")

            check_retransmission_timeouts(sock)
            check_session_timeouts()

    except KeyboardInterrupt:
        print("\nServer shutting down.")

    finally:
        sock.close()

if __name__ == "__main__":
    run_server()