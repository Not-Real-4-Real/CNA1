import socket
from dataclasses import dataclass

from udp_protocol import *

HOST = "127.0.0.1"
PORT = 5000
BUFFER_SIZE = 1201

@dataclass
class Session:
    address: tuple
    session_id: bytes
    next_seq_num: int = 1

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

def send_pong(
    sock: socket.socket,
    session: Session,
    req_id: int,
    payload: dict,
) -> None:
    rdt = RDTHeader(
        session_id=session.session_id,
        seq_num=session.next_seq_num,
        type=RDTType.DATA,
    )

    packet = encode_datagram(
        rdt=rdt,
        app=AppHeader(length=0, type=MessageType.PONG, req_id=req_id,),
        payload={"message": "Pong from server"},
    )

    sock.sendto(packet, session.address)

    print(
        f"[PONG SENT] {session.address} "
        f"| session={session.session_id.hex()[:8]} "
        f"| seq={session.next_seq_num} "
        f"| req_id={req_id}"
    )

    session.next_seq_num += 1

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

    app_type = datagram.app.type
    req_id = datagram.app.req_id
    payload = datagram.payload

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

def run_server():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((HOST, PORT))

    print(f"UDP server listening on {HOST}:{PORT}")

    try:
        while True:
            data, address = sock.recvfrom(BUFFER_SIZE)

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
                print(
                    f"[ACK RECEIVED] {address} "
                    f"| ack_num={rdt.ack_num} "
                    "(reliability not implemented yet)"
                )

            else:
                print(f"[UNEXPECTED DATAGRAM]")

    except KeyboardInterrupt:
        print("\nServer shutting down.")

if __name__ == "__main__":
    run_server()