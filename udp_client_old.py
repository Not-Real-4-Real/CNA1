import secrets
import socket
import time
import threading

from udp_protocol import *

SERVER_HOST = "127.0.0.1"
SERVER_PORT = 5000
SERVER_ADDRESS = (SERVER_HOST, SERVER_PORT)

BUFFER_SIZE = 1201

RETRANSMISSION_TIMEOUT = 1.0
MAX_RETRANSMISSIONS = 5

HEARTBEAT_INTERVAL = 2.0

class UDPClient:
    def __init__(self):
        self.session_id = secrets.token_bytes(16)

        # Outgoing DATA state
        self.send_seq_num = 0
        self.pending_seq_num = None
        self.pending_data = None
        self.pending_since = None
        self.retransmissions = 0

        # Incoming DATA state
        self.expected_recv_seq = 0

        self.next_req_id = 1

        self.sock = socket.socket(
            socket.AF_INET,
            socket.SOCK_DGRAM,
        )

        # Allows the ping loop to periodically check its timer.
        self.sock.settimeout(0.1)

        self.established = False

        self.running = threading.Event()
        self.running.set()

        self.state_lock = threading.Lock()

        # Events signaled by the receiver thread.
        self.ack_event = threading.Event()
        self.pong_event = threading.Event()
        self.close_event = threading.Event()
        self.stop_event = threading.Event()

        self.last_pong = None
        self.receiver_thread = None
        self.heartbeat_thread = None

    def send_control(self, packet_type: int) -> None:
        rdt = RDTHeader(session_id=self.session_id, type=packet_type)

        packet = encode_datagram(rdt)

        self.sock.sendto(packet, SERVER_ADDRESS)

        print(
            f"[SENT CONTROL] type={packet_type} "
            f"| session={self.session_id.hex()[:8]}"
        )

    def send_ack(self, ack_num: int) -> None:
        packet = encode_datagram(
            RDTHeader(
                session_id=self.session_id,
                ack_num=ack_num,
                type=RDTType.ACK,
            )
        )

        self.sock.sendto(packet, SERVER_ADDRESS)

        print(f"[ACK SENT] ack_num={ack_num}")

    def receive_datagram(self):
        data, address = self.sock.recvfrom(BUFFER_SIZE)

        if address != SERVER_ADDRESS:
            print(f"[IGNORED] Unexpected sender: {address}")
            return None

        try:
            datagram = decode_datagram(data)
        except ValueError as error:
            print(f"[INVALID DATAGRAM] {error}")
            return None

        if datagram.rdt.session_id != self.session_id:
            print("[IGNORED] Session ID mismatch")
            return None

        return datagram

    def open_session(self) -> bool:
        print(
            f"[OPENING SESSION] "
            f"session={self.session_id.hex()}"
        )

        self.send_control(RDTType.OPEN)

        try:
            while True:
                datagram = self.receive_datagram()

                if datagram is None:
                    continue

                if datagram.rdt.type == RDTType.OPEN_ACK:
                    self.established = True
                    print("[SESSION ESTABLISHED]")

                    self.receiver_thread = threading.Thread(
                        target=self.receiver_loop,
                        name="udp-receiver",
                        daemon=True,
                    )

                    self.heartbeat_thread = threading.Thread(
                        target=self.heartbeat_loop,
                        name="udp-heartbeat",
                        daemon=True,
                    )

                    self.receiver_thread.start()
                    self.heartbeat_thread.start()
                    return True

                print(
                    f"[IGNORED] Expected OPEN_ACK, "
                    f"received RDT type={datagram.rdt.type}"
                )

        except socket.timeout:
            print("[TIMEOUT] No OPEN_ACK received")
            return False

    def close_session(self) -> bool:
        if not self.established:
            return False

        print("[CLOSING SESSION]")

        self.send_control(RDTType.CLOSE)

        try:
            while True:
                datagram = self.receive_datagram()

                if datagram is None:
                    continue

                if datagram.rdt.type == RDTType.CLOSE_ACK:
                    self.established = False
                    print("[SESSION CLOSED]")

                    self.stop_event.set()
                    self.running.clear()

                    self.sock.close()

                    if self.heartbeat_thread is not None:
                        self.heartbeat_thread.join(timeout=1.0)

                    if self.receiver_thread is not None:
                        self.receiver_thread.join(timeout=1.0)
                        
                    return True

        except socket.timeout:
            print("[TIMEOUT] No CLOSE_ACK received")
            return False

    def handle_data(self, datagram: Datagram) -> bool:
        seq_num = datagram.rdt.seq_num

        if seq_num not in (0, 1):
            print(f"[REJECTED DATA] Invalid seq={seq_num}")
            return False

        if seq_num == self.expected_recv_seq:
            self.send_ack(seq_num)
            self.expected_recv_seq ^= 1

            if datagram.app is None:
                print("[REJECTED DATA] Missing application header")
                return False

            if datagram.app.type != MessageType.PONG:
                print(
                    f"[UNSUPPORTED APP TYPE] "
                    f"{datagram.app.type}"
                )
                return False

            print(
                f"[PONG RECEIVED] seq={seq_num} "
                f"| req_id={datagram.app.req_id} "
                f"| payload={datagram.payload}"
            )

            return True
        
        # Duplicate: ACK again, don't process it twice.
        print(f"[DUPLICATE DATA] seq={seq_num}")
        self.send_ack(seq_num)
        return False
    
    def ping(self, message: str) -> bool:
        if not self.established:
            print("[ERROR] No established session")
            return False

        if self.pending_data is not None:
            print("[ERROR] Previous DATA is still awaiting ACK")
            return False

        req_id = self.next_req_id

        packet = encode_datagram(
            RDTHeader(
                session_id=self.session_id,
                seq_num=self.send_seq_num,
                type=RDTType.DATA,
            ),
            AppHeader(
                length=0,
                type=MessageType.PING,
                req_id=req_id,
            ),
            payload={"message": message},
        )

        # Save the exact bytes before sending.
        self.pending_data = packet
        self.pending_seq_num = self.send_seq_num
        self.pending_since = time.monotonic()
        self.retransmissions = 0

        self.sock.sendto(packet, SERVER_ADDRESS)

        print(
            f"[PING SENT] seq={self.send_seq_num} "
            f"| req_id={req_id} "
            f"| payload={{'message': {message!r}}}"
        )

        self.next_req_id += 1

        ack_received = False
        pong_received = False

        # Allows time for the server to finish its own retries if the response or an ACK is lost.
        response_deadline = (
            time.monotonic()
            + RETRANSMISSION_TIMEOUT * (MAX_RETRANSMISSIONS + 3)
        )

        while not (ack_received and pong_received):
            now = time.monotonic()

            if now >= response_deadline:
                print("[TIMEOUT] PING transaction did not complete")
                return False

            # Retransmit only while our PING is unacknowledged.
            if self.pending_data is not None:
                elapsed = now - self.pending_since

                if elapsed >= RETRANSMISSION_TIMEOUT:
                    if self.retransmissions >= MAX_RETRANSMISSIONS:
                        print(
                            f"[TRANSMISSION FAILED] seq="
                            f"{self.pending_seq_num} "
                            f"| retries={self.retransmissions}"
                        )

                        self.pending_data = None
                        self.pending_seq_num = None
                        self.pending_since = None
                        return False

                    # Send the exact same packet, including sequence number.
                    self.sock.sendto(
                        self.pending_data,
                        SERVER_ADDRESS,
                    )

                    self.retransmissions += 1
                    self.pending_since = time.monotonic()

                    print(
                        f"[RETRANSMIT] seq={self.pending_seq_num} "
                        f"| retry={self.retransmissions}/"
                        f"{MAX_RETRANSMISSIONS}"
                    )

            try:
                datagram = self.receive_datagram()
            except socket.timeout:
                continue

            if datagram is None:
                continue

            rdt = datagram.rdt

            # Handle the ACK for our PING.
            if rdt.type == RDTType.ACK:
                if (
                    self.pending_seq_num is not None
                    and rdt.ack_num == self.pending_seq_num
                ):
                    print(f"[ACK ACCEPTED] ack_num={rdt.ack_num}")

                    self.pending_data = None
                    self.pending_seq_num = None
                    self.pending_since = None
                    self.retransmissions = 0

                    # Advance only after receiving the correct ACK.
                    self.send_seq_num ^= 1
                    ack_received = True

                else:
                    print(
                        f"[IGNORED ACK] ack_num={rdt.ack_num}"
                    )

                continue

            # Handle a PONG, including duplicate suppression.
            if rdt.type == RDTType.DATA:
                if datagram.app is None:
                    print("[IGNORED DATA] Missing application header")
                    continue

                if datagram.app.type != MessageType.PONG:
                    print(
                        f"[IGNORED DATA] Unexpected app type="
                        f"{datagram.app.type}"
                    )
                    continue

                # ACK even if it is a duplicate. 
                # Only mark the transaction complete for the matching request ID.
                is_new = self.handle_data(datagram)

                if (
                    is_new
                    and datagram.app.req_id == req_id
                ):
                    pong_received = True

                elif datagram.app.req_id != req_id:
                    print(
                        f"[IGNORED PONG] req_id="
                        f"{datagram.app.req_id}; expected={req_id}"
                    )

                continue

            print(f"[IGNORED] Unexpected RDT type={rdt.type}")

        print(f"[PING COMPLETE] req_id={req_id}")
        return True

    def heartbeat_loop(self):
        while not self.stop_event.wait(HEARTBEAT_INTERVAL):
            if not self.established:
                break

            try:
                packet = encode_datagram(
                    RDTHeader(
                        session_id=self.session_id,
                        type=RDTType.HEARTBEAT,
                    )
                )
                self.sock.sendto(packet, SERVER_ADDRESS)
                print("[HEARTBEAT SENT]")

            except OSError as error:
                if self.running.is_set():
                    print(f"[HEARTBEAT ERROR] {error}")
                self.running.clear()
                break

    def run(self):
        try:
            if not self.open_session():
                return

            while True:
                message = input(
                    "\nEnter a message to ping "
                    "(or 'quit' to disconnect): "
                ).strip()

                if message.lower() == "quit":
                    break

                if not message:
                    continue

                self.ping(message)

        except KeyboardInterrupt:
            print("\n[INTERRUPTED]")

        finally:
            try:
                if self.established:
                    self.close_session()
            finally:
                self.sock.close()
                print("[CLIENT SOCKET CLOSED]")

if __name__ == "__main__":
    client = UDPClient()
    client.run()