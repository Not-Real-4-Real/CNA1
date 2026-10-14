import secrets
import socket

from udp_protocol import *

SERVER_HOST = "127.0.0.1"
SERVER_PORT = 5000

SERVER_ADDRESS = (SERVER_HOST, SERVER_PORT)

BUFFER_SIZE = 1201
SOCKET_TIMEOUT = 3.0

class UDPClient:
    def __init__(self):
        self.session_id = secrets.token_bytes(16)
        self.next_seq_num = 1
        self.next_req_id = 1

        self.sock = socket.socket(
            socket.AF_INET,
            socket.SOCK_DGRAM,
        )

        self.sock.settimeout(SOCKET_TIMEOUT)

        self.established = False

    def send_control(self, packet_type: int) -> None:
        rdt = RDTHeader(session_id=self.session_id, type=packet_type)

        packet = encode_datagram(rdt)

        self.sock.sendto(packet, SERVER_ADDRESS)

        print(
            f"[SENT CONTROL] type={packet_type} "
            f"| session={self.session_id.hex()[:8]}"
        )

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
                    return True

                print(
                    f"[IGNORED] Expected CLOSE_ACK, "
                    f"received RDT type={datagram.rdt.type}"
                )

        except socket.timeout:
            print("[TIMEOUT] No OPEN_ACK received")
            return False

    def ping(self, message: str) -> bool:
        if not self.established:
            print("[ERROR] No established session")
            return False
    
        req_id = self.next_req_id

        rdt = RDTHeader(self.session_id, self.next_seq_num, type=RDTType.DATA,)

        app = AppHeader(length=0, type=MessageType.PING, req_id=req_id,)

        payload = { "message": message, }

        packet = encode_datagram(rdt, app, payload=payload)

        self.sock.sendto(packet, SERVER_ADDRESS)

        print(
            f"[PING SENT] seq={self.next_seq_num} "
            f"| req_id={req_id} "
            f"| payload={payload}"
        )

        self.next_seq_num += 1
        self.next_req_id += 1

        try:
            while True:
                datagram = self.receive_datagram()

                if datagram is None:
                    continue

                if datagram.rdt.type != RDTType.DATA:
                    print(
                        f"[IGNORED] Expected DATA, "
                        f"received RDT type={datagram.rdt.type}"
                    )
                    continue

                if datagram.app is None:
                    continue

                if datagram.app.type != MessageType.PONG:
                    print(
                        f"[IGNORED] Expected PONG, "
                        f"received application type={datagram.app.type}"
                    )
                    continue

                if datagram.app.req_id != req_id:
                    print(
                        f"[IGNORED] Request ID mismatch: "
                        f"{datagram.app.req_id}"
                    )
                    continue

                print(
                    f"[PONG RECEIVED] "
                    f"seq={datagram.rdt.seq_num} "
                    f"| req_id={datagram.app.req_id} "
                    f"| payload={datagram.payload}"
                )

                return True

        except socket.timeout:
            print("[TIMEOUT] No OPEN_ACK received")
            return False

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