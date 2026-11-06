
import socket
import struct

from udp_protocol import RDTType, MessageType, decode_datagram


PROXY_HOST = "127.0.0.1"
PROXY_PORT = 5001

SERVER_ADDRESS = ("127.0.0.1", 5000)
BUFFER_SIZE = 1201

RDT_HEADER_SIZE = struct.calcsize("!16sIIBB")
APP_HEADER_SIZE = struct.calcsize("!IBIB")

# Choose one:
# "none"
# "client_file_data"
# "server_ack_after_file_data"
# "client_ack_after_server_data"
DROP_MODE = "client_file_data"


def get_app_type(packet: bytes):
    """Read the application message type from an RDT DATA packet."""
    offset = RDT_HEADER_SIZE

    if len(packet) < offset + APP_HEADER_SIZE:
        return None

    # Application header: length (4B), type (1B), req_id (4B), flags (1B)
    return packet[offset + 4]


def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((PROXY_HOST, PROXY_PORT))

    client_address = None
    dropped = False

    # Used to target ACKs associated with file-transfer traffic.
    saw_file_data = False
    saw_server_data = False

    print(f"[PROXY] Listening on {PROXY_HOST}:{PROXY_PORT}")
    print(f"[PROXY] Forwarding to {SERVER_ADDRESS}")
    print(f"[PROXY] Loss mode: {DROP_MODE}")

    try:
        while True:
            packet, sender = sock.recvfrom(BUFFER_SIZE)

            from_server = sender == SERVER_ADDRESS

            if from_server:
                destination = client_address
                if destination is None:
                    continue
            else:
                client_address = sender
                destination = SERVER_ADDRESS

            try:
                datagram = decode_datagram(packet)
            except Exception as exc:
                print(f"[PROXY] Invalid datagram: {exc}")
                continue

            rdt_type = datagram.rdt.type
            is_ack = rdt_type == RDTType.ACK
            is_data = rdt_type == RDTType.DATA

            app_type = get_app_type(packet) if is_data else None
            is_file_data = (
                app_type == MessageType.FILE_DATA
            )

            should_drop = False

            if not dropped:
                if (
                    DROP_MODE == "client_file_data"
                    and not from_server
                    and is_file_data
                ):
                    should_drop = True

                elif (
                    DROP_MODE == "server_ack_after_file_data"
                    and from_server
                    and is_ack
                    and saw_file_data
                ):
                    should_drop = True

                elif (
                    DROP_MODE == "client_ack_after_server_data"
                    and not from_server
                    and is_ack
                    and saw_server_data
                ):
                    should_drop = True

            direction = (
                "server -> client"
                if from_server
                else "client -> server"
            )

            if should_drop:
                dropped = True
                print(
                    f"[DROP] {direction} "
                    f"| RDT={rdt_type} "
                    f"| ACK={datagram.rdt.ack_num if is_ack else '-'} "
                    f"| app_type={app_type}"
                )
                continue

            sock.sendto(packet, destination)

            print(
                f"[FORWARD] {direction} "
                f"| RDT={rdt_type} "
                f"| app_type={app_type}"
            )

            if not from_server and is_file_data:
                saw_file_data = True

            if from_server and is_data:
                saw_server_data = True

    except KeyboardInterrupt:
        print("\n[PROXY] Stopped")

    finally:
        sock.close()


if __name__ == "__main__":
    main()
