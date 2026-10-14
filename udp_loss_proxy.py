import socket

from udp_protocol import RDTType, decode_datagram


PROXY_HOST = "127.0.0.1"
PROXY_PORT = 5001

SERVER_ADDRESS = ("127.0.0.1", 5000)
BUFFER_SIZE = 1201

# Choose one:
# "none"         -> baseline, no loss
# "server_ack"   -> drop first server-to-client ACK
# "client_ack"   -> drop first client-to-server ACK
DROP_MODE = "client_ack"


def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((PROXY_HOST, PROXY_PORT))

    client_address = None
    dropped = False

    print(f"[PROXY] Listening on {PROXY_HOST}:{PROXY_PORT}")
    print(f"[PROXY] Forwarding to {SERVER_ADDRESS}")
    print(f"[PROXY] Loss mode: {DROP_MODE}")

    try:
        while True:
            packet, sender = sock.recvfrom(BUFFER_SIZE)

            # Server responses originate from the server's address.
            from_server = sender == SERVER_ADDRESS

            if from_server:
                destination = client_address

                if destination is None:
                    continue
            else:
                # Remember the most recent client endpoint.
                client_address = sender
                destination = SERVER_ADDRESS

            try:
                datagram = decode_datagram(packet)
            except Exception as exc:
                print(f"[PROXY] Invalid datagram: {exc}")
                continue

            is_ack = datagram.rdt.type == RDTType.ACK

            should_drop = (
                not dropped
                and is_ack
                and (
                    (DROP_MODE == "server_ack" and from_server)
                    or
                    (DROP_MODE == "client_ack" and not from_server)
                )
            )

            direction = (
                "server -> client"
                if from_server
                else "client -> server"
            )

            if should_drop:
                dropped = True
                print(
                    f"[DROP] {direction} "
                    f"| ACK={datagram.rdt.ack_num}"
                )
                continue

            sock.sendto(packet, destination)

            print(
                f"[FORWARD] {direction} "
                f"| RDT type={datagram.rdt.type}"
            )

    except KeyboardInterrupt:
        print("\n[PROXY] Stopped")

    finally:
        sock.close()


if __name__ == "__main__":
    main()