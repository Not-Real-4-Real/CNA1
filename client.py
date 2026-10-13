import socket

from protocol import MessageType, send_frame, recv_frame

HOST = "127.0.0.1"
PORT = 5000


def start_client():
    client_socket = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM
    )

    next_req_id = 1

    try:
        client_socket.connect((HOST, PORT))
        print(f"Connected to server at {HOST}:{PORT}")

        # Step 1: Perform the handshake.
        send_frame(
            client_socket,
            MessageType.WELCOME,
            req_id=1,
            payload={"client": "Đây là test."}
        )

        # Receive the server's response.
        header, payload = recv_frame(client_socket)

        if (
            header.type != MessageType.WELCOME
            or header.req_id != next_req_id
        ):
            print(f"Handshake failed: {payload}")
            return

        print(f"Received type: {header.type}")
        print(f"Request ID: {header.req_id}")
        print(f"Payload length: {header.length}")
        print(f"Server: {payload['message']}")
        next_req_id += 1

        # Step 2: Exchange multiple messages.
        while True:
            command = input(
                "\nEnter 'ping' to test or 'quit' to disconnect: "
            ).strip().lower()

            if command == "ping":
                send_frame(
                    client_socket,
                    MessageType.PING,
                    req_id=next_req_id,
                    payload={"message": "Ping from client"},
                )

                header, payload = recv_frame(client_socket)

                print(f"Received type: {header.type}")
                print(f"Request ID: {header.req_id}")
                print(f"Payload length: {header.length}")
                print(f"Payload: {payload}")

                if (
                    header.type != MessageType.PONG
                    or header.req_id != next_req_id
                ):
                    print("Unexpected response.")

                next_req_id += 1

            elif command == "quit":
                send_frame(
                    client_socket,
                    MessageType.LOGOUT,
                    req_id=next_req_id,
                    payload={},
                )
                print("Disconnecting...")
                break

            else:
                print("Unknown command. Enter 'ping' or 'quit'.")

    except (OSError, ConnectionError, ValueError) as error:
        print(f"Client error: {error}")

    finally:
        client_socket.close()
        print("Client socket closed.")


if __name__ == "__main__":
    start_client()
