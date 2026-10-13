import socket
import queue
import threading

from protocol import MessageType, send_frame, recv_frame

# Request ID -> queue for the corresponding response.
pending_requests = {}
pending_lock = threading.Lock()

# Request IDs must be unique among outstanding requests.
next_req_id = 2
req_id_lock = threading.Lock()

# Shared state for peer membership.
peers = set()
peers_lock = threading.Lock()

running = threading.Event()
running.set()

HOST = "127.0.0.1"
PORT = 5000

def receiver_loop(sock):
    while running.is_set():
        try:
            header, payload = recv_frame(sock)

        except (OSError, ConnectionError, ValueError) as error:
            if running.is_set():
                print(f"\n[RECEIVER ERROR] {error}")
            running.clear()
            break  

        if header.req_id == 0:
            if header.type == MessageType.USER_JOIN:
                username = payload["username"]

                with peers_lock:
                    peers.add(username)

                print(f"\n[USER JOINED] {username}")

            elif header.type == MessageType.USER_LEAVE:
                username = payload["username"]

                with peers_lock:
                    peers.discard(username)

                print(f"\n[USER LEFT] {username}")

            else:
                print(
                    f"\n[SERVER EVENT] Type={header.type}, "
                    f"Payload={payload}"
                )

            continue

        with pending_lock:
            response_queue = pending_requests.get(header.req_id)

        if response_queue is not None:
            response_queue.put((header, payload))
        else:
            print(
                f"\n[UNMATCHED RESPONSE] Type={header.type}, "
                f"ReqID={header.req_id}, Payload={payload}"
            )

def send_request(sock, msg_type, payload, timeout = 10):
    global next_req_id

    with req_id_lock:
        req_id = next_req_id
        next_req_id += 1

    response_queue = queue.Queue(maxsize=1)
    with pending_lock:
        pending_requests[req_id] = response_queue

    try: 
        send_frame(sock, msg_type, req_id, payload)

        header, response_payload = response_queue.get(timeout=timeout)

        if header.type == MessageType.ERROR:
            print(
                f"Server error: {response_payload}"
            )
        else:
            print(
                f"Received response type: {header.type}\n"
                f"Request ID: {header.req_id}\n"
                f"Payload: {response_payload}"
            )

        return header, response_payload

    except queue.Empty:
        print(f"No response for request ID {req_id} within {timeout}s")
        return None

    finally:
        with pending_lock:
            pending_requests.pop(req_id, None)

def start_client():
    client_socket = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM
    )

    try:
        client_socket.connect((HOST, PORT))
        print(f"Connected to server at {HOST}:{PORT}")

        # Step 1: Perform the handshake.
        send_frame(
            client_socket,
            MessageType.HELLO,
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
        print(f"Payload length: {header.length}")
        print(f"Server: {payload['message']}")
        print(f"My username: {payload['username']}")
        print(f"Current peers: {payload['peers']}")

        receiver_thread = threading.Thread(
            target=receiver_loop,
            args=(client_socket,),
            daemon=True,
        )
        receiver_thread.start()

        # Step 2: Exchange multiple messages.
        while True:
            command = input("\nEnter 'ping' to test or 'quit' to disconnect: ").strip().lower()

            if command == "ping":
                send_request(
                    client_socket,
                    MessageType.PING,
                    {"message": "Ping from client"},
                )

            elif command == "quit":
                    try:
                        send_frame(
                            client_socket,
                            MessageType.LOGOUT,
                            req_id=0,
                            payload={},
                        )
                        print("Disconnecting...")

                    except (OSError, ConnectionError, ValueError) as error:
                        print(f"Error while sending LOGOUT: {error}")

                    running.clear()
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
