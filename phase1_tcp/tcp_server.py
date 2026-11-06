import socket
import threading

from CNA1.phase1_tcp.client_connection import ClientConnection
from CNA1.phase1_tcp.protocol import MessageType, send_frame, recv_frame

HOST = "127.0.0.1"
PORT = 5000

ACCOUNTS = {
    "alice": "alice123",
    "bob": "bob123",
    "charlie": "charlie123",
    "doug": "doug123",
}

# Shared registry: username -> client connection.
clients = {}

# Protects registry operations.
clients_lock = threading.Lock()

# Serialize join and leave so peer list stays consistent
membership_lock = threading.Lock()

def register_client(client, username):
    "Register the client."

    with clients_lock:
        if username in clients:
            return False, [], []

        client.username = username
        clients[username] = client

        peers = [
            {"username": name}
            for name in clients if name != username
        ]

        existing_clients = [
            peer for name, peer in clients.items() if name != username
        ]

    return True, peers, existing_clients

def unregister_client(client):
    "Remove a client from the registry if registered."
    if client.username is None:
        return

    with membership_lock:
        with clients_lock:
            username = client.username
            if clients.get(username) is not client:
                client.username = None
                return

            del clients[username]

            remaining_clients = list(clients.values())
            active_count = len(clients)

        for peer in remaining_clients:
            try:
                send_to_client(
                    peer,
                    MessageType.USER_LEAVE,
                    req_id = 0,
                    payload={"username": username}
                )
            except (OSError, ConnectionError, ValueError) as error:
                print(
                    f"[NOTIFY ERROR] Could not notify "
                    f"{peer.username} about {username} leaving: {error}"
                )

        client.username = None

    print(
        f"[UNREGISTERED] {client.username} | "
        f"{client.address} | Active clients: {active_count}"
    )

def get_connected_users():
    "Return a snapshot of currently registered usernames."
    with clients_lock:
        return list(clients.keys())

def broadcast_message(sender, message):
    with clients_lock:
        recipients = [client for client in clients.values() if client is not sender]

    successful_sends = 0

    for recipient in recipients:
        try:
            send_to_client(
                recipient,
                MessageType.MSG_DELIVER,
                req_id=0,
                payload={
                    "from": sender.username,
                    "message": message,
                },
            )
            successful_sends += 1

        except (OSError, ConnectionError, ValueError) as error:
            print(
                f"[BROADCAST ERROR] Could not send to "
                f"{recipient.username}: {error}"
            )    

    print(
        f"[BROADCAST] {sender.username} -> "
        f"{successful_sends}/{len(recipients)} recipients"
    )

    return successful_sends

def direct_message(sender, receiver_username, message):
    with clients_lock:
        recipient = clients.get(receiver_username)

    if recipient is None:
        return False, {
            "code": 404,
            "message": f"User '{receiver_username}' is not connected",
        }
    
    try:
        send_to_client(
            recipient,
            MessageType.MSG_DELIVER,
            req_id=0,
            payload={
                "from": sender.username,
                "message": message,
            },
        )

    except (OSError, ConnectionError, ValueError) as error:
        print(
            f"[DIRECT ERROR] Could not send to "
            f"{receiver_username}: {error}"
        ) 
        return False, {
            "code": 503,
            "message": f"Could not deliver message to '{receiver_username}'",
        }   

    print(
        f"[DIRECT] {sender.username} -> {receiver_username}"
    )
    return True, None

def send_to_client(client, msg_type, req_id, payload, flags=0):
    "Send one complete frame safely to a client."
    with client.send_lock:
        send_frame(
            client.sock,
            msg_type,
            req_id,
            payload,
            flags=flags,
        ) 

def handle_client(client_socket, client_address):
    "Thread to handle 1 client"
    client = ClientConnection(sock=client_socket, address=client_address)

    print(f"[TCP CONNECTED] {client_address}")

    try:
        # Timeout for testing connection timeout.
        # client.sock.settimeout(10.0)

        with client.sock:
            # 1. Receive and validate the handshake.
            hello_header, hello_payload = recv_frame(client.sock)

            if hello_header.type != MessageType.HELLO:
                send_to_client(
                    client,
                    MessageType.ERROR,
                    hello_header.req_id,
                    {
                        "code": 400,
                        "message": "Expected HELLO",
                    },
                )
                return

            # 2. Complete the handshake.
            send_to_client(
                client,
                MessageType.WELCOME,
                hello_header.req_id,
                {
                    "message": "Connection established",

                },
            )

            print(f"[HELLO] TCP session established: {client.address}")

            auth_header, auth_payload = recv_frame(client.sock)

            if auth_header.type != MessageType.AUTH:
                send_to_client(
                    client,
                    MessageType.ERROR,
                    auth_header.req_id,
                    {
                        "code": 401,
                        "message": "Authentication required"
                    },
                )
                return

            username = auth_payload.get("username")
            password = auth_payload.get("password")

            if (
                not isinstance(username, str)
                or not username
                or not isinstance(password, str)
                or not password
                or ACCOUNTS.get(username) != password
            ):
                send_to_client(
                    client,
                    MessageType.ERROR,
                    auth_header.req_id,
                    {
                        "code": 401,
                        "message": "Invalid username or password"
                    },
                )
                return

            # 2. Register only after receiving a valid AUTH.
            with membership_lock:
                registered, peers, existing_clients = register_client(client, username)

                if not registered:
                    send_to_client(
                        client,
                        MessageType.ERROR,
                        auth_header.req_id,
                        {
                            "code": 409,
                            "message": "Account already connected"
                        },
                    )
                    return

                send_to_client(
                    client,
                    MessageType.AUTH_OK,
                    auth_header.req_id,
                    {
                        "username": username,
                        "message": "Authentication successful",
                        "peers": peers,
                    },
                )

                for peer in existing_clients:
                    try:
                        send_to_client(
                            peer,
                            MessageType.USER_JOIN,
                            req_id=0,
                            payload={"username": username},
                        )
                    except (OSError, ConnectionError, ValueError) as error:
                        print(
                            f"[NOTIFY ERROR] Could not notify "
                            f"{peer.username} about {username} joining: "
                            f"{error}"
                        )

            print(
                f"[AUTH OK] {username} | {client.address} | "
                f"Active clients: {len(clients)}"
            )

            # Step 2: Keep the connection alive.
            while True:
                header, payload = recv_frame(client_socket)

                print(
                    f"[RECEIVED] {client_address} | "
                    f"Type: {header.type} | "
                    f"ReqID: {header.req_id} | "
                    f"Payload length: {header.length} | "
                    f"Payload: {payload}"
                )

                if header.type == MessageType.PING:
                    send_frame(
                        client_socket,
                        MessageType.PONG,
                        header.req_id,
                        {"message": "Pong from server"},
                    )

                elif header.type == MessageType.MSG_BROADCAST:
                    message = payload.get("message")

                    if not isinstance(message, str) or not message.strip():
                        send_to_client(
                            client,
                            MessageType.ERROR,
                            header.req_id,
                            {
                                "code": 400,
                                "message": "Broadcast message must be a non-empty string",
                            },
                        )
                        continue

                    recipient_count = broadcast_message(client, message)

                    send_to_client(
                        client,
                        MessageType.MSG_SENT,
                        header.req_id,
                        {
                            "message": "Broadcast accepted",
                            "recipients": recipient_count,
                        },
                    )

                elif header.type == MessageType.MSG_DIRECT:
                    receiver = payload.get("to")
                    message = payload.get("message")

                    if (not isinstance(receiver, str) or not receiver.strip()):
                        send_to_client(
                            client,
                            MessageType.ERROR,
                            header.req_id,
                            {
                                "code": 400,
                                "message": "A recipient username is required",
                            },
                        )
                        continue

                    if (not isinstance(message, str) or not message.strip()):
                        send_to_client(
                            client,
                            MessageType.ERROR,
                            header.req_id,
                            {
                                "code": 400,
                                "message": "Direct message must be a non-empty string",
                            },
                        )
                        continue

                    receiver = receiver.strip()

                    success, error_payload = direct_message(
                        client,
                        receiver,
                        message,
                    )

                    if not success:
                        send_to_client(
                            client,
                            MessageType.ERROR,
                            header.req_id,
                            error_payload,
                        )
                        continue

                    send_to_client(
                        client,
                        MessageType.MSG_SENT,
                        header.req_id,
                        {
                            "message": "Direct message accepted",
                            "recipient": receiver,
                        },
                    )

                elif header.type == MessageType.LOGOUT:
                    print(f"[LOGOUT] {client_address}")
                    break

                else:
                    send_frame(
                        client_socket,
                        MessageType.ERROR,
                        header.req_id,
                        {
                            "code": 400,
                            "message": "Unsupported message type",
                        },
                    )

    except socket.timeout:
        print(f"[TIMEOUT] {client_address} did not send data in time")

    except (OSError, ConnectionError, ValueError) as error:
        print(f"[CONNECTION ERROR] {client_address}: {error}")

    finally:
        unregister_client(client)

def start_server():
    # 1. Create a TCP socket using IPv4.
    server_socket = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM
    )

    try:
        server_socket.bind((HOST, PORT))
        server_socket.listen(5)

        print(f"Server listening on {HOST}:{PORT}")

        while True:
            client_socket, client_address = server_socket.accept()

            # Give each client its own thread.
            client_thread = threading.Thread(
                target=handle_client,
                args=(client_socket, client_address),
                daemon=True,
            )
            client_thread.start()

    except KeyboardInterrupt:
        print("\nServer shutting down.")
        
    except OSError as error:
        print(f"Socket error: {error}")

    finally:
        server_socket.close()
        print("Server socket closed.")


if __name__ == "__main__":
    start_server()