import socket
import threading

from protocol import MessageType, send_frame, recv_frame

HOST = "127.0.0.1"
PORT = 5000

clients_lock = threading.Lock()
active_client = 0


def handle_client(client_socket, client_address):
    "Thread to handle 1 client"

    global next_username_id

    with clients_lock:
        active_clients += 1
        client_count = active_clients

    print(
        f"[CONNECTED] {client_address} | "
        f"Active clients: {client_count}"
    )

    try:  
        with client_socket:
            # Step 1: Perform the initial handshake.
            header, payload = recv_frame(client_socket)
        
            print(f"Received type: {header.type}")
            print(f"Request ID: {header.req_id}")
            print(f"Payload length: {header.length}")
            print(f"Payload: {payload}")
        
            # Send the application-level acknowledgement.
            send_frame(
                client_socket,
                MessageType.WELCOME,
                header.req_id,
                {"message": "Connection established"}
            )
        
            print(f"[HANDSHAKE] {client_address} completed")

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
        with clients_lock:
            active_clients -= 1
            client_count = active_clients

        print(
            f"[DISCONNECTED] {client_address} | "
            f"Active clients: {client_count}"
        )  


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