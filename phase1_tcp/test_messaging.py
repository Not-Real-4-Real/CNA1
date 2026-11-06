
import socket
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

from CNA1.phase1_tcp.protocol import MessageType, send_frame, recv_frame


HOST = "127.0.0.1"
PORT = 5000
SOCKET_TIMEOUT = 10

# Increase payload size to make concurrent frame-write races
# easier to expose than with tiny messages.
MESSAGES_PER_SENDER = 20
MESSAGE_SIZE = 16 * 1024


class TestClient:
    def __init__(self):
        self.sock = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM,
        )
        self.sock.settimeout(SOCKET_TIMEOUT)
        self.sock.connect((HOST, PORT))

        self.next_req_id = 2
        self.sock_id = uuid.uuid4().hex[:8]

        send_frame(
            self.sock,
            MessageType.HELLO,
            req_id=1,
            payload={"client": "automated-test"},
        )

        header, payload = recv_frame(self.sock)

        if (
            header.type != MessageType.WELCOME
            or header.req_id != 1
        ):
            self.sock.close()
            raise RuntimeError(
                f"Handshake failed: {header.type}, {payload}"
            )

        self.username = payload["username"]
        print(f"[CONNECTED] {self.username}")

    def request(self, msg_type, payload):
        req_id = self.next_req_id
        self.next_req_id += 1

        send_frame(
            self.sock,
            msg_type,
            req_id=req_id,
            payload=payload,
        )

        # Membership notifications and delivered messages are
        # unsolicited events. Skip them until the matching
        # response arrives.
        while True:
            header, response = recv_frame(self.sock)

            if header.req_id == 0:
                continue

            if header.req_id != req_id:
                raise AssertionError(
                    f"Expected response ID {req_id}, "
                    f"received {header.req_id}"
                )

            return header, response

    def close(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

        self.sock.close()


def connect_clients(count):
    return [TestClient() for _ in range(count)]



def test_concurrent_broadcasts():
    print("\n=== TEST A: Concurrent broadcasts ===")

    sender_a, sender_b, receiver = connect_clients(3)

    token = uuid.uuid4().hex
    expected = set()

    messages_a = []
    messages_b = []

    for i in range(MESSAGES_PER_SENDER):
        message = f"{token}-A-{i}" + ("A" * MESSAGE_SIZE)
        messages_a.append(message)
        expected.add((sender_a.username, message))

    for i in range(MESSAGES_PER_SENDER):
        message = f"{token}-B-{i}" + ("B" * MESSAGE_SIZE)
        messages_b.append(message)
        expected.add((sender_b.username, message))

    # The receiver must drain incoming frames while senders are active.
    received = []
    receiver_errors = []
    receiver_done = threading.Event()

    def receive_deliveries():
        try:
            for _ in range(len(expected)):
                header, payload = recv_frame(receiver.sock)

                assert header.type == MessageType.MSG_DELIVER, (
                    f"Unexpected frame type: {header.type}"
                )
                assert header.req_id == 0, (
                    f"Expected event ID 0, got {header.req_id}"
                )

                received.append(
                    (payload.get("from"), payload.get("message"))
                )

        except Exception as exc:
            receiver_errors.append(exc)
        finally:
            receiver_done.set()

    receiver_thread = threading.Thread(
        target=receive_deliveries,
        daemon=True,
    )

    # Allow extra time for this stress test.
    for client in (sender_a, sender_b, receiver):
        client.sock.settimeout(30)

    receiver_thread.start()

    barrier = threading.Barrier(2)

    def send_many(client, messages):
        barrier.wait()

        for index, message in enumerate(messages):
            header, response = client.request(
                MessageType.MSG_BROADCAST,
                {"message": message},
            )

            assert header.type == MessageType.MSG_SENT, (
                f"{client.username}, message {index}: "
                f"unexpected response {header.type}: {response}"
            )

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            future_a = pool.submit(
                send_many, sender_a, messages_a
            )
            future_b = pool.submit(
                send_many, sender_b, messages_b
            )

            # Propagate any sender-side exceptions.
            future_a.result()
            future_b.result()

        # The receiver should have consumed all deliveries already.
        receiver_thread.join(timeout=35)

        assert not receiver_thread.is_alive(), (
            "Receiver did not finish reading all expected deliveries"
        )

        if receiver_errors:
            raise receiver_errors[0]

        received_set = set(received)

        assert len(received) == len(expected), (
            f"Expected {len(expected)} frames, got {len(received)}"
        )
        assert len(received_set) == len(received), (
            "Duplicate deliveries detected"
        )
        assert received_set == expected, (
            "Delivered messages do not match the sent messages"
        )

        print(f"[PASS] Received {len(received)} intact messages.")
        print("[PASS] No missing, duplicate, or corrupted payloads.")
        print("[PASS] Both senders completed concurrent broadcasts.")

    finally:
        sender_a.close()
        sender_b.close()
        receiver.close()


def test_disconnected_recipient():
    print("\n=== TEST B: Recipient disconnected ===")

    sender, recipient = connect_clients(2)
    recipient_name = recipient.username

    try:
        recipient.close()

        # Allow the server's handler to observe EOF and unregister
        # the recipient before testing the missing-recipient case.
        # This is a state-convergence wait, not a fixed sleep.
        import time
        deadline = time.monotonic() + 5

        while time.monotonic() < deadline:

            # This server version does not implement USER_LIST yet.
            # Use a short retry loop with a direct-message request
            # instead; a 404 confirms the registry has been updated.
            try:
                header, payload = sender.request(
                    MessageType.MSG_DIRECT,
                    {
                        "to": recipient_name,
                        "message": "disconnect-test",
                    },
                )
            except (OSError, ConnectionError, ValueError):
                raise

            if (
                header.type == MessageType.ERROR
                and payload.get("code") == 404
            ):
                print("[PASS] Disconnected recipient returns 404.")
                return

            if header.type == MessageType.MSG_SENT:
                # The send won the race before unregister completed.
                continue

            raise AssertionError(
                f"Unexpected response: {header.type}, {payload}"
            )

        raise AssertionError(
            "Recipient did not become unavailable within 5 seconds"
        )

    finally:
        sender.close()
        recipient.close()


def test_disconnect_race():
    print("\n=== TEST C: Disconnect race ===")

    sender = TestClient()

    try:
        for attempt in range(5):
            recipient = TestClient()
            recipient_name = recipient.username
            barrier = threading.Barrier(2)

            def disconnect_recipient():
                barrier.wait()
                recipient.close()

            def send_during_disconnect():
                barrier.wait()
                return sender.request(
                    MessageType.MSG_DIRECT,
                    {
                        "to": recipient_name,
                        "message": f"race-test-{attempt}",
                    },
                )

            try:
                with ThreadPoolExecutor(max_workers=2) as pool:
                    close_future = pool.submit(disconnect_recipient)
                    send_future = pool.submit(send_during_disconnect)

                    close_future.result()
                    header, payload = send_future.result()

                # Both outcomes are valid. The server might send the
                # frame before it notices the disconnect, or it might
                # detect that the recipient is unavailable.
                assert header.type in (
                    MessageType.MSG_SENT,
                    MessageType.ERROR,
                ), (header.type, payload)

                if header.type == MessageType.ERROR:
                    assert payload.get("code") in (404, 503), payload

                print(
                    f"[RACE {attempt + 1}] "
                    f"Response type={header.type}, payload={payload}"
                )

            finally:
                recipient.close()

        # Verify the server is still serving requests after the races.
        header, payload = sender.request(
            MessageType.PING,
            {"message": "server-health-check"},
        )

        assert header.type == MessageType.PONG, (
            f"Server health check failed: {header.type}, {payload}"
        )

        print("[PASS] Server remained responsive after disconnect races.")

    finally:
        sender.close()


if __name__ == "__main__":
    test_concurrent_broadcasts()
    test_disconnected_recipient()
    test_disconnect_race()

    print("\nAll messaging tests passed.")