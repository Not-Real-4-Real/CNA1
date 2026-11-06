### Known limitation: no persistent UDP receiver

**Observed behavior:** When the client receives both the ACK for its outgoing PING and the corresponding PONG, `ping()` returns. If the ACK sent for that PONG is lost, the server retransmits the PONG, but the client no longer receives and acknowledges it. The server eventually exhausts its retransmission limit and invalidates the session.

**Root cause:** Incoming datagram processing is coupled to the lifetime of the `ping()` transaction. There is no persistent receive loop to handle retransmitted DATA or unsolicited application messages after the transaction completes.

**Planned solution — Step 5:** Introduce a dedicated receiver loop that owns socket reception and dispatches incoming datagrams to the transport layer or the appropriate application handler.

The transport layer must continue to:
- ACK valid new DATA packets.
- Re-ACK duplicate DATA packets without delivering them to the application twice.
- Process ACKs against the correct outstanding transmission.
- Maintain sequence-number and retransmission state independently of application request lifetimes.

The application layer will use the receiver's dispatch mechanism for unsolicited events, including peer-list updates and messaging.

**Current status:** Single-loss recovery for a lost server ACK has passed. Recovery from a lost client ACK for the PONG has exposed the expected limitation of the current receive-loop architecture. Persistent receive handling is deferred to Step 5.

**Showcase:**

[DATAGRAM RECEIVED] ('127.0.0.1', 5001) | bytes=54
[ACK SENT] ('127.0.0.1', 5001) | ack_num=0
[DATA ACCEPTED] ('127.0.0.1', 5001) | seq=0 | req_id=1 | payload={'message': 'ping'}
[PING RECEIVED] ('127.0.0.1', 5001) | seq=0 | req_id=1 | payload={'message': 'ping'}
[PONG SENT] ('127.0.0.1', 5001) | seq=0 | req_id=1 | awaiting ACK
[RETRANSMIT] ('127.0.0.1', 5001) | seq=0 | retry=1/5
[RETRANSMIT] ('127.0.0.1', 5001) | seq=0 | retry=2/5
[RETRANSMIT] ('127.0.0.1', 5001) | seq=0 | retry=3/5
[RETRANSMIT] ('127.0.0.1', 5001) | seq=0 | retry=4/5
[RETRANSMIT] ('127.0.0.1', 5001) | seq=0 | retry=5/5
[TRANSMISSION FAILED] ('127.0.0.1', 5001) | seq=0 | retries=5 | invalidating session

[PROXY] Forwarding to ('127.0.0.1', 5000)
[PROXY] Loss mode: client_ack
[FORWARD] client -> server | RDT type=1
[FORWARD] server -> client | RDT type=2
[FORWARD] client -> server | RDT type=3
[FORWARD] server -> client | RDT type=4
[FORWARD] server -> client | RDT type=3
[DROP] client -> server | ACK=0
[FORWARD] server -> client | RDT type=3
[FORWARD] server -> client | RDT type=3
[FORWARD] server -> client | RDT type=3
[FORWARD] server -> client | RDT type=3
[FORWARD] server -> client | RDT type=3

Enter a message to ping (or 'quit' to disconnect): ping
[PING SENT] seq=0 | req_id=1 | payload={'message': 'ping'}
[ACK ACCEPTED] ack_num=0
[ACK SENT] ack_num=0
[PONG RECEIVED] seq=0 | req_id=1 | payload={'message': 'Pong from server', 'echo': 'ping'}
[PING COMPLETE] req_id=1
