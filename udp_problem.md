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