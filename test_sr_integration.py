"""Component-level integration tests for the Selective Repeat client path.

Place this file in either:
    MMT/test_sr_integration.py
or:
    MMT/phase2_udp/tests/test_sr_integration.py

Run from the phase directory:
    python -m unittest -v test_sr_integration.py

Run from MMT when this file is under phase2_udp/tests:
    python -m unittest discover -s phase2_udp/tests -v

These tests exercise the real client.receive_data() and
client.check_retransmissions() functions. Network I/O is replaced at the
transport boundary so the tests are deterministic and do not require a
running server, Alice, or Bob.

This is not yet a full end-to-end UDP fault-proxy test. It verifies the
client's SR receive/retransmission behavior in isolation from the network.
"""

from __future__ import annotations

import queue
import sys
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


# Support both MMT/test_sr_integration.py and MMT/phase2_udp/tests/...
_THIS_DIR = Path(__file__).resolve().parent
if (_THIS_DIR / "client.py").is_file():
    _PROJECT_DIR = _THIS_DIR
elif (_THIS_DIR.parent / "client.py").is_file():
    _PROJECT_DIR = _THIS_DIR.parent
elif (_THIS_DIR.parent.parent / "client.py").is_file():
    _PROJECT_DIR = _THIS_DIR.parent.parent
else:
    raise RuntimeError(
        "Cannot find client.py. Put this test beside client.py or in "
        "phase2_udp/tests/."
    )

if str(_PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(_PROJECT_DIR))

import client  # noqa: E402  (project import after sys.path setup)
from udp_protocol import (  # noqa: E402
    AppHeader,
    RDTHeader,
    RDTType,
    decode_datagram,
    encode_datagram,
)


class SelectiveRepeatClientIntegrationTests(unittest.TestCase):
    """Exercise the production client SR functions with a fake transport."""

    SESSION_ID = b"sr-integrate-001"  # exactly 16 bytes

    def setUp(self) -> None:
        # Keep references to the original module state and restore it in
        # tearDown so these tests do not contaminate a later test or client run.
        self._old_incoming_data = client.incoming_data
        self._old_receive_buffer = client.receive_buffer
        self._old_receive_base = client.receive_base
        self._old_outstanding_packets = client.outstanding_packets
        self._old_acked_sequences = client.acked_sequences

        client.incoming_data = queue.Queue(maxsize=client.DATA_BUFFER_SIZE)
        client.receive_buffer = {}
        client.receive_base = 10
        client.outstanding_packets = {}
        client.acked_sequences = set()

    def tearDown(self) -> None:
        client.incoming_data = self._old_incoming_data
        client.receive_buffer = self._old_receive_buffer
        client.receive_base = self._old_receive_base
        client.outstanding_packets = self._old_outstanding_packets
        client.acked_sequences = self._old_acked_sequences

    @classmethod
    def make_data_datagram(cls, seq_num: int):
        """Build a valid DATA datagram using the project's real codec."""
        payload = f"payload-{seq_num}".encode("utf-8")
        packet = encode_datagram(
            rdt=RDTHeader(
                session_id=cls.SESSION_ID,
                seq_num=seq_num,
                rwnd=client.DATA_BUFFER_SIZE,
                type=RDTType.DATA,
            ),
            app=AppHeader(
                length=len(payload),
                type=1,
                req_id=seq_num,
            ),
            payload=payload,
        )
        return decode_datagram(packet)

    def test_receiver_buffers_out_of_order_and_delivers_in_order(self) -> None:
        """seq 12 is ACKed/buffered until seq 11 arrives after seq 10."""
        acknowledged = []

        with (
            patch.object(client, "send_ack", side_effect=acknowledged.append),
            patch.object(client, "update_peer_window"),
        ):
            client.receive_data(self.make_data_datagram(10))
            client.receive_data(self.make_data_datagram(12))

            # seq 12 must already have its own ACK, but cannot be delivered
            # to the application before the missing seq 11 arrives.
            self.assertEqual(acknowledged, [10, 12])
            self.assertEqual(client.incoming_data.qsize(), 1)
            self.assertIn(12, client.receive_buffer)
            self.assertEqual(client.receive_base, 11)

            client.receive_data(self.make_data_datagram(11))

        delivered = []
        while not client.incoming_data.empty():
            delivered.append(client.incoming_data.get_nowait().rdt.seq_num)

        self.assertEqual(acknowledged, [10, 12, 11])
        self.assertEqual(delivered, [10, 11, 12])
        self.assertEqual(client.receive_base, 13)
        self.assertEqual(client.receive_buffer, {})

    def test_duplicate_data_is_acked_but_not_delivered_twice(self) -> None:
        """A duplicate retransmission is ACKed again without duplicate data."""
        acknowledged = []

        with (
            patch.object(client, "send_ack", side_effect=acknowledged.append),
            patch.object(client, "update_peer_window"),
        ):
            original = self.make_data_datagram(10)
            duplicate = self.make_data_datagram(10)
            client.receive_data(original)
            client.receive_data(duplicate)

        self.assertEqual(acknowledged, [10, 10])
        self.assertEqual(client.incoming_data.qsize(), 1)
        self.assertEqual(
            client.incoming_data.get_nowait().rdt.seq_num,
            10,
        )
        self.assertTrue(client.incoming_data.empty())
        self.assertEqual(client.receive_base, 11)

    def test_sender_retransmits_only_the_expired_packet(self) -> None:
        """An expired packet is resent; a non-expired ACK-pending packet is not."""
        now = time.monotonic()
        lost_packet = SimpleNamespace(
            packet=b"wire-packet-seq-42",
            sent_at=now - client.RETRANSMISSION_TIMEOUT - 0.5,
            retransmissions=0,
            success=None,
            error=None,
            completed=threading.Event(),
        )
        not_expired_packet = SimpleNamespace(
            packet=b"wire-packet-seq-43",
            sent_at=now,
            retransmissions=0,
            success=None,
            error=None,
            completed=threading.Event(),
        )
        client.outstanding_packets = {
            42: lost_packet,
            43: not_expired_packet,
        }
        transmitted = []

        with patch.object(client, "transport_send", side_effect=transmitted.append):
            client.check_retransmissions()

        self.assertEqual(transmitted, [b"wire-packet-seq-42"])
        self.assertEqual(lost_packet.retransmissions, 1)
        self.assertEqual(not_expired_packet.retransmissions, 0)
        self.assertIn(42, client.outstanding_packets)
        self.assertIn(43, client.outstanding_packets)


if __name__ == "__main__":
    unittest.main(verbosity=2)
