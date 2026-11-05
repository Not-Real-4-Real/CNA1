import unittest
from udp_protocol import (
    RDTHeader, RDTType, Datagram, encode_datagram, decode_datagram,
    RDT_HEADER_SIZE, APP_HEADER_SIZE, MAX_DATAGRAM_SIZE,
    SR_WINDOW_SIZE, SEQ_MODULUS, seq_add, seq_distance,
    seq_in_window, seq_is_recent_duplicate,
)


class SelectiveRepeatProtocolTests(unittest.TestCase):
    def test_header_size_and_payload_budget(self):
        self.assertEqual(RDT_HEADER_SIZE, 27)
        self.assertEqual(APP_HEADER_SIZE, 10)
        self.assertEqual(MAX_DATAGRAM_SIZE - RDT_HEADER_SIZE - APP_HEADER_SIZE, 1163)

    def test_control_packet_window_types_round_trip(self):
        session_id = b"0123456789abcdef"
        for packet_type in (RDTType.WINDOW_UPDATE, RDTType.WINDOW_PROBE):
            packet = encode_datagram(
                RDTHeader(session_id=session_id, rwnd=17, type=packet_type)
            )
            decoded = decode_datagram(packet)
            self.assertEqual(decoded.rdt.type, packet_type)
            self.assertEqual(decoded.rdt.rwnd, 17)

    def test_sequence_arithmetic_wraparound(self):
        self.assertEqual(seq_add(SEQ_MODULUS - 1), 0)
        self.assertEqual(seq_distance(0, SEQ_MODULUS - 2), 2)

    def test_receive_window_membership(self):
        base = 100
        self.assertTrue(seq_in_window(100, base, SR_WINDOW_SIZE))
        self.assertTrue(seq_in_window(107, base, SR_WINDOW_SIZE))
        self.assertFalse(seq_in_window(108, base, SR_WINDOW_SIZE))

    def test_recent_duplicate_detection(self):
        base = 108
        self.assertTrue(seq_is_recent_duplicate(107, base, SR_WINDOW_SIZE))
        self.assertTrue(seq_is_recent_duplicate(100, base, SR_WINDOW_SIZE))
        self.assertFalse(seq_is_recent_duplicate(99, base, SR_WINDOW_SIZE))

    def test_window_membership_across_wraparound(self):
        base = SEQ_MODULUS - 3
        self.assertTrue(seq_in_window(SEQ_MODULUS - 1, base, SR_WINDOW_SIZE))
        self.assertTrue(seq_in_window(0, base, SR_WINDOW_SIZE))
        self.assertTrue(seq_in_window(4, base, SR_WINDOW_SIZE))
        self.assertFalse(seq_in_window(5, base, SR_WINDOW_SIZE))


if __name__ == "__main__":
    unittest.main(verbosity=2)
