import unittest

import canonical_decision_packet


class CanonicalDecisionPacketTests(unittest.TestCase):
    def test_live_and_step2_rows_share_packet_shape(self):
        base_feature = {
            "score": 6.5,
            "conviction": "HIGH",
            "signal_quality": {"score_components": {"btc": 1.0}},
        }
        live = canonical_decision_packet.from_live_signal_parity({
            "created_at": 1778250600,
            "created_at_ct": "2026-05-08T08:30:00-05:00",
            "parity_key": "abc",
            "ticker": "CLSK",
            "side": "LONG",
            "setup_type": "btc_relative_strength",
            "decision": "entered",
            "reason": "entered",
            "price": 10.0,
            "score": 6.5,
            "conviction": "HIGH",
            "feature_snapshot_hash": "fh",
            "feature_snapshot": base_feature,
            "extra": {"tp": 10.04, "sl": 9.96, "qty": 100, "alloc": 1000},
        })
        step2 = canonical_decision_packet.from_step2_decision({
            "day": "2026-05-08",
            "created_at": 1778250600,
            "parity_key": "abc",
            "ticker": "CLSK",
            "side": "LONG",
            "setup_type": "btc_relative_strength",
            "decision": "entered",
            "reason": "entered",
            "price": 10.0,
            "score": 6.5,
            "conviction": "HIGH",
            "feature_snapshot_hash": "fh",
            "feature_snapshot": base_feature,
            "outcome_summary": {"entry": 10.0, "tp": 10.04, "sl": 9.96, "pnl": 4.0},
        })

        self.assertEqual(live["join_key"], step2["join_key"])
        self.assertEqual(live["brackets"]["tp_price"], step2["brackets"]["tp_price"])
        self.assertTrue(live["packet_hash"])
        self.assertEqual(canonical_decision_packet.validate_packet(live), [])


if __name__ == "__main__":
    unittest.main()
