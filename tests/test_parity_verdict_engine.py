import unittest

import parity_verdict_engine


def _packet(join_key="k1", source="live", intent_hash="intent", action_hash="action"):
    return {
        "schema_version": 1,
        "packet_type": "canonical_decision_packet",
        "source_system": source,
        "stage": "entry",
        "day": "2026-05-08",
        "created_at": 1778250600,
        "created_at_ct": "2026-05-08T08:30:00-05:00",
        "join_key": join_key,
        "identity": {
            "ticker": "CLSK",
            "side": "LONG",
            "setup_type": "btc_relative_strength",
            "timestamp_second": 1778250600,
        },
        "decision": "entered",
        "reason": "entered",
        "features": {
            "feature_snapshot_hash": "feature",
            "score_breakdown": {"score": 6.5},
        },
        "brackets": {
            "entry_price": 10.0,
            "tp_price": 10.04,
            "sl_price": 9.96,
        },
        "state": {
            "execution_kernel_hash": "kernel",
            "step2_parity_contract_hash": "parity",
            "step2_execution_contract_hash": "execution",
        },
        "profile": {"hash": "profile"},
        "action_plan": {"semantic_action_hash": action_hash},
        "execution": {"semantic_execution_intent_hash": intent_hash},
    }


def _lifecycle_rows(include_exit_result=True):
    events = [
        "entry_intent_created",
        "entry_execution_result",
        "entry_committed",
        "exit_intent_created",
        "position_closed",
    ]
    if include_exit_result:
        events.insert(4, "exit_execution_result")
    rows = []
    for idx, event in enumerate(events):
        rows.append({
            "event": event,
            "stage": "closed" if event == "position_closed" else event,
            "trade_id": "trade1",
            "symbol": "CLSK",
            "ts": 1778250600 + idx,
        })
    return rows


class ParityVerdictEngineTests(unittest.TestCase):
    def test_matching_packets_and_lifecycle_are_promotion_safe(self):
        payload = parity_verdict_engine.verdict_from_components(
            "2026-05-08",
            [_packet(source="live")],
            [_packet(source="step2")],
            lifecycle_rows=_lifecycle_rows(),
            lifecycle_summary={"anomalies": [], "open_trade_count": 0},
            schema_compatibility={"ok": True},
            canonical_diff={"ok": True, "mismatch_count": 0},
        )

        self.assertEqual(payload["verdict"], "PARITY_OK")
        self.assertTrue(payload["promotion_safe"])

    def test_execution_intent_mismatch_blocks_promotion(self):
        payload = parity_verdict_engine.verdict_from_components(
            "2026-05-08",
            [_packet(source="live", intent_hash="live-intent")],
            [_packet(source="step2", intent_hash="step2-intent")],
            lifecycle_rows=_lifecycle_rows(),
            lifecycle_summary={"anomalies": [], "open_trade_count": 0},
            schema_compatibility={"ok": True},
            canonical_diff={"ok": True, "mismatch_count": 1},
        )

        self.assertEqual(payload["verdict"], "PARITY_FAIL")
        self.assertFalse(payload["promotion_safe"])
        self.assertIn("entry_field_mismatch", payload["issue_kind_counts"])

    def test_missing_exit_lifecycle_event_blocks_promotion(self):
        payload = parity_verdict_engine.verdict_from_components(
            "2026-05-08",
            [_packet(source="live")],
            [_packet(source="step2")],
            lifecycle_rows=_lifecycle_rows(include_exit_result=False),
            lifecycle_summary={"anomalies": [], "open_trade_count": 0},
            schema_compatibility={"ok": True},
            canonical_diff={"ok": True, "mismatch_count": 0},
        )

        self.assertEqual(payload["verdict"], "PARITY_FAIL")
        self.assertIn("lifecycle_missing_exit_event", payload["issue_kind_counts"])


if __name__ == "__main__":
    unittest.main()
