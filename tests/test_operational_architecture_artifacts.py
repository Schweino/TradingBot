import unittest

import broker_lifecycle_guard
import daily_parity_scorecard
import live_market_data_watchdog
import market_data_freshness_guard
import order_lifecycle_reconciliation
from datetime import datetime
from zoneinfo import ZoneInfo


class OperationalArchitectureArtifactTests(unittest.TestCase):
    def test_live_market_data_watchdog_fails_active_empty_tape(self):
        payload = live_market_data_watchdog.evaluate(
            "2099-05-08",
            now=datetime(2099, 5, 8, 9, 0, tzinfo=ZoneInfo("America/Chicago")),
        )

        self.assertFalse(payload["ok"])
        self.assertEqual("LIVE_DATA_FAIL", payload["verdict"])
        kinds = {row["kind"] for row in payload["issues"]}
        self.assertIn("live_tape_missing", kinds)

    def test_order_lifecycle_accepts_complete_chain(self):
        lifecycle_rows = [
            {"event": "entry_intent_created", "stage": "intent_created", "trade_id": "T1", "symbol": "CLSK", "ts": 1},
            {"event": "entry_execution_result", "stage": "execution_result", "trade_id": "T1", "symbol": "CLSK", "ts": 2},
            {"event": "entry_committed", "stage": "opened", "trade_id": "T1", "symbol": "CLSK", "ts": 3},
            {"event": "exit_intent_created", "stage": "intent_created", "trade_id": "T1", "symbol": "CLSK", "ts": 4},
            {"event": "exit_execution_result", "stage": "execution_result", "trade_id": "T1", "symbol": "CLSK", "ts": 5},
            {"event": "position_closed", "stage": "closed", "trade_id": "T1", "symbol": "CLSK", "ts": 6},
        ]
        payload = order_lifecycle_reconciliation.evaluate(
            "2026-05-08",
            lifecycle_rows=lifecycle_rows,
            signal_rows=[{"decision": "entered", "trade_id": "T1"}],
            trade_rows=[{"trade_id": "T1", "pnl": 10.0}],
            latency_rows=[{"trade_id": "T1"}],
            lifecycle_summary={"anomalies": []},
        )

        self.assertTrue(payload["ok"])
        self.assertEqual("LIFECYCLE_OK", payload["verdict"])

    def test_order_lifecycle_flags_closed_trade_without_commit(self):
        payload = order_lifecycle_reconciliation.evaluate(
            "2026-05-08",
            lifecycle_rows=[{"event": "position_closed", "stage": "closed", "trade_id": "T1", "symbol": "CLSK", "ts": 1}],
            signal_rows=[{"decision": "entered", "trade_id": "T1"}],
            trade_rows=[{"trade_id": "T1", "pnl": -5.0}],
            latency_rows=[],
            lifecycle_summary={"anomalies": []},
        )

        self.assertFalse(payload["ok"])
        kinds = {row["kind"] for row in payload["issues"]}
        self.assertIn("closed_trade_without_entry_commit", kinds)

    def test_order_lifecycle_flags_duplicate_broker_identity(self):
        lifecycle_rows = [
            {"event": "entry_intent_created", "stage": "intent_created", "trade_id": "T1", "symbol": "CLSK", "client_order_id": "cid-1", "broker_order_id": "oid-1", "ts": 1},
            {"event": "entry_committed", "stage": "opened", "trade_id": "T1", "symbol": "CLSK", "client_order_id": "cid-1", "broker_order_id": "oid-1", "ts": 2},
            {"event": "position_closed", "stage": "closed", "trade_id": "T1", "symbol": "CLSK", "client_order_id": "cid-1", "broker_order_id": "oid-1", "ts": 3},
            {"event": "entry_intent_created", "stage": "intent_created", "trade_id": "T2", "symbol": "MARA", "client_order_id": "cid-2", "broker_order_id": "oid-1", "ts": 4},
            {"event": "entry_committed", "stage": "opened", "trade_id": "T2", "symbol": "MARA", "client_order_id": "cid-2", "broker_order_id": "oid-1", "ts": 5},
            {"event": "position_closed", "stage": "closed", "trade_id": "T2", "symbol": "MARA", "client_order_id": "cid-2", "broker_order_id": "oid-1", "ts": 6},
        ]
        payload = order_lifecycle_reconciliation.evaluate(
            "2026-05-08",
            lifecycle_rows=lifecycle_rows,
            signal_rows=[{"decision": "entered", "trade_id": "T1"}, {"decision": "entered", "trade_id": "T2"}],
            trade_rows=[{"trade_id": "T1"}, {"trade_id": "T2"}],
            latency_rows=[{"trade_id": "T1"}, {"trade_id": "T2"}],
            lifecycle_summary={"anomalies": []},
        )

        self.assertFalse(payload["ok"])
        kinds = {row["kind"] for row in payload["issues"]}
        self.assertIn("duplicate_order_identity", kinds)
        self.assertEqual(1, payload["scorecard"]["duplicate_identity_count"])

    def test_broker_lifecycle_guard_blocks_unmapped_order(self):
        payload = broker_lifecycle_guard.evaluate_runtime_broker_state(
            {"positions": {}, "pending_entries": {}},
            broker_positions=[],
            broker_orders=[{"symbol": "CLSK", "id": "oid-1", "client_order_id": "cid-1", "status": "new"}],
            watched=["CLSK", "MARA", "RIOT"],
            broker_reachable=True,
        )

        self.assertFalse(payload["ok"])
        kinds = {row["kind"] for row in payload["issues"]}
        self.assertIn("broker_open_order_unmapped", kinds)

    def test_broker_lifecycle_guard_accepts_pending_order_match(self):
        payload = broker_lifecycle_guard.evaluate_runtime_broker_state(
            {
                "positions": {},
                "pending_entries": {
                    "CLSK": {"trade_id": "T1", "client_order_id": "cid-1", "alpaca_order_id": "oid-1"}
                },
            },
            broker_positions=[],
            broker_orders=[{"symbol": "CLSK", "id": "oid-1", "client_order_id": "cid-1", "status": "new"}],
            watched=["CLSK", "MARA", "RIOT"],
            broker_reachable=True,
        )

        self.assertTrue(payload["ok"])
        self.assertEqual("BROKER_LIFECYCLE_OK", payload["verdict"])

    def test_market_data_freshness_flags_closed_day_missing_intraday_rows(self):
        payload = market_data_freshness_guard.evaluate_components(
            "2026-05-08",
            integrity={"ok": True, "verdict": "DATA_OK"},
            compare={"missing_from_intraday_rows": 10, "live_only_rows": 0},
            selected_tape_meta={"exists": True, "path": "C:/fake/tape.gz", "age_sec": 1},
            incremental_manifest={"ok": True},
            duplicate_stats={"duplicate_event_keys": 0},
            intraday=False,
        )

        self.assertFalse(payload["ok"])
        self.assertEqual("FRESHNESS_FAIL", payload["verdict"])

    def test_market_data_freshness_warns_for_canonical_source_intraday_gap(self):
        payload = market_data_freshness_guard.evaluate_components(
            "2026-05-08",
            integrity={"ok": True, "verdict": "DATA_OK"},
            compare={"missing_from_intraday_rows": 10, "live_only_rows": 0},
            selected_tape_meta={"exists": True, "path": "C:/fake/tape.gz", "age_sec": 1},
            incremental_manifest={"ok": True},
            duplicate_stats={"duplicate_event_keys": 0},
            intraday=False,
            source_used="canonical",
        )

        self.assertTrue(payload["ok"])
        self.assertEqual("FRESHNESS_WARN", payload["verdict"])

    def test_daily_scorecard_buckets_gaps(self):
        report = {
            "summary": {
                "trade_rows": 9,
                "decision_mismatch_count": 0,
                "outcome_mismatch_count": 2,
                "entered_without_trade_row": 1,
                "trades_without_entered_signal_row": 0,
                "trades_without_latency_row": 0,
                "step2_only_signal_keys": 3,
                "live_only_signal_keys": 0,
                "market_data_integrity_critical_count": 0,
            },
            "live_trade_results": {"pnl": 100.0, "trades": 9, "wins": 7, "losses": 2},
            "step2_decision_parity": {"pnl": 130.0, "entered": 12},
            "trust_gate": {"contract_mismatches": []},
            "latency": {},
        }
        payload = daily_parity_scorecard.evaluate(
            "2026-05-08",
            report,
            lifecycle={"critical_count": 0, "warning_count": 0},
            freshness={"critical_count": 0, "warning_count": 0},
        )

        self.assertEqual(30.0, payload["pnl"]["step2_minus_live"])
        self.assertEqual(3, payload["trades"]["step2_minus_live"])
        bucket_names = {row["name"] for row in payload["root_cause_buckets"]}
        self.assertIn("outcome_mismatch", bucket_names)
        self.assertIn("step2_only_opportunities", bucket_names)


if __name__ == "__main__":
    unittest.main()
