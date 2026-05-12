import unittest

import live_state_rollover


class LiveStateRolloverTests(unittest.TestCase):
    def test_clears_transients_and_preserves_durable_trade_history(self):
        state = {
            "state_market_day": "2026-05-08",
            "balance": 100123.45,
            "start_balance": 100000.0,
            "positions": {},
            "pending_entries": {
                "CLSK": {"created_at": 1778267000},
            },
            "setup_pauses": {
                "CLSK:LONG:momentum": {
                    "created_at": 1778267000,
                    "until": 1778268000,
                }
            },
            "skipped_signals": [
                {"created_at": 1778267000, "ticker": "CLSK"},
                {"created_at": 1778527000, "ticker": "MARA"},
            ],
            "daily_budget_date": "2026-05-08",
            "daily_budget_cash": 99000,
            "daily_per_ticker_budget": 33000,
            "trades": [
                {"trade_id": "t1", "ticker": "CLSK", "closed_at": 1778268000, "pnl": 12.3}
            ],
        }

        proof = live_state_rollover.rollover_state(
            state,
            "2026-05-11",
            now_ts=1778527000,
            apply=True,
        )

        self.assertTrue(proof["ok"])
        self.assertEqual(state["state_market_day"], "2026-05-11")
        self.assertEqual(state["pending_entries"], {})
        self.assertEqual(state["setup_pauses"], {})
        self.assertNotIn("daily_budget_date", state)
        self.assertEqual(len(state["trades"]), 1)
        self.assertEqual(state["balance"], 100123.45)
        self.assertEqual(state["skipped_signals"], [{"created_at": 1778527000, "ticker": "MARA"}])

    def test_blocks_stale_pending_with_broker_identity(self):
        state = {
            "state_market_day": "2026-05-08",
            "positions": {},
            "pending_entries": {
                "MARA": {
                    "created_at": 1778267000,
                    "client_order_id": "scalp-MARA-1",
                }
            },
            "setup_pauses": {},
            "trades": [],
        }

        proof = live_state_rollover.rollover_state(
            state,
            "2026-05-11",
            now_ts=1778527000,
            apply=True,
        )

        self.assertFalse(proof["ok"])
        self.assertEqual(proof["blocked_stale_broker_pending_entries"], ["MARA"])
        self.assertIn("MARA", state["pending_entries"])


if __name__ == "__main__":
    unittest.main()
