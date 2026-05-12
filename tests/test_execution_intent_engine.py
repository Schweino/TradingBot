import unittest

import execution_action_engine
import execution_intent_engine
import execution_kernel


class ExecutionIntentEngineTests(unittest.TestCase):
    def _entry_plan(self):
        return execution_action_engine.plan_entry(
            {"positions": {}, "pending_entries": {}, "trades": []},
            {
                "ticker": "CLSK",
                "side": "LONG",
                "price": 10.0,
                "setup_type": "btc_relative_strength",
                "opportunity_id": "opp1",
            },
            1778250600,
            execution_kernel.contract_from_config({}),
            entry_price=10.0,
            brackets={"side": "LONG", "entry_price": 10.0, "tp": 10.04, "sl": 9.96},
            qty=100,
            alloc=1000,
            trade_id="trade1",
        )

    def test_entry_intent_and_result_validate(self):
        plan = self._entry_plan()
        intent = execution_intent_engine.entry_intent(
            plan,
            venue="step2_sim",
            qty=100,
            trade_id="trade1",
            entry_price=10.0,
            tp_price=10.04,
            sl_price=9.96,
            order_kind="simulated_entry",
            ts=1778250600,
        )
        result = execution_intent_engine.simulated_entry_result(intent, ts=1778250600)

        self.assertEqual(execution_intent_engine.validate_intent(intent), [])
        self.assertEqual(execution_intent_engine.validate_result(result), [])
        self.assertEqual(intent["order"]["broker_side"], "buy")
        self.assertEqual(result["fill"]["price"], 10.0)

    def test_semantic_hash_ignores_transport_details(self):
        plan = self._entry_plan()
        step2_intent = execution_intent_engine.entry_intent(
            plan,
            venue="step2_sim",
            qty=100,
            trade_id="step2-trade",
            order_kind="simulated_entry",
            ts=1778250600,
        )
        live_intent = execution_intent_engine.entry_intent(
            plan,
            venue="alpaca",
            qty=100,
            trade_id="live-trade",
            client_order_id="client-live",
            order_kind="bracket_entry",
            ts=1778250600,
        )

        self.assertNotEqual(step2_intent["execution_intent_hash"], live_intent["execution_intent_hash"])
        self.assertEqual(
            step2_intent["semantic_execution_intent_hash"],
            live_intent["semantic_execution_intent_hash"],
        )

    def test_short_exit_broker_side_is_buy(self):
        position = {
            "ticker": "MARA",
            "side": "SHORT",
            "entry": 20.0,
            "entry_ts": 100,
            "tp": 19.92,
            "sl": 20.08,
            "trade_id": "trade2",
        }
        plan = execution_action_engine.plan_exit(
            position,
            19.9,
            130,
            True,
            execution_kernel.contract_from_config({}),
        )
        intent = execution_intent_engine.exit_intent(
            plan,
            venue="step2_sim",
            qty=50,
            trade_id="trade2",
            exit_price=19.92,
            reason="take_profit",
            ts=130,
        )

        self.assertEqual(execution_intent_engine.validate_intent(intent), [])
        self.assertEqual(intent["order"]["broker_side"], "buy")


if __name__ == "__main__":
    unittest.main()
