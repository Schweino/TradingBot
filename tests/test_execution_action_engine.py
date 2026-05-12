import unittest

import execution_action_engine
import execution_adapters
import execution_kernel


class ExecutionActionEngineTests(unittest.TestCase):
    def test_entry_plan_blocks_same_ticker_cooldown(self):
        runtime = {'positions': {}, 'pending_entries': {}, 'trades': []}
        adapter = execution_adapters.ReplayExecutionAdapter(day='2026-05-08', runtime_state=runtime)
        contract = execution_kernel.contract_from_config({
            'trade_size_pct': 0.25,
            'step2': {'same_ticker_reentry_cooldown_sec': 5},
        })
        adapter.commit_entry('CLSK', 't1', 'LONG', 10.0, qty=1, alloc=10, tp=10.04, sl=9.96, ts=100)
        adapter.close_position('CLSK', 't1', 'LONG', 'take_profit', 10.04, pnl=0.04, ts=120)
        plan = execution_action_engine.plan_entry(
            runtime,
            {'ticker': 'CLSK', 'side': 'LONG', 'price': 10.05, 'setup_type': 'x'},
            124,
            contract,
            entry_price=10.05,
            sl_pct=0.004,
            tp_pct=0.004,
        )
        self.assertEqual(plan['action'], 'skip')
        self.assertTrue(plan['reason'].startswith('step2_reentry_cooldown:CLSK'))
        self.assertIsNotNone(plan['action_plan_hash'])
        self.assertIsNotNone(plan['semantic_action_hash'])

    def test_entry_plan_brackets_are_side_aware(self):
        plan = execution_action_engine.plan_entry(
            {'positions': {}, 'pending_entries': {}, 'trades': []},
            {'ticker': 'MARA', 'side': 'SHORT', 'price': 20.0, 'setup_type': 'x'},
            100,
            execution_kernel.contract_from_config({}),
            entry_price=20.0,
            sl_pct=0.004,
            tp_pct=0.004,
        )
        self.assertEqual(plan['action'], 'enter')
        self.assertEqual(plan['brackets']['tp_price'], 19.92)
        self.assertEqual(plan['brackets']['sl_price'], 20.08)

    def test_exit_plan_uses_same_strict_bracket_rules(self):
        position = {
            'ticker': 'RIOT',
            'side': 'LONG',
            'entry': 25.0,
            'entry_ts': 100,
            'tp': 25.1,
            'sl': 24.9,
            'trade_id': 't2',
        }
        plan = execution_action_engine.plan_exit(
            position,
            25.11,
            130,
            True,
            execution_kernel.contract_from_config({}),
        )
        self.assertEqual(plan['action'], 'exit')
        self.assertEqual(plan['reason'], 'take_profit')
        self.assertEqual(plan['market']['exit_price'], 25.1)


if __name__ == '__main__':
    unittest.main()
