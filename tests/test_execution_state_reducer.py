import unittest

import execution_adapters
import execution_kernel
import execution_state_reducer


class ExecutionStateReducerTests(unittest.TestCase):
    def test_reducer_blocks_same_ticker_until_close_plus_cooldown(self):
        runtime = {'positions': {}, 'pending_entries': {}, 'trades': []}
        adapter = execution_adapters.ReplayExecutionAdapter(day='2026-05-08', runtime_state=runtime)
        contract = execution_kernel.contract_from_config({
            'trade_size_pct': 0.25,
            'step2': {'same_ticker_reentry_cooldown_sec': 5},
        })
        adapter.commit_entry('CLSK', 't1', 'LONG', 10.0, qty=1, alloc=10, tp=10.04, sl=9.96, ts=100)
        self.assertEqual(adapter.entry_block_reason('CLSK', 101, contract), 'already_in_position')
        adapter.close_position('CLSK', 't1', 'LONG', 'take_profit', 10.04, pnl=0.04, ts=120)
        reason = adapter.entry_block_reason('CLSK', 124, contract)
        self.assertTrue(reason.startswith('step2_reentry_cooldown:CLSK'))
        self.assertIsNone(adapter.entry_block_reason('CLSK', 125, contract))

    def test_runtime_view_comparison_catches_drift(self):
        runtime = {'positions': {'CLSK': {'trade_id': 't1'}}, 'pending_entries': {}, 'trades': []}
        execution_state_reducer.bootstrap_runtime_state(runtime, day='2026-05-08')
        self.assertTrue(execution_state_reducer.compare_runtime_view(runtime)['ok'])
        runtime['positions']['MARA'] = {'trade_id': 't2'}
        out = execution_state_reducer.compare_runtime_view(runtime)
        self.assertFalse(out['ok'])
        self.assertEqual(out['mismatches'][0]['runtime_only'], ['MARA'])


if __name__ == '__main__':
    unittest.main()
