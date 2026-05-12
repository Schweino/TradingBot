import time
import unittest
from unittest.mock import patch

import engine_validation
import ws_scalp


def base_ind(side='LONG'):
    bull = side == 'LONG'
    return {
        'ready': True,
        'symbol': 'CLSK',
        'price': 10.0,
        'bars': 600,
        'last_trade_age_sec': 0.5,
        'last_quote_age_sec': 0.5,
        'mom_5s': 0.08 if bull else -0.08,
        'mom_15s': 0.09 if bull else -0.09,
        'mom_30s': 0.12 if bull else -0.12,
        'mom_60s': 0.20 if bull else -0.20,
        'mom_180s': 0.20 if bull else -0.20,
        'ema_stack': 'bull' if bull else 'bear',
        'vwap_dist': 0.05 if bull else -0.05,
        'vwap_dist_sigma': 1.2 if bull else -1.2,
        'flow_30s': {'buy_pct': 55.0, 'ratio': 1.2},
        'flow_30s_prev': {'buy_pct': 50.0, 'ratio': 1.0},
        'flow_30s_delta': 5.0,
        'flow_120s': {'buy_pct': 55.0, 'ratio': 1.2},
        'vol_z_60s': 2.5,
        'tick_z_30s': 2.5,
        'realized_vol_60s_pct': 0.10,
        'session_range_pos': 0.50,
        'best_bid': 9.995,
        'best_ask': 10.005,
        'bid_size': 1000,
        'ask_size': 1000,
        'quote_imbalance': 0.0,
        'quote_imbalance_delta_5s': 0.0,
    }


def btc_ind(stack='bull', mom=0.20):
    return {
        'ready': True,
        'price': 100000.0,
        'last_trade_age_sec': 0.5,
        'last_quote_age_sec': 0.5,
        'mom_5s': mom / 4,
        'mom_15s': mom / 2,
        'mom_30s': mom,
        'mom_60s': mom,
        'mom_180s': mom,
        'ema_stack': stack,
    }


class EngineGuardTests(unittest.TestCase):
    def test_miners_require_btc_context(self):
        self.assertIsNone(ws_scalp.detect_signal('CLSK', base_ind('LONG'), None))

    def test_fresh_buying_pressure_does_not_score_short_by_itself(self):
        ind = base_ind('SHORT')
        ind['flow_30s'] = {'buy_pct': 70.0, 'ratio': 2.3}
        ind['flow_120s'] = {'buy_pct': 68.0, 'ratio': 2.1}
        ind['flow_30s_delta'] = -6.0
        sig = ws_scalp.detect_signal('CLSK', ind, btc_ind('bear', -0.20), miner_indicators={'CLSK': ind})
        self.assertIsNotNone(sig)
        comps = sig['signal_quality']['score_components']
        self.assertEqual(comps.get('flow_30s'), 0)
        self.assertEqual(comps.get('flow_120s'), 0)

    def test_score_model_separates_direction_regime_and_execution(self):
        sig = ws_scalp.detect_signal('CLSK', base_ind('LONG'), btc_ind('bull', 0.20),
                                     miner_indicators={'CLSK': base_ind('LONG')})
        self.assertIsNotNone(sig)
        model = sig['signal_quality']['score_model']
        self.assertIn('direction_score', model)
        self.assertIn('regime_score', model)
        self.assertIn('execution_score', model)
        self.assertIsNotNone(model['execution_score'])

    def test_flow_fade_requires_rollover_plus_micro_confirm(self):
        ind = base_ind('SHORT')
        ind['flow_30s'] = {'buy_pct': 70.0, 'ratio': 2.3}
        ind['flow_120s'] = {'buy_pct': 68.0, 'ratio': 2.1}
        ind['flow_30s_delta'] = -6.0
        ind['mom_5s'] = 0.01
        ind['quote_imbalance'] = 0.0
        ind['quote_imbalance_delta_5s'] = 0.0
        self.assertFalse(ws_scalp._flow_fade_confirmed('SHORT', ind))
        ind['mom_5s'] = -0.03
        self.assertTrue(ws_scalp._flow_fade_confirmed('SHORT', ind))

    def test_extreme_opposing_flow_needs_confirmation_even_when_btc_relative(self):
        ind = base_ind('SHORT')
        ind['mom_60s'] = -0.40
        ind['mom_180s'] = -0.40
        ind['flow_30s'] = {'buy_pct': 70.0, 'ratio': 2.3}
        ind['flow_120s'] = {'buy_pct': 68.0, 'ratio': 2.1}
        ind['flow_30s_delta'] = 0.0
        ind['mom_5s'] = -0.03
        self.assertIsNone(
            ws_scalp.detect_signal('CLSK', ind, btc_ind('bear', -0.10),
                                   miner_indicators={'CLSK': ind})
        )
        ind['flow_30s_delta'] = -6.0
        sig = ws_scalp.detect_signal('CLSK', ind, btc_ind('bear', -0.10),
                                     miner_indicators={'CLSK': ind})
        self.assertIsNotNone(sig)
        self.assertEqual(sig['setup_type'], 'btc_relative_strength')
        self.assertIn('flow_exhaustion_fade', sig['signal_quality']['setup_tags'])
        self.assertTrue(sig['signal_quality']['flow_fade_confirmed'])

    def test_scalarize_snapshot_preserves_nested_flow(self):
        out = ws_scalp._scalarize_snapshot({
            'symbol': 'CLSK',
            'flow_30s': {'buy_pct': 67.5, 'ratio': 2.1, 'raw': object()},
            'nested_list': [1, 2, 3],
        })
        self.assertEqual(out['flow_30s']['buy_pct'], 67.5)
        self.assertEqual(out['flow_30s']['ratio'], 2.1)
        self.assertNotIn('raw', out['flow_30s'])
        self.assertNotIn('nested_list', out)

    def test_miner_basket_conflict_is_not_confirmation(self):
        target = base_ind('SHORT')
        confirming = base_ind('SHORT')
        opposing = base_ind('LONG')
        basket = ws_scalp._miner_basket_context(
            'CLSK', 'SHORT',
            {'CLSK': target, 'MARA': confirming, 'RIOT': opposing},
        )
        self.assertEqual(basket['state'], 'mixed_conflict')
        self.assertEqual(basket['score'], 0)

    def test_quote_state_and_dual_side_shadow_are_passive_context(self):
        ind = base_ind('LONG')
        ind['best_bid'] = 10.01
        ind['best_ask'] = 10.01
        ind['quote_state'] = ws_scalp._quote_state(10.01, 10.01, 10.0, 0.2, 0.0)
        ind['condition_quality'] = ws_scalp._condition_quality(['@'], [], 'V', 'V', 'V')
        sig = ws_scalp.detect_signal(
            'CLSK',
            ind,
            btc_ind('bull', 0.20),
            miner_indicators={'CLSK': ind, 'MARA': base_ind('LONG')},
        )
        self.assertIsNotNone(sig)
        self.assertIn('quote_locked', sig['execution_quality']['reasons'])
        dual = sig['shadow_dual_side_score']
        self.assertTrue(dual['enabled'])
        self.assertEqual(dual['mode'], 'passive_shadow_only')
        self.assertIn(dual['chosen_side_by_shadow'], ('LONG', 'SHORT'))

    def test_execution_quality_handles_string_condition_score(self):
        sig = {
            'side': 'LONG',
            'price': 10.0,
            'best_bid': 9.99,
            'best_ask': 10.01,
            'indicators': {
                'last_quote_age_sec': 0.2,
                'condition_quality': {'score': '70', 'tags': ['odd_condition_codes']},
            },
        }
        out = ws_scalp.execution_quality(sig)
        self.assertLess(out['score'], 100)
        self.assertIn('odd_condition_codes', out['reasons'])

    def test_rolling_spread_baseline_ignores_crossed_quotes(self):
        now_ms = int(time.time() * 1000)
        st = ws_scalp.SymbolState('CLSK')
        st.last_trade_price = 10.0
        st.last_trade_ts_ms = now_ms
        st.last_quote_ts_ms = now_ms
        st.best_bid = 9.99
        st.best_ask = 10.01
        st.bid_size = 1000
        st.ask_size = 1000
        st.session_pv_sum = 10.0 * 10
        st.session_v_sum = 10
        for i in range(5):
            st.bars_1s.append({
                'ts_s': int(now_ms / 1000) - 5 + i,
                'o': 10.0,
                'h': 10.02,
                'l': 9.98,
                'c': 10.0,
                'v': 10,
                'buy_v': 6,
                'sell_v': 4,
                'n': 2,
            })
        st.quote_history.append((now_ms - 2000, 10.05, 10.04, 100, 100, 0.0, {}))
        st.quote_history.append((now_ms - 1000, 9.99, 10.01, 100, 100, 0.0, {}))
        ind = ws_scalp.compute_indicators(st)
        self.assertGreater(ind['rolling_spread_median_pct'], 0)
        self.assertGreaterEqual(ind['spread_vs_rolling_median'], 0.99)

    def test_exit_policy_candidates_reject_winner_damage(self):
        rolling = {
            'days': ['2026-05-01', '2026-05-02'],
            'scoreboard': [{
                'policy': 'failed_followthrough_90s',
                'estimated_delta_vs_actual': 25.0,
                'matched': 12,
                'days_seen': 2,
                'days_positive': 2,
                'hurt_winners': 1,
                'hurt_winner_delta': -5.0,
                'examples': {},
            }],
        }
        with patch.object(engine_validation, 'rolling_exit_policy_replay', return_value=rolling):
            out = engine_validation.exit_policy_candidate_config('2026-05-02')
        self.assertEqual(out['candidates'], [])
        self.assertIn('Winner damage', out['watch'][0]['why_not_promoted'])


if __name__ == '__main__':
    unittest.main()
