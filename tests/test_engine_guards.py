import unittest

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


if __name__ == '__main__':
    unittest.main()

