import os
import json
import unittest
from unittest.mock import patch

import automation_ops
import engine_replay
import market_calendar
import mock_trader
import review_artifacts
import runtime_guard
import tick_replay
import weekend_readiness


class RuntimeGuardTests(unittest.TestCase):
    def test_single_instance_lock_blocks_second_holder(self):
        path = os.path.join(runtime_guard.RUNTIME_DIR, 'test_app.lock')
        first = runtime_guard.SingleInstanceLock(path)
        second = runtime_guard.SingleInstanceLock(path)
        try:
            self.assertTrue(first.acquire())
            self.assertFalse(second.acquire())
        finally:
            second.release()
            first.release()
            try:
                os.remove(path)
            except OSError:
                pass


class BrokerExposureBlockTests(unittest.TestCase):
    def _flat_snapshot(self):
        return {
            'watched': ['CLSK', 'MARA', 'RIOT'],
            'local': {
                'positions': {},
                'pending_entries': {},
                'broker_exposure_block': {
                    'symbols': ['CLSK'],
                    'reason': 'session_flat_positions_not_flat_after_close_attempt',
                },
            },
            'broker': {
                'reachable': True,
                'watched_positions': [],
                'watched_open_orders': [],
            },
            'verdict': {
                'flat_watched': True,
                'no_watched_open_orders': True,
                'no_local_positions': True,
                'no_pending_entries': True,
                'no_exposure_block': False,
            },
        }

    def test_flat_snapshot_clears_stale_exposure_block(self):
        mt = mock_trader.MockTrader.__new__(mock_trader.MockTrader)
        mt.lock = mock_trader.threading.RLock()
        mt.state = {
            'broker_exposure_block': {
                'symbols': ['CLSK'],
                'reason': 'session_flat_positions_not_flat_after_close_attempt',
            }
        }
        mt._save = lambda *a, **k: True
        events = []
        mt._audit_event = lambda event, data=None, **k: events.append((event, data))

        sidecar = os.path.join(os.getcwd(), '.test_missing_critical_state_block.json')
        with patch.object(mock_trader, 'EMERGENCY_STATE_BLOCK_PATH', sidecar):
            self.assertTrue(mt._clear_block_if_snapshot_flat(self._flat_snapshot(), 'test', 'snapshot.json'))
        self.assertNotIn('broker_exposure_block', mt.state)
        self.assertEqual(events[0][0], 'broker_exposure_block_cleared')

    def test_start_uses_flat_snapshot_when_broker_client_unavailable(self):
        mt = mock_trader.MockTrader.__new__(mock_trader.MockTrader)
        mt.lock = mock_trader.threading.RLock()
        mt.trader = None
        mt.state = {
            'running': False,
            'positions': {},
            'broker_exposure_block': {
                'symbols': ['CLSK'],
                'reason': 'session_flat_positions_not_flat_after_close_attempt',
            },
        }
        mt._save = lambda *a, **k: True
        mt._audit_event = lambda *a, **k: None
        mt._latest_flat_broker_safety_snapshot = lambda symbols: {
            'path': 'snapshot.json',
            'payload': self._flat_snapshot(),
        }
        mt._monitor_thread = type('ThreadStub', (), {'is_alive': lambda self: True})()

        sidecar = os.path.join(os.getcwd(), '.test_missing_critical_state_block.json')
        with patch.object(mock_trader, 'EMERGENCY_STATE_BLOCK_PATH', sidecar):
            result = mt.start()
        self.assertTrue(result['ok'])
        self.assertTrue(mt.state['running'])
        self.assertNotIn('broker_exposure_block', mt.state)

    def test_rotate_file_keeps_rotated_copy(self):
        path = os.path.join(runtime_guard.RUNTIME_DIR, 'test_rotate.log')
        os.makedirs(os.path.dirname(path), exist_ok=True)
        for suffix in ('', '.1', '.2'):
            try:
                os.remove(path + suffix)
            except OSError:
                pass
        try:
            with open(path, 'w', encoding='utf-8') as f:
                f.write('x' * 128)
            rotated = runtime_guard.rotate_file(path, max_bytes=10, backup_count=2)
            self.assertEqual(rotated, path + '.1')
            self.assertFalse(os.path.exists(path))
            self.assertTrue(os.path.exists(path + '.1'))
        finally:
            for suffix in ('', '.1', '.2'):
                try:
                    os.remove(path + suffix)
                except OSError:
                    pass


class LearningArtifactTests(unittest.TestCase):
    def test_engine_replay_carries_components_and_peer_context(self):
        signal = {
            'side': 'LONG',
            'score': 6,
            'setup_type': 'btc_relative_strength',
            'signal_quality': {
                'setup_tags': ['btc_relative_strength'],
                'score_components': {'btc': 2, 'miner_basket': 1},
            },
            'miner_basket': {'state': 'confirmed'},
        }
        compact = engine_replay._compact_signal(signal)
        self.assertEqual(compact['components']['btc'], 2)
        self.assertEqual(compact['setup_tags'], ['btc_relative_strength'])
        self.assertEqual(compact['miner_basket']['state'], 'confirmed')

        samples = [
            {'ticker': 'CLSK', 'created_at': 1000.0, 'indicators': {'ready': True, 'price': 10}},
            {'ticker': 'MARA', 'created_at': 1003.0, 'indicators': {'ready': True, 'price': 20}},
            {'ticker': 'RIOT', 'created_at': 1010.0, 'indicators': {'ready': True, 'price': 30}},
        ]
        context = engine_replay._miner_context_for_sample(samples[0], samples)
        self.assertEqual(sorted(context), ['CLSK', 'MARA'])

    def test_score_calibration_groups_by_score_execution_and_context(self):
        trades = [
            {
                'ticker': 'CLSK',
                'side': 'LONG',
                'pnl': 12.0,
                'entry_score': 6.4,
                'entry_quality_score': 80,
                'setup_type': 'trend_pullback',
                'btc_context': {'regime': 'bull'},
                'market_tape_at_entry': {
                    'SPY': {'day_pct': 0.30, 'above_vwap': True},
                    'QQQ': {'day_pct': 0.35, 'above_vwap': True},
                    'IWM': {'day_pct': 0.10, 'above_vwap': True},
                },
                'entry_thesis': {
                    'signal_quality': {
                        'score_components': {'btc': 2, 'execution_quality': 1},
                    },
                },
            },
            {
                'ticker': 'CLSK',
                'side': 'LONG',
                'pnl': -7.0,
                'entry_score': 6.2,
                'entry_quality_score': 61,
                'setup_type': 'trend_pullback',
                'btc_context': {'regime': 'bull'},
                'market_tape_at_entry': {
                    'SPY': {'day_pct': -0.20, 'above_vwap': False},
                    'QQQ': {'day_pct': -0.10, 'above_vwap': False},
                    'IWM': {'day_pct': -0.50, 'above_vwap': False},
                },
                'entry_thesis': {
                    'signal_quality': {
                        'score_components': {'btc': 1, 'spread_quality_gate': -1},
                    },
                },
            },
            {
                'ticker': 'MARA',
                'side': 'SHORT',
                'pnl': 5.0,
                'entry_thesis': {
                    'score': 7.1,
                    'setup_type': 'btc_relative_strength',
                    'btc_context': {'regime_detail': 'bear'},
                    'market_tape': {
                        'SPY': {'day_pct': -0.15, 'above_vwap': False},
                        'QQQ': {'day_pct': -0.25, 'above_vwap': False},
                        'IWM': {'day_pct': -0.45, 'above_vwap': False},
                    },
                    'decision_audit': {'execution_quality': {'score': 91}},
                    'signal_quality': {
                        'score_components': {'btc': -2, 'relative_strength': -2},
                    },
                },
            },
        ]
        with patch.object(review_artifacts, '_available_postmortem_days', return_value=['2026-05-02']):
            with patch.object(review_artifacts, '_tape', return_value=trades):
                out = review_artifacts.build_score_calibration('2026-05-02')
        score_rows = {r['bucket']: r for r in out['tables']['score']}
        exec_rows = {r['bucket']: r for r in out['tables']['execution_quality']}
        regime_rows = {r['bucket']: r for r in out['tables']['btc_regime']}
        market_rows = {r['bucket']: r for r in out['tables']['market_regime']}
        component_rows = {r['bucket']: r for r in out['tables']['component_effect']}
        self.assertEqual(score_rows['6-6.9']['trades'], 2)
        self.assertEqual(score_rows['6-6.9']['pnl'], 5.0)
        self.assertEqual(exec_rows['75-84']['trades'], 1)
        self.assertEqual(exec_rows['<65']['losses'], 1)
        self.assertEqual(regime_rows['bull']['trades'], 2)
        self.assertEqual(regime_rows['bear']['wins'], 1)
        self.assertEqual(market_rows['broad_risk_on']['wins'], 1)
        self.assertEqual(market_rows['smallcap_risk_off']['trades'], 2)
        self.assertEqual(component_rows['btc|supported_entry_side']['trades'], 3)
        self.assertEqual(component_rows['spread_quality_gate|warned_against_entry_side']['losses'], 1)

    def test_market_tape_attribution_groups_entry_context(self):
        trades = [{
            'ticker': 'CLSK',
            'side': 'LONG',
            'pnl': -4.0,
            'market_tape_at_entry': {
                'SPY': {'day_pct': -0.20, 'above_vwap': False},
                'QQQ': {'day_pct': -0.25, 'above_vwap': False},
                'IWM': {'day_pct': -0.60, 'above_vwap': False},
            },
        }]
        with patch.object(review_artifacts, '_tape', return_value=trades):
            out = review_artifacts.build_market_tape_attribution('2026-05-02')
        rows = {r['bucket']: r for r in out['market_regime_buckets']}
        self.assertEqual(rows['smallcap_risk_off']['losses'], 1)
        self.assertEqual(rows['smallcap_risk_off']['avg_iwm_day_pct'], -0.6)

    def test_passive_learning_artifacts_grade_side_and_rules(self):
        trades = [
            {
                'trade_id': 't1',
                'ticker': 'CLSK',
                'side': 'LONG',
                'setup_type': 'btc_relative_strength',
                'pnl': 14.0,
                'reason': 'take_profit',
                'setup_grade_at_entry': {'grade': 'A', 'score': 82, 'tags': ['btc_confirmed']},
                'fwd5': 0.5,
                'fwd15': 0.8,
                'entry_thesis': {'signal_quality': {'score_components': {'btc': 2, 'execution_quality': 1}}},
            },
            {
                'trade_id': 't2',
                'ticker': 'MARA',
                'side': 'SHORT',
                'setup_type': 'flow_exhaustion_fade',
                'pnl': -9.0,
                'reason': 'short_conviction_decay',
                'setup_grade_at_entry': {'grade': 'C', 'score': 58, 'tags': ['wide_spread']},
                'fwd5': -0.4,
                'fwd15': -0.6,
                'fwd60': 0.1,
                'entry_thesis': {'signal_quality': {'score_components': {'btc': -1, 'flow_30s': 2}}},
            },
        ]
        with patch.object(review_artifacts, '_available_postmortem_days', return_value=['2026-05-02']):
            with patch.object(review_artifacts, '_tape', return_value=trades):
                grades = review_artifacts.build_setup_grade_review('2026-05-02')
                side = review_artifacts.build_counterfactual_side_review('2026-05-02')
                rules = review_artifacts.build_rule_attribution_review('2026-05-02')

        grade_rows = {r['bucket']: r for r in grades['by_grade']}
        self.assertEqual(grade_rows['A']['wins'], 1)
        self.assertEqual(grade_rows['C']['losses'], 1)
        self.assertEqual(side['rows'][0]['opposite_side'], 'LONG')
        self.assertEqual(side['rows'][0]['verdict'], 'opposite_side_showed_fixed_horizon_edge')
        rule_rows = {r['rule']: r for r in rules['rows']}
        self.assertEqual(rule_rows['component:flow_30s|warned_against_entry_side']['helped'], 1)
        self.assertEqual(rule_rows['component:btc|supported_entry_side']['trades'], 2)

    def test_hypothesis_confidence_scores_active_watchlist(self):
        active = [{
            'id': 'H123',
            'key': 'short_buy_flow',
            'monitor': 'Watch SHORTs with sustained buy flow.',
            'evidence_days': 3,
            'seen_dates': ['2026-04-29', '2026-04-30', '2026-05-02'],
            'created': '2026-04-29',
            'last_seen': '2026-05-02',
            'days_since_evidence': 0,
            'deduction': '2/5 losing SHORTs had sustained buying flow.',
        }]
        with patch.object(review_artifacts, '_trade_day', return_value={
            'trust_today_for_learning': {'quality_score': 88, 'trust': 'usable', 'quality_label': 'clean'}
        }), patch.object(review_artifacts, '_read_json', return_value={'active': active}), \
             patch.object(review_artifacts, '_tape', return_value=[]):
            out = review_artifacts.build_hypothesis_confidence('2026-05-02')

        self.assertEqual(out['rows'][0]['id'], 'H123')
        self.assertEqual(out['rows'][0]['confidence_label'], 'ready_for_human_review')
        self.assertEqual(out['counts']['ready_for_human_review'], 1)

    def test_matched_controls_exit_efficiency_and_fingerprints(self):
        trades = [
            {
                'trade_id': 'loss1',
                'time': '09:30',
                'ticker': 'CLSK',
                'side': 'LONG',
                'setup_type': 'trend_pullback',
                'pnl': -10.0,
                'entry': 10.0,
                'exit': 9.95,
                'mfe_pct': 0.35,
                'mae_pct': 0.55,
                'entry_quality_tier': 'C',
                'btc': {'regime': 'bull'},
                'forensics': {'entry_quality': {'spread_pct': 0.10}},
                'path': {
                    'first_red_ts': 1005,
                    'time_to_mfe_sec': 20,
                    'time_to_mae_sec': 90,
                    'checks': {'30s': {'signed_return_pct': -0.2}, '1m': {'signed_return_pct': -0.4}},
                },
                'opened_at': 1000,
            },
            {
                'trade_id': 'win1',
                'time': '09:35',
                'ticker': 'CLSK',
                'side': 'LONG',
                'setup_type': 'trend_pullback',
                'pnl': 12.0,
                'entry': 10.0,
                'exit': 10.08,
                'mfe_pct': 0.45,
                'mae_pct': 0.05,
                'entry_quality_tier': 'A',
                'btc': {'regime': 'bull'},
                'forensics': {'entry_quality': {'spread_pct': 0.03}},
                'path': {'checks': {'30s': {'signed_return_pct': 0.2}}},
                'opened_at': 1100,
            },
        ]
        with patch.object(review_artifacts, '_available_postmortem_days', return_value=['2026-05-02']):
            with patch.object(review_artifacts, '_tape', return_value=trades):
                matched = review_artifacts.build_matched_control_review('2026-05-02')
                exits = review_artifacts.build_exit_efficiency_review('2026-05-02')
                timeline = review_artifacts.build_mae_mfe_timeline('2026-05-02')
                fingerprints = review_artifacts.build_loser_fingerprints('2026-05-02')
                verdicts = review_artifacts.build_trade_verdicts('2026-05-02')
                avoid = review_artifacts.build_best_avoided_loser_simulator('2026-05-02')

        self.assertEqual(matched['rows'][0]['deduction'], 'counterexamples_present_do_not_overfit')
        self.assertEqual(exits['rows'][0]['label'], 'gave_back_open_profit')
        self.assertEqual(timeline['rows'][0]['checkpoints'][1]['status'], 'invalidated')
        self.assertEqual(fingerprints['rows'][0]['primary'], 'execution_or_slippage')
        self.assertEqual(verdicts['rows'][0]['verdict'], 'would_skip')
        avoid_rows = {r['rule']: r for r in avoid['rows']}
        self.assertGreater(avoid_rows['avoid_wide_spread_entries']['net_pnl_if_blocked'], 0)

    def test_hypothesis_counterexamples_detect_winning_matches(self):
        active = [{
            'id': 'H200',
            'key': 'short_buy_flow',
            'monitor': 'Watch SHORTs with flow_120s buy_pct >= 65 continue losing.',
        }]
        trades = [
            {
                'trade_id': 'loss1',
                'ticker': 'MARA',
                'side': 'SHORT',
                'pnl': -8.0,
                'ind': {'flow_120s': {'buy_pct': 70}},
            },
            {
                'trade_id': 'win1',
                'ticker': 'RIOT',
                'side': 'SHORT',
                'pnl': 6.0,
                'ind': {'flow_120s': {'buy_pct': 68}},
            },
        ]
        with patch.object(review_artifacts, '_read_json', return_value={'active': active}), \
             patch.object(review_artifacts, '_tape', return_value=trades):
            out = review_artifacts.build_hypothesis_counterexamples('2026-05-02')
        self.assertEqual(out['rows'][0]['evidence_losers'], 1)
        self.assertEqual(out['rows'][0]['counterexample_winners'], 1)
        self.assertEqual(out['counts']['with_counterexamples'], 1)

    def test_section_confidence_and_rolling_dashboard_are_compact(self):
        trades = [{'trade_id': 't1', 'ticker': 'CLSK', 'side': 'LONG', 'pnl': 1.0}]
        with patch.object(review_artifacts, '_available_postmortem_days', return_value=['2026-05-02']), \
             patch.object(review_artifacts, '_tape', return_value=trades), \
             patch.object(review_artifacts, '_read_json', return_value={'active': []}):
            section = review_artifacts.build_postmortem_section_confidence('2026-05-02')
            rolling = review_artifacts.build_rolling_learning_dashboard('2026-05-02')
        self.assertIn('sections', section)
        self.assertEqual(rolling['totals']['trades'], 1)

    def test_elite_learning_layer_builds_replay_registry_and_weekly_packet(self):
        trades = [
            {
                'trade_id': 'loss1',
                'time': '08:45',
                'ticker': 'MARA',
                'side': 'SHORT',
                'setup_type': 'flow_exhaustion_fade',
                'pnl': -20.0,
                'entry': 10.0,
                'exit': 10.02,
                'qty': 100,
                'spread_pct': 0.10,
                'setup_grade_at_entry': {'grade': 'C', 'score': 54, 'tags': ['wide_spread']},
                'ind': {'flow_120s': {'buy_pct': 70}, 'mom_60s': 0.03},
                'btc': {'regime': 'bull', 'mom_60s': 0.09},
                'path': {
                    'checks': {
                        '30s': {'signed_return_pct': -0.10},
                        '1m': {'signed_return_pct': -0.20},
                    },
                },
            },
            {
                'trade_id': 'win1',
                'time': '08:52',
                'ticker': 'CLSK',
                'side': 'LONG',
                'setup_type': 'btc_relative_strength',
                'pnl': 12.0,
                'entry': 10.0,
                'exit': 10.12,
                'qty': 100,
                'spread_pct': 0.03,
                'setup_grade_at_entry': {'grade': 'A', 'score': 84, 'tags': ['btc_confirmed']},
                'ind': {'flow_120s': {'buy_pct': 62}, 'mom_60s': 0.10},
                'btc': {'regime': 'bull', 'mom_60s': 0.11},
                'market_tape_at_entry': {
                    'SPY': {'day_pct': 0.20, 'above_vwap': True},
                    'QQQ': {'day_pct': 0.25, 'above_vwap': True},
                    'IWM': {'day_pct': 0.15, 'above_vwap': True},
                },
            },
        ]
        active_hypotheses = [{
            'id': 'H900',
            'key': 'short_buy_flow',
            'monitor': 'Watch SHORTs with flow_120s buy_pct >= 65 continue losing.',
            'evidence_days': 3,
            'seen_dates': ['2026-04-30', '2026-05-01', '2026-05-02'],
            'created': '2026-04-30',
            'last_seen': '2026-05-02',
            'days_since_evidence': 0,
            'deduction': '1/1 losing SHORTs had sustained buying flow.',
        }]

        def fake_read_json(path, default=None):
            if path.endswith('hypotheses.json'):
                return {'active': active_hypotheses}
            if path.endswith('experiment_registry.json'):
                return {'experiments': {}}
            if path.endswith('config_intent_ledger.json'):
                return {
                    'items': {
                        'abc123': {
                            'era': 'test_learning_era',
                            'first_seen_day': '2026-05-02',
                            'why_changed': 'test seed',
                            'expected_impact': ['Improve loser avoidance.'],
                        }
                    }
                }
            if path.endswith('human_feedback_2026-05-02.json'):
                return {'decisions': [{'target_type': 'hypothesis', 'target_id': 'H900', 'decision': 'agree'}]}
            if path.endswith('human_feedback.json'):
                return {'decisions': []}
            if path.endswith('promotion_queue.json'):
                return {'items': {}}
            return default if default is not None else {}

        with patch.object(review_artifacts, '_available_postmortem_days', return_value=['2026-05-02']), \
             patch.object(review_artifacts, '_tape', return_value=trades), \
             patch.object(review_artifacts, '_trade_day', return_value={
                 'trust_today_for_learning': {'quality_score': 90, 'trust': 'usable', 'quality_label': 'clean'}
             }), \
             patch.object(review_artifacts, 'ensure_human_feedback_template', return_value='postmortem/human_feedback_2026-05-02.json'), \
             patch.object(review_artifacts, '_read_json', side_effect=fake_read_json):
            replay = review_artifacts.build_true_counterfactual_replay('2026-05-02')
            feature_table = review_artifacts.build_feature_outcome_table('2026-05-02')
            confidence_decay = review_artifacts.build_rule_confidence_decay('2026-05-02')
            feedback = review_artifacts.build_human_feedback_summary('2026-05-02')
            regimes = review_artifacts.build_market_regime_library('2026-05-02')
            registry = review_artifacts.build_experiment_registry(
                '2026-05-02',
                replay=replay,
                human_feedback=feedback,
            )
            weekly = review_artifacts.build_weekly_promotion_meeting_packet(
                '2026-05-02',
                replay=replay,
                registry=registry,
                confidence=confidence_decay,
                regimes=regimes,
                feedback=feedback,
            )
            index = review_artifacts.build_review_index('2026-05-02')

        replay_rows = {row['policy']: row for row in replay['policies']}
        self.assertGreater(replay_rows['skip_c_grade_entries']['net_delta_vs_actual'], 0)
        self.assertEqual(feature_table['rows'][0]['flow_120s_buy_pct'], 70)
        self.assertEqual(confidence_decay['rows'][0]['label'], 'ready_for_human_review')
        self.assertIn('config:abc123', registry['experiments'])
        self.assertGreaterEqual(len(weekly['decisions']['promote']), 1)
        self.assertIn('true_counterfactual_replay', index)

    def test_automation_validate_only_does_not_write_artifacts(self):
        def fake_run(args, timeout=120):
            return {'command': args, 'returncode': 0, 'ok': True, 'output': ''}

        with patch.object(automation_ops, '_status_reachable', return_value=True), \
             patch.object(automation_ops, '_run', side_effect=fake_run), \
             patch.object(automation_ops, '_write_json') as write_json:
            out = automation_ops.run_phase('pre-open', '2026-05-02', validate_only=True)
        self.assertTrue(out['ok'])
        self.assertTrue(out['artifact_write_skipped'])
        self.assertIsNone(out['artifact'])
        self.assertFalse(write_json.called)

    def test_market_calendar_skips_known_holiday_for_scheduled_automation(self):
        self.assertTrue(market_calendar.is_trading_day('2026-05-04'))
        holiday = market_calendar.market_calendar_status('2026-05-25')
        self.assertFalse(holiday['is_trading_day'])
        self.assertEqual(holiday['reason'], 'Memorial Day')

        writes = []

        def fake_write(path, payload):
            writes.append((path, payload))
            return path

        with patch.object(automation_ops, '_write_json', side_effect=fake_write):
            out = automation_ops.run_phase('pre-open', '2026-05-25')
        self.assertTrue(out['ok'])
        self.assertTrue(out['skipped_market_closed'])
        self.assertEqual(out['market_calendar']['reason'], 'Memorial Day')
        self.assertTrue(writes)

    def test_post_close_step2_uses_planned_rebuild_not_forced_full_rebuild(self):
        step = automation_ops._post_close_compiled_step2_step('2026-05-04', pure_validation=True)
        self.assertTrue(step['ok'])
        self.assertNotIn('--full-rebuild', step['command'])
        self.assertIn('--write-unified-ledger', step['command'])

    def test_runtime_restart_alerts_promote_unexpected_restart(self):
        rows = [{
            'kind': 'process_boot',
            'created_at_ct': '2026-05-02T08:30:00-05:00',
            'payload': {
                'previous_pid': 10,
                'current_pid': 20,
                'downtime_sec': 240,
                'unexpected_restart': True,
                'previous_heartbeat_iso': '2026-05-02T08:25:00-05:00',
            },
        }]
        with patch.object(weekend_readiness, '_runtime_event_rows', return_value=rows):
            out = weekend_readiness.build_runtime_restart_alerts('2026-05-02')
        self.assertFalse(out['ok'])
        self.assertEqual(out['unexpected_restart_count'], 1)
        self.assertEqual(out['alerts'][0]['severity'], 'critical')

    def test_tick_replay_dedupes_and_scores_entry_path(self):
        day = '2026-05-02'
        base = 1777660000000
        tmp = os.path.join(runtime_guard.RUNTIME_DIR, 'test_tick_replay')
        tick_root = os.path.join(tmp, 'tick_logs')
        out_dir = os.path.join(tmp, 'postmortem')
        day_dir = os.path.join(tick_root, day)
        os.makedirs(day_dir, exist_ok=True)
        path = os.path.join(day_dir, 'CLSK_093000_LONG_HIGH_test.jsonl')
        exit_path = os.path.join(day_dir, 'CLSK_093500_EXIT_LONG_test.jsonl')
        rows = [
                {
                    'type': 'header',
                    'label': 'LONG_HIGH',
                    'ticker': 'CLSK',
                    'capture_id': 'test-trade-1',
                    'started_ms': base,
                    'signal': {
                        'ticker': 'CLSK',
                        'side': 'LONG',
                        'setup_type': 'btc_relative_strength',
                        'conviction': 'HIGH',
                        'score': 7,
                        'price': 10.0,
                        'btc_indicators': {'price': 100000},
                    },
                },
                {'phase': 'pre', 'ts_ms': base - 1000, 'symbol': 'CLSK', 'event': 'trade', 'price': 9.99, 'size': 10, 'side': 1},
                {'phase': 'pre', 'ts_ms': base - 500, 'symbol': 'CLSK', 'event': 'quote', 'bid': 9.99, 'ask': 10.01, 'bid_size': 100, 'ask_size': 80, 'imbalance': 0.111},
                {'phase': 'pre', 'ts_ms': base - 400, 'symbol': 'BTC/USD', 'event': 'trade', 'price': 99950, 'size': 0, 'side': 1},
                {'phase': 'post', 'ts_ms': base + 1000, 'symbol': 'CLSK', 'event': 'trade', 'price': 10.02, 'size': 100, 'side': 1},
                {'phase': 'post', 'ts_ms': base + 1000, 'symbol': 'CLSK', 'event': 'trade', 'price': 10.02, 'size': 100, 'side': 1},
                {'phase': 'post', 'ts_ms': base + 5000, 'symbol': 'CLSK', 'event': 'trade', 'price': 10.06, 'size': 50, 'side': 1},
                {'phase': 'post', 'ts_ms': base + 10000, 'symbol': 'CLSK', 'event': 'trade', 'price': 9.98, 'size': 20, 'side': -1},
                {'phase': 'post', 'ts_ms': base + 2000, 'symbol': 'CLSK', 'event': 'quote', 'bid': 10.01, 'ask': 10.03, 'bid_size': 120, 'ask_size': 90, 'imbalance': 0.143},
                {'phase': 'post', 'ts_ms': base + 60000, 'symbol': 'BTC/USD', 'event': 'trade', 'price': 100100, 'size': 0, 'side': 1},
                {'phase': 'pre', 'ts_ms': base - 1000, 'symbol': 'MARA', 'event': 'trade', 'price': 20.0, 'size': 10, 'side': 1},
                {'phase': 'post', 'ts_ms': base + 60000, 'symbol': 'MARA', 'event': 'trade', 'price': 20.1, 'size': 10, 'side': 1},
        ]
        with open(path, 'w', encoding='utf-8') as f:
            for row in rows:
                f.write(f'{json.dumps(row)}\n')
        exit_rows = [
            {
                'type': 'header',
                'label': 'EXIT_LONG_profit_protect',
                'ticker': 'CLSK',
                'capture_id': 'exit-test-1',
                'started_ms': base + 300000,
                'signal': {
                    'capture_type': 'exit',
                    'ticker': 'CLSK',
                    'side': 'LONG',
                    'price': 10.0,
                    'exit_reason': 'profit_protect',
                    'trade_id': 'test-trade-1',
                    'exit_reason_hierarchy': {'selected': 'profit_protect', 'candidates': [{'kind': 'profit'}]},
                },
            },
            {'phase': 'post', 'ts_ms': base + 301000, 'symbol': 'CLSK', 'event': 'trade', 'price': 10.02, 'size': 100, 'side': 1},
            {'phase': 'post', 'ts_ms': base + 360000, 'symbol': 'CLSK', 'event': 'trade', 'price': 10.12, 'size': 100, 'side': 1},
            {'phase': 'post', 'ts_ms': base + 360000, 'symbol': 'BTC/USD', 'event': 'trade', 'price': 100200, 'size': 0, 'side': 1},
        ]
        with open(exit_path, 'w', encoding='utf-8') as f:
            for row in exit_rows:
                f.write(f'{json.dumps(row)}\n')

        with patch.object(tick_replay, 'TICK_DIR', tick_root), \
             patch.object(tick_replay, 'OUT_DIR', out_dir):
            out = tick_replay.build_tick_replay(day)
            written, written_payload = tick_replay.write_tick_replay(day)

        self.assertEqual(out['summary']['captures_analyzed'], 2)
        self.assertEqual(out['summary']['duplicate_rows_removed'], 1)
        rows_by_type = {r['capture_type']: r for r in out['rows']}
        row = rows_by_type['entry']
        self.assertTrue(row['stock_path']['available'])
        self.assertEqual(row['stock_path']['mfe_pct'], 0.6)
        self.assertEqual(row['stock_path']['mae_pct'], -0.2)
        self.assertTrue(row['btc_path']['supports_side_60s'])
        self.assertTrue(row['post_entry_flow']['support_30s'])
        self.assertEqual(row['miner_peer_context']['post_state'], 'peers_confirmed_side')
        self.assertIn('instant_followthrough', row['tick_path_tags'])
        exit_row = rows_by_type['exit']
        self.assertEqual(exit_row['exit_replay']['verdict'], 'likely_exited_too_early')
        self.assertEqual(exit_row['exit_context']['candidate_count'], 1)
        self.assertTrue(os.path.exists(written))
        self.assertEqual(written_payload['summary']['captures_analyzed'], 2)

    def test_new_execution_learning_summaries_are_compact(self):
        trades = [{
            'trade_id': 't1',
            'ticker': 'CLSK',
            'side': 'SHORT',
            'pnl': -5.0,
            'entry': 10.0,
            'entry_timing': {
                'windows_sec': {
                    '15': {
                        'best_entry_px': 10.05,
                        'worst_entry_px': 9.98,
                        'could_have_improved_entry_pct': 0.5,
                    }
                }
            },
            'forensics': {
                'entry_quality': {
                    'quote_state': {'state': 'normal'},
                    'condition_quality': {'score': 95, 'tags': []},
                    'spread_vs_rolling_median': 1.2,
                },
                'shadow_dual_side_score': {
                    'enabled': True,
                    'chosen_side_by_shadow': 'LONG',
                    'opposite_side': 'SHORT',
                    'side_gap': 2.0,
                    'long': {'score': 5},
                    'short': {'score': 3},
                    'why_opposite_failed': ['btc_regime_opposes_side'],
                },
            },
        }]
        fill_rows = [{
            'trade_id': 't1',
            'ticker': 'CLSK',
            'side': 'SHORT',
            'stage': 'entry_fill',
            'quote': {'quote_state': {'state': 'locked'}, 'spread_abnormal': True},
            'condition_quality': {'score': 70, 'tags': ['trade_conditions_present']},
        }]
        latency_rows = [{
            'trade_id': 't1',
            'ticker': 'CLSK',
            'side': 'SHORT',
            'status': 'entry_filled',
            'from_signal_ms': {'entry_filled_from_signal_ms': 850, 'committed_from_signal_ms': 120},
            'adjacent_ms': {'broker_submit_start_ms_to_broker_submit_end_ms': 80},
        }]
        with patch.object(review_artifacts, '_tape', return_value=trades), \
             patch.object(review_artifacts, '_skipped_signal_rows', return_value=[]), \
             patch.object(review_artifacts, '_gate_timeline_rows', return_value=[]), \
             patch.object(review_artifacts, '_no_signal_snapshot_rows', return_value=[]), \
             patch.object(review_artifacts, '_fill_attribution_rows', return_value=fill_rows), \
             patch.object(review_artifacts, '_latency_attribution_rows', return_value=latency_rows):
            dual = review_artifacts.build_dual_side_shadow_review('2026-05-02')
            timing = review_artifacts.build_entry_timing_efficiency('2026-05-02')
            latency = review_artifacts.build_latency_attribution_summary('2026-05-02')
            quote = review_artifacts.build_quote_condition_quality_summary('2026-05-02')

        self.assertEqual(dual['counts']['shadow_preferred_opposite_side'], 1)
        self.assertEqual(timing['by_window']['15']['avg_improvement_available_pct'], 0.5)
        self.assertEqual(latency['by_status']['entry_filled']['avg_fill_from_signal_ms'], 850.0)
        self.assertEqual(quote['quote_state_counts']['locked'], 1)
        self.assertEqual(quote['condition_tag_counts']['trade_conditions_present'], 1)


if __name__ == '__main__':
    unittest.main()
