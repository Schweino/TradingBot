from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo


HERE = os.path.dirname(os.path.abspath(__file__))
CT = ZoneInfo('America/Chicago')


def _json(path, default):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return default


def _sha256(path):
    try:
        h = hashlib.sha256()
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(65536), b''):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None


def _tail(path, n=40):
    try:
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            return f.read().splitlines()[-n:]
    except Exception:
        return []


def _file_info(path):
    try:
        st = os.stat(path)
        return {'path': path, 'bytes': st.st_size, 'mtime': int(st.st_mtime)}
    except Exception:
        return {'path': path, 'missing': True}


def build_packet(compact: bool = False, day: str = None):
    today = day or datetime.now(CT).date().isoformat()
    state = _json(os.path.join(HERE, 'mock_trader_state.json'), {})
    config_path = os.path.join(HERE, 'trading_config.json')
    audit_path = os.path.join(HERE, 'audit', f'trade_lifecycle_{today}.jsonl')
    skipped_path = os.path.join(HERE, 'postmortem', 'skipped_signals',
                                f'skipped_signals_{today}.jsonl')
    trades = state.get('trades', [])
    skipped = state.get('skipped_signals', [])
    latest_trades = trades[-5:] if compact else trades[-20:]
    latest_skipped = skipped[-5:] if compact else skipped[-20:]
    packet = {
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'day': today,
        'config_sha256': _sha256(config_path),
        'state_summary': {
            'running': state.get('running'),
            'balance': state.get('balance'),
            'start_balance': state.get('start_balance'),
            'positions': state.get('positions', {}),
            'pending_entries': state.get('pending_entries', {}),
            'broker_exposure_block': state.get('broker_exposure_block'),
            'trade_count': len(trades),
            'skipped_state_count': len(skipped),
        },
        'latest_trades': latest_trades,
        'latest_skipped_state': latest_skipped,
        'log_tail': _tail(os.path.join(HERE, 'mock_trader.log'), 15 if compact else 80),
        'audit_tail': _tail(audit_path, 15 if compact else 80),
        'skipped_corpus_tail': [] if compact else _tail(skipped_path, 80),
        'artifact_manifest': [
            _file_info(os.path.join(HERE, 'mock_trader_state.json')),
            _file_info(os.path.join(HERE, 'mock_trader.log')),
            _file_info(audit_path),
            _file_info(skipped_path),
            _file_info(os.path.join(HERE, 'postmortem', f'postmortem_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', 'trading_events.sqlite')),
        ],
        'compact_review_files': [
            _file_info(os.path.join(HERE, 'postmortem', f'review_index_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'loser_summary_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'risk_summary_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'execution_summary_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'shadow_exit_summary_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'entry_retry_summary_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'exit_hierarchy_summary_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'per_ticker_learning_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'regime_scoring_review_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'config_diff_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'pre_market_checklist_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'config_changes_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'config_freeze_{today}_pre_open.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'config_change_watch_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'trade_alerts_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'MONDAY_REVIEW_START_HERE_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'notes_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'loser_clusters_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'loser_archetypes_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'entry_quality_tiers_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'winner_damage_report_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'clean_day_score_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'no_trade_opportunity_grades_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'thesis_failure_review_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'loser_replay_snapshots_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'per_symbol_personality_{today}_10d.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'out_of_sample_scoreboard_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'first_hour_review_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'strategy_conclusion_gate_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'market_context_scoreboard_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'trade_thesis_timelines_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'winner_quality_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'rule_candidate_quarantine_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'feed_health_score_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'broker_execution_score_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'era_scorecard_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'monday_scorecard_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'ws_scalp_replay_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'decision_audit_summary_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'current_engine_regrade_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'regime_specific_scorecards_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'near_miss_winners_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'exit_quality_score_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'replay_confidence_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'false_positive_negative_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'daily_review_gate_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'rule_lifecycle_dashboard_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'NOW_STATUS_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', 'NOW_STATUS.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'world_class_dashboard_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'strategy_operations_split_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'why_no_trade_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'market_open_monitor_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'current_config_replay_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'monday_live_review_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'postmarket_artifact_validation_{today}.json')),
            _file_info(os.path.join(HERE, 'postmortem', 'change_impact_ledger.json')),
            _file_info(os.path.join(HERE, 'postmortem', 'config_intent_ledger.json')),
            _file_info(os.path.join(HERE, 'postmortem', 'promotion_queue.json')),
            _file_info(os.path.join(HERE, 'postmortem', f'monday_review_packet_{today}.json')),
        ],
        'compact_review_payloads': {
            'review_index': _json(os.path.join(HERE, 'postmortem', f'review_index_{today}.json'), {}),
            'risk_summary': _json(os.path.join(HERE, 'postmortem', f'risk_summary_{today}.json'), {}),
            'loser_summary': _json(os.path.join(HERE, 'postmortem', f'loser_summary_{today}.json'), {}) if not compact else {},
            'execution_summary': _json(os.path.join(HERE, 'postmortem', f'execution_summary_{today}.json'), {}),
            'shadow_exit_summary': _json(os.path.join(HERE, 'postmortem', f'shadow_exit_summary_{today}.json'), {}),
            'entry_retry_summary': _json(os.path.join(HERE, 'postmortem', f'entry_retry_summary_{today}.json'), {}),
            'exit_hierarchy_summary': _json(os.path.join(HERE, 'postmortem', f'exit_hierarchy_summary_{today}.json'), {}),
            'per_ticker_learning': _json(os.path.join(HERE, 'postmortem', f'per_ticker_learning_{today}.json'), {}),
            'regime_scoring_review': _json(os.path.join(HERE, 'postmortem', f'regime_scoring_review_{today}.json'), {}),
            'config_diff': _json(os.path.join(HERE, 'postmortem', f'config_diff_{today}.json'), {}),
            'pre_market_checklist': _json(os.path.join(HERE, 'postmortem', f'pre_market_checklist_{today}.json'), {}),
            'config_freeze': _json(os.path.join(HERE, 'postmortem', f'config_freeze_{today}_pre_open.json'), {}),
            'config_change_watch': _json(os.path.join(HERE, 'postmortem', f'config_change_watch_{today}.json'), {}),
            'trade_alerts': _json(os.path.join(HERE, 'postmortem', f'trade_alerts_{today}.json'), {}),
            'review_start_here': _json(os.path.join(HERE, 'postmortem', f'MONDAY_REVIEW_START_HERE_{today}.json'), {}),
            'loser_clusters': _json(os.path.join(HERE, 'postmortem', f'loser_clusters_{today}.json'), {}),
            'loser_archetypes': _json(os.path.join(HERE, 'postmortem', f'loser_archetypes_{today}.json'), {}),
            'entry_quality_tiers': _json(os.path.join(HERE, 'postmortem', f'entry_quality_tiers_{today}.json'), {}),
            'winner_damage_report': _json(os.path.join(HERE, 'postmortem', f'winner_damage_report_{today}.json'), {}),
            'clean_day_score': _json(os.path.join(HERE, 'postmortem', f'clean_day_score_{today}.json'), {}),
            'no_trade_opportunity_grades': _json(os.path.join(HERE, 'postmortem', f'no_trade_opportunity_grades_{today}.json'), {}),
            'thesis_failure_review': _json(os.path.join(HERE, 'postmortem', f'thesis_failure_review_{today}.json'), {}),
            'loser_replay_snapshots': _json(os.path.join(HERE, 'postmortem', f'loser_replay_snapshots_{today}.json'), {}),
            'per_symbol_personality': _json(os.path.join(HERE, 'postmortem', f'per_symbol_personality_{today}_10d.json'), {}),
            'out_of_sample_scoreboard': _json(os.path.join(HERE, 'postmortem', f'out_of_sample_scoreboard_{today}.json'), {}),
            'first_hour_review': _json(os.path.join(HERE, 'postmortem', f'first_hour_review_{today}.json'), {}),
            'strategy_conclusion_gate': _json(os.path.join(HERE, 'postmortem', f'strategy_conclusion_gate_{today}.json'), {}),
            'market_context_scoreboard': _json(os.path.join(HERE, 'postmortem', f'market_context_scoreboard_{today}.json'), {}),
            'trade_thesis_timelines': _json(os.path.join(HERE, 'postmortem', f'trade_thesis_timelines_{today}.json'), {}),
            'winner_quality': _json(os.path.join(HERE, 'postmortem', f'winner_quality_{today}.json'), {}),
            'rule_candidate_quarantine': _json(os.path.join(HERE, 'postmortem', f'rule_candidate_quarantine_{today}.json'), {}),
            'feed_health_score': _json(os.path.join(HERE, 'postmortem', f'feed_health_score_{today}.json'), {}),
            'broker_execution_score': _json(os.path.join(HERE, 'postmortem', f'broker_execution_score_{today}.json'), {}),
            'era_scorecard': _json(os.path.join(HERE, 'postmortem', f'era_scorecard_{today}.json'), {}),
            'monday_scorecard': _json(os.path.join(HERE, 'postmortem', f'monday_scorecard_{today}.json'), {}),
            'ws_scalp_replay': _json(os.path.join(HERE, 'postmortem', f'ws_scalp_replay_{today}.json'), {}),
            'decision_audit_summary': _json(os.path.join(HERE, 'postmortem', f'decision_audit_summary_{today}.json'), {}),
            'current_engine_regrade': _json(os.path.join(HERE, 'postmortem', f'current_engine_regrade_{today}.json'), {}),
            'regime_specific_scorecards': _json(os.path.join(HERE, 'postmortem', f'regime_specific_scorecards_{today}.json'), {}),
            'near_miss_winners': _json(os.path.join(HERE, 'postmortem', f'near_miss_winners_{today}.json'), {}),
            'exit_quality_score': _json(os.path.join(HERE, 'postmortem', f'exit_quality_score_{today}.json'), {}),
            'replay_confidence': _json(os.path.join(HERE, 'postmortem', f'replay_confidence_{today}.json'), {}),
            'false_positive_negative': _json(os.path.join(HERE, 'postmortem', f'false_positive_negative_{today}.json'), {}),
            'daily_review_gate': _json(os.path.join(HERE, 'postmortem', f'daily_review_gate_{today}.json'), {}),
            'rule_lifecycle_dashboard': _json(os.path.join(HERE, 'postmortem', f'rule_lifecycle_dashboard_{today}.json'), {}),
            'now_status': _json(os.path.join(HERE, 'postmortem', f'NOW_STATUS_{today}.json'), {}),
            'world_class_dashboard': _json(os.path.join(HERE, 'postmortem', f'world_class_dashboard_{today}.json'), {}),
            'strategy_operations_split': _json(os.path.join(HERE, 'postmortem', f'strategy_operations_split_{today}.json'), {}),
            'why_no_trade': _json(os.path.join(HERE, 'postmortem', f'why_no_trade_{today}.json'), {}),
            'market_open_monitor': _json(os.path.join(HERE, 'postmortem', f'market_open_monitor_{today}.json'), {}),
            'current_config_replay': _json(os.path.join(HERE, 'postmortem', f'current_config_replay_{today}.json'), {}),
            'monday_live_review': _json(os.path.join(HERE, 'postmortem', f'monday_live_review_{today}.json'), {}),
            'postmarket_artifact_validation': _json(os.path.join(HERE, 'postmortem', f'postmarket_artifact_validation_{today}.json'), {}),
            'change_impact_ledger': _json(os.path.join(HERE, 'postmortem', 'change_impact_ledger.json'), {}),
            'config_intent_ledger': _json(os.path.join(HERE, 'postmortem', 'config_intent_ledger.json'), {}),
            'promotion_queue': _json(os.path.join(HERE, 'postmortem', 'promotion_queue.json'), {}),
        },
        'important_files': [
            'SYSTEM_MAP.md',
            'REVIEW_CHECKLIST.md',
            'trading_config.json',
            'mock_trader.py',
            'ws_scalp.py',
            'daily_postmortem.py',
            'postmortem_hypotheses.py',
            'alpaca_trading.py',
            'smoke_check.py',
            'eod_integrity_check.py',
            'audit/trade_lifecycle_YYYY-MM-DD.jsonl',
            'postmortem/skipped_signals/skipped_signals_YYYY-MM-DD.jsonl',
        ],
    }
    return packet


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('day', nargs='?')
    parser.add_argument('--compact', action='store_true')
    args = parser.parse_args()
    out = build_packet(compact=args.compact, day=args.day)
    path = os.path.join(HERE, 'REVIEW_PACKET_COMPACT.json' if args.compact else 'REVIEW_PACKET.json')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=2, default=str)
    print(path)
