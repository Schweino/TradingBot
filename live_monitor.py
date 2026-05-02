from __future__ import annotations

import argparse
import json
import time
from datetime import datetime

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from weekend_readiness import (
    write_broker_execution_score,
    write_config_change_watch,
    write_config_intent_ledger,
    write_current_engine_regrade,
    write_daily_review_gate,
    write_decision_audit_summary,
    write_era_scorecard,
    write_exit_quality_score,
    write_false_positive_negative_tables,
    write_feed_health_score,
    write_market_open_monitor,
    write_monday_scorecard,
    write_monday_launch_checklist,
    write_now_status,
    write_replay_confidence,
    write_monday_live_review,
    write_first_hour_review,
    write_market_context_scoreboard,
    write_review_start_here,
    write_rule_candidate_quarantine,
    write_rule_lifecycle_dashboard,
    write_regime_specific_scorecards,
    write_session_checkpoint,
    write_strategy_conclusion_gate,
    write_trade_thesis_timeline_artifact,
    write_trade_alerts,
    write_winner_quality_artifact,
    write_world_class_dashboard,
    write_ws_scalp_replay,
    write_near_miss_winners,
    write_why_no_trade_summary,
)


CT = ZoneInfo('America/Chicago')


def _today() -> str:
    return datetime.now(CT).date().isoformat()


def write_once(day: str) -> dict:
    paths = {}
    for key, writer in (
        ('market_open_monitor', write_market_open_monitor),
        ('why_no_trade', write_why_no_trade_summary),
        ('trade_alerts', write_trade_alerts),
        ('config_change_watch', write_config_change_watch),
        ('first_hour_review', write_first_hour_review),
        ('strategy_conclusion_gate', write_strategy_conclusion_gate),
        ('market_context_scoreboard', write_market_context_scoreboard),
        ('trade_thesis_timelines', write_trade_thesis_timeline_artifact),
        ('winner_quality', write_winner_quality_artifact),
        ('rule_candidate_quarantine', write_rule_candidate_quarantine),
        ('feed_health_score', write_feed_health_score),
        ('broker_execution_score', write_broker_execution_score),
        ('era_scorecard', write_era_scorecard),
        ('monday_scorecard', write_monday_scorecard),
        ('ws_scalp_replay', write_ws_scalp_replay),
        ('decision_audit_summary', write_decision_audit_summary),
        ('current_engine_regrade', write_current_engine_regrade),
        ('regime_specific_scorecards', write_regime_specific_scorecards),
        ('near_miss_winners', write_near_miss_winners),
        ('exit_quality_score', write_exit_quality_score),
        ('replay_confidence', write_replay_confidence),
        ('false_positive_negative', write_false_positive_negative_tables),
        ('daily_review_gate', write_daily_review_gate),
        ('rule_lifecycle_dashboard', write_rule_lifecycle_dashboard),
        ('now_status', write_now_status),
        ('monday_launch_checklist', write_monday_launch_checklist),
        ('config_intent_ledger', write_config_intent_ledger),
        ('world_class_dashboard', write_world_class_dashboard),
        ('monday_live_review', write_monday_live_review),
        ('review_start_here', write_review_start_here),
    ):
        path, _payload = writer(day)
        paths[key] = path
    return {'day': day, 'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'), 'paths': paths}


def main() -> int:
    ap = argparse.ArgumentParser(description='Refresh compact live review artifacts.')
    ap.add_argument('day', nargs='?', default=_today())
    ap.add_argument('--loop', action='store_true', help='Refresh until stopped.')
    ap.add_argument('--interval-sec', type=int, default=30)
    ap.add_argument('--checkpoint', action='store_true', help='Also write a session checkpoint each refresh.')
    ap.add_argument('--checkpoint-label', default='live_monitor')
    ap.add_argument('--json', action='store_true')
    args = ap.parse_args()
    while True:
        payload = write_once(args.day)
        if args.checkpoint:
            path, _ = write_session_checkpoint(args.day, label=args.checkpoint_label)
            payload['paths']['session_checkpoint'] = path
        if args.json:
            print(json.dumps(payload, indent=2, default=str))
        else:
            print(f"live monitor refreshed {payload['created_at_ct']}")
            for path in payload['paths'].values():
                print(path)
        if not args.loop:
            return 0
        time.sleep(max(5, args.interval_sec))


if __name__ == '__main__':
    raise SystemExit(main())
