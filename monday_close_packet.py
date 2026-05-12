from __future__ import annotations

from output_paths import output_path

import argparse
import json
import os
from datetime import datetime

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo


HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = output_path('postmortem')
CT = ZoneInfo('America/Chicago')


def _today() -> str:
    return datetime.now(CT).date().isoformat()


def build_close_packet(day: str, write_gdoc: bool = False, mutate_hypotheses: bool = True) -> dict:
    paths = {}
    errors = {}
    try:
        import daily_postmortem
        pm = daily_postmortem.run(
            day_iso=day,
            write_gdoc=write_gdoc,
            mutate_hypotheses=mutate_hypotheses,
            write_files=True,
            detail_refresh=True,
            quiet=True,
        )
        paths['postmortem_json'] = os.path.join(OUT_DIR, f'postmortem_{day}.json')
        paths['postmortem_txt'] = os.path.join(OUT_DIR, f'postmortem_{day}.txt')
        if isinstance(pm, dict):
            paths['daily_postmortem_return'] = pm
    except Exception as e:
        errors['daily_postmortem'] = str(e)

    if os.getenv('WRITE_LEGACY_REVIEW_ARTIFACTS') == '1':
        try:
            from review_artifacts import write_all
            review_paths, _payloads = write_all(day)
            paths.update({f'review_{k}': v for k, v in review_paths.items() if v})
        except Exception as e:
            errors['review_artifacts'] = str(e)
    else:
        paths['legacy_review_artifacts'] = 'skipped; set WRITE_LEGACY_REVIEW_ARTIFACTS=1 to rebuild archived forensic review pack'

    try:
        from weekend_readiness import (
            write_broker_execution_score,
            write_broker_safety_snapshot,
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
            write_postmarket_artifact_validation,
            write_review_start_here,
            write_order_latency_summary,
            write_market_regime_day_label,
            write_edge_quality_activity,
            write_multi_day_scorecard,
            write_strategy_operations_split,
            write_trade_alerts,
            write_why_no_trade_summary,
            write_entry_quality_tiers,
            write_loser_archetypes,
            write_winner_damage_report,
            write_clean_day_score_artifact,
            write_no_trade_opportunity_grades,
            write_thesis_failure_review,
            write_loser_replay_snapshots,
            write_per_symbol_personality,
            write_out_of_sample_scoreboard,
            write_first_hour_review,
            write_strategy_conclusion_gate,
            write_market_context_scoreboard,
            write_trade_thesis_timeline_artifact,
            write_winner_quality_artifact,
            write_rule_candidate_quarantine,
            write_rule_lifecycle_dashboard,
            write_regime_specific_scorecards,
            write_world_class_dashboard,
            write_ws_scalp_replay,
            write_near_miss_winners,
        )
        for key, fn in (
            ('why_no_trade', write_why_no_trade_summary),
            ('market_open_monitor', write_market_open_monitor),
            ('trade_alerts', write_trade_alerts),
            ('strategy_operations_split', write_strategy_operations_split),
            ('entry_quality_tiers', write_entry_quality_tiers),
            ('loser_archetypes', write_loser_archetypes),
            ('winner_damage_report', write_winner_damage_report),
            ('clean_day_score', write_clean_day_score_artifact),
            ('no_trade_opportunity_grades', write_no_trade_opportunity_grades),
            ('thesis_failure_review', write_thesis_failure_review),
            ('loser_replay_snapshots', write_loser_replay_snapshots),
            ('per_symbol_personality', write_per_symbol_personality),
            ('out_of_sample_scoreboard', write_out_of_sample_scoreboard),
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
            ('multi_day_scorecard', write_multi_day_scorecard),
            ('market_regime_day', write_market_regime_day_label),
            ('edge_quality_activity', write_edge_quality_activity),
            ('order_latency', write_order_latency_summary),
            ('review_start_here', write_review_start_here),
            ('broker_safety_snapshot', lambda d: write_broker_safety_snapshot(d, label='post_market')),
            ('postmarket_artifact_validation', write_postmarket_artifact_validation),
        ):
            path, _payload = fn(day)
            paths[key] = path
    except Exception as e:
        errors['weekend_readiness'] = str(e)

    try:
        import eod_integrity_check
        rc = eod_integrity_check.main(day)
        paths['eod_integrity_rc'] = rc
        if rc != 0:
            errors['eod_integrity_check'] = f'exit_code={rc}'
    except Exception as e:
        errors['eod_integrity_check'] = str(e)

    try:
        from promotion_review import write_promotion_review
        path, _payload = write_promotion_review(day)
        paths['promotion_review'] = path
    except Exception as e:
        errors['promotion_review'] = str(e)

    payload = {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'ok': not errors,
        'write_gdoc': write_gdoc,
        'mutate_hypotheses': mutate_hypotheses,
        'paths': paths,
        'errors': errors,
        'review_order': [
            'postmortem_txt',
            'review_review_index',
            'review_monday_review_packet',
            'review_start_here',
            'promotion_review',
            'loser_clusters',
            'loser_archetypes',
            'entry_quality_tiers',
            'winner_damage_report',
            'clean_day_score',
            'no_trade_opportunity_grades',
            'thesis_failure_review',
            'loser_replay_snapshots',
            'per_symbol_personality',
            'out_of_sample_scoreboard',
            'first_hour_review',
            'strategy_conclusion_gate',
            'market_context_scoreboard',
            'trade_thesis_timelines',
            'winner_quality',
            'rule_candidate_quarantine',
            'feed_health_score',
            'broker_execution_score',
            'era_scorecard',
            'monday_scorecard',
            'ws_scalp_replay',
            'decision_audit_summary',
            'current_engine_regrade',
            'regime_specific_scorecards',
            'near_miss_winners',
            'exit_quality_score',
            'replay_confidence',
            'false_positive_negative',
            'daily_review_gate',
            'rule_lifecycle_dashboard',
            'now_status',
            'world_class_dashboard',
            'strategy_operations_split',
            'why_no_trade',
            'postmarket_artifact_validation',
        ],
    }
    out_path = os.path.join(OUT_DIR, f'monday_close_packet_{day}.json')
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, default=str)
    payload['paths']['monday_close_packet'] = out_path
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description='Run post-market close packet workflow.')
    ap.add_argument('day', nargs='?', default=_today())
    ap.add_argument('--write-gdoc', action='store_true')
    ap.add_argument('--dry-run', action='store_true', help='Do not mutate hypothesis evidence or write Google Docs.')
    ap.add_argument('--json', action='store_true')
    args = ap.parse_args()
    payload = build_close_packet(
        args.day,
        write_gdoc=(args.write_gdoc and not args.dry_run),
        mutate_hypotheses=not args.dry_run,
    )
    if args.json:
        print(json.dumps(payload, indent=2, default=str))
    else:
        print(f"close packet ok={payload.get('ok')} day={args.day}")
        if payload.get('errors'):
            for key, err in payload['errors'].items():
                print(f"ERROR {key}: {err}")
        for key in payload.get('review_order') or []:
            value = (payload.get('paths') or {}).get(key)
            if value:
                print(value)
        packet = (payload.get('paths') or {}).get('monday_close_packet')
        if packet:
            print(packet)
    return 0 if payload.get('ok') else 1


if __name__ == '__main__':
    raise SystemExit(main())
