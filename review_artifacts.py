from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Iterable, Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo


HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, 'postmortem')
CT = ZoneInfo('America/Chicago')
_POSTMORTEM_OVERRIDES: dict[str, dict] = {}
_MISSING = object()


def _read_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return default


def _iter_jsonl(path: str) -> Iterable[dict]:
    if not os.path.exists(path):
        return
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                yield json.loads(line)
            except Exception:
                continue


def _sha256(path: str) -> Optional[str]:
    try:
        h = hashlib.sha256()
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(65536), b''):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None


def _file_info(path: str) -> dict:
    try:
        st = os.stat(path)
        return {
            'path': path,
            'exists': True,
            'bytes': st.st_size,
            'modified_at_ct': datetime.fromtimestamp(st.st_mtime, CT).isoformat(timespec='seconds'),
        }
    except Exception:
        return {'path': path, 'exists': False}


def _sum(rows: list[dict], key: str) -> float:
    return round(sum(float(r.get(key) or 0) for r in rows), 2)


def _avg(values: list[float]) -> Optional[float]:
    vals = [float(v) for v in values if v is not None]
    return round(sum(vals) / len(vals), 4) if vals else None


def _safe_pct(num: float, den: float) -> Optional[float]:
    return round(num / den * 100, 1) if den else None


def _num(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _counter(rows: list[dict], key: str, top: int = 20) -> list[dict]:
    c = Counter(str(r.get(key) if r.get(key) is not None else 'None') for r in rows)
    return [{'value': k, 'count': v} for k, v in c.most_common(top)]


def _nested(row: dict, *path: str) -> Any:
    cur = row
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def _trade_day(day: str) -> dict:
    if day in _POSTMORTEM_OVERRIDES:
        return _POSTMORTEM_OVERRIDES[day]
    return _read_json(os.path.join(OUT_DIR, f'postmortem_{day}.json'), {}) or {}


def _tape(day: str) -> list[dict]:
    return list((_trade_day(day).get('tape') or []))


def _shadow_exit_rows(day: str) -> list[dict]:
    path = os.path.join(OUT_DIR, 'shadow_exits', f'shadow_exits_{day}.jsonl')
    return list(_iter_jsonl(path))


def _entry_retry_rows(day: str) -> list[dict]:
    path = os.path.join(OUT_DIR, 'entry_retry_candidates', f'entry_retry_candidates_{day}.jsonl')
    return list(_iter_jsonl(path))


def _skipped_signal_rows(day: str) -> list[dict]:
    path = os.path.join(OUT_DIR, 'skipped_signals', f'skipped_signals_{day}.jsonl')
    return list(_iter_jsonl(path))


def _audit_rows(day: str) -> list[dict]:
    path = os.path.join(HERE, 'audit', f'trade_lifecycle_{day}.jsonl')
    return list(_iter_jsonl(path))


def _decision_audit_rows(day: str) -> list[dict]:
    path = os.path.join(OUT_DIR, 'decision_audits', f'decision_audits_{day}.jsonl')
    return list(_iter_jsonl(path))


def _near_signal_rows(day: str) -> list[dict]:
    path = os.path.join(OUT_DIR, 'near_signals', f'near_signals_{day}.jsonl')
    return list(_iter_jsonl(path))


def _rule_dry_run_rows(day: str) -> list[dict]:
    path = os.path.join(OUT_DIR, 'rule_dry_run', f'rule_dry_run_{day}.jsonl')
    return list(_iter_jsonl(path))


def _fill_attribution_rows(day: str) -> list[dict]:
    path = os.path.join(OUT_DIR, 'fill_attribution', f'fill_attribution_{day}.jsonl')
    return list(_iter_jsonl(path))


def _latency_attribution_rows(day: str) -> list[dict]:
    path = os.path.join(OUT_DIR, 'latency_attribution', f'latency_attribution_{day}.jsonl')
    return list(_iter_jsonl(path))


def _gate_timeline_rows(day: str) -> list[dict]:
    path = os.path.join(OUT_DIR, 'gate_timeline', f'gate_timeline_{day}.jsonl')
    return list(_iter_jsonl(path))


def _no_signal_snapshot_rows(day: str) -> list[dict]:
    path = os.path.join(OUT_DIR, 'no_signal_snapshots', f'no_signal_snapshots_{day}.jsonl')
    return list(_iter_jsonl(path))


def _tick_replay(day: str) -> dict:
    path = os.path.join(OUT_DIR, f'tick_replay_{day}.json')
    payload = _read_json(path, {}) or {}
    if payload:
        return payload
    try:
        from tick_replay import build_tick_replay
        return build_tick_replay(day)
    except Exception as e:
        return {'error': str(e)}


def _manifest_paths(day: str) -> list[str]:
    return [
        os.path.join(OUT_DIR, f'postmortem_{day}.json'),
        os.path.join(OUT_DIR, f'postmortem_{day}.txt'),
        os.path.join(OUT_DIR, f'review_index_{day}.json'),
        os.path.join(OUT_DIR, f'loser_summary_{day}.json'),
        os.path.join(OUT_DIR, f'risk_summary_{day}.json'),
        os.path.join(OUT_DIR, f'execution_summary_{day}.json'),
        os.path.join(OUT_DIR, f'feature_coverage_{day}.json'),
        os.path.join(OUT_DIR, f'data_collection_coverage_{day}.json'),
        os.path.join(OUT_DIR, f'gate_timeline_summary_{day}.json'),
        os.path.join(OUT_DIR, f'no_signal_snapshot_summary_{day}.json'),
        os.path.join(OUT_DIR, f'fill_attribution_summary_{day}.json'),
        os.path.join(OUT_DIR, f'latency_attribution_summary_{day}.json'),
        os.path.join(OUT_DIR, f'entry_timing_efficiency_{day}.json'),
        os.path.join(OUT_DIR, f'dual_side_shadow_review_{day}.json'),
        os.path.join(OUT_DIR, f'quote_condition_quality_{day}.json'),
        os.path.join(OUT_DIR, f'shadow_entry_variants_{day}.json'),
        os.path.join(OUT_DIR, 'ARTIFACT_SCHEMA_DICTIONARY.json'),
        os.path.join(OUT_DIR, f'new_gate_attribution_{day}.json'),
        os.path.join(OUT_DIR, f'shadow_exit_summary_{day}.json'),
        os.path.join(OUT_DIR, f'entry_retry_summary_{day}.json'),
        os.path.join(OUT_DIR, f'exit_hierarchy_summary_{day}.json'),
        os.path.join(OUT_DIR, f'per_ticker_learning_{day}.json'),
        os.path.join(OUT_DIR, f'regime_scoring_review_{day}.json'),
        os.path.join(OUT_DIR, f'config_diff_{day}.json'),
        os.path.join(OUT_DIR, f'market_tape_attribution_{day}.json'),
        os.path.join(OUT_DIR, f'setup_grade_review_{day}_10d.json'),
        os.path.join(OUT_DIR, f'counterfactual_side_review_{day}.json'),
        os.path.join(OUT_DIR, f'rule_attribution_review_{day}_10d.json'),
        os.path.join(OUT_DIR, f'hypothesis_confidence_{day}.json'),
        os.path.join(OUT_DIR, f'daily_conclusion_ledger_{day}.json'),
        os.path.join(OUT_DIR, 'learning_conclusion_ledger.json'),
        os.path.join(OUT_DIR, f'matched_control_review_{day}_10d.json'),
        os.path.join(OUT_DIR, f'exit_efficiency_review_{day}.json'),
        os.path.join(OUT_DIR, f'mae_mfe_timeline_{day}.json'),
        os.path.join(OUT_DIR, f'hypothesis_counterexamples_{day}.json'),
        os.path.join(OUT_DIR, f'loser_fingerprints_{day}.json'),
        os.path.join(OUT_DIR, f'postmortem_section_confidence_{day}.json'),
        os.path.join(OUT_DIR, f'trade_verdicts_{day}.json'),
        os.path.join(OUT_DIR, f'best_avoided_loser_simulator_{day}.json'),
        os.path.join(OUT_DIR, f'rolling_learning_dashboard_{day}_10d.json'),
        os.path.join(OUT_DIR, f'true_counterfactual_replay_{day}_10d.json'),
        os.path.join(OUT_DIR, f'experiment_registry_snapshot_{day}.json'),
        os.path.join(OUT_DIR, 'experiment_registry.json'),
        os.path.join(OUT_DIR, f'feature_outcome_table_{day}_10d.json'),
        os.path.join(OUT_DIR, f'rule_confidence_decay_{day}.json'),
        os.path.join(OUT_DIR, f'human_feedback_summary_{day}.json'),
        os.path.join(OUT_DIR, f'human_feedback_{day}.json'),
        os.path.join(OUT_DIR, 'human_feedback.json'),
        os.path.join(OUT_DIR, f'weekly_promotion_meeting_{day}_10d.json'),
        os.path.join(OUT_DIR, f'market_regime_library_{day}_20d.json'),
        os.path.join(OUT_DIR, f'pre_market_checklist_{day}.json'),
        os.path.join(OUT_DIR, f'config_changes_{day}.json'),
        os.path.join(OUT_DIR, f'config_freeze_{day}_pre_open.json'),
        os.path.join(OUT_DIR, f'config_change_watch_{day}.json'),
        os.path.join(OUT_DIR, f'trade_alerts_{day}.json'),
        os.path.join(OUT_DIR, f'DAILY_REVIEW_START_HERE_{day}.json'),
        os.path.join(OUT_DIR, f'loser_clusters_{day}.json'),
        os.path.join(OUT_DIR, f'loser_archetypes_{day}.json'),
        os.path.join(OUT_DIR, f'entry_quality_tiers_{day}.json'),
        os.path.join(OUT_DIR, f'winner_damage_report_{day}.json'),
        os.path.join(OUT_DIR, f'clean_day_score_{day}.json'),
        os.path.join(OUT_DIR, f'no_trade_opportunity_grades_{day}.json'),
        os.path.join(OUT_DIR, f'thesis_failure_review_{day}.json'),
        os.path.join(OUT_DIR, f'loser_replay_snapshots_{day}.json'),
        os.path.join(OUT_DIR, f'per_symbol_personality_{day}_10d.json'),
        os.path.join(OUT_DIR, f'out_of_sample_scoreboard_{day}.json'),
        os.path.join(OUT_DIR, f'first_hour_review_{day}.json'),
        os.path.join(OUT_DIR, f'strategy_conclusion_gate_{day}.json'),
        os.path.join(OUT_DIR, f'market_context_scoreboard_{day}.json'),
        os.path.join(OUT_DIR, f'trade_thesis_timelines_{day}.json'),
        os.path.join(OUT_DIR, f'winner_quality_{day}.json'),
        os.path.join(OUT_DIR, f'rule_candidate_quarantine_{day}.json'),
        os.path.join(OUT_DIR, f'feed_health_score_{day}.json'),
        os.path.join(OUT_DIR, f'broker_execution_score_{day}.json'),
        os.path.join(OUT_DIR, f'era_scorecard_{day}.json'),
        os.path.join(OUT_DIR, f'monday_scorecard_{day}.json'),
        os.path.join(OUT_DIR, f'ws_scalp_replay_{day}.json'),
        os.path.join(OUT_DIR, f'tick_replay_{day}.json'),
        os.path.join(OUT_DIR, f'decision_audit_summary_{day}.json'),
        os.path.join(OUT_DIR, f'current_engine_regrade_{day}.json'),
        os.path.join(OUT_DIR, f'regime_specific_scorecards_{day}.json'),
        os.path.join(OUT_DIR, f'near_miss_winners_{day}.json'),
        os.path.join(OUT_DIR, f'exit_quality_score_{day}.json'),
        os.path.join(OUT_DIR, f'replay_confidence_{day}.json'),
        os.path.join(OUT_DIR, f'false_positive_negative_{day}.json'),
        os.path.join(OUT_DIR, f'daily_review_gate_{day}.json'),
        os.path.join(OUT_DIR, f'rule_lifecycle_dashboard_{day}.json'),
        os.path.join(OUT_DIR, f'NOW_STATUS_{day}.json'),
        os.path.join(OUT_DIR, 'NOW_STATUS.json'),
        os.path.join(OUT_DIR, f'world_class_dashboard_{day}.json'),
        os.path.join(OUT_DIR, f'strategy_operations_split_{day}.json'),
        os.path.join(OUT_DIR, f'multi_day_scorecard_{day}_10d.json'),
        os.path.join(OUT_DIR, f'market_regime_day_{day}.json'),
        os.path.join(OUT_DIR, f'edge_quality_activity_{day}_10d.json'),
        os.path.join(OUT_DIR, f'order_latency_{day}.json'),
        os.path.join(OUT_DIR, f'rule_dry_run_scoreboard_{day}.json'),
        os.path.join(OUT_DIR, f'execution_context_summary_{day}.json'),
        os.path.join(OUT_DIR, f'retention_plan_{day}.json'),
        os.path.join(OUT_DIR, f'promotion_review_{day}.json'),
        os.path.join(OUT_DIR, f'why_no_trade_{day}.json'),
        os.path.join(OUT_DIR, f'market_open_monitor_{day}.json'),
        os.path.join(OUT_DIR, f'session_checkpoint_{day}_manual.json'),
        os.path.join(OUT_DIR, f'MONDAY_REVIEW_START_HERE_{day}.json'),
        os.path.join(OUT_DIR, f'notes_{day}.json'),
        os.path.join(OUT_DIR, f'current_config_replay_{day}.json'),
        os.path.join(OUT_DIR, f'monday_live_review_{day}.json'),
        os.path.join(OUT_DIR, f'postmarket_artifact_validation_{day}.json'),
        os.path.join(OUT_DIR, 'change_impact_ledger.json'),
        os.path.join(OUT_DIR, 'config_intent_ledger.json'),
        os.path.join(OUT_DIR, 'promotion_queue.json'),
        os.path.join(OUT_DIR, f'artifacts_{day}.json'),
        os.path.join(OUT_DIR, f'monday_review_packet_{day}.json'),
        os.path.join(OUT_DIR, f'engine_scoreboard_{day}.json'),
        os.path.join(OUT_DIR, f'engine_validation_{day}.json'),
        os.path.join(OUT_DIR, f'engine_validation_rolling_{day}_5d.json'),
        os.path.join(OUT_DIR, f'exit_policy_replay_{day}.json'),
        os.path.join(OUT_DIR, f'exit_policy_replay_rolling_{day}_5d.json'),
        os.path.join(OUT_DIR, f'exit_policy_candidate_config_{day}.json'),
        os.path.join(OUT_DIR, f'ev_table_{day}.json'),
        os.path.join(HERE, 'audit', f'trade_lifecycle_{day}.jsonl'),
        os.path.join(OUT_DIR, 'skipped_signals', f'skipped_signals_{day}.jsonl'),
        os.path.join(OUT_DIR, 'shadow_decisions', f'shadow_decisions_{day}.jsonl'),
        os.path.join(OUT_DIR, 'shadow_exits', f'shadow_exits_{day}.jsonl'),
        os.path.join(OUT_DIR, 'entry_retry_candidates', f'entry_retry_candidates_{day}.jsonl'),
        os.path.join(OUT_DIR, 'near_signals', f'near_signals_{day}.jsonl'),
        os.path.join(OUT_DIR, 'health_heartbeats', f'health_heartbeats_{day}.jsonl'),
    ]


def build_risk_summary(day: str) -> dict:
    tape = _tape(day)
    winners = [t for t in tape if (t.get('pnl') or 0) > 0]
    losers = [t for t in tape if (t.get('pnl') or 0) < 0]
    balance = 0.0
    max_drawdown = 0.0
    peak = 0.0
    for t in sorted(tape, key=lambda r: r.get('closed_at') or 0):
        balance += float(t.get('pnl') or 0)
        peak = max(peak, balance)
        max_drawdown = min(max_drawdown, balance - peak)
    def grouped(key: str) -> list[dict]:
        acc = defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0})
        for t in tape:
            name = str(t.get(key) or 'unknown')
            acc[name]['trades'] += 1
            acc[name]['wins'] += 1 if (t.get('pnl') or 0) > 0 else 0
            acc[name]['losses'] += 1 if (t.get('pnl') or 0) < 0 else 0
            acc[name]['pnl'] += float(t.get('pnl') or 0)
        return [
            {**v, 'key': k, 'pnl': round(v['pnl'], 2),
             'win_rate': _safe_pct(v['wins'], v['trades'])}
            for k, v in sorted(acc.items(), key=lambda kv: kv[1]['pnl'])
        ]
    gross_win = _sum(winners, 'pnl')
    gross_loss = abs(_sum(losers, 'pnl'))
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'trades': len(tape),
        'wins': len(winners),
        'losses': len(losers),
        'win_rate': _safe_pct(len(winners), len(tape)),
        'pnl': round(gross_win - gross_loss, 2),
        'gross_win': gross_win,
        'gross_loss': round(gross_loss, 2),
        'profit_factor': round(gross_win / gross_loss, 3) if gross_loss else None,
        'avg_win': _avg([t.get('pnl') for t in winners]),
        'avg_loss': _avg([t.get('pnl') for t in losers]),
        'largest_win': max([t.get('pnl') for t in winners], default=None),
        'largest_loss': min([t.get('pnl') for t in losers], default=None),
        'max_intraday_drawdown': round(max_drawdown, 2),
        'by_side': grouped('side'),
        'by_ticker': grouped('ticker'),
        'by_setup': grouped('setup_type'),
        'by_exit_reason': grouped('reason'),
        'loss_concentration': [
            {
                'ticker': t.get('ticker'), 'side': t.get('side'), 'time': t.get('time'),
                'pnl': t.get('pnl'), 'reason': t.get('reason'),
                'setup_type': t.get('setup_type'),
            }
            for t in sorted(losers, key=lambda r: r.get('pnl') or 0)[:5]
        ],
    }


def build_loser_summary(day: str) -> dict:
    pm = _trade_day(day)
    tape = pm.get('tape') or []
    review = {r.get('trade_id'): r for r in pm.get('loser_decision_review', []) if r.get('trade_id')}
    losers = []
    for t in tape:
        if (t.get('pnl') or 0) >= 0:
            continue
        r = review.get(t.get('trade_id')) or {}
        ind = t.get('indicators') or t.get('ind') or {}
        btc = t.get('btc_indicators') or t.get('btc') or {}
        forensics = t.get('forensics') or {}
        entry_thesis = t.get('entry_thesis') or {}
        decision_quality = t.get('decision_quality') or {}
        flow30 = ind.get('flow_30s') or {}
        flow120 = ind.get('flow_120s') or {}
        setup_type = (
            t.get('setup_type') or t.get('setup')
            or forensics.get('setup_type')
            or entry_thesis.get('setup_type')
            or _nested(forensics, 'signal', 'setup_type')
        )
        score = (
            t.get('entry_score')
            or _nested(forensics, 'signal', 'score')
            or _nested(entry_thesis, 'signal', 'score')
        )
        primary_cause = (
            t.get('primary_loss_cause') or r.get('label')
            or decision_quality.get('label')
            or decision_quality.get('primary')
        )
        grade = r.get('grade') or decision_quality.get('grade')
        losers.append({
            'trade_id': t.get('trade_id'),
            'time': t.get('time'),
            'ticker': t.get('ticker'),
            'side': t.get('side'),
            'setup_type': setup_type,
            'score': score,
            'pnl': t.get('pnl'),
            'reason': t.get('reason'),
            'primary_cause': primary_cause,
            'grade': grade,
            'prevention_candidates': (
                r.get('preventers') or t.get('prevention_candidates')
                or decision_quality.get('preventers')
                or decision_quality.get('prevention_candidates')
            ),
            'indicators': {
                'mom_15s': ind.get('mom_15s'),
                'mom_60s': ind.get('mom_60s'),
                'ema_stack': ind.get('ema_stack'),
                'vwap_dist_sigma': ind.get('vwap_dist_sigma'),
                'flow_30s_buy_pct': flow30.get('buy_pct'),
                'flow_120s_buy_pct': flow120.get('buy_pct'),
                'spread_pct': (
                    (t.get('execution_quality') or t.get('entry_quality') or {}).get('spread_pct')
                    or _nested(t, 'forensics', 'entry_quality', 'spread_pct')
                    or _nested(t, 'entry_thesis', 'execution_quality', 'spread_pct')
                ),
                'quote_age_sec': (
                    (t.get('execution_quality') or t.get('entry_quality') or {}).get('quote_age_sec')
                    or _nested(t, 'forensics', 'entry_quality', 'quote_age_sec')
                    or _nested(t, 'entry_thesis', 'execution_quality', 'quote_age_sec')
                ),
            },
            'btc': {
                'regime': btc.get('regime'),
                'stack': btc.get('stack') or btc.get('ema_stack'),
                'mom_15s': btc.get('mom_15s'),
                'mom_60s': btc.get('mom_60s'),
                'stale': btc.get('stale'),
            },
            'path': {
                'mfe_pct': t.get('mfe_pct'),
                'mae_pct': t.get('mae_pct'),
                'duration_min': t.get('duration_min'),
            },
        })
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'losers': losers,
        'primary_causes': _counter(losers, 'primary_cause'),
        'largest_damage_contributors': sorted(losers, key=lambda r: r.get('pnl') or 0)[:5],
    }


def build_execution_summary(day: str) -> dict:
    tape = _tape(day)
    audits = _audit_rows(day)
    spreads = []
    quote_ages = []
    slippage = []
    stale_events = []
    for t in tape:
        ex = t.get('execution_quality') or t.get('entry_quality') or {}
        spread = (
            ex.get('spread_pct')
            or _nested(t, 'forensics', 'entry_quality', 'spread_pct')
            or _nested(t, 'entry_thesis', 'execution_quality', 'spread_pct')
        )
        quote_age = (
            ex.get('quote_age_sec')
            or _nested(t, 'forensics', 'entry_quality', 'quote_age_sec')
            or _nested(t, 'entry_thesis', 'execution_quality', 'quote_age_sec')
        )
        slip = t.get('entry_fill_slippage_pct')
        if slip is None:
            slip = t.get('entry_slippage_pct')
        if spread is not None:
            spreads.append(float(spread))
        if quote_age is not None:
            quote_ages.append(float(quote_age))
        if slip is not None:
            slippage.append(float(slip))
        if (t.get('btc_indicators') or t.get('btc') or {}).get('stale'):
            stale_events.append({'ticker': t.get('ticker'), 'time': t.get('time'), 'kind': 'btc_stale'})
    audit_counts = Counter(a.get('event') for a in audits)
    broker_issues = [
        a for a in audits
        if str(a.get('event') or '').startswith('broker_')
        or 'flat' in str(a.get('event') or '')
        or 'exposure' in str(a.get('event') or '')
    ]
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'spread': {'avg_pct': _avg(spreads), 'max_pct': max(spreads, default=None), 'samples': len(spreads)},
        'quote_age': {'avg_sec': _avg(quote_ages), 'max_sec': max(quote_ages, default=None), 'samples': len(quote_ages)},
        'slippage': {'avg_pct': _avg(slippage), 'max_pct': max(slippage, default=None), 'samples': len(slippage)},
        'pre_submit_blocks': audit_counts.get('entry_pre_submit_blocked', 0),
        'bad_fill_or_slippage_trades': [
            {
                'ticker': t.get('ticker'), 'side': t.get('side'), 'time': t.get('time'),
                'pnl': t.get('pnl'),
                'slippage_pct': t.get('entry_fill_slippage_pct') or t.get('entry_slippage_pct'),
            }
            for t in tape
            if abs(float(t.get('entry_fill_slippage_pct') or t.get('entry_slippage_pct') or 0)) >= 0.10
        ],
        'stale_data_incidents': {
            'count': len(stale_events),
            'examples': stale_events[:10],
        },
        'broker_safety': {
            'event_counts': dict(audit_counts),
            'issue_count': len(broker_issues),
            'recent_issues': broker_issues[-10:],
        },
    }


def build_feature_coverage(day: str) -> dict:
    tape = _tape(day)
    total = len(tape)
    def present(fn):
        return sum(1 for t in tape if fn(t))

    def broker_entry_order_present(t: dict) -> bool:
        candidates = (
            t.get('broker_entry_order_id'),
            t.get('alpaca_order_id'),
            _nested(t, 'broker_entry', 'order_id'),
            _nested(t, 'broker_entry', 'id'),
            _nested(t, 'broker_order', 'id'),
            _nested(t, 'pending_entry', 'alpaca_order_id'),
            _nested(t, 'forensics', 'broker_entry_order_id'),
            _nested(t, 'entry_thesis', 'broker_entry_order_id'),
            _nested(t, 'entry_thesis', 'alpaca_order_id'),
            _nested(t, 'entry_thesis', 'decision_audit', 'broker_entry_order_id'),
        )
        return any(bool(v) for v in candidates)

    checks = {
        'btc_context': present(lambda t: bool(t.get('btc_indicators') or t.get('btc') or _nested(t, 'forensics', 'btc'))),
        'entry_spread': present(lambda t: _nested(t, 'forensics', 'entry_quality', 'spread_pct') is not None),
        'quote_age': present(lambda t: _nested(t, 'forensics', 'entry_quality', 'quote_age_sec') is not None),
        'broker_entry_order': present(broker_entry_order_present),
        'broker_exit_fill': present(lambda t: bool(t.get('broker_exit_fill_price') or _nested(t, 'broker_close', 'fill', 'filled_avg_price'))),
        'tick_path': present(lambda t: bool(t.get('path'))),
        'forward_returns': present(lambda t: any(t.get(k) is not None for k in ('fwd5', 'fwd15', 'fwd60', 'fwd_5m', 'fwd_15m'))),
        'decision_quality': present(lambda t: bool(t.get('decision_quality'))),
    }
    notes = []
    if total and checks['broker_entry_order'] == 0:
        notes.append(
            'No broker entry order ids were found in recognized fields. '
            'For old days this can be a legacy-data limitation; new trades should populate alpaca_order_id/entry_thesis.'
        )
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'total_trades': total,
        'coverage': {
            key: {
                'count': value,
                'pct': _safe_pct(value, total),
            }
            for key, value in checks.items()
        },
        'notes': notes,
    }


def build_data_collection_coverage(day: str) -> dict:
    tape = _tape(day)
    total = len(tape)
    shorts = [t for t in tape if t.get('side') == 'SHORT']
    skipped = _skipped_signal_rows(day)
    near = _near_signal_rows(day)
    audits = _audit_rows(day)
    decision_audits = _decision_audit_rows(day)
    rule_dry = _rule_dry_run_rows(day)
    fill_attr = _fill_attribution_rows(day)
    latency_attr = _latency_attribution_rows(day)
    gate_timeline = _gate_timeline_rows(day)
    no_signal_snapshots = _no_signal_snapshot_rows(day)
    tick_replay = _tick_replay(day)
    tick_summary = tick_replay.get('summary') or {}
    capture_counts = tick_summary.get('capture_type_counts') or {}
    tick_rows = tick_replay.get('rows') or []
    advanced_intro = '2026-05-04'
    field_introduced_on = {
        'market_microstructure': advanced_intro,
        'liquidity_impact': advanced_intro,
        'gap_context': advanced_intro,
        'btc_proxy_basket': advanced_intro,
        'catalyst_context': advanced_intro,
        'short_borrow_snapshot': advanced_intro,
        'transaction_cost_estimate': advanced_intro,
        'broker_entry_order_id': advanced_intro,
        'decision_audit_rows': advanced_intro,
        'rule_dry_run_entered': advanced_intro,
        'rule_dry_run_closed': advanced_intro,
        'market_condition_metadata': advanced_intro,
        'fill_vs_quote_attribution': advanced_intro,
        'latency_attribution': advanced_intro,
        'entry_timing_efficiency': advanced_intro,
        'dual_side_shadow_score': advanced_intro,
        'quote_condition_quality': advanced_intro,
        'gate_timeline': advanced_intro,
        'no_signal_snapshots': advanced_intro,
    }

    def has_trade_field(t: dict, *paths: tuple[str, ...]) -> bool:
        for path in paths:
            if len(path) == 1:
                value = t.get(path[0])
            else:
                value = _nested(t, *path)
            if value not in (None, '', {}, []):
                return True
        return False

    def count_trades(*paths: tuple[str, ...]) -> int:
        return sum(1 for t in tape if has_trade_field(t, *paths))

    def item(name: str, count: int, expected: int, min_pct: float = 95.0,
             severity: str = 'required', note: str = '',
             introduced_on: Optional[str] = None) -> dict:
        pct = _safe_pct(count, expected)
        intro = introduced_on or field_introduced_on.get(name)
        if intro and day < intro:
            status = 'pre_field_not_expected'
        elif expected <= 0:
            status = 'not_applicable'
        elif pct is not None and pct >= min_pct:
            status = 'ok'
        elif count > 0:
            status = 'partial'
        else:
            status = 'missing'
        return {
            'name': name,
            'status': status,
            'count': count,
            'expected': expected,
            'pct': pct,
            'min_pct': min_pct if expected > 0 and status != 'pre_field_not_expected' else None,
            'severity': severity,
            'introduced_on': intro,
            'note': note,
        }

    rows = [
        item(
            'entry_forensics',
            count_trades(('forensics',), ('entry_thesis', 'decision_audit', 'forensics')),
            total,
            note='Main entry thesis and indicator snapshot attached to closed trades.',
        ),
        item(
            'stock_indicators',
            count_trades(('indicators',), ('forensics', 'signal_quality'), ('entry_thesis', 'decision_audit', 'indicators')),
            total,
            note='Stock-side indicator snapshot exists for trade review.',
        ),
        item(
            'btc_context',
            count_trades(('btc_indicators',), ('forensics', 'btc'), ('entry_thesis', 'btc_context')),
            total,
            note='BTC context is present for the miner/BTC correlation thesis.',
        ),
        item(
            'execution_spread',
            count_trades(('forensics', 'entry_quality', 'spread_pct'), ('liquidity_impact_at_entry', 'spread_pct')),
            total,
            note='Entry spread captured for execution-quality diagnosis.',
        ),
        item(
            'quote_age',
            count_trades(('forensics', 'entry_quality', 'quote_age_sec'), ('market_microstructure_at_entry', 'quote_age_sec')),
            total,
            note='Quote freshness captured so stale-tape conclusions can be trusted.',
        ),
        item(
            'broker_entry_order_id',
            count_trades(('broker_entry_order_id',), ('alpaca_order_id',), ('entry_thesis', 'client_order_id')),
            total,
            min_pct=90.0,
            note='Broker/order id linkage for reconciliation and fill audit.',
        ),
        item(
            'broker_exit_fill',
            count_trades(('broker_exit_fill_price',), ('broker_close', 'fill', 'filled_avg_price')),
            total,
            min_pct=85.0,
            note='Actual broker close fill attached where available.',
        ),
        item(
            'market_microstructure',
            count_trades(('market_microstructure_at_entry',), ('forensics', 'market_microstructure')),
            total,
            note='Halt/LULD-like/SSR/freshness context exists.',
        ),
        item(
            'liquidity_impact',
            count_trades(('liquidity_impact_at_entry',), ('forensics', 'liquidity_impact')),
            total,
            note='Top-of-book liquidity/impact context exists.',
        ),
        item(
            'gap_context',
            count_trades(('gap_context_at_entry',), ('forensics', 'gap_context')),
            total,
            min_pct=90.0,
            note='Prior-close/gap context exists where daily bar cache was warm.',
        ),
        item(
            'btc_proxy_basket',
            count_trades(('btc_proxy_basket_at_entry',), ('forensics', 'btc_proxy_basket')),
            total,
            min_pct=90.0,
            note='MSTR/COIN/IBIT proxy context exists.',
        ),
        item(
            'catalyst_context',
            count_trades(('catalyst_context_at_entry',), ('forensics', 'catalyst_context')),
            total,
            min_pct=90.0,
            severity='watch',
            note='Manual catalyst flag file was read; empty catalyst list is still valid coverage.',
        ),
        item(
            'short_borrow_snapshot',
            sum(1 for t in shorts if has_trade_field(t, ('short_borrow_snapshot',), ('forensics', 'short_borrow_snapshot'))),
            len(shorts),
            min_pct=90.0,
            note='Shortable/easy-to-borrow snapshot captured for SHORT entries.',
        ),
        item(
            'transaction_cost_estimate',
            count_trades(('estimated_transaction_costs',), ('net_pnl_after_estimated_costs',)),
            total,
            min_pct=90.0,
            severity='watch',
            note='Config-based fee/friction estimate attached to closed trades.',
        ),
        item(
            'market_condition_metadata',
            count_trades(
                ('forensics', 'entry_quality', 'last_trade_exchange'),
                ('forensics', 'entry_quality', 'last_quote_bid_exchange'),
                ('forensics', 'entry_quality', 'last_quote_ask_exchange'),
            ) or sum(
                1 for r in tick_rows
                if _nested(r, 'market_metadata', 'stock', 'condition_metadata_present')
            ),
            total,
            min_pct=85.0,
            severity='watch',
            note='Trade/quote exchange, tape, and condition metadata captured for execution diagnosis.',
        ),
        item(
            'fill_vs_quote_attribution',
            count_trades(('fill_attribution',), ('forensics', 'fill_attribution'))
            or len({r.get('trade_id') for r in fill_attr if r.get('trade_id')}),
            total,
            min_pct=90.0,
            note='Signal/pre-submit/submit/fill snapshots exist for fill-vs-quote attribution.',
        ),
        item(
            'latency_attribution',
            count_trades(('latency_attribution',), ('latency_chain',), ('forensics', 'latency_attribution'))
            or len({r.get('trade_id') for r in latency_attr if r.get('trade_id')}),
            total,
            min_pct=90.0,
            note='Signal/pre-submit/broker/commit/fill latency chain exists for execution-delay attribution.',
        ),
        item(
            'entry_timing_efficiency',
            count_trades(('entry_timing',)),
            total,
            min_pct=90.0,
            note='Actual entry is compared with best/worst prices after entry for timing-efficiency analysis.',
        ),
        item(
            'dual_side_shadow_score',
            count_trades(
                ('entry_thesis', 'shadow_dual_side_score'),
                ('forensics', 'shadow_dual_side_score'),
                ('decision_audit', 'shadow_dual_side_score'),
            ) or sum(1 for r in gate_timeline + no_signal_snapshots if r.get('shadow_dual_side_score')),
            total if total else (1 if gate_timeline or no_signal_snapshots else 0),
            min_pct=85.0,
            severity='watch',
            note='Passive LONG-vs-SHORT scoring exists for side-selection review.',
        ),
        item(
            'quote_condition_quality',
            count_trades(
                ('forensics', 'entry_quality', 'quote_state'),
                ('forensics', 'entry_quality', 'condition_quality'),
            ) or sum(1 for r in fill_attr if r.get('condition_quality') or _nested(r, 'quote', 'quote_state')),
            total,
            min_pct=85.0,
            severity='watch',
            note='Quote-state and condition-code quality context exists for losers/execution review.',
        ),
        item(
            'decision_audit_rows',
            len(decision_audits),
            total,
            min_pct=90.0,
            note='Append-only decision audit rows exist for entered trades.',
        ),
        item(
            'rule_dry_run_entered',
            sum(1 for r in rule_dry if r.get('event') == 'signal_entered'),
            total,
            min_pct=90.0,
            note='Candidate rule scoreboard saw entries live.',
        ),
        item(
            'rule_dry_run_closed',
            sum(1 for r in rule_dry if r.get('event') == 'trade_closed_outcome'),
            total,
            min_pct=90.0,
            note='Candidate rule scoreboard got outcomes for closed trades.',
        ),
        item(
            'tick_entry_captures',
            int(capture_counts.get('entry') or 0),
            total,
            min_pct=80.0,
            severity='watch',
            note='Tick-level entry capture exists for replay.',
        ),
        item(
            'tick_exit_captures',
            int(capture_counts.get('exit') or 0),
            total,
            min_pct=70.0,
            severity='watch',
            note='Tick-level exit capture exists for exit-quality replay.',
        ),
        item(
            'skipped_signal_corpus',
            len(skipped),
            1 if (total or skipped or near) else 0,
            min_pct=100.0,
            severity='watch',
            note='Skipped signals were logged for no-trade and false-negative analysis.',
        ),
        item(
            'near_signal_corpus',
            len(near),
            1 if (total or skipped or near) else 0,
            min_pct=100.0,
            severity='watch',
            note='Near-miss signals were logged for threshold and missed-opportunity analysis.',
        ),
        item(
            'health_heartbeat_or_audit',
            len(audits),
            1 if (total or skipped or near) else 0,
            min_pct=100.0,
            severity='watch',
            note='Lifecycle/audit trail exists for ops review.',
        ),
        item(
            'gate_timeline',
            len(gate_timeline),
            1 if (total or skipped or near or gate_timeline or no_signal_snapshots) else 0,
            min_pct=100.0,
            severity='watch',
            note='Per-ticker gate timeline exists for why-no-trade and false-negative review.',
        ),
        item(
            'no_signal_snapshots',
            len(no_signal_snapshots),
            1 if (total or skipped or near or gate_timeline or no_signal_snapshots) else 0,
            min_pct=100.0,
            severity='watch',
            note='Periodic no-signal snapshots exist, including quiet periods below near-signal thresholds.',
        ),
    ]
    required = [r for r in rows if r.get('severity') == 'required' and r.get('expected', 0) > 0]
    missing_required = [r for r in required if r.get('status') == 'missing']
    partial_required = [r for r in required if r.get('status') == 'partial']
    watch_gaps = [r for r in rows if r.get('severity') == 'watch' and r.get('status') in ('missing', 'partial')]
    if total == 0:
        verdict = 'no_trades_yet'
        trust_impact = 'No closed trades; coverage gate will grade trade fields after entries occur.'
    elif missing_required:
        verdict = 'insufficient_data_coverage'
        trust_impact = 'Do not promote strategy changes from this day until missing required fields are explained.'
    elif partial_required:
        verdict = 'usable_with_required_gaps'
        trust_impact = 'Use conclusions carefully; required fields are present only on part of the day.'
    elif watch_gaps:
        verdict = 'usable_with_watch_gaps'
        trust_impact = 'Core trade data is usable; some advanced/forensic fields need attention.'
    else:
        verdict = 'complete'
        trust_impact = 'Data collection is complete enough for normal postmortem learning gates.'
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'verdict': verdict,
        'trust_impact': trust_impact,
        'total_trades': total,
        'short_trades': len(shorts),
        'skipped_signals': len(skipped),
        'near_signals': len(near),
        'decision_audit_rows': len(decision_audits),
        'rule_dry_run_rows': len(rule_dry),
        'fill_attribution_rows': len(fill_attr),
        'latency_attribution_rows': len(latency_attr),
        'gate_timeline_rows': len(gate_timeline),
        'no_signal_snapshot_rows': len(no_signal_snapshots),
        'tick_capture_type_counts': capture_counts,
        'coverage_rows': rows,
        'missing_required': missing_required,
        'partial_required': partial_required,
        'watch_gaps': watch_gaps,
        'files': {
            'postmortem_json': os.path.join(OUT_DIR, f'postmortem_{day}.json'),
            'decision_audits': os.path.join(OUT_DIR, 'decision_audits', f'decision_audits_{day}.jsonl'),
            'rule_dry_run': os.path.join(OUT_DIR, 'rule_dry_run', f'rule_dry_run_{day}.jsonl'),
            'fill_attribution': os.path.join(OUT_DIR, 'fill_attribution', f'fill_attribution_{day}.jsonl'),
            'latency_attribution': os.path.join(OUT_DIR, 'latency_attribution', f'latency_attribution_{day}.jsonl'),
            'gate_timeline': os.path.join(OUT_DIR, 'gate_timeline', f'gate_timeline_{day}.jsonl'),
            'no_signal_snapshots': os.path.join(OUT_DIR, 'no_signal_snapshots', f'no_signal_snapshots_{day}.jsonl'),
            'tick_replay': os.path.join(OUT_DIR, f'tick_replay_{day}.json'),
            'skipped_signals': os.path.join(OUT_DIR, 'skipped_signals', f'skipped_signals_{day}.jsonl'),
            'near_signals': os.path.join(OUT_DIR, 'near_signals', f'near_signals_{day}.jsonl'),
        },
        'purpose': (
            'Referee layer for postmortem conclusions. It grades whether each data family was '
            'captured before the bot/human trusts loser diagnoses or candidate rule promotions.'
        ),
    }


def build_gate_timeline_summary(day: str) -> dict:
    rows = _gate_timeline_rows(day)
    by_ticker: dict[str, dict] = {}
    blocker_counts = Counter()
    stage_counts = Counter()
    for row in rows:
        tkr = str(row.get('ticker') or 'unknown')
        bucket = by_ticker.setdefault(tkr, {
            'rows': 0,
            'stage_counts': Counter(),
            'blocked_by': Counter(),
            'last_snapshot': None,
        })
        bucket['rows'] += 1
        bucket['stage_counts'][str(row.get('stage') or 'unknown')] += 1
        stage_counts[str(row.get('stage') or 'unknown')] += 1
        for blocker in row.get('blocked_by') or []:
            blocker_counts[str(blocker)] += 1
            bucket['blocked_by'][str(blocker)] += 1
        bucket['last_snapshot'] = {
            'created_at_ct': row.get('created_at_ct') or row.get('created_at_et'),
            'stage': row.get('stage'),
            'price': row.get('price'),
            'blocked_by': row.get('blocked_by') or [],
            'gates': row.get('gates') or {},
        }
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'rows': len(rows),
        'stage_counts': dict(stage_counts),
        'top_blockers': [{'blocker': k, 'count': v} for k, v in blocker_counts.most_common(20)],
        'by_ticker': {
            tkr: {
                'rows': bucket['rows'],
                'stage_counts': dict(bucket['stage_counts']),
                'top_blockers': [
                    {'blocker': k, 'count': v}
                    for k, v in bucket['blocked_by'].most_common(12)
                ],
                'last_snapshot': bucket['last_snapshot'],
            }
            for tkr, bucket in sorted(by_ticker.items())
        },
        'source_file': os.path.join(OUT_DIR, 'gate_timeline', f'gate_timeline_{day}.jsonl'),
        'deduction': (
            'Use this to explain quiet periods and no-trade sessions by gate state, not memory. '
            'It is intentionally compact and ticker-level.'
        ),
    }


def build_no_signal_snapshot_summary(day: str) -> dict:
    rows = _no_signal_snapshot_rows(day)
    by_ticker: dict[str, dict] = {}
    blockers = Counter()
    for row in rows:
        tkr = str(row.get('ticker') or 'unknown')
        bucket = by_ticker.setdefault(tkr, {
            'rows': 0,
            'blocked_by': Counter(),
            'last_snapshot': None,
        })
        bucket['rows'] += 1
        for blocker in row.get('blocked_by') or []:
            blockers[str(blocker)] += 1
            bucket['blocked_by'][str(blocker)] += 1
        bucket['last_snapshot'] = {
            'created_at_ct': row.get('created_at_ct') or row.get('created_at_et'),
            'price': row.get('price'),
            'blocked_by': row.get('blocked_by') or [],
            'indicators': row.get('indicators') or {},
            'btc_context': row.get('btc_context') or {},
        }
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'rows': len(rows),
        'top_blockers': [{'blocker': k, 'count': v} for k, v in blockers.most_common(20)],
        'by_ticker': {
            tkr: {
                'rows': bucket['rows'],
                'top_blockers': [
                    {'blocker': k, 'count': v}
                    for k, v in bucket['blocked_by'].most_common(12)
                ],
                'last_snapshot': bucket['last_snapshot'],
            }
            for tkr, bucket in sorted(by_ticker.items())
        },
        'source_file': os.path.join(OUT_DIR, 'no_signal_snapshots', f'no_signal_snapshots_{day}.jsonl'),
        'deduction': (
            'Use this for false-negative review: it captures ticker/BTC context even when no setup '
            'was close enough to create a near-signal row.'
        ),
    }


def build_fill_attribution_summary(day: str) -> dict:
    rows = _fill_attribution_rows(day)
    by_stage = defaultdict(list)
    by_ticker = defaultdict(list)
    for row in rows:
        by_stage[str(row.get('stage') or 'unknown')].append(row)
        by_ticker[str(row.get('ticker') or 'unknown')].append(row)

    def summarize(bucket: list[dict]) -> dict:
        adverse = [_num(r.get('adverse_fill_vs_mid_pct')) for r in bucket]
        adverse = [v for v in adverse if v is not None]
        spreads = [_num(_nested(r, 'quote', 'spread_pct')) for r in bucket]
        spreads = [v for v in spreads if v is not None]
        stale = [
            r for r in bucket
            if (_num(_nested(r, 'quote', 'quote_age_sec')) is not None
                and _num(_nested(r, 'quote', 'quote_age_sec')) > 3)
        ]
        return {
            'rows': len(bucket),
            'avg_adverse_fill_vs_mid_pct': _avg(adverse),
            'max_adverse_fill_vs_mid_pct': max(adverse) if adverse else None,
            'avg_spread_pct': _avg(spreads),
            'stale_quote_rows': len(stale),
        }

    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'rows': len(rows),
        'by_stage': {stage: summarize(bucket) for stage, bucket in sorted(by_stage.items())},
        'by_ticker': {tkr: summarize(bucket) for tkr, bucket in sorted(by_ticker.items())},
        'worst_adverse_rows': sorted(
            [
                {
                    'ticker': r.get('ticker'),
                    'side': r.get('side'),
                    'stage': r.get('stage'),
                    'trade_id': r.get('trade_id'),
                    'adverse_fill_vs_mid_pct': r.get('adverse_fill_vs_mid_pct'),
                    'spread_pct': _nested(r, 'quote', 'spread_pct'),
                    'quote_age_sec': _nested(r, 'quote', 'quote_age_sec'),
                }
                for r in rows
                if r.get('adverse_fill_vs_mid_pct') is not None
            ],
            key=lambda r: float(r.get('adverse_fill_vs_mid_pct') or 0),
            reverse=True,
        )[:12],
        'source_file': os.path.join(OUT_DIR, 'fill_attribution', f'fill_attribution_{day}.jsonl'),
        'deduction': 'Use this to separate strategy losses from execution/friction losses.',
    }


def _shadow_dual_of(row: dict) -> dict:
    return (
        row.get('shadow_dual_side_score')
        or _nested(row, 'entry_thesis', 'shadow_dual_side_score')
        or _nested(row, 'forensics', 'shadow_dual_side_score')
        or _nested(row, 'decision_audit', 'shadow_dual_side_score')
        or _nested(row, 'entry_thesis', 'signal_quality', 'score_model', 'dual_side_shadow')
        or _nested(row, 'forensics', 'signal_quality', 'score_model', 'dual_side_shadow')
        or _nested(row, 'signal_quality', 'score_model', 'dual_side_shadow')
        or {}
    )


def build_dual_side_shadow_review(day: str) -> dict:
    rows = []
    counts = Counter()
    for source, corpus in (
        ('trade', _tape(day)),
        ('skipped_signal', _skipped_signal_rows(day)),
        ('gate_timeline', _gate_timeline_rows(day)),
        ('no_signal_snapshot', _no_signal_snapshot_rows(day)),
    ):
        for row in corpus:
            dual = _shadow_dual_of(row)
            if not dual or dual.get('enabled') is False:
                continue
            engine_side = row.get('side') or _nested(row, 'entry_thesis', 'side')
            long_score = _num(_nested(dual, 'long', 'score'))
            short_score = _num(_nested(dual, 'short', 'score'))
            chosen = dual.get('chosen_side_by_shadow')
            gap = _num(dual.get('side_gap'))
            verdict = 'aligned'
            if engine_side in ('LONG', 'SHORT') and chosen in ('LONG', 'SHORT') and engine_side != chosen:
                verdict = 'shadow_preferred_opposite_side'
            elif gap is not None and gap < 1.0:
                verdict = 'side_edge_thin'
            counts[verdict] += 1
            rows.append({
                'source': source,
                'ticker': row.get('ticker'),
                'trade_id': row.get('trade_id'),
                'side': engine_side,
                'pnl': row.get('pnl'),
                'shadow_chosen_side': chosen,
                'shadow_opposite_side': dual.get('opposite_side'),
                'side_gap': gap,
                'long_score': long_score,
                'short_score': short_score,
                'why_opposite_failed': dual.get('why_opposite_failed') or [],
                'verdict': verdict,
            })
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'rows': rows[:500],
        'counts': dict(counts),
        'deduction': (
            'Passive dual-side scoring answers whether a losing SHORT had stronger LONG evidence, '
            'or whether the opposite side failed for clear reasons. It does not auto-flip trades.'
        ),
    }


def build_entry_timing_efficiency(day: str) -> dict:
    rows = []
    by_window = defaultdict(list)
    for trade in _tape(day):
        timing = trade.get('entry_timing') or {}
        windows = timing.get('windows_sec') or {}
        if not windows:
            continue
        trade_rows = []
        for sec, row in sorted(windows.items(), key=lambda kv: int(kv[0]) if str(kv[0]).isdigit() else 9999):
            improved = _num(row.get('could_have_improved_entry_pct'))
            worsened = _num(row.get('could_have_worsened_entry_pct'))
            by_window[str(sec)].append(improved or 0.0)
            trade_rows.append({
                'window_sec': int(sec) if str(sec).isdigit() else sec,
                'best_entry_px': row.get('best_entry_px'),
                'worst_entry_px': row.get('worst_entry_px'),
                'could_have_improved_entry_pct': improved,
                'could_have_worsened_entry_pct': worsened,
                'entry_timing_grade': row.get('entry_timing_grade'),
            })
        rows.append({
            'trade_id': trade.get('trade_id'),
            'ticker': trade.get('ticker'),
            'side': trade.get('side'),
            'pnl': trade.get('pnl'),
            'entry': trade.get('entry'),
            'result': trade.get('result'),
            'windows': trade_rows,
            'worst_improvement_pct': max([r.get('could_have_improved_entry_pct') or 0 for r in trade_rows], default=0),
        })
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'rows': sorted(rows, key=lambda r: r.get('worst_improvement_pct') or 0, reverse=True)[:200],
        'by_window': {
            sec: {
                'trades': len(vals),
                'avg_improvement_available_pct': _avg(vals),
                'max_improvement_available_pct': max(vals) if vals else None,
            }
            for sec, vals in sorted(by_window.items(), key=lambda kv: int(kv[0]) if str(kv[0]).isdigit() else 9999)
        },
        'deduction': (
            'Shows whether the entry was well-timed relative to the next 15/30/60/120 seconds. '
            'High improvement_available means the bot may be entering too early/late.'
        ),
    }


def build_latency_attribution_summary(day: str) -> dict:
    rows = _latency_attribution_rows(day)
    by_status = defaultdict(list)
    for row in rows:
        by_status[str(row.get('status') or 'unknown')].append(row)

    def avg_delta(bucket: list[dict], key: str) -> Optional[float]:
        vals = [_num(_nested(r, 'from_signal_ms', key)) for r in bucket]
        vals = [v for v in vals if v is not None]
        return _avg(vals)

    summary = {
        status: {
            'rows': len(bucket),
            'avg_reserved_from_signal_ms': avg_delta(bucket, 'reserved_from_signal_ms'),
            'avg_submit_from_signal_ms': avg_delta(bucket, 'broker_submit_start_from_signal_ms'),
            'avg_commit_from_signal_ms': avg_delta(bucket, 'committed_from_signal_ms'),
            'avg_fill_from_signal_ms': avg_delta(bucket, 'entry_filled_from_signal_ms'),
        }
        for status, bucket in sorted(by_status.items())
    }
    worst = sorted(
        [
            {
                'ticker': r.get('ticker'),
                'side': r.get('side'),
                'trade_id': r.get('trade_id'),
                'status': r.get('status'),
                'fill_from_signal_ms': _nested(r, 'from_signal_ms', 'entry_filled_from_signal_ms'),
                'commit_from_signal_ms': _nested(r, 'from_signal_ms', 'committed_from_signal_ms'),
                'adjacent_ms': r.get('adjacent_ms') or {},
            }
            for r in rows
        ],
        key=lambda r: _num(r.get('fill_from_signal_ms') or r.get('commit_from_signal_ms')) or 0,
        reverse=True,
    )[:20]
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'rows': len(rows),
        'by_status': summary,
        'worst_latency_rows': worst,
        'source_file': os.path.join(OUT_DIR, 'latency_attribution', f'latency_attribution_{day}.jsonl'),
        'deduction': 'Use this to quantify whether slow broker/API/commit latency damaged entries.',
    }


def build_quote_condition_quality_summary(day: str) -> dict:
    fill_rows = _fill_attribution_rows(day)
    trades = _tape(day)
    quote_states = Counter()
    condition_tags = Counter()
    spread_abnormal_rows = 0
    for row in fill_rows:
        q = row.get('quote') or {}
        qstate = (q.get('quote_state') or {}).get('state')
        if qstate:
            quote_states[str(qstate)] += 1
        if q.get('spread_abnormal'):
            spread_abnormal_rows += 1
        cq = row.get('condition_quality') or {}
        for tag in cq.get('tags') or []:
            condition_tags[str(tag)] += 1
    trade_rows = []
    for trade in trades:
        eq = _nested(trade, 'forensics', 'entry_quality') or {}
        trade_rows.append({
            'trade_id': trade.get('trade_id'),
            'ticker': trade.get('ticker'),
            'side': trade.get('side'),
            'pnl': trade.get('pnl'),
            'quote_state': _nested(eq, 'quote_state', 'state'),
            'condition_quality_score': _nested(eq, 'condition_quality', 'score'),
            'condition_tags': _nested(eq, 'condition_quality', 'tags') or [],
            'spread_vs_rolling_median': eq.get('spread_vs_rolling_median'),
            'spread_abnormal': eq.get('spread_abnormal'),
        })
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'fill_rows': len(fill_rows),
        'quote_state_counts': dict(quote_states),
        'condition_tag_counts': dict(condition_tags),
        'spread_abnormal_rows': spread_abnormal_rows,
        'trade_rows': trade_rows[:200],
        'deduction': (
            'Summarizes locked/crossed/one-sided quotes, condition-code context, and abnormal spreads '
            'so loser reviews can separate bad thesis from bad market data/execution context.'
        ),
    }


def build_shadow_entry_variants_review(day: str) -> dict:
    samples = []
    for source, rows in (
        ('skipped_signal', _skipped_signal_rows(day)),
        ('near_signal', _near_signal_rows(day)),
        ('gate_timeline', _gate_timeline_rows(day)),
        ('no_signal_snapshot', _no_signal_snapshot_rows(day)),
    ):
        for row in rows:
            samples.append((source, row))
    variants = {
        'strict_btc': Counter(),
        'strict_execution': Counter(),
        'no_flow_fade': Counter(),
        'wide_spread_block': Counter(),
        'dual_score_gap_2': Counter(),
    }
    rows_out = []
    for source, row in samples[:2000]:
        ind = row.get('indicators') or _nested(row, 'forensics', 'entry_quality') or {}
        btc = row.get('btc_context') or _nested(row, 'forensics', 'btc') or {}
        dual = _shadow_dual_of(row)
        side = row.get('side') or (dual or {}).get('chosen_side_by_shadow')
        spread = _num(ind.get('spread_pct') or _nested(row, 'execution_quality', 'spread_pct'))
        exec_score = _num(_nested(row, 'execution_quality', 'score') or _nested(row, 'forensics', 'execution_quality', 'score'))
        setup_tags = _nested(row, 'signal_quality', 'setup_tags') or _nested(row, 'forensics', 'signal_quality', 'setup_tags') or []
        fwd5 = _num(row.get('fwd5') or _nested(row, 'fwd', '5m'))
        sample_verdicts = {}
        strict_btc_pass = bool(btc.get('ready', True)) and not bool(btc.get('stale')) and not bool(btc.get('conflict'))
        strict_execution_pass = (exec_score is None or exec_score >= 75) and (spread is None or spread <= 0.08)
        no_flow_fade_pass = 'flow_exhaustion_fade' not in setup_tags and row.get('setup_type') != 'flow_exhaustion_fade'
        wide_spread_block_pass = spread is None or spread <= 0.08
        dual_gap = _num((dual or {}).get('side_gap'))
        dual_pass = dual_gap is None or dual_gap >= 2.0
        for name, passed in (
            ('strict_btc', strict_btc_pass),
            ('strict_execution', strict_execution_pass),
            ('no_flow_fade', no_flow_fade_pass),
            ('wide_spread_block', wide_spread_block_pass),
            ('dual_score_gap_2', dual_pass),
        ):
            key = 'would_allow' if passed else 'would_block'
            variants[name][key] += 1
            if fwd5 is not None:
                variants[name]['fwd5_sum_if_allowed' if passed else 'fwd5_sum_if_blocked'] += fwd5
            sample_verdicts[name] = key
        rows_out.append({
            'source': source,
            'ticker': row.get('ticker'),
            'side': side,
            'reason': row.get('reason'),
            'setup_type': row.get('setup_type'),
            'score': row.get('score'),
            'fwd5': fwd5,
            'spread_pct': spread,
            'execution_score': exec_score,
            'dual_side_gap': dual_gap,
            'variant_verdicts': sample_verdicts,
        })
    summary = {
        name: {
            'would_allow': counts.get('would_allow', 0),
            'would_block': counts.get('would_block', 0),
            'fwd5_sum_if_allowed': round(counts.get('fwd5_sum_if_allowed', 0), 4),
            'fwd5_sum_if_blocked': round(counts.get('fwd5_sum_if_blocked', 0), 4),
        }
        for name, counts in variants.items()
    }
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'samples': len(samples),
        'summary': summary,
        'rows': rows_out[:500],
        'mode': 'passive_shadow_entry_variants',
        'deduction': (
            'Passive entry-policy variants over skipped/near/gate/no-signal samples. '
            'Use this before changing live gates; it estimates what stricter/looser policies would have done.'
        ),
    }


def build_artifact_schema_dictionary() -> dict:
    return {
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'purpose': (
            'Human/Codex data dictionary. Read this before raw artifacts so future reviews '
            'can stay token-efficient and know which file answers which question.'
        ),
        'artifacts': {
            'review_index_YYYY-MM-DD.json': 'Token-efficient first-read packet with links and top summaries.',
            'gate_timeline/gate_timeline_YYYY-MM-DD.jsonl': 'Per-ticker eligibility snapshots: why a trade was or was not allowed.',
            'no_signal_snapshots/no_signal_snapshots_YYYY-MM-DD.jsonl': 'Periodic ticker/BTC context even when no setup was near firing.',
            'fill_attribution/fill_attribution_YYYY-MM-DD.jsonl': 'Signal/pre-submit/submit/fill quote snapshots and fill-vs-mid slippage.',
            'latency_attribution/latency_attribution_YYYY-MM-DD.jsonl': 'Signal-to-submit-to-fill timing chain.',
            'tick_logs/YYYY-MM-DD/*.jsonl': 'Raw tick/quote captures around entries, exits, and near/skipped signals.',
            'dual_side_shadow_review_YYYY-MM-DD.json': 'Passive LONG-vs-SHORT score comparison and side-selection warnings.',
            'entry_timing_efficiency_YYYY-MM-DD.json': 'Actual entry vs best/worst observed price after entry.',
            'quote_condition_quality_YYYY-MM-DD.json': 'Locked/crossed quote, condition-code, exchange, and abnormal-spread summary.',
            'shadow_entry_variants_YYYY-MM-DD.json': 'Passive strict/loose entry gate variants over skipped/near/no-signal samples.',
        },
        'core_fields': {
            'shadow_dual_side_score': 'Passive side scorecard; never auto-flips trades.',
            'entry_timing.windows_sec.*.could_have_improved_entry_pct': 'Potential entry improvement vs actual price.',
            'latency_attribution.from_signal_ms': 'Milestones measured from original signal timestamp.',
            'fill_attribution.adverse_fill_vs_mid_pct': 'Positive means fill was adverse versus quote mid for that side/stage.',
            'quote.quote_state.state': 'normal, locked, crossed, missing, one_sided, invalid, or unknown.',
            'condition_quality.score': '100 is clean; lower means trade/quote condition/venue metadata needs review.',
            'spread_vs_rolling_median': 'Current spread divided by ticker-specific rolling median spread.',
        },
        'review_order': [
            'review_index_YYYY-MM-DD.json',
            'DAILY_REVIEW_START_HERE_YYYY-MM-DD.json',
            'dual_side_shadow_review_YYYY-MM-DD.json',
            'entry_timing_efficiency_YYYY-MM-DD.json',
            'latency_attribution_summary_YYYY-MM-DD.json',
            'quote_condition_quality_YYYY-MM-DD.json',
            'shadow_entry_variants_YYYY-MM-DD.json',
            'tick_replay_YYYY-MM-DD.json',
        ],
    }


def _score_components(row: dict) -> dict:
    candidates = (
        _nested(row, 'forensics', 'signal_quality', 'score_components'),
        _nested(row, 'entry_thesis', 'signal_quality', 'score_components'),
        _nested(row, 'entry_thesis', 'decision_audit', 'signal_quality', 'score_components'),
        _nested(row, 'decision_audit', 'signal_quality', 'score_components'),
        _nested(row, 'signal_quality', 'score_components'),
        row.get('components'),
    )
    for c in candidates:
        if isinstance(c, dict):
            return c
    return {}


def _component_effect(side: str, value: Any) -> str:
    n = _num(value)
    if n is None or n == 0:
        return 'neutral_or_unknown'
    if (side == 'LONG' and n > 0) or (side == 'SHORT' and n < 0):
        return 'supported_entry_side'
    return 'warned_against_entry_side'


def _trade_example(row: dict) -> dict:
    return {
        'trade_id': row.get('trade_id'),
        'time': row.get('time'),
        'ticker': row.get('ticker'),
        'side': row.get('side'),
        'setup_type': row.get('setup_type') or _nested(row, 'forensics', 'setup_type'),
        'pnl': row.get('pnl'),
        'reason': row.get('reason'),
    }


def _setup_grade(row: dict) -> dict:
    grade = (
        row.get('setup_grade_at_entry')
        or _nested(row, 'entry_thesis', 'setup_grade_at_entry')
        or _nested(row, 'decision_audit', 'setup_grade_at_entry')
        or _nested(row, 'entry_thesis', 'decision_audit', 'setup_grade_at_entry')
    )
    if isinstance(grade, dict):
        tier = grade.get('grade') or grade.get('tier') or 'unknown'
        score = _num(grade.get('score'))
        tags = grade.get('tags') if isinstance(grade.get('tags'), list) else []
        label = grade.get('label')
    else:
        tier = (
            row.get('entry_quality_tier')
            or _nested(row, 'entry_thesis', 'entry_quality_tier')
            or _nested(row, 'decision_audit', 'entry_quality_tier')
            or 'unknown'
        )
        score = _num(
            row.get('entry_quality_score')
            or _nested(row, 'entry_thesis', 'entry_quality_score')
            or _nested(row, 'decision_audit', 'entry_quality_score')
        )
        tags = _nested(row, 'entry_thesis', 'entry_quality_tags') or []
        label = None
    if tier == 'A':
        label = label or 'high_quality_entry'
    elif tier == 'B':
        label = label or 'acceptable_entry'
    elif tier == 'C':
        label = label or 'marginal_entry'
    else:
        label = label or 'unknown_entry_quality'
    return {'grade': tier, 'score': score, 'tags': tags, 'label': label}


def _fwd(row: dict, minutes: int) -> Optional[float]:
    direct = row.get(f'fwd{minutes}')
    val = _num(direct)
    if val is not None:
        return val
    nested = _nested(row, 'fwd', f'{minutes}m', 'signed_return_pct')
    val = _num(nested)
    if val is not None:
        return val
    return None


def _affected_count_from_text(text: str) -> int:
    if not text:
        return 0
    match = re.search(r'(\d+)\s*/\s*(\d+)', text)
    if match:
        return int(match.group(1))
    match = re.search(r'(\d+)\s+(?:losing|loser|trade|trades|SHORT|LONG)', text, re.I)
    if match:
        return int(match.group(1))
    return 0


def _indicators(row: dict) -> dict:
    return (
        row.get('ind')
        or row.get('indicators')
        or _nested(row, 'forensics', 'signal', 'indicators')
        or _nested(row, 'entry_thesis', 'indicators')
        or {}
    )


def _btc_context(row: dict) -> dict:
    return (
        row.get('btc')
        or row.get('btc_indicators')
        or row.get('btc_context')
        or _nested(row, 'forensics', 'btc')
        or _nested(row, 'entry_thesis', 'btc_context')
        or {}
    )


def _entry_spread_pct(row: dict) -> Optional[float]:
    return _num(
        row.get('spread_pct')
        or _nested(row, 'execution_quality', 'spread_pct')
        or _nested(row, 'entry_quality', 'spread_pct')
        or _nested(row, 'forensics', 'entry_quality', 'spread_pct')
        or _nested(row, 'entry_thesis', 'execution_quality', 'spread_pct')
        or _nested(row, 'entry_thesis', 'entry_quality', 'spread_pct')
    )


def _spread_bucket(row: dict) -> str:
    spread = _entry_spread_pct(row)
    if spread is None:
        return 'spread_unknown'
    if spread <= 0.04:
        return 'tight_spread'
    if spread <= 0.08:
        return 'normal_spread'
    return 'wide_spread'


def _signed_return_pct(row: dict) -> Optional[float]:
    direct = _num(row.get('signed_return_pct'))
    if direct is not None:
        return direct
    entry = _num(row.get('entry'))
    exit_px = _num(row.get('exit'))
    side = str(row.get('side') or '').upper()
    if entry is None or exit_px is None or entry == 0:
        return None
    raw = (exit_px - entry) / entry * 100
    return round(-raw if side == 'SHORT' else raw, 4)


def _path_of(row: dict) -> dict:
    path = row.get('path') or _nested(row, 'replay', 'path') or {}
    return path if isinstance(path, dict) else {}


def _checkpoint(row: dict, label: str) -> dict:
    chk = (_path_of(row).get('checks') or {}).get(label) or {}
    return chk if isinstance(chk, dict) else {}


def _checkpoint_status(signed: Any) -> str:
    val = _num(signed)
    if val is None:
        return 'missing'
    if val >= 0.12:
        return 'confirmed'
    if val > 0:
        return 'slightly_green'
    if val <= -0.12:
        return 'invalidated'
    if val < 0:
        return 'slightly_red'
    return 'flat'


def _trade_context_key(row: dict, fields: tuple[str, ...]) -> tuple:
    grade = _setup_grade(row).get('grade')
    mapping = {
        'ticker': row.get('ticker') or 'unknown',
        'side': row.get('side') or 'unknown',
        'setup': _setup_of(row),
        'btc_regime': _btc_regime_of(row),
        'market_regime': _market_regime_bucket(row),
        'spread': _spread_bucket(row),
        'grade': grade or 'unknown',
        'score_bucket': _score_bucket(_entry_score(row)),
    }
    return tuple(str(mapping.get(f, 'unknown')) for f in fields)


def _control_summary(rows: list[dict]) -> dict:
    wins = [r for r in rows if float(r.get('pnl') or 0) > 0]
    losses = [r for r in rows if float(r.get('pnl') or 0) < 0]
    pnl = round(sum(float(r.get('pnl') or 0) for r in rows), 2)
    return {
        'controls': len(rows),
        'wins': len(wins),
        'losses': len(losses),
        'win_rate': _safe_pct(len(wins), len(rows)),
        'pnl': pnl,
        'avg_pnl': round(pnl / len(rows), 2) if rows else None,
        'examples': [_trade_example(r) for r in rows[:8]],
    }


def _loser_fingerprint(row: dict) -> dict:
    ind = _indicators(row)
    btc = _btc_context(row)
    path = _path_of(row)
    reason = str(row.get('reason') or '')
    side = str(row.get('side') or '').upper()
    setup = str(_setup_of(row))
    tags = []
    spread = _entry_spread_pct(row)
    quote_stale = bool(
        _nested(row, 'forensics', 'entry_quality', 'quote_stale')
        or _nested(row, 'entry_thesis', 'execution_quality', 'quote_stale')
    )
    slippage = _num(row.get('entry_slippage_pct') or row.get('entry_fill_slippage_pct'))
    if (spread is not None and spread >= 0.08) or quote_stale or (slippage is not None and abs(slippage) >= 0.08):
        tags.append('execution_or_slippage')
    btc_mom = _num(btc.get('mom_60s'))
    btc_stack = str(btc.get('ema_stack') or btc.get('stack') or btc.get('regime') or '').lower()
    if btc.get('stale') or btc.get('conflict'):
        tags.append('btc_disagreement_or_stale')
    elif side == 'LONG' and ('bear' in btc_stack or (btc_mom is not None and btc_mom < -0.08)):
        tags.append('btc_disagreement_or_stale')
    elif side == 'SHORT' and ('bull' in btc_stack or (btc_mom is not None and btc_mom > 0.08)):
        tags.append('btc_disagreement_or_stale')
    flow120 = _nested(ind, 'flow_120s', 'buy_pct')
    if flow120 is not None:
        flow120 = _num(flow120)
        if side == 'SHORT' and flow120 is not None and flow120 >= 62:
            tags.append('failed_flow_fade_or_sustained_buying')
        if side == 'LONG' and flow120 is not None and flow120 <= 38:
            tags.append('failed_flow_fade_or_sustained_selling')
    vwap_sigma = _num(ind.get('vwap_dist_sigma'))
    if vwap_sigma is not None and abs(vwap_sigma) >= 2.0:
        tags.append('stretched_vwap_location')
    first_green = path.get('first_green_ts')
    first_red = path.get('first_red_ts')
    opened = row.get('opened_at')
    if opened and first_red and (not first_green or first_red <= first_green):
        try:
            if int(first_red - opened) <= 30:
                tags.append('immediate_thesis_failure')
        except Exception:
            pass
    mfe = _num(row.get('mfe_pct'))
    if mfe is not None and mfe >= 0.15 and float(row.get('pnl') or 0) < 0:
        tags.append('gave_back_open_profit')
    duration = _num(row.get('duration_min'))
    if 'cond_time_stop' in reason or (duration is not None and duration >= 5 and float(row.get('pnl') or 0) < 0):
        tags.append('exit_too_slow_or_stale_thesis')
    if reason in ('manual_stop', 'session_end', 'external_broker_exit') or reason.startswith('external_'):
        tags.append('operational_or_forced_exit')
    if not tags:
        tags.append('normal_variance_or_unclassified')
    priority = [
        'operational_or_forced_exit',
        'execution_or_slippage',
        'btc_disagreement_or_stale',
        'failed_flow_fade_or_sustained_buying',
        'failed_flow_fade_or_sustained_selling',
        'immediate_thesis_failure',
        'gave_back_open_profit',
        'exit_too_slow_or_stale_thesis',
        'stretched_vwap_location',
        'normal_variance_or_unclassified',
    ]
    primary = next((tag for tag in priority if tag in tags), tags[0])
    return {
        'primary': primary,
        'tags': tags,
        'evidence': {
            'spread_pct': spread,
            'slippage_pct': slippage,
            'btc_mom_60s': btc_mom,
            'btc_stack': btc_stack or None,
            'flow_120s_buy_pct': flow120,
            'vwap_dist_sigma': vwap_sigma,
            'mfe_pct': mfe,
            'mae_pct': _num(row.get('mae_pct')),
            'time_to_mfe_sec': path.get('time_to_mfe_sec'),
            'time_to_mae_sec': path.get('time_to_mae_sec'),
        },
    }


def _gate_component_summary(tape: list[dict], component_name: str) -> dict:
    rows = [t for t in tape if component_name in _score_components(t)]
    buckets = defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0, 'examples': []})
    for row in rows:
        effect = _component_effect(str(row.get('side') or ''), _score_components(row).get(component_name))
        bucket = buckets[effect]
        pnl = float(row.get('pnl') or 0)
        bucket['trades'] += 1
        bucket['wins'] += 1 if pnl > 0 else 0
        bucket['losses'] += 1 if pnl < 0 else 0
        bucket['pnl'] += pnl
        if len(bucket['examples']) < 8:
            ex = _trade_example(row)
            ex['component_value'] = _score_components(row).get(component_name)
            bucket['examples'].append(ex)
    out = {}
    for key, bucket in buckets.items():
        out[key] = {
            **bucket,
            'pnl': round(bucket['pnl'], 2),
            'win_rate': _safe_pct(bucket['wins'], bucket['trades']),
        }
    return {
        'component': component_name,
        'trades_with_component': len(rows),
        'by_effect': out,
    }


def build_new_gate_attribution(day: str) -> dict:
    tape = _tape(day)
    skipped = _skipped_signal_rows(day)
    retry_rows = _entry_retry_rows(day)

    miner_rows = []
    miner_state = defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0, 'examples': []})
    for row in tape:
        basket = (
            row.get('miner_basket')
            or _nested(row, 'forensics', 'miner_basket')
            or _nested(row, 'entry_thesis', 'miner_basket')
            or _nested(row, 'entry_thesis', 'decision_audit', 'miner_basket')
        )
        if not isinstance(basket, dict):
            comp = _score_components(row).get('miner_basket')
            if comp is None:
                continue
            basket = {'state': _component_effect(str(row.get('side') or ''), comp), 'score': comp}
        state = basket.get('state') or 'unknown'
        pnl = float(row.get('pnl') or 0)
        b = miner_state[state]
        b['trades'] += 1
        b['wins'] += 1 if pnl > 0 else 0
        b['losses'] += 1 if pnl < 0 else 0
        b['pnl'] += pnl
        if len(b['examples']) < 8:
            ex = _trade_example(row)
            ex['miner_basket'] = {
                'state': state,
                'score': basket.get('score'),
                'ready_peers': basket.get('ready_peers'),
                'confirming': basket.get('confirming'),
                'opposing': basket.get('opposing'),
            }
            b['examples'].append(ex)
        miner_rows.append(row)
    miner_out = {}
    for state, b in miner_state.items():
        miner_out[state] = {
            **b,
            'pnl': round(b['pnl'], 2),
            'win_rate': _safe_pct(b['wins'], b['trades']),
        }

    raw_spread_block_rows = [
        r for r in [*skipped, *retry_rows]
        if 'spread' in str(r.get('reason') or '').lower()
        and (
            'quality_gate' in str(r.get('reason') or '').lower()
            or 'hard_gate' in str(r.get('reason') or '').lower()
            or 'spread_too_wide' in str(r.get('reason') or '').lower()
        )
    ]
    seen_spread = set()
    spread_block_rows = []
    for row in raw_spread_block_rows:
        key = (
            row.get('ticker'),
            row.get('side'),
            row.get('reason'),
            int((row.get('created_at') or 0) // 5),
            round(float(row.get('price') or row.get('signal_price') or 0), 3),
        )
        if key in seen_spread:
            continue
        seen_spread.add(key)
        spread_block_rows.append(row)

    cap_rows = []
    cap_stop_rows = []
    for row in tape:
        bracket = row.get('bracket_policy') or _nested(row, 'entry_thesis', 'bracket_policy') or {}
        notes = bracket.get('notes') if isinstance(bracket, dict) else []
        note_text = ' '.join(str(n) for n in (notes or []))
        if 'broker_disaster_stop_cap' not in note_text:
            continue
        cap_rows.append(row)
        reason = str(row.get('reason') or '')
        if reason in ('stop_loss', 'broker_stop', 'external_broker_exit') or 'stop' in reason:
            cap_stop_rows.append(row)

    gates = {
        'miner_basket': {
            'trades_with_basket_context': len(miner_rows),
            'by_state': miner_out,
            'deduction': (
                'Miner basket is now attributable by confirmed/opposed/mixed peer context. '
                'Treat opposed losing clusters as entry-quality evidence, not an automatic rule change.'
            ),
        },
        'btc_chop_penalty': _gate_component_summary(tape, 'btc_chop_penalty'),
        'realized_vol_chop': _gate_component_summary(tape, 'realized_vol_chop'),
        'spread_quality_gate': {
            'blocked_or_retried_signals': len(spread_block_rows),
            'raw_rows_before_dedupe': len(raw_spread_block_rows),
            'by_reason': _counter(spread_block_rows, 'reason'),
            'sample': spread_block_rows[:25],
            'deduction': (
                'This separates "did not trade because spread/quality was poor" from actual loser analysis.'
            ),
        },
        'broker_disaster_stop_cap': {
            'trades_with_cap_applied': len(cap_rows),
            'cap_trade_pnl': round(sum(float(r.get('pnl') or 0) for r in cap_rows), 2),
            'cap_stop_like_exits': len(cap_stop_rows),
            'cap_stop_like_pnl': round(sum(float(r.get('pnl') or 0) for r in cap_stop_rows), 2),
            'examples': [_trade_example(r) for r in cap_stop_rows[:12]],
            'deduction': (
                'Broker disaster stops are a safety circuit. Judge them separately from strategy exits: '
                'a painful cap exit may still be correct if it prevented unbounded broker exposure.'
            ),
        },
    }

    warnings = []
    for name in ('btc_chop_penalty', 'realized_vol_chop'):
        warned = gates[name].get('by_effect', {}).get('warned_against_entry_side', {})
        if warned.get('losses'):
            warnings.append(
                f"{name} warned against {warned['losses']} losing trade(s); review whether score threshold still allowed weak entries."
            )
    if cap_stop_rows:
        warnings.append(
            f"{len(cap_stop_rows)} stop-like exit(s) occurred with broker_disaster_stop_cap applied; review stop cap vs trade thesis separately."
        )

    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'purpose': 'Explicit attribution for newly added live gates so postmortems can judge gate impact instead of burying it in raw trade rows.',
        'gates': gates,
        'warnings': warnings,
        'promotion_note': 'No automatic gate changes. Promote only after clean multi-day evidence and winner-damage checks.',
    }


def build_shadow_exit_summary(day: str) -> dict:
    rows = _shadow_exit_rows(day)
    tape = _tape(day)
    by_trade = {t.get('trade_id'): t for t in tape if t.get('trade_id')}
    policy = defaultdict(lambda: {
        'events': 0, 'saved_loser': 0, 'cut_winner': 0, 'improved_winner': 0,
        'worsened_loser': 0, 'neutral': 0, 'needs_actual_exit': 0, 'examples': [],
    })
    graded = []
    for row in rows:
        trade = by_trade.get(row.get('trade_id'))
        label = 'needs_actual_exit'
        actual_pnl = None
        if trade:
            actual_pnl = float(trade.get('pnl') or 0)
            signed_pct = float(row.get('signed_pct') or 0)
            entry = float(row.get('entry') or trade.get('entry') or 0)
            qty = float(row.get('qty') or trade.get('qty') or 0)
            sim_pnl = round(entry * qty * signed_pct / 100, 2) if entry and qty else None
            if sim_pnl is not None:
                delta = sim_pnl - actual_pnl
                if actual_pnl < 0 and delta > 0.01:
                    label = 'saved_loser'
                elif actual_pnl > 0 and delta < -0.01:
                    label = 'cut_winner'
                elif actual_pnl > 0 and delta > 0.01:
                    label = 'improved_winner'
                elif actual_pnl < 0 and delta < -0.01:
                    label = 'worsened_loser'
                else:
                    label = 'neutral'
            else:
                delta = None
        else:
            sim_pnl = delta = None
        item = {
            **row,
            'grade': label,
            'actual_pnl': actual_pnl,
            'estimated_shadow_pnl': sim_pnl,
            'estimated_delta_vs_actual': delta,
        }
        graded.append(item)
        acc = policy[row.get('policy') or 'unknown']
        acc['events'] += 1
        acc[label] += 1
        if len(acc['examples']) < 8:
            acc['examples'].append(item)
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'rows': len(rows),
        'by_policy': dict(policy),
        'graded_rows_sample': graded[:50],
        'promotion_note': (
            'Promote only after multiple days show saved_loser events without repeated cut_winner damage.'
        ),
    }


def build_entry_retry_summary(day: str) -> dict:
    rows = _entry_retry_rows(day)
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'rows': len(rows),
        'by_reason': _counter(rows, 'reason'),
        'by_ticker': _counter(rows, 'ticker'),
        'by_setup': _counter(rows, 'setup_type'),
        'sample': rows[:50],
        'mode': 'passive_log_only',
        'promotion_note': (
            'Retry logic should only become active if repeated transient blocks later show favorable forward returns.'
        ),
    }


def build_exit_hierarchy_summary(day: str) -> dict:
    tape = _tape(day)
    rows = []
    for t in tape:
        hierarchy = t.get('exit_reason_hierarchy') or _nested(t, 'replay', 'exit_reason_hierarchy')
        if not hierarchy:
            continue
        rows.append({
            'ticker': t.get('ticker'),
            'side': t.get('side'),
            'time': t.get('time'),
            'pnl': t.get('pnl'),
            'actual_reason': t.get('reason'),
            'selected': hierarchy.get('selected'),
            'candidates': hierarchy.get('candidates') or [],
        })
    candidate_counts = Counter()
    selected_counts = Counter()
    for row in rows:
        selected_counts[row.get('selected') or row.get('actual_reason') or 'None'] += 1
        for c in row.get('candidates') or []:
            candidate_counts[c.get('kind') or c.get('reason') or 'unknown'] += 1
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'rows': len(rows),
        'selected_counts': dict(selected_counts),
        'candidate_kind_counts': dict(candidate_counts),
        'sample': rows[:50],
    }


def _available_postmortem_days(day: str, lookback: int) -> list[str]:
    days = []
    if os.path.isdir(OUT_DIR):
        for name in sorted(os.listdir(OUT_DIR)):
            if name.startswith('postmortem_') and name.endswith('.json'):
                d = name[len('postmortem_'):-len('.json')]
                if d <= day:
                    days.append(d)
    return days[-lookback:]


def build_per_ticker_learning(day: str, lookback: int = 7) -> dict:
    days = _available_postmortem_days(day, lookback)
    acc = defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0, 'gross_loss': 0.0})
    for d in days:
        for t in _tape(d):
            key = (t.get('ticker') or 'unknown', t.get('side') or 'unknown', t.get('setup_type') or 'unknown')
            row = acc[key]
            pnl = float(t.get('pnl') or 0)
            row['trades'] += 1
            row['wins'] += 1 if pnl > 0 else 0
            row['losses'] += 1 if pnl < 0 else 0
            row['pnl'] += pnl
            row['gross_loss'] += abs(pnl) if pnl < 0 else 0
    rows = []
    for (ticker, side, setup), row in acc.items():
        win_rate = _safe_pct(row['wins'], row['trades'])
        suggestion = 'collect_more'
        if row['trades'] >= 5 and row['pnl'] < 0 and row['losses'] >= row['wins']:
            suggestion = 'stage_reduce_size_or_raise_min_score'
        elif row['trades'] >= 5 and row['pnl'] > 0 and win_rate is not None and win_rate >= 65:
            suggestion = 'protect_or_allow_current'
        rows.append({
            'ticker': ticker,
            'side': side,
            'setup_type': setup,
            'days': days,
            'trades': row['trades'],
            'wins': row['wins'],
            'losses': row['losses'],
            'win_rate': win_rate,
            'pnl': round(row['pnl'], 2),
            'gross_loss': round(row['gross_loss'], 2),
            'suggestion': suggestion,
            'mode': 'staged_review_only',
        })
    rows.sort(key=lambda r: (r['suggestion'] != 'stage_reduce_size_or_raise_min_score', r['pnl']))
    return {
        'day': day,
        'lookback': lookback,
        'days': days,
        'rows': rows,
        'note': 'Per-ticker personality learning is staged only; live thresholds are unchanged.',
    }


def build_regime_scoring_review(day: str, lookback: int = 7) -> dict:
    days = _available_postmortem_days(day, lookback)
    acc = defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0})
    for d in days:
        for t in _tape(d):
            btc = t.get('btc') or t.get('btc_indicators') or {}
            regime = btc.get('regime_detail') or btc.get('regime') or t.get('btc_regime') or 'unknown'
            key = (regime, t.get('side') or 'unknown', t.get('setup_type') or 'unknown')
            row = acc[key]
            pnl = float(t.get('pnl') or 0)
            row['trades'] += 1
            row['wins'] += 1 if pnl > 0 else 0
            row['losses'] += 1 if pnl < 0 else 0
            row['pnl'] += pnl
    rows = []
    for (regime, side, setup), row in acc.items():
        suggestion = 'collect_more'
        if row['trades'] >= 5 and row['pnl'] < 0:
            suggestion = 'stage_regime_score_penalty'
        elif row['trades'] >= 5 and row['pnl'] > 0:
            suggestion = 'regime_supports_current_scoring'
        rows.append({
            'btc_regime': regime,
            'side': side,
            'setup_type': setup,
            'trades': row['trades'],
            'wins': row['wins'],
            'losses': row['losses'],
            'win_rate': _safe_pct(row['wins'], row['trades']),
            'pnl': round(row['pnl'], 2),
            'suggestion': suggestion,
            'mode': 'staged_review_only',
        })
    rows.sort(key=lambda r: (r['suggestion'] != 'stage_regime_score_penalty', r['pnl']))
    return {
        'day': day,
        'lookback': lookback,
        'days': days,
        'rows': rows,
        'note': 'Regime-specific scoring review is passive; ws_scalp only logs passive_regime_profile.',
    }


def _score_bucket(score: Any) -> str:
    val = _num(score)
    if val is None:
        return 'unknown'
    val = abs(val)
    if val < 5:
        return '<5'
    if val < 6:
        return '5-5.9'
    if val < 7:
        return '6-6.9'
    return '7+'


def _execution_bucket(trade: dict) -> str:
    score = (
        trade.get('entry_quality_score')
        or _nested(trade, 'entry_thesis', 'entry_quality_score')
        or _nested(trade, 'entry_thesis', 'decision_audit', 'execution_quality', 'score')
        or _nested(trade, 'decision_audit', 'execution_quality', 'score')
    )
    val = _num(score)
    if val is None:
        return 'unknown'
    if val >= 85:
        return '85+'
    if val >= 75:
        return '75-84'
    if val >= 65:
        return '65-74'
    return '<65'


def _entry_score(trade: dict) -> Optional[float]:
    for value in (
        trade.get('entry_score'),
        trade.get('score'),
        _nested(trade, 'entry_thesis', 'score'),
        _nested(trade, 'entry_thesis', 'signal', 'score'),
        _nested(trade, 'forensics', 'signal', 'score'),
        _nested(trade, 'decision_audit', 'score'),
        _nested(trade, 'entry_thesis', 'decision_audit', 'score'),
    ):
        val = _num(value)
        if val is not None:
            return val
    return None


def _setup_of(trade: dict) -> str:
    return (
        trade.get('setup_type')
        or trade.get('setup')
        or _nested(trade, 'forensics', 'setup_type')
        or _nested(trade, 'entry_thesis', 'setup_type')
        or _nested(trade, 'decision_audit', 'setup_type')
        or _nested(trade, 'entry_thesis', 'decision_audit', 'setup_type')
        or 'unknown'
    )


def _btc_regime_of(trade: dict) -> str:
    btc = (
        trade.get('btc_context')
        or trade.get('btc')
        or _nested(trade, 'forensics', 'btc')
        or _nested(trade, 'forensics', 'btc_context')
        or _nested(trade, 'entry_thesis', 'btc_context')
        or _nested(trade, 'decision_audit', 'btc_context')
        or _nested(trade, 'entry_thesis', 'decision_audit', 'btc_context')
        or {}
    )
    if isinstance(btc, dict):
        return btc.get('regime_detail') or btc.get('regime') or 'unknown'
    return 'unknown'


def _market_tape_of(trade: dict) -> dict:
    tape = (
        trade.get('market_tape_at_entry')
        or _nested(trade, 'forensics', 'market_tape')
        or _nested(trade, 'entry_thesis', 'market_tape')
        or _nested(trade, 'decision_audit', 'market_tape')
        or _nested(trade, 'entry_thesis', 'decision_audit', 'market_tape')
        or {}
    )
    return tape if isinstance(tape, dict) else {}


def _market_regime_bucket(trade: dict) -> str:
    tape = _market_tape_of(trade)
    if not tape:
        return 'unknown'
    risk_on = 0
    risk_off = 0
    smallcap_off = False
    for sym in ('SPY', 'QQQ', 'IWM'):
        row = tape.get(sym) or {}
        if not isinstance(row, dict):
            continue
        day_pct = _num(row.get('day_pct'))
        above = row.get('above_vwap')
        if day_pct is None or above is None:
            continue
        if day_pct > 0 and above is True:
            risk_on += 1
        elif day_pct < 0 and above is False:
            risk_off += 1
            if sym == 'IWM':
                smallcap_off = True
    if risk_on >= 2 and risk_off == 0:
        return 'broad_risk_on'
    if risk_off >= 2 and risk_on == 0:
        return 'smallcap_risk_off' if smallcap_off else 'broad_risk_off'
    if risk_on or risk_off:
        return 'mixed_market'
    return 'unknown'


def build_market_tape_attribution(day: str) -> dict:
    tape = _tape(day)
    buckets = defaultdict(lambda: {
        'trades': 0,
        'wins': 0,
        'losses': 0,
        'pnl': 0.0,
        'spy_day_pct': [],
        'qqq_day_pct': [],
        'iwm_day_pct': [],
        'examples': [],
    })
    side_buckets = defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0})
    for trade in tape:
        pnl = float(trade.get('pnl') or 0)
        regime = _market_regime_bucket(trade)
        bucket = buckets[regime]
        bucket['trades'] += 1
        bucket['wins'] += 1 if pnl > 0 else 0
        bucket['losses'] += 1 if pnl < 0 else 0
        bucket['pnl'] += pnl
        mt = _market_tape_of(trade)
        for sym, key in (('SPY', 'spy_day_pct'), ('QQQ', 'qqq_day_pct'), ('IWM', 'iwm_day_pct')):
            val = _num((mt.get(sym) or {}).get('day_pct') if isinstance(mt.get(sym), dict) else None)
            if val is not None:
                bucket[key].append(val)
        if len(bucket['examples']) < 8:
            ex = _trade_example(trade)
            ex['market_regime'] = regime
            ex['market_tape'] = {
                sym: {
                    'day_pct': _num((mt.get(sym) or {}).get('day_pct')) if isinstance(mt.get(sym), dict) else None,
                    'above_vwap': (mt.get(sym) or {}).get('above_vwap') if isinstance(mt.get(sym), dict) else None,
                }
                for sym in ('SPY', 'QQQ', 'IWM')
            }
            bucket['examples'].append(ex)
        side_key = '|'.join([str(trade.get('side') or 'unknown'), regime])
        sb = side_buckets[side_key]
        sb['trades'] += 1
        sb['wins'] += 1 if pnl > 0 else 0
        sb['losses'] += 1 if pnl < 0 else 0
        sb['pnl'] += pnl

    def finish(rows: dict) -> list[dict]:
        out = []
        for key, row in rows.items():
            out.append({
                'bucket': key,
                'trades': row['trades'],
                'wins': row['wins'],
                'losses': row['losses'],
                'win_rate': _safe_pct(row['wins'], row['trades']),
                'pnl': round(row['pnl'], 2),
                'avg_pnl': round(row['pnl'] / row['trades'], 2) if row['trades'] else None,
                'avg_spy_day_pct': _avg(row.get('spy_day_pct') or []),
                'avg_qqq_day_pct': _avg(row.get('qqq_day_pct') or []),
                'avg_iwm_day_pct': _avg(row.get('iwm_day_pct') or []),
                'examples': row.get('examples', [])[:8],
                'confidence': 'usable' if row['trades'] >= 10 else 'watch_only',
            })
        out.sort(key=lambda r: (r['bucket'] == 'unknown', -r['trades'], r['bucket']))
        return out

    side_rows = []
    for key, row in side_buckets.items():
        side_rows.append({
            'bucket': key,
            'trades': row['trades'],
            'wins': row['wins'],
            'losses': row['losses'],
            'win_rate': _safe_pct(row['wins'], row['trades']),
            'pnl': round(row['pnl'], 2),
            'avg_pnl': round(row['pnl'] / row['trades'], 2) if row['trades'] else None,
            'confidence': 'usable' if row['trades'] >= 10 else 'watch_only',
        })
    side_rows.sort(key=lambda r: (-r['trades'], r['bucket']))
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'purpose': (
            'Attributes trade outcomes to SPY/QQQ/IWM context captured at entry. '
            'Use it to learn whether broad-market or small-cap regime should tighten miner entries.'
        ),
        'market_regime_buckets': finish(buckets),
        'side_by_market_regime': side_rows,
        'deduction': (
            'This is a passive learning table. Promote only after multi-day evidence shows a regime '
            'hurts entries without blocking high-quality winners.'
        ),
    }


def build_score_calibration(day: str, lookback: int = 10) -> dict:
    days = _available_postmortem_days(day, lookback)
    buckets = {
        'score': defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0}),
        'ticker_side_setup': defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0}),
        'btc_regime': defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0}),
        'execution_quality': defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0}),
        'score_side_setup_btc_exec': defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0}),
        'market_regime': defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0}),
        'component_effect': defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0}),
    }
    for d in days:
        for trade in _tape(d):
            pnl = float(trade.get('pnl') or 0)
            score = _entry_score(trade)
            setup = str(_setup_of(trade))
            side = str(trade.get('side') or 'unknown')
            btc_regime = _btc_regime_of(trade)
            exec_bucket = _execution_bucket(trade)
            keys = {
                'score': _score_bucket(score),
                'ticker_side_setup': '|'.join([
                    str(trade.get('ticker') or 'unknown'),
                    side,
                    setup,
                ]),
                'btc_regime': btc_regime,
                'execution_quality': exec_bucket,
                'score_side_setup_btc_exec': '|'.join([
                    _score_bucket(score), side, setup, btc_regime, exec_bucket,
                ]),
                'market_regime': _market_regime_bucket(trade),
            }
            for name, key in keys.items():
                row = buckets[name][key]
                row['trades'] += 1
                row['wins'] += 1 if pnl > 0 else 0
                row['losses'] += 1 if pnl < 0 else 0
                row['pnl'] += pnl
            for component, value in _score_components(trade).items():
                if isinstance(value, bool) or not isinstance(value, (int, float)) or value == 0:
                    continue
                key = f'{component}|{_component_effect(side, value)}'
                row = buckets['component_effect'][key]
                row['trades'] += 1
                row['wins'] += 1 if pnl > 0 else 0
                row['losses'] += 1 if pnl < 0 else 0
                row['pnl'] += pnl

    def finish(rows: dict) -> list[dict]:
        out = []
        for key, row in rows.items():
            out.append({
                'bucket': key,
                'trades': row['trades'],
                'wins': row['wins'],
                'losses': row['losses'],
                'win_rate': _safe_pct(row['wins'], row['trades']),
                'pnl': round(row['pnl'], 2),
                'avg_pnl': round(row['pnl'] / row['trades'], 2) if row['trades'] else None,
                'confidence': 'usable' if row['trades'] >= 10 else 'watch_only',
            })
        out.sort(key=lambda r: (-r['trades'], r['bucket']))
        return out

    return {
        'day': day,
        'lookback': lookback,
        'days': days,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'tables': {name: finish(rows) for name, rows in buckets.items()},
        'deduction': (
            'Use this as the daily score calibration table. Do not change broad weights '
            'until a bucket has enough trades and winner-damage checks are acceptable.'
        ),
    }


def build_setup_grade_review(day: str, lookback: int = 10) -> dict:
    days = _available_postmortem_days(day, lookback)
    by_grade = defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0, 'examples': []})
    by_setup_grade = defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0, 'examples': []})
    rows = []
    for d in days:
        for trade in _tape(d):
            pnl = float(trade.get('pnl') or 0)
            grade = _setup_grade(trade)
            setup = _setup_of(trade)
            side = str(trade.get('side') or 'unknown')
            keys = {
                'grade': str(grade.get('grade') or 'unknown'),
                'setup_grade': '|'.join([side, str(setup), str(grade.get('grade') or 'unknown')]),
            }
            for acc, key in ((by_grade, keys['grade']), (by_setup_grade, keys['setup_grade'])):
                bucket = acc[key]
                bucket['trades'] += 1
                bucket['wins'] += 1 if pnl > 0 else 0
                bucket['losses'] += 1 if pnl < 0 else 0
                bucket['pnl'] += pnl
                if len(bucket['examples']) < 8:
                    ex = _trade_example(trade)
                    ex['setup_grade_at_entry'] = grade
                    bucket['examples'].append(ex)
            rows.append({
                **_trade_example(trade),
                'setup_grade_at_entry': grade,
                'setup_type': setup,
                'grade_outcome': (
                    'high_quality_loss'
                    if grade.get('grade') == 'A' and pnl < 0 else
                    'marginal_winner'
                    if grade.get('grade') == 'C' and pnl > 0 else
                    'grade_matched_outcome'
                ),
            })

    def finish(acc: dict) -> list[dict]:
        out = []
        for key, bucket in acc.items():
            out.append({
                'bucket': key,
                'trades': bucket['trades'],
                'wins': bucket['wins'],
                'losses': bucket['losses'],
                'win_rate': _safe_pct(bucket['wins'], bucket['trades']),
                'pnl': round(bucket['pnl'], 2),
                'avg_pnl': round(bucket['pnl'] / bucket['trades'], 2) if bucket['trades'] else None,
                'examples': bucket['examples'][:8],
                'confidence': 'usable' if bucket['trades'] >= 10 else 'watch_only',
            })
        out.sort(key=lambda r: (r['bucket'] == 'unknown', -r['trades'], r['bucket']))
        return out

    return {
        'day': day,
        'lookback': lookback,
        'days': days,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'purpose': (
            'Answers whether losses came from high-quality setups that failed naturally '
            'or marginal setups that should be tightened.'
        ),
        'by_grade': finish(by_grade),
        'by_side_setup_grade': finish(by_setup_grade),
        'trade_rows_sample': rows[:100],
        'deduction': 'A-grade losers are research questions. C-grade losers are entry-quality repair candidates.',
    }


def build_counterfactual_side_review(day: str) -> dict:
    rows = []
    summary = defaultdict(lambda: {
        'losers': 0,
        'evaluable': 0,
        'opposite_positive_5m': 0,
        'opposite_positive_15m': 0,
        'opposite_positive_60m': 0,
        'examples': [],
    })
    for trade in _tape(day):
        if float(trade.get('pnl') or 0) >= 0:
            continue
        side = str(trade.get('side') or 'unknown')
        opposite = 'SHORT' if side == 'LONG' else ('LONG' if side == 'SHORT' else 'unknown')
        setup = _setup_of(trade)
        fwd = {m: _fwd(trade, m) for m in (5, 15, 60)}
        opposite_fwd = {m: (-v if v is not None else None) for m, v in fwd.items()}
        evaluable = any(v is not None for v in opposite_fwd.values())
        verdict = 'insufficient_forward_data'
        if evaluable:
            positives = [m for m, v in opposite_fwd.items() if v is not None and v > 0]
            if len(positives) >= 2:
                verdict = 'opposite_side_showed_fixed_horizon_edge'
            elif positives:
                verdict = 'opposite_side_mixed_or_brief_edge'
            else:
                verdict = 'opposite_side_not_supported'
        row = {
            **_trade_example(trade),
            'actual_side': side,
            'opposite_side': opposite,
            'setup_type': setup,
            'actual_signed_fwd': fwd,
            'opposite_signed_fwd': opposite_fwd,
            'verdict': verdict,
            'note': (
                'Diagnostic only: this estimates fixed-horizon opposite-side return from the same entry timestamp, '
                'not whether the opposite side would have passed entry rules or filled cleanly.'
            ),
        }
        rows.append(row)
        key = '|'.join([side, str(setup)])
        bucket = summary[key]
        bucket['losers'] += 1
        bucket['evaluable'] += 1 if evaluable else 0
        bucket['opposite_positive_5m'] += 1 if (opposite_fwd.get(5) or 0) > 0 else 0
        bucket['opposite_positive_15m'] += 1 if (opposite_fwd.get(15) or 0) > 0 else 0
        bucket['opposite_positive_60m'] += 1 if (opposite_fwd.get(60) or 0) > 0 else 0
        if len(bucket['examples']) < 8:
            bucket['examples'].append(row)

    summary_rows = []
    for key, bucket in summary.items():
        summary_rows.append({
            'bucket': key,
            **{k: v for k, v in bucket.items() if k != 'examples'},
            'opposite_5m_rate': _safe_pct(bucket['opposite_positive_5m'], bucket['evaluable']),
            'opposite_15m_rate': _safe_pct(bucket['opposite_positive_15m'], bucket['evaluable']),
            'opposite_60m_rate': _safe_pct(bucket['opposite_positive_60m'], bucket['evaluable']),
            'examples': bucket['examples'],
        })
    summary_rows.sort(key=lambda r: (-int(r.get('evaluable') or 0), r['bucket']))
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'purpose': 'Prevents gut-feel side flipping by showing whether losing entries had fixed-horizon opposite-side evidence.',
        'summary': summary_rows,
        'rows': rows,
        'promotion_note': (
            'Do not flip entries from this alone. Require the opposite side to pass the live entry model '
            'and repeat across clean days.'
        ),
    }


def build_rule_attribution_review(day: str, lookback: int = 10) -> dict:
    days = _available_postmortem_days(day, lookback)
    rules = defaultdict(lambda: {
        'trades': 0,
        'wins': 0,
        'losses': 0,
        'pnl': 0.0,
        'helped': 0,
        'hurt': 0,
        'neutral_or_watch': 0,
        'examples': [],
    })
    for d in days:
        for trade in _tape(d):
            pnl = float(trade.get('pnl') or 0)
            side = str(trade.get('side') or '')
            for component, value in _score_components(trade).items():
                if isinstance(value, bool) or not isinstance(value, (int, float)) or value == 0:
                    continue
                effect = _component_effect(side, value)
                key = f'component:{component}|{effect}'
                row = rules[key]
                row['trades'] += 1
                row['wins'] += 1 if pnl > 0 else 0
                row['losses'] += 1 if pnl < 0 else 0
                row['pnl'] += pnl
                if (effect == 'supported_entry_side' and pnl > 0) or (effect == 'warned_against_entry_side' and pnl < 0):
                    row['helped'] += 1
                elif (effect == 'supported_entry_side' and pnl < 0) or (effect == 'warned_against_entry_side' and pnl > 0):
                    row['hurt'] += 1
                else:
                    row['neutral_or_watch'] += 1
                if len(row['examples']) < 8:
                    ex = _trade_example(trade)
                    ex['component_value'] = value
                    ex['effect'] = effect
                    row['examples'].append(ex)
            exit_key = f"exit:{trade.get('reason') or 'unknown'}"
            row = rules[exit_key]
            row['trades'] += 1
            row['wins'] += 1 if pnl > 0 else 0
            row['losses'] += 1 if pnl < 0 else 0
            row['pnl'] += pnl
            if pnl > 0:
                row['helped'] += 1
            elif pnl < 0:
                row['hurt'] += 1
            else:
                row['neutral_or_watch'] += 1
            if len(row['examples']) < 8:
                row['examples'].append(_trade_example(trade))

    out = []
    for key, row in rules.items():
        trades = row['trades']
        out.append({
            'rule': key,
            'trades': trades,
            'wins': row['wins'],
            'losses': row['losses'],
            'win_rate': _safe_pct(row['wins'], trades),
            'pnl': round(row['pnl'], 2),
            'helped': row['helped'],
            'hurt': row['hurt'],
            'neutral_or_watch': row['neutral_or_watch'],
            'deduction': (
                'helped'
                if row['helped'] > row['hurt'] and row['pnl'] >= 0 else
                'hurt_or_needs_review'
                if row['hurt'] > row['helped'] or row['pnl'] < 0 else
                'neutral_or_watch'
            ),
            'examples': row['examples'][:8],
            'confidence': 'usable' if trades >= 10 else 'watch_only',
        })
    out.sort(key=lambda r: (r['deduction'] != 'hurt_or_needs_review', -abs(float(r.get('pnl') or 0)), r['rule']))
    return {
        'day': day,
        'lookback': lookback,
        'days': days,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'purpose': 'Aggregates which scoring components and exits helped, hurt, or need review.',
        'rows': out,
        'top_hurt_or_watch': [r for r in out if r['deduction'] == 'hurt_or_needs_review'][:12],
        'top_helped': [r for r in out if r['deduction'] == 'helped'][:12],
    }


def build_hypothesis_confidence(day: str) -> dict:
    pm = _trade_day(day)
    trust = pm.get('trust_today_for_learning') or {}
    data_quality = _num(trust.get('quality_score')) or 0
    hyp_state = _read_json(os.path.join(OUT_DIR, 'hypotheses.json'), {}) or {}
    try:
        from postmortem_hypotheses import _run_detectors
        today_findings = _run_detectors(_tape(day))
    except Exception:
        today_findings = []
    today_by_key = {key: {'deduction': ded, 'monitor': mon} for key, ded, mon in today_findings}
    rows = []
    for h in hyp_state.get('active', []) or []:
        evidence_days = int(h.get('evidence_days') or len(h.get('seen_dates') or []) or 0)
        days_since = int(h.get('days_since_evidence') or 0)
        current = today_by_key.get(h.get('key')) or {}
        affected_today = _affected_count_from_text(current.get('deduction') or h.get('deduction') or '')
        score = min(45, evidence_days * 15) + min(20, affected_today * 5) + min(25, data_quality * 0.25)
        if h.get('stale') or days_since >= 3:
            score -= 20
        score = max(0, min(100, round(score, 1)))
        if evidence_days >= 3 and score >= 70:
            label = 'ready_for_human_review'
        elif evidence_days >= 3 or score >= 60:
            label = 'actionable_soon'
        elif evidence_days >= 2 or score >= 40:
            label = 'building'
        else:
            label = 'weak'
        rows.append({
            'id': h.get('id'),
            'key': h.get('key'),
            'monitor': h.get('monitor'),
            'confidence_score': score,
            'confidence_label': label,
            'evidence_days': evidence_days,
            'seen_dates': h.get('seen_dates') or [],
            'first_seen': h.get('created'),
            'last_seen': h.get('last_seen'),
            'days_since_evidence': days_since,
            'current_day_match': bool(current),
            'supporting_trade_count_estimate_today': affected_today,
            'data_quality': {
                'score': data_quality,
                'trust': trust.get('trust'),
                'label': trust.get('quality_label'),
            },
            'winner_damage_risk': (
                'requires_counterfactual_review'
                if label in ('actionable_soon', 'ready_for_human_review') else
                'not_enough_evidence_to_estimate'
            ),
            'next_step': (
                'promote to bold action item only after winner-damage review clears'
                if label == 'ready_for_human_review' else
                'continue tracking'
            ),
        })
    rows.sort(key=lambda r: (-r['confidence_score'], r.get('id') or ''))
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'purpose': 'Ranks active hypotheses by evidence depth, data quality, recency, and estimated affected trades.',
        'rows': rows,
        'counts': dict(Counter(r['confidence_label'] for r in rows)),
        'promotion_note': 'Ready-for-human-review still does not auto-change code/config.',
    }


def build_daily_conclusion_ledger(day: str) -> dict:
    risk = build_risk_summary(day)
    losers = build_loser_summary(day)
    setup_grade = build_setup_grade_review(day)
    side_cf = build_counterfactual_side_review(day)
    rule_attr = build_rule_attribution_review(day)
    hyp_conf = build_hypothesis_confidence(day)
    matched = build_matched_control_review(day)
    exit_eff = build_exit_efficiency_review(day)
    fingerprints = build_loser_fingerprints(day)
    verdicts = build_trade_verdicts(day)
    avoid = build_best_avoided_loser_simulator(day)
    learned = []
    rejected = []
    uncertain = []
    watch = []

    learned.append(
        f"Day result: {risk.get('wins')}W/{risk.get('losses')}L, pnl=${risk.get('pnl'):+.2f}, "
        f"win_rate={risk.get('win_rate')}%, PF={risk.get('profit_factor')}."
    )
    grade_rows = setup_grade.get('by_grade') or []
    if grade_rows:
        top = ', '.join(
            f"{r.get('bucket')}: {r.get('trades')} trades pnl=${r.get('pnl'):+.2f}"
            for r in grade_rows[:3]
        )
        learned.append(f"Setup-grade split: {top}.")
    damage = losers.get('largest_damage_contributors') or []
    if damage:
        learned.append(
            'Largest loser themes: '
            + '; '.join(
                f"{r.get('time')} {r.get('ticker')} {r.get('side')} "
                f"{r.get('primary_cause') or 'unclassified'} ${float(r.get('pnl') or 0):+.2f}"
                for r in damage[:3]
            )
        )
    hurt = rule_attr.get('top_hurt_or_watch') or []
    if hurt:
        learned.append(
            'Rules/components needing review: '
            + '; '.join(f"{r.get('rule')} pnl=${r.get('pnl'):+.2f}" for r in hurt[:3])
        )
    fp_rows = fingerprints.get('summary') or []
    if fp_rows:
        learned.append(
            'Loser fingerprints: '
            + '; '.join(f"{r.get('fingerprint')} ({r.get('losers')} loser(s), ${r.get('gross_loss'):.2f})" for r in fp_rows[:3])
        )
    exit_rows = exit_eff.get('summary') or []
    if exit_rows:
        learned.append(
            'Exit efficiency labels: '
            + '; '.join(f"{r.get('label')} ({r.get('trades')} trade(s), pnl=${r.get('pnl'):+.2f})" for r in exit_rows[:3])
        )

    side_rows = side_cf.get('summary') or []
    strong_flip_rows = [
        r for r in side_rows
        if (r.get('evaluable') or 0) >= 3
        and (r.get('opposite_5m_rate') or 0) >= 70
        and (r.get('opposite_15m_rate') or 0) >= 70
    ]
    if strong_flip_rows:
        uncertain.append('Opposite-side diagnostics showed repeated fixed-horizon edge; require live-entry replay before any flip thesis.')
    else:
        rejected.append('No side-flip thesis cleared evidence; opposite-side review remains diagnostic only.')

    matched_counts = matched.get('deduction_counts') or {}
    if matched_counts.get('counterexamples_present_do_not_overfit'):
        rejected.append(
            f"{matched_counts['counterexamples_present_do_not_overfit']} loser pattern(s) had matched winning controls; do not overfit them."
        )
    weak_controls = matched_counts.get('matched_context_also_weak') or 0
    if weak_controls:
        watch.append(f"{weak_controls} loser context(s) had weak matched controls; keep these high on next review.")

    skip_count = (verdicts.get('counts') or {}).get('would_skip') or 0
    if skip_count:
        watch.append(f"{skip_count} trade(s) received would_skip verdicts; compare them with Monday winners before rule promotion.")

    promising = [
        row for row in avoid.get('rows') or []
        if row.get('confidence') == 'promising_needs_multi_day'
    ]
    for row in promising[:3]:
        watch.append(
            f"Simulator watch: {row.get('rule')} net_pnl_if_blocked=${row.get('net_pnl_if_blocked'):+.2f}, "
            f"winner_damage=${row.get('winner_damage'):+.2f}."
        )

    for row in (hyp_conf.get('rows') or [])[:5]:
        if row.get('confidence_label') in ('ready_for_human_review', 'actionable_soon'):
            watch.append(f"{row.get('id')} {row.get('confidence_label')}: {row.get('monitor')}")
        elif row.get('confidence_label') in ('weak', 'building'):
            uncertain.append(f"{row.get('id')} {row.get('confidence_label')}: {row.get('monitor')}")

    if not watch:
        watch.append('Continue collecting clean evidence; do not promote isolated one-day patterns.')
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'what_we_learned': learned,
        'what_we_rejected': rejected,
        'what_is_still_uncertain': uncertain[:10],
        'what_to_watch_next': watch[:10],
        'source_files': {
            'review_index': os.path.join(OUT_DIR, f'review_index_{day}.json'),
            'setup_grade_review': os.path.join(OUT_DIR, f'setup_grade_review_{day}_10d.json'),
            'counterfactual_side_review': os.path.join(OUT_DIR, f'counterfactual_side_review_{day}.json'),
            'rule_attribution_review': os.path.join(OUT_DIR, f'rule_attribution_review_{day}_10d.json'),
            'hypothesis_confidence': os.path.join(OUT_DIR, f'hypothesis_confidence_{day}.json'),
            'matched_control_review': os.path.join(OUT_DIR, f'matched_control_review_{day}_10d.json'),
            'exit_efficiency_review': os.path.join(OUT_DIR, f'exit_efficiency_review_{day}.json'),
            'loser_fingerprints': os.path.join(OUT_DIR, f'loser_fingerprints_{day}.json'),
            'trade_verdicts': os.path.join(OUT_DIR, f'trade_verdicts_{day}.json'),
            'best_avoided_loser_simulator': os.path.join(OUT_DIR, f'best_avoided_loser_simulator_{day}.json'),
        },
        'rule': 'This ledger is passive memory. It never changes live trading rules by itself.',
    }


def write_daily_conclusion_ledger(day: str) -> tuple[str, dict]:
    payload = build_daily_conclusion_ledger(day)
    day_path = write_json(f'daily_conclusion_ledger_{day}.json', payload)
    ledger_path = os.path.join(OUT_DIR, 'learning_conclusion_ledger.json')
    ledger = _read_json(ledger_path, {}) or {}
    rows = [r for r in ledger.get('days', []) if r.get('day') != day]
    rows.append(payload)
    rows.sort(key=lambda r: r.get('day') or '')
    ledger = {
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'days': rows[-60:],
        'latest_day': day,
        'latest': payload,
    }
    with open(ledger_path, 'w', encoding='utf-8') as f:
        json.dump(ledger, f, indent=2, default=str)
    return day_path, payload


def build_matched_control_review(day: str, lookback: int = 10) -> dict:
    days = _available_postmortem_days(day, lookback)
    all_trades = []
    for d in days:
        for trade in _tape(d):
            all_trades.append({**trade, '_day': d})
    winners = [t for t in all_trades if float(t.get('pnl') or 0) > 0]
    control_levels = [
        ('strict', ('ticker', 'side', 'setup', 'btc_regime', 'spread', 'market_regime')),
        ('no_market', ('ticker', 'side', 'setup', 'btc_regime', 'spread')),
        ('no_spread', ('ticker', 'side', 'setup', 'btc_regime')),
        ('ticker_side_setup', ('ticker', 'side', 'setup')),
        ('side_setup', ('side', 'setup')),
    ]
    rows = []
    for loser in [t for t in _tape(day) if float(t.get('pnl') or 0) < 0]:
        matches = []
        match_level = None
        fields_used = None
        for level, fields in control_levels:
            key = _trade_context_key(loser, fields)
            controls = [
                w for w in winners
                if w.get('trade_id') != loser.get('trade_id')
                and _trade_context_key(w, fields) == key
            ]
            if controls:
                matches = controls
                match_level = level
                fields_used = fields
                break
        summary = _control_summary(matches)
        if not matches:
            deduction = 'no_winning_controls_found'
        elif summary.get('win_rate') is not None and summary['win_rate'] >= 70 and summary['pnl'] > 0:
            deduction = 'counterexamples_present_do_not_overfit'
        elif summary.get('pnl', 0) < 0:
            deduction = 'matched_context_also_weak'
        else:
            deduction = 'mixed_controls_watch_only'
        rows.append({
            **_trade_example(loser),
            'match_level': match_level or 'none',
            'fields_used': list(fields_used or []),
            'context': {
                'ticker': loser.get('ticker'),
                'side': loser.get('side'),
                'setup': _setup_of(loser),
                'btc_regime': _btc_regime_of(loser),
                'market_regime': _market_regime_bucket(loser),
                'spread': _spread_bucket(loser),
                'grade': _setup_grade(loser).get('grade'),
            },
            'controls': summary,
            'deduction': deduction,
        })
    rows.sort(key=lambda r: (r['deduction'] != 'matched_context_also_weak', r.get('pnl') or 0))
    return {
        'day': day,
        'lookback': lookback,
        'days': days,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'purpose': (
            'Compares each loser with similar winning trades before promoting a rule. '
            'Counterexamples lower confidence and reduce overfitting.'
        ),
        'rows': rows,
        'deduction_counts': dict(Counter(r['deduction'] for r in rows)),
        'promotion_note': 'Treat loser patterns as actionable only when matched controls are also weak or absent across clean days.',
    }


def build_exit_efficiency_review(day: str) -> dict:
    rows = []
    buckets = defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0, 'examples': []})
    for trade in _tape(day):
        pnl = float(trade.get('pnl') or 0)
        signed = _signed_return_pct(trade)
        mfe = _num(trade.get('mfe_pct'))
        mae = _num(trade.get('mae_pct'))
        fwd5 = _fwd(trade, 5)
        fwd15 = _fwd(trade, 15)
        capture = None
        if signed is not None and mfe is not None and mfe > 0:
            capture = round(signed / mfe * 100, 1)
        giveback = round(mfe - signed, 4) if mfe is not None and signed is not None else None
        post_exit_proxy = None
        if signed is not None and fwd5 is not None:
            post_exit_proxy = round(fwd5 - signed, 4)
        if pnl < 0 and mfe is not None and mfe >= 0.15 and signed is not None and signed < 0:
            label = 'gave_back_open_profit'
        elif pnl < 0 and (mfe is None or mfe < 0.08) and (mae is None or mae >= 0.15):
            label = 'bad_entry_or_fast_fail'
        elif pnl < 0 and fwd5 is not None and signed is not None and fwd5 < signed - 0.10:
            label = 'good_loss_cut_before_worse'
        elif pnl > 0 and post_exit_proxy is not None and post_exit_proxy > 0.15:
            label = 'profit_taken_but_left_followthrough'
        elif pnl > 0 and post_exit_proxy is not None and post_exit_proxy < -0.15:
            label = 'good_exit_before_reversal'
        elif pnl > 0:
            label = 'winner_exit_acceptable'
        else:
            label = 'exit_quality_unclear'
        row = {
            **_trade_example(trade),
            'signed_return_pct': signed,
            'mfe_pct': mfe,
            'mae_pct': mae,
            'capture_ratio_pct': capture,
            'giveback_pct': giveback,
            'fwd5_signed_return_pct': fwd5,
            'fwd15_signed_return_pct': fwd15,
            'post_exit_proxy_5m_minus_exit_pct': post_exit_proxy,
            'label': label,
            'note': 'Post-exit proxy uses fixed horizon from entry when true post-exit bars are unavailable.',
        }
        rows.append(row)
        bucket = buckets[label]
        bucket['trades'] += 1
        bucket['wins'] += 1 if pnl > 0 else 0
        bucket['losses'] += 1 if pnl < 0 else 0
        bucket['pnl'] += pnl
        if len(bucket['examples']) < 8:
            bucket['examples'].append(row)
    summary = []
    for label, bucket in buckets.items():
        summary.append({
            'label': label,
            'trades': bucket['trades'],
            'wins': bucket['wins'],
            'losses': bucket['losses'],
            'win_rate': _safe_pct(bucket['wins'], bucket['trades']),
            'pnl': round(bucket['pnl'], 2),
            'examples': bucket['examples'][:8],
        })
    summary.sort(key=lambda r: (r['label'] not in ('gave_back_open_profit', 'bad_entry_or_fast_fail'), r['pnl']))
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'purpose': 'Separates bad entries from slow exits and profit giveback.',
        'summary': summary,
        'rows': rows,
    }


def build_mae_mfe_timeline(day: str) -> dict:
    labels = ('15s', '30s', '1m', '2m', '3m', '5m')
    rows = []
    buckets = defaultdict(lambda: {
        'trades': 0,
        'wins': 0,
        'losses': 0,
        'confirmed_by_checkpoint': Counter(),
        'invalidated_by_checkpoint': Counter(),
        'missing_by_checkpoint': Counter(),
        'time_to_mfe': [],
        'time_to_mae': [],
        'examples': [],
    })
    for trade in _tape(day):
        path = _path_of(trade)
        checkpoints = []
        for label in labels:
            chk = _checkpoint(trade, label)
            signed = chk.get('signed_return_pct')
            status = _checkpoint_status(signed)
            checkpoints.append({
                'checkpoint': label,
                'signed_return_pct': _num(signed),
                'price': chk.get('price'),
                'status': status,
            })
        first_green = None
        first_red = None
        opened = trade.get('opened_at')
        try:
            if opened and path.get('first_green_ts'):
                first_green = int(path.get('first_green_ts') - opened)
            if opened and path.get('first_red_ts'):
                first_red = int(path.get('first_red_ts') - opened)
        except Exception:
            pass
        row = {
            **_trade_example(trade),
            'result': 'winner' if float(trade.get('pnl') or 0) > 0 else ('loser' if float(trade.get('pnl') or 0) < 0 else 'flat'),
            'mfe_pct': _num(trade.get('mfe_pct')),
            'mae_pct': _num(trade.get('mae_pct')),
            'time_to_mfe_sec': path.get('time_to_mfe_sec'),
            'time_to_mae_sec': path.get('time_to_mae_sec'),
            'first_green_sec': first_green,
            'first_red_sec': first_red,
            'checkpoints': checkpoints,
        }
        rows.append(row)
        key = '|'.join([str(trade.get('side') or 'unknown'), str(_setup_of(trade)), row['result']])
        bucket = buckets[key]
        bucket['trades'] += 1
        bucket['wins'] += 1 if row['result'] == 'winner' else 0
        bucket['losses'] += 1 if row['result'] == 'loser' else 0
        if path.get('time_to_mfe_sec') is not None:
            bucket['time_to_mfe'].append(float(path.get('time_to_mfe_sec')))
        if path.get('time_to_mae_sec') is not None:
            bucket['time_to_mae'].append(float(path.get('time_to_mae_sec')))
        for chk in checkpoints:
            if chk['status'] in ('confirmed', 'slightly_green'):
                bucket['confirmed_by_checkpoint'][chk['checkpoint']] += 1
            elif chk['status'] in ('invalidated', 'slightly_red'):
                bucket['invalidated_by_checkpoint'][chk['checkpoint']] += 1
            else:
                bucket['missing_by_checkpoint'][chk['checkpoint']] += 1
        if len(bucket['examples']) < 8:
            bucket['examples'].append(row)
    summary = []
    for key, bucket in buckets.items():
        summary.append({
            'bucket': key,
            'trades': bucket['trades'],
            'wins': bucket['wins'],
            'losses': bucket['losses'],
            'avg_time_to_mfe_sec': _avg(bucket['time_to_mfe']),
            'avg_time_to_mae_sec': _avg(bucket['time_to_mae']),
            'confirmed_by_checkpoint': dict(bucket['confirmed_by_checkpoint']),
            'invalidated_by_checkpoint': dict(bucket['invalidated_by_checkpoint']),
            'missing_by_checkpoint': dict(bucket['missing_by_checkpoint']),
            'examples': bucket['examples'][:8],
        })
    summary.sort(key=lambda r: (-r['losses'], r['bucket']))
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'purpose': 'Shows whether trades confirmed or invalidated quickly after entry, using all available path checkpoints.',
        'checkpoint_labels': labels,
        'summary': summary,
        'rows': rows,
        'data_note': 'Missing 15s/2m/5m checkpoints are surfaced explicitly; current tape often has 30s/1m/3m.',
    }


def _hypothesis_trade_match(hypothesis: dict, trade: dict) -> tuple[bool, list[str]]:
    text = ' '.join(str(hypothesis.get(k) or '') for k in ('key', 'monitor', 'deduction')).lower()
    side = str(trade.get('side') or '').lower()
    criteria = []
    if 'short' in text:
        criteria.append('side=SHORT')
        if side != 'short':
            return False, criteria
    if 'long' in text:
        criteria.append('side=LONG')
        if side != 'long':
            return False, criteria
    ind = _indicators(trade)
    btc = _btc_context(trade)
    if 'flow_120s' in text and 'buy_pct' in text:
        threshold = 60.0
        match = re.search(r'flow_120s[^\d]*(?:buy_pct)?[^\d]*(?:>=|>|at least)\s*(\d+(?:\.\d+)?)', text)
        if match:
            threshold = float(match.group(1))
        val = _num(_nested(ind, 'flow_120s', 'buy_pct'))
        criteria.append(f'flow_120s_buy_pct>={threshold:g}')
        if val is None or val < threshold:
            return False, criteria
    if 'mom_60s' in text and '> 0' in text:
        val = _num(ind.get('mom_60s'))
        criteria.append('mom_60s>0')
        if val is None or val <= 0:
            return False, criteria
    if 'weak-spread' in text or 'weak spread' in text or 'ema_15s/60s' in text:
        e15 = _num(ind.get('ema_15s'))
        e60 = _num(ind.get('ema_60s'))
        price = _num(ind.get('price') or trade.get('entry'))
        spread = abs(e15 - e60) / price * 100 if e15 is not None and e60 is not None and price else None
        criteria.append('ema_15s_60s_spread_near_zero')
        if spread is None or spread > 0.03:
            return False, criteria
    hour_match = re.search(r'hours?\s+(\d{1,2})\s*-\s*(\d{1,2})', text)
    if hour_match:
        start = int(hour_match.group(1))
        end = int(hour_match.group(2))
        try:
            hh = int(str(trade.get('time') or '00:00').split(':')[0])
        except Exception:
            hh = -1
        criteria.append(f'hour_{start}_{end}')
        if not (start <= hh < end):
            return False, criteria
    if 'non-bearish btc' in text or 'btc was not bearish' in text or 'btc not bearish' in text:
        regime = str(btc.get('regime') or btc.get('regime_detail') or btc.get('ema_stack') or '').lower()
        mom = _num(btc.get('mom_60s'))
        criteria.append('btc_not_bearish')
        if 'bear' in regime or (mom is not None and mom < -0.05):
            return False, criteria
    if not criteria:
        criteria.append('text_no_specific_machine_criteria')
        return False, criteria
    return True, criteria


def build_hypothesis_counterexamples(day: str) -> dict:
    hyp_state = _read_json(os.path.join(OUT_DIR, 'hypotheses.json'), {}) or {}
    rows = []
    for h in hyp_state.get('active', []) or []:
        evidence_losers = []
        counterexample_winners = []
        neutral = []
        criteria_seen = set()
        for trade in _tape(day):
            matched, criteria = _hypothesis_trade_match(h, trade)
            criteria_seen.update(criteria)
            if not matched:
                continue
            pnl = float(trade.get('pnl') or 0)
            ex = _trade_example(trade)
            ex['criteria'] = criteria
            if pnl > 0:
                counterexample_winners.append(ex)
            elif pnl < 0:
                evidence_losers.append(ex)
            else:
                neutral.append(ex)
        total = len(evidence_losers) + len(counterexample_winners) + len(neutral)
        counter_rate = _safe_pct(len(counterexample_winners), total)
        rows.append({
            'id': h.get('id'),
            'key': h.get('key'),
            'monitor': h.get('monitor'),
            'criteria_detected': sorted(criteria_seen),
            'matched_trades': total,
            'evidence_losers': len(evidence_losers),
            'counterexample_winners': len(counterexample_winners),
            'neutral': len(neutral),
            'counterexample_rate': counter_rate,
            'confidence_effect': (
                'lower_confidence_counterexamples_present'
                if counterexample_winners else
                'no_counterexamples_today'
                if total else
                'not_observed_today'
            ),
            'examples': {
                'losers': evidence_losers[:8],
                'winners': counterexample_winners[:8],
            },
        })
    rows.sort(key=lambda r: (-r['counterexample_winners'], -r['evidence_losers'], r.get('id') or ''))
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'purpose': 'Tracks when active hypotheses also appear on winners, so the bot does not promote brittle rules.',
        'rows': rows,
        'counts': {
            'hypotheses': len(rows),
            'with_counterexamples': sum(1 for r in rows if r['counterexample_winners']),
            'with_loser_evidence': sum(1 for r in rows if r['evidence_losers']),
        },
    }


def build_loser_fingerprints(day: str) -> dict:
    rows = []
    buckets = defaultdict(lambda: {'losers': 0, 'gross_loss': 0.0, 'examples': []})
    for trade in _tape(day):
        if float(trade.get('pnl') or 0) >= 0:
            continue
        fp = _loser_fingerprint(trade)
        row = {**_trade_example(trade), **fp}
        rows.append(row)
        bucket = buckets[fp['primary']]
        bucket['losers'] += 1
        bucket['gross_loss'] += abs(float(trade.get('pnl') or 0))
        if len(bucket['examples']) < 8:
            bucket['examples'].append(row)
    summary = []
    for key, bucket in buckets.items():
        summary.append({
            'fingerprint': key,
            'losers': bucket['losers'],
            'gross_loss': round(bucket['gross_loss'], 2),
            'examples': bucket['examples'][:8],
        })
    summary.sort(key=lambda r: (-r['gross_loss'], r['fingerprint']))
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'purpose': 'Labels losing trades into repeatable root-cause fingerprints for faster daily review.',
        'summary': summary,
        'rows': rows,
    }


def build_postmortem_section_confidence(day: str) -> dict:
    tape = _tape(day)
    losers = [t for t in tape if float(t.get('pnl') or 0) < 0]
    fwd_ok = sum(1 for t in tape if t.get('fwd_status') in (None, 'ok') and any(_fwd(t, m) is not None for m in (5, 15, 60)))
    path_ok = sum(1 for t in tape if _path_of(t))
    hyp = build_hypothesis_confidence(day)
    matched = build_matched_control_review(day)

    def verdict(samples: int, needed: int, coverage_pct: Optional[float] = None) -> str:
        if samples <= 0:
            return 'ignore_no_data'
        if samples < needed:
            return 'watch_only_low_sample'
        if coverage_pct is not None and coverage_pct < 60:
            return 'watch_only_low_coverage'
        return 'usable'

    sections = [
        {
            'section': 'daily_risk',
            'samples': len(tape),
            'needed': 5,
            'coverage_pct': 100.0 if tape else 0.0,
        },
        {
            'section': 'loser_fingerprints',
            'samples': len(losers),
            'needed': 3,
            'coverage_pct': 100.0 if losers else 0.0,
        },
        {
            'section': 'forward_returns',
            'samples': fwd_ok,
            'needed': max(1, min(5, len(tape))),
            'coverage_pct': _safe_pct(fwd_ok, len(tape)) if tape else 0,
        },
        {
            'section': 'mae_mfe_timeline',
            'samples': path_ok,
            'needed': max(1, min(5, len(tape))),
            'coverage_pct': _safe_pct(path_ok, len(tape)) if tape else 0,
        },
        {
            'section': 'matched_controls',
            'samples': sum(1 for r in matched.get('rows') or [] if _nested(r, 'controls', 'controls')),
            'needed': max(1, min(3, len(losers))),
            'coverage_pct': _safe_pct(sum(1 for r in matched.get('rows') or [] if _nested(r, 'controls', 'controls')), len(losers)) if losers else 0,
        },
        {
            'section': 'hypothesis_confidence',
            'samples': len(hyp.get('rows') or []),
            'needed': 1,
            'coverage_pct': 100.0 if hyp.get('rows') else 0.0,
        },
    ]
    for section in sections:
        section['verdict'] = verdict(section['samples'], section['needed'], section.get('coverage_pct'))
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'purpose': 'Marks each postmortem section as usable/watch-only/ignore so humans and Codex do not overweight weak data.',
        'sections': sections,
        'counts': dict(Counter(s['verdict'] for s in sections)),
    }


def build_trade_verdicts(day: str) -> dict:
    rows = []
    for trade in _tape(day):
        pnl = float(trade.get('pnl') or 0)
        grade = _setup_grade(trade)
        fp = _loser_fingerprint(trade) if pnl < 0 else {'primary': None, 'tags': []}
        fill_adverse = max([
            _num(r.get('adverse_fill_vs_mid_pct')) or 0
            for r in trade.get('fill_attribution') or []
        ], default=0)
        timing_improvement = max([
            _num(row.get('could_have_improved_entry_pct')) or 0
            for row in (trade.get('entry_timing') or {}).get('windows_sec', {}).values()
        ], default=0)
        latency_ms = _num(_nested(trade, 'latency_attribution', 'from_signal_ms', 'entry_filled_from_signal_ms'))
        if pnl < 0 and fill_adverse >= 0.12:
            idea_execution_verdict = 'good_or_unclear_thesis_bad_fill'
        elif pnl < 0 and timing_improvement >= 0.12:
            idea_execution_verdict = 'entry_timing_cost'
        elif pnl < 0 and latency_ms is not None and latency_ms >= 1500:
            idea_execution_verdict = 'execution_delay_cost'
        elif pnl < 0 and fp['primary'] in ('btc_disagreement_or_stale', 'bad_entry_location', 'failed_flow_fade_context'):
            idea_execution_verdict = 'bad_thesis_or_context'
        elif pnl < 0 and fp['primary'] in ('gave_back_open_profit', 'exit_too_slow_or_stale_thesis'):
            idea_execution_verdict = 'bad_exit_after_working_entry'
        elif pnl < 0:
            idea_execution_verdict = 'unclear_loss_collect_more_data'
        elif pnl > 0 and fill_adverse >= 0.12:
            idea_execution_verdict = 'winner_despite_bad_fill'
        elif pnl > 0:
            idea_execution_verdict = 'idea_and_execution_worked'
        else:
            idea_execution_verdict = 'flat_no_verdict'
        if fp['primary'] == 'operational_or_forced_exit':
            verdict = 'exclude_from_strategy_learning'
            reason = 'Exit was operational/forced, not a clean strategy read.'
        elif pnl < 0 and fp['primary'] in ('execution_or_slippage', 'btc_disagreement_or_stale'):
            verdict = 'would_skip'
            reason = f"Loser had {fp['primary']}."
        elif pnl < 0 and grade.get('grade') == 'C':
            verdict = 'would_skip'
            reason = 'Marginal C-grade loser; entry quality needs repair.'
        elif pnl < 0 and fp['primary'] in ('gave_back_open_profit', 'exit_too_slow_or_stale_thesis'):
            verdict = 'watch_exit_policy'
            reason = 'Entry may have worked briefly; review exit timing before banning setup.'
        elif pnl < 0:
            verdict = 'would_take_again_watch'
            reason = 'Loss does not yet show a clear avoidable defect.'
        elif pnl > 0 and grade.get('grade') == 'C':
            verdict = 'winner_but_do_not_overtrust'
            reason = 'Trade won despite marginal entry grade; keep as counterexample, not proof.'
        elif pnl > 0:
            verdict = 'would_take_again'
            reason = 'Winner aligned with current recorded criteria.'
        else:
            verdict = 'neutral'
            reason = 'Flat result.'
        rows.append({
            **_trade_example(trade),
            'setup_grade_at_entry': grade,
            'fingerprint': fp.get('primary'),
            'idea_execution_verdict': idea_execution_verdict,
            'fill_adverse_pct': fill_adverse,
            'entry_timing_improvement_available_pct': timing_improvement,
            'entry_fill_latency_ms': latency_ms,
            'verdict': verdict,
            'one_line': f"{trade.get('ticker')} {trade.get('side')} {trade.get('time')}: {verdict} - {reason}",
        })
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'purpose': 'One-line answer per trade: take again, skip, watch exit, or exclude from strategy learning.',
        'rows': rows,
        'counts': dict(Counter(r['verdict'] for r in rows)),
    }


def build_best_avoided_loser_simulator(day: str) -> dict:
    tape = _tape(day)

    def has_fingerprint(name: str):
        return lambda t: float(t.get('pnl') or 0) < 0 and name in _loser_fingerprint(t).get('tags', [])

    rules = [
        ('avoid_c_grade_entries', lambda t: _setup_grade(t).get('grade') == 'C', 'Entry quality'),
        ('avoid_wide_spread_entries', lambda t: _spread_bucket(t) == 'wide_spread', 'Execution'),
        ('avoid_btc_disagreement_or_stale', lambda t: 'btc_disagreement_or_stale' in _loser_fingerprint(t).get('tags', []), 'BTC context'),
        ('avoid_stretched_vwap_location', lambda t: 'stretched_vwap_location' in _loser_fingerprint(t).get('tags', []), 'Location'),
        ('avoid_failed_flow_fade_context', lambda t: any(tag in _loser_fingerprint(t).get('tags', []) for tag in (
            'failed_flow_fade_or_sustained_buying',
            'failed_flow_fade_or_sustained_selling',
        )), 'Flow'),
        ('avoid_bad_quote_state', lambda t: _nested(t, 'forensics', 'entry_quality', 'quote_state', 'state') in ('locked', 'crossed', 'one_sided', 'missing', 'invalid'), 'Execution'),
        ('avoid_abnormal_spread_vs_ticker_baseline', lambda t: bool(_nested(t, 'forensics', 'entry_quality', 'spread_abnormal')), 'Execution'),
        ('require_dual_side_gap_2', lambda t: (_num(_nested(t, 'entry_thesis', 'shadow_dual_side_score', 'side_gap'))
                                               or _num(_nested(t, 'forensics', 'shadow_dual_side_score', 'side_gap'))
                                               or 99) < 2.0, 'Side selection'),
        ('exit_faster_after_immediate_thesis_failure', has_fingerprint('immediate_thesis_failure'), 'Exit timing'),
        ('protect_profit_after_mfe_giveback', has_fingerprint('gave_back_open_profit'), 'Exit timing'),
    ]
    rows = []
    for name, predicate, category in rules:
        blocked = [t for t in tape if predicate(t)]
        losers = [t for t in blocked if float(t.get('pnl') or 0) < 0]
        winners = [t for t in blocked if float(t.get('pnl') or 0) > 0]
        avoided_loss = round(abs(sum(float(t.get('pnl') or 0) for t in losers)), 2)
        winner_damage = round(sum(float(t.get('pnl') or 0) for t in winners), 2)
        net_impact = round(avoided_loss - winner_damage, 2)
        if not blocked:
            confidence = 'no_data'
        elif len(blocked) < 3:
            confidence = 'watch_only_low_sample'
        elif net_impact > 0 and winner_damage <= avoided_loss * 0.4:
            confidence = 'promising_needs_multi_day'
        else:
            confidence = 'do_not_promote_winner_damage'
        rows.append({
            'rule': name,
            'category': category,
            'blocked_trades': len(blocked),
            'losers_avoided': len(losers),
            'winners_blocked': len(winners),
            'avoided_loss': avoided_loss,
            'winner_damage': winner_damage,
            'net_pnl_if_blocked': net_impact,
            'confidence': confidence,
            'blocked_examples': [_trade_example(t) for t in blocked[:10]],
        })
    rows.sort(key=lambda r: (r['confidence'] not in ('promising_needs_multi_day',), -r['net_pnl_if_blocked']))
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'purpose': 'Shows which hypothetical avoid/exit rules would have reduced loser damage and how many winners they would have damaged.',
        'rows': rows,
        'promotion_note': 'Simulator output is not a live change. Promote only after clean multi-day evidence and matched-control review.',
    }


def build_rolling_learning_dashboard(day: str, lookback: int = 10) -> dict:
    days = _available_postmortem_days(day, lookback)
    daily = []
    archetypes = Counter()
    pnl_total = 0.0
    trades_total = 0
    for d in days:
        risk = build_risk_summary(d)
        fp = build_loser_fingerprints(d)
        hyp = build_hypothesis_confidence(d)
        conclusion = build_daily_conclusion_ledger(d)
        pnl_total += float(risk.get('pnl') or 0)
        trades_total += int(risk.get('trades') or 0)
        for row in fp.get('summary') or []:
            archetypes[row.get('fingerprint')] += int(row.get('losers') or 0)
        daily.append({
            'day': d,
            'pnl': risk.get('pnl'),
            'trades': risk.get('trades'),
            'win_rate': risk.get('win_rate'),
            'top_loser_fingerprint': (fp.get('summary') or [{}])[0].get('fingerprint'),
            'hypothesis_counts': hyp.get('counts'),
            'watch_next': (conclusion.get('what_to_watch_next') or [])[:3],
        })
    return {
        'day': day,
        'lookback': lookback,
        'days': days,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'purpose': 'Rolling executive dashboard for learning progress, recurring loser types, and hypothesis maturity.',
        'totals': {
            'trades': trades_total,
            'pnl': round(pnl_total, 2),
            'days': len(days),
        },
        'recurring_loser_fingerprints': [
            {'fingerprint': k, 'losers': v}
            for k, v in archetypes.most_common(10)
        ],
        'daily': daily,
        'rule': 'This is the weekly learning lens; it summarizes, never auto-edits live rules.',
    }


def _estimate_pnl_at_signed_pct(trade: dict, signed_pct: Optional[float]) -> Optional[float]:
    pct = _num(signed_pct)
    if pct is None:
        return None
    entry = _num(trade.get('entry'))
    qty = _num(trade.get('qty') or trade.get('shares') or _nested(trade, 'entry_thesis', 'qty'))
    if entry is not None and qty is not None:
        return round(entry * qty * pct / 100.0, 2)
    actual_signed = _signed_return_pct(trade)
    actual_pnl = _num(trade.get('pnl'))
    if actual_signed is not None and abs(actual_signed) > 0.0001 and actual_pnl is not None:
        return round(actual_pnl * (pct / actual_signed), 2)
    return None


def _learning_policy_catalog() -> list[dict]:
    return [
        {
            'name': 'skip_c_grade_entries',
            'kind': 'entry_skip',
            'description': 'Skip trades graded C at entry.',
            'predicate': lambda t: _setup_grade(t).get('grade') == 'C',
        },
        {
            'name': 'skip_wide_spread_entries',
            'kind': 'entry_skip',
            'description': 'Skip trades entered with wide spread context.',
            'predicate': lambda t: _spread_bucket(t) == 'wide_spread',
        },
        {
            'name': 'skip_btc_disagreement_or_stale',
            'kind': 'entry_skip',
            'description': 'Skip entries where BTC disagreed with the chosen side or was stale.',
            'predicate': lambda t: 'btc_disagreement_or_stale' in _loser_fingerprint(t).get('tags', []),
        },
        {
            'name': 'skip_failed_flow_fade_context',
            'kind': 'entry_skip',
            'description': 'Skip fade entries when flow looks sustained instead of rolling over.',
            'predicate': lambda t: any(tag in _loser_fingerprint(t).get('tags', []) for tag in (
                'failed_flow_fade_or_sustained_buying',
                'failed_flow_fade_or_sustained_selling',
            )),
        },
        {
            'name': 'exit_30s_thesis_invalidated',
            'kind': 'exit_replay',
            'description': 'Exit at 30 seconds when the thesis is already invalidated.',
            'checkpoint': '30s',
            'predicate': lambda t: _checkpoint_status(_checkpoint(t, '30s').get('signed_return_pct')) == 'invalidated',
        },
        {
            'name': 'exit_1m_thesis_invalidated',
            'kind': 'exit_replay',
            'description': 'Exit at 1 minute when the thesis remains invalidated.',
            'checkpoint': '1m',
            'predicate': lambda t: _checkpoint_status(_checkpoint(t, '1m').get('signed_return_pct')) == 'invalidated',
        },
        {
            'name': 'protect_mfe_giveback_floor',
            'kind': 'exit_replay',
            'description': 'If a trade had at least +0.15% MFE but closed red, replay a flat protective exit.',
            'checkpoint': None,
            'predicate': lambda t: (_num(t.get('mfe_pct')) or 0) >= 0.15 and float(t.get('pnl') or 0) < 0,
            'fixed_signed_pct': 0.0,
        },
    ]


def build_true_counterfactual_replay(day: str, lookback: int = 10) -> dict:
    days = _available_postmortem_days(day, lookback)
    policies = _learning_policy_catalog()
    policy_rows = []
    for policy in policies:
        trades = []
        actual_total = 0.0
        simulated_total = 0.0
        affected = 0
        winners_damaged = 0
        losers_helped = 0
        unknown_replay = 0
        predicate_errors = 0
        for d in days:
            for trade in _tape(d):
                actual = float(trade.get('pnl') or 0)
                actual_total += actual
                simulated = actual
                action = 'unchanged'
                try:
                    matched_policy = bool(policy['predicate'](trade))
                except Exception:
                    matched_policy = False
                    predicate_errors += 1
                if matched_policy:
                    affected += 1
                    if policy['kind'] == 'entry_skip':
                        simulated = 0.0
                        action = 'skipped'
                    elif policy['kind'] == 'exit_replay':
                        if policy.get('fixed_signed_pct') is not None:
                            replay_pnl = _estimate_pnl_at_signed_pct(trade, policy.get('fixed_signed_pct'))
                        else:
                            chk = _checkpoint(trade, policy.get('checkpoint') or '')
                            replay_pnl = _estimate_pnl_at_signed_pct(trade, chk.get('signed_return_pct'))
                        if replay_pnl is None:
                            unknown_replay += 1
                            action = 'matched_but_unpriced'
                        else:
                            simulated = replay_pnl
                            action = f"exit_at_{policy.get('checkpoint') or 'protective_floor'}"
                    if actual > 0 and simulated < actual:
                        winners_damaged += 1
                    if actual < 0 and simulated > actual:
                        losers_helped += 1
                    trades.append({
                        'day': d,
                        **_trade_example(trade),
                        'actual_pnl': round(actual, 2),
                        'simulated_pnl': round(simulated, 2),
                        'delta_vs_actual': round(simulated - actual, 2),
                        'action': action,
                        'setup_grade': _setup_grade(trade),
                        'fingerprint': _loser_fingerprint(trade).get('primary') if actual < 0 else None,
                    })
                simulated_total += simulated
        net_delta = round(simulated_total - actual_total, 2)
        winner_damage = round(sum(min(0.0, r['delta_vs_actual']) for r in trades if r['actual_pnl'] > 0), 2)
        loser_benefit = round(sum(max(0.0, r['delta_vs_actual']) for r in trades if r['actual_pnl'] < 0), 2)
        if affected == 0:
            verdict = 'no_data'
        elif affected < 3:
            verdict = 'watch_only_low_sample'
        elif unknown_replay:
            verdict = 'watch_only_incomplete_pricing'
        elif net_delta > 0 and winner_damage >= -max(25.0, loser_benefit * 0.35):
            verdict = 'promising_needs_multi_day'
        elif net_delta > 0:
            verdict = 'blocked_by_winner_damage'
        else:
            verdict = 'do_not_promote'
        policy_rows.append({
            'policy': policy['name'],
            'kind': policy['kind'],
            'description': policy['description'],
            'days': days,
            'baseline_pnl': round(actual_total, 2),
            'simulated_pnl': round(simulated_total, 2),
            'net_delta_vs_actual': net_delta,
            'affected_trades': affected,
            'losers_helped': losers_helped,
            'winners_damaged': winners_damaged,
            'winner_damage': winner_damage,
            'loser_benefit': loser_benefit,
            'unknown_replay_count': unknown_replay,
            'predicate_error_count': predicate_errors,
            'verdict': verdict,
            'sample': trades[:25],
        })
    policy_rows.sort(key=lambda r: (r['verdict'] != 'promising_needs_multi_day', -r['net_delta_vs_actual']))
    return {
        'day': day,
        'lookback': lookback,
        'days': days,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'purpose': 'Replay proposed skip/exit policies against actual trades and price-path checkpoints to estimate net impact and winner damage.',
        'policies': policy_rows,
        'promotion_note': 'Evidence only. Live rules still require human approval and multi-day confirmation.',
    }


def build_feature_outcome_table(day: str, lookback: int = 10) -> dict:
    days = _available_postmortem_days(day, lookback)
    rows = []
    for d in days:
        for trade in _tape(d):
            ind = _indicators(trade)
            btc = _btc_context(trade)
            fp = _loser_fingerprint(trade) if float(trade.get('pnl') or 0) < 0 else {'primary': None, 'tags': []}
            rows.append({
                'day': d,
                'trade_id': trade.get('trade_id'),
                'time': trade.get('time'),
                'ticker': trade.get('ticker'),
                'side': trade.get('side'),
                'setup_type': _setup_of(trade),
                'opened_at': trade.get('opened_at'),
                'closed_at': trade.get('closed_at'),
                'entry': _num(trade.get('entry')),
                'exit': _num(trade.get('exit')),
                'qty': _num(trade.get('qty') or trade.get('shares') or _nested(trade, 'entry_thesis', 'qty')),
                'entry_score': _entry_score(trade),
                'setup_grade': _setup_grade(trade).get('grade'),
                'market_regime': _market_regime_bucket(trade),
                'btc_regime': _btc_regime_of(trade),
                'btc_mom_15s': _num(btc.get('mom_15s')),
                'btc_mom_60s': _num(btc.get('mom_60s')),
                'stock_mom_15s': _num(ind.get('mom_15s')),
                'stock_mom_60s': _num(ind.get('mom_60s')),
                'ema_stack': ind.get('ema_stack'),
                'vwap_dist_sigma': _num(ind.get('vwap_dist_sigma')),
                'flow_30s_buy_pct': _num(_nested(ind, 'flow_30s', 'buy_pct')),
                'flow_120s_buy_pct': _num(_nested(ind, 'flow_120s', 'buy_pct')),
                'spread_bucket': _spread_bucket(trade),
                'spread_pct': _entry_spread_pct(trade),
                'pnl': round(float(trade.get('pnl') or 0), 2),
                'result': 'winner' if float(trade.get('pnl') or 0) > 0 else ('loser' if float(trade.get('pnl') or 0) < 0 else 'flat'),
                'reason': trade.get('reason'),
                'mfe_pct': _num(trade.get('mfe_pct')),
                'mae_pct': _num(trade.get('mae_pct')),
                'fwd5': _fwd(trade, 5),
                'fwd15': _fwd(trade, 15),
                'fwd60': _fwd(trade, 60),
                'loser_fingerprint': fp.get('primary'),
                'fingerprint_tags': fp.get('tags'),
            })
    return {
        'day': day,
        'lookback': lookback,
        'days': days,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'purpose': 'Canonical compact ML-style table: entry features -> outcome -> exit/path labels.',
        'rows': rows,
        'columns': list(rows[0].keys()) if rows else [],
    }


def _classify_day_regime(day: str) -> dict:
    tape = _tape(day)
    if not tape:
        return {'primary': 'no_trades', 'tags': ['no_trades']}
    btc_moms = [_num(_btc_context(t).get('mom_60s')) for t in tape]
    btc_vals = [v for v in btc_moms if v is not None]
    avg_btc = sum(btc_vals) / len(btc_vals) if btc_vals else 0.0
    range_positions = [_num(_btc_context(t).get('session_range_pos')) for t in tape]
    range_vals = [v for v in range_positions if v is not None]
    avg_range_pos = sum(range_vals) / len(range_vals) if range_vals else None
    market_counts = Counter(_market_regime_bucket(t) for t in tape)
    tags = []
    if avg_btc >= 0.08:
        tags.append('btc_trend_up')
    elif avg_btc <= -0.08:
        tags.append('btc_trend_down')
    else:
        tags.append('btc_chop')
    if avg_range_pos is not None and avg_range_pos >= 0.75:
        tags.append('btc_upper_range')
    elif avg_range_pos is not None and avg_range_pos <= 0.25:
        tags.append('btc_lower_range')
    top_market = market_counts.most_common(1)[0][0] if market_counts else 'market_unknown'
    if top_market != 'unknown':
        tags.append(top_market)
    early = [t for t in tape if str(t.get('time') or '') < '09:30']
    if len(early) >= max(3, len(tape) * 0.35):
        tags.append('opening_drive_active')
    fp = build_loser_fingerprints(day).get('summary') or []
    if fp and fp[0].get('fingerprint') in ('immediate_thesis_failure', 'stretched_vwap_location'):
        tags.append('fake_breakout_or_chop_risk')
    primary = '+'.join(tags[:2]) if tags else 'unclassified'
    return {
        'primary': primary,
        'tags': tags,
        'avg_btc_mom_60s': round(avg_btc, 4),
        'avg_btc_range_pos': round(avg_range_pos, 4) if avg_range_pos is not None else None,
        'market_regime_counts': dict(market_counts),
    }


def build_market_regime_library(day: str, lookback: int = 20) -> dict:
    days = _available_postmortem_days(day, lookback)
    regimes = defaultdict(lambda: {'days': 0, 'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0, 'examples': []})
    daily = []
    for d in days:
        risk = build_risk_summary(d)
        regime = _classify_day_regime(d)
        row = regimes[regime['primary']]
        row['days'] += 1
        row['trades'] += int(risk.get('trades') or 0)
        row['wins'] += int(risk.get('wins') or 0)
        row['losses'] += int(risk.get('losses') or 0)
        row['pnl'] += float(risk.get('pnl') or 0)
        if len(row['examples']) < 8:
            row['examples'].append({'day': d, 'pnl': risk.get('pnl'), 'tags': regime.get('tags')})
        daily.append({'day': d, **regime, 'pnl': risk.get('pnl'), 'trades': risk.get('trades'), 'win_rate': risk.get('win_rate')})
    summary = []
    for name, row in regimes.items():
        summary.append({
            'regime': name,
            'days': row['days'],
            'trades': row['trades'],
            'wins': row['wins'],
            'losses': row['losses'],
            'win_rate': _safe_pct(row['wins'], row['trades']),
            'pnl': round(row['pnl'], 2),
            'avg_pnl_per_day': round(row['pnl'] / row['days'], 2) if row['days'] else None,
            'examples': row['examples'],
        })
    summary.sort(key=lambda r: (r['pnl'], -r['days']))
    return {
        'day': day,
        'lookback': lookback,
        'days': days,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'purpose': 'Classifies days into reusable BTC/miner/broad-market regimes so future reviews compare like with like.',
        'daily': daily,
        'regimes': summary,
    }


def _human_feedback_template(day: str) -> dict:
    return {
        'day': day,
        'instructions': 'Optional human feedback. Edit decisions only; the bot reads this but never auto-edits live rules.',
        'decisions': [
            {
                'target_type': 'hypothesis|policy|trade|experiment',
                'target_id': '',
                'decision': 'agree|disagree|bad_data|do_not_consider|watch_harder|promote_manually|kill_manually',
                'note': '',
            }
        ],
    }


def ensure_human_feedback_template(day: str) -> str:
    path = os.path.join(OUT_DIR, f'human_feedback_{day}.json')
    os.makedirs(OUT_DIR, exist_ok=True)
    if not os.path.exists(path):
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(_human_feedback_template(day), f, indent=2, default=str)
    global_path = os.path.join(OUT_DIR, 'human_feedback.json')
    if not os.path.exists(global_path):
        with open(global_path, 'w', encoding='utf-8') as f:
            json.dump({'instructions': 'Optional cross-day human feedback.', 'decisions': []}, f, indent=2, default=str)
    return path


def build_human_feedback_summary(day: str) -> dict:
    ensure_human_feedback_template(day)
    day_fb = _read_json(os.path.join(OUT_DIR, f'human_feedback_{day}.json'), {}) or {}
    global_fb = _read_json(os.path.join(OUT_DIR, 'human_feedback.json'), {}) or {}
    decisions = []
    for source, payload in (('day', day_fb), ('global', global_fb)):
        for row in payload.get('decisions') or []:
            if not row.get('target_id') and source == 'day':
                continue
            decisions.append({'source': source, **row})
    counts = Counter(str(d.get('decision') or 'unknown') for d in decisions)
    by_target = defaultdict(list)
    for d in decisions:
        by_target[str(d.get('target_id') or 'unknown')].append(d)
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'purpose': 'Human feedback loop. These annotations influence learning confidence and weekly decisions, not live code.',
        'feedback_file': os.path.join(OUT_DIR, f'human_feedback_{day}.json'),
        'global_feedback_file': os.path.join(OUT_DIR, 'human_feedback.json'),
        'counts': dict(counts),
        'decisions': decisions,
        'by_target': dict(by_target),
    }


def build_rule_confidence_decay(day: str) -> dict:
    hyp = build_hypothesis_confidence(day)
    counters = build_hypothesis_counterexamples(day)
    feedback = build_human_feedback_summary(day)
    counter_by_id = {r.get('id'): r for r in counters.get('rows') or []}
    feedback_by_target = feedback.get('by_target') or {}
    rows = []
    for row in hyp.get('rows') or []:
        cid = row.get('id')
        counter = counter_by_id.get(cid) or {}
        base = float(row.get('confidence_score') or 0)
        counter_penalty = min(35, int(counter.get('counterexample_winners') or 0) * 12)
        stale_penalty = min(30, int(row.get('days_since_evidence') or 0) * 8)
        feedback_items = feedback_by_target.get(str(cid), [])
        feedback_penalty = 0
        feedback_boost = 0
        for item in feedback_items:
            decision = str(item.get('decision') or '')
            if decision in ('disagree', 'bad_data', 'do_not_consider', 'kill_manually'):
                feedback_penalty += 25
            elif decision in ('agree', 'watch_harder', 'promote_manually'):
                feedback_boost += 10
        decayed = max(0, min(100, round(base - counter_penalty - stale_penalty - feedback_penalty + feedback_boost, 1)))
        if decayed >= 75 and row.get('evidence_days', 0) >= 3:
            label = 'ready_for_human_review'
        elif decayed >= 55:
            label = 'building'
        elif decayed >= 30:
            label = 'weak_watch'
        else:
            label = 'decayed_or_rejected'
        rows.append({
            'id': cid,
            'key': row.get('key'),
            'monitor': row.get('monitor'),
            'base_confidence': base,
            'decayed_confidence': decayed,
            'label': label,
            'counterexample_winners': counter.get('counterexample_winners', 0),
            'counterexample_penalty': counter_penalty,
            'stale_penalty': stale_penalty,
            'feedback_penalty': feedback_penalty,
            'feedback_boost': feedback_boost,
            'feedback': feedback_items,
        })
    rows.sort(key=lambda r: (-r['decayed_confidence'], r.get('id') or ''))
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'purpose': 'Automatically lowers confidence when hypotheses go stale, collect counterexamples, or receive negative human feedback.',
        'rows': rows,
        'counts': dict(Counter(r['label'] for r in rows)),
    }


def build_experiment_registry(day: str, lookback: int = 10, replay: Optional[dict] = None,
                              human_feedback: Optional[dict] = None) -> dict:
    prior = _read_json(os.path.join(OUT_DIR, 'experiment_registry.json'), {}) or {'experiments': {}}
    experiments = dict(prior.get('experiments') or {})
    intent = _read_json(os.path.join(OUT_DIR, 'config_intent_ledger.json'), {}) or {}
    for key, item in (intent.get('items') or {}).items():
        exp_id = f"config:{key[:12]}"
        exp = experiments.setdefault(exp_id, {
            'id': exp_id,
            'kind': 'config_era',
            'name': item.get('era') or exp_id,
            'first_seen_day': item.get('first_seen_day') or day,
            'status': 'collecting',
            'why': item.get('why_changed'),
            'expected_impact': item.get('expected_impact') or [],
            'prove_worked': item.get('prove_worked') or [],
            'prove_failed': item.get('prove_failed') or [],
            'human_approval_required_for_live_rule_changes': True,
            'daily_metrics': [],
        })
        exp['last_seen_day'] = day
        exp['config_sha256'] = item.get('config_sha256') or key
    replay = replay or build_true_counterfactual_replay(day, lookback=lookback)
    for policy in replay.get('policies') or []:
        if policy.get('verdict') == 'no_data':
            continue
        exp_id = f"policy:{policy.get('policy')}"
        exp = experiments.setdefault(exp_id, {
            'id': exp_id,
            'kind': 'counterfactual_policy',
            'name': policy.get('policy'),
            'first_seen_day': day,
            'status': 'collecting',
            'why': policy.get('description'),
            'expected_impact': ['Improve net P&L without unacceptable winner damage.'],
            'prove_worked': ['Positive counterfactual net delta across multiple clean days.', 'Winner damage stays below threshold.'],
            'prove_failed': ['Net delta turns negative.', 'Winner damage outweighs loser benefit.'],
            'human_approval_required_for_live_rule_changes': True,
            'daily_metrics': [],
        })
        exp['last_seen_day'] = day
    risk = build_risk_summary(day)
    fb = human_feedback or build_human_feedback_summary(day)
    for exp in experiments.values():
        metrics = [m for m in exp.get('daily_metrics', []) if m.get('day') != day]
        policy = next((p for p in replay.get('policies') or [] if f"policy:{p.get('policy')}" == exp.get('id')), None)
        metric = {
            'day': day,
            'pnl': risk.get('pnl'),
            'win_rate': risk.get('win_rate'),
            'trades': risk.get('trades'),
        }
        if policy:
            metric.update({
                'counterfactual_delta': policy.get('net_delta_vs_actual'),
                'winner_damage': policy.get('winner_damage'),
                'affected_trades': policy.get('affected_trades'),
                'verdict': policy.get('verdict'),
            })
        metrics.append(metric)
        exp['daily_metrics'] = metrics[-lookback:]
        target_feedback = (fb.get('by_target') or {}).get(str(exp.get('id')), [])
        if any(f.get('decision') in ('kill_manually', 'do_not_consider') for f in target_feedback):
            exp['status'] = 'killed_by_human_feedback'
        elif any(f.get('decision') == 'promote_manually' for f in target_feedback):
            exp['status'] = 'promoted_by_human_feedback'
        elif policy and policy.get('verdict') == 'promising_needs_multi_day' and len(metrics) >= 2:
            exp['status'] = 'ready_for_human_review'
        elif len(metrics) >= 2:
            exp['status'] = 'collecting'
    return {
        'day': day,
        'lookback': lookback,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'purpose': 'Formal experiment cards for config eras and candidate policies. This is the audit trail for why a change exists and how it is judged.',
        'experiments': experiments,
        'counts': dict(Counter(exp.get('status') for exp in experiments.values())),
        'feedback_counts': fb.get('counts'),
    }


def write_experiment_registry(day: str, lookback: int = 10, replay: Optional[dict] = None,
                              human_feedback: Optional[dict] = None) -> tuple[str, str, dict]:
    payload = build_experiment_registry(day, lookback=lookback, replay=replay, human_feedback=human_feedback)
    registry_path = os.path.join(OUT_DIR, 'experiment_registry.json')
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(registry_path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, default=str)
    snapshot = write_json(f'experiment_registry_snapshot_{day}.json', payload)
    return registry_path, snapshot, payload


def build_weekly_promotion_meeting_packet(day: str, lookback: int = 10, replay: Optional[dict] = None,
                                          registry: Optional[dict] = None,
                                          confidence: Optional[dict] = None,
                                          regimes: Optional[dict] = None,
                                          feedback: Optional[dict] = None) -> dict:
    replay = replay or build_true_counterfactual_replay(day, lookback=lookback)
    feedback = feedback or build_human_feedback_summary(day)
    registry = registry or build_experiment_registry(day, lookback=lookback, replay=replay, human_feedback=feedback)
    confidence = confidence or build_rule_confidence_decay(day)
    regimes = regimes or build_market_regime_library(day)
    queue = _read_json(os.path.join(OUT_DIR, 'promotion_queue.json'), {}) or {}
    queue_items = queue.get('items') or {}
    if isinstance(queue_items, dict):
        queue_values = list(queue_items.values())
        queue_count = len(queue_items)
    elif isinstance(queue_items, list):
        queue_values = queue_items
        queue_count = len(queue_items)
    else:
        queue_values = []
        queue_count = 0
    decisions = {'promote': [], 'kill': [], 'keep_watching': [], 'needs_more_data': [], 'ignore_ops_or_bad_data': []}
    for policy in replay.get('policies') or []:
        item = {
            'type': 'counterfactual_policy',
            'name': policy.get('policy'),
            'evidence': {
                'net_delta_vs_actual': policy.get('net_delta_vs_actual'),
                'affected_trades': policy.get('affected_trades'),
                'winner_damage': policy.get('winner_damage'),
                'verdict': policy.get('verdict'),
            },
        }
        if policy.get('verdict') == 'promising_needs_multi_day':
            decisions['keep_watching'].append(item)
        elif policy.get('verdict') in ('do_not_promote', 'blocked_by_winner_damage'):
            decisions['kill'].append(item)
        elif policy.get('verdict') in ('watch_only_low_sample', 'watch_only_incomplete_pricing'):
            decisions['needs_more_data'].append(item)
    for row in confidence.get('rows') or []:
        item = {'type': 'hypothesis', 'name': row.get('id'), 'evidence': row}
        if row.get('label') == 'ready_for_human_review':
            decisions['promote'].append(item)
        elif row.get('label') == 'decayed_or_rejected':
            decisions['kill'].append(item)
        else:
            decisions['keep_watching'].append(item)
    for target, rows in (feedback.get('by_target') or {}).items():
        if any(r.get('decision') in ('bad_data', 'do_not_consider') for r in rows):
            decisions['ignore_ops_or_bad_data'].append({'target': target, 'feedback': rows})
    return {
        'day': day,
        'lookback': lookback,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'purpose': 'Weekly decision packet: daily postmortems observe; this packet decides promote/kill/watch/needs-more-data for human review.',
        'decisions': decisions,
        'experiment_counts': registry.get('counts'),
        'promotion_queue_counts': {
            'items': queue_count,
            'eligible': sum(1 for i in queue_values if isinstance(i, dict) and i.get('status') == 'eligible_for_human_review'),
        },
        'market_regime_library': {
            'regimes': (regimes.get('regimes') or [])[:10],
            'file': os.path.join(OUT_DIR, f'market_regime_library_{day}_20d.json'),
        },
        'required_human_action': (
            'Review promote/kill lists. Nothing is applied automatically; approved changes must be made explicitly.'
        ),
    }


def build_config_diff(day: str) -> dict:
    current_path = os.path.join(HERE, 'trading_config.json')
    current_hash = _sha256(current_path)
    prior = None
    if os.path.isdir(OUT_DIR):
        for name in sorted(os.listdir(OUT_DIR), reverse=True):
            if not name.startswith('artifacts_') or not name.endswith('.json'):
                continue
            d = name[len('artifacts_'):-len('.json')]
            if d < day:
                prior = _read_json(os.path.join(OUT_DIR, name), {})
                break
    prior_hash = (prior or {}).get('config_sha256')
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'current_config_sha256': current_hash,
        'previous_artifact_config_sha256': prior_hash,
        'changed_since_previous_artifact': (
            None if prior_hash is None else prior_hash != current_hash
        ),
        'note': 'Full threshold snapshots are stored on new trades going forward; historical diffs may only have hashes.',
    }


def build_review_index(day: str) -> dict:
    risk = build_risk_summary(day)
    losers = build_loser_summary(day)
    execution = build_execution_summary(day)
    coverage = build_feature_coverage(day)
    data_coverage = build_data_collection_coverage(day)
    gate_timeline = build_gate_timeline_summary(day)
    no_signal_summary = build_no_signal_snapshot_summary(day)
    fill_attribution = build_fill_attribution_summary(day)
    latency_attribution = build_latency_attribution_summary(day)
    entry_timing = build_entry_timing_efficiency(day)
    dual_side_shadow = build_dual_side_shadow_review(day)
    quote_condition = build_quote_condition_quality_summary(day)
    shadow_entry_variants = build_shadow_entry_variants_review(day)
    new_gate_attr = build_new_gate_attribution(day)
    shadow = build_shadow_exit_summary(day)
    retry = build_entry_retry_summary(day)
    ticker_learning = build_per_ticker_learning(day)
    regime_review = build_regime_scoring_review(day)
    score_calibration = build_score_calibration(day)
    market_tape = build_market_tape_attribution(day)
    tick_replay = _tick_replay(day)
    setup_grade = build_setup_grade_review(day)
    side_review = build_counterfactual_side_review(day)
    rule_review = build_rule_attribution_review(day)
    hyp_conf = build_hypothesis_confidence(day)
    conclusion = build_daily_conclusion_ledger(day)
    matched = build_matched_control_review(day)
    exit_eff = build_exit_efficiency_review(day)
    timeline = build_mae_mfe_timeline(day)
    hyp_counter = build_hypothesis_counterexamples(day)
    fingerprints = build_loser_fingerprints(day)
    section_conf = build_postmortem_section_confidence(day)
    verdicts = build_trade_verdicts(day)
    avoid = build_best_avoided_loser_simulator(day)
    rolling = build_rolling_learning_dashboard(day)
    true_replay = build_true_counterfactual_replay(day)
    feature_table = build_feature_outcome_table(day)
    confidence_decay = build_rule_confidence_decay(day)
    human_feedback = build_human_feedback_summary(day)
    market_library = build_market_regime_library(day)
    experiments = build_experiment_registry(day, replay=true_replay, human_feedback=human_feedback)
    weekly_packet = build_weekly_promotion_meeting_packet(
        day,
        replay=true_replay,
        registry=experiments,
        confidence=confidence_decay,
        regimes=market_library,
        feedback=human_feedback,
    )
    runtime_alerts = _read_json(os.path.join(OUT_DIR, f'runtime_restart_alerts_{day}.json'), {}) or {}
    pm = _trade_day(day)
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'purpose': 'Token-efficient entry point for future reviews; prefer this before raw postmortem JSON.',
        'trust': pm.get('trust_today_for_learning'),
        'config_sha256': _sha256(os.path.join(HERE, 'trading_config.json')),
        'headline': {
            'pnl': risk.get('pnl'),
            'trades': risk.get('trades'),
            'win_rate': risk.get('win_rate'),
            'profit_factor': risk.get('profit_factor'),
            'largest_loss': risk.get('largest_loss'),
            'max_intraday_drawdown': risk.get('max_intraday_drawdown'),
        },
        'largest_damage_contributors': losers.get('largest_damage_contributors', [])[:3],
        'execution_friction': {
            'spread': execution.get('spread'),
            'quote_age': execution.get('quote_age'),
            'slippage': execution.get('slippage'),
            'pre_submit_blocks': execution.get('pre_submit_blocks'),
            'stale_data_incidents': execution.get('stale_data_incidents'),
        },
        'feature_coverage': coverage,
        'data_collection_coverage': {
            'verdict': data_coverage.get('verdict'),
            'trust_impact': data_coverage.get('trust_impact'),
            'missing_required': [
                {'name': r.get('name'), 'count': r.get('count'), 'expected': r.get('expected'), 'pct': r.get('pct')}
                for r in data_coverage.get('missing_required', [])
            ],
            'partial_required': [
                {'name': r.get('name'), 'count': r.get('count'), 'expected': r.get('expected'), 'pct': r.get('pct')}
                for r in data_coverage.get('partial_required', [])
            ],
            'watch_gaps': [
                {'name': r.get('name'), 'count': r.get('count'), 'expected': r.get('expected'), 'pct': r.get('pct')}
                for r in (data_coverage.get('watch_gaps') or [])[:8]
            ],
            'file': os.path.join(OUT_DIR, f'data_collection_coverage_{day}.json'),
        },
        'gate_timeline': {
            'rows': gate_timeline.get('rows'),
            'top_blockers': (gate_timeline.get('top_blockers') or [])[:8],
            'by_ticker': gate_timeline.get('by_ticker'),
            'file': os.path.join(OUT_DIR, f'gate_timeline_summary_{day}.json'),
        },
        'no_signal_snapshots': {
            'rows': no_signal_summary.get('rows'),
            'top_blockers': (no_signal_summary.get('top_blockers') or [])[:8],
            'file': os.path.join(OUT_DIR, f'no_signal_snapshot_summary_{day}.json'),
        },
        'fill_attribution': {
            'rows': fill_attribution.get('rows'),
            'by_stage': fill_attribution.get('by_stage'),
            'worst_adverse_rows': (fill_attribution.get('worst_adverse_rows') or [])[:5],
            'file': os.path.join(OUT_DIR, f'fill_attribution_summary_{day}.json'),
        },
        'latency_attribution': {
            'rows': latency_attribution.get('rows'),
            'by_status': latency_attribution.get('by_status'),
            'worst_latency_rows': (latency_attribution.get('worst_latency_rows') or [])[:5],
            'file': os.path.join(OUT_DIR, f'latency_attribution_summary_{day}.json'),
        },
        'entry_timing_efficiency': {
            'by_window': entry_timing.get('by_window'),
            'worst_rows': (entry_timing.get('rows') or [])[:5],
            'file': os.path.join(OUT_DIR, f'entry_timing_efficiency_{day}.json'),
        },
        'dual_side_shadow_review': {
            'counts': dual_side_shadow.get('counts'),
            'watch_rows': [
                r for r in (dual_side_shadow.get('rows') or [])
                if r.get('verdict') != 'aligned'
            ][:8],
            'file': os.path.join(OUT_DIR, f'dual_side_shadow_review_{day}.json'),
        },
        'quote_condition_quality': {
            'quote_state_counts': quote_condition.get('quote_state_counts'),
            'condition_tag_counts': quote_condition.get('condition_tag_counts'),
            'spread_abnormal_rows': quote_condition.get('spread_abnormal_rows'),
            'file': os.path.join(OUT_DIR, f'quote_condition_quality_{day}.json'),
        },
        'shadow_entry_variants': {
            'samples': shadow_entry_variants.get('samples'),
            'summary': shadow_entry_variants.get('summary'),
            'file': os.path.join(OUT_DIR, f'shadow_entry_variants_{day}.json'),
        },
        'new_gate_attribution': {
            'warnings': new_gate_attr.get('warnings'),
            'gate_counts': {
                'miner_basket_trades': _nested(new_gate_attr, 'gates', 'miner_basket', 'trades_with_basket_context'),
                'spread_blocks': _nested(new_gate_attr, 'gates', 'spread_quality_gate', 'blocked_or_retried_signals'),
                'broker_disaster_cap_trades': _nested(new_gate_attr, 'gates', 'broker_disaster_stop_cap', 'trades_with_cap_applied'),
                'broker_disaster_stop_like_exits': _nested(new_gate_attr, 'gates', 'broker_disaster_stop_cap', 'cap_stop_like_exits'),
            },
            'file': os.path.join(OUT_DIR, f'new_gate_attribution_{day}.json'),
        },
        'side_setup_ticker_damage': {
            'by_side': risk.get('by_side'),
            'by_setup': risk.get('by_setup'),
            'by_ticker': risk.get('by_ticker'),
        },
        'winner_protection': {
            'shadow_exit_policy_grades': shadow.get('by_policy'),
            'exit_policy_candidate_config': pm.get('exit_policy_candidate_config'),
        },
        'entry_retry_candidates': {
            'rows': retry.get('rows'),
            'by_reason': retry.get('by_reason'),
        },
        'per_ticker_learning': {
            'top_rows': (ticker_learning.get('rows') or [])[:8],
            'mode': 'staged_review_only',
        },
        'regime_scoring_review': {
            'top_rows': (regime_review.get('rows') or [])[:8],
            'mode': 'staged_review_only',
        },
        'score_calibration': {
            'score_buckets': ((score_calibration.get('tables') or {}).get('score') or [])[:8],
            'execution_quality': ((score_calibration.get('tables') or {}).get('execution_quality') or [])[:8],
            'btc_regime': ((score_calibration.get('tables') or {}).get('btc_regime') or [])[:8],
            'market_regime': ((score_calibration.get('tables') or {}).get('market_regime') or [])[:8],
            'component_effect': ((score_calibration.get('tables') or {}).get('component_effect') or [])[:12],
            'score_side_setup_btc_exec': ((score_calibration.get('tables') or {}).get('score_side_setup_btc_exec') or [])[:12],
            'mode': 'staged_review_only',
            'file': os.path.join(OUT_DIR, f'score_calibration_{day}_10d.json'),
        },
        'market_tape_attribution': {
            'market_regime_buckets': (market_tape.get('market_regime_buckets') or [])[:8],
            'side_by_market_regime': (market_tape.get('side_by_market_regime') or [])[:8],
            'mode': 'staged_review_only',
            'file': os.path.join(OUT_DIR, f'market_tape_attribution_{day}.json'),
        },
        'tick_replay': {
            'summary': tick_replay.get('summary'),
            'by_capture_type': (tick_replay.get('by_capture_type') or [])[:8],
            'by_ticker': (tick_replay.get('by_ticker') or [])[:8],
            'by_setup_type': (tick_replay.get('by_setup_type') or [])[:8],
            'watch_rows': (tick_replay.get('watch_rows') or [])[:12],
            'mode': 'passive_tick_forensics',
            'file': os.path.join(OUT_DIR, f'tick_replay_{day}.json'),
        },
        'setup_grade_review': {
            'by_grade': (setup_grade.get('by_grade') or [])[:8],
            'by_side_setup_grade': (setup_grade.get('by_side_setup_grade') or [])[:12],
            'file': os.path.join(OUT_DIR, f'setup_grade_review_{day}_10d.json'),
        },
        'counterfactual_side_review': {
            'summary': (side_review.get('summary') or [])[:10],
            'promotion_note': side_review.get('promotion_note'),
            'file': os.path.join(OUT_DIR, f'counterfactual_side_review_{day}.json'),
        },
        'rule_attribution_review': {
            'top_hurt_or_watch': (rule_review.get('top_hurt_or_watch') or [])[:8],
            'top_helped': (rule_review.get('top_helped') or [])[:8],
            'file': os.path.join(OUT_DIR, f'rule_attribution_review_{day}_10d.json'),
        },
        'hypothesis_confidence': {
            'counts': hyp_conf.get('counts'),
            'top_rows': (hyp_conf.get('rows') or [])[:8],
            'file': os.path.join(OUT_DIR, f'hypothesis_confidence_{day}.json'),
        },
        'daily_conclusion_ledger': {
            'what_we_learned': conclusion.get('what_we_learned'),
            'what_we_rejected': conclusion.get('what_we_rejected'),
            'what_is_still_uncertain': conclusion.get('what_is_still_uncertain'),
            'what_to_watch_next': conclusion.get('what_to_watch_next'),
            'file': os.path.join(OUT_DIR, f'daily_conclusion_ledger_{day}.json'),
            'rolling_file': os.path.join(OUT_DIR, 'learning_conclusion_ledger.json'),
        },
        'matched_control_review': {
            'deduction_counts': matched.get('deduction_counts'),
            'top_rows': (matched.get('rows') or [])[:8],
            'file': os.path.join(OUT_DIR, f'matched_control_review_{day}_10d.json'),
        },
        'exit_efficiency_review': {
            'summary': (exit_eff.get('summary') or [])[:8],
            'file': os.path.join(OUT_DIR, f'exit_efficiency_review_{day}.json'),
        },
        'mae_mfe_timeline': {
            'summary': (timeline.get('summary') or [])[:8],
            'data_note': timeline.get('data_note'),
            'file': os.path.join(OUT_DIR, f'mae_mfe_timeline_{day}.json'),
        },
        'hypothesis_counterexamples': {
            'counts': hyp_counter.get('counts'),
            'top_rows': (hyp_counter.get('rows') or [])[:8],
            'file': os.path.join(OUT_DIR, f'hypothesis_counterexamples_{day}.json'),
        },
        'loser_fingerprints': {
            'summary': (fingerprints.get('summary') or [])[:8],
            'file': os.path.join(OUT_DIR, f'loser_fingerprints_{day}.json'),
        },
        'postmortem_section_confidence': {
            'counts': section_conf.get('counts'),
            'sections': section_conf.get('sections'),
            'file': os.path.join(OUT_DIR, f'postmortem_section_confidence_{day}.json'),
        },
        'trade_verdicts': {
            'counts': verdicts.get('counts'),
            'rows': (verdicts.get('rows') or [])[:12],
            'file': os.path.join(OUT_DIR, f'trade_verdicts_{day}.json'),
        },
        'best_avoided_loser_simulator': {
            'top_rows': (avoid.get('rows') or [])[:8],
            'file': os.path.join(OUT_DIR, f'best_avoided_loser_simulator_{day}.json'),
        },
        'rolling_learning_dashboard': {
            'totals': rolling.get('totals'),
            'recurring_loser_fingerprints': rolling.get('recurring_loser_fingerprints'),
            'daily': (rolling.get('daily') or [])[-5:],
            'file': os.path.join(OUT_DIR, f'rolling_learning_dashboard_{day}_10d.json'),
        },
        'true_counterfactual_replay': {
            'top_policies': (true_replay.get('policies') or [])[:8],
            'file': os.path.join(OUT_DIR, f'true_counterfactual_replay_{day}_10d.json'),
        },
        'experiment_registry': {
            'counts': experiments.get('counts'),
            'top_experiments': list((experiments.get('experiments') or {}).values())[:8],
            'file': os.path.join(OUT_DIR, 'experiment_registry.json'),
            'snapshot_file': os.path.join(OUT_DIR, f'experiment_registry_snapshot_{day}.json'),
        },
        'feature_outcome_table': {
            'rows': len(feature_table.get('rows') or []),
            'columns': feature_table.get('columns'),
            'file': os.path.join(OUT_DIR, f'feature_outcome_table_{day}_10d.json'),
        },
        'rule_confidence_decay': {
            'counts': confidence_decay.get('counts'),
            'top_rows': (confidence_decay.get('rows') or [])[:8],
            'file': os.path.join(OUT_DIR, f'rule_confidence_decay_{day}.json'),
        },
        'human_feedback_summary': {
            'counts': human_feedback.get('counts'),
            'feedback_file': human_feedback.get('feedback_file'),
            'global_feedback_file': human_feedback.get('global_feedback_file'),
            'file': os.path.join(OUT_DIR, f'human_feedback_summary_{day}.json'),
        },
        'market_regime_library': {
            'regimes': (market_library.get('regimes') or [])[:8],
            'current_day': next((r for r in market_library.get('daily') or [] if r.get('day') == day), None),
            'file': os.path.join(OUT_DIR, f'market_regime_library_{day}_20d.json'),
        },
        'weekly_promotion_meeting': {
            'decision_counts': {k: len(v or []) for k, v in (weekly_packet.get('decisions') or {}).items()},
            'experiment_counts': weekly_packet.get('experiment_counts'),
            'file': os.path.join(OUT_DIR, f'weekly_promotion_meeting_{day}_10d.json'),
        },
        'runtime_restart_alerts': {
            'ok': runtime_alerts.get('ok'),
            'unexpected_restart_count': runtime_alerts.get('unexpected_restart_count'),
            'alerts': (runtime_alerts.get('alerts') or [])[:5],
            'file': os.path.join(OUT_DIR, f'runtime_restart_alerts_{day}.json'),
        },
        'promotion_gate_status': {
            'engine_candidate_config': pm.get('engine_candidate_config'),
            'exit_policy_candidate_config': pm.get('exit_policy_candidate_config'),
            'recommendation': 'No live rule change warranted unless candidate gates clear multi-day evidence.',
        },
        'top_learning_sections': {
            'daily_human_summary': pm.get('daily_learning_brief'),
            'ultimate_learning_model': {
                'trade_archetype_grades': (pm.get('trade_archetype_grades') or [])[:8],
                'loser_root_cause_buckets': (pm.get('loser_root_cause_buckets') or [])[:8],
                'rule_attribution_matrix': (pm.get('rule_attribution_matrix') or [])[:8],
            },
            'decision_confidence_meter': (pm.get('decision_confidence_meter') or [])[:8],
        },
        'artifact_manifest_file': os.path.join(OUT_DIR, f'artifacts_{day}.json'),
    }


def build_artifact_manifest(day: str) -> dict:
    infos = [_file_info(p) for p in _manifest_paths(day)]
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'config_sha256': _sha256(os.path.join(HERE, 'trading_config.json')),
        'corpus_size_stats': {
            'total_bytes': sum(i.get('bytes') or 0 for i in infos if i.get('exists')),
            'large_files': [i for i in infos if (i.get('bytes') or 0) >= 1_000_000],
            'review_preferred_files': [
                os.path.join(OUT_DIR, f'review_index_{day}.json'),
                os.path.join(OUT_DIR, f'loser_summary_{day}.json'),
                os.path.join(OUT_DIR, f'risk_summary_{day}.json'),
                os.path.join(OUT_DIR, f'execution_summary_{day}.json'),
                os.path.join(OUT_DIR, f'shadow_exit_summary_{day}.json'),
                os.path.join(OUT_DIR, f'new_gate_attribution_{day}.json'),
                os.path.join(OUT_DIR, f'entry_retry_summary_{day}.json'),
                os.path.join(OUT_DIR, f'exit_hierarchy_summary_{day}.json'),
                os.path.join(OUT_DIR, f'per_ticker_learning_{day}.json'),
                os.path.join(OUT_DIR, f'regime_scoring_review_{day}.json'),
                os.path.join(OUT_DIR, f'score_calibration_{day}_10d.json'),
                os.path.join(OUT_DIR, f'market_tape_attribution_{day}.json'),
                os.path.join(OUT_DIR, f'setup_grade_review_{day}_10d.json'),
                os.path.join(OUT_DIR, f'counterfactual_side_review_{day}.json'),
                os.path.join(OUT_DIR, f'rule_attribution_review_{day}_10d.json'),
                os.path.join(OUT_DIR, f'hypothesis_confidence_{day}.json'),
                os.path.join(OUT_DIR, f'daily_conclusion_ledger_{day}.json'),
                os.path.join(OUT_DIR, 'learning_conclusion_ledger.json'),
                os.path.join(OUT_DIR, f'matched_control_review_{day}_10d.json'),
                os.path.join(OUT_DIR, f'exit_efficiency_review_{day}.json'),
                os.path.join(OUT_DIR, f'mae_mfe_timeline_{day}.json'),
                os.path.join(OUT_DIR, f'hypothesis_counterexamples_{day}.json'),
                os.path.join(OUT_DIR, f'loser_fingerprints_{day}.json'),
                os.path.join(OUT_DIR, f'postmortem_section_confidence_{day}.json'),
                os.path.join(OUT_DIR, f'trade_verdicts_{day}.json'),
                os.path.join(OUT_DIR, f'best_avoided_loser_simulator_{day}.json'),
                os.path.join(OUT_DIR, f'rolling_learning_dashboard_{day}_10d.json'),
                os.path.join(OUT_DIR, f'true_counterfactual_replay_{day}_10d.json'),
                os.path.join(OUT_DIR, f'experiment_registry_snapshot_{day}.json'),
                os.path.join(OUT_DIR, 'experiment_registry.json'),
                os.path.join(OUT_DIR, f'feature_outcome_table_{day}_10d.json'),
                os.path.join(OUT_DIR, f'rule_confidence_decay_{day}.json'),
                os.path.join(OUT_DIR, f'human_feedback_summary_{day}.json'),
                os.path.join(OUT_DIR, f'human_feedback_{day}.json'),
                os.path.join(OUT_DIR, 'human_feedback.json'),
                os.path.join(OUT_DIR, f'weekly_promotion_meeting_{day}_10d.json'),
                os.path.join(OUT_DIR, f'market_regime_library_{day}_20d.json'),
                os.path.join(OUT_DIR, f'config_diff_{day}.json'),
                os.path.join(OUT_DIR, f'pre_market_checklist_{day}.json'),
                os.path.join(OUT_DIR, f'config_freeze_{day}_pre_open.json'),
                os.path.join(OUT_DIR, f'config_change_watch_{day}.json'),
                os.path.join(OUT_DIR, f'trade_alerts_{day}.json'),
                os.path.join(OUT_DIR, f'DAILY_REVIEW_START_HERE_{day}.json'),
                os.path.join(OUT_DIR, f'MONDAY_REVIEW_START_HERE_{day}.json'),
                os.path.join(OUT_DIR, f'multi_day_scorecard_{day}_10d.json'),
                os.path.join(OUT_DIR, f'market_regime_day_{day}.json'),
                os.path.join(OUT_DIR, f'edge_quality_activity_{day}_10d.json'),
                os.path.join(OUT_DIR, f'order_latency_{day}.json'),
                os.path.join(OUT_DIR, f'data_collection_coverage_{day}.json'),
                os.path.join(OUT_DIR, f'fill_attribution_summary_{day}.json'),
                os.path.join(OUT_DIR, f'latency_attribution_summary_{day}.json'),
                os.path.join(OUT_DIR, f'entry_timing_efficiency_{day}.json'),
                os.path.join(OUT_DIR, f'dual_side_shadow_review_{day}.json'),
                os.path.join(OUT_DIR, f'quote_condition_quality_{day}.json'),
                os.path.join(OUT_DIR, f'shadow_entry_variants_{day}.json'),
                os.path.join(OUT_DIR, 'ARTIFACT_SCHEMA_DICTIONARY.json'),
                os.path.join(OUT_DIR, f'rule_dry_run_scoreboard_{day}.json'),
                os.path.join(OUT_DIR, f'execution_context_summary_{day}.json'),
                os.path.join(OUT_DIR, f'retention_plan_{day}.json'),
                os.path.join(OUT_DIR, f'loser_clusters_{day}.json'),
                os.path.join(OUT_DIR, f'loser_archetypes_{day}.json'),
                os.path.join(OUT_DIR, f'entry_quality_tiers_{day}.json'),
                os.path.join(OUT_DIR, f'winner_damage_report_{day}.json'),
                os.path.join(OUT_DIR, f'clean_day_score_{day}.json'),
                os.path.join(OUT_DIR, f'no_trade_opportunity_grades_{day}.json'),
                os.path.join(OUT_DIR, f'thesis_failure_review_{day}.json'),
                os.path.join(OUT_DIR, f'loser_replay_snapshots_{day}.json'),
                os.path.join(OUT_DIR, f'per_symbol_personality_{day}_10d.json'),
                os.path.join(OUT_DIR, f'out_of_sample_scoreboard_{day}.json'),
                os.path.join(OUT_DIR, f'first_hour_review_{day}.json'),
                os.path.join(OUT_DIR, f'strategy_conclusion_gate_{day}.json'),
                os.path.join(OUT_DIR, f'market_context_scoreboard_{day}.json'),
                os.path.join(OUT_DIR, f'trade_thesis_timelines_{day}.json'),
                os.path.join(OUT_DIR, f'winner_quality_{day}.json'),
                os.path.join(OUT_DIR, f'rule_candidate_quarantine_{day}.json'),
                os.path.join(OUT_DIR, f'feed_health_score_{day}.json'),
                os.path.join(OUT_DIR, f'broker_execution_score_{day}.json'),
                os.path.join(OUT_DIR, f'era_scorecard_{day}.json'),
                os.path.join(OUT_DIR, f'monday_scorecard_{day}.json'),
                os.path.join(OUT_DIR, f'ws_scalp_replay_{day}.json'),
                os.path.join(OUT_DIR, f'tick_replay_{day}.json'),
                os.path.join(OUT_DIR, f'decision_audit_summary_{day}.json'),
                os.path.join(OUT_DIR, f'current_engine_regrade_{day}.json'),
                os.path.join(OUT_DIR, f'regime_specific_scorecards_{day}.json'),
                os.path.join(OUT_DIR, f'near_miss_winners_{day}.json'),
                os.path.join(OUT_DIR, f'exit_quality_score_{day}.json'),
                os.path.join(OUT_DIR, f'replay_confidence_{day}.json'),
                os.path.join(OUT_DIR, f'false_positive_negative_{day}.json'),
                os.path.join(OUT_DIR, f'daily_review_gate_{day}.json'),
                os.path.join(OUT_DIR, f'rule_lifecycle_dashboard_{day}.json'),
                os.path.join(OUT_DIR, f'NOW_STATUS_{day}.json'),
                os.path.join(OUT_DIR, 'NOW_STATUS.json'),
                os.path.join(OUT_DIR, f'world_class_dashboard_{day}.json'),
                os.path.join(OUT_DIR, f'strategy_operations_split_{day}.json'),
                os.path.join(OUT_DIR, f'why_no_trade_{day}.json'),
                os.path.join(OUT_DIR, f'market_open_monitor_{day}.json'),
                os.path.join(OUT_DIR, f'current_config_replay_{day}.json'),
                os.path.join(OUT_DIR, f'monday_live_review_{day}.json'),
                os.path.join(OUT_DIR, f'postmarket_artifact_validation_{day}.json'),
                os.path.join(OUT_DIR, f'runtime_restart_alerts_{day}.json'),
                os.path.join(OUT_DIR, 'change_impact_ledger.json'),
                os.path.join(OUT_DIR, 'config_intent_ledger.json'),
                os.path.join(OUT_DIR, 'promotion_queue.json'),
            ],
        },
        'files': infos,
    }


def _build_monday_review_packet_legacy(day: str) -> dict:
    promotion_queue = _read_json(os.path.join(OUT_DIR, 'promotion_queue.json'), {}) or {}
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'review_order': [
            'risk_summary',
            'loser_summary',
            'execution_summary',
            'new_gate_attribution',
            'shadow_exit_summary',
            'per_ticker_learning',
            'regime_scoring_review',
            'score_calibration',
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
            'market_open_monitor',
            'runtime_restart_alerts',
            'trade_alerts',
            'review_start_here',
            'multi_day_scorecard',
            'market_regime_day',
            'edge_quality_activity',
            'order_latency',
            'data_collection_coverage',
            'rule_dry_run_scoreboard',
            'execution_context_summary',
            'retention_plan',
            'promotion_review',
            'current_config_replay',
            'change_impact_ledger',
            'config_intent_ledger',
            'promotion_queue',
            'promotion_gate_status',
        ],
        'review_index': build_review_index(day),
        'risk_summary': build_risk_summary(day),
        'loser_summary': build_loser_summary(day),
        'execution_summary': build_execution_summary(day),
            'shadow_exit_summary': build_shadow_exit_summary(day),
        'feature_coverage': build_feature_coverage(day),
        'data_collection_coverage': build_data_collection_coverage(day),
        'entry_retry_summary': build_entry_retry_summary(day),
        'exit_hierarchy_summary': build_exit_hierarchy_summary(day),
        'per_ticker_learning': build_per_ticker_learning(day),
        'regime_scoring_review': build_regime_scoring_review(day),
        'score_calibration': build_score_calibration(day),
        'config_diff': build_config_diff(day),
        'weekend_readiness': {
            'pre_market_checklist': _read_json(os.path.join(OUT_DIR, f'pre_market_checklist_{day}.json'), {}),
            'config_freeze': _read_json(os.path.join(OUT_DIR, f'config_freeze_{day}_pre_open.json'), {}),
            'config_change_watch': _read_json(os.path.join(OUT_DIR, f'config_change_watch_{day}.json'), {}),
            'trade_alerts': _read_json(os.path.join(OUT_DIR, f'trade_alerts_{day}.json'), {}),
            'review_start_here': _read_json(os.path.join(OUT_DIR, f'DAILY_REVIEW_START_HERE_{day}.json'), {}),
            'multi_day_scorecard': _read_json(os.path.join(OUT_DIR, f'multi_day_scorecard_{day}_10d.json'), {}),
            'market_regime_day': _read_json(os.path.join(OUT_DIR, f'market_regime_day_{day}.json'), {}),
            'edge_quality_activity': _read_json(os.path.join(OUT_DIR, f'edge_quality_activity_{day}_10d.json'), {}),
            'order_latency': _read_json(os.path.join(OUT_DIR, f'order_latency_{day}.json'), {}),
            'data_collection_coverage': _read_json(os.path.join(OUT_DIR, f'data_collection_coverage_{day}.json'), {}),
            'rule_dry_run_scoreboard': _read_json(os.path.join(OUT_DIR, f'rule_dry_run_scoreboard_{day}.json'), {}),
            'execution_context_summary': _read_json(os.path.join(OUT_DIR, f'execution_context_summary_{day}.json'), {}),
            'retention_plan': _read_json(os.path.join(OUT_DIR, f'retention_plan_{day}.json'), {}),
            'promotion_review': _read_json(os.path.join(OUT_DIR, f'promotion_review_{day}.json'), {}),
            'loser_clusters': _read_json(os.path.join(OUT_DIR, f'loser_clusters_{day}.json'), {}),
            'loser_archetypes': _read_json(os.path.join(OUT_DIR, f'loser_archetypes_{day}.json'), {}),
            'entry_quality_tiers': _read_json(os.path.join(OUT_DIR, f'entry_quality_tiers_{day}.json'), {}),
            'winner_damage_report': _read_json(os.path.join(OUT_DIR, f'winner_damage_report_{day}.json'), {}),
            'clean_day_score': _read_json(os.path.join(OUT_DIR, f'clean_day_score_{day}.json'), {}),
            'no_trade_opportunity_grades': _read_json(os.path.join(OUT_DIR, f'no_trade_opportunity_grades_{day}.json'), {}),
            'thesis_failure_review': _read_json(os.path.join(OUT_DIR, f'thesis_failure_review_{day}.json'), {}),
            'loser_replay_snapshots': _read_json(os.path.join(OUT_DIR, f'loser_replay_snapshots_{day}.json'), {}),
            'per_symbol_personality': _read_json(os.path.join(OUT_DIR, f'per_symbol_personality_{day}_10d.json'), {}),
            'out_of_sample_scoreboard': _read_json(os.path.join(OUT_DIR, f'out_of_sample_scoreboard_{day}.json'), {}),
            'first_hour_review': _read_json(os.path.join(OUT_DIR, f'first_hour_review_{day}.json'), {}),
            'strategy_conclusion_gate': _read_json(os.path.join(OUT_DIR, f'strategy_conclusion_gate_{day}.json'), {}),
            'market_context_scoreboard': _read_json(os.path.join(OUT_DIR, f'market_context_scoreboard_{day}.json'), {}),
            'trade_thesis_timelines': _read_json(os.path.join(OUT_DIR, f'trade_thesis_timelines_{day}.json'), {}),
            'winner_quality': _read_json(os.path.join(OUT_DIR, f'winner_quality_{day}.json'), {}),
            'rule_candidate_quarantine': _read_json(os.path.join(OUT_DIR, f'rule_candidate_quarantine_{day}.json'), {}),
            'feed_health_score': _read_json(os.path.join(OUT_DIR, f'feed_health_score_{day}.json'), {}),
            'broker_execution_score': _read_json(os.path.join(OUT_DIR, f'broker_execution_score_{day}.json'), {}),
            'era_scorecard': _read_json(os.path.join(OUT_DIR, f'era_scorecard_{day}.json'), {}),
            'monday_scorecard': _read_json(os.path.join(OUT_DIR, f'monday_scorecard_{day}.json'), {}),
            'ws_scalp_replay': _read_json(os.path.join(OUT_DIR, f'ws_scalp_replay_{day}.json'), {}),
            'tick_replay': _read_json(os.path.join(OUT_DIR, f'tick_replay_{day}.json'), {}),
            'decision_audit_summary': _read_json(os.path.join(OUT_DIR, f'decision_audit_summary_{day}.json'), {}),
            'current_engine_regrade': _read_json(os.path.join(OUT_DIR, f'current_engine_regrade_{day}.json'), {}),
            'regime_specific_scorecards': _read_json(os.path.join(OUT_DIR, f'regime_specific_scorecards_{day}.json'), {}),
            'near_miss_winners': _read_json(os.path.join(OUT_DIR, f'near_miss_winners_{day}.json'), {}),
            'exit_quality_score': _read_json(os.path.join(OUT_DIR, f'exit_quality_score_{day}.json'), {}),
            'replay_confidence': _read_json(os.path.join(OUT_DIR, f'replay_confidence_{day}.json'), {}),
            'false_positive_negative': _read_json(os.path.join(OUT_DIR, f'false_positive_negative_{day}.json'), {}),
            'daily_review_gate': _read_json(os.path.join(OUT_DIR, f'daily_review_gate_{day}.json'), {}),
            'rule_lifecycle_dashboard': _read_json(os.path.join(OUT_DIR, f'rule_lifecycle_dashboard_{day}.json'), {}),
            'now_status': _read_json(os.path.join(OUT_DIR, f'NOW_STATUS_{day}.json'), {}),
            'world_class_dashboard': _read_json(os.path.join(OUT_DIR, f'world_class_dashboard_{day}.json'), {}),
            'strategy_operations_split': _read_json(os.path.join(OUT_DIR, f'strategy_operations_split_{day}.json'), {}),
            'why_no_trade': _read_json(os.path.join(OUT_DIR, f'why_no_trade_{day}.json'), {}),
            'market_open_monitor': _read_json(os.path.join(OUT_DIR, f'market_open_monitor_{day}.json'), {}),
            'runtime_restart_alerts': _read_json(os.path.join(OUT_DIR, f'runtime_restart_alerts_{day}.json'), {}),
            'current_config_replay': _read_json(os.path.join(OUT_DIR, f'current_config_replay_{day}.json'), {}),
            'postmarket_artifact_validation': _read_json(os.path.join(OUT_DIR, f'postmarket_artifact_validation_{day}.json'), {}),
            'change_impact_ledger': _read_json(os.path.join(OUT_DIR, 'change_impact_ledger.json'), {}),
            'config_intent_ledger': _read_json(os.path.join(OUT_DIR, 'config_intent_ledger.json'), {}),
        },
        'promotion_queue': promotion_queue,
        'no_change_recommendation': (
            'No automatic live changes. Promote only after multi-day evidence clears gates and winner damage is acceptable.'
        ),
    }


def _artifact_files(day: str) -> dict:
    return {
        'review_index': os.path.join(OUT_DIR, f'review_index_{day}.json'),
        'postmortem': os.path.join(OUT_DIR, f'postmortem_{day}.json'),
        'risk_summary': os.path.join(OUT_DIR, f'risk_summary_{day}.json'),
        'loser_summary': os.path.join(OUT_DIR, f'loser_summary_{day}.json'),
        'execution_summary': os.path.join(OUT_DIR, f'execution_summary_{day}.json'),
        'new_gate_attribution': os.path.join(OUT_DIR, f'new_gate_attribution_{day}.json'),
        'shadow_exit_summary': os.path.join(OUT_DIR, f'shadow_exit_summary_{day}.json'),
        'entry_retry_summary': os.path.join(OUT_DIR, f'entry_retry_summary_{day}.json'),
        'exit_hierarchy_summary': os.path.join(OUT_DIR, f'exit_hierarchy_summary_{day}.json'),
        'per_ticker_learning': os.path.join(OUT_DIR, f'per_ticker_learning_{day}.json'),
        'regime_scoring_review': os.path.join(OUT_DIR, f'regime_scoring_review_{day}.json'),
        'score_calibration': os.path.join(OUT_DIR, f'score_calibration_{day}_10d.json'),
        'market_tape_attribution': os.path.join(OUT_DIR, f'market_tape_attribution_{day}.json'),
        'tick_replay': os.path.join(OUT_DIR, f'tick_replay_{day}.json'),
        'setup_grade_review': os.path.join(OUT_DIR, f'setup_grade_review_{day}_10d.json'),
        'counterfactual_side_review': os.path.join(OUT_DIR, f'counterfactual_side_review_{day}.json'),
        'rule_attribution_review': os.path.join(OUT_DIR, f'rule_attribution_review_{day}_10d.json'),
        'hypothesis_confidence': os.path.join(OUT_DIR, f'hypothesis_confidence_{day}.json'),
        'daily_conclusion_ledger': os.path.join(OUT_DIR, f'daily_conclusion_ledger_{day}.json'),
        'learning_conclusion_ledger': os.path.join(OUT_DIR, 'learning_conclusion_ledger.json'),
        'matched_control_review': os.path.join(OUT_DIR, f'matched_control_review_{day}_10d.json'),
        'exit_efficiency_review': os.path.join(OUT_DIR, f'exit_efficiency_review_{day}.json'),
        'mae_mfe_timeline': os.path.join(OUT_DIR, f'mae_mfe_timeline_{day}.json'),
        'hypothesis_counterexamples': os.path.join(OUT_DIR, f'hypothesis_counterexamples_{day}.json'),
        'loser_fingerprints': os.path.join(OUT_DIR, f'loser_fingerprints_{day}.json'),
        'postmortem_section_confidence': os.path.join(OUT_DIR, f'postmortem_section_confidence_{day}.json'),
        'trade_verdicts': os.path.join(OUT_DIR, f'trade_verdicts_{day}.json'),
        'best_avoided_loser_simulator': os.path.join(OUT_DIR, f'best_avoided_loser_simulator_{day}.json'),
        'rolling_learning_dashboard': os.path.join(OUT_DIR, f'rolling_learning_dashboard_{day}_10d.json'),
        'true_counterfactual_replay': os.path.join(OUT_DIR, f'true_counterfactual_replay_{day}_10d.json'),
        'experiment_registry_snapshot': os.path.join(OUT_DIR, f'experiment_registry_snapshot_{day}.json'),
        'experiment_registry': os.path.join(OUT_DIR, 'experiment_registry.json'),
        'feature_outcome_table': os.path.join(OUT_DIR, f'feature_outcome_table_{day}_10d.json'),
        'rule_confidence_decay': os.path.join(OUT_DIR, f'rule_confidence_decay_{day}.json'),
        'human_feedback_summary': os.path.join(OUT_DIR, f'human_feedback_summary_{day}.json'),
        'human_feedback_day': os.path.join(OUT_DIR, f'human_feedback_{day}.json'),
        'human_feedback_global': os.path.join(OUT_DIR, 'human_feedback.json'),
        'weekly_promotion_meeting': os.path.join(OUT_DIR, f'weekly_promotion_meeting_{day}_10d.json'),
        'market_regime_library': os.path.join(OUT_DIR, f'market_regime_library_{day}_20d.json'),
        'daily_review_gate': os.path.join(OUT_DIR, f'daily_review_gate_{day}.json'),
        'replay_confidence': os.path.join(OUT_DIR, f'replay_confidence_{day}.json'),
        'strategy_operations_split': os.path.join(OUT_DIR, f'strategy_operations_split_{day}.json'),
        'world_class_dashboard': os.path.join(OUT_DIR, f'world_class_dashboard_{day}.json'),
        'now_status': os.path.join(OUT_DIR, f'NOW_STATUS_{day}.json'),
        'pre_market_checklist': os.path.join(OUT_DIR, f'pre_market_checklist_{day}.json'),
        'trade_alerts': os.path.join(OUT_DIR, f'trade_alerts_{day}.json'),
        'runtime_restart_alerts': os.path.join(OUT_DIR, f'runtime_restart_alerts_{day}.json'),
        'data_collection_coverage': os.path.join(OUT_DIR, f'data_collection_coverage_{day}.json'),
        'gate_timeline_summary': os.path.join(OUT_DIR, f'gate_timeline_summary_{day}.json'),
        'no_signal_snapshot_summary': os.path.join(OUT_DIR, f'no_signal_snapshot_summary_{day}.json'),
        'fill_attribution_summary': os.path.join(OUT_DIR, f'fill_attribution_summary_{day}.json'),
        'rule_dry_run_scoreboard': os.path.join(OUT_DIR, f'rule_dry_run_scoreboard_{day}.json'),
        'execution_context_summary': os.path.join(OUT_DIR, f'execution_context_summary_{day}.json'),
        'retention_plan': os.path.join(OUT_DIR, f'retention_plan_{day}.json'),
        'promotion_review': os.path.join(OUT_DIR, f'promotion_review_{day}.json'),
        'promotion_queue': os.path.join(OUT_DIR, 'promotion_queue.json'),
        'change_impact_ledger': os.path.join(OUT_DIR, 'change_impact_ledger.json'),
        'config_intent_ledger': os.path.join(OUT_DIR, 'config_intent_ledger.json'),
        'artifact_manifest': os.path.join(OUT_DIR, f'artifacts_{day}.json'),
    }


def _compact_review_status(day: str) -> dict:
    files = _artifact_files(day)
    gate = _read_json(files['daily_review_gate'], {}) or {}
    replay = _read_json(files['replay_confidence'], {}) or {}
    ops = _read_json(files['strategy_operations_split'], {}) or {}
    world = _read_json(files['world_class_dashboard'], {}) or {}
    now = _read_json(files['now_status'], {}) or _read_json(os.path.join(OUT_DIR, 'NOW_STATUS.json'), {}) or {}
    pre = _read_json(files['pre_market_checklist'], {}) or {}
    alerts = _read_json(files['runtime_restart_alerts'], {}) or {}
    return {
        'daily_review_gate': {
            'verdict': gate.get('verdict'),
            'checks_passed': gate.get('checks_passed'),
            'checks_total': gate.get('checks_total'),
            'blockers': (gate.get('blockers') or gate.get('failures') or [])[:8],
        },
        'replay_confidence': {
            'verdict': replay.get('verdict'),
            'usable_trades': replay.get('usable_trades'),
            'low_confidence_trades': replay.get('low_confidence_trades'),
            'missing_replay_trades': replay.get('missing_replay_trades'),
        },
        'pre_market_checklist': {
            'ok': pre.get('ok'),
            'checks_passed': pre.get('checks_passed'),
            'checks_total': pre.get('checks_total'),
            'failures': (pre.get('failures') or [])[:8],
        },
        'runtime_restart_alerts': {
            'ok': alerts.get('ok'),
            'unexpected_restart_count': alerts.get('unexpected_restart_count'),
            'alerts': (alerts.get('alerts') or [])[:5],
        },
        'strategy_operations_split': {
            'strategy_verdict': ops.get('strategy_verdict') or ops.get('verdict'),
            'operations_verdict': ops.get('operations_verdict'),
            'strategy_findings': (ops.get('strategy_findings') or [])[:6],
            'operations_findings': (ops.get('operations_findings') or [])[:6],
        },
        'world_class_dashboard': {
            'verdict': world.get('verdict'),
            'score': world.get('score'),
            'top_gaps': (world.get('top_gaps') or world.get('gaps') or [])[:8],
        },
        'now_status': {
            'state': now.get('state'),
            'safe': now.get('safe'),
            'review_gate': now.get('review_gate'),
            'alerts': (now.get('alerts') or [])[:8],
        },
    }


def _summary_only(payload: dict, keys: list[str]) -> dict:
    return {key: payload.get(key) for key in keys if key in payload}


def build_monday_review_packet(day: str) -> dict:
    promotion_queue = _read_json(os.path.join(OUT_DIR, 'promotion_queue.json'), {}) or {}
    market_tape = build_market_tape_attribution(day)
    new_gate = build_new_gate_attribution(day)
    shadow = build_shadow_exit_summary(day)
    retry = build_entry_retry_summary(day)
    exits = build_exit_hierarchy_summary(day)
    ticker_learning = build_per_ticker_learning(day)
    regime_review = build_regime_scoring_review(day)
    score_calibration = build_score_calibration(day)
    setup_grade = build_setup_grade_review(day)
    side_review = build_counterfactual_side_review(day)
    rule_review = build_rule_attribution_review(day)
    hyp_conf = build_hypothesis_confidence(day)
    conclusion = build_daily_conclusion_ledger(day)
    matched = build_matched_control_review(day)
    exit_eff = build_exit_efficiency_review(day)
    timeline = build_mae_mfe_timeline(day)
    hyp_counter = build_hypothesis_counterexamples(day)
    fingerprints = build_loser_fingerprints(day)
    section_conf = build_postmortem_section_confidence(day)
    verdicts = build_trade_verdicts(day)
    avoid = build_best_avoided_loser_simulator(day)
    rolling = build_rolling_learning_dashboard(day)
    true_replay = build_true_counterfactual_replay(day)
    feature_table = build_feature_outcome_table(day)
    confidence_decay = build_rule_confidence_decay(day)
    human_feedback = build_human_feedback_summary(day)
    market_library = build_market_regime_library(day)
    experiments = build_experiment_registry(day, replay=true_replay, human_feedback=human_feedback)
    weekly_packet = build_weekly_promotion_meeting_packet(
        day,
        replay=true_replay,
        registry=experiments,
        confidence=confidence_decay,
        regimes=market_library,
        feedback=human_feedback,
    )
    tick_replay = _tick_replay(day)
    review_index = build_review_index(day)
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'purpose': (
            'Compact all-in-one review packet. It contains summaries plus artifact paths; '
            'read raw artifacts only when a summary points to a specific issue.'
        ),
        'review_order': [
            'review_index',
            'risk_summary',
            'loser_summary',
            'execution_summary',
            'market_tape_attribution',
            'tick_replay',
            'new_gate_attribution',
            'score_calibration',
            'setup_grade_review',
            'counterfactual_side_review',
            'rule_attribution_review',
            'hypothesis_confidence',
            'daily_conclusion_ledger',
            'matched_control_review',
            'exit_efficiency_review',
            'mae_mfe_timeline',
            'hypothesis_counterexamples',
            'loser_fingerprints',
            'postmortem_section_confidence',
            'trade_verdicts',
            'best_avoided_loser_simulator',
            'rolling_learning_dashboard',
            'true_counterfactual_replay',
            'experiment_registry',
            'feature_outcome_table',
            'rule_confidence_decay',
            'human_feedback_summary',
            'market_regime_library',
            'weekly_promotion_meeting',
            'shadow_exit_summary',
            'entry_retry_summary',
            'exit_hierarchy_summary',
            'per_ticker_learning',
            'regime_scoring_review',
            'review_status_summary',
            'promotion_queue_summary',
            'artifact_files',
        ],
        'review_index': review_index,
        'risk_summary': build_risk_summary(day),
        'loser_summary': build_loser_summary(day),
        'execution_summary': build_execution_summary(day),
        'market_tape_attribution': {
            'market_regime_buckets': (market_tape.get('market_regime_buckets') or [])[:8],
            'side_by_market_regime': (market_tape.get('side_by_market_regime') or [])[:8],
            'file': os.path.join(OUT_DIR, f'market_tape_attribution_{day}.json'),
        },
        'tick_replay': {
            'summary': tick_replay.get('summary'),
            'by_capture_type': (tick_replay.get('by_capture_type') or [])[:8],
            'by_ticker': (tick_replay.get('by_ticker') or [])[:8],
            'by_setup_type': (tick_replay.get('by_setup_type') or [])[:8],
            'watch_rows': (tick_replay.get('watch_rows') or [])[:12],
            'file': os.path.join(OUT_DIR, f'tick_replay_{day}.json'),
        },
        'new_gate_attribution': {
            'warnings': new_gate.get('warnings'),
            'gate_counts': {
                'miner_basket_trades': _nested(new_gate, 'gates', 'miner_basket', 'trades_with_basket_context'),
                'spread_blocks': _nested(new_gate, 'gates', 'spread_quality_gate', 'blocked_or_retried_signals'),
                'broker_disaster_cap_trades': _nested(new_gate, 'gates', 'broker_disaster_stop_cap', 'trades_with_cap_applied'),
            },
            'file': os.path.join(OUT_DIR, f'new_gate_attribution_{day}.json'),
        },
        'shadow_exit_summary': {
            'rows': shadow.get('rows'),
            'by_policy': shadow.get('by_policy'),
            'file': os.path.join(OUT_DIR, f'shadow_exit_summary_{day}.json'),
        },
        'entry_retry_summary': {
            'rows': retry.get('rows'),
            'by_reason': retry.get('by_reason'),
            'file': os.path.join(OUT_DIR, f'entry_retry_summary_{day}.json'),
        },
        'exit_hierarchy_summary': {
            'rows': exits.get('rows'),
            'selected_counts': exits.get('selected_counts'),
            'candidate_kind_counts': exits.get('candidate_kind_counts'),
            'file': os.path.join(OUT_DIR, f'exit_hierarchy_summary_{day}.json'),
        },
        'per_ticker_learning': {
            'top_rows': (ticker_learning.get('rows') or [])[:8],
            'file': os.path.join(OUT_DIR, f'per_ticker_learning_{day}.json'),
        },
        'regime_scoring_review': {
            'top_rows': (regime_review.get('rows') or [])[:8],
            'file': os.path.join(OUT_DIR, f'regime_scoring_review_{day}.json'),
        },
        'score_calibration': {
            'tables': {
                name: rows[:8 if name != 'component_effect' else 12]
                for name, rows in (score_calibration.get('tables') or {}).items()
            },
            'file': os.path.join(OUT_DIR, f'score_calibration_{day}_10d.json'),
        },
        'setup_grade_review': {
            'by_grade': (setup_grade.get('by_grade') or [])[:8],
            'by_side_setup_grade': (setup_grade.get('by_side_setup_grade') or [])[:12],
            'file': os.path.join(OUT_DIR, f'setup_grade_review_{day}_10d.json'),
        },
        'counterfactual_side_review': {
            'summary': (side_review.get('summary') or [])[:10],
            'promotion_note': side_review.get('promotion_note'),
            'file': os.path.join(OUT_DIR, f'counterfactual_side_review_{day}.json'),
        },
        'rule_attribution_review': {
            'top_hurt_or_watch': (rule_review.get('top_hurt_or_watch') or [])[:8],
            'top_helped': (rule_review.get('top_helped') or [])[:8],
            'file': os.path.join(OUT_DIR, f'rule_attribution_review_{day}_10d.json'),
        },
        'hypothesis_confidence': {
            'counts': hyp_conf.get('counts'),
            'top_rows': (hyp_conf.get('rows') or [])[:8],
            'file': os.path.join(OUT_DIR, f'hypothesis_confidence_{day}.json'),
        },
        'daily_conclusion_ledger': {
            'what_we_learned': conclusion.get('what_we_learned'),
            'what_we_rejected': conclusion.get('what_we_rejected'),
            'what_is_still_uncertain': conclusion.get('what_is_still_uncertain'),
            'what_to_watch_next': conclusion.get('what_to_watch_next'),
            'file': os.path.join(OUT_DIR, f'daily_conclusion_ledger_{day}.json'),
            'rolling_file': os.path.join(OUT_DIR, 'learning_conclusion_ledger.json'),
        },
        'matched_control_review': {
            'deduction_counts': matched.get('deduction_counts'),
            'top_rows': (matched.get('rows') or [])[:8],
            'file': os.path.join(OUT_DIR, f'matched_control_review_{day}_10d.json'),
        },
        'exit_efficiency_review': {
            'summary': (exit_eff.get('summary') or [])[:8],
            'file': os.path.join(OUT_DIR, f'exit_efficiency_review_{day}.json'),
        },
        'mae_mfe_timeline': {
            'summary': (timeline.get('summary') or [])[:8],
            'data_note': timeline.get('data_note'),
            'file': os.path.join(OUT_DIR, f'mae_mfe_timeline_{day}.json'),
        },
        'hypothesis_counterexamples': {
            'counts': hyp_counter.get('counts'),
            'top_rows': (hyp_counter.get('rows') or [])[:8],
            'file': os.path.join(OUT_DIR, f'hypothesis_counterexamples_{day}.json'),
        },
        'loser_fingerprints': {
            'summary': (fingerprints.get('summary') or [])[:8],
            'file': os.path.join(OUT_DIR, f'loser_fingerprints_{day}.json'),
        },
        'postmortem_section_confidence': {
            'counts': section_conf.get('counts'),
            'sections': section_conf.get('sections'),
            'file': os.path.join(OUT_DIR, f'postmortem_section_confidence_{day}.json'),
        },
        'trade_verdicts': {
            'counts': verdicts.get('counts'),
            'rows': (verdicts.get('rows') or [])[:12],
            'file': os.path.join(OUT_DIR, f'trade_verdicts_{day}.json'),
        },
        'best_avoided_loser_simulator': {
            'top_rows': (avoid.get('rows') or [])[:8],
            'file': os.path.join(OUT_DIR, f'best_avoided_loser_simulator_{day}.json'),
        },
        'rolling_learning_dashboard': {
            'totals': rolling.get('totals'),
            'recurring_loser_fingerprints': rolling.get('recurring_loser_fingerprints'),
            'daily': (rolling.get('daily') or [])[-5:],
            'file': os.path.join(OUT_DIR, f'rolling_learning_dashboard_{day}_10d.json'),
        },
        'true_counterfactual_replay': {
            'top_policies': (true_replay.get('policies') or [])[:8],
            'file': os.path.join(OUT_DIR, f'true_counterfactual_replay_{day}_10d.json'),
        },
        'experiment_registry': {
            'counts': experiments.get('counts'),
            'top_experiments': list((experiments.get('experiments') or {}).values())[:8],
            'file': os.path.join(OUT_DIR, 'experiment_registry.json'),
            'snapshot_file': os.path.join(OUT_DIR, f'experiment_registry_snapshot_{day}.json'),
        },
        'feature_outcome_table': {
            'rows': len(feature_table.get('rows') or []),
            'columns': feature_table.get('columns'),
            'file': os.path.join(OUT_DIR, f'feature_outcome_table_{day}_10d.json'),
        },
        'rule_confidence_decay': {
            'counts': confidence_decay.get('counts'),
            'top_rows': (confidence_decay.get('rows') or [])[:8],
            'file': os.path.join(OUT_DIR, f'rule_confidence_decay_{day}.json'),
        },
        'human_feedback_summary': {
            'counts': human_feedback.get('counts'),
            'feedback_file': human_feedback.get('feedback_file'),
            'global_feedback_file': human_feedback.get('global_feedback_file'),
            'file': os.path.join(OUT_DIR, f'human_feedback_summary_{day}.json'),
        },
        'market_regime_library': {
            'regimes': (market_library.get('regimes') or [])[:8],
            'current_day': next((r for r in market_library.get('daily') or [] if r.get('day') == day), None),
            'file': os.path.join(OUT_DIR, f'market_regime_library_{day}_20d.json'),
        },
        'weekly_promotion_meeting': {
            'decision_counts': {k: len(v or []) for k, v in (weekly_packet.get('decisions') or {}).items()},
            'experiment_counts': weekly_packet.get('experiment_counts'),
            'file': os.path.join(OUT_DIR, f'weekly_promotion_meeting_{day}_10d.json'),
        },
        'config_diff': _summary_only(build_config_diff(day), [
            'current_config_sha256',
            'previous_artifact_config_sha256',
            'changed_since_previous_artifact',
        ]),
        'review_status_summary': _compact_review_status(day),
        'artifact_files': _artifact_files(day),
        'promotion_queue_summary': {
            'active': len(promotion_queue.get('active') or []),
            'watch': len(promotion_queue.get('watch') or []),
            'promoted': len(promotion_queue.get('promoted') or []),
            'killed': len(promotion_queue.get('killed') or []),
            'top_active': (promotion_queue.get('active') or [])[:8],
            'file': os.path.join(OUT_DIR, 'promotion_queue.json'),
        },
        'no_change_recommendation': (
            'No automatic live changes. Promote only after multi-day evidence clears gates '
            'and winner damage is acceptable.'
        ),
    }


def write_json(name: str, payload: dict) -> str:
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, name)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, default=str)
    return path


def write_text(path: str, text: str) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(text)
    return path


def build_codex_context(day: str, review_index: Optional[dict] = None,
                        manifest: Optional[dict] = None) -> str:
    idx = review_index or build_review_index(day)
    man = manifest or build_artifact_manifest(day)
    headline = idx.get('headline') or {}
    trust = idx.get('trust') or {}
    files = (man.get('corpus_size_stats') or {}).get('review_preferred_files') or []
    lines = [
        f'# Codex Context - {day}',
        '',
        'Read this file first for future reviews. It points to compact artifacts so we do not load multi-MB raw corpuses unless needed.',
        '',
        '## Day Snapshot',
        f"- P&L: ${headline.get('pnl')}",
        f"- Trades: {headline.get('trades')}",
        f"- Win rate: {headline.get('win_rate')}%",
        f"- Profit factor: {headline.get('profit_factor')}",
        f"- Trust: {trust.get('trust')} ({trust.get('quality_score')}/100 {trust.get('quality_label')})",
        f"- Config SHA256: {idx.get('config_sha256')}",
        '',
        '## Read Order',
        f"1. {os.path.join(OUT_DIR, f'review_index_{day}.json')}",
        f"2. {os.path.join(OUT_DIR, f'POSTMORTEM_START_HERE_{day}.json')}",
        f"3. {os.path.join(OUT_DIR, f'artifacts_{day}.json')}",
        '',
        '## Preferred Compact Files',
    ]
    for path in files[:35]:
        lines.append(f'- {path}')
    lines.extend([
        '',
        '## Rule Discipline',
        'No automatic live rule changes. Promote only after clean multi-day evidence, acceptable winner damage, and explicit human approval.',
        '',
    ])
    return '\n'.join(lines)


def write_all(day: str, postmortem_payload: Optional[dict] = None) -> tuple[dict, dict]:
    if postmortem_payload is not None:
        old = _POSTMORTEM_OVERRIDES.get(day, _MISSING)
        _POSTMORTEM_OVERRIDES[day] = postmortem_payload
        try:
            return write_all(day)
        finally:
            if old is _MISSING:
                _POSTMORTEM_OVERRIDES.pop(day, None)
            else:
                _POSTMORTEM_OVERRIDES[day] = old

    promotion_payload = None
    promotion_path = None
    try:
        from promotion_queue import write_queue
        promotion_path, promotion_payload = write_queue(day)
        try:
            from promotion_review import write_promotion_review
            write_promotion_review(day)
        except Exception:
            pass
    except Exception as e:
        promotion_payload = {'error': str(e)}
    weekend_paths = {}
    weekend_payloads = {}
    try:
        from weekend_readiness import write_weekend_artifacts
        weekend_paths, weekend_payloads = write_weekend_artifacts(day)
    except Exception as e:
        weekend_payloads = {'error': str(e)}
    feedback_template_path = ensure_human_feedback_template(day)
    true_replay_payload = build_true_counterfactual_replay(day)
    feature_outcome_payload = build_feature_outcome_table(day)
    confidence_decay_payload = build_rule_confidence_decay(day)
    human_feedback_payload = build_human_feedback_summary(day)
    market_library_payload = build_market_regime_library(day)
    experiment_registry_path, experiment_snapshot_path, experiment_payload = write_experiment_registry(
        day,
        replay=true_replay_payload,
        human_feedback=human_feedback_payload,
    )
    weekly_packet_payload = build_weekly_promotion_meeting_packet(
        day,
        replay=true_replay_payload,
        registry=experiment_payload,
        confidence=confidence_decay_payload,
        regimes=market_library_payload,
        feedback=human_feedback_payload,
    )
    try:
        from tick_replay import build_tick_replay
        tick_replay_payload = build_tick_replay(day)
    except Exception as e:
        tick_replay_payload = {'error': str(e)}
    payloads = {
        'risk_summary': build_risk_summary(day),
        'loser_summary': build_loser_summary(day),
        'execution_summary': build_execution_summary(day),
        'shadow_exit_summary': build_shadow_exit_summary(day),
        'feature_coverage': build_feature_coverage(day),
        'data_collection_coverage': build_data_collection_coverage(day),
        'gate_timeline_summary': build_gate_timeline_summary(day),
        'no_signal_snapshot_summary': build_no_signal_snapshot_summary(day),
        'fill_attribution_summary': build_fill_attribution_summary(day),
        'latency_attribution_summary': build_latency_attribution_summary(day),
        'entry_timing_efficiency': build_entry_timing_efficiency(day),
        'dual_side_shadow_review': build_dual_side_shadow_review(day),
        'quote_condition_quality': build_quote_condition_quality_summary(day),
        'shadow_entry_variants': build_shadow_entry_variants_review(day),
        'artifact_schema_dictionary': build_artifact_schema_dictionary(),
        'new_gate_attribution': build_new_gate_attribution(day),
        'entry_retry_summary': build_entry_retry_summary(day),
        'exit_hierarchy_summary': build_exit_hierarchy_summary(day),
        'per_ticker_learning': build_per_ticker_learning(day),
        'regime_scoring_review': build_regime_scoring_review(day),
        'score_calibration': build_score_calibration(day),
        'setup_grade_review': build_setup_grade_review(day),
        'counterfactual_side_review': build_counterfactual_side_review(day),
        'rule_attribution_review': build_rule_attribution_review(day),
        'hypothesis_confidence': build_hypothesis_confidence(day),
        'market_tape_attribution': build_market_tape_attribution(day),
        'tick_replay': tick_replay_payload,
        'matched_control_review': build_matched_control_review(day),
        'exit_efficiency_review': build_exit_efficiency_review(day),
        'mae_mfe_timeline': build_mae_mfe_timeline(day),
        'hypothesis_counterexamples': build_hypothesis_counterexamples(day),
        'loser_fingerprints': build_loser_fingerprints(day),
        'postmortem_section_confidence': build_postmortem_section_confidence(day),
        'trade_verdicts': build_trade_verdicts(day),
        'best_avoided_loser_simulator': build_best_avoided_loser_simulator(day),
        'rolling_learning_dashboard': build_rolling_learning_dashboard(day),
        'true_counterfactual_replay': true_replay_payload,
        'feature_outcome_table': feature_outcome_payload,
        'rule_confidence_decay': confidence_decay_payload,
        'human_feedback_summary': human_feedback_payload,
        'weekly_promotion_meeting': weekly_packet_payload,
        'market_regime_library': market_library_payload,
        'experiment_registry': experiment_payload,
        'config_diff': build_config_diff(day),
        'promotion_queue': promotion_payload,
        'weekend_readiness': weekend_payloads,
    }
    daily_conclusion_path, daily_conclusion_payload = write_daily_conclusion_ledger(day)
    payloads['daily_conclusion_ledger'] = daily_conclusion_payload
    tick_replay_path = write_json(f'tick_replay_{day}.json', payloads['tick_replay'])
    payloads['review_index'] = build_review_index(day)
    payloads['monday_review_packet'] = build_monday_review_packet(day)
    paths = {
        'risk_summary': write_json(f'risk_summary_{day}.json', payloads['risk_summary']),
        'loser_summary': write_json(f'loser_summary_{day}.json', payloads['loser_summary']),
        'execution_summary': write_json(f'execution_summary_{day}.json', payloads['execution_summary']),
        'shadow_exit_summary': write_json(f'shadow_exit_summary_{day}.json', payloads['shadow_exit_summary']),
        'feature_coverage': write_json(f'feature_coverage_{day}.json', payloads['feature_coverage']),
        'data_collection_coverage': write_json(f'data_collection_coverage_{day}.json', payloads['data_collection_coverage']),
        'gate_timeline_summary': write_json(f'gate_timeline_summary_{day}.json', payloads['gate_timeline_summary']),
        'no_signal_snapshot_summary': write_json(f'no_signal_snapshot_summary_{day}.json', payloads['no_signal_snapshot_summary']),
        'fill_attribution_summary': write_json(f'fill_attribution_summary_{day}.json', payloads['fill_attribution_summary']),
        'latency_attribution_summary': write_json(f'latency_attribution_summary_{day}.json', payloads['latency_attribution_summary']),
        'entry_timing_efficiency': write_json(f'entry_timing_efficiency_{day}.json', payloads['entry_timing_efficiency']),
        'dual_side_shadow_review': write_json(f'dual_side_shadow_review_{day}.json', payloads['dual_side_shadow_review']),
        'quote_condition_quality': write_json(f'quote_condition_quality_{day}.json', payloads['quote_condition_quality']),
        'shadow_entry_variants': write_json(f'shadow_entry_variants_{day}.json', payloads['shadow_entry_variants']),
        'artifact_schema_dictionary': write_json('ARTIFACT_SCHEMA_DICTIONARY.json', payloads['artifact_schema_dictionary']),
        'new_gate_attribution': write_json(f'new_gate_attribution_{day}.json', payloads['new_gate_attribution']),
        'entry_retry_summary': write_json(f'entry_retry_summary_{day}.json', payloads['entry_retry_summary']),
        'exit_hierarchy_summary': write_json(f'exit_hierarchy_summary_{day}.json', payloads['exit_hierarchy_summary']),
        'per_ticker_learning': write_json(f'per_ticker_learning_{day}.json', payloads['per_ticker_learning']),
        'regime_scoring_review': write_json(f'regime_scoring_review_{day}.json', payloads['regime_scoring_review']),
        'score_calibration': write_json(f'score_calibration_{day}_10d.json', payloads['score_calibration']),
        'setup_grade_review': write_json(f'setup_grade_review_{day}_10d.json', payloads['setup_grade_review']),
        'counterfactual_side_review': write_json(f'counterfactual_side_review_{day}.json', payloads['counterfactual_side_review']),
        'rule_attribution_review': write_json(f'rule_attribution_review_{day}_10d.json', payloads['rule_attribution_review']),
        'hypothesis_confidence': write_json(f'hypothesis_confidence_{day}.json', payloads['hypothesis_confidence']),
        'daily_conclusion_ledger': daily_conclusion_path,
        'market_tape_attribution': write_json(f'market_tape_attribution_{day}.json', payloads['market_tape_attribution']),
        'tick_replay': tick_replay_path,
        'matched_control_review': write_json(f'matched_control_review_{day}_10d.json', payloads['matched_control_review']),
        'exit_efficiency_review': write_json(f'exit_efficiency_review_{day}.json', payloads['exit_efficiency_review']),
        'mae_mfe_timeline': write_json(f'mae_mfe_timeline_{day}.json', payloads['mae_mfe_timeline']),
        'hypothesis_counterexamples': write_json(f'hypothesis_counterexamples_{day}.json', payloads['hypothesis_counterexamples']),
        'loser_fingerprints': write_json(f'loser_fingerprints_{day}.json', payloads['loser_fingerprints']),
        'postmortem_section_confidence': write_json(f'postmortem_section_confidence_{day}.json', payloads['postmortem_section_confidence']),
        'trade_verdicts': write_json(f'trade_verdicts_{day}.json', payloads['trade_verdicts']),
        'best_avoided_loser_simulator': write_json(f'best_avoided_loser_simulator_{day}.json', payloads['best_avoided_loser_simulator']),
        'rolling_learning_dashboard': write_json(f'rolling_learning_dashboard_{day}_10d.json', payloads['rolling_learning_dashboard']),
        'true_counterfactual_replay': write_json(f'true_counterfactual_replay_{day}_10d.json', payloads['true_counterfactual_replay']),
        'feature_outcome_table': write_json(f'feature_outcome_table_{day}_10d.json', payloads['feature_outcome_table']),
        'rule_confidence_decay': write_json(f'rule_confidence_decay_{day}.json', payloads['rule_confidence_decay']),
        'human_feedback_summary': write_json(f'human_feedback_summary_{day}.json', payloads['human_feedback_summary']),
        'human_feedback_day': feedback_template_path,
        'human_feedback_global': os.path.join(OUT_DIR, 'human_feedback.json'),
        'weekly_promotion_meeting': write_json(f'weekly_promotion_meeting_{day}_10d.json', payloads['weekly_promotion_meeting']),
        'market_regime_library': write_json(f'market_regime_library_{day}_20d.json', payloads['market_regime_library']),
        'experiment_registry': experiment_registry_path,
        'experiment_registry_snapshot': experiment_snapshot_path,
        'config_diff': write_json(f'config_diff_{day}.json', payloads['config_diff']),
        'promotion_queue': promotion_path,
        **weekend_paths,
        'monday_review_packet': write_json(f'monday_review_packet_{day}.json', payloads['monday_review_packet']),
    }
    manifest = build_artifact_manifest(day)
    payloads['review_index']['artifact_manifest'] = manifest
    paths['review_index'] = write_json(f'review_index_{day}.json', payloads['review_index'])
    paths['artifact_manifest'] = write_json(f'artifacts_{day}.json', manifest)
    payloads['artifact_manifest'] = manifest
    context_text = build_codex_context(day, payloads['review_index'], manifest)
    paths['codex_context'] = write_text(os.path.join(OUT_DIR, f'CODEX_CONTEXT_{day}.md'), context_text)
    paths['codex_context_latest'] = write_text(os.path.join(HERE, 'CODEX_CONTEXT.md'), context_text)
    paths['review_index_latest'] = write_json('REVIEW_INDEX_LATEST.json', payloads['review_index'])
    return paths, payloads


def main() -> int:
    ap = argparse.ArgumentParser(description='Write compact trading-review artifacts.')
    ap.add_argument('day')
    ap.add_argument('--json', action='store_true')
    ap.add_argument('--paths-only', action='store_true',
                    help='With --json, print only written artifact paths to keep command output compact.')
    args = ap.parse_args()
    paths, payloads = write_all(args.day)
    if args.json:
        body = {'paths': paths} if args.paths_only else {'paths': paths, 'payloads': payloads}
        print(json.dumps(body, indent=2, default=str))
    else:
        for path in paths.values():
            print(path)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
