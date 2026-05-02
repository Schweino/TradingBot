from __future__ import annotations

import argparse
import hashlib
import json
import os
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


def _manifest_paths(day: str) -> list[str]:
    return [
        os.path.join(OUT_DIR, f'postmortem_{day}.json'),
        os.path.join(OUT_DIR, f'postmortem_{day}.txt'),
        os.path.join(OUT_DIR, f'review_index_{day}.json'),
        os.path.join(OUT_DIR, f'loser_summary_{day}.json'),
        os.path.join(OUT_DIR, f'risk_summary_{day}.json'),
        os.path.join(OUT_DIR, f'execution_summary_{day}.json'),
        os.path.join(OUT_DIR, f'feature_coverage_{day}.json'),
        os.path.join(OUT_DIR, f'new_gate_attribution_{day}.json'),
        os.path.join(OUT_DIR, f'shadow_exit_summary_{day}.json'),
        os.path.join(OUT_DIR, f'entry_retry_summary_{day}.json'),
        os.path.join(OUT_DIR, f'exit_hierarchy_summary_{day}.json'),
        os.path.join(OUT_DIR, f'per_ticker_learning_{day}.json'),
        os.path.join(OUT_DIR, f'regime_scoring_review_{day}.json'),
        os.path.join(OUT_DIR, f'config_diff_{day}.json'),
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

    spread_block_rows = [
        r for r in [*skipped, *retry_rows]
        if 'spread' in str(r.get('reason') or '').lower()
        and (
            'quality_gate' in str(r.get('reason') or '').lower()
            or 'hard_gate' in str(r.get('reason') or '').lower()
            or 'spread_too_wide' in str(r.get('reason') or '').lower()
        )
    ]

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


def build_per_ticker_learning(day: str, lookback: int = 7) -> dict:
    days = []
    if os.path.isdir(OUT_DIR):
        for name in sorted(os.listdir(OUT_DIR)):
            if name.startswith('postmortem_') and name.endswith('.json'):
                d = name[len('postmortem_'):-len('.json')]
                if d <= day:
                    days.append(d)
    days = days[-lookback:]
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
    days = []
    if os.path.isdir(OUT_DIR):
        for name in sorted(os.listdir(OUT_DIR)):
            if name.startswith('postmortem_') and name.endswith('.json'):
                d = name[len('postmortem_'):-len('.json')]
                if d <= day:
                    days.append(d)
    days = days[-lookback:]
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
    new_gate_attr = build_new_gate_attribution(day)
    shadow = build_shadow_exit_summary(day)
    retry = build_entry_retry_summary(day)
    ticker_learning = build_per_ticker_learning(day)
    regime_review = build_regime_scoring_review(day)
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
                os.path.join(OUT_DIR, 'change_impact_ledger.json'),
                os.path.join(OUT_DIR, 'config_intent_ledger.json'),
                os.path.join(OUT_DIR, 'promotion_queue.json'),
            ],
        },
        'files': infos,
    }


def build_monday_review_packet(day: str) -> dict:
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
            'trade_alerts',
            'review_start_here',
            'multi_day_scorecard',
            'market_regime_day',
            'edge_quality_activity',
            'order_latency',
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
        'entry_retry_summary': build_entry_retry_summary(day),
        'exit_hierarchy_summary': build_exit_hierarchy_summary(day),
        'per_ticker_learning': build_per_ticker_learning(day),
        'regime_scoring_review': build_regime_scoring_review(day),
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
    for path in files[:20]:
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
    payloads = {
        'risk_summary': build_risk_summary(day),
        'loser_summary': build_loser_summary(day),
        'execution_summary': build_execution_summary(day),
        'shadow_exit_summary': build_shadow_exit_summary(day),
        'feature_coverage': build_feature_coverage(day),
        'new_gate_attribution': build_new_gate_attribution(day),
        'entry_retry_summary': build_entry_retry_summary(day),
        'exit_hierarchy_summary': build_exit_hierarchy_summary(day),
        'per_ticker_learning': build_per_ticker_learning(day),
        'regime_scoring_review': build_regime_scoring_review(day),
        'config_diff': build_config_diff(day),
        'promotion_queue': promotion_payload,
        'weekend_readiness': weekend_payloads,
    }
    payloads['review_index'] = build_review_index(day)
    payloads['monday_review_packet'] = build_monday_review_packet(day)
    paths = {
        'risk_summary': write_json(f'risk_summary_{day}.json', payloads['risk_summary']),
        'loser_summary': write_json(f'loser_summary_{day}.json', payloads['loser_summary']),
        'execution_summary': write_json(f'execution_summary_{day}.json', payloads['execution_summary']),
        'shadow_exit_summary': write_json(f'shadow_exit_summary_{day}.json', payloads['shadow_exit_summary']),
        'feature_coverage': write_json(f'feature_coverage_{day}.json', payloads['feature_coverage']),
        'new_gate_attribution': write_json(f'new_gate_attribution_{day}.json', payloads['new_gate_attribution']),
        'entry_retry_summary': write_json(f'entry_retry_summary_{day}.json', payloads['entry_retry_summary']),
        'exit_hierarchy_summary': write_json(f'exit_hierarchy_summary_{day}.json', payloads['exit_hierarchy_summary']),
        'per_ticker_learning': write_json(f'per_ticker_learning_{day}.json', payloads['per_ticker_learning']),
        'regime_scoring_review': write_json(f'regime_scoring_review_{day}.json', payloads['regime_scoring_review']),
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
    args = ap.parse_args()
    paths, payloads = write_all(args.day)
    if args.json:
        print(json.dumps({'paths': paths, 'payloads': payloads}, indent=2, default=str))
    else:
        for path in paths.values():
            print(path)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
