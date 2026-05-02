from __future__ import annotations

import argparse
import csv
import json
import os
import re
from collections import Counter, defaultdict
from datetime import datetime, time
from typing import Any, Callable, Iterable, Optional
from zoneinfo import ZoneInfo

from scalp_replay import replay_day


HERE = os.path.dirname(os.path.abspath(__file__))
POSTMORTEM_DIR = os.path.join(HERE, 'postmortem')
TICK_LOG_DIR = os.path.join(HERE, 'tick_logs')
CT = ZoneInfo('America/Chicago')


def _read_json(path: str) -> dict:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def _iter_jsonl(path: str) -> Iterable[dict]:
    if not os.path.exists(path):
        return
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                yield json.loads(line)
            except Exception:
                continue


def _day_path(day: str, folder: str, stem: str) -> str:
    return os.path.join(POSTMORTEM_DIR, folder, f'{stem}_{day}.jsonl')


def _tick_paths_for_day(day: str) -> list[str]:
    folder = os.path.join(TICK_LOG_DIR, day)
    if not os.path.isdir(folder):
        return []
    return [
        os.path.join(folder, name)
        for name in sorted(os.listdir(folder))
        if name.lower().endswith('.jsonl')
    ]


def _ts_to_ct(ts: Optional[float]) -> Optional[str]:
    if not ts:
        return None
    try:
        return datetime.fromtimestamp(float(ts), CT).isoformat(timespec='seconds')
    except Exception:
        return None


def _ct_time_to_ts(day: str, hhmm: Optional[str]) -> Optional[int]:
    if not hhmm:
        return None
    try:
        hh, mm = [int(x) for x in str(hhmm).split(':')[:2]]
        dt = datetime.combine(datetime.fromisoformat(day).date(), time(hh, mm), tzinfo=CT)
        return int(dt.timestamp())
    except Exception:
        return None


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None or v == '':
            return None
        return float(v)
    except Exception:
        return None


def _infer_setup_from_reason(row: dict) -> Optional[str]:
    reason = str(row.get('reason') or '').lower()
    if 'flow' in reason:
        return 'flow_exhaustion_fade'
    if 'btc' in reason:
        return 'btc_relative_strength'
    if 'vwap' in reason:
        return 'vwap_reclaim_breakdown'
    if 'pullback' in reason:
        return 'trend_pullback'
    return None


def _setup_from_components(components: dict) -> Optional[str]:
    if not components:
        return None
    if components.get('flow_fade_confirmed') or 'flow_30s' in components:
        return 'flow_exhaustion_fade'
    if 'relative_strength' in components or 'btc' in components or 'btc_lead_lag' in components:
        return 'btc_relative_strength'
    if components.get('momentum') or components.get('burst'):
        return 'momentum_breakout'
    return None


def _nested(row: dict, *path: str) -> Any:
    cur: Any = row
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def _signal_quality(row: dict) -> dict:
    return (
        row.get('signal_quality')
        or _nested(row, 'forensics', 'signal_quality')
        or _nested(row, 'entry_thesis', 'signal_quality')
        or {}
    )


def _indicators(row: dict) -> dict:
    return (
        row.get('indicators')
        or row.get('ind')
        or _nested(row, 'forensics', 'indicators')
        or _nested(row, 'entry_thesis', 'indicators')
        or {}
    )


def _btc(row: dict) -> dict:
    return (
        row.get('btc_indicators')
        or row.get('btc')
        or row.get('btc_context')
        or _nested(row, 'forensics', 'btc')
        or _nested(row, 'entry_thesis', 'btc')
        or {}
    )


def _execution(row: dict) -> dict:
    return (
        row.get('execution_quality')
        or _nested(row, 'forensics', 'execution_quality')
        or _nested(row, 'entry_thesis', 'execution_quality')
        or {}
    )


def _relative_strength(row: dict) -> dict:
    return (
        row.get('relative_strength')
        or _nested(row, 'forensics', 'relative_strength')
        or _nested(row, 'entry_thesis', 'relative_strength')
        or {}
    )


def _components(row: dict) -> dict:
    quality = _signal_quality(row)
    return quality.get('score_components') or row.get('components') or {}


def _audit_row(source: str, row: dict) -> dict:
    ind = _indicators(row)
    btc = _btc(row)
    ex = _execution(row)
    rel = _relative_strength(row)
    quality = _signal_quality(row)
    components = _components(row)
    ts = (
        row.get('created_at')
        or row.get('opened_at')
        or row.get('closed_at')
        or _nested(row, 'entry_thesis', 'captured_at')
        or _nested(row, 'forensics', 'captured_at')
    )
    if not ts and row.get('_day_iso'):
        ts = _ct_time_to_ts(row.get('_day_iso'), row.get('time'))
    score = _safe_float(row.get('score') or _nested(row, 'signal', 'score'))
    side = row.get('side')
    flow_30 = ind.get('flow_30s') or {}
    flow_120 = ind.get('flow_120s') or {}
    setup = (
        row.get('setup_type')
        or _nested(row, 'forensics', 'setup_type')
        or row.get('setup')
        or _setup_from_components(components)
        or _infer_setup_from_reason(row)
    )
    decision = row.get('decision')
    if source == 'actual_trade':
        decision = 'taken'
    return {
        'ts': ts,
        'ct': _ts_to_ct(ts),
        'source': source,
        'decision': decision or source,
        'ticker': row.get('ticker') or ind.get('symbol'),
        'side': side,
        'setup_type': setup,
        'conviction': row.get('conviction') or row.get('conv'),
        'price': _safe_float(row.get('price') or row.get('entry')),
        'score': score,
        'abs_score': abs(score) if score is not None else None,
        'reason': row.get('reason'),
        'result': row.get('result'),
        'pnl': _safe_float(row.get('pnl')),
        'duration_min': _safe_float(row.get('duration_min')),
        'exit_reason': row.get('exit_reason') or row.get('reason') if source == 'actual_trade' else None,
        'stock_mom_15s': _safe_float(ind.get('mom_15s')),
        'stock_mom_60s': _safe_float(ind.get('mom_60s')),
        'stock_ema_stack': ind.get('ema_stack'),
        'vwap_dist': _safe_float(ind.get('vwap_dist')),
        'vwap_dist_sigma': _safe_float(ind.get('vwap_dist_sigma')),
        'flow_30s_buy_pct': _safe_float(flow_30.get('buy_pct')),
        'flow_30s_delta': _safe_float(ind.get('flow_30s_delta')),
        'flow_120s_buy_pct': _safe_float(flow_120.get('buy_pct')),
        'vol_z_60s': _safe_float(ind.get('vol_z_60s')),
        'tick_z_30s': _safe_float(ind.get('tick_z_30s')),
        'btc_regime': btc.get('regime'),
        'btc_stack': btc.get('stack') or btc.get('ema_stack'),
        'btc_mom_15s': _safe_float(btc.get('mom_15s')),
        'btc_mom_60s': _safe_float(btc.get('mom_60s')),
        'btc_stale': bool(btc.get('stale')) if 'stale' in btc else None,
        'stock_minus_btc_60s': _safe_float(
            rel.get('stock_minus_btc_implied_60s')
            or btc.get('stock_minus_btc_implied_60s')
        ),
        'execution_score': _safe_float(ex.get('score')),
        'spread_pct': _safe_float(ex.get('spread_pct') or _nested(row, 'forensics', 'entry_quality', 'spread_pct')),
        'quote_age_sec': _safe_float(ex.get('quote_age_sec') or _nested(row, 'forensics', 'entry_quality', 'quote_age_sec')),
        'near_vwap_chop': quality.get('near_vwap_chop'),
        'btc_conflict': quality.get('btc_conflict'),
        'flow_fade_confirmed': quality.get('flow_fade_confirmed'),
        'score_components': json.dumps(components, sort_keys=True),
        'reasons': json.dumps(row.get('reasons') or _nested(row, 'signal', 'reasons') or []),
    }


def load_decision_audit(day: str) -> list[dict]:
    rows: list[dict] = []
    pm_path = os.path.join(POSTMORTEM_DIR, f'postmortem_{day}.json')
    if os.path.exists(pm_path):
        pm = _read_json(pm_path)
        for trade in pm.get('tape', []):
            trade = {**trade, '_day_iso': day}
            rows.append(_audit_row('actual_trade', trade))

    for row in _iter_jsonl(_day_path(day, 'skipped_signals', 'skipped_signals')):
        rows.append(_audit_row('skipped_signal', row))
    for row in _iter_jsonl(_day_path(day, 'shadow_decisions', 'shadow_decisions')):
        rows.append(_audit_row('shadow_decision', row))
    for row in _iter_jsonl(_day_path(day, 'near_signals', 'near_signals')):
        rows.append(_audit_row('near_signal', row))
    rows.sort(key=lambda r: (r.get('ts') or 0, r.get('source') or ''))
    return rows


def _gate_strict_short_flow(row: dict) -> tuple[bool, Optional[str]]:
    if row.get('side') != 'SHORT':
        return True, None
    flow_120 = row.get('flow_120s_buy_pct')
    flow_delta = row.get('flow_30s_delta')
    btc60 = row.get('btc_mom_60s')
    stock_vs_btc = row.get('stock_minus_btc_60s')
    if flow_120 is not None and flow_120 >= 58:
        rolling = flow_delta is not None and flow_delta <= -4
        strong_macro = btc60 is not None and btc60 <= -0.18
        stock_weak = stock_vs_btc is not None and stock_vs_btc <= -0.18
        if not (rolling or strong_macro or stock_weak):
            return False, 'short_buy_flow_not_exhausted'
    return True, None


def _gate_btc_alignment(row: dict) -> tuple[bool, Optional[str]]:
    side = row.get('side')
    b15 = row.get('btc_mom_15s')
    b60 = row.get('btc_mom_60s')
    if side == 'LONG' and b15 is not None and b60 is not None and b15 < -0.05 and b60 < -0.08:
        return False, 'long_against_btc'
    if side == 'SHORT' and b15 is not None and b60 is not None and b15 > 0.05 and b60 > 0.08:
        return False, 'short_against_btc'
    return True, None


def _gate_execution_quality_65(row: dict) -> tuple[bool, Optional[str]]:
    score = row.get('execution_score')
    if score is not None and score < 65:
        return False, 'execution_score_below_65'
    return True, None


def _gate_no_chop(row: dict) -> tuple[bool, Optional[str]]:
    if row.get('near_vwap_chop') is True:
        return False, 'near_vwap_chop'
    return True, None


def _gate_score_6(row: dict) -> tuple[bool, Optional[str]]:
    if (row.get('abs_score') or 0) < 6:
        return False, 'abs_score_below_6'
    return True, None


Gate = Callable[[dict], tuple[bool, Optional[str]]]


GATES: dict[str, list[Gate]] = {
    'current_observed': [],
    'score_6_plus': [_gate_score_6],
    'btc_alignment_hard': [_gate_btc_alignment],
    'execution_quality_65': [_gate_execution_quality_65],
    'strict_short_flow_rollover': [_gate_strict_short_flow],
    'no_chop_score_6_execution_65': [_gate_no_chop, _gate_score_6, _gate_execution_quality_65],
    'elite_combo_v1': [_gate_score_6, _gate_btc_alignment, _gate_execution_quality_65, _gate_strict_short_flow],
}


def _apply_gates(row: dict, gates: list[Gate]) -> tuple[bool, list[str]]:
    reasons = []
    for gate in gates:
        ok, reason = gate(row)
        if not ok:
            reasons.append(reason or gate.__name__)
    return not reasons, reasons


def gate_scoreboard(rows: list[dict]) -> list[dict]:
    actual = [r for r in rows if r.get('source') == 'actual_trade']
    board = []
    for name, gates in GATES.items():
        kept = []
        blocked = []
        block_reasons = Counter()
        for row in actual:
            ok, reasons = _apply_gates(row, gates)
            if ok:
                kept.append(row)
            else:
                blocked.append(row)
                block_reasons.update(reasons)
        kept_pnl = sum(r.get('pnl') or 0 for r in kept)
        blocked_pnl = sum(r.get('pnl') or 0 for r in blocked)
        wins = sum(1 for r in kept if (r.get('pnl') or 0) > 0)
        losses = sum(1 for r in kept if (r.get('pnl') or 0) < 0)
        board.append({
            'variant': name,
            'kept': len(kept),
            'blocked': len(blocked),
            'kept_wins': wins,
            'kept_losses': losses,
            'kept_win_rate': round(wins / len(kept) * 100, 1) if kept else None,
            'kept_pnl': round(kept_pnl, 2),
            'blocked_pnl': round(blocked_pnl, 2),
            'estimated_delta_vs_actual': round(-blocked_pnl, 2),
            'block_reasons': dict(block_reasons),
            'blocked_trades': [
                {
                    'ct': r.get('ct'),
                    'ticker': r.get('ticker'),
                    'side': r.get('side'),
                    'setup_type': r.get('setup_type'),
                    'pnl': r.get('pnl'),
                    'reason': r.get('reason'),
                    'block_reasons': _apply_gates(r, gates)[1],
                }
                for r in blocked[:50]
            ],
            'winner_sacrifice': winner_sacrifice_analysis(blocked),
        })
    return sorted(board, key=lambda r: (r['kept_pnl'], r['estimated_delta_vs_actual']), reverse=True)


def _warning_tags_for_row(row: dict) -> list[str]:
    tags = []
    if (row.get('abs_score') or 0) < 6:
        tags.append('low_abs_score')
    if row.get('execution_score') is not None and row.get('execution_score') < 65:
        tags.append('weak_execution')
    if row.get('spread_pct') is not None and row.get('spread_pct') >= 0.12:
        tags.append('wide_spread')
    if row.get('btc_conflict') is True or _gate_btc_alignment(row)[0] is False:
        tags.append('btc_conflict_or_against_btc')
    if row.get('side') == 'SHORT' and row.get('flow_120s_buy_pct') is not None and row.get('flow_120s_buy_pct') >= 58:
        tags.append('short_buy_flow_high')
    if row.get('side') == 'LONG' and row.get('flow_120s_buy_pct') is not None and row.get('flow_120s_buy_pct') <= 45:
        tags.append('long_weak_flow')
    if row.get('vwap_dist_sigma') is not None and abs(row.get('vwap_dist_sigma')) >= 2.5:
        tags.append('extended_from_vwap')
    return tags


def winner_sacrifice_analysis(blocked_rows: list[dict]) -> dict:
    winners = [r for r in blocked_rows if (r.get('pnl') or 0) > 0]
    tag_counts = Counter()
    examples = []
    for row in winners:
        tags = _warning_tags_for_row(row)
        tag_counts.update(tags or ['clean_winner'])
        if len(examples) < 12:
            examples.append({
                'ct': row.get('ct'),
                'ticker': row.get('ticker'),
                'side': row.get('side'),
                'setup_type': row.get('setup_type'),
                'pnl': row.get('pnl'),
                'warning_tags': tags,
                'reason': row.get('reason'),
            })
    return {
        'blocked_winners': len(winners),
        'blocked_winner_pnl': round(sum(r.get('pnl') or 0 for r in winners), 2),
        'warning_tag_counts': dict(tag_counts),
        'examples': examples,
    }


def loser_diagnostics(rows: list[dict]) -> dict:
    losers = [r for r in rows if r.get('source') == 'actual_trade' and (r.get('pnl') or 0) < 0]
    tags = Counter()
    by_setup = Counter()
    by_side = Counter()
    examples = defaultdict(list)
    for row in losers:
        by_setup[row.get('setup_type') or 'unknown'] += 1
        by_side[row.get('side') or 'unknown'] += 1
        checks = {
            'low_abs_score': (row.get('abs_score') or 0) < 6,
            'weak_execution': row.get('execution_score') is not None and row['execution_score'] < 65,
            'wide_spread': row.get('spread_pct') is not None and row['spread_pct'] >= 0.12,
            'btc_conflict': row.get('btc_conflict') is True,
            'short_buy_flow_high': row.get('side') == 'SHORT' and row.get('flow_120s_buy_pct') is not None and row['flow_120s_buy_pct'] >= 58,
            'short_flow_not_rolling': row.get('side') == 'SHORT' and row.get('flow_30s_delta') is not None and row['flow_30s_delta'] > -4,
            'long_weak_flow': row.get('side') == 'LONG' and row.get('flow_120s_buy_pct') is not None and row['flow_120s_buy_pct'] <= 45,
            'against_btc': _gate_btc_alignment(row)[0] is False,
            'extended_from_vwap': row.get('vwap_dist_sigma') is not None and abs(row['vwap_dist_sigma']) >= 2.5,
        }
        for tag, active in checks.items():
            if active:
                tags[tag] += 1
                if len(examples[tag]) < 5:
                    examples[tag].append({
                        'ct': row.get('ct'),
                        'ticker': row.get('ticker'),
                        'side': row.get('side'),
                        'pnl': row.get('pnl'),
                        'setup_type': row.get('setup_type'),
                    })
    return {
        'losers': len(losers),
        'by_side': dict(by_side),
        'by_setup': dict(by_setup),
        'tags': dict(tags),
        'examples': dict(examples),
    }


def normalization_quality(rows: list[dict]) -> dict:
    actual = [r for r in rows if r.get('source') == 'actual_trade']
    fields = (
        'ct', 'setup_type', 'score', 'stock_mom_60s', 'btc_mom_60s',
        'flow_120s_buy_pct', 'execution_score',
    )
    completeness = {}
    for field in fields:
        have = sum(1 for r in actual if r.get(field) not in (None, '', 'unknown'))
        completeness[field] = round(have / len(actual) * 100, 1) if actual else None
    inferred_setup = sum(1 for r in actual if r.get('setup_type') and r.get('setup_type') != 'unknown')
    return {
        'actual_trades': len(actual),
        'completeness_pct': completeness,
        'setup_known': inferred_setup,
    }


def decision_corpus_summary(rows: list[dict]) -> dict:
    by_source = Counter(r.get('source') for r in rows)
    by_decision = Counter(r.get('decision') for r in rows)
    by_reason = Counter(r.get('reason') for r in rows if r.get('reason'))
    actual = [r for r in rows if r.get('source') == 'actual_trade']
    return {
        'rows': len(rows),
        'by_source': dict(by_source),
        'by_decision': dict(by_decision),
        'top_reasons': dict(by_reason.most_common(15)),
        'actual': {
            'trades': len(actual),
            'pnl': round(sum(r.get('pnl') or 0 for r in actual), 2),
            'wins': sum(1 for r in actual if (r.get('pnl') or 0) > 0),
            'losses': sum(1 for r in actual if (r.get('pnl') or 0) < 0),
        },
    }


def _load_tick_capture(path: str) -> dict:
    header = {}
    events = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                row = json.loads(line)
            except Exception:
                continue
            if row.get('type') == 'header':
                header = row
                continue
            row_sym = (row.get('symbol') or header.get('ticker') or '').upper()
            header_sym = (header.get('ticker') or '').upper()
            if header_sym and row_sym and row_sym != header_sym:
                continue
            if row.get('event') == 'trade' and row.get('price') is not None and row.get('ts_ms'):
                events.append(row)
    events.sort(key=lambda r: r.get('ts_ms') or 0)
    sig = header.get('signal') or {}
    started_ms = header.get('started_ms') or sig.get('ts')
    return {
        'path': path,
        'ticker': (header.get('ticker') or sig.get('ticker') or os.path.basename(path).split('_', 1)[0]).upper(),
        'side': sig.get('side'),
        'entry': _safe_float(sig.get('price')),
        'started_ms': int(started_ms) if started_ms else None,
        'started_ts': int(started_ms // 1000) if isinstance(started_ms, (int, float)) else None,
        'signal': sig,
        'events': events,
        'event_count': len(events),
    }


def load_tick_captures(day: str) -> list[dict]:
    captures = []
    for path in _tick_paths_for_day(day):
        try:
            cap = _load_tick_capture(path)
        except Exception:
            continue
        if cap.get('event_count'):
            captures.append(cap)
    captures.sort(key=lambda c: c.get('started_ms') or 0)
    return captures


def _trade_qty(trade: dict) -> float:
    qty = _safe_float(trade.get('qty'))
    if qty:
        return qty
    entry = _safe_float(trade.get('entry')) or 0
    exit_px = _safe_float(trade.get('exit')) or 0
    pnl = _safe_float(trade.get('pnl')) or 0
    side = trade.get('side')
    per_share = (exit_px - entry) if side == 'LONG' else (entry - exit_px)
    if per_share:
        return abs(pnl / per_share)
    alloc = _safe_float(trade.get('alloc')) or _safe_float(trade.get('allocation')) or 0
    return alloc / entry if entry else 0


def _match_capture(trade: dict, captures: list[dict]) -> Optional[dict]:
    tkr = (trade.get('ticker') or '').upper()
    side = trade.get('side')
    entry_ts = trade.get('opened_at') or _ct_time_to_ts(trade.get('_day_iso'), trade.get('time'))
    candidates = [
        c for c in captures
        if c.get('ticker') == tkr and (not side or c.get('side') == side)
    ]
    if not candidates:
        return None
    if entry_ts:
        return min(candidates, key=lambda c: abs((c.get('started_ts') or 0) - entry_ts))
    entry = _safe_float(trade.get('entry'))
    if entry:
        return min(candidates, key=lambda c: abs((c.get('entry') or 0) - entry))
    return candidates[0]


def _signed_return(side: str, entry: float, price: float) -> float:
    if side == 'LONG':
        return (price - entry) / entry * 100
    return (entry - price) / entry * 100


def _pnl_from_return(entry: float, qty: float, ret_pct: float) -> float:
    return entry * qty * ret_pct / 100.0


EXIT_POLICIES = {
    'actual_close': {},
    'hard_adverse_0_45': {'hard_adverse_pct': 0.45},
    'failed_followthrough_60s': {'failed_after_sec': 60, 'failed_min_mfe_pct': 0.12},
    'failed_followthrough_90s': {'failed_after_sec': 90, 'failed_min_mfe_pct': 0.12},
    'profit_protect_aggressive': {'profit_trigger_pct': 0.30, 'profit_giveback_pct': 0.45},
    'profit_protect_live': {'profit_trigger_pct': 0.45, 'profit_giveback_pct': 0.55},
    'trail_after_half_pct': {'profit_trigger_pct': 0.50, 'profit_giveback_pct': 0.35},
}


EXIT_POLICY_CANDIDATE_MAP = {
    'failed_followthrough_90s': {
        'staged_config': {
            'adaptive_management.failed_followthrough_after_sec': 90,
            'adaptive_management.failed_followthrough_min_mfe_pct': 0.12,
        },
        'live_behavior_change': (
            'Tightens failed-follow-through review from the live baseline only after '
            'multi-day evidence confirms it saves losers without cutting too many winners.'
        ),
    },
    'trail_after_half_pct': {
        'staged_config': {
            'adaptive_management.profit_protect_trigger_pct': 0.50,
            'adaptive_management.profit_protect_giveback_pct': 0.35,
        },
        'live_behavior_change': (
            'Lets trades reach roughly +0.50% MFE, then trails more tightly than the '
            'current profit-protection giveback.'
        ),
    },
}


def _simulate_exit_policy(trade: dict, cap: dict, policy: str, cfg: dict) -> dict:
    entry = _safe_float(trade.get('entry')) or cap.get('entry') or 0
    side = trade.get('side') or cap.get('side')
    qty = _trade_qty(trade)
    if not entry or side not in ('LONG', 'SHORT') or not cap.get('events'):
        return {'policy': policy, 'status': 'missing_inputs'}
    start_ms = cap.get('started_ms') or cap['events'][0].get('ts_ms')
    best_ret = -999.0
    worst_ret = 999.0
    best_price = entry
    worst_price = entry
    for ev in cap['events']:
        price = _safe_float(ev.get('price'))
        if price is None:
            continue
        elapsed = max(0, int((ev.get('ts_ms') - start_ms) / 1000)) if start_ms else 0
        ret = _signed_return(side, entry, price)
        if ret > best_ret:
            best_ret = ret
            best_price = price
        if ret < worst_ret:
            worst_ret = ret
            worst_price = price
        hard = cfg.get('hard_adverse_pct')
        if hard is not None and ret <= -float(hard):
            return {
                'policy': policy,
                'status': 'triggered',
                'reason': f'hard_adverse_{hard}',
                'elapsed_sec': elapsed,
                'exit_price': price,
                'estimated_pnl': round(_pnl_from_return(entry, qty, ret), 2),
                'return_pct': round(ret, 3),
                'mfe_pct': round(max(best_ret, 0), 3),
                'mae_pct': round(abs(min(worst_ret, 0)), 3),
            }
        failed_after = cfg.get('failed_after_sec')
        if failed_after is not None and elapsed >= int(failed_after):
            if max(best_ret, 0) < float(cfg.get('failed_min_mfe_pct', 0.12)) and ret <= 0:
                return {
                    'policy': policy,
                    'status': 'triggered',
                    'reason': f'failed_followthrough_{failed_after}s',
                    'elapsed_sec': elapsed,
                    'exit_price': price,
                    'estimated_pnl': round(_pnl_from_return(entry, qty, ret), 2),
                    'return_pct': round(ret, 3),
                    'mfe_pct': round(max(best_ret, 0), 3),
                    'mae_pct': round(abs(min(worst_ret, 0)), 3),
                }
        trigger = cfg.get('profit_trigger_pct')
        if trigger is not None and best_ret >= float(trigger):
            giveback = 1.0 - (ret / max(best_ret, 0.0001))
            if giveback >= float(cfg.get('profit_giveback_pct', 0.55)):
                return {
                    'policy': policy,
                    'status': 'triggered',
                    'reason': f'profit_giveback_{trigger}',
                    'elapsed_sec': elapsed,
                    'exit_price': price,
                    'estimated_pnl': round(_pnl_from_return(entry, qty, ret), 2),
                    'return_pct': round(ret, 3),
                    'mfe_pct': round(max(best_ret, 0), 3),
                    'mae_pct': round(abs(min(worst_ret, 0)), 3),
                }
    last = cap['events'][-1]
    last_price = _safe_float(last.get('price')) or entry
    ret = _signed_return(side, entry, last_price)
    return {
        'policy': policy,
        'status': 'not_triggered',
        'reason': 'capture_end',
        'elapsed_sec': int((last.get('ts_ms') - start_ms) / 1000) if start_ms else None,
        'exit_price': last_price,
        'estimated_pnl': round(_pnl_from_return(entry, qty, ret), 2),
        'return_pct': round(ret, 3),
        'mfe_pct': round(max(best_ret, 0), 3),
        'mae_pct': round(abs(min(worst_ret, 0)), 3),
    }


def exit_policy_replay(day: str) -> dict:
    pm_path = os.path.join(POSTMORTEM_DIR, f'postmortem_{day}.json')
    trades = []
    if os.path.exists(pm_path):
        pm = _read_json(pm_path)
        trades = [{**t, '_day_iso': day} for t in pm.get('tape', [])]
    captures = load_tick_captures(day)
    rows = []
    policy_totals = {
        name: {
            'policy': name,
            'matched': 0,
            'triggered': 0,
            'estimated_pnl': 0.0,
            'actual_pnl': 0.0,
            'better_trades': 0,
            'worse_trades': 0,
            'flat_trades': 0,
            'improved_losers': 0,
            'improved_loser_delta': 0.0,
            'hurt_losers': 0,
            'hurt_loser_delta': 0.0,
            'improved_winners': 0,
            'improved_winner_delta': 0.0,
            'hurt_winners': 0,
            'hurt_winner_delta': 0.0,
            'examples': {
                'improved_losers': [],
                'hurt_winners': [],
                'worse_trades': [],
            },
        }
        for name in EXIT_POLICIES
    }
    matched = 0
    for trade in trades:
        cap = _match_capture(trade, captures)
        if not cap:
            rows.append({
                'ticker': trade.get('ticker'),
                'side': trade.get('side'),
                'time': trade.get('time'),
                'actual_pnl': trade.get('pnl'),
                'status': 'no_capture',
            })
            continue
        matched += 1
        sims = {}
        actual_pnl = _safe_float(trade.get('pnl')) or 0
        for name, cfg in EXIT_POLICIES.items():
            if name == 'actual_close':
                sim = {
                    'policy': name,
                    'status': 'actual',
                    'reason': trade.get('reason'),
                    'estimated_pnl': round(actual_pnl, 2),
                    'return_pct': None,
                    'mfe_pct': trade.get('mfe_pct'),
                    'mae_pct': trade.get('mae_pct'),
                }
            else:
                sim = _simulate_exit_policy(trade, cap, name, cfg)
            sims[name] = sim
            total = policy_totals[name]
            total['matched'] += 1
            total['actual_pnl'] += actual_pnl
            sim_pnl = float(sim.get('estimated_pnl') or 0)
            total['estimated_pnl'] += sim_pnl
            if sim.get('status') == 'triggered':
                total['triggered'] += 1
            delta = sim_pnl - actual_pnl
            if delta > 0.005:
                total['better_trades'] += 1
            elif delta < -0.005:
                total['worse_trades'] += 1
            else:
                total['flat_trades'] += 1
            example = {
                'ticker': trade.get('ticker'),
                'side': trade.get('side'),
                'time': trade.get('time'),
                'setup_type': trade.get('setup_type'),
                'actual_reason': trade.get('reason'),
                'policy_reason': sim.get('reason'),
                'actual_pnl': round(actual_pnl, 2),
                'sim_pnl': round(sim_pnl, 2),
                'delta': round(delta, 2),
                'elapsed_sec': sim.get('elapsed_sec'),
            }
            if actual_pnl < 0 and delta > 0.005:
                total['improved_losers'] += 1
                total['improved_loser_delta'] += delta
                if len(total['examples']['improved_losers']) < 5:
                    total['examples']['improved_losers'].append(example)
            elif actual_pnl < 0 and delta < -0.005:
                total['hurt_losers'] += 1
                total['hurt_loser_delta'] += delta
                if len(total['examples']['worse_trades']) < 5:
                    total['examples']['worse_trades'].append(example)
            elif actual_pnl > 0 and delta > 0.005:
                total['improved_winners'] += 1
                total['improved_winner_delta'] += delta
            elif actual_pnl > 0 and delta < -0.005:
                total['hurt_winners'] += 1
                total['hurt_winner_delta'] += delta
                if len(total['examples']['hurt_winners']) < 5:
                    total['examples']['hurt_winners'].append(example)
        rows.append({
            'ticker': trade.get('ticker'),
            'side': trade.get('side'),
            'time': trade.get('time'),
            'setup_type': trade.get('setup_type'),
            'actual_reason': trade.get('reason'),
            'actual_pnl': actual_pnl,
            'capture_file': os.path.basename(cap.get('path') or ''),
            'capture_events': cap.get('event_count'),
            'policies': sims,
        })
    scoreboard = []
    for total in policy_totals.values():
        actual = total['actual_pnl']
        est = total['estimated_pnl']
        scoreboard.append({
            'policy': total['policy'],
            'matched': total['matched'],
            'triggered': total['triggered'],
            'estimated_pnl': round(est, 2),
            'actual_pnl': round(actual, 2),
            'estimated_delta_vs_actual': round(est - actual, 2),
            'better_trades': total['better_trades'],
            'worse_trades': total['worse_trades'],
            'flat_trades': total['flat_trades'],
            'improved_losers': total['improved_losers'],
            'improved_loser_delta': round(total['improved_loser_delta'], 2),
            'hurt_losers': total['hurt_losers'],
            'hurt_loser_delta': round(total['hurt_loser_delta'], 2),
            'improved_winners': total['improved_winners'],
            'improved_winner_delta': round(total['improved_winner_delta'], 2),
            'hurt_winners': total['hurt_winners'],
            'hurt_winner_delta': round(total['hurt_winner_delta'], 2),
            'examples': total['examples'],
        })
    scoreboard.sort(key=lambda r: r['estimated_delta_vs_actual'], reverse=True)
    return {
        'day': day,
        'trades': len(trades),
        'captures': len(captures),
        'matched_trades': matched,
        'coverage_pct': round(matched / len(trades) * 100, 1) if trades else None,
        'scoreboard': scoreboard,
        'rows': rows,
        'caveats': [
            'Replay uses captured trade prints, not broker order-book fills.',
            'Captures are limited to the configured post-entry window, so late exits may be approximated at capture end.',
            'This is for exit-policy research only; it does not alter live behavior.',
        ],
    }


def run_validation(day: str, include_replay: bool = True) -> dict:
    rows = load_decision_audit(day)
    payload = {
        'day': day,
        'normalization_quality': normalization_quality(rows),
        'decision_corpus': decision_corpus_summary(rows),
        'loser_diagnostics': loser_diagnostics(rows),
        'gate_scoreboard': gate_scoreboard(rows),
        'next_review_questions': [
            'Do blocked losers share the same pre-entry tag across multiple days?',
            'Are any gates sacrificing too many winners for one avoided loss?',
            'Do skipped signals with strong forward returns point to an overly strict filter?',
            'Are BTC alignment failures concentrated in one ticker or one setup type?',
        ],
    }
    if include_replay:
        try:
            replay = replay_day(day)
            replay.pop('files_detail', None)
            payload['tick_replay'] = replay
        except Exception as e:
            payload['tick_replay_error'] = str(e)
        try:
            er = exit_policy_replay(day)
            rows_detail = er.pop('rows', [])
            er['rows_sample'] = rows_detail[:20]
            payload['exit_policy_replay'] = er
        except Exception as e:
            payload['exit_policy_replay_error'] = str(e)
    return payload


def available_days(end_day: Optional[str] = None) -> list[str]:
    days = []
    if not os.path.isdir(POSTMORTEM_DIR):
        return days
    for name in os.listdir(POSTMORTEM_DIR):
        m = re.match(r'postmortem_(\d{4}-\d{2}-\d{2})\.json$', name)
        if not m:
            continue
        day = m.group(1)
        if end_day is None or day <= end_day:
            days.append(day)
    return sorted(set(days))


def rolling_validation(end_day: str, lookback: int = 5) -> dict:
    days = available_days(end_day)[-lookback:]
    daily = []
    gate_totals: dict[str, dict] = {}
    tag_totals = Counter()
    source_totals = Counter()
    for day in days:
        payload = run_validation(day, include_replay=False)
        daily.append({
            'day': day,
            'decision_corpus': payload.get('decision_corpus'),
            'loser_diagnostics': payload.get('loser_diagnostics'),
            'gate_scoreboard': payload.get('gate_scoreboard'),
        })
        source_totals.update((payload.get('decision_corpus') or {}).get('by_source') or {})
        tag_totals.update((payload.get('loser_diagnostics') or {}).get('tags') or {})
        for row in payload.get('gate_scoreboard') or []:
            name = row.get('variant')
            if not name:
                continue
            acc = gate_totals.setdefault(name, {
                'variant': name,
                'days_seen': 0,
                'days_positive': 0,
                'kept': 0,
                'blocked': 0,
                'kept_wins': 0,
                'kept_losses': 0,
                'kept_pnl': 0.0,
                'blocked_pnl': 0.0,
                'estimated_delta_vs_actual': 0.0,
                'block_reasons': Counter(),
            })
            acc['days_seen'] += 1
            if (row.get('estimated_delta_vs_actual') or 0) > 0:
                acc['days_positive'] += 1
            for key in ('kept', 'blocked', 'kept_wins', 'kept_losses'):
                acc[key] += int(row.get(key) or 0)
            for key in ('kept_pnl', 'blocked_pnl', 'estimated_delta_vs_actual'):
                acc[key] += float(row.get(key) or 0)
            acc['block_reasons'].update(row.get('block_reasons') or {})

    variants = []
    for acc in gate_totals.values():
        kept = acc['kept']
        wins = acc['kept_wins']
        losses = acc['kept_losses']
        variants.append({
            'variant': acc['variant'],
            'days_seen': acc['days_seen'],
            'days_positive': acc['days_positive'],
            'kept': kept,
            'blocked': acc['blocked'],
            'kept_wins': wins,
            'kept_losses': losses,
            'kept_win_rate': round(wins / kept * 100, 1) if kept else None,
            'kept_pnl': round(acc['kept_pnl'], 2),
            'blocked_pnl': round(acc['blocked_pnl'], 2),
            'estimated_delta_vs_actual': round(acc['estimated_delta_vs_actual'], 2),
            'block_reasons': dict(acc['block_reasons']),
        })
    variants.sort(
        key=lambda r: (
            r['estimated_delta_vs_actual'],
            r['days_positive'],
            r['kept_pnl'],
        ),
        reverse=True,
    )
    return {
        'end_day': end_day,
        'lookback': lookback,
        'days': days,
        'decision_rows_by_source': dict(source_totals),
        'loser_tags': dict(tag_totals),
        'gate_scoreboard': variants,
        'daily': daily,
    }


def rolling_exit_policy_replay(end_day: str, lookback: int = 5) -> dict:
    days = available_days(end_day)[-lookback:]
    daily = []
    policy_totals: dict[str, dict] = {}
    for day in days:
        payload = exit_policy_replay(day)
        compact = {
            'day': day,
            'trades': payload.get('trades'),
            'captures': payload.get('captures'),
            'matched_trades': payload.get('matched_trades'),
            'coverage_pct': payload.get('coverage_pct'),
            'scoreboard': payload.get('scoreboard') or [],
        }
        daily.append(compact)
        if not payload.get('matched_trades'):
            continue
        for row in payload.get('scoreboard') or []:
            name = row.get('policy')
            if not name:
                continue
            acc = policy_totals.setdefault(name, {
                'policy': name,
                'days_seen': 0,
                'days_positive': 0,
                'matched': 0,
                'triggered': 0,
                'estimated_pnl': 0.0,
                'actual_pnl': 0.0,
                'estimated_delta_vs_actual': 0.0,
                'better_trades': 0,
                'worse_trades': 0,
                'flat_trades': 0,
                'improved_losers': 0,
                'improved_loser_delta': 0.0,
                'hurt_losers': 0,
                'hurt_loser_delta': 0.0,
                'improved_winners': 0,
                'improved_winner_delta': 0.0,
                'hurt_winners': 0,
                'hurt_winner_delta': 0.0,
                'examples': {
                    'improved_losers': [],
                    'hurt_winners': [],
                    'worse_trades': [],
                },
            })
            acc['days_seen'] += 1
            if float(row.get('estimated_delta_vs_actual') or 0) > 0:
                acc['days_positive'] += 1
            for key in ('matched', 'triggered', 'better_trades', 'worse_trades', 'flat_trades',
                        'improved_losers', 'hurt_losers', 'improved_winners', 'hurt_winners'):
                acc[key] += int(row.get(key) or 0)
            for key in ('estimated_pnl', 'actual_pnl', 'estimated_delta_vs_actual',
                        'improved_loser_delta', 'hurt_loser_delta',
                        'improved_winner_delta', 'hurt_winner_delta'):
                acc[key] += float(row.get(key) or 0)
            examples = row.get('examples') or {}
            for bucket in ('improved_losers', 'hurt_winners', 'worse_trades'):
                room = max(0, 8 - len(acc['examples'][bucket]))
                if room:
                    acc['examples'][bucket].extend((examples.get(bucket) or [])[:room])

    scoreboard = []
    for acc in policy_totals.values():
        scoreboard.append({
            'policy': acc['policy'],
            'days_seen': acc['days_seen'],
            'days_positive': acc['days_positive'],
            'matched': acc['matched'],
            'triggered': acc['triggered'],
            'estimated_pnl': round(acc['estimated_pnl'], 2),
            'actual_pnl': round(acc['actual_pnl'], 2),
            'estimated_delta_vs_actual': round(acc['estimated_delta_vs_actual'], 2),
            'better_trades': acc['better_trades'],
            'worse_trades': acc['worse_trades'],
            'flat_trades': acc['flat_trades'],
            'improved_losers': acc['improved_losers'],
            'improved_loser_delta': round(acc['improved_loser_delta'], 2),
            'hurt_losers': acc['hurt_losers'],
            'hurt_loser_delta': round(acc['hurt_loser_delta'], 2),
            'improved_winners': acc['improved_winners'],
            'improved_winner_delta': round(acc['improved_winner_delta'], 2),
            'hurt_winners': acc['hurt_winners'],
            'hurt_winner_delta': round(acc['hurt_winner_delta'], 2),
            'examples': acc['examples'],
        })
    scoreboard.sort(
        key=lambda r: (
            r['estimated_delta_vs_actual'],
            r['days_positive'],
            r['improved_loser_delta'],
            -abs(r['hurt_winner_delta']),
        ),
        reverse=True,
    )
    return {
        'end_day': end_day,
        'lookback': lookback,
        'days': days,
        'scoreboard': scoreboard,
        'daily': daily,
        'promotion_rules': [
            'Candidate exits require at least 2 replay days, 2 positive days, 10 matched trades, and positive net delta.',
            'Winner damage is reviewed explicitly; a policy that saves losers but repeatedly cuts good winners stays shadow-only.',
            'Replay is evidence for human review, not automatic production config.',
        ],
    }


def candidate_config(day: str, lookback: int = 5) -> dict:
    rolling = rolling_validation(day, lookback=lookback)
    candidates = []
    watch = []
    for row in rolling.get('gate_scoreboard') or []:
        variant = row.get('variant')
        if variant == 'current_observed':
            continue
        delta = float(row.get('estimated_delta_vs_actual') or 0)
        blocked = int(row.get('blocked') or 0)
        days_seen = int(row.get('days_seen') or 0)
        days_pos = int(row.get('days_positive') or 0)
        if days_seen >= 3 and days_pos >= 2 and blocked >= 3 and delta > 0:
            candidates.append({
                'type': 'engine_gate_candidate',
                'variant': variant,
                'evidence': row,
                'recommendation': 'Promote to human review; do not auto-apply.',
            })
        elif blocked or delta:
            watch.append({
                'type': 'watch_gate_variant',
                'variant': variant,
                'evidence': row,
                'why_not_promoted': (
                    'Needs at least 3 days, 2 positive days, 3 blocked trades, and positive net delta.'
                ),
            })
    return {
        'day': day,
        'lookback': lookback,
        'candidates': candidates,
        'watch': watch[:10],
        'rolling_validation': {
            'days': rolling.get('days'),
            'loser_tags': rolling.get('loser_tags'),
            'top_gate_variants': (rolling.get('gate_scoreboard') or [])[:8],
        },
    }


def _compact_exit_policy_evidence(row: dict, example_limit: int = 2) -> dict:
    examples = row.get('examples') or {}
    return {
        'policy': row.get('policy'),
        'days_seen': row.get('days_seen'),
        'days_positive': row.get('days_positive'),
        'matched': row.get('matched'),
        'triggered': row.get('triggered'),
        'estimated_pnl': row.get('estimated_pnl'),
        'actual_pnl': row.get('actual_pnl'),
        'estimated_delta_vs_actual': row.get('estimated_delta_vs_actual'),
        'better_trades': row.get('better_trades'),
        'worse_trades': row.get('worse_trades'),
        'improved_losers': row.get('improved_losers'),
        'improved_loser_delta': row.get('improved_loser_delta'),
        'hurt_winners': row.get('hurt_winners'),
        'hurt_winner_delta': row.get('hurt_winner_delta'),
        'examples': {
            'improved_losers': (examples.get('improved_losers') or [])[:example_limit],
            'hurt_winners': (examples.get('hurt_winners') or [])[:example_limit],
        },
    }


def exit_policy_candidate_config(day: str, lookback: int = 5) -> dict:
    rolling = rolling_exit_policy_replay(day, lookback=lookback)
    candidates = []
    watch = []
    for row in rolling.get('scoreboard') or []:
        policy = row.get('policy')
        if policy == 'actual_close':
            continue
        delta = float(row.get('estimated_delta_vs_actual') or 0)
        matched = int(row.get('matched') or 0)
        days_seen = int(row.get('days_seen') or 0)
        days_pos = int(row.get('days_positive') or 0)
        hurt_winner_delta = float(row.get('hurt_winner_delta') or 0)
        base = {
            'type': 'exit_policy_candidate',
            'policy': policy,
            'evidence': _compact_exit_policy_evidence(row),
            'staged_config': (EXIT_POLICY_CANDIDATE_MAP.get(policy) or {}).get('staged_config'),
            'live_behavior_change': (EXIT_POLICY_CANDIDATE_MAP.get(policy) or {}).get('live_behavior_change'),
        }
        if (policy in EXIT_POLICY_CANDIDATE_MAP
                and days_seen >= 2
                and days_pos >= 2
                and matched >= 10
                and delta > 0):
            candidates.append({
                **base,
                'recommendation': (
                    'Strong candidate for human review. Keep live config unchanged until the operator explicitly approves.'
                ),
                'shadow_logging': 'Already eligible for live shadow logging if configured.',
                'risk_note': (
                    'Review hurt_winners/hurt_winner_delta before promotion; positive net delta alone is not enough.'
                    if hurt_winner_delta < 0 else
                    'No material winner-damage warning in the replay window.'
                ),
            })
        elif delta or matched:
            watch.append({
                **base,
                'why_not_promoted': (
                    'Needs known policy mapping, at least 2 replay days, 2 positive days, '
                    '10 matched trades, and positive net delta.'
                ),
            })
    return {
        'day': day,
        'lookback': lookback,
        'candidates': candidates,
        'watch': watch[:10],
        'rolling_exit_policy_replay': {
            'days': rolling.get('days'),
            'top_exit_policies': [
                _compact_exit_policy_evidence(row, example_limit=0)
                for row in (rolling.get('scoreboard') or [])[:8]
            ],
            'promotion_rules': rolling.get('promotion_rules'),
        },
    }


def write_rolling_validation(end_day: str, lookback: int = 5) -> tuple[str, dict]:
    payload = rolling_validation(end_day, lookback=lookback)
    path = os.path.join(POSTMORTEM_DIR, f'engine_validation_rolling_{end_day}_{lookback}d.json')
    os.makedirs(POSTMORTEM_DIR, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, default=str)
    return path, payload


def write_rolling_exit_policy_replay(end_day: str, lookback: int = 5) -> tuple[str, dict]:
    payload = rolling_exit_policy_replay(end_day, lookback=lookback)
    path = os.path.join(POSTMORTEM_DIR, f'exit_policy_replay_rolling_{end_day}_{lookback}d.json')
    os.makedirs(POSTMORTEM_DIR, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, default=str)
    return path, payload


def write_exit_policy_candidate_config(day: str, lookback: int = 5) -> tuple[str, dict]:
    payload = exit_policy_candidate_config(day, lookback=lookback)
    path = os.path.join(POSTMORTEM_DIR, f'exit_policy_candidate_config_{day}.json')
    os.makedirs(POSTMORTEM_DIR, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, default=str)
    return path, payload


def write_candidate_config(day: str, lookback: int = 5) -> tuple[str, dict]:
    payload = candidate_config(day, lookback=lookback)
    path = os.path.join(POSTMORTEM_DIR, f'engine_candidate_config_{day}.json')
    os.makedirs(POSTMORTEM_DIR, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, default=str)
    return path, payload


def write_exit_policy_replay(day: str) -> tuple[str, dict]:
    payload = exit_policy_replay(day)
    path = os.path.join(POSTMORTEM_DIR, f'exit_policy_replay_{day}.json')
    os.makedirs(POSTMORTEM_DIR, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, default=str)
    return path, payload


def weekend_packet(day: str, lookback: int = 5) -> dict:
    daily = run_validation(day, include_replay=True)
    rolling = rolling_validation(day, lookback=lookback)
    candidates = candidate_config(day, lookback=lookback)
    exit_replay = exit_policy_replay(day)
    exit_rows = exit_replay.pop('rows', [])
    rolling_exit_replay = rolling_exit_policy_replay(day, lookback=lookback)
    exit_candidates = exit_policy_candidate_config(day, lookback=lookback)
    gate_watch = []
    for row in candidates.get('watch') or []:
        ev = row.get('evidence') or {}
        if ev.get('estimated_delta_vs_actual') is not None:
            gate_watch.append({
                'variant': row.get('variant'),
                'days_positive': ev.get('days_positive'),
                'days_seen': ev.get('days_seen'),
                'blocked': ev.get('blocked'),
                'estimated_delta_vs_actual': ev.get('estimated_delta_vs_actual'),
                'why_not_promoted': row.get('why_not_promoted'),
            })
    return {
        'day': day,
        'lookback': lookback,
        'files_expected': {
            'daily_validation': f'engine_validation_{day}.json',
            'decision_audit': f'decision_audit_{day}.csv',
            'rolling_validation': f'engine_validation_rolling_{day}_{lookback}d.json',
            'candidate_config': f'engine_candidate_config_{day}.json',
            'exit_policy_replay': f'exit_policy_replay_{day}.json',
            'rolling_exit_policy_replay': f'exit_policy_replay_rolling_{day}_{lookback}d.json',
            'exit_policy_candidate_config': f'exit_policy_candidate_config_{day}.json',
        },
        'executive_summary': {
            'decision_rows': (daily.get('decision_corpus') or {}).get('rows'),
            'actual': (daily.get('decision_corpus') or {}).get('actual'),
            'normalization_quality': daily.get('normalization_quality'),
            'top_loser_tags': (daily.get('loser_diagnostics') or {}).get('tags'),
            'top_rolling_gates': (rolling.get('gate_scoreboard') or [])[:5],
            'candidate_count': len(candidates.get('candidates') or []),
            'exit_candidate_count': len(exit_candidates.get('candidates') or []),
            'exit_replay_scoreboard': exit_replay.get('scoreboard')[:6],
            'rolling_exit_policy_scoreboard': (rolling_exit_replay.get('scoreboard') or [])[:6],
        },
        'do_not_change_reasons': [
            row for row in gate_watch
            if (row.get('days_positive') or 0) < 2
            or (row.get('estimated_delta_vs_actual') or 0) <= 0
        ][:10],
        'candidate_config': candidates,
        'rolling_validation': rolling,
        'daily_validation': daily,
        'exit_policy_replay': {**exit_replay, 'rows_sample': exit_rows[:25]},
        'rolling_exit_policy_replay': rolling_exit_replay,
        'exit_policy_candidate_config': exit_candidates,
    }


def write_weekend_packet(day: str, lookback: int = 5) -> tuple[str, dict]:
    # Materialize all component artifacts first so the packet is a real review bundle.
    write_validation(day, include_replay=True)
    write_rolling_validation(day, lookback=lookback)
    write_candidate_config(day, lookback=lookback)
    write_exit_policy_replay(day)
    write_rolling_exit_policy_replay(day, lookback=lookback)
    write_exit_policy_candidate_config(day, lookback=lookback)
    payload = weekend_packet(day, lookback=lookback)
    path = os.path.join(POSTMORTEM_DIR, f'weekend_validation_packet_{day}.json')
    os.makedirs(POSTMORTEM_DIR, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, default=str)
    return path, payload


def write_audit_csv(day: str, rows: list[dict]) -> str:
    path = os.path.join(POSTMORTEM_DIR, f'decision_audit_{day}.csv')
    os.makedirs(POSTMORTEM_DIR, exist_ok=True)
    fields = [
        'ts', 'ct', 'source', 'decision', 'ticker', 'side', 'setup_type',
        'conviction', 'price', 'score', 'abs_score', 'reason', 'result',
        'pnl', 'duration_min', 'exit_reason', 'stock_mom_15s',
        'stock_mom_60s', 'stock_ema_stack', 'vwap_dist', 'vwap_dist_sigma',
        'flow_30s_buy_pct', 'flow_30s_delta', 'flow_120s_buy_pct',
        'vol_z_60s', 'tick_z_30s', 'btc_regime', 'btc_stack',
        'btc_mom_15s', 'btc_mom_60s', 'btc_stale', 'stock_minus_btc_60s',
        'execution_score', 'spread_pct', 'quote_age_sec', 'near_vwap_chop',
        'btc_conflict', 'flow_fade_confirmed', 'score_components', 'reasons',
    ]
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)
    return path


def write_validation(day: str, include_replay: bool = True) -> tuple[str, str, dict]:
    rows = load_decision_audit(day)
    csv_path = write_audit_csv(day, rows)
    payload = run_validation(day, include_replay=include_replay)
    json_path = os.path.join(POSTMORTEM_DIR, f'engine_validation_{day}.json')
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, default=str)
    return json_path, csv_path, payload


def main() -> int:
    ap = argparse.ArgumentParser(description='Build daily engine validation and decision-audit artifacts.')
    ap.add_argument('day', nargs='?')
    ap.add_argument('--write', action='store_true')
    ap.add_argument('--no-replay', action='store_true')
    ap.add_argument('--json', action='store_true')
    ap.add_argument('--rolling', type=int)
    ap.add_argument('--candidate-config', action='store_true')
    ap.add_argument('--exit-replay', action='store_true')
    ap.add_argument('--rolling-exit-replay', type=int)
    ap.add_argument('--exit-candidate-config', action='store_true')
    ap.add_argument('--weekend-packet', action='store_true')
    ap.add_argument('--lookback', type=int, default=5)
    args = ap.parse_args()
    if not args.day:
        ap.error('day is required')

    if args.rolling:
        if args.write:
            path, payload = write_rolling_validation(args.day, lookback=args.rolling)
            print(path)
        else:
            payload = rolling_validation(args.day, lookback=args.rolling)
        if args.json or args.write:
            print(json.dumps(payload, indent=2, default=str))
        else:
            print(f"Rolling engine validation through {args.day} ({args.rolling} day lookback)")
            print(f"Days: {', '.join(payload.get('days') or []) or 'none'}")
            print(f"Loser tags: {payload.get('loser_tags')}")
            for row in (payload.get('gate_scoreboard') or [])[:6]:
                print(f"  {row['variant']}: days+={row['days_positive']}/{row['days_seen']} "
                      f"blocked={row['blocked']} delta=${row['estimated_delta_vs_actual']:+.2f} "
                      f"kept_pnl=${row['kept_pnl']:+.2f}")
        return 0

    if args.rolling_exit_replay:
        if args.write:
            path, payload = write_rolling_exit_policy_replay(args.day, lookback=args.rolling_exit_replay)
            print(path)
        else:
            payload = rolling_exit_policy_replay(args.day, lookback=args.rolling_exit_replay)
        if args.json or args.write:
            print(json.dumps(payload, indent=2, default=str))
        else:
            print(f"Rolling exit-policy replay through {args.day} "
                  f"({args.rolling_exit_replay} day lookback)")
            print(f"Days: {', '.join(payload.get('days') or []) or 'none'}")
            for row in (payload.get('scoreboard') or [])[:6]:
                print(f"  {row['policy']}: +days={row['days_positive']}/{row['days_seen']} "
                      f"matched={row['matched']} triggered={row['triggered']} "
                      f"delta=${row['estimated_delta_vs_actual']:+.2f} "
                      f"hurt_winners=${row['hurt_winner_delta']:+.2f}")
        return 0

    if args.candidate_config:
        if args.write:
            path, payload = write_candidate_config(args.day, lookback=args.lookback)
            print(path)
        else:
            payload = candidate_config(args.day, lookback=args.lookback)
        print(json.dumps(payload, indent=2, default=str))
        return 0

    if args.exit_candidate_config:
        if args.write:
            path, payload = write_exit_policy_candidate_config(args.day, lookback=args.lookback)
            print(path)
        else:
            payload = exit_policy_candidate_config(args.day, lookback=args.lookback)
        print(json.dumps(payload, indent=2, default=str))
        return 0

    if args.exit_replay:
        if args.write:
            path, payload = write_exit_policy_replay(args.day)
            print(path)
        else:
            payload = exit_policy_replay(args.day)
        if args.json or args.write:
            print(json.dumps(payload, indent=2, default=str))
        else:
            print(f"Exit policy replay {args.day}: "
                  f"matched={payload['matched_trades']}/{payload['trades']} "
                  f"coverage={payload['coverage_pct']}%")
            for row in payload.get('scoreboard') or []:
                print(f"  {row['policy']}: triggered={row['triggered']} "
                      f"est=${row['estimated_pnl']:+.2f} "
                      f"delta=${row['estimated_delta_vs_actual']:+.2f}")
        return 0

    if args.weekend_packet:
        if args.write:
            path, payload = write_weekend_packet(args.day, lookback=args.lookback)
            print(path)
        else:
            payload = weekend_packet(args.day, lookback=args.lookback)
        if args.json or args.write:
            print(json.dumps(payload, indent=2, default=str))
        else:
            summary = payload.get('executive_summary') or {}
            actual = summary.get('actual') or {}
            print(f"Weekend validation packet {args.day}: "
                  f"trades={actual.get('trades')} pnl=${actual.get('pnl'):+.2f}")
            print(f"Candidate gates: {summary.get('candidate_count')}")
            print(f"Top loser tags: {summary.get('top_loser_tags')}")
        return 0

    if args.write:
        json_path, csv_path, payload = write_validation(args.day, include_replay=not args.no_replay)
        print(json_path)
        print(csv_path)
    else:
        payload = run_validation(args.day, include_replay=not args.no_replay)

    if args.json or args.write:
        print(json.dumps(payload, indent=2, default=str))
    else:
        corpus = payload['decision_corpus']
        actual = corpus['actual']
        print(f"Engine validation {args.day}")
        print(f"Rows={corpus['rows']} trades={actual['trades']} "
              f"{actual['wins']}W/{actual['losses']}L pnl=${actual['pnl']:+.2f}")
        print('Top gate variants:')
        for row in payload['gate_scoreboard'][:5]:
            print(f"  {row['variant']}: kept={row['kept']} blocked={row['blocked']} "
                  f"kept_pnl=${row['kept_pnl']:+.2f} delta=${row['estimated_delta_vs_actual']:+.2f}")
        losers = payload['loser_diagnostics']
        print(f"Losers={losers['losers']} tags={losers['tags']}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
