from __future__ import annotations

import json
import os
from collections import defaultdict
from datetime import datetime
from typing import Iterable, Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore


HERE = os.path.dirname(os.path.abspath(__file__))
POSTMORTEM_DIR = os.path.join(HERE, 'postmortem')
CT = ZoneInfo('America/Chicago')


def _num(v, default=0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _bucket_exec_quality(row: dict) -> str:
    f = row.get('forensics') or {}
    eq = f.get('execution_quality') or {}
    score = eq.get('score')
    if score is None:
        spread = (f.get('entry_quality') or {}).get('spread_pct')
        if spread is None:
            label = (row.get('decision_quality') or {}).get('label')
            if label == 'bad_execution_context':
                return 'exec_poor'
            slip = row.get('entry_slippage_pct')
            if slip is not None and _num(slip) < -0.05:
                return 'exec_poor'
            return 'exec_unknown'
        if spread <= 0.04:
            return 'exec_clean'
        if spread <= 0.08:
            return 'exec_ok'
        return 'exec_poor'
    score = _num(score)
    if score >= 80:
        return 'exec_clean'
    if score >= 65:
        return 'exec_ok'
    if score >= 50:
        return 'exec_marginal'
    return 'exec_poor'


def _bucket_btc(row: dict) -> str:
    f = row.get('forensics') or {}
    btc = f.get('btc') or {}
    row_btc = row.get('btc') or {}
    regime = btc.get('regime') or row.get('btc_regime') or row_btc.get('regime')
    detail = btc.get('regime_detail')
    if detail and detail not in ('unknown', 'neutral'):
        return str(detail)
    stack = btc.get('stack') or row_btc.get('ema_stack') or row_btc.get('stack')
    mom60 = btc.get('mom_60s') or row_btc.get('mom_60s') or row_btc.get('mom_60')
    if regime in (None, 'unknown', 'neutral') and stack:
        try:
            mom = float(mom60 or 0)
        except Exception:
            mom = 0
        if stack == 'bull' and mom > 0:
            return 'btc_bull_confirm'
        if stack == 'bear' and mom < 0:
            return 'btc_bear_confirm'
        return f'btc_{stack}'
    return str(regime or 'btc_unknown')


def _setup(row: dict) -> str:
    f = row.get('forensics') or {}
    return str(f.get('setup_type') or row.get('setup') or row.get('setup_type') or 'unknown')


def _opening_state(row: dict) -> str:
    f = row.get('forensics') or {}
    loc = f.get('location') or {}
    ind = row.get('ind') or {}
    return str(
        loc.get('opening_15m_break_state')
        or loc.get('opening_5m_break_state')
        or row.get('opening_state')
        or ind.get('opening_15m_break_state')
        or ind.get('opening_5m_break_state')
        or 'opening_unknown'
    )


def _session_phase(row: dict) -> str:
    f = row.get('forensics') or {}
    if f.get('session_phase'):
        return str(f.get('session_phase'))
    time_s = row.get('time')
    if isinstance(time_s, str) and len(time_s) >= 5:
        try:
            hh, mm = [int(x) for x in time_s[:5].split(':')]
            minutes = hh * 60 + mm
            if minutes < 9 * 60:
                return 'opening_drive'
            if minutes < 10 * 60 + 30:
                return 'morning'
            if minutes < 13 * 60 + 30:
                return 'midday'
            return 'power_hour'
        except Exception:
            pass
    return 'phase_unknown'


def _ev_key(row: dict) -> str:
    return '|'.join([
        str(row.get('ticker') or '?'),
        str(row.get('side') or '?'),
        _setup(row),
        _bucket_btc(row),
        _opening_state(row),
        _bucket_exec_quality(row),
        _session_phase(row),
    ])


def _compact_trade(row: dict, day: Optional[str] = None) -> dict:
    return {
        'day': day,
        'time': row.get('time'),
        'ticker': row.get('ticker'),
        'side': row.get('side'),
        'setup': _setup(row),
        'pnl': round(_num(row.get('pnl')), 2),
        'mfe_pct': row.get('mfe_pct'),
        'mae_pct': row.get('mae_pct'),
        'reason': row.get('reason'),
    }


def _add_trade(groups: dict, row: dict, day: Optional[str]):
    key = _ev_key(row)
    g = groups.setdefault(key, {
        'key': key,
        'ticker': row.get('ticker'),
        'side': row.get('side'),
        'setup': _setup(row),
        'btc_bucket': _bucket_btc(row),
        'opening_state': _opening_state(row),
        'execution_bucket': _bucket_exec_quality(row),
        'session_phase': _session_phase(row),
        'days': set(),
        'trades': 0,
        'wins': 0,
        'losses': 0,
        'pnl': 0.0,
        'gross_win': 0.0,
        'gross_loss': 0.0,
        'mfe': [],
        'mae': [],
        'examples': [],
    })
    pnl = _num(row.get('pnl'))
    if day:
        g['days'].add(day)
    g['trades'] += 1
    g['wins'] += 1 if pnl > 0 else 0
    g['losses'] += 1 if pnl < 0 else 0
    g['pnl'] += pnl
    if pnl > 0:
        g['gross_win'] += pnl
    elif pnl < 0:
        g['gross_loss'] += abs(pnl)
    if row.get('mfe_pct') is not None:
        g['mfe'].append(_num(row.get('mfe_pct')))
    if row.get('mae_pct') is not None:
        g['mae'].append(_num(row.get('mae_pct')))
    if len(g['examples']) < 5:
        g['examples'].append(_compact_trade(row, day))


def _rank(row: dict) -> str:
    if row['trades'] < 3 or row['evidence_days'] < 2:
        return 'collect_more'
    if row['pnl'] > 0 and row['win_rate'] >= 65 and row['expectancy'] > 0:
        return 'size_candidate'
    if row['pnl'] < 0 and row['win_rate'] < 45:
        return 'avoid_candidate'
    if row['profit_factor'] is not None and row['profit_factor'] < 0.8:
        return 'watch_negative'
    return 'neutral'


def _finalize(groups: dict) -> list[dict]:
    rows = []
    for g in groups.values():
        trades = g['trades']
        gross_loss = g['gross_loss']
        profit_factor = (g['gross_win'] / gross_loss) if gross_loss else None
        row = {
            'key': g['key'],
            'ticker': g['ticker'],
            'side': g['side'],
            'setup': g['setup'],
            'btc_bucket': g['btc_bucket'],
            'opening_state': g['opening_state'],
            'execution_bucket': g['execution_bucket'],
            'session_phase': g['session_phase'],
            'days': sorted(g['days']),
            'evidence_days': len(g['days']),
            'trades': trades,
            'wins': g['wins'],
            'losses': g['losses'],
            'win_rate': round(g['wins'] / trades * 100, 1) if trades else 0,
            'pnl': round(g['pnl'], 2),
            'expectancy': round(g['pnl'] / trades, 2) if trades else 0,
            'gross_win': round(g['gross_win'], 2),
            'gross_loss': round(gross_loss, 2),
            'profit_factor': round(profit_factor, 2) if profit_factor is not None else None,
            'avg_mfe_pct': round(sum(g['mfe']) / len(g['mfe']), 3) if g['mfe'] else None,
            'avg_mae_pct': round(sum(g['mae']) / len(g['mae']), 3) if g['mae'] else None,
            'examples': g['examples'],
        }
        row['verdict'] = _rank(row)
        rows.append(row)
    return sorted(
        rows,
        key=lambda r: (
            {'avoid_candidate': 0, 'watch_negative': 1, 'size_candidate': 2,
             'neutral': 3, 'collect_more': 4}.get(r['verdict'], 9),
            r['pnl'],
        )
    )


def _postmortem_days(before_or_equal: str, max_days: int) -> list[str]:
    if not os.path.isdir(POSTMORTEM_DIR):
        return []
    days = []
    for name in sorted(os.listdir(POSTMORTEM_DIR), reverse=True):
        if not (name.startswith('postmortem_') and name.endswith('.json')):
            continue
        day = name[len('postmortem_'):-len('.json')]
        if len(day) == 10 and day <= before_or_equal:
            days.append(day)
        if len(days) >= max_days:
            break
    return sorted(days)


def _load_tape(day: str) -> list[dict]:
    path = os.path.join(POSTMORTEM_DIR, f'postmortem_{day}.json')
    try:
        with open(path, 'r', encoding='utf-8') as f:
            payload = json.load(f)
        return payload.get('tape') or []
    except Exception:
        return []


def build_ev_table(day: str, current_tape: Optional[Iterable[dict]] = None,
                   lookback_days: int = 10) -> dict:
    groups = {}
    included_days = _postmortem_days(day, lookback_days)
    for d in included_days:
        for row in _load_tape(d):
            _add_trade(groups, row, d)
    if current_tape is not None and day not in included_days:
        included_days.append(day)
        for row in current_tape:
            _add_trade(groups, row, day)
    elif current_tape is not None and not _load_tape(day):
        for row in current_tape:
            _add_trade(groups, row, day)
    rows = _finalize(groups)
    return {
        'date_iso': day,
        'generated_at': datetime.now(CT).isoformat(timespec='seconds'),
        'lookback_days': lookback_days,
        'included_days': sorted(set(included_days)),
        'grain': [
            'ticker', 'side', 'setup', 'btc_bucket', 'opening_state',
            'execution_bucket', 'session_phase',
        ],
        'summary': {
            'buckets': len(rows),
            'trades': sum(r['trades'] for r in rows),
            'avoid_candidates': sum(1 for r in rows if r['verdict'] == 'avoid_candidate'),
            'size_candidates': sum(1 for r in rows if r['verdict'] == 'size_candidate'),
            'collect_more': sum(1 for r in rows if r['verdict'] == 'collect_more'),
        },
        'rows': rows,
        'notes': [
            'Passive analytics only; do not change live behavior from this table alone.',
            'Buckets need repeated days and at least 3 trades before size/avoid labels matter.',
        ],
    }


def write_ev_table(day: str, current_tape: Optional[Iterable[dict]] = None,
                   lookback_days: int = 10) -> tuple[str, dict]:
    payload = build_ev_table(day, current_tape=current_tape, lookback_days=lookback_days)
    os.makedirs(POSTMORTEM_DIR, exist_ok=True)
    path = os.path.join(POSTMORTEM_DIR, f'ev_table_{day}.json')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, default=str)
    return path, payload


if __name__ == '__main__':
    import argparse

    ap = argparse.ArgumentParser(description='Build passive EV bucket table.')
    ap.add_argument('day')
    ap.add_argument('--write', action='store_true')
    ap.add_argument('--lookback-days', type=int, default=10)
    args = ap.parse_args()
    if args.write:
        path, payload = write_ev_table(args.day, lookback_days=args.lookback_days)
        print(path)
    else:
        payload = build_ev_table(args.day, lookback_days=args.lookback_days)
    print(json.dumps(payload, indent=2, default=str))
