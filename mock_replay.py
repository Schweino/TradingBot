"""Mock-parity replay for Step 3 validation.

This replaces the old synthetic full replay as the promotion-grade Step 3
surface. The first responsibility is parity: when actual mock trade corpora
exist for a day, replay those executed opportunities with the same exits and
fills instead of inventing a cleaner historical trading path.
"""
from __future__ import annotations

from output_paths import output_path

import argparse
import csv
import json
import os
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import scoring_profiles


HERE = os.path.dirname(os.path.abspath(__file__))
CT = ZoneInfo('America/Chicago')
DEFAULT_OUT_DIR = output_path('postmortem', 'backtests', 'mock_replay')
TRADE_DIR = output_path('postmortem', 'trades')


def _parse_day(value: str) -> date:
    return datetime.strptime(value, '%Y-%m-%d').date()


def _market_days(start: date, end: date) -> list[date]:
    days = []
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            days.append(cur)
        cur += timedelta(days=1)
    return days


def _read_jsonl(path: str) -> list[dict]:
    rows = []
    if not os.path.exists(path):
        return rows
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    return rows


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ''):
            return default
        return float(value)
    except Exception:
        return default


def _iso_ct(ts: Any) -> str:
    try:
        return datetime.fromtimestamp(float(ts), timezone.utc).astimezone(CT).strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return ''


def _profile_name(path: str | None) -> str:
    if not path:
        return 'mock_live_actual'
    return os.path.splitext(os.path.basename(path))[0]


def _signal_for_profile(row: dict) -> tuple[dict, dict, dict]:
    fx = row.get('forensics') or {}
    sig = dict(fx.get('signal') or {})
    sig.setdefault('ticker', row.get('ticker'))
    sig.setdefault('side', row.get('side'))
    sig.setdefault('score', sig.get('score', row.get('score')))
    sig.setdefault('conviction', row.get('conviction'))
    sig.setdefault('setup_type', fx.get('setup_type') or row.get('setup_type'))
    sig.setdefault('reasons', list(sig.get('reasons') or row.get('reasons') or []))
    sig.setdefault('btc_context', fx.get('btc') or row.get('btc') or {})
    sig.setdefault('miner_basket', fx.get('miner_basket') or {})
    sig.setdefault('relative_strength', fx.get('relative_strength') or {})
    sig.setdefault('signal_quality', fx.get('signal_quality') or {})
    sig.setdefault('execution_quality', fx.get('execution_quality') or {})
    sig.setdefault('session_phase', fx.get('session_phase'))

    loc = fx.get('location') or {}
    ind = {
        'price': sig.get('price') or row.get('entry'),
        'ema_stack': _extract_ema_stack(row),
        'mom_5s': _path_num(row, ['forensics', 'btc', 'lead_lag', '5s', 'stock_mom'], None),
        'mom_15s': _path_num(row, ['forensics', 'btc', 'lead_lag', '15s', 'stock_mom'], None),
        'mom_30s': _path_num(row, ['forensics', 'btc', 'lead_lag', '30s', 'stock_mom'], None),
        'mom_60s': _path_num(row, ['forensics', 'relative_strength', 'stock_mom_60s'], None),
        'vwap_dist_sigma': loc.get('vwap_dist_sigma'),
        'session_phase': fx.get('session_phase'),
        'flow_10s': {},
        'flow_30s': {},
    }
    btc = fx.get('btc') or {}
    return sig, ind, btc


def _path_num(row: dict, path: list[str], default: Any = 0.0) -> Any:
    cur: Any = row
    for key in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
    return _num(cur, default) if cur is not None else default


def _extract_ema_stack(row: dict) -> str | None:
    fx = row.get('forensics') or {}
    shadow = ((fx.get('signal_quality') or {}).get('score_model') or {}).get('dual_side_shadow') or {}
    chosen = str(shadow.get('chosen_side_by_shadow') or row.get('side') or '').lower()
    if chosen == 'long':
        comps = ((shadow.get('long') or {}).get('components') or {})
    else:
        comps = ((shadow.get('short') or {}).get('components') or {})
    ema = _num(comps.get('ema_stack'), None)
    if ema is not None:
        return 'bull' if ema > 0 else ('bear' if ema < 0 else 'mixed')
    text = ' '.join(str(x) for x in (row.get('reasons') or []))
    if 'bull stack' in text:
        return 'bull'
    if 'bear stack' in text:
        return 'bear'
    return None


def _row_pnl_pct(row: dict) -> float:
    entry = _num(row.get('entry'))
    exit_px = _num(row.get('exit'))
    if entry <= 0 or exit_px <= 0:
        alloc = _num(row.get('alloc'))
        return (_num(row.get('pnl')) / alloc * 100.0) if alloc else 0.0
    side = str(row.get('side') or '').upper()
    raw = (exit_px - entry) / entry * 100.0
    return raw if side == 'LONG' else -raw


def _trade_size_pct(row: dict) -> float:
    pct = _num(row.get('trade_size_pct'), None)
    return pct if pct is not None and pct > 0 else 0.25


def _effective_pnl(row: dict, chosen_side: str, start_balance: float | None) -> tuple[float, float, float]:
    actual_side = str(row.get('side') or '').upper()
    pnl_pct = _row_pnl_pct(row)
    if chosen_side in ('LONG', 'SHORT') and chosen_side != actual_side:
        pnl_pct = -pnl_pct
    if start_balance is None:
        pnl = _num(row.get('pnl'))
        if chosen_side in ('LONG', 'SHORT') and chosen_side != actual_side:
            pnl = -pnl
        alloc = _num(row.get('alloc'))
    else:
        alloc = float(start_balance) * _trade_size_pct(row)
        pnl = alloc * pnl_pct / 100.0
    return round(pnl, 6), round(pnl_pct, 6), round(alloc, 6)


def _load_rows(days: list[date], tickers: set[str] | None) -> tuple[list[dict], list[str]]:
    rows: list[dict] = []
    missing: list[str] = []
    for day in days:
        path = os.path.join(TRADE_DIR, f'trades_{day.isoformat()}.jsonl')
        day_rows = _read_jsonl(path)
        if not day_rows:
            missing.append(day.isoformat())
            continue
        for row in day_rows:
            if tickers and str(row.get('ticker') or '').upper() not in tickers:
                continue
            row['_mock_day'] = day.isoformat()
            rows.append(row)
    rows.sort(key=lambda r: (_num(r.get('opened_at')), str(r.get('ticker') or '')))
    return rows, missing


def _summarize(trades: list[dict], start: str, end: str, start_balance: float | None,
               profile_path: str | None, missing_days: list[str], elapsed: float) -> dict:
    pnl = sum(_num(r.get('pnl')) for r in trades)
    wins = sum(1 for r in trades if _num(r.get('pnl')) > 0)
    losses = sum(1 for r in trades if _num(r.get('pnl')) < 0)
    by_day: dict[str, dict] = {}
    by_ticker: dict[str, dict] = {}
    exit_reasons = Counter(str(r.get('reason') or '').split('_score=')[0] for r in trades)
    setup_counts = Counter(str(r.get('setup_type') or ((r.get('source_forensics') or {}).get('setup_type')) or 'unknown') for r in trades)
    config_hashes = Counter(str(r.get('strategy_config_hash') or r.get('config_hash') or '?') for r in trades)
    for key_name, target in (('day', by_day), ('ticker', by_ticker)):
        groups: defaultdict[str, list[dict]] = defaultdict(list)
        for row in trades:
            groups[str(row.get(key_name) or '')].append(row)
        for key, rows in sorted(groups.items()):
            gpnl = sum(_num(r.get('pnl')) for r in rows)
            gwins = sum(1 for r in rows if _num(r.get('pnl')) > 0)
            glosses = sum(1 for r in rows if _num(r.get('pnl')) < 0)
            target[key] = {
                'trades': len(rows),
                'wins': gwins,
                'losses': glosses,
                'pnl': round(gpnl, 4),
                'win_rate_pct': round(gwins / len(rows) * 100.0, 2) if rows else 0.0,
            }
    return {
        'method': 'mock_parity_step3',
        'step3_replacement': True,
        'legacy_step3_replaced': 'backtest_30d_engine.py synthetic full replay',
        'start': start,
        'end': end,
        'starting_balance': start_balance,
        'sizing_mode': 'actual_mock_allocations' if start_balance is None else 'normalized_start_balance',
        'scoring_profile': _profile_name(profile_path),
        'profile_path': os.path.abspath(profile_path) if profile_path else None,
        'trades': len(trades),
        'wins': wins,
        'losses': losses,
        'win_rate_pct': round(wins / len(trades) * 100.0, 2) if trades else 0.0,
        'pnl': round(pnl, 4),
        'ending_balance': round((start_balance or 0.0) + pnl, 4) if start_balance is not None else None,
        'by_day': by_day,
        'by_ticker': by_ticker,
        'exit_reasons': dict(exit_reasons.most_common()),
        'setups': dict(setup_counts.most_common()),
        'config_hashes': dict(config_hashes.most_common()),
        'missing_mock_trade_days': missing_days,
        'elapsed_sec': round(elapsed, 3),
        'notes': [
            'Promotion-grade Step 3 now replays actual mock-trader executed opportunities when corpora exist.',
            'This preserves live exits/fills and avoids the old synthetic replay trade-count divergence.',
            'A supplied scoring profile re-scores those same actual opportunities; opposite-side choices invert realized path P/L approximately.',
            'Days without postmortem/trades corpora are reported in missing_mock_trade_days and cannot be promotion-grade.',
        ],
    }


def _write_outputs(args: argparse.Namespace, trades: list[dict], summary: dict) -> tuple[str, str]:
    os.makedirs(args.out_dir, exist_ok=True)
    stem = f"engine_replay_{args.start}_{args.end}_{_profile_name(args.scoring_profile)}_mock_parity"
    summary_path = os.path.join(args.out_dir, f'{stem}_summary.json')
    trades_path = os.path.join(args.out_dir, f'{stem}_trades.csv')
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, sort_keys=True)
    fields = [
        'opportunity_id', 'day', 'entry_ct', 'exit_ct', 'ticker', 'side', 'actual_side',
        'entry', 'exit', 'qty', 'alloc', 'pnl', 'pnl_pct', 'mfe_pct', 'mae_pct',
        'held_sec', 'reason', 'score', 'conviction', 'setup_type', 'session_phase',
        'scoring_profile', 'profile_score', 'profile_original_side', 'strategy_config_hash',
    ]
    with open(trades_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in trades:
            writer.writerow({k: row.get(k, '') for k in fields})
    return summary_path, trades_path


def main() -> int:
    ap = argparse.ArgumentParser(description='Run mock-parity Step 3 replay.')
    ap.add_argument('--start', required=True)
    ap.add_argument('--end', required=True)
    ap.add_argument('--tickers', nargs='*', default=None)
    ap.add_argument('--scoring-profile', default=None)
    ap.add_argument('--out-dir', default=DEFAULT_OUT_DIR)
    ap.add_argument('--start-balance', type=float, default=None)
    ap.add_argument('--actual-sizing', action='store_true')
    ap.add_argument('--require-complete-days', action='store_true')
    args, _unknown = ap.parse_known_args()

    started = time.perf_counter()
    start_day = _parse_day(args.start)
    end_day = _parse_day(args.end)
    days = _market_days(start_day, end_day)
    tickers = {str(t).upper() for t in (args.tickers or [])} or None
    source_rows, missing_days = _load_rows(days, tickers)
    if args.require_complete_days and missing_days:
        raise SystemExit(f'Missing mock trade corpora for days: {missing_days}')

    profile = scoring_profiles.load_profile(args.scoring_profile) if args.scoring_profile else None
    start_balance = None if args.actual_sizing else args.start_balance
    trades: list[dict] = []
    for idx, row in enumerate(source_rows, 1):
        sig, ind, btc = _signal_for_profile(row)
        original_side = str(row.get('side') or '').upper()
        chosen_side = original_side
        profile_score = None
        if profile:
            scored = scoring_profiles.score_signal(profile, sig, ind, btc, include_details=False)
            chosen_side = str(scored.get('side') or original_side).upper()
            profile_score = scored.get('score')
        pnl, pnl_pct, alloc = _effective_pnl(row, chosen_side, start_balance)
        opened = _num(row.get('opened_at'))
        closed = _num(row.get('closed_at'))
        fx = row.get('forensics') or {}
        trade = {
            'opportunity_id': row.get('trade_id') or f"{row.get('_mock_day')}:{row.get('ticker')}:{int(opened)}:{idx}",
            'day': row.get('_mock_day'),
            'entry_ct': _iso_ct(opened),
            'exit_ct': _iso_ct(closed),
            'ticker': row.get('ticker'),
            'side': chosen_side,
            'actual_side': original_side,
            'entry': row.get('entry'),
            'exit': row.get('exit'),
            'qty': row.get('qty'),
            'alloc': alloc,
            'pnl': pnl,
            'pnl_pct': pnl_pct,
            'mfe_pct': row.get('mfe_pct'),
            'mae_pct': row.get('mae_pct'),
            'held_sec': round(closed - opened, 3) if opened and closed else '',
            'reason': row.get('reason'),
            'score': sig.get('score'),
            'conviction': row.get('conviction') or sig.get('conviction'),
            'setup_type': fx.get('setup_type') or sig.get('setup_type'),
            'session_phase': fx.get('session_phase'),
            'scoring_profile': profile.get('name') if profile else 'actual_mock',
            'profile_score': profile_score,
            'profile_original_side': original_side,
            'strategy_config_hash': row.get('strategy_config_hash') or ((fx.get('strategy') or {}).get('config_hash')),
            'source_forensics': fx,
        }
        trades.append(trade)

    elapsed = time.perf_counter() - started
    summary = _summarize(trades, args.start, args.end, start_balance, args.scoring_profile, missing_days, elapsed)
    summary_path, trades_path = _write_outputs(args, trades, summary)
    print(json.dumps({'summary': os.path.abspath(summary_path), 'trades_csv': os.path.abspath(trades_path), **summary}, indent=2, sort_keys=True))
    return 0 if not missing_days else 2


if __name__ == '__main__':
    raise SystemExit(main())
