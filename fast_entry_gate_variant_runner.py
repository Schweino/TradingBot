"""Run fast entry-quality gate variants against reusable decision tapes.

This is for causal gate research after a scoring profile has already chosen a
side. It does not alter the live engine and it does not rebuild market replay.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import time
from collections import Counter

import backtest_30d_engine as replay
import scoring_profiles
import simulate_decision_tape


HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUT_DIR = os.path.join(HERE, 'postmortem', 'backtests', 'entry_gate_variants')
DEFAULT_MODES = [
    'ticker-negative',
    'ticker-negative-below-vwap',
    'ticker-negative-btc-not-bull',
    'ticker-negative-below-vwap-btc-not-bull',
    'ticker-negative-below-vwap-weak-flow',
    'ticker-negative-below-vwap-lower-half-range',
    'ticker-negative-below-vwap-stock-lagging',
]
DEFAULT_THRESHOLDS = [-0.25, -0.35, -0.5, -0.6, -0.75, -1.0]


def _safe_name(value: str) -> str:
    cleaned = ''.join(ch if ch.isalnum() or ch in '._-' else '_' for ch in value).strip('._')
    digest = hashlib.sha1(value.encode('utf-8')).hexdigest()[:8]
    return f'{cleaned[:48]}_{digest}'


def _market_days(start: str, end: str) -> list[str]:
    return [day.isoformat() for day in replay._market_days(replay._parse_day(start), replay._parse_day(end))]


def _load_rows(args) -> dict[str, list[dict]]:
    shim = argparse.Namespace(
        tickers=args.tickers,
        feed=args.feed,
        quote_mode=args.quote_mode,
        btc_mode=args.btc_mode,
        tape_profile_name=args.tape_profile_name,
        tape_dir=args.tape_dir,
    )
    return {
        day: simulate_decision_tape._read_tape(simulate_decision_tape._tape_path(shim, day))
        for day in _market_days(args.start, args.end)
    }


def _summary(trades: list[dict], skipped: Counter, start_balance: float, ending_balance: float,
             tickers: list[str]) -> dict:
    wins = sum(1 for row in trades if float(row.get('pnl') or 0) > 0)
    losses = sum(1 for row in trades if float(row.get('pnl') or 0) < 0)
    by_day = {}
    by_ticker = {}
    for row in trades:
        day = str(row.get('entry_ct') or '')[:10]
        if day:
            bucket = by_day.setdefault(day, {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0})
            pnl = float(row.get('pnl') or 0)
            bucket['trades'] += 1
            bucket['wins'] += int(pnl > 0)
            bucket['losses'] += int(pnl < 0)
            bucket['pnl'] += pnl
    for ticker in tickers:
        scoped = [row for row in trades if row.get('ticker') == ticker]
        ticker_wins = sum(1 for row in scoped if float(row.get('pnl') or 0) > 0)
        by_ticker[ticker] = {
            'trades': len(scoped),
            'wins': ticker_wins,
            'losses': sum(1 for row in scoped if float(row.get('pnl') or 0) < 0),
            'pnl': round(sum(float(row.get('pnl') or 0) for row in scoped), 2),
            'win_rate_pct': round(100 * ticker_wins / len(scoped), 2) if scoped else None,
        }
    for bucket in by_day.values():
        bucket['pnl'] = round(bucket['pnl'], 2)
        bucket['win_rate_pct'] = round(100 * bucket['wins'] / bucket['trades'], 2) if bucket['trades'] else None
    return {
        'trades': len(trades),
        'wins': wins,
        'losses': losses,
        'win_rate_pct': round(100 * wins / len(trades), 2) if trades else None,
        'pnl': round(sum(float(row.get('pnl') or 0) for row in trades), 2),
        'starting_balance': round(start_balance, 2),
        'ending_balance': round(ending_balance, 2),
        'by_day': dict(sorted(by_day.items())),
        'by_ticker': by_ticker,
        'exit_reasons': dict(Counter(row.get('reason') for row in trades)),
        'skipped': dict(skipped),
    }


def _simulate_range(rows_by_day: dict[str, list[dict]], profile: dict | None, gate: dict,
                    start: str, end: str, start_balance: float, tickers: list[str]) -> dict:
    balance = float(start_balance)
    trades: list[dict] = []
    skipped = Counter()
    for day in _market_days(start, end):
        day_trades, balance, day_skipped = simulate_decision_tape.simulate_day(
            rows_by_day.get(day, []),
            profile,
            balance,
            tickers,
            gate,
        )
        trades.extend(day_trades)
        skipped.update(day_skipped)
    return _summary(trades, skipped, start_balance, balance, tickers)


def _gate_variants(args) -> list[dict]:
    if args.variants_json:
        with open(args.variants_json, 'r', encoding='utf-8') as f:
            payload = json.load(f)
        return payload.get('variants', payload) if isinstance(payload, dict) else payload
    variants = []
    for mode in args.modes:
        for threshold in args.thresholds:
            gate = {
                'name': f'{mode}_thr{threshold:+.2f}',
                'mode': mode,
                'ticker_session_return_below_pct': float(threshold),
                'btc_bullish_regimes': ['bull', 'bull_momentum', 'impulse_up'],
            }
            variants.append(gate)
    return variants


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Fast decision-tape entry-gate variant runner.')
    ap.add_argument('--start', default='2026-04-06')
    ap.add_argument('--end', default='2026-05-01')
    ap.add_argument('--train-start', default=None)
    ap.add_argument('--train-end', default=None)
    ap.add_argument('--test-start', default=None)
    ap.add_argument('--test-end', default=None)
    ap.add_argument('--tickers', nargs='+', default=replay.TICKERS)
    ap.add_argument('--feed', default='sip')
    ap.add_argument('--quote-mode', default='per-second')
    ap.add_argument('--btc-mode', default='bars')
    ap.add_argument('--tape-dir', default=simulate_decision_tape.DEFAULT_TAPE_DIR)
    ap.add_argument('--tape-profile-name', default='')
    ap.add_argument('--scoring-profile', default=None)
    ap.add_argument('--start-balance', type=float, default=100000.0)
    ap.add_argument('--modes', nargs='+', default=DEFAULT_MODES)
    ap.add_argument('--thresholds', nargs='+', type=float, default=DEFAULT_THRESHOLDS)
    ap.add_argument('--variants-json', default=None)
    ap.add_argument('--out-dir', default=DEFAULT_OUT_DIR)
    ap.add_argument('--name', default=None)
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    args.tickers = [ticker.upper() for ticker in args.tickers]
    started = time.perf_counter()
    profile = scoring_profiles.load_profile(args.scoring_profile) if args.scoring_profile else None
    rows_by_day = _load_rows(args)
    variants = _gate_variants(args)

    results = []
    for gate in variants:
        full = _simulate_range(rows_by_day, profile, gate, args.start, args.end, args.start_balance, args.tickers)
        train = None
        test = None
        if args.train_start and args.train_end:
            train = _simulate_range(rows_by_day, profile, gate, args.train_start, args.train_end, args.start_balance, args.tickers)
        if args.test_start and args.test_end:
            test = _simulate_range(rows_by_day, profile, gate, args.test_start, args.test_end, args.start_balance, args.tickers)
        results.append({
            'gate': gate,
            'full': full,
            'train': train,
            'test': test,
            'rank_score': ((test or full)['pnl'], full['pnl'], full['win_rate_pct'] or 0),
        })
    results.sort(key=lambda row: row['rank_score'], reverse=True)
    for idx, row in enumerate(results, 1):
        row['rank'] = idx
        row.pop('rank_score', None)

    run_name = args.name or f"entry_gate_variants_{args.start}_{args.end}_{time.strftime('%Y%m%d_%H%M%S')}"
    run_dir = os.path.join(args.out_dir, _safe_name(run_name))
    os.makedirs(run_dir, exist_ok=True)
    out_json = os.path.join(run_dir, 'entry_gate_variant_summary.json')
    payload = {
        'schema_version': 1,
        'elapsed_seconds': round(time.perf_counter() - started, 3),
        'config': vars(args),
        'count': len(results),
        'method_note': (
            'Uses decision tapes with precomputed LONG/SHORT outcomes and causal gate_features. '
            'Rows with missing required gate_features are not blocked and are counted in skipped.'
        ),
        'results': results,
    }
    with open(out_json, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, sort_keys=True)

    out_csv = os.path.join(run_dir, 'entry_gate_variant_summary.csv')
    with open(out_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(
            f,
            fieldnames=['rank', 'name', 'mode', 'threshold_pct', 'full_pnl', 'full_trades',
                        'full_win_rate_pct', 'train_pnl', 'test_pnl', 'missing_feature_skips'],
        )
        writer.writeheader()
        for row in results:
            skipped = row['full'].get('skipped') or {}
            writer.writerow({
                'rank': row['rank'],
                'name': row['gate'].get('name'),
                'mode': row['gate'].get('mode'),
                'threshold_pct': row['gate'].get('ticker_session_return_below_pct'),
                'full_pnl': row['full'].get('pnl'),
                'full_trades': row['full'].get('trades'),
                'full_win_rate_pct': row['full'].get('win_rate_pct'),
                'train_pnl': (row.get('train') or {}).get('pnl'),
                'test_pnl': (row.get('test') or {}).get('pnl'),
                'missing_feature_skips': sum(v for k, v in skipped.items() if str(k).startswith('entry_gate_missing_features')),
            })

    print(json.dumps({
        'out_json': out_json,
        'out_csv': out_csv,
        'elapsed_seconds': payload['elapsed_seconds'],
        'variants': len(results),
        'leader': results[0] if results else None,
    }, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
