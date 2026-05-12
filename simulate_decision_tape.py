"""Simulate scoring profiles against reusable decision tapes."""
from __future__ import annotations

import argparse
import gzip
import json
import os
from collections import Counter, defaultdict

import backtest_30d_engine as replay
import decision_tape_gates
import scoring_profiles


HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_TAPE_DIR = os.path.join(HERE, 'postmortem', 'backtests', 'decision_tapes')
DEFAULT_OUT_DIR = os.path.join(HERE, 'postmortem', 'backtests')


def _read_tape(path: str) -> list[dict]:
    rows = []
    if not os.path.exists(path):
        return rows
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _tape_path(args, day: str) -> str:
    tickers = '-'.join(args.tickers)
    suffix = f'_{args.tape_profile_name}' if args.tape_profile_name else ''
    indicator_mode = getattr(args, 'indicator_mode', 'live') or 'live'
    return os.path.join(
        args.tape_dir,
        f'decision_tape_{args.feed}_{args.quote_mode}_{args.btc_mode}_{indicator_mode}_{tickers}{suffix}_{day}.jsonl.gz',
    )


def _profile_side(profile: dict | None, row: dict) -> tuple[str, float | None]:
    original = row.get('original_side')
    if not profile:
        score = row.get('score')
        return original, float(score) if score not in (None, '') else None
    weights = profile.get('weights') or {}
    score = float(profile.get('bias') or 0.0)
    feats = row.get('model_features') or {}
    for name, weight in weights.items():
        score += float(weight or 0.0) * float(feats.get(name, 0.0) or 0.0)
    if score > 0:
        return 'LONG', score
    if score < 0:
        return 'SHORT', score
    return original, score


def _trade_row(row: dict, side: str, profile_score: float | None, balance: float, tickers_count: int) -> dict:
    outcome = (row.get('outcomes') or {}).get(side)
    if not outcome:
        raise ValueError(f"missing outcome for {side}: {row.get('opportunity_id')}")
    entry = float(outcome['entry'])
    exit_price = float(outcome['exit'])
    alloc = round(balance * replay.TRADE_SIZE_PCT / max(1, tickers_count), 2)
    qty = alloc / entry if entry > 0 else 0.0
    gross = (exit_price - entry) * qty
    if side == 'SHORT':
        gross *= -1
    return {
        'opportunity_id': row.get('opportunity_id'),
        'entry_ct': row.get('ts_ct'),
        'exit_ct': outcome.get('exit_ct'),
        'ticker': row.get('ticker'),
        'side': side,
        'original_side': row.get('original_side'),
        'entry': round(entry, 4),
        'exit': round(exit_price, 4),
        'qty': round(qty, 6),
        'alloc': alloc,
        'pnl': round(gross, 4),
        'pnl_pct': outcome.get('pnl_pct'),
        'mfe_pct': outcome.get('mfe_pct'),
        'mae_pct': outcome.get('mae_pct'),
        'held_sec': outcome.get('held_sec'),
        'reason': outcome.get('reason'),
        'score': row.get('score'),
        'profile_score': round(profile_score, 6) if profile_score is not None else None,
        'conviction': row.get('conviction'),
        'setup_type': row.get('setup_type'),
        'session_phase': row.get('session_phase'),
        'btc_regime': row.get('btc_regime'),
    }


def _load_gate(path: str | None, mode: str | None, threshold_pct: float | None) -> dict | None:
    gate = None
    if path:
        with open(path, 'r', encoding='utf-8') as f:
            gate = json.load(f)
    elif mode:
        gate = {'mode': mode}
    if gate and threshold_pct is not None:
        gate['ticker_session_return_below_pct'] = float(threshold_pct)
    return gate


def simulate_day(rows: list[dict], profile: dict | None, balance: float, tickers: list[str],
                 entry_gate: dict | None = None) -> tuple[list[dict], float, Counter]:
    open_until: dict[str, int] = {}
    last_signal_ts: dict[str, float] = {}
    closed = []
    skipped = Counter()
    for row in sorted(rows, key=lambda r: (int(r.get('ts') or 0), r.get('ticker') or '')):
        ts = int(row.get('ts') or 0)
        ticker = row.get('ticker')
        if not ticker:
            continue
        if ts < int(open_until.get(ticker, 0) or 0):
            skipped['open_position'] += 1
            continue
        if row.get('conviction') not in replay.MIN_CONVICTION:
            skipped['below_min_conviction'] += 1
            continue
        side, profile_score = _profile_side(profile, row)
        missing = decision_tape_gates.missing_gate_features(row, entry_gate)
        if missing:
            skipped[f"entry_gate_missing_features:{','.join(missing)}"] += 1
        else:
            gate_reason = decision_tape_gates.gate_reason(entry_gate, row, side)
            if gate_reason:
                skipped[f'entry_gate:{gate_reason}'] += 1
                continue
        setup = row.get('setup_type') or 'unknown'
        key = f'{ticker}:{side}:{setup}'
        cooldown = float(replay.ws_scalp.SETUP_COOLDOWN_SEC.get(setup, 30))
        if ts - float(last_signal_ts.get(key, 0) or 0) < cooldown:
            skipped['cooldown'] += 1
            continue
        trade = _trade_row(row, side, profile_score, balance, len(tickers))
        closed.append(trade)
        balance += float(trade['pnl'])
        open_until[ticker] = int(ts + int(trade.get('held_sec') or 0))
        last_signal_ts[key] = float(ts)
    return closed, balance, skipped


def summarize(rows: list[dict], skipped: Counter, args, ending_balance: float) -> dict:
    wins = [r for r in rows if float(r.get('pnl') or 0) > 0]
    losses = [r for r in rows if float(r.get('pnl') or 0) < 0]
    by_ticker = {}
    for ticker in args.tickers:
        scoped = [r for r in rows if r.get('ticker') == ticker]
        by_ticker[ticker] = {
            'trades': len(scoped),
            'wins': sum(1 for r in scoped if float(r.get('pnl') or 0) > 0),
            'losses': sum(1 for r in scoped if float(r.get('pnl') or 0) < 0),
            'pnl': round(sum(float(r.get('pnl') or 0) for r in scoped), 2),
            'win_rate_pct': round(100 * sum(1 for r in scoped if float(r.get('pnl') or 0) > 0) / len(scoped), 2) if scoped else None,
        }
    return {
        'profile': args.scoring_profile,
        'entry_gate': getattr(args, 'entry_gate_mode', None) or getattr(args, 'entry_gate_json', None),
        'start': args.start,
        'end': args.end,
        'trades': len(rows),
        'wins': len(wins),
        'losses': len(losses),
        'win_rate_pct': round(100 * len(wins) / len(rows), 2) if rows else None,
        'pnl': round(sum(float(r.get('pnl') or 0) for r in rows), 2),
        'starting_balance': args.start_balance,
        'ending_balance': round(ending_balance, 2),
        'by_ticker': by_ticker,
        'exit_reasons': dict(Counter(r.get('reason') for r in rows)),
        'skipped': dict(skipped),
        'method_note': (
            'Decision-tape simulation uses captured decision points and precomputed LONG/SHORT outcomes. '
            'It is much faster than full event replay and best for scoring-profile and causal entry-gate iteration. '
            'Entry gates only apply when the tape contains the required causal gate_features.'
        ),
    }


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Simulate a scoring profile on decision tapes.')
    ap.add_argument('--start', required=True)
    ap.add_argument('--end', required=True)
    ap.add_argument('--tickers', nargs='+', default=replay.TICKERS)
    ap.add_argument('--feed', default='sip')
    ap.add_argument('--quote-mode', default='per-second')
    ap.add_argument('--btc-mode', default='bars')
    ap.add_argument('--indicator-mode', choices=['live', 'fast'], default='live')
    ap.add_argument('--tape-dir', default=DEFAULT_TAPE_DIR)
    ap.add_argument('--tape-profile-name', default='')
    ap.add_argument('--scoring-profile', default=None)
    ap.add_argument('--entry-gate-json', default=None, help='Optional JSON gate spec to apply after profile side selection.')
    ap.add_argument('--entry-gate-mode', default=None, help='Optional built-in gate mode, e.g. ticker-negative-below-vwap-btc-not-bull.')
    ap.add_argument('--entry-gate-threshold-pct', type=float, default=None)
    ap.add_argument('--start-balance', type=float, default=100000.0)
    ap.add_argument('--out', default=None)
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    args.tickers = [t.upper() for t in args.tickers]
    profile = scoring_profiles.load_profile(args.scoring_profile) if args.scoring_profile else None
    entry_gate = _load_gate(args.entry_gate_json, args.entry_gate_mode, args.entry_gate_threshold_pct)
    balance = float(args.start_balance)
    all_trades = []
    skipped = Counter()
    for day in replay._market_days(replay._parse_day(args.start), replay._parse_day(args.end)):
        rows = _read_tape(_tape_path(args, day.isoformat()))
        day_trades, balance, day_skipped = simulate_day(rows, profile, balance, args.tickers, entry_gate)
        all_trades.extend(day_trades)
        skipped.update(day_skipped)
    summary = summarize(all_trades, skipped, args, balance)
    payload = {'summary': summary, 'trades': all_trades}
    out = args.out or os.path.join(DEFAULT_OUT_DIR, f"decision_tape_sim_{args.start}_{args.end}.json")
    os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    print(json.dumps({'out': out, 'summary': summary}, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
