"""Rewrite decision-tape outcomes without rescanning live indicators.

This is the Step 3 speed lever: once the live signal/feature tape exists, we can
reuse it and rebuild only the outcome layer for a bracket policy. That avoids the
slow live indicator scan for every TP/SL or bracket hypothesis.
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date
from typing import Any

import backtest_30d_engine as replay
import build_decision_tape
import replay_artifacts
import step2_execution_contract
import tournament_safety
import worker_policy


HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_TAPE_DIR = os.path.join(HERE, 'postmortem', 'backtests', 'decision_tapes')
DEFAULT_OUT_DIR = os.path.join(HERE, 'postmortem', 'backtests', 'decision_tapes_step3_policy')


def _num(value: Any, default: float | None = None) -> float | None:
    try:
        if value in (None, ''):
            return default
        return float(value)
    except Exception:
        return default


def _read_jsonl_gz(path: str) -> list[dict]:
    rows = []
    if not os.path.exists(path):
        return rows
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _write_jsonl_gz(path: str, rows: list[dict]) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    tmp = path + '.tmp'
    with gzip.open(tmp, 'wt', encoding='utf-8') as f:
        for row in rows:
            f.write(json.dumps(row, separators=(',', ':'), sort_keys=True) + '\n')
    os.replace(tmp, path)


def _tape_path(root: str, args: argparse.Namespace, day: date) -> str:
    tickers = '-'.join(args.tickers)
    suffix = f'_{args.tape_profile_name}' if args.tape_profile_name else ''
    return os.path.join(
        root,
        f'decision_tape_{args.feed}_{args.quote_mode}_{args.btc_mode}_{args.indicator_mode}_{tickers}{suffix}_{day.isoformat()}.jsonl.gz',
    )


def _fixed_ticker_brackets(raw: str) -> dict[str, tuple[float | None, float | None]]:
    out: dict[str, tuple[float | None, float | None]] = {}
    for item in str(raw or '').split(','):
        parts = [part.strip() for part in item.split(':')]
        if len(parts) != 3 or not parts[0]:
            continue
        tp = None if parts[1].lower() in ('', 'live', 'none') else float(parts[1])
        sl = None if parts[2].lower() in ('', 'live', 'none') else float(parts[2])
        out[parts[0].upper()] = (tp, sl)
    return out


def _row_chop_applies(row: dict, args: argparse.Namespace) -> bool:
    if (args.chop_bracket_mode or 'off') == 'off':
        return False
    scoped = {t.strip().upper() for t in str(args.chop_bracket_tickers or '').split(',') if t.strip()}
    ticker = str(row.get('ticker') or '').upper()
    if scoped and ticker not in scoped:
        return False
    gate_features = row.get('gate_features') or {}
    metrics = gate_features.get('entry_chop_metrics') or row.get('entry_chop_metrics') or {}
    rng = _num(metrics.get('range_180s_pct'))
    eff = _num(metrics.get('efficiency_180s'))
    flips = _num(metrics.get('flips_180s'))
    score = _num(row.get('score'))
    return (
        rng is not None
        and eff is not None
        and flips is not None
        and rng >= float(args.chop_bracket_range_threshold_pct)
        and eff <= float(args.chop_bracket_efficiency_threshold)
        and flips >= float(args.chop_bracket_flips_threshold)
        and (score is None or score <= float(args.chop_bracket_max_score))
    )


def _policy_bracket(row: dict, args: argparse.Namespace, overrides: dict[str, tuple[float | None, float | None]]) -> tuple[float, float, str]:
    ticker = str(row.get('ticker') or '').upper()
    cfg = replay.TICKER_CFG.get(ticker, {'sl': 0.05, 'tp': 0.01})
    tp = float(cfg.get('tp') or 0.01)
    sl = float(cfg.get('sl') or 0.05)
    if args.fixed_tp_pct is not None:
        tp = float(args.fixed_tp_pct)
    if args.fixed_sl_pct is not None:
        sl = float(args.fixed_sl_pct)
    if ticker in overrides:
        fixed_tp, fixed_sl = overrides[ticker]
        if fixed_tp is not None:
            tp = float(fixed_tp)
        if fixed_sl is not None:
            sl = float(fixed_sl)
    policy = 'fixed_or_live'
    if _row_chop_applies(row, args):
        tp = float(args.chop_bracket_tp_pct)
        sl = float(args.chop_bracket_sl_pct)
        policy = 'chop_tiny'
    return tp, sl, policy


def _outcome(side: str, ticker: str, entry_ts: int, entry_price: float,
             prices: list[float | None], start_sec: int, flatten_ts: int, end_sec: int,
             tp_pct: float, sl_pct: float) -> dict:
    bracket = step2_execution_contract.exit_brackets(side, float(entry_price), float(sl_pct), float(tp_pct))
    sl = bracket['sl']
    tp = bracket['tp']

    best = worst = entry_price
    exit_ts = end_sec
    exit_price = entry_price
    reason = 'end_of_data'
    for sec in range(max(entry_ts, start_sec), end_sec + 1):
        idx = sec - start_sec
        price = prices[idx] if 0 <= idx < len(prices) else None
        if price is None:
            continue
        exit_price = float(price)
        if side == 'LONG':
            best = max(best, exit_price)
            worst = min(worst, exit_price)
            unrealized = exit_price - entry_price
            if exit_price <= sl:
                exit_ts, exit_price, reason = sec, sl, 'stop_loss'
                break
            if exit_price >= tp:
                exit_ts, exit_price, reason = sec, tp, 'take_profit'
                break
        else:
            best = min(best, exit_price)
            worst = max(worst, exit_price)
            unrealized = entry_price - exit_price
            if exit_price >= sl:
                exit_ts, exit_price, reason = sec, sl, 'stop_loss'
                break
            if exit_price <= tp:
                exit_ts, exit_price, reason = sec, tp, 'take_profit'
                break
        held_min = (sec - entry_ts) / 60
        if held_min >= replay.COND_STOP_MIN and sec < flatten_ts and unrealized < 0:
            exit_ts, reason = sec, 'cond_time_stop'
            break
        if sec >= flatten_ts:
            exit_ts, reason = sec, 'session_end'
            break

    pnl_pct = (exit_price - entry_price) / entry_price * 100
    if side == 'SHORT':
        pnl_pct *= -1
    return {
        'side': side,
        'entry': round(entry_price, 4),
        'exit': round(float(exit_price), 4),
        'exit_ts': int(exit_ts),
        'exit_ct': replay._iso_ct(int(exit_ts)),
        'held_sec': int(exit_ts - entry_ts),
        'reason': reason,
        'pnl_pct': round(pnl_pct, 6),
        'mfe_pct': round(abs(best - entry_price) / entry_price * 100, 6),
        'mae_pct': round(abs(worst - entry_price) / entry_price * 100, 6),
    }


def _price_series(day: date, ticker: str, args: argparse.Namespace) -> list[float | None]:
    key = replay_artifacts.key_from_args(args)
    shard = replay_artifacts.read_price_shard(args.artifact_dir, key, day, ticker)
    prices = (shard or {}).get('price') if isinstance(shard, dict) else None
    if isinstance(prices, list) and prices:
        return prices
    stats = replay.ReplayStats()
    events = replay._load_day_events(day, args, stats)
    _start_iso, _end_iso, start_sec, end_sec = replay._session_bounds_utc(day)
    return build_decision_tape._price_series(events, ticker, start_sec, end_sec)


def rewrite_day(day_iso: str, args_dict: dict) -> dict:
    args = argparse.Namespace(**args_dict)
    day = replay._parse_day(day_iso)
    rows = _read_jsonl_gz(_tape_path(args.tape_dir, args, day))
    if not rows:
        return {'day': day_iso, 'status': 'missing_input', 'rows': 0}
    _start_iso, _end_iso, start_sec, end_sec = replay._session_bounds_utc(day)
    flatten_ts = replay._flatten_ts(day)
    prices_by_ticker = {ticker: _price_series(day, ticker, args) for ticker in args.tickers}
    overrides = _fixed_ticker_brackets(args.fixed_ticker_brackets)
    policy_counts: dict[str, int] = {}
    for row in rows:
        ticker = str(row.get('ticker') or '').upper()
        entry_ts = int(row.get('ts') or 0)
        entry_price = _num(row.get('price'))
        if ticker not in prices_by_ticker or entry_price is None:
            continue
        tp, sl, policy = _policy_bracket(row, args, overrides)
        policy_counts[policy] = policy_counts.get(policy, 0) + 1
        prices = prices_by_ticker[ticker]
        row['outcomes'] = {
            'LONG': _outcome('LONG', ticker, entry_ts, float(entry_price), prices, start_sec, flatten_ts, end_sec, tp, sl),
            'SHORT': _outcome('SHORT', ticker, entry_ts, float(entry_price), prices, start_sec, flatten_ts, end_sec, tp, sl),
        }
        row['outcome_policy'] = {
            'name': args.policy_name,
            'tp_pct': round(tp, 8),
            'sl_pct': round(sl, 8),
            'policy': policy,
        }
    out_path = _tape_path(args.out_dir, args, day)
    _write_jsonl_gz(out_path, rows)
    return {
        'day': day_iso,
        'status': 'ok',
        'rows': len(rows),
        'out': out_path,
        'policy_counts': policy_counts,
    }


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Rewrite decision-tape outcomes for a bracket policy.')
    ap.add_argument('--start', required=True)
    ap.add_argument('--end', required=True)
    ap.add_argument('--tickers', nargs='+', default=replay.TICKERS)
    ap.add_argument('--feed', default='sip')
    ap.add_argument('--quote-mode', default='per-second')
    ap.add_argument('--btc-mode', default='bars')
    ap.add_argument('--indicator-mode', choices=['live', 'fast'], default='live')
    ap.add_argument('--tape-dir', default=DEFAULT_TAPE_DIR)
    ap.add_argument('--out-dir', default=DEFAULT_OUT_DIR)
    ap.add_argument('--artifact-dir', default=replay_artifacts.DEFAULT_ARTIFACT_DIR)
    ap.add_argument('--prepared-cache-dir', default=replay.DEFAULT_PREPARED_DIR)
    ap.add_argument('--tape-profile-name', default='')
    ap.add_argument('--policy-name', default='policy')
    ap.add_argument('--fixed-tp-pct', type=float, default=None)
    ap.add_argument('--fixed-sl-pct', type=float, default=None)
    ap.add_argument('--fixed-ticker-brackets', default='')
    ap.add_argument('--chop-bracket-mode', choices=['off', 'tiny'], default='off')
    ap.add_argument('--chop-bracket-tickers', default='')
    ap.add_argument('--chop-bracket-range-threshold-pct', type=float, default=0.75)
    ap.add_argument('--chop-bracket-efficiency-threshold', type=float, default=0.20)
    ap.add_argument('--chop-bracket-flips-threshold', type=float, default=10)
    ap.add_argument('--chop-bracket-max-score', type=float, default=999.0)
    ap.add_argument('--chop-bracket-tp-pct', type=float, default=0.0010)
    ap.add_argument('--chop-bracket-sl-pct', type=float, default=0.0010)
    ap.add_argument('--workers', type=int, default=worker_policy.DEFAULT_MAX_WORKERS)
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    args.tickers = [ticker.upper() for ticker in args.tickers]
    days = [day.isoformat() for day in replay._market_days(replay._parse_day(args.start), replay._parse_day(args.end))]
    workers = worker_policy.clamp_workers(args.workers, len(days) or 1)
    started = time.time()
    args_dict = vars(args).copy()
    if workers > 1:
        results = []
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(rewrite_day, day, args_dict): day for day in days}
            for fut in as_completed(futures):
                row = fut.result()
                results.append(row)
                print(f"[outcome-rewrite] {row.get('day')} rows={row.get('rows')} status={row.get('status')}", flush=True)
        results.sort(key=lambda row: row.get('day') or '')
    else:
        results = [rewrite_day(day, args_dict) for day in days]
    payload = {
        'schema_version': 1,
        'policy_name': args.policy_name,
        'elapsed_seconds': round(time.time() - started, 3),
        'rows': sum(int(row.get('rows') or 0) for row in results),
        'out_dir': os.path.abspath(args.out_dir),
        'config': {
            key: value for key, value in vars(args).items()
            if key not in ('workers',)
        },
        'code_hashes': {
            'rewrite_decision_tape_outcomes.py': tournament_safety._file_sha256(__file__),
            'build_decision_tape.py': tournament_safety._file_sha256('build_decision_tape.py'),
            'replay_artifacts.py': tournament_safety._file_sha256('replay_artifacts.py'),
            'step2_execution_contract.py': tournament_safety._file_sha256('step2_execution_contract.py'),
        },
        'results': results,
    }
    os.makedirs(args.out_dir, exist_ok=True)
    summary_path = os.path.join(args.out_dir, f'{args.policy_name}_rewrite_summary.json')
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    print(json.dumps({'summary': os.path.abspath(summary_path), 'rows': payload['rows'], 'elapsed_seconds': payload['elapsed_seconds']}, indent=2))
    return 1 if any(row.get('status') != 'ok' for row in results) else 0


if __name__ == '__main__':
    raise SystemExit(main())
