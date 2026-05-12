"""Build sharded replay artifacts for faster research and finalist validation.

This prepares deterministic per-day/ticker price and outcome shards from the
same prepared event tapes used by the full replay harness.
"""
from __future__ import annotations

import argparse
import json
import os
from concurrent.futures import ProcessPoolExecutor, as_completed

import backtest_30d_engine as replay
import replay_artifacts
import worker_policy


def _read_opportunity_entry_ts(day, args) -> dict[str, set[int]]:
    out = {ticker: set() for ticker in args.tickers}
    if args.outcome_mode != 'decision':
        return out
    tickers = '-'.join(args.tickers)
    opp_path = os.path.join(
        args.opportunity_dir,
        f'engine_opportunities_{args.feed}_{args.quote_mode}_{args.btc_mode}_{tickers}_{day.isoformat()}.jsonl',
    )
    rows = []
    if os.path.exists(opp_path):
        with open(opp_path, 'r', encoding='utf-8') as f:
            rows = [json.loads(line) for line in f if line.strip()]
    else:
        checkpoint = os.path.join(
            replay.DEFAULT_OUT_DIR,
            'engine_replay_days',
            f'{args.feed}_{args.quote_mode}_{args.btc_mode}_{tickers}_100000_{day.isoformat()}.json',
        )
        payload = replay._read_json(checkpoint, {}) or {}
        rows = payload.get('opportunities') or []
    for row in rows:
        ticker = str(row.get('ticker') or '').upper()
        if ticker in out and row.get('ts') is not None:
            out[ticker].add(int(row.get('ts')))
    return out


def build_day(day_iso: str, args_dict: dict) -> dict:
    args = argparse.Namespace(**args_dict)
    day = replay._parse_day(day_iso)
    args.tickers = [ticker.upper() for ticker in args.tickers]
    key = replay_artifacts.key_from_args(args)
    stats = replay.ReplayStats()
    events = replay._load_day_events(day, args, stats)
    _start_iso, _end_iso, start_sec, end_sec = replay._session_bounds_utc(day)
    flatten_ts = replay._flatten_ts(day)
    manifest = replay_artifacts.write_manifest(
        args.artifact_dir,
        args,
        day,
        {
            'prepared_events': len(events),
            'artifact_key': key.label,
            'exit_replay_model': replay_artifacts.EXIT_REPLAY_MODEL_VERSION,
        },
    )
    entry_ts = _read_opportunity_entry_ts(day, args)

    price_paths = {}
    outcome_paths = {}
    outcome_rows = {}
    for ticker in args.tickers:
        ts_values, prices = replay_artifacts.price_series_from_events(events, ticker, start_sec, end_sec)
        _exit_ts, exit_points = replay_artifacts.exit_series_from_events(events, ticker, start_sec, end_sec)
        price_paths[ticker] = replay_artifacts.write_price_shard(
            args.artifact_dir,
            key,
            day,
            ticker,
            ts_values,
            prices,
            manifest.get('fingerprint'),
        )
        filter_set = entry_ts.get(ticker) if args.outcome_mode == 'decision' else None
        rows = replay_artifacts.build_outcome_rows(
            ticker,
            ts_values,
            prices,
            start_sec,
            flatten_ts,
            end_sec,
            filter_set,
            exit_points,
        )
        outcome_rows[ticker] = len(rows)
        outcome_paths[ticker] = replay_artifacts.write_outcome_shard(
            args.artifact_dir,
            key,
            day,
            ticker,
            rows,
            args.outcome_mode,
            manifest.get('fingerprint'),
        )
    return {
        'day': day.isoformat(),
        'prepared_events': len(events),
        'fingerprint': manifest.get('fingerprint'),
        'price_paths': price_paths,
        'outcome_paths': outcome_paths,
        'outcome_rows': outcome_rows,
        'cache_hits': dict(stats.cache_hits),
        'fetched': dict(stats.fetched),
    }


def parse_args() -> argparse.Namespace:
    start, end = replay._default_dates()
    ap = argparse.ArgumentParser(description='Build reusable replay price/outcome artifacts.')
    ap.add_argument('--start', default=start.isoformat())
    ap.add_argument('--end', default=end.isoformat())
    ap.add_argument('--tickers', nargs='+', default=replay.TICKERS)
    ap.add_argument('--feed', default='sip', choices=['sip', 'iex', 'delayed_sip'])
    ap.add_argument('--quote-mode', default='per-second', choices=['per-second', 'all', 'off'])
    ap.add_argument('--btc-mode', default='bars', choices=['bars', 'off'])
    ap.add_argument('--cache-dir', default=replay.DEFAULT_CACHE_DIR)
    ap.add_argument('--prepared-cache-dir', default=replay.DEFAULT_PREPARED_DIR)
    ap.add_argument('--artifact-dir', default=replay_artifacts.DEFAULT_ARTIFACT_DIR)
    ap.add_argument('--opportunity-dir', default=os.path.join(replay.DEFAULT_OUT_DIR, 'opportunities'))
    ap.add_argument('--outcome-mode', choices=['decision', 'all-seconds'], default='decision')
    ap.add_argument('--refresh', action='store_true')
    ap.add_argument('--refresh-prepared-events', action='store_true')
    ap.add_argument('--max-pages', type=int, default=1000)
    ap.add_argument('--workers', type=int, default=worker_policy.DEFAULT_MAX_WORKERS)
    return ap.parse_args()


def _args_dict(args: argparse.Namespace) -> dict:
    return {
        'tickers': list(args.tickers),
        'feed': args.feed,
        'quote_mode': args.quote_mode,
        'btc_mode': args.btc_mode,
        'cache_dir': args.cache_dir,
        'prepared_cache_dir': args.prepared_cache_dir,
        'artifact_dir': args.artifact_dir,
        'opportunity_dir': args.opportunity_dir,
        'outcome_mode': args.outcome_mode,
        'refresh': args.refresh,
        'refresh_prepared_events': args.refresh_prepared_events,
        'max_pages': args.max_pages,
        'use_prepared_events': True,
        'write_prepared_events': True,
    }


def main() -> int:
    args = parse_args()
    args.tickers = [ticker.upper() for ticker in args.tickers]
    days = replay._market_days(replay._parse_day(args.start), replay._parse_day(args.end))
    if not days:
        raise SystemExit('no market days in requested range')

    completed = []
    workers = worker_policy.clamp_workers(args.workers, len(days))
    args_dict = _args_dict(args)
    if workers > 1:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(build_day, day.isoformat(), args_dict): day for day in days}
            for fut in as_completed(futures):
                row = fut.result()
                completed.append(row)
                print(f"[replay-artifacts] {row['day']} outcomes={row['outcome_rows']}", flush=True)
    else:
        for day in days:
            row = build_day(day.isoformat(), args_dict)
            completed.append(row)
            print(f"[replay-artifacts] {row['day']} outcomes={row['outcome_rows']}", flush=True)

    completed.sort(key=lambda row: row['day'])
    print(json.dumps({
        'days': len(completed),
        'artifact_dir': os.path.abspath(args.artifact_dir),
        'outcome_mode': args.outcome_mode,
        'rows': completed,
    }, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
