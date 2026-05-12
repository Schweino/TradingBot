"""Build reusable latency-aware Step 2 outcome shards.

The normal outcome shards are intentionally instant-fill. These shards are
separate and keyed by latency-model hash so fast Step 2 search can reuse live-
latency outcomes without mixing them with older optimistic caches.
"""
from __future__ import annotations

import argparse
import json
import os
from concurrent.futures import ProcessPoolExecutor, as_completed

import backtest_30d_engine as replay
import build_decision_tape
import replay_artifacts
import step2_latency_model
import worker_policy


def _latency_shard_exists(args: argparse.Namespace, key: replay_artifacts.ArtifactKey,
                          day, ticker: str) -> bool:
    cache_key = replay_artifacts.latency_cache_key(
        args.step2_latency_mode,
        args.step2_latency_percentile,
        args.step2_latency_model,
    )
    path = replay_artifacts.latency_outcome_shard_path(args.artifact_dir, key, day, ticker, cache_key)
    return os.path.exists(path)


def build_day(day_iso: str, args_dict: dict) -> dict:
    args = argparse.Namespace(**args_dict)
    args.tickers = [ticker.upper() for ticker in args.tickers]
    day = replay._parse_day(day_iso)
    key = replay_artifacts.key_from_args(args)
    existing = {
        ticker: _latency_shard_exists(args, key, day, ticker)
        for ticker in args.tickers
    }
    if all(existing.values()) and not args.refresh:
        return {
            'day': day.isoformat(),
            'status': 'skipped_existing',
            'outcome_rows': {ticker: 0 for ticker in args.tickers},
            'existing': existing,
        }

    opportunities, source, prepared_events = build_decision_tape._scan_signal_opportunities_parallel(day, args)
    entry_ts_by_ticker = {ticker: set() for ticker in args.tickers}
    for row in opportunities:
        ticker = str(row.get('ticker') or '').upper()
        ts = row.get('ts')
        if ticker in entry_ts_by_ticker and ts is not None:
            entry_ts_by_ticker[ticker].add(int(ts))

    tape_args = argparse.Namespace(
        tickers=args.tickers,
        feed=args.feed,
        quote_mode=args.quote_mode,
        btc_mode=args.btc_mode,
        cache_dir=args.cache_dir,
        prepared_cache_dir=args.prepared_cache_dir,
        use_prepared_events=True,
        refresh_prepared_events=False,
        refresh=False,
        max_pages=1000,
    )
    stats = replay.ReplayStats()
    events = replay._load_day_events(day, tape_args, stats)
    _start_iso, _end_iso, start_sec, end_sec = replay._session_bounds_utc(day)
    flatten_ts = replay._flatten_ts(day)
    manifest = replay_artifacts.write_manifest(
        args.artifact_dir,
        args,
        day,
        {
            'prepared_events': len(events),
            'artifact_key': key.label,
            'latency_outcome_shards': True,
            'step2_latency_mode': args.step2_latency_mode,
            'step2_latency_model': os.path.abspath(args.step2_latency_model),
            'step2_latency_model_hash': replay_artifacts.latency_model_hash(args.step2_latency_model),
            'step2_latency_percentile': args.step2_latency_percentile,
            'executable_trade_filter_version': replay_artifacts.EXECUTABLE_TRADE_FILTER_VERSION,
            'exit_replay_model': replay_artifacts.EXIT_REPLAY_MODEL_VERSION,
            'non_executable_trade_conditions': sorted(replay_artifacts.NON_EXECUTABLE_TRADE_CONDITIONS),
            'opportunity_source': source,
        },
    )
    model = step2_latency_model.load_model(args.step2_latency_model)
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
        if existing.get(ticker) and not args.refresh:
            outcome_rows[ticker] = 0
            outcome_paths[ticker] = 'existing'
            continue
        rows = replay_artifacts.build_latency_outcome_rows(
            ticker,
            ts_values,
            prices,
            start_sec,
            flatten_ts,
            end_sec,
            model,
            args.step2_latency_mode,
            args.step2_latency_percentile,
            entry_ts_by_ticker.get(ticker),
            exit_points,
        )
        outcome_rows[ticker] = len(rows)
        outcome_paths[ticker] = replay_artifacts.write_latency_outcome_shard(
            args.artifact_dir,
            key,
            day,
            ticker,
            rows,
            args.step2_latency_mode,
            args.step2_latency_percentile,
            args.step2_latency_model,
            manifest.get('fingerprint'),
        )
    return {
        'day': day.isoformat(),
        'status': 'ok',
        'opportunities': len(opportunities),
        'prepared_events': prepared_events or len(events),
        'fingerprint': manifest.get('fingerprint'),
        'price_paths': price_paths,
        'outcome_paths': outcome_paths,
        'outcome_rows': outcome_rows,
        'cache_hits': dict(stats.cache_hits),
        'fetched': dict(stats.fetched),
    }


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Build reusable latency-aware Step 2 outcome shards.')
    ap.add_argument('--start', required=True)
    ap.add_argument('--end', required=True)
    ap.add_argument('--tickers', nargs='+', default=replay.TICKERS)
    ap.add_argument('--feed', default='sip')
    ap.add_argument('--quote-mode', default='per-second')
    ap.add_argument('--btc-mode', default='bars')
    ap.add_argument('--indicator-mode', choices=['live', 'fast'], default='live')
    ap.add_argument('--cache-dir', default=replay.DEFAULT_CACHE_DIR)
    ap.add_argument('--prepared-cache-dir', default=replay.DEFAULT_PREPARED_DIR)
    ap.add_argument('--artifact-dir', default=replay_artifacts.DEFAULT_ARTIFACT_DIR)
    ap.add_argument('--workers', type=int, default=worker_policy.DEFAULT_MAX_WORKERS)
    ap.add_argument('--refresh', action='store_true')
    ap.add_argument('--step2-latency-mode', choices=['entry', 'entry-exit'], default='entry-exit')
    ap.add_argument('--step2-latency-model', default=step2_latency_model.DEFAULT_MODEL_PATH)
    ap.add_argument('--step2-latency-percentile', choices=['p50', 'p75', 'p95', 'default'], default='p75')
    ap.add_argument('--decision-start-ts', type=int, default=0)
    ap.add_argument('--decision-end-ts', type=int, default=0)
    ap.add_argument('--disable-active-scoring-profile', action='store_true')
    return ap.parse_args()


def _args_dict(args: argparse.Namespace) -> dict:
    return {
        'tickers': list(args.tickers),
        'feed': args.feed,
        'quote_mode': args.quote_mode,
        'btc_mode': args.btc_mode,
        'indicator_mode': args.indicator_mode,
        'cache_dir': args.cache_dir,
        'prepared_cache_dir': args.prepared_cache_dir,
        'artifact_dir': args.artifact_dir,
        'workers': args.workers,
        'refresh': args.refresh,
        'step2_latency_mode': args.step2_latency_mode,
        'step2_latency_model': args.step2_latency_model,
        'step2_latency_percentile': args.step2_latency_percentile,
        'decision_start_ts': args.decision_start_ts,
        'decision_end_ts': args.decision_end_ts,
        'disable_active_scoring_profile': args.disable_active_scoring_profile,
    }


def main() -> int:
    args = parse_args()
    args.tickers = [ticker.upper() for ticker in args.tickers]
    days = replay._market_days(replay._parse_day(args.start), replay._parse_day(args.end))
    if not days:
        raise SystemExit('no market days in requested range')
    workers = worker_policy.clamp_workers(args.workers, len(days))
    args_dict = _args_dict(args)
    completed = []
    if workers > 1:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(build_day, day.isoformat(), args_dict): day for day in days}
            for fut in as_completed(futures):
                row = fut.result()
                completed.append(row)
                print(f"[latency-shards] {row['day']} status={row['status']} outcomes={row.get('outcome_rows')}", flush=True)
    else:
        for day in days:
            row = build_day(day.isoformat(), args_dict)
            completed.append(row)
            print(f"[latency-shards] {row['day']} status={row['status']} outcomes={row.get('outcome_rows')}", flush=True)
    completed.sort(key=lambda row: row['day'])
    print(json.dumps({
        'days': len(completed),
        'artifact_dir': os.path.abspath(args.artifact_dir),
        'step2_latency_mode': args.step2_latency_mode,
        'step2_latency_model': os.path.abspath(args.step2_latency_model),
        'step2_latency_model_hash': replay_artifacts.latency_model_hash(args.step2_latency_model),
        'step2_latency_percentile': args.step2_latency_percentile,
        'executable_trade_filter_version': replay_artifacts.EXECUTABLE_TRADE_FILTER_VERSION,
        'exit_replay_model': replay_artifacts.EXIT_REPLAY_MODEL_VERSION,
        'non_executable_trade_conditions': sorted(replay_artifacts.NON_EXECUTABLE_TRADE_CONDITIONS),
        'rows': completed,
    }, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
