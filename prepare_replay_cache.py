"""Build reusable per-day replay event tapes for full engine replays."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed

import backtest_30d_engine as replay
import worker_policy


def _worker(args_dict: dict, day_iso: str) -> dict:
    args = argparse.Namespace(**args_dict)
    day = replay._parse_day(day_iso)
    stats = replay.ReplayStats()
    events = replay._build_day_events(day, args, stats)
    return {
        'day': day_iso,
        'events': len(events),
        'path': replay._prepared_tape_path(args, day),
        'cache_hits': dict(stats.cache_hits),
        'fetched': dict(stats.fetched),
    }


def parse_args() -> argparse.Namespace:
    start, end = replay._default_dates()
    ap = argparse.ArgumentParser(description='Prebuild gzipped replay event tapes from cached Alpaca data.')
    ap.add_argument('--start', default=start.isoformat())
    ap.add_argument('--end', default=end.isoformat())
    ap.add_argument('--tickers', nargs='+', default=replay.TICKERS)
    ap.add_argument('--feed', default='sip', choices=['sip', 'iex', 'delayed_sip'])
    ap.add_argument('--quote-mode', default='per-second', choices=['per-second', 'all', 'off'])
    ap.add_argument('--btc-mode', default='bars', choices=['bars', 'off'])
    ap.add_argument('--cache-dir', default=replay.DEFAULT_CACHE_DIR)
    ap.add_argument('--prepared-cache-dir', default=replay.DEFAULT_PREPARED_DIR)
    ap.add_argument('--refresh', action='store_true', help='Refetch raw Alpaca data before building tapes.')
    ap.add_argument('--refresh-prepared-events', action='store_true', help='Rebuild existing prepared tapes.')
    ap.add_argument('--max-pages', type=int, default=1000)
    ap.add_argument('--workers', type=int, default=1)
    ap.add_argument('--build-artifacts', action='store_true',
                    help='After prepared event tapes are built, also build reusable replay price/outcome artifacts.')
    ap.add_argument('--artifact-outcome-mode', choices=['decision', 'all-seconds'], default='decision')
    return ap.parse_args()


def _args_dict(args: argparse.Namespace) -> dict:
    return {
        'start': args.start,
        'end': args.end,
        'tickers': list(args.tickers),
        'feed': args.feed,
        'quote_mode': args.quote_mode,
        'btc_mode': args.btc_mode,
        'cache_dir': args.cache_dir,
        'prepared_cache_dir': args.prepared_cache_dir,
        'refresh': args.refresh,
        'refresh_prepared_events': args.refresh_prepared_events,
        'max_pages': args.max_pages,
        'use_prepared_events': False,
        'write_prepared_events': True,
    }


def main() -> int:
    args = parse_args()
    args.tickers = [t.upper() for t in args.tickers]
    start = replay._parse_day(args.start)
    end = replay._parse_day(args.end)
    days = replay._market_days(start, end)
    if not days:
        raise SystemExit('no market days in requested range')

    completed = []
    workers = worker_policy.clamp_workers(args.workers, len(days))
    if workers > 1:
        args_dict = _args_dict(args)
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_worker, args_dict, day.isoformat()): day for day in days}
            for fut in as_completed(futures):
                row = fut.result()
                completed.append(row)
                print(f"[prepare-replay-cache] {row['day']} events={row['events']} path={row['path']}", flush=True)
    else:
        args_dict = _args_dict(args)
        for day in days:
            row = _worker(args_dict, day.isoformat())
            completed.append(row)
            print(f"[prepare-replay-cache] {row['day']} events={row['events']} path={row['path']}", flush=True)

    completed.sort(key=lambda r: r['day'])
    total_events = sum(int(r.get('events') or 0) for r in completed)
    artifact_step = None
    if args.build_artifacts:
        cmd = [
            sys.executable,
            os.path.join(os.path.dirname(os.path.abspath(__file__)), 'build_replay_artifacts.py'),
            '--start', args.start,
            '--end', args.end,
            '--tickers', *args.tickers,
            '--feed', args.feed,
            '--quote-mode', args.quote_mode,
            '--btc-mode', args.btc_mode,
            '--cache-dir', args.cache_dir,
            '--prepared-cache-dir', args.prepared_cache_dir,
            '--outcome-mode', args.artifact_outcome_mode,
            '--workers', str(workers),
        ]
        proc = subprocess.run(cmd, cwd=os.path.dirname(os.path.abspath(__file__)), text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        artifact_step = {
            'command': cmd,
            'returncode': proc.returncode,
            'ok': proc.returncode == 0,
            'output_tail': proc.stdout[-6000:],
        }
    print(json.dumps({
        'days': len(completed),
        'events': total_events,
        'prepared_cache_dir': os.path.abspath(args.prepared_cache_dir),
        'artifact_step': artifact_step,
        'rows': completed,
    }, indent=2))
    return 1 if artifact_step and not artifact_step.get('ok') else 0


if __name__ == '__main__':
    raise SystemExit(main())
