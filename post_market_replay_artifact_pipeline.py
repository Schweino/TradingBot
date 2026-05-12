"""One-day post-market replay artifact pipeline.

Use this after the daily Alpaca cache fill and single-day replay checkpoint. It
refreshes reusable research artifacts for the day so future scoring/gate runs
can use the fast path immediately.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import subprocess
import sys
import time

import worker_policy


def _run(cmd: list[str], timeout: int) -> dict:
    started = time.perf_counter()
    proc = subprocess.run(
        cmd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
    )
    return {
        'command': cmd,
        'returncode': proc.returncode,
        'ok': proc.returncode == 0,
        'elapsed_seconds': round(time.perf_counter() - started, 3),
        'output_tail': proc.stdout[-6000:],
    }


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Build one-day post-market replay artifacts.')
    ap.add_argument('day')
    ap.add_argument('--tickers', nargs='+', default=['CLSK', 'MARA', 'RIOT'])
    ap.add_argument('--feed', default='sip')
    ap.add_argument('--quote-mode', default='per-second')
    ap.add_argument('--btc-mode', default='bars')
    ap.add_argument('--workers', type=int, default=worker_policy.DEFAULT_MAX_WORKERS)
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    tickers = [ticker.upper() for ticker in args.tickers]
    workers = worker_policy.clamp_workers(args.workers)
    common = [
        '--start', args.day,
        '--end', args.day,
        '--tickers', *tickers,
        '--feed', args.feed,
        '--quote-mode', args.quote_mode,
        '--btc-mode', args.btc_mode,
    ]
    artifact_step = _run([
            sys.executable,
            'build_replay_artifacts.py',
            *common,
            '--outcome-mode', 'decision',
            '--workers', str(workers),
        ], timeout=600)
    latency_outcome_step = _run([
            sys.executable,
            'build_latency_outcome_shards.py',
            *common,
            '--indicator-mode', 'live',
            '--workers', str(workers),
            '--step2-latency-mode', 'entry-exit',
            '--step2-latency-model', os.path.join('postmortem', 'latency_model', 'step2_latency_model.json'),
            '--step2-latency-percentile', 'p75',
        ], timeout=900)
    decision_step = _run([
            sys.executable,
            'build_decision_tape.py',
            *common,
        ], timeout=600)
    tape_pattern = os.path.join(
        'postmortem', 'backtests', 'decision_tapes',
        f"decision_tape_{args.feed}_{args.quote_mode}_{args.btc_mode}_{'-'.join(tickers)}_{args.day}.jsonl.gz",
    )
    tapes = glob.glob(tape_pattern)
    feature_store_step = {
        'command': [],
        'returncode': 1,
        'ok': False,
        'elapsed_seconds': 0,
        'output_tail': f'missing decision tape for feature store: {tape_pattern}',
    }
    if tapes:
        feature_store_step = _run([
            sys.executable,
            'scoring_feature_store.py',
            '--source-kind', 'decision-tape',
            '--tapes', *tapes,
            '--name', f"decision_tape_{args.feed}_{args.quote_mode}_{args.btc_mode}_{'-'.join(tickers)}_{args.day}",
        ], timeout=600)
    compiled_tape_step = {
        'command': [],
        'returncode': 1,
        'ok': False,
        'elapsed_seconds': 0,
        'output_tail': f'missing decision tape for compiled tape cache: {tape_pattern}',
    }
    if tapes:
        compiled_tape_step = _run([
            sys.executable,
            'decision_tape_compiled.py',
            '--start', args.day,
            '--end', args.day,
            '--tickers', *tickers,
            '--feed', args.feed,
            '--quote-mode', args.quote_mode,
            '--btc-mode', args.btc_mode,
            '--name', f"compiled_decision_tape_{args.feed}_{args.quote_mode}_{args.btc_mode}_{'-'.join(tickers)}_{args.day}",
        ], timeout=600)
    steps = [
        artifact_step,
        latency_outcome_step,
        decision_step,
        feature_store_step,
        compiled_tape_step,
        _run([
            sys.executable,
            'replay_artifact_status.py',
            *common,
        ], timeout=180),
    ]
    payload = {
        'day': args.day,
        'tickers': tickers,
        'ok': all(step.get('ok') for step in steps),
        'steps': steps,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload['ok'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
