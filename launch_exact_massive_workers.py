"""Launch local workers for an exact massive scoring run.

This is an orchestration helper only. Workers still use
scoring_variant_lab_exact_massive.py, SQLite chunk leases, run hashes, and
canary checks. It just starts several independent worker processes, polls
status, and optionally merges/reports when complete.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time

import worker_policy


HERE = os.path.dirname(os.path.abspath(__file__))
RUNNER = os.path.join(HERE, 'scoring_variant_lab_exact_massive.py')


def _run_json(cmd: list[str], timeout: int = 120) -> dict:
    proc = subprocess.run(cmd, cwd=HERE, text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, timeout=timeout)
    try:
        payload = json.loads(proc.stdout)
    except Exception:
        payload = {'raw_output': proc.stdout}
    payload['_returncode'] = proc.returncode
    return payload


def _status(run_dir: str) -> dict:
    return _run_json([sys.executable, RUNNER, '--mode', 'status', '--run-dir', run_dir], timeout=60).get('status') or {}


def _done(status: dict) -> bool:
    forecast = status.get('forecast') or {}
    total = int(forecast.get('total_chunks') or 0)
    complete = int(forecast.get('complete_chunks') or 0)
    return total > 0 and complete >= total


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Launch safe local exact-scoring workers.')
    ap.add_argument('--run-dir', required=True)
    ap.add_argument('--workers', type=int, default=4)
    ap.add_argument('--chunks-per-worker', type=int, default=1)
    ap.add_argument('--poll-seconds', type=float, default=15.0)
    ap.add_argument('--reset-stale-running-minutes', type=float, default=30.0)
    ap.add_argument('--merge-when-done', action='store_true')
    ap.add_argument('--report-when-done', action='store_true')
    ap.add_argument('--allow-hash-mismatch', action='store_true')
    ap.add_argument('--dry-run', action='store_true')
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    run_dir = os.path.abspath(args.run_dir)
    workers = worker_policy.clamp_workers(args.workers)
    common = [sys.executable, RUNNER, '--mode', 'run', '--run-dir', run_dir,
              '--max-chunks', str(max(1, int(args.chunks_per_worker or 1))),
              '--workers', '1', '--no-run-lock']
    if args.allow_hash_mismatch:
        common.append('--allow-hash-mismatch')

    print(json.dumps({'event': 'launcher_start', 'run_dir': run_dir, 'workers': workers}, sort_keys=True), flush=True)
    if args.dry_run:
        print(json.dumps({'event': 'dry_run', 'commands': [
            [*common, '--worker-id', f'local-{idx + 1:02d}'] for idx in range(workers)
        ]}, indent=2, sort_keys=True))
        return 0

    while True:
        if args.reset_stale_running_minutes and args.reset_stale_running_minutes > 0:
            reset_cmd = [
                sys.executable, RUNNER, '--mode', 'reset', '--run-dir', run_dir,
                '--reset-stale-running-minutes', str(args.reset_stale_running_minutes),
            ]
            _run_json(reset_cmd, timeout=60)
        status = _status(run_dir)
        print(json.dumps({'event': 'status', **(status.get('forecast') or {})}, sort_keys=True), flush=True)
        if _done(status):
            break

        procs = []
        for idx in range(workers):
            cmd = [*common, '--worker-id', f'local-{idx + 1:02d}']
            procs.append(subprocess.Popen(cmd, cwd=HERE, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT))
        for idx, proc in enumerate(procs, 1):
            out, _ = proc.communicate()
            print(json.dumps({
                'event': 'worker_done',
                'worker_id': f'local-{idx:02d}',
                'returncode': proc.returncode,
                'output_tail': (out or '')[-2000:],
            }, sort_keys=True), flush=True)
        time.sleep(max(1.0, float(args.poll_seconds or 15.0)))

    if args.merge_when_done:
        merge_cmd = [sys.executable, RUNNER, '--mode', 'merge', '--run-dir', run_dir]
        if args.allow_hash_mismatch:
            merge_cmd.append('--allow-hash-mismatch')
        print(json.dumps({'event': 'merge', 'result': _run_json(merge_cmd, timeout=600)}, sort_keys=True), flush=True)
    if args.report_when_done:
        report_cmd = [sys.executable, RUNNER, '--mode', 'report', '--run-dir', run_dir]
        if args.allow_hash_mismatch:
            report_cmd.append('--allow-hash-mismatch')
        print(json.dumps({'event': 'report', 'result': _run_json(report_cmd, timeout=180)}, sort_keys=True), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
