"""Build and compile live-indicator decision tapes for fast Step 2 scoring."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys

import build_decision_tape
import decision_tape_compiled
import step2_latency_model
import worker_policy


HERE = os.path.dirname(os.path.abspath(__file__))


def _run(cmd: list[str]) -> None:
    print(json.dumps({'event': 'run', 'cmd': cmd}), flush=True)
    subprocess.run(cmd, cwd=HERE, check=True)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Prepare live-indicator compiled decision tape for Step 2.')
    ap.add_argument('--start', required=True)
    ap.add_argument('--end', required=True)
    ap.add_argument('--tickers', nargs='+', default=['CLSK', 'MARA', 'RIOT'])
    ap.add_argument('--feed', default='sip')
    ap.add_argument('--quote-mode', default='per-second')
    ap.add_argument('--btc-mode', default='bars')
    ap.add_argument('--indicator-mode', choices=['live', 'fast'], default='live')
    ap.add_argument('--workers', type=int, default=worker_policy.DEFAULT_MAX_WORKERS)
    ap.add_argument('--cache-dir', default=build_decision_tape.replay.DEFAULT_CACHE_DIR)
    ap.add_argument('--prepared-cache-dir', default=build_decision_tape.replay.DEFAULT_PREPARED_DIR)
    ap.add_argument('--tape-dir', default=build_decision_tape.DEFAULT_OUT_DIR)
    ap.add_argument('--compiled-dir', default=decision_tape_compiled.DEFAULT_OUT_DIR)
    ap.add_argument('--name', default='')
    ap.add_argument('--source', choices=['signal-scan', 'opportunity-ledger'], default='signal-scan')
    ap.add_argument('--no-outcome-shards', action='store_true')
    ap.add_argument('--decision-start-ts', type=int, default=0)
    ap.add_argument('--decision-end-ts', type=int, default=0)
    ap.add_argument('--append-existing', action='store_true')
    ap.add_argument('--compile-only', action='store_true',
                    help='Skip decision-tape materialization and only rebuild the compiled arrays/manifest.')
    ap.add_argument('--reuse-existing-signals', action='store_true',
                    help='Reuse existing decision-tape signals/features and refresh only outcomes before compiling.')
    ap.add_argument('--use-state-checkpoints', action='store_true',
                    help='Use exact replay state checkpoints for incremental signal scans.')
    ap.add_argument('--checkpoint-bucket-sec', type=int, default=300)
    ap.add_argument('--step2-latency-mode', choices=['off', 'entry', 'entry-exit'], default='entry-exit')
    ap.add_argument('--step2-latency-model', default=step2_latency_model.DEFAULT_MODEL_PATH)
    ap.add_argument('--step2-latency-percentile', choices=['p50', 'p75', 'p95', 'default'], default='p75')
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    tickers = [ticker.upper() for ticker in args.tickers]
    name = args.name or (
        f"compiled_decision_tape_{args.feed}_{args.quote_mode}_{args.btc_mode}_"
        f"{args.indicator_mode}_{'-'.join(tickers)}_{args.start}_{args.end}"
    )
    build_cmd = [
        sys.executable, 'build_decision_tape.py',
        '--start', args.start,
        '--end', args.end,
        '--tickers', *tickers,
        '--feed', args.feed,
        '--quote-mode', args.quote_mode,
        '--btc-mode', args.btc_mode,
        '--indicator-mode', args.indicator_mode,
        '--source', args.source,
        '--workers', str(worker_policy.clamp_workers(args.workers)),
        '--cache-dir', args.cache_dir,
        '--prepared-cache-dir', args.prepared_cache_dir,
        '--out-dir', args.tape_dir,
        '--step2-latency-mode', args.step2_latency_mode,
        '--step2-latency-model', args.step2_latency_model,
        '--step2-latency-percentile', args.step2_latency_percentile,
    ]
    if args.no_outcome_shards:
        build_cmd.append('--no-outcome-shards')
    if int(args.decision_start_ts or 0) > 0:
        build_cmd.extend(['--decision-start-ts', str(int(args.decision_start_ts))])
    if int(args.decision_end_ts or 0) > 0:
        build_cmd.extend(['--decision-end-ts', str(int(args.decision_end_ts))])
    if args.append_existing:
        build_cmd.append('--append-existing')
    if args.reuse_existing_signals:
        build_cmd.append('--reuse-existing-signals')
    if args.use_state_checkpoints:
        build_cmd.append('--use-state-checkpoints')
        build_cmd.extend(['--checkpoint-bucket-sec', str(int(args.checkpoint_bucket_sec or 300))])
    if not args.compile_only:
        _run(build_cmd)

    compile_cmd = [
        sys.executable, 'decision_tape_compiled.py',
        '--start', args.start,
        '--end', args.end,
        '--tickers', *tickers,
        '--feed', args.feed,
        '--quote-mode', args.quote_mode,
        '--btc-mode', args.btc_mode,
        '--indicator-mode', args.indicator_mode,
        '--tape-dir', args.tape_dir,
        '--out-dir', args.compiled_dir,
        '--name', name,
    ]
    _run(compile_cmd)
    manifest = os.path.join(args.compiled_dir, name, 'manifest.json')
    print(json.dumps({
        'event': 'done',
        'manifest': os.path.abspath(manifest),
        'step2_arg': f'--compiled-decision-tape {os.path.abspath(manifest)}',
    }, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
