from __future__ import annotations

from output_paths import output_path

import argparse
import gzip
import json
import os
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore

import live_step2_feed
import artifact_version_registry
import step2_cache_layers
import step2_latency_model
import step2_rebuild_planner
import worker_policy


HERE = os.path.dirname(os.path.abspath(__file__))
CT = ZoneInfo('America/Chicago')


def _decision_tape_path(day: str, tickers: list[str], feed: str, quote_mode: str,
                        btc_mode: str, indicator_mode: str) -> str:
    tickers_part = '-'.join(tickers)
    return os.path.join(
        HERE,
        'postmortem', 'backtests', 'decision_tapes',
        f'decision_tape_{feed}_{quote_mode}_{btc_mode}_{indicator_mode}_{tickers_part}_{day}.jsonl.gz',
    )


def _decision_tape_watermark(path: str) -> dict:
    if not os.path.exists(path):
        return {'exists': False, 'rows': 0, 'max_ts': 0, 'min_ts': 0}
    rows = 0
    min_ts = 0
    max_ts = 0
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            ts = int(row.get('ts') or 0)
            if ts:
                min_ts = ts if not min_ts else min(min_ts, ts)
                max_ts = max(max_ts, ts)
            rows += 1
    return {'exists': True, 'rows': rows, 'max_ts': max_ts, 'min_ts': min_ts}


def _prepared_watermark(path: str) -> dict:
    if not os.path.exists(path):
        return {'exists': False, 'rows': 0, 'max_ts_ms': 0, 'min_ts_ms': 0}
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        rows = json.load(f)
    min_ts = min((int(row.get('t') or 0) for row in rows), default=0)
    max_ts = max((int(row.get('t') or 0) for row in rows), default=0)
    return {
        'exists': True,
        'rows': len(rows),
        'min_ts_ms': min_ts,
        'max_ts_ms': max_ts,
        'min_ct': datetime.fromtimestamp(min_ts / 1000.0, CT).isoformat() if min_ts else None,
        'max_ct': datetime.fromtimestamp(max_ts / 1000.0, CT).isoformat() if max_ts else None,
    }


def _near_signal_summary(day: str, start_ts: int = 0) -> dict:
    path = output_path('postmortem', 'near_signals', f'near_signals_{day}.jsonl')
    if not os.path.exists(path):
        return {'exists': False, 'rows': 0}
    reasons = Counter()
    tickers = Counter()
    sides = Counter()
    rows = 0
    max_ts = 0
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            ts = int(row.get('created_at') or row.get('ts') or 0)
            if start_ts and ts < start_ts:
                continue
            rows += 1
            max_ts = max(max_ts, ts)
            reasons[str(row.get('reason') or 'unknown')] += 1
            tickers[str(row.get('ticker') or row.get('symbol') or 'unknown')] += 1
            sides[str(row.get('side') or row.get('chosen_side') or 'unknown')] += 1
    return {
        'exists': True,
        'rows': rows,
        'max_ts': max_ts,
        'max_ct': datetime.fromtimestamp(max_ts, CT).isoformat() if max_ts else None,
        'top_reasons': [{'reason': k, 'count': v} for k, v in reasons.most_common(15)],
        'tickers': [{'ticker': k, 'count': v} for k, v in tickers.most_common()],
        'sides': [{'side': k, 'count': v} for k, v in sides.most_common()],
    }


def _write_refresh_report(day: str, payload: dict) -> str:
    out_dir = output_path('postmortem', 'step2_freshness')
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f'step2_freshness_{day}.json')
    tmp = f'{path}.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    os.replace(tmp, path)
    return path


def _patch_manifest_freshness(manifest_path: str, freshness: dict) -> None:
    if not os.path.exists(manifest_path):
        return
    with open(manifest_path, 'r', encoding='utf-8') as f:
        manifest = json.load(f)
    manifest['step2_freshness'] = freshness
    tmp = f'{manifest_path}.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
    os.replace(tmp, manifest_path)


def _run(cmd: list[str]) -> None:
    print(json.dumps({'event': 'run', 'cmd': cmd}), flush=True)
    subprocess.run(cmd, cwd=HERE, check=True)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Refresh current-day Step 2 live/mock-parity compiled tape.')
    ap.add_argument('--day', default=datetime.now(CT).date().isoformat())
    ap.add_argument('--tickers', nargs='+', default=['CLSK', 'MARA', 'RIOT'])
    ap.add_argument('--feed', default='sip')
    ap.add_argument('--quote-mode', default='per-second')
    ap.add_argument('--btc-mode', default='bars')
    ap.add_argument('--indicator-mode', choices=['live', 'fast'], default='live')
    ap.add_argument('--workers', type=int, default=worker_policy.DEFAULT_MAX_WORKERS)
    ap.add_argument('--name', default='')
    ap.add_argument('--no-existing', action='store_true')
    ap.add_argument('--materialize-only', action='store_true',
                    help='Only merge live-captured events into the provisional intraday replay tape.')
    ap.add_argument('--full-rebuild', action='store_true',
                    help='Rebuild the whole current day decision tape instead of appending from the watermark.')
    ap.add_argument('--reuse-existing-signals', action='store_true',
                    help='Reuse existing decision-tape signal/features rows and refresh only outcomes.')
    ap.add_argument('--overlap-sec', type=int, default=120,
                    help='Incremental rebuild overlap before the current decision-tape watermark.')
    ap.add_argument('--no-state-checkpoints', dest='use_state_checkpoints', action='store_false',
                    help='Disable exact replay state checkpoints for incremental signal scans.')
    ap.set_defaults(use_state_checkpoints=True)
    ap.add_argument('--loop', action='store_true',
                    help='Refresh repeatedly for current-day intraday Step 2.')
    ap.add_argument('--interval-sec', type=int, default=300)
    ap.add_argument('--step2-latency-mode', choices=['off', 'entry', 'entry-exit'], default='entry-exit')
    ap.add_argument('--step2-latency-model', default=step2_latency_model.DEFAULT_MODEL_PATH)
    ap.add_argument('--step2-latency-percentile', choices=['p50', 'p75', 'p95', 'default'], default='p75')
    return ap.parse_args()


def refresh_once(args: argparse.Namespace) -> dict:
    tickers = [ticker.upper() for ticker in args.tickers]
    materialized = live_step2_feed.materialize(
        day=args.day,
        tickers=tickers,
        feed=args.feed,
        quote_mode=args.quote_mode,
        btc_mode=args.btc_mode,
        include_existing=not args.no_existing,
        skip_if_fresh=True,
    )
    if args.materialize_only:
        payload = {
            'event': 'intraday_step2_materialized',
            'day': args.day,
            'tickers': tickers,
            'materialized': materialized,
        }
        print(json.dumps(payload, indent=2, sort_keys=True), flush=True)
        return payload
    name = args.name or f"compiled_step2_live_mockparity_{'-'.join(tickers)}_{args.day}_intraday"
    rebuild_plan = step2_rebuild_planner.plan(
        day=args.day,
        tickers=tickers,
        feed=args.feed,
        quote_mode=args.quote_mode,
        btc_mode=args.btc_mode,
        indicator_mode=args.indicator_mode,
        compiled_name=name,
        materialized=materialized,
        overlap_sec=int(args.overlap_sec or 0),
        force_full=bool(args.full_rebuild),
        force_reuse_existing_signals=bool(args.reuse_existing_signals),
    )
    if rebuild_plan.get('score_only'):
        manifest = os.path.join(
            HERE,
            'postmortem', 'backtests', 'compiled_decision_tapes', name, 'manifest.json',
        )
        payload = {
            'event': 'intraday_step2_refresh_skipped_score_only',
            'day': args.day,
            'tickers': tickers,
            'worker_policy': worker_policy.describe_policy(),
            'workers_requested': args.workers,
            'workers_effective': worker_policy.clamp_workers(args.workers),
            'materialized': materialized,
            'rebuild_plan': rebuild_plan,
            'compiled_manifest': os.path.abspath(manifest),
        }
        payload['freshness_report'] = os.path.abspath(_write_refresh_report(args.day, payload))
        print(json.dumps(payload, indent=2, sort_keys=True), flush=True)
        return payload
    tape_path = _decision_tape_path(
        args.day, tickers, args.feed, args.quote_mode, args.btc_mode, args.indicator_mode,
    )
    watermark = _decision_tape_watermark(tape_path)
    decision_start_ts = 0
    append_existing = False
    incremental_store = materialized.get('incremental_market_store') or {}
    changed_partitions = incremental_store.get('changed_partitions') or []
    earliest_changed_ts = min(
        (int(row.get('bucket_start') or 0) for row in changed_partitions if row.get('bucket_start')),
        default=0,
    )
    if rebuild_plan.get('reuse_existing_signals'):
        args.reuse_existing_signals = True
    if rebuild_plan.get('full_rebuild'):
        args.full_rebuild = True
    compile_only = bool(rebuild_plan.get('recompile_only'))
    if rebuild_plan.get('append_existing') and int(rebuild_plan.get('decision_start_ts') or 0) > 0:
        decision_start_ts = int(rebuild_plan.get('decision_start_ts') or 0)
        append_existing = True
    elif not args.full_rebuild and not compile_only and watermark.get('max_ts'):
        overlap = max(0, int(args.overlap_sec or 0))
        watermark_start = max(0, int(watermark['max_ts']) - overlap)
        changed_start = max(0, earliest_changed_ts - overlap) if earliest_changed_ts else 0
        decision_start_ts = min(watermark_start, changed_start) if changed_start else watermark_start
        append_existing = True
    _run([
        sys.executable, 'prepare_step2_live_cache.py',
        '--start', args.day,
        '--end', args.day,
        '--tickers', *tickers,
        '--feed', args.feed,
        '--quote-mode', args.quote_mode,
        '--btc-mode', args.btc_mode,
        '--indicator-mode', args.indicator_mode,
        '--workers', str(worker_policy.clamp_workers(args.workers)),
        '--prepared-cache-dir', live_step2_feed.DEFAULT_PREPARED_DIR,
        '--name', name,
        '--step2-latency-mode', args.step2_latency_mode,
        '--step2-latency-model', args.step2_latency_model,
        '--step2-latency-percentile', args.step2_latency_percentile,
    ] + (
        ['--decision-start-ts', str(decision_start_ts), '--append-existing']
        if append_existing and not args.reuse_existing_signals else []
    ) + (
        ['--compile-only'] if compile_only else []
    ) + (
        ['--reuse-existing-signals'] if args.reuse_existing_signals else []
    ) + (
        ['--use-state-checkpoints'] if bool(getattr(args, 'use_state_checkpoints', True)) and append_existing else []
    ))
    manifest = os.path.join(
        HERE,
        'postmortem', 'backtests', 'compiled_decision_tapes', name, 'manifest.json',
    )
    prepared = _prepared_watermark(materialized.get('prepared_path') or '')
    decision_after = _decision_tape_watermark(tape_path)
    near_after_decision = _near_signal_summary(args.day, int(decision_after.get('max_ts') or 0))
    freshness = {
        'created_at_ct': datetime.now(CT).isoformat(),
        'prepared_event_tape': prepared,
        'decision_tape': decision_after,
        'near_signals_after_decision_watermark': near_after_decision,
        'deduction': (
            'Prepared event tape is raw market-data freshness. Decision tape max_ts is latest accepted '
            'Step 2 opportunity. Near-signal rows after decision max_ts explain no-trade periods.'
        ),
    }
    _patch_manifest_freshness(manifest, freshness)
    payload = {
        'event': 'intraday_step2_refreshed',
        'day': args.day,
        'tickers': tickers,
        'worker_policy': worker_policy.describe_policy(),
        'workers_requested': args.workers,
        'workers_effective': worker_policy.clamp_workers(args.workers),
        'materialized': materialized,
        'decision_tape_watermark_before': watermark,
        'incremental': {
            'enabled': append_existing,
            'decision_start_ts': decision_start_ts,
            'overlap_sec': int(args.overlap_sec or 0),
            'reuse_existing_signals': bool(args.reuse_existing_signals),
            'compile_only': compile_only,
            'rebuild_plan_action': rebuild_plan.get('action'),
            'rebuild_plan_reason': rebuild_plan.get('reason'),
            'rebuild_plan_path': rebuild_plan.get('path'),
            'incremental_market_store_manifest': incremental_store.get('manifest_path'),
            'changed_partition_count': incremental_store.get('changed_partition_count'),
            'earliest_changed_partition_ts': earliest_changed_ts or None,
        },
        'compiled_manifest': os.path.abspath(manifest),
        'freshness': freshness,
    }
    payload['freshness_report'] = os.path.abspath(_write_refresh_report(args.day, payload))
    try:
        payload['cache_layer_report'] = step2_cache_layers.write_report(
            args.day,
            tickers,
            feed=args.feed,
            quote_mode=args.quote_mode,
            btc_mode=args.btc_mode,
            indicator_mode=args.indicator_mode,
            compiled_name=name,
        )
    except Exception as exc:
        payload['cache_layer_report_error'] = repr(exc)
    try:
        payload['artifact_registry_path'] = artifact_version_registry.write(
            args.day,
            tickers,
            extra_artifacts={
                'intraday_step2_refresh_report': payload.get('freshness_report') or '',
                'compiled_manifest': manifest,
            },
        )
    except Exception as exc:
        payload['artifact_registry_error'] = repr(exc)
    print(json.dumps(payload, indent=2, sort_keys=True), flush=True)
    return payload


def main() -> int:
    args = parse_args()
    if not args.loop:
        refresh_once(args)
        return 0
    while True:
        try:
            refresh_once(args)
        except Exception as exc:
            print(json.dumps({
                'event': 'intraday_step2_refresh_failed',
                'error': repr(exc),
            }), flush=True)
        time.sleep(max(30, int(args.interval_sec or 300)))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
