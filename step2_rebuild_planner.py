from __future__ import annotations

import gzip
import json
import os
import time
from datetime import datetime
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import compiled_chunk_store
import replay_artifacts
import step2_artifact_identity
import step2_cache_layers


HERE = os.path.dirname(os.path.abspath(__file__))
CT = ZoneInfo('America/Chicago')
OUT_DIR = os.path.join(HERE, 'postmortem', 'rebuild_plans')
SCHEMA_VERSION = 1
ACTION_COST = {
    'score_only': {'rank': 0, 'estimate_sec': 1, 'quality': 'certified_existing_cache'},
    'score_only_uncertified': {'rank': 1, 'estimate_sec': 1, 'quality': 'quick_score_requires_recertification'},
    'recompile_only': {'rank': 2, 'estimate_sec': 45, 'quality': 'reuse_decision_tapes'},
    'range_link_only': {'rank': 2, 'estimate_sec': 2, 'quality': 'reuse_certified_day_shards'},
    'refresh_outcomes_compile': {'rank': 3, 'estimate_sec': 30, 'quality': 'reuse_signal_rows_refresh_outcomes'},
    'incremental_append': {'rank': 4, 'estimate_sec': 90, 'quality': 'checkpointed_incremental_signal_scan'},
    'full_signal_rebuild': {'rank': 5, 'estimate_sec': 900, 'quality': 'full_exact_replay'},
}


def _read_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return default


def _write_json(path: str, payload: dict[str, Any]) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f'{path}.{os.getpid()}.{int(time.time() * 1000)}.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
    os.replace(tmp, path)
    return os.path.abspath(path)


def _decision_tape_watermark(path: str) -> dict[str, Any]:
    try:
        identity = step2_artifact_identity.jsonl_gz_identity(path)
        return {
            'exists': identity.get('exists'),
            'rows': identity.get('rows'),
            'min_ts': identity.get('min_ts'),
            'max_ts': identity.get('max_ts'),
            'physical_sha256': identity.get('physical_sha256'),
            'semantic_sha256': identity.get('semantic_sha256'),
        }
    except Exception as exc:
        return {'exists': os.path.exists(path), 'rows': 0, 'min_ts': 0, 'max_ts': 0, 'error': repr(exc)}


def _affected_decision_window(watermark: dict[str, Any], changed: list[dict[str, Any]], overlap_sec: int) -> dict[str, Any]:
    if not changed:
        return {
            'changed': False,
            'decision_start_ts': 0,
            'decision_end_ts': 0,
            'overlap_sec': int(overlap_sec or 0),
            'reason': 'no_market_partitions_changed',
        }
    earliest = min((int(row.get('bucket_start') or 0) for row in changed if row.get('bucket_start')), default=0)
    latest = max((int(row.get('bucket_start') or 0) for row in changed if row.get('bucket_start')), default=0)
    start = max(0, earliest - int(overlap_sec or 0)) if earliest else 0
    min_ts = int(watermark.get('min_ts') or 0)
    max_ts = int(watermark.get('max_ts') or 0)
    if min_ts and start:
        start = max(min_ts, start)
    return {
        'changed': True,
        'earliest_changed_partition_ts': earliest or None,
        'latest_changed_partition_ts': latest or None,
        'decision_start_ts': start,
        'decision_end_ts': max_ts,
        'overlap_sec': int(overlap_sec or 0),
        'reason': 'market_diff_mapped_to_decision_window',
    }


def _outcome_coverage(
    *,
    day: str,
    tickers: list[str],
    decision_tape_path: str,
    artifact_dir: str,
    feed: str,
    quote_mode: str,
    btc_mode: str,
    step2_latency_mode: str,
    step2_latency_percentile: str,
    step2_latency_model: str | None,
) -> dict[str, Any]:
    if not os.path.exists(decision_tape_path):
        return {'checked': False, 'reason': 'decision_tape_missing'}
    rows = step2_artifact_identity.read_jsonl_gz(decision_tape_path)
    needed: dict[str, set[int]] = {ticker: set() for ticker in tickers}
    for row in rows:
        ticker = str(row.get('ticker') or '').upper()
        ts = int(row.get('ts') or 0)
        if ticker in needed and ts:
            needed[ticker].add(ts)
    key = replay_artifacts.ArtifactKey(feed, quote_mode, btc_mode, tuple(tickers))
    day_obj = datetime.strptime(day, '%Y-%m-%d').date()
    missing_by_ticker: dict[str, int] = {}
    covered = 0
    needed_total = 0
    for ticker in tickers:
        if step2_latency_mode != 'off':
            shard = replay_artifacts.read_latency_outcome_shard(
                artifact_dir, key, day_obj, ticker, step2_latency_mode, step2_latency_percentile, step2_latency_model,
            )
        else:
            shard = replay_artifacts.read_outcome_shard(artifact_dir, key, day_obj, ticker)
        shard_keys = set((shard or {}).keys())
        need = needed.get(ticker) or set()
        missing = need - shard_keys
        needed_total += len(need)
        covered += len(need) - len(missing)
        missing_by_ticker[ticker] = len(missing)
    ratio = (covered / needed_total) if needed_total else 1.0
    return {
        'checked': True,
        'decision_rows': len(rows),
        'needed_entries': needed_total,
        'covered_entries': covered,
        'coverage_ratio': round(ratio, 6),
        'complete': covered == needed_total,
        'missing_by_ticker': missing_by_ticker,
        'mode': step2_latency_mode,
    }


def _cost(action: str, day_count: int = 1, *, outcome_coverage: dict[str, Any] | None = None) -> dict[str, Any]:
    base = dict(ACTION_COST.get(action) or ACTION_COST['full_signal_rebuild'])
    estimate = float(base.get('estimate_sec') or 0)
    if action in ('full_signal_rebuild', 'incremental_append', 'refresh_outcomes_compile'):
        estimate *= max(1, int(day_count or 1))
    if action == 'refresh_outcomes_compile' and outcome_coverage and outcome_coverage.get('complete'):
        estimate = min(estimate, 10.0 * max(1, int(day_count or 1)))
        base['quality'] = 'reuse_signal_rows_with_complete_outcome_shards'
    base['estimate_sec'] = round(estimate, 3)
    return base


def _paths(day: str, tickers: list[str], feed: str, quote_mode: str, btc_mode: str,
           indicator_mode: str, compiled_name: str) -> dict[str, str]:
    ticker_part = '-'.join(tickers)
    return {
        'decision_tape': os.path.join(
            HERE, 'postmortem', 'backtests', 'decision_tapes',
            f'decision_tape_{feed}_{quote_mode}_{btc_mode}_{indicator_mode}_{ticker_part}_{day}.jsonl.gz',
        ),
        'compiled_manifest': os.path.join(
            HERE, 'postmortem', 'backtests', 'compiled_decision_tapes', compiled_name, 'manifest.json',
        ),
        'incremental_market_store': os.path.join(
            HERE, 'data_cache', 'incremental_market_store', day, 'manifest.json',
        ),
    }


def plan(
    day: str,
    tickers: list[str],
    feed: str = 'sip',
    quote_mode: str = 'per-second',
    btc_mode: str = 'bars',
    indicator_mode: str = 'live',
    compiled_name: str | None = None,
    materialized: dict[str, Any] | None = None,
    overlap_sec: int = 120,
    force_full: bool = False,
    force_reuse_existing_signals: bool = False,
    use_incremental_market_store: bool = True,
    artifact_dir: str | None = None,
    step2_latency_mode: str = 'entry-exit',
    step2_latency_percentile: str = 'p75',
    step2_latency_model: str | None = None,
) -> dict[str, Any]:
    tickers = [str(t).upper() for t in tickers]
    compiled_name = compiled_name or f"compiled_step2_live_mockparity_{'-'.join(tickers)}_{day}_intraday"
    paths = _paths(day, tickers, feed, quote_mode, btc_mode, indicator_mode, compiled_name)
    cache_report = step2_cache_layers.report(
        day,
        tickers,
        feed=feed,
        quote_mode=quote_mode,
        btc_mode=btc_mode,
        indicator_mode=indicator_mode,
        compiled_name=compiled_name,
    )
    market_manifest = {}
    if use_incremental_market_store:
        market_manifest = (
            (materialized or {}).get('incremental_market_store')
            or _read_json(paths['incremental_market_store'], {}) or {}
        )
    changed = compiled_chunk_store.changed_buckets_from_market_store(market_manifest)
    earliest_changed = min((int(row.get('bucket_start') or 0) for row in changed), default=0)
    watermark = _decision_tape_watermark(paths['decision_tape'])
    decision_window = _affected_decision_window(watermark, changed, int(overlap_sec or 0))
    cache_action = cache_report.get('recommended_action') or 'unknown'
    lineage = cache_report.get('compiled_tape_lineage') if isinstance(cache_report.get('compiled_tape_lineage'), dict) else {}
    stale_layers = list(cache_report.get('stale_layers') or [])
    decision_start_ts = 0
    action = 'score_only'
    reason = 'compiled_cache_fresh'

    if force_full:
        action = 'full_signal_rebuild'
        reason = 'forced_full_rebuild'
    elif force_reuse_existing_signals:
        action = 'refresh_outcomes_compile'
        reason = 'forced_reuse_existing_signals'
    elif not watermark.get('exists') or int(watermark.get('rows') or 0) <= 0:
        action = 'full_signal_rebuild'
        reason = 'decision_tape_missing'
    elif cache_action in ('score_only', 'score_only_uncertified', 'scorer_recertification') and not changed:
        action = 'score_only'
        if cache_action in ('score_only_uncertified', 'scorer_recertification'):
            action = 'score_only_uncertified'
            reason = (
                'scorer_runtime_requires_recertification'
                if cache_action == 'scorer_recertification'
                else 'lineage_allows_quick_score_but_not_certified'
            )
        else:
            reason = 'no_market_or_code_changes'
    elif cache_action == 'recompile_only':
        action = 'recompile_only'
        reason = 'lineage_requires_recompile_only'
    elif cache_action in ('refresh_outcomes_compile', 'reuse_existing_signals_refresh_outcomes_compile'):
        action = 'refresh_outcomes_compile'
        reason = 'lineage_requires_outcome_refresh_compile'
    elif cache_action in ('full_signal_rebuild', 'full_rebuild'):
        action = 'full_signal_rebuild'
        reason = 'lineage_requires_full_signal_rebuild'
    elif changed and watermark.get('max_ts'):
        action = 'incremental_append'
        reason = 'market_partitions_changed'
        changed_start = max(0, int(earliest_changed) - int(overlap_sec or 0)) if earliest_changed else 0
        watermark_start = max(0, int(watermark.get('max_ts') or 0) - int(overlap_sec or 0))
        decision_start_ts = min(watermark_start, changed_start) if changed_start else watermark_start
    elif stale_layers:
        action = 'full_signal_rebuild'
        reason = 'unclassified_stale_layers'
    else:
        action = 'score_only'
        reason = 'default_fresh'

    if action == 'incremental_append' and decision_window.get('decision_start_ts'):
        decision_start_ts = int(decision_window.get('decision_start_ts') or 0)
    elif action == 'incremental_append' and not decision_start_ts:
        decision_start_ts = max(0, int(watermark.get('max_ts') or 0) - int(overlap_sec or 0))

    outcome_coverage = {'checked': False, 'reason': 'not_needed_for_action'}
    if action == 'refresh_outcomes_compile':
        outcome_coverage = _outcome_coverage(
            day=day,
            tickers=tickers,
            decision_tape_path=paths['decision_tape'],
            artifact_dir=artifact_dir or os.path.join(HERE, 'postmortem', 'backtests', 'replay_artifacts'),
            feed=feed,
            quote_mode=quote_mode,
            btc_mode=btc_mode,
            step2_latency_mode=step2_latency_mode,
            step2_latency_percentile=step2_latency_percentile,
            step2_latency_model=step2_latency_model,
        )

    payload = {
        'schema_version': SCHEMA_VERSION,
        'source': 'step2_rebuild_planner',
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'day': day,
        'tickers': tickers,
        'feed': feed,
        'quote_mode': quote_mode,
        'btc_mode': btc_mode,
        'indicator_mode': indicator_mode,
        'compiled_name': compiled_name,
        'action': action,
        'reason': reason,
        'decision_start_ts': int(decision_start_ts or 0),
        'overlap_sec': int(overlap_sec or 0),
        'reuse_existing_signals': action == 'refresh_outcomes_compile',
        'full_rebuild': action == 'full_signal_rebuild',
        'recompile_only': action == 'recompile_only',
        'append_existing': action == 'incremental_append',
        'score_only': action in ('score_only', 'score_only_uncertified'),
        'range_link_only': action == 'range_link_only',
        'certified_score_only': action == 'score_only',
        'uncertified_score_only': action == 'score_only_uncertified',
        'paths': paths,
        'decision_tape_watermark': watermark,
        'decision_window': decision_window,
        'outcome_coverage': outcome_coverage,
        'cost_estimate': _cost(action, outcome_coverage=outcome_coverage),
        'cache_action': cache_action,
        'lineage_status': lineage.get('status'),
        'lineage_certified': lineage.get('certified'),
        'lineage_quick_score_allowed': lineage.get('quick_score_allowed'),
        'lineage_rebuild_required': lineage.get('rebuild_required'),
        'stale_layers': stale_layers,
        'changed_partition_count': len(changed),
        'use_incremental_market_store': bool(use_incremental_market_store),
        'earliest_changed_partition_ts': earliest_changed or None,
        'changed_partitions': changed[:20],
        'deduction': (
            'This plan chooses the narrowest exact rebuild. It never downgrades market data or indicator mode; '
            'when it cannot classify a change safely, it falls back to full_rebuild.'
        ),
    }
    payload['path'] = _write_json(os.path.join(OUT_DIR, f'step2_rebuild_plan_{day}.json'), payload)
    return payload


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser(description='Plan the narrowest exact Step 2 rebuild path.')
    ap.add_argument('--day', required=True)
    ap.add_argument('--tickers', nargs='+', default=['CLSK', 'MARA', 'RIOT'])
    ap.add_argument('--feed', default='sip')
    ap.add_argument('--quote-mode', default='per-second')
    ap.add_argument('--btc-mode', default='bars')
    ap.add_argument('--indicator-mode', default='live')
    ap.add_argument('--compiled-name', default='')
    ap.add_argument('--overlap-sec', type=int, default=120)
    args = ap.parse_args()
    payload = plan(
        day=args.day,
        tickers=args.tickers,
        feed=args.feed,
        quote_mode=args.quote_mode,
        btc_mode=args.btc_mode,
        indicator_mode=args.indicator_mode,
        compiled_name=args.compiled_name or None,
        overlap_sec=args.overlap_sec,
    )
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
