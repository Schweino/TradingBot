"""Safe acceleration helpers for the four-step variant testing pipeline.

This module is replay/research-only. It does not change live trading logic.

It adds three reusable pieces:

* feature-store matrix scoring for Step 1 experiments
* decision-vector hashes for exact dedupe before Step 2
* partition summaries by day/ticker for robustness checks
"""
from __future__ import annotations

from output_paths import output_path

import argparse
import gzip
import heapq
import json
import os
import time
from collections import defaultdict
from typing import Iterable

import numpy as np

import scoring_compiled_kernels
import scoring_feature_store
import scoring_variant_lab as slow_lab
import scoring_variant_random_access
import scoring_variant_shards
import scoring_variant_lab_massive as massive
import scoring_variant_matrix
import tournament_safety


HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUT_DIR = output_path('postmortem', 'backtests', 'pipeline_accelerator')


def _read_json(path: str) -> dict:
    opener = gzip.open if str(path).endswith('.gz') else open
    with opener(path, 'rt', encoding='utf-8') as f:
        return json.load(f)


def _write_json(path: str, payload: dict) -> str:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    opener = gzip.open if str(path).endswith('.gz') else open
    with opener(path, 'wt', encoding='utf-8') as f:
        json.dump(payload, f, indent=None if str(path).endswith('.gz') else 2, sort_keys=True)
    return path


def _variant_from_row(row: dict) -> slow_lab.Variant:
    return slow_lab.Variant(
        str(row.get('variant') or row.get('name') or 'variant'),
        dict(row.get('weights') or {}),
        float(row.get('bias') or 0.0),
    )


def _load_variants_json(path: str, limit: int | None = None) -> tuple[dict, list[slow_lab.Variant]]:
    payload = _read_json(path)
    rows = payload.get('results') if isinstance(payload, dict) else payload
    rows = list(rows or [])
    if limit:
        rows = rows[:limit]
    return payload if isinstance(payload, dict) else {'results': rows}, [_variant_from_row(row) for row in rows]


def _variant_range(args) -> Iterable[slow_lab.Variant]:
    start = int(args.variant_start or 0)
    end = start + int(args.variant_count)
    if args.variant_shard_dir:
        yield from scoring_variant_shards.load_range(args.variant_shard_dir, start, end)
        return
    yield from scoring_variant_random_access.variant_range(
        start,
        end,
        not args.no_existing,
        not args.no_broad_full,
        not args.no_v29_full,
        args.include_expanded_full,
        args.include_active_local,
    )


def _aggregate_for_store(store: dict) -> dict:
    return scoring_feature_store.aggregate_store(store)


def _load_ids(store: dict) -> dict:
    manifest = store['manifest']
    with gzip.open(manifest['ids_path'], 'rt', encoding='utf-8') as f:
        return json.load(f)


def _decision_hashes_for_rows(store: dict, variants: list[slow_lab.Variant],
                              chunk_rows: int = 4096) -> list[str]:
    import hashlib

    arrays = store['arrays']
    features = np.asarray(arrays['features'], dtype=np.float64)
    original_side = np.asarray(arrays['original_side'])
    weights, bias = massive._variant_matrix(variants)
    hashes = [hashlib.sha256() for _ in variants]
    for start in range(0, features.shape[0], max(1, int(chunk_rows))):
        stop = min(features.shape[0], start + max(1, int(chunk_rows)))
        scores = features[start:stop] @ weights.T
        if bias.size:
            scores = scores + bias.reshape(1, -1)
        chosen = np.where(scores > 0.0, 1, np.where(scores < 0.0, -1, original_side[start:stop].reshape(-1, 1)))
        packed = np.packbits((chosen > 0).astype(np.uint8), axis=0)
        for idx, h in enumerate(hashes):
            h.update(packed[:, idx].tobytes())
    return [h.hexdigest()[:24] for h in hashes]


def _partition_summary(store: dict, variant: slow_lab.Variant) -> dict:
    arrays = store['arrays']
    ids = _load_ids(store)
    features = np.asarray(arrays['features'], dtype=np.float64)
    long_pnl = np.asarray(arrays['long_pnl'], dtype=np.float64)
    short_pnl = np.asarray(arrays['short_pnl'], dtype=np.float64)
    weights, bias = massive._variant_matrix([variant])
    scores = features @ weights[0].reshape(-1, 1)
    scores = scores.reshape(-1) + float(bias[0] if bias.size else 0.0)
    chosen_long = scores > 0.0
    pnl = np.where(chosen_long, long_pnl, short_pnl)
    side = np.where(chosen_long, 'LONG', 'SHORT')

    def add(bucket: dict, idx: int) -> None:
        p = float(pnl[idx])
        bucket['trades'] += 1
        bucket['wins'] += int(p > 0)
        bucket['losses'] += int(p < 0)
        bucket['pnl'] += p
        bucket['longs'] += int(side[idx] == 'LONG')
        bucket['shorts'] += int(side[idx] == 'SHORT')

    by_day = defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0, 'longs': 0, 'shorts': 0})
    by_ticker = defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0, 'longs': 0, 'shorts': 0})
    for idx in range(features.shape[0]):
        add(by_day[str(ids['day'][idx])], idx)
        add(by_ticker[str(ids['ticker'][idx])], idx)
    for rows in (by_day, by_ticker):
        for bucket in rows.values():
            bucket['pnl'] = round(bucket['pnl'], 2)
            bucket['win_rate_pct'] = round(100 * bucket['wins'] / bucket['trades'], 2) if bucket['trades'] else None
    worst_day_key = min(by_day, key=lambda k: by_day[k]['pnl']) if by_day else None
    total_pnl = round(float(pnl.sum()), 2)
    return {
        'pnl': total_pnl,
        'trades': int(features.shape[0]),
        'wins': int((pnl > 0).sum()),
        'losses': int((pnl < 0).sum()),
        'win_rate_pct': round(100 * int((pnl > 0).sum()) / max(1, int(features.shape[0])), 2),
        'by_day': dict(sorted(by_day.items())),
        'by_ticker': dict(sorted(by_ticker.items())),
        'worst_day': {'day': worst_day_key, **by_day[worst_day_key]} if worst_day_key else None,
    }


def _push(heap: list, row: dict, top_k: int, seq: int) -> None:
    item = (float(row.get('pnl') or 0.0), seq, row)
    if len(heap) < top_k:
        heapq.heappush(heap, item)
    elif item[0] > heap[0][0]:
        heapq.heapreplace(heap, item)


def score_store(args) -> dict:
    started = time.perf_counter()
    store = scoring_feature_store.load_store(args.feature_store)
    agg = _aggregate_for_store(store)
    top_k = max(1, int(args.top_k))
    heap = []
    seq = 0
    total = 0
    total_requested = int(args.variant_count)
    batch_size = max(1, int(args.batch_size))
    while total < total_requested:
        batch_start = int(args.variant_start or 0) + total
        batch_end = batch_start + min(batch_size, total_requested - total)
        if args.variant_shard_dir or args.disable_direct_matrix:
            batch_args = argparse.Namespace(**vars(args))
            batch_args.variant_start = batch_start
            batch_args.variant_count = batch_end - batch_start
            batch = list(_variant_range(batch_args))
            if not batch:
                break
            scored_arrays = scoring_compiled_kernels.score_batch_arrays(agg, batch)
            global_indexes = np.arange(batch_start, batch_start + len(batch), dtype=np.int64)
        else:
            weights, bias, global_indexes = scoring_variant_matrix.matrix_for_global_range(
                batch_start,
                batch_end,
                not args.no_existing,
                not args.no_broad_full,
                not args.no_v29_full,
                args.include_expanded_full,
                args.include_active_local,
            )
            if weights.shape[0] == 0:
                break
            scored_arrays = scoring_compiled_kernels.score_weight_matrix_arrays(agg, weights, bias)
            batch = None
        if len(global_indexes) == 0:
            break
        pnl = scored_arrays[0]
        keep_n = min(top_k, len(global_indexes))
        if keep_n < len(global_indexes):
            keep_idx = np.argpartition(pnl, -keep_n)[-keep_n:]
            keep_idx = keep_idx[np.argsort(pnl[keep_idx])[::-1]]
        else:
            keep_idx = np.argsort(pnl)[::-1]
        decision_hashes = None
        if args.decision_hashes:
            if batch is None:
                batch = [
                    scoring_variant_matrix.variant_at_global_index(
                        int(idx),
                        not args.no_existing,
                        not args.no_broad_full,
                        not args.no_v29_full,
                        args.include_expanded_full,
                        args.include_active_local,
                    )
                    for idx in global_indexes
                ]
            hashes = _decision_hashes_for_rows(store, batch)
            decision_hashes = hashes
        for batch_idx in keep_idx:
            global_index = int(global_indexes[int(batch_idx)])
            variant = (
                batch[int(batch_idx)]
                if batch is not None
                else scoring_variant_matrix.variant_at_global_index(
                    global_index,
                    not args.no_existing,
                    not args.no_broad_full,
                    not args.no_v29_full,
                    args.include_expanded_full,
                    args.include_active_local,
                )
            )
            row = scoring_compiled_kernels.row_from_arrays(
                variant,
                int(batch_idx),
                scored_arrays,
                int(agg['trades']),
                float(args.starting_balance),
            )
            row['global_variant_index'] = global_index
            if decision_hashes:
                row['decision_vector_hash'] = decision_hashes[int(batch_idx)]
            _push(heap, row, top_k, seq)
            seq += 1
        total += len(global_indexes)
        if args.progress_every and total % int(args.progress_every) == 0:
            print(json.dumps({'event': 'progress', 'variants': total}, sort_keys=True), flush=True)
    results = [item[2] for item in sorted(heap, key=lambda item: item[0], reverse=True)]
    results = tournament_safety.enrich_lab_results(results)
    payload = {
        'schema_version': 1,
        'mode': 'score-store',
        'feature_store': store['manifest'],
        'starting_balance': args.starting_balance,
        'variant_start': args.variant_start,
        'variant_count': args.variant_count,
        'top_k': top_k,
        'elapsed_seconds': round(time.perf_counter() - started, 3),
        'code_hashes': {
            'scoring_pipeline_accelerator.py': tournament_safety._file_sha256('scoring_pipeline_accelerator.py'),
            'scoring_compiled_kernels.py': tournament_safety._file_sha256('scoring_compiled_kernels.py'),
            'scoring_feature_store.py': tournament_safety._file_sha256('scoring_feature_store.py'),
            'scoring_variant_random_access.py': tournament_safety._file_sha256('scoring_variant_random_access.py'),
        },
        'results': results,
    }
    payload['run_hash'] = tournament_safety.stable_json_hash({
        'mode': payload['mode'],
        'store_hash': (store['manifest'] or {}).get('store_hash'),
        'variant_start': args.variant_start,
        'variant_count': args.variant_count,
        'top_k': top_k,
        'results': results,
    }, 24)
    _write_json(args.out, payload)
    return {'out': args.out, 'results': len(results), 'elapsed_seconds': payload['elapsed_seconds'], 'run_hash': payload['run_hash']}


def enrich_topk(args) -> dict:
    started = time.perf_counter()
    store = scoring_feature_store.load_store(args.feature_store)
    source, variants = _load_variants_json(args.input, args.limit or None)
    rows = list((source.get('results') or [])[:len(variants)])
    hashes = _decision_hashes_for_rows(store, variants)
    first_by_hash = {}
    duplicate_groups = defaultdict(list)
    for row, variant, h in zip(rows, variants, hashes):
        row['decision_vector_hash'] = h
        row['decision_vector_duplicate_of'] = first_by_hash.get(h)
        if h not in first_by_hash:
            first_by_hash[h] = variant.name
        duplicate_groups[h].append(variant.name)
    partition_limit = min(len(variants), max(0, int(args.partition_top_n)))
    partitions = []
    for idx in range(partition_limit):
        row = rows[idx]
        variant = variants[idx]
        partitions.append({
            'variant': variant.name,
            'rank': row.get('global_lab_rank') or row.get('lab_rank') or idx + 1,
            'decision_vector_hash': row.get('decision_vector_hash'),
            'partition_summary': _partition_summary(store, variant),
        })
    payload = {
        'schema_version': 1,
        'mode': 'enrich-topk',
        'source': args.input,
        'feature_store': store['manifest'],
        'input_results': len(rows),
        'unique_decision_vectors': len(first_by_hash),
        'deduped_variants': len(rows) - len(first_by_hash),
        'duplicate_groups': [
            {'decision_vector_hash': h, 'count': len(names), 'variants': names[:25]}
            for h, names in sorted(duplicate_groups.items(), key=lambda kv: len(kv[1]), reverse=True)
            if len(names) > 1
        ][:100],
        'partition_top_n': partition_limit,
        'partition_summaries': partitions,
        'results': rows,
        'elapsed_seconds': round(time.perf_counter() - started, 3),
    }
    payload['run_hash'] = tournament_safety.stable_json_hash({
        'mode': payload['mode'],
        'source': tournament_safety._file_sha256(args.input),
        'store_hash': (store['manifest'] or {}).get('store_hash'),
        'unique_decision_vectors': payload['unique_decision_vectors'],
        'results_hash': tournament_safety.stable_json_hash(rows, 24),
    }, 24)
    _write_json(args.out, payload)
    return {
        'out': args.out,
        'input_results': len(rows),
        'unique_decision_vectors': len(first_by_hash),
        'deduped_variants': len(rows) - len(first_by_hash),
        'elapsed_seconds': payload['elapsed_seconds'],
        'run_hash': payload['run_hash'],
    }


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Safe Step 1/2 acceleration helpers.')
    ap.add_argument('--mode', choices=['score-store', 'enrich-topk'], required=True)
    ap.add_argument('--feature-store', required=True)
    ap.add_argument('--input', default=None, help='Top-K JSON for mode=enrich-topk.')
    ap.add_argument('--out', default=os.path.join(DEFAULT_OUT_DIR, 'pipeline_accelerator_output.json'))
    ap.add_argument('--starting-balance', type=float, default=100000.0)
    ap.add_argument('--top-k', type=int, default=10000)
    ap.add_argument('--batch-size', type=int, default=50000)
    ap.add_argument('--variant-start', type=int, default=0)
    ap.add_argument('--variant-count', type=int, default=0)
    ap.add_argument('--variant-shard-dir', default=None)
    ap.add_argument('--no-existing', action='store_true')
    ap.add_argument('--no-broad-full', action='store_true')
    ap.add_argument('--no-v29-full', action='store_true')
    ap.add_argument('--include-expanded-full', action='store_true')
    ap.add_argument('--include-active-local', action='store_true')
    ap.add_argument('--decision-hashes', action='store_true')
    ap.add_argument('--disable-direct-matrix', action='store_true',
                    help='Diagnostic fallback: materialize Variant objects before scoring.')
    ap.add_argument('--partition-top-n', type=int, default=100)
    ap.add_argument('--limit', type=int, default=0)
    ap.add_argument('--progress-every', type=int, default=0)
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    if args.mode == 'score-store':
        if not args.variant_count:
            raise SystemExit('--variant-count is required for mode=score-store')
        payload = score_store(args)
    else:
        if not args.input:
            raise SystemExit('--input is required for mode=enrich-topk')
        payload = enrich_topk(args)
    print(json.dumps(payload, indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
