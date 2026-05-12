"""Exact, resumable massive scoring-lab runner.

This is the integrity-preserving path for very large variant screens:

* deterministic chunk manifests
* SQLite chunk ledger
* memory-mapped aggregate feature arrays
* exact per-chunk top-K heaps
* exact global top-K merge
* canary variants that must score identically across chunks

The scoring metric is still the Step-1 lab proxy, not full replay P/L.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import heapq
import json
import os
import sqlite3
import time
import uuid
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Iterable

import numpy as np

import scoring_variant_lab as slow
import scoring_variant_lab_fast as fast
import scoring_variant_lab_massive as massive
import active_engine_baseline
import scoring_compiled_kernels
import scoring_feature_store
import scoring_variant_random_access
import scoring_variant_matrix
import scoring_variant_shards
import tournament_safety
import worker_policy


HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUT_DIR = os.path.join(HERE, 'postmortem', 'backtests', 'exact_massive_scoring')
DEFAULT_CSV = massive.DEFAULT_CSV
DEFAULT_ACTIVE_FEATURE_STORE_POINTER = os.path.join(
    HERE,
    'postmortem',
    'backtests',
    'scoring_feature_store',
    'active_step1_feature_store_manifest.txt',
)


def _signature(variant: slow.Variant) -> str:
    return tournament_safety.stable_json_hash({
        'weights': {k: round(float(v or 0.0), 8) for k, v in sorted(variant.weights.items())},
        'bias': round(float(variant.bias or 0.0), 8),
    }, length=24)


def _run_dir(args) -> str:
    name = args.name or f"exact_massive_{time.strftime('%Y%m%d_%H%M%S')}"
    safe = ''.join(ch if ch.isalnum() or ch in '._-' else '_' for ch in name).strip('._')
    return os.path.abspath(os.path.join(args.out_dir, safe))


def _paths(run_dir: str) -> dict:
    return {
        'manifest': os.path.join(run_dir, 'run_manifest.json'),
        'ledger': os.path.join(run_dir, 'chunk_ledger.sqlite3'),
        'feature_dir': os.path.join(run_dir, 'feature_cache'),
        'chunks_dir': os.path.join(run_dir, 'chunks'),
        'merged': os.path.join(run_dir, 'global_topk.json'),
        'merged_csv': os.path.join(run_dir, 'global_topk.csv'),
        'merge_proof': os.path.join(run_dir, 'merge_proof.json'),
        'lock': os.path.join(run_dir, 'run.lock'),
        'pareto': os.path.join(run_dir, 'pareto_topk.json'),
        'pareto_csv': os.path.join(run_dir, 'pareto_topk.csv'),
        'beats_active': os.path.join(run_dir, 'beats_active_engine_topk.json'),
        'beats_active_csv': os.path.join(run_dir, 'beats_active_engine_topk.csv'),
    }


def _read_json(path: str, default=None):
    try:
        if str(path).endswith('.gz'):
            with gzip.open(path, 'rt', encoding='utf-8') as f:
                return json.load(f)
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return default


def _read_text(path: str) -> str | None:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            value = f.read().strip()
        return value or None
    except FileNotFoundError:
        return None


def _write_json(path: str, payload: dict) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if str(path).endswith('.gz'):
        with gzip.open(path, 'wt', encoding='utf-8') as f:
            json.dump(payload, f, separators=(',', ':'), sort_keys=True)
    else:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=2, sort_keys=True)
    return path


def _acquire_lock(run_dir: str, disabled: bool = False) -> str | None:
    if disabled:
        return None
    path = _paths(run_dir)['lock']
    os.makedirs(os.path.dirname(path), exist_ok=True)
    payload = {
        'pid': os.getpid(),
        'created_at_epoch': int(time.time()),
        'token': uuid.uuid4().hex,
    }
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    try:
        fd = os.open(path, flags)
    except FileExistsError:
        existing = _read_json(path, {}) or {}
        raise RuntimeError(f"run lock exists: {path} pid={existing.get('pid')} created={existing.get('created_at_epoch')}")
    with os.fdopen(fd, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    return path


def _release_lock(path: str | None) -> None:
    if path and os.path.exists(path):
        try:
            os.remove(path)
        except Exception:
            pass


def _build_feature_cache(csv_path: str, feature_dir: str, starting_balance: float,
                         feature_store_manifest: str | None = None) -> dict:
    os.makedirs(feature_dir, exist_ok=True)
    if feature_store_manifest:
        store = scoring_feature_store.load_store(feature_store_manifest)
        agg = scoring_feature_store.aggregate_store(store)
        store_meta = store['manifest']
    else:
        store_meta = scoring_feature_store.build_from_trade_csv(
            csv_path,
            os.path.join(feature_dir, 'canonical_store'),
            starting_balance,
            name=os.path.splitext(os.path.basename(csv_path))[0],
        )
        store = scoring_feature_store.load_store(store_meta['manifest_path'])
        agg = scoring_feature_store.aggregate_store(store)
    arrays = {
        'features': agg['features'],
        'original_side': agg['original_side'],
        'pnl_sum': agg['pnl_sum'],
        'count': agg['count'],
        'win_count': agg['win_count'],
        'loss_count': agg['loss_count'],
        'flipped_win_count': agg['flipped_win_count'],
        'flipped_loss_count': agg['flipped_loss_count'],
    }
    array_paths = {}
    for name, arr in arrays.items():
        path = os.path.join(feature_dir, f'{name}.npy')
        np.save(path, arr)
        array_paths[name] = path
    meta = {
        'schema_version': 1,
        'source_csv': os.path.abspath(csv_path),
        'source_csv_sha256': tournament_safety._file_sha256(csv_path),
        'canonical_feature_store': store_meta,
        'feature_names': list(fast.FEATURE_NAMES),
        'feature_names_hash': tournament_safety.stable_json_hash(list(fast.FEATURE_NAMES), 20),
        'rows': int(store_meta.get('rows') or 0),
        'buckets': int(agg['buckets']),
        'trades': int(agg['trades']),
        'starting_balance': starting_balance,
        'array_paths': array_paths,
    }
    meta['feature_cache_hash'] = tournament_safety.stable_json_hash({
        'source_csv_sha256': meta['source_csv_sha256'],
        'canonical_store_hash': store_meta.get('store_hash'),
        'feature_names': meta['feature_names'],
        'buckets': meta['buckets'],
        'trades': meta['trades'],
        'arrays': {k: tournament_safety._file_sha256(v) for k, v in array_paths.items()},
    }, 24)
    _write_json(os.path.join(feature_dir, 'feature_cache_manifest.json'), meta)
    return meta


def _default_feature_store_manifest(explicit: str | None = None) -> str | None:
    if explicit:
        return explicit
    pointer = _read_text(DEFAULT_ACTIVE_FEATURE_STORE_POINTER)
    if not pointer:
        return None
    path = pointer
    if not os.path.isabs(path):
        path = os.path.abspath(os.path.join(HERE, path))
    return path if os.path.exists(path) else None


def _load_feature_cache(feature_dir: str) -> dict:
    meta = _read_json(os.path.join(feature_dir, 'feature_cache_manifest.json'))
    if not meta:
        raise FileNotFoundError(f'missing feature cache manifest: {feature_dir}')
    arrays = {name: np.load(path, mmap_mode='r') for name, path in meta['array_paths'].items()}
    return {
        **arrays,
        'trades': int(meta['trades']),
        'buckets': int(meta['buckets']),
        'meta': meta,
    }


def _variant_iter(args_dict: dict) -> Iterable[slow.Variant]:
    return massive.variant_stream(
        not args_dict.get('no_existing', False),
        not args_dict.get('no_broad_full', False),
        not args_dict.get('no_v29_full', False),
    )


def _take_chunk(args_dict: dict, start_index: int, end_index: int) -> list[slow.Variant]:
    if args_dict.get('variant_shard_dir'):
        return scoring_variant_shards.load_range(args_dict['variant_shard_dir'], start_index, end_index)
    offset = int(args_dict.get('variant_index_offset') or 0)
    if args_dict.get('random_access_variants', True):
        return scoring_variant_random_access.variant_range(
            start_index + offset,
            end_index + offset,
            not args_dict.get('no_existing', False),
            not args_dict.get('no_broad_full', False),
            not args_dict.get('no_v29_full', False),
            args_dict.get('include_expanded_full', False),
            args_dict.get('include_active_local', False),
            args_dict.get('include_creative_full', False),
            args_dict.get('include_core2_creative', False),
            args_dict.get('include_core2_guarded', False),
            args_dict.get('include_micro_local', False),
            args_dict.get('include_replay_safe_local', False),
        )
    if args_dict.get('include_expanded_full'):
        raise RuntimeError('expanded_full requires random-access variants or --variant-shard-dir')
    out = []
    for idx, variant in enumerate(_variant_iter(args_dict)):
        if idx < start_index:
            continue
        if idx >= end_index:
            break
        out.append(variant)
    return out


def _direct_matrix_for_range(args_dict: dict, start_index: int, end_index: int):
    if args_dict.get('variant_shard_dir') or args_dict.get('disable_direct_matrix'):
        return None
    offset = int(args_dict.get('variant_index_offset') or 0)
    return scoring_variant_matrix.matrix_for_global_range(
        start_index + offset,
        end_index + offset,
        not args_dict.get('no_existing', False),
        not args_dict.get('no_broad_full', False),
        not args_dict.get('no_v29_full', False),
        args_dict.get('include_expanded_full', False),
        args_dict.get('include_active_local', False),
        args_dict.get('include_creative_full', False),
        args_dict.get('include_core2_creative', False),
        args_dict.get('include_core2_guarded', False),
        args_dict.get('include_micro_local', False),
        args_dict.get('include_replay_safe_local', False),
    )


def _direct_matrix_enabled(args_dict: dict) -> bool:
    return not args_dict.get('variant_shard_dir') and not args_dict.get('disable_direct_matrix')


def _variant_for_global_index(args_dict: dict, global_index: int) -> slow.Variant:
    return scoring_variant_matrix.variant_at_global_index(
        int(global_index),
        not args_dict.get('no_existing', False),
        not args_dict.get('no_broad_full', False),
        not args_dict.get('no_v29_full', False),
        args_dict.get('include_expanded_full', False),
        args_dict.get('include_active_local', False),
        args_dict.get('include_creative_full', False),
        args_dict.get('include_core2_creative', False),
        args_dict.get('include_core2_guarded', False),
        args_dict.get('include_micro_local', False),
        args_dict.get('include_replay_safe_local', False),
    )


def _score_batch_memmap(agg: dict, variants: list[slow.Variant], starting_balance: float) -> list[dict]:
    rows = scoring_compiled_kernels.score_batch(agg, variants, starting_balance)
    return tournament_safety.enrich_lab_results(rows)


def _push_top(heap: list, row: dict, top_k: int, seq: int, score_key: str = 'pnl') -> None:
    item = (float(row.get(score_key, row.get('pnl'))), seq, row)
    if len(heap) < top_k:
        heapq.heappush(heap, item)
    elif item[0] > heap[0][0]:
        heapq.heapreplace(heap, item)


def _active_weight_vector(manifest: dict) -> tuple[np.ndarray, float] | None:
    profile = (((manifest.get('active_engine_baseline') or {}).get('profile')) or {})
    weights = profile.get('weights') or {}
    if not weights:
        return None
    vec = np.zeros(len(fast.FEATURE_NAMES), dtype=np.float64)
    for idx, name in enumerate(fast.FEATURE_NAMES):
        vec[idx] = float(weights.get(name, 0.0) or 0.0)
    return vec, float(profile.get('bias') or 0.0)


def _weight_cosine_matrix(weights: np.ndarray, active_weights: np.ndarray) -> np.ndarray:
    active_norm = float(np.sqrt(np.sum(active_weights * active_weights)))
    weight_norm = np.sqrt(np.sum(weights * weights, axis=1))
    denom = weight_norm * active_norm
    out = np.zeros(weights.shape[0], dtype=np.float64)
    ok = denom > 0.0
    if np.any(ok):
        out[ok] = (weights[ok] @ active_weights) / denom[ok]
    return out


def _live_shape_metrics(agg: dict, weights: np.ndarray, bias: np.ndarray,
                        manifest: dict, pnl: np.ndarray, flipped_count: np.ndarray) -> dict[str, np.ndarray] | None:
    active = _active_weight_vector(manifest)
    if active is None:
        return None
    active_weights, active_bias = active
    features = np.asarray(agg['features'], dtype=np.float64)
    counts = np.asarray(agg['count'], dtype=np.float64)
    original = np.asarray(agg['original_side']).reshape(-1, 1)
    total = float(np.sum(counts)) or 1.0
    active_scores = features @ active_weights + active_bias
    active_side = np.where(active_scores > 0.0, 1, np.where(active_scores < 0.0, -1, np.asarray(agg['original_side'])))
    scores = features @ weights.T
    if bias.size:
        scores = scores + bias.reshape(1, -1)
    chosen = np.where(scores > 0.0, 1, np.where(scores < 0.0, -1, original))
    agreement = ((chosen == active_side.reshape(-1, 1)) * counts.reshape(-1, 1)).sum(axis=0) / total
    flip_rate = np.asarray(flipped_count, dtype=np.float64) / total
    active_flip_rate = float(np.sum((active_side != np.asarray(agg['original_side'])) * counts) / total)
    weight_cosine = _weight_cosine_matrix(weights, active_weights)
    retention_score = (
        np.asarray(pnl, dtype=np.float64)
        + 2500.0 * agreement
        + 750.0 * weight_cosine
        - 3500.0 * np.maximum(0.0, 0.62 - agreement)
        - 2500.0 * np.abs(flip_rate - active_flip_rate)
    )
    return {
        'step1_retention_score': retention_score,
        'active_side_agreement_rate': agreement,
        'active_side_flip_rate': 1.0 - agreement,
        'active_original_flip_rate': np.full_like(agreement, active_flip_rate, dtype=np.float64),
        'variant_original_flip_rate': flip_rate,
        'weight_similarity_to_active': weight_cosine,
    }


def _variant_from_row(row: dict) -> slow.Variant:
    return slow.Variant(
        str(row.get('variant') or row.get('name') or 'custom_canary'),
        dict(row.get('weights') or {}),
        float(row.get('bias') or 0.0),
    )


def _load_custom_canaries(path: str | None) -> list[slow.Variant]:
    if not path:
        return []
    payload = _read_json(path, {})
    rows = payload.get('variants') if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        raise ValueError(f'custom canary file must be a list or contain variants list: {path}')
    return [_variant_from_row(row) for row in rows]


def _canary_scores(agg: dict, starting_balance: float, canary_variants: list[slow.Variant] | None = None) -> dict:
    canaries = list(canary_variants or slow.VARIANTS[: min(5, len(slow.VARIANTS))])
    return {
        row['variant']: {
            'pnl': row['pnl'],
            'wins': row['wins'],
            'losses': row['losses'],
            'flipped': row['flipped'],
        }
        for row in _score_batch_memmap(agg, canaries, starting_balance)
    }


def _connect_ledger(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = sqlite3.connect(path, timeout=60)
    con.execute("""
        CREATE TABLE IF NOT EXISTS chunks (
            chunk_id INTEGER PRIMARY KEY,
            start_index INTEGER NOT NULL,
            end_index INTEGER NOT NULL,
            status TEXT NOT NULL,
            variant_count INTEGER DEFAULT 0,
            chunk_hash TEXT,
            top_path TEXT,
            elapsed_seconds REAL,
            error TEXT,
            lease_id TEXT,
            worker_id TEXT,
            lease_started_at_epoch INTEGER,
            updated_at_epoch INTEGER NOT NULL
        )
    """)
    existing_cols = {row[1] for row in con.execute("PRAGMA table_info(chunks)")}
    for col, ddl in {
        'lease_id': 'ALTER TABLE chunks ADD COLUMN lease_id TEXT',
        'lease_started_at_epoch': 'ALTER TABLE chunks ADD COLUMN lease_started_at_epoch INTEGER',
        'worker_id': 'ALTER TABLE chunks ADD COLUMN worker_id TEXT',
    }.items():
        if col not in existing_cols:
            con.execute(ddl)
    con.commit()
    return con


def _init_chunks(ledger_path: str, total_variants: int, chunk_size: int) -> None:
    con = _connect_ledger(ledger_path)
    now = int(time.time())
    chunk_id = 0
    for start in range(0, total_variants, chunk_size):
        end = min(total_variants, start + chunk_size)
        con.execute(
            """
            INSERT OR IGNORE INTO chunks
            (chunk_id, start_index, end_index, status, updated_at_epoch)
            VALUES (?, ?, ?, 'pending', ?)
            """,
            (chunk_id, start, end, now),
        )
        chunk_id += 1
    con.commit()
    con.close()


def _chunk_rows(ledger_path: str, statuses: tuple[str, ...]) -> list[dict]:
    con = _connect_ledger(ledger_path)
    q = ','.join('?' for _ in statuses)
    rows = [
        {
            'chunk_id': r[0],
            'start_index': r[1],
            'end_index': r[2],
            'status': r[3],
            'top_path': r[4],
            'chunk_hash': r[5],
            'lease_id': r[6],
        }
        for r in con.execute(f"SELECT chunk_id,start_index,end_index,status,top_path,chunk_hash,lease_id FROM chunks WHERE status IN ({q}) ORDER BY chunk_id", statuses)
    ]
    con.close()
    return rows


def _mark_chunk(ledger_path: str, chunk_id: int, status: str, **fields) -> None:
    allowed = {'variant_count', 'chunk_hash', 'top_path', 'elapsed_seconds', 'error', 'lease_id', 'lease_started_at_epoch'}
    assignments = ['status=?', 'updated_at_epoch=?']
    values: list = [status, int(time.time())]
    for key, value in fields.items():
        if key in allowed:
            assignments.append(f'{key}=?')
            values.append(value)
    values.append(chunk_id)
    con = _connect_ledger(ledger_path)
    con.execute(f"UPDATE chunks SET {', '.join(assignments)} WHERE chunk_id=?", values)
    con.commit()
    con.close()


def _lease_chunks(ledger_path: str, limit: int, statuses: tuple[str, ...] = ('pending', 'failed'),
                  worker_id: str | None = None) -> list[dict]:
    con = _connect_ledger(ledger_path)
    lease_id = uuid.uuid4().hex
    q = ','.join('?' for _ in statuses)
    now = int(time.time())
    try:
        con.execute('BEGIN IMMEDIATE')
        selected = list(con.execute(
            f"SELECT chunk_id,start_index,end_index,status,top_path,chunk_hash,lease_id FROM chunks WHERE status IN ({q}) ORDER BY chunk_id LIMIT ?",
            (*statuses, int(limit)),
        ))
        for row in selected:
            con.execute(
                "UPDATE chunks SET status='running', lease_id=?, worker_id=?, lease_started_at_epoch=?, updated_at_epoch=?, error=NULL WHERE chunk_id=?",
                (lease_id, worker_id, now, now, row[0]),
            )
        con.commit()
    finally:
        con.close()
    return [
        {
            'chunk_id': r[0],
            'start_index': r[1],
            'end_index': r[2],
            'status': 'running',
            'top_path': r[4],
            'chunk_hash': r[5],
            'lease_id': lease_id,
            'worker_id': worker_id,
        }
        for r in selected
    ]


def reset_chunks(args) -> dict:
    run_dir = os.path.abspath(args.run_dir)
    paths = _paths(run_dir)
    con = _connect_ledger(paths['ledger'])
    now = int(time.time())
    changed = 0
    if args.reset_failed:
        cur = con.execute("UPDATE chunks SET status='pending', error=NULL, lease_id=NULL, lease_started_at_epoch=NULL, updated_at_epoch=? WHERE status='failed'", (now,))
        changed += cur.rowcount if cur.rowcount is not None else 0
    if args.reset_stale_running_minutes and args.reset_stale_running_minutes > 0:
        cutoff = now - int(args.reset_stale_running_minutes * 60)
        cur = con.execute(
            "UPDATE chunks SET status='pending', error='reset_stale_running', lease_id=NULL, lease_started_at_epoch=NULL, updated_at_epoch=? WHERE status='running' AND COALESCE(lease_started_at_epoch, updated_at_epoch) < ?",
            (now, cutoff),
        )
        changed += cur.rowcount if cur.rowcount is not None else 0
    con.commit()
    con.close()
    return {'run_dir': run_dir, 'reset_chunks': changed}


def process_chunk(run_dir: str, args_dict: dict, chunk: dict) -> dict:
    paths = _paths(run_dir)
    manifest = _read_json(paths['manifest'])
    if not manifest:
        raise RuntimeError('missing run manifest')
    started = time.perf_counter()
    try:
        agg = _load_feature_cache(paths['feature_dir'])
        expected_canaries = manifest.get('canary_scores') or {}
        canary_variants = [_variant_from_row(row) for row in manifest.get('canary_variants', [])]
        actual_canaries = _canary_scores(agg, float(args_dict['starting_balance']), canary_variants or None)
        if actual_canaries != expected_canaries:
            raise RuntimeError('canary score mismatch; refusing to process chunk')
        variants = _take_chunk(args_dict, int(chunk['start_index']), int(chunk['end_index']))
        heap: list = []
        seq = 0
        batch_size = max(1, int(args_dict['batch_size']))
        chunk_start = int(chunk['start_index'])
        chunk_end = int(chunk['end_index'])
        direct_matrix = _direct_matrix_enabled(args_dict)
        if direct_matrix:
            variants = None
            total_len = chunk_end - chunk_start
        else:
            variants = _take_chunk(args_dict, chunk_start, chunk_end)
            total_len = len(variants)
        for start in range(0, total_len, batch_size):
            if direct_matrix:
                batch_start = chunk_start + start
                batch_end = min(chunk_end, batch_start + batch_size)
                weights, bias, global_indexes = _direct_matrix_for_range(args_dict, batch_start, batch_end)
                scored_arrays = scoring_compiled_kernels.score_weight_matrix_arrays(agg, weights, bias)
                batch = None
            else:
                batch = variants[start:start + batch_size]
                global_indexes = np.arange(chunk_start + start, chunk_start + start + len(batch), dtype=np.int64)
                weights, bias = massive._variant_matrix(batch)
                scored_arrays = scoring_compiled_kernels.score_batch_arrays(agg, batch)
            pnl = scored_arrays[0]
            retention_key = 'pnl'
            rank_values = pnl
            shape_metrics = None
            if args_dict.get('step1_retention_mode') == 'live-shape':
                shape_metrics = _live_shape_metrics(agg, weights, bias, manifest, pnl, scored_arrays[3])
                if shape_metrics:
                    retention_key = 'step1_retention_score'
                    rank_values = shape_metrics[retention_key]
            keep_n = min(int(args_dict['top_k']), len(global_indexes))
            if keep_n < len(global_indexes):
                keep_idx = np.argpartition(rank_values, -keep_n)[-keep_n:]
                keep_idx = keep_idx[np.argsort(rank_values[keep_idx])[::-1]]
            else:
                keep_idx = np.argsort(rank_values)[::-1]
            for batch_idx in keep_idx:
                global_index = int(global_indexes[int(batch_idx)])
                variant = batch[int(batch_idx)] if batch is not None else _variant_for_global_index(args_dict, global_index)
                row = scoring_compiled_kernels.row_from_arrays(
                    variant,
                    int(batch_idx),
                    scored_arrays,
                    int(agg['trades']),
                    float(args_dict['starting_balance']),
                )
                row['global_variant_index'] = global_index
                if shape_metrics:
                    for key, values in shape_metrics.items():
                        row[key] = round(float(values[int(batch_idx)]), 6)
                _push_top(heap, row, int(args_dict['top_k']), seq, retention_key)
                seq += 1
        top = [item[2] for item in sorted(heap, key=lambda item: item[0], reverse=True)]
        payload = {
            'schema_version': 1,
            'chunk_id': chunk['chunk_id'],
            'start_index': chunk['start_index'],
            'end_index': chunk['end_index'],
            'variant_count': total_len,
            'top_k': int(args_dict['top_k']),
            'run_hash': manifest.get('run_hash'),
            'feature_cache_hash': manifest.get('feature_cache', {}).get('feature_cache_hash'),
            'canary_scores': actual_canaries,
            'results': top,
        }
        payload['chunk_hash'] = tournament_safety.stable_json_hash({
            'chunk_id': payload['chunk_id'],
            'start_index': payload['start_index'],
            'end_index': payload['end_index'],
            'run_hash': payload['run_hash'],
            'feature_cache_hash': payload['feature_cache_hash'],
            'results': top,
        }, 24)
        top_path = os.path.join(paths['chunks_dir'], f"chunk_{int(chunk['chunk_id']):08d}.top.json.gz")
        _write_json(top_path, payload)
        elapsed = round(time.perf_counter() - started, 3)
        _mark_chunk(
            paths['ledger'],
            chunk['chunk_id'],
            'complete',
            variant_count=total_len,
            chunk_hash=payload['chunk_hash'],
            top_path=top_path,
            elapsed_seconds=elapsed,
            error=None,
            lease_id=None,
            lease_started_at_epoch=None,
        )
        return {'chunk_id': chunk['chunk_id'], 'status': 'complete', 'elapsed_seconds': elapsed, 'variant_count': total_len}
    except Exception as exc:
        _mark_chunk(paths['ledger'], chunk['chunk_id'], 'failed', error=str(exc))
        return {'chunk_id': chunk['chunk_id'], 'status': 'failed', 'error': str(exc)}


def _estimated_total(args) -> int:
    if args.variant_shard_dir:
        manifest = scoring_variant_shards.load_manifest(args.variant_shard_dir)
        total = int(manifest.get('unique_variants') or 0)
        if args.max_variants and args.max_variants > 0:
            return min(total, int(args.max_variants))
        return total
    counts = scoring_variant_random_access.estimated_count(
        not args.no_existing,
        not args.no_broad_full,
        not args.no_v29_full,
        args.include_expanded_full,
        args.include_active_local,
        args.include_creative_full,
        args.include_core2_creative,
        args.include_core2_guarded,
        args.include_micro_local,
        args.include_replay_safe_local,
    )
    raw = int(counts['raw_total_before_dedup'])
    if args.max_variants and args.max_variants > 0:
        return min(raw, int(args.max_variants))
    return raw


def init_run(args) -> dict:
    run_dir = _run_dir(args)
    paths = _paths(run_dir)
    os.makedirs(run_dir, exist_ok=True)
    feature_store_manifest = _default_feature_store_manifest(args.feature_store)
    feature_cache = _build_feature_cache(args.csv, paths['feature_dir'], args.starting_balance, feature_store_manifest)
    agg = _load_feature_cache(paths['feature_dir'])
    canary_variants = _load_custom_canaries(args.canary_variants_json)
    if not canary_variants:
        canary_variants = list(slow.VARIANTS[: min(5, len(slow.VARIANTS))])
    canaries = _canary_scores(agg, args.starting_balance, canary_variants)
    active_baseline = None
    if not args.no_active_baseline:
        active_lab = active_engine_baseline.lab_baseline(agg, args.starting_balance)
        active_baseline = {
            **active_engine_baseline.active_profile_payload(),
            'lab': active_lab,
            'thresholds': {
                'min_pnl_margin': float(args.beat_active_min_pnl_margin or 0.0),
                'require_win_rate': bool(args.beat_active_require_win_rate),
            },
            'scope_note': (
                'Step 1 baseline scores the active scoring profile on the same canonical '
                'feature/outcome store. Live entry gates are recorded as metadata but are '
                'not applied in the Step 1 scoring proxy.'
            ),
        }
    total = _estimated_total(args)
    effective_top_k = int(args.top_k or 10000)
    generator_config = {
        'no_existing': args.no_existing,
        'no_broad_full': args.no_broad_full,
        'no_v29_full': args.no_v29_full,
        'include_expanded_full': args.include_expanded_full,
        'include_active_local': args.include_active_local,
        'include_creative_full': args.include_creative_full,
        'include_core2_creative': args.include_core2_creative,
        'include_core2_guarded': args.include_core2_guarded,
        'include_micro_local': args.include_micro_local,
        'include_replay_safe_local': args.include_replay_safe_local,
        'max_variants': args.max_variants,
        'variant_index_offset': int(args.variant_index_offset or 0),
        'step1_retention_mode': args.step1_retention_mode,
        'estimated_total': total,
        'random_access_variants': not args.disable_random_access_variants,
        'disable_direct_matrix': args.disable_direct_matrix,
        'variant_shard_dir': os.path.abspath(args.variant_shard_dir) if args.variant_shard_dir else None,
        'variant_shard_manifest': scoring_variant_shards.load_manifest(args.variant_shard_dir) if args.variant_shard_dir else None,
        'random_access_counts': scoring_variant_random_access.estimated_count(
            not args.no_existing,
            not args.no_broad_full,
            not args.no_v29_full,
            args.include_expanded_full,
            args.include_active_local,
            args.include_creative_full,
            args.include_core2_creative,
            args.include_core2_guarded,
            args.include_micro_local,
            args.include_replay_safe_local,
        ),
    }
    manifest = {
        'schema_version': 1,
        'created_at_epoch': int(time.time()),
        'run_name': args.name,
        'csv': os.path.abspath(args.csv),
        'csv_sha256': tournament_safety._file_sha256(args.csv),
        'top_k': effective_top_k,
        'chunk_size': args.chunk_size,
        'batch_size': args.batch_size,
        'starting_balance': args.starting_balance,
        'feature_cache': feature_cache,
        'feature_store_manifest': os.path.abspath(feature_store_manifest) if feature_store_manifest else None,
        'generator_config': generator_config,
        'generator_hash': tournament_safety.stable_json_hash(generator_config, 24),
        'code_hashes': {name: tournament_safety._file_sha256(name) for name in [
            'scoring_variant_lab.py',
            'scoring_variant_lab_fast.py',
            'scoring_variant_lab_massive.py',
            'scoring_variant_lab_exact_massive.py',
            'scoring_feature_store.py',
            'scoring_compiled_kernels.py',
            'scoring_pipeline_accelerator.py',
            'scoring_variant_matrix.py',
            'scoring_variant_random_access.py',
            'scoring_variant_shards.py',
            'active_engine_baseline.py',
        ]},
        'scoring_kernel': {
            'kernel_mode_env': os.environ.get('SCORING_KERNEL', 'auto'),
            'matrix_available': bool(getattr(scoring_compiled_kernels, 'MATRIX_AVAILABLE', False)),
            'numba_available': bool(getattr(scoring_compiled_kernels, 'COMPILED_AVAILABLE', False)),
            'note': 'Kernel selection is an arithmetic accelerator only; canary scores protect equivalence.',
        },
        'active_engine_baseline': active_baseline,
        'canary_scores': canaries,
        'canary_variants': [
            {'variant': v.name, 'weights': dict(v.weights), 'bias': float(v.bias or 0.0)}
            for v in canary_variants
        ],
        'integrity_notes': [
            'Exact run: every variant in every completed chunk is scored; no sampling or pruning.',
            'Global top-K is produced by exact merge of every completed chunk top-K.',
            'Duplicate weight signatures emitted by the upstream generator are deduped by the upstream stream.',
            'Feature inputs are read from a canonical, fingerprinted feature/outcome store.',
            'Optional compiled kernels are permitted only as an arithmetic accelerator with NumPy fallback.',
            'The matrix kernel scores a batch as feature_matrix @ weight_matrix; it preserves the same scoring math.',
            'Direct weight-matrix generation avoids materializing Variant objects except for retained Top-K rows.',
            'Chunk workers use deterministic random-access variant ranges, so they do not replay/skip earlier variants.',
            'When variant_shard_dir is supplied, chunk workers use compact deduped shard manifests matching the old stream order.',
            'Pareto and diversity outputs are post-score selection views only; they never change model scores.',
        ],
    }
    manifest['run_hash'] = tournament_safety.stable_json_hash(manifest, 24)
    _write_json(paths['manifest'], manifest)
    _init_chunks(paths['ledger'], total, args.chunk_size)
    return {'run_dir': run_dir, 'manifest': paths['manifest'], 'ledger': paths['ledger'], 'total_variants': total}


def _current_code_hashes() -> dict:
    return {name: tournament_safety._file_sha256(name) for name in [
        'scoring_variant_lab.py',
        'scoring_variant_lab_fast.py',
        'scoring_variant_lab_massive.py',
        'scoring_variant_lab_exact_massive.py',
        'scoring_feature_store.py',
        'scoring_compiled_kernels.py',
        'scoring_pipeline_accelerator.py',
        'scoring_variant_matrix.py',
        'scoring_variant_random_access.py',
        'scoring_variant_shards.py',
        'active_engine_baseline.py',
    ]}


def _check_manifest_compatibility(run_dir: str, allow_hash_mismatch: bool = False) -> dict:
    paths = _paths(run_dir)
    manifest = _read_json(paths['manifest'])
    if not manifest:
        raise RuntimeError('missing run manifest')
    mismatches = []
    current_code = _current_code_hashes()
    for name, expected in (manifest.get('code_hashes') or {}).items():
        if current_code.get(name) != expected:
            mismatches.append(f'code_hash:{name}')
    csv_path = manifest.get('csv')
    if tournament_safety._file_sha256(csv_path) != manifest.get('csv_sha256'):
        mismatches.append('source_csv_sha256')
    feature_manifest = _read_json(os.path.join(paths['feature_dir'], 'feature_cache_manifest.json')) or {}
    if feature_manifest.get('feature_cache_hash') != (manifest.get('feature_cache') or {}).get('feature_cache_hash'):
        mismatches.append('feature_cache_hash')
    if mismatches and not allow_hash_mismatch:
        raise RuntimeError(f"manifest compatibility check failed: {','.join(mismatches)}")
    return {'ok': not mismatches, 'mismatches': mismatches}


def run_chunks(args) -> dict:
    run_dir = os.path.abspath(args.run_dir)
    paths = _paths(run_dir)
    compat = _check_manifest_compatibility(run_dir, args.allow_hash_mismatch)
    lock_path = _acquire_lock(run_dir, disabled=args.no_run_lock)
    manifest = _read_json(paths['manifest'])
    try:
        lease_limit = args.max_chunks if args.max_chunks and args.max_chunks > 0 else 10**9
        lease_statuses = ('failed',) if args.retry_failed_only else ('pending', 'failed')
        pending = _lease_chunks(paths['ledger'], lease_limit, statuses=lease_statuses, worker_id=args.worker_id)
        args_dict = {
            'no_existing': (manifest.get('generator_config') or {}).get('no_existing', False),
            'no_broad_full': (manifest.get('generator_config') or {}).get('no_broad_full', False),
            'no_v29_full': (manifest.get('generator_config') or {}).get('no_v29_full', False),
            'include_expanded_full': (manifest.get('generator_config') or {}).get('include_expanded_full', False),
            'include_active_local': (manifest.get('generator_config') or {}).get('include_active_local', False),
            'include_creative_full': (manifest.get('generator_config') or {}).get('include_creative_full', False),
            'include_core2_creative': (manifest.get('generator_config') or {}).get('include_core2_creative', False),
            'include_core2_guarded': (manifest.get('generator_config') or {}).get('include_core2_guarded', False),
            'include_micro_local': (manifest.get('generator_config') or {}).get('include_micro_local', False),
            'include_replay_safe_local': (manifest.get('generator_config') or {}).get('include_replay_safe_local', False),
            'random_access_variants': (manifest.get('generator_config') or {}).get('random_access_variants', True),
            'variant_shard_dir': (manifest.get('generator_config') or {}).get('variant_shard_dir'),
            'disable_direct_matrix': (manifest.get('generator_config') or {}).get('disable_direct_matrix', False),
            'variant_index_offset': int((manifest.get('generator_config') or {}).get('variant_index_offset') or 0),
            'step1_retention_mode': (manifest.get('generator_config') or {}).get('step1_retention_mode', 'raw'),
            'starting_balance': manifest.get('starting_balance', 100000.0),
            'top_k': manifest.get('top_k', args.top_k),
            'batch_size': manifest.get('batch_size', args.batch_size),
        }
        results = []
        workers = worker_policy.clamp_workers(args.workers)
        if workers > 1 and len(pending) > 1:
            with ProcessPoolExecutor(max_workers=worker_policy.clamp_workers(workers, len(pending))) as pool:
                futures = {pool.submit(process_chunk, run_dir, args_dict, chunk): chunk for chunk in pending}
                for fut in as_completed(futures):
                    row = fut.result()
                    results.append(row)
                    print(json.dumps({'event': 'chunk_done', **row}, sort_keys=True), flush=True)
        else:
            for chunk in pending:
                row = process_chunk(run_dir, args_dict, chunk)
                results.append(row)
                print(json.dumps({'event': 'chunk_done', **row}, sort_keys=True), flush=True)
        return {'run_dir': run_dir, 'processed_chunks': len(results), 'leased_chunks': len(pending), 'compatibility': compat, 'results': results}
    finally:
        _release_lock(lock_path)


def merge_chunks(args) -> dict:
    run_dir = os.path.abspath(args.run_dir)
    paths = _paths(run_dir)
    _check_manifest_compatibility(run_dir, args.allow_hash_mismatch)
    manifest = _read_json(paths['manifest'])
    top_k = int(args.top_k or manifest.get('top_k') or 10000)
    incomplete = _chunk_rows(paths['ledger'], ('pending', 'running', 'failed'))
    if incomplete and not args.allow_partial_merge:
        raise RuntimeError(f"refusing partial merge; incomplete chunks={len(incomplete)}")
    complete = _chunk_rows(paths['ledger'], ('complete',))
    heap: list = []
    seq = 0
    retention_key = 'step1_retention_score' if (manifest.get('generator_config') or {}).get('step1_retention_mode') == 'live-shape' else 'pnl'
    proof_chunks = []
    for chunk in complete:
        payload = _read_json(chunk.get('top_path') or '')
        if not payload:
            continue
        actual_hash = tournament_safety.stable_json_hash({
            'chunk_id': payload.get('chunk_id'),
            'start_index': payload.get('start_index'),
            'end_index': payload.get('end_index'),
            'run_hash': payload.get('run_hash'),
            'feature_cache_hash': payload.get('feature_cache_hash'),
            'results': payload.get('results') or [],
        }, 24)
        if actual_hash != chunk.get('chunk_hash') or actual_hash != payload.get('chunk_hash'):
            raise RuntimeError(f"chunk hash mismatch during merge: chunk_id={chunk.get('chunk_id')}")
        proof_chunks.append({
            'chunk_id': chunk.get('chunk_id'),
            'start_index': payload.get('start_index'),
            'end_index': payload.get('end_index'),
            'variant_count': payload.get('variant_count'),
            'chunk_hash': payload.get('chunk_hash'),
            'top_path': chunk.get('top_path'),
            'top_rows': len(payload.get('results') or []),
        })
        for row in payload.get('results') or []:
            _push_top(heap, row, top_k, seq, retention_key)
            seq += 1
    top = [item[2] for item in sorted(heap, key=lambda item: item[0], reverse=True)]
    for idx, row in enumerate(top, 1):
        row['global_lab_rank'] = idx
    active_baseline = manifest.get('active_engine_baseline') or {}
    active_lab = active_baseline.get('lab')
    thresholds = active_baseline.get('thresholds') or {}
    active_engine_baseline.annotate_lab_rows(
        top,
        active_lab,
        float(thresholds.get('min_pnl_margin') or getattr(args, 'beat_active_min_pnl_margin', 0.0) or 0.0),
        bool(thresholds.get('require_win_rate') or getattr(args, 'beat_active_require_win_rate', False)),
    )
    out = {
        'schema_version': 1,
        'run_dir': run_dir,
        'run_hash': manifest.get('run_hash'),
        'active_engine_baseline': active_baseline,
        'complete_chunks': len(complete),
        'partial_merge': bool(incomplete),
        'incomplete_chunks': len(incomplete),
        'top_k': top_k,
        'exact_merge_note': 'Exact global top-K from all completed chunk top-K files.',
        'results': top,
    }
    out['merge_hash'] = tournament_safety.stable_json_hash({
        'run_hash': out['run_hash'],
        'complete_chunks': out['complete_chunks'],
        'top_k': top_k,
        'results': top,
    }, 24)
    _write_json(paths['merged'], out)
    _write_global_csv(paths['merged_csv'], top)
    beats_active = _write_beats_active_outputs(paths, top, active_baseline, out['run_hash'], top_k)
    pareto = _write_pareto_outputs(paths, top, manifest.get('run_hash'), int(args.pareto_top_n or min(1000, top_k)))
    proof = {
        'schema_version': 1,
        'run_dir': run_dir,
        'run_hash': manifest.get('run_hash'),
        'feature_cache_hash': (manifest.get('feature_cache') or {}).get('feature_cache_hash'),
        'complete_chunks': len(complete),
        'partial_merge': bool(incomplete),
        'incomplete_chunks': len(incomplete),
        'chunk_files': proof_chunks,
        'global_topk_path': paths['merged'],
        'global_topk_csv_path': paths['merged_csv'],
        'pareto_topk_path': paths['pareto'],
        'pareto_topk_csv_path': paths['pareto_csv'],
        'beats_active_topk_path': paths['beats_active'],
        'beats_active_topk_csv_path': paths['beats_active_csv'],
        'merge_hash': out['merge_hash'],
        'beats_active_hash': beats_active.get('beats_active_hash'),
        'pareto_hash': pareto.get('pareto_hash'),
        'proof_hash': tournament_safety.stable_json_hash({
            'run_hash': manifest.get('run_hash'),
            'chunks': proof_chunks,
            'merge_hash': out['merge_hash'],
            'beats_active_hash': beats_active.get('beats_active_hash'),
            'pareto_hash': pareto.get('pareto_hash'),
        }, 24),
    }
    _write_json(paths['merge_proof'], proof)
    return {
        'run_dir': run_dir,
        'out': paths['merged'],
        'csv': paths['merged_csv'],
        'proof': paths['merge_proof'],
        'pareto': pareto,
        'beats_active': beats_active,
        'complete_chunks': len(complete),
        'results': len(top),
    }


def partial_snapshot(args) -> dict:
    args.allow_partial_merge = True
    result = merge_chunks(args)
    run_dir = os.path.abspath(args.run_dir)
    paths = _paths(run_dir)
    partial_json = os.path.join(run_dir, 'partial_global_topk.json')
    partial_csv = os.path.join(run_dir, 'partial_global_topk.csv')
    payload = _read_json(paths['merged'], {})
    payload['partial_snapshot'] = True
    _write_json(partial_json, payload)
    rows = payload.get('results') or []
    _write_global_csv(partial_csv, rows)
    return {'run_dir': run_dir, 'out': partial_json, 'csv': partial_csv, 'source_merge': result}


def _write_global_csv(path: str, rows: list[dict]) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fields = [
        'global_lab_rank', 'variant', 'model_id', 'family_id', 'step1_retention_score',
        'active_side_agreement_rate', 'active_side_flip_rate', 'weight_similarity_to_active',
        'variant_original_flip_rate', 'pnl', 'trades',
        'wins', 'losses', 'win_rate_pct', 'flipped', 'active_lab_pnl',
        'pnl_delta_vs_active_lab', 'beats_active_lab', 'beats_active_lab_strict',
        'bias',
    ]
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k) for k in fields})
    return path


def _write_beats_active_outputs(paths: dict, rows: list[dict], active_baseline: dict | None,
                                run_hash: str | None, top_k: int) -> dict:
    active_lab = (active_baseline or {}).get('lab')
    if not active_lab:
        payload = {
            'schema_version': 1,
            'run_hash': run_hash,
            'top_k_source': top_k,
            'active_engine_baseline': active_baseline,
            'results': [],
            'note': 'No active Step 1 baseline was available for this run.',
        }
    else:
        thresholds = (active_baseline or {}).get('thresholds') or {}
        strict_key = 'beats_active_lab_strict' if thresholds.get('require_win_rate') else 'beats_active_lab'
        beaters = [dict(row) for row in rows if row.get(strict_key)]
        payload = {
            'schema_version': 1,
            'run_hash': run_hash,
            'top_k_source': top_k,
            'active_engine_baseline': active_baseline,
            'selection_key': strict_key,
            'beats_active_count_in_topk': len(beaters),
            'results': beaters,
            'note': (
                'Rows are selected from the exact global top-K only. Increase --top-k if you '
                'want a wider retained set of active-beating variants.'
            ),
        }
    payload['beats_active_hash'] = tournament_safety.stable_json_hash({
        'run_hash': run_hash,
        'top_k_source': top_k,
        'active': active_lab,
        'results': payload.get('results') or [],
    }, 24)
    _write_json(paths['beats_active'], payload)
    _write_global_csv(paths['beats_active_csv'], payload.get('results') or [])
    return {
        'out': paths['beats_active'],
        'csv': paths['beats_active_csv'],
        'results': len(payload.get('results') or []),
        'beats_active_hash': payload['beats_active_hash'],
    }


def _pareto_dominates(a: dict, b: dict) -> bool:
    a_pnl = float(a.get('pnl') or 0.0)
    b_pnl = float(b.get('pnl') or 0.0)
    a_wr = float(a.get('win_rate_pct') or 0.0)
    b_wr = float(b.get('win_rate_pct') or 0.0)
    a_trades = int(a.get('trades') or 0)
    b_trades = int(b.get('trades') or 0)
    a_flipped = int(a.get('flipped') or 0)
    b_flipped = int(b.get('flipped') or 0)
    better_or_equal = (
        a_pnl >= b_pnl
        and a_wr >= b_wr
        and a_trades >= b_trades
        and a_flipped <= b_flipped
    )
    strictly_better = (
        a_pnl > b_pnl
        or a_wr > b_wr
        or a_trades > b_trades
        or a_flipped < b_flipped
    )
    return better_or_equal and strictly_better


def _pareto_front(rows: list[dict], limit: int) -> list[dict]:
    front = []
    for row in rows:
        dominated = False
        for other in rows:
            if other is row:
                continue
            if _pareto_dominates(other, row):
                dominated = True
                break
        if not dominated:
            front.append(dict(row))
        if len(front) >= limit:
            break
    front.sort(key=lambda r: (float(r.get('pnl') or 0.0), float(r.get('win_rate_pct') or 0.0)), reverse=True)
    for idx, row in enumerate(front, 1):
        row['pareto_rank'] = idx
    return front


def _write_pareto_outputs(paths: dict, rows: list[dict], run_hash: str | None, limit: int) -> dict:
    diverse = tournament_safety.select_diverse_finalists(rows, min(limit, len(rows)), per_family=3)
    front = _pareto_front(diverse, min(limit, len(diverse)))
    payload = {
        'schema_version': 1,
        'run_hash': run_hash,
        'limit': limit,
        'selection_note': (
            'Post-score Pareto/diversity view. It does not prune the exact run; '
            'it only prioritizes already-scored models for downstream validation.'
        ),
        'results': front,
    }
    payload['pareto_hash'] = tournament_safety.stable_json_hash({
        'run_hash': run_hash,
        'limit': limit,
        'results': front,
    }, 24)
    _write_json(paths['pareto'], payload)
    fields = [
        'pareto_rank', 'global_lab_rank', 'variant', 'model_id', 'family_id',
        'step1_retention_score', 'active_side_agreement_rate', 'active_side_flip_rate',
        'weight_similarity_to_active', 'variant_original_flip_rate',
        'pnl', 'trades', 'wins', 'losses', 'win_rate_pct', 'flipped',
        'active_lab_pnl', 'pnl_delta_vs_active_lab', 'beats_active_lab',
        'beats_active_lab_strict', 'bias',
    ]
    os.makedirs(os.path.dirname(paths['pareto_csv']), exist_ok=True)
    with open(paths['pareto_csv'], 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in front:
            writer.writerow({k: row.get(k) for k in fields})
    return {'out': paths['pareto'], 'csv': paths['pareto_csv'], 'results': len(front), 'pareto_hash': payload['pareto_hash']}


def verify_run(args) -> dict:
    run_dir = os.path.abspath(args.run_dir)
    paths = _paths(run_dir)
    compat = _check_manifest_compatibility(run_dir, args.allow_hash_mismatch)
    manifest = _read_json(paths['manifest'])
    complete = _chunk_rows(paths['ledger'], ('complete',))
    incomplete = _chunk_rows(paths['ledger'], ('pending', 'running', 'failed'))
    errors = []
    verified_chunks = []
    for chunk in complete:
        payload = _read_json(chunk.get('top_path') or '')
        if not payload:
            errors.append(f"missing_chunk_payload:{chunk.get('chunk_id')}")
            continue
        actual_hash = tournament_safety.stable_json_hash({
            'chunk_id': payload.get('chunk_id'),
            'start_index': payload.get('start_index'),
            'end_index': payload.get('end_index'),
            'run_hash': payload.get('run_hash'),
            'feature_cache_hash': payload.get('feature_cache_hash'),
            'results': payload.get('results') or [],
        }, 24)
        if actual_hash != payload.get('chunk_hash'):
            errors.append(f"payload_chunk_hash_mismatch:{chunk.get('chunk_id')}")
        if actual_hash != chunk.get('chunk_hash'):
            errors.append(f"ledger_chunk_hash_mismatch:{chunk.get('chunk_id')}")
        if payload.get('run_hash') != manifest.get('run_hash'):
            errors.append(f"run_hash_mismatch:{chunk.get('chunk_id')}")
        if payload.get('canary_scores') != manifest.get('canary_scores'):
            errors.append(f"canary_mismatch:{chunk.get('chunk_id')}")
        verified_chunks.append(chunk.get('chunk_id'))
    merged = _read_json(paths['merged'])
    proof = _read_json(paths['merge_proof'])
    if merged:
        expected_merge_hash = tournament_safety.stable_json_hash({
            'run_hash': merged.get('run_hash'),
            'complete_chunks': merged.get('complete_chunks'),
            'top_k': merged.get('top_k'),
            'results': merged.get('results') or [],
        }, 24)
        if expected_merge_hash != merged.get('merge_hash'):
            errors.append('global_topk_merge_hash_mismatch')
        if not args.allow_partial_merge and merged.get('incomplete_chunks'):
            errors.append('global_topk_is_partial')
    if proof:
        if merged and proof.get('merge_hash') != merged.get('merge_hash'):
            errors.append('proof_merge_hash_mismatch')
        expected_proof_hash = tournament_safety.stable_json_hash({
            'run_hash': manifest.get('run_hash'),
            'chunks': proof.get('chunk_files') or [],
            'merge_hash': proof.get('merge_hash'),
            'beats_active_hash': proof.get('beats_active_hash'),
            'pareto_hash': proof.get('pareto_hash'),
        }, 24)
        if expected_proof_hash != proof.get('proof_hash'):
            errors.append('proof_hash_mismatch')
        proof_chunk_ids = {row.get('chunk_id') for row in proof.get('chunk_files') or []}
        complete_ids = {row.get('chunk_id') for row in complete}
        if proof_chunk_ids != complete_ids and not args.allow_partial_merge:
            errors.append('proof_chunk_set_mismatch')
    elif merged:
        errors.append('missing_merge_proof')
    payload = {
        'run_dir': run_dir,
        'ok': not errors and (args.allow_partial_merge or not incomplete),
        'errors': errors,
        'compatibility': compat,
        'complete_chunks': len(complete),
        'incomplete_chunks': len(incomplete),
        'verified_chunks': len(verified_chunks),
        'merged_exists': bool(merged),
        'proof_exists': bool(proof),
    }
    if incomplete and not args.allow_partial_merge:
        payload['errors'].append(f'incomplete_chunks:{len(incomplete)}')
        payload['ok'] = False
    return payload


def create_step2_package(args) -> dict:
    run_dir = os.path.abspath(args.run_dir)
    paths = _paths(run_dir)
    verify_payload = verify_run(args)
    if not verify_payload.get('ok') and not args.allow_partial_merge:
        raise RuntimeError('verify failed; refusing to create Step 2 package')
    topk = _read_json(paths['merged'])
    if not topk:
        raise RuntimeError('missing global_topk.json; run merge first')
    manifest = _read_json(paths['manifest'], {}) or {}
    limit = int(args.step2_top_n or 10000)
    package_dir = os.path.join(run_dir, 'step2_package')
    os.makedirs(package_dir, exist_ok=True)
    step2_json = os.path.join(package_dir, f'step2_top{limit}.json')
    rows = (topk.get('results') or [])[:limit]
    payload = {
        'schema_version': 1,
        'source_run_dir': run_dir,
        'source_run_hash': topk.get('run_hash'),
        'source_merge_hash': topk.get('merge_hash'),
        'active_engine_baseline': topk.get('active_engine_baseline') or manifest.get('active_engine_baseline'),
        'top_n': limit,
        'results': rows,
        'decision_tape_command': [
            'python', 'decision_tape_validate_top.py',
            '--input', step2_json,
            '--limit', str(limit),
        ],
        'artifacts': {
            'run_manifest': paths['manifest'],
            'merge_proof': paths['merge_proof'],
            'global_topk': paths['merged'],
            'global_topk_csv': paths['merged_csv'],
            'pareto_topk': paths['pareto'],
            'pareto_topk_csv': paths['pareto_csv'],
            'beats_active_topk': paths['beats_active'],
            'beats_active_topk_csv': paths['beats_active_csv'],
        },
    }
    _write_json(step2_json, payload)
    return {'run_dir': run_dir, 'out': step2_json, 'rows': len(rows), 'command': payload['decision_tape_command']}


def integrity_report(args) -> dict:
    run_dir = os.path.abspath(args.run_dir)
    paths = _paths(run_dir)
    manifest = _read_json(paths['manifest'], {}) or {}
    status_payload = status(args)
    verify_payload = verify_run(args)
    topk = _read_json(paths['merged'], {}) or {}
    proof = _read_json(paths['merge_proof'], {}) or {}
    active = topk.get('active_engine_baseline') or manifest.get('active_engine_baseline') or {}
    active_lab = active.get('lab') or {}
    lines = [
        '# Exact Massive Scoring Integrity Report',
        '',
        f"- Run directory: `{run_dir}`",
        f"- Run hash: `{manifest.get('run_hash')}`",
        f"- Feature cache hash: `{(manifest.get('feature_cache') or {}).get('feature_cache_hash')}`",
        f"- Verify OK: `{verify_payload.get('ok')}`",
        f"- Complete chunks: `{verify_payload.get('complete_chunks')}`",
        f"- Incomplete chunks: `{verify_payload.get('incomplete_chunks')}`",
        f"- Merge hash: `{topk.get('merge_hash')}`",
        f"- Proof hash: `{proof.get('proof_hash')}`",
        f"- Canonical store hash: `{(((manifest.get('feature_cache') or {}).get('canonical_feature_store') or {}).get('store_hash'))}`",
        f"- Active lab baseline P/L: `{active_lab.get('pnl')}`",
        '',
        '## Forecast',
        '',
        '```json',
        json.dumps(status_payload.get('forecast'), indent=2, sort_keys=True),
        '```',
        '',
        '## Top Models',
        '',
    ]
    for row in (topk.get('results') or [])[:10]:
        lines.append(f"- #{row.get('global_lab_rank')}: `{row.get('variant')}` pnl={row.get('pnl')} delta_vs_active={row.get('pnl_delta_vs_active_lab')} win_rate={row.get('win_rate_pct')} model_id=`{row.get('model_id')}`")
    lines.extend([
        '',
        '## Integrity Notes',
        '',
        '- Exact screen: no sampling, pruning, mixed precision, or approximate merge.',
        '- Chunk files are hash-checked against the SQLite ledger and merge proof.',
        '- Canary scores are verified for every completed chunk.',
        '- Canonical feature/outcome store hashes are pinned in the run manifest.',
        '- Pareto outputs are downstream prioritization only; they do not alter scores.',
    ])
    out = os.path.join(run_dir, 'integrity_report.md')
    with open(out, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')
    return {'run_dir': run_dir, 'out': out, 'verify_ok': verify_payload.get('ok')}


def cleanup_run(args) -> dict:
    run_dir = os.path.abspath(args.run_dir)
    paths = _paths(run_dir)
    verify_payload = verify_run(args)
    if not verify_payload.get('ok') and not args.allow_partial_merge:
        raise RuntimeError('verify failed; refusing cleanup')
    removed = []
    if args.cleanup_chunks:
        for name in os.listdir(paths['chunks_dir']) if os.path.exists(paths['chunks_dir']) else []:
            path = os.path.join(paths['chunks_dir'], name)
            if os.path.isfile(path):
                os.remove(path)
                removed.append(path)
    return {'run_dir': run_dir, 'removed_files': removed, 'verify_ok': verify_payload.get('ok')}


def status(args) -> dict:
    run_dir = os.path.abspath(args.run_dir)
    paths = _paths(run_dir)
    con = _connect_ledger(paths['ledger'])
    rows = list(con.execute("SELECT status, COUNT(*), COALESCE(SUM(variant_count),0), COALESCE(SUM(elapsed_seconds),0) FROM chunks GROUP BY status"))
    total_chunks = con.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
    total_expected = con.execute("SELECT COALESCE(SUM(end_index - start_index),0) FROM chunks").fetchone()[0]
    running = [
        {'chunk_id': r[0], 'worker_id': r[1], 'lease_started_at_epoch': r[2]}
        for r in con.execute("SELECT chunk_id,worker_id,lease_started_at_epoch FROM chunks WHERE status='running' ORDER BY chunk_id")
    ]
    con.close()
    by_status = {r[0]: {'chunks': r[1], 'variants': int(r[2]), 'elapsed_seconds': round(float(r[3]), 3)} for r in rows}
    complete = by_status.get('complete', {})
    complete_chunks = int(complete.get('chunks') or 0)
    complete_variants = int(complete.get('variants') or 0)
    elapsed = float(complete.get('elapsed_seconds') or 0.0)
    variants_per_sec = round(complete_variants / elapsed, 3) if elapsed > 0 else None
    remaining_variants = max(0, int(total_expected) - complete_variants)
    eta_seconds = round(remaining_variants / variants_per_sec, 2) if variants_per_sec else None
    return {
        'run_dir': run_dir,
        'status': by_status,
        'forecast': {
            'total_chunks': total_chunks,
            'complete_chunks': complete_chunks,
            'total_variants_expected': int(total_expected),
            'complete_variants': complete_variants,
            'remaining_variants': remaining_variants,
            'variants_per_sec_completed_chunks': variants_per_sec,
            'eta_seconds': eta_seconds,
            'completion_pct': round(100 * complete_chunks / total_chunks, 3) if total_chunks else None,
        },
        'running_leases': running,
        'manifest': paths['manifest'],
        'merged': paths['merged'],
    }


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Exact chunked massive scoring-lab runner.')
    ap.add_argument('--mode', choices=['init', 'run', 'merge', 'status', 'verify', 'reset', 'snapshot', 'package-step2', 'report', 'cleanup', 'all'], default='all')
    ap.add_argument('--csv', default=DEFAULT_CSV)
    ap.add_argument('--feature-store', default=None,
                    help='Optional canonical feature-store manifest or directory to reuse for init.')
    ap.add_argument('--variant-shard-dir', default=None,
                    help='Optional deduped variant shard manifest/dir from scoring_variant_shards.py.')
    ap.add_argument('--out-dir', default=DEFAULT_OUT_DIR)
    ap.add_argument('--run-dir', default=None)
    ap.add_argument('--name', default=None)
    ap.add_argument('--starting-balance', type=float, default=100000.0)
    ap.add_argument('--top-k', type=int, default=0,
                    help='Top-K to keep. Defaults to 10000 for init/all, or manifest top_k for merge/run.')
    ap.add_argument('--chunk-size', type=int, default=100000)
    ap.add_argument('--batch-size', type=int, default=20000)
    ap.add_argument('--workers', type=int, default=1)
    ap.add_argument('--max-chunks', type=int, default=0)
    ap.add_argument('--max-variants', type=int, default=0)
    ap.add_argument('--variant-index-offset', type=int, default=0,
                    help='Start random-access variant generation at this global index offset.')
    ap.add_argument('--step1-retention-mode', choices=['raw', 'live-shape'], default='raw',
                    help='Rank retained Step 1 top-K rows by raw P/L or by live-shape composite.')
    ap.add_argument('--no-existing', action='store_true')
    ap.add_argument('--no-broad-full', action='store_true')
    ap.add_argument('--no-v29-full', action='store_true')
    ap.add_argument('--include-expanded-full', action='store_true',
                    help='Include the large all-indicator expanded variant family.')
    ap.add_argument('--include-active-local', action='store_true',
                    help='Include the deterministic local search universe around the active scoring profile.')
    ap.add_argument('--include-creative-full', action='store_true',
                    help='Include a stronger experimental all-indicator creative variant family.')
    ap.add_argument('--include-core2-creative', action='store_true',
                    help='Include active-profile variants with relative/vwap anchored and all other indicators creatively varied.')
    ap.add_argument('--include-core2-guarded', action='store_true',
                    help='Include live-edge-preserving variants around relative/vwap, BTC/momentum/chop, and restrained ticker/phase packs.')
    ap.add_argument('--include-micro-local', action='store_true',
                    help='Include true live-local variants with tiny core deltas and one small auxiliary knob at a time.')
    ap.add_argument('--include-replay-safe-local', action='store_true',
                    help='Include replay-safe local variants with up to three tiny safe moves and one capped auxiliary probe.')
    ap.add_argument('--disable-random-access-variants', action='store_true',
                    help='Diagnostic fallback. Use streaming/skip variant access instead of direct chunk ranges.')
    ap.add_argument('--disable-direct-matrix', action='store_true',
                    help='Diagnostic fallback. Materialize Variant objects before scoring instead of direct weight matrices.')
    ap.add_argument('--allow-partial-merge', action='store_true',
                    help='Diagnostic only. Official runs should merge only after every chunk is complete.')
    ap.add_argument('--allow-hash-mismatch', action='store_true',
                    help='Diagnostic only. Official runs should refuse manifest/code/data hash mismatches.')
    ap.add_argument('--no-run-lock', action='store_true',
                    help='Diagnostic only. Official runs should keep the run lock enabled.')
    ap.add_argument('--retry-failed-only', action='store_true',
                    help='Only lease failed chunks for mode=run.')
    ap.add_argument('--reset-failed', action='store_true',
                    help='For mode=reset, move failed chunks back to pending.')
    ap.add_argument('--reset-stale-running-minutes', type=float, default=0.0,
                    help='For mode=reset, move old running chunks back to pending.')
    ap.add_argument('--worker-id', default=None,
                    help='Optional worker identifier stored in chunk leases for distributed runs.')
    ap.add_argument('--canary-variants-json', default=None,
                    help='Optional JSON list of custom canary variants to verify for every chunk.')
    ap.add_argument('--step2-top-n', type=int, default=10000)
    ap.add_argument('--pareto-top-n', type=int, default=1000,
                    help='Post-merge Pareto/diverse view size. Does not change exact scoring.')
    ap.add_argument('--no-active-baseline', action='store_true',
                    help='Do not score/record the currently active engine as a Step 1 baseline.')
    ap.add_argument('--beat-active-min-pnl-margin', type=float, default=0.0,
                    help='Minimum Step 1 P/L margin required for beats-active outputs.')
    ap.add_argument('--beat-active-require-win-rate', action='store_true',
                    help='Strict beats-active outputs must also match/exceed active win rate.')
    ap.add_argument('--cleanup-chunks', action='store_true',
                    help='For mode=cleanup, remove compressed per-chunk top files after verification.')
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    payload = {}
    if args.mode in ('init', 'all'):
        payload['init'] = init_run(args)
        args.run_dir = payload['init']['run_dir']
    if args.mode in ('run', 'all'):
        if not args.run_dir:
            raise SystemExit('--run-dir required for mode=run')
        payload['run'] = run_chunks(args)
    if args.mode in ('merge', 'all'):
        if not args.run_dir:
            raise SystemExit('--run-dir required for mode=merge')
        payload['merge'] = merge_chunks(args)
    if args.mode == 'status':
        if not args.run_dir:
            raise SystemExit('--run-dir required for mode=status')
        payload['status'] = status(args)
    if args.mode == 'verify':
        if not args.run_dir:
            raise SystemExit('--run-dir required for mode=verify')
        payload['verify'] = verify_run(args)
    if args.mode == 'snapshot':
        if not args.run_dir:
            raise SystemExit('--run-dir required for mode=snapshot')
        payload['snapshot'] = partial_snapshot(args)
    if args.mode == 'package-step2':
        if not args.run_dir:
            raise SystemExit('--run-dir required for mode=package-step2')
        payload['package_step2'] = create_step2_package(args)
    if args.mode == 'report':
        if not args.run_dir:
            raise SystemExit('--run-dir required for mode=report')
        payload['report'] = integrity_report(args)
    if args.mode == 'cleanup':
        if not args.run_dir:
            raise SystemExit('--run-dir required for mode=cleanup')
        payload['cleanup'] = cleanup_run(args)
    if args.mode == 'reset':
        if not args.run_dir:
            raise SystemExit('--run-dir required for mode=reset')
        payload['reset'] = reset_chunks(args)
    print(json.dumps(payload, indent=2, sort_keys=True))
    if payload.get('verify') and not payload['verify'].get('ok'):
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
