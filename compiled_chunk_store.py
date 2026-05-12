from __future__ import annotations

import json
import os
import time
from typing import Any

import numpy as np

import step2_artifact_identity
import tournament_safety


HERE = os.path.dirname(os.path.abspath(__file__))
SCHEMA_VERSION = 1
DEFAULT_CHUNK_SEC = 300


ARRAY_NAMES = (
    'features',
    'ts',
    'ticker_code',
    'setup_code',
    'day_code',
    'original_side',
    'source_decision_code',
    'conviction_ok',
    'conviction_high',
    'long_pnl_pct',
    'short_pnl_pct',
    'long_held',
    'short_held',
    'long_reason_code',
    'short_reason_code',
    'ticker_session_return_pct',
    'vwap_dist',
    'vwap_dist_sigma',
    'btc_bullish',
    'btc_mom_60',
    'exec_score',
    'flow_fade_confirmed',
    'short_recovery_blocked',
)


def _safe_name(value: str) -> str:
    return ''.join(ch if ch.isalnum() or ch in '._-' else '_' for ch in str(value)).strip('._') or 'unknown'


def _bucket_start(ts: int, chunk_sec: int) -> int:
    ts_i = int(ts)
    return ts_i - (ts_i % int(chunk_sec))


def _reverse_map(mapping: dict[str, int]) -> dict[int, str]:
    return {int(v): str(k) for k, v in (mapping or {}).items()}


def write_chunks(
    compiled: dict[str, Any],
    store_dir: str,
    chunk_sec: int = DEFAULT_CHUNK_SEC,
) -> dict[str, Any]:
    rows = int(compiled.get('rows') or 0)
    chunk_root = os.path.join(store_dir, 'chunks')
    os.makedirs(chunk_root, exist_ok=True)
    if rows <= 0:
        manifest = {
            'schema_version': SCHEMA_VERSION,
            'created_at_epoch': int(time.time()),
            'chunk_sec': int(chunk_sec),
            'rows': 0,
            'chunk_count': 0,
            'chunks': [],
        }
        path = os.path.join(chunk_root, 'chunk_manifest.json')
        step2_artifact_identity.write_json(path, manifest)
        manifest['path'] = path
        return manifest

    ts = np.asarray(compiled['ts'])
    day_code = np.asarray(compiled.get('day_code', np.zeros(rows, dtype=np.int16)))
    day_names = _reverse_map(compiled.get('day_map') or {})
    bucket_keys: dict[tuple[str, int], list[int]] = {}
    for idx in range(rows):
        day = day_names.get(int(day_code[idx]), str(int(day_code[idx])))
        bucket = _bucket_start(int(ts[idx]), chunk_sec)
        bucket_keys.setdefault((day, bucket), []).append(idx)

    chunks = []
    for (day, bucket), indices in sorted(bucket_keys.items(), key=lambda item: (item[0][0], item[0][1])):
        idx_arr = np.asarray(indices, dtype=np.int64)
        day_dir = os.path.join(chunk_root, _safe_name(day))
        os.makedirs(day_dir, exist_ok=True)
        path = os.path.join(day_dir, f'{bucket}.npz')
        payload = {}
        for name in ARRAY_NAMES:
            if name in compiled:
                payload[name] = np.asarray(compiled[name])[idx_arr]
        identity = step2_artifact_identity.write_deterministic_npz(path, payload)
        chunks.append({
            'day': day,
            'bucket_start': int(bucket),
            'rows': int(len(indices)),
            'path': os.path.abspath(path),
            'sha256': identity.get('physical_sha256'),
            'array_payload_sha256': identity.get('array_payload_sha256'),
            'min_ts': int(np.min(ts[idx_arr])) if len(indices) else 0,
            'max_ts': int(np.max(ts[idx_arr])) if len(indices) else 0,
        })
    manifest = {
        'schema_version': SCHEMA_VERSION,
        'created_at_epoch': int(time.time()),
        'chunk_sec': int(chunk_sec),
        'rows': rows,
        'chunk_count': len(chunks),
        'chunks': chunks,
    }
    path = os.path.join(chunk_root, 'chunk_manifest.json')
    step2_artifact_identity.write_json(path, manifest)
    manifest['path'] = os.path.abspath(path)
    return manifest


def write_day_shards(
    compiled: dict[str, Any],
    store_dir: str,
    source_identities: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    rows = int(compiled.get('rows') or 0)
    shard_root = os.path.join(store_dir, 'day_shards')
    os.makedirs(shard_root, exist_ok=True)
    day_names = _reverse_map(compiled.get('day_map') or {})
    day_code = np.asarray(compiled.get('day_code', np.zeros(rows, dtype=np.int16)))
    ts = np.asarray(compiled.get('ts', np.zeros(rows, dtype=np.int64)))
    source_identities = source_identities or {}
    shards: list[dict[str, Any]] = []
    for day_idx, day in sorted(day_names.items(), key=lambda item: item[1]):
        indices = np.where(day_code == int(day_idx))[0]
        payload: dict[str, Any] = {}
        for name in ARRAY_NAMES:
            if name in compiled:
                payload[name] = np.asarray(compiled[name])[indices]
        if 'cooldown_by_setup' in compiled:
            payload['cooldown_by_setup'] = np.asarray(compiled['cooldown_by_setup'])
        day_dir = os.path.join(shard_root, _safe_name(day))
        os.makedirs(day_dir, exist_ok=True)
        arrays_path = os.path.join(day_dir, 'arrays.npz')
        identity = step2_artifact_identity.write_deterministic_npz(arrays_path, payload)
        source_rows = {
            path: identity_row
            for path, identity_row in source_identities.items()
            if str(identity_row.get('path') or path).endswith(f'_{day}.jsonl.gz')
        }
        manifest = {
            'schema_version': SCHEMA_VERSION,
            'day': day,
            'rows': int(len(indices)),
            'path': os.path.abspath(arrays_path),
            'sha256': identity.get('physical_sha256'),
            'array_payload_sha256': identity.get('array_payload_sha256'),
            'min_ts': int(np.min(ts[indices])) if len(indices) else 0,
            'max_ts': int(np.max(ts[indices])) if len(indices) else 0,
            'source_identities': source_rows,
        }
        manifest_path = os.path.join(day_dir, 'manifest.json')
        step2_artifact_identity.write_json(manifest_path, manifest)
        manifest['manifest_path'] = os.path.abspath(manifest_path)
        shards.append(manifest)
    aggregate = {
        'schema_version': SCHEMA_VERSION,
        'created_at_epoch': int(time.time()),
        'rows': rows,
        'day_count': len(shards),
        'shards': shards,
    }
    aggregate['day_shards_hash'] = step2_artifact_identity.stable_json_hash({
        'schema_version': aggregate['schema_version'],
        'shards': [
            {
                'day': row.get('day'),
                'rows': row.get('rows'),
                'array_payload_sha256': row.get('array_payload_sha256'),
                'source_semantic_sha256': {
                    path: src.get('semantic_sha256')
                    for path, src in (row.get('source_identities') or {}).items()
                },
            }
            for row in shards
        ],
    })
    path = os.path.join(shard_root, 'day_shard_manifest.json')
    step2_artifact_identity.write_json(path, aggregate)
    aggregate['path'] = os.path.abspath(path)
    return aggregate


def changed_buckets_from_market_store(market_manifest: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(market_manifest, dict):
        return []
    out = []
    for row in market_manifest.get('changed_partitions') or []:
        try:
            out.append({
                'bucket_start': int(row.get('bucket_start') or 0),
                'bucket_ct': row.get('bucket_ct'),
                'rows': row.get('rows'),
                'path': row.get('path'),
                'old_hash': row.get('old_hash'),
                'new_hash': row.get('new_hash'),
            })
        except Exception:
            continue
    return out
