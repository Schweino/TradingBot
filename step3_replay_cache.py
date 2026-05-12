"""Exact artifact cache helpers for Step 3 full replay finalists.

The cache is keyed by replay inputs and fingerprints. It never estimates or
changes replay results; it only copies exact summary/trade artifacts that were
produced by the same replay configuration.
"""
from __future__ import annotations

from output_paths import output_path

import hashlib
import json
import os
import shutil
from typing import Any


HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CACHE_DIR = output_path('postmortem', 'backtests', 'full_replay_exact_cache')
SCHEMA_VERSION = 1


def file_sha256(path: str) -> str | None:
    if not os.path.exists(path):
        return None
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def payload_sha256(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('utf-8')
    ).hexdigest()


def replay_key_payload(args, variant, profile_path: str, start: str, end: str, label: str) -> dict:
    return {
        'schema_version': SCHEMA_VERSION,
        'variant': {
            'name': variant.name,
            'weights': dict(variant.weights),
            'bias': float(variant.bias or 0.0),
            'profile_sha256': file_sha256(profile_path),
        },
        'window': {
            'label': label,
            'start': start,
            'end': end,
        },
        'replay': {
            'tickers': list(args.tickers),
            'feed': args.feed,
            'quote_mode': args.quote_mode,
            'btc_mode': args.btc_mode,
            'start_balance': float(args.start_balance),
            'validation_mode': 'finalist',
            'indicator_mode': 'fast',
            'entry_gate_mode': getattr(args, 'entry_gate_mode', None),
            'entry_gate_threshold_pct': getattr(args, 'entry_gate_threshold_pct', None),
            'brs_tp_multiplier': float(getattr(args, 'brs_tp_multiplier', 1.0) or 1.0),
            'momentum_tp_multiplier': float(getattr(args, 'momentum_tp_multiplier', 1.0) or 1.0),
            'high_conviction_tp_multiplier': float(getattr(args, 'high_conviction_tp_multiplier', 1.0) or 1.0),
            'entry_confirmation_delay_sec': int(float(getattr(args, 'entry_confirmation_delay_sec', 0.0) or 0.0)),
            'entry_confirmation_mode': getattr(args, 'entry_confirmation_mode', 'off'),
            'entry_confirmation_setup': getattr(args, 'entry_confirmation_setup', 'all'),
            'entry_confirmation_min_favorable_pct': float(getattr(args, 'entry_confirmation_min_favorable_pct', 0.0) or 0.0),
            'entry_confirmation_adverse_pct': float(getattr(args, 'entry_confirmation_adverse_pct', 0.0) or 0.0),
            'entry_confirmation_session_phase': getattr(args, 'entry_confirmation_session_phase', 'all'),
            'entry_confirmation_edge_threshold_pct': getattr(args, 'entry_confirmation_edge_threshold_pct', None),
            'entry_confirmation_rel_threshold_pct': getattr(args, 'entry_confirmation_rel_threshold_pct', None),
            'path_failure_after_sec': int(float(getattr(args, 'path_failure_after_sec', 0.0) or 0.0)),
            'path_failure_edge_threshold_pct': float(getattr(args, 'path_failure_edge_threshold_pct', 0.0) or 0.0),
            'path_failure_action': getattr(args, 'path_failure_action', 'off'),
            'path_failure_setup': getattr(args, 'path_failure_setup', 'all'),
            'path_failure_session_phase': getattr(args, 'path_failure_session_phase', 'all'),
            'path_failure_rel_threshold_pct': getattr(args, 'path_failure_rel_threshold_pct', None),
            'path_failure_window_sec': int(float(getattr(args, 'path_failure_window_sec', 0.0) or 0.0)),
            'path_failure_reduce_fraction': float(getattr(args, 'path_failure_reduce_fraction', 0.0) or 0.0),
            'use_prepared_events': True,
        },
        'fingerprints': {
            'step3_run': getattr(args, 'step3_run_fingerprint_sha256', None),
            'code_config': getattr(args, 'step3_code_config_sha256', None),
            'market_data': getattr(args, 'step3_data_fingerprint_sha256', None),
        },
    }


def replay_key(args, variant, profile_path: str, start: str, end: str, label: str) -> tuple[str, dict]:
    payload = replay_key_payload(args, variant, profile_path, start, end, label)
    return payload_sha256(payload), payload


def cache_paths(cache_dir: str, key: str) -> dict:
    root = os.path.join(cache_dir, key[:2], key)
    return {
        'root': root,
        'meta': os.path.join(root, 'meta.json'),
        'summary': os.path.join(root, 'summary.json'),
        'trades': os.path.join(root, 'trades.csv'),
    }


def restore(cache_dir: str, key: str, key_payload: dict, summary_path: str, csv_path: str) -> dict | None:
    paths = cache_paths(cache_dir, key)
    if not os.path.exists(paths['meta']) or not os.path.exists(paths['summary']):
        return None
    with open(paths['meta'], 'r', encoding='utf-8') as f:
        meta = json.load(f)
    if meta.get('key_payload') != key_payload:
        return None
    os.makedirs(os.path.dirname(summary_path), exist_ok=True)
    shutil.copy2(paths['summary'], summary_path)
    restored = {
        'cache_key': key,
        'cache_meta': paths['meta'],
        'summary_path': summary_path,
    }
    if os.path.exists(paths['trades']):
        shutil.copy2(paths['trades'], csv_path)
        restored['csv_path'] = csv_path
    with open(summary_path, 'r', encoding='utf-8') as f:
        restored['summary'] = json.load(f)
    return restored


def store(cache_dir: str, key: str, key_payload: dict, summary_path: str, csv_path: str) -> dict | None:
    if not os.path.exists(summary_path):
        return None
    paths = cache_paths(cache_dir, key)
    os.makedirs(paths['root'], exist_ok=True)
    shutil.copy2(summary_path, paths['summary'])
    copied = {'summary': paths['summary']}
    if os.path.exists(csv_path):
        shutil.copy2(csv_path, paths['trades'])
        copied['trades'] = paths['trades']
    meta = {
        'schema_version': SCHEMA_VERSION,
        'cache_key': key,
        'key_payload': key_payload,
        'artifacts': copied,
    }
    tmp = paths['meta'] + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(meta, f, indent=2, sort_keys=True)
    os.replace(tmp, paths['meta'])
    return {'cache_key': key, 'cache_meta': paths['meta'], 'artifacts': copied}
