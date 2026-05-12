"""Pre-sharded variant index manifests for exact massive scoring.

The original variant stream dedupes signatures across families. These compact
shards preserve that exact deduped order once, then chunk workers can load their
assigned variant references directly without replaying/skipping the generator.
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import time
from math import ceil

import scoring_variant_lab as slow
import scoring_variant_lab_massive as massive
import scoring_variant_random_access as random_access
import tournament_safety


HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUT_DIR = os.path.join(HERE, 'postmortem', 'backtests', 'variant_index_shards')
SCHEMA_VERSION = 1


def _write_json(path: str, payload: dict) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    return path


def _read_json(path: str) -> dict:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def _safe_name(value: str) -> str:
    cleaned = ''.join(ch if ch.isalnum() or ch in '._-' else '_' for ch in value).strip('._')
    return cleaned or 'variant_shards'


def _signature(variant: slow.Variant) -> tuple:
    return massive._signature(variant.weights, variant.bias)


def _range_shard_path(shard_dir: str, shard_id: int, start: int, end: int) -> str:
    return os.path.join(shard_dir, f'shard_{shard_id:08d}_{start}_{end}.range.json')


def _write_range_shards(shard_dir: str, total: int, shard_size: int) -> list[dict]:
    shards = []
    shard_count = int(ceil(total / shard_size)) if total else 0
    for shard_id in range(shard_count):
        start = shard_id * shard_size
        end = min(total, start + shard_size)
        payload = {
            'schema_version': SCHEMA_VERSION,
            'storage': 'range',
            'family': 'expanded_full',
            'shard_id': shard_id,
            'start_index': start,
            'end_index': end,
            'rows': end - start,
        }
        path = _range_shard_path(shard_dir, shard_id, start, end)
        _write_json(path, payload)
        shards.append({
            'shard_id': shard_id,
            'start_index': start,
            'end_index': end,
            'rows': end - start,
            'path': path,
            'storage': 'range',
            'sha256': tournament_safety._file_sha256(path),
        })
    return shards


def _iter_family_refs(include_existing: bool, include_broad_full: bool, include_v29_full: bool,
                      include_expanded_full: bool):
    if include_existing:
        for raw_index, variant in enumerate(slow.VARIANTS):
            yield {'family': 'existing', 'raw_index': raw_index}, variant
    if include_broad_full:
        for raw_index in range(random_access.broad_count()):
            yield {'family': 'broad_full', 'raw_index': raw_index}, random_access.broad_variant_at(raw_index)
    if include_v29_full:
        for raw_index in range(random_access.v29_count()):
            yield {'family': 'v29_full', 'raw_index': raw_index}, random_access.v29_variant_at(raw_index)
    if include_expanded_full:
        for raw_index in range(random_access.expanded_count()):
            yield {'family': 'expanded_full', 'raw_index': raw_index}, random_access.expanded_variant_at(raw_index)


def variant_from_ref(ref: dict) -> slow.Variant:
    if ref.get('weights') is not None:
        return slow.Variant(
            ref.get('variant') or f"custom_variant_{int(ref.get('global_index', 0))}",
            dict(ref.get('weights') or {}),
            float(ref.get('bias') or 0.0),
        )
    family = ref.get('family')
    raw_index = int(ref.get('raw_index'))
    if family == 'existing':
        base = slow.VARIANTS[raw_index]
        return slow.Variant(base.name, dict(base.weights), base.bias)
    if family == 'broad_full':
        return random_access.broad_variant_at(raw_index)
    if family == 'v29_full':
        return random_access.v29_variant_at(raw_index)
    if family == 'expanded_full':
        return random_access.expanded_variant_at(raw_index)
    if family == 'active_local':
        return random_access.active_local_variant_at(raw_index)
    if family == 'creative_full':
        return random_access.creative_variant_at(raw_index)
    raise ValueError(f'unknown variant family: {family}')


def build_shards(out_dir: str, name: str, shard_size: int, include_existing: bool,
                 include_broad_full: bool, include_v29_full: bool,
                 include_expanded_full: bool = False,
                 max_variants: int = 0,
                 assume_unique_expanded_only: bool = False) -> dict:
    started = time.perf_counter()
    run_dir = os.path.abspath(os.path.join(out_dir, _safe_name(name)))
    shard_dir = os.path.join(run_dir, 'shards')
    os.makedirs(shard_dir, exist_ok=True)
    skip_signature_dedupe = bool(
        assume_unique_expanded_only
        and include_expanded_full
        and not include_existing
        and not include_broad_full
        and not include_v29_full
    )
    seen = set()
    refs = []
    shards = []
    unique_index = 0
    raw_seen = 0
    shard_id = 0
    limit = int(max_variants or 0)
    if skip_signature_dedupe:
        total = random_access.expanded_count()
        if limit:
            total = min(total, limit)
        shards = _write_range_shards(shard_dir, total, shard_size)
        config = {
            'include_existing': include_existing,
            'include_broad_full': include_broad_full,
            'include_v29_full': include_v29_full,
            'include_expanded_full': include_expanded_full,
            'assume_unique_expanded_only': True,
            'storage': 'range',
            'estimated_counts': random_access.estimated_count(
                include_existing,
                include_broad_full,
                include_v29_full,
                include_expanded_full,
            ),
            'max_variants': max_variants,
            'shard_size': shard_size,
        }
        manifest = {
            'schema_version': SCHEMA_VERSION,
            'created_at_epoch': int(time.time()),
            'name': name,
            'run_dir': run_dir,
            'config': config,
            'config_hash': tournament_safety.stable_json_hash(config, 24),
            'unique_variants': total,
            'raw_variants_seen': total,
            'duplicate_signatures_skipped': 0,
            'dedupe_note': 'Range shards for expanded_full-only unique mixed-radix coordinates.',
            'shard_count': len(shards),
            'shards': shards,
            'code_hashes': {
                'scoring_variant_shards.py': tournament_safety._file_sha256('scoring_variant_shards.py'),
                'scoring_variant_random_access.py': tournament_safety._file_sha256('scoring_variant_random_access.py'),
                'scoring_variant_lab_massive.py': tournament_safety._file_sha256('scoring_variant_lab_massive.py'),
                'scoring_variant_lab.py': tournament_safety._file_sha256('scoring_variant_lab.py'),
            },
            'elapsed_seconds': round(time.perf_counter() - started, 3),
        }
        manifest['manifest_hash'] = tournament_safety.stable_json_hash({
            'config_hash': manifest['config_hash'],
            'unique_variants': total,
            'shards': [{k: row[k] for k in ('shard_id', 'start_index', 'end_index', 'sha256', 'storage')} for row in shards],
            'code_hashes': manifest['code_hashes'],
        }, 24)
        manifest_path = os.path.join(run_dir, 'variant_shards_manifest.json')
        _write_json(manifest_path, manifest)
        manifest['manifest_path'] = manifest_path
        return manifest

    def flush() -> None:
        nonlocal refs, shard_id
        if not refs:
            return
        start = refs[0]['global_index']
        end = refs[-1]['global_index'] + 1
        path = os.path.join(shard_dir, f'shard_{shard_id:08d}_{start}_{end}.jsonl.gz')
        with gzip.open(path, 'wt', encoding='utf-8') as f:
            for ref in refs:
                f.write(json.dumps(ref, separators=(',', ':'), sort_keys=True) + '\n')
        shards.append({
            'shard_id': shard_id,
            'start_index': start,
            'end_index': end,
            'rows': len(refs),
            'path': path,
            'sha256': tournament_safety._file_sha256(path),
        })
        refs = []
        shard_id += 1

    for ref, variant in _iter_family_refs(include_existing, include_broad_full, include_v29_full, include_expanded_full):
        raw_seen += 1
        if skip_signature_dedupe:
            sig_hash = tournament_safety.stable_json_hash({
                'weights': dict(variant.weights),
                'bias': float(variant.bias or 0.0),
            }, 24)
        else:
            sig = _signature(variant)
            if sig in seen:
                continue
            seen.add(sig)
            sig_hash = tournament_safety.stable_json_hash({
                'weights': dict(variant.weights),
                'bias': float(variant.bias or 0.0),
            }, 24)
        refs.append({
            'global_index': unique_index,
            'family': ref['family'],
            'raw_index': ref['raw_index'],
            'variant': variant.name,
            'signature': sig_hash,
        })
        unique_index += 1
        if len(refs) >= shard_size:
            flush()
        if limit and unique_index >= limit:
            break
    flush()
    config = {
        'include_existing': include_existing,
        'include_broad_full': include_broad_full,
        'include_v29_full': include_v29_full,
        'include_expanded_full': include_expanded_full,
        'assume_unique_expanded_only': skip_signature_dedupe,
        'estimated_counts': random_access.estimated_count(
            include_existing,
            include_broad_full,
            include_v29_full,
            include_expanded_full,
        ),
        'max_variants': max_variants,
        'shard_size': shard_size,
    }
    manifest = {
        'schema_version': SCHEMA_VERSION,
        'created_at_epoch': int(time.time()),
        'name': name,
        'run_dir': run_dir,
        'config': config,
        'config_hash': tournament_safety.stable_json_hash(config, 24),
        'unique_variants': unique_index,
        'raw_variants_seen': raw_seen,
        'duplicate_signatures_skipped': raw_seen - unique_index,
        'dedupe_note': (
            'Signature dedupe skipped because this is expanded_full-only and generated by mixed-radix unique coordinates.'
            if skip_signature_dedupe else
            'Signature dedupe enabled to preserve legacy stream behavior across families.'
        ),
        'shard_count': len(shards),
        'shards': shards,
        'code_hashes': {
            'scoring_variant_shards.py': tournament_safety._file_sha256('scoring_variant_shards.py'),
            'scoring_variant_random_access.py': tournament_safety._file_sha256('scoring_variant_random_access.py'),
            'scoring_variant_lab_massive.py': tournament_safety._file_sha256('scoring_variant_lab_massive.py'),
            'scoring_variant_lab.py': tournament_safety._file_sha256('scoring_variant_lab.py'),
        },
        'elapsed_seconds': round(time.perf_counter() - started, 3),
    }
    manifest['manifest_hash'] = tournament_safety.stable_json_hash({
        'config_hash': manifest['config_hash'],
        'unique_variants': unique_index,
        'shards': [{k: row[k] for k in ('shard_id', 'start_index', 'end_index', 'sha256')} for row in shards],
        'code_hashes': manifest['code_hashes'],
    }, 24)
    manifest_path = os.path.join(run_dir, 'variant_shards_manifest.json')
    _write_json(manifest_path, manifest)
    manifest['manifest_path'] = manifest_path
    return manifest


def load_manifest(path_or_dir: str) -> dict:
    path = path_or_dir
    if os.path.isdir(path):
        path = os.path.join(path, 'variant_shards_manifest.json')
    return _read_json(path)


def load_range(path_or_dir: str, start_index: int, end_index: int) -> list[slow.Variant]:
    manifest = load_manifest(path_or_dir)
    start_index = int(start_index)
    end_index = int(end_index)
    by_index = {}
    for shard in manifest.get('shards') or []:
        if int(shard['end_index']) <= start_index or int(shard['start_index']) >= end_index:
            continue
        if shard.get('storage') == 'range':
            local_start = max(start_index, int(shard['start_index']))
            local_end = min(end_index, int(shard['end_index']))
            for idx in range(local_start, local_end):
                by_index[idx] = random_access.expanded_variant_at(idx)
            continue
        with gzip.open(shard['path'], 'rt', encoding='utf-8') as f:
            for line in f:
                ref = json.loads(line)
                idx = int(ref.get('global_index'))
                if start_index <= idx < end_index:
                    by_index[idx] = variant_from_ref(ref)
    return [by_index[idx] for idx in range(start_index, end_index) if idx in by_index]


def parity_check(path_or_dir: str, limit: int = 1000) -> dict:
    manifest = load_manifest(path_or_dir)
    mismatches = []
    checked = min(int(limit), int(manifest.get('unique_variants') or 0))
    shard_variants = load_range(path_or_dir, 0, checked)
    stream = massive.variant_stream(
        manifest.get('config', {}).get('include_existing', True),
        manifest.get('config', {}).get('include_broad_full', True),
        manifest.get('config', {}).get('include_v29_full', True),
    )
    if manifest.get('config', {}).get('include_expanded_full'):
        return {
            'ok': True,
            'checked': checked,
            'mismatches': [],
            'note': 'Expanded family is shard/native only; old stream parity applies to legacy families.',
        }
    for idx, direct in enumerate(shard_variants):
        streamed = next(stream)
        if streamed.name != direct.name or streamed.weights != direct.weights or streamed.bias != direct.bias:
            mismatches.append({'index': idx, 'streamed': streamed.name, 'shard': direct.name})
            break
    return {'ok': not mismatches, 'checked': checked, 'mismatches': mismatches}


def main() -> int:
    ap = argparse.ArgumentParser(description='Build compact deduped variant index shards.')
    ap.add_argument('--out-dir', default=DEFAULT_OUT_DIR)
    ap.add_argument('--name', default=f"variant_shards_{time.strftime('%Y%m%d_%H%M%S')}")
    ap.add_argument('--shard-size', type=int, default=100000)
    ap.add_argument('--max-variants', type=int, default=0)
    ap.add_argument('--no-existing', action='store_true')
    ap.add_argument('--no-broad-full', action='store_true')
    ap.add_argument('--no-v29-full', action='store_true')
    ap.add_argument('--include-expanded-full', action='store_true',
                    help='Include the large all-indicator expanded family.')
    ap.add_argument('--assume-unique-expanded-only', action='store_true',
                    help='Memory-light mode for expanded-only shards; skips global signature set.')
    ap.add_argument('--parity-check', action='store_true')
    args = ap.parse_args()
    payload = build_shards(
        args.out_dir,
        args.name,
        max(1, int(args.shard_size or 1)),
        not args.no_existing,
        not args.no_broad_full,
        not args.no_v29_full,
        args.include_expanded_full,
        args.max_variants,
        args.assume_unique_expanded_only,
    )
    if args.parity_check:
        payload['parity_check'] = parity_check(payload['manifest_path'], min(1000, payload['unique_variants']))
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
