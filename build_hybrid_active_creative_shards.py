"""Build a hybrid Step 1 shard around active-local and Step 2 survivors."""
from __future__ import annotations

from output_paths import output_path

import argparse
import gzip
import json
import os
import time

import scoring_variant_lab as slow
import scoring_variant_lab_massive as massive
import scoring_variant_random_access as ra
import tournament_safety


HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUT_DIR = output_path('postmortem', 'backtests', 'variant_index_shards')
CORE_NAMES = [
    'relative',
    'vwap',
    'btc_chop',
    'flow_contra',
    'miner',
    'setup_momentum_breakout',
    'ema',
    'momentum',
    'btc',
    'exec_penalty',
    'burst',
]
LOCAL_DELTAS = [-1.25, -0.6, 0.0, 0.6, 1.25]
LOCAL_SCALES = [0.8, 1.0, 1.25, 1.55]
LOCAL_BIASES = [-1.0, -0.5, 0.0, 0.5, 1.0]
LOCAL_PACKS = [
    ('plain', {}),
    ('midday_harder', {'midday_phase': 3.0}),
    ('midday_softer', {'midday_phase': 1.5}),
    ('btc_rel_skeptic', {'setup_btc_relative_strength': -2.75}),
    ('breakout_push', {'setup_momentum_breakout': 2.5}),
    ('pullback_push', {'setup_trend_pullback': 2.5}),
    ('vwap_fade', {'vwap_sigma_ext': -2.25}),
    ('flow_fade', {'flow_pressure_abs': -2.0}),
    ('btc_mom_fade', {'btc_mom_abs': -2.0}),
    ('riot_short_wall', {'riot_short_penalty': 4.5}),
    ('riot_reversal', {'riot_short_penalty': 3.5, 'riot_long_penalty': -2.5}),
]


def _safe_name(value: str) -> str:
    return ''.join(ch if ch.isalnum() or ch in '._-' else '_' for ch in str(value)).strip('._') or 'hybrid_shards'


def _signature(weights: dict, bias: float) -> tuple:
    return massive._signature(weights, bias)


def _read_json(path: str) -> dict:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def _write_json(path: str, payload: dict) -> str:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    return path


def _survivors(path: str) -> list[dict]:
    payload = _read_json(path)
    return [row for row in payload.get('results') or [] if row.get('beats_active_decision')]


def _active_ref(raw_index: int, global_index: int) -> dict:
    variant = ra.active_local_variant_at(raw_index)
    return {
        'global_index': global_index,
        'family': 'active_local',
        'raw_index': raw_index,
        'variant': variant.name,
        'signature': tournament_safety.stable_json_hash({'weights': variant.weights, 'bias': variant.bias}, 24),
    }


def _local_variant(anchor: dict, raw_index: int) -> slow.Variant:
    dims = [
        len(LOCAL_SCALES),
        len(LOCAL_BIASES),
        *([len(LOCAL_DELTAS)] * len(CORE_NAMES)),
        len(LOCAL_PACKS),
    ]
    mixed = (raw_index * 1_299_721 + 44_443) % _local_count()
    parts = ra._unravel(mixed, dims)
    scale_i, bias_i = parts[:2]
    core_i = parts[2:2 + len(CORE_NAMES)]
    pack_i = parts[-1]
    weights = {
        name: round(float(value or 0.0) * LOCAL_SCALES[scale_i], 6)
        for name, value in (anchor.get('weights') or {}).items()
    }
    for name, delta_i in zip(CORE_NAMES, core_i):
        weights[name] = round(float(weights.get(name, 0.0) or 0.0) + LOCAL_DELTAS[delta_i], 6)
    pack_name, pack = LOCAL_PACKS[pack_i]
    weights.update(pack)
    for feature in ra.fast.FEATURE_NAMES:
        weights.setdefault(feature, 0.0)
    bias = round(float(anchor.get('bias') or 0.0) + LOCAL_BIASES[bias_i], 6)
    tag = ''.join(str(i) for i in core_i)
    return slow.Variant(
        f"hybrid_local_{raw_index + 1:012d}_{anchor.get('variant', 'anchor')[:32]}_{pack_name}_s{scale_i}_b{bias_i}_c{tag}",
        weights,
        bias,
    )


def _local_count() -> int:
    return len(LOCAL_SCALES) * len(LOCAL_BIASES) * (len(LOCAL_DELTAS) ** len(CORE_NAMES)) * len(LOCAL_PACKS)


def _flush(shards: list[dict], refs: list[dict], shard_dir: str, shard_id: int) -> int:
    if not refs:
        return shard_id
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
    refs.clear()
    return shard_id + 1


def build(args) -> dict:
    started = time.perf_counter()
    run_dir = os.path.abspath(os.path.join(args.out_dir, _safe_name(args.name)))
    shard_dir = os.path.join(run_dir, 'shards')
    os.makedirs(shard_dir, exist_ok=True)
    anchors = _survivors(args.step2)
    if not anchors:
        raise SystemExit('No Step 2 survivors found to anchor hybrid local search.')
    target = max(1, int(args.total_variants or 1))
    random_count = min(target, int(args.random_active or target // 2))
    local_count = target - random_count
    seen = set()
    refs: list[dict] = []
    shards: list[dict] = []
    global_index = 0
    shard_id = 0

    def add_ref(ref: dict, variant: slow.Variant) -> None:
        nonlocal global_index, shard_id
        sig = _signature(variant.weights, variant.bias)
        if sig in seen:
            return
        seen.add(sig)
        ref['global_index'] = global_index
        refs.append(ref)
        global_index += 1
        if len(refs) >= max(1, int(args.shard_size or 1)):
            shard_id = _flush(shards, refs, shard_dir, shard_id)

    active_total = ra.active_local_count()
    random_offset = max(0, int(args.random_offset or 0))
    local_offset = max(0, int(args.local_offset or 0))
    for i in range(random_count):
        raw_index = ((i + random_offset) * 1_000_003 + 711_911) % active_total
        add_ref(_active_ref(raw_index, global_index), ra.active_local_variant_at(raw_index))

    per_anchor = max(1, local_count // len(anchors))
    generated = 0
    for anchor_idx, anchor in enumerate(anchors):
        limit = per_anchor + (1 if anchor_idx < (local_count % len(anchors)) else 0)
        for j in range(limit):
            raw_index = local_offset + anchor_idx * per_anchor + j
            variant = _local_variant(anchor, raw_index)
            add_ref({
                'family': 'hybrid_local',
                'raw_index': raw_index,
                'anchor_variant': anchor.get('variant'),
                'variant': variant.name,
                'weights': variant.weights,
                'bias': variant.bias,
            }, variant)
            generated += 1
    shard_id = _flush(shards, refs, shard_dir, shard_id)
    config = {
        'step2': os.path.abspath(args.step2),
        'total_variants_requested': target,
        'random_active_requested': random_count,
        'random_offset': random_offset,
        'local_offset': local_offset,
        'local_requested': local_count,
        'anchors': [row.get('variant') for row in anchors],
        'local_deltas': LOCAL_DELTAS,
        'local_scales': LOCAL_SCALES,
        'local_biases': LOCAL_BIASES,
        'local_packs': [name for name, _pack in LOCAL_PACKS],
        'shard_size': args.shard_size,
    }
    manifest = {
        'schema_version': 1,
        'created_at_epoch': int(time.time()),
        'name': args.name,
        'run_dir': run_dir,
        'config': config,
        'config_hash': tournament_safety.stable_json_hash(config, 24),
        'unique_variants': global_index,
        'raw_variants_seen': random_count + generated,
        'duplicate_signatures_skipped': random_count + generated - global_index,
        'shard_count': len(shards),
        'shards': shards,
        'code_hashes': {
            'build_hybrid_active_creative_shards.py': tournament_safety._file_sha256(__file__),
            'scoring_variant_shards.py': tournament_safety._file_sha256(os.path.join(HERE, 'scoring_variant_shards.py')),
            'scoring_variant_random_access.py': tournament_safety._file_sha256(os.path.join(HERE, 'scoring_variant_random_access.py')),
        },
        'elapsed_seconds': round(time.perf_counter() - started, 3),
    }
    manifest['manifest_hash'] = tournament_safety.stable_json_hash({
        'config_hash': manifest['config_hash'],
        'unique_variants': manifest['unique_variants'],
        'shards': [{k: row[k] for k in ('shard_id', 'start_index', 'end_index', 'sha256')} for row in shards],
        'code_hashes': manifest['code_hashes'],
    }, 24)
    path = _write_json(os.path.join(run_dir, 'variant_shards_manifest.json'), manifest)
    manifest['manifest_path'] = path
    return manifest


def main() -> int:
    ap = argparse.ArgumentParser(description='Build hybrid active-creative Step 1 shards.')
    ap.add_argument('--step2', required=True)
    ap.add_argument('--out-dir', default=DEFAULT_OUT_DIR)
    ap.add_argument('--name', default='hybrid_active_creative')
    ap.add_argument('--total-variants', type=int, default=500000)
    ap.add_argument('--random-active', type=int, default=250000)
    ap.add_argument('--random-offset', type=int, default=0)
    ap.add_argument('--local-offset', type=int, default=0)
    ap.add_argument('--shard-size', type=int, default=50000)
    args = ap.parse_args()
    print(json.dumps(build(args), indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
