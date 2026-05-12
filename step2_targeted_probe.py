"""Targeted Step 2 probe for the current live variant.

This is a focused companion to ``step2_adaptive_hunter.py``.  It starts from
saved high-performing variants, checks the oracle headroom, then runs dense
bias and coordinate sweeps around the current leader.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import time
from pathlib import Path
from typing import Any

import numpy as np

import active_engine_baseline
import candidate_profile_schema
import decision_tape_compiled
import scoring_variant_lab as lab
import scoring_variant_lab_fast as fast
import step2_parity_contract
import tournament_safety


HERE = Path(__file__).resolve().parent
DEFAULT_OUT = HERE / 'postmortem' / 'backtests' / 'step2_adaptive_hunter'
DEFAULT_COMPILED = HERE / 'postmortem' / 'backtests' / 'compiled_decision_tapes' / (
    'compiled_step2_current_live_CLSK-MARA-RIOT_2026-04-06_2026-05-08'
) / 'manifest.json'


def _variant(name: str, weights: dict[str, float], bias: float = 0.0) -> lab.Variant:
    clean = {
        k: round(float(v), 6)
        for k, v in (weights or {}).items()
        if k in fast.FEATURE_NAMES and abs(float(v or 0.0)) > 1e-9
    }
    return lab.Variant(str(name), clean, round(float(bias or 0.0), 6))


def _active_seed() -> lab.Variant:
    active = active_engine_baseline.active_variant()
    return _variant(active.name, dict(active.weights), active.bias)


def _load_trading_config() -> dict[str, Any]:
    with (HERE / 'trading_config.json').open('r', encoding='utf-8') as f:
        return json.load(f)


def _row_to_variant(row: dict[str, Any], fallback: str) -> lab.Variant | None:
    weights = row.get('weights') or {}
    if not isinstance(weights, dict) or not weights:
        return None
    return _variant(str(row.get('variant') or row.get('name') or fallback), weights, float(row.get('bias') or 0.0))


def _walk_rows(obj: Any):
    if isinstance(obj, dict):
        if isinstance(obj.get('weights'), dict):
            yield obj
        for value in obj.values():
            yield from _walk_rows(value)
    elif isinstance(obj, list):
        for value in obj:
            yield from _walk_rows(value)


def _load_seed_variants(paths: list[str], limit: int) -> list[lab.Variant]:
    variants: list[lab.Variant] = []
    seen = set()
    for raw in paths:
        path = Path(raw)
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text(encoding='utf-8'))
        except Exception:
            continue
        for idx, row in enumerate(_walk_rows(data)):
            variant = _row_to_variant(row, f'{path.stem}_{idx}')
            if not variant:
                continue
            key = tournament_safety.model_id(variant.name, variant.weights, variant.bias)
            if key in seen:
                continue
            seen.add(key)
            variants.append(variant)
            if len(variants) >= limit:
                return variants
    return variants


def _candidate_json_paths(root: Path) -> list[str]:
    paths = []
    if root.exists():
        for name in ('summary.json', 'checkpoint.json'):
            paths.extend(str(p) for p in root.rglob(name))
    paths.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return paths


def _score(compiled: dict, variants: list[lab.Variant], start_balance: float, batch_size: int,
           sim_config: dict[str, float | int]) -> list[dict]:
    out: list[dict] = []
    for start in range(0, len(variants), batch_size):
        batch = variants[start:start + batch_size]
        rows = decision_tape_compiled.simulate_variants(
            compiled,
            batch,
            start_balance,
            gate=None,
            sim_config=sim_config,
        )
        if rows is None:
            raise RuntimeError('compiled Step 2 simulation unavailable')
        for row in rows:
            full = row.get('decision_full') or {}
            out.append({
                'variant': row.get('variant'),
                'weights': row.get('weights') or {},
                'bias': float(row.get('bias') or 0.0),
                'step2_pnl': round(float(full.get('pnl') or 0.0), 2),
                'step2_trades': int(full.get('trades') or 0),
                'step2_win_rate_pct': full.get('win_rate_pct'),
                'by_day': full.get('by_day') or {},
                'by_ticker': full.get('by_ticker') or {},
                'skipped': full.get('skipped') or {},
            })
    out.sort(key=lambda r: float(r['step2_pnl']), reverse=True)
    return out


def _score_sides(compiled: dict, variants: list[lab.Variant], sides: np.ndarray, start_balance: float,
                 sim_config: dict[str, float | int]) -> list[dict]:
    rows = decision_tape_compiled.simulate_side_matrix(
        compiled,
        variants,
        sides,
        start_balance,
        gate=None,
        sim_config=sim_config,
    )
    if rows is None:
        raise RuntimeError('compiled Step 2 side simulation unavailable')
    out = []
    for row in rows:
        full = row.get('decision_full') or {}
        out.append({
            'variant': row.get('variant'),
            'weights': row.get('weights') or {},
            'bias': float(row.get('bias') or 0.0),
            'step2_pnl': round(float(full.get('pnl') or 0.0), 2),
            'step2_trades': int(full.get('trades') or 0),
            'step2_win_rate_pct': full.get('win_rate_pct'),
            'by_day': full.get('by_day') or {},
            'by_ticker': full.get('by_ticker') or {},
            'skipped': full.get('skipped') or {},
        })
    out.sort(key=lambda r: float(r['step2_pnl']), reverse=True)
    return out


def _best(rows: list[dict]) -> dict:
    return max(rows, key=lambda r: float(r['step2_pnl']))


def _decorate(row: dict, active_pnl: float, target_pnl: float) -> dict:
    out = dict(row)
    pnl = float(out['step2_pnl'])
    out['step2_delta_vs_active'] = round(pnl - active_pnl, 4)
    out['step2_delta_pct_vs_active'] = round((pnl / active_pnl - 1.0) * 100.0, 4) if active_pnl else None
    out['beats_target'] = pnl >= target_pnl
    return out


def _emit_progress(stage: str, best_row: dict, active_pnl: float, target_pnl: float) -> None:
    row = _decorate(best_row, active_pnl, target_pnl)
    print(json.dumps({
        'stage': stage,
        'best_variant': row['variant'],
        'best_pnl': row['step2_pnl'],
        'delta_pct': row['step2_delta_pct_vs_active'],
        'target_pnl': round(target_pnl, 2),
        'beats_target': row['beats_target'],
    }), flush=True)


def _bias_values(raw_scores: np.ndarray, current_bias: float, quantiles: int) -> list[float]:
    finite = raw_scores[np.isfinite(raw_scores)]
    if finite.size == 0:
        return [current_bias]
    thresholds = -finite
    qs = np.linspace(0.002, 0.998, max(3, quantiles))
    values = set(float(np.quantile(thresholds, q)) for q in qs)
    std = float(np.std(finite)) or 1.0
    for span in (0.05, 0.1, 0.2, 0.35, 0.5, 0.75, 1.0, 1.5, 2.0):
        step = std * span
        values.add(float(current_bias + step))
        values.add(float(current_bias - step))
    for delta in (-6, -4, -3, -2, -1, -0.5, -0.25, 0, 0.25, 0.5, 1, 2, 3, 4, 6):
        values.add(float(current_bias + delta))
    return sorted(round(v, 6) for v in values if math.isfinite(v))


def _bias_sweep(compiled: dict, seeds: list[lab.Variant], start_balance: float, batch_size: int,
                sim_config: dict[str, float | int], max_seeds: int, quantiles: int) -> list[dict]:
    x = np.asarray(compiled['features'])
    variants: list[lab.Variant] = []
    seen = set()
    for seed in seeds[:max_seeds]:
        w = np.zeros(len(fast.FEATURE_NAMES), dtype=np.float64)
        for idx, name in enumerate(fast.FEATURE_NAMES):
            w[idx] = float(seed.weights.get(name, 0.0) or 0.0)
        raw_scores = x @ w
        for bias in _bias_values(raw_scores, float(seed.bias or 0.0), quantiles):
            variant = _variant(f'bias_{seed.name[:48]}_{bias:+.6f}', dict(seed.weights), bias)
            key = tournament_safety.model_id(variant.name, variant.weights, variant.bias)
            if key in seen:
                continue
            seen.add(key)
            variants.append(variant)
    return _score(compiled, variants, start_balance, batch_size, sim_config)


def _coordinate_sweep(compiled: dict, seed: lab.Variant, start_balance: float, batch_size: int,
                      sim_config: dict[str, float | int], iterations: int, weight_limit: float) -> list[dict]:
    current = seed
    all_rows: list[dict] = []
    deltas = [-4.0, -3.0, -2.25, -1.5, -1.0, -0.6, -0.35, -0.2, -0.1,
              0.1, 0.2, 0.35, 0.6, 1.0, 1.5, 2.25, 3.0, 4.0]
    bias_deltas = [-6.0, -4.0, -2.5, -1.5, -0.8, -0.4, -0.2, -0.1,
                   0.1, 0.2, 0.4, 0.8, 1.5, 2.5, 4.0, 6.0]
    for iteration in range(1, iterations + 1):
        variants: list[lab.Variant] = []
        for feature in fast.FEATURE_NAMES:
            base = float(current.weights.get(feature, 0.0) or 0.0)
            for delta in deltas:
                weights = dict(current.weights)
                value = max(-weight_limit, min(weight_limit, base + delta))
                if abs(value) < 1e-9:
                    weights.pop(feature, None)
                else:
                    weights[feature] = value
                variants.append(_variant(f'coordp_i{iteration:02d}_{feature}_{value:+.3f}', weights, current.bias))
        for delta in bias_deltas:
            variants.append(_variant(f'coordp_i{iteration:02d}_bias_{float(current.bias or 0.0) + delta:+.3f}',
                                     dict(current.weights), float(current.bias or 0.0) + delta))
        scored = _score(compiled, variants, start_balance, batch_size, sim_config)
        all_rows.extend(scored)
        best = scored[0]
        if float(best['step2_pnl']) <= float(
            _score(compiled, [current], start_balance, batch_size, sim_config)[0]['step2_pnl']
        ) + 0.004:
            continue
        current = _variant(str(best['variant']), dict(best['weights']), float(best.get('bias') or 0.0))
        print(json.dumps({
            'stage': 'coordinate_iteration',
            'iteration': iteration,
            'best_variant': best['variant'],
            'best_pnl': best['step2_pnl'],
        }), flush=True)
    all_rows.sort(key=lambda r: float(r['step2_pnl']), reverse=True)
    return all_rows


def main() -> int:
    ap = argparse.ArgumentParser(description='Targeted Step 2 probe around saved leaders.')
    ap.add_argument('--compiled-decision-tape', default=str(DEFAULT_COMPILED))
    ap.add_argument('--out-dir', default=str(DEFAULT_OUT))
    ap.add_argument('--name', default='targeted_probe')
    ap.add_argument('--start-balance', type=float, default=100000.0)
    ap.add_argument('--beat-pct', type=float, default=25.0)
    ap.add_argument('--seed-json', action='append', default=[])
    ap.add_argument('--seed-limit', type=int, default=1500)
    ap.add_argument('--batch-size', type=int, default=500)
    ap.add_argument('--bias-seeds', type=int, default=60)
    ap.add_argument('--bias-quantiles', type=int, default=251)
    ap.add_argument('--coord-iterations', type=int, default=8)
    ap.add_argument('--weight-limit', type=float, default=6.0)
    args = ap.parse_args()

    started = time.perf_counter()
    out_dir = Path(args.out_dir) / args.name
    out_dir.mkdir(parents=True, exist_ok=True)

    compiled = decision_tape_compiled.load_compiled(args.compiled_decision_tape, mmap=True)
    trading_config = _load_trading_config()
    sim_config = step2_parity_contract.sim_config(trading_config)
    active = _active_seed()
    active_row = _score(compiled, [active], float(args.start_balance), int(args.batch_size), sim_config)[0]
    active_pnl = float(active_row['step2_pnl'])
    target_pnl = active_pnl * (1.0 + float(args.beat_pct) / 100.0)

    seed_paths = list(args.seed_json) or _candidate_json_paths(DEFAULT_OUT)
    seeds = [active]
    seeds.extend(_load_seed_variants(seed_paths, int(args.seed_limit)))
    seed_scores = _score(compiled, seeds, float(args.start_balance), int(args.batch_size), sim_config)
    seed_scores = [_decorate(r, active_pnl, target_pnl) for r in seed_scores]
    seed_scores.sort(key=lambda r: float(r['step2_pnl']), reverse=True)
    seed_variants = [_variant(str(r['variant']), dict(r['weights']), float(r.get('bias') or 0.0)) for r in seed_scores]

    oracle_sides = np.where(
        np.asarray(compiled['long_pnl_pct']) >= np.asarray(compiled['short_pnl_pct']),
        decision_tape_compiled.SIDE_LONG,
        decision_tape_compiled.SIDE_SHORT,
    ).reshape(1, -1)
    oracle = _score_sides(
        compiled,
        [_variant('oracle_best_available_side_per_opportunity', {}, 0.0)],
        oracle_sides.astype(np.int8),
        float(args.start_balance),
        sim_config,
    )[0]

    best_rows: list[dict] = []
    best_rows.extend(seed_scores[:100])
    _emit_progress('seed_rescore', seed_scores[0], active_pnl, target_pnl)
    print(json.dumps({
        'stage': 'oracle',
        'oracle_pnl': oracle['step2_pnl'],
        'oracle_trades': oracle['step2_trades'],
        'oracle_delta_pct': round((float(oracle['step2_pnl']) / active_pnl - 1.0) * 100.0, 4),
    }), flush=True)

    bias_rows = _bias_sweep(
        compiled,
        seed_variants,
        float(args.start_balance),
        int(args.batch_size),
        sim_config,
        int(args.bias_seeds),
        int(args.bias_quantiles),
    )
    bias_rows = [_decorate(r, active_pnl, target_pnl) for r in bias_rows]
    best_rows.extend(bias_rows[:250])
    _emit_progress('bias_sweep', bias_rows[0], active_pnl, target_pnl)

    coord_seed_row = _best(best_rows)
    coord_seed = _variant(str(coord_seed_row['variant']), dict(coord_seed_row['weights']), float(coord_seed_row.get('bias') or 0.0))
    coord_rows = _coordinate_sweep(
        compiled,
        coord_seed,
        float(args.start_balance),
        int(args.batch_size),
        sim_config,
        int(args.coord_iterations),
        float(args.weight_limit),
    )
    coord_rows = [_decorate(r, active_pnl, target_pnl) for r in coord_rows]
    best_rows.extend(coord_rows[:500])
    best_rows.sort(key=lambda r: float(r['step2_pnl']), reverse=True)

    parity = step2_parity_contract.contract(trading_config)
    summary = {
        'script': 'step2_targeted_probe.py',
        'completed': bool(best_rows and float(best_rows[0]['step2_pnl']) >= target_pnl),
        'active': _decorate(active_row, active_pnl, target_pnl),
        'active_step2_pnl': active_pnl,
        'target_pnl': target_pnl,
        'beat_pct': float(args.beat_pct),
        'oracle': oracle,
        'seed_count': len(seeds),
        'elapsed_sec': round(time.perf_counter() - started, 3),
        'step2_parity_contract': parity,
        'step2_parity_contract_hash': step2_parity_contract.contract_hash(parity),
        'leaderboard': best_rows[:100],
        'winners': [r for r in best_rows if float(r['step2_pnl']) >= target_pnl][:25],
    }
    candidate_profile_schema.decorate_payload(summary, context={
        'script': 'step2_targeted_probe.py',
        'start_balance': args.start_balance,
        'compiled_decision_tape': args.compiled_decision_tape,
        'active_step2_pnl': active_pnl,
        'target_pnl': target_pnl,
        'scored_total': len(best_rows),
    })
    (out_dir / 'summary.json').write_text(json.dumps(summary, indent=2, sort_keys=True), encoding='utf-8')
    print(json.dumps({
        'out': str(out_dir / 'summary.json'),
        'completed': summary['completed'],
        'best_variant': best_rows[0]['variant'],
        'best_pnl': best_rows[0]['step2_pnl'],
        'best_delta_pct': best_rows[0]['step2_delta_pct_vs_active'],
        'target_pnl': round(target_pnl, 2),
        'elapsed_sec': summary['elapsed_sec'],
    }), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
