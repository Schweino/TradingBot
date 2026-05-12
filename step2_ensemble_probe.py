"""Composite Step 2 scorer probe.

The active live profile is a single linear scoring vector.  This probe keeps the
same compiled opportunities and Step 2 execution state machine, but combines
several high-performing scoring vectors before choosing LONG/SHORT.  It is a
research pass to see whether the remaining edge needs a composite scorer.
"""
from __future__ import annotations

import argparse
import itertools
import json
import random
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
DEFAULT_COMPILED = HERE / 'postmortem' / 'backtests' / 'compiled_decision_tapes' / (
    'compiled_step2_current_live_CLSK-MARA-RIOT_2026-04-06_2026-05-08'
) / 'manifest.json'
DEFAULT_OUT = HERE / 'postmortem' / 'backtests' / 'step2_adaptive_hunter'


def _load_config() -> dict[str, Any]:
    with (HERE / 'trading_config.json').open('r', encoding='utf-8') as f:
        return json.load(f)


def _variant(name: str, weights: dict[str, float], bias: float = 0.0) -> lab.Variant:
    clean = {
        k: round(float(v), 6)
        for k, v in (weights or {}).items()
        if k in fast.FEATURE_NAMES and abs(float(v or 0.0)) > 1e-10
    }
    return lab.Variant(str(name), clean, round(float(bias or 0.0), 6))


def _walk_rows(obj: Any):
    if isinstance(obj, dict):
        if isinstance(obj.get('weights'), dict):
            yield obj
        for value in obj.values():
            yield from _walk_rows(value)
    elif isinstance(obj, list):
        for value in obj:
            yield from _walk_rows(value)


def _load_seeds(paths: list[str], limit: int) -> list[lab.Variant]:
    seeds = [active_engine_baseline.active_variant()]
    seen = {tournament_safety.model_id(seeds[0].name, seeds[0].weights, seeds[0].bias)}
    for raw in paths:
        path = Path(raw)
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text(encoding='utf-8'))
        except Exception:
            continue
        for idx, row in enumerate(_walk_rows(data)):
            weights = row.get('weights') or {}
            if not isinstance(weights, dict) or not weights:
                continue
            variant = _variant(str(row.get('variant') or f'{path.stem}_{idx}'), weights, float(row.get('bias') or 0.0))
            key = tournament_safety.model_id(variant.name, variant.weights, variant.bias)
            if key in seen:
                continue
            seen.add(key)
            seeds.append(variant)
            if len(seeds) >= limit:
                return seeds
    return seeds


def _score_variants(compiled: dict, variants: list[lab.Variant], start_balance: float, batch_size: int,
                    sim_config: dict[str, float | int]) -> list[dict]:
    out: list[dict] = []
    for start in range(0, len(variants), batch_size):
        batch = variants[start:start + batch_size]
        rows = decision_tape_compiled.simulate_variants(compiled, batch, start_balance, gate=None, sim_config=sim_config)
        if rows is None:
            raise RuntimeError('compiled Step 2 simulation unavailable')
        out.extend(_rows_to_summary(rows))
    out.sort(key=lambda r: float(r['step2_pnl']), reverse=True)
    return out


def _score_sides(compiled: dict, variants: list[lab.Variant], sides: np.ndarray, start_balance: float,
                 sim_config: dict[str, float | int]) -> list[dict]:
    rows = decision_tape_compiled.simulate_side_matrix(compiled, variants, sides, start_balance, gate=None, sim_config=sim_config)
    if rows is None:
        raise RuntimeError('compiled Step 2 side simulation unavailable')
    return _rows_to_summary(rows)


def _rows_to_summary(rows: list[dict]) -> list[dict]:
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
    return out


def _decorate(row: dict, active_pnl: float, target_pnl: float) -> dict:
    out = dict(row)
    pnl = float(out['step2_pnl'])
    out['step2_delta_vs_active'] = round(pnl - active_pnl, 4)
    out['step2_delta_pct_vs_active'] = round((pnl / active_pnl - 1.0) * 100.0, 4) if active_pnl else None
    out['beats_target'] = pnl >= target_pnl
    return out


def _variant_matrix(variants: list[lab.Variant]) -> tuple[np.ndarray, np.ndarray]:
    weights = np.zeros((len(variants), len(fast.FEATURE_NAMES)), dtype=np.float64)
    bias = np.zeros(len(variants), dtype=np.float64)
    for i, variant in enumerate(variants):
        for j, feature in enumerate(fast.FEATURE_NAMES):
            weights[i, j] = float(variant.weights.get(feature, 0.0) or 0.0)
        bias[i] = float(variant.bias or 0.0)
    return weights, bias


def _ensemble_candidates(rng: random.Random, seed_count: int, max_candidates: int) -> list[dict]:
    candidates = []
    seen = set()
    top = list(range(seed_count))
    for size in (2, 3, 4, 5, 7):
        for combo in itertools.combinations(top[:min(seed_count, 12)], size):
            for mode in ('vote', 'mean_tanh', 'weighted_tanh', 'raw_ranked'):
                key = (mode, combo)
                if key not in seen:
                    seen.add(key)
                    candidates.append({'mode': mode, 'combo': combo})
                    if len(candidates) >= max_candidates:
                        return candidates
    while len(candidates) < max_candidates:
        size = rng.choice([3, 4, 5, 6, 8, 10])
        pool = min(seed_count, rng.choice([12, 20, 35, seed_count]))
        combo = tuple(sorted(rng.sample(range(pool), min(size, pool))))
        mode = rng.choice(['vote', 'mean_tanh', 'weighted_tanh', 'raw_ranked'])
        temp = rng.choice([0.25, 0.5, 0.8, 1.2, 2.0, 4.0])
        key = (mode, combo, temp)
        if key in seen:
            continue
        seen.add(key)
        candidates.append({'mode': mode, 'combo': combo, 'temp': temp})
    return candidates


def _candidate_sides(scores: np.ndarray, original: np.ndarray, candidates: list[dict], batch: list[dict]) -> np.ndarray:
    sides = np.zeros((len(batch), scores.shape[0]), dtype=np.int8)
    for idx, candidate in enumerate(batch):
        combo = list(candidate['combo'])
        subset = scores[:, combo]
        mode = candidate.get('mode')
        temp = float(candidate.get('temp') or 1.0)
        if mode == 'vote':
            combined = np.sign(subset).sum(axis=1)
        elif mode == 'raw_ranked':
            weights = np.linspace(1.0, 0.35, len(combo), dtype=np.float64)
            combined = subset @ weights
        elif mode == 'weighted_tanh':
            weights = np.linspace(1.0, 0.35, len(combo), dtype=np.float64)
            combined = np.tanh(subset / temp) @ weights
        else:
            combined = np.tanh(subset / temp).mean(axis=1)
        sides[idx] = np.where(combined > 0.0, 1, np.where(combined < 0.0, -1, original))
    return sides


def main() -> int:
    ap = argparse.ArgumentParser(description='Composite Step 2 scorer probe.')
    ap.add_argument('--compiled-decision-tape', default=str(DEFAULT_COMPILED))
    ap.add_argument('--out-dir', default=str(DEFAULT_OUT))
    ap.add_argument('--name', default='ensemble_probe')
    ap.add_argument('--seed-json', action='append', default=[])
    ap.add_argument('--seed-limit', type=int, default=1000)
    ap.add_argument('--top-seeds', type=int, default=80)
    ap.add_argument('--max-candidates', type=int, default=10000)
    ap.add_argument('--batch-size', type=int, default=250)
    ap.add_argument('--start-balance', type=float, default=100000.0)
    ap.add_argument('--beat-pct', type=float, default=25.0)
    ap.add_argument('--seed', type=int, default=20260508)
    args = ap.parse_args()

    started = time.perf_counter()
    rng = random.Random(int(args.seed))
    cfg = _load_config()
    sim_config = step2_parity_contract.sim_config(cfg)
    compiled = decision_tape_compiled.load_compiled(args.compiled_decision_tape, mmap=True)
    out_dir = Path(args.out_dir) / args.name
    out_dir.mkdir(parents=True, exist_ok=True)

    active = active_engine_baseline.active_variant()
    active_row = _score_variants(compiled, [active], float(args.start_balance), int(args.batch_size), sim_config)[0]
    active_pnl = float(active_row['step2_pnl'])
    target_pnl = active_pnl * (1.0 + float(args.beat_pct) / 100.0)

    seeds = _load_seeds(args.seed_json, int(args.seed_limit))
    seed_rows = _score_variants(compiled, seeds, float(args.start_balance), int(args.batch_size), sim_config)
    seed_rows = [_decorate(r, active_pnl, target_pnl) for r in seed_rows]
    seed_rows.sort(key=lambda r: float(r['step2_pnl']), reverse=True)
    top_seed_rows = seed_rows[:int(args.top_seeds)]
    top_variants = [_variant(str(r['variant']), dict(r['weights']), float(r.get('bias') or 0.0)) for r in top_seed_rows]
    weights, bias = _variant_matrix(top_variants)
    scores = np.asarray(compiled['features']) @ weights.T
    scores += bias.reshape(1, -1)

    print(json.dumps({
        'stage': 'seed_rescore',
        'seed_count': len(seeds),
        'top_seeds': len(top_variants),
        'best_variant': seed_rows[0]['variant'],
        'best_pnl': seed_rows[0]['step2_pnl'],
        'delta_pct': seed_rows[0]['step2_delta_pct_vs_active'],
        'target_pnl': round(target_pnl, 2),
    }), flush=True)

    candidates = _ensemble_candidates(rng, len(top_variants), int(args.max_candidates))
    original = np.asarray(compiled['original_side'], dtype=np.int8)
    rows: list[dict] = []
    for start in range(0, len(candidates), int(args.batch_size)):
        batch = candidates[start:start + int(args.batch_size)]
        sides = _candidate_sides(scores, original, candidates, batch)
        variants = [
            _variant(f"ensemble_{start + i:06d}_{batch[i]['mode']}_{'_'.join(map(str, batch[i]['combo']))}", {}, 0.0)
            for i in range(len(batch))
        ]
        scored = _score_sides(compiled, variants, sides, float(args.start_balance), sim_config)
        for row, meta in zip(scored, batch):
            row['ensemble'] = {
                'mode': meta['mode'],
                'combo': list(meta['combo']),
                'temp': meta.get('temp'),
                'components': [
                    {
                        'rank': int(i + 1),
                        'variant': top_seed_rows[i]['variant'],
                        'step2_pnl': top_seed_rows[i]['step2_pnl'],
                    }
                    for i in meta['combo']
                ],
            }
            rows.append(_decorate(row, active_pnl, target_pnl))
        rows.sort(key=lambda r: float(r['step2_pnl']), reverse=True)
        if rows and rows[0].get('beats_target'):
            break
        if start == 0 or start % (int(args.batch_size) * 10) == 0:
            print(json.dumps({
                'event': 'ensemble_batch',
                'scored_total': min(start + int(args.batch_size), len(candidates)),
                'global_best_pnl': rows[0]['step2_pnl'] if rows else None,
                'global_delta_pct': rows[0]['step2_delta_pct_vs_active'] if rows else None,
                'target_pnl': round(target_pnl, 2),
            }), flush=True)

    rows.sort(key=lambda r: float(r['step2_pnl']), reverse=True)
    summary = {
        'script': 'step2_ensemble_probe.py',
        'completed': bool(rows and rows[0]['beats_target']),
        'active': _decorate(active_row, active_pnl, target_pnl),
        'active_step2_pnl': active_pnl,
        'target_pnl': target_pnl,
        'beat_pct': float(args.beat_pct),
        'seed_leaderboard': top_seed_rows,
        'scored_total': len(rows),
        'elapsed_sec': round(time.perf_counter() - started, 3),
        'leaderboard': rows[:100],
        'winners': [r for r in rows if r['beats_target']][:25],
        'step2_parity_contract': step2_parity_contract.contract(cfg),
        'step2_parity_contract_hash': step2_parity_contract.contract_hash(step2_parity_contract.contract(cfg)),
    }
    candidate_profile_schema.decorate_payload(summary, context={
        'script': 'step2_ensemble_probe.py',
        'start_balance': args.start_balance,
        'compiled_decision_tape': args.compiled_decision_tape,
        'active_step2_pnl': active_pnl,
        'target_pnl': target_pnl,
        'scored_total': len(rows),
    })
    (out_dir / 'summary.json').write_text(json.dumps(summary, indent=2, sort_keys=True), encoding='utf-8')
    print(json.dumps({
        'out': str(out_dir / 'summary.json'),
        'completed': summary['completed'],
        'best_variant': rows[0]['variant'],
        'best_pnl': rows[0]['step2_pnl'],
        'best_delta_pct': rows[0]['step2_delta_pct_vs_active'],
        'scored_total': len(rows),
        'target_pnl': round(target_pnl, 2),
        'elapsed_sec': summary['elapsed_sec'],
    }), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
