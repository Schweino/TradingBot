"""Outcome-guided Step 2 variant probe.

This does not change Step 2 semantics.  It uses the compiled tape's long/short
outcome columns to generate directed mutations, then scores every candidate
through the normal Step 2 state machine.
"""
from __future__ import annotations

import argparse
import json
import math
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


def _load_seeds(paths: list[str], limit: int) -> list[lab.Variant]:
    seeds: list[lab.Variant] = [active_engine_baseline.active_variant()]
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
            variant = _row_to_variant(row, f'{path.stem}_{idx}')
            if not variant:
                continue
            key = tournament_safety.model_id(variant.name, variant.weights, variant.bias)
            if key in seen:
                continue
            seen.add(key)
            seeds.append(variant)
            if len(seeds) >= limit:
                return seeds
    return seeds


def _score(compiled: dict, variants: list[lab.Variant], start_balance: float, batch_size: int,
           sim_config: dict[str, float | int]) -> list[dict]:
    out: list[dict] = []
    for start in range(0, len(variants), batch_size):
        batch = variants[start:start + batch_size]
        rows = decision_tape_compiled.simulate_variants(compiled, batch, start_balance, gate=None, sim_config=sim_config)
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


def _vector(variant: lab.Variant) -> tuple[np.ndarray, float]:
    w = np.zeros(len(fast.FEATURE_NAMES), dtype=np.float64)
    for idx, name in enumerate(fast.FEATURE_NAMES):
        w[idx] = float(variant.weights.get(name, 0.0) or 0.0)
    return w, float(variant.bias or 0.0)


def _from_vector(name: str, weights: np.ndarray, bias: float, limit: float) -> lab.Variant:
    clipped = np.clip(weights, -limit, limit)
    return _variant(name, {feature: float(clipped[idx]) for idx, feature in enumerate(fast.FEATURE_NAMES)}, bias)


def _decorate(row: dict, active_pnl: float, target_pnl: float) -> dict:
    out = dict(row)
    pnl = float(out['step2_pnl'])
    out['step2_delta_vs_active'] = round(pnl - active_pnl, 4)
    out['step2_delta_pct_vs_active'] = round((pnl / active_pnl - 1.0) * 100.0, 4) if active_pnl else None
    out['beats_target'] = pnl >= target_pnl
    return out


def _safe_norm(vec: np.ndarray) -> np.ndarray:
    scale = float(np.percentile(np.abs(vec), 90)) if vec.size else 0.0
    if not math.isfinite(scale) or scale <= 1e-12:
        scale = float(np.max(np.abs(vec))) if vec.size else 0.0
    if not math.isfinite(scale) or scale <= 1e-12:
        return vec.copy()
    return vec / scale


def _gradient_direction(x: np.ndarray, y: np.ndarray, advantage: np.ndarray, score: np.ndarray,
                        mask: np.ndarray, power: float, temp: float) -> tuple[np.ndarray, float]:
    selected = mask & np.isfinite(score)
    if int(selected.sum()) == 0:
        return np.zeros(x.shape[1], dtype=np.float64), 0.0
    margin = y[selected] * score[selected]
    pressure = 1.0 / (1.0 + np.exp(np.clip(margin / max(temp, 1e-6), -40, 40)))
    weights = np.power(np.maximum(advantage[selected], 1e-9), power) * pressure
    if float(weights.sum()) <= 1e-12:
        return np.zeros(x.shape[1], dtype=np.float64), 0.0
    signed = weights * y[selected]
    grad = signed @ x[selected]
    bias_grad = float(signed.sum())
    grad = _safe_norm(np.asarray(grad, dtype=np.float64))
    if math.isfinite(bias_grad):
        bias_grad = float(np.clip(bias_grad / max(float(np.abs(signed).sum()), 1.0), -2.0, 2.0))
    else:
        bias_grad = 0.0
    return grad, bias_grad


def _generate_variants(compiled: dict, seed_rows: list[dict], rng: random.Random,
                       limit: float, max_variants: int) -> list[lab.Variant]:
    x = np.asarray(compiled['features'], dtype=np.float64)
    long_pnl = np.asarray(compiled['long_pnl_pct'], dtype=np.float64)
    short_pnl = np.asarray(compiled['short_pnl_pct'], dtype=np.float64)
    y = np.where(long_pnl >= short_pnl, 1.0, -1.0)
    advantage = np.abs(long_pnl - short_pnl)
    ticker_code = np.asarray(compiled['ticker_code'])
    setup_code = np.asarray(compiled['setup_code'])
    day_code = np.asarray(compiled['day_code'])

    variants: list[lab.Variant] = []
    seen = set()

    masks: list[tuple[str, np.ndarray]] = [('all', np.ones(x.shape[0], dtype=bool))]
    for code in sorted(set(int(v) for v in ticker_code.tolist())):
        masks.append((f'ticker{code}', ticker_code == code))
    for code in sorted(set(int(v) for v in setup_code.tolist())):
        masks.append((f'setup{code}', setup_code == code))
    day_pnls = []
    for code in sorted(set(int(v) for v in day_code.tolist())):
        day_pnls.append((code, np.percentile(advantage[day_code == code], 85)))
    for code, _ in sorted(day_pnls, key=lambda item: item[1], reverse=True)[:8]:
        masks.append((f'day{code}', day_code == code))
    for q in (0.70, 0.80, 0.90, 0.95):
        masks.append((f'adv{int(q * 100)}', advantage >= float(np.quantile(advantage, q))))

    alphas = [-5.0, -3.0, -2.0, -1.25, -0.75, -0.35, 0.35, 0.75, 1.25, 2.0, 3.0, 5.0]
    bias_alphas = [-3.0, -1.5, -0.75, 0.0, 0.75, 1.5, 3.0]
    powers = [0.25, 0.5, 1.0, 1.5, 2.0]
    temps = [0.35, 0.75, 1.5, 3.0, 6.0]

    for seed_idx, row in enumerate(seed_rows):
        seed = _variant(str(row['variant']), dict(row['weights']), float(row.get('bias') or 0.0))
        base_w, base_b = _vector(seed)
        score = x @ base_w + base_b
        chosen = np.where(score > 0.0, 1.0, -1.0)
        masks_for_seed = list(masks)
        masks_for_seed.extend([
            ('wrong', chosen != y),
            ('wrong_adv75', (chosen != y) & (advantage >= float(np.quantile(advantage, 0.75)))),
            ('near', np.abs(score) <= float(np.quantile(np.abs(score), 0.35))),
            ('near_wrong', (np.abs(score) <= float(np.quantile(np.abs(score), 0.55))) & (chosen != y)),
        ])
        for mask_name, mask in masks_for_seed:
            for power in powers:
                for temp in temps:
                    grad, bias_grad = _gradient_direction(x, y, advantage, score, mask, power, temp)
                    if float(np.max(np.abs(grad))) <= 1e-12:
                        continue
                    for alpha in alphas:
                        for bias_alpha in bias_alphas:
                            weights = base_w + alpha * grad
                            bias = base_b + bias_alpha * bias_grad
                            name = f'grad_s{seed_idx:02d}_{mask_name}_p{power:g}_t{temp:g}_a{alpha:+g}_b{bias_alpha:+g}'
                            variant = _from_vector(name, weights, bias, limit)
                            key = tournament_safety.model_id(variant.name, variant.weights, variant.bias)
                            if key in seen:
                                continue
                            seen.add(key)
                            variants.append(variant)
                            if len(variants) >= max_variants:
                                return variants
        for _ in range(120):
            weights = base_w.copy()
            for feature_idx in rng.sample(range(len(fast.FEATURE_NAMES)), rng.randint(2, 10)):
                weights[feature_idx] += rng.gauss(0.0, rng.choice([0.15, 0.35, 0.8, 1.4]))
            bias = base_b + rng.gauss(0.0, rng.choice([0.05, 0.15, 0.35, 0.8]))
            variant = _from_vector(f'grad_jitter_s{seed_idx:02d}_{len(variants):06d}', weights, bias, limit)
            key = tournament_safety.model_id(variant.name, variant.weights, variant.bias)
            if key not in seen:
                seen.add(key)
                variants.append(variant)
                if len(variants) >= max_variants:
                    return variants
    return variants


def main() -> int:
    ap = argparse.ArgumentParser(description='Outcome-guided Step 2 gradient probe.')
    ap.add_argument('--compiled-decision-tape', default=str(DEFAULT_COMPILED))
    ap.add_argument('--out-dir', default=str(DEFAULT_OUT))
    ap.add_argument('--name', default='gradient_probe')
    ap.add_argument('--seed-json', action='append', default=[])
    ap.add_argument('--seed-limit', type=int, default=400)
    ap.add_argument('--top-seeds', type=int, default=12)
    ap.add_argument('--max-variants', type=int, default=60000)
    ap.add_argument('--batch-size', type=int, default=500)
    ap.add_argument('--start-balance', type=float, default=100000.0)
    ap.add_argument('--beat-pct', type=float, default=25.0)
    ap.add_argument('--weight-limit', type=float, default=12.0)
    ap.add_argument('--seed', type=int, default=20260508)
    args = ap.parse_args()

    started = time.perf_counter()
    rng = random.Random(int(args.seed))
    compiled = decision_tape_compiled.load_compiled(args.compiled_decision_tape, mmap=True)
    cfg = _load_config()
    sim_config = step2_parity_contract.sim_config(cfg)
    out_dir = Path(args.out_dir) / args.name
    out_dir.mkdir(parents=True, exist_ok=True)

    active = active_engine_baseline.active_variant()
    active_row = _score(compiled, [active], float(args.start_balance), int(args.batch_size), sim_config)[0]
    active_pnl = float(active_row['step2_pnl'])
    target_pnl = active_pnl * (1.0 + float(args.beat_pct) / 100.0)

    seeds = _load_seeds(args.seed_json, int(args.seed_limit))
    seed_rows = _score(compiled, seeds, float(args.start_balance), int(args.batch_size), sim_config)
    seed_rows = [_decorate(r, active_pnl, target_pnl) for r in seed_rows]
    seed_rows.sort(key=lambda r: float(r['step2_pnl']), reverse=True)
    print(json.dumps({
        'stage': 'seed_rescore',
        'seed_count': len(seeds),
        'best_variant': seed_rows[0]['variant'],
        'best_pnl': seed_rows[0]['step2_pnl'],
        'delta_pct': seed_rows[0]['step2_delta_pct_vs_active'],
        'target_pnl': round(target_pnl, 2),
    }), flush=True)

    variants = _generate_variants(
        compiled,
        seed_rows[:int(args.top_seeds)],
        rng,
        float(args.weight_limit),
        int(args.max_variants),
    )
    scored = _score(compiled, variants, float(args.start_balance), int(args.batch_size), sim_config)
    scored = [_decorate(r, active_pnl, target_pnl) for r in scored]
    leaderboard = sorted(seed_rows[:100] + scored, key=lambda r: float(r['step2_pnl']), reverse=True)
    winners = [r for r in leaderboard if float(r['step2_pnl']) >= target_pnl]
    summary = {
        'script': 'step2_gradient_probe.py',
        'completed': bool(winners),
        'active': _decorate(active_row, active_pnl, target_pnl),
        'active_step2_pnl': active_pnl,
        'target_pnl': target_pnl,
        'beat_pct': float(args.beat_pct),
        'seed_count': len(seeds),
        'generated_variants': len(variants),
        'elapsed_sec': round(time.perf_counter() - started, 3),
        'step2_parity_contract': step2_parity_contract.contract(cfg),
        'step2_parity_contract_hash': step2_parity_contract.contract_hash(step2_parity_contract.contract(cfg)),
        'leaderboard': leaderboard[:100],
        'winners': winners[:25],
    }
    candidate_profile_schema.decorate_payload(summary, context={
        'script': 'step2_gradient_probe.py',
        'start_balance': args.start_balance,
        'compiled_decision_tape': args.compiled_decision_tape,
        'active_step2_pnl': active_pnl,
        'target_pnl': target_pnl,
        'scored_total': len(leaderboard),
    })
    (out_dir / 'summary.json').write_text(json.dumps(summary, indent=2, sort_keys=True), encoding='utf-8')
    print(json.dumps({
        'out': str(out_dir / 'summary.json'),
        'completed': summary['completed'],
        'generated_variants': len(variants),
        'best_variant': leaderboard[0]['variant'],
        'best_pnl': leaderboard[0]['step2_pnl'],
        'best_delta_pct': leaderboard[0]['step2_delta_pct_vs_active'],
        'target_pnl': round(target_pnl, 2),
        'elapsed_sec': summary['elapsed_sec'],
    }), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
