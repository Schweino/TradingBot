"""
Vectorized scoring-variant lab.

This keeps the same hypothesis as scoring_variant_lab.py:
  - read replayed trade opportunities from CSV
  - turn indicator/reason text into numeric features
  - score each variant
  - mechanically invert P/L when a variant picks the opposite side

The difference is performance: features and weights are NumPy matrices, so
large variant batches are evaluated with vectorized math instead of Python
loops.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import time
from typing import Iterable

import numpy as np

import scoring_variant_lab as slow


HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CSV = os.path.join(
    HERE,
    'postmortem',
    'backtests',
    'engine_replay_2026-04-06_2026-05-01_trades.csv',
)
DEFAULT_OUT = os.path.join(
    HERE,
    'postmortem',
    'backtests',
    'scoring_variant_lab_fast_2026-04-06_2026-05-01.json',
)

FEATURE_NAMES = [
    'ema',
    'vwap',
    'momentum',
    'btc',
    'relative',
    'miner',
    'burst',
    'flow_contra',
    'btc_chop',
    'exec_penalty',
    'open_phase',
    'midday_phase',
    'riot_short_penalty',
    'riot_long_penalty',
    'vwap_sigma_ext',
    'btc_mom_abs',
    'flow_pressure_abs',
    'setup_btc_relative_strength',
    'setup_momentum_breakout',
    'setup_trend_pullback',
    'setup_flow_exhaustion_fade',
    'setup_vwap_reclaim_breakdown',
    'brs_weak_followthrough',
    'brs_open_risk',
    'brs_bear_normal_risk',
    'brs_open_weak_followthrough',
    'timeout_decay_risk',
    'brs_open_continuation_quality',
    'brs_normal_continuation_quality',
    'medium_brs_penalty',
    'brs_open_medium_risk',
    'brs_open_inversion_risk',
    'brs_open_non_riot_inversion_risk',
    'brs_clsk_mara_inversion_risk',
    'brs_open_btc_neutral_risk',
    'brs_open_low_range_risk',
    'brs_open_low_range_020',
    'brs_open_low_range_025',
    'brs_open_low_range_035',
    'brs_open_low_range_040',
    'brs_open_low_range_045',
    'brs_open_side_bad_range_025',
    'brs_open_side_bad_range_031',
    'brs_open_side_bad_range_040',
    'brs_wide_spread_risk',
    'brs_open_book_worsening_risk',
    'brs_open_flow_quote_failure',
    'brs_open_flow_book_failure',
    'brs_open_flow_book_failure_loose',
    'brs_open_session_neutral_failure',
    'brs_open_range_neutral_mom_failure',
    'brs_open_three_tape_failure',
    'brs_alignment_failure',
    'brs_open_alignment_failure',
    'brs_normal_alignment_failure',
]


def load_rows(path: str) -> list[dict]:
    with open(path, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def build_matrices(rows: list[dict]):
    features = np.zeros((len(rows), len(FEATURE_NAMES)), dtype=np.float64)
    pnl = np.zeros(len(rows), dtype=np.float64)
    original_side = np.zeros(len(rows), dtype=np.int8)
    tickers = []
    for i, row in enumerate(rows):
        feats = slow.features(row)
        for j, name in enumerate(FEATURE_NAMES):
            features[i, j] = float(feats.get(name, 0.0) or 0.0)
        pnl[i] = slow._num(row.get('pnl'))
        original_side[i] = 1 if str(row.get('side')).upper() == 'LONG' else -1
        tickers.append(row.get('ticker') or '')
    return features, pnl, original_side, np.array(tickers, dtype=object)


def variant_matrix(variants: list[slow.Variant]):
    weights = np.zeros((len(variants), len(FEATURE_NAMES)), dtype=np.float64)
    bias = np.zeros(len(variants), dtype=np.float64)
    for i, variant in enumerate(variants):
        for j, name in enumerate(FEATURE_NAMES):
            weights[i, j] = float(variant.weights.get(name, 0.0) or 0.0)
        bias[i] = float(variant.bias or 0.0)
    return weights, bias


def summarize_batch(features, pnl, original_side, variants: list[slow.Variant], starting_balance: float):
    weights, bias = variant_matrix(variants)
    scores = features @ weights.T
    scores += bias.reshape(1, -1)
    chosen_side = np.where(scores > 0, 1, np.where(scores < 0, -1, original_side.reshape(-1, 1)))
    flipped = chosen_side != original_side.reshape(-1, 1)
    model_pnl = np.where(flipped, -pnl.reshape(-1, 1), pnl.reshape(-1, 1))
    wins = (model_pnl > 0).sum(axis=0)
    losses = (model_pnl < 0).sum(axis=0)
    pnl_sum = model_pnl.sum(axis=0)
    flipped_sum = flipped.sum(axis=0)
    n = len(pnl)
    results = []
    for i, variant in enumerate(variants):
        results.append({
            'variant': variant.name,
            'trades': n,
            'wins': int(wins[i]),
            'losses': int(losses[i]),
            'flipped': int(flipped_sum[i]),
            'win_rate_pct': round(100 * int(wins[i]) / n, 2) if n else None,
            'pnl': round(float(pnl_sum[i]), 2),
            'starting_balance': starting_balance,
            'ending_balance': round(starting_balance + float(pnl_sum[i]), 2),
        })
    results.sort(key=lambda r: r['pnl'], reverse=True)
    return results


def evaluate(variants: list[slow.Variant], rows: list[dict], starting_balance: float, batch_size: int):
    features, pnl, original_side, tickers = build_matrices(rows)
    all_results = []
    for start in range(0, len(variants), batch_size):
        batch = variants[start:start + batch_size]
        all_results.extend(summarize_batch(features, pnl, original_side, batch, starting_balance))
    all_results.sort(key=lambda r: r['pnl'], reverse=True)
    return all_results


def add_top_details(results: list[dict], variants: list[slow.Variant], rows: list[dict], starting_balance: float, top_n: int):
    by_name = {variant.name: variant for variant in variants}
    detailed = []
    for result in results[:top_n]:
        variant = by_name[result['variant']]
        summary = slow.evaluate(rows, variant, starting_balance)
        summary['weights'] = variant.weights
        summary['bias'] = variant.bias
        detailed.append(summary)
    return detailed


def extra_elite_variants(count: int, seed: int = 20260503) -> list[slow.Variant]:
    """Deterministically sample a broad local grid around the current elite winner."""
    if count <= 0:
        return []

    grids = {
        'ema': [0.6, 0.75, 0.9, 1.0, 1.1, 1.25, 1.45],
        'vwap': [-0.4, -0.25, -0.1, 0.0, 0.1, 0.25, 0.4],
        'momentum': [-0.4, -0.25, -0.1, 0.0, 0.1, 0.25, 0.4, 0.6],
        'btc': [0.1, 0.2, 0.3, 0.4, 0.55, 0.7, 0.9, 1.15],
        'relative': [-1.25, -1.0, -0.8, -0.65, -0.5, -0.35, -0.2],
        'miner': [0.0, 0.2, 0.35, 0.5, 0.65, 0.8, 1.0],
        'burst': [-0.25, 0.0, 0.1, 0.25, 0.4, 0.6, 0.85],
        'flow_contra': [-0.8, -0.6, -0.4, -0.25, -0.1, 0.0, 0.15, 0.35],
        'btc_chop': [0.2, 0.4, 0.6, 0.75, 0.9, 1.1, 1.35, 1.65],
        'exec_penalty': [0.0, 0.2, 0.35, 0.5, 0.65, 0.8, 1.0],
        'setup_btc_relative_strength': [-2.0, -1.75, -1.5, -1.25, -1.0, -0.75, -0.5],
        'midday_phase': [0.0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5],
        'open_phase': [-0.75, -0.4, 0.0, 0.4, 0.75],
        'setup_momentum_breakout': [-0.5, 0.0, 0.5, 1.0],
        'setup_trend_pullback': [-1.0, -0.5, 0.0, 0.5],
        'riot_short_penalty': [-0.5, 0.0, 0.5, 1.0],
        'riot_long_penalty': [-1.0, -0.5, 0.0, 0.5],
        'vwap_sigma_ext': [-0.5, -0.25, 0.0, 0.25],
        'btc_mom_abs': [-0.25, 0.0, 0.25, 0.5],
        'flow_pressure_abs': [-0.5, -0.25, 0.0, 0.25],
    }
    base = {
        'ema': 1.0,
        'vwap': 0.0,
        'momentum': 0.0,
        'btc': 0.4,
        'relative': -0.5,
        'miner': 0.5,
        'burst': 0.25,
        'flow_contra': -0.25,
        'btc_chop': 0.75,
        'exec_penalty': 0.5,
        'setup_btc_relative_strength': -1.25,
        'midday_phase': 0.75,
    }

    rng = np.random.default_rng(seed)
    names = list(grids)
    variants: list[slow.Variant] = []
    seen: set[tuple[tuple[str, float], ...]] = set()
    attempts = 0
    while len(variants) < count:
        attempts += 1
        if attempts > count * 50:
            raise RuntimeError(f'could only generate {len(variants)} unique extra elite variants')
        weights = dict(base)
        for name in names:
            if name not in base and rng.random() > 0.35:
                continue
            values = grids[name]
            weights[name] = float(values[int(rng.integers(0, len(values)))])
        signature = tuple(sorted(weights.items()))
        if signature in seen:
            continue
        seen.add(signature)
        variants.append(slow.Variant(
            f'fast_v29_elite_extra_{seed}_{len(variants) + 1:06d}',
            weights,
        ))
    return variants


def _verify(rows: list[dict], starting_balance: float) -> dict:
    names = {
        'v29_relative_strength_chase_penalty',
        'v54427_v29_elite_rel0_vw0_mom0_btc0_chop0_exec3_ema2_miner2_burst2_setup3_flow0_x6',
    }
    variants = [v for v in slow.VARIANTS if v.name in names]
    fast_results = evaluate(variants, rows, starting_balance, batch_size=max(1, len(variants)))
    fast_results = add_top_details(fast_results, variants, rows, starting_balance, len(variants))
    slow_results = [slow.evaluate(rows, v, starting_balance) for v in variants]
    by_name_fast = {r['variant']: r for r in fast_results}
    by_name_slow = {r['variant']: r for r in slow_results}
    diffs = {}
    for name in names:
        f = by_name_fast[name]
        s = by_name_slow[name]
        diffs[name] = {
            'fast_pnl': f['pnl'],
            'slow_pnl': s['pnl'],
            'pnl_diff': round(f['pnl'] - s['pnl'], 6),
            'fast_wins': f['wins'],
            'slow_wins': s['wins'],
            'fast_losses': f['losses'],
            'slow_losses': s['losses'],
            'fast_flipped': f['flipped'],
            'slow_flipped': s['flipped'],
        }
    return diffs


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--csv', default=DEFAULT_CSV)
    ap.add_argument('--out', default=DEFAULT_OUT)
    ap.add_argument('--starting-balance', type=float, default=100000.0)
    ap.add_argument('--batch-size', type=int, default=5000)
    ap.add_argument('--top-n', type=int, default=100)
    ap.add_argument('--verify', action='store_true')
    ap.add_argument('--extra-elite-variants', type=int, default=0)
    ap.add_argument('--extra-seed', type=int, default=20260503)
    args = ap.parse_args()

    started_at = time.perf_counter()
    rows = load_rows(args.csv)
    variants = list(slow.VARIANTS)
    if args.extra_elite_variants:
        variants.extend(extra_elite_variants(args.extra_elite_variants, args.extra_seed))
    verification = _verify(rows, args.starting_balance) if args.verify else None
    results = evaluate(variants, rows, args.starting_balance, args.batch_size)
    top_results = add_top_details(results, variants, rows, args.starting_balance, args.top_n)
    elapsed_seconds = round(time.perf_counter() - started_at, 3)
    payload = {
        'source_csv': args.csv,
        'variants': len(variants),
        'base_variants': len(slow.VARIANTS),
        'extra_elite_variants': args.extra_elite_variants,
        'extra_seed': args.extra_seed,
        'elapsed_seconds': elapsed_seconds,
        'method_note': (
            'Vectorized equivalent of scoring_variant_lab.py. Uses the same '
            'feature extraction, weights, flip logic, and P/L math.'
        ),
        'verification': verification,
        'results': top_results,
    }
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    print(json.dumps({
        'out': args.out,
        'variants': len(variants),
        'elapsed_seconds': elapsed_seconds,
        'best': top_results[0],
        'verification': verification,
    }, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
