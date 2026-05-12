"""Confidence-deadband Step 2 probe.

Tests whether the remaining edge comes from skipping low-margin scoring
opportunities instead of forcing every opportunity to be LONG or SHORT.
"""
from __future__ import annotations

import argparse
import json
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

try:
    from numba import njit
except Exception:  # pragma: no cover
    njit = None


HERE = Path(__file__).resolve().parent
DEFAULT_COMPILED = HERE / 'postmortem' / 'backtests' / 'compiled_decision_tapes' / (
    'compiled_step2_current_live_CLSK-MARA-RIOT_2026-04-06_2026-05-08'
) / 'manifest.json'
DEFAULT_OUT = HERE / 'postmortem' / 'backtests' / 'step2_adaptive_hunter'
TRADE_SIZE_PCT = 0.25


if njit is not None:
    @njit(cache=True)
    def _simulate_skip_core(sides, ts, ticker_code, setup_code, day_code,
                            long_pnl_pct, short_pnl_pct, long_held, short_held,
                            cooldown_by_setup, starting_balance, trade_size_pct,
                            ticker_count, day_count, same_ticker_reentry_cooldown_sec):
        variants = sides.shape[0]
        rows = sides.shape[1]
        pnl = np.zeros(variants, dtype=np.float64)
        wins = np.zeros(variants, dtype=np.int64)
        losses = np.zeros(variants, dtype=np.int64)
        trades = np.zeros(variants, dtype=np.int64)
        skipped_open = np.zeros(variants, dtype=np.int64)
        skipped_cooldown = np.zeros(variants, dtype=np.int64)
        skipped_deadband = np.zeros(variants, dtype=np.int64)
        by_day_pnl = np.zeros((variants, day_count), dtype=np.float64)
        by_day_trades = np.zeros((variants, day_count), dtype=np.int64)
        by_ticker_pnl = np.zeros((variants, ticker_count), dtype=np.float64)
        by_ticker_trades = np.zeros((variants, ticker_count), dtype=np.int64)
        setup_count = 8
        for i in range(rows):
            setup_i = int(setup_code[i])
            if setup_i + 1 > setup_count:
                setup_count = setup_i + 1
        for v in range(variants):
            balance = starting_balance
            open_until = np.zeros(ticker_count, dtype=np.int64)
            last_signal = np.full((ticker_count, 2, setup_count), -1e18, dtype=np.float64)
            for i in range(rows):
                t = int(ts[i])
                tkr = int(ticker_code[i])
                d = int(day_code[i])
                if t < open_until[tkr]:
                    skipped_open[v] += 1
                    continue
                side = int(sides[v, i])
                if side == 0:
                    skipped_deadband[v] += 1
                    continue
                setup = int(setup_code[i])
                side_idx = 0 if side == 1 else 1
                if float(t) - last_signal[tkr, side_idx, setup] < cooldown_by_setup[setup]:
                    skipped_cooldown[v] += 1
                    continue
                pct = long_pnl_pct[i] if side == 1 else short_pnl_pct[i]
                held = long_held[i] if side == 1 else short_held[i]
                alloc = np.round((balance * trade_size_pct / max(1, ticker_count)) * 100.0) / 100.0
                row_pnl = alloc * pct / 100.0
                pnl[v] += row_pnl
                balance += row_pnl
                trades[v] += 1
                wins[v] += 1 if row_pnl > 0.0 else 0
                losses[v] += 1 if row_pnl < 0.0 else 0
                by_day_pnl[v, d] += row_pnl
                by_day_trades[v, d] += 1
                by_ticker_pnl[v, tkr] += row_pnl
                by_ticker_trades[v, tkr] += 1
                open_until[tkr] = t + int(held) + same_ticker_reentry_cooldown_sec
                last_signal[tkr, side_idx, setup] = float(t)
        return (pnl, wins, losses, trades, skipped_open, skipped_cooldown,
                skipped_deadband, by_day_pnl, by_day_trades, by_ticker_pnl, by_ticker_trades)
else:
    _simulate_skip_core = None


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


def _score_regular(compiled: dict, variants: list[lab.Variant], start_balance: float,
                   sim_config: dict[str, float | int]) -> list[dict]:
    rows = decision_tape_compiled.simulate_variants(compiled, variants, start_balance, gate=None, sim_config=sim_config)
    if rows is None:
        raise RuntimeError('compiled Step 2 unavailable')
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
        })
    out.sort(key=lambda r: float(r['step2_pnl']), reverse=True)
    return out


def _variant_matrix(variants: list[lab.Variant]) -> tuple[np.ndarray, np.ndarray]:
    weights = np.zeros((len(variants), len(fast.FEATURE_NAMES)), dtype=np.float64)
    bias = np.zeros(len(variants), dtype=np.float64)
    for i, variant in enumerate(variants):
        for j, feature in enumerate(fast.FEATURE_NAMES):
            weights[i, j] = float(variant.weights.get(feature, 0.0) or 0.0)
        bias[i] = float(variant.bias or 0.0)
    return weights, bias


def _reverse_map(mapping: dict) -> dict[int, str]:
    return {int(v): str(k) for k, v in (mapping or {}).items()}


def _simulate(compiled: dict, names: list[str], sides: np.ndarray, start_balance: float,
              thresholds: list[float], active_pnl: float, target_pnl: float) -> list[dict]:
    if _simulate_skip_core is None:
        raise RuntimeError('numba unavailable')
    arrays = _simulate_skip_core(
        sides.astype(np.int8),
        compiled['ts'],
        compiled['ticker_code'],
        compiled['setup_code'],
        compiled['day_code'],
        compiled['long_pnl_pct'],
        compiled['short_pnl_pct'],
        compiled['long_held'],
        compiled['short_held'],
        compiled['cooldown_by_setup'],
        float(start_balance),
        float(TRADE_SIZE_PCT),
        int(compiled['ticker_count']),
        int(compiled.get('day_count') or 1),
        5,
    )
    (pnl, wins, losses, trades, skipped_open, skipped_cooldown, skipped_deadband,
     by_day_pnl, by_day_trades, by_ticker_pnl, by_ticker_trades) = arrays
    day_names = _reverse_map(compiled.get('day_map') or {})
    ticker_names = _reverse_map(compiled.get('ticker_map') or {})
    out = []
    for idx, name in enumerate(names):
        by_day = {
            day_names.get(day_idx, str(day_idx)): {
                'pnl': round(float(by_day_pnl[idx, day_idx]), 2),
                'trades': int(by_day_trades[idx, day_idx]),
            }
            for day_idx in range(by_day_pnl.shape[1])
        }
        by_ticker = {
            ticker_names.get(ticker_idx, str(ticker_idx)): {
                'pnl': round(float(by_ticker_pnl[idx, ticker_idx]), 2),
                'trades': int(by_ticker_trades[idx, ticker_idx]),
            }
            for ticker_idx in range(by_ticker_pnl.shape[1])
        }
        pnl_i = float(pnl[idx])
        out.append({
            'variant': name,
            'confidence_deadband': thresholds[idx],
            'step2_pnl': round(pnl_i, 2),
            'step2_trades': int(trades[idx]),
            'step2_win_rate_pct': round(100.0 * int(wins[idx]) / int(trades[idx]), 2) if int(trades[idx]) else None,
            'wins': int(wins[idx]),
            'losses': int(losses[idx]),
            'skipped': {
                'open_position': int(skipped_open[idx]),
                'cooldown': int(skipped_cooldown[idx]),
                'confidence_deadband': int(skipped_deadband[idx]),
            },
            'by_day': by_day,
            'by_ticker': by_ticker,
            'step2_delta_vs_active': round(pnl_i - active_pnl, 4),
            'step2_delta_pct_vs_active': round((pnl_i / active_pnl - 1.0) * 100.0, 4) if active_pnl else None,
            'beats_target': pnl_i >= target_pnl,
        })
    out.sort(key=lambda r: float(r['step2_pnl']), reverse=True)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description='Confidence-deadband Step 2 probe.')
    ap.add_argument('--compiled-decision-tape', default=str(DEFAULT_COMPILED))
    ap.add_argument('--out-dir', default=str(DEFAULT_OUT))
    ap.add_argument('--name', default='confidence_gate_probe')
    ap.add_argument('--seed-json', action='append', default=[])
    ap.add_argument('--seed-limit', type=int, default=500)
    ap.add_argument('--top-seeds', type=int, default=25)
    ap.add_argument('--thresholds', type=int, default=80)
    ap.add_argument('--start-balance', type=float, default=100000.0)
    ap.add_argument('--beat-pct', type=float, default=25.0)
    args = ap.parse_args()

    cfg = _load_config()
    sim_config = step2_parity_contract.sim_config(cfg)
    compiled = decision_tape_compiled.load_compiled(args.compiled_decision_tape, mmap=True)
    out_dir = Path(args.out_dir) / args.name
    out_dir.mkdir(parents=True, exist_ok=True)

    active = active_engine_baseline.active_variant()
    active_row = _score_regular(compiled, [active], float(args.start_balance), sim_config)[0]
    active_pnl = float(active_row['step2_pnl'])
    target_pnl = active_pnl * (1.0 + float(args.beat_pct) / 100.0)

    seeds = _load_seeds(args.seed_json, int(args.seed_limit))
    seed_rows = _score_regular(compiled, seeds, float(args.start_balance), sim_config)
    seed_rows.sort(key=lambda r: float(r['step2_pnl']), reverse=True)
    top = seed_rows[:int(args.top_seeds)]
    top_variants = [_variant(str(r['variant']), dict(r['weights']), float(r.get('bias') or 0.0)) for r in top]
    weights, bias = _variant_matrix(top_variants)
    scores = np.asarray(compiled['features']) @ weights.T
    scores += bias.reshape(1, -1)
    original = np.asarray(compiled['original_side'], dtype=np.int8)

    rows = []
    names = []
    thresholds = []
    sides_list = []
    for seed_idx, row in enumerate(top):
        score = scores[:, seed_idx]
        abs_score = np.abs(score[np.isfinite(score)])
        qs = np.linspace(0.0, 0.85, max(4, int(args.thresholds)))
        raw_thresholds = sorted(set(round(float(np.quantile(abs_score, q)), 6) for q in qs))
        raw_thresholds.extend([0.0, 0.02, 0.05, 0.1, 0.2, 0.35, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0])
        for threshold in sorted(set(raw_thresholds)):
            sides = np.where(score > threshold, 1, np.where(score < -threshold, -1, 0)).astype(np.int8)
            # Threshold zero should match normal sign behavior except exact-zero rows,
            # where the production scorer falls back to original side.
            if threshold == 0.0:
                sides = np.where(score > 0.0, 1, np.where(score < 0.0, -1, original)).astype(np.int8)
            sides_list.append(sides)
            names.append(f'conf_s{seed_idx:02d}_{str(row["variant"])[:40]}_thr{threshold:.6f}')
            thresholds.append(float(threshold))
            if len(sides_list) >= 250:
                batch_rows = _simulate(compiled, names, np.vstack(sides_list), float(args.start_balance),
                                       thresholds, active_pnl, target_pnl)
                rows.extend(batch_rows)
                names, thresholds, sides_list = [], [], []
    if sides_list:
        rows.extend(_simulate(compiled, names, np.vstack(sides_list), float(args.start_balance),
                              thresholds, active_pnl, target_pnl))
    rows.sort(key=lambda r: float(r['step2_pnl']), reverse=True)

    summary = {
        'script': 'step2_confidence_gate_probe.py',
        'completed': bool(rows and rows[0]['beats_target']),
        'active': active_row,
        'active_step2_pnl': active_pnl,
        'target_pnl': target_pnl,
        'beat_pct': float(args.beat_pct),
        'seed_leaderboard': top,
        'scored_total': len(rows),
        'leaderboard': rows[:100],
        'winners': [r for r in rows if r['beats_target']][:25],
        'step2_parity_contract': step2_parity_contract.contract(cfg),
        'step2_parity_contract_hash': step2_parity_contract.contract_hash(step2_parity_contract.contract(cfg)),
    }
    candidate_profile_schema.decorate_payload(summary, context={
        'script': 'step2_confidence_gate_probe.py',
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
        'best_trades': rows[0]['step2_trades'],
        'best_threshold': rows[0]['confidence_deadband'],
        'target_pnl': round(target_pnl, 2),
        'scored_total': len(rows),
    }), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
