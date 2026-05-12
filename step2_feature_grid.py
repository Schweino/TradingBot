"""Compact multi-feature grid around a Step 2 leader."""
from __future__ import annotations

import argparse
import itertools
import json
from pathlib import Path
from typing import Any

import active_engine_baseline
import candidate_profile_schema
import decision_tape_compiled
import scoring_variant_lab as lab
import scoring_variant_lab_fast as fast
import step2_parity_contract


HERE = Path(__file__).resolve().parent
DEFAULT_COMPILED = HERE / 'postmortem' / 'backtests' / 'compiled_decision_tapes' / (
    'compiled_step2_current_live_CLSK-MARA-RIOT_2026-04-06_2026-05-08'
) / 'manifest.json'
DEFAULT_OUT = HERE / 'postmortem' / 'backtests' / 'step2_adaptive_hunter'


def _load_config() -> dict[str, Any]:
    with (HERE / 'trading_config.json').open('r', encoding='utf-8') as f:
        return json.load(f)


def _variant(name: str, weights: dict[str, float], bias: float = 0.0) -> lab.Variant:
    clean = {k: round(float(v), 6) for k, v in weights.items() if k in fast.FEATURE_NAMES and abs(float(v)) > 1e-10}
    return lab.Variant(name, clean, round(float(bias or 0.0), 6))


def _walk_rows(obj: Any):
    if isinstance(obj, dict):
        if isinstance(obj.get('weights'), dict):
            yield obj
        for value in obj.values():
            yield from _walk_rows(value)
    elif isinstance(obj, list):
        for value in obj:
            yield from _walk_rows(value)


def _load_seed(path: str) -> lab.Variant:
    data = json.loads(Path(path).read_text(encoding='utf-8'))
    best = None
    for row in _walk_rows(data):
        if not isinstance(row.get('weights'), dict):
            continue
        if best is None or float(row.get('step2_pnl') or -1e18) > float(best.get('step2_pnl') or -1e18):
            best = row
    if best is None:
        raise RuntimeError(f'no seed found in {path}')
    return _variant(str(best.get('variant') or 'seed'), dict(best['weights']), float(best.get('bias') or 0.0))


def _score(compiled: dict, variants: list[lab.Variant], start_balance: float, batch_size: int,
           sim_config: dict[str, float | int]) -> list[dict]:
    out = []
    for start in range(0, len(variants), batch_size):
        batch = variants[start:start + batch_size]
        rows = decision_tape_compiled.simulate_variants(compiled, batch, start_balance, gate=None, sim_config=sim_config)
        if rows is None:
            raise RuntimeError('compiled Step 2 unavailable')
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


def _decorate(row: dict, active_pnl: float, target_pnl: float) -> dict:
    out = dict(row)
    pnl = float(out['step2_pnl'])
    out['step2_delta_vs_active'] = round(pnl - active_pnl, 4)
    out['step2_delta_pct_vs_active'] = round((pnl / active_pnl - 1.0) * 100.0, 4) if active_pnl else None
    out['beats_target'] = pnl >= target_pnl
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description='Compact Step 2 feature grid.')
    ap.add_argument('--compiled-decision-tape', default=str(DEFAULT_COMPILED))
    ap.add_argument('--out-dir', default=str(DEFAULT_OUT))
    ap.add_argument('--name', default='feature_grid')
    ap.add_argument('--seed-json', required=True)
    ap.add_argument('--features', nargs='+', required=True)
    ap.add_argument('--deltas', nargs='+', type=float, default=[-2, -1, -0.5, 0, 0.5, 1, 2])
    ap.add_argument('--batch-size', type=int, default=500)
    ap.add_argument('--start-balance', type=float, default=100000.0)
    ap.add_argument('--beat-pct', type=float, default=25.0)
    ap.add_argument('--weight-limit', type=float, default=32.0)
    args = ap.parse_args()

    cfg = _load_config()
    sim_config = step2_parity_contract.sim_config(cfg)
    compiled = decision_tape_compiled.load_compiled(args.compiled_decision_tape, mmap=True)
    out_dir = Path(args.out_dir) / args.name
    out_dir.mkdir(parents=True, exist_ok=True)
    active = active_engine_baseline.active_variant()
    active_row = _score(compiled, [active], float(args.start_balance), int(args.batch_size), sim_config)[0]
    active_pnl = float(active_row['step2_pnl'])
    target_pnl = active_pnl * (1.0 + float(args.beat_pct) / 100.0)
    seed = _load_seed(args.seed_json)

    variants = []
    for idx, deltas in enumerate(itertools.product(args.deltas, repeat=len(args.features))):
        weights = dict(seed.weights)
        parts = []
        for feature, delta in zip(args.features, deltas):
            value = max(-float(args.weight_limit), min(float(args.weight_limit), float(weights.get(feature, 0.0) or 0.0) + float(delta)))
            weights[feature] = value
            parts.append(f'{feature}:{delta:+g}')
        variants.append(_variant(f'grid_{idx:07d}', weights, seed.bias))
    scored = _score(compiled, variants, float(args.start_balance), int(args.batch_size), sim_config)
    scored = [_decorate(r, active_pnl, target_pnl) for r in scored]
    summary = {
        'script': 'step2_feature_grid.py',
        'completed': bool(scored and scored[0]['beats_target']),
        'active': _decorate(active_row, active_pnl, target_pnl),
        'active_step2_pnl': active_pnl,
        'target_pnl': target_pnl,
        'beat_pct': float(args.beat_pct),
        'features': list(args.features),
        'deltas': list(args.deltas),
        'scored_total': len(scored),
        'leaderboard': scored[:100],
        'winners': [r for r in scored if r['beats_target']][:25],
        'step2_parity_contract_hash': step2_parity_contract.contract_hash(step2_parity_contract.contract(cfg)),
    }
    candidate_profile_schema.decorate_payload(summary, context={
        'script': 'step2_feature_grid.py',
        'start_balance': args.start_balance,
        'compiled_decision_tape': args.compiled_decision_tape,
        'active_step2_pnl': active_pnl,
        'target_pnl': target_pnl,
        'scored_total': len(scored),
    })
    (out_dir / 'summary.json').write_text(json.dumps(summary, indent=2, sort_keys=True), encoding='utf-8')
    print(json.dumps({
        'out': str(out_dir / 'summary.json'),
        'completed': summary['completed'],
        'best_variant': scored[0]['variant'],
        'best_pnl': scored[0]['step2_pnl'],
        'best_delta_pct': scored[0]['step2_delta_pct_vs_active'],
        'scored_total': len(scored),
        'target_pnl': round(target_pnl, 2),
    }), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
