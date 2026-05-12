"""Systematic two-weight sweep around a Step 2 leader."""
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


def _load_seed(path: str) -> lab.Variant:
    data = json.loads(Path(path).read_text(encoding='utf-8'))
    best = None
    for row in _walk_rows(data):
        if not isinstance(row.get('weights'), dict):
            continue
        if best is None or float(row.get('step2_pnl') or -1e18) > float(best.get('step2_pnl') or -1e18):
            best = row
    if best is None:
        raise RuntimeError(f'no weighted seed rows in {path}')
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
    ap = argparse.ArgumentParser(description='Systematic Step 2 pair sweep.')
    ap.add_argument('--compiled-decision-tape', default=str(DEFAULT_COMPILED))
    ap.add_argument('--out-dir', default=str(DEFAULT_OUT))
    ap.add_argument('--name', default='pair_sweep')
    ap.add_argument('--seed-json', required=True)
    ap.add_argument('--iterations', type=int, default=5)
    ap.add_argument('--batch-size', type=int, default=500)
    ap.add_argument('--start-balance', type=float, default=100000.0)
    ap.add_argument('--beat-pct', type=float, default=25.0)
    ap.add_argument('--weight-limit', type=float, default=24.0)
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

    current = _load_seed(args.seed_json)
    current_row = _decorate(_score(compiled, [current], float(args.start_balance), int(args.batch_size), sim_config)[0],
                            active_pnl, target_pnl)
    leaders = [current_row]
    deltas = [-2.0, -1.25, -0.75, -0.4, -0.2, 0.2, 0.4, 0.75, 1.25, 2.0]
    seen = set()
    scored_total = 0
    for iteration in range(1, int(args.iterations) + 1):
        variants: list[lab.Variant] = []
        base_weights = dict(current.weights)
        for left, right in itertools.combinations(fast.FEATURE_NAMES, 2):
            for dl in deltas:
                for dr in deltas:
                    weights = dict(base_weights)
                    lv = max(-float(args.weight_limit), min(float(args.weight_limit), float(weights.get(left, 0.0) or 0.0) + dl))
                    rv = max(-float(args.weight_limit), min(float(args.weight_limit), float(weights.get(right, 0.0) or 0.0) + dr))
                    weights[left] = lv
                    weights[right] = rv
                    variant = _variant(f'pairfull_i{iteration:02d}_{left}_{dl:+.2f}_{right}_{dr:+.2f}',
                                       weights, current.bias)
                    key = tournament_safety.model_id(variant.name, variant.weights, variant.bias)
                    if key in seen:
                        continue
                    seen.add(key)
                    variants.append(variant)
        scored = _score(compiled, variants, float(args.start_balance), int(args.batch_size), sim_config)
        scored = [_decorate(r, active_pnl, target_pnl) for r in scored]
        scored_total += len(scored)
        best = scored[0]
        leaders.extend(scored[:50])
        leaders.sort(key=lambda r: float(r['step2_pnl']), reverse=True)
        if float(best['step2_pnl']) > float(current_row['step2_pnl']) + 0.004:
            current = _variant(str(best['variant']), dict(best['weights']), float(best.get('bias') or 0.0))
            current_row = best
        print(json.dumps({
            'event': 'iteration_done',
            'iteration': iteration,
            'scored_total': scored_total,
            'batch_best_pnl': best['step2_pnl'],
            'global_best_pnl': leaders[0]['step2_pnl'],
            'global_delta_pct': leaders[0]['step2_delta_pct_vs_active'],
            'target_pnl': round(target_pnl, 2),
            'beats_target': leaders[0]['beats_target'],
        }), flush=True)
        if leaders[0]['beats_target']:
            break

    leaders.sort(key=lambda r: float(r['step2_pnl']), reverse=True)
    summary = {
        'script': 'step2_pair_sweep.py',
        'completed': bool(leaders and leaders[0]['beats_target']),
        'active': _decorate(active_row, active_pnl, target_pnl),
        'active_step2_pnl': active_pnl,
        'target_pnl': target_pnl,
        'beat_pct': float(args.beat_pct),
        'scored_total': scored_total,
        'leaderboard': leaders[:100],
        'winners': [r for r in leaders if r['beats_target']][:25],
        'step2_parity_contract': step2_parity_contract.contract(cfg),
        'step2_parity_contract_hash': step2_parity_contract.contract_hash(step2_parity_contract.contract(cfg)),
    }
    candidate_profile_schema.decorate_payload(summary, context={
        'script': 'step2_pair_sweep.py',
        'start_balance': args.start_balance,
        'compiled_decision_tape': args.compiled_decision_tape,
        'active_step2_pnl': active_pnl,
        'target_pnl': target_pnl,
        'scored_total': scored_total,
    })
    (out_dir / 'summary.json').write_text(json.dumps(summary, indent=2, sort_keys=True), encoding='utf-8')
    print(json.dumps({
        'out': str(out_dir / 'summary.json'),
        'completed': summary['completed'],
        'best_variant': leaders[0]['variant'],
        'best_pnl': leaders[0]['step2_pnl'],
        'best_delta_pct': leaders[0]['step2_delta_pct_vs_active'],
        'scored_total': scored_total,
        'target_pnl': round(target_pnl, 2),
    }), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
