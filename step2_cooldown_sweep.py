"""Sweep same-ticker re-entry cooldown for Step 2 leaders."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import active_engine_baseline
import candidate_profile_schema
import decision_tape_compiled
import scoring_variant_lab as lab
import step2_parity_contract


HERE = Path(__file__).resolve().parent
DEFAULT_COMPILED = HERE / 'postmortem' / 'backtests' / 'compiled_decision_tapes' / (
    'compiled_step2_current_live_CLSK-MARA-RIOT_2026-04-06_2026-05-08'
) / 'manifest.json'
DEFAULT_OUT = HERE / 'postmortem' / 'backtests' / 'step2_adaptive_hunter'


def _load_config() -> dict[str, Any]:
    with (HERE / 'trading_config.json').open('r', encoding='utf-8') as f:
        return json.load(f)


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
    return lab.Variant(str(best.get('variant') or 'seed'), dict(best['weights']), float(best.get('bias') or 0.0))


def _summarize(row: dict, active_pnl: float, target_pnl: float, cooldown: int) -> dict:
    full = row.get('decision_full') or {}
    pnl = float(full.get('pnl') or 0.0)
    return {
        'variant': row.get('variant'),
        'weights': row.get('weights') or {},
        'bias': float(row.get('bias') or 0.0),
        'same_ticker_reentry_cooldown_sec': int(cooldown),
        'step2_pnl': round(pnl, 2),
        'step2_trades': int(full.get('trades') or 0),
        'step2_win_rate_pct': full.get('win_rate_pct'),
        'by_day': full.get('by_day') or {},
        'by_ticker': full.get('by_ticker') or {},
        'skipped': full.get('skipped') or {},
        'step2_delta_vs_active': round(pnl - active_pnl, 4),
        'step2_delta_pct_vs_active': round((pnl / active_pnl - 1.0) * 100.0, 4) if active_pnl else None,
        'beats_target': pnl >= target_pnl,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description='Step 2 cooldown sweep.')
    ap.add_argument('--compiled-decision-tape', default=str(DEFAULT_COMPILED))
    ap.add_argument('--out-dir', default=str(DEFAULT_OUT))
    ap.add_argument('--name', default='cooldown_sweep')
    ap.add_argument('--seed-json', required=True)
    ap.add_argument('--cooldowns', nargs='+', type=int, default=[0, 1, 2, 3, 5, 8, 10, 15])
    ap.add_argument('--start-balance', type=float, default=100000.0)
    ap.add_argument('--beat-pct', type=float, default=25.0)
    args = ap.parse_args()

    cfg = _load_config()
    base_sim = step2_parity_contract.sim_config(cfg)
    compiled = decision_tape_compiled.load_compiled(args.compiled_decision_tape, mmap=True)
    out_dir = Path(args.out_dir) / args.name
    out_dir.mkdir(parents=True, exist_ok=True)

    active = active_engine_baseline.active_variant()
    seed = _load_seed(args.seed_json)
    active_rows = []
    candidate_rows = []
    baseline_sim = dict(base_sim)
    baseline_cooldown = int(base_sim.get('same_ticker_reentry_cooldown_sec') or 0)
    active_baseline = decision_tape_compiled.simulate_variants(
        compiled, [active], float(args.start_balance), gate=None, sim_config=baseline_sim
    )[0]
    active_pnl = float((active_baseline.get('decision_full') or {}).get('pnl') or 0.0)
    target_pnl = active_pnl * (1.0 + float(args.beat_pct) / 100.0)

    for cooldown in args.cooldowns:
        sim = dict(base_sim)
        sim['same_ticker_reentry_cooldown_sec'] = int(cooldown)
        rows = decision_tape_compiled.simulate_variants(
            compiled, [active, seed], float(args.start_balance), gate=None, sim_config=sim
        )
        active_rows.append(_summarize(rows[0], active_pnl, target_pnl, cooldown))
        candidate_rows.append(_summarize(rows[1], active_pnl, target_pnl, cooldown))

    candidate_rows.sort(key=lambda r: float(r['step2_pnl']), reverse=True)
    summary = {
        'script': 'step2_cooldown_sweep.py',
        'completed': bool(candidate_rows and candidate_rows[0]['beats_target']),
        'active_baseline_cooldown_sec': baseline_cooldown,
        'active_step2_pnl': round(active_pnl, 2),
        'target_pnl': target_pnl,
        'beat_pct': float(args.beat_pct),
        'active_by_cooldown': active_rows,
        'candidate_by_cooldown': candidate_rows,
        'leaderboard': candidate_rows,
        'winners': [r for r in candidate_rows if r['beats_target']],
        'step2_parity_contract': step2_parity_contract.contract(cfg),
        'step2_parity_contract_hash': step2_parity_contract.contract_hash(step2_parity_contract.contract(cfg)),
    }
    candidate_profile_schema.decorate_payload(summary, context={
        'script': 'step2_cooldown_sweep.py',
        'start_balance': args.start_balance,
        'compiled_decision_tape': args.compiled_decision_tape,
        'active_step2_pnl': active_pnl,
        'target_pnl': target_pnl,
        'scored_total': len(candidate_rows),
    })
    (out_dir / 'summary.json').write_text(json.dumps(summary, indent=2, sort_keys=True), encoding='utf-8')
    best = candidate_rows[0]
    print(json.dumps({
        'out': str(out_dir / 'summary.json'),
        'completed': summary['completed'],
        'best_variant': best['variant'],
        'best_pnl': best['step2_pnl'],
        'best_delta_pct': best['step2_delta_pct_vs_active'],
        'best_cooldown_sec': best['same_ticker_reentry_cooldown_sec'],
        'best_trades': best['step2_trades'],
        'target_pnl': round(target_pnl, 2),
    }), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
