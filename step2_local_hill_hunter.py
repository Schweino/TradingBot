"""Local hill-climbing Step 2 hunter.

This is intentionally less chaotic than the adaptive hunter.  Once a strong
shape exists, most candidates perturb only one to four weights and preserve the
rest of the scoring profile.
"""
from __future__ import annotations

import argparse
import json
import random
import time
from pathlib import Path
from typing import Any

import active_engine_baseline
import candidate_profile_schema
import decision_tape_compiled
import scoring_variant_lab as lab
import scoring_variant_lab_fast as fast
import step2_manifest_resolver
import step2_parity_contract
import step2_score_cache
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


def _score(compiled: dict, variants: list[lab.Variant], start_balance: float, batch_size: int,
           sim_config: dict[str, float | int], *, cache_db: str = str(step2_score_cache.DEFAULT_CACHE_DB),
           use_score_cache: bool = True) -> list[dict]:
    out: list[dict] = []
    for start in range(0, len(variants), batch_size):
        batch = variants[start:start + batch_size]
        if use_score_cache:
            rows = step2_score_cache.score_variants_cached(
                compiled,
                batch,
                start_balance,
                gate=None,
                sim_config=sim_config,
                cache_db=cache_db,
            )
        else:
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
                'score_cache': row.get('score_cache') or {},
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


def _mutate_local(seed: dict, rng: random.Random, idx: int, generation: int, weight_limit: float,
                  base_scale: float) -> lab.Variant:
    weights = dict(seed.get('weights') or {})
    feature_count = rng.choices([1, 2, 3, 4, 6, 9], weights=[34, 28, 18, 12, 6, 2], k=1)[0]
    keys = list(fast.FEATURE_NAMES)
    chosen = rng.sample(keys, feature_count)
    scale = base_scale * rng.choice([0.25, 0.5, 0.8, 1.0, 1.4, 2.0])
    for key in chosen:
        cur = float(weights.get(key, 0.0) or 0.0)
        if rng.random() < 0.08:
            cur = rng.choice([-1.0, 1.0]) * rng.uniform(0.02, min(4.0, weight_limit))
        else:
            cur += rng.gauss(0.0, scale)
        cur = max(-weight_limit, min(weight_limit, cur))
        if abs(cur) < 1e-8:
            weights.pop(key, None)
        else:
            weights[key] = cur
    bias = float(seed.get('bias') or 0.0)
    if rng.random() < 0.35:
        bias += rng.gauss(0.0, scale / 5.0)
    return _variant(f'hill_g{generation:04d}_{idx:06d}_{str(seed.get("variant") or "seed")[:28]}', weights, bias)


def _crossover(a: dict, b: dict, rng: random.Random, idx: int, generation: int, weight_limit: float) -> lab.Variant:
    weights = {}
    for key in fast.FEATURE_NAMES:
        av = float((a.get('weights') or {}).get(key, 0.0) or 0.0)
        bv = float((b.get('weights') or {}).get(key, 0.0) or 0.0)
        if rng.random() < 0.45:
            value = av
        elif rng.random() < 0.9:
            value = bv
        else:
            value = (av + bv) / 2.0
        value = max(-weight_limit, min(weight_limit, value))
        if abs(value) > 1e-10:
            weights[key] = value
    bias = float(a.get('bias') or 0.0) if rng.random() < 0.5 else float(b.get('bias') or 0.0)
    return _variant(f'hill_cross_g{generation:04d}_{idx:06d}', weights, bias)


def main() -> int:
    ap = argparse.ArgumentParser(description='Local Step 2 hill-climb hunter.')
    ap.add_argument('--compiled-decision-tape', default=str(DEFAULT_COMPILED))
    ap.add_argument('--no-cache-resolver', dest='cache_resolver', action='store_false',
                    help='Diagnostics only: use --compiled-decision-tape exactly as provided.')
    ap.set_defaults(cache_resolver=True)
    ap.add_argument('--cache-start', default=step2_manifest_resolver.DEFAULT_START)
    ap.add_argument('--cache-end', default=step2_manifest_resolver.DEFAULT_END)
    ap.add_argument('--cache-tickers', nargs='*', default=step2_manifest_resolver.DEFAULT_TICKERS)
    ap.add_argument('--out-dir', default=str(DEFAULT_OUT))
    ap.add_argument('--name', default='local_hill_hunter')
    ap.add_argument('--seed-json', action='append', default=[])
    ap.add_argument('--seed-limit', type=int, default=600)
    ap.add_argument('--batch-size', type=int, default=500)
    ap.add_argument('--generations', type=int, default=400)
    ap.add_argument('--start-balance', type=float, default=100000.0)
    ap.add_argument('--beat-pct', type=float, default=25.0)
    ap.add_argument('--weight-limit', type=float, default=14.0)
    ap.add_argument('--seed', type=int, default=20260508)
    ap.add_argument('--score-cache-db', default=str(step2_score_cache.DEFAULT_CACHE_DB))
    ap.add_argument('--no-score-cache', dest='use_score_cache', action='store_false')
    ap.set_defaults(use_score_cache=True)
    args = ap.parse_args()

    started = time.perf_counter()
    rng = random.Random(int(args.seed))
    cfg = _load_config()
    sim_config = step2_parity_contract.sim_config(cfg)
    out_dir = Path(args.out_dir) / args.name
    out_dir.mkdir(parents=True, exist_ok=True)
    cache_resolution = {'ok': True, 'source': 'cache_resolver_disabled', 'manifest_path': args.compiled_decision_tape}
    if args.cache_resolver:
        explicit = ''
        try:
            if Path(args.compiled_decision_tape).resolve() != DEFAULT_COMPILED.resolve():
                explicit = args.compiled_decision_tape
        except Exception:
            explicit = args.compiled_decision_tape
        payload = step2_manifest_resolver.resolve_best_manifest(
            compiled_manifest=explicit or None,
            start=args.cache_start,
            end=args.cache_end,
            tickers=args.cache_tickers,
            write_certification=True,
            write_receipt=True,
            label=args.name,
        )
        cache_resolution = step2_manifest_resolver.compact_resolution(payload)
        step2_manifest_resolver.write_json(out_dir / 'cache_manifest_resolution.json', payload)
        if not payload.get('ok'):
            summary = {
                'script': 'step2_local_hill_hunter.py',
                'completed': False,
                'promotable': False,
                'non_promotable_reason': 'step2_cache_resolution_failed',
                'compiled_decision_tape': args.compiled_decision_tape,
                'cache_manifest_resolution': cache_resolution,
            }
            step2_manifest_resolver.write_json(out_dir / 'summary.json', summary)
            print(json.dumps(summary, indent=2, sort_keys=True), flush=True)
            return 4
        args.compiled_decision_tape = str(payload['manifest_path'])
    compiled = decision_tape_compiled.load_compiled(args.compiled_decision_tape, mmap=True)
    checkpoint = out_dir / 'checkpoint.json'

    active = active_engine_baseline.active_variant()
    active_row = _score(compiled, [active], float(args.start_balance), int(args.batch_size), sim_config,
                        cache_db=args.score_cache_db, use_score_cache=args.use_score_cache)[0]
    active_pnl = float(active_row['step2_pnl'])
    target_pnl = active_pnl * (1.0 + float(args.beat_pct) / 100.0)

    seed_variants = _load_seeds(args.seed_json, int(args.seed_limit))
    seed_rows = _score(compiled, seed_variants, float(args.start_balance), int(args.batch_size), sim_config,
                       cache_db=args.score_cache_db, use_score_cache=args.use_score_cache)
    seed_rows = [_decorate(r, active_pnl, target_pnl) for r in seed_rows]
    elites = sorted(seed_rows, key=lambda r: float(r['step2_pnl']), reverse=True)[:250]
    seen = {
        tournament_safety.model_id(str(r['variant']), r.get('weights') or {}, float(r.get('bias') or 0.0))
        for r in elites
    }
    print(json.dumps({
        'stage': 'seed_rescore',
        'best_variant': elites[0]['variant'],
        'best_pnl': elites[0]['step2_pnl'],
        'delta_pct': elites[0]['step2_delta_pct_vs_active'],
        'target_pnl': round(target_pnl, 2),
    }), flush=True)

    winners: list[dict] = [r for r in elites if float(r['step2_pnl']) >= target_pnl]
    scored_total = 0
    for generation in range(1, int(args.generations) + 1):
        base_scale = max(0.015, 0.45 * (0.992 ** generation))
        variants: list[lab.Variant] = []
        while len(variants) < int(args.batch_size):
            if rng.random() < 0.18 and len(elites) >= 2:
                seed_a = rng.choice(elites[:min(40, len(elites))])
                seed_b = rng.choice(elites[:min(80, len(elites))])
                variant = _crossover(seed_a, seed_b, rng, generation * 1_000_000 + len(variants), generation, float(args.weight_limit))
            else:
                pool = elites[:min(len(elites), rng.choice([10, 20, 40, 80, 120]))]
                seed = rng.choice(pool)
                variant = _mutate_local(seed, rng, generation * 1_000_000 + len(variants), generation,
                                        float(args.weight_limit), base_scale)
            key = tournament_safety.model_id(variant.name, variant.weights, variant.bias)
            if key in seen:
                continue
            seen.add(key)
            variants.append(variant)
        scored = _score(compiled, variants, float(args.start_balance), int(args.batch_size), sim_config,
                        cache_db=args.score_cache_db, use_score_cache=args.use_score_cache)
        scored = [_decorate(r, active_pnl, target_pnl) for r in scored]
        scored_total += len(scored)
        elites = sorted(elites + scored, key=lambda r: float(r['step2_pnl']), reverse=True)[:300]
        winners = [r for r in elites if float(r['step2_pnl']) >= target_pnl]
        if generation == 1 or scored[0]['step2_pnl'] >= elites[0]['step2_pnl'] or generation % 10 == 0:
            print(json.dumps({
                'event': 'generation_done',
                'generation': generation,
                'scored_total': scored_total,
                'batch_best_pnl': scored[0]['step2_pnl'],
                'global_best_pnl': elites[0]['step2_pnl'],
                'global_delta_pct': elites[0]['step2_delta_pct_vs_active'],
                'winners': len(winners),
                'target_pnl': round(target_pnl, 2),
            }), flush=True)
        summary = {
            'script': 'step2_local_hill_hunter.py',
            'completed': bool(winners),
            'active': _decorate(active_row, active_pnl, target_pnl),
            'active_step2_pnl': active_pnl,
            'target_pnl': target_pnl,
            'beat_pct': float(args.beat_pct),
            'completed_generations': generation,
            'scored_total': scored_total,
            'elapsed_sec': round(time.perf_counter() - started, 3),
            'leaderboard': elites[:100],
            'winners': winners[:25],
            'step2_parity_contract': step2_parity_contract.contract(cfg),
            'step2_parity_contract_hash': step2_parity_contract.contract_hash(step2_parity_contract.contract(cfg)),
            'cache_manifest_resolution': cache_resolution,
        }
        candidate_profile_schema.decorate_payload(summary, context={
            'script': 'step2_local_hill_hunter.py',
            'start_balance': args.start_balance,
            'compiled_decision_tape': args.compiled_decision_tape,
            'active_step2_pnl': active_pnl,
            'target_pnl': target_pnl,
            'scored_total': scored_total,
        })
        checkpoint.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding='utf-8')
        if winners:
            break

    final = json.loads(checkpoint.read_text(encoding='utf-8'))
    candidate_profile_schema.decorate_payload(final, context={
        'script': 'step2_local_hill_hunter.py',
        'start_balance': args.start_balance,
        'compiled_decision_tape': args.compiled_decision_tape,
        'active_step2_pnl': final.get('active_step2_pnl'),
        'target_pnl': final.get('target_pnl'),
        'scored_total': final.get('scored_total'),
    })
    (out_dir / 'summary.json').write_text(json.dumps(final, indent=2, sort_keys=True), encoding='utf-8')
    print(json.dumps({
        'out': str(out_dir / 'summary.json'),
        'completed': final['completed'],
        'best_variant': final['leaderboard'][0]['variant'],
        'best_pnl': final['leaderboard'][0]['step2_pnl'],
        'best_delta_pct': final['leaderboard'][0]['step2_delta_pct_vs_active'],
        'scored_total': final['scored_total'],
        'target_pnl': round(target_pnl, 2),
        'elapsed_sec': final['elapsed_sec'],
    }), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
