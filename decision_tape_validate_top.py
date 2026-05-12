"""Validate top lab variants against decision tapes.

Input is a scoring lab JSON with variant weights. Output is a ranked decision
tape leaderboard. This is Step 2 in the tournament funnel.
"""
from __future__ import annotations

import argparse
import json
import os
import time

import numpy as np

import scoring_variant_lab as slow_lab
import active_engine_baseline
import decision_tape_compiled
import simulate_decision_tape
import tournament_safety
import variant_tournament_runner as tournament


HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_IN = os.path.join(
    HERE,
    'postmortem',
    'backtests',
    'scoring_variant_lab_massive_2026-04-06_2026-05-01_top10000.json',
)
DEFAULT_OUT = os.path.join(
    HERE,
    'postmortem',
    'backtests',
    'decision_tape_validation_2026-04-06_2026-05-01_top10000.json',
)


def _load_variants(path: str, limit: int | None) -> tuple[dict, list[slow_lab.Variant]]:
    with open(path, 'r', encoding='utf-8') as f:
        payload = json.load(f)
    rows = payload.get('results') or []
    if limit:
        rows = rows[:limit]
    variants = [
        slow_lab.Variant(
            row['variant'],
            dict(row.get('weights') or {}),
            float(row.get('bias') or 0.0),
        )
        for row in rows
    ]
    return payload, variants


def _compiled_tape_name(args) -> str:
    tickers = '-'.join(args.tickers)
    suffix = f'_{args.tape_profile_name}' if args.tape_profile_name else ''
    indicator_mode = getattr(args, 'indicator_mode', 'live') or 'live'
    return f"compiled_decision_tape_{args.feed}_{args.quote_mode}_{args.btc_mode}_{indicator_mode}_{tickers}{suffix}_{args.start}_{args.end}"


def _compiled_manifest_path(args) -> str:
    if args.compiled_tape_manifest:
        return args.compiled_tape_manifest
    return os.path.join(
        args.compiled_tape_dir,
        _compiled_tape_name(args),
        'manifest.json',
    )


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Decision-tape validate top lab variants.')
    ap.add_argument('--input', default=DEFAULT_IN)
    ap.add_argument('--out', default=DEFAULT_OUT)
    ap.add_argument('--start', default='2026-04-06')
    ap.add_argument('--end', default='2026-05-01')
    ap.add_argument('--train-start', default=None)
    ap.add_argument('--train-end', default=None)
    ap.add_argument('--test-start', default=None)
    ap.add_argument('--test-end', default=None)
    ap.add_argument('--tickers', nargs='+', default=tournament.replay.TICKERS)
    ap.add_argument('--feed', default='sip')
    ap.add_argument('--quote-mode', default='per-second')
    ap.add_argument('--btc-mode', default='bars')
    ap.add_argument('--indicator-mode', choices=['live', 'fast'], default='live')
    ap.add_argument('--tape-dir', default=simulate_decision_tape.DEFAULT_TAPE_DIR)
    ap.add_argument('--tape-profile-name', default='')
    ap.add_argument('--compiled-tape-dir', default=decision_tape_compiled.DEFAULT_OUT_DIR)
    ap.add_argument('--compiled-tape-manifest', default=None)
    ap.add_argument('--compiled-batch-size', type=int, default=512)
    ap.add_argument('--entry-gate-json', default=None)
    ap.add_argument('--entry-gate-mode', default=None)
    ap.add_argument('--entry-gate-threshold-pct', type=float, default=None)
    ap.add_argument('--active-baseline-include-gate', action='store_true',
                    help='Apply the active long-entry gate to the active-engine decision baseline.')
    ap.add_argument('--no-active-baseline', action='store_true')
    ap.add_argument('--beat-active-min-pnl-margin', type=float, default=0.0)
    ap.add_argument('--beat-active-require-win-rate', action='store_true')
    ap.add_argument('--start-balance', type=float, default=100000.0)
    ap.add_argument('--limit', type=int, default=0)
    ap.add_argument('--progress-every', type=int, default=500)
    ap.add_argument('--registry-path', default=tournament_safety.DEFAULT_REGISTRY)
    ap.add_argument('--no-registry', action='store_true')
    ap.add_argument('--disable-compiled-tape', action='store_true',
                    help='Force the exact Python decision-tape simulator even when the compiled accelerator is compatible.')
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    args.tickers = [t.upper() for t in args.tickers]
    lab_payload, variants = _load_variants(args.input, args.limit or None)
    started = time.perf_counter()
    rows_by_day = None
    compiled_tape = None
    entry_gate = simulate_decision_tape._load_gate(
        getattr(args, 'entry_gate_json', None),
        getattr(args, 'entry_gate_mode', None),
        getattr(args, 'entry_gate_threshold_pct', None),
    )
    compiled_allowed = (
        not args.disable_compiled_tape
        and args.train_start is None
        and args.train_end is None
        and args.test_start is None
        and args.test_end is None
        and decision_tape_compiled.compatible_gate(entry_gate)
    )
    if compiled_allowed:
        compiled_path = _compiled_manifest_path(args)
        if os.path.exists(compiled_path):
            compiled_tape = decision_tape_compiled.load_compiled(compiled_path)
        else:
            rows_by_day = tournament._load_decision_rows(args)
            compiled_tape = decision_tape_compiled.compile_rows(rows_by_day, args.tickers)
    if rows_by_day is None and (compiled_tape is None or not args.no_active_baseline):
        rows_by_day = tournament._load_decision_rows(args)
    load_elapsed = round(time.perf_counter() - started, 3)
    active_decision = None
    if not args.no_active_baseline:
        active_decision = active_engine_baseline.decision_baseline(
            args,
            rows_by_day,
            include_active_gate=args.active_baseline_include_gate,
        )
    results = []
    simulated_by_decision_hash = {}
    next_progress = max(1, args.progress_every)
    if compiled_tape is not None:
        batch_size = max(1, int(args.compiled_batch_size or 512))
        for start_idx in range(0, len(variants), batch_size):
            batch = variants[start_idx:start_idx + batch_size]
            sides, hashes = decision_tape_compiled.side_matrix_and_hashes(compiled_tape, batch)
            unique_positions = []
            batch_rows = [None] * len(batch)
            unique_variants = []
            unique_sides = []
            for pos, (variant, decision_hash) in enumerate(zip(batch, hashes)):
                cached = simulated_by_decision_hash.get(decision_hash)
                if cached:
                    batch_rows[pos] = {
                        **cached,
                        'variant': variant.name,
                        'weights': dict(variant.weights),
                        'bias': float(variant.bias or 0.0),
                        'deduped_from_variant': cached.get('variant'),
                    }
                else:
                    unique_positions.append(pos)
                    unique_variants.append(variant)
                    unique_sides.append(sides[pos])
            if unique_variants:
                sim_rows = decision_tape_compiled.simulate_side_matrix(
                    compiled_tape,
                    unique_variants,
                    np.asarray(unique_sides, dtype=np.int8),
                    args.start_balance,
                    entry_gate,
                )
                for pos, row in zip(unique_positions, sim_rows or []):
                    row['decision_train'] = dict(row['decision_full'])
                    row['decision_test'] = None
                    row['deduped_from_variant'] = None
                    simulated_by_decision_hash[hashes[pos]] = dict(row)
                    batch_rows[pos] = row
            for pos, row in enumerate(batch_rows):
                idx = start_idx + pos + 1
                row['decision_vector_hash'] = hashes[pos]
                lab_row = (lab_payload.get('results') or [])[idx - 1]
                row['lab_rank'] = lab_row.get('global_lab_rank') or lab_row.get('lab_rank') or idx
                row['lab_pnl'] = lab_row.get('pnl')
                row['lab_win_rate_pct'] = lab_row.get('win_rate_pct')
                row['lab_flipped'] = lab_row.get('flipped')
                results.append(row)
            processed = min(start_idx + len(batch), len(variants))
            if processed >= next_progress:
                elapsed = time.perf_counter() - started
                best = max(results, key=lambda r: r['decision_full']['pnl'])
                print(json.dumps({
                    'event': 'progress',
                    'processed': processed,
                    'total': len(variants),
                    'elapsed_seconds': round(elapsed, 2),
                    'variants_per_sec': round(processed / elapsed, 2) if elapsed else None,
                    'eta_seconds': round((len(variants) - processed) / (processed / elapsed), 2) if processed and elapsed else None,
                    'unique_decision_vectors': len(simulated_by_decision_hash),
                    'current_best': {
                        'variant': best['variant'],
                        'decision_pnl': best['decision_full']['pnl'],
                        'lab_pnl': best.get('lab_pnl'),
                    },
                }, sort_keys=True), flush=True)
                while processed >= next_progress:
                    next_progress += max(1, args.progress_every)
    else:
        for idx, variant in enumerate(variants, 1):
            decision_hash = tournament.decision_vector_hash(rows_by_day, variant, args.start, args.end)
            cached = simulated_by_decision_hash.get(decision_hash)
            if cached:
                row = {
                    **cached,
                    'variant': variant.name,
                    'weights': dict(variant.weights),
                    'bias': float(variant.bias or 0.0),
                    'deduped_from_variant': cached.get('variant'),
                }
            else:
                row = tournament._decision_stage(args, [variant], rows_by_day)[0]
                row['deduped_from_variant'] = None
                simulated_by_decision_hash[decision_hash] = dict(row)
            row['decision_vector_hash'] = decision_hash
            lab_row = (lab_payload.get('results') or [])[idx - 1]
            row['lab_rank'] = lab_row.get('global_lab_rank') or lab_row.get('lab_rank') or idx
            row['lab_pnl'] = lab_row.get('pnl')
            row['lab_win_rate_pct'] = lab_row.get('win_rate_pct')
            row['lab_flipped'] = lab_row.get('flipped')
            results.append(row)
            if idx >= next_progress:
                elapsed = time.perf_counter() - started
                best = max(results, key=lambda r: r['decision_full']['pnl'])
                print(json.dumps({
                    'event': 'progress',
                    'processed': idx,
                    'total': len(variants),
                    'elapsed_seconds': round(elapsed, 2),
                    'variants_per_sec': round(idx / elapsed, 2) if elapsed else None,
                    'eta_seconds': round((len(variants) - idx) / (idx / elapsed), 2) if idx and elapsed else None,
                    'unique_decision_vectors': len(simulated_by_decision_hash),
                    'current_best': {
                        'variant': best['variant'],
                        'decision_pnl': best['decision_full']['pnl'],
                        'lab_pnl': best.get('lab_pnl'),
                    },
                }, sort_keys=True), flush=True)
                next_progress += max(1, args.progress_every)
    lab_meta_by_variant = {
        row.get('variant'): row
        for row in (lab_payload.get('results') or [])[:len(variants)]
    }
    results = tournament_safety.enrich_decision_results(results)
    for row in results:
        lab_row = lab_meta_by_variant.get(row.get('variant')) or {}
        row['lab_rank'] = lab_row.get('global_lab_rank') or lab_row.get('lab_rank') or row.get('lab_rank')
        row['lab_pnl'] = lab_row.get('pnl', row.get('lab_pnl'))
        row['lab_win_rate_pct'] = lab_row.get('win_rate_pct', row.get('lab_win_rate_pct'))
        row['lab_flipped'] = lab_row.get('flipped', row.get('lab_flipped'))
    active_engine_baseline.annotate_decision_rows(
        results,
        active_decision,
        args.beat_active_min_pnl_margin,
        args.beat_active_require_win_rate,
    )
    results.sort(key=lambda r: r['decision_full']['pnl'], reverse=True)
    for rank, row in enumerate(results, 1):
        row['decision_rank'] = rank
    elapsed = round(time.perf_counter() - started, 3)
    payload = {
        'schema_version': 2,
        'source_lab': args.input,
        'fingerprints': tournament_safety.run_fingerprint(args, extra_paths=[args.input]),
        'start': args.start,
        'end': args.end,
        'variants': len(variants),
        'unique_decision_vectors': len(simulated_by_decision_hash),
        'deduped_variants': len(variants) - len(simulated_by_decision_hash),
        'load_elapsed_seconds': load_elapsed,
        'elapsed_seconds': elapsed,
        'active_engine_baseline': active_decision,
        'compiled_decision_tape': {
            'attempted': bool(compiled_allowed),
            'used': bool(compiled_tape is not None),
            'numba_available': bool(decision_tape_compiled.COMPILED_AVAILABLE),
            'compatible_gate': bool(decision_tape_compiled.compatible_gate(entry_gate)),
            'rows': int((compiled_tape or {}).get('rows') or 0),
            'manifest': ((compiled_tape or {}).get('manifest') or {}).get('manifest_path') or _compiled_manifest_path(args),
            'batch_size': int(args.compiled_batch_size or 512),
            'note': 'Compiled tape is used only for full-window Step 2 without custom train/test splits and compatible entry gates.',
        },
        'method_note': (
            'Decision-tape validation uses captured decision points and precomputed LONG/SHORT paths. '
            'It is closer than the scoring lab but still not a full engine replay.'
        ),
        'results': results,
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    registry_path = None
    if not args.no_registry:
        registry_path = tournament_safety.append_registry(
            tournament_safety.registry_rows({
                'run_name': os.path.splitext(os.path.basename(args.out))[0],
                'fingerprints': payload.get('fingerprints'),
                'decision_results': results,
            }, args.out),
            args.registry_path,
        )
    print(json.dumps({
        'event': 'done',
        'out': args.out,
        'registry': registry_path,
        'variants': len(variants),
        'elapsed_seconds': elapsed,
        'top10': [
            {
                'decision_rank': row['decision_rank'],
                'lab_rank': row.get('lab_rank'),
                'variant': row['variant'],
                'decision_pnl': row['decision_full']['pnl'],
                'decision_pnl_delta_vs_active': row.get('decision_pnl_delta_vs_active'),
                'beats_active_decision': row.get('beats_active_decision'),
                'decision_trades': row['decision_full']['trades'],
                'decision_win_rate_pct': row['decision_full']['win_rate_pct'],
                'lab_pnl': row.get('lab_pnl'),
            }
            for row in results[:10]
        ],
    }, indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == '__main__':
    import canonical_command_registry as _canonical_commands
    _canonical_commands.enforce_direct_script_allowed(__file__)
    raise SystemExit(main())
