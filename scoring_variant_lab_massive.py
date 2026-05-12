"""Massive streaming scoring-lab run.

This is the Step-1 screen for very large variant spaces. It keeps only the top
N results in memory while streaming variants in batches.

Important: this is still the hypothetical lab metric. It uses the same
mechanical side-flip P/L proxy as scoring_variant_lab_fast.py, not full replay.
"""
from __future__ import annotations

from output_paths import output_path

import argparse
import heapq
import itertools
import json
import os
import time
from collections import defaultdict
from typing import Iterable

import numpy as np

import scoring_variant_lab as slow
import scoring_variant_lab_fast as fast
import tournament_safety


HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CSV = output_path('postmortem', 'backtests', 'engine_replay_2026-04-06_2026-05-01_trades.csv')
DEFAULT_OUT = output_path('postmortem', 'backtests', 'scoring_variant_lab_massive_top10000.json')


def _signature(weights: dict, bias: float = 0.0) -> tuple:
    return (round(float(bias or 0.0), 8), tuple(sorted((k, round(float(v or 0.0), 8)) for k, v in weights.items() if float(v or 0.0) != 0.0)))


def _aggregate_rows(rows: list[dict]):
    """Aggregate rows with identical model features and original side.

    For a given variant, all rows in one bucket choose the same side. That lets
    us score millions of variants against hundreds of buckets instead of
    thousands of individual trades.
    """
    buckets: dict[tuple, dict] = {}
    for row in rows:
        feats = slow.features(row)
        values = tuple(float(feats.get(name, 0.0) or 0.0) for name in fast.FEATURE_NAMES)
        original = 1 if str(row.get('side')).upper() == 'LONG' else -1
        key = (values, original)
        pnl = slow._num(row.get('pnl'))
        bucket = buckets.setdefault(key, {
            'features': values,
            'original_side': original,
            'count': 0,
            'pnl_sum': 0.0,
            'win_count': 0,
            'loss_count': 0,
            'flipped_win_count': 0,
            'flipped_loss_count': 0,
        })
        bucket['count'] += 1
        bucket['pnl_sum'] += pnl
        bucket['win_count'] += int(pnl > 0)
        bucket['loss_count'] += int(pnl < 0)
        bucket['flipped_win_count'] += int(-pnl > 0)
        bucket['flipped_loss_count'] += int(-pnl < 0)

    ordered = list(buckets.values())
    features = np.array([b['features'] for b in ordered], dtype=np.float64)
    original_side = np.array([b['original_side'] for b in ordered], dtype=np.int8)
    pnl_sum = np.array([b['pnl_sum'] for b in ordered], dtype=np.float64)
    count = np.array([b['count'] for b in ordered], dtype=np.int64)
    win_count = np.array([b['win_count'] for b in ordered], dtype=np.int64)
    loss_count = np.array([b['loss_count'] for b in ordered], dtype=np.int64)
    flipped_win_count = np.array([b['flipped_win_count'] for b in ordered], dtype=np.int64)
    flipped_loss_count = np.array([b['flipped_loss_count'] for b in ordered], dtype=np.int64)
    return {
        'features': features,
        'original_side': original_side,
        'pnl_sum': pnl_sum,
        'count': count,
        'win_count': win_count,
        'loss_count': loss_count,
        'flipped_win_count': flipped_win_count,
        'flipped_loss_count': flipped_loss_count,
        'trades': int(count.sum()),
        'buckets': len(ordered),
    }


def _variant_matrix(variants: list[slow.Variant]):
    weights = np.zeros((len(variants), len(fast.FEATURE_NAMES)), dtype=np.float64)
    bias = np.zeros(len(variants), dtype=np.float64)
    for i, variant in enumerate(variants):
        for j, name in enumerate(fast.FEATURE_NAMES):
            weights[i, j] = float(variant.weights.get(name, 0.0) or 0.0)
        bias[i] = float(variant.bias or 0.0)
    return weights, bias


def _score_batch(agg: dict, variants: list[slow.Variant], starting_balance: float) -> list[dict]:
    weights, bias = _variant_matrix(variants)
    scores = agg['features'] @ weights.T
    scores += bias.reshape(1, -1)
    chosen_side = np.where(scores > 0, 1, np.where(scores < 0, -1, agg['original_side'].reshape(-1, 1)))
    flipped = chosen_side != agg['original_side'].reshape(-1, 1)

    pnl = np.where(flipped, -agg['pnl_sum'].reshape(-1, 1), agg['pnl_sum'].reshape(-1, 1)).sum(axis=0)
    wins = np.where(flipped, agg['flipped_win_count'].reshape(-1, 1), agg['win_count'].reshape(-1, 1)).sum(axis=0)
    losses = np.where(flipped, agg['flipped_loss_count'].reshape(-1, 1), agg['loss_count'].reshape(-1, 1)).sum(axis=0)
    flipped_count = np.where(flipped, agg['count'].reshape(-1, 1), 0).sum(axis=0)
    trades = agg['trades']

    out = []
    for i, variant in enumerate(variants):
        pnl_i = float(pnl[i])
        out.append({
            'variant': variant.name,
            'trades': trades,
            'wins': int(wins[i]),
            'losses': int(losses[i]),
            'flipped': int(flipped_count[i]),
            'win_rate_pct': round(100 * int(wins[i]) / trades, 2) if trades else None,
            'pnl': round(pnl_i, 2),
            'starting_balance': starting_balance,
            'ending_balance': round(starting_balance + pnl_i, 2),
            'weights': dict(variant.weights),
            'bias': float(variant.bias or 0.0),
        })
    return out


def _push_top(heap: list, row: dict, top_n: int, seq: int) -> None:
    item = (float(row['pnl']), seq, row)
    if len(heap) < top_n:
        heapq.heappush(heap, item)
    elif item[0] > heap[0][0]:
        heapq.heapreplace(heap, item)


def _batched(items: Iterable[slow.Variant], batch_size: int):
    batch = []
    for item in items:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def broad_full_variants() -> Iterable[slow.Variant]:
    archetypes = [
        ('cont', {'ema': 1.5, 'vwap': 0.75, 'momentum': 1.25, 'btc': 1.25, 'relative': 0.75, 'miner': 0.75, 'burst': 0.75}),
        ('vwap_rev', {'ema': 0.5, 'vwap': -2.0, 'momentum': 0.25, 'btc': 0.75, 'relative': 0.25, 'miner': 0.5, 'burst': 0.0}),
        ('flow_fade', {'ema': 0.5, 'vwap': -0.75, 'momentum': -0.25, 'btc': 0.25, 'relative': 0.25, 'miner': 0.25, 'burst': -0.5, 'flow_contra': 3.0}),
        ('btc_rel', {'ema': 0.75, 'vwap': 0.25, 'momentum': 0.5, 'btc': 2.0, 'relative': 2.5, 'miner': 0.5, 'burst': 0.25}),
        ('anti_btc_rel', {'ema': 0.75, 'vwap': -0.25, 'momentum': 0.25, 'btc': -1.5, 'relative': -2.5, 'miner': 0.25, 'burst': 0.25}),
        ('burst_follow', {'ema': 1.0, 'vwap': 0.5, 'momentum': 1.5, 'btc': 1.0, 'relative': 0.5, 'miner': 0.5, 'burst': 2.5}),
        ('burst_exhaust', {'ema': 0.25, 'vwap': -1.5, 'momentum': -1.0, 'btc': 0.0, 'relative': 0.0, 'miner': 0.25, 'burst': -2.5, 'flow_contra': 2.0}),
        ('miner_heavy', {'ema': 0.75, 'vwap': 0.25, 'momentum': 0.75, 'btc': 1.0, 'relative': 0.75, 'miner': 3.0, 'burst': 0.25}),
        ('stock_only', {'ema': 2.0, 'vwap': 1.0, 'momentum': 2.0, 'btc': 0.0, 'relative': 0.0, 'miner': 0.5, 'burst': 1.0}),
        ('mean_rev_all', {'ema': -0.75, 'vwap': -1.5, 'momentum': -1.25, 'btc': -0.5, 'relative': -0.75, 'miner': -0.25, 'burst': -0.75, 'flow_contra': 1.5}),
    ]
    flow_weights = [-1.5, 0.0, 1.5, 3.0, 4.5]
    chop_weights = [-3.0, -1.0, 0.75, 2.0, 4.0]
    exec_weights = [-3.0, -1.5, -0.5, 0.0, 2.0]
    phase_biases = [
        ('phase_neutral', {}),
        ('open_fade', {'open_phase': 2.0}),
        ('open_follow', {'open_phase': -1.5}),
        ('midday_fade', {'midday_phase': 1.5}),
        ('midday_skeptic', {'midday_phase': -1.5}),
    ]
    setup_biases = [
        ('setup_neutral', {}),
        ('rel_skeptic', {'setup_btc_relative_strength': -1.5}),
        ('rel_chaser', {'setup_btc_relative_strength': 1.5}),
        ('breakout_pref', {'setup_momentum_breakout': 2.0}),
        ('pullback_skeptic', {'setup_trend_pullback': -1.25}),
    ]
    side_ticker_biases = [
        ('ticker_neutral', {}),
        ('riot_short_skeptic', {'riot_short_penalty': 3.0}),
        ('riot_reversal', {'riot_short_penalty': 2.0, 'riot_long_penalty': -1.0}),
        ('short_skeptic', {'bias': 0.75}),
        ('long_skeptic', {'bias': -0.75}),
    ]
    idx = 1
    for (ai, (aname, base)), (fi, flow_w), (ci, chop_w), (ei, exec_w), (pi, (phase_name, phase_w)), (si, (setup_name, setup_w)), (ti, (ticker_name, ticker_w)) in itertools.product(
        enumerate(archetypes), enumerate(flow_weights), enumerate(chop_weights), enumerate(exec_weights),
        enumerate(phase_biases), enumerate(setup_biases), enumerate(side_ticker_biases),
    ):
        weights = dict(base)
        revish = any(token in aname for token in ('rev', 'fade', 'inverse', 'chase'))
        weights.update({
            'flow_contra': flow_w,
            'btc_chop': chop_w,
            'exec_penalty': exec_w,
            'vwap_sigma_ext': -0.75 if revish else 0.35,
            'btc_mom_abs': 0.5 if 'btc' in aname else 0.0,
            'flow_pressure_abs': 0.5 if flow_w < 0 else -0.35,
        })
        weights.update(phase_w)
        weights.update(setup_w)
        weights.update(ticker_w)
        bias = float(weights.pop('bias', 0.0) or 0.0)
        yield slow.Variant(f'broad_full_{idx:06d}_{aname}_{phase_name}_{setup_name}_{ticker_name}_f{fi}_c{ci}_e{ei}_p{pi}_s{si}_t{ti}', weights, bias)
        idx += 1


def v29_full_variants() -> Iterable[slow.Variant]:
    relative_weights = [-0.75, -1.0, -1.25, -1.5, -1.75, -2.0, -2.5]
    vwap_weights = [-0.25, -0.75, -1.0, -1.5, -2.0]
    momentum_weights = [0.25, 0.75, 1.0, 1.5]
    btc_weights = [0.25, 0.75, 1.0, 1.5, 2.0]
    flow_weights = [0.0, 0.75, 1.5, 2.25]
    chop_weights = [0.0, 0.75, 1.5, 2.5]
    exec_choices = [-2.0, -1.0, -0.5, 0.5]
    ema_choices = [0.5, 1.0, 1.5]
    miner_choices = [0.0, 0.5, 1.0, 1.5]
    burst_choices = [-0.5, 0.0, 0.5, 1.0]
    extras = [
        ('x0', {}),
        ('x1', {'riot_short_penalty': 1.5}),
        ('x2', {'riot_short_penalty': 3.0}),
        ('x3', {'open_phase': 1.5}),
        ('x4', {'open_phase': -1.0}),
        ('x5', {'midday_phase': 1.5}),
        ('x6', {'setup_btc_relative_strength': -1.0}),
        ('x7', {'setup_btc_relative_strength': -2.0}),
        ('x8', {'setup_momentum_breakout': 1.5}),
        ('x9', {'setup_trend_pullback': -1.0}),
        ('x10', {'vwap_sigma_ext': -0.75}),
        ('x11', {'flow_pressure_abs': -0.5}),
        ('x12', {'btc_mom_abs': 0.75}),
    ]
    idx = 1
    for ri, rel_w in enumerate(relative_weights):
        for vi, vwap_w in enumerate(vwap_weights):
            for mi, mom_w in enumerate(momentum_weights):
                for bi, btc_w in enumerate(btc_weights):
                    for fi, flow_w in enumerate(flow_weights):
                        for ci, chop_w in enumerate(chop_weights):
                            for ei, exec_w in enumerate(exec_choices):
                                for emi, ema_w in enumerate(ema_choices):
                                    for mini, miner_w in enumerate(miner_choices):
                                        for bui, burst_w in enumerate(burst_choices):
                                            for xi, (extra_name, extra) in enumerate(extras):
                                                weights = {
                                                    'relative': rel_w,
                                                    'vwap': vwap_w,
                                                    'momentum': mom_w,
                                                    'btc': btc_w,
                                                    'flow_contra': flow_w,
                                                    'btc_chop': chop_w,
                                                    'exec_penalty': exec_w,
                                                    'ema': ema_w,
                                                    'miner': miner_w,
                                                    'burst': burst_w,
                                                }
                                                weights.update(extra)
                                                yield slow.Variant(
                                                    f'v29_full_{idx:08d}_rel{ri}_vw{vi}_mom{mi}_btc{bi}_flow{fi}_chop{ci}_exec{ei}_ema{emi}_miner{mini}_burst{bui}_{extra_name}',
                                                    weights,
                                                )
                                                idx += 1


def variant_stream(include_existing: bool, include_broad_full: bool, include_v29_full: bool) -> Iterable[slow.Variant]:
    seen = set()
    if include_existing:
        for variant in slow.VARIANTS:
            sig = _signature(variant.weights, variant.bias)
            if sig in seen:
                continue
            seen.add(sig)
            yield variant
    if include_broad_full:
        for variant in broad_full_variants():
            sig = _signature(variant.weights, variant.bias)
            if sig in seen:
                continue
            seen.add(sig)
            yield variant
    if include_v29_full:
        for variant in v29_full_variants():
            sig = _signature(variant.weights, variant.bias)
            if sig in seen:
                continue
            seen.add(sig)
            yield variant


def estimated_count(include_existing: bool, include_broad_full: bool, include_v29_full: bool) -> dict:
    return {
        'existing_current': len(slow.VARIANTS) if include_existing else 0,
        'broad_full_raw': 156250 if include_broad_full else 0,
        'v29_full_raw': 27955200 if include_v29_full else 0,
        'raw_total_before_dedup': (len(slow.VARIANTS) if include_existing else 0) + (156250 if include_broad_full else 0) + (27955200 if include_v29_full else 0),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description='Stream a massive scoring-lab search and keep top N.')
    ap.add_argument('--csv', default=DEFAULT_CSV)
    ap.add_argument('--out', default=DEFAULT_OUT)
    ap.add_argument('--starting-balance', type=float, default=100000.0)
    ap.add_argument('--top-n', type=int, default=10000)
    ap.add_argument('--batch-size', type=int, default=20000)
    ap.add_argument('--progress-every', type=int, default=500000)
    ap.add_argument('--no-existing', action='store_true')
    ap.add_argument('--no-broad-full', action='store_true')
    ap.add_argument('--no-v29-full', action='store_true')
    args = ap.parse_args()

    started = time.perf_counter()
    rows = fast.load_rows(args.csv)
    agg = _aggregate_rows(rows)
    heap: list = []
    processed = 0
    seq = 0
    next_progress = args.progress_every
    counts = estimated_count(not args.no_existing, not args.no_broad_full, not args.no_v29_full)
    print(json.dumps({'event': 'start', 'counts': counts, 'aggregation': {'rows': len(rows), 'buckets': agg['buckets']}}, sort_keys=True), flush=True)

    stream = variant_stream(not args.no_existing, not args.no_broad_full, not args.no_v29_full)
    for batch in _batched(stream, max(1, args.batch_size)):
        for row in _score_batch(agg, batch, args.starting_balance):
            _push_top(heap, row, args.top_n, seq)
            seq += 1
        processed += len(batch)
        if processed >= next_progress:
            elapsed = time.perf_counter() - started
            best = max(heap, key=lambda item: item[0])[2] if heap else None
            print(json.dumps({
                'event': 'progress',
                'processed': processed,
                'elapsed_seconds': round(elapsed, 2),
                'variants_per_sec': round(processed / elapsed, 2) if elapsed else None,
                'current_best': {'variant': best.get('variant'), 'pnl': best.get('pnl')} if best else None,
            }, sort_keys=True), flush=True)
            next_progress += args.progress_every

    top = tournament_safety.enrich_lab_results([item[2] for item in sorted(heap, key=lambda item: item[0], reverse=True)])
    elapsed = round(time.perf_counter() - started, 3)
    payload = {
        'schema_version': 2,
        'source_csv': args.csv,
        'source_csv_sha256': tournament_safety._file_sha256(args.csv),
        'elapsed_seconds': elapsed,
        'processed_variants': processed,
        'top_n': args.top_n,
        'counts': counts,
        'aggregation': {'rows': len(rows), 'buckets': agg['buckets']},
        'method_note': (
            'Massive lab screen. Uses mechanical inverse P/L for side flips; '
            'this is a ranking proxy, not a full replay verdict. model_id and family_id are stable audit helpers.'
        ),
        'results': top,
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    print(json.dumps({
        'event': 'done',
        'out': args.out,
        'processed_variants': processed,
        'elapsed_seconds': elapsed,
        'best': top[0] if top else None,
    }, indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
