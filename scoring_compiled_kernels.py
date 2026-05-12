"""Optional accelerated kernels for replay-only scoring screens.

Numba and matrix kernels are used only as arithmetic accelerators. The fallback
path is the existing NumPy/Python implementation, so correctness does not depend
on optional compiled code.
"""
from __future__ import annotations

import os

import numpy as np

import scoring_variant_lab as slow
import scoring_variant_lab_massive as massive


try:  # pragma: no cover - depends on local optional package
    from numba import njit
except Exception:  # pragma: no cover
    njit = None


COMPILED_AVAILABLE = njit is not None
MATRIX_AVAILABLE = True


if njit is not None:  # pragma: no cover - exercised only when numba exists
    @njit(cache=True)
    def _score_core(features, original_side, pnl_sum, count, win_count, loss_count,
                    flipped_win_count, flipped_loss_count, weights, bias):
        buckets = features.shape[0]
        variants = weights.shape[0]
        pnl = np.zeros(variants, dtype=np.float64)
        wins = np.zeros(variants, dtype=np.int64)
        losses = np.zeros(variants, dtype=np.int64)
        flipped = np.zeros(variants, dtype=np.int64)
        for v in range(variants):
            for b in range(buckets):
                score = bias[v]
                for f in range(features.shape[1]):
                    score += features[b, f] * weights[v, f]
                chosen = original_side[b]
                if score > 0.0:
                    chosen = 1
                elif score < 0.0:
                    chosen = -1
                if chosen != original_side[b]:
                    pnl[v] -= pnl_sum[b]
                    wins[v] += flipped_win_count[b]
                    losses[v] += flipped_loss_count[b]
                    flipped[v] += count[b]
                else:
                    pnl[v] += pnl_sum[b]
                    wins[v] += win_count[b]
                    losses[v] += loss_count[b]
        return pnl, wins, losses, flipped
else:
    _score_core = None


def _kernel_mode() -> str:
    mode = os.environ.get('SCORING_KERNEL', 'auto').strip().lower()
    if mode in ('matrix', 'numba', 'python', 'auto'):
        return mode
    return 'auto'


def should_use_compiled() -> bool:
    value = os.environ.get('SCORING_USE_COMPILED', '1').strip().lower()
    return COMPILED_AVAILABLE and value not in ('0', 'false', 'no', 'off')


def should_use_matrix() -> bool:
    value = os.environ.get('SCORING_USE_MATRIX', '1').strip().lower()
    return MATRIX_AVAILABLE and value not in ('0', 'false', 'no', 'off')


def _score_core_matrix(agg: dict, weights: np.ndarray, bias: np.ndarray) -> tuple[np.ndarray, ...]:
    features = np.asarray(agg['features'], dtype=np.float64)
    original_side = np.asarray(agg['original_side'])
    pnl_sum = np.asarray(agg['pnl_sum'], dtype=np.float64)
    count = np.asarray(agg['count'])
    win_count = np.asarray(agg['win_count'])
    loss_count = np.asarray(agg['loss_count'])
    flipped_win_count = np.asarray(agg['flipped_win_count'])
    flipped_loss_count = np.asarray(agg['flipped_loss_count'])

    scores = features @ weights.T
    if bias.size:
        scores = scores + bias.reshape(1, -1)
    original = original_side.reshape(-1, 1)
    chosen = np.where(scores > 0.0, 1, np.where(scores < 0.0, -1, original))
    flipped = chosen != original
    pnl = np.where(flipped, -pnl_sum.reshape(-1, 1), pnl_sum.reshape(-1, 1)).sum(axis=0)
    wins = np.where(flipped, flipped_win_count.reshape(-1, 1), win_count.reshape(-1, 1)).sum(axis=0)
    losses = np.where(flipped, flipped_loss_count.reshape(-1, 1), loss_count.reshape(-1, 1)).sum(axis=0)
    flipped_count = np.where(flipped, count.reshape(-1, 1), 0).sum(axis=0)
    return pnl, wins.astype(np.int64), losses.astype(np.int64), flipped_count.astype(np.int64)


def score_weight_matrix_arrays(agg: dict, weights: np.ndarray, bias: np.ndarray) -> tuple[np.ndarray, ...]:
    mode = _kernel_mode()
    if mode in ('auto', 'matrix') and should_use_matrix():
        return _score_core_matrix(agg, weights, bias)
    if mode in ('auto', 'numba') and should_use_compiled():
        return _score_core(
            np.asarray(agg['features']),
            np.asarray(agg['original_side']),
            np.asarray(agg['pnl_sum']),
            np.asarray(agg['count']),
            np.asarray(agg['win_count']),
            np.asarray(agg['loss_count']),
            np.asarray(agg['flipped_win_count']),
            np.asarray(agg['flipped_loss_count']),
            weights,
            bias,
        )
    # Last-resort matrix path keeps this helper usable even when env flags
    # disable optional accelerators.
    return _score_core_matrix(agg, weights, bias)


def decision_hashes(agg: dict, variants: list[slow.Variant], chunk_rows: int = 4096) -> list[str]:
    """Hash each variant's bucket-level side choices for dedupe-friendly reporting.

    This is a post-score identity helper. It does not prune or alter results.
    """
    import hashlib

    weights, bias = massive._variant_matrix(variants)
    features = np.asarray(agg['features'], dtype=np.float64)
    original_side = np.asarray(agg['original_side'])
    hashes = [hashlib.sha256() for _ in variants]
    for start in range(0, features.shape[0], max(1, int(chunk_rows))):
        stop = min(features.shape[0], start + max(1, int(chunk_rows)))
        scores = features[start:stop] @ weights.T
        if bias.size:
            scores = scores + bias.reshape(1, -1)
        original = original_side[start:stop].reshape(-1, 1)
        chosen = np.where(scores > 0.0, 1, np.where(scores < 0.0, -1, original)).astype(np.int8)
        packed = np.packbits((chosen > 0).astype(np.uint8), axis=0)
        for idx, h in enumerate(hashes):
            h.update(packed[:, idx].tobytes())
    return [h.hexdigest()[:24] for h in hashes]


def score_batch(agg: dict, variants: list[slow.Variant], starting_balance: float) -> list[dict]:
    pnl, wins, losses, flipped_count = score_batch_arrays(agg, variants)
    trades = int(agg['trades'])
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


def score_batch_arrays(agg: dict, variants: list[slow.Variant]) -> tuple[np.ndarray, ...]:
    """Return raw score arrays without materializing per-variant dictionaries."""
    weights, bias = massive._variant_matrix(variants)
    mode = _kernel_mode()
    if mode == 'python':
        rows = massive._score_batch(agg, variants, 0.0)
        return (
            np.asarray([float(r['pnl']) for r in rows], dtype=np.float64),
            np.asarray([int(r['wins']) for r in rows], dtype=np.int64),
            np.asarray([int(r['losses']) for r in rows], dtype=np.int64),
            np.asarray([int(r['flipped']) for r in rows], dtype=np.int64),
        )
    if mode in ('auto', 'matrix') and should_use_matrix():
        pnl, wins, losses, flipped_count = _score_core_matrix(agg, weights, bias)
    elif mode in ('auto', 'numba') and should_use_compiled():
        pnl, wins, losses, flipped_count = _score_core(
            np.asarray(agg['features']),
            np.asarray(agg['original_side']),
            np.asarray(agg['pnl_sum']),
            np.asarray(agg['count']),
            np.asarray(agg['win_count']),
            np.asarray(agg['loss_count']),
            np.asarray(agg['flipped_win_count']),
            np.asarray(agg['flipped_loss_count']),
            weights,
            bias,
        )
    else:
        rows = massive._score_batch(agg, variants, 0.0)
        return (
            np.asarray([float(r['pnl']) for r in rows], dtype=np.float64),
            np.asarray([int(r['wins']) for r in rows], dtype=np.int64),
            np.asarray([int(r['losses']) for r in rows], dtype=np.int64),
            np.asarray([int(r['flipped']) for r in rows], dtype=np.int64),
        )
    return pnl, wins, losses, flipped_count


def row_from_arrays(variant: slow.Variant, index: int, arrays: tuple[np.ndarray, ...],
                    trades: int, starting_balance: float) -> dict:
    pnl, wins, losses, flipped_count = arrays
    pnl_i = float(pnl[index])
    wins_i = int(wins[index])
    return {
        'variant': variant.name,
        'trades': trades,
        'wins': wins_i,
        'losses': int(losses[index]),
        'flipped': int(flipped_count[index]),
        'win_rate_pct': round(100 * wins_i / trades, 2) if trades else None,
        'pnl': round(pnl_i, 2),
        'starting_balance': starting_balance,
        'ending_balance': round(starting_balance + pnl_i, 2),
        'weights': dict(variant.weights),
        'bias': float(variant.bias or 0.0),
    }
