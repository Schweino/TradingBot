"""Direct weight-matrix generation for random-access scoring variants.

This is a Step-1 accelerator. It mirrors scoring_variant_random_access.py but
returns numeric arrays instead of materializing millions of Variant objects.
Names and dict metadata are still produced by scoring_variant_random_access only
for retained Top-K rows.
"""
from __future__ import annotations

import numpy as np

import scoring_variant_lab as slow
import scoring_variant_lab_fast as fast
import scoring_variant_random_access as ra


FEATURE_NAMES = list(fast.FEATURE_NAMES)
FEATURE_INDEX = {name: idx for idx, name in enumerate(FEATURE_NAMES)}


def _unravel_many(indexes: np.ndarray, dims: list[int]) -> list[np.ndarray]:
    idx = indexes.astype(np.int64, copy=True)
    out = [None] * len(dims)
    for pos in range(len(dims) - 1, -1, -1):
        dim = int(dims[pos])
        out[pos] = idx % dim
        idx //= dim
    return out


def _apply_dicts(weights: np.ndarray, selector: np.ndarray, choices: list[tuple[str, dict]]) -> None:
    for choice_idx, (_name, mapping) in enumerate(choices):
        mask = selector == choice_idx
        if not np.any(mask):
            continue
        for feature, value in mapping.items():
            if feature == 'bias' or feature not in FEATURE_INDEX:
                continue
            weights[mask, FEATURE_INDEX[feature]] = float(value or 0.0)


def _apply_bias_dicts(bias: np.ndarray, selector: np.ndarray, choices: list[tuple[str, dict]]) -> None:
    for choice_idx, (_name, mapping) in enumerate(choices):
        value = mapping.get('bias')
        if value is not None:
            bias[selector == choice_idx] = float(value or 0.0)


def _set_feature(weights: np.ndarray, name: str, values) -> None:
    if name in FEATURE_INDEX:
        weights[:, FEATURE_INDEX[name]] = values


def _broad_matrix(raw_indexes: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    dims = [
        len(ra.BROAD_ARCHETYPES),
        len(ra.BROAD_FLOW_WEIGHTS),
        len(ra.BROAD_CHOP_WEIGHTS),
        len(ra.BROAD_EXEC_WEIGHTS),
        len(ra.BROAD_PHASE_BIASES),
        len(ra.BROAD_SETUP_BIASES),
        len(ra.BROAD_SIDE_TICKER_BIASES),
    ]
    ai, fi, ci, ei, pi, si, ti = _unravel_many(raw_indexes, dims)
    weights = np.zeros((len(raw_indexes), len(FEATURE_NAMES)), dtype=np.float64)
    bias = np.zeros(len(raw_indexes), dtype=np.float64)
    _apply_dicts(weights, ai, ra.BROAD_ARCHETYPES)
    _set_feature(weights, 'flow_contra', np.asarray(ra.BROAD_FLOW_WEIGHTS, dtype=np.float64)[fi])
    _set_feature(weights, 'btc_chop', np.asarray(ra.BROAD_CHOP_WEIGHTS, dtype=np.float64)[ci])
    _set_feature(weights, 'exec_penalty', np.asarray(ra.BROAD_EXEC_WEIGHTS, dtype=np.float64)[ei])
    revish = np.zeros(len(raw_indexes), dtype=bool)
    for idx, (name, _base) in enumerate(ra.BROAD_ARCHETYPES):
        if any(token in name for token in ('rev', 'fade', 'inverse', 'chase')):
            revish |= ai == idx
    _set_feature(weights, 'vwap_sigma_ext', np.where(revish, -0.75, 0.35))
    btcish = np.zeros(len(raw_indexes), dtype=bool)
    for idx, (name, _base) in enumerate(ra.BROAD_ARCHETYPES):
        if 'btc' in name:
            btcish |= ai == idx
    _set_feature(weights, 'btc_mom_abs', np.where(btcish, 0.5, 0.0))
    _set_feature(weights, 'flow_pressure_abs', np.where(np.asarray(ra.BROAD_FLOW_WEIGHTS, dtype=np.float64)[fi] < 0, 0.5, -0.35))
    _apply_dicts(weights, pi, ra.BROAD_PHASE_BIASES)
    _apply_dicts(weights, si, ra.BROAD_SETUP_BIASES)
    _apply_dicts(weights, ti, ra.BROAD_SIDE_TICKER_BIASES)
    _apply_bias_dicts(bias, ti, ra.BROAD_SIDE_TICKER_BIASES)
    return weights, bias


def _v29_matrix(raw_indexes: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    dims = [
        len(ra.V29_RELATIVE_WEIGHTS),
        len(ra.V29_VWAP_WEIGHTS),
        len(ra.V29_MOMENTUM_WEIGHTS),
        len(ra.V29_BTC_WEIGHTS),
        len(ra.V29_FLOW_WEIGHTS),
        len(ra.V29_CHOP_WEIGHTS),
        len(ra.V29_EXEC_CHOICES),
        len(ra.V29_EMA_CHOICES),
        len(ra.V29_MINER_CHOICES),
        len(ra.V29_BURST_CHOICES),
        len(ra.V29_EXTRAS),
    ]
    ri, vi, mi, bi, fi, ci, ei, emi, mini, bui, xi = _unravel_many(raw_indexes, dims)
    weights = np.zeros((len(raw_indexes), len(FEATURE_NAMES)), dtype=np.float64)
    bias = np.zeros(len(raw_indexes), dtype=np.float64)
    _set_feature(weights, 'relative', np.asarray(ra.V29_RELATIVE_WEIGHTS, dtype=np.float64)[ri])
    _set_feature(weights, 'vwap', np.asarray(ra.V29_VWAP_WEIGHTS, dtype=np.float64)[vi])
    _set_feature(weights, 'momentum', np.asarray(ra.V29_MOMENTUM_WEIGHTS, dtype=np.float64)[mi])
    _set_feature(weights, 'btc', np.asarray(ra.V29_BTC_WEIGHTS, dtype=np.float64)[bi])
    _set_feature(weights, 'flow_contra', np.asarray(ra.V29_FLOW_WEIGHTS, dtype=np.float64)[fi])
    _set_feature(weights, 'btc_chop', np.asarray(ra.V29_CHOP_WEIGHTS, dtype=np.float64)[ci])
    _set_feature(weights, 'exec_penalty', np.asarray(ra.V29_EXEC_CHOICES, dtype=np.float64)[ei])
    _set_feature(weights, 'ema', np.asarray(ra.V29_EMA_CHOICES, dtype=np.float64)[emi])
    _set_feature(weights, 'miner', np.asarray(ra.V29_MINER_CHOICES, dtype=np.float64)[mini])
    _set_feature(weights, 'burst', np.asarray(ra.V29_BURST_CHOICES, dtype=np.float64)[bui])
    _apply_dicts(weights, xi, ra.V29_EXTRAS)
    return weights, bias


def _expanded_matrix(raw_indexes: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    core_names = list(ra.EXPANDED_CORE_OVERRIDES)
    dims = [
        len(ra.EXPANDED_ARCHETYPES),
        len(ra.EXPANDED_SCALES),
        len(ra.EXPANDED_BIASES),
        *([5] * len(core_names)),
        len(ra.EXPANDED_PHASE_PACKS),
        len(ra.EXPANDED_TICKER_PACKS),
        len(ra.EXPANDED_SETUP_PACKS),
        len(ra.EXPANDED_EXTREME_PACKS),
    ]
    parts = _unravel_many(raw_indexes, dims)
    ai, si, bi = parts[:3]
    core_indexes = parts[3:3 + len(core_names)]
    phase_i, ticker_i, setup_i, extreme_i = parts[3 + len(core_names):]
    weights = np.zeros((len(raw_indexes), len(FEATURE_NAMES)), dtype=np.float64)
    bias = np.asarray(ra.EXPANDED_BIASES, dtype=np.float64)[bi].copy()
    scales = np.asarray(ra.EXPANDED_SCALES, dtype=np.float64)[si]
    for arche_idx, (_name, base) in enumerate(ra.EXPANDED_ARCHETYPES):
        mask = ai == arche_idx
        if not np.any(mask):
            continue
        for feature, value in base.items():
            if feature in FEATURE_INDEX:
                weights[mask, FEATURE_INDEX[feature]] = float(value or 0.0) * scales[mask]
    for name, choice_i in zip(core_names, core_indexes):
        _set_feature(weights, name, np.asarray(ra.EXPANDED_CORE_OVERRIDES[name], dtype=np.float64)[choice_i])
    _apply_dicts(weights, phase_i, ra.EXPANDED_PHASE_PACKS)
    _apply_dicts(weights, ticker_i, ra.EXPANDED_TICKER_PACKS)
    _apply_dicts(weights, setup_i, ra.EXPANDED_SETUP_PACKS)
    _apply_dicts(weights, extreme_i, ra.EXPANDED_EXTREME_PACKS)
    return weights, bias


def _active_local_matrix(raw_indexes: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    dims = [
        len(ra.ACTIVE_SCALES),
        len(ra.ACTIVE_BIASES),
        *([len(ra.ACTIVE_DELTAS)] * len(ra.ACTIVE_CORE_NAMES)),
        len(ra.ACTIVE_PHASE_PACKS),
        len(ra.ACTIVE_TICKER_PACKS),
        len(ra.ACTIVE_SETUP_PACKS),
        len(ra.ACTIVE_EXTREME_PACKS),
    ]
    coordinate_index = (raw_indexes.astype(np.int64) * 1_000_003 + 97_531) % ra.active_local_count()
    parts = _unravel_many(coordinate_index, dims)
    scale_i, bias_i = parts[:2]
    core_indexes = parts[2:2 + len(ra.ACTIVE_CORE_NAMES)]
    phase_i, ticker_i, setup_i, extreme_i = parts[2 + len(ra.ACTIVE_CORE_NAMES):]
    weights = np.zeros((len(raw_indexes), len(FEATURE_NAMES)), dtype=np.float64)
    scales = np.asarray(ra.ACTIVE_SCALES, dtype=np.float64)[scale_i]
    for feature, value in ra.ACTIVE_BASE_WEIGHTS.items():
        if feature in FEATURE_INDEX:
            weights[:, FEATURE_INDEX[feature]] = float(value or 0.0) * scales
    for name, delta_i in zip(ra.ACTIVE_CORE_NAMES, core_indexes):
        if name in FEATURE_INDEX:
            base = float(ra.ACTIVE_BASE_WEIGHTS.get(name, 0.0) or 0.0)
            weights[:, FEATURE_INDEX[name]] = base + np.asarray(ra.ACTIVE_DELTAS, dtype=np.float64)[delta_i]
    _apply_dicts(weights, phase_i, ra.ACTIVE_PHASE_PACKS)
    _apply_dicts(weights, ticker_i, ra.ACTIVE_TICKER_PACKS)
    _apply_dicts(weights, setup_i, ra.ACTIVE_SETUP_PACKS)
    _apply_dicts(weights, extreme_i, ra.ACTIVE_EXTREME_PACKS)
    bias = np.asarray(ra.ACTIVE_BIASES, dtype=np.float64)[bias_i].copy()
    return weights, bias


def _creative_matrix(raw_indexes: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    dims = [
        len(ra.CREATIVE_BASES),
        len(ra.CREATIVE_SCALES),
        len(ra.CREATIVE_BIASES),
        *([len(ra.CREATIVE_DELTAS)] * len(ra.CREATIVE_CORE_NAMES)),
        len(ra.CREATIVE_PHASE_PACKS),
        len(ra.CREATIVE_TICKER_PACKS),
        len(ra.CREATIVE_SETUP_PACKS),
        len(ra.CREATIVE_EXTREME_PACKS),
    ]
    coordinate_index = (raw_indexes.astype(np.int64) * 9_176_191 + 271_828) % ra.creative_count()
    parts = _unravel_many(coordinate_index, dims)
    base_i, scale_i, bias_i = parts[:3]
    core_indexes = parts[3:3 + len(ra.CREATIVE_CORE_NAMES)]
    phase_i, ticker_i, setup_i, extreme_i = parts[3 + len(ra.CREATIVE_CORE_NAMES):]
    weights = np.zeros((len(raw_indexes), len(FEATURE_NAMES)), dtype=np.float64)
    scales = np.asarray(ra.CREATIVE_SCALES, dtype=np.float64)[scale_i]
    for base_idx, (_base_name, base) in enumerate(ra.CREATIVE_BASES):
        mask = base_i == base_idx
        if not np.any(mask):
            continue
        for feature, value in base.items():
            if feature in FEATURE_INDEX:
                weights[mask, FEATURE_INDEX[feature]] = float(value or 0.0) * scales[mask]
    deltas = np.asarray(ra.CREATIVE_DELTAS, dtype=np.float64)
    for name, delta_i in zip(ra.CREATIVE_CORE_NAMES, core_indexes):
        if name in FEATURE_INDEX:
            weights[:, FEATURE_INDEX[name]] += deltas[delta_i]
    _apply_dicts(weights, phase_i, ra.CREATIVE_PHASE_PACKS)
    _apply_dicts(weights, ticker_i, ra.CREATIVE_TICKER_PACKS)
    _apply_dicts(weights, setup_i, ra.CREATIVE_SETUP_PACKS)
    _apply_dicts(weights, extreme_i, ra.CREATIVE_EXTREME_PACKS)
    bias = np.asarray(ra.CREATIVE_BIASES, dtype=np.float64)[bias_i].copy()
    return weights, bias


def _core2_creative_matrix(raw_indexes: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    dims = [
        len(ra.ACTIVE_SCALES),
        len(ra.ACTIVE_BIASES),
        *([len(ra.CREATIVE_DELTAS)] * len(ra.CORE2_MUTABLE_NAMES)),
        len(ra.CREATIVE_PHASE_PACKS),
        len(ra.CREATIVE_TICKER_PACKS),
        len(ra.CREATIVE_SETUP_PACKS),
        len(ra.CREATIVE_EXTREME_PACKS),
    ]
    coordinate_index = (raw_indexes.astype(np.int64) * 6_700_417 + 314_159) % ra.core2_creative_count()
    parts = _unravel_many(coordinate_index, dims)
    scale_i, bias_i = parts[:2]
    core_indexes = parts[2:2 + len(ra.CORE2_MUTABLE_NAMES)]
    phase_i, ticker_i, setup_i, extreme_i = parts[2 + len(ra.CORE2_MUTABLE_NAMES):]
    weights = np.zeros((len(raw_indexes), len(FEATURE_NAMES)), dtype=np.float64)
    scales = np.asarray(ra.ACTIVE_SCALES, dtype=np.float64)[scale_i]
    for feature, value in ra.ACTIVE_BASE_WEIGHTS.items():
        if feature in FEATURE_INDEX:
            weights[:, FEATURE_INDEX[feature]] = float(value or 0.0) * scales
    for name in ra.CORE2_ANCHOR_NAMES:
        _set_feature(weights, name, float(ra.ACTIVE_BASE_WEIGHTS.get(name, 0.0) or 0.0))
    deltas = np.asarray(ra.CREATIVE_DELTAS, dtype=np.float64)
    for name, delta_i in zip(ra.CORE2_MUTABLE_NAMES, core_indexes):
        if name in FEATURE_INDEX:
            weights[:, FEATURE_INDEX[name]] += deltas[delta_i]
    _apply_dicts(weights, phase_i, ra.CREATIVE_PHASE_PACKS)
    _apply_dicts(weights, ticker_i, ra.CREATIVE_TICKER_PACKS)
    _apply_dicts(weights, setup_i, ra.CREATIVE_SETUP_PACKS)
    _apply_dicts(weights, extreme_i, ra.CREATIVE_EXTREME_PACKS)
    for name in ra.CORE2_ANCHOR_NAMES:
        _set_feature(weights, name, float(ra.ACTIVE_BASE_WEIGHTS.get(name, 0.0) or 0.0))
    bias = np.asarray(ra.CREATIVE_BIASES, dtype=np.float64)[bias_i].copy()
    return weights, bias


def _core2_guarded_matrix(raw_indexes: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    core_names = list(ra.GUARDED_CORE_CHOICES)
    dims = [
        len(ra.GUARDED_RELATIVE_WEIGHTS),
        len(ra.GUARDED_VWAP_WEIGHTS),
        len(ra.GUARDED_BIASES),
        *[len(ra.GUARDED_CORE_CHOICES[name]) for name in core_names],
        len(ra.GUARDED_PHASE_PACKS),
        len(ra.GUARDED_TICKER_PACKS),
        len(ra.GUARDED_SETUP_PACKS),
        len(ra.GUARDED_EXTREME_PACKS),
    ]
    coordinate_index = (raw_indexes.astype(np.int64) * 5_430_131 + 618_033) % ra.core2_guarded_count()
    parts = _unravel_many(coordinate_index, dims)
    rel_i, vwap_i, bias_i = parts[:3]
    core_indexes = parts[3:3 + len(core_names)]
    phase_i, ticker_i, setup_i, extreme_i = parts[3 + len(core_names):]
    weights = np.zeros((len(raw_indexes), len(FEATURE_NAMES)), dtype=np.float64)
    _set_feature(weights, 'relative', np.asarray(ra.GUARDED_RELATIVE_WEIGHTS, dtype=np.float64)[rel_i])
    _set_feature(weights, 'vwap', np.asarray(ra.GUARDED_VWAP_WEIGHTS, dtype=np.float64)[vwap_i])
    for name, choice_i in zip(core_names, core_indexes):
        _set_feature(weights, name, np.asarray(ra.GUARDED_CORE_CHOICES[name], dtype=np.float64)[choice_i])
    _apply_dicts(weights, phase_i, ra.GUARDED_PHASE_PACKS)
    _apply_dicts(weights, ticker_i, ra.GUARDED_TICKER_PACKS)
    _apply_dicts(weights, setup_i, ra.GUARDED_SETUP_PACKS)
    _apply_dicts(weights, extreme_i, ra.GUARDED_EXTREME_PACKS)
    bias = np.asarray(ra.GUARDED_BIASES, dtype=np.float64)[bias_i].copy()
    return weights, bias


def _micro_local_matrix(raw_indexes: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    core_names = list(ra.MICRO_LOCAL_CORE_DELTAS)
    dims = [
        len(ra.MICRO_LOCAL_BIASES),
        *[len(ra.MICRO_LOCAL_CORE_DELTAS[name]) for name in core_names],
        len(ra.MICRO_LOCAL_AUX_PACKS),
    ]
    weights = np.zeros((len(raw_indexes), len(FEATURE_NAMES)), dtype=np.float64)
    bias = np.zeros(len(raw_indexes), dtype=np.float64)
    live_mask = raw_indexes.astype(np.int64) == 0
    non_live = ~live_mask
    for feature, value in ra.ACTIVE_BASE_WEIGHTS.items():
        if feature in FEATURE_INDEX:
            weights[:, FEATURE_INDEX[feature]] = float(value or 0.0)
    bias[:] = float(ra.ACTIVE_BASE_PROFILE.get('bias') or 0.0)
    if np.any(non_live):
        combo_count = ra.micro_local_count() - 1
        coordinate_index = ((raw_indexes[non_live].astype(np.int64) - 1) * 1_299_709 + 41_729) % combo_count
        parts = _unravel_many(coordinate_index, dims)
        bias_i = parts[0]
        core_indexes = parts[1:1 + len(core_names)]
        aux_i = parts[1 + len(core_names)]
        non_live_positions = np.where(non_live)[0]
        for name, delta_i in zip(core_names, core_indexes):
            if name in FEATURE_INDEX:
                base = float(ra.ACTIVE_BASE_WEIGHTS.get(name, 0.0) or 0.0)
                values = base + np.asarray(ra.MICRO_LOCAL_CORE_DELTAS[name], dtype=np.float64)[delta_i]
                weights[non_live_positions, FEATURE_INDEX[name]] = values
        for choice_idx, (_name, mapping) in enumerate(ra.MICRO_LOCAL_AUX_PACKS):
            mask = aux_i == choice_idx
            if not np.any(mask):
                continue
            positions = non_live_positions[mask]
            for feature, value in mapping.items():
                if feature in FEATURE_INDEX:
                    weights[positions, FEATURE_INDEX[feature]] = float(value or 0.0)
        bias[non_live_positions] = (
            float(ra.ACTIVE_BASE_PROFILE.get('bias') or 0.0)
            + np.asarray(ra.MICRO_LOCAL_BIASES, dtype=np.float64)[bias_i]
        )
    return weights, bias


def _replay_safe_local_matrix(raw_indexes: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    dims = [
        len(ra.REPLAY_SAFE_BIASES),
        len(ra.REPLAY_SAFE_MOVE_PACKS),
        len(ra.REPLAY_SAFE_MOVE_PACKS),
        len(ra.REPLAY_SAFE_MOVE_PACKS),
        len(ra.REPLAY_SAFE_AUX_PACKS),
    ]
    weights = np.zeros((len(raw_indexes), len(FEATURE_NAMES)), dtype=np.float64)
    bias = np.zeros(len(raw_indexes), dtype=np.float64)
    for feature, value in ra.ACTIVE_BASE_WEIGHTS.items():
        if feature in FEATURE_INDEX:
            weights[:, FEATURE_INDEX[feature]] = float(value or 0.0)
    bias[:] = float(ra.ACTIVE_BASE_PROFILE.get('bias') or 0.0)
    live_mask = raw_indexes.astype(np.int64) == 0
    non_live = ~live_mask
    if np.any(non_live):
        combo_count = ra.replay_safe_local_count() - 1
        coordinate_index = ((raw_indexes[non_live].astype(np.int64) - 1) * 911_111 + 27_182) % combo_count
        bias_i, move_a_i, move_b_i, move_c_i, aux_i = _unravel_many(coordinate_index, dims)
        non_live_positions = np.where(non_live)[0]
        for selector, choices in [
            (move_a_i, ra.REPLAY_SAFE_MOVE_PACKS),
            (move_b_i, ra.REPLAY_SAFE_MOVE_PACKS),
            (move_c_i, ra.REPLAY_SAFE_MOVE_PACKS),
            (aux_i, ra.REPLAY_SAFE_AUX_PACKS),
        ]:
            for choice_idx, (_name, mapping) in enumerate(choices):
                mask = selector == choice_idx
                if not np.any(mask):
                    continue
                positions = non_live_positions[mask]
                for feature, delta in mapping.items():
                    if feature in FEATURE_INDEX:
                        weights[positions, FEATURE_INDEX[feature]] += float(delta or 0.0)
        bias[non_live_positions] = (
            float(ra.ACTIVE_BASE_PROFILE.get('bias') or 0.0)
            + np.asarray(ra.REPLAY_SAFE_BIASES, dtype=np.float64)[bias_i]
        )
    return weights, bias


def _existing_matrix(raw_indexes: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    weights = np.zeros((len(raw_indexes), len(FEATURE_NAMES)), dtype=np.float64)
    bias = np.zeros(len(raw_indexes), dtype=np.float64)
    for row_idx, raw in enumerate(raw_indexes):
        variant = slow.VARIANTS[int(raw)]
        for feature, value in variant.weights.items():
            if feature in FEATURE_INDEX:
                weights[row_idx, FEATURE_INDEX[feature]] = float(value or 0.0)
        bias[row_idx] = float(variant.bias or 0.0)
    return weights, bias


def matrix_for_global_range(start_index: int, end_index: int, include_existing: bool,
                            include_broad_full: bool, include_v29_full: bool,
                            include_expanded_full: bool = False,
                            include_active_local: bool = False,
                            include_creative_full: bool = False,
                            include_core2_creative: bool = False,
                            include_core2_guarded: bool = False,
                            include_micro_local: bool = False,
                            include_replay_safe_local: bool = False) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    indexes = []
    weight_blocks = []
    bias_blocks = []
    start = int(start_index)
    end = int(end_index)
    offset = 0
    builders = {
        'existing': _existing_matrix,
        'broad_full': _broad_matrix,
        'v29_full': _v29_matrix,
        'expanded_full': _expanded_matrix,
        'active_local': _active_local_matrix,
        'creative_full': _creative_matrix,
        'core2_creative': _core2_creative_matrix,
        'core2_guarded': _core2_guarded_matrix,
        'micro_local': _micro_local_matrix,
        'replay_safe_local': _replay_safe_local_matrix,
    }
    for spec in ra.family_specs(include_existing, include_broad_full, include_v29_full, include_expanded_full, include_active_local, include_creative_full, include_core2_creative, include_core2_guarded, include_micro_local, include_replay_safe_local):
        family_start = offset
        family_end = offset + spec.count
        lo = max(start, family_start)
        hi = min(end, family_end)
        if lo < hi:
            raw = np.arange(lo - family_start, hi - family_start, dtype=np.int64)
            weights, bias = builders[spec.name](raw)
            weight_blocks.append(weights)
            bias_blocks.append(bias)
            indexes.append(np.arange(lo, hi, dtype=np.int64))
        offset = family_end
    if not weight_blocks:
        return (
            np.zeros((0, len(FEATURE_NAMES)), dtype=np.float64),
            np.zeros(0, dtype=np.float64),
            np.zeros(0, dtype=np.int64),
        )
    return np.vstack(weight_blocks), np.concatenate(bias_blocks), np.concatenate(indexes)


def variant_at_global_index(global_index: int, include_existing: bool, include_broad_full: bool,
                            include_v29_full: bool, include_expanded_full: bool = False,
                            include_active_local: bool = False,
                            include_creative_full: bool = False,
                            include_core2_creative: bool = False,
                            include_core2_guarded: bool = False,
                            include_micro_local: bool = False,
                            include_replay_safe_local: bool = False):
    return ra.variant_at(global_index, include_existing, include_broad_full, include_v29_full,
                         include_expanded_full, include_active_local, include_creative_full, include_core2_creative, include_core2_guarded, include_micro_local, include_replay_safe_local)
