"""Deterministic random access for scoring-variant families.

The streaming generator is convenient for small runs, but huge chunked runs
must not spend time replaying/skipping earlier variants. This module maps
global variant indexes directly to the existing built-in variant families.

It intentionally mirrors scoring_variant_lab_massive.py's current family
definitions and names. Keep the smoke parity tests passing whenever those
families change.
"""
from __future__ import annotations

import itertools
from dataclasses import dataclass

import scoring_variant_lab as slow
import scoring_variant_lab_fast as fast
import scoring_variant_lab_massive as massive
import active_engine_baseline


@dataclass(frozen=True)
class FamilySpec:
    name: str
    count: int


BROAD_ARCHETYPES = [
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
BROAD_FLOW_WEIGHTS = [-1.5, 0.0, 1.5, 3.0, 4.5]
BROAD_CHOP_WEIGHTS = [-3.0, -1.0, 0.75, 2.0, 4.0]
BROAD_EXEC_WEIGHTS = [-3.0, -1.5, -0.5, 0.0, 2.0]
BROAD_PHASE_BIASES = [
    ('phase_neutral', {}),
    ('open_fade', {'open_phase': 2.0}),
    ('open_follow', {'open_phase': -1.5}),
    ('midday_fade', {'midday_phase': 1.5}),
    ('midday_skeptic', {'midday_phase': -1.5}),
]
BROAD_SETUP_BIASES = [
    ('setup_neutral', {}),
    ('rel_skeptic', {'setup_btc_relative_strength': -1.5}),
    ('rel_chaser', {'setup_btc_relative_strength': 1.5}),
    ('breakout_pref', {'setup_momentum_breakout': 2.0}),
    ('pullback_skeptic', {'setup_trend_pullback': -1.25}),
]
BROAD_SIDE_TICKER_BIASES = [
    ('ticker_neutral', {}),
    ('riot_short_skeptic', {'riot_short_penalty': 3.0}),
    ('riot_reversal', {'riot_short_penalty': 2.0, 'riot_long_penalty': -1.0}),
    ('short_skeptic', {'bias': 0.75}),
    ('long_skeptic', {'bias': -0.75}),
]

V29_RELATIVE_WEIGHTS = [-0.75, -1.0, -1.25, -1.5, -1.75, -2.0, -2.5]
V29_VWAP_WEIGHTS = [-0.25, -0.75, -1.0, -1.5, -2.0]
V29_MOMENTUM_WEIGHTS = [0.25, 0.75, 1.0, 1.5]
V29_BTC_WEIGHTS = [0.25, 0.75, 1.0, 1.5, 2.0]
V29_FLOW_WEIGHTS = [0.0, 0.75, 1.5, 2.25]
V29_CHOP_WEIGHTS = [0.0, 0.75, 1.5, 2.5]
V29_EXEC_CHOICES = [-2.0, -1.0, -0.5, 0.5]
V29_EMA_CHOICES = [0.5, 1.0, 1.5]
V29_MINER_CHOICES = [0.0, 0.5, 1.0, 1.5]
V29_BURST_CHOICES = [-0.5, 0.0, 0.5, 1.0]
V29_EXTRAS = [
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


EXPANDED_ARCHETYPES = [
    ('v29_like', {'ema': 1.0, 'vwap': -1.0, 'momentum': 1.0, 'btc': 1.0, 'relative': -1.0, 'miner': 0.5, 'burst': 0.5, 'flow_contra': 1.0, 'btc_chop': 1.0, 'exec_penalty': -1.0}),
    ('trend_follow', {'ema': 1.8, 'vwap': 0.8, 'momentum': 1.6, 'btc': 1.2, 'relative': 0.7, 'miner': 0.7, 'burst': 1.0, 'flow_contra': -1.0, 'btc_chop': -1.5, 'exec_penalty': -0.8}),
    ('mean_revert', {'ema': -0.6, 'vwap': -1.8, 'momentum': -0.8, 'btc': -0.3, 'relative': -0.5, 'miner': 0.1, 'burst': -0.8, 'flow_contra': 2.0, 'btc_chop': 1.6, 'exec_penalty': -0.8}),
    ('btc_align', {'ema': 0.8, 'vwap': 0.2, 'momentum': 0.6, 'btc': 2.2, 'relative': 1.4, 'miner': 0.6, 'burst': 0.3, 'flow_contra': -0.5, 'btc_chop': -1.2, 'exec_penalty': -0.8}),
    ('anti_btc_rel', {'ema': 0.6, 'vwap': -0.4, 'momentum': 0.2, 'btc': -1.6, 'relative': -2.2, 'miner': 0.3, 'burst': 0.2, 'flow_contra': 1.0, 'btc_chop': 1.4, 'exec_penalty': -0.8}),
    ('flow_fade', {'ema': 0.4, 'vwap': -0.8, 'momentum': -0.3, 'btc': 0.2, 'relative': 0.2, 'miner': 0.2, 'burst': -0.6, 'flow_contra': 3.2, 'btc_chop': 1.0, 'exec_penalty': -0.8}),
    ('flow_follow', {'ema': 1.0, 'vwap': 0.5, 'momentum': 1.0, 'btc': 0.8, 'relative': 0.5, 'miner': 0.5, 'burst': 1.2, 'flow_contra': -2.0, 'btc_chop': -0.8, 'exec_penalty': -0.8}),
    ('burst_exhaust', {'ema': 0.2, 'vwap': -1.4, 'momentum': -1.2, 'btc': 0.0, 'relative': 0.0, 'miner': 0.2, 'burst': -3.0, 'flow_contra': 2.0, 'btc_chop': 1.2, 'exec_penalty': -0.8}),
    ('burst_breakout', {'ema': 1.3, 'vwap': 0.7, 'momentum': 1.8, 'btc': 1.0, 'relative': 0.7, 'miner': 0.7, 'burst': 3.0, 'flow_contra': -1.5, 'btc_chop': -1.4, 'exec_penalty': -0.8}),
    ('miner_confirm', {'ema': 0.9, 'vwap': 0.4, 'momentum': 0.9, 'btc': 1.0, 'relative': 0.7, 'miner': 3.0, 'burst': 0.4, 'flow_contra': -0.7, 'btc_chop': -0.8, 'exec_penalty': -0.8}),
    ('miner_inverse_chop', {'ema': 0.5, 'vwap': -0.9, 'momentum': 0.2, 'btc': 0.3, 'relative': 0.2, 'miner': -2.0, 'burst': 0.1, 'flow_contra': 1.2, 'btc_chop': 2.5, 'exec_penalty': -0.8}),
    ('execution_strict', {'ema': 1.0, 'vwap': 0.5, 'momentum': 0.8, 'btc': 1.0, 'relative': 0.5, 'miner': 0.5, 'burst': 0.5, 'flow_contra': 0.0, 'btc_chop': -0.5, 'exec_penalty': -3.0}),
    ('execution_flip', {'ema': 0.8, 'vwap': -0.2, 'momentum': 0.4, 'btc': 0.6, 'relative': 0.2, 'miner': 0.2, 'burst': 0.2, 'flow_contra': 1.0, 'btc_chop': 0.8, 'exec_penalty': 2.5}),
    ('open_reversal', {'ema': -0.4, 'vwap': -1.2, 'momentum': -0.8, 'btc': -0.2, 'relative': -0.2, 'miner': 0.0, 'burst': -1.0, 'flow_contra': 2.2, 'btc_chop': 1.0, 'exec_penalty': -0.8, 'open_phase': 2.0}),
    ('open_breakout', {'ema': 1.7, 'vwap': 0.9, 'momentum': 1.7, 'btc': 1.1, 'relative': 0.7, 'miner': 0.6, 'burst': 1.7, 'flow_contra': -1.5, 'btc_chop': -1.3, 'exec_penalty': -0.8, 'open_phase': -1.0}),
    ('midday_chop', {'ema': 0.2, 'vwap': -1.0, 'momentum': -0.3, 'btc': 0.0, 'relative': -0.2, 'miner': 0.0, 'burst': -0.4, 'flow_contra': 1.7, 'btc_chop': 2.2, 'exec_penalty': -0.8, 'midday_phase': 1.5}),
]
EXPANDED_SCALES = [0.65, 0.85, 1.0, 1.2, 1.45]
EXPANDED_BIASES = [-1.25, -0.5, 0.0, 0.5, 1.25]
EXPANDED_CORE_OVERRIDES = {
    'ema': [-1.0, -0.25, 0.5, 1.0, 1.75],
    'vwap': [-2.0, -1.0, -0.25, 0.5, 1.25],
    'momentum': [-1.5, -0.5, 0.25, 1.0, 1.75],
    'btc': [-2.0, -0.5, 0.25, 1.0, 2.0],
    'relative': [-2.5, -1.25, -0.25, 0.75, 2.0],
    'miner': [-2.0, 0.0, 0.5, 1.25, 3.0],
    'burst': [-3.0, -0.75, 0.0, 0.75, 2.5],
    'flow_contra': [-2.5, -0.5, 0.75, 2.0, 4.0],
    'btc_chop': [-3.0, -1.0, 0.5, 1.5, 3.5],
    'exec_penalty': [-3.5, -1.5, -0.5, 0.5, 2.5],
}
EXPANDED_PHASE_PACKS = [
    ('phase_neutral', {}),
    ('open_follow', {'open_phase': -1.5}),
    ('open_fade', {'open_phase': 2.0}),
    ('midday_follow', {'midday_phase': -1.25}),
    ('midday_fade', {'midday_phase': 1.75}),
]
EXPANDED_TICKER_PACKS = [
    ('ticker_neutral', {}),
    ('riot_short_skeptic', {'riot_short_penalty': 2.5}),
    ('riot_short_like', {'riot_short_penalty': -1.5}),
    ('riot_long_skeptic', {'riot_long_penalty': 2.0}),
    ('riot_reversal', {'riot_short_penalty': 2.0, 'riot_long_penalty': -1.0}),
]
EXPANDED_SETUP_PACKS = [
    ('setup_neutral', {}),
    ('btc_rel_skeptic', {'setup_btc_relative_strength': -2.0}),
    ('btc_rel_chase', {'setup_btc_relative_strength': 2.0}),
    ('breakout_follow', {'setup_momentum_breakout': 2.0}),
    ('breakout_fade', {'setup_momentum_breakout': -1.5}),
    ('pullback_follow', {'setup_trend_pullback': 1.5}),
    ('pullback_fade', {'setup_trend_pullback': -1.5}),
    ('flow_exhaust_fade', {'setup_flow_exhaustion_fade': 2.0}),
    ('flow_exhaust_follow', {'setup_flow_exhaustion_fade': -1.5}),
    ('vwap_reclaim_follow', {'setup_vwap_reclaim_breakdown': 2.0}),
    ('vwap_reclaim_fade', {'setup_vwap_reclaim_breakdown': -1.5}),
]
EXPANDED_EXTREME_PACKS = [
    ('ext_neutral', {}),
    ('vwap_ext_fade', {'vwap_sigma_ext': -1.0}),
    ('vwap_ext_follow', {'vwap_sigma_ext': 0.8}),
    ('btc_mom_fade', {'btc_mom_abs': -0.8}),
    ('btc_mom_follow', {'btc_mom_abs': 0.8}),
    ('flow_pressure_fade', {'flow_pressure_abs': -0.8}),
    ('flow_pressure_follow', {'flow_pressure_abs': 0.8}),
]


ACTIVE_BASE_PROFILE = active_engine_baseline.load_active_profile()
ACTIVE_BASE_WEIGHTS = dict(ACTIVE_BASE_PROFILE['weights'])
ACTIVE_CORE_NAMES = [
    'relative',
    'vwap',
    'btc_chop',
    'flow_contra',
    'miner',
    'setup_momentum_breakout',
    'ema',
    'momentum',
    'btc',
    'exec_penalty',
    'burst',
]
ACTIVE_DELTAS = [-1.75, -0.9, 0.0, 0.9, 1.75]
ACTIVE_SCALES = [0.65, 0.85, 1.0, 1.25, 1.6]
ACTIVE_BIASES = [-1.5, -0.75, 0.0, 0.75, 1.5]
ACTIVE_PHASE_PACKS = [
    ('phase_neutral', {}),
    ('open_follow', {'open_phase': -2.0}),
    ('open_fade', {'open_phase': 2.5}),
    ('midday_follow', {'midday_phase': -1.75}),
    ('midday_fade', {'midday_phase': 2.5}),
]
ACTIVE_TICKER_PACKS = [
    ('ticker_neutral', {}),
    ('riot_short_skeptic', {'riot_short_penalty': 2.5}),
    ('riot_short_strict', {'riot_short_penalty': 4.0}),
    ('riot_long_skeptic', {'riot_long_penalty': 2.5}),
    ('riot_reversal', {'riot_short_penalty': 3.0, 'riot_long_penalty': -2.0}),
]
ACTIVE_SETUP_PACKS = [
    ('setup_neutral', {}),
    ('btc_rel_skeptic', {'setup_btc_relative_strength': -2.25}),
    ('btc_rel_chase', {'setup_btc_relative_strength': 2.25}),
    ('breakout_more', {'setup_momentum_breakout': 1.75}),
    ('breakout_less', {'setup_momentum_breakout': -1.75}),
    ('pullback_follow', {'setup_trend_pullback': 2.0}),
    ('pullback_fade', {'setup_trend_pullback': -2.0}),
    ('flow_exhaust_fade', {'setup_flow_exhaustion_fade': 2.0}),
    ('flow_exhaust_follow', {'setup_flow_exhaustion_fade': -2.0}),
    ('vwap_reclaim_follow', {'setup_vwap_reclaim_breakdown': 2.0}),
    ('vwap_reclaim_fade', {'setup_vwap_reclaim_breakdown': -2.0}),
]
ACTIVE_EXTREME_PACKS = [
    ('ext_neutral', {}),
    ('vwap_ext_fade', {'vwap_sigma_ext': -1.75}),
    ('vwap_ext_follow', {'vwap_sigma_ext': 1.5}),
    ('btc_mom_fade', {'btc_mom_abs': -1.5}),
    ('btc_mom_follow', {'btc_mom_abs': 1.5}),
    ('flow_pressure_fade', {'flow_pressure_abs': -1.5}),
    ('flow_pressure_follow', {'flow_pressure_abs': 1.5}),
]

CORE2_ANCHOR_NAMES = ['relative', 'vwap']
CORE2_MUTABLE_NAMES = [name for name in ACTIVE_CORE_NAMES if name not in CORE2_ANCHOR_NAMES]
GUARDED_RELATIVE_WEIGHTS = [-2.25, -2.0, -1.75]
GUARDED_VWAP_WEIGHTS = [-1.5, -1.25]
GUARDED_CORE_CHOICES = {
    'btc': [1.0, 1.25, 1.5, 1.75],
    'momentum': [1.25, 1.5, 1.75, 2.0, 2.25],
    'btc_chop': [1.75, 2.0, 2.25, 2.5, 2.75],
    'setup_momentum_breakout': [1.75, 2.0, 2.25, 2.5, 2.75],
    'miner': [1.25, 1.5, 1.75, 2.0],
    'flow_contra': [1.0, 1.25, 1.5, 1.75],
    'ema': [0.75, 1.0, 1.25],
    'exec_penalty': [0.5, 0.75, 1.0, 1.25, 1.5],
    'burst': [0.0, 0.25, 0.5, 0.75],
}
GUARDED_BIASES = [-0.75, 0.0, 0.75]
GUARDED_PHASE_PACKS = [
    ('phase_neutral', {}),
    ('midday_soft', {'midday_phase': 0.75}),
    ('midday_medium', {'midday_phase': 1.25}),
    ('midday_firm', {'midday_phase': 1.75}),
    ('midday_strong', {'midday_phase': 2.0}),
    ('open_soft_fade', {'open_phase': 1.0}),
]
GUARDED_TICKER_PACKS = [
    ('ticker_neutral', {}),
    ('riot_short_skeptic_soft', {'riot_short_penalty': 1.0}),
    ('riot_short_skeptic', {'riot_short_penalty': 2.0}),
    ('riot_long_skeptic_soft', {'riot_long_penalty': 1.0}),
    ('riot_reversal_soft', {'riot_short_penalty': 1.75, 'riot_long_penalty': -0.75}),
]
GUARDED_SETUP_PACKS = [
    ('setup_neutral', {}),
    ('btc_rel_skeptic_soft', {'setup_btc_relative_strength': -1.0}),
    ('btc_rel_skeptic', {'setup_btc_relative_strength': -1.5}),
    ('pullback_fade_soft', {'setup_trend_pullback': -1.0}),
    ('vwap_reclaim_follow_soft', {'setup_vwap_reclaim_breakdown': 1.0}),
    ('vwap_reclaim_fade_soft', {'setup_vwap_reclaim_breakdown': -1.0}),
    ('flow_exhaust_fade_soft', {'setup_flow_exhaustion_fade': 1.25}),
]
GUARDED_EXTREME_PACKS = [
    ('ext_neutral', {}),
    ('vwap_ext_fade_soft', {'vwap_sigma_ext': -0.75}),
    ('btc_mom_follow_soft', {'btc_mom_abs': 0.75}),
    ('btc_mom_fade_soft', {'btc_mom_abs': -0.75}),
    ('flow_pressure_fade_soft', {'flow_pressure_abs': -0.75}),
    ('flow_pressure_follow_soft', {'flow_pressure_abs': 0.75}),
    ('btc_flow_converge_soft', {'btc_mom_abs': 0.75, 'flow_pressure_abs': 0.75}),
]

MICRO_LOCAL_CORE_DELTAS = {
    'relative': [-0.25, 0.0, 0.25],
    'vwap': [-0.25, 0.0, 0.25],
    'btc': [-0.25, 0.0, 0.25],
    'momentum': [-0.25, 0.0, 0.25],
    'setup_momentum_breakout': [-0.25, 0.0, 0.25],
    'ema': [-0.25, 0.0, 0.25],
    'exec_penalty': [-0.25, 0.0, 0.25],
    'burst': [-0.25, 0.0, 0.25],
    'btc_chop': [-0.125, 0.0, 0.125],
    'flow_contra': [-0.125, 0.0, 0.125],
    'miner': [-0.125, 0.0, 0.125],
}
MICRO_LOCAL_AUX_PACKS = [
    ('aux_neutral', {}),
    ('midday_tiny_fade', {'midday_phase': 0.25}),
    ('midday_tiny_follow', {'midday_phase': -0.25}),
    ('flow_exhaust_tiny_fade', {'setup_flow_exhaustion_fade': 0.25}),
    ('flow_exhaust_tiny_follow', {'setup_flow_exhaustion_fade': -0.25}),
    ('btc_mom_tiny_fade', {'btc_mom_abs': -0.125}),
    ('btc_mom_tiny_follow', {'btc_mom_abs': 0.125}),
    ('vwap_ext_tiny_fade', {'vwap_sigma_ext': -0.25}),
    ('flow_pressure_tiny_follow', {'flow_pressure_abs': 0.25}),
]
MICRO_LOCAL_BIASES = [-0.25, 0.0, 0.25]

REPLAY_SAFE_MOVE_PACKS = [
    ('none', {}),
    ('btc_dn_0125', {'btc': -0.125}),
    ('btc_up_0125', {'btc': 0.125}),
    ('btc_dn_0250', {'btc': -0.25}),
    ('btc_up_0250', {'btc': 0.25}),
    ('mom_dn_0125', {'momentum': -0.125}),
    ('mom_up_0125', {'momentum': 0.125}),
    ('mom_dn_0250', {'momentum': -0.25}),
    ('mom_up_0250', {'momentum': 0.25}),
    ('breakout_dn_0250', {'setup_momentum_breakout': -0.25}),
    ('breakout_up_0250', {'setup_momentum_breakout': 0.25}),
    ('breakout_dn_0500', {'setup_momentum_breakout': -0.5}),
    ('breakout_up_0500', {'setup_momentum_breakout': 0.5}),
    ('flow_exhaust_dn_0250', {'setup_flow_exhaustion_fade': -0.25}),
    ('flow_exhaust_up_0250', {'setup_flow_exhaustion_fade': 0.25}),
    ('ema_dn_0125', {'ema': -0.125}),
    ('ema_up_0125', {'ema': 0.125}),
    ('exec_dn_0125', {'exec_penalty': -0.125}),
    ('exec_up_0125', {'exec_penalty': 0.125}),
    ('vwap_dn_0125', {'vwap': -0.125}),
    ('vwap_up_0125', {'vwap': 0.125}),
    ('relative_dn_0125', {'relative': -0.125}),
    ('relative_up_0125', {'relative': 0.125}),
    ('btc_chop_dn_00625', {'btc_chop': -0.0625}),
    ('btc_chop_up_00625', {'btc_chop': 0.0625}),
    ('burst_dn_0125', {'burst': -0.125}),
    ('burst_up_0125', {'burst': 0.125}),
]
REPLAY_SAFE_AUX_PACKS = [
    ('aux_neutral', {}),
    ('midday_probe_dn_0125', {'midday_phase': -0.125}),
    ('midday_probe_up_0125', {'midday_phase': 0.125}),
    ('btc_mom_probe_dn_00625', {'btc_mom_abs': -0.0625}),
    ('btc_mom_probe_up_00625', {'btc_mom_abs': 0.0625}),
    ('vwap_ext_probe_dn_0125', {'vwap_sigma_ext': -0.125}),
    ('flow_pressure_probe_up_0125', {'flow_pressure_abs': 0.125}),
]
REPLAY_SAFE_BIASES = [-0.125, 0.0, 0.125]


CREATIVE_BASES = [
    ('active_hot', ACTIVE_BASE_WEIGHTS),
    ('v29_midday_fade', {'relative': -1.25, 'vwap': -0.75, 'momentum': 0.75, 'btc': 0.75, 'flow_contra': 0.75, 'btc_chop': 2.0, 'exec_penalty': 0.5, 'ema': 0.75, 'miner': 0.75, 'burst': 0.0, 'midday_phase': 1.5}),
    ('btc_stock_disagree', {'relative': -2.0, 'vwap': -0.5, 'momentum': 0.5, 'btc': 2.5, 'flow_contra': 0.25, 'btc_chop': 1.5, 'exec_penalty': 0.0, 'ema': 0.5, 'miner': 1.0, 'burst': 0.25}),
    ('stock_leads_btc', {'relative': 1.5, 'vwap': 0.75, 'momentum': 2.0, 'btc': -0.75, 'flow_contra': -0.5, 'btc_chop': -1.0, 'exec_penalty': -0.5, 'ema': 1.5, 'miner': 0.5, 'burst': 1.5}),
    ('flow_snapback', {'relative': -0.75, 'vwap': -1.5, 'momentum': -0.5, 'btc': 0.25, 'flow_contra': 3.5, 'btc_chop': 2.5, 'exec_penalty': 1.0, 'ema': -0.25, 'miner': 0.5, 'burst': -1.0}),
    ('execution_hard_filter', {'relative': -1.0, 'vwap': -0.5, 'momentum': 1.0, 'btc': 1.0, 'flow_contra': 1.0, 'btc_chop': 1.0, 'exec_penalty': -4.0, 'ema': 1.0, 'miner': 1.0, 'burst': 0.5}),
    ('riot_asymmetry', {'relative': -1.25, 'vwap': -1.0, 'momentum': 1.25, 'btc': 1.0, 'flow_contra': 0.75, 'btc_chop': 2.0, 'exec_penalty': 0.5, 'ema': 1.0, 'miner': 1.5, 'burst': 0.25, 'riot_short_penalty': 3.5, 'riot_long_penalty': -1.5}),
    ('open_impulse_fade', {'relative': -0.5, 'vwap': -2.25, 'momentum': -1.0, 'btc': 0.0, 'flow_contra': 2.5, 'btc_chop': 1.25, 'exec_penalty': 0.75, 'ema': -0.5, 'miner': 0.0, 'burst': -1.5, 'open_phase': 2.5}),
]
CREATIVE_CORE_NAMES = [
    'relative',
    'vwap',
    'momentum',
    'btc',
    'flow_contra',
    'btc_chop',
    'exec_penalty',
    'ema',
    'miner',
    'burst',
    'setup_momentum_breakout',
    'setup_trend_pullback',
]
CREATIVE_DELTAS = [-2.0, -1.25, -0.5, 0.0, 0.5, 1.25, 2.5]
CREATIVE_SCALES = [0.7, 0.9, 1.1, 1.35, 1.7]
CREATIVE_BIASES = [-2.0, -1.0, 0.0, 1.0, 2.0]
CREATIVE_PHASE_PACKS = [
    ('phase_neutral', {}),
    ('open_hard_follow', {'open_phase': -2.5}),
    ('open_hard_fade', {'open_phase': 3.0}),
    ('midday_hard_follow', {'midday_phase': -2.25}),
    ('midday_hard_fade', {'midday_phase': 2.75}),
    ('open_follow_midday_fade', {'open_phase': -1.5, 'midday_phase': 2.25}),
]
CREATIVE_TICKER_PACKS = [
    ('ticker_neutral', {}),
    ('riot_short_wall', {'riot_short_penalty': 4.0}),
    ('riot_short_invite', {'riot_short_penalty': -2.5}),
    ('riot_long_wall', {'riot_long_penalty': 3.0}),
    ('riot_reversal_plus', {'riot_short_penalty': 3.0, 'riot_long_penalty': -2.0}),
]
CREATIVE_SETUP_PACKS = [
    ('setup_neutral', {}),
    ('btc_rel_hard_skeptic', {'setup_btc_relative_strength': -3.0}),
    ('btc_rel_hard_chase', {'setup_btc_relative_strength': 3.0}),
    ('breakout_hard_follow', {'setup_momentum_breakout': 3.0}),
    ('breakout_hard_fade', {'setup_momentum_breakout': -2.5}),
    ('pullback_hard_follow', {'setup_trend_pullback': 2.5}),
    ('pullback_hard_fade', {'setup_trend_pullback': -2.5}),
    ('flow_exhaust_hard_fade', {'setup_flow_exhaustion_fade': 3.0}),
    ('flow_exhaust_hard_follow', {'setup_flow_exhaustion_fade': -2.5}),
    ('vwap_reclaim_hard_follow', {'setup_vwap_reclaim_breakdown': 3.0}),
    ('vwap_reclaim_hard_fade', {'setup_vwap_reclaim_breakdown': -2.5}),
]
CREATIVE_EXTREME_PACKS = [
    ('ext_neutral', {}),
    ('vwap_ext_hard_fade', {'vwap_sigma_ext': -2.0}),
    ('vwap_ext_hard_follow', {'vwap_sigma_ext': 1.75}),
    ('btc_mom_hard_fade', {'btc_mom_abs': -1.75}),
    ('btc_mom_hard_follow', {'btc_mom_abs': 1.75}),
    ('flow_pressure_hard_fade', {'flow_pressure_abs': -1.75}),
    ('flow_pressure_hard_follow', {'flow_pressure_abs': 1.75}),
    ('btc_flow_diverge', {'btc_mom_abs': 1.5, 'flow_pressure_abs': -1.5}),
    ('btc_flow_converge', {'btc_mom_abs': 1.5, 'flow_pressure_abs': 1.5}),
]


def _unravel(index: int, dims: list[int]) -> list[int]:
    out = [0] * len(dims)
    for pos in range(len(dims) - 1, -1, -1):
        out[pos] = index % dims[pos]
        index //= dims[pos]
    return out


def broad_count() -> int:
    return (
        len(BROAD_ARCHETYPES)
        * len(BROAD_FLOW_WEIGHTS)
        * len(BROAD_CHOP_WEIGHTS)
        * len(BROAD_EXEC_WEIGHTS)
        * len(BROAD_PHASE_BIASES)
        * len(BROAD_SETUP_BIASES)
        * len(BROAD_SIDE_TICKER_BIASES)
    )


def v29_count() -> int:
    return (
        len(V29_RELATIVE_WEIGHTS)
        * len(V29_VWAP_WEIGHTS)
        * len(V29_MOMENTUM_WEIGHTS)
        * len(V29_BTC_WEIGHTS)
        * len(V29_FLOW_WEIGHTS)
        * len(V29_CHOP_WEIGHTS)
        * len(V29_EXEC_CHOICES)
        * len(V29_EMA_CHOICES)
        * len(V29_MINER_CHOICES)
        * len(V29_BURST_CHOICES)
        * len(V29_EXTRAS)
    )


def expanded_count() -> int:
    return (
        len(EXPANDED_ARCHETYPES)
        * len(EXPANDED_SCALES)
        * len(EXPANDED_BIASES)
        * 5 ** len(EXPANDED_CORE_OVERRIDES)
        * len(EXPANDED_PHASE_PACKS)
        * len(EXPANDED_TICKER_PACKS)
        * len(EXPANDED_SETUP_PACKS)
        * len(EXPANDED_EXTREME_PACKS)
    )


def active_local_count() -> int:
    return (
        len(ACTIVE_SCALES)
        * len(ACTIVE_BIASES)
        * (len(ACTIVE_DELTAS) ** len(ACTIVE_CORE_NAMES))
        * len(ACTIVE_PHASE_PACKS)
        * len(ACTIVE_TICKER_PACKS)
        * len(ACTIVE_SETUP_PACKS)
        * len(ACTIVE_EXTREME_PACKS)
    )


def core2_creative_count() -> int:
    return (
        len(ACTIVE_SCALES)
        * len(ACTIVE_BIASES)
        * (len(CREATIVE_DELTAS) ** len(CORE2_MUTABLE_NAMES))
        * len(CREATIVE_PHASE_PACKS)
        * len(CREATIVE_TICKER_PACKS)
        * len(CREATIVE_SETUP_PACKS)
        * len(CREATIVE_EXTREME_PACKS)
    )


def core2_guarded_count() -> int:
    total = len(GUARDED_RELATIVE_WEIGHTS) * len(GUARDED_VWAP_WEIGHTS) * len(GUARDED_BIASES)
    for choices in GUARDED_CORE_CHOICES.values():
        total *= len(choices)
    total *= len(GUARDED_PHASE_PACKS)
    total *= len(GUARDED_TICKER_PACKS)
    total *= len(GUARDED_SETUP_PACKS)
    total *= len(GUARDED_EXTREME_PACKS)
    return total


def micro_local_count() -> int:
    combos = len(MICRO_LOCAL_BIASES) * len(MICRO_LOCAL_AUX_PACKS)
    for deltas in MICRO_LOCAL_CORE_DELTAS.values():
        combos *= len(deltas)
    return 1 + combos


def replay_safe_local_count() -> int:
    return 1 + (
        len(REPLAY_SAFE_BIASES)
        * len(REPLAY_SAFE_MOVE_PACKS)
        * len(REPLAY_SAFE_MOVE_PACKS)
        * len(REPLAY_SAFE_MOVE_PACKS)
        * len(REPLAY_SAFE_AUX_PACKS)
    )


def creative_count() -> int:
    return (
        len(CREATIVE_BASES)
        * len(CREATIVE_SCALES)
        * len(CREATIVE_BIASES)
        * (len(CREATIVE_DELTAS) ** len(CREATIVE_CORE_NAMES))
        * len(CREATIVE_PHASE_PACKS)
        * len(CREATIVE_TICKER_PACKS)
        * len(CREATIVE_SETUP_PACKS)
        * len(CREATIVE_EXTREME_PACKS)
    )


def family_specs(include_existing: bool, include_broad_full: bool, include_v29_full: bool,
                 include_expanded_full: bool = False,
                 include_active_local: bool = False,
                 include_creative_full: bool = False,
                 include_core2_creative: bool = False,
                 include_core2_guarded: bool = False,
                 include_micro_local: bool = False,
                 include_replay_safe_local: bool = False) -> list[FamilySpec]:
    specs = []
    if include_existing:
        specs.append(FamilySpec('existing', len(slow.VARIANTS)))
    if include_broad_full:
        specs.append(FamilySpec('broad_full', broad_count()))
    if include_v29_full:
        specs.append(FamilySpec('v29_full', v29_count()))
    if include_expanded_full:
        specs.append(FamilySpec('expanded_full', expanded_count()))
    if include_active_local:
        specs.append(FamilySpec('active_local', active_local_count()))
    if include_creative_full:
        specs.append(FamilySpec('creative_full', creative_count()))
    if include_core2_creative:
        specs.append(FamilySpec('core2_creative', core2_creative_count()))
    if include_core2_guarded:
        specs.append(FamilySpec('core2_guarded', core2_guarded_count()))
    if include_micro_local:
        specs.append(FamilySpec('micro_local', micro_local_count()))
    if include_replay_safe_local:
        specs.append(FamilySpec('replay_safe_local', replay_safe_local_count()))
    return specs


def estimated_count(include_existing: bool, include_broad_full: bool, include_v29_full: bool,
                    include_expanded_full: bool = False,
                    include_active_local: bool = False,
                    include_creative_full: bool = False,
                    include_core2_creative: bool = False,
                    include_core2_guarded: bool = False,
                    include_micro_local: bool = False,
                    include_replay_safe_local: bool = False) -> dict:
    specs = family_specs(include_existing, include_broad_full, include_v29_full, include_expanded_full, include_active_local, include_creative_full, include_core2_creative, include_core2_guarded, include_micro_local, include_replay_safe_local)
    return {
        'existing_current': len(slow.VARIANTS) if include_existing else 0,
        'broad_full_raw': broad_count() if include_broad_full else 0,
        'v29_full_raw': v29_count() if include_v29_full else 0,
        'expanded_full_raw': expanded_count() if include_expanded_full else 0,
        'active_local_raw': active_local_count() if include_active_local else 0,
        'creative_full_raw': creative_count() if include_creative_full else 0,
        'core2_creative_raw': core2_creative_count() if include_core2_creative else 0,
        'core2_guarded_raw': core2_guarded_count() if include_core2_guarded else 0,
        'micro_local_raw': micro_local_count() if include_micro_local else 0,
        'replay_safe_local_raw': replay_safe_local_count() if include_replay_safe_local else 0,
        'raw_total_before_dedup': sum(spec.count for spec in specs),
        'random_access': True,
        'feature_names': list(fast.FEATURE_NAMES),
        'family_specs': [spec.__dict__ for spec in specs],
    }


def broad_variant_at(raw_index: int) -> slow.Variant:
    dims = [
        len(BROAD_ARCHETYPES),
        len(BROAD_FLOW_WEIGHTS),
        len(BROAD_CHOP_WEIGHTS),
        len(BROAD_EXEC_WEIGHTS),
        len(BROAD_PHASE_BIASES),
        len(BROAD_SETUP_BIASES),
        len(BROAD_SIDE_TICKER_BIASES),
    ]
    ai, fi, ci, ei, pi, si, ti = _unravel(raw_index, dims)
    aname, base = BROAD_ARCHETYPES[ai]
    flow_w = BROAD_FLOW_WEIGHTS[fi]
    chop_w = BROAD_CHOP_WEIGHTS[ci]
    exec_w = BROAD_EXEC_WEIGHTS[ei]
    phase_name, phase_w = BROAD_PHASE_BIASES[pi]
    setup_name, setup_w = BROAD_SETUP_BIASES[si]
    ticker_name, ticker_w = BROAD_SIDE_TICKER_BIASES[ti]
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
    idx = raw_index + 1
    return slow.Variant(
        f'broad_full_{idx:06d}_{aname}_{phase_name}_{setup_name}_{ticker_name}_f{fi}_c{ci}_e{ei}_p{pi}_s{si}_t{ti}',
        weights,
        bias,
    )


def v29_variant_at(raw_index: int) -> slow.Variant:
    dims = [
        len(V29_RELATIVE_WEIGHTS),
        len(V29_VWAP_WEIGHTS),
        len(V29_MOMENTUM_WEIGHTS),
        len(V29_BTC_WEIGHTS),
        len(V29_FLOW_WEIGHTS),
        len(V29_CHOP_WEIGHTS),
        len(V29_EXEC_CHOICES),
        len(V29_EMA_CHOICES),
        len(V29_MINER_CHOICES),
        len(V29_BURST_CHOICES),
        len(V29_EXTRAS),
    ]
    ri, vi, mi, bi, fi, ci, ei, emi, mini, bui, xi = _unravel(raw_index, dims)
    extra_name, extra = V29_EXTRAS[xi]
    weights = {
        'relative': V29_RELATIVE_WEIGHTS[ri],
        'vwap': V29_VWAP_WEIGHTS[vi],
        'momentum': V29_MOMENTUM_WEIGHTS[mi],
        'btc': V29_BTC_WEIGHTS[bi],
        'flow_contra': V29_FLOW_WEIGHTS[fi],
        'btc_chop': V29_CHOP_WEIGHTS[ci],
        'exec_penalty': V29_EXEC_CHOICES[ei],
        'ema': V29_EMA_CHOICES[emi],
        'miner': V29_MINER_CHOICES[mini],
        'burst': V29_BURST_CHOICES[bui],
    }
    weights.update(extra)
    idx = raw_index + 1
    return slow.Variant(
        f'v29_full_{idx:08d}_rel{ri}_vw{vi}_mom{mi}_btc{bi}_flow{fi}_chop{ci}_exec{ei}_ema{emi}_miner{mini}_burst{bui}_{extra_name}',
        weights,
    )


def expanded_variant_at(raw_index: int) -> slow.Variant:
    core_names = list(EXPANDED_CORE_OVERRIDES)
    dims = [
        len(EXPANDED_ARCHETYPES),
        len(EXPANDED_SCALES),
        len(EXPANDED_BIASES),
        *([5] * len(core_names)),
        len(EXPANDED_PHASE_PACKS),
        len(EXPANDED_TICKER_PACKS),
        len(EXPANDED_SETUP_PACKS),
        len(EXPANDED_EXTREME_PACKS),
    ]
    parts = _unravel(raw_index, dims)
    ai, si, bi = parts[:3]
    core_indexes = parts[3:3 + len(core_names)]
    phase_i, ticker_i, setup_i, extreme_i = parts[3 + len(core_names):]
    archetype_name, base = EXPANDED_ARCHETYPES[ai]
    scale = EXPANDED_SCALES[si]
    weights = {name: round(float(value) * scale, 6) for name, value in base.items()}
    for name, choice_i in zip(core_names, core_indexes):
        weights[name] = float(EXPANDED_CORE_OVERRIDES[name][choice_i])
    phase_name, phase_w = EXPANDED_PHASE_PACKS[phase_i]
    ticker_name, ticker_w = EXPANDED_TICKER_PACKS[ticker_i]
    setup_name, setup_w = EXPANDED_SETUP_PACKS[setup_i]
    extreme_name, extreme_w = EXPANDED_EXTREME_PACKS[extreme_i]
    weights.update(phase_w)
    weights.update(ticker_w)
    weights.update(setup_w)
    weights.update(extreme_w)
    for feature_name in fast.FEATURE_NAMES:
        weights.setdefault(feature_name, 0.0)
    bias = float(EXPANDED_BIASES[bi])
    idx = raw_index + 1
    core_tag = ''.join(str(v) for v in core_indexes)
    return slow.Variant(
        (
            f'expanded_full_{idx:012d}_{archetype_name}_{phase_name}_{ticker_name}_'
            f'{setup_name}_{extreme_name}_s{si}_b{bi}_c{core_tag}'
        ),
        weights,
        bias,
    )


def active_local_variant_at(raw_index: int) -> slow.Variant:
    dims = [
        len(ACTIVE_SCALES),
        len(ACTIVE_BIASES),
        *([len(ACTIVE_DELTAS)] * len(ACTIVE_CORE_NAMES)),
        len(ACTIVE_PHASE_PACKS),
        len(ACTIVE_TICKER_PACKS),
        len(ACTIVE_SETUP_PACKS),
        len(ACTIVE_EXTREME_PACKS),
    ]
    # Scramble indexes so bounded searches (for example the first 100M) spread
    # across every active-local dimension instead of only the fastest axes.
    coordinate_index = (int(raw_index) * 1_000_003 + 97_531) % active_local_count()
    parts = _unravel(coordinate_index, dims)
    scale_i, bias_i = parts[:2]
    core_indexes = parts[2:2 + len(ACTIVE_CORE_NAMES)]
    phase_i, ticker_i, setup_i, extreme_i = parts[2 + len(ACTIVE_CORE_NAMES):]
    scale = ACTIVE_SCALES[scale_i]
    weights = {name: round(float(value) * scale, 6) for name, value in ACTIVE_BASE_WEIGHTS.items()}
    for name, delta_i in zip(ACTIVE_CORE_NAMES, core_indexes):
        weights[name] = round(float(ACTIVE_BASE_WEIGHTS.get(name, 0.0)) + ACTIVE_DELTAS[delta_i], 6)
    phase_name, phase_w = ACTIVE_PHASE_PACKS[phase_i]
    ticker_name, ticker_w = ACTIVE_TICKER_PACKS[ticker_i]
    setup_name, setup_w = ACTIVE_SETUP_PACKS[setup_i]
    extreme_name, extreme_w = ACTIVE_EXTREME_PACKS[extreme_i]
    weights.update(phase_w)
    weights.update(ticker_w)
    weights.update(setup_w)
    weights.update(extreme_w)
    for feature_name in fast.FEATURE_NAMES:
        weights.setdefault(feature_name, 0.0)
    bias = float(ACTIVE_BIASES[bias_i])
    idx = raw_index + 1
    core_tag = ''.join(str(v) for v in core_indexes)
    return slow.Variant(
        (
            f'active_local_{idx:012d}_{phase_name}_{ticker_name}_{setup_name}_'
            f'{extreme_name}_s{scale_i}_b{bias_i}_c{core_tag}'
        ),
        weights,
        bias,
    )


def creative_variant_at(raw_index: int) -> slow.Variant:
    dims = [
        len(CREATIVE_BASES),
        len(CREATIVE_SCALES),
        len(CREATIVE_BIASES),
        *([len(CREATIVE_DELTAS)] * len(CREATIVE_CORE_NAMES)),
        len(CREATIVE_PHASE_PACKS),
        len(CREATIVE_TICKER_PACKS),
        len(CREATIVE_SETUP_PACKS),
        len(CREATIVE_EXTREME_PACKS),
    ]
    coordinate_index = (int(raw_index) * 9_176_191 + 271_828) % creative_count()
    parts = _unravel(coordinate_index, dims)
    base_i, scale_i, bias_i = parts[:3]
    core_indexes = parts[3:3 + len(CREATIVE_CORE_NAMES)]
    phase_i, ticker_i, setup_i, extreme_i = parts[3 + len(CREATIVE_CORE_NAMES):]
    base_name, base = CREATIVE_BASES[base_i]
    scale = CREATIVE_SCALES[scale_i]
    weights = {name: round(float(value or 0.0) * scale, 6) for name, value in base.items()}
    for name, delta_i in zip(CREATIVE_CORE_NAMES, core_indexes):
        weights[name] = round(float(weights.get(name, 0.0) or 0.0) + CREATIVE_DELTAS[delta_i], 6)
    phase_name, phase_w = CREATIVE_PHASE_PACKS[phase_i]
    ticker_name, ticker_w = CREATIVE_TICKER_PACKS[ticker_i]
    setup_name, setup_w = CREATIVE_SETUP_PACKS[setup_i]
    extreme_name, extreme_w = CREATIVE_EXTREME_PACKS[extreme_i]
    weights.update(phase_w)
    weights.update(ticker_w)
    weights.update(setup_w)
    weights.update(extreme_w)
    for feature_name in fast.FEATURE_NAMES:
        weights.setdefault(feature_name, 0.0)
    bias = float(CREATIVE_BIASES[bias_i])
    idx = raw_index + 1
    core_tag = ''.join(str(v) for v in core_indexes)
    return slow.Variant(
        (
            f'creative_full_{idx:012d}_{base_name}_{phase_name}_{ticker_name}_'
            f'{setup_name}_{extreme_name}_s{scale_i}_b{bias_i}_c{core_tag}'
        ),
        weights,
        bias,
    )


def core2_creative_variant_at(raw_index: int) -> slow.Variant:
    dims = [
        len(ACTIVE_SCALES),
        len(ACTIVE_BIASES),
        *([len(CREATIVE_DELTAS)] * len(CORE2_MUTABLE_NAMES)),
        len(CREATIVE_PHASE_PACKS),
        len(CREATIVE_TICKER_PACKS),
        len(CREATIVE_SETUP_PACKS),
        len(CREATIVE_EXTREME_PACKS),
    ]
    coordinate_index = (int(raw_index) * 6_700_417 + 314_159) % core2_creative_count()
    parts = _unravel(coordinate_index, dims)
    scale_i, bias_i = parts[:2]
    core_indexes = parts[2:2 + len(CORE2_MUTABLE_NAMES)]
    phase_i, ticker_i, setup_i, extreme_i = parts[2 + len(CORE2_MUTABLE_NAMES):]
    scale = ACTIVE_SCALES[scale_i]
    weights = {name: round(float(value or 0.0) * scale, 6) for name, value in ACTIVE_BASE_WEIGHTS.items()}
    for name in CORE2_ANCHOR_NAMES:
        weights[name] = round(float(ACTIVE_BASE_WEIGHTS.get(name, 0.0) or 0.0), 6)
    for name, delta_i in zip(CORE2_MUTABLE_NAMES, core_indexes):
        weights[name] = round(float(weights.get(name, 0.0) or 0.0) + CREATIVE_DELTAS[delta_i], 6)
    phase_name, phase_w = CREATIVE_PHASE_PACKS[phase_i]
    ticker_name, ticker_w = CREATIVE_TICKER_PACKS[ticker_i]
    setup_name, setup_w = CREATIVE_SETUP_PACKS[setup_i]
    extreme_name, extreme_w = CREATIVE_EXTREME_PACKS[extreme_i]
    weights.update(phase_w)
    ticker_bias = dict(ticker_w)
    weights.update(ticker_bias)
    weights.update(setup_w)
    weights.update(extreme_w)
    for name in CORE2_ANCHOR_NAMES:
        weights[name] = round(float(ACTIVE_BASE_WEIGHTS.get(name, 0.0) or 0.0), 6)
    for feature_name in fast.FEATURE_NAMES:
        weights.setdefault(feature_name, 0.0)
    bias = float(CREATIVE_BIASES[bias_i])
    idx = raw_index + 1
    core_tag = ''.join(str(v) for v in core_indexes)
    return slow.Variant(
        (
            f'core2_creative_{idx:012d}_{phase_name}_{ticker_name}_{setup_name}_'
            f'{extreme_name}_s{scale_i}_b{bias_i}_c{core_tag}'
        ),
        weights,
        bias,
    )


def core2_guarded_variant_at(raw_index: int) -> slow.Variant:
    core_names = list(GUARDED_CORE_CHOICES)
    dims = [
        len(GUARDED_RELATIVE_WEIGHTS),
        len(GUARDED_VWAP_WEIGHTS),
        len(GUARDED_BIASES),
        *[len(GUARDED_CORE_CHOICES[name]) for name in core_names],
        len(GUARDED_PHASE_PACKS),
        len(GUARDED_TICKER_PACKS),
        len(GUARDED_SETUP_PACKS),
        len(GUARDED_EXTREME_PACKS),
    ]
    coordinate_index = (int(raw_index) * 5_430_131 + 618_033) % core2_guarded_count()
    parts = _unravel(coordinate_index, dims)
    rel_i, vwap_i, bias_i = parts[:3]
    core_indexes = parts[3:3 + len(core_names)]
    phase_i, ticker_i, setup_i, extreme_i = parts[3 + len(core_names):]
    weights = {name: 0.0 for name in fast.FEATURE_NAMES}
    weights.update({
        'relative': GUARDED_RELATIVE_WEIGHTS[rel_i],
        'vwap': GUARDED_VWAP_WEIGHTS[vwap_i],
    })
    for name, choice_i in zip(core_names, core_indexes):
        weights[name] = float(GUARDED_CORE_CHOICES[name][choice_i])
    phase_name, phase_w = GUARDED_PHASE_PACKS[phase_i]
    ticker_name, ticker_w = GUARDED_TICKER_PACKS[ticker_i]
    setup_name, setup_w = GUARDED_SETUP_PACKS[setup_i]
    extreme_name, extreme_w = GUARDED_EXTREME_PACKS[extreme_i]
    weights.update(phase_w)
    weights.update(ticker_w)
    weights.update(setup_w)
    weights.update(extreme_w)
    bias = float(GUARDED_BIASES[bias_i])
    idx = raw_index + 1
    core_tag = ''.join(str(v) for v in [rel_i, vwap_i, *core_indexes])
    return slow.Variant(
        (
            f'core2_guarded_{idx:012d}_{phase_name}_{ticker_name}_{setup_name}_'
            f'{extreme_name}_b{bias_i}_c{core_tag}'
        ),
        weights,
        bias,
    )


def micro_local_variant_at(raw_index: int) -> slow.Variant:
    if int(raw_index) == 0:
        weights = {name: 0.0 for name in fast.FEATURE_NAMES}
        weights.update({name: float(value or 0.0) for name, value in ACTIVE_BASE_WEIGHTS.items()})
        return slow.Variant('micro_local_000000000000_live_exact', weights, float(ACTIVE_BASE_PROFILE.get('bias') or 0.0))

    core_names = list(MICRO_LOCAL_CORE_DELTAS)
    dims = [
        len(MICRO_LOCAL_BIASES),
        *[len(MICRO_LOCAL_CORE_DELTAS[name]) for name in core_names],
        len(MICRO_LOCAL_AUX_PACKS),
    ]
    combo_count = micro_local_count() - 1
    coordinate_index = ((int(raw_index) - 1) * 1_299_709 + 41_729) % combo_count
    parts = _unravel(coordinate_index, dims)
    bias_i = parts[0]
    core_indexes = parts[1:1 + len(core_names)]
    aux_i = parts[1 + len(core_names)]
    weights = {name: 0.0 for name in fast.FEATURE_NAMES}
    weights.update({name: float(value or 0.0) for name, value in ACTIVE_BASE_WEIGHTS.items()})
    for name, delta_i in zip(core_names, core_indexes):
        weights[name] = round(float(ACTIVE_BASE_WEIGHTS.get(name, 0.0) or 0.0) + MICRO_LOCAL_CORE_DELTAS[name][delta_i], 6)
    aux_name, aux_w = MICRO_LOCAL_AUX_PACKS[aux_i]
    weights.update(aux_w)
    bias = round(float(ACTIVE_BASE_PROFILE.get('bias') or 0.0) + MICRO_LOCAL_BIASES[bias_i], 6)
    idx = raw_index + 1
    core_tag = ''.join(str(v) for v in core_indexes)
    return slow.Variant(
        f'micro_local_{idx:012d}_{aux_name}_b{bias_i}_c{core_tag}',
        weights,
        bias,
    )


def _add_weight_delta(weights: dict, mapping: dict) -> None:
    for name, delta in mapping.items():
        weights[name] = round(float(weights.get(name, 0.0) or 0.0) + float(delta or 0.0), 6)


def replay_safe_local_variant_at(raw_index: int) -> slow.Variant:
    if int(raw_index) == 0:
        weights = {name: 0.0 for name in fast.FEATURE_NAMES}
        weights.update({name: float(value or 0.0) for name, value in ACTIVE_BASE_WEIGHTS.items()})
        return slow.Variant('replay_safe_local_000000000000_live_exact', weights, float(ACTIVE_BASE_PROFILE.get('bias') or 0.0))

    dims = [
        len(REPLAY_SAFE_BIASES),
        len(REPLAY_SAFE_MOVE_PACKS),
        len(REPLAY_SAFE_MOVE_PACKS),
        len(REPLAY_SAFE_MOVE_PACKS),
        len(REPLAY_SAFE_AUX_PACKS),
    ]
    combo_count = replay_safe_local_count() - 1
    coordinate_index = ((int(raw_index) - 1) * 911_111 + 27_182) % combo_count
    bias_i, move_a_i, move_b_i, move_c_i, aux_i = _unravel(coordinate_index, dims)
    weights = {name: 0.0 for name in fast.FEATURE_NAMES}
    weights.update({name: float(value or 0.0) for name, value in ACTIVE_BASE_WEIGHTS.items()})
    move_names = []
    for move_i in (move_a_i, move_b_i, move_c_i):
        move_name, move_w = REPLAY_SAFE_MOVE_PACKS[move_i]
        move_names.append(move_name)
        _add_weight_delta(weights, move_w)
    aux_name, aux_w = REPLAY_SAFE_AUX_PACKS[aux_i]
    _add_weight_delta(weights, aux_w)
    bias = round(float(ACTIVE_BASE_PROFILE.get('bias') or 0.0) + REPLAY_SAFE_BIASES[bias_i], 6)
    idx = raw_index + 1
    return slow.Variant(
        (
            f'replay_safe_local_{idx:012d}_{move_names[0]}_{move_names[1]}_'
            f'{move_names[2]}_{aux_name}_b{bias_i}'
        ),
        weights,
        bias,
    )


def variant_at(global_index: int, include_existing: bool, include_broad_full: bool,
               include_v29_full: bool, include_expanded_full: bool = False,
               include_active_local: bool = False,
               include_creative_full: bool = False,
               include_core2_creative: bool = False,
               include_core2_guarded: bool = False,
               include_micro_local: bool = False,
               include_replay_safe_local: bool = False) -> slow.Variant:
    idx = int(global_index)
    for spec in family_specs(include_existing, include_broad_full, include_v29_full, include_expanded_full, include_active_local, include_creative_full, include_core2_creative, include_core2_guarded, include_micro_local, include_replay_safe_local):
        if idx >= spec.count:
            idx -= spec.count
            continue
        if spec.name == 'existing':
            return slow.Variant(slow.VARIANTS[idx].name, dict(slow.VARIANTS[idx].weights), slow.VARIANTS[idx].bias)
        if spec.name == 'broad_full':
            return broad_variant_at(idx)
        if spec.name == 'v29_full':
            return v29_variant_at(idx)
        if spec.name == 'expanded_full':
            return expanded_variant_at(idx)
        if spec.name == 'active_local':
            return active_local_variant_at(idx)
        if spec.name == 'creative_full':
            return creative_variant_at(idx)
        if spec.name == 'core2_creative':
            return core2_creative_variant_at(idx)
        if spec.name == 'core2_guarded':
            return core2_guarded_variant_at(idx)
        if spec.name == 'micro_local':
            return micro_local_variant_at(idx)
        if spec.name == 'replay_safe_local':
            return replay_safe_local_variant_at(idx)
    raise IndexError(global_index)


def variant_range(start_index: int, end_index: int, include_existing: bool,
                  include_broad_full: bool, include_v29_full: bool,
                  include_expanded_full: bool = False,
                  include_active_local: bool = False,
                  include_creative_full: bool = False,
                  include_core2_creative: bool = False,
                  include_core2_guarded: bool = False,
                  include_micro_local: bool = False,
                  include_replay_safe_local: bool = False) -> list[slow.Variant]:
    return [
        variant_at(i, include_existing, include_broad_full, include_v29_full, include_expanded_full, include_active_local, include_creative_full, include_core2_creative, include_core2_guarded, include_micro_local, include_replay_safe_local)
        for i in range(int(start_index), int(end_index))
    ]


def parity_check(limit: int = 1000) -> dict:
    stream = itertools.islice(massive.variant_stream(True, True, True), limit)
    mismatches = []
    for idx, streamed in enumerate(stream):
        direct = variant_at(idx, True, True, True)
        if streamed.name != direct.name or streamed.weights != direct.weights or streamed.bias != direct.bias:
            mismatches.append({
                'index': idx,
                'streamed': streamed.name,
                'direct': direct.name,
            })
            break
    return {'ok': not mismatches, 'checked': limit, 'mismatches': mismatches}
