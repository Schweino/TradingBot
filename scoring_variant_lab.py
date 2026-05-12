"""
Hypothetical scoring-variant lab for the SIP engine replay.

This does not import or modify the live trading engine. It reads an existing
replay trades CSV and re-scores the same entry opportunities with alternate
indicator weights. If a variant scores the opposite side, the trade P/L is
mechanically inverted as a proxy for "same entry, opposite direction."

Use this as a fast research pass. A final candidate should still be validated
by a full replay harness with raw indicators.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
from dataclasses import dataclass
from typing import Any


HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CSV = os.path.join(
    HERE,
    'postmortem',
    'backtests',
    'engine_replay_2026-04-06_2026-04-13_trades.csv',
)
DEFAULT_OUT = os.path.join(
    HERE,
    'postmortem',
    'backtests',
    'scoring_variant_lab_2026-04-06_2026-04-13.json',
)


@dataclass(frozen=True)
class Variant:
    name: str
    weights: dict[str, float]
    bias: float = 0.0


BASE_VARIANTS = [
    Variant('v01_baseline_reason_proxy', {
        'ema': 2.0, 'vwap': 1.0, 'momentum': 1.0, 'btc': 2.0,
        'relative': 1.0, 'miner': 1.0, 'burst': 1.0,
        'flow_contra': 0.0, 'btc_chop': -1.0, 'exec_penalty': -1.0,
    }),
    Variant('v02_momentum_first', {
        'ema': 1.0, 'vwap': 0.5, 'momentum': 2.5, 'btc': 1.0,
        'relative': 1.0, 'miner': 0.5, 'burst': 1.5,
        'flow_contra': -1.0, 'btc_chop': -1.5, 'exec_penalty': -1.0,
    }),
    Variant('v03_btc_heavy', {
        'ema': 1.0, 'vwap': 0.5, 'momentum': 1.0, 'btc': 3.0,
        'relative': 2.0, 'miner': 1.0, 'burst': 0.5,
        'flow_contra': -0.5, 'btc_chop': -2.0, 'exec_penalty': -1.0,
    }),
    Variant('v04_flow_contrarian', {
        'ema': 1.0, 'vwap': 0.5, 'momentum': 1.0, 'btc': 1.0,
        'relative': 1.0, 'miner': 0.5, 'burst': 0.5,
        'flow_contra': 3.0, 'btc_chop': -0.5, 'exec_penalty': -1.0,
    }),
    Variant('v05_chop_fade', {
        'ema': 1.0, 'vwap': 0.5, 'momentum': 0.5, 'btc': 1.0,
        'relative': 0.5, 'miner': 0.5, 'burst': 0.5,
        'flow_contra': 1.0, 'btc_chop': 2.0, 'exec_penalty': -1.0,
    }),
    Variant('v06_vwap_reversion', {
        'ema': 1.0, 'vwap': -2.0, 'momentum': 0.5, 'btc': 1.0,
        'relative': 0.5, 'miner': 0.5, 'burst': 0.5,
        'flow_contra': 1.0, 'btc_chop': 1.0, 'exec_penalty': -1.0,
    }),
    Variant('v07_strong_trend_only', {
        'ema': 3.0, 'vwap': 1.5, 'momentum': 1.5, 'btc': 1.5,
        'relative': 1.0, 'miner': 1.0, 'burst': 1.0,
        'flow_contra': -2.0, 'btc_chop': -2.0, 'exec_penalty': -1.5,
    }),
    Variant('v08_relative_strength_heavy', {
        'ema': 1.0, 'vwap': 0.5, 'momentum': 1.0, 'btc': 1.0,
        'relative': 3.0, 'miner': 1.0, 'burst': 0.5,
        'flow_contra': -0.5, 'btc_chop': -1.5, 'exec_penalty': -1.0,
    }),
    Variant('v09_miner_basket_heavy', {
        'ema': 1.0, 'vwap': 0.5, 'momentum': 1.0, 'btc': 1.0,
        'relative': 1.0, 'miner': 3.0, 'burst': 0.5,
        'flow_contra': -0.5, 'btc_chop': -1.0, 'exec_penalty': -1.0,
    }),
    Variant('v10_execution_quality_heavy', {
        'ema': 1.5, 'vwap': 1.0, 'momentum': 1.0, 'btc': 1.5,
        'relative': 1.0, 'miner': 1.0, 'burst': 0.5,
        'flow_contra': -1.0, 'btc_chop': -1.0, 'exec_penalty': -3.0,
    }),
    Variant('v11_open_volatility_fade', {
        'ema': 1.0, 'vwap': -1.0, 'momentum': -1.0, 'btc': 0.5,
        'relative': 0.5, 'miner': 0.5, 'burst': -1.5,
        'flow_contra': 2.0, 'btc_chop': 1.0, 'exec_penalty': -1.0,
        'open_phase': -1.0,
    }),
    Variant('v12_breakout_confirmed', {
        'ema': 2.0, 'vwap': 1.0, 'momentum': 2.0, 'btc': 1.5,
        'relative': 1.0, 'miner': 1.0, 'burst': 2.0,
        'flow_contra': -2.0, 'btc_chop': -2.0, 'exec_penalty': -1.0,
    }),
    Variant('v13_anti_stale_relative_strength', {
        'ema': 1.0, 'vwap': 0.5, 'momentum': 1.5, 'btc': 1.0,
        'relative': 0.0, 'miner': 0.5, 'burst': 1.0,
        'flow_contra': 1.0, 'btc_chop': -2.0, 'exec_penalty': -1.0,
    }),
    Variant('v14_riot_short_skeptic', {
        'ema': 2.0, 'vwap': 1.0, 'momentum': 1.0, 'btc': 2.0,
        'relative': 1.0, 'miner': 1.0, 'burst': 0.5,
        'flow_contra': -1.0, 'btc_chop': -2.0, 'exec_penalty': -1.0,
        'riot_short_penalty': 4.0,
    }),
    Variant('v15_counter_signal_hunter', {
        'ema': -1.0, 'vwap': -1.0, 'momentum': -1.5, 'btc': -1.0,
        'relative': -1.0, 'miner': -0.5, 'burst': -0.5,
        'flow_contra': 2.0, 'btc_chop': 2.0, 'exec_penalty': -0.5,
    }),
]


EXTRA_VARIANTS = [
    Variant('v16_vwap_revert_flow_confirm', {
        'ema': 0.5, 'vwap': -2.0, 'momentum': 0.5, 'btc': 0.75,
        'relative': 0.5, 'miner': 0.5, 'burst': 0.25,
        'flow_contra': 2.0, 'btc_chop': 1.0, 'exec_penalty': -1.0,
    }),
    Variant('v17_vwap_revert_no_btc', {
        'ema': 0.75, 'vwap': -2.5, 'momentum': 0.5, 'btc': 0.0,
        'relative': 0.25, 'miner': 0.25, 'burst': 0.25,
        'flow_contra': 1.0, 'btc_chop': 1.0, 'exec_penalty': -1.0,
    }),
    Variant('v18_vwap_revert_burst_fade', {
        'ema': 0.5, 'vwap': -2.0, 'momentum': -0.5, 'btc': 0.5,
        'relative': 0.25, 'miner': 0.25, 'burst': -1.5,
        'flow_contra': 1.5, 'btc_chop': 1.0, 'exec_penalty': -1.0,
    }),
    Variant('v19_vwap_revert_open_only_bias', {
        'ema': 0.75, 'vwap': -2.0, 'momentum': 0.25, 'btc': 0.75,
        'relative': 0.5, 'miner': 0.5, 'burst': 0.0,
        'flow_contra': 1.0, 'btc_chop': 1.0, 'exec_penalty': -1.0,
        'open_phase': 2.0,
    }),
    Variant('v20_vwap_revert_riot_short_skeptic', {
        'ema': 0.75, 'vwap': -2.0, 'momentum': 0.5, 'btc': 0.75,
        'relative': 0.5, 'miner': 0.5, 'burst': 0.25,
        'flow_contra': 1.0, 'btc_chop': 1.0, 'exec_penalty': -1.0,
        'riot_short_penalty': 3.0,
    }),
    Variant('v21_flow_fade_heavy', {
        'ema': 0.5, 'vwap': -1.0, 'momentum': -0.25, 'btc': 0.25,
        'relative': 0.25, 'miner': 0.25, 'burst': -0.5,
        'flow_contra': 4.0, 'btc_chop': 1.0, 'exec_penalty': -1.0,
    }),
    Variant('v22_flow_fade_trend_aware', {
        'ema': 1.0, 'vwap': -0.5, 'momentum': 0.5, 'btc': 0.75,
        'relative': 0.5, 'miner': 0.5, 'burst': 0.0,
        'flow_contra': 3.0, 'btc_chop': 0.5, 'exec_penalty': -1.0,
    }),
    Variant('v23_flow_fade_against_burst', {
        'ema': 0.5, 'vwap': -0.75, 'momentum': -0.75, 'btc': 0.25,
        'relative': 0.25, 'miner': 0.25, 'burst': -2.0,
        'flow_contra': 3.0, 'btc_chop': 1.0, 'exec_penalty': -1.0,
    }),
    Variant('v24_chop_means_reverse', {
        'ema': 0.5, 'vwap': -1.5, 'momentum': -0.5, 'btc': 0.0,
        'relative': 0.0, 'miner': 0.25, 'burst': -0.5,
        'flow_contra': 1.5, 'btc_chop': 4.0, 'exec_penalty': -1.0,
    }),
    Variant('v25_chop_means_stand_pat', {
        'ema': 1.0, 'vwap': 0.5, 'momentum': 0.5, 'btc': 0.5,
        'relative': 0.5, 'miner': 0.5, 'burst': 0.25,
        'flow_contra': 0.0, 'btc_chop': 0.0, 'exec_penalty': -1.0,
    }),
    Variant('v26_btc_alignment_strict', {
        'ema': 0.75, 'vwap': 0.5, 'momentum': 0.5, 'btc': 4.0,
        'relative': 1.0, 'miner': 0.5, 'burst': 0.25,
        'flow_contra': -1.0, 'btc_chop': -3.0, 'exec_penalty': -1.0,
    }),
    Variant('v27_btc_alignment_plus_vwap_revert', {
        'ema': 0.5, 'vwap': -1.5, 'momentum': 0.5, 'btc': 3.0,
        'relative': 1.0, 'miner': 0.5, 'burst': 0.25,
        'flow_contra': 1.0, 'btc_chop': 0.5, 'exec_penalty': -1.0,
    }),
    Variant('v28_btc_relative_inverse', {
        'ema': 0.75, 'vwap': -0.5, 'momentum': 0.5, 'btc': 1.0,
        'relative': -2.0, 'miner': 0.5, 'burst': 0.25,
        'flow_contra': 1.0, 'btc_chop': 1.0, 'exec_penalty': -1.0,
    }),
    Variant('v29_relative_strength_chase_penalty', {
        'ema': 1.0, 'vwap': -1.0, 'momentum': 1.0, 'btc': 1.0,
        'relative': -1.0, 'miner': 0.5, 'burst': 0.5,
        'flow_contra': 1.0, 'btc_chop': 1.0, 'exec_penalty': -1.0,
    }),
    Variant('v30_miner_basket_inverse_when_chop', {
        'ema': 0.75, 'vwap': -1.0, 'momentum': 0.5, 'btc': 0.5,
        'relative': 0.5, 'miner': -1.5, 'burst': 0.25,
        'flow_contra': 1.0, 'btc_chop': 2.0, 'exec_penalty': -1.0,
    }),
    Variant('v31_miner_confirmation_strict', {
        'ema': 1.0, 'vwap': 0.5, 'momentum': 1.0, 'btc': 1.5,
        'relative': 1.0, 'miner': 4.0, 'burst': 0.5,
        'flow_contra': -1.0, 'btc_chop': -2.0, 'exec_penalty': -1.0,
    }),
    Variant('v32_momentum_over_everything', {
        'ema': 0.5, 'vwap': 0.0, 'momentum': 4.0, 'btc': 0.5,
        'relative': 0.5, 'miner': 0.25, 'burst': 1.0,
        'flow_contra': -1.0, 'btc_chop': -1.0, 'exec_penalty': -1.0,
    }),
    Variant('v33_momentum_fade', {
        'ema': 0.5, 'vwap': -1.0, 'momentum': -3.0, 'btc': 0.0,
        'relative': 0.0, 'miner': 0.25, 'burst': -1.0,
        'flow_contra': 2.0, 'btc_chop': 1.0, 'exec_penalty': -1.0,
    }),
    Variant('v34_burst_required_continuation', {
        'ema': 1.0, 'vwap': 0.5, 'momentum': 1.5, 'btc': 1.0,
        'relative': 0.75, 'miner': 0.75, 'burst': 3.0,
        'flow_contra': -1.5, 'btc_chop': -1.5, 'exec_penalty': -1.0,
    }),
    Variant('v35_burst_exhaustion_fade', {
        'ema': 0.25, 'vwap': -1.5, 'momentum': -1.0, 'btc': 0.0,
        'relative': 0.0, 'miner': 0.25, 'burst': -3.0,
        'flow_contra': 2.0, 'btc_chop': 1.5, 'exec_penalty': -1.0,
    }),
    Variant('v36_execution_penalty_flip', {
        'ema': 1.0, 'vwap': 0.5, 'momentum': 0.75, 'btc': 1.0,
        'relative': 0.75, 'miner': 0.5, 'burst': 0.5,
        'flow_contra': 0.5, 'btc_chop': 0.5, 'exec_penalty': 3.0,
    }),
    Variant('v37_execution_quality_ignore', {
        'ema': 1.0, 'vwap': 0.5, 'momentum': 1.0, 'btc': 1.0,
        'relative': 1.0, 'miner': 0.5, 'burst': 0.5,
        'flow_contra': 0.0, 'btc_chop': -0.5, 'exec_penalty': 0.0,
    }),
    Variant('v38_open_fade_all', {
        'ema': -0.5, 'vwap': -1.0, 'momentum': -1.0, 'btc': -0.25,
        'relative': -0.25, 'miner': -0.25, 'burst': -1.0,
        'flow_contra': 2.0, 'btc_chop': 1.0, 'exec_penalty': -0.5,
        'open_phase': 3.0,
    }),
    Variant('v39_open_trend_only', {
        'ema': 2.0, 'vwap': 1.0, 'momentum': 2.0, 'btc': 1.5,
        'relative': 1.0, 'miner': 1.0, 'burst': 2.0,
        'flow_contra': -2.0, 'btc_chop': -2.0, 'exec_penalty': -1.0,
        'open_phase': -1.0,
    }),
    Variant('v40_midday_fade_bias', {
        'ema': 0.5, 'vwap': -1.5, 'momentum': -0.5, 'btc': 0.25,
        'relative': 0.25, 'miner': 0.25, 'burst': -0.5,
        'flow_contra': 2.0, 'btc_chop': 1.0, 'exec_penalty': -1.0,
    }),
    Variant('v41_riot_all_skeptic', {
        'ema': 1.0, 'vwap': -1.0, 'momentum': 0.5, 'btc': 0.75,
        'relative': 0.5, 'miner': 0.25, 'burst': 0.25,
        'flow_contra': 1.0, 'btc_chop': 1.0, 'exec_penalty': -1.0,
        'riot_short_penalty': 2.0,
    }),
    Variant('v42_short_skeptic_general', {
        'ema': 1.0, 'vwap': -0.75, 'momentum': 0.5, 'btc': 0.75,
        'relative': 0.5, 'miner': 0.5, 'burst': 0.25,
        'flow_contra': 1.0, 'btc_chop': 1.0, 'exec_penalty': -1.0,
    }, bias=0.75),
    Variant('v43_long_skeptic_general', {
        'ema': 1.0, 'vwap': -0.75, 'momentum': 0.5, 'btc': 0.75,
        'relative': 0.5, 'miner': 0.5, 'burst': 0.25,
        'flow_contra': 1.0, 'btc_chop': 1.0, 'exec_penalty': -1.0,
    }, bias=-0.75),
    Variant('v44_low_weight_consensus', {
        'ema': 0.75, 'vwap': 0.25, 'momentum': 0.75, 'btc': 0.75,
        'relative': 0.75, 'miner': 0.75, 'burst': 0.25,
        'flow_contra': 0.0, 'btc_chop': -0.5, 'exec_penalty': -0.5,
    }),
    Variant('v45_high_weight_consensus', {
        'ema': 2.0, 'vwap': 1.0, 'momentum': 2.0, 'btc': 2.0,
        'relative': 2.0, 'miner': 2.0, 'burst': 1.0,
        'flow_contra': -1.0, 'btc_chop': -2.0, 'exec_penalty': -1.0,
    }),
    Variant('v46_reversal_when_extended_and_chop', {
        'ema': 0.25, 'vwap': -3.0, 'momentum': -0.5, 'btc': 0.0,
        'relative': 0.0, 'miner': 0.25, 'burst': -0.25,
        'flow_contra': 1.0, 'btc_chop': 3.0, 'exec_penalty': -1.0,
    }),
    Variant('v47_reversal_when_extended_keep_btc', {
        'ema': 0.25, 'vwap': -3.0, 'momentum': -0.25, 'btc': 1.0,
        'relative': 0.5, 'miner': 0.25, 'burst': 0.0,
        'flow_contra': 1.0, 'btc_chop': 2.0, 'exec_penalty': -1.0,
    }),
    Variant('v48_follow_btc_ignore_stock', {
        'ema': 0.0, 'vwap': 0.0, 'momentum': 0.0, 'btc': 5.0,
        'relative': 0.0, 'miner': 0.0, 'burst': 0.0,
        'flow_contra': 0.0, 'btc_chop': -2.0, 'exec_penalty': -1.0,
    }),
    Variant('v49_follow_stock_ignore_btc', {
        'ema': 2.0, 'vwap': 1.0, 'momentum': 2.0, 'btc': 0.0,
        'relative': 0.0, 'miner': 0.5, 'burst': 1.0,
        'flow_contra': -1.0, 'btc_chop': 0.0, 'exec_penalty': -1.0,
    }),
    Variant('v50_balanced_reversion_hybrid', {
        'ema': 0.75, 'vwap': -1.75, 'momentum': 0.25, 'btc': 0.75,
        'relative': 0.5, 'miner': 0.5, 'burst': -0.25,
        'flow_contra': 1.75, 'btc_chop': 1.25, 'exec_penalty': -1.0,
        'riot_short_penalty': 1.5,
    }),
]


def _make_variant(name: str, **weights: float) -> Variant:
    bias = float(weights.pop('bias', 0.0))
    return Variant(name, {k: float(v) for k, v in weights.items()}, bias=bias)


def _generated_variants() -> list[Variant]:
    """Generate a broad but deterministic model zoo.

    The dimensions below intentionally combine continuation, reversion,
    BTC-relative, flow, phase, setup, and ticker-side priors. These are still
    reason-text proxies from the replay CSV, not raw-indicator backtests.
    """
    target_generated = 1750
    variants: list[Variant] = []
    archetypes = [
        ('cont', {'ema': 1.5, 'vwap': 0.75, 'momentum': 1.25, 'btc': 1.25, 'relative': 0.75, 'miner': 0.75, 'burst': 0.75}),
        ('vwap_rev', {'ema': 0.5, 'vwap': -2.0, 'momentum': 0.25, 'btc': 0.75, 'relative': 0.25, 'miner': 0.5, 'burst': 0.0}),
        ('flow_fade', {'ema': 0.5, 'vwap': -0.75, 'momentum': -0.25, 'btc': 0.25, 'relative': 0.25, 'miner': 0.25, 'burst': -0.5, 'flow_contra': 3.0}),
        ('btc_lead', {'ema': 0.5, 'vwap': 0.25, 'momentum': 0.5, 'btc': 3.0, 'relative': 1.5, 'miner': 0.5, 'burst': 0.25}),
        ('anti_chase', {'ema': 0.75, 'vwap': -1.0, 'momentum': 0.5, 'btc': 0.75, 'relative': -1.75, 'miner': 0.5, 'burst': 0.25}),
        ('burst_exhaust', {'ema': 0.25, 'vwap': -1.5, 'momentum': -1.0, 'btc': 0.0, 'relative': 0.0, 'miner': 0.25, 'burst': -2.5, 'flow_contra': 2.0}),
        ('miner_heavy', {'ema': 0.75, 'vwap': 0.25, 'momentum': 0.75, 'btc': 1.0, 'relative': 0.75, 'miner': 3.0, 'burst': 0.25}),
        ('stock_only', {'ema': 2.0, 'vwap': 1.0, 'momentum': 2.0, 'btc': 0.0, 'relative': 0.0, 'miner': 0.5, 'burst': 1.0}),
        ('btc_inverse', {'ema': 0.75, 'vwap': -0.5, 'momentum': 0.25, 'btc': -1.0, 'relative': -2.0, 'miner': 0.25, 'burst': 0.25}),
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

    idx = 51
    for ai, (aname, base) in enumerate(archetypes):
        for fi, flow_w in enumerate(flow_weights):
            for ci, chop_w in enumerate(chop_weights):
                for ei, exec_w in enumerate(exec_weights):
                    for pi, (phase_name, phase_w) in enumerate(phase_biases):
                        for si, (setup_name, setup_w) in enumerate(setup_biases):
                            for ti, (ticker_name, ticker_w) in enumerate(side_ticker_biases):
                                if len(variants) >= target_generated:
                                    return variants
                                # Deterministically thin the huge grid while keeping
                                # broad cross-dimensional coverage.
                                if (ai + fi * 2 + ci * 3 + ei + pi * 5 + si * 7 + ti * 11) % 4 == 0:
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
                                    variants.append(_make_variant(
                                        f'v{idx:03d}_{aname}_{phase_name}_{setup_name}_{ticker_name}_f{fi}_c{ci}_e{ei}_p{pi}_s{si}_t{ti}',
                                        **weights,
                                    ))
                                    idx += 1

    return variants


def _v29_family_variants() -> list[Variant]:
    """Local search around the current winner.

    Keep the key idea from v29: relative-strength chasing is suspect, so
    relative gets a negative weight. Vary the surrounding weights to see
    whether a nearby formula improves the result.
    """
    target_v29_family = 25000
    variants: list[Variant] = []
    base = {
        'ema': 1.0,
        'vwap': -1.0,
        'momentum': 1.0,
        'btc': 1.0,
        'relative': -1.0,
        'miner': 0.5,
        'burst': 0.5,
        'flow_contra': 1.0,
        'btc_chop': 1.0,
        'exec_penalty': -1.0,
    }
    relative_weights = [-0.75, -1.0, -1.25, -1.5, -1.75, -2.0, -2.5]
    vwap_weights = [-0.25, -0.75, -1.0, -1.5, -2.0]
    momentum_weights = [0.25, 0.75, 1.0, 1.5]
    btc_weights = [0.25, 0.75, 1.0, 1.5, 2.0]
    flow_weights = [0.0, 0.75, 1.5, 2.25]
    chop_weights = [0.0, 0.75, 1.5, 2.5]
    extras = [
        {},
        {'riot_short_penalty': 1.5},
        {'riot_short_penalty': 3.0},
        {'open_phase': 1.5},
        {'open_phase': -1.0},
        {'midday_phase': 1.5},
        {'setup_btc_relative_strength': -1.0},
        {'setup_btc_relative_strength': -2.0},
        {'setup_momentum_breakout': 1.5},
        {'setup_trend_pullback': -1.0},
        {'vwap_sigma_ext': -0.75},
        {'flow_pressure_abs': -0.5},
        {'btc_mom_abs': 0.75},
    ]
    idx = 701
    exec_choices = [-2.0, -1.0, -0.5, 0.5]
    ema_choices = [0.5, 1.0, 1.5]
    miner_choices = [0.0, 0.5, 1.0, 1.5]
    burst_choices = [-0.5, 0.0, 0.5, 1.0]
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
                                            for xi, extra in enumerate(extras):
                                                if len(variants) >= target_v29_family:
                                                    return variants
                                                # Thin the enormous grid deterministically while
                                                # sweeping every axis over the first 25k samples.
                                                gate = (
                                                    ri * 3 + vi * 5 + mi * 7 + bi * 11
                                                    + fi * 13 + ci * 17 + ei * 19
                                                    + emi * 23 + mini * 29 + bui * 31 + xi * 37
                                                )
                                                if gate % 11 not in (0, 3):
                                                    continue
                                                weights = dict(base)
                                                weights.update({
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
                                                })
                                                weights.update(extra)
                                                variants.append(_make_variant(
                                                    (
                                                        f'v{idx:05d}_v29_family_rel{ri}_vw{vi}_mom{mi}_btc{bi}'
                                                        f'_flow{fi}_chop{ci}_exec{ei}_ema{emi}_miner{mini}'
                                                        f'_burst{bui}_x{xi}'
                                                    ),
                                                    **weights,
                                                ))
                                                idx += 1
    return variants


def _v29_elite_variants() -> list[Variant]:
    """Tight local search around the best 25k V29-family formula."""
    target = 5000
    variants: list[Variant] = []
    relative_weights = [-0.5, -0.65, -0.75, -0.9, -1.05, -1.25]
    vwap_weights = [0.0, -0.1, -0.25, -0.4, -0.6, -0.75]
    momentum_weights = [0.0, 0.15, 0.25, 0.4, 0.6, 0.75]
    btc_weights = [0.4, 0.6, 0.75, 0.9, 1.1, 1.25]
    miner_weights = [0.0, 0.25, 0.5, 0.75, 1.0]
    chop_weights = [0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 2.5]
    exec_weights = [-0.25, 0.0, 0.25, 0.5, 0.75, 1.0]
    ema_weights = [0.5, 0.75, 1.0, 1.25, 1.5]
    burst_weights = [-0.25, 0.0, 0.25, 0.5]
    setup_rel_weights = [-0.5, -0.75, -1.0, -1.25, -1.5]
    flow_weights = [-0.25, 0.0, 0.25, 0.5]
    extras = [
        {},
        {'riot_short_penalty': 0.75},
        {'riot_short_penalty': 1.5},
        {'riot_long_penalty': -0.75},
        {'open_phase': 0.75},
        {'open_phase': -0.75},
        {'midday_phase': 0.75},
        {'setup_momentum_breakout': 0.75},
        {'setup_trend_pullback': -0.75},
        {'vwap_sigma_ext': -0.25},
        {'btc_mom_abs': 0.5},
        {'flow_pressure_abs': -0.25},
    ]
    idx = 50001
    for ri, rel_w in enumerate(relative_weights):
        for vi, vwap_w in enumerate(vwap_weights):
            for mi, mom_w in enumerate(momentum_weights):
                for bi, btc_w in enumerate(btc_weights):
                    for chi, chop_w in enumerate(chop_weights):
                        for exi, exec_w in enumerate(exec_weights):
                            for emi, ema_w in enumerate(ema_weights):
                                for mini, miner_w in enumerate(miner_weights):
                                    for bui, burst_w in enumerate(burst_weights):
                                        for sri, setup_rel_w in enumerate(setup_rel_weights):
                                            for fli, flow_w in enumerate(flow_weights):
                                                for xi, extra in enumerate(extras):
                                                    if len(variants) >= target:
                                                        return variants
                                                    gate = (
                                                        ri * 3 + vi * 5 + mi * 7 + bi * 11
                                                        + chi * 13 + exi * 17 + emi * 19
                                                        + mini * 23 + bui * 29 + sri * 31
                                                        + fli * 37 + xi * 41
                                                    )
                                                    if gate % 19 != 0:
                                                        continue
                                                    weights = {
                                                        'ema': ema_w,
                                                        'vwap': vwap_w,
                                                        'momentum': mom_w,
                                                        'btc': btc_w,
                                                        'relative': rel_w,
                                                        'miner': miner_w,
                                                        'burst': burst_w,
                                                        'flow_contra': flow_w,
                                                        'btc_chop': chop_w,
                                                        'exec_penalty': exec_w,
                                                        'setup_btc_relative_strength': setup_rel_w,
                                                    }
                                                    weights.update(extra)
                                                    variants.append(_make_variant(
                                                        (
                                                            f'v{idx:05d}_v29_elite_rel{ri}_vw{vi}_mom{mi}_btc{bi}'
                                                            f'_chop{chi}_exec{exi}_ema{emi}_miner{mini}'
                                                            f'_burst{bui}_setup{sri}_flow{fli}_x{xi}'
                                                        ),
                                                        **weights,
                                                    ))
                                                    idx += 1
    return variants


def _v29_elite2_variants() -> list[Variant]:
    """Second local search around the best elite formula."""
    target = 5000
    variants: list[Variant] = []
    relative_weights = [-0.35, -0.5, -0.65, -0.8, -1.0]
    vwap_weights = [-0.25, -0.1, 0.0, 0.1, 0.25]
    momentum_weights = [-0.25, -0.1, 0.0, 0.1, 0.25, 0.4]
    btc_weights = [0.2, 0.3, 0.4, 0.55, 0.7, 0.9]
    miner_weights = [0.25, 0.4, 0.5, 0.65, 0.8]
    burst_weights = [0.0, 0.1, 0.25, 0.4, 0.6]
    flow_weights = [-0.5, -0.35, -0.25, -0.1, 0.0, 0.15]
    chop_weights = [0.4, 0.6, 0.75, 0.9, 1.1, 1.35]
    exec_weights = [0.2, 0.35, 0.5, 0.65, 0.8]
    setup_rel_weights = [-0.75, -1.0, -1.25, -1.5, -1.75]
    ema_weights = [0.75, 0.9, 1.0, 1.1, 1.25]
    midday_weights = [0.25, 0.5, 0.75, 1.0, 1.25]
    extras = [
        {},
        {'open_phase': 0.4},
        {'open_phase': -0.4},
        {'setup_momentum_breakout': 0.5},
        {'setup_trend_pullback': -0.5},
        {'riot_short_penalty': 0.5},
        {'riot_long_penalty': -0.5},
        {'vwap_sigma_ext': -0.15},
        {'btc_mom_abs': 0.25},
        {'flow_pressure_abs': -0.15},
    ]
    idx = 60001
    for ri, rel_w in enumerate(relative_weights):
        for vi, vwap_w in enumerate(vwap_weights):
            for mi, mom_w in enumerate(momentum_weights):
                for bi, btc_w in enumerate(btc_weights):
                    for mini, miner_w in enumerate(miner_weights):
                        for bui, burst_w in enumerate(burst_weights):
                            for fli, flow_w in enumerate(flow_weights):
                                for chi, chop_w in enumerate(chop_weights):
                                    for exi, exec_w in enumerate(exec_weights):
                                        for sri, setup_rel_w in enumerate(setup_rel_weights):
                                            for emi, ema_w in enumerate(ema_weights):
                                                for mdi, midday_w in enumerate(midday_weights):
                                                    for xi, extra in enumerate(extras):
                                                        if len(variants) >= target:
                                                            return variants
                                                        gate = (
                                                            ri * 3 + vi * 5 + mi * 7 + bi * 11
                                                            + mini * 13 + bui * 17 + fli * 19
                                                            + chi * 23 + exi * 29 + sri * 31
                                                            + emi * 37 + mdi * 41 + xi * 43
                                                        )
                                                        if gate % 29 != 0:
                                                            continue
                                                        weights = {
                                                            'ema': ema_w,
                                                            'vwap': vwap_w,
                                                            'momentum': mom_w,
                                                            'btc': btc_w,
                                                            'relative': rel_w,
                                                            'miner': miner_w,
                                                            'burst': burst_w,
                                                            'flow_contra': flow_w,
                                                            'btc_chop': chop_w,
                                                            'exec_penalty': exec_w,
                                                            'setup_btc_relative_strength': setup_rel_w,
                                                            'midday_phase': midday_w,
                                                        }
                                                        weights.update(extra)
                                                        variants.append(_make_variant(
                                                            (
                                                                f'v{idx:05d}_v29_elite2_rel{ri}_vw{vi}_mom{mi}_btc{bi}'
                                                                f'_miner{mini}_burst{bui}_flow{fli}_chop{chi}'
                                                                f'_exec{exi}_setup{sri}_ema{emi}_mid{mdi}_x{xi}'
                                                            ),
                                                            **weights,
                                                        ))
                                                        idx += 1
    return variants


VARIANTS = BASE_VARIANTS + EXTRA_VARIANTS + _generated_variants() + _v29_family_variants() + _v29_elite_variants() + _v29_elite2_variants()


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ''):
            return default
        return float(value)
    except Exception:
        return default


def _side_sign(side: str) -> int:
    return 1 if str(side).upper() == 'LONG' else -1


def _momentum_sign(reasons: str) -> int:
    vals = []
    for key in ('5s', '15s'):
        m = re.search(rf'{key}:([+-]?\d+(?:\.\d+)?)%', reasons)
        if m:
            vals.append(_num(m.group(1)))
    if not vals:
        return 0
    avg = sum(vals) / len(vals)
    if avg > 0.03:
        return 1
    if avg < -0.03:
        return -1
    return 0


def _first_float(pattern: str, text: str, default: float = 0.0) -> float:
    m = re.search(pattern, text)
    return _num(m.group(1), default) if m else default


def _flow_pressure(reasons: str) -> float:
    vals = []
    for label in ('buy pressure watch', 'sell pressure watch'):
        for m in re.finditer(rf'{label} \((\d+(?:\.\d+)?)%?', reasons):
            val = _num(m.group(1))
            vals.append(val)
    if not vals:
        return 0.0
    return max(vals)


def features(row: dict) -> dict[str, float]:
    reasons = row.get('reasons') or ''
    side_sign = _side_sign(row.get('side'))
    out = {k: 0.0 for k in (
        'ema', 'vwap', 'momentum', 'btc', 'relative', 'miner', 'burst',
        'flow_contra', 'btc_chop', 'exec_penalty', 'open_phase',
        'midday_phase', 'riot_short_penalty', 'riot_long_penalty',
        'vwap_sigma_ext', 'btc_mom_abs', 'flow_pressure_abs',
        'setup_btc_relative_strength', 'setup_momentum_breakout',
        'setup_trend_pullback', 'setup_flow_exhaustion_fade',
        'setup_vwap_reclaim_breakdown',
        'brs_weak_followthrough', 'brs_open_risk', 'brs_bear_normal_risk',
        'brs_open_weak_followthrough', 'timeout_decay_risk',
        'brs_open_continuation_quality', 'brs_normal_continuation_quality',
        'medium_brs_penalty', 'brs_open_medium_risk',
        'brs_open_inversion_risk', 'brs_open_non_riot_inversion_risk',
        'brs_clsk_mara_inversion_risk', 'brs_open_btc_neutral_risk',
        'brs_open_low_range_risk', 'brs_open_low_range_020',
        'brs_open_low_range_025', 'brs_open_low_range_035',
        'brs_open_low_range_040', 'brs_open_low_range_045',
        'brs_open_side_bad_range_025', 'brs_open_side_bad_range_031',
        'brs_open_side_bad_range_040', 'brs_wide_spread_risk',
        'brs_open_book_worsening_risk', 'brs_open_flow_quote_failure',
        'brs_open_flow_book_failure', 'brs_open_flow_book_failure_loose',
        'brs_open_session_neutral_failure', 'brs_open_range_neutral_mom_failure',
        'brs_open_three_tape_failure', 'brs_alignment_failure',
        'brs_open_alignment_failure', 'brs_normal_alignment_failure',
    )}
    if 'bull stack' in reasons:
        out['ema'] = 1
    elif 'bear stack' in reasons:
        out['ema'] = -1
    if 'above VWAP' in reasons:
        out['vwap'] = 1
    elif 'below VWAP' in reasons:
        out['vwap'] = -1
    sigma = _first_float(r'VWAP [+-]?(\d+(?:\.\d+)?) sigma', reasons)
    out['vwap_sigma_ext'] = _side_sign(row.get('side')) * min(5.0, abs(sigma)) / 5.0 if sigma else 0.0
    out['momentum'] = _momentum_sign(reasons)
    if 'BTC aligned bull' in reasons:
        out['btc'] = 1
        out['btc_mom_abs'] = min(1.0, abs(_first_float(r'BTC aligned bull, 60s ([+-]?\d+(?:\.\d+)?)%', reasons)) / 0.25)
    elif 'BTC aligned bear' in reasons:
        out['btc'] = -1
        out['btc_mom_abs'] = min(1.0, abs(_first_float(r'BTC aligned bear, 60s ([+-]?\d+(?:\.\d+)?)%', reasons)) / 0.25)
    elif 'BTC bull conflict' in reasons:
        out['btc'] = 1
    elif 'BTC bear conflict' in reasons:
        out['btc'] = -1
    if 'stock leading BTC' in reasons:
        out['relative'] = 1
    elif 'stock weak vs' in reasons:
        out['relative'] = -1
    elif 'stock lagging' in reasons:
        out['relative'] = -1
    elif 'stock strong vs' in reasons:
        out['relative'] = 1
    if 'miner basket confirmed' in reasons:
        out['miner'] = side_sign
    elif 'miner basket opposed' in reasons:
        out['miner'] = -side_sign
    if 'vol/tick burst' in reasons:
        out['burst'] = side_sign
    # Flow watch means current flow is against a continuation entry. Positive
    # here is contrarian/opposite-side pressure.
    if 'buy pressure watch' in reasons:
        out['flow_contra'] = 1
    elif 'sell pressure watch' in reasons:
        out['flow_contra'] = -1
    out['flow_pressure_abs'] = min(1.0, _flow_pressure(reasons) / 100.0)
    if 'BTC chop penalty' in reasons:
        out['btc_chop'] = -side_sign
    if 'execution quality penalty' in reasons:
        out['exec_penalty'] = -side_sign
    if row.get('session_phase') == 'open':
        out['open_phase'] = -side_sign
    if row.get('session_phase') == 'midday':
        out['midday_phase'] = -side_sign
    if row.get('ticker') == 'RIOT' and row.get('side') == 'SHORT':
        out['riot_short_penalty'] = 1
    if row.get('ticker') == 'RIOT' and row.get('side') == 'LONG':
        out['riot_long_penalty'] = 1
    setup = row.get('setup_type') or ''
    setup_key = f'setup_{setup}'
    if setup_key in out:
        out[setup_key] = side_sign
    setup_brs = setup == 'btc_relative_strength'
    phase = str(row.get('session_phase') or '').lower()
    side = str(row.get('side') or '').upper()
    if setup_brs:
        if phase == 'open':
            out['brs_open_risk'] = 1.0
            out['brs_open_inversion_risk'] = side_sign
            out['brs_open_non_riot_inversion_risk'] = side_sign if row.get('ticker') != 'RIOT' else 0.0
        if row.get('ticker') in ('CLSK', 'MARA'):
            out['brs_clsk_mara_inversion_risk'] = side_sign
        if str(row.get('conviction') or '').upper() == 'MEDIUM':
            out['medium_brs_penalty'] = 1.0
            if phase == 'open' and side == 'LONG':
                out['brs_open_medium_risk'] = 1.0
        weak_reasons = 0
        if out['vwap'] <= 0 and side == 'LONG':
            weak_reasons += 1
        if out['momentum'] <= 0 and side == 'LONG':
            weak_reasons += 1
        if out['relative'] < 0:
            weak_reasons += 1
        if out['btc_chop']:
            weak_reasons += 1
        weak = min(1.0, weak_reasons / 3.0)
        if phase == 'open' and side == 'LONG':
            out['brs_open_weak_followthrough'] = weak
        elif phase == 'normal' and side == 'LONG':
            out['brs_weak_followthrough'] = weak
        quality = max(0.0, 1.0 - weak)
        if phase == 'open':
            out['brs_open_continuation_quality'] = quality
        elif phase == 'normal':
            out['brs_normal_continuation_quality'] = quality
        if weak >= 0.75:
            out['brs_alignment_failure'] = side_sign * weak
            if phase == 'open':
                out['brs_open_alignment_failure'] = side_sign * weak
            elif phase == 'normal':
                out['brs_normal_alignment_failure'] = side_sign * weak
    if setup in ('btc_relative_strength', 'trend_pullback'):
        timeout_reasons = 0
        if out['burst'] == 0:
            timeout_reasons += 1
        if out['momentum'] == 0:
            timeout_reasons += 1
        if out['btc_chop']:
            timeout_reasons += 1
        if out['exec_penalty']:
            timeout_reasons += 1
        out['timeout_decay_risk'] = min(1.0, timeout_reasons / 3.0)
    return out


def score_variant(row: dict, variant: Variant) -> float:
    f = features(row)
    score = variant.bias
    for name, weight in variant.weights.items():
        if name == 'riot_short_penalty':
            # Penalty pushes away from the original RIOT short side.
            score += weight * f[name]
            continue
        score += weight * f.get(name, 0.0)
    return score


def evaluate(rows: list[dict], variant: Variant, starting_balance: float) -> dict:
    trades = []
    for row in rows:
        score = score_variant(row, variant)
        if score == 0:
            chosen_side = row.get('side')
        else:
            chosen_side = 'LONG' if score > 0 else 'SHORT'
        flipped = chosen_side != row.get('side')
        pnl = _num(row.get('pnl'))
        model_pnl = -pnl if flipped else pnl
        trades.append({
            'ticker': row.get('ticker'),
            'original_side': row.get('side'),
            'chosen_side': chosen_side,
            'flipped': flipped,
            'pnl': model_pnl,
            'score': score,
            'was_win': model_pnl > 0,
            'was_loss': model_pnl < 0,
        })
    pnl_total = sum(t['pnl'] for t in trades)
    wins = sum(1 for t in trades if t['was_win'])
    losses = sum(1 for t in trades if t['was_loss'])
    by_ticker = {}
    for ticker in sorted({t['ticker'] for t in trades}):
        scoped = [t for t in trades if t['ticker'] == ticker]
        scoped_pnl = sum(t['pnl'] for t in scoped)
        by_ticker[ticker] = {
            'trades': len(scoped),
            'wins': sum(1 for t in scoped if t['was_win']),
            'losses': sum(1 for t in scoped if t['was_loss']),
            'flipped': sum(1 for t in scoped if t['flipped']),
            'pnl': round(scoped_pnl, 2),
            'win_rate_pct': round(100 * sum(1 for t in scoped if t['was_win']) / len(scoped), 2) if scoped else None,
        }
    return {
        'variant': variant.name,
        'trades': len(trades),
        'wins': wins,
        'losses': losses,
        'flipped': sum(1 for t in trades if t['flipped']),
        'win_rate_pct': round(100 * wins / len(trades), 2) if trades else None,
        'pnl': round(pnl_total, 2),
        'starting_balance': starting_balance,
        'ending_balance': round(starting_balance + pnl_total, 2),
        'by_ticker': by_ticker,
    }


def load_rows(path: str) -> list[dict]:
    with open(path, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--csv', default=DEFAULT_CSV)
    ap.add_argument('--out', default=DEFAULT_OUT)
    ap.add_argument('--starting-balance', type=float, default=100000.0)
    args = ap.parse_args()
    rows = load_rows(args.csv)
    results = [evaluate(rows, v, args.starting_balance) for v in VARIANTS]
    results.sort(key=lambda r: r['pnl'], reverse=True)
    payload = {
        'source_csv': args.csv,
        'method_note': (
            'Fast hypothetical lab: variants re-score the same replayed entry '
            'opportunities from CSV reason text. Flipped trades use mechanical '
            'inverse P/L. This is not a full raw-indicator replay.'
        ),
        'results': results,
    }
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    print(json.dumps({'out': args.out, 'variants': len(results), 'best': results[0]}, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
