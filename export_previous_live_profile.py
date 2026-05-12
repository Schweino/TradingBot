"""Export the pre-rank4 live profile for same-window baseline comparisons."""
from __future__ import annotations

import json
import os


PROFILE = {
    'name': 'previous_live_replay_safe_local_000000021683',
    'bias': 0.0,
    'weights': {
        'brs_open_low_range_025': -0.03125,
        'brs_open_weak_followthrough': -0.25,
        'brs_weak_followthrough': -0.5,
        'btc': 1.0,
        'btc_chop': 1.5625,
        'btc_mom_abs': 0.0625,
        'burst': 0.0,
        'ema': 1.0,
        'exec_penalty': 0.5,
        'flow_contra': 1.5,
        'flow_pressure_abs': 0.0,
        'midday_phase': 0.0,
        'miner': 1.5,
        'momentum': 1.0,
        'open_phase': 0.0,
        'relative': -1.75,
        'riot_long_penalty': 0.0,
        'riot_short_penalty': 0.0,
        'setup_btc_relative_strength': 0.0,
        'setup_flow_exhaustion_fade': 0.0,
        'setup_momentum_breakout': 1.75,
        'setup_trend_pullback': 0.0,
        'setup_vwap_reclaim_breakdown': 0.0,
        'timeout_decay_risk': -0.125,
        'vwap': -1.5,
        'vwap_sigma_ext': 0.0,
    },
    'notes': ['Pre-rank4 live profile exported for out-of-sample comparison.'],
}


def main() -> int:
    path = os.path.join('postmortem', 'backtests', 'rank4_oos_20260504_20260506_100k', 'previous_live_profile.json')
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(PROFILE, f, indent=2, sort_keys=True)
    print(os.path.abspath(path))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
