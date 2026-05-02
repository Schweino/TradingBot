from __future__ import annotations

from engine_validation import (
    _gate_btc_alignment,
    _gate_execution_quality_65,
    _gate_strict_short_flow,
    gate_scoreboard,
    loser_diagnostics,
)


def test_strict_short_flow_blocks_unproven_buy_flow():
    ok, reason = _gate_strict_short_flow({
        'side': 'SHORT',
        'flow_120s_buy_pct': 63.0,
        'flow_30s_delta': -1.0,
        'btc_mom_60s': -0.05,
        'stock_minus_btc_60s': -0.04,
    })
    assert not ok
    assert reason == 'short_buy_flow_not_exhausted'


def test_strict_short_flow_allows_confirmed_rollover():
    ok, reason = _gate_strict_short_flow({
        'side': 'SHORT',
        'flow_120s_buy_pct': 63.0,
        'flow_30s_delta': -8.0,
        'btc_mom_60s': -0.05,
        'stock_minus_btc_60s': -0.04,
    })
    assert ok
    assert reason is None


def test_btc_alignment_blocks_wrong_way_impulse():
    ok, reason = _gate_btc_alignment({
        'side': 'LONG',
        'btc_mom_15s': -0.10,
        'btc_mom_60s': -0.20,
    })
    assert not ok
    assert reason == 'long_against_btc'


def test_execution_quality_gate_blocks_poor_entry():
    ok, reason = _gate_execution_quality_65({'execution_score': 55})
    assert not ok
    assert reason == 'execution_score_below_65'


def test_gate_scoreboard_estimates_blocked_pnl_delta():
    rows = [
        {
            'source': 'actual_trade',
            'side': 'SHORT',
            'pnl': -100.0,
            'abs_score': 7,
            'execution_score': 70,
            'flow_120s_buy_pct': 64,
            'flow_30s_delta': 0,
            'btc_mom_60s': -0.03,
            'stock_minus_btc_60s': -0.02,
        },
        {
            'source': 'actual_trade',
            'side': 'LONG',
            'pnl': 40.0,
            'abs_score': 7,
            'execution_score': 75,
            'btc_mom_15s': 0.1,
            'btc_mom_60s': 0.1,
        },
    ]
    board = {row['variant']: row for row in gate_scoreboard(rows)}
    strict = board['strict_short_flow_rollover']
    assert strict['blocked'] == 1
    assert strict['estimated_delta_vs_actual'] == 100.0


def test_loser_diagnostics_tags_common_failure_modes():
    diag = loser_diagnostics([
        {
            'source': 'actual_trade',
            'side': 'LONG',
            'pnl': -25,
            'abs_score': 5,
            'execution_score': 55,
            'btc_mom_15s': -0.1,
            'btc_mom_60s': -0.2,
            'flow_120s_buy_pct': 30,
            'setup_type': 'momentum_breakout',
        }
    ])
    assert diag['losers'] == 1
    assert diag['tags']['low_abs_score'] == 1
    assert diag['tags']['weak_execution'] == 1
    assert diag['tags']['against_btc'] == 1
