"""Reusable scoring-profile helpers for replay-only experiments.

These helpers are intentionally separate from ws_scalp.py. A profile can
override replay direction/scoring without changing the live engine.
"""
from __future__ import annotations

import json
import os
import re
from typing import Any

import routed_scoring_profile


HERE = os.path.dirname(os.path.abspath(__file__))


def load_profile(path: str | None) -> dict | None:
    if not path:
        return None
    with open(path, 'r', encoding='utf-8') as f:
        payload = json.load(f)
    if 'weights' not in payload and 'results' in payload:
        result = (payload.get('results') or [{}])[0]
        payload = {
            'name': result.get('variant') or payload.get('name') or os.path.basename(path),
            'weights': result.get('weights') or {},
            'bias': result.get('bias', 0.0),
            'routes': result.get('routes') or [],
        }
    payload.setdefault('name', os.path.splitext(os.path.basename(path))[0])
    payload.setdefault('bias', 0.0)
    payload.setdefault('weights', {})
    payload.setdefault('routes', [])
    return payload


def write_profile_from_lab_result(lab_json: str, out_path: str, variant: str | None = None) -> dict:
    with open(lab_json, 'r', encoding='utf-8') as f:
        payload = json.load(f)
    matches = [
        r for r in payload.get('results', [])
        if variant is None or r.get('variant') == variant
    ]
    if not matches:
        raise ValueError(f'variant not found in lab output: {variant}')
    result = matches[0]
    profile = {
        'name': result.get('variant'),
        'source': os.path.abspath(lab_json),
        'weights': result.get('weights') or {},
        'bias': result.get('bias', 0.0),
        'notes': [
            'Replay-only scoring profile generated from scoring_variant_lab_fast output.',
            'Does not change ws_scalp.py or live trading behavior.',
        ],
    }
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(profile, f, indent=2, sort_keys=True)
    return profile


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ''):
            return default
        return float(value)
    except Exception:
        return default


def _text(sig: dict) -> str:
    parts = []
    parts.extend(str(x) for x in (sig.get('reasons') or []))
    quality = sig.get('signal_quality') or {}
    model = quality.get('score_model') or {}
    parts.extend(str(x) for x in (model.get('execution_reasons') or []))
    return ' | '.join(parts).lower()


def _side_sign(sig: dict) -> int:
    return 1 if str(sig.get('side')).upper() == 'LONG' else -1


def _setup(sig: dict, name: str) -> float:
    return 1.0 if sig.get('setup_type') == name else 0.0


def features_from_signal(sig: dict, ind: dict | None = None, btc_ind: dict | None = None) -> dict[str, float]:
    """Build the same feature family used by the hypothetical lab, from replay state."""
    ind = ind or {}
    side_sign = _side_sign(sig)
    text = _text(sig)
    components = ((sig.get('signal_quality') or {}).get('score_components') or {})
    btc_ctx = sig.get('btc_context') or {}
    relative = sig.get('relative_strength') or {}
    ticker = str(sig.get('ticker') or '').upper()
    side = str(sig.get('side') or '').upper()

    ema = 0.0
    stack = str(ind.get('ema_stack') or '').lower()
    if 'bull' in stack:
        ema = 1.0
    elif 'bear' in stack:
        ema = -1.0

    vwap = 0.0
    vwap_sigma = _num(ind.get('vwap_dist_sigma'), None)
    if vwap_sigma is not None:
        vwap = 1.0 if vwap_sigma > 0 else (-1.0 if vwap_sigma < 0 else 0.0)
    elif 'above vwap' in text:
        vwap = 1.0
    elif 'below vwap' in text:
        vwap = -1.0

    mom5 = _num(ind.get('mom_5s'))
    mom15 = _num(ind.get('mom_15s'))
    mom30 = _num(ind.get('mom_30s'))
    mom60 = _num(ind.get('mom_60s'))
    momentum = 1.0 if mom5 > 0 and mom15 > 0 else (-1.0 if mom5 < 0 and mom15 < 0 else 0.0)

    btc_mom = _num((btc_ctx or {}).get('mom_60s'), _num((btc_ind or {}).get('mom_60s')))
    btc = 1.0 if btc_mom > 0 else (-1.0 if btc_mom < 0 else 0.0)
    rel_value = _num(
        relative.get('stock_minus_btc_implied_60s'),
        _num((btc_ctx or {}).get('stock_minus_btc_implied_60s')),
    )
    rel = 1.0 if rel_value > 0 else (-1.0 if rel_value < 0 else 0.0)

    miner_score = _num((sig.get('miner_basket') or {}).get('score'), _num(components.get('miner_basket')))
    miner = 1.0 if miner_score > 0 else (-1.0 if miner_score < 0 else 0.0)

    burst_score = _num(components.get('burst_score'))
    burst = 1.0 if burst_score > 0 else (-1.0 if burst_score < 0 else 0.0)
    if burst == 0 and 'vol/tick burst' in text:
        burst = side_sign

    flow_contra = 0.0
    if re.search(r'buy pressure watch|sell pressure watch', text):
        flow_contra = -side_sign
    flow_pressure_abs = 1.0 if flow_contra else 0.0

    btc_chop = -side_sign if 'btc chop penalty' in text or components.get('btc_chop_penalty') else 0.0
    exec_penalty = -side_sign if 'execution quality penalty' in text else 0.0

    phase = str(sig.get('session_phase') or ind.get('session_phase') or '').lower()
    open_phase = 1.0 if phase == 'open' else 0.0
    midday_phase = 1.0 if phase == 'midday' else 0.0
    flow_10s = ind.get('flow_10s') or {}
    flow_30s = ind.get('flow_30s') or {}
    flow_buy_pct_10s = _num(flow_10s.get('buy_pct'), None)
    flow_buy_pct = _num(flow_30s.get('buy_pct'), None)
    basket = sig.get('miner_basket') or {}
    brs_weak_followthrough = 0.0
    if sig.get('setup_type') == 'btc_relative_strength' and side == 'LONG' and phase == 'normal':
        weak_reasons = 0
        if vwap <= 0:
            weak_reasons += 1
        if str(btc_ctx.get('regime') or '').lower() not in ('bull', 'bull_momentum', 'impulse_up'):
            weak_reasons += 1
        if basket.get('state') != 'confirmed':
            weak_reasons += 1
        if rel_value < 0:
            weak_reasons += 1
        if flow_buy_pct is not None and flow_buy_pct < 50:
            weak_reasons += 1
        if momentum <= 0:
            weak_reasons += 1
        if btc_chop:
            weak_reasons += 1
        brs_weak_followthrough = min(1.0, weak_reasons / 3.0)
    brs_open_risk = 1.0 if sig.get('setup_type') == 'btc_relative_strength' and side == 'LONG' and phase == 'open' else 0.0
    brs_bear_normal_risk = 1.0 if (
        sig.get('setup_type') == 'btc_relative_strength'
        and side == 'LONG'
        and phase == 'normal'
        and str(btc_ctx.get('regime') or '').lower() in ('bear', 'bear_momentum', 'impulse_down')
    ) else 0.0
    brs_open_weak_followthrough = 0.0
    if sig.get('setup_type') == 'btc_relative_strength' and side == 'LONG' and phase == 'open':
        weak_reasons = 0
        if vwap <= 0:
            weak_reasons += 1
        if str(btc_ctx.get('regime') or '').lower() not in ('bull', 'bull_momentum', 'impulse_up'):
            weak_reasons += 1
        if basket.get('state') != 'confirmed':
            weak_reasons += 1
        if rel_value < 0:
            weak_reasons += 1
        if flow_buy_pct is not None and flow_buy_pct < 50:
            weak_reasons += 1
        if momentum <= 0:
            weak_reasons += 1
        if btc_chop:
            weak_reasons += 1
        brs_open_weak_followthrough = min(1.0, weak_reasons / 3.0)
    timeout_decay_risk = 0.0
    if sig.get('setup_type') in ('btc_relative_strength', 'trend_pullback'):
        weak_reasons = 0
        if burst == 0:
            weak_reasons += 1
        if momentum == 0:
            weak_reasons += 1
        if side == 'LONG' and flow_buy_pct is not None and flow_buy_pct < 50:
            weak_reasons += 1
        if side == 'SHORT' and flow_buy_pct is not None and flow_buy_pct > 50:
            weak_reasons += 1
        if btc_chop:
            weak_reasons += 1
        if exec_penalty:
            weak_reasons += 1
        timeout_decay_risk = min(1.0, weak_reasons / 3.0)
    brs_quality_points = 0
    brs_quality_possible = 0
    if sig.get('setup_type') == 'btc_relative_strength':
        checks = [
            vwap > 0 if side == 'LONG' else vwap < 0,
            momentum > 0 if side == 'LONG' else momentum < 0,
            rel_value > 0 if side == 'LONG' else rel_value < 0,
            str(btc_ctx.get('regime') or '').lower() not in (
                ('bear', 'bear_momentum', 'impulse_down') if side == 'LONG'
                else ('bull', 'bull_momentum', 'impulse_up')
            ),
            basket.get('state') == 'confirmed',
            True if flow_buy_pct is None else (flow_buy_pct >= 55 if side == 'LONG' else flow_buy_pct <= 45),
            not bool(btc_chop),
        ]
        brs_quality_possible = len(checks)
        brs_quality_points = sum(1 for ok in checks if ok)
    brs_continuation_quality = (
        brs_quality_points / brs_quality_possible if brs_quality_possible else 0.0
    )
    brs_open_continuation_quality = brs_continuation_quality if (
        sig.get('setup_type') == 'btc_relative_strength' and phase == 'open'
    ) else 0.0
    brs_normal_continuation_quality = brs_continuation_quality if (
        sig.get('setup_type') == 'btc_relative_strength' and phase == 'normal'
    ) else 0.0
    medium_brs_penalty = 1.0 if (
        sig.get('setup_type') == 'btc_relative_strength'
        and str(sig.get('conviction') or '').upper() == 'MEDIUM'
    ) else 0.0
    brs_open_medium_risk = 1.0 if (
        sig.get('setup_type') == 'btc_relative_strength'
        and side == 'LONG'
        and phase == 'open'
        and str(sig.get('conviction') or '').upper() == 'MEDIUM'
    ) else 0.0
    brs_open_inversion_risk = side_sign if (
        sig.get('setup_type') == 'btc_relative_strength'
        and phase == 'open'
    ) else 0.0
    brs_open_non_riot_inversion_risk = side_sign if (
        sig.get('setup_type') == 'btc_relative_strength'
        and phase == 'open'
        and ticker != 'RIOT'
    ) else 0.0
    brs_clsk_mara_inversion_risk = side_sign if (
        sig.get('setup_type') == 'btc_relative_strength'
        and ticker in ('CLSK', 'MARA')
    ) else 0.0
    brs_open_btc_neutral_risk = side_sign if (
        sig.get('setup_type') == 'btc_relative_strength'
        and phase == 'open'
        and str(btc_ctx.get('regime') or '').lower() == 'neutral'
    ) else 0.0
    session_range_pos = _num(ind.get('session_range_pos'), 0.5)
    brs_open_low_range_risk = side_sign if (
        sig.get('setup_type') == 'btc_relative_strength'
        and phase == 'open'
        and session_range_pos <= 0.31
    ) else 0.0
    brs_open_low_range_020 = side_sign if (
        sig.get('setup_type') == 'btc_relative_strength' and phase == 'open' and session_range_pos <= 0.20
    ) else 0.0
    brs_open_low_range_025 = side_sign if (
        sig.get('setup_type') == 'btc_relative_strength' and phase == 'open' and session_range_pos <= 0.25
    ) else 0.0
    brs_open_low_range_035 = side_sign if (
        sig.get('setup_type') == 'btc_relative_strength' and phase == 'open' and session_range_pos <= 0.35
    ) else 0.0
    brs_open_low_range_040 = side_sign if (
        sig.get('setup_type') == 'btc_relative_strength' and phase == 'open' and session_range_pos <= 0.40
    ) else 0.0
    brs_open_low_range_045 = side_sign if (
        sig.get('setup_type') == 'btc_relative_strength' and phase == 'open' and session_range_pos <= 0.45
    ) else 0.0
    brs_open_side_bad_range_025 = side_sign if (
        sig.get('setup_type') == 'btc_relative_strength'
        and phase == 'open'
        and ((side == 'LONG' and session_range_pos <= 0.25) or (side == 'SHORT' and session_range_pos >= 0.75))
    ) else 0.0
    brs_open_side_bad_range_031 = side_sign if (
        sig.get('setup_type') == 'btc_relative_strength'
        and phase == 'open'
        and ((side == 'LONG' and session_range_pos <= 0.31) or (side == 'SHORT' and session_range_pos >= 0.69))
    ) else 0.0
    brs_open_side_bad_range_040 = side_sign if (
        sig.get('setup_type') == 'btc_relative_strength'
        and phase == 'open'
        and ((side == 'LONG' and session_range_pos <= 0.40) or (side == 'SHORT' and session_range_pos >= 0.60))
    ) else 0.0
    brs_wide_spread_risk = side_sign if (
        sig.get('setup_type') == 'btc_relative_strength'
        and _num(ind.get('spread_vs_rolling_median'), 1.0) >= 1.005
    ) else 0.0
    brs_open_book_worsening_risk = side_sign if (
        sig.get('setup_type') == 'btc_relative_strength'
        and phase == 'open'
        and side_sign * _num(ind.get('quote_imbalance_delta_5s')) >= 0.351
    ) else 0.0
    signed_vwap_dist = side_sign * _num(ind.get('vwap_dist'))
    signed_mom15 = side_sign * mom15
    signed_mom30 = side_sign * mom30
    signed_mom60 = side_sign * mom60
    signed_rel60 = side_sign * rel_value
    signed_session_return = side_sign * _num(ind.get('session_return_pct'))
    flow10_aligned = None if flow_buy_pct_10s is None else side_sign * (flow_buy_pct_10s - 50.0)
    flow30_aligned = None if flow_buy_pct is None else side_sign * (flow_buy_pct - 50.0)
    quote_delta_aligned = side_sign * _num(ind.get('quote_imbalance_delta_5s'))
    brs_open_flow_quote_failure = side_sign if (
        sig.get('setup_type') == 'btc_relative_strength'
        and phase == 'open'
        and flow30_aligned is not None
        and flow30_aligned <= 0
        and quote_delta_aligned >= 0.25
    ) else 0.0
    brs_open_flow_book_failure = side_sign if (
        sig.get('setup_type') == 'btc_relative_strength'
        and phase == 'open'
        and signed_vwap_dist <= 0
        and flow10_aligned is not None
        and flow10_aligned <= 0
        and flow30_aligned is not None
        and flow30_aligned <= 0
        and quote_delta_aligned >= 0.25
    ) else 0.0
    brs_open_flow_book_failure_loose = side_sign if (
        sig.get('setup_type') == 'btc_relative_strength'
        and phase == 'open'
        and signed_vwap_dist <= 0
        and flow30_aligned is not None
        and flow30_aligned <= 0
        and quote_delta_aligned >= 0.25
    ) else 0.0
    brs_open_session_neutral_failure = side_sign if (
        sig.get('setup_type') == 'btc_relative_strength'
        and phase == 'open'
        and signed_session_return <= 0
        and str(btc_ctx.get('regime') or '').lower() == 'neutral'
    ) else 0.0
    brs_open_range_neutral_mom_failure = side_sign if (
        sig.get('setup_type') == 'btc_relative_strength'
        and phase == 'open'
        and signed_mom30 <= 0
        and str(btc_ctx.get('regime') or '').lower() == 'neutral'
        and ((side == 'LONG' and session_range_pos <= 0.25) or (side == 'SHORT' and session_range_pos >= 0.75))
    ) else 0.0
    brs_open_three_tape_failure = side_sign if (
        sig.get('setup_type') == 'btc_relative_strength'
        and phase == 'open'
        and signed_mom15 <= 0
        and signed_mom60 <= 0
        and signed_rel60 <= 0
        and flow30_aligned is not None
        and flow30_aligned <= 0
    ) else 0.0
    brs_alignment_failure = 0.0
    brs_open_alignment_failure = 0.0
    brs_normal_alignment_failure = 0.0
    if sig.get('setup_type') == 'btc_relative_strength':
        fail_reasons = 0
        if not (vwap > 0 if side == 'LONG' else vwap < 0):
            fail_reasons += 1
        if not (momentum > 0 if side == 'LONG' else momentum < 0):
            fail_reasons += 1
        if not (rel_value > 0 if side == 'LONG' else rel_value < 0):
            fail_reasons += 1
        regime = str(btc_ctx.get('regime') or '').lower()
        if side == 'LONG' and regime in ('bear', 'bear_momentum', 'impulse_down'):
            fail_reasons += 1
        if side == 'SHORT' and regime in ('bull', 'bull_momentum', 'impulse_up'):
            fail_reasons += 1
        if basket.get('state') != 'confirmed':
            fail_reasons += 1
        if flow_buy_pct is not None:
            if side == 'LONG' and flow_buy_pct < 52:
                fail_reasons += 1
            if side == 'SHORT' and flow_buy_pct > 48:
                fail_reasons += 1
        if btc_chop:
            fail_reasons += 1
        failure_score = min(1.0, fail_reasons / 4.0)
        brs_alignment_failure = side_sign * failure_score
        brs_open_alignment_failure = brs_alignment_failure if phase == 'open' else 0.0
        brs_normal_alignment_failure = brs_alignment_failure if phase == 'normal' else 0.0

    return {
        'ema': ema,
        'vwap': vwap,
        'momentum': momentum,
        'btc': btc,
        'relative': rel,
        'miner': miner,
        'burst': burst,
        'flow_contra': flow_contra,
        'btc_chop': btc_chop,
        'exec_penalty': exec_penalty,
        'open_phase': open_phase,
        'midday_phase': midday_phase,
        'riot_short_penalty': 1.0 if ticker == 'RIOT' and side == 'SHORT' else 0.0,
        'riot_long_penalty': 1.0 if ticker == 'RIOT' and side == 'LONG' else 0.0,
        'vwap_sigma_ext': 1.0 if abs(_num(ind.get('vwap_dist_sigma'))) >= 1.0 else 0.0,
        'btc_mom_abs': abs(btc_mom),
        'flow_pressure_abs': flow_pressure_abs,
        'brs_weak_followthrough': brs_weak_followthrough,
        'brs_open_risk': brs_open_risk,
        'brs_bear_normal_risk': brs_bear_normal_risk,
        'brs_open_weak_followthrough': brs_open_weak_followthrough,
        'timeout_decay_risk': timeout_decay_risk,
        'brs_open_continuation_quality': brs_open_continuation_quality,
        'brs_normal_continuation_quality': brs_normal_continuation_quality,
        'medium_brs_penalty': medium_brs_penalty,
        'brs_open_medium_risk': brs_open_medium_risk,
        'brs_open_inversion_risk': brs_open_inversion_risk,
        'brs_open_non_riot_inversion_risk': brs_open_non_riot_inversion_risk,
        'brs_clsk_mara_inversion_risk': brs_clsk_mara_inversion_risk,
        'brs_open_btc_neutral_risk': brs_open_btc_neutral_risk,
        'brs_open_low_range_risk': brs_open_low_range_risk,
        'brs_open_low_range_020': brs_open_low_range_020,
        'brs_open_low_range_025': brs_open_low_range_025,
        'brs_open_low_range_035': brs_open_low_range_035,
        'brs_open_low_range_040': brs_open_low_range_040,
        'brs_open_low_range_045': brs_open_low_range_045,
        'brs_open_side_bad_range_025': brs_open_side_bad_range_025,
        'brs_open_side_bad_range_031': brs_open_side_bad_range_031,
        'brs_open_side_bad_range_040': brs_open_side_bad_range_040,
        'brs_wide_spread_risk': brs_wide_spread_risk,
        'brs_open_book_worsening_risk': brs_open_book_worsening_risk,
        'brs_open_flow_quote_failure': brs_open_flow_quote_failure,
        'brs_open_flow_book_failure': brs_open_flow_book_failure,
        'brs_open_flow_book_failure_loose': brs_open_flow_book_failure_loose,
        'brs_open_session_neutral_failure': brs_open_session_neutral_failure,
        'brs_open_range_neutral_mom_failure': brs_open_range_neutral_mom_failure,
        'brs_open_three_tape_failure': brs_open_three_tape_failure,
        'brs_alignment_failure': brs_alignment_failure,
        'brs_open_alignment_failure': brs_open_alignment_failure,
        'brs_normal_alignment_failure': brs_normal_alignment_failure,
        'setup_btc_relative_strength': side_sign if _setup(sig, 'btc_relative_strength') else 0.0,
        'setup_momentum_breakout': side_sign if _setup(sig, 'momentum_breakout') else 0.0,
        'setup_trend_pullback': side_sign if _setup(sig, 'trend_pullback') else 0.0,
        'setup_flow_exhaustion_fade': side_sign if _setup(sig, 'flow_exhaustion_fade') else 0.0,
        'setup_vwap_reclaim_breakdown': side_sign if _setup(sig, 'vwap_reclaim_breakdown') else 0.0,
    }


def _score_weights(weights: dict, bias: float, feats: dict[str, float],
                   include_details: bool) -> tuple[float, dict[str, float]]:
    score = float(bias or 0.0)
    contributions = {}
    for name, weight in weights.items():
        contribution = float(weight or 0.0) * float(feats.get(name, 0.0) or 0.0)
        if include_details:
            contributions[name] = round(contribution, 6)
        score += contribution
    return score, contributions


def _norm(value: Any) -> str:
    return str(value).strip().lower().replace('-', '_')


def _values(mapping: dict, *keys: str) -> list[Any]:
    out = []
    for key in keys:
        if key not in mapping:
            continue
        value = mapping.get(key)
        if isinstance(value, (list, tuple, set)):
            out.extend(value)
        elif value not in (None, ''):
            out.append(value)
    return out


def _route_matches(route: dict, sig: dict, feats: dict[str, float]) -> bool:
    match = route.get('match') if isinstance(route.get('match'), dict) else {}
    if not match:
        return True
    if _values(match, 'ticker', 'tickers'):
        wanted = {_norm(value).upper() for value in _values(match, 'ticker', 'tickers')}
        if str(sig.get('ticker') or '').upper() not in wanted:
            return False
    if _values(match, 'setup', 'setup_type', 'setups', 'setup_types'):
        wanted = {_norm(value) for value in _values(match, 'setup', 'setup_type', 'setups', 'setup_types')}
        if _norm(sig.get('setup_type')) not in wanted:
            return False
    if _values(match, 'session_phase', 'session_phases', 'phase', 'phases'):
        wanted = {_norm(value) for value in _values(match, 'session_phase', 'session_phases', 'phase', 'phases')}
        phase = _norm(sig.get('session_phase') or '')
        if phase == 'normal':
            phase = 'late'
        normalized = {'normal' if value in ('late', 'regular') else value for value in wanted}
        if _norm(sig.get('session_phase') or '') not in wanted and phase not in wanted and phase not in normalized:
            return False
    if _values(match, 'side', 'sides', 'original_side'):
        wanted = {str(value).strip().upper() for value in _values(match, 'side', 'sides', 'original_side')}
        if str(sig.get('side') or '').upper() not in wanted:
            return False
    for feature, minimum in dict(match.get('min_features') or {}).items():
        if float(feats.get(str(feature), 0.0) or 0.0) < float(minimum):
            return False
    for feature, maximum in dict(match.get('max_features') or {}).items():
        if float(feats.get(str(feature), 0.0) or 0.0) > float(maximum):
            return False
    for clause in match.get('feature_filters') or ():
        if not _feature_filter_matches(clause, feats):
            return False
    if {'feature', 'op', 'value'} <= set(match):
        return _feature_filter_matches(match, feats)
    return True


def _feature_filter_matches(clause: dict, feats: dict[str, float]) -> bool:
    value = float(feats.get(str(clause.get('feature') or ''), 0.0) or 0.0)
    target = float(clause.get('value') or 0.0)
    op = str(clause.get('op') or '==').strip()
    if op in ('>=', 'gte'):
        return value >= target
    if op in ('>', 'gt'):
        return value > target
    if op in ('<=', 'lte'):
        return value <= target
    if op in ('<', 'lt'):
        return value < target
    if op in ('!=', 'ne'):
        return value != target
    return value == target


def _route_action(route: dict) -> str:
    action = str(route.get('action') or 'score').strip().lower().replace('-', '_')
    if action not in routed_scoring_profile.VALID_ACTIONS:
        raise ValueError(f'unsupported routed scoring action: {action}')
    return action


def score_signal(profile: dict, sig: dict, ind: dict | None = None, btc_ind: dict | None = None,
                 include_details: bool = True) -> dict:
    weights = profile.get('weights') or {}
    feats = features_from_signal(sig, ind, btc_ind)
    score, contributions = _score_weights(weights, float(profile.get('bias') or 0.0), feats, include_details)
    chosen_side = 'LONG' if score > 0 else ('SHORT' if score < 0 else sig.get('side'))
    result = {
        'profile': profile.get('name'),
        'score': round(score, 6),
        'side': chosen_side,
    }
    selected_route = None
    route_evaluations = []
    routes = profile.get('routes') if isinstance(profile.get('routes'), list) else []
    for raw_route in routes:
        if not isinstance(raw_route, dict):
            continue
        matched = _route_matches(raw_route, sig, feats)
        route_evaluations.append({
            'route': raw_route.get('name') or 'route',
            'action': raw_route.get('action') or 'score',
            'matched': bool(matched),
        })
        if not matched:
            continue
        action = _route_action(raw_route)
        selected_route = {
            'route': raw_route.get('name') or 'route',
            'action': action,
            'match': dict(raw_route.get('match') or {}),
        }
        if action in ('skip', 'veto'):
            chosen_side = 'SKIP'
            break
        if action == 'force_long':
            chosen_side = 'LONG'
            break
        if action == 'force_short':
            chosen_side = 'SHORT'
            break
        if action == 'fallback':
            chosen_side = sig.get('side')
            break
        route_score, route_contributions = _score_weights(
            raw_route.get('weights') or {},
            float(raw_route.get('bias') or 0.0),
            feats,
            include_details,
        )
        score = route_score
        contributions = route_contributions
        chosen_side = 'LONG' if score > 0 else ('SHORT' if score < 0 else sig.get('side'))
        break
    if routes:
        result.update({
            'routed_scoring_profile': True,
            'route_evaluations': route_evaluations,
            'selected_route': selected_route,
            'side': chosen_side,
            'score': round(score, 6),
        })
    if include_details:
        result['features'] = feats
        result['contributions'] = contributions
    return result


def apply_profile(profile: dict | None, sig: dict, ind: dict | None = None, btc_ind: dict | None = None,
                  include_details: bool = True) -> dict:
    if not profile:
        return sig
    result = score_signal(profile, sig, ind, btc_ind, include_details=include_details)
    original_side = sig.get('side')
    original_score = sig.get('score')
    if result['side'] in ('LONG', 'SHORT'):
        sig['side'] = result['side']
    elif result['side'] == 'SKIP':
        sig['scoring_profile'] = result
        sig['scoring_profile_rejected'] = True
        sig['scoring_profile_original_side'] = original_side
        sig['scoring_profile_original_score'] = original_score
        sig['reasons'] = list(sig.get('reasons') or [])
        route = result.get('selected_route') or {}
        sig['reasons'].append(
            f"scoring_profile:{profile.get('name')} route={route.get('route') or 'unknown'} skip"
        )
        return sig
    signed_abs = abs(float(original_score or 0.0))
    sig['score'] = signed_abs if sig['side'] == 'LONG' else -signed_abs
    sig['scoring_profile'] = result
    if profile.get('exit_management'):
        sig['scoring_profile_exit_management'] = dict(profile.get('exit_management') or {})
    sig['scoring_profile_original_side'] = original_side
    sig['scoring_profile_original_score'] = original_score
    sig['reasons'] = list(sig.get('reasons') or [])
    sig['reasons'].append(
        f"scoring_profile:{profile.get('name')} score={result['score']:+.3f} "
        f"side={original_side}->{sig['side']}"
    )
    return sig
