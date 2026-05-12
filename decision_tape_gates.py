"""Entry-quality gate helpers for reusable decision-tape simulations.

These gates are replay/research utilities. They use only causal fields captured
at the decision timestamp and are intentionally side-effect free.
"""
from __future__ import annotations

from typing import Any


BULLISH_BTC_REGIMES = {'bull', 'bull_momentum', 'impulse_up'}


def _num(value: Any) -> float | None:
    try:
        if value in (None, ''):
            return None
        return float(value)
    except Exception:
        return None


def _text(value: Any) -> str:
    if value is None:
        return ''
    return str(value).strip().lower()


def _gate_features(row: dict) -> dict:
    features = dict(row.get('gate_features') or {})
    features.setdefault('ticker_session_return_pct', row.get('ticker_session_return_pct'))
    features.setdefault('vwap_dist', row.get('vwap_dist'))
    features.setdefault('vwap_dist_sigma', row.get('vwap_dist_sigma'))
    features.setdefault('session_range_pos', row.get('session_range_pos'))
    features.setdefault('flow_30s_buy_pct', row.get('flow_30s_buy_pct'))
    features.setdefault('stock_minus_btc_implied_60s', row.get('stock_minus_btc_implied_60s'))
    features.setdefault('btc_regime', row.get('btc_regime'))
    return features


def requires_gate_features(gate: dict | None) -> set[str]:
    if not gate or str(gate.get('mode') or 'off') == 'off':
        return set()
    mode = str(gate.get('mode') or gate.get('name') or '').lower()
    required = {'ticker_session_return_pct'}
    if 'vwap' in mode:
        required.add('vwap_dist')
    if 'btc-not-bull' in mode:
        required.add('btc_regime')
    if 'weak-flow' in mode:
        required.add('flow_30s_buy_pct')
    if 'lower-half-range' in mode:
        required.add('session_range_pos')
    if 'stock-lagging' in mode or 'btc-relative' in mode:
        required.add('stock_minus_btc_implied_60s')
    return required


def missing_gate_features(row: dict, gate: dict | None) -> list[str]:
    features = _gate_features(row)
    missing = []
    for name in sorted(requires_gate_features(gate)):
        value = features.get(name)
        if name == 'btc_regime':
            if not _text(value):
                missing.append(name)
        elif _num(value) is None:
            missing.append(name)
    return missing


def gate_reason(gate: dict | None, row: dict, side: str) -> str | None:
    """Return a skip reason if the gate blocks this decision-tape row."""
    if not gate or str(gate.get('mode') or 'off') == 'off':
        return None
    if str(side or '').upper() != 'LONG':
        return None

    mode = str(gate.get('mode') or gate.get('name') or '').lower()
    features = _gate_features(row)
    threshold = float(
        gate.get('ticker_session_return_below_pct',
                 gate.get('threshold_pct', gate.get('threshold', -0.5)))
    )
    ticker_ret = _num(features.get('ticker_session_return_pct'))
    if ticker_ret is None or ticker_ret >= threshold:
        return None

    if mode == 'ticker-negative':
        return f'{mode}:ticker_session_return<{threshold:g}'

    vwap_dist = _num(features.get('vwap_dist'))
    below_vwap = vwap_dist is not None and vwap_dist < 0
    if mode in {
        'ticker-negative-below-vwap',
        'ticker-negative-below-vwap-btc-not-bull',
        'ticker-negative-below-vwap-weak-flow',
        'ticker-negative-below-vwap-lower-half-range',
        'ticker-negative-below-vwap-stock-lagging',
    } and not below_vwap:
        return None

    if mode == 'ticker-negative-below-vwap':
        return f'{mode}:ticker_session_return<{threshold:g}'

    if mode == 'ticker-negative-btc-not-bull':
        btc_regime = _text(features.get('btc_regime'))
        bullish = set(gate.get('btc_bullish_regimes') or BULLISH_BTC_REGIMES)
        if btc_regime not in bullish:
            return f'{mode}:ticker_session_return<{threshold:g}'
        return None

    if mode == 'ticker-negative-below-vwap-btc-not-bull':
        btc_regime = _text(features.get('btc_regime'))
        bullish = {str(x).lower() for x in (gate.get('btc_bullish_regimes') or BULLISH_BTC_REGIMES)}
        if btc_regime not in bullish:
            return f'{mode}:ticker_session_return<{threshold:g}'
        return None

    if mode == 'ticker-negative-below-vwap-weak-flow':
        buy_pct = _num(features.get('flow_30s_buy_pct'))
        max_buy_pct = float(gate.get('flow_30s_buy_pct_below', 50.0))
        if buy_pct is not None and buy_pct < max_buy_pct:
            return f'{mode}:ticker_session_return<{threshold:g}'
        return None

    if mode == 'ticker-negative-below-vwap-lower-half-range':
        range_pos = _num(features.get('session_range_pos'))
        max_pos = float(gate.get('session_range_pos_below', 0.5))
        if range_pos is not None and range_pos < max_pos:
            return f'{mode}:ticker_session_return<{threshold:g}'
        return None

    if mode in {'ticker-negative-below-vwap-stock-lagging', 'ticker-negative-below-vwap-btc-relative'}:
        rel = _num(features.get('stock_minus_btc_implied_60s'))
        max_rel = float(gate.get('stock_minus_btc_implied_60s_below', 0.0))
        if rel is not None and rel < max_rel:
            return f'{mode}:ticker_session_return<{threshold:g}'
        return None

    return None
