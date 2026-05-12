"""Routed scoring profiles for Step 2 compiled decision-tape research.

A normal scoring variant uses one weight vector for every opportunity. A routed
variant keeps that fallback vector, then applies ordered route overrides to
specific ticker/setup/session/feature slices. This lets the hunter test ideas
like "score RIOT open BRS differently" or "skip this losing setup bucket"
without rebuilding the compiled cache.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np

import scoring_variant_lab_fast as fast


SIDE_SKIP = 0
SIDE_LONG = 1
SIDE_SHORT = -1
VALID_ACTIONS = {'score', 'skip', 'veto', 'force_long', 'force_short', 'fallback'}
FEATURE_INDEX = {name: idx for idx, name in enumerate(fast.FEATURE_NAMES)}


@dataclass(frozen=True)
class Route:
    name: str
    match: dict[str, Any] = field(default_factory=dict)
    weights: dict[str, float] = field(default_factory=dict)
    bias: float = 0.0
    action: str = 'score'


@dataclass(frozen=True)
class RoutedVariant:
    name: str
    weights: dict[str, float]
    bias: float = 0.0
    routes: tuple[Route, ...] = ()


def route(name: str, match: dict[str, Any] | None = None,
          weights: dict[str, float] | None = None, bias: float = 0.0,
          action: str = 'score') -> Route:
    return Route(
        name=str(name),
        match=dict(match or {}),
        weights=_clean_weights(weights or {}),
        bias=float(bias or 0.0),
        action=_normal_action(action),
    )


def routed_variant(name: str, fallback_weights: dict[str, float], bias: float = 0.0,
                   routes: list[Route] | tuple[Route, ...] | None = None) -> RoutedVariant:
    return RoutedVariant(
        name=str(name),
        weights=_clean_weights(fallback_weights),
        bias=float(bias or 0.0),
        routes=tuple(routes or ()),
    )


def is_routed_variant(variant: Any) -> bool:
    return isinstance(variant, RoutedVariant) or bool(getattr(variant, 'routes', None))


def route_to_dict(item: Route) -> dict:
    return {
        'name': item.name,
        'match': dict(item.match or {}),
        'weights': dict(item.weights or {}),
        'bias': float(item.bias or 0.0),
        'action': _normal_action(item.action),
    }


def routes_to_dicts(variant: Any) -> list[dict]:
    return [route_to_dict(_coerce_route(item)) for item in (getattr(variant, 'routes', None) or [])]


def route_attribution(compiled: dict, variant: Any) -> dict[str, np.ndarray]:
    rows = int(compiled.get('rows') or len(compiled.get('original_side') or []))
    names = np.full(rows, 'fallback', dtype=object)
    actions = np.full(rows, 'score', dtype=object)
    matched = np.zeros(rows, dtype=bool)
    if not is_routed_variant(variant):
        return {'route_name': names, 'route_action': actions, 'route_matched': matched}
    for route_item in (getattr(variant, 'routes', None) or []):
        item = _coerce_route(route_item)
        mask = route_mask(compiled, item)
        if not np.any(mask):
            continue
        names[mask] = item.name
        actions[mask] = _normal_action(item.action)
        matched[mask] = True
    return {'route_name': names, 'route_action': actions, 'route_matched': matched}


def route_audit(compiled: dict, variant: Any, sides: np.ndarray | None = None,
                trace_rows: list[dict] | None = None) -> dict:
    if not is_routed_variant(variant):
        return {'routed_scoring_profile': False, 'skipped': True}
    attrs = route_attribution(compiled, variant)
    route_names = attrs['route_name']
    route_actions = attrs['route_action']
    total_rows = int(compiled.get('rows') or len(route_names))
    sides = np.asarray(sides if sides is not None else side_matrix(compiled, [variant])[0])
    rows = []
    for route_item in (getattr(variant, 'routes', None) or []):
        item = _coerce_route(route_item)
        mask = route_names == item.name
        count = int(np.sum(mask))
        rows.append({
            'route': item.name,
            'action': _normal_action(item.action),
            'match': dict(item.match or {}),
            'matched_opportunities': count,
            'opportunity_share_pct': round(100.0 * count / total_rows, 4) if total_rows else 0.0,
            'long_sides': int(np.sum(mask & (sides == SIDE_LONG))),
            'short_sides': int(np.sum(mask & (sides == SIDE_SHORT))),
            'skipped_sides': int(np.sum(mask & (sides == SIDE_SKIP))),
        })
    fallback_mask = route_names == 'fallback'
    rows.append({
        'route': 'fallback',
        'action': 'score',
        'match': {},
        'matched_opportunities': int(np.sum(fallback_mask)),
        'opportunity_share_pct': round(100.0 * int(np.sum(fallback_mask)) / total_rows, 4) if total_rows else 0.0,
        'long_sides': int(np.sum(fallback_mask & (sides == SIDE_LONG))),
        'short_sides': int(np.sum(fallback_mask & (sides == SIDE_SHORT))),
        'skipped_sides': int(np.sum(fallback_mask & (sides == SIDE_SKIP))),
    })
    if trace_rows:
        _attach_trace_stats(rows, trace_rows)
    return {
        'routed_scoring_profile': True,
        'route_count': len(getattr(variant, 'routes', None) or []),
        'opportunities': total_rows,
        'routes': rows,
    }


def variant_to_dict(variant: Any) -> dict:
    return {
        'name': str(getattr(variant, 'name', 'routed_variant')),
        'weights': dict(getattr(variant, 'weights', {}) or {}),
        'bias': float(getattr(variant, 'bias', 0.0) or 0.0),
        'routes': routes_to_dicts(variant),
    }


def route_from_dict(payload: dict) -> Route:
    return route(
        name=str(payload.get('name') or 'route'),
        match=dict(payload.get('match') or {}),
        weights=dict(payload.get('weights') or {}),
        bias=float(payload.get('bias') or 0.0),
        action=str(payload.get('action') or 'score'),
    )


def variant_from_dict(payload: dict) -> RoutedVariant:
    return routed_variant(
        name=str(payload.get('name') or payload.get('variant') or 'routed_variant'),
        fallback_weights=dict(payload.get('weights') or {}),
        bias=float(payload.get('bias') or 0.0),
        routes=[route_from_dict(item) for item in (payload.get('routes') or [])],
    )


def route_mask(compiled: dict, route_item: Route | dict) -> np.ndarray:
    item = _coerce_route(route_item)
    match = item.match or {}
    rows = int(compiled.get('rows') or len(compiled.get('original_side') or []))
    mask = np.ones(rows, dtype=bool)

    if _has_any(match, 'ticker', 'tickers'):
        mask &= _code_mask(compiled, 'ticker_code', compiled.get('ticker_map') or {}, _values(match, 'ticker', 'tickers'))
    if _has_any(match, 'setup', 'setup_type', 'setups', 'setup_types'):
        mask &= _code_mask(
            compiled,
            'setup_code',
            compiled.get('setup_map') or {},
            _values(match, 'setup', 'setup_type', 'setups', 'setup_types'),
        )
    if _has_any(match, 'day', 'days'):
        mask &= _code_mask(compiled, 'day_code', compiled.get('day_map') or {}, _values(match, 'day', 'days'))
    if _has_any(match, 'side', 'sides', 'original_side'):
        side_values = {_side_code(value) for value in _values(match, 'side', 'sides', 'original_side')}
        mask &= np.isin(np.asarray(compiled['original_side']), list(side_values))
    if _has_any(match, 'session_phase', 'session_phases', 'phase', 'phases'):
        mask &= _phase_mask(compiled, _values(match, 'session_phase', 'session_phases', 'phase', 'phases'))

    for feature, minimum in dict(match.get('min_features') or {}).items():
        mask &= _feature_values(compiled, feature) >= float(minimum)
    for feature, maximum in dict(match.get('max_features') or {}).items():
        mask &= _feature_values(compiled, feature) <= float(maximum)
    for clause in match.get('feature_filters') or ():
        mask &= _feature_filter(compiled, clause)
    if {'feature', 'op', 'value'} <= set(match):
        mask &= _feature_filter(compiled, match)
    return mask


def side_matrix(compiled: dict, variants: list, side_long: int = SIDE_LONG,
                side_short: int = SIDE_SHORT, side_skip: int = SIDE_SKIP) -> np.ndarray:
    rows = int(compiled.get('rows') or len(compiled.get('original_side') or []))
    out = np.zeros((len(variants), rows), dtype=np.int8)
    original = np.asarray(compiled['original_side'], dtype=np.int8)
    for idx, variant in enumerate(variants):
        fallback_weights = dict(getattr(variant, 'weights', {}) or {})
        fallback_bias = float(getattr(variant, 'bias', 0.0) or 0.0)
        scores = score_vector(compiled, fallback_weights, fallback_bias)
        chosen = np.where(scores > 0.0, side_long, np.where(scores < 0.0, side_short, original))
        for route_item in (getattr(variant, 'routes', None) or []):
            item = _coerce_route(route_item)
            action = _normal_action(item.action)
            mask = route_mask(compiled, item)
            if not np.any(mask):
                continue
            if action in ('skip', 'veto'):
                chosen[mask] = side_skip
            elif action == 'force_long':
                chosen[mask] = side_long
            elif action == 'force_short':
                chosen[mask] = side_short
            elif action == 'fallback':
                chosen[mask] = original[mask]
            else:
                route_scores = score_vector(compiled, item.weights, item.bias)
                chosen[mask] = np.where(
                    route_scores[mask] > 0.0,
                    side_long,
                    np.where(route_scores[mask] < 0.0, side_short, original[mask]),
                )
        out[idx] = chosen.astype(np.int8)
    return out


def score_vector(compiled: dict, weights: dict[str, float], bias: float = 0.0) -> np.ndarray:
    features = np.asarray(compiled['features'])
    scores = np.full(features.shape[0], float(bias or 0.0), dtype=np.float64)
    for name, raw_value in (weights or {}).items():
        idx = FEATURE_INDEX.get(str(name))
        if idx is None:
            continue
        scores += features[:, idx] * float(raw_value)
    return scores


def _coerce_route(item: Route | dict) -> Route:
    if isinstance(item, Route):
        return item
    if isinstance(item, dict):
        return route_from_dict(item)
    raise TypeError(f'unsupported route type: {type(item).__name__}')


def _clean_weights(weights: dict[str, Any]) -> dict[str, float]:
    clean = {}
    for key, raw_value in (weights or {}).items():
        value = float(raw_value or 0.0)
        if abs(value) > 1e-12:
            clean[str(key)] = value
    return clean


def _normal_action(action: str) -> str:
    value = str(action or 'score').strip().lower().replace('-', '_')
    if value not in VALID_ACTIONS:
        raise ValueError(f'unsupported routed scoring action: {action}')
    return value


def _has_any(mapping: dict, *keys: str) -> bool:
    return any(key in mapping and mapping.get(key) not in (None, '', []) for key in keys)


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


def _code_mask(compiled: dict, code_key: str, name_map: dict, values: list[Any]) -> np.ndarray:
    wanted_names = {_norm_name(value) for value in values}
    wanted_codes = set()
    for name, raw_code in (name_map or {}).items():
        if _norm_name(name) in wanted_names:
            wanted_codes.add(int(raw_code))
    for value in values:
        try:
            wanted_codes.add(int(value))
        except Exception:
            pass
    return np.isin(np.asarray(compiled[code_key]), list(wanted_codes))


def _phase_mask(compiled: dict, values: list[Any]) -> np.ndarray:
    rows = int(compiled.get('rows') or len(compiled.get('original_side') or []))
    mask = np.zeros(rows, dtype=bool)
    phases = {_norm_name(value) for value in values}
    open_values = _feature_values(compiled, 'open_phase')
    midday_values = _feature_values(compiled, 'midday_phase')
    if phases & {'open', 'opening', 'open_phase'}:
        mask |= open_values > 0.0
    if phases & {'midday', 'mid', 'midday_phase'}:
        mask |= midday_values > 0.0
    if phases & {'late', 'close', 'normal', 'regular'}:
        mask |= (open_values <= 0.0) & (midday_values <= 0.0)
    return mask


def _feature_values(compiled: dict, feature: str) -> np.ndarray:
    idx = FEATURE_INDEX.get(str(feature))
    rows = int(compiled.get('rows') or len(compiled.get('original_side') or []))
    if idx is None:
        return np.zeros(rows, dtype=np.float64)
    return np.asarray(compiled['features'])[:, idx]


def _feature_filter(compiled: dict, clause: dict) -> np.ndarray:
    values = _feature_values(compiled, str(clause.get('feature') or ''))
    op = str(clause.get('op') or '==').strip()
    target = float(clause.get('value') or 0.0)
    if op in ('>=', 'gte'):
        return values >= target
    if op in ('>', 'gt'):
        return values > target
    if op in ('<=', 'lte'):
        return values <= target
    if op in ('<', 'lt'):
        return values < target
    if op in ('!=', 'ne'):
        return values != target
    return values == target


def _side_code(value: Any) -> int:
    text = str(value).strip().upper()
    if text in ('LONG', 'L', '1'):
        return SIDE_LONG
    if text in ('SHORT', 'S', '-1'):
        return SIDE_SHORT
    if text in ('SKIP', 'VETO', '0'):
        return SIDE_SKIP
    raise ValueError(f'unsupported side match: {value}')


def _norm_name(value: Any) -> str:
    return str(value).strip().lower().replace('-', '_')


def _attach_trace_stats(rows: list[dict], trace_rows: list[dict]) -> None:
    by_route = {row['route']: row for row in rows}
    for row in rows:
        row.update({
            'accepted_trades': 0,
            'rejected_opportunities': 0,
            'trace_pnl': 0.0,
            'wins': 0,
            'losses': 0,
            'reject_reasons': {},
        })
    for trace in trace_rows:
        route_name = str(trace.get('route_name') or 'fallback')
        bucket = by_route.get(route_name)
        if bucket is None:
            bucket = {
                'route': route_name,
                'action': trace.get('route_action') or 'unknown',
                'match': {},
                'matched_opportunities': 0,
                'opportunity_share_pct': 0.0,
                'long_sides': 0,
                'short_sides': 0,
                'skipped_sides': 0,
                'accepted_trades': 0,
                'rejected_opportunities': 0,
                'trace_pnl': 0.0,
                'wins': 0,
                'losses': 0,
                'reject_reasons': {},
            }
            rows.append(bucket)
            by_route[route_name] = bucket
        if trace.get('decision_status') == 'accepted':
            pnl = float((trace.get('outcome') or {}).get('pnl') or 0.0)
            bucket['accepted_trades'] += 1
            bucket['trace_pnl'] = round(float(bucket.get('trace_pnl') or 0.0) + pnl, 4)
            bucket['wins'] += 1 if pnl > 0 else 0
            bucket['losses'] += 1 if pnl < 0 else 0
        else:
            bucket['rejected_opportunities'] += 1
            reason = str(trace.get('reject_reason') or 'unknown')
            reasons = bucket.setdefault('reject_reasons', {})
            reasons[reason] = int(reasons.get(reason, 0)) + 1
