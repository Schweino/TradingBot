"""Canonical schema helpers for Step 2 candidate/profile artifacts."""
from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import semantic_config
import routed_profile_safety
import step2_execution_contract
import step2_parity_contract
import step2_quote_aware_guard
import tournament_safety


HERE = os.path.dirname(os.path.abspath(__file__))
CT = ZoneInfo('America/Chicago')
SCHEMA_VERSION = 1


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ''):
            return default
        return float(value)
    except Exception:
        return default


def _int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ''):
            return default
        return int(value)
    except Exception:
        return default


def _clean_weights(weights: Any) -> dict[str, float]:
    if not isinstance(weights, dict):
        return {}
    return {
        str(k): round(float(v), 8)
        for k, v in sorted(weights.items())
        if abs(float(v or 0.0)) > 1e-12
    }


def score(row: dict[str, Any]) -> float:
    for key in ('step2_pnl', 'pnl'):
        if row.get(key) is not None:
            return _num(row.get(key), float('-inf'))
    for key in ('decision_full', 'result'):
        value = row.get(key)
        if isinstance(value, dict) and value.get('pnl') is not None:
            return _num(value.get('pnl'), float('-inf'))
    score_obj = row.get('score') if isinstance(row.get('score'), dict) else {}
    result = score_obj.get('result') if isinstance(score_obj.get('result'), dict) else {}
    if result.get('pnl') is not None:
        return _num(result.get('pnl'), float('-inf'))
    step2 = row.get('step2') if isinstance(row.get('step2'), dict) else {}
    if step2.get('pnl') is not None:
        return _num(step2.get('pnl'), float('-inf'))
    return float('-inf')


def _decision_full(row: dict[str, Any]) -> dict[str, Any]:
    if isinstance(row.get('decision_full'), dict):
        return row['decision_full']
    if isinstance(row.get('result'), dict):
        return row['result']
    score_obj = row.get('score') if isinstance(row.get('score'), dict) else {}
    if isinstance(score_obj.get('result'), dict):
        return score_obj['result']
    return {}


def contracts(config: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = config or _read_config()
    parity = step2_parity_contract.contract(cfg)
    return {
        'step2_parity_contract_hash': step2_parity_contract.contract_hash(parity),
        'step2_parity_contract': parity,
        'step2_execution_contract_hash': step2_execution_contract.execution_contract_hash(cfg),
        'step2_execution_contract': step2_execution_contract.execution_contract(cfg),
        'semantic_config_section_hashes': semantic_config.section_hashes(cfg),
        'compiled_tape_semantic_hash': semantic_config.combined_hash(
            semantic_config.COMPILED_TAPE_SECTIONS,
            cfg,
        ),
    }


def _read_config() -> dict[str, Any]:
    try:
        with open(os.path.join(HERE, 'trading_config.json'), 'r', encoding='utf-8-sig') as f:
            return json.load(f)
    except Exception:
        return {}


def normalize(row: dict[str, Any],
              context: dict[str, Any] | None = None,
              source_payload: dict[str, Any] | None = None) -> dict[str, Any]:
    context = context or {}
    source_payload = source_payload or {}
    weights = _clean_weights(row.get('weights'))
    routes = row.get('routes') if isinstance(row.get('routes'), list) else []
    route_audit = row.get('route_audit') if isinstance(row.get('route_audit'), dict) else {}
    route_safety = routed_profile_safety.evaluate_candidate({
        **row,
        'routes': routes,
        'route_audit': route_audit,
    }) if routes else {'ok': True, 'skipped': True, 'status': 'not_routed'}
    name = str(row.get('variant') or row.get('name') or 'candidate')
    bias = round(_num(row.get('bias'), 0.0), 8)
    decision = _decision_full(row)
    step2_pnl = score(row)
    exit_replay_model = (
        row.get('exit_replay_model')
        or decision.get('exit_replay_model')
        or context.get('exit_replay_model')
        or source_payload.get('exit_replay_model')
    )
    active_pnl = context.get('active_step2_pnl', source_payload.get('active_step2_pnl'))
    target_pnl = context.get('target_pnl', source_payload.get('target_pnl'))
    delta = None
    delta_pct = None
    if active_pnl is not None and step2_pnl != float('-inf'):
        active = _num(active_pnl, 0.0)
        delta = round(step2_pnl - active, 6)
        if abs(active) > 1e-12:
            delta_pct = round((step2_pnl - active) / abs(active) * 100.0, 6)
    run_contracts = dict(context.get('contracts') or {})
    if not run_contracts:
        for key in (
            'step2_parity_contract_hash',
            'step2_execution_contract_hash',
            'step2_parity_contract',
            'step2_execution_contract',
        ):
            value = row.get(key, source_payload.get(key))
            if value is not None:
                run_contracts[key] = value
    if not run_contracts:
        run_contracts = contracts()
    if row.get('model_id'):
        model_id = row.get('model_id')
    elif routes:
        model_id = tournament_safety.stable_json_hash({
            'name': name,
            'weights': weights,
            'bias': bias,
            'routes': routes,
        }, length=20)
    else:
        model_id = tournament_safety.model_id(name, weights, bias)
    if row.get('family_id'):
        family_id = row.get('family_id')
    elif routes:
        family_id = tournament_safety.stable_json_hash([
            {
                'action': route.get('action'),
                'match': route.get('match'),
                'weights': route.get('weights'),
                'bias': route.get('bias'),
            }
            for route in routes
            if isinstance(route, dict)
        ], length=12)
    else:
        family_id = tournament_safety.variant_family(weights)
    quote_aware_candidate = {
        **row,
        'exit_replay_model': exit_replay_model,
        'step2': {'exit_replay_model': exit_replay_model},
        'data': {'exit_replay_model': exit_replay_model},
    }
    quote_aware_gate = step2_quote_aware_guard.candidate_gate(quote_aware_candidate)
    return {
        'candidate_schema_version': SCHEMA_VERSION,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'variant': name,
        'name': name,
        'exit_replay_model': exit_replay_model,
        'required_exit_replay_model': step2_quote_aware_guard.REQUIRED_EXIT_REPLAY_MODEL,
        'quote_aware_outcome_gate': quote_aware_gate,
        'model_id': model_id,
        'family_id': family_id,
        'bias': bias,
        'weights': weights,
        'routes': routes,
        'routed_scoring_profile': bool(row.get('routed_scoring_profile') or routes),
        'route_audit': route_audit,
        'routed_profile_safety': route_safety,
        'step2': {
            'pnl': None if step2_pnl == float('-inf') else round(float(step2_pnl), 6),
            'exit_replay_model': exit_replay_model,
            'required_exit_replay_model': step2_quote_aware_guard.REQUIRED_EXIT_REPLAY_MODEL,
            'trades': _int(row.get('step2_trades', decision.get('trades')), 0),
            'wins': _int(row.get('step2_wins', decision.get('wins')), 0),
            'losses': _int(row.get('step2_losses', decision.get('losses')), 0),
            'win_rate_pct': row.get('step2_win_rate_pct', decision.get('win_rate_pct')),
            'by_ticker': row.get('by_ticker', decision.get('by_ticker')),
            'by_day': row.get('by_day', decision.get('by_day')),
            'by_side': row.get('by_side', decision.get('by_side')),
            'skipped': row.get('skipped', decision.get('skipped')),
        },
        'baseline': {
            'active_step2_pnl': active_pnl,
            'target_pnl': target_pnl,
            'delta_vs_active': delta,
            'delta_pct_vs_active': delta_pct,
        },
        'contracts': run_contracts,
        'data': {
            'start_balance': context.get('start_balance', source_payload.get('start_balance')),
            'compiled_decision_tape': context.get(
                'compiled_decision_tape',
                source_payload.get('compiled_decision_tape') or source_payload.get('compiled_tape_manifest'),
            ),
            'exit_replay_model': exit_replay_model,
            'required_exit_replay_model': step2_quote_aware_guard.REQUIRED_EXIT_REPLAY_MODEL,
            'scored_total': context.get('scored_total', source_payload.get('scored_total')),
        },
        'source': {
            'script': context.get('script', source_payload.get('script')),
            'artifact': context.get('artifact', source_payload.get('candidate_path')),
            'rank': context.get('rank'),
        },
    }


def rows_from_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        return []
    if isinstance(payload.get('weights'), dict):
        return [payload]
    rows: list[dict[str, Any]] = []
    for key in (
        'winners',
        'leaderboard',
        'candidates',
        'results',
        'profiles',
        'robustness_adjusted_leaderboard',
        'recommended_leaderboard',
        'standard_winners',
        'standard_leaderboard',
        'standard_candidates',
        'standard_results',
        'standard_profiles',
        'standard_robustness_adjusted_leaderboard',
        'standard_recommended_leaderboard',
    ):
        values = payload.get(key)
        if isinstance(values, list):
            rows.extend(row for row in values if isinstance(row, dict))
    return rows


def decorate_payload(payload: dict[str, Any],
                     context: dict[str, Any] | None = None,
                     max_rows: int = 100) -> dict[str, Any]:
    context = dict(context or {})
    payload['candidate_schema_version'] = SCHEMA_VERSION
    payload.setdefault('candidate_contracts', contracts())
    context.setdefault('contracts', payload.get('candidate_contracts') or {})
    for key, out_key in (
        ('winners', 'standard_winners'),
        ('leaderboard', 'standard_leaderboard'),
        ('candidates', 'standard_candidates'),
        ('results', 'standard_results'),
        ('robustness_adjusted_leaderboard', 'standard_robustness_adjusted_leaderboard'),
        ('recommended_leaderboard', 'standard_recommended_leaderboard'),
    ):
        values = payload.get(key)
        if isinstance(values, list):
            payload[out_key] = [
                normalize(row, {**context, 'rank': idx + 1}, payload)
                for idx, row in enumerate(values[:max_rows])
                if isinstance(row, dict)
            ]
    return payload


def select(payload: dict[str, Any], variant: str = '', rank: int = 1) -> dict[str, Any]:
    rows = rows_from_payload(payload)
    if not rows:
        raise RuntimeError('candidate artifact did not contain any rows with weights')
    if variant:
        for row in rows:
            if str(row.get('variant') or row.get('name') or '') == variant:
                return row
        raise RuntimeError(f'variant not found in artifact: {variant}')
    if int(rank) == 1 and isinstance(payload.get('best'), dict):
        best = payload['best']
        if isinstance(best.get('weights'), dict):
            return best
    ordered: list[dict[str, Any]] = []
    for key in ('winners', 'leaderboard', 'raw_leaderboard'):
        values = payload.get(key)
        if isinstance(values, list):
            ordered.extend(row for row in values if isinstance(row, dict))
        if ordered:
            break
    if not ordered:
        ordered = sorted(rows, key=score, reverse=True)
    idx = max(0, int(rank) - 1)
    if idx >= len(ordered):
        raise RuntimeError(f'rank {rank} is outside artifact row count {len(ordered)}')
    return ordered[idx]
