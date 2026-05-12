"""Active engine baseline helpers for variant tournament runs.

These helpers intentionally read the promoted paper-trading profile from
``trading_config.json`` and score it with the same stage mechanics as the
candidate variants. They do not mutate live config.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import scoring_compiled_kernels
import routed_scoring_profile
import scoring_variant_lab as slow_lab
import step2_parity_contract
import tournament_safety
import variant_tournament_runner as tournament


HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG = os.path.join(HERE, 'trading_config.json')
MIGRATION_SNAPSHOT_DIR = os.path.join(HERE, 'postmortem', 'config_change_journal', 'snapshots')


def _read_json(path: str) -> dict:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def _safe_gate_mode(name: str | None) -> str | None:
    if not name:
        return None
    return str(name).strip().lower().replace('_', '-')


def load_active_profile(config_path: str = DEFAULT_CONFIG) -> dict:
    config_path = os.path.abspath(config_path)
    cfg = _read_json(config_path)
    smart = cfg.get('smart_entry') or {}
    base_profile = ((smart.get('active_scoring_profile')) or {})
    router = ((smart.get('active_scoring_router')) or {})
    if isinstance(router, dict) and router.get('enabled'):
        profile = dict(router)
        profile['weights'] = dict(profile.get('weights') or base_profile.get('weights') or {})
        profile['bias'] = float(profile.get('bias', base_profile.get('bias', 0.0)) or 0.0)
        profile['routes'] = list(profile.get('routes') or [])
    else:
        profile = base_profile
    if not profile.get('enabled'):
        raise RuntimeError(f'active scoring profile is disabled in {config_path}')
    weights = dict(profile.get('weights') or {})
    if not weights:
        raise RuntimeError(f'active scoring profile has no weights in {config_path}')
    out = {
        'name': profile.get('name') or 'active_scoring_profile',
        'weights': weights,
        'bias': float(profile.get('bias') or 0.0),
    }
    if profile.get('routes'):
        out['routes'] = list(profile.get('routes') or [])
        out['routed_scoring_profile'] = True
    return out


def _profile_from_payload(payload: dict[str, Any], *, source: str = "") -> dict[str, Any] | None:
    weights = dict(payload.get('weights') or {})
    if not weights:
        return None
    out = {
        'name': payload.get('name') or 'reference_scoring_profile',
        'weights': weights,
        'bias': float(payload.get('bias') or 0.0),
    }
    if payload.get('routes'):
        out['routes'] = list(payload.get('routes') or [])
        out['routed_scoring_profile'] = True
    if source:
        out['reference_profile_source'] = source
    return out


def load_reference_profile(config_path: str = DEFAULT_CONFIG) -> dict:
    """Load the active profile, or the preserved pre-migration profile for Step 2 baselines.

    Live-facing callers should keep using load_active_profile(). Step 2 hunts need a
    reference seed even after quote-aware migration disabled the old live profile.
    """
    try:
        return load_active_profile(config_path)
    except RuntimeError as exc:
        disabled = 'active scoring profile is disabled' in str(exc)
        if not disabled:
            raise

    cfg = _read_json(os.path.abspath(config_path))
    smart = cfg.get('smart_entry') or {}
    disabled_profile = smart.get('active_scoring_profile') if isinstance(smart.get('active_scoring_profile'), dict) else {}
    for key in ('reference_profile_path', 'disabled_reference_profile_path', 'previous_profile_path'):
        ref_path = disabled_profile.get(key)
        if not ref_path:
            continue
        path = Path(ref_path)
        if not path.is_absolute():
            path = Path(HERE) / path
        if path.exists():
            payload = _read_json(str(path))
            profile = _profile_from_payload(payload.get('profile') if isinstance(payload.get('profile'), dict) else payload, source=str(path.resolve()))
            if profile:
                profile['active_profile_disabled'] = True
                return profile

    snapshot_root = Path(MIGRATION_SNAPSHOT_DIR)
    if snapshot_root.exists():
        for path in sorted(snapshot_root.glob('*.json'), key=lambda item: item.stat().st_mtime, reverse=True):
            payload = _read_json(str(path))
            profile_payload = ((payload.get('smart_entry') or {}).get('active_scoring_profile') or {})
            if not isinstance(profile_payload, dict) or not profile_payload.get('enabled'):
                continue
            profile = _profile_from_payload(profile_payload, source=str(path.resolve()))
            if profile:
                profile['active_profile_disabled'] = True
                profile['reference_profile_reason'] = 'pre_quote_aware_migration_snapshot'
                return profile
    raise RuntimeError(f'active scoring profile is disabled and no reference seed profile was found in {config_path}')


def load_active_gate(config_path: str = DEFAULT_CONFIG) -> dict | None:
    config_path = os.path.abspath(config_path)
    cfg = _read_json(config_path)
    gate = (((cfg.get('smart_entry') or {}).get('long_entry_quality_gate')) or {})
    if not gate.get('enabled'):
        return None
    out = {
        'mode': _safe_gate_mode(gate.get('mode') or gate.get('name')),
        'ticker_session_return_below_pct': float(gate.get('ticker_session_return_below_pct', -0.5)),
    }
    if gate.get('btc_bullish_regimes'):
        out['btc_bullish_regimes'] = list(gate.get('btc_bullish_regimes') or [])
    return out


def active_profile_payload(config_path: str = DEFAULT_CONFIG) -> dict:
    config_path = os.path.abspath(config_path)
    cfg = _read_json(config_path)
    smart = cfg.get('smart_entry') or {}
    router = smart.get('active_scoring_router') if isinstance(smart.get('active_scoring_router'), dict) else {}
    profile = router if router.get('enabled') else (smart.get('active_scoring_profile') or {})
    reference_mode = False
    try:
        variant = load_active_profile(config_path)
        profile_snapshot = step2_parity_contract.active_profile_snapshot(cfg, include_weights=True)
    except RuntimeError as exc:
        if 'active scoring profile is disabled' not in str(exc):
            raise
        variant = load_reference_profile(config_path)
        reference_mode = True
        profile_snapshot = {
            'hash': tournament_safety.stable_json_hash({
                'name': variant.get('name'),
                'weights': variant.get('weights') or {},
                'bias': variant.get('bias'),
                'routes': variant.get('routes') or [],
            }, 64),
            'weight_count': len(variant.get('weights') or {}),
            'reference_profile_source': variant.get('reference_profile_source'),
            'reference_profile_reason': variant.get('reference_profile_reason'),
        }
    gate = load_active_gate(config_path)
    if variant.get('routes'):
        model_id = tournament_safety.stable_json_hash({
            'name': variant['name'],
            'weights': variant['weights'],
            'bias': variant['bias'],
            'routes': variant.get('routes') or [],
        }, length=20)
        family_id = tournament_safety.stable_json_hash(variant.get('routes') or [], length=12)
    else:
        model_id = tournament_safety.model_id(variant['name'], variant['weights'], variant['bias'])
        family_id = tournament_safety.variant_family(variant['weights'])
    return {
        'config_path': config_path,
        'config_sha256': tournament_safety._file_sha256(config_path),
        'profile': {
            **variant,
            'hash': profile_snapshot.get('hash'),
            'weight_count': profile_snapshot.get('weight_count'),
            'promoted_at': profile.get('promoted_at'),
            'promotion_reason': profile.get('promotion_reason'),
            'routes': variant.get('routes') or [],
            'routed_scoring_profile': bool(variant.get('routes')),
            'model_id': model_id,
            'family_id': family_id,
            'active_profile_disabled': bool(reference_mode),
            'reference_profile_source': variant.get('reference_profile_source'),
            'reference_profile_reason': variant.get('reference_profile_reason'),
        },
        'long_entry_quality_gate': gate,
    }


def active_variant(config_path: str = DEFAULT_CONFIG) -> slow_lab.Variant:
    profile = load_active_profile(config_path)
    return variant_from_profile(profile)


def reference_variant(config_path: str = DEFAULT_CONFIG) -> slow_lab.Variant:
    profile = load_reference_profile(config_path)
    return variant_from_profile(profile)


def variant_from_profile(profile: dict[str, Any]) -> slow_lab.Variant:
    if profile.get('routes'):
        return routed_scoring_profile.routed_variant(
            profile['name'],
            dict(profile['weights']),
            float(profile.get('bias') or 0.0),
            [routed_scoring_profile.route_from_dict(route) for route in profile.get('routes') or []],
        )
    return slow_lab.Variant(profile['name'], dict(profile['weights']), float(profile.get('bias') or 0.0))


def lab_baseline(agg: dict, starting_balance: float = 100000.0,
                 config_path: str = DEFAULT_CONFIG) -> dict:
    rows = scoring_compiled_kernels.score_batch(agg, [active_variant(config_path)], starting_balance)
    row = tournament_safety.enrich_lab_results(rows)[0]
    row['baseline_name'] = 'active_engine'
    return row


def _date_value(args: Any, name: str, default: str | None = None) -> str | None:
    return getattr(args, name, None) or default


def decision_baseline(args: Any, rows_by_day: dict[str, list[dict]],
                      config_path: str = DEFAULT_CONFIG,
                      include_active_gate: bool = False) -> dict:
    profile = load_active_profile(config_path)
    gate = load_active_gate(config_path) if include_active_gate else None
    base_args = SimpleNamespace(**vars(args)) if hasattr(args, '__dict__') else SimpleNamespace()
    if not hasattr(base_args, 'start_balance'):
        base_args.start_balance = getattr(args, 'starting_balance', 100000.0)
    if not hasattr(base_args, 'tickers'):
        base_args.tickers = tournament.replay.TICKERS
    start = _date_value(base_args, 'start')
    end = _date_value(base_args, 'end')
    if not start or not end:
        raise RuntimeError('decision baseline requires args.start and args.end')
    full = tournament._baseline_range(base_args, rows_by_day, profile, gate, start, end)
    train = None
    test = None
    if getattr(base_args, 'train_start', None) and getattr(base_args, 'train_end', None):
        train = tournament._baseline_range(base_args, rows_by_day, profile, gate, base_args.train_start, base_args.train_end)
    if getattr(base_args, 'test_start', None) and getattr(base_args, 'test_end', None):
        test = tournament._baseline_range(base_args, rows_by_day, profile, gate, base_args.test_start, base_args.test_end)
    return {
        'baseline_name': 'active_engine',
        'profile': active_profile_payload(config_path),
        'include_active_gate': bool(include_active_gate),
        'decision_full': full,
        'decision_train': train,
        'decision_test': test,
    }


def annotate_lab_rows(rows: list[dict], active_lab: dict | None,
                      min_pnl_margin: float = 0.0,
                      require_win_rate: bool = False) -> list[dict]:
    if not active_lab:
        return rows
    active_pnl = float(active_lab.get('pnl') or 0.0)
    active_wr = active_lab.get('win_rate_pct')
    for row in rows:
        pnl = float(row.get('pnl') or 0.0)
        wr = row.get('win_rate_pct')
        row['active_lab_pnl'] = active_pnl
        row['pnl_delta_vs_active_lab'] = round(pnl - active_pnl, 2)
        row['beats_active_lab'] = pnl > active_pnl + float(min_pnl_margin or 0.0)
        row['beats_active_lab_strict'] = bool(
            row['beats_active_lab']
            and (not require_win_rate or (wr is not None and active_wr is not None and float(wr) >= float(active_wr)))
        )
    return rows


def annotate_decision_rows(rows: list[dict], active_decision: dict | None,
                           min_pnl_margin: float = 0.0,
                           require_win_rate: bool = False) -> list[dict]:
    summary = ((active_decision or {}).get('decision_full') or {})
    if not summary:
        return rows
    active_pnl = float(summary.get('pnl') or 0.0)
    active_wr = summary.get('win_rate_pct')
    for row in rows:
        candidate = row.get('decision_full') or {}
        pnl = float(candidate.get('pnl') or 0.0)
        wr = candidate.get('win_rate_pct')
        row['active_decision_pnl'] = active_pnl
        row['decision_pnl_delta_vs_active'] = round(pnl - active_pnl, 2)
        row['beats_active_decision'] = pnl > active_pnl + float(min_pnl_margin or 0.0)
        row['beats_active_decision_strict'] = bool(
            row['beats_active_decision']
            and (not require_win_rate or (wr is not None and active_wr is not None and float(wr) >= float(active_wr)))
        )
    return rows
