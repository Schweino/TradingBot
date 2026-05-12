"""Shared Step 2 parity contract for fast validation and live mock trading."""
from __future__ import annotations

import hashlib
import json

import execution_kernel
from datetime import datetime
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore


CT = ZoneInfo('America/Chicago')


DEFAULT_STEP2_PARITY = {
    'contract_version': 1,
    'admission_mode': 'all',
    'require_conviction': False,
    'min_exec_score': 0.0,
    'use_live_long_gate': False,
    'use_live_short_guard': False,
    'setup_state_enabled': False,
    'state_min_confirm_sec': 2.0,
    'state_max_confirm_sec': 30.0,
    'stop_loss_cooldown_sec': 0,
    'loss_cluster_window_sec': 0,
    'loss_cluster_count': 0,
    'loss_cluster_cooldown_sec': 0,
    'use_step2_brs_long_guard': False,
    'same_ticker_reentry_cooldown_sec': 5,
    'max_trades_per_day': 0,
    'max_trades_per_ticker_day': 0,
    'one_open_position_per_ticker': True,
    'sizing_mode': 'compounded_internal_balance',
    'conditional_time_stop_enabled': False,
    'conditional_stop_min': 7,
    'strict_tp_sl_exits': False,
    'bypass_strategy_gates': True,
}


LIVE_PARITY_EXECUTION_MODE = 'step2_signal_scan_live'
LIVE_PARITY_EXPECTATIONS = {
    'admission_mode': 'all',
    'require_conviction': False,
    'min_exec_score': 0.0,
    'use_live_long_gate': False,
    'use_live_short_guard': False,
    'setup_state_enabled': False,
    'stop_loss_cooldown_sec': 0,
    'loss_cluster_window_sec': 0,
    'loss_cluster_count': 0,
    'loss_cluster_cooldown_sec': 0,
    'use_step2_brs_long_guard': False,
    'max_trades_per_day': 0,
    'max_trades_per_ticker_day': 0,
    'one_open_position_per_ticker': True,
    'conditional_time_stop_enabled': False,
    'bypass_strategy_gates': True,
}


def _cfg(config: dict[str, Any]) -> dict[str, Any]:
    raw = config.get('step2_parity') or {}
    return raw if isinstance(raw, dict) else {}


def contract(config: dict[str, Any]) -> dict[str, Any]:
    """Return the canonical Step 2/live parity contract."""
    step2 = dict(DEFAULT_STEP2_PARITY)
    step2.update(_cfg(config))
    step2['same_ticker_reentry_cooldown_sec'] = int(step2.get('same_ticker_reentry_cooldown_sec') or 0)
    step2['max_trades_per_day'] = int(step2.get('max_trades_per_day') or 0)
    step2['max_trades_per_ticker_day'] = int(step2.get('max_trades_per_ticker_day') or 0)
    step2['require_conviction'] = bool(step2.get('require_conviction'))
    step2['use_live_long_gate'] = bool(step2.get('use_live_long_gate'))
    step2['use_live_short_guard'] = bool(step2.get('use_live_short_guard'))
    step2['setup_state_enabled'] = bool(step2.get('setup_state_enabled'))
    step2['use_step2_brs_long_guard'] = bool(step2.get('use_step2_brs_long_guard'))
    step2['one_open_position_per_ticker'] = bool(step2.get('one_open_position_per_ticker'))
    step2['conditional_time_stop_enabled'] = bool(step2.get('conditional_time_stop_enabled'))
    step2['conditional_stop_min'] = int(step2.get('conditional_stop_min') or 0)
    step2['strict_tp_sl_exits'] = bool(step2.get('strict_tp_sl_exits'))
    step2['bypass_strategy_gates'] = bool(step2.get('bypass_strategy_gates'))
    kernel_config = dict(config or {})
    kernel_config['step2'] = step2
    kernel_config['step2_parity'] = step2
    execution_contract = execution_kernel.contract_from_config(kernel_config)
    step2['execution_kernel_hash'] = execution_contract.get('execution_kernel_hash')
    step2['execution_kernel_contract'] = execution_contract
    return step2


def contract_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()


def active_profile_snapshot(config: dict[str, Any], include_weights: bool = True) -> dict[str, Any]:
    """Return the canonical active profile identity used by Live and Step 2."""
    smart = config.get('smart_entry') or {}
    base_profile = (smart.get('active_scoring_profile') or {})
    router = (smart.get('active_scoring_router') or {})
    if isinstance(router, dict) and router.get('enabled'):
        profile = dict(router)
        profile['weights'] = dict(profile.get('weights') or base_profile.get('weights') or {})
        profile['bias'] = profile.get('bias', base_profile.get('bias', 0.0))
        profile['routes'] = list(profile.get('routes') or [])
        profile_type = 'routed'
    else:
        profile = base_profile
        profile_type = 'linear'
    weights = dict(profile.get('weights') or {})
    semantic = {
        'enabled': bool(profile.get('enabled')),
        'name': profile.get('name'),
        'bias': profile.get('bias'),
        'weights': weights,
    }
    if profile_type == 'routed':
        semantic['routes'] = list(profile.get('routes') or [])
        semantic['routed_scoring_profile'] = True
    out = dict(semantic)
    out['hash'] = contract_hash(semantic)
    out['weight_count'] = len(weights)
    out['profile_type'] = profile_type
    out['route_count'] = len(profile.get('routes') or []) if profile_type == 'routed' else 0
    out['promoted_at'] = profile.get('promoted_at')
    out['promotion_reason'] = profile.get('promotion_reason')
    if not include_weights:
        out.pop('weights', None)
        out.pop('routes', None)
    return out


def live_parity_checks(config: dict[str, Any]) -> dict[str, Any]:
    """Validate that a live config is wired for Step 2-equivalent execution."""
    step2 = contract(config)
    execution_contract = step2.get('execution_kernel_contract') or {}
    profile = active_profile_snapshot(config, include_weights=False)
    checks: list[dict[str, Any]] = []

    def add(name: str, ok: bool, actual: Any = None, expected: Any = None) -> None:
        row = {'name': name, 'ok': bool(ok)}
        if actual is not None:
            row['actual'] = actual
        if expected is not None:
            row['expected'] = expected
        checks.append(row)

    execution_mode = str(config.get('execution_mode') or '')
    add(
        'execution_mode_is_step2_signal_scan_live',
        execution_mode == LIVE_PARITY_EXECUTION_MODE,
        execution_mode,
        LIVE_PARITY_EXECUTION_MODE,
    )
    for key, expected in LIVE_PARITY_EXPECTATIONS.items():
        add(f'step2_parity.{key}', step2.get(key) == expected, step2.get(key), expected)
    cooldown = int(step2.get('same_ticker_reentry_cooldown_sec') or 0)
    add('step2_parity.same_ticker_reentry_cooldown_non_negative', cooldown >= 0, cooldown, '>= 0')
    add('execution_kernel_hash_present', bool(step2.get('execution_kernel_hash')), step2.get('execution_kernel_hash'))
    add(
        'execution_contract_hash_consistent',
        bool(
            execution_contract
            and step2.get('execution_kernel_hash') == execution_contract.get('execution_kernel_hash')
        ),
        step2.get('execution_kernel_hash'),
        execution_contract.get('execution_kernel_hash'),
    )
    add('active_scoring_profile.enabled', bool(profile.get('enabled')), profile.get('enabled'), True)
    add('active_scoring_profile.weights_present', int(profile.get('weight_count') or 0) > 0,
        profile.get('weight_count'), '> 0')
    if profile.get('profile_type') == 'routed':
        add('active_scoring_router.routes_present', int(profile.get('route_count') or 0) > 0,
            profile.get('route_count'), '> 0')
    ok = all(row.get('ok') for row in checks)
    return {
        'schema_version': 1,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'ok': ok,
        'status': 'ok' if ok else 'warn',
        'checks': checks,
        'execution_mode': execution_mode,
        'active_scoring_profile': profile,
        'step2_parity_contract_hash': contract_hash(step2),
        'step2_parity_contract': step2,
        'execution_kernel_hash': step2.get('execution_kernel_hash'),
    }


def sim_config(config: dict[str, Any]) -> dict[str, float | int]:
    """Compiled Step 2 kernel config derived from the same parity contract."""
    step2 = contract(config)
    admission = {'all': 0.0, 'accepted-only': 1.0, 'skip-cached-rejected': 2.0}
    return {
        'admission_mode': admission.get(str(step2.get('admission_mode') or 'all'), 0.0),
        'min_exec_score': float(step2.get('min_exec_score') or 0.0),
        'require_conviction': 1.0 if step2.get('require_conviction') else 0.0,
        'use_live_long_gate': 1.0 if step2.get('use_live_long_gate') else 0.0,
        'use_live_short_guard': 1.0 if step2.get('use_live_short_guard') else 0.0,
        'setup_state_enabled': 1.0 if step2.get('setup_state_enabled') else 0.0,
        'state_min_confirm_sec': float(step2.get('state_min_confirm_sec') or 2.0),
        'state_max_confirm_sec': float(step2.get('state_max_confirm_sec') or 30.0),
        'stop_loss_cooldown_sec': int(step2.get('stop_loss_cooldown_sec') or 0),
        'loss_cluster_window_sec': int(step2.get('loss_cluster_window_sec') or 0),
        'loss_cluster_count': int(step2.get('loss_cluster_count') or 0),
        'loss_cluster_cooldown_sec': int(step2.get('loss_cluster_cooldown_sec') or 0),
        'use_step2_brs_long_guard': 1.0 if step2.get('use_step2_brs_long_guard') else 0.0,
        'same_ticker_reentry_cooldown_sec': int(step2.get('same_ticker_reentry_cooldown_sec') or 0),
        'max_trades_per_day': int(step2.get('max_trades_per_day') or 0),
        'max_trades_per_ticker_day': int(step2.get('max_trades_per_ticker_day') or 0),
        'conditional_time_stop_enabled': 1.0 if step2.get('conditional_time_stop_enabled') else 0.0,
        'conditional_stop_min': int(step2.get('conditional_stop_min') or 0),
    }
