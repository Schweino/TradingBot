"""Pre-market guardrail for Live/Step 2 parity."""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from typing import Any
from urllib.request import urlopen

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import golden_parity_suite
import live_profile_recovery
import live_step2_execution_harness
import live_state_rollover
import rollback_drill
import semantic_config
import step2_parity_contract


HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, 'postmortem', 'pre_market_parity_gate')
CONFIG_PATH = os.path.join(HERE, 'trading_config.json')
REGISTRY_DIR = os.path.join(HERE, 'postmortem', 'promotions', 'active_scoring_profiles')
STATUS_URL = 'http://127.0.0.1:5000/mock/status'
CT = ZoneInfo('America/Chicago')
SCHEMA_VERSION = 1


def _read_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, 'r', encoding='utf-8-sig') as f:
            return json.load(f)
    except Exception:
        return default


def _write_json(path: str, payload: dict[str, Any]) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f'{path}.{os.getpid()}.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
    os.replace(tmp, path)
    return os.path.abspath(path)


def _latest_registry() -> dict[str, Any]:
    if not os.path.isdir(REGISTRY_DIR):
        return {'exists': False, 'path': None, 'entry': None}
    paths = [
        os.path.join(REGISTRY_DIR, name)
        for name in os.listdir(REGISTRY_DIR)
        if name.endswith('.json')
    ]
    if not paths:
        return {'exists': False, 'path': None, 'entry': None}
    path = max(paths, key=os.path.getmtime)
    return {'exists': True, 'path': os.path.abspath(path), 'entry': _read_json(path, {}) or {}}


def _status(timeout: float = 2.0) -> dict[str, Any]:
    try:
        with urlopen(STATUS_URL, timeout=timeout) as response:
            return {'ok': True, 'status': json.loads(response.read().decode('utf-8'))}
    except Exception as exc:
        return {'ok': False, 'error': str(exc)}


def build(day: str | None = None, allow_app_down: bool = True) -> dict[str, Any]:
    day = day or datetime.now(CT).date().isoformat()
    cfg = _read_json(CONFIG_PATH, {}) or {}
    live_checks = step2_parity_contract.live_parity_checks(cfg)
    semantic = semantic_config.report(cfg)
    registry = _latest_registry()
    active_hash = (semantic.get('section_hashes') or {}).get('active_scoring_profile')
    registry_profile = ((registry.get('entry') or {}).get('new_profile') or {})
    registry_active_hash = None
    if registry_profile:
        registry_active_hash = semantic_config.section_hashes({
            'smart_entry': {'active_scoring_profile': registry_profile},
        }).get('active_scoring_profile')
    app = _status()
    golden = golden_parity_suite.run()
    execution_harness = live_step2_execution_harness.run(day=day, write=False, config=cfg)
    last_known_good = live_profile_recovery.latest_last_known_good()
    rollback = rollback_drill.build(
        execute=False,
        restart=False,
        reason=f'Pre-market rollback drill for {day}: prove last-known-good restore target before trading.',
        actor='pre_market_parity_gate.py',
        write=False,
    )
    rollover = live_state_rollover.check_state_file(target_day=day)
    checks = []

    def add(name: str, ok: bool, actual: Any = None, expected: Any = None, severity: str = 'error') -> None:
        row = {'name': name, 'ok': bool(ok), 'severity': severity}
        if actual is not None:
            row['actual'] = actual
        if expected is not None:
            row['expected'] = expected
        checks.append(row)

    add('live_parity_contract_ok', bool(live_checks.get('ok')), live_checks.get('status'), 'ok')
    add('active_profile_hash_present', bool(active_hash), active_hash)
    add('promotion_registry_exists', bool(registry.get('exists')), registry.get('path'))
    if registry_active_hash:
        add('active_profile_matches_latest_registry', active_hash == registry_active_hash, active_hash, registry_active_hash)
    else:
        add('active_profile_matches_latest_registry', False, active_hash, None)
    add('golden_parity_suite_ok', bool(golden.get('ok')), golden.get('days'), 'all_default_goldens')
    add(
        'execution_contract_harness_ok',
        bool(execution_harness.get('ok')),
        execution_harness.get('failed_scenarios'),
        [],
    )
    add('last_known_good_profile_present', bool(last_known_good.get('ok')), last_known_good.get('updated_at_ct'), 'last-known-good pointer')
    add('last_known_good_profile_has_snapshot',
        bool(last_known_good.get('rollback_snapshot_path') or last_known_good.get('config_snapshot_path')),
        last_known_good.get('rollback_snapshot_path') or last_known_good.get('config_snapshot_path'),
        'rollback snapshot')
    add('rollback_drill_ok', bool(rollback.get('ok')), rollback.get('critical_failure_count'), 0)
    add('state_rollover_file_check_ok', bool(rollover.get('ok')), rollover.get('critical_failure_count'), 0)
    if app.get('ok'):
        status = app.get('status') or {}
        positions = status.get('positions') or {}
        pending = status.get('pending_entries') or {}
        setup_pauses = status.get('setup_pauses') or {}
        state_rollover = status.get('state_rollover') or {}
        target_is_today = day == datetime.now(CT).date().isoformat()
        add('mock_status_reachable', True)
        add('no_open_positions_before_gate', not bool(positions), sorted(positions.keys()) if isinstance(positions, dict) else positions, [])
        add('no_pending_entries_before_gate', not bool(pending), sorted(pending.keys()) if isinstance(pending, dict) else pending, [])
        if target_is_today:
            add('no_setup_pauses_before_gate', not bool(setup_pauses), sorted(setup_pauses.keys()) if isinstance(setup_pauses, dict) else setup_pauses, [])
            add('mock_state_market_day_matches_gate_day', status.get('state_market_day') == day, status.get('state_market_day'), day)
            add('mock_state_rollover_ok', bool(state_rollover.get('ok')), state_rollover.get('critical_failure_count'), 0)
            add('mock_state_rollover_target_day', state_rollover.get('target_day') == day, state_rollover.get('target_day'), day)
            add('daily_budget_date_current_or_unset_before_trade',
                status.get('daily_budget_date') in (None, day),
                status.get('daily_budget_date'),
                f'None|{day}')
        else:
            add('mock_state_rollover_future_day_validation_deferred',
                True,
                {
                    'state_market_day': status.get('state_market_day'),
                    'setup_pause_count': len(setup_pauses) if isinstance(setup_pauses, dict) else None,
                },
                f'checked on actual pre-open day {day}',
                severity='warn')
        startup = status.get('startup_self_check') or {}
        if startup:
            add('mock_startup_self_check_ok', bool(startup.get('ok')), startup.get('ok'), True)
    else:
        add('mock_status_reachable', bool(allow_app_down), app.get('error'), 'reachable_or_allowed_down',
            severity='warn' if allow_app_down else 'error')
    ok = all(row.get('ok') or row.get('severity') == 'warn' for row in checks)
    return {
        'schema_version': SCHEMA_VERSION,
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'ok': bool(ok),
        'checks': checks,
        'live_parity_checks': live_checks,
        'semantic_config': semantic,
        'latest_promotion_registry': registry,
        'mock_status': app,
        'golden_parity': golden,
        'execution_contract_harness': execution_harness,
        'last_known_good_profile': last_known_good,
        'rollback_drill': rollback,
        'state_rollover': rollover,
    }


def write(day: str | None = None, allow_app_down: bool = True) -> str:
    payload = build(day=day, allow_app_down=allow_app_down)
    path = os.path.join(OUT_DIR, f'pre_market_parity_gate_{payload["day"]}.json')
    return _write_json(path, payload)


def main() -> int:
    ap = argparse.ArgumentParser(description='Run pre-market Live/Step 2 parity gate.')
    ap.add_argument('day', nargs='?', default='')
    ap.add_argument('--allow-app-down', action='store_true')
    ap.add_argument('--json', action='store_true')
    args = ap.parse_args()
    payload = build(day=args.day or None, allow_app_down=args.allow_app_down)
    path = os.path.join(OUT_DIR, f'pre_market_parity_gate_{payload["day"]}.json')
    payload['path'] = _write_json(path, payload)
    print(json.dumps(payload if args.json else {'ok': payload.get('ok'), 'path': payload['path']},
                     indent=2, sort_keys=True, default=str))
    return 0 if payload.get('ok') else 2


if __name__ == '__main__':
    raise SystemExit(main())
