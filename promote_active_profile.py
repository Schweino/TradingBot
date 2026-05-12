"""Safely promote a Step 2 scoring profile into the live config.

This command changes ``smart_entry.active_scoring_profile`` for linear
candidates or ``smart_entry.active_scoring_router`` for routed candidates. It
refuses candidate artifacts that were scored under a different Step 2 parity
contract, and it records an immutable promotion entry before replacing config.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import step2_parity_contract
import baseline_drift_sentinel
import candidate_reproducibility_gate
import candidate_profile_schema
import config_change_journal
import promotion_candidate_quarantine
import promotion_manifest
import promotion_preflight_bundle
import candidate_lifecycle
import promotion_evidence_packet
import promotion_gate
import live_profile_recovery
import rollback_drill
import tournament_safety


HERE = Path(__file__).resolve().parent
CONFIG_PATH = HERE / 'trading_config.json'
REGISTRY_DIR = HERE / 'postmortem' / 'promotions' / 'active_scoring_profiles'
ROLLBACK_DIR = HERE / 'postmortem' / 'promotions' / 'rollback_snapshots'
PROMOTION_LOG_PATH = HERE / 'postmortem' / 'promotions' / 'promotion_log.jsonl'
CT = ZoneInfo('America/Chicago')


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec='seconds')


def _stamp() -> str:
    return datetime.now(CT).strftime('%Y%m%d_%H%M%S')


def _load_json(path: Path) -> Any:
    with path.open('r', encoding='utf-8-sig') as f:
        return json.load(f)


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + f'.{os.getpid()}.tmp')
    with tmp.open('w', encoding='utf-8') as f:
        json.dump(payload, f, indent=4, default=str)
        f.write('\n')
    os.replace(tmp, path)


def _write_json_exclusive(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
        f.write('\n')
    return str(path.resolve())


def _append_jsonl(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a', encoding='utf-8') as f:
        f.write(json.dumps(payload, sort_keys=True, default=str))
        f.write('\n')
    return str(path.resolve())


def _stable_json_hash(payload: Any) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(',', ':'), default=str).encode('utf-8')
    return hashlib.sha256(raw).hexdigest()


def _slug(text: str) -> str:
    cleaned = re.sub(r'[^A-Za-z0-9_.+-]+', '_', text or 'profile').strip('_')
    return (cleaned or 'profile')[:96]


def _profile_hash(profile: dict[str, Any]) -> str:
    semantic = {
        'enabled': bool(profile.get('enabled')),
        'name': profile.get('name'),
        'bias': profile.get('bias'),
        'weights': dict(profile.get('weights') or {}),
    }
    if profile.get('routes'):
        semantic['routes'] = list(profile.get('routes') or [])
        semantic['routed_scoring_profile'] = True
    return step2_parity_contract.contract_hash(semantic)


def _score(row: dict[str, Any]) -> float:
    schema_score = candidate_profile_schema.score(row)
    if schema_score != float('-inf'):
        return schema_score
    for key in ('step2_pnl', 'pnl'):
        if row.get(key) is not None:
            return float(row.get(key) or 0.0)
    decision = row.get('decision_full') if isinstance(row.get('decision_full'), dict) else {}
    if decision.get('pnl') is not None:
        return float(decision.get('pnl') or 0.0)
    result = row.get('result') if isinstance(row.get('result'), dict) else {}
    if result.get('pnl') is not None:
        return float(result.get('pnl') or 0.0)
    score = row.get('score') if isinstance(row.get('score'), dict) else {}
    result = score.get('result') if isinstance(score.get('result'), dict) else {}
    if result.get('pnl') is not None:
        return float(result.get('pnl') or 0.0)
    return float('-inf')


def _candidate_rows(payload: Any) -> list[dict[str, Any]]:
    rows = candidate_profile_schema.rows_from_payload(payload)
    if not rows and isinstance(payload, dict) and isinstance(payload.get('active'), dict) and isinstance(payload['active'].get('weights'), dict):
        rows.append(payload['active'])
    return rows


def _select_candidate(payload: dict[str, Any], variant: str = '', rank: int = 1) -> dict[str, Any]:
    rows = _candidate_rows(payload)
    if not rows:
        raise RuntimeError('candidate artifact did not contain any rows with weights')
    if variant:
        for row in rows:
            if str(row.get('variant') or row.get('name') or '') == variant:
                return row
        raise RuntimeError(f'variant not found in artifact: {variant}')
    ordered = sorted(rows, key=_score, reverse=True)
    idx = max(0, int(rank) - 1)
    if idx >= len(ordered):
        raise RuntimeError(f'rank {rank} is outside artifact row count {len(ordered)}')
    return ordered[idx]


def _profile_from_row(row: dict[str, Any], reason: str) -> dict[str, Any]:
    weights = row.get('weights')
    if not isinstance(weights, dict) or not weights:
        raise RuntimeError('selected candidate has no weights')
    clean_weights = {
        str(key): round(float(value), 6)
        for key, value in weights.items()
        if abs(float(value or 0.0)) > 1e-12
    }
    if not clean_weights:
        raise RuntimeError('selected candidate weights are all zero')
    name = str(row.get('variant') or row.get('name') or 'promoted_step2_profile')
    profile = {
        'enabled': True,
        'name': name,
        'promoted_at': datetime.now(CT).date().isoformat(),
        'promotion_reason': reason,
        'bias': round(float(row.get('bias') or 0.0), 6),
        'weights': dict(sorted(clean_weights.items())),
    }
    routes = row.get('routes') if isinstance(row.get('routes'), list) else []
    if routes:
        profile['routes'] = routes
        profile['routed_scoring_profile'] = True
        if isinstance(row.get('route_audit'), dict):
            profile['route_audit'] = row.get('route_audit')
        if isinstance(row.get('routed_profile_safety'), dict):
            profile['route_safety'] = row.get('routed_profile_safety')
    profile['profile_hash'] = _profile_hash(profile)
    return profile


def _current_profile(config: dict[str, Any]) -> dict[str, Any]:
    smart = config.get('smart_entry') or {}
    base_profile = ((smart.get('active_scoring_profile')) or {})
    router = ((smart.get('active_scoring_router')) or {})
    profile = router if isinstance(router, dict) and router.get('enabled') else base_profile
    if not profile.get('enabled') or not isinstance(profile.get('weights'), dict):
        raise RuntimeError('current active_scoring_profile is disabled or missing weights')
    out = {
        'enabled': True,
        'name': profile.get('name') or 'active_scoring_profile',
        'promoted_at': profile.get('promoted_at'),
        'promotion_reason': profile.get('promotion_reason'),
        'bias': float(profile.get('bias') or 0.0),
        'weights': dict(profile.get('weights') or {}),
    }
    if profile.get('routes'):
        out['routes'] = list(profile.get('routes') or [])
        out['routed_scoring_profile'] = True
    return out


def _current_profile_full(config: dict[str, Any]) -> dict[str, Any]:
    smart = config.get('smart_entry') or {}
    base_profile = ((smart.get('active_scoring_profile')) or {})
    router = ((smart.get('active_scoring_router')) or {})
    profile = router if isinstance(router, dict) and router.get('enabled') else base_profile
    if not profile.get('enabled') or not isinstance(profile.get('weights'), dict):
        raise RuntimeError('current active_scoring_profile is disabled or missing weights')
    full = dict(profile)
    full['enabled'] = True
    full['bias'] = float(full.get('bias') or 0.0)
    full['weights'] = dict(full.get('weights') or {})
    full.setdefault('name', profile.get('name') or 'active_scoring_profile')
    return full


def _artifact_contract_hash(payload: dict[str, Any], row: dict[str, Any]) -> str | None:
    contracts = row.get('contracts') if isinstance(row.get('contracts'), dict) else {}
    payload_contracts = payload.get('candidate_contracts') if isinstance(payload.get('candidate_contracts'), dict) else {}
    candidates = [
        row.get('step2_parity_contract_hash'),
        contracts.get('step2_parity_contract_hash'),
        payload.get('step2_parity_contract_hash'),
        payload_contracts.get('step2_parity_contract_hash'),
        (row.get('run_context') or {}).get('step2_parity_contract_hash') if isinstance(row.get('run_context'), dict) else None,
    ]
    for value in candidates:
        if value:
            return str(value)
    return None


def _candidate_execution_values(row: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    values: dict[str, Any] = {}
    row_contracts = row.get('contracts') if isinstance(row.get('contracts'), dict) else {}
    payload_contracts = payload.get('candidate_contracts') if isinstance(payload.get('candidate_contracts'), dict) else {}
    for source in (
        payload.get('step2_parity_contract'),
        payload_contracts.get('step2_parity_contract'),
        row.get('step2_parity_contract'),
        row_contracts.get('step2_parity_contract'),
        row.get('sim_config'),
    ):
        if isinstance(source, dict):
            for key in (
                'same_ticker_reentry_cooldown_sec',
                'max_trades_per_day',
                'max_trades_per_ticker_day',
            ):
                if source.get(key) is not None:
                    values[key] = source.get(key)
    for key in ('same_ticker_reentry_cooldown_sec', 'max_trades_per_day', 'max_trades_per_ticker_day'):
        if row.get(key) is not None:
            values[key] = row.get(key)
    return values


def _validate(config: dict[str, Any], payload: dict[str, Any], row: dict[str, Any],
              allow_execution_change: bool,
              allow_unapproved_candidate: bool = False,
              selected_rank: int = 1,
              selected_variant: str = '') -> dict[str, Any]:
    checks = []

    def add(name: str, ok: bool, actual: Any = None, expected: Any = None) -> None:
        item = {'name': name, 'ok': bool(ok)}
        if actual is not None:
            item['actual'] = actual
        if expected is not None:
            item['expected'] = expected
        checks.append(item)

    live_checks = step2_parity_contract.live_parity_checks(config)
    add('live_parity_self_check', bool(live_checks.get('ok')), live_checks.get('status'), 'ok')
    contract = live_checks.get('step2_parity_contract') or step2_parity_contract.contract(config)
    contract_hash = live_checks.get('step2_parity_contract_hash') or step2_parity_contract.contract_hash(contract)
    artifact_hash = _artifact_contract_hash(payload, row)
    if artifact_hash:
        add('artifact_step2_parity_contract_hash_matches_live', artifact_hash == contract_hash, artifact_hash, contract_hash)
    else:
        add('artifact_step2_parity_contract_hash_present', False, None, contract_hash)
    candidate_exec = _candidate_execution_values(row, payload)
    for key in ('same_ticker_reentry_cooldown_sec', 'max_trades_per_day', 'max_trades_per_ticker_day'):
        if key in candidate_exec:
            actual = int(candidate_exec.get(key) or 0)
            expected = int(contract.get(key) or 0)
            add(f'candidate_execution.{key}_matches_live', actual == expected or allow_execution_change, actual, expected)
    if baseline_drift_sentinel.source_looks_step2(payload):
        envelope = payload.get('step2_evaluation_envelope') if isinstance(payload.get('step2_evaluation_envelope'), dict) else {}
        add('step2_evaluation_envelope_present', bool(envelope), bool(envelope), True)
        add('step2_artifact_promotable', payload.get('promotable') is not False, payload.get('promotable'), True)
        add(
            'step2_evaluation_envelope_promotable',
            bool(envelope.get('promotable')) if envelope else False,
            envelope.get('promotable') if envelope else None,
            True,
        )
        drift = baseline_drift_sentinel.evaluate_payload(payload)
        add('step2_baseline_hash_matches_live', drift.get('status') == 'current', drift.get('status'), 'current')
        try:
            reproducibility = candidate_reproducibility_gate.evaluate_payload(
                payload,
                row,
                candidate_json=str(payload.get('candidate_path') or ''),
            )
        except Exception as exc:
            reproducibility = {'ok': False, 'error': repr(exc)}
        add(
            'step2_candidate_reproduces_from_envelope',
            bool(reproducibility.get('ok')),
            'ok' if reproducibility.get('ok') else reproducibility.get('error') or reproducibility.get('failed_checks'),
            'ok',
        )
        try:
            quarantine = promotion_candidate_quarantine.evaluate_artifact(
                str(payload.get('candidate_path') or ''),
                rank=selected_rank,
                variant=str(selected_variant or row.get('variant') or row.get('name') or ''),
            )
        except Exception as exc:
            quarantine = {'ok': False, 'error': repr(exc), 'failed_checks': []}
        add(
            'promotion_quarantine_approved_for_live',
            bool(quarantine.get('ok')) or allow_unapproved_candidate,
            'override' if allow_unapproved_candidate and not quarantine.get('ok') else (
                'ok' if quarantine.get('ok') else quarantine.get('error') or quarantine.get('failed_checks')
            ),
            'ok',
        )
    else:
        drift = {'status': 'skipped', 'reason': 'not_step2_payload'}
        reproducibility = {'ok': True, 'skipped': True, 'reason': 'not_step2_payload'}
        quarantine = {'ok': True, 'skipped': True, 'reason': 'not_step2_payload'}
    ok = all(item.get('ok') for item in checks)
    return {
        'ok': ok,
        'checks': checks,
        'live_parity': live_checks,
        'candidate_execution_values': candidate_exec,
        'baseline_drift': drift,
        'candidate_reproducibility': reproducibility,
        'promotion_quarantine': quarantine,
    }


def _write_registry(entry: dict[str, Any]) -> str:
    raw_path = entry.get('registry_path')
    path = Path(raw_path) if raw_path else _registry_path_for(entry)
    return _write_json_exclusive(path, entry)


def _registry_path_for(entry: dict[str, Any]) -> Path:
    REGISTRY_DIR.mkdir(parents=True, exist_ok=True)
    return REGISTRY_DIR / f"{_stamp()}_{_slug((entry.get('new_profile') or {}).get('name') or 'profile')}.json"


def _write_rollback_snapshot(config: dict[str, Any],
                             entry: dict[str, Any],
                             registry_path: str,
                             evidence_packet: dict[str, Any] | None) -> dict[str, Any]:
    prior = entry.get('prior_profile') or {}
    new_profile = entry.get('new_profile') or {}
    payload = {
        'schema_version': 1,
        'created_at_ct': _now_ct(),
        'action': 'rollback_active_scoring_profile',
        'config_path': str(CONFIG_PATH.resolve()),
        'promotion_registry_path': registry_path,
        'promotion_evidence_packet_path': ((evidence_packet or {}).get('output') or {}).get('json_path'),
        'prior_profile': prior,
        'new_profile': new_profile,
        'prior_profile_hash': _profile_hash(prior) if prior else None,
        'new_profile_hash': _profile_hash(new_profile) if new_profile else None,
        'config_hash_before': _stable_json_hash(config),
        'rollback_command': (
            'python promote_active_profile.py --record-current '
            '--reason "Manual rollback: restore prior_profile from this rollback snapshot"'
        ),
        'deduction': (
            'This snapshot is written before the live config changes so the exact prior '
            'active_scoring_profile can be restored if the promotion misbehaves.'
        ),
    }
    path = ROLLBACK_DIR / f"{_stamp()}_{_slug(str((new_profile or {}).get('name') or 'profile'))}_rollback.json"
    payload['path'] = _write_json_exclusive(path, payload)
    return payload


def _decorate_profile_metadata(profile: dict[str, Any],
                               entry: dict[str, Any],
                               evidence_packet: dict[str, Any] | None,
                               registry_path: str,
                               rollback_snapshot: dict[str, Any],
                               gate: dict[str, Any] | None) -> dict[str, Any]:
    evidence_output = (evidence_packet or {}).get('output') or {}
    decorated = dict(profile)
    decorated['profile_hash'] = _profile_hash(decorated)
    decorated['model_id'] = entry.get('model_id')
    decorated['family_id'] = entry.get('family_id')
    decorated['promotion_registry_path'] = registry_path
    decorated['promotion_evidence_packet_path'] = evidence_output.get('json_path')
    decorated['promotion_evidence_packet_hash'] = (evidence_packet or {}).get('packet_hash')
    decorated['promotion_gate_ok'] = bool((gate or {}).get('ok')) if gate is not None else True
    decorated['promotion_gate_created_at'] = (gate or {}).get('created_at_ct') if gate else None
    decorated['rollback_snapshot_path'] = rollback_snapshot.get('path')
    decorated['rollback_prior_profile_hash'] = rollback_snapshot.get('prior_profile_hash')
    return decorated


def _restart_live(timeout: float) -> dict[str, Any]:
    proc = subprocess.run(
        [sys.executable, 'live_engine_ops.py', '--timeout', str(timeout), 'restart'],
        cwd=str(HERE),
        capture_output=True,
        text=True,
        timeout=max(timeout + 10.0, 20.0),
    )
    payload = {
        'returncode': proc.returncode,
        'stdout': proc.stdout[-4000:],
        'stderr': proc.stderr[-4000:],
    }
    if proc.returncode != 0:
        raise RuntimeError(f'live restart failed: {payload}')
    return payload


def build_entry(args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    config = _load_json(CONFIG_PATH)
    if not isinstance(config, dict):
        raise RuntimeError(f'invalid config JSON: {CONFIG_PATH}')
    if args.record_current:
        payload: dict[str, Any] = {
            'source': 'current_live_config',
            'step2_parity_contract_hash': step2_parity_contract.contract_hash(
                step2_parity_contract.contract(config)
            ),
        }
        row = _current_profile(config)
    else:
        if not args.candidate_json:
            raise RuntimeError('--candidate-json is required unless --record-current is used')
        candidate_path = Path(args.candidate_json).resolve()
        payload = _load_json(candidate_path)
        if not isinstance(payload, dict):
            raise RuntimeError(f'candidate artifact must be a JSON object: {candidate_path}')
        row = _select_candidate(payload, variant=args.variant, rank=args.rank)
        payload['candidate_path'] = str(candidate_path)
    if args.reason:
        reason = args.reason
    elif args.record_current:
        reason = str(row.get('promotion_reason') or 'Registered current live active scoring profile.')
    else:
        reason = (
            f"Promoted via promote_active_profile.py from "
            f"{payload.get('candidate_path') or payload.get('source') or 'artifact'}; "
            f"Step 2 P/L={_score(row):.2f}."
        )
    new_profile = _profile_from_row(row, reason)
    prior_profile = _current_profile_full(config)
    standard_candidate = candidate_profile_schema.normalize(
        row,
        context={'artifact': payload.get('candidate_path') or payload.get('source'), 'rank': args.rank},
        source_payload=payload,
    )
    validation = _validate(
        config,
        payload,
        row,
        args.allow_execution_change,
        getattr(args, 'allow_unapproved_candidate', False),
        selected_rank=args.rank,
        selected_variant=args.variant,
    )
    if new_profile.get('routes'):
        model_id = tournament_safety.stable_json_hash({
            'name': new_profile.get('name'),
            'weights': new_profile.get('weights') or {},
            'bias': float(new_profile.get('bias') or 0.0),
            'routes': new_profile.get('routes') or [],
        }, length=20)
        family_id = tournament_safety.stable_json_hash(new_profile.get('routes') or [], length=12)
    else:
        model_id = tournament_safety.model_id(
            str(new_profile.get('name')),
            new_profile.get('weights') or {},
            float(new_profile.get('bias') or 0.0),
        )
        family_id = tournament_safety.variant_family(new_profile.get('weights') or {})
    entry = {
        'schema_version': 1,
        'created_at_ct': _now_ct(),
        'action': 'record_current' if args.record_current else 'promote_active_scoring_profile',
        'candidate_source': payload.get('candidate_path') or payload.get('source'),
        'selected_rank': args.rank,
        'selected_variant': row.get('variant') or row.get('name'),
        'selected_step2_pnl': None if _score(row) == float('-inf') else _score(row),
        'standard_candidate': standard_candidate,
        'model_id': model_id,
        'family_id': family_id,
        'prior_profile': prior_profile,
        'new_profile': new_profile,
        'validation': validation,
        'config_path': str(CONFIG_PATH.resolve()),
    }
    return config, entry, validation


def apply_promotion(config: dict[str, Any], entry: dict[str, Any]) -> None:
    smart = config.setdefault('smart_entry', {})
    new_profile = entry['new_profile']
    if new_profile.get('routes'):
        smart['active_scoring_router'] = new_profile
    else:
        smart['active_scoring_profile'] = new_profile
        router = smart.get('active_scoring_router')
        if isinstance(router, dict):
            router['enabled'] = False
    _write_json_atomic(CONFIG_PATH, config)


def _restore_from_rollback_snapshot(rollback_snapshot: dict[str, Any],
                                    entry: dict[str, Any],
                                    post_canary: dict[str, Any]) -> dict[str, Any]:
    prior = dict(rollback_snapshot.get('prior_profile') or {})
    if not prior.get('weights'):
        raise RuntimeError('rollback snapshot has no prior_profile weights')
    prior['enabled'] = True
    prior['profile_hash'] = _profile_hash(prior)
    prior['rollback_restored_at'] = _now_ct()
    prior['rollback_reason'] = 'post_promotion_canary_failed'
    prior['rollback_failed_profile_hash'] = (entry.get('new_profile') or {}).get('profile_hash')
    prior['rollback_failed_canary_path'] = post_canary.get('output_path')
    cfg = _load_json(CONFIG_PATH)
    smart = cfg.setdefault('smart_entry', {})
    if prior.get('routes'):
        smart['active_scoring_router'] = prior
    else:
        smart['active_scoring_profile'] = prior
        router = smart.get('active_scoring_router')
        if isinstance(router, dict):
            router['enabled'] = False
    _write_json_atomic(CONFIG_PATH, cfg)
    return {
        'ok': True,
        'restored_profile_name': prior.get('name'),
        'restored_profile_hash': prior.get('profile_hash'),
        'rollback_reason': prior.get('rollback_reason'),
        'failed_profile_hash': prior.get('rollback_failed_profile_hash'),
    }


def _write_promotion_manifest(entry: dict[str, Any],
                              config: dict[str, Any],
                              status: str,
                              attach_to_live_profile: bool = False) -> dict[str, Any]:
    manifest_path = promotion_manifest.path_for(entry, status=status).resolve()
    if attach_to_live_profile:
        profile = dict(entry.get('new_profile') or {})
        profile['promotion_manifest_path'] = str(manifest_path)
        profile['promotion_manifest_status'] = status
        entry['new_profile'] = profile
        apply_promotion(config, entry)
    manifest = promotion_manifest.write(entry, config, status=status, path=manifest_path)
    entry['promotion_manifest'] = {
        'status': status,
        'json_path': (manifest.get('output') or {}).get('json_path'),
        'manifest_hash': manifest.get('manifest_hash'),
        'live_profile_hash': (((manifest.get('live') or {}).get('active_profile') or {}).get('canonical') or {}).get('hash'),
    }
    return manifest


def main() -> int:
    ap = argparse.ArgumentParser(description='Promote a Step 2 scoring profile into live config safely.')
    ap.add_argument('--candidate-json', default='')
    ap.add_argument('--variant', default='')
    ap.add_argument('--rank', type=int, default=1)
    ap.add_argument('--reason', default='')
    ap.add_argument('--record-current', action='store_true')
    ap.add_argument('--allow-execution-change', action='store_true')
    ap.add_argument('--allow-unapproved-candidate', action='store_true',
                    help='Explicit override: allow Step 2 promotion without approved_for_live quarantine state.')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--restart', action='store_true')
    ap.add_argument('--restart-timeout', type=float, default=25.0)
    ap.add_argument('--evidence-days', nargs='*', default=[])
    ap.add_argument('--allow-preflight-failure', action='store_true',
                    help='Emergency override: write preflight bundle but do not block on its failure.')
    args = ap.parse_args()

    config, entry, validation = build_entry(args)
    config_before_change = copy.deepcopy(config)
    if not validation.get('ok'):
        print(json.dumps(entry, indent=2, sort_keys=True, default=str))
        return 2
    lifecycle_record = None
    pre_evidence = None
    lifecycle_eval = {'ok': True, 'skipped': True}
    gate = {'ok': True, 'skipped': True}
    if not args.record_current:
        lifecycle_record = candidate_lifecycle.register_candidate(
            entry.get('standard_candidate') or {},
            status='step2_validated',
            event='promotion_selected_candidate',
            artifacts={'candidate_json': args.candidate_json, 'rank': args.rank, 'variant': args.variant},
            write=not args.dry_run,
        )
        pre_evidence = promotion_evidence_packet.build(
            candidate_json=args.candidate_json or '',
            rank=args.rank,
            variant=args.variant,
            days=args.evidence_days or [],
            write=not args.dry_run,
        )
        if not args.dry_run and lifecycle_record.get('candidate_id'):
            lifecycle_record = candidate_lifecycle.attach_evidence(
                str(lifecycle_record['candidate_id']),
                pre_evidence,
                status='promotion_ready',
                write=True,
            )
        lifecycle_eval = candidate_lifecycle.evaluate_record(lifecycle_record, evidence_packet=pre_evidence)
        gate = promotion_gate.evaluate(
            entry.get('standard_candidate') or {},
            days=args.evidence_days or [],
            lifecycle_candidate_id=str((lifecycle_record or {}).get('candidate_id') or ''),
            evidence_packet=pre_evidence,
            require_lifecycle=not args.dry_run,
            require_evidence_packet=True,
            require_evidence_artifacts=not args.dry_run,
        )
        entry['candidate_lifecycle'] = {
            'candidate_id': (lifecycle_record or {}).get('candidate_id'),
            'status': (lifecycle_record or {}).get('status'),
            'registry_path': str(candidate_lifecycle.REGISTRY_PATH.resolve()),
        }
        entry['pre_promotion_evidence_packet'] = (pre_evidence or {}).get('output') or {'ok': (pre_evidence or {}).get('ok')}
        entry['candidate_lifecycle_gate'] = lifecycle_eval
        entry['promotion_gate'] = gate
        if not lifecycle_eval.get('ok') or not gate.get('ok'):
            print(json.dumps(entry, indent=2, sort_keys=True, default=str))
            return 2
    else:
        pre_evidence = promotion_evidence_packet.build(
            candidate_json='',
            rank=args.rank,
            variant='',
            days=args.evidence_days or [],
            write=not args.dry_run,
        )
        entry['pre_promotion_evidence_packet'] = (pre_evidence or {}).get('output') or {'ok': (pre_evidence or {}).get('ok')}
    if args.dry_run:
        entry['dry_run'] = True
        print(json.dumps(entry, indent=2, sort_keys=True, default=str))
        return 0
    registry_path = str(_registry_path_for(entry).resolve())
    entry['registry_path'] = registry_path
    rollback_snapshot = _write_rollback_snapshot(config, entry, registry_path, pre_evidence)
    entry['rollback_snapshot'] = {
        'path': rollback_snapshot.get('path'),
        'prior_profile_hash': rollback_snapshot.get('prior_profile_hash'),
        'new_profile_hash': rollback_snapshot.get('new_profile_hash'),
    }
    entry['new_profile'] = _decorate_profile_metadata(
        entry.get('new_profile') or {},
        entry,
        pre_evidence,
        registry_path,
        rollback_snapshot,
        gate,
    )
    entry['promotion_log_path'] = str(PROMOTION_LOG_PATH.resolve())
    registry_path = _write_registry(entry)
    entry['registry_path'] = registry_path
    try:
        preflight = promotion_preflight_bundle.build(
            candidate_json=args.candidate_json or '',
            rank=args.rank,
            variant=args.variant,
            days=args.evidence_days or [],
            rollback_snapshot_path=rollback_snapshot.get('path') or '',
            promotion_evidence=pre_evidence,
            write=True,
        )
    except Exception as exc:
        preflight = {'ok': False, 'error': repr(exc)}
    entry['promotion_preflight_bundle'] = (preflight.get('output') or {
        'ok': preflight.get('ok'),
        'error': preflight.get('error'),
        'critical_failure_count': preflight.get('critical_failure_count'),
    })
    if not preflight.get('ok') and not args.allow_preflight_failure:
        entry['promotion_preflight_failure'] = {
            'critical_failure_count': preflight.get('critical_failure_count'),
            'checks': [row for row in preflight.get('checks', []) if not row.get('ok')][:20],
            'error': preflight.get('error'),
            'override': '--allow-preflight-failure',
        }
        print(json.dumps(entry, indent=2, sort_keys=True, default=str))
        return 2
    apply_promotion(config, entry)
    log_event = {
        'schema_version': 1,
        'created_at_ct': _now_ct(),
        'action': entry.get('action'),
        'registry_path': registry_path,
        'rollback_snapshot_path': rollback_snapshot.get('path'),
        'evidence_packet_path': ((pre_evidence or {}).get('output') or {}).get('json_path'),
        'prior_profile': {
            'name': (entry.get('prior_profile') or {}).get('name'),
            'profile_hash': rollback_snapshot.get('prior_profile_hash'),
        },
        'new_profile': {
            'name': (entry.get('new_profile') or {}).get('name'),
            'profile_hash': (entry.get('new_profile') or {}).get('profile_hash'),
            'model_id': entry.get('model_id'),
            'family_id': entry.get('family_id'),
        },
        'reason': (entry.get('new_profile') or {}).get('promotion_reason'),
    }
    _append_jsonl(PROMOTION_LOG_PATH, log_event)
    entry['promotion_log_event'] = log_event
    if not args.record_current:
        if lifecycle_record and lifecycle_record.get('candidate_id'):
            candidate_lifecycle.record_promotion(
                str(lifecycle_record['candidate_id']),
                registry_path,
                config_path=str(CONFIG_PATH.resolve()),
                write=True,
            )
    else:
        try:
            current_record = candidate_lifecycle.register_candidate(
                entry.get('standard_candidate') or entry.get('new_profile') or {},
                status='promoted',
                event='current_profile_recorded',
                artifacts={'promotion_registry': registry_path, 'config': str(CONFIG_PATH.resolve())},
                write=True,
            )
            if current_record.get('candidate_id'):
                candidate_lifecycle.record_promotion(str(current_record['candidate_id']), registry_path, str(CONFIG_PATH.resolve()))
        except Exception as exc:
            entry['candidate_lifecycle_error'] = repr(exc)
    try:
        evidence = promotion_evidence_packet.build(
            candidate_json=args.candidate_json or '',
            rank=args.rank,
            variant=args.variant,
            days=args.evidence_days or [],
            promotion_registry_path=registry_path,
            write=True,
        )
        entry['promotion_evidence_packet'] = evidence.get('output') or {'ok': evidence.get('ok')}
        entry['new_profile']['promotion_evidence_packet_path'] = (evidence.get('output') or {}).get('json_path')
        entry['new_profile']['promotion_evidence_packet_hash'] = evidence.get('packet_hash')
        apply_promotion(config, entry)
        if lifecycle_record and lifecycle_record.get('candidate_id'):
            candidate_lifecycle.attach_evidence(str(lifecycle_record['candidate_id']), evidence, status='promoted', write=True)
    except Exception as exc:
        entry['promotion_evidence_packet_error'] = repr(exc)
    if not args.record_current and args.candidate_json:
        try:
            post_canary = candidate_reproducibility_gate.evaluate_file(
                args.candidate_json,
                rank=args.rank,
                variant=args.variant,
                write=True,
                label='post_promotion_canary',
                use_live_profile=True,
            )
        except Exception as exc:
            post_canary = {'ok': False, 'error': repr(exc)}
        entry['post_promotion_canary'] = {
            'ok': bool(post_canary.get('ok')),
            'output_path': post_canary.get('output_path'),
            'report_hash': post_canary.get('report_hash'),
            'failed_checks': post_canary.get('failed_checks', [])[:10],
            'error': post_canary.get('error'),
        }
        entry['new_profile']['post_promotion_canary_path'] = post_canary.get('output_path')
        entry['new_profile']['post_promotion_canary_hash'] = post_canary.get('report_hash')
        apply_promotion(config, entry)
        if not post_canary.get('ok'):
            try:
                entry['rollback_restore'] = _restore_from_rollback_snapshot(rollback_snapshot, entry, post_canary)
            except Exception as exc:
                entry['rollback_restore'] = {'ok': False, 'error': repr(exc)}
            try:
                entry['promotion_quarantine_record'] = promotion_candidate_quarantine.record_promotion(
                    args.candidate_json,
                    rank=args.rank,
                    variant=args.variant,
                    status='rejected',
                    promotion_registry_path=registry_path,
                    evidence_packet_path=((evidence or {}).get('output') or {}).get('json_path') if 'evidence' in locals() else '',
                    canary_path=post_canary.get('output_path') or '',
                    rollback_path=rollback_snapshot.get('path') or '',
                    write=True,
                )
            except Exception as exc:
                entry['promotion_quarantine_update_error'] = repr(exc)
            try:
                restored_config = _load_json(CONFIG_PATH)
                restore_status = 'rolled_back' if (entry.get('rollback_restore') or {}).get('ok') else 'rollback_failed'
                _write_promotion_manifest(entry, restored_config, restore_status, attach_to_live_profile=False)
            except Exception as exc:
                entry['promotion_manifest_error'] = repr(exc)
            try:
                restored_config = _load_json(CONFIG_PATH)
                entry['config_change_journal'] = config_change_journal.record_change(
                    config_before_change,
                    restored_config,
                    action='promotion_rollback_after_canary_failure',
                    reason='Post-promotion canary failed; restored prior active scoring profile from rollback snapshot.',
                    actor='promote_active_profile.py',
                    artifacts={
                        'promotion_registry_path': registry_path,
                        'rollback_snapshot_path': rollback_snapshot.get('path'),
                        'promotion_manifest_path': ((entry.get('promotion_manifest') or {}).get('json_path')),
                        'post_promotion_canary_path': entry.get('post_promotion_canary', {}).get('output_path'),
                        'promotion_preflight_bundle_path': ((entry.get('promotion_preflight_bundle') or {}).get('json_path')),
                    },
                    rollback_snapshot_path=rollback_snapshot.get('path') or '',
                    write=True,
                )
            except Exception as exc:
                entry['config_change_journal_error'] = repr(exc)
            if args.restart:
                entry['rollback_restart'] = _restart_live(args.restart_timeout)
            print(json.dumps(entry, indent=2, sort_keys=True, default=str))
            return 3
        try:
            entry['promotion_quarantine_record'] = promotion_candidate_quarantine.record_promotion(
                args.candidate_json,
                rank=args.rank,
                variant=args.variant,
                status='promoted',
                promotion_registry_path=registry_path,
                evidence_packet_path=((evidence or {}).get('output') or {}).get('json_path') if 'evidence' in locals() else '',
                canary_path=post_canary.get('output_path') or '',
                rollback_path=rollback_snapshot.get('path') or '',
                write=True,
            )
        except Exception as exc:
            entry['promotion_quarantine_update_error'] = repr(exc)
    manifest_status = 'recorded_current' if args.record_current else 'promoted'
    try:
        _write_promotion_manifest(entry, config, manifest_status, attach_to_live_profile=True)
    except Exception as exc:
        entry['promotion_manifest_error'] = repr(exc)
    try:
        after_config = _load_json(CONFIG_PATH)
        entry['config_change_journal'] = config_change_journal.record_change(
            config_before_change,
            after_config,
            action=entry.get('action') or 'promote_active_scoring_profile',
            reason=(entry.get('new_profile') or {}).get('promotion_reason') or args.reason or 'Promoted active scoring profile.',
            actor='promote_active_profile.py',
            artifacts={
                'candidate_json': args.candidate_json or '',
                'promotion_registry_path': entry.get('registry_path'),
                'rollback_snapshot_path': rollback_snapshot.get('path'),
                'promotion_manifest_path': ((entry.get('promotion_manifest') or {}).get('json_path')),
                'promotion_evidence_packet_path': ((entry.get('promotion_evidence_packet') or {}).get('json_path')),
                'promotion_preflight_bundle_path': ((entry.get('promotion_preflight_bundle') or {}).get('json_path')),
                'post_promotion_canary_path': (entry.get('post_promotion_canary') or {}).get('output_path'),
            },
            rollback_snapshot_path=rollback_snapshot.get('path') or '',
            write=True,
        )
    except Exception as exc:
        entry['config_change_journal_error'] = repr(exc)
    try:
        drill = rollback_drill.build(
            snapshot_path=rollback_snapshot.get('path') or '',
            execute=False,
            restart=False,
            reason='Post-promotion rollback drill: prove the prior profile can be restored if this promotion misbehaves.',
            actor='promote_active_profile.py',
            write=True,
        )
        entry['rollback_drill'] = {
            'ok': bool(drill.get('ok')),
            'path': drill.get('path'),
            'snapshot_path': drill.get('snapshot_path'),
            'snapshot_kind': drill.get('snapshot_kind'),
            'target_profile': drill.get('target_profile'),
            'critical_failure_count': drill.get('critical_failure_count'),
        }
    except Exception as exc:
        entry['rollback_drill'] = {'ok': False, 'error': repr(exc)}
    if args.restart:
        entry['restart'] = _restart_live(args.restart_timeout)
        try:
            runtime_proof = live_profile_recovery.promotion_safety_proof(
                config=_load_json(CONFIG_PATH),
                require_running_match=True,
                run_smoke=True,
                write=True,
                label='post_promotion_restart',
            )
            entry['post_promotion_runtime_proof'] = {
                'ok': bool(runtime_proof.get('ok')),
                'path': runtime_proof.get('path'),
                'critical_failure_count': runtime_proof.get('critical_failure_count'),
                'failed_checks': runtime_proof.get('failed_checks', [])[:10],
            }
            entry['last_known_good_profile'] = live_profile_recovery.record_last_known_good(
                config=_load_json(CONFIG_PATH),
                proof=runtime_proof,
                reason='Post-promotion restart, smoke, contract, manifest, evidence, journal, and startup checks passed.',
                write=True,
            )
        except Exception as exc:
            entry['post_promotion_runtime_proof'] = {'ok': False, 'error': repr(exc)}
            entry['last_known_good_profile'] = {
                'ok': False,
                'updated': False,
                'reason': 'post-promotion runtime proof errored',
                'error': repr(exc),
            }
    else:
        entry['last_known_good_profile'] = {
            'ok': False,
            'updated': False,
            'reason': 'not updated because --restart was not requested; startup self-check proof is required',
            'required_action': 'rerun promotion with --restart or run live_profile_recovery.py record-lkg after restarting Live',
        }
    print(json.dumps(entry, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
