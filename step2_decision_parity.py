from __future__ import annotations

from output_paths import output_path

import hashlib
import json
import os
from datetime import datetime
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore

import step2_parity_contract
import step2_execution_contract
import canonical_decision_packet


HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = output_path('postmortem', 'step2_decision_parity')
CT = ZoneInfo('America/Chicago')


def stable_json_hash(payload: Any) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(',', ':'), default=str)
    return hashlib.sha256(blob.encode('utf-8')).hexdigest()


def parity_key(sig: dict[str, Any], ts: int) -> str:
    payload = {
        'ts': int(ts),
        'ticker': sig.get('ticker'),
        'side': sig.get('side'),
        'setup_type': sig.get('setup_type'),
        'score': sig.get('score'),
    }
    return stable_json_hash(payload)[:24]


def active_profile_snapshot(config: dict[str, Any]) -> dict[str, Any]:
    return step2_parity_contract.active_profile_snapshot(config, include_weights=True)


def feature_snapshot(opp: dict[str, Any]) -> dict[str, Any]:
    sig = opp.get('signal') or {}
    features = opp.get('features') or {}
    payload = {
        'score': sig.get('score', opp.get('score')),
        'conviction': sig.get('conviction', opp.get('conviction')),
        'components': sig.get('components') or {},
        'indicators': features.get('ticker_indicators') or sig.get('indicators') or {},
        'btc_indicators': features.get('btc_indicators') or sig.get('btc_indicators') or {},
        'btc_context': sig.get('btc_context') or {},
        'signal_quality': sig.get('signal_quality') or {},
        'relative_strength': sig.get('relative_strength') or {},
        'miner_basket': sig.get('miner_basket') or {},
        'lead_lag': sig.get('lead_lag') or {},
        'forensics': {},
    }
    return {'hash': stable_json_hash(payload), 'payload': payload}


def path_for_day(day: str, out_dir: str = OUT_DIR) -> str:
    return os.path.join(out_dir, f'step2_decision_parity_{day}.jsonl')


def summary_path_for_day(day: str, out_dir: str = OUT_DIR) -> str:
    return os.path.join(out_dir, f'step2_decision_parity_{day}.summary.json')


def row_from_opportunity(opp: dict[str, Any], config: dict[str, Any],
                         run_context: dict[str, Any] | None = None) -> dict[str, Any]:
    sig = dict(opp.get('signal') or {})
    ticker = sig.get('ticker') or opp.get('ticker')
    side = sig.get('side') or opp.get('side')
    setup_type = sig.get('setup_type') or opp.get('setup_type')
    ts = int(opp.get('ts') or sig.get('ts') or 0)
    sig.setdefault('ticker', ticker)
    sig.setdefault('side', side)
    sig.setdefault('setup_type', setup_type)
    sig.setdefault('score', opp.get('score'))
    features = feature_snapshot(opp)
    profile = active_profile_snapshot(config)
    contract = step2_parity_contract.contract(config)
    execution_contract = step2_execution_contract.execution_contract(config)
    outcome = opp.get('outcome') or {}
    action_plan = opp.get('action_plan') or outcome.get('entry_action_plan') or {}
    execution_intent = (
        opp.get('execution_intent')
        or outcome.get('entry_execution_intent')
        or {}
    )
    execution_result = (
        opp.get('execution_result')
        or outcome.get('entry_execution_result')
        or {}
    )
    raw_decision = str(opp.get('decision') or 'detected')
    accepted = raw_decision == 'accepted'
    return {
        'schema_version': 1,
        'source': 'step2_replay',
        'day': opp.get('day'),
        'created_at': ts,
        'created_at_ct': opp.get('ts_ct') or datetime.fromtimestamp(ts, CT).isoformat(timespec='seconds'),
        'parity_key': parity_key(sig, ts),
        'opportunity_id': opp.get('opportunity_id'),
        'ticker': ticker,
        'side': side,
        'setup_type': setup_type,
        'decision': 'entered' if accepted else 'skipped',
        'step2_decision': raw_decision,
        'reason': 'entered' if accepted else (opp.get('reject_reason') or 'rejected'),
        'price': opp.get('price') if opp.get('price') is not None else sig.get('price'),
        'score': sig.get('score'),
        'conviction': sig.get('conviction') or opp.get('conviction'),
        'reasons': sig.get('reasons') or [],
        'execution_mode': 'step2_replay',
        'step2_parity_contract_hash': step2_parity_contract.contract_hash(contract),
        'step2_parity_contract': contract,
        'execution_kernel_hash': execution_contract.get('execution_kernel_hash'),
        'step2_execution_contract_hash': step2_execution_contract.execution_contract_hash(config),
        'active_profile_name': profile.get('name'),
        'active_profile_hash': profile.get('hash'),
        'active_profile_bias': profile.get('bias'),
        'feature_snapshot_hash': features.get('hash'),
        'feature_snapshot': features.get('payload'),
        'action_plan': action_plan or None,
        'action_plan_id': action_plan.get('action_plan_id') if isinstance(action_plan, dict) else None,
        'action_plan_hash': action_plan.get('action_plan_hash') if isinstance(action_plan, dict) else None,
        'execution_intent': execution_intent or None,
        'execution_intent_id': (
            execution_intent.get('execution_intent_id') if isinstance(execution_intent, dict) else None
        ),
        'semantic_execution_intent_hash': (
            execution_intent.get('semantic_execution_intent_hash') if isinstance(execution_intent, dict) else None
        ),
        'execution_result': execution_result or None,
        'execution_result_id': (
            execution_result.get('execution_result_id') if isinstance(execution_result, dict) else None
        ),
        'run_context': run_context or {},
        'outcome': outcome or None,
        'outcome_summary': {
            'pnl': outcome.get('pnl'),
            'reason': outcome.get('reason'),
            'entry': outcome.get('entry'),
            'exit': outcome.get('exit'),
            'tp': outcome.get('tp'),
            'sl': outcome.get('sl'),
            'entry_ct': outcome.get('entry_ct'),
            'exit_ct': outcome.get('exit_ct'),
            'held_sec': outcome.get('held_sec'),
        } if outcome else None,
        'join_hint': {
            'ticker': ticker,
            'side': side,
            'timestamp_second': ts,
            'setup_type': setup_type,
            'score': sig.get('score'),
        },
    }


def write_day(day: str, opportunities: list[dict[str, Any]], config: dict[str, Any],
              run_context: dict[str, Any] | None = None,
              out_dir: str = OUT_DIR) -> dict[str, Any]:
    rows = [row_from_opportunity(opp, config, run_context=run_context) for opp in opportunities]
    try:
        canonical_summary = canonical_decision_packet.write_step2_packets(day, rows)
    except Exception as exc:
        canonical_summary = {'ok': False, 'error': repr(exc)}
    path = path_for_day(day, out_dir=out_dir)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        for row in rows:
            f.write(json.dumps(row, separators=(',', ':'), sort_keys=True, default=str) + '\n')
    entered = [row for row in rows if row.get('decision') == 'entered']
    skipped = [row for row in rows if row.get('decision') == 'skipped']
    summary = {
        'schema_version': 1,
        'source': 'step2_replay',
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'path': os.path.abspath(path),
        'rows': len(rows),
        'entered': len(entered),
        'skipped': len(skipped),
        'pnl': round(sum(float(((row.get('outcome_summary') or {}).get('pnl')) or 0.0) for row in entered), 4),
        'run_context': run_context or {},
        'canonical_decision_packets': canonical_summary,
    }
    summary_path = summary_path_for_day(day, out_dir=out_dir)
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, sort_keys=True, default=str)
    summary['summary_path'] = os.path.abspath(summary_path)
    return summary
