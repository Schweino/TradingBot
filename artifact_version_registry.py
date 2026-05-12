"""Version registry for replay, Step 2, and parity artifacts."""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import active_engine_baseline
import execution_kernel
import semantic_config
import step2_execution_contract
import step2_latency_model
import step2_parity_contract
import tournament_safety


HERE = os.path.dirname(os.path.abspath(__file__))
CT = ZoneInfo('America/Chicago')
OUT_DIR = os.path.join(HERE, 'postmortem', 'artifact_registry')
SCHEMA_VERSION = 1


def _read_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return default


def _file_meta(path: str) -> dict[str, Any]:
    return {
        'path': os.path.abspath(path),
        'exists': os.path.exists(path),
        'bytes': os.path.getsize(path) if os.path.exists(path) else 0,
        'mtime': os.path.getmtime(path) if os.path.exists(path) else None,
        'sha256': tournament_safety._file_sha256(path),
    }


def _stable_hash(payload: Any) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(',', ':'), default=str).encode('utf-8')
    return hashlib.sha256(blob).hexdigest()


def _config() -> dict[str, Any]:
    return _read_json(os.path.join(HERE, 'trading_config.json'), {}) or {}


def _artifact_paths(day: str, tickers: list[str]) -> dict[str, str]:
    tickers_part = '-'.join(tickers)
    compiled_name = f'compiled_step2_live_mockparity_{tickers_part}_{day}_intraday'
    promotion_dir = os.path.join(HERE, 'postmortem', 'promotions', 'active_scoring_profiles')
    promotion_entries = []
    if os.path.isdir(promotion_dir):
        promotion_entries = [
            os.path.join(promotion_dir, name)
            for name in os.listdir(promotion_dir)
            if name.endswith('.json')
        ]
    latest_promotion = max(promotion_entries, key=os.path.getmtime) if promotion_entries else os.path.join(
        promotion_dir,
        'missing_active_profile_promotion_registry.json',
    )
    reproducibility_dir = os.path.join(HERE, 'postmortem', 'candidate_reproducibility')
    reproducibility_entries = []
    if os.path.isdir(reproducibility_dir):
        reproducibility_entries = [
            os.path.join(reproducibility_dir, name)
            for name in os.listdir(reproducibility_dir)
            if name.endswith('.json')
        ]
    latest_reproducibility = (
        max(reproducibility_entries, key=os.path.getmtime)
        if reproducibility_entries
        else os.path.join(reproducibility_dir, 'missing_candidate_reproducibility.json')
    )
    promotion_manifest_dir = os.path.join(HERE, 'postmortem', 'promotions', 'manifests')
    promotion_manifest_entries = []
    if os.path.isdir(promotion_manifest_dir):
        promotion_manifest_entries = [
            os.path.join(promotion_manifest_dir, name)
            for name in os.listdir(promotion_manifest_dir)
            if name.endswith('.json') and name != 'PROMOTION_MANIFEST_LATEST.json'
        ]
    latest_promotion_manifest = (
        max(promotion_manifest_entries, key=os.path.getmtime)
        if promotion_manifest_entries
        else os.path.join(promotion_manifest_dir, 'missing_promotion_manifest.json')
    )
    promotion_preflight_dir = os.path.join(HERE, 'postmortem', 'promotion_preflight')
    promotion_preflight_entries = []
    if os.path.isdir(promotion_preflight_dir):
        promotion_preflight_entries = [
            os.path.join(promotion_preflight_dir, name)
            for name in os.listdir(promotion_preflight_dir)
            if name.endswith('.json')
        ]
    latest_promotion_preflight = (
        max(promotion_preflight_entries, key=os.path.getmtime)
        if promotion_preflight_entries
        else os.path.join(promotion_preflight_dir, 'missing_promotion_preflight.json')
    )
    return {
        'intraday_events': os.path.join(
            HERE, 'data_cache', 'live_intraday_tapes',
            f'sip_per-second_bars_{tickers_part}_{day}.events.json.gz',
        ),
        'canonical_events': os.path.join(
            HERE, 'data_cache', 'alpaca_engine_replay_tapes',
            f'sip_per-second_bars_{tickers_part}_{day}.events.json.gz',
        ),
        'decision_tape': os.path.join(
            HERE, 'postmortem', 'backtests', 'decision_tapes',
            f'decision_tape_sip_per-second_bars_live_{tickers_part}_{day}.jsonl.gz',
        ),
        'compiled_manifest': os.path.join(
            HERE, 'postmortem', 'backtests', 'compiled_decision_tapes', compiled_name, 'manifest.json',
        ),
        'compiled_chunk_manifest': os.path.join(
            HERE, 'postmortem', 'backtests', 'compiled_decision_tapes',
            compiled_name, 'chunks', 'chunk_manifest.json',
        ),
        'step2_score': os.path.join(
            HERE, 'postmortem', 'backtests', 'step2_today_compiled', f'step2_today_compiled_{day}.json',
        ),
        'step2_today_compiled': os.path.join(
            HERE, 'postmortem', 'backtests', 'step2_today_compiled', f'step2_today_compiled_{day}.json',
        ),
        'step2_rebuild_plan': os.path.join(
            HERE, 'postmortem', 'rebuild_plans', f'step2_rebuild_plan_{day}.json',
        ),
        'step2_cache_certification': os.path.join(
            HERE,
            'postmortem',
            'cache_certifications',
            f"step2_cache_certification_{day}_{tickers_part}.json",
        ),
        'cache_certification': os.path.join(
            HERE,
            'postmortem',
            'cache_certifications',
            f"step2_cache_certification_{day}_{tickers_part}.json",
        ),
        'replay_state_checkpoint_manifest': os.path.join(
            HERE, 'postmortem', 'backtests', 'replay_state_checkpoints',
            f'sip_per-second_bars_live_{tickers_part}', day, 'manifest.json',
        ),
        'live_signal_parity': os.path.join(
            HERE, 'postmortem', 'live_signal_parity', f'live_signal_parity_{day}.jsonl',
        ),
        'step2_decision_parity': os.path.join(
            HERE, 'postmortem', 'step2_decision_parity', f'step2_decision_parity_{day}.jsonl',
        ),
        'step2_decision_parity_summary': os.path.join(
            HERE, 'postmortem', 'step2_decision_parity', f'step2_decision_parity_{day}.summary.json',
        ),
        'unified_step2_current_trace': os.path.join(
            HERE, 'postmortem', 'unified_decision_ledger', day, f'step2_current_trace_{day}.jsonl',
        ),
        'unified_step2_current_trace_summary': os.path.join(
            HERE, 'postmortem', 'unified_decision_ledger', day, f'step2_current_trace_{day}.summary.json',
        ),
        'unified_live_signal_parity': os.path.join(
            HERE, 'postmortem', 'unified_decision_ledger', day, f'live_signal_parity_{day}.jsonl',
        ),
        'unified_live_signal_parity_summary': os.path.join(
            HERE, 'postmortem', 'unified_decision_ledger', day, f'live_signal_parity_{day}.summary.json',
        ),
        'unified_live_decision_source': os.path.join(
            HERE, 'postmortem', 'unified_decision_ledger', day, f'live_decision_source_{day}.jsonl',
        ),
        'canonical_live_decision_packets': os.path.join(
            HERE, 'postmortem', 'canonical_decision_packets', day, f'live_decision_packets_{day}.jsonl',
        ),
        'canonical_live_pre_action_packets': os.path.join(
            HERE, 'postmortem', 'canonical_decision_packets', day, f'live_pre_action_packets_{day}.jsonl',
        ),
        'canonical_step2_decision_packets': os.path.join(
            HERE, 'postmortem', 'canonical_decision_packets', day, f'step2_decision_packets_{day}.jsonl',
        ),
        'canonical_live_exit_packets': os.path.join(
            HERE, 'postmortem', 'canonical_decision_packets', day, f'live_exit_packets_{day}.jsonl',
        ),
        'canonical_packet_diff': os.path.join(
            HERE, 'postmortem', 'canonical_decision_packets', day, f'canonical_packet_diff_{day}.json',
        ),
        'parity_verdict': os.path.join(
            HERE, 'postmortem', 'parity_verdict', f'parity_verdict_{day}.json',
        ),
        'parity_verdict_text': os.path.join(
            HERE, 'postmortem', 'parity_verdict', f'parity_verdict_{day}.txt',
        ),
        'market_data_integrity': os.path.join(
            HERE, 'postmortem', 'market_data_integrity', f'market_data_integrity_{day}.json',
        ),
        'market_data_integrity_text': os.path.join(
            HERE, 'postmortem', 'market_data_integrity', f'market_data_integrity_{day}.txt',
        ),
        'market_data_freshness': os.path.join(
            HERE, 'postmortem', 'market_data_freshness', f'market_data_freshness_{day}.json',
        ),
        'market_data_freshness_text': os.path.join(
            HERE, 'postmortem', 'market_data_freshness', f'market_data_freshness_{day}.txt',
        ),
        'baseline_drift_sentinel': os.path.join(
            HERE, 'postmortem', 'baseline_drift', f'baseline_drift_sentinel_{day}.json',
        ),
        'intraday_shadow_step2': os.path.join(
            HERE, 'postmortem', 'intraday_shadow_step2', day, f'shadow_step2_{day}.jsonl',
        ),
        'intraday_shadow_step2_summary': os.path.join(
            HERE, 'postmortem', 'intraday_shadow_step2', day, f'shadow_step2_{day}.summary.json',
        ),
        'shadow_variant_live_decisions': os.path.join(
            HERE, 'postmortem', 'shadow_variants', day, f'shadow_variant_live_decisions_{day}.jsonl',
        ),
        'shadow_variant_decisions': os.path.join(
            HERE, 'postmortem', 'shadow_variants', day, f'shadow_variant_decisions_{day}.jsonl',
        ),
        'shadow_variant_leaderboard': os.path.join(
            HERE, 'postmortem', 'shadow_variants', day, f'shadow_variant_leaderboard_{day}.json',
        ),
        'shadow_variant_candidates': os.path.join(
            HERE, 'postmortem', 'shadow_variants', 'candidates.json',
        ),
        'candidate_lifecycle_registry': os.path.join(
            HERE, 'postmortem', 'candidate_lifecycle', 'candidate_lifecycle_registry.json',
        ),
        'candidate_lifecycle_dashboard': os.path.join(
            HERE, 'postmortem', 'candidate_lifecycle', f'candidate_lifecycle_dashboard_{day}.json',
        ),
        'deterministic_live_replay': os.path.join(
            HERE, 'postmortem', 'deterministic_live_replay', f'deterministic_live_replay_{day}.json',
        ),
        'architecture_drift_gate': os.path.join(
            HERE, 'postmortem', 'architecture_drift', f'architecture_drift_gate_{day}.json',
        ),
        'execution_lifecycle': os.path.join(
            HERE, 'postmortem', 'execution_lifecycle', day, f'execution_lifecycle_{day}.jsonl',
        ),
        'execution_lifecycle_summary': os.path.join(
            HERE, 'postmortem', 'execution_lifecycle', day, f'execution_lifecycle_{day}.summary.json',
        ),
        'order_lifecycle_reconciliation': os.path.join(
            HERE, 'postmortem', 'order_lifecycle_reconciliation', f'order_lifecycle_reconciliation_{day}.json',
        ),
        'order_lifecycle_reconciliation_text': os.path.join(
            HERE, 'postmortem', 'order_lifecycle_reconciliation', f'order_lifecycle_reconciliation_{day}.txt',
        ),
        'daily_parity_scorecard': os.path.join(
            HERE, 'postmortem', 'daily_parity_scorecard', f'daily_parity_scorecard_{day}.json',
        ),
        'daily_parity_scorecard_text': os.path.join(
            HERE, 'postmortem', 'daily_parity_scorecard', f'daily_parity_scorecard_{day}.txt',
        ),
        'realtime_parity_alerts': os.path.join(
            HERE, 'postmortem', 'realtime_parity_alerts', f'parity_alerts_{day}.jsonl',
        ),
        'artifact_dag': os.path.join(
            HERE, 'postmortem', 'artifact_dag', f'artifact_dag_{day}.json',
        ),
        'contract_gate': os.path.join(
            HERE, 'postmortem', 'contract_gate', f'contract_gate_{day}.json',
        ),
        'golden_parity_suite': os.path.join(
            HERE, 'postmortem', 'golden_parity', 'golden_parity_all.json',
        ),
        'canonical_opportunities': os.path.join(
            HERE, 'postmortem', 'canonical_opportunities', f'canonical_opportunities_{day}.jsonl',
        ),
        'canonical_opportunities_summary': os.path.join(
            HERE, 'postmortem', 'canonical_opportunities', f'canonical_opportunities_{day}.summary.json',
        ),
        'incremental_market_store': os.path.join(
            HERE, 'data_cache', 'incremental_market_store', day, 'manifest.json',
        ),
        'active_profile_promotion_registry_latest': latest_promotion,
        'candidate_reproducibility_latest': latest_reproducibility,
        'promotion_candidate_quarantine': os.path.join(
            HERE, 'postmortem', 'promotion_quarantine', 'promotion_candidate_quarantine.json',
        ),
        'promotion_manifest_latest': latest_promotion_manifest,
        'promotion_manifest_latest_pointer': os.path.join(
            HERE, 'postmortem', 'promotions', 'manifests', 'PROMOTION_MANIFEST_LATEST.json',
        ),
        'config_change_journal_latest': os.path.join(
            HERE, 'postmortem', 'config_change_journal', 'CONFIG_CHANGE_JOURNAL_LATEST.json',
        ),
        'config_change_journal': os.path.join(
            HERE, 'postmortem', 'config_change_journal', 'config_change_journal.jsonl',
        ),
        'promotion_preflight_latest': latest_promotion_preflight,
    }


def build(day: str, tickers: list[str] | None = None,
          extra_artifacts: dict[str, str] | None = None) -> dict[str, Any]:
    tickers = [str(t).upper() for t in (tickers or ['CLSK', 'MARA', 'RIOT'])]
    cfg = _config()
    execution_kernel_contract = execution_kernel.contract_from_config(cfg)
    paths = _artifact_paths(day, tickers)
    if extra_artifacts:
        paths.update(extra_artifacts)
    active = active_engine_baseline.active_profile_payload()
    semantic = semantic_config.report(cfg)
    latency_path = step2_latency_model.DEFAULT_MODEL_PATH
    payload = {
        'schema_version': SCHEMA_VERSION,
        'day': day,
        'tickers': tickers,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'contracts': {
            'execution_kernel_hash': execution_kernel_contract.get('execution_kernel_hash'),
            'execution_kernel_contract': execution_kernel_contract,
            'step2_execution_contract_hash': step2_execution_contract.execution_contract_hash(cfg),
            'step2_parity_contract_hash': step2_parity_contract.contract_hash(step2_parity_contract.contract(cfg)),
            'execution_action_engine_code_hash': tournament_safety._file_sha256(
                os.path.join(HERE, 'execution_action_engine.py')
            ),
            'execution_intent_engine_code_hash': tournament_safety._file_sha256(
                os.path.join(HERE, 'execution_intent_engine.py')
            ),
            'latency_model_hash': tournament_safety._file_sha256(latency_path),
            'strategy_config_hash': tournament_safety._file_sha256(os.path.join(HERE, 'trading_config.json')),
            'active_profile_hash': ((active.get('profile') or {}).get('hash') or active.get('hash')),
            'active_profile': active.get('profile') or active,
            'semantic_config_section_hashes': semantic.get('section_hashes'),
            'compiled_tape_semantic_hash': semantic.get('compiled_tape_semantic_hash'),
            'semantic_config_layer_sections': semantic.get('layer_sections'),
        },
        'artifacts': {name: _file_meta(path) for name, path in sorted(paths.items())},
    }
    payload['registry_hash'] = _stable_hash({
        'day': payload['day'],
        'tickers': payload['tickers'],
        'contracts': payload['contracts'],
        'artifacts': payload['artifacts'],
    })
    return payload


def write(day: str, tickers: list[str] | None = None,
          extra_artifacts: dict[str, str] | None = None) -> str:
    payload = build(day, tickers=tickers, extra_artifacts=extra_artifacts)
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, f'artifact_registry_{day}.json')
    tmp = f'{path}.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
    os.replace(tmp, path)
    return os.path.abspath(path)


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser(description='Write artifact version registry for a day.')
    ap.add_argument('day')
    ap.add_argument('--tickers', nargs='+', default=['CLSK', 'MARA', 'RIOT'])
    ap.add_argument('--json', action='store_true')
    args = ap.parse_args()
    path = write(args.day, args.tickers)
    payload = _read_json(path, {}) or {}
    print(json.dumps(payload if args.json else {'path': path, 'registry_hash': payload.get('registry_hash')},
                     indent=2, sort_keys=True, default=str))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
