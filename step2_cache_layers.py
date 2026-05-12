"""Layered Step 2 cache manifests and rebuild diagnostics."""
from __future__ import annotations

from output_paths import output_path

import gzip
import hashlib
import json
import os
import time
from datetime import datetime
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import step2_execution_contract
import step2_latency_model
import step2_parity_contract
import compiled_tape_lineage
import semantic_config
import semantic_diagnostics
import step2_artifact_identity
import tournament_safety


HERE = os.path.dirname(os.path.abspath(__file__))
CT = ZoneInfo('America/Chicago')
OUT_DIR = output_path('postmortem', 'cache_layers')

LAYER_CODE_INPUTS = {
    'market_events': [
        'live_step2_feed.py',
        'incremental_market_store.py',
        'data_fetch.py',
        'event_store.py',
    ],
    'signal_features': [
        'ws_scalp.py',
        'scoring_profiles.py',
        'backtest_30d_engine.py',
        'replay_state_checkpoints.py',
        'semantic_config.py',
    ],
    'path_outcomes': [
        'build_decision_tape.py',
        'opportunity_outcome_cache.py',
        'replay_artifacts.py',
        'bracket_rounding.py',
        'step2_execution_contract.py',
        'step2_latency_model.py',
        'semantic_config.py',
    ],
    'compiled_step2': [
        'compiled_chunk_store.py',
        'decision_tape_event_cache.py',
        'decision_tape_compiled.py',
        'decision_tape_gates.py',
        'routed_scoring_profile.py',
        'simulate_decision_tape.py',
        'scoring_variant_lab_fast.py',
        'scoring_variant_lab_massive.py',
        'step2_parity_contract.py',
        'step2_range_linker.py',
        'semantic_config.py',
        'step2_artifact_identity.py',
    ],
    'live_signal_parity': [
        'live_signal_step2_parity.py',
        'live_step2_parity_report.py',
        'canonical_opportunity_ledger.py',
        'parity_diff_classifier.py',
        'artifact_version_registry.py',
        'step2_execution_contract.py',
        'step2_latency_model.py',
        'semantic_config.py',
    ],
}

LAYER_SEMANTIC_SECTIONS = semantic_config.LAYER_SECTIONS
LINEAGE_CODE_INPUTS = list(compiled_tape_lineage.DEFAULT_COMPILED_TAPE_CODE_HASH_INPUTS)


def _read_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return default


def _count_jsonl_gz(path: str) -> dict[str, Any]:
    identity = step2_artifact_identity.jsonl_gz_identity(path)
    return {
        'exists': identity.get('exists'),
        'rows': identity.get('rows'),
        'min_ts': identity.get('min_ts'),
        'max_ts': identity.get('max_ts'),
        'sha256': identity.get('physical_sha256'),
        'physical_sha256': identity.get('physical_sha256'),
        'semantic_sha256': identity.get('semantic_sha256'),
        'mtime': identity.get('mtime'),
    }


def _file_meta(path: str) -> dict[str, Any]:
    return {
        'path': os.path.abspath(path),
        'exists': os.path.exists(path),
        'sha256': tournament_safety._file_sha256(path),
        'bytes': os.path.getsize(path) if os.path.exists(path) else 0,
        'mtime': os.path.getmtime(path) if os.path.exists(path) else None,
    }


def _code_hashes(paths: list[str]) -> dict[str, str | None]:
    return {path: tournament_safety._file_sha256(os.path.join(HERE, path)) for path in paths}


def _hash_payload(payload: dict[str, Any]) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(',', ':'), default=str).encode('utf-8')
    return hashlib.sha256(blob).hexdigest()


def _paths(day: str, tickers: list[str], feed: str, quote_mode: str, btc_mode: str,
           indicator_mode: str, compiled_name: str | None = None) -> dict[str, str]:
    ticker_part = '-'.join(tickers)
    compiled = compiled_name or f'compiled_step2_live_mockparity_{ticker_part}_{day}_intraday'
    return {
        'intraday_events': os.path.join(
            HERE, 'data_cache', 'live_intraday_tapes',
            f'{feed}_{quote_mode}_{btc_mode}_{ticker_part}_{day}.events.json.gz',
        ),
        'canonical_events': os.path.join(
            HERE, 'data_cache', 'alpaca_engine_replay_tapes',
            f'{feed}_{quote_mode}_{btc_mode}_{ticker_part}_{day}.events.json.gz',
        ),
        'decision_tape': os.path.join(
            HERE, 'postmortem', 'backtests', 'decision_tapes',
            f'decision_tape_{feed}_{quote_mode}_{btc_mode}_{indicator_mode}_{ticker_part}_{day}.jsonl.gz',
        ),
        'compiled_manifest': os.path.join(
            HERE, 'postmortem', 'backtests', 'compiled_decision_tapes', compiled, 'manifest.json',
        ),
        'compiled_chunk_manifest': os.path.join(
            HERE, 'postmortem', 'backtests', 'compiled_decision_tapes', compiled, 'chunks', 'chunk_manifest.json',
        ),
        'compiled_day_shard_manifest': os.path.join(
            HERE, 'postmortem', 'backtests', 'compiled_decision_tapes', compiled, 'day_shards', 'day_shard_manifest.json',
        ),
        'step2_score': os.path.join(
            HERE, 'postmortem', 'backtests', 'step2_today_compiled', f'step2_today_compiled_{day}.json',
        ),
        'live_signal_parity': os.path.join(
            HERE, 'postmortem', 'live_signal_parity', f'live_signal_parity_{day}.jsonl',
        ),
        'step2_decision_parity': os.path.join(
            HERE, 'postmortem', 'step2_decision_parity', f'step2_decision_parity_{day}.jsonl',
        ),
        'canonical_opportunities': os.path.join(
            HERE, 'postmortem', 'canonical_opportunities', f'canonical_opportunities_{day}.jsonl',
        ),
        'incremental_market_store': os.path.join(
            HERE, 'data_cache', 'incremental_market_store', day, 'manifest.json',
        ),
        'artifact_registry': os.path.join(
            HERE, 'postmortem', 'artifact_registry', f'artifact_registry_{day}.json',
        ),
    }


def _compiled_lineage(manifest_path: str, config: dict[str, Any] | None = None) -> dict[str, Any]:
    if not os.path.exists(manifest_path):
        return compiled_tape_lineage.evaluate_mismatches(
            [{
                'path': manifest_path,
                'reason': 'compiled_manifest_missing',
                'layer': 'compiled_scoring_math',
                'score_affecting': True,
                'dependency_known': True,
                'lineage_reason': 'Compiled Step 2 manifest is missing.',
            }],
            manifest_path=manifest_path,
            code_hash_inputs=LINEAGE_CODE_INPUTS,
        )
    manifest = _read_json(manifest_path, {}) or {}
    recorded_semantic = manifest.get('semantic_config_section_hashes') or {}
    manifest['manifest_path'] = manifest_path
    semantic_mismatches = semantic_config.mismatches(
        recorded_semantic,
        manifest.get('compiled_tape_semantic_sections') or semantic_config.COMPILED_TAPE_SECTIONS,
        config,
    )
    semantic_mismatches = semantic_diagnostics.enrich_semantic_mismatches(
        semantic_mismatches,
        config=config,
        recorded_payloads=manifest.get('semantic_config_section_payloads') or {},
    )
    return compiled_tape_lineage.evaluate_manifest(
        manifest,
        code_hash_inputs=LINEAGE_CODE_INPUTS,
        semantic_mismatches=semantic_mismatches,
    )


def _compiled_mismatches(manifest_path: str, config: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    return list((_compiled_lineage(manifest_path, config).get('all_mismatches') or []))


def report(day: str, tickers: list[str], feed: str = 'sip', quote_mode: str = 'per-second',
           btc_mode: str = 'bars', indicator_mode: str = 'live',
           compiled_name: str | None = None, trading_config: dict | None = None) -> dict[str, Any]:
    tickers = [ticker.upper() for ticker in tickers]
    paths = _paths(day, tickers, feed, quote_mode, btc_mode, indicator_mode, compiled_name)
    cfg = trading_config or _read_json(os.path.join(HERE, 'trading_config.json'), {}) or {}
    parity = step2_parity_contract.contract(cfg)
    semantic = semantic_config.report(cfg)
    layers: dict[str, Any] = {
        'market_events': {
            'intraday': _file_meta(paths['intraday_events']),
            'canonical': _file_meta(paths['canonical_events']),
            'incremental_market_store': _file_meta(paths['incremental_market_store']),
            'code_hashes': _code_hashes(LAYER_CODE_INPUTS['market_events']),
            'semantic_sections': list(LAYER_SEMANTIC_SECTIONS.get('market_events', ())),
        },
        'signal_features': {
            'decision_tape': _count_jsonl_gz(paths['decision_tape']),
            'code_hashes': _code_hashes(LAYER_CODE_INPUTS['signal_features']),
            'semantic_sections': list(LAYER_SEMANTIC_SECTIONS.get('signal_features', ())),
        },
        'path_outcomes': {
            'decision_tape': _count_jsonl_gz(paths['decision_tape']),
            'latency_model': _file_meta(step2_latency_model.DEFAULT_MODEL_PATH),
            'latency_model_learning': semantic_diagnostics.latency_model_fingerprint(step2_latency_model.DEFAULT_MODEL_PATH),
            'execution_contract_hash': step2_execution_contract.execution_contract_hash(cfg),
            'code_hashes': _code_hashes(LAYER_CODE_INPUTS['path_outcomes']),
            'semantic_sections': list(LAYER_SEMANTIC_SECTIONS.get('path_outcomes', ())),
        },
        'compiled_step2': {
            'manifest': _file_meta(paths['compiled_manifest']),
            'chunk_manifest': _file_meta(paths['compiled_chunk_manifest']),
            'day_shard_manifest': _file_meta(paths['compiled_day_shard_manifest']),
            'score_report': _file_meta(paths['step2_score']),
            'code_hashes': _code_hashes(LAYER_CODE_INPUTS['compiled_step2']),
            'semantic_sections': list(LAYER_SEMANTIC_SECTIONS.get('compiled_step2', ())),
        },
        'live_signal_parity': {
            'live_signal_parity': _file_meta(paths['live_signal_parity']),
            'step2_decision_parity': _file_meta(paths['step2_decision_parity']),
            'canonical_opportunities': _file_meta(paths['canonical_opportunities']),
            'artifact_registry': _file_meta(paths['artifact_registry']),
            'code_hashes': _code_hashes(LAYER_CODE_INPUTS['live_signal_parity']),
            'semantic_sections': list(LAYER_SEMANTIC_SECTIONS.get('live_signal_parity', ())),
        },
    }
    compiled_manifest = _read_json(paths['compiled_manifest'], {}) or {}
    lineage = _compiled_lineage(paths['compiled_manifest'], cfg)
    mismatches = lineage.get('all_mismatches') or []
    stale_layers = sorted({str(row.get('layer') or 'unknown') for row in mismatches})
    scoring_stale_layers = sorted({
        str(row.get('layer') or 'unknown')
        for row in mismatches
        if row.get('score_affecting')
    })
    recommended_action = str(lineage.get('recommended_action') or 'full_signal_rebuild')
    payload = {
        'schema_version': 1,
        'day': day,
        'tickers': tickers,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'paths': paths,
        'layers': layers,
        'compiled_manifest_hash': compiled_manifest.get('compiled_tape_hash'),
        'compiled_step2_execution_contract_hash': compiled_manifest.get('step2_execution_contract_hash'),
        'current_step2_execution_contract_hash': step2_execution_contract.execution_contract_hash(cfg),
        'current_step2_parity_contract_hash': step2_parity_contract.contract_hash(parity),
        'semantic_config_section_hashes': semantic.get('section_hashes'),
        'compiled_tape_semantic_hash': semantic.get('compiled_tape_semantic_hash'),
        'semantic_layer_sections': semantic.get('layer_sections'),
        'stale_layers': stale_layers,
        'scoring_stale_layers': scoring_stale_layers,
        'stale_reasons': mismatches[:25],
        'compiled_tape_lineage': lineage,
        'lineage_status': lineage.get('status'),
        'lineage_certified': lineage.get('certified'),
        'lineage_quick_score_allowed': lineage.get('quick_score_allowed'),
        'lineage_rebuild_required': lineage.get('rebuild_required'),
        'recommended_action': recommended_action,
    }
    payload['cache_layer_hash'] = _hash_payload({
        'day': payload['day'],
        'tickers': payload['tickers'],
        'layers': payload['layers'],
        'compiled_manifest_hash': payload['compiled_manifest_hash'],
        'current_step2_execution_contract_hash': payload['current_step2_execution_contract_hash'],
        'current_step2_parity_contract_hash': payload['current_step2_parity_contract_hash'],
        'semantic_config_section_hashes': payload['semantic_config_section_hashes'],
    })
    return payload


def write_report(day: str, tickers: list[str], **kwargs: Any) -> str:
    payload = report(day, tickers, **kwargs)
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, f'step2_cache_layers_{day}.json')
    tmp = f'{path}.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
    os.replace(tmp, path)
    return os.path.abspath(path)


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser(description='Write Step 2 layered cache status/rebuild report.')
    ap.add_argument('day')
    ap.add_argument('--tickers', nargs='+', default=['CLSK', 'MARA', 'RIOT'])
    ap.add_argument('--feed', default='sip')
    ap.add_argument('--quote-mode', default='per-second')
    ap.add_argument('--btc-mode', default='bars')
    ap.add_argument('--indicator-mode', default='live')
    ap.add_argument('--json', action='store_true')
    args = ap.parse_args()
    payload = report(args.day, args.tickers, args.feed, args.quote_mode, args.btc_mode, args.indicator_mode)
    path = write_report(args.day, args.tickers, feed=args.feed, quote_mode=args.quote_mode,
                        btc_mode=args.btc_mode, indicator_mode=args.indicator_mode)
    payload['path'] = path
    print(json.dumps(payload if args.json else {'path': path, 'recommended_action': payload['recommended_action']},
                     indent=2, sort_keys=True, default=str))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
