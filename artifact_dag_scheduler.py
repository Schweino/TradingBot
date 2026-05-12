"""Content-aware artifact DAG planner for Step 2/Live parity artifacts."""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import step2_cache_layers
import step2_artifact_identity
import step2_rebuild_planner
import worker_policy


HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, 'postmortem', 'artifact_dag')
CT = ZoneInfo('America/Chicago')
SCHEMA_VERSION = 1
DEFAULT_TICKERS = ['CLSK', 'MARA', 'RIOT']

NODES = {
    'market_events': {'deps': [], 'layer': 'market_events'},
    'market_data_integrity': {'deps': ['market_events'], 'layer': 'market_events'},
    'signal_features': {'deps': ['market_events'], 'layer': 'signal_features'},
    'path_outcomes': {'deps': ['signal_features'], 'layer': 'path_outcomes'},
    'compiled_step2': {'deps': ['path_outcomes'], 'layer': 'compiled_step2'},
    'step2_score': {'deps': ['compiled_step2'], 'layer': 'compiled_step2'},
    'live_shadow_step2': {'deps': [], 'layer': 'live_signal_parity'},
    'shadow_variants': {'deps': ['market_events', 'live_shadow_step2'], 'layer': 'live_signal_parity'},
    'candidate_lifecycle': {'deps': ['shadow_variants'], 'layer': 'live_signal_parity'},
    'deterministic_live_replay': {'deps': ['live_shadow_step2'], 'layer': 'live_signal_parity'},
    'architecture_drift_gate': {'deps': ['deterministic_live_replay'], 'layer': 'live_signal_parity'},
    'live_signal_parity': {'deps': ['market_events', 'step2_score', 'live_shadow_step2', 'shadow_variants'], 'layer': 'live_signal_parity'},
    'canonical_decision_packets': {'deps': ['live_signal_parity'], 'layer': 'live_signal_parity'},
    'parity_report': {'deps': ['canonical_decision_packets', 'architecture_drift_gate', 'candidate_lifecycle'], 'layer': 'live_signal_parity'},
    'artifact_registry': {'deps': ['parity_report'], 'layer': 'live_signal_parity'},
}


def _write_json(path: str, payload: dict[str, Any]) -> str:
    return step2_artifact_identity.write_json(path, payload)


def _node_fingerprint(name: str, meta: dict[str, Any], cache: dict[str, Any]) -> str:
    layer = str(meta.get('layer') or '')
    layer_payload = ((cache.get('layers') or {}).get(layer) or {}) if isinstance(cache, dict) else {}
    return step2_artifact_identity.stable_json_hash({
        'node': name,
        'layer': layer,
        'deps': meta.get('deps') or [],
        'layer_payload': layer_payload,
        'compiled_manifest_hash': cache.get('compiled_manifest_hash'),
        'semantic_config_section_hashes': cache.get('semantic_config_section_hashes'),
    })


def _score_command(day: str, action: str, tickers: list[str], workers: int) -> list[str]:
    cmd = [
        sys.executable,
        'step2_today_compiled.py',
        '--day',
        day,
        '--tickers',
        *tickers,
        '--workers',
        str(workers),
    ]
    if action == 'score_only':
        cmd.append('--skip-refresh')
    elif action == 'full_rebuild':
        cmd.append('--full-rebuild')
    elif action == 'reuse_existing_signals_refresh_outcomes_compile':
        cmd.append('--reuse-existing-signals')
    return cmd


def _commands(day: str, tickers: list[str], action: str, workers: int) -> dict[str, list[str]]:
    return {
        'market_events': [sys.executable, 'live_step2_feed.py', 'materialize', '--day', day],
        'market_data_integrity': [sys.executable, 'market_data_integrity_gate.py', day, '--source', 'auto'],
        'signal_features': _score_command(day, 'full_rebuild', tickers, workers),
        'path_outcomes': _score_command(day, 'reuse_existing_signals_refresh_outcomes_compile', tickers, workers),
        'compiled_step2': _score_command(day, action, tickers, workers),
        'step2_score': _score_command(day, 'score_only' if action == 'score_only' else action, tickers, workers),
        'live_shadow_step2': [sys.executable, 'intraday_shadow_step2.py', day],
        'shadow_variants': [sys.executable, 'shadow_variant_engine.py', 'run-day', day],
        'candidate_lifecycle': [sys.executable, 'candidate_lifecycle.py', 'sync', '--day', day],
        'deterministic_live_replay': [sys.executable, 'deterministic_live_replay.py', day],
        'architecture_drift_gate': [sys.executable, 'architecture_drift_gate.py', '--day', day],
        'live_signal_parity': [sys.executable, 'live_signal_step2_parity.py', '--day', day],
        'canonical_decision_packets': [sys.executable, 'canonical_decision_packet.py', 'diff', day],
        'parity_report': [sys.executable, 'live_step2_parity_report.py', day],
        'artifact_registry': [sys.executable, 'artifact_version_registry.py', day],
    }


def plan(day: str, tickers: list[str] | None = None, workers: int | None = None,
         compiled_name: str | None = None) -> dict[str, Any]:
    tickers = [str(t).upper() for t in (tickers or DEFAULT_TICKERS)]
    workers = worker_policy.clamp_workers(workers or worker_policy.DEFAULT_MAX_WORKERS)
    cache = step2_cache_layers.report(day, tickers, compiled_name=compiled_name)
    rebuild = step2_rebuild_planner.plan(day, tickers, compiled_name=compiled_name)
    stale_layers = set(cache.get('stale_layers') or [])
    action = rebuild.get('action') or cache.get('recommended_action') or 'score_only'
    commands = _commands(day, tickers, action, workers)
    nodes = {}
    for name, meta in NODES.items():
        layer = meta['layer']
        stale = layer in stale_layers
        required = stale or name in (
            'step2_score',
            'market_data_integrity',
            'live_shadow_step2',
            'shadow_variants',
            'candidate_lifecycle',
            'deterministic_live_replay',
            'architecture_drift_gate',
            'canonical_decision_packets',
            'parity_report',
            'artifact_registry',
        )
        if action == 'score_only' and name in ('market_events', 'signal_features', 'path_outcomes', 'compiled_step2'):
            required = False
        if action == 'reuse_existing_signals_refresh_outcomes_compile' and name in ('market_events', 'signal_features'):
            required = False
        nodes[name] = {
            'deps': list(meta['deps']),
            'layer': layer,
            'stale': stale,
            'required': bool(required),
            'command': commands.get(name),
        }
        nodes[name]['fingerprint'] = _node_fingerprint(name, nodes[name], cache)
        nodes[name]['reuse_mode'] = 'reuse_existing' if not nodes[name]['required'] else 'materialize'
    ordered = [
        name for name in (
            'market_events',
            'market_data_integrity',
            'signal_features',
            'path_outcomes',
            'compiled_step2',
            'step2_score',
            'live_shadow_step2',
            'shadow_variants',
            'candidate_lifecycle',
            'deterministic_live_replay',
            'architecture_drift_gate',
            'live_signal_parity',
            'canonical_decision_packets',
            'parity_report',
            'artifact_registry',
        )
        if nodes[name]['required']
    ]
    payload = {
        'schema_version': SCHEMA_VERSION,
        'source': 'artifact_dag_scheduler',
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'day': day,
        'tickers': tickers,
        'workers': workers,
        'recommended_action': action,
        'ordered_nodes': ordered,
        'nodes': nodes,
        'dag_hash': step2_artifact_identity.stable_json_hash({
            'day': day,
            'tickers': tickers,
            'nodes': {
                name: {
                    'deps': row.get('deps'),
                    'fingerprint': row.get('fingerprint'),
                    'required': row.get('required'),
                }
                for name, row in nodes.items()
            },
            'recommended_action': action,
        }),
        'cache_layers': {
            'recommended_action': cache.get('recommended_action'),
            'stale_layers': cache.get('stale_layers'),
            'scoring_stale_layers': cache.get('scoring_stale_layers'),
            'semantic_config_section_hashes': cache.get('semantic_config_section_hashes'),
        },
        'rebuild_plan': {
            'action': rebuild.get('action'),
            'reason': rebuild.get('reason'),
            'decision_start_ts': rebuild.get('decision_start_ts'),
            'reuse_existing_signals': rebuild.get('reuse_existing_signals'),
            'full_rebuild': rebuild.get('full_rebuild'),
            'append_existing': rebuild.get('append_existing'),
            'score_only': rebuild.get('score_only'),
            'path': rebuild.get('path'),
        },
        'deduction': (
            'This is a planner, not a lossy shortcut. It chooses the narrowest exact artifact path, '
            'and leaves execution to automation_ops/step2_today_compiled.'
        ),
    }
    path = os.path.join(OUT_DIR, f'artifact_dag_{day}.json')
    payload['path'] = _write_json(path, payload)
    return payload


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser(description='Plan exact Step 2/Live artifact DAG actions.')
    ap.add_argument('day')
    ap.add_argument('--tickers', nargs='+', default=DEFAULT_TICKERS)
    ap.add_argument('--workers', type=int, default=worker_policy.DEFAULT_MAX_WORKERS)
    ap.add_argument('--compiled-name', default='')
    ap.add_argument('--json', action='store_true')
    args = ap.parse_args()
    payload = plan(args.day, args.tickers, workers=args.workers, compiled_name=args.compiled_name or None)
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
