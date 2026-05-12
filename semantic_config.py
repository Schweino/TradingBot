"""Semantic configuration hashing for Step 2/Live parity artifacts.

Raw ``trading_config.json`` hashes are too coarse for the replay stack: a scoring
profile promotion should not force a full market/signal/outcome rebuild, while
execution or bracket changes absolutely should. This module gives each cache
layer a precise config fingerprint to compare.
"""
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

import execution_kernel
import step2_execution_contract
import step2_latency_model
import step2_parity_contract
import tournament_safety


HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(HERE, 'trading_config.json')
CT = ZoneInfo('America/Chicago')
SCHEMA_VERSION = 1

COMPILED_TAPE_SECTIONS = (
    'signal_generation',
    'path_outcomes',
    'compiled_execution',
    'market_data_contract',
)

LAYER_SECTIONS = {
    'market_events': ('market_data_contract',),
    'signal_features': ('signal_generation', 'market_data_contract'),
    'path_outcomes': ('path_outcomes', 'compiled_execution', 'market_data_contract'),
    'compiled_step2': ('compiled_execution', 'market_data_contract'),
    'live_signal_parity': (
        'active_scoring_profile',
        'compiled_execution',
        'path_outcomes',
        'live_runtime',
        'market_data_contract',
    ),
}


def _read_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, 'r', encoding='utf-8-sig') as f:
            return json.load(f)
    except Exception:
        return default


def _stable_hash(payload: Any) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(',', ':'), default=str).encode('utf-8')
    return hashlib.sha256(blob).hexdigest()


def _smart_without_profile(config: dict[str, Any]) -> dict[str, Any]:
    smart = dict(config.get('smart_entry') or {})
    smart.pop('active_scoring_profile', None)
    return smart


def _active_profile(config: dict[str, Any]) -> dict[str, Any]:
    profile = ((config.get('smart_entry') or {}).get('active_scoring_profile') or {})
    weights = profile.get('weights') if isinstance(profile.get('weights'), dict) else {}
    return {
        'enabled': bool(profile.get('enabled')),
        'name': profile.get('name'),
        'bias': round(float(profile.get('bias') or 0.0), 8),
        'weights': {
            str(k): round(float(v), 8)
            for k, v in sorted((weights or {}).items())
            if abs(float(v or 0.0)) > 1e-12
        },
    }


def sections(config: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = config or _read_json(CONFIG_PATH, {}) or {}
    parity_contract = step2_parity_contract.contract(cfg)
    execution_contract = step2_execution_contract.execution_contract(cfg)
    kernel_contract = execution_kernel.contract_from_config(cfg)
    active_profile = _active_profile(cfg)
    ticker_cfg = cfg.get('ticker_cfg') or {}
    path_outcomes = {
        'tickers': cfg.get('tickers'),
        'ticker_cfg': ticker_cfg,
        'trade_size_pct': cfg.get('trade_size_pct'),
        'min_balance': cfg.get('min_balance'),
        'transaction_costs': cfg.get('transaction_costs'),
        'latency_model_hash': tournament_safety._file_sha256(step2_latency_model.DEFAULT_MODEL_PATH),
        'bracket_rounding_policy': execution_contract.get('bracket_rounding_policy'),
    }
    signal_generation = {
        'tickers': cfg.get('tickers'),
        'btc_beta': cfg.get('btc_beta'),
        'btc_proxy_symbols': cfg.get('btc_proxy_symbols'),
        'min_conviction': cfg.get('min_conviction'),
        'rvol_lookback': cfg.get('rvol_lookback'),
        'rvol_min': cfg.get('rvol_min'),
        'max_entry_spread_pct': cfg.get('max_entry_spread_pct'),
        'quote_stale_sec': cfg.get('quote_stale_sec'),
        'market_microstructure': cfg.get('market_microstructure'),
        'smart_entry_without_active_profile': _smart_without_profile(cfg),
        'wash_sale_blackouts': cfg.get('wash_sale_blackouts'),
    }
    compiled_execution = {
        'step2_parity_contract_hash': step2_parity_contract.contract_hash(parity_contract),
        'step2_parity_contract': parity_contract,
        'step2_execution_contract_hash': step2_execution_contract.execution_contract_hash(cfg),
        'step2_execution_contract': execution_contract,
        'execution_kernel_hash': kernel_contract.get('execution_kernel_hash'),
        'execution_kernel_contract': kernel_contract,
        'session': cfg.get('session'),
        'conditional_stop_min': cfg.get('conditional_stop_min'),
        'entry_fill_timeout_sec': cfg.get('entry_fill_timeout_sec'),
    }
    live_runtime = {
        'execution_mode': cfg.get('execution_mode'),
        'execution_mode_promoted_at': cfg.get('execution_mode_promoted_at'),
        'execution_mode_promotion_reason': cfg.get('execution_mode_promotion_reason'),
        'broker_api_degraded_threshold': cfg.get('broker_api_degraded_threshold'),
        'health_heartbeat_enabled': cfg.get('health_heartbeat_enabled'),
        'health_heartbeat_sec': cfg.get('health_heartbeat_sec'),
        'retention': cfg.get('retention'),
        'adaptive_management': cfg.get('adaptive_management'),
    }
    market_data_contract = {
        'feed': 'sip',
        'quote_mode': 'per-second',
        'btc_mode': 'bars',
        'indicator_mode': 'live',
        'tickers': cfg.get('tickers'),
        'step2_feed_module': tournament_safety._file_sha256(os.path.join(HERE, 'live_step2_feed.py')),
        'incremental_market_store_module': tournament_safety._file_sha256(
            os.path.join(HERE, 'incremental_market_store.py')
        ),
    }
    return {
        'active_scoring_profile': active_profile,
        'signal_generation': signal_generation,
        'path_outcomes': path_outcomes,
        'compiled_execution': compiled_execution,
        'live_runtime': live_runtime,
        'market_data_contract': market_data_contract,
        'full_config': cfg,
    }


def section_hashes(config: dict[str, Any] | None = None) -> dict[str, str]:
    return {name: _stable_hash(payload) for name, payload in sections(config).items()}


def selected_hashes(section_names: list[str] | tuple[str, ...],
                    config: dict[str, Any] | None = None) -> dict[str, str]:
    all_hashes = section_hashes(config)
    return {name: all_hashes.get(name) for name in section_names}


def combined_hash(section_names: list[str] | tuple[str, ...],
                  config: dict[str, Any] | None = None) -> str:
    return _stable_hash(selected_hashes(section_names, config))


def report(config: dict[str, Any] | None = None, include_payloads: bool = False) -> dict[str, Any]:
    cfg = config or _read_json(CONFIG_PATH, {}) or {}
    payloads = sections(cfg)
    hashes = {name: _stable_hash(value) for name, value in payloads.items()}
    out = {
        'schema_version': SCHEMA_VERSION,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'config_path': os.path.abspath(CONFIG_PATH),
        'config_sha256': tournament_safety._file_sha256(CONFIG_PATH),
        'section_hashes': hashes,
        'compiled_tape_sections': list(COMPILED_TAPE_SECTIONS),
        'compiled_tape_semantic_hash': combined_hash(COMPILED_TAPE_SECTIONS, cfg),
        'layer_sections': {k: list(v) for k, v in LAYER_SECTIONS.items()},
    }
    if include_payloads:
        out['sections'] = payloads
    return out


def mismatches(recorded: dict[str, Any] | None,
               section_names: list[str] | tuple[str, ...],
               config: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    recorded = recorded or {}
    current = selected_hashes(section_names, config)
    out = []
    for name in section_names:
        expected = recorded.get(name)
        actual = current.get(name)
        if expected is None:
            out.append({'section': name, 'reason': 'semantic_hash_not_recorded', 'actual': actual})
        elif expected != actual:
            out.append({'section': name, 'reason': 'semantic_hash_mismatch', 'expected': expected, 'actual': actual})
    return out


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser(description='Print semantic trading-config hashes.')
    ap.add_argument('--include-payloads', action='store_true')
    args = ap.parse_args()
    print(json.dumps(report(include_payloads=args.include_payloads), indent=2, sort_keys=True, default=str))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
