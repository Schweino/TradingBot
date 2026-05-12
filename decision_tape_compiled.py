"""Compiled decision-tape cache and simulator for Step 2 research.

This is an optional accelerator. It is deliberately narrow: when a requested
gate cannot be represented by the compiled arrays, callers should use the
existing Python simulator. The math mirrors the decision-tape rules: conviction
filter, optional compatible long-entry gate, per-ticker open-position blocking,
setup/side cooldown, compounding balance, wins/losses, and P/L.
"""
from __future__ import annotations

from output_paths import output_path

from collections import Counter
from typing import Any

import argparse
import gzip
import json
import os
import re
import time
from datetime import datetime

import numpy as np

import backtest_30d_engine as replay
import compiled_tape_lineage
import decision_tape_gates
import compiled_chunk_store
import routed_scoring_profile
import scoring_variant_lab_fast as fast
import scoring_variant_lab_massive as massive
import semantic_config
import step2_artifact_identity
import step2_execution_contract
import step2_quote_aware_guard
import tournament_safety
import ws_scalp

try:  # pragma: no cover - optional local package
    from numba import njit, prange
except Exception:  # pragma: no cover
    njit = None
    prange = range
try:  # pragma: no cover
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None


COMPILED_AVAILABLE = njit is not None
SIDE_LONG = 1
SIDE_SKIP = 0
SIDE_SHORT = -1
HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_TAPE_DIR = output_path('postmortem', 'backtests', 'decision_tapes')
DEFAULT_OUT_DIR = output_path('postmortem', 'backtests', 'compiled_decision_tapes')
CODE_HASH_INPUTS = list(compiled_tape_lineage.DEFAULT_COMPILED_TAPE_CODE_HASH_INPUTS)


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ''):
            return default
        return float(value)
    except Exception:
        return default


def _gate_features(row: dict) -> dict:
    features = dict(row.get('gate_features') or {})
    features.setdefault('ticker_session_return_pct', row.get('ticker_session_return_pct'))
    features.setdefault('vwap_dist', row.get('vwap_dist'))
    features.setdefault('btc_regime', row.get('btc_regime'))
    return features


def _gate_mode_code(gate: dict | None) -> int:
    if not gate or str(gate.get('mode') or 'off') == 'off':
        return 0
    mode = str(gate.get('mode') or gate.get('name') or '').lower()
    if mode == 'ticker-negative':
        return 1
    if mode == 'ticker-negative-below-vwap':
        return 2
    if mode == 'ticker-negative-btc-not-bull':
        return 3
    if mode == 'ticker-negative-below-vwap-btc-not-bull':
        return 4
    return -1


def compatible_gate(gate: dict | None) -> bool:
    return _gate_mode_code(gate) >= 0


def _compiled_indicator_mode(rows: list[dict]) -> str:
    modes = {str(row.get('indicator_mode') or '') for row in rows if row.get('indicator_mode')}
    if len(modes) == 1:
        return next(iter(modes))
    if len(modes) > 1:
        return 'mixed'
    return 'unknown'


REASON_CODE = {
    '': 0,
    'end_of_data': 1,
    'session_end': 2,
    'cond_time_stop': 3,
    'stop_loss': 4,
    'take_profit': 5,
    'brs_failed_followthrough': 6,
    'path_failure': 7,
    'path_failure_flip': 8,
    'giveback_exit': 9,
    'giveback_exit_flip': 10,
}


SOURCE_DECISION_CODE = {
    '': 0,
    'raw_signal': 0,
    'detected': 0,
    'accepted': 1,
    'rejected': 2,
}


def _reason_code(reason: Any) -> int:
    return int(REASON_CODE.get(str(reason or ''), 0))


def _source_decision_code(value: Any) -> int:
    return int(SOURCE_DECISION_CODE.get(str(value or '').lower(), 0))


def _live_long_gate_mode() -> tuple[int, float]:
    gate = getattr(ws_scalp, 'LONG_ENTRY_QUALITY_GATE', {}) or {}
    if not isinstance(gate, dict) or not gate.get('enabled'):
        return 0, 0.0
    name = str(gate.get('name') or '').lower()
    threshold = float(gate.get('ticker_session_return_below_pct', -0.5))
    if name == 'ticker_negative_below_vwap_btc_not_bull':
        return 4, threshold
    return 0, threshold


def _short_recovery_blocked(row: dict, gate_feats: dict) -> int:
    guard = getattr(ws_scalp, 'SHORT_RECOVERY_GUARD', {}) or {}
    if not isinstance(guard, dict) or not guard.get('enabled'):
        return 0
    model = row.get('model_features') or {}
    btc_regime = str(gate_feats.get('btc_regime') or '').lower()
    btc_stack = str(gate_feats.get('btc_ema_stack') or gate_feats.get('btc_stack') or '').lower()
    btc_m15 = _num(gate_feats.get('btc_mom_15s'), 0.0)
    btc_m60 = _num(gate_feats.get('btc_mom_60s'), 0.0)
    btc_bearish = (
        btc_regime in ('bear', 'bear_momentum', 'impulse_down')
        or (btc_stack == 'bear' and btc_m15 <= 0.0 and btc_m60 <= 0.0)
    )
    if guard.get('require_btc_not_bearish', True) and btc_bearish:
        return 0
    flags = 0
    if str(gate_feats.get('ema_stack') or '').lower() == 'bull' or _num(model.get('ema'), 0.0) > 0.0:
        flags += 1
    if _num(gate_feats.get('mom_15s'), 0.0) > 0.0 and _num(gate_feats.get('mom_60s'), 0.0) >= 0.0:
        flags += 1
    if _num(gate_feats.get('vwap_dist'), 0.0) > 0.0 or _num(model.get('vwap'), 0.0) > 0.0:
        flags += 1
    if _num(gate_feats.get('session_range_pos'), 0.0) >= float(guard.get('min_session_range_pos', 0.25)):
        flags += 1
    btc_not_bearish = (
        btc_regime in ('bull', 'bull_momentum', 'impulse_up')
        or (btc_stack == 'bull' and btc_m15 >= 0.0 and btc_m60 >= 0.0)
    )
    if btc_not_bearish:
        flags += 1
    return 1 if flags >= int(guard.get('min_bullish_flags', 4)) else 0


def compile_rows(rows_by_day: dict[str, list[dict]], tickers: list[str], indicator_mode: str | None = None) -> dict:
    ticker_map = {ticker: idx for idx, ticker in enumerate(sorted(set(tickers)))}
    setup_names = sorted({
        str(row.get('setup_type') or 'unknown')
        for rows in rows_by_day.values()
        for row in rows
    })
    setup_map = {name: idx for idx, name in enumerate(setup_names)}
    cooldown = np.zeros(len(setup_map), dtype=np.float64)
    for name, idx in setup_map.items():
        cooldown[idx] = float(replay.ws_scalp.SETUP_COOLDOWN_SEC.get(name, 30))

    flat_rows = []
    for day in sorted(rows_by_day):
        flat_rows.extend(sorted(rows_by_day.get(day, []), key=lambda r: (int(r.get('ts') or 0), r.get('ticker') or '')))

    n = len(flat_rows)
    features = np.zeros((n, len(fast.FEATURE_NAMES)), dtype=np.float64)
    ts = np.zeros(n, dtype=np.int64)
    ticker_code = np.zeros(n, dtype=np.int16)
    setup_code = np.zeros(n, dtype=np.int16)
    original_side = np.zeros(n, dtype=np.int8)
    source_decision_code = np.zeros(n, dtype=np.int8)
    conviction_ok = np.zeros(n, dtype=np.int8)
    conviction_high = np.zeros(n, dtype=np.int8)
    long_pnl_pct = np.zeros(n, dtype=np.float64)
    short_pnl_pct = np.zeros(n, dtype=np.float64)
    long_held = np.zeros(n, dtype=np.int64)
    short_held = np.zeros(n, dtype=np.int64)
    long_reason_code = np.zeros(n, dtype=np.int16)
    short_reason_code = np.zeros(n, dtype=np.int16)
    ticker_ret = np.zeros(n, dtype=np.float64)
    vwap_dist = np.zeros(n, dtype=np.float64)
    vwap_dist_sigma = np.zeros(n, dtype=np.float64)
    btc_bullish = np.zeros(n, dtype=np.int8)
    btc_mom_60 = np.full(n, np.nan, dtype=np.float64)
    exec_score = np.zeros(n, dtype=np.float64)
    flow_fade_confirmed = np.zeros(n, dtype=np.int8)
    short_recovery_blocked = np.zeros(n, dtype=np.int8)
    day_values = []
    for day, rows in sorted(rows_by_day.items()):
        if rows:
            day_values.append(day)
    day_map = {day: idx for idx, day in enumerate(day_values)}
    day_code = np.zeros(n, dtype=np.int16)

    row_day = []
    for day in sorted(rows_by_day):
        row_day.extend([day] * len(rows_by_day.get(day, [])))
    for i, row in enumerate(flat_rows):
        feats = row.get('model_features') or {}
        for j, name in enumerate(fast.FEATURE_NAMES):
            features[i, j] = _num(feats.get(name))
        ts[i] = int(row.get('ts') or 0)
        ticker = str(row.get('ticker') or '').upper()
        ticker_code[i] = ticker_map.get(ticker, 0)
        setup = str(row.get('setup_type') or 'unknown')
        setup_code[i] = setup_map.get(setup, 0)
        original_side[i] = SIDE_LONG if str(row.get('original_side')).upper() == 'LONG' else SIDE_SHORT
        source_decision_code[i] = _source_decision_code(row.get('source_decision'))
        conviction_ok[i] = 1 if row.get('conviction') in replay.MIN_CONVICTION else 0
        conviction_high[i] = 1 if row.get('conviction') == 'HIGH' else 0
        outcomes = row.get('outcomes') or {}
        long = outcomes.get('LONG') or {}
        short = outcomes.get('SHORT') or {}
        long_pnl_pct[i] = _num(long.get('pnl_pct'))
        short_pnl_pct[i] = _num(short.get('pnl_pct'))
        long_held[i] = int(long.get('held_sec') or 0)
        short_held[i] = int(short.get('held_sec') or 0)
        long_reason_code[i] = _reason_code(long.get('reason'))
        short_reason_code[i] = _reason_code(short.get('reason'))
        gate_feats = _gate_features(row)
        exec_score[i] = _num(row.get('exec_score'), _num(gate_feats.get('execution_score'), 0.0))
        ticker_ret[i] = _num(gate_feats.get('ticker_session_return_pct'), 999.0)
        vwap_dist[i] = _num(gate_feats.get('vwap_dist'), 0.0)
        vwap_dist_sigma[i] = _num(gate_feats.get('vwap_dist_sigma'), 999.0)
        btc_mom_60[i] = _num(gate_feats.get('btc_mom_60s'), np.nan)
        flow_fade_confirmed[i] = 1 if gate_feats.get('flow_fade_confirmed') in (True, 1, 'true', 'True') else 0
        short_recovery_blocked[i] = _short_recovery_blocked(row, gate_feats)
        btc_regime = str(gate_feats.get('btc_regime') or '').lower()
        btc_bullish[i] = 1 if btc_regime in decision_tape_gates.BULLISH_BTC_REGIMES else 0
        day_code[i] = day_map.get(row_day[i], 0) if i < len(row_day) else 0

    return {
        'features': features,
        'ts': ts,
        'ticker_code': ticker_code,
        'setup_code': setup_code,
        'day_code': day_code,
        'original_side': original_side,
        'source_decision_code': source_decision_code,
        'conviction_ok': conviction_ok,
        'conviction_high': conviction_high,
        'long_pnl_pct': long_pnl_pct,
        'short_pnl_pct': short_pnl_pct,
        'long_held': long_held,
        'short_held': short_held,
        'long_reason_code': long_reason_code,
        'short_reason_code': short_reason_code,
        'ticker_session_return_pct': ticker_ret,
        'vwap_dist': vwap_dist,
        'vwap_dist_sigma': vwap_dist_sigma,
        'btc_bullish': btc_bullish,
        'btc_mom_60': btc_mom_60,
        'exec_score': exec_score,
        'flow_fade_confirmed': flow_fade_confirmed,
        'short_recovery_blocked': short_recovery_blocked,
        'cooldown_by_setup': cooldown,
        'ticker_count': max(1, len(ticker_map)),
        'setup_count': max(1, len(setup_map)),
        'day_count': max(1, len(day_map)),
        'rows': n,
        'indicator_mode': indicator_mode or _compiled_indicator_mode(flat_rows),
        'ticker_map': ticker_map,
        'setup_map': setup_map,
        'day_map': day_map,
        'live_long_gate_mode': _live_long_gate_mode()[0],
        'live_long_gate_threshold': _live_long_gate_mode()[1],
    }


def _read_tape(path: str) -> list[dict]:
    rows = []
    if not os.path.exists(path):
        return rows
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _tape_path(args, day: str) -> str:
    tickers = '-'.join(args.tickers)
    suffix = f'_{args.tape_profile_name}' if args.tape_profile_name else ''
    indicator_mode = getattr(args, 'indicator_mode', 'live') or 'live'
    return os.path.join(
        args.tape_dir,
        f'decision_tape_{args.feed}_{args.quote_mode}_{args.btc_mode}_{indicator_mode}_{tickers}{suffix}_{day}.jsonl.gz',
    )


def _compiled_array_payload(compiled: dict) -> dict[str, Any]:
    return {
        'features': compiled['features'],
        'ts': compiled['ts'],
        'ticker_code': compiled['ticker_code'],
        'setup_code': compiled['setup_code'],
        'day_code': compiled['day_code'],
        'original_side': compiled['original_side'],
        'source_decision_code': compiled['source_decision_code'],
        'conviction_ok': compiled['conviction_ok'],
        'conviction_high': compiled['conviction_high'],
        'long_pnl_pct': compiled['long_pnl_pct'],
        'short_pnl_pct': compiled['short_pnl_pct'],
        'long_held': compiled['long_held'],
        'short_held': compiled['short_held'],
        'long_reason_code': compiled['long_reason_code'],
        'short_reason_code': compiled['short_reason_code'],
        'ticker_session_return_pct': compiled['ticker_session_return_pct'],
        'vwap_dist': compiled['vwap_dist'],
        'vwap_dist_sigma': compiled['vwap_dist_sigma'],
        'btc_bullish': compiled['btc_bullish'],
        'btc_mom_60': compiled['btc_mom_60'],
        'exec_score': compiled['exec_score'],
        'flow_fade_confirmed': compiled['flow_fade_confirmed'],
        'short_recovery_blocked': compiled['short_recovery_blocked'],
        'cooldown_by_setup': compiled['cooldown_by_setup'],
    }


def _source_identities(source_paths: list[str]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for path in source_paths:
        identity = step2_artifact_identity.jsonl_gz_identity(path)
        if identity.get('exists') and not identity.get('sidecar_used'):
            step2_artifact_identity.write_identity_sidecar(path, identity)
        out[os.path.abspath(path)] = identity
    return out


def _source_semantic_hashes(source_identity: dict[str, dict[str, Any]]) -> dict[str, str | None]:
    return {
        path: row.get('semantic_sha256')
        for path, row in source_identity.items()
    }


def _source_physical_hashes(source_identity: dict[str, dict[str, Any]]) -> dict[str, str | None]:
    return {
        path: row.get('physical_sha256')
        for path, row in source_identity.items()
    }


def _source_exit_replay_models(source_paths: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    pattern = re.compile(
        r"^decision_tape_(?P<feed>[^_]+)_(?P<quote>[^_]+)_(?P<btc>[^_]+)_(?P<indicator>[^_]+)_"
        r"(?P<tickers>.+)_(?P<day>\d{4}-\d{2}-\d{2})\.jsonl\.gz$"
    )
    artifact_root = output_path("postmortem", "backtests", "replay_artifacts", "manifests")
    for path in source_paths:
        model = ""
        try:
            with gzip.open(path, "rt", encoding="utf-8") as f:
                line = f.readline().strip()
                if line:
                    row = json.loads(line)
                    refresh = row.get("outcome_refresh") if isinstance(row.get("outcome_refresh"), dict) else {}
                    model = str(refresh.get("exit_replay_model") or "")
        except Exception:
            model = ""
        if not model:
            match = pattern.match(os.path.basename(path))
            if match:
                key = f"{match.group('feed')}_{match.group('quote')}_{match.group('btc')}_{match.group('tickers')}"
                manifest_path = os.path.join(artifact_root, key, f"{match.group('day')}.manifest.json")
                try:
                    with open(manifest_path, "r", encoding="utf-8-sig") as f:
                        manifest = json.load(f)
                    model = str(manifest.get("exit_replay_model") or "")
                except Exception:
                    model = ""
        out[os.path.abspath(path)] = model
    return out


def save_compiled(compiled: dict, out_dir: str, name: str, source_paths: list[str]) -> dict:
    os.makedirs(out_dir, exist_ok=True)
    safe = ''.join(ch if ch.isalnum() or ch in '._-' else '_' for ch in name).strip('._') or 'compiled_decision_tape'
    store_dir = os.path.join(out_dir, safe)
    os.makedirs(store_dir, exist_ok=True)
    arrays_path = os.path.join(store_dir, 'arrays.npz')
    array_payload = _compiled_array_payload(compiled)
    arrays_identity = step2_artifact_identity.write_deterministic_npz(arrays_path, array_payload)
    cfg = getattr(replay, 'TRADING_CONFIG', {}) or {}
    semantic = semantic_config.report(cfg)
    source_identity = _source_identities(source_paths)
    source_physical_hashes = _source_physical_hashes(source_identity)
    source_semantic_hashes = _source_semantic_hashes(source_identity)
    source_exit_replay_models = _source_exit_replay_models(source_paths)
    exit_models = {value for value in source_exit_replay_models.values() if value}
    exit_replay_model = next(iter(exit_models)) if len(exit_models) == 1 else ""
    manifest = {
        'schema_version': 2,
        'created_at_epoch': int(time.time()),
        'name': name,
        'indicator_mode': str(compiled.get('indicator_mode') or 'unknown'),
        'store_dir': store_dir,
        'arrays_path': arrays_path,
        'rows': int(compiled['rows']),
        'min_ts': int(np.min(compiled['ts'])) if int(compiled['rows']) else 0,
        'max_ts': int(np.max(compiled['ts'])) if int(compiled['rows']) else 0,
        'ticker_count': int(compiled['ticker_count']),
        'setup_count': int(compiled['setup_count']),
        'day_count': int(compiled['day_count']),
        'ticker_map': compiled['ticker_map'],
        'setup_map': compiled['setup_map'],
        'day_map': compiled['day_map'],
        'live_long_gate_mode': int(compiled.get('live_long_gate_mode') or 0),
        'live_long_gate_threshold': float(compiled.get('live_long_gate_threshold') or 0.0),
        'source_paths': [os.path.abspath(path) for path in source_paths],
        'source_hashes': source_physical_hashes,
        'source_physical_hashes': source_physical_hashes,
        'source_semantic_hashes': source_semantic_hashes,
        'source_identities': source_identity,
        'source_exit_replay_models': source_exit_replay_models,
        'exit_replay_model': exit_replay_model,
        'required_exit_replay_model': step2_quote_aware_guard.REQUIRED_EXIT_REPLAY_MODEL,
        'source_mtimes': {
            os.path.abspath(path): os.path.getmtime(path) if os.path.exists(path) else None
            for path in source_paths
        },
        'feature_names': list(fast.FEATURE_NAMES),
        'feature_count': len(fast.FEATURE_NAMES),
        'code_hashes': {
            path: tournament_safety._file_sha256(path)
            for path in CODE_HASH_INPUTS
        },
        'lineage_registry': compiled_tape_lineage.registry_for(CODE_HASH_INPUTS),
        'numba_available': bool(COMPILED_AVAILABLE),
        'step2_execution_contract_hash': step2_execution_contract.execution_contract_hash(cfg),
        'step2_execution_contract': step2_execution_contract.execution_contract(cfg),
        'semantic_config_section_hashes': semantic.get('section_hashes'),
        'compiled_tape_semantic_hash': semantic.get('compiled_tape_semantic_hash'),
        'compiled_tape_semantic_sections': list(semantic_config.COMPILED_TAPE_SECTIONS),
        'arrays_sha256': arrays_identity.get('physical_sha256'),
        'array_payload_sha256': arrays_identity.get('array_payload_sha256'),
        'arrays_identity': arrays_identity,
    }
    manifest['quote_aware_outcome_gate'] = step2_quote_aware_guard.manifest_gate(manifest)
    manifest['compiled_tape_hash'] = tournament_safety.stable_json_hash({
        'schema_version': manifest['schema_version'],
        'source_semantic_hashes': manifest['source_semantic_hashes'],
        'code_hashes': manifest['code_hashes'],
        'lineage_registry_hash': (manifest.get('lineage_registry') or {}).get('registry_hash'),
        'compiled_tape_semantic_hash': manifest['compiled_tape_semantic_hash'],
        'exit_replay_model': manifest.get('exit_replay_model'),
        'required_exit_replay_model': manifest.get('required_exit_replay_model'),
        'feature_names': manifest['feature_names'],
        'rows': manifest['rows'],
        'max_ts': manifest['max_ts'],
        'ticker_map': manifest['ticker_map'],
        'setup_map': manifest['setup_map'],
        'day_map': manifest['day_map'],
        'array_payload_sha256': manifest['array_payload_sha256'],
    }, 32)
    manifest_path = os.path.join(store_dir, 'manifest.json')
    try:
        manifest['chunk_store'] = compiled_chunk_store.write_chunks(compiled, store_dir)
    except Exception as exc:
        manifest['chunk_store'] = {'error': repr(exc)}
    try:
        manifest['day_shards'] = compiled_chunk_store.write_day_shards(compiled, store_dir, source_identity)
    except Exception as exc:
        manifest['day_shards'] = {'error': repr(exc)}
    step2_artifact_identity.write_json(manifest_path, manifest)
    manifest['manifest_path'] = manifest_path
    return manifest


def _validate_source_hashes(manifest: dict) -> dict:
    semantic_stale = semantic_config.mismatches(
        manifest.get('semantic_config_section_hashes') or {},
        manifest.get('compiled_tape_semantic_sections') or semantic_config.COMPILED_TAPE_SECTIONS,
        getattr(replay, 'TRADING_CONFIG', {}) or {},
    )
    lineage = compiled_tape_lineage.evaluate_manifest(
        manifest,
        code_hash_inputs=CODE_HASH_INPUTS,
        semantic_mismatches=semantic_stale,
    )
    if lineage.get('rebuild_required'):
        raise compiled_tape_lineage.CompiledTapeLineageError(lineage)
    return lineage


def _today_ct() -> str:
    if ZoneInfo is None:
        return datetime.now().date().isoformat()
    return datetime.now(ZoneInfo("America/Chicago")).date().isoformat()


def _verify_shard_sources(shard: dict) -> tuple[bool, list[dict]]:
    issues = []
    for path, identity in (shard.get('source_identities') or {}).items():
        if not isinstance(identity, dict):
            continue
        expected = identity.get('semantic_sha256')
        if not expected:
            continue
        if not os.path.exists(path):
            issues.append({'path': path, 'reason': 'source_missing'})
            continue
        try:
            current = step2_artifact_identity.jsonl_gz_identity(path)
            actual = current.get('semantic_sha256')
        except Exception as exc:
            issues.append({'path': path, 'reason': 'source_identity_error', 'error': repr(exc)})
            continue
        if actual != expected:
            issues.append({
                'path': path,
                'reason': 'source_semantic_hash_mismatch',
                'expected': expected,
                'actual': actual,
            })
    return not issues, issues


def _historical_day_shard_lineage(manifest: dict, unsafe_lineage: dict, day_subset=None,
                                  historical_cutoff_day: str | None = None) -> dict:
    cutoff = str(historical_cutoff_day or _today_ct())
    wanted = {str(day) for day in day_subset} if day_subset else None
    shards = list(((manifest.get('day_shards') or {}).get('shards') or []))
    allowed_days = []
    excluded_days = []
    shard_issues = []
    for shard in shards:
        day = str(shard.get('day') or '')
        if wanted is not None and day not in wanted:
            continue
        if not day or day >= cutoff:
            excluded_days.append({'day': day, 'reason': 'not_before_historical_cutoff'})
            continue
        if not shard.get('array_payload_sha256'):
            excluded_days.append({'day': day, 'reason': 'missing_array_payload_sha256'})
            continue
        ok, issues = _verify_shard_sources(shard)
        if not ok:
            excluded_days.append({'day': day, 'reason': 'source_identity_mismatch'})
            shard_issues.extend({'day': day, **issue} for issue in issues)
            continue
        allowed_days.append(day)
    if not allowed_days:
        return {
            'schema_version': 1,
            'source': 'decision_tape_compiled',
            'status': 'NO_HISTORICAL_DAY_SHARDS_AVAILABLE',
            'certified': False,
            'rebuild_required': True,
            'quick_score_allowed': False,
            'parent_lineage': unsafe_lineage,
            'historical_cutoff_day': cutoff,
            'allowed_days': [],
            'excluded_days': excluded_days,
            'shard_issues': shard_issues,
        }
    return {
        'schema_version': 1,
        'source': 'decision_tape_compiled',
        'status': 'PARTIAL_HISTORICAL_DAY_SHARD_SCORE_ALLOWED',
        'certified': False,
        'rebuild_required': False,
        'quick_score_allowed': True,
        'historical_day_shards_only': True,
        'historical_cutoff_day': cutoff,
        'allowed_days': allowed_days,
        'excluded_days': excluded_days,
        'shard_issues': shard_issues,
        'parent_status': unsafe_lineage.get('status'),
        'parent_rebuild_required': unsafe_lineage.get('rebuild_required'),
        'parent_lineage': unsafe_lineage,
        'deduction': (
            'Top-level lineage drift is unsafe for certified full-range scoring, but selected day shards '
            'are before the historical cutoff and their source semantic identities still match. These '
            'shards can be used for score-only historical research without rebuilding or including the '
            'current intraday day.'
        ),
    }


def _load_day_shard_arrays(manifest: dict, mmap: bool = True, day_subset: list[str] | tuple[str, ...] | set[str] | None = None) -> dict:
    day_shards = manifest.get('day_shards') if isinstance(manifest.get('day_shards'), dict) else {}
    shards = list(day_shards.get('shards') or [])
    if not shards:
        raise ValueError('compiled manifest has no day shards to load')
    requested_day_subset = None
    if day_subset:
        wanted = {str(day) for day in day_subset}
        requested_day_subset = sorted(wanted)
        shards = [row for row in shards if str(row.get('day') or '') in wanted]
        if not shards:
            raise ValueError(f'compiled manifest has no selected day shards: {sorted(wanted)}')
    shards.sort(key=lambda row: str(row.get('day') or ''))
    arrays_by_name: dict[str, list[np.ndarray]] = {name: [] for name in compiled_chunk_store.ARRAY_NAMES}
    cooldown = None
    day_map = {str(row.get('day')): idx for idx, row in enumerate(shards)}
    for day_idx, shard in enumerate(shards):
        arrays_path = str(shard.get('path') or '')
        if not arrays_path:
            raise ValueError(f"day shard missing arrays path: {shard}")
        loaded = np.load(arrays_path, mmap_mode='r' if mmap else None)
        rows = int(shard.get('rows') or (loaded['ts'].shape[0] if 'ts' in loaded.files else 0))
        for name in compiled_chunk_store.ARRAY_NAMES:
            if name == 'day_code':
                arrays_by_name[name].append(np.full(rows, day_idx, dtype=np.int16))
            elif name in loaded.files:
                arrays_by_name[name].append(np.asarray(loaded[name]))
        if cooldown is None and 'cooldown_by_setup' in loaded.files:
            cooldown = np.asarray(loaded['cooldown_by_setup'])
    out: dict[str, Any] = {}
    for name, values in arrays_by_name.items():
        if values:
            out[name] = np.concatenate(values, axis=0)
    if cooldown is None:
        cooldown = np.zeros(int(manifest.get('setup_count') or 0), dtype=np.float64)
    out['cooldown_by_setup'] = cooldown
    out['rows'] = int(len(out.get('ts', [])))
    out['min_ts'] = int(np.min(out['ts'])) if out.get('rows') and 'ts' in out else int(manifest.get('min_ts') or 0)
    out['max_ts'] = int(np.max(out['ts'])) if out.get('rows') and 'ts' in out else int(manifest.get('max_ts') or 0)
    out['ticker_count'] = int(manifest.get('ticker_count') or 0)
    out['setup_count'] = int(manifest.get('setup_count') or 0)
    out['day_count'] = int(manifest.get('day_count') or len(day_map) or 1)
    out['ticker_map'] = manifest.get('ticker_map') or {}
    out['setup_map'] = manifest.get('setup_map') or {}
    out['day_map'] = day_map
    out['live_long_gate_mode'] = int(manifest.get('live_long_gate_mode') or _live_long_gate_mode()[0])
    out['live_long_gate_threshold'] = float(manifest.get('live_long_gate_threshold') or _live_long_gate_mode()[1])
    out_manifest = dict(
        manifest,
        rows=out['rows'],
        min_ts=out['min_ts'],
        max_ts=out['max_ts'],
        day_count=out['day_count'],
        day_map=out['day_map'],
    )
    if requested_day_subset:
        out_manifest['day_subset'] = requested_day_subset
    else:
        out_manifest.pop('day_subset', None)
    out['manifest'] = out_manifest
    out['loaded_from_day_shards'] = True
    return out


def _fill_missing_arrays(out: dict, rows: int) -> None:
    if 'exec_score' not in out:
        out['exec_score'] = np.zeros(rows, dtype=np.float64)
    for missing_name, dtype in (
        ('long_reason_code', np.int16),
        ('short_reason_code', np.int16),
        ('source_decision_code', np.int8),
        ('conviction_high', np.int8),
        ('vwap_dist_sigma', np.float64),
        ('btc_mom_60', np.float64),
        ('flow_fade_confirmed', np.int8),
        ('short_recovery_blocked', np.int8),
    ):
        if missing_name not in out:
            if missing_name == 'btc_mom_60':
                out[missing_name] = np.full(rows, np.nan, dtype=dtype)
            else:
                out[missing_name] = np.zeros(rows, dtype=dtype)


def load_compiled(path: str, mmap: bool = True, validate_sources: bool = True,
                  prefer_day_shards: bool = False,
                  day_subset: list[str] | tuple[str, ...] | set[str] | None = None,
                  allow_historical_day_shard_score: bool = False,
                  historical_cutoff_day: str | None = None) -> dict:
    manifest_path = path
    if os.path.isdir(manifest_path):
        manifest_path = os.path.join(manifest_path, 'manifest.json')
    with open(manifest_path, 'r', encoding='utf-8-sig') as f:
        manifest = json.load(f)
    manifest['manifest_path'] = manifest_path
    lineage_validation = {'status': 'VALIDATION_SKIPPED', 'quick_score_allowed': True, 'certified': False}
    if validate_sources:
        try:
            lineage_validation = _validate_source_hashes(manifest)
        except compiled_tape_lineage.CompiledTapeLineageError as exc:
            if not (allow_historical_day_shard_score and prefer_day_shards):
                raise
            lineage_validation = _historical_day_shard_lineage(
                manifest,
                exc.lineage,
                day_subset=day_subset,
                historical_cutoff_day=historical_cutoff_day,
            )
            if not lineage_validation.get('quick_score_allowed'):
                raise
            day_subset = lineage_validation.get('allowed_days') or []
    manifest['lineage_validation'] = lineage_validation
    use_day_shards = bool(day_subset or prefer_day_shards or manifest.get('range_linked') or not manifest.get('arrays_path'))
    if use_day_shards:
        out = _load_day_shard_arrays(manifest, mmap=mmap, day_subset=day_subset)
        _fill_missing_arrays(out, int(out.get('rows') or 0))
        return out
    arrays = np.load(manifest['arrays_path'], mmap_mode='r' if mmap else None)
    expected_features = len(fast.FEATURE_NAMES)
    actual_features = int(arrays['features'].shape[1]) if 'features' in arrays.files else 0
    if actual_features != expected_features:
        raise ValueError(
            f"compiled decision tape feature mismatch: arrays have {actual_features} features, "
            f"current engine expects {expected_features}. Rebuild the compiled tape."
        )
    out = {name: arrays[name] for name in arrays.files}
    _fill_missing_arrays(out, int(manifest['rows']))
    out.update({
        'rows': int(manifest['rows']),
        'min_ts': int(manifest.get('min_ts') or 0),
        'max_ts': int(manifest.get('max_ts') or 0),
        'ticker_count': int(manifest['ticker_count']),
        'setup_count': int(manifest['setup_count']),
        'day_count': int(manifest.get('day_count') or 1),
        'ticker_map': manifest.get('ticker_map') or {},
        'setup_map': manifest.get('setup_map') or {},
        'day_map': manifest.get('day_map') or {},
        'live_long_gate_mode': int(manifest.get('live_long_gate_mode') or _live_long_gate_mode()[0]),
        'live_long_gate_threshold': float(manifest.get('live_long_gate_threshold') or _live_long_gate_mode()[1]),
        'manifest': manifest,
    })
    return out


def select_days(compiled: dict, days: list[str] | tuple[str, ...] | set[str]) -> dict:
    wanted = {str(day) for day in days}
    if not wanted:
        return compiled
    day_map = compiled.get('day_map') or {}
    selected_codes = {int(code) for day, code in day_map.items() if str(day) in wanted}
    if not selected_codes:
        raise ValueError(f'compiled cache has no selected days: {sorted(wanted)}')
    day_code = np.asarray(compiled.get('day_code'))
    mask = np.isin(day_code, np.asarray(sorted(selected_codes), dtype=day_code.dtype))
    rows = int(mask.sum())
    out: dict[str, Any] = {}
    for key, value in compiled.items():
        if isinstance(value, np.ndarray) and value.ndim >= 1 and value.shape[0] == int(compiled.get('rows') or value.shape[0]):
            out[key] = value[mask]
        else:
            out[key] = value
    old_to_new = {old: idx for idx, old in enumerate(sorted(selected_codes))}
    remapped_day_code = np.zeros(rows, dtype=np.int16)
    old_day_code = np.asarray(compiled.get('day_code'))[mask]
    for old, new in old_to_new.items():
        remapped_day_code[old_day_code == old] = new
    out['day_code'] = remapped_day_code
    reverse_days = {int(v): str(k) for k, v in day_map.items()}
    out['day_map'] = {reverse_days[old]: new for old, new in old_to_new.items()}
    out['rows'] = rows
    out['day_count'] = len(old_to_new)
    out['min_ts'] = int(np.min(out['ts'])) if rows and 'ts' in out else 0
    out['max_ts'] = int(np.max(out['ts'])) if rows and 'ts' in out else 0
    manifest = dict(compiled.get('manifest') or {})
    manifest['day_subset'] = sorted(wanted)
    manifest['rows'] = rows
    manifest['day_count'] = len(old_to_new)
    manifest['day_map'] = out['day_map']
    out['manifest'] = manifest
    return out


if njit is not None:  # pragma: no cover
    @njit(cache=True, parallel=True)
    def _simulate_core(side_matrix, ts, ticker_code, setup_code, conviction_ok, conviction_high,
                       day_code, source_decision_code, long_pnl_pct, short_pnl_pct, long_held, short_held,
                       long_reason_code, short_reason_code,
                       ticker_ret, vwap_dist, vwap_dist_sigma, btc_bullish, btc_mom_60, exec_score,
                       flow_fade_confirmed, short_recovery_blocked, setup_requires_state,
                       setup_state_trigger_kind, cooldown_by_setup,
                       gate_mode, gate_threshold, use_short_recovery_guard,
                       starting_balance, trade_size_pct,
                        ticker_count, setup_count, day_count, min_exec_score,
                        admission_mode, require_conviction,
                       setup_state_enabled, state_min_confirm_sec, state_max_confirm_sec,
                       stop_loss_cooldown_sec, loss_cluster_window_sec,
                       loss_cluster_count, loss_cluster_cooldown_sec,
                       use_step2_brs_long_guard, brs_setup_code,
                       same_ticker_reentry_cooldown_sec,
                       max_trades_per_day, max_trades_per_ticker_day):
        variants = side_matrix.shape[0]
        rows = side_matrix.shape[1]
        pnl = np.zeros(variants, dtype=np.float64)
        wins = np.zeros(variants, dtype=np.int64)
        losses = np.zeros(variants, dtype=np.int64)
        trades = np.zeros(variants, dtype=np.int64)
        skipped_open = np.zeros(variants, dtype=np.int64)
        skipped_cooldown = np.zeros(variants, dtype=np.int64)
        skipped_conviction = np.zeros(variants, dtype=np.int64)
        skipped_exec = np.zeros(variants, dtype=np.int64)
        skipped_gate = np.zeros(variants, dtype=np.int64)
        skipped_setup_state = np.zeros(variants, dtype=np.int64)
        skipped_stop_cooldown = np.zeros(variants, dtype=np.int64)
        skipped_loss_cluster = np.zeros(variants, dtype=np.int64)
        skipped_step2_brs_guard = np.zeros(variants, dtype=np.int64)
        skipped_mock_density = np.zeros(variants, dtype=np.int64)
        by_day_pnl = np.zeros((variants, day_count), dtype=np.float64)
        by_day_trades = np.zeros((variants, day_count), dtype=np.int64)
        by_ticker_pnl = np.zeros((variants, ticker_count), dtype=np.float64)
        by_ticker_trades = np.zeros((variants, ticker_count), dtype=np.int64)
        by_side_pnl = np.zeros((variants, 2), dtype=np.float64)
        by_side_trades = np.zeros((variants, 2), dtype=np.int64)
        by_side_wins = np.zeros((variants, 2), dtype=np.int64)
        by_side_losses = np.zeros((variants, 2), dtype=np.int64)
        for v in prange(variants):
            balance = starting_balance
            open_until = np.zeros(ticker_count, dtype=np.int64)
            stop_cooldown_until = np.zeros(ticker_count, dtype=np.int64)
            loss_cluster_until = np.zeros((ticker_count, 2, setup_count), dtype=np.int64)
            loss_cluster_recent = np.zeros((ticker_count, 2, setup_count, 8), dtype=np.int64)
            loss_cluster_recent_count = np.zeros((ticker_count, 2, setup_count), dtype=np.int64)
            last_signal = np.zeros((ticker_count, 2, setup_count), dtype=np.float64)
            setup_armed_at = np.zeros((ticker_count, 2, setup_count), dtype=np.float64)
            day_trade_count = np.zeros(day_count, dtype=np.int64)
            ticker_day_trade_count = np.zeros((ticker_count, day_count), dtype=np.int64)
            for i in range(rows):
                tkr = int(ticker_code[i])
                t = int(ts[i])
                d = int(day_code[i])
                source_decision = int(source_decision_code[i])
                if admission_mode == 1 and source_decision != 1:
                    skipped_gate[v] += 1
                    continue
                if admission_mode == 2 and source_decision == 2:
                    skipped_gate[v] += 1
                    continue
                if t < open_until[tkr]:
                    skipped_open[v] += 1
                    continue
                if stop_loss_cooldown_sec > 0 and t < stop_cooldown_until[tkr]:
                    skipped_stop_cooldown[v] += 1
                    continue
                if require_conviction > 0 and conviction_ok[i] == 0:
                    skipped_conviction[v] += 1
                    continue
                if exec_score[i] < min_exec_score:
                    skipped_exec[v] += 1
                    continue
                if max_trades_per_day > 0 and day_trade_count[d] >= max_trades_per_day:
                    skipped_mock_density[v] += 1
                    continue
                if max_trades_per_ticker_day > 0 and ticker_day_trade_count[tkr, d] >= max_trades_per_ticker_day:
                    skipped_mock_density[v] += 1
                    continue
                side = side_matrix[v, i]
                if side == SIDE_SKIP:
                    skipped_gate[v] += 1
                    continue
                setup = int(setup_code[i])
                if (use_step2_brs_long_guard > 0 and side == SIDE_LONG
                        and setup == brs_setup_code and conviction_high[i] == 0):
                    mom60 = btc_mom_60[i]
                    if not np.isnan(mom60) and mom60 < 0.0:
                        skipped_step2_brs_guard[v] += 1
                        continue
                if use_short_recovery_guard > 0 and side == SIDE_SHORT and short_recovery_blocked[i] == 1:
                    skipped_gate[v] += 1
                    continue
                if side == SIDE_LONG and gate_mode > 0 and ticker_ret[i] < gate_threshold:
                    blocked = False
                    if gate_mode == 1:
                        blocked = True
                    elif gate_mode == 2:
                        blocked = vwap_dist[i] < 0.0
                    elif gate_mode == 3:
                        blocked = btc_bullish[i] == 0
                    elif gate_mode == 4:
                        blocked = vwap_dist[i] < 0.0 and btc_bullish[i] == 0
                    if blocked:
                        skipped_gate[v] += 1
                        continue
                side_idx = 0 if side == SIDE_LONG else 1
                if loss_cluster_cooldown_sec > 0 and t < loss_cluster_until[tkr, side_idx, setup]:
                    skipped_loss_cluster[v] += 1
                    continue
                if setup_state_enabled > 0 and setup_requires_state[setup] == 1:
                    trigger = False
                    trigger_kind = setup_state_trigger_kind[setup]
                    if trigger_kind == 1:
                        trigger = flow_fade_confirmed[i] == 1
                    else:
                        sig = vwap_dist_sigma[i]
                        trigger = sig <= 1.0 and sig >= -1.0
                    if not trigger:
                        setup_armed_at[tkr, side_idx, setup] = 0.0
                        skipped_setup_state[v] += 1
                        continue
                    armed = setup_armed_at[tkr, side_idx, setup]
                    if armed <= 0.0:
                        setup_armed_at[tkr, side_idx, setup] = float(t)
                        skipped_setup_state[v] += 1
                        continue
                    age = float(t) - armed
                    if age > state_max_confirm_sec:
                        setup_armed_at[tkr, side_idx, setup] = float(t)
                        skipped_setup_state[v] += 1
                        continue
                    if age < state_min_confirm_sec:
                        skipped_setup_state[v] += 1
                        continue
                    setup_armed_at[tkr, side_idx, setup] = 0.0
                if float(t) - last_signal[tkr, side_idx, setup] < cooldown_by_setup[setup]:
                    skipped_cooldown[v] += 1
                    continue
                pct = long_pnl_pct[i] if side == SIDE_LONG else short_pnl_pct[i]
                reason_code = long_reason_code[i] if side == SIDE_LONG else short_reason_code[i]
                alloc = np.round((balance * trade_size_pct / max(1, ticker_count)) * 100.0) / 100.0
                row_pnl = alloc * pct / 100.0
                pnl[v] += row_pnl
                balance += row_pnl
                by_day_pnl[v, d] += row_pnl
                by_day_trades[v, d] += 1
                by_ticker_pnl[v, tkr] += row_pnl
                by_ticker_trades[v, tkr] += 1
                by_side_pnl[v, side_idx] += row_pnl
                by_side_trades[v, side_idx] += 1
                by_side_wins[v, side_idx] += 1 if row_pnl > 0.0 else 0
                by_side_losses[v, side_idx] += 1 if row_pnl < 0.0 else 0
                day_trade_count[d] += 1
                ticker_day_trade_count[tkr, d] += 1
                wins[v] += 1 if row_pnl > 0.0 else 0
                losses[v] += 1 if row_pnl < 0.0 else 0
                trades[v] += 1
                held = long_held[i] if side == SIDE_LONG else short_held[i]
                # Same-ticker re-entry is blocked until the simulated close
                # time plus the live cleanup/latency buffer.
                open_until[tkr] = t + held + same_ticker_reentry_cooldown_sec
                last_signal[tkr, side_idx, setup] = float(t)
                if reason_code == 4:
                    if stop_loss_cooldown_sec > 0:
                        stop_cooldown_until[tkr] = t + held + stop_loss_cooldown_sec
                    if loss_cluster_cooldown_sec > 0 and loss_cluster_window_sec > 0 and loss_cluster_count > 0:
                        exit_t = t + held
                        cur_count = int(loss_cluster_recent_count[tkr, side_idx, setup])
                        kept = 0
                        for j in range(cur_count):
                            old_t = loss_cluster_recent[tkr, side_idx, setup, j]
                            if old_t >= exit_t - loss_cluster_window_sec:
                                loss_cluster_recent[tkr, side_idx, setup, kept] = old_t
                                kept += 1
                        max_slots = loss_cluster_recent.shape[3]
                        if kept < max_slots:
                            loss_cluster_recent[tkr, side_idx, setup, kept] = exit_t
                            kept += 1
                        else:
                            for j in range(max_slots - 1):
                                loss_cluster_recent[tkr, side_idx, setup, j] = loss_cluster_recent[tkr, side_idx, setup, j + 1]
                            loss_cluster_recent[tkr, side_idx, setup, max_slots - 1] = exit_t
                            kept = max_slots
                        loss_cluster_recent_count[tkr, side_idx, setup] = kept
                        if kept >= loss_cluster_count:
                            loss_cluster_until[tkr, side_idx, setup] = exit_t + loss_cluster_cooldown_sec
                            loss_cluster_recent_count[tkr, side_idx, setup] = 0
        return (pnl, wins, losses, trades, skipped_open, skipped_cooldown,
                skipped_conviction, skipped_exec, skipped_gate, skipped_setup_state,
                skipped_stop_cooldown, skipped_loss_cluster, skipped_step2_brs_guard,
                skipped_mock_density, by_day_pnl, by_day_trades,
                by_ticker_pnl, by_ticker_trades, by_side_pnl, by_side_trades,
                by_side_wins, by_side_losses)
else:
    _simulate_core = None


def side_matrix(compiled: dict, variants: list) -> np.ndarray:
    if any(routed_scoring_profile.is_routed_variant(variant) for variant in variants):
        return routed_scoring_profile.side_matrix(compiled, variants, SIDE_LONG, SIDE_SHORT, SIDE_SKIP)
    delta = _side_matrix_delta(compiled, variants)
    if delta is not None:
        return delta
    weights, bias = massive._variant_matrix(variants)
    scores = compiled['features'] @ weights.T
    if bias.size:
        scores = scores + bias.reshape(1, -1)
    original = compiled['original_side'].reshape(-1, 1)
    chosen = np.where(scores > 0.0, SIDE_LONG, np.where(scores < 0.0, SIDE_SHORT, original))
    return chosen.T.astype(np.int8)


def _weight_vector(weights: dict[str, Any]) -> np.ndarray:
    arr = np.zeros(len(fast.FEATURE_NAMES), dtype=np.float64)
    for idx, name in enumerate(fast.FEATURE_NAMES):
        arr[idx] = float((weights or {}).get(name, 0.0) or 0.0)
    return arr


def _score_vector_cache(compiled: dict) -> dict[str, np.ndarray]:
    cache = compiled.get('_score_vector_cache')
    if not isinstance(cache, dict):
        cache = {}
        compiled['_score_vector_cache'] = cache
    return cache


def _side_matrix_delta(compiled: dict, variants: list) -> np.ndarray | None:
    if not variants:
        return np.zeros((0, int(compiled.get('rows') or 0)), dtype=np.int8)
    groups: dict[str, list[tuple[int, Any, dict[str, float], float]]] = {}
    fallback: list[tuple[int, Any]] = []
    for idx, variant in enumerate(variants):
        base_weights = getattr(variant, 'base_weights', None)
        if not isinstance(base_weights, dict):
            fallback.append((idx, variant))
            continue
        base_bias = float(getattr(variant, 'base_bias', 0.0) or 0.0)
        key = tournament_safety.stable_json_hash({
            'weights': base_weights,
            'bias': base_bias,
        }, 32)
        groups.setdefault(key, []).append((idx, variant, base_weights, base_bias))
    if not groups:
        return None
    rows = int(compiled.get('rows') or len(compiled.get('ts', [])))
    original = np.asarray(compiled['original_side'])
    out = np.empty((len(variants), rows), dtype=np.int8)
    if fallback:
        fallback_sides = side_matrix(compiled, [variant for _, variant in fallback])
        for local_idx, (idx, _) in enumerate(fallback):
            out[idx] = fallback_sides[local_idx]
    features = np.asarray(compiled['features'])
    score_cache = _score_vector_cache(compiled)
    for key, members in groups.items():
        base_weights = members[0][2]
        base_bias = members[0][3]
        base_scores = score_cache.get(key)
        if base_scores is None:
            base_scores = features @ _weight_vector(base_weights) + base_bias
            score_cache[key] = np.asarray(base_scores, dtype=np.float64)
        for idx, variant, _, _ in members:
            scores = np.array(base_scores, copy=True)
            weights = getattr(variant, 'weights', {}) or {}
            bias_delta = float(getattr(variant, 'bias', 0.0) or 0.0) - base_bias
            if bias_delta:
                scores += bias_delta
            for feature_idx, name in enumerate(fast.FEATURE_NAMES):
                delta = float(weights.get(name, 0.0) or 0.0) - float(base_weights.get(name, 0.0) or 0.0)
                if delta:
                    scores += features[:, feature_idx] * delta
            out[idx] = np.where(scores > 0.0, SIDE_LONG, np.where(scores < 0.0, SIDE_SHORT, original)).astype(np.int8)
    compiled['_last_side_matrix_mode'] = {
        'mode': 'delta',
        'variants': len(variants),
        'groups': len(groups),
        'fallback': len(fallback),
    }
    return out


def decision_hashes_from_sides(sides: np.ndarray) -> list[str]:
    import hashlib

    if np.any(sides == SIDE_SKIP):
        encoded = (sides.astype(np.int16) + 1).astype(np.uint8)
        return [hashlib.sha256(encoded[i].tobytes()).hexdigest()[:24] for i in range(encoded.shape[0])]
    packed = np.packbits((sides > 0).astype(np.uint8), axis=1)
    return [hashlib.sha256(packed[i].tobytes()).hexdigest()[:24] for i in range(packed.shape[0])]


def side_matrix_and_hashes(compiled: dict, variants: list) -> tuple[np.ndarray, list[str]]:
    sides = side_matrix(compiled, variants)
    return sides, decision_hashes_from_sides(sides)


def _reverse_map(mapping: dict) -> dict[int, str]:
    return {int(v): str(k) for k, v in (mapping or {}).items()}


def _sim_config_value(config: dict | None, key: str, default: float) -> float:
    try:
        if not config or config.get(key) in (None, ''):
            return float(default)
        return float(config.get(key))
    except Exception:
        return float(default)


def simulate_variants(compiled: dict, variants: list, starting_balance: float,
                      gate: dict | None = None, sim_config: dict | None = None) -> list[dict] | None:
    if not COMPILED_AVAILABLE or not compatible_gate(gate):
        return None
    mode = _gate_mode_code(gate)
    threshold = float((gate or {}).get('ticker_session_return_below_pct',
                                       (gate or {}).get('threshold_pct', -0.5)))
    sides = side_matrix(compiled, variants)
    return simulate_side_matrix(compiled, variants, sides, starting_balance, gate, sim_config=sim_config)


def simulate_side_matrix(compiled: dict, variants: list, sides: np.ndarray, starting_balance: float,
                         gate: dict | None = None, sim_config: dict | None = None) -> list[dict] | None:
    if not COMPILED_AVAILABLE or not compatible_gate(gate):
        return None
    if len(variants) == 0:
        return []
    mode = _gate_mode_code(gate)
    threshold = float((gate or {}).get('ticker_session_return_below_pct',
                                       (gate or {}).get('threshold_pct', -0.5)))
    if mode == 0 and int(_sim_config_value(sim_config, 'use_live_long_gate', 1.0)) > 0:
        mode = int(compiled.get('live_long_gate_mode') or 0)
        threshold = float(compiled.get('live_long_gate_threshold') or threshold)
    setup_map = compiled.get('setup_map') or {}
    setup_requires_state = np.zeros(int(compiled['setup_count']), dtype=np.int8)
    setup_state_trigger_kind = np.zeros(int(compiled['setup_count']), dtype=np.int8)
    for name, raw_idx in setup_map.items():
        idx = int(raw_idx)
        if name == 'flow_exhaustion_fade':
            setup_requires_state[idx] = 1
            setup_state_trigger_kind[idx] = 1
        elif name == 'vwap_reclaim_breakdown':
            setup_requires_state[idx] = 1
            setup_state_trigger_kind[idx] = 2
    arrays = _simulate_core(
        sides,
        compiled['ts'],
        compiled['ticker_code'],
        compiled['setup_code'],
        compiled['conviction_ok'],
        compiled['conviction_high'],
        compiled['day_code'],
        compiled['source_decision_code'],
        compiled['long_pnl_pct'],
        compiled['short_pnl_pct'],
        compiled['long_held'],
        compiled['short_held'],
        compiled['long_reason_code'],
        compiled['short_reason_code'],
        compiled['ticker_session_return_pct'],
        compiled['vwap_dist'],
        compiled['vwap_dist_sigma'],
        compiled['btc_bullish'],
        compiled['btc_mom_60'],
        compiled['exec_score'],
        compiled['flow_fade_confirmed'],
        compiled['short_recovery_blocked'],
        setup_requires_state,
        setup_state_trigger_kind,
        compiled['cooldown_by_setup'],
        int(mode),
        float(threshold),
        int(_sim_config_value(sim_config, 'use_live_short_guard', 0.0)),
        float(starting_balance),
        float(replay.TRADE_SIZE_PCT),
        int(compiled['ticker_count']),
        int(compiled['setup_count']),
        int(compiled.get('day_count') or 1),
        _sim_config_value(sim_config, 'min_exec_score', float(ws_scalp.EXECUTION_QUALITY_MIN)),
        int(_sim_config_value(sim_config, 'admission_mode', 0.0)),
        int(_sim_config_value(sim_config, 'require_conviction', 1.0)),
        int(_sim_config_value(sim_config, 'setup_state_enabled', 1.0)),
        _sim_config_value(sim_config, 'state_min_confirm_sec', float(getattr(ws_scalp, 'STATE_MACHINE_MIN_CONFIRM_SEC', 5))),
        _sim_config_value(sim_config, 'state_max_confirm_sec', float(getattr(ws_scalp, 'STATE_MACHINE_MAX_CONFIRM_SEC', 45))),
        int(_sim_config_value(sim_config, 'stop_loss_cooldown_sec', 0.0)),
        int(_sim_config_value(sim_config, 'loss_cluster_window_sec', 0.0)),
        int(_sim_config_value(sim_config, 'loss_cluster_count', 0.0)),
        int(_sim_config_value(sim_config, 'loss_cluster_cooldown_sec', 0.0)),
        int(_sim_config_value(sim_config, 'use_step2_brs_long_guard', 1.0)),
        int(setup_map.get('btc_relative_strength', -1)),
        int(_sim_config_value(sim_config, 'same_ticker_reentry_cooldown_sec', 0.0)),
        int(_sim_config_value(sim_config, 'max_trades_per_day', 0.0)),
        int(_sim_config_value(sim_config, 'max_trades_per_ticker_day', 0.0)),
    )
    (pnl, wins, losses, trades, skipped_open, skipped_cooldown, skipped_conviction,
     skipped_exec, skipped_gate, skipped_setup_state, skipped_stop_cooldown,
     skipped_loss_cluster, skipped_step2_brs_guard, skipped_mock_density, by_day_pnl, by_day_trades,
     by_ticker_pnl, by_ticker_trades, by_side_pnl, by_side_trades,
     by_side_wins, by_side_losses) = arrays
    out = []
    day_names = _reverse_map(compiled.get('day_map') or {})
    ticker_names = _reverse_map(compiled.get('ticker_map') or {})
    for idx, variant in enumerate(variants):
        tr = int(trades[idx])
        by_day = {}
        for day_idx in range(by_day_pnl.shape[1]):
            trades_i = int(by_day_trades[idx, day_idx])
            by_day[day_names.get(day_idx, str(day_idx))] = {
                'trades': trades_i,
                'pnl': round(float(by_day_pnl[idx, day_idx]), 2),
            }
        by_ticker = {}
        for ticker_idx in range(by_ticker_pnl.shape[1]):
            trades_i = int(by_ticker_trades[idx, ticker_idx])
            by_ticker[ticker_names.get(ticker_idx, str(ticker_idx))] = {
                'trades': trades_i,
                'pnl': round(float(by_ticker_pnl[idx, ticker_idx]), 2),
            }
        by_side = {
            'LONG': {
                'trades': int(by_side_trades[idx, 0]),
                'wins': int(by_side_wins[idx, 0]),
                'losses': int(by_side_losses[idx, 0]),
                'pnl': round(float(by_side_pnl[idx, 0]), 2),
            },
            'SHORT': {
                'trades': int(by_side_trades[idx, 1]),
                'wins': int(by_side_wins[idx, 1]),
                'losses': int(by_side_losses[idx, 1]),
                'pnl': round(float(by_side_pnl[idx, 1]), 2),
            },
        }
        worst_day = None
        if by_day:
            worst_key = min(by_day, key=lambda key: by_day[key]['pnl'])
            worst_day = {'day': worst_key, **by_day[worst_key]}
        row = {
            'variant': variant.name,
            'weights': dict(variant.weights),
            'bias': float(variant.bias or 0.0),
            'exit_replay_model': step2_quote_aware_guard.manifest_exit_replay_model(compiled.get('manifest') or {}),
            'required_exit_replay_model': step2_quote_aware_guard.REQUIRED_EXIT_REPLAY_MODEL,
            'decision_full': {
                'trades': tr,
                'wins': int(wins[idx]),
                'losses': int(losses[idx]),
                'win_rate_pct': round(100 * int(wins[idx]) / tr, 2) if tr else None,
                'pnl': round(float(pnl[idx]), 2),
                'starting_balance': round(float(starting_balance), 2),
                'ending_balance': round(float(starting_balance) + float(pnl[idx]), 2),
                'by_day': by_day,
                'by_ticker': by_ticker,
                'by_side': by_side,
                'worst_day': worst_day,
                'skipped': {
                    'open_position': int(skipped_open[idx]),
                    'cooldown': int(skipped_cooldown[idx]),
                    'below_min_conviction': int(skipped_conviction[idx]),
                    'execution_quality_low': int(skipped_exec[idx]),
                    'entry_gate': int(skipped_gate[idx]),
                    'setup_state_pending': int(skipped_setup_state[idx]),
                    'stop_loss_cooldown': int(skipped_stop_cooldown[idx]),
                    'loss_cluster_throttle': int(skipped_loss_cluster[idx]),
                    'step2_brs_long_btc_negative_mom60': int(skipped_step2_brs_guard[idx]),
                    'mock_density_cap': int(skipped_mock_density[idx]),
                },
                'compiled_decision_tape': True,
                'exit_replay_model': step2_quote_aware_guard.manifest_exit_replay_model(compiled.get('manifest') or {}),
                    'mock_parity_guards': {
                    'step2_brs_long_btc_negative_mom60': bool(
                        int(_sim_config_value(sim_config, 'use_step2_brs_long_guard', 1.0))
                    ),
                    'same_ticker_reentry_cooldown_sec': int(
                        _sim_config_value(sim_config, 'same_ticker_reentry_cooldown_sec', 0.0)
                    ),
                    'max_trades_per_day': int(_sim_config_value(sim_config, 'max_trades_per_day', 0.0)),
                    'max_trades_per_ticker_day': int(_sim_config_value(sim_config, 'max_trades_per_ticker_day', 0.0)),
                    'execution_contract_hash': (compiled.get('manifest') or {}).get(
                        'step2_execution_contract_hash'
                    ),
                },
            },
        }
        if routed_scoring_profile.is_routed_variant(variant):
            row['routed_scoring_profile'] = True
            row['routes'] = routed_scoring_profile.routes_to_dicts(variant)
            row['route_audit'] = routed_scoring_profile.route_audit(compiled, variant, sides=sides[idx])
        out.append(row)
    return out


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Build/load compiled decision-tape arrays for Step 2 acceleration.')
    ap.add_argument('--start', required=True)
    ap.add_argument('--end', required=True)
    ap.add_argument('--tickers', nargs='+', default=replay.TICKERS)
    ap.add_argument('--feed', default='sip')
    ap.add_argument('--quote-mode', default='per-second')
    ap.add_argument('--btc-mode', default='bars')
    ap.add_argument('--indicator-mode', choices=['live', 'fast'], default='live')
    ap.add_argument('--tape-dir', default=DEFAULT_TAPE_DIR)
    ap.add_argument('--tape-profile-name', default='')
    ap.add_argument('--out-dir', default=DEFAULT_OUT_DIR)
    ap.add_argument('--name', default=None)
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    args.tickers = [ticker.upper() for ticker in args.tickers]
    rows_by_day = {}
    source_paths = []
    for day in replay._market_days(replay._parse_day(args.start), replay._parse_day(args.end)):
        day_s = day.isoformat()
        path = _tape_path(args, day_s)
        rows_by_day[day_s] = _read_tape(path)
        source_paths.append(path)
    compiled = compile_rows(rows_by_day, args.tickers, indicator_mode=args.indicator_mode)
    name = args.name or f"compiled_decision_tape_{args.feed}_{args.quote_mode}_{args.btc_mode}_{args.indicator_mode}_{'-'.join(args.tickers)}_{args.start}_{args.end}"
    manifest = save_compiled(compiled, args.out_dir, name, source_paths)
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
