"""Opt-in multi-profile full replay runner.

This shares market event feeding and indicator calculation across many scoring
profiles while keeping every profile's portfolio state isolated. It is not used
by the normal Step 3 path unless explicitly invoked.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
import csv
import hashlib
import json
import os
import subprocess
import sys
import time

import backtest_30d_engine as replay
import replay_artifacts
import scoring_profiles
import scoring_variant_lab as slow_lab
import variant_tournament_runner as tournament
import ws_scalp


HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUT_DIR = os.path.join(HERE, 'postmortem', 'backtests', 'multi_profile_full_replay')
DEFAULT_SHARED_CACHE_DIR = os.path.join(HERE, 'postmortem', 'backtests', 'multi_profile_exact_cache')
DEFAULT_PARITY_CACHE_DIR = os.path.join(HERE, 'postmortem', 'backtests', 'multi_profile_parity_cache')


@dataclass
class ProfileState:
    variant: str
    profile_path: str
    profile: dict
    balance: float
    stats: replay.ReplayStats = field(default_factory=replay.ReplayStats)
    setup_states: dict = field(default_factory=dict)
    last_signal_ts: dict[str, float] = field(default_factory=dict)
    giveback_cooldown_until: dict[str, int] = field(default_factory=dict)
    stop_loss_cooldown_until: dict[str, int] = field(default_factory=dict)
    open_positions: dict[str, replay.Position] = field(default_factory=dict)
    pending_entries: dict[str, dict] = field(default_factory=dict)
    rows: list[dict] = field(default_factory=list)
    opportunity_seq: int = 0
    day_trade_count: int = 0
    ticker_day_trade_count: dict[str, int] = field(default_factory=dict)


def _load_rows(path: str, limit: int) -> list[dict]:
    with open(path, 'r', encoding='utf-8') as f:
        payload = json.load(f)
    rows = payload.get('results') or []
    return rows[:limit] if limit else rows


def _selected_days(args) -> list:
    return replay._selected_market_days(args, replay._parse_day(args.start), replay._parse_day(args.end))


def _variant_from_row(row: dict) -> slow_lab.Variant:
    return slow_lab.Variant(
        row['variant'],
        dict(row.get('weights') or {}),
        float(row.get('bias') or 0.0),
    )


def _write_outputs(rows: list[dict], summary: dict, args, profile_id: str) -> tuple[str, str]:
    out_dir = os.path.join(args.out_dir, 'profiles', tournament._safe_name(profile_id))
    os.makedirs(out_dir, exist_ok=True)
    indicator_mode = getattr(args, 'indicator_mode', 'fast')
    stem = f'engine_replay_{args.start}_{args.end}_{tournament._short_id(profile_id)}_finalist_{indicator_mode}'
    csv_path = os.path.join(out_dir, f'{stem}_trades.csv')
    json_path = os.path.join(out_dir, f'{stem}_summary.json')
    fieldnames = [
        'opportunity_id', 'entry_ct', 'exit_ct', 'ticker', 'side', 'entry',
        'exit', 'qty', 'alloc', 'pnl', 'pnl_pct', 'mfe_pct', 'mae_pct',
        'held_sec', 'reason', 'score', 'conviction', 'setup_type',
        'session_phase', 'btc_regime', 'exec_score', 'min_score_required',
        'scoring_profile', 'profile_score', 'profile_original_side',
        'reasons',
    ]
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k) for k in fieldnames})
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, sort_keys=True)
    return csv_path, json_path


def _trade_rows_hash(rows: list[dict]) -> str:
    return hashlib.sha256(
        json.dumps(_rows_signature(rows), sort_keys=True, separators=(',', ':')).encode('utf-8')
    ).hexdigest()


def _result_hashes(result: dict) -> dict:
    csv_path = result.get('csv')
    rows = _load_csv_rows(csv_path) if csv_path and os.path.exists(csv_path) else []
    summary = result.get('summary') or {}
    return {
        'trade_rows_sha256': _trade_rows_hash(rows),
        'summary_sha256': hashlib.sha256(
            json.dumps(summary, sort_keys=True, separators=(',', ':')).encode('utf-8')
        ).hexdigest(),
    }


def _annotate_result_hashes(results: list[dict]) -> None:
    for result in results:
        result.setdefault('hashes', {}).update(_result_hashes(result))


def _copy_counter(dst: Counter, src: Counter) -> None:
    for key, value in src.items():
        dst[key] += value


def _profile_specs_hash(profile_specs: list[dict]) -> str:
    payload = [
        {
            'variant': spec['variant'],
            'profile_sha256': replay._file_sha256(spec['profile_path']),
            'profile': spec['profile'],
        }
        for spec in profile_specs
    ]
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('utf-8')).hexdigest()


def _shared_pass_cache_key(args, profile_specs: list[dict], method: str) -> tuple[str, dict]:
    days = _selected_days(args)
    payload = {
        'schema_version': 1,
        'method': method,
        'profiles_sha256': _profile_specs_hash(profile_specs),
        'replay': {
            'start': args.start,
            'end': args.end,
            'sample_days': getattr(args, 'sample_days', ''),
            'tickers': list(args.tickers),
            'feed': args.feed,
            'quote_mode': args.quote_mode,
            'btc_mode': args.btc_mode,
            'start_balance': float(args.start_balance),
            'entry_gate_mode': getattr(args, 'entry_gate_mode', None),
            'entry_gate_threshold_pct': getattr(args, 'entry_gate_threshold_pct', None),
            'brs_tp_multiplier': float(getattr(args, 'brs_tp_multiplier', 1.0) or 1.0),
            'momentum_tp_multiplier': float(getattr(args, 'momentum_tp_multiplier', 1.0) or 1.0),
            'high_conviction_tp_multiplier': float(getattr(args, 'high_conviction_tp_multiplier', 1.0) or 1.0),
            'fixed_tp_pct': getattr(args, 'fixed_tp_pct', None),
            'fixed_sl_pct': getattr(args, 'fixed_sl_pct', None),
            'entry_confirmation_delay_sec': int(float(getattr(args, 'entry_confirmation_delay_sec', 0.0) or 0.0)),
            'entry_confirmation_mode': getattr(args, 'entry_confirmation_mode', 'off'),
            'entry_confirmation_setup': getattr(args, 'entry_confirmation_setup', 'all'),
            'entry_confirmation_min_favorable_pct': float(getattr(args, 'entry_confirmation_min_favorable_pct', 0.0) or 0.0),
            'entry_confirmation_adverse_pct': float(getattr(args, 'entry_confirmation_adverse_pct', 0.0) or 0.0),
            'entry_confirmation_session_phase': getattr(args, 'entry_confirmation_session_phase', 'all'),
            'entry_confirmation_edge_threshold_pct': getattr(args, 'entry_confirmation_edge_threshold_pct', None),
            'entry_confirmation_rel_threshold_pct': getattr(args, 'entry_confirmation_rel_threshold_pct', None),
            'path_failure_after_sec': int(float(getattr(args, 'path_failure_after_sec', 0.0) or 0.0)),
            'path_failure_edge_threshold_pct': float(getattr(args, 'path_failure_edge_threshold_pct', 0.0) or 0.0),
            'path_failure_action': getattr(args, 'path_failure_action', 'off'),
            'path_failure_setup': getattr(args, 'path_failure_setup', 'all'),
            'path_failure_session_phase': getattr(args, 'path_failure_session_phase', 'all'),
            'path_failure_rel_threshold_pct': getattr(args, 'path_failure_rel_threshold_pct', None),
            'path_failure_window_sec': int(float(getattr(args, 'path_failure_window_sec', 0.0) or 0.0)),
            'path_failure_reduce_fraction': float(getattr(args, 'path_failure_reduce_fraction', 0.0) or 0.0),
            'giveback_exit_after_sec': int(float(getattr(args, 'giveback_exit_after_sec', 0.0) or 0.0)),
            'giveback_exit_min_mfe_pct': float(getattr(args, 'giveback_exit_min_mfe_pct', 0.0) or 0.0),
            'giveback_exit_giveback_pct': float(getattr(args, 'giveback_exit_giveback_pct', 0.0) or 0.0),
            'giveback_exit_max_pnl_pct': float(getattr(args, 'giveback_exit_max_pnl_pct', 0.0) or 0.0),
            'giveback_exit_confirm_sec': int(float(getattr(args, 'giveback_exit_confirm_sec', 0.0) or 0.0)),
            'giveback_exit_action': getattr(args, 'giveback_exit_action', 'off'),
            'giveback_exit_setup': getattr(args, 'giveback_exit_setup', 'all'),
            'giveback_exit_cooldown_sec': int(float(getattr(args, 'giveback_exit_cooldown_sec', 0.0) or 0.0)),
            'validation_mode': 'finalist',
            'indicator_mode': getattr(args, 'indicator_mode', 'fast'),
        },
        'data_fingerprints': {
            day.isoformat(): replay_artifacts.artifact_fingerprint(args, day).get('fingerprint')
            for day in days
        },
        'code_hashes': {
            'multi_profile_full_replay.py': replay._file_sha256(os.path.join(HERE, 'multi_profile_full_replay.py')),
            'backtest_30d_engine.py': replay._file_sha256(os.path.join(HERE, 'backtest_30d_engine.py')),
            'ws_scalp.py': replay._file_sha256(os.path.join(HERE, 'ws_scalp.py')),
            'scoring_profiles.py': replay._file_sha256(os.path.join(HERE, 'scoring_profiles.py')),
            'trading_config.json': replay._file_sha256(os.path.join(HERE, 'trading_config.json')),
        },
    }
    key = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('utf-8')).hexdigest()
    return key, payload


def _shared_cache_paths(cache_dir: str, key: str) -> dict:
    root = os.path.join(cache_dir, key[:2], key)
    return {
        'root': root,
        'meta': os.path.join(root, 'meta.json'),
        'results': os.path.join(root, 'results'),
    }


def _parity_cache_key(args, variant: slow_lab.Variant, profile_path: str) -> tuple[str, dict]:
    days = _selected_days(args)
    payload = {
        'schema_version': 1,
        'variant': {
            'name': variant.name,
            'weights': dict(variant.weights),
            'bias': float(variant.bias or 0.0),
            'profile_sha256': replay._file_sha256(profile_path),
        },
        'replay': {
            'start': args.start,
            'end': args.end,
            'sample_days': getattr(args, 'sample_days', ''),
            'tickers': list(args.tickers),
            'feed': args.feed,
            'quote_mode': args.quote_mode,
            'btc_mode': args.btc_mode,
            'start_balance': float(args.start_balance),
            'entry_gate_mode': getattr(args, 'entry_gate_mode', None),
            'entry_gate_threshold_pct': getattr(args, 'entry_gate_threshold_pct', None),
            'brs_tp_multiplier': float(getattr(args, 'brs_tp_multiplier', 1.0) or 1.0),
            'momentum_tp_multiplier': float(getattr(args, 'momentum_tp_multiplier', 1.0) or 1.0),
            'high_conviction_tp_multiplier': float(getattr(args, 'high_conviction_tp_multiplier', 1.0) or 1.0),
            'fixed_tp_pct': getattr(args, 'fixed_tp_pct', None),
            'fixed_sl_pct': getattr(args, 'fixed_sl_pct', None),
            'entry_confirmation_delay_sec': int(float(getattr(args, 'entry_confirmation_delay_sec', 0.0) or 0.0)),
            'entry_confirmation_mode': getattr(args, 'entry_confirmation_mode', 'off'),
            'entry_confirmation_setup': getattr(args, 'entry_confirmation_setup', 'all'),
            'entry_confirmation_min_favorable_pct': float(getattr(args, 'entry_confirmation_min_favorable_pct', 0.0) or 0.0),
            'entry_confirmation_adverse_pct': float(getattr(args, 'entry_confirmation_adverse_pct', 0.0) or 0.0),
            'entry_confirmation_session_phase': getattr(args, 'entry_confirmation_session_phase', 'all'),
            'entry_confirmation_edge_threshold_pct': getattr(args, 'entry_confirmation_edge_threshold_pct', None),
            'entry_confirmation_rel_threshold_pct': getattr(args, 'entry_confirmation_rel_threshold_pct', None),
            'path_failure_after_sec': int(float(getattr(args, 'path_failure_after_sec', 0.0) or 0.0)),
            'path_failure_edge_threshold_pct': float(getattr(args, 'path_failure_edge_threshold_pct', 0.0) or 0.0),
            'path_failure_action': getattr(args, 'path_failure_action', 'off'),
            'path_failure_setup': getattr(args, 'path_failure_setup', 'all'),
            'path_failure_session_phase': getattr(args, 'path_failure_session_phase', 'all'),
            'path_failure_rel_threshold_pct': getattr(args, 'path_failure_rel_threshold_pct', None),
            'path_failure_window_sec': int(float(getattr(args, 'path_failure_window_sec', 0.0) or 0.0)),
            'path_failure_reduce_fraction': float(getattr(args, 'path_failure_reduce_fraction', 0.0) or 0.0),
            'giveback_exit_after_sec': int(float(getattr(args, 'giveback_exit_after_sec', 0.0) or 0.0)),
            'giveback_exit_min_mfe_pct': float(getattr(args, 'giveback_exit_min_mfe_pct', 0.0) or 0.0),
            'giveback_exit_giveback_pct': float(getattr(args, 'giveback_exit_giveback_pct', 0.0) or 0.0),
            'giveback_exit_max_pnl_pct': float(getattr(args, 'giveback_exit_max_pnl_pct', 0.0) or 0.0),
            'giveback_exit_confirm_sec': int(float(getattr(args, 'giveback_exit_confirm_sec', 0.0) or 0.0)),
            'giveback_exit_action': getattr(args, 'giveback_exit_action', 'off'),
            'giveback_exit_setup': getattr(args, 'giveback_exit_setup', 'all'),
            'giveback_exit_cooldown_sec': int(float(getattr(args, 'giveback_exit_cooldown_sec', 0.0) or 0.0)),
            'validation_mode': 'finalist',
            'indicator_mode': getattr(args, 'indicator_mode', 'fast'),
            'workers': 1,
        },
        'data_fingerprints': {
            day.isoformat(): replay_artifacts.artifact_fingerprint(args, day).get('fingerprint')
            for day in days
        },
        'code_hashes': {
            'backtest_30d_engine.py': replay._file_sha256(os.path.join(HERE, 'backtest_30d_engine.py')),
            'ws_scalp.py': replay._file_sha256(os.path.join(HERE, 'ws_scalp.py')),
            'scoring_profiles.py': replay._file_sha256(os.path.join(HERE, 'scoring_profiles.py')),
            'trading_config.json': replay._file_sha256(os.path.join(HERE, 'trading_config.json')),
        },
    }
    key = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('utf-8')).hexdigest()
    return key, payload


def _parity_cache_paths(cache_dir: str, key: str) -> dict:
    root = os.path.join(cache_dir, key[:2], key)
    return {
        'root': root,
        'meta': os.path.join(root, 'meta.json'),
        'summary': os.path.join(root, 'summary.json'),
        'trades': os.path.join(root, 'trades.csv'),
    }


def _restore_parity_cache(args, variant: slow_lab.Variant, profile_path: str,
                          single_json: str, single_csv: str) -> dict | None:
    if not getattr(args, 'multi_profile_parity_cache', False):
        return None
    key, key_payload = _parity_cache_key(args, variant, profile_path)
    paths = _parity_cache_paths(getattr(args, 'multi_profile_parity_cache_dir', DEFAULT_PARITY_CACHE_DIR), key)
    if not os.path.exists(paths['meta']) or not os.path.exists(paths['summary']) or not os.path.exists(paths['trades']):
        return None
    with open(paths['meta'], 'r', encoding='utf-8') as f:
        meta = json.load(f)
    if meta.get('key_payload') != key_payload:
        return None
    import shutil
    os.makedirs(os.path.dirname(single_json), exist_ok=True)
    shutil.copy2(paths['summary'], single_json)
    shutil.copy2(paths['trades'], single_csv)
    return {'cache_key': key, 'cache_meta': paths['meta']}


def _store_parity_cache(args, variant: slow_lab.Variant, profile_path: str,
                        single_json: str, single_csv: str) -> dict | None:
    if not getattr(args, 'multi_profile_parity_cache', False):
        return None
    if not os.path.exists(single_json) or not os.path.exists(single_csv):
        return None
    key, key_payload = _parity_cache_key(args, variant, profile_path)
    paths = _parity_cache_paths(getattr(args, 'multi_profile_parity_cache_dir', DEFAULT_PARITY_CACHE_DIR), key)
    os.makedirs(paths['root'], exist_ok=True)
    import shutil
    shutil.copy2(single_json, paths['summary'])
    shutil.copy2(single_csv, paths['trades'])
    meta = {
        'schema_version': 1,
        'cache_key': key,
        'key_payload': key_payload,
        'summary_sha256': replay._file_sha256(single_json),
        'trades_sha256': replay._file_sha256(single_csv),
    }
    tmp = paths['meta'] + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(meta, f, indent=2, sort_keys=True)
    os.replace(tmp, paths['meta'])
    return {'cache_key': key, 'cache_meta': paths['meta']}


def _restore_shared_cache(args, profile_specs: list[dict], method: str) -> list[dict] | None:
    if not getattr(args, 'multi_profile_shared_cache', False):
        return None
    key, key_payload = _shared_pass_cache_key(args, profile_specs, method)
    paths = _shared_cache_paths(getattr(args, 'multi_profile_shared_cache_dir', DEFAULT_SHARED_CACHE_DIR), key)
    if not os.path.exists(paths['meta']):
        return None
    with open(paths['meta'], 'r', encoding='utf-8') as f:
        meta = json.load(f)
    if meta.get('key_payload') != key_payload:
        return None
    results = []
    for item in meta.get('results') or []:
        variant = item.get('variant')
        src_summary = os.path.join(paths['results'], item.get('summary_name') or '')
        src_csv = os.path.join(paths['results'], item.get('csv_name') or '')
        if not variant or not os.path.exists(src_summary) or not os.path.exists(src_csv):
            return None
        out_dir = os.path.join(args.out_dir, 'profiles', tournament._safe_name(variant))
        os.makedirs(out_dir, exist_ok=True)
        stem = f'engine_replay_{args.start}_{args.end}_{tournament._short_id(variant)}_finalist_fast'
        dst_summary = os.path.join(out_dir, f'{stem}_summary.json')
        dst_csv = os.path.join(out_dir, f'{stem}_trades.csv')
        import shutil
        shutil.copy2(src_summary, dst_summary)
        shutil.copy2(src_csv, dst_csv)
        with open(dst_summary, 'r', encoding='utf-8') as f:
            summary = json.load(f)
        results.append({
            'variant': variant,
            'profile': item.get('profile'),
            'summary': summary,
            'csv': dst_csv,
            'summary_path': dst_summary,
            'reused_multi_profile_shared_cache': True,
            'cache_key': key,
        })
    _annotate_result_hashes(results)
    results.sort(key=lambda row: float((row.get('summary') or {}).get('pnl') or 0.0), reverse=True)
    return results


def _store_shared_cache(args, profile_specs: list[dict], method: str, results: list[dict]) -> dict | None:
    if not getattr(args, 'multi_profile_shared_cache', False):
        return None
    key, key_payload = _shared_pass_cache_key(args, profile_specs, method)
    paths = _shared_cache_paths(getattr(args, 'multi_profile_shared_cache_dir', DEFAULT_SHARED_CACHE_DIR), key)
    os.makedirs(paths['results'], exist_ok=True)
    meta_results = []
    import shutil
    for result in results:
        variant = result.get('variant')
        if not variant or not os.path.exists(result.get('summary_path') or '') or not os.path.exists(result.get('csv') or ''):
            return None
        safe = tournament._short_id(variant)
        summary_name = f'{safe}_summary.json'
        csv_name = f'{safe}_trades.csv'
        shutil.copy2(result['summary_path'], os.path.join(paths['results'], summary_name))
        shutil.copy2(result['csv'], os.path.join(paths['results'], csv_name))
        meta_results.append({
            'variant': variant,
            'profile': result.get('profile'),
            'summary_name': summary_name,
            'csv_name': csv_name,
            'hashes': result.get('hashes'),
        })
    meta = {
        'schema_version': 1,
        'cache_key': key,
        'key_payload': key_payload,
        'results': meta_results,
    }
    tmp = paths['meta'] + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(meta, f, indent=2, sort_keys=True)
    os.replace(tmp, paths['meta'])
    return {'cache_key': key, 'cache_meta': paths['meta']}


def _day_shard_fingerprint(args, day, profile_specs: list[dict]) -> str:
    payload = {
        'schema_version': 1,
        'day': day.isoformat(),
        'profiles_sha256': _profile_specs_hash(profile_specs),
        'replay': {
            'tickers': list(args.tickers),
            'feed': args.feed,
            'quote_mode': args.quote_mode,
            'btc_mode': args.btc_mode,
            'start_balance': float(args.start_balance),
            'entry_gate_mode': getattr(args, 'entry_gate_mode', None),
            'entry_gate_threshold_pct': getattr(args, 'entry_gate_threshold_pct', None),
            'brs_tp_multiplier': float(getattr(args, 'brs_tp_multiplier', 1.0) or 1.0),
            'momentum_tp_multiplier': float(getattr(args, 'momentum_tp_multiplier', 1.0) or 1.0),
            'high_conviction_tp_multiplier': float(getattr(args, 'high_conviction_tp_multiplier', 1.0) or 1.0),
            'fixed_tp_pct': getattr(args, 'fixed_tp_pct', None),
            'fixed_sl_pct': getattr(args, 'fixed_sl_pct', None),
            'entry_confirmation_delay_sec': int(float(getattr(args, 'entry_confirmation_delay_sec', 0.0) or 0.0)),
            'entry_confirmation_mode': getattr(args, 'entry_confirmation_mode', 'off'),
            'entry_confirmation_setup': getattr(args, 'entry_confirmation_setup', 'all'),
            'entry_confirmation_min_favorable_pct': float(getattr(args, 'entry_confirmation_min_favorable_pct', 0.0) or 0.0),
            'entry_confirmation_adverse_pct': float(getattr(args, 'entry_confirmation_adverse_pct', 0.0) or 0.0),
            'entry_confirmation_session_phase': getattr(args, 'entry_confirmation_session_phase', 'all'),
            'entry_confirmation_edge_threshold_pct': getattr(args, 'entry_confirmation_edge_threshold_pct', None),
            'entry_confirmation_rel_threshold_pct': getattr(args, 'entry_confirmation_rel_threshold_pct', None),
            'path_failure_after_sec': int(float(getattr(args, 'path_failure_after_sec', 0.0) or 0.0)),
            'path_failure_edge_threshold_pct': float(getattr(args, 'path_failure_edge_threshold_pct', 0.0) or 0.0),
            'path_failure_action': getattr(args, 'path_failure_action', 'off'),
            'path_failure_setup': getattr(args, 'path_failure_setup', 'all'),
            'path_failure_session_phase': getattr(args, 'path_failure_session_phase', 'all'),
            'path_failure_rel_threshold_pct': getattr(args, 'path_failure_rel_threshold_pct', None),
            'path_failure_window_sec': int(float(getattr(args, 'path_failure_window_sec', 0.0) or 0.0)),
            'path_failure_reduce_fraction': float(getattr(args, 'path_failure_reduce_fraction', 0.0) or 0.0),
            'giveback_exit_after_sec': int(float(getattr(args, 'giveback_exit_after_sec', 0.0) or 0.0)),
            'giveback_exit_min_mfe_pct': float(getattr(args, 'giveback_exit_min_mfe_pct', 0.0) or 0.0),
            'giveback_exit_giveback_pct': float(getattr(args, 'giveback_exit_giveback_pct', 0.0) or 0.0),
            'giveback_exit_max_pnl_pct': float(getattr(args, 'giveback_exit_max_pnl_pct', 0.0) or 0.0),
            'giveback_exit_confirm_sec': int(float(getattr(args, 'giveback_exit_confirm_sec', 0.0) or 0.0)),
            'giveback_exit_action': getattr(args, 'giveback_exit_action', 'off'),
            'giveback_exit_setup': getattr(args, 'giveback_exit_setup', 'all'),
            'giveback_exit_cooldown_sec': int(float(getattr(args, 'giveback_exit_cooldown_sec', 0.0) or 0.0)),
            'validation_mode': 'finalist',
            'indicator_mode': getattr(args, 'indicator_mode', 'fast'),
        },
        'data_fingerprint': replay_artifacts.artifact_fingerprint(args, day).get('fingerprint'),
        'code_hashes': {
            'multi_profile_full_replay.py': replay._file_sha256(os.path.join(HERE, 'multi_profile_full_replay.py')),
            'backtest_30d_engine.py': replay._file_sha256(os.path.join(HERE, 'backtest_30d_engine.py')),
            'ws_scalp.py': replay._file_sha256(os.path.join(HERE, 'ws_scalp.py')),
            'scoring_profiles.py': replay._file_sha256(os.path.join(HERE, 'scoring_profiles.py')),
            'trading_config.json': replay._file_sha256(os.path.join(HERE, 'trading_config.json')),
        },
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('utf-8')).hexdigest()


def _day_shard_path(args, day, profile_specs: list[dict]) -> str:
    key = _day_shard_fingerprint(args, day, profile_specs)[:24]
    return os.path.join(args.out_dir, 'day_shards', f'{day.isoformat()}_{key}.json')


def _stats_payload(stats: replay.ReplayStats) -> dict:
    return {
        'fetched': dict(stats.fetched),
        'cache_hits': dict(stats.cache_hits),
        'skipped_signals': dict(stats.skipped_signals),
        'timing': dict(stats.timing),
        'emitted_signals': stats.emitted_signals,
        'seconds': stats.seconds,
    }


def _stats_from_payload(payload: dict) -> replay.ReplayStats:
    stats = replay.ReplayStats()
    stats.fetched.update(payload.get('fetched') or {})
    stats.cache_hits.update(payload.get('cache_hits') or {})
    stats.skipped_signals.update(payload.get('skipped_signals') or {})
    stats.timing.update(payload.get('timing') or {})
    stats.emitted_signals = int(payload.get('emitted_signals') or 0)
    stats.seconds = int(payload.get('seconds') or 0)
    return stats


def _merge_stats(dst: replay.ReplayStats, payload: dict) -> None:
    dst.seconds += int(payload.get('seconds') or 0)
    dst.emitted_signals += int(payload.get('emitted_signals') or 0)
    dst.cache_hits.update(payload.get('cache_hits') or {})
    dst.fetched.update(payload.get('fetched') or {})
    dst.skipped_signals.update(payload.get('skipped_signals') or {})
    dst.timing.update(payload.get('timing') or {})


def _day_profile_payload(day, profile_state: ProfileState) -> dict:
    return {
        'day': day.isoformat(),
        'variant': profile_state.variant,
        'profile': profile_state.profile_path,
        'start_balance': round(float(getattr(profile_state, 'day_start_balance', 0.0) or 0.0), 4),
        'ending_balance': round(float(profile_state.balance), 4),
        'pnl': round(float(profile_state.balance) - float(getattr(profile_state, 'day_start_balance', 0.0) or 0.0), 4),
        'rows': profile_state.rows,
        'stats': _stats_payload(profile_state.stats),
    }


def _run_day_multi(day, args, clock: replay.ReplayClock, profiles: list[ProfileState]) -> None:
    _start_iso, _end_iso, start_sec, end_sec = replay._session_bounds_utc(day)
    flatten_ts = replay._flatten_ts(day)
    entry_cutoff = replay._entry_cutoff_ts(day)
    for profile_state in profiles:
        profile_state.setup_states = {}
        profile_state.last_signal_ts = {}
        profile_state.open_positions = {}
        profile_state.pending_entries = {}
        profile_state.opportunity_seq = 0
        profile_state.day_trade_count = 0
        profile_state.ticker_day_trade_count = {}
        profile_state.day_start_balance = float(profile_state.balance)
    states = {sym: replay._state(sym) for sym in list(args.tickers) + [replay.BTC_SYMBOL]}
    load_stats = replay.ReplayStats()
    events = replay._load_day_events(day, args, load_stats)
    by_sec: dict[int, list[dict]] = defaultdict(list)
    for event in events:
        sec = int(event['t']) // 1000
        if start_sec <= sec < end_sec:
            by_sec[sec].append(event)
    for profile_state in profiles:
        _copy_counter(profile_state.stats.cache_hits, load_stats.cache_hits)
        _copy_counter(profile_state.stats.fetched, load_stats.fetched)

    compute_indicators = (
        replay._replay_compute_indicators
        if getattr(args, 'indicator_mode', 'live') == 'fast'
        else ws_scalp.compute_indicators
    )
    finalist_mode = replay._finalist_mode(args)

    for sec in range(start_sec, end_sec):
        clock.now = sec + 0.999
        for profile_state in profiles:
            profile_state.stats.seconds += 1
        for event in by_sec.get(sec, []):
            st = states.get(event['symbol'])
            if not st:
                continue
            if event['kind'] == 'stock_trade':
                replay._feed_stock_trade(st, event['symbol'], event['row'])
            elif event['kind'] == 'stock_quote':
                replay._feed_stock_quote(st, event['row'])
            elif event['kind'] == 'btc_synth_trade':
                replay._feed_btc_synth(st, event['row'])
        for st in states.values():
            replay._roll_bar(st, sec)

        exit_miner_indicators = None
        if any(
            replay._exit_management_value(pos, args, 'giveback_exit_miner_max_pct', None) is not None
            for profile_state in profiles
            for pos in profile_state.open_positions.values()
        ):
            exit_miner_indicators = {
                t: compute_indicators(states[t])
                for t in args.tickers
                if len(states[t].bars_1s) >= 60
            }
        for profile_state in profiles:
            for tkr, pos in list(profile_state.open_positions.items()):
                price = states[tkr].last_trade_price
                replay._mark_position(pos, price)
                reason, exit_price = replay._exit_reason(
                    pos, price, sec, flatten_ts, args,
                    btc_price=states[replay.BTC_SYMBOL].last_trade_price,
                    miner_indicators=exit_miner_indicators,
                )
                if reason and exit_price is not None:
                    if reason == 'path_failure_reduce':
                        reduce_fraction = max(0.0, min(1.0, float(getattr(args, 'path_failure_reduce_fraction', 0.0) or 0.0)))
                        if reduce_fraction <= 0.0:
                            continue
                        partial = replay._partial_position(pos, reduce_fraction)
                        row = replay._close_position(partial, reason, float(exit_price), sec)
                        profile_state.rows.append(row)
                        profile_state.balance += row['pnl']
                        pos.qty *= (1.0 - reduce_fraction)
                        pos.alloc = round(pos.alloc * (1.0 - reduce_fraction), 2)
                        pos.signal['path_failure_reduced'] = True
                        pos.signal['path_failure_reduce_fraction'] = round(reduce_fraction, 6)
                        if pos.qty <= 1e-9 or pos.alloc <= 0.0:
                            profile_state.open_positions.pop(tkr, None)
                        continue
                    row = replay._close_position(pos, reason, float(exit_price), sec)
                    profile_state.rows.append(row)
                    profile_state.balance += row['pnl']
                    profile_state.open_positions.pop(tkr, None)
                    if reason in ('giveback_exit', 'giveback_exit_flip'):
                        cooldown_sec = int(float(replay._exit_management_value(pos, args, 'giveback_exit_cooldown_sec', 0.0) or 0.0))
                        if cooldown_sec > 0:
                            profile_state.giveback_cooldown_until[tkr] = sec + cooldown_sec
                    stop_loss_tickers = {
                        t.strip().upper()
                        for t in str(getattr(args, 'stop_loss_cooldown_tickers', '') or '').split(',')
                        if t.strip()
                    }
                    stop_loss_cooldown_sec = int(float(getattr(args, 'stop_loss_cooldown_sec', 0) or 0))
                    if reason == 'stop_loss' and stop_loss_cooldown_sec > 0 and tkr.upper() in stop_loss_tickers:
                        profile_state.stop_loss_cooldown_until[tkr] = sec + stop_loss_cooldown_sec
                    if reason in ('path_failure_flip', 'giveback_exit_flip'):
                        flip_sig = replay._flip_signal_from_position(pos, float(exit_price), sec, reason)
                        flip_sig['path_failure_entry_btc_price'] = float(states[replay.BTC_SYMBOL].last_trade_price or 0.0)
                        profile_state.open_positions[tkr] = replay._open_position(flip_sig, profile_state.balance, args)
            for tkr, pending in list(profile_state.pending_entries.items()):
                cur_price = states[tkr].last_trade_price
                if cur_price is not None:
                    pending['best_price'] = max(float(pending.get('best_price') or cur_price), float(cur_price))
                    pending['worst_price'] = min(float(pending.get('worst_price') or cur_price), float(cur_price))
                if sec < int(pending.get('due_ts') or 0):
                    continue
                result, resolved_sig = replay._resolve_pending_entry(
                    pending, states[tkr].last_trade_price, args,
                    current_btc_price=states[replay.BTC_SYMBOL].last_trade_price,
                )
                if result == 'enter' and resolved_sig is not None:
                    resolved_sig['path_failure_entry_btc_price'] = float(states[replay.BTC_SYMBOL].last_trade_price or 0.0)
                    profile_state.open_positions[tkr] = replay._open_position(resolved_sig, profile_state.balance, args)
                    profile_state.day_trade_count += 1
                    profile_state.ticker_day_trade_count[tkr] = int(profile_state.ticker_day_trade_count.get(tkr, 0) or 0) + 1
                else:
                    profile_state.stats.skipped_signals[f'entry_confirmation_{result}'] += 1
                profile_state.pending_entries.pop(tkr, None)

        if sec > entry_cutoff:
            continue

        active_profiles = []
        for profile_state in profiles:
            max_day_trades = int(getattr(args, 'max_trades_per_day', 0) or 0)
            if max_day_trades > 0 and profile_state.day_trade_count >= max_day_trades:
                profile_state.stats.skipped_signals['mock_density_cap'] += 1
                continue
            if (
                not getattr(args, 'disable_all_open_signal_skip', False)
                and len(profile_state.open_positions) + len(profile_state.pending_entries) >= len(args.tickers)
            ):
                profile_state.stats.skipped_signals['all_tickers_open_skip_signal_pass'] += 1
                continue
            active_profiles.append(profile_state)
        if not active_profiles:
            continue

        btc_ind = compute_indicators(states[replay.BTC_SYMBOL]) if args.btc_mode != 'off' else {
            'ready': True,
            'ema_stack': 'mixed',
            'last_trade_age_sec': 0,
            'last_quote_age_sec': 0,
            'mom_5s': 0,
            'mom_15s': 0,
            'mom_30s': 0,
            'mom_60s': 0,
            'mom_180s': 0,
        }
        stock_indicators = {}
        for tkr in args.tickers:
            if len(states[tkr].bars_1s) >= 60:
                stock_indicators[tkr] = compute_indicators(states[tkr])

        for tkr, ind in stock_indicators.items():
            eligible = [
                p for p in active_profiles
                if tkr not in p.open_positions and tkr not in p.pending_entries
                and sec >= int(p.giveback_cooldown_until.get(tkr, 0) or 0)
                and sec >= int(p.stop_loss_cooldown_until.get(tkr, 0) or 0)
                and (
                    int(getattr(args, 'max_trades_per_ticker_day', 0) or 0) <= 0
                    or int(p.ticker_day_trade_count.get(tkr, 0) or 0) < int(getattr(args, 'max_trades_per_ticker_day', 0) or 0)
                )
            ]
            if not eligible:
                continue
            base_sig = ws_scalp.detect_signal(tkr, ind, btc_ind, miner_indicators=stock_indicators)
            if not base_sig:
                continue
            for profile_state in eligible:
                sig = replay._json_clone(base_sig)
                sig = scoring_profiles.apply_profile(
                    profile_state.profile,
                    sig,
                    ind,
                    btc_ind,
                    include_details=not finalist_mode,
                )
                sig['ts'] = sec
                opp_id = (
                    f"{day.isoformat()}:{tkr}:{sec}:{sig.get('side')}:"
                    f"{sig.get('setup_type', 'unknown')}:{profile_state.opportunity_seq}"
                )
                profile_state.opportunity_seq += 1
                sig['opportunity_id'] = opp_id
                throttle_reason = replay._long_throttle_reason(args, sig, ind, btc_ind, stock_indicators)
                if throttle_reason:
                    profile_state.stats.skipped_signals[f'regime_long_throttle:{throttle_reason}'] += 1
                    continue
                mock_guard_reason = replay._step2_brs_long_btc_negative_mom60_reason(sig, btc_ind)
                if mock_guard_reason:
                    profile_state.stats.skipped_signals[mock_guard_reason] += 1
                    continue
                warning_reason = replay._entry_warning_gate_reason(args, tkr, sig, ind)
                if warning_reason and (getattr(args, 'entry_warning_gate_action', 'veto') or 'veto') == 'veto':
                    profile_state.stats.skipped_signals[f'entry_warning_gate:{warning_reason}'] += 1
                    continue
                if bool(getattr(args, 'require_conviction', False)) and sig.get('conviction') not in replay.MIN_CONVICTION:
                    profile_state.stats.skipped_signals['below_min_conviction'] += 1
                    continue
                if not replay._setup_state_gate(profile_state.setup_states, tkr, sig, ind, clock.now):
                    profile_state.stats.skipped_signals['setup_state_pending'] += 1
                    continue
                key = f"{tkr}:{sig['side']}:{sig.get('setup_type', 'unknown')}"
                cooldown = float(ws_scalp.SETUP_COOLDOWN_SEC.get(sig.get('setup_type'), 30))
                if clock.now - float(profile_state.last_signal_ts.get(key, 0) or 0) < cooldown:
                    profile_state.stats.skipped_signals['cooldown'] += 1
                    continue
                sig = replay._decorate_signal(sig, states[tkr], ind, include_shadow=not finalist_mode)
                if float((sig.get('execution_quality') or {}).get('score') or 0) < ws_scalp.EXECUTION_QUALITY_MIN:
                    profile_state.stats.skipped_signals['execution_quality_low'] += 1
                    continue
                profile_state.last_signal_ts[key] = clock.now
                profile_state.stats.emitted_signals += 1
                if replay._entry_confirmation_enabled(args, sig, ind, tkr):
                    profile_state.pending_entries[tkr] = {
                        'sig': replay._json_clone(sig),
                        'due_ts': sec + int(float(getattr(args, 'entry_confirmation_delay_sec', 0.0) or 0.0)),
                        'signal_price': float(sig.get('price') or states[tkr].last_trade_price or 0.0),
                        'signal_btc_price': float(states[replay.BTC_SYMBOL].last_trade_price or 0.0),
                        'best_price': float(sig.get('price') or states[tkr].last_trade_price or 0.0),
                        'worst_price': float(sig.get('price') or states[tkr].last_trade_price or 0.0),
                    }
                else:
                    sig['path_failure_entry_btc_price'] = float(states[replay.BTC_SYMBOL].last_trade_price or 0.0)
                    profile_state.open_positions[tkr] = replay._open_position(sig, profile_state.balance, args)
                    profile_state.day_trade_count += 1
                    profile_state.ticker_day_trade_count[tkr] = int(profile_state.ticker_day_trade_count.get(tkr, 0) or 0) + 1

    for profile_state in profiles:
        profile_state.pending_entries.clear()
        for tkr, pos in list(profile_state.open_positions.items()):
            price = states[tkr].last_trade_price or pos.entry_price
            row = replay._close_position(pos, 'end_of_data', float(price), end_sec)
            profile_state.rows.append(row)
            profile_state.balance += row['pnl']
        profile_state.open_positions.clear()


def _run_multi(args, profile_states: list[ProfileState]) -> list[dict]:
    clock = replay.ReplayClock()
    original_time = ws_scalp.time.time
    original_near_signal_logger = getattr(ws_scalp, '_log_near_signal', None)
    original_active_scoring_profile = getattr(ws_scalp, 'ACTIVE_SCORING_PROFILE', None)
    ws_scalp.time.time = clock.time
    ws_scalp.ACTIVE_SCORING_PROFILE = {}
    if original_near_signal_logger is not None:
        ws_scalp._log_near_signal = lambda *a, **kw: None
    try:
        for day in _selected_days(args):
            _run_day_multi(day, args, clock, profile_states)
            print(json.dumps({
                'event': 'multi_day_done',
                'day': day.isoformat(),
                'profiles': len(profile_states),
            }, sort_keys=True), flush=True)
    finally:
        ws_scalp.time.time = original_time
        ws_scalp.ACTIVE_SCORING_PROFILE = original_active_scoring_profile
        if original_near_signal_logger is not None:
            ws_scalp._log_near_signal = original_near_signal_logger

    results = []
    for profile_state in profile_states:
        summary_args = argparse.Namespace(**vars(args))
        summary_args._scoring_profile_data = profile_state.profile
        summary = replay._summarize(profile_state.rows, profile_state.stats, summary_args, profile_state.balance)
        csv_path, json_path = _write_outputs(profile_state.rows, summary, args, profile_state.variant)
        results.append({
            'variant': profile_state.variant,
            'profile': profile_state.profile_path,
            'summary': summary,
            'csv': csv_path,
            'summary_path': json_path,
        })
    _annotate_result_hashes(results)
    results.sort(key=lambda row: float((row.get('summary') or {}).get('pnl') or 0.0), reverse=True)
    return results


def _run_day_shard_worker(args_dict: dict, day_iso: str, profile_specs: list[dict]) -> dict:
    args = argparse.Namespace(**args_dict)
    day = replay._parse_day(day_iso)
    checkpoint_path = _day_shard_path(args, day, profile_specs)
    expected_fingerprint = _day_shard_fingerprint(args, day, profile_specs)
    if getattr(args, 'multi_profile_resume_days', False) and os.path.exists(checkpoint_path):
        with open(checkpoint_path, 'r', encoding='utf-8') as f:
            payload = json.load(f)
        if payload.get('fingerprint') == expected_fingerprint:
            payload['resumed'] = True
            return payload
    clock = replay.ReplayClock()
    original_time = ws_scalp.time.time
    original_near_signal_logger = getattr(ws_scalp, '_log_near_signal', None)
    original_active_scoring_profile = getattr(ws_scalp, 'ACTIVE_SCORING_PROFILE', None)
    ws_scalp.time.time = clock.time
    ws_scalp.ACTIVE_SCORING_PROFILE = {}
    if original_near_signal_logger is not None:
        ws_scalp._log_near_signal = lambda *a, **kw: None
    try:
        states = [
            ProfileState(
                variant=spec['variant'],
                profile_path=spec['profile_path'],
                profile=spec['profile'],
                balance=float(args.start_balance),
            )
            for spec in profile_specs
        ]
        _run_day_multi(day, args, clock, states)
    finally:
        ws_scalp.time.time = original_time
        ws_scalp.ACTIVE_SCORING_PROFILE = original_active_scoring_profile
        if original_near_signal_logger is not None:
            ws_scalp._log_near_signal = original_near_signal_logger
    payload = {
        'schema_version': 1,
        'day': day.isoformat(),
        'fingerprint': expected_fingerprint,
        'profiles_sha256': _profile_specs_hash(profile_specs),
        'profiles': {
            state.variant: _day_profile_payload(day, state)
            for state in states
        },
    }
    if getattr(args, 'multi_profile_resume_days', False):
        os.makedirs(os.path.dirname(checkpoint_path), exist_ok=True)
        tmp = checkpoint_path + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(payload, f, separators=(',', ':'), sort_keys=True)
        os.replace(tmp, checkpoint_path)
        payload['checkpoint_path'] = checkpoint_path
    return payload


def _compound_day_shards(args, profile_specs: list[dict], day_payloads: dict[str, dict]) -> list[dict]:
    profile_rows: dict[str, list[dict]] = {spec['variant']: [] for spec in profile_specs}
    profile_stats: dict[str, replay.ReplayStats] = {spec['variant']: replay.ReplayStats() for spec in profile_specs}
    profile_balances: dict[str, float] = {spec['variant']: float(args.start_balance) for spec in profile_specs}
    profiles_by_variant = {spec['variant']: spec for spec in profile_specs}
    days = _selected_days(args)
    for day in days:
        payload = day_payloads[day.isoformat()]
        for variant, day_profile in (payload.get('profiles') or {}).items():
            current_balance = profile_balances[variant]
            rows = day_profile.get('rows') or []
            day_start = float(day_profile.get('start_balance') or args.start_balance)
            day_end = float(day_profile.get('ending_balance') or day_start)
            factor = current_balance / day_start if day_start else 1.0
            scaled_rows = replay._scale_day_rows(rows, factor)
            profile_rows[variant].extend(scaled_rows)
            profile_balances[variant] = current_balance + (day_end - day_start) * factor
            _merge_stats(profile_stats[variant], day_profile.get('stats') or {})
    results = []
    for variant, rows in profile_rows.items():
        spec = profiles_by_variant[variant]
        summary_args = argparse.Namespace(**vars(args))
        summary_args._scoring_profile_data = spec['profile']
        summary = replay._summarize(rows, profile_stats[variant], summary_args, profile_balances[variant])
        csv_path, json_path = _write_outputs(rows, summary, args, variant)
        results.append({
            'variant': variant,
            'profile': spec['profile_path'],
            'summary': summary,
            'csv': csv_path,
            'summary_path': json_path,
        })
    _annotate_result_hashes(results)
    results.sort(key=lambda row: float((row.get('summary') or {}).get('pnl') or 0.0), reverse=True)
    return results


def _run_multi_day_sharded(args, profile_specs: list[dict]) -> list[dict]:
    days = _selected_days(args)
    args_dict = vars(args).copy()
    day_payloads: dict[str, dict] = {}
    day_workers = min(max(1, int(getattr(args, 'multi_profile_day_workers', 1) or 1)), len(days))
    if day_workers <= 1:
        for day in days:
            payload = _run_day_shard_worker(args_dict, day.isoformat(), profile_specs)
            day_payloads[day.isoformat()] = payload
            print(json.dumps({
                'event': 'multi_day_shard_done',
                'day': day.isoformat(),
                'profiles': len(profile_specs),
                'resumed': bool(payload.get('resumed')),
            }, sort_keys=True), flush=True)
    else:
        try:
            with ProcessPoolExecutor(max_workers=day_workers) as pool:
                futures = {
                    pool.submit(_run_day_shard_worker, args_dict, day.isoformat(), profile_specs): day
                    for day in days
                }
                for fut in as_completed(futures):
                    day = futures[fut]
                    payload = fut.result()
                    day_payloads[day.isoformat()] = payload
                    print(json.dumps({
                        'event': 'multi_day_shard_done',
                        'day': day.isoformat(),
                        'profiles': len(profile_specs),
                        'resumed': bool(payload.get('resumed')),
                    }, sort_keys=True), flush=True)
        except PermissionError as exc:
            print(json.dumps({
                'event': 'multi_day_shard_parallel_fallback',
                'reason': repr(exc),
            }, sort_keys=True), flush=True)
            for day in days:
                payload = _run_day_shard_worker(args_dict, day.isoformat(), profile_specs)
                day_payloads[day.isoformat()] = payload
    return _compound_day_shards(args, profile_specs, day_payloads)


def _rows_signature(rows: list[dict]) -> list[tuple]:
    return [
        (
            row.get('opportunity_id'),
            row.get('entry_ct'),
            row.get('exit_ct'),
            row.get('ticker'),
            row.get('side'),
            row.get('entry'),
            row.get('exit'),
            row.get('qty'),
            row.get('alloc'),
            row.get('pnl'),
            row.get('reason'),
        )
        for row in rows
    ]


def _load_csv_rows(path: str) -> list[dict]:
    with open(path, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def _select_parity_rows(rows: list[dict], multi_results: list[dict], limit: int) -> list[dict]:
    if limit <= 0:
        return []
    by_variant = {row.get('variant'): row for row in rows}
    selected: list[dict] = []
    seen = set()

    def add_variant(name: str | None) -> None:
        if name and name in by_variant and name not in seen:
            selected.append(by_variant[name])
            seen.add(name)

    if rows:
        add_variant(rows[0].get('variant'))
    if multi_results:
        leader = max(multi_results, key=lambda r: float((r.get('summary') or {}).get('pnl') or -10**12))
        laggard = min(multi_results, key=lambda r: float((r.get('summary') or {}).get('pnl') or 10**12))
        busiest = max(multi_results, key=lambda r: int((r.get('summary') or {}).get('trades') or 0))
        quietest = min(multi_results, key=lambda r: int((r.get('summary') or {}).get('trades') or 10**9))
        for item in (leader, laggard, busiest, quietest):
            add_variant(item.get('variant'))
    for row in sorted(rows, key=lambda r: hashlib.sha256(str(r.get('variant') or '').encode('utf-8')).hexdigest()):
        if len(selected) >= limit:
            break
        add_variant(row.get('variant'))
    return selected[:limit]


def _first_trade_mismatch(single_rows: list[dict], multi_rows: list[dict]) -> dict | None:
    single_sig = _rows_signature(single_rows)
    multi_sig = _rows_signature(multi_rows)
    for idx, (single, multi) in enumerate(zip(single_sig, multi_sig)):
        if single != multi:
            return {
                'index': idx,
                'single': single,
                'multi': multi,
            }
    if len(single_sig) != len(multi_sig):
        return {
            'index': min(len(single_sig), len(multi_sig)),
            'single_len': len(single_sig),
            'multi_len': len(multi_sig),
        }
    return None


def _parity_check(args, rows: list[dict], multi_results: list[dict]) -> dict:
    checks = []
    by_variant = {row['variant']: row for row in multi_results}
    single_root = os.path.join(args.out_dir, 'single_profile_parity')
    selected_rows = _select_parity_rows(rows, multi_results, int(args.parity_check or 0))
    for row in selected_rows:
        variant = _variant_from_row(row)
        profile_path = os.path.join(args.out_dir, 'generated_profiles', f'{tournament._short_id(variant.name)}.json')
        cmd = [
            sys.executable,
            os.path.join(HERE, 'backtest_30d_engine.py'),
            '--start', args.start,
            '--end', args.end,
            '--feed', args.feed,
            '--quote-mode', args.quote_mode,
            '--btc-mode', args.btc_mode,
            '--tickers',
            *args.tickers,
            '--start-balance', str(args.start_balance),
            '--use-prepared-events',
            '--validation-mode', 'finalist',
            '--indicator-mode', getattr(args, 'indicator_mode', 'fast'),
            '--workers', '1',
            '--scoring-profile', profile_path,
            '--out-dir', os.path.join(single_root, tournament._short_id(variant.name)),
        ]
        if getattr(args, 'sample_days', ''):
            cmd.extend(['--sample-days', args.sample_days])
        if args.entry_gate_mode:
            cmd.extend(['--replay-regime-long-throttle', args.entry_gate_mode])
        if args.entry_gate_threshold_pct is not None:
            cmd.extend(['--replay-regime-threshold-pct', str(args.entry_gate_threshold_pct)])
        proc = subprocess.run(cmd, cwd=HERE, capture_output=True, text=True)
        single_dir = os.path.join(single_root, tournament._short_id(variant.name))
        stem = f"engine_replay_{args.start}_{args.end}_{tournament._short_id(variant.name)}_finalist_fast"
        single_csv = os.path.join(single_dir, f'{stem}_trades.csv')
        single_json = os.path.join(single_dir, f'{stem}_summary.json')
        parity_cache = _restore_parity_cache(args, variant, profile_path, single_json, single_csv)
        proc_returncode = 0
        proc_reused_cache = bool(parity_cache)
        if parity_cache is None:
            proc = subprocess.run(cmd, cwd=HERE, capture_output=True, text=True)
            proc_returncode = proc.returncode
            if proc.returncode == 0:
                parity_cache = _store_parity_cache(args, variant, profile_path, single_json, single_csv)
        else:
            proc = None
        single_summary = {}
        if os.path.exists(single_json):
            with open(single_json, 'r', encoding='utf-8') as f:
                single_summary = json.load(f)
        multi = by_variant.get(variant.name) or {}
        multi_summary = multi.get('summary') or {}
        single_rows = _load_csv_rows(single_csv) if os.path.exists(single_csv) else []
        multi_rows = _load_csv_rows(multi.get('csv')) if multi.get('csv') else []
        summary_match = {
            'trades': single_summary.get('trades') == multi_summary.get('trades'),
            'wins': single_summary.get('wins') == multi_summary.get('wins'),
            'losses': single_summary.get('losses') == multi_summary.get('losses'),
            'pnl': single_summary.get('pnl') == multi_summary.get('pnl'),
            'ending_balance': single_summary.get('ending_balance') == multi_summary.get('ending_balance'),
        }
        single_pnl = float(single_summary.get('pnl') or 0.0)
        multi_pnl = float(multi_summary.get('pnl') or 0.0)
        single_balance = float(single_summary.get('ending_balance') or 0.0)
        multi_balance = float(multi_summary.get('ending_balance') or 0.0)
        trade_rows_match = _rows_signature(single_rows) == _rows_signature(multi_rows)
        checks.append({
            'variant': variant.name,
            'single_returncode': proc_returncode,
            'single_reused_parity_cache': proc_reused_cache,
            'single_parity_cache': parity_cache,
            'single_command': cmd,
            'summary_match': summary_match,
            'trade_rows_match': trade_rows_match,
            'single_trade_rows_sha256': _trade_rows_hash(single_rows),
            'multi_trade_rows_sha256': _trade_rows_hash(multi_rows),
            'delta': {
                'pnl': round(multi_pnl - single_pnl, 8),
                'ending_balance': round(multi_balance - single_balance, 8),
                'trades': int(multi_summary.get('trades') or 0) - int(single_summary.get('trades') or 0),
            },
            'first_trade_mismatch': None if trade_rows_match else _first_trade_mismatch(single_rows, multi_rows),
            'single_summary': single_summary,
            'multi_summary': multi_summary,
        })
    return {
        'schema_version': 1,
        'checked': len(checks),
        'selection': [row.get('variant') for row in selected_rows],
        'passed': all(
            check.get('single_returncode') == 0
            and all((check.get('summary_match') or {}).values())
            and check.get('trade_rows_match')
            for check in checks
        ),
        'checks': checks,
    }


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Opt-in shared-pass full replay for multiple scoring profiles.')
    ap.add_argument('--input', required=True)
    ap.add_argument('--out-dir', default=DEFAULT_OUT_DIR)
    ap.add_argument('--name', default='multi_profile_run')
    ap.add_argument('--start', default='2026-04-06')
    ap.add_argument('--end', default='2026-05-01')
    ap.add_argument('--sample-days', default='',
                    help='Comma-separated YYYY-MM-DD sample for live/accuracy validation. Empty uses every market day.')
    ap.add_argument('--tickers', nargs='+', default=replay.TICKERS)
    ap.add_argument('--feed', default='sip')
    ap.add_argument('--quote-mode', default='per-second')
    ap.add_argument('--btc-mode', default='bars')
    ap.add_argument('--start-balance', type=float, default=100000.0)
    ap.add_argument('--limit', type=int, default=10)
    ap.add_argument('--cache-dir', default=replay.DEFAULT_CACHE_DIR)
    ap.add_argument('--prepared-cache-dir', default=replay.DEFAULT_PREPARED_DIR)
    ap.add_argument('--indicator-mode', choices=['fast', 'live'], default='fast')
    ap.add_argument('--entry-gate-mode', default=None)
    ap.add_argument('--entry-gate-threshold-pct', type=float, default=None)
    ap.add_argument('--brs-tp-multiplier', type=float, default=1.0)
    ap.add_argument('--momentum-tp-multiplier', type=float, default=1.0)
    ap.add_argument('--high-conviction-tp-multiplier', type=float, default=1.0)
    ap.add_argument('--require-conviction', action='store_true')
    ap.add_argument('--disable-conditional-stop', action='store_true')
    ap.add_argument('--entry-confirmation-delay-sec', type=int, default=0)
    ap.add_argument('--entry-confirmation-mode', choices=['off', 'confirm', 'skip_adverse', 'flip_adverse', 'skip_path_failure'], default='off')
    ap.add_argument('--entry-confirmation-setup', default='all')
    ap.add_argument('--entry-confirmation-min-favorable-pct', type=float, default=0.0)
    ap.add_argument('--entry-confirmation-adverse-pct', type=float, default=0.0)
    ap.add_argument('--entry-confirmation-session-phase', default='all')
    ap.add_argument('--entry-confirmation-edge-threshold-pct', type=float, default=None)
    ap.add_argument('--entry-confirmation-rel-threshold-pct', type=float, default=None)
    ap.add_argument('--path-failure-after-sec', type=int, default=0)
    ap.add_argument('--path-failure-edge-threshold-pct', type=float, default=0.0)
    ap.add_argument('--path-failure-action', choices=['off', 'exit', 'flip', 'reduce'], default='off')
    ap.add_argument('--path-failure-setup', default='all')
    ap.add_argument('--path-failure-session-phase', default='all')
    ap.add_argument('--path-failure-rel-threshold-pct', type=float, default=None)
    ap.add_argument('--path-failure-window-sec', type=int, default=0)
    ap.add_argument('--path-failure-reduce-fraction', type=float, default=0.0)
    ap.add_argument('--entry-warning-gate-mode', choices=['off', 'mom60_rel60_veto', 'ticker_chop_veto'], default='off')
    ap.add_argument('--entry-warning-gate-action', choices=['veto', 'confirm_only'], default='veto')
    ap.add_argument('--entry-warning-gate-tickers', default='')
    ap.add_argument('--entry-warning-gate-setup', default='all')
    ap.add_argument('--entry-warning-gate-session-phase', default='all')
    ap.add_argument('--entry-warning-gate-stock-mom60-threshold-pct', type=float, default=-0.10)
    ap.add_argument('--entry-warning-gate-rel60-threshold-pct', type=float, default=-0.10)
    ap.add_argument('--entry-warning-gate-chop-range-threshold-pct', type=float, default=0.75)
    ap.add_argument('--entry-warning-gate-chop-efficiency-threshold', type=float, default=0.20)
    ap.add_argument('--entry-warning-gate-chop-flips-threshold', type=float, default=10)
    ap.add_argument('--entry-warning-gate-max-score', type=float, default=999.0)
    ap.add_argument('--stop-loss-cooldown-tickers', default='')
    ap.add_argument('--stop-loss-cooldown-sec', type=int, default=0)
    ap.add_argument('--giveback-exit-after-sec', type=int, default=0)
    ap.add_argument('--giveback-exit-min-mfe-pct', type=float, default=0.0)
    ap.add_argument('--giveback-exit-giveback-pct', type=float, default=0.0)
    ap.add_argument('--giveback-exit-max-pnl-pct', type=float, default=0.0)
    ap.add_argument('--giveback-exit-confirm-sec', type=int, default=0)
    ap.add_argument('--giveback-exit-action', choices=['off', 'exit', 'flip'], default='off')
    ap.add_argument('--giveback-exit-setup', default='all')
    ap.add_argument('--giveback-exit-cooldown-sec', type=int, default=0)
    ap.add_argument('--parity-check', type=int, default=0,
                    help='Run this many profiles through the existing single-profile replay and compare exact outputs.')
    ap.add_argument('--multi-profile-day-shards', action='store_true',
                    help='Run each market day as an independent shared-pass shard, then compound in date order.')
    ap.add_argument('--multi-profile-day-workers', type=int, default=1,
                    help='Number of independent day shards to run concurrently.')
    ap.add_argument('--multi-profile-resume-days', action='store_true',
                    help='Write/reuse exact day-shard checkpoints keyed by profile/config/code/data fingerprints.')
    ap.add_argument('--force-multi-day-day-shards', action='store_true',
                    help='Allow multi-day day-sharded runs. Without this, day shards are limited to one market day because multi-day parity can fail on compounding details.')
    ap.add_argument('--multi-profile-shared-cache', action='store_true',
                    help='Reuse/store exact shared-pass outputs keyed by profile/config/code/data fingerprints.')
    ap.add_argument('--multi-profile-shared-cache-dir', default=DEFAULT_SHARED_CACHE_DIR)
    ap.add_argument('--multi-profile-parity-cache', action='store_true',
                    help='Reuse/store exact single-profile parity replay outputs keyed by profile/config/code/data fingerprints.')
    ap.add_argument('--multi-profile-parity-cache-dir', default=DEFAULT_PARITY_CACHE_DIR)
    ap.add_argument('--disable-all-open-signal-skip', action='store_true')
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    args.tickers = [t.upper() for t in args.tickers]
    args.validation_mode = 'finalist'
    args.indicator_mode = getattr(args, 'indicator_mode', 'fast') or 'fast'
    args.use_prepared_events = True
    args.write_prepared_events = False
    args.refresh_prepared_events = False
    args.refresh = False
    args.max_pages = 1000
    args.write_opportunity_ledger = False
    args.opportunity_dir = None
    args.resume_days = False
    args.rebuild_missing_opportunities = False
    args.replay_regime_long_throttle = args.entry_gate_mode or 'off'
    args.replay_regime_threshold_pct = args.entry_gate_threshold_pct or 0.0
    args.brs_tp_multiplier = float(getattr(args, 'brs_tp_multiplier', 1.0) or 1.0)
    args.momentum_tp_multiplier = float(getattr(args, 'momentum_tp_multiplier', 1.0) or 1.0)
    args.high_conviction_tp_multiplier = float(getattr(args, 'high_conviction_tp_multiplier', 1.0) or 1.0)
    args.fixed_tp_pct = getattr(args, 'fixed_tp_pct', None)
    args.fixed_sl_pct = getattr(args, 'fixed_sl_pct', None)
    args.entry_confirmation_delay_sec = int(float(getattr(args, 'entry_confirmation_delay_sec', 0.0) or 0.0))
    args.entry_confirmation_mode = getattr(args, 'entry_confirmation_mode', 'off') or 'off'
    args.entry_confirmation_setup = getattr(args, 'entry_confirmation_setup', 'all') or 'all'
    args.entry_confirmation_min_favorable_pct = float(getattr(args, 'entry_confirmation_min_favorable_pct', 0.0) or 0.0)
    args.entry_confirmation_adverse_pct = float(getattr(args, 'entry_confirmation_adverse_pct', 0.0) or 0.0)
    args.entry_confirmation_session_phase = getattr(args, 'entry_confirmation_session_phase', 'all') or 'all'
    args.entry_confirmation_edge_threshold_pct = getattr(args, 'entry_confirmation_edge_threshold_pct', None)
    args.entry_confirmation_rel_threshold_pct = getattr(args, 'entry_confirmation_rel_threshold_pct', None)
    args.path_failure_after_sec = int(float(getattr(args, 'path_failure_after_sec', 0.0) or 0.0))
    args.path_failure_edge_threshold_pct = float(getattr(args, 'path_failure_edge_threshold_pct', 0.0) or 0.0)
    args.path_failure_action = getattr(args, 'path_failure_action', 'off') or 'off'
    args.path_failure_setup = getattr(args, 'path_failure_setup', 'all') or 'all'
    args.path_failure_session_phase = getattr(args, 'path_failure_session_phase', 'all') or 'all'
    args.path_failure_rel_threshold_pct = getattr(args, 'path_failure_rel_threshold_pct', None)
    args.path_failure_window_sec = int(float(getattr(args, 'path_failure_window_sec', 0.0) or 0.0))
    args.path_failure_reduce_fraction = float(getattr(args, 'path_failure_reduce_fraction', 0.0) or 0.0)
    args.entry_warning_gate_mode = getattr(args, 'entry_warning_gate_mode', 'off') or 'off'
    args.entry_warning_gate_action = getattr(args, 'entry_warning_gate_action', 'veto') or 'veto'
    args.entry_warning_gate_tickers = getattr(args, 'entry_warning_gate_tickers', '') or ''
    args.entry_warning_gate_setup = getattr(args, 'entry_warning_gate_setup', 'all') or 'all'
    args.entry_warning_gate_session_phase = getattr(args, 'entry_warning_gate_session_phase', 'all') or 'all'
    args.entry_warning_gate_stock_mom60_threshold_pct = float(getattr(args, 'entry_warning_gate_stock_mom60_threshold_pct', -0.10) or -0.10)
    args.entry_warning_gate_rel60_threshold_pct = float(getattr(args, 'entry_warning_gate_rel60_threshold_pct', -0.10) or -0.10)
    args.entry_warning_gate_chop_range_threshold_pct = float(getattr(args, 'entry_warning_gate_chop_range_threshold_pct', 0.75) or 0.75)
    args.entry_warning_gate_chop_efficiency_threshold = float(getattr(args, 'entry_warning_gate_chop_efficiency_threshold', 0.20) or 0.20)
    args.entry_warning_gate_chop_flips_threshold = float(getattr(args, 'entry_warning_gate_chop_flips_threshold', 10) or 10)
    args.entry_warning_gate_max_score = float(getattr(args, 'entry_warning_gate_max_score', 999.0) or 999.0)
    args.stop_loss_cooldown_tickers = getattr(args, 'stop_loss_cooldown_tickers', '') or ''
    args.stop_loss_cooldown_sec = int(float(getattr(args, 'stop_loss_cooldown_sec', 0) or 0))
    args.giveback_exit_after_sec = int(float(getattr(args, 'giveback_exit_after_sec', 0.0) or 0.0))
    args.giveback_exit_min_mfe_pct = float(getattr(args, 'giveback_exit_min_mfe_pct', 0.0) or 0.0)
    args.giveback_exit_giveback_pct = float(getattr(args, 'giveback_exit_giveback_pct', 0.0) or 0.0)
    args.giveback_exit_max_pnl_pct = float(getattr(args, 'giveback_exit_max_pnl_pct', 0.0) or 0.0)
    args.giveback_exit_confirm_sec = int(float(getattr(args, 'giveback_exit_confirm_sec', 0.0) or 0.0))
    args.giveback_exit_action = getattr(args, 'giveback_exit_action', 'off') or 'off'
    args.giveback_exit_setup = getattr(args, 'giveback_exit_setup', 'all') or 'all'
    args.giveback_exit_cooldown_sec = int(float(getattr(args, 'giveback_exit_cooldown_sec', 0.0) or 0.0))
    args.profile_replay_timing = False

    run_dir = os.path.join(args.out_dir, tournament._safe_name(args.name))
    args.out_dir = run_dir
    os.makedirs(run_dir, exist_ok=True)
    if args.multi_profile_day_shards:
        days = _selected_days(args)
        if len(days) > 1 and not args.force_multi_day_day_shards:
            raise RuntimeError('--multi-profile-day-shards is limited to one market day unless --force-multi-day-day-shards is set.')
    source_rows = _load_rows(args.input, args.limit)
    profile_dir = os.path.join(run_dir, 'generated_profiles')
    profile_states = []
    profile_specs = []
    for row in source_rows:
        variant = _variant_from_row(row)
        profile_path = tournament._write_profile(variant, profile_dir, os.path.abspath(args.input))
        if row.get('exit_management'):
            with open(profile_path, 'r', encoding='utf-8') as f:
                profile_payload = json.load(f)
            profile_payload['exit_management'] = dict(row.get('exit_management') or {})
            with open(profile_path, 'w', encoding='utf-8') as f:
                json.dump(profile_payload, f, indent=2, sort_keys=True)
        profile = scoring_profiles.load_profile(profile_path)
        profile_states.append(ProfileState(
            variant=variant.name,
            profile_path=profile_path,
            profile=profile,
            balance=float(args.start_balance),
        ))
        profile_specs.append({
            'variant': variant.name,
            'profile_path': profile_path,
            'profile': profile,
        })

    started = time.perf_counter()
    method = 'multi_profile_day_sharded_shared_market_indicator_pass' if args.multi_profile_day_shards else 'multi_profile_shared_market_indicator_pass'
    cached = _restore_shared_cache(args, profile_specs, method)
    cache_info = None
    if cached is not None:
        results = cached
        cache_info = {'reused': True}
    elif args.multi_profile_day_shards:
        results = _run_multi_day_sharded(args, profile_specs)
        cache_info = _store_shared_cache(args, profile_specs, method, results)
    else:
        results = _run_multi(args, profile_states)
        cache_info = _store_shared_cache(args, profile_specs, method, results)
    payload = {
        'schema_version': 1,
        'method': method,
        'input': os.path.abspath(args.input),
        'config': vars(args),
        'elapsed_seconds': round(time.perf_counter() - started, 3),
        'results': results,
        'leaderboard': results[:50],
        'cache': cache_info,
    }
    if args.parity_check:
        payload['parity'] = _parity_check(args, source_rows, results)
    out_path = os.path.join(run_dir, 'multi_profile_full_replay_summary.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    print(json.dumps({
        'event': 'done',
        'out': out_path,
        'profiles': len(results),
        'elapsed_seconds': payload['elapsed_seconds'],
        'parity_passed': (payload.get('parity') or {}).get('passed'),
        'top10': [
            {
                'rank': i + 1,
                'variant': row.get('variant'),
                'pnl': (row.get('summary') or {}).get('pnl'),
                'trades': (row.get('summary') or {}).get('trades'),
                'win_rate_pct': (row.get('summary') or {}).get('win_rate_pct'),
            }
            for i, row in enumerate(results[:10])
        ],
    }, indent=2, sort_keys=True), flush=True)
    return 0 if not payload.get('parity') or payload['parity']['passed'] else 2


if __name__ == '__main__':
    import canonical_command_registry as _canonical_commands
    _canonical_commands.enforce_direct_script_allowed(__file__)
    raise SystemExit(main())
