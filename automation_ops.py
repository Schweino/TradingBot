from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import traceback
from datetime import datetime
from typing import Optional
from urllib.request import Request, urlopen

import live_step2_feed
import live_step2_parity_report
import intraday_parity_sentinel
import artifact_version_registry
import artifact_dag_scheduler
import canonical_decision_packet
import canonical_opportunity_ledger
import contract_gate
import architecture_drift_gate
import baseline_drift_sentinel
import certify_step2_cache
import parity_verdict_engine
import candidate_lifecycle
import deterministic_live_replay
import execution_lifecycle
import golden_parity_suite
import intraday_shadow_step2
import live_step2_execution_harness
import market_data_integrity_gate
import market_data_freshness_guard
import order_lifecycle_reconciliation
import daily_parity_scorecard
import shadow_variant_engine
import pre_market_parity_gate
import repair_market_data
import live_market_data_watchdog
import rollback_drill
import run_supervisor
import step2_cache_layers
import step2_latency_model
import worker_policy

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo


HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, 'postmortem')
CT = ZoneInfo('America/Chicago')
PID_PATH = os.path.join(OUT_DIR, 'live_monitor.pid')
RUN_LEDGER_PATH = os.path.join(OUT_DIR, 'automation_run_ledger.json')
APP_URL = 'http://127.0.0.1:5000/mock/status'
PHASES = ('pre-open', 'start-monitor', 'post-open', 'intraday', 'parity-sentinel', 'pre-flat', 'post-close', 'repair-market-data', 'verify-day')
MOCK_REQUIRED_HOUR_CT = 8
MOCK_REQUIRED_MINUTE_CT = 25
REPLAY_TICKERS = ('CLSK', 'MARA', 'RIOT')
REPLAY_FEED = 'sip'
REPLAY_QUOTE_MODE = 'per-second'
REPLAY_BTC_MODE = 'bars'
STEP2_COMPILED_WORKERS = worker_policy.DEFAULT_MAX_WORKERS


def _today() -> str:
    return datetime.now(CT).date().isoformat()


def _run(args: list[str], timeout: int = 120) -> dict:
    try:
        result = run_supervisor.run_and_track(
            args,
            cwd=HERE,
            timeout=timeout,
            name=os.path.splitext(os.path.basename(args[1] if len(args) > 1 else args[0]))[0],
            workers=STEP2_COMPILED_WORKERS,
        )
        result['output'] = str(result.get('output') or '')[-6000:]
        return result
    except subprocess.TimeoutExpired as e:
        output = e.stdout or ''
        if isinstance(output, bytes):
            output = output.decode('utf-8', errors='replace')
        return {
            'command': args,
            'returncode': None,
            'ok': False,
            'timeout_sec': timeout,
            'output': str(output)[-6000:],
            'error': 'timeout_expired',
        }
    except Exception as e:
        return {
            'command': args,
            'returncode': None,
            'ok': False,
            'output': '',
            'error': str(e),
        }


def _read_json(path: str, default=None):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return default


def _write_json(path: str, payload: dict) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, default=str)
    return path


def _run_key(phase: str, day: str) -> str:
    return f'{phase}:{day}'


def _automation_artifact_path(phase: str, day: str) -> str:
    return os.path.join(OUT_DIR, f'automation_{phase}_{day}.json')


def _prepared_replay_tape_path(day: str) -> str:
    ticker_part = '-'.join(REPLAY_TICKERS)
    filename = f'{REPLAY_FEED}_{REPLAY_QUOTE_MODE}_{REPLAY_BTC_MODE}_{ticker_part}_{day}.events.json.gz'
    return os.path.join(HERE, 'data_cache', 'alpaca_engine_replay_tapes', filename)


def _intraday_replay_tape_path(day: str) -> str:
    ticker_part = '-'.join(REPLAY_TICKERS)
    filename = f'{REPLAY_FEED}_{REPLAY_QUOTE_MODE}_{REPLAY_BTC_MODE}_{ticker_part}_{day}.events.json.gz'
    return os.path.join(HERE, 'data_cache', 'live_intraday_tapes', filename)


def _compiled_step2_manifest_path(day: str) -> str:
    ticker_part = '-'.join(REPLAY_TICKERS)
    name = f'compiled_step2_live_mockparity_{ticker_part}_{day}_intraday'
    return os.path.join(OUT_DIR, 'backtests', 'compiled_decision_tapes', name, 'manifest.json')


def _compiled_step2_chunk_manifest_path(day: str) -> str:
    ticker_part = '-'.join(REPLAY_TICKERS)
    name = f'compiled_step2_live_mockparity_{ticker_part}_{day}_intraday'
    return os.path.join(OUT_DIR, 'backtests', 'compiled_decision_tapes', name, 'chunks', 'chunk_manifest.json')


def _compiled_step2_score_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'backtests', 'step2_today_compiled', f'step2_today_compiled_{day}.json')


def _step2_rebuild_plan_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'rebuild_plans', f'step2_rebuild_plan_{day}.json')


def _step2_cache_certification_path(day: str) -> str:
    ticker_part = '-'.join(REPLAY_TICKERS)
    return os.path.join(
        OUT_DIR,
        'cache_certifications',
        f'step2_cache_certification_{day}_{ticker_part}.json',
    )


def _unified_step2_current_trace_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'unified_decision_ledger', day, f'step2_current_trace_{day}.jsonl')


def _unified_step2_current_trace_summary_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'unified_decision_ledger', day, f'step2_current_trace_{day}.summary.json')


def _unified_live_signal_parity_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'unified_decision_ledger', day, f'live_signal_parity_{day}.jsonl')


def _unified_live_signal_parity_summary_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'unified_decision_ledger', day, f'live_signal_parity_{day}.summary.json')


def _unified_live_decision_source_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'unified_decision_ledger', day, f'live_decision_source_{day}.jsonl')


def _canonical_live_decision_packet_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'canonical_decision_packets', day, f'live_decision_packets_{day}.jsonl')


def _canonical_step2_decision_packet_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'canonical_decision_packets', day, f'step2_decision_packets_{day}.jsonl')


def _canonical_live_exit_packet_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'canonical_decision_packets', day, f'live_exit_packets_{day}.jsonl')


def _canonical_packet_diff_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'canonical_decision_packets', day, f'canonical_packet_diff_{day}.json')


def _parity_verdict_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'parity_verdict', f'parity_verdict_{day}.json')


def _parity_verdict_text_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'parity_verdict', f'parity_verdict_{day}.txt')


def _market_data_integrity_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'market_data_integrity', f'market_data_integrity_{day}.json')


def _market_data_integrity_text_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'market_data_integrity', f'market_data_integrity_{day}.txt')


def _market_data_freshness_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'market_data_freshness', f'market_data_freshness_{day}.json')


def _market_data_freshness_text_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'market_data_freshness', f'market_data_freshness_{day}.txt')


def _baseline_drift_sentinel_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'baseline_drift', f'baseline_drift_sentinel_{day}.json')


def _intraday_shadow_step2_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'intraday_shadow_step2', day, f'shadow_step2_{day}.jsonl')


def _intraday_shadow_step2_summary_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'intraday_shadow_step2', day, f'shadow_step2_{day}.summary.json')


def _shadow_variant_decisions_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'shadow_variants', day, f'shadow_variant_decisions_{day}.jsonl')


def _shadow_variant_leaderboard_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'shadow_variants', day, f'shadow_variant_leaderboard_{day}.json')


def _deterministic_live_replay_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'deterministic_live_replay', f'deterministic_live_replay_{day}.json')


def _architecture_drift_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'architecture_drift', f'architecture_drift_gate_{day}.json')


def _candidate_lifecycle_dashboard_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'candidate_lifecycle', f'candidate_lifecycle_dashboard_{day}.json')


def _candidate_lifecycle_registry_path() -> str:
    return os.path.join(OUT_DIR, 'candidate_lifecycle', 'candidate_lifecycle_registry.json')


def _execution_lifecycle_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'execution_lifecycle', day, f'execution_lifecycle_{day}.jsonl')


def _execution_lifecycle_summary_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'execution_lifecycle', day, f'execution_lifecycle_{day}.summary.json')


def _order_lifecycle_reconciliation_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'order_lifecycle_reconciliation', f'order_lifecycle_reconciliation_{day}.json')


def _order_lifecycle_reconciliation_text_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'order_lifecycle_reconciliation', f'order_lifecycle_reconciliation_{day}.txt')


def _daily_parity_scorecard_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'daily_parity_scorecard', f'daily_parity_scorecard_{day}.json')


def _daily_parity_scorecard_text_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'daily_parity_scorecard', f'daily_parity_scorecard_{day}.txt')


def _artifact_dag_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'artifact_dag', f'artifact_dag_{day}.json')


def _contract_gate_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'contract_gate', f'contract_gate_{day}.json')


def _golden_parity_path() -> str:
    return os.path.join(OUT_DIR, 'golden_parity', 'golden_parity_all.json')


def _run_supervisor_cleanup_path(day: str, phase: str) -> str:
    return os.path.join(OUT_DIR, 'runtime_supervisor', f'run_supervisor_cleanup_{phase}_{day}.json')


def _pre_market_parity_gate_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'pre_market_parity_gate', f'pre_market_parity_gate_{day}.json')


def _execution_contract_harness_path(day: str) -> str:
    return os.path.join(OUT_DIR, 'execution_contract_harness', f'execution_contract_harness_{day}.json')


def _rollback_drill_latest_path() -> str:
    return os.path.join(OUT_DIR, 'rollback_drills', 'ROLLBACK_DRILL_LATEST.json')


def _post_close_architecture_required_files(day: str) -> list[str]:
    return [
        os.path.join(OUT_DIR, 'live_step2_parity', f'live_step2_parity_{day}.json'),
        os.path.join(OUT_DIR, 'live_step2_parity', f'live_step2_parity_{day}.txt'),
        os.path.join(OUT_DIR, 'cache_layers', f'step2_cache_layers_{day}.json'),
        _step2_cache_certification_path(day),
        os.path.join(OUT_DIR, 'execution_replay_inputs', f'execution_replay_inputs_{day}.jsonl'),
        os.path.join(OUT_DIR, 'canonical_opportunities', f'canonical_opportunities_{day}.jsonl'),
        os.path.join(OUT_DIR, 'canonical_opportunities', f'canonical_opportunities_{day}.summary.json'),
        _canonical_live_decision_packet_path(day),
        _canonical_step2_decision_packet_path(day),
        _canonical_live_exit_packet_path(day),
        _canonical_packet_diff_path(day),
        _parity_verdict_path(day),
        _parity_verdict_text_path(day),
        _market_data_integrity_path(day),
        _market_data_integrity_text_path(day),
        _market_data_freshness_path(day),
        _market_data_freshness_text_path(day),
        _baseline_drift_sentinel_path(day),
        os.path.join(OUT_DIR, 'parity_diff_classifier', f'parity_diff_classifier_{day}.json'),
        os.path.join(OUT_DIR, 'artifact_registry', f'artifact_registry_{day}.json'),
        os.path.join(OUT_DIR, 'parity_sentinel', f'parity_sentinel_{day}.json'),
        os.path.join(HERE, 'data_cache', 'incremental_market_store', day, 'manifest.json'),
        os.path.join(OUT_DIR, 'step2_decision_parity', f'step2_decision_parity_{day}.jsonl'),
        os.path.join(OUT_DIR, 'step2_decision_parity', f'step2_decision_parity_{day}.summary.json'),
        _compiled_step2_manifest_path(day),
        _compiled_step2_chunk_manifest_path(day),
        _compiled_step2_score_path(day),
        _step2_rebuild_plan_path(day),
        _unified_step2_current_trace_path(day),
        _unified_step2_current_trace_summary_path(day),
        _unified_live_signal_parity_path(day),
        _unified_live_signal_parity_summary_path(day),
        _unified_live_decision_source_path(day),
        _canonical_live_decision_packet_path(day),
        _canonical_step2_decision_packet_path(day),
        _canonical_live_exit_packet_path(day),
        _canonical_packet_diff_path(day),
        _intraday_shadow_step2_path(day),
        _intraday_shadow_step2_summary_path(day),
        _shadow_variant_decisions_path(day),
        _shadow_variant_leaderboard_path(day),
        _candidate_lifecycle_dashboard_path(day),
        _candidate_lifecycle_registry_path(),
        _deterministic_live_replay_path(day),
        _architecture_drift_path(day),
        _execution_lifecycle_path(day),
        _execution_lifecycle_summary_path(day),
        _order_lifecycle_reconciliation_path(day),
        _order_lifecycle_reconciliation_text_path(day),
        _daily_parity_scorecard_path(day),
        _daily_parity_scorecard_text_path(day),
        _artifact_dag_path(day),
        _contract_gate_path(day),
        _execution_contract_harness_path(day),
        _golden_parity_path(),
        _pre_market_parity_gate_path(day),
        _rollback_drill_latest_path(),
        _run_supervisor_cleanup_path(day, 'post-close'),
    ]


def _missing_post_close_architecture_files(day: str) -> list[str]:
    return [path for path in _post_close_architecture_required_files(day) if not os.path.exists(path)]


def _prepare_replay_cache_step(day: str, pure_validation: bool = False) -> dict:
    cmd = [
        sys.executable,
        'prepare_replay_cache.py',
        '--start',
        day,
        '--end',
        day,
        '--tickers',
        *REPLAY_TICKERS,
        '--feed',
        REPLAY_FEED,
        '--quote-mode',
        REPLAY_QUOTE_MODE,
        '--btc-mode',
        REPLAY_BTC_MODE,
        '--workers',
        '1',
        '--build-artifacts',
    ]
    tape_path = _prepared_replay_tape_path(day)
    if pure_validation:
        return {
            'command': cmd,
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_market_data_fetch',
            'expected_prepared_tape': tape_path,
            'prepared_tape_exists': os.path.exists(tape_path),
        }
    result = _run(cmd, timeout=1800)
    result['expected_prepared_tape'] = tape_path
    result['prepared_tape_exists'] = os.path.exists(tape_path)
    if result.get('ok') and not result.get('prepared_tape_exists'):
        result['ok'] = False
        result['error'] = 'prepared_replay_tape_missing_after_success'
    return result


def _canonical_replay_manifest_step(day: str, pure_validation: bool = False) -> dict:
    tape_path = _prepared_replay_tape_path(day)
    manifest_path = tape_path[:-len('.events.json.gz')] + '.manifest.json'
    if pure_validation:
        return {
            'command': ['live_step2_feed', 'manifest', day],
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_manifest_write',
            'expected_manifest': manifest_path,
            'prepared_tape_exists': os.path.exists(tape_path),
            'manifest_exists': os.path.exists(manifest_path),
        }
    try:
        payload = live_step2_feed.write_prepared_manifest(
            day=day,
            tickers=list(REPLAY_TICKERS),
            source='alpaca_historical_eod',
            canonical=True,
            feed=REPLAY_FEED,
            quote_mode=REPLAY_QUOTE_MODE,
            btc_mode=REPLAY_BTC_MODE,
            prepared_cache_dir=os.path.dirname(tape_path),
        )
        return {'command': ['live_step2_feed', 'manifest', day], 'ok': bool(payload.get('prepared_exists')), **payload}
    except Exception as e:
        return {'command': ['live_step2_feed', 'manifest', day], 'ok': False, 'error': str(e)}


def _compare_intraday_to_canonical_step(day: str, pure_validation: bool = False) -> dict:
    canonical_path = _prepared_replay_tape_path(day)
    intraday_path = _intraday_replay_tape_path(day)
    compare_path = os.path.join(OUT_DIR, 'step2_freshness', f'intraday_vs_canonical_{day}.json')
    if pure_validation:
        return {
            'command': ['live_step2_feed', 'compare', day],
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_compare_write',
            'canonical_path': canonical_path,
            'intraday_path': intraday_path,
            'canonical_exists': os.path.exists(canonical_path),
            'intraday_exists': os.path.exists(intraday_path),
            'expected_compare': compare_path,
            'compare_exists': os.path.exists(compare_path),
        }
    try:
        payload = live_step2_feed.compare_prepared(
            day=day,
            tickers=list(REPLAY_TICKERS),
            feed=REPLAY_FEED,
            quote_mode=REPLAY_QUOTE_MODE,
            btc_mode=REPLAY_BTC_MODE,
            intraday_prepared_dir=os.path.dirname(intraday_path),
            canonical_prepared_dir=os.path.dirname(canonical_path),
        )
        ok = bool(payload.get('canonical_exists'))
        return {'command': ['live_step2_feed', 'compare', day], 'ok': ok, **payload}
    except Exception as e:
        return {'command': ['live_step2_feed', 'compare', day], 'ok': False, 'error': str(e)}


def _market_data_integrity_step(day: str, pure_validation: bool = False,
                                source: str = 'auto') -> dict:
    json_path = _market_data_integrity_path(day)
    txt_path = _market_data_integrity_text_path(day)
    if pure_validation:
        return {
            'command': ['market_data_integrity_gate', day, '--source', source],
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_market_data_integrity_write',
            'expected_json': json_path,
            'expected_txt': txt_path,
            'json_exists': os.path.exists(json_path),
            'txt_exists': os.path.exists(txt_path),
        }
    try:
        payload = market_data_integrity_gate.build(
            day=day,
            tickers=list(REPLAY_TICKERS),
            feed=REPLAY_FEED,
            quote_mode=REPLAY_QUOTE_MODE,
            btc_mode=REPLAY_BTC_MODE,
            source=source,
            write=True,
            use_cached=True,
        )
        return {
            'command': ['market_data_integrity_gate', day, '--source', source],
            'ok': bool(payload.get('ok')),
            'promotion_safe': bool(payload.get('promotion_safe')),
            'verdict': payload.get('verdict'),
            'critical_count': payload.get('critical_count'),
            'warning_count': payload.get('warning_count'),
            'path': payload.get('path') or json_path,
            'text_path': payload.get('text_path') or txt_path,
        }
    except Exception as e:
        return {'command': ['market_data_integrity_gate', day, '--source', source], 'ok': False, 'error': str(e)}


def _market_data_freshness_step(day: str, pure_validation: bool = False,
                                source: str = 'auto') -> dict:
    json_path = _market_data_freshness_path(day)
    txt_path = _market_data_freshness_text_path(day)
    if pure_validation:
        return {
            'command': ['market_data_freshness_guard', day, '--source', source],
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_market_data_freshness_write',
            'expected_json': json_path,
            'expected_txt': txt_path,
            'json_exists': os.path.exists(json_path),
            'txt_exists': os.path.exists(txt_path),
        }
    try:
        payload = market_data_freshness_guard.build(
            day=day,
            tickers=list(REPLAY_TICKERS),
            source=source,
            write=True,
            use_cached_integrity=True,
        )
        return {
            'command': ['market_data_freshness_guard', day, '--source', source],
            'ok': bool(payload.get('ok')),
            'verdict': payload.get('verdict'),
            'critical_count': payload.get('critical_count'),
            'warning_count': payload.get('warning_count'),
            'path': payload.get('path') or json_path,
            'text_path': payload.get('text_path') or txt_path,
        }
    except Exception as e:
        return {'command': ['market_data_freshness_guard', day, '--source', source], 'ok': False, 'error': str(e)}


def _market_data_repair_step(day: str, pure_validation: bool = False,
                             download: bool = False) -> dict:
    out_path = os.path.join(OUT_DIR, 'market_data_repairs', f'market_data_repair_{day}.json')
    if pure_validation:
        return {
            'command': ['repair_market_data', day],
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_market_data_repair_write',
            'expected_json': out_path,
            'json_exists': os.path.exists(out_path),
        }
    try:
        args = argparse.Namespace(
            day=day,
            tickers=list(REPLAY_TICKERS),
            feed=REPLAY_FEED,
            quote_mode=REPLAY_QUOTE_MODE,
            btc_mode=REPLAY_BTC_MODE,
            workers=4,
            download=bool(download),
            repair_intraday=True,
            repair_live_only=False,
            require_clean_compare=False,
            json=True,
        )
        payload = repair_market_data.repair(args)
        return {
            'command': ['repair_market_data', day],
            'ok': bool(payload.get('ok')),
            'path': payload.get('path') or out_path,
            'download_attempted': payload.get('download_attempted'),
            'intraday_repair': payload.get('intraday_repair'),
            'before_compare': payload.get('before_compare'),
            'after_compare': payload.get('after_compare'),
            'freshness': payload.get('freshness'),
        }
    except Exception as e:
        return {'command': ['repair_market_data', day], 'ok': False, 'error': str(e)}


def _live_market_data_watchdog_step(day: str, pure_validation: bool = False) -> dict:
    out_path = os.path.join(OUT_DIR, 'live_market_data_watchdog', f'live_market_data_watchdog_{day}.json')
    if pure_validation:
        return {
            'command': ['live_market_data_watchdog', day],
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_live_market_data_rematerialize',
            'expected_json': out_path,
            'json_exists': os.path.exists(out_path),
        }
    try:
        args = argparse.Namespace(
            day=day,
            tickers=list(REPLAY_TICKERS),
            feed=REPLAY_FEED,
            quote_mode=REPLAY_QUOTE_MODE,
            btc_mode=REPLAY_BTC_MODE,
            max_tape_age_sec=180,
            min_stock_rows=1,
            force_materialize=False,
            json=True,
        )
        payload = live_market_data_watchdog.check_and_heal(args)
        restart_step = None
        if payload.get('restart_needed'):
            stop_payload = stop_live_monitor(day)
            start_payload = start_live_monitor(day)
            restart_step = {
                'stop': stop_payload,
                'start': start_payload,
                'ok': bool(stop_payload.get('ok') and start_payload.get('ok')),
            }
            payload['restart'] = restart_step
        ok = bool(payload.get('ok') or (restart_step and restart_step.get('ok')))
        return {
            'command': ['live_market_data_watchdog', day],
            'ok': ok,
            'path': payload.get('path') or out_path,
            'verdict': (payload.get('after_materialize') or {}).get('verdict'),
            'restart_needed': payload.get('restart_needed'),
            'restart': restart_step,
            'before': {
                'verdict': (payload.get('before') or {}).get('verdict'),
                'rows': ((payload.get('before') or {}).get('tape') or {}).get('rows'),
                'stock_rows': (payload.get('before') or {}).get('stock_rows'),
                'issues': (payload.get('before') or {}).get('issues'),
            },
            'after_materialize': {
                'verdict': (payload.get('after_materialize') or {}).get('verdict'),
                'rows': ((payload.get('after_materialize') or {}).get('tape') or {}).get('rows'),
                'stock_rows': (payload.get('after_materialize') or {}).get('stock_rows'),
                'issues': (payload.get('after_materialize') or {}).get('issues'),
            },
        }
    except Exception as e:
        return {'command': ['live_market_data_watchdog', day], 'ok': False, 'error': str(e)}


def _baseline_drift_sentinel_step(day: str, pure_validation: bool = False) -> dict:
    out_path = _baseline_drift_sentinel_path(day)
    if pure_validation:
        return {
            'command': ['baseline_drift_sentinel', 'scan', '--write-labels', '--label', day],
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_baseline_drift_mutation',
            'expected_report': out_path,
            'report_exists': os.path.exists(out_path),
        }
    try:
        payload = baseline_drift_sentinel.scan(mutate=True, write_report=True, label=day)
        return {
            'command': ['baseline_drift_sentinel', 'scan', '--write-labels', '--label', day],
            'ok': bool(payload.get('promotion_safe')),
            'path': payload.get('output_path'),
            'counts': payload.get('counts'),
            'mutated_count': payload.get('mutated_count'),
        }
    except Exception as e:
        return {'command': ['baseline_drift_sentinel', 'scan', '--write-labels', '--label', day], 'ok': False, 'error': str(e)}


def _step2_decision_parity_step(
    day: str,
    pure_validation: bool = False,
    prepared_cache_dir: str | None = None,
) -> dict:
    ledger_path = os.path.join(OUT_DIR, 'step2_decision_parity', f'step2_decision_parity_{day}.jsonl')
    summary_path = os.path.join(OUT_DIR, 'step2_decision_parity', f'step2_decision_parity_{day}.summary.json')
    cmd = [
        sys.executable,
        'backtest_30d_engine.py',
        '--start',
        day,
        '--end',
        day,
        '--tickers',
        *REPLAY_TICKERS,
        '--feed',
        REPLAY_FEED,
        '--quote-mode',
        REPLAY_QUOTE_MODE,
        '--btc-mode',
        REPLAY_BTC_MODE,
        '--use-prepared-events',
        '--prepared-cache-dir',
        prepared_cache_dir or os.path.join(HERE, 'data_cache', 'alpaca_prepared_events'),
        '--resume-days',
        '--rebuild-missing-opportunities',
        '--write-step2-decision-parity',
        '--validation-mode',
        'finalist',
        '--indicator-mode',
        'live',
        '--start-balance',
        '100000',
        '--step2-latency-mode',
        'entry-exit',
        '--step2-latency-model',
        os.path.join(HERE, 'postmortem', 'latency_model', 'step2_latency_model.json'),
        '--step2-latency-percentile',
        'p75',
    ]
    if pure_validation:
        return {
            'command': cmd,
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_step2_replay',
            'expected_ledger': ledger_path,
            'expected_summary': summary_path,
            'prepared_cache_dir': prepared_cache_dir or os.path.join(HERE, 'data_cache', 'alpaca_prepared_events'),
            'ledger_exists': os.path.exists(ledger_path),
            'summary_exists': os.path.exists(summary_path),
        }
    result = _run(cmd, timeout=900)
    result['expected_ledger'] = ledger_path
    result['expected_summary'] = summary_path
    result['prepared_cache_dir'] = prepared_cache_dir or os.path.join(HERE, 'data_cache', 'alpaca_prepared_events')
    result['ledger_exists'] = os.path.exists(ledger_path)
    result['summary_exists'] = os.path.exists(summary_path)
    if result.get('ok') and not result.get('ledger_exists'):
        result['ok'] = False
        result['error'] = 'step2_decision_parity_ledger_missing_after_success'
    return result


def _fast_live_signal_step2_parity_step(day: str, pure_validation: bool = False) -> dict:
    ledger_path = os.path.join(OUT_DIR, 'step2_decision_parity', f'step2_decision_parity_{day}.jsonl')
    summary_path = os.path.join(OUT_DIR, 'step2_decision_parity', f'step2_decision_parity_{day}.summary.json')
    unified_path = _unified_live_signal_parity_path(day)
    unified_summary_path = _unified_live_signal_parity_summary_path(day)
    prepared_cache_dir = os.path.join(HERE, 'data_cache', 'live_intraday_tapes')
    cmd = [
        sys.executable,
        'live_signal_step2_parity.py',
        '--day',
        day,
        '--tickers',
        *REPLAY_TICKERS,
        '--feed',
        REPLAY_FEED,
        '--quote-mode',
        REPLAY_QUOTE_MODE,
        '--btc-mode',
        REPLAY_BTC_MODE,
        '--prepared-cache-dir',
        prepared_cache_dir,
        '--latency-model',
        os.path.join(HERE, 'postmortem', 'latency_model', 'step2_latency_model.json'),
        '--latency-percentile',
        'p75',
    ]
    if pure_validation:
        return {
            'command': cmd,
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_fast_live_signal_step2_parity',
            'expected_ledger': ledger_path,
            'expected_summary': summary_path,
            'prepared_cache_dir': prepared_cache_dir,
            'ledger_exists': os.path.exists(ledger_path),
            'summary_exists': os.path.exists(summary_path),
            'unified_ledger_exists': os.path.exists(unified_path),
            'unified_summary_exists': os.path.exists(unified_summary_path),
        }
    result = _run(cmd, timeout=120)
    result['expected_ledger'] = ledger_path
    result['expected_summary'] = summary_path
    result['prepared_cache_dir'] = prepared_cache_dir
    result['expected_unified_ledger'] = unified_path
    result['expected_unified_summary'] = unified_summary_path
    result['ledger_exists'] = os.path.exists(ledger_path)
    result['summary_exists'] = os.path.exists(summary_path)
    result['unified_ledger_exists'] = os.path.exists(unified_path)
    result['unified_summary_exists'] = os.path.exists(unified_summary_path)
    if result.get('ok') and not result.get('ledger_exists'):
        result['ok'] = False
        result['error'] = 'fast_step2_decision_parity_ledger_missing_after_success'
    if result.get('ok') and not result.get('unified_ledger_exists'):
        result['ok'] = False
        result['error'] = 'live_signal_unified_decision_ledger_missing_after_success'
    return result


def _step2_latency_model_step(day: str, pure_validation: bool = False) -> dict:
    out_path = os.path.join(HERE, 'postmortem', 'latency_model', 'step2_latency_model.json')
    cmd = [
        sys.executable,
        'step2_latency_model.py',
        'build',
        '--days',
        day,
        '--out',
        out_path,
    ]
    if pure_validation:
        return {
            'command': cmd,
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_latency_model_refresh',
            'expected_model': out_path,
            'model_exists': os.path.exists(out_path),
        }
    result = _run(cmd, timeout=120)
    result['expected_model'] = out_path
    result['model_exists'] = os.path.exists(out_path)
    if result.get('ok') and not result.get('model_exists'):
        result['ok'] = False
        result['error'] = 'step2_latency_model_missing_after_success'
    return result


def _latency_outcome_shards_step(day: str, pure_validation: bool = False) -> dict:
    cmd = [
        sys.executable,
        'build_latency_outcome_shards.py',
        '--start',
        day,
        '--end',
        day,
        '--tickers',
        *REPLAY_TICKERS,
        '--feed',
        REPLAY_FEED,
        '--quote-mode',
        REPLAY_QUOTE_MODE,
        '--btc-mode',
        REPLAY_BTC_MODE,
        '--indicator-mode',
        'live',
        '--workers',
        str(STEP2_COMPILED_WORKERS),
        '--step2-latency-mode',
        'entry-exit',
        '--step2-latency-model',
        os.path.join(HERE, 'postmortem', 'latency_model', 'step2_latency_model.json'),
        '--step2-latency-percentile',
        'p75',
    ]
    if pure_validation:
        return {
            'command': cmd,
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_latency_outcome_shard_prebuild',
        }
    return _run(cmd, timeout=1200)


def _cache_layer_report_step(day: str, pure_validation: bool = False) -> dict:
    out_path = os.path.join(OUT_DIR, 'cache_layers', f'step2_cache_layers_{day}.json')
    if pure_validation:
        return {
            'command': ['step2_cache_layers', day],
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_cache_layer_report_write',
            'expected_report': out_path,
            'report_exists': os.path.exists(out_path),
        }
    try:
        path = step2_cache_layers.write_report(
            day,
            list(REPLAY_TICKERS),
            feed=REPLAY_FEED,
            quote_mode=REPLAY_QUOTE_MODE,
            btc_mode=REPLAY_BTC_MODE,
            indicator_mode='live',
        )
        payload = _read_json(path, {}) or {}
        return {
            'command': ['step2_cache_layers', day],
            'ok': os.path.exists(path),
            'path': path,
            'recommended_action': payload.get('recommended_action'),
            'stale_layers': payload.get('stale_layers'),
        }
    except Exception as e:
        return {'command': ['step2_cache_layers', day], 'ok': False, 'error': str(e)}


def _certify_step2_cache_step(day: str, pure_validation: bool = False) -> dict:
    out_path = _step2_cache_certification_path(day)
    if pure_validation:
        return {
            'command': ['certify_step2_cache', day],
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_step2_cache_certification_write',
            'expected_receipt': out_path,
            'receipt_exists': os.path.exists(out_path),
        }
    try:
        args = argparse.Namespace(
            day=day,
            tickers=list(REPLAY_TICKERS),
            feed=REPLAY_FEED,
            quote_mode=REPLAY_QUOTE_MODE,
            btc_mode=REPLAY_BTC_MODE,
            indicator_mode='live',
            name='',
            compiled_manifest='',
            workers=STEP2_COMPILED_WORKERS,
            no_existing=False,
            overlap_sec=120,
            step2_latency_mode='entry-exit',
            step2_latency_model=str(step2_latency_model.DEFAULT_MODEL_PATH),
            step2_latency_percentile=0.75,
            use_state_checkpoints=True,
            check_only=False,
            no_recertify_non_score=False,
        )
        payload = certify_step2_cache.certify(args, write=True)
        return {
            'command': ['certify_step2_cache', day],
            'ok': bool(payload.get('ok')),
            'path': ((payload.get('output') or {}).get('json_path')) or out_path,
            'selected_action': payload.get('selected_action'),
            'before': payload.get('before'),
            'after': payload.get('after'),
            'failure_reasons': payload.get('failure_reasons'),
            'certification_hash': payload.get('certification_hash'),
        }
    except Exception as e:
        return {'command': ['certify_step2_cache', day], 'ok': False, 'error': str(e)}


def _canonical_opportunity_ledger_step(day: str, pure_validation: bool = False) -> dict:
    out_path = os.path.join(OUT_DIR, 'canonical_opportunities', f'canonical_opportunities_{day}.jsonl')
    summary_path = os.path.join(OUT_DIR, 'canonical_opportunities', f'canonical_opportunities_{day}.summary.json')
    if pure_validation:
        return {
            'command': ['canonical_opportunity_ledger', day],
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_canonical_ledger_write',
            'expected_ledger': out_path,
            'expected_summary': summary_path,
            'ledger_exists': os.path.exists(out_path),
            'summary_exists': os.path.exists(summary_path),
        }
    try:
        payload = canonical_opportunity_ledger.build_day(day)
        return {
            'command': ['canonical_opportunity_ledger', day],
            'ok': os.path.exists(payload.get('path') or out_path),
            'path': payload.get('path') or out_path,
            'summary_path': payload.get('summary_path') or summary_path,
            'rows': payload.get('rows'),
            'counts': payload.get('counts'),
            'pnl': payload.get('pnl'),
        }
    except Exception as e:
        return {'command': ['canonical_opportunity_ledger', day], 'ok': False, 'error': str(e)}


def _canonical_decision_packet_step(day: str, pure_validation: bool = False) -> dict:
    live_path = _canonical_live_decision_packet_path(day)
    step2_path = _canonical_step2_decision_packet_path(day)
    exit_path = _canonical_live_exit_packet_path(day)
    diff_path = _canonical_packet_diff_path(day)
    if pure_validation:
        return {
            'command': ['canonical_decision_packet', 'diff', day],
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_canonical_packet_write',
            'expected_live_packets': live_path,
            'expected_step2_packets': step2_path,
            'expected_exit_packets': exit_path,
            'expected_diff': diff_path,
            'live_packets_exist': os.path.exists(live_path),
            'step2_packets_exist': os.path.exists(step2_path),
            'exit_packets_exist': os.path.exists(exit_path),
            'diff_exists': os.path.exists(diff_path),
        }
    try:
        payload = canonical_decision_packet.diff_day(day)
        return {
            'command': ['canonical_decision_packet', 'diff', day],
            'ok': bool((payload.get('schema_compatibility') or {}).get('ok')),
            'path': payload.get('path'),
            'shared_entry_packets': payload.get('shared_entry_packets'),
            'live_only_entry_packets': payload.get('live_only_entry_packets'),
            'step2_only_entry_packets': payload.get('step2_only_entry_packets'),
            'mismatch_count': payload.get('mismatch_count'),
            'schema_compatibility': payload.get('schema_compatibility'),
        }
    except Exception as e:
        return {'command': ['canonical_decision_packet', 'diff', day], 'ok': False, 'error': str(e)}


def _parity_verdict_step(day: str, pure_validation: bool = False) -> dict:
    json_path = _parity_verdict_path(day)
    txt_path = _parity_verdict_text_path(day)
    if pure_validation:
        return {
            'command': ['parity_verdict_engine', day],
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_parity_verdict_write',
            'expected_json': json_path,
            'expected_txt': txt_path,
            'json_exists': os.path.exists(json_path),
            'txt_exists': os.path.exists(txt_path),
        }
    try:
        payload = parity_verdict_engine.build(day=day, write=True, rebuild_packets=False)
        return {
            'command': ['parity_verdict_engine', day],
            'ok': bool(payload.get('ok')),
            'promotion_safe': bool(payload.get('promotion_safe')),
            'verdict': payload.get('verdict'),
            'critical_count': payload.get('critical_count'),
            'warning_count': payload.get('warning_count'),
            'issue_kind_counts': payload.get('issue_kind_counts'),
            'path': payload.get('path') or json_path,
            'text_path': payload.get('text_path') or txt_path,
        }
    except Exception as e:
        return {'command': ['parity_verdict_engine', day], 'ok': False, 'error': str(e)}


def _artifact_registry_step(day: str, pure_validation: bool = False) -> dict:
    out_path = os.path.join(OUT_DIR, 'artifact_registry', f'artifact_registry_{day}.json')
    if pure_validation:
        return {
            'command': ['artifact_version_registry', day],
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_artifact_registry_write',
            'expected_registry': out_path,
            'registry_exists': os.path.exists(out_path),
        }
    try:
        path = artifact_version_registry.write(day, list(REPLAY_TICKERS))
        payload = _read_json(path, {}) or {}
        return {
            'command': ['artifact_version_registry', day],
            'ok': os.path.exists(path),
            'path': path,
            'registry_hash': payload.get('registry_hash'),
        }
    except Exception as e:
        return {'command': ['artifact_version_registry', day], 'ok': False, 'error': str(e)}


def _golden_parity_step(day: str, pure_validation: bool = False) -> dict:
    out_path = _golden_parity_path()
    if pure_validation:
        return {
            'command': ['golden_parity_suite', '--days', day],
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_golden_parity_write',
            'expected_report': out_path,
            'report_exists': os.path.exists(out_path),
        }
    try:
        payload = golden_parity_suite.run(days=[day])
        payload['path'] = _write_json(out_path, payload)
        return {
            'command': ['golden_parity_suite', '--days', day],
            'ok': bool(payload.get('ok')),
            'path': payload.get('path'),
            'days': payload.get('days'),
            'failed': [
                {'day': r.get('day'), 'checks': [c for c in r.get('checks', []) if not c.get('ok')]}
                for r in payload.get('results', [])
                if not r.get('ok')
            ],
        }
    except Exception as e:
        return {'command': ['golden_parity_suite'], 'ok': False, 'error': str(e)}


def _contract_gate_step(day: str, pure_validation: bool = False) -> dict:
    out_path = _contract_gate_path(day)
    if pure_validation:
        return {
            'command': ['contract_gate', '--day', day],
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_contract_gate_write',
            'expected_report': out_path,
            'report_exists': os.path.exists(out_path),
        }
    try:
        payload = contract_gate.write(day=day)
        return {
            'command': ['contract_gate', '--day', day, '--write'],
            'ok': bool(payload.get('ok')),
            'path': payload.get('path'),
            'critical_failure_count': payload.get('critical_failure_count'),
        }
    except Exception as e:
        return {'command': ['contract_gate', '--day', day], 'ok': False, 'error': str(e)}


def _run_supervisor_cleanup_step(day: str, phase: str, pure_validation: bool = False) -> dict:
    out_path = _run_supervisor_cleanup_path(day, phase)
    if pure_validation:
        return {
            'command': ['run_supervisor', 'cleanup', '--kill'],
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_process_mutation',
            'expected_report': out_path,
            'report_exists': os.path.exists(out_path),
        }
    try:
        payload = run_supervisor.cleanup_stale(kill=True, stale_sec=300)
        payload['runs_after_cleanup'] = run_supervisor.list_runs()[:25]
        payload['worker_policy'] = worker_policy.describe_policy()
        payload['path'] = _write_json(out_path, payload)
        return {
            'command': ['run_supervisor', 'cleanup', '--kill'],
            'ok': bool(payload.get('ok')),
            'path': payload.get('path'),
            'cleaned_count': payload.get('count'),
            'killed': payload.get('killed'),
        }
    except Exception as e:
        return {'command': ['run_supervisor', 'cleanup', '--kill'], 'ok': False, 'error': str(e)}


def _raw_tick_retention_step(day: str, pure_validation: bool = False) -> dict:
    if pure_validation:
        try:
            import retention_policy
            payload = retention_policy.build_retention_plan(day)
            raw_plan = payload.get('raw_tick_market_day_retention') or {}
            return {
                'command': [sys.executable, 'retention_policy.py', day, '--apply'],
                'ok': True,
                'skipped': True,
                'reason': 'validate_only_avoids_destructive_raw_tick_deletes',
                'raw_tick_market_days': raw_plan.get('raw_tick_market_days_found'),
                'delete_candidate_days': len(raw_plan.get('delete_market_days') or []),
                'delete_candidate_bytes': raw_plan.get('delete_candidate_bytes'),
            }
        except Exception as e:
            return {'command': ['retention_policy', '--plan'], 'ok': False, 'error': str(e)}
    try:
        import retention_policy
        payload = retention_policy.apply_raw_tick_retention(day)
        path = os.path.join(OUT_DIR, f'retention_apply_{day}.json')
        payload['path'] = _write_json(path, payload)
        return {
            'command': [sys.executable, 'retention_policy.py', day, '--apply'],
            'ok': bool(payload.get('ok')),
            'path': payload.get('path'),
            'applied': payload.get('applied'),
            'deleted_market_days_count': payload.get('deleted_market_days_count', 0),
            'deleted_bytes': payload.get('deleted_bytes', 0),
            'errors': payload.get('errors', []),
        }
    except Exception as e:
        return {'command': ['retention_policy', '--apply'], 'ok': False, 'error': str(e)}


def _pre_market_parity_gate_step(day: str, pure_validation: bool = False) -> dict:
    if pure_validation:
        payload = pre_market_parity_gate.build(day=day, allow_app_down=True)
        payload.update({
            'observed_ok': bool(payload.get('ok')),
            'ok': True,
            'command': [sys.executable, 'pre_market_parity_gate.py', day, '--allow-app-down'],
            'skipped_write': True,
            'expected_path': _pre_market_parity_gate_path(day),
            'artifact_exists': os.path.exists(_pre_market_parity_gate_path(day)),
            'reason': 'validate_only_observes_gate_without_blocking_or_writing',
        })
        return payload
    result = _run([sys.executable, 'pre_market_parity_gate.py', day, '--allow-app-down'], timeout=120)
    result['expected_path'] = _pre_market_parity_gate_path(day)
    result['artifact_exists'] = os.path.exists(result['expected_path'])
    return result


def _execution_contract_harness_step(day: str, pure_validation: bool = False) -> dict:
    out_path = _execution_contract_harness_path(day)
    try:
        payload = live_step2_execution_harness.run(day=day, write=not pure_validation)
        return {
            'command': ['live_step2_execution_harness', day] + (['--no-write'] if pure_validation else []),
            'ok': bool(payload.get('ok')),
            'scenario_count': payload.get('scenario_count'),
            'failed_count': payload.get('failed_count'),
            'failed_scenarios': payload.get('failed_scenarios') or [],
            'path': payload.get('path') or out_path,
            'artifact_exists': os.path.exists(out_path),
            'pure_validation': bool(pure_validation),
        }
    except Exception as e:
        return {'command': ['live_step2_execution_harness', day], 'ok': False, 'error': str(e)}


def _rollback_drill_step(day: str, pure_validation: bool = False) -> dict:
    try:
        payload = rollback_drill.build(
            execute=False,
            restart=False,
            reason=f'Pre-market rollback drill for {day}: prove last-known-good restore target before trading.',
            actor='automation_ops.py',
            write=not pure_validation,
        )
        payload.update({
            'observed_ok': bool(payload.get('ok')),
            'ok': True if pure_validation else bool(payload.get('ok')),
            'command': [sys.executable, 'rollback_drill.py', '--json'],
            'expected_path': _rollback_drill_latest_path(),
            'artifact_exists': os.path.exists(_rollback_drill_latest_path()),
            'skipped_write': bool(pure_validation),
            'reason': (
                'validate_only_observes_rollback_drill_without_blocking_or_writing'
                if pure_validation else payload.get('reason')
            ),
        })
        return payload
    except Exception as e:
        return {
            'command': [sys.executable, 'rollback_drill.py', '--json'],
            'ok': False,
            'error': str(e),
            'expected_path': _rollback_drill_latest_path(),
            'artifact_exists': os.path.exists(_rollback_drill_latest_path()),
        }


def _artifact_dag_step(day: str, pure_validation: bool = False) -> dict:
    if pure_validation:
        return {
            'command': ['artifact_dag_scheduler', day],
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_artifact_writes',
            'expected_path': _artifact_dag_path(day),
            'exists': os.path.exists(_artifact_dag_path(day)),
        }
    try:
        payload = artifact_dag_scheduler.plan(day, list(REPLAY_TICKERS), workers=STEP2_COMPILED_WORKERS)
        return {
            'command': ['artifact_dag_scheduler', day],
            'ok': bool(payload.get('path')),
            'path': payload.get('path'),
            'recommended_action': payload.get('recommended_action'),
            'ordered_nodes': payload.get('ordered_nodes'),
        }
    except Exception as e:
        return {'command': ['artifact_dag_scheduler', day], 'ok': False, 'error': str(e)}


def _intraday_shadow_step2_step(day: str, pure_validation: bool = False) -> dict:
    if pure_validation:
        return {
            'command': ['intraday_shadow_step2', day],
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_shadow_rewrite',
            'expected_path': _intraday_shadow_step2_path(day),
            'expected_summary': _intraday_shadow_step2_summary_path(day),
            'exists': os.path.exists(_intraday_shadow_step2_path(day)),
            'summary_exists': os.path.exists(_intraday_shadow_step2_summary_path(day)),
        }
    try:
        payload = intraday_shadow_step2.build_day(day)
        return {
            'command': ['intraday_shadow_step2', day],
            'ok': int(payload.get('critical_count') or 0) == 0,
            'path': payload.get('path'),
            'summary_path': payload.get('summary_path'),
            'rows': payload.get('rows'),
            'critical_count': payload.get('critical_count'),
            'warning_count': payload.get('warning_count'),
        }
    except Exception as e:
        return {'command': ['intraday_shadow_step2', day], 'ok': False, 'error': str(e)}


def _shadow_variant_step(day: str, pure_validation: bool = False) -> dict:
    if pure_validation:
        return {
            'command': ['shadow_variant_engine', 'run-day', day],
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_shadow_variant_replay',
            'expected_decisions': _shadow_variant_decisions_path(day),
            'expected_leaderboard': _shadow_variant_leaderboard_path(day),
            'decisions_exists': os.path.exists(_shadow_variant_decisions_path(day)),
            'leaderboard_exists': os.path.exists(_shadow_variant_leaderboard_path(day)),
        }
    try:
        if not os.path.exists(shadow_variant_engine.CANDIDATE_PATH):
            shadow_variant_engine.bootstrap_candidates(max_profiles=100)
        payload = shadow_variant_engine.build_day(
            day,
            tickers=list(REPLAY_TICKERS),
            feed=REPLAY_FEED,
            quote_mode=REPLAY_QUOTE_MODE,
            btc_mode=REPLAY_BTC_MODE,
            prepared_cache_dir=os.path.join(HERE, 'data_cache', 'live_intraday_tapes'),
            max_profiles=100,
        )
        return {
            'command': ['shadow_variant_engine', 'run-day', day],
            'ok': bool(payload.get('path')),
            'path': payload.get('path'),
            'decision_path': payload.get('decision_path'),
            'profile_count': payload.get('profile_count'),
            'signal_rows': payload.get('signal_rows'),
            'top5': payload.get('top5'),
        }
    except Exception as e:
        return {'command': ['shadow_variant_engine', 'run-day', day], 'ok': False, 'error': str(e)}


def _candidate_lifecycle_step(day: str, pure_validation: bool = False) -> dict:
    dashboard_path = _candidate_lifecycle_dashboard_path(day)
    registry_path = _candidate_lifecycle_registry_path()
    if pure_validation:
        return {
            'command': ['candidate_lifecycle', 'sync', '--day', day],
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_candidate_lifecycle_write',
            'expected_dashboard': dashboard_path,
            'expected_registry': registry_path,
            'dashboard_exists': os.path.exists(dashboard_path),
            'registry_exists': os.path.exists(registry_path),
        }
    try:
        sync = candidate_lifecycle.sync_from_known_artifacts(day, write=True)
        dashboard = candidate_lifecycle.write_dashboard(day)
        ok = bool(sync.get('registry_path')) and os.path.exists(dashboard_path)
        return {
            'command': ['candidate_lifecycle', 'sync', '--day', day],
            'ok': ok,
            'sync': sync,
            'dashboard': dashboard.get('output') or {},
            'expected_dashboard': dashboard_path,
            'expected_registry': registry_path,
            'dashboard_exists': os.path.exists(dashboard_path),
            'registry_exists': os.path.exists(registry_path),
        }
    except Exception as e:
        return {'command': ['candidate_lifecycle', 'sync', '--day', day], 'ok': False, 'error': str(e)}


def _deterministic_live_replay_step(day: str, pure_validation: bool = False) -> dict:
    if pure_validation:
        return {
            'command': ['deterministic_live_replay', day],
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_deterministic_replay_write',
            'expected_report': _deterministic_live_replay_path(day),
            'report_exists': os.path.exists(_deterministic_live_replay_path(day)),
        }
    try:
        payload = deterministic_live_replay.build(day, write=True)
        return {
            'command': ['deterministic_live_replay', day],
            'ok': bool(payload.get('ok')),
            'path': payload.get('path'),
            'blockers': payload.get('blockers'),
            'live_signal_rows': payload.get('live_signal_rows'),
            'decision_mismatch_count': payload.get('decision_mismatch_count'),
        }
    except Exception as e:
        return {'command': ['deterministic_live_replay', day], 'ok': False, 'error': str(e)}


def _architecture_drift_gate_step(day: str, pure_validation: bool = False) -> dict:
    if pure_validation:
        return {
            'command': ['architecture_drift_gate', '--day', day],
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_architecture_drift_write',
            'expected_report': _architecture_drift_path(day),
            'report_exists': os.path.exists(_architecture_drift_path(day)),
        }
    try:
        live_signal_path = os.path.join(OUT_DIR, 'live_signal_parity', f'live_signal_parity_{day}.jsonl')
        replay_day = day if os.path.exists(live_signal_path) else None
        payload = architecture_drift_gate.build(day=replay_day, write=True)
        return {
            'command': ['architecture_drift_gate', '--day', day],
            'ok': bool(payload.get('ok')),
            'path': payload.get('path'),
            'replay_day': replay_day,
            'failed_count': payload.get('failed_count'),
            'failed_checks': payload.get('failed_checks', [])[:10],
        }
    except Exception as e:
        return {'command': ['architecture_drift_gate', '--day', day], 'ok': False, 'error': str(e)}


def _execution_lifecycle_step(day: str, pure_validation: bool = False) -> dict:
    if pure_validation:
        return {
            'command': ['execution_lifecycle', day],
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_lifecycle_replay_write',
            'expected_path': _execution_lifecycle_path(day),
            'expected_summary': _execution_lifecycle_summary_path(day),
            'exists': os.path.exists(_execution_lifecycle_path(day)),
            'summary_exists': os.path.exists(_execution_lifecycle_summary_path(day)),
        }
    try:
        payload = execution_lifecycle.write_snapshot(day)
        return {
            'command': ['execution_lifecycle', day],
            'ok': not bool(payload.get('anomalies')),
            'path': payload.get('path'),
            'summary_path': payload.get('summary_path'),
            'rows': payload.get('rows'),
            'open_trade_count': payload.get('open_trade_count'),
            'anomalies': payload.get('anomalies', [])[:10],
        }
    except Exception as e:
        return {'command': ['execution_lifecycle', day], 'ok': False, 'error': str(e)}


def _order_lifecycle_reconciliation_step(day: str, pure_validation: bool = False) -> dict:
    json_path = _order_lifecycle_reconciliation_path(day)
    txt_path = _order_lifecycle_reconciliation_text_path(day)
    if pure_validation:
        return {
            'command': ['order_lifecycle_reconciliation', day],
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_order_lifecycle_reconciliation_write',
            'expected_json': json_path,
            'expected_txt': txt_path,
            'json_exists': os.path.exists(json_path),
            'txt_exists': os.path.exists(txt_path),
        }
    try:
        payload = order_lifecycle_reconciliation.build(day, write=True)
        return {
            'command': ['order_lifecycle_reconciliation', day],
            'ok': bool(payload.get('ok')),
            'verdict': payload.get('verdict'),
            'critical_count': payload.get('critical_count'),
            'warning_count': payload.get('warning_count'),
            'path': payload.get('path') or json_path,
            'text_path': payload.get('text_path') or txt_path,
        }
    except Exception as e:
        return {'command': ['order_lifecycle_reconciliation', day], 'ok': False, 'error': str(e)}


def _live_step2_parity_report_step(day: str, pure_validation: bool = False) -> dict:
    json_path = os.path.join(OUT_DIR, 'live_step2_parity', f'live_step2_parity_{day}.json')
    txt_path = os.path.join(OUT_DIR, 'live_step2_parity', f'live_step2_parity_{day}.txt')
    if pure_validation:
        return {
            'command': ['live_step2_parity_report', day],
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_report_writes',
            'expected_json': json_path,
            'expected_txt': txt_path,
            'json_exists': os.path.exists(json_path),
            'txt_exists': os.path.exists(txt_path),
        }
    try:
        payload = live_step2_parity_report.build_and_write(day=day)
        output = payload.get('output') or {}
        ok = bool(os.path.exists(output.get('json_path') or json_path) and os.path.exists(output.get('txt_path') or txt_path))
        return {
            'command': ['live_step2_parity_report', day],
            'ok': ok,
            'json_path': output.get('json_path') or json_path,
            'txt_path': output.get('txt_path') or txt_path,
            'summary': payload.get('summary'),
            'step2_data_freshness': payload.get('step2_data_freshness'),
        }
    except Exception as e:
        return {'command': ['live_step2_parity_report', day], 'ok': False, 'error': str(e)}


def _daily_parity_scorecard_step(day: str, pure_validation: bool = False) -> dict:
    json_path = _daily_parity_scorecard_path(day)
    txt_path = _daily_parity_scorecard_text_path(day)
    if pure_validation:
        return {
            'command': ['daily_parity_scorecard', day],
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_daily_parity_scorecard_write',
            'expected_json': json_path,
            'expected_txt': txt_path,
            'json_exists': os.path.exists(json_path),
            'txt_exists': os.path.exists(txt_path),
        }
    try:
        payload = daily_parity_scorecard.build(day, write=True, refresh_report=False)
        return {
            'command': ['daily_parity_scorecard', day],
            'ok': bool(payload.get('ok')),
            'verdict': payload.get('verdict'),
            'critical_count': payload.get('critical_count'),
            'warning_count': payload.get('warning_count'),
            'pnl': payload.get('pnl'),
            'trades': payload.get('trades'),
            'path': payload.get('path') or json_path,
            'text_path': payload.get('text_path') or txt_path,
        }
    except Exception as e:
        return {'command': ['daily_parity_scorecard', day], 'ok': False, 'error': str(e)}


def _intraday_parity_sentinel_step(day: str, pure_validation: bool = False) -> dict:
    latest_path = os.path.join(OUT_DIR, 'parity_sentinel', f'parity_sentinel_{day}.json')
    history_path = os.path.join(OUT_DIR, 'parity_sentinel', f'parity_sentinel_{day}.jsonl')
    cmd = [
        sys.executable,
        'intraday_parity_sentinel.py',
        day,
        '--refresh-report',
    ]
    if pure_validation:
        return {
            'command': cmd,
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_intraday_parity_sentinel_write',
            'expected_latest': latest_path,
            'expected_history': history_path,
            'latest_exists': os.path.exists(latest_path),
            'history_exists': os.path.exists(history_path),
        }
    try:
        payload = intraday_parity_sentinel.check_once(day, refresh_report=True)
        return {
            'command': ['intraday_parity_sentinel', day, '--refresh-report'],
            'ok': bool(payload.get('ok')),
            'severity': payload.get('severity'),
            'alerts': payload.get('alerts', [])[:10],
            'metrics': payload.get('metrics'),
            'elapsed_sec': payload.get('elapsed_sec'),
            'output': payload.get('output'),
        }
    except Exception as e:
        return {'command': cmd, 'ok': False, 'error': str(e)}


def _intraday_step2_refresh_step(day: str, pure_validation: bool = False) -> dict:
    cmd = [
        sys.executable, 'step2_today_compiled.py',
        '--day', day,
        '--tickers', *REPLAY_TICKERS,
        '--feed', REPLAY_FEED,
        '--quote-mode', REPLAY_QUOTE_MODE,
        '--btc-mode', REPLAY_BTC_MODE,
        '--indicator-mode', 'live',
        '--workers', str(STEP2_COMPILED_WORKERS),
        '--overlap-sec', '120',
        '--step2-latency-mode',
        'entry-exit',
        '--step2-latency-model',
        os.path.join(HERE, 'postmortem', 'latency_model', 'step2_latency_model.json'),
        '--step2-latency-percentile',
        'p75',
    ]
    if pure_validation:
        prepared_path = _intraday_replay_tape_path(day)
        decision_path = os.path.join(
            OUT_DIR,
            'backtests',
            'decision_tapes',
            f"decision_tape_{REPLAY_FEED}_{REPLAY_QUOTE_MODE}_{REPLAY_BTC_MODE}_"
            f"live_{'-'.join(REPLAY_TICKERS)}_{day}.jsonl.gz",
        )
        manifest_path = _compiled_step2_manifest_path(day)
        chunk_manifest_path = _compiled_step2_chunk_manifest_path(day)
        score_path = _compiled_step2_score_path(day)
        rebuild_plan_path = _step2_rebuild_plan_path(day)
        return {
            'command': cmd,
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_intraday_compiled_step2_refresh',
            'expected_intraday_prepared_tape': prepared_path,
            'expected_decision_tape': decision_path,
            'expected_compiled_manifest': manifest_path,
            'expected_compiled_chunk_manifest': chunk_manifest_path,
            'expected_score_report': score_path,
            'expected_rebuild_plan': rebuild_plan_path,
            'intraday_prepared_exists': os.path.exists(prepared_path),
            'decision_tape_exists': os.path.exists(decision_path),
            'compiled_manifest_exists': os.path.exists(manifest_path),
            'compiled_chunk_manifest_exists': os.path.exists(chunk_manifest_path),
            'score_report_exists': os.path.exists(score_path),
            'rebuild_plan_exists': os.path.exists(rebuild_plan_path),
        }
    result = _run(cmd, timeout=1800)
    result['expected_compiled_manifest'] = _compiled_step2_manifest_path(day)
    result['expected_compiled_chunk_manifest'] = _compiled_step2_chunk_manifest_path(day)
    result['expected_score_report'] = _compiled_step2_score_path(day)
    result['expected_rebuild_plan'] = _step2_rebuild_plan_path(day)
    result['compiled_manifest_exists'] = os.path.exists(result['expected_compiled_manifest'])
    result['compiled_chunk_manifest_exists'] = os.path.exists(result['expected_compiled_chunk_manifest'])
    result['score_report_exists'] = os.path.exists(result['expected_score_report'])
    result['rebuild_plan_exists'] = os.path.exists(result['expected_rebuild_plan'])
    if result.get('ok') and not result.get('compiled_manifest_exists'):
        result['ok'] = False
        result['error'] = 'compiled_step2_manifest_missing_after_success'
    if result.get('ok') and not result.get('compiled_chunk_manifest_exists'):
        result['ok'] = False
        result['error'] = 'compiled_step2_chunk_manifest_missing_after_success'
    if result.get('ok') and not result.get('score_report_exists'):
        result['ok'] = False
        result['error'] = 'compiled_step2_score_report_missing_after_success'
    return result


def _post_close_compiled_step2_step(day: str, pure_validation: bool = False) -> dict:
    cmd = [
        sys.executable, 'step2_today_compiled.py',
        '--day', day,
        '--tickers', *REPLAY_TICKERS,
        '--feed', REPLAY_FEED,
        '--quote-mode', REPLAY_QUOTE_MODE,
        '--btc-mode', REPLAY_BTC_MODE,
        '--indicator-mode', 'live',
        '--workers', str(STEP2_COMPILED_WORKERS),
        '--step2-latency-mode',
        'entry-exit',
        '--step2-latency-model',
        os.path.join(HERE, 'postmortem', 'latency_model', 'step2_latency_model.json'),
        '--step2-latency-percentile',
        'p75',
        '--write-unified-ledger',
    ]
    manifest_path = _compiled_step2_manifest_path(day)
    chunk_manifest_path = _compiled_step2_chunk_manifest_path(day)
    score_path = _compiled_step2_score_path(day)
    rebuild_plan_path = _step2_rebuild_plan_path(day)
    unified_trace_path = _unified_step2_current_trace_path(day)
    unified_trace_summary_path = _unified_step2_current_trace_summary_path(day)
    if pure_validation:
        return {
            'command': cmd,
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_post_close_compiled_step2_rebuild',
            'expected_compiled_manifest': manifest_path,
            'expected_compiled_chunk_manifest': chunk_manifest_path,
            'expected_score_report': score_path,
            'expected_rebuild_plan': rebuild_plan_path,
            'compiled_manifest_exists': os.path.exists(manifest_path),
            'compiled_chunk_manifest_exists': os.path.exists(chunk_manifest_path),
            'score_report_exists': os.path.exists(score_path),
            'rebuild_plan_exists': os.path.exists(rebuild_plan_path),
            'unified_trace_exists': os.path.exists(unified_trace_path),
            'unified_trace_summary_exists': os.path.exists(unified_trace_summary_path),
        }
    result = _run(cmd, timeout=2400)
    result['expected_compiled_manifest'] = manifest_path
    result['expected_compiled_chunk_manifest'] = chunk_manifest_path
    result['expected_score_report'] = score_path
    result['expected_rebuild_plan'] = rebuild_plan_path
    result['expected_unified_trace'] = unified_trace_path
    result['expected_unified_trace_summary'] = unified_trace_summary_path
    result['compiled_manifest_exists'] = os.path.exists(manifest_path)
    result['compiled_chunk_manifest_exists'] = os.path.exists(chunk_manifest_path)
    result['score_report_exists'] = os.path.exists(score_path)
    result['rebuild_plan_exists'] = os.path.exists(rebuild_plan_path)
    result['unified_trace_exists'] = os.path.exists(unified_trace_path)
    result['unified_trace_summary_exists'] = os.path.exists(unified_trace_summary_path)
    if result.get('ok') and not result.get('compiled_manifest_exists'):
        result['ok'] = False
        result['error'] = 'post_close_compiled_step2_manifest_missing_after_success'
    if result.get('ok') and not result.get('compiled_chunk_manifest_exists'):
        result['ok'] = False
        result['error'] = 'post_close_compiled_step2_chunk_manifest_missing_after_success'
    if result.get('ok') and not result.get('score_report_exists'):
        result['ok'] = False
        result['error'] = 'post_close_compiled_step2_score_report_missing_after_success'
    if result.get('ok') and not result.get('unified_trace_exists'):
        result['ok'] = False
        result['error'] = 'post_close_step2_unified_trace_missing_after_success'
    return result


def _step2_warm_scorer_step(day: str, pure_validation: bool = False) -> dict:
    manifest_path = _compiled_step2_manifest_path(day)
    cmd = [
        sys.executable,
        'step2_warm_scorer_ops.py',
        'start',
        '--day',
        day,
        '--manifest-path',
        manifest_path,
    ]
    if pure_validation:
        return {
            'command': cmd,
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_step2_warm_scorer_start',
            'expected_compiled_manifest': manifest_path,
            'compiled_manifest_exists': os.path.exists(manifest_path),
        }
    result = _run(cmd, timeout=60)
    result['expected_compiled_manifest'] = manifest_path
    result['compiled_manifest_exists'] = os.path.exists(manifest_path)
    if result.get('ok') and not result.get('compiled_manifest_exists'):
        result['ok'] = False
        result['error'] = 'step2_warm_scorer_manifest_missing_after_success'
    return result


def _compiled_step2_fast_score_step(day: str, pure_validation: bool = False) -> dict:
    cmd = [
        sys.executable, 'step2_today_compiled.py',
        '--day', day,
        '--skip-refresh',
        '--write-unified-ledger',
    ]
    manifest_path = _compiled_step2_manifest_path(day)
    chunk_manifest_path = _compiled_step2_chunk_manifest_path(day)
    score_path = _compiled_step2_score_path(day)
    unified_trace_path = _unified_step2_current_trace_path(day)
    unified_trace_summary_path = _unified_step2_current_trace_summary_path(day)
    if pure_validation:
        return {
            'command': cmd,
            'ok': True,
            'skipped': True,
            'reason': 'validate_only_avoids_compiled_step2_fast_score',
            'expected_compiled_manifest': manifest_path,
            'expected_compiled_chunk_manifest': chunk_manifest_path,
            'expected_score_report': score_path,
            'compiled_manifest_exists': os.path.exists(manifest_path),
            'compiled_chunk_manifest_exists': os.path.exists(chunk_manifest_path),
            'score_report_exists': os.path.exists(score_path),
            'unified_trace_exists': os.path.exists(unified_trace_path),
            'unified_trace_summary_exists': os.path.exists(unified_trace_summary_path),
        }
    result = _run(cmd, timeout=120)
    result['expected_compiled_manifest'] = manifest_path
    result['expected_compiled_chunk_manifest'] = chunk_manifest_path
    result['expected_score_report'] = score_path
    result['expected_unified_trace'] = unified_trace_path
    result['expected_unified_trace_summary'] = unified_trace_summary_path
    result['compiled_manifest_exists'] = os.path.exists(manifest_path)
    result['compiled_chunk_manifest_exists'] = os.path.exists(chunk_manifest_path)
    result['score_report_exists'] = os.path.exists(score_path)
    result['unified_trace_exists'] = os.path.exists(unified_trace_path)
    result['unified_trace_summary_exists'] = os.path.exists(unified_trace_summary_path)
    if result.get('ok') and not result.get('unified_trace_exists'):
        result['ok'] = False
        result['error'] = 'compiled_step2_unified_trace_missing_after_success'
    return result


def _read_run_ledger() -> dict:
    return _read_json(RUN_LEDGER_PATH, {}) or {}


def _write_run_ledger(ledger: dict) -> str:
    return _write_json(RUN_LEDGER_PATH, ledger)


def _ledger_done(phase: str, day: str) -> Optional[dict]:
    entry = _read_run_ledger().get(_run_key(phase, day))
    if isinstance(entry, dict) and entry.get('ok'):
        return entry
    return None


def _mark_ledger_started(phase: str, day: str, market: Optional[dict] = None) -> None:
    ledger = _read_run_ledger()
    ledger[_run_key(phase, day)] = {
        'phase': phase,
        'day': day,
        'status': 'running',
        'ok': False,
        'started_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'market_calendar': market,
    }
    _write_run_ledger(ledger)


def _mark_ledger_result(phase: str, day: str, ok: bool, artifact: Optional[str],
                        steps: list[dict], market: Optional[dict] = None,
                        skipped_reason: Optional[str] = None) -> None:
    ledger = _read_run_ledger()
    prior = ledger.get(_run_key(phase, day)) if isinstance(ledger.get(_run_key(phase, day)), dict) else {}
    prior.update({
        'phase': phase,
        'day': day,
        'status': 'completed' if ok else 'failed',
        'ok': bool(ok),
        'artifact': artifact,
        'completed_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'step_count': len(steps),
        'market_calendar': market,
    })
    if skipped_reason:
        prior['skipped_reason'] = skipped_reason
    ledger[_run_key(phase, day)] = prior
    _write_run_ledger(ledger)


def _status_reachable(timeout: int = 5) -> bool:
    try:
        with urlopen(APP_URL, timeout=timeout) as resp:
            return resp.status == 200
    except Exception:
        return False


def _read_app_status(timeout: int = 5) -> dict:
    try:
        with urlopen(APP_URL, timeout=timeout) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except Exception as e:
        return {'_read_error': str(e)}


def _mock_required_by_now() -> bool:
    now = datetime.now(CT)
    if now.weekday() >= 5:
        return False
    required_at = now.replace(
        hour=MOCK_REQUIRED_HOUR_CT,
        minute=MOCK_REQUIRED_MINUTE_CT,
        second=0,
        microsecond=0,
    )
    return now >= required_at


def ensure_mock_running_in_window() -> dict:
    status = _read_app_status(timeout=5)
    if status.get('_read_error'):
        return {
            'ok': False,
            'url': APP_URL,
            'error': status.get('_read_error'),
            'reason': 'status_unreadable',
        }
    required_by_now = _mock_required_by_now()
    if not status.get('in_window') and not required_by_now:
        return {
            'ok': True,
            'skipped': True,
            'reason': 'before_0825_required_start',
            'running': status.get('running'),
            'in_window': status.get('in_window'),
            'required_by_now': required_by_now,
            'required_start_ct': f'{MOCK_REQUIRED_HOUR_CT:02d}:{MOCK_REQUIRED_MINUTE_CT:02d}',
        }
    if status.get('running') is True:
        return {
            'ok': True,
            'already_running': True,
            'running': True,
            'in_window': status.get('in_window'),
            'required_by_now': required_by_now,
        }
    blockers = {
        'broker_exposure_block': status.get('broker_exposure_block'),
        'broker_lifecycle_block': status.get('broker_lifecycle_block'),
        'broker_api_degraded': status.get('broker_api_degraded'),
        'kill_switch': status.get('kill_switch') if (status.get('kill_switch') or {}).get('enabled') else None,
        'pending_entries': status.get('pending_entries') or None,
    }
    active_blockers = {k: v for k, v in blockers.items() if v}
    if active_blockers:
        return {
            'ok': False,
            'reason': 'start_blocked_by_safety_state',
            'running': status.get('running'),
            'in_window': status.get('in_window'),
            'required_by_now': required_by_now,
            'blockers': active_blockers,
        }
    try:
        req = Request('http://127.0.0.1:5000/mock/start', method='POST')
        with urlopen(req, timeout=10) as resp:
            payload = json.loads(resp.read().decode('utf-8'))
        started_status = payload.get('status') or {}
        return {
            'ok': bool(payload.get('ok') and started_status.get('running') is True),
            'reason': payload.get('reason'),
            'started': bool(payload.get('ok')),
            'running': started_status.get('running'),
            'in_window': started_status.get('in_window'),
            'required_by_now': required_by_now,
            'broker_exposure_block': started_status.get('broker_exposure_block'),
            'broker_api_degraded': started_status.get('broker_api_degraded'),
            'pending_entries': started_status.get('pending_entries'),
        }
    except Exception as e:
        return {
            'ok': False,
            'reason': 'start_request_failed',
            'error': str(e),
            'running': status.get('running'),
            'in_window': status.get('in_window'),
            'required_by_now': required_by_now,
        }


def _dry_run_mock_start_guard() -> dict:
    status = _read_app_status(timeout=3)
    reachable = _status_reachable(timeout=3)
    return {
        'ok': reachable,
        'dry_run': True,
        'would': 'ensure_mock_running_in_window',
        'running': status.get('running'),
        'in_window': status.get('in_window'),
        'required_by_now': _mock_required_by_now(),
        'required_start_ct': f'{MOCK_REQUIRED_HOUR_CT:02d}:{MOCK_REQUIRED_MINUTE_CT:02d}',
        'error': None if reachable else status.get('_read_error'),
    }


def _live_state_rollover_step(day: str, pure_validation: bool = False) -> dict:
    if pure_validation:
        result = _run([sys.executable, 'live_state_rollover.py', 'check', day, '--json'], timeout=60)
        result['pure_validation'] = True
        return result
    try:
        req = Request(
            f'http://127.0.0.1:5000/mock/rollover?day={day}&reason=pre_open',
            data=b'{}',
            headers={'Content-Type': 'application/json'},
            method='POST',
        )
        with urlopen(req, timeout=20) as resp:
            payload = json.loads(resp.read().decode('utf-8'))
        payload['command'] = ['mock_rollover', day]
        return payload
    except Exception as e:
        return {
            'ok': False,
            'command': ['mock_rollover', day],
            'reason': 'mock_rollover_request_failed',
            'error': str(e),
        }


def _broker_lifecycle_gate_step(day: str, pure_validation: bool = False,
                                enforce: bool = True,
                                label: str = 'automation') -> dict:
    if pure_validation:
        status = _read_app_status(timeout=3)
        reachable = _status_reachable(timeout=3)
        gate = status.get('broker_lifecycle_gate') if isinstance(status, dict) else None
        return {
            'ok': reachable,
            'command': ['mock_broker_lifecycle', day, '--dry-run'],
            'pure_validation': True,
            'would': 'run broker lifecycle gate against watched broker orders/positions',
            'status_reachable': reachable,
            'existing_gate': gate,
            'error': None if reachable else status.get('_read_error'),
        }
    try:
        req = Request(
            f'http://127.0.0.1:5000/mock/broker-lifecycle?label={label}&enforce={1 if enforce else 0}',
            data=b'{}',
            headers={'Content-Type': 'application/json'},
            method='POST',
        )
        with urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read().decode('utf-8'))
        payload['command'] = ['mock_broker_lifecycle', day, label, {'enforce': enforce}]
        return payload
    except Exception as e:
        return {
            'ok': False,
            'command': ['mock_broker_lifecycle', day, label],
            'reason': 'mock_broker_lifecycle_request_failed',
            'error': str(e),
        }


def ensure_app_running() -> dict:
    if _status_reachable(timeout=3):
        return {'ok': True, 'already_running': True, 'url': APP_URL}
    log_path = os.path.join(HERE, 'flask_boot.log')
    try:
        from runtime_guard import append_runtime_event
        append_runtime_event('app_status_unreachable_restart_attempt', {
            'url': APP_URL,
            'phase': 'ensure_app_running',
            'market_hours_guard': True,
            'deduction': 'Automation watchdog could not reach /mock/status and attempted a local app launch.',
        })
    except Exception:
        pass
    try:
        from runtime_guard import rotate_runtime_logs
        rotate_runtime_logs()
    except Exception:
        pass
    creationflags = getattr(subprocess, 'CREATE_NO_WINDOW', 0)
    try:
        log = open(log_path, 'a', encoding='utf-8')
    except OSError:
        # Another process may hold an exclusive lock on flask_boot.log.
        # Fall back to a unique log file so the watchdog can still start the app.
        ts = datetime.now(CT).strftime('%Y%m%d_%H%M%S')
        log_path = os.path.join(HERE, f'flask_boot_{ts}.log')
        log = open(log_path, 'a', encoding='utf-8')
    with log:
        log.write(f'[{datetime.now(CT).isoformat(timespec="seconds")}] automation_ops launching local_server.py\n')
        log.flush()
        proc = subprocess.Popen(
            [sys.executable, os.path.join(HERE, 'local_server.py')],
            cwd=HERE,
            stdout=log,
            stderr=subprocess.STDOUT,
            creationflags=creationflags,
        )
    for _ in range(12):
        if _status_reachable(timeout=2):
            try:
                from runtime_guard import append_runtime_event
                append_runtime_event('app_status_unreachable_restart_recovered', {
                    'url': APP_URL,
                    'pid': proc.pid,
                    'boot_log': log_path,
                })
            except Exception:
                pass
            return {
                'ok': True,
                'already_running': False,
                'pid': proc.pid,
                'url': APP_URL,
                'boot_log': log_path,
            }
        if proc.poll() is not None:
            break
    return {
        'ok': False,
        'already_running': False,
        'pid': proc.pid,
        'url': APP_URL,
        'boot_log': log_path,
        'returncode': proc.poll(),
        'error': 'app_not_reachable_after_start',
    }


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _terminate_pid(pid: int, grace_sec: float = 5.0) -> Optional[str]:
    first_error = None
    try:
        os.kill(pid, 15)
    except Exception as exc:
        first_error = str(exc)
    else:
        deadline = time.time() + max(0.0, grace_sec)
        while time.time() < deadline:
            if not _pid_alive(pid):
                return None
            time.sleep(0.25)
    if os.name == 'nt':
        try:
            proc = subprocess.run(
                ['powershell', '-NoProfile', '-Command', f'Stop-Process -Id {int(pid)} -Force'],
                cwd=HERE,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=10,
            )
            if proc.returncode == 0:
                return None
            prefix = f'{first_error};' if first_error else ''
            return f'{prefix}force_stop_failed:{proc.stdout[-500:]}'
        except Exception as exc:
            prefix = f'{first_error};' if first_error else ''
            return f'{prefix}force_stop_exception:{exc}'
    return first_error or 'still_alive_after_grace'


def _live_monitor_checkpoint(day: str) -> str:
    return os.path.join(OUT_DIR, f'session_checkpoint_{day}_auto_loop.json')


def _cleanup_stale_live_monitors(day: str) -> list[int]:
    if os.name != 'nt':
        return []
    killed: list[int] = []
    current_pid = os.getpid()
    try:
        proc = subprocess.run(
            [
                'powershell',
                '-NoProfile',
                '-Command',
                (
                    "Get-CimInstance Win32_Process -Filter \"name = 'python.exe'\" | "
                    "Select-Object ProcessId,CommandLine | ConvertTo-Json -Compress"
                ),
            ],
            cwd=HERE,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=10,
        )
        if proc.returncode != 0 or not proc.stdout.strip():
            return []
        rows = json.loads(proc.stdout)
        if isinstance(rows, dict):
            rows = [rows]
        for row in rows or []:
            pid = int(row.get('ProcessId') or 0)
            cmd = str(row.get('CommandLine') or '')
            if not pid or pid == current_pid:
                continue
            if 'live_monitor.py' not in cmd or '--loop' not in cmd:
                continue
            if f'live_monitor.py {day} ' in cmd or f'live_monitor.py" {day} ' in cmd:
                continue
            os.kill(pid, 15)
            killed.append(pid)
    except Exception:
        return killed
    return killed


def stop_live_monitor(day: str | None = None) -> dict:
    target_day = day or _today()
    stopped: list[int] = []
    errors: list[str] = []
    current_pid = os.getpid()
    old = _read_json(PID_PATH, {}) or {}
    candidate_pids: set[int] = set()
    try:
        if old.get('pid') and (not old.get('day') or str(old.get('day')) == target_day):
            candidate_pids.add(int(old.get('pid')))
    except Exception:
        pass
    if os.name == 'nt':
        try:
            proc = subprocess.run(
                [
                    'powershell',
                    '-NoProfile',
                    '-Command',
                    (
                        "Get-CimInstance Win32_Process -Filter \"name = 'python.exe'\" | "
                        "Select-Object ProcessId,CommandLine | ConvertTo-Json -Compress"
                    ),
                ],
                cwd=HERE,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                timeout=10,
            )
            if proc.returncode == 0 and proc.stdout.strip():
                rows = json.loads(proc.stdout)
                if isinstance(rows, dict):
                    rows = [rows]
                for row in rows or []:
                    pid = int(row.get('ProcessId') or 0)
                    cmd = str(row.get('CommandLine') or '')
                    if not pid or pid == current_pid:
                        continue
                    if 'live_monitor.py' in cmd and '--loop' in cmd and target_day in cmd:
                        candidate_pids.add(pid)
        except Exception as exc:
            errors.append(f'process_scan_failed:{exc}')
    for pid in sorted(candidate_pids):
        err = _terminate_pid(pid)
        if not err:
            stopped.append(pid)
        else:
            errors.append(f'pid_{pid}:{err}')
    if os.path.exists(PID_PATH):
        try:
            os.remove(PID_PATH)
        except Exception as exc:
            errors.append(f'pid_file_remove:{exc}')
    return {
        'ok': not errors,
        'day': target_day,
        'stopped_pids': stopped,
        'pid_file': PID_PATH,
        'errors': errors,
    }


def _live_monitor_healthy(day: str, pid: int, max_age_sec: int = 180) -> dict:
    checkpoint = _live_monitor_checkpoint(day)
    alive = _pid_alive(pid)
    fresh = False
    age_sec = None
    if os.path.exists(checkpoint):
        age_sec = max(0, int(time.time() - os.path.getmtime(checkpoint)))
        fresh = age_sec <= max_age_sec
    return {
        'alive': alive,
        'fresh_checkpoint': fresh,
        'checkpoint_age_sec': age_sec,
        'checkpoint': checkpoint,
        'healthy': bool(alive and fresh),
    }


def start_live_monitor(day: str, interval_sec: int = 30) -> dict:
    os.makedirs(OUT_DIR, exist_ok=True)
    stale_killed = _cleanup_stale_live_monitors(day)
    old = _read_json(PID_PATH, {}) or {}
    old_pid = old.get('pid')
    if old_pid:
        try:
            old_pid_i = int(old_pid)
            health = _live_monitor_healthy(day, old_pid_i)
            if health.get('healthy') and old.get('parity_sentinel'):
                return {'ok': True, 'already_running': True, 'pid': old_pid_i,
                        'pid_file': PID_PATH, 'health': health,
                        'stale_monitor_pids_stopped': stale_killed}
            if health.get('alive'):
                err = _terminate_pid(old_pid_i)
                if err:
                    return {'ok': False, 'pid': old_pid_i, 'pid_file': PID_PATH,
                            'health': health, 'error': f'old_monitor_missing_parity_sentinel:{err}'}
        except Exception:
            pass
    out_path = os.path.join(OUT_DIR, f'live_monitor_loop_{day}.log')
    log = open(out_path, 'a', encoding='utf-8')
    creationflags = getattr(subprocess, 'CREATE_NO_WINDOW', 0)
    proc = subprocess.Popen(
        [
            sys.executable,
            os.path.join(HERE, 'live_monitor.py'),
            day,
            '--loop',
            '--interval-sec',
            str(interval_sec),
            '--checkpoint',
            '--checkpoint-label',
            'auto_loop',
            '--cleanup-stale-loops',
            '--parity-sentinel',
            '--parity-sentinel-interval-sec',
            '300',
            '--auto-exit-after-ct',
            '15:20',
        ],
        cwd=HERE,
        stdout=log,
        stderr=subprocess.STDOUT,
        creationflags=creationflags,
    )
    log.close()
    payload = {
        'ok': True,
        'already_running': False,
        'pid': proc.pid,
        'pid_file': PID_PATH,
        'log_file': out_path,
        'started_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'day': day,
        'interval_sec': interval_sec,
        'parity_sentinel': True,
        'parity_sentinel_interval_sec': 300,
        'stale_monitor_pids_stopped': stale_killed,
    }
    _write_json(PID_PATH, payload)
    return payload


def _dry_run_app_check() -> dict:
    return {
        'ok': _status_reachable(timeout=3),
        'dry_run': True,
        'would': 'ensure_app_running',
        'url': APP_URL,
    }


def _dry_run_monitor_start(day: str) -> dict:
    return {
        'ok': True,
        'dry_run': True,
        'would': 'start_live_monitor',
        'day': day,
        'pid_file': PID_PATH,
    }


def _dry_run_monitor_stop(day: str) -> dict:
    return {
        'ok': True,
        'dry_run': True,
        'would': 'stop_live_monitor',
        'day': day,
        'pid_file': PID_PATH,
    }


def run_phase(phase: str, day: Optional[str] = None, force: bool = False,
              dry_run: bool = False, validate_only: bool = False) -> dict:
    day = day or _today()
    steps = []
    pure_validation = bool(dry_run or validate_only)
    try:
        from market_calendar import market_calendar_status
        market = market_calendar_status(day)
    except Exception as e:
        market = {'day': day, 'is_trading_day': True, 'reason': f'calendar_unavailable:{e}'}
    if not pure_validation and phase != 'verify-day' and not market.get('is_trading_day') and not force:
        payload = {
            'phase': phase,
            'day': day,
            'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
            'dry_run': dry_run,
            'validate_only': validate_only,
            'ok': True,
            'skipped_market_closed': True,
            'market_calendar': market,
            'steps': [],
            'now_status': {},
            'daily_review_gate': {},
        }
        path = _automation_artifact_path(phase, day)
        payload['artifact'] = _write_json(path, payload)
        _mark_ledger_result(phase, day, True, path, [], market=market, skipped_reason='market_closed')
        return payload
    if phase == 'post-close' and not force and not pure_validation:
        existing = _ledger_done(phase, day)
        missing_architecture_files = _missing_post_close_architecture_files(day)
        if existing and not missing_architecture_files:
            stop_step = {
                'command': ['stop_live_monitor'],
                **stop_live_monitor(day),
            }
            payload = {
                'phase': phase,
                'day': day,
                'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
                'ok': bool(stop_step.get('ok')),
                'skipped_existing_success': True,
                'existing_run': existing,
                'steps': [stop_step],
                'now_status': {},
                'daily_review_gate': {},
                'market_calendar': market,
            }
            path = _automation_artifact_path(phase, day)
            payload['artifact'] = _write_json(path, payload)
            _mark_ledger_result(phase, day, True, path, [], market=market, skipped_reason='existing_success')
            return payload
        if existing and missing_architecture_files:
            steps.append({
                'command': ['post_close_existing_success_current_architecture_check'],
                'ok': True,
                'existing_run': existing,
                'missing_current_architecture_files': missing_architecture_files,
                'deduction': 'Prior post-close success predates the current architecture; rerunning post-close outputs.',
            })
    if not pure_validation:
        _mark_ledger_started(phase, day, market=market)
    if phase == 'pre-open':
        steps.append(_run_supervisor_cleanup_step(day, phase, pure_validation=pure_validation))
        steps.append({
            'command': ['ensure_app_running', '--dry-run'] if pure_validation else ['ensure_app_running'],
            **(_dry_run_app_check() if pure_validation else ensure_app_running()),
        })
        steps.append(_live_state_rollover_step(day, pure_validation=pure_validation))
        steps.append(_broker_lifecycle_gate_step(day, pure_validation=pure_validation, enforce=True, label='pre_open'))
        steps.append({
            'command': ['ensure_mock_running_in_window', '--dry-run'] if pure_validation else ['ensure_mock_running_in_window'],
            **(_dry_run_mock_start_guard() if pure_validation else ensure_mock_running_in_window()),
        })
        steps.append(_live_market_data_watchdog_step(day, pure_validation=pure_validation))
        steps.append(_contract_gate_step(day, pure_validation=pure_validation))
        steps.append(_architecture_drift_gate_step(day, pure_validation=pure_validation))
        steps.append(_execution_contract_harness_step(day, pure_validation=pure_validation))
        steps.append(_pre_market_parity_gate_step(day, pure_validation=pure_validation))
        steps.append(_rollback_drill_step(day, pure_validation=pure_validation))
        steps.append(_run([
            sys.executable, 'smoke_check.py', '--mode', 'no-surprises',
            *([] if pure_validation else ['--refresh-artifacts']),
        ], timeout=180))
        if not pure_validation:
            steps.append(_run([sys.executable, 'weekend_readiness.py', day], timeout=180))
            steps.append(_run([sys.executable, 'monday_ops.py', 'ready', day], timeout=120))
        else:
            steps.append({
                'command': [sys.executable, 'weekend_readiness.py', day],
                'ok': True,
                'skipped': True,
                'reason': 'validate_only_avoids_artifact_writes',
            })
            steps.append({
                'command': [sys.executable, 'monday_ops.py', 'ready', day],
                'ok': True,
                'skipped': True,
                'reason': 'validate_only_avoids_artifact_writes',
            })
        steps.append({
            'command': ['start_live_monitor', '--dry-run'] if pure_validation else ['start_live_monitor'],
            **(_dry_run_monitor_start(day) if pure_validation else start_live_monitor(day)),
        })
    elif phase == 'start-monitor':
        steps.append({
            'command': ['ensure_app_running', '--dry-run'] if pure_validation else ['ensure_app_running'],
            **(_dry_run_app_check() if pure_validation else ensure_app_running()),
        })
        steps.append(_live_state_rollover_step(day, pure_validation=pure_validation))
        steps.append(_broker_lifecycle_gate_step(day, pure_validation=pure_validation, enforce=True, label='start_monitor'))
        steps.append({
            'command': ['ensure_mock_running_in_window', '--dry-run'] if pure_validation else ['ensure_mock_running_in_window'],
            **(_dry_run_mock_start_guard() if pure_validation else ensure_mock_running_in_window()),
        })
        steps.append({
            'command': ['start_live_monitor', '--dry-run'] if pure_validation else ['start_live_monitor'],
            **(_dry_run_monitor_start(day) if pure_validation else start_live_monitor(day)),
        })
    elif phase == 'post-open':
        steps.append({
            'command': ['ensure_app_running', '--dry-run'] if pure_validation else ['ensure_app_running'],
            **(_dry_run_app_check() if pure_validation else ensure_app_running()),
        })
        steps.append(_broker_lifecycle_gate_step(day, pure_validation=pure_validation, enforce=True, label='intraday'))
        steps.append({
            'command': ['ensure_mock_running_in_window', '--dry-run'] if pure_validation else ['ensure_mock_running_in_window'],
            **(_dry_run_mock_start_guard() if pure_validation else ensure_mock_running_in_window()),
        })
        steps.append(_run([sys.executable, 'smoke_check.py', '--mode', 'post-open'], timeout=120))
        if pure_validation:
            steps.append({
                'command': [sys.executable, 'live_monitor.py', day, '--checkpoint', '--checkpoint-label', 'post_open_auto'],
                'ok': True,
                'skipped': True,
                'reason': 'validate_only_avoids_checkpoint_writes',
            })
        else:
            steps.append(_run([sys.executable, 'live_monitor.py', day, '--checkpoint', '--checkpoint-label', 'post_open_auto'], timeout=120))
    elif phase == 'intraday':
        steps.append({
            'command': ['ensure_app_running', '--dry-run'] if pure_validation else ['ensure_app_running'],
            **(_dry_run_app_check() if pure_validation else ensure_app_running()),
        })
        steps.append(_broker_lifecycle_gate_step(day, pure_validation=pure_validation, enforce=True, label='parity_sentinel'))
        steps.append({
            'command': ['ensure_mock_running_in_window', '--dry-run'] if pure_validation else ['ensure_mock_running_in_window'],
            **(_dry_run_mock_start_guard() if pure_validation else ensure_mock_running_in_window()),
        })
        steps.append(_live_market_data_watchdog_step(day, pure_validation=pure_validation))
        steps.append(_contract_gate_step(day, pure_validation=pure_validation))
        if pure_validation:
            steps.append({
                'command': [sys.executable, 'live_monitor.py', day, '--checkpoint', '--checkpoint-label', 'intraday_auto'],
                'ok': True,
                'skipped': True,
                'reason': 'validate_only_avoids_checkpoint_writes',
            })
        else:
            steps.append(_run([sys.executable, 'live_monitor.py', day, '--checkpoint', '--checkpoint-label', 'intraday_auto'], timeout=120))
        steps.append(_artifact_dag_step(day, pure_validation=pure_validation))
        steps.append(_intraday_step2_refresh_step(day, pure_validation=pure_validation))
        steps.append(_market_data_integrity_step(day, pure_validation=pure_validation, source='auto'))
        steps.append(_market_data_freshness_step(day, pure_validation=pure_validation, source='auto'))
        steps.append(_step2_latency_model_step(day, pure_validation=pure_validation))
        steps.append(_step2_warm_scorer_step(day, pure_validation=pure_validation))
        steps.append(_compiled_step2_fast_score_step(day, pure_validation=pure_validation))
        steps.append(_fast_live_signal_step2_parity_step(day, pure_validation=pure_validation))
        steps.append(_canonical_decision_packet_step(day, pure_validation=pure_validation))
        steps.append(_intraday_shadow_step2_step(day, pure_validation=pure_validation))
        steps.append(_shadow_variant_step(day, pure_validation=pure_validation))
        steps.append(_candidate_lifecycle_step(day, pure_validation=pure_validation))
        steps.append(_baseline_drift_sentinel_step(day, pure_validation=pure_validation))
        steps.append(_execution_lifecycle_step(day, pure_validation=pure_validation))
        steps.append(_order_lifecycle_reconciliation_step(day, pure_validation=pure_validation))
        steps.append(_deterministic_live_replay_step(day, pure_validation=pure_validation))
        steps.append(_architecture_drift_gate_step(day, pure_validation=pure_validation))
        steps.append(_execution_contract_harness_step(day, pure_validation=pure_validation))
        steps.append(_parity_verdict_step(day, pure_validation=pure_validation))
        steps.append(_live_step2_parity_report_step(day, pure_validation=pure_validation))
        steps.append(_daily_parity_scorecard_step(day, pure_validation=pure_validation))
        steps.append(_intraday_parity_sentinel_step(day, pure_validation=pure_validation))
    elif phase == 'parity-sentinel':
        steps.append({
            'command': ['ensure_app_running', '--dry-run'] if pure_validation else ['ensure_app_running'],
            **(_dry_run_app_check() if pure_validation else ensure_app_running()),
        })
        steps.append({
            'command': ['ensure_mock_running_in_window', '--dry-run'] if pure_validation else ['ensure_mock_running_in_window'],
            **(_dry_run_mock_start_guard() if pure_validation else ensure_mock_running_in_window()),
        })
        steps.append(_live_market_data_watchdog_step(day, pure_validation=pure_validation))
        steps.append(_intraday_parity_sentinel_step(day, pure_validation=pure_validation))
    elif phase == 'pre-flat':
        steps.append({
            'command': ['ensure_app_running', '--dry-run'] if pure_validation else ['ensure_app_running'],
            **(_dry_run_app_check() if pure_validation else ensure_app_running()),
        })
        if pure_validation:
            steps.append({
                'command': [sys.executable, 'live_monitor.py', day, '--checkpoint', '--checkpoint-label', 'pre_flat_auto'],
                'ok': True,
                'skipped': True,
                'reason': 'validate_only_avoids_checkpoint_writes',
            })
        else:
            steps.append(_run([sys.executable, 'live_monitor.py', day, '--checkpoint', '--checkpoint-label', 'pre_flat_auto'], timeout=120))
    elif phase == 'post-close':
        steps.append({
            'command': ['ensure_app_running', '--dry-run'] if pure_validation else ['ensure_app_running'],
            **(_dry_run_app_check() if pure_validation else ensure_app_running()),
        })
        steps.append(_broker_lifecycle_gate_step(day, pure_validation=pure_validation, enforce=True, label='post_close'))
        steps.append(_contract_gate_step(day, pure_validation=pure_validation))
        steps.append(_prepare_replay_cache_step(day, pure_validation=pure_validation))
        steps.append(_canonical_replay_manifest_step(day, pure_validation=pure_validation))
        steps.append(_compare_intraday_to_canonical_step(day, pure_validation=pure_validation))
        steps.append(_market_data_integrity_step(day, pure_validation=pure_validation, source='canonical'))
        steps.append(_market_data_freshness_step(day, pure_validation=pure_validation, source='canonical'))
        steps.append(_market_data_repair_step(day, pure_validation=pure_validation))
        steps.append(_artifact_dag_step(day, pure_validation=pure_validation))
        steps.append(_step2_latency_model_step(day, pure_validation=pure_validation))
        steps.append(_latency_outcome_shards_step(day, pure_validation=pure_validation))
        steps.append(_post_close_compiled_step2_step(day, pure_validation=pure_validation))
        steps.append(_certify_step2_cache_step(day, pure_validation=pure_validation))
        steps.append(_step2_warm_scorer_step(day, pure_validation=pure_validation))
        steps.append(_step2_decision_parity_step(day, pure_validation=pure_validation))
        steps.append(_canonical_opportunity_ledger_step(day, pure_validation=pure_validation))
        steps.append(_canonical_decision_packet_step(day, pure_validation=pure_validation))
        steps.append(_intraday_shadow_step2_step(day, pure_validation=pure_validation))
        steps.append(_shadow_variant_step(day, pure_validation=pure_validation))
        steps.append(_candidate_lifecycle_step(day, pure_validation=pure_validation))
        steps.append(_baseline_drift_sentinel_step(day, pure_validation=pure_validation))
        steps.append(_execution_lifecycle_step(day, pure_validation=pure_validation))
        steps.append(_order_lifecycle_reconciliation_step(day, pure_validation=pure_validation))
        steps.append(_deterministic_live_replay_step(day, pure_validation=pure_validation))
        steps.append(_architecture_drift_gate_step(day, pure_validation=pure_validation))
        steps.append(_execution_contract_harness_step(day, pure_validation=pure_validation))
        steps.append(_parity_verdict_step(day, pure_validation=pure_validation))
        steps.append(_live_step2_parity_report_step(day, pure_validation=pure_validation))
        steps.append(_daily_parity_scorecard_step(day, pure_validation=pure_validation))
        steps.append(_intraday_parity_sentinel_step(day, pure_validation=pure_validation))
        steps.append(_cache_layer_report_step(day, pure_validation=pure_validation))
        steps.append(_artifact_registry_step(day, pure_validation=pure_validation))
        steps.append(_golden_parity_step(day, pure_validation=pure_validation))
        if pure_validation:
            steps.append({
                'command': [sys.executable, 'monday_close_packet.py', day, '--write-gdoc'],
                'ok': True,
                'skipped': True,
                'reason': 'validate_only_avoids_gdoc_and_learning_mutations',
            })
            steps.append({
                'command': [sys.executable, 'ops.py', 'context', day],
                'ok': True,
                'skipped': True,
                'reason': 'validate_only_avoids_artifact_writes',
            })
        else:
            steps.append(_run([sys.executable, 'monday_close_packet.py', day, '--write-gdoc'], timeout=600))
            steps.append(_run([sys.executable, 'ops.py', 'context', day], timeout=120))
        steps.append(_run([sys.executable, 'smoke_check.py', '--mode', 'post-market'], timeout=180))
        steps.append({
            'command': ['stop_live_monitor', '--dry-run'] if pure_validation else ['stop_live_monitor'],
            **(_dry_run_monitor_stop(day) if pure_validation else stop_live_monitor(day)),
        })
        steps.append(_run_supervisor_cleanup_step(day, phase, pure_validation=pure_validation))
        steps.append(_raw_tick_retention_step(day, pure_validation=pure_validation))
    elif phase == 'repair-market-data':
        steps.append(_market_data_repair_step(day, pure_validation=pure_validation))
    elif phase == 'verify-day':
        steps.extend(_verify_day_steps(day, market))
    else:
        raise ValueError(f'unknown phase: {phase}')
    now_status = _read_json(os.path.join(OUT_DIR, 'NOW_STATUS.json'), {}) or {}
    gate = _read_json(os.path.join(OUT_DIR, f'daily_review_gate_{day}.json'), {}) or {}
    payload = {
        'phase': phase,
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'dry_run': dry_run,
        'validate_only': validate_only,
        'ok': all(bool(s.get('ok')) for s in steps),
        'market_calendar': market,
        'steps': steps,
        'now_status': {
            'state': now_status.get('state'),
            'safe': now_status.get('safe'),
            'review_gate': now_status.get('review_gate'),
            'alerts': now_status.get('alerts'),
        },
        'daily_review_gate': {
            'verdict': gate.get('verdict'),
            'checks_passed': gate.get('checks_passed'),
            'checks_total': gate.get('checks_total'),
        },
    }
    if pure_validation:
        payload['artifact'] = None
        payload['artifact_write_skipped'] = True
    else:
        path = _automation_artifact_path(phase, day)
        payload['artifact'] = _write_json(path, payload)
    if not pure_validation:
        _mark_ledger_result(phase, day, bool(payload.get('ok')), payload.get('artifact'), steps, market=market)
    return payload


def _verify_day_steps(day: str, market: dict) -> list[dict]:
    if not market.get('is_trading_day'):
        return [{
            'command': ['verify-day', 'market-calendar'],
            'ok': True,
            'skipped': True,
            'reason': f"market closed: {market.get('reason')}",
        }]
    steps = []
    expected = ('pre-open', 'post-open', 'intraday', 'pre-flat', 'post-close')
    ledger = _read_run_ledger()
    for phase in expected:
        path = _automation_artifact_path(phase, day)
        payload = _read_json(path, {}) or {}
        entry = ledger.get(_run_key(phase, day)) if isinstance(ledger, dict) else None
        artifact_ok = bool(payload.get('ok'))
        ledger_ok = bool(isinstance(entry, dict) and entry.get('ok'))
        steps.append({
            'command': ['verify-artifact', phase],
            'ok': bool(os.path.exists(path) and artifact_ok and ledger_ok),
            'artifact': path,
            'artifact_exists': os.path.exists(path),
            'artifact_ok': artifact_ok,
            'ledger_ok': ledger_ok,
            'ledger_status': entry.get('status') if isinstance(entry, dict) else None,
        })
    required_files = [
        os.path.join(OUT_DIR, f'session_checkpoint_{day}_post_open_auto.json'),
        os.path.join(OUT_DIR, f'session_checkpoint_{day}_pre_flat_auto.json'),
        _prepared_replay_tape_path(day),
        _intraday_replay_tape_path(day),
        os.path.join(HERE, 'data_cache', 'incremental_market_store', day, 'manifest.json'),
        os.path.join(OUT_DIR, 'step2_freshness', f'step2_freshness_{day}.json'),
        os.path.join(OUT_DIR, 'step2_freshness', f'intraday_vs_canonical_{day}.json'),
        os.path.join(OUT_DIR, f'monday_close_packet_{day}.json'),
        os.path.join(OUT_DIR, f'review_index_{day}.json'),
        os.path.join(OUT_DIR, 'live_step2_parity', f'live_step2_parity_{day}.json'),
        os.path.join(OUT_DIR, 'live_step2_parity', f'live_step2_parity_{day}.txt'),
        os.path.join(OUT_DIR, 'cache_layers', f'step2_cache_layers_{day}.json'),
        _step2_cache_certification_path(day),
        os.path.join(OUT_DIR, 'execution_replay_inputs', f'execution_replay_inputs_{day}.jsonl'),
        os.path.join(OUT_DIR, 'canonical_opportunities', f'canonical_opportunities_{day}.jsonl'),
        os.path.join(OUT_DIR, 'canonical_opportunities', f'canonical_opportunities_{day}.summary.json'),
        os.path.join(OUT_DIR, 'parity_diff_classifier', f'parity_diff_classifier_{day}.json'),
        os.path.join(OUT_DIR, 'artifact_registry', f'artifact_registry_{day}.json'),
        os.path.join(OUT_DIR, 'step2_decision_parity', f'step2_decision_parity_{day}.jsonl'),
        os.path.join(OUT_DIR, 'step2_decision_parity', f'step2_decision_parity_{day}.summary.json'),
        os.path.join(OUT_DIR, 'parity_sentinel', f'parity_sentinel_{day}.json'),
        os.path.join(OUT_DIR, 'parity_sentinel', 'PARITY_SENTINEL_LATEST.json'),
        _compiled_step2_manifest_path(day),
        _compiled_step2_chunk_manifest_path(day),
        _compiled_step2_score_path(day),
        _step2_rebuild_plan_path(day),
        _unified_step2_current_trace_path(day),
        _unified_step2_current_trace_summary_path(day),
        _unified_live_signal_parity_path(day),
        _unified_live_signal_parity_summary_path(day),
        _unified_live_decision_source_path(day),
        _intraday_shadow_step2_path(day),
        _intraday_shadow_step2_summary_path(day),
        _execution_lifecycle_path(day),
        _execution_lifecycle_summary_path(day),
        _order_lifecycle_reconciliation_path(day),
        _order_lifecycle_reconciliation_text_path(day),
        _parity_verdict_path(day),
        _parity_verdict_text_path(day),
        _market_data_integrity_path(day),
        _market_data_integrity_text_path(day),
        _market_data_freshness_path(day),
        _market_data_freshness_text_path(day),
        _daily_parity_scorecard_path(day),
        _daily_parity_scorecard_text_path(day),
        _baseline_drift_sentinel_path(day),
        _artifact_dag_path(day),
        _golden_parity_path(),
        os.path.join(OUT_DIR, 'NOW_STATUS.json'),
    ]
    for path in required_files:
        steps.append({
            'command': ['verify-file', os.path.basename(path)],
            'ok': os.path.exists(path),
            'path': path,
        })
    health_path = os.path.join(OUT_DIR, f'automation_day_health_{day}.json')
    payload = {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'market_calendar': market,
        'ok': all(s.get('ok') for s in steps),
        'checks': steps,
    }
    _write_json(health_path, payload)
    steps.append({
        'command': ['write-day-health'],
        'ok': True,
        'path': health_path,
    })
    return steps


def main() -> int:
    ap = argparse.ArgumentParser(description='Stable scheduled automation entrypoint.')
    ap.add_argument('phase', choices=PHASES)
    ap.add_argument('day', nargs='?', default=None)
    ap.add_argument('--json', action='store_true')
    ap.add_argument('--force', action='store_true', help='Allow rerunning a mutating/idempotent phase such as post-close.')
    ap.add_argument('--dry-run', action='store_true', help='For pre-open, validate without launching app or monitor processes.')
    ap.add_argument('--validate-only', action='store_true',
                    help='Pure validation mode: avoid artifact refreshes, GDoc writes, monitors, and ledgers.')
    args = ap.parse_args()
    try:
        payload = run_phase(args.phase, args.day, force=args.force,
                            dry_run=args.dry_run, validate_only=args.validate_only)
    except Exception as e:
        day = args.day or _today()
        payload = {
            'phase': args.phase,
            'day': day,
            'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
            'ok': False,
            'fatal_error': str(e),
            'traceback': traceback.format_exc()[-6000:],
            'steps': [],
        }
        try:
            payload['artifact'] = _write_json(_automation_artifact_path(args.phase, day), payload)
            _mark_ledger_result(args.phase, day, False, payload.get('artifact'), [])
        except Exception:
            pass
    if args.json:
        print(json.dumps(payload, indent=2, default=str))
    else:
        print(f"automation phase={payload['phase']} day={payload['day']} ok={payload['ok']}")
        print(payload.get('artifact') or 'artifact_write_skipped')
        for step in payload['steps']:
            print(f"{'OK' if step.get('ok') else 'FAIL'} {step.get('command')}")
    return 0 if payload.get('ok') else 1


if __name__ == '__main__':
    raise SystemExit(main())
