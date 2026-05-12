from __future__ import annotations

import argparse
import json
import os
from urllib.request import urlopen
from collections import Counter, defaultdict
from datetime import datetime
from statistics import median
from typing import Any, Iterable

import live_execution_replay_schema
import artifact_version_registry
import canonical_decision_packet
import canonical_opportunity_ledger
import market_data_integrity_gate
import market_data_freshness_guard
import order_lifecycle_reconciliation
import parity_diff_classifier
import parity_verdict_engine
import step2_cache_layers

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore


HERE = os.path.dirname(os.path.abspath(__file__))
POSTMORTEM_DIR = os.path.join(HERE, 'postmortem')
OUT_DIR = os.path.join(POSTMORTEM_DIR, 'live_step2_parity')
CT = ZoneInfo('America/Chicago')
APP_URL = 'http://127.0.0.1:5000/mock/status'


def _today() -> str:
    return datetime.now(CT).date().isoformat()


def _jsonl_rows(path: str) -> list[dict[str, Any]]:
    if not os.path.exists(path):
        return []
    rows: list[dict[str, Any]] = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows


def _read_json(path: str) -> dict[str, Any]:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _read_live_status() -> dict[str, Any]:
    try:
        with urlopen(APP_URL, timeout=3) as resp:
            payload = json.loads(resp.read().decode('utf-8'))
        return payload if isinstance(payload, dict) else {}
    except Exception as exc:
        return {'_read_error': str(exc)}


def _write_json(path: str, payload: dict[str, Any]) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f'{path}.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
    os.replace(tmp, path)
    return path


def _write_text(path: str, text: str) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f'{path}.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        f.write(text)
    os.replace(tmp, path)
    return path


def _num(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _sum(values: Iterable[Any]) -> float:
    total = 0.0
    for value in values:
        n = _num(value)
        if n is not None:
            total += n
    return round(total, 2)


def _pctile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * pct))))
    return round(ordered[idx], 2)


def _latency_stats(rows: list[dict[str, Any]], key: str) -> dict[str, Any]:
    vals = [_num(row.get(key)) for row in rows]
    nums = [float(v) for v in vals if v is not None]
    if not nums:
        return {'count': 0, 'avg_ms': None, 'p50_ms': None, 'p95_ms': None, 'max_ms': None}
    return {
        'count': len(nums),
        'avg_ms': round(sum(nums) / len(nums), 2),
        'p50_ms': round(median(nums), 2),
        'p95_ms': _pctile(nums, 0.95),
        'max_ms': round(max(nums), 2),
    }


def _group_key(row: dict[str, Any]) -> str:
    ticker = row.get('ticker') or row.get('symbol') or 'UNKNOWN'
    side = row.get('side') or 'UNKNOWN'
    return f'{ticker}:{side}'


def _counter_dict(counter: Counter) -> dict[str, int]:
    return dict(sorted(counter.items(), key=lambda item: (-item[1], str(item[0]))))


def _file(day: str, folder: str, stem: str, suffix: str = '.jsonl') -> str:
    return os.path.join(POSTMORTEM_DIR, folder, f'{stem}_{day}{suffix}')


def _freshness_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    stock_stale = 0
    btc_stale = 0
    stock_missing = 0
    btc_missing = 0
    max_stock_age = None
    max_btc_age = None
    for row in rows:
        fresh = row.get('market_freshness') or {}
        stock = fresh.get('stock') or {}
        btc = fresh.get('btc') or {}
        stock_age = _num(stock.get('trade_age_sec') or stock.get('quote_age_sec'))
        btc_age = _num(btc.get('trade_age_sec') or btc.get('quote_age_sec'))
        if stock_age is None:
            stock_missing += 1
        else:
            max_stock_age = stock_age if max_stock_age is None else max(max_stock_age, stock_age)
            if stock_age > 5:
                stock_stale += 1
        if btc_age is None:
            btc_missing += 1
        else:
            max_btc_age = btc_age if max_btc_age is None else max(max_btc_age, btc_age)
            if btc_age > 5:
                btc_stale += 1
    return {
        'stock_stale_over_5s': stock_stale,
        'btc_stale_over_5s': btc_stale,
        'stock_missing_age': stock_missing,
        'btc_missing_age': btc_missing,
        'max_stock_age_sec': round(max_stock_age, 3) if max_stock_age is not None else None,
        'max_btc_age_sec': round(max_btc_age, 3) if max_btc_age is not None else None,
    }


def _trade_summary(trades: list[dict[str, Any]]) -> dict[str, Any]:
    by_ticker: dict[str, dict[str, Any]] = defaultdict(lambda: {'trades': 0, 'pnl': 0.0, 'wins': 0, 'losses': 0})
    by_side: dict[str, dict[str, Any]] = defaultdict(lambda: {'trades': 0, 'pnl': 0.0, 'wins': 0, 'losses': 0})
    for trade in trades:
        pnl = _num(trade.get('pnl') if trade.get('pnl') is not None else trade.get('net_pnl_after_estimated_costs')) or 0.0
        ticker = str(trade.get('ticker') or 'UNKNOWN')
        side = str(trade.get('side') or 'UNKNOWN')
        for bucket in (by_ticker[ticker], by_side[f'{ticker}:{side}']):
            bucket['trades'] += 1
            bucket['pnl'] = round(bucket['pnl'] + pnl, 2)
            if pnl > 0:
                bucket['wins'] += 1
            elif pnl < 0:
                bucket['losses'] += 1
    return {
        'trades': len(trades),
        'pnl': _sum(t.get('pnl') if t.get('pnl') is not None else t.get('net_pnl_after_estimated_costs') for t in trades),
        'wins': sum(1 for t in trades if (_num(t.get('pnl')) or 0.0) > 0),
        'losses': sum(1 for t in trades if (_num(t.get('pnl')) or 0.0) < 0),
        'by_ticker': dict(sorted(by_ticker.items())),
        'by_ticker_side': dict(sorted(by_side.items())),
    }


def build_report(day: str) -> dict[str, Any]:
    paths = {
        'live_signal_parity': _file(day, 'live_signal_parity', 'live_signal_parity'),
        'step2_decision_parity': _file(day, 'step2_decision_parity', 'step2_decision_parity'),
        'step2_decision_parity_summary': _file(day, 'step2_decision_parity', 'step2_decision_parity', '.summary.json'),
        'trades': _file(day, 'trades', 'trades'),
        'skipped_signals': _file(day, 'skipped_signals', 'skipped_signals'),
        'latency_attribution': _file(day, 'latency_attribution', 'latency_attribution'),
        'step2_freshness_compare': os.path.join(POSTMORTEM_DIR, 'step2_freshness', f'intraday_vs_canonical_{day}.json'),
        'step2_today_compiled': os.path.join(POSTMORTEM_DIR, 'backtests', 'step2_today_compiled', f'step2_today_compiled_{day}.json'),
        'unified_live_decision_source': os.path.join(
            POSTMORTEM_DIR, 'unified_decision_ledger', day, f'live_decision_source_{day}.jsonl',
        ),
        'canonical_live_decision_packets': os.path.join(
            POSTMORTEM_DIR, 'canonical_decision_packets', day, f'live_decision_packets_{day}.jsonl',
        ),
        'canonical_step2_decision_packets': os.path.join(
            POSTMORTEM_DIR, 'canonical_decision_packets', day, f'step2_decision_packets_{day}.jsonl',
        ),
        'canonical_live_exit_packets': os.path.join(
            POSTMORTEM_DIR, 'canonical_decision_packets', day, f'live_exit_packets_{day}.jsonl',
        ),
        'canonical_packet_diff': os.path.join(
            POSTMORTEM_DIR, 'canonical_decision_packets', day, f'canonical_packet_diff_{day}.json',
        ),
        'parity_verdict': os.path.join(
            POSTMORTEM_DIR, 'parity_verdict', f'parity_verdict_{day}.json',
        ),
        'parity_verdict_text': os.path.join(
            POSTMORTEM_DIR, 'parity_verdict', f'parity_verdict_{day}.txt',
        ),
        'market_data_integrity': os.path.join(
            POSTMORTEM_DIR, 'market_data_integrity', f'market_data_integrity_{day}.json',
        ),
        'market_data_integrity_text': os.path.join(
            POSTMORTEM_DIR, 'market_data_integrity', f'market_data_integrity_{day}.txt',
        ),
        'market_data_freshness': os.path.join(
            POSTMORTEM_DIR, 'market_data_freshness', f'market_data_freshness_{day}.json',
        ),
        'market_data_freshness_text': os.path.join(
            POSTMORTEM_DIR, 'market_data_freshness', f'market_data_freshness_{day}.txt',
        ),
        'intraday_shadow_step2': os.path.join(
            POSTMORTEM_DIR, 'intraday_shadow_step2', day, f'shadow_step2_{day}.jsonl',
        ),
        'intraday_shadow_step2_summary': os.path.join(
            POSTMORTEM_DIR, 'intraday_shadow_step2', day, f'shadow_step2_{day}.summary.json',
        ),
        'shadow_variant_decisions': os.path.join(
            POSTMORTEM_DIR, 'shadow_variants', day, f'shadow_variant_decisions_{day}.jsonl',
        ),
        'shadow_variant_leaderboard': os.path.join(
            POSTMORTEM_DIR, 'shadow_variants', day, f'shadow_variant_leaderboard_{day}.json',
        ),
        'candidate_lifecycle_dashboard': os.path.join(
            POSTMORTEM_DIR, 'candidate_lifecycle', f'candidate_lifecycle_dashboard_{day}.json',
        ),
        'candidate_lifecycle_registry': os.path.join(
            POSTMORTEM_DIR, 'candidate_lifecycle', 'candidate_lifecycle_registry.json',
        ),
        'deterministic_live_replay': os.path.join(
            POSTMORTEM_DIR, 'deterministic_live_replay', f'deterministic_live_replay_{day}.json',
        ),
        'architecture_drift_gate': os.path.join(
            POSTMORTEM_DIR, 'architecture_drift', f'architecture_drift_gate_{day}.json',
        ),
        'execution_lifecycle': os.path.join(
            POSTMORTEM_DIR, 'execution_lifecycle', day, f'execution_lifecycle_{day}.jsonl',
        ),
        'execution_lifecycle_summary': os.path.join(
            POSTMORTEM_DIR, 'execution_lifecycle', day, f'execution_lifecycle_{day}.summary.json',
        ),
        'order_lifecycle_reconciliation': os.path.join(
            POSTMORTEM_DIR, 'order_lifecycle_reconciliation', f'order_lifecycle_reconciliation_{day}.json',
        ),
        'order_lifecycle_reconciliation_text': os.path.join(
            POSTMORTEM_DIR, 'order_lifecycle_reconciliation', f'order_lifecycle_reconciliation_{day}.txt',
        ),
        'realtime_parity_alerts': os.path.join(
            POSTMORTEM_DIR, 'realtime_parity_alerts', f'parity_alerts_{day}.jsonl',
        ),
        'artifact_dag': os.path.join(
            POSTMORTEM_DIR, 'artifact_dag', f'artifact_dag_{day}.json',
        ),
        'contract_gate': os.path.join(
            POSTMORTEM_DIR, 'contract_gate', f'contract_gate_{day}.json',
        ),
    }
    parity = _jsonl_rows(paths['live_signal_parity'])
    step2 = _jsonl_rows(paths['step2_decision_parity'])
    step2_summary = _read_json(paths['step2_decision_parity_summary'])
    trades = _jsonl_rows(paths['trades'])
    skipped = _jsonl_rows(paths['skipped_signals'])
    latency = _jsonl_rows(paths['latency_attribution'])
    live_decision_source = _jsonl_rows(paths['unified_live_decision_source'])
    shadow_summary = _read_json(paths['intraday_shadow_step2_summary'])
    shadow_variants = _read_json(paths['shadow_variant_leaderboard'])
    candidate_lifecycle_dashboard = _read_json(paths['candidate_lifecycle_dashboard'])
    deterministic_replay = _read_json(paths['deterministic_live_replay'])
    architecture_drift = _read_json(paths['architecture_drift_gate'])
    lifecycle_summary = _read_json(paths['execution_lifecycle_summary'])
    realtime_alerts = _jsonl_rows(paths['realtime_parity_alerts'])
    artifact_dag = _read_json(paths['artifact_dag'])
    contract_gate_report = _read_json(paths['contract_gate'])
    compare = _read_json(paths['step2_freshness_compare'])
    step2_today = _read_json(paths['step2_today_compiled'])
    live_status = _read_live_status()

    entered = [row for row in parity if row.get('decision') == 'entered']
    skipped_parity = [row for row in parity if row.get('decision') == 'skipped']
    trade_ids = {t.get('trade_id') for t in trades if t.get('trade_id')}
    entered_trade_ids = {r.get('trade_id') for r in entered if r.get('trade_id')}
    latency_trade_ids = {r.get('trade_id') for r in latency if r.get('trade_id')}

    reason_counter = Counter(str(row.get('reason') or 'unknown') for row in skipped_parity)
    skipped_file_reason_counter = Counter(str(row.get('reason') or 'unknown') for row in skipped)
    decision_counter = Counter(str(row.get('decision') or 'unknown') for row in parity)
    group_counter = Counter(_group_key(row) for row in parity)
    feature_counter = Counter(str(row.get('feature_snapshot_hash') or 'missing') for row in parity)
    profile_counter = Counter(str(row.get('active_profile_name') or 'missing') for row in parity)
    profile_hash_counter = Counter(str(row.get('active_profile_hash') or 'missing') for row in parity)
    live_by_key = {str(row.get('parity_key')): row for row in parity if row.get('parity_key')}
    step2_by_key = {str(row.get('parity_key')): row for row in step2 if row.get('parity_key')}
    shared_keys = set(live_by_key) & set(step2_by_key)
    live_only_keys = set(live_by_key) - set(step2_by_key)
    step2_only_keys = set(step2_by_key) - set(live_by_key)
    decision_mismatches = []
    feature_mismatches = []
    outcome_mismatches = []
    for key in sorted(shared_keys):
        live_row = live_by_key[key]
        step2_row = step2_by_key[key]
        if live_row.get('decision') != step2_row.get('decision'):
            decision_mismatches.append({
                'parity_key': key,
                'ticker': live_row.get('ticker') or step2_row.get('ticker'),
                'side': live_row.get('side') or step2_row.get('side'),
                'live_decision': live_row.get('decision'),
                'step2_decision': step2_row.get('decision'),
                'live_reason': live_row.get('reason'),
                'step2_reason': step2_row.get('reason'),
                'created_at_ct': live_row.get('created_at_ct') or step2_row.get('created_at_ct'),
            })
        if (
            live_row.get('feature_snapshot_hash')
            and step2_row.get('feature_snapshot_hash')
            and live_row.get('feature_snapshot_hash') != step2_row.get('feature_snapshot_hash')
        ):
            feature_mismatches.append({
                'parity_key': key,
                'ticker': live_row.get('ticker') or step2_row.get('ticker'),
                'side': live_row.get('side') or step2_row.get('side'),
                'live_feature_hash': live_row.get('feature_snapshot_hash'),
                'step2_feature_hash': step2_row.get('feature_snapshot_hash'),
                'created_at_ct': live_row.get('created_at_ct') or step2_row.get('created_at_ct'),
            })
        live_trade = next((t for t in trades if t.get('trade_id') == live_row.get('trade_id')), None)
        step2_outcome = step2_row.get('outcome_summary') or {}
        if live_trade and step2_outcome:
            live_reason = live_trade.get('reason')
            step2_reason = step2_outcome.get('reason')
            live_pnl = _num(live_trade.get('pnl'))
            step2_pnl = _num(step2_outcome.get('pnl'))
            if live_reason != step2_reason or (
                live_pnl is not None and step2_pnl is not None and abs(live_pnl - step2_pnl) > 0.01
            ):
                outcome_mismatches.append({
                    'parity_key': key,
                    'ticker': live_row.get('ticker') or step2_row.get('ticker'),
                    'side': live_row.get('side') or step2_row.get('side'),
                    'live_reason': live_reason,
                    'step2_reason': step2_reason,
                    'live_pnl': live_pnl,
                    'step2_pnl': step2_pnl,
                    'created_at_ct': live_row.get('created_at_ct') or step2_row.get('created_at_ct'),
                })

    signals_by_trade_id = {
        str(row.get('trade_id')): row
        for row in parity
        if row.get('trade_id')
    }
    latency_by_trade_id = {
        str(row.get('trade_id')): row
        for row in latency
        if row.get('trade_id')
    }
    execution_replay = live_execution_replay_schema.write_day(
        day,
        trades,
        signals_by_trade_id=signals_by_trade_id,
        latency_by_trade_id=latency_by_trade_id,
    )
    canonical_summary = canonical_opportunity_ledger.build_day(day)
    canonical_rows = _jsonl_rows(canonical_summary.get('path') or '')
    canonical_packet_diff = canonical_decision_packet.diff_day(day)
    parity_verdict = parity_verdict_engine.build(day, write=True, rebuild_packets=False)
    market_data_integrity = market_data_integrity_gate.build(day, tickers=['CLSK', 'MARA', 'RIOT'], source='auto', write=True)
    market_data_freshness = market_data_freshness_guard.build(day, tickers=['CLSK', 'MARA', 'RIOT'], source='auto', write=True)
    order_lifecycle = order_lifecycle_reconciliation.build(day, write=True)
    diff_classification = parity_diff_classifier.write_day(day, canonical_rows)
    tickers = sorted({str(row.get('ticker') or '').upper() for row in parity if row.get('ticker')}) or ['CLSK', 'MARA', 'RIOT']
    cache_layers = step2_cache_layers.report(day, tickers)
    registry_path = artifact_version_registry.write(day, tickers, extra_artifacts={
        'live_step2_parity_report': os.path.join(OUT_DIR, f'live_step2_parity_{day}.json'),
        'execution_replay_inputs': execution_replay.get('path') or '',
        'canonical_opportunities': canonical_summary.get('path') or '',
        'canonical_live_decision_packets': paths.get('canonical_live_decision_packets') or '',
        'canonical_step2_decision_packets': paths.get('canonical_step2_decision_packets') or '',
        'canonical_live_exit_packets': paths.get('canonical_live_exit_packets') or '',
        'canonical_packet_diff': canonical_packet_diff.get('path') or '',
        'parity_verdict': parity_verdict.get('path') or '',
        'parity_verdict_text': parity_verdict.get('text_path') or '',
        'market_data_integrity': market_data_integrity.get('path') or '',
        'market_data_integrity_text': market_data_integrity.get('text_path') or '',
        'market_data_freshness': market_data_freshness.get('path') or '',
        'market_data_freshness_text': market_data_freshness.get('text_path') or '',
        'parity_diff_classifier': diff_classification.get('path') or '',
        'unified_live_decision_source': paths.get('unified_live_decision_source') or '',
        'intraday_shadow_step2': paths.get('intraday_shadow_step2') or '',
        'intraday_shadow_step2_summary': paths.get('intraday_shadow_step2_summary') or '',
        'shadow_variant_decisions': paths.get('shadow_variant_decisions') or '',
        'shadow_variant_leaderboard': paths.get('shadow_variant_leaderboard') or '',
        'candidate_lifecycle_dashboard': paths.get('candidate_lifecycle_dashboard') or '',
        'candidate_lifecycle_registry': paths.get('candidate_lifecycle_registry') or '',
        'deterministic_live_replay': paths.get('deterministic_live_replay') or '',
        'architecture_drift_gate': paths.get('architecture_drift_gate') or '',
        'execution_lifecycle': paths.get('execution_lifecycle') or '',
        'execution_lifecycle_summary': paths.get('execution_lifecycle_summary') or '',
        'order_lifecycle_reconciliation': order_lifecycle.get('path') or '',
        'order_lifecycle_reconciliation_text': order_lifecycle.get('text_path') or '',
        'realtime_parity_alerts': paths.get('realtime_parity_alerts') or '',
        'artifact_dag': paths.get('artifact_dag') or '',
        'contract_gate': paths.get('contract_gate') or '',
    })
    artifact_registry = _read_json(registry_path)
    live_execution_hash = live_status.get('step2_execution_contract_hash')
    live_active_profile = live_status.get('active_scoring_profile') or {}
    live_startup_self_check = live_status.get('startup_self_check') or {}
    step2_execution_hash = (
        (((step2_today.get('score') or {}).get('result') or {}).get('mock_parity_guards') or {}).get('execution_contract_hash')
        or cache_layers.get('compiled_step2_execution_contract_hash')
    )
    live_parity_hashes = Counter(str(r.get('step2_parity_contract_hash') or 'missing') for r in parity)
    step2_parity_hash = (
        ((step2_today.get('score') or {}).get('step2_parity_contract_hash'))
        or cache_layers.get('current_step2_parity_contract_hash')
    )
    contract_mismatches = []
    if live_execution_hash and step2_execution_hash and live_execution_hash != step2_execution_hash:
        contract_mismatches.append({
            'type': 'execution_contract_hash_mismatch',
            'live': live_execution_hash,
            'step2': step2_execution_hash,
        })
    if step2_parity_hash and live_parity_hashes:
        nonmatching = {
            key: count for key, count in live_parity_hashes.items()
            if key not in ('missing', str(step2_parity_hash))
        }
        if nonmatching:
            contract_mismatches.append({
                'type': 'mixed_or_stale_live_step2_parity_contract_hashes',
                'step2': step2_parity_hash,
                'live_counts': dict(live_parity_hashes),
            })
    live_profile_hash = live_active_profile.get('hash')
    if live_profile_hash and profile_hash_counter:
        stale_profiles = {
            key: count for key, count in profile_hash_counter.items()
            if key not in ('missing', str(live_profile_hash))
        }
        if stale_profiles:
            contract_mismatches.append({
                'type': 'mixed_or_stale_active_profile_hashes',
                'live': live_profile_hash,
                'live_signal_counts': dict(profile_hash_counter),
            })
    if live_startup_self_check and live_startup_self_check.get('ok') is False:
        contract_mismatches.append({
            'type': 'live_startup_self_check_warn',
            'failed_checks': [
                row.get('name')
                for row in live_startup_self_check.get('checks') or []
                if not row.get('ok')
            ],
        })

    report = {
        'schema_version': 1,
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'paths': paths,
        'files': {name: {'exists': os.path.exists(path), 'bytes': os.path.getsize(path) if os.path.exists(path) else 0}
                  for name, path in paths.items()},
        'trust_gate': {
            'ok': not contract_mismatches,
            'contract_mismatches': contract_mismatches,
            'live_status_read_error': live_status.get('_read_error'),
            'live_execution_contract_hash': live_execution_hash,
            'step2_execution_contract_hash': step2_execution_hash,
            'step2_parity_contract_hash': step2_parity_hash,
            'live_signal_step2_parity_contract_hash_counts': dict(live_parity_hashes),
            'live_active_profile_hash': live_profile_hash,
            'live_active_profile_name': live_active_profile.get('name'),
            'live_startup_self_check_status': live_startup_self_check.get('status'),
            'cache_recommended_action': cache_layers.get('recommended_action'),
            'cache_stale_layers': cache_layers.get('stale_layers'),
            'deduction': (
                'Trust Live-vs-Step2 comparisons only when contract/profile hashes match and cache layers are fresh. '
                'A mismatch means the report can still diagnose logs, but not score parity reliably.'
            ),
        },
        'execution_replay_inputs': execution_replay,
        'canonical_opportunities': canonical_summary,
        'canonical_decision_packets': canonical_packet_diff,
        'parity_verdict': parity_verdict,
        'market_data_integrity': market_data_integrity,
        'market_data_freshness': market_data_freshness,
        'parity_diff_classifier': diff_classification,
        'artifact_registry': artifact_registry,
        'cache_layers': cache_layers,
        'summary': {
            'live_signal_rows': len(parity),
            'step2_decision_rows': len(step2),
            'entered_signal_rows': len(entered),
            'skipped_signal_rows': len(skipped_parity),
            'trade_rows': len(trades),
            'skipped_signal_file_rows': len(skipped),
            'latency_rows': len(latency),
            'live_decision_source_rows': len(live_decision_source),
            'intraday_shadow_step2_rows': shadow_summary.get('rows'),
            'intraday_shadow_step2_critical': shadow_summary.get('critical_count'),
            'intraday_shadow_step2_warnings': shadow_summary.get('warning_count'),
            'shadow_variant_profile_count': shadow_variants.get('profile_count'),
            'shadow_variant_signal_rows': shadow_variants.get('signal_rows'),
            'shadow_variant_top_pnl': ((shadow_variants.get('top5') or [{}])[0]).get('pnl') if shadow_variants else None,
            'candidate_lifecycle_candidates': candidate_lifecycle_dashboard.get('candidate_count'),
            'candidate_lifecycle_status_counts': candidate_lifecycle_dashboard.get('status_counts'),
            'deterministic_live_replay_ok': deterministic_replay.get('ok'),
            'deterministic_live_replay_blockers': deterministic_replay.get('blockers'),
            'architecture_drift_ok': architecture_drift.get('ok'),
            'architecture_drift_failed_count': architecture_drift.get('failed_count'),
            'execution_lifecycle_rows': lifecycle_summary.get('rows'),
            'execution_lifecycle_anomalies': len(lifecycle_summary.get('anomalies') or []),
            'realtime_parity_alert_rows': len(realtime_alerts),
            'contract_gate_ok': contract_gate_report.get('ok'),
            'contract_gate_failures': contract_gate_report.get('critical_failure_count'),
            'entered_without_trade_row': len(entered_trade_ids - trade_ids),
            'trades_without_entered_signal_row': len(trade_ids - entered_trade_ids),
            'trades_without_latency_row': len(trade_ids - latency_trade_ids),
            'matched_live_step2_keys': len(shared_keys),
            'live_only_signal_keys': len(live_only_keys),
            'step2_only_signal_keys': len(step2_only_keys),
            'decision_mismatch_count': len(decision_mismatches),
            'feature_mismatch_count': len(feature_mismatches),
            'outcome_mismatch_count': len(outcome_mismatches),
            'canonical_opportunity_rows': canonical_summary.get('rows'),
            'canonical_packet_shared': canonical_packet_diff.get('shared_entry_packets'),
            'canonical_packet_mismatches': canonical_packet_diff.get('mismatch_count'),
            'canonical_packet_live_only': canonical_packet_diff.get('live_only_entry_packets'),
            'canonical_packet_step2_only': canonical_packet_diff.get('step2_only_entry_packets'),
            'parity_verdict': parity_verdict.get('verdict'),
            'parity_promotion_safe': parity_verdict.get('promotion_safe'),
            'parity_critical_count': parity_verdict.get('critical_count'),
            'parity_warning_count': parity_verdict.get('warning_count'),
            'market_data_integrity_verdict': market_data_integrity.get('verdict'),
            'market_data_integrity_promotion_safe': market_data_integrity.get('promotion_safe'),
            'market_data_integrity_critical_count': market_data_integrity.get('critical_count'),
            'market_data_integrity_warning_count': market_data_integrity.get('warning_count'),
            'market_data_freshness_verdict': market_data_freshness.get('verdict'),
            'market_data_freshness_critical_count': market_data_freshness.get('critical_count'),
            'market_data_freshness_warning_count': market_data_freshness.get('warning_count'),
            'order_lifecycle_verdict': order_lifecycle.get('verdict'),
            'order_lifecycle_critical_count': order_lifecycle.get('critical_count'),
            'order_lifecycle_warning_count': order_lifecycle.get('warning_count'),
        },
        'intraday_shadow_step2': shadow_summary or {'summary_exists': False, 'path': paths['intraday_shadow_step2_summary']},
        'shadow_variants': shadow_variants or {'exists': False, 'path': paths['shadow_variant_leaderboard']},
        'candidate_lifecycle': candidate_lifecycle_dashboard or {'exists': False, 'path': paths['candidate_lifecycle_dashboard']},
        'deterministic_live_replay': deterministic_replay or {'exists': False, 'path': paths['deterministic_live_replay']},
        'architecture_drift_gate': architecture_drift or {'exists': False, 'path': paths['architecture_drift_gate']},
        'execution_lifecycle': lifecycle_summary or {'summary_exists': False, 'path': paths['execution_lifecycle_summary']},
        'order_lifecycle_reconciliation': order_lifecycle,
        'realtime_parity_alerts': {
            'rows': len(realtime_alerts),
            'path': paths['realtime_parity_alerts'],
            'latest': realtime_alerts[-1] if realtime_alerts else None,
        },
        'artifact_dag': artifact_dag or {'exists': False, 'path': paths['artifact_dag']},
        'contract_gate': contract_gate_report or {'exists': False, 'path': paths['contract_gate']},
        'step2_decision_parity': {
            'summary_exists': bool(step2_summary),
            'rows': step2_summary.get('rows', len(step2)),
            'entered': step2_summary.get('entered'),
            'skipped': step2_summary.get('skipped'),
            'pnl': step2_summary.get('pnl'),
            'path': step2_summary.get('path') or paths['step2_decision_parity'],
            'run_context': step2_summary.get('run_context') or {},
        },
        'step2_data_freshness': {
            'compare_exists': bool(compare),
            'intraday_exists': compare.get('intraday_exists'),
            'canonical_exists': compare.get('canonical_exists'),
            'intraday_rows': (compare.get('intraday') or {}).get('rows'),
            'canonical_rows': (compare.get('canonical_summary') or {}).get('rows'),
            'overlap_rows': compare.get('overlap_rows'),
            'missing_from_intraday_rows': compare.get('missing_from_intraday_rows'),
            'live_only_rows': compare.get('live_only_rows'),
            'compare_path': compare.get('compare_path') or paths['step2_freshness_compare'],
            'market_data_integrity_verdict': market_data_integrity.get('verdict'),
            'market_data_integrity_promotion_safe': market_data_integrity.get('promotion_safe'),
            'market_data_integrity_critical_count': market_data_integrity.get('critical_count'),
            'market_data_integrity_warning_count': market_data_integrity.get('warning_count'),
            'market_data_integrity_path': market_data_integrity.get('path') or paths['market_data_integrity'],
        },
        'decisions': {
            'by_decision': _counter_dict(decision_counter),
            'by_ticker_side': _counter_dict(group_counter),
            'top_skip_reasons_from_parity': _counter_dict(reason_counter),
            'top_skip_reasons_from_skipped_file': _counter_dict(skipped_file_reason_counter),
        },
        'strategy_identity': {
            'live_active_profile': live_active_profile,
            'profiles': _counter_dict(profile_counter),
            'active_profile_hashes': _counter_dict(profile_hash_counter),
            'feature_hash_count': len(feature_counter),
            'top_feature_hashes': dict(feature_counter.most_common(10)),
            'strategy_config_hashes': _counter_dict(Counter(str(r.get('strategy_config_hash') or 'missing') for r in parity)),
            'step2_contract_hashes': _counter_dict(Counter(str(r.get('step2_parity_contract_hash') or 'missing') for r in parity)),
        },
        'market_freshness': _freshness_summary(parity),
        'live_trade_results': _trade_summary(trades),
        'latency': {
            'entry_time_to_submit_ms': _latency_stats(latency, 'entry_time_to_submit_ms'),
            'entry_time_to_fill_ms': _latency_stats(latency, 'entry_time_to_fill_ms'),
            'entry_broker_submit_ms': _latency_stats(latency, 'entry_broker_submit_ms'),
            'exit_time_to_submit_ms': _latency_stats(latency, 'exit_time_to_submit_ms'),
            'exit_time_to_fill_ms': _latency_stats(latency, 'exit_time_to_fill_ms'),
            'exit_time_to_flat_ms': _latency_stats(latency, 'exit_time_to_flat_ms'),
        },
        'investigation_queues': {
            'entered_without_trade_row': sorted([x for x in entered_trade_ids - trade_ids if x])[:50],
            'trades_without_entered_signal_row': sorted([x for x in trade_ids - entered_trade_ids if x])[:50],
            'trades_without_latency_row': sorted([x for x in trade_ids - latency_trade_ids if x])[:50],
            'live_only_signal_examples': [
                {
                    'parity_key': key,
                    'created_at_ct': live_by_key[key].get('created_at_ct'),
                    'ticker': live_by_key[key].get('ticker'),
                    'side': live_by_key[key].get('side'),
                    'decision': live_by_key[key].get('decision'),
                    'reason': live_by_key[key].get('reason'),
                }
                for key in sorted(live_only_keys)[:50]
            ],
            'step2_only_signal_examples': [
                {
                    'parity_key': key,
                    'created_at_ct': step2_by_key[key].get('created_at_ct'),
                    'ticker': step2_by_key[key].get('ticker'),
                    'side': step2_by_key[key].get('side'),
                    'decision': step2_by_key[key].get('decision'),
                    'reason': step2_by_key[key].get('reason'),
                }
                for key in sorted(step2_only_keys)[:50]
            ],
            'decision_mismatches': decision_mismatches[:50],
            'feature_mismatches': feature_mismatches[:50],
            'outcome_mismatches': outcome_mismatches[:50],
            'top_operational_skip_examples': [
                {
                    'created_at_ct': row.get('created_at_ct'),
                    'ticker': row.get('ticker'),
                    'side': row.get('side'),
                    'reason': row.get('reason'),
                    'parity_key': row.get('parity_key'),
                }
                for row in skipped_parity[:50]
            ],
        },
        'deduction': (
            'This report anchors parity investigations by joining Live signal decisions, Live trade outcomes, '
            'latency attribution, skipped-signal reasons, and Step 2 feed freshness. If Step 2 looked profitable '
            'but Live did not, start with missing intraday rows, skipped operational reasons, trade/decision join '
            'gaps, and latency p95/max values.'
        ),
    }
    return report


def render_text(report: dict[str, Any]) -> str:
    s = report['summary']
    trade = report['live_trade_results']
    fresh = report['step2_data_freshness']
    step2 = report.get('step2_decision_parity') or {}
    latency = report['latency']
    trust = report.get('trust_gate') or {}
    replay_inputs = report.get('execution_replay_inputs') or {}
    replay_coverage = replay_inputs.get('coverage') or {}
    canonical = report.get('canonical_opportunities') or {}
    canonical_packets = report.get('canonical_decision_packets') or {}
    parity_verdict = report.get('parity_verdict') or {}
    market_data_integrity = report.get('market_data_integrity') or {}
    market_data_freshness = report.get('market_data_freshness') or {}
    diff = report.get('parity_diff_classifier') or {}
    registry = report.get('artifact_registry') or {}
    shadow = report.get('intraday_shadow_step2') or {}
    shadow_variants = report.get('shadow_variants') or {}
    candidate_lifecycle = report.get('candidate_lifecycle') or {}
    deterministic_replay = report.get('deterministic_live_replay') or {}
    architecture_drift = report.get('architecture_drift_gate') or {}
    lifecycle = report.get('execution_lifecycle') or {}
    order_lifecycle = report.get('order_lifecycle_reconciliation') or {}
    lines = [
        f"Live vs Step 2 Parity Report - {report['day']}",
        f"Created CT: {report['created_at_ct']}",
        "",
        "Trust gate",
        f"- OK: {trust.get('ok')}",
        f"- Live active profile: {trust.get('live_active_profile_name')} hash={trust.get('live_active_profile_hash')}",
        f"- Live startup self-check: {trust.get('live_startup_self_check_status')}",
        f"- Live execution contract: {trust.get('live_execution_contract_hash')}",
        f"- Step 2 execution contract: {trust.get('step2_execution_contract_hash')}",
        f"- Cache action: {trust.get('cache_recommended_action')} stale_layers={trust.get('cache_stale_layers')}",
        f"- Contract mismatches: {json.dumps(trust.get('contract_mismatches') or [], sort_keys=True)}",
        "",
        "Core counts",
        f"- Live signal rows: {s['live_signal_rows']} ({s['entered_signal_rows']} entered, {s['skipped_signal_rows']} skipped)",
        f"- Step 2 decision rows: {s['step2_decision_rows']} ({step2.get('entered')} entered, {step2.get('skipped')} skipped, P/L={step2.get('pnl')})",
        f"- Matched Live/Step2 keys: {s['matched_live_step2_keys']}",
        f"- Live-only keys: {s['live_only_signal_keys']} | Step2-only keys: {s['step2_only_signal_keys']}",
        f"- Decision mismatches: {s['decision_mismatch_count']} | Feature mismatches: {s['feature_mismatch_count']} | Outcome mismatches: {s['outcome_mismatch_count']}",
        f"- Shadow Step 2 rows: {s.get('intraday_shadow_step2_rows')} critical={s.get('intraday_shadow_step2_critical')} warnings={s.get('intraday_shadow_step2_warnings')}",
        f"- Shadow variants: profiles={s.get('shadow_variant_profile_count')} top_pnl={s.get('shadow_variant_top_pnl')}",
        f"- Candidate lifecycle: candidates={s.get('candidate_lifecycle_candidates')} statuses={json.dumps(s.get('candidate_lifecycle_status_counts') or {}, sort_keys=True)}",
        f"- Deterministic replay: ok={s.get('deterministic_live_replay_ok')} blockers={s.get('deterministic_live_replay_blockers')}",
        f"- Architecture drift: ok={s.get('architecture_drift_ok')} failed={s.get('architecture_drift_failed_count')}",
        f"- Lifecycle rows: {s.get('execution_lifecycle_rows')} anomalies={s.get('execution_lifecycle_anomalies')}",
        f"- Realtime parity alerts: {s.get('realtime_parity_alert_rows')}",
        f"- Contract gate: ok={s.get('contract_gate_ok')} critical_failures={s.get('contract_gate_failures')}",
        f"- Canonical opportunity rows: {s.get('canonical_opportunity_rows')}",
        f"- Canonical packet shared/live-only/step2-only: {s.get('canonical_packet_shared')}/{s.get('canonical_packet_live_only')}/{s.get('canonical_packet_step2_only')}",
        f"- Canonical packet mismatches: {s.get('canonical_packet_mismatches')}",
        f"- Parity verdict: {s.get('parity_verdict')} promotion_safe={s.get('parity_promotion_safe')} critical={s.get('parity_critical_count')} warnings={s.get('parity_warning_count')}",
        f"- Market data integrity: {s.get('market_data_integrity_verdict')} promotion_safe={s.get('market_data_integrity_promotion_safe')} critical={s.get('market_data_integrity_critical_count')} warnings={s.get('market_data_integrity_warning_count')}",
        f"- Market data freshness: {s.get('market_data_freshness_verdict')} critical={s.get('market_data_freshness_critical_count')} warnings={s.get('market_data_freshness_warning_count')}",
        f"- Order lifecycle: {s.get('order_lifecycle_verdict')} critical={s.get('order_lifecycle_critical_count')} warnings={s.get('order_lifecycle_warning_count')}",
        f"- Trade rows: {s['trade_rows']} | P/L: {trade['pnl']}",
        f"- Skipped signal file rows: {s['skipped_signal_file_rows']}",
        f"- Latency rows: {s['latency_rows']}",
        f"- Entered signals missing trade rows: {s['entered_without_trade_row']}",
        f"- Trades missing entered-signal rows: {s['trades_without_entered_signal_row']}",
        f"- Trades missing latency rows: {s['trades_without_latency_row']}",
        "",
        "Step 2 feed freshness",
        f"- Intraday tape exists: {fresh['intraday_exists']} rows={fresh['intraday_rows']}",
        f"- Canonical tape exists: {fresh['canonical_exists']} rows={fresh['canonical_rows']}",
        f"- Overlap rows: {fresh['overlap_rows']}",
        f"- Missing from intraday: {fresh['missing_from_intraday_rows']}",
        f"- Live-only rows: {fresh['live_only_rows']}",
        f"- Integrity verdict: {fresh.get('market_data_integrity_verdict')} promotion_safe={fresh.get('market_data_integrity_promotion_safe')}",
        f"- Integrity path: {fresh.get('market_data_integrity_path')}",
        "",
        "Live trade results",
        f"- Wins/Losses: {trade['wins']}/{trade['losses']}",
        f"- By ticker: {json.dumps(trade['by_ticker'], sort_keys=True)}",
        "",
        "Top skip reasons",
    ]
    for reason, count in list(report['decisions']['top_skip_reasons_from_parity'].items())[:10]:
        lines.append(f"- {reason}: {count}")
    lines.extend([
        "",
        "Latency",
        f"- Entry signal to fill: {latency['entry_time_to_fill_ms']}",
        f"- Exit trigger to fill: {latency['exit_time_to_fill_ms']}",
        f"- Exit trigger to flat: {latency['exit_time_to_flat_ms']}",
        "",
        "Execution replay inputs",
        f"- Path: {replay_inputs.get('path')}",
        f"- Complete rows: {replay_coverage.get('complete_rows')}/{replay_coverage.get('rows')} ({replay_coverage.get('complete_pct')}%)",
        f"- Missing fields: {json.dumps(replay_coverage.get('missing_field_counts') or {}, sort_keys=True)}",
        "",
        "Canonical opportunity ledger",
        f"- Path: {canonical.get('path')}",
        f"- Live/Step2 counts: {json.dumps(canonical.get('counts') or {}, sort_keys=True)}",
        f"- P/L: {json.dumps(canonical.get('pnl') or {}, sort_keys=True)}",
        "",
        "Canonical decision packets",
        f"- Diff path: {canonical_packets.get('path')}",
        f"- Schema OK: {(canonical_packets.get('schema_compatibility') or {}).get('ok')}",
        f"- Field mismatch counts: {json.dumps(canonical_packets.get('field_mismatch_counts') or {}, sort_keys=True)}",
        "",
        "Parity verdict",
        f"- Verdict: {parity_verdict.get('verdict')}",
        f"- Promotion safe: {parity_verdict.get('promotion_safe')}",
        f"- Issues: critical={parity_verdict.get('critical_count')} warnings={parity_verdict.get('warning_count')}",
        f"- Issue counts: {json.dumps(parity_verdict.get('issue_kind_counts') or {}, sort_keys=True)}",
        f"- Path: {parity_verdict.get('path')}",
        "",
        "Market data integrity",
        f"- Verdict: {market_data_integrity.get('verdict')}",
        f"- Promotion safe: {market_data_integrity.get('promotion_safe')}",
        f"- Issues: critical={market_data_integrity.get('critical_count')} warnings={market_data_integrity.get('warning_count')}",
        f"- Source used: {market_data_integrity.get('source_used')}",
        f"- Tape: {(market_data_integrity.get('source_tape') or {}).get('path')}",
        f"- Issue counts: {json.dumps(market_data_integrity.get('issue_kind_counts') or {}, sort_keys=True)}",
        "",
        "Market data freshness",
        f"- Verdict: {market_data_freshness.get('verdict')}",
        f"- Issues: critical={market_data_freshness.get('critical_count')} warnings={market_data_freshness.get('warning_count')}",
        f"- Path: {market_data_freshness.get('path')}",
        "",
        "Order lifecycle",
        f"- Verdict: {order_lifecycle.get('verdict')}",
        f"- Issues: critical={order_lifecycle.get('critical_count')} warnings={order_lifecycle.get('warning_count')}",
        f"- Path: {order_lifecycle.get('path')}",
        "",
        "Diff classifier",
        f"- Path: {diff.get('path')}",
        f"- Classes: {json.dumps(diff.get('counts') or {}, sort_keys=True)}",
        f"- P/L gap by class: {json.dumps(diff.get('pnl_gap_by_class') or {}, sort_keys=True)}",
        "",
        "Artifact registry",
        f"- Registry hash: {registry.get('registry_hash')}",
        "",
        "Shadow variants",
        f"- Leaderboard: {shadow_variants.get('path')}",
        f"- Top 5: {json.dumps(shadow_variants.get('top5') or [], sort_keys=True)[:1200]}",
        "",
        "Candidate lifecycle",
        f"- Dashboard: {candidate_lifecycle.get('output', {}).get('json_path') or candidate_lifecycle.get('path')}",
        f"- Statuses: {json.dumps(candidate_lifecycle.get('status_counts') or {}, sort_keys=True)}",
        f"- Promotion ready: {json.dumps(candidate_lifecycle.get('promotion_ready') or [], sort_keys=True)[:1200]}",
        "",
        "Deterministic replay / drift",
        f"- Replay path: {deterministic_replay.get('path')} ok={deterministic_replay.get('ok')}",
        f"- Drift path: {architecture_drift.get('path')} ok={architecture_drift.get('ok')}",
        "",
        "Use",
        "- If Step 2 and Live diverge, start with the diff classifier, then feed freshness, skipped operational reasons, missing joins, and latency tails.",
        "",
    ])
    return '\n'.join(lines)


def build_and_write(day: str | None = None, out_dir: str = OUT_DIR) -> dict[str, Any]:
    day = day or _today()
    report = build_report(day)
    json_path = os.path.join(out_dir, f'live_step2_parity_{day}.json')
    txt_path = os.path.join(out_dir, f'live_step2_parity_{day}.txt')
    report['output'] = {
        'json_path': os.path.abspath(json_path),
        'txt_path': os.path.abspath(txt_path),
    }
    _write_json(json_path, report)
    _write_text(txt_path, render_text(report))
    try:
        tickers = sorted({str(row.get('ticker') or '').upper()
                          for row in _jsonl_rows(report['paths']['live_signal_parity'])
                          if row.get('ticker')}) or ['CLSK', 'MARA', 'RIOT']
        registry_path = artifact_version_registry.write(day, tickers, extra_artifacts={
            'live_step2_parity_report_json': json_path,
            'live_step2_parity_report_txt': txt_path,
            'execution_replay_inputs': (report.get('execution_replay_inputs') or {}).get('path') or '',
            'canonical_opportunities': (report.get('canonical_opportunities') or {}).get('path') or '',
            'canonical_live_decision_packets': (report.get('paths') or {}).get('canonical_live_decision_packets') or '',
            'canonical_step2_decision_packets': (report.get('paths') or {}).get('canonical_step2_decision_packets') or '',
            'canonical_live_exit_packets': (report.get('paths') or {}).get('canonical_live_exit_packets') or '',
            'canonical_packet_diff': (report.get('canonical_decision_packets') or {}).get('path') or '',
            'parity_verdict': (report.get('parity_verdict') or {}).get('path') or '',
            'parity_verdict_text': (report.get('parity_verdict') or {}).get('text_path') or '',
            'market_data_integrity': (report.get('market_data_integrity') or {}).get('path') or '',
            'market_data_integrity_text': (report.get('market_data_integrity') or {}).get('text_path') or '',
            'market_data_freshness': (report.get('market_data_freshness') or {}).get('path') or '',
            'market_data_freshness_text': (report.get('market_data_freshness') or {}).get('text_path') or '',
            'parity_diff_classifier': (report.get('parity_diff_classifier') or {}).get('path') or '',
            'unified_live_decision_source': (report.get('paths') or {}).get('unified_live_decision_source') or '',
            'intraday_shadow_step2': (report.get('paths') or {}).get('intraday_shadow_step2') or '',
            'intraday_shadow_step2_summary': (report.get('paths') or {}).get('intraday_shadow_step2_summary') or '',
            'shadow_variant_decisions': (report.get('paths') or {}).get('shadow_variant_decisions') or '',
            'shadow_variant_leaderboard': (report.get('paths') or {}).get('shadow_variant_leaderboard') or '',
            'candidate_lifecycle_dashboard': (report.get('paths') or {}).get('candidate_lifecycle_dashboard') or '',
            'candidate_lifecycle_registry': (report.get('paths') or {}).get('candidate_lifecycle_registry') or '',
            'deterministic_live_replay': (report.get('paths') or {}).get('deterministic_live_replay') or '',
            'architecture_drift_gate': (report.get('paths') or {}).get('architecture_drift_gate') or '',
            'execution_lifecycle': (report.get('paths') or {}).get('execution_lifecycle') or '',
            'execution_lifecycle_summary': (report.get('paths') or {}).get('execution_lifecycle_summary') or '',
            'order_lifecycle_reconciliation': (report.get('order_lifecycle_reconciliation') or {}).get('path') or '',
            'order_lifecycle_reconciliation_text': (report.get('order_lifecycle_reconciliation') or {}).get('text_path') or '',
            'realtime_parity_alerts': (report.get('paths') or {}).get('realtime_parity_alerts') or '',
            'artifact_dag': (report.get('paths') or {}).get('artifact_dag') or '',
            'contract_gate': (report.get('paths') or {}).get('contract_gate') or '',
        })
        report['artifact_registry'] = _read_json(registry_path)
        _write_json(json_path, report)
        _write_text(txt_path, render_text(report))
    except Exception as exc:
        report['artifact_registry_error'] = repr(exc)
    return report


def main() -> int:
    ap = argparse.ArgumentParser(description='Build Live-vs-Step2 end-of-day parity report.')
    ap.add_argument('day', nargs='?', default=None)
    ap.add_argument('--out-dir', default=OUT_DIR)
    ap.add_argument('--json', action='store_true')
    args = ap.parse_args()
    report = build_and_write(args.day or _today(), args.out_dir)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True, default=str))
    else:
        print(report['output']['json_path'])
        print(report['output']['txt_path'])
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
