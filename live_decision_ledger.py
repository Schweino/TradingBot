"""Append-only source-of-truth decision rows from the live/mock engine."""
from __future__ import annotations

from output_paths import output_path

import json
import os
from datetime import datetime
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore


HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = output_path('postmortem', 'unified_decision_ledger')
CT = ZoneInfo('America/Chicago')
SCHEMA_VERSION = 1


def _day_from_ts(ts: int | float | None) -> str:
    try:
        return datetime.fromtimestamp(float(ts or 0), CT).date().isoformat()
    except Exception:
        return datetime.now(CT).date().isoformat()


def _path(day: str) -> str:
    return os.path.join(OUT_DIR, day, f'live_decision_source_{day}.jsonl')


def append(row: dict[str, Any], day: str | None = None) -> str:
    ts = row.get('created_at') or row.get('closed_at') or row.get('opened_at')
    day_iso = day or row.get('day') or _day_from_ts(ts)
    out = dict(row)
    out.setdefault('schema_version', SCHEMA_VERSION)
    out.setdefault('day', day_iso)
    out.setdefault('created_at_ct', datetime.now(CT).isoformat(timespec='seconds'))
    path = _path(day_iso)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'a', encoding='utf-8') as f:
        f.write(json.dumps(out, separators=(',', ':'), default=str) + '\n')
    return os.path.abspath(path)


def from_live_signal_parity(parity_row: dict[str, Any],
                            event_type: str = 'signal_decision',
                            decision_stage: str = 'entry') -> dict[str, Any]:
    return {
        'schema_version': SCHEMA_VERSION,
        'event_type': event_type,
        'decision_stage': decision_stage,
        'created_at': parity_row.get('created_at'),
        'day': _day_from_ts(parity_row.get('created_at')),
        'parity_key': parity_row.get('parity_key'),
        'ticker': parity_row.get('ticker'),
        'side': parity_row.get('side'),
        'setup_type': parity_row.get('setup_type'),
        'decision': parity_row.get('decision'),
        'reason': parity_row.get('reason'),
        'price': parity_row.get('price'),
        'trade_id': parity_row.get('trade_id'),
        'client_order_id': parity_row.get('client_order_id'),
        'broker_order_id': parity_row.get('broker_order_id'),
        'strategy_config_hash': parity_row.get('strategy_config_hash'),
        'execution_kernel_hash': parity_row.get('execution_kernel_hash'),
        'step2_parity_contract_hash': parity_row.get('step2_parity_contract_hash'),
        'step2_execution_contract_hash': parity_row.get('step2_execution_contract_hash'),
        'active_profile_name': parity_row.get('active_profile_name'),
        'active_profile_hash': parity_row.get('active_profile_hash'),
        'feature_snapshot_hash': parity_row.get('feature_snapshot_hash'),
        'join_hint': parity_row.get('join_hint') or {},
        'source_row': 'live_signal_parity',
    }


def from_closed_trade(trade: dict[str, Any]) -> dict[str, Any]:
    return {
        'schema_version': SCHEMA_VERSION,
        'event_type': 'trade_outcome',
        'decision_stage': 'exit',
        'created_at': trade.get('closed_at'),
        'day': _day_from_ts(trade.get('closed_at')),
        'trade_id': trade.get('trade_id'),
        'ticker': trade.get('ticker'),
        'side': trade.get('side'),
        'decision': 'closed',
        'reason': trade.get('reason'),
        'entry': trade.get('entry'),
        'exit': trade.get('exit'),
        'sl': trade.get('sl'),
        'tp': trade.get('tp'),
        'qty': trade.get('qty'),
        'pnl': trade.get('pnl'),
        'result': trade.get('result'),
        'opened_at': trade.get('opened_at'),
        'closed_at': trade.get('closed_at'),
        'strategy_config_hash': trade.get('strategy_config_hash'),
        'exit_decision_context': trade.get('exit_decision_context'),
        'bracket_policy': trade.get('bracket_policy'),
        'latency_chain': trade.get('latency_chain'),
        'entry_timing': trade.get('entry_timing'),
        'exit_latency_attribution': trade.get('exit_latency_attribution'),
        'source_row': 'trade_closed',
    }
