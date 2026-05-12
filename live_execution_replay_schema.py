"""Canonical Live execution replay input schema for parity investigations."""
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
CT = ZoneInfo('America/Chicago')
OUT_DIR = output_path('postmortem', 'execution_replay_inputs')

SCHEMA_VERSION = 1
REQUIRED_FIELDS = (
    'trade_id',
    'ticker',
    'side',
    'signal_ts',
    'entry_submit_ts',
    'entry_fill_ts',
    'entry_signal_price',
    'entry_fill_price',
    'tp_price',
    'sl_price',
    'exit_trigger_ts',
    'exit_fill_ts',
    'exit_fill_price',
    'exit_reason',
)


def _num(value: Any) -> float | None:
    try:
        if value in (None, ''):
            return None
        return float(value)
    except Exception:
        return None


def _int(value: Any) -> int | None:
    try:
        if value in (None, ''):
            return None
        return int(float(value))
    except Exception:
        return None


def _ts_from_ms(value: Any) -> int | None:
    raw = _int(value)
    if raw is None:
        return None
    return int(raw / 1000) if raw > 10_000_000_000 else raw


def _iso(ts: int | None) -> str | None:
    if not ts:
        return None
    return datetime.fromtimestamp(int(ts), CT).isoformat(timespec='seconds')


def build_row(trade: dict[str, Any], signal: dict[str, Any] | None = None,
              latency: dict[str, Any] | None = None) -> dict[str, Any]:
    signal = signal or {}
    latency = latency or {}
    bracket = trade.get('bracket_policy') or {}
    entry_timing = trade.get('entry_timing') or {}
    latency_chain = trade.get('latency_chain') or {}
    broker_close = trade.get('broker_close') or {}
    broker_exit_fill = broker_close.get('fill') if isinstance(broker_close, dict) else {}
    entry_submit_ts = (
        _ts_from_ms(latency_chain.get('entry_submit_ms'))
        or _ts_from_ms(latency.get('entry_submit_ms'))
        or _int(trade.get('opened_at'))
    )
    entry_fill_ts = (
        _ts_from_ms(latency_chain.get('entry_fill_ms'))
        or _ts_from_ms(latency.get('entry_fill_ms'))
        or _int(trade.get('entry_filled_ts'))
        or _int(trade.get('opened_at'))
    )
    exit_trigger_ts = (
        _ts_from_ms(latency_chain.get('exit_trigger_ms'))
        or _ts_from_ms(latency.get('exit_trigger_ms'))
        or _int(trade.get('closed_at'))
    )
    exit_fill_ts = (
        _ts_from_ms(latency_chain.get('exit_fill_ms'))
        or _ts_from_ms(latency.get('exit_fill_ms'))
        or _int(trade.get('closed_at'))
    )
    row = {
        'schema_version': SCHEMA_VERSION,
        'trade_id': trade.get('trade_id'),
        'ticker': trade.get('ticker'),
        'side': trade.get('side'),
        'signal_ts': _int(signal.get('created_at') or signal.get('ts') or trade.get('opened_at')),
        'entry_submit_ts': entry_submit_ts,
        'entry_fill_ts': entry_fill_ts,
        'entry_signal_price': _num(signal.get('price') or entry_timing.get('signal_price') or trade.get('entry')),
        'entry_fill_price': _num(trade.get('entry_fill_price') or trade.get('broker_entry_fill_price') or trade.get('entry')),
        'tp_price': _num(bracket.get('tp') or bracket.get('take_profit') or trade.get('tp')),
        'sl_price': _num(bracket.get('sl') or bracket.get('stop_loss') or trade.get('sl')),
        'exit_trigger_ts': exit_trigger_ts,
        'exit_fill_ts': exit_fill_ts,
        'exit_fill_price': _num(
            trade.get('broker_exit_fill_price')
            or (broker_exit_fill or {}).get('filled_avg_price')
            or trade.get('exit')
        ),
        'exit_reason': trade.get('reason'),
        'pnl': _num(trade.get('pnl')),
        'mfe_pct': _num(trade.get('mfe_pct')),
        'mae_pct': _num(trade.get('mae_pct')),
        'latency_ms': {
            'entry_time_to_submit_ms': _num(latency.get('entry_time_to_submit_ms') or trade.get('entry_time_to_submit_ms')),
            'entry_time_to_fill_ms': _num(latency.get('entry_time_to_fill_ms') or trade.get('seconds_to_fill')),
            'exit_time_to_submit_ms': _num(latency.get('exit_time_to_submit_ms') or trade.get('exit_time_to_submit_ms')),
            'exit_time_to_fill_ms': _num(latency.get('exit_time_to_fill_ms') or trade.get('exit_time_to_fill_ms')),
            'exit_time_to_flat_ms': _num(latency.get('exit_time_to_flat_ms') or trade.get('exit_time_to_flat_ms')),
        },
    }
    row['timestamps_ct'] = {
        'signal': _iso(row['signal_ts']),
        'entry_submit': _iso(row['entry_submit_ts']),
        'entry_fill': _iso(row['entry_fill_ts']),
        'exit_trigger': _iso(row['exit_trigger_ts']),
        'exit_fill': _iso(row['exit_fill_ts']),
    }
    missing = [field for field in REQUIRED_FIELDS if row.get(field) in (None, '')]
    row['schema_complete'] = not missing
    row['missing_fields'] = missing
    return row


def coverage(rows: list[dict[str, Any]]) -> dict[str, Any]:
    missing_counts = {field: 0 for field in REQUIRED_FIELDS}
    complete = 0
    for row in rows:
        missing = row.get('missing_fields') or []
        if not missing:
            complete += 1
        for field in missing:
            if field in missing_counts:
                missing_counts[field] += 1
    total = len(rows)
    return {
        'schema_version': SCHEMA_VERSION,
        'rows': total,
        'complete_rows': complete,
        'complete_pct': round(100.0 * complete / total, 2) if total else None,
        'missing_field_counts': {k: v for k, v in missing_counts.items() if v},
        'required_fields': list(REQUIRED_FIELDS),
    }


def write_day(day: str, trades: list[dict[str, Any]], signals_by_trade_id: dict[str, dict[str, Any]] | None = None,
              latency_by_trade_id: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    signals_by_trade_id = signals_by_trade_id or {}
    latency_by_trade_id = latency_by_trade_id or {}
    rows = [
        build_row(
            trade,
            signals_by_trade_id.get(str(trade.get('trade_id') or '')),
            latency_by_trade_id.get(str(trade.get('trade_id') or '')),
        )
        for trade in trades
    ]
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, f'execution_replay_inputs_{day}.jsonl')
    tmp = f'{path}.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        for row in rows:
            f.write(json.dumps(row, sort_keys=True, separators=(',', ':'), default=str) + '\n')
    os.replace(tmp, path)
    return {
        'path': os.path.abspath(path),
        'coverage': coverage(rows),
    }
