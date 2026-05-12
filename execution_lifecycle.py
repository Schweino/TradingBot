"""Append-only order lifecycle ledger and replay helpers.

This is the event-sourced mirror for Live. It does not make trading decisions;
it records the lifecycle milestones that let us reconstruct what Live believed
was true at any moment.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import execution_state_reducer


HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, 'postmortem', 'execution_lifecycle')
CT = ZoneInfo('America/Chicago')
SCHEMA_VERSION = 1

EVENT_STAGE = {
    'signal_seen': 'signal_seen',
    'signal_skipped': 'rejected',
    'entry_intent_created': 'intent_created',
    'entry_execution_result': 'execution_result',
    'exit_intent_created': 'intent_created',
    'exit_execution_result': 'execution_result',
    'entry_reserved': 'reserved',
    'entry_pre_submit_blocked': 'rejected',
    'entry_submitted': 'submitted',
    'entry_committed': 'opened',
    'entry_filled': 'filled',
    'entry_fill_timeout_cancelled': 'cancelled',
    'bracket_exit_filled': 'exit_filled',
    'local_strict_broker_cleanup_requested': 'exit_requested',
    'local_strict_broker_cleanup_submitted': 'exit_submitted',
    'broker_flat_verified': 'broker_flat',
    'position_closed': 'closed',
    'broker_orphan_bracket_recovered_on_boot': 'recovered',
    'boot_position_mismatch': 'reconcile_mismatch',
    'boot_internal_flat_reconcile': 'reconciled',
    'critical_state_save_failed': 'state_persistence_failed',
}

CLOSED_STAGES = {'closed', 'cancelled', 'rejected', 'broker_flat'}
OPEN_STAGES = {'reserved', 'submitted', 'opened', 'filled', 'exit_requested', 'exit_submitted', 'exit_filled'}


def _stable_hash(payload: Any) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(',', ':'), default=str)
    return hashlib.sha256(blob.encode('utf-8')).hexdigest()[:24]


def _day(ts: int | float | None = None) -> str:
    return datetime.fromtimestamp(float(ts or time.time()), CT).date().isoformat()


def _path(day: str) -> str:
    return os.path.join(OUT_DIR, day, f'execution_lifecycle_{day}.jsonl')


def _summary_path(day: str) -> str:
    return os.path.join(OUT_DIR, day, f'execution_lifecycle_{day}.summary.json')


def _audit_path(day: str) -> str:
    return os.path.join(HERE, 'audit', f'trade_lifecycle_{day}.jsonl')


def _read_jsonl(path: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not os.path.exists(path):
        return rows
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


def _normalize_audit_row(audit_row: dict[str, Any]) -> dict[str, Any] | None:
    event = str(audit_row.get('event') or '')
    stage = EVENT_STAGE.get(event)
    if not stage:
        return None
    data = audit_row.get('data') if isinstance(audit_row.get('data'), dict) else {}
    ts = int(audit_row.get('ts') or time.time())
    symbol = audit_row.get('symbol')
    decision_audit = data.get('decision_audit') if isinstance(data.get('decision_audit'), dict) else {}
    trade_id = data.get('trade_id') or decision_audit.get('trade_id')
    return {
        'schema_version': SCHEMA_VERSION,
        'event_id': _stable_hash({'stage': stage, 'event': event, 'symbol': symbol, 'trade_id': trade_id, 'ts': ts, 'data': data}),
        'source': 'legacy_mock_trader_audit_backfill',
        'ts': ts,
        'ts_ct': audit_row.get('ts_ct') or datetime.fromtimestamp(ts, CT).isoformat(timespec='seconds'),
        'day': _day(ts),
        'stage': stage,
        'event': event,
        'symbol': symbol,
        'trade_id': trade_id,
        'client_order_id': data.get('client_order_id'),
        'broker_order_id': data.get('order_id') or data.get('broker_order_id'),
        'data': data,
    }


def append_event(stage: str, event: str, symbol: str | None = None,
                 trade_id: str | None = None, data: dict[str, Any] | None = None,
                 ts: int | None = None, source: str = 'live') -> dict[str, Any]:
    ts = int(ts or time.time())
    day = _day(ts)
    data = data or {}
    row = {
        'schema_version': SCHEMA_VERSION,
        'event_id': _stable_hash({'stage': stage, 'event': event, 'symbol': symbol, 'trade_id': trade_id, 'ts': ts, 'data': data}),
        'source': source,
        'ts': ts,
        'ts_ct': datetime.fromtimestamp(ts, CT).isoformat(timespec='seconds'),
        'day': day,
        'stage': stage,
        'event': event,
        'symbol': symbol,
        'trade_id': trade_id or data.get('trade_id'),
        'client_order_id': data.get('client_order_id'),
        'broker_order_id': data.get('order_id') or data.get('broker_order_id'),
        'data': data,
    }
    path = _path(day)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'a', encoding='utf-8') as f:
        f.write(json.dumps(row, sort_keys=True, separators=(',', ':'), default=str) + '\n')
    try:
        from event_store import record_event
        record_event('execution_lifecycle', row, symbol=symbol, trade_id=row.get('trade_id'), ts=ts)
    except Exception:
        pass
    return row


def append_from_audit(audit_row: dict[str, Any]) -> dict[str, Any] | None:
    event = str(audit_row.get('event') or '')
    stage = EVENT_STAGE.get(event)
    if not stage:
        return None
    data = audit_row.get('data') if isinstance(audit_row.get('data'), dict) else {}
    return append_event(
        stage=stage,
        event=event,
        symbol=audit_row.get('symbol'),
        trade_id=data.get('trade_id'),
        data=data,
        ts=int(audit_row.get('ts') or time.time()),
        source='mock_trader_audit',
    )


def replay_day(day: str) -> dict[str, Any]:
    rows = sorted(_read_jsonl(_path(day)), key=lambda r: (int(r.get('ts') or 0), str(r.get('event_id') or '')))
    if not rows:
        rows = [
            row for row in (_normalize_audit_row(audit) for audit in _read_jsonl(_audit_path(day)))
            if row is not None
        ]
        rows.sort(key=lambda r: (int(r.get('ts') or 0), str(r.get('event_id') or '')))
    by_trade: dict[str, dict[str, Any]] = {}
    by_symbol_open: dict[str, list[str]] = defaultdict(list)
    orphan_rows = []
    inferred_trade_events = 0
    for row in rows:
        trade_id = str(row.get('trade_id') or '')
        symbol = str(row.get('symbol') or '')
        stage = str(row.get('stage') or '')
        if not trade_id and symbol and stage in (OPEN_STAGES | CLOSED_STAGES):
            # Older audit rows did not always carry trade_id on fill/close
            # milestones. Reattach them to the active symbol position during
            # replay so historical audits do not produce false open trades.
            open_ids = by_symbol_open.get(symbol) or []
            if open_ids and stage != 'opened':
                trade_id = str(open_ids[0])
                inferred_trade_events += 1
        if not trade_id:
            orphan_rows.append(row)
            continue
        state = by_trade.setdefault(trade_id, {
            'trade_id': trade_id,
            'symbol': symbol,
            'first_ts': row.get('ts'),
            'last_ts': row.get('ts'),
            'stage': None,
            'events': [],
        })
        state['last_ts'] = row.get('ts')
        state['stage'] = stage
        state['events'].append({
            'ts': row.get('ts'),
            'ts_ct': row.get('ts_ct'),
            'event': row.get('event'),
            'stage': stage,
        })
        if symbol and stage in OPEN_STAGES and trade_id not in by_symbol_open[symbol]:
            by_symbol_open[symbol].append(trade_id)
        if symbol and stage in CLOSED_STAGES and trade_id in by_symbol_open[symbol]:
            by_symbol_open[symbol].remove(trade_id)
    stage_counts = Counter(str(row.get('stage') or 'unknown') for row in rows)
    reducer_state = execution_state_reducer.from_events(rows, day=day)
    open_trades = [
        state for state in by_trade.values()
        if str(state.get('stage') or '') in OPEN_STAGES
    ]
    anomalies = []
    for symbol, ids in by_symbol_open.items():
        if len(ids) > 1:
            anomalies.append({'kind': 'multiple_open_trades_for_symbol', 'symbol': symbol, 'trade_ids': ids})
    for state in by_trade.values():
        events = [row.get('event') for row in state.get('events') or []]
        if state.get('stage') == 'closed' and 'entry_committed' not in events:
            anomalies.append({'kind': 'closed_without_entry_committed', 'trade_id': state.get('trade_id')})
    return {
        'schema_version': SCHEMA_VERSION,
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'path': os.path.abspath(_path(day)),
        'rows': len(rows),
        'trade_count': len(by_trade),
        'stage_counts': dict(sorted(stage_counts.items())),
        'authoritative_state': {
            'schema_version': reducer_state.get('schema_version'),
            'state_hash': reducer_state.get('state_hash'),
            'open_position_count': len(reducer_state.get('positions') or {}),
            'pending_entry_count': len(reducer_state.get('pending_entries') or {}),
            'last_closed_at': reducer_state.get('last_closed_at') or {},
        },
        'open_trade_count': len(open_trades),
        'open_trades': open_trades,
        'orphan_event_count': len(orphan_rows),
        'inferred_trade_event_count': inferred_trade_events,
        'anomalies': anomalies,
    }


def write_snapshot(day: str) -> dict[str, Any]:
    lifecycle_path = _path(day)
    if not os.path.exists(lifecycle_path):
        rows = [
            row for row in (_normalize_audit_row(audit) for audit in _read_jsonl(_audit_path(day)))
            if row is not None
        ]
        if rows:
            rows.sort(key=lambda r: (int(r.get('ts') or 0), str(r.get('event_id') or '')))
            os.makedirs(os.path.dirname(lifecycle_path), exist_ok=True)
            tmp_rows = f'{lifecycle_path}.{os.getpid()}.{int(time.time() * 1000)}.tmp'
            with open(tmp_rows, 'w', encoding='utf-8') as f:
                for row in rows:
                    f.write(json.dumps(row, sort_keys=True, separators=(',', ':'), default=str) + '\n')
            os.replace(tmp_rows, lifecycle_path)
    payload = replay_day(day)
    path = _summary_path(day)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f'{path}.{os.getpid()}.{int(time.time() * 1000)}.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
    os.replace(tmp, path)
    payload['summary_path'] = os.path.abspath(path)
    return payload


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser(description='Replay/write execution lifecycle state for a day.')
    ap.add_argument('day', nargs='?', default=_day())
    ap.add_argument('--json', action='store_true')
    args = ap.parse_args()
    payload = write_snapshot(args.day)
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
