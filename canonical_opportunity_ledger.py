"""Canonical opportunity ledger for Live, shadow, and Step 2 parity review."""
from __future__ import annotations

from output_paths import output_path

import json
import os
import time
from collections import Counter
from datetime import datetime
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore


HERE = os.path.dirname(os.path.abspath(__file__))
CT = ZoneInfo('America/Chicago')
OUT_DIR = output_path('postmortem', 'canonical_opportunities')
SCHEMA_VERSION = 1


def _jsonl_rows(path: str) -> list[dict[str, Any]]:
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


def _write_json(path: str, payload: dict[str, Any]) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f'{path}.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
    os.replace(tmp, path)
    return os.path.abspath(path)


def _write_jsonl(path: str, rows: list[dict[str, Any]]) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f'{path}.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        for row in rows:
            f.write(json.dumps(row, sort_keys=True, separators=(',', ':'), default=str) + '\n')
    os.replace(tmp, path)
    return os.path.abspath(path)


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


def _day_from_ts(ts: int | None) -> str:
    return datetime.fromtimestamp(int(ts or time.time()), CT).date().isoformat()


def _live_path(day: str) -> str:
    return os.path.join(OUT_DIR, f'live_opportunities_{day}.jsonl')


def ledger_path(day: str) -> str:
    return os.path.join(OUT_DIR, f'canonical_opportunities_{day}.jsonl')


def summary_path(day: str) -> str:
    return os.path.join(OUT_DIR, f'canonical_opportunities_{day}.summary.json')


def normalize_live_decision(row: dict[str, Any]) -> dict[str, Any]:
    extra = row.get('extra') or {}
    ts = _int(row.get('created_at'))
    decision = str(row.get('decision') or 'unknown')
    return {
        'schema_version': SCHEMA_VERSION,
        'source': 'live',
        'opportunity_key': row.get('parity_key'),
        'parity_key': row.get('parity_key'),
        'opportunity_id': row.get('opportunity_id'),
        'trade_id': row.get('trade_id'),
        'ticker': row.get('ticker'),
        'side': row.get('side'),
        'signal_ts': ts,
        'signal_ct': row.get('created_at_ct') or (
            datetime.fromtimestamp(ts, CT).isoformat(timespec='seconds') if ts else None
        ),
        'setup_type': row.get('setup_type'),
        'score': row.get('score'),
        'conviction': row.get('conviction'),
        'entry_price': _num(row.get('price')),
        'tp_price': _num(extra.get('tp')),
        'sl_price': _num(extra.get('sl')),
        'live_decision': 'accepted' if decision == 'entered' else 'rejected',
        'live_reason': row.get('reason'),
        'live_trade_id': row.get('trade_id'),
        'step2_decision': None,
        'step2_reason': None,
        'step2_expected_pnl': None,
        'live_outcome_pnl': None,
        'live_shadow_mode': True,
        'live_theoretical_decision': 'accepted' if decision == 'entered' else 'rejected',
        'broker_action': 'submitted' if decision == 'entered' else 'none',
        'feature_snapshot_hash': row.get('feature_snapshot_hash'),
        'active_profile_hash': row.get('active_profile_hash'),
        'strategy_config_hash': row.get('strategy_config_hash'),
        'step2_parity_contract_hash': row.get('step2_parity_contract_hash'),
        'step2_execution_contract_hash': row.get('step2_execution_contract_hash'),
        'join_hint': row.get('join_hint') or {},
    }


def append_live_decision(row: dict[str, Any]) -> dict[str, Any] | None:
    """Append a normalized live opportunity row as soon as Live sees the signal."""
    try:
        normalized = normalize_live_decision(row)
        day = _day_from_ts(normalized.get('signal_ts'))
        os.makedirs(OUT_DIR, exist_ok=True)
        with open(_live_path(day), 'a', encoding='utf-8') as f:
            f.write(json.dumps(normalized, sort_keys=True, separators=(',', ':'), default=str) + '\n')
        return normalized
    except Exception:
        return None


def _trade_by_id(day: str) -> dict[str, dict[str, Any]]:
    path = output_path('postmortem', 'trades', f'trades_{day}.jsonl')
    return {
        str(row.get('trade_id')): row
        for row in _jsonl_rows(path)
        if row.get('trade_id')
    }


def _step2_by_key(day: str) -> dict[str, dict[str, Any]]:
    path = output_path('postmortem', 'step2_decision_parity', f'step2_decision_parity_{day}.jsonl')
    return {
        str(row.get('parity_key')): row
        for row in _jsonl_rows(path)
        if row.get('parity_key')
    }


def _live_by_key(day: str) -> dict[str, dict[str, Any]]:
    live_signal_path = output_path('postmortem', 'live_signal_parity', f'live_signal_parity_{day}.jsonl')
    rows = [normalize_live_decision(row) for row in _jsonl_rows(live_signal_path)]
    shadow_rows = _jsonl_rows(_live_path(day))
    merged: dict[str, dict[str, Any]] = {}
    for row in rows + shadow_rows:
        key = str(row.get('opportunity_key') or row.get('parity_key') or '')
        if key:
            merged[key] = row
    return merged


def build_day(day: str) -> dict[str, Any]:
    live = _live_by_key(day)
    step2 = _step2_by_key(day)
    trades = _trade_by_id(day)
    keys = sorted(set(live) | set(step2))
    rows: list[dict[str, Any]] = []
    for key in keys:
        live_row = dict(live.get(key) or {})
        step2_row = step2.get(key) or {}
        row = live_row or {
            'schema_version': SCHEMA_VERSION,
            'source': 'step2_only',
            'opportunity_key': key,
            'parity_key': key,
            'ticker': step2_row.get('ticker'),
            'side': step2_row.get('side'),
            'signal_ts': step2_row.get('created_at'),
            'signal_ct': step2_row.get('created_at_ct'),
            'setup_type': step2_row.get('setup_type'),
            'score': step2_row.get('score'),
            'conviction': step2_row.get('conviction'),
            'entry_price': _num(step2_row.get('price')),
            'live_decision': None,
            'live_reason': None,
            'live_shadow_mode': False,
        }
        outcome = step2_row.get('outcome_summary') or {}
        row.update({
            'step2_decision': 'accepted' if step2_row.get('decision') == 'entered' else (
                'rejected' if step2_row else None
            ),
            'step2_reason': step2_row.get('reason'),
            'step2_expected_pnl': _num(outcome.get('pnl')),
            'step2_exit_reason': outcome.get('reason'),
            'step2_exit_price': _num(outcome.get('exit')),
            'step2_exit_ct': outcome.get('exit_ct'),
            'step2_source': step2_row.get('source'),
            'step2_run_context': step2_row.get('run_context') or {},
        })
        trade = trades.get(str(row.get('live_trade_id') or row.get('trade_id') or ''))
        if trade:
            row.update({
                'live_outcome_pnl': _num(trade.get('pnl')),
                'live_exit_reason': trade.get('reason'),
                'live_exit_price': _num(trade.get('exit')),
                'live_exit_ts': _int(trade.get('closed_at')),
                'live_exit_ct': (
                    datetime.fromtimestamp(int(trade.get('closed_at')), CT).isoformat(timespec='seconds')
                    if trade.get('closed_at') else None
                ),
            })
        rows.append(row)
    path = _write_jsonl(ledger_path(day), rows)
    counts = Counter(
        f"{row.get('live_decision') or 'missing_live'}:{row.get('step2_decision') or 'missing_step2'}"
        for row in rows
    )
    summary = {
        'schema_version': SCHEMA_VERSION,
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'path': path,
        'rows': len(rows),
        'live_rows': len(live),
        'step2_rows': len(step2),
        'counts': dict(sorted(counts.items())),
        'pnl': {
            'step2_expected': round(sum(_num(row.get('step2_expected_pnl')) or 0.0 for row in rows), 4),
            'live_actual': round(sum(_num(row.get('live_outcome_pnl')) or 0.0 for row in rows), 4),
        },
    }
    summary['summary_path'] = _write_json(summary_path(day), summary)
    return summary


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser(description='Build canonical opportunity ledger for a day.')
    ap.add_argument('day')
    ap.add_argument('--json', action='store_true')
    args = ap.parse_args()
    payload = build_day(args.day)
    print(json.dumps(payload if args.json else {'path': payload['path'], 'summary_path': payload['summary_path']},
                     indent=2, sort_keys=True, default=str))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
