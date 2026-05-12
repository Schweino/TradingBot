"""Immediate shadow Step 2 comparison for live signal decisions."""
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

import realtime_parity_alerts


HERE = os.path.dirname(os.path.abspath(__file__))
POSTMORTEM_DIR = output_path('postmortem')
LIVE_SIGNAL_DIR = os.path.join(POSTMORTEM_DIR, 'live_signal_parity')
OUT_DIR = os.path.join(POSTMORTEM_DIR, 'intraday_shadow_step2')
CT = ZoneInfo('America/Chicago')
SCHEMA_VERSION = 1

OPERATIONAL_SKIP_REASONS = {
    'not_running',
    'kill_switch',
    'broker_api_degraded',
    'outside_entry_window',
    'invalid_signal',
    'open_position',
    'pending_entry',
    'same_ticker_reentry_cooldown',
    'daily_trade_cap_reached',
    'ticker_daily_trade_cap_reached',
    'insufficient_per_ticker_budget',
    'insufficient_buying_power',
    'alloc_below_one_share',
    'non_positive_qty',
}


def _day(ts: int | float | None = None) -> str:
    return datetime.fromtimestamp(float(ts or time.time()), CT).date().isoformat()


def _path(day: str) -> str:
    return os.path.join(OUT_DIR, day, f'shadow_step2_{day}.jsonl')


def _summary_path(day: str) -> str:
    return os.path.join(OUT_DIR, day, f'shadow_step2_{day}.summary.json')


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


def _entry_decision_from_row(row: dict[str, Any]) -> dict[str, Any] | None:
    feature = row.get('feature_snapshot') if isinstance(row.get('feature_snapshot'), dict) else {}
    forensics = feature.get('forensics') if isinstance(feature.get('forensics'), dict) else {}
    decision = forensics.get('step2_entry_decision')
    return decision if isinstance(decision, dict) else None


def compare_row(row: dict[str, Any]) -> dict[str, Any]:
    ts = int(row.get('created_at') or time.time())
    live_decision = str(row.get('decision') or 'unknown')
    live_reason = str(row.get('reason') or '')
    entry_decision = _entry_decision_from_row(row)
    evaluable = bool(entry_decision)
    evidence = 'step2_entry_decision'
    if entry_decision:
        expected_decision = 'entered' if entry_decision.get('decision') == 'accepted' else 'skipped'
        expected_reason = 'entered' if expected_decision == 'entered' else str(entry_decision.get('reason') or 'skipped')
    elif live_decision == 'skipped' and live_reason in OPERATIONAL_SKIP_REASONS:
        evaluable = True
        evidence = 'known_operational_skip'
        expected_decision = 'skipped'
        expected_reason = live_reason
    else:
        evidence = 'not_enough_pre_decision_state'
        expected_decision = live_decision
        expected_reason = live_reason
    decision_match = live_decision == expected_decision
    reason_match = (
        not evaluable
        or expected_decision != 'skipped'
        or not expected_reason
        or live_reason == expected_reason
    )
    severity = 'ok'
    if evaluable and not decision_match:
        severity = 'critical'
    elif evaluable and not reason_match:
        severity = 'warning'
    elif not evaluable:
        severity = 'info'
    out = {
        'schema_version': SCHEMA_VERSION,
        'source': 'intraday_shadow_step2',
        'created_at': ts,
        'created_at_ct': datetime.fromtimestamp(ts, CT).isoformat(timespec='seconds'),
        'day': _day(ts),
        'parity_key': row.get('parity_key'),
        'trade_id': row.get('trade_id'),
        'ticker': row.get('ticker'),
        'side': row.get('side'),
        'setup_type': row.get('setup_type'),
        'live_decision': live_decision,
        'live_reason': live_reason,
        'expected_decision': expected_decision,
        'expected_reason': expected_reason,
        'decision_match': decision_match,
        'reason_match': reason_match,
        'evaluable': evaluable,
        'evidence': evidence,
        'severity': severity,
        'active_profile_hash': row.get('active_profile_hash'),
        'step2_parity_contract_hash': row.get('step2_parity_contract_hash'),
        'step2_execution_contract_hash': row.get('step2_execution_contract_hash'),
    }
    return out


def append_live_row(row: dict[str, Any]) -> dict[str, Any]:
    shadow = compare_row(row)
    path = _path(shadow['day'])
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'a', encoding='utf-8') as f:
        f.write(json.dumps(shadow, sort_keys=True, separators=(',', ':'), default=str) + '\n')
    if shadow['severity'] in ('warning', 'critical'):
        realtime_parity_alerts.emit(
            shadow['severity'],
            'intraday_shadow_step2_mismatch',
            'Live signal decision diverged from the immediate Step 2 shadow decision.',
            details=shadow,
            ts=shadow['created_at'],
            dedupe_key=str(shadow.get('parity_key') or shadow.get('trade_id') or ''),
        )
    return shadow


def build_day(day: str) -> dict[str, Any]:
    live_path = os.path.join(LIVE_SIGNAL_DIR, f'live_signal_parity_{day}.jsonl')
    rows = [compare_row(row) for row in _read_jsonl(live_path)]
    path = _path(day)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f'{path}.{os.getpid()}.{int(time.time() * 1000)}.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        for row in rows:
            f.write(json.dumps(row, sort_keys=True, separators=(',', ':'), default=str) + '\n')
    os.replace(tmp, path)
    counts = Counter(str(row.get('severity') or 'unknown') for row in rows)
    mismatches = [row for row in rows if row.get('severity') in ('warning', 'critical')]
    summary = {
        'schema_version': SCHEMA_VERSION,
        'source': 'intraday_shadow_step2',
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'path': os.path.abspath(path),
        'rows': len(rows),
        'evaluable_rows': sum(1 for row in rows if row.get('evaluable')),
        'counts': dict(sorted(counts.items())),
        'mismatch_count': len(mismatches),
        'critical_count': sum(1 for row in mismatches if row.get('severity') == 'critical'),
        'warning_count': sum(1 for row in mismatches if row.get('severity') == 'warning'),
        'sample_mismatches': mismatches[:10],
    }
    summary_path = _summary_path(day)
    tmp_summary = f'{summary_path}.{os.getpid()}.{int(time.time() * 1000)}.tmp'
    with open(tmp_summary, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, sort_keys=True, default=str)
    os.replace(tmp_summary, summary_path)
    summary['summary_path'] = os.path.abspath(summary_path)
    return summary


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser(description='Build immediate intraday shadow Step 2 comparison for live signals.')
    ap.add_argument('day', nargs='?', default=_day())
    ap.add_argument('--json', action='store_true')
    args = ap.parse_args()
    payload = build_day(args.day)
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0 if int(payload.get('critical_count') or 0) == 0 else 2


if __name__ == '__main__':
    raise SystemExit(main())
