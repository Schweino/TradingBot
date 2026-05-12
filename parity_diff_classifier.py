"""Classify Live-vs-Step2 parity gaps into actionable buckets."""
from __future__ import annotations

from output_paths import output_path

import json
import os
from collections import Counter
from datetime import datetime
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore


HERE = os.path.dirname(os.path.abspath(__file__))
CT = ZoneInfo('America/Chicago')
OUT_DIR = output_path('postmortem', 'parity_diff_classifier')
SCHEMA_VERSION = 1


def _num(value: Any) -> float | None:
    try:
        if value in (None, ''):
            return None
        return float(value)
    except Exception:
        return None


def _classify_row(row: dict[str, Any]) -> str:
    live_decision = row.get('live_decision')
    step2_decision = row.get('step2_decision')
    live_pnl = _num(row.get('live_outcome_pnl'))
    step2_pnl = _num(row.get('step2_expected_pnl'))
    if live_decision is None and step2_decision is not None:
        return 'missing_live_signal'
    if step2_decision is None and live_decision is not None:
        return 'missing_step2_signal'
    if live_decision == 'rejected' and step2_decision == 'accepted':
        reason = str(row.get('live_reason') or '').lower()
        if any(token in reason for token in ('buying_power', 'alpaca', 'shortable', 'broker')):
            return 'broker_or_operational_block'
        if any(token in reason for token in ('open_position', 'cooldown', 'pending', 'entry_block')):
            return 'state_machine_block'
        return 'live_rejected_step2_trade'
    if live_decision == 'accepted' and step2_decision == 'rejected':
        return 'step2_rejected_live_trade'
    if live_decision == 'accepted' and step2_decision == 'accepted':
        if live_pnl is None:
            return 'live_trade_missing_outcome'
        if step2_pnl is None:
            return 'step2_trade_missing_outcome'
        if row.get('live_exit_reason') != row.get('step2_exit_reason'):
            return 'same_entry_different_exit_reason'
        if abs(live_pnl - step2_pnl) > 1.0:
            return 'same_entry_different_fill_or_latency'
        return 'matched_trade'
    if live_decision == 'rejected' and step2_decision == 'rejected':
        if row.get('live_reason') != row.get('step2_reason'):
            return 'same_reject_different_reason'
        return 'matched_reject'
    return 'unclassified'


def classify_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    classified = []
    counts = Counter()
    pnl_gap_by_class: Counter[str] = Counter()
    for row in rows:
        cls = _classify_row(row)
        live_pnl = _num(row.get('live_outcome_pnl')) or 0.0
        step2_pnl = _num(row.get('step2_expected_pnl')) or 0.0
        gap = round(live_pnl - step2_pnl, 4)
        out = {
            'class': cls,
            'opportunity_key': row.get('opportunity_key') or row.get('parity_key'),
            'ticker': row.get('ticker'),
            'side': row.get('side'),
            'signal_ct': row.get('signal_ct'),
            'live_decision': row.get('live_decision'),
            'step2_decision': row.get('step2_decision'),
            'live_reason': row.get('live_reason'),
            'step2_reason': row.get('step2_reason'),
            'live_exit_reason': row.get('live_exit_reason'),
            'step2_exit_reason': row.get('step2_exit_reason'),
            'live_pnl': live_pnl,
            'step2_pnl': step2_pnl,
            'pnl_gap_live_minus_step2': gap,
        }
        classified.append(out)
        counts[cls] += 1
        pnl_gap_by_class[cls] += gap
    return {
        'schema_version': SCHEMA_VERSION,
        'rows': len(rows),
        'counts': dict(sorted(counts.items(), key=lambda item: (-item[1], item[0]))),
        'pnl_gap_by_class': {k: round(v, 4) for k, v in sorted(pnl_gap_by_class.items())},
        'examples': classified[:100],
    }


def write_day(day: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    payload = classify_rows(rows)
    payload['day'] = day
    payload['created_at_ct'] = datetime.now(CT).isoformat(timespec='seconds')
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, f'parity_diff_classifier_{day}.json')
    tmp = f'{path}.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
    os.replace(tmp, path)
    payload['path'] = os.path.abspath(path)
    return payload
