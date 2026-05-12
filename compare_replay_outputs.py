"""Compare two replay trade/summary outputs exactly.

This is a small audit helper for Step 3/4 parity checks. It does not run any
replay logic; it reports whether two existing artifacts match and where the
first trade-row mismatch occurs.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os


TRADE_KEYS = (
    'opportunity_id',
    'entry_ct',
    'exit_ct',
    'ticker',
    'side',
    'entry',
    'exit',
    'qty',
    'alloc',
    'pnl',
    'reason',
)


def _load_json(path: str) -> dict:
    if not path or not os.path.exists(path):
        return {}
    with open(path, 'r', encoding='utf-8') as f:
        payload = json.load(f)
    return payload if isinstance(payload, dict) else {}


def _load_csv(path: str) -> list[dict]:
    if not path or not os.path.exists(path):
        return []
    with open(path, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def _signature(rows: list[dict]) -> list[tuple]:
    return [tuple(row.get(key) for key in TRADE_KEYS) for row in rows]


def _hash(rows: list[dict]) -> str:
    return hashlib.sha256(
        json.dumps(_signature(rows), sort_keys=True, separators=(',', ':')).encode('utf-8')
    ).hexdigest()


def _first_mismatch(left_rows: list[dict], right_rows: list[dict]) -> dict | None:
    left_sig = _signature(left_rows)
    right_sig = _signature(right_rows)
    for idx, (left, right) in enumerate(zip(left_sig, right_sig)):
        if left != right:
            return {'index': idx, 'left': left, 'right': right}
    if len(left_sig) != len(right_sig):
        return {'index': min(len(left_sig), len(right_sig)), 'left_len': len(left_sig), 'right_len': len(right_sig)}
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description='Compare two replay output artifacts exactly.')
    ap.add_argument('--left-summary', required=True)
    ap.add_argument('--left-csv', required=True)
    ap.add_argument('--right-summary', required=True)
    ap.add_argument('--right-csv', required=True)
    args = ap.parse_args()

    left_summary = _load_json(args.left_summary)
    right_summary = _load_json(args.right_summary)
    left_rows = _load_csv(args.left_csv)
    right_rows = _load_csv(args.right_csv)
    summary_match = {
        'trades': left_summary.get('trades') == right_summary.get('trades'),
        'wins': left_summary.get('wins') == right_summary.get('wins'),
        'losses': left_summary.get('losses') == right_summary.get('losses'),
        'pnl': left_summary.get('pnl') == right_summary.get('pnl'),
        'ending_balance': left_summary.get('ending_balance') == right_summary.get('ending_balance'),
    }
    rows_match = _signature(left_rows) == _signature(right_rows)
    payload = {
        'summary_match': summary_match,
        'trade_rows_match': rows_match,
        'left_trade_rows_sha256': _hash(left_rows),
        'right_trade_rows_sha256': _hash(right_rows),
        'delta': {
            'pnl': round(float(right_summary.get('pnl') or 0.0) - float(left_summary.get('pnl') or 0.0), 8),
            'ending_balance': round(float(right_summary.get('ending_balance') or 0.0) - float(left_summary.get('ending_balance') or 0.0), 8),
            'trades': int(right_summary.get('trades') or 0) - int(left_summary.get('trades') or 0),
        },
        'first_trade_mismatch': None if rows_match else _first_mismatch(left_rows, right_rows),
    }
    payload['passed'] = all(summary_match.values()) and rows_match
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload['passed'] else 2


if __name__ == '__main__':
    raise SystemExit(main())
