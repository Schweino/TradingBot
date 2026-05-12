from __future__ import annotations

from output_paths import output_path

import argparse
import json
import os
from datetime import datetime

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from promotion_queue import update_queue


HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = output_path('postmortem')
CT = ZoneInfo('America/Chicago')


def _today() -> str:
    return datetime.now(CT).date().isoformat()


def build_promotion_review(day: str) -> dict:
    queue = update_queue(day)
    rows = []
    for item in (queue.get('items') or {}).values():
        ev = item.get('evidence') or {}
        rows.append({
            'key': item.get('key'),
            'kind': item.get('kind'),
            'name': item.get('name'),
            'status': item.get('status'),
            'lifecycle': item.get('lifecycle'),
            'seen_days': item.get('seen_days') or [],
            'affected_trades': ev.get('affected_trades'),
            'estimated_delta_vs_actual': ev.get('estimated_delta_vs_actual') or ev.get('pnl'),
            'winner_damage': ev.get('winner_damage') or ev.get('hurt_winner_delta'),
            'ops_contaminated_day': ev.get('ops_contaminated_day'),
            'why_not_eligible': _why_not_eligible(item),
            'evidence_files': [
                os.path.join(OUT_DIR, 'promotion_queue.json'),
                os.path.join(OUT_DIR, f'engine_candidate_config_{day}.json'),
                os.path.join(OUT_DIR, f'exit_policy_candidate_config_{day}.json'),
                os.path.join(OUT_DIR, f'strategy_operations_split_{day}.json'),
            ],
        })
    rows.sort(key=lambda r: (r.get('status') != 'eligible_for_human_review', r.get('status') or '', r.get('key') or ''))
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'eligible': [r for r in rows if r.get('status') == 'eligible_for_human_review'],
        'lifecycle_counts': {
            key: sum(1 for r in rows if r.get('lifecycle') == key)
            for key in ('proven', 'promising', 'watch_only', 'rejected', 'insufficient_data')
        },
        'watch': [r for r in rows if r.get('status') != 'eligible_for_human_review'],
        'rules': [
            'lifecycle states: proven, promising, watch_only, rejected, insufficient_data',
            '2+ evidence days',
            '8+ affected trades',
            'positive net delta',
            'winner damage no worse than threshold',
            'no operational contamination',
            'manual approval required',
        ],
    }


def _why_not_eligible(item: dict) -> list[str]:
    if item.get('status') == 'eligible_for_human_review':
        return []
    ev = item.get('evidence') or {}
    reasons = []
    if len(item.get('seen_days') or []) < 2:
        reasons.append('needs_2_seen_days')
    if int(ev.get('affected_trades') or 0) < 8:
        reasons.append('needs_8_affected_trades')
    if float(ev.get('estimated_delta_vs_actual') or ev.get('pnl') or 0) <= 0:
        reasons.append('needs_positive_delta')
    if float(ev.get('winner_damage') or ev.get('hurt_winner_delta') or 0) < -100:
        reasons.append('winner_damage_too_high')
    if ev.get('ops_contaminated_day'):
        reasons.append('ops_contaminated_day')
    return reasons


def write_promotion_review(day: str) -> tuple[str, dict]:
    payload = build_promotion_review(day)
    path = os.path.join(OUT_DIR, f'promotion_review_{day}.json')
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, default=str)
    return path, payload


def main() -> int:
    ap = argparse.ArgumentParser(description='Print promotion queue evidence and eligibility.')
    ap.add_argument('day', nargs='?', default=_today())
    ap.add_argument('--json', action='store_true')
    args = ap.parse_args()
    path, payload = write_promotion_review(args.day)
    if args.json:
        print(json.dumps(payload, indent=2, default=str))
    else:
        print(path)
        print(f"eligible={len(payload.get('eligible') or [])} watch={len(payload.get('watch') or [])}")
        for row in (payload.get('eligible') or [])[:10]:
            print(f"ELIGIBLE {row['key']} delta={row['estimated_delta_vs_actual']} affected={row['affected_trades']}")
        for row in (payload.get('watch') or [])[:12]:
            why = ','.join(row.get('why_not_eligible') or [])
            print(f"WATCH {row['key']} status={row['status']} why={why}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
