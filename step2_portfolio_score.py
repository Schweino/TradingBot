"""Temporary Step 2 portfolio-promotion scorer.

This does not change decision-tape P/L. It adds a separate promotion score for
building a Step 3 slate, so Step 2 can prefer robust coverage instead of only
raw decision P/L.
"""
from __future__ import annotations

import argparse
import json
import math
import os


def _num(value, default: float = 0.0) -> float:
    try:
        if value in (None, ''):
            return default
        return float(value)
    except Exception:
        return default


def _max_share(summary: dict, key: str) -> float:
    total = abs(_num(summary.get('pnl')))
    rows = summary.get(key) or {}
    if total <= 0 or not rows:
        return 0.0
    return max(abs(_num(row.get('pnl'))) for row in rows.values()) / total


def _portfolio_score(row: dict, active: dict, active_lab_pnl: float) -> dict:
    summary = row.get('decision_full') or {}
    pnl = _num(summary.get('pnl'))
    lab_pnl = _num(row.get('lab_pnl'), active_lab_pnl if row.get('is_active_control') else 0.0)
    worst_day = summary.get('worst_day') or {}
    worst_day_penalty = abs(min(0.0, _num(worst_day.get('pnl'))))
    max_day_share = _max_share(summary, 'by_day')
    max_ticker_share = _max_share(summary, 'by_ticker')
    concentration = max(max_day_share, max_ticker_share)
    active_trades = max(1.0, _num((active.get('decision_full') or {}).get('trades'), 1.0))
    trade_ratio = _num(summary.get('trades')) / active_trades
    trade_similarity = max(0.0, 1.0 - abs(math.log(max(trade_ratio, 1e-9))))
    win_rate = _num(summary.get('win_rate_pct'))

    # Keep units P/L-like: raw decision P/L stays dominant, while lab support
    # and robustness can rescue plausible Step 3 candidates without pretending
    # the score is actual money.
    score = (
        pnl
        + 0.18 * max(0.0, lab_pnl)
        + 400.0 * trade_similarity
        + 25.0 * win_rate
        - 0.45 * worst_day_penalty
        - 900.0 * concentration
    )
    return {
        'step2_raw_pnl': round(pnl, 4),
        'step2_portfolio_score': round(score, 4),
        'lab_support_pnl': round(lab_pnl, 4),
        'trade_similarity': round(trade_similarity, 6),
        'worst_day_penalty': round(worst_day_penalty, 4),
        'concentration': round(concentration, 6),
        'win_rate_pct': round(win_rate, 4),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description='Temporary Step 2 portfolio scorer.')
    ap.add_argument('--input', required=True)
    ap.add_argument('--out', default=None)
    ap.add_argument('--include-active-control', action='store_true')
    ap.add_argument('--top', type=int, default=20)
    args = ap.parse_args()

    with open(args.input, 'r', encoding='utf-8') as f:
        payload = json.load(f)
    active = payload.get('active_engine_baseline') or {}
    active_profile = ((active.get('profile') or {}).get('profile') or {})
    active_lab_pnl = _num((((active.get('profile') or {}).get('lab') or {}).get('pnl')), 10426.4)

    rows = list(payload.get('results') or [])
    if args.include_active_control and active.get('decision_full') and active_profile:
        rows.append({
            'variant': active_profile.get('name') or 'active_engine',
            'weights': active_profile.get('weights') or {},
            'bias': active_profile.get('bias') or 0.0,
            'decision_full': active.get('decision_full'),
            'lab_pnl': active_lab_pnl,
            'is_active_control': True,
        })

    scored = []
    for row in rows:
        out_row = dict(row)
        out_row['portfolio_score_detail'] = _portfolio_score(row, active, active_lab_pnl)
        out_row['step2_portfolio_score'] = out_row['portfolio_score_detail']['step2_portfolio_score']
        scored.append(out_row)
    scored.sort(key=lambda r: r.get('step2_portfolio_score', -10**12), reverse=True)
    for idx, row in enumerate(scored, 1):
        row['step2_portfolio_rank'] = idx

    out = {
        'schema_version': 1,
        'source': os.path.abspath(args.input),
        'method': 'temporary_portfolio_score_v1',
        'active_engine_baseline': active,
        'results': scored,
        'top': scored[:max(0, int(args.top or 0))],
    }
    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
        with open(args.out, 'w', encoding='utf-8') as f:
            json.dump(out, f, indent=2, sort_keys=True)
    print(json.dumps({
        'out': args.out,
        'rows': len(scored),
        'top': [
            {
                'rank': row.get('step2_portfolio_rank'),
                'variant': row.get('variant'),
                'is_active_control': bool(row.get('is_active_control')),
                **row.get('portfolio_score_detail', {}),
            }
            for row in scored[:max(0, int(args.top or 0))]
        ],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
