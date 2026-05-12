"""Explain where the active Step 2 profile leaves oracle-side P/L on the tape."""
from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Any

import numpy as np

import active_engine_baseline
import decision_tape_compiled
import scoring_variant_lab as lab
import scoring_variant_lab_fast as fast
import step2_parity_contract


HERE = Path(__file__).resolve().parent
DEFAULT_OUT = HERE / 'postmortem' / 'backtests' / 'step2_oracle_gap_report.json'
FEATURE_INDEX = {name: idx for idx, name in enumerate(fast.FEATURE_NAMES)}


def _load_trading_config() -> dict:
    path = HERE / 'trading_config.json'
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return {}


def _sim_config(args: argparse.Namespace) -> dict:
    sim = step2_parity_contract.sim_config(_load_trading_config())
    sim['max_trades_per_day'] = int(getattr(args, 'max_trades_per_day', 0) or 0)
    sim['max_trades_per_ticker_day'] = int(getattr(args, 'max_trades_per_ticker_day', 0) or 0)
    return sim


def _reverse_map(mapping: dict) -> dict[int, str]:
    return {int(value): str(key) for key, value in (mapping or {}).items()}


def _feature(compiled: dict, name: str) -> np.ndarray:
    idx = FEATURE_INDEX.get(name)
    if idx is None:
        return np.zeros(int(compiled.get('rows') or 0), dtype=np.float64)
    return np.asarray(compiled['features'])[:, idx]


def _session_labels(compiled: dict) -> np.ndarray:
    open_phase = _feature(compiled, 'open_phase')
    midday_phase = _feature(compiled, 'midday_phase')
    labels = np.full(open_phase.shape[0], 'late', dtype=object)
    labels[midday_phase > 0.0] = 'midday'
    labels[open_phase > 0.0] = 'open'
    return labels


def _side_labels(sides: np.ndarray) -> np.ndarray:
    labels = np.full(sides.shape[0], 'SKIP', dtype=object)
    labels[sides == decision_tape_compiled.SIDE_LONG] = 'LONG'
    labels[sides == decision_tape_compiled.SIDE_SHORT] = 'SHORT'
    return labels


def _score(compiled: dict, variant: lab.Variant, sides: np.ndarray | None,
           args: argparse.Namespace) -> dict:
    if sides is None:
        rows = decision_tape_compiled.simulate_variants(
            compiled,
            [variant],
            float(args.start_balance),
            gate=None,
            sim_config=_sim_config(args),
        )
    else:
        rows = decision_tape_compiled.simulate_side_matrix(
            compiled,
            [variant],
            sides.reshape(1, -1).astype(np.int8),
            float(args.start_balance),
            gate=None,
            sim_config=_sim_config(args),
        )
    if not rows:
        raise RuntimeError('compiled Step 2 simulation unavailable')
    return rows[0]


def _bucket_summary(keys: list[str], gap_pct: np.ndarray, wrong_side: np.ndarray,
                    oracle_positive: np.ndarray, active_losing: np.ndarray) -> list[dict]:
    buckets: dict[str, dict[str, Any]] = defaultdict(lambda: {
        'rows': 0,
        'wrong_side_rows': 0,
        'oracle_positive_rows': 0,
        'active_losing_rows': 0,
        'gap_pct_sum': 0.0,
    })
    for idx, key in enumerate(keys):
        bucket = buckets[str(key)]
        bucket['rows'] += 1
        bucket['wrong_side_rows'] += int(bool(wrong_side[idx]))
        bucket['oracle_positive_rows'] += int(bool(oracle_positive[idx]))
        bucket['active_losing_rows'] += int(bool(active_losing[idx]))
        bucket['gap_pct_sum'] += float(max(gap_pct[idx], 0.0))
    rows = []
    for key, bucket in buckets.items():
        count = int(bucket['rows'])
        rows.append({
            'bucket': key,
            'rows': count,
            'wrong_side_rows': int(bucket['wrong_side_rows']),
            'oracle_positive_rows': int(bucket['oracle_positive_rows']),
            'active_losing_rows': int(bucket['active_losing_rows']),
            'avg_gap_pct': round(float(bucket['gap_pct_sum']) / count, 6) if count else 0.0,
            'gap_pct_sum': round(float(bucket['gap_pct_sum']), 6),
        })
    rows.sort(key=lambda row: (row['gap_pct_sum'], row['wrong_side_rows']), reverse=True)
    return rows


def build_report(compiled: dict, args: argparse.Namespace) -> dict:
    active = active_engine_baseline.reference_variant()
    active_sides = decision_tape_compiled.side_matrix(compiled, [active])[0]
    long_pct = np.asarray(compiled['long_pnl_pct'], dtype=np.float64)
    short_pct = np.asarray(compiled['short_pnl_pct'], dtype=np.float64)
    oracle_sides = np.where(long_pct >= short_pct, decision_tape_compiled.SIDE_LONG, decision_tape_compiled.SIDE_SHORT)
    active_pct = np.where(active_sides == decision_tape_compiled.SIDE_LONG, long_pct, short_pct)
    oracle_pct = np.where(oracle_sides == decision_tape_compiled.SIDE_LONG, long_pct, short_pct)
    gap_pct = oracle_pct - active_pct
    wrong_side = active_sides != oracle_sides
    both_negative = np.maximum(long_pct, short_pct) <= 0.0
    oracle_positive = oracle_pct > 0.0
    active_losing = active_pct < 0.0

    active_row = _score(compiled, active, None, args)
    oracle_variant = lab.Variant('oracle_best_side_per_opportunity', {}, 0.0)
    oracle_row = _score(compiled, oracle_variant, oracle_sides, args)
    skip_oracle_sides = np.where(both_negative, decision_tape_compiled.SIDE_SKIP, oracle_sides)
    skip_oracle_variant = lab.Variant('oracle_best_side_or_skip_per_opportunity', {}, 0.0)
    skip_oracle_row = _score(compiled, skip_oracle_variant, skip_oracle_sides, args)

    ticker_names = _reverse_map(compiled.get('ticker_map') or {})
    setup_names = _reverse_map(compiled.get('setup_map') or {})
    ticker_labels = [ticker_names.get(int(code), str(int(code))) for code in np.asarray(compiled['ticker_code'])]
    setup_labels = [setup_names.get(int(code), str(int(code))) for code in np.asarray(compiled['setup_code'])]
    session_labels = list(_session_labels(compiled))
    active_side_labels = list(_side_labels(active_sides))
    oracle_side_labels = list(_side_labels(oracle_sides))

    classes = {
        'wrong_side': _class_count(wrong_side, gap_pct),
        'both_sides_negative_should_skip': _class_count(both_negative, gap_pct),
        'active_losing_oracle_positive': _class_count(active_losing & oracle_positive, gap_pct),
        'open_brs_gap': _class_count(
            (_feature(compiled, 'setup_btc_relative_strength') > 0.0)
            & (_feature(compiled, 'open_phase') > 0.0)
            & (gap_pct > 0.0),
            gap_pct,
        ),
        'exit_path_sensitive': _class_count(
            np.abs(np.asarray(compiled['long_held']) - np.asarray(compiled['short_held'])) >= 30,
            gap_pct,
        ),
    }

    top_rows = []
    order = np.argsort(gap_pct)[::-1][:int(args.top_rows)]
    for raw_idx in order:
        idx = int(raw_idx)
        if gap_pct[idx] <= 0.0:
            continue
        top_rows.append({
            'row_index': idx,
            'ts': int(np.asarray(compiled['ts'])[idx]),
            'ticker': ticker_labels[idx],
            'setup': setup_labels[idx],
            'session_phase': session_labels[idx],
            'active_side': active_side_labels[idx],
            'oracle_side': oracle_side_labels[idx],
            'active_pnl_pct': round(float(active_pct[idx]), 6),
            'oracle_pnl_pct': round(float(oracle_pct[idx]), 6),
            'gap_pct': round(float(gap_pct[idx]), 6),
            'both_sides_negative': bool(both_negative[idx]),
        })

    by_ticker = _bucket_summary(ticker_labels, gap_pct, wrong_side, oracle_positive, active_losing)[:50]
    by_setup = _bucket_summary(setup_labels, gap_pct, wrong_side, oracle_positive, active_losing)[:50]
    by_session = _bucket_summary(session_labels, gap_pct, wrong_side, oracle_positive, active_losing)[:10]
    by_route_seed = _bucket_summary(
        [f'{ticker}|{setup}|{phase}' for ticker, setup, phase in zip(ticker_labels, setup_labels, session_labels)],
        gap_pct,
        wrong_side,
        oracle_positive,
        active_losing,
    )[:100]

    active_full = active_row.get('decision_full') or {}
    oracle_full = oracle_row.get('decision_full') or {}
    skip_oracle_full = skip_oracle_row.get('decision_full') or {}
    active_pnl = float(active_full.get('pnl') or 0.0)
    oracle_pnl = float(oracle_full.get('pnl') or 0.0)
    skip_oracle_pnl = float(skip_oracle_full.get('pnl') or 0.0)
    return {
        'schema_version': 1,
        'script': 'step2_oracle_gap_report.py',
        'compiled_decision_tape': str(args.compiled_decision_tape),
        'start_balance': float(args.start_balance),
        'rows': int(compiled.get('rows') or len(active_sides)),
        'active': active_row,
        'oracle': oracle_row,
        'skip_oracle': skip_oracle_row,
        'summary': {
            'active_pnl': round(active_pnl, 2),
            'oracle_pnl': round(oracle_pnl, 2),
            'skip_oracle_pnl': round(skip_oracle_pnl, 2),
            'oracle_gap_pnl': round(oracle_pnl - active_pnl, 2),
            'skip_oracle_gap_pnl': round(skip_oracle_pnl - active_pnl, 2),
            'oracle_gap_pct_vs_active': round((oracle_pnl / active_pnl - 1.0) * 100.0, 4) if active_pnl else None,
            'skip_oracle_gap_pct_vs_active': round((skip_oracle_pnl / active_pnl - 1.0) * 100.0, 4) if active_pnl else None,
            'wrong_side_rows': int(np.sum(wrong_side)),
            'both_sides_negative_rows': int(np.sum(both_negative)),
            'active_losing_oracle_positive_rows': int(np.sum(active_losing & oracle_positive)),
            'positive_gap_rows': int(np.sum(gap_pct > 0.0)),
            'row_gap_pct_sum': round(float(np.sum(np.maximum(gap_pct, 0.0))), 6),
        },
        'classes': classes,
        'by_ticker': by_ticker,
        'by_setup': by_setup,
        'by_session': by_session,
        'route_seed_buckets': by_route_seed,
        'top_gap_rows': top_rows,
        'notes': [
            'Row gap pct is opportunity-level, while active/oracle P/L uses the compiled simulator with compounding and gates.',
            'route_seed_buckets are meant as candidates for routed overrides, not proof that a route will survive cooldown interactions.',
        ],
    }


def _class_count(mask: np.ndarray, gap_pct: np.ndarray) -> dict:
    count = int(np.sum(mask))
    return {
        'rows': count,
        'positive_gap_rows': int(np.sum(mask & (gap_pct > 0.0))),
        'gap_pct_sum': round(float(np.sum(np.maximum(gap_pct[mask], 0.0))), 6) if count else 0.0,
    }


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding='utf-8')
    os.replace(tmp, path)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Build an oracle-gap report for the active Step 2 scoring profile.')
    ap.add_argument('--compiled-decision-tape', required=True)
    ap.add_argument('--out', default=str(DEFAULT_OUT))
    ap.add_argument('--start-balance', type=float, default=100000.0)
    ap.add_argument('--top-rows', type=int, default=100)
    ap.add_argument('--max-trades-per-day', type=int, default=0)
    ap.add_argument('--max-trades-per-ticker-day', type=int, default=0)
    ap.add_argument('--allow-uncertified-cache', action='store_true',
                    help='Diagnostics only: skip compiled lineage validation.')
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    compiled = decision_tape_compiled.load_compiled(
        args.compiled_decision_tape,
        mmap=True,
        validate_sources=not args.allow_uncertified_cache,
    )
    payload = build_report(compiled, args)
    out = Path(args.out)
    _write_json(out, payload)
    print(json.dumps({
        'out': str(out),
        'active_pnl': payload['summary']['active_pnl'],
        'oracle_pnl': payload['summary']['oracle_pnl'],
        'skip_oracle_pnl': payload['summary']['skip_oracle_pnl'],
        'oracle_gap_pnl': payload['summary']['oracle_gap_pnl'],
        'skip_oracle_gap_pnl': payload['summary']['skip_oracle_gap_pnl'],
        'wrong_side_rows': payload['summary']['wrong_side_rows'],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
