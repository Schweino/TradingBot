"""Canonical feature/outcome store for scoring-model research.

This module builds immutable, fingerprinted NumPy artifacts from either replay
trade CSVs or decision-tape rows. It is deliberately replay-only: it does not
modify live trading logic or reinterpret fills. The goal is to parse expensive
JSON/CSV inputs once, then let scoring screens operate on the same verified
arrays every time.
"""
from __future__ import annotations

from output_paths import output_path

import argparse
import csv
import gzip
import hashlib
import json
import os
import time
from typing import Iterable

import numpy as np

import backtest_30d_engine as replay
import scoring_profiles
import scoring_variant_lab as slow
import scoring_variant_lab_fast as fast
import tournament_safety


HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_STORE_DIR = output_path('postmortem', 'backtests', 'scoring_feature_store')
SCHEMA_VERSION = 1


def _read_jsonl(path: str) -> Iterable[dict]:
    opener = gzip.open if str(path).endswith('.gz') else open
    with opener(path, 'rt', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def _read_csv(path: str) -> list[dict]:
    with open(path, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def _safe_name(value: str) -> str:
    cleaned = ''.join(ch if ch.isalnum() or ch in '._-' else '_' for ch in value).strip('._')
    if len(cleaned) > 80:
        cleaned = cleaned[:80].rstrip('._-')
    return cleaned or 'feature_store'


def _file_hashes(paths: list[str]) -> dict[str, str | None]:
    return {
        os.path.relpath(os.path.abspath(path), HERE).replace('\\', '/'): tournament_safety._file_sha256(path)
        for path in paths
    }


def _fingerprint(source_kind: str, source_paths: list[str], starting_balance: float,
                 extra: dict | None = None) -> dict:
    payload = {
        'schema_version': SCHEMA_VERSION,
        'source_kind': source_kind,
        'source_hashes': _file_hashes(source_paths),
        'starting_balance': float(starting_balance),
        'feature_names': list(fast.FEATURE_NAMES),
        'feature_names_hash': tournament_safety.stable_json_hash(list(fast.FEATURE_NAMES), 24),
        'code_hashes': {
            'scoring_feature_store.py': tournament_safety._file_sha256('scoring_feature_store.py'),
            'scoring_variant_lab.py': tournament_safety._file_sha256('scoring_variant_lab.py'),
            'scoring_variant_lab_fast.py': tournament_safety._file_sha256('scoring_variant_lab_fast.py'),
            'backtest_30d_engine.py': tournament_safety._file_sha256('backtest_30d_engine.py'),
            'replay_artifacts.py': tournament_safety._file_sha256('replay_artifacts.py'),
        },
        'replay_rules': {
            'trade_size_pct': replay.TRADE_SIZE_PCT,
            'ticker_cfg': replay.TICKER_CFG,
            'conditional_stop_min': replay.COND_STOP_MIN,
            'flatten_hour': replay.FLATTEN_H,
            'flatten_minute': replay.FLATTEN_M,
        },
        'extra': extra or {},
    }
    payload['fingerprint'] = tournament_safety.stable_json_hash(payload, 32)
    return payload


def _feature_vector_from_trade(row: dict) -> list[float]:
    feats = slow.features(row)
    return [float(feats.get(name, 0.0) or 0.0) for name in fast.FEATURE_NAMES]


def _feature_vector_from_tape(row: dict) -> list[float]:
    feats = row.get('model_features') or (row.get('gate_features') or {}).get('profile_features') or {}
    return [float(feats.get(name, 0.0) or 0.0) for name in fast.FEATURE_NAMES]


def _feature_vector_from_opportunity(row: dict) -> list[float]:
    signal = row.get('signal') or {}
    features = row.get('features') or {}
    ticker_ind = features.get('ticker_indicators') or signal.get('indicators') or {}
    btc_ind = features.get('btc_indicators') or signal.get('btc_indicators') or {}
    feats = scoring_profiles.features_from_signal(signal, ticker_ind, btc_ind)
    return [float(feats.get(name, 0.0) or 0.0) for name in fast.FEATURE_NAMES]


def _day_from_row(row: dict) -> str:
    for key in ('day', 'entry_ct', 'ts_ct'):
        value = row.get(key)
        if value:
            return str(value)[:10]
    return ''


def _ticker_code_map(values: list[str]) -> dict[str, int]:
    return {ticker: idx for idx, ticker in enumerate(sorted(set(values)))}


def build_from_trade_csv(csv_path: str, out_dir: str, starting_balance: float,
                         name: str | None = None) -> dict:
    rows = _read_csv(csv_path)
    feature_rows = []
    pnl = []
    long_pnl = []
    short_pnl = []
    original_side = []
    days = []
    tickers = []
    opportunity_ids = []
    for idx, row in enumerate(rows):
        side = 1 if str(row.get('side')).upper() == 'LONG' else -1
        row_pnl = slow._num(row.get('pnl'))
        feature_rows.append(_feature_vector_from_trade(row))
        pnl.append(row_pnl)
        original_side.append(side)
        long_pnl.append(row_pnl if side == 1 else -row_pnl)
        short_pnl.append(row_pnl if side == -1 else -row_pnl)
        days.append(_day_from_row(row))
        tickers.append(str(row.get('ticker') or '').upper())
        opportunity_ids.append(str(row.get('opportunity_id') or f'csv_row:{idx}'))
    return write_store(
        out_dir,
        name or os.path.splitext(os.path.basename(csv_path))[0],
        'trade_csv',
        [csv_path],
        starting_balance,
        feature_rows,
        pnl,
        long_pnl,
        short_pnl,
        original_side,
        days,
        tickers,
        opportunity_ids,
        {
            'source_csv': os.path.abspath(csv_path),
            'outcome_note': 'CSV lab proxy. long/short outcome is original P/L or mechanical inverse.',
        },
    )


def _decision_pnl(outcome: dict | None, side: str, starting_balance: float, tickers_count: int) -> float:
    if not outcome:
        return 0.0
    entry = float(outcome.get('entry') or 0.0)
    exit_price = float(outcome.get('exit') or entry or 0.0)
    if entry <= 0:
        return 0.0
    alloc = round(float(starting_balance) * replay.TRADE_SIZE_PCT / max(1, tickers_count), 2)
    qty = alloc / entry
    gross = (exit_price - entry) * qty
    if side == 'SHORT':
        gross *= -1
    return round(gross, 6)


def build_from_decision_tapes(paths: list[str], out_dir: str, starting_balance: float,
                              name: str | None = None, tickers_count: int = 3) -> dict:
    feature_rows = []
    pnl = []
    long_pnl = []
    short_pnl = []
    original_side = []
    days = []
    tickers = []
    opportunity_ids = []
    exit_reason_long = []
    exit_reason_short = []
    held_long = []
    held_short = []
    mfe_long = []
    mfe_short = []
    mae_long = []
    mae_short = []
    for path in paths:
        for row in _read_jsonl(path):
            side_s = str(row.get('original_side') or '').upper()
            if side_s not in ('LONG', 'SHORT'):
                continue
            outcomes = row.get('outcomes') or {}
            lp = _decision_pnl(outcomes.get('LONG'), 'LONG', starting_balance, tickers_count)
            sp = _decision_pnl(outcomes.get('SHORT'), 'SHORT', starting_balance, tickers_count)
            original = 1 if side_s == 'LONG' else -1
            feature_rows.append(_feature_vector_from_tape(row))
            long_pnl.append(lp)
            short_pnl.append(sp)
            pnl.append(lp if original == 1 else sp)
            original_side.append(original)
            days.append(str(row.get('day') or '')[:10])
            tickers.append(str(row.get('ticker') or '').upper())
            opportunity_ids.append(str(row.get('opportunity_id') or f"{path}:{len(opportunity_ids)}"))
            exit_reason_long.append(str((outcomes.get('LONG') or {}).get('reason') or ''))
            exit_reason_short.append(str((outcomes.get('SHORT') or {}).get('reason') or ''))
            held_long.append(int((outcomes.get('LONG') or {}).get('held_sec') or 0))
            held_short.append(int((outcomes.get('SHORT') or {}).get('held_sec') or 0))
            mfe_long.append(float((outcomes.get('LONG') or {}).get('mfe_pct') or 0.0))
            mfe_short.append(float((outcomes.get('SHORT') or {}).get('mfe_pct') or 0.0))
            mae_long.append(float((outcomes.get('LONG') or {}).get('mae_pct') or 0.0))
            mae_short.append(float((outcomes.get('SHORT') or {}).get('mae_pct') or 0.0))
    return write_store(
        out_dir,
        name or 'decision_tapes',
        'decision_tape',
        paths,
        starting_balance,
        feature_rows,
        pnl,
        long_pnl,
        short_pnl,
        original_side,
        days,
        tickers,
        opportunity_ids,
        {
            'source_tapes': [os.path.abspath(path) for path in paths],
            'outcome_note': 'Decision tape exact outcomes from reusable replay artifacts.',
            'exit_reason_long': exit_reason_long,
            'exit_reason_short': exit_reason_short,
            'held_long': held_long,
            'held_short': held_short,
            'mfe_long': mfe_long,
            'mfe_short': mfe_short,
            'mae_long': mae_long,
            'mae_short': mae_short,
        },
    )


def build_from_opportunity_ledgers(paths: list[str], out_dir: str, starting_balance: float,
                                   name: str | None = None) -> dict:
    feature_rows = []
    pnl = []
    long_pnl = []
    short_pnl = []
    original_side = []
    days = []
    tickers = []
    opportunity_ids = []
    skipped = {
        'missing_outcome': 0,
        'missing_signal': 0,
        'invalid_side': 0,
    }
    outcome_meta = {
        'held_sec': [],
        'mfe_pct': [],
        'mae_pct': [],
        'exit_reason': [],
        'profile_original_side': [],
    }
    for path in paths:
        for row in _read_jsonl(path):
            outcome = row.get('outcome') or {}
            if not outcome:
                skipped['missing_outcome'] += 1
                continue
            signal = row.get('signal') or {}
            if not signal:
                skipped['missing_signal'] += 1
                continue
            side_s = str(outcome.get('side') or row.get('side') or signal.get('side') or '').upper()
            if side_s not in ('LONG', 'SHORT'):
                skipped['invalid_side'] += 1
                continue
            row_pnl = slow._num(outcome.get('pnl'))
            original = 1 if side_s == 'LONG' else -1
            feature_rows.append(_feature_vector_from_opportunity(row))
            pnl.append(row_pnl)
            original_side.append(original)
            long_pnl.append(row_pnl if original == 1 else -row_pnl)
            short_pnl.append(row_pnl if original == -1 else -row_pnl)
            days.append(str(row.get('day') or outcome.get('entry_ct') or row.get('ts_ct') or '')[:10])
            tickers.append(str(row.get('ticker') or outcome.get('ticker') or '').upper())
            opportunity_ids.append(str(row.get('opportunity_id') or outcome.get('opportunity_id') or f"{path}:{len(opportunity_ids)}"))
            outcome_meta['held_sec'].append(int(float(outcome.get('held_sec') or 0)))
            outcome_meta['mfe_pct'].append(float(outcome.get('mfe_pct') or 0.0))
            outcome_meta['mae_pct'].append(float(outcome.get('mae_pct') or 0.0))
            outcome_meta['exit_reason'].append(str(outcome.get('reason') or ''))
            outcome_meta['profile_original_side'].append(str(outcome.get('profile_original_side') or ''))
    return write_store(
        out_dir,
        name or 'opportunity_ledgers',
        'opportunity_ledger',
        paths,
        starting_balance,
        feature_rows,
        pnl,
        long_pnl,
        short_pnl,
        original_side,
        days,
        tickers,
        opportunity_ids,
        {
            'source_ledgers': [os.path.abspath(path) for path in paths],
            'outcome_note': (
                'Opportunity ledger accepted outcomes with features rebuilt via '
                'scoring_profiles.features_from_signal(signal, ticker_indicators, btc_indicators).'
            ),
            'skipped': skipped,
            **outcome_meta,
        },
    )


def write_store(out_dir: str, name: str, source_kind: str, source_paths: list[str],
                starting_balance: float, feature_rows: list[list[float]], pnl: list[float],
                long_pnl: list[float], short_pnl: list[float], original_side: list[int],
                days: list[str], tickers: list[str], opportunity_ids: list[str],
                extra: dict | None = None) -> dict:
    os.makedirs(out_dir, exist_ok=True)
    fp = _fingerprint(source_kind, source_paths, starting_balance, {
        'name': name,
        'rows': len(feature_rows),
        'extra_keys': sorted((extra or {}).keys()),
    })
    store_dir = os.path.join(out_dir, f"{_safe_name(name)}_{fp['fingerprint'][:16]}")
    os.makedirs(store_dir, exist_ok=True)
    ticker_map = _ticker_code_map(tickers)
    day_map = {day: idx for idx, day in enumerate(sorted(set(days)))}
    arrays_path = os.path.join(store_dir, 'arrays.npz')
    np.savez_compressed(
        arrays_path,
        features=np.asarray(feature_rows, dtype=np.float64),
        pnl=np.asarray(pnl, dtype=np.float64),
        long_pnl=np.asarray(long_pnl, dtype=np.float64),
        short_pnl=np.asarray(short_pnl, dtype=np.float64),
        original_side=np.asarray(original_side, dtype=np.int8),
        ticker_code=np.asarray([ticker_map[t] for t in tickers], dtype=np.int16),
        day_code=np.asarray([day_map[d] for d in days], dtype=np.int16),
    )
    ids_path = os.path.join(store_dir, 'ids.json.gz')
    with gzip.open(ids_path, 'wt', encoding='utf-8') as f:
        json.dump({
            'opportunity_id': opportunity_ids,
            'day': days,
            'ticker': tickers,
            'ticker_map': ticker_map,
            'day_map': day_map,
        }, f, separators=(',', ':'), sort_keys=True)
    extra_path = None
    if extra:
        extra_path = os.path.join(store_dir, 'outcome_metadata.json.gz')
        with gzip.open(extra_path, 'wt', encoding='utf-8') as f:
            json.dump(extra, f, separators=(',', ':'), sort_keys=True, default=str)
    manifest = {
        **fp,
        'created_at_epoch': int(time.time()),
        'name': name,
        'store_dir': store_dir,
        'arrays_path': arrays_path,
        'ids_path': ids_path,
        'extra_path': extra_path,
        'rows': len(feature_rows),
        'feature_count': len(fast.FEATURE_NAMES),
        'arrays_sha256': _sha256(arrays_path),
        'ids_sha256': _sha256(ids_path),
        'extra_sha256': _sha256(extra_path) if extra_path else None,
    }
    manifest['store_hash'] = tournament_safety.stable_json_hash({
        'fingerprint': manifest['fingerprint'],
        'arrays_sha256': manifest['arrays_sha256'],
        'ids_sha256': manifest['ids_sha256'],
        'extra_sha256': manifest['extra_sha256'],
        'rows': manifest['rows'],
        'feature_count': manifest['feature_count'],
    }, 32)
    manifest_path = os.path.join(store_dir, 'manifest.json')
    with open(manifest_path, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
    manifest['manifest_path'] = manifest_path
    return manifest


def _sha256(path: str | None) -> str | None:
    if not path or not os.path.exists(path):
        return None
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def load_store(store_dir_or_manifest: str, mmap: bool = True) -> dict:
    manifest_path = store_dir_or_manifest
    if os.path.isdir(manifest_path):
        manifest_path = os.path.join(manifest_path, 'manifest.json')
    with open(manifest_path, 'r', encoding='utf-8') as f:
        manifest = json.load(f)
    arrays = np.load(manifest['arrays_path'], mmap_mode='r' if mmap else None)
    return {'manifest': manifest, 'arrays': arrays}


def aggregate_store(store: dict) -> dict:
    arrays = store['arrays']
    features = arrays['features']
    original_side = arrays['original_side']
    pnl = arrays['pnl']
    long_pnl = arrays['long_pnl']
    short_pnl = arrays['short_pnl']
    buckets: dict[tuple, dict] = {}
    for idx in range(features.shape[0]):
        values = tuple(float(v) for v in features[idx])
        original = int(original_side[idx])
        key = (values, original)
        row_pnl = float(pnl[idx])
        flip_pnl = float(short_pnl[idx] if original == 1 else long_pnl[idx])
        bucket = buckets.setdefault(key, {
            'features': values,
            'original_side': original,
            'count': 0,
            'pnl_sum': 0.0,
            'win_count': 0,
            'loss_count': 0,
            'flipped_win_count': 0,
            'flipped_loss_count': 0,
        })
        bucket['count'] += 1
        bucket['pnl_sum'] += row_pnl
        bucket['win_count'] += int(row_pnl > 0)
        bucket['loss_count'] += int(row_pnl < 0)
        bucket['flipped_win_count'] += int(flip_pnl > 0)
        bucket['flipped_loss_count'] += int(flip_pnl < 0)
    ordered = list(buckets.values())
    return {
        'features': np.array([b['features'] for b in ordered], dtype=np.float64),
        'original_side': np.array([b['original_side'] for b in ordered], dtype=np.int8),
        'pnl_sum': np.array([b['pnl_sum'] for b in ordered], dtype=np.float64),
        'count': np.array([b['count'] for b in ordered], dtype=np.int64),
        'win_count': np.array([b['win_count'] for b in ordered], dtype=np.int64),
        'loss_count': np.array([b['loss_count'] for b in ordered], dtype=np.int64),
        'flipped_win_count': np.array([b['flipped_win_count'] for b in ordered], dtype=np.int64),
        'flipped_loss_count': np.array([b['flipped_loss_count'] for b in ordered], dtype=np.int64),
        'trades': int(sum(b['count'] for b in ordered)),
        'buckets': len(ordered),
        'store_manifest': store['manifest'],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description='Build canonical scoring feature/outcome stores.')
    ap.add_argument('--source-kind', choices=['trade-csv', 'decision-tape', 'opportunity-ledger'], required=True)
    ap.add_argument('--csv')
    ap.add_argument('--tapes', nargs='*')
    ap.add_argument('--ledgers', nargs='*')
    ap.add_argument('--out-dir', default=DEFAULT_STORE_DIR)
    ap.add_argument('--name')
    ap.add_argument('--starting-balance', type=float, default=100000.0)
    ap.add_argument('--tickers-count', type=int, default=3)
    args = ap.parse_args()
    if args.source_kind == 'trade-csv':
        if not args.csv:
            raise SystemExit('--csv required for --source-kind trade-csv')
        payload = build_from_trade_csv(args.csv, args.out_dir, args.starting_balance, args.name)
    elif args.source_kind == 'decision-tape':
        if not args.tapes:
            raise SystemExit('--tapes required for --source-kind decision-tape')
        payload = build_from_decision_tapes(args.tapes, args.out_dir, args.starting_balance, args.name, args.tickers_count)
    else:
        if not args.ledgers:
            raise SystemExit('--ledgers required for --source-kind opportunity-ledger')
        payload = build_from_opportunity_ledgers(args.ledgers, args.out_dir, args.starting_balance, args.name)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
