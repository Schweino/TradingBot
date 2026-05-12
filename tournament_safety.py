"""Safety/audit helpers for staged scoring-model tournaments.

This module does not change trading logic or replay math. It adds immutable
identifiers, fingerprints, diversity grouping, composite ranking, and registry
records so model selection is auditable across the four-stage workflow.
"""
from __future__ import annotations

from output_paths import output_path

import csv
import hashlib
import json
import os
import time
from typing import Any

import backtest_30d_engine as replay
import replay_artifacts


HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_REGISTRY = output_path('postmortem', 'backtests', 'tournament_registry.csv')
HASH_CACHE_PATH = output_path('data_cache', 'file_hash_cache.json')
_HASH_CACHE: dict[str, Any] | None = None
CODE_INPUTS = [
    'ws_scalp.py',
    'backtest_30d_engine.py',
    'scoring_profiles.py',
    'scoring_variant_lab.py',
    'scoring_variant_lab_fast.py',
    'simulate_decision_tape.py',
    'decision_tape_gates.py',
    'trading_config.json',
]


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ''):
            return default
        return float(value)
    except Exception:
        return default


def _load_hash_cache() -> dict[str, Any]:
    global _HASH_CACHE
    if _HASH_CACHE is not None:
        return _HASH_CACHE
    try:
        with open(HASH_CACHE_PATH, 'r', encoding='utf-8') as f:
            payload = json.load(f)
        _HASH_CACHE = payload if isinstance(payload, dict) else {}
    except Exception:
        _HASH_CACHE = {}
    return _HASH_CACHE


def _write_hash_cache() -> None:
    if _HASH_CACHE is None:
        return
    try:
        os.makedirs(os.path.dirname(HASH_CACHE_PATH), exist_ok=True)
        tmp = f'{HASH_CACHE_PATH}.{os.getpid()}.{int(time.time() * 1000)}.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(_HASH_CACHE, f, sort_keys=True, separators=(',', ':'))
        os.replace(tmp, HASH_CACHE_PATH)
    except Exception:
        try:
            if 'tmp' in locals() and os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


def _file_sha256(path: str) -> str | None:
    if not path:
        return None
    if not os.path.isabs(path):
        path = os.path.join(HERE, path)
    if not os.path.exists(path):
        return None
    try:
        st = os.stat(path)
        key = os.path.abspath(path)
        marker = {
            'size': int(st.st_size),
            'mtime_ns': int(getattr(st, 'st_mtime_ns', int(st.st_mtime * 1_000_000_000))),
        }
        cache = _load_hash_cache()
        cached = cache.get(key)
        if isinstance(cached, dict) and cached.get('size') == marker['size'] and cached.get('mtime_ns') == marker['mtime_ns']:
            digest = cached.get('sha256')
            if digest:
                return str(digest)
    except Exception:
        key = ''
        marker = {}
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    digest = h.hexdigest()
    if key and marker:
        cache = _load_hash_cache()
        cache[key] = {**marker, 'sha256': digest}
        _write_hash_cache()
    return digest


def stable_json_hash(payload: Any, length: int = 16) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(',', ':'), default=str).encode('utf-8')
    return hashlib.sha256(raw).hexdigest()[:length]


def model_id(name: str, weights: dict | None, bias: float = 0.0) -> str:
    return stable_json_hash({
        'name': name,
        'weights': {k: round(float(v or 0.0), 8) for k, v in sorted((weights or {}).items())},
        'bias': round(float(bias or 0.0), 8),
    }, length=20)


def variant_family(weights: dict | None, top_n: int = 4) -> str:
    weights = weights or {}
    ranked = sorted(
        ((k, float(v or 0.0)) for k, v in weights.items() if abs(float(v or 0.0)) > 1e-9),
        key=lambda kv: (-abs(kv[1]), kv[0]),
    )
    signature = tuple((k, 1 if v > 0 else -1) for k, v in ranked[:top_n])
    return stable_json_hash(signature, length=12)


def run_fingerprint(args, extra_paths: list[str] | None = None) -> dict:
    code_hashes = {path: _file_sha256(path) for path in CODE_INPUTS}
    extra_hashes = {}
    for path in extra_paths or []:
        extra_hashes[path] = _file_sha256(path)
    day_fingerprints = {}
    try:
        for day in replay._market_days(replay._parse_day(args.start), replay._parse_day(args.end)):
            day_fingerprints[day.isoformat()] = replay_artifacts.artifact_fingerprint(args, day).get('fingerprint')
    except Exception:
        day_fingerprints = {}
    payload = {
        'schema_version': 1,
        'created_at_epoch': int(time.time()),
        'config': {
            'start': getattr(args, 'start', None),
            'end': getattr(args, 'end', None),
            'train_start': getattr(args, 'train_start', None),
            'train_end': getattr(args, 'train_end', None),
            'test_start': getattr(args, 'test_start', None),
            'test_end': getattr(args, 'test_end', None),
            'tickers': list(getattr(args, 'tickers', []) or []),
            'feed': getattr(args, 'feed', None),
            'quote_mode': getattr(args, 'quote_mode', None),
            'btc_mode': getattr(args, 'btc_mode', None),
            'entry_gate_mode': getattr(args, 'entry_gate_mode', None),
            'entry_gate_threshold_pct': getattr(args, 'entry_gate_threshold_pct', None),
            'start_balance': getattr(args, 'start_balance', None),
        },
        'code_hashes': code_hashes,
        'extra_hashes': extra_hashes,
        'day_artifact_fingerprints': day_fingerprints,
    }
    payload['run_hash'] = stable_json_hash(payload, length=20)
    return payload


def enrich_lab_results(results: list[dict]) -> list[dict]:
    for rank, row in enumerate(results, 1):
        row.setdefault('lab_rank', rank)
        row['model_id'] = model_id(row.get('variant') or '', row.get('weights') or {}, row.get('bias') or 0.0)
        row['family_id'] = variant_family(row.get('weights') or {})
        row['stage'] = 'lab'
    return results


def _worst_day(summary: dict | None) -> float:
    if not summary:
        return 0.0
    worst = summary.get('worst_day') or {}
    return _num(worst.get('pnl'))


def _max_day_share(summary: dict | None) -> float:
    if not summary:
        return 0.0
    total = abs(_num(summary.get('pnl')))
    if total <= 0:
        return 0.0
    by_day = summary.get('by_day') or {}
    if not by_day:
        return 0.0
    return max(abs(_num(row.get('pnl'))) for row in by_day.values()) / total


def _max_ticker_share(summary: dict | None) -> float:
    if not summary:
        return 0.0
    total = abs(_num(summary.get('pnl')))
    if total <= 0:
        return 0.0
    by_ticker = summary.get('by_ticker') or {}
    if not by_ticker:
        return 0.0
    return max(abs(_num(row.get('pnl'))) for row in by_ticker.values()) / total


def disqualification_flags(full: dict | None, train: dict | None = None, test: dict | None = None,
                           min_trades: int = 50, max_trades: int = 100000,
                           max_concentration: float = 0.75) -> list[str]:
    flags = []
    full = full or {}
    trades = int(_num(full.get('trades')))
    if trades < min_trades:
        flags.append('too_few_trades')
    if trades > max_trades:
        flags.append('too_many_trades')
    if _max_day_share(full) > max_concentration:
        flags.append('one_day_concentration')
    if _max_ticker_share(full) > max_concentration:
        flags.append('one_ticker_concentration')
    if train and test:
        train_pnl = _num(train.get('pnl'))
        test_pnl = _num(test.get('pnl'))
        if train_pnl > 0 and test_pnl < 0:
            flags.append('train_positive_test_negative')
        if abs(test_pnl) > 0 and train_pnl / max(abs(test_pnl), 1.0) > 8:
            flags.append('train_test_divergence')
    return flags


def composite_score(row: dict, lab_rank: int | None = None, decision_rank: int | None = None,
                    full_rank: int | None = None, max_rank: int = 10000) -> float:
    full = row.get('decision_full') or ((row.get('full_replay') or {}).get('summary')) or {}
    train = row.get('decision_train') or ((row.get('train_replay') or {}).get('summary'))
    test = row.get('decision_test') or ((row.get('test_replay') or {}).get('summary'))
    lab_component = 1.0 - min(lab_rank or max_rank, max_rank) / max_rank
    decision_component = 1.0 - min(decision_rank or max_rank, max_rank) / max_rank
    full_component = 1.0 - min(full_rank or max_rank, max_rank) / max_rank
    pnl_component = _num((test or full).get('pnl')) / 10000.0
    stability_penalty = abs(_num((train or {}).get('pnl')) - _num((test or train or {}).get('pnl'))) / 20000.0 if train and test else 0.0
    worst_day_penalty = abs(min(0.0, _worst_day(full))) / 5000.0
    concentration_penalty = max(_max_day_share(full), _max_ticker_share(full)) * 0.5
    flags = disqualification_flags(full, train, test)
    flag_penalty = 0.4 * len(flags)
    return round(
        0.10 * lab_component
        + 0.25 * decision_component
        + 0.35 * full_component
        + 0.30 * pnl_component
        - stability_penalty
        - worst_day_penalty
        - concentration_penalty
        - flag_penalty,
        6,
    )


def enrich_decision_results(results: list[dict], lab_results: list[dict] | None = None,
                            min_trades: int = 50, max_trades: int = 100000) -> list[dict]:
    lab_by_name = {row.get('variant'): row for row in (lab_results or [])}
    for idx, row in enumerate(results, 1):
        row.setdefault('decision_rank', idx)
        lab = lab_by_name.get(row.get('variant')) or {}
        row['model_id'] = model_id(row.get('variant') or '', row.get('weights') or {}, row.get('bias') or 0.0)
        row['family_id'] = variant_family(row.get('weights') or {})
        row['lab_rank'] = lab.get('lab_rank')
        row['lab_pnl'] = lab.get('pnl', row.get('lab_pnl'))
        row['selection_flags'] = disqualification_flags(
            row.get('decision_full'),
            row.get('decision_train'),
            row.get('decision_test'),
            min_trades=min_trades,
            max_trades=max_trades,
        )
        row['composite_score'] = composite_score(row, row.get('lab_rank'), row.get('decision_rank'), None)
        row['stage'] = 'decision_tape'
    results.sort(key=lambda r: (r.get('composite_score', -10**9), _num((r.get('decision_test') or r.get('decision_full') or {}).get('pnl'))), reverse=True)
    for idx, row in enumerate(results, 1):
        row['composite_rank'] = idx
    return results


def select_diverse_finalists(results: list[dict], limit: int, per_family: int = 3) -> list[dict]:
    selected = []
    counts: dict[str, int] = {}
    for row in results:
        fam = row.get('family_id') or 'unknown'
        if counts.get(fam, 0) >= per_family:
            continue
        selected.append(row)
        counts[fam] = counts.get(fam, 0) + 1
        if len(selected) >= limit:
            break
    if len(selected) < limit:
        seen = {row.get('variant') for row in selected}
        for row in results:
            if row.get('variant') in seen:
                continue
            selected.append(row)
            if len(selected) >= limit:
                break
    return selected


def enrich_full_results(results: list[dict], decision_results: list[dict] | None = None) -> list[dict]:
    decision_by_name = {row.get('variant'): row for row in (decision_results or [])}
    sorted_full = sorted(
        results,
        key=lambda r: _num((((r.get('test_replay') or {}).get('summary')) or ((r.get('full_replay') or {}).get('summary')) or {}).get('pnl')),
        reverse=True,
    )
    full_rank_by_name = {row.get('variant'): idx for idx, row in enumerate(sorted_full, 1)}
    for row in results:
        decision = decision_by_name.get(row.get('variant')) or {}
        summary = ((row.get('full_replay') or {}).get('summary')) or {}
        train = ((row.get('train_replay') or {}).get('summary')) or None
        test = ((row.get('test_replay') or {}).get('summary')) or None
        row['model_id'] = decision.get('model_id') or model_id(row.get('variant') or '', decision.get('weights') or {}, decision.get('bias') or 0.0)
        row['family_id'] = decision.get('family_id') or variant_family(decision.get('weights') or {})
        row['decision_rank'] = decision.get('decision_rank')
        row['composite_rank_from_decision'] = decision.get('composite_rank')
        row['selection_flags'] = disqualification_flags(summary, train, test)
        row['composite_score'] = composite_score(row, decision.get('lab_rank'), decision.get('decision_rank'), full_rank_by_name.get(row.get('variant')))
        row['stage'] = 'full_replay'
    results.sort(key=lambda r: (r.get('composite_score', -10**9), _num((((r.get('test_replay') or {}).get('summary')) or ((r.get('full_replay') or {}).get('summary')) or {}).get('pnl'))), reverse=True)
    for idx, row in enumerate(results, 1):
        row['composite_rank'] = idx
    return results


def registry_rows(payload: dict, summary_path: str) -> list[dict]:
    run_hash = ((payload.get('fingerprints') or {}).get('run_hash')) or stable_json_hash(payload.get('config') or {}, 20)
    out = []
    for stage_name, key in [('lab', 'lab_results'), ('decision_tape', 'decision_results'), ('full_replay', 'full_replay_results')]:
        for row in payload.get(key) or []:
            if stage_name == 'lab':
                summary = row
                pnl = row.get('pnl')
                trades = row.get('trades')
            elif stage_name == 'decision_tape':
                summary = row.get('decision_test') or row.get('decision_full') or {}
                pnl = summary.get('pnl')
                trades = summary.get('trades')
            else:
                summary = ((row.get('test_replay') or {}).get('summary')) or ((row.get('full_replay') or {}).get('summary')) or {}
                pnl = summary.get('pnl')
                trades = summary.get('trades')
            out.append({
                'created_at_epoch': int(time.time()),
                'run_name': payload.get('run_name'),
                'run_hash': run_hash,
                'stage': stage_name,
                'model_id': row.get('model_id'),
                'family_id': row.get('family_id'),
                'variant': row.get('variant'),
                'composite_rank': row.get('composite_rank'),
                'composite_score': row.get('composite_score'),
                'pnl': pnl,
                'trades': trades,
                'win_rate_pct': summary.get('win_rate_pct'),
                'flags': '|'.join(row.get('selection_flags') or []),
                'summary_path': summary_path,
            })
    return out


def append_registry(rows: list[dict], path: str = DEFAULT_REGISTRY) -> str:
    if not rows:
        return path
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fieldnames = [
        'created_at_epoch', 'run_name', 'run_hash', 'stage', 'model_id', 'family_id',
        'variant', 'composite_rank', 'composite_score', 'pnl', 'trades',
        'win_rate_pct', 'flags', 'summary_path',
    ]
    exists = os.path.exists(path)
    with open(path, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})
    return path
