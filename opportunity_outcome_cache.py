from __future__ import annotations

import hashlib
import json
import os
import time
from typing import Any, Callable

import replay_artifacts
import step2_execution_contract
import step2_latency_model
import tournament_safety


HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_ROOT = os.path.join(HERE, 'postmortem', 'backtests', 'opportunity_outcome_cache')
SCHEMA_VERSION = 1


def _hash_payload(payload: Any, length: int = 32) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(',', ':'), default=str).encode('utf-8')
    return hashlib.sha256(blob).hexdigest()[:length]


def price_series_hash(prices: list[float | None]) -> str:
    return _hash_payload({'prices': prices}, 32)


def code_fingerprint() -> dict[str, str | None]:
    paths = [
        'opportunity_outcome_cache.py',
        'build_decision_tape.py',
        'replay_artifacts.py',
        'bracket_rounding.py',
        'step2_execution_contract.py',
        'step2_latency_model.py',
        'trading_config.json',
    ]
    return {path: tournament_safety._file_sha256(os.path.join(HERE, path)) for path in paths}


def _trading_config() -> dict[str, Any]:
    try:
        with open(os.path.join(HERE, 'trading_config.json'), 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def cache_key(
    day: str,
    ticker: str,
    entry_ts: int,
    entry_price: float,
    price_hash: str,
    start_sec: int,
    flatten_ts: int,
    end_sec: int,
    args: Any,
) -> str:
    latency_mode = str(getattr(args, 'step2_latency_mode', 'off') or 'off')
    latency_model = getattr(args, 'step2_latency_model', None)
    latency_percentile = str(getattr(args, 'step2_latency_percentile', 'p75') or 'p75')
    payload = {
        'schema_version': SCHEMA_VERSION,
        'day': str(day),
        'ticker': str(ticker).upper(),
        'entry_ts': int(entry_ts),
        'entry_price': round(float(entry_price), 6),
        'price_series_hash': price_hash,
        'start_sec': int(start_sec),
        'flatten_ts': int(flatten_ts),
        'end_sec': int(end_sec),
        'execution_contract_hash': step2_execution_contract.execution_contract_hash(_trading_config()),
        'latency': {
            'mode': latency_mode,
            'percentile': latency_percentile,
            'model_hash': replay_artifacts.latency_model_hash(latency_model),
        },
        'executable_trade_filter_version': replay_artifacts.EXECUTABLE_TRADE_FILTER_VERSION,
        'exit_replay_model': replay_artifacts.EXIT_REPLAY_MODEL_VERSION,
        'code_hashes': code_fingerprint(),
    }
    return _hash_payload(payload, 40)


def _cache_path(root: str, day: str, ticker: str, key: str) -> str:
    return os.path.join(root, str(day), str(ticker).upper(), f'{key}.json')


def read(root: str, day: str, ticker: str, key: str) -> dict[str, Any] | None:
    path = _cache_path(root, day, ticker, key)
    if not os.path.exists(path):
        return None
    try:
        with open(path, 'r', encoding='utf-8') as f:
            payload = json.load(f)
    except Exception:
        return None
    if not isinstance(payload, dict) or payload.get('cache_key') != key:
        return None
    outcomes = payload.get('outcomes')
    if not isinstance(outcomes, dict) or not outcomes.get('LONG') or not outcomes.get('SHORT'):
        return None
    return payload


def write(root: str, day: str, ticker: str, key: str, payload: dict[str, Any]) -> str:
    path = _cache_path(root, day, ticker, key)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f'{path}.{os.getpid()}.{int(time.time() * 1000)}.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
    os.replace(tmp, path)
    return os.path.abspath(path)


def get_or_compute(
    day: str,
    ticker: str,
    entry_ts: int,
    entry_price: float,
    prices: list[float | None],
    start_sec: int,
    flatten_ts: int,
    end_sec: int,
    args: Any,
    compute: Callable[[], dict[str, dict[str, Any]]],
    root: str = DEFAULT_ROOT,
    precomputed_price_hash: str | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    price_hash = precomputed_price_hash or price_series_hash(prices)
    key = cache_key(day, ticker, entry_ts, entry_price, price_hash, start_sec, flatten_ts, end_sec, args)
    cached = read(root, day, ticker, key)
    if cached:
        return cached['outcomes'], {
            'source': 'opportunity_outcome_cache',
            'cache_hit': True,
            'cache_key': key,
            'path': _cache_path(root, day, ticker, key),
        }
    outcomes = compute()
    payload = {
        'schema_version': SCHEMA_VERSION,
        'cache_key': key,
        'created_at_epoch': int(time.time()),
        'day': str(day),
        'ticker': str(ticker).upper(),
        'entry_ts': int(entry_ts),
        'entry_price': round(float(entry_price), 6),
        'price_series_hash': price_hash,
        'start_sec': int(start_sec),
        'flatten_ts': int(flatten_ts),
        'end_sec': int(end_sec),
        'latency': {
            'mode': str(getattr(args, 'step2_latency_mode', 'off') or 'off'),
            'percentile': str(getattr(args, 'step2_latency_percentile', 'p75') or 'p75'),
            'model_hash': replay_artifacts.latency_model_hash(getattr(args, 'step2_latency_model', None)),
        },
        'outcomes': outcomes,
    }
    path = write(root, day, ticker, key, payload)
    return outcomes, {
        'source': 'computed_inline_cached',
        'cache_hit': False,
        'cache_key': key,
        'path': path,
    }
