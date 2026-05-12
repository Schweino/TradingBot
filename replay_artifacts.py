"""Reusable replay artifact helpers.

The artifacts here are deterministic caches for expensive replay inputs:

* price shards: per-day/ticker second-level price arrays from prepared events
* outcome shards: precomputed LONG/SHORT trade-management outcomes per entry second
* manifests/fingerprints: enough metadata to know when caches are stale

They do not change live trading behavior.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import os
import platform
from dataclasses import dataclass
from datetime import date
from typing import Any

import backtest_30d_engine as replay
import step2_artifact_identity
import step2_execution_contract
import step2_latency_model


HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_ARTIFACT_DIR = os.path.join(HERE, 'postmortem', 'backtests', 'replay_artifacts')
PRICE_SCHEMA_VERSION = 1
OUTCOME_SCHEMA_VERSION = 1
LATENCY_OUTCOME_SCHEMA_VERSION = 1
MANIFEST_SCHEMA_VERSION = 1
EXECUTABLE_TRADE_FILTER_VERSION = 'execfilter_v1'
EXIT_REPLAY_MODEL_VERSION = 'quote_aware_exit_v1'
NON_EXECUTABLE_TRADE_CONDITIONS = {'B', 'W', 'Z'}


@dataclass(frozen=True)
class ArtifactKey:
    feed: str
    quote_mode: str
    btc_mode: str
    tickers: tuple[str, ...]

    @property
    def label(self) -> str:
        return f"{self.feed}_{self.quote_mode}_{self.btc_mode}_{'-'.join(self.tickers)}"


def key_from_args(args) -> ArtifactKey:
    return ArtifactKey(
        str(getattr(args, 'feed', 'sip')),
        str(getattr(args, 'quote_mode', 'per-second')),
        str(getattr(args, 'btc_mode', 'bars')),
        tuple(str(t).upper() for t in getattr(args, 'tickers', replay.TICKERS)),
    )


def _json_gz_read(path: str) -> Any | None:
    if not os.path.exists(path):
        return None
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        return json.load(f)


def _json_gz_write(path: str, payload: Any) -> None:
    step2_artifact_identity.write_canonical_json_gz(path, payload)


def _file_sha256(path: str) -> str | None:
    if not os.path.exists(path):
        return None
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ''):
            return None
        return float(value)
    except Exception:
        return None


def price_shard_path(root: str, key: ArtifactKey, day: date, ticker: str) -> str:
    return os.path.join(root, 'price_shards', key.label, day.isoformat(), f'{ticker.upper()}.prices.json.gz')


def outcome_shard_path(root: str, key: ArtifactKey, day: date, ticker: str) -> str:
    return os.path.join(root, 'outcome_shards', key.label, day.isoformat(), f'{ticker.upper()}.outcomes.json.gz')


def latency_outcome_shard_path(root: str, key: ArtifactKey, day: date, ticker: str,
                               latency_key: str) -> str:
    safe_key = ''.join(ch if ch.isalnum() or ch in '._-' else '_' for ch in latency_key)
    return os.path.join(
        root,
        'latency_outcome_shards',
        key.label,
        safe_key,
        day.isoformat(),
        f'{ticker.upper()}.outcomes.json.gz',
    )


def manifest_path(root: str, key: ArtifactKey, day: date) -> str:
    return os.path.join(root, 'manifests', key.label, f'{day.isoformat()}.manifest.json')


def latency_model_hash(path: str | None) -> str | None:
    return _file_sha256(path or step2_latency_model.DEFAULT_MODEL_PATH)


def latency_cache_key(mode: str, percentile: str, model_path: str | None) -> str:
    model_hash = latency_model_hash(model_path) or 'missing'
    return f'{mode}_{percentile}_{EXECUTABLE_TRADE_FILTER_VERSION}_{EXIT_REPLAY_MODEL_VERSION}_{model_hash[:16]}'


def is_executable_stock_trade(row: dict[str, Any] | None) -> bool:
    if not isinstance(row, dict):
        return False
    codes = row.get('c') or []
    if not isinstance(codes, list):
        codes = [codes]
    clean = {str(code).strip().upper() for code in codes if str(code).strip()}
    return not bool(clean & NON_EXECUTABLE_TRADE_CONDITIONS)


def raw_input_paths(args, day: date) -> list[str]:
    paths = []
    cache_dir = getattr(args, 'cache_dir', replay.DEFAULT_CACHE_DIR)
    feed = getattr(args, 'feed', 'sip')
    quote_mode = getattr(args, 'quote_mode', 'per-second')
    for ticker in getattr(args, 'tickers', replay.TICKERS):
        paths.append(replay._cache_path(cache_dir, 'trades', ticker, day, feed))
        if quote_mode != 'off':
            paths.append(replay._cache_path(cache_dir, f'quotes_{quote_mode}', ticker, day, feed))
    if getattr(args, 'btc_mode', 'bars') == 'bars':
        paths.append(replay._cache_path(cache_dir, 'crypto_bars_1Min', replay.BTC_SYMBOL, day, 'crypto-us'))
    paths.append(replay._prepared_tape_path(args, day))
    return paths


def artifact_fingerprint(args, day: date) -> dict:
    code_paths = [
        os.path.join(HERE, 'backtest_30d_engine.py'),
        os.path.join(HERE, 'ws_scalp.py'),
        os.path.join(HERE, 'scoring_profiles.py'),
        os.path.join(HERE, 'trading_config.json'),
    ]
    raw_paths = raw_input_paths(args, day)
    payload = {
        'schema_version': MANIFEST_SCHEMA_VERSION,
        'day': day.isoformat(),
        'key': key_from_args(args).label,
        'python': platform.python_version(),
        'session': {
            'start_hour': replay.SESSION_START_H,
            'start_minute': replay.SESSION_START_M,
            'entry_cutoff_hour': replay.ENTRY_CUTOFF_H,
            'entry_cutoff_minute': replay.ENTRY_CUTOFF_M,
            'flatten_hour': replay.FLATTEN_H,
            'flatten_minute': replay.FLATTEN_M,
            'conditional_stop_min': replay.COND_STOP_MIN,
            'trade_size_pct': replay.TRADE_SIZE_PCT,
            'ticker_cfg': replay.TICKER_CFG,
        },
        'code_hashes': {os.path.basename(path): _file_sha256(path) for path in code_paths},
        'input_hashes': {
            os.path.relpath(path, HERE).replace('\\', '/'): _file_sha256(path)
            for path in raw_paths
        },
    }
    digest_source = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('utf-8')
    payload['fingerprint'] = hashlib.sha256(digest_source).hexdigest()
    return payload


def write_manifest(root: str, args, day: date, extra: dict | None = None) -> dict:
    key = key_from_args(args)
    payload = artifact_fingerprint(args, day)
    if extra:
        payload.update(extra)
    path = manifest_path(root, key, day)
    step2_artifact_identity.write_json(path, payload)
    return payload


def price_series_from_events(events: list[dict], ticker: str, start_sec: int, end_sec: int) -> tuple[list[int], list[float | None]]:
    by_sec: dict[int, float] = {}
    for event in events:
        if event.get('symbol') != ticker or event.get('kind') != 'stock_trade':
            continue
        if not is_executable_stock_trade(event.get('row') or {}):
            continue
        sec = int(event.get('t') or 0) // 1000
        if start_sec <= sec <= end_sec:
            price = _safe_float((event.get('row') or {}).get('p'))
            if price is not None:
                by_sec[sec] = price
    ts_values = list(range(start_sec, end_sec + 1))
    prices: list[float | None] = []
    last = None
    for sec in ts_values:
        if sec in by_sec:
            last = by_sec[sec]
        prices.append(last)
    return ts_values, prices


def exit_series_from_events(events: list[dict], ticker: str, start_sec: int, end_sec: int) -> tuple[list[int], list[dict[str, Any]]]:
    by_sec: dict[int, dict[str, Any]] = {}
    has_quotes = False
    for event in events:
        if event.get('symbol') != ticker:
            continue
        kind = event.get('kind')
        row = event.get('row') or {}
        sec = int(event.get('t') or row.get('t') or 0) // 1000
        if not (start_sec <= sec <= end_sec):
            continue
        point = by_sec.setdefault(sec, {'kind': 'empty'})
        if kind == 'stock_quote':
            bid = _safe_float(row.get('bp'))
            ask = _safe_float(row.get('ap'))
            if bid is None and ask is None:
                continue
            has_quotes = True
            point.update({
                'kind': 'quote',
                'bid': bid,
                'ask': ask,
                'bid_size': _safe_float(row.get('bs')),
                'ask_size': _safe_float(row.get('as')),
            })
        elif kind == 'stock_trade':
            if not is_executable_stock_trade(row):
                continue
            price = _safe_float(row.get('p'))
            if price is None:
                continue
            point.update({
                'trade_price': price,
                'trade_size': _safe_float(row.get('s')),
                'trade_conditions': row.get('c') or [],
            })
            if point.get('kind') == 'empty':
                point['kind'] = 'trade'
    ts_values = list(range(start_sec, end_sec + 1))
    points: list[dict[str, Any]] = []
    last_trade = None
    for sec in ts_values:
        point = dict(by_sec.get(sec) or {'kind': 'empty'})
        if point.get('trade_price') is not None:
            last_trade = point.get('trade_price')
        point['last_trade_price'] = last_trade
        point['allow_trade_take_profit'] = not has_quotes
        point['exit_model'] = EXIT_REPLAY_MODEL_VERSION
        points.append(point)
    return ts_values, points


def _legacy_exit_point(value: Any) -> dict[str, Any]:
    return {
        'kind': 'trade',
        'trade_price': _safe_float(value),
        'last_trade_price': _safe_float(value),
        'allow_trade_take_profit': True,
        'legacy_price_path': True,
    }


def _as_exit_point(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else _legacy_exit_point(value)


def _mark_price(point: dict[str, Any], side: str) -> float | None:
    if point.get('kind') == 'quote':
        if side == 'LONG':
            return _safe_float(point.get('bid')) or _safe_float(point.get('last_trade_price'))
        if side == 'SHORT':
            return _safe_float(point.get('ask')) or _safe_float(point.get('last_trade_price'))
    return _safe_float(point.get('trade_price')) or _safe_float(point.get('last_trade_price'))


def _exit_trigger(side: str, point: dict[str, Any], sl: float, tp: float) -> tuple[int | None, float, str, dict[str, Any]] | None:
    if point.get('kind') == 'quote':
        bid = _safe_float(point.get('bid'))
        ask = _safe_float(point.get('ask'))
        if side == 'LONG':
            if bid is not None and bid <= sl:
                return None, sl, 'stop_loss', {'kind': 'quote', 'bid': bid, 'ask': ask}
            if bid is not None and bid >= tp:
                return None, tp, 'take_profit', {'kind': 'quote', 'bid': bid, 'ask': ask}
        else:
            if ask is not None and ask >= sl:
                return None, sl, 'stop_loss', {'kind': 'quote', 'bid': bid, 'ask': ask}
            if ask is not None and ask <= tp:
                return None, tp, 'take_profit', {'kind': 'quote', 'bid': bid, 'ask': ask}
    price = _safe_float(point.get('trade_price'))
    if price is None:
        return None
    if side == 'LONG':
        if price <= sl:
            return None, sl, 'stop_loss', {'kind': 'trade', 'price': price}
        if bool(point.get('allow_trade_take_profit')) and price >= tp:
            return None, tp, 'take_profit', {'kind': 'trade', 'price': price}
    else:
        if price >= sl:
            return None, sl, 'stop_loss', {'kind': 'trade', 'price': price}
        if bool(point.get('allow_trade_take_profit')) and price <= tp:
            return None, tp, 'take_profit', {'kind': 'trade', 'price': price}
    return None


def write_price_shard(root: str, key: ArtifactKey, day: date, ticker: str,
                      ts_values: list[int], prices: list[float | None],
                      fingerprint: str | None = None) -> str:
    payload = {
        'schema_version': PRICE_SCHEMA_VERSION,
        'day': day.isoformat(),
        'ticker': ticker.upper(),
        'key': key.label,
        'fingerprint': fingerprint,
        'ts': ts_values,
        'price': prices,
    }
    path = price_shard_path(root, key, day, ticker)
    _json_gz_write(path, payload)
    return path


def read_price_shard(root: str, key: ArtifactKey, day: date, ticker: str) -> dict | None:
    payload = _json_gz_read(price_shard_path(root, key, day, ticker))
    if not isinstance(payload, dict):
        return None
    return payload


def _outcome_for_entry(side: str, ticker: str, entry_ts: int, entry_price: float,
                       prices: list[Any], start_sec: int, flatten_ts: int,
                       end_sec: int) -> dict:
    return replay_outcome(side, ticker, entry_ts, entry_price, prices, start_sec, flatten_ts, end_sec)


def replay_outcome(side: str, ticker: str, entry_ts: int, entry_price: float,
                   prices: list[Any], start_sec: int, flatten_ts: int,
                   end_sec: int) -> dict:
    cfg = replay.TICKER_CFG.get(ticker, {'sl': 0.05, 'tp': 0.01})
    sl_pct = float(cfg.get('sl') or 0.05)
    tp_pct = float(cfg.get('tp') or 0.01)
    bracket = step2_execution_contract.exit_brackets(side, float(entry_price), sl_pct, tp_pct)
    sl = bracket['sl']
    tp = bracket['tp']

    best = worst = entry_price
    exit_ts = end_sec
    exit_price = entry_price
    exit_evidence = None
    reason = 'end_of_data'
    for sec in range(max(entry_ts, start_sec), end_sec + 1):
        idx = sec - start_sec
        point = _as_exit_point(prices[idx]) if 0 <= idx < len(prices) else _legacy_exit_point(None)
        price = _mark_price(point, side)
        if price is None:
            continue
        exit_price = float(price)
        if side == 'LONG':
            best = max(best, exit_price)
            worst = min(worst, exit_price)
            unrealized = exit_price - entry_price
        else:
            best = min(best, exit_price)
            worst = max(worst, exit_price)
            unrealized = entry_price - exit_price
        trigger = _exit_trigger(side, point, sl, tp)
        if trigger:
            _unused, exit_price, reason, evidence = trigger
            exit_ts = sec
            exit_evidence = evidence
            exit_evidence['model'] = EXIT_REPLAY_MODEL_VERSION
            break
        held_min = (sec - entry_ts) / 60
        if held_min >= replay.COND_STOP_MIN and sec < flatten_ts and unrealized < 0:
            exit_ts, reason = sec, 'cond_time_stop'
            break
        if sec >= flatten_ts:
            exit_ts, reason = sec, 'session_end'
            break

    pnl_pct = (exit_price - entry_price) / entry_price * 100
    if side == 'SHORT':
        pnl_pct *= -1
    return {
        'side': side,
        'entry': round(entry_price, 4),
        'exit': round(float(exit_price), 4),
        'exit_ts': int(exit_ts),
        'exit_ct': replay._iso_ct(int(exit_ts)),
        'held_sec': int(exit_ts - entry_ts),
        'reason': reason,
        'pnl_pct': round(pnl_pct, 6),
        'mfe_pct': round(abs(best - entry_price) / entry_price * 100, 6),
        'mae_pct': round(abs(worst - entry_price) / entry_price * 100, 6),
        'exit_evidence': exit_evidence,
    }


def replay_latency_outcome(side: str, ticker: str, entry_ts: int, entry_price: float,
                           prices: list[Any], start_sec: int, flatten_ts: int,
                           end_sec: int, model: dict, mode: str = 'entry-exit',
                           percentile: str = 'p75') -> dict:
    bracket_price = float(entry_price)
    modeled_entry = float(entry_price)
    active_entry_ts = int(entry_ts)
    latency_context = {'ticker': ticker, 'side': side, 'ts': entry_ts, 'price': entry_price}
    entry_delay_ms = 0.0
    entry_slippage_bps = 0.0
    if mode in ('entry', 'entry-exit'):
        entry_delay_ms = step2_latency_model.latency_ms(model, ticker, side, 'entry_delay_ms', percentile, context=latency_context)
        entry_slippage_bps = step2_latency_model.slippage_bps(model, ticker, side, 'entry_slippage_bps', 'p50', context=latency_context)
        active_entry_ts = int(entry_ts) + int(round(entry_delay_ms / 1000.0))
        modeled_entry = float(step2_latency_model.apply_slippage(modeled_entry, side, entry_slippage_bps, 'entry'))

    cfg = replay.TICKER_CFG.get(ticker, {'sl': 0.05, 'tp': 0.01})
    sl_pct = float(cfg.get('sl') or 0.05)
    tp_pct = float(cfg.get('tp') or 0.01)
    bracket = step2_execution_contract.exit_brackets(side, float(bracket_price), sl_pct, tp_pct)
    sl = bracket['sl']
    tp = bracket['tp']

    best = worst = modeled_entry
    exit_ts = end_sec
    exit_price = modeled_entry
    exit_evidence = None
    reason = 'end_of_data'
    for sec in range(max(active_entry_ts, start_sec), end_sec + 1):
        idx = sec - start_sec
        point = _as_exit_point(prices[idx]) if 0 <= idx < len(prices) else _legacy_exit_point(None)
        price = _mark_price(point, side)
        if price is None:
            continue
        exit_price = float(price)
        if side == 'LONG':
            best = max(best, exit_price)
            worst = min(worst, exit_price)
            unrealized = exit_price - modeled_entry
        else:
            best = min(best, exit_price)
            worst = max(worst, exit_price)
            unrealized = modeled_entry - exit_price
        trigger = _exit_trigger(side, point, sl, tp)
        if trigger:
            _unused, exit_price, reason, evidence = trigger
            exit_ts = sec
            exit_evidence = evidence
            exit_evidence['model'] = EXIT_REPLAY_MODEL_VERSION
            break
        held_min = (sec - int(entry_ts)) / 60
        if held_min >= replay.COND_STOP_MIN and sec < flatten_ts and unrealized < 0:
            exit_ts, reason = sec, 'cond_time_stop'
            break
        if sec >= flatten_ts:
            exit_ts, reason = sec, 'session_end'
            break

    exit_delay_ms = 0.0
    exit_slippage_bps = 0.0
    if mode == 'entry-exit':
        exit_delay_ms = step2_latency_model.latency_ms(model, ticker, side, 'exit_delay_ms', percentile, context=latency_context)
        exit_slippage_bps = step2_latency_model.slippage_bps(model, ticker, side, 'exit_slippage_bps', 'p50', context=latency_context)
        exit_price = float(step2_latency_model.apply_slippage(float(exit_price), side, exit_slippage_bps, 'exit'))
        exit_ts += int(round(exit_delay_ms / 1000.0))

    pnl_pct = (exit_price - modeled_entry) / modeled_entry * 100
    if side == 'SHORT':
        pnl_pct *= -1
    return {
        'side': side,
        'entry': round(modeled_entry, 4),
        'entry_signal_price': round(bracket_price, 4),
        'entry_active_ts': int(active_entry_ts),
        'exit': round(float(exit_price), 4),
        'exit_ts': int(exit_ts),
        'exit_ct': replay._iso_ct(int(exit_ts)),
        'held_sec': int(exit_ts - int(entry_ts)),
        'reason': reason,
        'pnl_pct': round(pnl_pct, 6),
        'mfe_pct': round(abs(best - modeled_entry) / modeled_entry * 100, 6),
        'mae_pct': round(abs(worst - modeled_entry) / modeled_entry * 100, 6),
        'exit_evidence': exit_evidence,
        'latency_model': {
            'mode': mode,
            'percentile': percentile,
            'entry_delay_ms': entry_delay_ms,
            'entry_slippage_bps': entry_slippage_bps,
            'exit_delay_ms': exit_delay_ms,
            'exit_slippage_bps': exit_slippage_bps,
        },
    }


def build_outcome_rows(ticker: str, ts_values: list[int], prices: list[float | None],
                       start_sec: int, flatten_ts: int, end_sec: int,
                       entry_ts_filter: set[int] | None = None,
                       exit_points: list[Any] | None = None) -> list[dict]:
    rows = []
    replay_points = exit_points if exit_points is not None else prices
    for ts, price in zip(ts_values, prices):
        if price is None:
            continue
        if entry_ts_filter is not None and ts not in entry_ts_filter:
            continue
        entry_price = float(price)
        rows.append({
            'ts': int(ts),
            'price': round(entry_price, 4),
            'LONG': _outcome_for_entry('LONG', ticker, int(ts), entry_price, replay_points, start_sec, flatten_ts, end_sec),
            'SHORT': _outcome_for_entry('SHORT', ticker, int(ts), entry_price, replay_points, start_sec, flatten_ts, end_sec),
        })
    return rows


def write_outcome_shard(root: str, key: ArtifactKey, day: date, ticker: str,
                        outcome_rows: list[dict], mode: str, fingerprint: str | None = None) -> str:
    payload = {
        'schema_version': OUTCOME_SCHEMA_VERSION,
        'day': day.isoformat(),
        'ticker': ticker.upper(),
        'key': key.label,
        'mode': mode,
        'exit_replay_model': EXIT_REPLAY_MODEL_VERSION,
        'fingerprint': fingerprint,
        'rows': outcome_rows,
    }
    path = outcome_shard_path(root, key, day, ticker)
    _json_gz_write(path, payload)
    return path


def read_outcome_shard(root: str, key: ArtifactKey, day: date, ticker: str) -> dict[int, dict] | None:
    payload = _json_gz_read(outcome_shard_path(root, key, day, ticker))
    if not isinstance(payload, dict):
        return None
    if payload.get('exit_replay_model') != EXIT_REPLAY_MODEL_VERSION:
        return None
    rows = payload.get('rows')
    if not isinstance(rows, list):
        return None
    return {int(row.get('ts')): row for row in rows if row.get('ts') is not None}


def build_latency_outcome_rows(ticker: str, ts_values: list[int], prices: list[float | None],
                               start_sec: int, flatten_ts: int, end_sec: int,
                               model: dict, mode: str, percentile: str,
                               entry_ts_filter: set[int] | None = None,
                               exit_points: list[Any] | None = None) -> list[dict]:
    rows = []
    replay_points = exit_points if exit_points is not None else prices
    for ts, price in zip(ts_values, prices):
        if price is None:
            continue
        if entry_ts_filter is not None and ts not in entry_ts_filter:
            continue
        entry_price = float(price)
        rows.append({
            'ts': int(ts),
            'price': round(entry_price, 4),
            'LONG': replay_latency_outcome('LONG', ticker, int(ts), entry_price, replay_points, start_sec, flatten_ts, end_sec, model, mode, percentile),
            'SHORT': replay_latency_outcome('SHORT', ticker, int(ts), entry_price, replay_points, start_sec, flatten_ts, end_sec, model, mode, percentile),
        })
    return rows


def write_latency_outcome_shard(root: str, key: ArtifactKey, day: date, ticker: str,
                                outcome_rows: list[dict], mode: str, percentile: str,
                                model_path: str | None, fingerprint: str | None = None) -> str:
    model_path = os.path.abspath(model_path or step2_latency_model.DEFAULT_MODEL_PATH)
    model_hash = latency_model_hash(model_path)
    cache_key = latency_cache_key(mode, percentile, model_path)
    payload = {
        'schema_version': LATENCY_OUTCOME_SCHEMA_VERSION,
        'day': day.isoformat(),
        'ticker': ticker.upper(),
        'key': key.label,
        'mode': 'decision',
        'fingerprint': fingerprint,
        'latency': {
            'cache_key': cache_key,
            'mode': mode,
            'percentile': percentile,
            'model_path': model_path,
            'model_hash': model_hash,
            'exit_replay_model': EXIT_REPLAY_MODEL_VERSION,
        },
        'rows': outcome_rows,
    }
    path = latency_outcome_shard_path(root, key, day, ticker, cache_key)
    _json_gz_write(path, payload)
    return path


def read_latency_outcome_shard(root: str, key: ArtifactKey, day: date, ticker: str,
                               mode: str, percentile: str, model_path: str | None) -> dict[int, dict] | None:
    cache_key = latency_cache_key(mode, percentile, model_path)
    payload = _json_gz_read(latency_outcome_shard_path(root, key, day, ticker, cache_key))
    if not isinstance(payload, dict):
        return None
    latency = payload.get('latency') or {}
    if latency.get('cache_key') != cache_key:
        return None
    if latency.get('exit_replay_model') != EXIT_REPLAY_MODEL_VERSION:
        return None
    rows = payload.get('rows')
    if not isinstance(rows, list):
        return None
    return {int(row.get('ts')): row for row in rows if row.get('ts') is not None}
