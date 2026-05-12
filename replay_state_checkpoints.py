from __future__ import annotations

from output_paths import output_path

import gzip
import json
import os
import threading
import time
from collections import deque
from datetime import date, datetime
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import backtest_30d_engine as replay
import tournament_safety
import ws_scalp


HERE = os.path.dirname(os.path.abspath(__file__))
CT = ZoneInfo('America/Chicago')
DEFAULT_ROOT = output_path('postmortem', 'backtests', 'replay_state_checkpoints')
SCHEMA_VERSION = 1
DEFAULT_BUCKET_SEC = 300


def _json_gz_write(path: str, payload: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f'{path}.{os.getpid()}.{int(time.time() * 1000)}.tmp'
    with gzip.open(tmp, 'wt', encoding='utf-8') as f:
        json.dump(payload, f, sort_keys=True, separators=(',', ':'), default=str)
    os.replace(tmp, path)


def _json_gz_read(path: str) -> Any | None:
    if not os.path.exists(path):
        return None
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        return json.load(f)


def _safe_label(args: Any) -> str:
    tickers = '-'.join(str(t).upper() for t in getattr(args, 'tickers', []))
    return f"{getattr(args, 'feed', 'sip')}_{getattr(args, 'quote_mode', 'per-second')}_{getattr(args, 'btc_mode', 'bars')}_{getattr(args, 'indicator_mode', 'live')}_{tickers}"


def _state_path(root: str, args: Any, day: date, checkpoint_sec: int) -> str:
    return os.path.join(root, _safe_label(args), day.isoformat(), f'{int(checkpoint_sec)}.state.json.gz')


def manifest_path(root: str, args: Any, day: date) -> str:
    return os.path.join(root, _safe_label(args), day.isoformat(), 'manifest.json')


def _deque_payload(value: Any) -> list:
    if isinstance(value, deque):
        return list(value)
    if isinstance(value, list):
        return value
    return list(value or [])


def serialize_state(st: ws_scalp.SymbolState) -> dict[str, Any]:
    with st.lock:
        payload = {
            'symbol': st.symbol,
            'last_trade_price': st.last_trade_price,
            'last_trade_size': st.last_trade_size,
            'last_trade_ts_ms': st.last_trade_ts_ms,
            'last_trade_exchange': st.last_trade_exchange,
            'last_trade_conditions': list(st.last_trade_conditions or []),
            'last_trade_tape': st.last_trade_tape,
            'last_trade_id': st.last_trade_id,
            'best_bid': st.best_bid,
            'best_ask': st.best_ask,
            'bid_size': st.bid_size,
            'ask_size': st.ask_size,
            'last_quote_ts_ms': st.last_quote_ts_ms,
            'last_quote_bid_exchange': st.last_quote_bid_exchange,
            'last_quote_ask_exchange': st.last_quote_ask_exchange,
            'last_quote_conditions': list(st.last_quote_conditions or []),
            'last_quote_tape': st.last_quote_tape,
            'quote_history': _deque_payload(st.quote_history),
            'trades': _deque_payload(st.trades),
            'pending_trades': _deque_payload(st.pending_trades),
            'bars_1s': _deque_payload(st.bars_1s),
            'flow_history': _deque_payload(st.flow_history),
            'session_pv_sum': st.session_pv_sum,
            'session_v_sum': st.session_v_sum,
            'session_date': st.session_date,
            'replay_bar_cache': _normalize_cache(getattr(st, '_replay_bar_cache', None)),
            'replay_quote_spreads': _deque_payload(getattr(st, '_replay_quote_spreads', [])),
        }
    return payload


def _normalize_cache(cache: Any) -> Any:
    if isinstance(cache, deque):
        return list(cache)
    if isinstance(cache, dict):
        out = {}
        for key, value in cache.items():
            if isinstance(value, deque):
                out[key] = list(value)
            elif isinstance(value, dict):
                out[key] = _normalize_cache(value)
            else:
                out[key] = value
        return out
    return cache


def _restore_deque(rows: list, maxlen: int | None = None) -> deque:
    return deque(rows or [], maxlen=maxlen)


def restore_state(payload: dict[str, Any]) -> ws_scalp.SymbolState:
    st = ws_scalp.SymbolState(symbol=str(payload.get('symbol') or ''))
    st.last_trade_price = payload.get('last_trade_price')
    st.last_trade_size = int(payload.get('last_trade_size') or 0)
    st.last_trade_ts_ms = int(payload.get('last_trade_ts_ms') or 0)
    st.last_trade_exchange = payload.get('last_trade_exchange')
    st.last_trade_conditions = list(payload.get('last_trade_conditions') or [])
    st.last_trade_tape = payload.get('last_trade_tape')
    st.last_trade_id = payload.get('last_trade_id')
    st.best_bid = payload.get('best_bid')
    st.best_ask = payload.get('best_ask')
    st.bid_size = payload.get('bid_size')
    st.ask_size = payload.get('ask_size')
    st.last_quote_ts_ms = int(payload.get('last_quote_ts_ms') or 0)
    st.last_quote_bid_exchange = payload.get('last_quote_bid_exchange')
    st.last_quote_ask_exchange = payload.get('last_quote_ask_exchange')
    st.last_quote_conditions = list(payload.get('last_quote_conditions') or [])
    st.last_quote_tape = payload.get('last_quote_tape')
    st.quote_history = _restore_deque(payload.get('quote_history') or [], 300)
    st.trades = _restore_deque(payload.get('trades') or [], ws_scalp.TRADE_RING)
    st.pending_trades = _restore_deque(payload.get('pending_trades') or [])
    st.bars_1s = _restore_deque(payload.get('bars_1s') or [], ws_scalp.RING_SECONDS)
    st.flow_history = _restore_deque(payload.get('flow_history') or [], ws_scalp.FLOW_MINUTES)
    st.session_pv_sum = float(payload.get('session_pv_sum') or 0.0)
    st.session_v_sum = float(payload.get('session_v_sum') or 0.0)
    st.session_date = payload.get('session_date')
    st.lock = threading.Lock()
    cache = payload.get('replay_bar_cache')
    if isinstance(cache, dict):
        cache = dict(cache)
        if isinstance(cache.get('high_max'), list):
            cache['high_max'] = deque(tuple(x) for x in cache.get('high_max') or [])
        if isinstance(cache.get('low_min'), list):
            cache['low_min'] = deque(tuple(x) for x in cache.get('low_min') or [])
        st._replay_bar_cache = cache
    else:
        st._replay_bar_cache = getattr(replay._state(st.symbol), '_replay_bar_cache', {})
    st._replay_quote_spreads = _restore_deque(payload.get('replay_quote_spreads') or [], 300)
    return st


def restore_states(payload: dict[str, Any]) -> dict[str, ws_scalp.SymbolState]:
    return {
        symbol: restore_state(row)
        for symbol, row in (payload.get('states') or {}).items()
        if isinstance(row, dict)
    }


def _fingerprint(args: Any, day: date) -> dict[str, Any]:
    paths = [
        os.path.join(HERE, 'backtest_30d_engine.py'),
        os.path.join(HERE, 'ws_scalp.py'),
        os.path.join(HERE, 'trading_config.json'),
        replay._prepared_tape_path(args, day),
    ]
    payload = {
        'schema_version': SCHEMA_VERSION,
        'label': _safe_label(args),
        'day': day.isoformat(),
        'paths': {os.path.relpath(path, HERE).replace('\\', '/'): tournament_safety._file_sha256(path) for path in paths},
    }
    payload['fingerprint'] = tournament_safety.stable_json_hash(payload, 32)
    return payload


def load_best_before(root: str, args: Any, day: date, target_sec: int) -> dict[str, Any] | None:
    manifest = _read_manifest(root, args, day)
    if not manifest:
        return None
    current_fp = _fingerprint(args, day).get('fingerprint')
    if manifest.get('fingerprint') != current_fp:
        return None
    checkpoints = [
        row for row in manifest.get('checkpoints') or []
        if int(row.get('checkpoint_sec') or 0) < int(target_sec)
    ]
    if not checkpoints:
        return None
    row = max(checkpoints, key=lambda item: int(item.get('checkpoint_sec') or 0))
    payload = _json_gz_read(row.get('path') or '')
    if not isinstance(payload, dict):
        return None
    if payload.get('fingerprint') != current_fp:
        return None
    return payload


def _read_manifest(root: str, args: Any, day: date) -> dict[str, Any] | None:
    path = manifest_path(root, args, day)
    if not os.path.exists(path):
        return None
    try:
        with open(path, 'r', encoding='utf-8') as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def ensure_day(root: str, args: Any, day: date, bucket_sec: int = DEFAULT_BUCKET_SEC) -> dict[str, Any]:
    current_fp = _fingerprint(args, day)
    existing = _read_manifest(root, args, day)
    if existing and existing.get('fingerprint') == current_fp.get('fingerprint'):
        return {'status': 'fresh', **existing}

    tape_args = type('Args', (), {
        'tickers': args.tickers,
        'feed': args.feed,
        'quote_mode': args.quote_mode,
        'btc_mode': args.btc_mode,
        'cache_dir': args.cache_dir,
        'prepared_cache_dir': args.prepared_cache_dir,
        'use_prepared_events': True,
        'refresh_prepared_events': False,
        'refresh': False,
        'max_pages': 1000,
    })()
    stats = replay.ReplayStats()
    events = replay._load_day_events(day, tape_args, stats)
    _start_iso, _end_iso, start_sec, end_sec = replay._session_bounds_utc(day)
    states = {sym: replay._state(sym) for sym in list(args.tickers) + [replay.BTC_SYMBOL]}
    by_sec: dict[int, list[dict]] = {}
    for event in events:
        sec = int(event['t']) // 1000
        if start_sec <= sec < end_sec:
            by_sec.setdefault(sec, []).append(event)

    checkpoints = []
    for sec in range(start_sec, end_sec):
        for event in by_sec.get(sec, []):
            st = states.get(event['symbol'])
            if not st:
                continue
            if event['kind'] == 'stock_trade':
                replay._feed_stock_trade(st, event['symbol'], event['row'])
            elif event['kind'] == 'stock_quote':
                replay._feed_stock_quote(st, event['row'])
            elif event['kind'] == 'btc_synth_trade':
                replay._feed_btc_synth(st, event['row'])
        for st in states.values():
            replay._roll_bar(st, sec)
        if sec > start_sec and sec % int(bucket_sec) == 0:
            payload = {
                'schema_version': SCHEMA_VERSION,
                'day': day.isoformat(),
                'checkpoint_sec': int(sec),
                'checkpoint_ct': datetime.fromtimestamp(sec, CT).isoformat(timespec='seconds'),
                'fingerprint': current_fp.get('fingerprint'),
                'states': {symbol: serialize_state(st) for symbol, st in states.items()},
            }
            path = _state_path(root, args, day, sec)
            _json_gz_write(path, payload)
            checkpoints.append({
                'checkpoint_sec': int(sec),
                'checkpoint_ct': payload['checkpoint_ct'],
                'path': os.path.abspath(path),
            })
    manifest = {
        'schema_version': SCHEMA_VERSION,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'day': day.isoformat(),
        'label': current_fp.get('label'),
        'fingerprint': current_fp.get('fingerprint'),
        'bucket_sec': int(bucket_sec),
        'checkpoint_count': len(checkpoints),
        'checkpoints': checkpoints,
        'cache_hits': dict(stats.cache_hits),
        'fetched': dict(stats.fetched),
    }
    path = manifest_path(root, args, day)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
    manifest['manifest_path'] = os.path.abspath(path)
    return {'status': 'rebuilt', **manifest}
