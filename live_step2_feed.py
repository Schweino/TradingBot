from __future__ import annotations

import argparse
import gzip
import json
import os
import queue
import threading
import time
from datetime import datetime, time as dt_time
from typing import Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore

HERE = os.path.dirname(os.path.abspath(__file__))
CT = ZoneInfo('America/Chicago')
LIVE_EVENT_DIR = os.path.join(HERE, 'postmortem', 'live_step2_events')
BTC_SYMBOL = 'BTC/USD'
CANONICAL_PREPARED_DIR = os.path.join(HERE, 'data_cache', 'alpaca_engine_replay_tapes')
DEFAULT_PREPARED_DIR = os.path.join(HERE, 'data_cache', 'live_intraday_tapes')
SESSION_START_CT = dt_time(8, 30)
SESSION_END_CT = dt_time(15, 1)

_QUEUE: queue.Queue[dict | None] = queue.Queue(maxsize=200_000)
_THREAD: Optional[threading.Thread] = None
_MATERIALIZER_THREAD: Optional[threading.Thread] = None
_STOP = threading.Event()
_MATERIALIZER_STOP = threading.Event()
_LOCK = threading.Lock()
_DROPPED = 0
_WRITTEN = 0

try:
    import incremental_market_store
except Exception:
    incremental_market_store = None


def _day_from_ts_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, CT).date().isoformat()


def _jsonl_path(day: str) -> str:
    return os.path.join(LIVE_EVENT_DIR, f'live_step2_events_{day}.jsonl')


def _prepared_path(day: str, tickers: list[str], feed: str, quote_mode: str, btc_mode: str,
                   prepared_cache_dir: str) -> str:
    tickers_part = '-'.join(tickers)
    stem = f'{feed}_{quote_mode}_{btc_mode}_{tickers_part}_{day}.events.json.gz'
    return os.path.join(prepared_cache_dir, stem)


def _live_index_dir(day: str) -> str:
    if incremental_market_store is None:
        return ''
    return os.path.join(incremental_market_store.DEFAULT_ROOT, day, 'live_index')


def _live_index_paths(day: str) -> list[str]:
    index_dir = _live_index_dir(day)
    if not index_dir or not os.path.isdir(index_dir):
        return []
    return [
        os.path.join(index_dir, name)
        for name in os.listdir(index_dir)
        if name.endswith('.events.jsonl') and os.path.isfile(os.path.join(index_dir, name))
    ]


def _live_sources_mtime(day: str) -> float:
    index_paths = _live_index_paths(day)
    if index_paths:
        return max(os.path.getmtime(path) for path in index_paths)
    mtimes = []
    jsonl = _jsonl_path(day)
    if os.path.exists(jsonl):
        mtimes.append(os.path.getmtime(jsonl))
    index_dir = _live_index_dir(day)
    if index_dir and os.path.isdir(index_dir):
        mtimes.extend(os.path.getmtime(path) for path in _live_index_paths(day))
    return max(mtimes, default=0.0)


def _in_replay_session(ts_ms: int) -> bool:
    if not ts_ms:
        return False
    local = datetime.fromtimestamp(ts_ms / 1000.0, CT)
    return SESSION_START_CT <= local.time() < SESSION_END_CT


def _writer_loop() -> None:
    global _WRITTEN
    handles: dict[str, object] = {}
    last_flush = time.time()
    try:
        while not _STOP.is_set() or not _QUEUE.empty():
            try:
                row = _QUEUE.get(timeout=0.5)
            except queue.Empty:
                row = None
            if row is not None:
                day = row.get('day') or _day_from_ts_ms(int(row.get('t') or 0))
                row['day'] = day
                path = _jsonl_path(day)
                handle = handles.get(path)
                if handle is None:
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    handle = open(path, 'a', encoding='utf-8')
                    handles[path] = handle
                handle.write(json.dumps(row, separators=(',', ':'), sort_keys=True) + '\n')
                _WRITTEN += 1
                if incremental_market_store is not None:
                    try:
                        incremental_market_store.append_live_event(row)
                    except Exception:
                        pass
            now = time.time()
            if now - last_flush >= 1.0:
                for handle in handles.values():
                    handle.flush()
                last_flush = now
    finally:
        for handle in handles.values():
            try:
                handle.flush()
                handle.close()
            except Exception:
                pass


def start() -> None:
    global _THREAD
    with _LOCK:
        if _THREAD and _THREAD.is_alive():
            return
        _STOP.clear()
        _THREAD = threading.Thread(target=_writer_loop, name='live-step2-feed-writer', daemon=True)
        _THREAD.start()


def stop(timeout: float = 2.0) -> None:
    _STOP.set()
    thread = _THREAD
    if thread and thread.is_alive():
        thread.join(timeout=timeout)
    _MATERIALIZER_STOP.set()
    materializer = _MATERIALIZER_THREAD
    if materializer and materializer.is_alive():
        materializer.join(timeout=timeout)


def status() -> dict:
    return {
        'enabled': True,
        'queue_size': _QUEUE.qsize(),
        'dropped': _DROPPED,
        'written': _WRITTEN,
        'dir': LIVE_EVENT_DIR,
        'intraday_prepared_dir': DEFAULT_PREPARED_DIR,
        'canonical_prepared_dir': CANONICAL_PREPARED_DIR,
        'materializer_running': bool(_MATERIALIZER_THREAD and _MATERIALIZER_THREAD.is_alive()),
    }


def _materializer_loop(tickers: list[str], interval_sec: int, feed: str,
                       quote_mode: str, btc_mode: str) -> None:
    while not _MATERIALIZER_STOP.is_set():
        day = datetime.now(CT).date().isoformat()
        try:
            materialize(
                day=day,
                tickers=tickers,
                feed=feed,
                quote_mode=quote_mode,
                btc_mode=btc_mode,
                skip_if_fresh=True,
            )
        except Exception:
            pass
        _MATERIALIZER_STOP.wait(max(5, int(interval_sec or 30)))


def start_materializer_loop(tickers: Optional[list[str]] = None, interval_sec: int = 30,
                            feed: str = 'sip', quote_mode: str = 'per-second',
                            btc_mode: str = 'bars') -> None:
    global _MATERIALIZER_THREAD
    with _LOCK:
        if _MATERIALIZER_THREAD and _MATERIALIZER_THREAD.is_alive():
            return
        _MATERIALIZER_STOP.clear()
        safe_tickers = [str(t).upper() for t in (tickers or ['CLSK', 'MARA', 'RIOT'])]
        _MATERIALIZER_THREAD = threading.Thread(
            target=_materializer_loop,
            args=(safe_tickers, interval_sec, feed, quote_mode, btc_mode),
            name='live-step2-materializer',
            daemon=True,
        )
        _MATERIALIZER_THREAD.start()


def record(kind: str, symbol: str, ts_ms: int, row: dict) -> None:
    global _DROPPED
    if not symbol or not ts_ms or not row:
        return
    start()
    event = {
        'kind': kind,
        'symbol': symbol,
        't': int(ts_ms),
        'row': row,
        'recorded_at_ms': int(time.time() * 1000),
    }
    try:
        _QUEUE.put_nowait(event)
    except queue.Full:
        _DROPPED += 1


def record_stock_trade(symbol: str, raw: dict, ts_ms: int) -> None:
    row = {
        't': int(ts_ms),
        'p': raw.get('p'),
        's': raw.get('s'),
        'x': raw.get('x'),
        'c': raw.get('c') or [],
        'z': raw.get('z'),
        'i': raw.get('i'),
    }
    record('stock_trade', symbol, int(ts_ms), row)


def record_stock_quote(symbol: str, raw: dict, ts_ms: int) -> None:
    row = {
        't': int(ts_ms),
        'bp': raw.get('bp'),
        'ap': raw.get('ap'),
        'bs': raw.get('bs'),
        'as': raw.get('as'),
        'bx': raw.get('bx'),
        'ax': raw.get('ax'),
        'c': raw.get('c') or [],
        'z': raw.get('z'),
    }
    record('stock_quote', symbol, int(ts_ms), row)


def record_btc_synth(symbol: str, price: float, ts_ms: int) -> None:
    row = {'t': int(ts_ms), 'p': price, 's': 0}
    record('btc_synth_trade', symbol, int(ts_ms), row)


def _read_prepared(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        return json.load(f)


def _write_prepared(path: str, rows: list[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f'{path}.{os.getpid()}.{int(time.time() * 1000)}.tmp'
    with gzip.open(tmp, 'wt', encoding='utf-8') as f:
        json.dump(rows, f, separators=(',', ':'))
    last_error = None
    for attempt in range(10):
        try:
            os.replace(tmp, path)
            return
        except PermissionError as exc:
            last_error = exc
            time.sleep(0.25 * (attempt + 1))
    try:
        os.remove(tmp)
    except OSError:
        pass
    if last_error:
        raise last_error


def _manifest_path(prepared_path: str) -> str:
    if prepared_path.endswith('.events.json.gz'):
        return prepared_path[:-len('.events.json.gz')] + '.manifest.json'
    if prepared_path.endswith('.json'):
        return prepared_path
    return prepared_path + '.manifest.json'


def _source_checkpoint_path(prepared_path: str) -> str:
    if prepared_path.endswith('.events.json.gz'):
        return prepared_path[:-len('.events.json.gz')] + '.source_checkpoint.json'
    return prepared_path + '.source_checkpoint.json'


def _summary_from_manifest(manifest: dict) -> dict:
    return {
        'rows': int(manifest.get('rows') or 0),
        'by_kind': manifest.get('by_kind') or {},
        'by_symbol': manifest.get('by_symbol') or {},
        'by_symbol_kind': manifest.get('by_symbol_kind') or {},
        'min_ts_ms': int(manifest.get('min_ts_ms') or 0),
        'max_ts_ms': int(manifest.get('max_ts_ms') or 0),
        'min_ct': manifest.get('min_ct'),
        'max_ct': manifest.get('max_ct'),
    }


def _write_json(path: str, payload: dict) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f'{path}.{os.getpid()}.{int(time.time() * 1000)}.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    os.replace(tmp, path)
    return os.path.abspath(path)


def _write_source_checkpoint(prepared_path: str, day: str, jsonl_size: int,
                             jsonl_mtime: float, session_only: bool) -> str:
    return _write_json(_source_checkpoint_path(prepared_path), {
        'day': day,
        'prepared_path': os.path.abspath(prepared_path),
        'jsonl_path': os.path.abspath(_jsonl_path(day)),
        'jsonl_size': int(jsonl_size or 0),
        'jsonl_mtime': float(jsonl_mtime or 0.0),
        'session_only': bool(session_only),
        'updated_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
    })


def _event_key(event: dict) -> tuple:
    row = event.get('row') or {}
    return (
        event.get('kind'),
        event.get('symbol'),
        int(event.get('t') or row.get('t') or 0),
        row.get('i'),
        row.get('p'),
        row.get('bp'),
        row.get('ap'),
    )


def _load_live_events_from_jsonl(day: str, tickers: list[str], session_only: bool = True,
                                 start_offset: int = 0) -> tuple[list[dict], int]:
    path = _jsonl_path(day)
    if not os.path.exists(path):
        return [], 0
    allowed = set(tickers) | {BTC_SYMBOL}
    rows: list[dict] = []
    with open(path, 'rb') as f:
        if start_offset > 0:
            f.seek(int(start_offset))
        for raw_line in f:
            try:
                line = raw_line.decode('utf-8').strip()
            except Exception:
                continue
            if not line:
                continue
            try:
                event = json.loads(line)
            except Exception:
                continue
            if event.get('symbol') not in allowed:
                continue
            kind = event.get('kind')
            if kind not in ('stock_trade', 'stock_quote', 'btc_synth_trade'):
                continue
            ts_ms = int(event.get('t') or 0)
            if session_only and not _in_replay_session(ts_ms):
                continue
            rows.append({
                'kind': kind,
                'symbol': event.get('symbol'),
                't': ts_ms,
                'row': event.get('row') or {},
            })
        end_offset = f.tell()
    return rows, int(end_offset or 0)


def load_live_events(day: str, tickers: list[str], session_only: bool = True) -> list[dict]:
    rows, _offset = _load_live_events_from_jsonl(day, tickers, session_only=session_only)
    return rows


def _summarize_events(rows: list[dict]) -> dict:
    by_kind: dict[str, int] = {}
    by_symbol: dict[str, int] = {}
    by_symbol_kind: dict[str, int] = {}
    min_ts = 0
    max_ts = 0
    for event in rows:
        kind = str(event.get('kind') or 'unknown')
        symbol = str(event.get('symbol') or 'unknown')
        ts = int(event.get('t') or (event.get('row') or {}).get('t') or 0)
        by_kind[kind] = by_kind.get(kind, 0) + 1
        by_symbol[symbol] = by_symbol.get(symbol, 0) + 1
        key = f'{symbol}:{kind}'
        by_symbol_kind[key] = by_symbol_kind.get(key, 0) + 1
        if ts:
            min_ts = ts if not min_ts else min(min_ts, ts)
            max_ts = max(max_ts, ts)
    return {
        'rows': len(rows),
        'by_kind': dict(sorted(by_kind.items())),
        'by_symbol': dict(sorted(by_symbol.items())),
        'by_symbol_kind': dict(sorted(by_symbol_kind.items())),
        'min_ts_ms': min_ts,
        'max_ts_ms': max_ts,
        'min_ct': datetime.fromtimestamp(min_ts / 1000.0, CT).isoformat() if min_ts else None,
        'max_ct': datetime.fromtimestamp(max_ts / 1000.0, CT).isoformat() if max_ts else None,
    }


def _write_manifest(prepared_path: str, payload: dict) -> str:
    path = _manifest_path(prepared_path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f'{path}.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    os.replace(tmp, path)
    return path


def materialize(day: str, tickers: list[str], feed: str = 'sip', quote_mode: str = 'per-second',
                btc_mode: str = 'bars', prepared_cache_dir: str = DEFAULT_PREPARED_DIR,
                include_existing: bool = True, session_only: bool = True,
                skip_if_fresh: bool = False) -> dict:
    tickers = [t.upper() for t in tickers]
    path = _prepared_path(day, tickers, feed, quote_mode, btc_mode, prepared_cache_dir)
    live_tail_seed = None
    if skip_if_fresh and include_existing and os.path.exists(path):
        source_mtime = _live_sources_mtime(day)
        prepared_mtime = os.path.getmtime(path)
        prepared_manifest_path = _manifest_path(path)
        prepared_manifest = _read_manifest_json(prepared_manifest_path)
        manifest_ready = bool(os.path.exists(prepared_manifest_path) and prepared_manifest)
        incremental = None
        incremental_manifest = (
            os.path.join(incremental_market_store.DEFAULT_ROOT, day, 'manifest.json')
            if incremental_market_store is not None else ''
        )
        incremental_fresh = bool(
            incremental_manifest
            and os.path.exists(incremental_manifest)
            and os.path.getmtime(incremental_manifest) >= prepared_mtime
        )
        if manifest_ready and source_mtime and source_mtime <= prepared_mtime and incremental_fresh:
            summary = _summary_from_manifest(prepared_manifest)
            return {
                'prepared_path': os.path.abspath(path),
                'manifest_path': os.path.abspath(_manifest_path(path)),
                'incremental_market_store': _read_manifest_json(incremental_manifest),
                'source': 'live_intraday',
                'canonical': False,
                'existing_rows': int(prepared_manifest.get('rows') or 0),
                'live_rows': 0,
                'skipped_fresh_materialize': True,
                'source_mtime': source_mtime,
                'prepared_mtime': prepared_mtime,
                **summary,
            }
        if source_mtime and source_mtime <= prepared_mtime and incremental_market_store is not None:
            try:
                incremental = incremental_market_store.update_from_prepared(path, day)
                prepared_manifest = _read_manifest_json(_manifest_path(path))
                if prepared_manifest:
                    summary = _summary_from_manifest(prepared_manifest)
                else:
                    summary = _summarize_events(_read_prepared(path))
                return {
                    'prepared_path': os.path.abspath(path),
                    'manifest_path': os.path.abspath(_manifest_path(path)),
                    'incremental_market_store': incremental,
                    'source': 'live_intraday',
                    'canonical': False,
                    'existing_rows': int(summary.get('rows') or 0),
                    'live_rows': 0,
                    'skipped_fresh_materialize': True,
                    'source_mtime': source_mtime,
                    'prepared_mtime': prepared_mtime,
                    **summary,
                }
            except Exception:
                pass
        if manifest_ready and not _live_index_paths(day):
            jsonl = _jsonl_path(day)
            checkpoint = _read_manifest_json(_source_checkpoint_path(path))
            prior_size = int(checkpoint.get('jsonl_size') or 0)
            current_size = os.path.getsize(jsonl) if os.path.exists(jsonl) else 0
            if prior_size > 0 and current_size >= prior_size:
                tail_live, end_offset = _load_live_events_from_jsonl(
                    day,
                    tickers,
                    session_only=session_only,
                    start_offset=prior_size,
                )
                if not tail_live:
                    _write_source_checkpoint(
                        path,
                        day,
                        end_offset,
                        os.path.getmtime(jsonl) if os.path.exists(jsonl) else 0.0,
                        session_only,
                    )
                    summary = _summary_from_manifest(prepared_manifest)
                    return {
                        'prepared_path': os.path.abspath(path),
                        'manifest_path': os.path.abspath(_manifest_path(path)),
                        'incremental_market_store': _read_manifest_json(incremental_manifest),
                        'source': 'live_intraday',
                        'canonical': False,
                        'existing_rows': int(prepared_manifest.get('rows') or 0),
                        'live_rows': 0,
                        'skipped_fresh_materialize': True,
                        'jsonl_tail_checked': True,
                        'jsonl_tail_start': prior_size,
                        'jsonl_tail_end': end_offset,
                        'source_mtime': source_mtime,
                        'prepared_mtime': prepared_mtime,
                        **summary,
                    }
                live_tail_seed = tail_live
            else:
                live_tail_seed = None
        else:
            live_tail_seed = None
    existing = _read_prepared(path) if include_existing else []
    index_available = bool(_live_index_paths(day))
    if index_available:
        live = []
    elif live_tail_seed is not None:
        live = live_tail_seed
    else:
        live, _jsonl_end_offset = _load_live_events_from_jsonl(day, tickers, session_only=session_only)
    if incremental_market_store is not None:
        try:
            indexed = incremental_market_store.load_live_index_events(day, tickers, session_only=session_only)
            if indexed:
                live = live + indexed
        except Exception:
            pass
    by_key: dict[tuple, dict] = {}
    for event in existing + live:
        if event.get('kind') == 'btc_synth_trade' and btc_mode == 'off':
            continue
        by_key[_event_key(event)] = event
    rows = sorted(by_key.values(), key=lambda e: int(e.get('t') or 0))
    _write_prepared(path, rows)
    summary = _summarize_events(rows)
    source = 'live_intraday' if os.path.abspath(prepared_cache_dir) == os.path.abspath(DEFAULT_PREPARED_DIR) else 'custom'
    manifest = {
        'source': source,
        'canonical': False,
        'day': day,
        'tickers': tickers,
        'feed': feed,
        'quote_mode': quote_mode,
        'btc_mode': btc_mode,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'prepared_path': os.path.abspath(path),
        'live_event_jsonl': os.path.abspath(_jsonl_path(day)),
        'session_only': bool(session_only),
        'existing_rows_before_merge': len(existing),
        'live_rows_loaded': len(live),
        **summary,
    }
    manifest_path = _write_manifest(path, manifest)
    incremental = None
    if incremental_market_store is not None:
        try:
            incremental = incremental_market_store.update_from_prepared(path, day)
        except Exception as exc:
            incremental = {'error': repr(exc)}
    if not index_available:
        jsonl = _jsonl_path(day)
        try:
            _write_source_checkpoint(
                path,
                day,
                os.path.getsize(jsonl) if os.path.exists(jsonl) else 0,
                os.path.getmtime(jsonl) if os.path.exists(jsonl) else 0.0,
                session_only,
            )
        except Exception:
            pass
    return {
        'prepared_path': os.path.abspath(path),
        'manifest_path': os.path.abspath(manifest_path),
        'incremental_market_store': incremental,
        'source': source,
        'canonical': False,
        'existing_rows': len(existing),
        'live_rows': len(live),
        **summary,
    }


def _read_manifest_json(path: str) -> dict:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def write_prepared_manifest(day: str, tickers: list[str], source: str, canonical: bool,
                            feed: str = 'sip', quote_mode: str = 'per-second',
                            btc_mode: str = 'bars',
                            prepared_cache_dir: str = CANONICAL_PREPARED_DIR) -> dict:
    tickers = [t.upper() for t in tickers]
    path = _prepared_path(day, tickers, feed, quote_mode, btc_mode, prepared_cache_dir)
    rows = _read_prepared(path)
    payload = {
        'source': source,
        'canonical': bool(canonical),
        'day': day,
        'tickers': tickers,
        'feed': feed,
        'quote_mode': quote_mode,
        'btc_mode': btc_mode,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'prepared_path': os.path.abspath(path),
        'prepared_exists': os.path.exists(path),
        **_summarize_events(rows),
    }
    manifest_path = _write_manifest(path, payload)
    payload['manifest_path'] = os.path.abspath(manifest_path)
    return payload


def compare_prepared(day: str, tickers: list[str], feed: str = 'sip',
                     quote_mode: str = 'per-second', btc_mode: str = 'bars',
                     intraday_prepared_dir: str = DEFAULT_PREPARED_DIR,
                     canonical_prepared_dir: str = CANONICAL_PREPARED_DIR) -> dict:
    tickers = [t.upper() for t in tickers]
    intraday_path = _prepared_path(day, tickers, feed, quote_mode, btc_mode, intraday_prepared_dir)
    canonical_path = _prepared_path(day, tickers, feed, quote_mode, btc_mode, canonical_prepared_dir)
    intraday = _read_prepared(intraday_path)
    canonical = _read_prepared(canonical_path)
    intraday_keys = {_event_key(event) for event in intraday}
    canonical_keys = {_event_key(event) for event in canonical}
    missing_from_intraday = canonical_keys - intraday_keys
    live_only = intraday_keys - canonical_keys
    payload = {
        'source': 'intraday_vs_canonical_compare',
        'canonical': False,
        'day': day,
        'tickers': tickers,
        'feed': feed,
        'quote_mode': quote_mode,
        'btc_mode': btc_mode,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'intraday_path': os.path.abspath(intraday_path),
        'canonical_path': os.path.abspath(canonical_path),
        'intraday_exists': os.path.exists(intraday_path),
        'canonical_exists': os.path.exists(canonical_path),
        'intraday': _summarize_events(intraday),
        'canonical_summary': _summarize_events(canonical),
        'overlap_rows': len(intraday_keys & canonical_keys),
        'missing_from_intraday_rows': len(missing_from_intraday),
        'live_only_rows': len(live_only),
    }
    compare_path = os.path.join(
        HERE, 'postmortem', 'step2_freshness',
        f'intraday_vs_canonical_{day}.json',
    )
    _write_manifest(compare_path, payload)
    payload['compare_path'] = os.path.abspath(compare_path)
    return payload


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Materialize or compare live Step 2 intraday tapes.')
    ap.add_argument('command', choices=['materialize', 'compare', 'manifest', 'status'])
    ap.add_argument('--day', default=datetime.now(CT).date().isoformat())
    ap.add_argument('--tickers', nargs='+', default=['CLSK', 'MARA', 'RIOT'])
    ap.add_argument('--feed', default='sip')
    ap.add_argument('--quote-mode', default='per-second')
    ap.add_argument('--btc-mode', default='bars')
    ap.add_argument('--prepared-cache-dir', default=DEFAULT_PREPARED_DIR)
    ap.add_argument('--intraday-prepared-dir', default=DEFAULT_PREPARED_DIR)
    ap.add_argument('--canonical-prepared-dir', default=CANONICAL_PREPARED_DIR)
    ap.add_argument('--no-existing', action='store_true')
    ap.add_argument('--include-after-hours', action='store_true')
    ap.add_argument('--skip-if-fresh', action='store_true',
                    help='Return existing prepared/partition manifests without rereading tapes when inputs are unchanged.')
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == 'status':
        print(json.dumps(status(), indent=2, sort_keys=True))
        return 0
    if args.command == 'compare':
        payload = compare_prepared(
            day=args.day,
            tickers=args.tickers,
            feed=args.feed,
            quote_mode=args.quote_mode,
            btc_mode=args.btc_mode,
            intraday_prepared_dir=args.intraday_prepared_dir,
            canonical_prepared_dir=args.canonical_prepared_dir,
        )
    elif args.command == 'manifest':
        payload = write_prepared_manifest(
            day=args.day,
            tickers=args.tickers,
            source='alpaca_historical',
            canonical=True,
            feed=args.feed,
            quote_mode=args.quote_mode,
            btc_mode=args.btc_mode,
            prepared_cache_dir=args.canonical_prepared_dir,
        )
    else:
        payload = materialize(
            day=args.day,
            tickers=args.tickers,
            feed=args.feed,
            quote_mode=args.quote_mode,
            btc_mode=args.btc_mode,
            prepared_cache_dir=args.prepared_cache_dir,
            include_existing=not args.no_existing,
            session_only=not args.include_after_hours,
            skip_if_fresh=args.skip_if_fresh,
        )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
