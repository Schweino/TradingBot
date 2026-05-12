"""Partitioned incremental market-data store for Step 2 rebuild planning."""
from __future__ import annotations

import gzip
import hashlib
import json
import os
from collections import defaultdict
from datetime import datetime, time as dt_time
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore


HERE = os.path.dirname(os.path.abspath(__file__))
CT = ZoneInfo('America/Chicago')
DEFAULT_ROOT = os.path.join(HERE, 'data_cache', 'incremental_market_store')
SCHEMA_VERSION = 1
DEFAULT_BUCKET_SEC = 300
SESSION_START_CT = dt_time(8, 30)
SESSION_END_CT = dt_time(15, 1)


def _read_gz_json(path: str) -> list[dict[str, Any]]:
    if not os.path.exists(path):
        return []
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        payload = json.load(f)
    return payload if isinstance(payload, list) else []


def _write_gz_json(path: str, payload: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f'{path}.tmp'
    with gzip.open(tmp, 'wt', encoding='utf-8') as f:
        json.dump(payload, f, sort_keys=True, separators=(',', ':'), default=str)
    os.replace(tmp, path)


def _write_json(path: str, payload: dict[str, Any]) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f'{path}.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
    os.replace(tmp, path)
    return os.path.abspath(path)


def _hash_rows(rows: list[dict[str, Any]]) -> str:
    blob = json.dumps(rows, sort_keys=True, separators=(',', ':'), default=str).encode('utf-8')
    return hashlib.sha256(blob).hexdigest()


def _bucket_start(ts_ms: int, bucket_sec: int) -> int:
    sec = int(ts_ms / 1000)
    return sec - (sec % bucket_sec)


def _bucket_label(bucket_start_sec: int) -> str:
    return datetime.fromtimestamp(bucket_start_sec, CT).strftime('%H%M%S')


def _event_key(event: dict[str, Any]) -> tuple[Any, ...]:
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


def _partition_path(root: str, day: str, bucket_start_sec: int) -> str:
    return os.path.join(root, day, 'partitions', f'{_bucket_label(bucket_start_sec)}.events.json.gz')


def _live_index_path(root: str, day: str, bucket_start_sec: int) -> str:
    return os.path.join(root, day, 'live_index', f'{_bucket_label(bucket_start_sec)}.events.jsonl')


def manifest_path(root: str, day: str) -> str:
    return os.path.join(root, day, 'manifest.json')


def append_live_event(event: dict[str, Any], root: str = DEFAULT_ROOT,
                      bucket_sec: int = DEFAULT_BUCKET_SEC) -> dict[str, Any]:
    ts_ms = int(event.get('t') or (event.get('row') or {}).get('t') or 0)
    day = event.get('day')
    if not day:
        day = datetime.fromtimestamp(ts_ms / 1000.0, CT).date().isoformat() if ts_ms else datetime.now(CT).date().isoformat()
    bucket = _bucket_start(ts_ms, bucket_sec) if ts_ms else _bucket_start(int(datetime.now(CT).timestamp() * 1000), bucket_sec)
    path = _live_index_path(root, str(day), bucket)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'a', encoding='utf-8') as f:
        f.write(json.dumps(event, sort_keys=True, separators=(',', ':'), default=str) + '\n')
    return {
        'day': str(day),
        'bucket_start': bucket,
        'bucket_ct': datetime.fromtimestamp(bucket, CT).isoformat(timespec='seconds'),
        'path': os.path.abspath(path),
    }


def load_live_index_events(day: str, tickers: list[str], root: str = DEFAULT_ROOT,
                           session_only: bool = True) -> list[dict[str, Any]]:
    allowed = set(str(t).upper() for t in tickers) | {'BTC/USD'}
    index_dir = os.path.join(root, day, 'live_index')
    if not os.path.isdir(index_dir):
        return []
    rows: list[dict[str, Any]] = []
    for name in sorted(os.listdir(index_dir)):
        if not name.endswith('.events.jsonl'):
            continue
        path = os.path.join(index_dir, name)
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except Exception:
                    continue
                symbol = event.get('symbol')
                if symbol not in allowed:
                    continue
                kind = event.get('kind')
                if kind not in ('stock_trade', 'stock_quote', 'btc_synth_trade'):
                    continue
                ts_ms = int(event.get('t') or 0)
                if session_only:
                    local = datetime.fromtimestamp(ts_ms / 1000.0, CT) if ts_ms else None
                    if local and not (SESSION_START_CT <= local.time() < SESSION_END_CT):
                        continue
                rows.append({
                    'kind': kind,
                    'symbol': symbol,
                    't': ts_ms,
                    'row': event.get('row') or {},
                })
    return rows


def update_from_prepared(prepared_path: str, day: str, root: str = DEFAULT_ROOT,
                         bucket_sec: int = DEFAULT_BUCKET_SEC) -> dict[str, Any]:
    rows = _read_gz_json(prepared_path)
    grouped: dict[int, dict[tuple[Any, ...], dict[str, Any]]] = defaultdict(dict)
    for event in rows:
        ts_ms = int(event.get('t') or (event.get('row') or {}).get('t') or 0)
        if not ts_ms:
            continue
        grouped[_bucket_start(ts_ms, bucket_sec)][_event_key(event)] = event
    partitions = []
    changed = []
    for bucket_start, keyed in sorted(grouped.items()):
        part_rows = sorted(keyed.values(), key=lambda row: int(row.get('t') or 0))
        path = _partition_path(root, day, bucket_start)
        new_hash = _hash_rows(part_rows)
        old_hash = None
        if os.path.exists(path):
            old_hash = _hash_rows(_read_gz_json(path))
        if old_hash != new_hash:
            _write_gz_json(path, part_rows)
            changed.append({
                'bucket_start': bucket_start,
                'bucket_ct': datetime.fromtimestamp(bucket_start, CT).isoformat(timespec='seconds'),
                'path': os.path.abspath(path),
                'old_hash': old_hash,
                'new_hash': new_hash,
                'rows': len(part_rows),
            })
        partitions.append({
            'bucket_start': bucket_start,
            'bucket_ct': datetime.fromtimestamp(bucket_start, CT).isoformat(timespec='seconds'),
            'path': os.path.abspath(path),
            'rows': len(part_rows),
            'hash': new_hash,
        })
    payload = {
        'schema_version': SCHEMA_VERSION,
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'prepared_path': os.path.abspath(prepared_path),
        'prepared_exists': os.path.exists(prepared_path),
        'bucket_sec': int(bucket_sec),
        'rows': len(rows),
        'partition_count': len(partitions),
        'changed_partition_count': len(changed),
        'changed_partitions': changed,
        'partitions': partitions,
        'deduction': (
            'Step 2 can use changed_partitions to rebuild only affected time buckets plus overlap, '
            'instead of invalidating the whole day when market data is appended or corrected.'
        ),
    }
    payload['manifest_path'] = _write_json(manifest_path(root, day), payload)
    return payload


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser(description='Update incremental market-data partitions from a prepared tape.')
    ap.add_argument('--prepared-path', required=True)
    ap.add_argument('--day', required=True)
    ap.add_argument('--root', default=DEFAULT_ROOT)
    ap.add_argument('--bucket-sec', type=int, default=DEFAULT_BUCKET_SEC)
    ap.add_argument('--json', action='store_true')
    args = ap.parse_args()
    payload = update_from_prepared(args.prepared_path, args.day, args.root, args.bucket_sec)
    print(json.dumps(payload if args.json else {
        'manifest_path': payload.get('manifest_path'),
        'changed_partition_count': payload.get('changed_partition_count'),
        'partition_count': payload.get('partition_count'),
    }, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
