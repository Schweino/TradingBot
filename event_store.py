from __future__ import annotations

from output_paths import output_path

import json
import os
import sqlite3
import threading
import time
from datetime import datetime
from typing import Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore


HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = output_path('postmortem', 'trading_events.sqlite')
CT = ZoneInfo('America/Chicago')
_CONN: Optional[sqlite3.Connection] = None
_CONN_LOCK = threading.RLock()
_SCHEMA_READY = False


def _conn():
    global _CONN, _SCHEMA_READY
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    if _CONN is None:
        _CONN = sqlite3.connect(DB_PATH, timeout=5, check_same_thread=False)
    if not _SCHEMA_READY:
        _CONN.execute('PRAGMA journal_mode=WAL')
        _CONN.execute('PRAGMA synchronous=NORMAL')
        _CONN.execute(
            '''
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                day TEXT NOT NULL,
                kind TEXT NOT NULL,
                symbol TEXT,
                trade_id TEXT,
                payload_json TEXT NOT NULL
            )
            '''
        )
        _CONN.execute('CREATE INDEX IF NOT EXISTS idx_events_day_kind ON events(day, kind)')
        _CONN.execute('CREATE INDEX IF NOT EXISTS idx_events_trade_id ON events(trade_id)')
        _CONN.commit()
        _SCHEMA_READY = True
    return _CONN


def record_event(kind: str, payload: dict, symbol: Optional[str] = None,
                 trade_id: Optional[str] = None, ts: Optional[int] = None) -> None:
    ts = int(ts or time.time())
    day = datetime.fromtimestamp(ts, CT).date().isoformat()
    row = (
        ts,
        day,
        str(kind),
        symbol,
        trade_id,
        json.dumps(payload or {}, separators=(',', ':'), default=str),
    )
    with _CONN_LOCK:
        conn = _conn()
        conn.execute(
            'INSERT INTO events(ts, day, kind, symbol, trade_id, payload_json) VALUES (?, ?, ?, ?, ?, ?)',
            row,
        )
        conn.commit()


def event_counts(day: Optional[str] = None) -> dict:
    day = day or datetime.now(CT).date().isoformat()
    with _CONN_LOCK:
        conn = _conn()
        rows = conn.execute(
            'SELECT kind, COUNT(*) FROM events WHERE day = ? GROUP BY kind ORDER BY kind',
            (day,),
        ).fetchall()
    return {kind: count for kind, count in rows}
