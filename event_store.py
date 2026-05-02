from __future__ import annotations

import json
import os
import sqlite3
import time
from datetime import datetime
from typing import Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore


HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(HERE, 'postmortem', 'trading_events.sqlite')
CT = ZoneInfo('America/Chicago')


def _conn():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=5)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    conn.execute(
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
    conn.execute('CREATE INDEX IF NOT EXISTS idx_events_day_kind ON events(day, kind)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_events_trade_id ON events(trade_id)')
    return conn


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
    with _conn() as conn:
        conn.execute(
            'INSERT INTO events(ts, day, kind, symbol, trade_id, payload_json) VALUES (?, ?, ?, ?, ?, ?)',
            row,
        )


def event_counts(day: Optional[str] = None) -> dict:
    day = day or datetime.now(CT).date().isoformat()
    with _conn() as conn:
        rows = conn.execute(
            'SELECT kind, COUNT(*) FROM events WHERE day = ? GROUP BY kind ORDER BY kind',
            (day,),
        ).fetchall()
    return {kind: count for kind, count in rows}
