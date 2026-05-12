"""Lightweight real-time parity alert sink."""
from __future__ import annotations

from output_paths import output_path

import hashlib
import json
import os
import time
from datetime import datetime
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore


HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = output_path('postmortem', 'realtime_parity_alerts')
CT = ZoneInfo('America/Chicago')
SCHEMA_VERSION = 1


def _day(ts: int | float | None = None) -> str:
    return datetime.fromtimestamp(float(ts or time.time()), CT).date().isoformat()


def _stable_hash(payload: Any) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(',', ':'), default=str)
    return hashlib.sha256(blob.encode('utf-8')).hexdigest()[:24]


def _write_json(path: str, payload: dict[str, Any]) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f'{path}.{os.getpid()}.{int(time.time() * 1000)}.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
    os.replace(tmp, path)
    return os.path.abspath(path)


def emit(severity: str, code: str, message: str, details: dict[str, Any] | None = None,
         ts: int | None = None, dedupe_key: str | None = None) -> dict[str, Any]:
    ts = int(ts or time.time())
    details = details or {}
    day = _day(ts)
    alert = {
        'schema_version': SCHEMA_VERSION,
        'created_at': ts,
        'created_at_ct': datetime.fromtimestamp(ts, CT).isoformat(timespec='seconds'),
        'day': day,
        'severity': severity,
        'code': code,
        'message': message,
        'details': details,
        'dedupe_key': dedupe_key or _stable_hash({'code': code, 'details': details}),
    }
    os.makedirs(OUT_DIR, exist_ok=True)
    history = os.path.join(OUT_DIR, f'parity_alerts_{day}.jsonl')
    with open(history, 'a', encoding='utf-8') as f:
        f.write(json.dumps(alert, sort_keys=True, separators=(',', ':'), default=str) + '\n')
    latest = os.path.join(OUT_DIR, 'PARITY_ALERT_LATEST.json')
    alert['history_path'] = os.path.abspath(history)
    alert['latest_path'] = _write_json(latest, alert)
    try:
        from event_store import record_event
        record_event('parity_alert', alert, symbol=(details or {}).get('ticker'), ts=ts)
    except Exception:
        pass
    return alert
