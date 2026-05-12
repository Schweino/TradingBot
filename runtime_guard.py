from __future__ import annotations

import glob
import json
import os
import time
from datetime import datetime
from typing import Optional
from urllib.request import urlopen

try:
    import msvcrt  # type: ignore
except Exception:  # pragma: no cover - non-Windows fallback
    msvcrt = None

try:
    import fcntl  # type: ignore
except Exception:  # pragma: no cover - Windows fallback
    fcntl = None

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore


HERE = os.path.dirname(os.path.abspath(__file__))
POSTMORTEM_DIR = os.path.join(HERE, 'postmortem')
RUNTIME_DIR = os.path.join(POSTMORTEM_DIR, 'runtime')
APP_LOCK_PATH = os.path.join(RUNTIME_DIR, 'app.lock')
CT = ZoneInfo('America/Chicago')


def status_reachable(url: str = 'http://127.0.0.1:5000/mock/status',
                     timeout: int = 3) -> bool:
    try:
        with urlopen(url, timeout=timeout) as resp:
            return resp.status == 200
    except Exception:
        return False


class SingleInstanceLock:
    """Cross-process lock so only one trading app owns the runtime."""

    def __init__(self, path: str = APP_LOCK_PATH):
        self.path = path
        self._fh = None
        self.acquired = False

    def acquire(self) -> bool:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._fh = open(self.path, 'a+', encoding='utf-8')
        self._fh.seek(0)
        try:
            if msvcrt is not None:
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_NBLCK, 1)
            elif fcntl is not None:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self._fh.close()
            self._fh = None
            self.acquired = False
            return False
        self.acquired = True
        self._fh.seek(0)
        self._fh.truncate()
        self._fh.write(json.dumps({
            'pid': os.getpid(),
            'acquired_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
            'path': self.path,
        }, separators=(',', ':')))
        self._fh.flush()
        return True

    def release(self) -> None:
        if not self._fh:
            return
        try:
            self._fh.seek(0)
            if msvcrt is not None and self.acquired:
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
            elif fcntl is not None and self.acquired:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        finally:
            self._fh.close()
            self._fh = None
            self.acquired = False

    def __enter__(self) -> 'SingleInstanceLock':
        if not self.acquire():
            raise RuntimeError(f'app already running or lock held: {self.path}')
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


def rotate_file(path: str, max_bytes: int = 2_000_000,
                backup_count: int = 5) -> Optional[str]:
    try:
        if not os.path.exists(path) or os.path.getsize(path) < max_bytes:
            return None
        for idx in range(backup_count - 1, 0, -1):
            src = f'{path}.{idx}'
            dst = f'{path}.{idx + 1}'
            if os.path.exists(src):
                if idx + 1 > backup_count and os.path.exists(dst):
                    os.remove(dst)
                os.replace(src, dst)
        dst = f'{path}.1'
        if os.path.exists(dst):
            os.remove(dst)
        os.replace(path, dst)
        return dst
    except Exception:
        return None


def rotate_runtime_logs() -> dict:
    specs = [
        ('flask_boot.log', 1_500_000, 5),
        ('flask.log', 1_000_000, 3),
        ('proc_monitor.log', 1_500_000, 5),
        ('proc_crash.log', 1_500_000, 5),
        ('beta_refit.log', 500_000, 3),
        ('mock_trader.log', 2_000_000, 5),
        ('scalp.log', 2_000_000, 5),
    ]
    rotated = []
    for name, max_bytes, backups in specs:
        dst = rotate_file(os.path.join(HERE, name), max_bytes=max_bytes, backup_count=backups)
        if dst:
            rotated.append(dst)
    for pattern in ('_*.log', '*_check.log'):
        for path in glob.glob(os.path.join(HERE, pattern)):
            dst = rotate_file(path, max_bytes=750_000, backup_count=2)
            if dst:
                rotated.append(dst)
    payload = {
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'rotated': rotated,
        'count': len(rotated),
    }
    if rotated:
        os.makedirs(RUNTIME_DIR, exist_ok=True)
        day = datetime.now(CT).date().isoformat()
        with open(os.path.join(RUNTIME_DIR, f'log_rotation_{day}.jsonl'), 'a', encoding='utf-8') as f:
            f.write(json.dumps(payload, separators=(',', ':'), default=str) + '\n')
    return payload


def append_runtime_event(kind: str, payload: dict) -> None:
    os.makedirs(RUNTIME_DIR, exist_ok=True)
    now = int(time.time())
    day = datetime.fromtimestamp(now, CT).date().isoformat()
    row = {
        'ts': now,
        'created_at_ct': datetime.fromtimestamp(now, CT).isoformat(timespec='seconds'),
        'kind': kind,
        'payload': payload or {},
    }
    with open(os.path.join(RUNTIME_DIR, f'runtime_events_{day}.jsonl'), 'a', encoding='utf-8') as f:
        f.write(json.dumps(row, separators=(',', ':'), default=str) + '\n')
