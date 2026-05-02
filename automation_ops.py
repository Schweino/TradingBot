from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from typing import Optional
from urllib.request import urlopen

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo


HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, 'postmortem')
CT = ZoneInfo('America/Chicago')
PID_PATH = os.path.join(OUT_DIR, 'live_monitor.pid')
RUN_LEDGER_PATH = os.path.join(OUT_DIR, 'automation_run_ledger.json')
APP_URL = 'http://127.0.0.1:5000/mock/status'


def _today() -> str:
    return datetime.now(CT).date().isoformat()


def _run(args: list[str], timeout: int = 120) -> dict:
    proc = subprocess.run(
        args,
        cwd=HERE,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
    )
    return {
        'command': args,
        'returncode': proc.returncode,
        'ok': proc.returncode == 0,
        'output': proc.stdout[-6000:],
    }


def _read_json(path: str, default=None):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return default


def _write_json(path: str, payload: dict) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, default=str)
    return path


def _run_key(phase: str, day: str) -> str:
    return f'{phase}:{day}'


def _read_run_ledger() -> dict:
    return _read_json(RUN_LEDGER_PATH, {}) or {}


def _write_run_ledger(ledger: dict) -> str:
    return _write_json(RUN_LEDGER_PATH, ledger)


def _ledger_done(phase: str, day: str) -> Optional[dict]:
    entry = _read_run_ledger().get(_run_key(phase, day))
    if isinstance(entry, dict) and entry.get('ok'):
        return entry
    return None


def _mark_ledger_done(phase: str, day: str, artifact: Optional[str], steps: list[dict]) -> None:
    ledger = _read_run_ledger()
    ledger[_run_key(phase, day)] = {
        'phase': phase,
        'day': day,
        'ok': True,
        'artifact': artifact,
        'completed_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'step_count': len(steps),
    }
    _write_run_ledger(ledger)


def _status_reachable(timeout: int = 5) -> bool:
    try:
        with urlopen(APP_URL, timeout=timeout) as resp:
            return resp.status == 200
    except Exception:
        return False


def ensure_app_running() -> dict:
    if _status_reachable(timeout=3):
        return {'ok': True, 'already_running': True, 'url': APP_URL}
    pythonw = os.path.join(os.path.dirname(sys.executable), 'pythonw.exe')
    exe = pythonw if os.path.exists(pythonw) else sys.executable
    creationflags = getattr(subprocess, 'CREATE_NO_WINDOW', 0)
    proc = subprocess.Popen(
        [exe, os.path.join(HERE, 'app.py')],
        cwd=HERE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=creationflags,
    )
    for _ in range(12):
        if _status_reachable(timeout=2):
            return {'ok': True, 'already_running': False, 'pid': proc.pid, 'url': APP_URL}
    return {'ok': False, 'already_running': False, 'pid': proc.pid, 'url': APP_URL, 'error': 'app_not_reachable_after_start'}


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _live_monitor_checkpoint(day: str) -> str:
    return os.path.join(OUT_DIR, f'session_checkpoint_{day}_auto_loop.json')


def _live_monitor_healthy(day: str, pid: int, max_age_sec: int = 180) -> dict:
    checkpoint = _live_monitor_checkpoint(day)
    alive = _pid_alive(pid)
    fresh = False
    age_sec = None
    if os.path.exists(checkpoint):
        age_sec = max(0, int(time.time() - os.path.getmtime(checkpoint)))
        fresh = age_sec <= max_age_sec
    return {
        'alive': alive,
        'fresh_checkpoint': fresh,
        'checkpoint_age_sec': age_sec,
        'checkpoint': checkpoint,
        'healthy': bool(alive and fresh),
    }


def start_live_monitor(day: str, interval_sec: int = 30) -> dict:
    os.makedirs(OUT_DIR, exist_ok=True)
    old = _read_json(PID_PATH, {}) or {}
    old_pid = old.get('pid')
    if old_pid:
        try:
            old_pid_i = int(old_pid)
            health = _live_monitor_healthy(day, old_pid_i)
            if health.get('healthy'):
                return {'ok': True, 'already_running': True, 'pid': old_pid_i,
                        'pid_file': PID_PATH, 'health': health}
        except Exception:
            pass
    out_path = os.path.join(OUT_DIR, f'live_monitor_loop_{day}.log')
    log = open(out_path, 'a', encoding='utf-8')
    creationflags = getattr(subprocess, 'CREATE_NO_WINDOW', 0)
    proc = subprocess.Popen(
        [
            sys.executable,
            os.path.join(HERE, 'live_monitor.py'),
            day,
            '--loop',
            '--interval-sec',
            str(interval_sec),
            '--checkpoint',
            '--checkpoint-label',
            'auto_loop',
        ],
        cwd=HERE,
        stdout=log,
        stderr=subprocess.STDOUT,
        creationflags=creationflags,
    )
    payload = {
        'ok': True,
        'already_running': False,
        'pid': proc.pid,
        'pid_file': PID_PATH,
        'log_file': out_path,
        'started_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'day': day,
        'interval_sec': interval_sec,
    }
    _write_json(PID_PATH, payload)
    return payload


def run_phase(phase: str, day: Optional[str] = None, force: bool = False) -> dict:
    day = day or _today()
    steps = []
    if phase == 'post-close' and not force:
        existing = _ledger_done(phase, day)
        if existing:
            payload = {
                'phase': phase,
                'day': day,
                'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
                'ok': True,
                'skipped_existing_success': True,
                'existing_run': existing,
                'steps': [],
                'now_status': {},
                'daily_review_gate': {},
            }
            path = os.path.join(OUT_DIR, f'automation_{phase}_{day}.json')
            payload['artifact'] = _write_json(path, payload)
            return payload
    if phase == 'pre-open':
        steps.append({'command': ['ensure_app_running'], **ensure_app_running()})
        steps.append(_run([sys.executable, 'smoke_check.py', '--mode', 'no-surprises'], timeout=180))
        steps.append(_run([sys.executable, 'weekend_readiness.py', day], timeout=180))
        steps.append(_run([sys.executable, 'monday_ops.py', 'ready', day], timeout=120))
        steps.append({'command': ['start_live_monitor'], **start_live_monitor(day)})
    elif phase == 'start-monitor':
        steps.append({'command': ['ensure_app_running'], **ensure_app_running()})
        steps.append({'command': ['start_live_monitor'], **start_live_monitor(day)})
    elif phase == 'post-open':
        steps.append({'command': ['ensure_app_running'], **ensure_app_running()})
        steps.append(_run([sys.executable, 'smoke_check.py', '--mode', 'post-open'], timeout=120))
        steps.append(_run([sys.executable, 'live_monitor.py', day, '--checkpoint', '--checkpoint-label', 'post_open_auto'], timeout=120))
    elif phase == 'intraday':
        steps.append({'command': ['ensure_app_running'], **ensure_app_running()})
        steps.append(_run([sys.executable, 'live_monitor.py', day, '--checkpoint', '--checkpoint-label', 'intraday_auto'], timeout=120))
    elif phase == 'pre-flat':
        steps.append({'command': ['ensure_app_running'], **ensure_app_running()})
        steps.append(_run([sys.executable, 'live_monitor.py', day, '--checkpoint', '--checkpoint-label', 'pre_flat_auto'], timeout=120))
    elif phase == 'post-close':
        steps.append({'command': ['ensure_app_running'], **ensure_app_running()})
        steps.append(_run([sys.executable, 'monday_close_packet.py', day, '--write-gdoc'], timeout=600))
        steps.append(_run([sys.executable, 'ops.py', 'context', day], timeout=120))
        steps.append(_run([sys.executable, 'smoke_check.py', '--mode', 'post-market'], timeout=180))
    else:
        raise ValueError(f'unknown phase: {phase}')
    now_status = _read_json(os.path.join(OUT_DIR, 'NOW_STATUS.json'), {}) or {}
    gate = _read_json(os.path.join(OUT_DIR, f'daily_review_gate_{day}.json'), {}) or {}
    payload = {
        'phase': phase,
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'ok': all(bool(s.get('ok')) for s in steps),
        'steps': steps,
        'now_status': {
            'state': now_status.get('state'),
            'safe': now_status.get('safe'),
            'review_gate': now_status.get('review_gate'),
            'alerts': now_status.get('alerts'),
        },
        'daily_review_gate': {
            'verdict': gate.get('verdict'),
            'checks_passed': gate.get('checks_passed'),
            'checks_total': gate.get('checks_total'),
        },
    }
    path = os.path.join(OUT_DIR, f'automation_{phase}_{day}.json')
    payload['artifact'] = _write_json(path, payload)
    if phase == 'post-close' and payload.get('ok'):
        _mark_ledger_done(phase, day, payload.get('artifact'), steps)
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description='Stable scheduled automation entrypoint.')
    ap.add_argument('phase', choices=('pre-open', 'start-monitor', 'post-open', 'intraday', 'pre-flat', 'post-close'))
    ap.add_argument('day', nargs='?', default=None)
    ap.add_argument('--json', action='store_true')
    ap.add_argument('--force', action='store_true', help='Allow rerunning a mutating/idempotent phase such as post-close.')
    args = ap.parse_args()
    payload = run_phase(args.phase, args.day, force=args.force)
    if args.json:
        print(json.dumps(payload, indent=2, default=str))
    else:
        print(f"automation phase={payload['phase']} day={payload['day']} ok={payload['ok']}")
        print(payload['artifact'])
        for step in payload['steps']:
            print(f"{'OK' if step.get('ok') else 'FAIL'} {step.get('command')}")
    return 0 if payload.get('ok') else 1


if __name__ == '__main__':
    raise SystemExit(main())
