from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import step2_warm_scorer
from output_paths import output_path


HERE = Path(__file__).resolve().parent
RUNTIME_DIR = Path(output_path('runtime'))
LOG_DIR = Path(output_path('logs'))
PID_PATH = RUNTIME_DIR / 'step2_warm_scorer.pid'
LOG_PATH = LOG_DIR / 'step2_warm_scorer.log'


def _write_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _read_pid() -> int:
    try:
        return int(PID_PATH.read_text(encoding='utf-8').strip())
    except Exception:
        return 0


def _status(host: str, port: int, timeout: float = 1.0) -> dict[str, Any]:
    pid = _read_pid()
    alive = _pid_alive(pid)
    if pid and not alive:
        try:
            PID_PATH.unlink()
        except OSError:
            pass
        pid = 0
    payload: dict[str, Any] = {
        'pid_path': str(PID_PATH.resolve()),
        'pid': pid or None,
        'pid_alive': alive if pid else False,
        'log_path': str(LOG_PATH.resolve()),
    }
    try:
        payload['service'] = step2_warm_scorer.client_get('/status', host, port, timeout=timeout)
        payload['ok'] = bool(payload['service'].get('ok'))
    except Exception as exc:
        payload['ok'] = False
        payload['service_error'] = repr(exc)
    return payload


def _start(args: argparse.Namespace) -> dict[str, Any]:
    manifest = args.manifest_path
    if not manifest and args.day:
        manifest = str(step2_warm_scorer.default_manifest_path(args.day, args.tickers, args.name))
    current = _status(args.host, args.port, timeout=0.5)
    if current.get('ok'):
        if manifest:
            try:
                current['reload'] = step2_warm_scorer.client_post(
                    '/reload',
                    {'manifest_path': str(Path(manifest).resolve())},
                    args.host,
                    args.port,
                    timeout=30.0,
                )
                current['ok'] = bool(current['reload'].get('ok'))
            except Exception as exc:
                current['ok'] = False
                current['reload_error'] = repr(exc)
        current['already_running'] = True
        return current
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        str(HERE / 'step2_warm_scorer.py'),
        'serve',
        '--host',
        args.host,
        '--port',
        str(int(args.port)),
    ]
    if manifest:
        cmd.extend(['--manifest-path', manifest])
    creationflags = getattr(subprocess, 'CREATE_NO_WINDOW', 0)
    with LOG_PATH.open('a', encoding='utf-8') as log:
        log.write(f'\n[{time.strftime("%Y-%m-%d %H:%M:%S")}] starting: {cmd}\n')
        log.flush()
        proc = subprocess.Popen(
            cmd,
            cwd=str(HERE),
            stdout=log,
            stderr=subprocess.STDOUT,
            creationflags=creationflags,
        )
    PID_PATH.write_text(str(proc.pid), encoding='utf-8')
    last = {}
    for _ in range(30):
        time.sleep(0.25)
        last = _status(args.host, args.port, timeout=0.5)
        if last.get('ok'):
            last['started'] = True
            last['command'] = cmd
            return last
        if proc.poll() is not None:
            break
    return {
        'ok': False,
        'started': False,
        'pid': proc.pid,
        'returncode': proc.poll(),
        'command': cmd,
        'last_status': last,
        'log_path': str(LOG_PATH.resolve()),
    }


def _stop(args: argparse.Namespace) -> dict[str, Any]:
    before = _status(args.host, args.port, timeout=0.5)
    service_stop = None
    try:
        service_stop = step2_warm_scorer.client_post('/shutdown', {}, args.host, args.port, timeout=1.0)
    except Exception as exc:
        service_stop = {'ok': False, 'error': repr(exc)}
    pid = _read_pid()
    for _ in range(60):
        if not _pid_alive(pid):
            break
        time.sleep(0.25)
    after = _status(args.host, args.port, timeout=0.5)
    stopped = (not after.get('ok')) and (not _pid_alive(pid) or bool((service_stop or {}).get('ok')))
    if stopped:
        try:
            PID_PATH.unlink()
        except OSError:
            pass
    return {
        'ok': stopped,
        'before': before,
        'service_stop': service_stop,
        'after': after,
    }


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Manage the persistent Step 2 warm scorer service.')
    sub = ap.add_subparsers(dest='command', required=True)
    for name in ('start', 'restart', 'stop', 'status'):
        p = sub.add_parser(name)
        p.add_argument('--host', default=step2_warm_scorer.DEFAULT_HOST)
        p.add_argument('--port', type=int, default=step2_warm_scorer.DEFAULT_PORT)
        p.add_argument('--day', default='')
        p.add_argument('--tickers', nargs='+', default=step2_warm_scorer.DEFAULT_TICKERS)
        p.add_argument('--name', default='')
        p.add_argument('--manifest-path', default='')
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == 'status':
        payload = _status(args.host, args.port)
    elif args.command == 'start':
        payload = _start(args)
    elif args.command == 'stop':
        payload = _stop(args)
    elif args.command == 'restart':
        stopped = _stop(args)
        started = _start(args)
        payload = {'ok': bool(started.get('ok')), 'stopped': stopped, 'started': started}
    else:
        raise ValueError(args.command)
    _write_json(payload)
    return 0 if payload.get('ok') else 1


if __name__ == '__main__':
    raise SystemExit(main())
