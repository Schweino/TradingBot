from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import worker_policy


HERE = Path(__file__).resolve().parent
CT = ZoneInfo('America/Chicago')
RUN_DIR = HERE / 'runtime' / 'runs'
LOG_DIR = HERE / 'logs' / 'runs'
HEARTBEAT_SEC = 5.0
WORKER_FLAGS = {
    '--workers',
    '--full-workers',
    '--model-workers',
    '--multi-profile-day-workers',
    '--target-total-workers',
}


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec='seconds')


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f'.{os.getpid()}.tmp')
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding='utf-8')
    os.replace(tmp, path)


def _read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return default


def _tail_text(path: Path, limit: int = 6000) -> str:
    try:
        with path.open('rb') as f:
            try:
                f.seek(-limit, os.SEEK_END)
            except OSError:
                f.seek(0)
            return f.read().decode('utf-8', errors='replace')
    except Exception:
        return ''


def _run_id(name: str | None = None) -> str:
    prefix = ''.join(ch if ch.isalnum() or ch in ('-', '_') else '_' for ch in str(name or 'run'))
    prefix = prefix.strip('_')[:50] or 'run'
    return f'{datetime.now(CT).strftime("%Y%m%d_%H%M%S")}_{prefix}_{uuid.uuid4().hex[:8]}'


def _process_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == 'nt':
        try:
            proc = subprocess.run(
                ['tasklist', '/FI', f'PID eq {pid}', '/FO', 'CSV', '/NH'],
                capture_output=True,
                text=True,
                timeout=5,
                creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0),
            )
            out = (proc.stdout or '').strip()
            return str(pid) in out and 'No tasks are running' not in out
        except Exception:
            return False
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _kill_tree(pid: int) -> dict[str, Any]:
    if pid <= 0:
        return {'ok': False, 'pid': pid, 'reason': 'invalid_pid'}
    if os.name == 'nt':
        proc = subprocess.run(
            ['taskkill', '/PID', str(pid), '/T', '/F'],
            capture_output=True,
            text=True,
            timeout=30,
            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0),
        )
        return {
            'ok': proc.returncode == 0,
            'pid': pid,
            'returncode': proc.returncode,
            'output': ((proc.stdout or '') + (proc.stderr or ''))[-4000:],
        }
    try:
        os.kill(pid, 15)
        return {'ok': True, 'pid': pid, 'signal': 15}
    except Exception as exc:
        return {'ok': False, 'pid': pid, 'error': repr(exc)}


def clamp_command_workers(command: list[str]) -> tuple[list[str], list[dict[str, Any]]]:
    effective = list(command)
    changes: list[dict[str, Any]] = []
    idx = 0
    while idx < len(effective) - 1:
        token = effective[idx]
        if token in WORKER_FLAGS:
            original = effective[idx + 1]
            clamped = worker_policy.clamp_workers(original)
            if str(original) != str(clamped):
                effective[idx + 1] = str(clamped)
                changes.append({'flag': token, 'original': original, 'clamped': clamped})
            idx += 2
            continue
        idx += 1
    return effective, changes


def run_and_track(
    command: list[str],
    *,
    cwd: str | os.PathLike[str] | None = None,
    timeout: int | float | None = None,
    name: str | None = None,
    workers: int | None = None,
    heartbeat_sec: float = HEARTBEAT_SEC,
) -> dict[str, Any]:
    effective_command, worker_changes = clamp_command_workers([str(part) for part in command])
    default_name = name or (Path(effective_command[1]).stem if len(effective_command) > 1 else Path(effective_command[0]).stem)
    run_id = _run_id(default_name)
    manifest_path = RUN_DIR / f'{run_id}.json'
    log_path = LOG_DIR / f'{run_id}.log'
    cwd_path = Path(cwd or HERE).resolve()
    worker_budget = worker_policy.clamp_workers(workers if workers is not None else None)
    env = os.environ.copy()
    env[worker_policy.WORKER_ENV_VAR] = str(worker_budget)
    payload: dict[str, Any] = {
        'schema_version': 1,
        'run_id': run_id,
        'name': default_name,
        'status': 'starting',
        'command': command,
        'effective_command': effective_command,
        'worker_policy': worker_policy.describe_policy(),
        'worker_budget': worker_budget,
        'worker_arg_changes': worker_changes,
        'cwd': str(cwd_path),
        'manifest_path': str(manifest_path.resolve()),
        'log_path': str(log_path.resolve()),
        'started_at_ct': _now_ct(),
        'heartbeat_at_ct': _now_ct(),
        'heartbeat_epoch': time.time(),
        'timeout_sec': timeout,
    }
    _write_json_atomic(manifest_path, payload)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    proc: subprocess.Popen[Any] | None = None
    timed_out = False
    kill_result: dict[str, Any] | None = None
    try:
        with log_path.open('a', encoding='utf-8', buffering=1) as log:
            log.write(json.dumps({'event': 'start', 'run_id': run_id, 'cmd': effective_command}, sort_keys=True) + '\n')
            proc = subprocess.Popen(
                effective_command,
                cwd=str(cwd_path),
                stdout=log,
                stderr=subprocess.STDOUT,
                text=True,
                env=env,
                creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0),
            )
            payload.update({'status': 'running', 'pid': proc.pid, 'heartbeat_at_ct': _now_ct(), 'heartbeat_epoch': time.time()})
            _write_json_atomic(manifest_path, payload)
            deadline = (time.time() + float(timeout)) if timeout else None
            while proc.poll() is None:
                if deadline and time.time() >= deadline:
                    timed_out = True
                    kill_result = _kill_tree(proc.pid)
                    break
                payload['heartbeat_at_ct'] = _now_ct()
                payload['heartbeat_epoch'] = time.time()
                payload['elapsed_sec'] = round(time.perf_counter() - started, 3)
                _write_json_atomic(manifest_path, payload)
                wait_for = max(0.5, float(heartbeat_sec or HEARTBEAT_SEC))
                if deadline:
                    wait_for = max(0.1, min(wait_for, deadline - time.time()))
                try:
                    proc.wait(timeout=wait_for)
                except subprocess.TimeoutExpired:
                    pass
            try:
                returncode = proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                kill_result = kill_result or _kill_tree(proc.pid)
                returncode = proc.poll()
            log.write(json.dumps({'event': 'finish', 'run_id': run_id, 'returncode': returncode}, sort_keys=True) + '\n')
    except Exception as exc:
        payload.update({
            'status': 'error',
            'error': repr(exc),
            'elapsed_sec': round(time.perf_counter() - started, 3),
            'heartbeat_at_ct': _now_ct(),
            'heartbeat_epoch': time.time(),
        })
        _write_json_atomic(manifest_path, payload)
        return {
            'command': command,
            'effective_command': effective_command,
            'returncode': None,
            'ok': False,
            'error': repr(exc),
            'output': _tail_text(log_path),
            'run_id': run_id,
            'manifest_path': str(manifest_path.resolve()),
            'log_path': str(log_path.resolve()),
        }

    returncode = proc.returncode if proc is not None else None
    status = 'timeout' if timed_out else ('ok' if returncode == 0 else 'failed')
    payload.update({
        'status': status,
        'returncode': returncode,
        'ok': returncode == 0 and not timed_out,
        'timed_out': timed_out,
        'kill_result': kill_result,
        'elapsed_sec': round(time.perf_counter() - started, 3),
        'heartbeat_at_ct': _now_ct(),
        'heartbeat_epoch': time.time(),
        'finished_at_ct': _now_ct(),
    })
    _write_json_atomic(manifest_path, payload)
    return {
        'command': command,
        'effective_command': effective_command,
        'returncode': returncode,
        'ok': bool(payload['ok']),
        'timeout_sec': timeout if timed_out else None,
        'timed_out': timed_out,
        'kill_result': kill_result,
        'elapsed_seconds': payload['elapsed_sec'],
        'output': _tail_text(log_path),
        'run_id': run_id,
        'manifest_path': str(manifest_path.resolve()),
        'log_path': str(log_path.resolve()),
        'worker_policy': payload['worker_policy'],
        'worker_arg_changes': worker_changes,
    }


def list_runs() -> list[dict[str, Any]]:
    rows = []
    for path in sorted(RUN_DIR.glob('*.json'), reverse=True):
        payload = _read_json(path, {}) or {}
        pid = int(payload.get('pid') or 0)
        alive = _process_alive(pid) if pid else False
        row = {
            'run_id': payload.get('run_id') or path.stem,
            'name': payload.get('name'),
            'status': payload.get('status'),
            'pid': pid or None,
            'pid_alive': alive,
            'started_at_ct': payload.get('started_at_ct'),
            'heartbeat_at_ct': payload.get('heartbeat_at_ct'),
            'elapsed_sec': payload.get('elapsed_sec'),
            'manifest_path': str(path.resolve()),
            'log_path': payload.get('log_path'),
        }
        rows.append(row)
    return rows


def cleanup_stale(*, kill: bool = False, stale_sec: int = 300) -> dict[str, Any]:
    now = time.time()
    updated = []
    killed = []
    for path in RUN_DIR.glob('*.json'):
        payload = _read_json(path, {}) or {}
        status = str(payload.get('status') or '')
        if status not in ('starting', 'running'):
            continue
        pid = int(payload.get('pid') or 0)
        alive = _process_alive(pid) if pid else False
        heartbeat_raw = payload.get('heartbeat_epoch')
        heartbeat_age = None
        if heartbeat_raw:
            try:
                heartbeat_age = now - float(heartbeat_raw)
            except Exception:
                heartbeat_age = None
        if heartbeat_age is None:
            try:
                heartbeat_age = now - path.stat().st_mtime
            except Exception:
                heartbeat_age = None
        stale = not alive or (heartbeat_age is not None and heartbeat_age > stale_sec)
        if not stale:
            continue
        if kill and alive:
            killed.append(_kill_tree(pid))
        payload.update({
            'status': 'stale_cleaned',
            'pid_alive_at_cleanup': alive,
            'cleanup_at_ct': _now_ct(),
        })
        _write_json_atomic(path, payload)
        updated.append(str(path.resolve()))
    return {'ok': True, 'updated': updated, 'killed': killed, 'count': len(updated)}


def stop_run(run_id: str) -> dict[str, Any]:
    path = RUN_DIR / f'{run_id}.json'
    payload = _read_json(path, {}) or {}
    pid = int(payload.get('pid') or 0)
    result = _kill_tree(pid)
    payload.update({
        'status': 'stopped',
        'stop_requested_at_ct': _now_ct(),
        'stop_result': result,
    })
    if path.exists():
        _write_json_atomic(path, payload)
    return {'ok': bool(result.get('ok')), 'run_id': run_id, 'pid': pid, 'result': result}


def main() -> int:
    ap = argparse.ArgumentParser(description='Supervise project-local heavy runs.')
    sub = ap.add_subparsers(dest='cmd', required=True)
    launch = sub.add_parser('launch')
    launch.add_argument('--name', default='')
    launch.add_argument('--timeout', type=float, default=0)
    launch.add_argument('--workers', type=int, default=worker_policy.DEFAULT_MAX_WORKERS)
    launch.add_argument('command', nargs=argparse.REMAINDER)
    sub.add_parser('status')
    cleanup = sub.add_parser('cleanup')
    cleanup.add_argument('--kill', action='store_true')
    cleanup.add_argument('--stale-sec', type=int, default=300)
    stop = sub.add_parser('stop')
    stop.add_argument('run_id')
    args = ap.parse_args()
    if args.cmd == 'launch':
        command = list(args.command)
        if command and command[0] == '--':
            command = command[1:]
        if not command:
            raise SystemExit('launch requires a command after --')
        payload = run_and_track(
            command,
            cwd=HERE,
            timeout=args.timeout or None,
            name=args.name or None,
            workers=args.workers,
        )
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
        return 0 if payload.get('ok') else 1
    if args.cmd == 'status':
        print(json.dumps({'ok': True, 'worker_policy': worker_policy.describe_policy(), 'runs': list_runs()}, indent=2, sort_keys=True))
        return 0
    if args.cmd == 'cleanup':
        print(json.dumps(cleanup_stale(kill=args.kill, stale_sec=args.stale_sec), indent=2, sort_keys=True))
        return 0
    if args.cmd == 'stop':
        payload = stop_run(args.run_id)
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0 if payload.get('ok') else 1
    raise ValueError(args.cmd)


if __name__ == '__main__':
    raise SystemExit(main())
