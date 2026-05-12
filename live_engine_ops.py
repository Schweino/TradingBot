"""Safe local live-engine process helper.

This keeps future approvals narrow: approve `python live_engine_ops.py ...`
instead of broad PowerShell process-management commands with changing PIDs.
The script only targets this workspace's Flask/mock app on localhost:5000.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen


HERE = Path(__file__).resolve().parent
PYTHON = HERE.parent / 'python.exe'
STATUS_URL = 'http://127.0.0.1:5000/mock/status'
BOOT_LOG = HERE / 'flask_boot.log'
BOOT_ERR_LOG = HERE / 'flask_boot.err.log'


def _read_status(timeout: float = 5.0, full: bool = False) -> dict:
    try:
        suffix = '?full=1' if full else ''
        with urlopen(STATUS_URL + suffix, timeout=timeout) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except Exception as exc:
        return {'reachable': False, 'error': repr(exc)}


def _port_5000_pids() -> list[int]:
    try:
        proc = subprocess.run(
            ['netstat', '-ano'],
            cwd=str(HERE),
            capture_output=True,
            text=True,
            timeout=10,
        )
    except Exception:
        return []
    pids: set[int] = set()
    for line in proc.stdout.splitlines():
        parts = line.split()
        if len(parts) < 5:
            continue
        local_addr = parts[1]
        state = parts[3] if len(parts) >= 5 else ''
        pid_raw = parts[-1]
        if not local_addr.endswith(':5000') or state.upper() != 'LISTENING':
            continue
        try:
            pids.add(int(pid_raw))
        except ValueError:
            pass
    return sorted(pids)


def status(args: argparse.Namespace) -> int:
    mock_status = _read_status(timeout=args.timeout, full=args.full)
    if not args.full and isinstance(mock_status, dict) and mock_status.get('reachable') is False:
        mock_status = {
            'reachable': False,
            'error': mock_status.get('error'),
        }
    elif not args.full and isinstance(mock_status, dict):
        mock_status = {
            key: mock_status.get(key)
            for key in (
                'running', 'in_window', 'positions', 'pending_entries',
                'broker_exposure_block', 'broker_lifecycle_block',
                'broker_lifecycle_gate', 'broker_api_degraded',
                'kill_switch', 'strategy_config_hash',
                'execution_kernel_hash', 'step2_parity_contract_hash',
                'step2_execution_contract_hash', 'active_scoring_profile',
                'startup_self_check', 'startup_contract_gate_block',
                'state_market_day', 'state_rollover', 'state_rollover_block',
                'step2_density_counts_today',
                'feed_health',
            )
        }
    payload = {
        'port_5000_pids': _port_5000_pids(),
        'mock_status': mock_status,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def stop(args: argparse.Namespace) -> int:
    pids = _port_5000_pids()
    stopped = []
    for pid in pids:
        subprocess.run(
            ['taskkill', '/PID', str(pid), '/F'],
            cwd=str(HERE),
            capture_output=True,
            text=True,
            timeout=20,
        )
        stopped.append(pid)
    deadline = time.time() + args.timeout
    while time.time() < deadline and _port_5000_pids():
        time.sleep(0.25)
    remaining = _port_5000_pids()
    print(json.dumps({'stopped': stopped, 'remaining_port_5000_pids': remaining}, indent=2))
    return 0 if not remaining else 1


def start(args: argparse.Namespace) -> int:
    if _port_5000_pids():
        print(json.dumps({'started': False, 'reason': 'already_listening', 'pids': _port_5000_pids()}, indent=2))
        return 0
    exe = PYTHON if PYTHON.exists() else Path(sys.executable)
    with BOOT_LOG.open('a', encoding='utf-8') as out, BOOT_ERR_LOG.open('a', encoding='utf-8') as err:
        proc = subprocess.Popen(
            [str(exe), 'local_server.py'],
            cwd=str(HERE),
            stdout=out,
            stderr=err,
            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0),
        )
    deadline = time.time() + args.timeout
    last_status = {}
    while time.time() < deadline:
        if _port_5000_pids():
            last_status = _read_status(timeout=2)
            if not last_status.get('error'):
                break
        time.sleep(0.5)
    print(json.dumps({
        'started': True,
        'pid': proc.pid,
        'port_5000_pids': _port_5000_pids(),
        'mock_status': last_status or _read_status(timeout=2),
    }, indent=2, sort_keys=True))
    return 0 if _port_5000_pids() else 1


def restart(args: argparse.Namespace) -> int:
    stop_code = stop(args)
    if stop_code != 0:
        return stop_code
    return start(args)


def smoke(args: argparse.Namespace) -> int:
    cmd = [sys.executable, 'smoke_check.py']
    if args.pre_market:
        cmd.append('--pre-market')
    proc = subprocess.run(cmd, cwd=str(HERE), text=True)
    return int(proc.returncode)


def main() -> int:
    ap = argparse.ArgumentParser(description='Project-scoped live engine operations.')
    ap.add_argument('--timeout', type=float, default=60.0)
    sub = ap.add_subparsers(dest='cmd', required=True)
    status_ap = sub.add_parser('status')
    status_ap.add_argument('--full', action='store_true')
    sub.add_parser('stop')
    sub.add_parser('start')
    sub.add_parser('restart')
    smoke_ap = sub.add_parser('smoke')
    smoke_ap.add_argument('--pre-market', action='store_true')
    args = ap.parse_args()
    return globals()[args.cmd](args)


if __name__ == '__main__':
    raise SystemExit(main())
