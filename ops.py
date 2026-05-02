from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from urllib.request import urlopen

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from automation_ops import ensure_app_running, run_phase, start_live_monitor


HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, 'postmortem')
CT = ZoneInfo('America/Chicago')
STATUS_URL = 'http://127.0.0.1:5000/mock/status'


def _today() -> str:
    return datetime.now(CT).date().isoformat()


def _read_status(timeout: int = 5) -> dict:
    try:
        with urlopen(STATUS_URL, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode('utf-8'))
        return {'ok': True, 'status': payload}
    except Exception as exc:
        return {'ok': False, 'error': str(exc), 'status': {}}


def _compact_status() -> dict:
    raw = _read_status()
    status = raw.get('status') or {}
    payload = {
        'ok': raw.get('ok'),
        'generated_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'app_url': STATUS_URL,
        'running': status.get('running'),
        'open_position_count': status.get('open_position_count'),
        'pending_entries': status.get('pending_entries'),
        'broker_exposure_block': status.get('broker_exposure_block'),
        'broker_api_degraded': status.get('broker_api_degraded'),
        'kill_switch': status.get('kill_switch'),
        'flatten_cutoff_ct': status.get('flatten_cutoff_ct'),
        'alpaca_equity': status.get('alpaca_equity'),
        'alpaca_cash': status.get('alpaca_cash'),
        'positions': status.get('positions'),
    }
    if not raw.get('ok'):
        payload['error'] = raw.get('error')
    return payload


def _write_payload(name: str, day: str, payload: dict) -> dict:
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, f'ops_{name}_{day}.json')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, default=str)
    payload['artifact'] = path
    return payload


def _write_codex_context(day: str) -> dict:
    from review_artifacts import build_artifact_manifest, build_codex_context, build_review_index
    index = build_review_index(day)
    manifest = build_artifact_manifest(day)
    text = build_codex_context(day, index, manifest)
    path = os.path.join(HERE, 'CODEX_CONTEXT.md')
    with open(path, 'w', encoding='utf-8') as f:
        f.write(text)
    latest_index = os.path.join(OUT_DIR, 'REVIEW_INDEX_LATEST.json')
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(latest_index, 'w', encoding='utf-8') as f:
        json.dump(index, f, indent=2, default=str)
    return {
        'ok': True,
        'command': 'context',
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'context_file': path,
        'review_index_latest': latest_index,
    }


def run_command(command: str, day: str | None = None, force: bool = False) -> dict:
    day = day or _today()
    if command == 'status':
        return _write_payload(command, day, _compact_status())
    if command == 'app-start':
        payload = ensure_app_running()
        payload.update({'command': command, 'day': day, 'created_at_ct': datetime.now(CT).isoformat(timespec='seconds')})
        return _write_payload(command, day, payload)
    if command == 'monitor':
        payload = start_live_monitor(day)
        payload.update({'command': command, 'day': day, 'created_at_ct': datetime.now(CT).isoformat(timespec='seconds')})
        return _write_payload(command, day, payload)
    if command == 'context':
        return _write_payload(command, day, _write_codex_context(day))

    phase_by_command = {
        'pre-open': 'pre-open',
        'post-open': 'post-open',
        'intraday': 'intraday',
        'pre-flat': 'pre-flat',
        'post-close': 'post-close',
    }
    phase = phase_by_command.get(command)
    if not phase:
        raise ValueError(f'unknown ops command: {command}')
    return run_phase(phase, day, force=force)


def main() -> int:
    ap = argparse.ArgumentParser(
        description='Single stable operator entrypoint for the trading bot.'
    )
    ap.add_argument(
        'command',
        choices=('status', 'app-start', 'monitor', 'context', 'pre-open', 'post-open', 'intraday', 'pre-flat', 'post-close'),
    )
    ap.add_argument('day', nargs='?', default=None)
    ap.add_argument('--json', action='store_true')
    ap.add_argument('--force', action='store_true', help='Allow rerunning idempotent guarded commands such as post-close.')
    args = ap.parse_args()

    payload = run_command(args.command, args.day, force=args.force)
    if args.json:
        print(json.dumps(payload, indent=2, default=str))
    else:
        print(f"ops command={args.command} day={args.day or _today()} ok={payload.get('ok')}")
        if payload.get('artifact'):
            print(payload['artifact'])
    return 0 if payload.get('ok') is not False else 1


if __name__ == '__main__':
    sys.exit(main())
