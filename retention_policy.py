from __future__ import annotations

import json
import os
import re
import shutil
from datetime import datetime, timedelta
from typing import Any, Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore


HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, 'postmortem')
TICK_DIR = os.path.join(HERE, 'tick_logs')
CONFIG_PATH = os.path.join(HERE, 'trading_config.json')
CT = ZoneInfo('America/Chicago')
DATE_DIR_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')


def _read_json(path: str, default: Any = None) -> Any:
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


def _file_info(path: str) -> dict:
    try:
        st = os.stat(path)
        return {
            'path': path,
            'bytes': st.st_size,
            'modified_at_ct': datetime.fromtimestamp(st.st_mtime, CT).isoformat(timespec='seconds'),
        }
    except Exception as e:
        return {'path': path, 'error': str(e)}


def _walk_files(root: str) -> list[str]:
    out = []
    if not os.path.exists(root):
        return out
    for base, _, names in os.walk(root):
        for name in names:
            out.append(os.path.join(base, name))
    return out


def _safe_workspace_path(path: str) -> str:
    resolved = os.path.abspath(path)
    workspace = os.path.abspath(HERE)
    if resolved != workspace and not resolved.startswith(workspace + os.sep):
        raise ValueError(f'path is outside workspace: {resolved}')
    return resolved


def _market_day_dirs(root: str = TICK_DIR, as_of_day: Optional[str] = None) -> list[dict]:
    days = []
    if not os.path.isdir(root):
        return days
    cutoff = None
    if as_of_day:
        cutoff = datetime.strptime(as_of_day, '%Y-%m-%d').date()
    for entry in os.scandir(root):
        if not entry.is_dir() or not DATE_DIR_RE.match(entry.name):
            continue
        try:
            day_date = datetime.strptime(entry.name, '%Y-%m-%d').date()
        except Exception:
            continue
        files = _walk_files(entry.path)
        total_bytes = sum(int(os.path.getsize(path) or 0) for path in files if os.path.exists(path))
        stat = entry.stat()
        days.append({
            'day': entry.name,
            'path': os.path.abspath(entry.path),
            'files': len(files),
            'bytes': total_bytes,
            'modified_at_ct': datetime.fromtimestamp(stat.st_mtime, CT).isoformat(timespec='seconds'),
            'future_of_as_of_day': bool(cutoff and day_date > cutoff),
        })
    return sorted(days, key=lambda row: row['day'])


def _raw_tick_market_day_plan(day: str, keep_market_days: int) -> dict:
    all_days = _market_day_dirs(TICK_DIR, as_of_day=day)
    eligible = [row for row in all_days if not row.get('future_of_as_of_day')]
    future_days = [row for row in all_days if row.get('future_of_as_of_day')]
    keep_count = max(0, int(keep_market_days))
    kept_days = eligible[-keep_count:] if keep_count else []
    delete_days = eligible[:-keep_count] if keep_count else eligible
    return {
        'mode': 'market_data_days',
        'keep_market_days': keep_count,
        'raw_tick_market_days_found': len(eligible),
        'future_tick_days_preserved': len(future_days),
        'kept_market_days': kept_days,
        'delete_market_days': [
            {
                **row,
                'recommended_action': 'delete_raw_tick_market_day_folder',
            }
            for row in delete_days
        ],
        'delete_candidate_bytes': sum(int(row.get('bytes') or 0) for row in delete_days),
        'delete_candidate_files': sum(int(row.get('files') or 0) for row in delete_days),
    }


def apply_raw_tick_retention(day: Optional[str] = None, *, require_delete_enabled: bool = True) -> dict:
    day = day or datetime.now(CT).date().isoformat()
    cfg = (_read_json(CONFIG_PATH, {}) or {}).get('retention', {}) or {}
    keep_market_days = int(cfg.get('raw_tick_keep_market_days', cfg.get('raw_tick_keep_days', 7)))
    delete_enabled = bool(cfg.get('delete_enabled', False))
    raw_tick_delete_enabled = bool(cfg.get('raw_tick_delete_enabled', delete_enabled))
    plan = _raw_tick_market_day_plan(day, keep_market_days)
    deleted = []
    errors = []
    if require_delete_enabled and not (delete_enabled and raw_tick_delete_enabled):
        return {
            'day': day,
            'mode': 'delete_raw_tick_logs',
            'ok': True,
            'applied': False,
            'reason': 'retention.delete_enabled and retention.raw_tick_delete_enabled must both be true',
            'policy': {
                'delete_enabled': delete_enabled,
                'raw_tick_delete_enabled': raw_tick_delete_enabled,
                'raw_tick_keep_market_days': keep_market_days,
            },
            'plan': plan,
        }
    tick_root = _safe_workspace_path(TICK_DIR)
    for row in plan['delete_market_days']:
        path = _safe_workspace_path(str(row.get('path') or ''))
        if os.path.dirname(path) != tick_root:
            errors.append({**row, 'error': 'candidate is not a direct child of tick_logs'})
            continue
        if not DATE_DIR_RE.match(os.path.basename(path)):
            errors.append({**row, 'error': 'candidate folder name is not YYYY-MM-DD'})
            continue
        try:
            shutil.rmtree(path)
            deleted.append(row)
        except Exception as exc:
            errors.append({**row, 'error': str(exc)})
    return {
        'day': day,
        'mode': 'delete_raw_tick_logs',
        'ok': not errors,
        'applied': True,
        'policy': {
            'delete_enabled': delete_enabled,
            'raw_tick_delete_enabled': raw_tick_delete_enabled,
            'raw_tick_keep_market_days': keep_market_days,
        },
        'deleted_market_days': deleted,
        'deleted_market_days_count': len(deleted),
        'deleted_bytes': sum(int(row.get('bytes') or 0) for row in deleted),
        'deleted_files': sum(int(row.get('files') or 0) for row in deleted),
        'errors': errors,
        'plan': plan,
    }


def _date_from_name(path: str) -> Optional[str]:
    name = os.path.basename(path)
    for i in range(max(0, len(name) - 9)):
        chunk = name[i:i + 10]
        try:
            datetime.strptime(chunk, '%Y-%m-%d')
            return chunk
        except Exception:
            continue
    return None


def build_retention_plan(day: Optional[str] = None) -> dict:
    day = day or datetime.now(CT).date().isoformat()
    cfg = (_read_json(CONFIG_PATH, {}) or {}).get('retention', {}) or {}
    raw_keep_days = int(cfg.get('raw_tick_keep_days', 7))
    raw_keep_market_days = int(cfg.get('raw_tick_keep_market_days', raw_keep_days))
    compact_keep_days = int(cfg.get('compact_artifact_keep_days', 180))
    summary_keep_days = int(cfg.get('postmortem_summary_keep_days', 365))
    delete_enabled = bool(cfg.get('delete_enabled', False))
    raw_tick_delete_enabled = bool(cfg.get('raw_tick_delete_enabled', delete_enabled))
    now_day = datetime.strptime(day, '%Y-%m-%d').date()
    raw_cutoff = (now_day - timedelta(days=raw_keep_days)).isoformat()
    compact_cutoff = (now_day - timedelta(days=compact_keep_days)).isoformat()
    summary_cutoff = (now_day - timedelta(days=summary_keep_days)).isoformat()

    raw_tick_files = []
    compact_files = []
    summary_files = []
    archive_candidates = []
    raw_market_day_plan = _raw_tick_market_day_plan(day, raw_keep_market_days)
    for path in _walk_files(TICK_DIR):
        fday = _date_from_name(path)
        info = _file_info(path)
        info['day'] = fday
        raw_tick_files.append(info)
        if fday and fday < raw_cutoff:
            archive_candidates.append({**info, 'recommended_action': 'archive_or_compress_raw_tick_log'})
    for path in _walk_files(OUT_DIR):
        fday = _date_from_name(path)
        if not fday:
            continue
        info = _file_info(path)
        info['day'] = fday
        name = os.path.basename(path)
        if name.startswith(('postmortem_', 'POSTMORTEM_START_HERE_', 'DAILY_REVIEW_START_HERE_')):
            summary_files.append(info)
            if fday < summary_cutoff:
                archive_candidates.append({**info, 'recommended_action': 'archive_old_summary'})
        else:
            compact_files.append(info)
            if fday < compact_cutoff:
                archive_candidates.append({**info, 'recommended_action': 'archive_old_compact_artifact'})

    return {
        'day': day,
        'mode': 'non_destructive_plan_only',
        'delete_enabled': delete_enabled,
        'policy': {
            'raw_tick_keep_days': raw_keep_days,
            'raw_tick_keep_market_days': raw_keep_market_days,
            'raw_tick_delete_enabled': raw_tick_delete_enabled,
            'compact_artifact_keep_days': compact_keep_days,
            'postmortem_summary_keep_days': summary_keep_days,
            'raw_cutoff_before_day': raw_cutoff,
            'raw_market_days_retained': raw_market_day_plan['keep_market_days'],
            'compact_cutoff_before_day': compact_cutoff,
            'summary_cutoff_before_day': summary_cutoff,
        },
        'totals': {
            'raw_tick_files': len(raw_tick_files),
            'raw_tick_bytes': sum(int(f.get('bytes') or 0) for f in raw_tick_files),
            'compact_files': len(compact_files),
            'compact_bytes': sum(int(f.get('bytes') or 0) for f in compact_files),
            'summary_files': len(summary_files),
            'summary_bytes': sum(int(f.get('bytes') or 0) for f in summary_files),
            'archive_candidates': len(archive_candidates),
            'archive_candidate_bytes': sum(int(f.get('bytes') or 0) for f in archive_candidates),
            'raw_tick_market_days': raw_market_day_plan['raw_tick_market_days_found'],
            'raw_tick_market_day_delete_candidates': len(raw_market_day_plan['delete_market_days']),
            'raw_tick_market_day_delete_candidate_bytes': raw_market_day_plan['delete_candidate_bytes'],
        },
        'raw_tick_market_day_retention': raw_market_day_plan,
        'archive_candidates': sorted(archive_candidates, key=lambda r: (r.get('day') or '', r.get('path') or ''))[:250],
        'deduction': (
            'The bot keeps compact learning artifacts local and plans raw tick retention by market-data days, '
            'not calendar days. This plan is non-destructive; old raw tick day folders are deleted only when '
            'retention_policy.py is run with --apply and retention.delete_enabled plus '
            'retention.raw_tick_delete_enabled are both true.'
        ),
    }


def write_retention_plan(day: Optional[str] = None) -> tuple[str, dict]:
    payload = build_retention_plan(day)
    path = os.path.join(OUT_DIR, f'retention_plan_{payload["day"]}.json')
    return _write_json(path, payload), payload


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser(description='Build or apply postmortem/tick-log retention policy.')
    ap.add_argument('day', nargs='?', default=datetime.now(CT).date().isoformat())
    ap.add_argument('--write', action='store_true')
    ap.add_argument('--json', action='store_true')
    ap.add_argument('--apply', action='store_true', help='Permanently delete raw tick day folders beyond the retained market-day window.')
    ap.add_argument(
        '--force-delete-raw-tick-logs',
        action='store_true',
        help='Allow deletion even when retention delete toggles are disabled; intended for manual one-off use.',
    )
    args = ap.parse_args()
    if args.apply:
        payload = apply_raw_tick_retention(
            args.day,
            require_delete_enabled=not args.force_delete_raw_tick_logs,
        )
        path = os.path.join(OUT_DIR, f'retention_apply_{args.day}.json')
        _write_json(path, payload)
        payload = {**payload, 'file': path}
    elif args.write:
        path, payload = write_retention_plan(args.day)
        payload = {**payload, 'file': path}
    else:
        payload = build_retention_plan(args.day)
    if args.json:
        print(json.dumps(payload, indent=2, default=str))
    else:
        print(payload.get('file') or os.path.join(OUT_DIR, f'retention_plan_{args.day}.json'))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
