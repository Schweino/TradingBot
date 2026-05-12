from __future__ import annotations

import json
import os
import shutil
from datetime import datetime
from pathlib import Path


HERE = Path(__file__).resolve().parent
POSTMORTEM_DIR = HERE / 'postmortem'
ARCHIVE_ROOT = POSTMORTEM_DIR / 'archive'
PRESERVE_NAMES = {
    'backtests',
    'cache_certifications',
    'cache_catalog',
    'health_heartbeats',
    'live_step2_events',
    'rebuild_plans',
    'runtime',
    'step2_freshness',
    'trading_events.sqlite',
    'trading_events.sqlite-shm',
    'trading_events.sqlite-wal',
}


def _unique_archive_dir() -> Path:
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    base = ARCHIVE_ROOT / f'legacy_pre_simple_postmortem_{stamp}'
    path = base
    idx = 2
    while path.exists():
        path = ARCHIVE_ROOT / f'{base.name}_{idx}'
        idx += 1
    return path


def main() -> int:
    workspace = HERE.resolve()
    postmortem = POSTMORTEM_DIR.resolve()
    archive_root = ARCHIVE_ROOT.resolve()
    if workspace not in postmortem.parents:
        raise RuntimeError(f'postmortem dir is outside workspace: {postmortem}')

    existing = sorted(ARCHIVE_ROOT.glob('legacy_pre_simple_postmortem_*'))
    incomplete = [p for p in existing if p.is_dir() and not (p / 'LEGACY_ARCHIVE_MANIFEST.json').exists()]
    archive_dir = incomplete[-1] if incomplete else _unique_archive_dir()
    archive_dir.mkdir(parents=True, exist_ok=True)

    moved = []
    skipped = []
    for item in sorted(POSTMORTEM_DIR.iterdir(), key=lambda p: p.name.lower()):
        if item.resolve() == archive_root:
            skipped.append({'name': item.name, 'reason': 'archive_root'})
            continue
        if item.name in PRESERVE_NAMES:
            skipped.append({'name': item.name, 'reason': 'operational_cache_preserved'})
            continue
        dest = archive_dir / item.name
        try:
            shutil.move(str(item), str(dest))
            moved.append({
                'name': item.name,
                'kind': 'dir' if dest.is_dir() else 'file',
                'bytes': dest.stat().st_size if dest.is_file() else None,
            })
        except PermissionError as e:
            skipped.append({'name': item.name, 'reason': f'locked_or_denied: {e}'})
        except OSError as e:
            skipped.append({'name': item.name, 'reason': f'os_error: {e}'})

    manifest = {
        'created_at': datetime.now().isoformat(timespec='seconds'),
        'reason': 'Archived legacy postmortem corpus before enabling simple postmortem memory.',
        'source_dir': str(POSTMORTEM_DIR),
        'archive_dir': str(archive_dir),
        'moved_count': len(moved),
        'moved': moved,
        'skipped': skipped,
    }
    manifest_path = archive_dir / 'LEGACY_ARCHIVE_MANIFEST.json'
    with manifest_path.open('w', encoding='utf-8') as f:
        json.dump(manifest, f, indent=2)
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
