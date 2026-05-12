from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path

from daily_postmortem import OUT_DIR, externalize_postmortem_payloads, write_json_atomic


POSTMORTEM_NAME = re.compile(r'^postmortem_(\d{4}-\d{2}-\d{2})\.json$')


def archive_file(path: Path, min_bytes: int) -> dict:
    before = path.stat().st_size
    if before < min_bytes:
        return {'path': str(path), 'changed': False, 'reason': 'below_min_bytes', 'before_bytes': before}

    match = POSTMORTEM_NAME.match(path.name)
    if not match:
        return {'path': str(path), 'changed': False, 'reason': 'not_daily_postmortem'}

    with path.open('r', encoding='utf-8') as f:
        payload = json.load(f)

    compacted = externalize_postmortem_payloads(payload, match.group(1))
    write_json_atomic(str(path), compacted)
    after = path.stat().st_size
    archived = sorted((compacted.get('archived_embedded_payloads') or {}).keys())
    return {
        'path': str(path),
        'changed': after < before,
        'before_bytes': before,
        'after_bytes': after,
        'saved_bytes': before - after,
        'archived_keys': archived,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description='Archive bulky embedded daily postmortem payloads.')
    parser.add_argument('days', nargs='*', help='Optional YYYY-MM-DD day filters.')
    parser.add_argument('--min-bytes', type=int, default=250_000)
    args = parser.parse_args()

    root = Path(OUT_DIR)
    allowed_days = set(args.days)
    results = []
    for path in sorted(root.glob('postmortem_*.json')):
        match = POSTMORTEM_NAME.match(path.name)
        if not match:
            continue
        if allowed_days and match.group(1) not in allowed_days:
            continue
        results.append(archive_file(path, args.min_bytes))

    print(json.dumps({'postmortem_dir': os.path.abspath(OUT_DIR), 'results': results}, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
