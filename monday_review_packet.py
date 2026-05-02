from __future__ import annotations

import argparse
import json
import os
from datetime import datetime

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from review_artifacts import build_monday_review_packet, write_all


HERE = os.path.dirname(os.path.abspath(__file__))
CT = ZoneInfo('America/Chicago')


def today_ct() -> str:
    return datetime.now(CT).date().isoformat()


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Build the compact decision-ready review packet for a trading day.'
    )
    parser.add_argument('day', nargs='?', default=today_ct())
    parser.add_argument('--json', action='store_true', help='Print the packet JSON after writing artifacts.')
    args = parser.parse_args()

    paths, _ = write_all(args.day)
    packet_path = paths.get('monday_review_packet')
    if args.json:
        print(json.dumps(build_monday_review_packet(args.day), indent=2, default=str))
    else:
        print(packet_path)
        print(paths.get('review_index'))
        print(paths.get('shadow_exit_summary'))
        print(paths.get('execution_summary'))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
