from __future__ import annotations

import argparse
import json
import os
from datetime import datetime

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo


HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, 'postmortem')
CT = ZoneInfo('America/Chicago')


def write_schedule_plan() -> tuple[str, dict]:
    payload = {
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'timezone': 'America/Chicago',
        'daily_close': {
            'recommended_time_ct': '15:10',
            'command': 'python daily_close_packet.py',
            'purpose': 'Run postmortem, compact artifacts, promotion review, EOD checks, and close packet every trading day.',
        },
        'intraday_monitor': {
            'recommended_window_ct': '08:30-15:00',
            'command': 'python live_monitor.py --loop --interval-sec 30',
            'purpose': 'Refresh why-no-trade, passive alerts, live review, config drift watch, and start-here artifact.',
        },
        'post_open_sanity': {
            'recommended_time_ct': '08:35',
            'command': 'python smoke_check.py --mode post-open',
            'purpose': 'Confirm app/feeds/broker health after the open.',
        },
        'note': 'Codex automations may also be configured for daily close. Intraday 30-second monitoring should run locally as a process.',
    }
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, 'daily_schedule_plan.json')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2)
    return path, payload


def main() -> int:
    ap = argparse.ArgumentParser(description='Write daily schedule plan.')
    ap.add_argument('--json', action='store_true')
    args = ap.parse_args()
    path, payload = write_schedule_plan()
    if args.json:
        print(json.dumps({'path': path, 'payload': payload}, indent=2))
    else:
        print(path)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
