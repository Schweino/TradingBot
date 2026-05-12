from __future__ import annotations

from output_paths import output_path

import argparse
import json
import os
from datetime import datetime

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo


HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = output_path('postmortem')
CT = ZoneInfo('America/Chicago')


def write_schedule_plan() -> tuple[str, dict]:
    payload = {
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'timezone': 'America/Chicago',
        'pre_open': {
            'recommended_time_ct': '08:15',
            'command': 'python automation_ops.py pre-open',
            'scheduled_action': 'powershell -NoProfile -ExecutionPolicy Bypass -File .\\run_automation_phase.ps1 pre-open',
            'task_name': 'ClaudeMockTraderPreOpen',
            'purpose': 'Ensure app is running, run no-surprises smoke, refresh readiness artifacts, and start live monitor.',
        },
        'post_open_sanity': {
            'recommended_time_ct': '08:35',
            'command': 'python automation_ops.py post-open',
            'scheduled_action': 'powershell -NoProfile -ExecutionPolicy Bypass -File .\\run_automation_phase.ps1 post-open',
            'task_name': 'ClaudeMockTraderPostOpen',
            'purpose': 'Confirm app/feeds/broker health after the open and write post-open checkpoint artifacts.',
        },
        'intraday_step2_parity': {
            'recommended_times_ct': ['09:30', '11:00', '12:30', '14:00'],
            'command': 'python automation_ops.py intraday',
            'scheduled_action': 'powershell -NoProfile -ExecutionPolicy Bypass -File .\\run_automation_phase.ps1 intraday',
            'task_names': [
                'ClaudeMockTraderIntradayStep2Warm0930',
                'ClaudeMockTraderIntradayStep2Warm1100',
                'ClaudeMockTraderIntradayStep2Warm1230',
                'ClaudeMockTraderIntradayStep2Warm1400',
            ],
            'purpose': (
                'Refresh the live Step 2 tape, update incremental market partitions, score the active profile, '
                'and write canonical opportunity, diff-classifier, parity, cache-layer, and registry artifacts.'
            ),
        },
        'pre_flat_checkpoint': {
            'recommended_time_ct': '14:50',
            'command': 'python automation_ops.py pre-flat',
            'scheduled_action': 'powershell -NoProfile -ExecutionPolicy Bypass -File .\\run_automation_phase.ps1 pre-flat',
            'task_name': 'ClaudeMockTraderPreFlat',
            'purpose': 'Write a final intraday safety/learning checkpoint before the 14:55 hard-flat cutoff.',
        },
        'post_close': {
            'recommended_time_ct': '15:10',
            'command': 'python automation_ops.py post-close',
            'scheduled_action': 'powershell -NoProfile -ExecutionPolicy Bypass -File .\\run_automation_phase.ps1 post-close',
            'task_name': 'ClaudeMockTraderPostClose',
            'purpose': (
                'Run postmortem plus Step 2/Live architecture outputs: latency shards, compiled Step 2, '
                'canonical opportunity ledger, diff classifier, cache layers, artifact registry, Google Doc summary, context packet, and post-market smoke.'
            ),
        },
        'verify_day': {
            'recommended_time_ct': '16:30',
            'command': 'python automation_ops.py verify-day',
            'scheduled_action': 'powershell -NoProfile -ExecutionPolicy Bypass -File .\\run_automation_phase.ps1 verify-day',
            'task_name': 'ClaudeMockTraderVerifyDay',
            'purpose': 'Final watchdog that verifies every scheduled phase, including intraday, produced the required architecture artifacts and ledger row.',
        },
        'intraday_monitor': {
            'recommended_window_ct': '08:15-15:00',
            'command': 'python automation_ops.py pre-open',
            'started_by_task': 'ClaudeMockTraderPreOpen',
            'purpose': 'Pre-open automation starts live_monitor.py --loop --interval-sec 30 with the parity sentinel refreshing every 5 minutes.',
        },
        'intraday_parity_sentinel': {
            'recommended_window_ct': '08:15-15:20',
            'command': 'python automation_ops.py parity-sentinel',
            'scheduled_action': 'powershell -NoProfile -ExecutionPolicy Bypass -File .\\run_automation_phase.ps1 parity-sentinel',
            'started_by_task': 'ClaudeMockTraderPreOpen live monitor loop',
            'purpose': 'On-demand Live-vs-Step2 runtime drift check using the current intraday tape and fast live-signal Step 2 parity ledger.',
        },
        'installer': {
            'command': 'powershell -ExecutionPolicy Bypass -File .\\register_postmortem_task.ps1',
            'purpose': 'Install or refresh all Windows Scheduled Tasks for the trading day.',
        },
        'note': 'The old direct daily_postmortem.py task is intentionally replaced by automation_ops.py post-close. Scheduled tasks use run_automation_phase.ps1 so every run has a local log even if Python exits early.',
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
