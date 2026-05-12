from __future__ import annotations

from output_paths import output_path

import argparse
import json
import os
import shutil
from datetime import datetime, timedelta
from typing import Optional
from urllib.request import urlopen

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from weekend_readiness import (
    build_broker_execution_score,
    build_broker_safety_snapshot,
    build_config_change_watch,
    build_era_scorecard,
    build_feed_health_score,
    build_monday_scorecard,
    build_premarket_checklist,
    build_trade_alerts,
    build_trader_notes_template,
    write_config_freeze,
    write_out_of_sample_boundary,
    write_trader_notes_template,
)


HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = output_path('postmortem')
CT = ZoneInfo('America/Chicago')


def _today() -> str:
    return datetime.now(CT).date().isoformat()


def _next_weekday(day: Optional[str] = None) -> str:
    d = datetime.fromisoformat(day).date() if day else datetime.now(CT).date()
    d += timedelta(days=1)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d.isoformat()


def _write_text(path: str, text: str) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(text)
    return path


def _write_json(path: str, payload: dict) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, default=str)
    return path


def _status() -> tuple[dict, Optional[str]]:
    try:
        with urlopen('http://127.0.0.1:5000/mock/status', timeout=5) as resp:
            return json.loads(resp.read().decode('utf-8')), None
    except Exception as e:
        return {}, str(e)


def write_monday_notes(day: Optional[str] = None, overwrite: bool = False) -> tuple[str, dict]:
    day = day or _next_weekday(_today())
    path, payload = write_trader_notes_template(day, overwrite=overwrite)
    if not payload:
        payload = build_trader_notes_template(day)
        path = _write_json(os.path.join(OUT_DIR, f'notes_{day}.json'), payload)
    return path, payload


def build_known_risks(day: Optional[str] = None) -> dict:
    day = day or _next_weekday(_today())
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'purpose': 'Human-readable watchlist for Monday. This file is not consumed by live trading logic.',
        'do_not_change_before_data': [
            'Do not alter live entry/exit logic intraday from a single trade.',
            'Do not promote candidates while operational contamination is present.',
            'Do not override the config freeze without documenting why.',
        ],
        'watch_items': [
            {
                'name': 'short_conviction_decay',
                'question': 'Does it reduce SHORT average loss without cutting high-quality SHORT winners?',
                'evidence_files': ['postmortem/trade_alerts_YYYY-MM-DD.json', 'postmortem/loser_clusters_YYYY-MM-DD.json'],
            },
            {
                'name': 'long_conviction_decay',
                'question': 'Does it trim weak LONG follow-through without damaging profitable LONG setups?',
                'evidence_files': ['postmortem/strategy_operations_split_YYYY-MM-DD.json'],
            },
            {
                'name': 'large_loss_reduction',
                'question': 'Are the largest losers smaller than Friday after guardrail changes?',
                'threshold': 'warn when largest loss <= -150',
            },
            {
                'name': 'operational_contamination',
                'question': 'Are manual/external/session-end exits separated from strategy quality?',
                'evidence_files': ['postmortem/strategy_operations_split_YYYY-MM-DD.json'],
            },
            {
                'name': 'feed_freshness',
                'question': 'Do BTC or stock feed staleness warnings appear during live checks?',
                'thresholds': {'BTC/USD': '20s', 'stocks': '10s'},
            },
            {
                'name': 'why_no_trade',
                'question': 'If quiet, are gates blocking for good reasons: score, BTC conflict, spread, stale quote, cooldown, or risk-off?',
                'evidence_files': ['postmortem/why_no_trade_YYYY-MM-DD.json'],
            },
            {
                'name': 'short_side_performance',
                'question': 'Do shorts recover under the new conviction logic, or keep underperforming?',
                'evidence_files': ['postmortem/per_ticker_learning_YYYY-MM-DD.json'],
            },
        ],
    }


def write_known_risks(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _next_weekday(_today())
    payload = build_known_risks(day)
    path = os.path.join(OUT_DIR, f'known_risks_{day}.json')
    return _write_json(path, payload), payload


def build_command_menu(day: Optional[str] = None) -> dict:
    day = day or _next_weekday(_today())
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'commands': [
            {'when': 'pre_open', 'command': 'python smoke_check.py --mode no-surprises'},
            {'when': 'start_intraday_monitor', 'command': 'python live_monitor.py --loop --interval-sec 30'},
            {'when': 'post_open_0835_ct', 'command': 'python smoke_check.py --mode post-open'},
            {'when': 'refresh_once', 'command': f'python live_monitor.py {day} --checkpoint --checkpoint-label manual'},
            {'when': 'post_market', 'command': 'python monday_close_packet.py'},
        ],
        'files_to_inspect_first': [
            f'postmortem/MONDAY_REVIEW_START_HERE_{day}.json',
            f'postmortem/monday_live_review_{day}.json',
            f'postmortem/first_hour_review_{day}.json',
            f'postmortem/world_class_dashboard_{day}.json',
            f'postmortem/feed_health_score_{day}.json',
            f'postmortem/broker_execution_score_{day}.json',
            f'postmortem/era_scorecard_{day}.json',
            f'postmortem/monday_scorecard_{day}.json',
            f'postmortem/ws_scalp_replay_{day}.json',
            f'postmortem/current_engine_regrade_{day}.json',
            f'postmortem/regime_specific_scorecards_{day}.json',
            f'postmortem/near_miss_winners_{day}.json',
            f'postmortem/exit_quality_score_{day}.json',
            f'postmortem/daily_review_gate_{day}.json',
            f'postmortem/rule_lifecycle_dashboard_{day}.json',
            f'postmortem/false_positive_negative_{day}.json',
            f'postmortem/replay_confidence_{day}.json',
            f'postmortem/NOW_STATUS_{day}.json',
            f'postmortem/market_context_scoreboard_{day}.json',
            f'postmortem/strategy_conclusion_gate_{day}.json',
            f'postmortem/why_no_trade_{day}.json',
            f'postmortem/trade_alerts_{day}.json',
            f'postmortem/known_risks_{day}.json',
            'postmortem/promotion_queue.json',
        ],
    }


def write_command_menu(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _next_weekday(_today())
    payload = build_command_menu(day)
    path = os.path.join(OUT_DIR, f'monday_command_menu_{day}.json')
    txt_path = os.path.join(HERE, 'MONDAY_COMMAND_MENU.txt')
    lines = [f'Monday command menu for {day}', '=' * 40, '']
    for row in payload['commands']:
        lines.append(f"{row['when']}: {row['command']}")
    lines.append('')
    lines.append('Files to inspect first:')
    lines.extend(f"- {p}" for p in payload['files_to_inspect_first'])
    _write_text(txt_path, '\n'.join(lines) + '\n')
    _write_json(path, payload)
    return path, payload


def build_ready_digest(day: Optional[str] = None) -> dict:
    day = day or _next_weekday(_today())
    status, status_error = _status()
    checklist = build_premarket_checklist(day)
    broker = build_broker_safety_snapshot(day, label='ready_digest')
    config_watch = build_config_change_watch(day)
    alerts = build_trade_alerts(day)
    feed = build_feed_health_score(day)
    execution = build_broker_execution_score(day)
    era = build_era_scorecard(day)
    monday = build_monday_scorecard(day)
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'bot_process_reachable': status_error is None,
        'bot_running': status.get('running'),
        'in_window': status.get('in_window'),
        'broker_flat': broker.get('verdict', {}).get('flat_watched'),
        'no_open_orders': broker.get('verdict', {}).get('no_watched_open_orders'),
        'config_frozen': os.path.exists(os.path.join(OUT_DIR, f'config_freeze_{day}_pre_open.json')),
        'config_changed_since_freeze': config_watch.get('changed_since_freeze'),
        'no_surprises_checklist_ok': checklist.get('ok'),
        'passive_alert_count': alerts.get('alert_count'),
        'feed_health_score': feed.get('score'),
        'broker_execution_score': execution.get('score'),
        'era': era.get('current_era'),
        'monday_scorecard_verdict': monday.get('verdict'),
        'what_to_run_at_0835_ct': 'python smoke_check.py --mode post-open',
        'what_to_watch_first': [
            f'postmortem/MONDAY_REVIEW_START_HERE_{day}.json',
            f'postmortem/trade_alerts_{day}.json',
            f'postmortem/why_no_trade_{day}.json',
            f'postmortem/market_open_monitor_{day}.json',
            f'postmortem/first_hour_review_{day}.json',
            f'postmortem/world_class_dashboard_{day}.json',
            f'postmortem/feed_health_score_{day}.json',
            f'postmortem/broker_execution_score_{day}.json',
            f'postmortem/monday_scorecard_{day}.json',
            f'postmortem/ws_scalp_replay_{day}.json',
            f'postmortem/current_engine_regrade_{day}.json',
            f'postmortem/regime_specific_scorecards_{day}.json',
            f'postmortem/near_miss_winners_{day}.json',
            f'postmortem/exit_quality_score_{day}.json',
            f'postmortem/daily_review_gate_{day}.json',
            f'postmortem/rule_lifecycle_dashboard_{day}.json',
            f'postmortem/false_positive_negative_{day}.json',
            f'postmortem/replay_confidence_{day}.json',
            f'postmortem/NOW_STATUS_{day}.json',
            f'postmortem/market_context_scoreboard_{day}.json',
            f'postmortem/strategy_conclusion_gate_{day}.json',
        ],
    }


def write_ready_digest(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _next_weekday(_today())
    payload = build_ready_digest(day)
    txt = [
        f'MONDAY READY DIGEST - {day}',
        '=' * 44,
        f"Bot status reachable: {payload['bot_process_reachable']}",
        f"Bot running now: {payload['bot_running']} (in_window={payload['in_window']})",
        f"Broker flat: {payload['broker_flat']}",
        f"No watched open orders: {payload['no_open_orders']}",
        f"Config frozen: {payload['config_frozen']}",
        f"Config changed since freeze: {payload['config_changed_since_freeze']}",
        f"No-surprises checklist OK: {payload['no_surprises_checklist_ok']}",
        f"Passive alert count: {payload['passive_alert_count']}",
        f"Feed health score: {payload['feed_health_score']}",
        f"Broker execution score: {payload['broker_execution_score']}",
        f"Era: {payload['era']}",
        f"Monday scorecard verdict: {payload['monday_scorecard_verdict']}",
        '',
        f"Run at 08:35 CT: {payload['what_to_run_at_0835_ct']}",
        '',
        'Watch first:',
        *[f"- {p}" for p in payload['what_to_watch_first']],
        '',
        'Strategy note: do not change live rules before Monday data unless there is a safety issue.',
    ]
    txt_path = os.path.join(HERE, 'MONDAY_READY.txt')
    json_path = os.path.join(OUT_DIR, f'monday_ready_{day}.json')
    _write_text(txt_path, '\n'.join(txt) + '\n')
    _write_json(json_path, payload)
    return json_path, payload


def archive_duplicate_artifacts(day: Optional[str] = None, dry_run: bool = True) -> dict:
    day = day or _today()
    archive_dir = os.path.join(OUT_DIR, 'archive', day)
    candidates = []
    snapshot_dir = os.path.join(OUT_DIR, 'broker_safety_snapshots')
    if os.path.isdir(snapshot_dir):
        files = [
            os.path.join(snapshot_dir, name)
            for name in os.listdir(snapshot_dir)
            if name.startswith(f'broker_safety_{day}_weekend_readiness_')
        ]
        files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        candidates.extend(files[1:])
    checkpoint_files = [
        os.path.join(OUT_DIR, name)
        for name in os.listdir(OUT_DIR)
        if name.startswith(f'session_checkpoint_{day}_') and name.endswith('.json')
        and not name.endswith('_manual.json')
    ]
    checkpoint_files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    candidates.extend(checkpoint_files[3:])
    moved = []
    if not dry_run:
        os.makedirs(archive_dir, exist_ok=True)
        for path in candidates:
            dest = os.path.join(archive_dir, os.path.basename(path))
            shutil.move(path, dest)
            moved.append({'from': path, 'to': dest})
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'dry_run': dry_run,
        'candidate_count': len(candidates),
        'candidates': candidates,
        'moved': moved,
        'policy': 'Archives duplicate generated snapshots/checkpoints only. Core postmortems, audit logs, config, and reports are never moved.',
    }


def write_archive_plan(day: Optional[str] = None, dry_run: bool = True) -> tuple[str, dict]:
    day = day or _today()
    payload = archive_duplicate_artifacts(day, dry_run=dry_run)
    path = os.path.join(OUT_DIR, f'artifact_archive_plan_{day}.json')
    return _write_json(path, payload), payload


def setup_monday(day: Optional[str] = None, overwrite_notes: bool = False) -> tuple[dict, dict]:
    day = day or _next_weekday(_today())
    paths = {}
    payloads = {}
    for key, fn in (
        ('notes', lambda d: write_monday_notes(d, overwrite=overwrite_notes)),
        ('config_freeze', write_config_freeze),
        ('out_of_sample_boundary', write_out_of_sample_boundary),
        ('known_risks', write_known_risks),
        ('command_menu', write_command_menu),
        ('ready_digest', write_ready_digest),
        ('archive_plan', lambda d: write_archive_plan(_today(), dry_run=True)),
    ):
        path, payload = fn(day)
        paths[key] = path
        payloads[key] = payload
    return paths, payloads


def main() -> int:
    parser = argparse.ArgumentParser(description='Monday operator helpers.')
    sub = parser.add_subparsers(dest='cmd')
    setup = sub.add_parser('setup')
    setup.add_argument('day', nargs='?', default=None)
    setup.add_argument('--overwrite-notes', action='store_true')
    menu = sub.add_parser('menu')
    menu.add_argument('day', nargs='?', default=None)
    ready = sub.add_parser('ready')
    ready.add_argument('day', nargs='?', default=None)
    notes = sub.add_parser('notes')
    notes.add_argument('day', nargs='?', default=None)
    notes.add_argument('--overwrite', action='store_true')
    archive = sub.add_parser('archive-plan')
    archive.add_argument('day', nargs='?', default=None)
    archive.add_argument('--apply', action='store_true')
    parser.add_argument('--json', action='store_true')
    args = parser.parse_args()

    if args.cmd == 'menu':
        paths, payload = write_command_menu(args.day)
        out = {'paths': {'command_menu': paths}, 'payload': payload}
    elif args.cmd == 'ready':
        path, payload = write_ready_digest(args.day)
        out = {'paths': {'ready_digest': path}, 'payload': payload}
    elif args.cmd == 'notes':
        path, payload = write_monday_notes(args.day, overwrite=args.overwrite)
        out = {'paths': {'notes': path}, 'payload': payload}
    elif args.cmd == 'archive-plan':
        path, payload = write_archive_plan(args.day, dry_run=not args.apply)
        out = {'paths': {'archive_plan': path}, 'payload': payload}
    else:
        paths, payloads = setup_monday(
            getattr(args, 'day', None),
            overwrite_notes=getattr(args, 'overwrite_notes', False),
        )
        out = {'paths': paths, 'payloads': payloads}

    if args.json:
        print(json.dumps(out, indent=2, default=str))
    else:
        for path in out.get('paths', {}).values():
            print(path)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
