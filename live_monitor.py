from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, time as dt_time

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from weekend_readiness import (
    write_broker_execution_score,
    write_config_change_watch,
    write_config_intent_ledger,
    write_current_engine_regrade,
    write_daily_review_gate,
    write_decision_audit_summary,
    write_era_scorecard,
    write_exit_quality_score,
    write_false_positive_negative_tables,
    write_feed_health_score,
    write_market_open_monitor,
    write_monday_scorecard,
    write_monday_launch_checklist,
    write_now_status,
    write_replay_confidence,
    write_monday_live_review,
    write_first_hour_review,
    write_market_context_scoreboard,
    write_review_start_here,
    write_rule_candidate_quarantine,
    write_rule_lifecycle_dashboard,
    write_regime_specific_scorecards,
    write_session_checkpoint,
    write_strategy_conclusion_gate,
    write_trade_thesis_timeline_artifact,
    write_trade_alerts,
    write_winner_quality_artifact,
    write_world_class_dashboard,
    write_ws_scalp_replay,
    write_near_miss_winners,
    write_why_no_trade_summary,
)

import intraday_parity_sentinel


CT = ZoneInfo('America/Chicago')
HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_AUTO_EXIT_CT = '15:20'


def _cleanup_stale_monitor_loops(day: str) -> list[int]:
    """Stop older date-specific live_monitor loops before this loop starts."""
    if os.name != 'nt':
        return []
    killed: list[int] = []
    current_pid = os.getpid()
    try:
        proc = subprocess.run(
            [
                'powershell',
                '-NoProfile',
                '-Command',
                (
                    "Get-CimInstance Win32_Process -Filter \"name = 'python.exe'\" | "
                    "Select-Object ProcessId,CommandLine | ConvertTo-Json -Compress"
                ),
            ],
            cwd=HERE,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=10,
        )
        if proc.returncode != 0 or not proc.stdout.strip():
            return []
        rows = json.loads(proc.stdout)
        if isinstance(rows, dict):
            rows = [rows]
        for row in rows or []:
            pid = int(row.get('ProcessId') or 0)
            cmd = str(row.get('CommandLine') or '')
            if pid == current_pid or not pid:
                continue
            if 'live_monitor.py' not in cmd or '--loop' not in cmd:
                continue
            if f'live_monitor.py {day} ' in cmd or f'live_monitor.py" {day} ' in cmd:
                continue
            os.kill(pid, signal.SIGTERM)
            killed.append(pid)
    except Exception:
        return killed
    return killed


def _today() -> str:
    return datetime.now(CT).date().isoformat()


def _parse_hhmm(value: str) -> dt_time:
    hour_s, minute_s = str(value or DEFAULT_AUTO_EXIT_CT).split(':', 1)
    return dt_time(int(hour_s), int(minute_s))


def write_once(day: str) -> dict:
    paths = {}
    for key, writer in (
        ('market_open_monitor', write_market_open_monitor),
        ('why_no_trade', write_why_no_trade_summary),
        ('trade_alerts', write_trade_alerts),
        ('config_change_watch', write_config_change_watch),
        ('first_hour_review', write_first_hour_review),
        ('strategy_conclusion_gate', write_strategy_conclusion_gate),
        ('market_context_scoreboard', write_market_context_scoreboard),
        ('trade_thesis_timelines', write_trade_thesis_timeline_artifact),
        ('winner_quality', write_winner_quality_artifact),
        ('rule_candidate_quarantine', write_rule_candidate_quarantine),
        ('feed_health_score', write_feed_health_score),
        ('broker_execution_score', write_broker_execution_score),
        ('era_scorecard', write_era_scorecard),
        ('monday_scorecard', write_monday_scorecard),
        ('ws_scalp_replay', write_ws_scalp_replay),
        ('decision_audit_summary', write_decision_audit_summary),
        ('current_engine_regrade', write_current_engine_regrade),
        ('regime_specific_scorecards', write_regime_specific_scorecards),
        ('near_miss_winners', write_near_miss_winners),
        ('exit_quality_score', write_exit_quality_score),
        ('replay_confidence', write_replay_confidence),
        ('false_positive_negative', write_false_positive_negative_tables),
        ('daily_review_gate', write_daily_review_gate),
        ('rule_lifecycle_dashboard', write_rule_lifecycle_dashboard),
        ('now_status', write_now_status),
        ('monday_launch_checklist', write_monday_launch_checklist),
        ('config_intent_ledger', write_config_intent_ledger),
        ('world_class_dashboard', write_world_class_dashboard),
        ('monday_live_review', write_monday_live_review),
        ('review_start_here', write_review_start_here),
    ):
        path, _payload = writer(day)
        paths[key] = path
    return {'day': day, 'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'), 'paths': paths}


def main() -> int:
    ap = argparse.ArgumentParser(description='Refresh compact live review artifacts.')
    ap.add_argument('day', nargs='?', default=_today())
    ap.add_argument('--loop', action='store_true', help='Refresh until stopped.')
    ap.add_argument('--interval-sec', type=int, default=30)
    ap.add_argument('--checkpoint', action='store_true', help='Also write a session checkpoint each refresh.')
    ap.add_argument('--checkpoint-label', default='live_monitor')
    ap.add_argument('--cleanup-stale-loops', action='store_true',
                    help='Stop older date-specific live_monitor --loop processes before starting this loop.')
    ap.add_argument('--auto-exit-after-ct', default=DEFAULT_AUTO_EXIT_CT,
                    help='In --loop mode, exit once this CT wall clock time is reached. Use off to disable.')
    ap.add_argument('--parity-sentinel', action='store_true',
                    help='Run the Live-vs-Step2 parity sentinel inside the monitor loop.')
    ap.add_argument('--parity-sentinel-interval-sec', type=int, default=300)
    ap.add_argument('--parity-sentinel-no-refresh', action='store_true',
                    help='Use the last parity report instead of refreshing the fast parity inputs first.')
    ap.add_argument('--json', action='store_true')
    args = ap.parse_args()
    if args.loop and args.cleanup_stale_loops:
        killed = _cleanup_stale_monitor_loops(args.day)
        if killed:
            print(f"stopped stale live monitor loop pids: {','.join(str(pid) for pid in killed)}")
    last_sentinel_at = 0.0
    while True:
        payload = write_once(args.day)
        if args.checkpoint:
            path, _ = write_session_checkpoint(args.day, label=args.checkpoint_label)
            payload['paths']['session_checkpoint'] = path
        if args.parity_sentinel and time.time() - last_sentinel_at >= max(30, args.parity_sentinel_interval_sec):
            try:
                sentinel = intraday_parity_sentinel.check_once(
                    args.day,
                    refresh_report=not args.parity_sentinel_no_refresh,
                )
                payload['paths']['parity_sentinel'] = sentinel.get('output', {}).get('latest_path')
                payload['parity_sentinel'] = {
                    'severity': sentinel.get('severity'),
                    'ok': sentinel.get('ok'),
                    'alerts': sentinel.get('alerts', [])[:5],
                    'metrics': sentinel.get('metrics'),
                    'elapsed_sec': sentinel.get('elapsed_sec'),
                }
            except Exception as exc:
                payload['parity_sentinel'] = {
                    'severity': 'critical',
                    'ok': False,
                    'error': repr(exc),
                }
            last_sentinel_at = time.time()
        if args.json:
            print(json.dumps(payload, indent=2, default=str))
        else:
            print(f"live monitor refreshed {payload['created_at_ct']}")
            for path in payload['paths'].values():
                print(path)
            if payload.get('parity_sentinel'):
                ps = payload['parity_sentinel']
                print(f"parity sentinel severity={ps.get('severity')} ok={ps.get('ok')} elapsed={ps.get('elapsed_sec')}")
        if not args.loop:
            return 0
        if str(args.auto_exit_after_ct).lower() not in ('off', 'false', '0', 'none'):
            cutoff = _parse_hhmm(args.auto_exit_after_ct)
            now = datetime.now(CT)
            if now.date().isoformat() >= str(args.day) and now.time() >= cutoff:
                print(f"live monitor auto-exit {now.isoformat(timespec='seconds')} cutoff={args.auto_exit_after_ct}")
                return 0
        time.sleep(max(5, args.interval_sec))


if __name__ == '__main__':
    raise SystemExit(main())
