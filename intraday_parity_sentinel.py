from __future__ import annotations

from output_paths import output_path

import argparse
import json
import os
import time
from datetime import datetime, time as dt_time
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore

import live_signal_step2_parity
import live_step2_feed
import live_step2_parity_report
import intraday_shadow_step2
import execution_lifecycle
import step2_latency_model


HERE = os.path.dirname(os.path.abspath(__file__))
POSTMORTEM_DIR = output_path('postmortem')
OUT_DIR = os.path.join(POSTMORTEM_DIR, 'parity_sentinel')
CT = ZoneInfo('America/Chicago')
DEFAULT_TICKERS = ('CLSK', 'MARA', 'RIOT')
DEFAULT_AUTO_EXIT_CT = '15:20'


def _today() -> str:
    return datetime.now(CT).date().isoformat()


def _read_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return default


def _write_json(path: str, payload: dict[str, Any]) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f'{path}.{os.getpid()}.{int(time.time() * 1000)}.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
    os.replace(tmp, path)
    return os.path.abspath(path)


def _append_jsonl(path: str, payload: dict[str, Any]) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'a', encoding='utf-8') as f:
        f.write(json.dumps(payload, sort_keys=True, separators=(',', ':'), default=str) + '\n')
    return os.path.abspath(path)


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except Exception:
        return default


def _parse_hhmm(value: str) -> dt_time:
    hour_s, minute_s = str(value or DEFAULT_AUTO_EXIT_CT).split(':', 1)
    return dt_time(int(hour_s), int(minute_s))


def _class_gap(diff: dict[str, Any], class_name: str) -> float:
    gaps = diff.get('pnl_gap_by_class') or {}
    return _num(gaps.get(class_name), 0.0)


def _compact_materialize(payload: dict[str, Any] | None) -> dict[str, Any]:
    payload = payload or {}
    incremental = payload.get('incremental_market_store') or {}
    return {
        'prepared_path': payload.get('prepared_path'),
        'manifest_path': payload.get('manifest_path'),
        'rows': payload.get('rows'),
        'live_rows': payload.get('live_rows'),
        'existing_rows': payload.get('existing_rows'),
        'by_kind': payload.get('by_kind'),
        'by_symbol': payload.get('by_symbol'),
        'min_ct': payload.get('min_ct'),
        'max_ct': payload.get('max_ct'),
        'incremental_market_store': {
            'manifest_path': incremental.get('manifest_path'),
            'partition_count': incremental.get('partition_count'),
            'changed_partition_count': incremental.get('changed_partition_count'),
            'changed_partitions': incremental.get('changed_partitions') or [],
            'prepared_exists': incremental.get('prepared_exists'),
            'rows': incremental.get('rows'),
        } if incremental else None,
    }


def refresh_inputs(
    day: str,
    tickers: list[str],
    feed: str,
    quote_mode: str,
    btc_mode: str,
    latency_percentile: str,
) -> dict[str, Any]:
    prepared_dir = live_step2_feed.DEFAULT_PREPARED_DIR
    materialize = live_step2_feed.materialize(
        day=day,
        tickers=tickers,
        feed=feed,
        quote_mode=quote_mode,
        btc_mode=btc_mode,
        prepared_cache_dir=prepared_dir,
    )
    latency_model_path = step2_latency_model.DEFAULT_MODEL_PATH
    step2_summary = live_signal_step2_parity.build(
        day=day,
        tickers=tickers,
        feed=feed,
        quote_mode=quote_mode,
        btc_mode=btc_mode,
        prepared_cache_dir=prepared_dir,
        out_dir=live_signal_step2_parity.OUT_DIR,
        latency_model_path=latency_model_path,
        latency_percentile=latency_percentile,
    )
    report = live_step2_parity_report.build_and_write(day=day)
    return {
        'materialize': materialize,
        'step2_decision_parity': step2_summary,
        'report': report,
    }


def _alert(level: str, code: str, message: str, **details: Any) -> dict[str, Any]:
    return {
        'level': level,
        'code': code,
        'message': message,
        'details': details,
    }


def classify(report: dict[str, Any], warn_pnl_gap: float, critical_pnl_gap: float) -> dict[str, Any]:
    if not report:
        return {
            'severity': 'critical',
            'ok': False,
            'alerts': [_alert(
                'critical',
                'missing_parity_report',
                'No Live-vs-Step2 parity report was available for the sentinel to evaluate.',
            )],
            'metrics': {
                'live_signal_rows': 0,
                'step2_decision_rows': 0,
                'matched_live_step2_keys': 0,
                'decision_mismatch_count': 0,
                'outcome_mismatch_count': 0,
                'live_trade_rows': 0,
                'live_pnl': 0.0,
                'step2_pnl': 0.0,
                'pnl_gap_live_minus_step2': 0.0,
                'diff_counts': {},
                'diff_pnl_gap_by_class': {},
                'intraday_tape_rows': 0,
                'intraday_missing_canonical_rows': 0,
            },
        }
    summary = report.get('summary') or {}
    trust = report.get('trust_gate') or {}
    diff = report.get('parity_diff_classifier') or {}
    diff_counts = diff.get('counts') or {}
    freshness = report.get('step2_data_freshness') or {}
    step2 = report.get('step2_decision_parity') or {}
    live = report.get('live_trade_results') or {}
    alerts: list[dict[str, Any]] = []

    if not trust.get('ok'):
        alerts.append(_alert(
            'warning',
            'contract_or_cache_trust_gate',
            'Live/Step 2 comparison is diagnostic, but contract/cache trust gate is not clean.',
            mismatches=trust.get('contract_mismatches') or [],
            cache_stale_layers=trust.get('cache_stale_layers') or [],
            cache_recommended_action=trust.get('cache_recommended_action'),
        ))

    for key, code, message in (
        ('live_only_signal_keys', 'live_only_signals', 'Live produced signal keys that Step 2 did not reproduce.'),
        ('step2_only_signal_keys', 'step2_only_signals', 'Step 2 produced signal keys that Live did not log.'),
        ('decision_mismatch_count', 'decision_mismatch', 'Live and Step 2 disagreed on enter/skip decisions.'),
        ('trades_without_entered_signal_row', 'trade_without_signal', 'Live trade rows exist without an entered signal row.'),
        ('trades_without_latency_row', 'missing_latency_rows', 'Live trades are missing latency attribution rows.'),
    ):
        count = _int(summary.get(key))
        if count:
            level = 'critical' if key != 'trades_without_latency_row' else 'warning'
            alerts.append(_alert(level, code, message, count=count))

    entered_without_trade = _int(summary.get('entered_without_trade_row'))
    if entered_without_trade:
        alerts.append(_alert(
            'warning',
            'entered_signal_without_trade',
            'Live entered signal rows did not all resolve to trade rows yet.',
            count=entered_without_trade,
        ))

    missing_intraday = _int(freshness.get('missing_from_intraday_rows'))
    if freshness.get('canonical_exists') and missing_intraday:
        alerts.append(_alert(
            'critical',
            'intraday_tape_missing_canonical_rows',
            'The intraday Step 2 tape is missing rows present in the canonical tape.',
            missing_from_intraday_rows=missing_intraday,
            compare_path=freshness.get('compare_path'),
        ))

    same_exit_gap = _class_gap(diff, 'same_entry_different_exit_reason')
    latency_gap = _class_gap(diff, 'same_entry_different_fill_or_latency')
    live_missing_gap = _class_gap(diff, 'live_trade_missing_outcome')
    for code, count_key, gap, message in (
        (
            'same_entry_different_exit_reason',
            'same_entry_different_exit_reason',
            same_exit_gap,
            'Live and Step 2 entered the same trade but resolved different exit reasons.',
        ),
        (
            'same_entry_different_fill_or_latency',
            'same_entry_different_fill_or_latency',
            latency_gap,
            'Live and Step 2 have materially different fill/latency P/L on same-entry trades.',
        ),
        (
            'live_trade_missing_outcome',
            'live_trade_missing_outcome',
            live_missing_gap,
            'Live has trades Step 2 could not simulate to an outcome.',
        ),
    ):
        count = _int(diff_counts.get(count_key))
        if not count:
            continue
        abs_gap = abs(gap)
        if abs_gap >= critical_pnl_gap:
            level = 'critical'
        elif abs_gap >= warn_pnl_gap:
            level = 'warning'
        else:
            level = 'info'
        if level != 'info':
            alerts.append(_alert(level, code, message, count=count, pnl_gap_live_minus_step2=round(gap, 4)))

    outcome_mismatch_count = _int(summary.get('outcome_mismatch_count'))
    if outcome_mismatch_count and not any(a['code'] == 'same_entry_different_exit_reason' for a in alerts):
        alerts.append(_alert(
            'info',
            'outcome_mismatch',
            'Outcome mismatch rows exist; current aggregate P/L gap is below warning threshold.',
            count=outcome_mismatch_count,
        ))

    live_pnl = _num(live.get('pnl'))
    step2_pnl = _num(step2.get('pnl'))
    pnl_gap = live_pnl - step2_pnl
    if abs(pnl_gap) >= critical_pnl_gap:
        alerts.append(_alert(
            'critical',
            'total_pnl_gap',
            'Live total P/L diverged from Step 2 beyond the critical threshold.',
            live_pnl=round(live_pnl, 4),
            step2_pnl=round(step2_pnl, 4),
            gap_live_minus_step2=round(pnl_gap, 4),
        ))
    elif abs(pnl_gap) >= warn_pnl_gap:
        alerts.append(_alert(
            'warning',
            'total_pnl_gap',
            'Live total P/L diverged from Step 2 beyond the warning threshold.',
            live_pnl=round(live_pnl, 4),
            step2_pnl=round(step2_pnl, 4),
            gap_live_minus_step2=round(pnl_gap, 4),
        ))

    severity_rank = {'ok': 0, 'info': 1, 'warning': 2, 'critical': 3}
    max_level = 'ok'
    for alert in alerts:
        if severity_rank.get(alert.get('level'), 0) > severity_rank[max_level]:
            max_level = alert['level']
    return {
        'severity': max_level,
        'ok': max_level in ('ok', 'info'),
        'alerts': alerts,
        'metrics': {
            'live_signal_rows': _int(summary.get('live_signal_rows')),
            'step2_decision_rows': _int(summary.get('step2_decision_rows')),
            'matched_live_step2_keys': _int(summary.get('matched_live_step2_keys')),
            'decision_mismatch_count': _int(summary.get('decision_mismatch_count')),
            'outcome_mismatch_count': outcome_mismatch_count,
            'live_trade_rows': _int(summary.get('trade_rows')),
            'live_pnl': round(live_pnl, 4),
            'step2_pnl': round(step2_pnl, 4),
            'pnl_gap_live_minus_step2': round(pnl_gap, 4),
            'diff_counts': diff_counts,
            'diff_pnl_gap_by_class': diff.get('pnl_gap_by_class') or {},
            'intraday_tape_rows': _int(freshness.get('intraday_rows')),
            'intraday_missing_canonical_rows': missing_intraday,
        },
    }


def check_once(
    day: str,
    refresh_report: bool = False,
    tickers: list[str] | None = None,
    feed: str = 'sip',
    quote_mode: str = 'per-second',
    btc_mode: str = 'bars',
    latency_percentile: str = 'p75',
    warn_pnl_gap: float = 100.0,
    critical_pnl_gap: float = 250.0,
) -> dict[str, Any]:
    tickers = [str(t).upper() for t in (tickers or list(DEFAULT_TICKERS))]
    started = time.time()
    refresh: dict[str, Any] | None = None
    if refresh_report:
        refresh = refresh_inputs(day, tickers, feed, quote_mode, btc_mode, latency_percentile)
        report = refresh.get('report') or {}
    else:
        path = os.path.join(POSTMORTEM_DIR, 'live_step2_parity', f'live_step2_parity_{day}.json')
        report = _read_json(path, {}) or {}
    classification = classify(report, warn_pnl_gap=warn_pnl_gap, critical_pnl_gap=critical_pnl_gap)
    shadow = intraday_shadow_step2.build_day(day)
    lifecycle = execution_lifecycle.write_snapshot(day)
    extra_alerts = []
    if int(shadow.get('critical_count') or 0):
        extra_alerts.append(_alert(
            'critical',
            'intraday_shadow_step2_critical',
            'Immediate Live-vs-shadow Step 2 decisions contain critical mismatches.',
            critical_count=shadow.get('critical_count'),
            warning_count=shadow.get('warning_count'),
            summary_path=shadow.get('summary_path'),
        ))
    elif int(shadow.get('warning_count') or 0):
        extra_alerts.append(_alert(
            'warning',
            'intraday_shadow_step2_warning',
            'Immediate Live-vs-shadow Step 2 decisions contain warning-level mismatches.',
            warning_count=shadow.get('warning_count'),
            summary_path=shadow.get('summary_path'),
        ))
    if lifecycle.get('anomalies'):
        extra_alerts.append(_alert(
            'critical',
            'execution_lifecycle_anomaly',
            'Event-sourced Live execution lifecycle has replay anomalies.',
            anomalies=lifecycle.get('anomalies')[:10],
            summary_path=lifecycle.get('summary_path'),
        ))
    if extra_alerts:
        classification['alerts'].extend(extra_alerts)
        severity_rank = {'ok': 0, 'info': 1, 'warning': 2, 'critical': 3}
        max_level = classification.get('severity') or 'ok'
        for alert in extra_alerts:
            if severity_rank.get(alert.get('level'), 0) > severity_rank.get(max_level, 0):
                max_level = alert.get('level')
        classification['severity'] = max_level
        classification['ok'] = max_level in ('ok', 'info')
    payload = {
        'schema_version': 1,
        'source': 'intraday_parity_sentinel',
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'elapsed_sec': round(time.time() - started, 3),
        'refresh_report': bool(refresh_report),
        'tickers': tickers,
        'feed': feed,
        'quote_mode': quote_mode,
        'btc_mode': btc_mode,
        'latency_percentile': latency_percentile,
        'thresholds': {
            'warn_pnl_gap': warn_pnl_gap,
            'critical_pnl_gap': critical_pnl_gap,
        },
        **classification,
        'intraday_shadow_step2': shadow,
        'execution_lifecycle': lifecycle,
        'report_paths': (report.get('output') or {}),
    }
    if refresh is not None:
        payload['refresh'] = {
            'materialize': _compact_materialize(refresh.get('materialize')),
            'step2_decision_parity': {
                'rows': (refresh.get('step2_decision_parity') or {}).get('rows'),
                'entered': (refresh.get('step2_decision_parity') or {}).get('entered'),
                'skipped': (refresh.get('step2_decision_parity') or {}).get('skipped'),
                'pnl': (refresh.get('step2_decision_parity') or {}).get('pnl'),
                'summary_path': (refresh.get('step2_decision_parity') or {}).get('summary_path'),
            },
        }
    latest_path = os.path.join(OUT_DIR, f'parity_sentinel_{day}.json')
    history_path = os.path.join(OUT_DIR, f'parity_sentinel_{day}.jsonl')
    global_latest_path = os.path.join(OUT_DIR, 'PARITY_SENTINEL_LATEST.json')
    alert_history_path = os.path.join(OUT_DIR, f'parity_alerts_{day}.jsonl')
    payload['output'] = {
        'latest_path': os.path.abspath(latest_path),
        'global_latest_path': os.path.abspath(global_latest_path),
        'history_path': os.path.abspath(history_path),
        'alert_history_path': os.path.abspath(alert_history_path),
    }
    _write_json(latest_path, payload)
    _write_json(global_latest_path, payload)
    _append_jsonl(history_path, payload)
    if payload['alerts']:
        _append_jsonl(alert_history_path, payload)
    return payload


def run_loop(args: argparse.Namespace) -> int:
    cutoff = None if str(args.auto_exit_after_ct).lower() in ('off', 'false', '0', 'none') else _parse_hhmm(args.auto_exit_after_ct)
    while True:
        payload = check_once(
            day=args.day,
            refresh_report=args.refresh_report,
            tickers=args.tickers,
            feed=args.feed,
            quote_mode=args.quote_mode,
            btc_mode=args.btc_mode,
            latency_percentile=args.latency_percentile,
            warn_pnl_gap=args.warn_pnl_gap,
            critical_pnl_gap=args.critical_pnl_gap,
        )
        print(json.dumps({
            'day': payload['day'],
            'created_at_ct': payload['created_at_ct'],
            'severity': payload['severity'],
            'ok': payload['ok'],
            'alerts': payload['alerts'][:5],
            'metrics': payload['metrics'],
            'elapsed_sec': payload['elapsed_sec'],
            'output': payload['output'],
        }, indent=2, sort_keys=True))
        if cutoff is not None:
            now = datetime.now(CT)
            if now.date().isoformat() >= str(args.day) and now.time() >= cutoff:
                return 0
        time.sleep(max(30, int(args.interval_sec or 300)))


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Intraday sentinel for Live vs Step 2 runtime parity drift.')
    ap.add_argument('day', nargs='?', default=_today())
    ap.add_argument('--refresh-report', action='store_true',
                    help='Materialize live tape, rebuild fast Live-signal Step 2 parity, and refresh parity report first.')
    ap.add_argument('--loop', action='store_true')
    ap.add_argument('--interval-sec', type=int, default=300)
    ap.add_argument('--auto-exit-after-ct', default=DEFAULT_AUTO_EXIT_CT)
    ap.add_argument('--tickers', nargs='+', default=list(DEFAULT_TICKERS))
    ap.add_argument('--feed', default='sip')
    ap.add_argument('--quote-mode', default='per-second')
    ap.add_argument('--btc-mode', default='bars')
    ap.add_argument('--latency-percentile', default='p75', choices=['p50', 'p75', 'p95', 'default'])
    ap.add_argument('--warn-pnl-gap', type=float, default=100.0)
    ap.add_argument('--critical-pnl-gap', type=float, default=250.0)
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    if args.loop:
        return run_loop(args)
    payload = check_once(
        day=args.day,
        refresh_report=args.refresh_report,
        tickers=args.tickers,
        feed=args.feed,
        quote_mode=args.quote_mode,
        btc_mode=args.btc_mode,
        latency_percentile=args.latency_percentile,
        warn_pnl_gap=args.warn_pnl_gap,
        critical_pnl_gap=args.critical_pnl_gap,
    )
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
