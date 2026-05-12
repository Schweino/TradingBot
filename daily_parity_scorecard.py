"""Compact daily scorecard for Step 2 vs Live parity.

The full parity report is intentionally broad. This module produces the daily
executive artifact: P/L gap, trade-count gap, root-cause buckets, and the next
investigation queue.
"""
from __future__ import annotations

from output_paths import output_path

import argparse
import json
import os
import time
from collections import Counter
from datetime import datetime
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import live_step2_parity_report


HERE = os.path.dirname(os.path.abspath(__file__))
POSTMORTEM_DIR = output_path("postmortem")
OUT_DIR = os.path.join(POSTMORTEM_DIR, "daily_parity_scorecard")
CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


def _read_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except Exception:
        return default


def _write_json(path: str, payload: dict[str, Any]) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.{os.getpid()}.{int(time.time() * 1000)}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
        f.write("\n")
    os.replace(tmp, path)
    return os.path.abspath(path)


def _write_text(path: str, text: str) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.{os.getpid()}.{int(time.time() * 1000)}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)
    return os.path.abspath(path)


def output_paths(day: str) -> dict[str, str]:
    return {
        "json": os.path.join(OUT_DIR, f"daily_parity_scorecard_{day}.json"),
        "txt": os.path.join(OUT_DIR, f"daily_parity_scorecard_{day}.txt"),
    }


def _report_path(day: str) -> str:
    return os.path.join(POSTMORTEM_DIR, "live_step2_parity", f"live_step2_parity_{day}.json")


def _order_lifecycle_path(day: str) -> str:
    return os.path.join(POSTMORTEM_DIR, "order_lifecycle_reconciliation", f"order_lifecycle_reconciliation_{day}.json")


def _freshness_path(day: str) -> str:
    return os.path.join(POSTMORTEM_DIR, "market_data_freshness", f"market_data_freshness_{day}.json")


def _num(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _bucket(name: str, level: str, count: int, detail: dict[str, Any]) -> dict[str, Any]:
    return {"name": name, "level": level, "count": int(count or 0), "detail": detail}


def evaluate(day: str, report: dict[str, Any], *,
             lifecycle: dict[str, Any] | None = None,
             freshness: dict[str, Any] | None = None) -> dict[str, Any]:
    lifecycle = lifecycle or {}
    freshness = freshness or {}
    summary = report.get("summary") or {}
    trade = report.get("live_trade_results") or {}
    step2 = report.get("step2_decision_parity") or {}
    trust = report.get("trust_gate") or {}
    latency = report.get("latency") or {}
    live_pnl = _num(trade.get("pnl"))
    step2_pnl = _num(step2.get("pnl"))
    pnl_gap = round((step2_pnl or 0.0) - (live_pnl or 0.0), 2) if step2_pnl is not None and live_pnl is not None else None
    live_trades = int(trade.get("trades") or summary.get("trade_rows") or 0)
    step2_trades = int(step2.get("entered") or 0)
    trade_gap = step2_trades - live_trades

    buckets = [
        _bucket("contract_or_profile_drift", "critical", len(trust.get("contract_mismatches") or []), {
            "mismatches": trust.get("contract_mismatches") or [],
        }),
        _bucket("market_data_freshness", "critical" if freshness.get("critical_count") else "warning",
                int(freshness.get("critical_count") or freshness.get("warning_count") or 0), {
                    "verdict": freshness.get("verdict"),
                    "issues": (freshness.get("issues") or [])[:10],
                }),
        _bucket("market_data_integrity", "critical",
                int(summary.get("market_data_integrity_critical_count") or 0), {
                    "verdict": summary.get("market_data_integrity_verdict"),
                    "warnings": summary.get("market_data_integrity_warning_count"),
                }),
        _bucket("order_lifecycle", "critical" if lifecycle.get("critical_count") else "warning",
                int(lifecycle.get("critical_count") or lifecycle.get("warning_count") or 0), {
                    "verdict": lifecycle.get("verdict"),
                    "issues": (lifecycle.get("issues") or [])[:10],
                }),
        _bucket("decision_mismatch", "critical", int(summary.get("decision_mismatch_count") or 0), {}),
        _bucket("outcome_mismatch", "warning", int(summary.get("outcome_mismatch_count") or 0), {}),
        _bucket("missing_trade_join", "warning", int(summary.get("entered_without_trade_row") or 0), {}),
        _bucket("missing_signal_join", "warning", int(summary.get("trades_without_entered_signal_row") or 0), {}),
        _bucket("missing_latency_join", "warning", int(summary.get("trades_without_latency_row") or 0), {}),
        _bucket("step2_only_opportunities", "warning", int(summary.get("step2_only_signal_keys") or 0), {}),
        _bucket("live_only_opportunities", "warning", int(summary.get("live_only_signal_keys") or 0), {}),
    ]
    nonzero = [row for row in buckets if row.get("count")]
    critical_count = sum(row["count"] for row in nonzero if row.get("level") == "critical")
    warning_count = sum(row["count"] for row in nonzero if row.get("level") == "warning")
    verdict = "PARITY_FAIL" if critical_count else ("PARITY_WARN" if warning_count or (pnl_gap and abs(pnl_gap) > 1.0) else "PARITY_OK")
    p95_exit_fill = (((latency.get("exit_time_to_fill_ms") or {}).get("p95_ms")))
    p95_entry_fill = (((latency.get("entry_time_to_fill_ms") or {}).get("p95_ms")))
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "daily_parity_scorecard",
        "day": day,
        "created_at_ct": _now_ct(),
        "ok": verdict != "PARITY_FAIL",
        "verdict": verdict,
        "critical_count": critical_count,
        "warning_count": warning_count,
        "pnl": {
            "live": live_pnl,
            "step2": step2_pnl,
            "step2_minus_live": pnl_gap,
        },
        "trades": {
            "live": live_trades,
            "step2_entered": step2_trades,
            "step2_minus_live": trade_gap,
            "live_wins": trade.get("wins"),
            "live_losses": trade.get("losses"),
        },
        "latency": {
            "entry_fill_p95_ms": p95_entry_fill,
            "exit_fill_p95_ms": p95_exit_fill,
        },
        "root_cause_buckets": nonzero,
        "bucket_counts": dict(sorted(Counter(row["name"] for row in nonzero).items())),
        "investigation_queues": report.get("investigation_queues") or {},
        "paths": {
            "live_step2_parity_report": _report_path(day),
            "order_lifecycle_reconciliation": _order_lifecycle_path(day),
            "market_data_freshness": _freshness_path(day),
        },
        "deduction": (
            "This scorecard is the fast daily answer: whether Step 2 and Live agreed enough to trust "
            "variant work, and which root-cause queue should be inspected first when they did not."
        ),
    }


def build(day: str, write: bool = True, refresh_report: bool = False) -> dict[str, Any]:
    report = live_step2_parity_report.build_and_write(day=day) if refresh_report else (_read_json(_report_path(day), {}) or {})
    if not report:
        report = live_step2_parity_report.build_and_write(day=day)
    lifecycle = _read_json(_order_lifecycle_path(day), {}) or {}
    freshness = _read_json(_freshness_path(day), {}) or {}
    payload = evaluate(day, report, lifecycle=lifecycle, freshness=freshness)
    if write:
        p = output_paths(day)
        payload["path"] = _write_json(p["json"], payload)
        payload["text_path"] = _write_text(p["txt"], render_text(payload))
    return payload


def render_text(payload: dict[str, Any]) -> str:
    lines = [
        f"Daily Parity Scorecard - {payload.get('day')}",
        f"Created CT: {payload.get('created_at_ct')}",
        "",
        "Verdict",
        f"- Verdict: {payload.get('verdict')}",
        f"- OK: {payload.get('ok')}",
        f"- Critical/warnings: {payload.get('critical_count')}/{payload.get('warning_count')}",
        "",
        "P/L and trades",
        f"- P/L Live/Step2/gap: {payload.get('pnl', {}).get('live')}/{payload.get('pnl', {}).get('step2')}/{payload.get('pnl', {}).get('step2_minus_live')}",
        f"- Trades Live/Step2/gap: {payload.get('trades', {}).get('live')}/{payload.get('trades', {}).get('step2_entered')}/{payload.get('trades', {}).get('step2_minus_live')}",
        "",
        "Root causes",
    ]
    for bucket in payload.get("root_cause_buckets") or []:
        lines.append(f"- {bucket.get('level')} {bucket.get('name')}: {bucket.get('count')}")
    if not payload.get("root_cause_buckets"):
        lines.append("- none")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="Build compact daily Step2-vs-Live parity scorecard.")
    ap.add_argument("day")
    ap.add_argument("--refresh-report", action="store_true")
    ap.add_argument("--no-write", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    payload = build(args.day, write=not args.no_write, refresh_report=args.refresh_report)
    print(json.dumps(payload, indent=2, sort_keys=True, default=str) if args.json else render_text(payload))
    return 0 if payload.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
