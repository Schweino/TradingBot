"""Golden parity checks for Step 2 and Live/Mock execution artifacts.

This module is intentionally file-based and deterministic. It does not submit
orders, start services, or fetch data. It compares already-produced artifacts so
promotion checks can fail fast when Live/Mock and Step 2 drift.
"""
from __future__ import annotations

from output_paths import output_path

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import execution_kernel
import canonical_decision_packet


HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = output_path("postmortem")
CT = ZoneInfo("America/Chicago")
DEFAULT_TICKERS = ("CLSK", "MARA", "RIOT")
DEFAULT_START_BALANCE = 100000.0
SCHEMA_VERSION = 1


@dataclass(frozen=True)
class GoldenExpectation:
    day: str
    step2_pnl: float | None = None
    step2_trades: int | None = None
    step2_wins: int | None = None
    step2_losses: int | None = None
    live_signal_pnl: float | None = None
    live_signal_entered: int | None = None
    live_signal_skipped: int | None = None


DEFAULT_GOLDENS = [
    # 2026-05-08 is a known ugly parity day. Exact metrics are intentionally
    # optional because the architecture is still moving; freeze them in
    # postmortem/golden_parity/golden_expectations.json when a baseline should
    # become immutable.
    GoldenExpectation(
        day="2026-05-08",
    ),
]


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
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
    os.replace(tmp, path)
    return os.path.abspath(path)


def _step2_score_path(day: str) -> str:
    return os.path.join(OUT_DIR, "backtests", "step2_today_compiled", f"step2_today_compiled_{day}.json")


def _live_signal_summary_path(day: str) -> str:
    return os.path.join(OUT_DIR, "step2_decision_parity", f"step2_decision_parity_{day}.summary.json")


def _artifact_registry_path(day: str) -> str:
    return os.path.join(OUT_DIR, "artifact_registry", f"artifact_registry_{day}.json")


def _execution_lifecycle_summary_path(day: str) -> str:
    return os.path.join(OUT_DIR, "execution_lifecycle", day, f"execution_lifecycle_{day}.summary.json")


def _contract_gate_path(day: str) -> str:
    return os.path.join(OUT_DIR, "contract_gate", f"contract_gate_{day}.json")


def _canonical_packet_diff_path(day: str) -> str:
    return os.path.join(OUT_DIR, "canonical_decision_packets", day, f"canonical_packet_diff_{day}.json")


def _expectations_path() -> str:
    return os.path.join(OUT_DIR, "golden_parity", "golden_expectations.json")


def _load_frozen_expectations() -> dict[str, GoldenExpectation]:
    payload = _read_json(_expectations_path(), {}) or {}
    out: dict[str, GoldenExpectation] = {}
    for day, row in (payload.get("days") or {}).items():
        if not isinstance(row, dict):
            continue
        out[str(day)] = GoldenExpectation(
            day=str(day),
            step2_pnl=row.get("step2_pnl"),
            step2_trades=row.get("step2_trades"),
            step2_wins=row.get("step2_wins"),
            step2_losses=row.get("step2_losses"),
            live_signal_pnl=row.get("live_signal_pnl"),
            live_signal_entered=row.get("live_signal_entered"),
            live_signal_skipped=row.get("live_signal_skipped"),
        )
    return out


def _metric_check(name: str, actual: Any, expected: Any, tolerance: float = 0.0) -> dict[str, Any]:
    if expected is None:
        return {"name": name, "ok": True, "actual": actual, "expected": expected, "skipped": True}
    if actual is None:
        ok = False
    elif isinstance(expected, float):
        ok = abs(float(actual) - float(expected)) <= float(tolerance)
    else:
        ok = actual == expected
    return {
        "name": name,
        "ok": bool(ok),
        "actual": actual,
        "expected": expected,
        "tolerance": tolerance,
    }


def _step2_metrics(day: str) -> dict[str, Any]:
    payload = _read_json(_step2_score_path(day), {}) or {}
    score = payload.get("score") or {}
    result = score.get("result") or {}
    return {
        "path": _step2_score_path(day),
        "exists": os.path.exists(_step2_score_path(day)),
        "served_by": score.get("served_by"),
        "pnl": result.get("pnl"),
        "trades": result.get("trades"),
        "wins": result.get("wins"),
        "losses": result.get("losses"),
        "execution_kernel_hash": score.get("execution_kernel_hash"),
        "step2_parity_contract_hash": score.get("step2_parity_contract_hash"),
    }


def _live_signal_metrics(day: str) -> dict[str, Any]:
    payload = _read_json(_live_signal_summary_path(day), {}) or {}
    return {
        "path": _live_signal_summary_path(day),
        "exists": os.path.exists(_live_signal_summary_path(day)),
        "pnl": payload.get("pnl"),
        "entered": payload.get("entered"),
        "skipped": payload.get("skipped"),
        "rows": payload.get("rows"),
        "execution_kernel_hash": ((payload.get("run_context") or {}).get("execution_kernel_hash")),
    }


def _registry_metrics(day: str) -> dict[str, Any]:
    payload = _read_json(_artifact_registry_path(day), {}) or {}
    contracts = payload.get("contracts") or {}
    return {
        "path": _artifact_registry_path(day),
        "exists": os.path.exists(_artifact_registry_path(day)),
        "registry_hash": payload.get("registry_hash"),
        "execution_kernel_hash": contracts.get("execution_kernel_hash"),
    }


def _lifecycle_metrics(day: str) -> dict[str, Any]:
    payload = _read_json(_execution_lifecycle_summary_path(day), {}) or {}
    return {
        "path": _execution_lifecycle_summary_path(day),
        "exists": os.path.exists(_execution_lifecycle_summary_path(day)),
        "rows": payload.get("rows"),
        "open_trade_count": payload.get("open_trade_count"),
        "anomaly_count": len(payload.get("anomalies") or []),
        "authoritative_state_hash": ((payload.get("authoritative_state") or {}).get("state_hash")),
    }


def _contract_gate_metrics(day: str) -> dict[str, Any]:
    payload = _read_json(_contract_gate_path(day), {}) or {}
    return {
        "path": _contract_gate_path(day),
        "exists": os.path.exists(_contract_gate_path(day)),
        "ok": payload.get("ok"),
        "critical_failure_count": payload.get("critical_failure_count"),
    }


def _canonical_packet_metrics(day: str) -> dict[str, Any]:
    try:
        payload = canonical_decision_packet.diff_day(day)
    except Exception:
        payload = _read_json(_canonical_packet_diff_path(day), {}) or {}
    schema = payload.get("schema_compatibility") or {}
    counts = schema.get("counts") or {}
    return {
        "path": payload.get("path") or _canonical_packet_diff_path(day),
        "exists": os.path.exists(payload.get("path") or _canonical_packet_diff_path(day)),
        "ok": schema.get("ok"),
        "step2_packets": counts.get("step2_decision_packets"),
        "live_packets": counts.get("live_decision_packets"),
        "mismatch_count": payload.get("mismatch_count"),
    }


def check_day(expectation: GoldenExpectation, pnl_tolerance: float = 0.01) -> dict[str, Any]:
    step2 = _step2_metrics(expectation.day)
    live_signal = _live_signal_metrics(expectation.day)
    registry = _registry_metrics(expectation.day)
    lifecycle = _lifecycle_metrics(expectation.day)
    contract = _contract_gate_metrics(expectation.day)
    packets = _canonical_packet_metrics(expectation.day)
    current_kernel = execution_kernel.contract_from_config({}).get("execution_kernel_hash")
    checks = [
        _metric_check("step2_artifact_exists", step2["exists"], True),
        _metric_check("step2_pnl", step2["pnl"], expectation.step2_pnl, pnl_tolerance),
        _metric_check("step2_trades", step2["trades"], expectation.step2_trades),
        _metric_check("step2_wins", step2["wins"], expectation.step2_wins),
        _metric_check("step2_losses", step2["losses"], expectation.step2_losses),
        _metric_check("live_signal_artifact_exists", live_signal["exists"], True),
        _metric_check("live_signal_pnl", live_signal["pnl"], expectation.live_signal_pnl, pnl_tolerance),
        _metric_check("live_signal_entered", live_signal["entered"], expectation.live_signal_entered),
        _metric_check("live_signal_skipped", live_signal["skipped"], expectation.live_signal_skipped),
        _metric_check("artifact_registry_exists", registry["exists"], True),
        _metric_check("execution_lifecycle_summary_exists", lifecycle["exists"], True),
        _metric_check("execution_lifecycle_no_anomalies", lifecycle["anomaly_count"], 0),
        _metric_check("execution_lifecycle_no_open_trades", lifecycle["open_trade_count"], 0),
        _metric_check("execution_lifecycle_state_hash_present", bool(lifecycle["authoritative_state_hash"]), True),
        _metric_check("contract_gate_report_exists", contract["exists"], True),
        _metric_check("contract_gate_ok", contract["ok"], True),
        _metric_check("canonical_packet_diff_exists", packets["exists"], True),
        _metric_check("canonical_packet_schema_ok", packets["ok"], True),
        _metric_check("step2_kernel_hash_present", bool(step2.get("execution_kernel_hash")), True),
        _metric_check("registry_kernel_hash_present", bool(registry.get("execution_kernel_hash")), True),
    ]
    if step2.get("execution_kernel_hash") and registry.get("execution_kernel_hash"):
        checks.append(_metric_check(
            "step2_registry_kernel_hash_match",
            step2.get("execution_kernel_hash"),
            registry.get("execution_kernel_hash"),
        ))
    if live_signal.get("execution_kernel_hash") and registry.get("execution_kernel_hash"):
        checks.append(_metric_check(
            "live_signal_registry_kernel_hash_match",
            live_signal.get("execution_kernel_hash"),
            registry.get("execution_kernel_hash"),
        ))
    return {
        "day": expectation.day,
        "ok": all(bool(c.get("ok")) for c in checks),
        "checks": checks,
        "step2": step2,
        "live_signal": live_signal,
        "artifact_registry": registry,
        "execution_lifecycle": lifecycle,
        "contract_gate": contract,
        "canonical_decision_packets": packets,
        "current_default_execution_kernel_hash": current_kernel,
    }


def run(days: list[str] | None = None, pnl_tolerance: float = 0.01) -> dict[str, Any]:
    selected = set(days or [])
    frozen = _load_frozen_expectations()
    defaults = {g.day: g for g in DEFAULT_GOLDENS}
    defaults.update(frozen)
    if selected:
        expectations = [defaults.get(day) or GoldenExpectation(day=day) for day in sorted(selected)]
    else:
        expectations = list(defaults.values())
    results = [check_day(g, pnl_tolerance=pnl_tolerance) for g in expectations]
    return {
        "schema_version": SCHEMA_VERSION,
        "created_at_ct": _now_ct(),
        "days": [r["day"] for r in results],
        "ok": all(bool(r.get("ok")) for r in results),
        "results": results,
        "deduction": (
            "Golden parity checks compare already-produced artifacts. They are a promotion "
            "safety rail, not a replacement for producing fresh Step 2 artifacts."
        ),
    }


def write(days: list[str] | None = None, out_dir: str | None = None,
          pnl_tolerance: float = 0.01) -> str:
    payload = run(days=days, pnl_tolerance=pnl_tolerance)
    label = "all" if not days else "-".join(days)
    out = os.path.join(out_dir or os.path.join(OUT_DIR, "golden_parity"), f"golden_parity_{label}.json")
    payload["path"] = _write_json(out, payload)
    _write_json(out, payload)
    return payload["path"]


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate golden Step 2/Live parity artifacts.")
    ap.add_argument("--days", nargs="*", default=[])
    ap.add_argument("--out-dir", default="")
    ap.add_argument("--pnl-tolerance", type=float, default=0.01)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    payload = run(days=args.days, pnl_tolerance=args.pnl_tolerance)
    if args.out_dir:
        payload["path"] = write(args.days, out_dir=args.out_dir, pnl_tolerance=args.pnl_tolerance)
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
