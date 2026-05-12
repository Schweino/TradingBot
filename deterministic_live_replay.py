"""Deterministic replay gate for Live decision logs.

This is intentionally log-first. It answers a narrow architecture question:
given the rows Live actually wrote, can the current parity/reducer stack replay
the decisions and lifecycle without contract drift or unexplained mismatches?
"""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import contract_gate
import execution_lifecycle
import intraday_shadow_step2


HERE = Path(__file__).resolve().parent
POSTMORTEM_DIR = HERE / "postmortem"
OUT_DIR = POSTMORTEM_DIR / "deterministic_live_replay"
LIVE_SIGNAL_DIR = POSTMORTEM_DIR / "live_signal_parity"
CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1


def _write_json(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.{int(time.time() * 1000)}.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
        f.write("\n")
    os.replace(tmp, path)
    return str(path.resolve())


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows


def report_path(day: str) -> Path:
    return OUT_DIR / f"deterministic_live_replay_{day}.json"


def build(day: str, write: bool = True) -> dict[str, Any]:
    live_path = LIVE_SIGNAL_DIR / f"live_signal_parity_{day}.jsonl"
    live_rows = _read_jsonl(live_path)
    replay_rows = [intraday_shadow_step2.compare_row(row) for row in live_rows]
    decision_mismatches = [row for row in replay_rows if row.get("evaluable") and not row.get("decision_match")]
    reason_warnings = [
        row for row in replay_rows
        if row.get("evaluable") and row.get("decision_match") and not row.get("reason_match")
    ]
    lifecycle = execution_lifecycle.replay_day(day)
    gate = contract_gate.check(day=day)
    reducer = lifecycle.get("authoritative_state") or {}
    blockers = []
    if not live_path.exists():
        blockers.append("live_signal_parity_missing")
    if int(gate.get("critical_failure_count") or 0) > 0:
        blockers.append("contract_gate_critical_failure")
    if decision_mismatches:
        blockers.append("decision_mismatches")
    if lifecycle.get("anomalies"):
        blockers.append("lifecycle_anomalies")
    if int(lifecycle.get("open_trade_count") or 0) > 0:
        blockers.append("lifecycle_open_trades")
    if not reducer.get("state_hash"):
        blockers.append("reducer_state_hash_missing")
    payload = {
        "schema_version": SCHEMA_VERSION,
        "source": "deterministic_live_replay",
        "created_at_ct": datetime.now(CT).isoformat(timespec="seconds"),
        "day": day,
        "ok": not blockers,
        "blockers": blockers,
        "paths": {
            "live_signal_parity": str(live_path.resolve()),
            "report": str(report_path(day).resolve()),
        },
        "live_signal_rows": len(live_rows),
        "evaluable_rows": sum(1 for row in replay_rows if row.get("evaluable")),
        "decision_mismatch_count": len(decision_mismatches),
        "reason_warning_count": len(reason_warnings),
        "sample_decision_mismatches": decision_mismatches[:20],
        "sample_reason_warnings": reason_warnings[:20],
        "contract_gate": {
            "ok": gate.get("ok"),
            "critical_failure_count": gate.get("critical_failure_count"),
            "hashes": gate.get("hashes"),
        },
        "execution_lifecycle": {
            "rows": lifecycle.get("rows"),
            "trade_count": lifecycle.get("trade_count"),
            "open_trade_count": lifecycle.get("open_trade_count"),
            "anomaly_count": len(lifecycle.get("anomalies") or []),
            "anomalies": (lifecycle.get("anomalies") or [])[:20],
            "authoritative_state": reducer,
        },
        "deduction": (
            "This gate replays the decisions Live already logged. It does not discover new signals; "
            "it proves the logged day can be deterministically explained by the current parity and lifecycle contracts."
        ),
    }
    if write:
        payload["path"] = _write_json(report_path(day), payload)
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description="Replay Live decision logs deterministically.")
    ap.add_argument("day")
    ap.add_argument("--no-write", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    payload = build(args.day, write=not args.no_write)
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0 if payload.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
