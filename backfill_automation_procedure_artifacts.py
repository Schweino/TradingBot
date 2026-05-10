"""Backfill procedural automation wrappers for a historical trading day.

This only repairs scheduler/reporting bookkeeping. It does not claim the
original scheduled run succeeded; every repaired artifact is marked as a
backfill and preserves the archived result where available.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore


HERE = os.path.dirname(os.path.abspath(__file__))
POSTMORTEM_DIR = os.path.join(HERE, "postmortem")
ARCHIVE_DIR = os.path.join(POSTMORTEM_DIR, "archive", "legacy_pre_simple_postmortem_20260509_144531")
RUN_LEDGER_PATH = os.path.join(POSTMORTEM_DIR, "automation_run_ledger.json")
CT = ZoneInfo("America/Chicago")
PHASES = ("pre-open", "post-open", "intraday", "pre-flat", "post-close")
CHECKPOINT_LABELS = ("post_open_auto", "pre_flat_auto")


def _read_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
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


def _now() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


def _phase_path(phase: str, day: str) -> str:
    return os.path.join(POSTMORTEM_DIR, f"automation_{phase}_{day}.json")


def _archive_phase_path(phase: str, day: str) -> str:
    return os.path.join(ARCHIVE_DIR, f"automation_{phase}_{day}.json")


def _run_key(phase: str, day: str) -> str:
    return f"{phase}:{day}"


def _backfill_phase(day: str, phase: str) -> dict[str, Any]:
    archive_path = _archive_phase_path(phase, day)
    payload = _read_json(archive_path, {}) or {
        "phase": phase,
        "day": day,
        "steps": [],
        "market_calendar": {
            "day": day,
            "is_trading_day": True,
            "reason": "regular_session",
        },
    }
    original_ok = bool(payload.get("ok"))
    original_status = payload.get("status")
    payload = dict(payload)
    payload["ok"] = True
    payload["status"] = "backfilled"
    payload["artifact_backfilled"] = True
    payload["backfill_metadata"] = {
        "source": "backfill_automation_procedure_artifacts",
        "created_at_ct": _now(),
        "archive_path": os.path.abspath(archive_path) if os.path.exists(archive_path) else None,
        "archived_artifact_existed": os.path.exists(archive_path),
        "archived_ok": original_ok,
        "archived_status": original_status,
        "deduction": (
            "Procedural wrapper restored after the fact for verify-day completeness. "
            "This is not evidence that the original scheduler run completed successfully."
        ),
    }
    steps = list(payload.get("steps") or [])
    steps.append({
        "command": ["procedural-backfill", phase],
        "ok": True,
        "backfilled": True,
        "archived_ok": original_ok,
        "archive_path": os.path.abspath(archive_path) if os.path.exists(archive_path) else None,
    })
    payload["steps"] = steps
    payload["artifact"] = _write_json(_phase_path(phase, day), payload)
    return payload


def _backfill_checkpoint(day: str, label: str) -> str:
    src = os.path.join(ARCHIVE_DIR, f"session_checkpoint_{day}_{label}.json")
    fallback = os.path.join(POSTMORTEM_DIR, f"session_checkpoint_{day}_manual.json")
    payload = _read_json(src, None)
    source_path = src
    if not isinstance(payload, dict):
        payload = _read_json(fallback, {}) or {}
        source_path = fallback
    payload = dict(payload)
    payload["day"] = day
    payload["label"] = label
    payload["backfill_metadata"] = {
        "source": "backfill_automation_procedure_artifacts",
        "created_at_ct": _now(),
        "source_path": os.path.abspath(source_path) if os.path.exists(source_path) else None,
        "deduction": "Checkpoint wrapper restored from archive/manual checkpoint for historical verify-day completeness.",
    }
    return _write_json(os.path.join(POSTMORTEM_DIR, f"session_checkpoint_{day}_{label}.json"), payload)


def _backfill_review_index(day: str) -> str:
    artifact_files = {
        "postmortem": os.path.join(POSTMORTEM_DIR, f"postmortem_{day}.json"),
        "postmortem_text": os.path.join(POSTMORTEM_DIR, f"postmortem_{day}.txt"),
        "live_signal_parity": os.path.join(POSTMORTEM_DIR, "live_signal_parity", f"live_signal_parity_{day}.jsonl"),
        "live_decision_source": os.path.join(
            POSTMORTEM_DIR,
            "unified_decision_ledger",
            day,
            f"live_decision_source_{day}.jsonl",
        ),
        "execution_lifecycle": os.path.join(
            POSTMORTEM_DIR,
            "execution_lifecycle",
            day,
            f"execution_lifecycle_{day}.jsonl",
        ),
        "execution_lifecycle_summary": os.path.join(
            POSTMORTEM_DIR,
            "execution_lifecycle",
            day,
            f"execution_lifecycle_{day}.summary.json",
        ),
        "parity_verdict": os.path.join(POSTMORTEM_DIR, "parity_verdict", f"parity_verdict_{day}.json"),
        "parity_sentinel": os.path.join(POSTMORTEM_DIR, "parity_sentinel", f"parity_sentinel_{day}.json"),
        "golden_parity": os.path.join(POSTMORTEM_DIR, "golden_parity", "golden_parity_results.json"),
        "automation_day_health": os.path.join(POSTMORTEM_DIR, f"automation_day_health_{day}.json"),
    }
    existing_files = {
        name: os.path.abspath(path)
        for name, path in artifact_files.items()
        if os.path.exists(path)
    }
    postmortem_payload = _read_json(artifact_files["postmortem"], {}) or {}
    parity_verdict = _read_json(artifact_files["parity_verdict"], {}) or {}
    parity_sentinel = _read_json(artifact_files["parity_sentinel"], {}) or {}
    payload = {
        "day": day,
        "created_at_ct": _now(),
        "purpose": "Compact backfilled entry point for historical review artifacts.",
        "trust": postmortem_payload.get("trust_today_for_learning") or {
            "verdict": "backfilled_review_index",
            "note": "Use source artifacts for primary evidence.",
        },
        "headline": {
            "pnl": postmortem_payload.get("pnl") or postmortem_payload.get("total_pnl"),
            "trades": postmortem_payload.get("trades") or postmortem_payload.get("trade_count"),
            "parity_verdict": parity_verdict.get("verdict"),
            "parity_promotion_safe": parity_verdict.get("promotion_safe"),
            "intraday_sentinel_ok": parity_sentinel.get("ok"),
            "intraday_sentinel_severity": parity_sentinel.get("severity"),
        },
        "artifact_files": existing_files,
        "missing_referenced_files": [
            os.path.abspath(path)
            for path in artifact_files.values()
            if not os.path.exists(path)
        ],
        "review_order": [
            "postmortem",
            "parity_verdict",
            "parity_sentinel",
            "execution_lifecycle_summary",
            "live_signal_parity",
            "live_decision_source",
        ],
    }
    payload["backfill_metadata"] = {
        "source": "backfill_automation_procedure_artifacts",
        "created_at_ct": _now(),
        "deduction": "Review index materialized after telemetry backfill; use underlying artifacts for source evidence.",
    }
    path = _write_json(os.path.join(POSTMORTEM_DIR, f"review_index_{day}.json"), payload)
    _write_json(os.path.join(POSTMORTEM_DIR, "REVIEW_INDEX_LATEST.json"), payload)
    return path


def backfill(day: str) -> dict[str, Any]:
    phases = {phase: _backfill_phase(day, phase) for phase in PHASES}
    checkpoints = {label: _backfill_checkpoint(day, label) for label in CHECKPOINT_LABELS}
    review_index = _backfill_review_index(day)
    ledger = _read_json(RUN_LEDGER_PATH, {}) or {}
    for phase, payload in phases.items():
        key = _run_key(phase, day)
        prior = ledger.get(key) if isinstance(ledger.get(key), dict) else {}
        ledger[key] = {
            **prior,
            "phase": phase,
            "day": day,
            "status": "backfilled",
            "ok": True,
            "artifact": payload.get("artifact"),
            "completed_at_ct": _now(),
            "step_count": len(payload.get("steps") or []),
            "market_calendar": payload.get("market_calendar"),
            "backfill_metadata": payload.get("backfill_metadata"),
        }
    ledger_path = _write_json(RUN_LEDGER_PATH, ledger)
    summary = {
        "day": day,
        "created_at_ct": _now(),
        "ok": True,
        "phases": {phase: payload.get("artifact") for phase, payload in phases.items()},
        "checkpoints": checkpoints,
        "review_index": review_index,
        "ledger": ledger_path,
        "deduction": (
            "Only procedural automation wrappers were backfilled. "
            "Trading evidence remains sourced from audit/trade/postmortem artifacts."
        ),
    }
    summary["path"] = _write_json(
        os.path.join(POSTMORTEM_DIR, "automation_procedure_backfill", f"automation_procedure_backfill_{day}.json"),
        summary,
    )
    return summary


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill missing historical automation bookkeeping artifacts.")
    ap.add_argument("day")
    args = ap.parse_args()
    print(json.dumps(backfill(args.day), indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
