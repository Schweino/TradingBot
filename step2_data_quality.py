"""Data quality and provenance contracts for Step 2 learning artifacts."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import step2_quote_aware_guard
import step2_void_registry


CONTRACT_VERSION = "step2_data_quality_v1"
REQUIRED_RUN_ARTIFACTS = ("final_summary.json",)


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _stable_hash(payload: Any, length: int = 24) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length]


def file_fingerprint(path: str | Path) -> dict[str, Any]:
    target = Path(path)
    try:
        resolved = target.resolve()
    except Exception:
        resolved = target
    out: dict[str, Any] = {
        "path": str(resolved),
        "exists": target.exists(),
        "is_file": target.is_file(),
        "sha256": "",
        "size_bytes": 0,
    }
    if not target.exists() or not target.is_file():
        return out
    try:
        data = target.read_bytes()
        out["sha256"] = hashlib.sha256(data).hexdigest()
        out["size_bytes"] = len(data)
    except Exception as exc:
        out["error"] = str(exc)
    return out


def run_provenance(run_dir: str | Path, summary: dict[str, Any] | None = None) -> dict[str, Any]:
    run_path = Path(run_dir)
    summary = summary if isinstance(summary, dict) else {}
    artifact_paths = summary.get("artifact_paths") if isinstance(summary.get("artifact_paths"), dict) else {}
    declared_artifacts = {
        str(name): file_fingerprint(path)
        for name, path in sorted(artifact_paths.items())
    }
    required = {}
    for name in REQUIRED_RUN_ARTIFACTS:
        path = run_path / name
        if not path.exists() and name == "final_summary.json":
            for fallback in ("coordinator_summary.json", "summary.json"):
                fallback_path = run_path / fallback
                if fallback_path.exists():
                    path = fallback_path
                    break
        required[name] = file_fingerprint(path)
    missing_required = [
        name for name, fingerprint in required.items()
        if not fingerprint.get("exists") or not fingerprint.get("is_file")
    ]
    void_report = step2_void_registry.contamination_report(summary, source_path=run_path)
    candidates = summary.get("top100") or summary.get("leaderboard") or summary.get("winners") or []
    if not isinstance(candidates, list):
        candidates = []
    contract = {
        "schema_version": 1,
        "contract_version": CONTRACT_VERSION,
        "run_dir": str(run_path.resolve()),
        "summary_hash": _stable_hash(summary, length=32),
        "required_artifacts": required,
        "declared_artifacts": declared_artifacts,
        "missing_required_artifacts": missing_required,
        "declared_artifact_count": len(declared_artifacts),
        "candidate_count": len(candidates),
        "voided_source_days": void_report.get("voided_source_days") or [],
    }
    blockers = []
    warnings = []
    if missing_required:
        blockers.append("missing_required_artifact")
    if contract["voided_source_days"]:
        blockers.append("voided_source_day")
    if not candidates:
        warnings.append("no_candidate_rows")
    missing_declared = [
        name for name, fingerprint in declared_artifacts.items()
        if not fingerprint.get("exists") or not fingerprint.get("is_file")
    ]
    if missing_declared:
        warnings.append("missing_declared_artifacts")
    score = 100.0
    score -= 60.0 if "voided_source_day" in blockers else 0.0
    score -= 35.0 if "missing_required_artifact" in blockers else 0.0
    score -= min(25.0, 5.0 * len(missing_declared))
    score -= 10.0 if "no_candidate_rows" in warnings else 0.0
    contract["blockers"] = blockers
    contract["warnings"] = warnings
    contract["score"] = round(max(0.0, score), 4)
    contract["learning_allowed"] = not blockers
    contract["contract_id"] = _stable_hash({
        "run_dir": contract["run_dir"],
        "summary_hash": contract["summary_hash"],
        "blockers": blockers,
        "warnings": warnings,
    }, length=32)
    return contract


def row_contract(row: dict[str, Any], *, source_path: str | Path | None = None) -> dict[str, Any]:
    row = row or {}
    blockers: list[str] = []
    warnings: list[str] = []
    if not row.get("variant"):
        blockers.append("missing_variant")
    if not isinstance(row.get("weights"), dict) and not row.get("routes"):
        blockers.append("missing_decision_shape")
    trades = int(_num(row.get("step2_trades"), 0.0))
    if trades <= 0:
        blockers.append("no_trades")
    if row.get("step2_pnl") in (None, ""):
        blockers.append("missing_pnl")
    if not isinstance(row.get("by_day"), dict) or not row.get("by_day"):
        warnings.append("missing_by_day")
    if not isinstance(row.get("by_ticker"), dict) or not row.get("by_ticker"):
        warnings.append("missing_by_ticker")
    quote_actual = step2_quote_aware_guard.candidate_exit_replay_model(row)
    quote_required = step2_quote_aware_guard.REQUIRED_EXIT_REPLAY_MODEL
    if quote_actual != quote_required:
        warnings.append("exit_replay_model_missing" if not quote_actual else "exit_replay_model_mismatch")
    void_report = step2_void_registry.contamination_report(row, source_path=source_path)
    if void_report.get("voided_source_days"):
        blockers.append("voided_source_day")
    score = 100.0
    score -= 70.0 if "voided_source_day" in blockers else 0.0
    score -= 25.0 * sum(1 for item in blockers if item != "voided_source_day")
    score -= 8.0 * len(warnings)
    return {
        "schema_version": 1,
        "contract_version": CONTRACT_VERSION,
        "contract_id": _stable_hash({
            "variant": row.get("variant"),
            "behavior_key": row.get("behavior_key"),
            "config_key": row.get("config_key"),
            "source_path": str(source_path or row.get("hunt_source_path") or row.get("source") or ""),
            "blockers": blockers,
            "warnings": warnings,
        }, length=32),
        "score": round(max(0.0, score), 4),
        "learning_allowed": not blockers,
        "promotion_allowed": not blockers and quote_actual == quote_required,
        "blockers": blockers,
        "warnings": warnings,
        "required_exit_replay_model": quote_required,
        "actual_exit_replay_model": quote_actual,
        "voided_source_days": void_report.get("voided_source_days") or [],
        "source_path": str(source_path or row.get("hunt_source_path") or row.get("source") or ""),
    }


def report(rows: list[dict[str, Any]], *, source: str = "step2") -> dict[str, Any]:
    contracts = [
        row.get("data_quality_contract")
        if isinstance(row.get("data_quality_contract"), dict)
        else row_contract(row, source_path=row.get("hunt_source_path") or row.get("source"))
        for row in rows
    ]
    blocker_counts: dict[str, int] = {}
    warning_counts: dict[str, int] = {}
    for contract in contracts:
        for blocker in contract.get("blockers") or []:
            blocker_counts[str(blocker)] = blocker_counts.get(str(blocker), 0) + 1
        for warning in contract.get("warnings") or []:
            warning_counts[str(warning)] = warning_counts.get(str(warning), 0) + 1
    eligible = [contract for contract in contracts if contract.get("learning_allowed")]
    avg_score = sum(_num(contract.get("score"), 0.0) for contract in contracts) / max(1, len(contracts))
    return {
        "schema_version": 1,
        "contract_version": CONTRACT_VERSION,
        "source": source,
        "candidate_count": len(rows),
        "learning_allowed_count": len(eligible),
        "learning_blocked_count": len(rows) - len(eligible),
        "average_score": round(avg_score, 4),
        "blocker_counts": blocker_counts,
        "warning_counts": warning_counts,
        "top_blocked": [
            {
                "variant": rows[idx].get("variant") if idx < len(rows) else None,
                "score": contract.get("score"),
                "blockers": contract.get("blockers") or [],
                "warnings": contract.get("warnings") or [],
            }
            for idx, contract in enumerate(contracts)
            if not contract.get("learning_allowed")
        ][:25],
    }
