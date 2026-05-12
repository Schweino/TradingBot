"""Top-level readiness audit for the Step 2 self-learning engine."""
from __future__ import annotations

from typing import Any

import step2_world_class_certification


AUDIT_VERSION = "step2_world_class_audit_v1"

CORE_RUN_SECTIONS = (
    "data_quality_report",
    "statistical_validation_report",
    "deployment_risk_report",
)

PROFIT_COMBO_SECTIONS = CORE_RUN_SECTIONS + (
    "execution_adjusted_objective_report",
    "closed_loop_causal_controller",
    "artifact_trust_report",
    "learning_ops_readiness_report",
)


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _candidate_name(row: dict[str, Any]) -> str:
    return str(row.get("variant") or row.get("name") or row.get("candidate_id") or "candidate")


def candidate_readiness(row: dict[str, Any]) -> dict[str, Any]:
    row = row or {}
    blockers: list[str] = []
    warnings: list[str] = []
    replay = row.get("replay_validity") if isinstance(row.get("replay_validity"), dict) else {}
    objective = row.get("execution_adjusted_objective") if isinstance(row.get("execution_adjusted_objective"), dict) else {}
    data_quality = row.get("data_quality_contract") if isinstance(row.get("data_quality_contract"), dict) else {}
    statistical = row.get("statistical_validation") if isinstance(row.get("statistical_validation"), dict) else {}
    deployment = row.get("deployment_risk") if isinstance(row.get("deployment_risk"), dict) else {}
    if replay and replay.get("ok") is False:
        blockers.append("replay_validity_failed")
    if data_quality:
        if data_quality.get("learning_allowed") is False:
            blockers.append("data_quality_learning_blocked")
        if data_quality.get("promotion_allowed") is False:
            warnings.append("data_quality_not_promotion_allowed")
    else:
        warnings.append("missing_data_quality_contract")
    if statistical:
        if statistical.get("learning_grade") is False:
            warnings.append("statistical_learning_grade_failed")
        if statistical.get("promotion_grade") is False:
            warnings.append("statistical_promotion_grade_failed")
    else:
        warnings.append("missing_statistical_validation")
    if deployment:
        if deployment.get("tier") == "blocked":
            blockers.append("deployment_risk_blocked")
        elif deployment.get("deployment_allowed") is not True:
            warnings.append(f"deployment_tier:{deployment.get('tier') or 'unknown'}")
    else:
        warnings.append("missing_deployment_risk")
    adjusted = _num(row.get("execution_adjusted_pnl") or objective.get("execution_adjusted_pnl"), _num(row.get("step2_pnl"), 0.0))
    if adjusted <= 0.0:
        blockers.append("execution_adjusted_pnl_not_positive")
    score = 100.0 - 30.0 * len(blockers) - 6.0 * len(warnings)
    score += min(8.0, max(0.0, adjusted) / 1000.0)
    score = max(0.0, min(100.0, score))
    return {
        "schema_version": 1,
        "audit_version": AUDIT_VERSION,
        "variant": _candidate_name(row),
        "ok": not blockers,
        "world_class_candidate": bool(not blockers and deployment.get("deployment_allowed") is True and statistical.get("promotion_grade") is True),
        "score": round(score, 4),
        "blockers": blockers,
        "warnings": warnings,
        "tier": deployment.get("tier") if deployment else None,
        "execution_adjusted_pnl": round(adjusted, 6),
    }


def readiness_report(
    rows: list[dict[str, Any]] | None = None,
    *,
    payload: dict[str, Any] | None = None,
    required_sections: tuple[str, ...] | list[str] = CORE_RUN_SECTIONS,
    source: str = "step2",
) -> dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    rows = rows or []
    missing_sections = [key for key in required_sections if not payload.get(key)]
    blockers = []
    warnings = []
    if int(payload.get("void_filtered_rows") or 0) > 0:
        blockers.append("void_filtered_rows_present")
    void_days = payload.get("void_filtered_days") if isinstance(payload.get("void_filtered_days"), list) else []
    if void_days:
        blockers.append("void_filtered_days_present")
    if missing_sections:
        blockers.append("missing_required_readiness_sections")
    data_quality = payload.get("data_quality_report") if isinstance(payload.get("data_quality_report"), dict) else {}
    if int(data_quality.get("learning_blocked_count") or 0) > 0:
        blockers.append("data_quality_learning_blocks_present")
    statistical = payload.get("statistical_validation_report") if isinstance(payload.get("statistical_validation_report"), dict) else {}
    if rows and int(statistical.get("learning_grade_count") or 0) <= 0:
        warnings.append("no_statistical_learning_grade_candidates")
    deployment = payload.get("deployment_risk_report") if isinstance(payload.get("deployment_risk_report"), dict) else {}
    if rows and int(deployment.get("live_eligible_count") or 0) <= 0:
        warnings.append("no_live_eligible_candidates")
    closed_loop = payload.get("closed_loop_causal_controller") if isinstance(payload.get("closed_loop_causal_controller"), dict) else {}
    if closed_loop:
        directives = closed_loop.get("next_run_directives") if isinstance(closed_loop.get("next_run_directives"), dict) else {}
        if not any(directives.get(key) for key in ("scale_routes", "probe_routes", "repair_routes", "retire_routes", "allocation")):
            warnings.append("closed_loop_controller_has_no_directives")
    candidate_audits = [candidate_readiness(row) for row in rows]
    certification = step2_world_class_certification.certification_report(
        payload,
        rows,
        source=source,
    )
    candidate_blockers = sum(1 for row in candidate_audits if not row.get("ok"))
    if candidate_blockers and candidate_blockers == len(candidate_audits):
        blockers.append("all_candidates_blocked_by_readiness")
    avg_score = sum(_num(row.get("score"), 0.0) for row in candidate_audits) / max(1, len(candidate_audits))
    system_score = 100.0 - 22.0 * len(set(blockers)) - 5.0 * len(set(warnings))
    if candidate_audits:
        system_score = min(system_score, avg_score + 10.0)
    system_score = max(0.0, min(100.0, system_score))
    ok = not blockers
    return {
        "schema_version": 1,
        "audit_version": AUDIT_VERSION,
        "source": source,
        "ok": ok,
        "readiness_state": "ready" if ok and system_score >= 80.0 else "blocked" if blockers else "caution",
        "system_score": round(system_score, 4),
        "required_sections": list(required_sections),
        "missing_sections": missing_sections,
        "blockers": sorted(set(blockers)),
        "warnings": sorted(set(warnings)),
        "candidate_count": len(rows),
        "candidate_blocked_count": candidate_blockers,
        "world_class_candidate_count": sum(1 for row in candidate_audits if row.get("world_class_candidate")),
        "world_class_certification": certification,
        "world_class_certified": certification.get("certified_world_class"),
        "top_candidates": sorted(candidate_audits, key=lambda row: _num(row.get("score"), 0.0), reverse=True)[:25],
        "governance_rules": [
            "voided source days are never learning or promotion evidence",
            "raw P/L cannot outrank execution-adjusted/statistical/deployment readiness",
            "promotion requires quote-aware replay, statistical promotion grade, and deployment eligibility when present",
            "closed-loop decisions must scale, probe, repair, or retire routes from observed outcomes",
        ],
    }


def payload_readiness(payload: dict[str, Any], *, source: str = "step2_payload") -> dict[str, Any]:
    rows = []
    for key in ("leaderboard", "recommended_leaderboard", "winners", "raw_leaderboard"):
        value = payload.get(key)
        if isinstance(value, list) and value:
            rows = [row for row in value if isinstance(row, dict)]
            break
    required = PROFIT_COMBO_SECTIONS if payload.get("script") == "step2_profit_combo_hunter.py" or payload.get("profit_combo_learning_report") else CORE_RUN_SECTIONS
    return readiness_report(rows, payload=payload, required_sections=required, source=source)
