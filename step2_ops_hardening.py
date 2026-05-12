"""Operational hardening layer for Step 2 learning.

Tranches 19-24 convert learning artifacts into live-feedback, monitoring,
rollback, auditability, readiness, and runbook controls.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any


OPS_HARDENING_VERSION = "step2_ops_hardening_v1"


def _stable_hash(payload: Any, length: int = 24) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length]


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _i(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def live_feedback_loop_report(
    *,
    closed_loop_controller: dict[str, Any] | None = None,
    deployment_risk_report: dict[str, Any] | None = None,
    belief_calibration_report: dict[str, Any] | None = None,
    source: str = "step2_ops_hardening",
) -> dict[str, Any]:
    controller = closed_loop_controller or {}
    deployment = deployment_risk_report or {}
    calibration = belief_calibration_report or {}
    directives = controller.get("next_run_directives") if isinstance(controller.get("next_run_directives"), dict) else {}
    queues = []
    for key, action in (
        ("scale_routes", "scale"),
        ("probe_routes", "probe"),
        ("repair_routes", "repair"),
        ("retire_routes", "retire"),
    ):
        for route in directives.get(key) or []:
            queues.append({
                "feedback_id": _stable_hash({"action": action, "route": route}),
                "route_key": route.get("route_key") if isinstance(route, dict) else route,
                "action": action,
                "source": "closed_loop_controller",
                "required_observation": "paper_or_live_outcome_before_memory_upgrade",
            })
    for row in deployment.get("deployment_queue") or []:
        queues.append({
            "feedback_id": _stable_hash({"deployment": row.get("variant")}),
            "variant": row.get("variant"),
            "action": "monitor_live_candidate",
            "source": "deployment_risk_report",
            "required_observation": "fill_quality_pnl_drawdown_and_rule_parity",
        })
    pending = _i(calibration.get("pending_predictions"))
    if pending:
        queues.append({
            "feedback_id": _stable_hash({"pending_calibration": pending}),
            "action": "resolve_pending_beliefs",
            "source": "belief_calibration_report",
            "pending_predictions": pending,
            "required_observation": "future_validation_or_promotion_feedback",
        })
    return {
        "schema_version": 1,
        "ops_hardening_version": OPS_HARDENING_VERSION,
        "source": source,
        "tranche": 19,
        "feedback_item_count": len(queues),
        "live_feedback_ready": bool(queues),
        "feedback_queue": queues[:100],
    }


def drift_monitoring_report(
    *,
    learning_control_plane: dict[str, Any] | None = None,
    learning_depth_report: dict[str, Any] | None = None,
    statistical_validation_report: dict[str, Any] | None = None,
    data_quality_report: dict[str, Any] | None = None,
    source: str = "step2_ops_hardening",
) -> dict[str, Any]:
    monitors = []
    control = learning_control_plane or {}
    depth = learning_depth_report or {}
    stat = statistical_validation_report or {}
    dq = data_quality_report or {}
    directive = control.get("next_batch_directive") if isinstance(control.get("next_batch_directive"), dict) else {}
    if directive.get("top_attribution"):
        monitors.append({
            "monitor_id": "top_attribution_decay",
            "subject": (directive.get("top_attribution") or {}).get("route_key"),
            "metric": "attribution_score",
            "alert_if": "falls_below_prior_by_35_pct",
            "severity": "medium",
        })
    risk_state = ((depth.get("uncertainty_risk_pricing_report") or {}).get("risk_budget_state") if isinstance(depth.get("uncertainty_risk_pricing_report"), dict) else None)
    monitors.append({
        "monitor_id": "risk_budget_state",
        "subject": "learning_depth",
        "metric": "avg_uncertainty_price",
        "alert_if": "risk_budget_state_tight",
        "severity": "high" if risk_state == "tight" else "medium",
    })
    if _f(stat.get("false_discovery_pressure", stat.get("multiple_testing_pressure"))) >= 0.5:
        monitors.append({
            "monitor_id": "false_discovery_pressure",
            "subject": "statistical_validation",
            "metric": "false_discovery_pressure",
            "alert_if": "pressure_above_0_5",
            "severity": "high",
        })
    if dq and (dq.get("learning_allowed") is False or _f(dq.get("quality_score", dq.get("score")), 100.0) < 80.0):
        monitors.append({
            "monitor_id": "data_quality_regression",
            "subject": "data_quality",
            "metric": "quality_score",
            "alert_if": "learning_blocked_or_quality_under_80",
            "severity": "critical",
        })
    return {
        "schema_version": 1,
        "ops_hardening_version": OPS_HARDENING_VERSION,
        "source": source,
        "tranche": 20,
        "monitor_count": len(monitors),
        "critical_monitor_count": sum(1 for row in monitors if row.get("severity") == "critical"),
        "monitors": monitors,
    }


def rollback_kill_switch_report(
    *,
    deployment_risk_report: dict[str, Any] | None = None,
    learning_governance_report: dict[str, Any] | None = None,
    promotion_evidence_hardening_report: dict[str, Any] | None = None,
    world_class_readiness_report: dict[str, Any] | None = None,
    source: str = "step2_ops_hardening",
) -> dict[str, Any]:
    deployment = deployment_risk_report or {}
    governance = learning_governance_report or {}
    promotion = promotion_evidence_hardening_report or {}
    world = world_class_readiness_report or {}
    switches = []
    for name in deployment.get("global_kill_switches") or []:
        switches.append({"name": name, "source": "deployment_risk_report", "armed": True, "severity": "critical"})
    if governance.get("decision") == "block_learning_influence":
        switches.append({"name": "block_learning_influence", "source": "learning_governance_report", "armed": True, "severity": "critical"})
    if promotion.get("promotion_hardened") is False:
        switches.append({"name": "promotion_not_hardened", "source": "promotion_evidence_hardening_report", "armed": True, "severity": "high"})
    if world and world.get("ok") is not True:
        switches.append({"name": "world_class_readiness_not_ok", "source": "world_class_readiness_report", "armed": True, "severity": "critical"})
    rollback_steps = [
        "pause_new_promotions",
        "fall_back_to_last_known_good_profile",
        "clear_live_candidate_queue",
        "keep paper/shadow observation running",
        "open repair experiment from top blocker",
    ]
    return {
        "schema_version": 1,
        "ops_hardening_version": OPS_HARDENING_VERSION,
        "source": source,
        "tranche": 21,
        "kill_switch_count": len(switches),
        "critical_kill_switch_count": sum(1 for row in switches if row.get("severity") == "critical"),
        "deployment_may_scale": not any(row.get("severity") == "critical" for row in switches),
        "kill_switches": switches,
        "rollback_steps": rollback_steps,
    }


def auditability_lineage_report(
    *,
    artifact_paths: dict[str, Any] | None = None,
    artifact_trust_report: dict[str, Any] | None = None,
    learning_control_plane: dict[str, Any] | None = None,
    learning_depth_report: dict[str, Any] | None = None,
    source: str = "step2_ops_hardening",
) -> dict[str, Any]:
    paths = artifact_paths if isinstance(artifact_paths, dict) else {}
    trust = artifact_trust_report or {}
    required = [
        "belief_calibration_report",
        "learning_control_plane",
        "learning_depth_report",
        "deployment_risk_report",
        "world_class_readiness_report",
    ]
    entries = []
    for key in required:
        entries.append({
            "artifact": key,
            "path": paths.get(key),
            "present": key in paths or bool((learning_control_plane if key == "learning_control_plane" else learning_depth_report if key == "learning_depth_report" else None)),
            "audit_role": "required_for_end_to_end_replay",
        })
    trust_state = str(trust.get("trust_state") or "unknown")
    missing = [row["artifact"] for row in entries if not row["present"]]
    return {
        "schema_version": 1,
        "ops_hardening_version": OPS_HARDENING_VERSION,
        "source": source,
        "tranche": 22,
        "audit_ready": not missing and trust_state in {"trusted", "usable_with_gaps"},
        "artifact_trust_state": trust_state,
        "missing_artifacts": missing,
        "lineage_entries": entries,
    }


def end_to_end_readiness_gate(
    *,
    live_feedback_loop_report: dict[str, Any] | None = None,
    drift_monitoring_report: dict[str, Any] | None = None,
    rollback_kill_switch_report: dict[str, Any] | None = None,
    auditability_lineage_report: dict[str, Any] | None = None,
    learning_ops_readiness_report: dict[str, Any] | None = None,
    source: str = "step2_ops_hardening",
) -> dict[str, Any]:
    blockers = []
    warnings = []
    live = live_feedback_loop_report or {}
    drift = drift_monitoring_report or {}
    rollback = rollback_kill_switch_report or {}
    audit = auditability_lineage_report or {}
    readiness = learning_ops_readiness_report or {}
    if not live.get("live_feedback_ready"):
        warnings.append("live_feedback_queue_empty")
    if _i(drift.get("critical_monitor_count")) > 0:
        blockers.append("critical_drift_or_quality_monitor")
    if rollback.get("deployment_may_scale") is False:
        blockers.append("kill_switch_blocks_scale")
    if audit.get("audit_ready") is not True:
        blockers.append("audit_lineage_not_ready")
    if readiness and readiness.get("ready_for_next_batch") is False:
        warnings.extend(readiness.get("blockers") or [])
    state = "ready" if not blockers else "blocked"
    if not blockers and warnings:
        state = "caution"
    return {
        "schema_version": 1,
        "ops_hardening_version": OPS_HARDENING_VERSION,
        "source": source,
        "tranche": 23,
        "ready": not blockers,
        "readiness_state": state,
        "blockers": sorted(set(blockers)),
        "warnings": sorted(set(str(item) for item in warnings)),
        "required_before_live_scale": [
            "void_registry_ok",
            "audit_lineage_ready",
            "no_critical_drift_monitor",
            "no_critical_kill_switch",
            "live_feedback_queue_defined",
        ],
    }


def elite_runbook_report(
    *,
    end_to_end_readiness_gate: dict[str, Any] | None = None,
    active_experiment_design_report: dict[str, Any] | None = None,
    learning_depth_report: dict[str, Any] | None = None,
    source: str = "step2_ops_hardening",
) -> dict[str, Any]:
    gate = end_to_end_readiness_gate or {}
    experiments = (active_experiment_design_report or {}).get("experiments") or []
    depth = learning_depth_report or {}
    directive = depth.get("next_batch_depth_directive") if isinstance(depth.get("next_batch_depth_directive"), dict) else {}
    steps = [
        {"order": 1, "step": "verify_void_registry_and_data_quality", "owner": "pre_run_gate"},
        {"order": 2, "step": "run_top_active_experiment", "owner": "learning_controller", "experiment": experiments[0] if experiments else None},
        {"order": 3, "step": "monitor_feedback_and_drift", "owner": "ops_monitor"},
        {"order": 4, "step": "reconcile_beliefs_and_depth_memory", "owner": "learning_db"},
        {"order": 5, "step": "promote_only_if_evidence_hardened", "owner": "promotion_gate", "promotion_hardened": directive.get("promotion_hardened")},
    ]
    return {
        "schema_version": 1,
        "ops_hardening_version": OPS_HARDENING_VERSION,
        "source": source,
        "tranche": 24,
        "runbook_state": "execute" if gate.get("ready") else "repair_first",
        "top_experiment": experiments[0] if experiments else None,
        "risk_budget_state": directive.get("risk_budget_state"),
        "steps": steps,
    }


def ops_hardening_report(
    *,
    closed_loop_controller: dict[str, Any] | None = None,
    deployment_risk_report: dict[str, Any] | None = None,
    belief_calibration_report: dict[str, Any] | None = None,
    learning_control_plane: dict[str, Any] | None = None,
    learning_depth_report: dict[str, Any] | None = None,
    learning_governance_report: dict[str, Any] | None = None,
    promotion_evidence_hardening_report: dict[str, Any] | None = None,
    world_class_readiness_report: dict[str, Any] | None = None,
    statistical_validation_report: dict[str, Any] | None = None,
    data_quality_report: dict[str, Any] | None = None,
    artifact_paths: dict[str, Any] | None = None,
    artifact_trust_report: dict[str, Any] | None = None,
    learning_ops_readiness_report: dict[str, Any] | None = None,
    active_experiment_design_report: dict[str, Any] | None = None,
    source: str = "step2_ops_hardening",
) -> dict[str, Any]:
    live = live_feedback_loop_report(
        closed_loop_controller=closed_loop_controller,
        deployment_risk_report=deployment_risk_report,
        belief_calibration_report=belief_calibration_report,
        source=source,
    )
    drift = drift_monitoring_report(
        learning_control_plane=learning_control_plane,
        learning_depth_report=learning_depth_report,
        statistical_validation_report=statistical_validation_report,
        data_quality_report=data_quality_report,
        source=source,
    )
    rollback = rollback_kill_switch_report(
        deployment_risk_report=deployment_risk_report,
        learning_governance_report=learning_governance_report,
        promotion_evidence_hardening_report=promotion_evidence_hardening_report,
        world_class_readiness_report=world_class_readiness_report,
        source=source,
    )
    audit = auditability_lineage_report(
        artifact_paths=artifact_paths,
        artifact_trust_report=artifact_trust_report,
        learning_control_plane=learning_control_plane,
        learning_depth_report=learning_depth_report,
        source=source,
    )
    gate = end_to_end_readiness_gate(
        live_feedback_loop_report=live,
        drift_monitoring_report=drift,
        rollback_kill_switch_report=rollback,
        auditability_lineage_report=audit,
        learning_ops_readiness_report=learning_ops_readiness_report,
        source=source,
    )
    runbook = elite_runbook_report(
        end_to_end_readiness_gate=gate,
        active_experiment_design_report=active_experiment_design_report,
        learning_depth_report=learning_depth_report,
        source=source,
    )
    return {
        "schema_version": 1,
        "ops_hardening_version": OPS_HARDENING_VERSION,
        "source": source,
        "tranches": [19, 20, 21, 22, 23, 24],
        "live_feedback_loop_report": live,
        "drift_monitoring_report": drift,
        "rollback_kill_switch_report": rollback,
        "auditability_lineage_report": audit,
        "end_to_end_readiness_gate": gate,
        "elite_runbook_report": runbook,
        "ops_directive": {
            "readiness_state": gate.get("readiness_state"),
            "runbook_state": runbook.get("runbook_state"),
            "deployment_may_scale": rollback.get("deployment_may_scale"),
            "critical_monitor_count": drift.get("critical_monitor_count"),
        },
    }
