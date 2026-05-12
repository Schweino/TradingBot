"""Unified learning control plane for Step 2 hunt artifacts.

This module turns adjacent learning reports into one end-to-end control layer:
attribution, counterfactuals, regime routing, active experiment design, and
governance. It is intentionally deterministic and score-only.
"""
from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from typing import Any


CONTROL_PLANE_VERSION = "step2_learning_control_plane_v1"


def _stable_hash(payload: Any, length: int = 24) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length]


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _route_key_from_payload(route: dict[str, Any]) -> str:
    if not isinstance(route, dict):
        return ""
    if route.get("route_key"):
        return str(route.get("route_key"))
    match = route.get("match") if isinstance(route.get("match"), dict) else {}
    return "|".join([
        str(route.get("action") or "score"),
        str(match.get("ticker") or "*"),
        str(match.get("setup_type") or "*"),
        str(match.get("session_phase") or "*"),
        str(match.get("side") or "*"),
    ])


def _row_route_keys(row: dict[str, Any]) -> list[str]:
    routes = [
        _route_key_from_payload(route)
        for route in row.get("routes") or []
        if isinstance(route, dict)
    ]
    if not routes and row.get("route_key"):
        routes = [str(row.get("route_key"))]
    return [route for route in routes if route]


def _holdout(row: dict[str, Any]) -> float:
    gate = row.get("holdout_gate") if isinstance(row.get("holdout_gate"), dict) else {}
    return _as_float(gate.get("holdout_pnl", row.get("holdout_pnl")))


def outcome_attribution_report(
    rows: list[dict[str, Any]] | None,
    *,
    shapley_report: dict[str, Any] | None = None,
    pairwise_report: dict[str, Any] | None = None,
    feature_learner_report: dict[str, Any] | None = None,
    source: str = "step2_learning_control_plane",
) -> dict[str, Any]:
    route_scores: dict[str, dict[str, Any]] = {}
    for row in (shapley_report or {}).get("routes") or []:
        route = str(row.get("route_key") or "")
        if not route:
            continue
        holdout_credit = _as_float(row.get("avg_holdout_credit"))
        pnl_credit = _as_float(row.get("avg_pnl_credit"))
        observations = _as_int(row.get("observations"))
        route_scores[route] = {
            "route_key": route,
            "observations": observations,
            "attribution_score": round(holdout_credit * 2.0 + pnl_credit, 6),
            "avg_holdout_credit": round(holdout_credit, 6),
            "avg_pnl_credit": round(pnl_credit, 6),
            "confidence": round(min(1.0, observations / 10.0), 6),
            "state": "causal_candidate" if holdout_credit > 0.0 and pnl_credit > 0.0 else ("repair_candidate" if pnl_credit > 0.0 else "negative_attribution"),
        }
    if not route_scores:
        for row in rows or []:
            routes = _row_route_keys(row)
            if not routes:
                continue
            share = 1.0 / max(1, len(routes))
            pnl = _as_float(row.get("step2_pnl")) * share
            holdout = _holdout(row) * share
            for route in routes:
                item = route_scores.setdefault(route, {
                    "route_key": route,
                    "observations": 0,
                    "attribution_score": 0.0,
                    "avg_holdout_credit": 0.0,
                    "avg_pnl_credit": 0.0,
                    "confidence": 0.0,
                    "state": "unknown",
                })
                item["observations"] += 1
                item["avg_holdout_credit"] += holdout
                item["avg_pnl_credit"] += pnl
        for item in route_scores.values():
            obs = max(1, _as_int(item["observations"]))
            item["avg_holdout_credit"] = round(_as_float(item["avg_holdout_credit"]) / obs, 6)
            item["avg_pnl_credit"] = round(_as_float(item["avg_pnl_credit"]) / obs, 6)
            item["attribution_score"] = round(item["avg_holdout_credit"] * 2.0 + item["avg_pnl_credit"], 6)
            item["confidence"] = round(min(1.0, obs / 10.0), 6)
            item["state"] = "causal_candidate" if item["avg_holdout_credit"] > 0.0 and item["avg_pnl_credit"] > 0.0 else ("repair_candidate" if item["avg_pnl_credit"] > 0.0 else "negative_attribution")
    action_scores: dict[str, dict[str, Any]] = {}
    for route, row in route_scores.items():
        action = route.split("|", 1)[0] if "|" in route else route.split("_", 1)[0]
        item = action_scores.setdefault(action, {"action": action, "routes": 0, "score_sum": 0.0, "confidence_sum": 0.0})
        item["routes"] += 1
        item["score_sum"] += _as_float(row.get("attribution_score"))
        item["confidence_sum"] += _as_float(row.get("confidence"))
    actions = []
    for item in action_scores.values():
        routes_n = max(1, _as_int(item["routes"]))
        actions.append({
            "action": item["action"],
            "route_count": item["routes"],
            "avg_attribution_score": round(_as_float(item["score_sum"]) / routes_n, 6),
            "avg_confidence": round(_as_float(item["confidence_sum"]) / routes_n, 6),
        })
    interactions = []
    for row in (pairwise_report or {}).get("interactions") or []:
        score = _as_float(row.get("avg_holdout_pnl")) * 2.0 + _as_float(row.get("avg_pnl"))
        interactions.append({
            "route_set": row.get("route_set") or [],
            "observations": _as_int(row.get("observations")),
            "attribution_score": round(score, 6),
            "state": row.get("interaction_state"),
        })
    feature_rows = []
    for row in (feature_learner_report or {}).get("features") or []:
        feature_rows.append({
            "feature": row.get("feature"),
            "importance": _as_float(row.get("importance") or row.get("abs_corr") or row.get("edge_correlation")),
            "direction": row.get("direction") or row.get("signed_direction"),
        })
    route_attribution = sorted(route_scores.values(), key=lambda item: (_as_float(item.get("attribution_score")), _as_float(item.get("confidence"))), reverse=True)
    actions.sort(key=lambda item: _as_float(item.get("avg_attribution_score")), reverse=True)
    interactions.sort(key=lambda item: _as_float(item.get("attribution_score")), reverse=True)
    feature_rows.sort(key=lambda item: _as_float(item.get("importance")), reverse=True)
    return {
        "schema_version": 1,
        "control_plane_version": CONTROL_PLANE_VERSION,
        "source": source,
        "tranche": 9,
        "route_count": len(route_attribution),
        "action_count": len(actions),
        "interaction_count": len(interactions),
        "feature_count": len(feature_rows),
        "top_route": route_attribution[0] if route_attribution else None,
        "top_action": actions[0] if actions else None,
        "route_attribution": route_attribution[:100],
        "action_attribution": actions[:30],
        "interaction_attribution": interactions[:50],
        "feature_attribution": feature_rows[:50],
    }


def counterfactual_learning_report(
    *,
    counterfactual_backlog: dict[str, Any] | None = None,
    missed_winner_report: dict[str, Any] | None = None,
    attribution_report: dict[str, Any] | None = None,
    stress_test_queue: dict[str, Any] | None = None,
    source: str = "step2_learning_control_plane",
) -> dict[str, Any]:
    candidates = []
    for row in (counterfactual_backlog or {}).get("backlog") or []:
        candidates.append({
            "counterfactual_id": row.get("counterfactual_id") or _stable_hash(row),
            "subject": row.get("subject"),
            "question": row.get("question"),
            "source": "counterfactual_backlog",
            "expected_learning_value": _as_float(row.get("priority_score")),
            "expected_profit_lift_proxy": max(0.0, _as_float(row.get("priority_score")) * 0.25),
            "required_change": row.get("required_change"),
            "falsification_gate": "counterfactual_underperforms_control_or_adds_failure_class",
        })
    for row in (missed_winner_report or {}).get("missed") or []:
        candidates.append({
            "counterfactual_id": _stable_hash({"missed": row.get("variant"), "reason": row.get("reason")}),
            "subject": row.get("variant"),
            "question": f"Would this become promotable if we fixed {row.get('reason')}?",
            "source": "missed_winner_report",
            "expected_learning_value": abs(_as_float(row.get("holdout_pnl"))) + max(0.0, _as_float(row.get("pnl"))),
            "expected_profit_lift_proxy": max(0.0, _as_float(row.get("holdout_pnl"))),
            "required_change": row.get("recommended_action"),
            "falsification_gate": "fix_does_not_preserve_holdout_profit",
        })
    for row in (attribution_report or {}).get("route_attribution") or []:
        if row.get("state") != "causal_candidate":
            continue
        candidates.append({
            "counterfactual_id": _stable_hash({"disable_route": row.get("route_key")}),
            "subject": row.get("route_key"),
            "question": "Does disabling or isolating this route reduce expected profit?",
            "source": "outcome_attribution",
            "expected_learning_value": _as_float(row.get("attribution_score")) * max(0.2, _as_float(row.get("confidence"))),
            "expected_profit_lift_proxy": _as_float(row.get("avg_holdout_credit")),
            "required_change": "disable_or_isolate_route_counterfactual",
            "falsification_gate": "route_isolation_does_not_change_holdout_profit",
        })
    for row in (stress_test_queue or {}).get("tests") or []:
        candidates.append({
            "counterfactual_id": row.get("test_id") or _stable_hash(row),
            "subject": row.get("variant"),
            "question": f"Stress test: {row.get('stress_type')}",
            "source": "stress_test_queue",
            "expected_learning_value": 35.0,
            "expected_profit_lift_proxy": 0.0,
            "required_change": row.get("stress_type"),
            "falsification_gate": row.get("min_pass_condition"),
        })
    candidates.sort(key=lambda row: (_as_float(row.get("expected_learning_value")), _as_float(row.get("expected_profit_lift_proxy"))), reverse=True)
    return {
        "schema_version": 1,
        "control_plane_version": CONTROL_PLANE_VERSION,
        "source": source,
        "tranche": 10,
        "candidate_count": len(candidates),
        "top_counterfactual": candidates[0] if candidates else None,
        "counterfactuals": candidates[:100],
    }


def regime_conditioned_learning_report(
    *,
    regime_fingerprint_report: dict[str, Any] | None = None,
    attribution_report: dict[str, Any] | None = None,
    belief_calibration_report: dict[str, Any] | None = None,
    source: str = "step2_learning_control_plane",
) -> dict[str, Any]:
    top_routes = [
        row for row in (attribution_report or {}).get("route_attribution") or []
        if row.get("state") == "causal_candidate"
    ][:12]
    calibration = belief_calibration_report or {}
    multiplier = _as_float((calibration.get("learning_adjustments") or {}).get("belief_weight_multiplier"), 1.0)
    cells = []
    for row in (regime_fingerprint_report or {}).get("fingerprints") or []:
        avg_pnl = _as_float(row.get("avg_pnl"))
        holdout_rate = _as_float(row.get("holdout_positive_rate_pct"))
        count = _as_int(row.get("variant_count"))
        route_hint = top_routes[0].get("route_key") if top_routes else None
        score = (avg_pnl * 0.5 + holdout_rate * 2.0 + min(25.0, count * 2.0)) * multiplier
        state = row.get("regime_state") or ("expand" if score > 75.0 else "probe")
        cells.append({
            "regime_key": f"{row.get('bucket_type')}:{row.get('bucket')}",
            "bucket_type": row.get("bucket_type"),
            "bucket": row.get("bucket"),
            "variant_count": count,
            "avg_pnl": round(avg_pnl, 6),
            "holdout_positive_rate_pct": round(holdout_rate, 6),
            "regime_score": round(score, 6),
            "regime_state": state,
            "recommended_route_hint": route_hint,
            "policy": "exploit_with_small_cap" if state == "expand" else ("repair_or_retest" if state == "repair" else "probe_before_generalizing"),
        })
    cells.sort(key=lambda row: _as_float(row.get("regime_score")), reverse=True)
    return {
        "schema_version": 1,
        "control_plane_version": CONTROL_PLANE_VERSION,
        "source": source,
        "tranche": 11,
        "regime_count": len(cells),
        "top_regime": cells[0] if cells else None,
        "regimes": cells[:100],
        "belief_weight_multiplier": round(multiplier, 6),
    }


def active_experiment_design_report(
    *,
    experiment_sequencer_report: dict[str, Any] | None = None,
    counterfactual_learning_report: dict[str, Any] | None = None,
    regime_learning_report: dict[str, Any] | None = None,
    attribution_report: dict[str, Any] | None = None,
    batch_size: int = 0,
    source: str = "step2_learning_control_plane",
) -> dict[str, Any]:
    designs = []
    for row in (experiment_sequencer_report or {}).get("sequence") or []:
        designs.append({
            "experiment_id": _stable_hash({"sequence": row.get("order"), "subject": row.get("subject")}),
            "source": "experiment_sequencer",
            "subject": row.get("subject"),
            "question": row.get("question"),
            "variant_budget": _as_int(row.get("variant_budget")),
            "expected_value_of_information": _as_float(row.get("variant_budget")) * 1.5 + 20.0,
            "expected_profit_lift_proxy": 0.0,
            "success_gate": row.get("success_gate"),
            "stop_loss_rule": "stop_if_control_beats_treatment_or_failure_class_appears",
        })
    for row in (counterfactual_learning_report or {}).get("counterfactuals") or []:
        budget = max(4, min(40, int(round(max(1, batch_size) * 0.04))))
        designs.append({
            "experiment_id": row.get("counterfactual_id"),
            "source": "counterfactual_learning",
            "subject": row.get("subject"),
            "question": row.get("question"),
            "variant_budget": budget,
            "expected_value_of_information": _as_float(row.get("expected_learning_value")),
            "expected_profit_lift_proxy": _as_float(row.get("expected_profit_lift_proxy")),
            "success_gate": "positive_holdout_vs_control",
            "stop_loss_rule": row.get("falsification_gate"),
        })
    for row in (regime_learning_report or {}).get("regimes") or []:
        if row.get("regime_state") not in {"expand", "repair"}:
            continue
        budget = max(3, min(30, int(round(max(1, batch_size) * 0.03))))
        designs.append({
            "experiment_id": _stable_hash({"regime": row.get("regime_key")}),
            "source": "regime_learning",
            "subject": row.get("regime_key"),
            "question": f"Does {row.get('policy')} improve this regime without overfitting?",
            "variant_budget": budget,
            "expected_value_of_information": _as_float(row.get("regime_score")),
            "expected_profit_lift_proxy": max(0.0, _as_float(row.get("avg_pnl"))),
            "success_gate": "expanded_regime_sample_remains_holdout_positive",
            "stop_loss_rule": "retire_regime_rule_if_expansion_fails",
        })
    for row in (attribution_report or {}).get("route_attribution") or []:
        if row.get("state") != "causal_candidate":
            continue
        budget = max(3, min(25, int(round(max(1, batch_size) * 0.025))))
        designs.append({
            "experiment_id": _stable_hash({"attribution": row.get("route_key")}),
            "source": "outcome_attribution",
            "subject": row.get("route_key"),
            "question": "Can this attributed route produce lift when isolated from unrelated mutations?",
            "variant_budget": budget,
            "expected_value_of_information": _as_float(row.get("attribution_score")) * max(0.25, _as_float(row.get("confidence"))),
            "expected_profit_lift_proxy": _as_float(row.get("avg_holdout_credit")),
            "success_gate": "isolated_route_lift_positive",
            "stop_loss_rule": "downweight_route_if_isolated_lift_disappears",
        })
    deduped: dict[str, dict[str, Any]] = {}
    for row in designs:
        key = str(row.get("experiment_id") or _stable_hash(row))
        existing = deduped.get(key)
        if not existing or _as_float(row.get("expected_value_of_information")) > _as_float(existing.get("expected_value_of_information")):
            deduped[key] = row
    ordered = sorted(deduped.values(), key=lambda row: (_as_float(row.get("expected_value_of_information")), _as_float(row.get("expected_profit_lift_proxy"))), reverse=True)
    budget_cap = max(0, int(batch_size))
    allocated = 0
    final = []
    for idx, row in enumerate(ordered[:20], start=1):
        budget = min(_as_int(row.get("variant_budget")), max(0, budget_cap - allocated)) if budget_cap else _as_int(row.get("variant_budget"))
        if budget <= 0 and budget_cap:
            break
        allocated += budget
        final.append({**row, "rank": idx, "variant_budget": budget})
    return {
        "schema_version": 1,
        "control_plane_version": CONTROL_PLANE_VERSION,
        "source": source,
        "tranche": 12,
        "batch_size": int(batch_size),
        "experiment_count": len(final),
        "allocated_budget": allocated,
        "top_experiment": final[0] if final else None,
        "experiments": final,
    }


def learning_governance_report(
    *,
    data_quality_report: dict[str, Any] | None = None,
    statistical_validation_report: dict[str, Any] | None = None,
    belief_calibration_report: dict[str, Any] | None = None,
    artifact_trust_report: dict[str, Any] | None = None,
    deployment_risk_report: dict[str, Any] | None = None,
    active_experiment_design_report: dict[str, Any] | None = None,
    source: str = "step2_learning_control_plane",
) -> dict[str, Any]:
    blockers = []
    warnings = []
    dq = data_quality_report or {}
    if dq and dq.get("learning_allowed") is False:
        blockers.append("data_quality_blocks_learning")
    if _as_float(dq.get("quality_score", dq.get("score")), 100.0) < 70.0:
        warnings.append("data_quality_score_low")
    stat = statistical_validation_report or {}
    if stat and _as_float(stat.get("false_discovery_pressure", stat.get("multiple_testing_pressure")), 0.0) >= 0.75:
        warnings.append("multiple_testing_pressure_high")
    calibration = belief_calibration_report or {}
    cal_state = (calibration.get("learning_adjustments") or {}).get("state")
    if cal_state == "recalibrate_before_scaling":
        blockers.append("belief_calibration_requires_recalibration")
    elif cal_state == "await_outcomes":
        warnings.append("belief_calibration_awaiting_outcomes")
    trust = artifact_trust_report or {}
    if trust and str(trust.get("trust_state")) not in {"trusted", "usable_with_gaps"}:
        blockers.append("artifact_trust_not_usable")
    deploy = deployment_risk_report or {}
    risk_state = str(deploy.get("deployment_state") or deploy.get("risk_state") or "")
    if risk_state in {"blocked", "kill_switch", "do_not_deploy"}:
        blockers.append("deployment_risk_blocks_learning_scale")
    experiments = (active_experiment_design_report or {}).get("experiments") or []
    if not experiments:
        warnings.append("no_active_experiments_selected")
    if blockers:
        decision = "block_learning_influence"
    elif warnings:
        decision = "allow_probe_only"
    else:
        decision = "allow_learning_to_steer_next_batch"
    return {
        "schema_version": 1,
        "control_plane_version": CONTROL_PLANE_VERSION,
        "source": source,
        "tranche": 13,
        "decision": decision,
        "blockers": blockers,
        "warnings": warnings,
        "max_learning_influence": 0.0 if blockers else (0.35 if warnings else 0.75),
        "required_controls": [
            "exclude_voided_days",
            "score_only_no_rebuild",
            "resolve_or_keep_pending_beliefs",
            "prefer_active_experiments_over_uncontrolled_scaling",
            "downweight_stale_or_uncalibrated_lessons",
        ],
        "active_experiment_count": len(experiments),
    }


def learning_control_plane_report(
    rows: list[dict[str, Any]] | None,
    *,
    shapley_report: dict[str, Any] | None = None,
    pairwise_report: dict[str, Any] | None = None,
    feature_learner_report: dict[str, Any] | None = None,
    counterfactual_backlog: dict[str, Any] | None = None,
    missed_winner_report: dict[str, Any] | None = None,
    stress_test_queue: dict[str, Any] | None = None,
    regime_fingerprint_report: dict[str, Any] | None = None,
    belief_calibration_report: dict[str, Any] | None = None,
    experiment_sequencer_report: dict[str, Any] | None = None,
    data_quality_report: dict[str, Any] | None = None,
    statistical_validation_report: dict[str, Any] | None = None,
    artifact_trust_report: dict[str, Any] | None = None,
    deployment_risk_report: dict[str, Any] | None = None,
    batch_size: int = 0,
    source: str = "step2_learning_control_plane",
) -> dict[str, Any]:
    attribution = outcome_attribution_report(
        rows,
        shapley_report=shapley_report,
        pairwise_report=pairwise_report,
        feature_learner_report=feature_learner_report,
        source=source,
    )
    counterfactuals = counterfactual_learning_report(
        counterfactual_backlog=counterfactual_backlog,
        missed_winner_report=missed_winner_report,
        attribution_report=attribution,
        stress_test_queue=stress_test_queue,
        source=source,
    )
    regimes = regime_conditioned_learning_report(
        regime_fingerprint_report=regime_fingerprint_report,
        attribution_report=attribution,
        belief_calibration_report=belief_calibration_report,
        source=source,
    )
    experiments = active_experiment_design_report(
        experiment_sequencer_report=experiment_sequencer_report,
        counterfactual_learning_report=counterfactuals,
        regime_learning_report=regimes,
        attribution_report=attribution,
        batch_size=batch_size,
        source=source,
    )
    governance = learning_governance_report(
        data_quality_report=data_quality_report,
        statistical_validation_report=statistical_validation_report,
        belief_calibration_report=belief_calibration_report,
        artifact_trust_report=artifact_trust_report,
        deployment_risk_report=deployment_risk_report,
        active_experiment_design_report=experiments,
        source=source,
    )
    return {
        "schema_version": 1,
        "control_plane_version": CONTROL_PLANE_VERSION,
        "source": source,
        "tranches": [9, 10, 11, 12, 13],
        "outcome_attribution_report": attribution,
        "counterfactual_learning_report": counterfactuals,
        "regime_conditioned_learning_report": regimes,
        "active_experiment_design_report": experiments,
        "learning_governance_report": governance,
        "next_batch_directive": {
            "decision": governance.get("decision"),
            "max_learning_influence": governance.get("max_learning_influence"),
            "top_experiment": experiments.get("top_experiment"),
            "top_regime": regimes.get("top_regime"),
            "top_attribution": attribution.get("top_route"),
        },
    }
