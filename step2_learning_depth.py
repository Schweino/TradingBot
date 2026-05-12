"""Learning-depth layer for Step 2.

Tranches 14-18 deepen the learning model: outcome labels, lesson survival,
portfolio learning, uncertainty/risk pricing, and promotion evidence hardening.
"""
from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from typing import Any


LEARNING_DEPTH_VERSION = "step2_learning_depth_v1"


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


def _holdout(row: dict[str, Any]) -> float:
    gate = row.get("holdout_gate") if isinstance(row.get("holdout_gate"), dict) else {}
    return _f(gate.get("holdout_pnl", row.get("holdout_pnl")))


def _gate_ok(row: dict[str, Any], key: str) -> bool:
    gate = row.get(key) if isinstance(row.get(key), dict) else {}
    return bool(gate.get("ok"))


def _lane(row: dict[str, Any]) -> str:
    variant = str(row.get("variant") or "")
    if variant.startswith("profit_"):
        parts = variant.split("_")
        return parts[1] if len(parts) > 1 else "profit"
    return variant.split("_", 1)[0] if variant else "unknown"


def _route_key(route: dict[str, Any]) -> str:
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


def _row_routes(row: dict[str, Any]) -> list[str]:
    out = [_route_key(route) for route in row.get("routes") or [] if isinstance(route, dict)]
    if not out and row.get("route_key"):
        out = [str(row.get("route_key"))]
    return [item for item in out if item]


def enhanced_target_label_report(
    rows: list[dict[str, Any]] | None,
    *,
    true_profit_report: dict[str, Any] | None = None,
    source: str = "step2_learning_depth",
) -> dict[str, Any]:
    target_pnl = _f((true_profit_report or {}).get("target_pnl"))
    labels = []
    counts: dict[str, int] = defaultdict(int)
    for row in rows or []:
        pnl = _f(row.get("step2_pnl"))
        trades = _i(row.get("step2_trades"))
        holdout = _holdout(row)
        min_ok = _gate_ok(row, "min_trade_gate")
        holdout_ok = _gate_ok(row, "holdout_gate")
        if pnl >= target_pnl > 0.0 and min_ok and holdout_ok:
            label = "true_profit_target_candidate"
        elif pnl > 0.0 and min_ok and holdout_ok:
            label = "promotion_survivor"
        elif pnl > 0.0 and holdout > 0.0:
            label = "sample_expansion_candidate"
        elif pnl > 0.0:
            label = "fragile_positive"
        elif holdout > 0.0:
            label = "holdout_disagreement"
        else:
            label = "negative_or_unproven"
        counts[label] += 1
        labels.append({
            "label_id": _stable_hash({"variant": row.get("variant"), "pnl": pnl, "holdout": holdout}),
            "variant": row.get("variant"),
            "lane": _lane(row),
            "target_label": label,
            "pnl": round(pnl, 6),
            "holdout_pnl": round(holdout, 6),
            "trades": trades,
            "min_trade_gate_ok": min_ok,
            "holdout_gate_ok": holdout_ok,
            "target_gap": round(max(0.0, target_pnl - pnl), 6) if target_pnl else 0.0,
            "learning_weight": round(
                max(0.05, min(1.5, (1.0 if pnl > 0.0 else 0.25) + (0.35 if holdout > 0.0 else -0.15) + min(0.25, trades / 1000.0))),
                6,
            ),
        })
    labels.sort(key=lambda row: (row["learning_weight"], row["holdout_pnl"], row["pnl"]), reverse=True)
    return {
        "schema_version": 1,
        "learning_depth_version": LEARNING_DEPTH_VERSION,
        "source": source,
        "tranche": 14,
        "label_count": len(labels),
        "label_counts": dict(sorted(counts.items())),
        "top_labels": labels[:100],
    }


def lesson_survival_decay_report(
    *,
    lesson_half_life_report: dict[str, Any] | None = None,
    family_survival_report: dict[str, Any] | None = None,
    belief_calibration_report: dict[str, Any] | None = None,
    source: str = "step2_learning_depth",
) -> dict[str, Any]:
    calibration = belief_calibration_report or {}
    belief_multiplier = _f((calibration.get("learning_adjustments") or {}).get("belief_weight_multiplier"), 1.0)
    lessons = []
    for row in (lesson_half_life_report or {}).get("lessons") or []:
        state = str(row.get("half_life_state") or "")
        confidence = _f(row.get("confidence"))
        base = {
            "fresh": 0.85,
            "durable": 0.9,
            "negative_memory_active": 0.75,
            "needs_revalidation": 0.45,
            "stale_or_uncertain": 0.35,
            "too_young_to_trust": 0.3,
            "fragile": 0.4,
            "decay": 0.25,
        }.get(state, 0.4)
        score = max(0.0, min(1.0, (base * 0.7 + confidence * 0.3) * belief_multiplier))
        lessons.append({
            "subject": row.get("subject") or row.get("lesson"),
            "memory_type": row.get("memory_type"),
            "half_life_state": state,
            "survival_score": round(score, 6),
            "confidence": round(confidence, 6),
            "refresh_action": row.get("refresh_action"),
            "decay_action": "retain" if score >= 0.7 else ("revalidate" if score >= 0.35 else "quarantine_or_retire"),
        })
    for row in (family_survival_report or {}).get("families") or []:
        survival = _f(row.get("holdout_survival_rate_pct")) / 100.0
        score = max(0.0, min(1.0, survival * belief_multiplier))
        lessons.append({
            "subject": row.get("family_key"),
            "memory_type": "family_survival",
            "half_life_state": row.get("survival_state"),
            "survival_score": round(score, 6),
            "confidence": round(survival, 6),
            "refresh_action": "expand_siblings" if score >= 0.65 else "controlled_revalidation",
            "decay_action": "retain" if score >= 0.7 else ("revalidate" if score >= 0.35 else "quarantine_or_retire"),
        })
    lessons.sort(key=lambda row: row["survival_score"], reverse=True)
    return {
        "schema_version": 1,
        "learning_depth_version": LEARNING_DEPTH_VERSION,
        "source": source,
        "tranche": 15,
        "lesson_count": len(lessons),
        "retain_count": sum(1 for row in lessons if row["decay_action"] == "retain"),
        "revalidate_count": sum(1 for row in lessons if row["decay_action"] == "revalidate"),
        "retire_count": sum(1 for row in lessons if row["decay_action"] == "quarantine_or_retire"),
        "lessons": lessons[:150],
    }


def portfolio_learning_report(
    rows: list[dict[str, Any]] | None,
    *,
    outcome_attribution_report: dict[str, Any] | None = None,
    source: str = "step2_learning_depth",
) -> dict[str, Any]:
    route_scores = {
        str(row.get("route_key")): _f(row.get("attribution_score"))
        for row in (outcome_attribution_report or {}).get("route_attribution") or []
    }
    exposures: dict[str, dict[str, Any]] = defaultdict(lambda: {"count": 0, "pnl_sum": 0.0, "holdout_sum": 0.0, "routes": set()})
    candidates = []
    for row in rows or []:
        pnl = _f(row.get("step2_pnl"))
        holdout = _holdout(row)
        routes = _row_routes(row)
        for key in [f"lane:{_lane(row)}"] + [f"route:{route}" for route in routes]:
            exposures[key]["count"] += 1
            exposures[key]["pnl_sum"] += pnl
            exposures[key]["holdout_sum"] += holdout
            for route in routes:
                exposures[key]["routes"].add(route)
        score = holdout * 2.0 + pnl + sum(route_scores.get(route, 0.0) for route in routes)
        candidates.append({
            "variant": row.get("variant"),
            "lane": _lane(row),
            "portfolio_score": round(score, 6),
            "route_count": len(routes),
            "routes": routes[:20],
            "pnl": round(pnl, 6),
            "holdout_pnl": round(holdout, 6),
            "role": "core" if holdout > 0.0 and pnl > 0.0 else ("hedge_or_probe" if holdout > 0.0 or pnl > 0.0 else "exclude"),
        })
    exposure_rows = []
    for key, item in exposures.items():
        count = max(1, _i(item["count"]))
        exposure_rows.append({
            "exposure": key,
            "count": item["count"],
            "avg_pnl": round(_f(item["pnl_sum"]) / count, 6),
            "avg_holdout_pnl": round(_f(item["holdout_sum"]) / count, 6),
            "route_diversity": len(item["routes"]),
            "exposure_state": "crowded" if item["count"] >= max(3, len(rows or []) * 0.5) else "balanced",
        })
    candidates.sort(key=lambda row: row["portfolio_score"], reverse=True)
    exposure_rows.sort(key=lambda row: (row["exposure_state"] == "crowded", row["count"]), reverse=True)
    return {
        "schema_version": 1,
        "learning_depth_version": LEARNING_DEPTH_VERSION,
        "source": source,
        "tranche": 16,
        "candidate_count": len(candidates),
        "core_count": sum(1 for row in candidates if row["role"] == "core"),
        "crowded_exposure_count": sum(1 for row in exposure_rows if row["exposure_state"] == "crowded"),
        "top_portfolio_candidates": candidates[:100],
        "exposures": exposure_rows[:100],
    }


def uncertainty_risk_pricing_report(
    *,
    uncertainty_heatmap_report: dict[str, Any] | None = None,
    uncertainty_model_report: dict[str, Any] | None = None,
    deployment_risk_report: dict[str, Any] | None = None,
    belief_calibration_report: dict[str, Any] | None = None,
    source: str = "step2_learning_depth",
) -> dict[str, Any]:
    rows = []
    belief_multiplier = _f((belief_calibration_report or {}).get("learning_adjustments", {}).get("belief_weight_multiplier"), 1.0)
    for row in (uncertainty_heatmap_report or {}).get("cells") or []:
        uncertainty = _f(row.get("uncertainty_score")) / 100.0
        price = max(0.0, min(1.0, uncertainty / max(0.25, belief_multiplier)))
        rows.append({
            "subject": row.get("cell_key"),
            "source": "uncertainty_heatmap",
            "uncertainty_price": round(price, 6),
            "risk_adjusted_learning_weight": round(max(0.05, 1.0 - price), 6),
            "recommended_action": row.get("recommended_action"),
        })
    for row in (uncertainty_model_report or {}).get("cells") or []:
        uncertainty = _f(row.get("uncertainty_score"))
        price = max(0.0, min(1.0, uncertainty / max(0.25, belief_multiplier)))
        rows.append({
            "subject": row.get("cell") or row.get("subject") or row.get("route_key"),
            "source": "uncertainty_model",
            "uncertainty_price": round(price, 6),
            "risk_adjusted_learning_weight": round(max(0.05, 1.0 - price), 6),
            "recommended_action": row.get("recommended_action"),
        })
    risk_state = str((deployment_risk_report or {}).get("risk_state") or (deployment_risk_report or {}).get("deployment_state") or "")
    if risk_state:
        deploy_price = 1.0 if risk_state in {"blocked", "kill_switch", "do_not_deploy"} else 0.35
        rows.append({
            "subject": "deployment_risk",
            "source": "deployment_risk_report",
            "uncertainty_price": deploy_price,
            "risk_adjusted_learning_weight": round(max(0.05, 1.0 - deploy_price), 6),
            "recommended_action": "block_scale" if deploy_price >= 1.0 else "paper_or_shadow_first",
        })
    rows.sort(key=lambda row: row["uncertainty_price"], reverse=True)
    avg_price = sum(_f(row.get("uncertainty_price")) for row in rows) / max(1, len(rows))
    return {
        "schema_version": 1,
        "learning_depth_version": LEARNING_DEPTH_VERSION,
        "source": source,
        "tranche": 17,
        "priced_cell_count": len(rows),
        "avg_uncertainty_price": round(avg_price, 6),
        "risk_budget_state": "tight" if avg_price >= 0.65 else ("moderate" if avg_price >= 0.35 else "normal"),
        "priced_cells": rows[:150],
    }


def promotion_evidence_hardening_report(
    *,
    promotion_evidence_gap_report: dict[str, Any] | None = None,
    statistical_validation_report: dict[str, Any] | None = None,
    world_class_readiness_report: dict[str, Any] | None = None,
    deployment_risk_report: dict[str, Any] | None = None,
    walk_forward_validation_bundle: dict[str, Any] | None = None,
    source: str = "step2_learning_depth",
) -> dict[str, Any]:
    candidates = []
    for row in (promotion_evidence_gap_report or {}).get("top_candidates") or []:
        gaps = list(row.get("gaps") or [])
        gap_names = [str(gap.get("gap")) for gap in gaps if isinstance(gap, dict)]
        hardening_score = max(0.0, 100.0 - len(gaps) * 15.0)
        candidates.append({
            "variant": row.get("variant"),
            "gap_count": len(gaps),
            "gaps": gap_names,
            "hardening_score": round(hardening_score, 6),
            "promotion_evidence_status": "hardened" if not gaps else "needs_more_evidence",
            "required_before_promotion": gap_names or ["independent_review"],
        })
    validation_count = len((walk_forward_validation_bundle or {}).get("validations") or [])
    stat_ok = bool((statistical_validation_report or {}).get("ok", True))
    world_ok = bool((world_class_readiness_report or {}).get("ok", False))
    risk_state = str((deployment_risk_report or {}).get("risk_state") or (deployment_risk_report or {}).get("deployment_state") or "")
    blockers = []
    if not stat_ok:
        blockers.append("statistical_validation_not_ok")
    if not world_ok:
        blockers.append("world_class_readiness_not_ok")
    if validation_count <= 0:
        blockers.append("walk_forward_validation_missing")
    if risk_state in {"blocked", "kill_switch", "do_not_deploy"}:
        blockers.append("deployment_risk_blocks_promotion")
    if any(row["gap_count"] > 0 for row in candidates[:5]):
        blockers.append("top_candidate_promotion_gaps_open")
    return {
        "schema_version": 1,
        "learning_depth_version": LEARNING_DEPTH_VERSION,
        "source": source,
        "tranche": 18,
        "candidate_count": len(candidates),
        "promotion_hardened": not blockers,
        "blockers": blockers,
        "validation_count": validation_count,
        "candidates": candidates[:100],
    }


def learning_depth_report(
    rows: list[dict[str, Any]] | None,
    *,
    true_profit_report: dict[str, Any] | None = None,
    lesson_half_life_report: dict[str, Any] | None = None,
    family_survival_report: dict[str, Any] | None = None,
    belief_calibration_report: dict[str, Any] | None = None,
    outcome_attribution_report: dict[str, Any] | None = None,
    uncertainty_heatmap_report: dict[str, Any] | None = None,
    uncertainty_model_report: dict[str, Any] | None = None,
    deployment_risk_report: dict[str, Any] | None = None,
    promotion_evidence_gap_report: dict[str, Any] | None = None,
    statistical_validation_report: dict[str, Any] | None = None,
    world_class_readiness_report: dict[str, Any] | None = None,
    walk_forward_validation_bundle: dict[str, Any] | None = None,
    source: str = "step2_learning_depth",
) -> dict[str, Any]:
    labels = enhanced_target_label_report(rows, true_profit_report=true_profit_report, source=source)
    survival = lesson_survival_decay_report(
        lesson_half_life_report=lesson_half_life_report,
        family_survival_report=family_survival_report,
        belief_calibration_report=belief_calibration_report,
        source=source,
    )
    portfolio = portfolio_learning_report(rows, outcome_attribution_report=outcome_attribution_report, source=source)
    risk = uncertainty_risk_pricing_report(
        uncertainty_heatmap_report=uncertainty_heatmap_report,
        uncertainty_model_report=uncertainty_model_report,
        deployment_risk_report=deployment_risk_report,
        belief_calibration_report=belief_calibration_report,
        source=source,
    )
    promotion = promotion_evidence_hardening_report(
        promotion_evidence_gap_report=promotion_evidence_gap_report,
        statistical_validation_report=statistical_validation_report,
        world_class_readiness_report=world_class_readiness_report,
        deployment_risk_report=deployment_risk_report,
        walk_forward_validation_bundle=walk_forward_validation_bundle,
        source=source,
    )
    return {
        "schema_version": 1,
        "learning_depth_version": LEARNING_DEPTH_VERSION,
        "source": source,
        "tranches": [14, 15, 16, 17, 18],
        "enhanced_target_label_report": labels,
        "lesson_survival_decay_report": survival,
        "portfolio_learning_report": portfolio,
        "uncertainty_risk_pricing_report": risk,
        "promotion_evidence_hardening_report": promotion,
        "next_batch_depth_directive": {
            "top_label": (labels.get("top_labels") or [None])[0],
            "top_surviving_lesson": (survival.get("lessons") or [None])[0],
            "top_portfolio_candidate": (portfolio.get("top_portfolio_candidates") or [None])[0],
            "risk_budget_state": risk.get("risk_budget_state"),
            "promotion_hardened": promotion.get("promotion_hardened"),
        },
    }
