"""Closed-loop causal controller for Step 2 learning decisions."""
from __future__ import annotations

from collections import defaultdict
from typing import Any


CONTROLLER_VERSION = "step2_closed_loop_controller_v1"


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _items(payload: dict[str, Any] | None, *keys: str) -> list[dict[str, Any]]:
    payload = payload if isinstance(payload, dict) else {}
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]
    return []


def _route(row: dict[str, Any]) -> str:
    return str(row.get("route_key") or row.get("route") or row.get("focus") or "unknown")


def _status_is_bad(status: Any) -> bool:
    return str(status or "").lower() in {"reject", "rejected", "failed", "blocked", "unsafe", "retire"}


def _status_is_good(status: Any) -> bool:
    return str(status or "").lower() in {"pass", "passed", "approved", "success", "worked", "scale"}


def _feedback_by_route(feedback_rows: list[dict[str, Any]] | None) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = defaultdict(lambda: {"total": 0.0, "rejects": 0.0, "approvals": 0.0})
    for row in feedback_rows or []:
        route = _route(row)
        out[route]["total"] += 1.0
        status = row.get("status") or row.get("decision") or row.get("verdict")
        if _status_is_bad(status):
            out[route]["rejects"] += 1.0
        if _status_is_good(status):
            out[route]["approvals"] += 1.0
    return out


def _validation_by_route(validation_rows: list[dict[str, Any]] | None) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = defaultdict(lambda: {"total": 0.0, "passes": 0.0, "metric_sum": 0.0})
    for row in validation_rows or []:
        route = _route(row)
        out[route]["total"] += 1.0
        if bool(row.get("passed")):
            out[route]["passes"] += 1.0
        metric = row.get("metric_value")
        if metric is None:
            metric = row.get("baseline_step2_pnl") or row.get("route_disabled_delta") or row.get("pnl")
        out[route]["metric_sum"] += _num(metric, 0.0)
    return out


def _outcomes_by_route(closed_loop_memory: dict[str, Any] | None) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = defaultdict(lambda: {"total": 0.0, "worked": 0.0, "reward_sum": 0.0})
    for row in _items(closed_loop_memory, "command_outcomes", "commands"):
        route = _route(row)
        out[route]["total"] += 1.0
        out[route]["worked"] += 1.0 if row.get("worked") else 0.0
        out[route]["reward_sum"] += _num(row.get("reward_score"), 0.0)
    return out


def _debt_by_route(experiment_debt: dict[str, Any] | None) -> dict[str, float]:
    out: dict[str, float] = defaultdict(float)
    for row in _items(experiment_debt, "queue", "debts"):
        out[_route(row)] += _num(row.get("priority_score"), 0.0)
    return out


def _experiment_by_route(experiments: dict[str, Any] | None) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = defaultdict(lambda: {"total": 0.0, "good": 0.0, "bad": 0.0})
    for row in _items(experiments, "experiments"):
        route = _route(row)
        verdict = row.get("verdict") or row.get("status") or row.get("next_action")
        out[route]["total"] += 1.0
        if _status_is_good(verdict) or str(verdict or "").lower() in {"scale_successful_treatment", "validated"}:
            out[route]["good"] += 1.0
        if _status_is_bad(verdict) or str(verdict or "").lower() in {"downweight_or_change_treatment", "abandon"}:
            out[route]["bad"] += 1.0
    return out


def route_controller(
    route_prior_model: dict[str, Any] | None = None,
    experiment_registry: dict[str, Any] | None = None,
    experiment_debt: dict[str, Any] | None = None,
    closed_loop_memory: dict[str, Any] | None = None,
    validation_rows: list[dict[str, Any]] | None = None,
    feedback_rows: list[dict[str, Any]] | None = None,
    *,
    batch_size: int = 500,
    limit: int = 25,
) -> dict[str, Any]:
    priors = _items(route_prior_model, "priors", "top_priors", "routes")
    feedback = _feedback_by_route(feedback_rows)
    validation = _validation_by_route(validation_rows)
    outcomes = _outcomes_by_route(closed_loop_memory)
    debt = _debt_by_route(experiment_debt)
    experiments = _experiment_by_route(experiment_registry)
    all_routes = {
        _route(row) for row in priors
    } | set(feedback) | set(validation) | set(outcomes) | set(debt) | set(experiments)
    decisions = []
    for route in sorted(route for route in all_routes if route and route != "unknown"):
        prior = next((row for row in priors if _route(row) == route), {})
        prior_score = _num(prior.get("prior_score") or prior.get("confidence") or prior.get("avg_pnl"), 0.0)
        pass_rate = _num(prior.get("validation_pass_rate"), 0.0)
        reject_rate = _num(prior.get("reject_rate"), 0.0)
        if validation[route]["total"]:
            pass_rate = validation[route]["passes"] / max(1.0, validation[route]["total"])
        if feedback[route]["total"]:
            reject_rate = feedback[route]["rejects"] / max(1.0, feedback[route]["total"])
        avg_reward = outcomes[route]["reward_sum"] / max(1.0, outcomes[route]["total"])
        worked_rate = outcomes[route]["worked"] / max(1.0, outcomes[route]["total"])
        exp_good = experiments[route]["good"]
        exp_bad = experiments[route]["bad"]
        debt_score = debt.get(route, 0.0)
        score = (
            prior_score * 0.35
            + pass_rate * 140.0
            + avg_reward * 3.0
            + worked_rate * 45.0
            + exp_good * 35.0
            - reject_rate * 180.0
            - exp_bad * 45.0
            + min(45.0, debt_score * 0.20)
        )
        if reject_rate >= 0.60 or (pass_rate < 0.25 and validation[route]["total"] >= 2) or exp_bad >= 2:
            decision = "retire_or_quarantine"
        elif debt_score >= 70.0 or validation[route]["total"] == 0:
            decision = "controlled_probe"
        elif score >= 120.0 and reject_rate < 0.35 and pass_rate >= 0.50:
            decision = "scale"
        elif pass_rate < 0.50 or avg_reward < 0.0:
            decision = "repair_before_more_budget"
        else:
            decision = "watch"
        decisions.append({
            "route_key": route,
            "decision": decision,
            "controller_score": round(score, 4),
            "prior_score": round(prior_score, 4),
            "validation_pass_rate": round(pass_rate, 4),
            "validation_count": int(validation[route]["total"]),
            "reject_rate": round(reject_rate, 4),
            "feedback_count": int(feedback[route]["total"]),
            "avg_command_reward": round(avg_reward, 4),
            "command_worked_rate": round(worked_rate, 4),
            "experiment_good_count": int(exp_good),
            "experiment_bad_count": int(exp_bad),
            "open_debt_priority": round(debt_score, 4),
        })
    decisions.sort(key=lambda row: ({"scale": 4, "controlled_probe": 3, "repair_before_more_budget": 2, "watch": 1, "retire_or_quarantine": 0}.get(row["decision"], 0), row["controller_score"]), reverse=True)
    budgetable = [row for row in decisions if row["decision"] in {"scale", "controlled_probe", "repair_before_more_budget"}]
    weights = {
        "scale": 3.0,
        "controlled_probe": 2.0,
        "repair_before_more_budget": 1.25,
    }
    total_weight = sum(weights.get(row["decision"], 0.5) * max(1.0, row["controller_score"] + 50.0) for row in budgetable) or 1.0
    allocation = []
    assigned = 0
    for idx, row in enumerate(budgetable[:limit]):
        weight = weights.get(row["decision"], 0.5) * max(1.0, row["controller_score"] + 50.0)
        budget = int(round(int(batch_size) * weight / total_weight))
        if idx == len(budgetable[:limit]) - 1:
            budget = max(0, int(batch_size) - assigned)
        assigned += budget
        allocation.append({
            "route_key": row["route_key"],
            "decision": row["decision"],
            "variant_budget": budget,
            "controller_score": row["controller_score"],
        })
    return {
        "schema_version": 1,
        "controller_version": CONTROLLER_VERSION,
        "source": "step2_closed_loop_controller",
        "route_count": len(decisions),
        "decisions": decisions[:limit],
        "scale_routes": [row["route_key"] for row in decisions if row["decision"] == "scale"][:limit],
        "probe_routes": [row["route_key"] for row in decisions if row["decision"] == "controlled_probe"][:limit],
        "repair_routes": [row["route_key"] for row in decisions if row["decision"] == "repair_before_more_budget"][:limit],
        "retire_routes": [row["route_key"] for row in decisions if row["decision"] == "retire_or_quarantine"][:limit],
        "allocation": allocation,
        "batch_size": int(batch_size),
        "selection_policy": [
            "scale only when validation, command reward, and low reject evidence agree",
            "probe high-priority debt or unvalidated high-prior routes with controls",
            "repair before more budget when validation or command outcomes weaken",
            "retire routes with high reject rate or repeated bad experiment outcomes",
        ],
    }


def action_controller(closed_loop_memory: dict[str, Any] | None = None, *, limit: int = 12) -> dict[str, Any]:
    rows = _items(closed_loop_memory, "action_league")
    if not rows:
        rows = _items(closed_loop_memory, "reward_calibration")
    decisions = []
    for row in rows:
        action = str(row.get("action") or "unknown")
        rating = _num(row.get("rating"), 1500.0)
        sample_count = int(_num(row.get("sample_count"), 0.0))
        success_rate = _num(row.get("success_rate"), 0.0)
        abs_error = _num(row.get("abs_error"), 0.0)
        score = (rating - 1500.0) / 10.0 + success_rate * 75.0 + min(25.0, sample_count * 2.0) - abs_error
        if sample_count == 0:
            decision = "probe"
        elif score >= 50.0 and success_rate >= 0.50:
            decision = "prefer"
        elif success_rate < 0.25 and sample_count >= 3:
            decision = "downweight"
        else:
            decision = "watch"
        decisions.append({
            "action": action,
            "decision": decision,
            "controller_score": round(score, 4),
            "rating": round(rating, 4),
            "sample_count": sample_count,
            "success_rate": round(success_rate, 4),
            "abs_error": round(abs_error, 4),
        })
    decisions.sort(key=lambda row: ({"prefer": 3, "probe": 2, "watch": 1, "downweight": 0}.get(row["decision"], 0), row["controller_score"]), reverse=True)
    return {
        "schema_version": 1,
        "controller_version": CONTROLLER_VERSION,
        "source": "step2_closed_loop_controller",
        "decisions": decisions[:limit],
        "preferred_actions": [row["action"] for row in decisions if row["decision"] == "prefer"][:limit],
        "downweighted_actions": [row["action"] for row in decisions if row["decision"] == "downweight"][:limit],
    }


def controller_report(
    *,
    route_prior_model: dict[str, Any] | None = None,
    experiment_registry: dict[str, Any] | None = None,
    experiment_debt: dict[str, Any] | None = None,
    closed_loop_memory: dict[str, Any] | None = None,
    validation_rows: list[dict[str, Any]] | None = None,
    feedback_rows: list[dict[str, Any]] | None = None,
    batch_size: int = 500,
    limit: int = 25,
) -> dict[str, Any]:
    route_decisions = route_controller(
        route_prior_model,
        experiment_registry,
        experiment_debt,
        closed_loop_memory,
        validation_rows,
        feedback_rows,
        batch_size=batch_size,
        limit=limit,
    )
    action_decisions = action_controller(closed_loop_memory, limit=limit)
    return {
        "schema_version": 1,
        "controller_version": CONTROLLER_VERSION,
        "source": "step2_closed_loop_controller",
        "route_controller": route_decisions,
        "action_controller": action_decisions,
        "next_run_directives": {
            "scale_routes": route_decisions.get("scale_routes") or [],
            "probe_routes": route_decisions.get("probe_routes") or [],
            "repair_routes": route_decisions.get("repair_routes") or [],
            "retire_routes": route_decisions.get("retire_routes") or [],
            "preferred_actions": action_decisions.get("preferred_actions") or [],
            "downweighted_actions": action_decisions.get("downweighted_actions") or [],
            "allocation": route_decisions.get("allocation") or [],
        },
    }
