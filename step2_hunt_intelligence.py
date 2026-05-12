"""Shared ranking and learning helpers for Step 2 hunts."""
from __future__ import annotations

import json
import math
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import step2_objective
import step2_data_quality
import step2_deployment_risk
import step2_void_registry
import step2_statistical_validation
import step2_world_class_audit
import tournament_safety


ROUTE_VARIANT_RE = re.compile(r"^[^_]+_\d+_(?P<ticker>[^|]+)\|(?P<setup>[^|]+)\|(?P<session>[^|]+)")


def read_json(path: str | Path, default: Any = None) -> Any:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except Exception:
        return default


def walk_rows(payload: Any):
    if isinstance(payload, dict):
        if isinstance(payload.get("weights"), dict) and ("step2_pnl" in payload or "decision_full" in payload):
            yield payload
        for key in (
            "leaderboard",
            "winners",
            "milestones",
            "results",
            "candidates",
            "rows",
            "robustness_adjusted_leaderboard",
            "recommended_leaderboard",
        ):
            if key in payload:
                yield from walk_rows(payload.get(key))
    elif isinstance(payload, list):
        for item in payload:
            yield from walk_rows(item)


def pnl(row: dict[str, Any]) -> float:
    full = row.get("decision_full") if isinstance(row.get("decision_full"), dict) else {}
    return float(row.get("step2_pnl") if row.get("step2_pnl") is not None else full.get("pnl") or -1e18)


def trade_count(row: dict[str, Any]) -> int:
    full = row.get("decision_full") if isinstance(row.get("decision_full"), dict) else {}
    return int(row.get("step2_trades") if row.get("step2_trades") is not None else full.get("trades") or 0)


def win_rate(row: dict[str, Any]) -> float:
    full = row.get("decision_full") if isinstance(row.get("decision_full"), dict) else {}
    return float(row.get("step2_win_rate_pct") if row.get("step2_win_rate_pct") is not None else full.get("win_rate_pct") or 0.0)


def live_delta(row: dict[str, Any], active_pnl: float | None = None) -> float:
    if row.get("step2_delta_vs_active") is not None:
        return float(row.get("step2_delta_vs_active") or 0.0)
    robustness = row.get("robustness") if isinstance(row.get("robustness"), dict) else {}
    if robustness.get("delta_vs_active") is not None:
        return float(robustness.get("delta_vs_active") or 0.0)
    if active_pnl is not None:
        return pnl(row) - float(active_pnl)
    return 0.0


def beats_current_live(row: dict[str, Any], active_pnl: float | None = None) -> bool:
    if row.get("beats_current_live") is True or row.get("beats_active") is True:
        return True
    if row.get("beats_current_live") is False or row.get("beats_active") is False:
        return False
    if row.get("step2_delta_vs_active") is not None:
        return float(row.get("step2_delta_vs_active") or 0.0) > 0.0
    if row.get("step2_delta_pct_vs_active") is not None:
        return float(row.get("step2_delta_pct_vs_active") or 0.0) > 0.0
    robustness = row.get("robustness") if isinstance(row.get("robustness"), dict) else {}
    if robustness.get("delta_pct_vs_active") is not None:
        return float(robustness.get("delta_pct_vs_active") or 0.0) > 0.0
    if active_pnl is not None:
        return pnl(row) > float(active_pnl)
    return False


def config_key(row: dict[str, Any]) -> str:
    return tournament_safety.stable_json_hash(
        {
            "weights": row.get("weights") or {},
            "bias": float(row.get("bias") or 0.0),
            "routes": row.get("routes") or [],
        },
        length=32,
    )


def lineage_info(row: dict[str, Any]) -> dict[str, Any]:
    raw = row.get("lineage") if isinstance(row.get("lineage"), dict) else {}
    parent_variant = raw.get("parent_variant") or row.get("parent_variant") or ""
    parent_behavior = raw.get("parent_behavior_key") or row.get("parent_behavior_key") or ""
    lane = raw.get("mutation_lane") or row.get("mutation_lane") or ""
    worker = raw.get("worker_role") or row.get("worker_role") or ""
    route_seed = raw.get("route_seed") or row.get("route_seed") or route_key_from_row(row)
    generation = raw.get("generation") if raw.get("generation") is not None else row.get("generation")
    mutation_scale = raw.get("mutation_scale") if raw.get("mutation_scale") is not None else row.get("mutation_scale")
    return {
        "parent_variant": str(parent_variant or ""),
        "parent_behavior_key": str(parent_behavior or ""),
        "mutation_lane": str(lane or "unknown"),
        "mutation_reason": str(raw.get("mutation_reason") or row.get("mutation_reason") or ""),
        "worker_role": str(worker or ""),
        "generation": int(generation or 0),
        "route_seed": str(route_seed or ""),
        "mutation_scale": float(mutation_scale or 0.0),
        "family_key": str(raw.get("family_key") or row.get("family_key") or ""),
        "ancestor_path": list(raw.get("ancestor_path") or row.get("ancestor_path") or []),
    }


def candidate_family_key(row: dict[str, Any]) -> str:
    lineage = lineage_info(row)
    if lineage.get("family_key"):
        return str(lineage["family_key"])
    parent = lineage.get("parent_behavior_key") or lineage.get("parent_variant")
    route = lineage.get("route_seed") or route_key_from_row(row)
    lane = lineage.get("mutation_lane") or "unknown"
    weights = row.get("weights") if isinstance(row.get("weights"), dict) else {}
    top = sorted(weights.items(), key=lambda kv: abs(float(kv[1] or 0.0)), reverse=True)[:3]
    signature = ",".join(f"{name}:{'+' if float(value or 0.0) >= 0 else '-'}" for name, value in top)
    return tournament_safety.stable_json_hash(
        {
            "parent": parent,
            "route": route,
            "lane": lane,
            "signature": signature,
        },
        length=24,
    )


def _compact_day_map(row: dict[str, Any]) -> dict[str, Any]:
    out = {}
    by_day = row.get("by_day") if isinstance(row.get("by_day"), dict) else {}
    for day, payload in sorted(by_day.items()):
        if isinstance(payload, dict):
            out[str(day)] = {
                "pnl": round(float(payload.get("pnl") or 0.0), 2),
                "trades": int(payload.get("trades") or 0),
            }
    return out


def _compact_ticker_map(row: dict[str, Any]) -> dict[str, Any]:
    out = {}
    by_ticker = row.get("by_ticker") if isinstance(row.get("by_ticker"), dict) else {}
    for ticker, payload in sorted(by_ticker.items()):
        if isinstance(payload, dict):
            out[str(ticker)] = {
                "pnl": round(float(payload.get("pnl") or 0.0), 2),
                "trades": int(payload.get("trades") or 0),
            }
    return out


def behavior_key(row: dict[str, Any]) -> str:
    return tournament_safety.stable_json_hash(
        {
            "pnl": round(pnl(row), 2),
            "trades": trade_count(row),
            "win_rate_pct": round(win_rate(row), 4),
            "by_day": _compact_day_map(row),
            "by_ticker": _compact_ticker_map(row),
        },
        length=32,
    )


def _positive_day_rate(row: dict[str, Any]) -> float:
    days = _compact_day_map(row)
    if not days:
        return 0.0
    positives = sum(1 for payload in days.values() if float(payload.get("pnl") or 0.0) > 0.0)
    return positives / max(1, len(days))


def _concentration(row: dict[str, Any], key: str) -> float:
    payload = row.get(key) if isinstance(row.get(key), dict) else {}
    values = [abs(float(v.get("pnl") or 0.0)) for v in payload.values() if isinstance(v, dict)]
    total = sum(values)
    return max(values) / total if total else 1.0


def route_narrowness(row: dict[str, Any]) -> float:
    match = route_match(row)
    if not match:
        return 0.0
    populated = sum(1 for value in match.values() if value)
    audit = row.get("route_audit") if isinstance(row.get("route_audit"), dict) else {}
    share = None
    routes = audit.get("routes") if isinstance(audit.get("routes"), list) else []
    for route in routes:
        if isinstance(route, dict) and route.get("route") != "fallback":
            share = float(route.get("opportunity_share_pct") or 0.0)
            break
    if share is None:
        share = 1.0 if populated >= 3 else 10.0
    specificity = populated / 3.0
    narrow_share = 1.0 - min(1.0, share / 10.0)
    return round(max(0.0, min(1.0, specificity * 0.55 + narrow_share * 0.45)), 4)


def overfit_risk_score(row: dict[str, Any]) -> float:
    robustness = row.get("robustness") if isinstance(row.get("robustness"), dict) else {}
    risk = 0.0
    risk += route_narrowness(row) * 28.0
    risk += max(0.0, _concentration(row, "by_ticker") - 0.45) * 35.0
    risk += max(0.0, _concentration(row, "by_side") - 0.60) * 22.0
    risk += max(0.0, 0.75 - _positive_day_rate(row)) * 30.0
    risk += 12.0 if int(row.get("behavior_alias_count") or 1) >= 5 else 0.0
    risk += 12.0 if trade_count(row) < 1000 else 0.0
    risk += 12.0 if any(abs(float(v or 0.0)) >= 3.95 for v in (row.get("weights") or {}).values()) else 0.0
    if float(robustness.get("holdout_delta_vs_active") or 0.0) < 0.0:
        risk += 18.0
    return round(max(0.0, min(100.0, risk)), 4)


def quality_score(row: dict[str, Any], active_pnl: float | None = None) -> float:
    delta = max(0.0, live_delta(row, active_pnl))
    robustness = row.get("robustness") if isinstance(row.get("robustness"), dict) else {}
    robustness_score = float(robustness.get("score") or 0.0)
    holdout_delta = max(0.0, float(robustness.get("holdout_delta_vs_active") or 0.0))
    day_rate = float(robustness.get("positive_day_rate") or _positive_day_rate(row))
    ticker_balance = 1.0 - min(1.0, _concentration(row, "by_ticker"))
    side_balance = 1.0 - min(1.0, _concentration(row, "by_side"))
    sample = min(1.0, trade_count(row) / 5000.0)
    return round(
        delta
        + holdout_delta * 0.25
        + robustness_score * 20.0
        + day_rate * 500.0
        + ticker_balance * 250.0
        + side_balance * 150.0
        + sample * 100.0,
        4,
    )


def _robustness_dict(row: dict[str, Any]) -> dict[str, Any]:
    value = row.get("robustness")
    return value if isinstance(value, dict) else {}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def promotion_readiness_score(row: dict[str, Any], active_pnl: float | None = None) -> float:
    """0-100 proxy for whether a live-beating candidate is worth promotion review."""
    robustness = _robustness_dict(row)
    adjusted = robustness.get("adjusted_score")
    if adjusted is None:
        adjusted = robustness.get("robustness_adjusted_score")
    if adjusted is None:
        adjusted = robustness.get("score")
    robustness_score = max(0.0, min(100.0, _safe_float(adjusted, 0.0)))
    delta_pct = row.get("step2_delta_pct_vs_active")
    if delta_pct is None and active_pnl:
        delta_pct = (pnl(row) / float(active_pnl) - 1.0) * 100.0
    delta_pct = _safe_float(delta_pct, 0.0)
    components = robustness.get("components") if isinstance(robustness.get("components"), dict) else {}
    holdout_points = _safe_float(components.get("holdout"), 0.0)
    day_points = _safe_float(components.get("day_consistency"), 0.0)
    ticker_points = _safe_float(components.get("ticker_balance"), 0.0)
    side_points = _safe_float(components.get("side_balance"), 0.0)
    trade_points = _safe_float(components.get("trade_sample"), 0.0)
    route_safety = row.get("routed_profile_safety") if isinstance(row.get("routed_profile_safety"), dict) else {}
    route_bonus = 5.0 if not route_safety or route_safety.get("ok") is not False else -12.0
    risk_penalty = max(0.0, _safe_float(row.get("overfit_risk_score"), overfit_risk_score(row)) - 45.0) * 0.25
    red_flags = robustness.get("red_flags") if isinstance(robustness.get("red_flags"), list) else []
    blocker_penalty = 5.0 * len(red_flags)
    score = (
        robustness_score * 0.42
        + min(18.0, max(0.0, delta_pct) * 4.0)
        + min(12.0, holdout_points / 20.0 * 12.0)
        + min(10.0, day_points / 25.0 * 10.0)
        + min(7.0, (ticker_points + side_points) / 20.0 * 7.0)
        + min(6.0, trade_points / 5.0 * 6.0)
        + route_bonus
        - risk_penalty
        - blocker_penalty
    )
    if not beats_current_live(row, active_pnl):
        score = min(score, 35.0)
    return round(max(0.0, min(100.0, score)), 4)


def candidate_learning_tags(row: dict[str, Any], active_pnl: float | None = None) -> list[str]:
    tags: list[str] = []
    robustness = _robustness_dict(row)
    components = robustness.get("components") if isinstance(robustness.get("components"), dict) else {}
    delta_pct = row.get("step2_delta_pct_vs_active")
    if delta_pct is None and active_pnl:
        delta_pct = (pnl(row) / float(active_pnl) - 1.0) * 100.0
    delta_pct = _safe_float(delta_pct, 0.0)
    readiness = promotion_readiness_score(row, active_pnl)
    if beats_current_live(row, active_pnl):
        tags.append("beats_live")
    else:
        tags.append("does_not_beat_live")
    if readiness >= 75.0:
        tags.append("promotion_ready")
    elif readiness >= 55.0:
        tags.append("promotion_near_miss")
    else:
        tags.append("promotion_weak")
    holdout = _safe_float(components.get("holdout"), 0.0)
    if holdout >= 14.0:
        tags.append("strong_holdout")
    elif holdout > 0.0:
        tags.append("thin_holdout_edge")
    else:
        tags.append("no_holdout_credit")
    if delta_pct >= 5.0:
        tags.append("large_total_edge")
    elif delta_pct > 0.0:
        tags.append("small_total_edge")
    day = _safe_float(components.get("day_consistency"), 0.0)
    if day >= 20.0 or _positive_day_rate(row) >= 0.80:
        tags.append("day_consistent")
    else:
        tags.append("weak_day_consistency")
    if _concentration(row, "by_ticker") <= 0.55:
        tags.append("ticker_balanced")
    else:
        tags.append("ticker_concentrated")
    if _concentration(row, "by_side") <= 0.70:
        tags.append("side_balanced")
    else:
        tags.append("side_concentrated")
    if trade_count(row) >= 1000:
        tags.append("sample_ok")
    else:
        tags.append("thin_sample")
    if route_narrowness(row) >= 0.65:
        tags.append("route_narrow")
    elif route_key_from_row(row) != "unrouted":
        tags.append("route_broader")
    overfit = _safe_float(row.get("overfit_risk_score"), overfit_risk_score(row))
    if overfit >= 65.0:
        tags.append("high_overfit_risk")
    elif overfit <= 35.0:
        tags.append("low_overfit_risk")
    for flag in robustness.get("red_flags") or []:
        tags.append(f"robustness_flag:{flag}")
    return sorted(set(tags))


def promotion_approve_probability(row: dict[str, Any], active_pnl: float | None = None) -> float:
    tags = set(row.get("learning_tags") or candidate_learning_tags(row, active_pnl))
    readiness = float(row.get("promotion_readiness_score") or promotion_readiness_score(row, active_pnl))
    risk = float(row.get("overfit_risk_score") or overfit_risk_score(row))
    prob = 0.05 + readiness / 100.0 * 0.70
    if "promotion_ready" in tags:
        prob += 0.12
    if "strong_holdout" in tags:
        prob += 0.08
    if "day_consistent" in tags:
        prob += 0.05
    if "ticker_balanced" in tags:
        prob += 0.03
    if "thin_holdout_edge" in tags:
        prob -= 0.10
    if "no_holdout_credit" in tags:
        prob -= 0.18
    if "weak_day_consistency" in tags:
        prob -= 0.12
    if "high_overfit_risk" in tags:
        prob -= 0.16
    if "route_narrow" in tags:
        prob -= 0.06
    prob -= max(0.0, risk - 50.0) / 100.0 * 0.18
    if not beats_current_live(row, active_pnl):
        prob = min(prob, 0.05)
    return round(max(0.01, min(0.98, prob)), 4)


def expected_promotable_pnl(row: dict[str, Any], active_pnl: float | None = None) -> float:
    approve = promotion_approve_probability(row, active_pnl)
    readiness_survival = max(0.05, float(row.get("promotion_readiness_score") or promotion_readiness_score(row, active_pnl)) / 100.0)
    robustness_survival = max(0.10, 1.0 - float(row.get("overfit_risk_score") or overfit_risk_score(row)) / 140.0)
    novelty_credit = 1.0 + min(0.20, float(row.get("novelty_score") or 0.0) / 500.0)
    return round(max(0.0, pnl(row)) * approve * readiness_survival * robustness_survival * novelty_credit, 4)


def novelty_score(row: dict[str, Any], prior_rows: list[dict[str, Any]] | None = None) -> float:
    """Score behavior/config novelty against already-kept candidates."""
    prior_rows = prior_rows or []
    if not prior_rows:
        return 100.0
    row_route = route_key_from_row(row)
    row_behavior = str(row.get("behavior_key") or behavior_key(row))
    row_weights = row.get("weights") if isinstance(row.get("weights"), dict) else {}
    min_distance = 1.0
    route_seen = False
    behavior_seen = False
    for prior in prior_rows:
        if row_behavior == str(prior.get("behavior_key") or behavior_key(prior)):
            behavior_seen = True
        if row_route == route_key_from_row(prior):
            route_seen = True
        prior_weights = prior.get("weights") if isinstance(prior.get("weights"), dict) else {}
        keys = set(row_weights) | set(prior_weights)
        if keys:
            dist = sum(abs(float(row_weights.get(k, 0.0) or 0.0) - float(prior_weights.get(k, 0.0) or 0.0)) for k in keys)
            norm = dist / max(1, len(keys))
            min_distance = min(min_distance, min(1.0, norm / 2.0))
    score = 100.0 * min_distance
    if route_seen:
        score *= 0.65
    if behavior_seen:
        score *= 0.10
    return round(score, 4)


def decorate_row(row: dict[str, Any], *, active_pnl: float | None = None, source_path: str = "") -> dict[str, Any]:
    out = dict(row)
    out["step2_pnl"] = pnl(out)
    out["step2_trades"] = trade_count(out)
    out["step2_win_rate_pct"] = win_rate(out)
    out["step2_delta_vs_active"] = round(live_delta(out, active_pnl), 4)
    if active_pnl:
        out["step2_delta_pct_vs_active"] = round((out["step2_pnl"] / float(active_pnl) - 1.0) * 100.0, 4)
    out["beats_current_live"] = beats_current_live(out, active_pnl)
    out["behavior_key"] = behavior_key(out)
    out["config_key"] = config_key(out)
    lineage = lineage_info(out)
    if not lineage.get("family_key"):
        lineage["family_key"] = candidate_family_key(out)
    out["lineage"] = lineage
    out["family_key"] = lineage["family_key"]
    out["promotion_quality_score"] = quality_score(out, active_pnl)
    out["promotion_readiness_score"] = promotion_readiness_score(out, active_pnl)
    out["route_narrowness"] = route_narrowness(out)
    out["overfit_risk_score"] = overfit_risk_score(out)
    out["learning_tags"] = candidate_learning_tags(out, active_pnl)
    out["promotion_approve_probability"] = promotion_approve_probability(out, active_pnl)
    out["expected_promotable_pnl"] = expected_promotable_pnl(out, active_pnl)
    objective = step2_objective.candidate_objective(out, active_pnl=active_pnl)
    out["execution_adjusted_objective"] = objective
    out["execution_adjusted_pnl"] = objective["execution_adjusted_pnl"]
    out["objective_score"] = objective["objective_score"]
    out["replay_validity"] = objective["replay_validity"]
    data_contract = step2_data_quality.row_contract(out, source_path=source_path or out.get("hunt_source_path") or out.get("source"))
    out["data_quality_contract"] = data_contract
    out["data_quality_score"] = data_contract["score"]
    out["learning_allowed"] = bool(data_contract["learning_allowed"])
    statistical = step2_statistical_validation.candidate_validation(out)
    out["statistical_validation"] = statistical
    out["statistical_validation_score"] = statistical["score"]
    deployment = step2_deployment_risk.candidate_risk(out)
    out["deployment_risk"] = deployment
    out["deployment_risk_score"] = deployment["risk_score"]
    if not objective["replay_validity"].get("ok"):
        tags = set(out.get("learning_tags") or [])
        tags.add("replay_validity_blocked")
        for blocker in objective["replay_validity"].get("blockers") or []:
            tags.add(f"replay_validity:{blocker}")
        out["learning_tags"] = sorted(tags)
    if data_contract.get("blockers") or data_contract.get("warnings"):
        tags = set(out.get("learning_tags") or [])
        for blocker in data_contract.get("blockers") or []:
            tags.add(f"data_quality_blocker:{blocker}")
        for warning in data_contract.get("warnings") or []:
            tags.add(f"data_quality_warning:{warning}")
        out["learning_tags"] = sorted(tags)
    if statistical.get("blockers"):
        tags = set(out.get("learning_tags") or [])
        for blocker in statistical.get("blockers") or []:
            tags.add(f"statistical_blocker:{blocker}")
        out["learning_tags"] = sorted(tags)
    if deployment.get("blockers") or deployment.get("warnings"):
        tags = set(out.get("learning_tags") or [])
        for blocker in deployment.get("blockers") or []:
            tags.add(f"deployment_blocker:{blocker}")
        for warning in deployment.get("warnings") or []:
            tags.add(f"deployment_warning:{warning}")
        out["learning_tags"] = sorted(tags)
    if source_path:
        out["hunt_source_path"] = source_path
    return out


def _dedupe_by_config(rows: list[dict[str, Any]], *, score_key: Any = pnl) -> list[dict[str, Any]]:
    best_by_key: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(row.get("config_key") or config_key(row))
        old = best_by_key.get(key)
        if old is None or score_key(row) > score_key(old):
            best_by_key[key] = row
    return sorted(best_by_key.values(), key=score_key, reverse=True)


def dedupe_by_behavior(rows: list[dict[str, Any]], *, score_key: Any = pnl) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("behavior_key") or behavior_key(row))].append(row)
    out = []
    for key, aliases in grouped.items():
        aliases = sorted(aliases, key=score_key, reverse=True)
        leader = dict(aliases[0])
        leader["behavior_key"] = key
        leader["behavior_alias_count"] = len(aliases)
        leader["behavior_aliases"] = [
            {
                "variant": alias.get("variant"),
                "config_key": alias.get("config_key") or config_key(alias),
                "source": alias.get("hunt_source_path"),
            }
            for alias in aliases[:25]
        ]
        out.append(leader)
    out.sort(key=score_key, reverse=True)
    return out


def _objective_rank_key(row: dict[str, Any]) -> tuple[float, float]:
    return (float(row.get("objective_score") or -1e18), pnl(row))


def _statistical_rank_key(row: dict[str, Any]) -> tuple[float, float, float]:
    return (
        float(row.get("statistical_validation_score") or -1e18),
        float(row.get("objective_score") or -1e18),
        pnl(row),
    )


def _deployment_rank_key(row: dict[str, Any]) -> tuple[float, float, float]:
    return (
        float(row.get("deployment_risk_score") or -1e18),
        float(row.get("statistical_validation_score") or -1e18),
        float(row.get("objective_score") or -1e18),
    )


def rank_rows(
    rows: list[dict[str, Any]],
    *,
    active_pnl: float | None = None,
    live_only: bool = True,
    behavioral_dedupe: bool = True,
    limit: int = 100,
    include_voided_for_forensics: bool = False,
) -> dict[str, Any]:
    candidate_rows, voided_rows = step2_void_registry.filter_clean_rows(
        rows,
        include_voided_for_forensics=include_voided_for_forensics,
    )
    decorated = [
        decorate_row(row, active_pnl=active_pnl, source_path=str(row.get("hunt_source_path") or ""))
        for row in candidate_rows
    ]
    if live_only:
        decorated = [row for row in decorated if row.get("beats_current_live")]
    for row in decorated:
        statistical = step2_statistical_validation.candidate_validation(row, tested_count=len(decorated))
        row["statistical_validation"] = statistical
        row["statistical_validation_score"] = statistical["score"]
        deployment = step2_deployment_risk.candidate_risk(row)
        row["deployment_risk"] = deployment
        row["deployment_risk_score"] = deployment["risk_score"]
    config_ranked = _dedupe_by_config(decorated)
    final_rows = dedupe_by_behavior(config_ranked) if behavioral_dedupe else config_ranked
    execution_config_ranked = _dedupe_by_config(decorated, score_key=_objective_rank_key)
    execution_rows = (
        dedupe_by_behavior(execution_config_ranked, score_key=_objective_rank_key)
        if behavioral_dedupe
        else execution_config_ranked
    )
    statistical_config_ranked = _dedupe_by_config(decorated, score_key=_statistical_rank_key)
    statistical_rows = (
        dedupe_by_behavior(statistical_config_ranked, score_key=_statistical_rank_key)
        if behavioral_dedupe
        else statistical_config_ranked
    )
    deployment_config_ranked = _dedupe_by_config(decorated, score_key=_deployment_rank_key)
    deployment_rows = (
        dedupe_by_behavior(deployment_config_ranked, score_key=_deployment_rank_key)
        if behavioral_dedupe
        else deployment_config_ranked
    )
    raw_ranked = sorted(final_rows, key=pnl, reverse=True)
    for idx, row in enumerate(raw_ranked):
        row["novelty_score"] = novelty_score(row, raw_ranked[:idx])
    quality_ranked = sorted(final_rows, key=lambda row: float(row.get("promotion_quality_score") or 0.0), reverse=True)
    readiness_ranked = sorted(final_rows, key=lambda row: (float(row.get("promotion_readiness_score") or 0.0), pnl(row)), reverse=True)
    novelty_ranked = sorted(final_rows, key=lambda row: (float(row.get("novelty_score") or 0.0), pnl(row)), reverse=True)
    promotable_ranked = sorted(final_rows, key=lambda row: (float(row.get("expected_promotable_pnl") or 0.0), pnl(row)), reverse=True)
    execution_ranked = sorted(execution_rows, key=_objective_rank_key, reverse=True)
    statistical_ranked = sorted(statistical_rows, key=_statistical_rank_key, reverse=True)
    deployment_ranked = sorted(deployment_rows, key=_deployment_rank_key, reverse=True)
    result = {
        "raw_leaderboard": raw_ranked[:limit],
        "promotion_quality_leaderboard": quality_ranked[:limit],
        "promotion_readiness_leaderboard": readiness_ranked[:limit],
        "novelty_leaderboard": novelty_ranked[:limit],
        "expected_promotable_leaderboard": promotable_ranked[:limit],
        "execution_adjusted_leaderboard": execution_ranked[:limit],
        "statistical_validation_leaderboard": statistical_ranked[:limit],
        "deployment_risk_leaderboard": deployment_ranked[:limit],
        "objective_version": step2_objective.OBJECTIVE_VERSION,
        "statistical_validation_version": step2_statistical_validation.VALIDATION_VERSION,
        "input_rows": len(rows),
        "void_filtered_rows": len(voided_rows),
        "void_filtered_days": sorted({day for row in voided_rows for day in row.get("voided_source_days", [])}),
        "include_voided_for_forensics": bool(include_voided_for_forensics),
        "live_filtered_rows": len(decorated),
        "config_unique_rows": len(config_ranked),
        "behavior_unique_rows": len(final_rows),
        "execution_config_unique_rows": len(execution_config_ranked),
        "execution_behavior_unique_rows": len(execution_rows),
        "data_quality_report": step2_data_quality.report(decorated, source="step2_hunt_intelligence"),
        "statistical_validation_report": step2_statistical_validation.report(
            decorated,
            source="step2_hunt_intelligence",
        ),
        "deployment_risk_report": step2_deployment_risk.report(
            decorated,
            source="step2_hunt_intelligence",
        ),
        "decorated_rows": decorated,
        "behavioral_dedupe": bool(behavioral_dedupe),
        "live_only": bool(live_only),
    }
    result["world_class_readiness_report"] = step2_world_class_audit.readiness_report(
        decorated,
        payload=result,
        source="step2_hunt_intelligence",
    )
    return result


def collect_ranked_from_paths(
    paths: list[Path],
    *,
    active_pnl: float | None = None,
    live_only: bool = True,
    behavioral_dedupe: bool = True,
    limit: int = 100,
    include_voided_for_forensics: bool = False,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for path in paths:
        if path.suffix.lower() == ".jsonl":
            payload_rows: list[dict[str, Any]] = []
            try:
                lines = path.read_text(encoding="utf-8-sig").splitlines()
            except Exception:
                lines = []
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except Exception:
                    continue
                if isinstance(payload, dict):
                    payload_rows.append(payload)
            source_rows = payload_rows
        else:
            payload = read_json(path, {}) or {}
            source_rows = list(walk_rows(payload))
        for row in source_rows:
            clean = dict(row)
            clean["hunt_source_path"] = str(path.resolve())
            rows.append(clean)
    return rank_rows(
        rows,
        active_pnl=active_pnl,
        live_only=live_only,
        behavioral_dedupe=behavioral_dedupe,
        limit=limit,
        include_voided_for_forensics=include_voided_for_forensics,
    )


def route_match(row: dict[str, Any]) -> dict[str, str]:
    routes = row.get("routes") if isinstance(row.get("routes"), list) else []
    ordered_routes = [
        route for route in routes
        if isinstance(route, dict) and str(route.get("action") or "").lower() not in {"skip", "veto"}
    ] + [
        route for route in routes
        if isinstance(route, dict) and str(route.get("action") or "").lower() in {"skip", "veto"}
    ]
    for route in ordered_routes:
        if not isinstance(route, dict):
            continue
        match = route.get("match") if isinstance(route.get("match"), dict) else {}
        ticker = str(match.get("ticker") or "")
        setup = str(match.get("setup_type") or "")
        session = str(match.get("session_phase") or "")
        if ticker or setup or session:
            return {"ticker": ticker, "setup_type": setup, "session_phase": session}
    match = ROUTE_VARIANT_RE.match(str(row.get("variant") or ""))
    if match:
        return {
            "ticker": match.group("ticker"),
            "setup_type": match.group("setup"),
            "session_phase": match.group("session"),
        }
    return {}


def route_key_from_row(row: dict[str, Any]) -> str:
    match = route_match(row)
    if not match:
        return "unrouted"
    return "|".join([match.get("ticker") or "*", match.get("setup_type") or "*", match.get("session_phase") or "*"])


def route_clusters(rows: list[dict[str, Any]], limit: int = 20) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[route_key_from_row(row)].append(row)
    clusters = []
    for key, group in grouped.items():
        if key == "unrouted":
            continue
        group = sorted(group, key=pnl, reverse=True)
        alias_count = sum(int(row.get("behavior_alias_count") or 1) for row in group)
        clusters.append(
            {
                "route_key": key,
                "count": len(group),
                "alias_count": alias_count,
                "best_variant": group[0].get("variant"),
                "best_step2_pnl": pnl(group[0]),
                "best_delta_vs_active": group[0].get("step2_delta_vs_active"),
                "avg_delta_vs_active": round(sum(live_delta(row) for row in group) / max(1, len(group)), 4),
                "sample_variants": [row.get("variant") for row in group[:5]],
            }
        )
    clusters.sort(
        key=lambda row: (
            float(row.get("best_step2_pnl") or 0.0),
            int(row.get("alias_count") or 0),
            int(row.get("count") or 0),
        ),
        reverse=True,
    )
    return clusters[:limit]


def next_hunt_plan(rows: list[dict[str, Any]], limit: int = 12) -> dict[str, Any]:
    clusters = route_clusters(rows, limit=limit)
    route_jobs = []
    for cluster in clusters[:limit]:
        parts = str(cluster.get("route_key") or "").split("|")
        ticker = parts[0] if len(parts) > 0 else "*"
        setup = parts[1] if len(parts) > 1 else "*"
        session = parts[2] if len(parts) > 2 else "*"
        route_jobs.append(
            {
                "focus": cluster.get("route_key"),
                "recommended_hunter": "router",
                "mutation_style": "route-local weights and bias",
                "sibling_routes": [
                    f"{ticker}|{setup}|midday",
                    f"{ticker}|{setup}|late",
                    f"{ticker}|{setup}|open",
                    f"CLSK|{setup}|{session}",
                    f"MARA|{setup}|{session}",
                    f"RIOT|{setup}|{session}",
                    f"{ticker}|momentum_breakout|{session}",
                    f"{ticker}|btc_relative_strength|{session}",
                    f"{ticker}|trend_pullback|{session}",
                ],
                "seed_variants": cluster.get("sample_variants") or [],
            }
        )
    families = Counter(str(row.get("variant") or "").split("_", 1)[0] or "unknown" for row in rows)
    return {
        "recommended_primary_direction": route_jobs[0]["focus"] if route_jobs else None,
        "route_jobs": route_jobs,
        "hunter_family_counts": dict(families),
        "notes": [
            "Prefer route-local router refinement before broad adaptive search when route clusters dominate.",
            "Dedupe behavioral aliases before promotion review.",
            "Use promotion_quality_leaderboard for finalist review; raw P/L remains discovery telemetry.",
        ],
    }


def finalist_row(row: dict[str, Any], rank: int) -> dict[str, Any]:
    return {
        "rank": int(rank),
        "variant": row.get("variant"),
        "step2_pnl": pnl(row),
        "step2_delta_vs_active": row.get("step2_delta_vs_active"),
        "step2_delta_pct_vs_active": row.get("step2_delta_pct_vs_active"),
        "step2_trades": trade_count(row),
        "step2_win_rate_pct": win_rate(row),
        "promotion_quality_score": row.get("promotion_quality_score"),
        "promotion_readiness_score": row.get("promotion_readiness_score"),
        "promotion_approve_probability": row.get("promotion_approve_probability"),
        "expected_promotable_pnl": row.get("expected_promotable_pnl"),
        "novelty_score": row.get("novelty_score"),
        "overfit_risk_score": row.get("overfit_risk_score"),
        "route_narrowness": row.get("route_narrowness"),
        "learning_tags": row.get("learning_tags") or candidate_learning_tags(row),
        "lineage": row.get("lineage") or lineage_info(row),
        "family_key": row.get("family_key") or candidate_family_key(row),
        "behavior_key": row.get("behavior_key") or behavior_key(row),
        "config_key": row.get("config_key") or config_key(row),
        "behavior_alias_count": int(row.get("behavior_alias_count") or 1),
        "behavior_aliases": list(row.get("behavior_aliases") or [])[:25],
        "route_key": route_key_from_row(row),
        "weights": row.get("weights") or {},
        "bias": float(row.get("bias") or 0.0),
        "routes": row.get("routes") or [],
        "by_day": row.get("by_day") or {},
        "by_ticker": row.get("by_ticker") or {},
        "by_side": row.get("by_side") or {},
        "score_cache": row.get("score_cache") or {},
        "execution_adjusted_pnl": row.get("execution_adjusted_pnl"),
        "objective_score": row.get("objective_score"),
        "replay_validity": row.get("replay_validity"),
        "data_quality_score": row.get("data_quality_score"),
        "data_quality_contract": row.get("data_quality_contract"),
        "statistical_validation_score": row.get("statistical_validation_score"),
        "statistical_validation": row.get("statistical_validation"),
        "deployment_risk_score": row.get("deployment_risk_score"),
        "deployment_risk": row.get("deployment_risk"),
        "world_class_candidate_audit": step2_world_class_audit.candidate_readiness(row),
        "source": row.get("hunt_source_path"),
    }


def finalists_payload(rows: list[dict[str, Any]], *, limit: int = 50, source: str = "") -> dict[str, Any]:
    rows = sorted(rows, key=pnl, reverse=True)[:limit]
    return {
        "schema_version": 1,
        "source": source or "step2_hunt_intelligence",
        "description": "Behavior-unique live-beating finalists for downstream robustness and promotion gates.",
        "finalist_count": len(rows),
        "finalists": [finalist_row(row, idx + 1) for idx, row in enumerate(rows)],
    }


def annotated_top_payload(rows: list[dict[str, Any]], *, limit: int = 100, source: str = "") -> dict[str, Any]:
    ranked = sorted(rows, key=pnl, reverse=True)[:limit]
    return {
        "schema_version": 1,
        "source": source or "step2_hunt_intelligence",
        "description": "Live-beating leaderboard annotated with promotion-readiness and learning tags.",
        "top_count": len(ranked),
        "top": [finalist_row(row, idx + 1) for idx, row in enumerate(ranked)],
    }


def near_miss_archive(rows: list[dict[str, Any]], *, limit: int = 100, source: str = "") -> dict[str, Any]:
    near = []
    for row in rows:
        if not beats_current_live(row):
            continue
        readiness = float(row.get("promotion_readiness_score") or promotion_readiness_score(row))
        tags = row.get("learning_tags") or candidate_learning_tags(row)
        if readiness >= 75.0:
            continue
        if readiness >= 45.0 or "thin_holdout_edge" in tags or "small_total_edge" in tags:
            near.append(row)
    near.sort(key=lambda row: (float(row.get("promotion_readiness_score") or 0.0), pnl(row)), reverse=True)
    return {
        "schema_version": 1,
        "source": source or "step2_hunt_intelligence",
        "description": "Live-beating candidates worth learning from even when not promotion-ready.",
        "archive_count": min(len(near), limit),
        "selection_policy": [
            "must beat current Live",
            "exclude promotion-ready candidates",
            "keep candidates with moderate readiness, thin holdout edge, or small total edge for repair/recombination",
        ],
        "near_misses": [finalist_row(row, idx + 1) for idx, row in enumerate(near[:limit])],
    }


def failure_memory_feedback(feedback_rows: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    feedback_rows = feedback_rows or []
    route_rejects: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter()
    route_reasons: dict[str, Counter[str]] = defaultdict(Counter)
    for row in feedback_rows:
        status = str(row.get("status") or row.get("decision") or "").lower()
        if status not in {"failed", "reject", "rejected", "blocked"}:
            continue
        route = str(row.get("route_key") or "unknown")
        route_rejects[route] += 1
        reasons = row.get("reject_reasons") or row.get("reasons") or []
        if isinstance(reasons, str):
            reasons = [reasons]
        for reason in reasons or ["reject_unspecified"]:
            reason = str(reason)
            reason_counts[reason] += 1
            route_reasons[route][reason] += 1
    directives = []
    for route, count in route_rejects.most_common(12):
        top_reason = route_reasons[route].most_common(1)[0][0] if route_reasons[route] else "reject_unspecified"
        action = "repair_before_exploit"
        if top_reason in {"robustness_score_below_70", "thin_holdout_edge", "holdout_does_not_beat_active"}:
            action = "send_to_holdout_repair_lane"
        elif top_reason in {"weak_day_consistency", "weak_positive_day_rate"}:
            action = "send_to_day_consistency_lane"
        directives.append({
            "route_key": route,
            "reject_count": count,
            "top_reject_reason": top_reason,
            "recommended_action": action,
        })
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "feedback_count": len(feedback_rows),
        "reject_reason_counts": dict(reason_counts),
        "route_reject_counts": dict(route_rejects),
        "directives": directives,
    }


def adaptive_mutation_lanes(
    rows: list[dict[str, Any]],
    feedback_rows: list[dict[str, Any]] | None = None,
    *,
    limit: int = 12,
) -> dict[str, Any]:
    feedback = failure_memory_feedback(feedback_rows)
    near = near_miss_archive(rows, limit=limit).get("near_misses") or []
    quality = sorted(rows, key=lambda row: (float(row.get("promotion_readiness_score") or 0.0), pnl(row)), reverse=True)
    raw = sorted(rows, key=pnl, reverse=True)
    by_tag = lambda tag: [row for row in rows if tag in (row.get("learning_tags") or candidate_learning_tags(row))]
    lanes = [
        {
            "lane": "exploitation",
            "purpose": "Mutate current best promotion-ready or high-readiness winners.",
            "budget_pct": 25,
            "seed_variants": [row.get("variant") for row in quality[:limit]],
            "focus_routes": [route_key_from_row(row) for row in quality[:limit] if route_key_from_row(row) != "unrouted"],
        },
        {
            "lane": "holdout_repair",
            "purpose": "Take live beaters with thin holdout edge and push recent-window robustness.",
            "budget_pct": 20,
            "seed_variants": [row.get("variant") for row in by_tag("thin_holdout_edge")[:limit]],
            "focus_routes": [route_key_from_row(row) for row in by_tag("thin_holdout_edge")[:limit] if route_key_from_row(row) != "unrouted"],
        },
        {
            "lane": "day_consistency_repair",
            "purpose": "Favor candidates whose edge survives more days instead of one or two spikes.",
            "budget_pct": 15,
            "seed_variants": [row.get("variant") for row in by_tag("weak_day_consistency")[:limit]],
            "focus_routes": [route_key_from_row(row) for row in by_tag("weak_day_consistency")[:limit] if route_key_from_row(row) != "unrouted"],
        },
        {
            "lane": "robustness_first",
            "purpose": "Rank by promotion readiness first and raw P/L second.",
            "budget_pct": 20,
            "seed_variants": [row.get("variant") for row in quality[:limit]],
            "focus_routes": [route_key_from_row(row) for row in quality[:limit] if route_key_from_row(row) != "unrouted"],
        },
        {
            "lane": "near_miss_recombination",
            "purpose": "Keep useful rejected DNA while avoiding direct promotion of weak shapes.",
            "budget_pct": 10,
            "seed_variants": [row.get("variant") for row in near[:limit]],
            "focus_routes": [row.get("route_key") for row in near[:limit] if row.get("route_key") != "unrouted"],
        },
        {
            "lane": "wild_shuffle",
            "purpose": "Reserve budget for indicator sign flips, sparse scoring, and broad route exploration.",
            "budget_pct": 10,
            "seed_variants": [row.get("variant") for row in raw[: max(3, limit // 2)]],
            "focus_routes": ["broad_exploration"],
        },
    ]
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Live hunt lane plan that learns while hunting rather than only after the run.",
        "lanes": lanes,
        "failure_memory_feedback": feedback,
    }


def worker_role_plan(rows: list[dict[str, Any]], *, max_workers: int = 4) -> dict[str, Any]:
    lanes = adaptive_mutation_lanes(rows).get("lanes") or []
    roles = [
        ("worker_1", "exploitation", "High-readiness candidates and top raw P/L live beaters."),
        ("worker_2", "holdout_repair", "Recent holdout edge repair and robustness pressure."),
        ("worker_3", "wild_shuffle", "Crazy scoring/indicator shuffles and broad exploration."),
        ("worker_4", "robustness_first", "Promotion-readiness ranking, near-miss recombination, and validation queue seeds."),
    ][: max(1, min(4, int(max_workers)))]
    lane_by_name = {lane.get("lane"): lane for lane in lanes}
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "max_workers": max(1, min(4, int(max_workers))),
        "roles": [
            {
                "worker": worker,
                "primary_lane": lane,
                "responsibility": responsibility,
                "lane_plan": lane_by_name.get(lane, {}),
            }
            for worker, lane, responsibility in roles
        ],
        "coordination_rule": "Workers specialize by lane and all outputs still flow through the shared live-only, behavior-deduped top-100.",
    }


def diversity_constrained_finalists(
    rows: list[dict[str, Any]],
    *,
    limit: int = 20,
    max_per_route: int = 3,
    max_per_ticker: int = 8,
) -> dict[str, Any]:
    selected = []
    route_counts: Counter[str] = Counter()
    ticker_counts: Counter[str] = Counter()
    seen_behaviors = set()
    ranked = sorted(
        rows,
        key=lambda row: (
            float(row.get("promotion_quality_score") or 0.0) - float(row.get("overfit_risk_score") or 0.0) * 10.0,
            float(row.get("novelty_score") or 0.0),
            pnl(row),
        ),
        reverse=True,
    )
    for row in ranked:
        route = route_key_from_row(row)
        ticker = route_match(row).get("ticker") or "unknown"
        bkey = str(row.get("behavior_key") or behavior_key(row))
        if bkey in seen_behaviors:
            continue
        if route_counts[route] >= max_per_route:
            continue
        if ticker_counts[ticker] >= max_per_ticker:
            continue
        selected.append(row)
        seen_behaviors.add(bkey)
        route_counts[route] += 1
        ticker_counts[ticker] += 1
        if len(selected) >= limit:
            break
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Finalists selected with route/ticker/behavior diversity constraints.",
        "limits": {
            "limit": limit,
            "max_per_route": max_per_route,
            "max_per_ticker": max_per_ticker,
        },
        "route_counts": dict(route_counts),
        "ticker_counts": dict(ticker_counts),
        "finalist_count": len(selected),
        "finalists": [finalist_row(row, idx + 1) for idx, row in enumerate(selected)],
    }


def oos_replay_queue(rows: list[dict[str, Any]], *, limit: int = 20, source: str = "") -> dict[str, Any]:
    finalists = [finalist_row(row, idx + 1) for idx, row in enumerate(sorted(rows, key=lambda r: float(r.get("promotion_quality_score") or 0.0), reverse=True)[:limit])]
    return {
        "schema_version": 1,
        "source": source or "step2_hunt_intelligence",
        "description": "Queue candidates for stricter out-of-sample or alternate-window replay before promotion.",
        "queue_count": len(finalists),
        "recommended_checks": [
            "holdout_day_replay",
            "alternate_date_window_replay",
            "route_trace_audit",
            "candidate_robustness_report",
            "candidate_reproducibility_gate",
            "promotion_safety_gate",
        ],
        "queue": [
            {
                "status": "pending",
                "priority": idx + 1,
                "variant": row.get("variant"),
                "route_key": row.get("route_key"),
                "behavior_key": row.get("behavior_key"),
                "config_key": row.get("config_key"),
                "step2_pnl": row.get("step2_pnl"),
                "step2_delta_vs_active": row.get("step2_delta_vs_active"),
                "promotion_quality_score": row.get("promotion_quality_score"),
                "weights": row.get("weights") or {},
                "bias": row.get("bias"),
                "routes": row.get("routes") or [],
                "source": row.get("source"),
            }
            for idx, row in enumerate(finalists)
        ],
    }


def counterfactual_route_attribution_plan(rows: list[dict[str, Any]], *, limit: int = 20) -> dict[str, Any]:
    tasks = []
    for idx, row in enumerate(sorted(rows, key=pnl, reverse=True)[:limit], 1):
        routes = row.get("routes") if isinstance(row.get("routes"), list) else []
        if not routes:
            continue
        match = route_match(row)
        route_key = route_key_from_row(row)
        tasks.append({
            "priority": idx,
            "variant": row.get("variant"),
            "route_key": route_key,
            "baseline_step2_pnl": pnl(row),
            "baseline_delta_vs_active": live_delta(row),
            "counterfactuals": [
                {"name": "route_disabled", "action": "remove_matching_route", "match": match},
                {"name": "route_widen_ticker_only", "action": "keep_only_match_keys", "match_keys": ["ticker"]},
                {"name": "route_widen_setup_only", "action": "keep_only_match_keys", "match_keys": ["setup_type"]},
                {"name": "route_narrow_exact", "action": "exact_current_match", "match": match},
                {"name": "route_force_long", "action": "replace_route_action", "route_action": "force_long"},
                {"name": "route_force_short", "action": "replace_route_action", "route_action": "force_short"},
                {"name": "route_skip", "action": "replace_route_action", "route_action": "skip"},
            ],
            "pass_condition": "candidate edge should materially weaken when route_disabled and degrade gracefully under small route perturbations",
            "weights": row.get("weights") or {},
            "bias": row.get("bias"),
            "routes": routes,
            "source": row.get("hunt_source_path"),
        })
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Replay plan to test whether a routed candidate wins because of its route rather than fallback noise.",
        "task_count": len(tasks),
        "tasks": tasks,
    }


def temporal_generalization_map(rows: list[dict[str, Any]]) -> dict[str, Any]:
    route_days: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        route = route_key_from_row(row)
        by_day = row.get("by_day") if isinstance(row.get("by_day"), dict) else {}
        for day, payload in by_day.items():
            if not isinstance(payload, dict):
                continue
            route_days[route][str(day)].append(float(payload.get("pnl") or 0.0))
    routes = []
    for route, day_map in route_days.items():
        ordered = sorted((day, sum(vals) / max(1, len(vals))) for day, vals in day_map.items())
        if not ordered:
            continue
        midpoint = len(ordered) // 2
        first = ordered[:midpoint] or ordered
        second = ordered[midpoint:] or ordered
        pnls = [p for _, p in ordered]
        mean = sum(pnls) / max(1, len(pnls))
        variance = sum((p - mean) ** 2 for p in pnls) / max(1, len(pnls))
        routes.append({
            "route_key": route,
            "days": len(ordered),
            "positive_day_rate": round(sum(1 for _, p in ordered if p > 0) / max(1, len(ordered)), 4),
            "first_half_pnl": round(sum(p for _, p in first), 2),
            "second_half_pnl": round(sum(p for _, p in second), 2),
            "mean_day_pnl": round(mean, 2),
            "day_pnl_stdev": round(math.sqrt(variance), 2),
            "best_day": max(ordered, key=lambda item: item[1])[0],
            "worst_day": min(ordered, key=lambda item: item[1])[0],
        })
    routes.sort(key=lambda row: (row["positive_day_rate"], row["second_half_pnl"], row["first_half_pnl"]), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Temporal generalization proxy by route using available by_day P/L.",
        "routes": routes,
    }


def feature_interaction_mining(rows: list[dict[str, Any]], *, top_features: int = 18, pair_limit: int = 30) -> dict[str, Any]:
    feature_values: dict[str, list[tuple[float, float]]] = defaultdict(list)
    for row in rows:
        improvement = max(0.0, live_delta(row))
        for feature, value in (row.get("weights") or {}).items():
            feature_values[str(feature)].append((float(value or 0.0), improvement))
    feature_scores = []
    for feature, values in feature_values.items():
        if not values:
            continue
        avg_abs = sum(abs(v) for v, _ in values) / len(values)
        weighted = sum(v * max(1.0, imp) for v, imp in values) / len(values)
        feature_scores.append((feature, abs(weighted) + avg_abs * 10.0))
    feature_scores.sort(key=lambda item: item[1], reverse=True)
    selected = [feature for feature, _ in feature_scores[:top_features]]
    pair_scores = []
    for i, a in enumerate(selected):
        for b in selected[i + 1:]:
            co = 0
            reward = 0.0
            for row in rows:
                weights = row.get("weights") or {}
                if a in weights and b in weights:
                    co += 1
                    reward += max(0.0, live_delta(row))
            if co:
                pair_scores.append({
                    "features": [a, b],
                    "cooccurrences": co,
                    "avg_live_delta": round(reward / co, 4),
                    "score": round(reward * math.log1p(co), 4),
                })
    pair_scores.sort(key=lambda row: row["score"], reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Feature pair interactions observed among live-beating candidates.",
        "top_features": selected,
        "pairs": pair_scores[:pair_limit],
    }


def adversarial_perturbation_plan(rows: list[dict[str, Any]], *, limit: int = 20) -> dict[str, Any]:
    tasks = []
    for idx, row in enumerate(sorted(rows, key=pnl, reverse=True)[:limit], 1):
        saturated = [
            feature for feature, value in (row.get("weights") or {}).items()
            if abs(float(value or 0.0)) >= 3.5
        ][:10]
        tasks.append({
            "priority": idx,
            "variant": row.get("variant"),
            "route_key": route_key_from_row(row),
            "baseline_step2_pnl": pnl(row),
            "overfit_risk_score": row.get("overfit_risk_score"),
            "perturbations": [
                {"name": "bias_plus_5bp", "bias_delta": 0.05},
                {"name": "bias_minus_5bp", "bias_delta": -0.05},
                {"name": "all_weights_shrink_5pct", "weight_scale": 0.95},
                {"name": "all_weights_expand_5pct", "weight_scale": 1.05},
                {"name": "saturated_weights_clip_90pct", "features": saturated, "weight_scale": 0.90},
                {"name": "execution_slippage_pressure", "sim_config_overlay": {"adversarial_slippage_bps": 1.0}},
            ],
            "pass_condition": "candidate remains live-beating or degrades smoothly without rank collapse",
            "weights": row.get("weights") or {},
            "bias": row.get("bias"),
            "routes": row.get("routes") or [],
            "source": row.get("hunt_source_path"),
        })
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Replay plan for adversarial sensitivity tests around top candidates.",
        "task_count": len(tasks),
        "tasks": tasks,
    }


def promotion_feedback_summary(feedback_rows: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    feedback_rows = feedback_rows or []
    counts: Counter[str] = Counter()
    penalties: Counter[str] = Counter()
    for row in feedback_rows:
        status = str(row.get("status") or row.get("decision") or "unknown")
        route = str(row.get("route_key") or "unknown")
        counts[status] += 1
        if status.lower() in {"failed", "reject", "rejected", "blocked"}:
            penalties[route] += 1
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "feedback_count": len(feedback_rows),
        "status_counts": dict(counts),
        "route_penalties": dict(penalties),
        "notes": [
            "Feed promotion/robustness failures here to penalize similar routes in future allocation.",
            "Rows should include variant, behavior_key, route_key, status, and reject_reasons when available.",
        ],
    }


def _validation_rows_for_route(validation_rows: list[dict[str, Any]], route_key: str) -> list[dict[str, Any]]:
    return [row for row in validation_rows or [] if str(row.get("route_key") or "") == route_key]


def validation_adjustment(row: dict[str, Any], validation_rows: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    route = route_key_from_row(row)
    rows = _validation_rows_for_route(validation_rows or [], route)
    if not rows:
        return {
            "route_key": route,
            "validation_count": 0,
            "quality_bonus": 0.0,
            "risk_delta": 0.0,
            "uncertainty_score": 85.0,
            "notes": ["no_validation_history"],
        }
    passes = 0
    causal = 0
    stable = 0
    failures = 0
    notes = []
    for item in rows:
        kind = str(item.get("kind") or "")
        if item.get("passed") is True or item.get("causal_route_signal") is True or item.get("stability_pass") is True:
            passes += 1
        else:
            failures += 1
        if item.get("causal_route_signal") is True or (kind == "counterfactual" and float(item.get("route_disabled_delta") or 0.0) < 0.0):
            causal += 1
        if item.get("stability_pass") is True or (kind == "adversarial" and float(item.get("worst_delta_vs_baseline") or 0.0) > -500.0):
            stable += 1
    count = len(rows)
    pass_rate = passes / max(1, count)
    causal_rate = causal / max(1, count)
    stable_rate = stable / max(1, count)
    uncertainty = round(100.0 / (1.0 + count) + max(0.0, 0.5 - abs(pass_rate - 0.5)) * 30.0, 4)
    quality_bonus = round(pass_rate * 600.0 + causal_rate * 350.0 + stable_rate * 300.0, 4)
    risk_delta = round(failures / max(1, count) * 35.0 - causal_rate * 12.0 - stable_rate * 10.0, 4)
    if causal_rate > 0:
        notes.append("causal_route_signal_observed")
    if stable_rate > 0:
        notes.append("adversarial_stability_observed")
    if failures:
        notes.append("validation_failures_observed")
    return {
        "route_key": route,
        "validation_count": count,
        "validation_pass_rate": round(pass_rate, 4),
        "causal_signal_rate": round(causal_rate, 4),
        "stability_pass_rate": round(stable_rate, 4),
        "quality_bonus": quality_bonus,
        "risk_delta": risk_delta,
        "uncertainty_score": uncertainty,
        "notes": notes,
    }


def apply_validation_adjustments(
    rows: list[dict[str, Any]],
    validation_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        item = dict(row)
        adjustment = validation_adjustment(item, validation_rows)
        item["validation_adjustment"] = adjustment
        item["validation_aware_quality_score"] = round(
            float(item.get("promotion_quality_score") or quality_score(item)) + float(adjustment.get("quality_bonus") or 0.0),
            4,
        )
        item["validation_aware_overfit_risk_score"] = round(
            max(0.0, min(100.0, float(item.get("overfit_risk_score") or overfit_risk_score(item)) + float(adjustment.get("risk_delta") or 0.0))),
            4,
        )
        item["uncertainty_score"] = adjustment.get("uncertainty_score")
        out.append(item)
    return out


def validation_aware_bandit(
    rows: list[dict[str, Any]],
    validation_rows: list[dict[str, Any]] | None = None,
    *,
    exploration_pct: float = 0.15,
    limit: int = 12,
) -> dict[str, Any]:
    adjusted = apply_validation_adjustments(rows, validation_rows)
    clusters = route_clusters(adjusted, limit=limit)
    if not clusters:
        return {
            "schema_version": 1,
            "source": "step2_hunt_intelligence",
            "exploration_pct": exploration_pct,
            "allocation": [],
        }
    route_adjustments = {
        route_key_from_row(row): row.get("validation_adjustment") or {}
        for row in adjusted
    }
    scored = []
    for cluster in clusters:
        route = str(cluster.get("route_key") or "")
        adj = route_adjustments.get(route, {})
        reward = max(0.0, float(cluster.get("best_delta_vs_active") or 0.0))
        reward += int(cluster.get("alias_count") or cluster.get("count") or 0) * 60.0
        reward += float(adj.get("quality_bonus") or 0.0)
        reward -= max(0.0, float(adj.get("risk_delta") or 0.0)) * 25.0
        reward += float(adj.get("uncertainty_score") or 0.0) * 2.0
        scored.append((cluster, max(1.0, reward), adj))
    total = sum(score for _, score, _ in scored) or 1.0
    exploit_pct = max(0.0, 1.0 - float(exploration_pct))
    allocation = []
    for cluster, score, adj in scored:
        allocation.append({
            "route_key": cluster.get("route_key"),
            "recommended_budget_pct": round(exploit_pct * score / total * 100.0, 2),
            "best_variant": cluster.get("best_variant"),
            "reward_score": round(score, 4),
            "validation_adjustment": adj,
        })
    allocation.append({
        "route_key": "exploration",
        "recommended_budget_pct": round(float(exploration_pct) * 100.0, 2),
        "best_variant": None,
        "reward_score": 0.0,
    })
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "exploration_pct": exploration_pct,
        "allocation": allocation,
        "arms": clusters,
    }


def auto_validation_scheduler(rows: list[dict[str, Any]], *, limit: int = 30) -> dict[str, Any]:
    categories = []
    by_name: dict[str, dict[str, Any]] = {}

    def add(row: dict[str, Any], reason: str, priority_bonus: float = 0.0) -> None:
        key = str(row.get("behavior_key") or behavior_key(row))
        priority = float(row.get("validation_aware_quality_score") or row.get("promotion_quality_score") or 0.0)
        priority += float(row.get("novelty_score") or 0.0) * 8.0
        priority += float(row.get("uncertainty_score") or 0.0) * 10.0
        priority += priority_bonus
        existing = by_name.get(key)
        if existing is None or priority > float(existing.get("priority_score") or 0.0):
            by_name[key] = {
                "variant": row.get("variant"),
                "behavior_key": key,
                "config_key": row.get("config_key") or config_key(row),
                "route_key": route_key_from_row(row),
                "priority_score": round(priority, 4),
                "reason": reason,
                "step2_pnl": pnl(row),
                "delta_vs_active": live_delta(row),
                "overfit_risk_score": row.get("overfit_risk_score"),
                "uncertainty_score": row.get("uncertainty_score"),
                "weights": row.get("weights") or {},
                "bias": row.get("bias"),
                "routes": row.get("routes") or [],
            }

    ranked_raw = sorted(rows, key=pnl, reverse=True)
    ranked_quality = sorted(rows, key=lambda r: float(r.get("validation_aware_quality_score") or r.get("promotion_quality_score") or 0.0), reverse=True)
    ranked_novelty = sorted(rows, key=lambda r: float(r.get("novelty_score") or 0.0), reverse=True)
    ranked_uncertainty = sorted(rows, key=lambda r: float(r.get("uncertainty_score") or 0.0), reverse=True)
    ranked_suspicious = sorted(rows, key=lambda r: (pnl(r), float(r.get("overfit_risk_score") or 0.0)), reverse=True)
    for row in ranked_raw[:5]:
        add(row, "top_raw_pnl", 500.0)
    for row in ranked_quality[:5]:
        add(row, "top_validation_aware_quality", 450.0)
    for row in ranked_novelty[:5]:
        add(row, "high_novelty", 300.0)
    for row in ranked_uncertainty[:5]:
        add(row, "high_uncertainty", 250.0)
    for row in ranked_suspicious[:8]:
        if float(row.get("overfit_risk_score") or 0.0) >= 40.0:
            add(row, "high_pnl_high_overfit", 350.0)
    queue = sorted(by_name.values(), key=lambda item: float(item.get("priority_score") or 0.0), reverse=True)[:limit]
    for idx, item in enumerate(queue, 1):
        item["priority"] = idx
        item["recommended_validations"] = [
            "counterfactual_route_attribution",
            "adversarial_perturbation",
            "oos_replay",
        ]
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "queue_count": len(queue),
        "queue": queue,
    }


def candidate_family_clustering(rows: list[dict[str, Any]], *, limit: int = 30) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        family = str(row.get("family_key") or candidate_family_key(row))
        grouped[family].append(row)
    families = []
    for family, group in grouped.items():
        group = sorted(group, key=pnl, reverse=True)
        lineage = lineage_info(group[0])
        families.append({
            "family_key": family,
            "route_key": route_key_from_row(group[0]),
            "parent_variant": lineage.get("parent_variant"),
            "mutation_lane": lineage.get("mutation_lane"),
            "worker_role": lineage.get("worker_role"),
            "ancestor_path": lineage.get("ancestor_path") or [],
            "count": len(group),
            "best_variant": group[0].get("variant"),
            "best_pnl": pnl(group[0]),
            "best_promotion_readiness_score": group[0].get("promotion_readiness_score"),
            "avg_delta_vs_active": round(sum(live_delta(row) for row in group) / max(1, len(group)), 4),
            "avg_overfit_risk": round(sum(float(row.get("overfit_risk_score") or 0.0) for row in group) / max(1, len(group)), 4),
            "sample_variants": [row.get("variant") for row in group[:5]],
        })
    families.sort(key=lambda item: (item["best_pnl"], item["count"]), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "families": families[:limit],
    }


def candidate_lineage_graph(
    rows: list[dict[str, Any]],
    feedback_rows: list[dict[str, Any]] | None = None,
    *,
    limit: int = 200,
) -> dict[str, Any]:
    nodes: dict[str, dict[str, Any]] = {}
    edges: list[dict[str, Any]] = []
    feedback_by_variant = {
        str(row.get("variant") or ""): row
        for row in feedback_rows or []
        if isinstance(row, dict) and row.get("variant")
    }

    def node(node_id: str, node_type: str, **attrs: Any) -> None:
        existing = nodes.get(node_id, {})
        existing.update({"id": node_id, "type": node_type, **attrs})
        nodes[node_id] = existing

    for row in sorted(rows, key=pnl, reverse=True)[:limit]:
        variant = str(row.get("variant") or row.get("name") or "")
        if not variant:
            continue
        lineage = lineage_info(row)
        family = str(row.get("family_key") or lineage.get("family_key") or candidate_family_key(row))
        route = route_key_from_row(row)
        lane = lineage.get("mutation_lane") or "unknown"
        worker = lineage.get("worker_role") or "unknown"
        candidate_id = f"candidate:{variant}"
        family_id = f"family:{family}"
        route_id = f"route:{route}"
        lane_id = f"lane:{lane}"
        worker_id = f"worker:{worker}"
        node(candidate_id, "candidate", label=variant, pnl=pnl(row), delta_vs_active=live_delta(row),
             promotion_readiness_score=row.get("promotion_readiness_score"), family_key=family)
        node(family_id, "family", label=family)
        node(route_id, "route", label=route)
        node(lane_id, "lane", label=lane)
        node(worker_id, "worker", label=worker)
        edges.extend([
            {"from": family_id, "to": candidate_id, "type": "contains"},
            {"from": route_id, "to": candidate_id, "type": "route_seed"},
            {"from": lane_id, "to": candidate_id, "type": "mutation_lane"},
            {"from": worker_id, "to": candidate_id, "type": "worker_role"},
        ])
        parent = lineage.get("parent_variant")
        if parent:
            parent_id = f"candidate:{parent}"
            node(parent_id, "parent_candidate", label=parent)
            edges.append({"from": parent_id, "to": candidate_id, "type": "parent_of"})
        for ancestor in lineage.get("ancestor_path") or []:
            ancestor_id = f"ancestor:{ancestor}"
            node(ancestor_id, "ancestor", label=str(ancestor))
            edges.append({"from": ancestor_id, "to": candidate_id, "type": "ancestor_of"})
        feedback = feedback_by_variant.get(variant)
        if feedback:
            outcome = str(feedback.get("decision") or feedback.get("status") or "review")
            outcome_id = f"review:{variant}:{outcome}"
            node(outcome_id, "review_outcome", label=outcome, reject_reasons=feedback.get("reject_reasons") or [])
            edges.append({"from": candidate_id, "to": outcome_id, "type": "reviewed_as"})
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Candidate ancestry graph linking parents, mutation lanes, routes, families, workers, and review outcomes.",
        "node_count": len(nodes),
        "edge_count": len(edges),
        "nodes": list(nodes.values()),
        "edges": edges,
    }


def family_rejection_memory(feedback_rows: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    feedback_rows = feedback_rows or []
    families: dict[str, dict[str, Any]] = defaultdict(lambda: {"reviews": 0, "rejects": 0, "reasons": Counter(), "variants": []})
    for row in feedback_rows:
        if not isinstance(row, dict):
            continue
        family = str(row.get("family_key") or candidate_family_key(row))
        item = families[family]
        item["reviews"] += 1
        status = str(row.get("status") or row.get("decision") or "").lower()
        if status in {"reject", "rejected", "blocked", "failed"}:
            item["rejects"] += 1
            for reason in row.get("reject_reasons") or ["reject_unspecified"]:
                item["reasons"][str(reason)] += 1
        if len(item["variants"]) < 10:
            item["variants"].append(row.get("variant"))
    rows = []
    for family, item in families.items():
        reviews = int(item["reviews"] or 0)
        rejects = int(item["rejects"] or 0)
        rows.append({
            "family_key": family,
            "reviews": reviews,
            "rejects": rejects,
            "reject_rate": round(rejects / max(1, reviews), 4),
            "top_reasons": [{"reason": reason, "count": count} for reason, count in item["reasons"].most_common(5)],
            "sample_variants": item["variants"],
            "recommended_action": "downweight_family" if rejects / max(1, reviews) >= 0.5 else "watch_family",
        })
    rows.sort(key=lambda item: (float(item["reject_rate"]), int(item["rejects"])), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "families": rows,
    }


def _experiment_parent_row(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    ranked = sorted(
        rows,
        key=lambda row: (
            float(row.get("promotion_readiness_score") or 0.0),
            float(row.get("novelty_score") or 0.0),
            pnl(row),
        ),
        reverse=True,
    )
    return ranked[0]


def _experiment_candidate_ref(row: dict[str, Any]) -> dict[str, Any]:
    lineage = lineage_info(row)
    return {
        "variant": row.get("variant"),
        "behavior_key": row.get("behavior_key") or behavior_key(row),
        "config_key": row.get("config_key") or config_key(row),
        "family_key": row.get("family_key") or candidate_family_key(row),
        "route_key": route_key_from_row(row),
        "step2_pnl": pnl(row),
        "delta_vs_active": live_delta(row),
        "promotion_readiness_score": row.get("promotion_readiness_score"),
        "learning_tags": row.get("learning_tags") or candidate_learning_tags(row),
        "lineage": lineage,
        "weights": row.get("weights") or {},
        "bias": row.get("bias"),
        "routes": row.get("routes") or [],
    }


def active_experiment_planner(
    rows: list[dict[str, Any]],
    feedback_rows: list[dict[str, Any]] | None = None,
    *,
    limit: int = 16,
) -> dict[str, Any]:
    """Create small controlled experiments that ask explicit hunt questions."""
    rows = list(rows or [])
    feedback_rows = feedback_rows or []
    family_memory = family_rejection_memory(feedback_rows)
    near = near_miss_archive(rows, limit=limit).get("near_misses") or []
    experiments = []

    def add(kind: str, hypothesis: str, parent: dict[str, Any], treatments: list[dict[str, Any]],
            success_metric: str, stop_rule: str, priority_bonus: float = 0.0) -> None:
        parent_ref = _experiment_candidate_ref(parent)
        exp_id = tournament_safety.stable_json_hash(
            {
                "kind": kind,
                "hypothesis": hypothesis,
                "parent": parent_ref.get("behavior_key"),
                "family": parent_ref.get("family_key"),
                "treatments": treatments,
            },
            length=20,
        )
        priority = (
            max(0.0, float(parent_ref.get("delta_vs_active") or 0.0))
            + float(parent_ref.get("promotion_readiness_score") or 0.0) * 40.0
            + priority_bonus
        )
        experiments.append({
            "experiment_id": exp_id,
            "kind": kind,
            "hypothesis": hypothesis,
            "parent": parent_ref,
            "family_key": parent_ref.get("family_key"),
            "route_key": parent_ref.get("route_key"),
            "treatments": treatments,
            "success_metric": success_metric,
            "stop_rule": stop_rule,
            "priority_score": round(priority, 4),
            "status": "planned",
        })

    parent = _experiment_parent_row(rows)
    if parent:
        add(
            "ab_mutation_radius",
            "Compare narrow vs wide mutations from the same promising parent to learn which radius improves promotion-readiness.",
            parent,
            [
                {"name": "narrow_mutation", "mutation_lane": "exploitation", "mutation_scale_multiplier": 0.55, "budget_pct": 25},
                {"name": "wide_mutation", "mutation_lane": "wild_shuffle", "mutation_scale_multiplier": 1.35, "budget_pct": 25},
                {"name": "holdout_repair", "mutation_lane": "holdout_repair", "mutation_scale_multiplier": 0.75, "budget_pct": 25},
                {"name": "day_consistency_repair", "mutation_lane": "day_consistency_repair", "mutation_scale_multiplier": 0.65, "budget_pct": 25},
            ],
            "best promotion_readiness_score, then holdout component, then raw P/L delta",
            "Stop after 3 cycles without a treatment improving the parent by 3 readiness points.",
            priority_bonus=500.0,
        )

    for item in near[:4]:
        parent_row = item if isinstance(item, dict) else {}
        if parent_row:
            add(
                "counterfactual_family_repair",
                "Test whether a live-beating near-miss family can be repaired instead of abandoned.",
                parent_row,
                [
                    {"name": "original_family_control", "mutation_lane": "exploitation", "mutation_scale_multiplier": 0.70},
                    {"name": "holdout_repair_version", "mutation_lane": "holdout_repair", "mutation_scale_multiplier": 0.60},
                    {"name": "route_widened_version", "mutation_lane": "edge_expansion", "route_action": "widen", "mutation_scale_multiplier": 1.15},
                    {"name": "route_narrowed_version", "mutation_lane": "route_safety_repair", "route_action": "narrow", "mutation_scale_multiplier": 0.55},
                    {"name": "feature_shrunk_version", "mutation_lane": "robustness_first", "weight_scale": 0.90},
                ],
                "candidate remains live-beating while improving robustness adjusted score or reducing reject reasons",
                "Abandon family if all repair treatments fail twice or review rejects descendants for the same reason.",
                priority_bonus=350.0,
            )

    for family in (family_memory.get("families") or [])[:4]:
        if family.get("recommended_action") != "downweight_family":
            continue
        matching = [row for row in rows if str(row.get("family_key") or candidate_family_key(row)) == str(family.get("family_key"))]
        family_parent = _experiment_parent_row(matching) or parent
        if not family_parent:
            continue
        top_reason = ((family.get("top_reasons") or [{}])[0] or {}).get("reason") or "reject_unspecified"
        add(
            "rejected_family_repair_or_abandon",
            f"Determine whether rejected family {family.get('family_key')} is fixable after {top_reason}.",
            family_parent,
            [
                {"name": "reject_reason_repair", "mutation_lane": "holdout_repair" if top_reason == "robustness_score_below_70" else "robustness_first"},
                {"name": "route_widen_probe", "mutation_lane": "edge_expansion", "route_action": "widen"},
                {"name": "route_narrow_probe", "mutation_lane": "route_safety_repair", "route_action": "narrow"},
            ],
            "review reject reason changes or disappears while candidate still beats Live",
            "Downweight whole family after 2 failed repair experiments.",
            priority_bonus=700.0,
        )

    experiments.sort(key=lambda row: float(row.get("priority_score") or 0.0), reverse=True)
    for idx, experiment in enumerate(experiments[:limit], 1):
        experiment["priority"] = idx
        experiment["worker_assignment"] = f"worker_{((idx - 1) % 4) + 1}"
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Active experiment plan: controlled A/B and repair tests that answer specific hunt questions.",
        "experiment_count": min(len(experiments), limit),
        "experiments": experiments[:limit],
        "selection_policy": [
            "Start with the best promotion-readiness parent.",
            "Probe near-miss families with controlled repair treatments.",
            "Use rejection memory to decide repair-or-abandon experiments.",
        ],
    }


def experiment_outcome_ledger(
    experiments: dict[str, Any] | list[dict[str, Any]],
    rows: list[dict[str, Any]],
    feedback_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if isinstance(experiments, dict):
        experiment_rows = list(experiments.get("experiments") or [])
    else:
        experiment_rows = list(experiments or [])
    feedback_rows = feedback_rows or []
    feedback_by_family: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for feedback in feedback_rows:
        if isinstance(feedback, dict):
            feedback_by_family[str(feedback.get("family_key") or candidate_family_key(feedback))].append(feedback)
    ledger = []
    for experiment in experiment_rows:
        family = str(experiment.get("family_key") or "")
        candidates = [
            row for row in rows
            if str(row.get("family_key") or candidate_family_key(row)) == family
        ]
        candidates.sort(key=lambda row: (
            float(row.get("promotion_readiness_score") or 0.0),
            pnl(row),
        ), reverse=True)
        best = candidates[0] if candidates else None
        rejects = [
            row for row in feedback_by_family.get(family, [])
            if str(row.get("status") or row.get("decision") or "").lower() in {"reject", "rejected", "blocked", "failed"}
        ]
        conclusion = "pending_no_descendants"
        next_action = "run_experiment"
        if best:
            parent = experiment.get("parent") if isinstance(experiment.get("parent"), dict) else {}
            improved = float(best.get("promotion_readiness_score") or 0.0) > float(parent.get("promotion_readiness_score") or 0.0)
            if improved and not rejects:
                conclusion = "promising_treatment_signal"
                next_action = "scale_successful_treatment"
            elif rejects:
                conclusion = "rejected_or_unrepaired_family"
                next_action = "downweight_or_change_treatment"
            else:
                conclusion = "tested_no_clear_improvement"
                next_action = "continue_small_probe"
        ledger.append({
            "experiment_id": experiment.get("experiment_id"),
            "kind": experiment.get("kind"),
            "hypothesis": experiment.get("hypothesis"),
            "family_key": family,
            "route_key": experiment.get("route_key"),
            "candidate_count": len(candidates),
            "best_candidate": _experiment_candidate_ref(best) if best else None,
            "reject_count": len(rejects),
            "conclusion": conclusion,
            "next_action": next_action,
            "treatments": experiment.get("treatments") or [],
        })
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Experiment outcome ledger tying hypotheses and treatments to observed candidates and review feedback.",
        "outcome_count": len(ledger),
        "outcomes": ledger,
    }


def _row_treatment(row: dict[str, Any]) -> str:
    lineage = lineage_info(row)
    lane = str(
        lineage.get("mutation_lane")
        or row.get("lane")
        or row.get("mutation_lane")
        or row.get("treatment")
        or ""
    )
    route_action = str(row.get("route_action") or "")
    reason = str(lineage.get("mutation_reason") or row.get("mutation_reason") or "")
    if route_action == "widen" or lane in {"edge_expansion", "route_widen", "widen_route"}:
        return "route_widen"
    if route_action == "narrow" or lane in {"route_safety_repair", "route_narrow", "narrow_route"}:
        return "route_narrow"
    if lane in {"holdout_repair", "repair_holdout"}:
        return "holdout_repair"
    if lane in {"day_consistency_repair", "repair_day_consistency"}:
        return "day_consistency_repair"
    if lane in {"wild_shuffle", "wide_mutation", "chaos", "exploration_wide"}:
        return "wide_mutation"
    if lane in {"robustness_first", "feature_shrink", "feature_shrink_or_robustness_first"}:
        return "feature_shrink_or_robustness_first"
    if lane in {"route_gap_mutation", "route_focus", "route_local_mutation"}:
        return "route_local_mutation"
    if lane in {"exploitation", "seed_mutation", "promotion_readiness", "raw_pnl_elite", "narrow_mutation"}:
        return "narrow_mutation"
    if "holdout" in reason:
        return "holdout_repair"
    if "day_consistency" in reason or "day consistency" in reason:
        return "day_consistency_repair"
    if "widen" in reason or "expansion" in reason:
        return "route_widen"
    if "narrow" in reason or "safety" in reason:
        return "route_narrow"
    return "unlabeled"


def _robustness_component(row: dict[str, Any], name: str) -> float:
    robustness = row.get("robustness") if isinstance(row.get("robustness"), dict) else {}
    components = robustness.get("components") if isinstance(robustness.get("components"), dict) else {}
    return _safe_float(components.get(name), 0.0)


def _avg(values: list[float]) -> float:
    return sum(values) / max(1, len(values))


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def _feedback_treatment(row: dict[str, Any]) -> str:
    treatment = _row_treatment(row)
    if treatment != "unlabeled":
        return treatment
    tags = {str(tag) for tag in (row.get("learning_tags") or [])}
    reasons = {str(reason) for reason in (row.get("reject_reasons") or [])}
    lane = str(row.get("lane") or row.get("action") or "")
    if "thin_holdout_edge" in tags or "no_holdout_credit" in tags or lane == "repair_holdout":
        return "holdout_repair"
    if "weak_day_consistency" in tags or lane == "repair_day_consistency":
        return "day_consistency_repair"
    if "small_total_edge" in tags or lane == "widen_route":
        return "route_widen"
    if "route_safety_failed" in reasons or lane == "narrow_route":
        return "route_narrow"
    if "high_overfit_risk" in tags or "robustness_score_below_70" in reasons:
        return "feature_shrink_or_robustness_first"
    return "unlabeled"


def _reject_rate_for_treatment(feedback_rows: list[dict[str, Any]], treatment: str) -> float:
    matching = [row for row in feedback_rows if _feedback_treatment(row) == treatment]
    if not matching:
        return 0.0
    rejected = [
        row for row in matching
        if str(row.get("status") or row.get("decision") or "").lower() in {"reject", "rejected", "blocked", "failed"}
    ]
    return len(rejected) / max(1, len(matching))


def _experiment_treatment_map(experiment_plan: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for experiment in (experiment_plan or {}).get("experiments") or []:
        if not isinstance(experiment, dict):
            continue
        for treatment in experiment.get("treatments") or []:
            if not isinstance(treatment, dict):
                continue
            normalized = _row_treatment(treatment)
            out.setdefault(normalized, {
                "treatment": normalized,
                "experiment_ids": [],
                "hypotheses": [],
                "success_metrics": [],
            })
            out[normalized]["experiment_ids"].append(experiment.get("experiment_id"))
            out[normalized]["hypotheses"].append(experiment.get("hypothesis"))
            out[normalized]["success_metrics"].append(experiment.get("success_metric"))
    return out


def _treatment_conclusion(row: dict[str, Any]) -> str:
    treatment = str(row.get("treatment") or "")
    pnl_lift = float(row.get("avg_pnl_lift_vs_baseline") or 0.0)
    readiness_lift = float(row.get("avg_readiness_lift_vs_baseline") or 0.0)
    holdout_lift = float(row.get("avg_holdout_lift_vs_baseline") or 0.0)
    reject_rate = float(row.get("reject_rate") or 0.0)
    novelty = float(row.get("novelty_yield_rate") or 0.0)
    alias_rate = float(row.get("alias_rate") or 0.0)
    if row.get("candidate_count") == 0:
        return "No observed live-beating descendants yet."
    parts = []
    if pnl_lift > 0:
        parts.append(f"improved P/L by {round(pnl_lift, 2)} vs the current treatment baseline")
    elif pnl_lift < 0:
        parts.append(f"trailed P/L by {round(abs(pnl_lift), 2)} vs the current treatment baseline")
    if readiness_lift > 0:
        parts.append(f"raised promotion readiness by {round(readiness_lift, 2)} pts")
    elif readiness_lift < -2:
        parts.append(f"reduced promotion readiness by {round(abs(readiness_lift), 2)} pts")
    if treatment == "holdout_repair" and holdout_lift > 0:
        parts.append(f"added {round(holdout_lift, 2)} holdout-component pts")
    if reject_rate >= 0.50:
        parts.append(f"but review rejects are high at {round(reject_rate * 100.0, 1)}%")
    if alias_rate >= 0.50 and novelty <= 0.10:
        parts.append("and most output looks alias-heavy rather than novel")
    if not parts:
        parts.append("has mixed or still-thin evidence")
    return f"{treatment}: " + "; ".join(parts) + "."


def treatment_effect_analyzer(
    rows: list[dict[str, Any]],
    feedback_rows: list[dict[str, Any]] | None = None,
    experiment_plan: dict[str, Any] | None = None,
) -> dict[str, Any]:
    feedback_rows = [row for row in (feedback_rows or []) if isinstance(row, dict)]
    live_rows = [row for row in rows if beats_current_live(row) or live_delta(row) > 0.0]
    baseline_pnl = _median([pnl(row) for row in live_rows])
    baseline_readiness = _median([promotion_readiness_score(row) for row in live_rows])
    baseline_holdout = _median([_robustness_component(row, "holdout") for row in live_rows])
    by_treatment: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in live_rows:
        by_treatment[_row_treatment(row)].append(row)

    experiment_map = _experiment_treatment_map(experiment_plan)
    rows_out = []
    for treatment, group in by_treatment.items():
        pnls = [pnl(row) for row in group]
        readiness = [promotion_readiness_score(row) for row in group]
        holdouts = [_robustness_component(row, "holdout") for row in group]
        day_scores = [_robustness_component(row, "day_consistency") for row in group]
        aliases = [int(row.get("behavior_alias_count") or 1) for row in group]
        novel = [row for row in group if float(row.get("novelty_score") or 0.0) >= 25.0]
        rejects = _reject_rate_for_treatment(feedback_rows, treatment)
        best = max(group, key=pnl) if group else {}
        avg_pnl = _avg(pnls)
        avg_readiness = _avg(readiness)
        avg_holdout = _avg(holdouts)
        avg_day = _avg(day_scores)
        evidence_score = (
            max(0.0, avg_pnl - baseline_pnl) * 0.010
            + max(0.0, avg_readiness - baseline_readiness) * 1.20
            + max(0.0, avg_holdout - baseline_holdout) * 0.75
            + len(group) * 0.50
            + len(novel) * 1.50
            - rejects * 18.0
            - max(0.0, (_avg(aliases) - 2.0)) * 1.75
        )
        treatment_row = {
            "treatment": treatment,
            "candidate_count": len(group),
            "best_variant": best.get("variant"),
            "best_step2_pnl": round(max(pnls), 4) if pnls else 0.0,
            "avg_step2_pnl": round(avg_pnl, 4),
            "avg_pnl_lift_vs_baseline": round(avg_pnl - baseline_pnl, 4),
            "best_pnl_lift_vs_baseline": round((max(pnls) if pnls else 0.0) - baseline_pnl, 4),
            "avg_promotion_readiness_score": round(avg_readiness, 4),
            "avg_readiness_lift_vs_baseline": round(avg_readiness - baseline_readiness, 4),
            "avg_holdout_component": round(avg_holdout, 4),
            "avg_holdout_lift_vs_baseline": round(avg_holdout - baseline_holdout, 4),
            "avg_day_consistency_component": round(avg_day, 4),
            "reject_rate": round(rejects, 4),
            "novelty_yield": len(novel),
            "novelty_yield_rate": round(len(novel) / max(1, len(group)), 4),
            "alias_rate": round(sum(1 for value in aliases if value >= 3) / max(1, len(aliases)), 4),
            "avg_behavior_alias_count": round(_avg([float(value) for value in aliases]), 4),
            "evidence_score": round(evidence_score, 4),
            "experiment_context": experiment_map.get(treatment, {}),
            "conclusion": "",
        }
        treatment_row["conclusion"] = _treatment_conclusion(treatment_row)
        rows_out.append(treatment_row)

    rows_out.sort(key=lambda row: float(row.get("evidence_score") or 0.0), reverse=True)
    best = rows_out[0] if rows_out else {}
    worst = rows_out[-1] if rows_out else {}
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Treatment effect analyzer comparing hunt mutation and repair lanes while hunting is still running.",
        "baseline": {
            "median_step2_pnl": round(baseline_pnl, 4),
            "median_promotion_readiness_score": round(baseline_readiness, 4),
            "median_holdout_component": round(baseline_holdout, 4),
            "candidate_count": len(live_rows),
        },
        "best_treatment": best.get("treatment"),
        "worst_treatment": worst.get("treatment"),
        "best_conclusion": best.get("conclusion"),
        "worst_conclusion": worst.get("conclusion"),
        "treatments": rows_out,
    }


def treatment_prior_model(treatment_effects: dict[str, Any]) -> dict[str, Any]:
    treatments = list((treatment_effects or {}).get("treatments") or [])
    priors = []
    for row in treatments:
        score = float(row.get("evidence_score") or 0.0)
        reject_rate = float(row.get("reject_rate") or 0.0)
        alias_rate = float(row.get("alias_rate") or 0.0)
        count = int(row.get("candidate_count") or 0)
        confidence = min(0.90, 0.25 + count * 0.08)
        prior = max(0.05, min(3.0, 1.0 + score / 50.0 - reject_rate * 0.50 - alias_rate * 0.25))
        priors.append({
            "treatment": row.get("treatment"),
            "prior_weight": round(prior, 4),
            "confidence": round(confidence, 4),
            "evidence_score": row.get("evidence_score"),
            "conclusion": row.get("conclusion"),
        })
    priors.sort(key=lambda row: (float(row.get("prior_weight") or 0.0), float(row.get("confidence") or 0.0)), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "best_treatment": (priors[0] or {}).get("treatment") if priors else None,
        "worst_treatment": (priors[-1] or {}).get("treatment") if priors else None,
        "priors": priors,
    }


def treatment_worker_budget(treatment_effects: dict[str, Any], max_workers: int = 4) -> dict[str, Any]:
    treatments = list((treatment_effects or {}).get("treatments") or [])
    if not treatments:
        return {
            "schema_version": 1,
            "source": "step2_hunt_intelligence",
            "max_workers": max_workers,
            "workers": [],
        }
    positive_scores = [max(0.10, float(row.get("evidence_score") or 0.0) + 10.0) for row in treatments]
    total = sum(positive_scores) or 1.0
    workers = []
    for idx in range(max(1, int(max_workers))):
        treatment_idx = idx % len(treatments)
        row = treatments[treatment_idx]
        pct = positive_scores[treatment_idx] / total * 100.0
        workers.append({
            "worker": f"worker_{idx + 1}",
            "treatment": row.get("treatment"),
            "recommended_budget_pct": round(pct, 2),
            "evidence_score": row.get("evidence_score"),
            "directive": row.get("conclusion"),
        })
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "max_workers": max_workers,
        "best_treatment": treatments[0].get("treatment"),
        "worst_treatment": treatments[-1].get("treatment"),
        "workers": workers,
    }


def treatment_confidence_model(treatment_effects: dict[str, Any]) -> dict[str, Any]:
    """Bayesian-ish treatment confidence and stop/scale/probe decisions."""
    decisions = []
    for row in list((treatment_effects or {}).get("treatments") or []):
        n = int(row.get("candidate_count") or 0)
        readiness_lift = float(row.get("avg_readiness_lift_vs_baseline") or 0.0)
        pnl_lift = float(row.get("avg_pnl_lift_vs_baseline") or 0.0)
        holdout_lift = float(row.get("avg_holdout_lift_vs_baseline") or 0.0)
        reject_rate = float(row.get("reject_rate") or 0.0)
        novelty_rate = float(row.get("novelty_yield_rate") or 0.0)
        alias_rate = float(row.get("alias_rate") or 0.0)
        success_signal = 0.0
        success_signal += 0.35 if readiness_lift >= 3.0 else 0.0
        success_signal += 0.25 if pnl_lift > 0.0 else 0.0
        success_signal += 0.20 if holdout_lift > 0.0 else 0.0
        success_signal += 0.15 if novelty_rate >= 0.20 else 0.0
        success_signal -= 0.35 if reject_rate >= 0.50 else 0.0
        success_signal -= 0.20 if alias_rate >= 0.50 and novelty_rate < 0.10 else 0.0
        success_signal = max(0.01, min(0.99, success_signal))
        alpha = 1.0 + success_signal * max(1, n)
        beta = 1.0 + (1.0 - success_signal) * max(1, n)
        mean = alpha / (alpha + beta)
        variance = (alpha * beta) / (((alpha + beta) ** 2) * (alpha + beta + 1.0))
        stdev = math.sqrt(max(0.0, variance))
        lo = max(0.0, mean - 1.64 * stdev)
        hi = min(1.0, mean + 1.64 * stdev)
        if n < 3:
            decision = "keep_probing_evidence_too_thin"
        elif lo >= 0.55 and reject_rate < 0.35:
            decision = "scale_treatment"
        elif hi <= 0.35 or reject_rate >= 0.75:
            decision = "abandon_or_quarantine_treatment"
        else:
            decision = "continue_controlled_probe"
        decisions.append({
            "treatment": row.get("treatment"),
            "candidate_count": n,
            "posterior_success_mean": round(mean, 4),
            "confidence_interval_90": [round(lo, 4), round(hi, 4)],
            "decision": decision,
            "reason": row.get("conclusion"),
        })
    decisions.sort(key=lambda row: float(row.get("posterior_success_mean") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Confidence-aware stop rules for treatment learning.",
        "decisions": decisions,
        "scale": [row for row in decisions if row.get("decision") == "scale_treatment"],
        "probe": [row for row in decisions if str(row.get("decision") or "").startswith(("continue", "keep"))],
        "abandon": [row for row in decisions if row.get("decision") == "abandon_or_quarantine_treatment"],
    }


def controlled_parent_sibling_experiments(
    rows: list[dict[str, Any]],
    treatment_confidence: dict[str, Any] | None = None,
    *,
    parent_limit: int = 6,
) -> dict[str, Any]:
    parents = sorted(
        rows,
        key=lambda row: (
            float(row.get("promotion_readiness_score") or 0.0),
            float(row.get("novelty_score") or 0.0),
            pnl(row),
        ),
        reverse=True,
    )[: max(1, int(parent_limit))]
    confidence_by_treatment = {
        str(row.get("treatment")): row
        for row in (treatment_confidence or {}).get("decisions") or []
        if isinstance(row, dict) and row.get("treatment")
    }
    template = [
        {"name": "same_parent_narrow", "mutation_lane": "exploitation", "treatment": "narrow_mutation", "mutation_scale_multiplier": 0.55},
        {"name": "same_parent_wide", "mutation_lane": "wild_shuffle", "treatment": "wide_mutation", "mutation_scale_multiplier": 1.35},
        {"name": "same_parent_holdout_repair", "mutation_lane": "holdout_repair", "treatment": "holdout_repair", "mutation_scale_multiplier": 0.70},
        {"name": "same_parent_route_widen", "mutation_lane": "edge_expansion", "treatment": "route_widen", "route_action": "widen", "mutation_scale_multiplier": 1.15},
        {"name": "same_parent_route_narrow", "mutation_lane": "route_safety_repair", "treatment": "route_narrow", "route_action": "narrow", "mutation_scale_multiplier": 0.55},
    ]
    experiments = []
    for idx, parent in enumerate(parents, 1):
        parent_ref = _experiment_candidate_ref(parent)
        treatments = []
        for item in template:
            conf = confidence_by_treatment.get(item["treatment"], {})
            budget = 20
            if conf.get("decision") == "scale_treatment":
                budget = 30
            elif conf.get("decision") == "abandon_or_quarantine_treatment":
                budget = 8
            treatments.append({**item, "budget_pct": budget, "confidence_decision": conf.get("decision")})
        experiments.append({
            "experiment_id": tournament_safety.stable_json_hash(
                {"kind": "controlled_parent_sibling", "parent": parent_ref.get("behavior_key"), "route": parent_ref.get("route_key")},
                length=20,
            ),
            "kind": "controlled_parent_sibling",
            "priority": idx,
            "parent": parent_ref,
            "family_key": parent_ref.get("family_key"),
            "route_key": parent_ref.get("route_key"),
            "hypothesis": "Hold parent fixed and compare sibling treatments so treatment effect is less confounded by parent quality.",
            "treatments": treatments,
            "success_metric": "winner must beat parent readiness or maintain readiness with higher P/L and no new review risk tags",
            "stop_rule": "Stop sibling branch after two cycles if no treatment beats the parent on readiness or P/L.",
            "worker_assignment": f"worker_{((idx - 1) % 4) + 1}",
        })
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Controlled sibling matrix: same parent, multiple mutation treatments.",
        "experiments": experiments,
    }


def _feedback_by_variant(feedback_rows: list[dict[str, Any]] | None = None) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in feedback_rows or []:
        if isinstance(row, dict):
            key = str(row.get("variant") or "")
            if key:
                out[key].append(row)
    return out


def _recommended_repair_lane(tags: list[str], reasons: list[str]) -> str:
    tag_set = set(tags)
    reason_set = set(reasons)
    if "thin_holdout_edge" in tag_set or "no_holdout_credit" in tag_set:
        return "holdout_repair"
    if "weak_day_consistency" in tag_set:
        return "day_consistency_repair"
    if "route_safety_failed" in reason_set or "route_contract_failed" in reason_set:
        return "route_safety_repair"
    if "small_total_edge" in tag_set:
        return "edge_expansion"
    if "high_overfit_risk" in tag_set or "robustness_score_below_70" in reason_set:
        return "robustness_first"
    return "promotion_readiness"


def live_beater_failure_autopsy(
    rows: list[dict[str, Any]],
    feedback_rows: list[dict[str, Any]] | None = None,
    *,
    limit: int = 50,
) -> dict[str, Any]:
    feedback = _feedback_by_variant(feedback_rows)
    autopsies = []
    for row in rows[:limit]:
        tags = list(row.get("learning_tags") or candidate_learning_tags(row))
        row_feedback = feedback.get(str(row.get("variant") or ""), [])
        reasons = sorted({
            str(reason)
            for item in row_feedback
            for reason in (item.get("reject_reasons") or [])
        })
        status = "passed_proxy"
        if row_feedback and any(str(item.get("status") or item.get("decision") or "").lower() in {"reject", "rejected", "blocked", "failed"} for item in row_feedback):
            status = "review_rejected"
        elif tags:
            status = "proxy_risk"
        by_day = row.get("by_day") if isinstance(row.get("by_day"), dict) else {}
        day_pnls = [float(item.get("pnl") or 0.0) for item in by_day.values() if isinstance(item, dict)]
        autopsies.append({
            "variant": row.get("variant"),
            "family_key": row.get("family_key") or candidate_family_key(row),
            "route_key": route_key_from_row(row),
            "status": status,
            "why_it_won": {
                "step2_pnl": pnl(row),
                "delta_vs_active": live_delta(row),
                "best_day_pnl": round(max(day_pnls), 4) if day_pnls else 0.0,
                "trade_count": trade_count(row),
                "win_rate_pct": win_rate(row),
            },
            "why_it_may_fail": tags[:10] + reasons[:10],
            "review_reject_reasons": reasons,
            "recommended_repair_lane": _recommended_repair_lane(tags, reasons),
            "family_action": "repair_family" if status != "passed_proxy" else "keep_as_promotion_candidate",
        })
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Autopsy for live beaters that still look likely to fail promotion or already rejected review.",
        "autopsies": autopsies,
    }


def worker_specialization_memory(
    rows: list[dict[str, Any]],
    treatment_effects: dict[str, Any] | None = None,
    *,
    max_workers: int = 4,
) -> dict[str, Any]:
    by_worker: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        worker = lineage_info(row).get("worker_role") or "unknown_worker"
        by_worker[str(worker)].append(row)
    treatment_scores = {
        str(row.get("treatment")): float(row.get("evidence_score") or 0.0)
        for row in (treatment_effects or {}).get("treatments") or []
        if isinstance(row, dict)
    }
    workers = []
    for worker, group in by_worker.items():
        treatments = Counter(_row_treatment(row) for row in group)
        best_treatment = treatments.most_common(1)[0][0] if treatments else "unlabeled"
        avg_pnl = _avg([pnl(row) for row in group])
        avg_ready = _avg([promotion_readiness_score(row) for row in group])
        skill_score = avg_ready + max(0.0, avg_pnl) * 0.01 + treatment_scores.get(best_treatment, 0.0)
        workers.append({
            "worker": worker,
            "candidate_count": len(group),
            "best_treatment": best_treatment,
            "treatment_counts": dict(treatments),
            "avg_step2_pnl": round(avg_pnl, 4),
            "avg_promotion_readiness_score": round(avg_ready, 4),
            "skill_score": round(skill_score, 4),
            "recommended_role": best_treatment if best_treatment != "unlabeled" else "broad_exploration",
        })
    if not workers:
        workers = [{"worker": f"worker_{idx}", "recommended_role": role, "candidate_count": 0, "skill_score": 0.0}
                   for idx, role in enumerate(["exploitation", "holdout_repair", "wild_shuffle", "robustness_first"], 1)]
    workers.sort(key=lambda row: float(row.get("skill_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Worker specialization memory based on actual live-beating output and treatment quality.",
        "workers": workers[: max(1, min(4, int(max_workers)))],
    }


def _regime_key(row: dict[str, Any]) -> str:
    match = route_match(row)
    ticker = match.get("ticker") or "*"
    setup = match.get("setup_type") or "*"
    session = match.get("session_phase") or "*"
    by_day = row.get("by_day") if isinstance(row.get("by_day"), dict) else {}
    day_pnls = [float(item.get("pnl") or 0.0) for item in by_day.values() if isinstance(item, dict)]
    spread = (max(day_pnls) - min(day_pnls)) if len(day_pnls) >= 2 else 0.0
    volatility = "volatile" if spread >= max(250.0, abs(pnl(row)) * 0.50) else "stable"
    return "|".join([ticker, setup, session, volatility])


def regime_aware_learning(
    rows: list[dict[str, Any]],
    feedback_rows: list[dict[str, Any]] | None = None,
    *,
    limit: int = 25,
) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[_regime_key(row)].append(row)
    regimes = []
    for key, group in grouped.items():
        effects = treatment_effect_analyzer(group, feedback_rows)
        best = effects.get("best_treatment")
        regimes.append({
            "regime_key": key,
            "candidate_count": len(group),
            "best_treatment": best,
            "worst_treatment": effects.get("worst_treatment"),
            "best_step2_pnl": round(max([pnl(row) for row in group] or [0.0]), 4),
            "avg_promotion_readiness_score": round(_avg([promotion_readiness_score(row) for row in group]), 4),
            "top_routes": [route_key_from_row(row) for row in sorted(group, key=pnl, reverse=True)[:3]],
            "directive": f"Prefer {best} in {key}" if best else "collect_more_regime_evidence",
        })
    regimes.sort(key=lambda row: (int(row.get("candidate_count") or 0), float(row.get("best_step2_pnl") or 0.0)), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Treatment and winner memory split by ticker/setup/session/volatility regime.",
        "regimes": regimes[:limit],
    }


def promotion_reject_simulator(
    rows: list[dict[str, Any]],
    feedback_rows: list[dict[str, Any]] | None = None,
    *,
    limit: int = 50,
) -> dict[str, Any]:
    feedback_memory = failure_memory_feedback(feedback_rows)
    reason_counts = Counter(feedback_memory.get("reject_reason_counts") or {})
    total_reasons = sum(reason_counts.values()) or 1
    predictions = []
    for row in rows[:limit]:
        tags = list(row.get("learning_tags") or candidate_learning_tags(row))
        likely = []
        base = 0.10
        for tag, reason, score in [
            ("thin_holdout_edge", "holdout_edge_too_thin", 0.30),
            ("no_holdout_credit", "holdout_edge_too_thin", 0.30),
            ("weak_day_consistency", "day_consistency_too_weak", 0.25),
            ("small_total_edge", "total_edge_too_small", 0.18),
            ("high_overfit_risk", "overfit_or_alias_risk", 0.30),
            ("route_narrow", "route_safety_or_overfit_risk", 0.22),
            ("ticker_concentrated", "ticker_concentration_risk", 0.18),
            ("side_concentrated", "side_concentration_risk", 0.16),
        ]:
            if tag in tags:
                memory_bonus = reason_counts.get(reason, 0) / total_reasons * 0.20
                likely.append({"reason": reason, "probability": round(min(0.95, base + score + memory_bonus), 4)})
        likely.sort(key=lambda item: float(item.get("probability") or 0.0), reverse=True)
        predictions.append({
            "variant": row.get("variant"),
            "route_key": route_key_from_row(row),
            "family_key": row.get("family_key") or candidate_family_key(row),
            "promotion_readiness_score": row.get("promotion_readiness_score"),
            "likely_reject_reasons": likely[:5],
            "predicted_reject_probability": round(max([float(item.get("probability") or 0.0) for item in likely] or [0.05]), 4),
            "repair_lane": _recommended_repair_lane(tags, [item.get("reason") for item in likely]),
        })
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Cheap promotion-reject simulator used before expensive promotion review.",
        "predictions": predictions,
    }


def search_portfolio_manager(
    rows: list[dict[str, Any]],
    treatment_confidence: dict[str, Any] | None = None,
    live_autopsy: dict[str, Any] | None = None,
    regime_learning: dict[str, Any] | None = None,
) -> dict[str, Any]:
    scale_count = len((treatment_confidence or {}).get("scale") or [])
    abandon_count = len((treatment_confidence or {}).get("abandon") or [])
    autopsies = list((live_autopsy or {}).get("autopsies") or [])
    repair_pressure = sum(1 for row in autopsies if row.get("status") != "passed_proxy")
    regime_count = len((regime_learning or {}).get("regimes") or [])
    exploit = 35.0 + min(15.0, scale_count * 5.0)
    repair = 25.0 + min(15.0, repair_pressure * 2.0)
    controlled = 25.0 + min(10.0, regime_count * 0.5)
    exploration = 15.0 + min(10.0, abandon_count * 3.0)
    total = exploit + repair + controlled + exploration
    buckets = [
        ("exploit_best_treatments", exploit, "Scale treatments with confidence and keep top P/L pressure."),
        ("repair_near_misses", repair, "Repair live beaters that fail promotion proxies or review feedback."),
        ("controlled_experiments", controlled, "Run sibling experiments and regime splits for causal learning."),
        ("weird_exploration", exploration, "Try scoring/indicator shuffles outside current basins."),
    ]
    allocations = [
        {"bucket": name, "budget_pct": round(value / total * 100.0, 2), "directive": directive}
        for name, value, directive in buckets
    ]
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Portfolio manager balancing exploitation, repair, controlled learning, and weird exploration.",
        "allocations": allocations,
        "top_routes": [row.get("route_key") for row in route_clusters(rows, limit=6)],
        "notes": [
            "Budget adapts upward for scaling when treatments have confidence.",
            "Repair budget rises when live beaters are failing promotion proxies.",
            "Exploration rises when treatments should be abandoned or current basins are stale.",
        ],
    }


def causal_experiment_registry(
    experiment_plan: dict[str, Any] | None = None,
    controlled_sibling_experiments: dict[str, Any] | None = None,
    outcome_ledger: dict[str, Any] | None = None,
    treatment_confidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    outcomes = {
        str(row.get("experiment_id")): row
        for row in (outcome_ledger or {}).get("outcomes") or []
        if isinstance(row, dict) and row.get("experiment_id")
    }
    confidence = {
        str(row.get("treatment")): row
        for row in (treatment_confidence or {}).get("decisions") or []
        if isinstance(row, dict) and row.get("treatment")
    }
    registry = []
    for source_name, plan in [
        ("active_experiment_plan", experiment_plan or {}),
        ("controlled_parent_sibling_experiments", controlled_sibling_experiments or {}),
    ]:
        for exp in plan.get("experiments") or []:
            if not isinstance(exp, dict):
                continue
            treatment_decisions = []
            for treatment in exp.get("treatments") or []:
                name = _row_treatment(treatment)
                if name in confidence:
                    treatment_decisions.append(confidence[name])
            outcome = outcomes.get(str(exp.get("experiment_id"))) or {}
            verdict = outcome.get("next_action") or "planned"
            if any(row.get("decision") == "scale_treatment" for row in treatment_decisions):
                verdict = "scale_best_arm"
            elif any(row.get("decision") == "abandon_or_quarantine_treatment" for row in treatment_decisions):
                verdict = "retry_or_abandon_weak_arms"
            registry.append({
                "experiment_id": exp.get("experiment_id"),
                "source_artifact": source_name,
                "kind": exp.get("kind"),
                "hypothesis": exp.get("hypothesis"),
                "family_key": exp.get("family_key"),
                "route_key": exp.get("route_key"),
                "parent": exp.get("parent"),
                "treatments": exp.get("treatments") or [],
                "expected_improvement": exp.get("success_metric"),
                "observed_outcome": outcome,
                "treatment_decisions": treatment_decisions,
                "verdict": verdict,
                "status": "resolved" if outcome else "planned",
            })
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "First-class causal experiment registry for hypotheses, treatment arms, outcomes, and verdicts.",
        "experiments": registry,
    }


def experiment_debt_queue(
    causal_registry: dict[str, Any] | None = None,
    treatment_confidence: dict[str, Any] | None = None,
    regime_learning: dict[str, Any] | None = None,
    failure_autopsy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    debt = []
    for exp in (causal_registry or {}).get("experiments") or []:
        if exp.get("status") != "resolved" or exp.get("verdict") in {"continue_small_probe", "retry_or_abandon_weak_arms", "planned"}:
            debt.append({
                "question": exp.get("hypothesis") or "unanswered_experiment",
                "experiment_id": exp.get("experiment_id"),
                "route_key": exp.get("route_key"),
                "family_key": exp.get("family_key"),
                "priority_score": float((exp.get("parent") or {}).get("promotion_readiness_score") or 0.0) + 50.0,
                "recommended_action": "run_controlled_followup",
            })
    for row in (treatment_confidence or {}).get("probe") or []:
        treatment = row.get("treatment")
        debt.append({
            "question": f"Does {treatment} improve expected promotability with more samples?",
            "experiment_id": "",
            "route_key": "",
            "family_key": "",
            "priority_score": 40.0 + float(row.get("posterior_success_mean") or 0.0) * 50.0,
            "recommended_action": "collect_more_treatment_evidence",
        })
    for regime in (regime_learning or {}).get("regimes") or []:
        if int(regime.get("candidate_count") or 0) < 3:
            debt.append({
                "question": f"Is {regime.get('best_treatment') or 'any treatment'} real in regime {regime.get('regime_key')}?",
                "experiment_id": "",
                "route_key": (regime.get("top_routes") or [""])[0],
                "family_key": "",
                "priority_score": 35.0,
                "recommended_action": "run_regime_probe",
            })
    for autopsy in (failure_autopsy or {}).get("autopsies") or []:
        if autopsy.get("status") != "passed_proxy":
            debt.append({
                "question": f"Can {autopsy.get('recommended_repair_lane')} repair {autopsy.get('variant')}?",
                "experiment_id": "",
                "route_key": autopsy.get("route_key"),
                "family_key": autopsy.get("family_key"),
                "priority_score": 60.0 + max(0.0, float((autopsy.get("why_it_won") or {}).get("delta_vs_active") or 0.0)) * 0.01,
                "recommended_action": "repair_live_beater_near_miss",
            })
    debt.sort(key=lambda row: float(row.get("priority_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Unanswered high-value learning questions preserved for future hunts.",
        "queue": debt[:100],
    }


def information_gain_scoring(
    rows: list[dict[str, Any]],
    experiment_debt: dict[str, Any] | None = None,
    treatment_confidence: dict[str, Any] | None = None,
    regime_learning: dict[str, Any] | None = None,
) -> dict[str, Any]:
    actions = []
    seen_routes = Counter(route_key_from_row(row) for row in rows)
    uncertain = [
        row for row in (treatment_confidence or {}).get("decisions") or []
        if row.get("decision") in {"keep_probing_evidence_too_thin", "continue_controlled_probe"}
    ]
    for row in uncertain:
        treatment = row.get("treatment")
        mean = float(row.get("posterior_success_mean") or 0.0)
        interval = row.get("confidence_interval_90") or [0.0, 1.0]
        width = float(interval[-1] or 0.0) - float(interval[0] or 0.0)
        actions.append({
            "action": "probe_treatment_uncertainty",
            "target": treatment,
            "information_gain_score": round(width * 60.0 + (1.0 - abs(mean - 0.5)) * 30.0, 4),
            "why": "wide confidence interval or ambiguous posterior treatment effect",
        })
    for debt in (experiment_debt or {}).get("queue") or []:
        actions.append({
            "action": debt.get("recommended_action"),
            "target": debt.get("question"),
            "route_key": debt.get("route_key"),
            "family_key": debt.get("family_key"),
            "information_gain_score": round(float(debt.get("priority_score") or 0.0), 4),
            "why": "unanswered experiment debt",
        })
    for regime in (regime_learning or {}).get("regimes") or []:
        route = (regime.get("top_routes") or [""])[0]
        scarcity = max(0.0, 5.0 - float(seen_routes.get(route, 0)))
        if scarcity > 0:
            actions.append({
                "action": "sample_underknown_regime",
                "target": regime.get("regime_key"),
                "route_key": route,
                "information_gain_score": round(25.0 + scarcity * 8.0, 4),
                "why": "regime has too little evidence to generalize safely",
            })
    actions.sort(key=lambda row: float(row.get("information_gain_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Ranks hunt actions by expected learning value, not just expected P/L.",
        "actions": actions[:100],
    }


def value_of_information_planner(
    information_gain: dict[str, Any] | None = None,
    experiment_debt: dict[str, Any] | None = None,
    treatment_confidence: dict[str, Any] | None = None,
    *,
    max_workers: int = 4,
) -> dict[str, Any]:
    confidence_rows = {
        str(row.get("treatment")): row
        for row in (treatment_confidence or {}).get("decisions") or []
        if isinstance(row, dict) and row.get("treatment")
    }
    plans = []
    for idx, action in enumerate((information_gain or {}).get("actions") or [], 1):
        target = str(action.get("target") or "")
        conf = confidence_rows.get(target, {})
        interval = conf.get("confidence_interval_90") or [0.0, 1.0]
        width = max(0.0, float(interval[-1] or 0.0) - float(interval[0] or 0.0))
        cost_cycles = 1 if action.get("action") in {"probe_treatment_uncertainty", "sample_underknown_regime"} else 2
        cost_workers = 1 if idx > max_workers else 2
        probability_changes_decision = min(0.95, 0.20 + width * 0.75 + float(action.get("information_gain_score") or 0.0) / 250.0)
        upside = float(action.get("information_gain_score") or 0.0) * 12.0
        ignore_downside = 50.0 if action.get("action") in {"repair_live_beater_near_miss", "run_controlled_followup"} else 20.0
        expected_value = probability_changes_decision * (upside + ignore_downside) / max(1.0, cost_cycles * cost_workers)
        plans.append({
            "rank": idx,
            "action": action.get("action"),
            "target": target,
            "route_key": action.get("route_key"),
            "family_key": action.get("family_key"),
            "cost_cycles": cost_cycles,
            "cost_workers": cost_workers,
            "probability_changes_decision": round(probability_changes_decision, 4),
            "upside_if_right": round(upside, 4),
            "downside_if_ignored": round(ignore_downside, 4),
            "expected_value_of_information": round(expected_value, 4),
            "why": action.get("why"),
        })
    for debt in (experiment_debt or {}).get("queue") or []:
        if any(plan.get("target") == debt.get("question") for plan in plans):
            continue
        score = float(debt.get("priority_score") or 0.0)
        plans.append({
            "rank": len(plans) + 1,
            "action": debt.get("recommended_action"),
            "target": debt.get("question"),
            "route_key": debt.get("route_key"),
            "family_key": debt.get("family_key"),
            "cost_cycles": 2,
            "cost_workers": 1,
            "probability_changes_decision": round(min(0.80, 0.25 + score / 200.0), 4),
            "upside_if_right": round(score * 10.0, 4),
            "downside_if_ignored": 40.0,
            "expected_value_of_information": round((0.25 + min(score / 200.0, 0.55)) * (score * 10.0 + 40.0) / 2.0, 4),
            "why": "open experiment debt",
        })
    plans.sort(key=lambda row: float(row.get("expected_value_of_information") or 0.0), reverse=True)
    for idx, plan in enumerate(plans, 1):
        plan["rank"] = idx
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Ranks experiments by expected value of information: decision-change odds, upside, downside, and cost.",
        "plans": plans[:100],
    }


def decision_change_tracker(
    treatment_confidence: dict[str, Any] | None = None,
    causal_registry: dict[str, Any] | None = None,
    search_portfolio: dict[str, Any] | None = None,
) -> dict[str, Any]:
    changes = []
    for row in (treatment_confidence or {}).get("decisions") or []:
        decision = row.get("decision")
        treatment = row.get("treatment")
        if decision in {"scale_treatment", "abandon_or_quarantine_treatment"}:
            changes.append({
                "type": "treatment_decision",
                "subject": treatment,
                "from": "probe",
                "to": decision,
                "confidence": row.get("posterior_success_mean"),
                "reason": row.get("reason"),
            })
    for exp in (causal_registry or {}).get("experiments") or []:
        verdict = str(exp.get("verdict") or "")
        if verdict not in {"planned", ""}:
            changes.append({
                "type": "experiment_verdict",
                "subject": exp.get("experiment_id"),
                "from": "planned",
                "to": verdict,
                "route_key": exp.get("route_key"),
                "reason": exp.get("hypothesis"),
            })
    allocations = (search_portfolio or {}).get("allocations") or []
    if allocations:
        leader = max(allocations, key=lambda row: float(row.get("budget_pct") or 0.0))
        changes.append({
            "type": "portfolio_budget",
            "subject": leader.get("bucket"),
            "from": "balanced",
            "to": "largest_budget_bucket",
            "budget_pct": leader.get("budget_pct"),
            "reason": leader.get("directive"),
        })
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Tracks learning velocity via decisions that changed because of evidence.",
        "changes": changes,
        "learning_velocity": round(len(changes), 4),
    }


def hypothesis_quality_scoring(causal_registry: dict[str, Any] | None = None) -> dict[str, Any]:
    scored = []
    for exp in (causal_registry or {}).get("experiments") or []:
        hypothesis = str(exp.get("hypothesis") or "")
        treatments = exp.get("treatments") or []
        score = 0.0
        reasons = []
        if exp.get("route_key") or exp.get("family_key"):
            score += 20.0
            reasons.append("specific_route_or_family")
        if len(treatments) >= 2:
            score += 20.0
            reasons.append("has_comparison_arms")
        if exp.get("expected_improvement"):
            score += 20.0
            reasons.append("measurable_success_metric")
        if "stop" in str((exp.get("observed_outcome") or {}).get("next_action") or exp.get("stop_rule") or "").lower() or exp.get("status"):
            score += 15.0
            reasons.append("has_stop_or_status_rule")
        if "same parent" in hypothesis.lower() or "parent" in exp:
            score += 15.0
            reasons.append("controls_parent")
        if "regime" in hypothesis.lower() or exp.get("route_key"):
            score += 10.0
            reasons.append("controls_regime_or_route")
        rewrite = ""
        if score < 60.0:
            rewrite = f"Rewrite as: For route/family {exp.get('route_key') or exp.get('family_key')}, compare named treatment arms against a parent/control and stop on a measurable promotion-readiness/P/L threshold."
        scored.append({
            "experiment_id": exp.get("experiment_id"),
            "hypothesis": hypothesis,
            "quality_score": round(min(100.0, score), 4),
            "reasons": reasons,
            "rewrite_suggestion": rewrite,
        })
    scored.sort(key=lambda row: float(row.get("quality_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Scores hypotheses for specificity, falsifiability, controls, metrics, and stop rules.",
        "hypotheses": scored,
    }


def evidence_sufficiency_gate(
    treatment_effects: dict[str, Any] | None = None,
    treatment_confidence: dict[str, Any] | None = None,
    *,
    min_candidates: int = 3,
    max_reject_rate: float = 0.40,
    max_alias_rate: float = 0.50,
    max_interval_width: float = 0.45,
) -> dict[str, Any]:
    effects = {
        str(row.get("treatment")): row
        for row in (treatment_effects or {}).get("treatments") or []
        if isinstance(row, dict) and row.get("treatment")
    }
    gates = []
    for conf in (treatment_confidence or {}).get("decisions") or []:
        treatment = str(conf.get("treatment") or "")
        effect = effects.get(treatment, {})
        interval = conf.get("confidence_interval_90") or [0.0, 1.0]
        width = max(0.0, float(interval[-1] or 0.0) - float(interval[0] or 0.0))
        failures = []
        if int(effect.get("candidate_count") or 0) < min_candidates:
            failures.append("not_enough_candidates")
        if float(effect.get("reject_rate") or 0.0) > max_reject_rate:
            failures.append("reject_rate_too_high")
        if float(effect.get("alias_rate") or 0.0) > max_alias_rate:
            failures.append("alias_rate_too_high")
        if width > max_interval_width:
            failures.append("confidence_interval_too_wide")
        gate = "allow_scale" if not failures and conf.get("decision") == "scale_treatment" else "keep_learning"
        if conf.get("decision") == "abandon_or_quarantine_treatment" and "not_enough_candidates" not in failures:
            gate = "allow_abandon"
        gates.append({
            "treatment": treatment,
            "candidate_count": int(effect.get("candidate_count") or 0),
            "reject_rate": effect.get("reject_rate"),
            "alias_rate": effect.get("alias_rate"),
            "confidence_interval_width": round(width, 4),
            "gate": gate,
            "failures": failures,
        })
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Requires enough evidence before scaling treatments or strategy changes.",
        "gates": gates,
        "scale_allowed": [row for row in gates if row.get("gate") == "allow_scale"],
        "abandon_allowed": [row for row in gates if row.get("gate") == "allow_abandon"],
    }


def counterfactual_shadow_board(rows: list[dict[str, Any]], *, limit: int = 20) -> dict[str, Any]:
    tasks = []
    for row in sorted(rows, key=lambda item: float(item.get("expected_promotable_pnl") or 0.0), reverse=True)[:limit]:
        weights = row.get("weights") if isinstance(row.get("weights"), dict) else {}
        top_feature = max(weights, key=lambda key: abs(float(weights.get(key) or 0.0))) if weights else ""
        route = route_key_from_row(row)
        tasks.extend([
            {
                "variant": row.get("variant"),
                "route_key": route,
                "check": "disable_route",
                "disprove_if": "candidate still wins without the route-specific edge",
                "priority_score": round(float(row.get("expected_promotable_pnl") or 0.0) + 25.0, 4),
            },
            {
                "variant": row.get("variant"),
                "route_key": route,
                "check": "weaken_top_feature",
                "feature": top_feature,
                "disprove_if": "small feature change collapses most of the edge",
                "priority_score": round(float(row.get("expected_promotable_pnl") or 0.0) + 15.0, 4),
            },
            {
                "variant": row.get("variant"),
                "route_key": route,
                "check": "alternate_day_window",
                "disprove_if": "edge only exists in the current day slice",
                "priority_score": round(float(row.get("expected_promotable_pnl") or 0.0) + 20.0, 4),
            },
        ])
    tasks.sort(key=lambda row: float(row.get("priority_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Counterfactual shadow checks that would disprove top candidates before promotion.",
        "tasks": tasks[:limit * 3],
    }


def learning_velocity_dashboard(
    rows: list[dict[str, Any]],
    decision_changes: dict[str, Any] | None = None,
    experiment_debt: dict[str, Any] | None = None,
    information_gain: dict[str, Any] | None = None,
    postmortem: dict[str, Any] | None = None,
) -> dict[str, Any]:
    top_expected = max([float(row.get("expected_promotable_pnl") or 0.0) for row in rows] or [0.0])
    changes = list((decision_changes or {}).get("changes") or [])
    debt = list((experiment_debt or {}).get("queue") or [])
    info = list((information_gain or {}).get("actions") or [])
    memo = list((postmortem or {}).get("memo") or [])
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Dashboard for learning velocity: facts, changed decisions, debt, and objective trend.",
        "new_facts_learned": memo[:10],
        "decisions_changed": changes,
        "unresolved_questions": len(debt),
        "top_unresolved_questions": debt[:10],
        "new_questions_created": len([row for row in debt if row.get("recommended_action")]),
        "top_information_gain_actions": info[:10],
        "expected_promotable_pnl_best": round(top_expected, 4),
        "learning_velocity_score": round(len(changes) * 10.0 + len(info[:5]) * 3.0 + min(25.0, top_expected / 100.0), 4),
    }


def policy_compiler(
    *,
    treatment_prior_model: dict[str, Any] | None = None,
    treatment_worker_budget: dict[str, Any] | None = None,
    search_portfolio: dict[str, Any] | None = None,
    evidence_gate: dict[str, Any] | None = None,
    value_of_information: dict[str, Any] | None = None,
    shadow_board: dict[str, Any] | None = None,
    experiment_debt: dict[str, Any] | None = None,
    active_experiment_plan: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Compiled machine-readable hunt policy: budgets, workers, stop rules, validation, debt, and promotion objective.",
        "objective": "maximize_truth_first_promotable_pnl",
        "treatment_budgets": (treatment_prior_model or {}).get("priors") or [],
        "worker_roles": (treatment_worker_budget or {}).get("workers") or [],
        "portfolio_budgets": (search_portfolio or {}).get("allocations") or [],
        "evidence_gates": (evidence_gate or {}).get("gates") or [],
        "scale_allowed": (evidence_gate or {}).get("scale_allowed") or [],
        "abandon_allowed": (evidence_gate or {}).get("abandon_allowed") or [],
        "value_of_information_queue": (value_of_information or {}).get("plans") or [],
        "validation_queue": (shadow_board or {}).get("tasks") or [],
        "experiment_debt_queue": (experiment_debt or {}).get("queue") or [],
        "active_experiments": (active_experiment_plan or {}).get("experiments") or [],
        "stop_rules": [
            "Do not scale a treatment unless evidence_sufficiency_gate allows it.",
            "Prefer VOI queue over raw exploration when decision-change probability is high.",
            "Run counterfactual shadow checks before promotion review on top expected-promotable candidates.",
            "Only keep candidates that beat current Live.",
        ],
    }


def policy_executor(compiled_policy: dict[str, Any] | None = None, *, max_workers: int = 4) -> dict[str, Any]:
    policy = compiled_policy or {}
    jobs = []

    def add_job(kind: str, source: dict[str, Any], priority: float, worker_hint: str = "") -> None:
        jobs.append({
            "job_id": tournament_safety.stable_json_hash({"kind": kind, "source": source, "priority": priority}, length=20),
            "kind": kind,
            "priority_score": round(float(priority or 0.0), 4),
            "worker_hint": worker_hint,
            "route_key": source.get("route_key"),
            "family_key": source.get("family_key"),
            "target": source.get("target") or source.get("question") or source.get("variant") or source.get("treatment"),
            "payload": source,
        })

    for row in policy.get("value_of_information_queue") or []:
        add_job("value_of_information", row, float(row.get("expected_value_of_information") or 0.0), "worker_1")
    for row in policy.get("experiment_debt_queue") or []:
        add_job("experiment_debt", row, float(row.get("priority_score") or 0.0), "worker_2")
    for row in policy.get("validation_queue") or []:
        add_job("shadow_validation", row, float(row.get("priority_score") or 0.0), "worker_3")
    for row in policy.get("truth_first_leaderboard") or []:
        add_job("truth_first_exploitation", row, float(row.get("truth_first_promotable_pnl") or row.get("expected_promotable_pnl") or 0.0), "worker_4")
    for row in policy.get("scale_allowed") or []:
        add_job("scale_allowed_treatment", row, 75.0 + float(row.get("candidate_count") or 0.0), "worker_1")

    jobs.sort(key=lambda row: float(row.get("priority_score") or 0.0), reverse=True)
    assignments = []
    for idx, job in enumerate(jobs[: max(1, int(max_workers)) * 6]):
        worker = job.get("worker_hint") or f"worker_{(idx % max(1, int(max_workers))) + 1}"
        assignments.append({**job, "worker": worker})
    focus_routes = []
    for job in assignments:
        route = str(job.get("route_key") or "")
        if route and route not in focus_routes:
            focus_routes.append(route)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Executable worker job queue compiled from the hunt policy.",
        "objective": policy.get("objective"),
        "jobs": jobs[:100],
        "assignments": assignments,
        "focus_routes": focus_routes[:12],
    }


def adaptive_worker_assignment(
    policy_execution: dict[str, Any] | None = None,
    worker_specialization: dict[str, Any] | None = None,
    *,
    max_workers: int = 4,
) -> dict[str, Any]:
    specialties = {
        str(row.get("worker")): row
        for row in (worker_specialization or {}).get("workers") or []
        if isinstance(row, dict) and row.get("worker")
    }
    remaining_jobs = list((policy_execution or {}).get("jobs") or [])
    assignments = []
    for idx in range(max(1, int(max_workers))):
        worker = f"worker_{idx + 1}"
        specialty = specialties.get(worker, {})
        role = str(specialty.get("recommended_role") or "")
        preferred = [
            job for job in remaining_jobs
            if role and (role in str(job.get("kind") or "") or role in str((job.get("payload") or {}).get("treatment") or ""))
        ]
        job = (preferred or remaining_jobs or [{}])[0]
        if job in remaining_jobs:
            remaining_jobs.remove(job)
        assignments.append({
            "worker": worker,
            "recommended_role": role or job.get("kind") or "policy_generalist",
            "job": job,
            "adjustment_rule": "Reassign next cycle based on policy outcome, calibration, and worker specialty memory.",
        })
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Adaptive worker assignment from executable policy jobs and worker specialty memory.",
        "assignments": assignments,
    }


def policy_backtester(rows: list[dict[str, Any]], compiled_policy: dict[str, Any] | None = None, *, limit: int = 50) -> dict[str, Any]:
    policy = compiled_policy or {}
    truth_keys = {
        str(row.get("behavior_key") or row.get("variant") or "")
        for row in (policy.get("truth_first_leaderboard") or [])[:limit]
    }
    raw_ranked = sorted(rows, key=pnl, reverse=True)[:limit]
    truth_ranked = sorted(rows, key=lambda row: float(row.get("truth_first_promotable_pnl") or row.get("expected_promotable_pnl") or 0.0), reverse=True)[:limit]
    raw_expected = _avg([float(row.get("expected_promotable_pnl") or 0.0) for row in raw_ranked[:10]])
    truth_expected = _avg([float(row.get("expected_promotable_pnl") or 0.0) for row in truth_ranked[:10]])
    avoided = [
        row.get("variant")
        for row in raw_ranked[:10]
        if "high_overfit_risk" in (row.get("learning_tags") or []) and str(row.get("behavior_key") or row.get("variant") or "") not in truth_keys
    ]
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Offline replay asking whether the compiled policy would prefer more promotable candidates than raw P/L.",
        "raw_top10_avg_expected_promotable_pnl": round(raw_expected, 4),
        "policy_top10_avg_expected_promotable_pnl": round(truth_expected, 4),
        "estimated_lift": round(truth_expected - raw_expected, 4),
        "avoided_high_risk_raw_pnl_variants": avoided,
        "verdict": "policy_improves_selection" if truth_expected >= raw_expected else "policy_needs_review",
    }


def policy_mutation_engine(compiled_policy: dict[str, Any] | None = None) -> dict[str, Any]:
    base = dict(compiled_policy or {})
    variants = []
    profiles = [
        ("truth_first_conservative", {"voi": 0.85, "validation": 1.25, "repair": 1.0, "explore": 0.70, "ood_penalty": 1.25}),
        ("high_voi_aggressive", {"voi": 1.45, "validation": 0.90, "repair": 0.85, "explore": 1.05, "ood_penalty": 0.90}),
        ("repair_heavy", {"voi": 0.95, "validation": 1.0, "repair": 1.50, "explore": 0.75, "ood_penalty": 1.0}),
        ("red_team_heavy", {"voi": 0.90, "validation": 1.60, "repair": 0.85, "explore": 0.80, "ood_penalty": 1.35}),
        ("weird_exploration_heavy", {"voi": 1.10, "validation": 0.85, "repair": 0.75, "explore": 1.60, "ood_penalty": 0.85}),
        ("raw_pnl_exploit", {"voi": 0.65, "validation": 0.70, "repair": 0.70, "explore": 0.55, "ood_penalty": 0.60}),
    ]
    for name, weights in profiles:
        policy = dict(base)
        policy["policy_id"] = tournament_safety.stable_json_hash({"base": base.get("objective"), "name": name, "weights": weights}, length=20)
        policy["policy_name"] = name
        policy["mutation_weights"] = weights
        policy["objective"] = "maximize_truth_first_promotable_pnl" if name != "raw_pnl_exploit" else "maximize_raw_pnl_with_safety_floor"
        variants.append(policy)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Mutated competing policies for policy-science tournaments.",
        "policies": variants,
    }


def _policy_variant_score(rows: list[dict[str, Any]], policy: dict[str, Any]) -> dict[str, Any]:
    weights = policy.get("mutation_weights") if isinstance(policy.get("mutation_weights"), dict) else {}
    truth = float(weights.get("ood_penalty") or 1.0)
    voi = float(weights.get("voi") or 1.0)
    validation = float(weights.get("validation") or 1.0)
    repair = float(weights.get("repair") or 1.0)
    explore = float(weights.get("explore") or 1.0)
    ranked = []
    for row in rows:
        tags = set(row.get("learning_tags") or [])
        score = float(row.get("expected_promotable_pnl") or 0.0)
        score += float(row.get("novelty_score") or 0.0) * 0.10 * explore
        if "thin_holdout_edge" in tags or "no_holdout_credit" in tags:
            score += 15.0 * repair
        if "high_overfit_risk" in tags or "route_narrow" in tags:
            score -= 20.0 * truth
            score += 8.0 * validation
        if policy.get("objective") == "maximize_raw_pnl_with_safety_floor":
            score = float(row.get("step2_pnl") or pnl(row)) - float(row.get("overfit_risk_score") or 0.0) * 1.5
        score += voi * 5.0
        ranked.append((score, row))
    ranked.sort(key=lambda item: item[0], reverse=True)
    top = [row for _, row in ranked[:10]]
    avg_truth = _avg([float(row.get("expected_promotable_pnl") or 0.0) for row in top])
    avg_pnl = _avg([pnl(row) for row in top])
    risk = _avg([float(row.get("overfit_risk_score") or 0.0) for row in top])
    return {
        "policy_id": policy.get("policy_id"),
        "policy_name": policy.get("policy_name"),
        "avg_expected_promotable_pnl": round(avg_truth, 4),
        "avg_step2_pnl": round(avg_pnl, 4),
        "avg_overfit_risk": round(risk, 4),
        "score": round(avg_truth + avg_pnl * 0.02 - risk * 2.0, 4),
        "top_variants": [row.get("variant") for row in top[:5]],
    }


def policy_tournament(rows: list[dict[str, Any]], policy_mutations: dict[str, Any] | None = None) -> dict[str, Any]:
    results = [_policy_variant_score(rows, policy) for policy in (policy_mutations or {}).get("policies") or []]
    results.sort(key=lambda row: float(row.get("score") or 0.0), reverse=True)
    for idx, row in enumerate(results, 1):
        row["rank"] = idx
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Tournament comparing mutated hunt policies on current/prior candidates.",
        "champion": results[0] if results else {},
        "challengers": results[1:],
        "results": results,
    }


def champion_challenger_memory(policy_tournament_payload: dict[str, Any] | None = None) -> dict[str, Any]:
    tournament = policy_tournament_payload or {}
    champion = tournament.get("champion") if isinstance(tournament.get("champion"), dict) else {}
    challengers = list(tournament.get("challengers") or [])
    switch = []
    for challenger in challengers[:5]:
        if float(challenger.get("avg_expected_promotable_pnl") or 0.0) > float(champion.get("avg_expected_promotable_pnl") or 0.0) * 1.10:
            switch.append({
                "challenger": challenger.get("policy_name"),
                "condition": "higher_expected_promotable_pnl_by_10pct",
                "reason": "challenger beats champion on truth-first objective",
            })
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Champion/challenger policy memory for deciding when to switch policy families.",
        "champion": champion,
        "challengers": challengers[:5],
        "switch_conditions": switch,
        "why_champion_won": (
            f"{champion.get('policy_name')} led tournament score with avg expected promotable P/L "
            f"{champion.get('avg_expected_promotable_pnl')}"
            if champion else ""
        ),
    }


def regime_specific_policies(regime_learning: dict[str, Any] | None = None, policy_tournament_payload: dict[str, Any] | None = None) -> dict[str, Any]:
    champion = ((policy_tournament_payload or {}).get("champion") or {}).get("policy_name") or "truth_first_conservative"
    policies = []
    for regime in (regime_learning or {}).get("regimes") or []:
        key = str(regime.get("regime_key") or "")
        best_treatment = str(regime.get("best_treatment") or "")
        if "volatile" in key:
            policy = "truth_first_conservative"
        elif best_treatment in {"holdout_repair", "day_consistency_repair"}:
            policy = "repair_heavy"
        elif int(regime.get("candidate_count") or 0) < 3:
            policy = "high_voi_aggressive"
        else:
            policy = champion
        policies.append({
            "regime_key": key,
            "policy_name": policy,
            "best_treatment": best_treatment,
            "route_keys": regime.get("top_routes") or [],
            "directive": f"Use {policy} in {key}",
        })
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Regime-specific policy choices from tournament results and regime learning.",
        "policies": policies,
    }


def causal_graph_of_learning(
    compiled_policy: dict[str, Any] | None = None,
    policy_execution: dict[str, Any] | None = None,
    rows: list[dict[str, Any]] | None = None,
    calibration_ledger: dict[str, Any] | None = None,
) -> dict[str, Any]:
    nodes = []
    edges = []

    def node(node_id: str, typ: str, **extra: Any) -> None:
        nodes.append({"id": node_id, "type": typ, **extra})

    policy_id = f"policy:{(compiled_policy or {}).get('objective') or 'unknown'}"
    node(policy_id, "policy", label=(compiled_policy or {}).get("objective"))
    for job in (policy_execution or {}).get("assignments") or []:
        job_id = f"job:{job.get('job_id')}"
        worker_id = f"worker:{job.get('worker')}"
        route_id = f"route:{job.get('route_key') or 'unrouted'}"
        node(job_id, "job", label=job.get("kind"), priority=job.get("priority_score"))
        node(worker_id, "worker", label=job.get("worker"))
        node(route_id, "route", label=job.get("route_key"))
        edges.extend([
            {"from": policy_id, "to": job_id, "type": "creates_job"},
            {"from": job_id, "to": worker_id, "type": "assigned_to"},
            {"from": job_id, "to": route_id, "type": "targets_route"},
        ])
    for row in rows or []:
        cand_id = f"candidate:{row.get('behavior_key') or row.get('variant')}"
        route_id = f"route:{route_key_from_row(row)}"
        node(cand_id, "candidate", label=row.get("variant"), pnl=pnl(row), expected_promotable_pnl=row.get("expected_promotable_pnl"))
        edges.append({"from": route_id, "to": cand_id, "type": "produced_candidate"})
    for entry in (calibration_ledger or {}).get("entries") or []:
        pred_id = f"prediction:{tournament_safety.stable_json_hash(entry, length=16)}"
        cand_id = f"candidate:{entry.get('variant')}"
        node(pred_id, "prediction", label=entry.get("prediction_type"), actual=entry.get("actual"))
        edges.append({"from": cand_id, "to": pred_id, "type": "calibrated_by"})
    unique = {item["id"]: item for item in nodes}
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Causal graph linking policy, worker assignment, route/regime, candidates, and calibration outcomes.",
        "nodes": list(unique.values()),
        "edges": edges,
    }


def policy_safety_rail(compiled_policy: dict[str, Any] | None = None) -> dict[str, Any]:
    policy = compiled_policy or {}
    failures = []
    if policy.get("objective") not in {"maximize_truth_first_promotable_pnl", "maximize_raw_pnl_with_safety_floor"}:
        failures.append("objective_not_recognized")
    if "Only keep candidates that beat current Live." not in (policy.get("stop_rules") or []):
        failures.append("missing_live_only_rule")
    if not policy.get("evidence_gates"):
        failures.append("missing_evidence_gates")
    if not policy.get("validation_queue"):
        failures.append("missing_validation_queue")
    if not policy.get("value_of_information_queue"):
        failures.append("missing_voi_queue")
    if not policy.get("portfolio_budgets"):
        failures.append("missing_portfolio_budget")
    if not policy.get("truth_first_leaderboard"):
        failures.append("missing_truth_first_leaderboard")
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Safety rail for compiled policies before execution.",
        "ok": not failures,
        "failures": failures,
        "required_properties": [
            "live_only",
            "evidence_gates",
            "validation_queue",
            "voi_queue",
            "portfolio_budget",
            "truth_first_leaderboard",
        ],
    }


def auto_promoted_field_manual(
    memory_distillation: dict[str, Any] | None = None,
    treatment_effects: dict[str, Any] | None = None,
    policy_tournament_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rules = []
    for rule in (memory_distillation or {}).get("durable_rules") or []:
        rules.append({"rule": rule, "status": "candidate_rule", "evidence": "memory_distillation"})
    for row in (treatment_effects or {}).get("treatments") or []:
        if int(row.get("candidate_count") or 0) >= 3 and float(row.get("reject_rate") or 0.0) <= 0.25 and float(row.get("evidence_score") or 0.0) > 5.0:
            rules.append({
                "rule": f"Scale {row.get('treatment')} when evidence resembles current sample.",
                "status": "promoted_rule",
                "evidence": row,
            })
    champion = (policy_tournament_payload or {}).get("champion") or {}
    if champion:
        rules.append({
            "rule": f"Default to policy {champion.get('policy_name')} until challenger beats champion.",
            "status": "promoted_rule",
            "evidence": champion,
        })
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Field manual rules promoted from hypotheses after enough evidence.",
        "rules": rules[:50],
    }


def policy_drift_detector(
    cycles: list[dict[str, Any]] | None = None,
    policy_tournament_payload: dict[str, Any] | None = None,
    calibration_ledger: dict[str, Any] | None = None,
    truth_first: dict[str, Any] | None = None,
    out_of_distribution: dict[str, Any] | None = None,
    feedback_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    cycles = cycles or []
    tournament = policy_tournament_payload or {}
    champion = tournament.get("champion") if isinstance(tournament.get("champion"), dict) else {}
    challengers = list(tournament.get("challengers") or [])
    calibration = float((calibration_ledger or {}).get("calibration_confidence") or 0.55)
    truth_rank1 = ((truth_first or {}).get("leaderboard") or [{}])[0] or {}
    truth_score = float(truth_rank1.get("truth_first_score") or truth_rank1.get("expected_promotable_pnl") or 0.0)
    detections = list((out_of_distribution or {}).get("detections") or [])
    ood_scores = [float(row.get("ood_score") or 0.0) for row in detections]
    ood_high = sum(1 for score in ood_scores if score >= 45.0)
    recent = cycles[-6:] if len(cycles) > 6 else cycles
    recent_scored = sum(_cycle_scored_total(cycle) for cycle in recent)
    recent_winners = sum(_cycle_winners(cycle) for cycle in recent)
    winner_yield_per_10k = recent_winners / max(1, recent_scored) * 10000.0
    rejects = [
        row for row in feedback_rows or []
        if str(row.get("status") or "").lower() in {"reject", "rejected", "failed", "fail"}
    ]
    reject_rate = len(rejects) / max(1, len(feedback_rows or []))
    best_challenger = challengers[0] if challengers else {}
    champion_score = float(champion.get("score") or 0.0)
    challenger_score = float(best_challenger.get("score") or 0.0)
    drift_reasons = []
    if best_challenger and challenger_score > champion_score * 1.05:
        drift_reasons.append("challenger_beating_champion")
    if calibration < 0.50:
        drift_reasons.append("calibration_confidence_low")
    if truth_score <= 0.0:
        drift_reasons.append("truth_first_edge_weak_or_missing")
    if ood_high >= 3:
        drift_reasons.append("ood_pressure_high")
    if recent and recent_winners == 0:
        drift_reasons.append("recent_hunt_yield_zero")
    elif recent_scored >= 500 and winner_yield_per_10k < 5.0:
        drift_reasons.append("recent_hunt_yield_low")
    if len(feedback_rows or []) >= 3 and reject_rate >= 0.50:
        drift_reasons.append("promotion_reject_rate_high")
    status = "stable"
    if drift_reasons:
        status = "drifting" if any(reason in drift_reasons for reason in {
            "challenger_beating_champion",
            "promotion_reject_rate_high",
            "recent_hunt_yield_zero",
        }) else "watch"
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Detects temporal drift in the hunt policy while the hunt is still running.",
        "status": status,
        "drift": status == "drifting",
        "champion_policy": champion.get("policy_name"),
        "best_challenger": best_challenger.get("policy_name"),
        "champion_score": round(champion_score, 4),
        "best_challenger_score": round(challenger_score, 4),
        "calibration_confidence": round(calibration, 4),
        "truth_first_rank1": truth_rank1.get("variant"),
        "truth_first_rank1_score": round(truth_score, 4),
        "recent_cycles": len(recent),
        "recent_scored": int(recent_scored),
        "recent_winners": int(recent_winners),
        "recent_winner_yield_per_10k": round(winner_yield_per_10k, 4),
        "promotion_reject_rate": round(reject_rate, 4),
        "high_ood_count": ood_high,
        "avg_ood_score": round(_avg(ood_scores), 4),
        "reasons": drift_reasons,
    }


def route_regime_half_life(
    rows: list[dict[str, Any]],
    regime_learning: dict[str, Any] | None = None,
    treatment_effects: dict[str, Any] | None = None,
    *,
    half_life_cycles: float = 6.0,
) -> dict[str, Any]:
    route_stats: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "count": 0,
        "decayed_sum": 0.0,
        "decay_mass": 0.0,
        "best_variant": "",
        "best_pnl": -1e18,
        "treatments": Counter(),
    })
    for idx, row in enumerate(rows):
        age = idx / max(1.0, float(half_life_cycles))
        decay = 0.5 ** age
        route_key = route_key_from_row(row)
        stats = route_stats[route_key]
        stats["count"] += 1
        stats["decayed_sum"] += float(row.get("expected_promotable_pnl") or pnl(row)) * decay
        stats["decay_mass"] += decay
        stats["treatments"][_row_treatment(row)] += 1
        row_pnl = pnl(row)
        if row_pnl > float(stats["best_pnl"]):
            stats["best_pnl"] = row_pnl
            stats["best_variant"] = row.get("variant") or ""
    routes = []
    for route, stats in route_stats.items():
        fresh = min(1.0, float(stats["decay_mass"]) / 3.5)
        decayed_expected = float(stats["decayed_sum"]) / max(1e-9, float(stats["decay_mass"]))
        action = "boost"
        if fresh < 0.35:
            action = "retest"
        elif decayed_expected <= 0.0:
            action = "downweight"
        elif fresh < 0.60:
            action = "probe"
        routes.append({
            "route_key": route,
            "candidate_count": int(stats["count"]),
            "decayed_score": round(float(stats["decayed_sum"]), 4),
            "decayed_expected_promotable_pnl": round(decayed_expected, 4),
            "freshness": round(fresh, 4),
            "freshness_score": round(fresh * 100.0, 2),
            "status": "fresh" if fresh >= 0.6 else "stale_retest",
            "recommended_action": action,
            "best_variant": stats.get("best_variant"),
            "best_step2_pnl": round(float(stats["best_pnl"]), 4) if float(stats["best_pnl"]) > -1e17 else 0.0,
            "dominant_treatment": (stats["treatments"].most_common(1)[0][0] if stats["treatments"] else "unknown"),
        })
    regimes = []
    for regime in (regime_learning or {}).get("regimes") or []:
        count = int(regime.get("candidate_count") or 0)
        freshness = min(1.0, count / 5.0)
        expected = float(regime.get("avg_expected_promotable_pnl") or regime.get("best_expected_promotable_pnl") or 0.0)
        regimes.append({
            "regime_key": regime.get("regime_key"),
            "freshness": round(freshness, 4),
            "freshness_score": round(freshness * 100.0, 2),
            "decayed_expected_promotable_pnl": round(expected * freshness, 4),
            "status": "fresh" if freshness >= 0.6 else "stale_retest",
            "policy_hint": "use_recent_evidence" if freshness >= 0.6 else "revalidate_before_scaling",
        })
    treatments = []
    for treatment in (treatment_effects or {}).get("treatments") or []:
        count = int(treatment.get("candidate_count") or 0)
        freshness = min(1.0, count / 5.0)
        expected = float(treatment.get("avg_expected_promotable_pnl") or treatment.get("evidence_score") or 0.0)
        reject_rate = float(treatment.get("reject_rate") or 0.0)
        treatments.append({
            "treatment": treatment.get("treatment"),
            "freshness": round(freshness, 4),
            "freshness_score": round(freshness * 100.0, 2),
            "decayed_expected_promotable_pnl": round(expected * freshness * (1.0 - min(0.9, reject_rate)), 4),
            "reject_rate": round(reject_rate, 4),
            "recommended_action": "boost" if freshness >= 0.6 and reject_rate < 0.35 else "retest",
        })
    routes.sort(key=lambda row: float(row.get("decayed_score") or 0.0), reverse=True)
    treatments.sort(key=lambda row: float(row.get("decayed_expected_promotable_pnl") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Half-life weighted route/regime evidence so stale wins fade over time.",
        "half_life_cycles": half_life_cycles,
        "routes": routes,
        "regimes": regimes,
        "treatments": treatments,
    }


def learning_market_map(
    rows: list[dict[str, Any]],
    treatment_effects: dict[str, Any] | None = None,
    regime_learning: dict[str, Any] | None = None,
    worker_assignment: dict[str, Any] | None = None,
    failure_memory: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cells: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "candidates": 0,
        "expected_sum": 0.0,
        "pnl_sum": 0.0,
        "routes": set(),
        "reject_reasons": Counter(),
    })
    treatment_by_name = {
        str(row.get("treatment")): row
        for row in (treatment_effects or {}).get("treatments") or []
        if isinstance(row, dict)
    }
    for row in rows:
        treatment = _row_treatment(row)
        key = f"{route_key_from_row(row)}::{treatment}"
        cell = cells[key]
        cell["candidates"] += 1
        cell["expected_sum"] += float(row.get("expected_promotable_pnl") or 0.0)
        cell["pnl_sum"] += pnl(row)
        cell["routes"].add(route_key_from_row(row))
    for directive in (failure_memory or {}).get("directives") or []:
        route = str(directive.get("route_key") or "")
        reason = str(directive.get("top_reject_reason") or "")
        if route and reason:
            for key, cell in cells.items():
                if key.startswith(f"{route}::"):
                    cell["reject_reasons"][reason] += int(directive.get("reject_count") or 1)
    market = []
    for key, cell in cells.items():
        route, treatment = key.split("::", 1)
        effect = treatment_by_name.get(treatment, {})
        candidates = int(cell["candidates"] or 0)
        reject_penalty = sum(cell["reject_reasons"].values()) * 2.5
        velocity = (
            float(cell["expected_sum"]) / max(1, candidates)
            + float(effect.get("evidence_score") or 0.0)
            + (float(cell["pnl_sum"]) / max(1, candidates)) * 0.02
            - reject_penalty
        )
        market.append({
            "cell": key,
            "cell_type": "route_x_treatment",
            "route_key": route,
            "treatment": treatment,
            "candidates": candidates,
            "learning_score": round(velocity, 4),
            "learning_velocity": round(velocity, 4),
            "top_reject_reasons": [{"reason": reason, "count": count} for reason, count in cell["reject_reasons"].most_common(3)],
            "recommended_budget": "increase" if velocity > 25.0 else "watch",
        })
    for regime in (regime_learning or {}).get("regimes") or []:
        score = float(regime.get("avg_expected_promotable_pnl") or regime.get("best_expected_promotable_pnl") or 0.0)
        count = int(regime.get("candidate_count") or 0)
        market.append({
            "cell": f"{regime.get('regime_key')}::regime_policy",
            "cell_type": "regime_x_policy",
            "regime_key": regime.get("regime_key"),
            "policy_name": regime.get("policy_name") or regime.get("best_policy"),
            "candidates": count,
            "learning_score": round(score * min(1.0, count / 5.0), 4),
            "recommended_budget": "increase" if count >= 3 and score > 0 else "probe",
        })
    for assignment in (worker_assignment or {}).get("assignments") or []:
        market.append({
            "cell": f"{assignment.get('worker')}::{assignment.get('kind')}",
            "cell_type": "worker_x_job",
            "worker": assignment.get("worker"),
            "job_kind": assignment.get("kind"),
            "route_key": assignment.get("route_key"),
            "learning_score": round(float(assignment.get("priority_score") or 0.0), 4),
            "recommended_budget": "increase" if float(assignment.get("priority_score") or 0.0) >= 70.0 else "watch",
        })
    market.sort(key=lambda row: float(row.get("learning_velocity") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Map of where learning is fastest across route x treatment and reject reasons.",
        "cells": sorted(market, key=lambda row: float(row.get("learning_score") or row.get("learning_velocity") or 0.0), reverse=True)[:200],
        "worker_assignments": (worker_assignment or {}).get("assignments") or [],
    }


def concept_drift_alarms(
    policy_drift: dict[str, Any] | None = None,
    half_life: dict[str, Any] | None = None,
    treatment_effects: dict[str, Any] | None = None,
    failure_memory: dict[str, Any] | None = None,
    policy_tournament_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    alarms = []
    if (policy_drift or {}).get("status") == "drifting" or (policy_drift or {}).get("drift"):
        alarms.append({
            "alarm": "policy_champion_drift",
            "severity": "high",
            "reasons": (policy_drift or {}).get("reasons") or [],
        })
    for route in (half_life or {}).get("routes") or []:
        if route.get("status") == "stale_retest" or route.get("recommended_action") == "retest":
            alarms.append({
                "alarm": "route_evidence_stale",
                "severity": "medium",
                "route_key": route.get("route_key"),
                "action": "schedule_revalidation",
            })
    for row in (treatment_effects or {}).get("treatments") or []:
        if float(row.get("reject_rate") or 0.0) >= 0.5:
            alarms.append({
                "alarm": "treatment_reject_rate_high",
                "severity": "high",
                "treatment": row.get("treatment"),
                "reject_rate": row.get("reject_rate"),
            })
    champion = ((policy_tournament_payload or {}).get("champion") or {})
    if champion and float(champion.get("avg_overfit_risk") or 0.0) >= 30.0:
        alarms.append({
            "alarm": "champion_policy_risk_high",
            "severity": "medium",
            "policy_name": champion.get("policy_name"),
            "avg_overfit_risk": champion.get("avg_overfit_risk"),
        })
    for directive in (failure_memory or {}).get("directives") or []:
        if int(directive.get("reject_count") or 0) >= 2:
            alarms.append({
                "alarm": "reject_reason_cluster",
                "severity": "medium",
                "route_key": directive.get("route_key"),
                "reason": directive.get("top_reject_reason"),
            })
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Concept drift alarms for policies, routes, treatments, and reject reasons.",
        "alarms": alarms,
        "alarm_count": len(alarms),
        "highest_severity": "high" if any(row.get("severity") == "high" for row in alarms) else ("medium" if alarms else "none"),
    }


def revalidation_scheduler(
    field_manual: dict[str, Any] | None = None,
    half_life: dict[str, Any] | None = None,
    concept_alarms: dict[str, Any] | None = None,
    policy_tournament_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    tasks = []
    for route in (half_life or {}).get("routes") or []:
        if route.get("status") == "stale_retest" or route.get("recommended_action") == "retest":
            tasks.append({
                "task": "revalidate_stale_route",
                "route_key": route.get("route_key"),
                "priority_score": round(70.0 + max(0.0, 35.0 - float(route.get("freshness_score") or 0.0)), 4),
                "reason": "route evidence half-life expired",
                "why": "route evidence half-life expired",
            })
    for rule in (field_manual or {}).get("rules") or []:
        if rule.get("status") == "promoted_rule":
            tasks.append({
                "task": "retest_promoted_rule",
                "rule": rule.get("rule"),
                "priority_score": 55.0,
                "reason": "promoted rules require periodic revalidation",
                "why": "promoted rules require periodic revalidation",
            })
    for alarm in (concept_alarms or {}).get("alarms") or []:
        tasks.append({
            "task": f"investigate_{alarm.get('alarm')}",
            "route_key": alarm.get("route_key"),
            "priority_score": 90.0 if alarm.get("severity") == "high" else 65.0,
            "reason": alarm.get("alarm"),
            "why": alarm,
        })
    champion = (policy_tournament_payload or {}).get("champion") or {}
    if champion:
        tasks.append({
            "task": "revalidate_champion_policy",
            "policy_name": champion.get("policy_name"),
            "priority_score": 60.0,
            "reason": "champion assumptions should be checked against drift",
            "why": "champion assumptions should be checked against drift",
        })
    tasks.sort(key=lambda row: float(row.get("priority_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Schedules stale routes, rules, and champion assumptions for revalidation.",
        "queue": tasks[:100],
        "tasks": tasks[:100],
    }


def temporal_ensemble_policy(
    policy_tournament_payload: dict[str, Any] | None = None,
    champion_memory: dict[str, Any] | None = None,
    regime_policies: dict[str, Any] | None = None,
    policy_drift: dict[str, Any] | None = None,
    revalidation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    recent = ((policy_tournament_payload or {}).get("champion") or {}).get("policy_name") or "truth_first_conservative"
    long_term = ((champion_memory or {}).get("champion") or {}).get("policy_name") or recent
    regime = ((regime_policies or {}).get("policies") or [{}])[0].get("policy_name") or recent
    status = (policy_drift or {}).get("status") or "stable"
    revalidation_queue = list((revalidation or {}).get("queue") or (revalidation or {}).get("tasks") or [])
    exploration = "high_voi_aggressive" if status in {"watch", "drifting"} else "weird_exploration_heavy"
    weights: dict[str, float] = defaultdict(float)
    weights[recent] += 0.35
    weights[long_term] += 0.30
    weights[regime] += 0.25
    weights[exploration] += 0.10
    if status == "drifting":
        weights[recent] = max(0.15, weights.get(recent, 0.0) - 0.15)
        weights[exploration] = weights.get(exploration, 0.0) + 0.10
        weights["truth_first_conservative"] = weights.get("truth_first_conservative", 0.0) + 0.05
    elif revalidation_queue:
        weights[exploration] = weights.get(exploration, 0.0) + 0.05
        weights["truth_first_conservative"] = weights.get("truth_first_conservative", 0.0) + 0.10
    total = sum(weights.values()) or 1.0
    ensemble = [
        {"policy_name": name, "weight": round(weight / total, 4)}
        for name, weight in sorted(weights.items(), key=lambda item: item[1], reverse=True)
    ]
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Temporal ensemble combining recent, long-term, regime, and exploration policies.",
        "primary_policy": ensemble[0]["policy_name"] if ensemble else recent,
        "drift_status": status,
        "components": ensemble,
        "ensemble": ensemble,
        "revalidation_pressure": len(revalidation_queue),
        "directive": "Blend policy queues by ensemble weights instead of betting on one champion.",
    }


def active_experiment_governor(
    cycles: list[dict[str, Any]] | None = None,
    experiment_plan: dict[str, Any] | None = None,
    outcome_ledger: dict[str, Any] | None = None,
    policy_drift: dict[str, Any] | None = None,
    learning_market: dict[str, Any] | None = None,
    revalidation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cycles = cycles or []
    recent = cycles[-4:] if len(cycles) > 4 else cycles
    scored = sum(_cycle_scored_total(cycle) for cycle in recent)
    winners = sum(_cycle_winners(cycle) for cycle in recent)
    yield_per_10k = winners / max(1, scored) * 10000.0
    drift_status = (policy_drift or {}).get("status") or "stable"
    top_market = ((learning_market or {}).get("cells") or [{}])[0] or {}
    revalidation_pressure = len((revalidation or {}).get("queue") or (revalidation or {}).get("tasks") or [])
    decisions = []
    for exp in (experiment_plan or {}).get("experiments") or []:
        exp_id = exp.get("experiment_id") or tournament_safety.stable_json_hash(exp, length=16)
        action = "continue"
        reason = "experiment still has useful unresolved evidence"
        priority = 55.0
        if drift_status == "drifting":
            action = "mutate"
            reason = "policy drift detected; mutate experiment assumptions before scaling"
            priority = 88.0
        elif yield_per_10k >= 25.0:
            action = "expand"
            reason = "recent hunt yield is strong enough to scale the active hypothesis"
            priority = 82.0
        elif recent and winners == 0 and scored >= 200:
            action = "stop_or_pivot"
            reason = "recent experiment window produced no live beaters"
            priority = 78.0
        elif revalidation_pressure >= 3:
            action = "pause_for_revalidation"
            reason = "too much stale evidence must be retested before trusting this lane"
            priority = 74.0
        decisions.append({
            "experiment_id": exp_id,
            "kind": exp.get("kind"),
            "hypothesis": exp.get("hypothesis"),
            "decision": action,
            "priority_score": round(priority, 4),
            "reason": reason,
            "recent_yield_per_10k": round(yield_per_10k, 4),
            "best_market_cell": top_market.get("cell"),
        })
    if not decisions:
        decisions.append({
            "experiment_id": "bootstrap_next_experiment",
            "kind": "market_map_bootstrap",
            "decision": "start",
            "priority_score": 60.0,
            "reason": "no active experiment exists; bootstrap from the highest learning market cell",
            "best_market_cell": top_market.get("cell"),
        })
    decisions.sort(key=lambda row: float(row.get("priority_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Mid-hunt governor deciding whether each experiment should continue, expand, mutate, pause, or stop.",
        "recent_scored": int(scored),
        "recent_winners": int(winners),
        "recent_yield_per_10k": round(yield_per_10k, 4),
        "decisions": decisions,
        "top_decision": decisions[0] if decisions else {},
    }


def route_state_machine(
    rows: list[dict[str, Any]],
    half_life: dict[str, Any] | None = None,
    learning_market: dict[str, Any] | None = None,
    failure_memory: dict[str, Any] | None = None,
    concept_alarms: dict[str, Any] | None = None,
) -> dict[str, Any]:
    reject_routes = {
        str(row.get("route_key") or ""): row
        for row in (failure_memory or {}).get("directives") or []
        if row.get("route_key")
    }
    alarm_routes = {
        str(row.get("route_key") or "")
        for row in (concept_alarms or {}).get("alarms") or []
        if row.get("route_key")
    }
    market_by_route: dict[str, float] = defaultdict(float)
    for cell in (learning_market or {}).get("cells") or []:
        route = str(cell.get("route_key") or "")
        if route:
            market_by_route[route] = max(market_by_route[route], float(cell.get("learning_score") or 0.0))
    pnl_by_route: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        pnl_by_route[route_key_from_row(row)].append(pnl(row))
    states = []
    seen_routes = {route_key_from_row(row) for row in rows}
    seen_routes.update(str(row.get("route_key") or "") for row in (half_life or {}).get("routes") or [] if row.get("route_key"))
    for route in sorted(route for route in seen_routes if route):
        half = next((row for row in (half_life or {}).get("routes") or [] if row.get("route_key") == route), {})
        count = len(pnl_by_route.get(route) or [])
        best = max(pnl_by_route.get(route) or [0.0])
        freshness = float(half.get("freshness_score") or 0.0)
        market_score = float(market_by_route.get(route) or 0.0)
        state = "new"
        next_action = "probe"
        if route in reject_routes:
            state = "retired"
            next_action = "avoid_until_repaired"
        elif route in alarm_routes or half.get("recommended_action") == "retest":
            state = "revalidation"
            next_action = "run_revalidation_slice"
        elif count >= 5 and best > 0 and market_score >= 25.0:
            state = "scaling"
            next_action = "increase_budget"
        elif count >= 2 and best > 0:
            state = "promising"
            next_action = "controlled_expand"
        elif freshness < 35.0 and count > 0:
            state = "stale"
            next_action = "refresh_or_retire"
        states.append({
            "route_key": route,
            "state": state,
            "next_action": next_action,
            "candidate_count": count,
            "best_step2_pnl": round(best, 4),
            "freshness_score": round(freshness, 4),
            "learning_score": round(market_score, 4),
            "reject_memory": reject_routes.get(route) or {},
        })
    state_counts = Counter(row["state"] for row in states)
    priority_order = {"scaling": 0, "promising": 1, "revalidation": 2, "new": 3, "stale": 4, "retired": 5}
    states.sort(key=lambda row: (priority_order.get(str(row.get("state")), 9), -float(row.get("learning_score") or 0.0)))
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Lifecycle state machine for route research: new, promising, scaling, stale, revalidation, retired.",
        "states": states,
        "state_counts": dict(state_counts),
        "focus_routes": [row["route_key"] for row in states if row["state"] in {"scaling", "promising", "revalidation"}][:12],
    }


def negative_knowledge_bank(
    rows: list[dict[str, Any]],
    feedback_rows: list[dict[str, Any]] | None = None,
    failure_memory: dict[str, Any] | None = None,
    route_states: dict[str, Any] | None = None,
    concept_alarms: dict[str, Any] | None = None,
) -> dict[str, Any]:
    patterns = []
    for row in feedback_rows or []:
        if str(row.get("status") or "").lower() not in {"reject", "rejected", "failed", "fail"}:
            continue
        route = str(row.get("route_key") or "")
        reasons = row.get("reject_reasons") or row.get("reasons") or []
        if isinstance(reasons, str):
            reasons = [reasons]
        pattern_key = tournament_safety.stable_json_hash({"route": route, "reasons": reasons}, length=20)
        patterns.append({
            "pattern_key": pattern_key,
            "route_key": route,
            "reason": ", ".join(str(reason) for reason in reasons[:3]) or "promotion_rejected",
            "severity": "hard_avoid" if len(reasons) >= 2 else "caution",
            "source": "promotion_feedback",
        })
    for directive in (failure_memory or {}).get("directives") or []:
        route = str(directive.get("route_key") or "")
        reason = str(directive.get("top_reject_reason") or "failure_cluster")
        patterns.append({
            "pattern_key": tournament_safety.stable_json_hash({"route": route, "reason": reason}, length=20),
            "route_key": route,
            "reason": reason,
            "severity": "hard_avoid" if int(directive.get("reject_count") or 0) >= 3 else "caution",
            "source": "failure_memory",
        })
    for state in (route_states or {}).get("states") or []:
        if state.get("state") in {"retired", "stale"}:
            patterns.append({
                "pattern_key": tournament_safety.stable_json_hash({"route": state.get("route_key"), "state": state.get("state")}, length=20),
                "route_key": state.get("route_key"),
                "reason": f"route_state_{state.get('state')}",
                "severity": "hard_avoid" if state.get("state") == "retired" else "caution",
                "source": "route_state_machine",
            })
    for alarm in (concept_alarms or {}).get("alarms") or []:
        if alarm.get("severity") == "high":
            patterns.append({
                "pattern_key": tournament_safety.stable_json_hash(alarm, length=20),
                "route_key": alarm.get("route_key"),
                "reason": alarm.get("alarm"),
                "severity": "caution",
                "source": "concept_drift_alarm",
            })
    by_key = {str(row.get("pattern_key")): row for row in patterns if row.get("pattern_key")}
    avoid_routes = sorted({str(row.get("route_key")) for row in by_key.values() if row.get("route_key") and row.get("severity") == "hard_avoid"})
    caution_routes = sorted({str(row.get("route_key")) for row in by_key.values() if row.get("route_key") and row.get("severity") != "hard_avoid"})
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Durable negative knowledge: failed route/reason/treatment patterns the hunt should avoid or repair first.",
        "patterns": list(by_key.values())[:200],
        "avoid_routes": avoid_routes,
        "caution_routes": caution_routes,
        "avoid_rules": [
            f"Avoid {row.get('route_key')} until repaired: {row.get('reason')}"
            for row in by_key.values()
            if row.get("route_key") and row.get("severity") == "hard_avoid"
        ][:50],
    }


def promotion_survivor_model(
    rows: list[dict[str, Any]],
    feedback_rows: list[dict[str, Any]] | None = None,
    reject_simulator: dict[str, Any] | None = None,
    truth_first: dict[str, Any] | None = None,
    negative_bank: dict[str, Any] | None = None,
) -> dict[str, Any]:
    reject_by_variant = {
        str(row.get("variant") or row.get("behavior_key") or ""): row
        for row in feedback_rows or []
    }
    predicted_reject = {
        str(row.get("variant") or ""): float(row.get("predicted_reject_probability") or 0.0)
        for row in (reject_simulator or {}).get("predictions") or []
    }
    truth_by_variant = {
        str(row.get("variant") or ""): row
        for row in (truth_first or {}).get("leaderboard") or []
    }
    avoid_routes = set(negative_bank.get("avoid_routes") or []) if isinstance(negative_bank, dict) else set()
    predictions = []
    for idx, row in enumerate(rows[:100]):
        route = route_key_from_row(row)
        tags = set(row.get("learning_tags") or [])
        risk = float(row.get("overfit_risk_score") or 0.0)
        reject_prob = predicted_reject.get(str(row.get("variant") or ""), 0.0)
        penalty = risk * 0.012 + reject_prob * 0.35
        failure_reasons = []
        for tag in sorted(tags):
            if tag in {"thin_holdout_edge", "weak_day_consistency", "ticker_concentrated", "side_concentrated", "high_overfit_risk", "route_narrow"}:
                failure_reasons.append(tag)
                penalty += 0.07
        if route in avoid_routes:
            failure_reasons.append("negative_knowledge_route")
            penalty += 0.20
        if str(row.get("variant") or "") in reject_by_variant:
            failure_reasons.append("prior_feedback_rejected")
            penalty += 0.25
        truth_row = truth_by_variant.get(str(row.get("variant") or ""), {})
        truth_bonus = min(0.25, max(0.0, float(truth_row.get("truth_first_score") or truth_row.get("expected_promotable_pnl") or 0.0) / 400.0))
        survivor = max(0.02, min(0.98, 0.82 + truth_bonus - penalty))
        predictions.append({
            "rank": idx + 1,
            "variant": row.get("variant"),
            "behavior_key": row.get("behavior_key") or behavior_key(row),
            "route_key": route,
            "survival_probability": round(survivor, 4),
            "predicted_reject_probability": round(1.0 - survivor, 4),
            "expected_survivor_pnl": round(float(row.get("expected_promotable_pnl") or pnl(row)) * survivor, 4),
            "failure_reasons": failure_reasons[:8],
        })
    predictions.sort(key=lambda row: (float(row.get("survival_probability") or 0.0), float(row.get("expected_survivor_pnl") or 0.0)), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Predicts whether live-beating candidates are likely to survive promotion review and why they may fail.",
        "predictions": predictions,
        "promotion_survivor_top10": predictions[:10],
    }


def mutation_grammar_learner(
    rows: list[dict[str, Any]],
    treatment_effects: dict[str, Any] | None = None,
    learning_market: dict[str, Any] | None = None,
    route_states: dict[str, Any] | None = None,
) -> dict[str, Any]:
    treatment_rank = {
        str(row.get("treatment")): float(row.get("evidence_score") or row.get("avg_expected_promotable_pnl") or 0.0)
        for row in (treatment_effects or {}).get("treatments") or []
    }
    route_state = {str(row.get("route_key")): row for row in (route_states or {}).get("states") or []}
    grammar = []
    for cell in (learning_market or {}).get("cells") or []:
        if cell.get("cell_type") != "route_x_treatment":
            continue
        route = str(cell.get("route_key") or "")
        treatment = str(cell.get("treatment") or "standard")
        state = (route_state.get(route) or {}).get("state") or "new"
        score = float(cell.get("learning_score") or 0.0) + treatment_rank.get(treatment, 0.0)
        mutation_radius = "wide" if state in {"new", "stale"} else ("tight" if state == "scaling" else "medium")
        grammar.append({
            "grammar_id": tournament_safety.stable_json_hash({"route": route, "treatment": treatment, "state": state}, length=18),
            "route_key": route,
            "treatment": treatment,
            "route_state": state,
            "mutation_radius": mutation_radius,
            "parameter_biases": {
                "holdout_days": "increase" if "holdout" in treatment else "keep",
                "session_filter": "shuffle" if mutation_radius == "wide" else "preserve",
                "risk_threshold": "tighten" if state == "scaling" else "explore",
            },
            "learning_score": round(score, 4),
            "template": f"{mutation_radius}_mutation::{route}::{treatment}",
        })
    if not grammar and rows:
        for row in rows[:10]:
            route = route_key_from_row(row)
            grammar.append({
                "grammar_id": tournament_safety.stable_json_hash({"route": route, "fallback": True}, length=18),
                "route_key": route,
                "treatment": _row_treatment(row),
                "route_state": "new",
                "mutation_radius": "medium",
                "parameter_biases": {"session_filter": "shuffle", "risk_threshold": "explore"},
                "learning_score": round(float(row.get("expected_promotable_pnl") or pnl(row)), 4),
                "template": f"medium_mutation::{route}::{_row_treatment(row)}",
            })
    grammar.sort(key=lambda row: float(row.get("learning_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Learns mutation grammar templates from route state, treatment evidence, and learning market cells.",
        "grammar": grammar[:100],
        "top_templates": grammar[:12],
    }


def real_time_worker_rebalancer(
    cycles: list[dict[str, Any]] | None = None,
    worker_memory: dict[str, Any] | None = None,
    learning_market: dict[str, Any] | None = None,
    experiment_governor: dict[str, Any] | None = None,
    max_workers: int = 4,
) -> dict[str, Any]:
    by_hunter: dict[str, dict[str, float]] = defaultdict(lambda: {"cycles": 0.0, "scored": 0.0, "winners": 0.0})
    for cycle in cycles or []:
        hunter = str(cycle.get("hunter") or "unknown")
        by_hunter[hunter]["cycles"] += 1
        by_hunter[hunter]["scored"] += _cycle_scored_total(cycle)
        by_hunter[hunter]["winners"] += _cycle_winners(cycle)
    worker_rows = list((worker_memory or {}).get("workers") or [])
    market_cells = list((learning_market or {}).get("cells") or [])
    top_decision = (experiment_governor or {}).get("top_decision") or {}
    assignments = []
    for idx in range(max(1, int(max_workers))):
        worker = f"worker_{idx + 1}"
        specialty = (worker_rows[idx % len(worker_rows)] if worker_rows else {})
        market = (market_cells[idx % len(market_cells)] if market_cells else {})
        role = specialty.get("recommended_role") or market.get("treatment") or "explore"
        if top_decision.get("decision") in {"pause_for_revalidation", "stop_or_pivot"} and idx == 0:
            role = "revalidation"
        assignments.append({
            "worker": worker,
            "role": role,
            "route_key": market.get("route_key") or specialty.get("route_key"),
            "budget_pct": round(100.0 / max(1, int(max_workers)), 2),
            "reason": f"market={market.get('cell') or 'n/a'}; governor={top_decision.get('decision') or 'continue'}",
        })
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Real-time worker rebalance plan based on yield, worker specialties, market cells, and experiment governance.",
        "hunter_yield": {
            hunter: {
                "cycles": int(item["cycles"]),
                "winner_yield_per_10k": round(item["winners"] / max(1.0, item["scored"]) * 10000.0, 4),
            }
            for hunter, item in by_hunter.items()
        },
        "assignments": assignments,
    }


def hunt_replay_simulator(
    rows: list[dict[str, Any]],
    temporal_ensemble: dict[str, Any] | None = None,
    route_states: dict[str, Any] | None = None,
    negative_bank: dict[str, Any] | None = None,
    survivor_model: dict[str, Any] | None = None,
) -> dict[str, Any]:
    avoid_routes = set((negative_bank or {}).get("avoid_routes") or [])
    state_bonus = {
        str(row.get("route_key")): {"scaling": 1.20, "promising": 1.10, "revalidation": 0.85, "stale": 0.70, "retired": 0.30}.get(str(row.get("state")), 1.0)
        for row in (route_states or {}).get("states") or []
    }
    survivor = {
        str(row.get("variant")): float(row.get("survival_probability") or 0.5)
        for row in (survivor_model or {}).get("predictions") or []
    }
    policies = (temporal_ensemble or {}).get("components") or (temporal_ensemble or {}).get("ensemble") or []
    simulations = []
    for policy in policies or [{"policy_name": "truth_first_conservative", "weight": 1.0}]:
        name = str(policy.get("policy_name") or "unknown")
        weight = float(policy.get("weight") or 1.0)
        score = 0.0
        kept = 0
        for row in rows[:100]:
            route = route_key_from_row(row)
            if route in avoid_routes:
                continue
            kept += 1
            policy_mult = 1.0
            if name == "repair_heavy" and "thin_holdout_edge" in (row.get("learning_tags") or []):
                policy_mult = 1.15
            elif name == "high_voi_aggressive":
                policy_mult = 1.05 + float(row.get("novelty_score") or 0.0) * 0.002
            elif name == "truth_first_conservative":
                policy_mult = 1.0 - min(0.35, float(row.get("overfit_risk_score") or 0.0) * 0.01)
            score += float(row.get("expected_promotable_pnl") or pnl(row)) * survivor.get(str(row.get("variant")), 0.55) * state_bonus.get(route, 1.0) * policy_mult * weight
        simulations.append({
            "policy_name": name,
            "ensemble_weight": round(weight, 4),
            "simulated_expected_survivor_pnl": round(score, 4),
            "eligible_candidates": kept,
            "verdict": "prefer" if score > 0 else "avoid",
        })
    simulations.sort(key=lambda row: float(row.get("simulated_expected_survivor_pnl") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Replay simulator estimating which policy mix should produce promotable live beaters before committing hunt budget.",
        "simulations": simulations,
        "recommended_policy": (simulations[0] if simulations else {}).get("policy_name"),
    }


def resurrection_engine(
    rows: list[dict[str, Any]],
    failure_autopsy: dict[str, Any] | None = None,
    survivor_model: dict[str, Any] | None = None,
    negative_bank: dict[str, Any] | None = None,
) -> dict[str, Any]:
    survivor_by_variant = {
        str(row.get("variant")): row
        for row in (survivor_model or {}).get("predictions") or []
    }
    caution_routes = set((negative_bank or {}).get("caution_routes") or [])
    avoid_routes = set((negative_bank or {}).get("avoid_routes") or [])
    repairs = []
    for row in rows[:100]:
        route = route_key_from_row(row)
        tags = set(row.get("learning_tags") or [])
        survivor = survivor_by_variant.get(str(row.get("variant")), {})
        reject_prob = float(survivor.get("predicted_reject_probability") or 0.0)
        repair_reasons = []
        if "thin_holdout_edge" in tags or "no_holdout_credit" in tags:
            repair_reasons.append("holdout_repair")
        if "weak_day_consistency" in tags:
            repair_reasons.append("day_consistency_repair")
        if "ticker_concentrated" in tags or "side_concentrated" in tags:
            repair_reasons.append("concentration_repair")
        if route in caution_routes:
            repair_reasons.append("negative_memory_repair")
        if route in avoid_routes and not repair_reasons:
            continue
        if not repair_reasons and reject_prob < 0.35:
            continue
        priority = float(row.get("expected_promotable_pnl") or pnl(row)) * 0.05 + reject_prob * 40.0 + len(repair_reasons) * 8.0
        repairs.append({
            "variant": row.get("variant"),
            "behavior_key": row.get("behavior_key") or behavior_key(row),
            "route_key": route,
            "priority_score": round(priority, 4),
            "repair_treatments": repair_reasons or ["promotion_survivor_repair"],
            "predicted_reject_probability": round(reject_prob, 4),
            "source_pnl": round(pnl(row), 4),
        })
    for item in (failure_autopsy or {}).get("repair_queue") or []:
        route = str(item.get("route_key") or "")
        if route and route not in avoid_routes:
            repairs.append({
                "variant": item.get("variant"),
                "route_key": route,
                "priority_score": float(item.get("priority_score") or 50.0),
                "repair_treatments": item.get("repair_treatments") or ["failure_autopsy_repair"],
                "source": "live_beater_failure_autopsy",
            })
    repairs.sort(key=lambda row: float(row.get("priority_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Repair-specific queue for close candidates and caution routes that may be resurrected safely.",
        "repair_queue": repairs[:100],
        "top_resurrection": repairs[0] if repairs else {},
    }


def _row_numeric_weights(row: dict[str, Any]) -> dict[str, float]:
    weights = row.get("weights") if isinstance(row.get("weights"), dict) else {}
    out = {}
    for key, value in weights.items():
        try:
            out[str(key)] = float(value)
        except (TypeError, ValueError):
            continue
    return out


def causal_mutation_attribution(
    rows: list[dict[str, Any]],
    mutation_grammar: dict[str, Any] | None = None,
    candidate_lineage: dict[str, Any] | None = None,
    treatment_effects: dict[str, Any] | None = None,
) -> dict[str, Any]:
    route_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        route_groups[route_key_from_row(row)].append(row)
    attributions = []
    for route, group in route_groups.items():
        if len(group) < 2:
            continue
        ranked = sorted(group, key=lambda row: float(row.get("expected_promotable_pnl") or pnl(row)), reverse=True)
        leader = ranked[0]
        baseline = ranked[-1]
        leader_weights = _row_numeric_weights(leader)
        base_weights = _row_numeric_weights(baseline)
        deltas = []
        for key in sorted(set(leader_weights) | set(base_weights)):
            delta = leader_weights.get(key, 0.0) - base_weights.get(key, 0.0)
            if abs(delta) < 1e-9:
                continue
            direction = "tighten" if delta < 0 else "widen"
            deltas.append({
                "parameter": key,
                "delta": round(delta, 6),
                "direction": direction,
                "human_summary": f"{direction} {key} by {round(abs(delta), 6)}",
            })
        effect = float(leader.get("expected_promotable_pnl") or pnl(leader)) - float(baseline.get("expected_promotable_pnl") or pnl(baseline))
        treatment = _row_treatment(leader)
        template = next((row for row in (mutation_grammar or {}).get("grammar") or [] if row.get("route_key") == route), {})
        attributions.append({
            "attribution_id": tournament_safety.stable_json_hash({"route": route, "leader": leader.get("variant"), "baseline": baseline.get("variant")}, length=20),
            "route_key": route,
            "treatment": treatment,
            "leader_variant": leader.get("variant"),
            "baseline_variant": baseline.get("variant"),
            "expected_pnl_lift": round(effect, 4),
            "changed_parameters": deltas[:12],
            "mutation_template": template.get("template"),
            "causal_confidence": round(min(0.95, 0.35 + min(0.4, len(group) * 0.05) + min(0.2, max(0.0, effect) / 200.0)), 4),
        })
    treatment_scores = {
        str(row.get("treatment")): row
        for row in (treatment_effects or {}).get("treatments") or []
    }
    attributions.sort(key=lambda row: float(row.get("expected_pnl_lift") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Attributes improvement/failure to exact parameter and treatment changes inside route families.",
        "attributions": attributions[:100],
        "top_parameter_moves": attributions[:12],
        "treatment_context": treatment_scores,
        "lineage_context_count": len((candidate_lineage or {}).get("edges") or []),
    }


def uncertainty_budgeting(
    half_life: dict[str, Any] | None = None,
    treatment_confidence: dict[str, Any] | None = None,
    policy_drift: dict[str, Any] | None = None,
    learning_market: dict[str, Any] | None = None,
    route_states: dict[str, Any] | None = None,
) -> dict[str, Any]:
    uncertainty_rows = []
    market_by_route = {
        str(row.get("route_key")): float(row.get("learning_score") or 0.0)
        for row in (learning_market or {}).get("cells") or []
        if row.get("route_key")
    }
    state_by_route = {str(row.get("route_key")): row for row in (route_states or {}).get("states") or []}
    for route in (half_life or {}).get("routes") or []:
        route_key = str(route.get("route_key") or "")
        freshness = float(route.get("freshness") or 0.0)
        if freshness > 1.0:
            freshness /= 100.0
        value = float(route.get("decayed_expected_promotable_pnl") or route.get("decayed_score") or 0.0)
        uncertainty = max(0.05, min(0.95, 1.0 - freshness))
        state = (state_by_route.get(route_key) or {}).get("state") or route.get("status")
        exploration_value = max(0.0, value) * (0.65 + uncertainty)
        if state in {"revalidation", "new", "promising"}:
            exploration_value += 15.0
        uncertainty_rows.append({
            "kind": "route",
            "key": route_key,
            "route_key": route_key,
            "state": state,
            "expected_value": round(value, 4),
            "uncertainty": round(uncertainty, 4),
            "learning_score": round(market_by_route.get(route_key, 0.0), 4),
            "budget_score": round(exploration_value + market_by_route.get(route_key, 0.0) * 0.25, 4),
            "recommended_budget": "learn" if uncertainty >= 0.45 else "exploit",
        })
    for row in (treatment_confidence or {}).get("probe") or []:
        treatment = str(row.get("treatment") or "")
        confidence = float(row.get("confidence") or row.get("posterior_confidence") or 0.35)
        uncertainty_rows.append({
            "kind": "treatment",
            "key": treatment,
            "treatment": treatment,
            "expected_value": round(float(row.get("expected_value") or row.get("evidence_score") or 0.0), 4),
            "uncertainty": round(max(0.05, 1.0 - confidence), 4),
            "budget_score": round(25.0 * max(0.05, 1.0 - confidence), 4),
            "recommended_budget": "probe",
        })
    drift_status = (policy_drift or {}).get("status") or "stable"
    if drift_status in {"watch", "drifting"}:
        uncertainty_rows.append({
            "kind": "policy",
            "key": "policy_drift_retest",
            "expected_value": 0.0,
            "uncertainty": 0.85 if drift_status == "drifting" else 0.55,
            "budget_score": 70.0 if drift_status == "drifting" else 45.0,
            "recommended_budget": "retest_policy",
        })
    uncertainty_rows.sort(key=lambda row: float(row.get("budget_score") or 0.0), reverse=True)
    total = sum(max(0.0, float(row.get("budget_score") or 0.0)) for row in uncertainty_rows[:12]) or 1.0
    for row in uncertainty_rows[:12]:
        row["recommended_budget_pct"] = round(100.0 * max(0.0, float(row.get("budget_score") or 0.0)) / total, 2)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Allocates hunt budget by both expected value and uncertainty/learning value.",
        "budgets": uncertainty_rows[:100],
        "top_budget": uncertainty_rows[0] if uncertainty_rows else {},
    }


def promotability_pareto_frontier(
    rows: list[dict[str, Any]],
    survivor_model: dict[str, Any] | None = None,
) -> dict[str, Any]:
    survivor = {
        str(row.get("variant")): float(row.get("survival_probability") or 0.5)
        for row in (survivor_model or {}).get("predictions") or []
    }
    points = []
    for row in rows[:100]:
        robust = row.get("robustness") if isinstance(row.get("robustness"), dict) else {}
        metrics = {
            "raw_pnl": pnl(row),
            "live_delta": float(row.get("step2_delta_vs_active") or row.get("delta_vs_active") or 0.0),
            "robustness": float(robust.get("adjusted_score") or robust.get("score") or row.get("promotion_quality_score") or 0.0),
            "survivor_probability": survivor.get(str(row.get("variant")), 0.55),
            "novelty": float(row.get("novelty_score") or 0.0),
        }
        points.append({
            "variant": row.get("variant"),
            "behavior_key": row.get("behavior_key") or behavior_key(row),
            "route_key": route_key_from_row(row),
            **{key: round(value, 4) for key, value in metrics.items()},
            "_metrics": metrics,
        })
    frontier = []
    for point in points:
        metrics = point["_metrics"]
        dominated = False
        for other in points:
            if other is point:
                continue
            other_metrics = other["_metrics"]
            if all(other_metrics[key] >= metrics[key] for key in metrics) and any(other_metrics[key] > metrics[key] for key in metrics):
                dominated = True
                break
        if not dominated:
            clean = dict(point)
            clean.pop("_metrics", None)
            clean["frontier_score"] = round(
                clean["raw_pnl"] * 0.01
                + clean["live_delta"] * 0.05
                + clean["robustness"] * 0.30
                + clean["survivor_probability"] * 60.0
                + clean["novelty"] * 0.10,
                4,
            )
            frontier.append(clean)
    frontier.sort(key=lambda row: float(row.get("frontier_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Non-dominated candidate frontier across P/L, robustness, survivor probability, novelty, and live delta.",
        "frontier": frontier[:50],
        "frontier_count": len(frontier),
    }


def false_lesson_detector(
    rows: list[dict[str, Any]],
    cycles: list[dict[str, Any]] | None = None,
    route_states: dict[str, Any] | None = None,
    out_of_distribution: dict[str, Any] | None = None,
    half_life: dict[str, Any] | None = None,
) -> dict[str, Any]:
    lessons = []
    route_counts = Counter(route_key_from_row(row) for row in rows)
    ticker_counts = Counter(str((row.get("ticker") or route_key_from_row(row).split("|")[0] or "")) for row in rows)
    recent = cycles[-4:] if cycles else []
    scored = sum(_cycle_scored_total(cycle) for cycle in recent)
    winners = sum(_cycle_winners(cycle) for cycle in recent)
    yield_per_10k = winners / max(1, scored) * 10000.0
    ood_by_variant = {
        str(row.get("variant")): float(row.get("ood_score") or 0.0)
        for row in (out_of_distribution or {}).get("detections") or []
    }
    freshness_by_route = {
        str(row.get("route_key")): float(row.get("freshness_score") or 0.0)
        for row in (half_life or {}).get("routes") or []
    }
    for row in rows[:50]:
        route = route_key_from_row(row)
        reasons = []
        risk = 0.0
        if route_counts[route] <= 1:
            reasons.append("single_candidate_route")
            risk += 25.0
        if ticker_counts[str(row.get("ticker") or route.split("|")[0] or "")] >= max(3, len(rows) // 3):
            reasons.append("ticker_concentration")
            risk += 15.0
        if float(row.get("overfit_risk_score") or 0.0) >= 25.0:
            reasons.append("overfit_risk_high")
            risk += 20.0
        if ood_by_variant.get(str(row.get("variant")), 0.0) >= 45.0:
            reasons.append("ood_candidate")
            risk += 20.0
        if freshness_by_route.get(route, 100.0) < 35.0:
            reasons.append("stale_route_evidence")
            risk += 15.0
        if recent and yield_per_10k <= 3.0:
            reasons.append("weak_recent_hunt_yield")
            risk += 10.0
        if reasons:
            lessons.append({
                "variant": row.get("variant"),
                "route_key": route,
                "false_lesson_risk_score": round(min(100.0, risk), 4),
                "reasons": reasons,
                "advice": "treat_as_hypothesis_not_rule" if risk >= 30.0 else "watch",
            })
    lessons.sort(key=lambda row: float(row.get("false_lesson_risk_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Detects likely false lessons caused by noisy wins, tiny samples, concentration, OOD, or stale evidence.",
        "lessons": lessons,
        "highest_risk": lessons[0] if lessons else {},
    }


def experiment_graduation_system(
    registry: dict[str, Any] | None = None,
    outcome_ledger: dict[str, Any] | None = None,
    experiment_governor: dict[str, Any] | None = None,
    evidence_gate: dict[str, Any] | None = None,
) -> dict[str, Any]:
    outcomes_by_id = {
        str(row.get("experiment_id") or ""): row
        for row in (outcome_ledger or {}).get("outcomes") or []
    }
    governor_by_id = {
        str(row.get("experiment_id") or ""): row
        for row in (experiment_governor or {}).get("decisions") or []
    }
    graduates = []
    for exp in (registry or {}).get("experiments") or []:
        exp_id = str(exp.get("experiment_id") or "")
        outcome = outcomes_by_id.get(exp_id, {})
        decision = governor_by_id.get(exp_id, {})
        verdict = str(exp.get("verdict") or outcome.get("verdict") or outcome.get("conclusion") or "")
        stage = "idea"
        next_action = "probe"
        if exp.get("status") in {"planned", "active"}:
            stage = "probe"
        if decision.get("decision") in {"expand", "continue"}:
            stage = "confirmed"
            next_action = "scale_probe"
        if decision.get("decision") == "expand" and (evidence_gate or {}).get("scale_allowed"):
            stage = "scaled"
            next_action = "promote_field_rule_if_repeatable"
        if "win" in verdict or "scale" in verdict:
            stage = "field_rule"
            next_action = "monitor_decay"
        if decision.get("decision") in {"stop_or_pivot", "pause_for_revalidation"} or "reject" in verdict:
            stage = "retired" if decision.get("decision") == "stop_or_pivot" else "probe"
            next_action = "retire_or_revalidate"
        graduates.append({
            "experiment_id": exp_id,
            "kind": exp.get("kind"),
            "hypothesis": exp.get("hypothesis"),
            "stage": stage,
            "next_action": next_action,
            "evidence": {"registry": exp, "outcome": outcome, "governor": decision},
        })
    if not graduates:
        graduates.append({
            "experiment_id": "bootstrap",
            "stage": "idea",
            "next_action": "create_probe_from_learning_market",
            "evidence": {},
        })
    stage_counts = Counter(row["stage"] for row in graduates)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Graduates experiments through idea, probe, confirmed, scaled, field_rule, and retired stages.",
        "experiments": graduates,
        "stage_counts": dict(stage_counts),
    }


def candidate_genealogy_diff_engine(
    rows: list[dict[str, Any]],
    candidate_lineage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    diffs = []
    by_route: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows[:100]:
        by_route[route_key_from_row(row)].append(row)
    for route, group in by_route.items():
        ranked = sorted(group, key=lambda row: pnl(row), reverse=True)
        for parent, child in zip(ranked[1:6], ranked[:5]):
            parent_weights = _row_numeric_weights(parent)
            child_weights = _row_numeric_weights(child)
            changes = []
            for key in sorted(set(parent_weights) | set(child_weights)):
                delta = child_weights.get(key, 0.0) - parent_weights.get(key, 0.0)
                if abs(delta) < 1e-9:
                    continue
                changes.append({
                    "parameter": key,
                    "from": round(parent_weights.get(key, 0.0), 6),
                    "to": round(child_weights.get(key, 0.0), 6),
                    "summary": f"{key}: {round(parent_weights.get(key, 0.0), 6)} -> {round(child_weights.get(key, 0.0), 6)}",
                })
            diffs.append({
                "diff_id": tournament_safety.stable_json_hash({"parent": parent.get("variant"), "child": child.get("variant")}, length=20),
                "route_key": route,
                "parent_variant": parent.get("variant"),
                "child_variant": child.get("variant"),
                "pnl_delta": round(pnl(child) - pnl(parent), 4),
                "expected_promotable_delta": round(float(child.get("expected_promotable_pnl") or pnl(child)) - float(parent.get("expected_promotable_pnl") or pnl(parent)), 4),
                "changes": changes[:12],
                "human_summary": "; ".join(change["summary"] for change in changes[:4]) or "No numeric weight changes detected",
            })
    diffs.sort(key=lambda row: float(row.get("expected_promotable_delta") or row.get("pnl_delta") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Explains parent/child/sibling candidate differences in human-readable terms.",
        "diffs": diffs[:100],
        "lineage_edge_count": len((candidate_lineage or {}).get("edges") or []),
    }


def off_policy_hunt_evaluator(
    rows: list[dict[str, Any]],
    policy_mutations: dict[str, Any] | None = None,
    survivor_model: dict[str, Any] | None = None,
    negative_bank: dict[str, Any] | None = None,
) -> dict[str, Any]:
    avoid_routes = set((negative_bank or {}).get("avoid_routes") or [])
    survivor = {
        str(row.get("variant")): float(row.get("survival_probability") or 0.55)
        for row in (survivor_model or {}).get("predictions") or []
    }
    evaluations = []
    for policy in (policy_mutations or {}).get("policies") or []:
        scored = []
        weights = policy.get("mutation_weights") if isinstance(policy.get("mutation_weights"), dict) else {}
        for row in rows[:100]:
            route = route_key_from_row(row)
            if route in avoid_routes:
                continue
            score = float(row.get("expected_promotable_pnl") or pnl(row)) * survivor.get(str(row.get("variant")), 0.55)
            score += float(row.get("novelty_score") or 0.0) * float(weights.get("explore") or 1.0)
            score -= float(row.get("overfit_risk_score") or 0.0) * float(weights.get("ood_penalty") or 1.0)
            scored.append((score, row))
        scored.sort(key=lambda item: item[0], reverse=True)
        top = [row for _, row in scored[:10]]
        evaluations.append({
            "policy_name": policy.get("policy_name"),
            "off_policy_expected_survivor_pnl": round(sum(float(row.get("expected_promotable_pnl") or pnl(row)) * survivor.get(str(row.get("variant")), 0.55) for row in top), 4),
            "top_variants": [row.get("variant") for row in top[:5]],
            "eligible_candidates": len(scored),
        })
    evaluations.sort(key=lambda row: float(row.get("off_policy_expected_survivor_pnl") or 0.0), reverse=True)
    baseline = evaluations[0]["off_policy_expected_survivor_pnl"] if evaluations else 0.0
    for row in evaluations:
        row["lift_vs_best"] = round(float(row.get("off_policy_expected_survivor_pnl") or 0.0) - baseline, 4)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Evaluates how today's policies would have ranked available candidates off-policy.",
        "evaluations": evaluations,
        "recommended_policy": (evaluations[0] if evaluations else {}).get("policy_name"),
    }


def self_competition_league(
    policy_tournament_payload: dict[str, Any] | None = None,
    off_policy: dict[str, Any] | None = None,
    hunt_replay: dict[str, Any] | None = None,
    treatment_effects: dict[str, Any] | None = None,
    route_states: dict[str, Any] | None = None,
) -> dict[str, Any]:
    entrants = []
    for idx, row in enumerate((policy_tournament_payload or {}).get("results") or []):
        score = float(row.get("score") or 0.0)
        entrants.append({
            "participant_key": f"policy:{row.get('policy_name')}",
            "participant_type": "policy",
            "name": row.get("policy_name"),
            "rating": round(1500.0 + score * 2.0 - idx * 8.0, 4),
            "score": round(score, 4),
            "evidence": "policy_tournament",
        })
    for row in (off_policy or {}).get("evaluations") or []:
        entrants.append({
            "participant_key": f"policy:{row.get('policy_name')}",
            "participant_type": "policy",
            "name": row.get("policy_name"),
            "rating": round(1500.0 + float(row.get("off_policy_expected_survivor_pnl") or 0.0) * 0.02, 4),
            "score": row.get("off_policy_expected_survivor_pnl"),
            "evidence": "off_policy",
        })
    replay_policy = (hunt_replay or {}).get("recommended_policy")
    if replay_policy:
        entrants.append({
            "participant_key": f"policy:{replay_policy}",
            "participant_type": "policy",
            "name": replay_policy,
            "rating": 1540.0,
            "score": 40.0,
            "evidence": "hunt_replay",
        })
    for row in (treatment_effects or {}).get("treatments") or []:
        entrants.append({
            "participant_key": f"treatment:{row.get('treatment')}",
            "participant_type": "treatment",
            "name": row.get("treatment"),
            "rating": round(1450.0 + float(row.get("evidence_score") or 0.0) * 4.0 - float(row.get("reject_rate") or 0.0) * 80.0, 4),
            "score": row.get("evidence_score"),
            "evidence": "treatment_effects",
        })
    for row in (route_states or {}).get("states") or []:
        entrants.append({
            "participant_key": f"route:{row.get('route_key')}",
            "participant_type": "route",
            "name": row.get("route_key"),
            "rating": round(1450.0 + float(row.get("learning_score") or 0.0) + float(row.get("freshness_score") or 0.0) * 0.5, 4),
            "score": row.get("learning_score"),
            "evidence": f"route_state:{row.get('state')}",
        })
    by_key: dict[str, dict[str, Any]] = {}
    for entry in entrants:
        key = str(entry.get("participant_key"))
        if key not in by_key or float(entry.get("rating") or 0.0) > float(by_key[key].get("rating") or 0.0):
            by_key[key] = entry
    leaderboard = sorted(by_key.values(), key=lambda row: float(row.get("rating") or 0.0), reverse=True)
    for idx, row in enumerate(leaderboard, 1):
        row["rank"] = idx
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Self-competition league with ELO-like ratings for policies, treatments, and routes.",
        "leaderboard": leaderboard[:100],
        "champion": leaderboard[0] if leaderboard else {},
    }


def evidence_contract_engine(
    field_manual: dict[str, Any] | None = None,
    experiment_graduation: dict[str, Any] | None = None,
    route_states: dict[str, Any] | None = None,
    league: dict[str, Any] | None = None,
    half_life: dict[str, Any] | None = None,
) -> dict[str, Any]:
    contracts = []
    freshness_by_route = {
        str(row.get("route_key")): float(row.get("freshness_score") or 0.0)
        for row in (half_life or {}).get("routes") or []
    }
    for idx, rule in enumerate((field_manual or {}).get("rules") or []):
        evidence = rule.get("evidence") if isinstance(rule.get("evidence"), dict) else {}
        route = str(evidence.get("route_key") or "")
        contract_id = tournament_safety.stable_json_hash({"rule": rule.get("rule"), "idx": idx}, length=20)
        contracts.append({
            "contract_id": contract_id,
            "belief": rule.get("rule"),
            "belief_type": "field_rule",
            "status": rule.get("status") or "candidate_rule",
            "supporting_evidence": evidence or {"source": rule.get("evidence")},
            "falsifiers": [
                "promotion_reject_rate_above_50pct",
                "route_state_retired_or_stale",
                "false_lesson_risk_above_60",
            ],
            "scope": {
                "route_key": route,
                "regime": evidence.get("regime_key"),
                "treatment": evidence.get("treatment"),
            },
            "expires_when": "half_life_freshness_below_35_or_after_next_revalidation",
            "freshness_score": round(freshness_by_route.get(route, 100.0 if not route else 0.0), 4),
        })
    for exp in (experiment_graduation or {}).get("experiments") or []:
        contract_id = tournament_safety.stable_json_hash({"experiment": exp.get("experiment_id"), "stage": exp.get("stage")}, length=20)
        contracts.append({
            "contract_id": contract_id,
            "belief": exp.get("hypothesis") or exp.get("experiment_id"),
            "belief_type": "experiment",
            "status": exp.get("stage"),
            "supporting_evidence": exp.get("evidence") or {},
            "falsifiers": ["governor_stop_or_pivot", "no_live_beaters_in_retest", "survivor_probability_below_50pct"],
            "scope": {"experiment_id": exp.get("experiment_id"), "kind": exp.get("kind")},
            "expires_when": "stage_changes_or_next_outcome_ledger_update",
        })
    for route in (route_states or {}).get("states") or []:
        if route.get("state") not in {"scaling", "promising", "revalidation"}:
            continue
        contract_id = tournament_safety.stable_json_hash({"route": route.get("route_key"), "state": route.get("state")}, length=20)
        contracts.append({
            "contract_id": contract_id,
            "belief": f"Route {route.get('route_key')} is {route.get('state')}",
            "belief_type": "route_state",
            "status": route.get("state"),
            "supporting_evidence": route,
            "falsifiers": ["freshness_below_35", "negative_knowledge_hard_avoid", "promotion_reject_cluster"],
            "scope": {"route_key": route.get("route_key")},
            "expires_when": "route_state_machine_changes",
            "freshness_score": route.get("freshness_score"),
        })
    champion = (league or {}).get("champion") or {}
    if champion:
        contracts.append({
            "contract_id": tournament_safety.stable_json_hash({"league_champion": champion.get("participant_key")}, length=20),
            "belief": f"{champion.get('name')} is current league champion",
            "belief_type": "league_rating",
            "status": "active",
            "supporting_evidence": champion,
            "falsifiers": ["off_policy_underperforms", "policy_drift_status_drifting"],
            "scope": {"participant_type": champion.get("participant_type"), "name": champion.get("name")},
            "expires_when": "next_policy_tournament_or_off_policy_evaluation",
        })
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Evidence contracts declare support, falsifiers, scope, and expiry for learned rules and beliefs.",
        "contracts": contracts[:200],
        "contract_count": len(contracts),
    }


def live_beater_quality_decomposer(
    rows: list[dict[str, Any]],
    survivor_model: dict[str, Any] | None = None,
    route_states: dict[str, Any] | None = None,
    regime_learning: dict[str, Any] | None = None,
    half_life: dict[str, Any] | None = None,
) -> dict[str, Any]:
    survivor = {
        str(row.get("variant")): float(row.get("survival_probability") or 0.55)
        for row in (survivor_model or {}).get("predictions") or []
    }
    route_state = {str(row.get("route_key")): row for row in (route_states or {}).get("states") or []}
    freshness = {
        str(row.get("route_key")): float(row.get("freshness_score") or 0.0)
        for row in (half_life or {}).get("routes") or []
    }
    regime_keys = {str(row.get("regime_key") or "") for row in (regime_learning or {}).get("regimes") or []}
    decomposed = []
    for idx, row in enumerate(rows[:100], 1):
        route = route_key_from_row(row)
        robust = row.get("robustness") if isinstance(row.get("robustness"), dict) else {}
        raw_component = pnl(row) * 0.01
        robustness_component = float(robust.get("adjusted_score") or robust.get("score") or row.get("promotion_quality_score") or 0.0) * 0.30
        novelty_component = float(row.get("novelty_score") or 0.0) * 0.12
        survivor_component = survivor.get(str(row.get("variant")), 0.55) * 55.0
        route_component = float((route_state.get(route) or {}).get("learning_score") or 0.0) * 0.25
        regime_component = 8.0 if any(route in key for key in regime_keys) else 0.0
        freshness_penalty = max(0.0, 45.0 - freshness.get(route, 80.0)) * 0.35
        risk_discount = float(row.get("overfit_risk_score") or 0.0) * 0.8
        quality_score = raw_component + robustness_component + novelty_component + survivor_component + route_component + regime_component - freshness_penalty - risk_discount
        decomposed.append({
            "rank": idx,
            "variant": row.get("variant"),
            "route_key": route,
            "quality_score": round(quality_score, 4),
            "components": {
                "raw_pnl": round(raw_component, 4),
                "robustness": round(robustness_component, 4),
                "novelty": round(novelty_component, 4),
                "survivor_probability": round(survivor_component, 4),
                "route_strength": round(route_component, 4),
                "regime_fit": round(regime_component, 4),
                "freshness_penalty": round(-freshness_penalty, 4),
                "risk_discount": round(-risk_discount, 4),
            },
        })
    decomposed.sort(key=lambda row: float(row.get("quality_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Decomposes each live-beating candidate into source-of-edge quality components.",
        "candidates": decomposed,
        "top_quality": decomposed[0] if decomposed else {},
    }


def contradiction_detector(
    route_states: dict[str, Any] | None = None,
    false_lessons: dict[str, Any] | None = None,
    survivor_model: dict[str, Any] | None = None,
    uncertainty_budget: dict[str, Any] | None = None,
    evidence_contracts: dict[str, Any] | None = None,
    negative_bank: dict[str, Any] | None = None,
    policy_drift: dict[str, Any] | None = None,
) -> dict[str, Any]:
    contradictions = []
    false_by_route: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in (false_lessons or {}).get("lessons") or []:
        false_by_route[str(row.get("route_key") or "")].append(row)
    survivor_by_route: dict[str, list[float]] = defaultdict(list)
    for row in (survivor_model or {}).get("predictions") or []:
        survivor_by_route[str(row.get("route_key") or "")].append(float(row.get("survival_probability") or 0.0))
    uncertain_by_route = {
        str(row.get("route_key")): row
        for row in (uncertainty_budget or {}).get("budgets") or []
        if row.get("route_key")
    }
    avoid_routes = set((negative_bank or {}).get("avoid_routes") or [])
    for route in (route_states or {}).get("states") or []:
        route_key = str(route.get("route_key") or "")
        state = str(route.get("state") or "")
        avg_survivor = _avg(survivor_by_route.get(route_key) or [])
        if state in {"scaling", "promising"} and false_by_route.get(route_key):
            contradictions.append({
                "kind": "scale_vs_false_lesson",
                "route_key": route_key,
                "severity": "high" if max(float(item.get("false_lesson_risk_score") or 0.0) for item in false_by_route[route_key]) >= 50.0 else "medium",
                "scale_signal": route,
                "caution_signal": false_by_route[route_key][0],
            })
        if state in {"scaling", "promising"} and avg_survivor and avg_survivor < 0.50:
            contradictions.append({
                "kind": "scale_vs_survivor_reject_risk",
                "route_key": route_key,
                "severity": "high",
                "scale_signal": route,
                "avg_survivor_probability": round(avg_survivor, 4),
            })
        if state in {"scaling", "promising"} and route_key in avoid_routes:
            contradictions.append({
                "kind": "scale_vs_negative_knowledge",
                "route_key": route_key,
                "severity": "high",
                "scale_signal": route,
                "negative_signal": "hard_avoid",
            })
        budget = uncertain_by_route.get(route_key)
        if state == "scaling" and budget and budget.get("recommended_budget") == "learn":
            contradictions.append({
                "kind": "scale_vs_uncertainty",
                "route_key": route_key,
                "severity": "medium",
                "scale_signal": route,
                "uncertainty_signal": budget,
            })
    if (policy_drift or {}).get("status") == "drifting":
        for contract in (evidence_contracts or {}).get("contracts") or []:
            if contract.get("belief_type") == "league_rating":
                contradictions.append({
                    "kind": "league_champion_vs_policy_drift",
                    "severity": "high",
                    "contract_id": contract.get("contract_id"),
                    "drift_reasons": (policy_drift or {}).get("reasons") or [],
                })
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Finds disagreements between scale, caution, survivor, uncertainty, negative memory, and policy drift signals.",
        "contradictions": contradictions,
        "contradiction_count": len(contradictions),
        "highest_severity": "high" if any(row.get("severity") == "high" for row in contradictions) else ("medium" if contradictions else "none"),
    }


def learning_conflict_resolver(
    contradictions: dict[str, Any] | None = None,
    route_states: dict[str, Any] | None = None,
    resurrection: dict[str, Any] | None = None,
    revalidation: dict[str, Any] | None = None,
    uncertainty_budget: dict[str, Any] | None = None,
) -> dict[str, Any]:
    actions = []
    routes_with_conflicts: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in (contradictions or {}).get("contradictions") or []:
        routes_with_conflicts[str(item.get("route_key") or item.get("contract_id") or "policy")].append(item)
    revalidation_routes = {
        str(row.get("route_key") or "")
        for row in ((revalidation or {}).get("queue") or (revalidation or {}).get("tasks") or [])
        if row.get("route_key")
    }
    repair_routes = {
        str(row.get("route_key") or "")
        for row in (resurrection or {}).get("repair_queue") or []
        if row.get("route_key")
    }
    uncertainty_by_route = {
        str(row.get("route_key") or ""): row
        for row in (uncertainty_budget or {}).get("budgets") or []
        if row.get("route_key")
    }
    for state in (route_states or {}).get("states") or []:
        route = str(state.get("route_key") or "")
        conflicts = routes_with_conflicts.get(route, [])
        action = "probe"
        rationale = ["default probe until stronger evidence"]
        if conflicts:
            if any(item.get("severity") == "high" for item in conflicts):
                action = "quarantine"
                rationale = ["high-severity contradiction present"]
            else:
                action = "revalidate"
                rationale = ["medium contradiction requires revalidation"]
        elif route in repair_routes:
            action = "repair"
            rationale = ["resurrection engine found repairable candidate DNA"]
        elif route in revalidation_routes:
            action = "revalidate"
            rationale = ["route is scheduled for revalidation"]
        elif state.get("state") == "scaling":
            action = "scale"
            rationale = ["route state is scaling with no active contradiction"]
        elif state.get("state") == "retired":
            action = "retire"
            rationale = ["route state retired"]
        elif (uncertainty_by_route.get(route) or {}).get("recommended_budget") == "learn":
            action = "probe"
            rationale = ["uncertainty budget says learning value remains high"]
        actions.append({
            "route_key": route,
            "state": state.get("state"),
            "final_action": action,
            "rationale": rationale,
            "conflicts": conflicts[:5],
            "priority_score": round(
                (90.0 if action == "quarantine" else 75.0 if action in {"revalidate", "repair"} else 65.0 if action == "scale" else 45.0)
                + float((uncertainty_by_route.get(route) or {}).get("budget_score") or 0.0) * 0.1,
                4,
            ),
        })
    if routes_with_conflicts.get("policy"):
        actions.append({
            "route_key": "",
            "state": "policy",
            "final_action": "revalidate",
            "rationale": ["policy-level contradiction detected"],
            "conflicts": routes_with_conflicts.get("policy") or [],
            "priority_score": 88.0,
        })
    actions.sort(key=lambda row: float(row.get("priority_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Resolves conflicting learning signals into final hunt actions: scale, probe, repair, revalidate, quarantine, or retire.",
        "actions": actions[:100],
        "top_action": actions[0] if actions else {},
    }


def cohort_based_memory(
    rows: list[dict[str, Any]],
    survivor_model: dict[str, Any] | None = None,
    failure_memory: dict[str, Any] | None = None,
) -> dict[str, Any]:
    survivor = {
        str(row.get("variant")): float(row.get("survival_probability") or 0.55)
        for row in (survivor_model or {}).get("predictions") or []
    }
    reject_by_route = {
        str(row.get("route_key") or ""): row
        for row in (failure_memory or {}).get("directives") or []
    }
    cohorts: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "count": 0,
        "pnl_sum": 0.0,
        "expected_sum": 0.0,
        "survivor_sum": 0.0,
        "variants": [],
    })
    for row in rows[:200]:
        route = route_key_from_row(row)
        treatment = _row_treatment(row)
        tags = row.get("learning_tags") or []
        failure_mode = "risk_clean"
        for tag in tags:
            if tag in {"thin_holdout_edge", "weak_day_consistency", "ticker_concentrated", "side_concentrated", "high_overfit_risk"}:
                failure_mode = str(tag)
                break
        key = f"{route}::{treatment}::{failure_mode}"
        cohort = cohorts[key]
        cohort["count"] += 1
        cohort["pnl_sum"] += pnl(row)
        cohort["expected_sum"] += float(row.get("expected_promotable_pnl") or pnl(row))
        cohort["survivor_sum"] += survivor.get(str(row.get("variant")), 0.55)
        cohort["variants"].append(row.get("variant"))
        cohort["route_key"] = route
        cohort["treatment"] = treatment
        cohort["failure_mode"] = failure_mode
        cohort["reject_memory"] = reject_by_route.get(route) or {}
    out = []
    for key, cohort in cohorts.items():
        count = int(cohort["count"] or 0)
        out.append({
            "cohort_key": key,
            "route_key": cohort.get("route_key"),
            "treatment": cohort.get("treatment"),
            "failure_mode": cohort.get("failure_mode"),
            "candidate_count": count,
            "avg_pnl": round(float(cohort["pnl_sum"]) / max(1, count), 4),
            "avg_expected_promotable_pnl": round(float(cohort["expected_sum"]) / max(1, count), 4),
            "avg_survivor_probability": round(float(cohort["survivor_sum"]) / max(1, count), 4),
            "variants": cohort["variants"][:10],
            "reject_memory": cohort.get("reject_memory") or {},
        })
    out.sort(key=lambda row: (float(row.get("avg_expected_promotable_pnl") or 0.0), float(row.get("avg_survivor_probability") or 0.0)), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Cohort memory groups candidates by route, treatment, and failure mode so lessons are learned at cohort level.",
        "cohorts": out[:150],
        "top_cohort": out[0] if out else {},
    }


def adaptive_hunt_throttle(
    cycles: list[dict[str, Any]] | None = None,
    policy_drift: dict[str, Any] | None = None,
    contradictions: dict[str, Any] | None = None,
    uncertainty_budget: dict[str, Any] | None = None,
    conflict_resolution: dict[str, Any] | None = None,
) -> dict[str, Any]:
    recent = (cycles or [])[-4:]
    scored = sum(_cycle_scored_total(cycle) for cycle in recent)
    winners = sum(_cycle_winners(cycle) for cycle in recent)
    yield_per_10k = winners / max(1, scored) * 10000.0
    contradiction_count = int((contradictions or {}).get("contradiction_count") or 0)
    drift_status = (policy_drift or {}).get("status") or "stable"
    top_uncertainty = float(((uncertainty_budget or {}).get("top_budget") or {}).get("uncertainty") or 0.0)
    top_action = ((conflict_resolution or {}).get("top_action") or {}).get("final_action")
    batch_multiplier = 1.0
    mutation_width = "medium"
    route_breadth = "balanced"
    worker_split = "balanced"
    if contradiction_count >= 3 or drift_status == "drifting" or top_action == "quarantine":
        batch_multiplier = 0.65
        mutation_width = "tight"
        route_breadth = "narrow_revalidation"
        worker_split = "2_revalidate_1_repair_1_explore"
    elif yield_per_10k >= 25.0 and contradiction_count == 0:
        batch_multiplier = 1.35
        mutation_width = "medium"
        route_breadth = "scale_promising_routes"
        worker_split = "2_scale_1_probe_1_repair"
    elif top_uncertainty >= 0.55:
        batch_multiplier = 1.0
        mutation_width = "wide"
        route_breadth = "uncertainty_probe"
        worker_split = "2_probe_1_revalidate_1_explore"
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Controls batch size, mutation width, route breadth, and worker split under yield/drift/uncertainty/contradiction pressure.",
        "controls": {
            "batch_size_multiplier": round(batch_multiplier, 4),
            "mutation_width": mutation_width,
            "route_breadth": route_breadth,
            "worker_split": worker_split,
        },
        "inputs": {
            "recent_yield_per_10k": round(yield_per_10k, 4),
            "contradiction_count": contradiction_count,
            "drift_status": drift_status,
            "top_uncertainty": round(top_uncertainty, 4),
            "top_conflict_action": top_action,
        },
    }


def promotion_readiness_simulator(
    rows: list[dict[str, Any]],
    survivor_model: dict[str, Any] | None = None,
    quality_decomposition: dict[str, Any] | None = None,
    contradictions: dict[str, Any] | None = None,
    false_lessons: dict[str, Any] | None = None,
) -> dict[str, Any]:
    survivor_by_variant = {
        str(row.get("variant")): row
        for row in (survivor_model or {}).get("predictions") or []
    }
    quality_by_variant = {
        str(row.get("variant")): row
        for row in (quality_decomposition or {}).get("candidates") or []
    }
    conflict_routes = {str(row.get("route_key") or "") for row in (contradictions or {}).get("contradictions") or [] if row.get("route_key")}
    false_by_route = {
        str(row.get("route_key") or ""): row
        for row in (false_lessons or {}).get("lessons") or []
    }
    simulations = []
    for row in rows[:50]:
        variant = str(row.get("variant") or "")
        route = route_key_from_row(row)
        survivor = survivor_by_variant.get(variant, {})
        quality = quality_by_variant.get(variant, {})
        reject_prob = float(survivor.get("predicted_reject_probability") or 0.35)
        reasons = list(survivor.get("failure_reasons") or [])
        if route in conflict_routes:
            reasons.append("learning_contradiction")
            reject_prob += 0.15
        if route in false_by_route:
            reasons.append("false_lesson_risk")
            reject_prob += 0.10
        reject_prob = min(0.98, reject_prob)
        repair = []
        if "thin_holdout_edge" in reasons or "no_holdout_credit" in reasons:
            repair.append("run_holdout_repair_experiment")
        if "weak_day_consistency" in reasons:
            repair.append("run_day_consistency_replay")
        if "learning_contradiction" in reasons:
            repair.append("resolve_learning_conflict_before_review")
        if "false_lesson_risk" in reasons:
            repair.append("add_controlled_retest_sample")
        simulations.append({
            "variant": row.get("variant"),
            "route_key": route,
            "simulated_decision": "likely_reject" if reject_prob >= 0.50 else "likely_approve",
            "predicted_reject_probability": round(reject_prob, 4),
            "likely_reject_reasons": reasons[:8],
            "repair_experiments": repair or ["standard_promotion_packet"],
            "quality_score": quality.get("quality_score"),
        })
    simulations.sort(key=lambda row: float(row.get("predicted_reject_probability") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Simulates promotion review outcomes and recommends exact repair experiments before review.",
        "simulations": simulations,
        "highest_reject_risk": simulations[0] if simulations else {},
    }


def research_trace_ledger(
    *,
    evidence_contracts: dict[str, Any] | None = None,
    quality_decomposition: dict[str, Any] | None = None,
    contradictions: dict[str, Any] | None = None,
    conflict_resolution: dict[str, Any] | None = None,
    cohort_memory: dict[str, Any] | None = None,
    throttle: dict[str, Any] | None = None,
    promotion_simulator: dict[str, Any] | None = None,
    league: dict[str, Any] | None = None,
) -> dict[str, Any]:
    entries = []

    def add(kind: str, subject: str, decision: str, evidence: Any, confidence: float = 0.5) -> None:
        payload = {"kind": kind, "subject": subject, "decision": decision, "evidence": evidence, "confidence": round(confidence, 4)}
        payload["trace_id"] = tournament_safety.stable_json_hash(payload, length=24)
        entries.append(payload)

    for contract in (evidence_contracts or {}).get("contracts") or []:
        add("evidence_contract", str(contract.get("belief") or contract.get("contract_id")), str(contract.get("status") or "active"), contract, 0.70)
    top_quality = (quality_decomposition or {}).get("top_quality") or {}
    if top_quality:
        add("quality_decomposition", str(top_quality.get("variant")), "rank_quality", top_quality, 0.65)
    for item in (contradictions or {}).get("contradictions") or []:
        add("contradiction", str(item.get("route_key") or item.get("contract_id")), str(item.get("kind")), item, 0.80 if item.get("severity") == "high" else 0.60)
    for action in (conflict_resolution or {}).get("actions") or []:
        add("resolved_action", str(action.get("route_key") or "policy"), str(action.get("final_action")), action, 0.75)
    top_cohort = (cohort_memory or {}).get("top_cohort") or {}
    if top_cohort:
        add("cohort_memory", str(top_cohort.get("cohort_key")), "cohort_ranked", top_cohort, 0.60)
    if throttle:
        add("hunt_throttle", "runtime_controls", str(((throttle or {}).get("controls") or {}).get("route_breadth")), throttle, 0.70)
    highest_risk = (promotion_simulator or {}).get("highest_reject_risk") or {}
    if highest_risk:
        add("promotion_readiness_simulation", str(highest_risk.get("variant")), str(highest_risk.get("simulated_decision")), highest_risk, 0.65)
    champion = (league or {}).get("champion") or {}
    if champion:
        add("self_competition", str(champion.get("participant_key")), "league_champion", champion, 0.65)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Durable trace ledger explaining why the system believes each rule, action, route state, and policy rating.",
        "entries": entries[:300],
        "entry_count": len(entries),
    }


def runtime_decision_kernel(
    conflict_resolution: dict[str, Any] | None = None,
    throttle: dict[str, Any] | None = None,
    route_states: dict[str, Any] | None = None,
    negative_bank: dict[str, Any] | None = None,
    mutation_grammar: dict[str, Any] | None = None,
    revalidation: dict[str, Any] | None = None,
    resurrection: dict[str, Any] | None = None,
    worker_rebalance: dict[str, Any] | None = None,
) -> dict[str, Any]:
    controls = (throttle or {}).get("controls") or {}
    actions = list((conflict_resolution or {}).get("actions") or [])
    focus_routes = []
    avoid_routes = set(str(route) for route in (negative_bank or {}).get("avoid_routes") or [] if route)
    repair_queue = list((resurrection or {}).get("repair_queue") or [])[:12]
    revalidation_queue = list((revalidation or {}).get("queue") or (revalidation or {}).get("tasks") or [])[:12]
    for action in actions:
        route = str(action.get("route_key") or "")
        if route and route not in avoid_routes and action.get("final_action") in {"scale", "probe", "repair", "revalidate"}:
            focus_routes.append(route)
    if not focus_routes:
        focus_routes = [
            str(route)
            for route in (route_states or {}).get("focus_routes") or []
            if route and str(route) not in avoid_routes
        ]
    commands = []
    templates = list((mutation_grammar or {}).get("top_templates") or (mutation_grammar or {}).get("grammar") or [])
    for idx, action in enumerate(actions[:12]):
        route = str(action.get("route_key") or "")
        if not route:
            continue
        final_action = str(action.get("final_action") or "probe")
        command = {
            "command_id": tournament_safety.stable_json_hash({"idx": idx, "route": route, "action": final_action}, length=20),
            "action": final_action,
            "route_key": route,
            "mutation_width": controls.get("mutation_width") or "medium",
            "batch_size_multiplier": controls.get("batch_size_multiplier") or 1.0,
            "reason": "; ".join(action.get("rationale") or []),
            "priority_score": action.get("priority_score"),
        }
        template = next((row for row in templates if row.get("route_key") == route), {})
        if template:
            command["mutation_template"] = template.get("template")
            command["parameter_biases"] = template.get("parameter_biases") or {}
        commands.append(command)
    if repair_queue:
        item = repair_queue[0]
        commands.append({
            "command_id": tournament_safety.stable_json_hash({"repair": item}, length=20),
            "action": "repair",
            "route_key": item.get("route_key"),
            "variant": item.get("variant"),
            "repair_treatments": item.get("repair_treatments") or [],
            "priority_score": item.get("priority_score"),
            "reason": "resurrection_engine_top_repair",
        })
    if revalidation_queue:
        item = revalidation_queue[0]
        commands.append({
            "command_id": tournament_safety.stable_json_hash({"revalidate": item}, length=20),
            "action": "revalidate",
            "route_key": item.get("route_key"),
            "priority_score": item.get("priority_score"),
            "reason": item.get("reason") or item.get("task"),
        })
    packet = {
        "live_only": True,
        "max_workers": 4,
        "focus_routes": sorted(set(focus_routes))[:12],
        "avoid_routes": sorted(avoid_routes),
        "mutation_width": controls.get("mutation_width") or "medium",
        "route_breadth": controls.get("route_breadth") or "balanced",
        "batch_size_multiplier": controls.get("batch_size_multiplier") or 1.0,
        "worker_roles": (worker_rebalance or {}).get("assignments") or [],
        "repair_queue": repair_queue,
        "revalidation_queue": revalidation_queue,
        "commands": commands[:20],
    }
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Canonical per-cycle command packet derived from learning, conflict resolution, throttle, repairs, and guardrails.",
        "command_packet": packet,
        "commands": packet["commands"],
    }


def action_outcome_tracker(
    cycles: list[dict[str, Any]] | None = None,
    runtime_kernel: dict[str, Any] | None = None,
    rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    cycles = cycles or []
    rows = rows or []
    recent = cycles[-3:] if len(cycles) > 3 else cycles
    scored = sum(_cycle_scored_total(cycle) for cycle in recent)
    winners = sum(_cycle_winners(cycle) for cycle in recent)
    winner_yield = winners / max(1, scored) * 10000.0
    best_expected = max([float(row.get("expected_promotable_pnl") or pnl(row)) for row in rows[:20]] or [0.0])
    outcomes = []
    for command in (runtime_kernel or {}).get("commands") or []:
        action = str(command.get("action") or "probe")
        route = str(command.get("route_key") or "")
        route_rows = [row for row in rows if route and route_key_from_row(row) == route]
        route_best = max([float(row.get("expected_promotable_pnl") or pnl(row)) for row in route_rows[:20]] or [0.0])
        reward = route_best * 0.05 + winner_yield
        if action == "quarantine":
            reward = 5.0 if not route_rows else -5.0
        elif action == "repair":
            reward += 8.0 if route_rows else 2.0
        elif action == "revalidate":
            reward += 4.0
        outcomes.append({
            "command_id": command.get("command_id"),
            "action": action,
            "route_key": route,
            "recent_scored": int(scored),
            "recent_winners": int(winners),
            "winner_yield_per_10k": round(winner_yield, 4),
            "route_best_expected_promotable_pnl": round(route_best, 4),
            "global_best_expected_promotable_pnl": round(best_expected, 4),
            "reward_score": round(reward, 4),
            "worked": reward > 5.0,
        })
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Tracks whether issued runtime commands appear to improve yield, survivor P/L, or avoidance outcomes.",
        "outcomes": outcomes,
        "recent_winner_yield_per_10k": round(winner_yield, 4),
    }


def closed_loop_reward_model(
    action_outcomes: dict[str, Any] | None = None,
    cohort_memory: dict[str, Any] | None = None,
    route_states: dict[str, Any] | None = None,
) -> dict[str, Any]:
    by_action: dict[str, dict[str, float]] = defaultdict(lambda: {"count": 0.0, "reward": 0.0, "worked": 0.0})
    for row in (action_outcomes or {}).get("outcomes") or []:
        item = by_action[str(row.get("action") or "unknown")]
        item["count"] += 1.0
        item["reward"] += float(row.get("reward_score") or 0.0)
        item["worked"] += 1.0 if row.get("worked") else 0.0
    route_state_bonus = {
        str(row.get("route_key")): {"scaling": 1.2, "promising": 1.1, "revalidation": 0.95, "retired": 0.5}.get(str(row.get("state")), 1.0)
        for row in (route_states or {}).get("states") or []
    }
    cohort_bonus = max([float(row.get("avg_expected_promotable_pnl") or 0.0) for row in (cohort_memory or {}).get("cohorts") or []] or [0.0]) * 0.02
    rewards = []
    for action, item in by_action.items():
        avg_reward = item["reward"] / max(1.0, item["count"])
        success = item["worked"] / max(1.0, item["count"])
        rewards.append({
            "action": action,
            "sample_count": int(item["count"]),
            "avg_reward_score": round(avg_reward + cohort_bonus, 4),
            "success_rate": round(success, 4),
            "recommended_use": "scale" if avg_reward > 10.0 and success >= 0.5 else "probe",
        })
    if not rewards:
        for action in ["scale", "probe", "repair", "revalidate", "quarantine"]:
            rewards.append({
                "action": action,
                "sample_count": 0,
                "avg_reward_score": round(cohort_bonus * (1.0 if action != "quarantine" else 0.5), 4),
                "success_rate": 0.0,
                "recommended_use": "probe",
            })
    rewards.sort(key=lambda row: float(row.get("avg_reward_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Learns which runtime actions produce better outcomes under current route/cohort conditions.",
        "action_rewards": rewards,
        "best_action": rewards[0] if rewards else {},
        "route_state_bonus": route_state_bonus,
    }


def autonomous_hunt_planner(
    runtime_kernel: dict[str, Any] | None = None,
    reward_model: dict[str, Any] | None = None,
    uncertainty_budget: dict[str, Any] | None = None,
    worker_rebalance: dict[str, Any] | None = None,
) -> dict[str, Any]:
    packet = (runtime_kernel or {}).get("command_packet") or {}
    reward_by_action = {
        str(row.get("action")): row
        for row in (reward_model or {}).get("action_rewards") or []
    }
    uncertainty_by_route = {
        str(row.get("route_key")): row
        for row in (uncertainty_budget or {}).get("budgets") or []
        if row.get("route_key")
    }
    jobs = []
    workers = (worker_rebalance or {}).get("assignments") or packet.get("worker_roles") or []
    worker_names = [row.get("worker") for row in workers[:4]] or [f"worker_{idx + 1}" for idx in range(4)]
    for idx, command in enumerate(packet.get("commands") or []):
        action = str(command.get("action") or "probe")
        route = str(command.get("route_key") or "")
        reward = reward_by_action.get(action, {})
        uncertainty = uncertainty_by_route.get(route, {})
        jobs.append({
            "job_id": tournament_safety.stable_json_hash({"idx": idx, "command": command}, length=20),
            "worker": worker_names[idx % max(1, len(worker_names))],
            "action": action,
            "route_key": route,
            "mutation_width": command.get("mutation_width") or packet.get("mutation_width"),
            "batch_size_multiplier": command.get("batch_size_multiplier") or packet.get("batch_size_multiplier"),
            "priority_score": round(float(command.get("priority_score") or 50.0) + float(reward.get("avg_reward_score") or 0.0) + float(uncertainty.get("budget_score") or 0.0) * 0.1, 4),
            "rationale": command.get("reason") or reward.get("recommended_use") or "runtime_kernel_command",
        })
    jobs.sort(key=lambda row: float(row.get("priority_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Chooses exact worker jobs from command packet, reward model, uncertainty, and worker rebalance.",
        "jobs": jobs[:20],
        "next_job": jobs[0] if jobs else {},
    }


def runtime_guardrails(
    runtime_kernel: dict[str, Any] | None = None,
    negative_bank: dict[str, Any] | None = None,
    max_workers: int = 4,
) -> dict[str, Any]:
    packet = dict((runtime_kernel or {}).get("command_packet") or {})
    failures = []
    if packet.get("live_only") is not True:
        failures.append("live_only_not_enforced")
        packet["live_only"] = True
    if int(packet.get("max_workers") or 0) > max_workers:
        failures.append("max_workers_exceeded")
        packet["max_workers"] = max_workers
    avoid = set(str(route) for route in (negative_bank or {}).get("avoid_routes") or [] if route)
    packet["avoid_routes"] = sorted(set(packet.get("avoid_routes") or []) | avoid)
    focus = [route for route in packet.get("focus_routes") or [] if route not in avoid]
    if len(focus) < len(packet.get("focus_routes") or []):
        failures.append("focus_routes_removed_by_avoid_list")
    packet["focus_routes"] = focus[:12]
    guarded_commands = []
    for command in packet.get("commands") or []:
        if command.get("route_key") in avoid and command.get("action") not in {"quarantine", "repair"}:
            failures.append("blocked_command_on_avoid_route")
            continue
        guarded_commands.append(command)
    packet["commands"] = guarded_commands[:20]
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Hard guards for live-only, max workers, promotion safety, avoid/quarantine constraints.",
        "ok": not failures,
        "failures": failures,
        "guarded_command_packet": packet,
    }


def command_replay_ledger(
    runtime_kernel: dict[str, Any] | None = None,
    guardrails: dict[str, Any] | None = None,
    action_outcomes: dict[str, Any] | None = None,
    trace_ledger: dict[str, Any] | None = None,
) -> dict[str, Any]:
    outcomes_by_id = {
        str(row.get("command_id")): row
        for row in (action_outcomes or {}).get("outcomes") or []
    }
    trace_ids = [row.get("trace_id") for row in (trace_ledger or {}).get("entries") or []][:10]
    entries = []
    for command in ((guardrails or {}).get("guarded_command_packet") or (runtime_kernel or {}).get("command_packet") or {}).get("commands") or []:
        command_id = str(command.get("command_id") or tournament_safety.stable_json_hash(command, length=20))
        entries.append({
            "command_id": command_id,
            "action": command.get("action"),
            "route_key": command.get("route_key"),
            "issued_because": command.get("reason"),
            "guardrail_ok": (guardrails or {}).get("ok"),
            "outcome": outcomes_by_id.get(command_id) or {},
            "trace_ids": trace_ids,
        })
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Replayable ledger of commands, reasons, guardrail checks, and observed outcomes.",
        "entries": entries[:100],
        "entry_count": len(entries),
    }


def action_elo_league(
    action_outcomes: dict[str, Any] | None = None,
    reward_model: dict[str, Any] | None = None,
) -> dict[str, Any]:
    entrants: dict[str, dict[str, Any]] = {}
    for row in (reward_model or {}).get("action_rewards") or []:
        action = str(row.get("action") or "unknown")
        rating = 1500.0 + float(row.get("avg_reward_score") or 0.0) * 4.0 + float(row.get("success_rate") or 0.0) * 60.0
        entrants[action] = {
            "action": action,
            "rating": round(rating, 4),
            "sample_count": row.get("sample_count"),
            "success_rate": row.get("success_rate"),
            "evidence": "closed_loop_reward_model",
        }
    for row in (action_outcomes or {}).get("outcomes") or []:
        action = str(row.get("action") or "unknown")
        current = entrants.setdefault(action, {"action": action, "rating": 1500.0, "sample_count": 0, "success_rate": 0.0, "evidence": "action_outcome_tracker"})
        current["rating"] = round(float(current.get("rating") or 1500.0) + (16.0 if row.get("worked") else -12.0), 4)
        current["sample_count"] = int(current.get("sample_count") or 0) + 1
    leaderboard = sorted(entrants.values(), key=lambda row: float(row.get("rating") or 0.0), reverse=True)
    for idx, row in enumerate(leaderboard, 1):
        row["rank"] = idx
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "ELO-style league for runtime actions such as scale, probe, repair, revalidate, quarantine, and mutation widths.",
        "leaderboard": leaderboard,
        "champion": leaderboard[0] if leaderboard else {},
    }


def human_readable_hunt_brief(
    runtime_kernel: dict[str, Any] | None = None,
    planner: dict[str, Any] | None = None,
    guardrails: dict[str, Any] | None = None,
    conflict_resolution: dict[str, Any] | None = None,
    action_league: dict[str, Any] | None = None,
) -> dict[str, Any]:
    packet = (guardrails or {}).get("guarded_command_packet") or (runtime_kernel or {}).get("command_packet") or {}
    next_job = (planner or {}).get("next_job") or {}
    top_action = (conflict_resolution or {}).get("top_action") or {}
    champion = (action_league or {}).get("champion") or {}
    lines = [
        f"Next job: {next_job.get('action') or 'probe'} on {next_job.get('route_key') or 'best available route'}.",
        f"Mutation width: {packet.get('mutation_width') or 'medium'}; batch multiplier: {packet.get('batch_size_multiplier') or 1.0}.",
        f"Focus routes: {', '.join((packet.get('focus_routes') or [])[:4]) or 'none yet'}.",
        f"Guardrails: {'ok' if (guardrails or {}).get('ok', True) else 'adjusted'}; avoid routes: {len(packet.get('avoid_routes') or [])}.",
        f"Conflict action: {top_action.get('final_action') or 'none'}; action champion: {champion.get('action') or 'n/a'}.",
    ]
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Compact human-readable brief explaining what the next hunt cycle will do and why.",
        "brief": " ".join(lines),
        "lines": lines,
        "next_job": next_job,
    }


def ab_route_experiment_executor(
    experiment_plan: dict[str, Any] | None = None,
    outcome_ledger: dict[str, Any] | None = None,
    worker_rebalance: dict[str, Any] | None = None,
    runtime_kernel: dict[str, Any] | None = None,
    *,
    max_workers: int = 4,
) -> dict[str, Any]:
    outcomes = {
        str(row.get("experiment_id")): row
        for row in (outcome_ledger or {}).get("outcomes") or []
        if row.get("experiment_id")
    }
    workers = [row.get("worker") for row in (worker_rebalance or {}).get("assignments") or [] if row.get("worker")]
    if not workers:
        workers = [f"worker_{idx + 1}" for idx in range(max(1, min(4, int(max_workers))))]
    packet_routes = list(((runtime_kernel or {}).get("command_packet") or {}).get("focus_routes") or [])
    assignments = []
    for exp in (experiment_plan or {}).get("experiments") or []:
        route = str(exp.get("route_key") or "")
        if not route:
            continue
        treatments = [row for row in (exp.get("treatments") or []) if isinstance(row, dict)]
        if len(treatments) < 2:
            continue
        outcome = outcomes.get(str(exp.get("experiment_id"))) or {}
        if outcome.get("next_action") in {"downweight_or_change_treatment"}:
            continue
        arm_count = min(2, len(treatments))
        for idx, treatment in enumerate(treatments[:arm_count]):
            assignments.append({
                "assignment_id": tournament_safety.stable_json_hash({"exp": exp.get("experiment_id"), "arm": treatment.get("name"), "idx": idx}, length=20),
                "experiment_id": exp.get("experiment_id"),
                "kind": exp.get("kind"),
                "worker": workers[idx % len(workers)],
                "route_key": route,
                "family_key": exp.get("family_key"),
                "arm": "control" if idx == 0 else "treatment",
                "treatment": treatment.get("name") or treatment.get("mutation_lane") or f"arm_{idx + 1}",
                "mutation_lane": treatment.get("mutation_lane"),
                "mutation_scale_multiplier": treatment.get("mutation_scale_multiplier", 1.0),
                "budget_pct": round(float(treatment.get("budget_pct") or 100.0 / arm_count), 2),
                "success_metric": exp.get("success_metric"),
                "stop_rule": exp.get("stop_rule"),
                "status": "active",
            })
    if not assignments and packet_routes:
        for idx, route in enumerate(packet_routes[:2]):
            assignments.append({
                "assignment_id": tournament_safety.stable_json_hash({"runtime_route_ab": route, "idx": idx}, length=20),
                "experiment_id": "runtime_route_probe",
                "kind": "runtime_ab_route_probe",
                "worker": workers[idx % len(workers)],
                "route_key": route,
                "arm": "control" if idx == 0 else "treatment",
                "treatment": "narrow_mutation" if idx == 0 else "wide_mutation",
                "mutation_lane": "exploitation" if idx == 0 else "wild_shuffle",
                "mutation_scale_multiplier": 0.65 if idx == 0 else 1.25,
                "budget_pct": 50.0,
                "success_metric": "live-beating yield and promotion readiness",
                "stop_rule": "stop if arm yields no live beaters after enough scored samples",
                "status": "bootstrap",
            })
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Executable A/B route experiments that split workers across control/treatment arms during the hunt.",
        "assignment_count": len(assignments),
        "assignments": assignments[: max(2, int(max_workers)) * 4],
        "focus_routes": sorted({row.get("route_key") for row in assignments if row.get("route_key")})[:12],
    }


def champion_challenger_runtime_slots(
    runtime_kernel: dict[str, Any] | None = None,
    action_league: dict[str, Any] | None = None,
    ab_executor: dict[str, Any] | None = None,
    reward_model: dict[str, Any] | None = None,
) -> dict[str, Any]:
    packet = (runtime_kernel or {}).get("command_packet") or {}
    commands = list(packet.get("commands") or [])
    champion_action = ((action_league or {}).get("champion") or {}).get("action")
    if not champion_action:
        champion_action = ((reward_model or {}).get("best_action") or {}).get("action") or "probe"
    challenger = next((cmd for cmd in commands if cmd.get("action") != champion_action), None)
    champion = next((cmd for cmd in commands if cmd.get("action") == champion_action), None) or (commands[0] if commands else {})
    if not challenger:
        challenger_assignment = ((ab_executor or {}).get("assignments") or [{}])[0] or {}
        challenger = {
            "command_id": tournament_safety.stable_json_hash({"challenger": challenger_assignment}, length=20),
            "action": "probe",
            "route_key": challenger_assignment.get("route_key") or ((packet.get("focus_routes") or [""])[0]),
            "reason": "challenger slot from A/B executor",
        }
    slots = [
        {
            "slot": "champion",
            "worker": "worker_1",
            "command": champion,
            "budget_pct": 50.0,
            "objective": "exploit current best action packet",
        },
        {
            "slot": "challenger",
            "worker": "worker_2",
            "command": challenger,
            "budget_pct": 25.0,
            "objective": "test a competing command packet without contaminating champion evidence",
        },
        {
            "slot": "reserve_validation",
            "worker": "worker_3",
            "command": {"action": "revalidate", "route_key": challenger.get("route_key") or champion.get("route_key")},
            "budget_pct": 25.0,
            "objective": "validate or falsify the challenger route quickly",
        },
    ]
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Runtime champion/challenger slots for exploiting the best packet while testing a challenger.",
        "slots": slots,
        "champion_action": champion_action,
        "challenger_action": challenger.get("action"),
    }


def adaptive_experiment_stopping(
    cycles: list[dict[str, Any]] | None = None,
    experiment_plan: dict[str, Any] | None = None,
    outcome_ledger: dict[str, Any] | None = None,
    experiment_governor: dict[str, Any] | None = None,
) -> dict[str, Any]:
    recent = (cycles or [])[-4:]
    scored = sum(_cycle_scored_total(cycle) for cycle in recent)
    winners = sum(_cycle_winners(cycle) for cycle in recent)
    yield_per_10k = winners / max(1, scored) * 10000.0
    outcomes = {
        str(row.get("experiment_id")): row
        for row in (outcome_ledger or {}).get("outcomes") or []
        if row.get("experiment_id")
    }
    governor = {
        str(row.get("experiment_id")): row
        for row in (experiment_governor or {}).get("decisions") or []
        if row.get("experiment_id")
    }
    decisions = []
    for exp in (experiment_plan or {}).get("experiments") or []:
        exp_id = str(exp.get("experiment_id") or "")
        outcome = outcomes.get(exp_id, {})
        gov = governor.get(exp_id, {})
        decision = "continue"
        reason = "experiment still has unresolved value"
        if gov.get("decision") in {"stop_or_pivot", "pause_for_revalidation"}:
            decision = gov.get("decision")
            reason = gov.get("reason") or "governor requested stop/pause"
        elif outcome.get("next_action") == "scale_successful_treatment":
            decision = "graduate_and_scale"
            reason = "observed candidate improved the parent without rejection"
        elif recent and scored >= 200 and winners == 0:
            decision = "stop_early"
            reason = "enough recent samples produced no live beaters"
        elif outcome.get("next_action") == "downweight_or_change_treatment":
            decision = "stop_or_change_arm"
            reason = "outcome ledger says treatment/family is failing"
        decisions.append({
            "experiment_id": exp_id,
            "route_key": exp.get("route_key"),
            "family_key": exp.get("family_key"),
            "decision": decision,
            "reason": reason,
            "recent_scored": int(scored),
            "recent_winners": int(winners),
            "recent_yield_per_10k": round(yield_per_10k, 4),
        })
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Stops, pauses, graduates, or mutates experiments while the hunt is still running.",
        "decisions": decisions,
        "stop_count": sum(1 for row in decisions if row.get("decision") in {"stop_early", "stop_or_pivot", "stop_or_change_arm"}),
        "top_decision": decisions[0] if decisions else {},
    }


def counterfactual_command_replay(
    runtime_kernel: dict[str, Any] | None = None,
    action_outcomes: dict[str, Any] | None = None,
    rows: list[dict[str, Any]] | None = None,
    cohort_memory: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rows = rows or []
    outcomes = {
        str(row.get("command_id")): row
        for row in (action_outcomes or {}).get("outcomes") or []
        if row.get("command_id")
    }
    cohort_by_route = {
        str(row.get("route_key")): row
        for row in (cohort_memory or {}).get("cohorts") or []
        if row.get("route_key")
    }
    simulations = []
    for command in ((runtime_kernel or {}).get("commands") or [])[:20]:
        route = str(command.get("route_key") or "")
        route_rows = [row for row in rows if route and route_key_from_row(row) == route]
        observed = outcomes.get(str(command.get("command_id"))) or {}
        route_best = max([float(row.get("expected_promotable_pnl") or pnl(row)) for row in route_rows[:20]] or [0.0])
        cohort = cohort_by_route.get(route) or {}
        for alt_action, multiplier in [("scale", 1.15), ("probe", 0.95), ("repair", 1.08), ("revalidate", 1.02), ("quarantine", 0.35)]:
            if alt_action == command.get("action"):
                continue
            simulated = route_best * multiplier + float(cohort.get("avg_expected_promotable_pnl") or 0.0) * 0.05
            simulations.append({
                "base_command_id": command.get("command_id"),
                "route_key": route,
                "actual_action": command.get("action"),
                "counterfactual_action": alt_action,
                "observed_reward_score": observed.get("reward_score"),
                "estimated_reward_score": round(simulated, 4),
                "estimated_lift_vs_observed": round(simulated - float(observed.get("reward_score") or 0.0), 4),
            })
    simulations.sort(key=lambda row: float(row.get("estimated_lift_vs_observed") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Estimates what would have happened if nearby command actions had been used instead.",
        "simulations": simulations[:100],
        "best_counterfactual": simulations[0] if simulations else {},
    }


def experiment_contamination_guard(
    ab_executor: dict[str, Any] | None = None,
    experiment_plan: dict[str, Any] | None = None,
    runtime_kernel: dict[str, Any] | None = None,
    negative_bank: dict[str, Any] | None = None,
) -> dict[str, Any]:
    route_to_assignments: dict[str, list[dict[str, Any]]] = defaultdict(list)
    family_to_assignments: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for assignment in (ab_executor or {}).get("assignments") or []:
        route = str(assignment.get("route_key") or "")
        family = str(assignment.get("family_key") or "")
        if route:
            route_to_assignments[route].append(assignment)
        if family:
            family_to_assignments[family].append(assignment)
    avoid = set(str(route) for route in (negative_bank or {}).get("avoid_routes") or [])
    runtime_routes = set(str(route) for route in ((runtime_kernel or {}).get("command_packet") or {}).get("focus_routes") or [])
    risks = []
    for route, assignments in route_to_assignments.items():
        experiments = {str(row.get("experiment_id")) for row in assignments}
        treatments = {str(row.get("treatment")) for row in assignments}
        if len(experiments) > 1 or len(treatments) > 2:
            risks.append({"kind": "route_overlap", "route_key": route, "experiment_ids": sorted(experiments), "treatments": sorted(treatments), "severity": "medium"})
        if route in avoid and route in runtime_routes:
            risks.append({"kind": "avoid_route_focus_conflict", "route_key": route, "severity": "high"})
    for family, assignments in family_to_assignments.items():
        if len({str(row.get("experiment_id")) for row in assignments}) > 1:
            risks.append({"kind": "family_overlap", "family_key": family, "severity": "medium"})
    blocked_routes = sorted({row.get("route_key") for row in risks if row.get("severity") == "high" and row.get("route_key")})
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Prevents overlapping experiments from teaching false lessons on the same route/family.",
        "ok": not any(row.get("severity") == "high" for row in risks),
        "risks": risks,
        "blocked_routes": blocked_routes,
        "safe_focus_routes": [route for route in (ab_executor or {}).get("focus_routes") or [] if route not in set(blocked_routes)],
    }


def learning_rate_controller(
    cycles: list[dict[str, Any]] | None = None,
    throttle: dict[str, Any] | None = None,
    adaptive_stopping: dict[str, Any] | None = None,
    reward_model: dict[str, Any] | None = None,
    contamination_guard: dict[str, Any] | None = None,
) -> dict[str, Any]:
    recent = (cycles or [])[-4:]
    scored = sum(_cycle_scored_total(cycle) for cycle in recent)
    winners = sum(_cycle_winners(cycle) for cycle in recent)
    yield_per_10k = winners / max(1, scored) * 10000.0
    base_mult = float(((throttle or {}).get("controls") or {}).get("batch_size_multiplier") or 1.0)
    best_reward = float(((reward_model or {}).get("best_action") or {}).get("avg_reward_score") or 0.0)
    stop_count = int((adaptive_stopping or {}).get("stop_count") or 0)
    contamination_ok = (contamination_guard or {}).get("ok", True)
    mode = "balanced"
    explore_pct = 35.0
    if not contamination_ok or stop_count >= 2:
        mode = "conservative_revalidation"
        explore_pct = 15.0
        base_mult = min(base_mult, 0.65)
    elif yield_per_10k >= 20.0 and best_reward >= 8.0:
        mode = "exploit_with_challenger"
        explore_pct = 20.0
        base_mult = max(base_mult, 1.15)
    elif yield_per_10k <= 2.0:
        mode = "explore_harder"
        explore_pct = 55.0
        base_mult = max(base_mult, 1.05)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Controls how aggressively the hunt learns versus exploits while it is running.",
        "mode": mode,
        "explore_pct": round(explore_pct, 2),
        "exploit_pct": round(100.0 - explore_pct, 2),
        "batch_size_multiplier": round(base_mult, 4),
        "recent_yield_per_10k": round(yield_per_10k, 4),
        "stop_count": stop_count,
        "contamination_ok": bool(contamination_ok),
    }


def worker_learning_report_cards(
    rows: list[dict[str, Any]],
    cycles: list[dict[str, Any]] | None = None,
    worker_memory: dict[str, Any] | None = None,
    ab_executor: dict[str, Any] | None = None,
    command_outcomes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    by_worker: dict[str, dict[str, Any]] = defaultdict(lambda: {"candidate_count": 0, "pnl_sum": 0.0, "ready_sum": 0.0, "live_beaters": 0, "assignments": []})
    for row in rows[:300]:
        worker = str(lineage_info(row).get("worker_role") or "unknown_worker")
        item = by_worker[worker]
        item["candidate_count"] += 1
        item["pnl_sum"] += pnl(row)
        item["ready_sum"] += float(row.get("promotion_readiness_score") or promotion_readiness_score(row))
        item["live_beaters"] += 1 if float(row.get("step2_delta_vs_active") or 0.0) > 0.0 else 0
    for assignment in (ab_executor or {}).get("assignments") or []:
        worker = str(assignment.get("worker") or "unknown_worker")
        by_worker[worker]["assignments"].append(assignment)
    specialty = {
        str(row.get("worker")): row
        for row in (worker_memory or {}).get("workers") or []
        if row.get("worker")
    }
    cards = []
    for worker, item in by_worker.items():
        count = int(item["candidate_count"] or 0)
        avg_ready = float(item["ready_sum"]) / max(1, count)
        avg_pnl = float(item["pnl_sum"]) / max(1, count)
        cards.append({
            "worker": worker,
            "candidate_count": count,
            "live_beater_count": int(item["live_beaters"] or 0),
            "avg_pnl": round(avg_pnl, 4),
            "avg_promotion_readiness_score": round(avg_ready, 4),
            "learning_score": round(avg_ready + int(item["live_beaters"] or 0) * 5.0 + len(item["assignments"]) * 2.0, 4),
            "recommended_role": (specialty.get(worker) or {}).get("recommended_role") or ((item["assignments"] or [{}])[0] or {}).get("treatment") or "explore",
            "active_assignments": item["assignments"][:4],
        })
    if not cards:
        cards = [{"worker": f"worker_{idx + 1}", "candidate_count": 0, "live_beater_count": 0, "avg_pnl": 0.0, "avg_promotion_readiness_score": 0.0, "learning_score": 0.0, "recommended_role": "explore", "active_assignments": []} for idx in range(4)]
    cards.sort(key=lambda row: float(row.get("learning_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Per-worker learning report cards showing who finds robust live beaters versus fragile or empty work.",
        "cards": cards[:8],
        "top_worker": cards[0] if cards else {},
    }


def experiment_to_promotion_trace(
    rows: list[dict[str, Any]],
    experiment_plan: dict[str, Any] | None = None,
    outcome_ledger: dict[str, Any] | None = None,
    ab_executor: dict[str, Any] | None = None,
    promotion_simulator: dict[str, Any] | None = None,
) -> dict[str, Any]:
    experiments = list((experiment_plan or {}).get("experiments") or [])
    outcomes = {
        str(row.get("experiment_id")): row
        for row in (outcome_ledger or {}).get("outcomes") or []
        if row.get("experiment_id")
    }
    assignments_by_route: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for assignment in (ab_executor or {}).get("assignments") or []:
        if assignment.get("route_key"):
            assignments_by_route[str(assignment.get("route_key"))].append(assignment)
    reject_by_variant = {
        str(row.get("variant")): row
        for row in (promotion_simulator or {}).get("simulations") or []
        if row.get("variant")
    }
    traces = []
    for row in rows[:50]:
        route = route_key_from_row(row)
        family = str(row.get("family_key") or candidate_family_key(row))
        exp = next((item for item in experiments if str(item.get("route_key") or "") == route or str(item.get("family_key") or "") == family), {})
        if not exp and not assignments_by_route.get(route):
            continue
        exp_id = str(exp.get("experiment_id") or ((assignments_by_route.get(route) or [{}])[0] or {}).get("experiment_id") or "")
        traces.append({
            "variant": row.get("variant"),
            "route_key": route,
            "family_key": family,
            "experiment_id": exp_id,
            "experiment_kind": exp.get("kind") or ((assignments_by_route.get(route) or [{}])[0] or {}).get("kind"),
            "treatments_tested": [item.get("treatment") for item in assignments_by_route.get(route, [])],
            "outcome": outcomes.get(exp_id) or {},
            "promotion_readiness_score": row.get("promotion_readiness_score") or promotion_readiness_score(row),
            "step2_pnl": pnl(row),
            "promotion_risk": reject_by_variant.get(str(row.get("variant"))) or {},
            "trace_summary": f"{row.get('variant')} came through {exp.get('kind') or 'runtime_experiment'} on {route}.",
        })
    traces.sort(key=lambda row: (float(row.get("promotion_readiness_score") or 0.0), float(row.get("step2_pnl") or 0.0)), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Traces promotion candidates back to the experiments, arms, and outcomes that produced them.",
        "traces": traces[:100],
        "top_trace": traces[0] if traces else {},
    }


def zero_yield_autopsy_engine(
    cycles: list[dict[str, Any]] | None = None,
    rows: list[dict[str, Any]] | None = None,
    route_states: dict[str, Any] | None = None,
    alias_detector: dict[str, Any] | None = None,
    coverage_map: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cycles = cycles or []
    rows = rows or []
    recent = cycles[-5:]
    autopsies = []
    state_by_route = {
        str(row.get("route_key")): row
        for row in (route_states or {}).get("states") or []
        if row.get("route_key")
    }
    alias_routes = {str(row.get("route_key")) for row in (alias_detector or {}).get("traps") or [] if row.get("route_key")}
    blind_routes = {str(row.get("route_key")) for row in (coverage_map or {}).get("blind_spots") or [] if row.get("route_key")}
    for cycle in recent:
        scored = _cycle_scored_total(cycle)
        winners = _cycle_winners(cycle)
        if winners > 0:
            continue
        routes = [str(route) for route in (cycle.get("route_seeds") or cycle.get("focus_routes") or []) if route]
        if not routes and cycle.get("route_seed"):
            routes = [str(cycle.get("route_seed"))]
        reasons = []
        if scored <= 0:
            reasons.append("no_scored_candidates")
        if routes and all((state_by_route.get(route) or {}).get("state") == "retired" for route in routes):
            reasons.append("stale_or_retired_route")
        if any(route in alias_routes for route in routes):
            reasons.append("alias_trap")
        if any(route in blind_routes for route in routes):
            reasons.append("coverage_blind_spot")
        if cycle.get("hunter") == "router" and scored >= 100 and not routes:
            reasons.append("bad_route_seed_or_missing_route_telemetry")
        if not reasons and scored >= 200:
            reasons.append("mutation_surface_not_generating_live_edge")
        if not reasons:
            reasons.append("insufficient_evidence")
        autopsies.append({
            "cycle": cycle.get("cycle"),
            "hunter": cycle.get("hunter"),
            "scored_total": int(scored),
            "winners": int(winners),
            "route_seeds": routes,
            "primary_reason": reasons[0],
            "reasons": reasons,
            "recommended_intervention": {
                "alias_trap": "force_structural_mutation",
                "stale_or_retired_route": "retire_route_seed",
                "coverage_blind_spot": "probe_uncovered_neighbor",
                "bad_route_seed_or_missing_route_telemetry": "rebuild_route_seed",
                "mutation_surface_not_generating_live_edge": "widen_mutation_width",
                "no_scored_candidates": "lower_batch_constraints_or_rebuild_cache",
                "insufficient_evidence": "continue_small_probe",
            }.get(reasons[0], "continue_small_probe"),
        })
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Classifies zero-yield cycles while the hunt is running and proposes exact tactical changes.",
        "autopsies": autopsies,
        "zero_yield_count": len(autopsies),
        "top_autopsy": autopsies[0] if autopsies else {},
    }


def stuck_loop_breaker(
    cycles: list[dict[str, Any]] | None = None,
    rows: list[dict[str, Any]] | None = None,
    *,
    repeat_threshold: int = 3,
) -> dict[str, Any]:
    route_counts: dict[str, int] = defaultdict(int)
    treatment_counts: dict[str, int] = defaultdict(int)
    family_counts: dict[str, int] = defaultdict(int)
    recent_cycles = (cycles or [])[-8:]
    for cycle in recent_cycles:
        for route in cycle.get("route_seeds") or cycle.get("focus_routes") or []:
            if route:
                route_counts[str(route)] += 1
    for row in (rows or [])[:100]:
        treatment_counts[_row_treatment(row)] += 1
        family_counts[str(row.get("family_key") or candidate_family_key(row))] += 1
    resets = []
    for route, count in route_counts.items():
        if count >= repeat_threshold:
            resets.append({"kind": "route_reset", "route_key": route, "repeat_count": count, "action": "force_neighbor_or_widen"})
    for treatment, count in treatment_counts.items():
        if treatment and treatment != "unlabeled" and count >= 35:
            resets.append({"kind": "treatment_reset", "treatment": treatment, "repeat_count": count, "action": "force_alternate_treatment"})
    for family, count in family_counts.items():
        if family and count >= 20:
            resets.append({"kind": "family_reset", "family_key": family, "repeat_count": count, "action": "dedupe_or_downweight_family"})
    resets.sort(key=lambda row: int(row.get("repeat_count") or 0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Detects repeated route/family/treatment loops and forces a tactical reset.",
        "resets": resets[:50],
        "reset_count": len(resets),
        "force_widen_routes": [row.get("route_key") for row in resets if row.get("kind") == "route_reset"][:10],
        "downweight_families": [row.get("family_key") for row in resets if row.get("kind") == "family_reset"][:10],
    }


def search_space_coverage_map(
    rows: list[dict[str, Any]] | None = None,
    cycles: list[dict[str, Any]] | None = None,
    ab_executor: dict[str, Any] | None = None,
) -> dict[str, Any]:
    route_counts: dict[str, int] = defaultdict(int)
    treatment_counts: dict[str, int] = defaultdict(int)
    width_counts: dict[str, int] = defaultdict(int)
    regime_counts: dict[str, int] = defaultdict(int)
    for row in rows or []:
        route_counts[route_key_from_row(row)] += 1
        treatment_counts[_row_treatment(row)] += 1
        width_counts[str(lineage_info(row).get("mutation_scale") or row.get("mutation_width") or "unknown")] += 1
        regime_counts[str(row.get("regime_key") or row.get("market_regime") or row.get("session_phase") or "unknown")] += 1
    for assignment in (ab_executor or {}).get("assignments") or []:
        if assignment.get("route_key"):
            route_counts[str(assignment.get("route_key"))] += 1
        if assignment.get("treatment"):
            treatment_counts[str(assignment.get("treatment"))] += 1
    cycle_routes = set()
    for cycle in cycles or []:
        for route in cycle.get("route_seeds") or []:
            if route:
                cycle_routes.add(str(route))
    blind_spots = []
    for route, count in route_counts.items():
        if count <= 2 and route not in cycle_routes:
            blind_spots.append({"route_key": route, "coverage_count": count, "recommended_action": "probe_neighbor"})
    undercovered_treatments = [
        {"treatment": treatment, "coverage_count": count, "recommended_action": "allocate_probe"}
        for treatment, count in treatment_counts.items()
        if treatment and count <= 2
    ]
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Maps explored routes, treatments, regimes, and mutation widths to reveal blind spots.",
        "route_coverage": dict(sorted(route_counts.items(), key=lambda item: item[1], reverse=True)[:100]),
        "treatment_coverage": dict(sorted(treatment_counts.items(), key=lambda item: item[1], reverse=True)),
        "mutation_width_coverage": dict(sorted(width_counts.items(), key=lambda item: item[1], reverse=True)[:30]),
        "regime_coverage": dict(sorted(regime_counts.items(), key=lambda item: item[1], reverse=True)[:30]),
        "blind_spots": blind_spots[:50],
        "undercovered_treatments": undercovered_treatments[:20],
    }


def alias_trap_detector(
    rows: list[dict[str, Any]] | None = None,
    cycles: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    by_route: dict[str, dict[str, Any]] = defaultdict(lambda: {"rows": 0, "aliases": 0, "behaviors": set(), "variants": []})
    for row in rows or []:
        route = route_key_from_row(row)
        item = by_route[route]
        item["rows"] += 1
        item["aliases"] += max(0, int(row.get("behavior_alias_count") or 1) - 1)
        item["behaviors"].add(str(row.get("behavior_key") or behavior_key(row)))
        item["variants"].append(row.get("variant"))
    traps = []
    for route, item in by_route.items():
        rows_count = int(item["rows"] or 0)
        behavior_count = len(item["behaviors"])
        alias_pressure = float(item["aliases"]) / max(1, rows_count)
        if rows_count >= 4 and (alias_pressure >= 0.75 or behavior_count <= max(1, rows_count // 4)):
            traps.append({
                "route_key": route,
                "candidate_count": rows_count,
                "unique_behavior_count": behavior_count,
                "alias_pressure": round(alias_pressure, 4),
                "variants": item["variants"][:10],
                "recommended_action": "force_structural_mutation",
            })
    telemetry_alias = []
    for cycle in cycles or []:
        if float(cycle.get("alias_rate") or 0.0) >= 0.80:
            telemetry_alias.append({"cycle": cycle.get("cycle"), "hunter": cycle.get("hunter"), "alias_rate": cycle.get("alias_rate")})
    traps.sort(key=lambda row: (float(row.get("alias_pressure") or 0.0), int(row.get("candidate_count") or 0)), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Detects routes where config variation is producing behaviorally identical candidates.",
        "traps": traps[:50],
        "trap_count": len(traps),
        "structural_mutation_routes": [row.get("route_key") for row in traps[:12]],
        "telemetry_alias_cycles": telemetry_alias[-20:],
    }


def live_beater_scarcity_mode(
    cycles: list[dict[str, Any]] | None = None,
    rows: list[dict[str, Any]] | None = None,
    coverage_map: dict[str, Any] | None = None,
    alias_detector_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    recent = (cycles or [])[-5:]
    scored = sum(_cycle_scored_total(cycle) for cycle in recent)
    winners = sum(_cycle_winners(cycle) for cycle in recent)
    yield_per_10k = winners / max(1, scored) * 10000.0
    live_rows = [row for row in rows or [] if float(row.get("step2_delta_vs_active") or 0.0) > 0.0]
    scarcity = yield_per_10k < 2.0 and len(live_rows) < 10
    focus_routes = [row.get("route_key") for row in (coverage_map or {}).get("blind_spots") or [] if row.get("route_key")]
    if not focus_routes:
        focus_routes = [route for route in (alias_detector_payload or {}).get("structural_mutation_routes") or [] if route]
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Switches from promotion-quality optimization to broad discovery when live beaters dry up.",
        "enabled": bool(scarcity),
        "recent_scored": int(scored),
        "recent_winners": int(winners),
        "recent_yield_per_10k": round(yield_per_10k, 4),
        "mode": "broad_discovery" if scarcity else "normal_truth_first",
        "mutation_width": "wide" if scarcity else "medium",
        "batch_size_multiplier": 1.25 if scarcity else 1.0,
        "focus_routes": focus_routes[:10],
        "duration_cycles": 2 if scarcity else 0,
    }


def route_seed_quality_score(
    cycles: list[dict[str, Any]] | None = None,
    rows: list[dict[str, Any]] | None = None,
    alias_detector_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    stats: dict[str, dict[str, float]] = defaultdict(lambda: {"cycles": 0.0, "scored": 0.0, "winners": 0.0, "best_pnl": 0.0, "aliases": 0.0})
    for cycle in cycles or []:
        routes = cycle.get("route_seeds") or cycle.get("focus_routes") or []
        for route in routes:
            route = str(route or "")
            if not route:
                continue
            item = stats[route]
            item["cycles"] += 1
            item["scored"] += _cycle_scored_total(cycle)
            item["winners"] += _cycle_winners(cycle)
            item["aliases"] += float(cycle.get("alias_rate") or 0.0)
    for row in rows or []:
        route = route_key_from_row(row)
        item = stats[route]
        item["best_pnl"] = max(float(item["best_pnl"] or 0.0), pnl(row))
        item["winners"] += 1 if float(row.get("step2_delta_vs_active") or 0.0) > 0.0 else 0
    alias_routes = set((alias_detector_payload or {}).get("structural_mutation_routes") or [])
    scores = []
    for route, item in stats.items():
        cycles_seen = max(1.0, item["cycles"])
        yield_per_10k = item["winners"] / max(1.0, item["scored"]) * 10000.0 if item["scored"] else item["winners"] * 5.0
        avg_alias = item["aliases"] / cycles_seen
        score = yield_per_10k + float(item["best_pnl"] or 0.0) * 0.01 - avg_alias * 15.0
        if route in alias_routes:
            score -= 20.0
        scores.append({
            "route_key": route,
            "quality_score": round(score, 4),
            "cycles": int(item["cycles"]),
            "winner_yield_per_10k": round(yield_per_10k, 4),
            "best_pnl": round(float(item["best_pnl"] or 0.0), 4),
            "avg_alias_rate": round(avg_alias, 4),
            "recommended_action": "clone_or_split_seed" if score >= 25.0 else "retire_seed" if score < -5.0 else "keep_probe",
        })
    scores.sort(key=lambda row: float(row.get("quality_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Scores route seeds so productive seeds are cloned/split and weak seeds are retired.",
        "scores": scores[:100],
        "clone_routes": [row.get("route_key") for row in scores if row.get("recommended_action") == "clone_or_split_seed"][:10],
        "retire_routes": [row.get("route_key") for row in scores if row.get("recommended_action") == "retire_seed"][:10],
        "top_seed": scores[0] if scores else {},
    }


def opportunity_cost_meter(
    rows: list[dict[str, Any]] | None = None,
    route_seed_quality: dict[str, Any] | None = None,
    coverage_map: dict[str, Any] | None = None,
    info_gain: dict[str, Any] | None = None,
) -> dict[str, Any]:
    seed_scores = list((route_seed_quality or {}).get("scores") or [])
    best_score = max([float(row.get("quality_score") or 0.0) for row in seed_scores] or [0.0])
    costs = []
    for row in seed_scores:
        score = float(row.get("quality_score") or 0.0)
        cost = max(0.0, best_score - score)
        if cost <= 0.0:
            continue
        costs.append({
            "route_key": row.get("route_key"),
            "quality_score": round(score, 4),
            "opportunity_cost": round(cost, 4),
            "recommended_action": "reduce_budget" if cost >= 25.0 else "watch",
        })
    for blind in (coverage_map or {}).get("blind_spots") or []:
        costs.append({
            "route_key": blind.get("route_key"),
            "quality_score": 0.0,
            "opportunity_cost": 12.0,
            "recommended_action": "allocate_probe_to_blind_spot",
        })
    info_actions = list((info_gain or {}).get("actions") or [])[:5]
    costs.sort(key=lambda row: float(row.get("opportunity_cost") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Estimates cycles wasted on low-value lanes versus higher expected-value alternatives.",
        "costs": costs[:100],
        "highest_cost": costs[0] if costs else {},
        "better_alternatives": info_actions,
        "reduce_budget_routes": [row.get("route_key") for row in costs if row.get("recommended_action") == "reduce_budget"][:10],
    }


def recovery_playbook_generator(
    zero_autopsy: dict[str, Any] | None = None,
    stuck_loop: dict[str, Any] | None = None,
    opportunity_cost: dict[str, Any] | None = None,
    coverage_map: dict[str, Any] | None = None,
    scarcity_mode: dict[str, Any] | None = None,
    alias_detector_payload: dict[str, Any] | None = None,
    seed_quality: dict[str, Any] | None = None,
) -> dict[str, Any]:
    interventions = []

    def add(action: str, route: str = "", reason: str = "", priority: float = 50.0, **extra: Any) -> None:
        interventions.append({
            "intervention_id": tournament_safety.stable_json_hash({"action": action, "route": route, "reason": reason, "extra": extra}, length=20),
            "action": action,
            "route_key": route,
            "reason": reason,
            "priority_score": round(priority, 4),
            **extra,
        })

    for autopsy in (zero_autopsy or {}).get("autopsies") or []:
        route = ((autopsy.get("route_seeds") or [""])[0] or "")
        intervention = autopsy.get("recommended_intervention") or "continue_small_probe"
        add(intervention, str(route), autopsy.get("primary_reason") or "zero_yield", 82.0)
    for route in (stuck_loop or {}).get("force_widen_routes") or []:
        add("widen_route", str(route), "stuck_loop_route_repeat", 78.0)
    for route in (alias_detector_payload or {}).get("structural_mutation_routes") or []:
        add("force_structural_mutation", str(route), "alias_trap", 88.0, mutation_width="wide")
    for route in (seed_quality or {}).get("retire_routes") or []:
        add("retire_seed", str(route), "route_seed_quality_low", 75.0)
    for route in (seed_quality or {}).get("clone_routes") or []:
        add("clone_or_split_seed", str(route), "route_seed_quality_high", 70.0)
    for route in (opportunity_cost or {}).get("reduce_budget_routes") or []:
        add("reduce_budget", str(route), "high_opportunity_cost", 65.0)
    if (scarcity_mode or {}).get("enabled"):
        for route in (scarcity_mode or {}).get("focus_routes") or []:
            add("scarcity_probe", str(route), "live_beater_scarcity", 80.0, mutation_width="wide")
    for blind in (coverage_map or {}).get("blind_spots") or []:
        add("probe_blind_spot", str(blind.get("route_key") or ""), "coverage_blind_spot", 58.0)
    interventions.sort(key=lambda row: float(row.get("priority_score") or 0.0), reverse=True)
    skip_routes = [row.get("route_key") for row in interventions if row.get("action") in {"retire_seed", "reduce_budget"} and row.get("route_key")]
    widen_routes = [row.get("route_key") for row in interventions if row.get("action") in {"widen_route", "force_structural_mutation", "scarcity_probe"} and row.get("route_key")]
    focus_routes = [row.get("route_key") for row in interventions if row.get("action") in {"clone_or_split_seed", "scarcity_probe", "probe_blind_spot"} and row.get("route_key")]
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Converts self-debugging diagnostics into exact next-cycle interventions.",
        "interventions": interventions[:100],
        "top_intervention": interventions[0] if interventions else {},
        "skip_routes": list(dict.fromkeys(skip_routes))[:12],
        "widen_routes": list(dict.fromkeys(widen_routes))[:12],
        "focus_routes": list(dict.fromkeys(focus_routes))[:12],
        "structural_mutation_routes": [row.get("route_key") for row in interventions if row.get("action") == "force_structural_mutation" and row.get("route_key")][:12],
    }


def meta_hunt_strategy_learner(
    cycles: list[dict[str, Any]] | None = None,
    rows: list[dict[str, Any]] | None = None,
    search_portfolio: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cycles = cycles or []
    rows = rows or []
    by_hunter: dict[str, dict[str, float]] = defaultdict(lambda: {"cycles": 0.0, "scored": 0.0, "winners": 0.0})
    for cycle in cycles:
        hunter = str(cycle.get("hunter") or "unknown")
        by_hunter[hunter]["cycles"] += 1.0
        by_hunter[hunter]["scored"] += float(_cycle_scored_total(cycle))
        by_hunter[hunter]["winners"] += float(_cycle_winners(cycle))
    strategies = []
    for hunter, item in by_hunter.items():
        yield_per_10k = item["winners"] / max(1.0, item["scored"]) * 10000.0
        strategies.append({
            "strategy": f"{hunter}_heavy",
            "cycles": int(item["cycles"]),
            "winner_yield_per_10k": round(yield_per_10k, 4),
            "recommended_budget_pct": round(min(55.0, 15.0 + yield_per_10k), 2),
        })
    if rows:
        repair_need = sum(1 for row in rows if "thin_holdout_edge" in (row.get("learning_tags") or []))
        strategies.append({
            "strategy": "promotion_readiness_first",
            "cycles": len(cycles),
            "winner_yield_per_10k": 0.0,
            "recommended_budget_pct": round(20.0 + min(25.0, repair_need * 2.0), 2),
        })
    for bucket in (search_portfolio or {}).get("allocations") or []:
        strategies.append({
            "strategy": str(bucket.get("bucket") or "portfolio_bucket"),
            "cycles": len(cycles),
            "winner_yield_per_10k": 0.0,
            "recommended_budget_pct": bucket.get("budget_pct"),
        })
    strategies.sort(key=lambda row: float(row.get("recommended_budget_pct") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Learns which hunt strategy mix is working, separate from candidate-level scoring.",
        "strategies": strategies,
    }


def run_to_run_postmortem_synthesizer(
    rows: list[dict[str, Any]],
    cycles: list[dict[str, Any]] | None,
    learning: dict[str, Any],
) -> dict[str, Any]:
    cycles = cycles or []
    top = rows[0] if rows else {}
    best_treatment = (learning.get("treatment_effects") or {}).get("best_treatment")
    worst_treatment = (learning.get("treatment_effects") or {}).get("worst_treatment")
    debt = (learning.get("experiment_debt_queue") or {}).get("queue") or []
    info = (learning.get("information_gain_scoring") or {}).get("actions") or []
    notes = []
    if top:
        notes.append(f"Best live beater was {top.get('variant')} at P/L {pnl(top)} with expected promotable P/L {top.get('expected_promotable_pnl')}.")
    if best_treatment:
        notes.append(f"Best treatment signal: {best_treatment}.")
    if worst_treatment:
        notes.append(f"Weakest treatment signal: {worst_treatment}.")
    if debt:
        notes.append(f"Top unanswered question: {debt[0].get('question')}.")
    if info:
        notes.append(f"Highest information-gain action: {info[0].get('action')} on {info[0].get('target')}.")
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Compact memo to feed into the next hunt automatically.",
        "cycles": len(cycles),
        "top_candidate": finalist_row(top, 1) if top else None,
        "what_worked": [note for note in notes if "Best" in note or "best" in note],
        "what_failed_or_needs_caution": [note for note in notes if "Weakest" in note or "unanswered" in note],
        "what_to_scale_next": (learning.get("treatment_confidence_model") or {}).get("scale") or [],
        "what_to_avoid_or_repair": (learning.get("treatment_confidence_model") or {}).get("abandon") or [],
        "top_unanswered_questions": debt[:10],
        "next_information_gain_actions": info[:10],
        "memo": notes,
    }


def prediction_calibration_ledger(
    rows: list[dict[str, Any]],
    feedback_rows: list[dict[str, Any]] | None = None,
    promotion_predictions: dict[str, Any] | None = None,
    treatment_confidence: dict[str, Any] | None = None,
    value_of_information: dict[str, Any] | None = None,
) -> dict[str, Any]:
    feedback_by_variant = _feedback_by_variant(feedback_rows)
    predictions_by_variant = {
        str(row.get("variant")): row
        for row in (promotion_predictions or {}).get("predictions") or []
        if isinstance(row, dict) and row.get("variant")
    }
    entries = []
    errors = []
    reason_hits = []
    false_positive_count = 0
    false_negative_count = 0
    for row in rows:
        variant = str(row.get("variant") or "")
        feedback = feedback_by_variant.get(variant, [])
        if not feedback:
            entries.append({
                "prediction_type": "promotion_approve_probability",
                "variant": variant,
                "predicted_probability": row.get("promotion_approve_probability") or promotion_approve_probability(row),
                "actual": "pending",
                "calibration_error": None,
            })
            continue
        rejected = any(str(item.get("status") or item.get("decision") or "").lower() in {"reject", "rejected", "blocked", "failed"} for item in feedback)
        actual_approve = 0.0 if rejected else 1.0
        predicted = float(row.get("promotion_approve_probability") or promotion_approve_probability(row))
        error = abs(predicted - actual_approve)
        errors.append(error)
        if predicted >= 0.65 and rejected:
            false_positive_count += 1
        if predicted <= 0.35 and not rejected:
            false_negative_count += 1
        prediction = predictions_by_variant.get(variant, {})
        predicted_reasons = {
            str(item.get("reason"))
            for item in prediction.get("likely_reject_reasons") or []
            if isinstance(item, dict) and item.get("reason")
        }
        actual_reasons = {
            str(reason)
            for item in feedback
            for reason in (item.get("reject_reasons") or [])
        }
        hit = bool(predicted_reasons & actual_reasons) if actual_reasons and predicted_reasons else None
        if hit is not None:
            reason_hits.append(1.0 if hit else 0.0)
        entries.append({
            "prediction_type": "promotion_approve_probability",
            "variant": variant,
            "predicted_probability": round(predicted, 4),
            "actual": "approved" if actual_approve else "rejected",
            "calibration_error": round(error, 4),
            "predicted_reject_reasons": sorted(predicted_reasons),
            "actual_reject_reasons": sorted(actual_reasons),
            "reject_reason_hit": hit,
        })
    treatment_entries = []
    for row in (treatment_confidence or {}).get("decisions") or []:
        treatment_entries.append({
            "prediction_type": "treatment_decision",
            "treatment": row.get("treatment"),
            "predicted_decision": row.get("decision"),
            "posterior_success_mean": row.get("posterior_success_mean"),
            "actual": "pending_future_outcome",
        })
    voi_entries = []
    for row in (value_of_information or {}).get("plans") or []:
        voi_entries.append({
            "prediction_type": "value_of_information",
            "action": row.get("action"),
            "target": row.get("target"),
            "predicted_decision_change_probability": row.get("probability_changes_decision"),
            "expected_value_of_information": row.get("expected_value_of_information"),
            "actual": "pending_future_outcome",
        })
    mean_error = _avg(errors) if errors else 0.45
    reason_accuracy = _avg(reason_hits) if reason_hits else 0.50
    calibration_confidence = max(0.10, min(0.95, 1.0 - mean_error)) * (0.75 + 0.25 * reason_accuracy)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Ledger comparing learner predictions against actual feedback when available.",
        "calibration_confidence": round(calibration_confidence, 4),
        "mean_probability_error": round(mean_error, 4),
        "reject_reason_accuracy": round(reason_accuracy, 4),
        "false_positive_count": false_positive_count,
        "false_negative_count": false_negative_count,
        "entries": entries,
        "treatment_entries": treatment_entries,
        "value_of_information_entries": voi_entries,
    }


def belief_revision_engine(calibration_ledger: dict[str, Any] | None = None) -> dict[str, Any]:
    ledger = calibration_ledger or {}
    confidence = float(ledger.get("calibration_confidence") or 0.55)
    mean_error = float(ledger.get("mean_probability_error") or 0.45)
    reason_accuracy = float(ledger.get("reject_reason_accuracy") or 0.50)
    false_positives = int(ledger.get("false_positive_count") or 0)
    false_negatives = int(ledger.get("false_negative_count") or 0)
    approve_weight = max(0.35, min(1.25, confidence + 0.25 - mean_error * 0.25))
    reject_reason_weight = max(0.30, min(1.25, 0.55 + reason_accuracy * 0.60))
    false_positive_penalty = min(0.35, false_positives * 0.05)
    false_negative_penalty = min(0.25, false_negatives * 0.04)
    truth_confidence = max(0.10, min(0.95, confidence - false_positive_penalty * 0.50 - false_negative_penalty * 0.25))
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Belief revision weights derived from prediction calibration.",
        "truth_calibration_confidence": round(truth_confidence, 4),
        "predictor_weights": {
            "approve_probability": round(approve_weight, 4),
            "reject_reason": round(reject_reason_weight, 4),
            "treatment_decision": round(max(0.35, min(1.15, confidence)), 4),
            "value_of_information": round(max(0.35, min(1.10, 0.50 + confidence * 0.50)), 4),
        },
        "penalties": {
            "false_positive_penalty": round(false_positive_penalty, 4),
            "false_negative_penalty": round(false_negative_penalty, 4),
            "probability_error_penalty": round(mean_error, 4),
        },
        "formula_adjustments": [
            "Downweight expected promotable P/L when approve predictions overstate actual outcomes.",
            "Increase reject-reason penalty when predicted reasons miss actual promotion feedback.",
            "Require stronger evidence gate when calibration confidence is low.",
        ],
    }


def out_of_distribution_detector(
    rows: list[dict[str, Any]],
    regime_learning: dict[str, Any] | None = None,
    *,
    limit: int = 100,
) -> dict[str, Any]:
    known_regimes = {str(row.get("regime_key") or "") for row in (regime_learning or {}).get("regimes") or []}
    known_routes = {
        str(route)
        for row in (regime_learning or {}).get("regimes") or []
        for route in (row.get("top_routes") or [])
    }
    detections = []
    for row in rows[:limit]:
        route = route_key_from_row(row)
        weights = row.get("weights") if isinstance(row.get("weights"), dict) else {}
        reasons = []
        score = 0.0
        if route == "unrouted" or (known_routes and route not in known_routes):
            score += 25.0
            reasons.append("new_or_unseen_route")
        regime = _regime_key(row)
        if known_regimes and regime not in known_regimes:
            score += 18.0
            reasons.append("new_regime_combination")
        if float(row.get("novelty_score") or 0.0) >= 70.0:
            score += 15.0
            reasons.append("high_config_behavior_novelty")
        if any(abs(float(value or 0.0)) >= 3.75 for value in weights.values()):
            score += 14.0
            reasons.append("extreme_feature_weight")
        tags = set(row.get("learning_tags") or candidate_learning_tags(row))
        for tag, points in [("ticker_concentrated", 12.0), ("side_concentrated", 10.0), ("thin_sample", 14.0), ("route_narrow", 10.0)]:
            if tag in tags:
                score += points
                reasons.append(tag)
        detections.append({
            "variant": row.get("variant"),
            "route_key": route,
            "regime_key": regime,
            "ood_score": round(min(100.0, score), 4),
            "ood_penalty": round(min(0.75, score / 140.0), 4),
            "reasons": reasons,
            "recommended_action": "learn_carefully_before_scaling" if score >= 35.0 else "in_distribution",
        })
    detections.sort(key=lambda row: float(row.get("ood_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Detects candidates outside prior winner/regime/treatment memory.",
        "detections": detections,
    }


def adversarial_red_team_learner(
    rows: list[dict[str, Any]],
    out_of_distribution: dict[str, Any] | None = None,
    treatment_effects: dict[str, Any] | None = None,
    *,
    limit: int = 30,
) -> dict[str, Any]:
    ood_by_variant = {
        str(row.get("variant")): row
        for row in (out_of_distribution or {}).get("detections") or []
        if isinstance(row, dict) and row.get("variant")
    }
    weak_treatments = {
        str(row.get("treatment"))
        for row in (treatment_effects or {}).get("treatments") or []
        if float(row.get("reject_rate") or 0.0) >= 0.4 or float(row.get("alias_rate") or 0.0) >= 0.5
    }
    tasks = []
    for row in sorted(rows, key=lambda item: float(item.get("expected_promotable_pnl") or 0.0), reverse=True)[:limit]:
        variant = str(row.get("variant") or "")
        lineage = lineage_info(row)
        treatment = _row_treatment(row)
        ood = ood_by_variant.get(variant, {})
        attack = "route_disable_and_feature_weaken"
        if float(ood.get("ood_score") or 0.0) >= 45.0:
            attack = "ood_stress_replay"
        elif treatment in weak_treatments:
            attack = "treatment_failure_replay"
        tasks.append({
            "variant": variant,
            "route_key": route_key_from_row(row),
            "treatment": treatment,
            "attack": attack,
            "how_it_could_be_fooling_us": [
                "edge is route-specific artifact",
                "top feature is overfit to current tape",
                "profit comes from concentrated ticker/session behavior",
                "promotion proxy is overconfident",
            ],
            "cheapest_break_test": (
                "disable_route" if route_key_from_row(row) != "unrouted"
                else "weaken_top_feature"
            ),
            "ood_reasons": ood.get("reasons") or [],
            "parent_variant": lineage.get("parent_variant"),
        })
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Adversarial tasks designed to break top candidate and treatment claims cheaply.",
        "tasks": tasks,
    }


def memory_compression_distiller(
    treatment_effects: dict[str, Any] | None = None,
    regime_learning: dict[str, Any] | None = None,
    failure_autopsy: dict[str, Any] | None = None,
    experiment_debt: dict[str, Any] | None = None,
    calibration_ledger: dict[str, Any] | None = None,
) -> dict[str, Any]:
    promising = []
    avoid = []
    for row in (treatment_effects or {}).get("treatments") or []:
        if float(row.get("evidence_score") or 0.0) > 0:
            promising.append(f"Treatment {row.get('treatment')} currently has positive evidence: {row.get('conclusion')}")
        if float(row.get("reject_rate") or 0.0) >= 0.5 or float(row.get("alias_rate") or 0.0) >= 0.5:
            avoid.append(f"Treat {row.get('treatment')} cautiously: reject/alias risk is elevated.")
    regime_rules = [
        f"In {row.get('regime_key')}, prefer {row.get('best_treatment')} until contradicted."
        for row in (regime_learning or {}).get("regimes") or []
        if row.get("best_treatment")
    ][:12]
    repair_rules = [
        f"Repair {row.get('variant')} with {row.get('recommended_repair_lane')} before promotion."
        for row in (failure_autopsy or {}).get("autopsies") or []
        if row.get("status") != "passed_proxy"
    ][:12]
    open_questions = [row.get("question") for row in (experiment_debt or {}).get("queue") or [] if row.get("question")][:20]
    confidence = float((calibration_ledger or {}).get("calibration_confidence") or 0.55)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Compressed field manual distilled from growing hunt memory.",
        "current_beliefs_confidence": round(confidence, 4),
        "durable_rules": regime_rules + repair_rules,
        "promising_patterns": promising[:20],
        "avoid_patterns": avoid[:20],
        "open_questions": open_questions,
    }


def self_audit_score(
    calibration_ledger: dict[str, Any] | None = None,
    experiment_debt: dict[str, Any] | None = None,
    evidence_gate: dict[str, Any] | None = None,
    learning_velocity: dict[str, Any] | None = None,
) -> dict[str, Any]:
    calibration = float((calibration_ledger or {}).get("calibration_confidence") or 0.55)
    debt_count = len((experiment_debt or {}).get("queue") or [])
    gate_count = len((evidence_gate or {}).get("gates") or [])
    blocked = sum(1 for row in (evidence_gate or {}).get("gates") or [] if row.get("failures"))
    velocity = float((learning_velocity or {}).get("learning_velocity_score") or 0.0)
    score = 100.0
    score -= max(0.0, 0.75 - calibration) * 55.0
    score -= min(25.0, debt_count * 0.75)
    score -= (blocked / max(1, gate_count)) * 20.0
    score += min(15.0, velocity * 0.10)
    score = max(0.0, min(100.0, score))
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Health score for the learning system itself.",
        "self_audit_score": round(score, 4),
        "components": {
            "calibration_confidence": round(calibration, 4),
            "unresolved_debt_count": debt_count,
            "evidence_gate_block_rate": round(blocked / max(1, gate_count), 4),
            "learning_velocity_score": round(velocity, 4),
        },
        "status": "healthy" if score >= 75.0 else "watch" if score >= 55.0 else "needs_calibration",
    }


def truth_first_promotion_objective(
    rows: list[dict[str, Any]],
    belief_revision: dict[str, Any] | None = None,
    out_of_distribution: dict[str, Any] | None = None,
    evidence_gate: dict[str, Any] | None = None,
    *,
    limit: int = 100,
) -> dict[str, Any]:
    truth_conf = float((belief_revision or {}).get("truth_calibration_confidence") or 0.55)
    ood_by_variant = {
        str(row.get("variant")): row
        for row in (out_of_distribution or {}).get("detections") or []
        if isinstance(row, dict) and row.get("variant")
    }
    gates = list((evidence_gate or {}).get("gates") or [])
    gate_fail_rate = sum(1 for row in gates if row.get("failures")) / max(1, len(gates))
    evidence_factor = max(0.35, 1.0 - gate_fail_rate * 0.35)
    ranked = []
    for row in rows:
        variant = str(row.get("variant") or "")
        ood = ood_by_variant.get(variant, {})
        ood_penalty = float(ood.get("ood_penalty") or 0.0)
        truth_score = float(row.get("expected_promotable_pnl") or expected_promotable_pnl(row))
        truth_score *= truth_conf
        truth_score *= max(0.20, 1.0 - ood_penalty)
        truth_score *= evidence_factor
        item = finalist_row(row, len(ranked) + 1)
        item["truth_first_promotable_pnl"] = round(truth_score, 4)
        item["truth_calibration_confidence"] = round(truth_conf, 4)
        item["ood_penalty"] = round(ood_penalty, 4)
        item["evidence_sufficiency_factor"] = round(evidence_factor, 4)
        item["truth_first_reasons"] = list(ood.get("reasons") or [])
        ranked.append(item)
    ranked.sort(key=lambda row: (float(row.get("truth_first_promotable_pnl") or 0.0), float(row.get("step2_pnl") or 0.0)), reverse=True)
    for idx, row in enumerate(ranked, 1):
        row["rank"] = idx
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Truth-first promotion objective: expected promotable P/L adjusted by calibration, OOD, and evidence sufficiency.",
        "leaderboard": ranked[:limit],
    }


def behavior_cache_payload(rows: list[dict[str, Any]], *, source: str = "") -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("behavior_key") or behavior_key(row))].append(row)
    entries = []
    for key, group in grouped.items():
        group = sorted(group, key=pnl, reverse=True)
        entries.append(
            {
                "behavior_key": key,
                "alias_count": sum(int(row.get("behavior_alias_count") or 1) for row in group),
                "leader_variant": group[0].get("variant"),
                "leader_config_key": group[0].get("config_key") or config_key(group[0]),
                "step2_pnl": pnl(group[0]),
                "step2_trades": trade_count(group[0]),
                "step2_win_rate_pct": win_rate(group[0]),
                "decision_hashes": sorted({
                    str((row.get("score_cache") or {}).get("decision_hash") or "")
                    for row in group
                    if isinstance(row.get("score_cache"), dict) and (row.get("score_cache") or {}).get("decision_hash")
                }),
                "aliases": [
                    {
                        "variant": row.get("variant"),
                        "config_key": row.get("config_key") or config_key(row),
                        "source": row.get("hunt_source_path"),
                    }
                    for row in group[:50]
                ],
            }
        )
    entries.sort(key=lambda row: float(row.get("step2_pnl") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": source or "step2_hunt_intelligence",
        "description": "Alias-aware behavior signatures observed in hunt outputs.",
        "behavior_count": len(entries),
        "entries": entries,
    }


def _cycle_scored_total(cycle: dict[str, Any]) -> int:
    text = str(cycle.get("stdout_tail") or "")
    matches = re.findall(r'"scored_total"\s*:\s*(\d+)', text)
    if matches:
        return int(matches[-1])
    return 0


def _cycle_winners(cycle: dict[str, Any]) -> int:
    text = str(cycle.get("stdout_tail") or "")
    matches = re.findall(r'"winners"\s*:\s*(\d+)', text)
    if matches:
        return int(matches[-1])
    return 0


def experiment_memory(cycles: list[dict[str, Any]], rows: list[dict[str, Any]], *, source: str = "") -> dict[str, Any]:
    by_hunter: dict[str, dict[str, Any]] = defaultdict(lambda: {"cycles": 0, "scored_total": 0, "reported_winners": 0})
    for cycle in cycles:
        hunter = str(cycle.get("hunter") or "unknown")
        by_hunter[hunter]["cycles"] += 1
        by_hunter[hunter]["scored_total"] += _cycle_scored_total(cycle)
        by_hunter[hunter]["reported_winners"] += _cycle_winners(cycle)
    by_route: dict[str, dict[str, Any]] = defaultdict(lambda: {"kept": 0, "best_pnl": -1e18, "best_variant": None, "quality_sum": 0.0})
    for row in rows:
        route = route_key_from_row(row)
        item = by_route[route]
        item["kept"] += int(row.get("behavior_alias_count") or 1)
        item["quality_sum"] += float(row.get("promotion_quality_score") or 0.0)
        if pnl(row) > float(item["best_pnl"]):
            item["best_pnl"] = pnl(row)
            item["best_variant"] = row.get("variant")
    route_rows = []
    for route, item in by_route.items():
        kept = int(item["kept"] or 0)
        route_rows.append({
            "route_key": route,
            "kept": kept,
            "best_pnl": item["best_pnl"],
            "best_variant": item["best_variant"],
            "avg_quality_score": round(float(item["quality_sum"]) / max(1, kept), 4),
        })
    route_rows.sort(key=lambda r: (int(r["kept"]), float(r["best_pnl"])), reverse=True)
    hunter_rows = []
    for hunter, item in by_hunter.items():
        scored = int(item["scored_total"] or 0)
        winners = int(item["reported_winners"] or 0)
        hunter_rows.append({
            "hunter": hunter,
            "cycles": int(item["cycles"] or 0),
            "scored_total": scored,
            "reported_winners": winners,
            "reported_winner_yield_per_10k": round(winners / max(1, scored) * 10000.0, 4),
        })
    hunter_rows.sort(key=lambda r: r["reported_winner_yield_per_10k"], reverse=True)
    return {
        "schema_version": 1,
        "source": source or "step2_hunt_intelligence",
        "description": "Cycle-level experiment memory for learning which search choices produce live beaters.",
        "hunter_memory": hunter_rows,
        "route_memory": route_rows,
    }


def bandit_allocation(rows: list[dict[str, Any]], *, exploration_pct: float = 0.15, limit: int = 12) -> dict[str, Any]:
    clusters = route_clusters(rows, limit=limit)
    if not clusters:
        return {
            "schema_version": 1,
            "source": "step2_hunt_intelligence",
            "exploration_pct": exploration_pct,
            "arms": [],
            "allocation": [],
        }
    scores = []
    for cluster in clusters:
        reward = max(0.0, float(cluster.get("best_delta_vs_active") or 0.0))
        reward += int(cluster.get("alias_count") or cluster.get("count") or 0) * 75.0
        reward += max(0.0, float(cluster.get("avg_delta_vs_active") or 0.0)) * 0.35
        scores.append((cluster, reward))
    total_reward = sum(score for _, score in scores) or 1.0
    exploit_pct = max(0.0, 1.0 - float(exploration_pct))
    allocation = []
    for cluster, reward in scores:
        pct = exploit_pct * reward / total_reward
        allocation.append({
            "route_key": cluster.get("route_key"),
            "recommended_budget_pct": round(pct * 100.0, 2),
            "best_variant": cluster.get("best_variant"),
            "reward_score": round(reward, 4),
        })
    allocation.append({
        "route_key": "exploration",
        "recommended_budget_pct": round(float(exploration_pct) * 100.0, 2),
        "best_variant": None,
        "reward_score": 0.0,
    })
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "exploration_pct": exploration_pct,
        "arms": clusters,
        "allocation": allocation,
    }


def mutation_telemetry(rows: list[dict[str, Any]], *, active_weights: dict[str, float] | None = None, limit: int = 30) -> dict[str, Any]:
    active_weights = active_weights or {}
    deltas: dict[str, list[float]] = defaultdict(list)
    weighted: dict[str, float] = defaultdict(float)
    for row in rows:
        weights = row.get("weights") if isinstance(row.get("weights"), dict) else {}
        improvement = max(0.0, live_delta(row))
        for feature, value in weights.items():
            delta = float(value or 0.0) - float(active_weights.get(feature, 0.0) or 0.0)
            deltas[str(feature)].append(delta)
            weighted[str(feature)] += delta * max(1.0, improvement)
    feature_rows = []
    for feature, values in deltas.items():
        avg = sum(values) / max(1, len(values))
        feature_rows.append({
            "feature": feature,
            "samples": len(values),
            "avg_delta_vs_active_weight": round(avg, 6),
            "weighted_improvement_direction": round(weighted[feature] / max(1, len(values)), 6),
        })
    feature_rows.sort(key=lambda r: abs(float(r["weighted_improvement_direction"])), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "description": "Feature-direction telemetry among kept live-beating candidates.",
        "features": feature_rows[:limit],
    }


def failure_taxonomy(rows: list[dict[str, Any]], *, active_pnl: float | None = None) -> dict[str, Any]:
    counts: Counter[str] = Counter()
    examples: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        reasons = []
        if not beats_current_live(row, active_pnl):
            reasons.append("does_not_beat_live")
        if _concentration(row, "by_ticker") > 0.55:
            reasons.append("ticker_concentration")
        if _concentration(row, "by_side") > 0.70:
            reasons.append("side_concentration")
        if _positive_day_rate(row) < 0.75:
            reasons.append("weak_day_consistency")
        if trade_count(row) < 250:
            reasons.append("thin_trade_sample")
        if trade_count(row) > 20000:
            reasons.append("trade_count_too_high")
        robustness = row.get("robustness") if isinstance(row.get("robustness"), dict) else {}
        if float(robustness.get("holdout_delta_vs_active") or 0.0) < 0.0:
            reasons.append("holdout_does_not_beat_active")
        if route_key_from_row(row) == "unrouted":
            reasons.append("unrouted_candidate")
        for reason in reasons or ["unclassified"]:
            counts[reason] += 1
            if len(examples[reason]) < 5:
                examples[reason].append(str(row.get("variant") or ""))
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "failure_counts": dict(counts),
        "examples": dict(examples),
    }


def curriculum_state(rows: list[dict[str, Any]], cycles: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    cycles = cycles or []
    clusters = route_clusters(rows, limit=5)
    behavior_count = len(rows)
    top_delta = max([live_delta(row) for row in rows] or [0.0])
    if behavior_count < 10:
        phase = "broad_route_discovery"
    elif clusters and int(clusters[0].get("alias_count") or clusters[0].get("count") or 0) >= 5:
        phase = "route_local_refinement"
    elif len(clusters) >= 3:
        phase = "sibling_expansion"
    elif top_delta > 0:
        phase = "robustness_pressure"
    else:
        phase = "broad_route_discovery"
    if behavior_count >= 20 and top_delta > 0:
        next_phase = "finalist_validation"
    elif phase == "route_local_refinement":
        next_phase = "sibling_expansion"
    elif phase == "sibling_expansion":
        next_phase = "robustness_pressure"
    else:
        next_phase = phase
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "current_phase": phase,
        "recommended_next_phase": next_phase,
        "behavior_unique_live_beaters": behavior_count,
        "cycles": len(cycles),
        "top_delta_vs_active": top_delta,
        "top_route_clusters": clusters,
    }


def why_won_report(rows: list[dict[str, Any]], *, limit: int = 10) -> dict[str, Any]:
    reports = []
    for row in sorted(rows, key=pnl, reverse=True)[:limit]:
        by_day = row.get("by_day") if isinstance(row.get("by_day"), dict) else {}
        day_rows = []
        for day, payload in by_day.items():
            if isinstance(payload, dict):
                day_rows.append((str(day), float(payload.get("pnl") or 0.0), int(payload.get("trades") or 0)))
        day_rows.sort(key=lambda item: item[1], reverse=True)
        by_ticker = row.get("by_ticker") if isinstance(row.get("by_ticker"), dict) else {}
        by_side = row.get("by_side") if isinstance(row.get("by_side"), dict) else {}
        reports.append({
            "variant": row.get("variant"),
            "route_key": route_key_from_row(row),
            "step2_pnl": pnl(row),
            "delta_vs_active": live_delta(row),
            "trades": trade_count(row),
            "win_rate_pct": win_rate(row),
            "top_days": [{"day": d, "pnl": p, "trades": t} for d, p, t in day_rows[:3]],
            "worst_days": [{"day": d, "pnl": p, "trades": t} for d, p, t in sorted(day_rows, key=lambda item: item[1])[:3]],
            "by_ticker": by_ticker,
            "by_side": by_side,
            "route_match": route_match(row),
            "alias_count": int(row.get("behavior_alias_count") or 1),
            "novelty_score": row.get("novelty_score"),
        })
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "reports": reports,
    }


def stop_continue_criteria(rows: list[dict[str, Any]], cycles: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    cycles = cycles or []
    recent = cycles[-20:]
    recent_winners = sum(_cycle_winners(cycle) for cycle in recent)
    clusters = route_clusters(rows, limit=10)
    exhausted = []
    continue_routes = []
    for cluster in clusters:
        alias_count = int(cluster.get("alias_count") or cluster.get("count") or 0)
        avg_delta = float(cluster.get("avg_delta_vs_active") or 0.0)
        if alias_count >= 6 and avg_delta < 250.0:
            exhausted.append({
                "route_key": cluster.get("route_key"),
                "reason": "many_aliases_low_incremental_delta",
                "alias_count": alias_count,
                "avg_delta_vs_active": avg_delta,
            })
        else:
            continue_routes.append(cluster.get("route_key"))
    decision = "continue"
    if len(cycles) >= 20 and recent_winners == 0:
        decision = "stop_or_expand_search_space"
    elif exhausted and clusters and exhausted[0].get("route_key") == clusters[0].get("route_key"):
        decision = "shrink_or_rotate_primary_basin"
    return {
        "schema_version": 1,
        "source": "step2_hunt_intelligence",
        "decision": decision,
        "recent_cycles": len(recent),
        "recent_reported_winners": recent_winners,
        "exhausted_or_shrink_routes": exhausted,
        "continue_routes": continue_routes[:10],
    }


def learning_report(
    *,
    rows: list[dict[str, Any]],
    cycles: list[dict[str, Any]] | None = None,
    all_rows: list[dict[str, Any]] | None = None,
    active_weights: dict[str, float] | None = None,
    feedback_rows: list[dict[str, Any]] | None = None,
    validation_rows: list[dict[str, Any]] | None = None,
    source: str = "",
) -> dict[str, Any]:
    cycles = cycles or []
    all_rows = all_rows or rows
    adjusted_rows = apply_validation_adjustments(rows, validation_rows or [])
    experiment_plan = active_experiment_planner(adjusted_rows, feedback_rows)
    treatment_effects = treatment_effect_analyzer(adjusted_rows, feedback_rows, experiment_plan=experiment_plan)
    treatment_priors = treatment_prior_model(treatment_effects)
    treatment_workers = treatment_worker_budget(treatment_effects)
    treatment_confidence = treatment_confidence_model(treatment_effects)
    sibling_experiments = controlled_parent_sibling_experiments(adjusted_rows, treatment_confidence)
    failure_autopsy = live_beater_failure_autopsy(adjusted_rows, feedback_rows)
    worker_memory = worker_specialization_memory(adjusted_rows, treatment_effects)
    regime_learning = regime_aware_learning(adjusted_rows, feedback_rows)
    reject_simulator = promotion_reject_simulator(adjusted_rows, feedback_rows)
    portfolio = search_portfolio_manager(adjusted_rows, treatment_confidence, failure_autopsy, regime_learning)
    outcome_ledger = experiment_outcome_ledger(experiment_plan, adjusted_rows, feedback_rows)
    registry = causal_experiment_registry(experiment_plan, sibling_experiments, outcome_ledger, treatment_confidence)
    debt_queue = experiment_debt_queue(registry, treatment_confidence, regime_learning, failure_autopsy)
    info_gain = information_gain_scoring(adjusted_rows, debt_queue, treatment_confidence, regime_learning)
    voi = value_of_information_planner(info_gain, debt_queue, treatment_confidence)
    decision_changes = decision_change_tracker(treatment_confidence, registry, portfolio)
    hypothesis_quality = hypothesis_quality_scoring(registry)
    evidence_gate = evidence_sufficiency_gate(treatment_effects, treatment_confidence)
    shadow_board = counterfactual_shadow_board(adjusted_rows)
    calibration_ledger = prediction_calibration_ledger(adjusted_rows, feedback_rows, reject_simulator, treatment_confidence, voi)
    belief_revision = belief_revision_engine(calibration_ledger)
    ood_detector = out_of_distribution_detector(adjusted_rows, regime_learning)
    red_team = adversarial_red_team_learner(adjusted_rows, ood_detector, treatment_effects)
    memory_distillation = memory_compression_distiller(treatment_effects, regime_learning, failure_autopsy, debt_queue, calibration_ledger)
    truth_first = truth_first_promotion_objective(adjusted_rows, belief_revision, ood_detector, evidence_gate)
    meta_strategy = meta_hunt_strategy_learner(cycles, adjusted_rows, portfolio)
    partial_learning = {
        "treatment_effects": treatment_effects,
        "treatment_confidence_model": treatment_confidence,
        "experiment_debt_queue": debt_queue,
        "information_gain_scoring": info_gain,
    }
    postmortem = run_to_run_postmortem_synthesizer(adjusted_rows, cycles, partial_learning)
    velocity = learning_velocity_dashboard(adjusted_rows, decision_changes, debt_queue, info_gain, postmortem)
    self_audit = self_audit_score(calibration_ledger, debt_queue, evidence_gate, velocity)
    compiled_policy = policy_compiler(
        treatment_prior_model=treatment_priors,
        treatment_worker_budget=treatment_workers,
        search_portfolio=portfolio,
        evidence_gate=evidence_gate,
        value_of_information=voi,
        shadow_board=shadow_board,
        experiment_debt=debt_queue,
        active_experiment_plan=experiment_plan,
    )
    compiled_policy["objective"] = "maximize_truth_first_promotable_pnl"
    compiled_policy["truth_first_leaderboard"] = truth_first.get("leaderboard") or []
    compiled_policy["calibration_confidence"] = belief_revision.get("truth_calibration_confidence")
    compiled_policy["self_audit_score"] = self_audit.get("self_audit_score")
    policy_exec = policy_executor(compiled_policy)
    worker_assignment = adaptive_worker_assignment(policy_exec, worker_memory)
    backtest = policy_backtester(adjusted_rows, compiled_policy)
    policy_mutations = policy_mutation_engine(compiled_policy)
    tournament = policy_tournament(adjusted_rows, policy_mutations)
    champion_memory = champion_challenger_memory(tournament)
    regime_policies = regime_specific_policies(regime_learning, tournament)
    safety = policy_safety_rail(compiled_policy)
    learning_graph = causal_graph_of_learning(compiled_policy, policy_exec, adjusted_rows, calibration_ledger)
    field_manual = auto_promoted_field_manual(memory_distillation, treatment_effects, tournament)
    failure_memory = failure_memory_feedback(feedback_rows)
    policy_drift = policy_drift_detector(cycles, tournament, calibration_ledger, truth_first, ood_detector, feedback_rows)
    half_life = route_regime_half_life(adjusted_rows, regime_learning, treatment_effects)
    market_map = learning_market_map(adjusted_rows, treatment_effects, regime_learning, worker_assignment, failure_memory)
    drift_alarms = concept_drift_alarms(policy_drift, half_life, treatment_effects, failure_memory, tournament)
    revalidation = revalidation_scheduler(field_manual, half_life, drift_alarms, tournament)
    temporal_ensemble = temporal_ensemble_policy(tournament, champion_memory, regime_policies, policy_drift, revalidation)
    experiment_governor = active_experiment_governor(cycles, experiment_plan, outcome_ledger, policy_drift, market_map, revalidation)
    route_states = route_state_machine(adjusted_rows, half_life, market_map, failure_memory, drift_alarms)
    negative_bank = negative_knowledge_bank(adjusted_rows, feedback_rows, failure_memory, route_states, drift_alarms)
    survivor_model = promotion_survivor_model(adjusted_rows, feedback_rows, reject_simulator, truth_first, negative_bank)
    mutation_grammar = mutation_grammar_learner(adjusted_rows, treatment_effects, market_map, route_states)
    worker_rebalance = real_time_worker_rebalancer(cycles, worker_memory, market_map, experiment_governor)
    replay_sim = hunt_replay_simulator(adjusted_rows, temporal_ensemble, route_states, negative_bank, survivor_model)
    resurrection = resurrection_engine(adjusted_rows, failure_autopsy, survivor_model, negative_bank)
    lineage = candidate_lineage_graph(adjusted_rows, feedback_rows)
    causal_mutations = causal_mutation_attribution(adjusted_rows, mutation_grammar, lineage, treatment_effects)
    uncertainty_budget = uncertainty_budgeting(half_life, treatment_confidence, policy_drift, market_map, route_states)
    pareto_frontier = promotability_pareto_frontier(adjusted_rows, survivor_model)
    false_lessons = false_lesson_detector(adjusted_rows, cycles, route_states, ood_detector, half_life)
    experiment_graduation = experiment_graduation_system(registry, outcome_ledger, experiment_governor, evidence_gate)
    genealogy_diffs = candidate_genealogy_diff_engine(adjusted_rows, lineage)
    off_policy = off_policy_hunt_evaluator(adjusted_rows, policy_mutations, survivor_model, negative_bank)
    league = self_competition_league(tournament, off_policy, replay_sim, treatment_effects, route_states)
    evidence_contracts = evidence_contract_engine(field_manual, experiment_graduation, route_states, league, half_life)
    quality_decomposition = live_beater_quality_decomposer(adjusted_rows, survivor_model, route_states, regime_learning, half_life)
    contradictions = contradiction_detector(route_states, false_lessons, survivor_model, uncertainty_budget, evidence_contracts, negative_bank, policy_drift)
    conflict_resolution = learning_conflict_resolver(contradictions, route_states, resurrection, revalidation, uncertainty_budget)
    cohort_memory = cohort_based_memory(adjusted_rows, survivor_model, failure_memory)
    throttle = adaptive_hunt_throttle(cycles, policy_drift, contradictions, uncertainty_budget, conflict_resolution)
    promotion_sim = promotion_readiness_simulator(adjusted_rows, survivor_model, quality_decomposition, contradictions, false_lessons)
    trace_ledger = research_trace_ledger(
        evidence_contracts=evidence_contracts,
        quality_decomposition=quality_decomposition,
        contradictions=contradictions,
        conflict_resolution=conflict_resolution,
        cohort_memory=cohort_memory,
        throttle=throttle,
        promotion_simulator=promotion_sim,
        league=league,
    )
    runtime_kernel = runtime_decision_kernel(conflict_resolution, throttle, route_states, negative_bank, mutation_grammar, revalidation, resurrection, worker_rebalance)
    action_outcomes = action_outcome_tracker(cycles, runtime_kernel, adjusted_rows)
    reward_model = closed_loop_reward_model(action_outcomes, cohort_memory, route_states)
    hunt_planner = autonomous_hunt_planner(runtime_kernel, reward_model, uncertainty_budget, worker_rebalance)
    guardrails = runtime_guardrails(runtime_kernel, negative_bank)
    command_ledger = command_replay_ledger(runtime_kernel, guardrails, action_outcomes, trace_ledger)
    action_league = action_elo_league(action_outcomes, reward_model)
    ab_executor = ab_route_experiment_executor(experiment_plan, outcome_ledger, worker_rebalance, runtime_kernel)
    champion_slots = champion_challenger_runtime_slots(runtime_kernel, action_league, ab_executor, reward_model)
    adaptive_stopping = adaptive_experiment_stopping(cycles, experiment_plan, outcome_ledger, experiment_governor)
    counterfactual_replay = counterfactual_command_replay(runtime_kernel, action_outcomes, adjusted_rows, cohort_memory)
    contamination_guard = experiment_contamination_guard(ab_executor, experiment_plan, runtime_kernel, negative_bank)
    learning_rate = learning_rate_controller(cycles, throttle, adaptive_stopping, reward_model, contamination_guard)
    worker_cards = worker_learning_report_cards(adjusted_rows, cycles, worker_memory, ab_executor, action_outcomes)
    promotion_trace = experiment_to_promotion_trace(adjusted_rows, experiment_plan, outcome_ledger, ab_executor, promotion_sim)
    coverage_map = search_space_coverage_map(adjusted_rows, cycles, ab_executor)
    alias_traps = alias_trap_detector(adjusted_rows, cycles)
    scarcity_mode = live_beater_scarcity_mode(cycles, adjusted_rows, coverage_map, alias_traps)
    seed_quality = route_seed_quality_score(cycles, adjusted_rows, alias_traps)
    opportunity_cost = opportunity_cost_meter(adjusted_rows, seed_quality, coverage_map, info_gain)
    stuck_breaker = stuck_loop_breaker(cycles, adjusted_rows)
    zero_autopsy = zero_yield_autopsy_engine(cycles, adjusted_rows, route_states, alias_traps, coverage_map)
    recovery_playbook = recovery_playbook_generator(zero_autopsy, stuck_breaker, opportunity_cost, coverage_map, scarcity_mode, alias_traps, seed_quality)
    hunt_brief = human_readable_hunt_brief(runtime_kernel, hunt_planner, guardrails, conflict_resolution, action_league)
    return {
        "schema_version": 1,
        "source": source or "step2_hunt_intelligence",
        "experiment_memory": experiment_memory(cycles, adjusted_rows, source=source),
        "bandit_allocation": bandit_allocation(adjusted_rows),
        "validation_aware_bandit": validation_aware_bandit(adjusted_rows, validation_rows or []),
        "auto_validation_scheduler": auto_validation_scheduler(adjusted_rows),
        "candidate_family_clusters": candidate_family_clustering(adjusted_rows),
        "candidate_lineage_graph": lineage,
        "family_rejection_memory": family_rejection_memory(feedback_rows),
        "active_experiment_plan": experiment_plan,
        "experiment_outcome_ledger": outcome_ledger,
        "causal_experiment_registry": registry,
        "experiment_debt_queue": debt_queue,
        "information_gain_scoring": info_gain,
        "value_of_information_planner": voi,
        "decision_change_tracker": decision_changes,
        "hypothesis_quality_scoring": hypothesis_quality,
        "evidence_sufficiency_gate": evidence_gate,
        "counterfactual_shadow_board": shadow_board,
        "prediction_calibration_ledger": calibration_ledger,
        "belief_revision_engine": belief_revision,
        "adversarial_red_team_learner": red_team,
        "out_of_distribution_detector": ood_detector,
        "memory_compression_distiller": memory_distillation,
        "self_audit_score": self_audit,
        "truth_first_promotion_objective": truth_first,
        "learning_velocity_dashboard": velocity,
        "compiled_hunt_policy": compiled_policy,
        "policy_executor": policy_exec,
        "adaptive_worker_assignment": worker_assignment,
        "policy_backtester": backtest,
        "policy_mutation_engine": policy_mutations,
        "policy_tournament": tournament,
        "champion_challenger_memory": champion_memory,
        "regime_specific_policies": regime_policies,
        "causal_graph_of_learning": learning_graph,
        "policy_safety_rail": safety,
        "auto_promoted_field_manual": field_manual,
        "policy_drift_detector": policy_drift,
        "route_regime_half_life": half_life,
        "learning_market_map": market_map,
        "concept_drift_alarms": drift_alarms,
        "revalidation_scheduler": revalidation,
        "temporal_ensemble_policy": temporal_ensemble,
        "active_experiment_governor": experiment_governor,
        "route_state_machine": route_states,
        "negative_knowledge_bank": negative_bank,
        "promotion_survivor_model": survivor_model,
        "mutation_grammar_learner": mutation_grammar,
        "real_time_worker_rebalancer": worker_rebalance,
        "hunt_replay_simulator": replay_sim,
        "resurrection_engine": resurrection,
        "causal_mutation_attribution": causal_mutations,
        "uncertainty_budgeting": uncertainty_budget,
        "promotability_pareto_frontier": pareto_frontier,
        "false_lesson_detector": false_lessons,
        "experiment_graduation_system": experiment_graduation,
        "candidate_genealogy_diff_engine": genealogy_diffs,
        "off_policy_hunt_evaluator": off_policy,
        "self_competition_league": league,
        "evidence_contract_engine": evidence_contracts,
        "live_beater_quality_decomposer": quality_decomposition,
        "contradiction_detector": contradictions,
        "learning_conflict_resolver": conflict_resolution,
        "cohort_based_memory": cohort_memory,
        "adaptive_hunt_throttle": throttle,
        "promotion_readiness_simulator": promotion_sim,
        "research_trace_ledger": trace_ledger,
        "runtime_decision_kernel": runtime_kernel,
        "action_outcome_tracker": action_outcomes,
        "closed_loop_reward_model": reward_model,
        "autonomous_hunt_planner": hunt_planner,
        "runtime_guardrails": guardrails,
        "command_replay_ledger": command_ledger,
        "action_elo_league": action_league,
        "human_readable_hunt_brief": hunt_brief,
        "ab_route_experiment_executor": ab_executor,
        "champion_challenger_runtime_slots": champion_slots,
        "adaptive_experiment_stopping": adaptive_stopping,
        "counterfactual_command_replay": counterfactual_replay,
        "experiment_contamination_guard": contamination_guard,
        "learning_rate_controller": learning_rate,
        "worker_learning_report_cards": worker_cards,
        "experiment_to_promotion_trace": promotion_trace,
        "zero_yield_autopsy_engine": zero_autopsy,
        "stuck_loop_breaker": stuck_breaker,
        "opportunity_cost_meter": opportunity_cost,
        "search_space_coverage_map": coverage_map,
        "live_beater_scarcity_mode": scarcity_mode,
        "alias_trap_detector": alias_traps,
        "route_seed_quality_score": seed_quality,
        "recovery_playbook_generator": recovery_playbook,
        "meta_hunt_strategy_learner": meta_strategy,
        "run_to_run_postmortem": postmortem,
        "treatment_effects": treatment_effects,
        "treatment_prior_model": treatment_priors,
        "treatment_worker_budget": treatment_workers,
        "treatment_confidence_model": treatment_confidence,
        "controlled_parent_sibling_experiments": sibling_experiments,
        "live_beater_failure_autopsy": failure_autopsy,
        "worker_specialization_memory": worker_memory,
        "regime_aware_learning": regime_learning,
        "promotion_reject_simulator": reject_simulator,
        "search_portfolio_manager": portfolio,
        "annotated_top100": annotated_top_payload(adjusted_rows, source=source),
        "near_miss_archive": near_miss_archive(adjusted_rows, source=source),
        "adaptive_mutation_lanes": adaptive_mutation_lanes(adjusted_rows, feedback_rows),
        "worker_role_plan": worker_role_plan(adjusted_rows),
        "mutation_telemetry": mutation_telemetry(adjusted_rows, active_weights=active_weights),
        "feature_interactions": feature_interaction_mining(adjusted_rows),
        "failure_taxonomy": failure_taxonomy(all_rows),
        "curriculum_state": curriculum_state(adjusted_rows, cycles),
        "why_won_report": why_won_report(adjusted_rows),
        "stop_continue_criteria": stop_continue_criteria(adjusted_rows, cycles),
        "counterfactual_route_attribution_plan": counterfactual_route_attribution_plan(adjusted_rows),
        "temporal_generalization_map": temporal_generalization_map(adjusted_rows),
        "adversarial_perturbation_plan": adversarial_perturbation_plan(adjusted_rows),
        "diversity_constrained_finalists": diversity_constrained_finalists(adjusted_rows),
        "promotion_feedback": promotion_feedback_summary(feedback_rows),
        "failure_memory_feedback": failure_memory,
    }
