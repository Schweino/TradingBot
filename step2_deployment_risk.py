"""Deployment risk controls for Step 2 candidates.

This module is intentionally conservative. It turns learning evidence into a
tradeability contract: whether a candidate is live-eligible, paper-only, or
blocked, and how much capital/exposure it may receive if it progresses.
"""
from __future__ import annotations

from typing import Any


RISK_VERSION = "step2_deployment_risk_v1"
DEFAULT_START_BALANCE = 100000.0


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _trade_count(row: dict[str, Any]) -> int:
    return int(_num(row.get("step2_trades") or row.get("trades"), 0.0))


def _pnl(row: dict[str, Any]) -> float:
    return _num(row.get("step2_pnl") or row.get("pnl"), 0.0)


def _concentration(row: dict[str, Any], key: str) -> float:
    payload = row.get(key) if isinstance(row.get(key), dict) else {}
    values = []
    for item in payload.values():
        if isinstance(item, dict):
            values.append(abs(_num(item.get("pnl"), 0.0)))
        else:
            values.append(abs(_num(item, 0.0)))
    total = sum(values)
    return max(values) / total if total > 0.0 and values else 0.0


def _daily_drawdown(row: dict[str, Any]) -> float:
    by_day = row.get("by_day") if isinstance(row.get("by_day"), dict) else {}
    peak = 0.0
    cumulative = 0.0
    max_dd = 0.0
    for day in sorted(by_day):
        payload = by_day.get(day) if isinstance(by_day.get(day), dict) else {}
        cumulative += _num(payload.get("pnl"), 0.0)
        peak = max(peak, cumulative)
        max_dd = max(max_dd, peak - cumulative)
    return max_dd


def candidate_risk(
    row: dict[str, Any],
    *,
    start_balance: float = DEFAULT_START_BALANCE,
    closed_loop_decision: dict[str, Any] | None = None,
) -> dict[str, Any]:
    row = row or {}
    blockers: list[str] = []
    warnings: list[str] = []
    objective = row.get("execution_adjusted_objective") if isinstance(row.get("execution_adjusted_objective"), dict) else {}
    replay = row.get("replay_validity") if isinstance(row.get("replay_validity"), dict) else objective.get("replay_validity") if isinstance(objective.get("replay_validity"), dict) else {}
    data_quality = row.get("data_quality_contract") if isinstance(row.get("data_quality_contract"), dict) else {}
    stats = row.get("statistical_validation") if isinstance(row.get("statistical_validation"), dict) else {}
    closed_loop_decision = closed_loop_decision or {}
    trades = _trade_count(row)
    pnl = _pnl(row)
    adjusted = _num(row.get("execution_adjusted_pnl") or objective.get("execution_adjusted_pnl"), pnl)
    ticker_conc = _concentration(row, "by_ticker")
    side_conc = _concentration(row, "by_side")
    drawdown = _daily_drawdown(row)
    drawdown_pct = drawdown / max(1.0, float(start_balance)) * 100.0
    overfit = _num(row.get("overfit_risk_score"), 0.0)
    if data_quality and data_quality.get("promotion_allowed") is False:
        blockers.append("data_quality_not_promotion_allowed")
    if replay and replay.get("ok") is False:
        blockers.append("replay_validity_failed")
    if stats and stats.get("promotion_grade") is False:
        blockers.append("statistical_validation_not_promotion_grade")
    if adjusted <= 0.0:
        blockers.append("execution_adjusted_pnl_not_positive")
    if closed_loop_decision.get("decision") in {"retire_or_quarantine", "repair_before_more_budget"}:
        blockers.append(f"closed_loop_{closed_loop_decision.get('decision')}")
    if trades < 100:
        warnings.append("thin_trade_sample")
    if ticker_conc > 0.75:
        warnings.append("ticker_concentration_high")
    if side_conc > 0.85:
        warnings.append("side_concentration_high")
    if drawdown_pct > 2.0:
        warnings.append("daily_drawdown_above_2pct")
    if overfit >= 65.0:
        warnings.append("overfit_risk_high")
    score = 100.0
    score -= 30.0 * len(blockers)
    score -= 7.0 * len(warnings)
    score -= max(0.0, ticker_conc - 0.55) * 35.0
    score -= max(0.0, side_conc - 0.70) * 25.0
    score -= min(20.0, drawdown_pct * 4.0)
    score -= min(18.0, overfit * 0.18)
    score += min(12.0, max(0.0, adjusted) / max(1.0, float(start_balance)) * 100.0)
    score = max(0.0, min(100.0, score))
    if blockers:
        tier = "blocked"
    elif score >= 82.0 and not warnings:
        tier = "live_eligible"
    elif score >= 62.0:
        tier = "paper_trade"
    else:
        tier = "shadow_only"
    max_capital_pct = 0.0
    if tier == "live_eligible":
        max_capital_pct = min(5.0, 1.0 + score / 25.0)
    elif tier == "paper_trade":
        max_capital_pct = min(1.0, score / 100.0)
    kill_switches = [
        {"name": "daily_loss_stop", "threshold_pct_of_start_balance": 0.75 if tier == "live_eligible" else 0.25},
        {"name": "consecutive_losing_days_stop", "threshold": 2},
        {"name": "quote_aware_replay_required", "threshold": True},
        {"name": "void_registry_recheck_before_enable", "threshold": True},
    ]
    return {
        "schema_version": 1,
        "risk_version": RISK_VERSION,
        "variant": row.get("variant"),
        "tier": tier,
        "deployment_allowed": tier == "live_eligible",
        "paper_trade_allowed": tier in {"live_eligible", "paper_trade"},
        "risk_score": round(score, 4),
        "max_capital_pct": round(max_capital_pct, 4),
        "max_notional": round(float(start_balance) * max_capital_pct / 100.0, 4),
        "blockers": blockers,
        "warnings": warnings,
        "metrics": {
            "step2_pnl": round(pnl, 6),
            "execution_adjusted_pnl": round(adjusted, 6),
            "trades": trades,
            "ticker_concentration": round(ticker_conc, 6),
            "side_concentration": round(side_conc, 6),
            "daily_drawdown": round(drawdown, 6),
            "daily_drawdown_pct": round(drawdown_pct, 6),
            "overfit_risk_score": round(overfit, 6),
        },
        "exposure_caps": {
            "max_ticker_exposure_pct": 35.0 if tier == "live_eligible" else 15.0,
            "max_side_exposure_pct": 55.0 if tier == "live_eligible" else 25.0,
            "one_open_position_per_ticker": True,
            "max_trades_per_day": 4 if tier == "live_eligible" else 1,
        },
        "kill_switches": kill_switches,
    }


def report(
    rows: list[dict[str, Any]],
    *,
    start_balance: float = DEFAULT_START_BALANCE,
    closed_loop_controller: dict[str, Any] | None = None,
    limit: int = 50,
    source: str = "step2",
) -> dict[str, Any]:
    decisions = {}
    route_controller = (closed_loop_controller or {}).get("route_controller") if isinstance((closed_loop_controller or {}).get("route_controller"), dict) else {}
    for item in route_controller.get("decisions") or []:
        if isinstance(item, dict) and item.get("route_key"):
            decisions[str(item["route_key"])] = item
    risks = []
    for row in rows:
        route = str(row.get("route_key") or "")
        risks.append(
            candidate_risk(row, start_balance=start_balance, closed_loop_decision=decisions.get(route))
            if decisions
            else row.get("deployment_risk")
            if isinstance(row.get("deployment_risk"), dict)
            else candidate_risk(row, start_balance=start_balance, closed_loop_decision=decisions.get(route))
        )
    risks.sort(key=lambda item: (float(item.get("risk_score") or 0.0), float((item.get("metrics") or {}).get("execution_adjusted_pnl") or 0.0)), reverse=True)
    tier_counts: dict[str, int] = {}
    blocker_counts: dict[str, int] = {}
    for item in risks:
        tier = str(item.get("tier") or "unknown")
        tier_counts[tier] = tier_counts.get(tier, 0) + 1
        for blocker in item.get("blockers") or []:
            blocker_counts[str(blocker)] = blocker_counts.get(str(blocker), 0) + 1
    return {
        "schema_version": 1,
        "risk_version": RISK_VERSION,
        "source": source,
        "candidate_count": len(rows),
        "tier_counts": tier_counts,
        "blocker_counts": blocker_counts,
        "live_eligible_count": tier_counts.get("live_eligible", 0),
        "paper_trade_count": tier_counts.get("paper_trade", 0),
        "top": risks[:limit],
        "deployment_queue": [row for row in risks if row.get("deployment_allowed")][:limit],
        "paper_trade_queue": [row for row in risks if row.get("paper_trade_allowed") and not row.get("deployment_allowed")][:limit],
        "global_kill_switches": [
            "disable_if_void_registry_flags_source_day",
            "disable_if_quote_aware_exit_replay_missing",
            "disable_if_statistical_validation_regresses",
            "disable_if_live_daily_loss_exceeds_candidate_cap",
        ],
    }
