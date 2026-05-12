"""Statistical validation helpers for Step 2 candidates.

This layer treats every hunt as a multiple-comparison experiment. A candidate
can still be useful research material when it fails here, but promotion-grade
learning should prefer candidates whose edge survives day rotation, simple
confidence bounds, concentration checks, and search-pressure adjustment.
"""
from __future__ import annotations

import math
from typing import Any


VALIDATION_VERSION = "step2_statistical_validation_v2"


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            return default
        return number
    except Exception:
        return default


def _trade_count(row: dict[str, Any]) -> int:
    return int(_num(row.get("step2_trades") or row.get("trades"), 0.0))


def _pnl(row: dict[str, Any]) -> float:
    return _num(row.get("step2_pnl") or row.get("pnl"), 0.0)


def _bucket_values(row: dict[str, Any], key: str) -> list[float]:
    payload = row.get(key) if isinstance(row.get(key), dict) else {}
    values = []
    for value in payload.values():
        if isinstance(value, dict):
            values.append(_num(value.get("pnl"), 0.0))
        else:
            values.append(_num(value, 0.0))
    return values


def _day_rows(row: dict[str, Any]) -> list[dict[str, Any]]:
    by_day = row.get("by_day") if isinstance(row.get("by_day"), dict) else {}
    rows = []
    for day, payload in by_day.items():
        if not isinstance(payload, dict):
            continue
        rows.append({
            "day": str(day),
            "pnl": _num(payload.get("pnl"), 0.0),
            "trades": int(_num(payload.get("trades"), 0.0)),
        })
    return sorted(rows, key=lambda item: item["day"])


def _normal_one_sided_p(z_score: float) -> float:
    return 0.5 * math.erfc(float(z_score) / math.sqrt(2.0))


def _concentration(values: list[float]) -> float:
    total = sum(abs(value) for value in values)
    if total <= 0.0 or not values:
        return 0.0
    return max(abs(value) for value in values) / total


def candidate_validation(
    row: dict[str, Any],
    *,
    tested_count: int = 1,
    min_trades: int = 100,
    min_days: int = 3,
) -> dict[str, Any]:
    days = _day_rows(row)
    day_pnls = [item["pnl"] for item in days]
    pnl = _pnl(row)
    trades = _trade_count(row)
    day_count = len(day_pnls)
    positive_days = sum(1 for value in day_pnls if value > 0.0)
    positive_day_rate = positive_days / max(1, day_count)
    total = sum(day_pnls) if day_pnls else pnl
    mean = total / max(1, day_count)
    variance = (
        sum((value - mean) ** 2 for value in day_pnls) / max(1, day_count - 1)
        if day_count >= 2
        else 0.0
    )
    stddev = math.sqrt(max(0.0, variance))
    standard_error = stddev / math.sqrt(day_count) if day_count else 0.0
    lower_90 = mean - 1.28155 * standard_error if day_count >= 2 else mean
    lower_95 = mean - 1.64485 * standard_error if day_count >= 2 else mean
    z_score = mean / standard_error if standard_error > 0.0 else (10.0 if mean > 0.0 else 0.0)
    raw_p = _normal_one_sided_p(z_score)
    adjusted_p = min(1.0, raw_p * max(1, int(tested_count or 1)))
    leave_one = []
    if day_count >= 2:
        for item in days:
            leave_one.append({
                "held_out_day": item["day"],
                "remaining_pnl": round(total - item["pnl"], 6),
            })
    leave_one_min = min((item["remaining_pnl"] for item in leave_one), default=None)
    ticker_concentration = _concentration(_bucket_values(row, "by_ticker"))
    side_concentration = _concentration(_bucket_values(row, "by_side"))
    holdout = row.get("holdout_gate") if isinstance(row.get("holdout_gate"), dict) else {}
    holdout_pnl = _num(holdout.get("holdout_pnl"), 0.0)
    research_checks = {
        "positive_total_pnl": pnl > 0.0,
        "min_trade_sample": trades >= int(min_trades),
        "min_day_sample": day_count >= int(min_days),
        "positive_day_rate": positive_day_rate >= 0.60,
        "lower_90_day_mean_positive": lower_90 > 0.0,
        "leave_one_day_profitable": leave_one_min is not None and leave_one_min > 0.0,
        "ticker_not_concentrated": ticker_concentration <= 0.75,
        "side_not_concentrated": side_concentration <= 0.85,
        "holdout_positive_if_present": holdout_pnl > 0.0 if holdout else True,
        "multiple_testing_adjusted": adjusted_p <= 0.20,
    }
    promotion_checks = {
        **research_checks,
        "promotion_min_trade_sample": trades >= max(int(min_trades), int(min_trades) * 2),
        "promotion_min_day_sample": day_count >= max(int(min_days), 10),
        "promotion_positive_day_rate": positive_day_rate >= 0.70,
        "lower_95_day_mean_positive": lower_95 > 0.0,
        "multiple_testing_adjusted_strict": adjusted_p <= 0.05,
    }
    research_blockers = [name for name, ok in research_checks.items() if not ok]
    promotion_blockers = [name for name, ok in promotion_checks.items() if not ok]
    score = 100.0
    score -= 12.0 if not research_checks["min_trade_sample"] else 0.0
    score -= 14.0 if not research_checks["min_day_sample"] else 0.0
    score -= max(0.0, 0.80 - positive_day_rate) * 30.0
    score -= 12.0 if lower_90 <= 0.0 else 0.0
    score -= 12.0 if leave_one_min is None or leave_one_min <= 0.0 else 0.0
    score -= max(0.0, ticker_concentration - 0.55) * 45.0
    score -= max(0.0, side_concentration - 0.70) * 35.0
    score -= 15.0 if adjusted_p > 0.20 else 0.0
    score -= 16.0 if holdout and holdout_pnl <= 0.0 else 0.0
    score = max(0.0, min(100.0, score))
    return {
        "schema_version": 1,
        "validation_version": VALIDATION_VERSION,
        "variant": row.get("variant"),
        "tested_count": int(max(1, tested_count or 1)),
        "pnl": round(pnl, 6),
        "trades": trades,
        "day_count": day_count,
        "positive_day_count": positive_days,
        "positive_day_rate": round(positive_day_rate, 6),
        "mean_day_pnl": round(mean, 6),
        "stddev_day_pnl": round(stddev, 6),
        "standard_error_day_pnl": round(standard_error, 6),
        "lower_90_day_mean_pnl": round(lower_90, 6),
        "lower_95_day_mean_pnl": round(lower_95, 6),
        "z_score": round(z_score, 6),
        "raw_p_value": round(raw_p, 8),
        "multiple_testing_adjusted_p_value": round(adjusted_p, 8),
        "leave_one_day_min_pnl": round(leave_one_min, 6) if leave_one_min is not None else None,
        "ticker_concentration": round(ticker_concentration, 6),
        "side_concentration": round(side_concentration, 6),
        "holdout_pnl": round(holdout_pnl, 6) if holdout else None,
        "checks": promotion_checks,
        "research_checks": research_checks,
        "promotion_checks": promotion_checks,
        "research_blockers": research_blockers,
        "promotion_blockers": promotion_blockers,
        "blockers": promotion_blockers,
        "score": round(score, 4),
        "promotion_grade": bool(score >= 75.0 and not promotion_blockers),
        "learning_grade": bool(score >= 50.0 and research_checks["positive_total_pnl"] and not research_blockers),
        "research_grade": bool(score >= 50.0 and research_checks["positive_total_pnl"]),
        "promotion_gate_note": "Promotion-grade validation uses stricter day count, 95% lower bound, 70% positive-day rate, and adjusted p <= 0.05; research-grade signals can remain exploratory.",
    }


def report(
    rows: list[dict[str, Any]],
    *,
    min_trades: int = 100,
    min_days: int = 3,
    limit: int = 50,
    source: str = "step2",
) -> dict[str, Any]:
    tested = len(rows)
    validations = [
        row.get("statistical_validation")
        if isinstance(row.get("statistical_validation"), dict)
        else candidate_validation(row, tested_count=tested, min_trades=min_trades, min_days=min_days)
        for row in rows
    ]
    validations.sort(key=lambda item: (float(item.get("score") or 0.0), float(item.get("pnl") or 0.0)), reverse=True)
    blocker_counts: dict[str, int] = {}
    for validation in validations:
        for blocker in validation.get("blockers") or []:
            blocker_counts[str(blocker)] = blocker_counts.get(str(blocker), 0) + 1
    return {
        "schema_version": 1,
        "validation_version": VALIDATION_VERSION,
        "source": source,
        "candidate_count": tested,
        "promotion_grade_count": sum(1 for item in validations if item.get("promotion_grade")),
        "learning_grade_count": sum(1 for item in validations if item.get("learning_grade")),
        "blocker_counts": blocker_counts,
        "leaderboard": validations[:limit],
    }
