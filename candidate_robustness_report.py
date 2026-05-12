"""Report-only robustness scoring for Step 2 candidates.

The adaptive hunter can find a high raw P/L quickly. This module adds the next
question: did the candidate win broadly, or did one day/ticker/side carry it?
It is intentionally not a promotion gate. It produces fast, deterministic
context for human review and downstream evidence packets.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import baseline_drift_sentinel
import candidate_profile_schema
import tournament_safety


HERE = Path(__file__).resolve().parent
OUT_DIR = HERE / "postmortem" / "candidate_robustness"
CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


def _read_json(path: str | os.PathLike[str]) -> Any:
    with open(path, "r", encoding="utf-8-sig") as f:
        return json.load(f)


def _write_json(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.{int(time.time() * 1000)}.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
        f.write("\n")
    os.replace(tmp, path)
    return str(path.resolve())


def _stable_hash(payload: Any) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            return default
        return number
    except Exception:
        return default


def _int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(value)
    except Exception:
        return default


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(value)))


def _decision(row: dict[str, Any]) -> dict[str, Any]:
    if isinstance(row.get("decision_full"), dict):
        return row["decision_full"]
    if isinstance(row.get("result"), dict):
        return row["result"]
    score = row.get("score") if isinstance(row.get("score"), dict) else {}
    if isinstance(score.get("result"), dict):
        return score["result"]
    step2 = row.get("step2") if isinstance(row.get("step2"), dict) else {}
    return step2 if isinstance(step2, dict) else {}


def _pnl(row: dict[str, Any]) -> float:
    value = candidate_profile_schema.score(row)
    if value != float("-inf"):
        return round(float(value), 6)
    decision = _decision(row)
    return round(_num(decision.get("pnl")), 6)


def _trades(row: dict[str, Any]) -> int:
    decision = _decision(row)
    for value in (row.get("step2_trades"), row.get("trades"), decision.get("trades")):
        if value is not None:
            return _int(value)
    return 0


def _win_rate(row: dict[str, Any]) -> float | None:
    decision = _decision(row)
    for value in (row.get("step2_win_rate_pct"), row.get("win_rate_pct"), decision.get("win_rate_pct")):
        if value is not None:
            return round(_num(value), 4)
    return None


def _nested_summary(row: dict[str, Any], key: str) -> dict[str, dict[str, Any]]:
    decision = _decision(row)
    value = row.get(key)
    if not isinstance(value, dict):
        value = decision.get(key)
    if not isinstance(value, dict):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for name, raw in sorted(value.items()):
        raw = raw if isinstance(raw, dict) else {"pnl": raw}
        out[str(name)] = {
            "pnl": round(_num(raw.get("pnl")), 6),
            "trades": _int(raw.get("trades")),
            "wins": _int(raw.get("wins")) if raw.get("wins") is not None else None,
            "losses": _int(raw.get("losses")) if raw.get("losses") is not None else None,
        }
    return out


def _variant_name(row: dict[str, Any]) -> str:
    return str(row.get("variant") or row.get("name") or "candidate")


def _model_id(row: dict[str, Any]) -> str:
    routes = row.get("routes") if isinstance(row.get("routes"), list) else []
    if routes:
        return str(row.get("model_id") or tournament_safety.stable_json_hash({
            "name": _variant_name(row),
            "weights": row.get("weights") or {},
            "bias": round(float(row.get("bias") or 0.0), 8),
            "routes": routes,
        }, length=20))
    return str(row.get("model_id") or tournament_safety.model_id(
        _variant_name(row),
        row.get("weights") or {},
        float(row.get("bias") or 0.0),
    ))


def _split_days(days: list[str]) -> tuple[list[str], list[str]]:
    if len(days) < 2:
        return days, []
    holdout_count = max(1, int(math.ceil(len(days) * 0.30)))
    if len(days) - holdout_count < 1:
        holdout_count = 1
    return days[:-holdout_count], days[-holdout_count:]


def _sum_named(rows: dict[str, dict[str, Any]], names: list[str], field: str = "pnl") -> float:
    return round(sum(_num((rows.get(name) or {}).get(field)) for name in names), 6)


def _max_drawdown(day_rows: list[dict[str, Any]]) -> dict[str, Any]:
    peak = 0.0
    cumulative = 0.0
    max_dd = 0.0
    trough_day = None
    equity_curve = []
    for row in day_rows:
        cumulative += _num(row.get("candidate_pnl"))
        peak = max(peak, cumulative)
        dd = peak - cumulative
        if dd > max_dd:
            max_dd = dd
            trough_day = row.get("day")
        equity_curve.append({"day": row.get("day"), "cumulative_pnl": round(cumulative, 6)})
    return {
        "max_drawdown": round(max_dd, 6),
        "trough_day": trough_day,
        "ending_cumulative_pnl": round(cumulative, 6),
        "equity_curve": equity_curve,
    }


def _concentration(rows: dict[str, dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "top_abs_share": None,
            "top_abs_name": None,
            "top_positive_share": None,
            "top_positive_name": None,
        }
    abs_total = sum(abs(_num(row.get("pnl"))) for row in rows.values())
    positive_total = sum(max(0.0, _num(row.get("pnl"))) for row in rows.values())
    top_abs_name = max(rows, key=lambda key: abs(_num(rows[key].get("pnl"))))
    positive_rows = {key: row for key, row in rows.items() if _num(row.get("pnl")) > 0.0}
    top_positive_name = max(positive_rows, key=lambda key: _num(positive_rows[key].get("pnl"))) if positive_rows else None
    return {
        "top_abs_share": round(abs(_num(rows[top_abs_name].get("pnl"))) / abs_total, 6) if abs_total else None,
        "top_abs_name": top_abs_name,
        "top_positive_share": (
            round(_num(rows[top_positive_name].get("pnl")) / positive_total, 6)
            if positive_total and top_positive_name else None
        ),
        "top_positive_name": top_positive_name,
    }


def _rate(count: int, total: int) -> float | None:
    return round(float(count) / float(total), 6) if total else None


def _score_component(value: float | None, weight: float) -> float:
    if value is None:
        return weight * 0.5
    return weight * _clamp(value)


def _grade(score: float) -> str:
    if score >= 85:
        return "A"
    if score >= 75:
        return "B"
    if score >= 65:
        return "C"
    if score >= 50:
        return "D"
    return "F"


def _hard_flag(flag: str) -> bool:
    return flag in {
        "does_not_beat_active_total",
        "holdout_does_not_beat_active",
        "holdout_not_profitable",
        "weak_day_consistency",
        "high_day_or_ticker_concentration",
        "low_trade_sample",
        "large_drawdown_vs_profit",
    }


def recommendation_from_report(report: dict[str, Any]) -> dict[str, Any]:
    robustness_score = _num(report.get("robustness_score"))
    delta_pct = report.get("delta_pct_vs_active")
    pnl = _num(report.get("step2_pnl"))
    active_pnl = report.get("active_step2_pnl")
    red_flags = [str(flag) for flag in (report.get("red_flags") or [])]
    blockers = [flag for flag in red_flags if _hard_flag(flag)]
    train_holdout = report.get("train_holdout") if isinstance(report.get("train_holdout"), dict) else {}
    day_profile = report.get("day_profile") if isinstance(report.get("day_profile"), dict) else {}
    if robustness_score < 70.0:
        blockers.append("robustness_score_below_70")
    if active_pnl is not None and _num(report.get("delta_vs_active")) <= 0.0:
        blockers.append("does_not_beat_active_total")
    elif active_pnl is None and pnl <= 0.0:
        blockers.append("not_profitable_total")
    if train_holdout.get("holdout_days") and _num(train_holdout.get("holdout_pnl")) <= 0.0:
        blockers.append("holdout_not_profitable")
    if day_profile.get("total_days") and _num(day_profile.get("positive_day_rate")) < 0.50:
        blockers.append("weak_positive_day_rate")
    blockers = sorted(set(blockers))

    delta_quality = 0.50
    if delta_pct is not None:
        delta_quality = 0.50 + (_clamp(_num(delta_pct), -25.0, 25.0) / 50.0)
    elif active_pnl is None:
        delta_quality = _clamp(pnl / 10000.0, 0.0, 1.0)
    delta_component = round(18.0 * _clamp(delta_quality), 6)
    red_flag_penalty = sum(8.0 if _hard_flag(flag) else 3.0 for flag in red_flags)
    adjusted = round(_clamp((robustness_score * 0.82) + delta_component - red_flag_penalty, 0.0, 100.0), 6)
    recommended = not blockers and adjusted >= 70.0
    if recommended and adjusted >= 90.0:
        tier = "strong"
    elif recommended:
        tier = "review"
    elif active_pnl is not None and _num(report.get("delta_vs_active")) > 0.0:
        tier = "watch"
    else:
        tier = "reject"
    return {
        "recommended": bool(recommended),
        "tier": tier,
        "robustness_adjusted_score": adjusted,
        "raw_robustness_score": robustness_score,
        "delta_component": delta_component,
        "delta_pct_bonus": delta_component,
        "red_flag_penalty": round(red_flag_penalty, 6),
        "blockers": blockers,
        "report_only": True,
        "deduction": (
            "Recommended means the candidate beats the active baseline and has no hard "
            "robustness blockers. This is a search/review ranking, not a trading gate."
        ),
    }


def evaluate_candidate(
    row: dict[str, Any],
    active_row: dict[str, Any] | None = None,
    *,
    start_balance: float = 100000.0,
    rank: int | None = None,
) -> dict[str, Any]:
    active_row = active_row or {}
    candidate_pnl = _pnl(row)
    active_pnl = _pnl(active_row) if active_row else None
    active_trades = _trades(active_row) if active_row else 0
    candidate_trades = _trades(row)
    delta = None if active_pnl is None else round(candidate_pnl - active_pnl, 6)
    delta_pct = None
    if active_pnl not in (None, 0.0):
        delta_pct = round((candidate_pnl - active_pnl) / abs(active_pnl) * 100.0, 6)

    by_day = _nested_summary(row, "by_day")
    active_by_day = _nested_summary(active_row, "by_day") if active_row else {}
    day_names = sorted(set(by_day) | set(active_by_day))
    day_rows = []
    for day in day_names:
        cand = by_day.get(day) or {"pnl": 0.0, "trades": 0}
        active = active_by_day.get(day) or {"pnl": 0.0, "trades": 0}
        cand_pnl = _num(cand.get("pnl"))
        act_pnl = _num(active.get("pnl"))
        day_rows.append({
            "day": day,
            "candidate_pnl": round(cand_pnl, 6),
            "active_pnl": round(act_pnl, 6) if active_row else None,
            "delta_vs_active": round(cand_pnl - act_pnl, 6) if active_row else None,
            "candidate_trades": _int(cand.get("trades")),
            "active_trades": _int(active.get("trades")) if active_row else None,
            "candidate_positive": cand_pnl > 0.0,
            "beats_active": cand_pnl > act_pnl if active_row else None,
        })
    train_days, holdout_days = _split_days(day_names)
    train_pnl = _sum_named(by_day, train_days)
    holdout_pnl = _sum_named(by_day, holdout_days)
    active_train_pnl = _sum_named(active_by_day, train_days) if active_row else None
    active_holdout_pnl = _sum_named(active_by_day, holdout_days) if active_row else None
    positive_days = sum(1 for row_day in day_rows if row_day.get("candidate_positive"))
    beats_active_days = sum(1 for row_day in day_rows if row_day.get("beats_active"))
    worst_day = min(day_rows, key=lambda item: item["candidate_pnl"]) if day_rows else None
    best_day = max(day_rows, key=lambda item: item["candidate_pnl"]) if day_rows else None
    drawdown = _max_drawdown(day_rows)
    day_concentration = _concentration(by_day)

    by_ticker = _nested_summary(row, "by_ticker")
    active_by_ticker = _nested_summary(active_row, "by_ticker") if active_row else {}
    ticker_rows = []
    for ticker in sorted(set(by_ticker) | set(active_by_ticker)):
        cand = by_ticker.get(ticker) or {"pnl": 0.0, "trades": 0}
        active = active_by_ticker.get(ticker) or {"pnl": 0.0, "trades": 0}
        ticker_rows.append({
            "ticker": ticker,
            "candidate_pnl": round(_num(cand.get("pnl")), 6),
            "active_pnl": round(_num(active.get("pnl")), 6) if active_row else None,
            "delta_vs_active": round(_num(cand.get("pnl")) - _num(active.get("pnl")), 6) if active_row else None,
            "candidate_trades": _int(cand.get("trades")),
            "active_trades": _int(active.get("trades")) if active_row else None,
            "candidate_positive": _num(cand.get("pnl")) > 0.0,
            "beats_active": _num(cand.get("pnl")) > _num(active.get("pnl")) if active_row else None,
        })
    ticker_concentration = _concentration(by_ticker)

    by_side = _nested_summary(row, "by_side")
    active_by_side = _nested_summary(active_row, "by_side") if active_row else {}
    side_rows = []
    for side in sorted(set(by_side) | set(active_by_side)):
        cand = by_side.get(side) or {"pnl": 0.0, "trades": 0}
        active = active_by_side.get(side) or {"pnl": 0.0, "trades": 0}
        side_rows.append({
            "side": side,
            "candidate_pnl": round(_num(cand.get("pnl")), 6),
            "active_pnl": round(_num(active.get("pnl")), 6) if active_row else None,
            "delta_vs_active": round(_num(cand.get("pnl")) - _num(active.get("pnl")), 6) if active_row else None,
            "candidate_trades": _int(cand.get("trades")),
            "active_trades": _int(active.get("trades")) if active_row else None,
            "candidate_positive": _num(cand.get("pnl")) > 0.0,
            "beats_active": _num(cand.get("pnl")) > _num(active.get("pnl")) if active_row else None,
        })

    positive_ticker_rate = _rate(sum(1 for item in ticker_rows if item.get("candidate_positive")), len(ticker_rows))
    beats_ticker_rate = _rate(sum(1 for item in ticker_rows if item.get("beats_active")), len(ticker_rows)) if active_row else None
    positive_side_rate = _rate(sum(1 for item in side_rows if item.get("candidate_positive")), len(side_rows))
    beats_side_rate = _rate(sum(1 for item in side_rows if item.get("beats_active")), len(side_rows)) if active_row else None

    total_delta_quality = None
    if active_pnl is not None and active_pnl != 0.0:
        total_delta_quality = (delta_pct or 0.0) / 10.0
    elif start_balance > 0:
        total_delta_quality = (candidate_pnl / float(start_balance) * 100.0) / 10.0
    day_quality = _rate(beats_active_days, len(day_rows)) if active_row else _rate(positive_days, len(day_rows))
    holdout_delta = None
    if holdout_days and active_holdout_pnl is not None:
        holdout_delta = round(holdout_pnl - active_holdout_pnl, 6)
    holdout_quality = None
    if holdout_days:
        if active_holdout_pnl not in (None, 0.0):
            holdout_quality = (holdout_delta or 0.0) / max(abs(active_holdout_pnl), 1.0)
        else:
            holdout_quality = 1.0 if holdout_pnl > 0.0 else 0.0
    ticker_quality = beats_ticker_rate if beats_ticker_rate is not None else positive_ticker_rate
    side_quality = beats_side_rate if beats_side_rate is not None else positive_side_rate
    dd_denominator = max(abs(candidate_pnl), float(start_balance) * 0.02, 1.0)
    drawdown_quality = 1.0 - (_num(drawdown.get("max_drawdown")) / dd_denominator)
    concentration_value = max(
        _num(day_concentration.get("top_abs_share"), 0.0),
        _num(ticker_concentration.get("top_abs_share"), 0.0),
    )
    concentration_quality = 1.0 - max(0.0, concentration_value - 0.35) / 0.45
    min_trade_sample = max(100, int(active_trades * 0.50)) if active_trades else 100
    trade_quality = candidate_trades / float(min_trade_sample) if min_trade_sample else 1.0

    components = {
        "total_delta": round(_score_component(total_delta_quality, 20.0), 4),
        "day_consistency": round(_score_component(day_quality, 25.0), 4),
        "holdout": round(_score_component(holdout_quality, 20.0), 4),
        "ticker_balance": round(_score_component(ticker_quality, 10.0), 4),
        "side_balance": round(_score_component(side_quality, 10.0), 4),
        "drawdown_control": round(_score_component(drawdown_quality, 10.0), 4),
        "trade_sample": round(_score_component(trade_quality, 5.0), 4),
    }
    robustness_score = round(sum(components.values()), 4)

    red_flags: list[str] = []
    if len(day_rows) < 3:
        red_flags.append("fewer_than_3_days")
    if holdout_days and holdout_pnl <= 0.0:
        red_flags.append("holdout_not_profitable")
    if active_row and holdout_delta is not None and holdout_delta <= 0.0:
        red_flags.append("holdout_does_not_beat_active")
    if day_quality is not None and day_quality < 0.50:
        red_flags.append("weak_day_consistency")
    if concentration_value >= 0.65:
        red_flags.append("high_day_or_ticker_concentration")
    if candidate_trades < min_trade_sample:
        red_flags.append("low_trade_sample")
    if candidate_pnl > 0 and _num(drawdown.get("max_drawdown")) > abs(candidate_pnl) * 0.50:
        red_flags.append("large_drawdown_vs_profit")
    if active_row and delta is not None and delta <= 0.0:
        red_flags.append("does_not_beat_active_total")

    result = {
        "schema_version": SCHEMA_VERSION,
        "rank": rank,
        "variant": _variant_name(row),
        "model_id": _model_id(row),
        "bias": round(_num(row.get("bias")), 8),
        "weight_count": len(row.get("weights") or {}),
        "step2_pnl": round(candidate_pnl, 6),
        "active_step2_pnl": round(active_pnl, 6) if active_pnl is not None else None,
        "delta_vs_active": delta,
        "delta_pct_vs_active": delta_pct,
        "trades": candidate_trades,
        "active_trades": active_trades if active_row else None,
        "win_rate_pct": _win_rate(row),
        "robustness_score": robustness_score,
        "robustness_grade": _grade(robustness_score),
        "components": components,
        "red_flags": red_flags,
        "train_holdout": {
            "method": "chronological_last_30pct_holdout",
            "train_days": train_days,
            "holdout_days": holdout_days,
            "train_pnl": train_pnl,
            "active_train_pnl": active_train_pnl,
            "train_delta_vs_active": round(train_pnl - active_train_pnl, 6) if active_train_pnl is not None else None,
            "holdout_pnl": holdout_pnl,
            "active_holdout_pnl": active_holdout_pnl,
            "holdout_delta_vs_active": holdout_delta,
        },
        "day_profile": {
            "total_days": len(day_rows),
            "positive_days": positive_days,
            "positive_day_rate": _rate(positive_days, len(day_rows)),
            "beats_active_days": beats_active_days if active_row else None,
            "beats_active_day_rate": _rate(beats_active_days, len(day_rows)) if active_row else None,
            "worst_day": worst_day,
            "best_day": best_day,
            "max_drawdown": drawdown,
            "concentration": day_concentration,
            "days": day_rows,
        },
        "ticker_profile": {
            "positive_ticker_rate": positive_ticker_rate,
            "beats_active_ticker_rate": beats_ticker_rate,
            "concentration": ticker_concentration,
            "tickers": ticker_rows,
        },
        "side_profile": {
            "available": bool(side_rows),
            "positive_side_rate": positive_side_rate,
            "beats_active_side_rate": beats_side_rate,
            "sides": side_rows,
        },
        "trade_sample": {
            "candidate_trades": candidate_trades,
            "active_trades": active_trades if active_row else None,
            "minimum_reference_sample": min_trade_sample,
            "sample_ratio": round(candidate_trades / float(min_trade_sample), 6) if min_trade_sample else None,
        },
        "deduction": (
            "Robustness is report-only. It rewards broad day/ticker/side performance, "
            "holdout strength, drawdown control, and adequate trade count; it does not "
            "change the raw Step 2 P/L."
        ),
    }
    result["recommendation"] = recommendation_from_report(result)
    result["robustness_adjusted_score"] = result["recommendation"]["robustness_adjusted_score"]
    result["recommended"] = result["recommendation"]["recommended"]
    return result


def annotate_rows(
    rows: list[dict[str, Any]],
    *,
    active_row: dict[str, Any] | None = None,
    start_balance: float = 100000.0,
) -> list[dict[str, Any]]:
    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        report = evaluate_candidate(row, active_row=active_row, start_balance=start_balance, rank=idx + 1)
        row["robustness"] = {
            "score": report.get("robustness_score"),
            "adjusted_score": report.get("robustness_adjusted_score"),
            "grade": report.get("robustness_grade"),
            "recommended": report.get("recommended"),
            "recommendation_tier": (report.get("recommendation") or {}).get("tier"),
            "recommendation_blockers": (report.get("recommendation") or {}).get("blockers") or [],
            "red_flags": report.get("red_flags") or [],
            "components": report.get("components") or {},
            "delta_pct_vs_active": report.get("delta_pct_vs_active"),
            "holdout_delta_vs_active": (report.get("train_holdout") or {}).get("holdout_delta_vs_active"),
            "beats_active_day_rate": (report.get("day_profile") or {}).get("beats_active_day_rate"),
            "positive_day_rate": (report.get("day_profile") or {}).get("positive_day_rate"),
            "top_day_concentration": ((report.get("day_profile") or {}).get("concentration") or {}).get("top_abs_share"),
            "top_ticker_concentration": ((report.get("ticker_profile") or {}).get("concentration") or {}).get("top_abs_share"),
        }
    return rows


def build(
    *,
    rows: list[dict[str, Any]],
    active_row: dict[str, Any] | None = None,
    start_balance: float = 100000.0,
    top_n: int = 50,
    source: dict[str, Any] | None = None,
) -> dict[str, Any]:
    source = source or {}
    deduped: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        deduped[_model_id(row)] = row
    ordered_by_pnl = sorted(
        list(deduped.values()),
        key=candidate_profile_schema.score,
        reverse=True,
    )[:max(0, int(top_n))]
    reports = [
        evaluate_candidate(row, active_row=active_row, start_balance=start_balance, rank=idx + 1)
        for idx, row in enumerate(ordered_by_pnl)
    ]
    top_by_robustness = sorted(
        reports,
        key=lambda item: (_num(item.get("robustness_score")), _num(item.get("step2_pnl"))),
        reverse=True,
    )
    top_by_adjusted = sorted(
        reports,
        key=lambda item: (_num(item.get("robustness_adjusted_score")), _num(item.get("step2_pnl"))),
        reverse=True,
    )
    recommended = [item for item in top_by_adjusted if item.get("recommended")]
    red_flag_counts: dict[str, int] = {}
    for report in reports:
        for flag in report.get("red_flags") or []:
            red_flag_counts[str(flag)] = red_flag_counts.get(str(flag), 0) + 1
    payload = {
        "schema_version": SCHEMA_VERSION,
        "source": "candidate_robustness_report",
        "created_at_ct": _now_ct(),
        "input": source,
        "start_balance": float(start_balance),
        "row_count": len(reports),
        "active": {
            "variant": _variant_name(active_row) if active_row else None,
            "pnl": _pnl(active_row) if active_row else None,
            "trades": _trades(active_row) if active_row else None,
            "win_rate_pct": _win_rate(active_row) if active_row else None,
        },
        "summary": {
            "top_raw_variant": reports[0].get("variant") if reports else None,
            "top_raw_pnl": reports[0].get("step2_pnl") if reports else None,
            "top_robust_variant": top_by_robustness[0].get("variant") if top_by_robustness else None,
            "top_robust_score": top_by_robustness[0].get("robustness_score") if top_by_robustness else None,
            "top_robust_pnl": top_by_robustness[0].get("step2_pnl") if top_by_robustness else None,
            "top_adjusted_variant": top_by_adjusted[0].get("variant") if top_by_adjusted else None,
            "top_adjusted_score": top_by_adjusted[0].get("robustness_adjusted_score") if top_by_adjusted else None,
            "top_adjusted_pnl": top_by_adjusted[0].get("step2_pnl") if top_by_adjusted else None,
            "best_recommended_variant": recommended[0].get("variant") if recommended else None,
            "best_recommended_score": recommended[0].get("robustness_adjusted_score") if recommended else None,
            "recommended_count": len(recommended),
            "average_robustness_score": (
                round(sum(_num(report.get("robustness_score")) for report in reports) / len(reports), 6)
                if reports else None
            ),
            "red_flag_counts": dict(sorted(red_flag_counts.items())),
            "report_only": True,
        },
        "top_by_raw_pnl": reports,
        "top_by_robustness": top_by_robustness,
        "top_by_adjusted": top_by_adjusted,
        "recommended": recommended,
    }
    payload["report_hash"] = _stable_hash({k: v for k, v in payload.items() if k not in {"report_hash", "output_path"}})
    return payload


def build_from_payload(
    payload: dict[str, Any],
    *,
    top_n: int = 50,
    start_balance: float | None = None,
    source_path: str = "",
) -> dict[str, Any]:
    active = payload.get("active") if isinstance(payload.get("active"), dict) else {}
    rows = candidate_profile_schema.rows_from_payload(payload)
    balance = start_balance
    if balance is None:
        balance = _num(payload.get("start_balance"), 100000.0)
    source = {
        "candidate_json": source_path,
        "script": payload.get("script"),
        "artifact_hash": tournament_safety._file_sha256(source_path) if source_path else "",
        "baseline_drift": (
            baseline_drift_sentinel.evaluate_payload(payload)
            if baseline_drift_sentinel.source_looks_step2(payload)
            else {"status": "skipped", "reason": "not_step2_payload"}
        ),
    }
    return build(rows=rows, active_row=active, start_balance=float(balance), top_n=top_n, source=source)


def write_report(payload: dict[str, Any], path: str | os.PathLike[str] | None = None) -> str:
    if path is None:
        stem = datetime.now(CT).strftime("%Y%m%d_%H%M%S")
        top = ((payload.get("summary") or {}).get("top_robust_variant") or "candidate").replace(os.sep, "_")
        safe = "".join(ch if ch.isalnum() or ch in "._+-" else "_" for ch in top)[:80] or "candidate"
        path = OUT_DIR / f"{stem}_{safe}.json"
    out = _write_json(Path(path), payload)
    payload["output_path"] = out
    _write_json(Path(out), payload)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Build report-only robustness metrics for a Step 2 candidate artifact.")
    ap.add_argument("--candidate-json", required=True)
    ap.add_argument("--top-n", type=int, default=50)
    ap.add_argument("--start-balance", type=float, default=None)
    ap.add_argument("--no-write", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    path = Path(args.candidate_json).resolve()
    payload = _read_json(path)
    if not isinstance(payload, dict):
        raise RuntimeError(f"candidate artifact must be a JSON object: {path}")
    report = build_from_payload(
        payload,
        top_n=int(args.top_n),
        start_balance=args.start_balance,
        source_path=str(path),
    )
    if not args.no_write:
        write_report(report)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True, default=str))
    else:
        summary = report.get("summary") or {}
        print(json.dumps({
            "ok": True,
            "row_count": report.get("row_count"),
            "top_raw_variant": summary.get("top_raw_variant"),
            "top_raw_pnl": summary.get("top_raw_pnl"),
            "top_robust_variant": summary.get("top_robust_variant"),
            "top_robust_score": summary.get("top_robust_score"),
            "top_adjusted_variant": summary.get("top_adjusted_variant"),
            "top_adjusted_score": summary.get("top_adjusted_score"),
            "best_recommended_variant": summary.get("best_recommended_variant"),
            "recommended_count": summary.get("recommended_count"),
            "output_path": report.get("output_path"),
        }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
