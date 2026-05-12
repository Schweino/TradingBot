"""Execution-aware Step 2 objective helpers.

Raw replay P/L is discovery telemetry. This module adds a conservative,
machine-readable objective that accounts for replay validity, transaction
costs, slippage/latency pressure, and fill uncertainty before a row is treated
as truly useful.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import step2_quote_aware_guard


OBJECTIVE_VERSION = "execution_adjusted_v1"
DEFAULT_START_BALANCE = 100000.0
DEFAULT_TRADE_SIZE_PCT = 0.25
DEFAULT_SLIPPAGE_BPS = 2.5
DEFAULT_LATENCY_BPS = 1.0
DEFAULT_SPREAD_BPS = 1.0
DEFAULT_FEE_BPS = 0.25


def _read_json(path: str | Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _trade_count(row: dict[str, Any]) -> int:
    full = row.get("decision_full") if isinstance(row.get("decision_full"), dict) else {}
    return int(row.get("step2_trades") if row.get("step2_trades") is not None else full.get("trades") or 0)


def _pnl(row: dict[str, Any]) -> float:
    full = row.get("decision_full") if isinstance(row.get("decision_full"), dict) else {}
    return _num(row.get("step2_pnl") if row.get("step2_pnl") is not None else full.get("pnl"), 0.0)


def load_trading_config(path: str | Path = "trading_config.json") -> dict[str, Any]:
    return _read_json(path)


def replay_validity(row: dict[str, Any], *, require_quote_aware: bool = True) -> dict[str, Any]:
    actual = step2_quote_aware_guard.candidate_exit_replay_model(row)
    required = step2_quote_aware_guard.REQUIRED_EXIT_REPLAY_MODEL
    blockers: list[str] = []
    if require_quote_aware and actual != required:
        blockers.append("exit_replay_model_mismatch" if actual else "exit_replay_model_missing")
    if _trade_count(row) <= 0:
        blockers.append("no_trade_sample")
    if not isinstance(row.get("by_day"), dict) or not row.get("by_day"):
        blockers.append("missing_by_day")
    score = 100.0
    if "exit_replay_model_missing" in blockers:
        score = min(score, 35.0)
    if "exit_replay_model_mismatch" in blockers:
        score = min(score, 0.0)
    if "missing_by_day" in blockers:
        score = min(score, 70.0)
    if "no_trade_sample" in blockers:
        score = min(score, 0.0)
    return {
        "ok": not blockers,
        "score": round(score, 4),
        "required_exit_replay_model": required,
        "actual_exit_replay_model": actual,
        "blockers": blockers,
    }


def cost_assumptions(config: dict[str, Any] | None = None) -> dict[str, float]:
    config = config or {}
    objective = config.get("step2_objective") if isinstance(config.get("step2_objective"), dict) else {}
    trade_size_pct = _num(config.get("trade_size_pct"), DEFAULT_TRADE_SIZE_PCT)
    return {
        "trade_size_pct": _num(objective.get("trade_size_pct"), trade_size_pct),
        "slippage_bps": _num(objective.get("slippage_bps"), DEFAULT_SLIPPAGE_BPS),
        "latency_bps": _num(objective.get("latency_bps"), DEFAULT_LATENCY_BPS),
        "spread_bps": _num(objective.get("spread_bps"), DEFAULT_SPREAD_BPS),
        "fee_bps": _num(objective.get("fee_bps"), DEFAULT_FEE_BPS),
    }


def estimate_execution_costs(
    row: dict[str, Any],
    *,
    start_balance: float = DEFAULT_START_BALANCE,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    trades = _trade_count(row)
    assumptions = cost_assumptions(config)
    notional_per_trade = _num(row.get("estimated_notional_per_trade"), 0.0)
    if notional_per_trade <= 0.0:
        notional_per_trade = max(0.0, float(start_balance)) * assumptions["trade_size_pct"]
    bps = (
        assumptions["slippage_bps"]
        + assumptions["latency_bps"]
        + assumptions["spread_bps"]
        + assumptions["fee_bps"]
    )
    total_cost = trades * notional_per_trade * bps / 10000.0
    return {
        "trades": trades,
        "notional_per_trade": round(notional_per_trade, 6),
        "total_bps": round(bps, 6),
        "slippage_bps": assumptions["slippage_bps"],
        "latency_bps": assumptions["latency_bps"],
        "spread_bps": assumptions["spread_bps"],
        "fee_bps": assumptions["fee_bps"],
        "estimated_total_cost": round(total_cost, 6),
    }


def fill_probability(row: dict[str, Any]) -> float:
    explicit = row.get("fill_probability")
    if explicit is not None:
        return max(0.0, min(1.0, _num(explicit, 1.0)))
    execution = row.get("execution") if isinstance(row.get("execution"), dict) else {}
    if execution.get("fill_probability") is not None:
        return max(0.0, min(1.0, _num(execution.get("fill_probability"), 1.0)))
    quality = row.get("execution_quality_score")
    if quality is not None:
        return max(0.05, min(1.0, _num(quality, 100.0) / 100.0))
    return 0.92


def candidate_objective(
    row: dict[str, Any],
    *,
    active_pnl: float | None = None,
    start_balance: float = DEFAULT_START_BALANCE,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    gross = _pnl(row)
    costs = estimate_execution_costs(row, start_balance=start_balance, config=config)
    validity = replay_validity(row)
    fill_prob = fill_probability(row)
    replay_multiplier = max(0.0, min(1.0, _num(validity.get("score"), 0.0) / 100.0))
    execution_adjusted = (gross - _num(costs.get("estimated_total_cost"), 0.0)) * fill_prob * replay_multiplier
    active_delta = None
    if active_pnl is not None:
        active_delta = round(execution_adjusted - float(active_pnl), 6)
    return {
        "objective_version": OBJECTIVE_VERSION,
        "gross_pnl": round(gross, 6),
        "execution_adjusted_pnl": round(execution_adjusted, 6),
        "execution_adjusted_delta_vs_active": active_delta,
        "objective_score": round(execution_adjusted, 6),
        "fill_probability": round(fill_prob, 6),
        "replay_validity": validity,
        "estimated_execution_costs": costs,
        "deduction": "Raw replay P/L is discounted by estimated execution cost, fill probability, and quote-aware replay validity.",
    }


def objective_report(rows: list[dict[str, Any]], *, limit: int = 25) -> dict[str, Any]:
    ranked = sorted(
        rows,
        key=lambda row: _num((row.get("execution_adjusted_objective") or {}).get("objective_score"), -1e18),
        reverse=True,
    )
    invalid = [
        row for row in rows
        if isinstance(row.get("replay_validity"), dict) and row["replay_validity"].get("ok") is False
    ]
    return {
        "schema_version": 1,
        "objective_version": OBJECTIVE_VERSION,
        "candidate_count": len(rows),
        "invalid_replay_count": len(invalid),
        "top": [
            {
                "variant": row.get("variant"),
                "gross_pnl": row.get("step2_pnl"),
                "execution_adjusted_pnl": row.get("execution_adjusted_pnl"),
                "objective_score": row.get("objective_score"),
                "replay_validity": row.get("replay_validity"),
            }
            for row in ranked[:limit]
        ],
    }
