"""Shared deterministic execution rules for Live and Step 2.

The kernel is intentionally pure: no broker calls, no file I/O, and no
background services. Live can import this in the hot path without adding
network or disk latency, while replay/scoring code can use the same contract
hash to prove it is evaluating the same execution rules.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Any


SCHEMA_VERSION = 1
DEFAULT_REENTRY_COOLDOWN_SEC = 5
DEFAULT_TRADE_SIZE_PCT = 1.0
SIDE_LONG = "LONG"
SIDE_SHORT = "SHORT"


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return int(default)
        return int(float(value))
    except Exception:
        return int(default)


def _bool(value: Any, default: bool = False) -> bool:
    if value in (None, ""):
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def stable_hash(payload: Any, length: int = 64) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:length]


@dataclass(frozen=True)
class ExecutionContract:
    schema_version: int = SCHEMA_VERSION
    one_open_trade_per_ticker: bool = True
    same_ticker_reentry_cooldown_sec: int = DEFAULT_REENTRY_COOLDOWN_SEC
    price_rounding: str = "nearest_cent_half_up"
    entry_rounding: str = "nearest_cent_half_up"
    take_profit_rounding: str = "nearest_cent_half_up"
    stop_loss_rounding: str = "nearest_cent_half_up"
    conditional_time_stop_enabled: bool = False
    risk_off_enabled: bool = False
    conviction_decay_enabled: bool = False
    profit_protection_enabled: bool = False
    strict_tp_sl: bool = True
    trade_size_pct: float = DEFAULT_TRADE_SIZE_PCT
    allocation_scope: str = "per_ticker_equal_weight"
    latency_mode: str = "entry-exit"
    latency_percentile: str = "p75"
    admission_mode: int = 0
    require_conviction: bool = False
    setup_state_enabled: bool = False
    use_live_long_gate: bool = False
    use_live_short_guard: bool = False
    use_step2_brs_long_guard: bool = False
    min_exec_score: float = 0.0
    max_trades_per_day: int = 0
    max_trades_per_ticker_day: int = 0
    stop_loss_cooldown_sec: int = 0
    loss_cluster_window_sec: int = 0
    loss_cluster_count: int = 0
    loss_cluster_cooldown_sec: int = 0

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["execution_kernel_hash"] = contract_hash(payload)
        return payload


def contract_from_config(config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build the canonical execution contract from the live config payload."""
    cfg = config or {}
    step2 = cfg.get("step2") if isinstance(cfg.get("step2"), dict) else {}
    execution = cfg.get("execution") if isinstance(cfg.get("execution"), dict) else {}
    risk = cfg.get("risk") if isinstance(cfg.get("risk"), dict) else {}
    latency = cfg.get("latency") if isinstance(cfg.get("latency"), dict) else {}

    contract = ExecutionContract(
        same_ticker_reentry_cooldown_sec=_int(
            step2.get("same_ticker_reentry_cooldown_sec",
                      execution.get("same_ticker_reentry_cooldown_sec", DEFAULT_REENTRY_COOLDOWN_SEC)),
            DEFAULT_REENTRY_COOLDOWN_SEC,
        ),
        conditional_time_stop_enabled=_bool(
            execution.get("conditional_time_stop_enabled",
                          risk.get("conditional_time_stop_enabled", cfg.get("conditional_time_stop_enabled"))),
            False,
        ),
        risk_off_enabled=_bool(risk.get("risk_off_enabled", cfg.get("risk_off_enabled")), False),
        conviction_decay_enabled=_bool(risk.get("conviction_decay_enabled", cfg.get("conviction_decay_enabled")), False),
        profit_protection_enabled=_bool(risk.get("profit_protection_enabled", cfg.get("profit_protection_enabled")), False),
        trade_size_pct=_num(cfg.get("TRADE_SIZE_PCT", cfg.get("trade_size_pct", DEFAULT_TRADE_SIZE_PCT)),
                            DEFAULT_TRADE_SIZE_PCT),
        latency_mode=str(latency.get("mode", step2.get("latency_mode", "entry-exit")) or "entry-exit"),
        latency_percentile=str(latency.get("percentile", step2.get("latency_percentile", "p75")) or "p75"),
        admission_mode=_int(step2.get("admission_mode", cfg.get("admission_mode", 0)), 0),
        require_conviction=_bool(step2.get("require_conviction", cfg.get("require_conviction")), False),
        setup_state_enabled=_bool(step2.get("setup_state_enabled", cfg.get("setup_state_enabled")), False),
        use_live_long_gate=_bool(step2.get("use_live_long_gate", cfg.get("use_live_long_gate")), False),
        use_live_short_guard=_bool(step2.get("use_live_short_guard", cfg.get("use_live_short_guard")), False),
        use_step2_brs_long_guard=_bool(
            step2.get("use_step2_brs_long_guard", cfg.get("use_step2_brs_long_guard")),
            False,
        ),
        min_exec_score=_num(step2.get("min_exec_score", cfg.get("min_exec_score")), 0.0),
        max_trades_per_day=_int(step2.get("max_trades_per_day", cfg.get("max_trades_per_day")), 0),
        max_trades_per_ticker_day=_int(
            step2.get("max_trades_per_ticker_day", cfg.get("max_trades_per_ticker_day")),
            0,
        ),
        stop_loss_cooldown_sec=_int(step2.get("stop_loss_cooldown_sec", cfg.get("stop_loss_cooldown_sec")), 0),
        loss_cluster_window_sec=_int(step2.get("loss_cluster_window_sec", cfg.get("loss_cluster_window_sec")), 0),
        loss_cluster_count=_int(step2.get("loss_cluster_count", cfg.get("loss_cluster_count")), 0),
        loss_cluster_cooldown_sec=_int(
            step2.get("loss_cluster_cooldown_sec", cfg.get("loss_cluster_cooldown_sec")),
            0,
        ),
    )
    return contract.to_dict()


def contract_hash(contract: dict[str, Any] | None = None) -> str:
    payload = dict(contract or {})
    payload.pop("execution_kernel_hash", None)
    return stable_hash(payload)


def normalize_side(side: Any) -> str:
    value = str(side or "").strip().upper()
    if value in {"SHORT", "S", "-1"}:
        return SIDE_SHORT
    return SIDE_LONG


def round_price_to_cent(price: Any) -> float:
    quantized = Decimal(str(_num(price))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return float(quantized)


def bracket_prices(entry_price: Any, side: Any, take_profit_pct: float, stop_loss_pct: float) -> dict[str, float]:
    entry = round_price_to_cent(entry_price)
    direction = normalize_side(side)
    tp_factor = 1.0 + (_num(take_profit_pct) / 100.0 if direction == SIDE_LONG else -_num(take_profit_pct) / 100.0)
    sl_factor = 1.0 - (_num(stop_loss_pct) / 100.0 if direction == SIDE_LONG else -_num(stop_loss_pct) / 100.0)
    return {
        "entry_price": entry,
        "take_profit_price": round_price_to_cent(entry * tp_factor),
        "stop_loss_price": round_price_to_cent(entry * sl_factor),
    }


def ticker_open_until(entry_ts: Any, held_sec: Any, contract: dict[str, Any] | None = None) -> int:
    cooldown = _int((contract or {}).get("same_ticker_reentry_cooldown_sec"), DEFAULT_REENTRY_COOLDOWN_SEC)
    return _int(entry_ts) + max(0, _int(held_sec)) + max(0, cooldown)


def can_enter_ticker(ts: Any, open_until: Any) -> dict[str, Any]:
    now = _int(ts)
    blocked_until = _int(open_until)
    allowed = now >= blocked_until
    return {
        "allowed": allowed,
        "reason": None if allowed else "open_position_or_reentry_cooldown",
        "remaining_sec": max(0, blocked_until - now),
    }


def allocation(balance: Any, ticker_count: Any, contract: dict[str, Any] | None = None) -> float:
    trade_size_pct = _num((contract or {}).get("trade_size_pct"), DEFAULT_TRADE_SIZE_PCT)
    count = max(1, _int(ticker_count, 1))
    return round((_num(balance) * trade_size_pct / count) * 100.0) / 100.0


def exit_hit(side: Any, price: Any, take_profit_price: Any, stop_loss_price: Any) -> str | None:
    direction = normalize_side(side)
    px = _num(price)
    tp = _num(take_profit_price)
    sl = _num(stop_loss_price)
    if direction == SIDE_LONG:
        if px >= tp:
            return "take_profit"
        if px <= sl:
            return "stop_loss"
    else:
        if px <= tp:
            return "take_profit"
        if px >= sl:
            return "stop_loss"
    return None
