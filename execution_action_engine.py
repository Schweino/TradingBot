"""Canonical action planner shared by Live and Step 2.

This module is pure decision logic: it has no broker calls, no file I/O, and
no background services. Live should use its plans to submit orders; replay
should use the same plans to simulate orders.
"""
from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, tzinfo
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import execution_kernel
import execution_state_reducer
import step2_execution_contract


CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1


def stable_hash(payload: Any, length: int = 64) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:length]


def _int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return int(default)
        return int(float(value))
    except Exception:
        return int(default)


def _num(value: Any, default: float | None = None) -> float | None:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except Exception:
        return default


def _ticker(value: Any) -> str:
    return str(value or "").upper()


def _day(ts: int, tz: tzinfo | None = CT) -> str:
    return datetime.fromtimestamp(int(ts), tz).date().isoformat()


def _get(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _contract_ids(contract: dict[str, Any] | None, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    contract = contract or {}
    metadata = metadata or {}
    nested = contract.get("execution_kernel_contract") if isinstance(contract.get("execution_kernel_contract"), dict) else {}
    return {
        "execution_kernel_hash": (
            metadata.get("execution_kernel_hash")
            or contract.get("execution_kernel_hash")
            or nested.get("execution_kernel_hash")
        ),
        "step2_parity_contract_hash": (
            metadata.get("step2_parity_contract_hash")
            or contract.get("step2_parity_contract_hash")
        ),
        "step2_execution_contract_hash": (
            metadata.get("step2_execution_contract_hash")
            or contract.get("step2_execution_contract_hash")
            or stable_hash(contract)
        ),
        "action_engine_hash": stable_hash({
            "schema_version": SCHEMA_VERSION,
            "entry": "execution_state_reducer.entry_block_reason",
            "exit": "execution_kernel.exit_hit",
            "brackets": "step2_execution_contract.exit_brackets",
        }),
    }


def _semantic_payload(plan: dict[str, Any]) -> dict[str, Any]:
    identity = plan.get("identity") or {}
    return {
        "schema_version": plan.get("schema_version"),
        "plan_type": plan.get("plan_type"),
        "action": plan.get("action"),
        "decision": plan.get("decision"),
        "reason": plan.get("reason"),
        "identity": {
            "ticker": identity.get("ticker"),
            "side": identity.get("side"),
            "setup_type": identity.get("setup_type"),
            "timestamp_second": identity.get("timestamp_second"),
        },
        "market": plan.get("market") or {},
        "score": plan.get("score") or {},
        "brackets": plan.get("brackets") or {},
        "contracts": plan.get("contracts") or {},
    }


def _finalize(plan: dict[str, Any]) -> dict[str, Any]:
    identity = plan.get("identity") or {}
    plan["action_plan_id"] = stable_hash({
        "schema_version": plan.get("schema_version"),
        "action": plan.get("action"),
        "decision": plan.get("decision"),
        "reason": plan.get("reason"),
        "identity": identity,
    }, length=32)
    plan["semantic_action_hash"] = stable_hash(_semantic_payload(plan))
    plan["action_plan_hash"] = stable_hash(plan)
    return plan


def rehash_plan(plan: dict[str, Any]) -> dict[str, Any]:
    """Refresh the content hash after a caller attaches execution details."""
    plan.pop("semantic_action_hash", None)
    plan.pop("action_plan_hash", None)
    plan["semantic_action_hash"] = stable_hash(_semantic_payload(plan))
    plan["action_plan_hash"] = stable_hash(plan)
    return plan


def plan_brackets(side: str, entry_price: Any, sl_pct: Any, tp_pct: Any) -> dict[str, Any]:
    """Return the canonical side-aware bracket prices for an entry."""
    brackets = step2_execution_contract.exit_brackets(
        str(side or "").upper(),
        float(_num(entry_price, 0.0) or 0.0),
        float(_num(sl_pct, 0.0) or 0.0),
        float(_num(tp_pct, 0.0) or 0.0),
    )
    brackets["entry_price"] = execution_kernel.round_price_to_cent(entry_price)
    return brackets


def _normalize_brackets(side: str, entry_price: Any,
                        sl_pct: Any = None, tp_pct: Any = None,
                        brackets: dict[str, Any] | None = None) -> dict[str, Any]:
    if brackets:
        out = dict(brackets)
        if "entry_price" not in out:
            out["entry_price"] = execution_kernel.round_price_to_cent(entry_price)
        if "sl" not in out and "stop_loss_price" in out:
            out["sl"] = out.get("stop_loss_price")
        if "tp" not in out and "take_profit_price" in out:
            out["tp"] = out.get("take_profit_price")
        out.setdefault("side", str(side or "").upper())
        out.setdefault("rounding", "side_aware_penny_v1")
        return out
    if entry_price is None or sl_pct is None or tp_pct is None:
        return {}
    return plan_brackets(side, entry_price, sl_pct, tp_pct)


def plan_entry(runtime_state: dict[str, Any],
               signal: dict[str, Any],
               signal_ts: int | None,
               contract: dict[str, Any] | None,
               signal_scan_live_mode: bool = True,
               tz: tzinfo | None = CT,
               entry_price: Any = None,
               sl_pct: Any = None,
               tp_pct: Any = None,
               brackets: dict[str, Any] | None = None,
               qty: Any = None,
               alloc: Any = None,
               trade_id: str | None = None,
               client_order_id: str | None = None,
               broker_action: str | None = None,
               metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    """Plan whether a signal may enter and what immutable entry contract it uses."""
    metadata = metadata or {}
    ts = _int(signal_ts or signal.get("ts"), int(time.time()))
    day = str(metadata.get("day") or _day(ts, tz))
    ticker = _ticker(signal.get("ticker") or metadata.get("ticker"))
    side = str(signal.get("side") or metadata.get("side") or "").upper()
    price = _num(entry_price if entry_price is not None else signal.get("price"))
    block_reason = None
    if ticker:
        block_reason = step2_execution_contract.entry_block_reason(
            runtime_state or {},
            ticker,
            ts,
            day,
            contract or {},
            signal_scan_live_mode=signal_scan_live_mode,
            tz=tz,
        )
    if block_reason is None and entry_price is not None and (price is None or price <= 0):
        block_reason = "invalid_entry_price"
    action = "skip" if block_reason else "enter"
    bracket_plan = {} if block_reason else _normalize_brackets(side, price, sl_pct, tp_pct, brackets)
    state_view = execution_state_reducer.effective_view(runtime_state or {})
    plan = {
        "schema_version": SCHEMA_VERSION,
        "source": "execution_action_engine",
        "plan_type": "entry",
        "action": action,
        "decision": "skipped" if block_reason else "entered",
        "reason": block_reason or "entered",
        "day": day,
        "created_at": ts,
        "created_at_ct": datetime.fromtimestamp(ts, tz).isoformat(timespec="seconds"),
        "identity": {
            "ticker": ticker,
            "side": side,
            "setup_type": signal.get("setup_type") or metadata.get("setup_type"),
            "timestamp_second": ts,
            "opportunity_id": signal.get("opportunity_id") or metadata.get("opportunity_id"),
            "trade_id": trade_id or metadata.get("trade_id"),
            "client_order_id": client_order_id or metadata.get("client_order_id"),
        },
        "market": {
            "entry_price": price,
        },
        "score": {
            "score": signal.get("score"),
            "conviction": signal.get("conviction"),
            "components": signal.get("components") or {},
            "reasons": signal.get("reasons") or [],
        },
        "brackets": {
            "entry_price": _num(bracket_plan.get("entry_price"), price),
            "tp_price": _num(bracket_plan.get("tp")),
            "sl_price": _num(bracket_plan.get("sl")),
            "tp_raw": _num(bracket_plan.get("tp_raw")),
            "sl_raw": _num(bracket_plan.get("sl_raw")),
            "rounding": bracket_plan.get("rounding") or "side_aware_penny_v1",
            "policy": bracket_plan,
        },
        "state": {
            "positions": sorted((state_view.get("positions") or {}).keys()),
            "pending_entries": sorted((state_view.get("pending_entries") or {}).keys()),
            "last_closed_at": (state_view.get("last_closed_at") or {}).get(ticker),
            "state_hash": state_view.get("state_hash"),
        },
        "contracts": _contract_ids(contract, metadata),
        "execution": {
            "broker_action": broker_action or ("submit_or_simulate_entry" if not block_reason else "none"),
            "qty": _num(qty),
            "alloc": _num(alloc),
            "trade_id": trade_id or metadata.get("trade_id"),
            "client_order_id": client_order_id or metadata.get("client_order_id"),
        },
        "metadata": metadata,
    }
    return _finalize(plan)


def plan_exit(position: Any,
              price: Any,
              now_ts: int | None,
              in_trading_window: bool,
              contract: dict[str, Any] | None = None,
              include_strict_brackets: bool = True,
              include_conditional_time_stop: bool = False,
              conditional_time_stop_min: float = 0.0,
              include_session_end: bool = True,
              metadata: dict[str, Any] | None = None,
              tz: tzinfo | None = CT) -> dict[str, Any]:
    """Plan the canonical exit action for an open position."""
    metadata = metadata or {}
    ts = _int(now_ts, int(time.time()))
    ticker = _ticker(_get(position, "ticker") or metadata.get("ticker"))
    side = str(_get(position, "side") or metadata.get("side") or "").upper()
    entry = _num(_get(position, "entry", _get(position, "entry_price")))
    entry_ts = _int(_get(position, "entry_ts", _get(position, "opened_at")), ts)
    tp = _num(_get(position, "tp"))
    sl = _num(_get(position, "sl"))
    px = _num(price)
    action = "hold"
    decision = "hold"
    reason = None
    exit_price = None
    source = "execution_action_engine"
    if px is not None and include_strict_brackets and tp is not None and sl is not None:
        hit = execution_kernel.exit_hit(side, px, tp, sl)
        if hit == "take_profit":
            action = "exit"
            decision = "closed"
            reason = "take_profit"
            exit_price = round(float(tp), 4)
            source = "execution_kernel.exit_hit"
        elif hit == "stop_loss":
            action = "exit"
            decision = "closed"
            reason = "stop_loss"
            exit_price = round(float(sl), 4)
            source = "execution_kernel.exit_hit"
    if action != "exit" and px is not None and include_conditional_time_stop and in_trading_window:
        held_min = (ts - entry_ts) / 60.0
        if held_min >= float(conditional_time_stop_min or 0.0) and entry is not None:
            unrealized = (px - entry) if side == "LONG" else (entry - px)
            if unrealized < 0:
                action = "exit"
                decision = "closed"
                reason = "cond_time_stop"
                exit_price = round(float(px), 4)
                source = "conditional_time_stop"
    if action != "exit" and px is not None and include_session_end and not in_trading_window:
        action = "exit"
        decision = "closed"
        reason = "session_end"
        exit_price = round(float(px), 4)
        source = "session_window"
    plan = {
        "schema_version": SCHEMA_VERSION,
        "source": "execution_action_engine",
        "plan_type": "exit",
        "action": action,
        "decision": decision,
        "reason": reason,
        "day": str(metadata.get("day") or _day(ts, tz)),
        "created_at": ts,
        "created_at_ct": datetime.fromtimestamp(ts, tz).isoformat(timespec="seconds"),
        "identity": {
            "ticker": ticker,
            "side": side,
            "timestamp_second": ts,
            "trade_id": _get(position, "trade_id") or metadata.get("trade_id"),
            "opportunity_id": _get(position, "opportunity_id") or metadata.get("opportunity_id"),
            "setup_type": _get(position, "setup_type") or metadata.get("setup_type"),
        },
        "market": {
            "market_price": px,
            "entry_price": entry,
            "exit_price": exit_price,
        },
        "brackets": {
            "tp_price": tp,
            "sl_price": sl,
        },
        "contracts": _contract_ids(contract, metadata),
        "execution": {
            "source": source,
            "held_sec": ts - entry_ts if entry_ts else None,
        },
        "metadata": metadata,
    }
    return _finalize(plan)


def legacy_entry_decision(plan: dict[str, Any], contract: dict[str, Any] | None = None) -> dict[str, Any]:
    """Compatibility shape for older Live/Step 2 call sites."""
    return {
        "schema_version": SCHEMA_VERSION,
        "decision": "skipped" if plan.get("action") == "skip" else "accepted",
        "reason": plan.get("reason") if plan.get("action") == "skip" else None,
        "ticker": (plan.get("identity") or {}).get("ticker"),
        "signal_ts": plan.get("created_at"),
        "day": plan.get("day"),
        "contract_hash": stable_hash(contract or {}),
        "contract": contract or {},
        "action_plan_id": plan.get("action_plan_id"),
        "action_plan_hash": plan.get("action_plan_hash"),
        "semantic_action_hash": plan.get("semantic_action_hash"),
        "action_plan": plan,
    }


def legacy_exit_decision(plan: dict[str, Any]) -> dict[str, Any]:
    """Compatibility shape for older exit call sites."""
    return {
        "schema_version": SCHEMA_VERSION,
        "decision": "exit" if plan.get("action") == "exit" else "hold",
        "reason": plan.get("reason"),
        "exit_price": (plan.get("market") or {}).get("exit_price"),
        "market_price": (plan.get("market") or {}).get("market_price"),
        "source": (plan.get("execution") or {}).get("source"),
        "action_plan_id": plan.get("action_plan_id"),
        "action_plan_hash": plan.get("action_plan_hash"),
        "semantic_action_hash": plan.get("semantic_action_hash"),
        "action_plan": plan,
    }
