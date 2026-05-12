"""Canonical execution intents and results for Live and Step 2.

The action engine decides what should happen. This module turns that action
plan into an execution intent and records the result shape that both broker
Live and Step 2 simulation must speak.
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


CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1


def stable_hash(payload: Any, length: int = 64) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:length]


def _num(value: Any, default: float | None = None) -> float | None:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except Exception:
        return default


def _int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return int(default)
        return int(float(value))
    except Exception:
        return int(default)


def _now(ts: int | None = None) -> int:
    return _int(ts, int(time.time()))


def _day(ts: int, tz: tzinfo | None = CT) -> str:
    return datetime.fromtimestamp(int(ts), tz).date().isoformat()


def _iso(ts: int, tz: tzinfo | None = CT) -> str:
    return datetime.fromtimestamp(int(ts), tz).isoformat(timespec="seconds")


def _identity(action_plan: dict[str, Any] | None, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    action_plan = action_plan or {}
    metadata = metadata or {}
    src = action_plan.get("identity") if isinstance(action_plan.get("identity"), dict) else {}
    return {
        "ticker": src.get("ticker") or metadata.get("ticker"),
        "side": src.get("side") or metadata.get("side"),
        "setup_type": src.get("setup_type") or metadata.get("setup_type"),
        "timestamp_second": src.get("timestamp_second") or metadata.get("timestamp_second"),
        "opportunity_id": src.get("opportunity_id") or metadata.get("opportunity_id"),
        "trade_id": src.get("trade_id") or metadata.get("trade_id"),
        "client_order_id": src.get("client_order_id") or metadata.get("client_order_id"),
    }


def _order_side(intent_type: str, strategy_side: str | None) -> str | None:
    side = str(strategy_side or "").upper()
    if side == "LONG":
        return "buy" if intent_type == "entry" else "sell"
    if side == "SHORT":
        return "sell" if intent_type == "entry" else "buy"
    return None


def _semantic_payload(intent: dict[str, Any]) -> dict[str, Any]:
    identity = intent.get("identity") or {}
    order = intent.get("order") or {}
    return {
        "schema_version": intent.get("schema_version"),
        "intent_type": intent.get("intent_type"),
        "identity": {
            "ticker": identity.get("ticker"),
            "side": identity.get("side"),
            "setup_type": identity.get("setup_type"),
            "timestamp_second": identity.get("timestamp_second"),
        },
        "action_plan": {
            "action_plan_id": intent.get("action_plan_id"),
            "semantic_action_hash": intent.get("semantic_action_hash"),
        },
        "order": {
            "symbol": order.get("symbol"),
            "strategy_side": order.get("strategy_side"),
            "broker_side": order.get("broker_side"),
            "qty": order.get("qty"),
            "entry_price": order.get("entry_price"),
            "exit_price": order.get("exit_price"),
            "tp_price": order.get("tp_price"),
            "sl_price": order.get("sl_price"),
        },
    }


def _finalize_intent(intent: dict[str, Any]) -> dict[str, Any]:
    intent["semantic_execution_intent_hash"] = stable_hash(_semantic_payload(intent))
    intent["execution_intent_id"] = stable_hash({
        "schema_version": intent.get("schema_version"),
        "intent_type": intent.get("intent_type"),
        "venue": intent.get("venue"),
        "identity": intent.get("identity"),
        "semantic_execution_intent_hash": intent.get("semantic_execution_intent_hash"),
    }, length=32)
    intent["execution_intent_hash"] = stable_hash(intent)
    return intent


def entry_intent(action_plan: dict[str, Any],
                 venue: str,
                 qty: Any,
                 trade_id: str | None = None,
                 client_order_id: str | None = None,
                 broker_order_id: str | None = None,
                 tp_price: Any = None,
                 sl_price: Any = None,
                 entry_price: Any = None,
                 order_kind: str = "bracket_entry",
                 timeout_sec: int | None = None,
                 metadata: dict[str, Any] | None = None,
                 ts: int | None = None,
                 tz: tzinfo | None = CT) -> dict[str, Any]:
    ts_i = _now(ts)
    metadata = metadata or {}
    identity = _identity(action_plan, metadata)
    strategy_side = str(identity.get("side") or "").upper()
    brackets = action_plan.get("brackets") if isinstance(action_plan.get("brackets"), dict) else {}
    market = action_plan.get("market") if isinstance(action_plan.get("market"), dict) else {}
    trade_id = trade_id or identity.get("trade_id")
    client_order_id = client_order_id or identity.get("client_order_id")
    intent = {
        "schema_version": SCHEMA_VERSION,
        "record_type": "execution_intent",
        "intent_type": "entry",
        "source": "execution_intent_engine",
        "venue": venue,
        "created_at": ts_i,
        "created_at_ct": _iso(ts_i, tz),
        "day": metadata.get("day") or _day(ts_i, tz),
        "action_plan_id": action_plan.get("action_plan_id"),
        "action_plan_hash": action_plan.get("action_plan_hash"),
        "semantic_action_hash": action_plan.get("semantic_action_hash"),
        "identity": {
            **identity,
            "trade_id": trade_id,
            "client_order_id": client_order_id,
        },
        "order": {
            "symbol": identity.get("ticker"),
            "strategy_side": strategy_side,
            "broker_side": _order_side("entry", strategy_side),
            "order_kind": order_kind,
            "qty": _num(qty),
            "entry_price": _num(entry_price if entry_price is not None else market.get("entry_price")),
            "tp_price": _num(tp_price if tp_price is not None else brackets.get("tp_price")),
            "sl_price": _num(sl_price if sl_price is not None else brackets.get("sl_price")),
            "client_order_id": client_order_id,
            "broker_order_id": broker_order_id,
        },
        "policy": {
            "timeout_sec": timeout_sec,
            "strict_tp_sl": True,
            "one_open_position_per_ticker": True,
        },
        "contracts": action_plan.get("contracts") or {},
        "metadata": metadata,
    }
    return _finalize_intent(intent)


def exit_intent(action_plan: dict[str, Any] | None,
                venue: str,
                qty: Any,
                trade_id: str | None = None,
                client_order_id: str | None = None,
                broker_order_id: str | None = None,
                exit_price: Any = None,
                reason: str | None = None,
                order_kind: str = "market_exit",
                metadata: dict[str, Any] | None = None,
                ts: int | None = None,
                tz: tzinfo | None = CT) -> dict[str, Any]:
    ts_i = _now(ts)
    action_plan = action_plan or {}
    metadata = metadata or {}
    identity = _identity(action_plan, metadata)
    strategy_side = str(identity.get("side") or "").upper()
    market = action_plan.get("market") if isinstance(action_plan.get("market"), dict) else {}
    brackets = action_plan.get("brackets") if isinstance(action_plan.get("brackets"), dict) else {}
    trade_id = trade_id or identity.get("trade_id")
    client_order_id = client_order_id or identity.get("client_order_id")
    intent = {
        "schema_version": SCHEMA_VERSION,
        "record_type": "execution_intent",
        "intent_type": "exit",
        "source": "execution_intent_engine",
        "venue": venue,
        "created_at": ts_i,
        "created_at_ct": _iso(ts_i, tz),
        "day": metadata.get("day") or _day(ts_i, tz),
        "action_plan_id": action_plan.get("action_plan_id"),
        "action_plan_hash": action_plan.get("action_plan_hash"),
        "semantic_action_hash": action_plan.get("semantic_action_hash"),
        "identity": {
            **identity,
            "trade_id": trade_id,
            "client_order_id": client_order_id,
        },
        "order": {
            "symbol": identity.get("ticker"),
            "strategy_side": strategy_side,
            "broker_side": _order_side("exit", strategy_side),
            "order_kind": order_kind,
            "qty": _num(qty),
            "exit_price": _num(exit_price if exit_price is not None else market.get("exit_price")),
            "tp_price": _num(brackets.get("tp_price")),
            "sl_price": _num(brackets.get("sl_price")),
            "client_order_id": client_order_id,
            "broker_order_id": broker_order_id,
            "reason": reason or action_plan.get("reason"),
        },
        "policy": {
            "strict_tp_sl": True,
            "close_position": True,
        },
        "contracts": action_plan.get("contracts") or metadata.get("contracts") or {},
        "metadata": metadata,
    }
    return _finalize_intent(intent)


def result_from_intent(intent: dict[str, Any],
                       status: str,
                       ok: bool | None = None,
                       ts: int | None = None,
                       broker_order_id: str | None = None,
                       broker_status: str | None = None,
                       fill_price: Any = None,
                       filled_qty: Any = None,
                       reason: str | None = None,
                       error: Any = None,
                       raw_response: Any = None,
                       latency: dict[str, Any] | None = None,
                       metadata: dict[str, Any] | None = None,
                       tz: tzinfo | None = CT) -> dict[str, Any]:
    ts_i = _now(ts)
    status_s = str(status or "unknown")
    if ok is None:
        ok = status_s not in {"rejected", "failed", "cancelled", "timed_out", "error"}
    order = intent.get("order") if isinstance(intent.get("order"), dict) else {}
    identity = intent.get("identity") if isinstance(intent.get("identity"), dict) else {}
    result = {
        "schema_version": SCHEMA_VERSION,
        "record_type": "execution_result",
        "source": "execution_intent_engine",
        "intent_type": intent.get("intent_type"),
        "venue": intent.get("venue"),
        "created_at": ts_i,
        "created_at_ct": _iso(ts_i, tz),
        "day": intent.get("day") or _day(ts_i, tz),
        "execution_intent_id": intent.get("execution_intent_id"),
        "execution_intent_hash": intent.get("execution_intent_hash"),
        "semantic_execution_intent_hash": intent.get("semantic_execution_intent_hash"),
        "action_plan_id": intent.get("action_plan_id"),
        "semantic_action_hash": intent.get("semantic_action_hash"),
        "identity": identity,
        "status": status_s,
        "ok": bool(ok),
        "reason": reason,
        "broker": {
            "broker_order_id": broker_order_id or order.get("broker_order_id"),
            "broker_status": broker_status,
            "raw_response": raw_response,
        },
        "fill": {
            "price": _num(fill_price),
            "qty": _num(filled_qty if filled_qty is not None else order.get("qty")),
        },
        "latency": latency or {},
        "error": error,
        "metadata": metadata or {},
    }
    result["execution_result_id"] = stable_hash({
        "schema_version": result.get("schema_version"),
        "execution_intent_id": result.get("execution_intent_id"),
        "status": result.get("status"),
        "broker": result.get("broker"),
        "fill": result.get("fill"),
        "reason": result.get("reason"),
        "created_at": result.get("created_at"),
    }, length=32)
    result["execution_result_hash"] = stable_hash(result)
    return result


def simulated_entry_result(intent: dict[str, Any], ts: int | None = None,
                           fill_price: Any = None) -> dict[str, Any]:
    order = intent.get("order") if isinstance(intent.get("order"), dict) else {}
    return result_from_intent(
        intent,
        "simulated_filled",
        ok=True,
        ts=ts,
        fill_price=fill_price if fill_price is not None else order.get("entry_price"),
        filled_qty=order.get("qty"),
        reason="step2_simulated_entry",
    )


def simulated_exit_result(intent: dict[str, Any], ts: int | None = None,
                          fill_price: Any = None, reason: str | None = None) -> dict[str, Any]:
    order = intent.get("order") if isinstance(intent.get("order"), dict) else {}
    return result_from_intent(
        intent,
        "simulated_closed",
        ok=True,
        ts=ts,
        fill_price=fill_price if fill_price is not None else order.get("exit_price"),
        filled_qty=order.get("qty"),
        reason=reason or order.get("reason") or "step2_simulated_exit",
    )


def validate_intent(intent: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for key in ("schema_version", "record_type", "intent_type", "venue", "identity", "order"):
        if intent.get(key) in (None, ""):
            errors.append(f"missing:{key}")
    if intent.get("schema_version") != SCHEMA_VERSION:
        errors.append("schema_version_mismatch")
    order = intent.get("order") if isinstance(intent.get("order"), dict) else {}
    for key in ("symbol", "strategy_side", "broker_side", "qty"):
        if order.get(key) in (None, ""):
            errors.append(f"order_missing:{key}")
    if not intent.get("execution_intent_id"):
        errors.append("missing:execution_intent_id")
    if not intent.get("semantic_execution_intent_hash"):
        errors.append("missing:semantic_execution_intent_hash")
    return errors


def validate_result(result: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for key in ("schema_version", "record_type", "intent_type", "status", "execution_intent_id"):
        if result.get(key) in (None, ""):
            errors.append(f"missing:{key}")
    if result.get("schema_version") != SCHEMA_VERSION:
        errors.append("schema_version_mismatch")
    if not result.get("execution_result_id"):
        errors.append("missing:execution_result_id")
    return errors
