"""Canonical execution-state reducer shared by Live and Step 2.

The reducer owns the deterministic view of pending entries, open positions,
closed trades, and same-ticker cooldown anchors. Broker-facing code can keep
rich payloads in its runtime dictionaries, but admission decisions should flow
through this reducer so Live and replay agree on state.
"""
from __future__ import annotations

import hashlib
import json
import time
from collections import Counter
from datetime import datetime, tzinfo
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import execution_kernel


CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1

EVENT_STAGE = {
    "signal_seen": "signal_seen",
    "signal_skipped": "rejected",
    "entry_intent_created": "intent_created",
    "entry_execution_result": "execution_result",
    "exit_intent_created": "intent_created",
    "exit_execution_result": "execution_result",
    "entry_reserved": "reserved",
    "entry_pre_submit_blocked": "rejected",
    "entry_submitted": "submitted",
    "entry_committed": "opened",
    "entry_filled": "filled",
    "entry_fill_timeout_cancelled": "cancelled",
    "bracket_exit_filled": "exit_filled",
    "local_strict_broker_cleanup_requested": "exit_requested",
    "local_strict_broker_cleanup_submitted": "exit_submitted",
    "broker_flat_verified": "broker_flat",
    "position_closed": "closed",
    "broker_orphan_bracket_recovered_on_boot": "recovered",
    "boot_position_mismatch": "reconcile_mismatch",
    "boot_internal_flat_reconcile": "reconciled",
    "critical_state_save_failed": "state_persistence_failed",
}

OPEN_STAGES = {"reserved", "submitted", "opened", "filled", "exit_requested", "exit_submitted", "exit_filled", "recovered"}
CLOSED_STAGES = {"closed", "cancelled", "broker_flat"}


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


def _day(ts: int | float | None = None, tz: tzinfo | None = CT) -> str:
    return datetime.fromtimestamp(float(ts or time.time()), tz).date().isoformat()


def _stable_hash(payload: Any) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def fresh_state(day: str | None = None) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "execution_state_reducer",
        "day": day,
        "positions": {},
        "pending_entries": {},
        "closed_trades": {},
        "last_closed_at": {},
        "stage_counts": {},
        "event_count": 0,
        "state_hash": None,
    }


def _refresh_hash(state: dict[str, Any]) -> dict[str, Any]:
    semantic = {
        "schema_version": state.get("schema_version"),
        "day": state.get("day"),
        "positions": state.get("positions") or {},
        "pending_entries": state.get("pending_entries") or {},
        "last_closed_at": state.get("last_closed_at") or {},
        "stage_counts": state.get("stage_counts") or {},
        "event_count": state.get("event_count") or 0,
    }
    state["state_hash"] = _stable_hash(semantic)
    return state


def _pending_from_payload(symbol: str, trade_id: str, data: dict[str, Any], ts: int) -> dict[str, Any]:
    return {
        "ticker": symbol,
        "trade_id": trade_id,
        "stage": "reserved",
        "created_at": _int(data.get("created_at") or ts, ts),
        "side": data.get("side"),
        "price": _num(data.get("price")),
        "qty": _num(data.get("qty")),
        "alloc": _num(data.get("alloc")),
        "client_order_id": data.get("client_order_id"),
        "broker_order_id": data.get("order_id") or data.get("broker_order_id") or data.get("alpaca_order_id"),
        "last_event_ts": ts,
    }


def _position_from_payload(symbol: str, trade_id: str, data: dict[str, Any], ts: int) -> dict[str, Any]:
    audit = data.get("decision_audit") if isinstance(data.get("decision_audit"), dict) else {}
    entry = data.get("entry", data.get("price", audit.get("price")))
    bracket = data.get("bracket_policy") if isinstance(data.get("bracket_policy"), dict) else {}
    return {
        "ticker": symbol,
        "trade_id": trade_id,
        "stage": "opened",
        "entry_ts": _int(data.get("entry_ts") or data.get("created_at") or audit.get("created_at") or ts, ts),
        "side": data.get("side") or audit.get("side"),
        "entry": _num(entry),
        "qty": _num(data.get("qty")),
        "alloc": _num(data.get("alloc")),
        "tp": _num(data.get("tp") or bracket.get("tp") or bracket.get("take_profit")),
        "sl": _num(data.get("sl") or bracket.get("sl") or bracket.get("stop_loss")),
        "client_order_id": data.get("client_order_id") or audit.get("client_order_id"),
        "broker_order_id": data.get("order_id") or data.get("broker_order_id") or data.get("alpaca_order_id"),
        "last_event_ts": ts,
    }


def normalize_event(row: dict[str, Any]) -> dict[str, Any] | None:
    event = str(row.get("event") or "")
    stage = str(row.get("stage") or EVENT_STAGE.get(event) or "")
    if not stage:
        return None
    data = row.get("data") if isinstance(row.get("data"), dict) else {}
    audit = data.get("decision_audit") if isinstance(data.get("decision_audit"), dict) else {}
    ts = _int(row.get("ts"), int(time.time()))
    symbol = _ticker(row.get("symbol") or data.get("ticker") or audit.get("ticker"))
    trade_id = str(row.get("trade_id") or data.get("trade_id") or audit.get("trade_id") or "")
    return {
        "schema_version": SCHEMA_VERSION,
        "event": event,
        "stage": stage,
        "ts": ts,
        "day": row.get("day") or _day(ts),
        "symbol": symbol,
        "trade_id": trade_id,
        "client_order_id": row.get("client_order_id") or data.get("client_order_id") or audit.get("client_order_id"),
        "broker_order_id": row.get("broker_order_id") or data.get("order_id") or data.get("broker_order_id"),
        "data": data,
    }


def apply_event(state: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
    event = normalize_event(row)
    if not event:
        return _refresh_hash(state)
    state.setdefault("schema_version", SCHEMA_VERSION)
    state.setdefault("source", "execution_state_reducer")
    state.setdefault("positions", {})
    state.setdefault("pending_entries", {})
    state.setdefault("closed_trades", {})
    state.setdefault("last_closed_at", {})
    state.setdefault("stage_counts", {})
    if not state.get("day"):
        state["day"] = event.get("day")
    stage = str(event.get("stage") or "")
    symbol = _ticker(event.get("symbol"))
    trade_id = str(event.get("trade_id") or "")
    data = event.get("data") if isinstance(event.get("data"), dict) else {}
    ts = _int(event.get("ts"), int(time.time()))
    counts = Counter(state.get("stage_counts") or {})
    counts[stage] += 1
    state["stage_counts"] = dict(sorted(counts.items()))
    state["event_count"] = _int(state.get("event_count")) + 1

    if not symbol:
        return _refresh_hash(state)
    if not trade_id and stage in OPEN_STAGES:
        trade_id = f"{symbol}-{ts}-unknown"

    if stage == "reserved":
        if trade_id:
            state["pending_entries"][symbol] = _pending_from_payload(symbol, trade_id, data, ts)
        return _refresh_hash(state)

    if stage == "submitted":
        pending = dict((state.get("pending_entries") or {}).get(symbol) or {})
        if not pending and trade_id:
            pending = _pending_from_payload(symbol, trade_id, data, ts)
        pending.update({
            "stage": "submitted",
            "trade_id": trade_id or pending.get("trade_id"),
            "client_order_id": event.get("client_order_id") or pending.get("client_order_id"),
            "broker_order_id": event.get("broker_order_id") or pending.get("broker_order_id"),
            "last_event_ts": ts,
        })
        if pending.get("trade_id"):
            state["pending_entries"][symbol] = pending
        return _refresh_hash(state)

    if stage in {"opened", "filled", "recovered"}:
        pending = dict((state.get("pending_entries") or {}).get(symbol) or {})
        position = _position_from_payload(symbol, trade_id or pending.get("trade_id") or f"{symbol}-{ts}-unknown", data, ts)
        if pending:
            for key, value in pending.items():
                position.setdefault(key, value)
        position["stage"] = stage
        position["last_event_ts"] = ts
        state["pending_entries"].pop(symbol, None)
        state["positions"][symbol] = position
        return _refresh_hash(state)

    if stage in {"exit_requested", "exit_submitted", "exit_filled"}:
        pos = dict((state.get("positions") or {}).get(symbol) or {})
        if pos:
            pos["stage"] = stage
            pos["last_event_ts"] = ts
            pos["exit_state"] = {
                "stage": stage,
                "event": event.get("event"),
                "reason": data.get("reason"),
                "price": _num(data.get("exit_price") or data.get("price") or data.get("exit")),
                "broker_order_id": event.get("broker_order_id") or data.get("order_id"),
                "ts": ts,
            }
            state["positions"][symbol] = pos
        return _refresh_hash(state)

    if stage in CLOSED_STAGES:
        if not trade_id:
            current = (state.get("positions") or {}).get(symbol) or (state.get("pending_entries") or {}).get(symbol) or {}
            trade_id = str(current.get("trade_id") or "")
        prior = dict((state.get("positions") or {}).pop(symbol, {}) or {})
        state["pending_entries"].pop(symbol, None)
        if trade_id:
            state["closed_trades"][trade_id] = {
                "ticker": symbol,
                "trade_id": trade_id,
                "stage": stage,
                "closed_at": ts,
                "reason": data.get("reason"),
                "pnl": _num(data.get("pnl")),
                "prior_stage": prior.get("stage"),
            }
        state["last_closed_at"][symbol] = ts
        return _refresh_hash(state)

    if stage == "rejected":
        if event.get("event") == "entry_pre_submit_blocked":
            state["pending_entries"].pop(symbol, None)
        return _refresh_hash(state)

    return _refresh_hash(state)


def from_events(rows: list[dict[str, Any]], day: str | None = None) -> dict[str, Any]:
    state = fresh_state(day)
    for row in rows:
        apply_event(state, row)
    return _refresh_hash(state)


def from_runtime_state(runtime_state: dict[str, Any], day: str | None = None) -> dict[str, Any]:
    state = fresh_state(day)
    for ticker, pending in (runtime_state.get("pending_entries") or {}).items():
        pending = dict(pending or {})
        trade_id = str(pending.get("trade_id") or f"{_ticker(ticker)}-{pending.get('created_at') or 0}-pending")
        state["pending_entries"][_ticker(ticker)] = _pending_from_payload(
            _ticker(ticker),
            trade_id,
            pending,
            _int(pending.get("created_at"), int(time.time())),
        )
    for ticker, pos in (runtime_state.get("positions") or {}).items():
        pos = dict(pos or {})
        trade_id = str(pos.get("trade_id") or f"{_ticker(ticker)}-{pos.get('entry_ts') or 0}-open")
        data = dict(pos)
        data["entry"] = pos.get("entry")
        state["positions"][_ticker(ticker)] = _position_from_payload(
            _ticker(ticker),
            trade_id,
            data,
            _int(pos.get("entry_ts"), int(time.time())),
        )
    for row in runtime_state.get("trades", []) or []:
        ticker = _ticker(row.get("ticker"))
        if not ticker:
            continue
        closed_at = _int(row.get("closed_at"), 0)
        if closed_at > 0:
            state["last_closed_at"][ticker] = max(_int(state["last_closed_at"].get(ticker), 0), closed_at)
            trade_id = str(row.get("trade_id") or f"{ticker}-{row.get('opened_at') or 0}-{closed_at}")
            state["closed_trades"][trade_id] = {
                "ticker": ticker,
                "trade_id": trade_id,
                "stage": "closed",
                "closed_at": closed_at,
                "reason": row.get("reason"),
                "pnl": _num(row.get("pnl")),
            }
    return _refresh_hash(state)


def bootstrap_runtime_state(runtime_state: dict[str, Any], day: str | None = None) -> dict[str, Any]:
    runtime_state["execution_state"] = from_runtime_state(runtime_state, day=day)
    return runtime_state["execution_state"]


def apply_audit_row(runtime_state: dict[str, Any], audit_row: dict[str, Any]) -> dict[str, Any]:
    ex_state = runtime_state.setdefault("execution_state", fresh_state())
    apply_event(ex_state, audit_row)
    return ex_state


def effective_view(runtime_state: dict[str, Any]) -> dict[str, Any]:
    ex_state = runtime_state.get("execution_state") if isinstance(runtime_state.get("execution_state"), dict) else {}
    positions = {
        _ticker(k): dict(v or {})
        for k, v in (ex_state.get("positions") or {}).items()
    }
    pending = {
        _ticker(k): dict(v or {})
        for k, v in (ex_state.get("pending_entries") or {}).items()
    }
    for ticker, pos in (runtime_state.get("positions") or {}).items():
        positions.setdefault(_ticker(ticker), dict(pos or {}))
    for ticker, row in (runtime_state.get("pending_entries") or {}).items():
        pending.setdefault(_ticker(ticker), dict(row or {}))
    last_closed = dict(ex_state.get("last_closed_at") or {})
    for row in runtime_state.get("trades", []) or []:
        ticker = _ticker(row.get("ticker"))
        closed_at = _int(row.get("closed_at"), 0)
        if ticker and closed_at > 0:
            last_closed[ticker] = max(_int(last_closed.get(ticker), 0), closed_at)
    return {
        "positions": positions,
        "pending_entries": pending,
        "last_closed_at": last_closed,
        "state_hash": ex_state.get("state_hash"),
    }


def entry_block_reason(runtime_state: dict[str, Any], ticker: str, signal_ts: int, day_iso: str,
                       contract: dict[str, Any], signal_scan_live_mode: bool = True,
                       tz: tzinfo | None = CT) -> str | None:
    view = effective_view(runtime_state)
    ticker_s = _ticker(ticker)
    if bool(contract.get("one_open_position_per_ticker", True)) and ticker_s in view["positions"]:
        return "already_in_position"
    if ticker_s in view["pending_entries"]:
        return "entry_pending"
    if not signal_scan_live_mode:
        return None
    cooldown = _int(contract.get("same_ticker_reentry_cooldown_sec"), 0)
    if cooldown > 0:
        closed_at = _int((view.get("last_closed_at") or {}).get(ticker_s), 0)
        until = closed_at + cooldown
        gate = execution_kernel.can_enter_ticker(signal_ts, until)
        if closed_at > 0 and not gate.get("allowed"):
            return (
                f"step2_reentry_cooldown:{ticker_s} "
                f"closed_at={closed_at} until={until} remaining_sec={max(0, until - int(signal_ts))}"
            )
    max_day = _int(contract.get("max_trades_per_day"), 0)
    max_ticker = _int(contract.get("max_trades_per_ticker_day"), 0)
    if max_day <= 0 and max_ticker <= 0:
        return None
    counts_total = 0
    counts_ticker = 0
    for row in runtime_state.get("trades", []) or []:
        opened = _int(row.get("opened_at") or row.get("entry_ts") or row.get("created_at"), 0)
        if opened <= 0 or _day(opened, tz) != day_iso:
            continue
        counts_total += 1
        if _ticker(row.get("ticker")) == ticker_s:
            counts_ticker += 1
    for rows in (view["positions"], view["pending_entries"]):
        for sym, item in rows.items():
            opened = _int(item.get("entry_ts") or item.get("created_at"), 0)
            if opened > 0 and _day(opened, tz) == day_iso:
                counts_total += 1
                if _ticker(sym) == ticker_s:
                    counts_ticker += 1
    if max_day > 0 and counts_total >= max_day:
        return f"step2_density_cap_day:{counts_total}/{max_day}"
    if max_ticker > 0 and counts_ticker >= max_ticker:
        return f"step2_density_cap_ticker:{ticker_s}={counts_ticker}/{max_ticker}"
    return None


def compare_runtime_view(runtime_state: dict[str, Any]) -> dict[str, Any]:
    ex = runtime_state.get("execution_state") if isinstance(runtime_state.get("execution_state"), dict) else {}
    ex_positions = set(_ticker(k) for k in (ex.get("positions") or {}).keys())
    ex_pending = set(_ticker(k) for k in (ex.get("pending_entries") or {}).keys())
    rt_positions = set(_ticker(k) for k in (runtime_state.get("positions") or {}).keys())
    rt_pending = set(_ticker(k) for k in (runtime_state.get("pending_entries") or {}).keys())
    mismatches = []
    if ex_positions != rt_positions:
        mismatches.append({
            "kind": "positions_mismatch",
            "execution_state_only": sorted(ex_positions - rt_positions),
            "runtime_only": sorted(rt_positions - ex_positions),
        })
    if ex_pending != rt_pending:
        mismatches.append({
            "kind": "pending_entries_mismatch",
            "execution_state_only": sorted(ex_pending - rt_pending),
            "runtime_only": sorted(rt_pending - ex_pending),
        })
    return {
        "schema_version": SCHEMA_VERSION,
        "ok": not mismatches,
        "mismatches": mismatches,
        "execution_state_hash": ex.get("state_hash"),
    }
