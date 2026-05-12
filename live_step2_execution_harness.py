"""Deterministic Live-vs-Step2 execution contract harness.

The goal is not to replay market data. It is to prove, before trading, that
Live-style broker execution and Step 2 simulation still agree on the execution
rules that matter: entry admission, bracket prices, close reasons, cooldowns,
and terminal lifecycle events.
"""
from __future__ import annotations

import argparse
import copy
import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, time as dt_time
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import broker_lifecycle_guard
import execution_action_engine
import execution_adapters
import execution_intent_engine
import live_step2_decision_kernel
import step2_execution_contract


HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, "postmortem", "execution_contract_harness")
CONFIG_PATH = os.path.join(HERE, "trading_config.json")
CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1
DEFAULT_DAY = "2026-05-08"
DEFAULT_TICKERS = ("CLSK", "MARA", "RIOT")


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


def _read_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except Exception:
        return default


def _write_json(path: str, payload: dict[str, Any]) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.{os.getpid()}.{int(time.time() * 1000)}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
        f.write("\n")
    os.replace(tmp, path)
    return os.path.abspath(path)


def _session_ts(day: str, hour: int, minute: int, second: int = 0) -> int:
    date = datetime.fromisoformat(day).date()
    return int(datetime.combine(date, dt_time(hour, minute, second), CT).timestamp())


def _ticker(value: Any) -> str:
    return str(value or "").upper()


def _round4(value: Any) -> float | None:
    try:
        return round(float(value), 4)
    except Exception:
        return None


def _pnl(side: str, entry: float, exit_price: float, qty: float) -> float:
    if str(side).upper() == "LONG":
        return round((float(exit_price) - float(entry)) * float(qty), 2)
    return round((float(entry) - float(exit_price)) * float(qty), 2)


@dataclass
class FakeBroker:
    """Tiny scripted Alpaca-like broker used only by the harness."""

    behavior: str = "normal"
    orders: list[dict[str, Any]] = field(default_factory=list)
    positions: dict[str, dict[str, Any]] = field(default_factory=dict)
    events: list[dict[str, Any]] = field(default_factory=list)
    next_id: int = 1

    def _id(self, prefix: str) -> str:
        value = f"{prefix}-{self.next_id:04d}"
        self.next_id += 1
        return value

    def submit_bracket_order(self, symbol: str, qty: float, side: str,
                             tp_price: float, sl_price: float,
                             client_order_id: str | None = None) -> dict[str, Any]:
        symbol = _ticker(symbol)
        if self.behavior == "reject_entry":
            order = {
                "id": self._id("rejected"),
                "client_order_id": client_order_id,
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "status": "rejected",
                "reject_reason": "scripted_reject_entry",
            }
            self.orders.append(order)
            self.events.append({"event": "submit_rejected", "order": order})
            return order
        status = "partially_filled" if self.behavior == "partial_then_fill" else "filled"
        order = {
            "id": self._id("entry"),
            "client_order_id": client_order_id,
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "filled_qty": qty if status == "filled" else max(1, float(qty) / 2.0),
            "status": status,
            "order_class": "bracket",
            "take_profit": {"limit_price": tp_price},
            "stop_loss": {"stop_price": sl_price},
            "legs": [
                {"id": self._id("tp"), "symbol": symbol, "type": "limit", "limit_price": tp_price, "status": "new"},
                {"id": self._id("sl"), "symbol": symbol, "type": "stop", "stop_price": sl_price, "status": "new"},
            ],
        }
        self.orders.append(order)
        if status == "filled":
            self.positions[symbol] = {
                "symbol": symbol,
                "side": "long" if side == "buy" else "short",
                "qty": qty,
                "avg_entry_price": None,
                "order_id": order["id"],
                "client_order_id": client_order_id,
            }
        self.events.append({"event": "submit_bracket_order", "order": order})
        return order

    def complete_partial_fill(self, order: dict[str, Any]) -> dict[str, Any]:
        order["status"] = "filled"
        order["filled_qty"] = order.get("qty")
        symbol = _ticker(order.get("symbol"))
        self.positions[symbol] = {
            "symbol": symbol,
            "side": "long" if order.get("side") == "buy" else "short",
            "qty": order.get("qty"),
            "avg_entry_price": None,
            "order_id": order.get("id"),
            "client_order_id": order.get("client_order_id"),
        }
        self.events.append({"event": "partial_fill_completed", "order_id": order.get("id")})
        return order

    def close_position(self, symbol: str) -> dict[str, Any]:
        symbol = _ticker(symbol)
        prior = self.positions.pop(symbol, None)
        response = {
            "id": self._id("close"),
            "symbol": symbol,
            "status": "filled" if prior else "not_found",
            "prior_position": prior,
        }
        self.events.append({"event": "close_position", "response": response})
        return response

    def list_positions(self) -> list[dict[str, Any]]:
        return list(self.positions.values())

    def list_orders(self, status: str = "open", symbols: list[str] | None = None, nested: bool = True) -> list[dict[str, Any]]:
        wanted = {_ticker(s) for s in (symbols or [])}
        rows = [o for o in self.orders if not wanted or _ticker(o.get("symbol")) in wanted]
        if status == "open":
            rows = [o for o in rows if str(o.get("status") or "").lower() not in {"filled", "rejected", "canceled", "cancelled"}]
        return rows


def _default_contract(config: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = config if config is not None else (_read_json(CONFIG_PATH, {}) or {})
    return step2_execution_contract.execution_contract(cfg)


def _base_signal(ticker: str, side: str, price: float, ts: int) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "side": side,
        "price": price,
        "score": 7.0,
        "conviction": "HIGH",
        "setup_type": "harness_contract",
        "opportunity_id": f"harness-{ticker}-{ts}",
        "reasons": ["execution_contract_harness"],
    }


def _scenario_definitions(day: str) -> list[dict[str, Any]]:
    base_ts = _session_ts(day, 8, 30, 0)
    return [
        {
            "name": "long_take_profit_round_trip",
            "kind": "round_trip",
            "signal": _base_signal("CLSK", "LONG", 10.00, base_ts + 1),
            "ts": base_ts + 1,
            "ticks": [{"ts": base_ts + 6, "price": 10.05, "in_trading_window": True}],
            "expected": {"entry_decision": "accepted", "exit_reason": "take_profit", "exit_price": 10.04},
        },
        {
            "name": "short_stop_loss_round_trip",
            "kind": "round_trip",
            "signal": _base_signal("MARA", "SHORT", 20.00, base_ts + 11),
            "ts": base_ts + 11,
            "ticks": [{"ts": base_ts + 20, "price": 20.09, "in_trading_window": True}],
            "expected": {"entry_decision": "accepted", "exit_reason": "stop_loss", "exit_price": 20.08},
        },
        {
            "name": "session_end_exit",
            "kind": "round_trip",
            "signal": _base_signal("RIOT", "LONG", 30.00, base_ts + 30),
            "ts": base_ts + 30,
            "ticks": [{"ts": _session_ts(day, 14, 56, 0), "price": 30.01, "in_trading_window": False}],
            "expected": {"entry_decision": "accepted", "exit_reason": "session_end", "exit_price": 30.01},
        },
        {
            "name": "same_ticker_pending_entry_blocks",
            "kind": "entry_skip",
            "state": {
                "pending_entries": {
                    "CLSK": {"trade_id": "existing-pending", "created_at": base_ts, "side": "LONG", "price": 10.0}
                }
            },
            "signal": _base_signal("CLSK", "LONG", 10.01, base_ts + 40),
            "ts": base_ts + 40,
            "expected": {"entry_decision": "skipped", "reason_prefix": "entry_pending"},
        },
        {
            "name": "same_ticker_open_position_blocks",
            "kind": "entry_skip",
            "state": {
                "positions": {
                    "MARA": {"trade_id": "existing-open", "entry_ts": base_ts, "side": "LONG", "entry": 20.0, "tp": 20.08, "sl": 19.92, "qty": 10}
                }
            },
            "signal": _base_signal("MARA", "LONG", 20.01, base_ts + 50),
            "ts": base_ts + 50,
            "expected": {"entry_decision": "skipped", "reason_prefix": "already_in_position"},
        },
        {
            "name": "close_based_reentry_cooldown_blocks",
            "kind": "entry_skip",
            "state": {
                "trades": [
                    {"ticker": "RIOT", "trade_id": "closed-riot", "opened_at": base_ts, "closed_at": base_ts + 60, "reason": "take_profit", "pnl": 1.0}
                ]
            },
            "signal": _base_signal("RIOT", "LONG", 30.01, base_ts + 63),
            "ts": base_ts + 63,
            "expected": {"entry_decision": "skipped", "reason_prefix": "step2_reentry_cooldown:RIOT"},
        },
        {
            "name": "close_based_reentry_cooldown_allows_after_5s",
            "kind": "entry_only",
            "state": {
                "trades": [
                    {"ticker": "RIOT", "trade_id": "closed-riot", "opened_at": base_ts, "closed_at": base_ts + 70, "reason": "take_profit", "pnl": 1.0}
                ]
            },
            "signal": _base_signal("RIOT", "LONG", 30.01, base_ts + 75),
            "ts": base_ts + 75,
            "expected": {"entry_decision": "accepted"},
        },
        {
            "name": "broker_rejected_entry_terminal",
            "kind": "broker_reject",
            "broker_behavior": "reject_entry",
            "signal": _base_signal("CLSK", "LONG", 10.00, base_ts + 80),
            "ts": base_ts + 80,
            "expected": {"step2_entry_decision": "accepted", "live_terminal_event": "entry_fill_timeout_cancelled"},
        },
        {
            "name": "partial_then_late_fill_take_profit",
            "kind": "round_trip",
            "broker_behavior": "partial_then_fill",
            "signal": _base_signal("CLSK", "LONG", 10.00, base_ts + 90),
            "ts": base_ts + 90,
            "ticks": [{"ts": base_ts + 98, "price": 10.05, "in_trading_window": True}],
            "expected": {"entry_decision": "accepted", "exit_reason": "take_profit", "exit_price": 10.04},
        },
        {
            "name": "duplicate_close_attempt_no_second_close",
            "kind": "round_trip",
            "duplicate_close_attempt": True,
            "signal": _base_signal("MARA", "SHORT", 20.00, base_ts + 110),
            "ts": base_ts + 110,
            "ticks": [
                {"ts": base_ts + 116, "price": 19.90, "in_trading_window": True},
                {"ts": base_ts + 117, "price": 19.88, "in_trading_window": True},
            ],
            "expected": {"entry_decision": "accepted", "exit_reason": "take_profit", "exit_price": 19.92, "closed_count": 1},
        },
        {
            "name": "known_broker_bracket_startup_guard_ok",
            "kind": "broker_guard",
            "guard_state": {
                "positions": {
                    "CLSK": {"trade_id": "guard-known", "client_order_id": "cid-known", "alpaca_order_id": "oid-known", "side": "LONG", "qty": 10}
                },
                "pending_entries": {},
            },
            "broker_positions": [{"symbol": "CLSK", "side": "long", "qty": 10, "avg_entry_price": 10.0}],
            "broker_orders": [{"symbol": "CLSK", "id": "oid-known", "client_order_id": "cid-known", "status": "new", "legs": []}],
            "expected": {"broker_guard_ok": True},
        },
        {
            "name": "orphan_broker_bracket_startup_guard_blocks",
            "kind": "broker_guard",
            "guard_state": {"positions": {}, "pending_entries": {}},
            "broker_positions": [{"symbol": "CLSK", "side": "long", "qty": 10, "avg_entry_price": 10.0}],
            "broker_orders": [{"symbol": "CLSK", "id": "oid-orphan", "client_order_id": "cid-orphan", "status": "new", "legs": []}],
            "expected": {"broker_guard_ok": False, "issue_kind": "broker_position_untracked"},
        },
    ]


def _entry_decision(state: dict[str, Any], scenario: dict[str, Any],
                    contract: dict[str, Any], broker_action: str,
                    client_order_id: str | None) -> dict[str, Any]:
    signal = dict(scenario["signal"])
    price = float(signal["price"])
    side = signal["side"]
    brackets = step2_execution_contract.exit_brackets(side, price, 0.004, 0.004)
    qty = float(scenario.get("qty") or 10)
    alloc = round(qty * price, 2)
    trade_id = scenario.get("trade_id") or f"{signal['ticker']}-{scenario['ts']}-harness"
    return live_step2_decision_kernel.entry_decision(
        state,
        signal["ticker"],
        int(scenario["ts"]),
        contract,
        True,
        CT,
        signal=signal,
        entry_price=price,
        sl_pct=0.004,
        tp_pct=0.004,
        brackets=brackets,
        qty=qty,
        alloc=alloc,
        trade_id=trade_id,
        client_order_id=client_order_id,
        broker_action=broker_action,
        metadata={
            "day": datetime.fromtimestamp(int(scenario["ts"]), CT).date().isoformat(),
            "execution_mode": "execution_contract_harness",
            "step2_execution_contract_hash": step2_execution_contract.execution_contract_hash({}),
        },
    )


def _entry_artifacts(decision: dict[str, Any], scenario: dict[str, Any]) -> dict[str, Any]:
    signal = scenario["signal"]
    plan = decision.get("action_plan") or {}
    brackets = plan.get("brackets") or {}
    return {
        "ticker": signal.get("ticker"),
        "side": signal.get("side"),
        "trade_id": (plan.get("identity") or {}).get("trade_id") or f"{signal['ticker']}-{scenario['ts']}-harness",
        "client_order_id": (plan.get("identity") or {}).get("client_order_id") or f"cid-{scenario['name']}",
        "entry": float(signal["price"]),
        "qty": float(scenario.get("qty") or 10),
        "alloc": round(float(signal["price"]) * float(scenario.get("qty") or 10), 2),
        "tp": _round4(brackets.get("tp_price")),
        "sl": _round4(brackets.get("sl_price")),
        "action_plan": plan,
    }


def _commit_entry(adapter: execution_adapters.BaseExecutionAdapter,
                  artifacts: dict[str, Any],
                  broker_order_id: str | None,
                  ts: int) -> None:
    adapter.commit_entry(
        artifacts["ticker"],
        artifacts["trade_id"],
        artifacts["side"],
        artifacts["entry"],
        qty=artifacts["qty"],
        alloc=artifacts["alloc"],
        tp=artifacts["tp"],
        sl=artifacts["sl"],
        client_order_id=artifacts["client_order_id"],
        broker_order_id=broker_order_id,
        ts=ts,
        extra={"entry_action_plan": artifacts["action_plan"]},
    )
    adapter.emit(
        "entry_filled",
        artifacts["ticker"],
        artifacts["trade_id"],
        {
            "trade_id": artifacts["trade_id"],
            "side": artifacts["side"],
            "entry": artifacts["entry"],
            "fill_price": artifacts["entry"],
            "qty": artifacts["qty"],
            "alloc": artifacts["alloc"],
            "tp": artifacts["tp"],
            "sl": artifacts["sl"],
            "client_order_id": artifacts["client_order_id"],
            "order_id": broker_order_id,
        },
        ts=ts,
    )


def _current_position(adapter: execution_adapters.BaseExecutionAdapter, ticker: str) -> dict[str, Any] | None:
    return (adapter.snapshot().get("positions") or {}).get(_ticker(ticker))


def _close_from_tick(adapter: execution_adapters.BaseExecutionAdapter,
                     artifacts: dict[str, Any],
                     tick: dict[str, Any],
                     contract: dict[str, Any],
                     venue: str,
                     broker: FakeBroker | None = None,
                     broker_order_id: str | None = None) -> dict[str, Any] | None:
    pos = _current_position(adapter, artifacts["ticker"])
    if not pos:
        return None
    decision = live_step2_decision_kernel.exit_decision(
        pos.get("side"),
        float(tick["price"]),
        float(pos["sl"]),
        float(pos["tp"]),
        bool(tick.get("in_trading_window", True)),
        entry_price=pos.get("entry"),
        entry_ts=pos.get("entry_ts"),
        now_ts=int(tick["ts"]),
        conditional_time_stop_enabled=False,
        conditional_time_stop_min=0,
        contract=contract,
        metadata={
            "ticker": artifacts["ticker"],
            "trade_id": artifacts["trade_id"],
            "setup_type": "harness_contract",
        },
    )
    if decision.get("decision") != "exit":
        return {"decision": decision, "closed": False}
    action_plan = decision.get("action_plan") or {}
    intent = execution_intent_engine.exit_intent(
        action_plan,
        venue=venue,
        qty=pos.get("qty"),
        trade_id=artifacts["trade_id"],
        client_order_id=artifacts["client_order_id"],
        broker_order_id=broker_order_id,
        exit_price=decision.get("exit_price"),
        reason=decision.get("reason"),
        order_kind="local_strict_exit" if venue == "alpaca" else "simulated_exit",
        ts=int(tick["ts"]),
    )
    result = execution_intent_engine.result_from_intent(
        intent,
        "local_strict_triggered" if venue == "alpaca" else "simulated_closed",
        ok=True,
        ts=int(tick["ts"]),
        broker_order_id=broker_order_id,
        broker_status="filled" if venue == "alpaca" else None,
        fill_price=decision.get("exit_price"),
        filled_qty=pos.get("qty"),
        reason=decision.get("reason"),
    )
    adapter.emit_intent(intent, ts=int(tick["ts"]))
    adapter.emit_result(result, ts=int(tick["ts"]))
    if broker is not None:
        broker.close_position(artifacts["ticker"])
    adapter.close_position(
        artifacts["ticker"],
        artifacts["trade_id"],
        artifacts["side"],
        str(decision.get("reason")),
        float(decision.get("exit_price")),
        pnl=_pnl(artifacts["side"], artifacts["entry"], float(decision.get("exit_price")), artifacts["qty"]),
        broker_order_id=broker_order_id,
        ts=int(tick["ts"]),
    )
    return {"decision": decision, "closed": True, "result": result}


def _semantic_result(name: str, adapter: execution_adapters.BaseExecutionAdapter,
                     entry_decision: dict[str, Any] | None,
                     close_result: dict[str, Any] | None,
                     extra: dict[str, Any] | None = None) -> dict[str, Any]:
    events = [str(row.get("event") or "") for row in adapter.events]
    snapshot = adapter.snapshot()
    payload = {
        "name": name,
        "entry_decision": (entry_decision or {}).get("decision"),
        "entry_reason": (entry_decision or {}).get("reason"),
        "entry_brackets": ((entry_decision or {}).get("action_plan") or {}).get("brackets") or {},
        "exit_reason": ((close_result or {}).get("decision") or {}).get("reason"),
        "exit_price": _round4(((close_result or {}).get("decision") or {}).get("exit_price")),
        "events": events,
        "milestones": {
            "signal_to_reserved": "entry_reserved" in events,
            "intent_created": "entry_intent_created" in events,
            "execution_result": "entry_execution_result" in events,
            "entry_submitted": "entry_submitted" in events,
            "entry_committed": "entry_committed" in events,
            "entry_filled": "entry_filled" in events,
            "exit_intent": "exit_intent_created" in events,
            "exit_result": "exit_execution_result" in events,
            "closed": "position_closed" in events,
            "entry_terminal_cancel": "entry_fill_timeout_cancelled" in events,
        },
        "closed_count": events.count("position_closed"),
        "final_positions": sorted((snapshot.get("positions") or {}).keys()),
        "final_pending_entries": sorted((snapshot.get("pending_entries") or {}).keys()),
        "last_closed_at": snapshot.get("last_closed_at") or {},
    }
    if extra:
        payload.update(extra)
    return payload


def _run_step2_execution(scenario: dict[str, Any], contract: dict[str, Any]) -> dict[str, Any]:
    state = copy.deepcopy(scenario.get("state") or {})
    adapter = execution_adapters.ReplayExecutionAdapter(
        day=datetime.fromtimestamp(int(scenario["ts"]), CT).date().isoformat(),
        runtime_state=state,
    )
    decision = _entry_decision(state, scenario, contract, "commit_sim_position", f"cid-{scenario['name']}")
    if decision.get("decision") == "skipped":
        adapter.emit("signal_skipped", scenario["signal"]["ticker"], data={"reason": decision.get("reason")}, ts=scenario["ts"])
        return _semantic_result(scenario["name"], adapter, decision, None)
    artifacts = _entry_artifacts(decision, scenario)
    adapter.reserve_entry(artifacts["ticker"], artifacts["trade_id"], artifacts["side"], artifacts["entry"], artifacts["qty"], artifacts["alloc"], artifacts["client_order_id"], ts=scenario["ts"])
    adapter.create_simulated_entry(artifacts["action_plan"], artifacts["qty"], artifacts["trade_id"], ts=scenario["ts"])
    _commit_entry(adapter, artifacts, None, int(scenario["ts"]))
    close_result = None
    for tick in scenario.get("ticks") or []:
        result = _close_from_tick(adapter, artifacts, tick, contract, "step2_sim")
        if result is not None:
            close_result = result
        if result and result.get("closed") and not scenario.get("duplicate_close_attempt"):
            break
    return _semantic_result(scenario["name"], adapter, decision, close_result)


def _run_live_execution(scenario: dict[str, Any], contract: dict[str, Any]) -> dict[str, Any]:
    state = copy.deepcopy(scenario.get("state") or {})
    adapter = execution_adapters.LiveExecutionAdapter(
        day=datetime.fromtimestamp(int(scenario["ts"]), CT).date().isoformat(),
        runtime_state=state,
    )
    client_order_id = f"cid-{scenario['name']}"
    broker = FakeBroker(behavior=str(scenario.get("broker_behavior") or "normal"))
    decision = _entry_decision(state, scenario, contract, "submit_bracket_order", client_order_id)
    if decision.get("decision") == "skipped":
        adapter.emit("signal_skipped", scenario["signal"]["ticker"], data={"reason": decision.get("reason")}, ts=scenario["ts"])
        return _semantic_result(scenario["name"], adapter, decision, None, {"broker_events": broker.events})
    artifacts = _entry_artifacts(decision, scenario)
    adapter.reserve_entry(artifacts["ticker"], artifacts["trade_id"], artifacts["side"], artifacts["entry"], artifacts["qty"], artifacts["alloc"], artifacts["client_order_id"], ts=scenario["ts"])
    intent = execution_intent_engine.entry_intent(
        artifacts["action_plan"],
        venue="alpaca",
        qty=artifacts["qty"],
        trade_id=artifacts["trade_id"],
        client_order_id=artifacts["client_order_id"],
        tp_price=artifacts["tp"],
        sl_price=artifacts["sl"],
        entry_price=artifacts["entry"],
        ts=int(scenario["ts"]),
    )
    adapter.emit_intent(intent, ts=int(scenario["ts"]))
    broker_side = "buy" if artifacts["side"] == "LONG" else "sell"
    order = broker.submit_bracket_order(
        artifacts["ticker"],
        artifacts["qty"],
        broker_side,
        artifacts["tp"],
        artifacts["sl"],
        artifacts["client_order_id"],
    )
    adapter.submit_entry(
        artifacts["ticker"],
        artifacts["trade_id"],
        artifacts["side"],
        broker_order_id=order.get("id"),
        client_order_id=artifacts["client_order_id"],
        ts=int(scenario["ts"]),
    )
    if str(order.get("status") or "").lower() == "rejected":
        result = execution_intent_engine.result_from_intent(
            intent,
            "rejected",
            ok=False,
            ts=int(scenario["ts"]),
            broker_order_id=order.get("id"),
            broker_status=order.get("status"),
            reason=order.get("reject_reason"),
            raw_response=order,
        )
        adapter.emit_result(result, ts=int(scenario["ts"]))
        adapter.emit(
            "entry_fill_timeout_cancelled",
            artifacts["ticker"],
            artifacts["trade_id"],
            {"trade_id": artifacts["trade_id"], "reason": "broker_rejected", "order_id": order.get("id")},
            ts=int(scenario["ts"]) + 1,
        )
        return _semantic_result(scenario["name"], adapter, decision, None, {"broker_events": broker.events})
    if str(order.get("status") or "").lower() == "partially_filled":
        result = execution_intent_engine.result_from_intent(
            intent,
            "partially_filled",
            ok=True,
            ts=int(scenario["ts"]),
            broker_order_id=order.get("id"),
            broker_status=order.get("status"),
            fill_price=artifacts["entry"],
            filled_qty=order.get("filled_qty"),
            raw_response=order,
        )
        adapter.emit_result(result, ts=int(scenario["ts"]))
        order = broker.complete_partial_fill(order)
    else:
        result = execution_intent_engine.result_from_intent(
            intent,
            "filled",
            ok=True,
            ts=int(scenario["ts"]),
            broker_order_id=order.get("id"),
            broker_status=order.get("status"),
            fill_price=artifacts["entry"],
            filled_qty=artifacts["qty"],
            raw_response=order,
        )
        adapter.emit_result(result, ts=int(scenario["ts"]))
    _commit_entry(adapter, artifacts, order.get("id"), int(scenario["ts"]) + (2 if scenario.get("broker_behavior") == "partial_then_fill" else 0))
    close_result = None
    for tick in scenario.get("ticks") or []:
        result = _close_from_tick(adapter, artifacts, tick, contract, "alpaca", broker, order.get("id"))
        if result is not None:
            close_result = result
        if result and result.get("closed") and not scenario.get("duplicate_close_attempt"):
            break
    return _semantic_result(scenario["name"], adapter, decision, close_result, {"broker_events": broker.events})


def _run_broker_guard(scenario: dict[str, Any], day: str) -> dict[str, Any]:
    payload = broker_lifecycle_guard.evaluate_runtime_broker_state(
        copy.deepcopy(scenario.get("guard_state") or {}),
        broker_positions=copy.deepcopy(scenario.get("broker_positions") or []),
        broker_orders=copy.deepcopy(scenario.get("broker_orders") or []),
        watched=list(DEFAULT_TICKERS),
        day=day,
        broker_reachable=True,
        label=scenario["name"],
    )
    expected = scenario.get("expected") or {}
    checks = [
        {
            "name": "broker_guard_expected_verdict",
            "ok": bool(payload.get("ok")) == bool(expected.get("broker_guard_ok")),
            "actual": payload.get("ok"),
            "expected": expected.get("broker_guard_ok"),
        }
    ]
    if expected.get("issue_kind"):
        kinds = {row.get("kind") for row in payload.get("issues") or []}
        checks.append({
            "name": "broker_guard_expected_issue_kind",
            "ok": expected["issue_kind"] in kinds,
            "actual": sorted(kinds),
            "expected": expected["issue_kind"],
        })
    return {
        "name": scenario["name"],
        "kind": scenario["kind"],
        "ok": all(row.get("ok") for row in checks),
        "checks": checks,
        "broker_guard": payload,
    }


def _compare_scenario(scenario: dict[str, Any],
                      step2: dict[str, Any] | None,
                      live: dict[str, Any] | None) -> dict[str, Any]:
    expected = scenario.get("expected") or {}
    checks: list[dict[str, Any]] = []

    def add(name: str, actual: Any, expected_value: Any, ok: bool | None = None) -> None:
        checks.append({
            "name": name,
            "ok": bool(actual == expected_value if ok is None else ok),
            "actual": actual,
            "expected": expected_value,
        })

    if scenario["kind"] in {"entry_skip", "entry_only", "round_trip"}:
        add("entry_decision_matches", live.get("entry_decision"), step2.get("entry_decision"))
        if expected.get("entry_decision") == "accepted":
            add("entry_accepted", live.get("entry_decision"), "accepted")
        if expected.get("entry_decision") == "skipped":
            add("entry_skipped", live.get("entry_decision"), "skipped")
            prefix = expected.get("reason_prefix")
            add("skip_reason_prefix_step2", True, True, str(step2.get("entry_reason") or "").startswith(prefix))
            add("skip_reason_prefix_live", True, True, str(live.get("entry_reason") or "").startswith(prefix))
        if scenario["kind"] in {"entry_only", "round_trip"} and expected.get("entry_decision") == "accepted":
            for key in ("tp_price", "sl_price", "rounding"):
                add(f"entry_bracket_{key}_matches", (live.get("entry_brackets") or {}).get(key), (step2.get("entry_brackets") or {}).get(key))
            add("entry_committed_step2", step2["milestones"].get("entry_committed"), True)
            add("entry_committed_live", live["milestones"].get("entry_committed"), True)
            add("entry_filled_step2", step2["milestones"].get("entry_filled"), True)
            add("entry_filled_live", live["milestones"].get("entry_filled"), True)
        if scenario["kind"] == "round_trip":
            add("exit_reason_matches", live.get("exit_reason"), step2.get("exit_reason"))
            add("exit_price_matches", live.get("exit_price"), step2.get("exit_price"))
            add("expected_exit_reason", live.get("exit_reason"), expected.get("exit_reason"))
            add("expected_exit_price", live.get("exit_price"), expected.get("exit_price"))
            add("closed_step2", step2["milestones"].get("closed"), True)
            add("closed_live", live["milestones"].get("closed"), True)
            add("final_positions_empty_step2", step2.get("final_positions"), [])
            add("final_positions_empty_live", live.get("final_positions"), [])
            if "closed_count" in expected:
                add("closed_count_live", live.get("closed_count"), expected.get("closed_count"))
                add("closed_count_step2", step2.get("closed_count"), expected.get("closed_count"))
    elif scenario["kind"] == "broker_reject":
        add("step2_planned_entry", step2.get("entry_decision"), expected.get("step2_entry_decision"))
        add("live_planned_entry", live.get("entry_decision"), expected.get("step2_entry_decision"))
        add("live_terminal_cancel_event", live["milestones"].get("entry_terminal_cancel"), True)
        add("live_no_open_position_after_reject", live.get("final_positions"), [])
        add("live_no_pending_after_reject", live.get("final_pending_entries"), [])

    return {
        "name": scenario["name"],
        "kind": scenario["kind"],
        "ok": all(row.get("ok") for row in checks),
        "checks": checks,
        "step2": step2,
        "live": live,
    }


def run(day: str | None = None, write: bool = True,
        config: dict[str, Any] | None = None) -> dict[str, Any]:
    day = day or datetime.now(CT).date().isoformat()
    contract = _default_contract(config)
    scenarios = _scenario_definitions(day)
    results = []
    for scenario in scenarios:
        if scenario["kind"] == "broker_guard":
            results.append(_run_broker_guard(scenario, day))
            continue
        step2 = _run_step2_execution(scenario, contract)
        live = _run_live_execution(scenario, contract)
        results.append(_compare_scenario(scenario, step2, live))
    failed = [row for row in results if not row.get("ok")]
    payload = {
        "schema_version": SCHEMA_VERSION,
        "source": "live_step2_execution_harness",
        "day": day,
        "created_at_ct": _now_ct(),
        "ok": not failed,
        "scenario_count": len(results),
        "failed_count": len(failed),
        "failed_scenarios": [row.get("name") for row in failed],
        "execution_contract_hash": step2_execution_contract.execution_contract_hash(config or (_read_json(CONFIG_PATH, {}) or {})),
        "contract": contract,
        "results": results,
        "deduction": (
            "This harness does not touch Alpaca. It deterministically compares Step 2 simulation "
            "with Live-style broker lifecycle semantics across known execution edge cases."
        ),
    }
    if write:
        path = os.path.join(OUT_DIR, f"execution_contract_harness_{day}.json")
        payload["path"] = _write_json(path, payload)
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description="Run deterministic Live/Step2 execution contract harness.")
    ap.add_argument("day", nargs="?", default="")
    ap.add_argument("--no-write", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    payload = run(day=args.day or None, write=not args.no_write)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    else:
        print(json.dumps({
            "ok": payload.get("ok"),
            "scenario_count": payload.get("scenario_count"),
            "failed_count": payload.get("failed_count"),
            "failed_scenarios": payload.get("failed_scenarios"),
            "path": payload.get("path"),
        }, indent=2, sort_keys=True))
    return 0 if payload.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
