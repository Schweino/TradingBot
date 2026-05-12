"""Shared execution adapter vocabulary for Live and Step 2 replay.

Live uses a broker adapter and replay uses an in-memory adapter, but both emit
the same lifecycle events and reduce state through execution_state_reducer.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import execution_state_reducer
import execution_intent_engine


CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1


@dataclass
class ExecutionAdapterResult:
    ok: bool
    event: str
    state_hash: str | None = None
    row: dict[str, Any] | None = None
    reason: str | None = None


@dataclass
class BaseExecutionAdapter:
    day: str | None = None
    source: str = "adapter"
    runtime_state: dict[str, Any] = field(default_factory=dict)
    events: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.runtime_state.setdefault("positions", {})
        self.runtime_state.setdefault("pending_entries", {})
        self.runtime_state.setdefault("trades", [])
        self.runtime_state.setdefault(
            "execution_state",
            execution_state_reducer.from_runtime_state(self.runtime_state, day=self.day),
        )

    def _row(self, event: str, symbol: str | None = None,
             trade_id: str | None = None, data: dict[str, Any] | None = None,
             ts: int | None = None) -> dict[str, Any]:
        ts = int(ts or time.time())
        row = {
            "schema_version": SCHEMA_VERSION,
            "source": self.source,
            "event": event,
            "ts": ts,
            "ts_ct": datetime.fromtimestamp(ts, CT).isoformat(timespec="seconds"),
            "day": self.day or datetime.fromtimestamp(ts, CT).date().isoformat(),
            "symbol": symbol,
            "trade_id": trade_id,
            "data": data or {},
        }
        return row

    def emit(self, event: str, symbol: str | None = None,
             trade_id: str | None = None, data: dict[str, Any] | None = None,
             ts: int | None = None) -> ExecutionAdapterResult:
        row = self._row(event, symbol=symbol, trade_id=trade_id, data=data, ts=ts)
        self.events.append(row)
        state = execution_state_reducer.apply_audit_row(self.runtime_state, row)
        return ExecutionAdapterResult(ok=True, event=event, state_hash=state.get("state_hash"), row=row)

    def entry_block_reason(self, ticker: str, signal_ts: int, contract: dict[str, Any],
                           signal_scan_live_mode: bool = True) -> str | None:
        day = self.day or datetime.fromtimestamp(int(signal_ts), CT).date().isoformat()
        return execution_state_reducer.entry_block_reason(
            self.runtime_state,
            ticker,
            int(signal_ts),
            day,
            contract,
            signal_scan_live_mode=signal_scan_live_mode,
            tz=CT,
        )

    def reserve_entry(self, ticker: str, trade_id: str, side: str, price: float,
                      qty: float | None = None, alloc: float | None = None,
                      client_order_id: str | None = None, ts: int | None = None) -> ExecutionAdapterResult:
        return self.emit("entry_reserved", ticker, trade_id, {
            "trade_id": trade_id,
            "side": side,
            "price": price,
            "qty": qty,
            "alloc": alloc,
            "client_order_id": client_order_id,
        }, ts=ts)

    def emit_intent(self, intent: dict[str, Any], ts: int | None = None) -> ExecutionAdapterResult:
        intent_type = str(intent.get("intent_type") or "execution")
        identity = intent.get("identity") if isinstance(intent.get("identity"), dict) else {}
        return self.emit(f"{intent_type}_intent_created", identity.get("ticker"), identity.get("trade_id"), {
            "trade_id": identity.get("trade_id"),
            "execution_intent_id": intent.get("execution_intent_id"),
            "semantic_execution_intent_hash": intent.get("semantic_execution_intent_hash"),
            "execution_intent": intent,
        }, ts=ts or intent.get("created_at"))

    def emit_result(self, result: dict[str, Any], ts: int | None = None) -> ExecutionAdapterResult:
        intent_type = str(result.get("intent_type") or "execution")
        identity = result.get("identity") if isinstance(result.get("identity"), dict) else {}
        return self.emit(f"{intent_type}_execution_result", identity.get("ticker"), identity.get("trade_id"), {
            "trade_id": identity.get("trade_id"),
            "execution_intent_id": result.get("execution_intent_id"),
            "execution_result_id": result.get("execution_result_id"),
            "semantic_execution_intent_hash": result.get("semantic_execution_intent_hash"),
            "execution_result": result,
        }, ts=ts or result.get("created_at"))

    def create_simulated_entry(self, action_plan: dict[str, Any], qty: float,
                               trade_id: str | None = None, ts: int | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
        intent = execution_intent_engine.entry_intent(
            action_plan,
            venue="step2_sim",
            qty=qty,
            trade_id=trade_id,
            order_kind="simulated_entry",
            ts=ts,
        )
        result = execution_intent_engine.simulated_entry_result(intent, ts=ts)
        self.emit_intent(intent, ts=ts)
        self.emit_result(result, ts=ts)
        return intent, result

    def create_simulated_exit(self, action_plan: dict[str, Any], qty: float,
                              trade_id: str | None = None, reason: str | None = None,
                              ts: int | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
        intent = execution_intent_engine.exit_intent(
            action_plan,
            venue="step2_sim",
            qty=qty,
            trade_id=trade_id,
            order_kind="simulated_exit",
            reason=reason,
            ts=ts,
        )
        result = execution_intent_engine.simulated_exit_result(
            intent,
            ts=ts,
            reason=reason,
        )
        self.emit_intent(intent, ts=ts)
        self.emit_result(result, ts=ts)
        return intent, result

    def submit_entry(self, ticker: str, trade_id: str, side: str,
                     broker_order_id: str | None = None,
                     client_order_id: str | None = None,
                     ts: int | None = None) -> ExecutionAdapterResult:
        return self.emit("entry_submitted", ticker, trade_id, {
            "trade_id": trade_id,
            "side": side,
            "order_id": broker_order_id,
            "client_order_id": client_order_id,
        }, ts=ts)

    def commit_entry(self, ticker: str, trade_id: str, side: str, entry: float,
                     qty: float | None = None, alloc: float | None = None,
                     tp: float | None = None, sl: float | None = None,
                     client_order_id: str | None = None,
                     broker_order_id: str | None = None,
                     ts: int | None = None,
                     extra: dict[str, Any] | None = None) -> ExecutionAdapterResult:
        data = {
            "trade_id": trade_id,
            "side": side,
            "entry": entry,
            "qty": qty,
            "alloc": alloc,
            "tp": tp,
            "sl": sl,
            "client_order_id": client_order_id,
            "order_id": broker_order_id,
        }
        if extra:
            data.update(extra)
        return self.emit("entry_committed", ticker, trade_id, data, ts=ts)

    def fill_entry(self, ticker: str, trade_id: str, fill_price: float | None = None,
                   broker_order_id: str | None = None, ts: int | None = None) -> ExecutionAdapterResult:
        return self.emit("entry_filled", ticker, trade_id, {
            "trade_id": trade_id,
            "fill_price": fill_price,
            "order_id": broker_order_id,
        }, ts=ts)

    def close_position(self, ticker: str, trade_id: str, side: str, reason: str,
                       exit_price: float, pnl: float | None = None,
                       broker_order_id: str | None = None,
                       ts: int | None = None) -> ExecutionAdapterResult:
        return self.emit("position_closed", ticker, trade_id, {
            "trade_id": trade_id,
            "side": side,
            "reason": reason,
            "exit": exit_price,
            "pnl": pnl,
            "broker_exit_order_id": broker_order_id,
        }, ts=ts)

    def snapshot(self) -> dict[str, Any]:
        state = self.runtime_state.get("execution_state") or {}
        return {
            "schema_version": SCHEMA_VERSION,
            "source": self.source,
            "day": self.day,
            "event_count": len(self.events),
            "state_hash": state.get("state_hash"),
            "positions": state.get("positions") or {},
            "pending_entries": state.get("pending_entries") or {},
            "last_closed_at": state.get("last_closed_at") or {},
        }


class ReplayExecutionAdapter(BaseExecutionAdapter):
    def __init__(self, day: str | None = None, runtime_state: dict[str, Any] | None = None):
        super().__init__(day=day, source="step2_replay_adapter", runtime_state=runtime_state or {})


class LiveExecutionAdapter(BaseExecutionAdapter):
    def __init__(self, runtime_state: dict[str, Any], day: str | None = None):
        super().__init__(day=day, source="live_broker_adapter", runtime_state=runtime_state)
