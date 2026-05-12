"""Shared Live/Step 2 decision helpers.

The live engine should not maintain private entry/exit rules that drift from
Step 2. These helpers are intentionally small wrappers around the Step 2
execution contract and execution kernel so Live can call the same surfaces.
"""
from __future__ import annotations

from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import execution_action_engine


CT = ZoneInfo('America/Chicago')
SCHEMA_VERSION = 1


def entry_decision(state: dict[str, Any],
                   ticker: str,
                   signal_ts: int,
                   contract: dict[str, Any],
                   signal_scan_live_mode: bool = True,
                   tz: ZoneInfo = CT,
                   signal: dict[str, Any] | None = None,
                   entry_price: float | None = None,
                   sl_pct: float | None = None,
                   tp_pct: float | None = None,
                   brackets: dict[str, Any] | None = None,
                   qty: float | None = None,
                   alloc: float | None = None,
                   trade_id: str | None = None,
                   client_order_id: str | None = None,
                   broker_action: str | None = None,
                   metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = dict(signal or {})
    payload.setdefault('ticker', ticker)
    plan = execution_action_engine.plan_entry(
        state,
        payload,
        signal_ts,
        contract,
        signal_scan_live_mode,
        tz,
        entry_price=entry_price,
        sl_pct=sl_pct,
        tp_pct=tp_pct,
        brackets=brackets,
        qty=qty,
        alloc=alloc,
        trade_id=trade_id,
        client_order_id=client_order_id,
        broker_action=broker_action,
        metadata=metadata,
    )
    return execution_action_engine.legacy_entry_decision(plan, contract)


def exit_decision(side: str,
                  price: float | None,
                  sl: float,
                  tp: float,
                  in_trading_window: bool,
                  entry_price: float | None = None,
                  entry_ts: int | None = None,
                  now_ts: int | None = None,
                  conditional_time_stop_enabled: bool = False,
                  conditional_time_stop_min: float = 0.0,
                  contract: dict[str, Any] | None = None,
                  metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    position = {
        'side': side,
        'entry': entry_price,
        'entry_ts': entry_ts,
        'sl': sl,
        'tp': tp,
    }
    plan = execution_action_engine.plan_exit(
        position,
        price,
        now_ts,
        in_trading_window,
        contract=contract,
        include_conditional_time_stop=conditional_time_stop_enabled,
        conditional_time_stop_min=conditional_time_stop_min,
        include_session_end=True,
        metadata=metadata,
    )
    return execution_action_engine.legacy_exit_decision(plan)


def bracket_prices(side: str, entry_price: float, sl_pct: float, tp_pct: float) -> dict[str, Any]:
    return execution_action_engine.plan_brackets(side, float(entry_price), float(sl_pct), float(tp_pct))
