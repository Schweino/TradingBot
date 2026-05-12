"""Runtime broker/order lifecycle guard.

This module keeps the live broker state tied to the local lifecycle records.
It is intentionally small and dependency-free so Live, automation, and tests
can all use the same rules.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore


CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1

ACTIVE_ORDER_STATUSES = {
    "new",
    "accepted",
    "pending_new",
    "partially_filled",
    "filled",
    "held",
    "pending_replace",
    "pending_cancel",
    "calculated",
}
TERMINAL_ORDER_STATUSES = {"canceled", "cancelled", "expired", "rejected", "replaced", "done_for_day"}
CLIENT_ORDER_ID_KEYS = {"client_order_id", "broker_client_order_id"}
BROKER_ORDER_ID_KEYS = {
    "broker_order_id",
    "alpaca_order_id",
    "order_id",
    "response_order_id",
    "parent_order_id",
}


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


def _norm_id(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() in {"none", "null", "nan"}:
        return ""
    return text


def _symbol(value: Any) -> str:
    if not isinstance(value, dict):
        return ""
    return str(value.get("symbol") or value.get("ticker") or "").upper()


def _walk(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


def extract_trade_identity(value: Any) -> dict[str, list[str]]:
    """Collect canonical trade/client/broker ids from arbitrary audit payloads."""
    trade_ids: set[str] = set()
    client_ids: set[str] = set()
    broker_ids: set[str] = set()
    symbols: set[str] = set()
    for row in _walk(value):
        sym = _symbol(row)
        if sym:
            symbols.add(sym)
        tid = _norm_id(row.get("trade_id"))
        if tid:
            trade_ids.add(tid)
        for key in CLIENT_ORDER_ID_KEYS:
            ident = _norm_id(row.get(key))
            if ident:
                client_ids.add(ident)
        for key in BROKER_ORDER_ID_KEYS:
            ident = _norm_id(row.get(key))
            if ident:
                broker_ids.add(ident)
        fill = row.get("fill")
        if isinstance(fill, dict):
            ident = _norm_id(fill.get("order_id") or fill.get("id"))
            if ident:
                broker_ids.add(ident)
    return {
        "trade_ids": sorted(trade_ids),
        "client_order_ids": sorted(client_ids),
        "broker_order_ids": sorted(broker_ids),
        "symbols": sorted(symbols),
    }


def _identity_from_record(symbol: str, phase: str, record: dict[str, Any]) -> dict[str, Any]:
    ident = extract_trade_identity(record)
    return {
        "symbol": symbol,
        "phase": phase,
        "trade_id": _norm_id(record.get("trade_id")) or (ident["trade_ids"][0] if ident["trade_ids"] else ""),
        "client_order_ids": ident["client_order_ids"],
        "broker_order_ids": ident["broker_order_ids"],
        "side": record.get("side"),
        "qty": record.get("qty"),
        "status": record.get("alpaca_status") or record.get("status"),
    }


def build_runtime_identity_map(state: dict[str, Any] | None,
                               watched: list[str] | tuple[str, ...] | None = None) -> dict[str, Any]:
    state = state if isinstance(state, dict) else {}
    watched_set = {str(s).upper() for s in (watched or []) if str(s).strip()}
    positions = state.get("positions") if isinstance(state.get("positions"), dict) else {}
    pending = state.get("pending_entries") if isinstance(state.get("pending_entries"), dict) else {}
    rows: list[dict[str, Any]] = []
    for phase, source in (("position", positions), ("pending_entry", pending)):
        for raw_symbol, record in source.items():
            symbol = str(raw_symbol).upper()
            if watched_set and symbol not in watched_set:
                continue
            if isinstance(record, dict):
                rows.append(_identity_from_record(symbol, phase, record))

    client_index: dict[str, list[str]] = defaultdict(list)
    broker_index: dict[str, list[str]] = defaultdict(list)
    trade_index: dict[str, list[str]] = defaultdict(list)
    active_symbols = set()
    for row in rows:
        symbol = str(row.get("symbol") or "").upper()
        if symbol:
            active_symbols.add(symbol)
        phase_symbol = f"{row.get('phase')}:{symbol}"
        if row.get("trade_id"):
            trade_index[str(row["trade_id"])].append(phase_symbol)
        for ident in row.get("client_order_ids") or []:
            client_index[str(ident)].append(phase_symbol)
        for ident in row.get("broker_order_ids") or []:
            broker_index[str(ident)].append(phase_symbol)

    duplicates = []
    for kind, index in (("client_order_id", client_index), ("broker_order_id", broker_index), ("trade_id", trade_index)):
        for ident, owners in index.items():
            unique = sorted(set(owners))
            if len(unique) > 1:
                duplicates.append({"kind": kind, "id": ident, "owners": unique})

    return {
        "schema_version": SCHEMA_VERSION,
        "source": "broker_lifecycle_guard",
        "active_symbols": sorted(active_symbols),
        "rows": rows,
        "client_order_ids": sorted(client_index.keys()),
        "broker_order_ids": sorted(broker_index.keys()),
        "trade_ids": sorted(trade_index.keys()),
        "client_order_id_to_owners": {k: sorted(set(v)) for k, v in sorted(client_index.items())},
        "broker_order_id_to_owners": {k: sorted(set(v)) for k, v in sorted(broker_index.items())},
        "trade_id_to_owners": {k: sorted(set(v)) for k, v in sorted(trade_index.items())},
        "duplicates": duplicates,
    }


def _broker_order_summary(order: dict[str, Any]) -> dict[str, Any]:
    legs = order.get("legs") if isinstance(order.get("legs"), list) else []
    return {
        "symbol": _symbol(order),
        "id": _norm_id(order.get("id")),
        "client_order_id": _norm_id(order.get("client_order_id")),
        "parent_order_id": _norm_id(order.get("parent_order_id")),
        "status": str(order.get("status") or "").lower(),
        "side": order.get("side"),
        "type": order.get("type"),
        "order_class": order.get("order_class"),
        "leg_count": len(legs),
        "leg_ids": [_norm_id(leg.get("id")) for leg in legs if isinstance(leg, dict) and _norm_id(leg.get("id"))],
        "leg_statuses": [str(leg.get("status") or "").lower() for leg in legs if isinstance(leg, dict)],
    }


def _broker_position_summary(position: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol": _symbol(position),
        "side": position.get("side"),
        "qty": position.get("qty"),
        "avg_entry_price": position.get("avg_entry_price"),
    }


def _issue(level: str, kind: str, detail: dict[str, Any]) -> dict[str, Any]:
    return {"level": level, "kind": kind, "detail": detail}


def fetch_broker_snapshot(trader: Any,
                          watched: list[str] | tuple[str, ...] | None = None) -> dict[str, Any]:
    watched_list = [str(s).upper() for s in (watched or []) if str(s).strip()]
    if trader is None:
        return {
            "reachable": False,
            "error": "broker_client_unavailable",
            "positions": [],
            "open_orders": [],
            "watched_positions": [],
            "watched_open_orders": [],
        }
    try:
        positions = trader.list_positions() or []
        orders = trader.list_orders(status="open", symbols=watched_list or None, nested=True) or []
        watched_set = set(watched_list)
        watched_positions = [
            p for p in positions
            if not watched_set or _symbol(p) in watched_set
        ]
        watched_orders = [
            o for o in orders
            if not watched_set or _symbol(o) in watched_set
        ]
        return {
            "reachable": True,
            "error": None,
            "positions": positions,
            "open_orders": orders,
            "watched_positions": watched_positions,
            "watched_open_orders": watched_orders,
        }
    except Exception as exc:
        return {
            "reachable": False,
            "error": str(exc),
            "positions": [],
            "open_orders": [],
            "watched_positions": [],
            "watched_open_orders": [],
        }


def _order_exactly_mapped(order: dict[str, Any], identity: dict[str, Any]) -> bool:
    client_ids = set(identity.get("client_order_ids") or [])
    broker_ids = set(identity.get("broker_order_ids") or [])
    summary = _broker_order_summary(order)
    if summary["client_order_id"] and summary["client_order_id"] in client_ids:
        return True
    for ident in [summary["id"], summary["parent_order_id"], *(summary.get("leg_ids") or [])]:
        if ident and ident in broker_ids:
            return True
    return False


def _order_symbol_mapped(order: dict[str, Any], identity: dict[str, Any]) -> bool:
    symbol = _symbol(order)
    return bool(symbol and symbol in set(identity.get("active_symbols") or []))


def _recommendations(issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
    kinds = {row.get("kind") for row in issues}
    out = []
    if "broker_open_order_unmapped" in kinds or "broker_position_untracked" in kinds:
        out.append({
            "action": "block_trading_until_reconciled",
            "reason": "Broker exposure exists without a matching local lifecycle owner.",
            "steps": [
                "inspect watched Alpaca orders/positions",
                "recover the lifecycle record if it is a valid bracket",
                "cancel/flatten the orphan only after broker state is confirmed",
            ],
        })
    if "broker_order_symbol_matched_identity_unknown" in kinds:
        out.append({
            "action": "inspect_identity_mapping",
            "reason": "Local symbol ownership exists, but the broker order id/client_order_id was not in state.",
            "steps": [
                "compare the parent Alpaca order id to the local alpaca_order_id/client_order_id",
                "persist the missing id if the order belongs to the active trade",
            ],
        })
    if "duplicate_runtime_identity" in kinds:
        out.append({
            "action": "stop_and_deduplicate_lifecycle_ids",
            "reason": "One trade/order identifier maps to more than one active local lifecycle owner.",
        })
    if "broker_unreachable" in kinds:
        out.append({
            "action": "retry_broker_safety_snapshot",
            "reason": "The guard could not prove broker state; do not clear exposure blocks from local state alone.",
        })
    if "pending_entry_without_broker_order" in kinds:
        out.append({
            "action": "refresh_pending_entry_status",
            "reason": "A local pending entry has no matching open broker order; it may be stale or already terminal.",
        })
    return out


def evaluate_runtime_broker_state(state: dict[str, Any] | None,
                                  broker_positions: list[dict[str, Any]] | None = None,
                                  broker_orders: list[dict[str, Any]] | None = None,
                                  watched: list[str] | tuple[str, ...] | None = None,
                                  day: str | None = None,
                                  broker_reachable: bool = True,
                                  broker_error: str | None = None,
                                  label: str = "runtime") -> dict[str, Any]:
    watched_list = [str(s).upper() for s in (watched or []) if str(s).strip()]
    identity = build_runtime_identity_map(state, watched_list)
    active_symbols = set(identity.get("active_symbols") or [])
    positions = broker_positions or []
    orders = broker_orders or []
    issues: list[dict[str, Any]] = []

    for dup in identity.get("duplicates") or []:
        issues.append(_issue("critical", "duplicate_runtime_identity", dup))

    if not broker_reachable:
        issues.append(_issue("warning", "broker_unreachable", {"error": broker_error}))

    broker_position_symbols = set()
    for pos in positions:
        sym = _symbol(pos)
        if not sym or (watched_list and sym not in watched_list):
            continue
        broker_position_symbols.add(sym)
        if sym not in active_symbols:
            issues.append(_issue("critical", "broker_position_untracked", _broker_position_summary(pos)))

    state_positions = (state or {}).get("positions") if isinstance((state or {}).get("positions"), dict) else {}
    for sym in sorted(set(str(s).upper() for s in state_positions) - broker_position_symbols):
        if watched_list and sym not in watched_list:
            continue
        issues.append(_issue("warning", "internal_position_missing_broker_position", {"symbol": sym}))

    order_summaries = []
    exact_order_symbols: set[str] = set()
    symbol_order_counts = Counter()
    for order in orders:
        summary = _broker_order_summary(order)
        sym = summary.get("symbol")
        if not sym or (watched_list and sym not in watched_list):
            continue
        status = str(summary.get("status") or "").lower()
        if status in TERMINAL_ORDER_STATUSES:
            continue
        symbol_order_counts[sym] += 1
        exact = _order_exactly_mapped(order, identity)
        symbol_mapped = _order_symbol_mapped(order, identity)
        summary["exact_identity_match"] = exact
        summary["symbol_lifecycle_match"] = symbol_mapped
        order_summaries.append(summary)
        if exact:
            exact_order_symbols.add(sym)
        elif symbol_mapped:
            issues.append(_issue("warning", "broker_order_symbol_matched_identity_unknown", summary))
        else:
            issues.append(_issue("critical", "broker_open_order_unmapped", summary))

    for sym, count in sorted(symbol_order_counts.items()):
        if count > 1 and sym not in broker_position_symbols and sym not in active_symbols:
            issues.append(_issue("critical", "duplicate_orphan_open_orders", {"symbol": sym, "open_order_count": count}))
        elif count > 2 and sym in active_symbols:
            issues.append(_issue("warning", "duplicate_close_risk", {"symbol": sym, "open_order_count": count}))

    state_pending = (state or {}).get("pending_entries") if isinstance((state or {}).get("pending_entries"), dict) else {}
    for sym, row in state_pending.items():
        symbol = str(sym).upper()
        if watched_list and symbol not in watched_list:
            continue
        if symbol in exact_order_symbols:
            continue
        pending_ident = extract_trade_identity(row)
        has_any_id = bool(pending_ident.get("client_order_ids") or pending_ident.get("broker_order_ids"))
        matching_order = any(_symbol(order) == symbol for order in orders)
        if has_any_id and not matching_order:
            issues.append(_issue("warning", "pending_entry_without_broker_order", {
                "symbol": symbol,
                "client_order_ids": pending_ident.get("client_order_ids") or [],
                "broker_order_ids": pending_ident.get("broker_order_ids") or [],
            }))

    critical = [row for row in issues if row.get("level") == "critical"]
    warnings = [row for row in issues if row.get("level") == "warning"]
    issue_counts = Counter(str(row.get("kind")) for row in issues)
    scorecard = {
        "unmapped_open_order_count": issue_counts.get("broker_open_order_unmapped", 0),
        "untracked_broker_position_count": issue_counts.get("broker_position_untracked", 0),
        "order_status_drift_count": issue_counts.get("broker_order_symbol_matched_identity_unknown", 0),
        "orphan_bracket_risk_count": issue_counts.get("broker_open_order_unmapped", 0) + issue_counts.get("duplicate_orphan_open_orders", 0),
        "duplicate_close_risk_count": issue_counts.get("duplicate_close_risk", 0),
        "duplicate_identity_count": issue_counts.get("duplicate_runtime_identity", 0),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "broker_lifecycle_guard",
        "day": day,
        "label": label,
        "created_at_ct": _now_ct(),
        "ok": not critical,
        "verdict": "BROKER_LIFECYCLE_FAIL" if critical else ("BROKER_LIFECYCLE_WARN" if warnings else "BROKER_LIFECYCLE_OK"),
        "critical_count": len(critical),
        "warning_count": len(warnings),
        "issue_kind_counts": dict(sorted(issue_counts.items())),
        "issues": issues[:100],
        "scorecard": scorecard,
        "runtime_identity_map": identity,
        "broker": {
            "reachable": bool(broker_reachable),
            "error": broker_error,
            "watched": watched_list,
            "position_count": len(positions),
            "open_order_count": len(orders),
            "positions": [_broker_position_summary(p) for p in positions],
            "open_orders": order_summaries[:100],
        },
        "repair_recommendations": _recommendations(issues),
        "deduction": (
            "Live may trade only when every watched broker position/open order has a local "
            "lifecycle owner by symbol and, preferably, by client_order_id or broker_order_id."
        ),
    }

