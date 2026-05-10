"""Backfill live/postmortem artifacts from the legacy trade lifecycle audit.

This is intentionally explicit about provenance: rows written by this utility
are reconstructed from ``audit/trade_lifecycle_YYYY-MM-DD.jsonl`` and the local
trade corpus, not original first-class runtime emissions.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from collections import Counter, defaultdict, deque
from datetime import datetime
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import active_engine_baseline
import execution_kernel
import execution_lifecycle
import live_decision_ledger
import step2_execution_contract
import step2_parity_contract
import unified_decision_ledger


HERE = os.path.dirname(os.path.abspath(__file__))
POSTMORTEM_DIR = os.path.join(HERE, "postmortem")
CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1
SOURCE = "legacy_audit_backfill"


def _stable_hash(payload: Any, length: int = 24) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:length]


def _file_sha256(path: str) -> str | None:
    if not os.path.exists(path):
        return None
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except Exception:
        return default


def _read_jsonl(path: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not os.path.exists(path):
        return rows
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows


def _write_json(path: str, payload: dict[str, Any]) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.{os.getpid()}.{int(time.time() * 1000)}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
        f.write("\n")
    os.replace(tmp, path)
    return os.path.abspath(path)


def _write_jsonl(path: str, rows: list[dict[str, Any]]) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.{os.getpid()}.{int(time.time() * 1000)}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, sort_keys=True, separators=(",", ":"), default=str) + "\n")
    os.replace(tmp, path)
    return os.path.abspath(path)


def _ct(ts: Any) -> str | None:
    try:
        return datetime.fromtimestamp(float(ts), CT).isoformat(timespec="seconds")
    except Exception:
        return None


def _day(ts: Any) -> str:
    try:
        return datetime.fromtimestamp(float(ts), CT).date().isoformat()
    except Exception:
        return datetime.now(CT).date().isoformat()


def _num(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _identity(config: dict[str, Any]) -> dict[str, Any]:
    parity = step2_parity_contract.contract(config)
    profile = step2_parity_contract.active_profile_snapshot(config, include_weights=True)
    try:
        active_payload = active_engine_baseline.active_profile_payload()
    except Exception as exc:
        active_payload = {"error": repr(exc)}
    return {
        "strategy_config_hash": _file_sha256(os.path.join(HERE, "trading_config.json")),
        "execution_kernel_hash": execution_kernel.contract_from_config(config).get("execution_kernel_hash"),
        "step2_parity_contract_hash": step2_parity_contract.contract_hash(parity),
        "step2_execution_contract_hash": step2_execution_contract.execution_contract_hash(config),
        "active_profile_name": profile.get("name"),
        "active_profile_hash": profile.get("hash"),
        "active_profile_bias": profile.get("bias"),
        "active_profile_weights": profile.get("weights") or {},
        "active_profile_payload": active_payload,
    }


def _feature_payload(decision: dict[str, Any]) -> dict[str, Any]:
    return {
        "score": decision.get("score"),
        "conviction": decision.get("conviction"),
        "components": decision.get("components") or {},
        "indicators": decision.get("indicators") or {},
        "btc_indicators": decision.get("btc_indicators") or {},
        "btc_context": decision.get("btc_context") or {},
        "signal_quality": decision.get("signal_quality") or {},
        "relative_strength": decision.get("relative_strength") or {},
        "miner_basket": decision.get("miner_basket") or {},
        "lead_lag": decision.get("lead_lag") or {},
        "forensics": decision.get("forensics") or {},
    }


def _opportunity_id(day: str, ticker: Any, ts: Any, side: Any, setup_type: Any) -> str:
    return f"{day}:{str(ticker or '').upper()}:{int(float(ts or 0))}:{str(side or '').upper()}:{setup_type or 'unknown'}:0"


def _trade_outcome(trade: dict[str, Any] | None) -> dict[str, Any] | None:
    if not trade:
        return None
    opened = _num(trade.get("opened_at"))
    closed = _num(trade.get("closed_at"))
    held = int(round(float(closed - opened))) if opened is not None and closed is not None else None
    return {
        "pnl": trade.get("pnl"),
        "held_sec": held,
        "reason": trade.get("reason"),
        "entry": trade.get("entry"),
        "exit": trade.get("exit"),
        "tp": trade.get("tp"),
        "sl": trade.get("sl"),
        "exit_ct": _ct(trade.get("closed_at")),
    }


def _semantic_action(row: dict[str, Any], trade: dict[str, Any] | None) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    extra = row.get("extra") if isinstance(row.get("extra"), dict) else {}
    action_payload = {
        "ticker": row.get("ticker"),
        "side": row.get("side"),
        "decision": row.get("decision"),
        "price": row.get("price"),
        "qty": extra.get("qty"),
        "alloc": extra.get("alloc"),
        "sl": extra.get("sl"),
        "tp": extra.get("tp"),
        "trade_id": row.get("trade_id"),
        "client_order_id": row.get("client_order_id"),
        "broker_order_id": row.get("broker_order_id"),
    }
    action_hash = _stable_hash(action_payload, 32)
    action_plan = {
        "schema_version": SCHEMA_VERSION,
        "source": SOURCE,
        "created_at": row.get("created_at"),
        "created_at_ct": row.get("created_at_ct"),
        "semantic_action_hash": action_hash,
        "action": "submit_entry_order" if row.get("decision") == "entered" else "no_order",
        **action_payload,
        "backfill_note": "Reconstructed from legacy audit decision/order fields.",
    }
    intent_payload = {
        **action_payload,
        "bracket_policy": extra.get("bracket_policy") or {},
        "strategy_config_hash": row.get("strategy_config_hash"),
    }
    intent_hash = _stable_hash(intent_payload, 32)
    execution_intent = {
        "schema_version": SCHEMA_VERSION,
        "source": SOURCE,
        "execution_intent_id": f"backfill-entry-intent-{intent_hash}",
        "semantic_execution_intent_hash": intent_hash,
        "created_at": row.get("created_at"),
        "created_at_ct": row.get("created_at_ct"),
        **intent_payload,
        "backfill_note": "Intent was reconstructed from the committed entry audit trail.",
    }
    result_payload = {
        "trade_id": row.get("trade_id"),
        "client_order_id": row.get("client_order_id"),
        "broker_order_id": row.get("broker_order_id"),
        "status": "committed" if row.get("decision") == "entered" else "not_submitted",
        "entry": trade.get("entry") if trade else row.get("price"),
        "qty": trade.get("qty") if trade else extra.get("qty"),
    }
    result_hash = _stable_hash(result_payload, 32)
    execution_result = {
        "schema_version": SCHEMA_VERSION,
        "source": SOURCE,
        "execution_result_id": f"backfill-entry-result-{result_hash}",
        "created_at": row.get("created_at"),
        "created_at_ct": row.get("created_at_ct"),
        **result_payload,
        "backfill_note": "Result was reconstructed from entry_committed/trade corpus fields.",
    }
    return action_plan, execution_intent, execution_result


def _entered_row(audit_row: dict[str, Any], identity: dict[str, Any],
                 trades_by_id: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    data = audit_row.get("data") if isinstance(audit_row.get("data"), dict) else {}
    decision = data.get("decision_audit") if isinstance(data.get("decision_audit"), dict) else {}
    if not decision:
        return None
    ts = int(decision.get("created_at") or audit_row.get("ts") or 0)
    day = _day(ts)
    ticker = str(decision.get("ticker") or audit_row.get("symbol") or "").upper()
    side = str(decision.get("side") or data.get("side") or "").upper()
    setup_type = decision.get("setup_type") or "unknown"
    trade_id = decision.get("trade_id") or data.get("trade_id")
    trade = trades_by_id.get(str(trade_id or ""))
    features = _feature_payload(decision)
    feature_hash = decision.get("live_signal_parity_feature_hash") or _stable_hash(features, 64)
    latency = decision.get("latency_attribution") if isinstance(decision.get("latency_attribution"), dict) else {}
    fills = decision.get("fill_attribution") if isinstance(decision.get("fill_attribution"), list) else []
    extra = {
        "qty": data.get("qty") if data.get("qty") is not None else (trade or {}).get("qty"),
        "alloc": data.get("alloc") if data.get("alloc") is not None else (trade or {}).get("alloc"),
        "tp": data.get("tp") if data.get("tp") is not None else (trade or {}).get("tp"),
        "sl": data.get("sl") if data.get("sl") is not None else (trade or {}).get("sl"),
        "bracket_policy": data.get("bracket_policy") or (trade or {}).get("bracket_policy") or {},
        "fill_attribution": fills,
        "latency_attribution": latency,
        "entry_quality_tier": decision.get("entry_quality_tier") or decision.get("setup_grade_at_entry"),
        "estimated_round_trip_costs_at_entry": decision.get("estimated_round_trip_costs_at_entry"),
        "gates": decision.get("gates") or {},
        "backfill_metadata": {
            "source": SOURCE,
            "audit_event": audit_row.get("event"),
            "audit_ts": audit_row.get("ts"),
            "contract_fields": "current schema-completion values from trading_config.json",
            "original_strategy_config_hash_preserved": bool(decision.get("strategy_config_hash")),
        },
    }
    row = {
        "schema_version": SCHEMA_VERSION,
        "source": SOURCE,
        "created_at": ts,
        "created_at_ct": decision.get("created_at_ct") or audit_row.get("ts_ct") or _ct(ts),
        "parity_key": decision.get("live_signal_parity_key") or _stable_hash({
            "ts": ts,
            "ticker": ticker,
            "side": side,
            "setup_type": setup_type,
            "score": decision.get("score"),
        }),
        "opportunity_id": _opportunity_id(day, ticker, ts, side, setup_type),
        "ticker": ticker,
        "side": side,
        "setup_type": setup_type,
        "decision": "entered",
        "reason": "entered",
        "price": round(float(decision.get("price") or data.get("entry") or 0.0), 4),
        "score": decision.get("score"),
        "conviction": decision.get("conviction"),
        "reasons": decision.get("reasons") or [],
        "trade_id": trade_id,
        "client_order_id": decision.get("client_order_id") or data.get("client_order_id"),
        "broker_order_id": data.get("order_id") or data.get("broker_order_id"),
        "execution_mode": SOURCE,
        "strategy_config_hash": decision.get("strategy_config_hash") or identity.get("strategy_config_hash"),
        "execution_kernel_hash": identity.get("execution_kernel_hash"),
        "step2_parity_contract_hash": identity.get("step2_parity_contract_hash"),
        "step2_execution_contract_hash": identity.get("step2_execution_contract_hash"),
        "active_profile_name": identity.get("active_profile_name"),
        "active_profile_hash": identity.get("active_profile_hash"),
        "active_profile_bias": identity.get("active_profile_bias"),
        "active_profile_weights": identity.get("active_profile_weights") or {},
        "feature_snapshot_hash": feature_hash,
        "feature_snapshot": features,
        "market_freshness": {},
        "latency_chain": latency.get("chain_ms") if isinstance(latency, dict) else {},
        "operational_state": {},
        "outcome_summary": _trade_outcome(trade),
        "join_hint": {
            "ticker": ticker,
            "side": side,
            "timestamp_second": ts,
            "setup_type": setup_type,
            "score": decision.get("score"),
        },
        "extra": extra,
        "backfill_metadata": extra["backfill_metadata"],
    }
    action_plan, execution_intent, execution_result = _semantic_action(row, trade)
    row["action_plan"] = action_plan
    row["execution_intent"] = execution_intent
    row["execution_result"] = execution_result
    row["extra"]["action_plan"] = action_plan
    row["extra"]["execution_intent"] = execution_intent
    row["extra"]["execution_result"] = execution_result
    return row


def _skipped_row(audit_row: dict[str, Any], identity: dict[str, Any]) -> dict[str, Any] | None:
    data = audit_row.get("data") if isinstance(audit_row.get("data"), dict) else {}
    ts = int(audit_row.get("ts") or 0)
    ticker = str(audit_row.get("symbol") or data.get("ticker") or "").upper()
    side = str(data.get("side") or "").upper()
    price = _num(data.get("price"))
    if not ts or not ticker or side not in ("LONG", "SHORT") or price is None:
        return None
    setup_type = data.get("setup_type") or "unknown"
    features = {
        "score": data.get("score"),
        "conviction": data.get("conviction"),
        "components": {},
        "indicators": {},
        "btc_indicators": {},
        "btc_context": {},
        "signal_quality": {},
        "relative_strength": {},
        "miner_basket": {},
        "lead_lag": {},
        "forensics": {
            "source": SOURCE,
            "audit_event": "signal_skipped",
            "raw_data": data,
        },
    }
    parity_key = _stable_hash({
        "event": "signal_skipped",
        "ts": ts,
        "ticker": ticker,
        "side": side,
        "reason": data.get("reason"),
        "price": price,
        "score": data.get("score"),
    })
    return {
        "schema_version": SCHEMA_VERSION,
        "source": SOURCE,
        "created_at": ts,
        "created_at_ct": audit_row.get("ts_ct") or _ct(ts),
        "parity_key": parity_key,
        "opportunity_id": _opportunity_id(_day(ts), ticker, ts, side, setup_type),
        "ticker": ticker,
        "side": side,
        "setup_type": setup_type,
        "decision": "skipped",
        "reason": data.get("reason") or "skipped",
        "price": round(float(price), 4),
        "score": data.get("score"),
        "conviction": data.get("conviction"),
        "reasons": [data.get("reason")] if data.get("reason") else [],
        "trade_id": None,
        "client_order_id": None,
        "broker_order_id": None,
        "execution_mode": SOURCE,
        "strategy_config_hash": identity.get("strategy_config_hash"),
        "execution_kernel_hash": identity.get("execution_kernel_hash"),
        "step2_parity_contract_hash": identity.get("step2_parity_contract_hash"),
        "step2_execution_contract_hash": identity.get("step2_execution_contract_hash"),
        "active_profile_name": identity.get("active_profile_name"),
        "active_profile_hash": identity.get("active_profile_hash"),
        "active_profile_bias": identity.get("active_profile_bias"),
        "active_profile_weights": identity.get("active_profile_weights") or {},
        "feature_snapshot_hash": _stable_hash(features, 64),
        "feature_snapshot": features,
        "market_freshness": {},
        "latency_chain": {},
        "operational_state": {},
        "join_hint": {
            "ticker": ticker,
            "side": side,
            "timestamp_second": ts,
            "setup_type": setup_type,
            "score": data.get("score"),
        },
        "extra": {
            "backfill_metadata": {
                "source": SOURCE,
                "audit_event": "signal_skipped",
                "contract_fields": "current schema-completion values from trading_config.json",
            },
        },
        "backfill_metadata": {
            "source": SOURCE,
            "audit_event": "signal_skipped",
        },
    }


def _dedupe(rows: list[dict[str, Any]], fields: tuple[str, ...]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for row in rows:
        key = _stable_hash({field: row.get(field) for field in fields}, 64)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _close_match_trade_id(audit: dict[str, Any], trade_rows: list[dict[str, Any]],
                          used_closed_trade_ids: set[str]) -> str | None:
    data = audit.get("data") if isinstance(audit.get("data"), dict) else {}
    symbol = str(audit.get("symbol") or "").upper()
    side = str(data.get("side") or "").upper()
    reason = str(data.get("reason") or "")
    ts = _num(audit.get("ts"))
    exit_px = _num(data.get("exit"))
    pnl = _num(data.get("pnl"))
    candidates: list[tuple[float, str]] = []
    for trade in trade_rows:
        trade_id = str(trade.get("trade_id") or "")
        if not trade_id or trade_id in used_closed_trade_ids:
            continue
        if str(trade.get("ticker") or "").upper() != symbol:
            continue
        if side and str(trade.get("side") or "").upper() != side:
            continue
        if reason and str(trade.get("reason") or "") != reason:
            continue
        trade_exit = _num(trade.get("exit"))
        trade_pnl = _num(trade.get("pnl"))
        closed_at = _num(trade.get("closed_at"))
        if exit_px is not None and trade_exit is not None and abs(exit_px - trade_exit) > 0.01:
            continue
        if pnl is not None and trade_pnl is not None and abs(pnl - trade_pnl) > 0.05:
            continue
        ts_gap = abs(float(ts or 0.0) - float(closed_at or 0.0)) if ts is not None and closed_at is not None else 999999.0
        if ts_gap > 60:
            continue
        candidates.append((ts_gap, trade_id))
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[1]))
    return candidates[0][1]


def _collect_attribution(live_rows: list[dict[str, Any]], trade_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    fill_rows: list[dict[str, Any]] = []
    latency_rows: list[dict[str, Any]] = []
    for row in live_rows:
        extra = row.get("extra") if isinstance(row.get("extra"), dict) else {}
        for fill in extra.get("fill_attribution") or []:
            if isinstance(fill, dict):
                out = dict(fill)
                out.setdefault("source", SOURCE)
                out.setdefault("backfill_source", SOURCE)
                fill_rows.append(out)
        latency = extra.get("latency_attribution")
        if isinstance(latency, dict):
            out = dict(latency)
            out.setdefault("source", SOURCE)
            out.setdefault("backfill_source", SOURCE)
            latency_rows.append(out)
    for trade in trade_rows:
        forensics = trade.get("forensics") if isinstance(trade.get("forensics"), dict) else {}
        for fill in forensics.get("fill_attribution") or []:
            if isinstance(fill, dict):
                out = dict(fill)
                out.setdefault("source", SOURCE)
                out.setdefault("backfill_source", SOURCE)
                fill_rows.append(out)
        for key in ("latency_attribution",):
            latency = forensics.get(key)
            if isinstance(latency, dict):
                out = dict(latency)
                out.setdefault("source", SOURCE)
                out.setdefault("backfill_source", SOURCE)
                latency_rows.append(out)
        exit_latency = trade.get("exit_latency_attribution")
        if isinstance(exit_latency, dict):
            out = dict(exit_latency)
            out.setdefault("source", SOURCE)
            out.setdefault("backfill_source", SOURCE)
            latency_rows.append(out)
    fill_rows = _dedupe(fill_rows, ("trade_id", "stage", "created_at", "broker_order_id", "price_ref", "fill_price"))
    latency_rows = _dedupe(latency_rows, ("trade_id", "status", "created_at", "broker_order_id"))
    fill_rows.sort(key=lambda r: (int(r.get("created_at") or 0), str(r.get("trade_id") or ""), str(r.get("stage") or "")))
    latency_rows.sort(key=lambda r: (int(r.get("created_at") or 0), str(r.get("trade_id") or ""), str(r.get("status") or "")))
    return fill_rows, latency_rows


def _synthetic_lifecycle_event(base: dict[str, Any], event: str, stage: str,
                               trade_id: str | None, data: dict[str, Any]) -> dict[str, Any]:
    ts = int(base.get("ts") or data.get("created_at") or time.time())
    symbol = base.get("symbol") or data.get("ticker")
    payload = {
        "schema_version": SCHEMA_VERSION,
        "event_id": _stable_hash({
            "source": SOURCE,
            "event": event,
            "stage": stage,
            "trade_id": trade_id,
            "ts": ts,
            "data": data,
        }),
        "source": SOURCE,
        "ts": ts,
        "ts_ct": base.get("ts_ct") or _ct(ts),
        "day": _day(ts),
        "stage": stage,
        "event": event,
        "symbol": symbol,
        "trade_id": trade_id,
        "client_order_id": data.get("client_order_id"),
        "broker_order_id": data.get("broker_order_id") or data.get("order_id"),
        "data": {
            **data,
            "backfill_metadata": {
                "source": SOURCE,
                "reconstructed_event": event,
                "source_audit_event": base.get("event"),
            },
        },
    }
    return payload


def _lifecycle_rows(day: str, audit_rows: list[dict[str, Any]], live_rows: list[dict[str, Any]],
                    trade_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    live_by_trade = {str(row.get("trade_id")): row for row in live_rows if row.get("trade_id")}
    order_to_trade: dict[str, str] = {}
    for row in live_rows:
        trade_id = str(row.get("trade_id") or "")
        if not trade_id:
            continue
        for key in (row.get("broker_order_id"), row.get("client_order_id")):
            if key:
                order_to_trade[str(key)] = trade_id

    rows: list[dict[str, Any]] = []
    open_by_symbol: dict[str, deque[str]] = defaultdict(deque)
    used_closed_trade_ids: set[str] = set()
    for audit in sorted(audit_rows, key=lambda r: int(r.get("ts") or 0)):
        normalized = execution_lifecycle._normalize_audit_row(audit)  # type: ignore[attr-defined]
        event = str(audit.get("event") or "")
        data = audit.get("data") if isinstance(audit.get("data"), dict) else {}
        symbol = str(audit.get("symbol") or "").upper()
        trade_id = None
        decision = data.get("decision_audit") if isinstance(data.get("decision_audit"), dict) else {}
        if decision.get("trade_id"):
            trade_id = str(decision.get("trade_id"))
        elif data.get("trade_id"):
            trade_id = str(data.get("trade_id"))
        else:
            for key in (data.get("order_id"), data.get("broker_order_id"), data.get("client_order_id")):
                if key and str(key) in order_to_trade:
                    trade_id = order_to_trade[str(key)]
                    break
        if event == "position_closed":
            matched_trade_id = _close_match_trade_id(audit, trade_rows, used_closed_trade_ids)
            if matched_trade_id:
                trade_id = matched_trade_id
                used_closed_trade_ids.add(matched_trade_id)
                if symbol and matched_trade_id in open_by_symbol.get(symbol, deque()):
                    try:
                        open_by_symbol[symbol].remove(matched_trade_id)
                    except ValueError:
                        pass
            elif not trade_id and open_by_symbol.get(symbol):
                trade_id = open_by_symbol[symbol].popleft()
        if normalized:
            if trade_id:
                normalized["trade_id"] = trade_id
            rows.append(normalized)
        if event == "entry_committed" and trade_id:
            live = live_by_trade.get(trade_id) or {}
            if symbol:
                open_by_symbol[symbol].append(trade_id)
            rows.append(_synthetic_lifecycle_event(
                audit,
                "entry_intent_created",
                "intent_created",
                trade_id,
                live.get("execution_intent") or {"trade_id": trade_id},
            ))
            rows.append(_synthetic_lifecycle_event(
                audit,
                "entry_execution_result",
                "execution_result",
                trade_id,
                live.get("execution_result") or {"trade_id": trade_id},
            ))
        elif event == "position_closed" and trade_id:
            rows.append(_synthetic_lifecycle_event(
                audit,
                "exit_intent_created",
                "intent_created",
                trade_id,
                {
                    "trade_id": trade_id,
                    "ticker": symbol,
                    "side": data.get("side"),
                    "reason": data.get("reason"),
                    "exit": data.get("exit"),
                    "source": SOURCE,
                },
            ))
            rows.append(_synthetic_lifecycle_event(
                audit,
                "exit_execution_result",
                "execution_result",
                trade_id,
                {
                    "trade_id": trade_id,
                    "ticker": symbol,
                    "side": data.get("side"),
                    "reason": data.get("reason"),
                    "exit": data.get("exit"),
                    "broker_exit_order_id": data.get("broker_exit_order_id"),
                    "source": SOURCE,
                },
            ))
    rows = _dedupe(rows, ("event_id",))
    rows = [row for row in rows if row.get("day") == day]
    rows.sort(key=lambda r: (int(r.get("ts") or 0), str(r.get("event_id") or "")))
    return rows


def backfill(day: str) -> dict[str, Any]:
    config = _read_json(os.path.join(HERE, "trading_config.json"), {}) or {}
    identity = _identity(config)
    audit_path = os.path.join(HERE, "audit", f"trade_lifecycle_{day}.jsonl")
    trade_path = os.path.join(POSTMORTEM_DIR, "trades", f"trades_{day}.jsonl")
    audit_rows = _read_jsonl(audit_path)
    trade_rows = _read_jsonl(trade_path)
    trades_by_id = {str(row.get("trade_id")): row for row in trade_rows if row.get("trade_id")}

    entered_rows = [
        row for row in (_entered_row(audit, identity, trades_by_id) for audit in audit_rows
                        if audit.get("event") == "entry_committed")
        if row is not None
    ]
    skipped_rows = [
        row for row in (_skipped_row(audit, identity) for audit in audit_rows
                        if audit.get("event") == "signal_skipped")
        if row is not None
    ]
    live_rows = sorted(entered_rows + skipped_rows, key=lambda r: (int(r.get("created_at") or 0), str(r.get("ticker") or "")))

    fill_rows, latency_rows = _collect_attribution(entered_rows, trade_rows)
    decision_audits = [
        (audit.get("data") or {}).get("decision_audit")
        for audit in audit_rows
        if audit.get("event") == "entry_committed" and isinstance((audit.get("data") or {}).get("decision_audit"), dict)
    ]
    lifecycle_rows = _lifecycle_rows(day, audit_rows, entered_rows, trade_rows)

    live_signal_path = _write_jsonl(
        os.path.join(POSTMORTEM_DIR, "live_signal_parity", f"live_signal_parity_{day}.jsonl"),
        live_rows,
    )
    skipped_path = _write_jsonl(
        os.path.join(POSTMORTEM_DIR, "skipped_signals", f"skipped_signals_{day}.jsonl"),
        skipped_rows,
    )
    fill_path = _write_jsonl(
        os.path.join(POSTMORTEM_DIR, "fill_attribution", f"fill_attribution_{day}.jsonl"),
        fill_rows,
    )
    latency_path = _write_jsonl(
        os.path.join(POSTMORTEM_DIR, "latency_attribution", f"latency_attribution_{day}.jsonl"),
        latency_rows,
    )
    decision_audit_path = _write_jsonl(
        os.path.join(POSTMORTEM_DIR, "decision_audits", f"decision_audits_{day}.jsonl"),
        [row for row in decision_audits if isinstance(row, dict)],
    )
    lifecycle_path = _write_jsonl(
        os.path.join(POSTMORTEM_DIR, "execution_lifecycle", day, f"execution_lifecycle_{day}.jsonl"),
        lifecycle_rows,
    )

    unified_summary = unified_decision_ledger.write_live_signal_parity(day, live_rows)
    live_decision_rows = [
        live_decision_ledger.from_live_signal_parity(row)
        for row in live_rows
    ] + [
        live_decision_ledger.from_closed_trade(row)
        for row in trade_rows
    ]
    live_decision_source_path = _write_jsonl(
        os.path.join(POSTMORTEM_DIR, "unified_decision_ledger", day, f"live_decision_source_{day}.jsonl"),
        live_decision_rows,
    )
    lifecycle_summary = execution_lifecycle.write_snapshot(day)

    summary = {
        "schema_version": SCHEMA_VERSION,
        "source": SOURCE,
        "day": day,
        "created_at_ct": datetime.now(CT).isoformat(timespec="seconds"),
        "audit_path": os.path.abspath(audit_path),
        "trade_path": os.path.abspath(trade_path),
        "counts": {
            "audit_rows": len(audit_rows),
            "trade_rows": len(trade_rows),
            "live_signal_rows": len(live_rows),
            "entered_rows": len(entered_rows),
            "skipped_rows": len(skipped_rows),
            "fill_attribution_rows": len(fill_rows),
            "latency_attribution_rows": len(latency_rows),
            "decision_audit_rows": len(decision_audits),
            "execution_lifecycle_rows": len(lifecycle_rows),
            "live_decision_source_rows": len(live_decision_rows),
        },
        "decisions": dict(Counter(str(row.get("decision") or "unknown") for row in live_rows)),
        "paths": {
            "live_signal_parity": live_signal_path,
            "skipped_signals": skipped_path,
            "fill_attribution": fill_path,
            "latency_attribution": latency_path,
            "decision_audits": decision_audit_path,
            "execution_lifecycle": lifecycle_path,
            "execution_lifecycle_summary": lifecycle_summary.get("summary_path"),
            "unified_live_signal_parity": unified_summary.get("path"),
            "unified_live_signal_parity_summary": unified_summary.get("summary_path"),
            "live_decision_source": live_decision_source_path,
        },
        "identity": {
            key: value for key, value in identity.items()
            if key != "active_profile_weights"
        },
        "deduction": (
            "Backfilled from legacy trade_lifecycle audit and local trade corpus. "
            "Original strategy_config_hash values from decision_audit are preserved where present; "
            "contract/profile/action-intent fields are schema-completion reconstructions."
        ),
    }
    summary_path = _write_json(
        os.path.join(POSTMORTEM_DIR, "live_audit_backfill", f"live_audit_backfill_{day}.json"),
        summary,
    )
    summary["summary_path"] = summary_path
    return summary


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill live parity/postmortem artifacts from legacy audit data.")
    ap.add_argument("day")
    args = ap.parse_args()
    payload = backfill(args.day)
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
