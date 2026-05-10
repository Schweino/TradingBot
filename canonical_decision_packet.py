"""Canonical decision packets shared by Live and Step 2.

The packet is the narrow contract for parity work: one signal/opportunity,
one normalized view of the data, score, state, brackets, and outcome. Live can
append packets in real time; Step 2 can write the same shape after replay.
"""
from __future__ import annotations

import argparse
import json
import os
import time
import hashlib
from collections import Counter
from datetime import datetime
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore


HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, "postmortem", "canonical_decision_packets")
CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1


def stable_json_hash(payload: Any) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _num(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _int(value: Any) -> int | None:
    try:
        if value in (None, ""):
            return None
        return int(float(value))
    except Exception:
        return None


def _day_from_ts(ts: Any) -> str:
    try:
        return datetime.fromtimestamp(float(ts or time.time()), CT).date().isoformat()
    except Exception:
        return datetime.now(CT).date().isoformat()


def _iso_ct(ts: Any) -> str | None:
    try:
        return datetime.fromtimestamp(float(ts), CT).isoformat(timespec="seconds")
    except Exception:
        return None


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


def _write_jsonl(path: str, rows: list[dict[str, Any]], replace: bool = True) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    mode = "w" if replace else "a"
    with open(path, mode, encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, separators=(",", ":"), sort_keys=True, default=str) + "\n")
    return os.path.abspath(path)


def _packet_dir(day: str) -> str:
    return os.path.join(OUT_DIR, str(day))


def packet_path(day: str, label: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in label).strip("._")
    return os.path.join(_packet_dir(day), f"{safe}_{day}.jsonl")


def summary_path(day: str, label: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in label).strip("._")
    return os.path.join(_packet_dir(day), f"{safe}_{day}.summary.json")


def diff_path(day: str) -> str:
    return os.path.join(_packet_dir(day), f"canonical_packet_diff_{day}.json")


def _score_breakdown(feature_snapshot: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
    signal_quality = feature_snapshot.get("signal_quality") if isinstance(feature_snapshot.get("signal_quality"), dict) else {}
    score_model = signal_quality.get("score_model") if isinstance(signal_quality.get("score_model"), dict) else {}
    components = feature_snapshot.get("components") if isinstance(feature_snapshot.get("components"), dict) else {}
    return {
        "score": row.get("score") if row.get("score") is not None else feature_snapshot.get("score"),
        "conviction": row.get("conviction") if row.get("conviction") is not None else feature_snapshot.get("conviction"),
        "components": components,
        "score_components": signal_quality.get("score_components") or {},
        "score_model": score_model,
        "profile_score": ((row.get("scoring_profile") or {}).get("score")
                          if isinstance(row.get("scoring_profile"), dict) else None),
    }


def _identity(source: str, stage: str, row: dict[str, Any], ts: int | None) -> dict[str, Any]:
    return {
        "source_system": source,
        "stage": stage,
        "parity_key": row.get("parity_key"),
        "opportunity_id": row.get("opportunity_id"),
        "trade_id": row.get("trade_id"),
        "ticker": row.get("ticker"),
        "side": row.get("side"),
        "setup_type": row.get("setup_type"),
        "timestamp_second": ts,
    }


def _finalize(packet: dict[str, Any]) -> dict[str, Any]:
    identity = packet.get("identity") or {}
    join_key = (
        identity.get("parity_key")
        or identity.get("opportunity_id")
        or ":".join(str(identity.get(k) or "") for k in ("ticker", "side", "timestamp_second", "setup_type"))
    )
    packet["join_key"] = join_key
    packet["packet_id"] = stable_json_hash({
        "schema_version": packet.get("schema_version"),
        "source_system": packet.get("source_system"),
        "stage": packet.get("stage"),
        "identity": identity,
        "decision": packet.get("decision"),
        "reason": packet.get("reason"),
    })[:32]
    packet["packet_hash"] = stable_json_hash(packet)
    return packet


def from_live_signal_parity(row: dict[str, Any]) -> dict[str, Any]:
    ts = _int(row.get("created_at"))
    day = row.get("day") or _day_from_ts(ts)
    features = row.get("feature_snapshot") if isinstance(row.get("feature_snapshot"), dict) else {}
    extra = row.get("extra") if isinstance(row.get("extra"), dict) else {}
    run_context = row.get("run_context") if isinstance(row.get("run_context"), dict) else {}
    execution_intent = extra.get("execution_intent") or row.get("execution_intent") or {}
    execution_result = extra.get("execution_result") or row.get("execution_result") or {}
    packet = {
        "schema_version": SCHEMA_VERSION,
        "packet_type": "canonical_decision_packet",
        "source_system": "live",
        "stage": "entry",
        "day": day,
        "created_at": ts,
        "created_at_ct": row.get("created_at_ct") or _iso_ct(ts),
        "identity": _identity("live", "entry", row, ts),
        "decision": row.get("decision"),
        "reason": row.get("reason"),
        "market": {
            "entry_price": _num(row.get("price")),
            "freshness": row.get("market_freshness") or {},
        },
        "features": {
            "feature_snapshot_hash": row.get("feature_snapshot_hash"),
            "feature_snapshot": features,
            "score_breakdown": _score_breakdown(features, row),
        },
        "brackets": {
            "entry_price": _num(row.get("price")),
            "tp_price": _num(extra.get("tp")),
            "sl_price": _num(extra.get("sl")),
            "tp_send": _num(extra.get("tp_send")),
            "sl_send": _num(extra.get("sl_send")),
            "policy": extra.get("bracket_policy") or {},
            "rounding": "nearest_cent_half_up",
        },
        "state": {
            "execution_mode": row.get("execution_mode"),
            "strategy_config_hash": row.get("strategy_config_hash"),
            "execution_kernel_hash": row.get("execution_kernel_hash"),
            "step2_parity_contract_hash": row.get("step2_parity_contract_hash"),
            "step2_execution_contract_hash": row.get("step2_execution_contract_hash"),
            "operational_state": row.get("operational_state") or {},
        },
        "profile": {
            "name": row.get("active_profile_name"),
            "hash": row.get("active_profile_hash"),
            "bias": row.get("active_profile_bias"),
            "weights": row.get("active_profile_weights") or {},
        },
        "execution": {
            "broker_action": "submitted" if row.get("decision") == "entered" else "none",
            "trade_id": row.get("trade_id"),
            "client_order_id": row.get("client_order_id"),
            "broker_order_id": row.get("broker_order_id"),
            "qty": _num(extra.get("qty")),
            "alloc": _num(extra.get("alloc")),
            "latency_chain": row.get("latency_chain") or {},
            "pre_action_packet_id": extra.get("pre_action_packet_id"),
            "execution_intent_id": (
                execution_intent.get("execution_intent_id") if isinstance(execution_intent, dict) else None
            ),
            "semantic_execution_intent_hash": (
                execution_intent.get("semantic_execution_intent_hash") if isinstance(execution_intent, dict) else None
            ),
            "execution_result_id": (
                execution_result.get("execution_result_id") if isinstance(execution_result, dict) else None
            ),
        },
        "action_plan": extra.get("action_plan") or row.get("action_plan") or None,
        "execution_intent": execution_intent or None,
        "execution_result": execution_result or None,
        "outcome": row.get("outcome_summary") or None,
        "source_row": "live_signal_parity",
    }
    return _finalize(packet)


def from_live_pre_action(sig: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    ts = _int(context.get("created_at") or sig.get("ts")) or int(time.time())
    row = {
        "parity_key": context.get("parity_key"),
        "opportunity_id": sig.get("opportunity_id"),
        "trade_id": context.get("trade_id"),
        "ticker": sig.get("ticker"),
        "side": sig.get("side"),
        "setup_type": sig.get("setup_type"),
    }
    features = context.get("feature_snapshot") if isinstance(context.get("feature_snapshot"), dict) else {}
    execution_intent = context.get("execution_intent") or {}
    execution_result = context.get("execution_result") or {}
    packet = {
        "schema_version": SCHEMA_VERSION,
        "packet_type": "canonical_decision_packet",
        "source_system": "live",
        "stage": "entry_pre_action",
        "day": _day_from_ts(ts),
        "created_at": ts,
        "created_at_ct": _iso_ct(ts),
        "identity": _identity("live", "entry_pre_action", row, ts),
        "decision": context.get("decision") or "prepared",
        "reason": context.get("reason") or "pre_action_ready",
        "market": {
            "entry_price": _num(context.get("entry_price") or sig.get("price")),
            "freshness": context.get("market_freshness") or {},
        },
        "features": {
            "feature_snapshot_hash": context.get("feature_snapshot_hash"),
            "feature_snapshot": features,
            "score_breakdown": _score_breakdown(features, {**sig, **context}),
        },
        "brackets": {
            "entry_price": _num(context.get("entry_price") or sig.get("price")),
            "tp_price": _num(context.get("tp")),
            "sl_price": _num(context.get("sl")),
            "tp_send": _num(context.get("tp_send")),
            "sl_send": _num(context.get("sl_send")),
            "policy": context.get("bracket_policy") or {},
            "rounding": "nearest_cent_half_up",
        },
        "state": {
            "execution_mode": context.get("execution_mode"),
            "strategy_config_hash": context.get("strategy_config_hash"),
            "execution_kernel_hash": context.get("execution_kernel_hash"),
            "step2_parity_contract_hash": context.get("step2_parity_contract_hash"),
            "step2_execution_contract_hash": context.get("step2_execution_contract_hash"),
            "operational_state": context.get("operational_state") or {},
        },
        "profile": context.get("profile") or {},
        "execution": {
            "broker_action": context.get("broker_action") or "submit_bracket_order",
            "trade_id": context.get("trade_id"),
            "client_order_id": context.get("client_order_id"),
            "qty": _num(context.get("qty")),
            "alloc": _num(context.get("alloc")),
            "latency_chain": context.get("latency_chain") or {},
            "execution_intent_id": (
                execution_intent.get("execution_intent_id") if isinstance(execution_intent, dict) else None
            ),
            "semantic_execution_intent_hash": (
                execution_intent.get("semantic_execution_intent_hash") if isinstance(execution_intent, dict) else None
            ),
            "execution_result_id": (
                execution_result.get("execution_result_id") if isinstance(execution_result, dict) else None
            ),
        },
        "action_plan": context.get("action_plan") or None,
        "execution_intent": execution_intent or None,
        "execution_result": execution_result or None,
        "outcome": None,
        "source_row": "live_pre_action",
    }
    return _finalize(packet)


def from_step2_decision(row: dict[str, Any]) -> dict[str, Any]:
    ts = _int(row.get("created_at"))
    day = row.get("day") or _day_from_ts(ts)
    features = row.get("feature_snapshot") if isinstance(row.get("feature_snapshot"), dict) else {}
    outcome = row.get("outcome_summary") or row.get("outcome") or {}
    extra = row.get("extra") if isinstance(row.get("extra"), dict) else {}
    run_context = row.get("run_context") if isinstance(row.get("run_context"), dict) else {}
    execution_intent = row.get("execution_intent") or {}
    execution_result = row.get("execution_result") or {}
    packet = {
        "schema_version": SCHEMA_VERSION,
        "packet_type": "canonical_decision_packet",
        "source_system": "step2",
        "stage": "entry",
        "day": day,
        "created_at": ts,
        "created_at_ct": row.get("created_at_ct") or _iso_ct(ts),
        "identity": _identity("step2", "entry", row, ts),
        "decision": row.get("decision"),
        "reason": row.get("reason"),
        "market": {
            "entry_price": _num(row.get("price") if row.get("price") is not None else outcome.get("entry")),
            "freshness": row.get("market_freshness") or {},
        },
        "features": {
            "feature_snapshot_hash": row.get("feature_snapshot_hash"),
            "feature_snapshot": features,
            "score_breakdown": _score_breakdown(features, row),
        },
        "brackets": {
            "entry_price": _num(row.get("price") if row.get("price") is not None else outcome.get("entry")),
            "tp_price": _num(extra.get("tp") if extra.get("tp") is not None else outcome.get("tp")),
            "sl_price": _num(extra.get("sl") if extra.get("sl") is not None else outcome.get("sl")),
            "tp_send": _num(extra.get("tp_send") if extra.get("tp_send") is not None else extra.get("tp")),
            "sl_send": _num(extra.get("sl_send") if extra.get("sl_send") is not None else extra.get("sl")),
            "policy": row.get("bracket_policy") or extra.get("bracket_policy") or {},
            "rounding": "nearest_cent_half_up",
        },
        "state": {
            "execution_mode": row.get("execution_mode") or "step2_replay",
            "strategy_config_hash": row.get("strategy_config_hash"),
            "execution_kernel_hash": run_context.get("execution_kernel_hash") or row.get("execution_kernel_hash"),
            "step2_parity_contract_hash": row.get("step2_parity_contract_hash") or run_context.get("step2_parity_contract_hash"),
            "step2_execution_contract_hash": run_context.get("step2_execution_contract_hash") or row.get("step2_execution_contract_hash"),
            "operational_state": row.get("operational_state") or {},
        },
        "profile": {
            "name": row.get("active_profile_name"),
            "hash": row.get("active_profile_hash"),
            "bias": row.get("active_profile_bias"),
        },
        "execution": {
            "broker_action": "simulated_entry" if row.get("decision") == "entered" else "none",
            "latency_chain": (row.get("latency_model") or {}),
            "execution_intent_id": (
                execution_intent.get("execution_intent_id") if isinstance(execution_intent, dict) else None
            ),
            "semantic_execution_intent_hash": (
                execution_intent.get("semantic_execution_intent_hash") if isinstance(execution_intent, dict) else None
            ),
            "execution_result_id": (
                execution_result.get("execution_result_id") if isinstance(execution_result, dict) else None
            ),
        },
        "action_plan": row.get("action_plan") or None,
        "execution_intent": execution_intent or None,
        "execution_result": execution_result or None,
        "outcome": outcome or None,
        "source_row": "step2_decision_parity",
    }
    return _finalize(packet)


def from_closed_trade(trade: dict[str, Any]) -> dict[str, Any]:
    ts = _int(trade.get("closed_at"))
    row = {
        "parity_key": trade.get("live_signal_parity_key"),
        "opportunity_id": trade.get("opportunity_id"),
        "trade_id": trade.get("trade_id"),
        "ticker": trade.get("ticker"),
        "side": trade.get("side"),
        "setup_type": ((trade.get("entry_thesis") or {}).get("setup_type")
                       if isinstance(trade.get("entry_thesis"), dict) else trade.get("setup_type")),
    }
    entry_execution_intent = trade.get("entry_execution_intent") or {}
    entry_execution_result = trade.get("entry_execution_result") or {}
    exit_execution_intent = trade.get("exit_execution_intent") or {}
    exit_execution_result = trade.get("exit_execution_result") or {}
    packet = {
        "schema_version": SCHEMA_VERSION,
        "packet_type": "canonical_decision_packet",
        "source_system": "live",
        "stage": "exit",
        "day": _day_from_ts(ts),
        "created_at": ts,
        "created_at_ct": _iso_ct(ts),
        "identity": _identity("live", "exit", row, ts),
        "decision": "closed",
        "reason": trade.get("reason"),
        "market": {
            "entry_price": _num(trade.get("entry")),
            "exit_price": _num(trade.get("exit")),
        },
        "features": {
            "feature_snapshot_hash": trade.get("live_signal_parity_feature_hash"),
            "score_breakdown": {
                "score": trade.get("entry_score"),
                "conviction": trade.get("conviction"),
            },
        },
        "brackets": {
            "entry_price": _num(trade.get("entry")),
            "tp_price": _num(trade.get("tp")),
            "sl_price": _num(trade.get("sl")),
            "policy": trade.get("bracket_policy") or {},
            "rounding": "nearest_cent_half_up",
        },
        "state": {
            "strategy_config_hash": trade.get("strategy_config_hash"),
            "execution_kernel_hash": trade.get("execution_kernel_hash"),
            "step2_execution_contract_hash": trade.get("step2_execution_contract_hash"),
        },
        "profile": {},
        "execution": {
            "broker_action": "closed",
            "trade_id": trade.get("trade_id"),
            "latency_chain": trade.get("latency_chain") or {},
            "entry_timing": trade.get("entry_timing") or {},
            "exit_latency_attribution": trade.get("exit_latency_attribution") or {},
            "entry_execution_intent_id": (
                entry_execution_intent.get("execution_intent_id") if isinstance(entry_execution_intent, dict) else None
            ),
            "entry_execution_result_id": (
                entry_execution_result.get("execution_result_id") if isinstance(entry_execution_result, dict) else None
            ),
            "exit_execution_intent_id": (
                exit_execution_intent.get("execution_intent_id") if isinstance(exit_execution_intent, dict) else None
            ),
            "semantic_exit_execution_intent_hash": (
                exit_execution_intent.get("semantic_execution_intent_hash") if isinstance(exit_execution_intent, dict) else None
            ),
            "exit_execution_result_id": (
                exit_execution_result.get("execution_result_id") if isinstance(exit_execution_result, dict) else None
            ),
        },
        "action_plan": trade.get("exit_action_plan") or None,
        "execution_intent": exit_execution_intent or None,
        "execution_result": exit_execution_result or None,
        "outcome": {
            "pnl": trade.get("pnl"),
            "result": trade.get("result"),
            "entry": trade.get("entry"),
            "exit": trade.get("exit"),
            "qty": trade.get("qty"),
            "opened_at": trade.get("opened_at"),
            "closed_at": trade.get("closed_at"),
        },
        "source_row": "trade_closed",
    }
    return _finalize(packet)


def _label_for(packet: dict[str, Any]) -> str:
    source = str(packet.get("source_system") or "unknown")
    stage = str(packet.get("stage") or "decision")
    if source == "step2":
        return "step2_decision_packets"
    if source == "live" and stage == "exit":
        return "live_exit_packets"
    if source == "live" and stage == "entry_pre_action":
        return "live_pre_action_packets"
    if source == "live":
        return "live_decision_packets"
    return f"{source}_{stage}_packets"


def append_packet(packet: dict[str, Any]) -> str:
    day = str(packet.get("day") or _day_from_ts(packet.get("created_at")))
    label = _label_for(packet)
    path = packet_path(day, label)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(packet, separators=(",", ":"), sort_keys=True, default=str) + "\n")
    return os.path.abspath(path)


def write_packets(day: str, packets: list[dict[str, Any]], label: str) -> dict[str, Any]:
    path = packet_path(day, label)
    _write_jsonl(path, packets, replace=True)
    decisions = Counter(str(p.get("decision") or "unknown") for p in packets)
    stages = Counter(str(p.get("stage") or "unknown") for p in packets)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "source": "canonical_decision_packet",
        "day": day,
        "label": label,
        "created_at_ct": datetime.now(CT).isoformat(timespec="seconds"),
        "path": os.path.abspath(path),
        "rows": len(packets),
        "decisions": dict(sorted(decisions.items())),
        "stages": dict(sorted(stages.items())),
        "packet_schema_version": SCHEMA_VERSION,
    }
    summary["summary_path"] = _write_json(summary_path(day, label), summary)
    return summary


def write_step2_packets(day: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    packets = []
    for row in rows:
        packet = from_step2_decision(row)
        row["canonical_decision_packet_id"] = packet.get("packet_id")
        packets.append(packet)
    return write_packets(day, packets, "step2_decision_packets")


def _unified_ledger_path(day: str, label: str) -> str:
    return os.path.join(HERE, "postmortem", "unified_decision_ledger", day, f"{label}_{day}.jsonl")


def _coerce_unified_step2_row(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    status = str(out.get("decision_status") or "").lower()
    if not out.get("created_at"):
        out["created_at"] = out.get("ts")
    if not out.get("created_at_ct"):
        out["created_at_ct"] = out.get("ts_ct")
    if not out.get("decision"):
        out["decision"] = "entered" if status == "accepted" else "skipped"
    if not out.get("reason"):
        out["reason"] = "entered" if out.get("decision") == "entered" else (out.get("reject_reason") or status or "rejected")
    if not out.get("outcome_summary") and isinstance(out.get("outcome"), dict):
        out["outcome_summary"] = out.get("outcome")
    run_context = out.get("run_context") if isinstance(out.get("run_context"), dict) else {}
    if not out.get("active_profile_name"):
        out["active_profile_name"] = run_context.get("variant")
    if not out.get("active_profile_bias"):
        out["active_profile_bias"] = run_context.get("bias")
    return out


def build_from_existing(day: str, replace: bool = True) -> dict[str, Any]:
    del replace  # retained for CLI/API clarity; build is always deterministic.
    live_rows = _read_jsonl(os.path.join(HERE, "postmortem", "live_signal_parity", f"live_signal_parity_{day}.jsonl"))
    if not live_rows:
        live_rows = _read_jsonl(_unified_ledger_path(day, "live_signal_parity"))
    step2_rows = _read_jsonl(os.path.join(HERE, "postmortem", "step2_decision_parity", f"step2_decision_parity_{day}.jsonl"))
    if not step2_rows:
        step2_rows = [_coerce_unified_step2_row(r) for r in _read_jsonl(_unified_ledger_path(day, "step2_current_trace"))]
    trade_rows = _read_jsonl(os.path.join(HERE, "postmortem", "trades", f"trades_{day}.jsonl"))
    summaries = {
        "live_decision_packets": write_packets(day, [from_live_signal_parity(r) for r in live_rows], "live_decision_packets"),
        "step2_decision_packets": write_packets(day, [from_step2_decision(r) for r in step2_rows], "step2_decision_packets"),
        "live_exit_packets": write_packets(day, [from_closed_trade(r) for r in trade_rows], "live_exit_packets"),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "canonical_decision_packet",
        "day": day,
        "created_at_ct": datetime.now(CT).isoformat(timespec="seconds"),
        "summaries": summaries,
        "rows": {name: summary.get("rows") for name, summary in summaries.items()},
    }


def _packet_rows(day: str, label: str) -> list[dict[str, Any]]:
    return _read_jsonl(packet_path(day, label))


def validate_packet(packet: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for key in ("schema_version", "packet_type", "source_system", "stage", "day", "identity", "decision"):
        if packet.get(key) in (None, ""):
            errors.append(f"missing:{key}")
    if packet.get("schema_version") != SCHEMA_VERSION:
        errors.append("schema_version_mismatch")
    identity = packet.get("identity") if isinstance(packet.get("identity"), dict) else {}
    for key in ("ticker", "side", "timestamp_second"):
        if identity.get(key) in (None, ""):
            errors.append(f"identity_missing:{key}")
    if not packet.get("packet_hash"):
        errors.append("missing:packet_hash")
    return errors


def schema_compatibility(day: str) -> dict[str, Any]:
    build_from_existing(day)
    labels = ("live_decision_packets", "step2_decision_packets", "live_exit_packets")
    errors: list[dict[str, Any]] = []
    counts = {}
    for label in labels:
        rows = _packet_rows(day, label)
        counts[label] = len(rows)
        for row in rows[:10000]:
            row_errors = validate_packet(row)
            if row_errors:
                errors.append({
                    "label": label,
                    "packet_id": row.get("packet_id"),
                    "join_key": row.get("join_key"),
                    "errors": row_errors,
                })
                if len(errors) >= 50:
                    break
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "canonical_decision_packet",
        "day": day,
        "ok": not errors and counts.get("step2_decision_packets", 0) > 0,
        "counts": counts,
        "errors": errors,
        "diff_path": diff_path(day),
    }


def _entry_map(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out = {}
    for row in rows:
        if row.get("stage") != "entry":
            continue
        key = str(row.get("join_key") or "")
        if key:
            out[key] = row
    return out


def _dig(row: dict[str, Any], path: str) -> Any:
    cur: Any = row
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def diff_day(day: str) -> dict[str, Any]:
    build_from_existing(day)
    live = _entry_map(_packet_rows(day, "live_decision_packets"))
    step2 = _entry_map(_packet_rows(day, "step2_decision_packets"))
    shared = sorted(set(live) & set(step2))
    live_only = sorted(set(live) - set(step2))
    step2_only = sorted(set(step2) - set(live))
    fields = [
        "decision",
        "reason",
        "identity.ticker",
        "identity.side",
        "identity.setup_type",
        "features.feature_snapshot_hash",
        "features.score_breakdown.score",
        "brackets.entry_price",
        "brackets.tp_price",
        "brackets.sl_price",
        "state.execution_kernel_hash",
        "state.step2_parity_contract_hash",
        "state.step2_execution_contract_hash",
        "profile.hash",
        "action_plan.semantic_action_hash",
        "execution.semantic_execution_intent_hash",
    ]
    mismatches: list[dict[str, Any]] = []
    field_counts: Counter[str] = Counter()
    for key in shared:
        lrow = live[key]
        srow = step2[key]
        diffs = []
        for field in fields:
            lv = _dig(lrow, field)
            sv = _dig(srow, field)
            if lv != sv and not (lv in (None, "") and sv in (None, "")):
                diffs.append({"field": field, "live": lv, "step2": sv})
                field_counts[field] += 1
        if diffs:
            mismatches.append({
                "join_key": key,
                "ticker": _dig(lrow, "identity.ticker") or _dig(srow, "identity.ticker"),
                "side": _dig(lrow, "identity.side") or _dig(srow, "identity.side"),
                "created_at_ct": lrow.get("created_at_ct") or srow.get("created_at_ct"),
                "diffs": diffs[:25],
            })
    schema = schema_compatibility(day)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "source": "canonical_decision_packet",
        "day": day,
        "created_at_ct": datetime.now(CT).isoformat(timespec="seconds"),
        "ok": bool(schema.get("ok")),
        "shared_entry_packets": len(shared),
        "live_only_entry_packets": len(live_only),
        "step2_only_entry_packets": len(step2_only),
        "mismatch_count": len(mismatches),
        "field_mismatch_counts": dict(sorted(field_counts.items())),
        "examples": {
            "mismatches": mismatches[:50],
            "live_only": live_only[:50],
            "step2_only": step2_only[:50],
        },
        "schema_compatibility": schema,
        "paths": {
            "live": packet_path(day, "live_decision_packets"),
            "step2": packet_path(day, "step2_decision_packets"),
            "live_exit": packet_path(day, "live_exit_packets"),
        },
        "deduction": (
            "This compares normalized packets, not raw ad hoc rows. A mismatch is a precise field-level "
            "lead for why Live and Step 2 diverged."
        ),
    }
    payload["path"] = _write_json(diff_path(day), payload)
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description="Build or diff canonical decision packets.")
    sub = ap.add_subparsers(dest="cmd", required=True)
    build = sub.add_parser("build-day")
    build.add_argument("day")
    diff = sub.add_parser("diff")
    diff.add_argument("day")
    compat = sub.add_parser("compat")
    compat.add_argument("day")
    args = ap.parse_args()
    if args.cmd == "build-day":
        payload = build_from_existing(args.day)
    elif args.cmd == "diff":
        payload = diff_day(args.day)
    else:
        payload = schema_compatibility(args.day)
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0 if payload.get("ok", True) else 1


if __name__ == "__main__":
    raise SystemExit(main())
