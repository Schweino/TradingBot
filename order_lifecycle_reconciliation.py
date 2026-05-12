"""Reconcile Live order lifecycle from signal to final close.

This turns the raw execution lifecycle/event/audit rows into a compact verdict:
which trades had a complete signal -> intent -> submit/fill -> exit -> close
chain, which rows are missing joins, and where broker/live behavior diverged.
"""
from __future__ import annotations

from output_paths import output_path

import argparse
import glob
import json
import os
import time
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import execution_lifecycle
import broker_lifecycle_guard


HERE = os.path.dirname(os.path.abspath(__file__))
POSTMORTEM_DIR = output_path("postmortem")
OUT_DIR = os.path.join(POSTMORTEM_DIR, "order_lifecycle_reconciliation")
CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 2

ENTRY_EVENTS = {"entry_reserved", "entry_intent_created", "entry_execution_result", "entry_submitted", "entry_committed", "entry_filled"}
EXIT_EVENTS = {"exit_intent_created", "exit_execution_result", "bracket_exit_filled", "local_strict_broker_cleanup_submitted", "broker_flat_verified", "position_closed"}
TERMINAL_EVENTS = {"position_closed", "entry_fill_timeout_cancelled", "entry_pre_submit_blocked", "broker_flat_verified"}


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


def _jsonl_rows(path: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not os.path.exists(path):
        return rows
    with open(path, "r", encoding="utf-8-sig") as f:
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


def _write_text(path: str, text: str) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.{os.getpid()}.{int(time.time() * 1000)}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)
    return os.path.abspath(path)


def paths(day: str) -> dict[str, str]:
    return {
        "execution_lifecycle": os.path.join(POSTMORTEM_DIR, "execution_lifecycle", day, f"execution_lifecycle_{day}.jsonl"),
        "execution_lifecycle_summary": os.path.join(POSTMORTEM_DIR, "execution_lifecycle", day, f"execution_lifecycle_{day}.summary.json"),
        "live_signal_parity": os.path.join(POSTMORTEM_DIR, "live_signal_parity", f"live_signal_parity_{day}.jsonl"),
        "trades": os.path.join(POSTMORTEM_DIR, "trades", f"trades_{day}.jsonl"),
        "latency_attribution": os.path.join(POSTMORTEM_DIR, "latency_attribution", f"latency_attribution_{day}.jsonl"),
        "legacy_audit": output_path("audit", f"trade_lifecycle_{day}.jsonl"),
        "json": os.path.join(OUT_DIR, f"order_lifecycle_reconciliation_{day}.json"),
        "txt": os.path.join(OUT_DIR, f"order_lifecycle_reconciliation_{day}.txt"),
    }


def _issue(level: str, kind: str, detail: dict[str, Any]) -> dict[str, Any]:
    return {"level": level, "kind": kind, "detail": detail}


def _trade_id(row: dict[str, Any]) -> str:
    data = row.get("data") if isinstance(row.get("data"), dict) else {}
    decision = data.get("decision_audit") if isinstance(data.get("decision_audit"), dict) else {}
    return str(row.get("trade_id") or data.get("trade_id") or decision.get("trade_id") or "")


def _rows_by_trade(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        tid = _trade_id(row)
        if tid:
            out[tid].append(row)
    return out


def _identity_from_rows(rows: list[dict[str, Any]]) -> dict[str, list[str]]:
    merged = {
        "trade_ids": set(),
        "client_order_ids": set(),
        "broker_order_ids": set(),
        "symbols": set(),
    }
    for row in rows:
        ident = broker_lifecycle_guard.extract_trade_identity(row)
        for key in merged:
            merged[key].update(ident.get(key) or [])
    return {key: sorted(values) for key, values in merged.items()}


def _nested_has_key(value: Any, key: str) -> bool:
    if isinstance(value, dict):
        if key in value and value.get(key) not in (None, "", [], {}):
            return True
        return any(_nested_has_key(child, key) for child in value.values())
    if isinstance(value, list):
        return any(_nested_has_key(child, key) for child in value)
    return False


def _nested_has_any_key(value: Any, keys: set[str]) -> bool:
    return any(_nested_has_key(value, key) for key in keys)


def _latest_broker_snapshot(day: str) -> dict[str, Any] | None:
    pattern = os.path.join(POSTMORTEM_DIR, "broker_safety_snapshots", f"broker_safety_{day}_*.json")
    try:
        candidates = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
    except Exception:
        candidates = []
    for path in candidates:
        payload = _read_json(path, None)
        if isinstance(payload, dict):
            payload["_path"] = path
            return payload
    return None


def _broker_snapshot_reconciliation(day: str,
                                    broker_snapshot: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(broker_snapshot, dict):
        return None
    broker = broker_snapshot.get("broker") if isinstance(broker_snapshot.get("broker"), dict) else {}
    local = broker_snapshot.get("local") if isinstance(broker_snapshot.get("local"), dict) else {}
    state = {
        "positions": local.get("positions") or {},
        "pending_entries": local.get("pending_entries") or {},
    }
    watched = broker_snapshot.get("watched") or []
    return broker_lifecycle_guard.evaluate_runtime_broker_state(
        state,
        broker_positions=broker.get("watched_positions") or [],
        broker_orders=broker.get("watched_open_orders") or [],
        watched=watched,
        day=day,
        broker_reachable=bool(broker.get("reachable")),
        broker_error=broker.get("error"),
        label=str(broker_snapshot.get("label") or "broker_safety_snapshot"),
    )


def evaluate(day: str,
             lifecycle_rows: list[dict[str, Any]] | None = None,
             signal_rows: list[dict[str, Any]] | None = None,
             trade_rows: list[dict[str, Any]] | None = None,
             latency_rows: list[dict[str, Any]] | None = None,
             lifecycle_summary: dict[str, Any] | None = None,
             broker_snapshot: dict[str, Any] | None = None) -> dict[str, Any]:
    p = paths(day)
    explicit_inputs = any(
        value is not None
        for value in (lifecycle_rows, signal_rows, trade_rows, latency_rows, lifecycle_summary)
    )
    lifecycle_rows = lifecycle_rows if lifecycle_rows is not None else _jsonl_rows(p["execution_lifecycle"])
    if not lifecycle_rows:
        lifecycle_summary = lifecycle_summary or execution_lifecycle.replay_day(day)
        normalizer = getattr(execution_lifecycle, "_normalize_audit_row", None)
        if normalizer is not None:
            lifecycle_rows = [
                row for row in (normalizer(audit) for audit in _jsonl_rows(p["legacy_audit"]))
                if row is not None
            ]
    signal_rows = signal_rows if signal_rows is not None else _jsonl_rows(p["live_signal_parity"])
    trade_rows = trade_rows if trade_rows is not None else _jsonl_rows(p["trades"])
    latency_rows = latency_rows if latency_rows is not None else _jsonl_rows(p["latency_attribution"])
    lifecycle_summary = lifecycle_summary if lifecycle_summary is not None else (_read_json(p["execution_lifecycle_summary"], {}) or {})
    broker_snapshot = broker_snapshot if broker_snapshot is not None else (None if explicit_inputs else _latest_broker_snapshot(day))

    by_trade: dict[str, list[dict[str, Any]]] = defaultdict(list)
    event_counts = Counter()
    stage_counts = Counter()
    for row in lifecycle_rows:
        tid = _trade_id(row)
        event_counts[str(row.get("event") or "unknown")] += 1
        stage_counts[str(row.get("stage") or "unknown")] += 1
        if tid:
            by_trade[tid].append(row)

    entered_signals = [row for row in signal_rows if row.get("decision") == "entered"]
    entered_ids = {str(row.get("trade_id")) for row in entered_signals if row.get("trade_id")}
    closed_ids = {str(row.get("trade_id")) for row in trade_rows if row.get("trade_id")}
    latency_ids = {str(row.get("trade_id")) for row in latency_rows if row.get("trade_id")}
    lifecycle_ids = set(by_trade)
    signals_by_trade = _rows_by_trade(entered_signals)
    trades_by_trade = _rows_by_trade(trade_rows)
    latency_by_trade = _rows_by_trade(latency_rows)

    issues: list[dict[str, Any]] = []
    for tid in sorted(entered_ids - lifecycle_ids):
        issues.append(_issue("critical", "entered_signal_missing_lifecycle", {"trade_id": tid}))
    for tid in sorted(closed_ids - lifecycle_ids):
        issues.append(_issue("critical", "closed_trade_missing_lifecycle", {"trade_id": tid}))
    for tid in sorted(closed_ids - entered_ids):
        issues.append(_issue("warning", "closed_trade_missing_entered_signal", {"trade_id": tid}))
    for tid in sorted(entered_ids - closed_ids):
        events = {str(row.get("event") or "") for row in by_trade.get(tid, [])}
        if not events.intersection(TERMINAL_EVENTS):
            issues.append(_issue("warning", "entered_signal_without_terminal_trade", {
                "trade_id": tid,
                "events": sorted(events),
            }))
    for tid in sorted(closed_ids - latency_ids):
        issues.append(_issue("warning", "closed_trade_missing_latency_row", {"trade_id": tid}))

    trade_chains = []
    client_owner_index: dict[str, set[str]] = defaultdict(set)
    broker_owner_index: dict[str, set[str]] = defaultdict(set)
    trade_owner_index: dict[str, set[str]] = defaultdict(set)
    for tid in sorted(lifecycle_ids | entered_ids | closed_ids):
        rows = sorted(by_trade.get(tid, []), key=lambda r: (int(r.get("ts") or 0), str(r.get("event_id") or "")))
        events = [str(row.get("event") or "") for row in rows]
        event_set = set(events)
        first = rows[0] if rows else {}
        last = rows[-1] if rows else {}
        related_rows = (
            rows
            + signals_by_trade.get(tid, [])
            + trades_by_trade.get(tid, [])
            + latency_by_trade.get(tid, [])
        )
        identity = _identity_from_rows(related_rows)
        client_ids = identity.get("client_order_ids") or []
        broker_ids = identity.get("broker_order_ids") or []
        for ident in client_ids:
            client_owner_index[ident].add(tid)
        for ident in broker_ids:
            broker_owner_index[ident].add(tid)
        for ident in identity.get("trade_ids") or [tid]:
            trade_owner_index[ident].add(tid)
        has_fill_attribution = any(_nested_has_key(row, "fill_attribution") for row in related_rows)
        has_entry_fill_price = any(_nested_has_any_key(row, {"entry_fill_price", "filled_avg_price"}) for row in related_rows)
        has_exit_fill = any(
            _nested_has_any_key(row, {"exit_broker_filled_ms", "exit_local_closed_ms", "broker_flat_verified", "local_strict_exit_price"})
            or str(row.get("event") or "") in {"bracket_exit_filled", "broker_flat_verified", "position_closed"}
            for row in related_rows
        )
        broker_backed = bool(client_ids or broker_ids)
        chain = {
            "trade_id": tid,
            "symbol": (
                first.get("symbol")
                or next((r.get("ticker") for r in entered_signals if str(r.get("trade_id")) == tid), None)
                or next(iter(identity.get("symbols") or []), None)
            ),
            "first_ts": first.get("ts"),
            "last_ts": last.get("ts"),
            "events": events,
            "identity": identity,
            "has_entered_signal": tid in entered_ids,
            "has_closed_trade": tid in closed_ids,
            "has_latency": tid in latency_ids,
            "has_entry_intent": "entry_intent_created" in event_set,
            "has_entry_result": "entry_execution_result" in event_set,
            "has_entry_commit": "entry_committed" in event_set,
            "has_entry_fill_or_sandbox_commit": bool(event_set.intersection({"entry_filled", "entry_committed"})),
            "has_fill_attribution": has_fill_attribution,
            "has_entry_fill_attribution": bool(has_entry_fill_price or has_fill_attribution),
            "has_exit_intent": "exit_intent_created" in event_set,
            "has_exit_result_or_bracket": bool(event_set.intersection({"exit_execution_result", "bracket_exit_filled", "broker_flat_verified", "position_closed"})),
            "has_exit_fill_attribution": has_exit_fill,
            "broker_backed": broker_backed,
            "terminal_event": next((event for event in reversed(events) if event in TERMINAL_EVENTS), None),
        }
        if chain["has_closed_trade"] and not chain["has_entry_commit"]:
            issues.append(_issue("critical", "closed_trade_without_entry_commit", {"trade_id": tid, "events": events}))
        if chain["has_entry_commit"] and not chain["has_exit_result_or_bracket"] and chain["has_closed_trade"]:
            issues.append(_issue("warning", "closed_trade_missing_exit_execution_context", {"trade_id": tid, "events": events}))
        if "entry_submitted" in event_set and not event_set.intersection({"entry_filled", "entry_committed", "entry_fill_timeout_cancelled"}):
            issues.append(_issue("critical", "submitted_entry_without_fill_commit_or_cancel", {"trade_id": tid, "events": events}))
        if chain["has_closed_trade"] and broker_backed and not chain["has_entry_fill_attribution"]:
            issues.append(_issue("warning", "closed_trade_missing_entry_fill_attribution", {"trade_id": tid, "client_order_ids": client_ids, "broker_order_ids": broker_ids}))
        if chain["has_closed_trade"] and broker_backed and not chain["has_exit_fill_attribution"]:
            issues.append(_issue("warning", "closed_trade_missing_exit_fill_attribution", {"trade_id": tid, "client_order_ids": client_ids, "broker_order_ids": broker_ids}))
        trade_chains.append(chain)

    identity_duplicates = []
    for kind, index in (("client_order_id", client_owner_index), ("broker_order_id", broker_owner_index), ("trade_id", trade_owner_index)):
        for ident, owners in sorted(index.items()):
            if len(owners) > 1:
                detail = {"kind": kind, "id": ident, "trade_ids": sorted(owners)}
                identity_duplicates.append(detail)
                issues.append(_issue("critical", "duplicate_order_identity", detail))

    broker_reconciliation = _broker_snapshot_reconciliation(day, broker_snapshot)
    if broker_reconciliation:
        for issue in broker_reconciliation.get("issues") or []:
            level = str(issue.get("level") or "warning")
            kind = f"broker_runtime_{issue.get('kind')}"
            issues.append(_issue(level, kind, issue.get("detail") or {}))

    critical = [row for row in issues if row.get("level") == "critical"]
    warnings = [row for row in issues if row.get("level") == "warning"]
    issue_counts = Counter(str(row.get("kind")) for row in issues)
    complete_chain_count = sum(
        1 for row in trade_chains
        if row.get("has_entered_signal")
        and row.get("has_entry_intent")
        and row.get("has_entry_commit")
        and row.get("has_exit_result_or_bracket")
        and row.get("has_closed_trade")
        and row.get("has_latency")
    )
    scorecard = {
        "complete_chain_count": complete_chain_count,
        "incomplete_chain_count": max(0, len(trade_chains) - complete_chain_count),
        "missing_fill_attribution_count": (
            issue_counts.get("closed_trade_missing_entry_fill_attribution", 0)
            + issue_counts.get("closed_trade_missing_latency_row", 0)
        ),
        "missing_exit_fill_count": issue_counts.get("closed_trade_missing_exit_fill_attribution", 0),
        "order_status_drift_count": issue_counts.get("broker_runtime_broker_order_symbol_matched_identity_unknown", 0),
        "orphan_bracket_risk_count": (
            issue_counts.get("broker_runtime_broker_open_order_unmapped", 0)
            + issue_counts.get("broker_runtime_duplicate_orphan_open_orders", 0)
        ),
        "duplicate_close_risk_count": issue_counts.get("broker_runtime_duplicate_close_risk", 0),
        "duplicate_identity_count": issue_counts.get("duplicate_order_identity", 0),
    }
    identity_map = {
        "client_order_id_to_trade_ids": {k: sorted(v) for k, v in sorted(client_owner_index.items())},
        "broker_order_id_to_trade_ids": {k: sorted(v) for k, v in sorted(broker_owner_index.items())},
        "trade_id_to_trade_ids": {k: sorted(v) for k, v in sorted(trade_owner_index.items())},
        "duplicates": identity_duplicates,
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "order_lifecycle_reconciliation",
        "day": day,
        "created_at_ct": _now_ct(),
        "ok": not critical,
        "verdict": "LIFECYCLE_FAIL" if critical else ("LIFECYCLE_WARN" if warnings else "LIFECYCLE_OK"),
        "critical_count": len(critical),
        "warning_count": len(warnings),
        "issue_kind_counts": dict(sorted(issue_counts.items())),
        "issues": issues[:200],
        "scorecard": scorecard,
        "summary": {
            "lifecycle_rows": len(lifecycle_rows),
            "lifecycle_trade_ids": len(lifecycle_ids),
            "entered_signal_trade_ids": len(entered_ids),
            "closed_trade_ids": len(closed_ids),
            "latency_trade_ids": len(latency_ids),
            "event_counts": dict(sorted(event_counts.items())),
            "stage_counts": dict(sorted(stage_counts.items())),
            "execution_lifecycle_anomalies": lifecycle_summary.get("anomalies") or [],
        },
        "canonical_identity_map": identity_map,
        "broker_runtime_reconciliation": broker_reconciliation,
        "repair_recommendations": (broker_reconciliation or {}).get("repair_recommendations") or [],
        "trade_chains": trade_chains[:500],
        "paths": p,
        "deduction": (
            "A parity-safe Live day needs each accepted signal to have an explainable lifecycle chain "
            "through entry, broker/local outcome, exit, final trade row, and latency attribution."
        ),
    }


def render_text(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") or {}
    scorecard = payload.get("scorecard") or {}
    lines = [
        f"Order Lifecycle Reconciliation - {payload.get('day')}",
        f"Created CT: {payload.get('created_at_ct')}",
        "",
        "Verdict",
        f"- Verdict: {payload.get('verdict')}",
        f"- OK: {payload.get('ok')}",
        f"- Critical/warnings: {payload.get('critical_count')}/{payload.get('warning_count')}",
        "",
        "Counts",
        f"- Lifecycle rows: {summary.get('lifecycle_rows')}",
        f"- Entered/closed/latency trade ids: {summary.get('entered_signal_trade_ids')}/{summary.get('closed_trade_ids')}/{summary.get('latency_trade_ids')}",
        f"- Lifecycle trade ids: {summary.get('lifecycle_trade_ids')}",
        f"- Event counts: {json.dumps(summary.get('event_counts') or {}, sort_keys=True)}",
        "",
        "Lifecycle Scorecard",
        f"- Complete/incomplete chains: {scorecard.get('complete_chain_count')}/{scorecard.get('incomplete_chain_count')}",
        f"- Missing fill attribution: {scorecard.get('missing_fill_attribution_count')}",
        f"- Missing exit fill: {scorecard.get('missing_exit_fill_count')}",
        f"- Order drift/orphan/duplicate-close: {scorecard.get('order_status_drift_count')}/{scorecard.get('orphan_bracket_risk_count')}/{scorecard.get('duplicate_close_risk_count')}",
        f"- Duplicate identities: {scorecard.get('duplicate_identity_count')}",
        "",
        "Issues",
    ]
    for issue in (payload.get("issues") or [])[:50]:
        lines.append(f"- {issue.get('level')} {issue.get('kind')}: {json.dumps(issue.get('detail') or {}, sort_keys=True)}")
    if not payload.get("issues"):
        lines.append("- none")
    recs = payload.get("repair_recommendations") or []
    if recs:
        lines.append("")
        lines.append("Repair Recommendations")
        for rec in recs[:10]:
            lines.append(f"- {rec.get('action')}: {rec.get('reason')}")
    lines.append("")
    return "\n".join(lines)


def build(day: str, write: bool = True) -> dict[str, Any]:
    payload = evaluate(day)
    if write:
        p = paths(day)
        payload["path"] = _write_json(p["json"], payload)
        payload["text_path"] = _write_text(p["txt"], render_text(payload))
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description="Reconcile Live order lifecycle for a day.")
    ap.add_argument("day")
    ap.add_argument("--no-write", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    payload = build(args.day, write=not args.no_write)
    print(json.dumps(payload, indent=2, sort_keys=True, default=str) if args.json else render_text(payload))
    return 0 if payload.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
