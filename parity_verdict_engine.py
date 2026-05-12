"""Promotion-grade verdict for Live vs Step 2 parity.

Reports are useful, but promotion decisions need a compact answer:
did Live and Step 2 behave the same way, and is the evidence complete enough
to trust the candidate?
"""
from __future__ import annotations

import argparse
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

import canonical_decision_packet
import execution_lifecycle


HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, "postmortem", "parity_verdict")
CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1

ENTRY_COMPARE_FIELDS = {
    "decision": "critical",
    "reason": "critical",
    "identity.ticker": "critical",
    "identity.side": "critical",
    "identity.setup_type": "critical",
    "features.feature_snapshot_hash": "critical",
    "features.score_breakdown.score": "critical",
    "brackets.entry_price": "critical",
    "brackets.tp_price": "critical",
    "brackets.sl_price": "critical",
    "state.execution_kernel_hash": "critical",
    "state.step2_parity_contract_hash": "critical",
    "state.step2_execution_contract_hash": "critical",
    "profile.hash": "critical",
    "action_plan.semantic_action_hash": "critical",
    "execution.semantic_execution_intent_hash": "critical",
}

ENTRY_REQUIRED_FIELDS = (
    "action_plan.semantic_action_hash",
    "execution.semantic_execution_intent_hash",
)

LIVE_ENTRY_REQUIRED_EVENTS = ("entry_intent_created", "entry_execution_result", "entry_committed")
LIVE_CLOSED_REQUIRED_EVENTS = ("exit_intent_created", "exit_execution_result", "position_closed")


def _read_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
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


def _write_text(path: str, text: str) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.{os.getpid()}.{int(time.time() * 1000)}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)
    return os.path.abspath(path)


def verdict_path(day: str) -> str:
    return os.path.join(OUT_DIR, f"parity_verdict_{day}.json")


def text_path(day: str) -> str:
    return os.path.join(OUT_DIR, f"parity_verdict_{day}.txt")


def _dig(row: dict[str, Any], path: str) -> Any:
    cur: Any = row
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _entry_map(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out = {}
    for row in rows:
        if row.get("stage") != "entry":
            continue
        key = str(row.get("join_key") or "")
        if key:
            out[key] = row
    return out


def _issue(kind: str, severity: str, message: str, **extra: Any) -> dict[str, Any]:
    payload = {
        "kind": kind,
        "severity": severity,
        "message": message,
    }
    payload.update({k: v for k, v in extra.items() if v is not None})
    return payload


def _is_entered(row: dict[str, Any]) -> bool:
    return str(row.get("decision") or "").lower() == "entered"


def _packet_field_issues(label: str, row: dict[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    if not _is_entered(row):
        return issues
    for field in ENTRY_REQUIRED_FIELDS:
        if _dig(row, field) in (None, ""):
            issues.append(_issue(
                "missing_required_entry_field",
                "critical",
                f"{label} entered packet is missing {field}",
                join_key=row.get("join_key"),
                field=field,
                source_system=row.get("source_system"),
                ticker=_dig(row, "identity.ticker"),
                side=_dig(row, "identity.side"),
            ))
    return issues


def _compare_entry_packets(live: dict[str, dict[str, Any]],
                           step2: dict[str, dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    shared = sorted(set(live) & set(step2))
    live_only = sorted(set(live) - set(step2))
    step2_only = sorted(set(step2) - set(live))
    for key in live_only:
        row = live[key]
        issues.append(_issue(
            "live_only_entry",
            "critical",
            "Live has an entry packet that Step 2 did not reproduce",
            join_key=key,
            ticker=_dig(row, "identity.ticker"),
            side=_dig(row, "identity.side"),
            created_at_ct=row.get("created_at_ct"),
        ))
    for key in step2_only:
        row = step2[key]
        issues.append(_issue(
            "step2_only_entry",
            "critical",
            "Step 2 has an entry packet that Live did not produce",
            join_key=key,
            ticker=_dig(row, "identity.ticker"),
            side=_dig(row, "identity.side"),
            created_at_ct=row.get("created_at_ct"),
        ))
    field_counts: Counter[str] = Counter()
    for key in shared:
        lrow = live[key]
        srow = step2[key]
        issues.extend(_packet_field_issues("live", lrow))
        issues.extend(_packet_field_issues("step2", srow))
        for field, severity in ENTRY_COMPARE_FIELDS.items():
            lv = _dig(lrow, field)
            sv = _dig(srow, field)
            if lv != sv and not (lv in (None, "") and sv in (None, "")):
                field_counts[field] += 1
                issues.append(_issue(
                    "entry_field_mismatch",
                    severity,
                    f"Live and Step 2 differ on {field}",
                    join_key=key,
                    field=field,
                    live=lv,
                    step2=sv,
                    ticker=_dig(lrow, "identity.ticker") or _dig(srow, "identity.ticker"),
                    side=_dig(lrow, "identity.side") or _dig(srow, "identity.side"),
                    created_at_ct=lrow.get("created_at_ct") or srow.get("created_at_ct"),
                ))
    stats = {
        "shared_entry_packets": len(shared),
        "live_only_entry_packets": len(live_only),
        "step2_only_entry_packets": len(step2_only),
        "field_mismatch_counts": dict(sorted(field_counts.items())),
    }
    return issues, stats


def _lifecycle_rows(day: str) -> list[dict[str, Any]]:
    path = os.path.join(HERE, "postmortem", "execution_lifecycle", day, f"execution_lifecycle_{day}.jsonl")
    rows = _read_jsonl(path)
    if rows:
        return rows
    # Rebuild from the legacy audit stream if the normalized file is absent.
    try:
        execution_lifecycle.write_snapshot(day)
    except Exception:
        pass
    return _read_jsonl(path)


def _lifecycle_issues(rows: list[dict[str, Any]], summary: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for anomaly in summary.get("anomalies") or []:
        issues.append(_issue(
            "execution_lifecycle_anomaly",
            "critical",
            f"Execution lifecycle anomaly: {anomaly.get('kind') or 'unknown'}",
            anomaly=anomaly,
        ))
    by_trade: dict[str, list[dict[str, Any]]] = defaultdict(list)
    orphan_count = 0
    for row in rows:
        trade_id = str(row.get("trade_id") or "")
        if not trade_id:
            orphan_count += 1
            continue
        by_trade[trade_id].append(row)
    missing_counts: Counter[str] = Counter()
    for trade_id, trade_rows in by_trade.items():
        events = {str(row.get("event") or "") for row in trade_rows}
        stages = {str(row.get("stage") or "") for row in trade_rows}
        if "entry_committed" in events:
            for event in LIVE_ENTRY_REQUIRED_EVENTS:
                if event not in events:
                    missing_counts[event] += 1
                    issues.append(_issue(
                        "lifecycle_missing_entry_event",
                        "critical",
                        f"Live committed an entry without {event}",
                        trade_id=trade_id,
                        missing_event=event,
                    ))
        if "position_closed" in events or "closed" in stages:
            for event in LIVE_CLOSED_REQUIRED_EVENTS:
                if event not in events:
                    missing_counts[event] += 1
                    issues.append(_issue(
                        "lifecycle_missing_exit_event",
                        "critical",
                        f"Live closed a position without {event}",
                        trade_id=trade_id,
                        missing_event=event,
                    ))
    stats = {
        "lifecycle_rows": len(rows),
        "lifecycle_trade_count": len(by_trade),
        "lifecycle_orphan_events": orphan_count,
        "lifecycle_open_trade_count": summary.get("open_trade_count"),
        "lifecycle_missing_event_counts": dict(sorted(missing_counts.items())),
        "lifecycle_stage_counts": summary.get("stage_counts") or {},
    }
    return issues, stats


def _schema_issues(schema: dict[str, Any]) -> list[dict[str, Any]]:
    if not schema:
        return [_issue("schema_compatibility_missing", "critical", "Canonical packet schema compatibility did not run")]
    if schema.get("ok"):
        return []
    return [_issue(
        "schema_compatibility_failed",
        "critical",
        "Canonical packet schema compatibility failed",
        errors=schema.get("errors") or [],
        counts=schema.get("counts") or {},
    )]


def _verdict(issues: list[dict[str, Any]], stats: dict[str, Any]) -> tuple[str, bool]:
    critical = [issue for issue in issues if issue.get("severity") == "critical"]
    warnings = [issue for issue in issues if issue.get("severity") == "warning"]
    if not stats.get("live_entry_packets") and not stats.get("step2_entry_packets"):
        return "PARITY_INCOMPLETE", False
    if critical:
        return "PARITY_FAIL", False
    if warnings:
        return "PARITY_WARN", True
    return "PARITY_OK", True


def verdict_from_components(day: str,
                            live_packets: list[dict[str, Any]],
                            step2_packets: list[dict[str, Any]],
                            live_exit_packets: list[dict[str, Any]] | None = None,
                            lifecycle_rows: list[dict[str, Any]] | None = None,
                            lifecycle_summary: dict[str, Any] | None = None,
                            schema_compatibility: dict[str, Any] | None = None,
                            canonical_diff: dict[str, Any] | None = None) -> dict[str, Any]:
    live_map = _entry_map(live_packets)
    step2_map = _entry_map(step2_packets)
    issues: list[dict[str, Any]] = []
    packet_issues, packet_stats = _compare_entry_packets(live_map, step2_map)
    issues.extend(packet_issues)
    issues.extend(_schema_issues(schema_compatibility or {}))
    lifecycle_rows = lifecycle_rows or []
    lifecycle_summary = lifecycle_summary or {}
    lifecycle_issues, lifecycle_stats = _lifecycle_issues(lifecycle_rows, lifecycle_summary)
    issues.extend(lifecycle_issues)
    stats = {
        "live_entry_packets": len(live_map),
        "step2_entry_packets": len(step2_map),
        "live_exit_packets": len(live_exit_packets or []),
        **packet_stats,
        **lifecycle_stats,
        "canonical_mismatch_count": (canonical_diff or {}).get("mismatch_count"),
    }
    severity_counts = Counter(str(issue.get("severity") or "unknown") for issue in issues)
    issue_kind_counts = Counter(str(issue.get("kind") or "unknown") for issue in issues)
    verdict, promotion_safe = _verdict(issues, stats)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "source": "parity_verdict_engine",
        "day": day,
        "created_at_ct": datetime.now(CT).isoformat(timespec="seconds"),
        "verdict": verdict,
        "ok": verdict in ("PARITY_OK", "PARITY_WARN"),
        "promotion_safe": promotion_safe,
        "critical_count": int(severity_counts.get("critical", 0)),
        "warning_count": int(severity_counts.get("warning", 0)),
        "issue_count": len(issues),
        "issue_kind_counts": dict(sorted(issue_kind_counts.items())),
        "stats": stats,
        "issues": issues[:250],
        "canonical_diff_summary": {
            "ok": (canonical_diff or {}).get("ok"),
            "path": (canonical_diff or {}).get("path"),
            "mismatch_count": (canonical_diff or {}).get("mismatch_count"),
            "live_only_entry_packets": (canonical_diff or {}).get("live_only_entry_packets"),
            "step2_only_entry_packets": (canonical_diff or {}).get("step2_only_entry_packets"),
            "schema_compatibility": (canonical_diff or {}).get("schema_compatibility"),
        },
        "deduction": (
            "Promotion-safe means Live and Step 2 shared entry decisions, hashes, brackets, "
            "execution intents, and Live lifecycle completeness. Broker fills/latency are kept "
            "as result evidence, not semantic intent mismatches."
        ),
    }
    return payload


def _render_text(payload: dict[str, Any]) -> str:
    stats = payload.get("stats") or {}
    lines = [
        f"Parity Verdict: {payload.get('verdict')}",
        f"Day: {payload.get('day')}",
        f"Promotion safe: {payload.get('promotion_safe')}",
        "",
        "Counts:",
        f"- Live entries: {stats.get('live_entry_packets')}",
        f"- Step 2 entries: {stats.get('step2_entry_packets')}",
        f"- Shared entries: {stats.get('shared_entry_packets')}",
        f"- Live-only entries: {stats.get('live_only_entry_packets')}",
        f"- Step2-only entries: {stats.get('step2_only_entry_packets')}",
        f"- Field mismatches: {sum((stats.get('field_mismatch_counts') or {}).values())}",
        f"- Lifecycle trades: {stats.get('lifecycle_trade_count')}",
        f"- Lifecycle rows: {stats.get('lifecycle_rows')}",
        "",
        "Issue Counts:",
    ]
    for key, value in (payload.get("issue_kind_counts") or {}).items():
        lines.append(f"- {key}: {value}")
    if not payload.get("issue_kind_counts"):
        lines.append("- none")
    lines.append("")
    lines.append("Top Issues:")
    for issue in (payload.get("issues") or [])[:25]:
        prefix = f"- [{issue.get('severity')}] {issue.get('kind')}: {issue.get('message')}"
        detail = []
        for key in ("join_key", "trade_id", "field", "ticker", "side"):
            if issue.get(key) is not None:
                detail.append(f"{key}={issue.get(key)}")
        lines.append(prefix + (f" ({', '.join(detail)})" if detail else ""))
    if not payload.get("issues"):
        lines.append("- none")
    lines.append("")
    lines.append(str(payload.get("deduction") or ""))
    return "\n".join(lines) + "\n"


def build(day: str, write: bool = True, rebuild_packets: bool = True) -> dict[str, Any]:
    canonical_diff: dict[str, Any] = {}
    if rebuild_packets:
        try:
            canonical_diff = canonical_decision_packet.diff_day(day)
        except Exception as exc:
            canonical_diff = {
                "ok": False,
                "error": repr(exc),
                "schema_compatibility": {"ok": False, "errors": [{"error": repr(exc)}]},
            }
    else:
        canonical_diff = _read_json(canonical_decision_packet.diff_path(day), {}) or {}
    live_packets = _read_jsonl(canonical_decision_packet.packet_path(day, "live_decision_packets"))
    step2_packets = _read_jsonl(canonical_decision_packet.packet_path(day, "step2_decision_packets"))
    live_exit_packets = _read_jsonl(canonical_decision_packet.packet_path(day, "live_exit_packets"))
    lifecycle_summary_path = os.path.join(
        HERE, "postmortem", "execution_lifecycle", day, f"execution_lifecycle_{day}.summary.json"
    )
    lifecycle_summary = _read_json(lifecycle_summary_path, {}) or {}
    lifecycle_rows = _lifecycle_rows(day)
    if not lifecycle_summary and lifecycle_rows:
        try:
            lifecycle_summary = execution_lifecycle.write_snapshot(day)
        except Exception:
            lifecycle_summary = {}
    payload = verdict_from_components(
        day,
        live_packets,
        step2_packets,
        live_exit_packets=live_exit_packets,
        lifecycle_rows=lifecycle_rows,
        lifecycle_summary=lifecycle_summary,
        schema_compatibility=(canonical_diff.get("schema_compatibility") if isinstance(canonical_diff, dict) else {}),
        canonical_diff=canonical_diff,
    )
    if write:
        payload["path"] = _write_json(verdict_path(day), payload)
        payload["text_path"] = _write_text(text_path(day), _render_text(payload))
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description="Build the Live/Step 2 parity verdict.")
    ap.add_argument("day")
    ap.add_argument("--no-write", action="store_true")
    ap.add_argument("--no-rebuild-packets", action="store_true")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--enforce", action="store_true", help="Exit nonzero unless the verdict is promotion-safe.")
    args = ap.parse_args()
    payload = build(args.day, write=not args.no_write, rebuild_packets=not args.no_rebuild_packets)
    print(json.dumps(payload if args.json else {
        "day": payload.get("day"),
        "verdict": payload.get("verdict"),
        "promotion_safe": payload.get("promotion_safe"),
        "critical_count": payload.get("critical_count"),
        "warning_count": payload.get("warning_count"),
        "path": payload.get("path"),
        "text_path": payload.get("text_path"),
    }, indent=2, sort_keys=True, default=str))
    if args.enforce and not payload.get("promotion_safe"):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
