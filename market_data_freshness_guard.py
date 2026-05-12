"""Freshness guardrails for intraday/canonical Step 2 market data.

``market_data_integrity_gate`` answers whether a selected tape is usable.
This guard adds operational freshness checks: tape age, intraday/canonical
compare gaps, duplicate event timestamps, and whether the incremental store
manifest exists for the day.
"""
from __future__ import annotations

from output_paths import output_path

import argparse
import gzip
import hashlib
import json
import os
import time
from collections import Counter
from datetime import date, datetime, time as dt_time
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import market_data_integrity_gate


HERE = os.path.dirname(os.path.abspath(__file__))
POSTMORTEM_DIR = output_path("postmortem")
OUT_DIR = os.path.join(POSTMORTEM_DIR, "market_data_freshness")
CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1
SESSION_START = dt_time(8, 30)
SESSION_END = dt_time(15, 0, 59)


def _now_ct() -> datetime:
    return datetime.now(CT)


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


def output_paths(day: str) -> dict[str, str]:
    return {
        "json": os.path.join(OUT_DIR, f"market_data_freshness_{day}.json"),
        "txt": os.path.join(OUT_DIR, f"market_data_freshness_{day}.txt"),
    }


def _compare_path(day: str) -> str:
    return os.path.join(POSTMORTEM_DIR, "step2_freshness", f"intraday_vs_canonical_{day}.json")


def _incremental_manifest_path(day: str) -> str:
    return output_path("data_cache", "incremental_market_store", day, "manifest.json")


def _file_meta(path: str) -> dict[str, Any]:
    exists = os.path.exists(path)
    now = time.time()
    mtime = os.path.getmtime(path) if exists else None
    return {
        "path": os.path.abspath(path),
        "exists": exists,
        "bytes": os.path.getsize(path) if exists else 0,
        "mtime": mtime,
        "age_sec": round(now - mtime, 3) if mtime is not None else None,
    }


def _is_intraday(day: str, now: datetime | None = None) -> bool:
    now = now or _now_ct()
    try:
        d = date.fromisoformat(day)
    except Exception:
        return False
    start = datetime.combine(d, SESSION_START, CT)
    end = datetime.combine(d, SESSION_END, CT)
    return start <= now <= end


def _event_key(row: dict[str, Any]) -> str:
    # Multiple SIP trades can legitimately share the same symbol/kind/timestamp.
    # Treat only exact normalized event duplication as a data-quality warning.
    return json.dumps(row, sort_keys=True, separators=(",", ":"), default=str)


def _duplicate_stats(path: str, limit_rows: int = 0) -> dict[str, Any]:
    if not os.path.exists(path):
        return {"checked": False, "duplicate_event_keys": 0, "top_duplicates": []}
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            rows = json.load(f)
    except Exception as exc:
        return {"checked": False, "error": repr(exc), "duplicate_event_keys": 0, "top_duplicates": []}
    if not isinstance(rows, list):
        return {"checked": False, "duplicate_event_keys": 0, "top_duplicates": []}
    if limit_rows and len(rows) > limit_rows:
        rows = rows[-limit_rows:]
    counts = Counter(_event_key(row) for row in rows)
    dups = [(key, count) for key, count in counts.items() if count > 1]
    return {
        "checked": True,
        "rows_checked": len(rows),
        "duplicate_event_keys": len(dups),
        "duplicate_rows_overage": sum(count - 1 for _, count in dups),
        "top_duplicates": [
            {"event_hash": hashlib.sha256(key.encode("utf-8")).hexdigest()[:24], "count": count}
            for key, count in sorted(dups, key=lambda item: (-item[1], str(item[0])))[:20]
        ],
    }


def _issue(level: str, kind: str, detail: dict[str, Any]) -> dict[str, Any]:
    return {"level": level, "kind": kind, "detail": detail}


def evaluate_components(day: str, *,
                        integrity: dict[str, Any],
                        compare: dict[str, Any],
                        selected_tape_meta: dict[str, Any],
                        incremental_manifest: dict[str, Any],
                        duplicate_stats: dict[str, Any],
                        intraday: bool,
                        source_used: str = "intraday",
                        max_intraday_tape_age_sec: int = 300) -> dict[str, Any]:
    issues: list[dict[str, Any]] = []
    if not selected_tape_meta.get("exists"):
        issues.append(_issue("critical", "selected_tape_missing", {"path": selected_tape_meta.get("path")}))
    if not integrity.get("ok"):
        issues.append(_issue("critical", "market_data_integrity_not_ok", {
            "verdict": integrity.get("verdict"),
            "critical_count": integrity.get("critical_count"),
        }))
    elif integrity.get("verdict") == "DATA_INCOMPLETE":
        issues.append(_issue("warning", "market_session_incomplete", {"verdict": integrity.get("verdict")}))
    missing = int(compare.get("missing_from_intraday_rows") or 0) if compare else 0
    live_only = int(compare.get("live_only_rows") or 0) if compare else 0
    if missing:
        level = "warning" if intraday or source_used == "canonical" else "critical"
        issues.append(_issue(level, "intraday_missing_canonical_rows", {
            "rows": missing,
            "source_used": source_used,
        }))
    if live_only:
        issues.append(_issue("warning", "intraday_live_only_rows", {"rows": live_only}))
    if not incremental_manifest:
        issues.append(_issue("warning", "incremental_market_store_manifest_missing", {"day": day}))
    if intraday:
        age = selected_tape_meta.get("age_sec")
        if age is None:
            issues.append(_issue("warning", "intraday_tape_age_unknown", {"path": selected_tape_meta.get("path")}))
        elif float(age) > max_intraday_tape_age_sec:
            issues.append(_issue("warning", "intraday_tape_stale", {
                "age_sec": age,
                "threshold_sec": max_intraday_tape_age_sec,
            }))
    duplicate_count = int(duplicate_stats.get("duplicate_event_keys") or 0)
    if duplicate_count:
        issues.append(_issue("warning", "duplicate_event_keys", {
            "duplicate_event_keys": duplicate_count,
            "duplicate_rows_overage": duplicate_stats.get("duplicate_rows_overage"),
            "examples": duplicate_stats.get("top_duplicates", [])[:5],
        }))
    critical = [row for row in issues if row.get("level") == "critical"]
    warnings = [row for row in issues if row.get("level") == "warning"]
    return {
        "ok": not critical,
        "verdict": "FRESHNESS_FAIL" if critical else ("FRESHNESS_WARN" if warnings else "FRESHNESS_OK"),
        "critical_count": len(critical),
        "warning_count": len(warnings),
        "issue_kind_counts": dict(sorted(Counter(str(row.get("kind")) for row in issues).items())),
        "issues": issues,
    }


def build(day: str, tickers: list[str] | None = None, source: str = "auto",
          write: bool = True, use_cached_integrity: bool = True,
          max_intraday_tape_age_sec: int = 300) -> dict[str, Any]:
    tickers = [str(t).upper() for t in (tickers or list(market_data_integrity_gate.DEFAULT_TICKERS))]
    integrity = market_data_integrity_gate.build(
        day,
        tickers=tickers,
        source=source,
        write=True,
        use_cached=use_cached_integrity,
    )
    selected_path = ((integrity.get("source_tape") or {}).get("path")
                     or market_data_integrity_gate.prepared_path(day, tickers, source=integrity.get("source_used") or "canonical"))
    compare = _read_json(_compare_path(day), {}) or {}
    incremental_manifest = _read_json(_incremental_manifest_path(day), {}) or {}
    selected_meta = _file_meta(selected_path)
    duplicate = _duplicate_stats(selected_path, limit_rows=0)
    intraday = _is_intraday(day)
    evaluated = evaluate_components(
        day,
        integrity=integrity,
        compare=compare,
        selected_tape_meta=selected_meta,
        incremental_manifest=incremental_manifest,
        duplicate_stats=duplicate,
        intraday=intraday,
        source_used=str(integrity.get("source_used") or source),
        max_intraday_tape_age_sec=max_intraday_tape_age_sec,
    )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "source": "market_data_freshness_guard",
        "day": day,
        "tickers": tickers,
        "created_at_ct": _now_ct().isoformat(timespec="seconds"),
        "source_requested": source,
        "source_used": integrity.get("source_used"),
        "intraday_session_active": intraday,
        "selected_tape": selected_meta,
        "canonical_tape": integrity.get("canonical_tape"),
        "intraday_tape": integrity.get("intraday_tape"),
        "incremental_market_store": incremental_manifest,
        "intraday_vs_canonical_compare": compare,
        "duplicate_stats": duplicate,
        "market_data_integrity": {
            "ok": integrity.get("ok"),
            "verdict": integrity.get("verdict"),
            "promotion_safe": integrity.get("promotion_safe"),
            "critical_count": integrity.get("critical_count"),
            "warning_count": integrity.get("warning_count"),
            "path": integrity.get("path"),
        },
        **evaluated,
        "deduction": (
            "This report is the freshness alarm for Step 2 parity. It is allowed to warn intraday, "
            "but closed-day criticals mean Step 2 rankings should not be trusted for promotion."
        ),
    }
    if write:
        p = output_paths(day)
        payload["path"] = _write_json(p["json"], payload)
        payload["text_path"] = _write_text(p["txt"], render_text(payload))
    return payload


def render_text(payload: dict[str, Any]) -> str:
    tape = payload.get("selected_tape") or {}
    lines = [
        f"Market Data Freshness Guard - {payload.get('day')}",
        f"Created CT: {payload.get('created_at_ct')}",
        "",
        "Verdict",
        f"- Verdict: {payload.get('verdict')}",
        f"- OK: {payload.get('ok')}",
        f"- Critical/warnings: {payload.get('critical_count')}/{payload.get('warning_count')}",
        f"- Source used: {payload.get('source_used')}",
        f"- Intraday active: {payload.get('intraday_session_active')}",
        "",
        "Selected tape",
        f"- Path: {tape.get('path')}",
        f"- Exists/bytes/age_sec: {tape.get('exists')}/{tape.get('bytes')}/{tape.get('age_sec')}",
        "",
        "Issues",
    ]
    for issue in (payload.get("issues") or [])[:50]:
        lines.append(f"- {issue.get('level')} {issue.get('kind')}: {json.dumps(issue.get('detail') or {}, sort_keys=True)}")
    if not payload.get("issues"):
        lines.append("- none")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="Build market-data freshness guardrails.")
    ap.add_argument("day")
    ap.add_argument("--tickers", nargs="+", default=list(market_data_integrity_gate.DEFAULT_TICKERS))
    ap.add_argument("--source", choices=("auto", "canonical", "intraday"), default="auto")
    ap.add_argument("--refresh-integrity", action="store_true")
    ap.add_argument("--max-intraday-tape-age-sec", type=int, default=300)
    ap.add_argument("--no-write", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    payload = build(
        args.day,
        tickers=args.tickers,
        source=args.source,
        write=not args.no_write,
        use_cached_integrity=not args.refresh_integrity,
        max_intraday_tape_age_sec=args.max_intraday_tape_age_sec,
    )
    print(json.dumps(payload, indent=2, sort_keys=True, default=str) if args.json else render_text(payload))
    return 0 if payload.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
