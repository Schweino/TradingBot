"""Market-data integrity gate for Step 2 and Live parity evidence.

The goal is deliberately narrow: before we trust a Step 2 score, promotion
packet, or parity report, prove that the tape underneath it was complete enough
to replay. This does not fetch data. It inspects the prepared replay tapes and
their companion manifests/comparison artifacts.
"""
from __future__ import annotations

from output_paths import output_path

import argparse
import gzip
import hashlib
import json
import os
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore


HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = output_path("postmortem", "market_data_integrity")
CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1
DEFAULT_TICKERS = ("CLSK", "MARA", "RIOT")
BTC_SYMBOL = "BTC/USD"
SESSION_START = time(8, 30)
SESSION_END = time(15, 0, 59)


def prepared_path(day: str, tickers: list[str] | tuple[str, ...] = DEFAULT_TICKERS,
                  feed: str = "sip", quote_mode: str = "per-second",
                  btc_mode: str = "bars", source: str = "canonical") -> str:
    tickers_part = "-".join(str(t).upper() for t in tickers)
    filename = f"{feed}_{quote_mode}_{btc_mode}_{tickers_part}_{day}.events.json.gz"
    folder = "alpaca_engine_replay_tapes" if source == "canonical" else "live_intraday_tapes"
    return output_path("data_cache", folder, filename)


def output_paths(day: str) -> dict[str, str]:
    return {
        "json": os.path.join(OUT_DIR, f"market_data_integrity_{day}.json"),
        "txt": os.path.join(OUT_DIR, f"market_data_integrity_{day}.txt"),
    }


def _manifest_path(path: str) -> str:
    if path.endswith(".events.json.gz"):
        return path[:-len(".events.json.gz")] + ".manifest.json"
    return path + ".manifest.json"


def _compare_path(day: str) -> str:
    return output_path("postmortem", "step2_freshness", f"intraday_vs_canonical_{day}.json")


def _incremental_manifest_path(day: str) -> str:
    return output_path("data_cache", "incremental_market_store", day, "manifest.json")


def _read_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except Exception:
        return default


def _write_json(path: str, payload: dict[str, Any]) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
    os.replace(tmp, path)
    return os.path.abspath(path)


def _write_text(path: str, text: str) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)
    return os.path.abspath(path)


def _file_sha256(path: str) -> str | None:
    if not os.path.exists(path):
        return None
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _file_meta(path: str, include_sha: bool = False) -> dict[str, Any]:
    exists = os.path.exists(path)
    return {
        "path": os.path.abspath(path),
        "exists": exists,
        "bytes": os.path.getsize(path) if exists else 0,
        "mtime": os.path.getmtime(path) if exists else None,
        "sha256": _file_sha256(path) if include_sha and exists else None,
    }


def _cached_payload(day: str, source_path: str) -> dict[str, Any] | None:
    path = output_paths(day)["json"]
    payload = _read_json(path, {}) or {}
    if not payload:
        return None
    source = payload.get("source_tape") or {}
    try:
        if (
            os.path.abspath(str(source.get("path") or "")) == os.path.abspath(source_path)
            and int(source.get("bytes") or -1) == int(os.path.getsize(source_path))
            and abs(float(source.get("mtime") or 0.0) - float(os.path.getmtime(source_path))) < 0.001
        ):
            payload["cache_hit"] = True
            return payload
    except Exception:
        return None
    return None


def _read_events(path: str) -> list[dict[str, Any]]:
    if not os.path.exists(path):
        return []
    with gzip.open(path, "rt", encoding="utf-8") as f:
        payload = json.load(f)
    return payload if isinstance(payload, list) else []


def _ts_ms(event: dict[str, Any]) -> int:
    raw = event.get("t")
    if raw is None and isinstance(event.get("row"), dict):
        raw = event["row"].get("t")
    try:
        ts = int(float(raw))
    except Exception:
        return 0
    if 0 < ts < 100_000_000_000:
        ts *= 1000
    return ts


def _ct(ts_ms: int) -> str | None:
    if not ts_ms:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, CT).isoformat(timespec="seconds")


def _session_bounds(day: str) -> tuple[int, int]:
    d = date.fromisoformat(day)
    start = datetime.combine(d, SESSION_START, CT)
    end = datetime.combine(d, SESSION_END, CT)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def _complete_expected(day: str, source_used: str) -> bool:
    if source_used == "canonical":
        return True
    now = datetime.now(CT)
    try:
        d = date.fromisoformat(day)
    except Exception:
        return True
    if d < now.date():
        return True
    if d > now.date():
        return False
    end = datetime.combine(d, SESSION_END, CT) + timedelta(minutes=10)
    return now >= end


def _summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_kind: dict[str, int] = defaultdict(int)
    by_symbol: dict[str, int] = defaultdict(int)
    by_symbol_kind: dict[str, int] = defaultdict(int)
    min_ts = 0
    max_ts = 0
    for row in rows:
        kind = str(row.get("kind") or "unknown")
        symbol = str(row.get("symbol") or "unknown")
        ts = _ts_ms(row)
        by_kind[kind] += 1
        by_symbol[symbol] += 1
        by_symbol_kind[f"{symbol}:{kind}"] += 1
        if ts:
            min_ts = ts if not min_ts else min(min_ts, ts)
            max_ts = max(max_ts, ts)
    return {
        "rows": len(rows),
        "by_kind": dict(sorted(by_kind.items())),
        "by_symbol": dict(sorted(by_symbol.items())),
        "by_symbol_kind": dict(sorted(by_symbol_kind.items())),
        "min_ts_ms": min_ts,
        "max_ts_ms": max_ts,
        "min_ct": _ct(min_ts),
        "max_ct": _ct(max_ts),
    }


def _series_stats(rows: list[dict[str, Any]], symbol: str, kind: str) -> dict[str, Any]:
    ts_values = sorted(_ts_ms(row) for row in rows if row.get("symbol") == symbol and row.get("kind") == kind and _ts_ms(row))
    if not ts_values:
        return {
            "symbol": symbol,
            "kind": kind,
            "count": 0,
            "first_ts_ms": 0,
            "last_ts_ms": 0,
            "first_ct": None,
            "last_ct": None,
            "max_gap_sec": None,
            "gap_over_120s": 0,
            "gap_over_300s": 0,
            "gap_over_600s": 0,
            "gap_over_900s": 0,
        }
    gaps = [(b - a) / 1000.0 for a, b in zip(ts_values, ts_values[1:])]
    return {
        "symbol": symbol,
        "kind": kind,
        "count": len(ts_values),
        "first_ts_ms": ts_values[0],
        "last_ts_ms": ts_values[-1],
        "first_ct": _ct(ts_values[0]),
        "last_ct": _ct(ts_values[-1]),
        "max_gap_sec": round(max(gaps), 3) if gaps else 0.0,
        "gap_over_120s": sum(1 for g in gaps if g > 120),
        "gap_over_300s": sum(1 for g in gaps if g > 300),
        "gap_over_600s": sum(1 for g in gaps if g > 600),
        "gap_over_900s": sum(1 for g in gaps if g > 900),
    }


def _issue(level: str, kind: str, detail: dict[str, Any]) -> dict[str, Any]:
    return {"level": level, "kind": kind, "detail": detail}


def _validate_series(day: str, rows: list[dict[str, Any]], tickers: list[str],
                     expected_complete: bool) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    per_series: dict[str, Any] = {}
    start_ms, end_ms = _session_bounds(day)
    for ticker in tickers:
        for kind in ("stock_trade", "stock_quote"):
            stats = _series_stats(rows, ticker, kind)
            per_series[f"{ticker}:{kind}"] = stats
            if stats["count"] <= 0:
                issues.append(_issue("critical" if expected_complete else "warning", "missing_market_series", {
                    "symbol": ticker,
                    "kind": kind,
                }))
                continue
            if expected_complete and stats["first_ts_ms"] > start_ms + 5 * 60 * 1000:
                issues.append(_issue("critical", "late_market_series_start", {
                    "symbol": ticker,
                    "kind": kind,
                    "first_ct": stats["first_ct"],
                }))
            if expected_complete and stats["last_ts_ms"] < end_ms - 2 * 60 * 1000:
                issues.append(_issue("critical", "early_market_series_end", {
                    "symbol": ticker,
                    "kind": kind,
                    "last_ct": stats["last_ct"],
                }))
            gap = float(stats["max_gap_sec"] or 0.0)
            fail_gap = 900.0 if kind == "stock_trade" else 600.0
            warn_gap = 300.0 if kind == "stock_trade" else 120.0
            if expected_complete and gap > fail_gap:
                issues.append(_issue("critical", "large_market_data_gap", {
                    "symbol": ticker,
                    "kind": kind,
                    "max_gap_sec": gap,
                    "threshold_sec": fail_gap,
                }))
            elif gap > warn_gap:
                issues.append(_issue("warning", "market_data_gap_warning", {
                    "symbol": ticker,
                    "kind": kind,
                    "max_gap_sec": gap,
                    "threshold_sec": warn_gap,
                }))
    btc_kinds = sorted({str(row.get("kind") or "") for row in rows if row.get("symbol") == BTC_SYMBOL and str(row.get("kind") or "").startswith("btc")})
    if not btc_kinds:
        issues.append(_issue("critical" if expected_complete else "warning", "missing_btc_series", {"symbol": BTC_SYMBOL}))
    for kind in btc_kinds or ["btc_synth_trade"]:
        stats = _series_stats(rows, BTC_SYMBOL, kind)
        per_series[f"{BTC_SYMBOL}:{kind}"] = stats
        if stats["count"] <= 0:
            continue
        if expected_complete and stats["first_ts_ms"] > start_ms + 5 * 60 * 1000:
            issues.append(_issue("critical", "late_btc_series_start", {"kind": kind, "first_ct": stats["first_ct"]}))
        if expected_complete and stats["last_ts_ms"] < end_ms - 2 * 60 * 1000:
            issues.append(_issue("critical", "early_btc_series_end", {"kind": kind, "last_ct": stats["last_ct"]}))
        gap = float(stats["max_gap_sec"] or 0.0)
        if expected_complete and gap > 300.0:
            issues.append(_issue("critical", "large_btc_data_gap", {
                "kind": kind,
                "max_gap_sec": gap,
                "threshold_sec": 300.0,
            }))
        elif gap > 120.0:
            issues.append(_issue("warning", "btc_data_gap_warning", {
                "kind": kind,
                "max_gap_sec": gap,
                "threshold_sec": 120.0,
            }))
    return issues, per_series


def _compare_issues(compare: dict[str, Any], source_used: str, expected_complete: bool) -> list[dict[str, Any]]:
    if not compare:
        return []
    issues: list[dict[str, Any]] = []
    if expected_complete and compare.get("canonical_exists") is False:
        issues.append(_issue("critical", "canonical_compare_tape_missing", {
            "canonical_path": compare.get("canonical_path"),
        }))
    missing = int(compare.get("missing_from_intraday_rows") or 0)
    live_only = int(compare.get("live_only_rows") or 0)
    if missing > 0:
        level = "critical" if source_used == "intraday" and expected_complete else "warning"
        issues.append(_issue(level, "intraday_missing_canonical_rows", {
            "rows": missing,
            "source_used": source_used,
            "compare_path": compare.get("compare_path") or _compare_path(str(compare.get("day") or "")),
        }))
    if live_only > 0:
        issues.append(_issue("warning", "intraday_live_only_rows", {
            "rows": live_only,
            "source_used": source_used,
        }))
    return issues


def evaluate_events(day: str, rows: list[dict[str, Any]], tickers: list[str] | None = None,
                    source_used: str = "canonical", source_exists: bool = True,
                    compare: dict[str, Any] | None = None) -> dict[str, Any]:
    tickers = [str(t).upper() for t in (tickers or list(DEFAULT_TICKERS))]
    expected_complete = _complete_expected(day, source_used)
    summary = _summarize(rows)
    issues: list[dict[str, Any]] = []
    if not source_exists:
        issues.append(_issue("critical", "selected_market_tape_missing", {"source_used": source_used}))
    if not expected_complete:
        issues.append(_issue("info", "session_not_complete_yet", {"day": day, "source_used": source_used}))
    if source_exists and summary["rows"] <= 0:
        issues.append(_issue("critical" if expected_complete else "warning", "selected_market_tape_empty", {
            "source_used": source_used,
        }))
    if source_exists and rows:
        start_ms, end_ms = _session_bounds(day)
        if expected_complete and int(summary["min_ts_ms"] or 0) > start_ms + 5 * 60 * 1000:
            issues.append(_issue("critical", "late_tape_start", {"min_ct": summary.get("min_ct")}))
        if expected_complete and int(summary["max_ts_ms"] or 0) < end_ms - 2 * 60 * 1000:
            issues.append(_issue("critical", "early_tape_end", {"max_ct": summary.get("max_ct")}))
        series_issues, per_series = _validate_series(day, rows, tickers, expected_complete)
        issues.extend(series_issues)
    else:
        per_series = {}
    issues.extend(_compare_issues(compare or {}, source_used, expected_complete))
    critical = [row for row in issues if row.get("level") == "critical"]
    warnings = [row for row in issues if row.get("level") == "warning"]
    if critical:
        verdict = "DATA_FAIL"
    elif not expected_complete:
        verdict = "DATA_INCOMPLETE"
    elif warnings:
        verdict = "DATA_WARN"
    else:
        verdict = "DATA_OK"
    return {
        "expected_complete": expected_complete,
        "ok": verdict != "DATA_FAIL",
        "promotion_safe": verdict in ("DATA_OK", "DATA_WARN"),
        "verdict": verdict,
        "critical_count": len(critical),
        "warning_count": len(warnings),
        "issue_kind_counts": {
            kind: sum(1 for row in issues if row.get("kind") == kind)
            for kind in sorted({str(row.get("kind")) for row in issues})
        },
        "issues": issues,
        "summary": summary,
        "per_series": per_series,
    }


def build(day: str, tickers: list[str] | None = None, feed: str = "sip",
          quote_mode: str = "per-second", btc_mode: str = "bars",
          source: str = "auto", write: bool = True,
          use_cached: bool = True) -> dict[str, Any]:
    tickers = [str(t).upper() for t in (tickers or list(DEFAULT_TICKERS))]
    canonical = prepared_path(day, tickers, feed, quote_mode, btc_mode, "canonical")
    intraday = prepared_path(day, tickers, feed, quote_mode, btc_mode, "intraday")
    if source == "canonical":
        source_used = "canonical"
        source_path = canonical
    elif source == "intraday":
        source_used = "intraday"
        source_path = intraday
    else:
        source_used = "canonical" if os.path.exists(canonical) else "intraday"
        source_path = canonical if source_used == "canonical" else intraday
    if use_cached and os.path.exists(source_path):
        cached = _cached_payload(day, source_path)
        if cached:
            return cached
    compare = _read_json(_compare_path(day), {}) or {}
    rows = _read_events(source_path) if os.path.exists(source_path) else []
    evaluated = evaluate_events(
        day,
        rows,
        tickers=tickers,
        source_used=source_used,
        source_exists=os.path.exists(source_path),
        compare=compare,
    )
    source_meta = _file_meta(source_path, include_sha=True)
    canonical_meta = _file_meta(canonical, include_sha=False)
    intraday_meta = _file_meta(intraday, include_sha=False)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "source": "market_data_integrity_gate",
        "day": day,
        "tickers": tickers,
        "feed": feed,
        "quote_mode": quote_mode,
        "btc_mode": btc_mode,
        "source_requested": source,
        "source_used": source_used,
        "created_at_ct": datetime.now(CT).isoformat(timespec="seconds"),
        "source_tape": source_meta,
        "canonical_tape": canonical_meta,
        "intraday_tape": intraday_meta,
        "source_manifest": _read_json(_manifest_path(source_path), {}) or {},
        "canonical_manifest": _read_json(_manifest_path(canonical), {}) or {},
        "intraday_manifest": _read_json(_manifest_path(intraday), {}) or {},
        "incremental_market_store": _read_json(_incremental_manifest_path(day), {}) or {},
        "intraday_vs_canonical_compare": compare,
        **evaluated,
        "deduction": (
            "Promotion-safe Step 2 evidence requires a complete selected replay tape with all miner "
            "tickers, BTC coverage, no critical session gaps, and recorded freshness/compare context."
        ),
    }
    if write:
        paths = output_paths(day)
        payload["path"] = _write_json(paths["json"], payload)
        payload["text_path"] = _write_text(paths["txt"], render_text(payload))
    return payload


def build_many(days: list[str], tickers: list[str] | None = None, source: str = "canonical",
               write: bool = True, use_cached: bool = True) -> dict[str, Any]:
    reports = {
        day: build(day, tickers=tickers, source=source, write=write, use_cached=use_cached)
        for day in days
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "market_data_integrity_gate_many",
        "days": days,
        "ok": all(bool(row.get("ok")) for row in reports.values()),
        "promotion_safe": all(bool(row.get("promotion_safe")) for row in reports.values()),
        "critical_count": sum(int(row.get("critical_count") or 0) for row in reports.values()),
        "warning_count": sum(int(row.get("warning_count") or 0) for row in reports.values()),
        "reports": reports,
    }


def render_text(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") or {}
    issues = payload.get("issues") or []
    lines = [
        f"Market Data Integrity - {payload.get('day')}",
        f"Created CT: {payload.get('created_at_ct')}",
        "",
        "Verdict",
        f"- Verdict: {payload.get('verdict')}",
        f"- OK: {payload.get('ok')}",
        f"- Promotion safe: {payload.get('promotion_safe')}",
        f"- Expected complete: {payload.get('expected_complete')}",
        f"- Critical/warnings: {payload.get('critical_count')}/{payload.get('warning_count')}",
        "",
        "Tape",
        f"- Source used: {payload.get('source_used')} requested={payload.get('source_requested')}",
        f"- Path: {(payload.get('source_tape') or {}).get('path')}",
        f"- Rows: {summary.get('rows')} min={summary.get('min_ct')} max={summary.get('max_ct')}",
        f"- By kind: {json.dumps(summary.get('by_kind') or {}, sort_keys=True)}",
        f"- By symbol: {json.dumps(summary.get('by_symbol') or {}, sort_keys=True)}",
        "",
        "Compare",
    ]
    compare = payload.get("intraday_vs_canonical_compare") or {}
    lines.extend([
        f"- Canonical exists: {compare.get('canonical_exists')}",
        f"- Intraday exists: {compare.get('intraday_exists')}",
        f"- Overlap rows: {compare.get('overlap_rows')}",
        f"- Missing from intraday: {compare.get('missing_from_intraday_rows')}",
        f"- Live-only rows: {compare.get('live_only_rows')}",
        "",
        "Issues",
    ])
    for issue in issues[:50]:
        lines.append(f"- {issue.get('level')} {issue.get('kind')}: {json.dumps(issue.get('detail') or {}, sort_keys=True)}")
    if not issues:
        lines.append("- none")
    lines.extend([
        "",
        "Use",
        "- If this is not promotion-safe, do not trust Step 2 rankings from this tape for promotion evidence.",
        "",
    ])
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="Build a market-data integrity report for a replay tape.")
    ap.add_argument("day")
    ap.add_argument("--tickers", nargs="+", default=list(DEFAULT_TICKERS))
    ap.add_argument("--feed", default="sip")
    ap.add_argument("--quote-mode", default="per-second")
    ap.add_argument("--btc-mode", default="bars")
    ap.add_argument("--source", choices=("auto", "canonical", "intraday"), default="auto")
    ap.add_argument("--no-write", action="store_true")
    ap.add_argument("--refresh", action="store_true", help="Ignore a valid cached integrity report.")
    ap.add_argument("--enforce", action="store_true", help="Exit non-zero unless the tape is promotion-safe.")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    payload = build(
        args.day,
        tickers=args.tickers,
        feed=args.feed,
        quote_mode=args.quote_mode,
        btc_mode=args.btc_mode,
        source=args.source,
        write=not args.no_write,
        use_cached=not args.refresh,
    )
    print(json.dumps(payload, indent=2, sort_keys=True, default=str) if args.json else render_text(payload))
    if args.enforce and not payload.get("promotion_safe"):
        return 2
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
