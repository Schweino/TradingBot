"""Intraday live market-data watchdog and self-heal helper."""
from __future__ import annotations

import argparse
import gzip
import json
import os
import time
from collections import Counter
from datetime import date, datetime, time as dt_time
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import live_step2_feed


HERE = Path(__file__).resolve().parent
CT = ZoneInfo("America/Chicago")
OUT_DIR = HERE / "postmortem" / "live_market_data_watchdog"
DEFAULT_TICKERS = ["CLSK", "MARA", "RIOT"]
SESSION_START_CT = dt_time(8, 30)
SESSION_END_CT = dt_time(15, 1)
SCHEMA_VERSION = 1


def _now_ct_dt() -> datetime:
    return datetime.now(CT)


def _now_ct() -> str:
    return _now_ct_dt().isoformat(timespec="seconds")


def _write_json(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.{int(time.time() * 1000)}.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
        f.write("\n")
    os.replace(tmp, path)
    return str(path.resolve())


def _read_events(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            payload = json.load(f)
        return payload if isinstance(payload, list) else []
    except Exception:
        return []


def _prepared_path(day: str, tickers: list[str], feed: str, quote_mode: str, btc_mode: str) -> Path:
    ticker_part = "-".join(tickers)
    return HERE / "data_cache" / "live_intraday_tapes" / f"{feed}_{quote_mode}_{btc_mode}_{ticker_part}_{day}.events.json.gz"


def _jsonl_path(day: str) -> Path:
    return HERE / "postmortem" / "live_step2_events" / f"live_step2_events_{day}.jsonl"


def _live_index_dir(day: str) -> Path:
    return HERE / "data_cache" / "incremental_market_store" / day / "live_index"


def _source_meta(day: str) -> dict[str, Any]:
    jsonl = _jsonl_path(day)
    index_dir = _live_index_dir(day)
    index_files = sorted(index_dir.glob("*.events.jsonl")) if index_dir.is_dir() else []
    mtimes = [p.stat().st_mtime for p in [jsonl, *index_files] if p.exists()]
    latest = max(mtimes, default=None)
    return {
        "jsonl_path": str(jsonl.resolve()),
        "jsonl_exists": jsonl.exists(),
        "jsonl_bytes": jsonl.stat().st_size if jsonl.exists() else 0,
        "live_index_dir": str(index_dir.resolve()),
        "live_index_file_count": len(index_files),
        "live_index_bytes": sum(p.stat().st_size for p in index_files),
        "latest_source_mtime": latest,
        "latest_source_age_sec": round(time.time() - latest, 3) if latest else None,
    }


def _is_session_active(day: str, now: datetime | None = None) -> bool:
    now = now or _now_ct_dt()
    try:
        d = date.fromisoformat(day)
    except Exception:
        return False
    if now.date() != d:
        return False
    return SESSION_START_CT <= now.time() < SESSION_END_CT


def _session_started(day: str, now: datetime | None = None, grace_min: int = 5) -> bool:
    now = now or _now_ct_dt()
    try:
        d = date.fromisoformat(day)
    except Exception:
        return False
    start = datetime.combine(d, SESSION_START_CT, CT)
    return now >= start and (now - start).total_seconds() >= grace_min * 60


def _summarize(path: Path) -> dict[str, Any]:
    rows = _read_events(path)
    by_kind = Counter(str(row.get("kind") or "unknown") for row in rows)
    by_symbol_kind = Counter(f"{row.get('symbol')}:{row.get('kind')}" for row in rows)
    mtime = path.stat().st_mtime if path.exists() else None
    return {
        "path": str(path.resolve()),
        "exists": path.exists(),
        "bytes": path.stat().st_size if path.exists() else 0,
        "mtime": mtime,
        "age_sec": round(time.time() - mtime, 3) if mtime else None,
        "rows": len(rows),
        "by_kind": dict(sorted(by_kind.items())),
        "by_symbol_kind": dict(sorted(by_symbol_kind.items())),
    }


def evaluate(
    day: str,
    *,
    tickers: list[str] | None = None,
    feed: str = "sip",
    quote_mode: str = "per-second",
    btc_mode: str = "bars",
    max_tape_age_sec: int = 180,
    min_stock_rows: int = 1,
    now: datetime | None = None,
) -> dict[str, Any]:
    tickers = [str(t).upper() for t in (tickers or DEFAULT_TICKERS)]
    active = _is_session_active(day, now)
    started = _session_started(day, now)
    tape = _summarize(_prepared_path(day, tickers, feed, quote_mode, btc_mode))
    source = _source_meta(day)
    stock_rows = sum(
        int(tape["by_symbol_kind"].get(f"{ticker}:stock_trade", 0))
        + int(tape["by_symbol_kind"].get(f"{ticker}:stock_quote", 0))
        for ticker in tickers
    )
    issues: list[dict[str, Any]] = []
    if active and started and not tape.get("exists"):
        issues.append({"level": "critical", "kind": "live_tape_missing"})
    if active and started and int(tape.get("rows") or 0) <= 0:
        issues.append({"level": "critical", "kind": "live_tape_empty"})
    if active and started and stock_rows < int(min_stock_rows):
        issues.append({"level": "critical", "kind": "live_stock_rows_missing", "stock_rows": stock_rows})
    age = tape.get("age_sec")
    if active and started and age is not None and float(age) > max_tape_age_sec:
        issues.append({"level": "warning", "kind": "live_tape_stale", "age_sec": age, "threshold_sec": max_tape_age_sec})
    source_age = source.get("latest_source_age_sec")
    if active and started and source_age is not None and float(source_age) > max_tape_age_sec:
        issues.append({"level": "warning", "kind": "live_source_stale", "age_sec": source_age, "threshold_sec": max_tape_age_sec})
    critical = [row for row in issues if row.get("level") == "critical"]
    warnings = [row for row in issues if row.get("level") == "warning"]
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "live_market_data_watchdog",
        "day": day,
        "tickers": tickers,
        "created_at_ct": _now_ct(),
        "session_active": active,
        "session_started": started,
        "ok": not critical,
        "verdict": "LIVE_DATA_FAIL" if critical else ("LIVE_DATA_WARN" if warnings else "LIVE_DATA_OK"),
        "critical_count": len(critical),
        "warning_count": len(warnings),
        "issues": issues,
        "stock_rows": stock_rows,
        "tape": tape,
        "sources": source,
    }


def check_and_heal(args: argparse.Namespace) -> dict[str, Any]:
    tickers = [str(t).upper() for t in args.tickers]
    before = evaluate(
        args.day,
        tickers=tickers,
        feed=args.feed,
        quote_mode=args.quote_mode,
        btc_mode=args.btc_mode,
        max_tape_age_sec=args.max_tape_age_sec,
        min_stock_rows=args.min_stock_rows,
    )
    materialize_payload: dict[str, Any] | None = None
    after_materialize = before
    if not before.get("ok") or args.force_materialize:
        materialize_payload = live_step2_feed.materialize(
            day=args.day,
            tickers=tickers,
            feed=args.feed,
            quote_mode=args.quote_mode,
            btc_mode=args.btc_mode,
            skip_if_fresh=False,
        )
        after_materialize = evaluate(
            args.day,
            tickers=tickers,
            feed=args.feed,
            quote_mode=args.quote_mode,
            btc_mode=args.btc_mode,
            max_tape_age_sec=args.max_tape_age_sec,
            min_stock_rows=args.min_stock_rows,
        )
    restart_needed = bool(after_materialize.get("session_active") and not after_materialize.get("ok"))
    payload = {
        "schema_version": SCHEMA_VERSION,
        "source": "live_market_data_watchdog",
        "created_at_ct": _now_ct(),
        "day": args.day,
        "tickers": tickers,
        "ok": bool(after_materialize.get("ok")),
        "restart_needed": restart_needed,
        "before": before,
        "materialize": materialize_payload,
        "after_materialize": after_materialize,
        "deduction": (
            "Intraday self-heal can rematerialize from captured live JSONL/index data. "
            "If capture itself is empty or stale, the caller should restart the live monitor/app path."
        ),
    }
    out = OUT_DIR / f"live_market_data_watchdog_{args.day}.json"
    payload["path"] = _write_json(out, payload)
    return payload


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Check and rematerialize intraday live market-data tape.")
    ap.add_argument("day")
    ap.add_argument("--tickers", nargs="+", default=DEFAULT_TICKERS)
    ap.add_argument("--feed", default="sip")
    ap.add_argument("--quote-mode", default="per-second")
    ap.add_argument("--btc-mode", default="bars")
    ap.add_argument("--max-tape-age-sec", type=int, default=180)
    ap.add_argument("--min-stock-rows", type=int, default=1)
    ap.add_argument("--force-materialize", action="store_true")
    ap.add_argument("--json", action="store_true")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    payload = check_and_heal(args)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    else:
        print(f"live_market_data_watchdog day={payload['day']} ok={payload['ok']} restart_needed={payload['restart_needed']}")
        print(payload.get("path"))
    return 0 if payload.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
