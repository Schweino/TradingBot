"""Repair market-data replay artifacts after live capture gaps.

This script is intentionally conservative: canonical EOD data remains the source
of truth, and any repaired intraday tape is marked as a canonical EOD repair.
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import incremental_market_store
import live_step2_feed
import market_data_freshness_guard
import market_data_integrity_gate
import worker_policy


HERE = Path(__file__).resolve().parent
CT = ZoneInfo("America/Chicago")
DEFAULT_TICKERS = ["CLSK", "MARA", "RIOT"]
SCHEMA_VERSION = 1


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


def _read_json(path: Path, default: Any = None) -> Any:
    try:
        with path.open("r", encoding="utf-8-sig") as f:
            return json.load(f)
    except Exception:
        return default


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
    with gzip.open(path, "rt", encoding="utf-8") as f:
        payload = json.load(f)
    return payload if isinstance(payload, list) else []


def _run(cmd: list[str], timeout: int = 900) -> dict[str, Any]:
    proc = subprocess.run(
        cmd,
        cwd=HERE,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
    )
    return {
        "command": cmd,
        "returncode": proc.returncode,
        "ok": proc.returncode == 0,
        "output_tail": proc.stdout[-6000:],
    }


def _manifest_path(path: Path) -> Path:
    name = str(path)
    if name.endswith(".events.json.gz"):
        return Path(name[: -len(".events.json.gz")] + ".manifest.json")
    return path.with_suffix(path.suffix + ".manifest.json")


def _prepared_path(day: str, tickers: list[str], source: str) -> Path:
    return Path(market_data_integrity_gate.prepared_path(day, tickers, source=source))


def _backup(path: Path, backup_dir: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path.resolve()), "exists": False}
    backup_dir.mkdir(parents=True, exist_ok=True)
    target = backup_dir / path.name
    shutil.copy2(path, target)
    return {
        "path": str(path.resolve()),
        "exists": True,
        "bytes": path.stat().st_size,
        "backup_path": str(target.resolve()),
    }


def _repair_intraday_from_canonical(day: str, tickers: list[str], reason: str) -> dict[str, Any]:
    canonical = _prepared_path(day, tickers, "canonical")
    intraday = _prepared_path(day, tickers, "intraday")
    if not canonical.exists():
        return {"ok": False, "reason": "canonical_tape_missing", "canonical_path": str(canonical)}

    stamp = datetime.now(CT).strftime("%Y%m%d_%H%M%S")
    backup_dir = HERE / "archive" / "market_data_repairs" / day / stamp
    backups = [
        _backup(intraday, backup_dir),
        _backup(_manifest_path(intraday), backup_dir),
    ]
    intraday.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(canonical, intraday)

    manifest = live_step2_feed.write_prepared_manifest(
        day=day,
        tickers=tickers,
        source="canonical_eod_repair",
        canonical=False,
        prepared_cache_dir=str(intraday.parent),
    )
    manifest_path = Path(manifest.get("manifest_path") or _manifest_path(intraday))
    current = _read_json(manifest_path, {}) or {}
    current.update({
        "source": "canonical_eod_repair",
        "repair": True,
        "repair_reason": reason,
        "repaired_at_ct": _now_ct(),
        "canonical_source_path": str(canonical.resolve()),
        "backups": backups,
        "deduction": (
            "This intraday tape was repaired from certified canonical EOD data. "
            "It is suitable for replay continuity, not evidence that live capture was healthy."
        ),
    })
    _write_json(manifest_path, current)
    return {
        "ok": True,
        "intraday_path": str(intraday.resolve()),
        "manifest_path": str(manifest_path.resolve()),
        "backups": backups,
    }


def repair(args: argparse.Namespace) -> dict[str, Any]:
    tickers = [str(t).upper() for t in args.tickers]
    workers = worker_policy.clamp_workers(args.workers)
    steps: list[dict[str, Any]] = []

    if args.download:
        steps.append(_run([
            sys.executable,
            "prepare_replay_cache.py",
            "--start", args.day,
            "--end", args.day,
            "--tickers", *tickers,
            "--feed", args.feed,
            "--quote-mode", args.quote_mode,
            "--btc-mode", args.btc_mode,
            "--refresh",
            "--refresh-prepared-events",
            "--workers", str(workers),
        ], timeout=1200))

    canonical_path = _prepared_path(args.day, tickers, "canonical")
    incremental = incremental_market_store.update_from_prepared(str(canonical_path), args.day)
    steps.append({"command": ["incremental_market_store", args.day], "ok": bool(incremental.get("manifest_path")), **incremental})

    integrity = market_data_integrity_gate.build(
        args.day,
        tickers=tickers,
        feed=args.feed,
        quote_mode=args.quote_mode,
        btc_mode=args.btc_mode,
        source="canonical",
        write=True,
        use_cached=False,
    )
    steps.append({"command": ["market_data_integrity_gate", args.day, "--source", "canonical"], "ok": bool(integrity.get("promotion_safe")), **integrity})

    before_compare = live_step2_feed.compare_prepared(
        day=args.day,
        tickers=tickers,
        feed=args.feed,
        quote_mode=args.quote_mode,
        btc_mode=args.btc_mode,
    )
    compare_bad = (
        int(before_compare.get("missing_from_intraday_rows") or 0) > 0
        or (args.repair_live_only and int(before_compare.get("live_only_rows") or 0) > 0)
        or int((before_compare.get("intraday") or {}).get("rows") or 0) <= 0
    )
    steps.append({"command": ["live_step2_feed", "compare", args.day, "before"], "ok": True, **before_compare})

    intraday_repair = {"attempted": False}
    if args.repair_intraday and compare_bad:
        reason = "intraday_missing_or_drifted_from_canonical"
        intraday_repair = {"attempted": True, **_repair_intraday_from_canonical(args.day, tickers, reason)}
        steps.append({"command": ["repair_intraday_from_canonical", args.day], **intraday_repair})

    after_compare = live_step2_feed.compare_prepared(
        day=args.day,
        tickers=tickers,
        feed=args.feed,
        quote_mode=args.quote_mode,
        btc_mode=args.btc_mode,
    )
    steps.append({"command": ["live_step2_feed", "compare", args.day, "after"], "ok": True, **after_compare})

    freshness = market_data_freshness_guard.build(
        args.day,
        tickers=tickers,
        source="canonical",
        write=True,
        use_cached_integrity=False,
    )
    steps.append({"command": ["market_data_freshness_guard", args.day, "--source", "canonical"], "ok": bool(freshness.get("ok")), **freshness})

    ok = bool(integrity.get("promotion_safe")) and bool(freshness.get("ok"))
    if args.require_clean_compare:
        ok = ok and int(after_compare.get("missing_from_intraday_rows") or 0) == 0 and int(after_compare.get("live_only_rows") or 0) == 0

    payload = {
        "schema_version": SCHEMA_VERSION,
        "source": "repair_market_data",
        "created_at_ct": _now_ct(),
        "day": args.day,
        "tickers": tickers,
        "ok": ok,
        "download_attempted": bool(args.download),
        "intraday_repair": intraday_repair,
        "before_compare": {
            "missing_from_intraday_rows": before_compare.get("missing_from_intraday_rows"),
            "live_only_rows": before_compare.get("live_only_rows"),
            "intraday_rows": (before_compare.get("intraday") or {}).get("rows"),
            "canonical_rows": (before_compare.get("canonical_summary") or {}).get("rows"),
        },
        "after_compare": {
            "missing_from_intraday_rows": after_compare.get("missing_from_intraday_rows"),
            "live_only_rows": after_compare.get("live_only_rows"),
            "intraday_rows": (after_compare.get("intraday") or {}).get("rows"),
            "canonical_rows": (after_compare.get("canonical_summary") or {}).get("rows"),
        },
        "integrity": {
            "verdict": integrity.get("verdict"),
            "promotion_safe": integrity.get("promotion_safe"),
            "critical_count": integrity.get("critical_count"),
            "warning_count": integrity.get("warning_count"),
        },
        "freshness": {
            "verdict": freshness.get("verdict"),
            "ok": freshness.get("ok"),
            "critical_count": freshness.get("critical_count"),
            "warning_count": freshness.get("warning_count"),
        },
        "steps": steps,
    }
    out = HERE / "postmortem" / "market_data_repairs" / f"market_data_repair_{args.day}.json"
    payload["path"] = _write_json(out, payload)
    return payload


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Repair canonical and intraday replay market-data artifacts.")
    ap.add_argument("day")
    ap.add_argument("--tickers", nargs="+", default=DEFAULT_TICKERS)
    ap.add_argument("--feed", default="sip")
    ap.add_argument("--quote-mode", default="per-second")
    ap.add_argument("--btc-mode", default="bars")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--download", action="store_true", help="Refetch historical Alpaca data before rebuilding the canonical tape.")
    ap.add_argument("--no-repair-intraday", dest="repair_intraday", action="store_false")
    ap.add_argument("--repair-live-only", action="store_true", help="Also replace live tape when it only has extra live rows.")
    ap.add_argument("--require-clean-compare", action="store_true")
    ap.add_argument("--json", action="store_true")
    ap.set_defaults(repair_intraday=True)
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    payload = repair(args)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    else:
        print(f"market_data_repair day={payload['day']} ok={payload['ok']}")
        print(payload.get("path"))
        print(f"before={payload['before_compare']}")
        print(f"after={payload['after_compare']}")
    return 0 if payload.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
