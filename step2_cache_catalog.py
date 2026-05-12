"""Certified Step 2 cache catalog and resolver.

The adaptive hunter should not trust a hardcoded compiled tape path. This module
builds a small catalog from compiled manifests plus cache-certification receipts
and resolves the newest certified manifest that covers the requested tickers and
dates.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import tournament_safety


HERE = Path(__file__).resolve().parent
COMPILED_DIR = HERE / "postmortem" / "backtests" / "compiled_decision_tapes"
CERT_DIR = HERE / "postmortem" / "cache_certifications"
OUT_DIR = HERE / "postmortem" / "cache_catalog"
CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1
DEFAULT_TICKERS = ["CLSK", "MARA", "RIOT"]


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


def _read_json(path: str | Path, default: Any = None) -> Any:
    try:
        with Path(path).open("r", encoding="utf-8-sig") as fh:
            payload = json.load(fh)
        return payload if payload is not None else default
    except Exception:
        return default


def _write_json(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.{int(time.time() * 1000)}.tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True, default=str)
        fh.write("\n")
    os.replace(tmp, path)
    return str(path.resolve())


def _file_meta(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    exists = p.exists()
    return {
        "path": str(p.resolve()) if exists else str(p),
        "exists": exists,
        "bytes": p.stat().st_size if exists else 0,
        "mtime": p.stat().st_mtime if exists else None,
        "sha256": tournament_safety._file_sha256(p) if exists else "",
    }


def _iter_manifest_paths() -> list[Path]:
    if not COMPILED_DIR.exists():
        return []
    return sorted(COMPILED_DIR.glob("*/manifest.json"))


def _iter_receipt_paths() -> list[Path]:
    if not CERT_DIR.exists():
        return []
    return sorted(CERT_DIR.glob("*.json"))


def _date_tokens(value: str) -> list[str]:
    return re.findall(r"20\d{2}-\d{2}-\d{2}", value or "")


def _source_days(manifest: dict[str, Any], manifest_path: str | Path | None = None) -> list[str]:
    day_map = manifest.get("day_map")
    if isinstance(day_map, dict):
        return sorted(str(day) for day in day_map)
    days = manifest.get("source_days")
    if isinstance(days, list):
        return sorted(str(day) for day in days)
    if manifest_path:
        tokens = _date_tokens(str(Path(manifest_path).parent.name))
        if len(tokens) == 1:
            return tokens
    return []


def _date_bounds(
    manifest: dict[str, Any],
    manifest_path: str | Path | None,
    days: list[str],
) -> tuple[str | None, str | None]:
    if days:
        return days[0], days[-1]
    raw_start = manifest.get("start_day") or manifest.get("first_day")
    raw_end = manifest.get("end_day") or manifest.get("last_day")
    if raw_start or raw_end:
        start = str(raw_start or raw_end)
        end = str(raw_end or raw_start)
        return start, end
    if manifest_path:
        tokens = _date_tokens(str(Path(manifest_path).parent.name))
        if len(tokens) >= 2:
            return tokens[0], tokens[-1]
        if len(tokens) == 1:
            return tokens[0], tokens[0]
    return None, None


def _tickers(manifest: dict[str, Any], manifest_path: str | Path | None = None) -> list[str]:
    ticker_map = manifest.get("ticker_map")
    if isinstance(ticker_map, dict) and ticker_map:
        return sorted(str(ticker).upper() for ticker in ticker_map)
    raw = manifest.get("tickers")
    if isinstance(raw, list) and raw:
        return sorted(str(ticker).upper() for ticker in raw)
    if manifest_path:
        name = Path(manifest_path).parent.name.upper()
        hits = [ticker for ticker in DEFAULT_TICKERS if ticker in name]
        if hits:
            return sorted(hits)
    return []


def _receipt_sort_key(path: Path, receipt: dict[str, Any]) -> tuple[str, float]:
    return str(receipt.get("created_at_ct") or ""), float(path.stat().st_mtime if path.exists() else 0.0)


def _latest_receipts_by_manifest() -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for path in _iter_receipt_paths():
        receipt = _read_json(path, {}) or {}
        compiled_meta = receipt.get("compiled_manifest") if isinstance(receipt.get("compiled_manifest"), dict) else {}
        raw_manifest = compiled_meta.get("path")
        if not raw_manifest:
            continue
        try:
            key = str(Path(raw_manifest).resolve())
        except Exception:
            key = str(raw_manifest)
        record = {
            "path": str(path.resolve()) if path.exists() else str(path),
            "receipt": receipt,
            "sort_key": _receipt_sort_key(path, receipt),
        }
        old = latest.get(key)
        if old is None or record["sort_key"] > old.get("sort_key", ("", 0.0)):
            latest[key] = record
    return latest


def _lineage_after(receipt: dict[str, Any]) -> dict[str, Any]:
    after = receipt.get("after")
    if isinstance(after, dict):
        return after
    return {}


def _receipt_valid_for_manifest(
    record: dict[str, Any] | None,
    manifest_meta: dict[str, Any],
) -> tuple[bool, list[str], dict[str, Any]]:
    if not record:
        return False, ["certification_receipt_missing"], {}
    receipt = record.get("receipt") if isinstance(record.get("receipt"), dict) else {}
    reasons: list[str] = []
    if receipt.get("ok") is not True:
        reasons.append("certification_receipt_not_ok")

    compiled_meta = receipt.get("compiled_manifest") if isinstance(receipt.get("compiled_manifest"), dict) else {}
    receipt_sha = str(compiled_meta.get("sha256") or "")
    manifest_sha = str(manifest_meta.get("sha256") or "")
    if receipt_sha and manifest_sha and receipt_sha != manifest_sha:
        reasons.append("certification_manifest_hash_mismatch")

    after = _lineage_after(receipt)
    status = after.get("lineage_status") or after.get("status")
    certified = after.get("lineage_certified")
    if certified is None:
        certified = after.get("certified")
    if status != "CERTIFIED_MATCH":
        reasons.append("certification_lineage_not_match")
    if certified is not True:
        reasons.append("certification_lineage_not_certified")
    return not reasons, reasons, receipt


def _manifest_entry(path: Path, receipts: dict[str, dict[str, Any]]) -> dict[str, Any]:
    manifest = _read_json(path, {}) or {}
    meta = _file_meta(path)
    days = _source_days(manifest, path)
    start_day, end_day = _date_bounds(manifest, path, days)
    try:
        receipt_record = receipts.get(str(Path(meta["path"]).resolve()))
    except Exception:
        receipt_record = receipts.get(str(meta["path"]))
    valid, reasons, receipt = _receipt_valid_for_manifest(receipt_record, meta)
    after = _lineage_after(receipt)
    compiled_meta = receipt.get("compiled_manifest") if isinstance(receipt.get("compiled_manifest"), dict) else {}
    certification = {
        "exists": bool(receipt_record),
        "valid": bool(valid),
        "ok": bool(receipt.get("ok")) if receipt else False,
        "path": (receipt_record or {}).get("path", ""),
        "receipt_sha256": tournament_safety._file_sha256((receipt_record or {}).get("path", "")) if receipt_record else "",
        "selected_action": receipt.get("selected_action"),
        "certification_mode": receipt.get("certification_mode"),
        "day": receipt.get("day"),
        "start": receipt.get("start"),
        "end": receipt.get("end"),
        "source_days": receipt.get("source_days") or [],
        "tickers": receipt.get("tickers") or [],
        "compiled_manifest": compiled_meta,
        "after": after,
        "failure_reasons": reasons,
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "name": path.parent.name,
        "certified": bool(valid),
        "manifest": {
            **meta,
            "name": path.parent.name,
        },
        "tickers": _tickers(manifest, path),
        "source_days": days,
        "start_day": start_day,
        "end_day": end_day,
        "row_count": int(manifest.get("row_count") or manifest.get("rows") or 0),
        "min_ts": manifest.get("min_ts") or manifest.get("first_ts"),
        "max_ts": manifest.get("max_ts") or manifest.get("last_ts"),
        "compiled_tape_hash": str(manifest.get("compiled_tape_hash") or ""),
        "arrays_sha256": str(manifest.get("arrays_sha256") or manifest.get("array_payload_hash") or ""),
        "step2_execution_contract_hash": str(manifest.get("step2_execution_contract_hash") or ""),
        "lineage_status_from_manifest": (
            (manifest.get("lineage_validation") or {}).get("status")
            if isinstance(manifest.get("lineage_validation"), dict)
            else None
        ),
        "lineage_certified_from_manifest": (
            (manifest.get("lineage_validation") or {}).get("certified")
            if isinstance(manifest.get("lineage_validation"), dict)
            else None
        ),
        "certification": certification,
    }


def build_catalog() -> dict[str, Any]:
    receipts = _latest_receipts_by_manifest()
    entries = [_manifest_entry(path, receipts) for path in _iter_manifest_paths()]
    entries.sort(key=_entry_sort_key, reverse=True)
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "step2_cache_catalog",
        "created_at_ct": _now_ct(),
        "compiled_dir": str(COMPILED_DIR.resolve()),
        "certification_dir": str(CERT_DIR.resolve()),
        "total_entries": len(entries),
        "certified_entries": sum(1 for entry in entries if entry.get("certified")),
        "uncertified_entries": sum(1 for entry in entries if not entry.get("certified")),
        "entries": entries,
    }


def write_catalog(path: str | Path | None = None) -> dict[str, Any]:
    payload = build_catalog()
    target = Path(path) if path else OUT_DIR / "step2_cache_catalog.json"
    payload["path"] = _write_json(target, payload)
    return payload


def _normalize_tickers(tickers: list[str] | tuple[str, ...] | None) -> list[str]:
    raw = tickers or []
    return sorted(str(ticker).upper() for ticker in raw if str(ticker).strip())


def _covers_date(entry: dict[str, Any], *, day: str = "", start: str = "", end: str = "") -> bool:
    days = [str(value) for value in (entry.get("source_days") or [])]
    start_day = str(entry.get("start_day") or "")
    end_day = str(entry.get("end_day") or "")
    if day:
        return day in days or bool(start_day and end_day and start_day <= day <= end_day)
    if start or end:
        req_start = start or end
        req_end = end or start
        return bool(start_day and end_day and start_day <= req_start and end_day >= req_end)
    return True


def _entry_sort_key(entry: dict[str, Any]) -> tuple[int, str, str, int, float]:
    manifest = entry.get("manifest") if isinstance(entry.get("manifest"), dict) else {}
    return (
        1 if entry.get("certified") else 0,
        str(entry.get("end_day") or ""),
        str(entry.get("start_day") or ""),
        int(entry.get("row_count") or 0),
        float(manifest.get("mtime") or 0.0),
    )


def _certification_hint(
    *,
    tickers: list[str],
    day: str,
    start: str,
    end: str,
    best_uncertified: dict[str, Any] | None,
) -> dict[str, Any]:
    hint_day = day
    manifest_path = ""
    if best_uncertified and isinstance(best_uncertified.get("manifest"), dict):
        manifest_path = str(best_uncertified["manifest"].get("path") or "")
    if not hint_day and best_uncertified:
        source_days = best_uncertified.get("source_days") or []
        if len(source_days) == 1:
            hint_day = str(source_days[0])
    if best_uncertified and not hint_day:
        hint_start = start or str(best_uncertified.get("start_day") or "")
        hint_end = end or str(best_uncertified.get("end_day") or "")
        if hint_start and hint_end:
            quoted_manifest = f' --compiled-manifest "{manifest_path}"' if manifest_path else ""
            return {
                "supported": True,
                "reason": "certify_range_cache",
                "command": (
                    "python certify_step2_cache.py --start "
                    f"{hint_start} --end {hint_end} --tickers {' '.join(tickers)}"
                    f"{quoted_manifest} --workers 6"
                ),
            }
    if hint_day:
        quoted_manifest = f' --compiled-manifest "{manifest_path}"' if manifest_path else ""
        return {
            "supported": True,
            "reason": "certify_one_day_cache",
            "command": (
                "python certify_step2_cache.py --day "
                f"{hint_day} --tickers {' '.join(tickers)}{quoted_manifest} --workers 6"
            ),
        }
    return {
        "supported": False,
        "reason": "multi_day_or_unspecified_cache_needs_range_build_then_certification",
        "requested_start": start,
        "requested_end": end,
    }


def resolve(
    *,
    tickers: list[str] | tuple[str, ...] | None = None,
    day: str = "",
    start: str = "",
    end: str = "",
    require_certified: bool = True,
    catalog: dict[str, Any] | None = None,
) -> dict[str, Any]:
    wanted = _normalize_tickers(list(tickers or [])) or DEFAULT_TICKERS
    payload = catalog if isinstance(catalog, dict) else build_catalog()
    entries = payload.get("entries") if isinstance(payload.get("entries"), list) else []
    matching: list[dict[str, Any]] = []
    ticker_matches = 0
    date_matches = 0
    uncertified_matches = 0
    for entry in entries:
        entry_tickers = _normalize_tickers(entry.get("tickers") or [])
        if entry_tickers != wanted:
            continue
        ticker_matches += 1
        if not _covers_date(entry, day=day, start=start, end=end):
            continue
        date_matches += 1
        if not entry.get("certified"):
            uncertified_matches += 1
            if require_certified:
                continue
        matching.append(entry)

    matching.sort(key=_entry_sort_key, reverse=True)
    selected = matching[0] if matching else None
    best_uncertified = None
    if require_certified:
        uncertified = [
            entry for entry in entries
            if _normalize_tickers(entry.get("tickers") or []) == wanted
            and _covers_date(entry, day=day, start=start, end=end)
            and not entry.get("certified")
        ]
        uncertified.sort(key=_entry_sort_key, reverse=True)
        best_uncertified = uncertified[0] if uncertified else None

    blockers: list[str] = []
    if ticker_matches == 0:
        blockers.append("no_cache_for_requested_tickers")
    elif date_matches == 0:
        blockers.append("no_cache_covers_requested_date_range")
    elif require_certified and not selected:
        blockers.append("no_certified_cache_for_requested_scope")

    manifest_path = ""
    if selected and isinstance(selected.get("manifest"), dict):
        manifest_path = str(selected["manifest"].get("path") or "")
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "step2_cache_catalog",
        "created_at_ct": _now_ct(),
        "ok": bool(selected),
        "requested": {
            "tickers": wanted,
            "day": day,
            "start": start,
            "end": end,
            "require_certified": require_certified,
        },
        "manifest_path": manifest_path,
        "selected": selected or {},
        "certification_receipt": (selected or {}).get("certification") or {},
        "blockers": blockers,
        "candidate_count": len(matching),
        "ticker_match_count": ticker_matches,
        "date_match_count": date_matches,
        "uncertified_candidate_count": uncertified_matches,
        "catalog_summary": {
            "created_at_ct": payload.get("created_at_ct"),
            "total_entries": payload.get("total_entries"),
            "certified_entries": payload.get("certified_entries"),
            "uncertified_entries": payload.get("uncertified_entries"),
        },
        "certification_hint": _certification_hint(
            tickers=wanted,
            day=day,
            start=start,
            end=end,
            best_uncertified=best_uncertified,
        ),
    }


def _compact_resolution(payload: dict[str, Any]) -> dict[str, Any]:
    selected = payload.get("selected") if isinstance(payload.get("selected"), dict) else {}
    manifest = selected.get("manifest") if isinstance(selected.get("manifest"), dict) else {}
    cert = selected.get("certification") if isinstance(selected.get("certification"), dict) else {}
    return {
        "ok": bool(payload.get("ok")),
        "source": payload.get("source") or "step2_cache_catalog",
        "requested": payload.get("requested") or {},
        "manifest_path": payload.get("manifest_path") or "",
        "selected_name": selected.get("name"),
        "selected_start_day": selected.get("start_day"),
        "selected_end_day": selected.get("end_day"),
        "selected_source_days": selected.get("source_days") or [],
        "selected_row_count": selected.get("row_count"),
        "manifest_sha256": manifest.get("sha256"),
        "certification": {
            "valid": cert.get("valid"),
            "ok": cert.get("ok"),
            "path": cert.get("path"),
            "selected_action": cert.get("selected_action"),
            "certification_mode": cert.get("certification_mode"),
            "failure_reasons": cert.get("failure_reasons") or [],
        },
        "blockers": payload.get("blockers") or [],
        "certification_hint": payload.get("certification_hint") or {},
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Resolve a certified compiled Step 2 cache.")
    ap.add_argument("--tickers", nargs="*", default=DEFAULT_TICKERS)
    ap.add_argument("--day", default="")
    ap.add_argument("--start", default="")
    ap.add_argument("--end", default="")
    ap.add_argument("--allow-uncertified", action="store_true")
    ap.add_argument("--write-catalog", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    catalog = write_catalog() if args.write_catalog else build_catalog()
    payload = resolve(
        tickers=args.tickers,
        day=args.day,
        start=args.start,
        end=args.end,
        require_certified=not args.allow_uncertified,
        catalog=catalog,
    )
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    else:
        compact = _compact_resolution(payload)
        print(json.dumps(compact, indent=2, sort_keys=True, default=str))
    return 0 if payload.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
