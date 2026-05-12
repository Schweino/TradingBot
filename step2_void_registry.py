"""Void-day registry and contamination checks for strategy learning.

Voided market days are forensic-only. They may explain operations incidents,
but they must not seed Step 2 hunts, promotion evidence, route memory, or
calibration state unless a caller explicitly opts into forensic reads.
"""
from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any


HERE = Path(__file__).resolve().parent
DEFAULT_REGISTRY = HERE / "postmortem" / "voided_days.json"
SCHEMA_VERSION = 1

_ISO_DAY_RE = re.compile(r"20\d{2}-\d{2}-\d{2}")
_COMPACT_DAY_RE = re.compile(r"(?<!\d)(20\d{2})(\d{2})(\d{2})(?!\d)")


def _read_json(path: str | os.PathLike[str], default: Any = None) -> Any:
    try:
        with Path(path).open("r", encoding="utf-8-sig") as handle:
            payload = json.load(handle)
        return payload if payload is not None else default
    except Exception:
        return default


def _write_json(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.{int(time.time() * 1000)}.tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, default=str)
        handle.write("\n")
    os.replace(tmp, path)
    return str(path.resolve())


def normalize_day(value: Any) -> str:
    text = str(value or "")
    match = _ISO_DAY_RE.search(text)
    if match:
        return match.group(0)
    match = _COMPACT_DAY_RE.search(text)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    return text[:10] if _ISO_DAY_RE.fullmatch(text[:10]) else ""


def day_tokens(value: Any) -> list[str]:
    text = str(value or "")
    days = set(_ISO_DAY_RE.findall(text))
    for year, month, day in _COMPACT_DAY_RE.findall(text):
        days.add(f"{year}-{month}-{day}")
    return sorted(days)


def load_registry(path: str | os.PathLike[str] = DEFAULT_REGISTRY) -> dict[str, Any]:
    payload = _read_json(path, {}) or {}
    if not isinstance(payload, dict):
        return {"schema_version": SCHEMA_VERSION, "voided_days": []}
    payload.setdefault("schema_version", SCHEMA_VERSION)
    payload.setdefault("voided_days", [])
    return payload


def voided_entries(registry: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    registry = registry or load_registry()
    entries = registry.get("voided_days")
    if isinstance(entries, dict):
        return [dict(value, day=str(key)) for key, value in entries.items() if isinstance(value, dict)]
    if isinstance(entries, list):
        return [entry for entry in entries if isinstance(entry, dict)]
    return []


def voided_day_set(registry: dict[str, Any] | None = None) -> set[str]:
    return {normalize_day(entry.get("day")) for entry in voided_entries(registry) if normalize_day(entry.get("day"))}


def is_day_voided(day: Any, registry: dict[str, Any] | None = None) -> bool:
    normalized = normalize_day(day)
    return bool(normalized and normalized in voided_day_set(registry))


def source_days_from_payload(payload: Any, *, source_path: str | os.PathLike[str] | None = None) -> list[str]:
    days: set[str] = set()
    if source_path:
        days.update(day_tokens(str(source_path)))
    if not isinstance(payload, dict):
        return sorted(days)

    by_day = payload.get("by_day")
    if isinstance(by_day, dict):
        days.update(normalize_day(day) for day in by_day if normalize_day(day))

    for key in ("day", "source_day", "trading_day"):
        day = normalize_day(payload.get(key))
        if day:
            days.add(day)

    for key in ("source_days", "days", "requested_days", "holdout_days"):
        raw = payload.get(key)
        if isinstance(raw, list):
            days.update(normalize_day(day) for day in raw if normalize_day(day))

    for key in ("start_day", "end_day", "start", "end"):
        day = normalize_day(payload.get(key))
        if day:
            days.add(day)

    for key in ("hunt_source_path", "memory_source_file", "memory_source_run", "compiled_decision_tape"):
        days.update(day_tokens(payload.get(key)))

    return sorted(days)


def contamination_report(
    payload: Any,
    *,
    source_path: str | os.PathLike[str] | None = None,
    registry: dict[str, Any] | None = None,
) -> dict[str, Any]:
    registry = registry or load_registry()
    source_days = source_days_from_payload(payload, source_path=source_path)
    voided = [day for day in source_days if is_day_voided(day, registry)]
    return {
        "source_days": source_days,
        "voided_source_days": voided,
        "contamination_status": "tainted" if voided else "clean",
        "strategy_learning_allowed": not bool(voided),
    }


def annotate_payload(
    payload: dict[str, Any],
    *,
    source_path: str | os.PathLike[str] | None = None,
    registry: dict[str, Any] | None = None,
) -> dict[str, Any]:
    report = contamination_report(payload, source_path=source_path, registry=registry)
    out = dict(payload)
    out["source_days"] = report["source_days"]
    out["voided_source_days"] = report["voided_source_days"]
    out["contamination_status"] = report["contamination_status"]
    out["strategy_learning_allowed"] = report["strategy_learning_allowed"]
    return out


def is_payload_contaminated(
    payload: Any,
    *,
    source_path: str | os.PathLike[str] | None = None,
    registry: dict[str, Any] | None = None,
) -> bool:
    return bool(contamination_report(payload, source_path=source_path, registry=registry)["voided_source_days"])


def filter_clean_rows(
    rows: list[dict[str, Any]],
    *,
    include_voided_for_forensics: bool = False,
    registry: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    clean: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for row in rows:
        annotated = annotate_payload(row, source_path=row.get("hunt_source_path"), registry=registry)
        if annotated["contamination_status"] == "tainted" and not include_voided_for_forensics:
            skipped.append(annotated)
        else:
            clean.append(annotated)
    return clean, skipped


def assert_learning_allowed(payload: Any, *, context: str = "strategy_learning") -> None:
    report = contamination_report(payload)
    if report["voided_source_days"]:
        raise ValueError(
            f"{context} blocked: payload includes voided source day(s) "
            f"{', '.join(report['voided_source_days'])}"
        )


def ensure_default_registry() -> str:
    payload = load_registry()
    days = {normalize_day(entry.get("day")) for entry in voided_entries(payload)}
    if "2026-05-11" not in days:
        payload.setdefault("voided_days", []).append(
            {
                "day": "2026-05-11",
                "scope": "full_day",
                "void_reason": "system_broken_contaminated_day",
                "void_reason_detail": "User marked all 2026-05-11 data void after feed/broker/engine failures.",
                "learning_allowed": False,
                "strategy_conclusions_allowed": False,
                "promotion_allowed": False,
                "hunt_seed_allowed": False,
                "calibration_allowed": False,
                "forensic_only": True,
            }
        )
    return _write_json(DEFAULT_REGISTRY, payload)
