"""Mark a trading day as void for all strategy-learning paths."""
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import step2_void_registry


CT = ZoneInfo("America/Chicago")


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


def upsert_void_day(
    day: str,
    *,
    reason: str,
    detail: str = "",
    scope: str = "full_day",
    operator: str = "operator",
    registry_path: str | Path = step2_void_registry.DEFAULT_REGISTRY,
) -> dict[str, Any]:
    normalized = step2_void_registry.normalize_day(day)
    if not normalized:
        raise ValueError(f"Invalid day: {day}")
    registry = step2_void_registry.load_registry(registry_path)
    entries = step2_void_registry.voided_entries(registry)
    entry = {
        "day": normalized,
        "scope": scope,
        "void_reason": reason,
        "void_reason_detail": detail,
        "operator": operator,
        "voided_at_ct": _now_ct(),
        "learning_allowed": False,
        "strategy_conclusions_allowed": False,
        "promotion_allowed": False,
        "hunt_seed_allowed": False,
        "calibration_allowed": False,
        "forensic_only": True,
    }
    replaced = False
    for idx, existing in enumerate(entries):
        if step2_void_registry.normalize_day(existing.get("day")) == normalized:
            entries[idx] = {**existing, **entry}
            replaced = True
            break
    if not replaced:
        entries.append(entry)
    registry["voided_days"] = entries
    path = step2_void_registry._write_json(Path(registry_path), registry)
    return {"day": normalized, "path": path, "replaced": replaced, "entry": entry}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Void a trading day for strategy learning.")
    parser.add_argument("day")
    parser.add_argument("--reason", default="manual_void")
    parser.add_argument("--detail", default="")
    parser.add_argument("--scope", default="full_day")
    parser.add_argument("--operator", default="operator")
    parser.add_argument("--registry", default=str(step2_void_registry.DEFAULT_REGISTRY))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = upsert_void_day(
        args.day,
        reason=args.reason,
        detail=args.detail,
        scope=args.scope,
        operator=args.operator,
        registry_path=args.registry,
    )
    print(result["path"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
