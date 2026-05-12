"""Per-day compiled shard certification registry.

Range manifests are now often cheap linked manifests. This registry records the
certified day-shard identities behind a range certification so future range
checks can reason about day-level validity without rebuilding or rescanning the
entire monolithic cache.
"""
from __future__ import annotations

import json
import os
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
CT = ZoneInfo("America/Chicago")
OUT_DIR = HERE / "postmortem" / "cache_certifications" / "day_shards"
SCHEMA_VERSION = 1


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


def _read_json(path: str | os.PathLike[str], default: Any = None) -> Any:
    try:
        with Path(path).open("r", encoding="utf-8-sig") as f:
            payload = json.load(f)
        return payload if payload is not None else default
    except Exception:
        return default


def _write_json(path: str | os.PathLike[str], payload: dict[str, Any]) -> str:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{os.getpid()}.{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    os.replace(tmp, target)
    return str(target.resolve())


def _day_shards(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    day_shards = manifest.get("day_shards") if isinstance(manifest.get("day_shards"), dict) else {}
    shards = day_shards.get("shards")
    return list(shards) if isinstance(shards, list) else []


def _entry_hash(entry: dict[str, Any]) -> str:
    return tournament_safety.stable_json_hash(entry, length=32)


def registry_for_manifest(
    manifest_path: str | os.PathLike[str],
    certification: dict[str, Any],
) -> dict[str, Any]:
    path = Path(manifest_path).resolve()
    manifest = _read_json(path, {}) or {}
    shards = _day_shards(manifest)
    after = certification.get("after") if isinstance(certification.get("after"), dict) else {}
    certified = bool(
        certification.get("ok")
        and (after.get("lineage_status") or after.get("status")) == "CERTIFIED_MATCH"
        and (after.get("lineage_certified") if "lineage_certified" in after else after.get("certified")) is True
    )
    entries = []
    for shard in shards:
        source_identities = shard.get("source_identities") if isinstance(shard.get("source_identities"), dict) else {}
        source_semantic = {
            str(src): identity.get("semantic_sha256")
            for src, identity in source_identities.items()
            if isinstance(identity, dict)
        }
        entry = {
            "day": str(shard.get("day") or ""),
            "rows": int(shard.get("rows") or 0),
            "min_ts": shard.get("min_ts"),
            "max_ts": shard.get("max_ts"),
            "array_payload_sha256": shard.get("array_payload_sha256"),
            "source_semantic_hashes": source_semantic,
            "source_path_count": len(source_identities),
            "certified": certified,
            "certification_hash": certification.get("certification_hash"),
            "parent_compiled_tape_hash": manifest.get("compiled_tape_hash"),
        }
        entry["shard_cert_hash"] = _entry_hash(entry)
        entries.append(entry)
    entries.sort(key=lambda row: str(row.get("day") or ""))
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "step2_shard_cert_registry",
        "created_at_ct": _now_ct(),
        "manifest_path": str(path),
        "manifest_name": path.parent.name,
        "manifest_sha256": tournament_safety._file_sha256(str(path)),
        "compiled_tape_hash": manifest.get("compiled_tape_hash"),
        "array_payload_sha256": manifest.get("array_payload_sha256") or manifest.get("arrays_sha256"),
        "range_linked": bool(manifest.get("range_linked")),
        "certification_hash": certification.get("certification_hash"),
        "certified": certified,
        "day_count": len(entries),
        "entries": entries,
        "registry_hash": tournament_safety.stable_json_hash(entries, length=32),
    }


def write_registry_for_manifest(
    manifest_path: str | os.PathLike[str],
    certification: dict[str, Any],
    *,
    out_dir: str | os.PathLike[str] = OUT_DIR,
) -> dict[str, Any]:
    payload = registry_for_manifest(manifest_path, certification)
    if payload["day_count"] <= 0:
        return {"written": False, "reason": "manifest_has_no_day_shards", "day_count": 0}
    safe_name = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in payload["manifest_name"]).strip("._")
    suffix = str(payload.get("compiled_tape_hash") or payload.get("registry_hash") or "unknown")[:16]
    path = Path(out_dir) / f"step2_day_shard_registry_{safe_name}_{suffix}.json"
    latest = Path(out_dir) / f"step2_day_shard_registry_{safe_name}.LATEST.json"
    payload["path"] = _write_json(path, payload)
    payload["latest_path"] = _write_json(latest, payload)
    return {
        "written": True,
        "path": payload["path"],
        "latest_path": payload["latest_path"],
        "day_count": payload["day_count"],
        "registry_hash": payload["registry_hash"],
        "certified": payload["certified"],
    }
