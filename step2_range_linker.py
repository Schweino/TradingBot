"""Link certified per-day compiled shards into a range manifest.

This is the cheap alternative to recompiling an entire Step 2 range when the
day shards already exist. The linked manifest is intentionally still certified
through the normal lineage path; it just points the loader at day shard arrays
instead of a monolithic arrays.npz.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any

import compiled_tape_lineage
import decision_tape_compiled
import semantic_config
import step2_artifact_identity
import step2_execution_contract
import step2_quote_aware_guard
import tournament_safety


HERE = Path(__file__).resolve().parent
DEFAULT_COMPILED_DIR = HERE / "postmortem" / "backtests" / "compiled_decision_tapes"
SCHEMA_VERSION = 1


def _read_json(path: str | os.PathLike[str], default: Any = None) -> Any:
    try:
        with Path(path).open("r", encoding="utf-8-sig") as f:
            return json.load(f)
    except Exception:
        return default


def _safe_name(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in str(value)).strip("._") or "linked_step2_range"


def _write_json(path: str | os.PathLike[str], payload: dict[str, Any]) -> str:
    return step2_artifact_identity.write_json(path, payload)


def _source_from_shards(shards: list[dict[str, Any]]) -> tuple[list[str], dict[str, Any], dict[str, str | None], dict[str, str | None]]:
    source_paths: list[str] = []
    source_identities: dict[str, Any] = {}
    source_physical: dict[str, str | None] = {}
    source_semantic: dict[str, str | None] = {}
    for shard in shards:
        for path, identity in (shard.get("source_identities") or {}).items():
            if path not in source_identities:
                source_paths.append(path)
            source_identities[path] = identity
            source_physical[path] = identity.get("physical_sha256") if isinstance(identity, dict) else None
            source_semantic[path] = identity.get("semantic_sha256") if isinstance(identity, dict) else None
    return source_paths, source_identities, source_physical, source_semantic


def _merge_source_exit_models(manifests: list[dict[str, Any]]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for manifest in manifests:
        models = manifest.get("source_exit_replay_models")
        if isinstance(models, dict):
            merged.update(models)
    return merged


def _manifest_label(path: Path) -> str:
    return str(path.resolve())


def _assert_compatible_sources(manifests: list[dict[str, Any]]) -> dict[str, Any]:
    if not manifests:
        raise ValueError("no source manifests provided")
    base = manifests[0]
    checks = {
        "indicator_mode": str(base.get("indicator_mode") or "unknown"),
        "ticker_map": base.get("ticker_map") or {},
        "setup_map": base.get("setup_map") or {},
        "feature_names": base.get("feature_names") or list(decision_tape_compiled.fast.FEATURE_NAMES),
        "exit_replay_model": step2_quote_aware_guard.manifest_exit_replay_model(base),
    }
    for manifest in manifests[1:]:
        source = str(manifest.get("manifest_path") or manifest.get("store_dir") or "unknown")
        if str(manifest.get("indicator_mode") or "unknown") != checks["indicator_mode"]:
            raise ValueError(f"incompatible indicator_mode in {source}")
        if (manifest.get("ticker_map") or {}) != checks["ticker_map"]:
            raise ValueError(f"incompatible ticker_map in {source}")
        if (manifest.get("setup_map") or {}) != checks["setup_map"]:
            raise ValueError(f"incompatible setup_map in {source}")
        feature_names = manifest.get("feature_names") or list(decision_tape_compiled.fast.FEATURE_NAMES)
        if list(feature_names) != list(checks["feature_names"]):
            raise ValueError(f"incompatible feature_names in {source}")
        if step2_quote_aware_guard.manifest_exit_replay_model(manifest) != checks["exit_replay_model"]:
            raise ValueError(f"incompatible exit_replay_model in {source}")
    return checks


def _compiled_hash_payload(manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": manifest.get("schema_version"),
        "range_linked": manifest.get("range_linked"),
        "source_semantic_hashes": manifest.get("source_semantic_hashes") or manifest.get("source_hashes"),
        "code_hashes": manifest.get("code_hashes"),
        "lineage_registry_hash": (manifest.get("lineage_registry") or {}).get("registry_hash"),
        "compiled_tape_semantic_hash": manifest.get("compiled_tape_semantic_hash"),
        "exit_replay_model": manifest.get("exit_replay_model"),
        "required_exit_replay_model": manifest.get("required_exit_replay_model"),
        "feature_names": manifest.get("feature_names"),
        "rows": manifest.get("rows"),
        "max_ts": manifest.get("max_ts"),
        "ticker_map": manifest.get("ticker_map"),
        "setup_map": manifest.get("setup_map"),
        "day_map": manifest.get("day_map"),
        "array_payload_sha256": manifest.get("array_payload_sha256") or manifest.get("arrays_sha256"),
    }


def link_range(
    *,
    source_manifest: str | os.PathLike[str],
    out_dir: str | os.PathLike[str] = DEFAULT_COMPILED_DIR,
    name: str = "",
    days: list[str] | None = None,
) -> dict[str, Any]:
    return link_ranges(
        source_manifests=[source_manifest],
        out_dir=out_dir,
        name=name,
        days=days,
    )


def link_ranges(
    *,
    source_manifests: list[str | os.PathLike[str]],
    out_dir: str | os.PathLike[str] = DEFAULT_COMPILED_DIR,
    name: str = "",
    days: list[str] | None = None,
) -> dict[str, Any]:
    sources: list[dict[str, Any]] = []
    all_shards: list[dict[str, Any]] = []
    seen_days: dict[str, str] = {}
    for raw_source in source_manifests:
        source_path = Path(raw_source).resolve()
        source = _read_json(source_path, {}) or {}
        source["manifest_path"] = str(source_path)
        day_shards = source.get("day_shards") or {}
        shards = list(day_shards.get("shards") or [])
        if not shards:
            raise ValueError(f"source manifest has no day_shards: {source_path}")
        sources.append(source)
        for shard in shards:
            day = str(shard.get("day") or "")
            if not day:
                raise ValueError(f"source manifest has day shard without day: {source_path}")
            prior = seen_days.get(day)
            if prior:
                raise ValueError(f"duplicate day shard {day} in {source_path}; already present in {prior}")
            seen_days[day] = _manifest_label(source_path)
            all_shards.append(shard)

    source = sources[0]
    compatibility = _assert_compatible_sources(sources)
    source_paths_input = [str(Path(path).resolve()) for path in source_manifests]
    wanted = set(str(day) for day in (days or []))
    selected = [row for row in all_shards if not wanted or str(row.get("day")) in wanted]
    if wanted and len(selected) != len(wanted):
        have = {str(row.get("day")) for row in selected}
        missing = sorted(wanted - have)
        raise ValueError(f"missing day shards for {missing}")
    selected.sort(key=lambda row: str(row.get("day") or ""))
    if not selected:
        raise ValueError("no day shards selected")

    out_name = name or f"{source.get('name') or Path(source_paths_input[0]).parent.name}_linked"
    store_dir = Path(out_dir) / _safe_name(out_name)
    source_paths, source_identities, source_physical, source_semantic = _source_from_shards(selected)
    cfg = getattr(decision_tape_compiled.replay, "TRADING_CONFIG", {}) or {}
    semantic = semantic_config.report(cfg)
    day_map = {str(row.get("day")): idx for idx, row in enumerate(selected)}
    payload_hash = step2_artifact_identity.stable_json_hash({
        "schema_version": SCHEMA_VERSION,
        "source_manifests": [
            {
                "path": source.get("manifest_path"),
                "compiled_tape_hash": source.get("compiled_tape_hash"),
                "array_payload_sha256": source.get("array_payload_sha256") or source.get("arrays_sha256"),
            }
            for source in sources
        ],
        "day_shards": [
            {
                "day": row.get("day"),
                "rows": row.get("rows"),
                "array_payload_sha256": row.get("array_payload_sha256"),
                "source_semantic_sha256": {
                    path: src.get("semantic_sha256")
                    for path, src in (row.get("source_identities") or {}).items()
                },
            }
            for row in selected
        ],
    })
    manifest = {
        "schema_version": 3,
        "range_linked": True,
        "created_at_epoch": int(time.time()),
        "name": out_name,
        "indicator_mode": compatibility["indicator_mode"],
        "store_dir": str(store_dir),
        "arrays_path": "",
        "rows": sum(int(row.get("rows") or 0) for row in selected),
        "min_ts": min((int(row.get("min_ts") or 0) for row in selected if int(row.get("min_ts") or 0)), default=0),
        "max_ts": max((int(row.get("max_ts") or 0) for row in selected), default=0),
        "ticker_count": int(source.get("ticker_count") or 0),
        "setup_count": int(source.get("setup_count") or 0),
        "day_count": len(selected),
        "ticker_map": compatibility["ticker_map"],
        "setup_map": compatibility["setup_map"],
        "day_map": day_map,
        "live_long_gate_mode": int(source.get("live_long_gate_mode") or 0),
        "live_long_gate_threshold": float(source.get("live_long_gate_threshold") or 0.0),
        "source_manifest": source_paths_input[0],
        "source_manifest_hash": tournament_safety._file_sha256(source_paths_input[0]),
        "source_manifests": source_paths_input,
        "source_manifest_hashes": {
            path: tournament_safety._file_sha256(path)
            for path in source_paths_input
        },
        "source_paths": source_paths,
        "source_hashes": source_physical,
        "source_physical_hashes": source_physical,
        "source_semantic_hashes": source_semantic,
        "source_identities": source_identities,
        "source_exit_replay_models": _merge_source_exit_models(sources),
        "exit_replay_model": compatibility["exit_replay_model"],
        "required_exit_replay_model": step2_quote_aware_guard.REQUIRED_EXIT_REPLAY_MODEL,
        "feature_names": compatibility["feature_names"],
        "feature_count": len(compatibility["feature_names"]),
        "code_hashes": {
            path: tournament_safety._file_sha256(path)
            for path in decision_tape_compiled.CODE_HASH_INPUTS
        },
        "lineage_registry": compiled_tape_lineage.registry_for(decision_tape_compiled.CODE_HASH_INPUTS),
        "numba_available": bool(decision_tape_compiled.COMPILED_AVAILABLE),
        "step2_execution_contract_hash": step2_execution_contract.execution_contract_hash(cfg),
        "step2_execution_contract": step2_execution_contract.execution_contract(cfg),
        "semantic_config_section_hashes": semantic.get("section_hashes"),
        "compiled_tape_semantic_hash": semantic.get("compiled_tape_semantic_hash"),
        "compiled_tape_semantic_sections": list(semantic_config.COMPILED_TAPE_SECTIONS),
        "arrays_sha256": "",
        "array_payload_sha256": payload_hash,
        "day_shards": {
            "schema_version": SCHEMA_VERSION,
            "rows": sum(int(row.get("rows") or 0) for row in selected),
            "day_count": len(selected),
            "day_shards_hash": payload_hash,
            "shards": selected,
        },
        "linker": {
            "source": "step2_range_linker",
            "source_manifest": source_paths_input[0],
            "source_manifests": source_paths_input,
            "selected_days": list(day_map),
        },
    }
    manifest["quote_aware_outcome_gate"] = step2_quote_aware_guard.manifest_gate(manifest)
    manifest["compiled_tape_hash"] = tournament_safety.stable_json_hash(_compiled_hash_payload(manifest), 32)
    manifest_path = store_dir / "manifest.json"
    _write_json(manifest_path, manifest)
    manifest["manifest_path"] = str(manifest_path.resolve())
    return manifest


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Link compiled Step 2 day shards into a cheap range manifest.")
    ap.add_argument("--source-manifest", action="append", required=True)
    ap.add_argument("--out-dir", default=str(DEFAULT_COMPILED_DIR))
    ap.add_argument("--name", default="")
    ap.add_argument("--days", nargs="*", default=[])
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    payload = link_ranges(
        source_manifests=args.source_manifest,
        out_dir=args.out_dir,
        name=args.name,
        days=args.days or None,
    )
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
