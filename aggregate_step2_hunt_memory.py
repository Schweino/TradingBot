"""Aggregate Step 2 hunt slice artifacts into a durable live-beater top100.

This helper is intentionally artifact-only: it does not rescore variants or
change hunt behavior. It lets sliced/segmented hunts keep one cross-run memory
of the best live-beating variants by P/L.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import routed_profile_safety
import step2_void_registry


DEFAULT_BASE_DIR = Path("postmortem") / "backtests" / "step2_three_hour_hunt"


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _leaderboard_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ("leaderboard", "top100", "rows", "variants"):
            rows = payload.get(key)
            if isinstance(rows, list):
                return [row for row in rows if isinstance(row, dict)]
    return []


def _float_value(row: dict[str, Any], *keys: str) -> float:
    for key in keys:
        value = row.get(key)
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                continue
    return 0.0


def _beats_live(row: dict[str, Any]) -> bool:
    explicit = row.get("beats_current_live")
    if isinstance(explicit, bool):
        return explicit
    return _float_value(row, "step2_delta_vs_active", "delta_vs_active") > 0


def _route_key(row: dict[str, Any]) -> str:
    for key in ("route_key", "route_seed"):
        value = row.get(key)
        if isinstance(value, str) and "|" in value:
            return value
    for key in ("winner_mechanism", "lineage"):
        nested = row.get(key)
        if isinstance(nested, dict):
            for nested_key in ("route_key", "route_seed"):
                value = nested.get(nested_key)
                if isinstance(value, str) and "|" in value:
                    return value
    variant = str(row.get("variant") or "")
    if "|" in variant:
        parts = variant.split("_")
        suffix = "_".join(parts[2:]) if len(parts) >= 3 else variant.rsplit("_", 1)[-1]
        if "|" in suffix:
            return suffix
    routes = row.get("routes")
    if isinstance(routes, list) and routes:
        match = (routes[0] or {}).get("match") if isinstance(routes[0], dict) else {}
        if isinstance(match, dict):
            return "|".join(
                str(match.get(key) or "*")
                for key in ("ticker", "setup_type", "session_phase")
            )
    return "unknown|unknown|unknown"


def _source_run_name(row: dict[str, Any]) -> str:
    source = row.get("memory_source_run")
    if isinstance(source, str) and source:
        return Path(source).name
    return ""


def _promotion_distance_passed(row: dict[str, Any]) -> bool:
    distance = row.get("promotion_distance")
    return bool(isinstance(distance, dict) and distance.get("passed") is True)


def _promotion_blockers_digest(row: dict[str, Any]) -> str:
    safety = row.get("memory_routed_profile_safety_gate")
    if isinstance(safety, dict) and safety.get("ok") is False:
        failed = [
            str(check.get("name"))
            for check in safety.get("checks", [])
            if isinstance(check, dict) and check.get("ok") is False
        ]
        if failed:
            return "safety: " + ", ".join(failed[:3])
        return "safety blocked"
    distance = row.get("promotion_distance")
    if isinstance(distance, dict):
        failed = [
            str(gate.get("gate"))
            for gate in distance.get("gates", [])
            if isinstance(gate, dict) and gate.get("passed") is False
        ]
        if failed:
            return "promotion: " + ", ".join(failed[:4])
        if distance.get("passed") is True:
            return "passes promotion distance"
    tags = [str(tag) for tag in row.get("learning_tags", []) if str(tag)]
    weak = [
        tag
        for tag in tags
        if tag in {
            "no_holdout_credit",
            "promotion_weak",
            "small_total_edge",
            "route_narrow",
            "weak_day_consistency",
            "high_overfit_risk",
            "thin_sample",
        }
        or tag.startswith("robustness_flag:")
    ]
    return ", ".join(weak[:4]) if weak else "no explicit blocker captured"


def _candidate_status(row: dict[str, Any]) -> str:
    safety = row.get("memory_routed_profile_safety_gate")
    if isinstance(safety, dict) and safety.get("ok") is False:
        return "blocked"
    if _promotion_ready(row):
        return "promotion_candidate"
    if _promotion_distance_passed(row):
        return "promotion_distance_passed"
    return "raw_only"


def _routed_safety(row: dict[str, Any]) -> dict[str, Any]:
    return routed_profile_safety.evaluate_candidate(row)


def _has_route_audit_identity_mismatch(safety: dict[str, Any]) -> bool:
    checks = safety.get("checks") if isinstance(safety.get("checks"), list) else []
    for check in checks:
        if (
            isinstance(check, dict)
            and check.get("name") == "route_audit_matches_current_routes"
            and check.get("ok") is False
        ):
            return True
    return False


def _sanitize_stale_route_audit(row: dict[str, Any], safety: dict[str, Any]) -> dict[str, Any]:
    if not _has_route_audit_identity_mismatch(safety):
        row["memory_route_audit_identity_status"] = "ok"
        return safety
    for key in ("route_audit", "routed_profile_audit", "routed_profile_safety"):
        row.pop(key, None)
    row["memory_route_audit_identity_status"] = "stale_audit_removed"
    row["memory_stale_route_audit_safety_before_sanitize"] = safety
    return _routed_safety(row)


def _promotion_ready(row: dict[str, Any]) -> bool:
    distance = row.get("promotion_distance") if isinstance(row.get("promotion_distance"), dict) else {}
    safety = row.get("memory_routed_profile_safety_gate") if isinstance(row.get("memory_routed_profile_safety_gate"), dict) else _routed_safety(row)
    return bool(distance.get("passed") is True and safety.get("ok") is True)


def _dedupe_key(row: dict[str, Any]) -> str:
    for key in ("decision_hash", "behavior_key", "config_key", "variant"):
        value = row.get(key)
        if isinstance(value, str) and value:
            return f"{key}:{value}"
    aliases = row.get("behavior_aliases")
    if isinstance(aliases, list) and aliases:
        first = aliases[0]
        if isinstance(first, dict):
            variant = first.get("variant")
            if isinstance(variant, str) and variant:
                return f"alias:{variant}"
    return json.dumps(row, sort_keys=True, default=str)


def _compact_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "memory_rank_by_pnl": row.get("memory_rank_by_pnl"),
        "variant": row.get("variant"),
        "route_key": row.get("route_key"),
        "step2_pnl": row.get("step2_pnl"),
        "step2_delta_vs_active": row.get("step2_delta_vs_active"),
        "promotion_readiness_score": row.get("promotion_readiness_score"),
        "evidence_adjusted_promotion_readiness_score": row.get("evidence_adjusted_promotion_readiness_score"),
        "promotion_distance_passed": row.get("promotion_distance_passed"),
        "safety_status": row.get("safety_status"),
        "candidate_status": row.get("candidate_status"),
        "promotion_blockers_digest": row.get("promotion_blockers_digest"),
        "source_run_name": row.get("source_run_name"),
    }


def _lane_summaries(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_route: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_route.setdefault(str(row.get("route_key") or "unknown|unknown|unknown"), []).append(row)
    summaries = []
    for route, lane_rows in by_route.items():
        lane_rows = sorted(lane_rows, key=lambda item: _float_value(item, "step2_pnl"), reverse=True)
        top = lane_rows[0] if lane_rows else {}
        promotion_ready = [row for row in lane_rows if row.get("candidate_status") == "promotion_candidate"]
        blocked = [row for row in lane_rows if row.get("candidate_status") == "blocked"]
        source_runs = {str(row.get("source_run_name") or "") for row in lane_rows if row.get("source_run_name")}
        summaries.append(
            {
                "route_key": route,
                "count": len(lane_rows),
                "source_run_count": len(source_runs),
                "best_variant": top.get("variant"),
                "best_pnl": top.get("step2_pnl"),
                "best_delta_vs_live": top.get("step2_delta_vs_active"),
                "best_readiness": top.get("promotion_readiness_score"),
                "promotion_candidate_count": len(promotion_ready),
                "blocked_count": len(blocked),
                "status": (
                    "expand"
                    if len(lane_rows) >= 3 and _float_value(top, "step2_delta_vs_active") > 1500
                    else "validate"
                    if len(lane_rows) >= 2
                    else "probe"
                ),
                "learning_label": (
                    "new_contender_lane_needs_validation"
                    if len(source_runs) <= 2 and _float_value(top, "step2_delta_vs_active") > 1500
                    else "repeatable_lane"
                    if len(source_runs) >= 3
                    else "single_run_signal"
                ),
            }
        )
    return sorted(
        summaries,
        key=lambda item: (float(item.get("best_pnl") or 0.0), int(item.get("count") or 0)),
        reverse=True,
    )


def _best(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    return _compact_row(rows[0]) if rows else None


def _run_dirs(base_dir: Path, prefixes: list[str]) -> list[Path]:
    if not base_dir.exists():
        return []
    dirs = [path for path in base_dir.iterdir() if path.is_dir()]
    if not prefixes:
        return sorted(dirs, key=lambda path: path.stat().st_mtime)
    matched: list[Path] = []
    for path in dirs:
        if any(path.name.startswith(prefix) for prefix in prefixes):
            matched.append(path)
    return sorted(matched, key=lambda path: path.stat().st_mtime)


def aggregate_top100(base_dir: Path, prefixes: list[str], limit: int = 100) -> dict[str, Any]:
    best_by_key: dict[str, dict[str, Any]] = {}
    source_dirs = []
    source_run_health = []
    skipped_files = 0
    rows_seen = 0
    live_rows_seen = 0

    for run_dir in _run_dirs(base_dir, prefixes):
        source_path = run_dir / "promotion_survival_top100.json"
        if not source_path.exists():
            continue
        source_dirs.append(str(run_dir))
        source_run_health.append(
            {
                "run_name": run_dir.name,
                "run_dir": str(run_dir),
                "has_promotion_survival_top100": True,
                "has_final_summary": (run_dir / "final_summary.json").exists(),
                "has_runtime_handoff_controls": (run_dir / "runtime_handoff_controls.json").exists(),
                "artifact_status": "complete" if (run_dir / "final_summary.json").exists() else "partial_valid_artifact",
            }
        )
        try:
            rows = _leaderboard_rows(_load_json(source_path))
        except (OSError, json.JSONDecodeError):
            skipped_files += 1
            continue
        for row in rows:
            rows_seen += 1
            candidate_probe = dict(row)
            candidate_probe["hunt_source_path"] = str(source_path)
            if step2_void_registry.is_payload_contaminated(candidate_probe, source_path=source_path):
                continue
            if not _beats_live(row):
                continue
            live_rows_seen += 1
            enriched = step2_void_registry.annotate_payload(dict(row), source_path=source_path)
            enriched["memory_source_run"] = str(run_dir)
            enriched["memory_source_file"] = str(source_path)
            enriched["source_run_name"] = run_dir.name
            safety = _routed_safety(enriched)
            enriched["memory_routed_profile_safety_gate"] = _sanitize_stale_route_audit(enriched, safety)
            enriched["route_key"] = _route_key(enriched)
            enriched["promotion_distance_passed"] = _promotion_distance_passed(enriched)
            enriched["safety_status"] = "ok" if enriched["memory_routed_profile_safety_gate"].get("ok") is True else "blocked"
            enriched["promotion_blockers_digest"] = _promotion_blockers_digest(enriched)
            enriched["candidate_status"] = _candidate_status(enriched)
            key = _dedupe_key(enriched)
            current = best_by_key.get(key)
            if current is None:
                best_by_key[key] = enriched
                continue
            if _float_value(enriched, "step2_pnl") > _float_value(current, "step2_pnl"):
                best_by_key[key] = enriched

    leaderboard = sorted(
        best_by_key.values(),
        key=lambda row: (
            _float_value(row, "step2_pnl"),
            _float_value(row, "step2_delta_vs_active", "delta_vs_active"),
            _float_value(row, "evidence_adjusted_promotion_readiness_score"),
            _float_value(row, "promotion_readiness_score"),
        ),
        reverse=True,
    )[:limit]

    for index, row in enumerate(leaderboard, start=1):
        row["memory_rank_by_pnl"] = index
        row["source_run_name"] = _source_run_name(row)
        row["route_key"] = _route_key(row)
        row["promotion_distance_passed"] = _promotion_distance_passed(row)
        row["safety_status"] = "ok" if (row.get("memory_routed_profile_safety_gate") or {}).get("ok") is True else "blocked"
        row["promotion_blockers_digest"] = _promotion_blockers_digest(row)
        row["candidate_status"] = _candidate_status(row)

    promotion_ready = [
        row
        for row in leaderboard
        if _promotion_ready(row)
    ]
    sanitized_rows = [
        row
        for row in leaderboard
        if row.get("memory_route_audit_identity_status") == "stale_audit_removed"
    ]
    blocked_safety_rows = [
        row
        for row in leaderboard
        if isinstance(row.get("memory_routed_profile_safety_gate"), dict)
        and row["memory_routed_profile_safety_gate"].get("ok") is False
    ]
    clean_rows = [row for row in leaderboard if row.get("safety_status") == "ok"]
    raw_only_rows = [row for row in leaderboard if row.get("candidate_status") == "raw_only"]
    by_lane = _lane_summaries(leaderboard)
    compact_top100 = [_compact_row(row) for row in leaderboard]
    compact_top10 = compact_top100[:10]
    promotion_queue = [_compact_row(row) for row in promotion_ready]
    partial_runs = [row for row in source_run_health if row.get("artifact_status") == "partial_valid_artifact"]
    rejected_non_live_rows = max(0, rows_seen - live_rows_seen)
    alias_or_duplicate_collapse_count = max(0, live_rows_seen - len(best_by_key))
    rank1 = compact_top10[0] if compact_top10 else None
    return {
        "schema_version": 2,
        "source": "aggregate_step2_hunt_memory",
        "base_dir": str(base_dir),
        "prefixes": prefixes,
        "source_run_count": len(source_dirs),
        "source_runs": source_dirs,
        "source_run_health": source_run_health,
        "partial_valid_source_run_count": len(partial_runs),
        "rows_seen": rows_seen,
        "live_rows_seen": live_rows_seen,
        "rejected_non_live_rows": rejected_non_live_rows,
        "deduped_live_rows": len(best_by_key),
        "alias_or_duplicate_collapse_count": alias_or_duplicate_collapse_count,
        "top100_count": len(leaderboard),
        "promotion_ready_in_top100": len(promotion_ready),
        "route_audit_sanitized_in_top100": len(sanitized_rows),
        "routed_safety_blocked_in_top100": len(blocked_safety_rows),
        "skipped_files": skipped_files,
        "sort": "step2_pnl_desc_then_delta_desc",
        "rank1": rank1.get("variant") if rank1 else None,
        "rank1_pnl": rank1.get("step2_pnl") if rank1 else None,
        "rank1_delta_vs_live": rank1.get("step2_delta_vs_active") if rank1 else None,
        "clean_rank1": _best(clean_rows),
        "best_promotion_ready": _best(promotion_ready),
        "best_raw_only": _best(raw_only_rows),
        "best_blocked": _best(blocked_safety_rows),
        "memory_health": {
            "top100_full": len(leaderboard) >= min(limit, 100),
            "all_retained_beat_live": rejected_non_live_rows == rows_seen - live_rows_seen and all(_beats_live(row) for row in leaderboard),
            "partial_valid_source_run_count": len(partial_runs),
            "safety_blocked_count": len(blocked_safety_rows),
            "promotion_ready_count": len(promotion_ready),
            "route_audit_sanitized_count": len(sanitized_rows),
            "dedupe_collapse_count": alias_or_duplicate_collapse_count,
            "recommended_next_action": (
                "review_best_promotion_ready"
                if promotion_ready
                else "validate_clean_rank1"
                if clean_rows
                else "fix_safety_blockers_before_review"
            ),
        },
        "lane_summary": by_lane,
        "compact_top100": compact_top100,
        "compact_top10": compact_top10,
        "promotion_queue": promotion_queue,
        "leaderboard": leaderboard,
        "top10": leaderboard[:10],
    }


def _write_companion_outputs(out_path: Path, result: dict[str, Any]) -> dict[str, str]:
    companions = {
        "compact": out_path.with_name(out_path.stem + "_compact.json"),
        "by_lane": out_path.with_name(out_path.stem + "_by_lane.json"),
        "promotion_queue": out_path.with_name(out_path.stem + "_promotion_queue.json"),
        "health": out_path.with_name(out_path.stem + "_health.json"),
    }
    payloads = {
        "compact": {
            "schema_version": result.get("schema_version"),
            "source": result.get("source"),
            "rank1": result.get("rank1"),
            "rank1_pnl": result.get("rank1_pnl"),
            "top100_count": result.get("top100_count"),
            "promotion_ready_in_top100": result.get("promotion_ready_in_top100"),
            "routed_safety_blocked_in_top100": result.get("routed_safety_blocked_in_top100"),
            "top100": result.get("compact_top100") or [],
        },
        "by_lane": {
            "schema_version": result.get("schema_version"),
            "source": result.get("source"),
            "lanes": result.get("lane_summary") or [],
        },
        "promotion_queue": {
            "schema_version": result.get("schema_version"),
            "source": result.get("source"),
            "queue_count": len(result.get("promotion_queue") or []),
            "queue": result.get("promotion_queue") or [],
        },
        "health": {
            "schema_version": result.get("schema_version"),
            "source": result.get("source"),
            "memory_health": result.get("memory_health") or {},
            "source_run_health": result.get("source_run_health") or [],
        },
    }
    written = {}
    for key, path in companions.items():
        with path.open("w", encoding="utf-8") as handle:
            json.dump(payloads[key], handle, indent=2, sort_keys=True)
        written[key] = str(path)
    return written


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-dir", type=Path, default=DEFAULT_BASE_DIR)
    parser.add_argument("--prefix", action="append", default=[])
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()

    result = aggregate_top100(args.base_dir.resolve(), args.prefix, args.limit)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8") as handle:
        json.dump(result, handle, indent=2, sort_keys=True)
    companion_outputs = _write_companion_outputs(args.out, result)
    print(
        json.dumps(
            {
                "ok": True,
                "out": str(args.out),
                "companion_outputs": companion_outputs,
                "source_run_count": result["source_run_count"],
                "top100_count": result["top100_count"],
                "promotion_ready_in_top100": result["promotion_ready_in_top100"],
                "route_audit_sanitized_in_top100": result["route_audit_sanitized_in_top100"],
                "routed_safety_blocked_in_top100": result["routed_safety_blocked_in_top100"],
                "rejected_non_live_rows": result["rejected_non_live_rows"],
                "alias_or_duplicate_collapse_count": result["alias_or_duplicate_collapse_count"],
                "partial_valid_source_run_count": result["partial_valid_source_run_count"],
                "rank1": result["rank1"],
                "rank1_pnl": result["rank1_pnl"],
                "clean_rank1": (result.get("clean_rank1") or {}).get("variant"),
                "best_promotion_ready": (result.get("best_promotion_ready") or {}).get("variant"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
