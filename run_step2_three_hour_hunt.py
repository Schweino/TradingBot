"""Run Step 2 variant hunts for a fixed wall-clock window.

This wrapper keeps the repo's existing Step 2 hunter coordinator in charge of
actual scoring, while adding a durable three-hour loop and a running top-100
leaderboard ranked by Step 2 P/L.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import step2_hunt_intelligence as hunt_intel
import step2_learning_db
import step2_online_learning
import candidate_decision_brief
import candidate_robustness_report
import routed_profile_safety


HERE = Path(__file__).resolve().parent
CT = ZoneInfo("America/Chicago")
DEFAULT_OUT = HERE / "postmortem" / "backtests" / "step2_three_hour_hunt"
WORLD_CLASS_META_KEYS = tuple(getattr(step2_online_learning, "WORLD_CLASS_META_LEARNING_ARTIFACTS", ()))
ELITE_LEARNING_KEYS = tuple(getattr(step2_online_learning, "ELITE_LEARNING_ARTIFACTS", ())) + ("elite_learning_system_summary",)
PROOF_LEARNING_KEYS = tuple(getattr(step2_online_learning, "PROOF_LEARNING_ARTIFACTS", ())) + ("proof_learning_system_summary",)
CLOSED_LOOP_CONTROL_KEYS = tuple(getattr(step2_online_learning, "CLOSED_LOOP_CONTROL_ARTIFACTS", ())) + ("closed_loop_control_learning_summary",)
WORLD_MODEL_NERVOUS_KEYS = tuple(getattr(step2_online_learning, "WORLD_MODEL_NERVOUS_SYSTEM_ARTIFACTS", ())) + ("world_model_nervous_system_summary",)
ORCHESTRATION_LEARNING_KEYS = tuple(getattr(step2_online_learning, "ORCHESTRATION_LEARNING_ARTIFACTS", ())) + ("orchestration_learning_summary",)
FITNESS_SELECTION_KEYS = tuple(getattr(step2_online_learning, "FITNESS_SELECTION_ARTIFACTS", ())) + ("fitness_selection_summary",)
_RANKINGS_CACHE: dict[tuple[Any, ...], dict[str, Any]] = {}
EVIDENCE_TIER_RANK = {
    "gold": 5,
    "silver": 4,
    "bronze": 3,
    "evidence_incomplete": 2,
    "blocked": 1,
    "unknown": 0,
}


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


def _stamp() -> str:
    return datetime.now(CT).strftime("%Y%m%d_%H%M%S")


def _run_dir_sort_key(path: Path) -> tuple[int, float, str]:
    match = re.search(r"(20\d{6})_(\d{6})", path.name)
    if match:
        try:
            return (1, datetime.strptime("".join(match.groups()), "%Y%m%d%H%M%S").timestamp(), path.name)
        except ValueError:
            pass
    try:
        mtime = path.stat().st_mtime
    except OSError:
        mtime = 0.0
    return (0, mtime, path.name)


def _write_json(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n"
    try:
        if path.exists() and path.read_text(encoding="utf-8") == rendered:
            return str(path.resolve())
    except Exception:
        pass
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(rendered, encoding="utf-8")
    os.replace(tmp, path)
    return str(path.resolve())


def _write_compact_json(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str) + "\n"
    try:
        if path.exists() and path.read_text(encoding="utf-8") == rendered:
            return str(path.resolve())
    except Exception:
        pass
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(rendered, encoding="utf-8")
    os.replace(tmp, path)
    return str(path.resolve())


def _tail_text(path: Path, max_chars: int = 4000) -> str:
    try:
        if not path.exists():
            return ""
        size = path.stat().st_size
        with path.open("rb") as handle:
            handle.seek(max(0, size - max_chars * 4))
            data = handle.read()
        return data.decode("utf-8", errors="replace")[-max_chars:]
    except Exception:
        return ""


def _payload_hash(payload: Any, length: int = 16) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length]


def _terminate_process_tree(proc: subprocess.Popen, *, grace_sec: float = 2.0) -> None:
    if proc.poll() is not None:
        return
    if os.name == "nt" and proc.pid:
        try:
            subprocess.run(
                ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
            proc.wait(timeout=max(0.1, float(grace_sec)))
            return
        except Exception:
            pass
    try:
        proc.terminate()
        proc.wait(timeout=max(0.1, float(grace_sec)))
        return
    except Exception:
        pass
    try:
        proc.kill()
    except Exception:
        pass


def _summary_paths(run_dir: Path) -> list[Path]:
    wanted = {"summary.json", "checkpoint.json", "coordinator_summary.json"}
    return sorted(
        [path for path in run_dir.rglob("*.json") if path.name in wanted],
        key=lambda path: str(path),
    )


def _scorer_summary_paths(run_dir: Path) -> list[Path]:
    """Return one true scorer output per hunter run, avoiding coordinator/checkpoint duplicates."""
    all_paths = _summary_paths(run_dir)
    manifests = sorted(run_dir.rglob("scored_variants.jsonl"), key=lambda path: str(path))
    if manifests:
        return manifests
    summaries = [path for path in all_paths if path.name == "summary.json"]
    checkpoints = [
        path
        for path in all_paths
        if path.name == "checkpoint.json" and not path.with_name("summary.json").exists()
    ]
    scorer_paths = sorted(summaries + checkpoints, key=lambda path: str(path))
    if scorer_paths:
        return scorer_paths
    return [path for path in all_paths if path.name == "coordinator_summary.json"]


def collect_rankings(args: argparse.Namespace, run_dir: Path) -> dict[str, Any]:
    paths = _scorer_summary_paths(run_dir)
    diagnostic_paths = _summary_paths(run_dir)
    cache_key = (
        str(run_dir.resolve()),
        bool(args.live_only),
        bool(args.behavioral_dedupe),
        int(args.leaderboard_limit),
        tuple((str(path.resolve()), path.stat().st_mtime_ns if path.exists() else 0, path.stat().st_size if path.exists() else 0) for path in paths),
        tuple((str(path.resolve()), path.stat().st_mtime_ns if path.exists() else 0, path.stat().st_size if path.exists() else 0) for path in diagnostic_paths),
    )
    cached = _RANKINGS_CACHE.get(cache_key)
    if cached is not None:
        return copy.deepcopy(cached)
    rankings = hunt_intel.collect_ranked_from_paths(
        paths,
        live_only=bool(args.live_only),
        behavioral_dedupe=bool(args.behavioral_dedupe),
        limit=int(args.leaderboard_limit),
    )
    diagnostics = hunt_intel.collect_ranked_from_paths(
        diagnostic_paths,
        live_only=False,
        behavioral_dedupe=False,
        limit=max(int(args.leaderboard_limit), 500),
    )
    rankings["diagnostic_rows"] = diagnostics.get("decorated_rows") or []
    rankings["scorer_source_paths"] = [str(path.resolve()) for path in paths]
    rankings["diagnostic_source_paths"] = [str(path.resolve()) for path in diagnostic_paths]
    for path in paths:
        payload = hunt_intel.read_json(path, {}) or {}
        if isinstance(payload.get("active"), dict) and payload.get("active"):
            rankings["active_row"] = payload.get("active")
            break
    if "active_row" not in rankings:
        for path in diagnostic_paths:
            payload = hunt_intel.read_json(path, {}) or {}
            if isinstance(payload.get("active"), dict) and payload.get("active"):
                rankings["active_row"] = payload.get("active")
                break
    for path in paths:
        payload = hunt_intel.read_json(path, {}) or {}
        if payload.get("active_step2_pnl") is not None:
            rankings["active_step2_pnl"] = payload.get("active_step2_pnl")
            break
    if "active_step2_pnl" not in rankings:
        for path in diagnostic_paths:
            payload = hunt_intel.read_json(path, {}) or {}
            if payload.get("active_step2_pnl") is not None:
                rankings["active_step2_pnl"] = payload.get("active_step2_pnl")
                break
    _RANKINGS_CACHE.clear()
    _RANKINGS_CACHE[cache_key] = copy.deepcopy(rankings)
    return rankings


def _seed_json_args(extra_seed_json: list[str]) -> list[str]:
    candidates = [
        HERE / "postmortem" / "backtests" / "step2_adaptive_hunter" / "current_cache_rescore_top250_20260509.json",
    ]
    out: list[str] = []
    for path in candidates:
        if path.exists():
            out.extend(["--seed-json", str(path.resolve())])
    for raw in extra_seed_json:
        if Path(raw).exists():
            out.extend(["--seed-json", str(Path(raw).resolve())])
    return out


def _shell_quote_arg(value: Any) -> str:
    """Quote a CLI token for copy/paste in PowerShell-safe command strings."""
    text = str(value)
    if text and all(ch.isalnum() or ch in "-_./:\\" for ch in text):
        return text
    return "'" + text.replace("'", "''") + "'"


def _shell_command_string(command: list[Any]) -> str:
    return " ".join(_shell_quote_arg(part) for part in command)


def _hunter_family(row: dict[str, Any]) -> str:
    variant = str(row.get("variant") or "")
    if variant.startswith("router_"):
        return "router"
    if variant.startswith("adapt"):
        return "adaptive"
    if variant.startswith("hill") or variant.startswith("local"):
        return "local"
    return "unknown"


def _choose_hunter(args: argparse.Namespace, cycle_idx: int, rankings: dict[str, Any] | None = None) -> str:
    if not args.dynamic_budget or not rankings:
        return args.hunters[cycle_idx % len(args.hunters)]
    rows = list(rankings.get("raw_leaderboard") or [])
    if not rows:
        return args.hunters[cycle_idx % len(args.hunters)]
    counts: dict[str, int] = {}
    for row in rows[:50]:
        family = _hunter_family(row)
        counts[family] = counts.get(family, 0) + int(row.get("behavior_alias_count") or 1)
    dominant = max(counts, key=counts.get) if counts else ""
    total = sum(counts.values()) or 1
    if dominant == "router" and counts.get("router", 0) / total >= 0.60:
        if cycle_idx % 9 == 4 and "local" in args.hunters:
            return "local"
        if cycle_idx % 13 == 7 and "adaptive" in args.hunters:
            return "adaptive"
        return "router"
    if dominant in args.hunters and counts.get(dominant, 0) / total >= 0.55:
        if cycle_idx % 5 != 3:
            return dominant
    return args.hunters[cycle_idx % len(args.hunters)]


def _promotion_evidence_repair_queue(rows: list[dict[str, Any]], *, limit: int = 100) -> dict[str, Any]:
    grouped: dict[tuple[str, tuple[str, ...]], dict[str, Any]] = {}
    for row in rows:
        tags = set(str(tag) for tag in (row.get("learning_tags") or hunt_intel.candidate_learning_tags(row)))
        readiness = float(row.get("promotion_readiness_score") or 0.0)
        gaps = []
        if "no_holdout_credit" in tags or "thin_holdout_edge" in tags:
            gaps.append("holdout_evidence")
        if "route_narrow" in tags:
            gaps.append("route_breadth")
        if "small_total_edge" in tags:
            gaps.append("edge_size")
        if "weak_day_consistency" in tags:
            gaps.append("day_consistency")
        if "promotion_weak" in tags or readiness < 70.0:
            gaps.append("promotion_readiness")
        if not gaps:
            continue
        route = hunt_intel.route_key_from_row(row)
        gap_key = tuple(sorted(set(gaps)))
        key = (route, gap_key)
        task = grouped.setdefault(key, {
            "variant": row.get("variant"),
            "variants": [],
            "candidate_count": 0,
            "route_key": route,
            "step2_pnl": row.get("step2_pnl"),
            "delta_vs_active": row.get("step2_delta_vs_active"),
            "promotion_readiness_score": readiness,
            "best_step2_pnl": row.get("step2_pnl"),
            "best_delta_vs_active": row.get("step2_delta_vs_active"),
            "best_promotion_readiness_score": readiness,
            "gaps": list(gap_key),
            "recommended_next_probe": (
                "holdout_replay_or_day_split" if "holdout_evidence" in gaps
                else "route_breadth_expansion" if "route_breadth" in gaps
                else "edge_size_amplification"
            ),
            "learning_tags": [],
        })
        task["candidate_count"] = int(task.get("candidate_count") or 0) + 1
        if len(task["variants"]) < 10:
            task["variants"].append(row.get("variant"))
        task["learning_tags"] = sorted(set(task.get("learning_tags") or []) | tags)
        if float(row.get("step2_pnl") or 0.0) > float(task.get("best_step2_pnl") or 0.0):
            task["variant"] = row.get("variant")
            task["best_step2_pnl"] = row.get("step2_pnl")
            task["best_delta_vs_active"] = row.get("step2_delta_vs_active")
            task["best_promotion_readiness_score"] = readiness
        task["promotion_readiness_score"] = max(float(task.get("promotion_readiness_score") or 0.0), readiness)
    tasks = list(grouped.values())
    tasks.sort(
        key=lambda item: (
            -int(item.get("candidate_count") or 0),
            len(item.get("gaps") or []),
            -float(item.get("promotion_readiness_score") or 0.0),
            -float(item.get("best_delta_vs_active") or item.get("delta_vs_active") or 0.0),
        )
    )
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "description": "Route/setup grouped live-beating candidates that need targeted evidence repair before promotion review.",
        "task_count": len(tasks),
        "tasks": tasks[:limit],
        "focus_routes": list(dict.fromkeys(task.get("route_key") for task in tasks if task.get("route_key")))[:12],
    }


def _evidence_adjusted_tags(row: dict[str, Any], evidence: dict[str, Any] | None = None) -> set[str]:
    tags = set(str(tag) for tag in (row.get("learning_tags") or hunt_intel.candidate_learning_tags(row)))
    evidence = evidence or {}
    holdout = evidence.get("train_holdout") if isinstance(evidence.get("train_holdout"), dict) else {}
    red_flags = set(str(flag) for flag in (evidence.get("red_flags") or []))
    positive_holdout = (
        holdout.get("holdout_delta_vs_active") is not None
        and float(holdout.get("holdout_delta_vs_active") or 0.0) > 0.0
        and float(holdout.get("holdout_pnl") or 0.0) > 0.0
    )
    if positive_holdout:
        tags.discard("no_holdout_credit")
        tags.add("positive_holdout_but_not_promotion_grade")
    if "weak_day_consistency" in red_flags:
        tags.add("weak_day_consistency")
    return tags


def _readiness_component_breakdown(row: dict[str, Any], evidence: dict[str, Any] | None = None) -> dict[str, Any]:
    evidence = evidence or {}
    tags = _evidence_adjusted_tags(row, evidence)
    base = float(row.get("promotion_readiness_score") or 0.0)
    holdout = evidence.get("train_holdout") if isinstance(evidence.get("train_holdout"), dict) else {}
    day = evidence.get("day_profile") if isinstance(evidence.get("day_profile"), dict) else {}
    robustness = float(evidence.get("robustness_score") or 0.0)
    red_flags = set(str(flag) for flag in (evidence.get("red_flags") or []))
    holdout_delta = holdout.get("holdout_delta_vs_active")
    holdout_pnl = holdout.get("holdout_pnl")
    positive_holdout = (
        holdout_delta is not None
        and float(holdout_delta or 0.0) > 0.0
        and float(holdout_pnl or 0.0) > 0.0
    )
    promotion_grade_holdout = positive_holdout and robustness >= 55.0 and "weak_day_consistency" not in red_flags
    route_breadth_credit = row.get("route_breadth_credit") if isinstance(row.get("route_breadth_credit"), dict) else {}
    route_breadth_ok = bool(route_breadth_credit.get("passed")) or "route_broader" in tags
    day_rate = day.get("beats_active_day_rate")
    day_points = 0.0
    if day_rate is not None:
        day_points = max(0.0, min(10.0, (float(day_rate or 0.0) - 0.50) * 25.0))
    elif "day_consistent" in tags:
        day_points = 6.0
    components = {
        "base": round(base, 4),
        "positive_holdout": 12.0 if promotion_grade_holdout else 8.0 if positive_holdout else 0.0,
        "robustness_floor": 10.0 if robustness >= 55.0 else 4.0 if robustness >= 45.0 else 0.0,
        "robustness_margin": max(0.0, min(6.0, (robustness - 55.0) * 0.7)) if robustness >= 55.0 else 0.0,
        "route_breadth": 6.0 if route_breadth_ok else 0.0,
        "day_consistency": day_points,
        "ticker_balance": 3.0 if "ticker_balanced" in tags else 0.0,
        "side_balance": 3.0 if "side_balanced" in tags else 0.0,
        "sample": 3.0 if "sample_ok" in tags else 0.0,
    }
    penalties = {
        "weak_day_consistency": 8.0 if "weak_day_consistency" in red_flags or "weak_day_consistency" in tags else 0.0,
        "thin_sample": 3.0 if "thin_sample" in tags else 0.0,
        "route_narrow": 4.0 if "route_narrow" in tags and not route_breadth_ok else 0.0,
        "missing_holdout": 6.0 if not positive_holdout and "no_holdout_credit" in tags else 0.0,
    }
    score = sum(components.values()) - sum(penalties.values())
    score = round(max(0.0, min(100.0, score)), 4)
    gap = round(max(0.0, 45.0 - score), 4)
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "variant": row.get("variant"),
        "route_key": hunt_intel.route_key_from_row(row),
        "score": score,
        "gap_to_floor": gap,
        "floor": 45.0,
        "passed_floor": score >= 45.0,
        "components": {key: round(float(value), 4) for key, value in components.items()},
        "penalties": {key: round(float(value), 4) for key, value in penalties.items() if float(value or 0.0) > 0.0},
        "inputs": {
            "raw_promotion_readiness_score": base,
            "robustness_score": robustness if evidence else None,
            "holdout_delta_vs_active": holdout_delta,
            "holdout_pnl": holdout_pnl,
            "positive_holdout": positive_holdout,
            "promotion_grade_holdout": promotion_grade_holdout,
            "beats_active_day_rate": day_rate,
            "route_breadth_passed": route_breadth_ok,
            "learning_tags": sorted(tags),
            "red_flags": sorted(red_flags),
        },
        "human_summary": (
            "readiness clears the 45 floor"
            if score >= 45.0
            else f"readiness is {score}, needs {gap} more points; biggest missing pieces are "
            + ", ".join(
                key
                for key, value in sorted(components.items(), key=lambda item: item[1])
                if key != "base" and float(value or 0.0) <= 0.0
            )[:120]
        ),
    }


def _evidence_adjusted_readiness(row: dict[str, Any], evidence: dict[str, Any] | None = None) -> float:
    if not evidence:
        return round(max(0.0, min(100.0, float(row.get("promotion_readiness_score") or 0.0))), 4)
    return float(_readiness_component_breakdown(row, evidence).get("score") or 0.0)


def _readiness_component_diagnostics(
    rows: list[dict[str, Any]],
    evidence_by_variant: dict[str, dict[str, Any]],
    *,
    limit: int = 100,
) -> dict[str, Any]:
    breakdowns = []
    missing_counter: Counter[str] = Counter()
    for row in rows[:limit]:
        evidence = evidence_by_variant.get(str(row.get("variant") or "")) or {}
        breakdown = _readiness_component_breakdown(row, evidence)
        breakdown["step2_pnl"] = row.get("step2_pnl")
        breakdown["delta_vs_active"] = row.get("step2_delta_vs_active")
        breakdown["promotion_distance"] = _promotion_distance(row, evidence)
        for key, value in (breakdown.get("components") or {}).items():
            if key != "base" and float(value or 0.0) <= 0.0:
                missing_counter[key] += 1
        for key in (breakdown.get("penalties") or {}):
            missing_counter[f"penalty:{key}"] += 1
        breakdowns.append(breakdown)
    breakdowns.sort(
        key=lambda item: (
            float(item.get("gap_to_floor") or 999.0),
            -float(item.get("delta_vs_active") or 0.0),
        )
    )
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "description": "Human-readable decomposition of why each live beater does or does not clear the promotion readiness floor.",
        "candidate_count": len(breakdowns),
        "floor": 45.0,
        "passed_floor_count": sum(1 for item in breakdowns if item.get("passed_floor")),
        "top_missing_components": [
            {"component": key, "count": count}
            for key, count in missing_counter.most_common()
        ],
        "closest_to_floor": breakdowns[:25],
        "breakdowns": breakdowns,
    }


def _repair_lane_performance_scoreboard(
    rows: list[dict[str, Any]],
    evidence_by_variant: dict[str, dict[str, Any]],
    *,
    limit: int = 100,
) -> dict[str, Any]:
    lanes: dict[str, dict[str, Any]] = {}
    for row in rows[:limit]:
        lineage = row.get("lineage") if isinstance(row.get("lineage"), dict) else {}
        lane = str(lineage.get("mutation_lane") or lineage.get("mutation_reason") or "unknown")
        evidence = evidence_by_variant.get(str(row.get("variant") or "")) or {}
        bar = _promotion_minimum_bar(row, evidence)
        distance = _promotion_distance(row, evidence)
        bucket = lanes.setdefault(lane, {
            "lane": lane,
            "candidate_count": 0,
            "live_beater_count": 0,
            "promotion_floor_pass_count": 0,
            "promotion_ready_count": 0,
            "total_delta": 0.0,
            "total_readiness": 0.0,
            "total_robustness": 0.0,
            "best_candidate": {},
            "failure_counts": Counter(),
        })
        bucket["candidate_count"] += 1
        if float(row.get("step2_delta_vs_active") or 0.0) > 0.0:
            bucket["live_beater_count"] += 1
        if float(bar.get("promotion_readiness_score") or 0.0) >= 45.0:
            bucket["promotion_floor_pass_count"] += 1
        if distance.get("passed"):
            bucket["promotion_ready_count"] += 1
        bucket["total_delta"] += float(row.get("step2_delta_vs_active") or 0.0)
        bucket["total_readiness"] += float(bar.get("promotion_readiness_score") or 0.0)
        bucket["total_robustness"] += float(bar.get("robustness_score") or 0.0)
        for failure in bar.get("failures") or []:
            bucket["failure_counts"][str(failure)] += 1
        best = bucket.get("best_candidate") if isinstance(bucket.get("best_candidate"), dict) else {}
        best_key = (
            int((best.get("promotion_distance") or {}).get("open_gate_count") if (best.get("promotion_distance") or {}).get("open_gate_count") is not None else 99),
            float((best.get("promotion_distance") or {}).get("distance_points") if (best.get("promotion_distance") or {}).get("distance_points") is not None else 1e9),
            -float(best.get("delta_vs_active") or 0.0),
        )
        candidate_key = (
            int(distance.get("open_gate_count") if distance.get("open_gate_count") is not None else 99),
            float(distance.get("distance_points") if distance.get("distance_points") is not None else 1e9),
            -float(row.get("step2_delta_vs_active") or 0.0),
        )
        if not best or candidate_key < best_key:
            bucket["best_candidate"] = {
                "variant": row.get("variant"),
                "route_key": hunt_intel.route_key_from_row(row),
                "step2_pnl": row.get("step2_pnl"),
                "delta_vs_active": row.get("step2_delta_vs_active"),
                "promotion_distance": distance,
                "promotion_minimum_bar": bar,
            }
    summaries = []
    for bucket in lanes.values():
        count = max(1, int(bucket.get("candidate_count") or 0))
        summaries.append({
            "lane": bucket.get("lane"),
            "candidate_count": bucket.get("candidate_count"),
            "live_beater_count": bucket.get("live_beater_count"),
            "promotion_floor_pass_count": bucket.get("promotion_floor_pass_count"),
            "promotion_ready_count": bucket.get("promotion_ready_count"),
            "avg_delta_vs_active": round(float(bucket.get("total_delta") or 0.0) / count, 4),
            "avg_evidence_adjusted_readiness": round(float(bucket.get("total_readiness") or 0.0) / count, 4),
            "avg_robustness_score": round(float(bucket.get("total_robustness") or 0.0) / count, 4),
            "failure_counts": dict(bucket.get("failure_counts") or {}),
            "best_candidate": bucket.get("best_candidate") or {},
        })
    summaries.sort(
        key=lambda item: (
            int(item.get("promotion_ready_count") or 0),
            int(item.get("promotion_floor_pass_count") or 0),
            float(item.get("avg_evidence_adjusted_readiness") or 0.0),
            float(item.get("avg_delta_vs_active") or 0.0),
        ),
        reverse=True,
    )
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "description": "Lane-level scoreboard showing which mutation/repair lanes are producing live beaters closest to promotion.",
        "lane_count": len(summaries),
        "lanes": summaries,
    }
    if robustness >= 55.0:
        readiness += 8.0
    elif robustness >= 45.0:
        readiness += 4.0
    day_rate = day.get("beats_active_day_rate")
    if day_rate is not None:
        readiness += max(0.0, min(10.0, (float(day_rate or 0.0) - 0.50) * 25.0))
    if "weak_day_consistency" in red_flags:
        readiness -= 8.0
    return round(max(0.0, min(100.0, readiness)), 4)


def _promotion_survival_score(row: dict[str, Any], evidence: dict[str, Any] | None = None) -> float:
    tags = _evidence_adjusted_tags(row, evidence)
    delta = max(0.0, float(row.get("step2_delta_vs_active") or hunt_intel.live_delta(row) or 0.0))
    readiness = _evidence_adjusted_readiness(row, evidence)
    if readiness >= 45.0:
        tags.discard("promotion_weak")
    quality = float(row.get("promotion_quality_score") or 0.0)
    route_action = _route_action_taxonomy(row)
    score = min(delta, 1000.0) * 0.06 + readiness * 1.9 + min(quality, 200.0) * 0.16
    if "day_consistent" in tags:
        score += 20.0
    if "low_overfit_risk" in tags:
        score += 18.0
    if "sample_ok" in tags:
        score += 10.0
    if "route_broader" in tags:
        score += 16.0
    if "ticker_balanced" in tags:
        score += 8.0
    if "side_balanced" in tags:
        score += 8.0
    penalties = {
        "no_holdout_credit": 160.0,
        "thin_holdout_edge": 86.0,
        "promotion_weak": 120.0,
        "route_narrow": 72.0,
        "small_total_edge": 42.0,
        "weak_day_consistency": 58.0,
        "high_overfit_risk": 32.0,
        "positive_holdout_but_not_promotion_grade": 70.0,
    }
    score -= sum(value for tag, value in penalties.items() if tag in tags)
    if {"no_holdout_credit", "promotion_weak"}.issubset(tags):
        score -= 100.0
    if readiness < 20.0:
        score -= 60.0
    if route_action.get("skip_only"):
        score -= 80.0
    if route_action.get("has_skip") and not route_action.get("has_score"):
        score -= 42.0
    return round(score, 4)


def _promotion_survival_leaderboard(
    rows: list[dict[str, Any]],
    *,
    limit: int = 100,
    evidence_by_variant: dict[str, dict[str, Any]] | None = None,
    sort_mode: str = "score",
) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        enriched = dict(row)
        evidence = (evidence_by_variant or {}).get(str(row.get("variant") or "")) or {}
        enriched["evidence_adjusted_learning_tags"] = sorted(_evidence_adjusted_tags(enriched, evidence))
        enriched["evidence_adjusted_promotion_readiness_score"] = _evidence_adjusted_readiness(enriched, evidence)
        enriched["readiness_component_breakdown"] = _readiness_component_breakdown(enriched, evidence)
        enriched["promotion_survival_score"] = _promotion_survival_score(enriched, evidence=evidence)
        enriched["promotion_minimum_bar"] = _promotion_minimum_bar(enriched, evidence=evidence)
        enriched["promotion_evidence_tier"] = (enriched.get("promotion_minimum_bar") or {}).get("evidence_tier")
        enriched["promotion_distance"] = _promotion_distance(enriched, evidence)
        enriched["route_action_taxonomy"] = _route_action_taxonomy(enriched)
        enriched["winner_mechanism"] = _winner_mechanism(enriched)
        enriched["suspicious_winner"] = _suspicious_winner(enriched, evidence)
        out.append(enriched)
    def sort_key(item: dict[str, Any]) -> tuple[float, float, float, float]:
        bar = item.get("promotion_minimum_bar") if isinstance(item.get("promotion_minimum_bar"), dict) else {}
        tier_rank = EVIDENCE_TIER_RANK.get(str(item.get("promotion_evidence_tier") or bar.get("evidence_tier") or "unknown"), 0)
        passed = 1 if bar.get("passed") else 0
        score = float(item.get("promotion_survival_score") or 0.0)
        delta = float(item.get("step2_delta_vs_active") or 0.0)
        pnl = float(item.get("step2_pnl") or 0.0)
        if sort_mode == "evidence_tier":
            return (float(tier_rank), float(passed), score, delta, pnl)
        return (float(passed), score, delta, pnl)
    return sorted(
        out,
        key=sort_key,
        reverse=True,
    )[:limit]


def _promotion_survival_reference(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {}
    top = rows[0]
    bar = top.get("promotion_minimum_bar") if isinstance(top.get("promotion_minimum_bar"), dict) else {}
    distance = top.get("promotion_distance") if isinstance(top.get("promotion_distance"), dict) else {}
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "variant": top.get("variant"),
        "route_key": hunt_intel.route_key_from_row(top),
        "step2_pnl": top.get("step2_pnl"),
        "delta_vs_live": top.get("step2_delta_vs_active"),
        "promotion_survival_score": top.get("promotion_survival_score"),
        "promotion_readiness_score": bar.get("promotion_readiness_score"),
        "robustness_score": bar.get("robustness_score"),
        "failures": list(bar.get("failures") or []),
        "next_best_repair": distance.get("next_best_repair"),
        "distance_points": distance.get("distance_points"),
        "blocking_open_gate_count": distance.get("blocking_open_gate_count"),
        "promotion_distance": distance,
        "readiness_component_breakdown": top.get("readiness_component_breakdown") or {},
    }


def _route_action_taxonomy(row: dict[str, Any]) -> dict[str, Any]:
    actions = []
    has_weight_shift = bool(row.get("weights"))
    has_bias_shift = abs(float(row.get("bias") or 0.0)) > 1e-9
    for route in row.get("routes") or []:
        if not isinstance(route, dict):
            continue
        action = str(route.get("action") or "").strip().lower()
        if action:
            actions.append(action)
        if route.get("weights"):
            has_weight_shift = True
        if abs(float(route.get("bias") or 0.0)) > 1e-9:
            has_bias_shift = True
    counts = Counter(actions)
    has_skip = counts.get("skip", 0) > 0
    has_score = counts.get("score", 0) > 0
    has_gate = any(action in {"gate", "force_long", "force_short"} or action.startswith("force_") for action in actions)
    primary = "score" if has_score else "skip" if has_skip else "gate" if has_gate else "global_weight_shift" if has_weight_shift else "unknown"
    return {
        "schema_version": 1,
        "route_key": hunt_intel.route_key_from_row(row),
        "route_count": len(row.get("routes") or []),
        "actions": dict(counts),
        "primary_action": primary,
        "has_skip": has_skip,
        "has_score": has_score,
        "has_gate": has_gate,
        "has_weight_shift": has_weight_shift,
        "has_bias_shift": has_bias_shift,
        "skip_only": has_skip and not has_score and not has_gate,
    }


def _positive_holdout_credit(evidence: dict[str, Any] | None = None) -> dict[str, Any]:
    evidence = evidence or {}
    holdout = evidence.get("train_holdout") if isinstance(evidence.get("train_holdout"), dict) else {}
    robustness_score = float(evidence.get("robustness_score") or 0.0)
    red_flags = set(str(flag) for flag in (evidence.get("red_flags") or []))
    holdout_delta = holdout.get("holdout_delta_vs_active")
    holdout_pnl = holdout.get("holdout_pnl")
    positive_holdout_delta = (
        holdout_delta is not None
        and float(holdout_delta or 0.0) > 0.0
        and float(holdout_pnl or 0.0) > 0.0
    )
    promotion_grade_holdout_credit = (
        positive_holdout_delta
        and robustness_score >= 55.0
        and "weak_day_consistency" not in red_flags
    )
    return {
        "holdout_delta_vs_active": holdout_delta,
        "holdout_pnl": holdout_pnl,
        "positive_holdout_delta": positive_holdout_delta,
        "promotion_grade_holdout_credit": promotion_grade_holdout_credit,
        "robustness_score": robustness_score,
    }


def _promotion_minimum_bar(row: dict[str, Any], evidence: dict[str, Any] | None = None) -> dict[str, Any]:
    tags = set(str(tag) for tag in (row.get("learning_tags") or hunt_intel.candidate_learning_tags(row)))
    delta = float(row.get("step2_delta_vs_active") or hunt_intel.live_delta(row) or 0.0)
    evidence = evidence or {}
    readiness = _evidence_adjusted_readiness(row, evidence) if evidence else float(row.get("promotion_readiness_score") or 0.0)
    holdout_credit = _positive_holdout_credit(evidence)
    robustness_score = float(holdout_credit.get("robustness_score") or 0.0)
    holdout_delta = holdout_credit.get("holdout_delta_vs_active")
    positive_holdout_delta = bool(holdout_credit.get("positive_holdout_delta"))
    promotion_grade_holdout_credit = bool(holdout_credit.get("promotion_grade_holdout_credit"))
    derived_holdout_credit = (
        positive_holdout_delta
        and robustness_score >= 55.0
    )
    route_breadth_credit = row.get("route_breadth_credit") if isinstance(row.get("route_breadth_credit"), dict) else {}
    route_safety = routed_profile_safety.evaluate_candidate(row)
    failures = []
    warnings = []
    if delta <= 0.0:
        failures.append("does_not_beat_current_live")
    if "no_holdout_credit" in tags and not promotion_grade_holdout_credit:
        failures.append("holdout_positive_but_not_promotion_grade" if positive_holdout_delta else "missing_holdout_credit")
    if evidence and robustness_score < 55.0:
        failures.append("robustness_below_floor")
    if readiness < 45.0:
        failures.append("promotion_readiness_below_floor")
    elif "promotion_weak" in tags:
        warnings.append("raw_readiness_weak_but_evidence_adjusted_floor_passed")
    if "route_narrow" in tags and not route_breadth_credit.get("passed"):
        failures.append("route_too_narrow")
    if not route_safety.get("ok"):
        failures.append("routed_profile_safety_gate")
    if "weak_day_consistency" in tags:
        failures.append("weak_day_consistency")
    if "thin_holdout_edge" in tags:
        warnings.append("thin_holdout_edge")
    if "small_total_edge" in tags:
        warnings.append("small_total_edge")
    if _route_action_taxonomy(row).get("skip_only"):
        warnings.append("skip_only_route_action")
    return {
        "schema_version": 1,
        "passed": not failures,
        "evidence_tier": _promotion_evidence_tier(row, evidence, failures),
        "derived_holdout_credit": derived_holdout_credit,
        "positive_holdout_delta": positive_holdout_delta,
        "promotion_grade_holdout_credit": promotion_grade_holdout_credit,
        "holdout_gate_glossary": {
            "positive_holdout_delta": "Chronological holdout beats active Live and remains positive.",
            "promotion_grade_holdout_credit": "Positive holdout plus robustness >= 55 and no weak-day consistency flag.",
        },
        "holdout_delta_vs_active": holdout_delta,
        "robustness_score": robustness_score if evidence else None,
        "failures": failures,
        "warnings": warnings,
        "readiness_floor": 45.0,
        "delta_vs_live": delta,
        "promotion_readiness_score": readiness,
        "route_breadth_credit": route_breadth_credit,
        "routed_profile_safety_gate": route_safety,
    }


def _promotion_evidence_tier(row: dict[str, Any], evidence: dict[str, Any] | None = None, failures: list[str] | None = None) -> str:
    evidence = evidence or {}
    failures = failures if failures is not None else _promotion_minimum_bar(row).get("failures", [])
    robustness = float(evidence.get("robustness_score") or 0.0)
    red_flags = set(str(flag) for flag in (evidence.get("red_flags") or []))
    if not failures and robustness >= 75.0 and not red_flags:
        return "gold"
    if not failures and robustness >= 65.0:
        return "silver"
    if not failures and robustness >= 55.0:
        return "bronze"
    failure_set = set(failures)
    if failures and failure_set.issubset({"promotion_readiness_below_floor"}) and robustness >= 55.0:
        return "bronze"
    if failures and failure_set.issubset({"missing_holdout_credit", "holdout_positive_but_not_promotion_grade", "promotion_readiness_below_floor", "robustness_below_floor"}):
        return "evidence_incomplete"
    return "blocked"


def _promotion_distance(row: dict[str, Any], evidence: dict[str, Any] | None = None) -> dict[str, Any]:
    evidence = evidence or {}
    bar = _promotion_minimum_bar(row, evidence=evidence)
    holdout = evidence.get("train_holdout") if isinstance(evidence.get("train_holdout"), dict) else {}
    robustness = float(evidence.get("robustness_score") or 0.0)
    readiness = float(bar.get("promotion_readiness_score") or 0.0)
    route_breadth_ok = "route_too_narrow" not in set(str(reason) for reason in (bar.get("failures") or []))
    route_safety = bar.get("routed_profile_safety_gate") if isinstance(bar.get("routed_profile_safety_gate"), dict) else routed_profile_safety.evaluate_candidate(row)
    gates = [
        {
            "gate": "beats_current_live",
            "passed": float(bar.get("delta_vs_live") or 0.0) > 0.0,
            "distance_to_pass": max(0.0, 0.01 - float(bar.get("delta_vs_live") or 0.0)),
        },
        {
            "gate": "promotion_grade_holdout",
            "passed": bool(bar.get("promotion_grade_holdout_credit")),
            "distance_to_pass": (
                0.0
                if bar.get("promotion_grade_holdout_credit") else
                max(1.0 if not bar.get("positive_holdout_delta") else 0.0, 55.0 - robustness)
            ),
            "holdout_delta_vs_active": holdout.get("holdout_delta_vs_active"),
            "glossary": "Requires positive chronological holdout, robustness >= 55, and no weak-day consistency flag.",
        },
        {
            "gate": "robustness_minimum_floor",
            "passed": robustness >= 55.0,
            "distance_to_pass": max(0.0, 55.0 - robustness),
            "threshold": 55.0,
        },
        {
            "gate": "robustness_promotion_grade_target",
            "passed": robustness >= 70.0,
            "distance_to_pass": max(0.0, 70.0 - robustness),
            "threshold": 70.0,
            "report_only": True,
        },
        {
            "gate": "promotion_readiness_floor",
            "passed": readiness >= 45.0,
            "distance_to_pass": max(0.0, 45.0 - readiness),
            "threshold": 45.0,
        },
        {
            "gate": "route_breadth",
            "passed": route_breadth_ok,
            "distance_to_pass": 0.0 if route_breadth_ok else 1.0,
        },
        {
            "gate": "routed_profile_safety",
            "passed": bool(route_safety.get("ok")),
            "distance_to_pass": 0.0 if route_safety.get("ok") else 1.0,
            "status": route_safety.get("status"),
        },
    ]
    open_gates = [gate for gate in gates if not gate.get("passed")]
    blocking_open_gates = [gate for gate in open_gates if not gate.get("report_only")]
    report_only_targets = [gate for gate in open_gates if gate.get("report_only")]
    priority_order = [
        "routed_profile_safety",
        "route_breadth",
        "robustness_minimum_floor",
        "promotion_readiness_floor",
        "promotion_grade_holdout",
        "robustness_promotion_grade_target",
    ]
    next_repair = next((name for name in priority_order if any(gate.get("gate") == name for gate in blocking_open_gates)), "")
    next_target = next((name for name in priority_order if any(gate.get("gate") == name for gate in report_only_targets)), "")
    distance_points = sum(float(gate.get("distance_to_pass") or 0.0) for gate in blocking_open_gates)
    return {
        "schema_version": 1,
        "passed": not blocking_open_gates,
        "open_gate_count": len(blocking_open_gates),
        "blocking_open_gate_count": len(blocking_open_gates),
        "report_only_target_count": len(report_only_targets),
        "report_only_targets": report_only_targets,
        "distance_points": round(distance_points, 4),
        "next_best_repair": next_repair or "promotion_review",
        "next_report_only_target": next_target,
        "gates": gates,
        "threshold_note": "Robustness 55 is the minimum promotion floor; 70 is the promotion-grade target used to prioritize repair.",
        "routed_profile_safety_gate": route_safety,
    }


def _winner_mechanism(row: dict[str, Any]) -> dict[str, Any]:
    taxonomy = _route_action_taxonomy(row)
    tags = list(row.get("learning_tags") or hunt_intel.candidate_learning_tags(row))[:8]
    route = hunt_intel.route_key_from_row(row)
    action = taxonomy.get("primary_action") or "unknown"
    readiness = float(row.get("promotion_readiness_score") or 0.0)
    delta = float(row.get("step2_delta_vs_active") or hunt_intel.live_delta(row) or 0.0)
    if taxonomy.get("skip_only"):
        summary = f"{route} appears to win by skipping a weak route; needs holdout/day proof before promotion."
    elif taxonomy.get("has_score"):
        summary = f"{route} appears to win by route-local score/weight changes with delta {round(delta, 2)}."
    else:
        summary = f"{route} appears to win through global scoring shifts; mechanism needs route attribution."
    return {
        "schema_version": 1,
        "route_key": route,
        "primary_action": action,
        "delta_vs_live": delta,
        "promotion_readiness_score": readiness,
        "learning_tags": tags,
        "summary": summary,
    }


def _suspicious_winner(row: dict[str, Any], evidence: dict[str, Any] | None = None) -> dict[str, Any]:
    tags = _evidence_adjusted_tags(row, evidence) if evidence else set(str(tag) for tag in (row.get("learning_tags") or hunt_intel.candidate_learning_tags(row)))
    taxonomy = _route_action_taxonomy(row)
    delta = float(row.get("step2_delta_vs_active") or hunt_intel.live_delta(row) or 0.0)
    readiness = _evidence_adjusted_readiness(row, evidence) if evidence else float(row.get("promotion_readiness_score") or 0.0)
    holdout_credit = _positive_holdout_credit(evidence) if evidence else {}
    reasons = []
    evidence_gaps = []
    score = 0.0
    if delta >= 750.0 and ("route_narrow" in tags or taxonomy.get("route_count", 0) <= 1):
        score += 45.0
        reasons.append("large_delta_from_narrow_route_change")
    if "no_holdout_credit" in tags and not holdout_credit.get("positive_holdout_delta"):
        evidence_gaps.append("no_holdout_credit")
    if taxonomy.get("skip_only"):
        score += 35.0
        reasons.append("skip_only_mechanism")
    if readiness < 25.0:
        evidence_gaps.append("very_low_promotion_readiness")
    if delta >= 1000.0 and readiness < 20.0:
        score += 20.0
        reasons.append("huge_delta_with_low_readiness")
    severity = "none"
    if evidence_gaps:
        severity = "high" if len(evidence_gaps) >= 2 and readiness < 10.0 else "medium"
    return {
        "schema_version": 1,
        "score": round(min(100.0, score), 4),
        "flagged": score >= 50.0,
        "reasons": reasons,
        "evidence_incomplete": bool(evidence_gaps),
        "evidence_incomplete_severity": severity,
        "evidence_gaps": evidence_gaps,
        "evidence_adjusted_readiness_score": round(readiness, 4),
    }


def _repair_worker_directives(repair_queue: dict[str, Any], *, worker_count: int = 4) -> dict[str, Any]:
    tasks = [task for task in (repair_queue.get("tasks") or []) if isinstance(task, dict)]
    directives = []
    seen: set[tuple[str, str]] = set()
    for task in tasks:
        gaps = list(task.get("gaps") or [])
        if "holdout_evidence" in gaps:
            action = "evidence_validation_holdout_day_split"
            width = "tight"
        elif "route_breadth" in gaps:
            action = "expand_route_breadth_controlled"
            width = "medium"
        elif "edge_size" in gaps:
            action = "amplify_edge_without_losing_balance"
            width = "wide"
        else:
            action = "promotion_readiness_repair"
            width = "tight"
        key = (str(task.get("route_key") or ""), action)
        if key in seen:
            continue
        seen.add(key)
        idx = len(directives)
        directives.append({
            "worker": f"worker_{(idx % max(1, worker_count)) + 1}",
            "action": action,
            "variant": task.get("variant"),
            "candidate_count": int(task.get("candidate_count") or 1),
            "route_key": task.get("route_key"),
            "mutation_width": width,
            "gaps": gaps,
            "target_failures": _target_failures_for_gaps(gaps),
            "success_metric": "raise_holdout_day_split_evidence_and_promotion_readiness_while_remaining_above_current_live",
            "stop_rule": "stop_if_variant_falls_below_live_or_readiness_regresses",
        })
        if len(directives) >= max(worker_count * 2, 8):
            break
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "description": "Executable worker directives derived from promotion evidence gaps.",
        "directive_count": len(directives),
        "directives": directives,
        "focus_routes": list(dict.fromkeys(item.get("route_key") for item in directives if item.get("route_key")))[:12],
    }


def _target_failures_for_gaps(gaps: list[str]) -> list[str]:
    targets = []
    if "holdout_evidence" in gaps:
        targets.extend(["promotion_grade_holdout_credit", "robustness_below_floor", "day_consistency"])
    if "route_breadth" in gaps:
        targets.append("route_breadth")
    if "promotion_readiness" in gaps:
        targets.append("promotion_readiness_below_floor")
    if "edge_size" in gaps:
        targets.append("edge_size")
    if "day_consistency" in gaps:
        targets.append("day_consistency")
    return list(dict.fromkeys(targets))


def _promotion_evidence_validation_report(
    rows: list[dict[str, Any]],
    *,
    active_row: dict[str, Any] | None = None,
    start_balance: float = 100000.0,
    limit: int = 100,
) -> dict[str, Any]:
    reports = []
    for idx, row in enumerate(rows[:limit], 1):
        try:
            evidence = candidate_robustness_report.evaluate_candidate(
                row,
                active_row=active_row,
                start_balance=start_balance,
                rank=idx,
            )
        except Exception as exc:
            evidence = {
                "variant": row.get("variant"),
                "route_key": hunt_intel.route_key_from_row(row),
                "error": repr(exc),
                "robustness_score": 0.0,
                "red_flags": ["evidence_validation_error"],
            }
        evidence["route_key"] = hunt_intel.route_key_from_row(row)
        bar = _promotion_minimum_bar(row, evidence=evidence)
        evidence["promotion_minimum_bar"] = bar
        evidence["promotion_evidence_tier"] = bar.get("evidence_tier")
        evidence["compact_evidence"] = _compact_evidence_line(evidence)
        reports.append(evidence)
    by_variant = {str(row.get("variant")): row for row in reports if row.get("variant")}
    tier_counts = Counter(str(row.get("promotion_evidence_tier") or "unknown") for row in reports)
    holdout_ready = [
        row for row in reports
        if float(((row.get("train_holdout") or {}).get("holdout_delta_vs_active") or 0.0)) > 0.0
        and float(((row.get("train_holdout") or {}).get("holdout_pnl") or 0.0)) > 0.0
    ]
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "description": "Actual evidence-building pass using chronological day split and active-baseline comparison.",
        "active_available": bool(active_row),
        "evaluated_count": len(reports),
        "tier_counts": dict(tier_counts),
        "holdout_ready_count": len(holdout_ready),
        "compact_topline": [_compact_evidence_line(row) for row in reports[:10]],
        "top_by_robustness": sorted(reports, key=lambda item: float(item.get("robustness_score") or 0.0), reverse=True)[:25],
        "reports": reports,
        "by_variant": by_variant,
    }


def _compact_evidence_line(evidence: dict[str, Any]) -> dict[str, Any]:
    holdout = evidence.get("train_holdout") if isinstance(evidence.get("train_holdout"), dict) else {}
    day = evidence.get("day_profile") if isinstance(evidence.get("day_profile"), dict) else {}
    recommendation = evidence.get("recommendation") if isinstance(evidence.get("recommendation"), dict) else {}
    return {
        "variant": evidence.get("variant"),
        "route_key": evidence.get("route_key"),
        "robustness_score": evidence.get("robustness_score"),
        "robustness_adjusted_score": evidence.get("robustness_adjusted_score"),
        "holdout_delta_vs_active": holdout.get("holdout_delta_vs_active"),
        "beats_active_day_rate": day.get("beats_active_day_rate"),
        "blockers": recommendation.get("blockers") or evidence.get("red_flags") or [],
        "evidence_tier": evidence.get("promotion_evidence_tier"),
    }


def _near_promotion_evidence_queue(
    rows: list[dict[str, Any]],
    evidence_validation: dict[str, Any],
    *,
    limit: int = 100,
) -> dict[str, Any]:
    evidence_by_variant = evidence_validation.get("by_variant") if isinstance(evidence_validation.get("by_variant"), dict) else {}
    queued = []
    for row in rows:
        evidence = evidence_by_variant.get(str(row.get("variant") or "")) or {}
        if not evidence:
            continue
        bar = _promotion_minimum_bar(row, evidence=evidence)
        holdout = evidence.get("train_holdout") if isinstance(evidence.get("train_holdout"), dict) else {}
        day = evidence.get("day_profile") if isinstance(evidence.get("day_profile"), dict) else {}
        failures = list(bar.get("failures") or [])
        blockers = list(((evidence.get("recommendation") or {}).get("blockers") if isinstance(evidence.get("recommendation"), dict) else []) or [])
        robustness_score = float(evidence.get("robustness_score") or 0.0)
        target_lane = (
            "robustness_minimum_floor" if robustness_score < 55.0
            else "robustness_below_70" if robustness_score < 70.0
            else "weak_day_consistency" if "weak_day_consistency" in set(str(flag) for flag in (evidence.get("red_flags") or failures))
            else "missing_holdout_credit" if "missing_holdout_credit" in failures
            else "positive_holdout_not_promotion_grade" if "holdout_positive_but_not_promotion_grade" in failures
            else "promotion_grade_holdout_credit" if not bar.get("promotion_grade_holdout_credit")
            else "promotion_readiness"
        )
        queued.append({
            "variant": row.get("variant"),
            "route_key": hunt_intel.route_key_from_row(row),
            "step2_pnl": row.get("step2_pnl"),
            "delta_vs_active": row.get("step2_delta_vs_active"),
            "promotion_survival_score": _promotion_survival_score(row),
            "promotion_evidence_tier": bar.get("evidence_tier"),
            "promotion_minimum_bar": bar,
            "promotion_distance": _promotion_distance(row, evidence),
            "evidence_adjusted_readiness_score": _evidence_adjusted_readiness(row, evidence),
            "robustness_score": evidence.get("robustness_score"),
            "holdout_delta_vs_active": holdout.get("holdout_delta_vs_active"),
            "beats_active_day_rate": day.get("beats_active_day_rate"),
            "target_repair_lane": target_lane,
            "human_next_step": _human_evidence_next_step(target_lane, failures + blockers),
            "compact_evidence": _compact_evidence_line(evidence),
        })
    queued.sort(
        key=lambda item: (
            -int((item.get("promotion_distance") or {}).get("open_gate_count") if (item.get("promotion_distance") or {}).get("open_gate_count") is not None else 99),
            -float((item.get("promotion_distance") or {}).get("distance_points") if (item.get("promotion_distance") or {}).get("distance_points") is not None else 1e9),
            EVIDENCE_TIER_RANK.get(str(item.get("promotion_evidence_tier") or "unknown"), 0),
            float(item.get("robustness_score") or 0.0),
            float(item.get("beats_active_day_rate") or 0.0),
            float(item.get("holdout_delta_vs_active") or 0.0),
            float(item.get("delta_vs_active") or 0.0),
        ),
        reverse=True,
    )
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "description": "Evidence-ranked live beaters closest to promotion review, separate from generic route repair.",
        "candidate_count": len(queued),
        "best_evidence_candidate": queued[0] if queued else {},
        "queue": queued[:limit],
    }


def _human_evidence_next_step(target_lane: str, blockers: list[str]) -> str:
    if target_lane == "robustness_minimum_floor":
        return "Lift robustness above the 55 minimum floor while preserving positive holdout and live edge."
    if target_lane == "robustness_below_70":
        return "Retest nearby mutations that preserve live edge while improving robustness score toward 70+."
    if target_lane == "weak_day_consistency":
        return "Probe day-stability variants; avoid increasing raw P/L at the cost of unstable day splits."
    if target_lane == "promotion_grade_holdout_credit":
        return "Focus on holdout-positive variants with stronger robustness and fewer red flags."
    if target_lane == "missing_holdout_credit":
        return "Find variants that produce a positive chronological holdout delta before spending on promotion review."
    if target_lane == "positive_holdout_not_promotion_grade":
        return "Preserve positive holdout while lifting robustness above the minimum floor and removing day instability."
    if "route_too_narrow" in blockers:
        return "Add controlled sibling-route breadth before another promotion review."
    return "Keep as a near-promotion candidate and raise readiness without losing the live edge."


def _evidence_repair_lanes(evidence_validation: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    row_by_variant = {str(row.get("variant") or ""): row for row in rows}
    lanes: dict[str, list[dict[str, Any]]] = {
        "robustness_minimum_floor": [],
        "robustness_below_70": [],
        "weak_day_consistency": [],
        "promotion_grade_holdout_credit": [],
        "positive_holdout_not_promotion_grade": [],
        "missing_holdout_credit": [],
        "route_breadth": [],
    }
    for evidence in evidence_validation.get("reports") or []:
        if not isinstance(evidence, dict):
            continue
        row = row_by_variant.get(str(evidence.get("variant") or ""), {})
        bar = _promotion_minimum_bar(row, evidence=evidence)
        failures = set(str(reason) for reason in (bar.get("failures") or []))
        red_flags = set(str(flag) for flag in (evidence.get("red_flags") or []))
        compact = {
            "variant": evidence.get("variant"),
            "route_key": evidence.get("route_key") or hunt_intel.route_key_from_row(row),
            "robustness_score": evidence.get("robustness_score"),
            "evidence_tier": bar.get("evidence_tier"),
            "failures": list(bar.get("failures") or []),
            "compact_evidence": _compact_evidence_line(evidence),
            "promotion_distance": _promotion_distance(row, evidence),
        }
        robustness_value = float(evidence.get("robustness_score") or 0.0)
        if robustness_value < 55.0:
            lanes["robustness_minimum_floor"].append(dict(compact, target_metric="robustness_score >= 55 minimum promotion floor"))
        if robustness_value < 70.0:
            lanes["robustness_below_70"].append(dict(compact, target_metric="robustness_score >= 70"))
        if "weak_day_consistency" in failures or "weak_day_consistency" in red_flags:
            lanes["weak_day_consistency"].append(dict(compact, target_metric="beats_active_day_rate and per-day P/L stability"))
        if not bar.get("promotion_grade_holdout_credit"):
            lanes["promotion_grade_holdout_credit"].append(dict(compact, target_metric="positive holdout delta with robustness >= 55 and no weak-day red flag"))
        if "holdout_positive_but_not_promotion_grade" in failures:
            lanes["positive_holdout_not_promotion_grade"].append(dict(compact, target_metric="preserve positive holdout while lifting robustness/readiness to promotion floor"))
        if "missing_holdout_credit" in failures:
            lanes["missing_holdout_credit"].append(dict(compact, target_metric="chronological holdout delta > 0 while still beating Live"))
        if "route_too_narrow" in failures:
            lanes["route_breadth"].append(dict(compact, target_metric="sibling route breadth without losing live edge"))
    for lane, candidates in lanes.items():
        candidates.sort(
            key=lambda item: (
                EVIDENCE_TIER_RANK.get(str(item.get("evidence_tier") or "unknown"), 0),
                float(item.get("robustness_score") or 0.0),
            ),
            reverse=True,
        )
        lanes[lane] = candidates[:50]
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "description": "Executable evidence repair lanes separated by the specific reason candidates failed promotion readiness.",
        "lane_counts": {lane: len(candidates) for lane, candidates in lanes.items()},
        "lanes": lanes,
    }


def _quality_floor_leaderboard(
    rows: list[dict[str, Any]],
    *,
    evidence_by_variant: dict[str, dict[str, Any]] | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    kept = []
    rejected = []
    tier_counts = Counter()
    for row in rows:
        enriched = dict(row)
        evidence = (evidence_by_variant or {}).get(str(row.get("variant") or "")) or {}
        bar = _promotion_minimum_bar(enriched, evidence=evidence)
        enriched["promotion_minimum_bar"] = bar
        enriched["promotion_evidence_tier"] = bar.get("evidence_tier")
        tier_counts[str(bar.get("evidence_tier") or "unknown")] += 1
        if bar.get("passed"):
            kept.append(enriched)
        else:
            rejected.append({
                "variant": row.get("variant"),
                "route_key": hunt_intel.route_key_from_row(row),
                "step2_pnl": row.get("step2_pnl"),
                "delta_vs_active": row.get("step2_delta_vs_active"),
                "failures": bar.get("failures") or [],
            })
    kept = _promotion_survival_leaderboard(kept, evidence_by_variant=evidence_by_variant, limit=limit)
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "description": "Promotion-floor queue: live beaters that also pass the minimum promotion-evidence floor.",
        "pool_description": "This is not the full top100/live-beater pool; use variant_funnel.top100_count for total behavior-unique kept live beaters.",
        "kept_count": len(kept),
        "promotion_floor_kept_count": len(kept),
        "input_live_beater_count": len(rows),
        "rejected_count": len(rejected),
        "tier_counts": dict(tier_counts),
        "promotion_queue": kept[:limit],
        "leaderboard": kept[:limit],
        "rejected_examples": rejected[:25],
    }


def _route_action_report(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_action = Counter()
    skip_rows = []
    score_rows = []
    force_gate_rows = []
    weight_shift_rows = []
    for row in rows:
        taxonomy = _route_action_taxonomy(row)
        action = str(taxonomy.get("primary_action") or "unknown")
        by_action[action] += 1
        compact = {
            "variant": row.get("variant"),
            "route_key": taxonomy.get("route_key"),
            "primary_action": action,
            "step2_pnl": row.get("step2_pnl"),
            "delta_vs_active": row.get("step2_delta_vs_active"),
            "promotion_survival_score": _promotion_survival_score(row),
        }
        if taxonomy.get("has_skip"):
            skip_rows.append(compact)
        if taxonomy.get("has_score"):
            score_rows.append(compact)
        if taxonomy.get("has_gate"):
            force_gate_rows.append(compact)
        if taxonomy.get("has_weight_shift") or taxonomy.get("has_bias_shift"):
            weight_shift_rows.append(compact)
    sorter = lambda item: float(item.get("step2_pnl") or 0.0)
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "action_counts": dict(by_action),
        "skip_route_winners": sorted(skip_rows, key=sorter, reverse=True)[:100],
        "score_route_winners": sorted(score_rows, key=sorter, reverse=True)[:100],
        "force_gate_winners": sorted(force_gate_rows, key=sorter, reverse=True)[:100],
        "weight_or_bias_shift_winners": sorted(weight_shift_rows, key=sorter, reverse=True)[:100],
        "taxonomy_contract": "primary_action is mutually exclusive; detail lists show non-exclusive mechanics.",
    }


def _route_crowding_report(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(hunt_intel.route_key_from_row(row) for row in rows)
    total = max(1, len(rows))
    crowded = []
    for route, count in counts.most_common():
        share = count / total
        if count >= 5 or share >= 0.12:
            crowded.append({
                "route_key": route,
                "count": count,
                "share_pct": round(share * 100.0, 4),
                "crowding_penalty": round(min(40.0, share * 140.0), 4),
                "recommended_action": "reduce_budget_or_require_holdout_repair" if share >= 0.20 else "monitor",
            })
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "total_rows": len(rows),
        "route_count": len(counts),
        "crowded_routes": crowded[:25],
        "avoid_routes": [row["route_key"] for row in crowded if row.get("recommended_action") == "reduce_budget_or_require_holdout_repair"][:12],
    }


def _tradeoff_frontier(
    rows: list[dict[str, Any]],
    *,
    evidence_by_variant: dict[str, dict[str, Any]] | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    enriched = []
    for idx, row in enumerate(sorted(rows, key=lambda item: float(item.get("step2_pnl") or 0.0), reverse=True), 1):
        item = dict(row)
        evidence = (evidence_by_variant or {}).get(str(row.get("variant") or "")) or {}
        item["raw_pnl_rank"] = idx
        item["promotion_survival_score"] = _promotion_survival_score(item, evidence=evidence)
        item["promotion_minimum_bar"] = _promotion_minimum_bar(item, evidence)
        enriched.append(item)
    survival_sorted = sorted(enriched, key=lambda item: float(item.get("promotion_survival_score") or 0.0), reverse=True)
    survival_rank = {str(row.get("variant")): idx for idx, row in enumerate(survival_sorted, 1)}
    points = []
    for row in enriched:
        points.append({
            "variant": row.get("variant"),
            "route_key": hunt_intel.route_key_from_row(row),
            "step2_pnl": row.get("step2_pnl"),
            "delta_vs_active": row.get("step2_delta_vs_active"),
            "raw_pnl_rank": row.get("raw_pnl_rank"),
            "promotion_survival_rank": survival_rank.get(str(row.get("variant"))),
            "promotion_survival_score": row.get("promotion_survival_score"),
            "promotion_bar_passed": (row.get("promotion_minimum_bar") or {}).get("passed"),
            "suspicious_winner_score": _suspicious_winner(row, (evidence_by_variant or {}).get(str(row.get("variant") or "")) or {}).get("score"),
        })
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "description": "Raw P/L versus promotability tradeoff surface for live-beating candidates.",
        "points": points[:limit],
        "raw_pnl_top10": points[:10],
        "promotion_top10": sorted(points, key=lambda item: float(item.get("promotion_survival_score") or 0.0), reverse=True)[:10],
    }


def _separate_leaderboards(
    rows: list[dict[str, Any]],
    *,
    evidence_by_variant: dict[str, dict[str, Any]] | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    discovery = sorted(rows, key=lambda row: float(row.get("step2_pnl") or 0.0), reverse=True)[:limit]
    promotion = [
        row
        for row in _promotion_survival_leaderboard(rows, limit=limit * 2, evidence_by_variant=evidence_by_variant)
        if (row.get("promotion_minimum_bar") or {}).get("passed")
    ][:limit]
    repair = sorted(
        [
            dict(row, promotion_minimum_bar=_promotion_minimum_bar(row, (evidence_by_variant or {}).get(str(row.get("variant") or "")) or {}))
            for row in rows
            if not _promotion_minimum_bar(row, (evidence_by_variant or {}).get(str(row.get("variant") or "")) or {}).get("passed")
        ],
        key=lambda row: (float(row.get("step2_delta_vs_active") or 0.0), float(row.get("step2_pnl") or 0.0)),
        reverse=True,
    )[:limit]
    weird = sorted(
        [
            dict(
                row,
                route_action_taxonomy=_route_action_taxonomy(row),
                suspicious_winner=_suspicious_winner(row, (evidence_by_variant or {}).get(str(row.get("variant") or "")) or {}),
            )
            for row in rows
        ],
        key=lambda row: (
            float((row.get("suspicious_winner") or {}).get("score") or 0.0),
            1 if _route_action_taxonomy(row).get("primary_action") not in {"score", "skip"} else 0,
            float(row.get("step2_delta_vs_active") or 0.0),
        ),
        reverse=True,
    )[:limit]
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "discovery_raw_pnl": discovery,
        "promotion_ready": promotion,
        "repair_needed": repair,
        "weird_exploration": weird,
    }


def _family_saturation_controls(rows: list[dict[str, Any]], route_crowding: dict[str, Any]) -> dict[str, Any]:
    counts = Counter()
    for row in rows:
        lineage = row.get("lineage") if isinstance(row.get("lineage"), dict) else {}
        family = str(row.get("mutation_lane") or lineage.get("mutation_lane") or hunt_intel.route_key_from_row(row) or "unknown")
        counts[family] += 1
    total = max(1, len(rows))
    saturated = [
        {
            "family": family,
            "count": count,
            "share_pct": round(count / total * 100.0, 4),
            "recommended_action": "pause_family" if count / total >= 0.20 else "cap_family_budget",
        }
        for family, count in counts.most_common()
        if count >= 5 or count / total >= 0.12
    ]
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "saturated_families": saturated[:25],
        "pause_families": [row["family"] for row in saturated if row.get("recommended_action") == "pause_family"][:12],
        "avoid_routes": list(route_crowding.get("avoid_routes") or [])[:12],
        "control_contract": "Use these as budget caps, not permanent bans; resume only after evidence repair or wider behavior diversity.",
    }


def _previous_run_summary(current_run_dir: Path) -> dict[str, Any]:
    parent = current_run_dir.parent
    candidates = [path for path in parent.iterdir() if path.is_dir() and path.resolve() != current_run_dir.resolve()] if parent.exists() else []
    if not candidates:
        return {}
    previous = sorted(candidates, key=_run_dir_sort_key, reverse=True)[0]
    funnel = hunt_intel.read_json(previous / "variant_funnel.json", {}) or {}
    brain = hunt_intel.read_json(previous / "hunt_brain_summary.json", {}) or {}
    return {
        "run_dir": str(previous.resolve()),
        "requested_variants": funnel.get("requested_variants"),
        "scored_total": funnel.get("scored_total"),
        "live_beaters_streamed": funnel.get("live_beaters_streamed"),
        "behavior_unique_rows": funnel.get("behavior_unique_rows"),
        "stop_go_decision": brain.get("stop_go_decision"),
    }


def _run_to_run_comparison(run_dir: Path, variant_funnel: dict[str, Any]) -> dict[str, Any]:
    previous = _previous_run_summary(run_dir)
    if not previous:
        return {"schema_version": 1, "source": "run_step2_three_hour_hunt", "previous_run": {}, "deltas": {}}
    def delta(key: str) -> Any:
        if previous.get(key) is None or variant_funnel.get(key) is None:
            return None
        return float(variant_funnel.get(key) or 0.0) - float(previous.get(key) or 0.0)
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "previous_run": previous,
        "current": {
            "requested_variants": variant_funnel.get("requested_variants"),
            "scored_total": variant_funnel.get("scored_total"),
            "live_beaters_streamed": variant_funnel.get("live_beaters_streamed"),
            "behavior_unique_rows": variant_funnel.get("behavior_unique_rows"),
        },
        "deltas": {
            "scored_total": delta("scored_total"),
            "live_beaters_streamed": delta("live_beaters_streamed"),
            "behavior_unique_rows": delta("behavior_unique_rows"),
        },
    }


def _repair_attempt_memory(
    run_dir: Path,
    rows: list[dict[str, Any]],
    repair_queue: dict[str, Any],
    evidence_by_variant: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    previous = _previous_run_summary(run_dir)
    routes = Counter(task.get("route_key") for task in (repair_queue.get("tasks") or []) if task.get("route_key"))
    current_best: dict[str, float] = {}
    for row in rows:
        route = hunt_intel.route_key_from_row(row)
        current_best[route] = max(current_best.get(route, 0.0), float(row.get("promotion_readiness_score") or 0.0))
    evidence_best: dict[str, float] = {}
    for row in rows:
        route = hunt_intel.route_key_from_row(row)
        evidence = (evidence_by_variant or {}).get(str(row.get("variant") or "")) or {}
        evidence_best[route] = max(evidence_best.get(route, 0.0), float(evidence.get("robustness_score") or 0.0))
    attempts = [
        {
            "route_key": route,
            "open_repair_tasks": count,
            "current_best_readiness": round(current_best.get(route, 0.0), 4),
            "current_best_evidence_score": round(evidence_best.get(route, 0.0), 4),
            "status": "improving_or_ready" if current_best.get(route, 0.0) >= 70.0 or evidence_best.get(route, 0.0) >= 65.0 else "still_needs_repair",
            "success_metric": "readiness_or_evidence_score_improvement",
        }
        for route, count in routes.most_common()
    ]
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "previous_run": previous,
        "attempts": attempts[:100],
        "repair_routes": [row["route_key"] for row in attempts if row.get("status") == "still_needs_repair"][:12],
    }


def _next_500_plan(
    rows: list[dict[str, Any]],
    repair_queue: dict[str, Any],
    route_crowding: dict[str, Any],
    quality_floor: dict[str, Any],
    anti_alias: dict[str, Any] | None = None,
    route_budget_caps: dict[str, Any] | None = None,
    preserved_routes: list[str] | None = None,
    suppressed_routes: list[str] | None = None,
) -> dict[str, Any]:
    suppressed = {str(route) for route in (suppressed_routes or []) if str(route)}
    task_routes = [
        str(task.get("route_key") or "")
        for task in (repair_queue.get("tasks") or [])
        if task.get("route_key") and str(task.get("route_key") or "") not in suppressed
    ]
    if not task_routes:
        task_routes = [str(task.get("route_key") or "") for task in (repair_queue.get("tasks") or []) if task.get("route_key")]
    repair_routes = list(dict.fromkeys(list(preserved_routes or []) + task_routes))[:8]
    broadened_routes = list(repair_routes)
    for route in repair_routes:
        for sibling in _sibling_routes(str(route)):
            if sibling not in broadened_routes:
                broadened_routes.append(sibling)
            if len(broadened_routes) >= 8:
                break
        if len(broadened_routes) >= 8:
            break
    if repair_routes and len(broadened_routes) < 3:
        broadened_routes.extend([route for route in ["CLSK|*|*", "MARA|*|*", "RIOT|*|*"] if route not in broadened_routes])
    broadened_routes = broadened_routes[:8]
    gap_counts = Counter()
    for task in repair_queue.get("tasks") or []:
        gap_counts.update(str(gap) for gap in (task.get("gaps") or []))
    promotion_ready = int(quality_floor.get("kept_count") or 0)
    has_readiness_gap = bool(gap_counts.get("promotion_readiness"))
    has_breadth_gap = bool(gap_counts.get("route_breadth"))
    if promotion_ready >= 5:
        objective = "scale_promotion_ready_routes"
    elif repair_routes and has_readiness_gap and has_breadth_gap:
        objective = "repair_promotion_readiness_and_route_breadth"
    elif repair_routes and has_breadth_gap:
        objective = "repair_route_breadth"
    elif repair_routes and has_readiness_gap:
        objective = "repair_promotion_readiness"
    elif repair_routes:
        objective = "repair_holdout_and_day_split_gaps"
    else:
        objective = "broaden_search_space_with_weird_indicator_shuffles"
    sibling_routes = []
    for route in repair_routes:
        for sibling in _sibling_routes(str(route)):
            if sibling not in sibling_routes and sibling not in repair_routes:
                sibling_routes.append(sibling)
    anti_pressure = str((anti_alias or {}).get("pressure") or "low")
    structural_variants = 135 if anti_pressure == "high" else 70 if anti_pressure == "medium" else 25
    behavior_rescue_variants = 90 if anti_pressure == "high" else 40 if anti_pressure == "medium" else 0
    has_holdout_gap = bool(gap_counts.get("holdout_evidence"))
    has_edge_gap = bool(gap_counts.get("edge_size"))
    weird_variants = 10 if repair_routes and (has_readiness_gap or has_breadth_gap or has_holdout_gap) else 80
    control_routes = [route for route in sibling_routes if route not in set(route_crowding.get("avoid_routes") or [])][:4]
    if not control_routes:
        control_routes = list(route_crowding.get("avoid_routes") or [])[:4]
    plan = {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "variant_budget": 500,
        "objective": objective,
        "failed_evidence_counts": dict(gap_counts),
        "route_budget_caps": route_budget_caps or {},
        "preserved_objective_routes": list(dict.fromkeys(preserved_routes or []))[:12],
        "suppressed_routes": list(dict.fromkeys(suppressed_routes or []))[:12],
        "allocation": [
            {"bucket": "holdout_day_split_validation", "variants": 120 if has_holdout_gap else 40, "routes": repair_routes},
            {"bucket": "promotion_readiness_repair", "variants": 150 if has_readiness_gap else 40, "routes": repair_routes},
            {"bucket": "route_breadth_repair", "variants": 25 if has_breadth_gap else 0, "routes": broadened_routes},
            {"bucket": "sibling_route_expansion", "variants": 45 if has_breadth_gap else 15, "routes": sibling_routes[:8]},
            {"bucket": "robustness_floor_repair", "variants": 125 if repair_routes else 0, "routes": repair_routes},
            {"bucket": "robustness_grade_target_repair", "variants": 85 if repair_routes else 0, "routes": repair_routes},
            {"bucket": "edge_cushion_repair", "variants": 35 if has_edge_gap else 0, "routes": repair_routes},
            {"bucket": "anti_alias_structural_mutation", "variants": structural_variants, "routes": list((anti_alias or {}).get("structural_mutation_routes") or [])[:6]},
            {"bucket": "behavior_unique_rescue", "variants": behavior_rescue_variants, "routes": broadened_routes + sibling_routes[:8]},
            {"bucket": "weird_exploration", "variants": weird_variants, "routes": []},
            {"bucket": "control_retest", "variants": 25 if repair_routes else 240, "routes": control_routes},
        ],
        "success_criteria": [
            "only keep variants above current Live",
            "promotion floor kept_count should rise",
            "top candidates should gain holdout/day-split evidence instead of only raw P/L",
            "small total-edge winners must increase edge cushion or stay out of promotion review",
            "route-breadth repairs should survive across ticker/phase siblings, not only the parent route",
            "promotion_readiness_score should clear 45 and trend toward the 70 promotion-grade target",
            "robustness should stay above the 55 floor while moving toward the report-only 70 target",
            "actual route telemetry should cover at least 80% of scored variants with human route keys",
            "top route should stay below 60% of kept candidates before scaling beyond controlled repair",
            "behavior-unique kept winners should reach at least 20 before any big run",
            "clone-heavy parent routes should be capped at generation time, not just deduped afterward",
        ],
    }
    plan["repair_routes"] = repair_routes
    plan["broadened_repair_routes"] = broadened_routes
    return _normalize_next_500_plan(plan, route_budget_caps or {})


def _normalize_variant_allocation(allocation: list[dict[str, Any]], budget: int) -> list[dict[str, Any]]:
    cleaned = [dict(row) for row in allocation if isinstance(row, dict) and int(row.get("variants") or row.get("variants_per_500") or 0) > 0]
    if not cleaned:
        return []
    key = "variants" if "variants" in cleaned[0] else "variants_per_500"
    total = sum(int(row.get(key) or 0) for row in cleaned)
    if total <= 0:
        return cleaned
    scale = float(budget) / float(total)
    normalized = []
    for row in cleaned:
        item = dict(row)
        item[key] = max(1, int(round(int(row.get(key) or 0) * scale)))
        normalized.append(item)
    diff = int(budget) - sum(int(row.get(key) or 0) for row in normalized)
    if normalized and diff:
        target = next((row for row in normalized if str(row.get("bucket") or row.get("lane") or "") in {"weird_exploration", "control_retest"}), normalized[-1])
        target[key] = max(1, int(target.get(key) or 0) + diff)
    return normalized


def _route_cap_lookup(route_budget_caps: dict[str, Any]) -> dict[str, int]:
    return {
        str(row.get("route_key") or ""): int(row.get("max_variants_per_500") or 0)
        for row in (route_budget_caps or {}).get("route_caps") or []
        if isinstance(row, dict) and row.get("route_key") and int(row.get("max_variants_per_500") or 0) > 0
    }


def _route_budget_usage(allocation: list[dict[str, Any]]) -> Counter:
    usage: Counter = Counter()
    for row in allocation:
        variants = int(row.get("variants") or 0)
        routes = [str(route) for route in (row.get("routes") or []) if str(route)]
        if not routes or variants <= 0:
            continue
        share = max(1, int(round(variants / max(1, len(routes)))))
        for route in routes:
            usage[route] += share
    return usage


def _route_cap_overflow_target(allocation: list[dict[str, Any]], capped_routes: list[str]) -> dict[str, Any]:
    sibling_routes: list[str] = []
    capped = {str(route) for route in capped_routes if str(route)}
    for route in capped:
        for sibling in _sibling_routes(route):
            if sibling and sibling not in capped and sibling not in sibling_routes:
                sibling_routes.append(sibling)
    if sibling_routes:
        target = next((row for row in allocation if str(row.get("bucket") or "") == "sibling_route_expansion"), None)
        if target is None:
            target = {"bucket": "sibling_route_expansion", "variants": 0, "routes": sibling_routes[:8]}
            allocation.append(target)
        else:
            routes = list(target.get("routes") or [])
            for route in sibling_routes:
                if route not in routes:
                    routes.append(route)
            target["routes"] = routes[:8]
        return target
    target = next((row for row in allocation if str(row.get("bucket") or "") == "weird_exploration"), None)
    if target is None:
        target = {"bucket": "weird_exploration", "variants": 0, "routes": []}
        allocation.append(target)
    return target


def _normalize_next_500_plan(plan: dict[str, Any], route_budget_caps: dict[str, Any]) -> dict[str, Any]:
    budget = int(plan.get("variant_budget") or 500)
    allocation = _normalize_variant_allocation(list(plan.get("allocation") or []), budget)
    caps = _route_cap_lookup(route_budget_caps)
    freed = 0
    if caps:
        usage = _route_budget_usage(allocation)
        for route, cap in caps.items():
            over = max(0, int(usage.get(route) or 0) - cap)
            if over <= 0:
                continue
            routed_rows = [row for row in allocation if route in set(str(item) for item in (row.get("routes") or []))]
            routed_total = sum(int(row.get("variants") or 0) for row in routed_rows) or 1
            for row in routed_rows:
                reduction = min(int(row.get("variants") or 0) - 1, int(round(over * int(row.get("variants") or 0) / routed_total)))
                if reduction > 0:
                    row["variants"] = int(row.get("variants") or 0) - reduction
                    freed += reduction
        if freed:
            target = _route_cap_overflow_target(allocation, list(caps))
            target["variants"] = int(target.get("variants") or 0) + freed
    allocation = _normalize_variant_allocation(allocation, budget)
    if caps:
        for _ in range(3):
            usage = _route_budget_usage(allocation)
            moved = 0
            for route, cap in caps.items():
                over = max(0, int(usage.get(route) or 0) - cap)
                if over <= 0:
                    continue
                routed_rows = sorted(
                    [row for row in allocation if route in set(str(item) for item in (row.get("routes") or []))],
                    key=lambda row: int(row.get("variants") or 0),
                    reverse=True,
                )
                for row in routed_rows:
                    if over <= 0:
                        break
                    reduction = min(over, max(0, int(row.get("variants") or 0) - 1))
                    if reduction > 0:
                        row["variants"] = int(row.get("variants") or 0) - reduction
                        moved += reduction
                        over -= reduction
            if moved <= 0:
                break
            target = _route_cap_overflow_target(allocation, list(caps))
            target["variants"] = int(target.get("variants") or 0) + moved
    usage = _route_budget_usage(allocation)
    compliance = []
    for route, cap in caps.items():
        used = int(usage.get(route) or 0)
        compliance.append({
            "route_key": route,
            "allocated_variants": used,
            "max_variants_per_500": cap,
            "cap_respected": used <= cap,
        })
    out = dict(plan)
    out["allocation"] = allocation
    out["route_budget_usage"] = dict(usage)
    out["route_cap_compliance"] = compliance
    out["allocation_total"] = sum(int(row.get("variants") or 0) for row in allocation)
    return out


def _rank_delta_summary(raw_rows: list[dict[str, Any]], survival_rows: list[dict[str, Any]]) -> dict[str, Any]:
    raw_rank = {str(row.get("variant") or ""): idx for idx, row in enumerate(raw_rows, 1)}
    survival_rank = {str(row.get("variant") or ""): idx for idx, row in enumerate(survival_rows, 1)}
    deltas = []
    for variant, raw_idx in raw_rank.items():
        if variant not in survival_rank:
            continue
        promo_idx = survival_rank[variant]
        deltas.append({
            "variant": variant,
            "raw_rank": raw_idx,
            "promotion_survival_rank": promo_idx,
            "rank_delta": raw_idx - promo_idx,
        })
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "description": "Positive rank_delta means the candidate improved when sorted by promotion survival; negative means raw P/L overstated it.",
        "raw_rank1": raw_rows[0].get("variant") if raw_rows else None,
        "promotion_rank1": survival_rows[0].get("variant") if survival_rows else None,
        "biggest_promotion_improvements": sorted(deltas, key=lambda item: int(item.get("rank_delta") or 0), reverse=True)[:10],
        "biggest_raw_pnl_overstatements": sorted(deltas, key=lambda item: int(item.get("rank_delta") or 0))[:10],
        "top_raw_rank_deltas": [row for row in deltas if int(row.get("raw_rank") or 999999) <= 10],
    }


def _why_raw_rank1_not_promotion_rank1(
    raw_rows: list[dict[str, Any]],
    survival_rows: list[dict[str, Any]],
    evidence_by_variant: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if not raw_rows:
        return {"schema_version": 1, "status": "no_raw_candidates"}
    raw1 = raw_rows[0]
    promo1 = survival_rows[0] if survival_rows else {}
    raw_variant = str(raw1.get("variant") or "")
    promo_variant = str(promo1.get("variant") or "")
    evidence = (evidence_by_variant or {}).get(raw_variant) or {}
    bar = _promotion_minimum_bar(raw1, evidence=evidence)
    if raw_variant == promo_variant:
        summary = "Raw rank 1 is also promotion-survival rank 1."
    else:
        failures = list(bar.get("failures") or [])
        summary = (
            f"Raw rank 1 lost the promotion-survival lead because it still has: {', '.join(failures[:4])}."
            if failures else
            "Raw rank 1 lost the promotion-survival lead on evidence/readiness tie-breakers."
        )
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "raw_rank1": raw1.get("variant"),
        "promotion_rank1": promo1.get("variant"),
        "same_candidate": raw_variant == promo_variant,
        "raw_rank1_pnl": raw1.get("step2_pnl"),
        "raw_rank1_delta_vs_active": raw1.get("step2_delta_vs_active"),
        "raw_rank1_promotion_bar": bar,
        "human_summary": summary,
    }


def _route_evidence_cards(rows: list[dict[str, Any]], evidence_by_variant: dict[str, dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        route = hunt_intel.route_key_from_row(row)
        evidence = evidence_by_variant.get(str(row.get("variant") or "")) or {}
        bar = _promotion_minimum_bar(row, evidence=evidence)
        card = grouped.setdefault(route, {
            "route_key": route,
            "candidate_count": 0,
            "best_raw_candidate": {},
            "best_evidence_candidate": {},
            "evidence_tier_counts": Counter(),
            "failure_counts": Counter(),
        })
        card["candidate_count"] += 1
        card["evidence_tier_counts"].update([str(bar.get("evidence_tier") or "unknown")])
        card["failure_counts"].update(str(reason) for reason in (bar.get("failures") or []))
        raw_compact = {
            "variant": row.get("variant"),
            "step2_pnl": row.get("step2_pnl"),
            "delta_vs_active": row.get("step2_delta_vs_active"),
        }
        if not card["best_raw_candidate"] or float(row.get("step2_pnl") or 0.0) > float(card["best_raw_candidate"].get("step2_pnl") or 0.0):
            card["best_raw_candidate"] = raw_compact
        evidence_score = (
            EVIDENCE_TIER_RANK.get(str(bar.get("evidence_tier") or "unknown"), 0) * 100.0
            + float(evidence.get("robustness_score") or 0.0)
        )
        if not card["best_evidence_candidate"] or evidence_score > float(card["best_evidence_candidate"].get("_evidence_score") or -1.0):
            card["best_evidence_candidate"] = dict(raw_compact, robustness_score=evidence.get("robustness_score"), evidence_tier=bar.get("evidence_tier"), _evidence_score=evidence_score)
    cards = []
    for route, card in grouped.items():
        failures = card["failure_counts"].most_common(3)
        action = "promotion_review" if not failures else "repair_" + str(failures[0][0])
        best_evidence = dict(card["best_evidence_candidate"])
        best_evidence.pop("_evidence_score", None)
        cards.append({
            "route_key": route,
            "candidate_count": card["candidate_count"],
            "best_raw_candidate": card["best_raw_candidate"],
            "best_evidence_candidate": best_evidence,
            "evidence_tier_counts": dict(card["evidence_tier_counts"]),
            "top_failure_reasons": [{"reason": reason, "count": count} for reason, count in failures],
            "recommended_next_action": action,
        })
    cards.sort(key=lambda item: (int(item.get("candidate_count") or 0), float((item.get("best_raw_candidate") or {}).get("step2_pnl") or 0.0)), reverse=True)
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "description": "Per-route evidence cards for deciding whether a route should get repair, breadth, or promotion review budget.",
        "route_count": len(cards),
        "cards": cards[:100],
    }


def _route_promotion_distance_cards(rows: list[dict[str, Any]], evidence_by_variant: dict[str, dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        route = hunt_intel.route_key_from_row(row)
        evidence = evidence_by_variant.get(str(row.get("variant") or "")) or {}
        grouped.setdefault(route, []).append({
            "variant": row.get("variant"),
            "step2_pnl": row.get("step2_pnl"),
            "delta_vs_active": row.get("step2_delta_vs_active"),
            "promotion_distance": _promotion_distance(row, evidence),
        })
    cards = []
    for route, candidates in grouped.items():
        candidates.sort(key=lambda item: (int((item.get("promotion_distance") or {}).get("open_gate_count") or 99), float((item.get("promotion_distance") or {}).get("distance_points") or 1e9), -float(item.get("delta_vs_active") or 0.0)))
        best = candidates[0]
        distance = best.get("promotion_distance") or {}
        cards.append({
            "route_key": route,
            "candidate_count": len(candidates),
            "closest_candidate": best,
            "open_gate_count": distance.get("open_gate_count"),
            "distance_points": distance.get("distance_points"),
            "next_best_repair": distance.get("next_best_repair"),
            "recommended_action": "promotion_review" if distance.get("passed") else f"repair_{distance.get('next_best_repair')}",
        })
    cards.sort(key=lambda item: (int(item.get("open_gate_count") or 99), float(item.get("distance_points") or 1e9), -int(item.get("candidate_count") or 0)))
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "description": "Route-level promotion distance: which route is closest to producing a promotable candidate and the next repair to try.",
        "closest_route": cards[0] if cards else {},
        "cards": cards[:100],
    }


def _repair_allocation_by_evidence_weakness(evidence_repair_lanes: dict[str, Any], route_budget_caps: dict[str, Any]) -> dict[str, Any]:
    lane_counts = evidence_repair_lanes.get("lane_counts") if isinstance(evidence_repair_lanes.get("lane_counts"), dict) else {}
    total = max(1, sum(int(value or 0) for value in lane_counts.values()))
    priority = [
        ("robustness_minimum_floor", 0.24),
        ("robustness_below_70", 0.16),
        ("route_breadth", 0.24),
        ("positive_holdout_not_promotion_grade", 0.18),
        ("missing_holdout_credit", 0.12),
        ("weak_day_consistency", 0.12),
        ("promotion_grade_holdout_credit", 0.06),
    ]
    allocation = []
    for lane, floor_share in priority:
        count = int(lane_counts.get(lane) or 0)
        if count <= 0:
            continue
        share = max(floor_share, count / total * 0.45)
        allocation.append({
            "lane": lane,
            "variants_per_500": int(round(500 * share)),
            "candidate_count": count,
            "objective": _human_evidence_next_step(lane, []),
        })
    planned = sum(int(row.get("variants_per_500") or 0) for row in allocation)
    if planned > 500:
        scale = 500.0 / planned
        for row in allocation:
            row["variants_per_500"] = max(10, int(round(int(row["variants_per_500"]) * scale)))
    allocation = _normalize_variant_allocation(allocation, 500)
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "description": "Repair budget allocated by the actual evidence weakness mix, with route caps applied separately.",
        "route_budget_caps_active": bool(route_budget_caps.get("overconcentrated")),
        "allocation": allocation,
    }


def _per_route_repair_lane_allocation(route_distance_cards: dict[str, Any]) -> dict[str, Any]:
    lane_by_gate = {
        "promotion_grade_holdout": "promotion_grade_holdout_credit",
        "robustness_minimum_floor": "robustness_minimum_floor",
        "robustness_promotion_grade_target": "robustness_below_70",
        "promotion_readiness_floor": "promotion_readiness_repair",
        "route_breadth": "route_breadth",
    }
    allocations = []
    for card in route_distance_cards.get("cards") or []:
        if not isinstance(card, dict):
            continue
        next_repair = str(card.get("next_best_repair") or "promotion_review")
        route = str(card.get("route_key") or "")
        lanes_by_name: dict[str, dict[str, Any]] = {}
        candidate = card.get("closest_candidate") if isinstance(card.get("closest_candidate"), dict) else {}
        distance = candidate.get("promotion_distance") if isinstance(candidate.get("promotion_distance"), dict) else {}
        for gate in distance.get("gates") or []:
            if not isinstance(gate, dict) or gate.get("passed"):
                continue
            lane = lane_by_gate.get(str(gate.get("gate") or ""))
            if lane:
                lanes_by_name[lane] = {
                    "lane": lane,
                    "variants_per_500": 80 if lane in {"route_breadth", "promotion_readiness_repair"} else 60,
                    "distance_to_pass": gate.get("distance_to_pass"),
                }
        if next_repair == "promotion_review" and not lanes_by_name:
            lanes_by_name["promotion_review_precheck"] = {"lane": "promotion_review_precheck", "variants_per_500": 0}
        if next_repair and next_repair != "promotion_review":
            lanes_by_name.setdefault(next_repair, {"lane": next_repair, "variants_per_500": 80})
        if card.get("open_gate_count") is not None:
            lanes_by_name.setdefault("holdout_preservation_control", {"lane": "holdout_preservation_control", "variants_per_500": 40})
        lanes = list(lanes_by_name.values())
        lanes = _normalize_variant_allocation(lanes, min(260, sum(int(row.get("variants_per_500") or 0) for row in lanes) or 120))
        allocations.append({
            "route_key": route,
            "closest_variant": candidate.get("variant"),
            "open_gate_count": card.get("open_gate_count"),
            "distance_points": card.get("distance_points"),
            "lanes": lanes,
        })
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "description": "Route-specific repair allocation so routes that already passed breadth stop spending breadth budget.",
        "routes": allocations[:50],
    }


def _best_repair_candidate_by_promotion_distance(rows: list[dict[str, Any]], evidence_by_variant: dict[str, dict[str, Any]]) -> dict[str, Any]:
    candidates = []
    for row in rows:
        evidence = evidence_by_variant.get(str(row.get("variant") or "")) or {}
        distance = _promotion_distance(row, evidence)
        candidates.append({
            "variant": row.get("variant"),
            "route_key": hunt_intel.route_key_from_row(row),
            "step2_pnl": row.get("step2_pnl"),
            "delta_vs_active": row.get("step2_delta_vs_active"),
            "robustness_score": evidence.get("robustness_score"),
            "holdout_delta_vs_active": ((evidence.get("train_holdout") or {}).get("holdout_delta_vs_active") if isinstance(evidence.get("train_holdout"), dict) else None),
            "evidence_adjusted_readiness_score": _evidence_adjusted_readiness(row, evidence),
            "promotion_distance": distance,
            "next_best_repair": distance.get("next_best_repair"),
        })
    candidates.sort(key=lambda item: (int((item.get("promotion_distance") or {}).get("open_gate_count") if (item.get("promotion_distance") or {}).get("open_gate_count") is not None else 99), float((item.get("promotion_distance") or {}).get("distance_points") if (item.get("promotion_distance") or {}).get("distance_points") is not None else 1e9), -float(item.get("delta_vs_active") or 0.0)))
    return candidates[0] if candidates else {}


def _next_command_recipe(args: argparse.Namespace, run_dir: Path, repair_now: dict[str, Any], route_budget_caps: dict[str, Any]) -> dict[str, Any]:
    mode = str(repair_now.get("recommended_hunt_mode") or "promotion_repair")
    focus_routes = list(dict.fromkeys(str(route) for route in (repair_now.get("recommended_focus_routes") or []) if str(route)))[: int(getattr(args, "max_focus_routes", 8) or 8)]
    repair_first = bool(repair_now.get("run_repair_cycle_now")) or str(repair_now.get("decision") or repair_now.get("recommended_action") or "").lower() in {"repair_first", "run_targeted_controlled_repair_first"}
    next_batch_size = max(500, int(getattr(args, "batch_size", 500) or 500)) if repair_first else int(getattr(args, "batch_size", 500) or 500)
    next_hours = max(0.15, float(getattr(args, "hours", 0.08) or 0.08)) if next_batch_size >= 500 else float(getattr(args, "hours", 0.08) or 0.08)
    command = [
        "python",
        "run_step2_three_hour_hunt.py",
        "--name",
        f"{getattr(args, 'name', 'step2_hunt')}_{'targeted500_repair_next' if next_batch_size >= 500 else 'repair_next'}",
        "--hours",
        str(round(next_hours, 4)),
        "--batch-size",
        str(next_batch_size),
        "--max-batches",
        "1" if next_batch_size >= 500 else str(int(getattr(args, "max_batches", 1) or 1)),
        "--max-cycles",
        "1",
        "--target-count",
        str(int(getattr(args, "target_count", 100) or 100)),
        "--hunt-mode",
        mode,
        "--exact-variant-count",
        "--promotion-survival-sort",
        "evidence_tier",
        "--runtime-bootstrap-json",
        str((run_dir / "runtime_handoff_controls.json").resolve()),
        "--json",
        "--json-verbosity",
        "digest",
        "--status-stdout-mode",
        "none",
        "--status-event-verbosity",
        "digest",
        "--artifact-profile",
        str(getattr(args, "artifact_profile", "compact") or "compact"),
        "--coordinator-artifact-mode",
        str(getattr(args, "coordinator_artifact_mode", "minimal") or "minimal"),
        "--score-cache-stats-mode",
        str(getattr(args, "score_cache_stats_mode", "fast") or "fast"),
    ]
    if str(getattr(args, "runtime_control_mode", "enforce") or "enforce") != "enforce":
        command.extend(["--runtime-control-mode", str(getattr(args, "runtime_control_mode", "observe") or "observe")])
    if bool(getattr(args, "allow_uncertified_cache", False)):
        command.append("--allow-uncertified-cache")
    for route in focus_routes:
        command.extend(["--focus-route", route])
    notes = [
        "Use promotion_repair for the next test run when repair_now is true.",
        "Use repair_exploration when repair yield is below the configured floor.",
        "Keep live-only and exact variant count enabled.",
        "Use one wrapper cycle for exact-count test runs so requested variants remain honest.",
        "Sort promotion survival by evidence tier when inspecting finalists.",
        "Bootstrap runtime controls from the previous run so focus, throttle, and behavior-diversity caps are active on cycle 0.",
        "Keep status stdout disabled for JSON digest recipes; full status events remain available in artifacts when requested.",
        "Keep coordinator artifacts minimal and score-cache stats fast unless debugging a specific raw artifact or cache-count question.",
    ]
    if bool(getattr(args, "allow_uncertified_cache", False)):
        notes.append("This recipe preserves --allow-uncertified-cache because the current run was diagnostic; certify or rebuild the cache before using results for promotion evidence.")
    if str(getattr(args, "runtime_control_mode", "enforce") or "enforce") != "enforce":
        notes.append("This recipe preserves runtime_control_mode=observe so diagnostic runs record controls without enforcing throttles.")
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "description": "Concrete next CLI posture produced from this run's repair decision.",
        "recommended_mode": mode,
        "recommended_run_size": "targeted_500" if next_batch_size >= 500 else "repeat_current_size",
        "focus_routes": focus_routes,
        "command": command,
        "command_string": _shell_command_string(command),
        "enforce_route_budget_caps": bool(route_budget_caps.get("overconcentrated")),
        "runtime_bootstrap_json": str((run_dir / "runtime_handoff_controls.json").resolve()),
        "notes": notes,
    }


def _align_next_command_recipe_with_handoff(next_command_recipe: dict[str, Any], runtime_handoff: dict[str, Any]) -> dict[str, Any]:
    """Make the human next command mirror the runtime bootstrap focus routes."""
    recipe = dict(next_command_recipe or {})
    handoff_focus = [
        str(route)
        for route in ((runtime_handoff or {}).get("focus_routes") or [])
        if str(route)
    ]
    existing_focus = [
        str(route)
        for route in (recipe.get("focus_routes") or [])
        if str(route)
    ]
    merged_focus = list(dict.fromkeys(handoff_focus + existing_focus))[:24]
    command = list(recipe.get("command") or [])
    cleaned = []
    idx = 0
    while idx < len(command):
        if command[idx] == "--focus-route" and idx + 1 < len(command):
            idx += 2
            continue
        cleaned.append(command[idx])
        idx += 1
    max_cli_focus = 12
    for route in merged_focus[:max_cli_focus]:
        cleaned.extend(["--focus-route", route])
    recipe["focus_routes"] = merged_focus
    recipe["runtime_handoff_focus_routes"] = handoff_focus[:24]
    recipe["command"] = cleaned
    recipe["command_string"] = _shell_command_string(cleaned)
    notes = list(recipe.get("notes") or [])
    notes.append("Focus routes are aligned to runtime_handoff_controls so stale-focus audits use the same repair center as the next command.")
    recipe["notes"] = list(dict.fromkeys(notes))
    return recipe


def _repair_success_scoreboard(rows: list[dict[str, Any]], evidence_by_variant: dict[str, dict[str, Any]], route_caps: dict[str, Any]) -> dict[str, Any]:
    robustness_values = []
    readiness_values = []
    route_breadth_failures = 0
    holdout_grade_failures = 0
    for row in rows:
        evidence = evidence_by_variant.get(str(row.get("variant") or "")) or {}
        bar = _promotion_minimum_bar(row, evidence=evidence)
        robustness_values.append(float(bar.get("robustness_score") or 0.0))
        readiness_values.append(float(bar.get("promotion_readiness_score") or 0.0))
        failures = set(str(reason) for reason in (bar.get("failures") or []))
        route_breadth_failures += 1 if "route_too_narrow" in failures else 0
        holdout_grade_failures += 1 if {"missing_holdout_credit", "holdout_positive_but_not_promotion_grade"} & failures else 0
    def avg(values: list[float]) -> float:
        return round(sum(values) / max(1, len(values)), 4)
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "description": "Scoreboard for the next repair test: it should improve these metrics while all kept variants still beat current Live.",
        "current": {
            "candidate_count": len(rows),
            "avg_robustness_score": avg(robustness_values),
            "avg_evidence_adjusted_readiness": avg(readiness_values),
            "route_breadth_failure_count": route_breadth_failures,
            "holdout_grade_failure_count": holdout_grade_failures,
            "overconcentrated_route_count": sum(1 for row in route_caps.get("route_caps") or [] if row.get("cap_required")),
        },
        "next_run_success_criteria": [
            "all kept variants still beat current Live",
            "avg robustness score increases",
            "evidence-adjusted readiness increases",
            "route_too_narrow failures decrease",
            "overconcentrated routes respect budget caps",
        ],
    }


def _repair_narrowness_warning(args: argparse.Namespace, variant_funnel: dict[str, Any]) -> dict[str, Any]:
    mode = str(getattr(args, "hunt_mode", "auto") or "auto")
    live_yield = float(variant_funnel.get("scored_to_live_stream_yield_pct") or 0.0)
    floor = float(getattr(args, "repair_yield_floor_pct", 1.0) or 1.0)
    active = mode in {"promotion_repair", "repair_exploration"} and live_yield < floor
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "active": active,
        "hunt_mode": mode,
        "live_yield_pct": live_yield,
        "yield_floor_pct": floor,
        "recommended_action": "loosen_repair" if active else "continue_current_aperture",
        "why": (
            f"Repair aperture is too narrow: live-yield {round(live_yield, 4)}% is below {floor}%."
            if active else
            "Repair aperture is within the configured live-yield floor."
        ),
    }


def _repair_progress_report(args: argparse.Namespace, run_dir: Path, rows: list[dict[str, Any]], evidence_by_variant: dict[str, dict[str, Any]], variant_funnel: dict[str, Any]) -> dict[str, Any]:
    previous = _latest_previous_run_summary(run_dir)
    current_tiers = Counter(str(_promotion_minimum_bar(row, evidence_by_variant.get(str(row.get("variant") or "")) or {}).get("evidence_tier") or "unknown") for row in rows)
    previous_failure = previous.get("top_failure_summary") if isinstance(previous.get("top_failure_summary"), dict) else {}
    previous_funnel = previous.get("variant_funnel") if isinstance(previous.get("variant_funnel"), dict) else {}
    prev_tiers = previous_failure.get("evidence_tier_counts") if isinstance(previous_failure.get("evidence_tier_counts"), dict) else {}
    current_best = rows[0] if rows else {}
    current_evidence = evidence_by_variant.get(str(current_best.get("variant") or "")) or {}
    current_distance = _promotion_distance(current_best, current_evidence) if current_best else {}
    prev_best = previous.get("best_repair_candidate_by_promotion_distance") if isinstance(previous.get("best_repair_candidate_by_promotion_distance"), dict) else {}
    prev_best_distance = prev_best.get("promotion_distance") if isinstance(prev_best.get("promotion_distance"), dict) else {}
    current_live_yield = float(variant_funnel.get("scored_to_live_stream_yield_pct") or 0.0)
    previous_live_yield = float(previous_funnel.get("scored_to_live_stream_yield_pct") or 0.0)
    current_live_beaters = int(variant_funnel.get("live_filtered_rows") or variant_funnel.get("live_beaters_streamed") or 0)
    previous_live_beaters = int(previous_funnel.get("live_filtered_rows") or previous_funnel.get("live_beaters_streamed") or 0)
    current_top100 = int(variant_funnel.get("top100_count") or variant_funnel.get("behavior_unique_rows") or len(rows))
    previous_top100 = int(previous_funnel.get("top100_count") or previous_funnel.get("behavior_unique_rows") or 0)
    current_points = float(current_distance.get("distance_points") or 0.0)
    previous_points = float(prev_best_distance.get("distance_points") or 0.0)
    near_delta = round(previous_points - current_points, 4) if prev_best_distance else None
    better = [
        "evidence tier improved" if _best_tier_rank(dict(current_tiers)) > _best_tier_rank(prev_tiers) else "",
        "top candidate passed route breadth" if current_distance and not any(g.get("gate") == "route_breadth" and not g.get("passed") for g in current_distance.get("gates") or []) else "",
        f"live-yield improved from {round(previous_live_yield, 4)}% to {round(current_live_yield, 4)}%" if previous_funnel and current_live_yield > previous_live_yield else "",
        f"live beaters improved from {previous_live_beaters} to {current_live_beaters}" if previous_funnel and current_live_beaters > previous_live_beaters else "",
        f"top100 kept count improved from {previous_top100} to {current_top100}" if previous_funnel and current_top100 > previous_top100 else "",
        f"closest promotion distance improved by {near_delta} points" if near_delta is not None and near_delta > 0 else "",
    ]
    worse = [
        "live-yield decreased" if previous_funnel and current_live_yield < previous_live_yield else "",
        "live beater count decreased" if previous_funnel and current_live_beaters < previous_live_beaters else "",
        "closest promotion distance widened" if near_delta is not None and near_delta < 0 else "",
    ]
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "previous_run_dir": previous.get("run_dir"),
        "what_got_better": [item for item in better if item],
        "what_got_worse": [item for item in worse if item],
        "current": {
            "live_yield_pct": variant_funnel.get("scored_to_live_stream_yield_pct"),
            "live_beaters": variant_funnel.get("live_filtered_rows"),
            "evidence_tier_counts": dict(current_tiers),
            "best_candidate": current_best.get("variant"),
            "best_route": hunt_intel.route_key_from_row(current_best) if current_best else "",
            "best_promotion_distance": current_distance,
            "top100_count": current_top100,
        },
        "previous": {
            "live_yield_pct": previous_live_yield if previous_funnel else None,
            "live_beaters": previous_live_beaters if previous_funnel else None,
            "evidence_tier_counts": prev_tiers,
            "top100_count": previous_top100 if previous_funnel else None,
            "best_promotion_distance": prev_best_distance,
        },
        "near_promotion_delta": {
            "previous_distance_points": previous_points if prev_best_distance else None,
            "current_distance_points": current_points if current_distance else None,
            "improvement_points": near_delta,
        },
    }


def _best_tier_rank(tiers: dict[str, Any]) -> int:
    return max([EVIDENCE_TIER_RANK.get(str(tier), 0) for tier, count in tiers.items() if int(count or 0) > 0] or [0])


def _latest_previous_run_summary(run_dir: Path) -> dict[str, Any]:
    root = Path(DEFAULT_OUT)
    if not root.exists():
        return {}
    candidates = sorted(
        [path for path in root.iterdir() if path.is_dir() and path.resolve() != run_dir.resolve() and (path / "final_summary.json").exists()],
        key=_run_dir_sort_key,
        reverse=True,
    )
    for prior in candidates[:10]:
        payload = hunt_intel.read_json(prior / "final_summary.json", {}) or {}
        if payload:
            return payload
    return {}


def _latest_previous_run_dir(run_dir: Path) -> Path | None:
    root = Path(DEFAULT_OUT)
    if not root.exists():
        return None
    candidates = sorted(
        [path for path in root.iterdir() if path.is_dir() and path.resolve() != run_dir.resolve() and (path / "final_summary.json").exists()],
        key=_run_dir_sort_key,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _load_runtime_bootstrap_controls(args: argparse.Namespace, run_dir: Path) -> dict[str, Any]:
    explicit = str(getattr(args, "runtime_bootstrap_json", "") or "")
    if explicit and Path(explicit).exists():
        payload = hunt_intel.read_json(explicit, {}) or {}
        if payload:
            payload["bootstrap_source_path"] = str(Path(explicit).resolve())
            return payload
    if not bool(getattr(args, "bootstrap_previous_runtime_controls", True)):
        return {}
    previous_dir = _latest_previous_run_dir(run_dir)
    if not previous_dir:
        return {}
    handoff = hunt_intel.read_json(previous_dir / "runtime_handoff_controls.json", {}) or {}
    if handoff:
        adapter = handoff.get("runtime_command_adapter") if isinstance(handoff.get("runtime_command_adapter"), dict) else {}
        if not adapter.get("quality_protection_controls") or not adapter.get("promotion_ready_repair_envelope"):
            route_admission = hunt_intel.read_json(previous_dir / "route_admission_gate.json", {}) or {}
            anti_alias = hunt_intel.read_json(previous_dir / "anti_alias_pressure.json", {}) or {}
            behavior = handoff.get("behavior_unique_generation_controls") if isinstance(handoff.get("behavior_unique_generation_controls"), dict) else {}
            if not behavior:
                behavior = hunt_intel.read_json(previous_dir / "behavior_unique_generation_controls.json", {}) or {}
            winner_distribution = hunt_intel.read_json(previous_dir / "winner_route_distribution.json", {}) or {}
            blocker_digest = hunt_intel.read_json(previous_dir / "promotion_blocker_digest.json", {}) or {}
            variant_funnel = hunt_intel.read_json(previous_dir / "variant_funnel.json", {}) or {}
            repair_progress = hunt_intel.read_json(previous_dir / "repair_progress_report.json", {}) or {}
            promotion_ready = hunt_intel.read_json(previous_dir / "promotion_ready_summary.json", {}) or {}
            quality = _promotion_quality_protection_controls(variant_funnel, repair_progress, blocker_digest, winner_distribution, route_admission)
            repair_now = hunt_intel.read_json(previous_dir / "repair_cycle_now_decision.json", {}) or {}
            route_caps = handoff.get("route_budget_caps") if isinstance(handoff.get("route_budget_caps"), dict) else hunt_intel.read_json(previous_dir / "repair_route_budget_caps.json", {}) or {}
            repair_allocation = handoff.get("repair_allocation_by_evidence_weakness") if isinstance(handoff.get("repair_allocation_by_evidence_weakness"), dict) else hunt_intel.read_json(previous_dir / "repair_allocation_by_evidence_weakness.json", {}) or {}
            survival_payload = hunt_intel.read_json(previous_dir / "promotion_survival_top100.json", {}) or {}
            survival_rows = (survival_payload.get("leaderboard") or survival_payload.get("top100")) if isinstance(survival_payload, dict) else survival_payload
            handoff = _runtime_handoff_controls(route_admission, anti_alias, behavior, quality, repair_now, route_caps, repair_allocation, promotion_ready, winner_distribution, blocker_digest, _promotion_survival_reference(survival_rows or []))
        handoff = _repair_empty_quality_targets(handoff)
        handoff["bootstrap_source_path"] = str((previous_dir / "runtime_handoff_controls.json").resolve())
        return handoff
    route_admission = hunt_intel.read_json(previous_dir / "route_admission_gate.json", {}) or {}
    anti_alias = hunt_intel.read_json(previous_dir / "anti_alias_pressure.json", {}) or {}
    behavior = hunt_intel.read_json(previous_dir / "behavior_unique_generation_controls.json", {}) or {}
    quality = hunt_intel.read_json(previous_dir / "promotion_quality_protection_controls.json", {}) or {}
    repair_now = hunt_intel.read_json(previous_dir / "repair_cycle_now_decision.json", {}) or {}
    route_caps = hunt_intel.read_json(previous_dir / "repair_route_budget_caps.json", {}) or {}
    repair_allocation = hunt_intel.read_json(previous_dir / "repair_allocation_by_evidence_weakness.json", {}) or {}
    if not any([route_admission, anti_alias, behavior, repair_now, route_caps]):
        return {}
    if not behavior:
        winner_distribution = hunt_intel.read_json(previous_dir / "winner_route_distribution.json", {}) or {}
        blocker_digest = hunt_intel.read_json(previous_dir / "promotion_blocker_digest.json", {}) or {}
        behavior = _behavior_unique_generation_controls(anti_alias, winner_distribution, route_admission, blocker_digest)
    if not quality:
        winner_distribution = hunt_intel.read_json(previous_dir / "winner_route_distribution.json", {}) or {}
        blocker_digest = hunt_intel.read_json(previous_dir / "promotion_blocker_digest.json", {}) or {}
        variant_funnel = hunt_intel.read_json(previous_dir / "variant_funnel.json", {}) or {}
        repair_progress = hunt_intel.read_json(previous_dir / "repair_progress_report.json", {}) or {}
        quality = _promotion_quality_protection_controls(variant_funnel, repair_progress, blocker_digest, winner_distribution, route_admission)
    winner_distribution = hunt_intel.read_json(previous_dir / "winner_route_distribution.json", {}) or {}
    blocker_digest = hunt_intel.read_json(previous_dir / "promotion_blocker_digest.json", {}) or {}
    promotion_ready = hunt_intel.read_json(previous_dir / "promotion_ready_summary.json", {}) or {}
    survival_payload = hunt_intel.read_json(previous_dir / "promotion_survival_top100.json", {}) or {}
    survival_rows = (survival_payload.get("leaderboard") or survival_payload.get("top100")) if isinstance(survival_payload, dict) else survival_payload
    payload = _runtime_handoff_controls(route_admission, anti_alias, behavior, quality, repair_now, route_caps, repair_allocation, promotion_ready, winner_distribution, blocker_digest, _promotion_survival_reference(survival_rows or []))
    payload = _repair_empty_quality_targets(payload)
    payload["bootstrap_source_path"] = str(previous_dir.resolve())
    return payload


def _apply_runtime_bootstrap_controls(run_dir: Path, controls: dict[str, Any]) -> None:
    if not controls:
        return
    _write_json(run_dir / "runtime_bootstrap_controls.json", controls)
    _merge_quarantine_controls(run_dir, controls)


def _promotion_review_precheck(row: dict[str, Any], evidence: dict[str, Any]) -> dict[str, Any]:
    if not row:
        return {"schema_version": 1, "skipped": True, "reason": "no_candidate"}
    bar = _promotion_minimum_bar(row, evidence=evidence)
    distance = _promotion_distance(row, evidence)
    failures = set(str(item) for item in (bar.get("failures") or []))
    repair_first_failures = {"promotion_readiness_below_floor", "route_too_narrow"}
    review_later = bool(failures) and failures.issubset(repair_first_failures) and float(bar.get("delta_vs_live") or 0.0) > 0.0
    decision = "approve_to_full_review" if bar.get("passed") else "do_not_review_yet_repair_first" if review_later else "expected_reject"
    if bar.get("passed"):
        human = "Candidate clears the promotion floor; run full promotion review."
    elif review_later:
        human = "Do not run full promotion review yet: this beats Live, but it needs readiness and route-breadth repair first."
    else:
        human = "Expected reject because: " + ", ".join(list(bar.get("failures") or [])[:5])
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "variant": row.get("variant"),
        "route_key": hunt_intel.route_key_from_row(row),
        "decision": decision,
        "reject_reasons": [] if bar.get("passed") else list(bar.get("failures") or []),
        "human_summary": human,
        "do_not_run_full_review_yet": review_later,
        "repair_first_reasons": list(sorted(failures & repair_first_failures)),
        "promotion_minimum_bar": bar,
        "promotion_distance": distance,
    }


def _post_test_recommendation_severity(
    auto_rec: dict[str, Any],
    quality_floor: dict[str, Any],
    anti_alias: dict[str, Any],
    repair_queue: dict[str, Any],
    variant_funnel: dict[str, Any] | None = None,
) -> dict[str, Any]:
    promotion_floor_kept = int(quality_floor.get("promotion_floor_kept_count") or quality_floor.get("kept_count") or 0)
    top100_kept = int((variant_funnel or {}).get("top100_count") or (variant_funnel or {}).get("behavior_unique_rows") or quality_floor.get("input_live_beater_count") or promotion_floor_kept)
    repairs = int(repair_queue.get("task_count") or 0)
    pressure = str(anti_alias.get("pressure") or "low")
    action = str(auto_rec.get("recommended_action") or "")
    if promotion_floor_kept == 0 and (repairs >= 3 or pressure == "high"):
        severity = "red"
        drivers = ["no promotion-floor candidate survived"]
        if repairs >= 3:
            drivers.append(f"{repairs} repair tasks are open")
        if pressure == "high":
            drivers.append("anti-alias pressure is high")
        why = "; ".join(drivers) + ". Fix these before scaling."
    elif action == "run_bigger" and promotion_floor_kept > 0 and pressure == "low":
        severity = "green"
        why = "Promotion-floor candidates exist and alias pressure is low."
    elif action == "loosen_repair":
        severity = "yellow"
        why = "Repair is learning but too narrow; widen into repair_exploration before scaling."
    else:
        severity = "yellow"
        why = "The machine can continue, but the next run should honor repair and evidence controls."
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "severity": severity,
        "recommended_action": action,
        "why": why,
        "kept_count": top100_kept,
        "top100_kept_count": top100_kept,
        "promotion_floor_kept_count": promotion_floor_kept,
        "repair_task_count": repairs,
        "anti_alias_pressure": pressure,
    }


def _top_failure_summary(
    rows: list[dict[str, Any]],
    repair_queue: dict[str, Any],
    suspicious: dict[str, Any],
    evidence_by_variant: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    failures = Counter()
    target_misses = Counter()
    tiers = Counter()
    ready_count = 0
    edge_cushions: list[float] = []
    for row in rows:
        evidence = (evidence_by_variant or {}).get(str(row.get("variant") or "")) or {}
        bar = _promotion_minimum_bar(row, evidence=evidence)
        distance = _promotion_distance(row, evidence)
        if distance.get("passed"):
            ready_count += 1
        failures.update((bar.get("failures") or []))
        target_misses.update(str(gate.get("gate")) for gate in (distance.get("report_only_targets") or []) if gate.get("gate"))
        tiers.update([str(bar.get("evidence_tier") or "unknown")])
        edge_cushions.append(float(bar.get("delta_vs_live") or 0.0))
    top_reasons = [{"reason": key, "count": value} for key, value in failures.most_common(12)]
    target_reasons = [{"target": key, "count": value} for key, value in target_misses.most_common(12)]
    if not rows:
        plain = "No live-beating candidates were kept, so promotion readiness cannot be evaluated yet."
    elif ready_count > 0:
        plain = f"{ready_count} candidate(s) clear the minimum promotion gate; remaining pool blockers are "
        plain += ", ".join(f"{item['count']} have {item['reason']}" for item in top_reasons[:3]) + "." if failures else "clear."
    elif not failures:
        plain = "Promotion-floor blockers are clear; review the promotion queue next."
    else:
        plain = "No candidate is promotable yet because "
        plain += ", ".join(f"{item['count']} have {item['reason']}" for item in top_reasons[:3]) + "."
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "top_failure_reasons": top_reasons,
        "hard_blockers": top_reasons,
        "promotion_grade_target_misses": target_reasons,
        "evidence_tier_counts": dict(tiers),
        "promotion_ready_count": ready_count,
        "small_edge_cushion": {
            "min_delta_vs_live": round(min(edge_cushions), 4) if edge_cushions else None,
            "median_delta_vs_live": round(sorted(edge_cushions)[len(edge_cushions) // 2], 4) if edge_cushions else None,
            "top10_min_delta_vs_live": round(min(edge_cushions[:10]), 4) if edge_cushions[:10] else None,
        },
        "repair_task_count": repair_queue.get("task_count") or 0,
        "suspicious_winner_count": suspicious.get("flagged_count") or 0,
        "why_no_candidate_is_promotable_yet": plain,
        "human_summary": (
            f"{sum(failures.values())} promotion-floor failures across top live beaters; "
            f"{repair_queue.get('task_count') or 0} repair tasks are open."
        ),
    }


def _promotion_blocker_digest(
    rows: list[dict[str, Any]],
    evidence_by_variant: dict[str, dict[str, Any]] | None = None,
    *,
    limit: int = 25,
) -> dict[str, Any]:
    """Human-readable explanation of why live beaters are not promotable yet."""
    evidence_by_variant = evidence_by_variant or {}
    reasons: dict[str, dict[str, Any]] = {}
    candidate_digests = []
    for idx, row in enumerate(rows[:limit], 1):
        variant = str(row.get("variant") or "")
        evidence = evidence_by_variant.get(variant) or {}
        bar = _promotion_minimum_bar(row, evidence=evidence)
        distance = _promotion_distance(row, evidence)
        holdout = evidence.get("train_holdout") if isinstance(evidence.get("train_holdout"), dict) else {}
        day = evidence.get("day_profile") if isinstance(evidence.get("day_profile"), dict) else {}
        failures = list(bar.get("failures") or [])
        gate_notes = []
        for failure in failures:
            if failure == "robustness_below_floor":
                gap = max(0.0, 55.0 - float(bar.get("robustness_score") or 0.0))
                note = f"robustness is {round(float(bar.get('robustness_score') or 0.0), 2)}, needs 55+"
                repair = "stabilize day/holdout behavior without giving up live edge"
                severity = gap
            elif failure == "promotion_readiness_below_floor":
                gap = max(0.0, 45.0 - float(bar.get("promotion_readiness_score") or 0.0))
                note = f"readiness is {round(float(bar.get('promotion_readiness_score') or 0.0), 2)}, needs 45+"
                repair = "raise readiness ingredients before spending promotion-review budget"
                severity = gap
            elif failure == "route_too_narrow":
                note = "winner is still route-narrow; it has not proven sibling ticker/phase breadth"
                repair = "force sibling-route validation and structural mutation"
                severity = 1.0
            elif failure == "holdout_positive_but_not_promotion_grade":
                note = (
                    f"holdout is positive ({round(float(holdout.get('holdout_delta_vs_active') or 0.0), 2)}) "
                    "but robustness/day quality is not promotion-grade"
                )
                repair = "preserve positive holdout while lifting robustness and removing red flags"
                severity = max(0.0, 55.0 - float(bar.get("robustness_score") or 0.0))
            elif failure == "missing_holdout_credit":
                note = "chronological holdout credit is missing"
                repair = "find variants with positive holdout delta before promotion review"
                severity = 1.0
            else:
                note = failure.replace("_", " ")
                repair = "repair this blocker while staying above current Live"
                severity = 1.0
            bucket = reasons.setdefault(failure, {
                "reason": failure,
                "count": 0,
                "max_gap": 0.0,
                "plain_english": note,
                "repair_focus": repair,
                "examples": [],
            })
            bucket["count"] += 1
            bucket["max_gap"] = max(float(bucket.get("max_gap") or 0.0), float(severity or 0.0))
            if len(bucket["examples"]) < 5:
                bucket["examples"].append({
                    "variant": row.get("variant"),
                    "route_key": hunt_intel.route_key_from_row(row),
                    "raw_pnl_rank": idx,
                    "detail": note,
                })
            gate_notes.append({"failure": failure, "detail": note, "repair_focus": repair})
        candidate_digests.append({
            "variant": row.get("variant"),
            "raw_pnl_rank": idx,
            "route_key": hunt_intel.route_key_from_row(row),
            "delta_vs_live": row.get("step2_delta_vs_active"),
            "robustness_score": bar.get("robustness_score"),
            "promotion_readiness_score": bar.get("promotion_readiness_score"),
            "holdout_delta_vs_active": holdout.get("holdout_delta_vs_active"),
            "beats_active_day_rate": day.get("beats_active_day_rate"),
            "blocking_open_gate_count": distance.get("blocking_open_gate_count"),
            "distance_points": distance.get("distance_points"),
            "next_best_repair": distance.get("next_best_repair"),
            "gate_notes": gate_notes,
        })
    reason_rows = sorted(
        reasons.values(),
        key=lambda item: (int(item.get("count") or 0), float(item.get("max_gap") or 0.0)),
        reverse=True,
    )
    universal = [
        item["reason"]
        for item in reason_rows
        if rows and int(item.get("count") or 0) == min(len(rows), limit)
    ]
    if not rows:
        summary = "No live-beating candidates were kept, so there is nothing promotion-reviewable."
    elif universal:
        summary = "Every kept live beater is blocked by: " + ", ".join(universal[:5]) + "."
    else:
        summary = "Promotion blockers are mixed; repair the highest-count blockers first."
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "evaluated_candidate_count": min(len(rows), limit),
        "reason_counts": reason_rows,
        "universal_blockers": universal,
        "candidate_digests": candidate_digests,
        "human_summary": summary,
    }


def _anti_alias_pressure_report(variant_funnel: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    live = int(variant_funnel.get("live_filtered_rows") or variant_funnel.get("live_beaters_streamed") or 0)
    behavior = int(variant_funnel.get("behavior_unique_rows") or len(rows) or 0)
    collapse = max(0, live - behavior)
    collapse_rate = collapse / max(1, live)
    pressure = "high" if live >= 10 and collapse_rate >= 0.60 else "medium" if live >= 5 and collapse_rate >= 0.35 else "low"
    route_counts = Counter(hunt_intel.route_key_from_row(row) for row in rows)
    top_route, top_route_count = route_counts.most_common(1)[0] if route_counts else ("", 0)
    route_share = top_route_count / max(1, behavior)
    if pressure == "low" and behavior >= 5 and route_share >= 0.80:
        pressure = "medium"
    structural_routes = [route for route, _count in route_counts.most_common(12) if route and route != "unrouted"]
    force_structural = pressure == "high" or (pressure == "medium" and route_share >= 0.70)
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "live_filtered_rows": live,
        "behavior_unique_rows": behavior,
        "live_to_behavior_unique_collapse": collapse,
        "collapse_rate": round(collapse_rate, 6),
        "pressure": pressure,
        "hard_behavior_unique_warning": pressure == "high",
        "minimum_behavior_unique_target": max(10, int(round(live * 0.40))) if pressure == "high" else max(5, behavior),
        "minimum_big_run_behavior_unique_target": max(20, int(round(live * 0.50))) if live else 20,
        "big_run_ready": pressure == "low" and behavior >= max(20, int(round(live * 0.50))) if live else False,
        "route_concentration": {
            "top_route": top_route,
            "top_route_count": top_route_count,
            "top_route_share_pct": round(route_share * 100.0, 4),
            "concentrated": behavior >= 5 and route_share >= 0.70,
        },
        "structural_mutation_routes": structural_routes,
        "recommended_action": "force_structural_mutation" if force_structural else "monitor_or_cap_alias_families",
    }


def _behavior_unique_generation_controls(
    anti_alias: dict[str, Any],
    winner_distribution: dict[str, Any],
    route_admission: dict[str, Any],
    blocker_digest: dict[str, Any],
) -> dict[str, Any]:
    pressure = str(anti_alias.get("pressure") or "low")
    high_pressure = pressure == "high"
    winner_top_route = str((winner_distribution or {}).get("top_route") or "")
    throttle_routes = [str(route) for route in (route_admission.get("throttle_routes") or []) if str(route)]
    structural_routes = list(dict.fromkeys(
        [str(route) for route in (anti_alias.get("structural_mutation_routes") or []) if str(route)]
        + throttle_routes
        + ([winner_top_route] if winner_top_route else [])
    ))
    diversity_routes = list(structural_routes)
    for route in structural_routes:
        for sibling in _sibling_routes(route):
            if sibling not in diversity_routes:
                diversity_routes.append(sibling)
    universal = set(str(item) for item in (blocker_digest.get("universal_blockers") or []))
    winner_share = float((winner_distribution or {}).get("top_route_share_pct") or 0.0)
    winner_dominant = bool(winner_top_route and winner_share >= 50.0)
    target_behavior = int(anti_alias.get("minimum_big_run_behavior_unique_target") or 20)
    behavior_count = int(anti_alias.get("behavior_unique_rows") or 0)
    needs_big_pool_lift = behavior_count < target_behavior
    cap_pct = 18.0 if high_pressure else 22.0 if (pressure == "medium" or needs_big_pool_lift) else 28.0
    max_share_pct_by_route = {route: cap_pct for route in structural_routes if route}
    for route in throttle_routes:
        max_share_pct_by_route[route] = min(max_share_pct_by_route.get(route, cap_pct), 12.0 if high_pressure else 18.0)
    if winner_dominant:
        max_share_pct_by_route[winner_top_route] = min(max_share_pct_by_route.get(winner_top_route, cap_pct), 12.0)
    max_winner_share_pct_by_route = {
        route: 20.0 if route == winner_top_route and winner_dominant else 25.0
        for route in structural_routes
        if route
    }
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "active": high_pressure or needs_big_pool_lift or bool(throttle_routes) or winner_dominant or bool({"promotion_readiness_below_floor", "route_too_narrow"} & universal),
        "pressure": pressure,
        "target_behavior_unique_count": target_behavior,
        "behavior_unique_count": behavior_count,
        "big_run_behavior_pool_lift_required": needs_big_pool_lift,
        "clone_prevention_mode": "hard" if high_pressure else "medium" if pressure == "medium" else "monitor",
        "structural_mutation_routes": structural_routes[:12],
        "diversity_focus_routes": diversity_routes[:16],
        "max_sample_share_pct_by_route": max_share_pct_by_route,
        "max_winner_share_pct_by_route": max_winner_share_pct_by_route,
        "default_max_sample_share_pct": 22.0 if needs_big_pool_lift else 28.0 if high_pressure else 24.0 if pressure == "medium" else 0.0,
        "required_winner_route_count": 5 if winner_dominant else 4,
        "required_behavior_unique_lift": True if high_pressure else False,
        "winner_dominance_controls": {
            "active": winner_dominant,
            "top_route": winner_top_route,
            "top_route_share_pct": winner_share,
            "max_next_sample_share_pct": max_share_pct_by_route.get(winner_top_route),
            "max_next_winner_share_pct": max_winner_share_pct_by_route.get(winner_top_route),
            "action": "cap_top_winner_route_and_force_sibling_routes" if winner_dominant else "monitor",
        },
        "repair_objectives": [
            "increase behavior-unique live beaters before top100 filtering",
            "force route-breadth siblings for every route-narrow winner",
            "raise promotion readiness while preserving live edge",
        ],
    }


def _promotion_quality_protection_controls(
    variant_funnel: dict[str, Any],
    repair_progress: dict[str, Any],
    blocker_digest: dict[str, Any],
    winner_distribution: dict[str, Any],
    route_admission: dict[str, Any],
) -> dict[str, Any]:
    near_delta = repair_progress.get("near_promotion_delta") if isinstance(repair_progress.get("near_promotion_delta"), dict) else {}
    improvement = near_delta.get("improvement_points")
    distance_widened = improvement is not None and float(improvement or 0.0) < 0.0
    universal = set(str(item) for item in (blocker_digest.get("universal_blockers") or []))
    evaluated_count = int(blocker_digest.get("evaluated_candidate_count") or 0)
    frequent_blockers = set()
    for row in blocker_digest.get("reason_counts") or []:
        if not isinstance(row, dict):
            continue
        reason = str(row.get("reason") or "")
        count = int(row.get("count") or 0)
        threshold = 0.25 if reason == "route_too_narrow" else 0.40
        if reason and evaluated_count and count / max(1, evaluated_count) >= threshold:
            frequent_blockers.add(reason)
    live_yield = float(variant_funnel.get("scored_to_live_stream_yield_pct") or 0.0)
    top_route = str((winner_distribution or {}).get("top_route") or "")
    dominant_share = float((winner_distribution or {}).get("top_route_share_pct") or 0.0)
    winner_dominant = bool(top_route and dominant_share >= 50.0)
    active = bool(
        distance_widened
        or winner_dominant
        or {"promotion_readiness_below_floor", "route_too_narrow"} & (universal | frequent_blockers)
        or {"robustness_below_floor", "holdout_positive_but_not_promotion_grade", "missing_holdout_credit"} & (universal | frequent_blockers)
        or live_yield < 3.0
    )
    avoid_as_center = {str(route) for route in (route_admission.get("avoid_as_center_routes") or []) if str(route)}
    protected_candidates = [str(route) for route in (route_admission.get("preserved_focus_routes") or []) if str(route) and str(route) not in avoid_as_center]
    if top_route:
        protected_candidates.append(top_route)
    protected_routes = [route for route in list(dict.fromkeys(protected_candidates)) if route not in avoid_as_center]
    throttle_routes = list(dict.fromkeys(
        [str(route) for route in (route_admission.get("throttle_routes") or []) if str(route)]
        + ([top_route] if active and winner_dominant and top_route else [])
    ))
    capped_protected_routes = protected_routes if active and (distance_widened or frequent_blockers) else []
    max_sample_share = {
        route: 8.0 if distance_widened and route == top_route else 12.0 if distance_widened else 18.0
        for route in list(dict.fromkeys(throttle_routes + capped_protected_routes))
    }
    max_winner_share = {
        route: 20.0 if route == top_route else 25.0
        for route in list(dict.fromkeys(throttle_routes + capped_protected_routes))
    }
    target_failures = []
    blocker_source = universal | frequent_blockers
    if "robustness_below_floor" in blocker_source:
        target_failures.append("robustness_below_floor")
    if "holdout_positive_but_not_promotion_grade" in blocker_source:
        target_failures.append("holdout_positive_but_not_promotion_grade")
    if "missing_holdout_credit" in blocker_source:
        target_failures.append("missing_holdout_credit")
    if "promotion_readiness_below_floor" in blocker_source:
        target_failures.append("promotion_readiness_below_floor")
    if "route_too_narrow" in blocker_source:
        target_failures.append("route_breadth")
    if active and not target_failures:
        target_failures.extend(["promotion_readiness_below_floor", "robustness_below_floor"])
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "active": active,
        "quality_protection_mode": "hard" if distance_widened else "medium" if active else "monitor",
        "promotion_distance_widened": distance_widened,
        "promotion_distance_improvement_points": improvement,
        "live_yield_pct": live_yield,
        "universal_blockers": sorted(universal),
        "frequent_blockers": sorted(frequent_blockers),
        "target_failures": target_failures,
        "protected_routes": protected_routes[:16],
        "throttle_routes": throttle_routes[:16],
        "max_sample_share_pct_by_route": max_sample_share,
        "max_winner_share_pct_by_route": max_winner_share,
        "quality_caps_active_on_protected_routes": bool(capped_protected_routes),
        "dominant_winner_route": top_route if winner_dominant else "",
        "dominant_winner_share_pct": dominant_share if winner_dominant else 0.0,
        "min_robustness_floor": 55.0,
        "min_readiness_floor": 45.0,
        "admit_new_center_only_if": [
            "promotion distance improves or stays flat",
            "robustness floor failures are no longer universal",
            "winner top route share is below 60%",
            "at least one promotion-ready candidate exists before scaling",
        ],
    }


def _repair_empty_quality_targets(controls: dict[str, Any]) -> dict[str, Any]:
    if not controls:
        return controls
    payload = dict(controls)
    adapter = payload.get("runtime_command_adapter") if isinstance(payload.get("runtime_command_adapter"), dict) else {}
    quality = adapter.get("quality_protection_controls") if isinstance(adapter.get("quality_protection_controls"), dict) else {}
    if not quality:
        quality = payload.get("promotion_quality_protection_controls") if isinstance(payload.get("promotion_quality_protection_controls"), dict) else {}
    if not quality or not quality.get("active") or quality.get("target_failures"):
        return controls
    repaired_targets = ["promotion_readiness_below_floor", "robustness_below_floor"]
    quality = dict(quality)
    quality["target_failures"] = repaired_targets
    quality["empty_target_repair"] = {
        "active": True,
        "reason": "active quality protection from a no-winner diagnostic must not erase repair intent",
        "fallback_target_failures": repaired_targets,
    }
    if adapter:
        adapter = dict(adapter)
        commands = []
        for command in adapter.get("commands") or []:
            if not isinstance(command, dict):
                continue
            patched = dict(command)
            if not patched.get("target_failures"):
                patched["target_failures"] = repaired_targets
                patched["prefer_promotion_readiness"] = True
                patched["preserve_holdout"] = True
            commands.append(patched)
        adapter["commands"] = commands
        adapter["quality_protection_controls"] = quality
        adapter["prefer_promotion_readiness"] = True
        adapter["route_breadth_required"] = False
        payload["runtime_command_adapter"] = adapter
    payload["promotion_quality_protection_controls"] = quality
    return payload


def _promotion_ready_repair_envelope(
    winner_distribution: dict[str, Any],
    quality_controls: dict[str, Any],
    blocker_digest: dict[str, Any],
    promotion_ready: dict[str, Any] | None = None,
    route_admission: dict[str, Any] | None = None,
    promotion_survival_reference: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a tight next-run envelope when winners beat Live but are not promotable yet."""
    promotion_ready = promotion_ready or {}
    route_admission = route_admission or {}
    universal = {str(item) for item in (blocker_digest.get("universal_blockers") or []) if str(item)}
    evaluated_count = int(blocker_digest.get("evaluated_candidate_count") or 0)
    high_count_blockers = set()
    for row in blocker_digest.get("reason_counts") or []:
        if not isinstance(row, dict):
            continue
        reason = str(row.get("reason") or "")
        count = int(row.get("count") or 0)
        if reason and evaluated_count and count / max(1, evaluated_count) >= 0.60:
            high_count_blockers.add(reason)
    target_failures = [str(item) for item in (quality_controls.get("target_failures") or []) if str(item)]
    if quality_controls.get("active") and not target_failures:
        target_failures = ["promotion_readiness_below_floor", "robustness_below_floor"]
    if ("route_too_narrow" in universal or "route_too_narrow" in high_count_blockers) and "route_breadth" not in target_failures:
        target_failures.append("route_breadth")
    for failure in ("promotion_readiness_below_floor", "robustness_below_floor", "holdout_positive_but_not_promotion_grade", "missing_holdout_credit"):
        if (failure in universal or failure in high_count_blockers) and failure not in target_failures:
            target_failures.append(failure)

    ready_count = int(promotion_ready.get("promotion_ready_count") or 0)
    survival_ref = promotion_survival_reference if isinstance(promotion_survival_reference, dict) else {}
    survival_route = str(survival_ref.get("route_key") or "")
    survival_failures = {str(item) for item in (survival_ref.get("failures") or []) if str(item)}
    survival_distance = survival_ref.get("promotion_distance") if isinstance(survival_ref.get("promotion_distance"), dict) else {}
    survival_distance_points = survival_ref.get("distance_points")
    if survival_distance_points is None:
        survival_distance_points = survival_distance.get("distance_points")
    try:
        survival_distance_points = float(survival_distance_points if survival_distance_points is not None else 999.0)
    except (TypeError, ValueError):
        survival_distance_points = 999.0
    readiness_gap = max(0.0, 45.0 - float(survival_ref.get("promotion_readiness_score") or 0.0))
    top_route = survival_route or str((winner_distribution or {}).get("top_route") or "")
    route_counts = winner_distribution.get("route_counts") if isinstance(winner_distribution.get("route_counts"), dict) else {}
    protected_candidates = [
        str(route)
        for route in (quality_controls.get("protected_routes") or [])
        if str(route) and str(route) in route_counts
    ]
    if top_route:
        protected_candidates.insert(0, top_route)
    anchors = list(dict.fromkeys(route for route in protected_candidates if route and route != "unrouted"))[:1]
    active_blockers = bool({"promotion_readiness_below_floor", "route_too_narrow", "robustness_below_floor", "holdout_positive_but_not_promotion_grade"} & (universal | high_count_blockers))
    distance_widened = bool(quality_controls.get("promotion_distance_widened"))
    active = bool(anchors and active_blockers and (ready_count <= 0 or distance_widened or ready_count < 3))
    if not active:
        return {
            "schema_version": 1,
            "source": "run_step2_three_hour_hunt",
            "active": False,
            "reason": "promotion-ready repair envelope not needed",
            "promotion_ready_count": ready_count,
            "universal_blockers": sorted(universal),
            "high_count_blockers": sorted(high_count_blockers),
        }

    close_siblings: list[str] = []
    for route in anchors:
        close_siblings.extend(_sibling_routes(route))
    anchor_setups = {route.split("|")[1] for route in anchors if len(route.split("|")) == 3}
    for route in route_counts:
        parts = str(route).split("|")
        if len(parts) == 3 and parts[1] in anchor_setups and route not in anchors:
            close_siblings.append(str(route))
    close_siblings = list(dict.fromkeys(route for route in close_siblings if route and route not in anchors))[:16]

    readiness_active = "promotion_readiness_below_floor" in set(target_failures) or "promotion_readiness_below_floor" in survival_failures
    robust_active = bool({"robustness_below_floor", "holdout_positive_but_not_promotion_grade"} & (set(target_failures) | survival_failures))
    breadth_active = "route_breadth" in set(target_failures)
    sample_caps = {route: 18.0 for route in anchors}
    sample_caps.update({route: 14.0 for route in close_siblings})
    winner_caps = {route: 18.0 for route in anchors}
    winner_caps.update({route: 14.0 for route in close_siblings})
    only_readiness_survival_gap = bool(survival_failures) and survival_failures.issubset({"promotion_readiness_below_floor"})
    finish_mode = bool(readiness_active and not breadth_active and (only_readiness_survival_gap or survival_distance_points <= 15.0))
    regression_guard_active = distance_widened
    primary_lane = (
        "promotion_ready_finish"
        if finish_mode else
        "promotion_readiness_repair"
        if readiness_active and not breadth_active else
        "edge_preservation_repair"
    )
    secondary_for_anchor = []
    if robust_active:
        secondary_for_anchor.extend(["robustness_holdout_repair", "robustness_floor_repair", "holdout_grade_repair"])
    elif {"holdout_positive_but_not_promotion_grade", "missing_holdout_credit", "promotion_grade_holdout_credit"} & (set(target_failures) | survival_failures):
        secondary_for_anchor.append("holdout_grade_repair")
    secondary_for_anchor = list(dict.fromkeys(secondary_for_anchor))
    commands = []
    for route in anchors:
        commands.append({
            "route_key": route,
            "action": "repair",
            "source": "promotion_ready_repair_envelope",
            "mutation_width": "tight" if primary_lane == "promotion_readiness_repair" else "micro",
            "target_failures": target_failures,
            "primary_repair_lane": primary_lane,
            "secondary_repair_lanes": secondary_for_anchor,
            "quality_protection": True,
            "edge_preservation": primary_lane == "edge_preservation_repair",
            "finish_mode": finish_mode,
            "readiness_gap_to_floor": round(readiness_gap, 4),
            "preserve_robustness_floor": finish_mode or robust_active,
            "preserve_holdout_grade": finish_mode or robust_active,
            "regression_guard_active": regression_guard_active,
            "close_sibling_only": False,
            "prefer_promotion_readiness": "promotion_readiness_below_floor" in set(target_failures),
            "preserve_holdout": True,
            "route_breadth_required": "route_breadth" in set(target_failures),
        })
    for route in close_siblings[:12]:
        commands.append({
            "route_key": route,
            "action": "revalidate",
            "source": "promotion_ready_repair_envelope",
            "mutation_width": "tight",
            "target_failures": target_failures,
            "primary_repair_lane": (
                "robustness_floor_repair"
                if robust_active and not breadth_active else
                "close_sibling_revalidation"
            ),
            "secondary_repair_lanes": ["promotion_readiness_repair"] if readiness_active else [],
            "quality_protection": True,
            "edge_preservation": False,
            "finish_mode": False,
            "readiness_gap_to_floor": round(readiness_gap, 4),
            "preserve_robustness_floor": robust_active,
            "preserve_holdout_grade": robust_active,
            "regression_guard_active": regression_guard_active,
            "close_sibling_only": True,
            "prefer_promotion_readiness": "promotion_readiness_below_floor" in set(target_failures),
            "preserve_holdout": True,
            "route_breadth_required": "route_breadth" in set(target_failures),
        })
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "active": True,
        "reason": (
            "promotion-ready candidates exist but bench depth is still thin; preserve the ready anchor while repairing nearby evidence"
            if ready_count > 0 else
            "live beaters exist but no candidate is promotion-ready; run the next repair pass inside the proven edge family"
        ),
        "promotion_ready_count": ready_count,
        "universal_blockers": sorted(universal),
        "high_count_blockers": sorted(high_count_blockers),
        "promotion_survival_reference": survival_ref,
        "near_candidate_finish_mode": finish_mode,
        "protected_candidate": survival_ref.get("variant") or "",
        "readiness_gap_to_floor": round(readiness_gap, 4),
        "regression_guard_active": regression_guard_active,
        "target_failures": target_failures,
        "edge_preservation_routes": anchors,
        "close_sibling_routes": close_siblings,
        "allowed_routes": list(dict.fromkeys(anchors + close_siblings))[:24],
        "commands": commands[:16],
        "max_sample_share_pct_by_route": sample_caps,
        "max_winner_share_pct_by_route": winner_caps,
        "outside_envelope_sample_weight_multiplier": 0.08,
        "generic_route_deprioritize_patterns": ["*|*|*"],
        "repair_mix": {
            "readiness_first_pct": 55 if finish_mode else 45 if readiness_active else 15,
            "robustness_floor_pct": 20 if robust_active else 5,
            "holdout_grade_pct": 15 if robust_active else 5,
            "edge_preservation_pct": 10 if finish_mode else 15,
            "close_sibling_validation_pct": 5 if not breadth_active else 25,
        },
        "human_summary": (
            "Next hunt should keep the proven edge routes as anchors, test only close ticker/phase siblings, "
            "and explicitly optimize readiness, robustness, breadth, and holdout quality."
        ),
    }


def _promotion_ready_summary(rows: list[dict[str, Any]], evidence_by_variant: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    ready = []
    near = []
    for idx, row in enumerate(rows, 1):
        evidence = (evidence_by_variant or {}).get(str(row.get("variant") or "")) or {}
        distance = _promotion_distance(row, evidence)
        item = {
            "variant": row.get("variant"),
            "raw_pnl_rank": idx,
            "route_key": hunt_intel.route_key_from_row(row),
            "step2_pnl": row.get("step2_pnl"),
            "delta_vs_live": row.get("step2_delta_vs_active"),
            "promotion_distance": distance,
        }
        if distance.get("passed"):
            ready.append(item)
        else:
            near.append(item)
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "promotion_ready_count": len(ready),
        "promotion_ready_top10": ready[:10],
        "closest_not_ready_top10": sorted(
            near,
            key=lambda item: (
                int((item.get("promotion_distance") or {}).get("open_gate_count") or 999),
                float((item.get("promotion_distance") or {}).get("distance_points") or 999999.0),
                -float(item.get("delta_vs_live") or 0.0),
            ),
        )[:10],
    }


def _apply_route_breadth_context(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Credit route breadth when the same setup has live-beating sibling support in the kept pool."""
    setup_routes: dict[str, set[str]] = defaultdict(set)
    setup_tickers: dict[str, set[str]] = defaultdict(set)
    setup_phases: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        match = hunt_intel.route_match(row)
        setup = str(match.get("setup_type") or "")
        if not setup:
            continue
        route = hunt_intel.route_key_from_row(row)
        setup_routes[setup].add(route)
        ticker = str(match.get("ticker") or "*")
        phase = str(match.get("session_phase") or "*")
        setup_tickers[setup].add(ticker)
        setup_phases[setup].add(phase)
    enriched = []
    for row in rows:
        item = row
        match = hunt_intel.route_match(row)
        setup = str(match.get("setup_type") or "")
        route = hunt_intel.route_key_from_row(row)
        ticker = str(match.get("ticker") or "*")
        phase = str(match.get("session_phase") or "*")
        distinct_tickers = setup_tickers.get(setup, set())
        distinct_phases = setup_phases.get(setup, set())
        concrete_tickers = {value for value in distinct_tickers if value and value != "*"}
        concrete_phases = {value for value in distinct_phases if value and value != "*"}
        broad_route = bool(setup and (ticker == "*" or phase == "*") and (len(concrete_tickers) >= 2 or len(concrete_phases) >= 2))
        sibling_ticker_support = bool(setup and ticker != "*" and len(concrete_tickers - {ticker}) >= 1)
        sibling_phase_support = bool(setup and phase != "*" and len(concrete_phases - {phase}) >= 1)
        passed = bool(setup and (broad_route or sibling_ticker_support or sibling_phase_support))
        credit = {
            "schema_version": 1,
            "passed": passed,
            "route_key": route,
            "setup_type": setup,
            "supporting_routes": sorted(setup_routes.get(setup, set()))[:12],
            "distinct_tickers": sorted(distinct_tickers),
            "distinct_phases": sorted(distinct_phases),
            "concrete_distinct_tickers": sorted(concrete_tickers),
            "concrete_distinct_phases": sorted(concrete_phases),
            "reason": (
                "setup has live-beating concrete ticker/phase sibling support in this kept pool"
                if passed else
                "setup has not yet proven concrete live-beating sibling breadth in this kept pool"
            ),
        }
        item["route_breadth_credit"] = credit
        existing_tags = set(str(tag) for tag in (item.get("learning_tags") or hunt_intel.candidate_learning_tags(item)))
        if passed:
            existing_tags.discard("route_narrow")
            existing_tags.add("route_broader")
            existing_tags.add("route_breadth_supported")
        item["learning_tags"] = sorted(existing_tags)
        enriched.append(item)
    return enriched


def _closest_to_promotion_summary(rows: list[dict[str, Any]], evidence_by_variant: dict[str, dict[str, Any]] | None = None, limit: int = 10) -> dict[str, Any]:
    candidates = []
    for idx, row in enumerate(rows, 1):
        evidence = (evidence_by_variant or {}).get(str(row.get("variant") or "")) or {}
        distance = _promotion_distance(row, evidence)
        candidates.append({
            "variant": row.get("variant"),
            "raw_pnl_rank": idx,
            "route_key": hunt_intel.route_key_from_row(row),
            "step2_pnl": row.get("step2_pnl"),
            "delta_vs_live": row.get("step2_delta_vs_active"),
            "blocking_open_gate_count": distance.get("blocking_open_gate_count"),
            "distance_points": distance.get("distance_points"),
            "next_best_repair": distance.get("next_best_repair"),
            "report_only_targets": distance.get("report_only_targets") or [],
            "promotion_distance": distance,
        })
    ordered = sorted(
        candidates,
        key=lambda item: (
            int(item.get("blocking_open_gate_count") or 999),
            float(item.get("distance_points") or 999999.0),
            -float(item.get("delta_vs_live") or 0.0),
        ),
    )
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "description": "Closest candidates by promotion blockers first, separate from raw P/L rank.",
        "closest_top10": ordered[:limit],
    }


def _route_concentration_warning(rows: list[dict[str, Any]], route_budget_caps: dict[str, Any] | None = None) -> dict[str, Any]:
    counts = Counter(hunt_intel.route_key_from_row(row) for row in rows)
    total = len(rows)
    top_route, top_count = counts.most_common(1)[0] if counts else ("", 0)
    share = top_count / max(1, total)
    caps = route_budget_caps or {}
    compliance = bool(caps) and not any(bool(row.get("cap_required")) for row in caps.get("route_caps") or [])
    pool_too_narrow = int(caps.get("route_count") or len(counts)) < int(caps.get("min_distinct_repair_routes") or 0)
    active = total >= 5 and share >= 0.60
    caution = total >= 5 and share >= 0.50
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "active": active,
        "caution": caution,
        "scale_block_threshold_pct": 60.0,
        "caution_threshold_pct": 50.0,
        "top_route": top_route,
        "top_route_count": top_count,
        "top_route_share_pct": round(share * 100.0, 4),
        "route_counts": dict(counts.most_common(25)),
        "cap_warning": bool(pool_too_narrow),
        "cap_warning_reason": "route pool is too narrow even if numeric caps are respected" if pool_too_narrow else "",
        "cap_compliance_is_not_enough": bool((compliance or caps) and active),
        "recommended_action": "expand ticker/phase siblings and force structural mutation" if active else "keep route caps and monitor concentration" if caution else "continue_route_caps",
    }


def _opaque_route_key(route: str) -> bool:
    route = str(route or "")
    return len(route) == 24 and all(char in "0123456789abcdef" for char in route.lower())


def _manifest_route_distribution(run_dir: Path) -> Counter:
    dist: Counter = Counter()
    for path in sorted(run_dir.rglob("scored_variants.jsonl")):
        for row in _read_jsonl(path):
            route = hunt_intel.route_key_from_row(row)
            dist[route] += 1
    return dist


def _actual_route_allocation(run_dir: Path) -> dict[str, Any]:
    events = _read_jsonl(run_dir / "streaming_telemetry.jsonl")
    dist: Counter = Counter()
    scored_total = 0
    for event in events:
        summary = event.get("summary") if isinstance(event.get("summary"), dict) else event
        route_dist = summary.get("sampled_route_distribution_full") if isinstance(summary.get("sampled_route_distribution_full"), dict) else {}
        if not route_dist:
            route_dist = summary.get("sampled_route_distribution") if isinstance(summary.get("sampled_route_distribution"), dict) else {}
        scored_total = max(scored_total, int(summary.get("scored_total") or 0))
        for route, count in route_dist.items():
            dist[str(route)] += int(count or 0)
    source = "streaming_telemetry"
    opaque_count = sum(count for route, count in dist.items() if _opaque_route_key(route))
    human_count = sum(count for route, count in dist.items() if not _opaque_route_key(route) and route not in {"", "unknown", "unrouted"})
    manifest_dist = Counter()
    if not dist or opaque_count > human_count:
        manifest_dist = _manifest_route_distribution(run_dir)
        if manifest_dist:
            dist = manifest_dist
            source = "scored_manifest_fallback"
            opaque_count = 0
            human_count = sum(count for route, count in dist.items() if route not in {"", "unknown", "unrouted"})
    total = sum(dist.values())
    unrouted_count = int(dist.get("unrouted") or 0) + int(dist.get("") or 0) + int(dist.get("unknown") or 0)
    human_coverage_pct = round(human_count / max(1, total) * 100.0, 4)
    coverage_pct = round(total / max(1, scored_total or total) * 100.0, 4)
    top_route, top_count = dist.most_common(1)[0] if dist else ("", 0)
    top_share = top_count / max(1, total)
    family_dist: Counter = Counter()
    for route, count in dist.items():
        parts = str(route).split("|")
        family = "|".join(parts[:2]) if len(parts) >= 2 else str(route)
        family_dist[family] += int(count or 0)
    top_family, top_family_count = family_dist.most_common(1)[0] if family_dist else ("", 0)
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "allocation_source": source,
        "telemetry_events": len(events),
        "sampled_route_total": total,
        "scored_total_seen": scored_total or total,
        "sample_coverage_pct": coverage_pct,
        "human_route_key_count": human_count,
        "opaque_route_key_count": opaque_count,
        "unrouted_count": unrouted_count,
        "human_route_key_coverage_pct": human_coverage_pct,
        "human_route_key_coverage_ok": opaque_count == 0 and human_count == total and total > 0,
        "human_route_key_coverage_status": "clean" if opaque_count == 0 and human_count == total and total > 0 else "partial",
        "sample_coverage_ok": coverage_pct >= 80.0,
        "route_distribution": dict(dist.most_common(50)),
        "top_route": top_route,
        "top_route_count": top_count,
        "top_route_share_pct": round(top_share * 100.0, 4),
        "top_route_over_big_run_cap": top_share >= 0.35,
        "route_family_distribution": dict(family_dist.most_common(25)),
        "top_route_family": top_family,
        "top_route_family_count": top_family_count,
        "top_route_family_share_pct": round(top_family_count / max(1, total) * 100.0, 4),
        "top_route_family_over_big_run_cap": (top_family_count / max(1, total)) >= 0.60,
        "telemetry_present": total > 0,
    }


def _plan_vs_actual_route_allocation(next_500: dict[str, Any], actual: dict[str, Any]) -> dict[str, Any]:
    planned = {str(route): int(count or 0) for route, count in (next_500.get("route_budget_usage") or {}).items()}
    observed = {str(route): int(count or 0) for route, count in (actual.get("route_distribution") or {}).items()}
    all_routes = sorted(set(planned) | set(observed))
    rows = []
    for route in all_routes:
        rows.append({
            "route_key": route,
            "planned_next_500": planned.get(route, 0),
            "actual_sampled_this_run": observed.get(route, 0),
            "delta_actual_minus_planned": observed.get(route, 0) - planned.get(route, 0),
        })
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "description": "Diagnostic comparison: compares the next 500-variant route budget against the current run's observed sample. Deltas are not failures by themselves.",
        "comparison_type": "next_plan_vs_current_observed",
        "rows": sorted(rows, key=lambda item: abs(int(item.get("delta_actual_minus_planned") or 0)), reverse=True)[:100],
        "actual_telemetry_present": bool(actual.get("telemetry_present")),
    }


def _winner_route_distribution(
    rows: list[dict[str, Any]],
    intended_focus_routes: list[str] | None = None,
    strict_focus_routes: list[str] | None = None,
) -> dict[str, Any]:
    intended = {str(route) for route in (intended_focus_routes or []) if str(route)}
    strict_intended = {str(route) for route in (strict_focus_routes or []) if str(route)}
    off_objective_basis = strict_intended or intended
    counts = Counter(hunt_intel.route_key_from_row(row) for row in rows)
    total = len(rows)
    top_route, top_count = counts.most_common(1)[0] if counts else ("", 0)
    def route_is_intended(route: str, patterns: set[str]) -> bool:
        if not patterns:
            return True
        if route in patterns:
            return True
        route_parts = str(route).split("|")
        for pattern in patterns:
            pattern_parts = str(pattern).split("|")
            if len(pattern_parts) == len(route_parts) and all(p == "*" or p == r or r == "*" for p, r in zip(pattern_parts, route_parts)):
                return True
        return False
    off_objective_count = sum(count for route, count in counts.items() if off_objective_basis and not route_is_intended(route, off_objective_basis))
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "winner_total": total,
        "intended_focus_routes": list(dict.fromkeys(intended_focus_routes or [])),
        "strict_focus_routes": list(dict.fromkeys(strict_focus_routes or [])),
        "off_objective_basis": "strict_cli_focus" if strict_intended else "expanded_focus",
        "route_counts": dict(counts.most_common(50)),
        "top_route": top_route,
        "top_route_count": top_count,
        "top_route_share_pct": round(top_count / max(1, total) * 100.0, 4),
        "off_objective_winner_count": off_objective_count,
        "off_objective_winner_share_pct": round(off_objective_count / max(1, total) * 100.0, 4),
        "off_objective_routes": [route for route, _count in counts.most_common() if off_objective_basis and not route_is_intended(route, off_objective_basis)],
    }


def _plan_vs_winner_route_allocation(next_500: dict[str, Any], winners: dict[str, Any]) -> dict[str, Any]:
    planned = {str(route): int(count or 0) for route, count in (next_500.get("route_budget_usage") or {}).items()}
    winner_counts = {str(route): int(count or 0) for route, count in (winners.get("route_counts") or {}).items()}
    rows = []
    for route in sorted(set(planned) | set(winner_counts)):
        rows.append({
            "route_key": route,
            "planned_next_500": planned.get(route, 0),
            "winner_count_this_run": winner_counts.get(route, 0),
            "winner_share_pct": round(winner_counts.get(route, 0) / max(1, int(winners.get("winner_total") or 0)) * 100.0, 4),
            "planned_but_no_winners": planned.get(route, 0) > 0 and winner_counts.get(route, 0) == 0,
            "winner_without_planned_budget": winner_counts.get(route, 0) > 0 and planned.get(route, 0) == 0,
        })
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "rows": sorted(rows, key=lambda item: (bool(item.get("winner_without_planned_budget")), float(item.get("winner_share_pct") or 0.0)), reverse=True)[:100],
    }


def _sampled_route_concentration_report(actual: dict[str, Any]) -> dict[str, Any]:
    total = int(actual.get("sampled_route_total") or 0)
    top_route_share = float(actual.get("top_route_share_pct") or 0.0)
    top_family_share = float(actual.get("top_route_family_share_pct") or 0.0)
    active = total > 0 and (top_route_share >= 35.0 or top_family_share >= 60.0)
    caution = total > 0 and (top_route_share >= 25.0 or top_family_share >= 45.0)
    cap_routes = []
    for route, count in (actual.get("route_distribution") or {}).items():
        share = int(count or 0) / max(1, total)
        if share >= 0.25:
            cap_routes.append({
                "route_key": route,
                "sampled_count": count,
                "sampled_share_pct": round(share * 100.0, 4),
                "max_next_500_variants": 175 if share >= 0.35 else 225,
            })
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "sampled_total": total,
        "active": active,
        "caution": caution,
        "top_route": actual.get("top_route"),
        "top_route_share_pct": top_route_share,
        "top_route_family": actual.get("top_route_family"),
        "top_route_family_share_pct": top_family_share,
        "cap_routes": cap_routes,
        "recommended_action": (
            "cap_sampled_route_budget_and_force_sibling_or_structural_lanes"
            if active else
            "monitor_sampled_route_budget"
            if caution else
            "sampled_route_budget_healthy"
        ),
    }


def _intended_focus_routes(args: argparse.Namespace, run_dir: Path, repair_queue: dict[str, Any], route_distance_cards: dict[str, Any]) -> list[str]:
    focus = [str(route) for route in (getattr(args, "focus_route", []) or []) if str(route)]
    if not focus:
        previous = _latest_previous_run_summary(run_dir)
        brain = previous.get("hunt_brain_summary") if isinstance(previous.get("hunt_brain_summary"), dict) else {}
        focus.extend(str(route) for route in (brain.get("focus_routes") or []) if str(route))
    focus.extend(str(route) for route in (repair_queue.get("focus_routes") or [])[:2] if str(route))
    closest = route_distance_cards.get("closest_route") if isinstance(route_distance_cards.get("closest_route"), dict) else {}
    if closest.get("route_key"):
        focus.append(str(closest.get("route_key")))
    expanded = list(dict.fromkeys(focus))
    for route in list(expanded):
        for sibling in _sibling_routes(route)[:3]:
            if sibling not in expanded:
                expanded.append(sibling)
    return expanded[:16]


def _repair_regression_detector(
    args: argparse.Namespace,
    run_dir: Path,
    variant_funnel: dict[str, Any],
    repair_progress: dict[str, Any],
    route_concentration: dict[str, Any],
    promotion_ready: dict[str, Any],
) -> dict[str, Any]:
    previous = _latest_previous_run_summary(run_dir)
    previous_funnel = previous.get("variant_funnel") if isinstance(previous.get("variant_funnel"), dict) else {}
    near_delta = repair_progress.get("near_promotion_delta") if isinstance(repair_progress.get("near_promotion_delta"), dict) else {}
    current_yield = float(variant_funnel.get("scored_to_live_stream_yield_pct") or 0.0)
    previous_yield = float(previous_funnel.get("scored_to_live_stream_yield_pct") or 0.0)
    current_top100 = int(variant_funnel.get("top100_count") or 0)
    previous_top100 = int(previous_funnel.get("top100_count") or 0)
    distance_improvement = near_delta.get("improvement_points")
    distance_widened = distance_improvement is not None and float(distance_improvement or 0.0) < 0.0
    yield_dropped = bool(previous_funnel) and current_yield < previous_yield * 0.80
    kept_dropped = bool(previous_funnel) and current_top100 < previous_top100 * 0.80
    no_ready = int(promotion_ready.get("promotion_ready_count") or 0) <= 0
    concentrated = bool(route_concentration.get("active"))
    mode = str(getattr(args, "hunt_mode", "auto") or "auto")
    regression_pressure = no_ready and (yield_dropped or kept_dropped or distance_widened or concentrated)
    soft_active = mode in {"promotion_repair", "repair_exploration", "auto"} and regression_pressure
    active = mode == "promotion_repair" and regression_pressure
    reasons = []
    if yield_dropped:
        reasons.append("live_yield_regressed")
    if kept_dropped:
        reasons.append("kept_count_regressed")
    if distance_widened:
        reasons.append("promotion_distance_widened")
    if concentrated:
        reasons.append("route_concentration_worsened")
    if no_ready:
        reasons.append("no_promotion_ready_candidates")
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "active": active,
        "soft_active": soft_active,
        "hunt_mode": mode,
        "recommended_next_mode": "repair_exploration" if soft_active else mode,
        "recommended_action": "fallback_to_controlled_repair_exploration" if active else "hold_controlled_repair_exploration" if soft_active else "continue_current_repair_posture",
        "reasons": reasons,
        "human_summary": (
            "Repair is not ready to scale: promotion distance/yield/concentration moved the wrong way while no candidate is promotion-ready."
            if soft_active else
            "No repair regression pressure detected."
        ),
        "current": {
            "live_yield_pct": current_yield,
            "top100_count": current_top100,
            "route_concentration_active": concentrated,
            "promotion_ready_count": int(promotion_ready.get("promotion_ready_count") or 0),
            "promotion_distance_improvement_points": distance_improvement,
        },
        "previous": {
            "live_yield_pct": previous_yield if previous_funnel else None,
            "top100_count": previous_top100 if previous_funnel else None,
        },
    }


def _route_admission_gate(
    rows: list[dict[str, Any]],
    intended_focus_routes: list[str],
    route_concentration: dict[str, Any],
    repair_regression: dict[str, Any],
) -> dict[str, Any]:
    intended = {str(route) for route in intended_focus_routes if str(route)}
    counts = Counter(hunt_intel.route_key_from_row(row) for row in rows)
    total = len(rows)
    top_route, top_count = counts.most_common(1)[0] if counts else ("", 0)
    top_share = top_count / max(1, total)
    def route_is_intended(route: str) -> bool:
        if not intended:
            return True
        if route in intended:
            return True
        route_parts = str(route).split("|")
        for pattern in intended:
            pattern_parts = str(pattern).split("|")
            if len(pattern_parts) == len(route_parts) and all(p == "*" or p == r or r == "*" for p, r in zip(pattern_parts, route_parts)):
                return True
        return False
    off_objective_routes = [route for route, _count in counts.most_common() if route and not route_is_intended(route)]
    off_objective_count = sum(int(counts.get(route) or 0) for route in off_objective_routes)
    off_objective_share = off_objective_count / max(1, total)
    off_objective = bool(intended and top_route and not route_is_intended(top_route))
    within_objective_dominant = bool(intended and top_route and route_is_intended(top_route) and top_share >= 0.45)
    regression_pressure = bool(repair_regression.get("active") or repair_regression.get("soft_active"))
    distance_widened = float((repair_regression.get("current") or {}).get("promotion_distance_improvement_points") or 0.0) < 0.0
    aggregate_off_objective = bool(off_objective_routes and off_objective_share >= 0.25)
    needs_admission = (off_objective and top_share >= 0.30) or aggregate_off_objective or within_objective_dominant
    admitted = not needs_admission or (
        top_share < 0.50
        and not regression_pressure
        and not route_concentration.get("active")
    )
    throttle_routes = list(dict.fromkeys((off_objective_routes if aggregate_off_objective else []) + ([top_route] if needs_admission and not admitted and top_route else [])))
    throttle_multiplier = 0.12 if within_objective_dominant and distance_widened else 0.25 if within_objective_dominant else 0.15
    throttle_multipliers = {route: (0.08 if route in off_objective_routes else throttle_multiplier) for route in throttle_routes}
    preserve = list(dict.fromkeys(list(intended_focus_routes) + [route for route in intended_focus_routes for route in _sibling_routes(route)[:2]]))[:12]
    preserve = [route for route in preserve if route not in set(throttle_routes)]
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "top_route": top_route,
        "top_route_count": top_count,
        "top_route_share_pct": round(top_share * 100.0, 4),
        "intended_focus_routes": list(dict.fromkeys(intended_focus_routes))[:12],
        "off_objective_winner": off_objective,
        "off_objective_routes": off_objective_routes[:12],
        "off_objective_winner_count": off_objective_count,
        "off_objective_winner_share_pct": round(off_objective_share * 100.0, 4),
        "aggregate_off_objective_pressure": aggregate_off_objective,
        "within_objective_dominant": within_objective_dominant,
        "promotion_distance_widened": distance_widened,
        "requires_admission_packet": needs_admission,
        "admitted_as_new_center": admitted,
        "throttle_routes": throttle_routes,
        "throttle_multipliers": throttle_multipliers,
        "max_winner_share_pct_by_route": {route: 10.0 if route in off_objective_routes else 20.0 for route in throttle_routes},
        "max_sample_share_pct_by_route": {route: 5.0 if route in off_objective_routes else 8.0 if distance_widened else 12.0 for route in throttle_routes},
        "preserved_focus_routes": preserve,
        "avoid_as_center_routes": throttle_routes,
        "human_summary": (
            "Off-objective winners are too common in aggregate; suppress them until the intended repair family produces promotion-ready candidates."
            if aggregate_off_objective else
            f"{top_route} is an off-objective dominant route and must be throttled until it proves breadth/robustness."
            if throttle_routes and off_objective else
            f"{top_route} is on-objective but over-dominant; cap it until it improves promotion blockers and promotion distance."
            if throttle_routes and within_objective_dominant else
            "Top route is within the intended objective or not dominant enough to require admission."
        ),
    }


def _stale_focus_audit(
    cycles: list[dict[str, Any]],
    repair_queue: dict[str, Any],
    route_distance_cards: dict[str, Any],
    next_command_recipe: dict[str, Any] | None = None,
    route_admission: dict[str, Any] | None = None,
) -> dict[str, Any]:
    latest_focus = list(dict.fromkeys(list(repair_queue.get("focus_routes") or [])))[:8]
    closest = route_distance_cards.get("closest_route") if isinstance(route_distance_cards.get("closest_route"), dict) else {}
    closest_route = str(closest.get("route_key") or "")
    if closest_route and closest_route not in latest_focus:
        latest_focus.insert(0, closest_route)
    used = []
    for cycle in cycles:
        cmd = list(cycle.get("cmd") or [])
        for idx, token in enumerate(cmd):
            if token == "--focus-route" and idx + 1 < len(cmd):
                used.append(str(cmd[idx + 1]))
    next_focus = list((next_command_recipe or {}).get("focus_routes") or [])
    if not next_focus:
        cmd = list((next_command_recipe or {}).get("command") or [])
        for idx, token in enumerate(cmd):
            if token == "--focus-route" and idx + 1 < len(cmd):
                next_focus.append(str(cmd[idx + 1]))
    intentionally_not_center = {
        str(route)
        for route in ((route_admission or {}).get("avoid_as_center_routes") or (route_admission or {}).get("throttle_routes") or [])
        if str(route)
    }
    required_latest_focus = [route for route in latest_focus if route not in intentionally_not_center]
    current_missing = [route for route in required_latest_focus if route and route not in used]
    next_missing = [route for route in required_latest_focus if route and route not in next_focus]
    requires_update = bool(next_missing) if next_command_recipe else bool(current_missing)
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "latest_focus_routes": latest_focus,
        "required_latest_focus_routes": required_latest_focus,
        "intentionally_not_center_routes": list(dict.fromkeys(intentionally_not_center)),
        "cycle_focus_routes_used": list(dict.fromkeys(used)),
        "next_command_focus_routes": list(dict.fromkeys(next_focus)),
        "missing_latest_focus_in_cycle_command": current_missing,
        "missing_latest_focus_in_next_command": next_missing,
        "requires_next_run_update": requires_update,
        "status": "next_command_ready" if not requires_update else "stale_or_not_yet_applied",
    }


def _big_run_preflight_gate(
    args: argparse.Namespace,
    run_dir: Path,
    variant_funnel: dict[str, Any],
    next_500: dict[str, Any],
    actual: dict[str, Any],
    promotion_ready: dict[str, Any],
    route_warning: dict[str, Any],
    stale_focus: dict[str, Any],
    failure_summary: dict[str, Any] | None = None,
    repair_regression: dict[str, Any] | None = None,
    route_admission: dict[str, Any] | None = None,
    anti_alias: dict[str, Any] | None = None,
    sampled_concentration: dict[str, Any] | None = None,
    repair_progress: dict[str, Any] | None = None,
    blocker_digest: dict[str, Any] | None = None,
    winner_distribution: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cycle_log_size = (run_dir / "cycle_log.json").stat().st_size if (run_dir / "cycle_log.json").exists() else 0
    failure_summary = failure_summary or {}
    anti_alias = anti_alias or {}
    sampled_concentration = sampled_concentration or {}
    repair_progress = repair_progress or {}
    blocker_digest = blocker_digest or {}
    winner_distribution = winner_distribution or {}
    promotion_ready_count = int(promotion_ready.get("promotion_ready_count") or 0)
    suspicious_count = int(failure_summary.get("suspicious_winner_count") or 0)
    top100_count = int(variant_funnel.get("top100_count") or variant_funnel.get("behavior_unique_rows") or 0)
    scored_total = int(variant_funnel.get("scored_total") or variant_funnel.get("requested_variants") or 0)
    behavior_yield = float(variant_funnel.get("scored_to_behavior_unique_yield_pct") or 0.0) / 100.0
    estimated_big_run_behavior_unique = int(round(max(top100_count, behavior_yield * 500.0)))
    behavior_target = int(anti_alias.get("minimum_big_run_behavior_unique_target") or 20)
    diagnostic_pool_projection_ok = scored_total < 500 and estimated_big_run_behavior_unique >= behavior_target
    collapse_rate = float(anti_alias.get("collapse_rate") or 0.0)
    near_delta = repair_progress.get("near_promotion_delta") if isinstance(repair_progress.get("near_promotion_delta"), dict) else {}
    promotion_distance_improved_or_unknown = near_delta.get("improvement_points") is None or float(near_delta.get("improvement_points") or 0.0) >= 0.0
    universal_blockers = list(blocker_digest.get("universal_blockers") or [])
    winner_route_count = len((winner_distribution.get("route_counts") or {}))
    checks = {
        "exact_variant_count_enabled": bool(args.exact_variant_count),
        "count_contract_ok": bool(variant_funnel.get("count_contract_ok", True)),
        "top100_collection_ok": int(variant_funnel.get("top100_count") or 0) > 0,
        "behavior_unique_pool_big_run_minimum": top100_count >= behavior_target or diagnostic_pool_projection_ok,
        "alias_collapse_under_big_run_cap": collapse_rate < 0.50,
        "anti_alias_pressure_not_low": str(anti_alias.get("pressure") or "low") == "low",
        "route_caps_respected": not any(not bool(row.get("cap_respected")) for row in next_500.get("route_cap_compliance") or []),
        "plan_totals_500": int(next_500.get("allocation_total") or 0) == 500,
        "compact_cycle_log_ok": cycle_log_size < 100_000,
        "status_policy_present": (run_dir / "status_policy.json").exists(),
        "actual_route_telemetry_present": bool(actual.get("telemetry_present")),
        "actual_route_telemetry_human_keyed": bool(actual.get("human_route_key_coverage_ok")),
        "actual_route_telemetry_sample_coverage_ok": bool(actual.get("sample_coverage_ok")),
        "sampled_route_budget_under_big_run_cap": not bool(sampled_concentration.get("active")),
        "top_sampled_route_below_35pct": float(actual.get("top_route_share_pct") or 0.0) < 35.0,
        "top_sampled_family_below_60pct": float(actual.get("top_route_family_share_pct") or 0.0) < 60.0,
        "stale_focus_update_needed": not bool(stale_focus.get("requires_next_run_update")),
        "route_concentration_below_scale_threshold": not bool(route_warning.get("active")),
        "winner_route_pool_broad_enough": winner_route_count >= 4 or top100_count < 5,
        "missing_promotion_ready_candidates": promotion_ready_count > 0,
        "promotion_distance_widened": promotion_distance_improved_or_unknown,
        "hard_blockers_are_universal": not bool(universal_blockers),
        "no_suspicious_winners": suspicious_count == 0,
        "repair_regression_present": not bool((repair_regression or {}).get("active") or (repair_regression or {}).get("soft_active")),
        "unadmitted_dominant_route_present": not bool((route_admission or {}).get("requires_admission_packet") and not (route_admission or {}).get("admitted_as_new_center")),
        "route_throttles_still_needed": not bool((route_admission or {}).get("throttle_routes")),
        "off_objective_winner_share_under_cap": float((winner_distribution or {}).get("off_objective_winner_share_pct") or 0.0) < 25.0,
    }
    blockers = [name for name, ok in checks.items() if not ok]
    check_glossary = {
        "anti_alias_pressure_not_low": "Passes when anti-alias pressure is low.",
        "missing_promotion_ready_candidates": "Passes when at least one promotion-ready candidate exists.",
        "promotion_distance_widened": "Passes when closest promotion distance is flat or improving.",
        "repair_regression_present": "Passes when repair regression detector is inactive.",
        "unadmitted_dominant_route_present": "Passes when no unadmitted dominant route is trying to become the center.",
        "route_throttles_still_needed": "Passes when no route throttle is still required before scaling.",
        "actual_route_telemetry_human_keyed": "Passes only when every sampled route has a human-readable route key.",
    }
    blocker_reasons = [
        {
            "check": name,
            "plain_english": check_glossary.get(name, f"{name} did not pass."),
        }
        for name in blockers
    ]
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "checks": checks,
        "check_glossary": check_glossary,
        "blocker_reasons": blocker_reasons,
        "blockers": blockers,
        "promotion_ready_count": promotion_ready_count,
        "suspicious_winner_count": suspicious_count,
        "repair_regression_detector": repair_regression or {},
        "route_admission_gate": route_admission or {},
        "anti_alias_pressure": anti_alias,
        "sampled_route_concentration": sampled_concentration,
        "repair_progress_report": repair_progress,
        "promotion_blocker_digest": blocker_digest,
        "winner_route_distribution": winner_distribution,
        "estimated_big_run_behavior_unique_rows": estimated_big_run_behavior_unique,
        "diagnostic_pool_projection_ok": diagnostic_pool_projection_ok,
        "decision": "ready_for_big_run" if not blockers else "run_targeted_controlled_repair_first",
        "human_summary": "Big run is cleared." if not blockers else "Do a targeted controlled repair run before scaling: " + ", ".join(blockers[:6]) + ".",
    }


def _run_size_recommendation(variant_funnel: dict[str, Any], preflight: dict[str, Any]) -> dict[str, Any]:
    live_yield = float(variant_funnel.get("scored_to_live_stream_yield_pct") or 0.0)
    top100 = int(variant_funnel.get("top100_count") or 0)
    blockers = list(preflight.get("blockers") or [])
    if blockers:
        size = "targeted_500"
        why = "preflight still has blockers, so keep the next run controlled and diagnostic"
    elif live_yield >= 2.0 and top100 >= 10:
        size = "controlled_1000_to_2000"
        why = "live yield and kept count are strong enough for a larger controlled sample"
    else:
        size = "targeted_500_to_1000"
        why = "signals exist but need a little more controlled evidence before a huge run"
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "recommended_run_size": size,
        "why": why,
        "live_yield_pct": live_yield,
        "top100_count": top100,
    }


def _before_big_run_issue_register(
    preflight: dict[str, Any],
    failure_summary: dict[str, Any],
    blocker_digest: dict[str, Any],
    anti_alias: dict[str, Any],
    sampled_concentration: dict[str, Any],
    repair_progress: dict[str, Any],
) -> dict[str, Any]:
    checks = preflight.get("checks") if isinstance(preflight.get("checks"), dict) else {}
    issues = []
    for check, ok in checks.items():
        if ok:
            continue
        if check.startswith("promotion") or check in {"missing_promotion_ready_candidates", "hard_blockers_are_universal", "repair_regression_present"}:
            category = "promotion_gate"
        elif "alias" in check or "behavior" in check:
            category = "behavior_diversity"
        elif "route" in check or "focus" in check:
            category = "route_budget"
        elif "telemetry" in check or "contract" in check or "cycle" in check:
            category = "runtime_contract"
        else:
            category = "scale_readiness"
        issues.append({
            "issue": check,
            "category": category,
            "severity": "blocker",
            "repair_control": {
                "promotion_gate": "promotion_gate_repair_layer",
                "behavior_diversity": "anti_alias_structural_mutation",
                "route_budget": "route_budget_caps_and_admission_gate",
                "runtime_contract": "runtime_preflight_contract",
                "scale_readiness": "controlled_500_before_big_run",
            }.get(category, "controlled_500_before_big_run"),
        })
    for item in blocker_digest.get("reason_counts") or []:
        issues.append({
            "issue": f"candidate_blocker:{item.get('reason')}",
            "category": "promotion_gate",
            "severity": "blocker" if int(item.get("count") or 0) else "watch",
            "candidate_count": item.get("count"),
            "repair_control": item.get("repair_focus"),
            "plain_english": item.get("plain_english"),
        })
    near_delta = repair_progress.get("near_promotion_delta") if isinstance(repair_progress.get("near_promotion_delta"), dict) else {}
    promotion_ready_count = int(preflight.get("promotion_ready_count") or 0)
    estimated_big_run_behavior = int(preflight.get("estimated_big_run_behavior_unique_rows") or 0)
    no_big_run_until = []
    satisfied_conditions = []
    conditions = [
        ("promotion_ready_count > 0", promotion_ready_count > 0),
        ("behavior_unique_top100_count >= 20 or projected >= 20", estimated_big_run_behavior >= 20),
        ("anti_alias pressure is low and collapse_rate < 50%", str(anti_alias.get("pressure") or "low") == "low" and float(anti_alias.get("collapse_rate") or 0.0) < 0.50),
        ("top sampled route < 35% and top sampled route family < 60%", not bool(sampled_concentration.get("active"))),
        ("closest promotion distance is flat or improving", near_delta.get("improvement_points") is None or float(near_delta.get("improvement_points") or 0.0) >= 0.0),
        ("no universal hard blocker across kept live beaters", not bool(blocker_digest.get("universal_blockers") or [])),
    ]
    for label, ok in conditions:
        (satisfied_conditions if ok else no_big_run_until).append(label)
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "decision": preflight.get("decision"),
        "issue_count": len(issues),
        "blocker_count": sum(1 for item in issues if item.get("severity") == "blocker"),
        "issues": issues[:200],
        "no_big_run_until": no_big_run_until,
        "satisfied_conditions": satisfied_conditions,
        "current_readouts": {
            "top_failure_summary": failure_summary.get("top_failure_reasons") or [],
            "universal_blockers": blocker_digest.get("universal_blockers") or [],
            "anti_alias_pressure": anti_alias.get("pressure"),
            "collapse_rate": anti_alias.get("collapse_rate"),
            "sampled_route_concentration_active": sampled_concentration.get("active"),
            "near_promotion_improvement_points": near_delta.get("improvement_points"),
        },
    }


def _big_run_launch_controls(
    preflight: dict[str, Any],
    runtime_handoff: dict[str, Any],
    next_command_recipe: dict[str, Any],
    top_failure_summary: dict[str, Any],
    repair_allocation: dict[str, Any],
    route_budget_caps: dict[str, Any],
    promotion_ready: dict[str, Any],
    winner_distribution: dict[str, Any],
    sampled_concentration: dict[str, Any],
    repair_lane_performance: dict[str, Any],
    readiness_components: dict[str, Any],
) -> dict[str, Any]:
    """Concrete controls that convert the issue register into a big-run launch posture."""
    blockers = set(str(item) for item in (preflight.get("blockers") or []))
    hard_blockers = sorted(blockers - {"behavior_unique_pool_big_run_minimum"})
    runtime_adapter = runtime_handoff.get("runtime_command_adapter") if isinstance(runtime_handoff.get("runtime_command_adapter"), dict) else {}
    avoid_routes = {str(route) for route in (runtime_adapter.get("throttle_routes") or []) if str(route)}
    route_caps = route_budget_caps.get("route_caps") if isinstance(route_budget_caps.get("route_caps"), list) else []
    max_route_caps = {
        str(row.get("route_key")): min(int(row.get("max_variants_per_500") or 90), 110)
        for row in route_caps
        if isinstance(row, dict) and row.get("route_key")
    }
    off_objective_routes = {
        str(route)
        for route in ((winner_distribution or {}).get("off_objective_routes") or [])
        if str(route)
    }
    protected = [route for route in list(dict.fromkeys(
        [str(route) for route in (runtime_handoff.get("focus_routes") or []) if str(route)]
        + [str(route) for route in ((runtime_adapter.get("promotion_ready_repair_envelope") or {}).get("allowed_routes") or []) if str(route)]
    )) if route not in avoid_routes and route not in off_objective_routes]
    promotion_ready_count = int(promotion_ready.get("promotion_ready_count") or 0)
    ready_with_controls = bool(
        promotion_ready_count > 0
        and not hard_blockers
        and not bool(sampled_concentration.get("active"))
        and float((winner_distribution or {}).get("off_objective_winner_share_pct") or 0.0) < 25.0
        and int(preflight.get("estimated_big_run_behavior_unique_rows") or 0) >= 20
    )
    launch_mode = "promotion_repair"
    controls = [
        "use_runtime_bootstrap_json",
        "promotion_survival_sort_evidence_tier",
        "hard_quality_protection",
        "only_keep_live_beaters",
        "exact_variant_count",
        "cap_top_sampled_routes",
        "preserve_promotion_ready_candidates",
        "protect_raw_edge_without_promoting_it",
        "prioritize_robustness_floor",
        "prioritize_holdout_grade",
        "prioritize_readiness_finish",
        "suppress_low_value_route_breadth_if_no_breadth_gap",
        "force_behavior_unique_lift",
        "monitor_route_concentration",
        "stop_if_promotion_distance_widens_again",
    ]
    command = list(next_command_recipe.get("command") or [])
    command_string = _shell_command_string(command)
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "decision": "launch_big_run_with_controls" if ready_with_controls else "run_targeted_500_first",
        "ready_with_controls": ready_with_controls,
        "hard_blockers_remaining": hard_blockers,
        "diagnostic_blockers_absorbed_by_projection": sorted(blockers & {"behavior_unique_pool_big_run_minimum"}),
        "promotion_ready_count": promotion_ready_count,
        "estimated_big_run_behavior_unique_rows": preflight.get("estimated_big_run_behavior_unique_rows"),
        "launch_mode": launch_mode,
        "required_controls": controls,
        "runtime_bootstrap_json": next_command_recipe.get("runtime_bootstrap_json"),
        "recommended_command": command,
        "recommended_command_string": command_string,
        "protected_focus_routes": protected[:16],
        "throttled_routes": sorted(avoid_routes),
        "suppressed_off_objective_routes": sorted(avoid_routes & off_objective_routes),
        "suppressed_dominant_on_objective_routes": sorted(avoid_routes - off_objective_routes),
        "max_route_caps_per_500": max_route_caps,
        "target_failure_mix": (runtime_adapter.get("quality_protection_controls") or {}).get("target_failures") or [],
        "repair_allocation_by_evidence_weakness": repair_allocation,
        "top_failure_summary": top_failure_summary.get("top_failure_reasons") or [],
        "readiness_top_missing_components": readiness_components.get("top_missing_components") or [],
        "repair_lane_performance": repair_lane_performance.get("lanes") or [],
        "winner_route_distribution": winner_distribution,
        "sampled_route_concentration": sampled_concentration,
        "human_summary": (
            "Big run can launch only with the runtime bootstrap, tightened caps, and promotion_repair objective."
            if ready_with_controls else
            "Run one targeted 500 promotion-repair pass before the big run."
        ),
    }


def _big_run_truth_summary(
    variant_funnel: dict[str, Any],
    promotion_ready: dict[str, Any],
    preflight: dict[str, Any],
    launch_controls: dict[str, Any],
    severity: dict[str, Any],
    run_size: dict[str, Any],
) -> dict[str, Any]:
    """Single human-facing source of truth for scale readiness."""
    promotion_ready_count = int(promotion_ready.get("promotion_ready_count") or 0)
    top100_count = int(variant_funnel.get("top100_count") or variant_funnel.get("behavior_unique_rows") or 0)
    blockers = list(preflight.get("blockers") or [])
    raw_launch_decision = str(launch_controls.get("decision") or preflight.get("decision") or "")
    recommended_size = str(run_size.get("recommended_run_size") or "")
    controlled_first = recommended_size.startswith("targeted_")
    launch_decision = "run_targeted_500_first" if controlled_first else raw_launch_decision
    if launch_decision == "launch_big_run_with_controls":
        verdict = "ready_for_big_run_with_controls"
    elif promotion_ready_count > 0:
        verdict = "targeted_repair_before_big_run"
    else:
        verdict = "not_ready_for_big_run"
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "verdict": verdict,
        "launch_decision": launch_decision,
        "raw_launch_decision": raw_launch_decision,
        "recommended_run_size": recommended_size,
        "top100_behavior_unique_count": top100_count,
        "promotion_ready_count": promotion_ready_count,
        "preflight_blockers": blockers,
        "severity": severity.get("severity"),
        "truth_table": {
            "has_live_beating_pool": top100_count > 0,
            "has_promotion_ready_candidate": promotion_ready_count > 0,
            "preflight_clear": not blockers,
            "launch_ready_with_controls": bool(launch_controls.get("ready_with_controls")) and not controlled_first,
            "controlled_run_recommended_first": controlled_first,
        },
        "human_summary": (
            "Big run can launch with controls."
            if verdict == "ready_for_big_run_with_controls" else
            "A promotion-ready candidate exists, but the machine recommends one targeted controlled repair run before the big run."
            if promotion_ready_count > 0 else
            "No promotion-ready candidate exists yet; repair before scaling."
        ),
    }


def _final_summary_control_payloads(run_dir: Path) -> dict[str, Any]:
    return {
        "big_run_launch_controls": hunt_intel.read_json(run_dir / "big_run_launch_controls.json", {}) or {},
        "big_run_truth_summary": hunt_intel.read_json(run_dir / "big_run_truth_summary.json", {}) or {},
    }


def _runtime_handoff_controls(
    route_admission: dict[str, Any],
    anti_alias: dict[str, Any],
    behavior_controls: dict[str, Any],
    quality_controls: dict[str, Any],
    repair_now: dict[str, Any],
    route_budget_caps: dict[str, Any],
    repair_allocation: dict[str, Any],
    promotion_ready: dict[str, Any] | None = None,
    winner_distribution: dict[str, Any] | None = None,
    blocker_digest: dict[str, Any] | None = None,
    promotion_survival_reference: dict[str, Any] | None = None,
) -> dict[str, Any]:
    repair_envelope = _promotion_ready_repair_envelope(
        winner_distribution or {},
        quality_controls or {},
        blocker_digest or {},
        promotion_ready,
        route_admission,
        promotion_survival_reference,
    )
    focus_routes = list(dict.fromkeys(
        [str(route) for route in (route_admission.get("preserved_focus_routes") or []) if str(route)]
        + [str(route) for route in (behavior_controls.get("diversity_focus_routes") or []) if str(route)]
        + [str(route) for route in (quality_controls.get("protected_routes") or []) if str(route)]
        + [str(route) for route in (repair_now.get("recommended_focus_routes") or []) if str(route)]
        + [str(route) for route in (repair_envelope.get("allowed_routes") or []) if str(route)]
    ))
    structural_routes = list(dict.fromkeys(
        [str(route) for route in (behavior_controls.get("structural_mutation_routes") or []) if str(route)]
        + [str(route) for route in (anti_alias.get("structural_mutation_routes") or []) if str(route)]
    ))
    throttle_routes = list(dict.fromkeys(
        [str(route) for route in (route_admission.get("throttle_routes") or []) if str(route)]
        + [str(route) for route in (quality_controls.get("throttle_routes") or []) if str(route)]
    ))
    throttle_multipliers = route_admission.get("throttle_multipliers") if isinstance(route_admission.get("throttle_multipliers"), dict) else {}
    if quality_controls.get("active"):
        for route in throttle_routes:
            throttle_multipliers[route] = min(float(throttle_multipliers.get(route, 1.0) or 1.0), 0.18 if quality_controls.get("promotion_distance_widened") else 0.35)
    sample_caps = dict(behavior_controls.get("max_sample_share_pct_by_route") or {})
    sample_caps.update(quality_controls.get("max_sample_share_pct_by_route") or {})
    sample_caps.update(repair_envelope.get("max_sample_share_pct_by_route") or {})
    sample_caps.update(route_admission.get("max_sample_share_pct_by_route") or {})
    if str(anti_alias.get("pressure") or "low") == "medium":
        for route in list(sample_caps):
            sample_caps[route] = min(float(sample_caps.get(route) or 100.0), 18.0)
        if not sample_caps:
            sample_caps.update({route: 18.0 for route in focus_routes[:12]})
    winner_caps = dict(behavior_controls.get("max_winner_share_pct_by_route") or {})
    winner_caps.update(quality_controls.get("max_winner_share_pct_by_route") or {})
    winner_caps.update(repair_envelope.get("max_winner_share_pct_by_route") or {})
    winner_caps.update(route_admission.get("max_winner_share_pct_by_route") or {})
    if str(anti_alias.get("pressure") or "low") == "medium":
        for route in list(winner_caps):
            winner_caps[route] = min(float(winner_caps.get(route) or 100.0), 18.0)
    target_failures = list(quality_controls.get("target_failures") or [])
    if quality_controls.get("active") and not target_failures:
        target_failures = ["promotion_readiness_below_floor", "robustness_below_floor"]
    route_breadth_required = "route_breadth" in set(target_failures)
    primary_lane = (
        "promotion_readiness_repair" if "promotion_readiness_below_floor" in set(target_failures)
        else "robustness_floor_repair" if "robustness_below_floor" in set(target_failures)
        else "holdout_grade_repair" if "holdout_positive_but_not_promotion_grade" in set(target_failures)
        else "runtime_repair"
    )
    secondary_lanes = []
    if "robustness_below_floor" in set(target_failures) and primary_lane != "robustness_floor_repair":
        secondary_lanes.append("robustness_floor_repair")
    if {"holdout_positive_but_not_promotion_grade", "missing_holdout_credit", "promotion_grade_holdout_credit"} & set(target_failures):
        secondary_lanes.append("holdout_grade_repair")
    commands = [
        {
            "route_key": route,
            "action": "repair",
            "mutation_width": "tight" if quality_controls.get("active") else "medium",
            "source": "runtime_handoff_quality_protection",
            "target_failures": target_failures,
            "primary_repair_lane": primary_lane,
            "secondary_repair_lanes": list(dict.fromkeys(secondary_lanes)),
            "quality_protection": bool(quality_controls.get("active")),
            "route_breadth_required": route_breadth_required,
            "prefer_promotion_readiness": "promotion_readiness_below_floor" in set(target_failures),
            "preserve_holdout": bool({"robustness_below_floor", "holdout_positive_but_not_promotion_grade"} & set(target_failures)),
        }
        for route in focus_routes[:12]
    ]
    if repair_envelope.get("active"):
        commands = list(repair_envelope.get("commands") or []) + [
            command for command in commands
            if str(command.get("route_key") or "") not in set(repair_envelope.get("allowed_routes") or [])
        ]
    adapter = {
        "source": "runtime_handoff_controls",
        "focus_routes": focus_routes[:24],
        "structural_mutation_routes": structural_routes[:24],
        "throttle_routes": throttle_routes[:24],
        "throttle_multipliers": throttle_multipliers,
        "commands": commands[:20],
        "max_winner_share_pct_by_route": winner_caps or {route: 25.0 for route in throttle_routes},
        "max_sample_share_pct_by_route": sample_caps,
        "default_max_sample_share_pct": behavior_controls.get("default_max_sample_share_pct"),
        "anti_alias_pressure": anti_alias.get("pressure"),
        "clone_prevention_mode": behavior_controls.get("clone_prevention_mode"),
        "route_breadth_required": route_breadth_required,
        "prefer_promotion_readiness": "promotion_readiness_below_floor" in set(target_failures),
        "dominant_route_not_admitted": bool(route_admission.get("requires_admission_packet") and not route_admission.get("admitted_as_new_center")),
        "quality_protection_mode": quality_controls.get("quality_protection_mode"),
        "quality_protection_controls": dict(quality_controls, target_failures=target_failures) if quality_controls.get("active") else quality_controls,
        "target_behavior_unique_count": behavior_controls.get("target_behavior_unique_count"),
        "required_winner_route_count": behavior_controls.get("required_winner_route_count"),
        "route_budget_caps": route_budget_caps or {},
        "repair_allocation_by_evidence_weakness": repair_allocation or {},
        "promotion_ready_repair_envelope": repair_envelope,
        "edge_preservation_routes": repair_envelope.get("edge_preservation_routes") or [],
        "close_sibling_routes": repair_envelope.get("close_sibling_routes") or [],
        "generic_route_deprioritize_patterns": repair_envelope.get("generic_route_deprioritize_patterns") or [],
        "outside_repair_envelope_multiplier": repair_envelope.get("outside_envelope_sample_weight_multiplier"),
    }
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "description": "Controls to bootstrap the next controlled repair run with this run's learned diversity, throttle, and repair objectives.",
        "focus_routes": focus_routes[:24],
        "structural_mutation_routes": structural_routes[:24],
        "throttle_routes": throttle_routes[:24],
        "throttle_multipliers": throttle_multipliers,
        "route_budget_caps": route_budget_caps or {},
        "repair_allocation_by_evidence_weakness": repair_allocation or {},
        "promotion_ready_repair_envelope": repair_envelope,
        "behavior_unique_generation_controls": behavior_controls,
        "promotion_quality_protection_controls": quality_controls,
        "runtime_command_adapter": adapter,
    }


def _repair_route_budget_caps(repair_queue: dict[str, Any], anti_alias: dict[str, Any], *, variant_budget: int = 500) -> dict[str, Any]:
    route_counts = Counter()
    for task in repair_queue.get("tasks") or []:
        route = str(task.get("route_key") or "")
        if route:
            route_counts[route] += max(1, int(task.get("candidate_count") or 1))
    total = sum(route_counts.values())
    pressure = str(anti_alias.get("pressure") or "low")
    route_count = len(route_counts)
    target_behavior = int(anti_alias.get("minimum_big_run_behavior_unique_target") or 20)
    behavior_count = int(anti_alias.get("behavior_unique_rows") or 0)
    needs_big_pool_lift = behavior_count < target_behavior
    max_share = 0.18 if pressure == "high" else 0.22 if (pressure == "medium" or needs_big_pool_lift) else 0.30
    if route_count <= 2 and total >= 2:
        max_share = min(max_share, 0.28)
    elif route_count <= 3 and total >= 4:
        max_share = min(max_share, 0.25)
    caps = []
    for route, count in route_counts.most_common():
        share = count / max(1, total)
        caps.append({
            "route_key": route,
            "repair_candidate_count": count,
            "current_repair_share_pct": round(share * 100.0, 4),
            "max_budget_share_pct": round(max_share * 100.0, 4),
            "max_variants_per_500": int(round(variant_budget * max_share)),
            "cap_required": share > max_share,
        })
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "description": "Budget caps that prevent the next repair cycle from over-feeding one crowded route family.",
        "anti_alias_pressure": pressure,
        "behavior_unique_count": behavior_count,
        "target_behavior_unique_count": target_behavior,
        "big_run_behavior_pool_lift_required": needs_big_pool_lift,
        "overconcentrated": any(row.get("cap_required") for row in caps),
        "route_count": route_count,
        "min_distinct_repair_routes": min(max(4 if total >= 3 else 3, route_count), 8) if route_counts else 0,
        "tiny_route_pool_pressure": route_count < (min(max(4 if total >= 3 else 3, route_count), 8) if route_counts else 0),
        "route_caps": caps[:25],
    }


def _distance_adjusted_route_budget_caps(route_budget_caps: dict[str, Any], route_distance_cards: dict[str, Any]) -> dict[str, Any]:
    caps = dict(route_budget_caps or {})
    closest = route_distance_cards.get("closest_route") if isinstance(route_distance_cards.get("closest_route"), dict) else {}
    closest_route = str(closest.get("route_key") or "")
    adjusted = []
    for row in caps.get("route_caps") or []:
        item = dict(row)
        if closest_route and str(item.get("route_key") or "") == closest_route:
            item["promotion_distance_priority_override"] = True
            item["cap_required"] = (float(item.get("current_repair_share_pct") or 0.0) > float(item.get("max_budget_share_pct") or 0.0))
        adjusted.append(item)
    caps["route_caps"] = adjusted
    caps["promotion_distance_priority_route"] = closest_route
    return caps


def _repair_cycle_now_decision(
    stop_go_gate: dict[str, Any],
    auto_rec: dict[str, Any],
    repair_queue: dict[str, Any],
    quality_floor: dict[str, Any],
    anti_alias: dict[str, Any],
    route_budget_caps: dict[str, Any] | None = None,
    route_admission: dict[str, Any] | None = None,
    repair_regression: dict[str, Any] | None = None,
) -> dict[str, Any]:
    should_repair = (
        stop_go_gate.get("decision") == "repair_first"
        or auto_rec.get("recommended_action") in {"repair_first", "loosen_repair"}
        or (int(quality_floor.get("kept_count") or 0) == 0 and int(repair_queue.get("task_count") or 0) > 0)
    )
    mode = str(auto_rec.get("next_mode") or ("promotion_repair" if should_repair else "discovery"))
    regression_pressure = bool((repair_regression or {}).get("active") or (repair_regression or {}).get("soft_active"))
    if regression_pressure:
        mode = "repair_exploration"
    if auto_rec.get("recommended_action") == "loosen_repair":
        why = str(auto_rec.get("why") or "Repair aperture is too narrow; continue repair_exploration.")
    elif regression_pressure:
        why = "Repair pressure regressed yield, concentration, or promotion distance; hold controlled repair_exploration while preserving the original objective."
    elif should_repair:
        if mode == "promotion_repair" and float(auto_rec.get("repair_yield_pct") or 0.0) >= float(auto_rec.get("repair_yield_floor_pct") or 1.0):
            why = "Repair exploration found enough live yield; tighten into promotion_repair to attack readiness and breadth blockers."
        else:
            why = "No promotion-floor candidate survived and repair tasks exist."
    else:
        why = "Promotion repair is not required before the next discovery run."
    admission = route_admission or {}
    focus_routes = list(dict.fromkeys(
        [str(route) for route in (admission.get("preserved_focus_routes") or []) if str(route)]
        + [str(route) for route in (repair_queue.get("focus_routes") or []) if str(route)]
    ))
    avoid_as_center = {str(route) for route in (admission.get("avoid_as_center_routes") or []) if str(route)}
    focus_routes = [route for route in focus_routes if route not in avoid_as_center]
    for route in list(focus_routes):
        for sibling in _sibling_routes(route):
            if sibling not in focus_routes:
                focus_routes.append(sibling)
            if len(focus_routes) >= 8:
                break
        if len(focus_routes) >= 8:
            break
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "run_repair_cycle_now": should_repair,
        "recommended_hunt_mode": mode,
        "recommended_focus_routes": focus_routes[:8],
        "route_admission_gate": admission,
        "repair_regression_detector": repair_regression or {},
        "anti_alias_pressure": anti_alias.get("pressure"),
        "route_budget_caps": route_budget_caps or {},
        "command_posture": {
            "hunt_mode": mode,
            "live_only": True,
            "exact_variant_count": True,
            "include_structural_mutation": anti_alias.get("pressure") in {"medium", "high"},
            "enforce_route_budget_caps": bool((route_budget_caps or {}).get("overconcentrated")),
            "preserve_original_objective": bool(admission.get("preserved_focus_routes")),
            "throttle_unadmitted_routes": list(admission.get("throttle_routes") or []),
        },
        "why": why,
        "why_this_mode": why,
    }


def _auto_hunt_recommendation(
    rows: list[dict[str, Any]],
    quality_floor: dict[str, Any],
    repair_queue: dict[str, Any],
    route_crowding: dict[str, Any],
    variant_funnel: dict[str, Any] | None = None,
    args: argparse.Namespace | None = None,
    repair_regression: dict[str, Any] | None = None,
    route_concentration: dict[str, Any] | None = None,
    anti_alias: dict[str, Any] | None = None,
    sampled_concentration: dict[str, Any] | None = None,
) -> dict[str, Any]:
    kept = int(quality_floor.get("kept_count") or 0)
    repair_tasks = int(repair_queue.get("task_count") or 0)
    crowded = len(route_crowding.get("crowded_routes") or [])
    mode = str(getattr(args, "hunt_mode", "auto") or "auto") if args else "auto"
    live_yield = float((variant_funnel or {}).get("scored_to_live_stream_yield_pct") or 0.0)
    repair_floor = float(getattr(args, "repair_yield_floor_pct", 1.0) or 1.0) if args else 1.0
    alias_high = str((anti_alias or {}).get("pressure") or "low") == "high"
    sampled_overconcentrated = bool((sampled_concentration or {}).get("active"))
    regression_pressure = bool((repair_regression or {}).get("active") or (repair_regression or {}).get("soft_active"))
    if regression_pressure:
        action = "loosen_repair"
        why = "repair pressure regressed; hold controlled repair_exploration, preserve original focus routes, and attack the failing promotion gates."
    elif alias_high:
        action = "repair_first"
        why = "anti-alias pressure is high; force structural mutation and behavior-unique yield before scaling."
    elif sampled_overconcentrated:
        action = "repair_first"
        why = "sampled route budget is overconcentrated; cap dominant routes and broaden before scaling."
    elif kept >= 10 and repair_tasks <= kept:
        action = "run_bigger"
        why = "promotion-floor candidates exist; larger sample can separate durable winners from luck."
    elif mode in {"promotion_repair", "repair_exploration"} and live_yield > 0.0 and live_yield < repair_floor and kept == 0:
        action = "loosen_repair"
        why = f"repair mode live-yield {round(live_yield, 4)}% is below floor {repair_floor}%; widen repair while preserving evidence goals."
    elif repair_tasks > 0 and kept == 0:
        action = "repair_first"
        why = "no promotion-floor candidates survived and repair tasks exist."
    elif repair_tasks >= max(3, kept):
        action = "repair_first"
        why = "too many live beaters are blocked by holdout/readiness/route breadth gaps."
    elif not rows:
        action = "change_search_space"
        why = "no live-beating candidates are available to learn from."
    elif (route_concentration or {}).get("active"):
        action = "repair_first"
        why = "winner concentration is above the scale threshold; repair must broaden before scaling."
    elif crowded >= 3:
        action = "change_search_space"
        why = "winner concentration is crowding narrow routes; broaden before scaling."
    else:
        action = "limited_learning_run"
        why = "signals exist but are not yet promotion-clean."
    next_mode = "repair_exploration" if action == "loosen_repair" or alias_high or sampled_overconcentrated else "promotion_repair" if action == "repair_first" else "discovery"
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "recommended_action": action,
        "why": why,
        "repair_yield_pct": live_yield,
        "repair_yield_floor_pct": repair_floor,
        "next_mode": next_mode,
        "repair_regression_detector": repair_regression or {},
        "route_concentration_warning": route_concentration or {},
        "anti_alias_pressure": anti_alias or {},
        "sampled_route_concentration": sampled_concentration or {},
    }


def _is_tiny_diagnostic_run(args: argparse.Namespace, variant_funnel: dict[str, Any] | None = None) -> bool:
    threshold = int(getattr(args, "tiny_run_threshold", 1000) or 1000)
    limited_min = int(getattr(args, "limited_learning_min_variants", 500) or 500)
    requested = int((variant_funnel or {}).get("requested_variants") or 0)
    if requested <= 0:
        requested = max(1, int(args.batch_size)) * max(1, int(args.max_batches))
    return requested < min(threshold, limited_min)


def _tiny_run_calibration_guard(args: argparse.Namespace, variant_funnel: dict[str, Any]) -> dict[str, Any]:
    tiny = _is_tiny_diagnostic_run(args, variant_funnel)
    threshold = int(getattr(args, "tiny_run_threshold", 1000) or 1000)
    limited_min = int(getattr(args, "limited_learning_min_variants", 500) or 500)
    requested = int(variant_funnel.get("requested_variants") or 0)
    if requested <= 0:
        requested = max(1, int(args.batch_size)) * max(1, int(args.max_batches))
    if tiny:
        tier = "diagnostic"
        multiplier = 0.25
    elif requested < threshold:
        tier = "limited_learning"
        multiplier = 0.50
    else:
        tier = "durable_learning"
        multiplier = 1.0
    cycle_tiers = [row for row in (variant_funnel.get("cycle_learning_tiers") or []) if isinstance(row, dict)]
    if cycle_tiers:
        tier_counts = Counter(str(row.get("learning_tier") or "unknown") for row in cycle_tiers)
        weighted = sum(float(row.get("learning_weight_multiplier") or 0.0) * int(row.get("requested_variants") or 0) for row in cycle_tiers)
        denom = sum(int(row.get("requested_variants") or 0) for row in cycle_tiers) or 1
        multiplier = round(weighted / denom, 4)
        if tier_counts.get("diagnostic") == len(cycle_tiers):
            tier = "diagnostic_rollup"
            tiny = True
        elif tier_counts.get("durable_learning"):
            tier = "mixed_learning_rollup" if len(tier_counts) > 1 else "durable_learning"
            tiny = False
        else:
            tier = "limited_learning_rollup"
            tiny = False
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "is_tiny_diagnostic": tiny,
        "learning_tier": tier,
        "threshold_variants": threshold,
        "limited_learning_min_variants": limited_min,
        "requested_variants": requested,
        "learning_weight_multiplier": multiplier,
        "persist_to_learning_db": (not tiny) or bool(getattr(args, "persist_tiny_run_learning", False)),
        "cycle_learning_tiers": cycle_tiers,
        "rules": [
            "diagnostic runs may validate plumbing and generate hypotheses",
            "500-999 variant runs are limited learning: lower weight, but durable enough to remember",
            "diagnostic runs should not update durable priors unless explicitly allowed",
            "promotion decisions still require larger holdout/robustness evidence",
        ],
    }


def _pre_hunt_stop_go_gate(
    args: argparse.Namespace,
    run_dir: Path,
    rankings: dict[str, Any] | None = None,
    variant_funnel: dict[str, Any] | None = None,
    online_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    state = online_state or step2_online_learning.read_state(run_dir / "online_state.json")
    readiness = hunt_intel.read_json(run_dir / "pre_hunt_readiness_gate.json", {}) or state.get("pre_hunt_readiness_gate") or {}
    sentinel = hunt_intel.read_json(run_dir / "learning_failure_sentinel.json", {}) or state.get("learning_failure_sentinel") or {}
    schema = hunt_intel.read_json(run_dir / "learning_artifact_schema_registry.json", {}) or state.get("learning_artifact_schema_registry") or {}
    repair_queue = hunt_intel.read_json(run_dir / "promotion_evidence_repair_queue.json", {}) or {}
    quality_floor = hunt_intel.read_json(run_dir / "live_beater_quality_floor_top100.json", {}) or {}
    auto_rec = hunt_intel.read_json(run_dir / "auto_hunt_recommendation.json", {}) or {}
    tiny_guard = _tiny_run_calibration_guard(args, variant_funnel or {})
    failures = []
    if schema and not schema.get("valid", True):
        failures.append("schema_registry_invalid")
    if sentinel and not sentinel.get("ok", True):
        failures.append("learning_failure_sentinel_blocking")
    if readiness and not readiness.get("ready", True):
        failures.append("pre_hunt_readiness_gate_failed")
    if not bool(args.live_only):
        failures.append("live_only_disabled")
    if failures:
        decision = "repair_first"
    elif auto_rec.get("recommended_action") in {"repair_first", "loosen_repair"}:
        decision = "repair_first"
    elif int(repair_queue.get("task_count") or 0) > 0 and int(quality_floor.get("kept_count") or 0) == 0 and len((rankings or {}).get("raw_leaderboard") or []) > 0:
        decision = "repair_first"
    elif tiny_guard.get("is_tiny_diagnostic") or str(args.runtime_control_mode or "enforce") == "observe":
        decision = "observe_only"
    else:
        decision = "ready_to_hunt"
    top_count = len((rankings or {}).get("raw_leaderboard") or [])
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "decision": decision,
        "failures": failures,
        "live_only": bool(args.live_only),
        "runtime_control_mode": str(args.runtime_control_mode or "enforce"),
        "top100_count": top_count,
        "repair_task_count": int(repair_queue.get("task_count") or 0),
        "promotion_quality_floor_kept": int(quality_floor.get("kept_count") or 0),
        "auto_hunt_recommendation": auto_rec,
        "tiny_run_calibration_guard": tiny_guard,
        "recommendation": {
            "ready_to_hunt": "run_normal_hunt_with_enforced_controls",
            "observe_only": "hunt_for_diagnostics_but_do_not_promote_or_update_durable_priors",
            "repair_first": "repair_learning_controls_or_promotion_evidence_before_large_hunt",
        }[decision],
    }


def _hunt_brain_summary(
    args: argparse.Namespace,
    run_dir: Path,
    rankings: dict[str, Any],
    cycles: list[dict[str, Any]],
    variant_funnel: dict[str, Any],
    repair_queue: dict[str, Any],
    repair_directives: dict[str, Any],
    stop_go_gate: dict[str, Any],
) -> dict[str, Any]:
    def compact_candidate(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "variant": row.get("variant"),
            "route_key": hunt_intel.route_key_from_row(row),
            "step2_pnl": row.get("step2_pnl"),
            "step2_delta_vs_active": row.get("step2_delta_vs_active"),
            "promotion_readiness_score": row.get("promotion_readiness_score"),
            "promotion_survival_score": row.get("promotion_survival_score"),
            "learning_tags": list(row.get("learning_tags") or [])[:8],
        }

    top = list(rankings.get("raw_leaderboard") or [])
    survival = _promotion_survival_leaderboard(top, limit=10)
    telemetry = step2_online_learning.telemetry_summary(_read_jsonl(run_dir / "streaming_telemetry.jsonl"))
    top_routes = [cluster.get("route_key") for cluster in hunt_intel.route_clusters(top, limit=5) if cluster.get("route_key")]
    failure_summary = hunt_intel.read_json(run_dir / "top_failure_summary.json", {}) or {}
    blocker_digest = hunt_intel.read_json(run_dir / "promotion_blocker_digest.json", {}) or {}
    anti_alias = hunt_intel.read_json(run_dir / "anti_alias_pressure.json", {}) or {}
    sampled_concentration = hunt_intel.read_json(run_dir / "sampled_route_concentration.json", {}) or {}
    auto_rec = hunt_intel.read_json(run_dir / "auto_hunt_recommendation.json", {}) or {}
    next_500 = hunt_intel.read_json(run_dir / "next_500_variant_plan.json", {}) or {}
    progress = hunt_intel.read_json(run_dir / "repair_progress_report.json", {}) or {}
    distance_cards = hunt_intel.read_json(run_dir / "route_promotion_distance_cards.json", {}) or {}
    closest = distance_cards.get("closest_route") if isinstance(distance_cards.get("closest_route"), dict) else {}
    weak_tags = Counter()
    for row in top:
        weak_tags.update(str(tag) for tag in (row.get("learning_tags") or []) if tag in {
            "no_holdout_credit", "promotion_weak", "small_total_edge", "route_narrow", "weak_day_consistency", "high_overfit_risk"
        })
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "objective": str(getattr(args, "ranking_objective", "promotion_survival")),
        "stop_go_decision": stop_go_gate.get("decision"),
        "what_worked": [
            f"scored {variant_funnel.get('scored_total')} variants with count_contract_ok={variant_funnel.get('count_contract_ok')}",
            f"found {variant_funnel.get('live_beaters_streamed')} live beaters and kept {len(top)} behavior-unique candidates",
            f"best raw P/L candidate: {(top[0] or {}).get('variant') if top else 'none'}",
        ],
        "what_failed": [
            f"top weak tag: {weak_tags.most_common(1)[0][0]} x{weak_tags.most_common(1)[0][1]}" if weak_tags else "no repeated weak tag found",
            f"promotion repair tasks open: {repair_queue.get('task_count') or 0}",
            (failure_summary.get("human_summary") or "promotion-floor failure summary pending"),
            (blocker_digest.get("human_summary") or "promotion blocker digest pending"),
            f"anti-alias pressure: {anti_alias.get('pressure')} collapse={anti_alias.get('collapse_rate')}",
            f"sampled route concentration: {sampled_concentration.get('recommended_action') or 'pending'}",
            f"tiny diagnostic guard active: {stop_go_gate.get('tiny_run_calibration_guard', {}).get('is_tiny_diagnostic')}",
        ],
        "what_changed": [
            "promotion-survival leaderboard now separates promotability from raw P/L",
            "repair directives are available for worker focus",
            f"runtime gate says {stop_go_gate.get('decision')}",
            f"repair progress better: {', '.join(progress.get('what_got_better') or [])}" if progress.get("what_got_better") else "repair progress comparison pending",
            f"repair progress worse: {', '.join(progress.get('what_got_worse') or [])}" if progress.get("what_got_worse") else "no repair regression flagged yet",
        ],
        "hunt_next": [
            "spend first budget on holdout/day-split repair for the best live beaters",
            "use route breadth expansion only after the candidate stays above Live",
            "keep crazy indicator shuffles, but graduate only candidates that lift promotion survival",
            f"auto recommendation: {auto_rec.get('recommended_action') or 'pending'}",
            f"next 500 objective: {next_500.get('objective') or 'pending'}",
            f"top route target: {closest.get('route_key')} next repair {closest.get('next_best_repair')}" if closest else "top route target pending",
        ],
        "stop_wasting_time_on": [
            "raw P/L aliases without new behavior",
            "route-narrow candidates that cannot gain holdout credit",
            "tiny-run conclusions treated as durable proof",
        ],
        "top_raw": [compact_candidate(row) for row in top[:5]],
        "top_promotion_survival": [compact_candidate(row) for row in survival[:5]],
        "focus_routes": list(dict.fromkeys(list(repair_directives.get("focus_routes") or []) + top_routes))[:12],
        "telemetry_summary": telemetry,
        "variant_funnel": variant_funnel,
    }


def _write_learning_artifacts(args: argparse.Namespace, run_dir: Path, rankings: dict[str, Any]) -> dict[str, str]:
    top100 = _apply_route_breadth_context(list(rankings.get("raw_leaderboard") or []))
    rankings["raw_leaderboard"] = top100
    quality = list(rankings.get("promotion_quality_leaderboard") or [])
    all_rows = list(rankings.get("diagnostic_rows") or rankings.get("decorated_rows") or top100)
    finalists = hunt_intel.finalists_payload(top100, limit=int(args.finalist_limit), source="run_step2_three_hour_hunt")
    oos_queue = hunt_intel.oos_replay_queue(quality or top100, limit=int(args.oos_queue_limit), source="run_step2_three_hour_hunt")
    behavior_cache = hunt_intel.behavior_cache_payload(top100, source="run_step2_three_hour_hunt")
    repair_queue = _promotion_evidence_repair_queue(top100, limit=100)
    repair_directives = _repair_worker_directives(repair_queue)
    cycles = _load_cycle_log(run_dir)
    variant_funnel = _variant_funnel(args, run_dir, cycles, rankings)
    anti_alias = _anti_alias_pressure_report(variant_funnel, top100)
    evidence_validation = _promotion_evidence_validation_report(
        top100,
        active_row=rankings.get("active_row") if isinstance(rankings.get("active_row"), dict) else None,
        start_balance=float(args.start_balance),
        limit=int(args.leaderboard_limit),
    )
    evidence_by_variant = evidence_validation.get("by_variant") if isinstance(evidence_validation.get("by_variant"), dict) else {}
    survival = _promotion_survival_leaderboard(
        top100,
        limit=int(args.leaderboard_limit),
        evidence_by_variant=evidence_by_variant,
        sort_mode=str(getattr(args, "promotion_survival_sort", "score") or "score"),
    )
    readiness_components = _readiness_component_diagnostics(top100, evidence_by_variant, limit=int(args.leaderboard_limit))
    near_evidence_queue = _near_promotion_evidence_queue(top100, evidence_validation, limit=int(args.leaderboard_limit))
    evidence_repair_lanes = _evidence_repair_lanes(evidence_validation, top100)
    quality_floor = _quality_floor_leaderboard(top100, evidence_by_variant=evidence_by_variant, limit=int(args.leaderboard_limit))
    route_actions = _route_action_report(top100)
    route_crowding = _route_crowding_report(top100)
    tradeoff = _tradeoff_frontier(top100, evidence_by_variant=evidence_by_variant, limit=int(args.leaderboard_limit))
    leaderboards = _separate_leaderboards(top100, evidence_by_variant=evidence_by_variant, limit=int(args.leaderboard_limit))
    rank_delta = _rank_delta_summary(top100, survival)
    raw_rank_reason = _why_raw_rank1_not_promotion_rank1(top100, survival, evidence_by_variant=evidence_by_variant)
    route_evidence_cards = _route_evidence_cards(top100, evidence_by_variant)
    route_promotion_distance = _route_promotion_distance_cards(top100, evidence_by_variant)
    per_route_lane_allocation = _per_route_repair_lane_allocation(route_promotion_distance)
    suspicious_rows = [
        {
            "variant": row.get("variant"),
            "route_key": hunt_intel.route_key_from_row(row),
            "step2_pnl": row.get("step2_pnl"),
            "delta_vs_active": row.get("step2_delta_vs_active"),
            "suspicious_winner": _suspicious_winner(row, evidence_by_variant.get(str(row.get("variant") or "")) or {}),
            "winner_mechanism": _winner_mechanism(row),
        }
        for row in top100
        if _suspicious_winner(row, evidence_by_variant.get(str(row.get("variant") or "")) or {}).get("flagged")
    ]
    incomplete_rows = [
        {
            "variant": row.get("variant"),
            "route_key": hunt_intel.route_key_from_row(row),
            "evidence_gaps": _suspicious_winner(row, evidence_by_variant.get(str(row.get("variant") or "")) or {}).get("evidence_gaps") or [],
            "severity": _suspicious_winner(row, evidence_by_variant.get(str(row.get("variant") or "")) or {}).get("evidence_incomplete_severity"),
        }
        for row in top100
        if _suspicious_winner(row, evidence_by_variant.get(str(row.get("variant") or "")) or {}).get("evidence_incomplete")
    ]
    suspicious = {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "flagged_count": len(suspicious_rows),
        "evidence_incomplete_count": len(incomplete_rows),
        "flagged": sorted(suspicious_rows, key=lambda item: float((item.get("suspicious_winner") or {}).get("score") or 0.0), reverse=True)[:100],
        "evidence_incomplete_examples": incomplete_rows[:25],
    }
    repair_memory = _repair_attempt_memory(run_dir, top100, repair_queue, evidence_by_variant=evidence_by_variant)
    saturation_controls = _family_saturation_controls(top100, route_crowding)
    route_budget_caps = _distance_adjusted_route_budget_caps(_repair_route_budget_caps(repair_queue, anti_alias), route_promotion_distance)
    failure_summary = _top_failure_summary(top100, repair_queue, suspicious, evidence_by_variant=evidence_by_variant)
    blocker_digest = _promotion_blocker_digest(top100, evidence_by_variant)
    repair_success = _repair_success_scoreboard(top100, evidence_by_variant, route_budget_caps)
    repair_lane_performance = _repair_lane_performance_scoreboard(top100, evidence_by_variant, limit=int(args.leaderboard_limit))
    repair_narrowness = _repair_narrowness_warning(args, variant_funnel)
    repair_progress = _repair_progress_report(args, run_dir, top100, evidence_by_variant, variant_funnel)
    best_repair_by_distance = _best_repair_candidate_by_promotion_distance(top100, evidence_by_variant)
    promotion_precheck = _promotion_review_precheck(top100[0] if top100 else {}, evidence_by_variant.get(str((top100[0] if top100 else {}).get("variant") or ""), {}) if top100 else {})
    promotion_ready_summary = _promotion_ready_summary(top100, evidence_by_variant)
    closest_to_promotion = _closest_to_promotion_summary(top100, evidence_by_variant)
    route_concentration = _route_concentration_warning(top100, route_budget_caps)
    intended_focus = _intended_focus_routes(args, run_dir, repair_queue, route_promotion_distance)
    strict_cli_focus = [str(route) for route in (getattr(args, "focus_route", []) or []) if str(route)]
    winner_distribution = _winner_route_distribution(top100, intended_focus, strict_focus_routes=strict_cli_focus)
    repair_regression = _repair_regression_detector(args, run_dir, variant_funnel, repair_progress, route_concentration, promotion_ready_summary)
    route_admission = _route_admission_gate(top100, intended_focus, route_concentration, repair_regression)
    actual_route_allocation = _actual_route_allocation(run_dir)
    sampled_concentration = _sampled_route_concentration_report(actual_route_allocation)
    behavior_generation_controls = _behavior_unique_generation_controls(anti_alias, winner_distribution, route_admission, blocker_digest)
    quality_protection_controls = _promotion_quality_protection_controls(variant_funnel, repair_progress, blocker_digest, winner_distribution, route_admission)
    next_500 = _next_500_plan(
        top100,
        repair_queue,
        route_crowding,
        quality_floor,
        anti_alias=anti_alias,
        route_budget_caps=route_budget_caps,
        preserved_routes=list(route_admission.get("preserved_focus_routes") or intended_focus),
        suppressed_routes=list(route_admission.get("avoid_as_center_routes") or []),
    )
    auto_recommendation = _auto_hunt_recommendation(
        top100,
        quality_floor,
        repair_queue,
        route_crowding,
        variant_funnel=variant_funnel,
        args=args,
        repair_regression=repair_regression,
        route_concentration=route_concentration,
        anti_alias=anti_alias,
        sampled_concentration=sampled_concentration,
    )
    recommendation_severity = _post_test_recommendation_severity(auto_recommendation, quality_floor, anti_alias, repair_queue, variant_funnel)
    run_comparison = _run_to_run_comparison(run_dir, variant_funnel)
    stop_go_gate_for_decision = _pre_hunt_stop_go_gate(args, run_dir, rankings, variant_funnel)
    repair_now = _repair_cycle_now_decision(
        stop_go_gate_for_decision,
        auto_recommendation,
        repair_queue,
        quality_floor,
        anti_alias,
        route_budget_caps=route_budget_caps,
        route_admission=route_admission,
        repair_regression=repair_regression,
    )
    repair_allocation = _repair_allocation_by_evidence_weakness(evidence_repair_lanes, route_budget_caps)
    runtime_handoff = _runtime_handoff_controls(
        route_admission,
        anti_alias,
        behavior_generation_controls,
        quality_protection_controls,
        repair_now,
        route_budget_caps,
        repair_allocation,
        promotion_ready_summary,
        winner_distribution,
        blocker_digest,
        _promotion_survival_reference(survival),
    )
    next_500["evidence_weakness_allocation"] = repair_allocation
    next_500["per_route_lane_allocation"] = per_route_lane_allocation
    next_500 = _normalize_next_500_plan(next_500, route_budget_caps)
    next_command_recipe = _align_next_command_recipe_with_handoff(
        _next_command_recipe(args, run_dir, repair_now, route_budget_caps),
        runtime_handoff,
    )
    plan_vs_actual_route_allocation = _plan_vs_actual_route_allocation(next_500, actual_route_allocation)
    plan_vs_winner_route_allocation = _plan_vs_winner_route_allocation(next_500, winner_distribution)
    stale_focus_audit = _stale_focus_audit(cycles, repair_queue, route_promotion_distance, next_command_recipe, route_admission=route_admission)
    big_run_preflight = _big_run_preflight_gate(
        args,
        run_dir,
        variant_funnel,
        next_500,
        actual_route_allocation,
        promotion_ready_summary,
        route_concentration,
        stale_focus_audit,
        failure_summary,
        repair_regression,
        route_admission,
        anti_alias=anti_alias,
        sampled_concentration=sampled_concentration,
        repair_progress=repair_progress,
        blocker_digest=blocker_digest,
        winner_distribution=winner_distribution,
    )
    run_size_recommendation = _run_size_recommendation(variant_funnel, big_run_preflight)
    before_big_run_issues = _before_big_run_issue_register(
        big_run_preflight,
        failure_summary,
        blocker_digest,
        anti_alias,
        sampled_concentration,
        repair_progress,
    )
    big_run_launch_controls = _big_run_launch_controls(
        big_run_preflight,
        runtime_handoff,
        next_command_recipe,
        failure_summary,
        repair_allocation,
        route_budget_caps,
        promotion_ready_summary,
        winner_distribution,
        sampled_concentration,
        repair_lane_performance,
        readiness_components,
    )
    big_run_truth_summary = _big_run_truth_summary(
        variant_funnel,
        promotion_ready_summary,
        big_run_preflight,
        big_run_launch_controls,
        recommendation_severity,
        run_size_recommendation,
    )
    next_500["plan_vs_actual_route_allocation"] = plan_vs_actual_route_allocation
    next_500["plan_vs_winner_route_allocation"] = plan_vs_winner_route_allocation
    next_500["actual_route_allocation"] = actual_route_allocation
    next_500["winner_route_distribution"] = winner_distribution
    next_500["route_admission_gate"] = route_admission
    next_500["repair_regression_detector"] = repair_regression
    next_500["sampled_route_concentration"] = sampled_concentration
    next_500["promotion_blocker_digest"] = blocker_digest
    next_500["behavior_unique_generation_controls"] = behavior_generation_controls
    next_500["promotion_quality_protection_controls"] = quality_protection_controls
    next_500["runtime_handoff_controls"] = runtime_handoff
    next_500["big_run_launch_controls"] = big_run_launch_controls
    next_500["big_run_truth_summary"] = big_run_truth_summary
    next_plan = hunt_intel.next_hunt_plan(top100)
    feedback_rows = []
    if args.feedback_json:
        feedback_payload = hunt_intel.read_json(args.feedback_json, {}) or {}
        feedback_rows = list(feedback_payload.get("feedback") or feedback_payload.get("rows") or feedback_payload.get("queue") or [])
    review_feedback = hunt_intel.read_json(run_dir / "promotion_review_feedback.json", {}) or {}
    feedback_rows.extend(list(review_feedback.get("feedback") or []))
    learning = hunt_intel.learning_report(
        rows=top100,
        cycles=cycles,
        all_rows=all_rows,
        feedback_rows=feedback_rows,
        source="run_step2_three_hour_hunt",
    )
    return {
        "finalists": _write_json(run_dir / "finalists.json", finalists),
        "annotated_top100": _write_json(run_dir / "annotated_top100.json", learning.get("annotated_top100") or {}),
        "near_miss_archive": _write_json(run_dir / "near_miss_archive.json", learning.get("near_miss_archive") or {}),
        "adaptive_mutation_lanes": _write_json(run_dir / "adaptive_mutation_lanes.json", learning.get("adaptive_mutation_lanes") or {}),
        "worker_role_plan": _write_json(run_dir / "worker_role_plan.json", learning.get("worker_role_plan") or {}),
        "candidate_lineage_graph": _write_json(run_dir / "candidate_lineage_graph.json", learning.get("candidate_lineage_graph") or {}),
        "family_rejection_memory": _write_json(run_dir / "family_rejection_memory.json", learning.get("family_rejection_memory") or {}),
        "active_experiment_plan": _write_json(run_dir / "active_experiment_plan.json", learning.get("active_experiment_plan") or {}),
        "experiment_outcome_ledger": _write_json(run_dir / "experiment_outcome_ledger.json", learning.get("experiment_outcome_ledger") or {}),
        "causal_experiment_registry": _write_json(run_dir / "causal_experiment_registry.json", learning.get("causal_experiment_registry") or {}),
        "experiment_debt_queue": _write_json(run_dir / "experiment_debt_queue.json", learning.get("experiment_debt_queue") or {}),
        "information_gain_scoring": _write_json(run_dir / "information_gain_scoring.json", learning.get("information_gain_scoring") or {}),
        "value_of_information_planner": _write_json(run_dir / "value_of_information_planner.json", learning.get("value_of_information_planner") or {}),
        "decision_change_tracker": _write_json(run_dir / "decision_change_tracker.json", learning.get("decision_change_tracker") or {}),
        "hypothesis_quality_scoring": _write_json(run_dir / "hypothesis_quality_scoring.json", learning.get("hypothesis_quality_scoring") or {}),
        "evidence_sufficiency_gate": _write_json(run_dir / "evidence_sufficiency_gate.json", learning.get("evidence_sufficiency_gate") or {}),
        "counterfactual_shadow_board": _write_json(run_dir / "counterfactual_shadow_board.json", learning.get("counterfactual_shadow_board") or {}),
        "prediction_calibration_ledger": _write_json(run_dir / "prediction_calibration_ledger.json", learning.get("prediction_calibration_ledger") or {}),
        "belief_revision_engine": _write_json(run_dir / "belief_revision_engine.json", learning.get("belief_revision_engine") or {}),
        "adversarial_red_team_learner": _write_json(run_dir / "adversarial_red_team_learner.json", learning.get("adversarial_red_team_learner") or {}),
        "out_of_distribution_detector": _write_json(run_dir / "out_of_distribution_detector.json", learning.get("out_of_distribution_detector") or {}),
        "memory_compression_distiller": _write_json(run_dir / "memory_compression_distiller.json", learning.get("memory_compression_distiller") or {}),
        "self_audit_score": _write_json(run_dir / "self_audit_score.json", learning.get("self_audit_score") or {}),
        "truth_first_promotion_objective": _write_json(run_dir / "truth_first_promotion_objective.json", learning.get("truth_first_promotion_objective") or {}),
        "learning_velocity_dashboard": _write_json(run_dir / "learning_velocity_dashboard.json", learning.get("learning_velocity_dashboard") or {}),
        "compiled_hunt_policy": _write_json(run_dir / "compiled_hunt_policy.json", learning.get("compiled_hunt_policy") or {}),
        "policy_executor": _write_json(run_dir / "policy_executor.json", learning.get("policy_executor") or {}),
        "adaptive_worker_assignment": _write_json(run_dir / "adaptive_worker_assignment.json", learning.get("adaptive_worker_assignment") or {}),
        "policy_backtester": _write_json(run_dir / "policy_backtester.json", learning.get("policy_backtester") or {}),
        "policy_mutation_engine": _write_json(run_dir / "policy_mutation_engine.json", learning.get("policy_mutation_engine") or {}),
        "policy_tournament": _write_json(run_dir / "policy_tournament.json", learning.get("policy_tournament") or {}),
        "champion_challenger_memory": _write_json(run_dir / "champion_challenger_memory.json", learning.get("champion_challenger_memory") or {}),
        "regime_specific_policies": _write_json(run_dir / "regime_specific_policies.json", learning.get("regime_specific_policies") or {}),
        "causal_graph_of_learning": _write_json(run_dir / "causal_graph_of_learning.json", learning.get("causal_graph_of_learning") or {}),
        "policy_safety_rail": _write_json(run_dir / "policy_safety_rail.json", learning.get("policy_safety_rail") or {}),
        "auto_promoted_field_manual": _write_json(run_dir / "auto_promoted_field_manual.json", learning.get("auto_promoted_field_manual") or {}),
        "policy_drift_detector": _write_json(run_dir / "policy_drift_detector.json", learning.get("policy_drift_detector") or {}),
        "route_regime_half_life": _write_json(run_dir / "route_regime_half_life.json", learning.get("route_regime_half_life") or {}),
        "learning_market_map": _write_json(run_dir / "learning_market_map.json", learning.get("learning_market_map") or {}),
        "concept_drift_alarms": _write_json(run_dir / "concept_drift_alarms.json", learning.get("concept_drift_alarms") or {}),
        "revalidation_scheduler": _write_json(run_dir / "revalidation_scheduler.json", learning.get("revalidation_scheduler") or {}),
        "temporal_ensemble_policy": _write_json(run_dir / "temporal_ensemble_policy.json", learning.get("temporal_ensemble_policy") or {}),
        "active_experiment_governor": _write_json(run_dir / "active_experiment_governor.json", learning.get("active_experiment_governor") or {}),
        "route_state_machine": _write_json(run_dir / "route_state_machine.json", learning.get("route_state_machine") or {}),
        "negative_knowledge_bank": _write_json(run_dir / "negative_knowledge_bank.json", learning.get("negative_knowledge_bank") or {}),
        "promotion_survivor_model": _write_json(run_dir / "promotion_survivor_model.json", learning.get("promotion_survivor_model") or {}),
        "mutation_grammar_learner": _write_json(run_dir / "mutation_grammar_learner.json", learning.get("mutation_grammar_learner") or {}),
        "real_time_worker_rebalancer": _write_json(run_dir / "real_time_worker_rebalancer.json", learning.get("real_time_worker_rebalancer") or {}),
        "hunt_replay_simulator": _write_json(run_dir / "hunt_replay_simulator.json", learning.get("hunt_replay_simulator") or {}),
        "resurrection_engine": _write_json(run_dir / "resurrection_engine.json", learning.get("resurrection_engine") or {}),
        "causal_mutation_attribution": _write_json(run_dir / "causal_mutation_attribution.json", learning.get("causal_mutation_attribution") or {}),
        "uncertainty_budgeting": _write_json(run_dir / "uncertainty_budgeting.json", learning.get("uncertainty_budgeting") or {}),
        "promotability_pareto_frontier": _write_json(run_dir / "promotability_pareto_frontier.json", learning.get("promotability_pareto_frontier") or {}),
        "false_lesson_detector": _write_json(run_dir / "false_lesson_detector.json", learning.get("false_lesson_detector") or {}),
        "experiment_graduation_system": _write_json(run_dir / "experiment_graduation_system.json", learning.get("experiment_graduation_system") or {}),
        "candidate_genealogy_diff_engine": _write_json(run_dir / "candidate_genealogy_diff_engine.json", learning.get("candidate_genealogy_diff_engine") or {}),
        "off_policy_hunt_evaluator": _write_json(run_dir / "off_policy_hunt_evaluator.json", learning.get("off_policy_hunt_evaluator") or {}),
        "self_competition_league": _write_json(run_dir / "self_competition_league.json", learning.get("self_competition_league") or {}),
        "evidence_contract_engine": _write_json(run_dir / "evidence_contract_engine.json", learning.get("evidence_contract_engine") or {}),
        "live_beater_quality_decomposer": _write_json(run_dir / "live_beater_quality_decomposer.json", learning.get("live_beater_quality_decomposer") or {}),
        "contradiction_detector": _write_json(run_dir / "contradiction_detector.json", learning.get("contradiction_detector") or {}),
        "learning_conflict_resolver": _write_json(run_dir / "learning_conflict_resolver.json", learning.get("learning_conflict_resolver") or {}),
        "cohort_based_memory": _write_json(run_dir / "cohort_based_memory.json", learning.get("cohort_based_memory") or {}),
        "adaptive_hunt_throttle": _write_json(run_dir / "adaptive_hunt_throttle.json", learning.get("adaptive_hunt_throttle") or {}),
        "promotion_readiness_simulator": _write_json(run_dir / "promotion_readiness_simulator.json", learning.get("promotion_readiness_simulator") or {}),
        "research_trace_ledger": _write_json(run_dir / "research_trace_ledger.json", learning.get("research_trace_ledger") or {}),
        "runtime_decision_kernel": _write_json(run_dir / "runtime_decision_kernel.json", learning.get("runtime_decision_kernel") or {}),
        "action_outcome_tracker": _write_json(run_dir / "action_outcome_tracker.json", learning.get("action_outcome_tracker") or {}),
        "closed_loop_reward_model": _write_json(run_dir / "closed_loop_reward_model.json", learning.get("closed_loop_reward_model") or {}),
        "autonomous_hunt_planner": _write_json(run_dir / "autonomous_hunt_planner.json", learning.get("autonomous_hunt_planner") or {}),
        "runtime_guardrails": _write_json(run_dir / "runtime_guardrails.json", learning.get("runtime_guardrails") or {}),
        "command_replay_ledger": _write_json(run_dir / "command_replay_ledger.json", learning.get("command_replay_ledger") or {}),
        "action_elo_league": _write_json(run_dir / "action_elo_league.json", learning.get("action_elo_league") or {}),
        "human_readable_hunt_brief": _write_json(run_dir / "human_readable_hunt_brief.json", learning.get("human_readable_hunt_brief") or {}),
        "ab_route_experiment_executor": _write_json(run_dir / "ab_route_experiment_executor.json", learning.get("ab_route_experiment_executor") or {}),
        "champion_challenger_runtime_slots": _write_json(run_dir / "champion_challenger_runtime_slots.json", learning.get("champion_challenger_runtime_slots") or {}),
        "adaptive_experiment_stopping": _write_json(run_dir / "adaptive_experiment_stopping.json", learning.get("adaptive_experiment_stopping") or {}),
        "counterfactual_command_replay": _write_json(run_dir / "counterfactual_command_replay.json", learning.get("counterfactual_command_replay") or {}),
        "experiment_contamination_guard": _write_json(run_dir / "experiment_contamination_guard.json", learning.get("experiment_contamination_guard") or {}),
        "learning_rate_controller": _write_json(run_dir / "learning_rate_controller.json", learning.get("learning_rate_controller") or {}),
        "worker_learning_report_cards": _write_json(run_dir / "worker_learning_report_cards.json", learning.get("worker_learning_report_cards") or {}),
        "experiment_to_promotion_trace": _write_json(run_dir / "experiment_to_promotion_trace.json", learning.get("experiment_to_promotion_trace") or {}),
        "zero_yield_autopsy_engine": _write_json(run_dir / "zero_yield_autopsy_engine.json", learning.get("zero_yield_autopsy_engine") or {}),
        "stuck_loop_breaker": _write_json(run_dir / "stuck_loop_breaker.json", learning.get("stuck_loop_breaker") or {}),
        "opportunity_cost_meter": _write_json(run_dir / "opportunity_cost_meter.json", learning.get("opportunity_cost_meter") or {}),
        "search_space_coverage_map": _write_json(run_dir / "search_space_coverage_map.json", learning.get("search_space_coverage_map") or {}),
        "live_beater_scarcity_mode": _write_json(run_dir / "live_beater_scarcity_mode.json", learning.get("live_beater_scarcity_mode") or {}),
        "alias_trap_detector": _write_json(run_dir / "alias_trap_detector.json", learning.get("alias_trap_detector") or {}),
        "route_seed_quality_score": _write_json(run_dir / "route_seed_quality_score.json", learning.get("route_seed_quality_score") or {}),
        "recovery_playbook_generator": _write_json(run_dir / "recovery_playbook_generator.json", learning.get("recovery_playbook_generator") or {}),
        "meta_hunt_strategy_learner": _write_json(run_dir / "meta_hunt_strategy_learner.json", learning.get("meta_hunt_strategy_learner") or {}),
        "run_to_run_postmortem": _write_json(run_dir / "run_to_run_postmortem.json", learning.get("run_to_run_postmortem") or {}),
        "treatment_effects": _write_json(run_dir / "treatment_effects.json", learning.get("treatment_effects") or {}),
        "treatment_prior_model": _write_json(run_dir / "treatment_prior_model.json", learning.get("treatment_prior_model") or {}),
        "treatment_worker_budget": _write_json(run_dir / "treatment_worker_budget.json", learning.get("treatment_worker_budget") or {}),
        "treatment_confidence_model": _write_json(run_dir / "treatment_confidence_model.json", learning.get("treatment_confidence_model") or {}),
        "controlled_parent_sibling_experiments": _write_json(run_dir / "controlled_parent_sibling_experiments.json", learning.get("controlled_parent_sibling_experiments") or {}),
        "live_beater_failure_autopsy": _write_json(run_dir / "live_beater_failure_autopsy.json", learning.get("live_beater_failure_autopsy") or {}),
        "worker_specialization_memory": _write_json(run_dir / "worker_specialization_memory.json", learning.get("worker_specialization_memory") or {}),
        "regime_aware_learning": _write_json(run_dir / "regime_aware_learning.json", learning.get("regime_aware_learning") or {}),
        "promotion_reject_simulator": _write_json(run_dir / "promotion_reject_simulator.json", learning.get("promotion_reject_simulator") or {}),
        "promotion_evidence_repair_queue": _write_json(run_dir / "promotion_evidence_repair_queue.json", repair_queue),
        "promotion_repair_worker_directives": _write_json(run_dir / "promotion_repair_worker_directives.json", repair_directives),
        "promotion_evidence_validation_report": _write_json(run_dir / "promotion_evidence_validation_report.json", evidence_validation),
        "readiness_component_diagnostics": _write_json(run_dir / "readiness_component_diagnostics.json", readiness_components),
        "near_promotion_evidence_queue": _write_json(run_dir / "near_promotion_evidence_queue.json", near_evidence_queue),
        "evidence_repair_lanes": _write_json(run_dir / "evidence_repair_lanes.json", evidence_repair_lanes),
        "promotion_survival_top100": _write_json(run_dir / "promotion_survival_top100.json", {
            "schema_version": 1,
            "source": "run_step2_three_hour_hunt",
            "objective": "P/L edge + holdout/readiness/day consistency/route breadth - overfit and evidence gaps",
            "sort_mode": str(getattr(args, "promotion_survival_sort", "score") or "score"),
            "leaderboard": survival,
        }),
        "live_beater_quality_floor_top100": _write_json(run_dir / "live_beater_quality_floor_top100.json", quality_floor),
        "route_action_taxonomy": _write_json(run_dir / "route_action_taxonomy.json", route_actions),
        "route_crowding_report": _write_json(run_dir / "route_crowding_report.json", route_crowding),
        "raw_pnl_vs_promotability_tradeoff": _write_json(run_dir / "raw_pnl_vs_promotability_tradeoff.json", tradeoff),
        "raw_vs_promotion_rank_delta": _write_json(run_dir / "raw_vs_promotion_rank_delta.json", rank_delta),
        "why_raw_rank1_not_promotion_rank1": _write_json(run_dir / "why_raw_rank1_not_promotion_rank1.json", raw_rank_reason),
        "route_evidence_cards": _write_json(run_dir / "route_evidence_cards.json", route_evidence_cards),
        "route_promotion_distance_cards": _write_json(run_dir / "route_promotion_distance_cards.json", route_promotion_distance),
        "per_route_repair_lane_allocation": _write_json(run_dir / "per_route_repair_lane_allocation.json", per_route_lane_allocation),
        "separate_learning_leaderboards": _write_json(run_dir / "separate_learning_leaderboards.json", leaderboards),
        "suspicious_winner_detector": _write_json(run_dir / "suspicious_winner_detector.json", suspicious),
        "repair_attempt_memory": _write_json(run_dir / "repair_attempt_memory.json", repair_memory),
        "anti_alias_pressure": _write_json(run_dir / "anti_alias_pressure.json", anti_alias),
        "behavior_unique_generation_controls": _write_json(run_dir / "behavior_unique_generation_controls.json", behavior_generation_controls),
        "promotion_quality_protection_controls": _write_json(run_dir / "promotion_quality_protection_controls.json", quality_protection_controls),
        "repair_route_budget_caps": _write_json(run_dir / "repair_route_budget_caps.json", route_budget_caps),
        "repair_allocation_by_evidence_weakness": _write_json(run_dir / "repair_allocation_by_evidence_weakness.json", repair_allocation),
        "best_repair_candidate_by_promotion_distance": _write_json(run_dir / "best_repair_candidate_by_promotion_distance.json", best_repair_by_distance),
        "repair_narrowness_warning": _write_json(run_dir / "repair_narrowness_warning.json", repair_narrowness),
        "repair_progress_report": _write_json(run_dir / "repair_progress_report.json", repair_progress),
        "promotion_review_precheck": _write_json(run_dir / "promotion_review_precheck.json", promotion_precheck),
        "promotion_ready_summary": _write_json(run_dir / "promotion_ready_summary.json", promotion_ready_summary),
        "closest_to_promotion_summary": _write_json(run_dir / "closest_to_promotion_summary.json", closest_to_promotion),
        "promotion_blocker_digest": _write_json(run_dir / "promotion_blocker_digest.json", blocker_digest),
        "route_concentration_warning": _write_json(run_dir / "route_concentration_warning.json", route_concentration),
        "winner_route_distribution": _write_json(run_dir / "winner_route_distribution.json", winner_distribution),
        "plan_vs_winner_route_allocation": _write_json(run_dir / "plan_vs_winner_route_allocation.json", plan_vs_winner_route_allocation),
        "sampled_route_concentration": _write_json(run_dir / "sampled_route_concentration.json", sampled_concentration),
        "repair_regression_detector": _write_json(run_dir / "repair_regression_detector.json", repair_regression),
        "route_admission_gate": _write_json(run_dir / "route_admission_gate.json", route_admission),
        "runtime_handoff_controls": _write_json(run_dir / "runtime_handoff_controls.json", runtime_handoff),
        "actual_route_allocation": _write_json(run_dir / "actual_route_allocation.json", actual_route_allocation),
        "plan_vs_actual_route_allocation": _write_json(run_dir / "plan_vs_actual_route_allocation.json", plan_vs_actual_route_allocation),
        "stale_focus_audit": _write_json(run_dir / "stale_focus_audit.json", stale_focus_audit),
        "big_run_preflight_gate": _write_json(run_dir / "big_run_preflight_gate.json", big_run_preflight),
        "run_size_recommendation": _write_json(run_dir / "run_size_recommendation.json", run_size_recommendation),
        "before_big_run_issue_register": _write_json(run_dir / "before_big_run_issue_register.json", before_big_run_issues),
        "big_run_launch_controls": _write_json(run_dir / "big_run_launch_controls.json", big_run_launch_controls),
        "big_run_truth_summary": _write_json(run_dir / "big_run_truth_summary.json", big_run_truth_summary),
        "next_command_recipe": _write_json(run_dir / "next_command_recipe.json", next_command_recipe),
        "repair_success_scoreboard": _write_json(run_dir / "repair_success_scoreboard.json", repair_success),
        "repair_lane_performance_scoreboard": _write_json(run_dir / "repair_lane_performance_scoreboard.json", repair_lane_performance),
        "next_500_variant_plan": _write_json(run_dir / "next_500_variant_plan.json", next_500),
        "variant_family_saturation": _write_json(run_dir / "variant_family_saturation.json", saturation_controls),
        "do_not_explore_controls": _write_json(run_dir / "do_not_explore_controls.json", saturation_controls),
        "top_failure_summary": _write_json(run_dir / "top_failure_summary.json", failure_summary),
        "auto_hunt_recommendation": _write_json(run_dir / "auto_hunt_recommendation.json", auto_recommendation),
        "post_test_recommendation_severity": _write_json(run_dir / "post_test_recommendation_severity.json", recommendation_severity),
        "repair_cycle_now_decision": _write_json(run_dir / "repair_cycle_now_decision.json", repair_now),
        "run_to_run_comparison": _write_json(run_dir / "run_to_run_comparison.json", run_comparison),
        "search_portfolio_manager": _write_json(run_dir / "search_portfolio_manager.json", learning.get("search_portfolio_manager") or {}),
        "diversity_finalists": _write_json(run_dir / "diversity_finalists.json", learning.get("diversity_constrained_finalists") or {}),
        "oos_replay_queue": _write_json(run_dir / "oos_replay_queue.json", oos_queue),
        "behavior_cache": _write_json(run_dir / "behavior_cache.json", behavior_cache),
        "next_hunt_plan": _write_json(run_dir / "next_hunt_plan.json", next_plan),
        "learning_report": _write_compact_json(run_dir / "learning_report.json", learning),
        "bandit_allocation": _write_json(run_dir / "bandit_allocation.json", learning.get("bandit_allocation") or {}),
        "validation_aware_bandit": _write_json(run_dir / "validation_aware_bandit.json", learning.get("validation_aware_bandit") or {}),
        "auto_validation_scheduler": _write_json(run_dir / "auto_validation_scheduler.json", learning.get("auto_validation_scheduler") or {}),
        "candidate_family_clusters": _write_json(run_dir / "candidate_family_clusters.json", learning.get("candidate_family_clusters") or {}),
        "counterfactual_route_attribution_plan": _write_json(run_dir / "counterfactual_route_attribution_plan.json", learning.get("counterfactual_route_attribution_plan") or {}),
        "temporal_generalization_map": _write_json(run_dir / "temporal_generalization_map.json", learning.get("temporal_generalization_map") or {}),
        "feature_interactions": _write_json(run_dir / "feature_interactions.json", learning.get("feature_interactions") or {}),
        "adversarial_perturbation_plan": _write_json(run_dir / "adversarial_perturbation_plan.json", learning.get("adversarial_perturbation_plan") or {}),
    }


def _status_policy(args: argparse.Namespace) -> dict[str, Any]:
    interval = max(1, int(args.status_interval_sec))
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "default_interval_sec": 300,
        "interval_sec": interval,
        "interval_min": round(interval / 60.0, 4),
        "contract": "Emit hunt_status updates every 5 minutes by default unless the user explicitly requests a lower interval.",
        "user_can_override_with": "--status-interval-sec",
        "status_stdout_mode": str(getattr(args, "status_stdout_mode", "all") or "all"),
        "status_verbosity": str(getattr(args, "status_verbosity", "compact") or "compact"),
        "status_event_verbosity": str(getattr(args, "status_event_verbosity", "digest") or "digest"),
        "artifact_profile": str(getattr(args, "artifact_profile", "compact") or "compact"),
        "coordinator_artifact_mode": str(getattr(args, "coordinator_artifact_mode", "minimal") or "minimal"),
        "score_cache_stats_mode": str(getattr(args, "score_cache_stats_mode", "fast") or "fast"),
    }


def _effective_run_seconds(args: argparse.Namespace) -> int:
    stop_after = int(getattr(args, "stop_after_sec", 0) or 0)
    if stop_after > 0:
        return max(1, stop_after)
    window_minutes = float(getattr(args, "window_minutes", 0.0) or 0.0)
    if window_minutes > 0:
        return max(1, int(window_minutes * 60.0))
    return max(1, int(float(args.hours) * 3600))


def _safety_status(row: dict[str, Any]) -> str:
    safety = row.get("memory_routed_profile_safety_gate") or row.get("routed_profile_safety")
    if isinstance(safety, dict):
        return "ok" if safety.get("ok") is True else "blocked"
    return "unknown"


def _promotion_passed(row: dict[str, Any]) -> bool:
    distance = row.get("promotion_distance")
    return bool(isinstance(distance, dict) and distance.get("passed") is True)


def _route_key(row: dict[str, Any]) -> str:
    try:
        return hunt_intel.route_key_from_row(row)
    except Exception:
        return str(row.get("route_key") or row.get("route_seed") or "unknown|unknown|unknown")


def _promotion_blockers(row: dict[str, Any]) -> str:
    safety = row.get("memory_routed_profile_safety_gate") or row.get("routed_profile_safety")
    if isinstance(safety, dict) and safety.get("ok") is False:
        failed = [
            str(check.get("name"))
            for check in safety.get("checks", [])
            if isinstance(check, dict) and check.get("ok") is False
        ]
        return "safety: " + ", ".join(failed[:3]) if failed else "safety blocked"
    distance = row.get("promotion_distance")
    if isinstance(distance, dict):
        failed = [
            str(gate.get("gate"))
            for gate in distance.get("gates", [])
            if isinstance(gate, dict) and gate.get("passed") is False
        ]
        if failed:
            return "promotion: " + ", ".join(failed[:3])
        if distance.get("passed") is True:
            return "passes promotion distance"
    tags = [str(tag) for tag in row.get("learning_tags", [])]
    weak = [
        tag
        for tag in tags
        if tag in {"no_holdout_credit", "promotion_weak", "small_total_edge", "route_narrow", "weak_day_consistency", "high_overfit_risk"}
        or tag.startswith("robustness_flag:")
    ]
    return ", ".join(weak[:3]) if weak else "none captured"


def _status_candidate(row: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(row, dict) or not row:
        return {}
    return {
        "variant": row.get("variant"),
        "route_key": _route_key(row),
        "step2_pnl": row.get("step2_pnl"),
        "delta_vs_live": row.get("step2_delta_vs_active") or row.get("delta_vs_active"),
        "promotion_readiness_score": row.get("promotion_readiness_score"),
        "evidence_adjusted_promotion_readiness_score": row.get("evidence_adjusted_promotion_readiness_score"),
        "promotion_distance_passed": _promotion_passed(row),
        "safety_status": _safety_status(row),
        "promotion_blockers_digest": _promotion_blockers(row),
    }


def _compact_status_line(
    *,
    phase: str,
    cycle_idx: int,
    hunter: str,
    top_count: int,
    rank1: dict[str, Any] | None,
    promotion_rank1: dict[str, Any] | None,
    next_lane: str,
    remaining_sec: int | None,
) -> str:
    raw = _status_candidate(rank1)
    promo = _status_candidate(promotion_rank1)
    if not raw:
        return f"{phase}: cycle={cycle_idx} hunter={hunter or 'n/a'} top100=0 remaining_sec={remaining_sec if remaining_sec is not None else 'n/a'}"
    return (
        f"{phase}: cycle={cycle_idx} hunter={hunter or 'n/a'} top100={top_count} "
        f"raw#1={raw.get('variant')} pnl={raw.get('step2_pnl')} delta={raw.get('delta_vs_live')} "
        f"ready={raw.get('promotion_readiness_score')} safety={raw.get('safety_status')} "
        f"promo_pass={raw.get('promotion_distance_passed')} blockers={raw.get('promotion_blockers_digest')}; "
        f"promo#1={(promo.get('variant') if promo else 'n/a')} promo_ready={(promo.get('promotion_readiness_score') if promo else 'n/a')}; "
        f"lane={next_lane or 'standard_rotation'} remaining_sec={remaining_sec if remaining_sec is not None else 'n/a'}"
    )


def _cmd_arg(cmd: list[str], flag: str, default: Any = None) -> Any:
    try:
        idx = list(cmd).index(flag)
        return cmd[idx + 1]
    except Exception:
        return default


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    try:
        with path.open("r", encoding="utf-8-sig") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except Exception:
                    continue
                if isinstance(payload, dict):
                    rows.append(payload)
    except Exception:
        return rows
    return rows


def _extract_json_object_from_tail(text: str) -> dict[str, Any]:
    text = str(text or "").strip()
    if not text:
        return {}
    for start in [idx for idx, char in enumerate(text) if char == "{"]:
        try:
            payload = json.loads(text[start:])
        except Exception:
            continue
        if isinstance(payload, dict):
            return payload
    return {}


def _coordinator_payload_from_cycle(cycle: dict[str, Any]) -> dict[str, Any]:
    payload = cycle.get("coordinator_payload")
    if isinstance(payload, dict):
        return payload
    parsed = _extract_json_object_from_tail(str(cycle.get("stdout_tail") or ""))
    path = parsed.get("path") if isinstance(parsed, dict) else ""
    if path:
        loaded = hunt_intel.read_json(Path(path), {}) or {}
        if isinstance(loaded, dict):
            return loaded
    return {}


def _compact_cycle_for_log(cycle: dict[str, Any]) -> dict[str, Any]:
    row = {}
    keep = {
        "cycle", "hunter", "hunt_mode", "learning_tier_contract", "seed", "cmd",
        "started_at_ct", "finished_at_ct", "elapsed_sec", "ok", "returncode",
        "timeout", "interrupted", "interrupt_reason", "validation_triggered",
        "streaming_telemetry_events", "streaming_telemetry_summary",
        "stdout_path", "stderr_path",
    }
    for key in keep:
        if key in cycle:
            row[key] = cycle.get(key)
    payload = _coordinator_payload_from_cycle(cycle)
    if payload:
        row["coordinator_summary"] = {
            "path": payload.get("path"),
            "ok": payload.get("ok"),
            "active_step2_pnl": payload.get("active_step2_pnl"),
            "hunters": payload.get("hunters"),
            "winner_count": len(payload.get("winners") or []) if isinstance(payload.get("winners"), list) else payload.get("winners"),
            "leaderboard_count": len(payload.get("leaderboard") or []) if isinstance(payload.get("leaderboard"), list) else 0,
            "run_count": len(payload.get("runs") or []) if isinstance(payload.get("runs"), list) else (1 if isinstance(payload.get("runs"), dict) else 0),
        }
    row["reported_scored_total"] = _cycle_scored_total(cycle)
    row["reported_winners"] = _cycle_winners(cycle)
    if cycle.get("stdout_tail"):
        row["stdout_tail"] = str(cycle.get("stdout_tail") or "")[-1200:]
    if cycle.get("stderr_tail"):
        row["stderr_tail"] = str(cycle.get("stderr_tail") or "")[-1200:]
    return row


def _cycle_log_payload(cycles: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "cycles": [_compact_cycle_for_log(cycle) for cycle in cycles],
    }


def _cycle_run_summaries(cycle: dict[str, Any]) -> list[dict[str, Any]]:
    payload = _coordinator_payload_from_cycle(cycle)
    runs = payload.get("runs") if isinstance(payload.get("runs"), list) else []
    if not runs and isinstance(payload.get("runs"), dict):
        runs = [payload.get("runs")]
    return [dict(row) for row in runs if isinstance(row, dict)]


def _cycle_scored_total(cycle: dict[str, Any]) -> int:
    direct = int(cycle.get("scored_total") or cycle.get("reported_scored_total") or 0)
    if direct > 0:
        return direct
    scored = int(hunt_intel._cycle_scored_total(cycle) or 0)
    if scored > 0:
        return scored
    total = 0
    for run in _cycle_run_summaries(cycle):
        run_scored = int(hunt_intel._cycle_scored_total(run) or run.get("scored_total") or 0)
        if run_scored <= 0 and run.get("summary_path"):
            summary = hunt_intel.read_json(Path(str(run.get("summary_path"))), {}) or {}
            run_scored = int(summary.get("scored_total") or summary.get("scored_manifest_rows") or 0)
        total += max(0, run_scored)
    return total


def _cycle_winners(cycle: dict[str, Any]) -> int:
    direct = int(cycle.get("winners") or cycle.get("reported_winners") or 0) if not isinstance(cycle.get("winners"), list) else len(cycle.get("winners") or [])
    if direct > 0:
        return direct
    winners = int(hunt_intel._cycle_winners(cycle) or 0)
    if winners > 0:
        return winners
    total = 0
    for run in _cycle_run_summaries(cycle):
        run_winners = int(hunt_intel._cycle_winners(run) or 0)
        if run_winners <= 0 and run.get("summary_path"):
            summary = hunt_intel.read_json(Path(str(run.get("summary_path"))), {}) or {}
            run_winners = len(summary.get("winners") or []) if isinstance(summary.get("winners"), list) else int(summary.get("winners") or 0)
        total += max(0, run_winners)
    return total


def _learning_tier_for_requested(args: argparse.Namespace, requested: int) -> dict[str, Any]:
    threshold = int(getattr(args, "tiny_run_threshold", 1000) or 1000)
    limited_min = int(getattr(args, "limited_learning_min_variants", 500) or 500)
    if requested < min(threshold, limited_min):
        tier = "diagnostic"
        weight = 0.25
    elif requested < threshold:
        tier = "limited_learning"
        weight = 0.50
    else:
        tier = "durable_learning"
        weight = 1.0
    return {
        "learning_tier": tier,
        "learning_weight_multiplier": weight,
        "requested_variants": requested,
    }


def _cycle_learning_tiers(args: argparse.Namespace, cycles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    tiers = []
    for cycle in cycles:
        cmd = list(cycle.get("cmd") or [])
        batch = int(_cmd_arg(cmd, "--batch-size", args.batch_size) or args.batch_size)
        batches = int(_cmd_arg(cmd, "--max-batches", 1) or 1)
        requested = max(1, batch) * max(1, batches)
        tier = dict(cycle.get("learning_tier_contract") or _learning_tier_for_requested(args, requested))
        tier.update({
            "cycle": cycle.get("cycle"),
            "hunter": cycle.get("hunter"),
            "hunt_mode": cycle.get("hunt_mode") or cycle.get("effective_hunt_mode"),
            "requested_variants": requested,
            "scored_total": _cycle_scored_total_with_fallback(args, cycle, requested),
        })
        tiers.append(tier)
    return tiers


def _cycle_scored_total_with_fallback(args: argparse.Namespace, cycle: dict[str, Any], requested: int) -> int:
    scored = int(_cycle_scored_total(cycle) or 0)
    if scored > 0:
        return scored
    telemetry = cycle.get("streaming_telemetry_summary") if isinstance(cycle.get("streaming_telemetry_summary"), dict) else {}
    if int(telemetry.get("events") or 0) > 0 and bool(cycle.get("ok")):
        return requested if bool(getattr(args, "exact_variant_count", False)) or "--exact-variant-count" in list(cycle.get("cmd") or []) else scored
    if bool(cycle.get("ok")) and "--exact-variant-count" in list(cycle.get("cmd") or []):
        return requested
    return scored


def _variant_funnel(
    args: argparse.Namespace,
    run_dir: Path,
    cycles: list[dict[str, Any]],
    rankings: dict[str, Any],
) -> dict[str, Any]:
    telemetry = _read_jsonl(run_dir / "streaming_telemetry.jsonl")
    requested_from_telemetry = sum(int(event.get("requested_batch_size") or 0) for event in telemetry)
    executed_from_telemetry = sum(int(event.get("batch_size") or 0) for event in telemetry)
    requested_from_cycles = 0
    for cycle in cycles:
        cmd = list(cycle.get("cmd") or [])
        batch = int(_cmd_arg(cmd, "--batch-size", args.batch_size) or args.batch_size)
        batches = int(_cmd_arg(cmd, "--max-batches", 1) or 1)
        requested_from_cycles += max(1, batch) * max(1, batches)
    scored_from_cycles = sum(_cycle_scored_total(cycle) for cycle in cycles)
    scored_from_telemetry = executed_from_telemetry
    scored_total = scored_from_cycles or scored_from_telemetry
    live_beaters = sum(int(event.get("live_beaters") or 0) for event in telemetry)
    reported_winners = sum(_cycle_winners(cycle) for cycle in cycles)
    input_rows = int(rankings.get("input_rows") or 0)
    raw_live_rows = int(rankings.get("live_filtered_rows") or 0)
    config_unique = int(rankings.get("config_unique_rows") or 0)
    behavior_unique = int(rankings.get("behavior_unique_rows") or 0)
    requested = requested_from_telemetry or requested_from_cycles
    live_unique_floor = min(raw_live_rows, live_beaters or raw_live_rows, config_unique or raw_live_rows)
    cycle_learning_tiers = _cycle_learning_tiers(args, cycles)
    if len(cycle_learning_tiers) == 1 and int(cycle_learning_tiers[0].get("scored_total") or 0) <= 0 and scored_total > 0:
        cycle_learning_tiers[0]["scored_total"] = scored_total
        cycle_learning_tiers[0]["scored_total_source"] = "variant_funnel_telemetry_fallback"
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "exact_variant_count": bool(args.exact_variant_count),
        "runtime_control_mode": str(args.runtime_control_mode or "enforce"),
        "smoke_validation_mode": bool(args.smoke_validation_mode),
        "requested_variants": requested,
        "requested_variants_source": "telemetry" if requested_from_telemetry else "cycle_command",
        "executed_batch_size_sum": executed_from_telemetry or None,
        "scored_total": scored_total,
        "live_beaters_streamed": live_beaters,
        "reported_winners": reported_winners,
        "count_definitions": {
            "scored_total": "All variants actually scored by child hunters/coordinator summaries.",
            "live_beaters_streamed": "Per-batch telemetry count of scored variants whose P/L beat current Live before behavior dedupe.",
            "reported_winners": "Child hunter winner count against its target threshold; may include active-seed ties depending on hunter.",
            "live_filtered_rows": "Collected live-beating rows after scorer artifact collection and live-only filtering.",
            "behavior_unique_rows": "Live-filtered rows after behavioral dedupe; this is the kept top100 count source.",
            "top100_count": "Behavior-unique variants retained in the running top100.",
        },
        "candidate_rows_collected": min(input_rows, scored_total) if scored_total else input_rows,
        "raw_scorer_rows_collected": input_rows,
        "raw_live_rows_collected": raw_live_rows,
        "live_filtered_rows": live_unique_floor,
        "config_unique_rows": config_unique,
        "behavior_unique_rows": behavior_unique,
        "top100_count": behavior_unique,
        "cycle_learning_tiers": cycle_learning_tiers,
        "scorer_source_count": len(rankings.get("scorer_source_paths") or []),
        "diagnostic_source_count": len(rankings.get("diagnostic_source_paths") or []),
        "scorer_source_paths": list(rankings.get("scorer_source_paths") or []),
        "stored_duplicate_rows_removed": max(0, input_rows - min(input_rows, scored_total)) if scored_total else 0,
        "live_duplicate_rows_removed": max(0, raw_live_rows - live_unique_floor),
        "live_to_behavior_unique_collapse": max(0, live_unique_floor - behavior_unique),
        "scored_to_behavior_unique_yield_pct": round(behavior_unique / max(1, scored_total) * 100.0, 4),
        "scored_to_live_stream_yield_pct": round(live_beaters / max(1, scored_total) * 100.0, 4),
        "count_contract_ok": (not bool(args.exact_variant_count)) or scored_total == requested,
    }


def _status_snapshot(
    args: argparse.Namespace,
    run_dir: Path,
    *,
    phase: str,
    cycle_idx: int,
    hunter: str = "",
    started_at: str = "",
    deadline: float | None = None,
    rankings: dict[str, Any] | None = None,
    telemetry_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rankings = rankings or collect_rankings(args, run_dir)
    top = list(rankings.get("raw_leaderboard") or [])
    readiness = list(rankings.get("promotion_readiness_leaderboard") or [])
    promotion_survival = _promotion_survival_leaderboard(top, limit=10) if top else []
    remaining_sec = max(0, int((deadline or time.time()) - time.time())) if deadline else None
    rank1 = hunt_intel.finalist_row(top[0], 1) if top else None
    readiness_rank1 = hunt_intel.finalist_row(readiness[0], 1) if readiness else None
    promotion_rank1 = hunt_intel.finalist_row(promotion_survival[0], 1) if promotion_survival else readiness_rank1
    online_state = step2_online_learning.read_state(run_dir / "online_state.json")
    micro_review = hunt_intel.read_json(run_dir / "promotion_micro_reviews.json", {}) or {}
    directives = list(micro_review.get("directives") or online_state.get("promotion_review_directives") or [])
    lane_pressure = online_state.get("promotion_review_lane_pressure") if isinstance(online_state.get("promotion_review_lane_pressure"), dict) else {}
    experiment_plan = hunt_intel.read_json(run_dir / "active_experiment_plan.json", {}) or {}
    experiment_ledger = hunt_intel.read_json(run_dir / "experiment_outcome_ledger.json", {}) or {}
    treatment_effects = hunt_intel.read_json(run_dir / "treatment_effects.json", {}) or {}
    treatment_confidence = hunt_intel.read_json(run_dir / "treatment_confidence_model.json", {}) or {}
    search_portfolio = hunt_intel.read_json(run_dir / "search_portfolio_manager.json", {}) or {}
    info_gain = hunt_intel.read_json(run_dir / "information_gain_scoring.json", {}) or {}
    debt_queue = hunt_intel.read_json(run_dir / "experiment_debt_queue.json", {}) or {}
    voi = hunt_intel.read_json(run_dir / "value_of_information_planner.json", {}) or {}
    evidence_gate = hunt_intel.read_json(run_dir / "evidence_sufficiency_gate.json", {}) or {}
    velocity = hunt_intel.read_json(run_dir / "learning_velocity_dashboard.json", {}) or {}
    truth_first = hunt_intel.read_json(run_dir / "truth_first_promotion_objective.json", {}) or {}
    self_audit = hunt_intel.read_json(run_dir / "self_audit_score.json", {}) or {}
    policy_exec = hunt_intel.read_json(run_dir / "policy_executor.json", {}) or {}
    policy_drift = hunt_intel.read_json(run_dir / "policy_drift_detector.json", {}) or {}
    concept_alarms = hunt_intel.read_json(run_dir / "concept_drift_alarms.json", {}) or {}
    revalidation = hunt_intel.read_json(run_dir / "revalidation_scheduler.json", {}) or {}
    temporal_ensemble = hunt_intel.read_json(run_dir / "temporal_ensemble_policy.json", {}) or {}
    experiment_governor = hunt_intel.read_json(run_dir / "active_experiment_governor.json", {}) or {}
    route_states = hunt_intel.read_json(run_dir / "route_state_machine.json", {}) or {}
    worker_rebalancer = hunt_intel.read_json(run_dir / "real_time_worker_rebalancer.json", {}) or {}
    replay_simulator = hunt_intel.read_json(run_dir / "hunt_replay_simulator.json", {}) or {}
    resurrection = hunt_intel.read_json(run_dir / "resurrection_engine.json", {}) or {}
    uncertainty_budget = hunt_intel.read_json(run_dir / "uncertainty_budgeting.json", {}) or {}
    pareto_frontier = hunt_intel.read_json(run_dir / "promotability_pareto_frontier.json", {}) or {}
    false_lessons = hunt_intel.read_json(run_dir / "false_lesson_detector.json", {}) or {}
    league = hunt_intel.read_json(run_dir / "self_competition_league.json", {}) or {}
    contradictions = hunt_intel.read_json(run_dir / "contradiction_detector.json", {}) or {}
    conflict_resolution = hunt_intel.read_json(run_dir / "learning_conflict_resolver.json", {}) or {}
    throttle = hunt_intel.read_json(run_dir / "adaptive_hunt_throttle.json", {}) or {}
    quality = hunt_intel.read_json(run_dir / "live_beater_quality_decomposer.json", {}) or {}
    runtime_kernel = hunt_intel.read_json(run_dir / "runtime_decision_kernel.json", {}) or {}
    guardrails = hunt_intel.read_json(run_dir / "runtime_guardrails.json", {}) or {}
    runtime_adapter = hunt_intel.read_json(run_dir / "runtime_command_adapter.json", {}) or online_state.get("runtime_command_adapter") or {}
    command_delta = hunt_intel.read_json(run_dir / "command_delta_status.json", {}) or online_state.get("command_delta_status") or {}
    outcome_backfill = hunt_intel.read_json(run_dir / "command_outcome_backfill.json", {}) or online_state.get("command_outcome_backfill") or {}
    reward_calibration = hunt_intel.read_json(run_dir / "action_reward_calibration.json", {}) or online_state.get("action_reward_calibration") or {}
    worker_contracts = hunt_intel.read_json(run_dir / "worker_job_contracts.json", {}) or online_state.get("worker_job_contracts") or {}
    ab_executor = hunt_intel.read_json(run_dir / "ab_route_experiment_executor.json", {}) or online_state.get("ab_route_experiment_executor") or {}
    champion_slots = hunt_intel.read_json(run_dir / "champion_challenger_runtime_slots.json", {}) or online_state.get("champion_challenger_runtime_slots") or {}
    adaptive_stopping = hunt_intel.read_json(run_dir / "adaptive_experiment_stopping.json", {}) or online_state.get("adaptive_experiment_stopping") or {}
    counterfactual_replay = hunt_intel.read_json(run_dir / "counterfactual_command_replay.json", {}) or online_state.get("counterfactual_command_replay") or {}
    contamination_guard = hunt_intel.read_json(run_dir / "experiment_contamination_guard.json", {}) or online_state.get("experiment_contamination_guard") or {}
    learning_rate = hunt_intel.read_json(run_dir / "learning_rate_controller.json", {}) or online_state.get("learning_rate_controller") or {}
    worker_cards = hunt_intel.read_json(run_dir / "worker_learning_report_cards.json", {}) or online_state.get("worker_learning_report_cards") or {}
    promotion_trace = hunt_intel.read_json(run_dir / "experiment_to_promotion_trace.json", {}) or online_state.get("experiment_to_promotion_trace") or {}
    zero_autopsy = hunt_intel.read_json(run_dir / "zero_yield_autopsy_engine.json", {}) or online_state.get("zero_yield_autopsy_engine") or {}
    stuck_breaker = hunt_intel.read_json(run_dir / "stuck_loop_breaker.json", {}) or online_state.get("stuck_loop_breaker") or {}
    opportunity_cost = hunt_intel.read_json(run_dir / "opportunity_cost_meter.json", {}) or online_state.get("opportunity_cost_meter") or {}
    coverage_map = hunt_intel.read_json(run_dir / "search_space_coverage_map.json", {}) or online_state.get("search_space_coverage_map") or {}
    scarcity_mode = hunt_intel.read_json(run_dir / "live_beater_scarcity_mode.json", {}) or online_state.get("live_beater_scarcity_mode") or {}
    alias_traps = hunt_intel.read_json(run_dir / "alias_trap_detector.json", {}) or online_state.get("alias_trap_detector") or {}
    seed_quality = hunt_intel.read_json(run_dir / "route_seed_quality_score.json", {}) or online_state.get("route_seed_quality_score") or {}
    recovery_playbook = hunt_intel.read_json(run_dir / "recovery_playbook_generator.json", {}) or online_state.get("recovery_playbook_generator") or {}
    pre_hunt_strategy = hunt_intel.read_json(run_dir / "pre_hunt_strategy_selector.json", {}) or online_state.get("pre_hunt_strategy_selector") or {}
    memory_reliability = hunt_intel.read_json(run_dir / "memory_reliability_scorer.json", {}) or online_state.get("memory_reliability_scorer") or {}
    falsification_queue = hunt_intel.read_json(run_dir / "memory_falsification_queue.json", {}) or online_state.get("memory_falsification_queue") or {}
    belief_retirement = hunt_intel.read_json(run_dir / "belief_retirement_engine.json", {}) or online_state.get("belief_retirement_engine") or {}
    memory_provenance = hunt_intel.read_json(run_dir / "memory_provenance_explorer.json", {}) or online_state.get("memory_provenance_explorer") or {}
    historical_disagreement = hunt_intel.read_json(run_dir / "current_vs_historical_disagreement_monitor.json", {}) or online_state.get("current_vs_historical_disagreement_monitor") or {}
    stress_pack = hunt_intel.read_json(run_dir / "memory_stress_test_pack.json", {}) or online_state.get("memory_stress_test_pack") or {}
    compressed_memory = hunt_intel.read_json(run_dir / "durable_memory_compression.json", {}) or online_state.get("durable_memory_compression") or {}
    memory_qa = hunt_intel.read_json(run_dir / "memory_qa_smoke_test.json", {}) or online_state.get("memory_qa_smoke_test") or {}
    hypothesis_factory = hunt_intel.read_json(run_dir / "hypothesis_factory.json", {}) or online_state.get("hypothesis_factory") or {}
    hypothesis_market = hunt_intel.read_json(run_dir / "hypothesis_market_maker.json", {}) or online_state.get("hypothesis_market_maker") or {}
    bet_sizer = hunt_intel.read_json(run_dir / "real_time_bet_sizer.json", {}) or online_state.get("real_time_bet_sizer") or {}
    contrarian = hunt_intel.read_json(run_dir / "contrarian_generator.json", {}) or online_state.get("contrarian_generator") or {}
    stop_loss = hunt_intel.read_json(run_dir / "learning_stop_loss.json", {}) or online_state.get("learning_stop_loss") or {}
    breakthroughs = hunt_intel.read_json(run_dir / "breakthrough_detector.json", {}) or online_state.get("breakthrough_detector") or {}
    recipes = hunt_intel.read_json(run_dir / "pattern_to_recipe_compiler.json", {}) or online_state.get("pattern_to_recipe_compiler") or {}
    narrative = hunt_intel.read_json(run_dir / "hunt_narrative_memory.json", {}) or online_state.get("hunt_narrative_memory") or {}
    attention = hunt_intel.read_json(run_dir / "attention_ledger.json", {}) or online_state.get("attention_ledger") or {}
    wasted_spend = hunt_intel.read_json(run_dir / "wasted_spend_autopsy.json", {}) or online_state.get("wasted_spend_autopsy") or {}
    yield_curve = hunt_intel.read_json(run_dir / "marginal_yield_curve.json", {}) or online_state.get("marginal_yield_curve") or {}
    regret = hunt_intel.read_json(run_dir / "explore_exploit_regret_tracker.json", {}) or online_state.get("explore_exploit_regret_tracker") or {}
    worker_alpha = hunt_intel.read_json(run_dir / "worker_alpha_attribution.json", {}) or online_state.get("worker_alpha_attribution") or {}
    budget_reallocator = hunt_intel.read_json(run_dir / "budget_reallocator.json", {}) or online_state.get("budget_reallocator") or {}
    time_plan = hunt_intel.read_json(run_dir / "time_aware_hunt_plan.json", {}) or online_state.get("time_aware_hunt_plan") or {}
    spend_narrative = hunt_intel.read_json(run_dir / "spend_efficiency_narrative.json", {}) or online_state.get("spend_efficiency_narrative") or {}
    idea_novelty = hunt_intel.read_json(run_dir / "idea_novelty_ledger.json", {}) or online_state.get("idea_novelty_ledger") or {}
    idea_saturation = hunt_intel.read_json(run_dir / "idea_saturation_detector.json", {}) or online_state.get("idea_saturation_detector") or {}
    creative_leap = hunt_intel.read_json(run_dir / "creative_leap_scorer.json", {}) or online_state.get("creative_leap_scorer") or {}
    imagination_autopsy = hunt_intel.read_json(run_dir / "failed_imagination_autopsy.json", {}) or online_state.get("failed_imagination_autopsy") or {}
    grammar_gaps = hunt_intel.read_json(run_dir / "mutation_grammar_gap_finder.json", {}) or online_state.get("mutation_grammar_gap_finder") or {}
    novelty_governor = hunt_intel.read_json(run_dir / "novelty_budget_governor.json", {}) or online_state.get("novelty_budget_governor") or {}
    idea_lineage = hunt_intel.read_json(run_dir / "idea_lineage_map.json", {}) or online_state.get("idea_lineage_map") or {}
    creative_brief = hunt_intel.read_json(run_dir / "creative_brief_compiler.json", {}) or online_state.get("creative_brief_compiler") or {}
    module_registry = hunt_intel.read_json(run_dir / "learning_module_registry.json", {}) or online_state.get("learning_module_registry") or {}
    module_attribution = hunt_intel.read_json(run_dir / "module_contribution_attribution.json", {}) or online_state.get("module_contribution_attribution") or {}
    module_conflicts = hunt_intel.read_json(run_dir / "module_conflict_detector.json", {}) or online_state.get("module_conflict_detector") or {}
    module_reliability = hunt_intel.read_json(run_dir / "module_reliability_scorer.json", {}) or online_state.get("module_reliability_scorer") or {}
    module_ablation = hunt_intel.read_json(run_dir / "module_ablation_planner.json", {}) or online_state.get("module_ablation_planner") or {}
    module_budget = hunt_intel.read_json(run_dir / "module_budget_governor.json", {}) or online_state.get("module_budget_governor") or {}
    self_audit = hunt_intel.read_json(run_dir / "learning_system_self_audit.json", {}) or online_state.get("learning_system_self_audit") or {}
    meta_brief = hunt_intel.read_json(run_dir / "meta_learning_brief.json", {}) or online_state.get("meta_learning_brief") or {}
    causal_scheduler = hunt_intel.read_json(run_dir / "causal_intervention_scheduler.json", {}) or online_state.get("causal_intervention_scheduler") or {}
    power_calculator = hunt_intel.read_json(run_dir / "experiment_power_calculator.json", {}) or online_state.get("experiment_power_calculator") or {}
    fragility_profiler = hunt_intel.read_json(run_dir / "winner_fragility_profiler.json", {}) or online_state.get("winner_fragility_profiler") or {}
    source_attribution = hunt_intel.read_json(run_dir / "live_beater_source_attribution.json", {}) or online_state.get("live_beater_source_attribution") or {}
    temperature_controller = hunt_intel.read_json(run_dir / "adaptive_search_temperature_controller.json", {}) or online_state.get("adaptive_search_temperature_controller") or {}
    route_interactions = hunt_intel.read_json(run_dir / "route_interaction_learner.json", {}) or online_state.get("route_interaction_learner") or {}
    false_firewall = hunt_intel.read_json(run_dir / "false_discovery_firewall.json", {}) or online_state.get("false_discovery_firewall") or {}
    strategy_v2 = hunt_intel.read_json(run_dir / "hunt_strategy_compiler_v2.json", {}) or online_state.get("hunt_strategy_compiler_v2") or {}
    lesson_survival = hunt_intel.read_json(run_dir / "lesson_survival_tracker.json", {}) or online_state.get("lesson_survival_tracker") or {}
    rejection_backprop = hunt_intel.read_json(run_dir / "promotion_rejection_backpropagation.json", {}) or online_state.get("promotion_rejection_backpropagation") or {}
    lesson_decay = hunt_intel.read_json(run_dir / "lesson_decay_model.json", {}) or online_state.get("lesson_decay_model") or {}
    causal_memory = hunt_intel.read_json(run_dir / "cross_hunt_causal_memory.json", {}) or online_state.get("cross_hunt_causal_memory") or {}
    evidence_chain = hunt_intel.read_json(run_dir / "evidence_chain_ledger.json", {}) or online_state.get("evidence_chain_ledger") or {}
    disagreement_court = hunt_intel.read_json(run_dir / "learning_disagreement_court.json", {}) or online_state.get("learning_disagreement_court") or {}
    promotion_objective = hunt_intel.read_json(run_dir / "promotion_aware_search_objective.json", {}) or online_state.get("promotion_aware_search_objective") or {}
    scientific_brief = hunt_intel.read_json(run_dir / "scientific_run_brief_v2.json", {}) or online_state.get("scientific_run_brief_v2") or {}
    strategy_registry = hunt_intel.read_json(run_dir / "strategy_genome_registry.json", {}) or online_state.get("strategy_genome_registry") or {}
    strategy_mutations = hunt_intel.read_json(run_dir / "strategy_mutation_engine.json", {}) or online_state.get("strategy_mutation_engine") or {}
    strategy_tournament = hunt_intel.read_json(run_dir / "strategy_tournament_memory.json", {}) or online_state.get("strategy_tournament_memory") or {}
    strategy_selector = hunt_intel.read_json(run_dir / "regime_conditioned_strategy_selector.json", {}) or online_state.get("regime_conditioned_strategy_selector") or {}
    meta_objective = hunt_intel.read_json(run_dir / "meta_objective_optimizer.json", {}) or online_state.get("meta_objective_optimizer") or {}
    exploration_debt = hunt_intel.read_json(run_dir / "exploration_debt_ledger.json", {}) or online_state.get("exploration_debt_ledger") or {}
    strategy_red_team = hunt_intel.read_json(run_dir / "adversarial_strategy_red_team.json", {}) or online_state.get("adversarial_strategy_red_team") or {}
    pivot_governor = hunt_intel.read_json(run_dir / "autonomous_pivot_governor.json", {}) or online_state.get("autonomous_pivot_governor") or {}
    learning_roi = hunt_intel.read_json(run_dir / "learning_roi_ledger.json", {}) or online_state.get("learning_roi_ledger") or {}
    usefulness_pruner = hunt_intel.read_json(run_dir / "artifact_usefulness_pruner.json", {}) or online_state.get("artifact_usefulness_pruner") or {}
    decision_trace = hunt_intel.read_json(run_dir / "decision_trace_explainer.json", {}) or online_state.get("decision_trace_explainer") or {}
    control_conflicts = hunt_intel.read_json(run_dir / "control_surface_conflict_auditor.json", {}) or online_state.get("control_surface_conflict_auditor") or {}
    control_simplifier = hunt_intel.read_json(run_dir / "runtime_control_simplifier.json", {}) or online_state.get("runtime_control_simplifier") or {}
    cost_meter = hunt_intel.read_json(run_dir / "learning_cost_meter.json", {}) or online_state.get("learning_cost_meter") or {}
    ablation_replay = hunt_intel.read_json(run_dir / "ablation_replay_harness.json", {}) or online_state.get("ablation_replay_harness") or {}
    fitness_brief = hunt_intel.read_json(run_dir / "architecture_fitness_brief.json", {}) or online_state.get("architecture_fitness_brief") or {}
    schema_registry = hunt_intel.read_json(run_dir / "learning_artifact_schema_registry.json", {}) or online_state.get("learning_artifact_schema_registry") or {}
    dependency_graph = hunt_intel.read_json(run_dir / "artifact_dependency_graph.json", {}) or online_state.get("artifact_dependency_graph") or {}
    incremental_cache = hunt_intel.read_json(run_dir / "incremental_learning_cache.json", {}) or online_state.get("incremental_learning_cache") or {}
    dashboard_feed = hunt_intel.read_json(run_dir / "live_learning_dashboard_feed.json", {}) or online_state.get("live_learning_dashboard_feed") or {}
    runbook = hunt_intel.read_json(run_dir / "hunt_runbook_compiler.json", {}) or online_state.get("hunt_runbook_compiler") or {}
    failure_sentinel = hunt_intel.read_json(run_dir / "learning_failure_sentinel.json", {}) or online_state.get("learning_failure_sentinel") or {}
    artifact_warehouse = hunt_intel.read_json(run_dir / "cross_run_artifact_warehouse.json", {}) or online_state.get("cross_run_artifact_warehouse") or {}
    readiness_gate = hunt_intel.read_json(run_dir / "pre_hunt_readiness_gate.json", {}) or online_state.get("pre_hunt_readiness_gate") or {}
    causal_bandit = hunt_intel.read_json(run_dir / "online_causal_bandit.json", {}) or online_state.get("online_causal_bandit") or {}
    dna_attribution = hunt_intel.read_json(run_dir / "variant_dna_attribution.json", {}) or online_state.get("variant_dna_attribution") or {}
    gene_suppression = hunt_intel.read_json(run_dir / "negative_gene_suppression.json", {}) or online_state.get("negative_gene_suppression") or {}
    family_tree = hunt_intel.read_json(run_dir / "live_winner_family_tree.json", {}) or online_state.get("live_winner_family_tree") or {}
    frontier_map = hunt_intel.read_json(run_dir / "exploration_frontier_map.json", {}) or online_state.get("exploration_frontier_map") or {}
    worker_personalities = hunt_intel.read_json(run_dir / "adaptive_worker_personalities.json", {}) or online_state.get("adaptive_worker_personalities") or {}
    cycle_delta = hunt_intel.read_json(run_dir / "cycle_level_learning_delta.json", {}) or online_state.get("cycle_level_learning_delta") or {}
    rejection_v2 = hunt_intel.read_json(run_dir / "promotion_rejection_predictor_v2.json", {}) or online_state.get("promotion_rejection_predictor_v2") or {}
    counterfactual_sim = hunt_intel.read_json(run_dir / "counterfactual_hunt_simulator.json", {}) or online_state.get("counterfactual_hunt_simulator") or {}
    missed_winners = hunt_intel.read_json(run_dir / "missed_winner_detector.json", {}) or online_state.get("missed_winner_detector") or {}
    regret_ledger = hunt_intel.read_json(run_dir / "causal_regret_ledger.json", {}) or online_state.get("causal_regret_ledger") or {}
    grammar_generator = hunt_intel.read_json(run_dir / "adaptive_search_grammar_generator.json", {}) or online_state.get("adaptive_search_grammar_generator") or {}
    hypothesis_court = hunt_intel.read_json(run_dir / "live_hypothesis_kill_scale_court.json", {}) or online_state.get("live_hypothesis_kill_scale_court") or {}
    interaction_v2 = hunt_intel.read_json(run_dir / "route_interaction_matrix_v2.json", {}) or online_state.get("route_interaction_matrix_v2") or {}
    shadow_scoring = hunt_intel.read_json(run_dir / "promotion_survival_shadow_scoring.json", {}) or online_state.get("promotion_survival_shadow_scoring") or {}
    autopilot = hunt_intel.read_json(run_dir / "hunt_autopilot_policy_compiler.json", {}) or online_state.get("hunt_autopilot_policy_compiler") or {}
    claim_verifier = hunt_intel.read_json(run_dir / "learning_claim_verifier.json", {}) or online_state.get("learning_claim_verifier") or {}
    confidence_calibration = hunt_intel.read_json(run_dir / "causal_confidence_calibration.json", {}) or online_state.get("causal_confidence_calibration") or {}
    false_warning = hunt_intel.read_json(run_dir / "false_discovery_early_warning.json", {}) or online_state.get("false_discovery_early_warning") or {}
    evidence_thresholds = hunt_intel.read_json(run_dir / "adaptive_evidence_thresholds.json", {}) or online_state.get("adaptive_evidence_thresholds") or {}
    debate_council = hunt_intel.read_json(run_dir / "self_debate_search_council.json", {}) or online_state.get("self_debate_search_council") or {}
    memory_compression = hunt_intel.read_json(run_dir / "experiment_memory_compression.json", {}) or online_state.get("experiment_memory_compression") or {}
    drift_monitor = hunt_intel.read_json(run_dir / "learning_drift_monitor.json", {}) or online_state.get("learning_drift_monitor") or {}
    promotion_first = hunt_intel.read_json(run_dir / "promotion_first_autopilot_v2.json", {}) or online_state.get("promotion_first_autopilot_v2") or {}
    horizon_memory = hunt_intel.read_json(run_dir / "multi_horizon_memory_stack.json", {}) or online_state.get("multi_horizon_memory_stack") or {}
    half_life_v2 = hunt_intel.read_json(run_dir / "lesson_half_life_engine_v2.json", {}) or online_state.get("lesson_half_life_engine_v2") or {}
    strategy_replay = hunt_intel.read_json(run_dir / "cross_hunt_strategy_replay.json", {}) or online_state.get("cross_hunt_strategy_replay") or {}
    regime_fingerprint = hunt_intel.read_json(run_dir / "temporal_regime_fingerprinting.json", {}) or online_state.get("temporal_regime_fingerprinting") or {}
    longitudinal_survival = hunt_intel.read_json(run_dir / "longitudinal_promotion_survival_model.json", {}) or online_state.get("longitudinal_promotion_survival_model") or {}
    conflict_court_v2 = hunt_intel.read_json(run_dir / "memory_conflict_court_v2.json", {}) or online_state.get("memory_conflict_court_v2") or {}
    strategy_aging = hunt_intel.read_json(run_dir / "strategy_aging_dashboard.json", {}) or online_state.get("strategy_aging_dashboard") or {}
    next_opening = hunt_intel.read_json(run_dir / "next_hunt_opening_policy_compiler.json", {}) or online_state.get("next_hunt_opening_policy_compiler") or {}
    question_planner = hunt_intel.read_json(run_dir / "question_driven_hunt_planner.json", {}) or online_state.get("question_driven_hunt_planner") or {}
    eig_v2 = hunt_intel.read_json(run_dir / "expected_information_gain_scorer_v2.json", {}) or online_state.get("expected_information_gain_scorer_v2") or {}
    heatmap = hunt_intel.read_json(run_dir / "uncertainty_heatmap.json", {}) or online_state.get("uncertainty_heatmap") or {}
    sequencer = hunt_intel.read_json(run_dir / "adaptive_experiment_sequencer.json", {}) or online_state.get("adaptive_experiment_sequencer") or {}
    learning_value_stop = hunt_intel.read_json(run_dir / "learning_value_stop_loss.json", {}) or online_state.get("learning_value_stop_loss") or {}
    causal_ledger = hunt_intel.read_json(run_dir / "causal_question_ledger.json", {}) or online_state.get("causal_question_ledger") or {}
    epistemic_roles = hunt_intel.read_json(run_dir / "worker_epistemic_roles_v2.json", {}) or online_state.get("worker_epistemic_roles_v2") or {}
    hypothesis_compiler = hunt_intel.read_json(run_dir / "hunt_hypothesis_compiler.json", {}) or online_state.get("hunt_hypothesis_compiler") or {}
    experiment_contracts = hunt_intel.read_json(run_dir / "experiment_contract_compiler.json", {}) or online_state.get("experiment_contract_compiler") or {}
    control_matcher = hunt_intel.read_json(run_dir / "control_route_matcher.json", {}) or online_state.get("control_route_matcher") or {}
    sequential_monitor = hunt_intel.read_json(run_dir / "sequential_test_monitor.json", {}) or online_state.get("sequential_test_monitor") or {}
    effect_ledger = hunt_intel.read_json(run_dir / "causal_effect_size_ledger.json", {}) or online_state.get("causal_effect_size_ledger") or {}
    false_pressure = hunt_intel.read_json(run_dir / "false_positive_pressure_gauge.json", {}) or online_state.get("false_positive_pressure_gauge") or {}
    debt_paydown = hunt_intel.read_json(run_dir / "exploration_debt_paydown_planner.json", {}) or online_state.get("exploration_debt_paydown_planner") or {}
    promotion_power = hunt_intel.read_json(run_dir / "promotion_aware_power_planner.json", {}) or online_state.get("promotion_aware_power_planner") or {}
    scientific_exec = hunt_intel.read_json(run_dir / "scientific_hunt_executive.json", {}) or online_state.get("scientific_hunt_executive") or {}
    live_evidence = hunt_intel.read_json(run_dir / "live_candidate_evidence_builder.json", {}) or online_state.get("live_candidate_evidence_builder") or {}
    failure_v3 = hunt_intel.read_json(run_dir / "promotion_failure_predictor_v3.json", {}) or online_state.get("promotion_failure_predictor_v3") or {}
    gap_router = hunt_intel.read_json(run_dir / "evidence_gap_router.json", {}) or online_state.get("evidence_gap_router") or {}
    review_queue_v2 = hunt_intel.read_json(run_dir / "review_ready_queue_v2.json", {}) or online_state.get("review_ready_queue_v2") or {}
    evidence_scorecard = hunt_intel.read_json(run_dir / "promotion_evidence_scorecard.json", {}) or online_state.get("promotion_evidence_scorecard") or {}
    lineage_v2 = hunt_intel.read_json(run_dir / "candidate_lineage_explainer_v2.json", {}) or online_state.get("candidate_lineage_explainer_v2") or {}
    control_diff = hunt_intel.read_json(run_dir / "live_vs_control_differential_report.json", {}) or online_state.get("live_vs_control_differential_report") or {}
    packet_exec = hunt_intel.read_json(run_dir / "promotion_packet_executive.json", {}) or online_state.get("promotion_packet_executive") or {}
    unified_learning = hunt_intel.read_json(run_dir / "unified_learning_state_reducer.json", {}) or online_state.get("unified_learning_state_reducer") or {}
    learning_budget = hunt_intel.read_json(run_dir / "learning_budget_optimizer.json", {}) or online_state.get("learning_budget_optimizer") or {}
    novelty_floor = hunt_intel.read_json(run_dir / "search_novelty_floor.json", {}) or online_state.get("search_novelty_floor") or {}
    backpressure_v2 = hunt_intel.read_json(run_dir / "false_discovery_backpressure_controller.json", {}) or online_state.get("false_discovery_backpressure_controller") or {}
    repair_recipes_v2 = hunt_intel.read_json(run_dir / "candidate_repair_recipe_generator.json", {}) or online_state.get("candidate_repair_recipe_generator") or {}
    world_dashboard = hunt_intel.read_json(run_dir / "world_state_dashboard_artifact.json", {}) or online_state.get("world_state_dashboard_artifact") or {}
    meta_governor = hunt_intel.read_json(run_dir / "meta_learning_governor.json", {}) or online_state.get("meta_learning_governor") or {}
    elite_summary = hunt_intel.read_json(run_dir / "elite_learning_system_summary.json", {}) or online_state.get("elite_learning_system_summary") or {}
    next_question = hunt_intel.read_json(run_dir / "world_state_next_best_question_engine.json", {}) or online_state.get("world_state_next_best_question_engine") or {}
    proof_summary = hunt_intel.read_json(run_dir / "proof_learning_system_summary.json", {}) or online_state.get("proof_learning_system_summary") or {}
    truth_ledger = hunt_intel.read_json(run_dir / "learning_truth_ledger.json", {}) or online_state.get("learning_truth_ledger") or {}
    control_summary = hunt_intel.read_json(run_dir / "closed_loop_control_learning_summary.json", {}) or online_state.get("closed_loop_control_learning_summary") or {}
    bayesian_belief = hunt_intel.read_json(run_dir / "bayesian_belief_engine.json", {}) or online_state.get("bayesian_belief_engine") or {}
    experiment_selector = hunt_intel.read_json(run_dir / "active_experiment_selector.json", {}) or online_state.get("active_experiment_selector") or {}
    realtime_learning_feed = hunt_intel.read_json(run_dir / "real_time_learning_dashboard_feed.json", {}) or online_state.get("real_time_learning_dashboard_feed") or {}
    nervous_summary = hunt_intel.read_json(run_dir / "world_model_nervous_system_summary.json", {}) or online_state.get("world_model_nervous_system_summary") or {}
    nervous_audit = hunt_intel.read_json(run_dir / "world_model_self_audit_loop.json", {}) or online_state.get("world_model_self_audit_loop") or {}
    orchestration_summary = hunt_intel.read_json(run_dir / "orchestration_learning_summary.json", {}) or online_state.get("orchestration_learning_summary") or {}
    orchestration_exec = hunt_intel.read_json(run_dir / "hunt_executive_controller.json", {}) or online_state.get("hunt_executive_controller") or {}
    fitness_summary = hunt_intel.read_json(run_dir / "fitness_selection_summary.json", {}) or online_state.get("fitness_selection_summary") or {}
    fitness_report = hunt_intel.read_json(run_dir / "world_model_fitness_report.json", {}) or online_state.get("world_model_fitness_report") or {}
    cold_start_pack = hunt_intel.read_json(run_dir / "cold_start_route_pack_generator.json", {}) or online_state.get("cold_start_route_pack_generator") or {}
    memory_conflicts = hunt_intel.read_json(run_dir / "memory_conflict_arbiter.json", {}) or online_state.get("memory_conflict_arbiter") or {}
    opening_playbook = hunt_intel.read_json(run_dir / "hunt_opening_playbook.json", {}) or online_state.get("hunt_opening_playbook") or {}
    cross_run_regression = hunt_intel.read_json(run_dir / "cross_run_learning_regression_test.json", {}) or online_state.get("cross_run_learning_regression_test") or {}
    planner = hunt_intel.read_json(run_dir / "autonomous_hunt_planner.json", {}) or {}
    action_league = hunt_intel.read_json(run_dir / "action_elo_league.json", {}) or {}
    hunt_brief = hunt_intel.read_json(run_dir / "human_readable_hunt_brief.json", {}) or {}
    active_experiment = ((experiment_plan.get("experiments") or [None])[0] or {}) if isinstance(experiment_plan, dict) else {}
    outcomes = list(experiment_ledger.get("outcomes") or []) if isinstance(experiment_ledger, dict) else []
    current_experiment_conclusion = ((outcomes[0] or {}).get("conclusion") if outcomes else "") or ""
    next_lane = max(lane_pressure, key=lane_pressure.get) if lane_pressure else ((directives[0] or {}).get("lane") if directives else "")
    if not next_lane and active_experiment:
        treatments = active_experiment.get("treatments") or []
        next_lane = ((treatments[0] or {}).get("mutation_lane") if treatments else "") or str(active_experiment.get("kind") or "")
    rank1_risk = {}
    if rank1:
        tags = list(rank1.get("learning_tags") or [])
        risk_tags = [tag for tag in tags if tag in {
            "thin_holdout_edge",
            "no_holdout_credit",
            "small_total_edge",
            "weak_day_consistency",
            "ticker_concentrated",
            "side_concentrated",
            "thin_sample",
            "high_overfit_risk",
            "route_narrow",
        } or str(tag).startswith("robustness_flag:")]
        rank1_risk = {
            "promotion_readiness_score": rank1.get("promotion_readiness_score"),
            "risk_tags": risk_tags,
            "digest": ", ".join(risk_tags[:4]) if risk_tags else "no obvious proxy risk tags",
        }
    chat_summary = _compact_status_line(
        phase=phase,
        cycle_idx=cycle_idx,
        hunter=hunter,
        top_count=len(top),
        rank1=rank1,
        promotion_rank1=promotion_rank1,
        next_lane=next_lane,
        remaining_sec=remaining_sec,
    )
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "event": "hunt_status",
        "phase": phase,
        "updated_at_ct": _now_ct(),
        "run_dir": str(run_dir.resolve()),
        "cycle": cycle_idx,
        "hunter": hunter,
        "started_at_ct": started_at,
        "remaining_sec": remaining_sec,
        "top100_count": len(top),
        "chat_summary": chat_summary,
        "rank1": rank1,
        "rank1_compact": _status_candidate(rank1),
        "promotion_survival_rank1": promotion_rank1,
        "promotion_survival_rank1_compact": _status_candidate(promotion_rank1),
        "promotion_readiness_rank1": readiness_rank1,
        "promotion_readiness_rank1_compact": _status_candidate(readiness_rank1),
        "rank1_risk": rank1_risk,
        "micro_promotion_review": {
            "cycle_count": micro_review.get("cycle_count"),
            "candidate_count": micro_review.get("candidate_count"),
            "promotion_ready_count": ((micro_review.get("promotion_ready_queue") or {}).get("queue_count")
                                      if isinstance(micro_review.get("promotion_ready_queue"), dict) else None),
            "directives": directives[:10],
        },
        "next_learning_shift": {
            "lane": next_lane or "standard_rotation",
            "lane_pressure": lane_pressure,
            "active_experiment": {
                "experiment_id": active_experiment.get("experiment_id"),
                "kind": active_experiment.get("kind"),
                "hypothesis": active_experiment.get("hypothesis"),
                "worker_assignment": active_experiment.get("worker_assignment"),
            } if active_experiment else {},
            "experiment_outcome_count": experiment_ledger.get("outcome_count"),
            "current_experiment_conclusion": current_experiment_conclusion,
            "best_treatment": treatment_effects.get("best_treatment"),
            "worst_treatment": treatment_effects.get("worst_treatment"),
            "best_treatment_conclusion": treatment_effects.get("best_conclusion"),
            "worst_treatment_conclusion": treatment_effects.get("worst_conclusion"),
            "treatment_stop_rules": {
                "scale": [row.get("treatment") for row in (treatment_confidence.get("scale") or [])[:5]],
                "probe": [row.get("treatment") for row in (treatment_confidence.get("probe") or [])[:5]],
                "abandon": [row.get("treatment") for row in (treatment_confidence.get("abandon") or [])[:5]],
            },
            "search_portfolio": (search_portfolio.get("allocations") or [])[:4],
            "top_information_gain_action": ((info_gain.get("actions") or [{}])[0] or {}),
            "top_experiment_debt": ((debt_queue.get("queue") or [{}])[0] or {}),
            "top_value_of_information_plan": ((voi.get("plans") or [{}])[0] or {}),
            "evidence_gate": {
                "scale_allowed": [row.get("treatment") for row in (evidence_gate.get("scale_allowed") or [])],
                "abandon_allowed": [row.get("treatment") for row in (evidence_gate.get("abandon_allowed") or [])],
            },
            "learning_velocity_score": velocity.get("learning_velocity_score"),
            "self_audit_score": self_audit.get("self_audit_score"),
            "truth_first_rank1": ((truth_first.get("leaderboard") or [{}])[0] or {}),
            "next_policy_assignment": ((policy_exec.get("assignments") or [{}])[0] or {}),
            "policy_drift_status": policy_drift.get("status"),
            "policy_drift_reasons": policy_drift.get("reasons") or [],
            "concept_alarm_count": concept_alarms.get("alarm_count") or len(concept_alarms.get("alarms") or []),
            "next_revalidation": ((revalidation.get("queue") or revalidation.get("tasks") or [{}])[0] or {}),
            "temporal_ensemble_primary": temporal_ensemble.get("primary_policy"),
            "temporal_ensemble_components": (temporal_ensemble.get("components") or temporal_ensemble.get("ensemble") or [])[:4],
            "experiment_governor_top_decision": experiment_governor.get("top_decision") or {},
            "route_state_counts": route_states.get("state_counts") or {},
            "route_lifecycle_focus": route_states.get("focus_routes") or [],
            "worker_rebalance": (worker_rebalancer.get("assignments") or [])[:4],
            "replay_recommended_policy": replay_simulator.get("recommended_policy"),
            "top_resurrection": resurrection.get("top_resurrection") or {},
            "uncertainty_top_budget": uncertainty_budget.get("top_budget") or {},
            "pareto_frontier_count": pareto_frontier.get("frontier_count"),
            "false_lesson_highest_risk": false_lessons.get("highest_risk") or {},
            "self_competition_champion": league.get("champion") or {},
            "contradiction_count": contradictions.get("contradiction_count"),
            "top_conflict_resolution": conflict_resolution.get("top_action") or {},
            "adaptive_throttle": throttle.get("controls") or {},
            "top_quality_decomposition": quality.get("top_quality") or {},
            "runtime_command_count": len((runtime_kernel.get("commands") or [])),
            "runtime_guardrails_ok": guardrails.get("ok"),
            "runtime_degrade_mode": runtime_adapter.get("degrade_mode"),
            "runtime_adapter": {
                "mutation_width": runtime_adapter.get("mutation_width"),
                "batch_size_multiplier": runtime_adapter.get("batch_size_multiplier"),
                "focus_routes": (runtime_adapter.get("focus_routes") or [])[:6],
                "avoid_routes": (runtime_adapter.get("avoid_routes") or [])[:6],
                "primary_command": runtime_adapter.get("primary_command") or {},
            },
            "command_delta": command_delta,
            "command_outcome_count": outcome_backfill.get("outcome_count"),
            "reward_calibration_mae": reward_calibration.get("mean_abs_error"),
            "worker_contract_count": worker_contracts.get("contract_count"),
            "ab_experiment_assignment_count": ab_executor.get("assignment_count"),
            "champion_challenger_slots": (champion_slots.get("slots") or [])[:4],
            "adaptive_experiment_stop_count": adaptive_stopping.get("stop_count"),
            "best_counterfactual_command": counterfactual_replay.get("best_counterfactual") or {},
            "experiment_contamination_ok": contamination_guard.get("ok"),
            "learning_rate_mode": learning_rate.get("mode"),
            "top_worker_report_card": worker_cards.get("top_worker") or {},
            "top_experiment_promotion_trace": promotion_trace.get("top_trace") or {},
            "zero_yield_count": zero_autopsy.get("zero_yield_count"),
            "top_zero_yield_autopsy": zero_autopsy.get("top_autopsy") or {},
            "stuck_reset_count": stuck_breaker.get("reset_count"),
            "highest_opportunity_cost": opportunity_cost.get("highest_cost") or {},
            "coverage_blind_spots": (coverage_map.get("blind_spots") or [])[:6],
            "live_beater_scarcity_mode": scarcity_mode.get("mode"),
            "alias_trap_count": alias_traps.get("trap_count"),
            "top_route_seed_quality": seed_quality.get("top_seed") or {},
            "top_recovery_intervention": recovery_playbook.get("top_intervention") or {},
            "pre_hunt_strategy": pre_hunt_strategy.get("strategy"),
            "memory_reliability_top": ((memory_reliability.get("scores") or [{}])[0] or {}),
            "memory_falsification_count": len(falsification_queue.get("queue") or []),
            "belief_retire_routes": belief_retirement.get("retire_routes") or [],
            "memory_provenance_subject_count": len(memory_provenance.get("entries") or []),
            "historical_disagreement_count": historical_disagreement.get("disagreement_count"),
            "memory_stress_task_count": len(stress_pack.get("tasks") or []),
            "durable_memory_rule_count": compressed_memory.get("rule_count"),
            "memory_qa_passed": memory_qa.get("passed"),
            "top_hypothesis": hypothesis_factory.get("top_hypothesis") or {},
            "top_hypothesis_quote": ((hypothesis_market.get("quotes") or [{}])[0] or {}),
            "hypothesis_bet_allocations": (bet_sizer.get("allocations") or [])[:4],
            "top_contrarian_experiment": ((contrarian.get("experiments") or [{}])[0] or {}),
            "learning_stop_loss_count": len(stop_loss.get("stops") or []),
            "top_breakthrough": breakthroughs.get("top_breakthrough") or {},
            "top_recipe": recipes.get("top_recipe") or {},
            "hunt_narrative": narrative.get("summary"),
            "attention_total_spend": attention.get("total_spend_units"),
            "top_wasted_spend": wasted_spend.get("top_waste") or {},
            "plateau_routes": yield_curve.get("plateau_routes") or [],
            "attention_regret": regret.get("recommendation"),
            "top_worker_alpha": worker_alpha.get("top_worker") or {},
            "budget_reallocation": {
                "focus_routes": budget_reallocator.get("focus_routes") or [],
                "avoid_routes": budget_reallocator.get("avoid_routes") or [],
                "recommendation": budget_reallocator.get("recommendation"),
            },
            "time_aware_phase": time_plan.get("phase"),
            "spend_efficiency": spend_narrative.get("summary"),
            "fresh_idea_count": len(idea_novelty.get("fresh_ideas") or []),
            "idea_saturation_count": idea_saturation.get("saturation_count"),
            "top_creative_leap": ((creative_leap.get("leaps") or [{}])[0] or {}),
            "failed_imagination_count": len(imagination_autopsy.get("autopsies") or []),
            "top_mutation_gap": grammar_gaps.get("top_gap") or {},
            "novelty_budget_pct": novelty_governor.get("novelty_budget_pct"),
            "idea_lineage_nodes": idea_lineage.get("node_count"),
            "creative_brief": creative_brief.get("summary"),
            "active_learning_modules": module_registry.get("active_modules") or [],
            "top_module_contribution": module_attribution.get("top_module") or {},
            "module_conflict_count": module_conflicts.get("conflict_count"),
            "top_module_reliability": ((module_reliability.get("scores") or [{}])[0] or {}),
            "module_ablation_count": module_ablation.get("test_count"),
            "module_budget_top": module_budget.get("top_module") or {},
            "learning_system_health": self_audit.get("health_score"),
            "meta_learning_brief": meta_brief.get("summary"),
            "top_causal_intervention": causal_scheduler.get("top_intervention") or {},
            "experiment_power_score": power_calculator.get("overall_power_score"),
            "underpowered_experiment_count": len(power_calculator.get("underpowered_experiments") or []),
            "top_winner_fragility": fragility_profiler.get("top_fragility") or {},
            "top_live_beater_source": source_attribution.get("top_source") or {},
            "search_temperature": temperature_controller.get("temperature"),
            "top_route_interaction": route_interactions.get("top_interaction") or {},
            "false_discovery_flag_count": false_firewall.get("flag_count"),
            "hunt_strategy_v2": strategy_v2.get("strategy"),
            "hunt_strategy_v2_summary": strategy_v2.get("summary"),
            "top_surviving_lesson": lesson_survival.get("top_lesson") or {},
            "promotion_rejection_backprop_count": rejection_backprop.get("rejection_count"),
            "lesson_decay_top": lesson_decay.get("top_lesson") or {},
            "cross_hunt_causal_record_count": causal_memory.get("record_count"),
            "top_evidence_chain": evidence_chain.get("top_chain") or {},
            "learning_court_case_count": disagreement_court.get("case_count"),
            "promotion_aware_top_candidate": promotion_objective.get("top_candidate") or {},
            "scientific_run_brief": scientific_brief.get("summary"),
            "strategy_genome_champion": strategy_registry.get("champion") or {},
            "top_strategy_challenger": strategy_mutations.get("top_challenger") or {},
            "strategy_tournament_champion": strategy_tournament.get("champion") or {},
            "selected_strategy_regime": strategy_selector.get("regime"),
            "meta_objective": meta_objective.get("objective"),
            "exploration_debt_count": exploration_debt.get("debt_count"),
            "strategy_red_team_top_attack": strategy_red_team.get("top_attack") or {},
            "autonomous_pivot": pivot_governor.get("pivot"),
            "autonomous_pivot_reason": pivot_governor.get("pivot_reason"),
            "top_learning_roi": learning_roi.get("top_roi") or {},
            "prune_candidate_count": len(usefulness_pruner.get("prune_candidates") or []),
            "decision_trace_count": decision_trace.get("trace_count"),
            "control_conflict_count": control_conflicts.get("conflict_count"),
            "simplified_runtime_width": control_simplifier.get("mutation_width"),
            "learning_cost_highest": cost_meter.get("highest_cost") or {},
            "ablation_replay_count": ablation_replay.get("test_count"),
            "architecture_fitness_verdict": fitness_brief.get("verdict"),
            "architecture_fitness_summary": fitness_brief.get("summary"),
            "learning_ops": {
                "dashboard_status": dashboard_feed.get("status"),
                "schema_valid": schema_registry.get("valid"),
                "schema_failure_count": schema_registry.get("failure_count"),
                "dirty_artifacts": dependency_graph.get("dirty_artifacts") or [],
                "cache_reuse_count": incremental_cache.get("reuse_count"),
                "cache_refresh_count": incremental_cache.get("refresh_count"),
                "runbook_worker_count": runbook.get("worker_count"),
                "sentinel_ok": failure_sentinel.get("ok"),
                "sentinel_failures": failure_sentinel.get("failures") or [],
                "warehouse_export_count": artifact_warehouse.get("export_count"),
                "pre_hunt_ready": readiness_gate.get("ready"),
                "readiness_failures": readiness_gate.get("failures") or [],
            },
            "real_time_causal_search": {
                "top_lane": causal_bandit.get("top_lane"),
                "allocations": (causal_bandit.get("allocations") or [])[:4],
                "top_gene": dna_attribution.get("top_gene") or {},
                "suppressed_gene_count": gene_suppression.get("suppressed_count"),
                "avoid_routes": gene_suppression.get("avoid_routes") or [],
                "champion_family": family_tree.get("champion_family") or {},
                "frontier_routes": frontier_map.get("focus_routes") or [],
                "worker_personalities": (worker_personalities.get("assignments") or [])[:4],
                "cycle_delta": cycle_delta.get("summary"),
                "promotion_rejection_highest_risk": rejection_v2.get("highest_risk") or {},
            },
            "counterfactual_opportunity": {
                "top_counterfactual": counterfactual_sim.get("top_counterfactual") or {},
                "missed_winner_count": missed_winners.get("missed_count"),
                "top_regret": regret_ledger.get("top_regret") or {},
                "top_grammar_templates": (grammar_generator.get("top_templates") or [])[:4],
                "hypothesis_court": {
                    "scale": hypothesis_court.get("scale_count"),
                    "kill": hypothesis_court.get("kill_count"),
                    "retest": hypothesis_court.get("retest_count"),
                },
                "top_route_interaction": interaction_v2.get("top_interaction") or {},
                "top_promotion_shadow": shadow_scoring.get("top_shadow") or {},
                "autopilot_policy": autopilot.get("policy_packet") or {},
            },
            "truth_maintenance": {
                "verified_claims": claim_verifier.get("verified_count"),
                "weak_claims": claim_verifier.get("weak_claim_count"),
                "top_causal_confidence": confidence_calibration.get("top_confidence") or {},
                "false_discovery_warning_count": false_warning.get("warning_count"),
                "evidence_threshold_mode": evidence_thresholds.get("mode"),
                "debate_resolution": debate_council.get("resolution") or {},
                "compressed_rule_count": memory_compression.get("rule_count"),
                "drift_count": drift_monitor.get("drift_count"),
                "promotion_first_policy": promotion_first.get("policy_packet") or {},
            },
            "temporal_memory": {
                "horizon_summary": horizon_memory.get("summary") or {},
                "top_half_life_lesson": half_life_v2.get("top_lesson") or {},
                "top_replay": strategy_replay.get("top_replay") or {},
                "top_regime": regime_fingerprint.get("top_fingerprint") or {},
                "top_survival": longitudinal_survival.get("top_survival_feature") or {},
                "conflict_case_count": conflict_court_v2.get("case_count"),
                "strategy_aging_counts": strategy_aging.get("aging_counts") or {},
                "next_opening_policy": next_opening.get("policy_packet") or {},
            },
            "active_uncertainty_learning": {
                "top_question": question_planner.get("top_question") or {},
                "top_information_gain": eig_v2.get("top_score") or {},
                "top_uncertainty_cell": heatmap.get("top_cell") or {},
                "next_sequence_step": sequencer.get("next_step") or {},
                "learning_stop_count": learning_value_stop.get("stop_count"),
                "open_causal_questions": causal_ledger.get("open_question_count"),
                "epistemic_roles": (epistemic_roles.get("assignments") or [])[:4],
                "top_hypothesis": hypothesis_compiler.get("top_hypothesis") or {},
            },
            "closed_loop_science": {
                "top_contract": experiment_contracts.get("top_contract") or {},
                "top_control": control_matcher.get("top_match") or {},
                "top_sequential_decision": sequential_monitor.get("top_decision") or {},
                "top_effect": effect_ledger.get("top_effect") or {},
                "false_positive_pressure": {
                    "score": false_pressure.get("pressure_score"),
                    "mode": false_pressure.get("mode"),
                },
                "top_paydown": debt_paydown.get("top_plan") or {},
                "top_power_plan": promotion_power.get("top_plan") or {},
                "executive": scientific_exec.get("summary") or {},
                "top_command": scientific_exec.get("top_command") or {},
            },
            "promotion_grade_evidence": {
                "top_evidence_packet": live_evidence.get("top_packet") or {},
                "highest_failure_risk": failure_v3.get("highest_risk") or {},
                "top_gap_task": gap_router.get("top_task") or {},
                "top_review_ready": review_queue_v2.get("top_ready") or {},
                "top_scorecard": evidence_scorecard.get("top_scorecard") or {},
                "top_lineage": lineage_v2.get("top_explanation") or {},
                "top_live_control_diff": control_diff.get("top_report") or {},
                "executive_summary": packet_exec.get("summary") or {},
                "top_packet_decision": packet_exec.get("top_decision") or {},
            },
            "world_class_meta_learning": {
                "unified_summary": unified_learning.get("summary") or {},
                "top_budget": learning_budget.get("top_allocation") or {},
                "novelty_floor": novelty_floor.get("summary") or {},
                "backpressure": {
                    "score": backpressure_v2.get("pressure_score"),
                    "mode": backpressure_v2.get("mode"),
                },
                "top_repair_recipe": repair_recipes_v2.get("top_recipe") or {},
                "world_state": world_dashboard.get("summary") or {},
                "governor": meta_governor.get("summary") or {},
                "elite_summary": elite_summary.get("summary") or {},
                "next_best_question": next_question.get("top_record") or {},
                "proof_summary": proof_summary.get("summary") or {},
                "truth_top_claim": truth_ledger.get("top_record") or {},
                "closed_loop_control_summary": control_summary.get("summary") or {},
                "bayesian_top_belief": bayesian_belief.get("top_record") or {},
                "active_experiment_next": experiment_selector.get("top_record") or {},
                "learning_status_feed": realtime_learning_feed.get("top_record") or {},
                "world_model_nervous_system_summary": nervous_summary.get("summary") or {},
                "world_model_self_audit": nervous_audit.get("top_record") or {},
                "orchestration_summary": orchestration_summary.get("summary") or {},
                "orchestration_hunt_exec": orchestration_exec.get("top_record") or {},
                "fitness_summary": fitness_summary.get("summary") or {},
                "fitness_report": fitness_report.get("top_record") or {},
            },
            "cold_start_focus_routes": (cold_start_pack.get("focus_routes") or [])[:8],
            "memory_conflict_count": memory_conflicts.get("conflict_count"),
            "opening_playbook": {
                "strategy": opening_playbook.get("strategy"),
                "mutation_width": opening_playbook.get("mutation_width"),
                "validation_cadence": opening_playbook.get("validation_cadence"),
            },
            "cross_run_regression_passed": cross_run_regression.get("passed"),
            "next_runtime_job": planner.get("next_job") or {},
            "action_league_champion": action_league.get("champion") or {},
            "hunt_brief": hunt_brief.get("brief"),
        },
        "ranking_stats": {k: rankings.get(k) for k in (
            "input_rows",
            "live_filtered_rows",
            "config_unique_rows",
            "behavior_unique_rows",
        )},
        "route_clusters": hunt_intel.route_clusters(top, limit=5),
        "telemetry_summary": telemetry_summary or {},
        "status_policy": _status_policy(args),
    }


def _emit_status_update(args: argparse.Namespace, run_dir: Path, payload: dict[str, Any]) -> None:
    printable = payload
    if str(getattr(args, "status_verbosity", "compact") or "compact") == "compact":
        next_shift = payload.get("next_learning_shift") if isinstance(payload.get("next_learning_shift"), dict) else {}
        learning_ops = next_shift.get("learning_ops") if isinstance(next_shift.get("learning_ops"), dict) else {}
        rank1 = payload.get("rank1") if isinstance(payload.get("rank1"), dict) else {}
        ops = {
            "schema_valid": learning_ops.get("schema_valid"),
            "sentinel_ok": learning_ops.get("sentinel_ok"),
            "pre_hunt_ready": learning_ops.get("pre_hunt_ready"),
        }
        telemetry = payload.get("telemetry_summary") if isinstance(payload.get("telemetry_summary"), dict) else {}
        no_telemetry_yet = int(telemetry.get("events") or 0) <= 0
        if (payload.get("phase") in {"hunt_started", "cycle_started"} or (payload.get("phase") == "cycle_running" and no_telemetry_yet)) and not rank1:
            ops = {key: ("pending" if value is False or value is None else value) for key, value in ops.items()}
        printable = {
            "event": payload.get("event"),
            "phase": payload.get("phase"),
            "cycle": payload.get("cycle"),
            "hunter": payload.get("hunter"),
            "chat_summary": str(payload.get("chat_summary") or "")[:700],
            "top100_count": payload.get("top100_count"),
            "rank1_compact": payload.get("rank1_compact") or {},
            "promotion_survival_rank1_compact": payload.get("promotion_survival_rank1_compact") or {},
            "promotion_readiness_rank1_compact": payload.get("promotion_readiness_rank1_compact") or {},
            "rank1": {
                "variant": rank1.get("variant"),
                "step2_pnl": rank1.get("step2_pnl"),
                "delta_vs_active": rank1.get("delta_vs_active") or rank1.get("step2_delta_vs_active"),
                "promotion_readiness_score": rank1.get("promotion_readiness_score"),
            } if rank1 else {},
            "learning_ops": ops,
            "telemetry_summary": payload.get("telemetry_summary") or {},
            "status_policy": payload.get("status_policy") or {},
        }
    event_payload = printable
    if str(getattr(args, "status_event_verbosity", "digest") or "digest") == "full":
        event_payload = payload
    else:
        event_payload = {
            **printable,
            "full_status_hash": _payload_hash(payload),
            "full_status_available_in": "status_full_snapshot events when --status-event-verbosity full is used",
        }
    step2_online_learning.append_event(run_dir, "hunt_status", event_payload)
    stdout_mode = str(getattr(args, "status_stdout_mode", "all") or "all").lower()
    if stdout_mode == "none":
        return
    if stdout_mode == "final" and payload.get("phase") != "hunt_finished":
        return
    print(json.dumps(printable, sort_keys=True, default=str), flush=True)


def _review_candidate_pool(rankings: dict[str, Any], limit: int) -> list[dict[str, Any]]:
    pools = [
        rankings.get("raw_leaderboard") or [],
        rankings.get("promotion_readiness_leaderboard") or [],
        rankings.get("promotion_quality_leaderboard") or [],
        rankings.get("novelty_leaderboard") or [],
    ]
    rows: list[dict[str, Any]] = []
    seen = set()
    for pool in pools:
        for row in list(pool)[: max(1, int(limit))]:
            key = str(row.get("behavior_key") or row.get("config_key") or row.get("variant") or "")
            if not key or key in seen:
                continue
            seen.add(key)
            rows.append(row)
            if len(rows) >= int(limit):
                return rows
    return rows


def _review_feedback_from_brief(brief: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
    decision = brief.get("decision") if isinstance(brief.get("decision"), dict) else {}
    candidate = brief.get("candidate") if isinstance(brief.get("candidate"), dict) else {}
    robustness = brief.get("robustness") if isinstance(brief.get("robustness"), dict) else {}
    metrics = brief.get("metrics") if isinstance(brief.get("metrics"), dict) else {}
    label = str(decision.get("label") or "REVIEW").lower()
    reject_reasons = list(decision.get("reject_reasons") or [])
    details = list(decision.get("reject_reason_details") or [])
    if label == "reject" and not reject_reasons:
        reject_reasons = ["review_rejected"]
    return {
        "variant": candidate.get("variant") or row.get("variant"),
        "behavior_key": row.get("behavior_key") or hunt_intel.behavior_key(row),
        "config_key": row.get("config_key") or hunt_intel.config_key(row),
        "family_key": row.get("family_key") or hunt_intel.candidate_family_key(row),
        "lineage": row.get("lineage") or hunt_intel.lineage_info(row),
        "route_key": hunt_intel.route_key_from_row(row),
        "status": label,
        "decision": decision.get("label"),
        "reject_reasons": reject_reasons,
        "reject_reason_details": details,
        "plain_english": brief.get("plain_english"),
        "step2_pnl": metrics.get("step2_pnl") or hunt_intel.pnl(row),
        "delta_vs_active": metrics.get("delta_vs_active") or hunt_intel.live_delta(row),
        "robustness_score": robustness.get("score"),
        "robustness_adjusted_score": robustness.get("adjusted_score"),
        "promotion_readiness_score": row.get("promotion_readiness_score"),
        "learning_tags": row.get("learning_tags") or hunt_intel.candidate_learning_tags(row),
        "weights": row.get("weights") or {},
        "bias": row.get("bias"),
        "routes": row.get("routes") or [],
    }


def _review_directive(feedback: dict[str, Any]) -> dict[str, Any]:
    route = str(feedback.get("route_key") or "unrouted")
    reasons = {str(reason) for reason in (feedback.get("reject_reasons") or [])}
    tags = {str(tag) for tag in (feedback.get("learning_tags") or [])}
    action = "continue"
    lane = "robustness_first"
    if reasons & {"reproducibility_failed", "reproducibility_mismatch"}:
        action = "skip_route"
        lane = "reproducibility_repair"
    elif reasons & {"route_safety_failed", "route_contract_failed"}:
        action = "narrow_route"
        lane = "route_safety_repair"
    elif "robustness_score_below_70" in reasons and ("thin_holdout_edge" in tags or "no_holdout_credit" in tags):
        action = "repair_holdout"
        lane = "holdout_repair"
    elif "robustness_score_below_70" in reasons or "weak_day_consistency" in tags:
        action = "repair_day_consistency"
        lane = "day_consistency_repair"
    elif "small_total_edge" in tags:
        action = "widen_route"
        lane = "edge_expansion"
    if str(feedback.get("status") or "").lower() in {"approve", "approved", "pass"}:
        action = "promote_to_full_review"
        lane = "promotion_ready"
    return {
        "route_key": route,
        "variant": feedback.get("variant"),
        "action": action,
        "lane": lane,
        "reasons": sorted(reasons),
        "learning_tags": sorted(tags),
        "plain_english": feedback.get("plain_english"),
    }


def _promotion_ready_queue(reviews: list[dict[str, Any]]) -> dict[str, Any]:
    ready = []
    for item in reviews:
        feedback = item.get("feedback") if isinstance(item.get("feedback"), dict) else {}
        decision = str(feedback.get("decision") or feedback.get("status") or "").lower()
        readiness = float(feedback.get("promotion_readiness_score") or 0.0)
        if decision in {"approve", "approved", "pass"} or readiness >= 75.0 and not feedback.get("reject_reasons"):
            ready.append(feedback)
    ready.sort(key=lambda row: (float(row.get("promotion_readiness_score") or 0.0), float(row.get("step2_pnl") or 0.0)), reverse=True)
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "description": "Candidates that passed lightweight in-loop review and should receive full promotion review.",
        "queue_count": len(ready),
        "queue": ready[:50],
    }


def _run_micro_promotion_review(
    args: argparse.Namespace,
    run_dir: Path,
    rankings: dict[str, Any],
    cycles: list[dict[str, Any]],
    *,
    force: bool = False,
) -> dict[str, Any]:
    if not args.micro_promotion_review:
        return {"skipped": True, "reason": "disabled"}
    cycle_count = len(cycles)
    if not force and cycle_count > 0 and cycle_count % int(args.micro_promotion_every_cycles) != 0:
        existing = hunt_intel.read_json(run_dir / "promotion_micro_reviews.json", {}) or {}
        return existing or {"skipped": True, "reason": "not_scheduled"}
    candidates = _review_candidate_pool(rankings, int(args.micro_promotion_limit))
    if not candidates:
        payload = {
            "schema_version": 1,
            "source": "run_step2_three_hour_hunt",
            "skipped": True,
            "reason": "no_candidates",
            "cycle_count": cycle_count,
        }
        _write_json(run_dir / "promotion_micro_reviews.json", payload)
        return payload
    review_payload = {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt_micro_review_payload",
        "start_balance": float(args.start_balance),
        "active": rankings.get("active_row") or {},
        "active_step2_pnl": rankings.get("active_step2_pnl"),
        "leaderboard": candidates,
        "rows": candidates,
    }
    review_payload_path = Path(_write_json(run_dir / "promotion_micro_review_payload.json", review_payload))
    reviews = []
    for idx, row in enumerate(candidates, 1):
        try:
            brief = candidate_decision_brief.build(
                payload=review_payload,
                rank=idx,
                variant=str(row.get("variant") or ""),
                candidate_json=str(review_payload_path),
                run_reproducibility=False,
                run_quarantine=False,
            )
            feedback = _review_feedback_from_brief(brief, row)
            reviews.append({
                "rank": idx,
                "variant": row.get("variant"),
                "ok": True,
                "decision": (brief.get("decision") or {}).get("label"),
                "plain_english": brief.get("plain_english"),
                "feedback": feedback,
                "directive": _review_directive(feedback),
                "brief": brief,
            })
        except Exception as exc:
            reviews.append({
                "rank": idx,
                "variant": row.get("variant"),
                "ok": False,
                "error": repr(exc),
                "feedback": {
                    "variant": row.get("variant"),
                    "route_key": hunt_intel.route_key_from_row(row),
                    "status": "review_error",
                    "reject_reasons": ["micro_promotion_review_error"],
                    "error": repr(exc),
                },
            })
    feedback_rows = [item.get("feedback") for item in reviews if isinstance(item.get("feedback"), dict)]
    directives = [item.get("directive") for item in reviews if isinstance(item.get("directive"), dict)]
    payload = {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "created_at_ct": _now_ct(),
        "cycle_count": cycle_count,
        "review_mode": "lightweight_no_repro_no_quarantine",
        "candidate_count": len(candidates),
        "reviews": reviews,
        "feedback": feedback_rows,
        "directives": directives,
        "promotion_ready_queue": _promotion_ready_queue(reviews),
    }
    _write_json(run_dir / "promotion_micro_reviews.json", payload)
    _write_json(run_dir / "promotion_review_feedback.json", {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "feedback": feedback_rows,
    })
    _write_json(run_dir / "promotion_ready_queue.json", payload["promotion_ready_queue"])
    step2_online_learning.append_event(run_dir, "micro_promotion_review_completed", {
        "cycle_count": cycle_count,
        "candidate_count": len(candidates),
        "feedback_count": len(feedback_rows),
        "directives": directives[:10],
    })
    return payload


def _persist_learning_db(args: argparse.Namespace, run_dir: Path) -> dict[str, Any]:
    if not args.learning_db:
        return {"skipped": True}
    guard = hunt_intel.read_json(run_dir / "tiny_run_calibration_guard.json", {}) or {}
    if guard.get("is_tiny_diagnostic") and not bool(getattr(args, "persist_tiny_run_learning", False)):
        return {
            "skipped": True,
            "reason": "tiny_diagnostic_run_not_persisted",
            "tiny_run_calibration_guard": guard,
        }
    db = step2_learning_db.Step2LearningDB(args.learning_db)
    try:
        payload = db.ingest_run_dir(run_dir)
        if args.feedback_json:
            payload["feedback_ingest"] = db.ingest_feedback_file(args.feedback_json)
        review_feedback_path = run_dir / "promotion_review_feedback.json"
        if review_feedback_path.exists():
            payload["micro_promotion_feedback_ingest"] = db.ingest_feedback_file(review_feedback_path)
        payload["validation_ingest"] = []
        for path in args.validation_results_json or []:
            payload["validation_ingest"].append(db.ingest_validation_results(path))
        payload["cross_run_route_memory"] = db.cross_run_route_memory(limit=20)
        payload["route_prior_model"] = db.route_prior_model(limit=20)
        payload["adaptive_mutation_controller"] = db.adaptive_mutation_controller(limit=20)
        payload["drift_detection"] = db.drift_detection(limit=20)
        payload["regime_aware_bandit"] = db.regime_aware_bandit(limit=50)
        payload["causal_experiment_registry"] = db.causal_experiment_registry(limit=50)
        payload["experiment_debt_queue"] = db.experiment_debt_queue(limit=50)
        payload["meta_strategy_learner"] = db.meta_strategy_learner(limit=50)
        payload["calibration_persistence"] = db.calibration_persistence(limit=50)
        payload["temporal_decay_memory"] = db.temporal_decay_memory(limit=50)
        payload["revalidation_candidates"] = db.revalidation_candidates(limit=50)
        payload["research_lab_memory"] = db.research_lab_memory(limit=50)
        payload["self_correction_memory"] = db.self_correction_memory(limit=50)
        payload["pressure_science_memory"] = db.pressure_science_memory(limit=50)
        payload["closed_loop_execution_memory"] = db.closed_loop_execution_memory(limit=50)
        payload["outcome_reconciliation"] = db.outcome_reconciliation(limit=50)
        global_memory = db.global_learning_memory(limit=50)
        payload["global_learning_memory"] = global_memory
        _write_json(run_dir / "global_learning_memory.json", global_memory)
        packet = db.next_experiment_packet(limit=20)
        payload["next_experiment_packet"] = packet
        _write_json(run_dir / "next_experiment_packet.json", packet)
        return payload
    finally:
        db.close()


def _cycle_command(
    args: argparse.Namespace,
    run_dir: Path,
    cycle_idx: int,
    seed: int,
    hunter: str,
    online_state: dict[str, Any] | None = None,
) -> list[str]:
    cycle_name = f"{args.name}_cycle_{cycle_idx:04d}"
    cmd = [
        sys.executable,
        str(HERE / "step2_hunt_coordinator.py"),
        "--name",
        cycle_name,
        "--out-dir",
        str(run_dir),
        "--hunters",
        hunter,
        "--batch-size",
        str(args.batch_size),
        "--max-batches",
        str(args.max_batches),
        "--target-count",
        str(args.target_count),
        "--beat-pct",
        str(args.beat_pct),
        "--target-pnl",
        str(args.target_pnl),
        "--start-balance",
        str(args.start_balance),
        "--seed",
        str(seed),
        "--leaderboard-limit",
        str(args.leaderboard_limit),
        "--hunter-timeout-sec",
        str(max(1, int(args.cycle_timeout_sec))),
        "--coordinator-artifact-mode",
        str(getattr(args, "coordinator_artifact_mode", "minimal") or "minimal"),
        "--score-cache-stats-mode",
        str(getattr(args, "score_cache_stats_mode", "fast") or "fast"),
        *_seed_json_args(args.seed_json),
    ]
    if not args.live_only:
        cmd.append("--include-non-live")
    if not args.behavioral_dedupe:
        cmd.append("--config-dedupe-only")
    plan_path = run_dir / "next_hunt_plan.json"
    if hunter == "router" and plan_path.exists():
        cmd.extend(["--focus-plan-json", str(plan_path.resolve())])
    if hunter == "router":
        repair_directives = hunt_intel.read_json(run_dir / "promotion_repair_worker_directives.json", {}) or {}
        hunt_mode = _effective_hunt_mode(args, run_dir)
        base_scale = step2_online_learning.mutation_scale_for_route(online_state or {})
        scale = base_scale
        scale_reason = "online_state"
        controls = _promotion_repair_runtime_controls(run_dir) if hunt_mode in {"promotion_repair", "repair_exploration"} else {}
        if hunt_mode == "promotion_repair" and controls.get("focus_routes"):
            scale *= float(getattr(args, "repair_tighten_scale", 0.72) or 0.72)
            scale_reason = "promotion_repair_tightening"
            _write_json(run_dir / "promotion_repair_hunt_controls.json", controls)
            _merge_quarantine_controls(run_dir, controls)
        elif hunt_mode == "repair_exploration" and controls.get("focus_routes"):
            scale *= float(getattr(args, "repair_loosen_scale", 0.90) or 0.90)
            scale_reason = "repair_exploration_loosened"
            controls["hunt_mode"] = "repair_exploration"
            _write_json(run_dir / "promotion_repair_hunt_controls.json", controls)
            _merge_quarantine_controls(run_dir, controls)
        elif hunt_mode == "promotion_repair":
            hunt_mode = "discovery"
        elif hunt_mode == "weird_exploration":
            scale *= 1.35
            scale_reason = "weird_exploration_expansion"
        if hunt_mode == "discovery" and scale < 0.95:
            scale = 0.95
            scale_reason = "discovery_floor_after_audit"
        _write_json(run_dir / "mutation_scale_audit.json", {
            "schema_version": 1,
            "source": "run_step2_three_hour_hunt",
            "hunt_mode": hunt_mode,
            "base_scale": base_scale,
            "final_scale": scale,
            "reason": scale_reason,
            "repair_yield_floor_pct": float(getattr(args, "repair_yield_floor_pct", 1.0) or 1.0),
            "repair_tighten_scale": float(getattr(args, "repair_tighten_scale", 0.72) or 0.72),
            "repair_loosen_scale": float(getattr(args, "repair_loosen_scale", 0.90) or 0.90),
        })
        cmd.extend(["--mutation-scale-multiplier", str(scale)])
        for route in list(getattr(args, "focus_route", []) or [])[: int(args.max_focus_routes)]:
            route = str(route or "")
            if route:
                cmd.extend(["--focus-route", route])
        if args.online_learning:
            cmd.extend(["--online-state-json", str((run_dir / "online_state.json").resolve())])
            cmd.extend(["--stream-telemetry-jsonl", str((run_dir / "streaming_telemetry.jsonl").resolve())])
            cmd.extend(["--stop-signal-json", str((run_dir / "stop_signal.json").resolve())])
            cmd.extend(["--quarantine-json", str((run_dir / "quarantine.json").resolve())])
            batch_mult = step2_online_learning.batch_size_multiplier(online_state or {})
            tuned_batch = max(1, int(round(int(args.batch_size) * batch_mult)))
            min_batch = max(1, int(args.min_online_batch_size))
            max_batch = int(args.max_online_batch_size or max(int(args.batch_size), tuned_batch))
            max_batch = max(min_batch, max_batch)
            cmd.extend(["--min-batch-size", str(min_batch)])
            cmd.extend(["--max-batch-size", str(max_batch)])
        allocation = hunt_intel.read_json(run_dir / "bandit_allocation.json", {}) or {}
        for arm in (allocation.get("allocation") or [])[: int(args.max_focus_routes)]:
            route = str(arm.get("route_key") or "")
            if route and route != "exploration":
                cmd.extend(["--focus-route", route])
        if args.online_learning:
            for route in step2_online_learning.focus_routes_from_state(online_state or {}, limit=int(args.max_focus_routes)):
                cmd.extend(["--focus-route", route])
            for route in list(repair_directives.get("focus_routes") or [])[: int(args.max_focus_routes)]:
                route = str(route or "")
                if route:
                    cmd.extend(["--focus-route", route])
            if hunt_mode in {"promotion_repair", "repair_exploration"}:
                controls = hunt_intel.read_json(run_dir / "promotion_repair_hunt_controls.json", {}) or {}
                for route in list(controls.get("focus_routes") or [])[: int(args.max_focus_routes)]:
                    route = str(route or "")
                    if route:
                        cmd.extend(["--focus-route", route])
            policy_exec = online_state.get("policy_executor") if isinstance(online_state.get("policy_executor"), dict) else {}
            for route in list(policy_exec.get("focus_routes") or [])[: int(args.max_focus_routes)]:
                route = str(route or "")
                if route:
                    cmd.extend(["--focus-route", route])
    if bool(args.exact_variant_count):
        cmd.append("--exact-variant-count")
    if str(args.runtime_control_mode or "enforce") != "enforce":
        cmd.extend(["--runtime-control-mode", str(args.runtime_control_mode)])
    if bool(getattr(args, "allow_uncertified_cache", False)):
        cmd.append("--allow-uncertified-cache")
        route_crowding = hunt_intel.read_json(run_dir / "route_crowding_report.json", {}) or {}
        do_not = hunt_intel.read_json(run_dir / "do_not_explore_controls.json", {}) or {}
        anti_alias = hunt_intel.read_json(run_dir / "anti_alias_pressure.json", {}) or {}
        if anti_alias.get("pressure") in {"medium", "high"} and anti_alias.get("structural_mutation_routes"):
            _merge_quarantine_controls(run_dir, {
                "schema_version": 1,
                "source": "anti_alias_pressure",
                "structural_mutation_routes": list(anti_alias.get("structural_mutation_routes") or [])[:12],
                "runtime_command_adapter": {
                    "structural_mutation_routes": list(anti_alias.get("structural_mutation_routes") or [])[:12],
                    "focus_routes": list(anti_alias.get("structural_mutation_routes") or [])[: int(args.max_focus_routes)],
                },
            })
        avoid_routes = set(str(route) for route in (route_crowding.get("avoid_routes") or []) + (do_not.get("avoid_routes") or []) if str(route))
        cmd = _normalize_focus_route_args(cmd, avoid_routes=avoid_routes, limit=int(args.max_focus_routes))
    return cmd


def _normalize_focus_route_args(cmd: list[str], *, avoid_routes: set[str], limit: int) -> list[str]:
    out = []
    focus = []
    idx = 0
    while idx < len(cmd):
        if cmd[idx] == "--focus-route" and idx + 1 < len(cmd):
            route = str(cmd[idx + 1] or "")
            if route and route not in avoid_routes and route not in focus:
                focus.append(route)
            idx += 2
            continue
        out.append(cmd[idx])
        idx += 1
    for route in focus[: max(1, limit)]:
        out.extend(["--focus-route", route])
    return out


def _persist_online_event(args: argparse.Namespace, run_dir: Path, event: dict[str, Any]) -> None:
    if not args.learning_db:
        return
    db = step2_learning_db.Step2LearningDB(args.learning_db)
    try:
        db.record_online_event(run_dir.name, event)
    finally:
        db.close()


def _record_cycle_db(args: argparse.Namespace, run_dir: Path, cycle: dict[str, Any]) -> None:
    if not args.learning_db:
        return
    db = step2_learning_db.Step2LearningDB(args.learning_db)
    try:
        db.record_cycle(run_dir.name, cycle)
    finally:
        db.close()


def _effective_hunt_mode(args: argparse.Namespace, run_dir: Path) -> str:
    requested = str(getattr(args, "hunt_mode", "auto") or "auto")
    if requested != "auto":
        return requested
    repair_regression = hunt_intel.read_json(run_dir / "repair_regression_detector.json", {}) or {}
    if repair_regression.get("active") or repair_regression.get("soft_active"):
        return "repair_exploration"
    recommendation = hunt_intel.read_json(run_dir / "auto_hunt_recommendation.json", {}) or {}
    if recommendation.get("next_mode"):
        return str(recommendation.get("next_mode"))
    repair_queue = hunt_intel.read_json(run_dir / "promotion_evidence_repair_queue.json", {}) or {}
    quality_floor = hunt_intel.read_json(run_dir / "live_beater_quality_floor_top100.json", {}) or {}
    if int(repair_queue.get("task_count") or 0) > 0 and int(quality_floor.get("kept_count") or 0) == 0:
        return "promotion_repair"
    if int(repair_queue.get("task_count") or 0) >= 3:
        return "promotion_repair"
    return "discovery"


def _promotion_repair_runtime_controls(run_dir: Path) -> dict[str, Any]:
    directives = hunt_intel.read_json(run_dir / "promotion_repair_worker_directives.json", {}) or {}
    route_budget_caps = hunt_intel.read_json(run_dir / "repair_route_budget_caps.json", {}) or {}
    route_admission = hunt_intel.read_json(run_dir / "route_admission_gate.json", {}) or {}
    repair_regression = hunt_intel.read_json(run_dir / "repair_regression_detector.json", {}) or {}
    behavior_controls = hunt_intel.read_json(run_dir / "behavior_unique_generation_controls.json", {}) or {}
    lane_allocation = hunt_intel.read_json(run_dir / "repair_allocation_by_evidence_weakness.json", {}) or {}
    if not lane_allocation:
        lanes = hunt_intel.read_json(run_dir / "evidence_repair_lanes.json", {}) or {}
        if lanes:
            lane_allocation = _repair_allocation_by_evidence_weakness(lanes, route_budget_caps)
            _write_json(run_dir / "repair_allocation_by_evidence_weakness.json", lane_allocation)
    near_queue = hunt_intel.read_json(run_dir / "near_promotion_evidence_queue.json", {}) or {}
    candidate_by_route = {
        str(row.get("route_key") or ""): row
        for row in (near_queue.get("queue") or [])
        if isinstance(row, dict) and row.get("route_key")
    }
    holdout = []
    day = []
    widen = []
    focus = []
    commands = []
    seen_commands: set[tuple[str, str]] = set()
    for directive in directives.get("directives") or []:
        if not isinstance(directive, dict):
            continue
        route = str(directive.get("route_key") or "")
        if not route:
            continue
        focus.append(route)
        gaps = set(str(gap) for gap in directive.get("gaps") or [])
        action = str(directive.get("action") or "")
        if "holdout_evidence" in gaps or action == "run_holdout_day_split_repair":
            holdout.append(route)
        if "day_consistency" in gaps or action == "run_holdout_day_split_repair":
            day.append(route)
        if "route_breadth" in gaps or action == "expand_route_breadth_controlled":
            widen.append(route)
            widen.extend(_sibling_routes(route))
        if "holdout_evidence" in gaps and "route_breadth" in gaps:
            widen.extend(_sibling_routes(route)[:3])
        command = {
            "route_key": route,
            "action": "repair",
            "mutation_width": directive.get("mutation_width") or "tight",
            "source": "promotion_repair_worker_directives",
            "target_failures": directive.get("target_failures") or [],
        }
        candidate = candidate_by_route.get(route) or {}
        if candidate:
            distance = candidate.get("promotion_distance") if isinstance(candidate.get("promotion_distance"), dict) else {}
            command["holdout_preservation_floor"] = candidate.get("holdout_delta_vs_active")
            command["robustness_gap_to_floor"] = _distance_for_gate(distance, "robustness_minimum_floor")
            command["readiness_gap_to_floor"] = _distance_for_gate(distance, "promotion_readiness_floor")
            command["next_best_repair"] = distance.get("next_best_repair")
        cap = _route_cap_for(route_budget_caps, route)
        if cap:
            command["budget_cap"] = cap
        command_key = (route, str(command["mutation_width"]))
        if command_key not in seen_commands:
            seen_commands.add(command_key)
            commands.append(command)
    capped_routes = [str(row.get("route_key") or "") for row in route_budget_caps.get("route_caps") or [] if row.get("cap_required")]
    uncapped_focus = [route for route in focus if route not in set(capped_routes)]
    if route_budget_caps.get("tiny_route_pool_pressure"):
        focus.extend(widen)
        uncapped_focus.extend([route for route in widen if route not in set(capped_routes)])
    preserved_focus = [str(route) for route in (route_admission.get("preserved_focus_routes") or []) if str(route)]
    throttle_routes = [str(route) for route in (route_admission.get("throttle_routes") or []) if str(route)]
    if preserved_focus:
        focus = list(dict.fromkeys(preserved_focus + focus))
        uncapped_focus = list(dict.fromkeys(preserved_focus + uncapped_focus))
    if repair_regression.get("active") or repair_regression.get("soft_active"):
        widen.extend([sibling for route in preserved_focus[:4] for sibling in _sibling_routes(route)[:3]])
    throttle_multipliers = route_admission.get("throttle_multipliers") if isinstance(route_admission.get("throttle_multipliers"), dict) else {}
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "hunt_mode": "promotion_repair",
        "focus_routes": list(dict.fromkeys(focus))[:12],
        "uncapped_focus_routes": list(dict.fromkeys(uncapped_focus + [route for route in focus if route in set(capped_routes)]))[:12],
        "capped_routes": list(dict.fromkeys(capped_routes))[:12],
        "holdout_repair_routes": list(dict.fromkeys(holdout))[:12],
        "day_consistency_repair_routes": list(dict.fromkeys(day))[:12],
        "widen_routes": list(dict.fromkeys(widen))[:12],
        "sibling_expansion_routes": list(dict.fromkeys(route for route in widen if route not in set(focus)))[:12],
        "route_admission_gate": route_admission,
        "repair_regression_detector": repair_regression,
        "throttle_routes": throttle_routes,
        "route_budget_caps": route_budget_caps,
        "repair_allocation_by_evidence_weakness": lane_allocation,
        "runtime_command_adapter": {
            "commands": commands[:8],
            "focus_routes": list(dict.fromkeys(uncapped_focus + focus))[:12],
            "throttle_routes": throttle_routes,
            "throttle_multipliers": throttle_multipliers or {route: 0.15 for route in throttle_routes},
            "max_winner_share_pct_by_route": {route: 25.0 for route in throttle_routes},
            "max_sample_share_pct_by_route": behavior_controls.get("max_sample_share_pct_by_route") or {},
            "default_max_sample_share_pct": behavior_controls.get("default_max_sample_share_pct"),
            "clone_prevention_mode": behavior_controls.get("clone_prevention_mode"),
            "target_behavior_unique_count": behavior_controls.get("target_behavior_unique_count"),
            "required_winner_route_count": behavior_controls.get("required_winner_route_count"),
            "preserved_focus_routes": preserved_focus,
            "route_actions": {command["route_key"]: command for command in commands[:8]},
            "route_budget_caps": route_budget_caps,
            "repair_allocation_by_evidence_weakness": lane_allocation,
        },
    }


def _route_cap_for(route_budget_caps: dict[str, Any], route: str) -> dict[str, Any]:
    for row in route_budget_caps.get("route_caps") or []:
        if str(row.get("route_key") or "") == str(route or ""):
            return row
    return {}


def _distance_for_gate(distance: dict[str, Any], gate_name: str) -> float | None:
    for gate in distance.get("gates") or []:
        if isinstance(gate, dict) and gate.get("gate") == gate_name:
            return gate.get("distance_to_pass")
    return None


def _sibling_routes(route: str) -> list[str]:
    parts = str(route or "").split("|")
    if len(parts) < 3:
        return []
    tickers = ["CLSK", "MARA", "RIOT"]
    phases = ["open", "midday", "late"]
    ticker, setup, phase = parts[0], parts[1], parts[2]
    siblings = []
    for candidate_ticker in tickers:
        if candidate_ticker != ticker and ticker not in {"", "*"}:
            siblings.append("|".join([candidate_ticker, setup or "*", phase or "*"]))
    for candidate_phase in phases:
        if candidate_phase != phase and phase not in {"", "*"}:
            siblings.append("|".join([ticker or "*", setup or "*", candidate_phase]))
    return siblings


def _merge_quarantine_controls(run_dir: Path, controls: dict[str, Any]) -> None:
    if not controls:
        return
    path = run_dir / "quarantine.json"
    payload = hunt_intel.read_json(path, {}) or {}
    for key in ("focus_routes", "uncapped_focus_routes", "capped_routes", "holdout_repair_routes", "day_consistency_repair_routes", "widen_routes", "structural_mutation_routes", "throttle_routes"):
        payload[key] = list(dict.fromkeys(list(payload.get(key) or []) + list(controls.get(key) or [])))[:24]
    adapter = payload.get("runtime_command_adapter") if isinstance(payload.get("runtime_command_adapter"), dict) else {}
    incoming = controls.get("runtime_command_adapter") if isinstance(controls.get("runtime_command_adapter"), dict) else {}
    commands = []
    seen: set[tuple[str, str]] = set()
    for command in list(adapter.get("commands") or []) + list(incoming.get("commands") or []):
        if not isinstance(command, dict):
            continue
        key = (str(command.get("route_key") or ""), str(command.get("action") or ""))
        if key in seen:
            continue
        seen.add(key)
        commands.append(command)
    adapter["commands"] = commands[:20]
    adapter["focus_routes"] = list(dict.fromkeys(list(adapter.get("focus_routes") or []) + list(incoming.get("focus_routes") or [])))[:24]
    adapter["structural_mutation_routes"] = list(dict.fromkeys(list(adapter.get("structural_mutation_routes") or []) + list(incoming.get("structural_mutation_routes") or [])))[:24]
    adapter["throttle_routes"] = list(dict.fromkeys(list(adapter.get("throttle_routes") or []) + list(incoming.get("throttle_routes") or [])))[:24]
    adapter["edge_preservation_routes"] = list(dict.fromkeys(list(adapter.get("edge_preservation_routes") or []) + list(incoming.get("edge_preservation_routes") or [])))[:24]
    adapter["close_sibling_routes"] = list(dict.fromkeys(list(adapter.get("close_sibling_routes") or []) + list(incoming.get("close_sibling_routes") or [])))[:24]
    adapter["generic_route_deprioritize_patterns"] = list(dict.fromkeys(list(adapter.get("generic_route_deprioritize_patterns") or []) + list(incoming.get("generic_route_deprioritize_patterns") or [])))[:24]
    throttle_multipliers = adapter.get("throttle_multipliers") if isinstance(adapter.get("throttle_multipliers"), dict) else {}
    throttle_multipliers.update(incoming.get("throttle_multipliers") or {})
    adapter["throttle_multipliers"] = throttle_multipliers
    winner_caps = adapter.get("max_winner_share_pct_by_route") if isinstance(adapter.get("max_winner_share_pct_by_route"), dict) else {}
    winner_caps.update(incoming.get("max_winner_share_pct_by_route") or {})
    adapter["max_winner_share_pct_by_route"] = winner_caps
    sample_caps = adapter.get("max_sample_share_pct_by_route") if isinstance(adapter.get("max_sample_share_pct_by_route"), dict) else {}
    sample_caps.update(incoming.get("max_sample_share_pct_by_route") or {})
    adapter["max_sample_share_pct_by_route"] = sample_caps
    for scalar_key in ("default_max_sample_share_pct", "clone_prevention_mode", "target_behavior_unique_count", "required_winner_route_count", "outside_repair_envelope_multiplier"):
        if incoming.get(scalar_key) is not None:
            adapter[scalar_key] = incoming.get(scalar_key)
    for scalar_key in ("quality_protection_mode", "route_breadth_required", "prefer_promotion_readiness", "dominant_route_not_admitted"):
        if incoming.get(scalar_key) is not None:
            adapter[scalar_key] = incoming.get(scalar_key)
    if isinstance(incoming.get("quality_protection_controls"), dict):
        adapter["quality_protection_controls"] = incoming.get("quality_protection_controls")
    if isinstance(incoming.get("promotion_ready_repair_envelope"), dict):
        adapter["promotion_ready_repair_envelope"] = incoming.get("promotion_ready_repair_envelope")
    adapter["preserved_focus_routes"] = list(dict.fromkeys(list(adapter.get("preserved_focus_routes") or []) + list(incoming.get("preserved_focus_routes") or [])))[:24]
    adapter["route_budget_caps"] = incoming.get("route_budget_caps") or adapter.get("route_budget_caps") or controls.get("route_budget_caps") or {}
    adapter["repair_allocation_by_evidence_weakness"] = incoming.get("repair_allocation_by_evidence_weakness") or adapter.get("repair_allocation_by_evidence_weakness") or controls.get("repair_allocation_by_evidence_weakness") or {}
    route_actions = adapter.get("route_actions") if isinstance(adapter.get("route_actions"), dict) else {}
    route_actions.update(incoming.get("route_actions") or {})
    adapter["route_actions"] = route_actions
    payload["runtime_command_adapter"] = adapter
    payload["promotion_repair_hunt_controls"] = controls
    _write_json(path, payload)


def _write_runtime_controls(run_dir: Path, state: dict[str, Any]) -> None:
    previous_snapshot = hunt_intel.read_json(run_dir / "runtime_command_snapshot.json", {}) or {}
    enriched = dict(state or {})
    if not isinstance(enriched.get("hypothesis_factory"), dict) or not enriched.get("hypothesis_factory"):
        enriched.update(step2_online_learning.hypothesis_learning_suite(enriched))
    if not isinstance(enriched.get("attention_ledger"), dict) or not enriched.get("attention_ledger"):
        enriched.update(step2_online_learning.attention_economics_suite(enriched))
    if not isinstance(enriched.get("idea_novelty_ledger"), dict) or not enriched.get("idea_novelty_ledger"):
        enriched.update(step2_online_learning.creative_imagination_suite(enriched))
    if not isinstance(enriched.get("learning_module_registry"), dict) or not enriched.get("learning_module_registry"):
        enriched.update(step2_online_learning.meta_learning_suite(enriched))
    if not isinstance(enriched.get("causal_intervention_scheduler"), dict) or not enriched.get("causal_intervention_scheduler"):
        enriched.update(step2_online_learning.runtime_science_suite(enriched))
    if not isinstance(enriched.get("lesson_survival_tracker"), dict) or not enriched.get("lesson_survival_tracker"):
        enriched.update(step2_online_learning.lesson_accountability_suite(enriched))
    if not isinstance(enriched.get("strategy_genome_registry"), dict) or not enriched.get("strategy_genome_registry"):
        enriched.update(step2_online_learning.strategy_evolution_suite(enriched))
    if not isinstance(enriched.get("learning_roi_ledger"), dict) or not enriched.get("learning_roi_ledger"):
        enriched.update(step2_online_learning.governance_suite(enriched))
    if not isinstance(enriched.get("online_causal_bandit"), dict) or not enriched.get("online_causal_bandit"):
        enriched.update(step2_online_learning.real_time_causal_search_suite(enriched))
    if not isinstance(enriched.get("counterfactual_hunt_simulator"), dict) or not enriched.get("counterfactual_hunt_simulator"):
        enriched.update(step2_online_learning.counterfactual_opportunity_suite(enriched))
    if not isinstance(enriched.get("learning_claim_verifier"), dict) or not enriched.get("learning_claim_verifier"):
        enriched.update(step2_online_learning.truth_maintenance_suite(enriched))
    if not isinstance(enriched.get("multi_horizon_memory_stack"), dict) or not enriched.get("multi_horizon_memory_stack"):
        enriched.update(step2_online_learning.temporal_memory_suite(enriched))
    if not isinstance(enriched.get("question_driven_hunt_planner"), dict) or not enriched.get("question_driven_hunt_planner"):
        enriched.update(step2_online_learning.active_uncertainty_learning_suite(enriched))
    if not isinstance(enriched.get("experiment_contract_compiler"), dict) or not enriched.get("experiment_contract_compiler"):
        enriched.update(step2_online_learning.closed_loop_scientific_execution_suite(enriched))
    if not isinstance(enriched.get("live_candidate_evidence_builder"), dict) or not enriched.get("live_candidate_evidence_builder"):
        enriched.update(step2_online_learning.promotion_grade_evidence_suite(enriched))
    if not isinstance(enriched.get("unified_learning_state_reducer"), dict) or not enriched.get("unified_learning_state_reducer"):
        enriched.update(step2_online_learning.world_class_meta_learning_suite(enriched))
    if not isinstance(enriched.get("world_state_next_best_question_engine"), dict) or not enriched.get("world_state_next_best_question_engine"):
        enriched.update(step2_online_learning.elite_world_class_learning_suite(enriched))
    if not isinstance(enriched.get("learning_truth_ledger"), dict) or not enriched.get("learning_truth_ledger"):
        enriched.update(step2_online_learning.proof_oriented_learning_suite(enriched))
    if not isinstance(enriched.get("bayesian_belief_engine"), dict) or not enriched.get("bayesian_belief_engine"):
        enriched.update(step2_online_learning.scientific_control_learning_suite(enriched))
    if not isinstance(enriched.get("hypothesis_dependency_graph"), dict) or not enriched.get("hypothesis_dependency_graph"):
        enriched.update(step2_online_learning.world_model_nervous_system_suite(enriched))
    if not isinstance(enriched.get("unified_artifact_dependency_resolver"), dict) or not enriched.get("unified_artifact_dependency_resolver"):
        enriched.update(step2_online_learning.orchestration_learning_suite(enriched))
    if not isinstance(enriched.get("learning_layer_ablation_harness"), dict) or not enriched.get("learning_layer_ablation_harness"):
        enriched.update(step2_online_learning.fitness_selection_suite(enriched))
    enriched["runtime_command_adapter"] = step2_online_learning.runtime_command_adapter(enriched)
    enriched["degraded_runtime_command_packet"] = step2_online_learning.degraded_runtime_command_packet(enriched)
    enriched["worker_job_contracts"] = step2_online_learning.planner_worker_contracts(enriched)
    enriched["action_reward_calibration"] = step2_online_learning.action_reward_calibration(enriched)
    if not isinstance(enriched.get("learning_artifact_schema_registry"), dict) or not enriched.get("learning_artifact_schema_registry"):
        enriched.update(step2_online_learning.learning_ops_suite(enriched))
        enriched["runtime_command_adapter"] = step2_online_learning.runtime_command_adapter(enriched)
    if previous_snapshot:
        enriched["command_delta_status"] = step2_online_learning.command_delta_status({"runtime_guardrails": {"guarded_command_packet": previous_snapshot}}, enriched)
    else:
        enriched["command_delta_status"] = enriched.get("command_delta_status") or step2_online_learning.command_delta_status({}, enriched)
    quarantine = step2_online_learning.quarantine_payload(enriched)
    quarantine.update({
        "runtime_command_adapter": enriched["runtime_command_adapter"],
        "degraded_runtime_command_packet": enriched["degraded_runtime_command_packet"],
        "worker_job_contracts": enriched["worker_job_contracts"],
        "command_delta_status": enriched["command_delta_status"],
        "action_reward_calibration": enriched["action_reward_calibration"],
        "command_outcome_backfill": enriched.get("command_outcome_backfill") or {},
        "ab_route_experiment_executor": enriched.get("ab_route_experiment_executor") or {},
        "champion_challenger_runtime_slots": enriched.get("champion_challenger_runtime_slots") or {},
        "adaptive_experiment_stopping": enriched.get("adaptive_experiment_stopping") or {},
        "counterfactual_command_replay": enriched.get("counterfactual_command_replay") or {},
        "experiment_contamination_guard": enriched.get("experiment_contamination_guard") or {},
        "learning_rate_controller": enriched.get("learning_rate_controller") or {},
        "worker_learning_report_cards": enriched.get("worker_learning_report_cards") or {},
        "experiment_to_promotion_trace": enriched.get("experiment_to_promotion_trace") or {},
        "zero_yield_autopsy_engine": enriched.get("zero_yield_autopsy_engine") or {},
        "stuck_loop_breaker": enriched.get("stuck_loop_breaker") or {},
        "opportunity_cost_meter": enriched.get("opportunity_cost_meter") or {},
        "search_space_coverage_map": enriched.get("search_space_coverage_map") or {},
        "live_beater_scarcity_mode": enriched.get("live_beater_scarcity_mode") or {},
        "alias_trap_detector": enriched.get("alias_trap_detector") or {},
        "route_seed_quality_score": enriched.get("route_seed_quality_score") or {},
        "recovery_playbook_generator": enriched.get("recovery_playbook_generator") or {},
        "cross_run_hunt_memory_compiler": enriched.get("cross_run_hunt_memory_compiler") or {},
        "pre_hunt_strategy_selector": enriched.get("pre_hunt_strategy_selector") or {},
        "cold_start_route_pack_generator": enriched.get("cold_start_route_pack_generator") or {},
        "longitudinal_treatment_decay": enriched.get("longitudinal_treatment_decay") or {},
        "run_level_promotion_survival_feedback": enriched.get("run_level_promotion_survival_feedback") or {},
        "memory_conflict_arbiter": enriched.get("memory_conflict_arbiter") or {},
        "hunt_opening_playbook": enriched.get("hunt_opening_playbook") or {},
        "cross_run_learning_regression_test": enriched.get("cross_run_learning_regression_test") or {},
        "memory_reliability_scorer": enriched.get("memory_reliability_scorer") or {},
        "memory_falsification_queue": enriched.get("memory_falsification_queue") or {},
        "belief_retirement_engine": enriched.get("belief_retirement_engine") or {},
        "memory_provenance_explorer": enriched.get("memory_provenance_explorer") or {},
        "current_vs_historical_disagreement_monitor": enriched.get("current_vs_historical_disagreement_monitor") or {},
        "memory_stress_test_pack": enriched.get("memory_stress_test_pack") or {},
        "durable_memory_compression": enriched.get("durable_memory_compression") or {},
        "memory_qa_smoke_test": enriched.get("memory_qa_smoke_test") or {},
        "hypothesis_factory": enriched.get("hypothesis_factory") or {},
        "hypothesis_market_maker": enriched.get("hypothesis_market_maker") or {},
        "real_time_bet_sizer": enriched.get("real_time_bet_sizer") or {},
        "contrarian_generator": enriched.get("contrarian_generator") or {},
        "learning_stop_loss": enriched.get("learning_stop_loss") or {},
        "breakthrough_detector": enriched.get("breakthrough_detector") or {},
        "pattern_to_recipe_compiler": enriched.get("pattern_to_recipe_compiler") or {},
        "hunt_narrative_memory": enriched.get("hunt_narrative_memory") or {},
        "attention_ledger": enriched.get("attention_ledger") or {},
        "wasted_spend_autopsy": enriched.get("wasted_spend_autopsy") or {},
        "marginal_yield_curve": enriched.get("marginal_yield_curve") or {},
        "explore_exploit_regret_tracker": enriched.get("explore_exploit_regret_tracker") or {},
        "worker_alpha_attribution": enriched.get("worker_alpha_attribution") or {},
        "budget_reallocator": enriched.get("budget_reallocator") or {},
        "time_aware_hunt_plan": enriched.get("time_aware_hunt_plan") or {},
        "spend_efficiency_narrative": enriched.get("spend_efficiency_narrative") or {},
        "idea_novelty_ledger": enriched.get("idea_novelty_ledger") or {},
        "idea_saturation_detector": enriched.get("idea_saturation_detector") or {},
        "creative_leap_scorer": enriched.get("creative_leap_scorer") or {},
        "failed_imagination_autopsy": enriched.get("failed_imagination_autopsy") or {},
        "mutation_grammar_gap_finder": enriched.get("mutation_grammar_gap_finder") or {},
        "novelty_budget_governor": enriched.get("novelty_budget_governor") or {},
        "idea_lineage_map": enriched.get("idea_lineage_map") or {},
        "creative_brief_compiler": enriched.get("creative_brief_compiler") or {},
        "learning_module_registry": enriched.get("learning_module_registry") or {},
        "module_contribution_attribution": enriched.get("module_contribution_attribution") or {},
        "module_conflict_detector": enriched.get("module_conflict_detector") or {},
        "module_reliability_scorer": enriched.get("module_reliability_scorer") or {},
        "module_ablation_planner": enriched.get("module_ablation_planner") or {},
        "module_budget_governor": enriched.get("module_budget_governor") or {},
        "learning_system_self_audit": enriched.get("learning_system_self_audit") or {},
        "meta_learning_brief": enriched.get("meta_learning_brief") or {},
        "causal_intervention_scheduler": enriched.get("causal_intervention_scheduler") or {},
        "experiment_power_calculator": enriched.get("experiment_power_calculator") or {},
        "winner_fragility_profiler": enriched.get("winner_fragility_profiler") or {},
        "live_beater_source_attribution": enriched.get("live_beater_source_attribution") or {},
        "adaptive_search_temperature_controller": enriched.get("adaptive_search_temperature_controller") or {},
        "route_interaction_learner": enriched.get("route_interaction_learner") or {},
        "false_discovery_firewall": enriched.get("false_discovery_firewall") or {},
        "hunt_strategy_compiler_v2": enriched.get("hunt_strategy_compiler_v2") or {},
        "lesson_survival_tracker": enriched.get("lesson_survival_tracker") or {},
        "promotion_rejection_backpropagation": enriched.get("promotion_rejection_backpropagation") or {},
        "lesson_decay_model": enriched.get("lesson_decay_model") or {},
        "cross_hunt_causal_memory": enriched.get("cross_hunt_causal_memory") or {},
        "evidence_chain_ledger": enriched.get("evidence_chain_ledger") or {},
        "learning_disagreement_court": enriched.get("learning_disagreement_court") or {},
        "promotion_aware_search_objective": enriched.get("promotion_aware_search_objective") or {},
        "scientific_run_brief_v2": enriched.get("scientific_run_brief_v2") or {},
        "strategy_genome_registry": enriched.get("strategy_genome_registry") or {},
        "strategy_mutation_engine": enriched.get("strategy_mutation_engine") or {},
        "strategy_tournament_memory": enriched.get("strategy_tournament_memory") or {},
        "regime_conditioned_strategy_selector": enriched.get("regime_conditioned_strategy_selector") or {},
        "meta_objective_optimizer": enriched.get("meta_objective_optimizer") or {},
        "exploration_debt_ledger": enriched.get("exploration_debt_ledger") or {},
        "adversarial_strategy_red_team": enriched.get("adversarial_strategy_red_team") or {},
        "autonomous_pivot_governor": enriched.get("autonomous_pivot_governor") or {},
        "learning_roi_ledger": enriched.get("learning_roi_ledger") or {},
        "artifact_usefulness_pruner": enriched.get("artifact_usefulness_pruner") or {},
        "decision_trace_explainer": enriched.get("decision_trace_explainer") or {},
        "control_surface_conflict_auditor": enriched.get("control_surface_conflict_auditor") or {},
        "runtime_control_simplifier": enriched.get("runtime_control_simplifier") or {},
        "learning_cost_meter": enriched.get("learning_cost_meter") or {},
        "ablation_replay_harness": enriched.get("ablation_replay_harness") or {},
        "architecture_fitness_brief": enriched.get("architecture_fitness_brief") or {},
        "learning_artifact_schema_registry": enriched.get("learning_artifact_schema_registry") or {},
        "artifact_dependency_graph": enriched.get("artifact_dependency_graph") or {},
        "incremental_learning_cache": enriched.get("incremental_learning_cache") or {},
        "live_learning_dashboard_feed": enriched.get("live_learning_dashboard_feed") or {},
        "hunt_runbook_compiler": enriched.get("hunt_runbook_compiler") or {},
        "learning_failure_sentinel": enriched.get("learning_failure_sentinel") or {},
        "cross_run_artifact_warehouse": enriched.get("cross_run_artifact_warehouse") or {},
        "pre_hunt_readiness_gate": enriched.get("pre_hunt_readiness_gate") or {},
        "online_causal_bandit": enriched.get("online_causal_bandit") or {},
        "variant_dna_attribution": enriched.get("variant_dna_attribution") or {},
        "negative_gene_suppression": enriched.get("negative_gene_suppression") or {},
        "live_winner_family_tree": enriched.get("live_winner_family_tree") or {},
        "exploration_frontier_map": enriched.get("exploration_frontier_map") or {},
        "adaptive_worker_personalities": enriched.get("adaptive_worker_personalities") or {},
        "cycle_level_learning_delta": enriched.get("cycle_level_learning_delta") or {},
        "promotion_rejection_predictor_v2": enriched.get("promotion_rejection_predictor_v2") or {},
        "counterfactual_hunt_simulator": enriched.get("counterfactual_hunt_simulator") or {},
        "missed_winner_detector": enriched.get("missed_winner_detector") or {},
        "causal_regret_ledger": enriched.get("causal_regret_ledger") or {},
        "adaptive_search_grammar_generator": enriched.get("adaptive_search_grammar_generator") or {},
        "live_hypothesis_kill_scale_court": enriched.get("live_hypothesis_kill_scale_court") or {},
        "route_interaction_matrix_v2": enriched.get("route_interaction_matrix_v2") or {},
        "promotion_survival_shadow_scoring": enriched.get("promotion_survival_shadow_scoring") or {},
        "hunt_autopilot_policy_compiler": enriched.get("hunt_autopilot_policy_compiler") or {},
        "learning_claim_verifier": enriched.get("learning_claim_verifier") or {},
        "causal_confidence_calibration": enriched.get("causal_confidence_calibration") or {},
        "false_discovery_early_warning": enriched.get("false_discovery_early_warning") or {},
        "adaptive_evidence_thresholds": enriched.get("adaptive_evidence_thresholds") or {},
        "self_debate_search_council": enriched.get("self_debate_search_council") or {},
        "experiment_memory_compression": enriched.get("experiment_memory_compression") or {},
        "learning_drift_monitor": enriched.get("learning_drift_monitor") or {},
        "promotion_first_autopilot_v2": enriched.get("promotion_first_autopilot_v2") or {},
        "multi_horizon_memory_stack": enriched.get("multi_horizon_memory_stack") or {},
        "lesson_half_life_engine_v2": enriched.get("lesson_half_life_engine_v2") or {},
        "cross_hunt_strategy_replay": enriched.get("cross_hunt_strategy_replay") or {},
        "temporal_regime_fingerprinting": enriched.get("temporal_regime_fingerprinting") or {},
        "longitudinal_promotion_survival_model": enriched.get("longitudinal_promotion_survival_model") or {},
        "memory_conflict_court_v2": enriched.get("memory_conflict_court_v2") or {},
        "strategy_aging_dashboard": enriched.get("strategy_aging_dashboard") or {},
        "next_hunt_opening_policy_compiler": enriched.get("next_hunt_opening_policy_compiler") or {},
        "question_driven_hunt_planner": enriched.get("question_driven_hunt_planner") or {},
        "expected_information_gain_scorer_v2": enriched.get("expected_information_gain_scorer_v2") or {},
        "uncertainty_heatmap": enriched.get("uncertainty_heatmap") or {},
        "adaptive_experiment_sequencer": enriched.get("adaptive_experiment_sequencer") or {},
        "learning_value_stop_loss": enriched.get("learning_value_stop_loss") or {},
        "causal_question_ledger": enriched.get("causal_question_ledger") or {},
        "worker_epistemic_roles_v2": enriched.get("worker_epistemic_roles_v2") or {},
        "hunt_hypothesis_compiler": enriched.get("hunt_hypothesis_compiler") or {},
        "experiment_contract_compiler": enriched.get("experiment_contract_compiler") or {},
        "control_route_matcher": enriched.get("control_route_matcher") or {},
        "sequential_test_monitor": enriched.get("sequential_test_monitor") or {},
        "causal_effect_size_ledger": enriched.get("causal_effect_size_ledger") or {},
        "false_positive_pressure_gauge": enriched.get("false_positive_pressure_gauge") or {},
        "exploration_debt_paydown_planner": enriched.get("exploration_debt_paydown_planner") or {},
        "promotion_aware_power_planner": enriched.get("promotion_aware_power_planner") or {},
        "scientific_hunt_executive": enriched.get("scientific_hunt_executive") or {},
        "live_candidate_evidence_builder": enriched.get("live_candidate_evidence_builder") or {},
        "promotion_failure_predictor_v3": enriched.get("promotion_failure_predictor_v3") or {},
        "evidence_gap_router": enriched.get("evidence_gap_router") or {},
        "review_ready_queue_v2": enriched.get("review_ready_queue_v2") or {},
        "promotion_evidence_scorecard": enriched.get("promotion_evidence_scorecard") or {},
        "candidate_lineage_explainer_v2": enriched.get("candidate_lineage_explainer_v2") or {},
        "live_vs_control_differential_report": enriched.get("live_vs_control_differential_report") or {},
        "promotion_packet_executive": enriched.get("promotion_packet_executive") or {},
    })
    for key in WORLD_CLASS_META_KEYS:
        quarantine[key] = enriched.get(key) or {}
    for key in ELITE_LEARNING_KEYS:
        quarantine[key] = enriched.get(key) or {}
    for key in PROOF_LEARNING_KEYS:
        quarantine[key] = enriched.get(key) or {}
    for key in CLOSED_LOOP_CONTROL_KEYS:
        quarantine[key] = enriched.get(key) or {}
    for key in WORLD_MODEL_NERVOUS_KEYS:
        quarantine[key] = enriched.get(key) or {}
    for key in ORCHESTRATION_LEARNING_KEYS:
        quarantine[key] = enriched.get(key) or {}
    for key in FITNESS_SELECTION_KEYS:
        quarantine[key] = enriched.get(key) or {}
    _write_compact_json(run_dir / "quarantine.json", quarantine)
    _write_json(run_dir / "runtime_command_adapter.json", enriched["runtime_command_adapter"])
    _write_json(run_dir / "degraded_runtime_command_packet.json", enriched["degraded_runtime_command_packet"])
    _write_json(run_dir / "worker_job_contracts.json", enriched["worker_job_contracts"])
    _write_json(run_dir / "command_delta_status.json", enriched["command_delta_status"])
    _write_json(run_dir / "action_reward_calibration.json", enriched["action_reward_calibration"])
    for key in (
        "ab_route_experiment_executor",
        "champion_challenger_runtime_slots",
        "adaptive_experiment_stopping",
        "counterfactual_command_replay",
        "experiment_contamination_guard",
        "learning_rate_controller",
        "worker_learning_report_cards",
        "experiment_to_promotion_trace",
        "zero_yield_autopsy_engine",
        "stuck_loop_breaker",
        "opportunity_cost_meter",
        "search_space_coverage_map",
        "live_beater_scarcity_mode",
        "alias_trap_detector",
        "route_seed_quality_score",
        "recovery_playbook_generator",
        "cross_run_hunt_memory_compiler",
        "memory_reliability_scorer",
        "memory_falsification_queue",
        "belief_retirement_engine",
        "memory_provenance_explorer",
        "current_vs_historical_disagreement_monitor",
        "memory_stress_test_pack",
        "durable_memory_compression",
        "memory_qa_smoke_test",
        "hypothesis_factory",
        "hypothesis_market_maker",
        "real_time_bet_sizer",
        "contrarian_generator",
        "learning_stop_loss",
        "breakthrough_detector",
        "pattern_to_recipe_compiler",
        "hunt_narrative_memory",
        "attention_ledger",
        "wasted_spend_autopsy",
        "marginal_yield_curve",
        "explore_exploit_regret_tracker",
        "worker_alpha_attribution",
        "budget_reallocator",
        "time_aware_hunt_plan",
        "spend_efficiency_narrative",
        "idea_novelty_ledger",
        "idea_saturation_detector",
        "creative_leap_scorer",
        "failed_imagination_autopsy",
        "mutation_grammar_gap_finder",
        "novelty_budget_governor",
        "idea_lineage_map",
        "creative_brief_compiler",
        "learning_module_registry",
        "module_contribution_attribution",
        "module_conflict_detector",
        "module_reliability_scorer",
        "module_ablation_planner",
        "module_budget_governor",
        "learning_system_self_audit",
        "meta_learning_brief",
        "causal_intervention_scheduler",
        "experiment_power_calculator",
        "winner_fragility_profiler",
        "live_beater_source_attribution",
        "adaptive_search_temperature_controller",
        "route_interaction_learner",
        "false_discovery_firewall",
        "hunt_strategy_compiler_v2",
        "lesson_survival_tracker",
        "promotion_rejection_backpropagation",
        "lesson_decay_model",
        "cross_hunt_causal_memory",
        "evidence_chain_ledger",
        "learning_disagreement_court",
        "promotion_aware_search_objective",
        "scientific_run_brief_v2",
        "strategy_genome_registry",
        "strategy_mutation_engine",
        "strategy_tournament_memory",
        "regime_conditioned_strategy_selector",
        "meta_objective_optimizer",
        "exploration_debt_ledger",
        "adversarial_strategy_red_team",
        "autonomous_pivot_governor",
        "learning_roi_ledger",
        "artifact_usefulness_pruner",
        "decision_trace_explainer",
        "control_surface_conflict_auditor",
        "runtime_control_simplifier",
        "learning_cost_meter",
        "ablation_replay_harness",
        "architecture_fitness_brief",
        "learning_artifact_schema_registry",
        "artifact_dependency_graph",
        "incremental_learning_cache",
        "live_learning_dashboard_feed",
        "hunt_runbook_compiler",
        "learning_failure_sentinel",
        "cross_run_artifact_warehouse",
        "pre_hunt_readiness_gate",
        "online_causal_bandit",
        "variant_dna_attribution",
        "negative_gene_suppression",
        "live_winner_family_tree",
        "exploration_frontier_map",
        "adaptive_worker_personalities",
        "cycle_level_learning_delta",
        "promotion_rejection_predictor_v2",
        "counterfactual_hunt_simulator",
        "missed_winner_detector",
        "causal_regret_ledger",
        "adaptive_search_grammar_generator",
        "live_hypothesis_kill_scale_court",
        "route_interaction_matrix_v2",
        "promotion_survival_shadow_scoring",
        "hunt_autopilot_policy_compiler",
        "learning_claim_verifier",
        "causal_confidence_calibration",
        "false_discovery_early_warning",
        "adaptive_evidence_thresholds",
        "self_debate_search_council",
        "experiment_memory_compression",
        "learning_drift_monitor",
        "promotion_first_autopilot_v2",
        "multi_horizon_memory_stack",
        "lesson_half_life_engine_v2",
        "cross_hunt_strategy_replay",
        "temporal_regime_fingerprinting",
        "longitudinal_promotion_survival_model",
        "memory_conflict_court_v2",
        "strategy_aging_dashboard",
        "next_hunt_opening_policy_compiler",
        "question_driven_hunt_planner",
        "expected_information_gain_scorer_v2",
        "uncertainty_heatmap",
        "adaptive_experiment_sequencer",
        "learning_value_stop_loss",
        "causal_question_ledger",
        "worker_epistemic_roles_v2",
        "hunt_hypothesis_compiler",
        "experiment_contract_compiler",
        "control_route_matcher",
        "sequential_test_monitor",
        "causal_effect_size_ledger",
        "false_positive_pressure_gauge",
        "exploration_debt_paydown_planner",
        "promotion_aware_power_planner",
        "scientific_hunt_executive",
        "live_candidate_evidence_builder",
        "promotion_failure_predictor_v3",
        "evidence_gap_router",
        "review_ready_queue_v2",
        "promotion_evidence_scorecard",
        "candidate_lineage_explainer_v2",
        "live_vs_control_differential_report",
        "promotion_packet_executive",
        *WORLD_CLASS_META_KEYS,
        *ELITE_LEARNING_KEYS,
        *PROOF_LEARNING_KEYS,
        *CLOSED_LOOP_CONTROL_KEYS,
        *WORLD_MODEL_NERVOUS_KEYS,
        *ORCHESTRATION_LEARNING_KEYS,
        *FITNESS_SELECTION_KEYS,
        "pre_hunt_strategy_selector",
        "cold_start_route_pack_generator",
        "longitudinal_treatment_decay",
        "run_level_promotion_survival_feedback",
        "memory_conflict_arbiter",
        "hunt_opening_playbook",
        "cross_run_learning_regression_test",
    ):
        if isinstance(enriched.get(key), dict):
            _write_json(run_dir / f"{key}.json", enriched.get(key) or {})
    _write_json(run_dir / "runtime_command_snapshot.json", step2_online_learning.active_runtime_command_packet(enriched))
    contracts_dir = run_dir / "worker_contracts"
    contracts_dir.mkdir(parents=True, exist_ok=True)
    for contract in enriched["worker_job_contracts"].get("contracts") or []:
        worker = str(contract.get("worker") or "").strip()
        if worker:
            _write_json(contracts_dir / f"{worker}.json", contract)
    _write_json(run_dir / "stop_signal.json", {"stop": False, "reason": ""})


def _attach_runtime_experiment_learning(state: dict[str, Any], learning: dict[str, Any]) -> dict[str, Any]:
    state = dict(state or {})
    for key in (
        "ab_route_experiment_executor",
        "champion_challenger_runtime_slots",
        "adaptive_experiment_stopping",
        "counterfactual_command_replay",
        "experiment_contamination_guard",
        "learning_rate_controller",
        "worker_learning_report_cards",
        "experiment_to_promotion_trace",
        "zero_yield_autopsy_engine",
        "stuck_loop_breaker",
        "opportunity_cost_meter",
        "search_space_coverage_map",
        "live_beater_scarcity_mode",
        "alias_trap_detector",
        "route_seed_quality_score",
        "recovery_playbook_generator",
        "cross_run_hunt_memory_compiler",
        "memory_reliability_scorer",
        "memory_falsification_queue",
        "belief_retirement_engine",
        "memory_provenance_explorer",
        "current_vs_historical_disagreement_monitor",
        "memory_stress_test_pack",
        "durable_memory_compression",
        "memory_qa_smoke_test",
        "hypothesis_factory",
        "hypothesis_market_maker",
        "real_time_bet_sizer",
        "contrarian_generator",
        "learning_stop_loss",
        "breakthrough_detector",
        "pattern_to_recipe_compiler",
        "hunt_narrative_memory",
        "attention_ledger",
        "wasted_spend_autopsy",
        "marginal_yield_curve",
        "explore_exploit_regret_tracker",
        "worker_alpha_attribution",
        "budget_reallocator",
        "time_aware_hunt_plan",
        "spend_efficiency_narrative",
        "idea_novelty_ledger",
        "idea_saturation_detector",
        "creative_leap_scorer",
        "failed_imagination_autopsy",
        "mutation_grammar_gap_finder",
        "novelty_budget_governor",
        "idea_lineage_map",
        "creative_brief_compiler",
        "learning_module_registry",
        "module_contribution_attribution",
        "module_conflict_detector",
        "module_reliability_scorer",
        "module_ablation_planner",
        "module_budget_governor",
        "learning_system_self_audit",
        "meta_learning_brief",
        "causal_intervention_scheduler",
        "experiment_power_calculator",
        "winner_fragility_profiler",
        "live_beater_source_attribution",
        "adaptive_search_temperature_controller",
        "route_interaction_learner",
        "false_discovery_firewall",
        "hunt_strategy_compiler_v2",
        "lesson_survival_tracker",
        "promotion_rejection_backpropagation",
        "lesson_decay_model",
        "cross_hunt_causal_memory",
        "evidence_chain_ledger",
        "learning_disagreement_court",
        "promotion_aware_search_objective",
        "scientific_run_brief_v2",
        "strategy_genome_registry",
        "strategy_mutation_engine",
        "strategy_tournament_memory",
        "regime_conditioned_strategy_selector",
        "meta_objective_optimizer",
        "exploration_debt_ledger",
        "adversarial_strategy_red_team",
        "autonomous_pivot_governor",
        "learning_roi_ledger",
        "artifact_usefulness_pruner",
        "decision_trace_explainer",
        "control_surface_conflict_auditor",
        "runtime_control_simplifier",
        "learning_cost_meter",
        "ablation_replay_harness",
        "architecture_fitness_brief",
        "learning_artifact_schema_registry",
        "artifact_dependency_graph",
        "incremental_learning_cache",
        "live_learning_dashboard_feed",
        "hunt_runbook_compiler",
        "learning_failure_sentinel",
        "cross_run_artifact_warehouse",
        "pre_hunt_readiness_gate",
        "online_causal_bandit",
        "variant_dna_attribution",
        "negative_gene_suppression",
        "live_winner_family_tree",
        "exploration_frontier_map",
        "adaptive_worker_personalities",
        "cycle_level_learning_delta",
        "promotion_rejection_predictor_v2",
        "counterfactual_hunt_simulator",
        "missed_winner_detector",
        "causal_regret_ledger",
        "adaptive_search_grammar_generator",
        "live_hypothesis_kill_scale_court",
        "route_interaction_matrix_v2",
        "promotion_survival_shadow_scoring",
        "hunt_autopilot_policy_compiler",
        "learning_claim_verifier",
        "causal_confidence_calibration",
        "false_discovery_early_warning",
        "adaptive_evidence_thresholds",
        "self_debate_search_council",
        "experiment_memory_compression",
        "learning_drift_monitor",
        "promotion_first_autopilot_v2",
        "multi_horizon_memory_stack",
        "lesson_half_life_engine_v2",
        "cross_hunt_strategy_replay",
        "temporal_regime_fingerprinting",
        "longitudinal_promotion_survival_model",
        "memory_conflict_court_v2",
        "strategy_aging_dashboard",
        "next_hunt_opening_policy_compiler",
        "question_driven_hunt_planner",
        "expected_information_gain_scorer_v2",
        "uncertainty_heatmap",
        "adaptive_experiment_sequencer",
        "learning_value_stop_loss",
        "causal_question_ledger",
        "worker_epistemic_roles_v2",
        "hunt_hypothesis_compiler",
        "experiment_contract_compiler",
        "control_route_matcher",
        "sequential_test_monitor",
        "causal_effect_size_ledger",
        "false_positive_pressure_gauge",
        "exploration_debt_paydown_planner",
        "promotion_aware_power_planner",
        "scientific_hunt_executive",
        "live_candidate_evidence_builder",
        "promotion_failure_predictor_v3",
        "evidence_gap_router",
        "review_ready_queue_v2",
        "promotion_evidence_scorecard",
        "candidate_lineage_explainer_v2",
        "live_vs_control_differential_report",
        "promotion_packet_executive",
        *WORLD_CLASS_META_KEYS,
        *ELITE_LEARNING_KEYS,
        *PROOF_LEARNING_KEYS,
        *CLOSED_LOOP_CONTROL_KEYS,
        *WORLD_MODEL_NERVOUS_KEYS,
        *ORCHESTRATION_LEARNING_KEYS,
        *FITNESS_SELECTION_KEYS,
        "pre_hunt_strategy_selector",
        "cold_start_route_pack_generator",
        "longitudinal_treatment_decay",
        "run_level_promotion_survival_feedback",
        "memory_conflict_arbiter",
        "hunt_opening_playbook",
        "cross_run_learning_regression_test",
    ):
        value = learning.get(key) if isinstance(learning.get(key), dict) else {}
        if value:
            state[key] = value
    state["runtime_command_adapter"] = step2_online_learning.runtime_command_adapter(state)
    state["worker_job_contracts"] = step2_online_learning.planner_worker_contracts(state)
    if not isinstance(state.get("learning_artifact_schema_registry"), dict) or not state.get("learning_artifact_schema_registry"):
        state.update(step2_online_learning.learning_ops_suite(state))
    if not isinstance(state.get("online_causal_bandit"), dict) or not state.get("online_causal_bandit"):
        state.update(step2_online_learning.real_time_causal_search_suite(state))
    if not isinstance(state.get("counterfactual_hunt_simulator"), dict) or not state.get("counterfactual_hunt_simulator"):
        state.update(step2_online_learning.counterfactual_opportunity_suite(state))
    if not isinstance(state.get("learning_claim_verifier"), dict) or not state.get("learning_claim_verifier"):
        state.update(step2_online_learning.truth_maintenance_suite(state))
    if not isinstance(state.get("multi_horizon_memory_stack"), dict) or not state.get("multi_horizon_memory_stack"):
        state.update(step2_online_learning.temporal_memory_suite(state))
    if not isinstance(state.get("question_driven_hunt_planner"), dict) or not state.get("question_driven_hunt_planner"):
        state.update(step2_online_learning.active_uncertainty_learning_suite(state))
    if not isinstance(state.get("experiment_contract_compiler"), dict) or not state.get("experiment_contract_compiler"):
        state.update(step2_online_learning.closed_loop_scientific_execution_suite(state))
    if not isinstance(state.get("live_candidate_evidence_builder"), dict) or not state.get("live_candidate_evidence_builder"):
        state.update(step2_online_learning.promotion_grade_evidence_suite(state))
    if not isinstance(state.get("unified_learning_state_reducer"), dict) or not state.get("unified_learning_state_reducer"):
        state.update(step2_online_learning.world_class_meta_learning_suite(state))
    if not isinstance(state.get("world_state_next_best_question_engine"), dict) or not state.get("world_state_next_best_question_engine"):
        state.update(step2_online_learning.elite_world_class_learning_suite(state))
    if not isinstance(state.get("learning_truth_ledger"), dict) or not state.get("learning_truth_ledger"):
        state.update(step2_online_learning.proof_oriented_learning_suite(state))
    if not isinstance(state.get("bayesian_belief_engine"), dict) or not state.get("bayesian_belief_engine"):
        state.update(step2_online_learning.scientific_control_learning_suite(state))
    if not isinstance(state.get("hypothesis_dependency_graph"), dict) or not state.get("hypothesis_dependency_graph"):
        state.update(step2_online_learning.world_model_nervous_system_suite(state))
    if not isinstance(state.get("unified_artifact_dependency_resolver"), dict) or not state.get("unified_artifact_dependency_resolver"):
        state.update(step2_online_learning.orchestration_learning_suite(state))
    if not isinstance(state.get("learning_layer_ablation_harness"), dict) or not state.get("learning_layer_ablation_harness"):
        state.update(step2_online_learning.fitness_selection_suite(state))
    state["runtime_command_adapter"] = step2_online_learning.runtime_command_adapter(state)
    directive = state.get("learning_upgrade_directive") if isinstance(state.get("learning_upgrade_directive"), dict) else {}
    directive.update({
        "ab_experiment_assignment_count": (state.get("ab_route_experiment_executor") or {}).get("assignment_count"),
        "champion_challenger_slots": (state.get("champion_challenger_runtime_slots") or {}).get("slots") or [],
        "adaptive_experiment_stop_count": (state.get("adaptive_experiment_stopping") or {}).get("stop_count"),
        "best_counterfactual_command": (state.get("counterfactual_command_replay") or {}).get("best_counterfactual") or {},
        "experiment_contamination_ok": (state.get("experiment_contamination_guard") or {}).get("ok"),
        "learning_rate_mode": (state.get("learning_rate_controller") or {}).get("mode"),
        "top_worker_report_card": (state.get("worker_learning_report_cards") or {}).get("top_worker") or {},
        "top_experiment_promotion_trace": (state.get("experiment_to_promotion_trace") or {}).get("top_trace") or {},
        "zero_yield_count": (state.get("zero_yield_autopsy_engine") or {}).get("zero_yield_count"),
        "stuck_reset_count": (state.get("stuck_loop_breaker") or {}).get("reset_count"),
        "highest_opportunity_cost": (state.get("opportunity_cost_meter") or {}).get("highest_cost") or {},
        "live_beater_scarcity_mode": (state.get("live_beater_scarcity_mode") or {}).get("mode"),
        "alias_trap_count": (state.get("alias_trap_detector") or {}).get("trap_count"),
        "top_route_seed_quality": (state.get("route_seed_quality_score") or {}).get("top_seed") or {},
        "top_recovery_intervention": (state.get("recovery_playbook_generator") or {}).get("top_intervention") or {},
        "attention_total_spend": (state.get("attention_ledger") or {}).get("total_spend_units"),
        "top_wasted_spend": (state.get("wasted_spend_autopsy") or {}).get("top_waste") or {},
        "plateau_routes": (state.get("marginal_yield_curve") or {}).get("plateau_routes") or [],
        "attention_regret": (state.get("explore_exploit_regret_tracker") or {}).get("recommendation"),
        "top_worker_alpha": (state.get("worker_alpha_attribution") or {}).get("top_worker") or {},
        "budget_reallocation": {
            "focus_routes": (state.get("budget_reallocator") or {}).get("focus_routes") or [],
            "avoid_routes": (state.get("budget_reallocator") or {}).get("avoid_routes") or [],
            "recommendation": (state.get("budget_reallocator") or {}).get("recommendation"),
        },
        "time_aware_phase": (state.get("time_aware_hunt_plan") or {}).get("phase"),
        "spend_efficiency": (state.get("spend_efficiency_narrative") or {}).get("summary"),
        "fresh_idea_count": len((state.get("idea_novelty_ledger") or {}).get("fresh_ideas") or []),
        "idea_saturation_count": (state.get("idea_saturation_detector") or {}).get("saturation_count"),
        "top_creative_leap": (((state.get("creative_leap_scorer") or {}).get("leaps") or [{}])[0]),
        "failed_imagination_count": len((state.get("failed_imagination_autopsy") or {}).get("autopsies") or []),
        "top_mutation_gap": (state.get("mutation_grammar_gap_finder") or {}).get("top_gap") or {},
        "novelty_budget_pct": (state.get("novelty_budget_governor") or {}).get("novelty_budget_pct"),
        "idea_lineage_nodes": (state.get("idea_lineage_map") or {}).get("node_count"),
        "creative_brief": (state.get("creative_brief_compiler") or {}).get("summary"),
        "active_learning_modules": (state.get("learning_module_registry") or {}).get("active_modules") or [],
        "top_module_contribution": (state.get("module_contribution_attribution") or {}).get("top_module") or {},
        "module_conflict_count": (state.get("module_conflict_detector") or {}).get("conflict_count"),
        "top_module_reliability": (((state.get("module_reliability_scorer") or {}).get("scores") or [{}])[0]),
        "module_ablation_count": (state.get("module_ablation_planner") or {}).get("test_count"),
        "module_budget_top": (state.get("module_budget_governor") or {}).get("top_module") or {},
        "learning_system_health": (state.get("learning_system_self_audit") or {}).get("health_score"),
        "meta_learning_brief": (state.get("meta_learning_brief") or {}).get("summary"),
        "top_causal_intervention": (state.get("causal_intervention_scheduler") or {}).get("top_intervention") or {},
        "experiment_power_score": (state.get("experiment_power_calculator") or {}).get("overall_power_score"),
        "underpowered_experiment_count": len((state.get("experiment_power_calculator") or {}).get("underpowered_experiments") or []),
        "top_winner_fragility": (state.get("winner_fragility_profiler") or {}).get("top_fragility") or {},
        "top_live_beater_source": (state.get("live_beater_source_attribution") or {}).get("top_source") or {},
        "search_temperature": (state.get("adaptive_search_temperature_controller") or {}).get("temperature"),
        "top_route_interaction": (state.get("route_interaction_learner") or {}).get("top_interaction") or {},
        "false_discovery_flag_count": (state.get("false_discovery_firewall") or {}).get("flag_count"),
        "hunt_strategy_v2": (state.get("hunt_strategy_compiler_v2") or {}).get("strategy"),
        "hunt_strategy_v2_summary": (state.get("hunt_strategy_compiler_v2") or {}).get("summary"),
        "top_surviving_lesson": (state.get("lesson_survival_tracker") or {}).get("top_lesson") or {},
        "promotion_rejection_backprop_count": (state.get("promotion_rejection_backpropagation") or {}).get("rejection_count"),
        "lesson_decay_top": (state.get("lesson_decay_model") or {}).get("top_lesson") or {},
        "cross_hunt_causal_record_count": (state.get("cross_hunt_causal_memory") or {}).get("record_count"),
        "top_evidence_chain": (state.get("evidence_chain_ledger") or {}).get("top_chain") or {},
        "learning_court_case_count": (state.get("learning_disagreement_court") or {}).get("case_count"),
        "promotion_aware_top_candidate": (state.get("promotion_aware_search_objective") or {}).get("top_candidate") or {},
        "scientific_run_brief": (state.get("scientific_run_brief_v2") or {}).get("summary"),
        "strategy_genome_champion": (state.get("strategy_genome_registry") or {}).get("champion") or {},
        "top_strategy_challenger": (state.get("strategy_mutation_engine") or {}).get("top_challenger") or {},
        "strategy_tournament_champion": (state.get("strategy_tournament_memory") or {}).get("champion") or {},
        "selected_strategy_regime": (state.get("regime_conditioned_strategy_selector") or {}).get("regime"),
        "meta_objective": (state.get("meta_objective_optimizer") or {}).get("objective"),
        "exploration_debt_count": (state.get("exploration_debt_ledger") or {}).get("debt_count"),
        "strategy_red_team_top_attack": (state.get("adversarial_strategy_red_team") or {}).get("top_attack") or {},
        "autonomous_pivot": (state.get("autonomous_pivot_governor") or {}).get("pivot"),
        "autonomous_pivot_reason": (state.get("autonomous_pivot_governor") or {}).get("pivot_reason"),
        "top_learning_roi": (state.get("learning_roi_ledger") or {}).get("top_roi") or {},
        "prune_candidate_count": len((state.get("artifact_usefulness_pruner") or {}).get("prune_candidates") or []),
        "decision_trace_count": (state.get("decision_trace_explainer") or {}).get("trace_count"),
        "control_conflict_count": (state.get("control_surface_conflict_auditor") or {}).get("conflict_count"),
        "simplified_runtime_width": (state.get("runtime_control_simplifier") or {}).get("mutation_width"),
        "learning_cost_highest": (state.get("learning_cost_meter") or {}).get("highest_cost") or {},
        "ablation_replay_count": (state.get("ablation_replay_harness") or {}).get("test_count"),
        "architecture_fitness_verdict": (state.get("architecture_fitness_brief") or {}).get("verdict"),
        "architecture_fitness_summary": (state.get("architecture_fitness_brief") or {}).get("summary"),
        "schema_registry_valid": (state.get("learning_artifact_schema_registry") or {}).get("valid"),
        "artifact_dependency_root_count": len((state.get("artifact_dependency_graph") or {}).get("root_nodes") or []),
        "incremental_cache_reuse_count": (state.get("incremental_learning_cache") or {}).get("reuse_count"),
        "dashboard_status": (state.get("live_learning_dashboard_feed") or {}).get("status"),
        "runbook_worker_count": (state.get("hunt_runbook_compiler") or {}).get("worker_count"),
        "learning_failure_count": (state.get("learning_failure_sentinel") or {}).get("failure_count"),
        "warehouse_export_count": (state.get("cross_run_artifact_warehouse") or {}).get("export_count"),
        "pre_hunt_ready": (state.get("pre_hunt_readiness_gate") or {}).get("ready"),
        "online_causal_top_lane": (state.get("online_causal_bandit") or {}).get("top_lane"),
        "online_causal_allocations": (state.get("online_causal_bandit") or {}).get("allocations") or [],
        "variant_dna_top_gene": ((state.get("variant_dna_attribution") or {}).get("top_gene") or {}).get("gene"),
        "negative_gene_suppressed_count": (state.get("negative_gene_suppression") or {}).get("suppressed_count"),
        "winner_family_count": (state.get("live_winner_family_tree") or {}).get("family_count"),
        "exploration_frontier_count": (state.get("exploration_frontier_map") or {}).get("frontier_count"),
        "worker_personality_assignments": (state.get("adaptive_worker_personalities") or {}).get("assignments") or [],
        "cycle_learning_delta": (state.get("cycle_level_learning_delta") or {}).get("summary"),
        "promotion_rejection_v2_highest_risk": (state.get("promotion_rejection_predictor_v2") or {}).get("highest_risk") or {},
        "top_counterfactual": (state.get("counterfactual_hunt_simulator") or {}).get("top_counterfactual") or {},
        "missed_winner_count": (state.get("missed_winner_detector") or {}).get("missed_count"),
        "top_causal_regret": (state.get("causal_regret_ledger") or {}).get("top_regret") or {},
        "search_grammar_top_templates": (state.get("adaptive_search_grammar_generator") or {}).get("top_templates") or [],
        "hypothesis_court_counts": {
            "scale": (state.get("live_hypothesis_kill_scale_court") or {}).get("scale_count"),
            "kill": (state.get("live_hypothesis_kill_scale_court") or {}).get("kill_count"),
            "retest": (state.get("live_hypothesis_kill_scale_court") or {}).get("retest_count"),
        },
        "route_interaction_v2_top": (state.get("route_interaction_matrix_v2") or {}).get("top_interaction") or {},
        "promotion_shadow_top": (state.get("promotion_survival_shadow_scoring") or {}).get("top_shadow") or {},
        "autopilot_policy_packet": (state.get("hunt_autopilot_policy_compiler") or {}).get("policy_packet") or {},
        "verified_learning_claim_count": (state.get("learning_claim_verifier") or {}).get("verified_count"),
        "causal_confidence_top": (state.get("causal_confidence_calibration") or {}).get("top_confidence") or {},
        "false_discovery_warning_count": (state.get("false_discovery_early_warning") or {}).get("warning_count"),
        "evidence_threshold_mode": (state.get("adaptive_evidence_thresholds") or {}).get("mode"),
        "self_debate_resolution": (state.get("self_debate_search_council") or {}).get("resolution") or {},
        "compressed_memory_rule_count": (state.get("experiment_memory_compression") or {}).get("rule_count"),
        "learning_drift_count": (state.get("learning_drift_monitor") or {}).get("drift_count"),
        "promotion_first_policy_packet": (state.get("promotion_first_autopilot_v2") or {}).get("policy_packet") or {},
        "memory_horizon_summary": (state.get("multi_horizon_memory_stack") or {}).get("summary") or {},
        "lesson_half_life_top": (state.get("lesson_half_life_engine_v2") or {}).get("top_lesson") or {},
        "cross_hunt_replay_top": (state.get("cross_hunt_strategy_replay") or {}).get("top_replay") or {},
        "temporal_regime_top": (state.get("temporal_regime_fingerprinting") or {}).get("top_fingerprint") or {},
        "longitudinal_survival_top": (state.get("longitudinal_promotion_survival_model") or {}).get("top_survival_feature") or {},
        "memory_conflict_v2_count": (state.get("memory_conflict_court_v2") or {}).get("case_count"),
        "strategy_aging_counts": (state.get("strategy_aging_dashboard") or {}).get("aging_counts") or {},
        "next_hunt_opening_policy": (state.get("next_hunt_opening_policy_compiler") or {}).get("policy_packet") or {},
        "question_planner_top": (state.get("question_driven_hunt_planner") or {}).get("top_question") or {},
        "expected_information_gain_top": (state.get("expected_information_gain_scorer_v2") or {}).get("top_score") or {},
        "uncertainty_heatmap_top": (state.get("uncertainty_heatmap") or {}).get("top_cell") or {},
        "adaptive_experiment_next_step": (state.get("adaptive_experiment_sequencer") or {}).get("next_step") or {},
        "learning_value_stop_count": (state.get("learning_value_stop_loss") or {}).get("stop_count"),
        "causal_question_top": (state.get("causal_question_ledger") or {}).get("top_question") or {},
        "epistemic_role_assignments": (state.get("worker_epistemic_roles_v2") or {}).get("assignments") or [],
        "compiled_hypotheses": (state.get("hunt_hypothesis_compiler") or {}).get("hypotheses") or [],
        "experiment_contract_top": (state.get("experiment_contract_compiler") or {}).get("top_contract") or {},
        "control_route_top": (state.get("control_route_matcher") or {}).get("top_match") or {},
        "sequential_test_top": (state.get("sequential_test_monitor") or {}).get("top_decision") or {},
        "causal_effect_top": (state.get("causal_effect_size_ledger") or {}).get("top_effect") or {},
        "false_positive_pressure": {
            "score": (state.get("false_positive_pressure_gauge") or {}).get("pressure_score"),
            "mode": (state.get("false_positive_pressure_gauge") or {}).get("mode"),
        },
        "exploration_paydown_top": (state.get("exploration_debt_paydown_planner") or {}).get("top_plan") or {},
        "promotion_power_top": (state.get("promotion_aware_power_planner") or {}).get("top_plan") or {},
        "scientific_executive_summary": (state.get("scientific_hunt_executive") or {}).get("summary") or {},
        "scientific_executive_top_command": (state.get("scientific_hunt_executive") or {}).get("top_command") or {},
        "live_candidate_evidence_top": (state.get("live_candidate_evidence_builder") or {}).get("top_packet") or {},
        "promotion_failure_v3_highest_risk": (state.get("promotion_failure_predictor_v3") or {}).get("highest_risk") or {},
        "evidence_gap_top_task": (state.get("evidence_gap_router") or {}).get("top_task") or {},
        "review_ready_v2_top": (state.get("review_ready_queue_v2") or {}).get("top_ready") or {},
        "promotion_scorecard_top": (state.get("promotion_evidence_scorecard") or {}).get("top_scorecard") or {},
        "candidate_lineage_top": (state.get("candidate_lineage_explainer_v2") or {}).get("top_explanation") or {},
        "live_control_differential_top": (state.get("live_vs_control_differential_report") or {}).get("top_report") or {},
        "promotion_packet_executive_summary": (state.get("promotion_packet_executive") or {}).get("summary") or {},
        "promotion_packet_top_decision": (state.get("promotion_packet_executive") or {}).get("top_decision") or {},
        "unified_learning_summary": (state.get("unified_learning_state_reducer") or {}).get("summary") or {},
        "artifact_priority_top": (state.get("artifact_priority_arbitration_engine") or {}).get("top_artifact") or {},
        "evidence_provenance_top": (state.get("evidence_provenance_graph") or {}).get("top_node") or {},
        "counterfactual_promotion_top": (state.get("counterfactual_promotion_replay") or {}).get("top_replay") or {},
        "candidate_survival_top": (state.get("candidate_survival_simulator") or {}).get("top_simulation") or {},
        "regime_shift_v2": {
            "mode": (state.get("live_regime_shift_detector_v2") or {}).get("mode"),
            "shift_score": (state.get("live_regime_shift_detector_v2") or {}).get("shift_score"),
        },
        "module_trust_top": (state.get("adaptive_trust_weights_per_module") or {}).get("top_module") or {},
        "worker_skill_top": (state.get("worker_skill_elo_v2") or {}).get("top_worker") or {},
        "knowledge_graph_summary": (state.get("route_genome_knowledge_graph") or {}).get("summary") or {},
        "causal_interaction_top": (state.get("causal_feature_interaction_miner") or {}).get("top_interaction") or {},
        "overfit_signature_top": (state.get("overfit_signature_library") or {}).get("top_signature") or {},
        "rejection_memory_top": (state.get("review_rejection_memory_bank_v2") or {}).get("top_memory") or {},
        "learning_budget_top": (state.get("learning_budget_optimizer") or {}).get("top_allocation") or {},
        "novelty_floor_top": (state.get("search_novelty_floor") or {}).get("summary") or {},
        "breakthrough_top": (state.get("breakthrough_escalation_protocol") or {}).get("top_breakthrough") or {},
        "false_discovery_backpressure": {
            "score": (state.get("false_discovery_backpressure_controller") or {}).get("pressure_score"),
            "mode": (state.get("false_discovery_backpressure_controller") or {}).get("mode"),
        },
        "strategy_portfolio_top": (state.get("multi_armed_strategy_portfolio") or {}).get("top_arm") or {},
        "historical_lesson_ab_top": (state.get("historical_lesson_ab_harness") or {}).get("top_test") or {},
        "promotion_packet_diff_top": (state.get("promotion_packet_diff_engine") or {}).get("top_diff") or {},
        "repair_recipe_top": (state.get("candidate_repair_recipe_generator") or {}).get("top_recipe") or {},
        "red_team_top": (state.get("automated_red_team_reviewer") or {}).get("top_objection") or {},
        "field_manual_top_rule": (state.get("learning_compression_field_manual") or {}).get("top_rule") or {},
        "outcome_attribution_top": (state.get("hunt_outcome_attribution_v2") or {}).get("top_attribution") or {},
        "world_state_summary": (state.get("world_state_dashboard_artifact") or {}).get("summary") or {},
        "meta_learning_governor_summary": (state.get("meta_learning_governor") or {}).get("summary") or {},
    })
    state["learning_upgrade_directive"] = directive
    return state


def _load_cross_run_opening_controls(args: argparse.Namespace, run_dir: Path) -> dict[str, Any]:
    if not args.learning_db:
        return {}
    db = step2_learning_db.Step2LearningDB(args.learning_db)
    try:
        global_memory = db.global_learning_memory(limit=50)
    finally:
        db.close()
    controls = step2_online_learning.cross_run_opening_controls(global_memory)
    controls["global_learning_memory"] = global_memory
    repair_continuation = _latest_repair_continuation_controls(run_dir)
    controls.update(repair_continuation)
    for key, value in controls.items():
        if isinstance(value, dict):
            _write_json(run_dir / f"{key}.json", value)
    return controls


def _latest_repair_continuation_controls(run_dir: Path) -> dict[str, Any]:
    root = Path(DEFAULT_OUT)
    if not root.exists():
        return {}
    candidates = sorted(
        [path for path in root.iterdir() if path.is_dir() and path.resolve() != run_dir.resolve()],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for prior in candidates[:12]:
        repair_now = hunt_intel.read_json(prior / "repair_cycle_now_decision.json", {}) or {}
        if not repair_now.get("run_repair_cycle_now"):
            continue
        repair_queue = hunt_intel.read_json(prior / "promotion_evidence_repair_queue.json", {}) or {}
        directives = hunt_intel.read_json(prior / "promotion_repair_worker_directives.json", {}) or {}
        caps = hunt_intel.read_json(prior / "repair_route_budget_caps.json", {}) or {}
        allocation = hunt_intel.read_json(prior / "repair_allocation_by_evidence_weakness.json", {}) or {}
        if not allocation:
            lanes = hunt_intel.read_json(prior / "evidence_repair_lanes.json", {}) or {}
            if lanes:
                allocation = _repair_allocation_by_evidence_weakness(lanes, caps)
        recipe = hunt_intel.read_json(prior / "next_command_recipe.json", {}) or {}
        focus = list(repair_now.get("recommended_focus_routes") or repair_queue.get("focus_routes") or [])[:12]
        if not focus:
            continue
        mode = str(repair_now.get("recommended_hunt_mode") or "promotion_repair")
        action = "loosen_repair" if mode == "repair_exploration" else "repair_first"
        return {
            "auto_hunt_recommendation": {
                "schema_version": 1,
                "source": "latest_repair_continuation_controls",
                "recommended_action": action,
                "next_mode": mode,
                "why": f"Continuing repair-first recommendation from {prior.name}.",
            },
            "promotion_evidence_repair_queue": repair_queue,
            "promotion_repair_worker_directives": directives,
            "repair_route_budget_caps": caps,
            "repair_allocation_by_evidence_weakness": allocation,
            "next_command_recipe": recipe,
            "hunt_opening_playbook": {
                "schema_version": 1,
                "source": "latest_repair_continuation_controls",
                "opening_mode": mode,
                "focus_routes": focus,
                "avoid_routes": [],
                "prior_run_dir": str(prior.resolve()),
            },
            "runtime_command_adapter": {
                "schema_version": 1,
                "source": "latest_repair_continuation_controls",
                "focus_routes": focus,
                "route_budget_caps": caps,
                "repair_allocation_by_evidence_weakness": allocation,
            },
        }
    return {}


def _apply_cross_run_opening_controls(state: dict[str, Any], controls: dict[str, Any]) -> dict[str, Any]:
    state = dict(state or {})
    for key in (
        "cross_run_hunt_memory_compiler",
        "memory_reliability_scorer",
        "memory_falsification_queue",
        "belief_retirement_engine",
        "memory_provenance_explorer",
        "current_vs_historical_disagreement_monitor",
        "memory_stress_test_pack",
        "durable_memory_compression",
        "memory_qa_smoke_test",
        "pre_hunt_strategy_selector",
        "cold_start_route_pack_generator",
        "longitudinal_treatment_decay",
        "run_level_promotion_survival_feedback",
        "memory_conflict_arbiter",
        "hunt_opening_playbook",
        "cross_run_learning_regression_test",
    ):
        value = controls.get(key) if isinstance(controls.get(key), dict) else {}
        if value:
            state[key] = value
    opening = state.get("hunt_opening_playbook") if isinstance(state.get("hunt_opening_playbook"), dict) else {}
    route_pack = state.get("cold_start_route_pack_generator") if isinstance(state.get("cold_start_route_pack_generator"), dict) else {}
    arbiter = state.get("memory_conflict_arbiter") if isinstance(state.get("memory_conflict_arbiter"), dict) else {}
    falsification = state.get("memory_falsification_queue") if isinstance(state.get("memory_falsification_queue"), dict) else {}
    stress = state.get("memory_stress_test_pack") if isinstance(state.get("memory_stress_test_pack"), dict) else {}
    retirement = state.get("belief_retirement_engine") if isinstance(state.get("belief_retirement_engine"), dict) else {}
    disagreement = state.get("current_vs_historical_disagreement_monitor") if isinstance(state.get("current_vs_historical_disagreement_monitor"), dict) else {}
    focus = (
        list(opening.get("focus_routes") or [])
        + list(arbiter.get("safe_focus_routes") or [])
        + list(route_pack.get("focus_routes") or [])
        + list(falsification.get("focus_routes") or [])
        + list(stress.get("focus_routes") or [])
        + list(disagreement.get("falsify_routes") or [])
    )
    avoid = (
        list(opening.get("avoid_routes") or [])
        + list(arbiter.get("avoid_routes") or [])
        + list(route_pack.get("avoid_routes") or [])
        + list(retirement.get("retire_routes") or [])
        + list(disagreement.get("override_history_routes") or [])
    )
    state["pre_hunt_focus_routes"] = list(dict.fromkeys(str(route) for route in focus if str(route)))[:12]
    state["pre_hunt_avoid_routes"] = list(dict.fromkeys(str(route) for route in avoid if str(route)))[:20]
    state["runtime_command_adapter"] = step2_online_learning.runtime_command_adapter(state)
    incoming_adapter = controls.get("runtime_command_adapter") if isinstance(controls.get("runtime_command_adapter"), dict) else {}
    if incoming_adapter:
        adapter = state.get("runtime_command_adapter") if isinstance(state.get("runtime_command_adapter"), dict) else {}
        adapter["focus_routes"] = list(dict.fromkeys(list(incoming_adapter.get("focus_routes") or []) + list(adapter.get("focus_routes") or [])))[:24]
        for key in (
            "route_budget_caps",
            "repair_allocation_by_evidence_weakness",
            "promotion_ready_repair_envelope",
            "edge_preservation_routes",
            "close_sibling_routes",
            "generic_route_deprioritize_patterns",
            "outside_repair_envelope_multiplier",
            "max_sample_share_pct_by_route",
            "max_winner_share_pct_by_route",
        ):
            if incoming_adapter.get(key):
                adapter[key] = incoming_adapter.get(key)
        if incoming_adapter.get("commands"):
            commands = []
            seen_commands: set[tuple[str, str]] = set()
            for command in list(incoming_adapter.get("commands") or []) + list(adapter.get("commands") or []):
                if not isinstance(command, dict):
                    continue
                key = (str(command.get("route_key") or ""), str(command.get("action") or ""))
                if key in seen_commands:
                    continue
                seen_commands.add(key)
                commands.append(command)
            adapter["commands"] = commands[:20]
        state["runtime_command_adapter"] = adapter
    state["worker_job_contracts"] = step2_online_learning.planner_worker_contracts(state)
    directive = state.get("learning_upgrade_directive") if isinstance(state.get("learning_upgrade_directive"), dict) else {}
    directive.update({
        "pre_hunt_strategy": (state.get("pre_hunt_strategy_selector") or {}).get("strategy"),
        "cold_start_focus_routes": state.get("pre_hunt_focus_routes") or [],
        "memory_reliability_top": ((state.get("memory_reliability_scorer") or {}).get("scores") or [{}])[0],
        "memory_falsification_count": len((state.get("memory_falsification_queue") or {}).get("queue") or []),
        "belief_retire_routes": (state.get("belief_retirement_engine") or {}).get("retire_routes") or [],
        "historical_disagreement_count": (state.get("current_vs_historical_disagreement_monitor") or {}).get("disagreement_count"),
        "memory_stress_task_count": len((state.get("memory_stress_test_pack") or {}).get("tasks") or []),
        "durable_memory_rule_count": (state.get("durable_memory_compression") or {}).get("rule_count"),
        "memory_qa_passed": (state.get("memory_qa_smoke_test") or {}).get("passed"),
        "memory_conflict_count": (state.get("memory_conflict_arbiter") or {}).get("conflict_count"),
        "cross_run_regression_passed": (state.get("cross_run_learning_regression_test") or {}).get("passed"),
    })
    state["learning_upgrade_directive"] = directive
    return state


def _run_cycle_process(
    args: argparse.Namespace,
    run_dir: Path,
    cmd: list[str],
    row: dict[str, Any],
    deadline: float,
    prior_online_state: dict[str, Any],
    cycles: list[dict[str, Any]],
) -> dict[str, Any]:
    timeout_sec = min(max(1, int(deadline - time.time())), int(args.cycle_timeout_sec))
    telemetry_path = run_dir / "streaming_telemetry.jsonl"
    telemetry_offset = telemetry_path.stat().st_size if telemetry_path.exists() else 0
    cycle_log_dir = run_dir / "cycle_process_logs"
    cycle_log_dir.mkdir(parents=True, exist_ok=True)
    cycle_id = int(row.get("cycle") or 0)
    stdout_path = cycle_log_dir / f"cycle_{cycle_id:04d}_{row.get('hunter') or 'hunter'}.stdout.log"
    stderr_path = cycle_log_dir / f"cycle_{cycle_id:04d}_{row.get('hunter') or 'hunter'}.stderr.log"
    all_telemetry: list[dict[str, Any]] = []
    validation_requested = False
    stdout_handle = stdout_path.open("a", encoding="utf-8")
    stderr_handle = stderr_path.open("a", encoding="utf-8")
    proc = subprocess.Popen(cmd, cwd=HERE, text=True, stdout=stdout_handle, stderr=stderr_handle)
    started = time.perf_counter()
    last_status = 0.0
    interrupted = False
    interrupt_reason = ""
    completion_reason = "completed"
    try:
        while True:
            now = time.time()
            if int(args.status_interval_sec) > 0 and (last_status <= 0.0 or now - last_status >= int(args.status_interval_sec)):
                last_status = now
                telemetry_now = step2_online_learning.telemetry_summary(all_telemetry)
                _emit_status_update(
                    args,
                    run_dir,
                    _status_snapshot(
                        args,
                        run_dir,
                        phase="cycle_running",
                        cycle_idx=int(row.get("cycle") or 0),
                        hunter=str(row.get("hunter") or ""),
                        deadline=deadline,
                        telemetry_summary=telemetry_now,
                    ),
                )
            if proc.poll() is not None:
                break
            if time.perf_counter() - started > timeout_sec:
                interrupted = True
                if time.time() >= deadline - 1:
                    interrupt_reason = "run_window_elapsed"
                    completion_reason = "partial_valid_artifact"
                else:
                    interrupt_reason = "cycle_timeout"
                    completion_reason = "cycle_timeout"
                _terminate_process_tree(proc, grace_sec=float(args.interrupt_grace_sec))
                break
            events, telemetry_offset = step2_online_learning.read_jsonl_since(telemetry_path, telemetry_offset)
            if events:
                all_telemetry.extend(events)
                state = step2_online_learning.update_state_from_telemetry(
                    run_dir=run_dir,
                    telemetry_events=events,
                    previous_state=step2_online_learning.read_state(run_dir / "online_state.json") or prior_online_state,
                    novelty_budget_pct=float(args.online_novelty_budget_pct),
                )
                state["batch_size_multiplier"] = step2_online_learning.batch_size_multiplier(state)
                _write_compact_json(run_dir / "online_state.json", state)
                _write_runtime_controls(run_dir, state)
                for event in events:
                    wrapped = step2_online_learning.append_event(run_dir, "hunter_batch_telemetry", event)
                    _persist_online_event(args, run_dir, wrapped)
                telemetry_now = step2_online_learning.telemetry_summary(all_telemetry)
                if args.online_micro_validation and not validation_requested and not bool(args.exact_variant_count):
                    trigger_live = int(args.micro_validation_trigger_live_beaters)
                    if int(telemetry_now.get("live_beaters") or 0) >= max(1, trigger_live):
                        validation_requested = True
                        interrupted = True
                        interrupt_reason = "in_cycle_validation_trigger"
                        completion_reason = "interrupted_for_validation"
                        trigger = {
                            "cycle": row.get("cycle"),
                            "reason": interrupt_reason,
                            "telemetry_summary": telemetry_now,
                            "latest_best_variant": (events[-1] or {}).get("best_variant"),
                        }
                        _write_json(run_dir / "stop_signal.json", {"stop": True, "reason": interrupt_reason})
                        wrapped = step2_online_learning.append_event(run_dir, "in_cycle_validation_triggered", trigger)
                        _persist_online_event(args, run_dir, wrapped)
                        time.sleep(float(args.interrupt_grace_sec))
                        _terminate_process_tree(proc, grace_sec=float(args.interrupt_grace_sec))
                        break
                should_stop, reason = step2_online_learning.should_interrupt_from_telemetry(
                    state,
                    all_telemetry,
                    min_events=int(args.interrupt_min_telemetry_events),
                )
                if args.interruptible_cycles and should_stop and not bool(args.exact_variant_count):
                    _write_json(run_dir / "stop_signal.json", {"stop": True, "reason": reason})
                    step2_online_learning.append_event(run_dir, "cycle_interrupt_requested", {
                        "cycle": row.get("cycle"),
                        "reason": reason,
                    })
                    time.sleep(float(args.interrupt_grace_sec))
                    _terminate_process_tree(proc, grace_sec=float(args.interrupt_grace_sec))
                    interrupted = True
                    interrupt_reason = reason
                    completion_reason = "online_interrupt"
                    break
            time.sleep(float(args.telemetry_poll_sec))
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            _terminate_process_tree(proc, grace_sec=float(args.interrupt_grace_sec))
    finally:
        stdout_handle.close()
        stderr_handle.close()
        if proc.poll() is None:
            _terminate_process_tree(proc, grace_sec=float(args.interrupt_grace_sec))
    row.update({
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "interrupted": interrupted,
        "interrupt_reason": interrupt_reason,
        "completion_reason": completion_reason,
        "partial_valid_artifact": completion_reason in {"partial_valid_artifact", "cycle_timeout", "online_interrupt", "interrupted_for_validation"},
        "validation_triggered": validation_requested,
        "streaming_telemetry_events": len(all_telemetry),
        "streaming_telemetry_summary": step2_online_learning.telemetry_summary(all_telemetry),
        "stdout_path": str(stdout_path.resolve()),
        "stderr_path": str(stderr_path.resolve()),
        "stdout_tail": _tail_text(stdout_path, 4000),
        "stderr_tail": _tail_text(stderr_path, 4000),
    })
    return row


def _run_micro_validation(args: argparse.Namespace, run_dir: Path, cycle_idx: int, remaining_sec: int, *, force: bool = False) -> list[dict[str, Any]]:
    if not args.online_micro_validation:
        return []
    if not force and (cycle_idx <= 0 or cycle_idx % int(args.micro_validation_every_cycles) != 0):
        return []
    if remaining_sec < int(args.micro_validation_min_remaining_sec):
        return []
    results = []
    plans = [
        ("counterfactual", run_dir / "counterfactual_route_attribution_plan.json"),
        ("adversarial", run_dir / "adversarial_perturbation_plan.json"),
    ]
    for kind, plan_path in plans:
        plan = hunt_intel.read_json(plan_path, {}) or {}
        if not plan.get("tasks"):
            continue
        cmd = [
            sys.executable,
            str(HERE / "step2_validation_executor.py"),
            "--plan-json",
            str(plan_path.resolve()),
            "--kind",
            kind,
            "--name",
            f"{args.name}_micro_{kind}_{cycle_idx:04d}",
            "--out-dir",
            str(run_dir / "micro_validation"),
            "--limit",
            str(int(args.micro_validation_limit)),
            "--json",
        ]
        t0 = time.perf_counter()
        try:
            proc = subprocess.run(
                cmd,
                cwd=HERE,
                text=True,
                capture_output=True,
                timeout=min(int(args.micro_validation_timeout_sec), max(5, remaining_sec - 5)),
            )
            row = {
                "kind": kind,
                "ok": proc.returncode == 0,
                "returncode": proc.returncode,
                "elapsed_sec": round(time.perf_counter() - t0, 3),
                "stdout_tail": proc.stdout[-2000:],
                "stderr_tail": proc.stderr[-2000:],
            }
            payload = None
            try:
                payload = json.loads(proc.stdout)
            except Exception:
                payload = None
            if isinstance(payload, dict) and payload.get("path"):
                row["result_path"] = payload.get("path")
        except subprocess.TimeoutExpired as exc:
            row = {
                "kind": kind,
                "ok": False,
                "timeout": True,
                "elapsed_sec": round(time.perf_counter() - t0, 3),
                "stdout_tail": (exc.stdout or "")[-2000:] if isinstance(exc.stdout, str) else "",
                "stderr_tail": (exc.stderr or "")[-2000:] if isinstance(exc.stderr, str) else "",
            }
        results.append(row)
        step2_online_learning.append_event(run_dir, "micro_validation_completed", row)
    return results


def _load_cycle_log(run_dir: Path) -> list[dict[str, Any]]:
    payload = hunt_intel.read_json(run_dir / "cycle_log.json", {}) or {}
    return list(payload.get("cycles") or [])


def _compact_run_payload(payload: dict[str, Any]) -> dict[str, Any]:
    def compact_candidate(row: dict[str, Any]) -> dict[str, Any]:
        variant = str(row.get("variant") or "")
        return {
            "variant": row.get("variant"),
            "raw_pnl_rank": raw_rank_by_variant.get(variant),
            "promotion_survival_rank": survival_rank_by_variant.get(variant),
            "step2_pnl": row.get("step2_pnl"),
            "step2_delta_vs_active": row.get("step2_delta_vs_active"),
            "promotion_readiness_score": row.get("promotion_readiness_score"),
            "evidence_adjusted_promotion_readiness_score": row.get("evidence_adjusted_promotion_readiness_score"),
            "promotion_survival_score": row.get("promotion_survival_score") if row.get("promotion_survival_score") is not None else _promotion_survival_score(row),
            "promotion_distance": row.get("promotion_distance"),
            "route_key": hunt_intel.route_key_from_row(row),
            "learning_tags": list(row.get("learning_tags") or [])[:8],
        }

    def compact_evidence_candidate(row: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(row, dict):
            return {}
        return {
            "variant": row.get("variant"),
            "route_key": row.get("route_key"),
            "step2_pnl": row.get("step2_pnl"),
            "delta_vs_active": row.get("delta_vs_active"),
            "robustness_score": row.get("robustness_score"),
            "holdout_delta_vs_active": row.get("holdout_delta_vs_active"),
            "beats_active_day_rate": row.get("beats_active_day_rate"),
            "evidence_adjusted_readiness_score": row.get("evidence_adjusted_readiness_score"),
            "target_repair_lane": row.get("target_repair_lane"),
            "promotion_distance": row.get("promotion_distance"),
            "human_next_step": row.get("human_next_step"),
        }

    brain = payload.get("hunt_brain_summary") if isinstance(payload.get("hunt_brain_summary"), dict) else {}
    gate = payload.get("pre_hunt_stop_go_gate") if isinstance(payload.get("pre_hunt_stop_go_gate"), dict) else {}
    funnel = payload.get("variant_funnel") if isinstance(payload.get("variant_funnel"), dict) else {}
    raw_rank_by_variant = {str(row.get("variant") or ""): idx for idx, row in enumerate(payload.get("top100") or [], 1)}
    survival_rank_by_variant = {str(row.get("variant") or ""): idx for idx, row in enumerate(payload.get("promotion_survival_top100") or [], 1)}
    return {
        "path": payload.get("path"),
        "ok": payload.get("ok"),
        "run_dir": payload.get("run_dir"),
        "top100_count": len(payload.get("top100") or []),
        "top10": [compact_candidate(row) for row in list(payload.get("top10") or [])[:10]],
        "promotion_survival_top10": [compact_candidate(row) for row in list(payload.get("promotion_survival_top10") or [])[:10]],
        "variant_funnel": funnel,
        "stop_go_decision": gate.get("decision"),
        "tiny_run_calibration_guard": payload.get("tiny_run_calibration_guard") or {},
        "post_window_health_report": payload.get("post_window_health_report") or {},
        "lane_learning_summary": payload.get("lane_learning_summary") or {},
        "auto_hunt_recommendation": payload.get("auto_hunt_recommendation") or {},
        "post_test_recommendation_severity": payload.get("post_test_recommendation_severity") or {},
        "top_failure_summary": payload.get("top_failure_summary") or {},
        "promotion_evidence_validation_report": {
            "evaluated_count": ((payload.get("promotion_evidence_validation_report") or {}).get("evaluated_count")),
            "tier_counts": ((payload.get("promotion_evidence_validation_report") or {}).get("tier_counts") or {}),
            "holdout_ready_count": ((payload.get("promotion_evidence_validation_report") or {}).get("holdout_ready_count")),
        },
        "best_evidence_candidate": compact_evidence_candidate((payload.get("near_promotion_evidence_queue") or {}).get("best_evidence_candidate") or {}),
        "best_repair_candidate": payload.get("best_repair_candidate_by_promotion_distance") or ((payload.get("promotion_evidence_repair_queue") or {}).get("tasks") or [{}])[0],
        "raw_vs_promotion_rank_delta": payload.get("raw_vs_promotion_rank_delta") or {},
        "why_raw_rank1_not_promotion_rank1": payload.get("why_raw_rank1_not_promotion_rank1") or {},
        "route_promotion_distance_cards": {
            "closest_route": ((payload.get("route_promotion_distance_cards") or {}).get("closest_route") or {}),
        },
        "per_route_repair_lane_allocation": payload.get("per_route_repair_lane_allocation") or {},
        "next_command_recipe": payload.get("next_command_recipe") or {},
        "repair_success_scoreboard": payload.get("repair_success_scoreboard") or {},
        "repair_narrowness_warning": payload.get("repair_narrowness_warning") or {},
        "repair_progress_report": payload.get("repair_progress_report") or {},
        "promotion_review_precheck": payload.get("promotion_review_precheck") or {},
        "promotion_ready_summary": payload.get("promotion_ready_summary") or {},
        "closest_to_promotion_summary": payload.get("closest_to_promotion_summary") or {},
        "promotion_blocker_digest": payload.get("promotion_blocker_digest") or {},
        "route_concentration_warning": payload.get("route_concentration_warning") or {},
        "winner_route_distribution": payload.get("winner_route_distribution") or {},
        "plan_vs_winner_route_allocation": payload.get("plan_vs_winner_route_allocation") or {},
        "sampled_route_concentration": payload.get("sampled_route_concentration") or {},
        "repair_regression_detector": payload.get("repair_regression_detector") or {},
        "route_admission_gate": payload.get("route_admission_gate") or {},
        "actual_route_allocation": payload.get("actual_route_allocation") or {},
        "plan_vs_actual_route_allocation": payload.get("plan_vs_actual_route_allocation") or {},
        "stale_focus_audit": payload.get("stale_focus_audit") or {},
        "big_run_preflight_gate": payload.get("big_run_preflight_gate") or {},
        "run_size_recommendation": payload.get("run_size_recommendation") or {},
        "before_big_run_issue_register": payload.get("before_big_run_issue_register") or {},
        "next_500_variant_plan": payload.get("next_500_variant_plan") or {},
        "anti_alias_pressure": payload.get("anti_alias_pressure") or {},
        "behavior_unique_generation_controls": payload.get("behavior_unique_generation_controls") or {},
        "promotion_quality_protection_controls": payload.get("promotion_quality_protection_controls") or {},
        "repair_route_budget_caps": payload.get("repair_route_budget_caps") or {},
        "repair_cycle_now_decision": payload.get("repair_cycle_now_decision") or {},
        "runtime_handoff_controls": payload.get("runtime_handoff_controls") or {},
        "hunt_brain_summary": {
            "what_worked": brain.get("what_worked") or [],
            "what_failed": brain.get("what_failed") or [],
            "hunt_next": brain.get("hunt_next") or [],
            "stop_wasting_time_on": brain.get("stop_wasting_time_on") or [],
            "focus_routes": brain.get("focus_routes") or [],
        },
        "artifact_paths": {
            key: (payload.get("artifact_paths") or {}).get(key)
            for key in (
                "variant_funnel",
                "hunt_brain_summary",
                "pre_hunt_stop_go_gate",
                "tiny_run_calibration_guard",
                "promotion_evidence_repair_queue",
                "promotion_repair_worker_directives",
                "promotion_evidence_validation_report",
                "readiness_component_diagnostics",
                "near_promotion_evidence_queue",
                "evidence_repair_lanes",
                "promotion_survival_top100",
                "live_beater_quality_floor_top100",
                "route_action_taxonomy",
                "raw_pnl_vs_promotability_tradeoff",
                "raw_vs_promotion_rank_delta",
                "why_raw_rank1_not_promotion_rank1",
                "route_evidence_cards",
                "route_promotion_distance_cards",
                "per_route_repair_lane_allocation",
                "separate_learning_leaderboards",
                "suspicious_winner_detector",
                "anti_alias_pressure",
                "behavior_unique_generation_controls",
                "promotion_quality_protection_controls",
                "repair_route_budget_caps",
                "repair_allocation_by_evidence_weakness",
                "best_repair_candidate_by_promotion_distance",
                "repair_narrowness_warning",
                "repair_progress_report",
                "promotion_review_precheck",
                "promotion_ready_summary",
                "closest_to_promotion_summary",
                "promotion_blocker_digest",
                "route_concentration_warning",
                "winner_route_distribution",
                "plan_vs_winner_route_allocation",
                "sampled_route_concentration",
                "repair_regression_detector",
                "route_admission_gate",
                "runtime_handoff_controls",
                "actual_route_allocation",
                "plan_vs_actual_route_allocation",
                "stale_focus_audit",
                "big_run_preflight_gate",
                "run_size_recommendation",
                "before_big_run_issue_register",
                "big_run_launch_controls",
                "next_command_recipe",
                "repair_success_scoreboard",
                "repair_lane_performance_scoreboard",
                "next_500_variant_plan",
                "top_failure_summary",
                "auto_hunt_recommendation",
                "post_test_recommendation_severity",
                "repair_cycle_now_decision",
            )
            if (payload.get("artifact_paths") or {}).get(key)
        },
    }


def _digest_run_payload(payload: dict[str, Any]) -> dict[str, Any]:
    artifacts = payload.get("artifact_paths") if isinstance(payload.get("artifact_paths"), dict) else {}
    digest = payload.get("operator_digest") if isinstance(payload.get("operator_digest"), dict) else {}
    funnel = payload.get("variant_funnel") if isinstance(payload.get("variant_funnel"), dict) else {}
    failure = payload.get("top_failure_summary") if isinstance(payload.get("top_failure_summary"), dict) else {}
    next_recipe = payload.get("next_command_recipe") if isinstance(payload.get("next_command_recipe"), dict) else {}
    speed = payload.get("speed_diagnostics") if isinstance(payload.get("speed_diagnostics"), dict) else {}
    budget = payload.get("artifact_budget_report") if isinstance(payload.get("artifact_budget_report"), dict) else {}
    top = list(payload.get("top100") or [])
    survival = list(payload.get("promotion_survival_top100") or [])
    return {
        "ok": payload.get("ok"),
        "path": payload.get("path"),
        "run_dir": payload.get("run_dir"),
        "operator_digest": artifacts.get("operator_digest") or str(Path(str(payload.get("run_dir") or ".")) / "operator_digest.json"),
        "final_summary": artifacts.get("final_summary") or payload.get("path"),
        "speed_diagnostics": artifacts.get("speed_diagnostics"),
        "loop_progress_summary": artifacts.get("loop_progress_summary"),
        "variant_funnel_path": artifacts.get("variant_funnel"),
        "top_failure_summary_path": artifacts.get("top_failure_summary"),
        "speed_summary": {
            "wall_elapsed_sec": speed.get("wall_elapsed_sec"),
            "cycle_elapsed_sec_total": speed.get("cycle_elapsed_sec_total"),
            "scored_per_cycle_sec": speed.get("scored_per_cycle_sec"),
            "artifact_total_mb": budget.get("total_mb") or speed.get("artifact_total_mb"),
            "artifact_file_count": budget.get("file_count") or speed.get("artifact_file_count"),
            "status_event_count": speed.get("status_event_count"),
            "score_cache_hit_rate_pct": ((speed.get("score_cache_observed") or {}).get("score_cache_hit_rate_pct") if isinstance(speed.get("score_cache_observed"), dict) else None),
        },
        "top100_count": len(top) if top else payload.get("top100_count"),
        "variant_funnel": {
            key: funnel.get(key)
            for key in (
                "requested_variants",
                "scored_total",
                "candidate_rows_collected",
                "live_beaters_streamed",
                "live_filtered_rows",
                "behavior_unique_rows",
                "reported_winners",
                "count_contract_ok",
                "runtime_control_mode",
            )
            if key in funnel
        },
        "top_candidate": _compact_candidate_digest(top[0] if top else {}),
        "promotion_survival_rank1": _compact_candidate_digest(survival[0] if survival else {}),
        "failure_summary": {
            "human_summary": failure.get("human_summary"),
            "promotion_ready_count": failure.get("promotion_ready_count"),
            "repair_task_count": failure.get("repair_task_count"),
            "top_failure_reasons": (failure.get("top_failure_reasons") or [])[:6],
        },
        "learning": digest.get("learning") if isinstance(digest.get("learning"), dict) else {},
        "next_command_recipe": {
            "recommended_mode": next_recipe.get("recommended_mode"),
            "recommended_run_size": next_recipe.get("recommended_run_size"),
            "runtime_bootstrap_json": next_recipe.get("runtime_bootstrap_json"),
            "command_string": next_recipe.get("command_string"),
        },
    }


def _lane_learning_summary(rows: list[dict[str, Any]], cycles: list[dict[str, Any]]) -> dict[str, Any]:
    by_route: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_route[_route_key(row)].append(row)
    lanes = []
    for route, lane_rows in by_route.items():
        lane_rows = sorted(lane_rows, key=lambda item: float(item.get("step2_pnl") or 0.0), reverse=True)
        top = lane_rows[0] if lane_rows else {}
        promotion_candidates = [row for row in lane_rows if _promotion_passed(row) and _safety_status(row) == "ok"]
        lanes.append({
            "route_key": route,
            "count": len(lane_rows),
            "best_variant": top.get("variant"),
            "best_pnl": top.get("step2_pnl"),
            "best_delta_vs_live": top.get("step2_delta_vs_active"),
            "best_readiness": top.get("promotion_readiness_score"),
            "promotion_candidate_count": len(promotion_candidates),
            "safety_blocked_count": sum(1 for row in lane_rows if _safety_status(row) == "blocked"),
            "decision": (
                "expand"
                if len(lane_rows) >= 3 and float(top.get("step2_delta_vs_active") or 0.0) > 1500
                else "validate"
                if len(lane_rows) >= 2
                else "probe"
            ),
            "learning_label": (
                "new_contender_lane_needs_validation"
                if len(lane_rows) <= 2 and float(top.get("step2_delta_vs_active") or 0.0) > 1500
                else "repeatable_lane"
                if len(lane_rows) >= 3
                else "single_run_signal"
            ),
        })
    lanes = sorted(lanes, key=lambda row: (float(row.get("best_pnl") or 0.0), int(row.get("count") or 0)), reverse=True)
    failed_cycles = [cycle for cycle in cycles if cycle.get("ok") is False and not cycle.get("partial_valid_artifact")]
    partial_cycles = [cycle for cycle in cycles if cycle.get("partial_valid_artifact")]
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "lane_count": len(lanes),
        "lanes": lanes[:25],
        "kill_keep_expand": {
            "expand": [lane for lane in lanes if lane.get("decision") == "expand"][:10],
            "validate": [lane for lane in lanes if lane.get("decision") == "validate"][:10],
            "probe": [lane for lane in lanes if lane.get("decision") == "probe"][:10],
        },
        "what_changed_since_last_window": [
            f"{len(rows)} live-beating behavior-unique rows retained",
            f"{len(partial_cycles)} partial-valid cycles and {len(failed_cycles)} failed cycles observed",
            f"top lane: {(lanes[0] or {}).get('route_key') if lanes else 'none'}",
        ],
        "what_failed": [
            f"cycle {cycle.get('cycle')} {cycle.get('hunter')} failed: {cycle.get('interrupt_reason') or cycle.get('returncode')}"
            for cycle in failed_cycles[:8]
        ],
        "next_best_experiments": [
            {
                "route_key": lane.get("route_key"),
                "action": "validate_repeatability" if lane.get("learning_label") == "new_contender_lane_needs_validation" else lane.get("decision"),
                "why": lane.get("learning_label"),
            }
            for lane in lanes[:8]
        ],
    }


def _post_window_health_report(
    args: argparse.Namespace,
    run_dir: Path,
    cycles: list[dict[str, Any]],
    top100: list[dict[str, Any]],
    variant_funnel: dict[str, Any],
    artifact_paths: dict[str, str],
) -> dict[str, Any]:
    failed_cycles = [cycle for cycle in cycles if cycle.get("ok") is False and not cycle.get("partial_valid_artifact")]
    partial_cycles = [cycle for cycle in cycles if cycle.get("partial_valid_artifact")]
    missing_artifacts = [
        key
        for key, path in artifact_paths.items()
        if path and not Path(str(path)).exists()
    ]
    clean_rows = [row for row in top100 if _safety_status(row) != "blocked"]
    promotion_rows = [row for row in clean_rows if _promotion_passed(row)]
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "run_dir": str(run_dir.resolve()),
        "native_window_control": {
            "stop_after_sec": int(getattr(args, "stop_after_sec", 0) or 0),
            "window_minutes": float(getattr(args, "window_minutes", 0.0) or 0.0),
            "used_native_window": int(getattr(args, "stop_after_sec", 0) or 0) > 0 or float(getattr(args, "window_minutes", 0.0) or 0.0) > 0,
        },
        "cycle_count": len(cycles),
        "failed_cycle_count": len(failed_cycles),
        "partial_valid_cycle_count": len(partial_cycles),
        "artifact_completeness": {
            "missing_artifact_count": len(missing_artifacts),
            "missing_artifacts": missing_artifacts[:20],
            "has_promotion_survival_top100": (run_dir / "promotion_survival_top100.json").exists(),
            "has_runtime_handoff_controls": (run_dir / "runtime_handoff_controls.json").exists(),
        },
        "top100_health": {
            "top100_count": len(top100),
            "clean_candidate_count": len(clean_rows),
            "promotion_distance_passed_count": len(promotion_rows),
            "safety_blocked_count": len(top100) - len(clean_rows),
            "best_clean_candidate": _status_candidate(clean_rows[0]) if clean_rows else {},
            "best_promotion_candidate": _status_candidate(promotion_rows[0]) if promotion_rows else {},
        },
        "variant_funnel": variant_funnel,
        "recommendation": (
            "review_best_promotion_candidate"
            if promotion_rows
            else "validate_best_clean_candidate"
            if clean_rows
            else "repair_safety_before_review"
        ),
    }


def _compact_candidate_digest(row: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(row, dict) or not row:
        return {}
    distance = row.get("promotion_distance") if isinstance(row.get("promotion_distance"), dict) else {}
    if not distance:
        distance = _promotion_distance(row)
    return {
        "variant": row.get("variant") or row.get("name"),
        "route_key": hunt_intel.route_key_from_row(row),
        "step2_pnl": row.get("step2_pnl"),
        "step2_delta_vs_active": row.get("step2_delta_vs_active") or row.get("delta_vs_active"),
        "promotion_readiness_score": row.get("promotion_readiness_score"),
        "promotion_survival_score": row.get("promotion_survival_score"),
        "promotion_distance_passed": distance.get("passed"),
        "blocking_gates": [
            gate.get("gate")
            for gate in (distance.get("open_gates") or distance.get("gates") or [])
            if isinstance(gate, dict) and gate.get("passed") is False
        ][:6],
        "learning_tags": list(row.get("learning_tags") or [])[:8],
    }


def _artifact_budget_report(run_dir: Path, *, warning_mb: float = 10.0, limit: int = 40) -> dict[str, Any]:
    files = []
    total = 0
    warning_bytes = int(max(0.0, float(warning_mb)) * 1024 * 1024)
    for path in run_dir.rglob("*"):
        if not path.is_file():
            continue
        try:
            size = path.stat().st_size
        except OSError:
            continue
        total += size
        rel = str(path.relative_to(run_dir))
        files.append({
            "path": rel,
            "bytes": size,
            "kb": round(size / 1024.0, 2),
            "mb": round(size / (1024.0 * 1024.0), 3),
            "warning": warning_bytes > 0 and size >= warning_bytes,
        })
    files.sort(key=lambda item: int(item.get("bytes") or 0), reverse=True)
    warnings = [item for item in files if item.get("warning")]
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "file_count": len(files),
        "total_bytes": total,
        "total_mb": round(total / (1024.0 * 1024.0), 3),
        "warning_threshold_mb": float(warning_mb),
        "warning_count": len(warnings),
        "largest_files": files[:limit],
        "review_guidance": [
            "Start with operator_digest.json, speed_diagnostics.json, and variant_funnel.json.",
            "Open final_summary.json only when the digest points to a specific raw-evidence question.",
        ],
    }


_COMPACT_FINAL_SUMMARY_KEEP_KEYS = {
    "artifact_budget_report",
    "artifact_paths",
    "completed_artifact_archive",
    "auto_hunt_recommendation",
    "big_run_preflight_gate",
    "cycles",
    "failure_summary",
    "learning_notes",
    "learning_roi_report",
    "loop_progress_summary",
    "next_command_recipe",
    "ok",
    "operator_digest",
    "path",
    "post_test_recommendation_severity",
    "promotion_ready_summary",
    "promotion_survival_top10",
    "ranking_stats",
    "repair_cycle_now_decision",
    "run_dir",
    "run_to_run_comparison",
    "speed_diagnostics",
    "status_policy",
    "stop_go_decision",
    "tiny_run_calibration_guard",
    "top10",
    "top100_count",
    "top_failure_summary",
    "variant_funnel",
}


_FINAL_SUMMARY_KEY_ARTIFACT_PATHS = (
    "operator_digest",
    "speed_diagnostics",
    "artifact_budget_report",
    "variant_funnel",
    "top_failure_summary",
    "promotion_ready_summary",
    "promotion_survival_top100",
    "running_top100",
    "learning_roi_report",
    "loop_progress_summary",
    "next_command_recipe",
    "runtime_handoff_controls",
    "finalists",
    "annotated_top100",
    "artifact_manifest",
    "completed_artifact_archive_manifest",
)


_HOT_RECEIPT_ARTIFACT_NAMES = {
    "actual_route_allocation.json",
    "anti_alias_pressure.json",
    "artifact_budget_report.json",
    "artifact_manifest.json",
    "auto_hunt_recommendation.json",
    "before_big_run_issue_register.json",
    "behavior_unique_generation_controls.json",
    "big_run_launch_controls.json",
    "big_run_preflight_gate.json",
    "big_run_truth_summary.json",
    "completed_artifact_archive_manifest.json",
    "cycle_log.json",
    "final_summary.json",
    "finalists.json",
    "hunt_brain_summary.json",
    "learning_roi_report.json",
    "loop_progress_summary.json",
    "next_500_variant_plan.json",
    "next_command_recipe.json",
    "operator_digest.json",
    "plan_vs_actual_route_allocation.json",
    "post_test_recommendation_severity.json",
    "promotion_blocker_digest.json",
    "promotion_quality_protection_controls.json",
    "promotion_ready_summary.json",
    "promotion_review_precheck.json",
    "promotion_survival_top100.json",
    "repair_allocation_by_evidence_weakness.json",
    "repair_cycle_now_decision.json",
    "repair_progress_report.json",
    "repair_regression_detector.json",
    "repair_route_budget_caps.json",
    "route_admission_gate.json",
    "route_concentration_warning.json",
    "run_size_recommendation.json",
    "running_top100.json",
    "runtime_bootstrap_controls.json",
    "runtime_handoff_controls.json",
    "sampled_route_concentration.json",
    "speed_diagnostics.json",
    "stale_focus_audit.json",
    "status_policy.json",
    "top_failure_summary.json",
    "variant_funnel.json",
    "winner_route_distribution.json",
}


def _safe_relative_path(path: Path, root: Path) -> str:
    resolved = path.resolve()
    for base in (root.resolve(), HERE.resolve()):
        try:
            return resolved.relative_to(base).as_posix()
        except ValueError:
            continue
    return str(resolved)


def _completed_run_archive_dir(args: argparse.Namespace, run_dir: Path) -> Path:
    raw = str(getattr(args, "okay_to_delete_dir", "okay_to_delete") or "okay_to_delete").strip()
    archive_root = Path(raw)
    if not archive_root.is_absolute():
        archive_root = HERE / archive_root
    archive_root = archive_root.resolve()
    workspace = HERE.resolve()
    if archive_root != workspace / "okay_to_delete" and workspace not in archive_root.parents:
        raise ValueError(f"okay_to_delete archive root must stay inside the workspace: {archive_root}")
    run_root = run_dir.resolve()
    try:
        run_key = run_root.relative_to(workspace)
    except ValueError:
        run_key = Path(run_root.name)
    return archive_root / run_key


def _archive_completed_run_artifacts(
    args: argparse.Namespace,
    run_dir: Path,
    artifact_paths: dict[str, str] | None = None,
) -> dict[str, Any]:
    if not bool(getattr(args, "archive_completed_artifacts", True)):
        return {
            "schema_version": 1,
            "source": "run_step2_three_hour_hunt",
            "enabled": False,
            "reason": "archive_completed_artifacts_disabled",
        }
    if not run_dir.exists():
        return {
            "schema_version": 1,
            "source": "run_step2_three_hour_hunt",
            "enabled": True,
            "error": "run_dir_missing",
            "run_dir": str(run_dir.resolve()),
        }

    archive_dir = _completed_run_archive_dir(args, run_dir)
    run_root = run_dir.resolve()
    if archive_dir == run_root or archive_dir in run_root.parents:
        raise ValueError(f"okay_to_delete archive cannot be the run directory or one of its parents: {archive_dir}")
    if run_root in archive_dir.parents:
        raise ValueError(f"okay_to_delete archive must live outside the hot run directory: {archive_dir}")
    archive_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = run_dir / "completed_artifact_archive_manifest.json"
    keep_names = set(_HOT_RECEIPT_ARTIFACT_NAMES)
    keep_names.add(manifest_path.name)
    moved: list[dict[str, Any]] = []
    kept: list[str] = []
    errors: list[dict[str, str]] = []

    for item in sorted(run_dir.iterdir(), key=lambda path: path.name.lower()):
        if item.name == archive_dir.name or item.name in keep_names:
            kept.append(item.name)
            continue
        if item.resolve() == archive_dir or archive_dir in item.resolve().parents:
            continue
        dest = archive_dir / item.name
        if dest.exists():
            suffix = datetime.now(CT).strftime("%Y%m%d_%H%M%S")
            dest = archive_dir / f"{item.stem}_{suffix}{item.suffix}" if item.is_file() else archive_dir / f"{item.name}_{suffix}"
        try:
            bytes_moved = 0
            file_count = 0
            if item.is_file():
                bytes_moved = item.stat().st_size
                file_count = 1
            elif item.is_dir():
                for child in item.rglob("*"):
                    if child.is_file():
                        file_count += 1
                        bytes_moved += child.stat().st_size
            shutil.move(str(item), str(dest))
            moved.append({
                "from": _safe_relative_path(item, run_dir),
                "to": _safe_relative_path(dest, run_dir),
                "kind": "dir" if dest.is_dir() else "file",
                "bytes": bytes_moved,
                "file_count": file_count,
            })
        except Exception as exc:
            errors.append({"path": str(item), "error": str(exc)})

    payload = {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "enabled": True,
        "updated_at_ct": _now_ct(),
        "run_dir": str(run_dir.resolve()),
        "archive_dir": str(archive_dir),
        "archive_dir_name": archive_dir.name,
        "hot_receipt_file_count": len([name for name in keep_names if (run_dir / name).exists()]),
        "kept_hot_receipts": sorted(name for name in keep_names if (run_dir / name).exists()),
        "moved_count": len(moved),
        "moved_file_count": sum(int(row.get("file_count") or 0) for row in moved),
        "moved_total_bytes": sum(int(row.get("bytes") or 0) for row in moved),
        "moved_total_mb": round(sum(int(row.get("bytes") or 0) for row in moved) / (1024.0 * 1024.0), 3),
        "moved": moved,
        "errors": errors,
        "artifact_paths_before_archive": dict(sorted((artifact_paths or {}).items())),
        "delete_guidance": [
            "The okay_to_delete folder is cold storage for bulky/debug artifacts after the run receipt is written.",
            "Future runs should bootstrap from the hot receipt files in the run directory, especially runtime_handoff_controls.json.",
            "Deleting okay_to_delete reduces audit/replay depth for that run but should not block the next hunt.",
        ],
    }
    _write_json(manifest_path, payload)
    return payload


def _remap_archived_cycle_log_paths(run_dir: Path, archive_report: dict[str, Any]) -> dict[str, Any]:
    """Keep hot cycle_log pointers usable after cold artifacts are archived."""
    if not archive_report.get("enabled") or not archive_report.get("moved"):
        return {
            "schema_version": 1,
            "source": "run_step2_three_hour_hunt",
            "updated": False,
            "reason": "no_archived_paths",
        }
    cycle_log_path = run_dir / "cycle_log.json"
    payload = hunt_intel.read_json(cycle_log_path, {}) or {}
    cycles = payload.get("cycles") if isinstance(payload.get("cycles"), list) else []
    if not cycles:
        return {
            "schema_version": 1,
            "source": "run_step2_three_hour_hunt",
            "updated": False,
            "reason": "cycle_log_missing_or_empty",
        }
    archive_dir = Path(str(archive_report.get("archive_dir") or "")).resolve()
    path_map: dict[str, str] = {}
    for moved in archive_report.get("moved") or []:
        if not isinstance(moved, dict):
            continue
        source_name = str(moved.get("from") or "").replace("\\", "/").split("/", 1)[0]
        dest_rel = str(moved.get("to") or "")
        if not source_name or not dest_rel:
            continue
        source_abs = (run_dir / source_name).resolve()
        if dest_rel.replace("\\", "/").startswith("okay_to_delete/") or os.path.isabs(dest_rel):
            dest_abs = (HERE / dest_rel).resolve() if not os.path.isabs(dest_rel) else Path(dest_rel).resolve()
        else:
            dest_abs = (archive_dir / Path(dest_rel).name).resolve()
        path_map[str(source_abs)] = str(dest_abs)

    def remap_value(value: Any) -> Any:
        if not isinstance(value, str) or not value:
            return value
        try:
            raw_path = Path(value)
        except Exception:
            return value
        if not raw_path.is_absolute():
            return value
        try:
            resolved = raw_path.resolve()
        except Exception:
            return value
        for source_abs, dest_abs in path_map.items():
            source_path = Path(source_abs)
            try:
                suffix = resolved.relative_to(source_path)
            except ValueError:
                continue
            return str((Path(dest_abs) / suffix).resolve())
        return value

    remapped = 0
    for cycle in cycles:
        if not isinstance(cycle, dict):
            continue
        for key in ("stdout_path", "stderr_path"):
            before = cycle.get(key)
            after = remap_value(before)
            if after != before:
                cycle[key] = after
                remapped += 1
        summary = cycle.get("coordinator_summary")
        if isinstance(summary, dict):
            before = summary.get("path")
            after = remap_value(before)
            if after != before:
                summary["path"] = after
                remapped += 1
    if remapped:
        payload["archive_path_remap"] = {
            "schema_version": 1,
            "source": "run_step2_three_hour_hunt",
            "archive_dir": str(archive_dir),
            "remapped_count": remapped,
            "updated_at_ct": _now_ct(),
        }
        _write_json(cycle_log_path, payload)
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "updated": bool(remapped),
        "remapped_count": remapped,
    }


def _write_artifact_manifest(run_dir: Path, artifact_paths: dict[str, str]) -> str:
    payload = {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "updated_at_ct": _now_ct(),
        "artifact_count": len(artifact_paths or {}),
        "artifact_paths": dict(sorted((artifact_paths or {}).items())),
    }
    return _write_compact_json(run_dir / "artifact_manifest.json", payload)


def _compact_artifact_paths_for_summary(artifact_paths: dict[str, str]) -> dict[str, Any]:
    paths = dict(artifact_paths or {})
    return {
        "artifact_count": len(paths),
        "artifact_manifest": paths.get("artifact_manifest"),
        "key_paths": {
            key: paths.get(key)
            for key in _FINAL_SUMMARY_KEY_ARTIFACT_PATHS
            if paths.get(key)
        },
        "full_path_map": paths.get("artifact_manifest") or "artifact_manifest.json",
    }


def _score_cache_observed_counts(run_dir: Path) -> dict[str, Any]:
    hit = miss = unknown = rows = 0
    paths = sorted(run_dir.glob("*_router/scored_variants.jsonl"))
    for path in paths:
        for row in _read_jsonl(path):
            rows += 1
            cache = row.get("score_cache") if isinstance(row.get("score_cache"), dict) else {}
            if cache.get("hit") is True:
                hit += 1
            elif cache.get("hit") is False:
                miss += 1
            else:
                unknown += 1
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "scored_variant_rows": rows,
        "score_cache_hits": hit,
        "score_cache_misses": miss,
        "score_cache_unknown": unknown,
        "score_cache_hit_rate_pct": round((hit / rows) * 100.0, 4) if rows else None,
        "source_paths": [str(path.resolve()) for path in paths],
    }


def _loop_campaign_from_run_dir(run_dir: Path) -> dict[str, Any]:
    name = run_dir.name
    marker = "_r"
    if marker in name:
        prefix = name.split(marker, 1)[0] + marker
    else:
        prefix = "loop100_r"
    digits = "".join(ch for ch in prefix if ch.isdigit())
    target = int(digits) if digits else 1000
    return {
        "prefix": prefix,
        "target_scored_total": max(100, target),
    }


def _loop_progress_summary(run_dir: Path) -> dict[str, Any]:
    root = run_dir.parent
    campaign = _loop_campaign_from_run_dir(run_dir)
    prefix = str(campaign.get("prefix") or "loop100_r")
    target_scored_total = int(campaign.get("target_scored_total") or 1000)
    rows = []
    if root.exists():
        for path in sorted(root.iterdir(), key=_run_dir_sort_key):
            if not path.is_dir() or not path.name.startswith(prefix):
                continue
            funnel = hunt_intel.read_json(path / "variant_funnel.json", {}) or {}
            if not funnel:
                continue
            failure = hunt_intel.read_json(path / "top_failure_summary.json", {}) or {}
            speed = hunt_intel.read_json(path / "speed_diagnostics.json", {}) or {}
            budget = hunt_intel.read_json(path / "artifact_budget_report.json", {}) or {}
            cycle_log = hunt_intel.read_json(path / "cycle_log.json", {}) or {}
            cycles = cycle_log.get("cycles") if isinstance(cycle_log.get("cycles"), list) else []
            operator = hunt_intel.read_json(path / "operator_digest.json", {}) or {}
            next_recipe = operator.get("next_command_recipe") if isinstance(operator.get("next_command_recipe"), dict) else {}
            rows.append({
                "run": path.name,
                "run_dir": str(path.resolve()),
                "hunt_mode": (cycles[0] or {}).get("hunt_mode") if cycles else None,
                "recommended_next_mode": next_recipe.get("recommended_mode"),
                "scored_total": int(funnel.get("scored_total") or 0),
                "count_contract_ok": bool(funnel.get("count_contract_ok")),
                "live_beaters_streamed": int(funnel.get("live_beaters_streamed") or 0),
                "behavior_unique_rows": int(funnel.get("behavior_unique_rows") or 0),
                "scored_to_live_stream_yield_pct": funnel.get("scored_to_live_stream_yield_pct"),
                "scored_to_behavior_unique_yield_pct": funnel.get("scored_to_behavior_unique_yield_pct"),
                "promotion_ready_count": int(failure.get("promotion_ready_count") or 0),
                "repair_task_count": int(failure.get("repair_task_count") or 0),
                "suspicious_winner_count": int(failure.get("suspicious_winner_count") or 0),
                "evidence_tier_counts": failure.get("evidence_tier_counts") if isinstance(failure.get("evidence_tier_counts"), dict) else {},
                "small_edge_cushion": failure.get("small_edge_cushion") if isinstance(failure.get("small_edge_cushion"), dict) else {},
                "top_failure_reasons": (failure.get("top_failure_reasons") or [])[:3],
                "wall_elapsed_sec": speed.get("wall_elapsed_sec"),
                "cycle_elapsed_sec_total": speed.get("cycle_elapsed_sec_total"),
                "score_cache_hit_rate_pct": ((speed.get("score_cache_observed") or {}).get("score_cache_hit_rate_pct") if isinstance(speed.get("score_cache_observed"), dict) else None),
                "artifact_total_mb": budget.get("total_mb"),
            })
    total_scored = sum(int(row.get("scored_total") or 0) for row in rows)
    measured_wall = [float(row.get("wall_elapsed_sec") or 0.0) for row in rows if row.get("wall_elapsed_sec") is not None]
    measured_artifacts = [float(row.get("artifact_total_mb") or 0.0) for row in rows if row.get("artifact_total_mb") is not None]
    measured_cache = [float(row.get("score_cache_hit_rate_pct") or 0.0) for row in rows if row.get("score_cache_hit_rate_pct") is not None]
    latest = rows[-1] if rows else {}
    best_by_ready = max(rows, key=lambda row: int(row.get("promotion_ready_count") or 0), default={})
    best_by_behavior_unique = max(rows, key=lambda row: int(row.get("behavior_unique_rows") or 0), default={})
    fastest_by_wall = min(
        (row for row in rows if row.get("wall_elapsed_sec") is not None),
        key=lambda row: float(row.get("wall_elapsed_sec") or 0.0),
        default={},
    )
    progress_pct = round((total_scored / target_scored_total) * 100.0, 3) if target_scored_total else None
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "updated_at_ct": _now_ct(),
        "current_run_dir": str(run_dir.resolve()),
        "campaign_prefix": prefix,
        "target_scored_total": target_scored_total,
        "runs_completed": len(rows),
        "cumulative_scored_total": total_scored,
        "target_progress_pct": progress_pct,
        "remaining_to_target": max(0, target_scored_total - total_scored),
        "remaining_to_1000": max(0, 1000 - total_scored) if target_scored_total == 1000 else None,
        "all_count_contracts_ok": all(bool(row.get("count_contract_ok")) for row in rows) if rows else False,
        "promotion_ready_counts": [row.get("promotion_ready_count") for row in rows],
        "best_promotion_ready_count": max([int(row.get("promotion_ready_count") or 0) for row in rows] or [0]),
        "best_run_by_promotion_ready": {
            "run": best_by_ready.get("run"),
            "promotion_ready_count": best_by_ready.get("promotion_ready_count"),
            "behavior_unique_rows": best_by_ready.get("behavior_unique_rows"),
            "recommended_next_mode": best_by_ready.get("recommended_next_mode"),
        } if best_by_ready else {},
        "best_run_by_behavior_unique": {
            "run": best_by_behavior_unique.get("run"),
            "behavior_unique_rows": best_by_behavior_unique.get("behavior_unique_rows"),
            "promotion_ready_count": best_by_behavior_unique.get("promotion_ready_count"),
            "recommended_next_mode": best_by_behavior_unique.get("recommended_next_mode"),
        } if best_by_behavior_unique else {},
        "fastest_run_by_wall": {
            "run": fastest_by_wall.get("run"),
            "wall_elapsed_sec": fastest_by_wall.get("wall_elapsed_sec"),
            "promotion_ready_count": fastest_by_wall.get("promotion_ready_count"),
            "score_cache_hit_rate_pct": fastest_by_wall.get("score_cache_hit_rate_pct"),
        } if fastest_by_wall else {},
        "total_suspicious_winner_count": sum(int(row.get("suspicious_winner_count") or 0) for row in rows),
        "latest_suspicious_winner_count": int(latest.get("suspicious_winner_count") or 0) if latest else 0,
        "latest_hunt_mode": latest.get("hunt_mode"),
        "latest_recommended_next_mode": latest.get("recommended_next_mode"),
        "latest_evidence_tier_counts": latest.get("evidence_tier_counts") or {},
        "latest_top_failure_reasons": latest.get("top_failure_reasons") or [],
        "timed_run_count": len(measured_wall),
        "avg_wall_elapsed_sec": round(sum(measured_wall) / len(measured_wall), 3) if measured_wall else None,
        "artifact_measured_run_count": len(measured_artifacts),
        "avg_artifact_total_mb": round(sum(measured_artifacts) / len(measured_artifacts), 3) if measured_artifacts else None,
        "cache_measured_run_count": len(measured_cache),
        "avg_score_cache_hit_rate_pct": round(sum(measured_cache) / len(measured_cache), 3) if measured_cache else None,
        "runs": rows,
    }


def _final_summary_payload_for_write(
    args: argparse.Namespace,
    payload: dict[str, Any],
    artifact_paths: dict[str, str],
) -> dict[str, Any]:
    if str(getattr(args, "artifact_profile", "compact") or "compact") == "full":
        return payload
    compact = dict(payload)
    if isinstance(compact.get("artifact_paths"), dict):
        compact["artifact_paths"] = _compact_artifact_paths_for_summary(artifact_paths)
    omitted: dict[str, dict[str, Any]] = {}
    for key, path in sorted((artifact_paths or {}).items()):
        if key in _COMPACT_FINAL_SUMMARY_KEEP_KEYS or key not in compact or not path:
            continue
        value = compact.get(key)
        if not isinstance(value, (dict, list)) or not value:
            continue
        compact[key] = {
            "artifact_path": path,
            "omitted_from_compact_final_summary": True,
            "reason": "standalone_artifact_preserves_full_payload",
        }
        omitted[key] = {
            "artifact_path": path,
            "type": type(value).__name__,
            "count": len(value) if hasattr(value, "__len__") else None,
        }
    compact["compact_final_summary"] = {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "active": True,
        "omitted_duplicate_artifact_count": len(omitted),
        "omitted_duplicate_artifacts": omitted,
        "full_data_contract": "No analysis is discarded; omitted top-level payloads are preserved in their standalone artifact_path files.",
    }
    return compact


def _speed_diagnostics_report(
    args: argparse.Namespace,
    run_dir: Path,
    cycles: list[dict[str, Any]],
    artifact_budget: dict[str, Any] | None = None,
    *,
    started_at: float | None = None,
) -> dict[str, Any]:
    status_events = [row for row in _read_jsonl(run_dir / "learning_events.jsonl") if row.get("event_type") == "hunt_status"]
    elapsed_values = [float(cycle.get("elapsed_sec") or 0.0) for cycle in cycles if cycle.get("elapsed_sec") is not None]
    scored_values = [_cycle_scored_total(cycle) for cycle in cycles]
    total_elapsed = sum(max(0.0, value) for value in elapsed_values)
    total_scored = sum(max(0, value) for value in scored_values)
    largest = list((artifact_budget or {}).get("largest_files") or [])[:12]
    cache_observed = _score_cache_observed_counts(run_dir)
    command_flags = []
    for cycle in cycles:
        cmd = list(cycle.get("cmd") or [])
        for flag in (
            "--coordinator-artifact-mode",
            "--score-cache-stats-mode",
            "--exact-variant-count",
            "--allow-uncertified-cache",
        ):
            if flag in cmd and flag not in command_flags:
                command_flags.append(flag)
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "run_dir": str(run_dir.resolve()),
        "created_at_ct": _now_ct(),
        "wall_elapsed_sec": round(max(0.0, time.time() - float(started_at)), 3) if started_at else None,
        "cycle_count": len(cycles),
        "cycle_elapsed_sec_total": round(total_elapsed, 3),
        "cycle_elapsed_sec_avg": round(total_elapsed / len(elapsed_values), 3) if elapsed_values else 0.0,
        "scored_total": total_scored,
        "scored_total_source": "_cycle_scored_total",
        "score_cache_observed": cache_observed,
        "scored_per_cycle_sec": round(total_scored / total_elapsed, 3) if total_elapsed > 0 else None,
        "status_event_count": len(status_events),
        "status_stdout_mode": str(getattr(args, "status_stdout_mode", "all") or "all"),
        "artifact_total_mb": (artifact_budget or {}).get("total_mb"),
        "artifact_file_count": (artifact_budget or {}).get("file_count"),
        "largest_files": largest,
        "command_flags_seen": command_flags,
        "speed_review": {
            "largest_artifact_mb": largest[0].get("mb") if largest and isinstance(largest[0], dict) else None,
            "largest_artifact_path": largest[0].get("path") if largest and isinstance(largest[0], dict) else None,
            "cache_stats_mode": "fast" if any("--score-cache-stats-mode" in list(cycle.get("cmd") or []) and "fast" in list(cycle.get("cmd") or []) for cycle in cycles) else "unknown",
            "coordinator_artifact_mode": "minimal" if any("--coordinator-artifact-mode" in list(cycle.get("cmd") or []) and "minimal" in list(cycle.get("cmd") or []) for cycle in cycles) else "unknown",
            "next_audit_starts_with": [
                "speed_diagnostics.json",
                "operator_digest.json",
                "artifact_budget_report.json",
                "variant_funnel.json",
            ],
        },
    }


def _write_full_running_top100(args: argparse.Namespace) -> bool:
    if str(getattr(args, "artifact_profile", "compact") or "compact") == "full":
        return True
    return bool(getattr(args, "write_full_running_top100", False))


def _learning_roi_report(
    run_dir: Path,
    rankings: dict[str, Any],
    cycles: list[dict[str, Any]],
    online_state: dict[str, Any],
    artifact_budget: dict[str, Any] | None = None,
) -> dict[str, Any]:
    top = list(rankings.get("raw_leaderboard") or [])
    allocation = (online_state.get("online_allocation") or {}).get("allocation") if isinstance(online_state.get("online_allocation"), dict) else []
    adapter = online_state.get("runtime_command_adapter") if isinstance(online_state.get("runtime_command_adapter"), dict) else {}
    route_focus = step2_online_learning.focus_routes_from_state(online_state, limit=10) if online_state else []
    telemetry = step2_online_learning.telemetry_summary(_read_jsonl(run_dir / "streaming_telemetry.jsonl"))
    changed_controls = []
    for key in (
        "runtime_command_adapter",
        "online_allocation",
        "mutation_controls",
        "basin_status",
        "learning_rate_controller",
        "worker_learning_report_cards",
        "learning_stop_loss",
        "learning_value_stop_loss",
        "alias_trap_detector",
        "search_space_coverage_map",
    ):
        if isinstance(online_state.get(key), dict) and online_state.get(key):
            changed_controls.append(key)
    return {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "run_dir": str(run_dir.resolve()),
        "cycle_count": len(cycles),
        "top_candidate": _compact_candidate_digest(top[0] if top else {}),
        "control_modules_that_changed_runtime": changed_controls,
        "focus_routes": route_focus,
        "runtime_command_count": len(adapter.get("commands") or []),
        "mutation_width": adapter.get("mutation_width"),
        "allocation_top": list(allocation or [])[:5],
        "telemetry_summary": telemetry,
        "artifact_cost": {
            "total_mb": (artifact_budget or {}).get("total_mb"),
            "large_artifact_count": (artifact_budget or {}).get("warning_count"),
            "largest_files": (artifact_budget or {}).get("largest_files", [])[:5],
        },
        "hard_attribution_gap": (
            "needs_control_vs_observe_comparison"
            if not (run_dir / "live_vs_control_differential_report.json").exists()
            else ""
        ),
    }


def _operator_digest(
    args: argparse.Namespace,
    run_dir: Path,
    rankings: dict[str, Any],
    cycles: list[dict[str, Any]],
    artifact_paths: dict[str, str],
    online_state: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    top = list(rankings.get("raw_leaderboard") or [])
    survival_payload = hunt_intel.read_json(run_dir / "promotion_survival_top100.json", {}) or {}
    if isinstance(survival_payload, dict):
        survival = list(survival_payload.get("leaderboard") or [])
    elif isinstance(survival_payload, list):
        survival = list(survival_payload)
    else:
        survival = []
    if not survival:
        survival = _promotion_survival_leaderboard(top, limit=10) if top else []
    survival_by_variant = {
        str(row.get("variant") or ""): row
        for row in survival
        if isinstance(row, dict) and row.get("variant")
    }
    top_digest_row = dict(top[0]) if top else {}
    if top_digest_row.get("variant") in survival_by_variant:
        top_digest_row = {**top_digest_row, **survival_by_variant[str(top_digest_row.get("variant"))]}
    brain = hunt_intel.read_json(run_dir / "hunt_brain_summary.json", {}) or {}
    blocker_digest = hunt_intel.read_json(run_dir / "promotion_blocker_digest.json", {}) or {}
    failure_summary = hunt_intel.read_json(run_dir / "top_failure_summary.json", {}) or {}
    ready = hunt_intel.read_json(run_dir / "promotion_ready_summary.json", {}) or {}
    next_recipe = hunt_intel.read_json(run_dir / "next_command_recipe.json", {}) or {}
    budget = hunt_intel.read_json(run_dir / "artifact_budget_report.json", {}) or {}
    speed = hunt_intel.read_json(run_dir / "speed_diagnostics.json", {}) or {}
    loop_progress = hunt_intel.read_json(run_dir / "loop_progress_summary.json", {}) or {}
    learning_roi = hunt_intel.read_json(run_dir / "learning_roi_report.json", {}) or {}
    state = online_state or step2_online_learning.read_state(run_dir / "online_state.json")
    telemetry = step2_online_learning.telemetry_summary(_read_jsonl(run_dir / "streaming_telemetry.jsonl"))
    digest = {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "updated_at_ct": _now_ct(),
        "run_dir": str(run_dir.resolve()),
        "artifact_profile": str(getattr(args, "artifact_profile", "compact") or "compact"),
        "cycles_completed": len(cycles),
        "top100_count": len(top),
        "top_candidate": _compact_candidate_digest(top_digest_row),
        "promotion_survival_rank1": _compact_candidate_digest(survival[0] if survival else {}),
        "promotion_ready_summary": {
            "ready_count": ready.get("ready_count") if ready.get("ready_count") is not None else ready.get("promotion_ready_count"),
            "review_queue_count": ready.get("review_queue_count"),
            "best_ready": _compact_candidate_digest(
                (ready.get("ready") or ready.get("promotion_ready_top10") or [{}])[0]
                if isinstance(ready.get("ready") or ready.get("promotion_ready_top10"), list) and (ready.get("ready") or ready.get("promotion_ready_top10"))
                else {}
            ),
        },
        "top_blockers": (blocker_digest.get("top_blockers") or blocker_digest.get("blockers") or [])[:8],
        "failure_summary": {
            "human_summary": failure_summary.get("human_summary"),
            "top_failure_reasons": (failure_summary.get("top_failure_reasons") or [])[:8],
        },
        "learning": {
            "what_worked": (brain.get("what_worked") or [])[:5],
            "what_failed": (brain.get("what_failed") or [])[:5],
            "hunt_next": (brain.get("hunt_next") or [])[:5],
            "stop_wasting_time_on": (brain.get("stop_wasting_time_on") or [])[:5],
            "control_modules_that_changed_runtime": (learning_roi.get("control_modules_that_changed_runtime") or [])[:12],
            "false_lesson_guard": {
                "claim_verifier": (state.get("learning_claim_verifier") or {}).get("summary") if isinstance(state.get("learning_claim_verifier"), dict) else None,
                "weak_claim_count": (state.get("learning_claim_verifier") or {}).get("weak_claim_count") if isinstance(state.get("learning_claim_verifier"), dict) else None,
                "contradiction_count": (state.get("contradiction_detector") or {}).get("contradiction_count") if isinstance(state.get("contradiction_detector"), dict) else None,
                "trust": "verify_before_scaling" if ((state.get("learning_claim_verifier") or {}).get("weak_claim_count") if isinstance(state.get("learning_claim_verifier"), dict) else 0) else "normal",
            },
        },
        "telemetry_summary": telemetry,
        "artifact_budget": {
            "total_mb": budget.get("total_mb"),
            "file_count": budget.get("file_count"),
            "warning_count": budget.get("warning_count"),
            "largest_files": (budget.get("largest_files") or [])[:5],
        },
        "speed_diagnostics": {
            "wall_elapsed_sec": speed.get("wall_elapsed_sec"),
            "cycle_elapsed_sec_total": speed.get("cycle_elapsed_sec_total"),
            "scored_per_cycle_sec": speed.get("scored_per_cycle_sec"),
            "status_event_count": speed.get("status_event_count"),
            "status_stdout_mode": speed.get("status_stdout_mode"),
            "cache_stats_mode": (speed.get("speed_review") or {}).get("cache_stats_mode") if isinstance(speed.get("speed_review"), dict) else None,
            "score_cache_hit_rate_pct": ((speed.get("score_cache_observed") or {}).get("score_cache_hit_rate_pct") if isinstance(speed.get("score_cache_observed"), dict) else None),
        },
        "loop_progress": {
            "campaign_prefix": loop_progress.get("campaign_prefix"),
            "runs_completed": loop_progress.get("runs_completed"),
            "cumulative_scored_total": loop_progress.get("cumulative_scored_total"),
            "target_scored_total": loop_progress.get("target_scored_total"),
            "remaining_to_target": loop_progress.get("remaining_to_target"),
            "best_promotion_ready_count": loop_progress.get("best_promotion_ready_count"),
            "avg_wall_elapsed_sec": loop_progress.get("avg_wall_elapsed_sec"),
        },
        "next_command_recipe": next_recipe,
        "artifact_paths": {
            key: artifact_paths.get(key)
            for key in (
                "final_summary",
                "running_top100",
                "promotion_survival_top100",
                "promotion_ready_summary",
                "top_failure_summary",
                "artifact_budget_report",
                "artifact_manifest",
                "speed_diagnostics",
                "loop_progress_summary",
                "learning_roi_report",
            )
            if artifact_paths.get(key)
        },
    }
    if extra:
        digest.update(extra)
    return digest


def run(args: argparse.Namespace) -> dict[str, Any]:
    if bool(getattr(args, "smoke_validation_mode", False)):
        args.exact_variant_count = True
        args.runtime_control_mode = "observe"
    run_dir = Path(args.resume_run_dir).resolve() if args.resume_run_dir else Path(args.out_dir) / f"{args.name}_{_stamp()}"
    run_dir.mkdir(parents=True, exist_ok=True)
    run_seconds = _effective_run_seconds(args)
    started_epoch = time.time()
    deadline = started_epoch + run_seconds
    cycles: list[dict[str, Any]] = _load_cycle_log(run_dir) if args.resume_run_dir else []
    cycle_idx = len(cycles)
    started_at = _now_ct()
    _write_json(run_dir / "status_policy.json", _status_policy(args))
    cross_run_controls = {}
    runtime_bootstrap_controls = _load_runtime_bootstrap_controls(args, run_dir)
    if args.online_learning and not args.resume_run_dir:
        cross_run_controls = _load_cross_run_opening_controls(args, run_dir)
        if cross_run_controls:
            opening_state = _apply_cross_run_opening_controls({}, cross_run_controls)
            opening_state["batch_size_multiplier"] = step2_online_learning.batch_size_multiplier(opening_state)
            _write_compact_json(run_dir / "online_state.json", opening_state)
            _write_runtime_controls(run_dir, opening_state)
    if runtime_bootstrap_controls:
        _apply_runtime_bootstrap_controls(run_dir, runtime_bootstrap_controls)
    initial_gate = _pre_hunt_stop_go_gate(args, run_dir)
    _write_json(run_dir / "pre_hunt_stop_go_gate.json", initial_gate)
    _emit_status_update(
        args,
        run_dir,
        _status_snapshot(
            args,
            run_dir,
            phase="hunt_started",
            cycle_idx=cycle_idx,
            started_at=started_at,
            deadline=deadline,
        ),
    )

    while time.time() < deadline:
        if int(getattr(args, "max_cycles", 0) or 0) > 0 and cycle_idx >= int(args.max_cycles):
            break
        remaining = max(1, int(deadline - time.time()))
        seed = int(args.seed) + cycle_idx
        current_rankings = collect_rankings(args, run_dir)
        online_state = step2_online_learning.read_state(run_dir / "online_state.json") if args.online_learning else {}
        if args.online_learning and cross_run_controls and cycle_idx == 0:
            online_state = _apply_cross_run_opening_controls(online_state, cross_run_controls)
        hunter = _choose_hunter(args, cycle_idx, current_rankings)
        opening_strategy = (online_state.get("pre_hunt_strategy_selector") or {}).get("strategy") if isinstance(online_state.get("pre_hunt_strategy_selector"), dict) else ""
        if cycle_idx == 0 and opening_strategy:
            preferred = (online_state.get("pre_hunt_strategy_selector") or {}).get("recommended_hunters") or []
            hunter = next((item for item in preferred if item in args.hunters), hunter)
        effective_mode = _effective_hunt_mode(args, run_dir)
        if effective_mode in {"promotion_repair", "repair_exploration", "weird_exploration"} and "router" in args.hunters:
            hunter = "router"
        if args.online_learning and online_state:
            online_state["hunt_mode"] = effective_mode
            online_state["batch_size_multiplier"] = step2_online_learning.batch_size_multiplier(online_state)
            _write_compact_json(run_dir / "online_state.json", online_state)
            _write_runtime_controls(run_dir, online_state)
            if runtime_bootstrap_controls and cycle_idx == 0:
                _apply_runtime_bootstrap_controls(run_dir, runtime_bootstrap_controls)
            allocation = (online_state.get("online_allocation") or {}).get("allocation") or []
            if allocation and str((allocation[0] or {}).get("route_key") or "") not in {"", "novelty_exploration", "broad_exploration"}:
                hunter = "router" if "router" in args.hunters else hunter
        elif args.online_learning:
            _write_runtime_controls(run_dir, {})
            if runtime_bootstrap_controls and cycle_idx == 0:
                _apply_runtime_bootstrap_controls(run_dir, runtime_bootstrap_controls)
        cmd = _cycle_command(args, run_dir, cycle_idx, seed, hunter, online_state)
        requested_for_cycle = max(1, int(args.batch_size)) * max(1, int(args.max_batches))
        learning_tier_contract = _learning_tier_for_requested(args, requested_for_cycle)
        row: dict[str, Any] = {
            "cycle": cycle_idx,
            "hunter": hunter,
            "hunt_mode": effective_mode,
            "learning_tier_contract": learning_tier_contract,
            "seed": seed,
            "cmd": cmd,
            "started_at_ct": _now_ct(),
        }
        step2_online_learning.append_event(run_dir, "cycle_started", {
            "cycle": cycle_idx,
            "hunter": hunter,
            "hunt_mode": effective_mode,
            "learning_tier_contract": learning_tier_contract,
            "seed": seed,
            "online_focus_routes": step2_online_learning.focus_routes_from_state(online_state, limit=int(args.max_focus_routes)) if args.online_learning else [],
            "mutation_scale": step2_online_learning.mutation_scale_for_route(online_state) if args.online_learning else 1.0,
        })
        _emit_status_update(
            args,
            run_dir,
            _status_snapshot(
                args,
                run_dir,
                phase="cycle_started",
                cycle_idx=cycle_idx,
                hunter=hunter,
                started_at=started_at,
                deadline=deadline,
                rankings=current_rankings,
            ),
        )
        t0 = time.perf_counter()
        if int(args.status_interval_sec) > 0 or (args.online_learning and hunter == "router"):
            row = _run_cycle_process(args, run_dir, cmd, row, deadline, online_state or {}, cycles)
        else:
            cycle_log_dir = run_dir / "cycle_process_logs"
            cycle_log_dir.mkdir(parents=True, exist_ok=True)
            stdout_path = cycle_log_dir / f"cycle_{cycle_idx:04d}_{hunter}.stdout.log"
            stderr_path = cycle_log_dir / f"cycle_{cycle_idx:04d}_{hunter}.stderr.log"
            try:
                with stdout_path.open("a", encoding="utf-8") as stdout_handle, stderr_path.open("a", encoding="utf-8") as stderr_handle:
                    proc = subprocess.run(
                        cmd,
                        cwd=HERE,
                        text=True,
                        stdout=stdout_handle,
                        stderr=stderr_handle,
                        timeout=min(remaining, int(args.cycle_timeout_sec)),
                    )
                row.update(
                    {
                        "ok": proc.returncode == 0,
                        "returncode": proc.returncode,
                        "completion_reason": "completed",
                        "partial_valid_artifact": False,
                        "stdout_path": str(stdout_path.resolve()),
                        "stderr_path": str(stderr_path.resolve()),
                        "stdout_tail": _tail_text(stdout_path, 4000),
                        "stderr_tail": _tail_text(stderr_path, 4000),
                    }
                )
            except subprocess.TimeoutExpired as exc:
                window_elapsed = time.time() >= deadline - 1
                row.update(
                    {
                        "ok": False,
                        "timeout": True,
                        "completion_reason": "partial_valid_artifact" if window_elapsed else "cycle_timeout",
                        "partial_valid_artifact": True,
                        "interrupt_reason": "run_window_elapsed" if window_elapsed else "cycle_timeout",
                        "stdout_path": str(stdout_path.resolve()),
                        "stderr_path": str(stderr_path.resolve()),
                        "stdout_tail": _tail_text(stdout_path, 4000),
                        "stderr_tail": _tail_text(stderr_path, 4000),
                    }
                )
        row["elapsed_sec"] = round(time.perf_counter() - t0, 3)
        row["finished_at_ct"] = _now_ct()
        cycles.append(row)
        _record_cycle_db(args, run_dir, row)
        step2_online_learning.append_event(run_dir, "cycle_finished", {
            "cycle": cycle_idx,
            "hunter": hunter,
            "ok": row.get("ok"),
            "elapsed_sec": row.get("elapsed_sec"),
            "reported_winners": _cycle_winners(row),
            "reported_scored_total": _cycle_scored_total(row),
        })
        rankings = collect_rankings(args, run_dir)
        top100 = list(rankings.get("raw_leaderboard") or [])
        quality = list(rankings.get("promotion_quality_leaderboard") or [])
        artifact_paths = _write_learning_artifacts(args, run_dir, rankings)
        cycle_learning = hunt_intel.read_json(run_dir / "learning_report.json", {}) or {}
        cycle_experiment_plan = cycle_learning.get("active_experiment_plan") if isinstance(cycle_learning.get("active_experiment_plan"), dict) else {}
        cycle_treatment_prior_model = cycle_learning.get("treatment_prior_model") if isinstance(cycle_learning.get("treatment_prior_model"), dict) else {}
        cycle_treatment_worker_budget = cycle_learning.get("treatment_worker_budget") if isinstance(cycle_learning.get("treatment_worker_budget"), dict) else {}
        micro_promotion = _run_micro_promotion_review(args, run_dir, rankings, cycles)
        promotion_feedback = list(micro_promotion.get("feedback") or []) if isinstance(micro_promotion, dict) else []
        promotion_directives = list(micro_promotion.get("directives") or []) if isinstance(micro_promotion, dict) else []
        if isinstance(micro_promotion, dict) and not micro_promotion.get("skipped"):
            artifact_paths["promotion_micro_reviews"] = str((run_dir / "promotion_micro_reviews.json").resolve())
            artifact_paths["promotion_review_feedback"] = str((run_dir / "promotion_review_feedback.json").resolve())
            artifact_paths["promotion_ready_queue"] = str((run_dir / "promotion_ready_queue.json").resolve())
        updated_online_state = {}
        if args.online_learning:
            updated_online_state = step2_online_learning.update_online_state(
                run_dir=run_dir,
                rankings=rankings,
                cycles=cycles,
                previous_state=online_state,
                novelty_budget_pct=float(args.online_novelty_budget_pct),
                promotion_feedback_rows=promotion_feedback,
                promotion_review_directives=promotion_directives,
                experiment_plan=cycle_experiment_plan,
                treatment_prior_model=cycle_treatment_prior_model,
                treatment_worker_budget=cycle_treatment_worker_budget,
                treatment_confidence=cycle_learning.get("treatment_confidence_model") if isinstance(cycle_learning.get("treatment_confidence_model"), dict) else {},
                controlled_sibling_experiments=cycle_learning.get("controlled_parent_sibling_experiments") if isinstance(cycle_learning.get("controlled_parent_sibling_experiments"), dict) else {},
                worker_specialization=cycle_learning.get("worker_specialization_memory") if isinstance(cycle_learning.get("worker_specialization_memory"), dict) else {},
                regime_learning=cycle_learning.get("regime_aware_learning") if isinstance(cycle_learning.get("regime_aware_learning"), dict) else {},
                promotion_reject_simulator=cycle_learning.get("promotion_reject_simulator") if isinstance(cycle_learning.get("promotion_reject_simulator"), dict) else {},
                search_portfolio=cycle_learning.get("search_portfolio_manager") if isinstance(cycle_learning.get("search_portfolio_manager"), dict) else {},
                causal_experiment_registry=cycle_learning.get("causal_experiment_registry") if isinstance(cycle_learning.get("causal_experiment_registry"), dict) else {},
                experiment_debt_queue=cycle_learning.get("experiment_debt_queue") if isinstance(cycle_learning.get("experiment_debt_queue"), dict) else {},
                information_gain_scoring=cycle_learning.get("information_gain_scoring") if isinstance(cycle_learning.get("information_gain_scoring"), dict) else {},
                value_of_information_planner=cycle_learning.get("value_of_information_planner") if isinstance(cycle_learning.get("value_of_information_planner"), dict) else {},
                decision_change_tracker=cycle_learning.get("decision_change_tracker") if isinstance(cycle_learning.get("decision_change_tracker"), dict) else {},
                hypothesis_quality_scoring=cycle_learning.get("hypothesis_quality_scoring") if isinstance(cycle_learning.get("hypothesis_quality_scoring"), dict) else {},
                evidence_sufficiency_gate=cycle_learning.get("evidence_sufficiency_gate") if isinstance(cycle_learning.get("evidence_sufficiency_gate"), dict) else {},
                counterfactual_shadow_board=cycle_learning.get("counterfactual_shadow_board") if isinstance(cycle_learning.get("counterfactual_shadow_board"), dict) else {},
                prediction_calibration_ledger=cycle_learning.get("prediction_calibration_ledger") if isinstance(cycle_learning.get("prediction_calibration_ledger"), dict) else {},
                belief_revision_engine=cycle_learning.get("belief_revision_engine") if isinstance(cycle_learning.get("belief_revision_engine"), dict) else {},
                adversarial_red_team_learner=cycle_learning.get("adversarial_red_team_learner") if isinstance(cycle_learning.get("adversarial_red_team_learner"), dict) else {},
                out_of_distribution_detector=cycle_learning.get("out_of_distribution_detector") if isinstance(cycle_learning.get("out_of_distribution_detector"), dict) else {},
                memory_compression_distiller=cycle_learning.get("memory_compression_distiller") if isinstance(cycle_learning.get("memory_compression_distiller"), dict) else {},
                self_audit_score=cycle_learning.get("self_audit_score") if isinstance(cycle_learning.get("self_audit_score"), dict) else {},
                truth_first_promotion_objective=cycle_learning.get("truth_first_promotion_objective") if isinstance(cycle_learning.get("truth_first_promotion_objective"), dict) else {},
                learning_velocity_dashboard=cycle_learning.get("learning_velocity_dashboard") if isinstance(cycle_learning.get("learning_velocity_dashboard"), dict) else {},
                compiled_hunt_policy=cycle_learning.get("compiled_hunt_policy") if isinstance(cycle_learning.get("compiled_hunt_policy"), dict) else {},
                policy_executor=cycle_learning.get("policy_executor") if isinstance(cycle_learning.get("policy_executor"), dict) else {},
                adaptive_worker_assignment=cycle_learning.get("adaptive_worker_assignment") if isinstance(cycle_learning.get("adaptive_worker_assignment"), dict) else {},
                policy_backtester=cycle_learning.get("policy_backtester") if isinstance(cycle_learning.get("policy_backtester"), dict) else {},
                policy_mutation_engine=cycle_learning.get("policy_mutation_engine") if isinstance(cycle_learning.get("policy_mutation_engine"), dict) else {},
                policy_tournament=cycle_learning.get("policy_tournament") if isinstance(cycle_learning.get("policy_tournament"), dict) else {},
                champion_challenger_memory=cycle_learning.get("champion_challenger_memory") if isinstance(cycle_learning.get("champion_challenger_memory"), dict) else {},
                regime_specific_policies=cycle_learning.get("regime_specific_policies") if isinstance(cycle_learning.get("regime_specific_policies"), dict) else {},
                causal_graph_of_learning=cycle_learning.get("causal_graph_of_learning") if isinstance(cycle_learning.get("causal_graph_of_learning"), dict) else {},
                policy_safety_rail=cycle_learning.get("policy_safety_rail") if isinstance(cycle_learning.get("policy_safety_rail"), dict) else {},
                auto_promoted_field_manual=cycle_learning.get("auto_promoted_field_manual") if isinstance(cycle_learning.get("auto_promoted_field_manual"), dict) else {},
                policy_drift_detector=cycle_learning.get("policy_drift_detector") if isinstance(cycle_learning.get("policy_drift_detector"), dict) else {},
                route_regime_half_life=cycle_learning.get("route_regime_half_life") if isinstance(cycle_learning.get("route_regime_half_life"), dict) else {},
                learning_market_map=cycle_learning.get("learning_market_map") if isinstance(cycle_learning.get("learning_market_map"), dict) else {},
                concept_drift_alarms=cycle_learning.get("concept_drift_alarms") if isinstance(cycle_learning.get("concept_drift_alarms"), dict) else {},
                revalidation_scheduler=cycle_learning.get("revalidation_scheduler") if isinstance(cycle_learning.get("revalidation_scheduler"), dict) else {},
                temporal_ensemble_policy=cycle_learning.get("temporal_ensemble_policy") if isinstance(cycle_learning.get("temporal_ensemble_policy"), dict) else {},
                active_experiment_governor=cycle_learning.get("active_experiment_governor") if isinstance(cycle_learning.get("active_experiment_governor"), dict) else {},
                route_state_machine=cycle_learning.get("route_state_machine") if isinstance(cycle_learning.get("route_state_machine"), dict) else {},
                negative_knowledge_bank=cycle_learning.get("negative_knowledge_bank") if isinstance(cycle_learning.get("negative_knowledge_bank"), dict) else {},
                promotion_survivor_model=cycle_learning.get("promotion_survivor_model") if isinstance(cycle_learning.get("promotion_survivor_model"), dict) else {},
                mutation_grammar_learner=cycle_learning.get("mutation_grammar_learner") if isinstance(cycle_learning.get("mutation_grammar_learner"), dict) else {},
                real_time_worker_rebalancer=cycle_learning.get("real_time_worker_rebalancer") if isinstance(cycle_learning.get("real_time_worker_rebalancer"), dict) else {},
                hunt_replay_simulator=cycle_learning.get("hunt_replay_simulator") if isinstance(cycle_learning.get("hunt_replay_simulator"), dict) else {},
                resurrection_engine=cycle_learning.get("resurrection_engine") if isinstance(cycle_learning.get("resurrection_engine"), dict) else {},
                causal_mutation_attribution=cycle_learning.get("causal_mutation_attribution") if isinstance(cycle_learning.get("causal_mutation_attribution"), dict) else {},
                uncertainty_budgeting=cycle_learning.get("uncertainty_budgeting") if isinstance(cycle_learning.get("uncertainty_budgeting"), dict) else {},
                promotability_pareto_frontier=cycle_learning.get("promotability_pareto_frontier") if isinstance(cycle_learning.get("promotability_pareto_frontier"), dict) else {},
                false_lesson_detector=cycle_learning.get("false_lesson_detector") if isinstance(cycle_learning.get("false_lesson_detector"), dict) else {},
                experiment_graduation_system=cycle_learning.get("experiment_graduation_system") if isinstance(cycle_learning.get("experiment_graduation_system"), dict) else {},
                candidate_genealogy_diff_engine=cycle_learning.get("candidate_genealogy_diff_engine") if isinstance(cycle_learning.get("candidate_genealogy_diff_engine"), dict) else {},
                off_policy_hunt_evaluator=cycle_learning.get("off_policy_hunt_evaluator") if isinstance(cycle_learning.get("off_policy_hunt_evaluator"), dict) else {},
                self_competition_league=cycle_learning.get("self_competition_league") if isinstance(cycle_learning.get("self_competition_league"), dict) else {},
                evidence_contract_engine=cycle_learning.get("evidence_contract_engine") if isinstance(cycle_learning.get("evidence_contract_engine"), dict) else {},
                live_beater_quality_decomposer=cycle_learning.get("live_beater_quality_decomposer") if isinstance(cycle_learning.get("live_beater_quality_decomposer"), dict) else {},
                contradiction_detector=cycle_learning.get("contradiction_detector") if isinstance(cycle_learning.get("contradiction_detector"), dict) else {},
                learning_conflict_resolver=cycle_learning.get("learning_conflict_resolver") if isinstance(cycle_learning.get("learning_conflict_resolver"), dict) else {},
                cohort_based_memory=cycle_learning.get("cohort_based_memory") if isinstance(cycle_learning.get("cohort_based_memory"), dict) else {},
                adaptive_hunt_throttle=cycle_learning.get("adaptive_hunt_throttle") if isinstance(cycle_learning.get("adaptive_hunt_throttle"), dict) else {},
                promotion_readiness_simulator=cycle_learning.get("promotion_readiness_simulator") if isinstance(cycle_learning.get("promotion_readiness_simulator"), dict) else {},
                research_trace_ledger=cycle_learning.get("research_trace_ledger") if isinstance(cycle_learning.get("research_trace_ledger"), dict) else {},
                runtime_decision_kernel=cycle_learning.get("runtime_decision_kernel") if isinstance(cycle_learning.get("runtime_decision_kernel"), dict) else {},
                action_outcome_tracker=cycle_learning.get("action_outcome_tracker") if isinstance(cycle_learning.get("action_outcome_tracker"), dict) else {},
                closed_loop_reward_model=cycle_learning.get("closed_loop_reward_model") if isinstance(cycle_learning.get("closed_loop_reward_model"), dict) else {},
                autonomous_hunt_planner=cycle_learning.get("autonomous_hunt_planner") if isinstance(cycle_learning.get("autonomous_hunt_planner"), dict) else {},
                runtime_guardrails=cycle_learning.get("runtime_guardrails") if isinstance(cycle_learning.get("runtime_guardrails"), dict) else {},
                command_replay_ledger=cycle_learning.get("command_replay_ledger") if isinstance(cycle_learning.get("command_replay_ledger"), dict) else {},
                action_elo_league=cycle_learning.get("action_elo_league") if isinstance(cycle_learning.get("action_elo_league"), dict) else {},
                human_readable_hunt_brief=cycle_learning.get("human_readable_hunt_brief") if isinstance(cycle_learning.get("human_readable_hunt_brief"), dict) else {},
                meta_hunt_strategy=cycle_learning.get("meta_hunt_strategy_learner") if isinstance(cycle_learning.get("meta_hunt_strategy_learner"), dict) else {},
                run_to_run_postmortem=cycle_learning.get("run_to_run_postmortem") if isinstance(cycle_learning.get("run_to_run_postmortem"), dict) else {},
            )
            updated_online_state = _attach_runtime_experiment_learning(updated_online_state, cycle_learning)
            updated_online_state["batch_size_multiplier"] = step2_online_learning.batch_size_multiplier(updated_online_state)
            _write_compact_json(run_dir / "online_state.json", updated_online_state)
            _write_runtime_controls(run_dir, updated_online_state)
            _write_json(run_dir / "command_outcome_backfill.json", updated_online_state.get("command_outcome_backfill") or {})
            _write_json(run_dir / "action_reward_calibration.json", updated_online_state.get("action_reward_calibration") or {})
            artifact_paths["online_state"] = str((run_dir / "online_state.json").resolve())
            artifact_paths["learning_events"] = str((run_dir / "learning_events.jsonl").resolve())
            artifact_paths["streaming_telemetry"] = str((run_dir / "streaming_telemetry.jsonl").resolve())
            artifact_paths["runtime_command_adapter"] = str((run_dir / "runtime_command_adapter.json").resolve())
            artifact_paths["worker_job_contracts"] = str((run_dir / "worker_job_contracts.json").resolve())
            artifact_paths["command_delta_status"] = str((run_dir / "command_delta_status.json").resolve())
            artifact_paths["command_outcome_backfill"] = str((run_dir / "command_outcome_backfill.json").resolve())
            artifact_paths["action_reward_calibration"] = str((run_dir / "action_reward_calibration.json").resolve())
            for key in (
                "ab_route_experiment_executor",
                "champion_challenger_runtime_slots",
                "adaptive_experiment_stopping",
                "counterfactual_command_replay",
                "experiment_contamination_guard",
                "learning_rate_controller",
                "worker_learning_report_cards",
                "experiment_to_promotion_trace",
                "zero_yield_autopsy_engine",
                "stuck_loop_breaker",
                "opportunity_cost_meter",
                "search_space_coverage_map",
                "live_beater_scarcity_mode",
                "alias_trap_detector",
                "route_seed_quality_score",
                "recovery_playbook_generator",
            ):
                artifact_paths[key] = str((run_dir / f"{key}.json").resolve())
        _emit_status_update(
            args,
            run_dir,
            _status_snapshot(
                args,
                run_dir,
                phase="cycle_finished",
                cycle_idx=cycle_idx,
                hunter=hunter,
                started_at=started_at,
                deadline=deadline,
                rankings=rankings,
                telemetry_summary=row.get("streaming_telemetry_summary") if isinstance(row.get("streaming_telemetry_summary"), dict) else {},
            ),
        )
        micro_validation = _run_micro_validation(
            args,
            run_dir,
            cycle_idx + 1,
            int(deadline - time.time()),
            force=bool(row.get("validation_triggered")),
        )
        if micro_validation:
            row["micro_validation"] = micro_validation
        cycle_variant_funnel = _variant_funnel(args, run_dir, cycles, rankings)
        artifact_paths["variant_funnel"] = _write_json(run_dir / "variant_funnel.json", cycle_variant_funnel)
        cycle_tiny_guard = _tiny_run_calibration_guard(args, cycle_variant_funnel)
        artifact_paths["tiny_run_calibration_guard"] = _write_json(run_dir / "tiny_run_calibration_guard.json", cycle_tiny_guard)
        cycle_repair_queue = hunt_intel.read_json(run_dir / "promotion_evidence_repair_queue.json", {}) or _promotion_evidence_repair_queue(top100, limit=100)
        cycle_repair_directives = hunt_intel.read_json(run_dir / "promotion_repair_worker_directives.json", {}) or _repair_worker_directives(cycle_repair_queue)
        cycle_stop_go = _pre_hunt_stop_go_gate(args, run_dir, rankings, cycle_variant_funnel, updated_online_state)
        artifact_paths["pre_hunt_stop_go_gate"] = _write_json(run_dir / "pre_hunt_stop_go_gate.json", cycle_stop_go)
        cycle_brain = _hunt_brain_summary(
            args,
            run_dir,
            rankings,
            cycles,
            cycle_variant_funnel,
            cycle_repair_queue,
            cycle_repair_directives,
            cycle_stop_go,
        )
        artifact_paths["hunt_brain_summary"] = _write_json(run_dir / "hunt_brain_summary.json", cycle_brain)
        running_payload = {
            "schema_version": 1,
            "source": "run_step2_three_hour_hunt",
            "updated_at_ct": _now_ct(),
            "run_dir": str(run_dir.resolve()),
            "cycles_completed": len(cycles),
            "top100_count": len(top100),
            "variant_funnel": cycle_variant_funnel,
            "ranking_stats": {k: rankings.get(k) for k in (
                "input_rows",
                "live_filtered_rows",
                "config_unique_rows",
                "behavior_unique_rows",
            )},
            "scorer_source_paths": rankings.get("scorer_source_paths") or [],
            "diagnostic_source_paths": rankings.get("diagnostic_source_paths") or [],
            "top100": top100,
            "promotion_survival_top100": _promotion_survival_leaderboard(top100, limit=int(args.leaderboard_limit)),
            "promotion_quality_top100": quality,
            "promotion_readiness_top100": list(rankings.get("promotion_readiness_leaderboard") or []),
            "promotion_evidence_repair_queue": cycle_repair_queue,
            "promotion_repair_worker_directives": cycle_repair_directives,
            "pre_hunt_stop_go_gate": cycle_stop_go,
            "tiny_run_calibration_guard": cycle_tiny_guard,
            "hunt_brain_summary": cycle_brain,
            "route_clusters": hunt_intel.route_clusters(top100),
            "next_hunt_plan": hunt_intel.next_hunt_plan(top100),
            "online_state": updated_online_state,
            "micro_promotion_review": micro_promotion,
            "learning_report": hunt_intel.learning_report(
                rows=top100,
                cycles=cycles,
                all_rows=list(rankings.get("diagnostic_rows") or rankings.get("decorated_rows") or top100),
                feedback_rows=promotion_feedback,
                source="run_step2_three_hour_hunt",
            ),
            "status_policy": _status_policy(args),
            "artifact_paths": artifact_paths,
        }
        if str(getattr(args, "artifact_profile", "compact") or "compact") == "full":
            artifact_paths["running_top100"] = _write_json(run_dir / "running_top100.json", running_payload)
        else:
            if _write_full_running_top100(args):
                artifact_paths["running_top100_full"] = _write_json(run_dir / "running_top100.full.json", running_payload)
            artifact_paths["running_top100"] = _write_compact_json(
                run_dir / "running_top100.json",
                {
                    "schema_version": 1,
                    "source": "run_step2_three_hour_hunt",
                    "updated_at_ct": running_payload["updated_at_ct"],
                    "run_dir": running_payload["run_dir"],
                    "cycles_completed": len(cycles),
                    "top100_count": len(top100),
                    "variant_funnel": cycle_variant_funnel,
                    "ranking_stats": running_payload["ranking_stats"],
                    "top10": [_compact_candidate_digest(row) for row in top100[:10]],
                    "promotion_survival_top10": [_compact_candidate_digest(row) for row in running_payload["promotion_survival_top100"][:10]],
                    "promotion_evidence_repair_queue": {
                        "task_count": cycle_repair_queue.get("task_count"),
                        "focus_routes": cycle_repair_queue.get("focus_routes") or [],
                        "tasks": (cycle_repair_queue.get("tasks") or [])[:5],
                    },
                    "pre_hunt_stop_go_gate": cycle_stop_go,
                    "hunt_brain_summary": {
                        "what_worked": (cycle_brain.get("what_worked") or [])[:5],
                        "what_failed": (cycle_brain.get("what_failed") or [])[:5],
                        "hunt_next": (cycle_brain.get("hunt_next") or [])[:5],
                    },
                    "artifact_paths": {
                        "full": artifact_paths.get("running_top100_full"),
                        "full_omitted_reason": "" if artifact_paths.get("running_top100_full") else "duplicate_of_checkpoint_artifacts_and_final_summary",
                        "learning_report": artifact_paths.get("learning_report"),
                        "promotion_survival_top100": artifact_paths.get("promotion_survival_top100"),
                    },
                },
            )
        artifact_budget = _artifact_budget_report(run_dir, warning_mb=float(getattr(args, "artifact_size_warning_mb", 10.0) or 10.0))
        artifact_paths["artifact_budget_report"] = _write_json(run_dir / "artifact_budget_report.json", artifact_budget)
        digest = _operator_digest(args, run_dir, rankings, cycles, artifact_paths, updated_online_state)
        artifact_paths["operator_digest"] = _write_compact_json(run_dir / "operator_digest.json", digest)
        _write_json(run_dir / "cycle_log.json", _cycle_log_payload(cycles))
        cycle_idx += 1
        if int(deadline - time.time()) < max(15, int(args.min_seconds_for_next_cycle)):
            break

    final_rankings = collect_rankings(args, run_dir)
    final_top100 = list(final_rankings.get("raw_leaderboard") or [])
    final_quality = list(final_rankings.get("promotion_quality_leaderboard") or [])
    final_readiness = list(final_rankings.get("promotion_readiness_leaderboard") or [])
    route_clusters = hunt_intel.route_clusters(final_top100)
    artifact_paths = _write_learning_artifacts(args, run_dir, final_rankings)
    final_micro_promotion = _run_micro_promotion_review(args, run_dir, final_rankings, cycles, force=True)
    final_promotion_feedback = list(final_micro_promotion.get("feedback") or []) if isinstance(final_micro_promotion, dict) else []
    final_promotion_directives = list(final_micro_promotion.get("directives") or []) if isinstance(final_micro_promotion, dict) else []
    if isinstance(final_micro_promotion, dict) and not final_micro_promotion.get("skipped"):
        artifact_paths["promotion_micro_reviews"] = str((run_dir / "promotion_micro_reviews.json").resolve())
        artifact_paths["promotion_review_feedback"] = str((run_dir / "promotion_review_feedback.json").resolve())
        artifact_paths["promotion_ready_queue"] = str((run_dir / "promotion_ready_queue.json").resolve())
    final_online_state = {}
    if args.online_learning:
        final_learning_file = hunt_intel.read_json(run_dir / "learning_report.json", {}) or {}
        final_online_state = step2_online_learning.update_online_state(
            run_dir=run_dir,
            rankings=final_rankings,
            cycles=cycles,
            previous_state=step2_online_learning.read_state(run_dir / "online_state.json"),
            novelty_budget_pct=float(args.online_novelty_budget_pct),
            promotion_feedback_rows=final_promotion_feedback,
            promotion_review_directives=final_promotion_directives,
            experiment_plan=final_learning_file.get("active_experiment_plan") or {},
            treatment_prior_model=final_learning_file.get("treatment_prior_model") or {},
            treatment_worker_budget=final_learning_file.get("treatment_worker_budget") or {},
            treatment_confidence=final_learning_file.get("treatment_confidence_model") or {},
            controlled_sibling_experiments=final_learning_file.get("controlled_parent_sibling_experiments") or {},
            worker_specialization=final_learning_file.get("worker_specialization_memory") or {},
            regime_learning=final_learning_file.get("regime_aware_learning") or {},
            promotion_reject_simulator=final_learning_file.get("promotion_reject_simulator") or {},
            search_portfolio=final_learning_file.get("search_portfolio_manager") or {},
            causal_experiment_registry=final_learning_file.get("causal_experiment_registry") or {},
            experiment_debt_queue=final_learning_file.get("experiment_debt_queue") or {},
            information_gain_scoring=final_learning_file.get("information_gain_scoring") or {},
            value_of_information_planner=final_learning_file.get("value_of_information_planner") or {},
            decision_change_tracker=final_learning_file.get("decision_change_tracker") or {},
            hypothesis_quality_scoring=final_learning_file.get("hypothesis_quality_scoring") or {},
            evidence_sufficiency_gate=final_learning_file.get("evidence_sufficiency_gate") or {},
            counterfactual_shadow_board=final_learning_file.get("counterfactual_shadow_board") or {},
            prediction_calibration_ledger=final_learning_file.get("prediction_calibration_ledger") or {},
            belief_revision_engine=final_learning_file.get("belief_revision_engine") or {},
            adversarial_red_team_learner=final_learning_file.get("adversarial_red_team_learner") or {},
            out_of_distribution_detector=final_learning_file.get("out_of_distribution_detector") or {},
            memory_compression_distiller=final_learning_file.get("memory_compression_distiller") or {},
            self_audit_score=final_learning_file.get("self_audit_score") or {},
            truth_first_promotion_objective=final_learning_file.get("truth_first_promotion_objective") or {},
            learning_velocity_dashboard=final_learning_file.get("learning_velocity_dashboard") or {},
            compiled_hunt_policy=final_learning_file.get("compiled_hunt_policy") or {},
            policy_executor=final_learning_file.get("policy_executor") or {},
            adaptive_worker_assignment=final_learning_file.get("adaptive_worker_assignment") or {},
            policy_backtester=final_learning_file.get("policy_backtester") or {},
            policy_mutation_engine=final_learning_file.get("policy_mutation_engine") or {},
            policy_tournament=final_learning_file.get("policy_tournament") or {},
            champion_challenger_memory=final_learning_file.get("champion_challenger_memory") or {},
            regime_specific_policies=final_learning_file.get("regime_specific_policies") or {},
            causal_graph_of_learning=final_learning_file.get("causal_graph_of_learning") or {},
            policy_safety_rail=final_learning_file.get("policy_safety_rail") or {},
            auto_promoted_field_manual=final_learning_file.get("auto_promoted_field_manual") or {},
            policy_drift_detector=final_learning_file.get("policy_drift_detector") or {},
            route_regime_half_life=final_learning_file.get("route_regime_half_life") or {},
            learning_market_map=final_learning_file.get("learning_market_map") or {},
            concept_drift_alarms=final_learning_file.get("concept_drift_alarms") or {},
            revalidation_scheduler=final_learning_file.get("revalidation_scheduler") or {},
            temporal_ensemble_policy=final_learning_file.get("temporal_ensemble_policy") or {},
            active_experiment_governor=final_learning_file.get("active_experiment_governor") or {},
            route_state_machine=final_learning_file.get("route_state_machine") or {},
            negative_knowledge_bank=final_learning_file.get("negative_knowledge_bank") or {},
            promotion_survivor_model=final_learning_file.get("promotion_survivor_model") or {},
            mutation_grammar_learner=final_learning_file.get("mutation_grammar_learner") or {},
            real_time_worker_rebalancer=final_learning_file.get("real_time_worker_rebalancer") or {},
            hunt_replay_simulator=final_learning_file.get("hunt_replay_simulator") or {},
            resurrection_engine=final_learning_file.get("resurrection_engine") or {},
            causal_mutation_attribution=final_learning_file.get("causal_mutation_attribution") or {},
            uncertainty_budgeting=final_learning_file.get("uncertainty_budgeting") or {},
            promotability_pareto_frontier=final_learning_file.get("promotability_pareto_frontier") or {},
            false_lesson_detector=final_learning_file.get("false_lesson_detector") or {},
            experiment_graduation_system=final_learning_file.get("experiment_graduation_system") or {},
            candidate_genealogy_diff_engine=final_learning_file.get("candidate_genealogy_diff_engine") or {},
            off_policy_hunt_evaluator=final_learning_file.get("off_policy_hunt_evaluator") or {},
            self_competition_league=final_learning_file.get("self_competition_league") or {},
            evidence_contract_engine=final_learning_file.get("evidence_contract_engine") or {},
            live_beater_quality_decomposer=final_learning_file.get("live_beater_quality_decomposer") or {},
            contradiction_detector=final_learning_file.get("contradiction_detector") or {},
            learning_conflict_resolver=final_learning_file.get("learning_conflict_resolver") or {},
            cohort_based_memory=final_learning_file.get("cohort_based_memory") or {},
            adaptive_hunt_throttle=final_learning_file.get("adaptive_hunt_throttle") or {},
            promotion_readiness_simulator=final_learning_file.get("promotion_readiness_simulator") or {},
            research_trace_ledger=final_learning_file.get("research_trace_ledger") or {},
            runtime_decision_kernel=final_learning_file.get("runtime_decision_kernel") or {},
            action_outcome_tracker=final_learning_file.get("action_outcome_tracker") or {},
            closed_loop_reward_model=final_learning_file.get("closed_loop_reward_model") or {},
            autonomous_hunt_planner=final_learning_file.get("autonomous_hunt_planner") or {},
            runtime_guardrails=final_learning_file.get("runtime_guardrails") or {},
            command_replay_ledger=final_learning_file.get("command_replay_ledger") or {},
            action_elo_league=final_learning_file.get("action_elo_league") or {},
            human_readable_hunt_brief=final_learning_file.get("human_readable_hunt_brief") or {},
            meta_hunt_strategy=final_learning_file.get("meta_hunt_strategy_learner") or {},
            run_to_run_postmortem=final_learning_file.get("run_to_run_postmortem") or {},
        )
        final_online_state = _attach_runtime_experiment_learning(final_online_state, final_learning_file)
        final_online_state["batch_size_multiplier"] = step2_online_learning.batch_size_multiplier(final_online_state)
        _write_compact_json(run_dir / "online_state.json", final_online_state)
        _write_runtime_controls(run_dir, final_online_state)
        _write_json(run_dir / "command_outcome_backfill.json", final_online_state.get("command_outcome_backfill") or {})
        _write_json(run_dir / "action_reward_calibration.json", final_online_state.get("action_reward_calibration") or {})
        artifact_paths["online_state"] = str((run_dir / "online_state.json").resolve())
        artifact_paths["learning_events"] = str((run_dir / "learning_events.jsonl").resolve())
        artifact_paths["streaming_telemetry"] = str((run_dir / "streaming_telemetry.jsonl").resolve())
        artifact_paths["runtime_command_adapter"] = str((run_dir / "runtime_command_adapter.json").resolve())
        artifact_paths["worker_job_contracts"] = str((run_dir / "worker_job_contracts.json").resolve())
        artifact_paths["command_delta_status"] = str((run_dir / "command_delta_status.json").resolve())
        artifact_paths["command_outcome_backfill"] = str((run_dir / "command_outcome_backfill.json").resolve())
        artifact_paths["action_reward_calibration"] = str((run_dir / "action_reward_calibration.json").resolve())
        for key in (
            "ab_route_experiment_executor",
            "champion_challenger_runtime_slots",
            "adaptive_experiment_stopping",
            "counterfactual_command_replay",
            "experiment_contamination_guard",
            "learning_rate_controller",
            "worker_learning_report_cards",
            "experiment_to_promotion_trace",
            "zero_yield_autopsy_engine",
            "stuck_loop_breaker",
            "opportunity_cost_meter",
            "search_space_coverage_map",
            "live_beater_scarcity_mode",
            "alias_trap_detector",
            "route_seed_quality_score",
            "recovery_playbook_generator",
            "cross_run_hunt_memory_compiler",
            "memory_reliability_scorer",
            "memory_falsification_queue",
            "belief_retirement_engine",
            "memory_provenance_explorer",
            "current_vs_historical_disagreement_monitor",
            "memory_stress_test_pack",
            "durable_memory_compression",
            "memory_qa_smoke_test",
            "hypothesis_factory",
            "hypothesis_market_maker",
            "real_time_bet_sizer",
            "contrarian_generator",
            "learning_stop_loss",
            "breakthrough_detector",
            "pattern_to_recipe_compiler",
            "hunt_narrative_memory",
            "attention_ledger",
            "wasted_spend_autopsy",
            "marginal_yield_curve",
            "explore_exploit_regret_tracker",
            "worker_alpha_attribution",
            "budget_reallocator",
            "time_aware_hunt_plan",
            "spend_efficiency_narrative",
            "idea_novelty_ledger",
            "idea_saturation_detector",
            "creative_leap_scorer",
            "failed_imagination_autopsy",
            "mutation_grammar_gap_finder",
            "novelty_budget_governor",
            "idea_lineage_map",
            "creative_brief_compiler",
            "learning_module_registry",
            "module_contribution_attribution",
            "module_conflict_detector",
            "module_reliability_scorer",
            "module_ablation_planner",
            "module_budget_governor",
            "learning_system_self_audit",
            "meta_learning_brief",
            "causal_intervention_scheduler",
            "experiment_power_calculator",
            "winner_fragility_profiler",
            "live_beater_source_attribution",
            "adaptive_search_temperature_controller",
            "route_interaction_learner",
            "false_discovery_firewall",
            "hunt_strategy_compiler_v2",
            "lesson_survival_tracker",
            "promotion_rejection_backpropagation",
            "lesson_decay_model",
            "cross_hunt_causal_memory",
            "evidence_chain_ledger",
            "learning_disagreement_court",
            "promotion_aware_search_objective",
            "scientific_run_brief_v2",
            "strategy_genome_registry",
            "strategy_mutation_engine",
            "strategy_tournament_memory",
            "regime_conditioned_strategy_selector",
            "meta_objective_optimizer",
            "exploration_debt_ledger",
            "adversarial_strategy_red_team",
            "autonomous_pivot_governor",
            "learning_roi_ledger",
            "artifact_usefulness_pruner",
            "decision_trace_explainer",
            "control_surface_conflict_auditor",
            "runtime_control_simplifier",
            "learning_cost_meter",
            "ablation_replay_harness",
            "architecture_fitness_brief",
            "learning_artifact_schema_registry",
            "artifact_dependency_graph",
            "incremental_learning_cache",
            "live_learning_dashboard_feed",
            "hunt_runbook_compiler",
            "learning_failure_sentinel",
            "cross_run_artifact_warehouse",
            "pre_hunt_readiness_gate",
            "online_causal_bandit",
            "variant_dna_attribution",
            "negative_gene_suppression",
            "live_winner_family_tree",
            "exploration_frontier_map",
            "adaptive_worker_personalities",
            "cycle_level_learning_delta",
            "promotion_rejection_predictor_v2",
            "counterfactual_hunt_simulator",
            "missed_winner_detector",
            "causal_regret_ledger",
            "adaptive_search_grammar_generator",
            "live_hypothesis_kill_scale_court",
            "route_interaction_matrix_v2",
            "promotion_survival_shadow_scoring",
            "hunt_autopilot_policy_compiler",
            "learning_claim_verifier",
            "causal_confidence_calibration",
            "false_discovery_early_warning",
            "adaptive_evidence_thresholds",
            "self_debate_search_council",
            "experiment_memory_compression",
            "learning_drift_monitor",
            "promotion_first_autopilot_v2",
            "multi_horizon_memory_stack",
            "lesson_half_life_engine_v2",
            "cross_hunt_strategy_replay",
            "temporal_regime_fingerprinting",
            "longitudinal_promotion_survival_model",
            "memory_conflict_court_v2",
            "strategy_aging_dashboard",
            "next_hunt_opening_policy_compiler",
            "question_driven_hunt_planner",
            "expected_information_gain_scorer_v2",
            "uncertainty_heatmap",
            "adaptive_experiment_sequencer",
            "learning_value_stop_loss",
            "causal_question_ledger",
            "worker_epistemic_roles_v2",
            "hunt_hypothesis_compiler",
            "experiment_contract_compiler",
            "control_route_matcher",
            "sequential_test_monitor",
            "causal_effect_size_ledger",
            "false_positive_pressure_gauge",
            "exploration_debt_paydown_planner",
            "promotion_aware_power_planner",
            "scientific_hunt_executive",
            "live_candidate_evidence_builder",
            "promotion_failure_predictor_v3",
            "evidence_gap_router",
            "review_ready_queue_v2",
            "promotion_evidence_scorecard",
            "candidate_lineage_explainer_v2",
            "live_vs_control_differential_report",
            "promotion_packet_executive",
            *WORLD_CLASS_META_KEYS,
            *ELITE_LEARNING_KEYS,
            *PROOF_LEARNING_KEYS,
            *CLOSED_LOOP_CONTROL_KEYS,
            *WORLD_MODEL_NERVOUS_KEYS,
            *ORCHESTRATION_LEARNING_KEYS,
            *FITNESS_SELECTION_KEYS,
            "pre_hunt_strategy_selector",
            "cold_start_route_pack_generator",
            "longitudinal_treatment_decay",
            "run_level_promotion_survival_feedback",
            "memory_conflict_arbiter",
            "hunt_opening_playbook",
            "cross_run_learning_regression_test",
        ):
            artifact_paths[key] = str((run_dir / f"{key}.json").resolve())
    learning = hunt_intel.learning_report(
        rows=final_top100,
        cycles=cycles,
        all_rows=list(final_rankings.get("diagnostic_rows") or final_rankings.get("decorated_rows") or final_top100),
        feedback_rows=(
            final_promotion_feedback
            + (list((hunt_intel.read_json(args.feedback_json, {}) or {}).get("feedback") or []) if args.feedback_json else [])
        ),
        source="run_step2_three_hour_hunt",
    )
    variant_funnel = _variant_funnel(args, run_dir, cycles, final_rankings)
    artifact_paths["variant_funnel"] = _write_json(run_dir / "variant_funnel.json", variant_funnel)
    tiny_guard = _tiny_run_calibration_guard(args, variant_funnel)
    artifact_paths["tiny_run_calibration_guard"] = _write_json(run_dir / "tiny_run_calibration_guard.json", tiny_guard)
    repair_queue = hunt_intel.read_json(run_dir / "promotion_evidence_repair_queue.json", {}) or _promotion_evidence_repair_queue(final_top100, limit=100)
    repair_directives = hunt_intel.read_json(run_dir / "promotion_repair_worker_directives.json", {}) or _repair_worker_directives(repair_queue)
    final_evidence_validation = hunt_intel.read_json(run_dir / "promotion_evidence_validation_report.json", {}) or {}
    final_evidence_by_variant = final_evidence_validation.get("by_variant") if isinstance(final_evidence_validation.get("by_variant"), dict) else {}
    promotion_survival_top100 = _promotion_survival_leaderboard(
        final_top100,
        limit=int(args.leaderboard_limit),
        evidence_by_variant=final_evidence_by_variant,
        sort_mode=str(getattr(args, "promotion_survival_sort", "score") or "score"),
    )
    enriched_by_variant = {str(row.get("variant") or ""): row for row in promotion_survival_top100}
    final_top100_enriched = [
        dict(row, **{
            key: value
            for key, value in (enriched_by_variant.get(str(row.get("variant") or "")) or {}).items()
            if key in {"promotion_survival_score", "promotion_minimum_bar", "promotion_evidence_tier", "promotion_distance", "evidence_adjusted_promotion_readiness_score", "evidence_adjusted_learning_tags"}
        })
        for row in final_top100
    ]
    stop_go_gate = _pre_hunt_stop_go_gate(args, run_dir, final_rankings, variant_funnel, final_online_state)
    artifact_paths["pre_hunt_stop_go_gate"] = _write_json(run_dir / "pre_hunt_stop_go_gate.json", stop_go_gate)
    hunt_brain = _hunt_brain_summary(
        args,
        run_dir,
        final_rankings,
        cycles,
        variant_funnel,
        repair_queue,
        repair_directives,
        stop_go_gate,
    )
    artifact_paths["hunt_brain_summary"] = _write_json(run_dir / "hunt_brain_summary.json", hunt_brain)
    lane_learning_summary = _lane_learning_summary(final_top100_enriched, cycles)
    artifact_paths["lane_learning_summary"] = _write_json(run_dir / "lane_learning_summary.json", lane_learning_summary)
    post_window_health = _post_window_health_report(
        args,
        run_dir,
        cycles,
        final_top100_enriched,
        variant_funnel,
        artifact_paths,
    )
    artifact_paths["post_window_health_report"] = _write_json(run_dir / "post_window_health_report.json", post_window_health)
    payload = {
        "schema_version": 1,
        "source": "run_step2_three_hour_hunt",
        "ok": bool(final_top100),
        "started_at_ct": started_at,
        "finished_at_ct": _now_ct(),
        "hours_requested": float(args.hours),
        "run_seconds_requested": run_seconds,
        "stop_after_sec": int(getattr(args, "stop_after_sec", 0) or 0),
        "window_minutes": float(getattr(args, "window_minutes", 0.0) or 0.0),
        "run_dir": str(run_dir.resolve()),
        "cycles": cycles,
        "live_only": bool(args.live_only),
        "behavioral_dedupe": bool(args.behavioral_dedupe),
        "exact_variant_count": bool(args.exact_variant_count),
        "runtime_control_mode": str(args.runtime_control_mode or "enforce"),
        "smoke_validation_mode": bool(args.smoke_validation_mode),
        "variant_funnel": variant_funnel,
        "pre_hunt_stop_go_gate": stop_go_gate,
        "stop_go_decision": stop_go_gate.get("decision"),
        "tiny_run_calibration_guard": tiny_guard,
        "hunt_brain_summary": hunt_brain,
        "lane_learning_summary": lane_learning_summary,
        "post_window_health_report": post_window_health,
        "ranking_stats": {k: final_rankings.get(k) for k in (
            "input_rows",
            "live_filtered_rows",
            "config_unique_rows",
            "behavior_unique_rows",
        )},
        "scorer_source_paths": final_rankings.get("scorer_source_paths") or [],
        "diagnostic_source_paths": final_rankings.get("diagnostic_source_paths") or [],
        "top10": final_top100_enriched[:10],
        "top100": final_top100_enriched,
        "top100_count": len(final_top100_enriched),
        "ranking_objective": str(args.ranking_objective),
        "promotion_survival_top10": promotion_survival_top100[:10],
        "promotion_survival_top100": promotion_survival_top100,
        "promotion_quality_top10": final_quality[:10],
        "promotion_quality_top100": final_quality,
        "promotion_readiness_top10": final_readiness[:10],
        "promotion_readiness_top100": final_readiness,
        "route_clusters": route_clusters,
        "next_hunt_plan": hunt_intel.next_hunt_plan(final_top100),
        "promotion_evidence_repair_queue": repair_queue,
        "promotion_repair_worker_directives": repair_directives,
        "promotion_evidence_validation_report": final_evidence_validation,
        "near_promotion_evidence_queue": hunt_intel.read_json(run_dir / "near_promotion_evidence_queue.json", {}) or {},
        "evidence_repair_lanes": hunt_intel.read_json(run_dir / "evidence_repair_lanes.json", {}) or {},
        "live_beater_quality_floor_top100": hunt_intel.read_json(run_dir / "live_beater_quality_floor_top100.json", {}) or {},
        "route_action_taxonomy": hunt_intel.read_json(run_dir / "route_action_taxonomy.json", {}) or {},
        "route_crowding_report": hunt_intel.read_json(run_dir / "route_crowding_report.json", {}) or {},
        "raw_pnl_vs_promotability_tradeoff": hunt_intel.read_json(run_dir / "raw_pnl_vs_promotability_tradeoff.json", {}) or {},
        "raw_vs_promotion_rank_delta": hunt_intel.read_json(run_dir / "raw_vs_promotion_rank_delta.json", {}) or {},
        "why_raw_rank1_not_promotion_rank1": hunt_intel.read_json(run_dir / "why_raw_rank1_not_promotion_rank1.json", {}) or {},
        "route_evidence_cards": hunt_intel.read_json(run_dir / "route_evidence_cards.json", {}) or {},
        "route_promotion_distance_cards": hunt_intel.read_json(run_dir / "route_promotion_distance_cards.json", {}) or {},
        "per_route_repair_lane_allocation": hunt_intel.read_json(run_dir / "per_route_repair_lane_allocation.json", {}) or {},
        "separate_learning_leaderboards": hunt_intel.read_json(run_dir / "separate_learning_leaderboards.json", {}) or {},
        "suspicious_winner_detector": hunt_intel.read_json(run_dir / "suspicious_winner_detector.json", {}) or {},
        "repair_attempt_memory": hunt_intel.read_json(run_dir / "repair_attempt_memory.json", {}) or {},
        "anti_alias_pressure": hunt_intel.read_json(run_dir / "anti_alias_pressure.json", {}) or {},
        "behavior_unique_generation_controls": hunt_intel.read_json(run_dir / "behavior_unique_generation_controls.json", {}) or {},
        "promotion_quality_protection_controls": hunt_intel.read_json(run_dir / "promotion_quality_protection_controls.json", {}) or {},
        "repair_route_budget_caps": hunt_intel.read_json(run_dir / "repair_route_budget_caps.json", {}) or {},
        "repair_allocation_by_evidence_weakness": hunt_intel.read_json(run_dir / "repair_allocation_by_evidence_weakness.json", {}) or {},
        "best_repair_candidate_by_promotion_distance": hunt_intel.read_json(run_dir / "best_repair_candidate_by_promotion_distance.json", {}) or {},
        "repair_narrowness_warning": hunt_intel.read_json(run_dir / "repair_narrowness_warning.json", {}) or {},
        "repair_progress_report": hunt_intel.read_json(run_dir / "repair_progress_report.json", {}) or {},
        "promotion_review_precheck": hunt_intel.read_json(run_dir / "promotion_review_precheck.json", {}) or {},
        "promotion_ready_summary": hunt_intel.read_json(run_dir / "promotion_ready_summary.json", {}) or {},
        "closest_to_promotion_summary": hunt_intel.read_json(run_dir / "closest_to_promotion_summary.json", {}) or {},
        "promotion_blocker_digest": hunt_intel.read_json(run_dir / "promotion_blocker_digest.json", {}) or {},
        "route_concentration_warning": hunt_intel.read_json(run_dir / "route_concentration_warning.json", {}) or {},
        "winner_route_distribution": hunt_intel.read_json(run_dir / "winner_route_distribution.json", {}) or {},
        "plan_vs_winner_route_allocation": hunt_intel.read_json(run_dir / "plan_vs_winner_route_allocation.json", {}) or {},
        "sampled_route_concentration": hunt_intel.read_json(run_dir / "sampled_route_concentration.json", {}) or {},
        "repair_regression_detector": hunt_intel.read_json(run_dir / "repair_regression_detector.json", {}) or {},
        "route_admission_gate": hunt_intel.read_json(run_dir / "route_admission_gate.json", {}) or {},
        "runtime_handoff_controls": hunt_intel.read_json(run_dir / "runtime_handoff_controls.json", {}) or {},
        "actual_route_allocation": hunt_intel.read_json(run_dir / "actual_route_allocation.json", {}) or {},
        "plan_vs_actual_route_allocation": hunt_intel.read_json(run_dir / "plan_vs_actual_route_allocation.json", {}) or {},
        "stale_focus_audit": hunt_intel.read_json(run_dir / "stale_focus_audit.json", {}) or {},
        "big_run_preflight_gate": hunt_intel.read_json(run_dir / "big_run_preflight_gate.json", {}) or {},
        "run_size_recommendation": hunt_intel.read_json(run_dir / "run_size_recommendation.json", {}) or {},
        "before_big_run_issue_register": hunt_intel.read_json(run_dir / "before_big_run_issue_register.json", {}) or {},
        **_final_summary_control_payloads(run_dir),
        "next_command_recipe": hunt_intel.read_json(run_dir / "next_command_recipe.json", {}) or {},
        "repair_success_scoreboard": hunt_intel.read_json(run_dir / "repair_success_scoreboard.json", {}) or {},
        "next_500_variant_plan": hunt_intel.read_json(run_dir / "next_500_variant_plan.json", {}) or {},
        "variant_family_saturation": hunt_intel.read_json(run_dir / "variant_family_saturation.json", {}) or {},
        "do_not_explore_controls": hunt_intel.read_json(run_dir / "do_not_explore_controls.json", {}) or {},
        "top_failure_summary": hunt_intel.read_json(run_dir / "top_failure_summary.json", {}) or {},
        "auto_hunt_recommendation": hunt_intel.read_json(run_dir / "auto_hunt_recommendation.json", {}) or {},
        "post_test_recommendation_severity": hunt_intel.read_json(run_dir / "post_test_recommendation_severity.json", {}) or {},
        "repair_cycle_now_decision": hunt_intel.read_json(run_dir / "repair_cycle_now_decision.json", {}) or {},
        "run_to_run_comparison": hunt_intel.read_json(run_dir / "run_to_run_comparison.json", {}) or {},
        "learning_report": learning,
        "annotated_top100": learning.get("annotated_top100") or {},
        "near_miss_archive": learning.get("near_miss_archive") or {},
        "adaptive_mutation_lanes": learning.get("adaptive_mutation_lanes") or {},
        "worker_role_plan": learning.get("worker_role_plan") or {},
        "candidate_lineage_graph": learning.get("candidate_lineage_graph") or {},
        "family_rejection_memory": learning.get("family_rejection_memory") or {},
        "active_experiment_plan": learning.get("active_experiment_plan") or {},
        "experiment_outcome_ledger": learning.get("experiment_outcome_ledger") or {},
        "causal_experiment_registry": learning.get("causal_experiment_registry") or {},
        "experiment_debt_queue": learning.get("experiment_debt_queue") or {},
        "information_gain_scoring": learning.get("information_gain_scoring") or {},
        "value_of_information_planner": learning.get("value_of_information_planner") or {},
        "decision_change_tracker": learning.get("decision_change_tracker") or {},
        "hypothesis_quality_scoring": learning.get("hypothesis_quality_scoring") or {},
        "evidence_sufficiency_gate": learning.get("evidence_sufficiency_gate") or {},
        "counterfactual_shadow_board": learning.get("counterfactual_shadow_board") or {},
        "prediction_calibration_ledger": learning.get("prediction_calibration_ledger") or {},
        "belief_revision_engine": learning.get("belief_revision_engine") or {},
        "adversarial_red_team_learner": learning.get("adversarial_red_team_learner") or {},
        "out_of_distribution_detector": learning.get("out_of_distribution_detector") or {},
        "memory_compression_distiller": learning.get("memory_compression_distiller") or {},
        "self_audit_score": learning.get("self_audit_score") or {},
        "truth_first_promotion_objective": learning.get("truth_first_promotion_objective") or {},
        "learning_velocity_dashboard": learning.get("learning_velocity_dashboard") or {},
        "compiled_hunt_policy": learning.get("compiled_hunt_policy") or {},
        "policy_executor": learning.get("policy_executor") or {},
        "adaptive_worker_assignment": learning.get("adaptive_worker_assignment") or {},
        "policy_backtester": learning.get("policy_backtester") or {},
        "policy_mutation_engine": learning.get("policy_mutation_engine") or {},
        "policy_tournament": learning.get("policy_tournament") or {},
        "champion_challenger_memory": learning.get("champion_challenger_memory") or {},
        "regime_specific_policies": learning.get("regime_specific_policies") or {},
        "causal_graph_of_learning": learning.get("causal_graph_of_learning") or {},
        "policy_safety_rail": learning.get("policy_safety_rail") or {},
        "auto_promoted_field_manual": learning.get("auto_promoted_field_manual") or {},
        "policy_drift_detector": learning.get("policy_drift_detector") or {},
        "route_regime_half_life": learning.get("route_regime_half_life") or {},
        "learning_market_map": learning.get("learning_market_map") or {},
        "concept_drift_alarms": learning.get("concept_drift_alarms") or {},
        "revalidation_scheduler": learning.get("revalidation_scheduler") or {},
        "temporal_ensemble_policy": learning.get("temporal_ensemble_policy") or {},
        "active_experiment_governor": learning.get("active_experiment_governor") or {},
        "route_state_machine": learning.get("route_state_machine") or {},
        "negative_knowledge_bank": learning.get("negative_knowledge_bank") or {},
        "promotion_survivor_model": learning.get("promotion_survivor_model") or {},
        "mutation_grammar_learner": learning.get("mutation_grammar_learner") or {},
        "real_time_worker_rebalancer": learning.get("real_time_worker_rebalancer") or {},
        "hunt_replay_simulator": learning.get("hunt_replay_simulator") or {},
        "resurrection_engine": learning.get("resurrection_engine") or {},
        "causal_mutation_attribution": learning.get("causal_mutation_attribution") or {},
        "uncertainty_budgeting": learning.get("uncertainty_budgeting") or {},
        "promotability_pareto_frontier": learning.get("promotability_pareto_frontier") or {},
        "false_lesson_detector": learning.get("false_lesson_detector") or {},
        "experiment_graduation_system": learning.get("experiment_graduation_system") or {},
        "candidate_genealogy_diff_engine": learning.get("candidate_genealogy_diff_engine") or {},
        "off_policy_hunt_evaluator": learning.get("off_policy_hunt_evaluator") or {},
        "self_competition_league": learning.get("self_competition_league") or {},
        "evidence_contract_engine": learning.get("evidence_contract_engine") or {},
        "live_beater_quality_decomposer": learning.get("live_beater_quality_decomposer") or {},
        "contradiction_detector": learning.get("contradiction_detector") or {},
        "learning_conflict_resolver": learning.get("learning_conflict_resolver") or {},
        "cohort_based_memory": learning.get("cohort_based_memory") or {},
        "adaptive_hunt_throttle": learning.get("adaptive_hunt_throttle") or {},
        "promotion_readiness_simulator": learning.get("promotion_readiness_simulator") or {},
        "research_trace_ledger": learning.get("research_trace_ledger") or {},
        "runtime_decision_kernel": learning.get("runtime_decision_kernel") or {},
        "action_outcome_tracker": learning.get("action_outcome_tracker") or {},
        "closed_loop_reward_model": learning.get("closed_loop_reward_model") or {},
        "autonomous_hunt_planner": learning.get("autonomous_hunt_planner") or {},
        "runtime_guardrails": learning.get("runtime_guardrails") or {},
        "command_replay_ledger": learning.get("command_replay_ledger") or {},
        "action_elo_league": learning.get("action_elo_league") or {},
        "human_readable_hunt_brief": learning.get("human_readable_hunt_brief") or {},
        "runtime_command_adapter": final_online_state.get("runtime_command_adapter") or hunt_intel.read_json(run_dir / "runtime_command_adapter.json", {}) or {},
        "worker_job_contracts": final_online_state.get("worker_job_contracts") or hunt_intel.read_json(run_dir / "worker_job_contracts.json", {}) or {},
        "command_delta_status": final_online_state.get("command_delta_status") or hunt_intel.read_json(run_dir / "command_delta_status.json", {}) or {},
        "command_outcome_backfill": final_online_state.get("command_outcome_backfill") or hunt_intel.read_json(run_dir / "command_outcome_backfill.json", {}) or {},
        "action_reward_calibration": final_online_state.get("action_reward_calibration") or hunt_intel.read_json(run_dir / "action_reward_calibration.json", {}) or {},
        "ab_route_experiment_executor": final_online_state.get("ab_route_experiment_executor") or learning.get("ab_route_experiment_executor") or {},
        "champion_challenger_runtime_slots": final_online_state.get("champion_challenger_runtime_slots") or learning.get("champion_challenger_runtime_slots") or {},
        "adaptive_experiment_stopping": final_online_state.get("adaptive_experiment_stopping") or learning.get("adaptive_experiment_stopping") or {},
        "counterfactual_command_replay": final_online_state.get("counterfactual_command_replay") or learning.get("counterfactual_command_replay") or {},
        "experiment_contamination_guard": final_online_state.get("experiment_contamination_guard") or learning.get("experiment_contamination_guard") or {},
        "learning_rate_controller": final_online_state.get("learning_rate_controller") or learning.get("learning_rate_controller") or {},
        "worker_learning_report_cards": final_online_state.get("worker_learning_report_cards") or learning.get("worker_learning_report_cards") or {},
        "experiment_to_promotion_trace": final_online_state.get("experiment_to_promotion_trace") or learning.get("experiment_to_promotion_trace") or {},
        "zero_yield_autopsy_engine": final_online_state.get("zero_yield_autopsy_engine") or learning.get("zero_yield_autopsy_engine") or {},
        "stuck_loop_breaker": final_online_state.get("stuck_loop_breaker") or learning.get("stuck_loop_breaker") or {},
        "opportunity_cost_meter": final_online_state.get("opportunity_cost_meter") or learning.get("opportunity_cost_meter") or {},
        "search_space_coverage_map": final_online_state.get("search_space_coverage_map") or learning.get("search_space_coverage_map") or {},
        "live_beater_scarcity_mode": final_online_state.get("live_beater_scarcity_mode") or learning.get("live_beater_scarcity_mode") or {},
        "alias_trap_detector": final_online_state.get("alias_trap_detector") or learning.get("alias_trap_detector") or {},
        "route_seed_quality_score": final_online_state.get("route_seed_quality_score") or learning.get("route_seed_quality_score") or {},
        "recovery_playbook_generator": final_online_state.get("recovery_playbook_generator") or learning.get("recovery_playbook_generator") or {},
        "cross_run_hunt_memory_compiler": final_online_state.get("cross_run_hunt_memory_compiler") or hunt_intel.read_json(run_dir / "cross_run_hunt_memory_compiler.json", {}) or {},
        "memory_reliability_scorer": final_online_state.get("memory_reliability_scorer") or hunt_intel.read_json(run_dir / "memory_reliability_scorer.json", {}) or {},
        "memory_falsification_queue": final_online_state.get("memory_falsification_queue") or hunt_intel.read_json(run_dir / "memory_falsification_queue.json", {}) or {},
        "belief_retirement_engine": final_online_state.get("belief_retirement_engine") or hunt_intel.read_json(run_dir / "belief_retirement_engine.json", {}) or {},
        "memory_provenance_explorer": final_online_state.get("memory_provenance_explorer") or hunt_intel.read_json(run_dir / "memory_provenance_explorer.json", {}) or {},
        "current_vs_historical_disagreement_monitor": final_online_state.get("current_vs_historical_disagreement_monitor") or hunt_intel.read_json(run_dir / "current_vs_historical_disagreement_monitor.json", {}) or {},
        "memory_stress_test_pack": final_online_state.get("memory_stress_test_pack") or hunt_intel.read_json(run_dir / "memory_stress_test_pack.json", {}) or {},
        "durable_memory_compression": final_online_state.get("durable_memory_compression") or hunt_intel.read_json(run_dir / "durable_memory_compression.json", {}) or {},
        "memory_qa_smoke_test": final_online_state.get("memory_qa_smoke_test") or hunt_intel.read_json(run_dir / "memory_qa_smoke_test.json", {}) or {},
        "hypothesis_factory": final_online_state.get("hypothesis_factory") or hunt_intel.read_json(run_dir / "hypothesis_factory.json", {}) or {},
        "hypothesis_market_maker": final_online_state.get("hypothesis_market_maker") or hunt_intel.read_json(run_dir / "hypothesis_market_maker.json", {}) or {},
        "real_time_bet_sizer": final_online_state.get("real_time_bet_sizer") or hunt_intel.read_json(run_dir / "real_time_bet_sizer.json", {}) or {},
        "contrarian_generator": final_online_state.get("contrarian_generator") or hunt_intel.read_json(run_dir / "contrarian_generator.json", {}) or {},
        "learning_stop_loss": final_online_state.get("learning_stop_loss") or hunt_intel.read_json(run_dir / "learning_stop_loss.json", {}) or {},
        "breakthrough_detector": final_online_state.get("breakthrough_detector") or hunt_intel.read_json(run_dir / "breakthrough_detector.json", {}) or {},
        "pattern_to_recipe_compiler": final_online_state.get("pattern_to_recipe_compiler") or hunt_intel.read_json(run_dir / "pattern_to_recipe_compiler.json", {}) or {},
        "hunt_narrative_memory": final_online_state.get("hunt_narrative_memory") or hunt_intel.read_json(run_dir / "hunt_narrative_memory.json", {}) or {},
        "attention_ledger": final_online_state.get("attention_ledger") or hunt_intel.read_json(run_dir / "attention_ledger.json", {}) or {},
        "wasted_spend_autopsy": final_online_state.get("wasted_spend_autopsy") or hunt_intel.read_json(run_dir / "wasted_spend_autopsy.json", {}) or {},
        "marginal_yield_curve": final_online_state.get("marginal_yield_curve") or hunt_intel.read_json(run_dir / "marginal_yield_curve.json", {}) or {},
        "explore_exploit_regret_tracker": final_online_state.get("explore_exploit_regret_tracker") or hunt_intel.read_json(run_dir / "explore_exploit_regret_tracker.json", {}) or {},
        "worker_alpha_attribution": final_online_state.get("worker_alpha_attribution") or hunt_intel.read_json(run_dir / "worker_alpha_attribution.json", {}) or {},
        "budget_reallocator": final_online_state.get("budget_reallocator") or hunt_intel.read_json(run_dir / "budget_reallocator.json", {}) or {},
        "time_aware_hunt_plan": final_online_state.get("time_aware_hunt_plan") or hunt_intel.read_json(run_dir / "time_aware_hunt_plan.json", {}) or {},
        "spend_efficiency_narrative": final_online_state.get("spend_efficiency_narrative") or hunt_intel.read_json(run_dir / "spend_efficiency_narrative.json", {}) or {},
        "idea_novelty_ledger": final_online_state.get("idea_novelty_ledger") or hunt_intel.read_json(run_dir / "idea_novelty_ledger.json", {}) or {},
        "idea_saturation_detector": final_online_state.get("idea_saturation_detector") or hunt_intel.read_json(run_dir / "idea_saturation_detector.json", {}) or {},
        "creative_leap_scorer": final_online_state.get("creative_leap_scorer") or hunt_intel.read_json(run_dir / "creative_leap_scorer.json", {}) or {},
        "failed_imagination_autopsy": final_online_state.get("failed_imagination_autopsy") or hunt_intel.read_json(run_dir / "failed_imagination_autopsy.json", {}) or {},
        "mutation_grammar_gap_finder": final_online_state.get("mutation_grammar_gap_finder") or hunt_intel.read_json(run_dir / "mutation_grammar_gap_finder.json", {}) or {},
        "novelty_budget_governor": final_online_state.get("novelty_budget_governor") or hunt_intel.read_json(run_dir / "novelty_budget_governor.json", {}) or {},
        "idea_lineage_map": final_online_state.get("idea_lineage_map") or hunt_intel.read_json(run_dir / "idea_lineage_map.json", {}) or {},
        "creative_brief_compiler": final_online_state.get("creative_brief_compiler") or hunt_intel.read_json(run_dir / "creative_brief_compiler.json", {}) or {},
        "learning_module_registry": final_online_state.get("learning_module_registry") or hunt_intel.read_json(run_dir / "learning_module_registry.json", {}) or {},
        "module_contribution_attribution": final_online_state.get("module_contribution_attribution") or hunt_intel.read_json(run_dir / "module_contribution_attribution.json", {}) or {},
        "module_conflict_detector": final_online_state.get("module_conflict_detector") or hunt_intel.read_json(run_dir / "module_conflict_detector.json", {}) or {},
        "module_reliability_scorer": final_online_state.get("module_reliability_scorer") or hunt_intel.read_json(run_dir / "module_reliability_scorer.json", {}) or {},
        "module_ablation_planner": final_online_state.get("module_ablation_planner") or hunt_intel.read_json(run_dir / "module_ablation_planner.json", {}) or {},
        "module_budget_governor": final_online_state.get("module_budget_governor") or hunt_intel.read_json(run_dir / "module_budget_governor.json", {}) or {},
        "learning_system_self_audit": final_online_state.get("learning_system_self_audit") or hunt_intel.read_json(run_dir / "learning_system_self_audit.json", {}) or {},
        "meta_learning_brief": final_online_state.get("meta_learning_brief") or hunt_intel.read_json(run_dir / "meta_learning_brief.json", {}) or {},
        "causal_intervention_scheduler": final_online_state.get("causal_intervention_scheduler") or hunt_intel.read_json(run_dir / "causal_intervention_scheduler.json", {}) or {},
        "experiment_power_calculator": final_online_state.get("experiment_power_calculator") or hunt_intel.read_json(run_dir / "experiment_power_calculator.json", {}) or {},
        "winner_fragility_profiler": final_online_state.get("winner_fragility_profiler") or hunt_intel.read_json(run_dir / "winner_fragility_profiler.json", {}) or {},
        "live_beater_source_attribution": final_online_state.get("live_beater_source_attribution") or hunt_intel.read_json(run_dir / "live_beater_source_attribution.json", {}) or {},
        "adaptive_search_temperature_controller": final_online_state.get("adaptive_search_temperature_controller") or hunt_intel.read_json(run_dir / "adaptive_search_temperature_controller.json", {}) or {},
        "route_interaction_learner": final_online_state.get("route_interaction_learner") or hunt_intel.read_json(run_dir / "route_interaction_learner.json", {}) or {},
        "false_discovery_firewall": final_online_state.get("false_discovery_firewall") or hunt_intel.read_json(run_dir / "false_discovery_firewall.json", {}) or {},
        "hunt_strategy_compiler_v2": final_online_state.get("hunt_strategy_compiler_v2") or hunt_intel.read_json(run_dir / "hunt_strategy_compiler_v2.json", {}) or {},
        "lesson_survival_tracker": final_online_state.get("lesson_survival_tracker") or hunt_intel.read_json(run_dir / "lesson_survival_tracker.json", {}) or {},
        "promotion_rejection_backpropagation": final_online_state.get("promotion_rejection_backpropagation") or hunt_intel.read_json(run_dir / "promotion_rejection_backpropagation.json", {}) or {},
        "lesson_decay_model": final_online_state.get("lesson_decay_model") or hunt_intel.read_json(run_dir / "lesson_decay_model.json", {}) or {},
        "cross_hunt_causal_memory": final_online_state.get("cross_hunt_causal_memory") or hunt_intel.read_json(run_dir / "cross_hunt_causal_memory.json", {}) or {},
        "evidence_chain_ledger": final_online_state.get("evidence_chain_ledger") or hunt_intel.read_json(run_dir / "evidence_chain_ledger.json", {}) or {},
        "learning_disagreement_court": final_online_state.get("learning_disagreement_court") or hunt_intel.read_json(run_dir / "learning_disagreement_court.json", {}) or {},
        "promotion_aware_search_objective": final_online_state.get("promotion_aware_search_objective") or hunt_intel.read_json(run_dir / "promotion_aware_search_objective.json", {}) or {},
        "scientific_run_brief_v2": final_online_state.get("scientific_run_brief_v2") or hunt_intel.read_json(run_dir / "scientific_run_brief_v2.json", {}) or {},
        "strategy_genome_registry": final_online_state.get("strategy_genome_registry") or hunt_intel.read_json(run_dir / "strategy_genome_registry.json", {}) or {},
        "strategy_mutation_engine": final_online_state.get("strategy_mutation_engine") or hunt_intel.read_json(run_dir / "strategy_mutation_engine.json", {}) or {},
        "strategy_tournament_memory": final_online_state.get("strategy_tournament_memory") or hunt_intel.read_json(run_dir / "strategy_tournament_memory.json", {}) or {},
        "regime_conditioned_strategy_selector": final_online_state.get("regime_conditioned_strategy_selector") or hunt_intel.read_json(run_dir / "regime_conditioned_strategy_selector.json", {}) or {},
        "meta_objective_optimizer": final_online_state.get("meta_objective_optimizer") or hunt_intel.read_json(run_dir / "meta_objective_optimizer.json", {}) or {},
        "exploration_debt_ledger": final_online_state.get("exploration_debt_ledger") or hunt_intel.read_json(run_dir / "exploration_debt_ledger.json", {}) or {},
        "adversarial_strategy_red_team": final_online_state.get("adversarial_strategy_red_team") or hunt_intel.read_json(run_dir / "adversarial_strategy_red_team.json", {}) or {},
        "autonomous_pivot_governor": final_online_state.get("autonomous_pivot_governor") or hunt_intel.read_json(run_dir / "autonomous_pivot_governor.json", {}) or {},
        "learning_roi_ledger": final_online_state.get("learning_roi_ledger") or hunt_intel.read_json(run_dir / "learning_roi_ledger.json", {}) or {},
        "artifact_usefulness_pruner": final_online_state.get("artifact_usefulness_pruner") or hunt_intel.read_json(run_dir / "artifact_usefulness_pruner.json", {}) or {},
        "decision_trace_explainer": final_online_state.get("decision_trace_explainer") or hunt_intel.read_json(run_dir / "decision_trace_explainer.json", {}) or {},
        "control_surface_conflict_auditor": final_online_state.get("control_surface_conflict_auditor") or hunt_intel.read_json(run_dir / "control_surface_conflict_auditor.json", {}) or {},
        "runtime_control_simplifier": final_online_state.get("runtime_control_simplifier") or hunt_intel.read_json(run_dir / "runtime_control_simplifier.json", {}) or {},
        "learning_cost_meter": final_online_state.get("learning_cost_meter") or hunt_intel.read_json(run_dir / "learning_cost_meter.json", {}) or {},
        "ablation_replay_harness": final_online_state.get("ablation_replay_harness") or hunt_intel.read_json(run_dir / "ablation_replay_harness.json", {}) or {},
        "architecture_fitness_brief": final_online_state.get("architecture_fitness_brief") or hunt_intel.read_json(run_dir / "architecture_fitness_brief.json", {}) or {},
        "learning_artifact_schema_registry": final_online_state.get("learning_artifact_schema_registry") or hunt_intel.read_json(run_dir / "learning_artifact_schema_registry.json", {}) or {},
        "artifact_dependency_graph": final_online_state.get("artifact_dependency_graph") or hunt_intel.read_json(run_dir / "artifact_dependency_graph.json", {}) or {},
        "incremental_learning_cache": final_online_state.get("incremental_learning_cache") or hunt_intel.read_json(run_dir / "incremental_learning_cache.json", {}) or {},
        "live_learning_dashboard_feed": final_online_state.get("live_learning_dashboard_feed") or hunt_intel.read_json(run_dir / "live_learning_dashboard_feed.json", {}) or {},
        "hunt_runbook_compiler": final_online_state.get("hunt_runbook_compiler") or hunt_intel.read_json(run_dir / "hunt_runbook_compiler.json", {}) or {},
        "learning_failure_sentinel": final_online_state.get("learning_failure_sentinel") or hunt_intel.read_json(run_dir / "learning_failure_sentinel.json", {}) or {},
        "cross_run_artifact_warehouse": final_online_state.get("cross_run_artifact_warehouse") or hunt_intel.read_json(run_dir / "cross_run_artifact_warehouse.json", {}) or {},
        "pre_hunt_readiness_gate": final_online_state.get("pre_hunt_readiness_gate") or hunt_intel.read_json(run_dir / "pre_hunt_readiness_gate.json", {}) or {},
        "online_causal_bandit": final_online_state.get("online_causal_bandit") or hunt_intel.read_json(run_dir / "online_causal_bandit.json", {}) or {},
        "variant_dna_attribution": final_online_state.get("variant_dna_attribution") or hunt_intel.read_json(run_dir / "variant_dna_attribution.json", {}) or {},
        "negative_gene_suppression": final_online_state.get("negative_gene_suppression") or hunt_intel.read_json(run_dir / "negative_gene_suppression.json", {}) or {},
        "live_winner_family_tree": final_online_state.get("live_winner_family_tree") or hunt_intel.read_json(run_dir / "live_winner_family_tree.json", {}) or {},
        "exploration_frontier_map": final_online_state.get("exploration_frontier_map") or hunt_intel.read_json(run_dir / "exploration_frontier_map.json", {}) or {},
        "adaptive_worker_personalities": final_online_state.get("adaptive_worker_personalities") or hunt_intel.read_json(run_dir / "adaptive_worker_personalities.json", {}) or {},
        "cycle_level_learning_delta": final_online_state.get("cycle_level_learning_delta") or hunt_intel.read_json(run_dir / "cycle_level_learning_delta.json", {}) or {},
        "promotion_rejection_predictor_v2": final_online_state.get("promotion_rejection_predictor_v2") or hunt_intel.read_json(run_dir / "promotion_rejection_predictor_v2.json", {}) or {},
        "counterfactual_hunt_simulator": final_online_state.get("counterfactual_hunt_simulator") or hunt_intel.read_json(run_dir / "counterfactual_hunt_simulator.json", {}) or {},
        "missed_winner_detector": final_online_state.get("missed_winner_detector") or hunt_intel.read_json(run_dir / "missed_winner_detector.json", {}) or {},
        "causal_regret_ledger": final_online_state.get("causal_regret_ledger") or hunt_intel.read_json(run_dir / "causal_regret_ledger.json", {}) or {},
        "adaptive_search_grammar_generator": final_online_state.get("adaptive_search_grammar_generator") or hunt_intel.read_json(run_dir / "adaptive_search_grammar_generator.json", {}) or {},
        "live_hypothesis_kill_scale_court": final_online_state.get("live_hypothesis_kill_scale_court") or hunt_intel.read_json(run_dir / "live_hypothesis_kill_scale_court.json", {}) or {},
        "route_interaction_matrix_v2": final_online_state.get("route_interaction_matrix_v2") or hunt_intel.read_json(run_dir / "route_interaction_matrix_v2.json", {}) or {},
        "promotion_survival_shadow_scoring": final_online_state.get("promotion_survival_shadow_scoring") or hunt_intel.read_json(run_dir / "promotion_survival_shadow_scoring.json", {}) or {},
        "hunt_autopilot_policy_compiler": final_online_state.get("hunt_autopilot_policy_compiler") or hunt_intel.read_json(run_dir / "hunt_autopilot_policy_compiler.json", {}) or {},
        "learning_claim_verifier": final_online_state.get("learning_claim_verifier") or hunt_intel.read_json(run_dir / "learning_claim_verifier.json", {}) or {},
        "causal_confidence_calibration": final_online_state.get("causal_confidence_calibration") or hunt_intel.read_json(run_dir / "causal_confidence_calibration.json", {}) or {},
        "false_discovery_early_warning": final_online_state.get("false_discovery_early_warning") or hunt_intel.read_json(run_dir / "false_discovery_early_warning.json", {}) or {},
        "adaptive_evidence_thresholds": final_online_state.get("adaptive_evidence_thresholds") or hunt_intel.read_json(run_dir / "adaptive_evidence_thresholds.json", {}) or {},
        "self_debate_search_council": final_online_state.get("self_debate_search_council") or hunt_intel.read_json(run_dir / "self_debate_search_council.json", {}) or {},
        "experiment_memory_compression": final_online_state.get("experiment_memory_compression") or hunt_intel.read_json(run_dir / "experiment_memory_compression.json", {}) or {},
        "learning_drift_monitor": final_online_state.get("learning_drift_monitor") or hunt_intel.read_json(run_dir / "learning_drift_monitor.json", {}) or {},
        "promotion_first_autopilot_v2": final_online_state.get("promotion_first_autopilot_v2") or hunt_intel.read_json(run_dir / "promotion_first_autopilot_v2.json", {}) or {},
        "multi_horizon_memory_stack": final_online_state.get("multi_horizon_memory_stack") or hunt_intel.read_json(run_dir / "multi_horizon_memory_stack.json", {}) or {},
        "lesson_half_life_engine_v2": final_online_state.get("lesson_half_life_engine_v2") or hunt_intel.read_json(run_dir / "lesson_half_life_engine_v2.json", {}) or {},
        "cross_hunt_strategy_replay": final_online_state.get("cross_hunt_strategy_replay") or hunt_intel.read_json(run_dir / "cross_hunt_strategy_replay.json", {}) or {},
        "temporal_regime_fingerprinting": final_online_state.get("temporal_regime_fingerprinting") or hunt_intel.read_json(run_dir / "temporal_regime_fingerprinting.json", {}) or {},
        "longitudinal_promotion_survival_model": final_online_state.get("longitudinal_promotion_survival_model") or hunt_intel.read_json(run_dir / "longitudinal_promotion_survival_model.json", {}) or {},
        "memory_conflict_court_v2": final_online_state.get("memory_conflict_court_v2") or hunt_intel.read_json(run_dir / "memory_conflict_court_v2.json", {}) or {},
        "strategy_aging_dashboard": final_online_state.get("strategy_aging_dashboard") or hunt_intel.read_json(run_dir / "strategy_aging_dashboard.json", {}) or {},
        "next_hunt_opening_policy_compiler": final_online_state.get("next_hunt_opening_policy_compiler") or hunt_intel.read_json(run_dir / "next_hunt_opening_policy_compiler.json", {}) or {},
        "question_driven_hunt_planner": final_online_state.get("question_driven_hunt_planner") or hunt_intel.read_json(run_dir / "question_driven_hunt_planner.json", {}) or {},
        "expected_information_gain_scorer_v2": final_online_state.get("expected_information_gain_scorer_v2") or hunt_intel.read_json(run_dir / "expected_information_gain_scorer_v2.json", {}) or {},
        "uncertainty_heatmap": final_online_state.get("uncertainty_heatmap") or hunt_intel.read_json(run_dir / "uncertainty_heatmap.json", {}) or {},
        "adaptive_experiment_sequencer": final_online_state.get("adaptive_experiment_sequencer") or hunt_intel.read_json(run_dir / "adaptive_experiment_sequencer.json", {}) or {},
        "learning_value_stop_loss": final_online_state.get("learning_value_stop_loss") or hunt_intel.read_json(run_dir / "learning_value_stop_loss.json", {}) or {},
        "causal_question_ledger": final_online_state.get("causal_question_ledger") or hunt_intel.read_json(run_dir / "causal_question_ledger.json", {}) or {},
        "worker_epistemic_roles_v2": final_online_state.get("worker_epistemic_roles_v2") or hunt_intel.read_json(run_dir / "worker_epistemic_roles_v2.json", {}) or {},
        "hunt_hypothesis_compiler": final_online_state.get("hunt_hypothesis_compiler") or hunt_intel.read_json(run_dir / "hunt_hypothesis_compiler.json", {}) or {},
        "experiment_contract_compiler": final_online_state.get("experiment_contract_compiler") or hunt_intel.read_json(run_dir / "experiment_contract_compiler.json", {}) or {},
        "control_route_matcher": final_online_state.get("control_route_matcher") or hunt_intel.read_json(run_dir / "control_route_matcher.json", {}) or {},
        "sequential_test_monitor": final_online_state.get("sequential_test_monitor") or hunt_intel.read_json(run_dir / "sequential_test_monitor.json", {}) or {},
        "causal_effect_size_ledger": final_online_state.get("causal_effect_size_ledger") or hunt_intel.read_json(run_dir / "causal_effect_size_ledger.json", {}) or {},
        "false_positive_pressure_gauge": final_online_state.get("false_positive_pressure_gauge") or hunt_intel.read_json(run_dir / "false_positive_pressure_gauge.json", {}) or {},
        "exploration_debt_paydown_planner": final_online_state.get("exploration_debt_paydown_planner") or hunt_intel.read_json(run_dir / "exploration_debt_paydown_planner.json", {}) or {},
        "promotion_aware_power_planner": final_online_state.get("promotion_aware_power_planner") or hunt_intel.read_json(run_dir / "promotion_aware_power_planner.json", {}) or {},
        "scientific_hunt_executive": final_online_state.get("scientific_hunt_executive") or hunt_intel.read_json(run_dir / "scientific_hunt_executive.json", {}) or {},
        "live_candidate_evidence_builder": final_online_state.get("live_candidate_evidence_builder") or hunt_intel.read_json(run_dir / "live_candidate_evidence_builder.json", {}) or {},
        "promotion_failure_predictor_v3": final_online_state.get("promotion_failure_predictor_v3") or hunt_intel.read_json(run_dir / "promotion_failure_predictor_v3.json", {}) or {},
        "evidence_gap_router": final_online_state.get("evidence_gap_router") or hunt_intel.read_json(run_dir / "evidence_gap_router.json", {}) or {},
        "review_ready_queue_v2": final_online_state.get("review_ready_queue_v2") or hunt_intel.read_json(run_dir / "review_ready_queue_v2.json", {}) or {},
        "promotion_evidence_scorecard": final_online_state.get("promotion_evidence_scorecard") or hunt_intel.read_json(run_dir / "promotion_evidence_scorecard.json", {}) or {},
        "candidate_lineage_explainer_v2": final_online_state.get("candidate_lineage_explainer_v2") or hunt_intel.read_json(run_dir / "candidate_lineage_explainer_v2.json", {}) or {},
        "live_vs_control_differential_report": final_online_state.get("live_vs_control_differential_report") or hunt_intel.read_json(run_dir / "live_vs_control_differential_report.json", {}) or {},
        "promotion_packet_executive": final_online_state.get("promotion_packet_executive") or hunt_intel.read_json(run_dir / "promotion_packet_executive.json", {}) or {},
        **{key: final_online_state.get(key) or hunt_intel.read_json(run_dir / f"{key}.json", {}) or {} for key in WORLD_CLASS_META_KEYS},
        **{key: final_online_state.get(key) or hunt_intel.read_json(run_dir / f"{key}.json", {}) or {} for key in ELITE_LEARNING_KEYS},
        **{key: final_online_state.get(key) or hunt_intel.read_json(run_dir / f"{key}.json", {}) or {} for key in PROOF_LEARNING_KEYS},
        **{key: final_online_state.get(key) or hunt_intel.read_json(run_dir / f"{key}.json", {}) or {} for key in CLOSED_LOOP_CONTROL_KEYS},
        **{key: final_online_state.get(key) or hunt_intel.read_json(run_dir / f"{key}.json", {}) or {} for key in WORLD_MODEL_NERVOUS_KEYS},
        **{key: final_online_state.get(key) or hunt_intel.read_json(run_dir / f"{key}.json", {}) or {} for key in ORCHESTRATION_LEARNING_KEYS},
        **{key: final_online_state.get(key) or hunt_intel.read_json(run_dir / f"{key}.json", {}) or {} for key in FITNESS_SELECTION_KEYS},
        "pre_hunt_strategy_selector": final_online_state.get("pre_hunt_strategy_selector") or hunt_intel.read_json(run_dir / "pre_hunt_strategy_selector.json", {}) or {},
        "cold_start_route_pack_generator": final_online_state.get("cold_start_route_pack_generator") or hunt_intel.read_json(run_dir / "cold_start_route_pack_generator.json", {}) or {},
        "longitudinal_treatment_decay": final_online_state.get("longitudinal_treatment_decay") or hunt_intel.read_json(run_dir / "longitudinal_treatment_decay.json", {}) or {},
        "run_level_promotion_survival_feedback": final_online_state.get("run_level_promotion_survival_feedback") or hunt_intel.read_json(run_dir / "run_level_promotion_survival_feedback.json", {}) or {},
        "memory_conflict_arbiter": final_online_state.get("memory_conflict_arbiter") or hunt_intel.read_json(run_dir / "memory_conflict_arbiter.json", {}) or {},
        "hunt_opening_playbook": final_online_state.get("hunt_opening_playbook") or hunt_intel.read_json(run_dir / "hunt_opening_playbook.json", {}) or {},
        "cross_run_learning_regression_test": final_online_state.get("cross_run_learning_regression_test") or hunt_intel.read_json(run_dir / "cross_run_learning_regression_test.json", {}) or {},
        "meta_hunt_strategy_learner": learning.get("meta_hunt_strategy_learner") or {},
        "run_to_run_postmortem": learning.get("run_to_run_postmortem") or {},
        "treatment_effects": learning.get("treatment_effects") or {},
        "treatment_prior_model": learning.get("treatment_prior_model") or {},
        "treatment_worker_budget": learning.get("treatment_worker_budget") or {},
        "treatment_confidence_model": learning.get("treatment_confidence_model") or {},
        "controlled_parent_sibling_experiments": learning.get("controlled_parent_sibling_experiments") or {},
        "live_beater_failure_autopsy": learning.get("live_beater_failure_autopsy") or {},
        "worker_specialization_memory": learning.get("worker_specialization_memory") or {},
        "regime_aware_learning": learning.get("regime_aware_learning") or {},
        "promotion_reject_simulator": learning.get("promotion_reject_simulator") or {},
        "search_portfolio_manager": learning.get("search_portfolio_manager") or {},
        "micro_promotion_review": final_micro_promotion,
        "promotion_ready_queue": final_micro_promotion.get("promotion_ready_queue") if isinstance(final_micro_promotion, dict) else {},
        "online_state": final_online_state,
        "status_policy": _status_policy(args),
        "artifact_paths": artifact_paths,
        "learning_notes": [
            "Raw P/L top100 is live-only by default and behavior-deduped.",
            "variant_funnel shows requested, scored, live-beating, config-unique, and behavior-unique counts.",
            "exact_variant_count locks router scoring count; runtime_control_mode=observe records learning controls without letting them throttle smoke runs.",
            "Promotion-quality top100 ranks by Live delta plus robustness and concentration proxies.",
            "Promotion-readiness top100 favors candidates that look more likely to survive review.",
            "Near-miss archive preserves useful live-beating DNA that failed readiness so future lanes can repair it.",
            "Status updates default to every 5 minutes unless --status-interval-sec is set lower.",
            "Route clusters and next_hunt_plan are generated from the kept candidates after each checkpoint.",
            "Active experiment plans assign small A/B and repair tests so workers answer explicit hunt questions.",
            "Treatment effect analysis updates priors and worker budget while the hunt is still running.",
            "Controlled sibling experiments, treatment confidence, regime splits, and portfolio budgets guide the next hunt cycle.",
            "Causal experiment registry, experiment debt, information-gain scoring, and run postmortems are saved for future hunts.",
            "Value-of-information, evidence gates, shadow validation, and compiled policy artifacts make the hunt executive.",
            "Prediction calibration, belief revision, OOD detection, red-team checks, and truth-first objective keep learning honest.",
            "Policy tournament, champion/challenger memory, and safety rails evolve better hunt policies.",
            "Promote only after robustness, parity, and candidate lifecycle gates pass on the saved finalists.",
        ],
    }
    payload["artifact_budget_report"] = {}
    payload["learning_roi_report"] = {}
    payload["speed_diagnostics"] = {}
    payload["path"] = _write_compact_json(run_dir / "final_summary.json", _final_summary_payload_for_write(args, payload, artifact_paths))
    artifact_paths["final_summary"] = payload["path"]
    running_final = {**payload, "top100_count": len(final_top100)}
    if str(getattr(args, "artifact_profile", "compact") or "compact") == "full":
        artifact_paths["running_top100"] = _write_json(run_dir / "running_top100.json", running_final)
    else:
        if _write_full_running_top100(args):
            artifact_paths["running_top100_full"] = _write_json(run_dir / "running_top100.full.json", running_final)
        artifact_paths["running_top100"] = _write_compact_json(
            run_dir / "running_top100.json",
            {
                "schema_version": 1,
                "source": "run_step2_three_hour_hunt",
                "updated_at_ct": _now_ct(),
                "run_dir": str(run_dir.resolve()),
                "cycles_completed": len(cycles),
                "top100_count": len(final_top100_enriched),
                "variant_funnel": variant_funnel,
                "ranking_stats": payload.get("ranking_stats") or {},
                "top10": [_compact_candidate_digest(row) for row in final_top100_enriched[:10]],
                "promotion_survival_top10": [_compact_candidate_digest(row) for row in promotion_survival_top100[:10]],
                "promotion_ready_summary": payload.get("promotion_ready_summary") or {},
                "top_failure_summary": payload.get("top_failure_summary") or {},
                "artifact_paths": {
                    "full": artifact_paths.get("running_top100_full"),
                    "full_omitted_reason": "" if artifact_paths.get("running_top100_full") else "duplicate_of_final_summary",
                    "final_summary": artifact_paths.get("final_summary"),
                    "operator_digest": str((run_dir / "operator_digest.json").resolve()),
                },
            },
        )
    payload["learning_db"] = _persist_learning_db(args, run_dir)
    artifact_budget = _artifact_budget_report(run_dir, warning_mb=float(getattr(args, "artifact_size_warning_mb", 10.0) or 10.0))
    artifact_paths["artifact_budget_report"] = _write_json(run_dir / "artifact_budget_report.json", artifact_budget)
    learning_roi = _learning_roi_report(run_dir, final_rankings, cycles, final_online_state, artifact_budget)
    artifact_paths["learning_roi_report"] = _write_json(run_dir / "learning_roi_report.json", learning_roi)
    speed_diagnostics = _speed_diagnostics_report(args, run_dir, cycles, artifact_budget, started_at=started_epoch)
    artifact_paths["speed_diagnostics"] = _write_json(run_dir / "speed_diagnostics.json", speed_diagnostics)
    loop_progress = _loop_progress_summary(run_dir)
    artifact_paths["loop_progress_summary"] = _write_compact_json(run_dir / "loop_progress_summary.json", loop_progress)
    payload["artifact_budget_report"] = artifact_budget
    payload["learning_roi_report"] = learning_roi
    payload["speed_diagnostics"] = speed_diagnostics
    payload["loop_progress_summary"] = loop_progress
    payload["artifact_paths"] = artifact_paths
    operator_digest = _operator_digest(args, run_dir, final_rankings, cycles, artifact_paths, final_online_state)
    artifact_paths["operator_digest"] = _write_compact_json(run_dir / "operator_digest.json", operator_digest)
    artifact_paths["artifact_manifest"] = _write_artifact_manifest(run_dir, artifact_paths)
    payload["operator_digest"] = operator_digest
    payload["artifact_paths"] = artifact_paths
    _write_compact_json(run_dir / "final_summary.json", _final_summary_payload_for_write(args, payload, artifact_paths))
    artifact_budget = _artifact_budget_report(run_dir, warning_mb=float(getattr(args, "artifact_size_warning_mb", 10.0) or 10.0))
    artifact_paths["artifact_budget_report"] = _write_json(run_dir / "artifact_budget_report.json", artifact_budget)
    speed_diagnostics = _speed_diagnostics_report(args, run_dir, cycles, artifact_budget, started_at=started_epoch)
    artifact_paths["speed_diagnostics"] = _write_json(run_dir / "speed_diagnostics.json", speed_diagnostics)
    loop_progress = _loop_progress_summary(run_dir)
    artifact_paths["loop_progress_summary"] = _write_compact_json(run_dir / "loop_progress_summary.json", loop_progress)
    payload["artifact_budget_report"] = artifact_budget
    payload["speed_diagnostics"] = speed_diagnostics
    payload["loop_progress_summary"] = loop_progress
    payload["artifact_paths"] = artifact_paths
    operator_digest = _operator_digest(args, run_dir, final_rankings, cycles, artifact_paths, final_online_state)
    artifact_paths["operator_digest"] = _write_compact_json(run_dir / "operator_digest.json", operator_digest)
    artifact_paths["artifact_manifest"] = _write_artifact_manifest(run_dir, artifact_paths)
    payload["operator_digest"] = operator_digest
    payload["artifact_paths"] = artifact_paths
    _write_compact_json(run_dir / "final_summary.json", _final_summary_payload_for_write(args, payload, artifact_paths))
    _emit_status_update(
        args,
        run_dir,
        _status_snapshot(
            args,
            run_dir,
            phase="hunt_finished",
            cycle_idx=cycle_idx,
            started_at=started_at,
            deadline=deadline,
            rankings=final_rankings,
        ),
    )
    completed_archive = _archive_completed_run_artifacts(args, run_dir, artifact_paths)
    payload["completed_artifact_archive"] = completed_archive
    if completed_archive.get("enabled"):
        payload["cycle_log_archive_path_remap"] = _remap_archived_cycle_log_paths(run_dir, completed_archive)
        archive_manifest = str((run_dir / "completed_artifact_archive_manifest.json").resolve())
        artifact_paths["completed_artifact_archive_manifest"] = archive_manifest
        payload["artifact_paths"] = artifact_paths
        artifact_paths["artifact_manifest"] = _write_artifact_manifest(run_dir, artifact_paths)
        payload["artifact_paths"] = artifact_paths
        _write_compact_json(run_dir / "final_summary.json", _final_summary_payload_for_write(args, payload, artifact_paths))
    return payload


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run Step 2 variant hunts for a fixed wall-clock window.")
    ap.add_argument("--name", default="overnight_step2_hunt")
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT))
    ap.add_argument("--hours", type=float, default=3.0)
    ap.add_argument("--stop-after-sec", type=int, default=0,
                    help="Native wall-clock stop for short hunt windows. Prefer this over external shell timeouts.")
    ap.add_argument("--window-minutes", type=float, default=0.0,
                    help="Native wall-clock stop in minutes. Overrides --hours when greater than zero.")
    ap.add_argument("--resume-run-dir", default="",
                    help="Resume an existing long-run directory and append more cycles.")
    ap.add_argument("--hunters", nargs="*", default=["adaptive", "local", "router"])
    ap.add_argument("--dynamic-budget", dest="dynamic_budget", action="store_true", default=True,
                    help="Shift cycle allocation toward the hunter family producing behavior-unique live beaters.")
    ap.add_argument("--static-budget", dest="dynamic_budget", action="store_false")
    ap.add_argument("--batch-size", type=int, default=1000)
    ap.add_argument("--max-batches", type=int, default=12)
    ap.add_argument("--max-cycles", type=int, default=0,
                    help="Optional wrapper-cycle cap. Use 1 for exact-count smoke/test runs.")
    ap.add_argument("--target-count", "--max-variants", dest="target_count", type=int, default=100,
                    help="Number of variants to target; --max-variants is accepted as a compatibility alias.")
    ap.add_argument("--beat-pct", type=float, default=0.0)
    ap.add_argument("--target-pnl", type=float, default=-1000000000.0)
    ap.add_argument("--start-balance", type=float, default=100000.0)
    ap.add_argument("--score-cache-stats-mode", choices=["fast", "full"], default="fast",
                    help="Fast avoids expensive exact aggregate scans of the large score-cache DB during runs.")
    ap.add_argument("--coordinator-artifact-mode", choices=["full", "minimal"], default="minimal",
                    help="Minimal avoids duplicate nested coordinator rollups; full is for raw child-artifact debugging.")
    ap.add_argument("--leaderboard-limit", type=int, default=100)
    ap.add_argument("--ranking-objective", choices=["raw_pnl", "promotion_survival"], default="promotion_survival",
                    help="Keep raw P/L leaderboards, but use promotion_survival for repair focus and hunt brain guidance.")
    ap.add_argument("--promotion-survival-sort", choices=["score", "evidence_tier"], default="score",
                    help="Sort promotion survival by score, or by evidence tier first when comparing promotion-ready finalists.")
    ap.add_argument("--hunt-mode", choices=["auto", "discovery", "promotion_repair", "repair_exploration", "weird_exploration"], default="auto",
                    help="Auto switches to promotion_repair when live beaters need evidence repair; explicit modes force the hunt posture.")
    ap.add_argument("--repair-yield-floor-pct", type=float, default=1.0,
                    help="If repair live-yield falls below this percent, recommend repair_exploration instead of tighter repair.")
    ap.add_argument("--repair-loosen-scale", type=float, default=0.90,
                    help="Mutation scale multiplier used by repair_exploration.")
    ap.add_argument("--repair-tighten-scale", type=float, default=0.72,
                    help="Mutation scale multiplier used by promotion_repair.")
    ap.add_argument("--finalist-limit", type=int, default=50)
    ap.add_argument("--oos-queue-limit", type=int, default=20)
    ap.add_argument("--max-focus-routes", type=int, default=6)
    ap.add_argument("--focus-route", "--route-focus", dest="focus_route", action="append", default=[],
                    help="Route bucket to pass through to router hunters, e.g. CLSK|vwap_reclaim_breakdown|*.")
    ap.add_argument("--learning-db", default=str(step2_learning_db.DEFAULT_DB))
    ap.add_argument("--feedback-json", default="",
                    help="Optional promotion/robustness feedback JSON to fold into learning and persist.")
    ap.add_argument("--validation-results-json", action="append", default=[],
                    help="Optional counterfactual/adversarial validation results JSON to ingest into learning DB.")
    ap.add_argument("--online-learning", dest="online_learning", action="store_true", default=True,
                    help="Maintain online_state.json and learning_events.jsonl during the run.")
    ap.add_argument("--no-online-learning", dest="online_learning", action="store_false")
    ap.add_argument("--online-novelty-budget-pct", type=float, default=15.0)
    ap.add_argument("--interruptible-cycles", dest="interruptible_cycles", action="store_true", default=True,
                    help="Let online telemetry stop a router cycle early when it is clearly unproductive.")
    ap.add_argument("--no-interruptible-cycles", dest="interruptible_cycles", action="store_false")
    ap.add_argument("--interrupt-min-telemetry-events", type=int, default=3,
                    help="Minimum streamed batch events before an online interrupt can fire.")
    ap.add_argument("--interrupt-grace-sec", type=float, default=5.0,
                    help="Seconds to wait after writing a stop signal before terminating a cycle process.")
    ap.add_argument("--telemetry-poll-sec", type=float, default=2.0,
                    help="Polling interval for streaming hunter telemetry.")
    ap.add_argument("--min-online-batch-size", type=int, default=50,
                    help="Lower bound for self-tuned router batch sizes.")
    ap.add_argument("--max-online-batch-size", type=int, default=0,
                    help="Upper bound for self-tuned router batch sizes; 0 keeps the current/default cap.")
    ap.add_argument("--exact-variant-count", action="store_true",
                    help="Score exactly --batch-size variants per router cycle, ignoring online batch throttles.")
    ap.add_argument("--runtime-control-mode", choices=["enforce", "observe"], default="enforce",
                    help="Use observe to record runtime controls without applying route skips, throttles, or degrade mode.")
    ap.add_argument("--allow-uncertified-cache", action="store_true",
                    help="Diagnostics only: allow uncertified cache resolution for plumbing smoke tests. Do not use for promotion evidence.")
    ap.add_argument("--runtime-bootstrap-json", "--bootstrap-controls", dest="runtime_bootstrap_json", default="",
                    help="Optional runtime_handoff_controls.json from a prior run to apply before cycle 0.")
    ap.add_argument("--bootstrap-previous-runtime-controls", dest="bootstrap_previous_runtime_controls", action="store_true", default=True,
                    help="Automatically apply the latest prior run's runtime handoff controls before cycle 0.")
    ap.add_argument("--no-bootstrap-previous-runtime-controls", dest="bootstrap_previous_runtime_controls", action="store_false")
    ap.add_argument("--smoke-validation-mode", action="store_true",
                    help="Shortcut for validation runs: locks the requested variant count and observes runtime controls.")
    ap.add_argument("--online-micro-validation", action="store_true",
                    help="Periodically execute tiny counterfactual/adversarial validation jobs during long hunts.")
    ap.add_argument("--micro-validation-every-cycles", type=int, default=12)
    ap.add_argument("--micro-validation-trigger-live-beaters", type=int, default=1,
                    help="With online micro-validation enabled, stop a router cycle and validate after this many streamed live beaters.")
    ap.add_argument("--micro-validation-limit", type=int, default=2)
    ap.add_argument("--micro-validation-timeout-sec", type=int, default=900)
    ap.add_argument("--micro-validation-min-remaining-sec", type=int, default=1200)
    ap.add_argument("--micro-promotion-review", dest="micro_promotion_review", action="store_true", default=True,
                    help="Run lightweight in-loop promotion decision briefs and feed reject reasons back into online learning.")
    ap.add_argument("--no-micro-promotion-review", dest="micro_promotion_review", action="store_false")
    ap.add_argument("--micro-promotion-every-cycles", type=int, default=3)
    ap.add_argument("--micro-promotion-limit", type=int, default=5)
    ap.add_argument("--status-interval-sec", type=int, default=300,
                    help="Emit hunt_status updates every N seconds. Defaults to 300 seconds / 5 minutes; set lower only when explicitly requested.")
    ap.add_argument("--status-verbosity", choices=["compact", "full"], default="compact",
                    help="Compact prints concise status updates while preserving full status payloads in learning_events.jsonl.")
    ap.add_argument("--status-stdout-mode", choices=["all", "final", "none"], default="all",
                    help="Control status JSON printed to stdout. Status events are still written to learning_events.jsonl.")
    ap.add_argument("--status-event-verbosity", choices=["digest", "full"], default="digest",
                    help="Digest stores compact hunt_status events; full stores the complete status snapshot in learning_events.jsonl.")
    ap.add_argument("--artifact-profile", choices=["compact", "full"], default="compact",
                    help="Compact keeps routine review artifacts small and writes raw evidence to separate files; full preserves legacy bulky rollups.")
    ap.add_argument("--write-full-running-top100", action="store_true",
                    help="In compact artifact mode, also write the legacy duplicate running_top100.full.json snapshot.")
    ap.add_argument("--artifact-size-warning-mb", type=float, default=10.0,
                    help="Warn in artifact_budget_report when any single artifact exceeds this size.")
    ap.add_argument("--archive-completed-artifacts", dest="archive_completed_artifacts", action="store_true", default=True,
                    help="After a completed run, move cold/debug artifacts into the workspace-level okay_to_delete folder.")
    ap.add_argument("--no-archive-completed-artifacts", dest="archive_completed_artifacts", action="store_false",
                    help="Leave all completed-run artifacts in the hot run directory.")
    ap.add_argument("--okay-to-delete-dir", default="okay_to_delete",
                    help="Workspace-level archive root for cold artifacts; relative paths are resolved under the repo root.")
    ap.add_argument("--live-only", dest="live_only", action="store_true", default=True)
    ap.add_argument("--include-non-live", dest="live_only", action="store_false")
    ap.add_argument("--behavioral-dedupe", dest="behavioral_dedupe", action="store_true", default=True)
    ap.add_argument("--config-dedupe-only", dest="behavioral_dedupe", action="store_false")
    ap.add_argument("--cycle-timeout-sec", type=int, default=2400)
    ap.add_argument("--min-seconds-for-next-cycle", type=int, default=120)
    ap.add_argument("--seed", type=int, default=2026051001)
    ap.add_argument("--seed-json", action="append", default=[])
    ap.add_argument("--tiny-run-threshold", type=int, default=1000,
                    help="Runs below this requested variant count are limited-weight learning unless below --limited-learning-min-variants.")
    ap.add_argument("--limited-learning-min-variants", type=int, default=500,
                    help="Runs below this count are diagnostic; 500+ variant runs are remembered with limited learning weight.")
    ap.add_argument("--persist-tiny-run-learning", action="store_true",
                    help="Allow diagnostic/tiny runs to update the persistent learning DB.")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--json-verbosity", choices=["digest", "compact", "full"], default="digest")
    return ap.parse_args(argv)


def main() -> int:
    args = parse_args()
    if bool(args.smoke_validation_mode):
        args.exact_variant_count = True
        args.runtime_control_mode = "observe"
    payload = run(args)
    if args.json:
        if str(args.json_verbosity) == "full":
            out = payload
        elif str(args.json_verbosity) == "compact":
            out = _compact_run_payload(payload)
        else:
            out = _digest_run_payload(payload)
        print(json.dumps(out, indent=2, sort_keys=True, default=str))
    else:
        print(json.dumps({"path": payload.get("path"), "ok": payload.get("ok"), "run_dir": payload.get("run_dir")}, indent=2))
    return 0 if payload.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
