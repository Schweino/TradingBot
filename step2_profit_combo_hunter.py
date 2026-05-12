"""Quote-aware Step 2 profit-pocket hunter.

This is a score-only hunter. It never rebuilds signals or compiled tapes. The
search surface is routed scoring: skip bad buckets, rescue promising pockets,
and require enough trades before a row can be ranked as a real contender.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import time
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

import numpy as np

import active_engine_baseline
import candidate_decision_brief
import candidate_profile_schema
import candidate_robustness_report
import decision_tape_compiled
import routed_profile_safety
import routed_scoring_profile as routed
import step2_belief_calibration
import step2_closed_loop_controller
import step2_data_quality
import step2_deployment_risk
import step2_hunt_intelligence as hunt_intel
import step2_learning_control_plane
import step2_learning_db
import step2_learning_depth
import step2_objective
import step2_ops_hardening
import step2_parity_contract
import step2_quote_aware_guard
import step2_score_cache
import step2_statistical_validation
import step2_world_class_audit
import tournament_safety


HERE = Path(__file__).resolve().parent
DEFAULT_OUT = HERE / "postmortem" / "backtests" / "step2_combo_hunter"
TICKERS = ("CLSK", "MARA", "RIOT")
PHASES = ("open", "midday", "late")
ACTION_SET = ("score", "force_long", "force_short")
MICRO_FILTER_FEATURES = (
    "vwap",
    "relative",
    "btc_chop",
    "exec_penalty",
    "btc",
    "momentum",
    "flow_contra",
    "burst",
    "vwap_sigma_ext",
    "btc_mom_abs",
    "flow_pressure_abs",
)


def _read_json(path: str | Path) -> dict:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except Exception:
        return {}


def _read_json_payload(path: str | Path | None) -> Any:
    if not path:
        return {}
    try:
        return json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except Exception:
        return {}


def _artifact_sha256(path: str | Path | None) -> str | None:
    if not path:
        return None
    try:
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()
    except Exception:
        return None


def _artifact_mtime_iso(path: str | Path | None) -> str | None:
    if not path:
        return None
    try:
        return datetime.fromtimestamp(Path(path).stat().st_mtime).date().isoformat()
    except Exception:
        return None


def _load_historical_split_protocol(path: str | Path | None) -> dict:
    payload = _read_json_payload(path)
    if not isinstance(payload, dict):
        return {}
    protocol = payload.get("historical_learning_protocol") or payload.get("split_protocol") or payload
    if not isinstance(protocol, dict):
        return {}
    return {
        **protocol,
        "external_artifact_path": str(Path(path).resolve()) if path else None,
        "external_artifact_sha256": _artifact_sha256(path),
        "external_artifact_mtime": _artifact_mtime_iso(path),
    }


def _load_forward_promotion_feedback(path: str | Path | None) -> dict:
    payload = _read_json_payload(path)
    if not isinstance(payload, dict):
        return {}
    stages = payload.get("promotion_stages") or payload.get("stages")
    if isinstance(stages, dict):
        payload = {**payload, "promotion_stages": stages}
    return {
        **payload,
        "external_artifact_path": str(Path(path).resolve()) if path else None,
        "external_artifact_sha256": _artifact_sha256(path),
        "external_artifact_mtime": _artifact_mtime_iso(path),
    }


def _load_external_benchmarks(path: str | Path | None) -> list[dict]:
    payload = _read_json_payload(path)
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        rows = (
            payload.get("external_benchmarks")
            or payload.get("benchmarks")
            or payload.get("baselines")
            or []
        )
    else:
        rows = []
    out = []
    for row in rows:
        if isinstance(row, dict):
            out.append({
                **row,
                "external_artifact_path": str(Path(path).resolve()) if path else None,
                "external_artifact_sha256": _artifact_sha256(path),
                "external_artifact_mtime": _artifact_mtime_iso(path),
            })
    return out


def _load_fill_quality_feedback(path: str | Path | None) -> dict:
    payload = _read_json_payload(path)
    if not isinstance(payload, dict):
        return {}
    fills = payload.get("fills") or payload.get("orders") or payload.get("fill_rows") or []
    return {
        **payload,
        "fills": [row for row in fills if isinstance(row, dict)] if isinstance(fills, list) else [],
        "external_artifact_path": str(Path(path).resolve()) if path else None,
        "external_artifact_sha256": _artifact_sha256(path),
        "external_artifact_mtime": _artifact_mtime_iso(path),
    }


def _load_actual_ablation_tests(path: str | Path | None) -> list[dict]:
    payload = _read_json_payload(path)
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        rows = (
            payload.get("actual_ablation_tests")
            or payload.get("ablations")
            or payload.get("tests")
            or []
        )
    else:
        rows = []
    out = []
    for row in rows:
        if isinstance(row, dict):
            out.append({
                **row,
                "external_artifact_path": str(Path(path).resolve()) if path else None,
                "external_artifact_sha256": _artifact_sha256(path),
                "external_artifact_mtime": _artifact_mtime_iso(path),
            })
    return out


def _load_calibration_feedback(path: str | Path | None) -> dict:
    payload = _read_json_payload(path)
    if not isinstance(payload, dict):
        return {}
    artifact = {
        "external_artifact_path": str(Path(path).resolve()) if path else None,
        "external_artifact_sha256": _artifact_sha256(path),
        "external_artifact_mtime": _artifact_mtime_iso(path),
    }

    def _rows(*keys: str) -> list[dict]:
        for key in keys:
            value = payload.get(key)
            if isinstance(value, list):
                return [{**row, **artifact} for row in value if isinstance(row, dict)]
        return []

    return {
        **artifact,
        "validation_rows": _rows("validation_rows", "route_validation_rows"),
        "feedback_rows": _rows("feedback_rows", "promotion_feedback_rows"),
        "command_outcomes": _rows("command_outcomes"),
        "additional_predictions": _rows("additional_predictions", "predictions", "resolved_predictions"),
    }


def _proof_artifact_manifest(
    historical_split: dict,
    forward_promotion: dict,
    external_benchmarks: list[dict],
    actual_ablations: list[dict],
    calibration_feedback: dict,
    fill_quality_feedback: dict | None = None,
) -> dict:
    def _list_artifact(rows: list[dict]) -> tuple[str | None, str | None, str | None]:
        for row in rows or []:
            if isinstance(row, dict) and row.get("external_artifact_path"):
                return row.get("external_artifact_path"), row.get("external_artifact_sha256"), row.get("external_artifact_mtime")
        return None, None, None

    benchmark_path, benchmark_hash, benchmark_mtime = _list_artifact(external_benchmarks)
    ablation_path, ablation_hash, ablation_mtime = _list_artifact(actual_ablations)
    entries = [
        {
            "role": "historical_split_protocol",
            "loaded": bool(historical_split),
            "record_count": 1 if historical_split else 0,
            "artifact_path": historical_split.get("external_artifact_path") if historical_split else None,
            "artifact_sha256": historical_split.get("external_artifact_sha256") if historical_split else None,
            "artifact_mtime": historical_split.get("external_artifact_mtime") if historical_split else None,
        },
        {
            "role": "forward_promotion_feedback",
            "loaded": bool(forward_promotion),
            "record_count": len((forward_promotion or {}).get("promotion_stages") or {}),
            "artifact_path": forward_promotion.get("external_artifact_path") if forward_promotion else None,
            "artifact_sha256": forward_promotion.get("external_artifact_sha256") if forward_promotion else None,
            "artifact_mtime": forward_promotion.get("external_artifact_mtime") if forward_promotion else None,
        },
        {
            "role": "external_benchmarks",
            "loaded": bool(external_benchmarks),
            "record_count": len(external_benchmarks or []),
            "artifact_path": benchmark_path,
            "artifact_sha256": benchmark_hash,
            "artifact_mtime": benchmark_mtime,
        },
        {
            "role": "actual_ablation_tests",
            "loaded": bool(actual_ablations),
            "record_count": len(actual_ablations or []),
            "artifact_path": ablation_path,
            "artifact_sha256": ablation_hash,
            "artifact_mtime": ablation_mtime,
        },
        {
            "role": "calibration_feedback",
            "loaded": bool(calibration_feedback),
            "record_count": (
                len((calibration_feedback or {}).get("validation_rows") or [])
                + len((calibration_feedback or {}).get("feedback_rows") or [])
                + len((calibration_feedback or {}).get("command_outcomes") or [])
                + len((calibration_feedback or {}).get("additional_predictions") or [])
            ),
            "artifact_path": calibration_feedback.get("external_artifact_path") if calibration_feedback else None,
            "artifact_sha256": calibration_feedback.get("external_artifact_sha256") if calibration_feedback else None,
            "artifact_mtime": calibration_feedback.get("external_artifact_mtime") if calibration_feedback else None,
        },
        {
            "role": "fill_quality_feedback",
            "loaded": bool(fill_quality_feedback),
            "record_count": len((fill_quality_feedback or {}).get("fills") or []),
            "artifact_path": (fill_quality_feedback or {}).get("external_artifact_path"),
            "artifact_sha256": (fill_quality_feedback or {}).get("external_artifact_sha256"),
            "artifact_mtime": (fill_quality_feedback or {}).get("external_artifact_mtime"),
        },
    ]
    missing = [entry["role"] for entry in entries if not entry["loaded"]]
    loaded_without_hash = [entry["role"] for entry in entries if entry["loaded"] and not entry["artifact_sha256"]]
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "artifact_count": sum(1 for entry in entries if entry["loaded"]),
        "record_count": sum(int(entry["record_count"] or 0) for entry in entries),
        "tamper_evident": not loaded_without_hash,
        "entries": entries,
        "missing_roles": missing,
        "loaded_without_hash": loaded_without_hash,
        "ready_for_world_best_certification": not missing and not loaded_without_hash,
    }


def _proof_freshness_report(proof_manifest: dict, max_age_days: int = 30, today: date | None = None) -> dict:
    today = today or date.today()
    entries = [row for row in (proof_manifest or {}).get("entries") or [] if isinstance(row, dict)]
    rows = []
    stale_roles = []
    missing_mtime_roles = []
    for entry in entries:
        if not entry.get("loaded"):
            continue
        mtime = _parse_iso_date(entry.get("artifact_mtime"))
        age_days = None if mtime is None else (today - mtime).days
        fresh = age_days is not None and age_days <= int(max_age_days)
        if mtime is None:
            missing_mtime_roles.append(entry.get("role"))
        elif not fresh:
            stale_roles.append(entry.get("role"))
        rows.append({
            "role": entry.get("role"),
            "artifact_path": entry.get("artifact_path"),
            "artifact_mtime": entry.get("artifact_mtime"),
            "age_days": age_days,
            "fresh": fresh,
        })
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "max_age_days": int(max_age_days),
        "checked_artifact_count": len(rows),
        "fresh_artifact_count": sum(1 for row in rows if row["fresh"]),
        "stale_roles": stale_roles,
        "missing_mtime_roles": missing_mtime_roles,
        "ready_for_world_best_certification": bool(rows) and not stale_roles and not missing_mtime_roles,
        "entries": rows,
        "deduction": "External proof must be fresh enough for current market structure; stale proof is treated as expired.",
    }


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str), encoding="utf-8")
    os.replace(tmp, path)


def _load_trading_config() -> dict:
    return _read_json(HERE / "trading_config.json")


def _sim_config(args: argparse.Namespace) -> dict:
    sim = step2_parity_contract.sim_config(_load_trading_config())
    sim["max_trades_per_day"] = int(args.max_trades_per_day)
    sim["max_trades_per_ticker_day"] = int(args.max_trades_per_ticker_day)
    return sim


def _active_variant() -> Any:
    return active_engine_baseline.reference_variant()


def _score(compiled: dict, variants: list[Any], args: argparse.Namespace) -> list[dict]:
    scorer = step2_score_cache.score_variants_cached if args.use_score_cache else decision_tape_compiled.simulate_variants
    kwargs = {"gate": None, "sim_config": _sim_config(args)}
    if args.use_score_cache:
        kwargs["cache_db"] = args.score_cache_db
    rows = scorer(compiled, variants, float(args.start_balance), **kwargs)
    if rows is None:
        raise RuntimeError("compiled Step 2 simulation unavailable")
    out: list[dict] = []
    for row in rows:
        full = row.get("decision_full") if isinstance(row.get("decision_full"), dict) else {}
        out.append({
            "variant": row.get("variant"),
            "weights": row.get("weights") or {},
            "bias": float(row.get("bias") or 0.0),
            "routes": row.get("routes") or [],
            "routed_scoring_profile": bool(row.get("routed_scoring_profile") or row.get("routes")),
            "route_audit": row.get("route_audit") or {},
            "routed_profile_safety": row.get("routed_profile_safety") or {},
            "step2_pnl": float(full.get("pnl") or 0.0),
            "step2_trades": int(full.get("trades") or 0),
            "step2_wins": int(full.get("wins") or 0),
            "step2_losses": int(full.get("losses") or 0),
            "step2_win_rate_pct": full.get("win_rate_pct"),
            "by_ticker": full.get("by_ticker"),
            "by_day": full.get("by_day"),
            "by_side": full.get("by_side"),
            "skipped": full.get("skipped"),
            "exit_replay_model": row.get("exit_replay_model") or full.get("exit_replay_model"),
            "required_exit_replay_model": row.get("required_exit_replay_model"),
            "score_cache": row.get("score_cache") or {},
        })
    return out


def _reverse_map(mapping: dict) -> dict[int, str]:
    return {int(v): str(k) for k, v in (mapping or {}).items()}


def _phase_vector(compiled: dict) -> np.ndarray:
    rows = int(compiled.get("rows") or 0)
    phases = np.full(rows, "late", dtype=object)
    open_idx = routed.FEATURE_INDEX.get("open_phase")
    mid_idx = routed.FEATURE_INDEX.get("midday_phase")
    features = np.asarray(compiled["features"])
    if open_idx is not None:
        phases[features[:, open_idx] > 0.0] = "open"
    if mid_idx is not None:
        phases[features[:, mid_idx] > 0.0] = "midday"
    return phases


def _match_from_parts(ticker: str = "*", setup: str = "*", phase: str = "*", side: str = "*") -> dict[str, Any]:
    match: dict[str, Any] = {}
    if ticker and ticker != "*":
        match["ticker"] = ticker
    if setup and setup != "*":
        match["setup_type"] = setup
    if phase and phase != "*":
        match["session_phase"] = phase
    if side and side != "*":
        match["side"] = side
    return match


def _route_key(match: dict[str, Any]) -> str:
    return "|".join([
        str(match.get("ticker") or "*"),
        str(match.get("setup_type") or "*"),
        str(match.get("session_phase") or "*"),
        str(match.get("side") or "*"),
    ])


def _bucket_stats(compiled: dict, active: Any) -> dict:
    sides = decision_tape_compiled.side_matrix(compiled, [active])[0]
    ticker_names = _reverse_map(compiled.get("ticker_map") or {})
    setup_names = _reverse_map(compiled.get("setup_map") or {})
    tickers = np.asarray([ticker_names.get(int(code), str(int(code))) for code in np.asarray(compiled["ticker_code"])], dtype=object)
    setups = np.asarray([setup_names.get(int(code), str(int(code))) for code in np.asarray(compiled["setup_code"])], dtype=object)
    phases = _phase_vector(compiled)
    long_pct = np.asarray(compiled["long_pnl_pct"], dtype=np.float64)
    short_pct = np.asarray(compiled["short_pnl_pct"], dtype=np.float64)
    active_pct = np.where(sides > 0, long_pct, np.where(sides < 0, short_pct, 0.0))
    long_sum = defaultdict(float)
    short_sum = defaultdict(float)
    active_sum = defaultdict(float)
    count = defaultdict(int)
    side_names = np.where(sides > 0, "LONG", np.where(sides < 0, "SHORT", "SKIP"))
    for ticker, setup, phase, side_name, lp, sp, ap in zip(tickers, setups, phases, side_names, long_pct, short_pct, active_pct):
        keys = [
            (ticker, "*", "*", "*"),
            ("*", setup, "*", "*"),
            (ticker, setup, "*", "*"),
            (ticker, setup, phase, "*"),
            (ticker, setup, phase, side_name),
        ]
        for key in keys:
            text = "|".join(str(part) for part in key)
            count[text] += 1
            long_sum[text] += float(lp)
            short_sum[text] += float(sp)
            active_sum[text] += float(ap)
    rows = []
    for key, n in count.items():
        long_avg = long_sum[key] / n if n else 0.0
        short_avg = short_sum[key] / n if n else 0.0
        active_avg = active_sum[key] / n if n else 0.0
        preferred = "force_long" if long_avg >= short_avg else "force_short"
        rows.append({
            "route_key": key,
            "opportunities": n,
            "active_sum_pct": round(active_sum[key], 6),
            "active_avg_pct": round(active_avg, 8),
            "long_sum_pct": round(long_sum[key], 6),
            "short_sum_pct": round(short_sum[key], 6),
            "long_avg_pct": round(long_avg, 8),
            "short_avg_pct": round(short_avg, 8),
            "preferred_action": preferred,
            "preferred_avg_pct": round(max(long_avg, short_avg), 8),
        })
    negative = sorted(rows, key=lambda row: (row["active_sum_pct"], -row["opportunities"]))
    positive = sorted(
        [row for row in rows if row["preferred_avg_pct"] > 0 and row["opportunities"] >= 50],
        key=lambda row: (row["preferred_avg_pct"], row["opportunities"]),
        reverse=True,
    )
    return {
        "negative": negative[:160],
        "positive": positive[:160],
        "counts": {
            "bucket_rows": len(rows),
            "negative_rows": sum(1 for row in rows if row["active_sum_pct"] < 0),
            "positive_preferred_rows": len(positive),
        },
    }


def _parse_route_key(route_key: str) -> dict[str, Any]:
    parts = (str(route_key).split("|") + ["*", "*", "*", "*"])[:4]
    return _match_from_parts(parts[0], parts[1], parts[2], parts[3])


def _skip_route(idx: int, route_key: str) -> routed.Route:
    match = _parse_route_key(route_key)
    return routed.route(f"skip_{idx:04d}_{route_key.replace('|', '_')}", match, action="skip")


def _rescue_route(idx: int, route_key: str, action: str, active: Any) -> routed.Route:
    match = _parse_route_key(route_key)
    weights = dict(getattr(active, "weights", {}) or {}) if action == "score" else {}
    bias = float(getattr(active, "bias", 0.0) or 0.0) if action == "score" else 0.0
    return routed.route(f"{action}_{idx:04d}_{route_key.replace('|', '_')}", match, weights, bias, action=action)


def _micro_route(idx: int, route_key: str, feature: str, op: str, value: float, active: Any) -> routed.Route:
    match = _parse_route_key(route_key)
    match["feature_filters"] = [{"feature": feature, "op": op, "value": round(float(value), 6)}]
    weights = dict(getattr(active, "weights", {}) or {})
    bias = float(getattr(active, "bias", 0.0) or 0.0)
    safe_feature = feature.replace("|", "_")
    safe_op = "gte" if op == ">=" else "lte"
    return routed.route(
        f"score_micro_{idx:04d}_{route_key.replace('|', '_')}_{safe_feature}_{safe_op}_{round(float(value), 4)}",
        match,
        weights,
        bias,
        action="score",
    )


def _micro_filter_routes(compiled: dict, positive: list[dict], active: Any, limit: int) -> list[routed.Route]:
    routes: list[routed.Route] = []
    seen_masks: set[str] = set()
    features = np.asarray(compiled["features"])
    for row in positive[:25]:
        base_match = _parse_route_key(row["route_key"])
        base_mask = routed.route_mask(compiled, routed.route("base", base_match))
        base_count = int(np.sum(base_mask))
        if base_count < 20:
            continue
        for feature in MICRO_FILTER_FEATURES:
            feature_idx = routed.FEATURE_INDEX.get(feature)
            if feature_idx is None:
                continue
            values = features[base_mask, feature_idx]
            if values.size < 20:
                continue
            for q in (0.25, 0.5, 0.75):
                threshold = float(np.quantile(values, q))
                for op in ("<=", ">="):
                    route = _micro_route(len(routes), row["route_key"], feature, op, threshold, active)
                    mask = routed.route_mask(compiled, route)
                    count = int(np.sum(mask))
                    mask_key = hashlib.sha1(mask.astype(np.uint8).tobytes()).hexdigest()[:20]
                    if 20 <= count < base_count and mask_key not in seen_masks:
                        seen_masks.add(mask_key)
                        routes.append(route)
                    if len(routes) >= limit:
                        return routes
    return routes


def _edge_density_routes(compiled: dict, positive: list[dict], active: Any, args: argparse.Namespace, limit: int) -> list[routed.Route]:
    routes: list[routed.Route] = []
    candidates = []
    seen_masks: set[str] = set()
    features = np.asarray(compiled["features"])
    long_pct = np.asarray(compiled["long_pnl_pct"], dtype=np.float64)
    short_pct = np.asarray(compiled["short_pnl_pct"], dtype=np.float64)
    min_count = max(3, min(int(getattr(args, "min_bucket_opportunities", 10) or 10), 20))
    max_count = max(min_count, int(getattr(args, "min_trades", 250) or 250))
    for row in positive[:80]:
        base_match = _parse_route_key(row["route_key"])
        base_route = routed.route("edge_base", base_match)
        base_mask = routed.route_mask(compiled, base_route)
        base_count = int(np.sum(base_mask))
        if base_count < min_count:
            continue
        for feature in MICRO_FILTER_FEATURES:
            feature_idx = routed.FEATURE_INDEX.get(feature)
            if feature_idx is None:
                continue
            values = features[base_mask, feature_idx]
            if values.size < min_count:
                continue
            for q in (0.10, 0.20, 0.35, 0.50, 0.65, 0.80, 0.90):
                threshold = float(np.quantile(values, q))
                for op in ("<=", ">="):
                    match = dict(base_match)
                    match["feature_filters"] = [{"feature": feature, "op": op, "value": round(float(threshold), 6)}]
                    probe = routed.route("edge_probe", match)
                    mask = routed.route_mask(compiled, probe)
                    count = int(np.sum(mask))
                    if count < min_count or count > max_count:
                        continue
                    long_sum = float(np.sum(long_pct[mask]))
                    short_sum = float(np.sum(short_pct[mask]))
                    if long_sum >= short_sum:
                        action = "force_long"
                        gross_sum = long_sum
                    else:
                        action = "force_short"
                        gross_sum = short_sum
                    edge_per_trade = gross_sum / count if count else 0.0
                    if edge_per_trade <= 0.0:
                        continue
                    mask_key = hashlib.sha1(mask.astype(np.uint8).tobytes()).hexdigest()[:20]
                    action_mask_key = f"{action}:{mask_key}"
                    if action_mask_key in seen_masks:
                        continue
                    seen_masks.add(action_mask_key)
                    candidates.append((edge_per_trade, gross_sum, count, action, match, feature, op, threshold))
    candidates.sort(key=lambda item: (item[0], item[1], -item[2]), reverse=True)
    for idx, (edge_per_trade, gross_sum, count, action, match, feature, op, threshold) in enumerate(candidates[: max(limit * 3, limit)]):
        route = routed.route(
            f"{action}_edge_density_{idx:04d}_{feature}_{'gte' if op == '>=' else 'lte'}_{round(float(threshold), 4)}_{count}ops",
            match,
            dict(getattr(active, "weights", {}) or {}) if action == "score" else {},
            float(getattr(active, "bias", 0.0) or 0.0) if action == "score" else 0.0,
            action=action,
        )
        routes.append(route)
        if len(routes) >= limit:
            return routes
    return routes


def _all_ticker_skip(routes: list[routed.Route]) -> bool:
    skipped = {
        str(item.match.get("ticker"))
        for item in routes
        if item.action == "skip" and set(item.match) == {"ticker"} and item.match.get("ticker")
    }
    return set(TICKERS) <= skipped


def _ordered_routes(routes: list[routed.Route]) -> list[routed.Route]:
    """Put broad skips before specific skips so attribution is not overwritten."""
    skip_routes = [item for item in routes if item.action == "skip"]
    other_routes = [item for item in routes if item.action != "skip"]
    skip_routes.sort(key=lambda item: (len(item.match or {}), item.name))
    other_routes.sort(key=lambda item: (len(item.match or {}), item.name))
    return skip_routes + other_routes


def _prune_zero_attribution_routes(compiled: dict, routes: list[routed.Route]) -> list[routed.Route]:
    """Remove routes that later routes fully overwrite in route attribution."""
    if not routes:
        return []
    masks = [routed.route_mask(compiled, item) for item in routes]
    later = np.zeros(int(compiled.get("rows") or 0), dtype=bool)
    keep = [False] * len(routes)
    for idx in range(len(routes) - 1, -1, -1):
        attributed = masks[idx] & ~later
        keep[idx] = bool(np.any(attributed))
        later |= masks[idx]
    return [item for item, should_keep in zip(routes, keep) if should_keep]


def _variant_key(variant: Any) -> str:
    payload = step2_score_cache.variant_payload(variant)
    payload = dict(payload)
    payload.pop("name", None)
    routes = []
    for route_payload in payload.get("routes") or []:
        if isinstance(route_payload, dict):
            item = dict(route_payload)
            item.pop("name", None)
            routes.append(item)
    payload["routes"] = routes
    return tournament_safety.stable_json_hash(payload, length=32)


def _seed_shape_key(row: dict) -> str:
    routes = []
    for route_payload in row.get("routes") or []:
        if isinstance(route_payload, dict):
            item = dict(route_payload)
            item.pop("name", None)
            routes.append(item)
    return tournament_safety.stable_json_hash({
        "weights": row.get("weights") or {},
        "bias": float(row.get("bias") or 0.0),
        "routes": routes,
    }, length=32)


def _row_edge_density(row: dict) -> float:
    trades = max(1, int(row.get("step2_trades") or 0))
    return float(row.get("step2_pnl") or 0.0) / trades


def _seed_positive_rows(paths: list[str], limit: int = 200) -> list[dict]:
    all_rows: list[dict] = []
    for raw_path in paths or []:
        payload = _read_json(raw_path)
        payloads = [payload]
        nested_execution = payload.get("execution_viability_gap_report") if isinstance(payload.get("execution_viability_gap_report"), dict) else {}
        if nested_execution:
            payloads.append(nested_execution)
        for source_payload in payloads:
            for key in ("holdout_winners", "winners", "low_sample_positive", "leaderboard", "execution_positive_candidates"):
                values = source_payload.get(key)
                if not isinstance(values, list):
                    continue
                for row in values:
                    if (
                        isinstance(row, dict)
                        and float(row.get("step2_pnl") or 0.0) > 0.0
                        and isinstance(row.get("routes"), list)
                        and row.get("routes")
                    ):
                        all_rows.append(row)

    holdout_rows = sorted(
        all_rows,
        key=lambda row: (
            float((row.get("holdout_gate") or {}).get("holdout_pnl") or -1_000_000.0),
            float(row.get("step2_pnl") or 0.0),
            int(row.get("step2_trades") or 0),
        ),
        reverse=True,
    )
    total_rows = sorted(
        all_rows,
        key=lambda row: (
            float(row.get("step2_pnl") or 0.0),
            float((row.get("holdout_gate") or {}).get("holdout_pnl") or -1_000_000.0),
            int(row.get("step2_trades") or 0),
        ),
        reverse=True,
    )
    trade_rows = sorted(
        all_rows,
        key=lambda row: (
            int(row.get("step2_trades") or 0),
            float((row.get("holdout_gate") or {}).get("holdout_pnl") or -1_000_000.0),
            float(row.get("step2_pnl") or 0.0),
        ),
        reverse=True,
    )
    density_rows = sorted(
        all_rows,
        key=lambda row: (
            _row_edge_density(row),
            float((row.get("holdout_gate") or {}).get("holdout_pnl") or -1_000_000.0) / max(1, int(row.get("step2_trades") or 0)),
            float(row.get("execution_adjusted_pnl") or -1_000_000.0),
            int(row.get("step2_trades") or 0),
        ),
        reverse=True,
    )

    ordered_rows: list[dict] = []
    for idx in range(max(len(holdout_rows), len(total_rows), len(trade_rows), len(density_rows))):
        if idx < len(holdout_rows):
            ordered_rows.append(holdout_rows[idx])
        if idx < len(total_rows):
            ordered_rows.append(total_rows[idx])
        if idx < len(trade_rows):
            ordered_rows.append(trade_rows[idx])
        if idx < len(density_rows):
            ordered_rows.append(density_rows[idx])

    out: list[dict] = []
    seen: set[str] = set()
    for row in ordered_rows:
        key = _behavior_key(row)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
        if len(out) >= limit:
            return out
    return out


def _seed_row_rescue_routes(row: dict) -> list[routed.Route]:
    routes: list[routed.Route] = []
    seen: set[str] = set()
    for route_payload in row.get("routes") or []:
        if not isinstance(route_payload, dict) or route_payload.get("action") == "skip":
            continue
        route = routed.route_from_dict(route_payload)
        key = tournament_safety.stable_json_hash(routed.route_to_dict(route), length=24)
        if key in seen:
            continue
        seen.add(key)
        routes.append(route)
    return routes


def _seed_row_routes(row: dict) -> list[routed.Route]:
    routes: list[routed.Route] = []
    for route_payload in row.get("routes") or []:
        if isinstance(route_payload, dict):
            routes.append(routed.route_from_dict(route_payload))
    return routes


def _seed_positive_routes(paths: list[str], limit: int = 120) -> list[routed.Route]:
    routes: list[routed.Route] = []
    seen: set[str] = set()
    for row in _seed_positive_rows(paths, limit=240):
        for route in _seed_row_rescue_routes(row):
            key = tournament_safety.stable_json_hash(routed.route_to_dict(route), length=24)
            if key in seen:
                continue
            seen.add(key)
            routes.append(route)
            if len(routes) >= limit:
                return routes
    return routes


def _seed_positive_variants(paths: list[str], limit: int = 60) -> list[Any]:
    variants: list[Any] = []
    seen: set[str] = set()
    decision_seen: set[str] = set()
    route_mask_hash_cache: dict[str, str] = {}
    for row in _seed_positive_rows(paths, limit=limit * 4):
        source_name = str(row.get("variant") or row.get("name") or "positive_seed")
        while source_name.startswith("seed_exact_"):
            source_name = source_name[len("seed_exact_"):]
        variant = routed.variant_from_dict({
            "name": f"seed_exact_{source_name}",
            "weights": row.get("weights") or {},
            "bias": float(row.get("bias") or 0.0),
            "routes": row.get("routes") or [],
        })
        key = _seed_shape_key(row)
        if key in seen:
            continue
        seen.add(key)
        variants.append(variant)
        if len(variants) >= limit:
            return variants
    return variants


def _route_side_bias(route: routed.Route) -> str:
    action = str(route.action or "").lower()
    if action == "force_long":
        return "long"
    if action == "force_short":
        return "short"
    side = str((route.match or {}).get("side") or "").strip().upper()
    if side == "LONG":
        return "long"
    if side == "SHORT":
        return "short"
    return "neutral"


def _registry_route_pools(active: Any) -> dict[str, list[routed.Route]]:
    registry = _read_json(DEFAULT_OUT / "route_learning_registry.json")
    values = list((registry.get("routes") or {}).values()) if isinstance(registry, dict) else []
    active_weights = dict(getattr(active, "weights", {}) or {})
    active_bias = float(getattr(active, "bias", 0.0) or 0.0)

    def make_route(item: dict, idx: int) -> routed.Route | None:
        action = str(item.get("route_action") or "score")
        match = item.get("route_match") if isinstance(item.get("route_match"), dict) else {}
        if not match:
            return None
        weights = active_weights if action == "score" else {}
        bias = active_bias if action == "score" else 0.0
        return routed.route(
            f"registry_{idx:04d}_{str(item.get('route') or action)[:80]}",
            match,
            weights,
            bias,
            action=action,
        )

    good_items = sorted(
        [item for item in values if item.get("status") == "good_candidate"],
        key=lambda item: (
            int(item.get("proven_good_count") or 0),
            float(item.get("avg_holdout_contribution") or 0.0),
            float(item.get("avg_pnl_contribution") or 0.0),
        ),
        reverse=True,
    )
    def strongly_toxic(item: dict) -> bool:
        if item.get("status") != "toxic_candidate":
            return False
        good_count = int(item.get("proven_good_count") or 0)
        toxic_count = int(item.get("proven_toxic_count") or 0)
        tested = max(1, int(item.get("tested_count") or good_count + toxic_count or 1))
        confidence = float(item.get("confidence") if item.get("confidence") is not None else abs(toxic_count - good_count) / tested)
        return bool(toxic_count > good_count and confidence >= 0.35 and not (good_count and toxic_count))

    toxic_items = [item for item in values if strongly_toxic(item)]
    good_routes = [route for idx, item in enumerate(good_items) for route in [make_route(item, idx)] if route is not None]
    toxic_keys = {
        _route_identity({
            "action": item.get("route_action"),
            "match": item.get("route_match") or {},
            "weights": active_weights if str(item.get("route_action") or "score") == "score" else {},
            "bias": active_bias if str(item.get("route_action") or "score") == "score" else 0.0,
        })
        for item in toxic_items
    }
    return {"good": good_routes, "toxic_keys": sorted(toxic_keys)}


def _build_variants(compiled: dict, active: Any, args: argparse.Namespace) -> tuple[list[Any], dict]:
    rng = random.Random(args.seed)
    stats = _bucket_stats(compiled, active)
    negative = [row for row in stats["negative"] if row["opportunities"] >= args.min_bucket_opportunities]
    positive = [row for row in stats["positive"] if row["opportunities"] >= args.min_bucket_opportunities]
    active_weights = dict(getattr(active, "weights", {}) or {})
    active_bias = float(getattr(active, "bias", 0.0) or 0.0)
    registry_pools = _registry_route_pools(active)
    registry_good_routes = registry_pools.get("good") or []
    registry_toxic_keys = set(registry_pools.get("toxic_keys") or [])
    variants: list[Any] = []
    seen: set[str] = set()
    seeded_phase = bool(getattr(args, "seed_summary", []))
    learned_phase = bool(seeded_phase or registry_good_routes)
    if learned_phase:
        single_quota = max(4, min(len(negative), int(args.batch_size * 0.02)))
        combo_quota = max(single_quota, int(args.batch_size * 0.03))
        rescue_quota = max(combo_quota, int(args.batch_size * 0.035))
        micro_quota = max(rescue_quota, int(args.batch_size * 0.10))
        portfolio_quota = max(micro_quota, int(args.batch_size * 0.98))
    else:
        single_quota = max(20, min(len(negative), int(args.batch_size * 0.20)))
        combo_quota = max(single_quota, int(args.batch_size * 0.45))
        rescue_quota = max(combo_quota, int(args.batch_size * 0.58))
        micro_quota = max(rescue_quota, int(args.batch_size * 0.75))
        portfolio_quota = max(micro_quota, int(args.batch_size * 0.90))

    builder_report = {
        "frontier_candidate_rows": 0,
        "frontier_added": 0,
        "frontier_rejected_toxic_base": 0,
        "frontier_allowed_contextual_toxic_base": 0,
        "frontier_extra_options_total": 0,
        "frontier_extra_noop_score_routes_skipped": 0,
        "frontier_synthetic_extra_options": 0,
        "frontier_synthetic_contextual_toxic_extra_options": 0,
        "frontier_bridge_combo_candidates": 0,
        "frontier_bridge_combo_added": 0,
        "toxic_rejected_variants": 0,
        "decision_duplicate_rejected": 0,
        "near_gate_candidate_rows": 0,
        "near_gate_trade_floor": 0,
        "boost_added": 0,
        "boost_extra_options_total": 0,
        "boost_synthetic_extra_options": 0,
        "edge_bridge_candidate_rows": 0,
        "edge_bridge_context_rows": 0,
        "edge_bridge_added": 0,
        "edge_bridge_attempted": 0,
        "edge_bridge_add_failed": 0,
        "edge_bridge_no_context_routes": 0,
        "edge_density_route_candidates": 0,
        "edge_density_added": 0,
        "edge_density_scale_added": 0,
        "mixed_added": 0,
        "mixed_attempts": 0,
        "mixed_cap": None,
        "mixed_skipped_no_positive_bucket": False,
    }

    def route_mask_hash(route: routed.Route) -> str:
        payload = routed.route_to_dict(route)
        payload.pop("name", None)
        key = tournament_safety.stable_json_hash(payload, length=32)
        cached = route_mask_hash_cache.get(key)
        if cached:
            return cached
        try:
            mask = routed.route_mask(compiled, route)
            cached = hashlib.sha1(np.asarray(mask, dtype=np.uint8).tobytes()).hexdigest()[:24]
        except Exception:
            cached = key
        route_mask_hash_cache[key] = cached
        return cached

    def decision_signature(variant: Any) -> str:
        route_parts = []
        for route in getattr(variant, "routes", ()) or ():
            payload = routed.route_to_dict(route)
            route_parts.append({
                "action": payload.get("action"),
                "mask": route_mask_hash(route),
                "weights": payload.get("weights") or {},
                "bias": round(float(payload.get("bias") or 0.0), 8),
            })
        return tournament_safety.stable_json_hash({
            "weights": dict(getattr(variant, "weights", {}) or {}),
            "bias": round(float(getattr(variant, "bias", 0.0) or 0.0), 8),
            "routes": route_parts,
        }, length=32)

    def add(
        name: str,
        routes: list[routed.Route],
        weights: dict | None = None,
        bias: float | None = None,
        *,
        allow_contextual_toxic_base: bool = False,
    ) -> bool:
        if len(variants) >= args.batch_size:
            return False
        if not routes:
            return False
        routes = _prune_zero_attribution_routes(compiled, _ordered_routes(routes))
        if not routes:
            return False
        rescue_count = sum(1 for item in routes if item.action != "skip")
        if _all_ticker_skip(routes) and rescue_count == 0:
            return False
        has_toxic_route = any(
            _route_identity(routed.route_to_dict(item)) in registry_toxic_keys
            for item in routes
            if item.action != "skip"
        )
        if has_toxic_route and not allow_contextual_toxic_base:
            builder_report["toxic_rejected_variants"] += 1
            return False
        if has_toxic_route and allow_contextual_toxic_base:
            builder_report["frontier_allowed_contextual_toxic_base"] += 1
        variant = routed.routed_variant(
            name,
            dict(weights if weights is not None else active_weights),
            active_bias if bias is None else float(bias),
            routes,
        )
        key = _variant_key(variant)
        if key in seen:
            return False
        try:
            decision_key = decision_signature(variant)
            if decision_key in decision_seen:
                builder_report["decision_duplicate_rejected"] += 1
                return False
        except Exception:
            decision_key = ""
        seen.add(key)
        if decision_key:
            decision_seen.add(decision_key)
        variants.append(variant)
        return True

    for variant in _seed_positive_variants(getattr(args, "seed_summary", []), limit=max(0, single_quota // 2)):
        if len(variants) >= args.batch_size:
            break
        key = _variant_key(variant)
        if key in seen:
            continue
        try:
            decision_key = decision_signature(variant)
            if decision_key in decision_seen:
                builder_report["decision_duplicate_rejected"] += 1
                continue
        except Exception:
            decision_key = ""
        seen.add(key)
        if decision_key:
            decision_seen.add(decision_key)
        variants.append(variant)

    # Skip probes: true score-only ablations of negative buckets.
    for idx, row in enumerate(negative[:single_quota]):
        add(f"profit_skip_single_{idx:04d}", [_skip_route(idx, row["route_key"])])

    for idx in range(args.batch_size):
        if len(variants) >= combo_quota:
            break
        pick_count = rng.randint(2, min(8, max(2, len(negative))))
        picks = rng.sample(negative[: min(len(negative), 120)], pick_count)
        routes = [_skip_route(idx * 10 + pos, row["route_key"]) for pos, row in enumerate(picks)]
        add(f"profit_skip_combo_{idx:04d}_{pick_count}routes", routes)

    # Trade-only pocket probes: skip broad areas first, then rescue specific pockets later.
    broad_skip_sets = [
        ["MARA|*|*|*"],
        ["MARA|*|*|*", "CLSK|*|*|*"],
        ["MARA|*|*|*", "RIOT|*|*|*"],
        ["CLSK|*|*|*", "RIOT|*|*|*"],
        ["CLSK|*|*|*", "MARA|*|*|*", "RIOT|*|*|*"],
    ]
    portfolio_skip_sets = [
        ["CLSK|*|*|*", "MARA|*|*|*", "RIOT|*|*|*"],
        ["MARA|*|*|*"],
        ["MARA|*|*|*", "CLSK|*|*|*"],
        ["MARA|*|*|*", "RIOT|*|*|*"],
        ["CLSK|*|*|*", "RIOT|*|*|*"],
    ]
    for idx, row in enumerate(positive):
        for action in dict.fromkeys([row["preferred_action"], "score", *ACTION_SET]):
            if len(variants) >= rescue_quota:
                break
            skips = rng.choice(broad_skip_sets)
            routes = [_skip_route(idx * 100 + pos, key) for pos, key in enumerate(skips)]
            routes.append(_rescue_route(idx, row["route_key"], action, active))
            add(f"profit_rescue_{idx:04d}_{action}_{row['route_key'].replace('|', '_')}", routes)
        if len(variants) >= rescue_quota:
            break

    micro_routes = _micro_filter_routes(compiled, positive, active, max(0, micro_quota - len(variants)))
    for idx, route in enumerate(micro_routes):
        if len(variants) >= micro_quota:
            break
        routes = [_skip_route(idx * 100 + pos, key) for pos, key in enumerate(["CLSK|*|*|*", "MARA|*|*|*", "RIOT|*|*|*"])]
        routes.append(route)
        add(f"profit_micro_{idx:04d}_{route.name}", routes)

    edge_density_quota = max(micro_quota, min(portfolio_quota, len(variants) + max(100, int(args.batch_size * 0.30))))
    edge_density_routes = _edge_density_routes(compiled, positive, active, args, max(0, edge_density_quota - len(variants)))
    builder_report["edge_density_route_candidates"] = len(edge_density_routes)
    edge_density_probe_quota = max(
        micro_quota,
        min(edge_density_quota, len(variants) + max(45, int(args.batch_size * 0.12))),
    )
    edge_idx = 0
    for idx, route in enumerate(edge_density_routes):
        if len(variants) >= edge_density_probe_quota:
            break
        routes = [_skip_route(24000 + idx * 100 + pos, key) for pos, key in enumerate(["CLSK|*|*|*", "MARA|*|*|*", "RIOT|*|*|*"])]
        routes.append(route)
        if add(f"profit_edge_density_single_{idx:04d}_{route.name}", routes):
            builder_report["edge_density_added"] += 1
        if len(variants) >= edge_density_probe_quota:
            break
        if idx % 4 == 0 and idx + 1 < len(edge_density_routes):
            edge_idx += 1
            routes = [_skip_route(25000 + edge_idx * 100 + pos, key) for pos, key in enumerate(["CLSK|*|*|*", "MARA|*|*|*", "RIOT|*|*|*"])]
            routes.extend(edge_density_routes[idx:idx + 2])
            if add(f"profit_edge_density_pair_{edge_idx:04d}_2routes", routes):
                builder_report["edge_density_added"] += 1
        if len(variants) >= edge_density_probe_quota:
            break
        if idx % 8 == 0 and idx + 2 < len(edge_density_routes):
            edge_idx += 1
            routes = [_skip_route(26000 + edge_idx * 100 + pos, key) for pos, key in enumerate(["CLSK|*|*|*", "MARA|*|*|*", "RIOT|*|*|*"])]
            routes.extend(edge_density_routes[idx:idx + 3])
            if add(f"profit_edge_density_triple_{edge_idx:04d}_3routes", routes):
                builder_report["edge_density_added"] += 1

    if edge_density_routes and len(variants) < edge_density_quota:
        long_pct = np.asarray(compiled["long_pnl_pct"], dtype=np.float64)
        short_pct = np.asarray(compiled["short_pnl_pct"], dtype=np.float64)
        scored_edge_routes = []
        for route in edge_density_routes:
            mask = routed.route_mask(compiled, route)
            count = int(np.sum(mask))
            if count <= 0:
                continue
            gross = float(np.sum(long_pct[mask])) if route.action == "force_long" else float(np.sum(short_pct[mask]))
            scored_edge_routes.append((gross / count if count else 0.0, gross, count, route, mask))
        scored_edge_routes.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
        scale_attempts = min(80, max(20, len(scored_edge_routes) * 2))
        for attempt_idx in range(scale_attempts):
            if len(variants) >= edge_density_quota:
                break
            selected: list[routed.Route] = []
            covered = np.zeros(int(compiled.get("rows") or 0), dtype=bool)
            start = attempt_idx % max(1, len(scored_edge_routes))
            ordered = scored_edge_routes[start:] + scored_edge_routes[:start]
            min_incremental = 3 if attempt_idx % 3 else 1
            for edge_per_trade, _gross, _count, route, mask in ordered:
                incremental_mask = mask & ~covered
                incremental_count = int(np.sum(incremental_mask))
                if incremental_count < min_incremental:
                    continue
                incremental_gross = float(np.sum(long_pct[incremental_mask])) if route.action == "force_long" else float(np.sum(short_pct[incremental_mask]))
                incremental_edge = incremental_gross / incremental_count if incremental_count else -1_000_000.0
                if incremental_edge <= 0.0 and len(selected) >= 2:
                    continue
                selected.append(route)
                covered |= mask
                if int(np.sum(covered)) >= int(args.min_trades) or len(selected) >= 24:
                    break
            if len(selected) < 2:
                continue
            routes = [_skip_route(27000 + attempt_idx * 100 + pos, key) for pos, key in enumerate(["CLSK|*|*|*", "MARA|*|*|*", "RIOT|*|*|*"])]
            routes.extend(selected)
            if add(f"profit_edge_density_scale_{attempt_idx:04d}_{len(selected)}routes_{int(np.sum(covered))}covered", routes):
                builder_report["edge_density_added"] += 1
                builder_report["edge_density_scale_added"] += 1

    registry_quota = max(micro_quota, min(portfolio_quota, len(variants) + max(30, int(args.batch_size * 0.12))))
    registry_idx = 0
    while registry_good_routes and len(variants) < registry_quota and registry_idx < args.batch_size * 4:
        registry_idx += 1
        pick_count = min(len(registry_good_routes), rng.randint(1, min(6, len(registry_good_routes))))
        routes = [_skip_route(30000 + registry_idx * 10 + pos, key) for pos, key in enumerate(["CLSK|*|*|*", "MARA|*|*|*", "RIOT|*|*|*"])]
        routes.extend(rng.sample(registry_good_routes, pick_count))
        add(f"profit_registry_good_{registry_idx:04d}_{pick_count}routes", routes)

    seed_rows = _seed_positive_rows(getattr(args, "seed_summary", []), limit=160)
    seed_routes = [
        route for route in _seed_positive_routes(getattr(args, "seed_summary", []), limit=160)
        if _route_identity(routed.route_to_dict(route)) not in registry_toxic_keys
    ]
    frontier_trade_floor = max(
        1,
        min(
            int(args.min_trades) - (75 if learned_phase else 25),
            int(int(args.min_trades) * (0.50 if learned_phase else 0.85)),
        ),
    )
    frontier_rows = [
        row for row in sorted(
            seed_rows,
            key=lambda item: (
                int(item.get("step2_trades") or 0),
                float((item.get("holdout_gate") or {}).get("holdout_pnl") or -1_000_000.0),
                float(item.get("step2_pnl") or 0.0),
            ),
            reverse=True,
        )
        if (
            int(row.get("step2_trades") or 0) >= frontier_trade_floor
            and float(row.get("step2_pnl") or 0.0) > 0.0
            and float((row.get("holdout_gate") or {}).get("holdout_pnl") or -1_000_000.0) > 0.0
        )
    ]
    builder_report["frontier_trade_floor"] = frontier_trade_floor
    builder_report["frontier_candidate_rows"] = len(frontier_rows)
    edge_bridge_reserve = max(0, max(80, int(args.batch_size * 0.25)) if edge_density_routes and seed_rows else 0)
    frontier_quota = min(
        max(0, args.batch_size - edge_bridge_reserve),
        len(variants) + max(140 if learned_phase else 80, int(args.batch_size * (0.30 if learned_phase else 0.20))),
    )
    builder_report["edge_bridge_reserved_slots"] = edge_bridge_reserve
    frontier_idx = 0
    for base_row in frontier_rows[:24]:
        if len(variants) >= frontier_quota:
            break
        base_weights = dict(base_row.get("weights") or active_weights)
        base_bias = float(base_row.get("bias") or active_bias)
        base_routes = [
            route for route in _seed_row_routes(base_row)
            if _route_identity(routed.route_to_dict(route)) not in registry_toxic_keys
        ]
        if not base_routes:
            continue
        base_route_keys = {_route_identity(routed.route_to_dict(route)) for route in base_routes}
        base_mask = np.zeros(int(compiled.get("rows") or 0), dtype=bool)
        base_increment_mask = np.zeros(int(compiled.get("rows") or 0), dtype=bool)
        for route in base_routes:
            mask = routed.route_mask(compiled, route)
            base_mask |= mask
            if route.action != "skip":
                base_increment_mask |= mask
        if not np.any(base_increment_mask):
            base_increment_mask = np.array(base_mask, copy=True)
        extra_options = []
        for route in (registry_good_routes + seed_routes):
            route_key = _route_identity(routed.route_to_dict(route))
            if route_key in base_route_keys or route_key in registry_toxic_keys:
                continue
            if (
                route.action == "score"
                and dict(route.weights or {}) == base_weights
                and float(route.bias or 0.0) == base_bias
            ):
                builder_report["frontier_extra_noop_score_routes_skipped"] += 1
                continue
            mask = routed.route_mask(compiled, route)
            matched = int(np.sum(mask))
            if matched <= 0:
                continue
            extra_options.append((matched, matched, route))
        if learned_phase or len(extra_options) < 5:
            synthetic_options = []
            synthetic_limit = 220 if learned_phase else 80
            for synthetic_idx, row in enumerate(positive[:synthetic_limit]):
                route = _rescue_route(70000 + synthetic_idx, row["route_key"], row["preferred_action"], active)
                route_key = _route_identity(routed.route_to_dict(route))
                if route_key in base_route_keys:
                    continue
                mask = routed.route_mask(compiled, route)
                matched = int(np.sum(mask))
                if matched <= 0:
                    continue
                synthetic_options.append((
                    matched,
                    matched,
                    float(row.get("preferred_avg_pct") or 0.0),
                    route,
                ))
            synthetic_options.sort(key=lambda item: (abs(item[0] - 20), -item[2], item[1]))
            needed = max(0, (8 if learned_phase else 5) - len(extra_options))
            for incremental, total, _preferred_avg, route in synthetic_options[:needed]:
                extra_options.append((incremental, total, route))
                builder_report["frontier_synthetic_extra_options"] += 1
                if _route_identity(routed.route_to_dict(route)) in registry_toxic_keys:
                    builder_report["frontier_synthetic_contextual_toxic_extra_options"] += 1
        extra_options.sort(key=lambda item: (abs(item[0] - 20), item[1]))
        if learned_phase and synthetic_options:
            existing_extra_keys = {
                _route_identity(routed.route_to_dict(route))
                for _incremental, _total, route in extra_options
            }
            supplemental_added = 0
            for incremental, total, _preferred_avg, route in synthetic_options:
                route_key = _route_identity(routed.route_to_dict(route))
                if route_key in existing_extra_keys:
                    continue
                extra_options.append((incremental, total, route))
                existing_extra_keys.add(route_key)
                supplemental_added += 1
                builder_report["frontier_synthetic_extra_options"] += 1
                if supplemental_added >= 24:
                    break
            extra_options.sort(key=lambda item: (abs(item[0] - 20), item[1]))
        extras = [route for _, _, route in extra_options]
        builder_report["frontier_extra_options_total"] += len(extras)
        max_extra_count = 8 if learned_phase else 5
        if learned_phase and extras:
            greedy_extras: list[routed.Route] = []
            current_mask = np.array(base_increment_mask, copy=True)
            remaining = list(extras)
            while remaining and len(greedy_extras) < max_extra_count:
                best = None
                for route in remaining:
                    mask = routed.route_mask(compiled, route)
                    incremental = int(np.sum(mask & ~current_mask))
                    if incremental <= 0:
                        continue
                    route_match = getattr(route, "match", {}) or {}
                    if str(route_match.get("ticker") or "") == "CLSK" and incremental > 20:
                        continue
                    score = (
                        0 if 5 <= incremental <= 40 else 1,
                        abs(incremental - 20),
                        incremental,
                        route.name,
                    )
                    if best is None or score < best[0]:
                        best = (score, route, mask)
                if best is None:
                    break
                _score, route, mask = best
                greedy_extras.append(route)
                current_mask |= mask
                remaining = [
                    item for item in remaining
                    if tournament_safety.stable_json_hash(routed.route_to_dict(item), length=24)
                    != tournament_safety.stable_json_hash(routed.route_to_dict(route), length=24)
                ]
            if greedy_extras:
                greedy_keys = {
                    tournament_safety.stable_json_hash(routed.route_to_dict(route), length=24)
                    for route in greedy_extras
                }
                extras = greedy_extras + [
                    route for route in extras
                    if tournament_safety.stable_json_hash(routed.route_to_dict(route), length=24) not in greedy_keys
                ]
        extra_counts = [0] + list(range(1, min(max_extra_count, len(extras)) + 1))
        for extra_count in extra_counts:
            if len(variants) >= frontier_quota:
                break
            frontier_idx += 1
            routes = list(base_routes)
            if extra_count:
                routes.extend(extras[:extra_count])
            before = len(variants)
            added = add(
                f"profit_frontier_{frontier_idx:04d}_{int(base_row.get('step2_trades') or 0)}base_{extra_count}extras",
                routes,
                weights=base_weights,
                bias=base_bias,
                allow_contextual_toxic_base=True,
            )
            if added:
                builder_report["frontier_added"] += 1
            elif len(variants) == before:
                builder_report["frontier_rejected_toxic_base"] += 0
        if learned_phase and extras and len(variants) < frontier_quota:
            trade_gap = max(1, int(args.min_trades) - int(base_row.get("step2_trades") or 0))
            extras_with_masks = []
            for route in extras[: min(len(extras), 32)]:
                mask = routed.route_mask(compiled, route)
                incremental = int(np.sum(mask & ~base_increment_mask))
                if incremental > 0:
                    extras_with_masks.append((route, mask, incremental))
            combo_candidates = []
            combo_seen: set[str] = set()

            def remember_combo(combo: list[tuple[routed.Route, np.ndarray, int]]) -> None:
                if len(combo) < 2:
                    return
                combo_key = "|".join(
                    tournament_safety.stable_json_hash(routed.route_to_dict(route), length=16)
                    for route, _mask, _incremental in combo
                )
                if combo_key in combo_seen:
                    return
                combo_seen.add(combo_key)
                union_mask = np.array(base_increment_mask, copy=True)
                for _route, mask, _incremental in combo:
                    union_mask |= mask
                incremental_total = int(np.sum(union_mask & ~base_increment_mask))
                if incremental_total <= 0:
                    return
                combo_candidates.append((abs(incremental_total - trade_gap), incremental_total, len(combo), combo_key, combo))

            for combo_size in range(2, min(8, len(extras_with_masks)) + 1):
                for start in range(0, max(1, len(extras_with_masks) - combo_size + 1)):
                    remember_combo(extras_with_masks[start:start + combo_size])
                for attempt_idx in range(min(80, len(extras_with_masks) * 3)):
                    pick_start = (frontier_idx + attempt_idx * 3 + combo_size) % len(extras_with_masks)
                    ordered = extras_with_masks[pick_start:] + extras_with_masks[:pick_start]
                    spread_combo = ordered[::max(1, len(ordered) // combo_size)][:combo_size]
                    remember_combo(spread_combo)
            combo_candidates.sort(key=lambda item: (item[0], abs(item[1] - trade_gap), item[2], item[3]))
            builder_report["frontier_bridge_combo_candidates"] += len(combo_candidates)
            for _distance, incremental_total, combo_size, _combo_key, combo in combo_candidates[:120]:
                if len(variants) >= frontier_quota:
                    break
                if incremental_total < max(20, int(trade_gap * 0.25)):
                    continue
                frontier_idx += 1
                routes = list(base_routes) + [route for route, _mask, _incremental in combo]
                if add(
                    f"profit_frontier_bridge_{frontier_idx:04d}_{int(base_row.get('step2_trades') or 0)}base_{combo_size}extras_{incremental_total}inc",
                    routes,
                    weights=base_weights,
                    bias=base_bias,
                    allow_contextual_toxic_base=True,
                ):
                    builder_report["frontier_bridge_combo_added"] += 1
    boost_quota = max(micro_quota, min(portfolio_quota, len(variants) + max(90, int(args.batch_size * 0.24))))
    if edge_bridge_reserve:
        boost_quota = min(boost_quota, max(len(variants), args.batch_size - edge_bridge_reserve))
    near_gate_trade_floor = max(
        1,
        min(
            int(args.min_trades) - (100 if learned_phase else 50),
            int(int(args.min_trades) * (0.50 if learned_phase else 0.80)),
        ),
    )
    near_gate_rows = [
        row for row in sorted(
            seed_rows,
            key=lambda item: (
                int(item.get("step2_trades") or 0),
                float((item.get("holdout_gate") or {}).get("holdout_pnl") or -1_000_000.0),
                float(item.get("step2_pnl") or 0.0),
            ),
            reverse=True,
        )
        if (
            int(row.get("step2_trades") or 0) >= near_gate_trade_floor
            and float(row.get("step2_pnl") or 0.0) > 0.0
            and float((row.get("holdout_gate") or {}).get("holdout_pnl") or -1_000_000.0) > 0.0
        )
    ]
    builder_report["near_gate_trade_floor"] = near_gate_trade_floor
    builder_report["near_gate_candidate_rows"] = len(near_gate_rows)
    boost_idx = 0
    for base_row in near_gate_rows[:24]:
        if len(variants) >= boost_quota:
            break
        base_routes = [
            route for route in _seed_row_routes(base_row)
            if _route_identity(routed.route_to_dict(route)) not in registry_toxic_keys
        ]
        if not base_routes:
            continue
        base_route_keys = {
            tournament_safety.stable_json_hash(routed.route_to_dict(route), length=24)
            for route in base_routes
        }
        base_identity_keys = {_route_identity(routed.route_to_dict(route)) for route in base_routes}
        base_mask = np.zeros(int(compiled.get("rows") or 0), dtype=bool)
        base_increment_mask = np.zeros(int(compiled.get("rows") or 0), dtype=bool)
        for route in base_routes:
            mask = routed.route_mask(compiled, route)
            base_mask |= mask
            if route.action != "skip":
                base_increment_mask |= mask
        if not np.any(base_increment_mask):
            base_increment_mask = np.array(base_mask, copy=True)
        trade_gap = max(1, int(args.min_trades) - int(base_row.get("step2_trades") or 0))
        extra_pool = []
        for route in seed_routes:
            route_key = tournament_safety.stable_json_hash(routed.route_to_dict(route), length=24)
            if route_key in base_route_keys:
                continue
            mask = routed.route_mask(compiled, route)
            incremental = int(np.sum(mask & ~base_increment_mask))
            matched = int(np.sum(mask))
            if incremental <= 0:
                continue
            if trade_gap <= 25 and incremental > 80:
                continue
            extra_pool.append((abs(incremental - trade_gap), incremental, matched, route))
        synthetic_limit = 260 if learned_phase else 80
        for synthetic_idx, row in enumerate(positive[:synthetic_limit]):
            route = _rescue_route(76000 + boost_idx * 1000 + synthetic_idx, row["route_key"], row["preferred_action"], active)
            route_hash = tournament_safety.stable_json_hash(routed.route_to_dict(route), length=24)
            route_identity = _route_identity(routed.route_to_dict(route))
            if route_hash in base_route_keys or route_identity in base_identity_keys or route_identity in registry_toxic_keys:
                continue
            mask = routed.route_mask(compiled, route)
            incremental = int(np.sum(mask & ~base_increment_mask))
            matched = int(np.sum(mask))
            if incremental <= 0:
                continue
            if trade_gap <= 25 and incremental > 80:
                continue
            extra_pool.append((abs(incremental - trade_gap), incremental, matched, route))
            builder_report["boost_synthetic_extra_options"] += 1
        extra_pool.sort(key=lambda item: (item[0], item[1], item[2], item[3].name))
        builder_report["boost_extra_options_total"] += len(extra_pool)
        extra_routes = [route for _, _, _, route in extra_pool]
        for extra_idx, extra_route in enumerate(extra_routes[:80]):
            boost_idx += 1
            routes = list(base_routes) + [extra_route]
            if add(
                f"profit_boost_single_{boost_idx:04d}_{int(base_row.get('step2_trades') or 0)}base",
                routes,
                weights=base_row.get("weights") or active_weights,
                bias=float(base_row.get("bias") or active_bias),
            ):
                builder_report["boost_added"] += 1
            if len(variants) >= boost_quota:
                break
            if extra_idx % 5 == 0 and extra_idx + 1 < len(extra_routes):
                boost_idx += 1
                routes = list(base_routes) + [extra_route, extra_routes[extra_idx + 1]]
                if add(
                    f"profit_boost_pair_{boost_idx:04d}_{int(base_row.get('step2_trades') or 0)}base",
                    routes,
                    weights=base_row.get("weights") or active_weights,
                    bias=float(base_row.get("bias") or active_bias),
                ):
                    builder_report["boost_added"] += 1
                if len(variants) >= boost_quota:
                    break

    edge_bridge_quota = max(micro_quota, min(args.batch_size, len(variants) + max(80, int(args.batch_size * 0.20))))
    edge_bridge_rows = [
        row for row in sorted(
            seed_rows,
            key=lambda item: (
                float(item.get("execution_adjusted_pnl") or -1_000_000.0),
                _row_edge_density(item),
                float((item.get("holdout_gate") or {}).get("holdout_pnl") or -1_000_000.0),
            ),
            reverse=True,
        )
        if (
            float(row.get("execution_adjusted_pnl") or -1_000_000.0) > 0.0
            and float(row.get("step2_pnl") or 0.0) > 0.0
            and float((row.get("holdout_gate") or {}).get("holdout_pnl") or -1_000_000.0) > 0.0
            and int(row.get("step2_trades") or 0) < int(args.min_trades)
        )
    ]
    edge_bridge_context_rows = [
        row for row in sorted(
            seed_rows,
            key=lambda item: (
                int(item.get("step2_trades") or 0),
                float((item.get("holdout_gate") or {}).get("holdout_pnl") or -1_000_000.0),
                float(item.get("step2_pnl") or 0.0),
            ),
            reverse=True,
        )
        if (
            int(row.get("step2_trades") or 0) >= max(1, int(args.min_trades * 0.55))
            and float(row.get("step2_pnl") or 0.0) > 0.0
            and float((row.get("holdout_gate") or {}).get("holdout_pnl") or -1_000_000.0) > 0.0
        )
    ]
    builder_report["edge_bridge_candidate_rows"] = len(edge_bridge_rows)
    builder_report["edge_bridge_context_rows"] = len(edge_bridge_context_rows)
    edge_bridge_idx = 0
    for edge_row in edge_bridge_rows[:12]:
        if len(variants) >= edge_bridge_quota:
            break
        edge_routes = [
            route for route in _seed_row_routes(edge_row)
            if _route_identity(routed.route_to_dict(route)) not in registry_toxic_keys
        ]
        if not edge_routes:
            continue
        edge_route_keys = {
            tournament_safety.stable_json_hash(routed.route_to_dict(route), length=24)
            for route in edge_routes
        }
        for context_start in range(min(16, len(edge_bridge_context_rows))):
            if len(variants) >= edge_bridge_quota:
                break
            for context_count in (1, 2, 3, 4, 6):
                context_rows = edge_bridge_context_rows[context_start:context_start + context_count]
                if not context_rows:
                    continue
                routes = list(edge_routes)
                route_seen = set(edge_route_keys)
                for context_row in context_rows:
                    for route in _seed_row_rescue_routes(context_row):
                        route_key = tournament_safety.stable_json_hash(routed.route_to_dict(route), length=24)
                        if route_key in route_seen or _route_identity(routed.route_to_dict(route)) in registry_toxic_keys:
                            continue
                        route_seen.add(route_key)
                        routes.append(route)
                        if len(route_seen) >= 36:
                            break
                    if len(route_seen) >= 36:
                        break
                if len(routes) <= len(edge_routes):
                    builder_report["edge_bridge_no_context_routes"] += 1
                    continue
                edge_bridge_idx += 1
                builder_report["edge_bridge_attempted"] += 1
                if add(
                    f"profit_edge_bridge_{edge_bridge_idx:04d}_{int(edge_row.get('step2_trades') or 0)}edge_{len(context_rows)}ctx_{len(route_seen)}routes",
                    routes,
                    weights=edge_row.get("weights") or active_weights,
                    bias=float(edge_row.get("bias") or active_bias),
                    allow_contextual_toxic_base=True,
                ):
                    builder_report["edge_bridge_added"] += 1
                else:
                    builder_report["edge_bridge_add_failed"] += 1

    if edge_density_routes and edge_bridge_context_rows and len(variants) < edge_bridge_quota:
        synthetic_edge_sets: list[list[routed.Route]] = []
        for idx, route in enumerate(edge_density_routes[:36]):
            synthetic_edge_sets.append([route])
            if idx % 4 == 0 and idx + 1 < len(edge_density_routes):
                synthetic_edge_sets.append(edge_density_routes[idx:idx + 2])
            if idx % 8 == 0 and idx + 2 < len(edge_density_routes):
                synthetic_edge_sets.append(edge_density_routes[idx:idx + 3])
        for edge_set_idx, edge_routes in enumerate(synthetic_edge_sets[:48]):
            if len(variants) >= edge_bridge_quota:
                break
            edge_route_keys = {
                tournament_safety.stable_json_hash(routed.route_to_dict(route), length=24)
                for route in edge_routes
            }
            for context_start in range(min(16, len(edge_bridge_context_rows))):
                if len(variants) >= edge_bridge_quota:
                    break
                for context_count in (1, 2, 3, 4, 6):
                    context_rows = edge_bridge_context_rows[context_start:context_start + context_count]
                    if not context_rows:
                        continue
                    routes = [_skip_route(28000 + edge_bridge_idx * 100 + pos, key) for pos, key in enumerate(["CLSK|*|*|*", "MARA|*|*|*", "RIOT|*|*|*"])]
                    routes.extend(edge_routes)
                    route_seen = set(edge_route_keys)
                    for context_row in context_rows:
                        for route in _seed_row_rescue_routes(context_row):
                            route_key = tournament_safety.stable_json_hash(routed.route_to_dict(route), length=24)
                            if route_key in route_seen or _route_identity(routed.route_to_dict(route)) in registry_toxic_keys:
                                continue
                            route_seen.add(route_key)
                            routes.append(route)
                            if len(route_seen) >= 36:
                                break
                        if len(route_seen) >= 36:
                            break
                    if len(route_seen) <= len(edge_route_keys):
                        builder_report["edge_bridge_no_context_routes"] += 1
                        continue
                    edge_bridge_idx += 1
                    builder_report["edge_bridge_attempted"] += 1
                    if add(
                        f"profit_edge_bridge_synth_{edge_bridge_idx:04d}_{len(edge_routes)}edge_{len(context_rows)}ctx_{len(route_seen)}routes",
                        routes,
                        allow_contextual_toxic_base=True,
                    ):
                        builder_report["edge_bridge_added"] += 1
                    else:
                        builder_report["edge_bridge_add_failed"] += 1

    scaleup_quota = max(micro_quota, min(portfolio_quota, len(variants) + max(50, int(args.batch_size * 0.18))))
    scaleup_sources = {
        "holdout": sorted(
            seed_rows,
            key=lambda row: (
                float((row.get("holdout_gate") or {}).get("holdout_pnl") or -1_000_000.0),
                float(row.get("step2_pnl") or 0.0),
                int(row.get("step2_trades") or 0),
            ),
            reverse=True,
        ),
        "total": sorted(
            seed_rows,
            key=lambda row: (
                float(row.get("step2_pnl") or 0.0),
                float((row.get("holdout_gate") or {}).get("holdout_pnl") or -1_000_000.0),
                int(row.get("step2_trades") or 0),
            ),
            reverse=True,
        ),
        "sample": sorted(
            seed_rows,
            key=lambda row: (
                int(row.get("step2_trades") or 0),
                float((row.get("holdout_gate") or {}).get("holdout_pnl") or -1_000_000.0),
                float(row.get("step2_pnl") or 0.0),
            ),
            reverse=True,
        ),
    }
    scaleup_idx = 0
    for lane, source_rows in scaleup_sources.items():
        if len(variants) >= scaleup_quota:
            break
        for start in range(min(18, len(source_rows))):
            if len(variants) >= scaleup_quota:
                break
            for pick_count in (2, 3, 4, 5, 6, 8, 10, 12):
                selected = source_rows[start:start + pick_count]
                if len(selected) < 2:
                    continue
                routes = [
                    _skip_route(scaleup_idx * 100 + pos, key)
                    for pos, key in enumerate(["CLSK|*|*|*", "MARA|*|*|*", "RIOT|*|*|*"])
                ]
                route_seen: set[str] = set()
                for row in selected:
                    for route in _seed_row_rescue_routes(row):
                        route_key = tournament_safety.stable_json_hash(routed.route_to_dict(route), length=24)
                        if route_key in route_seen or _route_identity(routed.route_to_dict(route)) in registry_toxic_keys:
                            continue
                        route_seen.add(route_key)
                        routes.append(route)
                        if len(route_seen) >= 32:
                            break
                    if len(route_seen) >= 32:
                        break
                scaleup_idx += 1
                add(f"profit_scaleup_{lane}_{scaleup_idx:04d}_{len(selected)}rows_{len(route_seen)}pockets", routes)
                if len(variants) >= scaleup_quota:
                    break

    long_seed_routes = [route for route in seed_routes if _route_side_bias(route) != "short"]
    strict_long_seed_routes = [route for route in seed_routes if _route_side_bias(route) == "long"]
    portfolio_idx = 0
    while seed_routes and len(variants) < portfolio_quota and portfolio_idx < args.batch_size * 10:
        portfolio_idx += 1
        if strict_long_seed_routes and portfolio_idx % 3 == 0:
            pool = strict_long_seed_routes
            lane = "strict_long"
        elif long_seed_routes and portfolio_idx % 2 == 0:
            pool = long_seed_routes
            lane = "non_short"
        else:
            pool = seed_routes
            lane = "all"
        pick_count = rng.randint(1, min(14, len(pool)))
        if portfolio_idx % 4 == 0:
            skip_keys = rng.choice(portfolio_skip_sets[1:])
            lane = f"{lane}_partial"
        else:
            skip_keys = portfolio_skip_sets[0]
        routes = [_skip_route(portfolio_idx * 100 + pos, key) for pos, key in enumerate(skip_keys)]
        routes.extend(rng.sample(pool, pick_count))
        add(f"profit_portfolio_{lane}_{portfolio_idx:04d}_{pick_count}pockets", routes)

    # Fill with mixed skip-plus-rescue combinations if the deterministic probes were not enough.
    attempt = 0
    mixed_cap = args.batch_size
    if learned_phase and seed_rows:
        mixed_cap = max(len(variants), int(args.batch_size * 0.35))
    builder_report["mixed_cap"] = mixed_cap
    if not positive:
        builder_report["mixed_skipped_no_positive_bucket"] = True
        mixed_cap = len(variants)
    while len(variants) < mixed_cap and attempt < args.batch_size * 20:
        attempt += 1
        routes = []
        for key in rng.choice(broad_skip_sets):
            routes.append(_skip_route(5000 + attempt + len(routes), key))
        for row in rng.sample(negative[: min(len(negative), 120)], rng.randint(0, 4)):
            routes.append(_skip_route(7000 + attempt + len(routes), row["route_key"]))
        for row in rng.sample(positive[: min(len(positive), 80)], rng.randint(1, 3)):
            action = rng.choice([row["preferred_action"], "score", "force_long", "force_short"])
            routes.append(_rescue_route(9000 + attempt + len(routes), row["route_key"], action, active))
        if add(f"profit_mixed_{attempt:04d}_{len(routes)}routes", routes):
            builder_report["mixed_added"] += 1
    builder_report["mixed_attempts"] = attempt

    stats["variant_builder_report"] = builder_report
    return variants[: args.batch_size], stats


def _decorate(rows: list[dict], active_pnl: float, args: argparse.Namespace) -> None:
    cfg = _load_trading_config()
    tested_count = len(rows)
    for row in rows:
        pnl = float(row.get("step2_pnl") or 0.0)
        trades = int(row.get("step2_trades") or 0)
        eligible = trades >= int(args.min_trades)
        row["step2_delta_vs_active"] = round(pnl - active_pnl, 4)
        row["step2_delta_pct_vs_active"] = round((pnl - active_pnl) / abs(active_pnl) * 100.0, 4) if active_pnl else None
        row["min_trade_gate"] = {
            "ok": eligible,
            "min_trades": int(args.min_trades),
            "actual_trades": trades,
        }
        row["beats_target_pnl"] = bool(pnl > float(args.target_pnl) and eligible)
        row["profit_hunt_rank_score"] = round(pnl if eligible else (-1_000_000_000.0 + pnl + trades / 1000.0), 6)
        objective = step2_objective.candidate_objective(
            row,
            active_pnl=active_pnl,
            start_balance=float(getattr(args, "start_balance", step2_objective.DEFAULT_START_BALANCE) or step2_objective.DEFAULT_START_BALANCE),
            config=cfg,
        )
        row["execution_adjusted_objective"] = objective
        row["execution_adjusted_pnl"] = objective["execution_adjusted_pnl"]
        row["objective_score"] = objective["objective_score"]
        row["replay_validity"] = objective["replay_validity"]
        data_contract = step2_data_quality.row_contract(row)
        row["data_quality_contract"] = data_contract
        row["data_quality_score"] = data_contract["score"]
        row["learning_allowed"] = bool(data_contract["learning_allowed"])
        statistical = step2_statistical_validation.candidate_validation(
            row,
            tested_count=tested_count,
            min_trades=int(getattr(args, "min_trades", 100) or 100),
        )
        row["statistical_validation"] = statistical
        row["statistical_validation_score"] = statistical["score"]
        deployment = step2_deployment_risk.candidate_risk(
            row,
            start_balance=float(getattr(args, "start_balance", step2_deployment_risk.DEFAULT_START_BALANCE) or step2_deployment_risk.DEFAULT_START_BALANCE),
        )
        row["deployment_risk"] = deployment
        row["deployment_risk_score"] = deployment["risk_score"]
        if row.get("routes") and not row.get("routed_profile_safety"):
            row["routed_profile_safety"] = routed_profile_safety.evaluate_candidate(row)


def _holdout_pnl(row: dict, holdout_days: list[str]) -> float:
    by_day = row.get("by_day") if isinstance(row.get("by_day"), dict) else {}
    return round(sum(float((by_day.get(day) or {}).get("pnl") or 0.0) for day in holdout_days), 6)


def _attach_holdout_rank(rows: list[dict], active: dict) -> None:
    active_days = sorted((active.get("by_day") or {}).keys())
    if not active_days:
        return
    holdout_count = max(1, int(round(len(active_days) * 0.30)))
    holdout_days = active_days[-holdout_count:]
    for row in rows:
        trades = int(row.get("step2_trades") or 0)
        pnl = float(row.get("step2_pnl") or 0.0)
        holdout = _holdout_pnl(row, holdout_days)
        row["holdout_gate"] = {
            "holdout_days": holdout_days,
            "holdout_pnl": holdout,
            "ok": holdout > 0.0,
        }
        eligible = bool((row.get("min_trade_gate") or {}).get("ok"))
        sample_bonus = min(500.0, trades) / 10.0
        holdout_bonus = holdout * 2.0 if holdout > 0.0 else holdout * 4.0
        row["profit_hunt_rank_score"] = round(
            (pnl + holdout_bonus + sample_bonus) if eligible else (-1_000_000_000.0 + pnl + trades / 1000.0),
            6,
        )


def _behavior_key(row: dict) -> str:
    return tournament_safety.stable_json_hash({
        "pnl": round(float(row.get("step2_pnl") or 0.0), 4),
        "trades": int(row.get("step2_trades") or 0),
        "wins": int(row.get("step2_wins") or 0),
        "losses": int(row.get("step2_losses") or 0),
        "holdout_pnl": round(float((row.get("holdout_gate") or {}).get("holdout_pnl") or 0.0), 4),
        "by_ticker": row.get("by_ticker") or {},
        "by_side": row.get("by_side") or {},
        "by_day": row.get("by_day") or {},
    }, length=32)


def _dedupe_behavior(rows: list[dict]) -> list[dict]:
    out: list[dict] = []
    seen: set[str] = set()
    for row in rows:
        key = _behavior_key(row)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _file_sha256(path: str | Path) -> str | None:
    try:
        h = hashlib.sha256()
        with Path(path).open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None


def _holdout_value(row: dict) -> float:
    return float((row.get("holdout_gate") or {}).get("holdout_pnl") or 0.0)


def _lane_name(row_or_variant: Any) -> str:
    name = str(getattr(row_or_variant, "name", "") or (row_or_variant.get("variant") if isinstance(row_or_variant, dict) else "") or "")
    while name.startswith("seed_exact_"):
        name = name[len("seed_exact_"):]
    for lane in ("frontier", "edge_bridge", "edge_density", "registry_good", "boost", "scaleup", "portfolio", "micro", "rescue", "mixed", "skip_combo", "skip_single"):
        if name.startswith(f"profit_{lane}"):
            return lane
    return "seed_exact" if str(getattr(row_or_variant, "name", "") or (row_or_variant.get("variant") if isinstance(row_or_variant, dict) else "")).startswith("seed_exact_") else "other"


def _behavior_duplicate_report(rows: list[dict], label: str) -> dict:
    counts: dict[str, int] = defaultdict(int)
    examples: dict[str, dict] = {}
    for row in rows:
        key = _behavior_key(row)
        counts[key] += 1
        examples.setdefault(key, row)
    duplicate_groups = [
        {
            "behavior_key": key,
            "count": count,
            "representative_variant": examples[key].get("variant"),
            "pnl": examples[key].get("step2_pnl"),
            "trades": examples[key].get("step2_trades"),
            "holdout_pnl": _holdout_value(examples[key]),
        }
        for key, count in counts.items()
        if count > 1
    ]
    duplicate_groups.sort(key=lambda item: item["count"], reverse=True)
    return {
        "label": label,
        "variant_count": len(rows),
        "distinct_behavior_count": len(counts),
        "duplicate_behavior_count": max(0, len(rows) - len(counts)),
        "duplicate_group_count": len(duplicate_groups),
        "top_duplicate_groups": duplicate_groups[:20],
    }


def _row_brief(row: dict | None) -> dict | None:
    if not isinstance(row, dict):
        return None
    return {
        "variant": row.get("variant"),
        "pnl": row.get("step2_pnl"),
        "trades": row.get("step2_trades"),
        "holdout_pnl": _holdout_value(row),
        "holdout_ok": bool((row.get("holdout_gate") or {}).get("ok")),
        "by_ticker": row.get("by_ticker"),
        "by_side": row.get("by_side"),
    }


def _execution_viability_gap_report(rows: list[dict], args: argparse.Namespace) -> dict:
    cost_cfg = _load_trading_config()
    cost_rows = []
    source_by_variant = {}
    for row in rows:
        source_by_variant[str(row.get("variant") or "")] = row
        objective = row.get("execution_adjusted_objective") if isinstance(row.get("execution_adjusted_objective"), dict) else {}
        costs = objective.get("estimated_execution_costs") if isinstance(objective.get("estimated_execution_costs"), dict) else {}
        trades = max(0, int(row.get("step2_trades") or 0))
        gross = float(row.get("step2_pnl") or 0.0)
        estimated_cost = float(costs.get("estimated_total_cost") or 0.0)
        fill_probability = float(objective.get("fill_probability") or 1.0)
        adjusted = float(row.get("execution_adjusted_pnl") or objective.get("execution_adjusted_pnl") or 0.0)
        gross_per_trade = gross / trades if trades else 0.0
        cost_per_trade = estimated_cost / trades if trades else 0.0
        cost_rows.append({
            "variant": row.get("variant"),
            "gross_pnl": round(gross, 6),
            "trades": trades,
            "gross_per_trade": round(gross_per_trade, 6),
            "estimated_cost": round(estimated_cost, 6),
            "estimated_cost_per_trade": round(cost_per_trade, 6),
            "execution_adjusted_pnl": round(adjusted, 6),
            "execution_gap_to_positive": round(max(0.0, estimated_cost - gross), 6),
            "fill_probability": round(fill_probability, 6),
            "holdout_pnl": _holdout_value(row),
            "holdout_ok": bool((row.get("holdout_gate") or {}).get("ok")),
            "min_trade_gate_ok": bool((row.get("min_trade_gate") or {}).get("ok")),
            "lane": _lane_name(row),
        })
    by_adjusted = sorted(cost_rows, key=lambda item: item["execution_adjusted_pnl"], reverse=True)
    by_density = sorted(cost_rows, key=lambda item: (item["gross_per_trade"], item["holdout_pnl"]), reverse=True)
    gate_adjusted = [item for item in by_adjusted if item["min_trade_gate_ok"]]
    adjusted_positive = [item for item in by_adjusted if item["execution_adjusted_pnl"] > float(getattr(args, "target_pnl", 0.0) or 0.0)]
    adjusted_gate_positive = [item for item in adjusted_positive if item["min_trade_gate_ok"]]
    min_trades = int(getattr(args, "min_trades", 0) or 0)
    representative_cost_per_trade = 0.0
    if cost_rows:
        representative_cost_per_trade = max(item["estimated_cost_per_trade"] for item in cost_rows)
    blockers = []
    if not adjusted_positive:
        blockers.append("no_execution_adjusted_positive_candidate")
    if not adjusted_gate_positive:
        blockers.append("no_min_trade_execution_adjusted_positive_candidate")
    if by_density and by_density[0]["gross_per_trade"] < representative_cost_per_trade:
        blockers.append("best_gross_per_trade_below_estimated_cost_per_trade")

    def seed_payload(item: dict) -> dict:
        source = source_by_variant.get(str(item.get("variant") or "")) or {}
        return {
            "variant": item.get("variant"),
            "step2_pnl": item.get("gross_pnl"),
            "step2_trades": item.get("trades"),
            "execution_adjusted_pnl": item.get("execution_adjusted_pnl"),
            "objective_score": item.get("execution_adjusted_pnl"),
            "holdout_gate": {
                "ok": item.get("holdout_ok"),
                "holdout_pnl": item.get("holdout_pnl"),
            },
            "weights": source.get("weights") or {},
            "bias": float(source.get("bias") or 0.0),
            "routes": source.get("routes") or [],
        }

    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "objective_version": step2_objective.OBJECTIVE_VERSION,
        "candidate_count": len(rows),
        "min_trades": min_trades,
        "target_pnl": float(getattr(args, "target_pnl", 0.0) or 0.0),
        "estimated_cost_per_trade_to_clear": round(representative_cost_per_trade, 6),
        "adjusted_positive_count": len(adjusted_positive),
        "adjusted_gate_positive_count": len(adjusted_gate_positive),
        "ready_for_live_learning_promotion": bool(adjusted_gate_positive),
        "blockers": blockers,
        "top_execution_adjusted": by_adjusted[:15],
        "top_gross_per_trade": by_density[:15],
        "top_min_trade_execution_adjusted": gate_adjusted[:15],
        "execution_positive_candidates": [
            seed_payload(item)
            for item in adjusted_positive[:50]
            if source_by_variant.get(str(item.get("variant") or "") or "")
        ],
        "cost_assumptions": step2_objective.cost_assumptions(cost_cfg),
    }


def _variant_name(row: dict | None) -> str | None:
    return row.get("variant") if isinstance(row, dict) else None


def _select_candidate_roles(
    *,
    winners: list[dict],
    holdout_winners: list[dict],
    low_sample_positive: list[dict],
    leaderboard: list[dict],
    raw_leaderboard: list[dict],
) -> dict:
    """Separate candidate roles so failed runs do not promote a negative diagnostic row."""
    best_gate_winner = winners[0] if winners else None
    best_holdout_winner = holdout_winners[0] if holdout_winners else None
    best_low_sample_positive = low_sample_positive[0] if low_sample_positive else None
    best_profitable_any = next(
        (row for row in raw_leaderboard if float(row.get("step2_pnl") or 0.0) > 0.0),
        None,
    )
    best_diagnostic_failure = next(
        (row for row in leaderboard if float(row.get("step2_pnl") or 0.0) <= 0.0),
        leaderboard[0] if leaderboard else None,
    )
    selected = best_gate_winner or best_low_sample_positive or best_profitable_any or best_diagnostic_failure
    selected_role = (
        "gate_winner" if best_gate_winner is selected and selected is not None
        else "low_sample_positive" if best_low_sample_positive is selected and selected is not None
        else "raw_profitable" if best_profitable_any is selected and selected is not None
        else "diagnostic_failure" if selected is not None
        else None
    )
    return {
        "selected": selected,
        "selected_role": selected_role,
        "best_gate_winner": best_gate_winner,
        "best_holdout_winner": best_holdout_winner,
        "best_low_sample_positive": best_low_sample_positive,
        "best_profitable_any": best_profitable_any,
        "best_diagnostic_failure": best_diagnostic_failure,
        "briefs": {
            "selected": _row_brief(selected),
            "best_gate_winner": _row_brief(best_gate_winner),
            "best_holdout_winner": _row_brief(best_holdout_winner),
            "best_low_sample_positive": _row_brief(best_low_sample_positive),
            "best_profitable_any": _row_brief(best_profitable_any),
            "best_diagnostic_failure": _row_brief(best_diagnostic_failure),
        },
    }


def _best_distinct(rows: list[dict], *, gate: int = 0, require_positive: bool = False,
                   require_holdout: bool = False) -> dict | None:
    filtered = []
    for row in rows:
        if int(row.get("step2_trades") or 0) < int(gate):
            continue
        if require_positive and float(row.get("step2_pnl") or 0.0) <= 0.0:
            continue
        if require_holdout and not ((row.get("holdout_gate") or {}).get("ok")):
            continue
        filtered.append(row)
    distinct = _dedupe_behavior(sorted(filtered, key=lambda item: float(item.get("profit_hunt_rank_score") or -1e18), reverse=True))
    return distinct[0] if distinct else None


def _frontier_report(rows: list[dict], args: argparse.Namespace) -> dict:
    base_gates = [10, 25, 50, 75, 100, 125, 150, 175, 200, 225, 250, 275, 300, 315, 320, 325, 350, 400, int(args.min_trades)]
    gates = sorted({gate for gate in base_gates if gate > 0})
    points = []
    for gate in gates:
        positive = _best_distinct(rows, gate=gate, require_positive=True)
        holdout = _best_distinct(rows, gate=gate, require_positive=True, require_holdout=True)
        raw = sorted(
            [row for row in rows if int(row.get("step2_trades") or 0) >= gate],
            key=lambda item: float(item.get("step2_pnl") or -1e18),
            reverse=True,
        )
        points.append({
            "gate": gate,
            "best_positive": _row_brief(positive),
            "best_holdout_positive": _row_brief(holdout),
            "raw_best": _row_brief(raw[0] if raw else None),
            "positive_distinct_count": len(_dedupe_behavior([
                row for row in rows
                if int(row.get("step2_trades") or 0) >= gate and float(row.get("step2_pnl") or 0.0) > 0.0
            ])),
            "holdout_positive_distinct_count": len(_dedupe_behavior([
                row for row in rows
                if (
                    int(row.get("step2_trades") or 0) >= gate
                    and float(row.get("step2_pnl") or 0.0) > 0.0
                    and (row.get("holdout_gate") or {}).get("ok")
                )
            ])),
        })
    viable = [item for item in points if item.get("best_holdout_positive")]
    failed = [item for item in points if item["gate"] >= int(args.min_trades) and not item.get("best_holdout_positive")]
    lowest_positive = next((item for item in points if item.get("best_positive")), None)
    lowest_holdout = next((item for item in points if item.get("best_holdout_positive")), None)
    return {
        "gates": points,
        "highest_holdout_positive_gate": viable[-1]["gate"] if viable else None,
        "lowest_positive_gate": lowest_positive["gate"] if lowest_positive else None,
        "lowest_holdout_positive_gate": lowest_holdout["gate"] if lowest_holdout else None,
        "first_failed_gate_at_or_above_requested": failed[0]["gate"] if failed else None,
    }


def _gate_break_diagnosis(rows: list[dict], frontier: dict, args: argparse.Namespace) -> dict:
    current = int(args.min_trades)
    highest = frontier.get("highest_holdout_positive_gate")
    failed = frontier.get("first_failed_gate_at_or_above_requested")
    target_gate = int(failed or current)
    current_gate_passed = bool(highest and int(highest) >= current)
    near_floor = max(1, min(target_gate, int(highest or target_gate)) - 25)
    near_rows = [
        row for row in rows
        if (
            float(row.get("step2_pnl") or 0.0) > 0.0
            and (row.get("holdout_gate") or {}).get("ok")
            and int(row.get("step2_trades") or 0) >= near_floor
        )
    ]
    near_rows = _dedupe_behavior(sorted(
        near_rows,
        key=lambda item: (
            int(item.get("step2_trades") or 0),
            float((item.get("holdout_gate") or {}).get("holdout_pnl") or 0.0),
            float(item.get("step2_pnl") or 0.0),
        ),
        reverse=True,
    ))
    best_below = [row for row in near_rows if int(row.get("step2_trades") or 0) < target_gate]
    closest = best_below[0] if best_below else None
    needed = max(0, target_gate - int((closest or {}).get("step2_trades") or 0)) if closest else None
    lane_counts = Counter(_lane_name(row) for row in near_rows)
    blockers = []
    if failed and current_gate_passed:
        blockers.append("next_higher_trade_gate_has_no_holdout_positive_profit_candidate")
    elif failed:
        blockers.append("requested_trade_gate_has_no_holdout_positive_profit_candidate")
    if closest and needed:
        blockers.append("profitable_frontier_needs_more_positive_incremental_trades")
    if not near_rows:
        blockers.append("no_profitable_holdout_near_gate_rows")
    return {
        "requested_min_trades": current,
        "diagnostic_target_gate": target_gate,
        "requested_gate_passed": current_gate_passed,
        "highest_holdout_positive_gate": highest,
        "first_failed_gate_at_or_above_requested": failed,
        "near_gate_floor": near_floor,
        "near_gate_holdout_positive_count": len(near_rows),
        "closest_profitable_below_requested": _row_brief(closest),
        "additional_trades_needed_for_closest": needed,
        "near_gate_lane_counts": dict(sorted(lane_counts.items())),
        "top_near_gate": [_row_brief(row) for row in near_rows[:10]],
        "blockers": blockers,
        "next_search_directive": (
            "expand_closest_profitable_frontier_with_small_contextual_routes"
            if closest else "rebuild_seed_diversity_at_lower_gate_without_rebuilding_tape"
        ),
    }


def _daily_risk(row: dict | None) -> dict:
    if not isinstance(row, dict):
        return {}
    by_day = row.get("by_day") if isinstance(row.get("by_day"), dict) else {}
    days = [
        {"day": day, "pnl": float((value or {}).get("pnl") or 0.0), "trades": int((value or {}).get("trades") or 0)}
        for day, value in by_day.items()
        if isinstance(value, dict)
    ]
    days.sort(key=lambda item: item["day"])
    total = float(row.get("step2_pnl") or 0.0)
    positive = sorted([item for item in days if item["pnl"] > 0.0], key=lambda item: item["pnl"], reverse=True)
    worst = min(days, key=lambda item: item["pnl"], default=None)
    best = max(days, key=lambda item: item["pnl"], default=None)
    streak = 0
    max_streak = 0
    for item in days:
        if item["pnl"] < 0.0:
            streak += 1
            max_streak = max(max_streak, streak)
        else:
            streak = 0
    top1 = positive[0]["pnl"] if positive else 0.0
    top3 = sum(item["pnl"] for item in positive[:3])
    denominator = abs(total) if abs(total) > 1e-9 else 1.0
    return {
        "best_day": best,
        "worst_day": worst,
        "losing_day_count": sum(1 for item in days if item["pnl"] < 0.0),
        "max_consecutive_losing_days": max_streak,
        "top_1_day_profit_share_pct": round(top1 / denominator * 100.0, 4),
        "top_3_day_profit_share_pct": round(top3 / denominator * 100.0, 4),
        "profit_concentration_warning": bool(total > 0.0 and top3 / denominator > 1.0),
    }


def _concentration_report(row: dict | None) -> dict:
    if not isinstance(row, dict):
        return {}
    def summarize(bucket: dict) -> dict:
        items = []
        for name, value in (bucket or {}).items():
            if isinstance(value, dict):
                items.append({"name": name, "pnl": float(value.get("pnl") or 0.0), "trades": int(value.get("trades") or 0)})
        total_pnl = float(row.get("step2_pnl") or 0.0)
        total_trades = max(1, int(row.get("step2_trades") or 0))
        top_pnl = max(items, key=lambda item: abs(item["pnl"]), default=None)
        top_trades = max(items, key=lambda item: item["trades"], default=None)
        return {
            "items": items,
            "largest_abs_pnl": top_pnl,
            "largest_trade_bucket": top_trades,
            "largest_trade_share_pct": round((top_trades["trades"] / total_trades * 100.0), 4) if top_trades else None,
            "single_bucket_pnl_dominates": bool(top_pnl and total_pnl > 0.0 and abs(top_pnl["pnl"]) / max(abs(total_pnl), 1.0) > 0.75),
        }
    return {
        "ticker": summarize(row.get("by_ticker") if isinstance(row.get("by_ticker"), dict) else {}),
        "side": summarize(row.get("by_side") if isinstance(row.get("by_side"), dict) else {}),
        "daily": _daily_risk(row),
    }


def _risk_concentration_reports(roles: dict) -> dict:
    reports = {}
    for role in (
        "selected",
        "best_gate_winner",
        "best_holdout_winner",
        "best_low_sample_positive",
        "best_profitable_any",
        "best_diagnostic_failure",
    ):
        row = roles.get(role)
        reports[role] = {
            "variant": _variant_name(row),
            "report": _concentration_report(row),
        }
    return reports


def _validation_matrix(row: dict | None) -> dict:
    if not isinstance(row, dict):
        return {"available": False, "reason": "missing_row"}
    by_day = row.get("by_day") if isinstance(row.get("by_day"), dict) else {}
    days = [
        {"day": day, "pnl": float((value or {}).get("pnl") or 0.0), "trades": int((value or {}).get("trades") or 0)}
        for day, value in by_day.items()
        if isinstance(value, dict)
    ]
    days.sort(key=lambda item: item["day"])
    if not days:
        return {"available": False, "reason": "missing_by_day"}
    total = sum(item["pnl"] for item in days)
    positive_days = [item for item in days if item["pnl"] > 0.0]
    holdout_count = max(1, int(round(len(days) * 0.30)))
    in_sample = days[:-holdout_count]
    holdout = days[-holdout_count:]
    leave_one = [
        {"held_out_day": item["day"], "remaining_pnl": round(total - item["pnl"], 6)}
        for item in days
    ]
    leave_one.sort(key=lambda item: item["remaining_pnl"])
    return {
        "available": True,
        "day_count": len(days),
        "positive_day_count": len(positive_days),
        "positive_day_rate_pct": round(len(positive_days) / max(1, len(days)) * 100.0, 4),
        "total_pnl": round(total, 6),
        "in_sample_pnl": round(sum(item["pnl"] for item in in_sample), 6),
        "chronological_holdout_pnl": round(sum(item["pnl"] for item in holdout), 6),
        "worst_leave_one_day_remaining_pnl": leave_one[0] if leave_one else None,
        "best_leave_one_day_remaining_pnl": leave_one[-1] if leave_one else None,
        "leave_one_day_profitable_count": sum(1 for item in leave_one if item["remaining_pnl"] > 0.0),
        "leave_one_day_count": len(leave_one),
    }


def _candidate_validation_reports(roles: dict) -> dict:
    return {
        role: {
            "variant": _variant_name(roles.get(role)),
            "validation": _validation_matrix(roles.get(role)),
        }
        for role in (
            "selected",
            "best_gate_winner",
            "best_holdout_winner",
            "best_low_sample_positive",
            "best_profitable_any",
            "best_diagnostic_failure",
        )
    }


def _data_coverage_report(compiled: dict, manifest: dict) -> dict:
    day_map = compiled.get("day_map") or manifest.get("day_map") or {}
    ticker_map = compiled.get("ticker_map") or manifest.get("ticker_map") or {}
    days = sorted(str(day) for day in day_map.keys())
    tickers = sorted(str(ticker) for ticker in ticker_map.keys())
    fidelity = _market_data_fidelity_report(days, tickers, manifest)
    return {
        "row_count": int(compiled.get("rows") or manifest.get("rows") or 0),
        "day_count": len(days),
        "first_day": days[0] if days else manifest.get("start"),
        "last_day": days[-1] if days else manifest.get("end"),
        "tickers": tickers,
        "ticker_count": len(tickers),
        "quote_aware": True,
        "exit_replay_model": manifest.get("exit_replay_model"),
        "lineage_status": (manifest.get("lineage_validation") or {}).get("status"),
        "market_data_fidelity": fidelity,
        "highest_available_quote_mode": fidelity.get("highest_available_quote_mode"),
        "compiled_quote_mode": fidelity.get("compiled_quote_mode"),
        "full_quote_tick_available_for_scope": fidelity.get("full_quote_tick_available_for_scope"),
        "promotion_data_fidelity_blockers": fidelity.get("promotion_blockers") or [],
        "research_scope_note": "Projected P/L only covers the compiled decision tape date/ticker universe.",
    }


def _infer_compiled_quote_mode(manifest: dict) -> str:
    for value in list(manifest.get("source_paths") or []) + [manifest.get("source_manifest")]:
        raw = str(value or "")
        if "_sip_all_bars_" in raw or "sip_all_bars_" in raw:
            return "all"
        if "_sip_per-second_bars_" in raw or "sip_per-second_bars_" in raw:
            return "per-second"
    store = str(manifest.get("store_dir") or manifest.get("name") or "")
    if "sip_all_bars" in store or "_all_" in store:
        return "all"
    if "sip_per-second_bars" in store or "per-second" in store:
        return "per-second"
    return "unknown"


def _market_data_fidelity_report(days: list[str], tickers: list[str], manifest: dict) -> dict:
    compiled_quote_mode = _infer_compiled_quote_mode(manifest)

    def prepared_path(mode: str, day: str) -> Path:
        tickers_part = "-".join(tickers)
        return HERE / "data_cache" / "alpaca_engine_replay_tapes" / f"sip_{mode}_bars_{tickers_part}_{day}.events.json.gz"

    def raw_quote_path(mode: str, ticker: str, day: str) -> Path:
        folder = "quotes_all" if mode == "all" else "quotes_per-second"
        return HERE / "data_cache" / "alpaca_engine_replay" / "sip" / folder / f"{ticker}_{day}.json.gz"

    def raw_trade_path(ticker: str, day: str) -> Path:
        return HERE / "data_cache" / "alpaca_engine_replay" / "sip" / "trades" / f"{ticker}_{day}.json.gz"

    def decision_tape_path(mode: str, day: str) -> Path:
        tickers_part = "-".join(tickers)
        return HERE / "postmortem" / "backtests" / "decision_tapes" / f"decision_tape_sip_{mode}_bars_live_{tickers_part}_{day}.jsonl.gz"

    quote_modes = {}
    for mode in ("per-second", "all"):
        prepared = [prepared_path(mode, day) for day in days]
        raw_quotes = [raw_quote_path(mode, ticker, day) for day in days for ticker in tickers]
        quote_modes[mode] = {
            "prepared_tape_count": len(prepared),
            "prepared_tape_present_count": sum(1 for path in prepared if path.exists()),
            "prepared_tape_bytes": sum(path.stat().st_size for path in prepared if path.exists()),
            "raw_quote_file_count": len(raw_quotes),
            "raw_quote_file_present_count": sum(1 for path in raw_quotes if path.exists()),
            "raw_quote_bytes": sum(path.stat().st_size for path in raw_quotes if path.exists()),
            "missing_prepared_days": [days[idx] for idx, path in enumerate(prepared) if not path.exists()][:20],
            "missing_raw_quote_files": [
                str(path.relative_to(HERE)) for path in raw_quotes if not path.exists()
            ][:20],
        }
    raw_trades = [raw_trade_path(ticker, day) for day in days for ticker in tickers]
    all_ready = (
        bool(days and tickers)
        and quote_modes["all"]["prepared_tape_present_count"] == quote_modes["all"]["prepared_tape_count"]
        and quote_modes["all"]["raw_quote_file_present_count"] == quote_modes["all"]["raw_quote_file_count"]
    )
    per_second_ready = (
        bool(days and tickers)
        and quote_modes["per-second"]["prepared_tape_present_count"] == quote_modes["per-second"]["prepared_tape_count"]
        and quote_modes["per-second"]["raw_quote_file_present_count"] == quote_modes["per-second"]["raw_quote_file_count"]
    )
    highest = "all" if all_ready else ("per-second" if per_second_ready else "incomplete")
    promotion_blockers = []
    if compiled_quote_mode == "per-second" and not per_second_ready:
        promotion_blockers.append("compiled_scope_missing_per_second_quote_cache")
    if compiled_quote_mode == "all" and not all_ready:
        promotion_blockers.append("compiled_scope_missing_full_quote_tick_cache")
    if compiled_quote_mode not in ("per-second", "all"):
        promotion_blockers.append("compiled_quote_mode_unknown")
    if all_ready and compiled_quote_mode != "all":
        promotion_blockers.append("compiled_not_using_highest_available_quote_fidelity")
    if any(not path.exists() for path in raw_trades):
        promotion_blockers.append("compiled_scope_missing_raw_trade_cache")
    all_decision_tapes = [decision_tape_path("all", day) for day in days]
    all_tape_completed_days = [day for day, path in zip(days, all_decision_tapes) if path.exists()]
    all_tape_missing_days = [day for day, path in zip(days, all_decision_tapes) if not path.exists()]
    resume_start_day = all_tape_missing_days[0] if all_tape_missing_days else None
    resume_command = []
    if resume_start_day and days:
        resume_command = [
            "python",
            "prepare_step2_live_cache.py",
            "--start", resume_start_day,
            "--end", days[-1],
            "--tickers", *tickers,
            "--feed", "sip",
            "--quote-mode", "all",
            "--btc-mode", "bars",
            "--indicator-mode", "live",
            "--workers", "1",
            "--name", f"compiled_step2_current_live_{'-'.join(tickers)}_{days[0]}_{days[-1]}_quote_all_live_profile",
        ]
    all_quote_progress = {
        "mode": "all",
        "decision_tape_count": len(all_decision_tapes),
        "completed_decision_tape_count": len(all_tape_completed_days),
        "missing_decision_tape_count": len(all_tape_missing_days),
        "completed_days": all_tape_completed_days,
        "missing_days": all_tape_missing_days,
        "resume_start_day": resume_start_day,
        "resume_command": resume_command,
        "all_quote_decision_tapes_complete": len(all_decision_tapes) > 0 and not all_tape_missing_days,
        "deduction": (
            "This tracks materialized all-quote daily decision tapes only. Raw full quote-tick tapes can be complete "
            "while the compiled learner cache still needs these daily decision tapes before an all-quote range compile."
        ),
    }
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "compiled_quote_mode": compiled_quote_mode,
        "highest_available_quote_mode": highest,
        "per_second_quote_available_for_scope": per_second_ready,
        "full_quote_tick_available_for_scope": all_ready,
        "day_count": len(days),
        "ticker_count": len(tickers),
        "quote_modes": quote_modes,
        "raw_trade_file_count": len(raw_trades),
        "raw_trade_file_present_count": sum(1 for path in raw_trades if path.exists()),
        "raw_trade_bytes": sum(path.stat().st_size for path in raw_trades if path.exists()),
        "missing_raw_trade_files": [str(path.relative_to(HERE)) for path in raw_trades if not path.exists()][:20],
        "all_quote_decision_tape_progress": all_quote_progress,
        "promotion_blockers": promotion_blockers,
        "ready_for_world_best_certification": not promotion_blockers,
        "deduction": (
            "A world-class learner should certify whether it scored per-second quotes or full quote ticks. "
            "When full quote-tick data exists for the same scope, promotion evidence from a per-second compiled "
            "tape is research-grade until recompiled or cross-validated at full quote fidelity."
        ),
    }


def _true_profit_target_report(rows: list[dict], args: argparse.Namespace) -> dict:
    target_pct = float(getattr(args, "true_profit_target_pct", 30.0) or 0.0)
    start_balance = float(getattr(args, "start_balance", 100000.0) or 100000.0)
    target_pnl = start_balance * target_pct / 100.0
    best = max(rows, key=lambda row: float(row.get("step2_pnl") or -1e18), default=None)
    best_gate = max(
        [row for row in rows if (row.get("min_trade_gate") or {}).get("ok")],
        key=lambda row: float(row.get("step2_pnl") or -1e18),
        default=None,
    )
    best_holdout_gate = max(
        [
            row for row in rows
            if (row.get("min_trade_gate") or {}).get("ok") and (row.get("holdout_gate") or {}).get("ok")
        ],
        key=lambda row: float(row.get("step2_pnl") or -1e18),
        default=None,
    )

    def progress(row: dict | None) -> dict:
        pnl = float((row or {}).get("step2_pnl") or 0.0)
        return {
            "variant": _variant_name(row),
            "pnl": round(pnl, 4),
            "return_pct": round(pnl / start_balance * 100.0, 6) if start_balance else None,
            "progress_to_target_pct": round(pnl / target_pnl * 100.0, 6) if target_pnl else None,
            "remaining_pnl_to_target": round(target_pnl - pnl, 4),
            "trades": int((row or {}).get("step2_trades") or 0),
            "holdout_ok": bool(((row or {}).get("holdout_gate") or {}).get("ok")),
        }

    return {
        "target_return_pct": target_pct,
        "start_balance": start_balance,
        "target_pnl": round(target_pnl, 4),
        "best_raw": progress(best),
        "best_gate": progress(best_gate),
        "best_holdout_gate": progress(best_holdout_gate),
        "target_reached": bool(best_holdout_gate and float(best_holdout_gate.get("step2_pnl") or 0.0) >= target_pnl),
        "deduction": "True-profit target is measured as P/L divided by configured start balance over the compiled tape window.",
    }


def _failure_taxonomy_report(rows: list[dict], args: argparse.Namespace) -> dict:
    min_trades = int(getattr(args, "min_trades", 0) or 0)
    classes: dict[str, dict[str, Any]] = defaultdict(lambda: {"count": 0, "examples": []})

    def add(reason: str, row: dict, detail: dict | None = None) -> None:
        bucket = classes[reason]
        bucket["count"] += 1
        if len(bucket["examples"]) < 10:
            example = _row_brief(row)
            if detail:
                example["detail"] = detail
            bucket["examples"].append(example)

    for row in rows:
        pnl = float(row.get("step2_pnl") or 0.0)
        trades = int(row.get("step2_trades") or 0)
        holdout = _holdout_value(row)
        min_gate_ok = bool((row.get("min_trade_gate") or {}).get("ok"))
        holdout_ok = bool((row.get("holdout_gate") or {}).get("ok"))

        if pnl > 0.0 and not min_gate_ok:
            add("positive_but_low_trade_sample", row, {"min_trades": min_trades, "trades": trades})
        if pnl > 0.0 and min_gate_ok and not holdout_ok:
            add("positive_total_but_holdout_failed", row, {"holdout_pnl": round(holdout, 4)})
        if trades >= max(min_trades, 1) and pnl < 0.0:
            add("trade_eligible_negative_pnl", row, {"pnl": round(pnl, 4), "trades": trades})
        if trades >= max(min_trades * 2, 250) and pnl < -500.0:
            add("high_sample_material_loss", row, {"pnl": round(pnl, 4), "trades": trades})

        validation = _validation_matrix(row)
        if validation.get("available"):
            if int(validation.get("leave_one_day_profitable_count") or 0) < int(validation.get("leave_one_day_count") or 0):
                add("leave_one_day_fragility", row, {
                    "worst_leave_one_day_remaining_pnl": validation.get("worst_leave_one_day_remaining_pnl"),
                })
            if float(validation.get("chronological_holdout_pnl") or 0.0) < 0.0:
                add("chronological_holdout_negative", row, {
                    "chronological_holdout_pnl": validation.get("chronological_holdout_pnl"),
                })

        concentration = _concentration_report(row)
        ticker = (concentration.get("ticker") or {}).get("largest_trade_bucket") or {}
        side = (concentration.get("side") or {}).get("largest_trade_bucket") or {}
        daily = concentration.get("daily") or {}
        if float((concentration.get("ticker") or {}).get("largest_trade_share_pct") or 0.0) >= 65.0:
            add("ticker_concentration_trap", row, ticker)
        if float((concentration.get("side") or {}).get("largest_trade_share_pct") or 0.0) >= 80.0:
            add("side_concentration_trap", row, side)
        if daily.get("profit_concentration_warning"):
            add("one_day_profit_dependency", row, {
                "top_3_day_profit_share_pct": daily.get("top_3_day_profit_share_pct"),
                "best_day": daily.get("best_day"),
            })

    ranked = [
        {
            "failure_class": key,
            "count": value["count"],
            "examples": value["examples"],
            "recommended_repair": {
                "positive_but_low_trade_sample": "expand_nearby_routes_without_changing_winner_core",
                "positive_total_but_holdout_failed": "repair_recent_holdout_before_scaling",
                "trade_eligible_negative_pnl": "reduce_or_retire_behavior_shape",
                "high_sample_material_loss": "suppress_family_until_new_hypothesis",
                "leave_one_day_fragility": "require_day_rotation_sibling_test",
                "chronological_holdout_negative": "prioritize_recent_regime_repair",
                "ticker_concentration_trap": "add_ticker_cap_or_cross_ticker_sibling",
                "side_concentration_trap": "add_side_balance_or_side_specific_gate",
                "one_day_profit_dependency": "run_leave_one_day_control_before_promotion",
            }.get(key, "review_before_next_batch"),
        }
        for key, value in classes.items()
    ]
    ranked.sort(key=lambda item: item["count"], reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "failure_class_count": len(ranked),
        "classes": ranked,
    }


def _next_hunt_command_packet(
    frontier: dict,
    lane_report: dict,
    negative_knowledge: dict,
    true_profit_report: dict,
    failure_taxonomy: dict,
    args: argparse.Namespace,
    false_discovery_pressure: dict | None = None,
    promotion_evidence_gap: dict | None = None,
    data_coverage_report: dict | None = None,
) -> dict:
    guidance = _next_step_guidance(frontier, args)
    lanes = []
    for lane, report in (lane_report or {}).items():
        lanes.append({
            "lane": lane,
            "variant_count": int(report.get("variant_count") or 0),
            "gate_holdout_positive_count": int(report.get("gate_holdout_positive_count") or 0),
            "avg_pnl": float(report.get("avg_pnl") or 0.0),
            "best": report.get("best"),
        })
    lanes.sort(
        key=lambda row: (
            int(row.get("gate_holdout_positive_count") or 0),
            float(row.get("avg_pnl") or 0.0),
            float(((row.get("best") or {}).get("pnl")) or 0.0),
        ),
        reverse=True,
    )
    focus_lanes = [
        row["lane"] for row in lanes
        if int(row.get("gate_holdout_positive_count") or 0) > 0 or float(row.get("avg_pnl") or 0.0) > 0.0
    ][:12]
    caution_lanes = sorted(set((negative_knowledge or {}).get("caution_routes") or []))
    avoid_lanes = sorted(set((negative_knowledge or {}).get("avoid_routes") or []))
    failure_classes = [
        row.get("failure_class")
        for row in (failure_taxonomy or {}).get("classes", [])[:8]
        if row.get("failure_class")
    ]
    best_holdout = true_profit_report.get("best_holdout_gate") if isinstance(true_profit_report, dict) else {}
    progress = float((best_holdout or {}).get("progress_to_target_pct") or 0.0)
    if progress < 1.0:
        target_mode = "discovery_expand_edge"
    elif progress < 10.0:
        target_mode = "compound_promising_frontier"
    else:
        target_mode = "promotion_survival_repair"
    false_mode = str((false_discovery_pressure or {}).get("mode") or "normal")
    promotion_gap_count = int(((promotion_evidence_gap or {}).get("top_candidates") or [{}])[0].get("gap_count") or 0)
    fidelity = (data_coverage_report or {}).get("market_data_fidelity") or {}
    fidelity_blockers = list((data_coverage_report or {}).get("promotion_data_fidelity_blockers") or fidelity.get("promotion_blockers") or [])
    data_fidelity_action_packet = {
        "compiled_quote_mode": fidelity.get("compiled_quote_mode"),
        "highest_available_quote_mode": fidelity.get("highest_available_quote_mode"),
        "full_quote_tick_available_for_scope": bool(fidelity.get("full_quote_tick_available_for_scope")),
        "all_quote_decision_tape_progress": fidelity.get("all_quote_decision_tape_progress") or {},
        "promotion_data_fidelity_blockers": fidelity_blockers,
        "pre_hunt_rebuild_required": "compiled_not_using_highest_available_quote_fidelity" in fidelity_blockers,
        "required_action": (
            "build_and_compare_quote_mode_all_compiled_tape"
            if "compiled_not_using_highest_available_quote_fidelity" in fidelity_blockers
            else "continue_score_only"
        ),
        "resume_command": ((fidelity.get("all_quote_decision_tape_progress") or {}).get("resume_command") or []),
        "deduction": (
            "If full quote-tick data exists for the same scope, the next promotion-grade hunt should score "
            "or at least cross-validate candidates on a quote-mode=all compiled tape before live deployment."
        ),
    }

    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "control_mode": "enforce_next_hunt",
        "no_signal_rebuild": True,
        "no_compiled_tape_rebuild": True,
        "score_only": True,
        "suggested_min_trades": guidance.get("suggested_next_min_trades"),
        "gate_mode": guidance.get("mode"),
        "target_mode": target_mode,
        "false_discovery_mode": false_mode,
        "top_promotion_gap_count": promotion_gap_count,
        "data_fidelity_action_packet": data_fidelity_action_packet,
        "true_profit_target_pct": float(getattr(args, "true_profit_target_pct", 30.0) or 0.0),
        "target_progress_pct": round(progress, 6),
        "focus_lanes": focus_lanes,
        "avoid_lanes": avoid_lanes,
        "caution_lanes": caution_lanes,
        "top_failure_classes": failure_classes,
        "mutation_width": "tight" if avoid_lanes or false_mode != "normal" or "positive_total_but_holdout_failed" in failure_classes else "medium",
        "batch_size_multiplier": 0.75 if false_mode == "strict_backpressure" else (0.85 if avoid_lanes or false_mode == "cautious_backpressure" else 1.0),
        "required_controls": [
            "quote_aware_exit_guard",
            "score_only_no_rebuild_contract",
            "day_rotation_validation",
            "failure_taxonomy_before_next_batch",
            "negative_knowledge_suppression",
            "true_profit_target_progress_report",
            "false_discovery_pressure_check",
            "promotion_evidence_gap_check",
            "market_data_fidelity_check",
        ],
        "next_batch_objectives": [
            "preserve_holdout_positive_frontier",
            "expand_trade_count_without_destroying_holdout",
            "run_sibling_repairs_for_top_failure_classes",
            "avoid_high_severity_negative_knowledge_lanes",
            "report_distance_to_true_profit_target",
        ],
    }


def _route_key_from_payload(route_payload: dict) -> str:
    match = route_payload.get("match") if isinstance(route_payload.get("match"), dict) else {}
    return _route_key(match)


def _route_prior_report(rows: list[dict]) -> dict:
    priors: dict[str, dict[str, Any]] = {}

    def record(key: str, row: dict, route_payload: dict | None = None) -> None:
        item = priors.setdefault(key, {
            "route_key": key,
            "observations": 0,
            "positive_count": 0,
            "negative_count": 0,
            "gate_positive_count": 0,
            "holdout_positive_count": 0,
            "pnl_sum": 0.0,
            "holdout_sum": 0.0,
            "trade_sum": 0,
            "actions": defaultdict(int),
            "examples": [],
        })
        pnl = float(row.get("step2_pnl") or 0.0)
        trades = int(row.get("step2_trades") or 0)
        holdout = _holdout_value(row)
        item["observations"] += 1
        item["positive_count"] += 1 if pnl > 0.0 else 0
        item["negative_count"] += 1 if pnl < 0.0 else 0
        item["gate_positive_count"] += 1 if pnl > 0.0 and (row.get("min_trade_gate") or {}).get("ok") else 0
        item["holdout_positive_count"] += 1 if pnl > 0.0 and (row.get("holdout_gate") or {}).get("ok") else 0
        item["pnl_sum"] += pnl
        item["holdout_sum"] += holdout
        item["trade_sum"] += trades
        if route_payload:
            item["actions"][str(route_payload.get("action") or "score")] += 1
        if len(item["examples"]) < 5:
            item["examples"].append(_row_brief(row))

    for row in rows:
        record(f"lane:{_lane_name(row)}", row)
        seen_routes = set()
        for route_payload in row.get("routes") or []:
            if not isinstance(route_payload, dict):
                continue
            key = f"route:{str(route_payload.get('action') or 'score')}:{_route_key_from_payload(route_payload)}"
            if key in seen_routes:
                continue
            seen_routes.add(key)
            record(key, row, route_payload)

    records = []
    for item in priors.values():
        obs = max(1, int(item["observations"]))
        avg_pnl = item["pnl_sum"] / obs
        avg_holdout = item["holdout_sum"] / obs
        positive_rate = item["positive_count"] / obs
        holdout_rate = item["holdout_positive_count"] / obs
        confidence = min(1.0, obs / 12.0) * max(0.0, positive_rate * 0.55 + holdout_rate * 0.45)
        if item["holdout_positive_count"] >= 2 and avg_pnl > 0.0 and avg_holdout >= 0.0:
            state = "scale_or_repair"
        elif item["negative_count"] >= 3 and avg_pnl < 0.0 and avg_holdout <= 0.0:
            state = "suppress_or_retest_with_control"
        elif obs < 3:
            state = "under_sampled"
        else:
            state = "uncertain_retest"
        records.append({
            "route_key": item["route_key"],
            "observations": item["observations"],
            "positive_count": item["positive_count"],
            "negative_count": item["negative_count"],
            "gate_positive_count": item["gate_positive_count"],
            "holdout_positive_count": item["holdout_positive_count"],
            "avg_pnl": round(avg_pnl, 4),
            "avg_holdout_pnl": round(avg_holdout, 4),
            "avg_trades": round(item["trade_sum"] / obs, 4),
            "positive_rate_pct": round(positive_rate * 100.0, 4),
            "holdout_positive_rate_pct": round(holdout_rate * 100.0, 4),
            "confidence": round(confidence, 6),
            "prior_state": state,
            "actions": dict(sorted(item["actions"].items())),
            "examples": item["examples"],
        })
    records.sort(key=lambda row: (row["confidence"], row["avg_holdout_pnl"], row["avg_pnl"]), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "prior_count": len(records),
        "top_priors": records[:100],
        "focus_routes": [row["route_key"] for row in records if row["prior_state"] == "scale_or_repair"][:20],
        "avoid_routes": [row["route_key"] for row in records if row["prior_state"] == "suppress_or_retest_with_control"][:20],
        "uncertain_routes": [row["route_key"] for row in records if row["prior_state"] in {"under_sampled", "uncertain_retest"}][:20],
    }


def _controlled_sibling_plan(rows: list[dict], route_priors: dict) -> dict:
    candidates = sorted(
        [
            row for row in rows
            if float(row.get("step2_pnl") or 0.0) > 0.0 and (row.get("holdout_gate") or {}).get("ok")
        ],
        key=lambda row: (
            float((row.get("holdout_gate") or {}).get("holdout_pnl") or 0.0),
            float(row.get("step2_pnl") or 0.0),
            int(row.get("step2_trades") or 0),
        ),
        reverse=True,
    )
    avoid = set((route_priors or {}).get("avoid_routes") or [])
    tests = []
    for row in candidates[:12]:
        routes = [route for route in row.get("routes") or [] if isinstance(route, dict)]
        non_skip = [route for route in routes if route.get("action") != "skip"]
        toxic_overlap = [
            f"route:{str(route.get('action') or 'score')}:{_route_key_from_payload(route)}"
            for route in non_skip
            if f"route:{str(route.get('action') or 'score')}:{_route_key_from_payload(route)}" in avoid
        ]
        mutations = [
            "hold_core_constant_add_one_nearby_trade_expander",
            "remove_each_non_skip_route_one_at_a_time",
            "tighten_recent_holdout_sensitive_route_filters",
            "cross_ticker_sibling_same_setup_session",
        ]
        if toxic_overlap:
            mutations.insert(0, "ablate_toxic_overlap_first")
        tests.append({
            "parent_variant": row.get("variant"),
            "parent_behavior_key": _behavior_key(row),
            "parent": _row_brief(row),
            "route_count": len(routes),
            "non_skip_route_count": len(non_skip),
            "toxic_prior_overlap": toxic_overlap,
            "sibling_mutations": mutations,
            "success_criteria": {
                "pnl_positive": True,
                "holdout_positive": True,
                "trades_at_or_above_parent": True,
                "no_new_failure_taxonomy_class": True,
            },
        })
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "parent_count": len(tests),
        "tests": tests,
    }


def _experiment_contract_report(rows: list[dict], failure_taxonomy: dict, route_priors: dict, args: argparse.Namespace) -> dict:
    classes = [row.get("failure_class") for row in (failure_taxonomy or {}).get("classes", []) if row.get("failure_class")]
    focus = list((route_priors or {}).get("focus_routes") or [])
    uncertain = list((route_priors or {}).get("uncertain_routes") or [])
    avoid = list((route_priors or {}).get("avoid_routes") or [])
    contracts = []

    def add(name: str, hypothesis: str, treatment: str, control: str, route_keys: list[str], stop_loss: str) -> None:
        contracts.append({
            "contract_id": f"profit_combo_contract_{len(contracts) + 1:03d}",
            "name": name,
            "hypothesis": hypothesis,
            "treatment": treatment,
            "control": control,
            "route_keys": route_keys[:10],
            "min_trades": int(getattr(args, "min_trades", 0) or 0),
            "success_metrics": [
                "positive_total_pnl",
                "positive_chronological_holdout",
                "leave_one_day_profitable",
                "no_material_concentration_trap",
            ],
            "stop_loss": stop_loss,
        })

    if focus:
        add(
            "scale_holdout_positive_priors",
            "Routes with repeated holdout-positive evidence can be expanded without losing edge.",
            "Generate controlled siblings around top focus routes.",
            "Keep parent route core unchanged and compare against no-extra-route sibling.",
            focus,
            "Stop if holdout turns negative or failure taxonomy adds one_day_profit_dependency.",
        )
    if "positive_but_low_trade_sample" in classes or uncertain:
        add(
            "pay_down_low_sample_uncertainty",
            "Low-sample positive candidates can become useful if trade count is expanded carefully.",
            "Add nearby contextual routes one at a time.",
            "Replay original low-sample parent.",
            uncertain,
            "Stop if added trades are negative on chronological holdout.",
        )
    if avoid:
        add(
            "falsify_negative_priors",
            "High-severity negative priors are truly toxic unless a narrow context rescues them.",
            "Retest avoid routes only inside a narrow controlled context.",
            "Skip broad avoid route families.",
            avoid,
            "Stop after one negative gate-eligible result.",
        )
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "contract_count": len(contracts),
        "contracts": contracts,
    }


def _learning_debt_report(lane_report: dict, route_priors: dict, failure_taxonomy: dict) -> dict:
    debts = []
    for lane, report in (lane_report or {}).items():
        count = int(report.get("variant_count") or 0)
        gate_holdout = int(report.get("gate_holdout_positive_count") or 0)
        avg_pnl = float(report.get("avg_pnl") or 0.0)
        if count >= 25 and gate_holdout == 0:
            debts.append({
                "debt_type": "zero_yield_lane",
                "subject": lane,
                "severity": "high" if avg_pnl < 0.0 else "medium",
                "evidence": {"variant_count": count, "avg_pnl": round(avg_pnl, 4)},
                "paydown_action": "retire_or_require_new_hypothesis_before_more_budget",
            })
        elif 0 < count < 10 and avg_pnl > 0.0:
            debts.append({
                "debt_type": "under_sampled_positive_lane",
                "subject": lane,
                "severity": "medium",
                "evidence": {"variant_count": count, "avg_pnl": round(avg_pnl, 4)},
                "paydown_action": "allocate_small_controlled_expansion_budget",
            })
    for route in (route_priors or {}).get("uncertain_routes") or []:
        debts.append({
            "debt_type": "uncertain_route_prior",
            "subject": route,
            "severity": "low",
            "evidence": {"source": "route_prior_report"},
            "paydown_action": "run_one_controlled_sibling_or_drop_from_focus",
        })
    for item in (failure_taxonomy or {}).get("classes", [])[:5]:
        debts.append({
            "debt_type": "unrepaired_failure_class",
            "subject": item.get("failure_class"),
            "severity": "high" if int(item.get("count") or 0) >= 10 else "medium",
            "evidence": {"count": item.get("count")},
            "paydown_action": item.get("recommended_repair"),
        })
    debts.sort(key=lambda row: ({"high": 2, "medium": 1, "low": 0}.get(row.get("severity"), 0), str(row.get("debt_type"))), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "debt_count": len(debts),
        "debts": debts[:100],
    }


def _variant_dna_report(rows: list[dict]) -> dict:
    genes: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "gene": "",
        "observations": 0,
        "positive_count": 0,
        "holdout_positive_count": 0,
        "pnl_sum": 0.0,
        "holdout_sum": 0.0,
        "examples": [],
    })
    candidate_dna = []

    def row_genes(row: dict) -> list[str]:
        out = [f"lane:{_lane_name(row)}"]
        for route_payload in row.get("routes") or []:
            if not isinstance(route_payload, dict):
                continue
            action = str(route_payload.get("action") or "score")
            route_key = _route_key_from_payload(route_payload)
            out.append(f"route:{action}:{route_key}")
            match = route_payload.get("match") if isinstance(route_payload.get("match"), dict) else {}
            if match.get("ticker"):
                out.append(f"ticker:{match.get('ticker')}")
            if match.get("setup_type"):
                out.append(f"setup:{match.get('setup_type')}")
            if match.get("session_phase"):
                out.append(f"phase:{match.get('session_phase')}")
            if match.get("side"):
                out.append(f"side:{match.get('side')}")
        for ticker, value in ((row.get("by_ticker") or {}).items() if isinstance(row.get("by_ticker"), dict) else []):
            if isinstance(value, dict) and int(value.get("trades") or 0) > 0:
                out.append(f"exposure_ticker:{ticker}")
        for side, value in ((row.get("by_side") or {}).items() if isinstance(row.get("by_side"), dict) else []):
            if isinstance(value, dict) and int(value.get("trades") or 0) > 0:
                out.append(f"exposure_side:{side}")
        return sorted(set(out))

    for row in rows:
        pnl = float(row.get("step2_pnl") or 0.0)
        holdout = _holdout_value(row)
        genes_for_row = row_genes(row)
        if len(candidate_dna) < 50:
            candidate_dna.append({
                "variant": row.get("variant"),
                "dna_hash": tournament_safety.stable_json_hash({"genes": genes_for_row}, length=24),
                "genes": genes_for_row,
                "pnl": round(pnl, 4),
                "holdout_pnl": round(holdout, 4),
                "trades": int(row.get("step2_trades") or 0),
            })
        for gene in genes_for_row:
            item = genes[gene]
            item["gene"] = gene
            item["observations"] += 1
            item["positive_count"] += 1 if pnl > 0.0 else 0
            item["holdout_positive_count"] += 1 if pnl > 0.0 and (row.get("holdout_gate") or {}).get("ok") else 0
            item["pnl_sum"] += pnl
            item["holdout_sum"] += holdout
            if len(item["examples"]) < 5:
                item["examples"].append(_row_brief(row))

    scored = []
    for item in genes.values():
        obs = max(1, int(item["observations"]))
        scored.append({
            "gene": item["gene"],
            "observations": item["observations"],
            "positive_count": item["positive_count"],
            "holdout_positive_count": item["holdout_positive_count"],
            "avg_pnl": round(item["pnl_sum"] / obs, 4),
            "avg_holdout_pnl": round(item["holdout_sum"] / obs, 4),
            "positive_rate_pct": round(item["positive_count"] / obs * 100.0, 4),
            "holdout_positive_rate_pct": round(item["holdout_positive_count"] / obs * 100.0, 4),
            "examples": item["examples"],
        })
    positive = sorted(scored, key=lambda row: (row["holdout_positive_count"], row["avg_holdout_pnl"], row["avg_pnl"]), reverse=True)
    negative = sorted(scored, key=lambda row: (row["avg_holdout_pnl"], row["avg_pnl"]))
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "gene_count": len(scored),
        "top_positive_genes": positive[:50],
        "top_negative_genes": negative[:50],
        "candidate_dna": candidate_dna,
    }


def _family_survival_report(rows: list[dict]) -> dict:
    families: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "family_key": "",
        "variant_count": 0,
        "positive_count": 0,
        "holdout_positive_count": 0,
        "pnl_sum": 0.0,
        "holdout_sum": 0.0,
        "trade_sum": 0,
        "best": None,
    })
    for row in rows:
        route_count = len([route for route in row.get("routes") or [] if isinstance(route, dict)])
        non_skip_count = len([route for route in row.get("routes") or [] if isinstance(route, dict) and route.get("action") != "skip"])
        key = f"{_lane_name(row)}|routes:{route_count}|non_skip:{non_skip_count}"
        item = families[key]
        item["family_key"] = key
        pnl = float(row.get("step2_pnl") or 0.0)
        holdout = _holdout_value(row)
        item["variant_count"] += 1
        item["positive_count"] += 1 if pnl > 0.0 else 0
        item["holdout_positive_count"] += 1 if pnl > 0.0 and (row.get("holdout_gate") or {}).get("ok") else 0
        item["pnl_sum"] += pnl
        item["holdout_sum"] += holdout
        item["trade_sum"] += int(row.get("step2_trades") or 0)
        best = item.get("best")
        if not best or pnl > float(best.get("pnl") or -1e18):
            item["best"] = _row_brief(row)

    out = []
    for item in families.values():
        count = max(1, int(item["variant_count"]))
        holdout_rate = item["holdout_positive_count"] / count
        if item["holdout_positive_count"] >= 2:
            state = "surviving_family"
        elif item["positive_count"] > 0:
            state = "fragile_family"
        else:
            state = "non_survivor"
        out.append({
            "family_key": item["family_key"],
            "variant_count": item["variant_count"],
            "positive_count": item["positive_count"],
            "holdout_positive_count": item["holdout_positive_count"],
            "avg_pnl": round(item["pnl_sum"] / count, 4),
            "avg_holdout_pnl": round(item["holdout_sum"] / count, 4),
            "avg_trades": round(item["trade_sum"] / count, 4),
            "holdout_survival_rate_pct": round(holdout_rate * 100.0, 4),
            "survival_state": state,
            "best": item["best"],
        })
    out.sort(key=lambda row: (row["holdout_positive_count"], row["avg_holdout_pnl"], row["avg_pnl"]), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "family_count": len(out),
        "families": out[:100],
        "surviving_families": [row for row in out if row["survival_state"] == "surviving_family"][:25],
        "non_survivor_families": [row for row in out if row["survival_state"] == "non_survivor"][:25],
    }


def _false_discovery_pressure_report(rows: list[dict], args: argparse.Namespace) -> dict:
    total = len(rows)
    positive = [row for row in rows if float(row.get("step2_pnl") or 0.0) > float(getattr(args, "target_pnl", 0.0) or 0.0)]
    gate_positive = [row for row in positive if (row.get("min_trade_gate") or {}).get("ok")]
    holdout_positive = [row for row in gate_positive if (row.get("holdout_gate") or {}).get("ok")]
    distinct = len(_dedupe_behavior(rows))
    duplicate_rate = 1.0 - (distinct / max(1, total))
    raw_without_holdout = max(0, len(positive) - len(holdout_positive))
    pressure = min(100.0, (total / 500.0) * 25.0 + duplicate_rate * 35.0 + (raw_without_holdout / max(1, len(positive))) * 40.0)
    if pressure >= 70.0:
        mode = "strict_backpressure"
    elif pressure >= 40.0:
        mode = "cautious_backpressure"
    else:
        mode = "normal"
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "tested_variants": total,
        "distinct_behavior_count": distinct,
        "duplicate_behavior_rate_pct": round(duplicate_rate * 100.0, 4),
        "positive_count": len(positive),
        "gate_positive_count": len(gate_positive),
        "holdout_positive_count": len(holdout_positive),
        "raw_positive_without_holdout_count": raw_without_holdout,
        "pressure_score": round(pressure, 4),
        "mode": mode,
        "required_response": (
            "raise_evidence_bar_and_prioritize_controls"
            if mode == "strict_backpressure"
            else "prefer_holdout_and_day_rotation_controls"
            if mode == "cautious_backpressure"
            else "continue_standard_learning_controls"
        ),
    }


def _day_robustness_leaderboard(rows: list[dict]) -> dict:
    scored = []
    for row in rows:
        if float(row.get("step2_pnl") or 0.0) <= 0.0:
            continue
        validation = _validation_matrix(row)
        if not validation.get("available"):
            continue
        day_count = int(validation.get("day_count") or 0)
        loo_count = int(validation.get("leave_one_day_count") or 0)
        loo_positive = int(validation.get("leave_one_day_profitable_count") or 0)
        positive_rate = float(validation.get("positive_day_rate_pct") or 0.0)
        score = positive_rate * 0.5 + (loo_positive / max(1, loo_count)) * 50.0
        scored.append({
            "variant": row.get("variant"),
            "pnl": row.get("step2_pnl"),
            "trades": row.get("step2_trades"),
            "day_count": day_count,
            "positive_day_rate_pct": positive_rate,
            "leave_one_day_profitable_count": loo_positive,
            "leave_one_day_count": loo_count,
            "chronological_holdout_pnl": validation.get("chronological_holdout_pnl"),
            "worst_leave_one_day_remaining_pnl": validation.get("worst_leave_one_day_remaining_pnl"),
            "day_robustness_score": round(score, 4),
        })
    scored.sort(key=lambda row: (row["day_robustness_score"], float(row.get("chronological_holdout_pnl") or 0.0)), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "candidate_count": len(scored),
        "leaderboard": scored[:50],
    }


def _promotion_evidence_gap_report(rows: list[dict], true_profit_report: dict, args: argparse.Namespace) -> dict:
    target_pnl = float((true_profit_report or {}).get("target_pnl") or 0.0)
    candidates = sorted(
        [row for row in rows if float(row.get("step2_pnl") or 0.0) > 0.0],
        key=lambda row: (
            bool((row.get("holdout_gate") or {}).get("ok")),
            float((row.get("holdout_gate") or {}).get("holdout_pnl") or 0.0),
            float(row.get("step2_pnl") or 0.0),
        ),
        reverse=True,
    )
    reports = []
    for row in candidates[:25]:
        gaps = []
        pnl = float(row.get("step2_pnl") or 0.0)
        trades = int(row.get("step2_trades") or 0)
        if target_pnl and pnl < target_pnl:
            gaps.append({"gap": "true_profit_target_shortfall", "remaining_pnl": round(target_pnl - pnl, 4)})
        if trades < int(getattr(args, "min_trades", 0) or 0):
            gaps.append({"gap": "below_min_trade_gate", "trades": trades, "min_trades": int(getattr(args, "min_trades", 0) or 0)})
        if not (row.get("holdout_gate") or {}).get("ok"):
            gaps.append({"gap": "holdout_not_positive", "holdout_pnl": _holdout_value(row)})
        validation = _validation_matrix(row)
        if validation.get("available"):
            if int(validation.get("leave_one_day_profitable_count") or 0) < int(validation.get("leave_one_day_count") or 0):
                gaps.append({"gap": "leave_one_day_not_fully_profitable"})
            if float(validation.get("positive_day_rate_pct") or 0.0) < 55.0:
                gaps.append({"gap": "positive_day_rate_below_55_pct", "positive_day_rate_pct": validation.get("positive_day_rate_pct")})
        concentration = _concentration_report(row)
        ticker_share = float((concentration.get("ticker") or {}).get("largest_trade_share_pct") or 0.0)
        side_share = float((concentration.get("side") or {}).get("largest_trade_share_pct") or 0.0)
        if ticker_share >= 65.0:
            gaps.append({"gap": "ticker_concentration_above_65_pct", "largest_trade_share_pct": round(ticker_share, 4)})
        if side_share >= 80.0:
            gaps.append({"gap": "side_concentration_above_80_pct", "largest_trade_share_pct": round(side_share, 4)})
        reports.append({
            "variant": row.get("variant"),
            "candidate": _row_brief(row),
            "gap_count": len(gaps),
            "gaps": gaps,
            "promotion_evidence_status": "review_ready" if not gaps else "needs_repair",
        })
    reports.sort(key=lambda row: (row["gap_count"], -float((row["candidate"] or {}).get("holdout_pnl") or 0.0)))
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "candidate_count": len(reports),
        "top_candidates": reports[:25],
    }


def _uncertainty_heatmap_report(
    route_priors: dict,
    failure_taxonomy: dict,
    learning_debt: dict,
    promotion_evidence_gap: dict,
) -> dict:
    cells: dict[str, dict[str, Any]] = {}

    def cell(key: str, source: str) -> dict[str, Any]:
        item = cells.setdefault(key, {
            "cell_key": key,
            "sources": set(),
            "signals": [],
            "uncertainty_score": 0.0,
        })
        item["sources"].add(source)
        return item

    for route in (route_priors or {}).get("uncertain_routes") or []:
        item = cell(str(route), "route_prior_uncertain")
        item["uncertainty_score"] += 35.0
        item["signals"].append("route_prior_uncertain")
    for item in (failure_taxonomy or {}).get("classes") or []:
        key = f"failure:{item.get('failure_class')}"
        target = cell(key, "failure_taxonomy")
        target["uncertainty_score"] += min(40.0, float(item.get("count") or 0) * 5.0)
        target["signals"].append("unresolved_failure_class")
    for debt in (learning_debt or {}).get("debts") or []:
        key = f"debt:{debt.get('debt_type')}:{debt.get('subject')}"
        target = cell(key, "learning_debt")
        target["uncertainty_score"] += {"high": 30.0, "medium": 18.0, "low": 10.0}.get(str(debt.get("severity")), 10.0)
        target["signals"].append(str(debt.get("paydown_action") or "pay_down_learning_debt"))
    for report in (promotion_evidence_gap or {}).get("top_candidates") or []:
        for gap in report.get("gaps") or []:
            key = f"promotion_gap:{gap.get('gap')}"
            target = cell(key, "promotion_evidence_gap")
            target["uncertainty_score"] += 12.0
            target["signals"].append(str(report.get("variant")))

    out = []
    for item in cells.values():
        score = min(100.0, float(item["uncertainty_score"]))
        out.append({
            "cell_key": item["cell_key"],
            "uncertainty_score": round(score, 4),
            "sources": sorted(item["sources"]),
            "signals": item["signals"][:10],
            "recommended_action": (
                "test_first_next_batch" if score >= 60.0
                else "allocate_small_probe" if score >= 30.0
                else "monitor"
            ),
        })
    out.sort(key=lambda row: row["uncertainty_score"], reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "cell_count": len(out),
        "cells": out[:100],
    }


def _repair_recipe_report(
    failure_taxonomy: dict,
    promotion_evidence_gap: dict,
    controlled_sibling_plan: dict,
    route_priors: dict,
) -> dict:
    recipes = []

    def add(subject: str, trigger: str, actions: list[str], priority: float, evidence: dict | None = None) -> None:
        recipes.append({
            "recipe_id": f"repair_recipe_{len(recipes) + 1:03d}",
            "subject": subject,
            "trigger": trigger,
            "actions": actions,
            "priority_score": round(priority, 4),
            "evidence": evidence or {},
        })

    for item in (failure_taxonomy or {}).get("classes") or []:
        cls = str(item.get("failure_class") or "unknown")
        repair = str(item.get("recommended_repair") or "review_before_next_batch")
        add(
            cls,
            "failure_taxonomy",
            [repair, "generate_controlled_sibling", "verify_with_day_rotation"],
            float(item.get("count") or 0) * 10.0,
            {"count": item.get("count")},
        )
    for candidate in (promotion_evidence_gap or {}).get("top_candidates") or []:
        if int(candidate.get("gap_count") or 0) <= 0:
            continue
        gap_names = [str(gap.get("gap")) for gap in candidate.get("gaps") or []]
        add(
            str(candidate.get("variant") or "candidate"),
            "promotion_evidence_gap",
            [f"repair:{gap}" for gap in gap_names[:5]] + ["re-score_after_repair"],
            50.0 - min(25.0, float(candidate.get("gap_count") or 0) * 4.0),
            {"gaps": gap_names},
        )
    for test in (controlled_sibling_plan or {}).get("tests") or []:
        add(
            str(test.get("parent_variant") or "sibling_parent"),
            "controlled_sibling_plan",
            list(test.get("sibling_mutations") or [])[:5],
            35.0 + float(test.get("non_skip_route_count") or 0),
            {"parent_behavior_key": test.get("parent_behavior_key")},
        )
    for route in (route_priors or {}).get("avoid_routes") or []:
        add(
            str(route),
            "negative_route_prior",
            ["falsify_in_narrow_context", "avoid_broad_reuse_until_control_passes"],
            45.0,
        )
    recipes.sort(key=lambda row: row["priority_score"], reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "recipe_count": len(recipes),
        "recipes": recipes[:100],
    }


def _search_budget_allocator_report(
    args: argparse.Namespace,
    route_priors: dict,
    false_discovery_pressure: dict,
    learning_debt: dict,
    uncertainty_heatmap: dict,
) -> dict:
    batch_size = int(getattr(args, "batch_size", 0) or 0)
    pressure_mode = str((false_discovery_pressure or {}).get("mode") or "normal")
    high_debt = sum(1 for item in (learning_debt or {}).get("debts") or [] if item.get("severity") == "high")
    top_uncertainty = float((((uncertainty_heatmap or {}).get("cells") or [{}])[0]).get("uncertainty_score") or 0.0)
    focus_count = len((route_priors or {}).get("focus_routes") or [])

    if pressure_mode == "strict_backpressure":
        weights = {
            "controlled_replication": 0.35,
            "promotion_repair": 0.25,
            "uncertainty_paydown": 0.20,
            "new_discovery": 0.10,
            "negative_falsification": 0.10,
        }
    elif high_debt >= 3 or top_uncertainty >= 60.0:
        weights = {
            "controlled_replication": 0.20,
            "promotion_repair": 0.20,
            "uncertainty_paydown": 0.30,
            "new_discovery": 0.20,
            "negative_falsification": 0.10,
        }
    elif focus_count:
        weights = {
            "controlled_replication": 0.20,
            "promotion_repair": 0.20,
            "uncertainty_paydown": 0.15,
            "new_discovery": 0.30,
            "negative_falsification": 0.15,
        }
    else:
        weights = {
            "controlled_replication": 0.15,
            "promotion_repair": 0.15,
            "uncertainty_paydown": 0.20,
            "new_discovery": 0.40,
            "negative_falsification": 0.10,
        }
    allocations = []
    allocated = 0
    items = list(weights.items())
    for idx, (bucket, weight) in enumerate(items):
        count = batch_size - allocated if idx == len(items) - 1 else int(round(batch_size * weight))
        allocated += count
        allocations.append({
            "bucket": bucket,
            "budget_pct": round(weight * 100.0, 4),
            "variant_budget": max(0, count),
        })
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "batch_size": batch_size,
        "pressure_mode": pressure_mode,
        "high_debt_count": high_debt,
        "top_uncertainty_score": round(top_uncertainty, 4),
        "allocations": allocations,
    }


def _belief_update_report(route_priors: dict, variant_dna: dict, family_survival: dict) -> dict:
    beliefs = []
    for row in (route_priors or {}).get("top_priors") or []:
        beliefs.append({
            "belief": f"route_prior:{row.get('route_key')}",
            "posterior_confidence": row.get("confidence"),
            "state": row.get("prior_state"),
            "evidence": {
                "observations": row.get("observations"),
                "avg_pnl": row.get("avg_pnl"),
                "avg_holdout_pnl": row.get("avg_holdout_pnl"),
            },
        })
    for row in (variant_dna or {}).get("top_positive_genes") or []:
        beliefs.append({
            "belief": f"positive_gene:{row.get('gene')}",
            "posterior_confidence": min(1.0, float(row.get("holdout_positive_count") or 0) / max(1.0, float(row.get("observations") or 1))),
            "state": "gene_supports_future_search",
            "evidence": {
                "observations": row.get("observations"),
                "avg_holdout_pnl": row.get("avg_holdout_pnl"),
            },
        })
    for row in (family_survival or {}).get("surviving_families") or []:
        beliefs.append({
            "belief": f"surviving_family:{row.get('family_key')}",
            "posterior_confidence": min(1.0, float(row.get("holdout_survival_rate_pct") or 0.0) / 100.0),
            "state": row.get("survival_state"),
            "evidence": {
                "variant_count": row.get("variant_count"),
                "avg_holdout_pnl": row.get("avg_holdout_pnl"),
            },
        })
    beliefs.sort(key=lambda row: float(row.get("posterior_confidence") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "belief_count": len(beliefs),
        "beliefs": beliefs[:100],
    }


def _learning_quality_scorecard(payload: dict) -> dict:
    checks = [
        ("score_only_contract", bool((payload.get("score_only_contract") or {}).get("score_only"))),
        ("quote_aware_guard", bool((payload.get("quote_aware_guard") or {}).get("ok"))),
        ("true_profit_target_report", bool(payload.get("true_profit_target_report"))),
        ("failure_taxonomy_report", bool(payload.get("failure_taxonomy_report"))),
        ("route_prior_report", bool(payload.get("route_prior_report"))),
        ("experiment_contract_report", bool(payload.get("experiment_contract_report"))),
        ("repair_recipe_report", bool(payload.get("repair_recipe_report"))),
        ("search_budget_allocator_report", bool(payload.get("search_budget_allocator_report"))),
        ("next_hunt_command_packet", bool(payload.get("next_hunt_command_packet"))),
    ]
    passed = sum(1 for _, ok in checks if ok)
    score = round(passed / max(1, len(checks)) * 100.0, 4)
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "learning_quality_score": score,
        "passed_checks": passed,
        "total_checks": len(checks),
        "checks": [{"check": name, "ok": ok} for name, ok in checks],
        "status": "healthy" if score >= 90.0 else "needs_attention",
    }


def _lesson_half_life_report(rows: list[dict], route_priors: dict, family_survival: dict) -> dict:
    all_days = sorted({
        str(day)
        for row in rows
        for day in ((row.get("by_day") or {}).keys() if isinstance(row.get("by_day"), dict) else [])
    })
    latest_day = all_days[-1] if all_days else None
    lessons = []

    for prior in (route_priors or {}).get("top_priors") or []:
        observations = int(prior.get("observations") or 0)
        confidence = float(prior.get("confidence") or 0.0)
        state = str(prior.get("prior_state") or "unknown")
        if state == "scale_or_repair":
            half_life = "fresh" if confidence >= 0.30 else "needs_revalidation"
        elif state == "suppress_or_retest_with_control":
            half_life = "negative_memory_active"
        elif observations < 3:
            half_life = "too_young_to_trust"
        else:
            half_life = "stale_or_uncertain"
        lessons.append({
            "lesson": f"route_prior:{prior.get('route_key')}",
            "subject": prior.get("route_key"),
            "memory_type": "route_prior",
            "half_life_state": half_life,
            "confidence": round(confidence, 6),
            "observations": observations,
            "refresh_action": (
                "controlled_revalidation"
                if half_life in {"needs_revalidation", "stale_or_uncertain", "too_young_to_trust"}
                else "keep_in_memory"
            ),
        })

    for family in (family_survival or {}).get("families") or []:
        state = str(family.get("survival_state") or "")
        lessons.append({
            "lesson": f"family:{family.get('family_key')}",
            "subject": family.get("family_key"),
            "memory_type": "family_survival",
            "half_life_state": "durable" if state == "surviving_family" else ("fragile" if state == "fragile_family" else "decay"),
            "confidence": round(float(family.get("holdout_survival_rate_pct") or 0.0) / 100.0, 6),
            "observations": int(family.get("variant_count") or 0),
            "refresh_action": "expand_siblings" if state == "surviving_family" else "retest_or_retire",
        })

    lessons.sort(key=lambda row: (row["half_life_state"] in {"fresh", "durable", "negative_memory_active"}, row["confidence"]), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "latest_day": latest_day,
        "lesson_count": len(lessons),
        "lessons": lessons[:100],
    }


def _negative_falsification_queue(route_priors: dict, negative_knowledge: dict, failure_taxonomy: dict) -> dict:
    queue = []
    avoid_routes = list((route_priors or {}).get("avoid_routes") or []) + list((negative_knowledge or {}).get("avoid_routes") or [])
    for route in sorted(set(avoid_routes)):
        queue.append({
            "test_id": f"negative_falsification_{len(queue) + 1:03d}",
            "subject": route,
            "hypothesis": "This route/family is genuinely toxic under quote-aware exits.",
            "treatment": "retest only inside narrow controlled context",
            "control": "skip broad route reuse",
            "max_budget_variants": 8,
            "pass_condition": "positive_holdout_with_no_new_failure_class",
            "fail_action": "keep_suppressed",
        })
    for item in (failure_taxonomy or {}).get("classes") or []:
        if str(item.get("failure_class")) in {"high_sample_material_loss", "trade_eligible_negative_pnl"}:
            queue.append({
                "test_id": f"negative_falsification_{len(queue) + 1:03d}",
                "subject": f"failure:{item.get('failure_class')}",
                "hypothesis": "The failure class is structural, not random noise.",
                "treatment": "run one narrow repair sibling",
                "control": "original losing behavior shape",
                "max_budget_variants": 5,
                "pass_condition": "repair sibling beats original and holdout is positive",
                "fail_action": "retire_behavior_shape",
            })
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "test_count": len(queue),
        "queue": queue[:100],
    }


def _promotion_power_report(rows: list[dict], true_profit_report: dict, args: argparse.Namespace) -> dict:
    target_pnl = float((true_profit_report or {}).get("target_pnl") or 0.0)
    start_balance = float(getattr(args, "start_balance", 100000.0) or 100000.0)
    best_holdout = (true_profit_report or {}).get("best_holdout_gate") if isinstance(true_profit_report, dict) else {}
    best_pnl = float((best_holdout or {}).get("pnl") or 0.0)
    remaining = max(0.0, target_pnl - best_pnl)
    positive_holdout = [
        row for row in rows
        if float(row.get("step2_pnl") or 0.0) > 0.0 and (row.get("holdout_gate") or {}).get("ok")
    ]
    avg_holdout = (
        sum(_holdout_value(row) for row in positive_holdout) / len(positive_holdout)
        if positive_holdout else 0.0
    )
    best_day_pnl = 0.0
    for row in positive_holdout:
        by_day = row.get("by_day") if isinstance(row.get("by_day"), dict) else {}
        for value in by_day.values():
            if isinstance(value, dict):
                best_day_pnl = max(best_day_pnl, float(value.get("pnl") or 0.0))
    estimated_batches = None
    if avg_holdout > 0.0:
        estimated_batches = int((remaining / avg_holdout) + (1 if remaining % avg_holdout else 0))
    power_score = min(100.0, (best_pnl / max(target_pnl, 1.0)) * 70.0 + min(30.0, len(positive_holdout) * 2.0))
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "target_pnl": round(target_pnl, 4),
        "target_return_pct": round(target_pnl / start_balance * 100.0, 6) if start_balance else None,
        "best_holdout_pnl": round(best_pnl, 4),
        "remaining_pnl": round(remaining, 4),
        "positive_holdout_candidate_count": len(positive_holdout),
        "avg_positive_holdout_pnl": round(avg_holdout, 4),
        "best_single_day_pnl_observed": round(best_day_pnl, 4),
        "estimated_batches_at_current_avg_holdout": estimated_batches,
        "promotion_power_score": round(power_score, 4),
        "power_state": "far_from_target" if power_score < 20.0 else ("building" if power_score < 60.0 else "credible"),
    }


def _next_best_question_report(
    uncertainty_heatmap: dict,
    repair_recipe: dict,
    promotion_power: dict,
    false_discovery_pressure: dict,
) -> dict:
    questions = []
    for cell in (uncertainty_heatmap or {}).get("cells") or []:
        questions.append({
            "question_id": f"question_{len(questions) + 1:03d}",
            "question": f"Can we resolve {cell.get('cell_key')} with a controlled test?",
            "source": "uncertainty_heatmap",
            "priority_score": float(cell.get("uncertainty_score") or 0.0),
            "recommended_action": cell.get("recommended_action"),
        })
    for recipe in (repair_recipe or {}).get("recipes") or []:
        questions.append({
            "question_id": f"question_{len(questions) + 1:03d}",
            "question": f"Does repair recipe {recipe.get('recipe_id')} improve {recipe.get('subject')} without new failure classes?",
            "source": "repair_recipe",
            "priority_score": float(recipe.get("priority_score") or 0.0),
            "recommended_action": "run_recipe_as_sibling_test",
        })
    if str((promotion_power or {}).get("power_state")) == "far_from_target":
        questions.append({
            "question_id": f"question_{len(questions) + 1:03d}",
            "question": "Which family has enough scalability to close the true-profit target gap?",
            "source": "promotion_power",
            "priority_score": 75.0,
            "recommended_action": "prioritize_family_scalability_tests",
        })
    if str((false_discovery_pressure or {}).get("mode")) != "normal":
        questions.append({
            "question_id": f"question_{len(questions) + 1:03d}",
            "question": "Are current positives robust or search-pressure artifacts?",
            "source": "false_discovery_pressure",
            "priority_score": float((false_discovery_pressure or {}).get("pressure_score") or 0.0),
            "recommended_action": "replicate_before_expanding",
        })
    questions.sort(key=lambda row: row["priority_score"], reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "question_count": len(questions),
        "questions": questions[:100],
        "top_question": questions[0] if questions else None,
    }


def _hunt_governor_report(
    false_discovery_pressure: dict,
    learning_quality: dict,
    promotion_power: dict,
    learning_debt: dict,
    next_best_question: dict,
) -> dict:
    pressure_mode = str((false_discovery_pressure or {}).get("mode") or "normal")
    quality_score = float((learning_quality or {}).get("learning_quality_score") or 0.0)
    power_state = str((promotion_power or {}).get("power_state") or "unknown")
    high_debt = sum(1 for item in (learning_debt or {}).get("debts") or [] if item.get("severity") == "high")
    if quality_score < 90.0:
        decision = "pause_and_repair_learning_artifacts"
    elif pressure_mode == "strict_backpressure":
        decision = "continue_with_replication_backpressure"
    elif high_debt >= 5:
        decision = "continue_with_learning_debt_paydown"
    elif power_state == "far_from_target":
        decision = "continue_with_scalability_discovery"
    else:
        decision = "continue_balanced_hunt"
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "decision": decision,
        "pressure_mode": pressure_mode,
        "learning_quality_score": round(quality_score, 4),
        "promotion_power_state": power_state,
        "high_debt_count": high_debt,
        "top_question": (next_best_question or {}).get("top_question"),
        "guardrails": [
            "score_only_no_rebuild",
            "quote_aware_only",
            "repair_before_scaling_when_backpressure_active",
            "do_not_promote_without_positive_holdout_and_day_rotation",
        ],
    }


def _regime_fingerprint_report(rows: list[dict]) -> dict:
    buckets: dict[str, dict[str, Any]] = {}

    def record(bucket_type: str, bucket: str, row: dict) -> None:
        item = buckets.setdefault(f"{bucket_type}:{bucket}", {
            "bucket_type": bucket_type,
            "bucket": bucket,
            "variant_count": 0,
            "positive_count": 0,
            "holdout_positive_count": 0,
            "pnl_sum": 0.0,
            "trade_sum": 0,
            "examples": [],
        })
        pnl = float(row.get("step2_pnl") or 0.0)
        item["variant_count"] += 1
        item["positive_count"] += 1 if pnl > 0.0 else 0
        item["holdout_positive_count"] += 1 if _holdout_value(row) > 0.0 else 0
        item["pnl_sum"] += pnl
        item["trade_sum"] += int(row.get("step2_trades") or 0)
        if len(item["examples"]) < 3:
            item["examples"].append(row.get("variant"))

    for row in rows:
        record("lane", _lane_name(row), row)
        for day in sorted((row.get("by_day") or {}).keys() if isinstance(row.get("by_day"), dict) else []):
            record("day", str(day), row)
        for ticker in sorted((row.get("by_ticker") or {}).keys() if isinstance(row.get("by_ticker"), dict) else []):
            record("ticker", str(ticker), row)
        for side in sorted((row.get("by_side") or {}).keys() if isinstance(row.get("by_side"), dict) else []):
            record("side", str(side), row)
        for route in row.get("routes") or []:
            match = route.get("match") if isinstance(route, dict) and isinstance(route.get("match"), dict) else {}
            for field in ("session_phase", "setup_type"):
                value = match.get(field)
                if value:
                    record(field, str(value), row)

    fingerprints = []
    for item in buckets.values():
        count = max(1, int(item["variant_count"]))
        avg_pnl = float(item["pnl_sum"]) / count
        holdout_rate = float(item["holdout_positive_count"]) / count * 100.0
        fingerprints.append({
            "bucket_type": item["bucket_type"],
            "bucket": item["bucket"],
            "variant_count": item["variant_count"],
            "positive_count": item["positive_count"],
            "holdout_positive_count": item["holdout_positive_count"],
            "holdout_positive_rate_pct": round(holdout_rate, 4),
            "avg_pnl": round(avg_pnl, 4),
            "avg_trades": round(float(item["trade_sum"]) / count, 4),
            "regime_state": (
                "expand"
                if avg_pnl > 0.0 and holdout_rate >= 40.0
                else ("repair" if avg_pnl > 0.0 else "suppress_or_probe")
            ),
            "examples": item["examples"],
        })
    fingerprints.sort(key=lambda row: (float(row["avg_pnl"]), float(row["holdout_positive_rate_pct"]), int(row["variant_count"])), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "fingerprint_count": len(fingerprints),
        "fingerprints": fingerprints[:150],
    }


def _candidate_lineage_report(rows: list[dict]) -> dict:
    lineage = []
    for row in rows[:500]:
        variant = str(row.get("variant") or "")
        lane = _lane_name(row)
        family = variant
        if "_" in variant:
            parts = variant.split("_")
            family = "_".join(parts[:3]) if len(parts) >= 3 else parts[0]
        route_keys = [
            _route_key_from_payload(route)
            for route in (row.get("routes") or [])
            if isinstance(route, dict)
        ]
        lineage.append({
            "variant": row.get("variant"),
            "lane": lane,
            "family_key": family,
            "behavior_key": _behavior_key(row),
            "route_count": len(route_keys),
            "route_keys": route_keys[:20],
            "pnl": round(float(row.get("step2_pnl") or 0.0), 4),
            "trades": int(row.get("step2_trades") or 0),
            "holdout_pnl": round(_holdout_value(row), 4),
            "lineage_state": (
                "promising_child"
                if float(row.get("step2_pnl") or 0.0) > 0.0 and _holdout_value(row) > 0.0
                else ("needs_repair" if float(row.get("step2_pnl") or 0.0) > 0.0 else "negative_child")
            ),
        })
    lineage.sort(key=lambda row: (float(row["holdout_pnl"]), float(row["pnl"]), int(row["trades"])), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "lineage_count": len(lineage),
        "lineage": lineage[:150],
    }


def _missed_winner_report(rows: list[dict], args: argparse.Namespace) -> dict:
    min_trades = int(getattr(args, "min_trades", 0) or 0)
    missed = []
    for row in rows:
        pnl = float(row.get("step2_pnl") or 0.0)
        trades = int(row.get("step2_trades") or 0)
        holdout = _holdout_value(row)
        gate_ok = bool((row.get("min_trade_gate") or {}).get("ok"))
        reason = None
        if pnl > 0.0 and holdout > 0.0 and not gate_ok:
            reason = "positive_holdout_but_under_trade_gate"
        elif pnl > 0.0 and holdout <= 0.0 and gate_ok:
            reason = "positive_total_but_holdout_negative"
        elif pnl <= 0.0 and holdout > 0.0:
            reason = "holdout_positive_but_total_negative"
        if not reason:
            continue
        missed.append({
            "variant": row.get("variant"),
            "reason": reason,
            "lane": _lane_name(row),
            "pnl": round(pnl, 4),
            "trades": trades,
            "holdout_pnl": round(holdout, 4),
            "trade_gap": max(0, min_trades - trades),
            "recommended_action": (
                "scale_sample_with_same_behavior"
                if reason == "positive_holdout_but_under_trade_gate"
                else ("repair_holdout_rotation" if reason == "positive_total_but_holdout_negative" else "reconcile_total_vs_holdout")
            ),
        })
    missed.sort(key=lambda row: (float(row["holdout_pnl"]), float(row["pnl"]), -int(row["trade_gap"])), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "missed_count": len(missed),
        "missed": missed[:100],
    }


def _search_space_coverage_report(rows: list[dict]) -> dict:
    lane_counts: dict[str, int] = defaultdict(int)
    route_field_values: dict[str, set[str]] = defaultdict(set)
    route_combo_counts: dict[str, int] = defaultdict(int)
    for row in rows:
        lane_counts[_lane_name(row)] += 1
        for route in row.get("routes") or []:
            if not isinstance(route, dict):
                continue
            match = route.get("match") if isinstance(route.get("match"), dict) else {}
            combo = _route_key(match)
            route_combo_counts[combo] += 1
            for field in ("ticker", "setup_type", "session_phase", "side"):
                value = match.get(field)
                if value:
                    route_field_values[field].add(str(value))
    total = max(1, len(rows))
    lanes = [
        {
            "lane": lane,
            "variant_count": count,
            "coverage_pct": round(count / total * 100.0, 4),
            "coverage_state": "over_concentrated" if count / total >= 0.45 else ("thin" if count / total <= 0.05 else "balanced"),
        }
        for lane, count in sorted(lane_counts.items(), key=lambda item: item[1], reverse=True)
    ]
    top_routes = [
        {"route_key": key, "variant_count": count}
        for key, count in sorted(route_combo_counts.items(), key=lambda item: item[1], reverse=True)[:50]
    ]
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "variant_count": len(rows),
        "lane_coverage": lanes,
        "field_coverage": {field: sorted(values) for field, values in route_field_values.items()},
        "unique_route_combo_count": len(route_combo_counts),
        "top_route_combos": top_routes,
        "coverage_state": "needs_more_route_diversity" if len(route_combo_counts) < max(3, len(rows) // 25) else "diverse_enough_for_current_batch",
    }


def _counterfactual_backlog_report(
    missed_winners: dict,
    route_priors: dict,
    repair_recipe: dict,
    regime_fingerprint: dict,
) -> dict:
    backlog = []
    for row in (missed_winners or {}).get("missed") or []:
        backlog.append({
            "counterfactual_id": f"counterfactual_{len(backlog) + 1:03d}",
            "subject": row.get("variant"),
            "question": f"What if {row.get('variant')} kept behavior but fixed {row.get('reason')}?",
            "source": "missed_winner_report",
            "priority_score": abs(float(row.get("holdout_pnl") or 0.0)) + max(0.0, float(row.get("pnl") or 0.0)),
            "required_change": row.get("recommended_action"),
        })
    for prior in (route_priors or {}).get("top_priors") or []:
        if str(prior.get("prior_state")) == "scale_or_repair":
            backlog.append({
                "counterfactual_id": f"counterfactual_{len(backlog) + 1:03d}",
                "subject": prior.get("route_key"),
                "question": "What if this route receives a controlled sibling instead of broad mutation?",
                "source": "route_prior_report",
                "priority_score": float(prior.get("avg_holdout_pnl") or 0.0) + float(prior.get("confidence") or 0.0) * 100.0,
                "required_change": "controlled_route_sibling",
            })
    for recipe in (repair_recipe or {}).get("recipes") or []:
        backlog.append({
            "counterfactual_id": f"counterfactual_{len(backlog) + 1:03d}",
            "subject": recipe.get("subject"),
            "question": f"What if repair recipe {recipe.get('recipe_id')} is isolated from other mutations?",
            "source": "repair_recipe_report",
            "priority_score": float(recipe.get("priority_score") or 0.0),
            "required_change": "single_variable_repair",
        })
    for item in (regime_fingerprint or {}).get("fingerprints") or []:
        if item.get("regime_state") == "expand":
            backlog.append({
                "counterfactual_id": f"counterfactual_{len(backlog) + 1:03d}",
                "subject": f"{item.get('bucket_type')}:{item.get('bucket')}",
                "question": "What if the next batch concentrates a small budget in this favorable regime?",
                "source": "regime_fingerprint_report",
                "priority_score": float(item.get("avg_pnl") or 0.0) + float(item.get("holdout_positive_rate_pct") or 0.0),
                "required_change": "regime_budget_probe",
            })
    backlog.sort(key=lambda row: float(row.get("priority_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "counterfactual_count": len(backlog),
        "backlog": backlog[:100],
    }


def _artifact_trust_report(artifacts: dict) -> dict:
    required = {
        "route_prior_report": "route priors",
        "failure_taxonomy_report": "failure classes",
        "promotion_evidence_gap_report": "promotion evidence gaps",
        "search_budget_allocator_report": "next batch budgets",
        "hunt_governor_report": "hunt governor",
        "regime_fingerprint_report": "regime fingerprints",
        "candidate_lineage_report": "candidate lineage",
        "counterfactual_backlog_report": "counterfactual backlog",
        "missed_winner_report": "missed winners",
        "search_space_coverage_report": "search coverage",
        "pairwise_route_ablation_report": "pairwise route interactions",
        "triple_route_ablation_report": "triple route interactions",
        "shapley_route_contribution_estimates": "route contribution estimates",
        "causal_ablation_proof_report": "actual causal ablation proof ledger",
        "proof_artifact_manifest": "tamper-evident external proof artifact manifest",
        "proof_freshness_report": "external proof freshness and expiry report",
        "proof_regime_match_report": "external proof regime compatibility report",
        "fill_quality_proof_report": "candidate-bound fill quality proof",
        "candidate_proof_dossier": "selected candidate proof dossier",
        "genetic_search_operator": "genetic search operator",
        "bandit_lane_allocator": "bandit lane allocator",
        "beam_search_route_set_optimizer": "beam search route optimizer",
        "slippage_sensitivity_sweep": "slippage sensitivity",
        "spread_sensitivity_sweep": "spread sensitivity",
        "worker_pool_parallel_batch_runner": "parallel runner contract",
        "opportunity_feature_learner_report": "indicator feature learner",
        "opportunity_target_model_report": "opportunity target model",
        "opportunity_rule_extraction_report": "indicator rule extractor",
        "variant_meta_model_report": "variant meta-model",
        "uncertainty_model_report": "uncertainty model",
        "teacher_student_learning_packet": "teacher-student learning packet",
        "walk_forward_validation_bundle": "walk-forward validation bundle",
        "historical_learning_protocol": "frozen historical split protocol",
        "oos_profitability_report": "out-of-sample profitability proof",
        "promotion_ladder_report": "forward promotion ladder proof",
        "benchmark_superiority_report": "external baseline superiority proof",
    }
    checks = []
    for key, label in required.items():
        value = artifacts.get(key)
        ok = bool(value)
        checks.append({
            "artifact": key,
            "label": label,
            "ok": ok,
            "trust_state": "trusted" if ok else "missing",
        })
    ok_count = sum(1 for item in checks if item["ok"])
    score = round(ok_count / max(1, len(checks)) * 100.0, 4)
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "trust_score": score,
        "trusted_count": ok_count,
        "required_count": len(checks),
        "checks": checks,
        "trust_state": "trusted" if score >= 95.0 else ("usable_with_gaps" if score >= 75.0 else "needs_repair"),
    }


def _experiment_sequencer_report(
    counterfactual_backlog: dict,
    next_best_question: dict,
    search_budget_allocator: dict,
    hunt_governor: dict,
    args: argparse.Namespace,
) -> dict:
    batch_size = int(getattr(args, "batch_size", 0) or (search_budget_allocator or {}).get("batch_size") or 0)
    items = []
    for row in (counterfactual_backlog or {}).get("backlog") or []:
        items.append({
            "source": "counterfactual_backlog",
            "subject": row.get("subject"),
            "question": row.get("question"),
            "priority_score": float(row.get("priority_score") or 0.0),
            "required_change": row.get("required_change"),
        })
    for row in (next_best_question or {}).get("questions") or []:
        items.append({
            "source": "next_best_question",
            "subject": row.get("question_id"),
            "question": row.get("question"),
            "priority_score": float(row.get("priority_score") or 0.0),
            "required_change": row.get("recommended_action"),
        })
    items.sort(key=lambda row: float(row.get("priority_score") or 0.0), reverse=True)

    sequence = []
    remaining = max(0, batch_size)
    for idx, row in enumerate(items[:12], start=1):
        phase = (
            "repair"
            if str(row.get("required_change") or "").startswith("repair") or row.get("source") == "repair_recipe_report"
            else ("replicate" if "robust" in str(row.get("question") or "").lower() else "discover")
        )
        budget = min(remaining, max(5, min(40, int(round(batch_size * (0.08 if idx <= 3 else 0.04))))))
        remaining -= budget
        sequence.append({
            "order": idx,
            "phase": phase,
            "subject": row.get("subject"),
            "question": row.get("question"),
            "source": row.get("source"),
            "variant_budget": budget,
            "success_gate": "positive_holdout_and_no_new_failure_class",
        })
        if remaining <= 0:
            break
    if remaining > 0 and sequence:
        sequence[0]["variant_budget"] += remaining
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "governor_decision": (hunt_governor or {}).get("decision"),
        "batch_size": batch_size,
        "sequence_count": len(sequence),
        "sequence": sequence,
    }


def _route_overlap_matrix(rows: list[dict]) -> dict:
    candidates = []
    ranked = sorted(
        rows,
        key=lambda row: (_holdout_value(row), float(row.get("step2_pnl") or 0.0), int(row.get("step2_trades") or 0)),
        reverse=True,
    )
    for row in ranked[:30]:
        route_keys = {
            _route_key_from_payload(route)
            for route in (row.get("routes") or [])
            if isinstance(route, dict)
        }
        if route_keys:
            candidates.append({
                "variant": row.get("variant"),
                "lane": _lane_name(row),
                "route_keys": route_keys,
                "pnl": float(row.get("step2_pnl") or 0.0),
                "holdout_pnl": _holdout_value(row),
            })
    pairs = []
    for i, left in enumerate(candidates):
        for right in candidates[i + 1:]:
            union = left["route_keys"] | right["route_keys"]
            if not union:
                continue
            overlap = len(left["route_keys"] & right["route_keys"]) / len(union)
            if overlap <= 0.0:
                continue
            pairs.append({
                "left": left["variant"],
                "right": right["variant"],
                "left_lane": left["lane"],
                "right_lane": right["lane"],
                "overlap_pct": round(overlap * 100.0, 4),
                "shared_route_count": len(left["route_keys"] & right["route_keys"]),
                "overlap_state": "crowded" if overlap >= 0.75 else ("related" if overlap >= 0.35 else "light"),
            })
    pairs.sort(key=lambda row: float(row["overlap_pct"]), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "candidate_count": len(candidates),
        "pair_count": len(pairs),
        "crowded_pair_count": sum(1 for row in pairs if row["overlap_state"] == "crowded"),
        "pairs": pairs[:100],
    }


def _sample_efficiency_report(rows: list[dict], args: argparse.Namespace) -> dict:
    min_trades = int(getattr(args, "min_trades", 0) or 0)
    top = []
    bands: dict[str, dict[str, Any]] = {}

    def band_for(trades: int) -> str:
        if min_trades and trades < min_trades:
            return "under_gate"
        if min_trades and trades < min_trades * 2:
            return "gate_to_2x"
        return "large_sample"

    for row in rows:
        trades = int(row.get("step2_trades") or 0)
        if trades <= 0:
            continue
        pnl = float(row.get("step2_pnl") or 0.0)
        holdout = _holdout_value(row)
        band = band_for(trades)
        item = bands.setdefault(band, {"variant_count": 0, "pnl_per_trade_sum": 0.0, "holdout_per_trade_sum": 0.0, "positive_count": 0})
        item["variant_count"] += 1
        item["pnl_per_trade_sum"] += pnl / trades
        item["holdout_per_trade_sum"] += holdout / trades
        item["positive_count"] += 1 if pnl > 0.0 else 0
        if pnl > 0.0 or holdout > 0.0:
            top.append({
                "variant": row.get("variant"),
                "lane": _lane_name(row),
                "trades": trades,
                "pnl": round(pnl, 4),
                "holdout_pnl": round(holdout, 4),
                "pnl_per_trade": round(pnl / trades, 6),
                "holdout_pnl_per_trade": round(holdout / trades, 6),
            })
    top.sort(key=lambda row: (float(row["holdout_pnl_per_trade"]), float(row["pnl_per_trade"])), reverse=True)
    band_summary = []
    for band, item in sorted(bands.items()):
        count = max(1, int(item["variant_count"]))
        band_summary.append({
            "band": band,
            "variant_count": item["variant_count"],
            "positive_rate_pct": round(float(item["positive_count"]) / count * 100.0, 4),
            "avg_pnl_per_trade": round(float(item["pnl_per_trade_sum"]) / count, 6),
            "avg_holdout_pnl_per_trade": round(float(item["holdout_per_trade_sum"]) / count, 6),
        })
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "top_efficiency_count": len(top),
        "top_efficiency": top[:100],
        "band_summary": band_summary,
    }


def _stress_test_queue(rows: list[dict], regime_fingerprint: dict, promotion_evidence_gap: dict) -> dict:
    tests = []
    ranked = sorted(rows, key=lambda row: (_holdout_value(row), float(row.get("step2_pnl") or 0.0)), reverse=True)
    for row in ranked[:20]:
        pnl = float(row.get("step2_pnl") or 0.0)
        holdout = _holdout_value(row)
        if pnl <= 0.0 and holdout <= 0.0:
            continue
        by_day = row.get("by_day") if isinstance(row.get("by_day"), dict) else {}
        by_ticker = row.get("by_ticker") if isinstance(row.get("by_ticker"), dict) else {}
        if len(by_day) >= 2:
            tests.append({
                "test_id": f"stress_{len(tests) + 1:03d}",
                "variant": row.get("variant"),
                "stress_type": "leave_worst_day_out_and_recheck",
                "reason": "candidate depends on day rotation robustness",
                "min_pass_condition": "remaining_days_positive_and_holdout_positive",
            })
        if len(by_ticker) >= 2:
            tests.append({
                "test_id": f"stress_{len(tests) + 1:03d}",
                "variant": row.get("variant"),
                "stress_type": "leave_best_ticker_out_and_recheck",
                "reason": "candidate may be ticker-concentrated",
                "min_pass_condition": "non_best_ticker_slice_not_materially_negative",
            })
    for item in (regime_fingerprint or {}).get("fingerprints") or []:
        if item.get("regime_state") == "expand" and int(item.get("variant_count") or 0) < 3:
            tests.append({
                "test_id": f"stress_{len(tests) + 1:03d}",
                "variant": f"{item.get('bucket_type')}:{item.get('bucket')}",
                "stress_type": "regime_sample_expansion",
                "reason": "favorable regime has thin supporting sample",
                "min_pass_condition": "expanded_sample_keeps_positive_holdout_rate",
            })
    for gap in (promotion_evidence_gap or {}).get("top_candidates") or []:
        for missing in gap.get("gaps") or []:
            tests.append({
                "test_id": f"stress_{len(tests) + 1:03d}",
                "variant": gap.get("variant"),
                "stress_type": f"promotion_gap:{missing}",
                "reason": "promotion evidence incomplete",
                "min_pass_condition": "gap_resolved_without_lowering_holdout_pnl",
            })
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "test_count": len(tests),
        "tests": tests[:100],
    }


def _champion_challenger_report(rows: list[dict], active: dict, args: argparse.Namespace) -> dict:
    active_pnl = float((active or {}).get("step2_pnl") or 0.0)
    active_trades = int((active or {}).get("step2_trades") or 0)
    min_trades = int(getattr(args, "min_trades", 0) or 0)
    challengers = []
    for row in rows:
        pnl = float(row.get("step2_pnl") or 0.0)
        trades = int(row.get("step2_trades") or 0)
        holdout = _holdout_value(row)
        if pnl <= active_pnl and holdout <= 0.0:
            continue
        challengers.append({
            "variant": row.get("variant"),
            "family_key": row.get("family_key") or _lane_name(row),
            "lineage_id": (row.get("lineage") or {}).get("family_key") if isinstance(row.get("lineage"), dict) else row.get("parent_variant"),
            "lane": _lane_name(row),
            "pnl": round(pnl, 4),
            "active_delta_pnl": round(pnl - active_pnl, 4),
            "trades": trades,
            "active_delta_trades": trades - active_trades,
            "holdout_pnl": round(holdout, 4),
            "challenge_state": (
                "promotion_challenger"
                if pnl > active_pnl and holdout > 0.0 and trades >= min_trades
                else ("research_challenger" if pnl > active_pnl or holdout > 0.0 else "watch")
            ),
        })
    challengers.sort(key=lambda row: (float(row["holdout_pnl"]), float(row["active_delta_pnl"]), int(row["trades"])), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "active_pnl": round(active_pnl, 4),
        "active_trades": active_trades,
        "challenger_count": len(challengers),
        "promotion_challenger_count": sum(1 for row in challengers if row["challenge_state"] == "promotion_challenger"),
        "challengers": challengers[:100],
    }


def _learning_ops_readiness_report(artifact_trust: dict, hunt_governor: dict, search_space_coverage: dict, stress_queue: dict) -> dict:
    blockers = []
    if str((artifact_trust or {}).get("trust_state")) != "trusted":
        blockers.append("artifact_trust_not_full")
    if str((hunt_governor or {}).get("decision")) == "pause_and_repair_learning_artifacts":
        blockers.append("hunt_governor_requests_learning_repair")
    if str((search_space_coverage or {}).get("coverage_state")) == "needs_more_route_diversity":
        blockers.append("search_space_needs_more_route_diversity")
    if int((stress_queue or {}).get("test_count") or 0) <= 0:
        blockers.append("no_stress_tests_generated")
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "ready_for_next_batch": not blockers,
        "blockers": blockers,
        "artifact_trust_state": (artifact_trust or {}).get("trust_state"),
        "hunt_governor_decision": (hunt_governor or {}).get("decision"),
        "coverage_state": (search_space_coverage or {}).get("coverage_state"),
        "stress_test_count": int((stress_queue or {}).get("test_count") or 0),
    }


def _route_interaction_report(rows: list[dict], size: int, label: str) -> dict:
    combos: dict[tuple[str, ...], dict[str, Any]] = {}
    for row in rows:
        route_keys = sorted({
            _route_key_from_payload(route)
            for route in (row.get("routes") or [])
            if isinstance(route, dict)
        })
        if len(route_keys) < size:
            continue
        for i, first in enumerate(route_keys):
            if size == 2:
                iterable = [(first, second) for second in route_keys[i + 1:]]
            else:
                iterable = [
                    (first, second, third)
                    for j, second in enumerate(route_keys[i + 1:], start=i + 1)
                    for third in route_keys[j + 1:]
                ]
            for combo in iterable:
                item = combos.setdefault(tuple(combo), {
                    "route_set": list(combo),
                    "observations": 0,
                    "positive_count": 0,
                    "holdout_positive_count": 0,
                    "pnl_sum": 0.0,
                    "holdout_sum": 0.0,
                    "trade_sum": 0,
                    "examples": [],
                })
                pnl = float(row.get("step2_pnl") or 0.0)
                holdout = _holdout_value(row)
                item["observations"] += 1
                item["positive_count"] += 1 if pnl > 0.0 else 0
                item["holdout_positive_count"] += 1 if holdout > 0.0 else 0
                item["pnl_sum"] += pnl
                item["holdout_sum"] += holdout
                item["trade_sum"] += int(row.get("step2_trades") or 0)
                if len(item["examples"]) < 3:
                    item["examples"].append(row.get("variant"))
    interactions = []
    for item in combos.values():
        count = max(1, int(item["observations"]))
        avg_pnl = float(item["pnl_sum"]) / count
        avg_holdout = float(item["holdout_sum"]) / count
        interactions.append({
            "route_set": item["route_set"],
            "observations": item["observations"],
            "positive_count": item["positive_count"],
            "holdout_positive_count": item["holdout_positive_count"],
            "avg_pnl": round(avg_pnl, 4),
            "avg_holdout_pnl": round(avg_holdout, 4),
            "avg_trades": round(float(item["trade_sum"]) / count, 4),
            "interaction_state": (
                "promising_interaction"
                if avg_pnl > 0.0 and avg_holdout > 0.0
                else ("repair_interaction" if avg_pnl > 0.0 else "avoid_or_retest")
            ),
            "examples": item["examples"],
        })
    interactions.sort(key=lambda row: (float(row["avg_holdout_pnl"]), float(row["avg_pnl"]), int(row["observations"])), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "interaction_size": size,
        "report": label,
        "interaction_count": len(interactions),
        "interactions": interactions[:100],
        "score_only_note": "Derived from observed routed rows; does not rescore removed-route counterfactuals.",
    }


def _shapley_route_contribution_report(rows: list[dict]) -> dict:
    contributions: dict[str, dict[str, Any]] = {}
    for row in rows:
        route_keys = sorted({
            _route_key_from_payload(route)
            for route in (row.get("routes") or [])
            if isinstance(route, dict)
        })
        if not route_keys:
            continue
        share = 1.0 / len(route_keys)
        pnl_credit = float(row.get("step2_pnl") or 0.0) * share
        holdout_credit = _holdout_value(row) * share
        trade_credit = int(row.get("step2_trades") or 0) * share
        for key in route_keys:
            item = contributions.setdefault(key, {
                "route_key": key,
                "observations": 0,
                "positive_credit": 0.0,
                "pnl_credit": 0.0,
                "holdout_credit": 0.0,
                "trade_credit": 0.0,
                "examples": [],
            })
            item["observations"] += 1
            item["positive_credit"] += share if float(row.get("step2_pnl") or 0.0) > 0.0 else 0.0
            item["pnl_credit"] += pnl_credit
            item["holdout_credit"] += holdout_credit
            item["trade_credit"] += trade_credit
            if len(item["examples"]) < 3:
                item["examples"].append(row.get("variant"))
    rows_out = []
    for item in contributions.values():
        obs = max(1, int(item["observations"]))
        rows_out.append({
            "route_key": item["route_key"],
            "observations": item["observations"],
            "avg_pnl_credit": round(float(item["pnl_credit"]) / obs, 4),
            "avg_holdout_credit": round(float(item["holdout_credit"]) / obs, 4),
            "avg_trade_credit": round(float(item["trade_credit"]) / obs, 4),
            "positive_credit": round(float(item["positive_credit"]), 4),
            "contribution_state": (
                "scale"
                if float(item["pnl_credit"]) > 0.0 and float(item["holdout_credit"]) > 0.0
                else ("repair" if float(item["pnl_credit"]) > 0.0 else "suppress")
            ),
            "examples": item["examples"],
        })
    rows_out.sort(key=lambda row: (float(row["avg_holdout_credit"]), float(row["avg_pnl_credit"]), int(row["observations"])), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "route_count": len(rows_out),
        "routes": rows_out[:150],
        "approximation": "equal_credit_observed_route_set_proxy",
    }


def _causal_ablation_proof_report(
    pairwise_report: dict,
    triple_report: dict,
    shapley_report: dict,
    actual_ablation_tests: list[dict] | None = None,
) -> dict:
    actual_tests = []
    for test in actual_ablation_tests or []:
        if isinstance(test, dict):
            actual_tests.append({**test, "source": test.get("source") or "external_actual_ablation_artifact"})
    for source, report in (
        ("pairwise_route_ablation_report", pairwise_report),
        ("triple_route_ablation_report", triple_report),
        ("shapley_route_contribution_estimates", shapley_report),
    ):
        for test in (report or {}).get("actual_ablation_tests") or []:
            if isinstance(test, dict):
                actual_tests.append({**test, "source": source})

    observed_candidates = []
    for report in (pairwise_report, triple_report):
        for item in (report or {}).get("interactions") or []:
            if not isinstance(item, dict):
                continue
            if float(item.get("avg_pnl") or 0.0) > 0.0 and float(item.get("avg_holdout_pnl") or 0.0) > 0.0:
                observed_candidates.append({
                    "route_set": item.get("route_set"),
                    "observations": int(item.get("observations") or 0),
                    "avg_pnl": float(item.get("avg_pnl") or 0.0),
                    "avg_holdout_pnl": float(item.get("avg_holdout_pnl") or 0.0),
                    "proof_state": "candidate_requires_removed_route_rescore",
                })
    isolated_candidates = [
        {
            "route_key": item.get("route_key"),
            "observations": int(item.get("observations") or 0),
            "avg_pnl_credit": float(item.get("avg_pnl_credit") or 0.0),
            "avg_holdout_credit": float(item.get("avg_holdout_credit") or 0.0),
            "proof_state": "candidate_requires_ab_test",
        }
        for item in (shapley_report or {}).get("routes") or []
        if isinstance(item, dict)
        and float(item.get("avg_pnl_credit") or 0.0) > 0.0
        and float(item.get("avg_holdout_credit") or 0.0) > 0.0
    ]

    passed = [
        test for test in actual_tests
        if test.get("passed") is True and float(test.get("lift_after_costs") or test.get("lift") or 0.0) > 0.0
    ]
    failed = [test for test in actual_tests if test.get("passed") is False]
    isolated = [test for test in passed if test.get("isolated_lift") is True]
    blockers = []
    if len(passed) < 10:
        blockers.append("fewer_than_10_passed_actual_ablations")
    if len(failed) > max(0, int(len(passed) * 0.35)):
        blockers.append("failed_ablation_rate_too_high")
    if len(isolated) < 3:
        blockers.append("fewer_than_3_isolated_lift_proofs")
    if not actual_tests:
        blockers.append("missing_removed_route_counterfactual_rescores")

    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "actual_ablation_count": len(actual_tests),
        "passed_ablation_count": len(passed),
        "failed_ablation_count": len(failed),
        "isolated_lift_count": len(isolated),
        "observed_candidate_count": len(observed_candidates),
        "isolated_candidate_count": len(isolated_candidates),
        "observed_candidates": observed_candidates[:50],
        "isolated_candidates": isolated_candidates[:50],
        "ready_for_world_best_certification": not blockers,
        "blockers": blockers,
        "deduction": (
            "Observed positive route interactions are treated as hypotheses, not causal proof. Promotion-grade "
            "causality requires actual removed-route counterfactual rescores or A/B ablations with positive "
            "lift after costs."
        ),
    }


def _promotion_ladder_report(
    historical_protocol: dict,
    oos_report: dict,
    champion_challenger: dict,
    live_feedback: dict | None = None,
) -> dict:
    live_feedback = live_feedback or {}
    stage_results = live_feedback.get("promotion_stages") if isinstance(live_feedback.get("promotion_stages"), dict) else {}
    candidate_identity = _promotion_candidate_identity(champion_challenger)
    candidate_variant = candidate_identity.get("variant")
    historical_passed = bool(
        (historical_protocol or {}).get("ready_for_world_best_certification") is True
        and (oos_report or {}).get("ready_for_world_best_certification") is True
    )

    def _stage(name: str) -> dict:
        payload = stage_results.get(name) if isinstance(stage_results.get(name), dict) else {}
        stage_variant = payload.get("variant") or payload.get("candidate_variant")
        passed = payload.get("passed") is True
        return {
            "passed": passed,
            "candidate_variant": stage_variant,
            "family_key": payload.get("family_key"),
            "lineage_id": payload.get("lineage_id"),
            "candidate_bound": _candidate_bound(candidate_identity, payload),
            "trade_count": int(payload.get("trade_count") or 0),
            "net_pnl": round(float(payload.get("net_pnl") or 0.0), 4),
            "source": payload.get("source") or "missing_forward_stage_evidence",
        }

    stages = {
        "historical": {
            "passed": historical_passed,
            "candidate_variant": candidate_variant,
            "family_key": candidate_identity.get("family_key"),
            "lineage_id": candidate_identity.get("lineage_id"),
            "candidate_bound": bool(candidate_variant or candidate_identity.get("family_key") or candidate_identity.get("lineage_id")),
            "trade_count": int((oos_report or {}).get("min_test_trades") or 0),
            "net_pnl": round(float((oos_report or {}).get("net_oos_pnl") or 0.0), 4),
            "source": "historical_learning_protocol_plus_oos_profitability_report",
        },
        "shadow": _stage("shadow"),
        "paper": _stage("paper"),
        "tiny_live": _stage("tiny_live"),
    }
    forward_trade_count = sum(int(stages[name]["trade_count"] or 0) for name in ("shadow", "paper", "tiny_live"))
    forward_net_pnl = sum(float(stages[name]["net_pnl"] or 0.0) for name in ("shadow", "paper", "tiny_live"))
    blockers = []
    for name, stage in stages.items():
        if stage.get("passed") is not True:
            blockers.append(f"{name}_stage_not_passed")
    if forward_trade_count < 100:
        blockers.append("forward_trade_count_below_100")
    if forward_net_pnl <= 0.0:
        blockers.append("forward_net_pnl_not_positive")
    if int((champion_challenger or {}).get("promotion_challenger_count") or 0) <= 0:
        blockers.append("missing_champion_challenger_candidates")
    if not candidate_variant:
        blockers.append("missing_candidate_identity_binding")
    for name, stage in stages.items():
        if stage.get("candidate_bound") is not True:
            blockers.append(f"{name}_stage_not_bound_to_candidate")

    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "candidate_variant": candidate_variant,
        "stages": stages,
        "forward_trade_count": forward_trade_count,
        "forward_net_pnl": round(forward_net_pnl, 4),
        "ready_for_world_best_certification": not blockers,
        "blockers": blockers,
        "deduction": (
            "Historical profit is only the first rung. Promotion requires the same candidate to survive "
            "shadow, paper, and tiny-live forward stages before it can be treated as deployable learning."
        ),
    }


def _benchmark_superiority_report(
    rows: list[dict],
    active: dict,
    external_benchmarks: list[dict] | None = None,
    candidate_variant: str | None = None,
) -> dict:
    external_benchmarks = [row for row in (external_benchmarks or []) if isinstance(row, dict)]
    candidate_rows = [row for row in rows or [] if isinstance(row, dict)]
    if candidate_variant:
        candidate_rows = [row for row in candidate_rows if row.get("variant") == candidate_variant]
    candidates = sorted(
        candidate_rows,
        key=lambda row: (float(row.get("step2_pnl") or 0.0), _holdout_value(row), int(row.get("step2_trades") or 0)),
        reverse=True,
    )
    candidate = candidates[0] if candidates else {}
    candidate_pnl = float(candidate.get("step2_pnl") or 0.0)
    baselines = []
    if isinstance(active, dict):
        baselines.append({
            "name": "current_live_engine",
            "type": "current_live_engine",
            "net_pnl": round(float(active.get("step2_pnl") or 0.0), 4),
        })
    baselines.append({"name": "no_trade", "type": "cash_baseline", "net_pnl": 0.0})
    for baseline in external_benchmarks:
        baselines.append({
            "name": baseline.get("name") or baseline.get("type") or f"external_{len(baselines) + 1}",
            "type": baseline.get("type") or "external",
            "net_pnl": round(float(baseline.get("net_pnl") or baseline.get("pnl") or 0.0), 4),
        })
    beaten = [baseline for baseline in baselines if candidate_pnl > float(baseline.get("net_pnl") or 0.0)]
    weakest_alpha = min(
        [candidate_pnl - float(baseline.get("net_pnl") or 0.0) for baseline in baselines],
        default=0.0,
    )
    includes_random = any(str(baseline.get("type") or "") == "random_entry" for baseline in baselines)
    includes_current = any(str(baseline.get("type") or "") == "current_live_engine" for baseline in baselines)
    blockers = []
    if candidate_variant and not candidate:
        blockers.append("missing_candidate_benchmark_row")
    if len(baselines) < 5:
        blockers.append("fewer_than_5_baselines")
    if len(beaten) < len(baselines):
        blockers.append("not_all_baselines_beaten")
    if weakest_alpha <= 0.0:
        blockers.append("net_alpha_after_costs_not_positive")
    if not includes_random:
        blockers.append("missing_random_entry_baseline")
    if not includes_current:
        blockers.append("missing_current_live_engine_baseline")

    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "candidate_variant": candidate_variant or candidate.get("variant"),
        "candidate_net_pnl": round(candidate_pnl, 4),
        "baseline_count": len(baselines),
        "baselines_beaten_count": len(beaten),
        "net_alpha_after_costs": round(weakest_alpha, 4),
        "includes_random_entry": includes_random,
        "includes_current_live_engine": includes_current,
        "baselines": baselines[:20],
        "ready_for_world_best_certification": not blockers,
        "blockers": blockers,
        "deduction": (
            "World-best claims require beating explicit external baselines, not just the current run's "
            "leaderboard. Random-entry and current-live-engine baselines are mandatory."
        ),
    }


def _fill_quality_proof_report(fill_feedback: dict, candidate_variant: str | None = None) -> dict:
    fills = [row for row in (fill_feedback or {}).get("fills") or [] if isinstance(row, dict)]
    if candidate_variant:
        fills = [
            row for row in fills
            if row.get("variant") == candidate_variant
            or row.get("candidate_variant") == candidate_variant
        ]
    count = len(fills)
    slippages = [float(row.get("slippage_bps") or row.get("slippage_pct") or 0.0) for row in fills]
    latencies = [float(row.get("latency_ms") or 0.0) for row in fills if row.get("latency_ms") is not None]
    rejected = sum(1 for row in fills if str(row.get("status") or "").lower() in {"rejected", "reject", "failed"})
    partial = sum(1 for row in fills if str(row.get("fill_state") or row.get("status") or "").lower() in {"partial", "partially_filled"})
    net_pnl = sum(float(row.get("net_pnl") or row.get("pnl_after_costs") or 0.0) for row in fills)
    avg_slippage = sum(slippages) / max(1, len(slippages))
    avg_latency = sum(latencies) / max(1, len(latencies)) if latencies else 0.0
    rejection_rate = rejected / max(1, count) * 100.0
    partial_rate = partial / max(1, count) * 100.0
    blockers = []
    if count < 100:
        blockers.append("fill_sample_below_100")
    if net_pnl <= 0.0:
        blockers.append("fill_adjusted_net_pnl_not_positive")
    if avg_slippage > 15.0:
        blockers.append("average_slippage_above_15_bps")
    if rejection_rate > 2.0:
        blockers.append("rejection_rate_above_2_pct")
    if partial_rate > 10.0:
        blockers.append("partial_fill_rate_above_10_pct")
    if candidate_variant and not fills:
        blockers.append("missing_candidate_fill_rows")
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "candidate_variant": candidate_variant,
        "fill_count": count,
        "net_pnl_after_fills": round(net_pnl, 4),
        "avg_slippage_bps": round(avg_slippage, 4),
        "avg_latency_ms": round(avg_latency, 4),
        "rejection_rate_pct": round(rejection_rate, 4),
        "partial_fill_rate_pct": round(partial_rate, 4),
        "external_artifact_path": (fill_feedback or {}).get("external_artifact_path"),
        "external_artifact_sha256": (fill_feedback or {}).get("external_artifact_sha256"),
        "ready_for_world_best_certification": not blockers,
        "blockers": blockers,
        "deduction": "Backtest edge is not enough; fill-adjusted profitability must survive realistic execution quality.",
    }


def _regime_keys_from_report(regime_report: dict) -> set[str]:
    keys = set()
    for row in (regime_report or {}).get("fingerprints") or []:
        if not isinstance(row, dict):
            continue
        if row.get("regime_state") != "expand":
            continue
        bucket_type = row.get("bucket_type")
        bucket = row.get("bucket")
        if bucket_type and bucket:
            keys.add(f"{bucket_type}:{bucket}")
    return keys


def _regime_keys_from_proof(proof: dict) -> set[str]:
    if not isinstance(proof, dict):
        return set()
    raw = proof.get("regime_keys")
    if raw is None and isinstance(proof.get("regime_fingerprint"), dict):
        raw = proof["regime_fingerprint"].get("regime_keys") or proof["regime_fingerprint"].get("keys")
    if raw is None and proof.get("market_regime"):
        raw = [proof.get("market_regime")]
    if isinstance(raw, str):
        raw = [raw]
    return {str(item) for item in raw or [] if item}


def _proof_regime_match_report(regime_report: dict, proof_sources: dict[str, dict]) -> dict:
    current = _regime_keys_from_report(regime_report)
    entries = []
    blockers = []
    for role, proof in sorted((proof_sources or {}).items()):
        if not proof:
            continue
        proof_keys = _regime_keys_from_proof(proof)
        matched = sorted(current & proof_keys)
        if not proof_keys:
            blockers.append(f"{role}_missing_regime_keys")
        elif not matched:
            blockers.append(f"{role}_regime_mismatch")
        entries.append({
            "role": role,
            "proof_regime_keys": sorted(proof_keys),
            "matched_regime_keys": matched,
            "match": bool(matched),
        })
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "current_regime_keys": sorted(current),
        "checked_proof_count": len(entries),
        "matched_proof_count": sum(1 for row in entries if row["match"]),
        "ready_for_world_best_certification": bool(entries) and not blockers,
        "blockers": blockers,
        "entries": entries,
        "deduction": "Proof must be tagged to regimes that overlap the current run's expandable regime fingerprints.",
    }


def _candidate_proof_dossier(
    candidate_variant: str | None,
    *,
    historical_protocol: dict,
    oos_report: dict,
    causal_report: dict,
    promotion_ladder: dict,
    benchmark_report: dict,
    fill_quality_report: dict,
    proof_manifest: dict,
    proof_freshness: dict,
    proof_regime_match: dict,
    calibration_report: dict,
) -> dict:
    sections = [
        ("historical_split", historical_protocol),
        ("oos_profitability", oos_report),
        ("causal_ablation", causal_report),
        ("promotion_ladder", promotion_ladder),
        ("benchmark_superiority", benchmark_report),
        ("fill_quality", fill_quality_report),
        ("proof_manifest", proof_manifest),
        ("proof_freshness", proof_freshness),
        ("proof_regime_match", proof_regime_match),
    ]
    resolved = int((calibration_report or {}).get("resolved_predictions") or 0)
    pending = int((calibration_report or {}).get("pending_predictions") or 0)
    calibration_error = float((calibration_report or {}).get("expected_calibration_error") or 1.0)
    calibration_ready = resolved >= 30 and pending <= resolved * 2 and calibration_error <= 0.12
    rows = []
    blockers = []
    for name, report in sections:
        report = report if isinstance(report, dict) else {}
        ready = report.get("ready_for_world_best_certification") is True
        section_blockers = [str(item) for item in report.get("blockers") or []]
        if not ready:
            blockers.extend(f"{name}:{item}" for item in (section_blockers or ["not_ready"]))
        rows.append({
            "section": name,
            "ready": ready,
            "blockers": section_blockers,
        })
    if not calibration_ready:
        blockers.append("calibration:not_enough_resolved_low_error_predictions")
    rows.append({
        "section": "belief_calibration",
        "ready": calibration_ready,
        "blockers": [] if calibration_ready else ["not_enough_resolved_low_error_predictions"],
        "resolved_predictions": resolved,
        "pending_predictions": pending,
        "expected_calibration_error": calibration_error,
    })
    ready_count = sum(1 for row in rows if row["ready"])
    score = round(ready_count / max(1, len(rows)) * 100.0, 4)
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "candidate_variant": candidate_variant,
        "section_count": len(rows),
        "ready_section_count": ready_count,
        "proof_score": score,
        "ready_for_world_best_certification": not blockers,
        "blockers": blockers,
        "sections": rows,
        "deduction": "A candidate is only elite-ready when every proof dimension is ready for the same selected candidate.",
    }


def _genetic_search_operator_report(candidate_lineage: dict, route_priors: dict, args: argparse.Namespace) -> dict:
    parents = [
        row for row in (candidate_lineage or {}).get("lineage") or []
        if row.get("lineage_state") in {"promising_child", "needs_repair"}
    ][:20]
    scale_routes = [
        row.get("route_key")
        for row in (route_priors or {}).get("top_priors") or []
        if row.get("prior_state") == "scale_or_repair"
    ][:20]
    batch_size = int(getattr(args, "batch_size", 0) or 0)
    operators = [
        {"operator": "elitism_replay", "budget_pct": 10.0, "purpose": "preserve best observed behavior exactly"},
        {"operator": "single_route_mutation", "budget_pct": 25.0, "purpose": "change one route while preserving lineage"},
        {"operator": "route_crossover", "budget_pct": 30.0, "purpose": "combine two promising parents"},
        {"operator": "repair_mutation", "budget_pct": 20.0, "purpose": "fix one known failure class"},
        {"operator": "regime_probe_mutation", "budget_pct": 15.0, "purpose": "test favorable regime fingerprints"},
    ]
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "enabled_for_next_generation": bool(parents or scale_routes),
        "parent_count": len(parents),
        "parents": parents[:20],
        "scale_route_count": len(scale_routes),
        "scale_routes": scale_routes,
        "operators": [
            {**row, "variant_budget": int(round(batch_size * float(row["budget_pct"]) / 100.0))}
            for row in operators
        ],
        "guardrails": ["preserve_score_only_contract", "quote_aware_exit_only", "dedupe_behavior_hashes"],
    }


def _bandit_lane_allocator_report(rows: list[dict], args: argparse.Namespace) -> dict:
    lanes: dict[str, dict[str, Any]] = {}
    for row in rows:
        lane = _lane_name(row)
        item = lanes.setdefault(lane, {"lane": lane, "observations": 0, "reward_sum": 0.0, "positive_count": 0})
        pnl = float(row.get("step2_pnl") or 0.0)
        holdout = _holdout_value(row)
        reward = pnl + holdout * 2.0 + min(250.0, int(row.get("step2_trades") or 0)) * 0.05
        item["observations"] += 1
        item["reward_sum"] += reward
        item["positive_count"] += 1 if pnl > 0.0 and holdout > 0.0 else 0
    scored = []
    min_score = 0.05
    for item in lanes.values():
        obs = max(1, int(item["observations"]))
        avg_reward = float(item["reward_sum"]) / obs
        score = max(min_score, avg_reward + 100.0)
        scored.append({
            "lane": item["lane"],
            "observations": item["observations"],
            "avg_reward": round(avg_reward, 4),
            "positive_holdout_count": item["positive_count"],
            "raw_weight": score,
        })
    total_weight = sum(float(row["raw_weight"]) for row in scored) or 1.0
    batch_size = int(getattr(args, "batch_size", 0) or 0)
    allocated = 0
    scored.sort(key=lambda row: float(row["raw_weight"]), reverse=True)
    for idx, row in enumerate(scored):
        budget = batch_size - allocated if idx == len(scored) - 1 else int(round(batch_size * float(row["raw_weight"]) / total_weight))
        allocated += budget
        row["budget_pct"] = round(float(row["raw_weight"]) / total_weight * 100.0, 4)
        row["variant_budget"] = max(0, budget)
        row["bandit_state"] = "exploit" if row["budget_pct"] >= 25.0 else ("probe" if row["positive_holdout_count"] else "explore_or_reduce")
        row.pop("raw_weight", None)
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "batch_size": batch_size,
        "lane_count": len(scored),
        "allocations": scored,
    }


def _beam_search_route_set_optimizer_report(route_priors: dict, shapley_report: dict, args: argparse.Namespace) -> dict:
    route_scores: dict[str, float] = {}
    for row in (route_priors or {}).get("top_priors") or []:
        route_scores[str(row.get("route_key"))] = max(
            route_scores.get(str(row.get("route_key")), -1e18),
            float(row.get("avg_holdout_pnl") or 0.0) * 2.0 + float(row.get("avg_pnl") or 0.0),
        )
    for row in (shapley_report or {}).get("routes") or []:
        key = str(row.get("route_key"))
        route_scores[key] = route_scores.get(key, 0.0) + float(row.get("avg_holdout_credit") or 0.0) * 2.0 + float(row.get("avg_pnl_credit") or 0.0)
    ordered = sorted(route_scores.items(), key=lambda item: item[1], reverse=True)[:12]
    beams = []
    for width in (1, 2, 3, 4):
        if len(ordered) < width:
            continue
        for start in range(0, min(6, len(ordered) - width + 1)):
            route_set = ordered[start:start + width]
            beams.append({
                "beam_id": f"beam_{len(beams) + 1:03d}",
                "route_set": [key for key, _ in route_set],
                "beam_width": width,
                "score": round(sum(score for _, score in route_set) / width, 4),
                "recommended_budget": max(3, int(round(int(getattr(args, "batch_size", 0) or 0) * (0.06 if width <= 2 else 0.04)))),
            })
    beams.sort(key=lambda row: float(row["score"]), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "beam_count": len(beams),
        "beams": beams[:50],
        "optimizer_state": "ready" if beams else "needs_route_priors",
    }


def _pnl_haircut_sensitivity_report(rows: list[dict], label: str, per_trade_haircuts: list[float]) -> dict:
    candidates = sorted(
        [row for row in rows if float(row.get("step2_pnl") or 0.0) > 0.0 or _holdout_value(row) > 0.0],
        key=lambda row: (_holdout_value(row), float(row.get("step2_pnl") or 0.0)),
        reverse=True,
    )[:50]
    scenarios = []
    for haircut in per_trade_haircuts:
        survivors = []
        for row in candidates:
            trades = int(row.get("step2_trades") or 0)
            pnl = float(row.get("step2_pnl") or 0.0) - trades * haircut
            holdout = _holdout_value(row) - trades * haircut
            if pnl > 0.0 and holdout > 0.0:
                survivors.append({
                    "variant": row.get("variant"),
                    "pnl_after_haircut": round(pnl, 4),
                    "holdout_after_haircut": round(holdout, 4),
                    "trades": trades,
                })
        scenarios.append({
            "haircut_per_trade": haircut,
            "survivor_count": len(survivors),
            "survivors": survivors[:25],
        })
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "sensitivity": label,
        "candidate_count": len(candidates),
        "scenarios": scenarios,
        "model_note": "score_only_pnl_haircut_proxy_until_fill_level_replay_inputs_exist",
    }


def _worker_pool_parallel_batch_runner_report(args: argparse.Namespace) -> dict:
    cpu_count = os.cpu_count() or 1
    recommended = max(1, min(3, cpu_count - 1 if cpu_count > 1 else 1))
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "runner_state": "contract_ready",
        "recommended_workers": recommended,
        "max_safe_workers_without_explicit_override": recommended,
        "score_only_requirements": [
            "load_existing_compiled_decision_tape_only",
            "do_not_rebuild_signals",
            "do_not_rebuild_compiled_tape",
            "partition_variant_batch_only",
            "merge_summary_artifacts_deterministically",
        ],
        "batch_size": int(getattr(args, "batch_size", 0) or 0),
    }


def _safe_feature_arrays(compiled: dict) -> tuple[np.ndarray, list[str], np.ndarray, np.ndarray]:
    features = np.asarray(compiled.get("features") if compiled.get("features") is not None else [], dtype=np.float64)
    if features.ndim != 2:
        features = np.zeros((0, 0), dtype=np.float64)
    feature_names = list(getattr(routed, "FEATURE_INDEX", {}) or {})
    if len(feature_names) != features.shape[1]:
        feature_names = [f"feature_{idx}" for idx in range(features.shape[1])]
    long_pct = np.asarray(compiled.get("long_pnl_pct") if compiled.get("long_pnl_pct") is not None else [], dtype=np.float64)
    short_pct = np.asarray(compiled.get("short_pnl_pct") if compiled.get("short_pnl_pct") is not None else [], dtype=np.float64)
    row_count = min(features.shape[0], long_pct.shape[0], short_pct.shape[0])
    return features[:row_count], feature_names[:features.shape[1]], long_pct[:row_count], short_pct[:row_count]


def _compiled_context_vectors(compiled: dict, row_count: int) -> dict[str, np.ndarray]:
    def names_from_codes(code_key: str, map_key: str, fallback_prefix: str) -> np.ndarray:
        codes = np.asarray(compiled.get(code_key) if compiled.get(code_key) is not None else [], dtype=np.int64)[:row_count]
        reverse = _reverse_map(compiled.get(map_key) or {})
        return np.asarray([reverse.get(int(code), f"{fallback_prefix}_{int(code)}") for code in codes], dtype=object)

    contexts = {
        "ticker": names_from_codes("ticker_code", "ticker_map", "ticker"),
        "setup_type": names_from_codes("setup_code", "setup_map", "setup"),
        "day": names_from_codes("day_code", "day_map", "day"),
    }
    side = np.asarray(compiled.get("original_side") if compiled.get("original_side") is not None else [], dtype=np.int8)[:row_count]
    contexts["side"] = np.asarray(np.where(side > 0, "LONG", np.where(side < 0, "SHORT", "SKIP")), dtype=object)
    try:
        contexts["session_phase"] = _phase_vector(compiled)[:row_count]
    except Exception:
        contexts["session_phase"] = np.full(row_count, "unknown", dtype=object)
    return contexts


def _opportunity_target_model_report(compiled: dict) -> dict:
    features, _, long_pct, short_pct = _safe_feature_arrays(compiled)
    row_count = int(features.shape[0])
    if row_count <= 0:
        return {
            "schema_version": 1,
            "source": "step2_profit_combo_hunter",
            "model_state": "unavailable",
            "reason": "compiled_feature_matrix_empty",
            "row_count": 0,
            "targets": {},
        }
    long_held = np.asarray(compiled.get("long_held") if compiled.get("long_held") is not None else np.zeros(row_count), dtype=np.float64)[:row_count]
    short_held = np.asarray(compiled.get("short_held") if compiled.get("short_held") is not None else np.zeros(row_count), dtype=np.float64)[:row_count]
    long_reason = np.asarray(compiled.get("long_reason_code") if compiled.get("long_reason_code") is not None else np.zeros(row_count), dtype=np.int16)[:row_count]
    short_reason = np.asarray(compiled.get("short_reason_code") if compiled.get("short_reason_code") is not None else np.zeros(row_count), dtype=np.int16)[:row_count]
    bad_reasons = {
        decision_tape_compiled.REASON_CODE.get("stop_loss", 4),
        decision_tape_compiled.REASON_CODE.get("path_failure", 7),
        decision_tape_compiled.REASON_CODE.get("path_failure_flip", 8),
    }
    long_survive = (long_pct > 0.0) & ~np.isin(long_reason, list(bad_reasons))
    short_survive = (short_pct > 0.0) & ~np.isin(short_reason, list(bad_reasons))

    def summarize(values: np.ndarray) -> dict:
        finite = values[np.isfinite(values)]
        if finite.size <= 0:
            return {"mean": 0.0, "p10": 0.0, "p05": 0.0, "tail_loss": 0.0}
        return {
            "mean": round(float(np.mean(finite)), 6),
            "p10": round(float(np.quantile(finite, 0.10)), 6),
            "p05": round(float(np.quantile(finite, 0.05)), 6),
            "tail_loss": round(float(min(0.0, np.quantile(finite, 0.05))), 6),
        }

    best_side = np.where(long_pct >= short_pct, "LONG", "SHORT")
    best_pct = np.maximum(long_pct, short_pct)
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "model_state": "trained",
        "row_count": row_count,
        "targets": {
            "expected_long_outcome_pct": summarize(long_pct),
            "expected_short_outcome_pct": summarize(short_pct),
            "expected_skip_value_pct": {"mean": 0.0, "p10": 0.0, "p05": 0.0, "tail_loss": 0.0},
            "quote_aware_survival_probability": {
                "long": round(float(np.mean(long_survive)), 6),
                "short": round(float(np.mean(short_survive)), 6),
                "best_side": round(float(np.mean(np.where(best_side == "LONG", long_survive, short_survive))), 6),
            },
            "expected_pnl_per_trade_pct": summarize(best_pct),
            "downside_tail_risk_pct": {
                "long_p05": round(float(np.quantile(long_pct, 0.05)), 6),
                "short_p05": round(float(np.quantile(short_pct, 0.05)), 6),
                "best_side_p05": round(float(np.quantile(best_pct, 0.05)), 6),
            },
            "expected_best_side_mix": {
                "long_count": int(np.sum(best_side == "LONG")),
                "short_count": int(np.sum(best_side == "SHORT")),
            },
            "avg_hold_seconds": {
                "long": round(float(np.mean(long_held)), 4),
                "short": round(float(np.mean(short_held)), 4),
            },
        },
    }


def _opportunity_feature_learner_report(compiled: dict, max_features: int = 40) -> dict:
    features, feature_names, long_pct, short_pct = _safe_feature_arrays(compiled)
    row_count = int(features.shape[0])
    if row_count <= 0 or features.shape[1] <= 0:
        return {
            "schema_version": 1,
            "source": "step2_profit_combo_hunter",
            "model_state": "unavailable",
            "reason": "compiled_feature_matrix_empty",
            "row_count": row_count,
            "features": [],
        }
    long_edge = long_pct
    short_edge = short_pct
    side_edge = np.maximum(long_edge, short_edge)
    signed_edge = long_edge - short_edge
    rows = []
    for idx, name in enumerate(feature_names):
        values = np.asarray(features[:, idx], dtype=np.float64)
        finite = np.isfinite(values) & np.isfinite(side_edge)
        if int(np.sum(finite)) < 10:
            continue
        vals = values[finite]
        edge = side_edge[finite]
        signed = signed_edge[finite]
        std = float(np.std(vals))
        if std <= 1e-12:
            corr = 0.0
            signed_corr = 0.0
        else:
            corr = float(np.corrcoef(vals, edge)[0, 1]) if len(vals) > 1 else 0.0
            signed_corr = float(np.corrcoef(vals, signed)[0, 1]) if len(vals) > 1 else 0.0
            if not np.isfinite(corr):
                corr = 0.0
            if not np.isfinite(signed_corr):
                signed_corr = 0.0
        q25, q50, q75 = [float(np.quantile(vals, q)) for q in (0.25, 0.50, 0.75)]
        high_mask = vals >= q75
        low_mask = vals <= q25
        high_edge = float(np.mean(edge[high_mask])) if np.any(high_mask) else 0.0
        low_edge = float(np.mean(edge[low_mask])) if np.any(low_mask) else 0.0
        high_long = float(np.mean(long_edge[finite][high_mask])) if np.any(high_mask) else 0.0
        high_short = float(np.mean(short_edge[finite][high_mask])) if np.any(high_mask) else 0.0
        low_long = float(np.mean(long_edge[finite][low_mask])) if np.any(low_mask) else 0.0
        low_short = float(np.mean(short_edge[finite][low_mask])) if np.any(low_mask) else 0.0
        rows.append({
            "feature": name,
            "observations": int(np.sum(finite)),
            "edge_corr": round(corr, 6),
            "long_minus_short_corr": round(signed_corr, 6),
            "q25": round(q25, 6),
            "median": round(q50, 6),
            "q75": round(q75, 6),
            "high_bucket_edge": round(high_edge, 6),
            "low_bucket_edge": round(low_edge, 6),
            "high_bucket_preferred_side": "LONG" if high_long >= high_short else "SHORT",
            "low_bucket_preferred_side": "LONG" if low_long >= low_short else "SHORT",
            "direction": "high_values_help" if high_edge >= low_edge else "low_values_help",
            "signal_strength": round(abs(high_edge - low_edge) + abs(corr), 6),
        })
    rows.sort(key=lambda row: float(row["signal_strength"]), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "model_state": "trained",
        "model_type": "univariate_feature_edge_learner",
        "row_count": row_count,
        "feature_count": len(rows),
        "target_columns": ["long_pnl_pct", "short_pnl_pct", "max_side_edge", "long_minus_short_edge"],
        "features": rows[:max_features],
    }


def _opportunity_rule_extraction_report(compiled: dict, feature_learner: dict, max_rules: int = 60) -> dict:
    features, feature_names, long_pct, short_pct = _safe_feature_arrays(compiled)
    feature_index = {name: idx for idx, name in enumerate(feature_names)}
    contexts = _compiled_context_vectors(compiled, int(features.shape[0]))
    rules = []

    def add_rule(rule: dict) -> None:
        rule["rule_id"] = f"feature_rule_{len(rules) + 1:03d}"
        rules.append(rule)

    for feature in (feature_learner or {}).get("features") or []:
        name = str(feature.get("feature") or "")
        idx = feature_index.get(name)
        if idx is None:
            continue
        threshold = float(feature.get("q75") if feature.get("direction") == "high_values_help" else feature.get("q25"))
        op = ">=" if feature.get("direction") == "high_values_help" else "<="
        values = features[:, idx]
        mask = values >= threshold if op == ">=" else values <= threshold
        support = int(np.sum(mask))
        if support <= 0:
            continue
        long_avg = float(np.mean(long_pct[mask]))
        short_avg = float(np.mean(short_pct[mask]))
        best_side = "LONG" if long_avg >= short_avg else "SHORT"
        best_edge = max(long_avg, short_avg)
        add_rule({
            "rule_type": "feature_threshold",
            "feature": name,
            "op": op,
            "threshold": round(threshold, 6),
            "support": support,
            "support_pct": round(support / max(1, int(features.shape[0])) * 100.0, 4),
            "preferred_side": best_side,
            "avg_long_pnl_pct": round(long_avg, 6),
            "avg_short_pnl_pct": round(short_avg, 6),
            "expected_edge_pct": round(best_edge, 6),
            "route_hint": {
                "feature_filters": [{"feature": name, "op": op, "value": round(threshold, 6)}],
                "action": "force_long" if best_side == "LONG" else "force_short",
            },
        })
        for context_name in ("ticker", "setup_type", "session_phase", "side"):
            context_values = contexts.get(context_name)
            if context_values is None or context_values.shape[0] != mask.shape[0]:
                continue
            for value in sorted(set(str(item) for item in context_values[mask]))[:20]:
                combo_mask = mask & (context_values == value)
                support = int(np.sum(combo_mask))
                if support < 3:
                    continue
                combo_long = float(np.mean(long_pct[combo_mask]))
                combo_short = float(np.mean(short_pct[combo_mask]))
                combo_best_side = "LONG" if combo_long >= combo_short else "SHORT"
                combo_best_edge = max(combo_long, combo_short)
                action = "force_long" if combo_best_side == "LONG" else "force_short"
                state = "profitable_pocket" if combo_best_edge > 0.0 else "toxic_pocket"
                add_rule({
                    "rule_type": "context_feature_threshold",
                    "context": {context_name: value},
                    "feature": name,
                    "op": op,
                    "threshold": round(threshold, 6),
                    "support": support,
                    "support_pct": round(support / max(1, int(features.shape[0])) * 100.0, 4),
                    "preferred_side": combo_best_side,
                    "avg_long_pnl_pct": round(combo_long, 6),
                    "avg_short_pnl_pct": round(combo_short, 6),
                    "expected_edge_pct": round(combo_best_edge, 6),
                    "rule_state": state,
                    "interpretation": f"{value} {combo_best_side.lower()} works when {name} {op} {round(threshold, 6)}" if state == "profitable_pocket" else f"{value} pocket is toxic around {name} {op} {round(threshold, 6)}",
                    "route_hint": {
                        context_name: value,
                        "feature_filters": [{"feature": name, "op": op, "value": round(threshold, 6)}],
                        "action": action,
                    },
                })
    rules.sort(key=lambda row: (float(row["expected_edge_pct"]), int(row["support"])), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "rule_count": len(rules),
        "rules": rules[:max_rules],
        "rule_family": "indicator_threshold_rules",
        "supports_context_feature_rules": True,
    }


def _variant_meta_model_report(rows: list[dict], feature_rules: dict, route_priors: dict, args: argparse.Namespace) -> dict:
    lane_stats: dict[str, dict[str, Any]] = {}
    route_combo_stats: dict[str, dict[str, Any]] = {}
    winner_feature_hits: dict[str, int] = defaultdict(int)
    operator_stats: dict[str, dict[str, Any]] = {}

    def operator_name(row: dict) -> str:
        name = str(row.get("variant") or "")
        for lane in ("frontier", "edge_bridge", "edge_density", "registry_good", "boost", "scaleup", "portfolio", "micro", "rescue", "mixed", "skip_combo", "skip_single"):
            if name.startswith(f"profit_{lane}"):
                return lane
        return _lane_name(row)

    for row in rows:
        lane = _lane_name(row)
        item = lane_stats.setdefault(lane, {"lane": lane, "count": 0, "pnl_sum": 0.0, "holdout_sum": 0.0, "positive_holdout": 0})
        item["count"] += 1
        item["pnl_sum"] += float(row.get("step2_pnl") or 0.0)
        item["holdout_sum"] += _holdout_value(row)
        item["positive_holdout"] += 1 if _holdout_value(row) > 0.0 else 0
        routes = sorted(_route_key_from_payload(route) for route in row.get("routes") or [] if isinstance(route, dict))
        combo_key = " + ".join(routes[:8]) if routes else "fallback"
        combo = route_combo_stats.setdefault(combo_key, {"route_set": routes[:8], "count": 0, "pnl_sum": 0.0, "holdout_sum": 0.0})
        combo["count"] += 1
        combo["pnl_sum"] += float(row.get("step2_pnl") or 0.0)
        combo["holdout_sum"] += _holdout_value(row)
        op = operator_stats.setdefault(operator_name(row), {"operator": operator_name(row), "count": 0, "reward_sum": 0.0, "positive_holdout": 0})
        op["count"] += 1
        op["reward_sum"] += float(row.get("step2_pnl") or 0.0) + _holdout_value(row) * 2.0
        op["positive_holdout"] += 1 if _holdout_value(row) > 0.0 else 0
        if _holdout_value(row) > 0.0:
            route_text = " ".join(routes)
            for rule in (feature_rules or {}).get("rules") or []:
                feature = str(rule.get("feature") or "")
                if feature and feature in route_text:
                    winner_feature_hits[feature] += 1
    lanes = []
    for item in lane_stats.values():
        count = max(1, int(item["count"]))
        avg_pnl = float(item["pnl_sum"]) / count
        avg_holdout = float(item["holdout_sum"]) / count
        probability = min(0.98, max(0.02, (item["positive_holdout"] + 1.0) / (count + 2.0)))
        lanes.append({
            "lane": item["lane"],
            "observations": item["count"],
            "avg_pnl": round(avg_pnl, 4),
            "avg_holdout_pnl": round(avg_holdout, 4),
            "estimated_holdout_success_prob": round(probability, 6),
            "meta_score": round(avg_pnl + avg_holdout * 2.0 + probability * 100.0, 4),
        })
    lanes.sort(key=lambda row: float(row["meta_score"]), reverse=True)
    route_combos = []
    for item in route_combo_stats.values():
        count = max(1, int(item["count"]))
        avg_pnl = float(item["pnl_sum"]) / count
        avg_holdout = float(item["holdout_sum"]) / count
        route_combos.append({
            "route_set": item["route_set"],
            "observations": item["count"],
            "avg_pnl": round(avg_pnl, 4),
            "avg_holdout_pnl": round(avg_holdout, 4),
            "works_state": "works" if avg_pnl > 0.0 and avg_holdout > 0.0 else ("false_discovery_risk" if avg_pnl > 0.0 else "does_not_work"),
        })
    route_combos.sort(key=lambda row: (float(row["avg_holdout_pnl"]), float(row["avg_pnl"])), reverse=True)
    operators = []
    for item in operator_stats.values():
        count = max(1, int(item["count"]))
        reward = float(item["reward_sum"]) / count
        operators.append({
            "operator": item["operator"],
            "observations": item["count"],
            "avg_reward": round(reward, 4),
            "positive_holdout_rate_pct": round(float(item["positive_holdout"]) / count * 100.0, 4),
            "worth_budget": reward > 0.0 or item["positive_holdout"] > 0,
        })
    operators.sort(key=lambda row: (bool(row["worth_budget"]), float(row["avg_reward"])), reverse=True)
    top_rules = (feature_rules or {}).get("rules") or []
    top_routes = (route_priors or {}).get("focus_routes") or []
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "model_state": "trained" if rows else "needs_scored_rows",
        "model_type": "variant_meta_ranker",
        "training_variant_count": len(rows),
        "batch_size": int(getattr(args, "batch_size", 0) or 0),
        "lane_scores": lanes[:50],
        "top_indicator_rule_hints": top_rules[:20],
        "top_route_hints": top_routes[:20],
        "route_combination_scores": route_combos[:50],
        "winner_feature_frequencies": [
            {"feature": feature, "winner_count": count}
            for feature, count in sorted(winner_feature_hits.items(), key=lambda item: item[1], reverse=True)[:50]
        ],
        "false_discovery_candidates": [row for row in route_combos if row["works_state"] == "false_discovery_risk"][:50],
        "operator_budget_worth": operators[:50],
    }


def _uncertainty_model_report(feature_learner: dict, variant_meta: dict, route_priors: dict) -> dict:
    cells = []
    for feature in (feature_learner or {}).get("features") or []:
        obs = int(feature.get("observations") or 0)
        strength = float(feature.get("signal_strength") or 0.0)
        uncertainty = (1.0 / max(1.0, obs ** 0.5)) + max(0.0, 1.0 - min(1.0, strength))
        cells.append({
            "subject": f"feature:{feature.get('feature')}",
            "subject_type": "indicator_feature",
            "observations": obs,
            "uncertainty_score": round(uncertainty, 6),
            "confidence": round(max(0.0, min(1.0, 1.0 - uncertainty / 2.0)), 6),
            "confidence_band": "low" if uncertainty >= 0.75 else ("medium" if uncertainty >= 0.35 else "high"),
            "recommended_action": "tiny_probe" if uncertainty >= 0.75 else ("controlled_sibling_test" if uncertainty >= 0.35 else "exploit_or_scale"),
        })
    for lane in (variant_meta or {}).get("lane_scores") or []:
        obs = int(lane.get("observations") or 0)
        prob = float(lane.get("estimated_holdout_success_prob") or 0.0)
        uncertainty = (1.0 / max(1.0, obs ** 0.5)) + abs(0.5 - prob)
        cells.append({
            "subject": f"lane:{lane.get('lane')}",
            "subject_type": "variant_lane",
            "observations": obs,
            "uncertainty_score": round(uncertainty, 6),
            "confidence": round(max(0.0, min(1.0, 1.0 - uncertainty / 2.0)), 6),
            "confidence_band": "low" if uncertainty >= 0.75 else ("medium" if uncertainty >= 0.35 else "high"),
            "recommended_action": "tiny_probe" if uncertainty >= 0.75 else ("controlled_sibling_test" if uncertainty >= 0.35 else "exploit_or_scale"),
        })
    for route in (route_priors or {}).get("uncertain_routes") or []:
        cells.append({
            "subject": f"route:{route}",
            "subject_type": "route_prior",
            "observations": None,
            "uncertainty_score": 1.0,
            "confidence": 0.0,
            "confidence_band": "low",
            "recommended_action": "tiny_probe",
        })
    cells.sort(key=lambda row: float(row["uncertainty_score"]), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "model_state": "trained" if cells else "no_uncertainty_cells",
        "cell_count": len(cells),
        "cells": cells[:100],
    }


def _teacher_student_packet(
    feature_learner: dict,
    feature_rules: dict,
    variant_meta: dict,
    uncertainty_model: dict,
    experiment_sequencer: dict,
    args: argparse.Namespace | None = None,
) -> dict:
    proposals = []
    for rule in (feature_rules or {}).get("rules") or []:
        confidence = min(0.99, max(0.01, float(rule.get("support_pct") or 0.0) / 100.0 + max(0.0, float(rule.get("expected_edge_pct") or 0.0))))
        proposals.append({
            "proposal_id": f"student_rule_{len(proposals) + 1:03d}",
            "proposal_type": "indicator_rule_route",
            "hypothesis": f"{rule.get('feature')} {rule.get('op')} {rule.get('threshold')} has positive {rule.get('preferred_side')} edge.",
            "route_hint": rule.get("route_hint"),
            "expected_value": round(float(rule.get("expected_edge_pct") or 0.0), 6),
            "confidence": round(confidence, 6),
            "why_this_should_work": rule.get("interpretation") or "indicator threshold has positive quote-aware historical edge",
            "what_would_falsify_it": "quote-aware score is not positive on holdout or creates a new failure class",
            "teacher_check": "quote_aware_score_variant_and_require_positive_holdout",
        })
        if len(proposals) >= 20:
            break
    for lane in (variant_meta or {}).get("lane_scores") or []:
        proposals.append({
            "proposal_id": f"student_lane_{len(proposals) + 1:03d}",
            "proposal_type": "lane_budget_bias",
            "hypothesis": f"Lane {lane.get('lane')} should receive budget based on meta score {lane.get('meta_score')}.",
            "expected_value": round(float(lane.get("avg_pnl") or 0.0) + float(lane.get("avg_holdout_pnl") or 0.0) * 2.0, 6),
            "confidence": round(float(lane.get("estimated_holdout_success_prob") or 0.0), 6),
            "why_this_should_work": "lane has scored-variant evidence and holdout-success estimate",
            "what_would_falsify_it": "controlled siblings fail holdout or underperform active baseline",
            "teacher_check": "score_controlled_siblings_and_compare_to_baseline",
        })
        if len(proposals) >= 40:
            break
    target_count = int(getattr(args, "batch_size", 0) or 0) if args is not None else 0
    if target_count <= 0:
        target_count = max(1, len(proposals))
    candidate_variants = []
    source = proposals or [{
        "proposal_id": "student_fallback_001",
        "proposal_type": "fallback_probe",
        "hypothesis": "No student proposal available; reserve a tiny baseline probe.",
        "expected_value": 0.0,
        "confidence": 0.01,
        "why_this_should_work": "keeps the teacher-student loop alive with a minimal probe",
        "what_would_falsify_it": "any material quote-aware loss",
        "teacher_check": "quote_aware_score_variant_and_require_positive_holdout",
    }]
    for idx in range(target_count):
        proposal = source[idx % len(source)]
        candidate_variants.append({
            "candidate_id": f"student_candidate_{idx + 1:04d}",
            "source_proposal_id": proposal.get("proposal_id"),
            "proposal_type": proposal.get("proposal_type"),
            "expected_value": proposal.get("expected_value"),
            "confidence": proposal.get("confidence"),
            "why_this_should_work": proposal.get("why_this_should_work"),
            "what_would_falsify_it": proposal.get("what_would_falsify_it"),
            "teacher_check": proposal.get("teacher_check"),
            "route_hint": proposal.get("route_hint"),
            "score_status": "proposed_not_scored_until_next_hunt",
        })
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "teacher": "quote_aware_step2_simulator",
        "student_models": [
            (feature_learner or {}).get("model_type"),
            (variant_meta or {}).get("model_type"),
            "uncertainty_model",
        ],
        "proposal_count": len(proposals),
        "proposals": proposals,
        "requested_candidate_count": target_count,
        "candidate_variant_count": len(candidate_variants),
        "candidate_variants": candidate_variants,
        "top_uncertainties": (uncertainty_model or {}).get("cells", [])[:20],
        "next_sequence": (experiment_sequencer or {}).get("sequence", [])[:20],
        "non_negotiable": [
            "student_never_replaces_quote_aware_teacher",
            "student_proposals_must_be_scored",
            "positive_total_without_holdout_is_not_promotion",
        ],
    }


def _walk_forward_validation_bundle(
    rows: list[dict],
    active: dict,
    stress_queue: dict,
    slippage_sensitivity: dict,
    spread_sensitivity: dict,
) -> dict:
    validations = []
    active_pnl = float((active or {}).get("step2_pnl") or 0.0)
    active_trades = int((active or {}).get("step2_trades") or 0)
    for row in sorted(rows, key=lambda item: (_holdout_value(item), float(item.get("step2_pnl") or 0.0)), reverse=True)[:100]:
        by_day = row.get("by_day") if isinstance(row.get("by_day"), dict) else {}
        by_ticker = row.get("by_ticker") if isinstance(row.get("by_ticker"), dict) else {}
        by_side = row.get("by_side") if isinstance(row.get("by_side"), dict) else {}
        routes = [route for route in row.get("routes") or [] if isinstance(route, dict)]
        route_keys = sorted({_route_key_from_payload(route) for route in routes})
        phases = sorted({
            str((route.get("match") or {}).get("session_phase"))
            for route in routes
            if (route.get("match") or {}).get("session_phase")
        })
        day_pnls = [float((value or {}).get("pnl") or 0.0) for value in by_day.values() if isinstance(value, dict)]
        ticker_pnls = [float((value or {}).get("pnl") or 0.0) for value in by_ticker.values() if isinstance(value, dict)]
        side_pnls = [float((value or {}).get("pnl") or 0.0) for value in by_side.values() if isinstance(value, dict)]
        leave_one_day_min = None
        if len(day_pnls) >= 2:
            total = sum(day_pnls)
            leave_one_day_min = min(total - pnl for pnl in day_pnls)
        checks = {
            "day_rotation_ok": bool(day_pnls) and min(day_pnls) > 0.0,
            "ticker_rotation_ok": bool(ticker_pnls) and min(ticker_pnls) > 0.0,
            "side_rotation_ok": bool(side_pnls) and min(side_pnls) > 0.0,
            "session_phase_rotation_present": bool(phases),
            "leave_one_day_ok": leave_one_day_min is not None and leave_one_day_min > 0.0,
            "beats_active_pnl": float(row.get("step2_pnl") or 0.0) > active_pnl,
            "beats_active_trade_count": int(row.get("step2_trades") or 0) >= active_trades,
        }
        validations.append({
            "variant": row.get("variant"),
            "route_keys": route_keys,
            "pnl": round(float(row.get("step2_pnl") or 0.0), 4),
            "holdout_pnl": round(_holdout_value(row), 4),
            "trades": int(row.get("step2_trades") or 0),
            "day_count": len(day_pnls),
            "ticker_count": len(ticker_pnls),
            "side_count": len(side_pnls),
            "session_phases": phases,
            "leave_one_day_min_pnl": round(float(leave_one_day_min), 4) if leave_one_day_min is not None else None,
            "checks": checks,
            "passed": all(checks.values()),
        })
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "validation_count": len(validations),
        "passed_count": sum(1 for item in validations if item["passed"]),
        "validations": validations,
        "stress_test_queue": stress_queue,
        "slippage_sensitivity_sweep": slippage_sensitivity,
        "spread_sensitivity_sweep": spread_sensitivity,
        "active_baseline": {
            "pnl": round(active_pnl, 4),
            "trades": active_trades,
        },
    }


def _calibration_validation_rows(walk_forward: dict) -> list[dict]:
    rows = []
    for validation in (walk_forward or {}).get("validations") or []:
        if not isinstance(validation, dict):
            continue
        for route_key in validation.get("route_keys") or []:
            rows.append({
                "route_key": route_key,
                "variant": validation.get("variant"),
                "passed": validation.get("passed") is True,
                "holdout_pnl": float(validation.get("holdout_pnl") or 0.0),
                "pnl": float(validation.get("pnl") or 0.0),
                "trades": int(validation.get("trades") or 0),
                "resolution_source": "walk_forward_route_validation",
            })
    return rows


def _parse_iso_date(value: Any) -> date | None:
    try:
        raw = str(value or "")[:10]
        if not raw:
            return None
        return date.fromisoformat(raw)
    except Exception:
        return None


def _split_topology_blockers(protocol: dict) -> list[str]:
    phases = [
        ("train", protocol.get("train_windows") if isinstance(protocol.get("train_windows"), list) else []),
        ("validation", protocol.get("validation_windows") if isinstance(protocol.get("validation_windows"), list) else []),
        ("test", protocol.get("test_windows") if isinstance(protocol.get("test_windows"), list) else []),
    ]
    blockers = []
    bounds: dict[str, list[tuple[date, date]]] = {}
    for label, windows in phases:
        parsed = []
        for window in windows:
            if not isinstance(window, dict):
                blockers.append(f"{label}_window_invalid")
                continue
            start = _parse_iso_date(window.get("start"))
            end = _parse_iso_date(window.get("end"))
            if start is None or end is None or start > end:
                blockers.append(f"{label}_window_invalid")
                continue
            parsed.append((start, end))
        parsed.sort(key=lambda item: item[0])
        for left, right in zip(parsed, parsed[1:]):
            if right[0] <= left[1]:
                blockers.append(f"{label}_windows_overlap")
                break
        bounds[label] = parsed
    if not all(bounds.get(label) for label, _ in phases):
        return blockers
    embargo_days = int(float(protocol.get("purge_embargo_days") or 0.0)) if protocol.get("purged_embargoed") is True else 0
    ordered = ["train", "validation", "test"]
    for left_label, right_label in zip(ordered, ordered[1:]):
        left_end = max(end for _, end in bounds[left_label])
        right_start = min(start for start, _ in bounds[right_label])
        gap = (right_start - left_end).days
        if gap <= embargo_days:
            blockers.append(f"{left_label}_to_{right_label}_embargo_violation")
    return blockers


def _historical_learning_protocol(
    rows: list[dict],
    active: dict,
    min_trades: int = 100,
    external_protocol: dict | None = None,
) -> dict:
    day_sources = []
    for row in list(rows or []) + ([active] if isinstance(active, dict) else []):
        by_day = row.get("by_day") if isinstance(row, dict) and isinstance(row.get("by_day"), dict) else {}
        for day, stats in by_day.items():
            if isinstance(stats, dict):
                day_sources.append({
                    "day": str(day),
                    "pnl": round(float(stats.get("pnl") or 0.0), 4),
                    "trades": int(stats.get("trades") or 0),
                })
    by_day = {}
    for item in day_sources:
        day = item["day"]
        bucket = by_day.setdefault(day, {"pnl": 0.0, "trades": 0})
        bucket["pnl"] += float(item["pnl"] or 0.0)
        bucket["trades"] += int(item["trades"] or 0)
    days = sorted(by_day)
    train_end = max(0, int(len(days) * 0.6))
    validation_end = max(train_end, int(len(days) * 0.8))
    train_days = days[:train_end]
    validation_days = days[train_end:validation_end]
    test_days = days[validation_end:]

    def _window(label: str, window_days: list[str]) -> dict:
        trades = sum(int((by_day.get(day) or {}).get("trades") or 0) for day in window_days)
        pnl = sum(float((by_day.get(day) or {}).get("pnl") or 0.0) for day in window_days)
        return {
            "label": label,
            "start": window_days[0] if window_days else None,
            "end": window_days[-1] if window_days else None,
            "day_count": len(window_days),
            "trade_count": trades,
            "pnl": round(pnl, 4),
        }

    train_window = _window("train", train_days)
    validation_window = _window("validation", validation_days)
    test_window = _window("test", test_days)
    external_protocol = external_protocol if isinstance(external_protocol, dict) else {}
    blockers = []
    if train_window["day_count"] < 5:
        blockers.append("insufficient_train_days")
    if validation_window["day_count"] < 2:
        blockers.append("insufficient_validation_days")
    if test_window["day_count"] < 2:
        blockers.append("insufficient_test_days")
    if test_window["trade_count"] < int(min_trades or 100):
        blockers.append("insufficient_test_trades")
    generated = {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "available_day_count": len(days),
        "train_windows": [train_window] if train_days else [],
        "validation_windows": [validation_window] if validation_days else [],
        "test_windows": [test_window] if test_days else [],
        "frozen_test_set": True,
        "purged_embargoed": True,
        "purge_embargo_days": 1,
        "test_reuse_policy": "retire_after_use",
        "split_enforced": False,
        "search_touched_test_set": True,
        "generated_blockers": blockers,
        "deduction": (
            "The hunter can now expose chronological train/validation/test evidence, but the current "
            "score-only search still ranks against the whole compiled tape. World-best certification "
            "requires precommitting this split before candidate search and never touching the retired "
            "test window during selection."
        ),
    }
    if external_protocol:
        for key in (
            "train_windows",
            "validation_windows",
            "test_windows",
            "frozen_test_set",
            "purged_embargoed",
            "purge_embargo_days",
            "test_reuse_policy",
            "split_enforced",
            "search_touched_test_set",
            "external_artifact_path",
            "external_artifact_sha256",
        ):
            if key in external_protocol:
                generated[key] = external_protocol[key]
        generated["external_protocol_loaded"] = True
    else:
        generated["external_protocol_loaded"] = False

    proof_blockers = []
    if not generated.get("train_windows"):
        proof_blockers.append("missing_train_windows")
    if not generated.get("validation_windows"):
        proof_blockers.append("missing_validation_windows")
    if not generated.get("test_windows"):
        proof_blockers.append("missing_test_windows")
    if generated.get("frozen_test_set") is not True:
        proof_blockers.append("test_set_not_marked_frozen")
    if generated.get("purged_embargoed") is not True:
        proof_blockers.append("split_not_purged_embargoed")
    if generated.get("test_reuse_policy") != "retire_after_use":
        proof_blockers.append("test_reuse_policy_not_retire_after_use")
    if generated.get("split_enforced") is not True:
        proof_blockers.append("search_not_precommitted_to_split")
    if generated.get("search_touched_test_set") is not False:
        proof_blockers.append("search_touched_test_set")
    proof_blockers.extend(_split_topology_blockers(generated))
    generated["blockers"] = proof_blockers
    generated["ready_for_world_best_certification"] = not proof_blockers
    return generated


def _promotion_candidate_variant(champion_challenger: dict) -> str | None:
    challengers = [
        row for row in (champion_challenger or {}).get("challengers") or []
        if isinstance(row, dict)
    ]
    candidate = next((row for row in challengers if row.get("challenge_state") == "promotion_challenger"), challengers[0] if challengers else {})
    return candidate.get("variant")


def _promotion_candidate_identity(champion_challenger: dict) -> dict:
    challengers = [
        row for row in (champion_challenger or {}).get("challengers") or []
        if isinstance(row, dict)
    ]
    candidate = next((row for row in challengers if row.get("challenge_state") == "promotion_challenger"), challengers[0] if challengers else {})
    return {
        "variant": candidate.get("variant"),
        "family_key": candidate.get("family_key"),
        "lineage_id": candidate.get("lineage_id") or candidate.get("parent"),
    }


def _candidate_bound(candidate: dict, payload: dict) -> bool:
    if not candidate or not isinstance(payload, dict):
        return False
    if candidate.get("variant") and (payload.get("variant") == candidate.get("variant") or payload.get("candidate_variant") == candidate.get("variant")):
        return True
    if candidate.get("family_key") and payload.get("family_key") == candidate.get("family_key"):
        return True
    if candidate.get("lineage_id") and payload.get("lineage_id") == candidate.get("lineage_id"):
        return True
    return False


def _oos_profitability_report(
    walk_forward: dict,
    split_protocol: dict,
    min_trades: int = 100,
    candidate_variant: str | None = None,
) -> dict:
    validations = [
        row for row in (walk_forward or {}).get("validations") or []
        if isinstance(row, dict)
    ]
    if candidate_variant:
        validations = [row for row in validations if row.get("variant") == candidate_variant]
    execution_adjusted = bool(
        (walk_forward or {}).get("slippage_sensitivity_sweep")
        and (walk_forward or {}).get("spread_sensitivity_sweep")
    )
    holdout_pnls = [float(row.get("holdout_pnl") or 0.0) for row in validations]
    positive_count = sum(1 for pnl in holdout_pnls if pnl > 0.0)
    passed_count = sum(1 for row in validations if row.get("passed") is True)
    trades = [int(row.get("trades") or 0) for row in validations]
    net_oos_pnl = sum(holdout_pnls)
    positive_rate = (positive_count / len(validations) * 100.0) if validations else 0.0
    min_test_trades = min(trades) if trades else 0
    blockers = []
    if not validations:
        blockers.append("missing_candidate_walk_forward_validation_rows" if candidate_variant else "missing_walk_forward_validation_rows")
    if passed_count <= 0:
        blockers.append("no_passed_walk_forward_validations")
    if net_oos_pnl <= 0.0:
        blockers.append("non_positive_net_oos_pnl")
    if positive_rate < 60.0:
        blockers.append("positive_window_rate_below_60_pct")
    if min_test_trades < int(min_trades or 100):
        blockers.append("insufficient_min_test_trades")
    if not execution_adjusted:
        blockers.append("missing_slippage_or_spread_sensitivity")
    if (split_protocol or {}).get("split_enforced") is not True:
        blockers.append("historical_split_not_enforced_before_search")

    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "candidate_variant": candidate_variant,
        "validation_count": len(validations),
        "passed_validation_count": passed_count,
        "net_oos_pnl": round(net_oos_pnl, 4),
        "positive_window_count": positive_count,
        "positive_window_rate_pct": round(positive_rate, 4),
        "min_test_trades": min_test_trades,
        "execution_adjusted": execution_adjusted,
        "ready_for_world_best_certification": not blockers,
        "blockers": blockers,
        "deduction": (
            "This report converts walk-forward validation into a promotion-grade OOS ledger. Positive "
            "holdout alone is not enough: it must beat costs, pass at least one full validation row, "
            "carry enough trades, and be tied to a pre-enforced frozen split."
        ),
    }


def _world_class_learning_controls() -> dict:
    implemented = [
        "score_only_no_rebuild_contract",
        "quote_aware_exit_guard",
        "compiled_lineage_report",
        "candidate_role_separation",
        "raw_vs_gate_vs_holdout_candidate_separation",
        "gate_break_diagnosis",
        "adaptive_next_gate_manifest",
        "lane_learning_report",
        "duplicate_behavior_report",
        "behavior_shape_dedupe_for_winners",
        "score_cache_telemetry",
        "route_marginal_ablation",
        "persistent_route_learning_registry",
        "conflicted_route_evidence_tracking",
        "contextual_toxic_route_filtering",
        "toxic_addon_suspect_report",
        "multi_role_risk_concentration_reports",
        "chronological_holdout_validation_report",
        "leave_one_day_validation_report",
        "data_coverage_scope_report",
        "true_profit_target_progress_report",
        "failure_taxonomy_report",
        "closed_loop_next_hunt_command_packet",
        "negative_knowledge_suppression_packet",
        "route_prior_report",
        "controlled_sibling_plan",
        "experiment_contract_report",
        "learning_debt_report",
        "variant_dna_report",
        "family_survival_report",
        "false_discovery_pressure_report",
        "day_robustness_leaderboard",
        "promotion_evidence_gap_report",
        "uncertainty_heatmap_report",
        "repair_recipe_report",
        "search_budget_allocator_report",
        "belief_update_report",
        "learning_quality_scorecard",
        "lesson_half_life_report",
        "negative_falsification_queue",
        "promotion_power_report",
        "next_best_question_report",
        "hunt_governor_report",
        "regime_fingerprint_report",
        "candidate_lineage_report",
        "missed_winner_report",
        "search_space_coverage_report",
        "counterfactual_backlog_report",
        "artifact_trust_report",
        "experiment_sequencer_report",
        "route_overlap_matrix",
        "sample_efficiency_report",
        "stress_test_queue",
        "champion_challenger_report",
        "learning_ops_readiness_report",
        "pairwise_route_ablation_report",
        "triple_route_ablation_report",
        "shapley_route_contribution_estimates",
        "causal_ablation_proof_report",
        "proof_artifact_manifest",
        "proof_freshness_report",
        "proof_regime_match_report",
        "fill_quality_proof_report",
        "candidate_proof_dossier",
        "genetic_search_operator",
        "bandit_lane_allocator",
        "beam_search_route_set_optimizer",
        "slippage_sensitivity_sweep",
        "spread_sensitivity_sweep",
        "execution_adjusted_objective_report",
        "data_quality_contract_report",
        "statistical_validation_report",
        "closed_loop_causal_controller",
        "deployment_risk_report",
        "world_class_readiness_report",
        "worker_pool_parallel_batch_runner",
        "opportunity_feature_learner_report",
        "opportunity_target_model_report",
        "opportunity_rule_extraction_report",
        "variant_meta_model_report",
        "uncertainty_model_report",
        "teacher_student_learning_packet",
        "walk_forward_validation_bundle",
        "historical_learning_protocol",
        "oos_profitability_report",
        "promotion_ladder_report",
        "benchmark_superiority_report",
        "belief_calibration_report",
        "learning_control_plane",
        "outcome_attribution_report",
        "counterfactual_learning_report",
        "regime_conditioned_learning_report",
        "active_experiment_design_report",
        "learning_governance_report",
        "learning_depth_report",
        "enhanced_target_label_report",
        "lesson_survival_decay_report",
        "portfolio_learning_report",
        "uncertainty_risk_pricing_report",
        "promotion_evidence_hardening_report",
        "ops_hardening_report",
        "live_feedback_loop_report",
        "drift_monitoring_report",
        "rollback_kill_switch_report",
        "auditability_lineage_report",
        "end_to_end_readiness_gate",
        "elite_runbook_report",
        "seed_summary_lineage_report",
        "candidate_decision_brief_alignment_audit",
        "non_promotable_blockers",
        "promotion_quarantine_ready_classification",
        "frontier_recombination_lane",
        "frontier_override_of_broad_skip_coverage",
        "micro_filter_threshold_probe_lane",
        "portfolio_recombination_lane",
        "registry_guided_sampling_lane",
        "skip_probe_budget_throttling_in_learned_phase",
        "exact_seed_replay",
        "risk_and_validation_reports_for_failed_runs",
    ]
    external_data_required = [
        "more_historical_market_days",
        "news_or_earnings_calendar_tags",
        "live_fill_quality_history",
        "partial_fill_model_inputs",
        "future_out_of_sample_market_days",
    ]
    planned_not_blocking_score_only = []
    return {
        "implemented_count": len(implemented),
        "implemented": implemented,
        "external_data_required_count": len(external_data_required),
        "external_data_required": external_data_required,
        "planned_not_blocking_score_only_count": len(planned_not_blocking_score_only),
        "planned_not_blocking_score_only": planned_not_blocking_score_only,
        "deduction": (
            "These controls turn each future batch into a learning artifact: it separates candidate roles, "
            "diagnoses gate failures, records route evidence, validates across days, and keeps rebuilds out "
            "of the score-only hunt path."
        ),
    }


def _profit_combo_learning_report(
    rows: list[dict],
    leaderboard: list[dict],
    raw_leaderboard: list[dict],
    winners: list[dict],
    holdout_winners: list[dict],
    low_sample_positive: list[dict],
    active_full: dict,
    args: argparse.Namespace,
) -> dict:
    kept_rows = _dedupe_behavior(
        holdout_winners
        + winners
        + low_sample_positive[:100]
        + leaderboard[:100]
        + raw_leaderboard[:100]
    )
    cycle = {
        "cycle": 0,
        "hunter": "profit_combo",
        "seed": int(args.seed),
        "ok": True,
        "elapsed_sec": 0.0,
        "scored_total": len(rows),
        "winners": len(winners),
        "stdout_tail": json.dumps({
            "scored_total": len(rows),
            "winners": len(winners),
            "holdout_winners": len(holdout_winners),
            "raw_best": (raw_leaderboard[0] or {}).get("variant") if raw_leaderboard else None,
        }, sort_keys=True),
    }
    try:
        report = hunt_intel.learning_report(
            rows=kept_rows,
            all_rows=rows,
            cycles=[cycle],
            active_weights=active_full.get("weights") if isinstance(active_full.get("weights"), dict) else None,
            source="step2_profit_combo_hunter",
        )
    except Exception as exc:
        return {
            "schema_version": 1,
            "source": "step2_profit_combo_hunter",
            "error": str(exc),
            "kept_rows": len(kept_rows),
            "all_rows": len(rows),
        }
    report["profit_combo_learning_context"] = {
        "min_trades": int(args.min_trades),
        "target_pnl": float(args.target_pnl),
        "kept_rows": len(kept_rows),
        "all_rows": len(rows),
        "score_only": True,
        "hunter": "profit_combo",
    }
    return report


def _profit_combo_negative_knowledge(rows: list[dict], lane_report: dict) -> dict:
    patterns = []
    for lane, report in sorted((lane_report or {}).items()):
        variant_count = int(report.get("variant_count") or 0)
        if not variant_count:
            continue
        gate_winners = int(report.get("gate_holdout_positive_count") or 0)
        avg_pnl = float(report.get("avg_pnl") or 0.0)
        if gate_winners == 0 and avg_pnl < 0.0:
            patterns.append({
                "pattern_key": f"lane:{lane}:zero_gate_holdout_negative_avg",
                "route_key": lane,
                "reason": "lane_produced_no_gate_holdout_winners_and_negative_average_pnl",
                "severity": "high" if avg_pnl < -1000.0 else "medium",
                "variant_count": variant_count,
                "avg_pnl": round(avg_pnl, 4),
                "recommended_action": "reduce_budget_or_require_new_hypothesis",
            })
    for row in rows:
        pnl = float(row.get("step2_pnl") or 0.0)
        trades = int(row.get("step2_trades") or 0)
        holdout = _holdout_value(row)
        if trades >= 500 and pnl < -1000.0:
            lane = _lane_name(row)
            patterns.append({
                "pattern_key": f"variant:{_behavior_key(row)}:high_sample_loss",
                "route_key": lane,
                "variant": row.get("variant"),
                "reason": "high_sample_variant_lost_material_pnl",
                "severity": "high",
                "pnl": round(pnl, 4),
                "trades": trades,
                "holdout_pnl": round(holdout, 4),
                "recommended_action": "do_not_repeat_behavior_shape",
            })
    patterns.sort(key=lambda item: (
        {"high": 2, "medium": 1}.get(str(item.get("severity")), 0),
        abs(float(item.get("avg_pnl") or item.get("pnl") or 0.0)),
    ), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_profit_combo_hunter",
        "description": "Negative knowledge extracted from all scored profit-combo rows, including bad lanes and high-sample losses.",
        "patterns": patterns[:100],
        "avoid_routes": sorted({row.get("route_key") for row in patterns if row.get("severity") == "high" and row.get("route_key")}),
        "caution_routes": sorted({row.get("route_key") for row in patterns if row.get("severity") != "high" and row.get("route_key")}),
    }


def _learning_artifact_projection(learning: dict) -> dict:
    keys = [
        "experiment_memory",
        "bandit_allocation",
        "active_experiment_plan",
        "causal_experiment_registry",
        "experiment_debt_queue",
        "information_gain_scoring",
        "value_of_information_planner",
        "route_state_machine",
        "negative_knowledge_bank",
        "mutation_grammar_learner",
        "uncertainty_budgeting",
        "learning_conflict_resolver",
        "adaptive_hunt_throttle",
        "autonomous_hunt_planner",
        "runtime_guardrails",
        "human_readable_hunt_brief",
        "ab_route_experiment_executor",
        "adaptive_experiment_stopping",
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
        "meta_hunt_strategy_learner",
        "run_to_run_postmortem",
        "treatment_effects",
        "treatment_prior_model",
        "treatment_worker_budget",
        "treatment_confidence_model",
        "controlled_parent_sibling_experiments",
        "live_beater_failure_autopsy",
        "worker_specialization_memory",
        "regime_aware_learning",
        "promotion_reject_simulator",
        "search_portfolio_manager",
        "failure_taxonomy",
        "curriculum_state",
        "why_won_report",
        "stop_continue_criteria",
        "counterfactual_route_attribution_plan",
        "next_hunt_command_packet",
        "route_prior_report",
        "controlled_sibling_plan",
        "experiment_contract_report",
        "learning_debt_report",
        "variant_dna_report",
        "family_survival_report",
        "false_discovery_pressure_report",
        "day_robustness_leaderboard",
        "promotion_evidence_gap_report",
        "uncertainty_heatmap_report",
        "repair_recipe_report",
        "search_budget_allocator_report",
        "belief_update_report",
        "belief_calibration_report",
        "learning_control_plane",
        "outcome_attribution_report",
        "counterfactual_learning_report",
        "regime_conditioned_learning_report",
        "active_experiment_design_report",
        "learning_governance_report",
        "learning_depth_report",
        "enhanced_target_label_report",
        "lesson_survival_decay_report",
        "portfolio_learning_report",
        "uncertainty_risk_pricing_report",
        "promotion_evidence_hardening_report",
        "ops_hardening_report",
        "live_feedback_loop_report",
        "drift_monitoring_report",
        "rollback_kill_switch_report",
        "auditability_lineage_report",
        "end_to_end_readiness_gate",
        "elite_runbook_report",
        "learning_quality_scorecard",
        "lesson_half_life_report",
        "negative_falsification_queue",
        "promotion_power_report",
        "next_best_question_report",
        "hunt_governor_report",
        "regime_fingerprint_report",
        "candidate_lineage_report",
        "missed_winner_report",
        "search_space_coverage_report",
        "counterfactual_backlog_report",
        "artifact_trust_report",
        "experiment_sequencer_report",
        "route_overlap_matrix",
        "sample_efficiency_report",
        "stress_test_queue",
        "champion_challenger_report",
        "learning_ops_readiness_report",
        "pairwise_route_ablation_report",
        "triple_route_ablation_report",
        "shapley_route_contribution_estimates",
        "causal_ablation_proof_report",
        "proof_artifact_manifest",
        "proof_freshness_report",
        "proof_regime_match_report",
        "fill_quality_proof_report",
        "candidate_proof_dossier",
        "genetic_search_operator",
        "bandit_lane_allocator",
        "beam_search_route_set_optimizer",
        "slippage_sensitivity_sweep",
        "spread_sensitivity_sweep",
        "execution_adjusted_objective_report",
        "data_quality_contract_report",
        "statistical_validation_report",
        "closed_loop_causal_controller",
        "deployment_risk_report",
        "world_class_readiness_report",
        "worker_pool_parallel_batch_runner",
        "opportunity_feature_learner_report",
        "opportunity_target_model_report",
        "opportunity_rule_extraction_report",
        "variant_meta_model_report",
        "uncertainty_model_report",
        "teacher_student_learning_packet",
        "walk_forward_validation_bundle",
        "historical_learning_protocol",
        "oos_profitability_report",
        "promotion_ladder_report",
        "benchmark_superiority_report",
    ]
    return {key: learning.get(key) for key in keys if key in learning}


def _write_learning_artifacts(out_dir: Path, learning: dict) -> dict:
    artifact_paths = {}
    for key, payload in _learning_artifact_projection(learning).items():
        path = out_dir / f"{key}.json"
        _write_json(path, payload if isinstance(payload, dict) else {"value": payload})
        artifact_paths[key] = str(path.resolve())
    return artifact_paths


def _persist_learning_db(out_dir: Path, args: argparse.Namespace) -> dict:
    if not bool(getattr(args, "persist_learning_db", True)):
        return {"enabled": False, "reason": "disabled_by_cli"}
    try:
        db = step2_learning_db.Step2LearningDB(getattr(args, "learning_db", step2_learning_db.DEFAULT_DB))
        try:
            stats = db.ingest_run_dir(out_dir)
            packet = db.next_experiment_packet()
        finally:
            db.close()
        return {
            "enabled": True,
            "db": str(getattr(args, "learning_db", step2_learning_db.DEFAULT_DB)),
            "ingested_run_dir": str(out_dir.resolve()),
            "stats": stats,
            "next_experiment_packet": packet,
        }
    except Exception as exc:
        return {
            "enabled": True,
            "db": str(getattr(args, "learning_db", step2_learning_db.DEFAULT_DB)),
            "error": str(exc),
        }


def _lane_report(rows: list[dict]) -> dict:
    lanes: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        lanes[_lane_name(row)].append(row)
    report = {}
    for lane, lane_rows in sorted(lanes.items()):
        positive = [row for row in lane_rows if float(row.get("step2_pnl") or 0.0) > 0.0]
        holdout = [row for row in positive if (row.get("holdout_gate") or {}).get("ok")]
        gate = [row for row in positive if (row.get("min_trade_gate") or {}).get("ok")]
        holdout_gate = [row for row in gate if (row.get("holdout_gate") or {}).get("ok")]
        best = max(lane_rows, key=lambda item: float(item.get("step2_pnl") or -1e18), default=None)
        report[lane] = {
            "variant_count": len(lane_rows),
            "positive_count": len(positive),
            "gate_positive_count": len(gate),
            "holdout_positive_count": len(holdout),
            "gate_holdout_positive_count": len(holdout_gate),
            "distinct_gate_holdout_positive_count": len(_dedupe_behavior(holdout_gate)),
            "best": _row_brief(best),
            "avg_pnl": round(sum(float(row.get("step2_pnl") or 0.0) for row in lane_rows) / max(1, len(lane_rows)), 4),
        }
    return report


def _toxic_addon_report(rows: list[dict], args: argparse.Namespace) -> dict:
    suspects = []
    for row in rows:
        pnl = float(row.get("step2_pnl") or 0.0)
        trades = int(row.get("step2_trades") or 0)
        holdout = _holdout_value(row)
        if trades < max(1, int(args.min_trades) - 25):
            continue
        ticker_losses = [
            {"ticker": ticker, "pnl": float(value.get("pnl") or 0.0), "trades": int(value.get("trades") or 0)}
            for ticker, value in ((row.get("by_ticker") or {}).items() if isinstance(row.get("by_ticker"), dict) else [])
            if isinstance(value, dict) and float(value.get("pnl") or 0.0) < -50.0
        ]
        side_losses = [
            {"side": side, "pnl": float(value.get("pnl") or 0.0), "trades": int(value.get("trades") or 0)}
            for side, value in ((row.get("by_side") or {}).items() if isinstance(row.get("by_side"), dict) else [])
            if isinstance(value, dict) and float(value.get("pnl") or 0.0) < -50.0
        ]
        if pnl > 0.0 and holdout > 0.0 and not ticker_losses and not side_losses:
            continue
        route_suspects = []
        for audit in ((row.get("route_audit") or {}).get("routes") or []):
            if audit.get("route") == "fallback" or audit.get("action") == "skip":
                continue
            matched = int(audit.get("matched_opportunities") or 0)
            share = float(audit.get("opportunity_share_pct") or 0.0)
            if matched >= 80 or share >= 0.04:
                route_suspects.append({
                    "route": audit.get("route"),
                    "match": audit.get("match"),
                    "action": audit.get("action"),
                    "matched_opportunities": matched,
                    "opportunity_share_pct": share,
                })
        if ticker_losses or side_losses or route_suspects:
            suspects.append({
                "variant": row.get("variant"),
                "pnl": row.get("step2_pnl"),
                "trades": trades,
                "holdout_pnl": holdout,
                "lane": _lane_name(row),
                "ticker_losses": ticker_losses,
                "side_losses": side_losses,
                "suspect_routes": route_suspects[:10],
            })
    suspects.sort(key=lambda item: (item["holdout_pnl"], item["pnl"]))
    return {
        "deduction": (
            "Suspects are heuristic: routes are flagged when a near/frontier candidate fails P/L or holdout "
            "and has broad additive routes or severe ticker/side losses. Confirm by ablation before blacklisting."
        ),
        "suspect_count": len(suspects),
        "suspects": suspects[:50],
    }


def _score_cache_report(rows: list[dict], args: argparse.Namespace) -> dict:
    keys: dict[str, int] = defaultdict(int)
    examples: dict[str, Any] = {}
    decision_hashes: dict[str, int] = defaultdict(int)
    hit_count = 0
    miss_count = 0
    for row in rows:
        cache = row.get("score_cache") if isinstance(row.get("score_cache"), dict) else {}
        status = str(cache.get("status") or cache.get("cache_status") or "").lower()
        if status in {"hit", "cache_hit"} or cache.get("hit") is True:
            hit_count += 1
        elif status in {"miss", "cache_miss"} or cache.get("hit") is False:
            miss_count += 1
        decision_hash = cache.get("decision_hash") or row.get("decision_hash")
        if decision_hash:
            decision_hashes[str(decision_hash)] += 1
        for key, value in cache.items():
            text = f"{key}={value}"
            keys[text] += 1
            examples.setdefault(key, value)
    duplicate_decision_hashes = {
        key: count for key, count in sorted(decision_hashes.items(), key=lambda item: item[1], reverse=True)
        if count > 1
    }
    observed = len(rows)
    known_cache_events = hit_count + miss_count
    return {
        "enabled": bool(args.use_score_cache),
        "db": str(args.score_cache_db) if args.use_score_cache else None,
        "observed_rows": observed,
        "cache_hit_count": hit_count,
        "cache_miss_count": miss_count,
        "cache_hit_rate_pct": round(hit_count / known_cache_events * 100.0, 4) if known_cache_events else None,
        "distinct_decision_hash_count": len(decision_hashes),
        "duplicate_decision_hash_count": max(0, observed - len(decision_hashes)) if decision_hashes else 0,
        "top_duplicate_decision_hashes": dict(list(duplicate_decision_hashes.items())[:20]),
        "observed_fields": examples,
        "observed_field_counts": dict(sorted(keys.items())),
    }


def _seed_summary_report(paths: list[str]) -> list[dict]:
    out = []
    for raw_path in paths or []:
        payload = _read_json(raw_path)
        out.append({
            "path": str(raw_path),
            "exists": Path(raw_path).exists(),
            "sha256": _file_sha256(raw_path),
            "run_name": payload.get("run_name"),
            "min_trades": payload.get("min_trades"),
            "winner_count": payload.get("winner_count"),
            "winner_variant_count": payload.get("winner_variant_count"),
            "holdout_winner_count": payload.get("holdout_winner_count"),
            "holdout_winner_variant_count": payload.get("holdout_winner_variant_count"),
            "duplicate_behavior_count": payload.get("winner_duplicate_behavior_count"),
        })
    return out


def _selection_explanation(payload: dict) -> dict:
    best = payload.get("best") if isinstance(payload.get("best"), dict) else {}
    raw = payload.get("raw_best") if isinstance(payload.get("raw_best"), dict) else {}
    reasons = []
    if best and raw and best.get("variant") != raw.get("variant"):
        if not ((raw.get("min_trade_gate") or {}).get("ok")):
            reasons.append("raw_best_failed_min_trade_gate")
        if not ((raw.get("holdout_gate") or {}).get("ok")):
            reasons.append("raw_best_failed_holdout_gate")
        if float(best.get("profit_hunt_rank_score") or -1e18) > float(raw.get("profit_hunt_rank_score") or -1e18):
            reasons.append("best_has_higher_holdout_aware_rank")
    return {
        "best_variant": payload.get("best_variant"),
        "raw_best_variant": payload.get("raw_best_variant"),
        "best_differs_from_raw_best": bool(best and raw and best.get("variant") != raw.get("variant")),
        "reasons": reasons,
    }


def _next_step_guidance(frontier: dict, args: argparse.Namespace) -> dict:
    current = int(args.min_trades)
    highest = frontier.get("highest_holdout_positive_gate")
    failed = frontier.get("first_failed_gate_at_or_above_requested")
    if highest and highest >= current:
        next_gate = highest + 5 if highest < 325 else highest + 25
        mode = "advance_gate_carefully"
    elif highest:
        next_gate = min(current, highest + 5)
        mode = "consolidate_frontier"
    else:
        low = frontier.get("lowest_holdout_positive_gate") or frontier.get("lowest_positive_gate")
        next_gate = int(low) if low else max(50, current - 25)
        mode = "lower_gate_to_first_profitable_frontier" if low else "lower_gate_and_rebuild_seed_diversity"
    if failed and highest and failed - highest <= 10:
        next_gate = highest + max(1, (failed - highest) // 2)
        mode = "bisect_gate_break"
    return {
        "mode": mode,
        "suggested_next_min_trades": int(next_gate),
        "highest_holdout_positive_gate": highest,
        "first_failed_gate_at_or_above_requested": failed,
        "seed_preference": "newest_clean_distinct_holdout_positive_summaries_first",
    }


def _route_identity(route_payload: dict) -> str:
    return tournament_safety.stable_json_hash({
        "action": route_payload.get("action"),
        "match": route_payload.get("match") or {},
        "weights": route_payload.get("weights") or {},
        "bias": float(route_payload.get("bias") or 0.0),
    }, length=32)


def _variant_from_row_with_routes(row: dict, name: str, routes: list[dict]) -> Any:
    return routed.variant_from_dict({
        "name": name,
        "weights": row.get("weights") or {},
        "bias": float(row.get("bias") or 0.0),
        "routes": routes,
    })


def _route_ablation_report(
    compiled: dict,
    rows: list[dict],
    args: argparse.Namespace,
    active_full: dict,
    active_pnl: float,
) -> dict:
    budget = int(getattr(args, "analysis_score_budget", 0) or 0)
    if budget <= 0:
        return {"enabled": False, "reason": "analysis_score_budget_zero"}
    base_rows = []
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("routes"), list):
            continue
        if not row.get("routes"):
            continue
        if row in base_rows:
            continue
        base_rows.append(row)
    base_rows = _dedupe_behavior(base_rows)

    variants = []
    metadata = []
    for base in base_rows:
        if len(variants) >= budget:
            break
        routes = [route for route in (base.get("routes") or []) if isinstance(route, dict)]
        for idx, route_payload in enumerate(routes):
            if len(variants) >= budget:
                break
            if route_payload.get("action") == "skip":
                continue
            kept = [route for pos, route in enumerate(routes) if pos != idx]
            if not kept:
                continue
            name = f"ablate_{len(variants):04d}_{str(base.get('variant') or 'candidate')[:80]}"
            variants.append(_variant_from_row_with_routes(base, name, kept))
            metadata.append({
                "base_variant": base.get("variant"),
                "base_behavior_key": _behavior_key(base),
                "base_pnl": float(base.get("step2_pnl") or 0.0),
                "base_trades": int(base.get("step2_trades") or 0),
                "base_holdout_pnl": _holdout_value(base),
                "route_index": idx,
                "route": route_payload.get("name"),
                "route_identity": _route_identity(route_payload),
                "route_action": route_payload.get("action"),
                "route_match": route_payload.get("match") or {},
                "base_by_ticker": base.get("by_ticker") or {},
                "base_by_side": base.get("by_side") or {},
            })
    if not variants:
        return {"enabled": True, "scored": 0, "reason": "no_non_skip_routes_to_ablate"}

    scored = _score(compiled, variants, args)
    _decorate(scored, active_pnl, args)
    _attach_holdout_rank(scored, active_full)
    rows_out = []
    for meta, ablated in zip(metadata, scored):
        pnl_delta = round(meta["base_pnl"] - float(ablated.get("step2_pnl") or 0.0), 6)
        trade_delta = int(meta["base_trades"]) - int(ablated.get("step2_trades") or 0)
        holdout_delta = round(meta["base_holdout_pnl"] - _holdout_value(ablated), 6)
        ticker_deltas = {}
        for ticker in sorted(set((meta.get("base_by_ticker") or {}).keys()) | set((ablated.get("by_ticker") or {}).keys())):
            before = (meta.get("base_by_ticker") or {}).get(ticker) or {}
            after = (ablated.get("by_ticker") or {}).get(ticker) or {}
            ticker_deltas[ticker] = {
                "pnl_contribution": round(float(before.get("pnl") or 0.0) - float(after.get("pnl") or 0.0), 6),
                "trade_contribution": int(before.get("trades") or 0) - int(after.get("trades") or 0),
            }
        side_deltas = {}
        for side in sorted(set((meta.get("base_by_side") or {}).keys()) | set((ablated.get("by_side") or {}).keys())):
            before = (meta.get("base_by_side") or {}).get(side) or {}
            after = (ablated.get("by_side") or {}).get(side) or {}
            side_deltas[side] = {
                "pnl_contribution": round(float(before.get("pnl") or 0.0) - float(after.get("pnl") or 0.0), 6),
                "trade_contribution": int(before.get("trades") or 0) - int(after.get("trades") or 0),
            }
        classification = "neutral"
        if pnl_delta > 25.0 and holdout_delta >= -10.0:
            classification = "proven_good"
        elif pnl_delta < -25.0 or holdout_delta < -25.0:
            classification = "proven_toxic"
        rows_out.append({
            **meta,
            "ablated_variant": ablated.get("variant"),
            "ablated_pnl": ablated.get("step2_pnl"),
            "ablated_trades": ablated.get("step2_trades"),
            "ablated_holdout_pnl": _holdout_value(ablated),
            "pnl_contribution": pnl_delta,
            "trade_contribution": trade_delta,
            "holdout_contribution": holdout_delta,
            "ticker_contribution": ticker_deltas,
            "side_contribution": side_deltas,
            "classification": classification,
        })
    rows_out.sort(key=lambda item: (item["classification"] == "proven_toxic", -abs(float(item["pnl_contribution"]))), reverse=True)
    return {
        "enabled": True,
        "score_budget": budget,
        "scored": len(rows_out),
        "deduction": "Each row removes one non-skip route from a frontier candidate and rescores the full variant. Positive contribution means the removed route helped the base candidate; negative means removing it improved results.",
        "proven_good_count": sum(1 for row in rows_out if row["classification"] == "proven_good"),
        "proven_toxic_count": sum(1 for row in rows_out if row["classification"] == "proven_toxic"),
        "rows": rows_out[:200],
    }


def _update_route_learning_registry(registry_path: Path, ablation_report: dict) -> dict:
    registry = _read_json(registry_path)
    if not registry:
        registry = {"schema_version": 1, "routes": {}, "updated_runs": []}
    routes = registry.setdefault("routes", {})
    for row in ablation_report.get("rows") or []:
        key = row.get("route_identity")
        if not key:
            continue
        item = routes.setdefault(key, {
            "route_identity": key,
            "route": row.get("route"),
            "route_action": row.get("route_action"),
            "route_match": row.get("route_match"),
            "tested_count": 0,
            "proven_good_count": 0,
            "proven_toxic_count": 0,
            "pnl_contribution_sum": 0.0,
            "holdout_contribution_sum": 0.0,
            "trade_contribution_sum": 0,
            "examples": [],
        })
        item["tested_count"] += 1
        item["proven_good_count"] += 1 if row.get("classification") == "proven_good" else 0
        item["proven_toxic_count"] += 1 if row.get("classification") == "proven_toxic" else 0
        item["pnl_contribution_sum"] = round(float(item.get("pnl_contribution_sum") or 0.0) + float(row.get("pnl_contribution") or 0.0), 6)
        item["holdout_contribution_sum"] = round(float(item.get("holdout_contribution_sum") or 0.0) + float(row.get("holdout_contribution") or 0.0), 6)
        item["trade_contribution_sum"] = int(item.get("trade_contribution_sum") or 0) + int(row.get("trade_contribution") or 0)
        examples = item.setdefault("examples", [])
        examples.append({
            "base_variant": row.get("base_variant"),
            "classification": row.get("classification"),
            "pnl_contribution": row.get("pnl_contribution"),
            "holdout_contribution": row.get("holdout_contribution"),
            "trade_contribution": row.get("trade_contribution"),
        })
        item["examples"] = examples[-10:]
        tested = max(1, int(item["tested_count"]))
        item["avg_pnl_contribution"] = round(float(item["pnl_contribution_sum"]) / tested, 6)
        item["avg_holdout_contribution"] = round(float(item["holdout_contribution_sum"]) / tested, 6)
        good_count = int(item.get("proven_good_count") or 0)
        toxic_count = int(item.get("proven_toxic_count") or 0)
        item["good_rate_pct"] = round(good_count / tested * 100.0, 4)
        item["toxic_rate_pct"] = round(toxic_count / tested * 100.0, 4)
        item["conflict_count"] = min(good_count, toxic_count)
        item["conflict_status"] = "conflicted" if good_count and toxic_count else "one_sided"
        item["confidence"] = round(abs(good_count - toxic_count) / tested, 6)
        item["context_key"] = tournament_safety.stable_json_hash({
            "action": item.get("route_action"),
            "match": item.get("route_match") or {},
        }, length=24)
        if toxic_count > good_count:
            item["status"] = "toxic_candidate"
        elif good_count > toxic_count:
            item["status"] = "good_candidate"
        else:
            item["status"] = "mixed_or_neutral"
    registry["updated_at_epoch"] = time.time()
    _write_json(registry_path, registry)
    toxic = [item for item in routes.values() if item.get("status") == "toxic_candidate"]
    good = [item for item in routes.values() if item.get("status") == "good_candidate"]
    conflicted = [item for item in routes.values() if item.get("conflict_status") == "conflicted"]
    toxic.sort(key=lambda item: (item.get("proven_toxic_count", 0), -float(item.get("avg_pnl_contribution") or 0.0)), reverse=True)
    good.sort(key=lambda item: (item.get("proven_good_count", 0), float(item.get("avg_pnl_contribution") or 0.0)), reverse=True)
    conflicted.sort(key=lambda item: (item.get("conflict_count", 0), -float(item.get("confidence") or 0.0)), reverse=True)
    return {
        "path": str(registry_path.resolve()),
        "route_count": len(routes),
        "toxic_candidate_count": len(toxic),
        "good_candidate_count": len(good),
        "conflicted_route_count": len(conflicted),
        "top_toxic_candidates": toxic[:20],
        "top_good_candidates": good[:20],
        "top_conflicted_candidates": conflicted[:20],
    }


def _write_adaptive_gate_manifest(path: Path, payload: dict) -> dict:
    guidance = payload.get("next_step_guidance") if isinstance(payload.get("next_step_guidance"), dict) else {}
    manifest = {
        "schema_version": 1,
        "source_run": payload.get("run_name"),
        "current_min_trades": payload.get("min_trades"),
        "suggested_next_min_trades": guidance.get("suggested_next_min_trades"),
        "mode": guidance.get("mode"),
        "highest_holdout_positive_gate": guidance.get("highest_holdout_positive_gate"),
        "first_failed_gate_at_or_above_requested": guidance.get("first_failed_gate_at_or_above_requested"),
        "seed_preference": guidance.get("seed_preference"),
        "best_variant": payload.get("best_variant"),
        "best_pnl": payload.get("best_pnl"),
        "best_trades": payload.get("best_trades"),
        "frontier_report": payload.get("frontier_report"),
        "do_not_rebuild": True,
        "score_only": True,
    }
    _write_json(path, manifest)
    return {"path": str(path.resolve()), **manifest}


def _attach_robustness(rows: list[dict], active: dict, start_balance: float) -> None:
    for idx, row in enumerate(rows, start=1):
        try:
            row["robustness"] = candidate_robustness_report.evaluate_candidate(
                row,
                active_row=active,
                start_balance=float(start_balance),
                rank=idx,
            )
        except Exception as exc:
            row["robustness_error"] = str(exc)


def _audit_payload(payload: dict) -> list[dict]:
    issues = []
    score_only = payload.get("score_only_contract") if isinstance(payload.get("score_only_contract"), dict) else {}
    if not (score_only.get("score_only") and score_only.get("no_signal_rebuild") and score_only.get("no_compiled_tape_rebuild")):
        issues.append({"severity": "critical", "issue": "summary_missing_score_only_no_rebuild_contract"})
    if payload.get("exit_replay_model") != step2_quote_aware_guard.REQUIRED_EXIT_REPLAY_MODEL:
        issues.append({"severity": "critical", "issue": "summary_missing_quote_aware_exit_model"})
    if not payload.get("quote_aware_guard"):
        issues.append({"severity": "warning", "issue": "summary_missing_quote_aware_guard"})
    elif (payload.get("quote_aware_guard") or {}).get("ok") is not True:
        issues.append({"severity": "critical", "issue": "quote_aware_guard_not_ok", "actual": payload.get("quote_aware_guard")})
    if not payload.get("compiled_lineage_validation"):
        issues.append({"severity": "warning", "issue": "summary_missing_compiled_lineage_validation"})
    else:
        lineage = payload.get("compiled_lineage_validation") or {}
        if lineage.get("rebuild_required"):
            issues.append({"severity": "critical", "issue": "compiled_lineage_requires_rebuild"})
        elif lineage.get("certified") is not True:
            issues.append({
                "severity": "warning",
                "issue": "compiled_lineage_uncertified_score_only",
                "status": lineage.get("status"),
                "quick_score_allowed": lineage.get("quick_score_allowed"),
            })
    if not payload.get("score_cache"):
        issues.append({"severity": "warning", "issue": "summary_missing_score_cache_status"})
    if not payload.get("data_coverage_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_data_coverage_report"})
    else:
        fidelity_blockers = (payload.get("data_coverage_report") or {}).get("promotion_data_fidelity_blockers") or []
        if fidelity_blockers:
            issues.append({
                "severity": "critical",
                "issue": "promotion_data_fidelity_blocked",
                "blockers": fidelity_blockers,
            })
    if not payload.get("true_profit_target_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_true_profit_target_report"})
    if not payload.get("failure_taxonomy_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_failure_taxonomy_report"})
    if not payload.get("next_hunt_command_packet"):
        issues.append({"severity": "warning", "issue": "summary_missing_next_hunt_command_packet"})
    if not payload.get("route_prior_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_route_prior_report"})
    if not payload.get("controlled_sibling_plan"):
        issues.append({"severity": "warning", "issue": "summary_missing_controlled_sibling_plan"})
    if not payload.get("experiment_contract_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_experiment_contract_report"})
    if not payload.get("learning_debt_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_learning_debt_report"})
    if not payload.get("variant_dna_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_variant_dna_report"})
    if not payload.get("family_survival_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_family_survival_report"})
    if not payload.get("false_discovery_pressure_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_false_discovery_pressure_report"})
    if not payload.get("day_robustness_leaderboard"):
        issues.append({"severity": "warning", "issue": "summary_missing_day_robustness_leaderboard"})
    if not payload.get("promotion_evidence_gap_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_promotion_evidence_gap_report"})
    if not payload.get("uncertainty_heatmap_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_uncertainty_heatmap_report"})
    if not payload.get("repair_recipe_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_repair_recipe_report"})
    if not payload.get("search_budget_allocator_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_search_budget_allocator_report"})
    if not payload.get("belief_update_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_belief_update_report"})
    if not payload.get("belief_calibration_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_belief_calibration_report"})
    if not payload.get("learning_control_plane"):
        issues.append({"severity": "warning", "issue": "summary_missing_learning_control_plane"})
    if not payload.get("outcome_attribution_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_outcome_attribution_report"})
    if not payload.get("counterfactual_learning_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_counterfactual_learning_report"})
    if not payload.get("regime_conditioned_learning_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_regime_conditioned_learning_report"})
    if not payload.get("active_experiment_design_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_active_experiment_design_report"})
    if not payload.get("learning_governance_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_learning_governance_report"})
    if not payload.get("learning_depth_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_learning_depth_report"})
    if not payload.get("enhanced_target_label_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_enhanced_target_label_report"})
    if not payload.get("lesson_survival_decay_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_lesson_survival_decay_report"})
    if not payload.get("portfolio_learning_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_portfolio_learning_report"})
    if not payload.get("uncertainty_risk_pricing_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_uncertainty_risk_pricing_report"})
    if not payload.get("promotion_evidence_hardening_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_promotion_evidence_hardening_report"})
    if not payload.get("ops_hardening_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_ops_hardening_report"})
    if not payload.get("live_feedback_loop_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_live_feedback_loop_report"})
    if not payload.get("drift_monitoring_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_drift_monitoring_report"})
    if not payload.get("rollback_kill_switch_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_rollback_kill_switch_report"})
    if not payload.get("auditability_lineage_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_auditability_lineage_report"})
    if not payload.get("end_to_end_readiness_gate"):
        issues.append({"severity": "warning", "issue": "summary_missing_end_to_end_readiness_gate"})
    if not payload.get("elite_runbook_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_elite_runbook_report"})
    if not payload.get("learning_quality_scorecard"):
        issues.append({"severity": "warning", "issue": "summary_missing_learning_quality_scorecard"})
    if not payload.get("lesson_half_life_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_lesson_half_life_report"})
    if not payload.get("negative_falsification_queue"):
        issues.append({"severity": "warning", "issue": "summary_missing_negative_falsification_queue"})
    if not payload.get("promotion_power_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_promotion_power_report"})
    if not payload.get("next_best_question_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_next_best_question_report"})
    if not payload.get("hunt_governor_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_hunt_governor_report"})
    if not payload.get("regime_fingerprint_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_regime_fingerprint_report"})
    if not payload.get("candidate_lineage_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_candidate_lineage_report"})
    if not payload.get("missed_winner_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_missed_winner_report"})
    if not payload.get("search_space_coverage_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_search_space_coverage_report"})
    else:
        coverage = payload.get("search_space_coverage_report") or {}
        for lane in coverage.get("lane_coverage") or []:
            if lane.get("coverage_state") == "over_concentrated":
                issues.append({
                    "severity": "warning",
                    "issue": "search_lane_over_concentrated",
                    "lane": lane.get("lane"),
                    "coverage_pct": lane.get("coverage_pct"),
                    "recommended_fix": "seed_previous_positive_summaries_or_shift_budget_to_frontier_expansion",
                })
                break
    duplicate_report = payload.get("duplicate_behavior_report") if isinstance(payload.get("duplicate_behavior_report"), dict) else {}
    for label in ("low_sample_positive", "raw_leaderboard_top100", "leaderboard_top100"):
        section = duplicate_report.get(label) if isinstance(duplicate_report.get(label), dict) else {}
        variant_count = int(section.get("variant_count") or 0)
        duplicate_count = int(section.get("duplicate_behavior_count") or 0)
        if variant_count and duplicate_count / max(1, variant_count) >= 0.25:
            issues.append({
                "severity": "warning",
                "issue": "duplicate_behavior_pressure",
                "label": label,
                "duplicate_behavior_count": duplicate_count,
                "variant_count": variant_count,
                "duplicate_pct": round(duplicate_count / max(1, variant_count) * 100.0, 4),
                "recommended_fix": "increase distinct seeded/frontier variants before spending another batch on equivalent behavior",
            })
            break
    if not payload.get("counterfactual_backlog_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_counterfactual_backlog_report"})
    if not payload.get("artifact_trust_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_artifact_trust_report"})
    if not payload.get("experiment_sequencer_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_experiment_sequencer_report"})
    if not payload.get("route_overlap_matrix"):
        issues.append({"severity": "warning", "issue": "summary_missing_route_overlap_matrix"})
    if not payload.get("sample_efficiency_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_sample_efficiency_report"})
    if not payload.get("stress_test_queue"):
        issues.append({"severity": "warning", "issue": "summary_missing_stress_test_queue"})
    if not payload.get("champion_challenger_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_champion_challenger_report"})
    if not payload.get("learning_ops_readiness_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_learning_ops_readiness_report"})
    if not payload.get("pairwise_route_ablation_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_pairwise_route_ablation_report"})
    if not payload.get("triple_route_ablation_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_triple_route_ablation_report"})
    if not payload.get("shapley_route_contribution_estimates"):
        issues.append({"severity": "warning", "issue": "summary_missing_shapley_route_contribution_estimates"})
    if not payload.get("causal_ablation_proof_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_causal_ablation_proof_report"})
    if not payload.get("proof_artifact_manifest"):
        issues.append({"severity": "warning", "issue": "summary_missing_proof_artifact_manifest"})
    if not payload.get("proof_freshness_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_proof_freshness_report"})
    if not payload.get("proof_regime_match_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_proof_regime_match_report"})
    if not payload.get("fill_quality_proof_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_fill_quality_proof_report"})
    if not payload.get("candidate_proof_dossier"):
        issues.append({"severity": "warning", "issue": "summary_missing_candidate_proof_dossier"})
    if not payload.get("genetic_search_operator"):
        issues.append({"severity": "warning", "issue": "summary_missing_genetic_search_operator"})
    if not payload.get("bandit_lane_allocator"):
        issues.append({"severity": "warning", "issue": "summary_missing_bandit_lane_allocator"})
    if not payload.get("beam_search_route_set_optimizer"):
        issues.append({"severity": "warning", "issue": "summary_missing_beam_search_route_set_optimizer"})
    if not payload.get("slippage_sensitivity_sweep"):
        issues.append({"severity": "warning", "issue": "summary_missing_slippage_sensitivity_sweep"})
    if not payload.get("spread_sensitivity_sweep"):
        issues.append({"severity": "warning", "issue": "summary_missing_spread_sensitivity_sweep"})
    if not payload.get("statistical_validation_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_statistical_validation_report"})
    if not payload.get("closed_loop_causal_controller"):
        issues.append({"severity": "warning", "issue": "summary_missing_closed_loop_causal_controller"})
    if not payload.get("promotion_ladder_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_promotion_ladder_report"})
    if not payload.get("benchmark_superiority_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_benchmark_superiority_report"})
    if not payload.get("deployment_risk_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_deployment_risk_report"})
    if not payload.get("world_class_readiness_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_world_class_readiness_report"})
    elif (payload.get("world_class_readiness_report") or {}).get("ok") is not True:
        issues.append({
            "severity": "critical",
            "issue": "world_class_readiness_blocked",
            "blockers": (payload.get("world_class_readiness_report") or {}).get("blockers") or [],
        })
    if not payload.get("worker_pool_parallel_batch_runner"):
        issues.append({"severity": "warning", "issue": "summary_missing_worker_pool_parallel_batch_runner"})
    if not payload.get("opportunity_feature_learner_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_opportunity_feature_learner_report"})
    if not payload.get("opportunity_target_model_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_opportunity_target_model_report"})
    if not payload.get("opportunity_rule_extraction_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_opportunity_rule_extraction_report"})
    if not payload.get("variant_meta_model_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_variant_meta_model_report"})
    if not payload.get("uncertainty_model_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_uncertainty_model_report"})
    if not payload.get("teacher_student_learning_packet"):
        issues.append({"severity": "warning", "issue": "summary_missing_teacher_student_learning_packet"})
    if not payload.get("walk_forward_validation_bundle"):
        issues.append({"severity": "warning", "issue": "summary_missing_walk_forward_validation_bundle"})
    if not payload.get("world_class_learning_controls"):
        issues.append({"severity": "warning", "issue": "summary_missing_world_class_learning_controls"})
    if not payload.get("profit_combo_learning_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_profit_combo_learning_report"})
    if not payload.get("artifact_paths"):
        issues.append({"severity": "warning", "issue": "summary_missing_learning_artifact_paths"})
    learning_db = payload.get("learning_db") if isinstance(payload.get("learning_db"), dict) else {}
    if learning_db.get("enabled") and learning_db.get("error"):
        issues.append({"severity": "warning", "issue": "learning_db_ingest_failed", "error": learning_db.get("error")})
    if int(payload.get("scored_total") or 0) != int(payload.get("requested_variants") or 0):
        issues.append({"severity": "warning", "issue": "scored_total_differs_from_requested_variants"})
    for row in payload.get("winners") or []:
        if float(row.get("step2_pnl") or 0.0) <= float(payload.get("target_pnl") or 0.0):
            issues.append({"severity": "critical", "issue": "non_profitable_row_marked_winner", "variant": row.get("variant")})
        if not ((row.get("min_trade_gate") or {}).get("ok")):
            issues.append({"severity": "critical", "issue": "low_sample_row_marked_winner", "variant": row.get("variant")})
    if int(payload.get("winner_count") or 0) > 0 and float((payload.get("best") or {}).get("step2_pnl") or 0.0) <= float(payload.get("target_pnl") or 0.0):
        issues.append({"severity": "critical", "issue": "best_row_not_selected_from_profitable_winners"})
    if int(payload.get("winner_count") or 0) <= 0 and payload.get("low_sample_positive_count") and float((payload.get("best") or {}).get("step2_pnl") or 0.0) <= float(payload.get("target_pnl") or 0.0):
        issues.append({"severity": "critical", "issue": "failed_run_best_should_prefer_low_sample_profitable_row_over_negative_diagnostic"})
    if int(payload.get("holdout_winner_count") or 0) > 0 and not ((payload.get("best") or {}).get("holdout_gate") or {}).get("ok"):
        issues.append({"severity": "warning", "issue": "best_row_not_holdout_positive_despite_holdout_winners"})
    raw_best = payload.get("raw_best") or {}
    if float(raw_best.get("step2_pnl") or 0.0) == 0.0 and int(raw_best.get("step2_trades") or 0) == 0:
        issues.append({
            "severity": "info",
            "issue": "raw_pnl_best_is_no_trade_flatline",
            "fix_status": "kept_out_of_ranked_leaderboard_by_min_trade_gate",
        })
    for row in payload.get("leaderboard") or []:
        for audit in ((row.get("route_audit") or {}).get("routes") or []):
            if audit.get("route") == "fallback":
                continue
            if int(audit.get("matched_opportunities") or 0) <= 0:
                issues.append({
                    "severity": "warning",
                    "issue": "ranked_route_has_zero_attributed_opportunities",
                    "variant": row.get("variant"),
                    "route": audit.get("route"),
                })
                break
    brief_selection = payload.get("candidate_decision_brief_selection") if isinstance(payload.get("candidate_decision_brief_selection"), dict) else {}
    if brief_selection:
        selected = brief_selection.get("variant")
        expected = payload.get("best_variant")
        if selected and expected and selected != expected:
            issues.append({
                "severity": "critical",
                "issue": "candidate_decision_brief_selected_non_best_variant",
                "expected": expected,
                "actual": selected,
            })
    classification = payload.get("candidate_classification") if isinstance(payload.get("candidate_classification"), dict) else {}
    if classification.get("deployable_candidate") and not payload.get("recommended_count"):
        issues.append({"severity": "critical", "issue": "deployable_candidate_without_recommended_count"})
    if payload.get("promotable") and not classification.get("deployable_candidate"):
        issues.append({"severity": "critical", "issue": "promotable_flag_disagrees_with_candidate_classification"})
    if not payload.get("frontier_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_frontier_report"})
    if not payload.get("gate_break_diagnosis"):
        issues.append({"severity": "warning", "issue": "summary_missing_gate_break_diagnosis"})
    if not payload.get("lane_learning_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_lane_learning_report"})
    if not payload.get("toxic_addon_report"):
        issues.append({"severity": "warning", "issue": "summary_missing_toxic_addon_report"})
    marginal = payload.get("route_marginal_report") if isinstance(payload.get("route_marginal_report"), dict) else {}
    if not marginal:
        issues.append({"severity": "warning", "issue": "summary_missing_route_marginal_report"})
    elif marginal.get("enabled") and int(marginal.get("scored") or 0) <= 0:
        issues.append({"severity": "info", "issue": "route_marginal_report_scored_no_routes", "reason": marginal.get("reason")})
    if not payload.get("route_learning_registry"):
        issues.append({"severity": "warning", "issue": "summary_missing_route_learning_registry"})
    if not payload.get("risk_concentration_reports"):
        issues.append({"severity": "warning", "issue": "summary_missing_multi_role_risk_reports"})
    if not payload.get("candidate_validation_reports"):
        issues.append({"severity": "warning", "issue": "summary_missing_candidate_validation_reports"})
    if not payload.get("adaptive_gate_manifest"):
        issues.append({"severity": "warning", "issue": "summary_missing_adaptive_gate_manifest"})
    return issues


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Quote-aware score-only profit-combo hunter.")
    ap.add_argument("--compiled-decision-tape", required=True)
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT))
    ap.add_argument("--name", default="quote_aware_profit_combo")
    ap.add_argument("--batch-size", type=int, default=500)
    ap.add_argument("--seed", type=int, default=20260511)
    ap.add_argument("--seed-summary", action="append", default=[],
                    help="Prior profit-hunter summary.json to combine positive pocket routes from.")
    ap.add_argument("--start-balance", type=float, default=100000.0)
    ap.add_argument("--target-pnl", type=float, default=0.0)
    ap.add_argument("--true-profit-target-pct", type=float, default=30.0,
                    help="Long-range true-profit target as a percent of start balance for progress reporting.")
    ap.add_argument("--min-trades", type=int, default=250)
    ap.add_argument("--min-bucket-opportunities", type=int, default=50)
    ap.add_argument("--score-cache-db", default=str(step2_score_cache.DEFAULT_CACHE_DB))
    ap.add_argument("--no-score-cache", dest="use_score_cache", action="store_false")
    ap.set_defaults(use_score_cache=True)
    ap.add_argument("--max-trades-per-day", type=int, default=0)
    ap.add_argument("--max-trades-per-ticker-day", type=int, default=0)
    ap.add_argument("--allow-uncertified-cache", action="store_true")
    ap.add_argument("--analysis-score-budget", type=int, default=80,
                    help="Extra score-only variants reserved for exact route ablation analysis.")
    ap.add_argument("--no-route-ablation", dest="route_ablation", action="store_false")
    ap.set_defaults(route_ablation=True)
    ap.add_argument("--learning-db", default=str(step2_learning_db.DEFAULT_DB),
                    help="Persistent Step 2 learning DB to update with this score-only hunt.")
    ap.add_argument("--no-persist-learning-db", dest="persist_learning_db", action="store_false",
                    help="Write learning artifacts but do not ingest this run into the persistent learning DB.")
    ap.set_defaults(persist_learning_db=True)
    ap.add_argument("--historical-split-protocol",
                    help="JSON proof that candidate search used frozen, purged train/validation/test splits.")
    ap.add_argument("--forward-promotion-feedback",
                    help="JSON proof for shadow, paper, and tiny-live forward promotion stages.")
    ap.add_argument("--external-benchmark-file",
                    help="JSON list/object of external baselines including random-entry and current-live comparators.")
    ap.add_argument("--causal-ablation-file",
                    help="JSON list/object of actual removed-route or A/B ablation tests with lift after costs.")
    ap.add_argument("--calibration-feedback-file",
                    help="JSON proof with resolved predictions, route validation rows, promotion feedback, or command outcomes.")
    ap.add_argument("--fill-quality-file",
                    help="JSON proof with real/paper fill rows, slippage, latency, rejection, and fill-adjusted PnL.")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    started = time.time()
    out_dir = Path(args.out_dir) / args.name
    out_dir.mkdir(parents=True, exist_ok=True)
    historical_split_input = _load_historical_split_protocol(getattr(args, "historical_split_protocol", None))
    forward_promotion_feedback = _load_forward_promotion_feedback(getattr(args, "forward_promotion_feedback", None))
    external_benchmarks = _load_external_benchmarks(getattr(args, "external_benchmark_file", None))
    actual_ablation_tests = _load_actual_ablation_tests(getattr(args, "causal_ablation_file", None))
    calibration_feedback = _load_calibration_feedback(getattr(args, "calibration_feedback_file", None))
    fill_quality_feedback = _load_fill_quality_feedback(getattr(args, "fill_quality_file", None))
    proof_artifact_manifest = _proof_artifact_manifest(
        historical_split_input,
        forward_promotion_feedback,
        external_benchmarks,
        actual_ablation_tests,
        calibration_feedback,
        fill_quality_feedback,
    )
    proof_freshness_report = _proof_freshness_report(proof_artifact_manifest)
    compiled = decision_tape_compiled.load_compiled(
        args.compiled_decision_tape,
        mmap=True,
        validate_sources=not args.allow_uncertified_cache,
        prefer_day_shards=True,
        allow_historical_day_shard_score=True,
    )
    manifest = compiled.get("manifest") if isinstance(compiled.get("manifest"), dict) else {}
    step2_quote_aware_guard.assert_manifest(manifest, context="step2_profit_combo_hunter")
    exit_model = step2_quote_aware_guard.manifest_exit_replay_model(manifest)
    quote_aware_guard = step2_quote_aware_guard.manifest_gate(manifest)
    lineage_validation = manifest.get("lineage_validation") if isinstance(manifest.get("lineage_validation"), dict) else {}
    active_variant = _active_variant()
    active_row = _score(compiled, [active_variant], args)[0]
    active_full = {
        "step2_pnl": float(active_row.get("step2_pnl") or 0.0),
        "step2_trades": int(active_row.get("step2_trades") or 0),
        "step2_wins": int(active_row.get("step2_wins") or 0),
        "step2_losses": int(active_row.get("step2_losses") or 0),
        "step2_win_rate_pct": active_row.get("step2_win_rate_pct"),
        "by_ticker": active_row.get("by_ticker"),
        "by_day": active_row.get("by_day"),
        "by_side": active_row.get("by_side"),
        "skipped": active_row.get("skipped"),
        "exit_replay_model": active_row.get("exit_replay_model"),
        "weights": dict(getattr(active_variant, "weights", {}) or {}),
        "bias": float(getattr(active_variant, "bias", 0.0) or 0.0),
    }
    active_pnl = float(active_full["step2_pnl"])
    variants, bucket_learning = _build_variants(compiled, active_variant, args)
    rows = _score(compiled, variants, args)
    _decorate(rows, active_pnl, args)
    _attach_holdout_rank(rows, active_full)
    raw_leaderboard = sorted(rows, key=lambda row: float(row.get("step2_pnl") or 0.0), reverse=True)
    leaderboard = sorted(rows, key=lambda row: float(row.get("profit_hunt_rank_score") or -1e18), reverse=True)
    winner_variants = [row for row in leaderboard if row.get("beats_target_pnl")]
    winners = _dedupe_behavior(winner_variants)
    holdout_winner_variants = [row for row in winner_variants if (row.get("holdout_gate") or {}).get("ok")]
    holdout_winners = _dedupe_behavior(holdout_winner_variants)
    low_sample_positive_variants = [
        row for row in raw_leaderboard
        if float(row.get("step2_pnl") or 0.0) > float(args.target_pnl) and not (row.get("min_trade_gate") or {}).get("ok")
    ]
    low_sample_positive = _dedupe_behavior(low_sample_positive_variants)
    robustness_rows = _dedupe_behavior(
        leaderboard[:100]
        + raw_leaderboard[:100]
        + winners
        + holdout_winners
        + low_sample_positive[:50]
    )
    _attach_robustness(robustness_rows, active_full, float(args.start_balance))
    recommended = [
        row for row in winners
        if ((row.get("robustness") or {}).get("recommended") is True)
    ]
    raw_best = raw_leaderboard[0] if raw_leaderboard else None
    candidate_roles = _select_candidate_roles(
        winners=winners,
        holdout_winners=holdout_winners,
        low_sample_positive=low_sample_positive,
        leaderboard=leaderboard,
        raw_leaderboard=raw_leaderboard,
    )
    best = candidate_roles.get("selected")
    frontier = _frontier_report(rows, args)
    duplicate_report = {
        "winners": _behavior_duplicate_report(winner_variants, "winners"),
        "holdout_winners": _behavior_duplicate_report(holdout_winner_variants, "holdout_winners"),
        "low_sample_positive": _behavior_duplicate_report(low_sample_positive_variants, "low_sample_positive"),
        "leaderboard_top100": _behavior_duplicate_report(leaderboard[:100], "leaderboard_top100"),
        "raw_leaderboard_top100": _behavior_duplicate_report(raw_leaderboard[:100], "raw_leaderboard_top100"),
    }
    best_robustness = (best.get("robustness") if isinstance(best, dict) and isinstance(best.get("robustness"), dict) else {})
    best_blockers = (best_robustness.get("recommendation") or {}).get("blockers") or best_robustness.get("red_flags") or []
    ablation_bases = _dedupe_behavior(
        ([best] if isinstance(best, dict) else [])
        + holdout_winners
        + winners
        + low_sample_positive[:10]
        + raw_leaderboard[:10]
    )
    route_marginal_report = (
        _route_ablation_report(compiled, ablation_bases, args, active_full, active_pnl)
        if getattr(args, "route_ablation", True)
        else {"enabled": False, "reason": "disabled_by_cli"}
    )
    learning_report = _profit_combo_learning_report(
        rows,
        leaderboard,
        raw_leaderboard,
        winners,
        holdout_winners,
        low_sample_positive,
        active_full,
        args,
    )
    lane_learning_report = _lane_report(rows)
    negative_knowledge = _profit_combo_negative_knowledge(rows, lane_learning_report)
    true_profit_target_report = _true_profit_target_report(rows, args)
    failure_taxonomy_report = _failure_taxonomy_report(rows, args)
    route_prior_report = _route_prior_report(rows)
    controlled_sibling_plan = _controlled_sibling_plan(rows, route_prior_report)
    experiment_contract_report = _experiment_contract_report(rows, failure_taxonomy_report, route_prior_report, args)
    learning_debt_report = _learning_debt_report(lane_learning_report, route_prior_report, failure_taxonomy_report)
    variant_dna_report = _variant_dna_report(rows)
    family_survival_report = _family_survival_report(rows)
    false_discovery_pressure_report = _false_discovery_pressure_report(rows, args)
    day_robustness_leaderboard = _day_robustness_leaderboard(rows)
    promotion_evidence_gap_report = _promotion_evidence_gap_report(rows, true_profit_target_report, args)
    uncertainty_heatmap_report = _uncertainty_heatmap_report(
        route_prior_report,
        failure_taxonomy_report,
        learning_debt_report,
        promotion_evidence_gap_report,
    )
    repair_recipe_report = _repair_recipe_report(
        failure_taxonomy_report,
        promotion_evidence_gap_report,
        controlled_sibling_plan,
        route_prior_report,
    )
    search_budget_allocator_report = _search_budget_allocator_report(
        args,
        route_prior_report,
        false_discovery_pressure_report,
        learning_debt_report,
        uncertainty_heatmap_report,
    )
    belief_update_report = _belief_update_report(route_prior_report, variant_dna_report, family_survival_report)
    lesson_half_life_report = _lesson_half_life_report(rows, route_prior_report, family_survival_report)
    negative_falsification_queue = _negative_falsification_queue(route_prior_report, negative_knowledge, failure_taxonomy_report)
    promotion_power_report = _promotion_power_report(rows, true_profit_target_report, args)
    next_best_question_report = _next_best_question_report(
        uncertainty_heatmap_report,
        repair_recipe_report,
        promotion_power_report,
        false_discovery_pressure_report,
    )
    learning_quality_scorecard = _learning_quality_scorecard({
        "score_only_contract": {"score_only": True},
        "quote_aware_guard": quote_aware_guard,
        "true_profit_target_report": true_profit_target_report,
        "failure_taxonomy_report": failure_taxonomy_report,
        "route_prior_report": route_prior_report,
        "experiment_contract_report": experiment_contract_report,
        "repair_recipe_report": repair_recipe_report,
        "search_budget_allocator_report": search_budget_allocator_report,
        "next_hunt_command_packet": {"score_only": True},
    })
    hunt_governor_report = _hunt_governor_report(
        false_discovery_pressure_report,
        learning_quality_scorecard,
        promotion_power_report,
        learning_debt_report,
        next_best_question_report,
    )
    regime_fingerprint_report = _regime_fingerprint_report(rows)
    candidate_lineage_report = _candidate_lineage_report(rows)
    missed_winner_report = _missed_winner_report(rows, args)
    search_space_coverage_report = _search_space_coverage_report(rows)
    counterfactual_backlog_report = _counterfactual_backlog_report(
        missed_winner_report,
        route_prior_report,
        repair_recipe_report,
        regime_fingerprint_report,
    )
    pairwise_route_ablation_report = _route_interaction_report(rows, 2, "pairwise_route_ablation")
    triple_route_ablation_report = _route_interaction_report(rows, 3, "triple_route_ablation")
    shapley_route_contribution_estimates = _shapley_route_contribution_report(rows)
    causal_ablation_proof_report = _causal_ablation_proof_report(
        pairwise_route_ablation_report,
        triple_route_ablation_report,
        shapley_route_contribution_estimates,
        actual_ablation_tests,
    )
    genetic_search_operator = _genetic_search_operator_report(candidate_lineage_report, route_prior_report, args)
    bandit_lane_allocator = _bandit_lane_allocator_report(rows, args)
    beam_search_route_set_optimizer = _beam_search_route_set_optimizer_report(route_prior_report, shapley_route_contribution_estimates, args)
    slippage_sensitivity_sweep = _pnl_haircut_sensitivity_report(rows, "slippage_sensitivity_sweep", [0.01, 0.03, 0.05, 0.10])
    spread_sensitivity_sweep = _pnl_haircut_sensitivity_report(rows, "spread_sensitivity_sweep", [0.02, 0.05, 0.10, 0.20])
    execution_adjusted_objective_report = step2_objective.objective_report(rows)
    execution_viability_gap_report = _execution_viability_gap_report(rows, args)
    data_quality_contract_report = step2_data_quality.report(rows, source="step2_profit_combo_hunter")
    statistical_validation_report = step2_statistical_validation.report(
        rows,
        min_trades=int(getattr(args, "min_trades", 100) or 100),
        source="step2_profit_combo_hunter",
    )
    closed_loop_causal_controller = step2_closed_loop_controller.controller_report(
        route_prior_model=route_prior_report,
        experiment_registry=experiment_contract_report,
        experiment_debt=learning_debt_report,
        closed_loop_memory={
            "action_league": (bandit_lane_allocator or {}).get("allocations") or [],
        },
        validation_rows=(walk_forward_validation_bundle or {}).get("validations") if "walk_forward_validation_bundle" in locals() else [],
        feedback_rows=[],
        batch_size=int(getattr(args, "batch_size", 500) or 500),
        limit=25,
    )
    worker_pool_parallel_batch_runner = _worker_pool_parallel_batch_runner_report(args)
    opportunity_target_model_report = _opportunity_target_model_report(compiled)
    opportunity_feature_learner_report = _opportunity_feature_learner_report(compiled)
    opportunity_rule_extraction_report = _opportunity_rule_extraction_report(compiled, opportunity_feature_learner_report)
    variant_meta_model_report = _variant_meta_model_report(rows, opportunity_rule_extraction_report, route_prior_report, args)
    uncertainty_model_report = _uncertainty_model_report(opportunity_feature_learner_report, variant_meta_model_report, route_prior_report)
    experiment_sequencer_report = _experiment_sequencer_report(
        counterfactual_backlog_report,
        next_best_question_report,
        search_budget_allocator_report,
        hunt_governor_report,
        args,
    )
    teacher_student_learning_packet = _teacher_student_packet(
        opportunity_feature_learner_report,
        opportunity_rule_extraction_report,
        variant_meta_model_report,
        uncertainty_model_report,
        experiment_sequencer_report,
        args,
    )
    route_overlap_matrix = _route_overlap_matrix(rows)
    sample_efficiency_report = _sample_efficiency_report(rows, args)
    stress_test_queue = _stress_test_queue(rows, regime_fingerprint_report, promotion_evidence_gap_report)
    walk_forward_validation_bundle = _walk_forward_validation_bundle(
        rows,
        active_full,
        stress_test_queue,
        slippage_sensitivity_sweep,
        spread_sensitivity_sweep,
    )
    historical_learning_protocol = _historical_learning_protocol(
        rows,
        active_full,
        int(getattr(args, "min_trades", 100) or 100),
        historical_split_input,
    )
    champion_challenger_report = _champion_challenger_report(rows, active_full, args)
    promotion_candidate_variant = _promotion_candidate_variant(champion_challenger_report)
    oos_profitability_report = _oos_profitability_report(
        walk_forward_validation_bundle,
        historical_learning_protocol,
        int(getattr(args, "min_trades", 100) or 100),
        promotion_candidate_variant,
    )
    walk_forward_validation_bundle["split_protocol"] = historical_learning_protocol
    walk_forward_validation_bundle["oos_profitability_report"] = oos_profitability_report
    benchmark_superiority_report = _benchmark_superiority_report(
        rows,
        active_full,
        external_benchmarks,
        promotion_candidate_variant,
    )
    fill_quality_proof_report = _fill_quality_proof_report(fill_quality_feedback, promotion_candidate_variant)
    proof_regime_match_report = _proof_regime_match_report(regime_fingerprint_report, {
        "historical_split_protocol": historical_split_input,
        "forward_promotion_feedback": forward_promotion_feedback,
        "calibration_feedback": calibration_feedback,
        "fill_quality_feedback": fill_quality_feedback,
    })
    promotion_ladder_report = _promotion_ladder_report(
        historical_learning_protocol,
        oos_profitability_report,
        champion_challenger_report,
        forward_promotion_feedback,
    )
    calibration_validation_rows = _calibration_validation_rows(walk_forward_validation_bundle)
    calibration_validation_inputs = calibration_validation_rows + (calibration_feedback.get("validation_rows") or [])
    belief_calibration_report = step2_belief_calibration.report_from_beliefs(
        belief_update_report,
        run_id=str(args.name),
        validation_rows=calibration_validation_inputs,
        feedback_rows=calibration_feedback.get("feedback_rows") or [],
        command_outcomes=calibration_feedback.get("command_outcomes") or [],
        additional_predictions=calibration_feedback.get("additional_predictions") or [],
        source="step2_profit_combo_hunter",
    )
    belief_calibration_report["validation_resolution_input_count"] = len(calibration_validation_rows)
    belief_calibration_report["external_validation_resolution_input_count"] = len(calibration_feedback.get("validation_rows") or [])
    belief_calibration_report["external_feedback_row_count"] = len(calibration_feedback.get("feedback_rows") or [])
    belief_calibration_report["external_command_outcome_count"] = len(calibration_feedback.get("command_outcomes") or [])
    belief_calibration_report["external_prediction_count"] = len(calibration_feedback.get("additional_predictions") or [])
    candidate_proof_dossier = _candidate_proof_dossier(
        promotion_candidate_variant,
        historical_protocol=historical_learning_protocol,
        oos_report=oos_profitability_report,
        causal_report=causal_ablation_proof_report,
        promotion_ladder=promotion_ladder_report,
        benchmark_report=benchmark_superiority_report,
        fill_quality_report=fill_quality_proof_report,
        proof_manifest=proof_artifact_manifest,
        proof_freshness=proof_freshness_report,
        proof_regime_match=proof_regime_match_report,
        calibration_report=belief_calibration_report,
    )
    deployment_risk_report = step2_deployment_risk.report(
        rows,
        start_balance=float(getattr(args, "start_balance", step2_deployment_risk.DEFAULT_START_BALANCE) or step2_deployment_risk.DEFAULT_START_BALANCE),
        closed_loop_controller=closed_loop_causal_controller,
        source="step2_profit_combo_hunter",
    )
    data_coverage_report = _data_coverage_report(compiled, manifest)
    readiness_context = {
        "data_coverage_report": data_coverage_report,
        "data_quality_report": data_quality_contract_report,
        "statistical_validation_report": statistical_validation_report,
        "deployment_risk_report": deployment_risk_report,
        "execution_adjusted_objective_report": execution_adjusted_objective_report,
        "closed_loop_causal_controller": closed_loop_causal_controller,
        "causal_ablation_proof_report": causal_ablation_proof_report,
        "proof_artifact_manifest": proof_artifact_manifest,
        "proof_freshness_report": proof_freshness_report,
        "proof_regime_match_report": proof_regime_match_report,
        "fill_quality_proof_report": fill_quality_proof_report,
        "candidate_proof_dossier": candidate_proof_dossier,
        "historical_learning_protocol": historical_learning_protocol,
        "oos_profitability_report": oos_profitability_report,
        "promotion_ladder_report": promotion_ladder_report,
        "benchmark_superiority_report": benchmark_superiority_report,
        "artifact_trust_report": {"pending": True},
        "learning_ops_readiness_report": {"pending": True},
    }
    world_class_readiness_report = step2_world_class_audit.readiness_report(
        rows,
        payload=readiness_context,
        required_sections=step2_world_class_audit.PROFIT_COMBO_SECTIONS,
        source="step2_profit_combo_hunter",
    )
    artifact_trust_report = _artifact_trust_report({
        "route_prior_report": route_prior_report,
        "failure_taxonomy_report": failure_taxonomy_report,
        "promotion_evidence_gap_report": promotion_evidence_gap_report,
        "search_budget_allocator_report": search_budget_allocator_report,
        "hunt_governor_report": hunt_governor_report,
        "regime_fingerprint_report": regime_fingerprint_report,
        "candidate_lineage_report": candidate_lineage_report,
        "counterfactual_backlog_report": counterfactual_backlog_report,
        "missed_winner_report": missed_winner_report,
        "search_space_coverage_report": search_space_coverage_report,
        "pairwise_route_ablation_report": pairwise_route_ablation_report,
        "triple_route_ablation_report": triple_route_ablation_report,
        "shapley_route_contribution_estimates": shapley_route_contribution_estimates,
        "causal_ablation_proof_report": causal_ablation_proof_report,
        "proof_artifact_manifest": proof_artifact_manifest,
        "proof_freshness_report": proof_freshness_report,
        "proof_regime_match_report": proof_regime_match_report,
        "fill_quality_proof_report": fill_quality_proof_report,
        "candidate_proof_dossier": candidate_proof_dossier,
        "genetic_search_operator": genetic_search_operator,
        "bandit_lane_allocator": bandit_lane_allocator,
        "beam_search_route_set_optimizer": beam_search_route_set_optimizer,
        "slippage_sensitivity_sweep": slippage_sensitivity_sweep,
        "spread_sensitivity_sweep": spread_sensitivity_sweep,
        "execution_adjusted_objective_report": execution_adjusted_objective_report,
        "execution_viability_gap_report": execution_viability_gap_report,
        "execution_positive_candidates": execution_viability_gap_report.get("execution_positive_candidates") or [],
        "data_quality_contract_report": data_quality_contract_report,
        "statistical_validation_report": statistical_validation_report,
        "closed_loop_causal_controller": closed_loop_causal_controller,
        "belief_calibration_report": belief_calibration_report,
        "deployment_risk_report": deployment_risk_report,
        "world_class_readiness_report": world_class_readiness_report,
        "worker_pool_parallel_batch_runner": worker_pool_parallel_batch_runner,
        "opportunity_target_model_report": opportunity_target_model_report,
        "opportunity_feature_learner_report": opportunity_feature_learner_report,
        "opportunity_rule_extraction_report": opportunity_rule_extraction_report,
        "variant_meta_model_report": variant_meta_model_report,
        "uncertainty_model_report": uncertainty_model_report,
        "teacher_student_learning_packet": teacher_student_learning_packet,
        "walk_forward_validation_bundle": walk_forward_validation_bundle,
        "historical_learning_protocol": historical_learning_protocol,
        "oos_profitability_report": oos_profitability_report,
        "promotion_ladder_report": promotion_ladder_report,
        "benchmark_superiority_report": benchmark_superiority_report,
    })
    learning_ops_readiness_report = _learning_ops_readiness_report(
        artifact_trust_report,
        hunt_governor_report,
        search_space_coverage_report,
        stress_test_queue,
    )
    readiness_context["artifact_trust_report"] = artifact_trust_report
    readiness_context["learning_ops_readiness_report"] = learning_ops_readiness_report
    world_class_readiness_report = step2_world_class_audit.readiness_report(
        rows,
        payload=readiness_context,
        required_sections=step2_world_class_audit.PROFIT_COMBO_SECTIONS,
        source="step2_profit_combo_hunter",
    )
    learning_control_plane = step2_learning_control_plane.learning_control_plane_report(
        rows,
        shapley_report=shapley_route_contribution_estimates,
        pairwise_report=pairwise_route_ablation_report,
        feature_learner_report=opportunity_feature_learner_report,
        counterfactual_backlog=counterfactual_backlog_report,
        missed_winner_report=missed_winner_report,
        stress_test_queue=stress_test_queue,
        regime_fingerprint_report=regime_fingerprint_report,
        belief_calibration_report=belief_calibration_report,
        experiment_sequencer_report=experiment_sequencer_report,
        data_quality_report=data_quality_contract_report,
        statistical_validation_report=statistical_validation_report,
        artifact_trust_report=artifact_trust_report,
        deployment_risk_report=deployment_risk_report,
        batch_size=int(getattr(args, "batch_size", 0) or 0),
        source="step2_profit_combo_hunter",
    )
    outcome_attribution_report = learning_control_plane["outcome_attribution_report"]
    counterfactual_learning_report = learning_control_plane["counterfactual_learning_report"]
    regime_conditioned_learning_report = learning_control_plane["regime_conditioned_learning_report"]
    active_experiment_design_report = learning_control_plane["active_experiment_design_report"]
    learning_governance_report = learning_control_plane["learning_governance_report"]
    learning_depth_report = step2_learning_depth.learning_depth_report(
        rows,
        true_profit_report=true_profit_target_report,
        lesson_half_life_report=lesson_half_life_report,
        family_survival_report=family_survival_report,
        belief_calibration_report=belief_calibration_report,
        outcome_attribution_report=outcome_attribution_report,
        uncertainty_heatmap_report=uncertainty_heatmap_report,
        uncertainty_model_report=uncertainty_model_report,
        deployment_risk_report=deployment_risk_report,
        promotion_evidence_gap_report=promotion_evidence_gap_report,
        statistical_validation_report=statistical_validation_report,
        world_class_readiness_report=world_class_readiness_report,
        walk_forward_validation_bundle=walk_forward_validation_bundle,
        source="step2_profit_combo_hunter",
    )
    enhanced_target_label_report = learning_depth_report["enhanced_target_label_report"]
    lesson_survival_decay_report = learning_depth_report["lesson_survival_decay_report"]
    portfolio_learning_report = learning_depth_report["portfolio_learning_report"]
    uncertainty_risk_pricing_report = learning_depth_report["uncertainty_risk_pricing_report"]
    promotion_evidence_hardening_report = learning_depth_report["promotion_evidence_hardening_report"]
    projected_artifact_paths = {
        key: str((out_dir / f"{key}.json").resolve())
        for key in (
            "belief_calibration_report",
            "learning_control_plane",
            "learning_depth_report",
            "deployment_risk_report",
            "world_class_readiness_report",
        )
    }
    ops_hardening_report = step2_ops_hardening.ops_hardening_report(
        closed_loop_controller=closed_loop_causal_controller,
        deployment_risk_report=deployment_risk_report,
        belief_calibration_report=belief_calibration_report,
        learning_control_plane=learning_control_plane,
        learning_depth_report=learning_depth_report,
        learning_governance_report=learning_governance_report,
        promotion_evidence_hardening_report=promotion_evidence_hardening_report,
        world_class_readiness_report=world_class_readiness_report,
        statistical_validation_report=statistical_validation_report,
        data_quality_report=data_quality_contract_report,
        artifact_paths=projected_artifact_paths,
        artifact_trust_report=artifact_trust_report,
        learning_ops_readiness_report=learning_ops_readiness_report,
        active_experiment_design_report=active_experiment_design_report,
        source="step2_profit_combo_hunter",
    )
    live_feedback_loop_report = ops_hardening_report["live_feedback_loop_report"]
    drift_monitoring_report = ops_hardening_report["drift_monitoring_report"]
    rollback_kill_switch_report = ops_hardening_report["rollback_kill_switch_report"]
    auditability_lineage_report = ops_hardening_report["auditability_lineage_report"]
    end_to_end_readiness_gate = ops_hardening_report["end_to_end_readiness_gate"]
    elite_runbook_report = ops_hardening_report["elite_runbook_report"]
    next_hunt_command_packet = _next_hunt_command_packet(
        frontier,
        lane_learning_report,
        negative_knowledge,
        true_profit_target_report,
        failure_taxonomy_report,
        args,
        false_discovery_pressure_report,
        promotion_evidence_gap_report,
        data_coverage_report,
    )
    learning_report["negative_knowledge_bank"] = negative_knowledge
    learning_report["failure_taxonomy_report"] = failure_taxonomy_report
    learning_report["route_prior_report"] = route_prior_report
    learning_report["controlled_sibling_plan"] = controlled_sibling_plan
    learning_report["experiment_contract_report"] = experiment_contract_report
    learning_report["learning_debt_report"] = learning_debt_report
    learning_report["variant_dna_report"] = variant_dna_report
    learning_report["family_survival_report"] = family_survival_report
    learning_report["false_discovery_pressure_report"] = false_discovery_pressure_report
    learning_report["day_robustness_leaderboard"] = day_robustness_leaderboard
    learning_report["promotion_evidence_gap_report"] = promotion_evidence_gap_report
    learning_report["uncertainty_heatmap_report"] = uncertainty_heatmap_report
    learning_report["repair_recipe_report"] = repair_recipe_report
    learning_report["search_budget_allocator_report"] = search_budget_allocator_report
    learning_report["belief_update_report"] = belief_update_report
    learning_report["belief_calibration_report"] = belief_calibration_report
    learning_report["learning_control_plane"] = learning_control_plane
    learning_report["outcome_attribution_report"] = outcome_attribution_report
    learning_report["counterfactual_learning_report"] = counterfactual_learning_report
    learning_report["regime_conditioned_learning_report"] = regime_conditioned_learning_report
    learning_report["active_experiment_design_report"] = active_experiment_design_report
    learning_report["learning_governance_report"] = learning_governance_report
    learning_report["learning_depth_report"] = learning_depth_report
    learning_report["enhanced_target_label_report"] = enhanced_target_label_report
    learning_report["lesson_survival_decay_report"] = lesson_survival_decay_report
    learning_report["portfolio_learning_report"] = portfolio_learning_report
    learning_report["uncertainty_risk_pricing_report"] = uncertainty_risk_pricing_report
    learning_report["promotion_evidence_hardening_report"] = promotion_evidence_hardening_report
    learning_report["ops_hardening_report"] = ops_hardening_report
    learning_report["live_feedback_loop_report"] = live_feedback_loop_report
    learning_report["drift_monitoring_report"] = drift_monitoring_report
    learning_report["rollback_kill_switch_report"] = rollback_kill_switch_report
    learning_report["auditability_lineage_report"] = auditability_lineage_report
    learning_report["end_to_end_readiness_gate"] = end_to_end_readiness_gate
    learning_report["elite_runbook_report"] = elite_runbook_report
    learning_report["learning_quality_scorecard"] = learning_quality_scorecard
    learning_report["lesson_half_life_report"] = lesson_half_life_report
    learning_report["negative_falsification_queue"] = negative_falsification_queue
    learning_report["promotion_power_report"] = promotion_power_report
    learning_report["next_best_question_report"] = next_best_question_report
    learning_report["hunt_governor_report"] = hunt_governor_report
    learning_report["regime_fingerprint_report"] = regime_fingerprint_report
    learning_report["candidate_lineage_report"] = candidate_lineage_report
    learning_report["missed_winner_report"] = missed_winner_report
    learning_report["search_space_coverage_report"] = search_space_coverage_report
    learning_report["counterfactual_backlog_report"] = counterfactual_backlog_report
    learning_report["artifact_trust_report"] = artifact_trust_report
    learning_report["experiment_sequencer_report"] = experiment_sequencer_report
    learning_report["route_overlap_matrix"] = route_overlap_matrix
    learning_report["sample_efficiency_report"] = sample_efficiency_report
    learning_report["stress_test_queue"] = stress_test_queue
    learning_report["champion_challenger_report"] = champion_challenger_report
    learning_report["learning_ops_readiness_report"] = learning_ops_readiness_report
    learning_report["pairwise_route_ablation_report"] = pairwise_route_ablation_report
    learning_report["triple_route_ablation_report"] = triple_route_ablation_report
    learning_report["shapley_route_contribution_estimates"] = shapley_route_contribution_estimates
    learning_report["causal_ablation_proof_report"] = causal_ablation_proof_report
    learning_report["proof_artifact_manifest"] = proof_artifact_manifest
    learning_report["proof_freshness_report"] = proof_freshness_report
    learning_report["proof_regime_match_report"] = proof_regime_match_report
    learning_report["fill_quality_proof_report"] = fill_quality_proof_report
    learning_report["candidate_proof_dossier"] = candidate_proof_dossier
    learning_report["genetic_search_operator"] = genetic_search_operator
    learning_report["bandit_lane_allocator"] = bandit_lane_allocator
    learning_report["beam_search_route_set_optimizer"] = beam_search_route_set_optimizer
    learning_report["slippage_sensitivity_sweep"] = slippage_sensitivity_sweep
    learning_report["spread_sensitivity_sweep"] = spread_sensitivity_sweep
    learning_report["execution_adjusted_objective_report"] = execution_adjusted_objective_report
    learning_report["execution_viability_gap_report"] = execution_viability_gap_report
    learning_report["data_quality_contract_report"] = data_quality_contract_report
    learning_report["statistical_validation_report"] = statistical_validation_report
    learning_report["closed_loop_causal_controller"] = closed_loop_causal_controller
    learning_report["deployment_risk_report"] = deployment_risk_report
    learning_report["world_class_readiness_report"] = world_class_readiness_report
    learning_report["worker_pool_parallel_batch_runner"] = worker_pool_parallel_batch_runner
    learning_report["opportunity_target_model_report"] = opportunity_target_model_report
    learning_report["opportunity_feature_learner_report"] = opportunity_feature_learner_report
    learning_report["opportunity_rule_extraction_report"] = opportunity_rule_extraction_report
    learning_report["variant_meta_model_report"] = variant_meta_model_report
    learning_report["uncertainty_model_report"] = uncertainty_model_report
    learning_report["teacher_student_learning_packet"] = teacher_student_learning_packet
    learning_report["walk_forward_validation_bundle"] = walk_forward_validation_bundle
    learning_report["historical_learning_protocol"] = historical_learning_protocol
    learning_report["oos_profitability_report"] = oos_profitability_report
    learning_report["promotion_ladder_report"] = promotion_ladder_report
    learning_report["benchmark_superiority_report"] = benchmark_superiority_report
    learning_report["next_hunt_command_packet"] = next_hunt_command_packet
    learning_artifact_paths = _write_learning_artifacts(out_dir, learning_report)
    payload = {
        "schema_version": 1,
        "script": "step2_profit_combo_hunter.py",
        "run_name": args.name,
        "score_only_contract": {
            "score_only": True,
            "no_signal_rebuild": True,
            "no_compiled_tape_rebuild": True,
            "full_rebuild_allowed": False,
            "requires_existing_compiled_decision_tape": True,
            "compiled_loader": "decision_tape_compiled.load_compiled",
            "deduction": "This hunter only loads an existing compiled decision tape and scores routed variants.",
        },
        "compiled_decision_tape": str(args.compiled_decision_tape),
        "compiled_tape_manifest": manifest,
        "compiled_lineage_validation": lineage_validation,
        "external_proof_inputs": {
            "historical_split_protocol_loaded": bool(historical_split_input),
            "forward_promotion_feedback_loaded": bool(forward_promotion_feedback),
            "external_benchmark_count": len(external_benchmarks),
            "actual_ablation_test_count": len(actual_ablation_tests),
            "calibration_feedback_loaded": bool(calibration_feedback),
            "external_calibration_prediction_count": len(calibration_feedback.get("additional_predictions") or []),
            "historical_split_protocol_path": historical_split_input.get("external_artifact_path") if historical_split_input else None,
            "forward_promotion_feedback_path": forward_promotion_feedback.get("external_artifact_path") if forward_promotion_feedback else None,
            "external_benchmark_file": str(Path(args.external_benchmark_file).resolve()) if getattr(args, "external_benchmark_file", None) else None,
            "causal_ablation_file": str(Path(args.causal_ablation_file).resolve()) if getattr(args, "causal_ablation_file", None) else None,
            "calibration_feedback_file": str(Path(args.calibration_feedback_file).resolve()) if getattr(args, "calibration_feedback_file", None) else None,
            "fill_quality_file": str(Path(args.fill_quality_file).resolve()) if getattr(args, "fill_quality_file", None) else None,
        },
        "quote_aware_guard": quote_aware_guard,
        "exit_replay_model": exit_model,
        "required_exit_replay_model": step2_quote_aware_guard.REQUIRED_EXIT_REPLAY_MODEL,
        "score_cache": {
            "enabled": bool(args.use_score_cache),
            "db": str(args.score_cache_db) if args.use_score_cache else None,
        },
        "start_balance": float(args.start_balance),
        "target_pnl": float(args.target_pnl),
        "target_formula": "absolute_target_pnl_strict_greater_than_target_and_min_trades",
        "min_trades": int(args.min_trades),
        "requested_variants": int(args.batch_size),
        "scored_total": len(rows),
        "cycles": [{
            "cycle": 0,
            "hunter": "profit_combo",
            "seed": int(args.seed),
            "ok": True,
            "elapsed_sec": round(time.time() - started, 4),
            "scored_total": len(rows),
            "winners": len(winners),
        }],
        "active": active_full,
        "active_step2_pnl": active_pnl,
        "active_baseline_rescored": {
            "rescored_in_this_run": True,
            "pnl": active_pnl,
            "trades": active_full.get("step2_trades"),
            "exit_replay_model": active_full.get("exit_replay_model"),
        },
        "world_class_learning_controls": _world_class_learning_controls(),
        "profit_combo_learning_report": learning_report,
        "negative_knowledge_bank": negative_knowledge,
        "failure_taxonomy_report": failure_taxonomy_report,
        "route_prior_report": route_prior_report,
        "controlled_sibling_plan": controlled_sibling_plan,
        "experiment_contract_report": experiment_contract_report,
        "learning_debt_report": learning_debt_report,
        "variant_dna_report": variant_dna_report,
        "family_survival_report": family_survival_report,
        "false_discovery_pressure_report": false_discovery_pressure_report,
        "day_robustness_leaderboard": day_robustness_leaderboard,
        "promotion_evidence_gap_report": promotion_evidence_gap_report,
        "uncertainty_heatmap_report": uncertainty_heatmap_report,
        "repair_recipe_report": repair_recipe_report,
        "search_budget_allocator_report": search_budget_allocator_report,
        "belief_update_report": belief_update_report,
        "belief_calibration_report": belief_calibration_report,
        "learning_control_plane": learning_control_plane,
        "outcome_attribution_report": outcome_attribution_report,
        "counterfactual_learning_report": counterfactual_learning_report,
        "regime_conditioned_learning_report": regime_conditioned_learning_report,
        "active_experiment_design_report": active_experiment_design_report,
        "learning_governance_report": learning_governance_report,
        "learning_depth_report": learning_depth_report,
        "enhanced_target_label_report": enhanced_target_label_report,
        "lesson_survival_decay_report": lesson_survival_decay_report,
        "portfolio_learning_report": portfolio_learning_report,
        "uncertainty_risk_pricing_report": uncertainty_risk_pricing_report,
        "promotion_evidence_hardening_report": promotion_evidence_hardening_report,
        "ops_hardening_report": ops_hardening_report,
        "live_feedback_loop_report": live_feedback_loop_report,
        "drift_monitoring_report": drift_monitoring_report,
        "rollback_kill_switch_report": rollback_kill_switch_report,
        "auditability_lineage_report": auditability_lineage_report,
        "end_to_end_readiness_gate": end_to_end_readiness_gate,
        "elite_runbook_report": elite_runbook_report,
        "learning_quality_scorecard": learning_quality_scorecard,
        "lesson_half_life_report": lesson_half_life_report,
        "negative_falsification_queue": negative_falsification_queue,
        "promotion_power_report": promotion_power_report,
        "next_best_question_report": next_best_question_report,
        "hunt_governor_report": hunt_governor_report,
        "regime_fingerprint_report": regime_fingerprint_report,
        "candidate_lineage_report": candidate_lineage_report,
        "missed_winner_report": missed_winner_report,
        "search_space_coverage_report": search_space_coverage_report,
        "counterfactual_backlog_report": counterfactual_backlog_report,
        "artifact_trust_report": artifact_trust_report,
        "experiment_sequencer_report": experiment_sequencer_report,
        "route_overlap_matrix": route_overlap_matrix,
        "sample_efficiency_report": sample_efficiency_report,
        "stress_test_queue": stress_test_queue,
        "champion_challenger_report": champion_challenger_report,
        "learning_ops_readiness_report": learning_ops_readiness_report,
        "pairwise_route_ablation_report": pairwise_route_ablation_report,
        "triple_route_ablation_report": triple_route_ablation_report,
        "shapley_route_contribution_estimates": shapley_route_contribution_estimates,
        "causal_ablation_proof_report": causal_ablation_proof_report,
        "proof_artifact_manifest": proof_artifact_manifest,
        "proof_freshness_report": proof_freshness_report,
        "proof_regime_match_report": proof_regime_match_report,
        "fill_quality_proof_report": fill_quality_proof_report,
        "candidate_proof_dossier": candidate_proof_dossier,
        "genetic_search_operator": genetic_search_operator,
        "bandit_lane_allocator": bandit_lane_allocator,
        "beam_search_route_set_optimizer": beam_search_route_set_optimizer,
        "slippage_sensitivity_sweep": slippage_sensitivity_sweep,
        "spread_sensitivity_sweep": spread_sensitivity_sweep,
        "execution_adjusted_objective_report": execution_adjusted_objective_report,
        "execution_viability_gap_report": execution_viability_gap_report,
        "execution_positive_candidates": execution_viability_gap_report.get("execution_positive_candidates") or [],
        "data_quality_contract_report": data_quality_contract_report,
        "statistical_validation_report": statistical_validation_report,
        "closed_loop_causal_controller": closed_loop_causal_controller,
        "deployment_risk_report": deployment_risk_report,
        "world_class_readiness_report": world_class_readiness_report,
        "worker_pool_parallel_batch_runner": worker_pool_parallel_batch_runner,
        "opportunity_target_model_report": opportunity_target_model_report,
        "opportunity_feature_learner_report": opportunity_feature_learner_report,
        "opportunity_rule_extraction_report": opportunity_rule_extraction_report,
        "variant_meta_model_report": variant_meta_model_report,
        "uncertainty_model_report": uncertainty_model_report,
        "teacher_student_learning_packet": teacher_student_learning_packet,
        "walk_forward_validation_bundle": walk_forward_validation_bundle,
        "historical_learning_protocol": historical_learning_protocol,
        "oos_profitability_report": oos_profitability_report,
        "promotion_ladder_report": promotion_ladder_report,
        "benchmark_superiority_report": benchmark_superiority_report,
        "true_profit_target_report": true_profit_target_report,
        "next_hunt_command_packet": next_hunt_command_packet,
        "artifact_paths": learning_artifact_paths,
        "data_coverage_report": data_coverage_report,
        "bucket_learning": bucket_learning,
        "seed_summary_inputs": _seed_summary_report(getattr(args, "seed_summary", [])),
        "variant_mix": {
            "skip_single": sum(1 for variant in variants if str(getattr(variant, "name", "")).startswith("profit_skip_single_")),
            "skip_combo": sum(1 for variant in variants if str(getattr(variant, "name", "")).startswith("profit_skip_combo_")),
            "rescue": sum(1 for variant in variants if str(getattr(variant, "name", "")).startswith("profit_rescue_")),
            "micro": sum(1 for variant in variants if str(getattr(variant, "name", "")).startswith("profit_micro_")),
            "edge_density": sum(1 for variant in variants if str(getattr(variant, "name", "")).startswith("profit_edge_density_")),
            "edge_bridge": sum(1 for variant in variants if str(getattr(variant, "name", "")).startswith("profit_edge_bridge_")),
            "registry_good": sum(1 for variant in variants if str(getattr(variant, "name", "")).startswith("profit_registry_good_")),
            "frontier": sum(1 for variant in variants if str(getattr(variant, "name", "")).startswith("profit_frontier_")),
            "boost": sum(1 for variant in variants if str(getattr(variant, "name", "")).startswith("profit_boost_")),
            "scaleup": sum(1 for variant in variants if str(getattr(variant, "name", "")).startswith("profit_scaleup_")),
            "portfolio": sum(1 for variant in variants if str(getattr(variant, "name", "")).startswith("profit_portfolio_")),
            "mixed": sum(1 for variant in variants if str(getattr(variant, "name", "")).startswith("profit_mixed_")),
        },
        "best": best,
        "best_role": candidate_roles.get("selected_role"),
        "candidate_roles": candidate_roles.get("briefs"),
        "best_gate_winner": candidate_roles.get("best_gate_winner"),
        "best_holdout_winner": candidate_roles.get("best_holdout_winner"),
        "best_profitable_any": candidate_roles.get("best_profitable_any"),
        "best_diagnostic_failure": candidate_roles.get("best_diagnostic_failure"),
        "best_variant": best.get("variant") if isinstance(best, dict) else None,
        "best_pnl": best.get("step2_pnl") if isinstance(best, dict) else None,
        "best_trades": best.get("step2_trades") if isinstance(best, dict) else None,
        "raw_best": raw_best,
        "raw_best_variant": raw_best.get("variant") if isinstance(raw_best, dict) else None,
        "raw_best_pnl": raw_best.get("step2_pnl") if isinstance(raw_best, dict) else None,
        "raw_best_trades": raw_best.get("step2_trades") if isinstance(raw_best, dict) else None,
        "winners": winners[:100],
        "winner_count": len(winners),
        "winner_variant_count": len(winner_variants),
        "winner_duplicate_behavior_count": max(0, len(winner_variants) - len(winners)),
        "holdout_winners": holdout_winners[:100],
        "holdout_winner_count": len(holdout_winners),
        "holdout_winner_variant_count": len(holdout_winner_variants),
        "holdout_winner_duplicate_behavior_count": max(0, len(holdout_winner_variants) - len(holdout_winners)),
        "recommended_leaderboard": recommended[:25],
        "recommended_count": len(recommended),
        "low_sample_positive": low_sample_positive[:25],
        "low_sample_positive_count": len(low_sample_positive),
        "low_sample_positive_variant_count": len(low_sample_positive_variants),
        "low_sample_positive_duplicate_behavior_count": max(0, len(low_sample_positive_variants) - len(low_sample_positive)),
        "leaderboard": leaderboard[:100],
        "raw_leaderboard": raw_leaderboard[:100],
        "duplicate_behavior_report": duplicate_report,
        "frontier_report": frontier,
        "gate_break_diagnosis": _gate_break_diagnosis(rows, frontier, args),
        "lane_learning_report": lane_learning_report,
        "toxic_addon_report": _toxic_addon_report(rows, args),
        "route_marginal_report": route_marginal_report,
        "risk_concentration_report": _concentration_report(best),
        "risk_concentration_reports": _risk_concentration_reports(candidate_roles),
        "candidate_validation_reports": _candidate_validation_reports(candidate_roles),
        "score_cache_observed": _score_cache_report(rows, args),
        "next_step_guidance": _next_step_guidance(frontier, args),
        "target_reached": bool(winners),
        "completed": True,
        "promotable": bool(recommended),
        "candidate_classification": {
            "research_candidate": bool(best and float(best.get("step2_pnl") or 0.0) > float(args.target_pnl)),
            "holdout_positive_research_candidate": bool(best and (best.get("holdout_gate") or {}).get("ok")),
            "deployable_candidate": bool(recommended),
            "candidate_role": candidate_roles.get("selected_role"),
            "min_trade_gate_ok": bool(best and (best.get("min_trade_gate") or {}).get("ok")),
            "report_only": True,
        },
        "non_promotable_blockers": best_blockers,
        "non_promotable_reason": (
            None if recommended
            else "no_recommended_candidate:" + ",".join(str(item) for item in best_blockers) if winners and best_blockers
            else "no_recommended_candidate" if winners
            else "no_profitable_min_trade_candidate"
        ),
        "learning_fixes_applied": [
            "no_trade_flatline_removed_from_ranked_leaderboard",
            "profit_winners_require_strict_positive_pnl",
            "profit_winners_require_min_trade_gate",
            "raw_pnl_leaderboard_kept_separate_from_trade_eligible_leaderboard",
            "all_ticker_skip_without_rescue_disallowed",
            "rescue_routes_test_trade_only_pockets_after_broad_skips",
            "broad_skip_routes_ordered_before_specific_routes_for_truthful_route_attribution",
            "batch_variant_slots_reserved_for_rescue_pocket_experiments",
            "routes_fully_overwritten_by_later_routes_pruned_before_scoring",
            "micro_filter_rescue_routes_probe_feature_thresholds_inside_profitable_pockets",
            "micro_filters_must_narrow_route_and_dedupe_identical_masks",
            "seed_summary_positive_pockets_combined_into_portfolio_variants",
            "portfolio_variants_can_combine_up_to_fourteen_positive_pockets_under_route_cap",
            "portfolio_generation_tests_all_non_short_and_strict_long_seed_lanes",
            "ranking_penalizes_negative_chronological_holdout_and_rewards_holdout_profit",
            "seed_positive_routes_prioritize_holdout_positive_pockets_before_total_pnl",
            "seeded_phase_allocates_most_budget_to_portfolio_recombination",
            "best_summary_row_prefers_top_profitable_winner_when_winners_exist",
            "summary_separates_total_winners_from_holdout_positive_winners",
            "portfolio_generation_adds_partial_fallback_lanes_to_scale_trade_sample",
            "exact_positive_seed_variants_are_replayed_before_mutating_routes",
            "exact_seed_replay_interleaves_holdout_frontier_and_total_pnl_frontier",
            "exact_seed_replay_ranks_candidates_globally_across_all_seed_summaries",
            "exact_seed_replay_dedupes_by_strategy_shape_and_strips_nested_seed_prefixes",
            "scaleup_portfolios_combine_holdout_positive_seed_rows_before_random_recombination",
            "near_gate_holdout_positive_seed_rows_get_targeted_positive_route_boosters",
            "near_gate_boosters_prefer_small_additive_routes_and_skip_broad_toxic_addons",
            "winner_and_seed_lists_are_deduped_by_distinct_scored_behavior",
            "candidate_decision_brief_is_built_for_summary_best_variant_not_raw_pnl_rank",
            "exact_route_ablation_scores_marginal_route_contribution_for_frontier_candidates",
            "persistent_route_learning_registry_tracks_good_and_toxic_addons",
            "adaptive_gate_manifest_written_for_next_score_only_hunt",
            "fresh_hunts_use_persistent_route_registry_to_sample_good_routes_and_avoid_toxic_routes",
            "learned_phase_reduces_unproductive_skip_probe_budget",
            "frontier_recombination_mutates_highest_gate_holdout_winners",
            "frontier_recombination_requires_incremental_route_coverage",
            "candidate_roles_separate_gate_winner_low_sample_profit_and_diagnostic_failure",
            "failed_runs_brief_best_profitable_row_instead_of_negative_high_trade_failure",
            "gate_break_diagnosis_explains_why_requested_trade_gate_failed",
            "risk_reports_cover_selected_raw_gate_holdout_low_sample_and_failure_roles",
            "candidate_validation_reports_add_chronological_and_leave_one_day_checks",
            "score_cache_report_tracks_hit_rate_and_duplicate_decision_hashes",
            "route_registry_tracks_conflicted_contextual_good_toxic_evidence",
            "data_coverage_report_states_exact_compiled_research_scope",
            "frontier_expansion_allows_broad_skip_override_routes_to_add_inside_base_coverage",
            "learned_phase_reduces_rescue_micro_duplicate_pressure_and_allocates_more_budget_to_scaleup",
            "learned_phase_tightens_micro_budget_when_low_sample_duplicate_pressure_persists",
            "frontier_parent_floor_expands_below_trade_gate_when_near_gate_candidates_stall",
            "frontier_bridge_tests_more_small_addons_when_200_trade_candidates_stall_below_gate",
            "frontier_addons_are_greedy_incremental_to_avoid_toxic_large_seventh_route",
            "frontier_bridge_supplements_seed_routes_with_narrow_synthetic_pockets",
            "prior_marginal_toxic_routes_are_pruned_from_frontier_boost_and_scaleup_parents",
            "frontier_bridge_skips_large_incremental_clsk_addons_after_clsk_loss_collapse",
        ],
        "elapsed_sec": round(time.time() - started, 3),
    }
    candidate_profile_schema.decorate_payload(payload, context={
        "script": "step2_profit_combo_hunter.py",
        "compiled_decision_tape": str(args.compiled_decision_tape),
        "active_step2_pnl": active_pnl,
        "target_pnl": float(args.target_pnl),
        "start_balance": float(args.start_balance),
        "scored_total": len(rows),
        "exit_replay_model": exit_model,
    })
    payload["selection_explanation"] = _selection_explanation(payload)
    registry_report = _update_route_learning_registry(out_dir.parent / "route_learning_registry.json", route_marginal_report)
    payload["route_learning_registry"] = registry_report
    payload["adaptive_gate_manifest"] = _write_adaptive_gate_manifest(out_dir / "adaptive_gate_manifest.json", payload)
    payload["issues"] = _audit_payload(payload)
    summary_path = out_dir / "summary.json"
    _write_json(summary_path, payload)
    try:
        brief = candidate_decision_brief.build(
            payload=payload,
            rank=1,
            variant=str(payload.get("best_variant") or ""),
            candidate_json=str(summary_path),
            run_reproducibility=False,
            run_quarantine=False,
        )
        _write_json(out_dir / "candidate_decision_brief.json", brief)
        payload["candidate_decision_brief"] = str((out_dir / "candidate_decision_brief.json").resolve())
        payload["candidate_decision_brief_selection"] = brief.get("selection") or {}
        payload["issues"] = _audit_payload(payload)
        _write_json(summary_path, payload)
    except Exception as exc:
        payload["candidate_decision_brief_error"] = str(exc)
        _write_json(summary_path, payload)
    payload["learning_db"] = _persist_learning_db(out_dir, args)
    payload["issues"] = _audit_payload(payload)
    _write_json(summary_path, payload)
    print(json.dumps({
        "summary": str(summary_path),
        "scored_total": len(rows),
        "best_variant": payload.get("best_variant"),
        "best_pnl": payload.get("best_pnl"),
        "best_trades": payload.get("best_trades"),
        "raw_best_variant": payload.get("raw_best_variant"),
        "raw_best_pnl": payload.get("raw_best_pnl"),
        "raw_best_trades": payload.get("raw_best_trades"),
        "best_low_sample_positive": _row_brief((payload.get("low_sample_positive") or [None])[0]) if payload.get("low_sample_positive") else None,
        "winner_count": payload.get("winner_count"),
        "winner_variant_count": payload.get("winner_variant_count"),
        "winner_duplicate_behavior_count": payload.get("winner_duplicate_behavior_count"),
        "recommended_count": payload.get("recommended_count"),
        "holdout_winner_count": payload.get("holdout_winner_count"),
        "holdout_winner_variant_count": payload.get("holdout_winner_variant_count"),
        "highest_holdout_positive_gate": (payload.get("frontier_report") or {}).get("highest_holdout_positive_gate"),
        "next_suggested_min_trades": (payload.get("next_step_guidance") or {}).get("suggested_next_min_trades"),
        "lineage_status": (payload.get("compiled_lineage_validation") or {}).get("status"),
        "issues": payload.get("issues"),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
