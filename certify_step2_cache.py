"""Certify the compiled Step 2 cache before variant hunting or promotion.

This is the one-button cache lifecycle:

1. inspect compiled tape lineage;
2. choose the narrowest safe rebuild path through the existing refresh planner;
3. optionally recertify non-score-only drift without rebuilding arrays;
4. confirm the final manifest is a certified lineage match;
5. write a receipt that downstream jobs can reference.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import compiled_tape_lineage
import decision_tape_compiled
import refresh_intraday_step2
import semantic_config
import semantic_diagnostics
import step2_cache_layers
import step2_latency_model
import step2_quote_aware_guard
import step2_shard_cert_registry
import tournament_safety
import worker_policy


HERE = Path(__file__).resolve().parent
OUT_DIR = HERE / "postmortem" / "cache_certifications"
CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


def _stable_hash(payload: Any) -> str:
    return tournament_safety.stable_json_hash(payload, length=64)


def _write_json(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.{int(time.time() * 1000)}.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
        f.write("\n")
    os.replace(tmp, path)
    return str(path.resolve())


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as f:
        payload = json.load(f)
    return payload if isinstance(payload, dict) else {}


def _file_meta(path: str | os.PathLike[str]) -> dict[str, Any]:
    raw = str(path or "")
    exists = bool(raw and os.path.exists(raw))
    return {
        "path": os.path.abspath(raw) if raw else "",
        "exists": exists,
        "bytes": os.path.getsize(raw) if exists else 0,
        "mtime": os.path.getmtime(raw) if exists else None,
        "sha256": tournament_safety._file_sha256(raw) if exists else None,
    }


def default_compiled_name(day: str, tickers: list[str]) -> str:
    return f"compiled_step2_live_mockparity_{'-'.join(tickers)}_{day}_intraday"


def default_range_compiled_name(start: str, end: str, tickers: list[str]) -> str:
    return f"compiled_step2_current_live_{'-'.join(tickers)}_{start}_{end}"


def _manifest_path(compiled_name: str) -> Path:
    return HERE / "postmortem" / "backtests" / "compiled_decision_tapes" / compiled_name / "manifest.json"


def _cache_report(
    *,
    day: str,
    tickers: list[str],
    feed: str,
    quote_mode: str,
    btc_mode: str,
    indicator_mode: str,
    compiled_name: str,
) -> dict[str, Any]:
    return step2_cache_layers.report(
        day,
        tickers,
        feed=feed,
        quote_mode=quote_mode,
        btc_mode=btc_mode,
        indicator_mode=indicator_mode,
        compiled_name=compiled_name,
    )


def _lineage(report: dict[str, Any]) -> dict[str, Any]:
    value = report.get("compiled_tape_lineage")
    return value if isinstance(value, dict) else {}


def _source_days_from_manifest(manifest: dict[str, Any]) -> list[str]:
    day_map = manifest.get("day_map")
    if isinstance(day_map, dict):
        return sorted(str(day) for day in day_map)
    days = manifest.get("source_days")
    if isinstance(days, list):
        return sorted(str(day) for day in days)
    return []


def _tickers_from_manifest(manifest: dict[str, Any]) -> list[str]:
    ticker_map = manifest.get("ticker_map")
    if isinstance(ticker_map, dict) and ticker_map:
        return sorted(str(ticker).upper() for ticker in ticker_map)
    tickers = manifest.get("tickers")
    if isinstance(tickers, list):
        return sorted(str(ticker).upper() for ticker in tickers)
    return []


def _scope_from_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    source_days = _source_days_from_manifest(manifest)
    return {
        "source_days": source_days,
        "start": source_days[0] if source_days else "",
        "end": source_days[-1] if source_days else "",
        "tickers": _tickers_from_manifest(manifest),
        "rows": manifest.get("rows") or manifest.get("row_count"),
        "min_ts": manifest.get("min_ts") or manifest.get("first_ts"),
        "max_ts": manifest.get("max_ts") or manifest.get("last_ts"),
        "arrays_sha256": (
            manifest.get("arrays_sha256")
            or manifest.get("array_payload_sha256")
            or manifest.get("array_payload_hash")
        ),
        "compiled_tape_hash": manifest.get("compiled_tape_hash"),
        "compiled_tape_semantic_hash": manifest.get("compiled_tape_semantic_hash"),
        "step2_execution_contract_hash": manifest.get("step2_execution_contract_hash"),
        "exit_replay_model": step2_quote_aware_guard.manifest_exit_replay_model(manifest),
        "required_exit_replay_model": step2_quote_aware_guard.REQUIRED_EXIT_REPLAY_MODEL,
    }


def _requested_scope(args: argparse.Namespace, tickers: list[str], manifest_scope: dict[str, Any]) -> dict[str, Any]:
    start = str(getattr(args, "start", "") or "")
    end = str(getattr(args, "end", "") or "")
    day = str(getattr(args, "day", "") or "")
    if day and not start and not end:
        start = day
        end = day
    if start and not end:
        end = start
    if end and not start:
        start = end
    if not start and not end:
        start = str(manifest_scope.get("start") or "")
        end = str(manifest_scope.get("end") or "")
    return {
        "day": day,
        "start": start,
        "end": end,
        "tickers": tickers,
    }


def _scope_blockers(requested: dict[str, Any], actual: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    requested_tickers = sorted(str(ticker).upper() for ticker in requested.get("tickers") or [])
    actual_tickers = sorted(str(ticker).upper() for ticker in actual.get("tickers") or [])
    if actual_tickers and requested_tickers and actual_tickers != requested_tickers:
        blockers.append("ticker_scope_mismatch")
    start = str(requested.get("start") or "")
    end = str(requested.get("end") or "")
    actual_start = str(actual.get("start") or "")
    actual_end = str(actual.get("end") or "")
    if start and end:
        if not actual_start or not actual_end:
            blockers.append("manifest_source_days_missing")
        elif actual_start > start or actual_end < end:
            blockers.append("manifest_does_not_cover_requested_range")
    return blockers


def _quote_aware_certification_gate(manifest: dict[str, Any], lineage: dict[str, Any]) -> dict[str, Any]:
    gate = step2_quote_aware_guard.manifest_gate(manifest)
    if gate.get("ok"):
        return gate
    blockers = set(str(item) for item in (gate.get("blockers") or []))
    lineage_certified = lineage.get("certified") is True and lineage.get("status") == "CERTIFIED_MATCH"
    if lineage_certified and blockers == {"exit_replay_model_missing"}:
        legacy_gate = dict(gate)
        legacy_gate.update({
            "ok": True,
            "blockers": [],
            "legacy_certified_manifest_without_exit_model": True,
            "deduction": (
                "Accepted only because compiled tape lineage is already CERTIFIED_MATCH. "
                "New or rebuilt manifests must record the quote-aware exit replay model."
            ),
        })
        return legacy_gate
    return gate


def _manifest_lineage(manifest_path: Path) -> dict[str, Any]:
    if not manifest_path.exists():
        return compiled_tape_lineage.evaluate_mismatches(
            [{
                "path": str(manifest_path),
                "reason": "compiled_manifest_missing",
                "layer": "compiled_scoring_math",
                "score_affecting": True,
                "dependency_known": True,
                "lineage_reason": "Compiled Step 2 manifest is missing.",
            }],
            manifest_path=str(manifest_path),
            code_hash_inputs=decision_tape_compiled.CODE_HASH_INPUTS,
        )
    manifest = _read_json(manifest_path)
    manifest["manifest_path"] = str(manifest_path)
    cfg = _read_json(HERE / "trading_config.json") if (HERE / "trading_config.json").exists() else {}
    semantic_mismatches = semantic_config.mismatches(
        manifest.get("semantic_config_section_hashes") or {},
        manifest.get("compiled_tape_semantic_sections") or semantic_config.COMPILED_TAPE_SECTIONS,
        cfg,
    )
    semantic_mismatches = semantic_diagnostics.enrich_semantic_mismatches(
        semantic_mismatches,
        config=cfg,
        recorded_payloads=manifest.get("semantic_config_section_payloads") or {},
    )
    return compiled_tape_lineage.evaluate_manifest(
        manifest,
        code_hash_inputs=decision_tape_compiled.CODE_HASH_INPUTS,
        semantic_mismatches=semantic_mismatches,
    )


def _lineage_report_for_manifest(manifest_path: Path) -> dict[str, Any]:
    manifest = _read_json(manifest_path) if manifest_path.exists() else {}
    lineage = _manifest_lineage(manifest_path)
    mismatches = lineage.get("all_mismatches") or []
    return {
        "recommended_action": lineage.get("recommended_action"),
        "compiled_tape_lineage": lineage,
        "lineage_status": lineage.get("status"),
        "lineage_certified": lineage.get("certified"),
        "lineage_rebuild_required": lineage.get("rebuild_required"),
        "lineage_quick_score_allowed": lineage.get("quick_score_allowed"),
        "stale_layers": sorted({str(row.get("layer") or "unknown") for row in mismatches}),
        "manifest_scope": _scope_from_manifest(manifest),
    }


def _compiled_tape_hash_payload(manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": manifest.get("schema_version"),
        "source_semantic_hashes": manifest.get("source_semantic_hashes") or manifest.get("source_hashes"),
        "code_hashes": manifest.get("code_hashes"),
        "lineage_registry_hash": (manifest.get("lineage_registry") or {}).get("registry_hash"),
        "compiled_tape_semantic_hash": manifest.get("compiled_tape_semantic_hash"),
        "exit_replay_model": step2_quote_aware_guard.manifest_exit_replay_model(manifest),
        "required_exit_replay_model": step2_quote_aware_guard.REQUIRED_EXIT_REPLAY_MODEL,
        "feature_names": manifest.get("feature_names"),
        "rows": manifest.get("rows"),
        "max_ts": manifest.get("max_ts"),
        "ticker_map": manifest.get("ticker_map"),
        "setup_map": manifest.get("setup_map"),
        "day_map": manifest.get("day_map"),
        "array_payload_sha256": manifest.get("array_payload_sha256") or manifest.get("arrays_sha256"),
    }


def _recertify_non_score_manifest(manifest_path: Path, lineage: dict[str, Any]) -> dict[str, Any]:
    if lineage.get("status") not in ("UNCERTIFIED_NON_SCORE_DRIFT", "UNCERTIFIED_SCORER_DRIFT"):
        return {"attempted": False, "reason": "lineage_not_recertifiable_drift"}
    manifest = _read_json(manifest_path)
    code_hashes = dict(manifest.get("code_hashes") or {})
    updates: list[dict[str, Any]] = []
    blockers: list[dict[str, Any]] = []
    for row in (lineage.get("non_score_drift") or []) + (lineage.get("scorer_drift") or []):
        reason = str(row.get("reason") or "")
        rel_path = str(row.get("path") or "")
        if reason not in ("code_hash_mismatch", "code_hash_not_recorded"):
            blockers.append({"path": rel_path, "reason": reason, "blocker": "not_code_hash_drift"})
            continue
        if row.get("score_affecting") and row.get("layer") != "scoring_runtime":
            blockers.append({"path": rel_path, "reason": reason, "blocker": "score_affecting"})
            continue
        abs_path = Path(rel_path) if os.path.isabs(rel_path) else HERE / rel_path
        if not abs_path.exists():
            blockers.append({"path": rel_path, "reason": reason, "blocker": "file_missing"})
            continue
        actual = tournament_safety._file_sha256(abs_path)
        code_hashes[rel_path] = actual
        updates.append({"path": rel_path, "sha256": actual, "reason": reason})
    if blockers:
        return {"attempted": True, "ok": False, "updates": updates, "blockers": blockers}

    manifest["code_hashes"] = code_hashes
    manifest["lineage_registry"] = compiled_tape_lineage.registry_for(decision_tape_compiled.CODE_HASH_INPUTS)
    manifest["compiled_tape_hash"] = tournament_safety.stable_json_hash(
        _compiled_tape_hash_payload(manifest),
        32,
    )
    manifest.setdefault("recertifications", [])
    manifest["recertifications"].append({
        "created_at_ct": _now_ct(),
        "source": "certify_step2_cache",
        "reason": "scorer_drift_recertified" if lineage.get("status") == "UNCERTIFIED_SCORER_DRIFT" else "non_score_drift_recertified",
        "updates": updates,
        "prior_lineage_status": lineage.get("status"),
        "scorer_recertification": bool(lineage.get("status") == "UNCERTIFIED_SCORER_DRIFT"),
    })
    _write_json(manifest_path, manifest)
    return {"attempted": True, "ok": True, "updates": updates, "blockers": []}


def _latency_percentile_arg(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"p50", "p75", "p95", "default"}:
        return raw
    try:
        num = float(raw)
    except Exception:
        return "p75"
    if abs(num - 0.50) < 1e-9:
        return "p50"
    if abs(num - 0.75) < 1e-9:
        return "p75"
    if abs(num - 0.95) < 1e-9:
        return "p95"
    return "p75"


def _refresh_args(args: argparse.Namespace, tickers: list[str], compiled_name: str, action: str) -> SimpleNamespace:
    return SimpleNamespace(
        day=args.day,
        tickers=tickers,
        feed=args.feed,
        quote_mode=args.quote_mode,
        btc_mode=args.btc_mode,
        indicator_mode=args.indicator_mode,
        workers=worker_policy.clamp_workers(args.workers),
        name=compiled_name,
        no_existing=bool(args.no_existing),
        materialize_only=False,
        full_rebuild=action in ("full_signal_rebuild", "full_rebuild"),
        reuse_existing_signals=action in ("refresh_outcomes_compile", "reuse_existing_signals_refresh_outcomes_compile"),
        overlap_sec=int(args.overlap_sec or 0),
        step2_latency_mode=args.step2_latency_mode,
        step2_latency_model=args.step2_latency_model,
        step2_latency_percentile=_latency_percentile_arg(args.step2_latency_percentile),
        use_state_checkpoints=bool(args.use_state_checkpoints),
    )


def _receipt_paths(scope_id: str, tickers: list[str]) -> tuple[Path, Path]:
    suffix = f"{scope_id}_{'-'.join(tickers)}"
    return (
        OUT_DIR / f"step2_cache_certification_{suffix}.json",
        OUT_DIR / f"step2_cache_certification_{suffix}.LATEST.json",
    )


def _write_receipt(receipt: dict[str, Any], scope_id: str, tickers: list[str]) -> None:
    path, latest = _receipt_paths(scope_id, tickers)
    receipt["output"] = {
        "json_path": _write_json(path, receipt),
    }
    receipt["output"]["latest_path"] = _write_json(latest, receipt)


def _failure_reasons(lineage: dict[str, Any], scope_blockers: list[str] | None = None) -> list[str]:
    reasons: list[str] = []
    if not (lineage.get("certified") is True and lineage.get("status") == "CERTIFIED_MATCH"):
        reasons.append("compiled_tape_not_certified")
    if lineage.get("rebuild_required"):
        reasons.append("lineage_rebuild_required")
    if lineage.get("quick_score_allowed") is False:
        reasons.append("quick_score_not_allowed")
    reasons.extend(scope_blockers or [])
    return sorted(set(reasons))


def _receipt_lineage_summary(report: dict[str, Any], lineage: dict[str, Any]) -> dict[str, Any]:
    return {
        "recommended_action": report.get("recommended_action"),
        "lineage_status": lineage.get("status"),
        "lineage_certified": lineage.get("certified"),
        "lineage_rebuild_required": lineage.get("rebuild_required"),
        "lineage_quick_score_allowed": lineage.get("quick_score_allowed"),
        "score_affecting_count": lineage.get("score_affecting_count"),
        "non_score_count": lineage.get("non_score_count"),
        "stale_layers": report.get("stale_layers"),
        "drift_diagnostics": _lineage_diagnostics(lineage),
    }


def _lineage_diagnostics(lineage: dict[str, Any]) -> list[dict[str, Any]]:
    diagnostics: list[dict[str, Any]] = []
    for row in lineage.get("all_mismatches") or []:
        diag = row.get("diagnostics")
        if diag:
            diagnostics.append({
                "path": row.get("path"),
                "reason": row.get("reason"),
                "layer": row.get("layer"),
                "score_affecting": row.get("score_affecting"),
                "diagnostics": diag,
            })
    return diagnostics[:10]


def _learning_summary(args: argparse.Namespace) -> dict[str, Any]:
    latency = semantic_diagnostics.latency_model_fingerprint(str(getattr(args, "step2_latency_model", "") or ""))
    return {
        "warnings": list(latency.get("learning_warnings") or []),
        "latency_model": latency,
    }


def certify_range(args: argparse.Namespace, *, write: bool = True) -> dict[str, Any]:
    tickers = [str(ticker).upper() for ticker in args.tickers]
    started = time.perf_counter()
    start = str(getattr(args, "start", "") or "")
    end = str(getattr(args, "end", "") or "")
    if start and not end:
        end = start
    if end and not start:
        start = end
    compiled_name = args.name or default_range_compiled_name(start, end, tickers)
    manifest_path = Path(args.compiled_manifest).resolve() if args.compiled_manifest else _manifest_path(compiled_name)
    manifest = _read_json(manifest_path) if manifest_path.exists() else {}
    manifest_scope = _scope_from_manifest(manifest)
    requested_scope = _requested_scope(args, tickers, manifest_scope)

    before = _lineage_report_for_manifest(manifest_path)
    before_lineage = _lineage(before)
    action = str(before.get("recommended_action") or before_lineage.get("recommended_action") or "full_signal_rebuild")
    recertification: dict[str, Any] = {"attempted": False}

    if before_lineage.get("certified") is True and before_lineage.get("status") == "CERTIFIED_MATCH":
        after = before
        selected_action = "score_only"
        reason = "already_certified"
    elif args.check_only:
        after = before
        selected_action = "check_only"
        reason = "certification_not_modified"
    elif before_lineage.get("status") in ("UNCERTIFIED_NON_SCORE_DRIFT", "UNCERTIFIED_SCORER_DRIFT") and not args.no_recertify_non_score:
        recertification = _recertify_non_score_manifest(manifest_path, before_lineage)
        selected_action = "recertify_non_score_drift"
        reason = "non_score_drift_recertified" if recertification.get("ok") else "non_score_recertification_failed"
        after = _lineage_report_for_manifest(manifest_path)
    else:
        after = before
        selected_action = "range_certification_rebuild_required"
        reason = "range_manifest_has_score_affecting_drift"

    after_lineage = _lineage(after)
    manifest_after = _read_json(manifest_path) if manifest_path.exists() else {}
    actual_scope = _scope_from_manifest(manifest_after)
    scope_blockers = _scope_blockers(requested_scope, actual_scope)
    ok = bool(
        after_lineage.get("certified") is True
        and after_lineage.get("status") == "CERTIFIED_MATCH"
        and not scope_blockers
    )
    failure_reasons = _failure_reasons(after_lineage, scope_blockers)
    quote_aware_gate = _quote_aware_certification_gate(manifest_after, after_lineage)
    if not quote_aware_gate.get("ok"):
        ok = False
        failure_reasons.extend(quote_aware_gate.get("blockers") or ["exit_replay_model_gate_failed"])
    scope_id = (
        requested_scope.get("day")
        if requested_scope.get("day") and requested_scope.get("start") == requested_scope.get("end")
        else f"{requested_scope.get('start')}_{requested_scope.get('end')}"
    )
    receipt = {
        "schema_version": SCHEMA_VERSION,
        "source": "certify_step2_cache",
        "certification_mode": "range",
        "created_at_ct": _now_ct(),
        "ok": ok,
        "day": str(getattr(args, "day", "") or ""),
        "start": requested_scope.get("start"),
        "end": requested_scope.get("end"),
        "source_days": actual_scope.get("source_days") or [],
        "tickers": tickers,
        "feed": args.feed,
        "quote_mode": args.quote_mode,
        "btc_mode": args.btc_mode,
        "indicator_mode": args.indicator_mode,
        "compiled_name": compiled_name,
        "compiled_manifest": _file_meta(manifest_path),
        "compiled_tape_hash": actual_scope.get("compiled_tape_hash"),
        "arrays_sha256": actual_scope.get("arrays_sha256"),
        "step2_execution_contract_hash": actual_scope.get("step2_execution_contract_hash"),
        "compiled_tape_semantic_hash": actual_scope.get("compiled_tape_semantic_hash"),
        "workers_requested": int(args.workers),
        "workers_effective": worker_policy.clamp_workers(args.workers),
        "selected_action": selected_action,
        "initial_action": action,
        "reason": reason,
        "failure_reasons": sorted(set(failure_reasons)),
        "quote_aware_outcome_gate": quote_aware_gate,
        "scope": {
            "requested": requested_scope,
            "actual": actual_scope,
            "ok": not scope_blockers,
            "blockers": scope_blockers,
        },
        "elapsed_sec": round(time.perf_counter() - started, 3),
        "before": _receipt_lineage_summary(before, before_lineage),
        "after": _receipt_lineage_summary(after, after_lineage),
        "recertification": recertification,
        "refresh": {},
        "learning": _learning_summary(args),
        "deduction": (
            "Range certification is tied to the exact compiled manifest path and hash. "
            "Score-affecting drift is refused here; rebuild the range cache before hunting."
        ),
    }
    receipt["certification_hash"] = _stable_hash({k: v for k, v in receipt.items() if k not in {"output", "day_shard_registry"}})
    if write:
        try:
            receipt["day_shard_registry"] = step2_shard_cert_registry.write_registry_for_manifest(manifest_path, receipt)
        except Exception as exc:
            receipt["day_shard_registry"] = {"written": False, "error": repr(exc)}
    else:
        receipt["day_shard_registry"] = {"written": False, "reason": "no_write"}
    if write:
        _write_receipt(receipt, str(scope_id or "manifest_scope"), tickers)
    return receipt


def certify(args: argparse.Namespace, *, write: bool = True) -> dict[str, Any]:
    if getattr(args, "start", "") or getattr(args, "end", "") or (not getattr(args, "day", "") and getattr(args, "compiled_manifest", "")):
        return certify_range(args, write=write)
    tickers = [str(ticker).upper() for ticker in args.tickers]
    compiled_name = args.name or default_compiled_name(args.day, tickers)
    manifest_path = Path(args.compiled_manifest).resolve() if args.compiled_manifest else _manifest_path(compiled_name)
    started = time.perf_counter()
    before = _cache_report(
        day=args.day,
        tickers=tickers,
        feed=args.feed,
        quote_mode=args.quote_mode,
        btc_mode=args.btc_mode,
        indicator_mode=args.indicator_mode,
        compiled_name=compiled_name,
    )
    before_lineage = _lineage(before)
    action = str(before.get("recommended_action") or before_lineage.get("recommended_action") or "full_signal_rebuild")
    refresh_payload: dict[str, Any] | None = None
    recertification: dict[str, Any] = {"attempted": False}

    if before_lineage.get("certified") is True and before_lineage.get("status") == "CERTIFIED_MATCH":
        after = before
        selected_action = "score_only"
        reason = "already_certified"
    elif args.check_only:
        after = before
        selected_action = "check_only"
        reason = "certification_not_modified"
    elif before_lineage.get("status") in ("UNCERTIFIED_NON_SCORE_DRIFT", "UNCERTIFIED_SCORER_DRIFT") and not args.no_recertify_non_score:
        recertification = _recertify_non_score_manifest(manifest_path, before_lineage)
        selected_action = "recertify_non_score_drift"
        reason = "non_score_drift_recertified" if recertification.get("ok") else "non_score_recertification_failed"
        after = _cache_report(
            day=args.day,
            tickers=tickers,
            feed=args.feed,
            quote_mode=args.quote_mode,
            btc_mode=args.btc_mode,
            indicator_mode=args.indicator_mode,
            compiled_name=compiled_name,
        )
    else:
        selected_action = action
        reason = "lineage_rebuild_required"
        refresh_payload = refresh_intraday_step2.refresh_once(_refresh_args(args, tickers, compiled_name, action))
        manifest_path = Path(refresh_payload.get("compiled_manifest") or manifest_path).resolve()
        after = _cache_report(
            day=args.day,
            tickers=tickers,
            feed=args.feed,
            quote_mode=args.quote_mode,
            btc_mode=args.btc_mode,
            indicator_mode=args.indicator_mode,
            compiled_name=compiled_name,
        )
        after_lineage = _lineage(after)
        if after_lineage.get("status") in ("UNCERTIFIED_NON_SCORE_DRIFT", "UNCERTIFIED_SCORER_DRIFT") and not args.no_recertify_non_score:
            recertification = _recertify_non_score_manifest(manifest_path, after_lineage)
            after = _cache_report(
                day=args.day,
                tickers=tickers,
                feed=args.feed,
                quote_mode=args.quote_mode,
                btc_mode=args.btc_mode,
                indicator_mode=args.indicator_mode,
                compiled_name=compiled_name,
            )

    after_lineage = _lineage(after)
    ok = bool(after_lineage.get("certified") is True and after_lineage.get("status") == "CERTIFIED_MATCH")
    failure_reasons: list[str] = []
    if not ok:
        failure_reasons.append("compiled_tape_not_certified")
    if after_lineage.get("rebuild_required"):
        failure_reasons.append("lineage_rebuild_required")
    if after_lineage.get("quick_score_allowed") is False:
        failure_reasons.append("quick_score_not_allowed")
    final_manifest = _read_json(manifest_path) if manifest_path.exists() else {}
    final_scope = _scope_from_manifest(final_manifest)
    requested_scope = _requested_scope(args, tickers, final_scope)
    quote_aware_gate = _quote_aware_certification_gate(final_manifest, after_lineage)
    if not quote_aware_gate.get("ok"):
        ok = False
        failure_reasons.extend(quote_aware_gate.get("blockers") or ["exit_replay_model_gate_failed"])

    receipt = {
        "schema_version": SCHEMA_VERSION,
        "source": "certify_step2_cache",
        "certification_mode": "day",
        "created_at_ct": _now_ct(),
        "ok": ok,
        "day": args.day,
        "start": requested_scope.get("start"),
        "end": requested_scope.get("end"),
        "source_days": final_scope.get("source_days") or [],
        "tickers": tickers,
        "feed": args.feed,
        "quote_mode": args.quote_mode,
        "btc_mode": args.btc_mode,
        "indicator_mode": args.indicator_mode,
        "compiled_name": compiled_name,
        "compiled_manifest": _file_meta(manifest_path),
        "compiled_tape_hash": final_scope.get("compiled_tape_hash"),
        "arrays_sha256": final_scope.get("arrays_sha256"),
        "step2_execution_contract_hash": final_scope.get("step2_execution_contract_hash"),
        "compiled_tape_semantic_hash": final_scope.get("compiled_tape_semantic_hash"),
        "workers_requested": int(args.workers),
        "workers_effective": worker_policy.clamp_workers(args.workers),
        "selected_action": selected_action,
        "initial_action": action,
        "reason": reason,
        "failure_reasons": sorted(set(failure_reasons)),
        "quote_aware_outcome_gate": quote_aware_gate,
        "scope": {
            "requested": requested_scope,
            "actual": final_scope,
            "ok": True,
            "blockers": [],
        },
        "elapsed_sec": round(time.perf_counter() - started, 3),
        "before": {
            "recommended_action": before.get("recommended_action"),
            "lineage_status": before_lineage.get("status"),
            "lineage_certified": before_lineage.get("certified"),
            "lineage_rebuild_required": before_lineage.get("rebuild_required"),
            "lineage_quick_score_allowed": before_lineage.get("quick_score_allowed"),
            "score_affecting_count": before_lineage.get("score_affecting_count"),
            "non_score_count": before_lineage.get("non_score_count"),
            "stale_layers": before.get("stale_layers"),
            "drift_diagnostics": _lineage_diagnostics(before_lineage),
        },
        "after": {
            "recommended_action": after.get("recommended_action"),
            "lineage_status": after_lineage.get("status"),
            "lineage_certified": after_lineage.get("certified"),
            "lineage_rebuild_required": after_lineage.get("rebuild_required"),
            "lineage_quick_score_allowed": after_lineage.get("quick_score_allowed"),
            "score_affecting_count": after_lineage.get("score_affecting_count"),
            "non_score_count": after_lineage.get("non_score_count"),
            "stale_layers": after.get("stale_layers"),
            "drift_diagnostics": _lineage_diagnostics(after_lineage),
        },
        "recertification": recertification,
        "refresh": refresh_payload or {},
        "learning": _learning_summary(args),
        "deduction": (
            "A compiled Step 2 cache is certified only when the final lineage status is "
            "CERTIFIED_MATCH. Unsafe or uncertified caches are not promotion-grade."
        ),
    }
    receipt["certification_hash"] = _stable_hash({k: v for k, v in receipt.items() if k not in {"output"}})
    if write:
        _write_receipt(receipt, args.day, tickers)
    return receipt


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Certify a compiled Step 2 cache before hunting or promotion.")
    ap.add_argument("--day", default="")
    ap.add_argument("--start", default="", help="Start day for certifying a multi-day compiled manifest.")
    ap.add_argument("--end", default="", help="End day for certifying a multi-day compiled manifest.")
    ap.add_argument("--tickers", nargs="+", default=["CLSK", "MARA", "RIOT"])
    ap.add_argument("--feed", default="sip")
    ap.add_argument("--quote-mode", default="per-second")
    ap.add_argument("--btc-mode", default="bars")
    ap.add_argument("--indicator-mode", default="live")
    ap.add_argument("--name", default="")
    ap.add_argument("--compiled-manifest", default="")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--no-existing", action="store_true")
    ap.add_argument("--overlap-sec", type=int, default=120)
    ap.add_argument("--step2-latency-mode", choices=["off", "entry", "entry-exit"], default="entry-exit")
    ap.add_argument("--step2-latency-model", default=str(step2_latency_model.DEFAULT_MODEL_PATH))
    ap.add_argument("--step2-latency-percentile", default="p75")
    ap.add_argument("--no-state-checkpoints", dest="use_state_checkpoints", action="store_false")
    ap.set_defaults(use_state_checkpoints=True)
    ap.add_argument("--check-only", action="store_true", help="Inspect and write a receipt without rebuilding or recertifying.")
    ap.add_argument("--no-recertify-non-score", action="store_true")
    ap.add_argument("--no-write", action="store_true")
    ap.add_argument("--json", action="store_true")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    if not args.day and not (args.start or args.end or args.compiled_manifest):
        raise SystemExit("--day, --start/--end, or --compiled-manifest is required")
    receipt = certify(args, write=not args.no_write)
    if args.json:
        print(json.dumps(receipt, indent=2, sort_keys=True, default=str))
    else:
        print(json.dumps({
            "ok": receipt.get("ok"),
            "certification_mode": receipt.get("certification_mode"),
            "day": receipt.get("day"),
            "start": receipt.get("start"),
            "end": receipt.get("end"),
            "source_days": receipt.get("source_days"),
            "tickers": receipt.get("tickers"),
            "selected_action": receipt.get("selected_action"),
            "before": receipt.get("before"),
            "after": receipt.get("after"),
            "learning": receipt.get("learning"),
            "failure_reasons": receipt.get("failure_reasons"),
            "output": receipt.get("output"),
        }, indent=2, sort_keys=True, default=str))
    return 0 if receipt.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
