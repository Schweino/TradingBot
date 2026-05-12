"""Resolve the fastest certified Step 2 compiled manifest for hunts.

The cache lifecycle now has two valid range shapes:

* monolithic compiled arrays, which are expensive to rebuild;
* linked range manifests, which cheaply stitch existing certified day shards.

This resolver is the pre-hunt gate. It prefers an already certified linked
manifest, creates one from a certified monolith when possible, and refuses to
hand a hunter a cache with score-affecting drift.
"""
from __future__ import annotations

import argparse
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

import certify_step2_cache
import decision_tape_compiled
import step2_latency_model
import step2_quote_aware_guard
import step2_range_linker
import tournament_safety
import worker_policy


HERE = Path(__file__).resolve().parent
CT = ZoneInfo("America/Chicago")
COMPILED_DIR = HERE / "postmortem" / "backtests" / "compiled_decision_tapes"
GATE_DIR = HERE / "postmortem" / "pre_hunt_cache_gates"
DEFAULT_TICKERS = ["CLSK", "MARA", "RIOT"]
DEFAULT_START = "2026-04-06"
DEFAULT_END = "2026-05-08"
SCHEMA_VERSION = 1

SAFE_CERTIFICATION_ACTIONS = {
    "score_only",
    "recertify_non_score_drift",
}
SAFE_RESOLVER_ACTIONS = {
    "existing_certified_linked",
    "existing_certified_monolithic",
    "explicit_certified_manifest",
    "range_link_certify",
}


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


def _read_json(path: str | os.PathLike[str], default: Any = None) -> Any:
    try:
        with Path(path).open("r", encoding="utf-8-sig") as f:
            payload = json.load(f)
        return payload if payload is not None else default
    except Exception:
        return default


def write_json(path: str | os.PathLike[str], payload: dict[str, Any]) -> str:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{os.getpid()}.{int(time.time() * 1000)}.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
        f.write("\n")
    os.replace(tmp, target)
    return str(target.resolve())


def _normalize_tickers(tickers: list[str] | tuple[str, ...] | None) -> list[str]:
    return sorted(str(ticker).upper() for ticker in (tickers or []) if str(ticker).strip())


def _file_meta(path: str | os.PathLike[str]) -> dict[str, Any]:
    p = Path(path)
    exists = p.exists()
    return {
        "path": str(p.resolve()) if exists else str(p),
        "exists": exists,
        "bytes": p.stat().st_size if exists else 0,
        "mtime": p.stat().st_mtime if exists else None,
        "sha256": tournament_safety._file_sha256(str(p)) if exists else "",
    }


def default_name(start: str, end: str, tickers: list[str] | tuple[str, ...] | None = None) -> str:
    return certify_step2_cache.default_range_compiled_name(start, end, _normalize_tickers(tickers) or DEFAULT_TICKERS)


def linked_name(name: str) -> str:
    raw = str(name or "").strip()
    return raw if raw.endswith("_linked") else f"{raw}_linked"


def manifest_path(name: str, compiled_dir: str | os.PathLike[str] = COMPILED_DIR) -> Path:
    return Path(compiled_dir) / str(name) / "manifest.json"


def _source_days(manifest: dict[str, Any]) -> list[str]:
    day_map = manifest.get("day_map")
    if isinstance(day_map, dict) and day_map:
        return sorted(str(day) for day in day_map)
    days = manifest.get("source_days")
    if isinstance(days, list):
        return sorted(str(day) for day in days)
    shards = ((manifest.get("day_shards") or {}).get("shards") if isinstance(manifest.get("day_shards"), dict) else [])
    if isinstance(shards, list):
        return sorted(str(row.get("day")) for row in shards if row.get("day"))
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
    days = _source_days(manifest)
    tickers = _tickers_from_manifest(manifest)
    return {
        "source_days": days,
        "start": days[0] if days else str(manifest.get("start_day") or manifest.get("first_day") or ""),
        "end": days[-1] if days else str(manifest.get("end_day") or manifest.get("last_day") or ""),
        "tickers": tickers,
        "rows": manifest.get("rows") or manifest.get("row_count"),
        "min_ts": manifest.get("min_ts") or manifest.get("first_ts"),
        "max_ts": manifest.get("max_ts") or manifest.get("last_ts"),
        "arrays_sha256": manifest.get("arrays_sha256") or manifest.get("array_payload_sha256") or manifest.get("array_payload_hash"),
        "compiled_tape_hash": manifest.get("compiled_tape_hash"),
        "compiled_tape_semantic_hash": manifest.get("compiled_tape_semantic_hash"),
        "exit_replay_model": step2_quote_aware_guard.manifest_exit_replay_model(manifest),
        "required_exit_replay_model": step2_quote_aware_guard.REQUIRED_EXIT_REPLAY_MODEL,
        "range_linked": bool(manifest.get("range_linked")),
        "has_day_shards": bool(((manifest.get("day_shards") or {}).get("shards") if isinstance(manifest.get("day_shards"), dict) else [])),
    }


def _scope_ok(scope: dict[str, Any], *, start: str, end: str, tickers: list[str]) -> tuple[bool, list[str]]:
    blockers: list[str] = []
    actual_tickers = _normalize_tickers(scope.get("tickers") or [])
    if actual_tickers and _normalize_tickers(tickers) and actual_tickers != _normalize_tickers(tickers):
        blockers.append("ticker_scope_mismatch")
    actual_start = str(scope.get("start") or "")
    actual_end = str(scope.get("end") or "")
    if start and end:
        if not actual_start or not actual_end:
            blockers.append("manifest_source_days_missing")
        elif actual_start > start or actual_end < end:
            blockers.append("manifest_does_not_cover_requested_range")
    return not blockers, blockers


def _compact_certification(receipt: dict[str, Any]) -> dict[str, Any]:
    after = receipt.get("after") if isinstance(receipt.get("after"), dict) else {}
    return {
        "ok": bool(receipt.get("ok")),
        "source": receipt.get("source"),
        "certification_mode": receipt.get("certification_mode"),
        "selected_action": receipt.get("selected_action"),
        "initial_action": receipt.get("initial_action"),
        "reason": receipt.get("reason"),
        "failure_reasons": receipt.get("failure_reasons") or [],
        "certification_hash": receipt.get("certification_hash"),
        "lineage_status": after.get("lineage_status") or after.get("status"),
        "lineage_certified": after.get("lineage_certified") if "lineage_certified" in after else after.get("certified"),
        "lineage_rebuild_required": after.get("lineage_rebuild_required"),
        "lineage_quick_score_allowed": after.get("lineage_quick_score_allowed"),
        "compiled_manifest": receipt.get("compiled_manifest") or {},
        "quote_aware_outcome_gate": receipt.get("quote_aware_outcome_gate") or {},
        "scope": receipt.get("scope") or {},
    }


def _cert_args(
    *,
    manifest: Path,
    start: str,
    end: str,
    tickers: list[str],
    feed: str,
    quote_mode: str,
    btc_mode: str,
    indicator_mode: str,
    workers: int,
    overlap_sec: int,
    step2_latency_mode: str,
    step2_latency_model_path: str,
    step2_latency_percentile: str | float,
    allow_recertify: bool,
    check_only: bool,
) -> argparse.Namespace:
    return argparse.Namespace(
        day="",
        start=start,
        end=end,
        tickers=tickers,
        feed=feed,
        quote_mode=quote_mode,
        btc_mode=btc_mode,
        indicator_mode=indicator_mode,
        name=manifest.parent.name,
        compiled_manifest=str(manifest),
        workers=int(workers or 1),
        no_existing=False,
        overlap_sec=int(overlap_sec or 0),
        step2_latency_mode=step2_latency_mode,
        step2_latency_model=str(step2_latency_model_path or ""),
        step2_latency_percentile=step2_latency_percentile,
        use_state_checkpoints=True,
        check_only=bool(check_only),
        no_recertify_non_score=not bool(allow_recertify),
    )


def certify_manifest(
    manifest: str | os.PathLike[str],
    *,
    start: str = "",
    end: str = "",
    tickers: list[str] | tuple[str, ...] | None = None,
    feed: str = "sip",
    quote_mode: str = "per-second",
    btc_mode: str = "bars",
    indicator_mode: str = "live",
    workers: int = 6,
    overlap_sec: int = 120,
    step2_latency_mode: str = "entry-exit",
    step2_latency_model_path: str = str(step2_latency_model.DEFAULT_MODEL_PATH),
    step2_latency_percentile: str | float = "p75",
    allow_recertify: bool = True,
    check_only: bool = False,
    write: bool = True,
) -> dict[str, Any]:
    manifest_path = Path(manifest).resolve()
    payload = _read_json(manifest_path, {}) or {}
    scope = _scope_from_manifest(payload)
    cert_start = start or str(scope.get("start") or "")
    cert_end = end or str(scope.get("end") or cert_start)
    if cert_start and not cert_end:
        cert_end = cert_start
    if cert_end and not cert_start:
        cert_start = cert_end
    cert_tickers = _normalize_tickers(tickers) or _normalize_tickers(scope.get("tickers") or []) or DEFAULT_TICKERS
    args = _cert_args(
        manifest=manifest_path,
        start=cert_start,
        end=cert_end,
        tickers=cert_tickers,
        feed=feed,
        quote_mode=quote_mode,
        btc_mode=btc_mode,
        indicator_mode=indicator_mode,
        workers=workers,
        overlap_sec=overlap_sec,
        step2_latency_mode=step2_latency_mode,
        step2_latency_model_path=step2_latency_model_path,
        step2_latency_percentile=step2_latency_percentile,
        allow_recertify=allow_recertify,
        check_only=check_only,
    )
    return certify_step2_cache.certify(args, write=write)


def _certification_ok(receipt: dict[str, Any]) -> bool:
    after = receipt.get("after") if isinstance(receipt.get("after"), dict) else {}
    lineage_status = after.get("lineage_status") or after.get("status")
    lineage_certified = after.get("lineage_certified") if "lineage_certified" in after else after.get("certified")
    return bool(receipt.get("ok") and lineage_status == "CERTIFIED_MATCH" and lineage_certified is True)


def _quote_gate_from_certification(certification: dict[str, Any]) -> dict[str, Any]:
    quote_gate = certification.get("quote_aware_outcome_gate")
    if isinstance(quote_gate, dict):
        return quote_gate
    if _certification_ok(certification):
        return {
            "ok": True,
            "required_exit_replay_model": step2_quote_aware_guard.REQUIRED_EXIT_REPLAY_MODEL,
            "actual_exit_replay_model": "",
            "blockers": [],
            "legacy_certified_manifest_without_exit_model": True,
            "deduction": (
                "Accepted only through an already-certified lineage receipt that predates "
                "the explicit quote-aware exit replay model field."
            ),
        }
    return {
        "ok": False,
        "required_exit_replay_model": step2_quote_aware_guard.REQUIRED_EXIT_REPLAY_MODEL,
        "actual_exit_replay_model": "",
        "blockers": ["exit_replay_model_missing"],
    }


def _gate(
    *,
    resolver_action: str,
    certification: dict[str, Any],
    require_certified: bool,
) -> dict[str, Any]:
    cert_action = str(certification.get("selected_action") or "")
    quote_gate = _quote_gate_from_certification(certification)
    ok = bool(
        (not require_certified or _certification_ok(certification))
        and resolver_action in SAFE_RESOLVER_ACTIONS
        and (not cert_action or cert_action in SAFE_CERTIFICATION_ACTIONS)
        and bool(quote_gate.get("ok"))
    )
    blockers: list[str] = []
    if require_certified and not _certification_ok(certification):
        blockers.append("compiled_tape_not_certified")
    if resolver_action not in SAFE_RESOLVER_ACTIONS:
        blockers.append("unsafe_resolver_action")
    if cert_action and cert_action not in SAFE_CERTIFICATION_ACTIONS:
        blockers.append("unsafe_certification_action")
    blockers.extend(str(item) for item in (quote_gate.get("blockers") or []))
    return {
        "ok": ok,
        "require_certified": require_certified,
        "resolver_action": resolver_action,
        "certification_action": cert_action,
        "quote_aware_outcome_gate": quote_gate,
        "allowed_resolver_actions": sorted(SAFE_RESOLVER_ACTIONS),
        "allowed_certification_actions": sorted(SAFE_CERTIFICATION_ACTIONS),
        "blockers": sorted(set(blockers)),
    }


def _plan_hint(
    *,
    start: str,
    end: str,
    tickers: list[str],
    name: str,
    compiled_dir: str | os.PathLike[str],
    feed: str,
    quote_mode: str,
    btc_mode: str,
    indicator_mode: str,
    workers: int,
) -> dict[str, Any]:
    return {
        "supported": True,
        "reason": "rebuild_or_certify_required_before_hunt",
        "command": (
            "python step2_fast_rebuild.py "
            f"--start {start} --end {end} --tickers {' '.join(tickers)} "
            f"--feed {feed} --quote-mode {quote_mode} --btc-mode {btc_mode} "
            f"--indicator-mode {indicator_mode} --workers {workers} "
            f"--compiled-dir \"{Path(compiled_dir)}\" --name {name}"
        ),
    }


def _candidate_record(
    path: Path,
    *,
    kind: str,
    certification: dict[str, Any] | None = None,
    error: str = "",
) -> dict[str, Any]:
    manifest = _read_json(path, {}) or {}
    return {
        "kind": kind,
        "manifest": _file_meta(path),
        "scope": _scope_from_manifest(manifest),
        "quote_aware_outcome_gate": step2_quote_aware_guard.manifest_gate(manifest),
        "certification": _compact_certification(certification or {}),
        "error": error,
    }


def _try_certify(
    path: Path,
    *,
    kind: str,
    start: str,
    end: str,
    tickers: list[str],
    feed: str,
    quote_mode: str,
    btc_mode: str,
    indicator_mode: str,
    workers: int,
    overlap_sec: int,
    step2_latency_mode: str,
    step2_latency_model_path: str,
    step2_latency_percentile: str | float,
    allow_recertify: bool,
    write_certification: bool,
    require_certified: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not path.exists():
        return _candidate_record(path, kind=kind, error="manifest_missing"), {}
    try:
        receipt = certify_manifest(
            path,
            start=start,
            end=end,
            tickers=tickers,
            feed=feed,
            quote_mode=quote_mode,
            btc_mode=btc_mode,
            indicator_mode=indicator_mode,
            workers=workers,
            overlap_sec=overlap_sec,
            step2_latency_mode=step2_latency_mode,
            step2_latency_model_path=step2_latency_model_path,
            step2_latency_percentile=step2_latency_percentile,
            allow_recertify=allow_recertify,
            check_only=not allow_recertify,
            write=write_certification,
        )
    except Exception as exc:
        return _candidate_record(path, kind=kind, error=repr(exc)), {}
    record = _candidate_record(path, kind=kind, certification=receipt)
    if require_certified and not _certification_ok(receipt):
        return record, {}
    return record, receipt


def write_pre_hunt_receipt(payload: dict[str, Any], *, label: str = "") -> str:
    safe_label = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in str(label or "")).strip("._")
    stem = f"step2_pre_hunt_cache_gate_{safe_label}" if safe_label else "step2_pre_hunt_cache_gate"
    stamp = datetime.now(CT).strftime("%Y%m%d_%H%M%S")
    path = GATE_DIR / f"{stem}_{stamp}.json"
    return write_json(path, payload)


def resolve_best_manifest(
    *,
    start: str = DEFAULT_START,
    end: str = DEFAULT_END,
    tickers: list[str] | tuple[str, ...] | None = None,
    compiled_manifest: str | os.PathLike[str] | None = None,
    compiled_dir: str | os.PathLike[str] = COMPILED_DIR,
    name: str = "",
    linked_manifest_name: str = "",
    prefer_linked: bool = True,
    allow_link: bool = True,
    allow_recertify: bool = True,
    require_certified: bool = True,
    write_certification: bool = True,
    write_receipt: bool = False,
    label: str = "",
    feed: str = "sip",
    quote_mode: str = "per-second",
    btc_mode: str = "bars",
    indicator_mode: str = "live",
    workers: int = 6,
    overlap_sec: int = 120,
    step2_latency_mode: str = "entry-exit",
    step2_latency_model_path: str = str(step2_latency_model.DEFAULT_MODEL_PATH),
    step2_latency_percentile: str | float = "p75",
) -> dict[str, Any]:
    wanted_tickers = _normalize_tickers(tickers) or DEFAULT_TICKERS
    req_start = str(start or DEFAULT_START)
    req_end = str(end or req_start or DEFAULT_END)
    if req_start and not req_end:
        req_end = req_start
    if req_end and not req_start:
        req_start = req_end
    base_name = name or default_name(req_start, req_end, wanted_tickers)
    link_name = linked_manifest_name or linked_name(base_name)
    compiled_root = Path(compiled_dir)
    base_manifest = manifest_path(base_name, compiled_root)
    linked_manifest = manifest_path(link_name, compiled_root)
    explicit_path = Path(compiled_manifest).resolve() if compiled_manifest else None

    selected: dict[str, Any] | None = None
    selected_certification: dict[str, Any] = {}
    candidates: list[dict[str, Any]] = []
    link_attempt: dict[str, Any] = {"attempted": False}

    candidate_paths: list[tuple[str, Path, str]] = []
    if explicit_path:
        explicit_is_base = explicit_path == base_manifest.resolve()
        if prefer_linked and explicit_is_base:
            candidate_paths.append(("linked", linked_manifest, "existing_certified_linked"))
            candidate_paths.append(("explicit", explicit_path, "explicit_certified_manifest"))
        else:
            candidate_paths.append(("explicit", explicit_path, "explicit_certified_manifest"))
    else:
        if prefer_linked:
            candidate_paths.append(("linked", linked_manifest, "existing_certified_linked"))
        if not allow_link:
            candidate_paths.append(("monolithic", base_manifest, "existing_certified_monolithic"))

    seen_paths: set[str] = set()
    for kind, path, resolver_action in candidate_paths:
        key = str(path.resolve())
        if key in seen_paths:
            continue
        seen_paths.add(key)
        record, receipt = _try_certify(
            path,
            kind=kind,
            start=req_start,
            end=req_end,
            tickers=wanted_tickers,
            feed=feed,
            quote_mode=quote_mode,
            btc_mode=btc_mode,
            indicator_mode=indicator_mode,
            workers=workers,
            overlap_sec=overlap_sec,
            step2_latency_mode=step2_latency_mode,
            step2_latency_model_path=step2_latency_model_path,
            step2_latency_percentile=step2_latency_percentile,
            allow_recertify=allow_recertify,
            write_certification=write_certification,
            require_certified=require_certified,
        )
        candidates.append(record)
        if receipt:
            manifest_payload = _read_json(path, {}) or {}
            scope_ok, scope_blockers = _scope_ok(_scope_from_manifest(manifest_payload), start=req_start, end=req_end, tickers=wanted_tickers)
            gate = _gate(resolver_action=resolver_action, certification=receipt, require_certified=require_certified)
            if scope_ok and gate.get("ok"):
                selected = {
                    "kind": kind,
                    "resolver_action": resolver_action,
                    "manifest_path": str(path.resolve()),
                    "manifest_name": path.parent.name,
                    "scope": _scope_from_manifest(manifest_payload),
                    "pre_hunt_gate": gate,
                    "scope_blockers": [],
                }
                selected_certification = receipt
                break
            record.setdefault("scope_blockers", scope_blockers)
            record.setdefault("pre_hunt_gate", gate)

    if selected is None and allow_link and not explicit_path and base_manifest.exists():
        base_record, base_receipt = _try_certify(
            base_manifest,
            kind="monolithic_source_for_link",
            start=req_start,
            end=req_end,
            tickers=wanted_tickers,
            feed=feed,
            quote_mode=quote_mode,
            btc_mode=btc_mode,
            indicator_mode=indicator_mode,
            workers=workers,
            overlap_sec=overlap_sec,
            step2_latency_mode=step2_latency_mode,
            step2_latency_model_path=step2_latency_model_path,
            step2_latency_percentile=step2_latency_percentile,
            allow_recertify=allow_recertify,
            write_certification=write_certification,
            require_certified=require_certified,
        )
        candidates.append(base_record)
        source_manifest_payload = _read_json(base_manifest, {}) or {}
        source_scope = _scope_from_manifest(source_manifest_payload)
        if base_receipt and source_scope.get("has_day_shards"):
            link_attempt = {
                "attempted": True,
                "source_manifest": str(base_manifest.resolve()),
                "linked_name": link_name,
            }
            try:
                linked = step2_range_linker.link_range(
                    source_manifest=str(base_manifest.resolve()),
                    out_dir=str(compiled_root),
                    name=link_name,
                )
                linked_path = Path(str(linked.get("manifest_path") or linked_manifest)).resolve()
                link_attempt.update({
                    "ok": True,
                    "manifest_path": str(linked_path),
                    "compiled_tape_hash": linked.get("compiled_tape_hash"),
                })
                linked_record, linked_receipt = _try_certify(
                    linked_path,
                    kind="linked_created",
                    start=req_start,
                    end=req_end,
                    tickers=wanted_tickers,
                    feed=feed,
                    quote_mode=quote_mode,
                    btc_mode=btc_mode,
                    indicator_mode=indicator_mode,
                    workers=workers,
                    overlap_sec=overlap_sec,
                    step2_latency_mode=step2_latency_mode,
                    step2_latency_model_path=step2_latency_model_path,
                    step2_latency_percentile=step2_latency_percentile,
                    allow_recertify=allow_recertify,
                    write_certification=write_certification,
                    require_certified=require_certified,
                )
                candidates.append(linked_record)
                if linked_receipt:
                    gate = _gate(resolver_action="range_link_certify", certification=linked_receipt, require_certified=require_certified)
                    if gate.get("ok"):
                        linked_payload = _read_json(linked_path, {}) or {}
                        selected = {
                            "kind": "linked",
                            "resolver_action": "range_link_certify",
                            "manifest_path": str(linked_path.resolve()),
                            "manifest_name": linked_path.parent.name,
                            "scope": _scope_from_manifest(linked_payload),
                            "pre_hunt_gate": gate,
                            "scope_blockers": [],
                        }
                        selected_certification = linked_receipt
            except Exception as exc:
                link_attempt.update({"ok": False, "error": repr(exc)})

        if selected is None and base_receipt:
            source_gate = _gate(
                resolver_action="existing_certified_monolithic",
                certification=base_receipt,
                require_certified=require_certified,
            )
            scope_ok, scope_blockers = _scope_ok(source_scope, start=req_start, end=req_end, tickers=wanted_tickers)
            if source_gate.get("ok") and scope_ok:
                selected = {
                    "kind": "monolithic",
                    "resolver_action": "existing_certified_monolithic",
                    "manifest_path": str(base_manifest.resolve()),
                    "manifest_name": base_manifest.parent.name,
                    "scope": source_scope,
                    "pre_hunt_gate": source_gate,
                    "scope_blockers": [],
                }
                selected_certification = base_receipt
            else:
                base_record.setdefault("scope_blockers", scope_blockers)
                base_record.setdefault("pre_hunt_gate", source_gate)

    ok = bool(selected)
    blockers = []
    if not ok:
        blockers.append("no_safe_certified_manifest")
    payload = {
        "schema_version": SCHEMA_VERSION,
        "source": "step2_manifest_resolver",
        "created_at_ct": _now_ct(),
        "ok": ok,
        "requested": {
            "start": req_start,
            "end": req_end,
            "tickers": wanted_tickers,
            "compiled_manifest": str(explicit_path) if explicit_path else "",
            "compiled_dir": str(compiled_root.resolve()),
            "name": base_name,
            "linked_name": link_name,
            "require_certified": require_certified,
            "prefer_linked": prefer_linked,
            "allow_link": allow_link,
            "allow_recertify": allow_recertify,
        },
        "manifest_path": selected.get("manifest_path") if selected else "",
        "manifest_kind": selected.get("kind") if selected else "",
        "manifest_name": selected.get("manifest_name") if selected else "",
        "resolver_action": selected.get("resolver_action") if selected else "",
        "scope": selected.get("scope") if selected else {},
        "pre_hunt_gate": selected.get("pre_hunt_gate") if selected else {
            "ok": False,
            "require_certified": require_certified,
            "blockers": blockers,
        },
        "certification": _compact_certification(selected_certification),
        "candidates": candidates,
        "link_attempt": link_attempt,
        "blockers": blockers,
        "recommendation": _plan_hint(
            start=req_start,
            end=req_end,
            tickers=wanted_tickers,
            name=base_name,
            compiled_dir=compiled_root,
            feed=feed,
            quote_mode=quote_mode,
            btc_mode=btc_mode,
            indicator_mode=indicator_mode,
            workers=worker_policy.clamp_workers(workers),
        ),
    }
    payload["gate_hash"] = tournament_safety.stable_json_hash({
        key: value for key, value in payload.items() if key not in {"candidates"}
    }, length=32)
    if write_receipt:
        payload["receipt_path"] = write_pre_hunt_receipt(payload, label=label or base_name)
    return payload


def load_best_compiled(
    *,
    mmap: bool = True,
    validate_sources: bool = True,
    **kwargs: Any,
) -> tuple[dict[str, Any], dict[str, Any]]:
    resolved = resolve_best_manifest(**kwargs)
    if not resolved.get("ok"):
        raise RuntimeError(f"no safe certified Step 2 manifest: {resolved.get('blockers')}")
    compiled = decision_tape_compiled.load_compiled(
        str(resolved["manifest_path"]),
        mmap=mmap,
        validate_sources=validate_sources,
    )
    return compiled, resolved


def compact_resolution(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "ok": bool(payload.get("ok")),
        "source": payload.get("source"),
        "manifest_path": payload.get("manifest_path") or "",
        "manifest_kind": payload.get("manifest_kind") or "",
        "manifest_name": payload.get("manifest_name") or "",
        "resolver_action": payload.get("resolver_action") or "",
        "requested": payload.get("requested") or {},
        "scope": payload.get("scope") or {},
        "pre_hunt_gate": payload.get("pre_hunt_gate") or {},
        "certification": payload.get("certification") or {},
        "link_attempt": payload.get("link_attempt") or {},
        "blockers": payload.get("blockers") or [],
        "recommendation": payload.get("recommendation") or {},
        "receipt_path": payload.get("receipt_path") or "",
    }


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Resolve and gate the best certified Step 2 cache for hunts.")
    ap.add_argument("--start", default=DEFAULT_START)
    ap.add_argument("--end", default=DEFAULT_END)
    ap.add_argument("--tickers", nargs="*", default=DEFAULT_TICKERS)
    ap.add_argument("--compiled-manifest", default="")
    ap.add_argument("--compiled-dir", default=str(COMPILED_DIR))
    ap.add_argument("--name", default="")
    ap.add_argument("--linked-name", default="")
    ap.add_argument("--no-link", dest="allow_link", action="store_false")
    ap.set_defaults(allow_link=True)
    ap.add_argument("--no-prefer-linked", dest="prefer_linked", action="store_false")
    ap.set_defaults(prefer_linked=True)
    ap.add_argument("--allow-uncertified", dest="require_certified", action="store_false")
    ap.set_defaults(require_certified=True)
    ap.add_argument("--no-recertify", dest="allow_recertify", action="store_false")
    ap.set_defaults(allow_recertify=True)
    ap.add_argument("--no-write-certification", dest="write_certification", action="store_false")
    ap.set_defaults(write_certification=True)
    ap.add_argument("--write-receipt", action="store_true")
    ap.add_argument("--label", default="")
    ap.add_argument("--feed", default="sip")
    ap.add_argument("--quote-mode", default="per-second")
    ap.add_argument("--btc-mode", default="bars")
    ap.add_argument("--indicator-mode", default="live")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--overlap-sec", type=int, default=120)
    ap.add_argument("--step2-latency-mode", choices=["off", "entry", "entry-exit"], default="entry-exit")
    ap.add_argument("--step2-latency-model", default=str(step2_latency_model.DEFAULT_MODEL_PATH))
    ap.add_argument("--step2-latency-percentile", default="p75")
    ap.add_argument("--json", action="store_true")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    payload = resolve_best_manifest(
        start=args.start,
        end=args.end,
        tickers=args.tickers,
        compiled_manifest=args.compiled_manifest or None,
        compiled_dir=args.compiled_dir,
        name=args.name,
        linked_manifest_name=args.linked_name,
        prefer_linked=args.prefer_linked,
        allow_link=args.allow_link,
        allow_recertify=args.allow_recertify,
        require_certified=args.require_certified,
        write_certification=args.write_certification,
        write_receipt=args.write_receipt,
        label=args.label,
        feed=args.feed,
        quote_mode=args.quote_mode,
        btc_mode=args.btc_mode,
        indicator_mode=args.indicator_mode,
        workers=args.workers,
        overlap_sec=args.overlap_sec,
        step2_latency_mode=args.step2_latency_mode,
        step2_latency_model_path=args.step2_latency_model,
        step2_latency_percentile=args.step2_latency_percentile,
    )
    print(json.dumps(payload if args.json else compact_resolution(payload), indent=2, sort_keys=True, default=str))
    return 0 if payload.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
