"""Plan and execute the narrowest safe Step 2 cache rebuild.

This is the range-level orchestration layer for the faster Step 2 cache path.
It uses day-level layer/lineage plans, rebuilds only days that actually need
work, compiles the requested range once, and then certifies the final manifest.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import build_decision_tape
import certify_step2_cache
import decision_tape_compiled
import replay_artifacts
import step2_artifact_identity
import step2_latency_model
import step2_range_linker
import step2_rebuild_planner
import worker_policy


HERE = Path(__file__).resolve().parent
CT = ZoneInfo("America/Chicago")
OUT_DIR = HERE / "postmortem" / "rebuild_plans"
STATE_DIR = OUT_DIR / "state"
STAGING_ROOT = HERE / "postmortem" / "backtests" / "step2_rebuild_staging"
BUILD_ACTIONS = {"full_signal_rebuild", "refresh_outcomes_compile", "incremental_append"}
NO_BUILD_ACTIONS = {"score_only", "score_only_uncertified", "recompile_only"}


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


def _safe_name(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in str(value)).strip("._") or "step2_fast_rebuild"


def _scope_id(start: str, end: str, tickers: list[str]) -> str:
    return _safe_name(f"{start}_{end}_{'-'.join(tickers)}")


def _state_path(scope: str) -> Path:
    return STATE_DIR / f"{scope}.json"


def _read_json(path: Path, default: Any = None) -> Any:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _write_json(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    os.replace(tmp, path)
    return str(path.resolve())


def _command_id(label: str, cmd: list[str]) -> str:
    blob = json.dumps({"label": label, "cmd": cmd}, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:24]


def _arg_value(cmd: list[str], flag: str) -> str:
    try:
        idx = cmd.index(flag)
    except ValueError:
        return ""
    if idx + 1 >= len(cmd):
        return ""
    return str(cmd[idx + 1])


def _replace_arg_value(cmd: list[str], flag: str, value: str) -> list[str]:
    out = [str(part) for part in cmd]
    try:
        idx = out.index(flag)
    except ValueError:
        out.extend([flag, value])
        return out
    if idx + 1 < len(out):
        out[idx + 1] = value
    else:
        out.append(value)
    return out


def _script_name(cmd: list[str]) -> str:
    if len(cmd) < 2:
        return ""
    return os.path.basename(str(cmd[1]))


def _replace_script(cmd: list[str], script: str) -> list[str]:
    out = [str(part) for part in cmd]
    if len(out) >= 2:
        out[1] = script
    return out


def _staging_enabled(args: argparse.Namespace, payload: dict[str, Any]) -> bool:
    return bool(
        getattr(args, "staged_promotion", True)
        and not getattr(args, "plan_only", False)
        and (payload.get("build_commands") or payload.get("compile_command"))
    )


def _staging_paths(scope: str, run_id: str) -> dict[str, str]:
    root = Path(getattr(_staging_paths, "root", STAGING_ROOT))
    base = root / scope / run_id
    return {
        "root": str(base.resolve()),
        "tape_dir": str((base / "decision_tapes").resolve()),
        "compiled_dir": str((base / "compiled_decision_tapes").resolve()),
        "artifact_dir": str((base / "replay_artifacts").resolve()),
    }


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


def _days(start: str, end: str) -> list[str]:
    start_day = build_decision_tape.replay._parse_day(start)
    end_day = build_decision_tape.replay._parse_day(end)
    return [day.isoformat() for day in build_decision_tape.replay._market_days(start_day, end_day)]


def _default_name(start: str, end: str, tickers: list[str]) -> str:
    return certify_step2_cache.default_range_compiled_name(start, end, tickers)


def _base_build_cmd(args: argparse.Namespace, start: str, end: str, tickers: list[str]) -> list[str]:
    cmd = [
        sys.executable,
        "build_decision_tape.py",
        "--start", start,
        "--end", end,
        "--tickers", *tickers,
        "--feed", args.feed,
        "--quote-mode", args.quote_mode,
        "--btc-mode", args.btc_mode,
        "--indicator-mode", args.indicator_mode,
        "--source", args.source,
        "--workers", str(worker_policy.clamp_workers(args.workers)),
        "--cache-dir", args.cache_dir,
        "--prepared-cache-dir", args.prepared_cache_dir,
        "--out-dir", args.tape_dir,
        "--artifact-dir", args.artifact_dir,
        "--step2-latency-mode", args.step2_latency_mode,
        "--step2-latency-model", args.step2_latency_model,
        "--step2-latency-percentile", _latency_percentile_arg(args.step2_latency_percentile),
    ]
    if args.no_outcome_shards:
        cmd.append("--no-outcome-shards")
    return [str(part) for part in cmd]


def _compile_cmd(args: argparse.Namespace, start: str, end: str, tickers: list[str], name: str) -> list[str]:
    return [
        sys.executable,
        "decision_tape_compiled.py",
        "--start", start,
        "--end", end,
        "--tickers", *tickers,
        "--feed", args.feed,
        "--quote-mode", args.quote_mode,
        "--btc-mode", args.btc_mode,
        "--indicator-mode", args.indicator_mode,
        "--tape-dir", args.tape_dir,
        "--out-dir", args.compiled_dir,
        "--name", name,
    ]


def _range_link_cmd(args: argparse.Namespace, name: str, manifest: str) -> list[str]:
    return [
        sys.executable,
        "step2_range_linker.py",
        "--source-manifest", manifest,
        "--out-dir", args.compiled_dir,
        "--name", name,
    ]


def _linked_name(name: str) -> str:
    raw = str(name or "").strip()
    return raw if raw.endswith("_linked") else f"{raw}_linked"


def _certify_cmd(args: argparse.Namespace, start: str, end: str, tickers: list[str], manifest: str) -> list[str]:
    cmd = [
        sys.executable,
        "certify_step2_cache.py",
        "--start", start,
        "--end", end,
        "--tickers", *tickers,
        "--feed", args.feed,
        "--quote-mode", args.quote_mode,
        "--btc-mode", args.btc_mode,
        "--indicator-mode", args.indicator_mode,
        "--compiled-manifest", manifest,
        "--workers", str(worker_policy.clamp_workers(args.workers)),
        "--overlap-sec", str(int(args.overlap_sec or 0)),
        "--step2-latency-mode", args.step2_latency_mode,
        "--step2-latency-model", args.step2_latency_model,
        "--step2-latency-percentile", _latency_percentile_arg(args.step2_latency_percentile),
        "--json",
    ]
    if not args.use_state_checkpoints:
        cmd.append("--no-state-checkpoints")
    return [str(part) for part in cmd]


def _contiguous_groups(day_plans: list[dict[str, Any]], action: str) -> list[list[dict[str, Any]]]:
    groups: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    for row in day_plans:
        if row.get("action") != action:
            if current:
                groups.append(current)
                current = []
            continue
        current.append(row)
    if current:
        groups.append(current)
    return groups


def _build_commands(args: argparse.Namespace, day_plans: list[dict[str, Any]], tickers: list[str]) -> list[dict[str, Any]]:
    commands: list[dict[str, Any]] = []
    for action in ("full_signal_rebuild", "refresh_outcomes_compile"):
        for group in _contiguous_groups(day_plans, action):
            if action == "refresh_outcomes_compile":
                incremental_rows = [
                    row for row in group
                    if ((row.get("decision_window") or {}).get("decision_start_ts"))
                ]
                if incremental_rows:
                    for row in group:
                        start = str(row["day"])
                        cmd = _base_build_cmd(args, start, start, tickers)
                        cmd = _replace_script(cmd, "step2_incremental_outcome_refresh.py")
                        cmd.append("--reuse-existing-signals")
                        window = row.get("decision_window") or {}
                        if window.get("decision_start_ts"):
                            cmd.extend(["--decision-start-ts", str(int(window.get("decision_start_ts") or 0))])
                        if window.get("decision_end_ts"):
                            cmd.extend(["--decision-end-ts", str(int(window.get("decision_end_ts") or 0))])
                        commands.append({
                            "action": action,
                            "start": start,
                            "end": start,
                            "days": [start],
                            "outcome_refresh_window": window,
                            "cmd": cmd,
                        })
                    continue
            start = str(group[0]["day"])
            end = str(group[-1]["day"])
            cmd = _base_build_cmd(args, start, end, tickers)
            if action == "refresh_outcomes_compile":
                cmd.append("--reuse-existing-signals")
            commands.append({
                "action": action,
                "start": start,
                "end": end,
                "days": [row["day"] for row in group],
                "cmd": cmd,
            })
    for row in day_plans:
        if row.get("action") != "incremental_append":
            continue
        cmd = _base_build_cmd(args, str(row["day"]), str(row["day"]), tickers)
        cmd.extend(["--decision-start-ts", str(int(row.get("decision_start_ts") or 0)), "--append-existing"])
        if args.use_state_checkpoints:
            cmd.extend(["--use-state-checkpoints", "--checkpoint-bucket-sec", str(int(args.checkpoint_bucket_sec or 300))])
        commands.append({
            "action": "incremental_append",
            "start": row["day"],
            "end": row["day"],
            "decision_start_ts": int(row.get("decision_start_ts") or 0),
            "days": [row["day"]],
            "cmd": cmd,
        })
    return commands


def _range_certified_day_plans(
    args: argparse.Namespace,
    *,
    manifest: str,
    days: list[str],
    tickers: list[str],
    compiled_name: str,
) -> list[dict[str, Any]] | None:
    if args.force_full or args.force_reuse_existing_signals or args.use_incremental_market_store:
        return None
    if not os.path.exists(manifest):
        return None
    manifest_path = Path(manifest)
    lineage_report = certify_step2_cache._lineage_report_for_manifest(manifest_path)
    lineage = lineage_report.get("compiled_tape_lineage") if isinstance(lineage_report.get("compiled_tape_lineage"), dict) else {}
    manifest_payload = certify_step2_cache._read_json(manifest_path)
    actual_scope = certify_step2_cache._scope_from_manifest(manifest_payload)
    requested_scope = {
        "day": "",
        "start": args.start,
        "end": args.end,
        "tickers": tickers,
    }
    if certify_step2_cache._scope_blockers(requested_scope, actual_scope):
        return None
    if not (lineage.get("certified") is True and lineage.get("status") == "CERTIFIED_MATCH"):
        return None
    plans = []
    for day in days:
        plans.append({
            "schema_version": step2_rebuild_planner.SCHEMA_VERSION,
            "source": "step2_fast_rebuild_range_shortcut",
            "created_at_ct": _now_ct(),
            "day": day,
            "tickers": tickers,
            "feed": args.feed,
            "quote_mode": args.quote_mode,
            "btc_mode": args.btc_mode,
            "indicator_mode": args.indicator_mode,
            "compiled_name": compiled_name,
            "action": "score_only",
            "reason": "range_manifest_certified",
            "decision_start_ts": 0,
            "overlap_sec": int(args.overlap_sec or 0),
            "reuse_existing_signals": False,
            "full_rebuild": False,
            "recompile_only": False,
            "append_existing": False,
            "score_only": True,
            "certified_score_only": True,
            "uncertified_score_only": False,
            "paths": {},
            "decision_tape_watermark": {},
            "cache_action": "score_only",
            "lineage_status": lineage.get("status"),
            "lineage_certified": lineage.get("certified"),
            "lineage_quick_score_allowed": lineage.get("quick_score_allowed"),
            "lineage_rebuild_required": lineage.get("rebuild_required"),
            "stale_layers": [],
            "changed_partition_count": 0,
            "use_incremental_market_store": False,
            "range_shortcut": {
                "enabled": True,
                "manifest": manifest,
                "compiled_tape_hash": actual_scope.get("compiled_tape_hash"),
                "array_payload_sha256": manifest_payload.get("array_payload_sha256"),
            },
        })
    return plans


def plan(args: argparse.Namespace) -> dict[str, Any]:
    tickers = [str(ticker).upper() for ticker in args.tickers]
    days = _days(args.start, args.end)
    scope = _scope_id(args.start, args.end, tickers)
    compiled_name = args.name or _default_name(args.start, args.end, tickers)
    source_manifest = str((Path(args.compiled_dir) / compiled_name / "manifest.json").resolve())
    linked_name = _linked_name(compiled_name)
    linked_manifest = str((Path(args.compiled_dir) / linked_name / "manifest.json").resolve())
    shortcut_manifest = linked_manifest if os.path.exists(linked_manifest) else source_manifest
    day_plans = _range_certified_day_plans(
        args,
        manifest=shortcut_manifest,
        days=days,
        tickers=tickers,
        compiled_name=compiled_name,
    )
    range_shortcut_used = day_plans is not None
    if day_plans is None:
        day_plans = []
        for day in days:
            day_plans.append(step2_rebuild_planner.plan(
                day=day,
                tickers=tickers,
                feed=args.feed,
                quote_mode=args.quote_mode,
                btc_mode=args.btc_mode,
                indicator_mode=args.indicator_mode,
                compiled_name=compiled_name,
                overlap_sec=int(args.overlap_sec or 0),
                force_full=bool(args.force_full),
            force_reuse_existing_signals=bool(args.force_reuse_existing_signals),
            use_incremental_market_store=bool(args.use_incremental_market_store),
            artifact_dir=args.artifact_dir,
            step2_latency_mode=args.step2_latency_mode,
            step2_latency_percentile=_latency_percentile_arg(args.step2_latency_percentile),
            step2_latency_model=args.step2_latency_model,
        ))
    unknown = [row for row in day_plans if row.get("action") not in BUILD_ACTIONS | NO_BUILD_ACTIONS]
    build_commands = [] if args.compile_only else _build_commands(args, day_plans, tickers)
    compile_needed = bool(args.compile_only or build_commands or any(row.get("action") == "recompile_only" for row in day_plans))
    if args.force_compile:
        compile_needed = True
    range_link_possible = bool(
        getattr(args, "use_range_linker", True)
        and compile_needed
        and not build_commands
        and os.path.exists(source_manifest)
    )
    if range_link_possible:
        compile_needed = False
    certify_needed = not bool(args.no_certify)
    final_action = "certify_only"
    build_day_count = sum(len(row.get("days") or []) for row in build_commands)
    if build_commands:
        final_action = "partial_rebuild_compile_certify" if build_day_count < len(days) else "rebuild_compile_certify"
    elif range_link_possible:
        final_action = "range_link_certify"
    elif compile_needed:
        final_action = "compile_certify"
    if unknown:
        final_action = "blocked_unknown_plan_action"
    final_manifest = linked_manifest if range_link_possible else (shortcut_manifest if range_shortcut_used else source_manifest)
    final_compiled_name = linked_name if range_link_possible else Path(final_manifest).parent.name
    compile_command = _compile_cmd(args, args.start, args.end, tickers, compiled_name) if compile_needed else []
    range_link_command = _range_link_cmd(args, linked_name, source_manifest) if range_link_possible else []
    certify_command = _certify_cmd(args, args.start, args.end, tickers, final_manifest) if certify_needed else []
    action_counts: dict[str, int] = {}
    for row in day_plans:
        action_counts[str(row.get("action"))] = action_counts.get(str(row.get("action")), 0) + 1
    payload = {
        "schema_version": 1,
        "source": "step2_fast_rebuild",
        "created_at_ct": _now_ct(),
        "start": args.start,
        "end": args.end,
        "tickers": tickers,
        "days": days,
        "day_count": len(days),
        "compiled_name": compiled_name,
        "final_compiled_name": final_compiled_name,
        "compiled_manifest": final_manifest,
        "source_compiled_manifest": source_manifest,
        "linked_compiled_manifest": linked_manifest,
        "scope_id": scope,
        "final_action": final_action,
        "ok_to_execute": not unknown,
        "action_counts": action_counts,
        "build_day_count": build_day_count,
        "build_command_count": len(build_commands),
        "compile_needed": compile_needed,
        "range_link_needed": range_link_possible,
        "certify_needed": certify_needed,
        "day_plans": day_plans,
        "range_shortcut_used": range_shortcut_used,
        "build_commands": build_commands,
        "execution_overrides": {
            "compile_only": bool(args.compile_only),
            "force_full": bool(args.force_full),
            "force_reuse_existing_signals": bool(args.force_reuse_existing_signals),
            "force_compile": bool(args.force_compile),
            "use_range_linker": bool(getattr(args, "use_range_linker", True)),
            "resume": bool(getattr(args, "resume", True)),
            "staged_promotion": bool(getattr(args, "staged_promotion", True)),
        },
        "staging": {
            "enabled_if_executed": bool(getattr(args, "staged_promotion", True) and (build_commands or compile_command)),
            "root": str(Path(getattr(args, "staging_dir", STAGING_ROOT)).resolve()),
            "promotion": "after_certification" if certify_needed else "after_compile_no_certify",
        },
        "compile_command": compile_command,
        "range_link_command": range_link_command,
        "certify_command": certify_command,
        "deduction": (
            "Fast rebuild uses layered lineage to avoid monolithic range rebuilds: "
            "score-only days are left alone, outcome-only drift reuses existing signal rows, "
            "incremental days append from the changed watermark, and the range compiles once."
        ),
    }
    payload["path"] = _write_json(OUT_DIR / f"step2_fast_rebuild_plan_{scope}.json", payload)
    return payload


def _run(cmd: list[str], executed: list[dict[str, Any]]) -> None:
    started = time.perf_counter()
    print(json.dumps({"event": "run", "cmd": cmd}), flush=True)
    subprocess.run(cmd, cwd=HERE, check=True)
    executed.append({
        "cmd": cmd,
        "elapsed_sec": round(time.perf_counter() - started, 3),
    })


def _decision_tape_path(tape_dir: str, args: argparse.Namespace, day: str, tickers: list[str]) -> str:
    ticker_part = "-".join(tickers)
    return str(Path(tape_dir) / (
        f"decision_tape_{args.feed}_{args.quote_mode}_{args.btc_mode}_{args.indicator_mode}_{ticker_part}_{day}.jsonl.gz"
    ))


def _load_state(scope: str, resume: bool, staging_root: str) -> dict[str, Any]:
    if not resume:
        return {}
    state = _read_json(_state_path(scope), {}) or {}
    if state.get("status") == "promoted":
        return {}
    if state.get("staging_root") and str(state.get("staging_root")) != str(staging_root):
        return {}
    return state


def _save_state(scope: str, state: dict[str, Any]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    _write_json(_state_path(scope), state)


def _prepare_staging(
    args: argparse.Namespace,
    payload: dict[str, Any],
    tickers: list[str],
    state: dict[str, Any],
) -> dict[str, Any]:
    scope = str(payload.get("scope_id") or _scope_id(args.start, args.end, tickers))
    run_id = str(state.get("run_id") or f"{int(time.time())}-{os.getpid()}")
    root = Path(getattr(args, "staging_dir", STAGING_ROOT)).resolve()
    setattr(_staging_paths, "root", root)
    paths = _staging_paths(scope, run_id)
    for path in paths.values():
        Path(path).mkdir(parents=True, exist_ok=True)
    copied = []
    for day in payload.get("days") or []:
        src = _decision_tape_path(args.tape_dir, args, str(day), tickers)
        dst = _decision_tape_path(paths["tape_dir"], args, str(day), tickers)
        if step2_artifact_identity.copy_if_exists(src, dst):
            copied.append({"day": day, "source": src, "staged": dst})
    state.update({
        "schema_version": 1,
        "source": "step2_fast_rebuild",
        "scope_id": scope,
        "run_id": run_id,
        "staging_root": str(root),
        "staging_paths": paths,
        "started_at_ct": state.get("started_at_ct") or _now_ct(),
        "completed_commands": state.get("completed_commands") or {},
        "status": "running",
    })
    _save_state(scope, state)
    return {"paths": paths, "preseeded_tapes": copied, "run_id": run_id}


def _stage_cmd(cmd: list[str], staging: dict[str, str]) -> list[str]:
    out = [str(part) for part in cmd]
    script = _script_name(out)
    if script == "build_decision_tape.py":
        out = _replace_arg_value(out, "--out-dir", staging["tape_dir"])
        out = _replace_arg_value(out, "--artifact-dir", staging["artifact_dir"])
    elif script == "decision_tape_compiled.py":
        out = _replace_arg_value(out, "--tape-dir", staging["tape_dir"])
        out = _replace_arg_value(out, "--out-dir", staging["compiled_dir"])
    elif script == "certify_step2_cache.py":
        manifest = _arg_value(out, "--compiled-manifest")
        if manifest:
            compiled_name = Path(manifest).parent.name
            staged_manifest = str(Path(staging["compiled_dir"]) / compiled_name / "manifest.json")
            out = _replace_arg_value(out, "--compiled-manifest", staged_manifest)
    return out


def _replace_prefix(value: Any, replacements: dict[str, str]) -> Any:
    if isinstance(value, str):
        normalized = value.replace("\\", "/")
        for old, new in replacements.items():
            old_norm = old.replace("\\", "/")
            if normalized.startswith(old_norm):
                suffix = normalized[len(old_norm):].lstrip("/")
                return str(Path(new) / suffix) if suffix else str(Path(new))
        return value
    if isinstance(value, list):
        return [_replace_prefix(item, replacements) for item in value]
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, item in value.items():
            new_key = _replace_prefix(str(key), replacements) if isinstance(key, str) else key
            out[new_key] = _replace_prefix(item, replacements)
        return out
    return value


def _patch_staged_manifest_paths(args: argparse.Namespace, payload: dict[str, Any], staging: dict[str, str]) -> dict[str, Any]:
    compiled_name = str(payload.get("compiled_name") or "")
    manifest_path = Path(staging["compiled_dir"]) / compiled_name / "manifest.json"
    manifest = _read_json(manifest_path, {}) or {}
    replacements = {
        str(Path(staging["compiled_dir"]).resolve()): str(Path(args.compiled_dir).resolve()),
        str(Path(staging["tape_dir"]).resolve()): str(Path(args.tape_dir).resolve()),
        str(Path(staging["artifact_dir"]).resolve()): str(Path(args.artifact_dir).resolve()),
    }
    patched = _replace_prefix(manifest, replacements)
    if isinstance(patched, dict):
        patched["compiled_tape_hash"] = certify_step2_cache.tournament_safety.stable_json_hash(
            certify_step2_cache._compiled_tape_hash_payload(patched),
            32,
        )
        step2_artifact_identity.write_json(manifest_path, patched)
        return {
            "manifest_path": str(manifest_path.resolve()),
            "compiled_tape_hash": patched.get("compiled_tape_hash"),
            "arrays_path": patched.get("arrays_path"),
            "source_paths": patched.get("source_paths"),
        }
    return {"manifest_path": str(manifest_path.resolve()), "error": "manifest_not_object"}


def _run_resumable(
    *,
    label: str,
    cmd: list[str],
    executed: list[dict[str, Any]],
    scope: str,
    state: dict[str, Any],
    resume: bool,
) -> None:
    cmd = [str(part) for part in cmd]
    cid = _command_id(label, cmd)
    completed = state.setdefault("completed_commands", {})
    if resume and cid in completed:
        executed.append({"cmd": cmd, "elapsed_sec": 0.0, "skipped_resume": True, "command_id": cid})
        return
    started = time.perf_counter()
    print(json.dumps({"event": "run", "label": label, "cmd": cmd}), flush=True)
    subprocess.run(cmd, cwd=HERE, check=True)
    row = {
        "cmd": cmd,
        "label": label,
        "command_id": cid,
        "elapsed_sec": round(time.perf_counter() - started, 3),
        "completed_at_ct": _now_ct(),
    }
    completed[cid] = row
    executed.append(row)
    _save_state(scope, state)


def _promote_staged_outputs(
    args: argparse.Namespace,
    payload: dict[str, Any],
    staging: dict[str, str],
    tickers: list[str],
) -> dict[str, Any]:
    built_days = sorted({
        str(day)
        for command in payload.get("build_commands") or []
        for day in (command.get("days") or [])
    })
    promoted_tapes = []
    for day in built_days:
        staged = _decision_tape_path(staging["tape_dir"], args, day, tickers)
        final = _decision_tape_path(args.tape_dir, args, day, tickers)
        promoted_tapes.append(step2_artifact_identity.promote_file(staged, final))
    promoted_compiled = None
    if payload.get("compile_command"):
        compiled_name = str(payload.get("compiled_name") or "")
        promoted_compiled = step2_artifact_identity.promote_tree(
            Path(staging["compiled_dir"]) / compiled_name,
            Path(args.compiled_dir) / compiled_name,
        )
    return {
        "promoted_tapes": promoted_tapes,
        "promoted_compiled": promoted_compiled,
        "built_days": built_days,
    }


def execute(payload: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    if not payload.get("ok_to_execute"):
        payload["executed"] = False
        payload["execution_error"] = "plan_contains_unknown_action"
        return payload
    if args.plan_only:
        payload["executed"] = False
        payload["execution_skipped_reason"] = "plan_only"
        return payload
    executed: list[dict[str, Any]] = []
    started = time.perf_counter()
    tickers = [str(ticker).upper() for ticker in args.tickers]
    scope = str(payload.get("scope_id") or _scope_id(args.start, args.end, tickers))
    resume = bool(getattr(args, "resume", True))
    state: dict[str, Any] = {}
    staging_payload: dict[str, Any] = {"enabled": False}
    staging_paths: dict[str, str] | None = None
    if _staging_enabled(args, payload):
        state = _load_state(scope, resume, str(Path(getattr(args, "staging_dir", STAGING_ROOT)).resolve()))
        staging_payload = _prepare_staging(args, payload, tickers, state)
        staging_paths = dict(staging_payload["paths"])
        payload["staging"] = {
            **(payload.get("staging") or {}),
            "enabled": True,
            "run_id": staging_payload.get("run_id"),
            "paths": staging_paths,
            "preseeded_tapes": staging_payload.get("preseeded_tapes") or [],
        }
    elif resume:
        state = _load_state(scope, resume, str(Path(getattr(args, "staging_dir", STAGING_ROOT)).resolve()))
    for command in payload.get("build_commands") or []:
        cmd = [str(part) for part in command["cmd"]]
        if staging_paths:
            cmd = _stage_cmd(cmd, staging_paths)
        _run_resumable(
            label=f"build:{command.get('action')}:{command.get('start')}:{command.get('end')}",
            cmd=cmd,
            executed=executed,
            scope=scope,
            state=state,
            resume=resume,
        )
    if payload.get("compile_command"):
        cmd = [str(part) for part in payload["compile_command"]]
        if staging_paths:
            cmd = _stage_cmd(cmd, staging_paths)
        _run_resumable(
            label="compile",
            cmd=cmd,
            executed=executed,
            scope=scope,
            state=state,
            resume=resume,
        )
        if staging_paths:
            payload.setdefault("staging", {})["manifest_path_patch"] = _patch_staged_manifest_paths(args, payload, staging_paths)
    if payload.get("range_link_command"):
        cmd = [str(part) for part in payload["range_link_command"]]
        _run_resumable(
            label="range-link",
            cmd=cmd,
            executed=executed,
            scope=scope,
            state=state,
            resume=resume,
        )
    if payload.get("certify_command"):
        cmd = [str(part) for part in payload["certify_command"]]
        if staging_paths:
            cmd = _stage_cmd(cmd, staging_paths)
        _run_resumable(
            label="certify",
            cmd=cmd,
            executed=executed,
            scope=scope,
            state=state,
            resume=resume,
        )
    if staging_paths:
        promotion = _promote_staged_outputs(args, payload, staging_paths, tickers)
        payload["promotion"] = promotion
        state["status"] = "promoted"
        state["promoted_at_ct"] = _now_ct()
        state["promotion"] = promotion
        _save_state(scope, state)
    payload["executed"] = True
    payload["executed_commands"] = executed
    payload["execution_elapsed_sec"] = round(time.perf_counter() - started, 3)
    payload["path"] = _write_json(Path(payload["path"]), payload)
    return payload


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Fast layered Step 2 cache rebuild orchestrator.")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--tickers", nargs="+", default=["CLSK", "MARA", "RIOT"])
    ap.add_argument("--feed", default="sip")
    ap.add_argument("--quote-mode", default="per-second")
    ap.add_argument("--btc-mode", default="bars")
    ap.add_argument("--indicator-mode", choices=["live", "fast"], default="live")
    ap.add_argument("--workers", type=int, default=worker_policy.DEFAULT_MAX_WORKERS)
    ap.add_argument("--cache-dir", default=build_decision_tape.replay.DEFAULT_CACHE_DIR)
    ap.add_argument("--prepared-cache-dir", default=build_decision_tape.replay.DEFAULT_PREPARED_DIR)
    ap.add_argument("--tape-dir", default=build_decision_tape.DEFAULT_OUT_DIR)
    ap.add_argument("--compiled-dir", default=decision_tape_compiled.DEFAULT_OUT_DIR)
    ap.add_argument("--artifact-dir", default=replay_artifacts.DEFAULT_ARTIFACT_DIR)
    ap.add_argument("--name", default="")
    ap.add_argument("--source", choices=["signal-scan", "opportunity-ledger"], default="signal-scan")
    ap.add_argument("--no-outcome-shards", action="store_true")
    ap.add_argument("--no-event-cache", action="store_true")
    ap.add_argument("--overlap-sec", type=int, default=120)
    ap.add_argument("--use-incremental-market-store", action="store_true",
                    help="Use live incremental-market-store changed partitions when planning this range.")
    ap.add_argument("--force-full", action="store_true")
    ap.add_argument("--force-reuse-existing-signals", action="store_true")
    ap.add_argument("--force-compile", action="store_true")
    ap.add_argument("--use-range-linker", action="store_true",
                    help="Build a linked range manifest from certified day shards instead of recompiling arrays when safe. This is now the default.")
    ap.add_argument("--no-range-linker", dest="use_range_linker", action="store_false",
                    help="Disable linked range manifests and force the older compile path when compile-only drift is present.")
    ap.set_defaults(use_range_linker=True)
    ap.add_argument("--compile-only", action="store_true")
    ap.add_argument("--no-certify", action="store_true")
    ap.add_argument("--plan-only", action="store_true")
    ap.add_argument("--no-staging", dest="staged_promotion", action="store_false",
                    help="Write rebuild outputs directly instead of staging and promoting after certification.")
    ap.set_defaults(staged_promotion=True)
    ap.add_argument("--staging-dir", default=str(STAGING_ROOT))
    ap.add_argument("--no-resume", dest="resume", action="store_false",
                    help="Ignore previous successful command checkpoints for this rebuild scope.")
    ap.set_defaults(resume=True)
    ap.add_argument("--no-state-checkpoints", dest="use_state_checkpoints", action="store_false")
    ap.set_defaults(use_state_checkpoints=True)
    ap.add_argument("--checkpoint-bucket-sec", type=int, default=300)
    ap.add_argument("--step2-latency-mode", choices=["off", "entry", "entry-exit"], default="entry-exit")
    ap.add_argument("--step2-latency-model", default=step2_latency_model.DEFAULT_MODEL_PATH)
    ap.add_argument("--step2-latency-percentile", default="p75")
    ap.add_argument("--json", action="store_true")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    payload = plan(args)
    payload = execute(payload, args)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    else:
        print(json.dumps({
            "path": payload.get("path"),
            "executed": payload.get("executed"),
            "final_action": payload.get("final_action"),
            "action_counts": payload.get("action_counts"),
            "build_day_count": payload.get("build_day_count"),
            "build_command_count": payload.get("build_command_count"),
            "compile_needed": payload.get("compile_needed"),
            "range_link_needed": payload.get("range_link_needed"),
            "certify_needed": payload.get("certify_needed"),
            "compiled_manifest": payload.get("compiled_manifest"),
        }, indent=2, sort_keys=True, default=str))
    return 0 if payload.get("ok_to_execute") else 2


if __name__ == "__main__":
    raise SystemExit(main())
