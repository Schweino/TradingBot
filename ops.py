from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from urllib.request import urlopen

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

import canonical_command_registry
import run_supervisor
import worker_policy
from automation_ops import ensure_app_running, run_phase, start_live_monitor


HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = Path(HERE)
OUT_DIR = os.path.join(HERE, "postmortem")
CT = ZoneInfo("America/Chicago")
STATUS_URL = "http://127.0.0.1:5000/mock/status"
PYTHON = sys.executable


def _today() -> str:
    return datetime.now(CT).date().isoformat()


def _stamp() -> str:
    return datetime.now(CT).strftime("%Y%m%d_%H%M%S")


def _read_status(timeout: int = 5) -> dict:
    try:
        with urlopen(STATUS_URL, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        return {"ok": True, "status": payload}
    except Exception as exc:
        return {"ok": False, "error": str(exc), "status": {}}


def _compact_status() -> dict:
    raw = _read_status()
    status = raw.get("status") or {}
    payload = {
        "ok": raw.get("ok"),
        "generated_at_ct": datetime.now(CT).isoformat(timespec="seconds"),
        "app_url": STATUS_URL,
        "running": status.get("running"),
        "open_position_count": status.get("open_position_count"),
        "pending_entries": status.get("pending_entries"),
        "broker_exposure_block": status.get("broker_exposure_block"),
        "broker_api_degraded": status.get("broker_api_degraded"),
        "kill_switch": status.get("kill_switch"),
        "flatten_cutoff_ct": status.get("flatten_cutoff_ct"),
        "alpaca_equity": status.get("alpaca_equity"),
        "alpaca_cash": status.get("alpaca_cash"),
        "positions": status.get("positions"),
    }
    if not raw.get("ok"):
        payload["error"] = raw.get("error")
    return payload


def _safe_name(value: str) -> str:
    clean = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in str(value or "ops"))
    return clean.strip("_") or "ops"


def _write_payload(name: str, day: str, payload: dict) -> dict:
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, f"ops_{_safe_name(name)}_{day}.json")
    payload["artifact"] = path
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)
    return payload


def _write_codex_context(day: str) -> dict:
    from review_artifacts import build_artifact_manifest, build_codex_context, build_review_index

    index = build_review_index(day)
    manifest = build_artifact_manifest(day)
    text = build_codex_context(day, index, manifest)
    path = os.path.join(HERE, "CODEX_CONTEXT.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    latest_index = os.path.join(OUT_DIR, "REVIEW_INDEX_LATEST.json")
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(latest_index, "w", encoding="utf-8") as f:
        json.dump(index, f, indent=2, default=str)
    return {
        "ok": True,
        "command": "context",
        "day": day,
        "created_at_ct": datetime.now(CT).isoformat(timespec="seconds"),
        "context_file": path,
        "review_index_latest": latest_index,
    }


def _tracked(
    *,
    name: str,
    day: str,
    command: list[str],
    timeout_sec: float | int | None = None,
    workers: int | None = None,
) -> dict:
    timeout = float(timeout_sec or 0)
    result = run_supervisor.run_and_track(
        command,
        cwd=HERE,
        timeout=timeout or None,
        name=name,
        workers=worker_policy.clamp_workers(workers),
    )
    result.update(
        {
            "ok": bool(result.get("ok")),
            "command_name": name,
            "day": day,
            "created_at_ct": datetime.now(CT).isoformat(timespec="seconds"),
        }
    )
    return _write_payload(name, day, result)


def _run_hunt(day: str, opts: dict) -> dict:
    name = opts.get("name") or f"canonical_step2_hunt_{_stamp()}"
    cmd = [
        PYTHON,
        "step2_adaptive_hunter.py",
        "--name",
        str(name),
        "--batch-size",
        str(int(opts.get("batch_size") or 500)),
        "--target-count",
        str(int(opts.get("target_count") or 1)),
        "--beat-pct",
        str(float(opts.get("beat_pct") if opts.get("beat_pct") is not None else 5.0)),
        "--max-batches",
        str(int(opts.get("max_batches") or 200)),
        "--start-balance",
        str(float(opts.get("start_balance") or 100000.0)),
        "--seed",
        str(int(opts.get("seed") or 20260507)),
    ]
    if opts.get("compiled_decision_tape"):
        cmd.extend(["--compiled-decision-tape", str(opts["compiled_decision_tape"])])
    if opts.get("out_dir"):
        cmd.extend(["--out-dir", str(opts["out_dir"])])
    for seed_json in opts.get("seed_json") or []:
        cmd.extend(["--seed-json", str(seed_json)])
    if opts.get("allow_data_integrity_fail"):
        cmd.append("--allow-data-integrity-fail")
    return _tracked(
        name="hunt",
        day=day,
        command=cmd,
        timeout_sec=opts.get("timeout_sec"),
        workers=opts.get("workers"),
    )


def _run_promote(day: str, opts: dict) -> dict:
    if not opts.get("candidate_json") and not opts.get("record_current"):
        return _write_payload(
            "promote",
            day,
            {
                "ok": False,
                "command_name": "promote",
                "day": day,
                "error": "promote requires --candidate-json unless --record-current is used",
            },
        )
    cmd = [PYTHON, "promote_active_profile.py"]
    if opts.get("candidate_json"):
        cmd.extend(["--candidate-json", str(opts["candidate_json"])])
    if opts.get("variant"):
        cmd.extend(["--variant", str(opts["variant"])])
    cmd.extend(["--rank", str(int(opts.get("rank") or 1))])
    if opts.get("reason"):
        cmd.extend(["--reason", str(opts["reason"])])
    if opts.get("record_current"):
        cmd.append("--record-current")
    if opts.get("dry_run"):
        cmd.append("--dry-run")
    if opts.get("restart", True):
        cmd.append("--restart")
    if opts.get("allow_execution_change"):
        cmd.append("--allow-execution-change")
    if opts.get("allow_unapproved_candidate"):
        cmd.append("--allow-unapproved-candidate")
    if opts.get("allow_preflight_failure"):
        cmd.append("--allow-preflight-failure")
    evidence_days = opts.get("evidence_days") or []
    if evidence_days:
        cmd.append("--evidence-days")
        cmd.extend(str(day_value) for day_value in evidence_days)
    return _tracked(
        name="promote",
        day=day,
        command=cmd,
        timeout_sec=opts.get("timeout_sec"),
        workers=1,
    )


def _run_parity(day: str, opts: dict) -> dict:
    cmd = [PYTHON, "live_step2_parity_report.py", day]
    if opts.get("out_dir"):
        cmd.extend(["--out-dir", str(opts["out_dir"])])
    cmd.append("--json")
    return _tracked(
        name="parity",
        day=day,
        command=cmd,
        timeout_sec=opts.get("timeout_sec") or 600,
        workers=opts.get("workers"),
    )


def _run_scorecard(day: str, opts: dict) -> dict:
    cmd = [PYTHON, "daily_parity_scorecard.py", day, "--json"]
    if opts.get("refresh_report"):
        cmd.append("--refresh-report")
    return _tracked(
        name="scorecard",
        day=day,
        command=cmd,
        timeout_sec=opts.get("timeout_sec") or 600,
        workers=opts.get("workers"),
    )


def _run_step3_audit(day: str, opts: dict) -> dict:
    start = opts.get("start") or day
    end = opts.get("end") or start
    cmd = [PYTHON, "mock_replay.py", "--start", str(start), "--end", str(end)]
    tickers = opts.get("tickers") or ["CLSK", "MARA", "RIOT"]
    if tickers:
        cmd.append("--tickers")
        cmd.extend(str(ticker).upper() for ticker in tickers)
    if opts.get("scoring_profile"):
        cmd.extend(["--scoring-profile", str(opts["scoring_profile"])])
    if opts.get("start_balance") is not None:
        cmd.extend(["--start-balance", str(float(opts["start_balance"]))])
    if opts.get("actual_sizing"):
        cmd.append("--actual-sizing")
    if opts.get("require_complete_days"):
        cmd.append("--require-complete-days")
    if opts.get("out_dir"):
        cmd.extend(["--out-dir", str(opts["out_dir"])])
    return _tracked(
        name="step3-audit",
        day=str(end),
        command=cmd,
        timeout_sec=opts.get("timeout_sec") or 1200,
        workers=opts.get("workers"),
    )


def _run_process_singleton_check(day: str) -> dict:
    import live_engine_ops

    pids = live_engine_ops._port_5000_pids()
    status = live_engine_ops._read_status(timeout=3.0, full=True)
    reachable = not (isinstance(status, dict) and status.get("reachable") is False)
    running = bool(status.get("running")) if isinstance(status, dict) else False
    checks = [
        {
            "name": "exactly_one_port_5000_listener",
            "ok": len(pids) == 1,
            "actual": pids,
            "expected": "one live app listener",
        },
        {
            "name": "mock_status_reachable",
            "ok": reachable,
            "actual": status.get("error") if isinstance(status, dict) else None,
            "expected": "reachable /mock/status",
        },
        {
            "name": "mock_engine_running",
            "ok": running,
            "actual": status.get("running") if isinstance(status, dict) else None,
            "expected": True,
        },
    ]
    return {
        "ok": all(row["ok"] for row in checks),
        "command_name": "readiness_process_singleton",
        "day": day,
        "created_at_ct": datetime.now(CT).isoformat(timespec="seconds"),
        "port_5000_pids": pids,
        "checks": checks,
    }


def _run_frozen_identity_check(day: str) -> dict:
    import live_engine_ops

    status = live_engine_ops._read_status(timeout=3.0)
    reachable = not (isinstance(status, dict) and status.get("reachable") is False)
    startup = status.get("startup_self_check") if isinstance(status, dict) else {}
    active = status.get("active_scoring_profile") if isinstance(status, dict) else {}
    provenance = status.get("promotion_provenance") if isinstance(status, dict) else {}
    config_provenance = status.get("config_provenance") if isinstance(status, dict) else {}
    manifest = (
        (provenance or {}).get("promotion_manifest_integrity")
        or (startup or {}).get("promotion_manifest_integrity")
        or {}
    )
    latest_manifest = manifest.get("latest_pointer") or {}

    live_kernel = status.get("execution_kernel_hash") if isinstance(status, dict) else None
    parity_hash = status.get("step2_parity_contract_hash") if isinstance(status, dict) else None
    execution_hash = status.get("step2_execution_contract_hash") if isinstance(status, dict) else None
    config_hash = status.get("strategy_config_hash") if isinstance(status, dict) else None
    startup_kernel = startup.get("execution_kernel_hash") if isinstance(startup, dict) else None
    startup_parity = startup.get("step2_parity_contract_hash") if isinstance(startup, dict) else None
    startup_execution = startup.get("step2_execution_contract_hash") if isinstance(startup, dict) else None
    startup_config = startup.get("strategy_config_hash") if isinstance(startup, dict) else None
    active_hash = active.get("hash") if isinstance(active, dict) else None

    checks = [
        {
            "name": "mock_status_reachable_for_identity",
            "ok": reachable,
            "actual": status.get("error") if isinstance(status, dict) else None,
            "expected": "reachable /mock/status",
        },
        {
            "name": "active_profile_hash_present",
            "ok": bool(active_hash),
            "actual": active_hash,
            "expected": "active scoring profile hash",
        },
        {
            "name": "execution_kernel_hash_frozen_since_boot",
            "ok": bool(live_kernel) and live_kernel == startup_kernel,
            "actual": live_kernel,
            "expected": startup_kernel,
        },
        {
            "name": "step2_parity_contract_hash_frozen_since_boot",
            "ok": bool(parity_hash) and parity_hash == startup_parity,
            "actual": parity_hash,
            "expected": startup_parity,
        },
        {
            "name": "step2_execution_contract_hash_frozen_since_boot",
            "ok": bool(execution_hash) and execution_hash == startup_execution,
            "actual": execution_hash,
            "expected": startup_execution,
        },
        {
            "name": "strategy_config_hash_frozen_since_boot",
            "ok": bool(config_hash) and config_hash == startup_config,
            "actual": config_hash,
            "expected": startup_config,
        },
        {
            "name": "promotion_manifest_integrity_ok",
            "ok": bool(manifest.get("ok")) and int(manifest.get("critical_failure_count") or 0) == 0,
            "actual": {
                "ok": manifest.get("ok"),
                "critical_failure_count": manifest.get("critical_failure_count"),
                "manifest_path": manifest.get("manifest_path") or latest_manifest.get("manifest_path"),
            },
            "expected": "valid active promotion manifest",
        },
        {
            "name": "config_provenance_ok",
            "ok": not config_provenance or bool(config_provenance.get("ok", True)),
            "actual": config_provenance,
            "expected": "valid config provenance or unavailable",
        },
    ]
    return {
        "ok": all(row["ok"] for row in checks),
        "command_name": "readiness_frozen_identity",
        "day": day,
        "created_at_ct": datetime.now(CT).isoformat(timespec="seconds"),
        "checks": checks,
        "identity": {
            "active_profile_hash": active_hash,
            "execution_kernel_hash": live_kernel,
            "step2_parity_contract_hash": parity_hash,
            "step2_execution_contract_hash": execution_hash,
            "strategy_config_hash": config_hash,
            "promotion_manifest_path": manifest.get("manifest_path") or latest_manifest.get("manifest_path"),
            "promotion_manifest_hash": manifest.get("manifest_hash") or latest_manifest.get("manifest_hash"),
        },
    }


def _run_direct_broker_check(day: str) -> dict:
    try:
        import live_engine_ops
        from alpaca_trading import AlpacaTrader

        with open(os.path.join(HERE, "trading_config.json"), "r", encoding="utf-8-sig") as f:
            cfg = json.load(f)
        watched = sorted(str(ticker).upper() for ticker in cfg.get("tickers", []))
        trader = AlpacaTrader.from_env(paper=True)
        positions = trader.list_positions()
        orders = trader.list_orders(status="open", symbols=watched, nested=True)
        watched_positions = sorted(
            str(pos.get("symbol", "")).upper()
            for pos in positions
            if str(pos.get("symbol", "")).upper() in watched
        )
        watched_orders = sorted(
            {
                str(order.get("symbol", "")).upper()
                for order in orders
                if str(order.get("symbol", "")).upper() in watched
            }
        )
        status = live_engine_ops._read_status(timeout=3.0)
        lifecycle = status.get("broker_lifecycle_gate") if isinstance(status, dict) else {}
        checks = [
            {
                "name": "direct_broker_reachable",
                "ok": True,
                "actual": "reachable",
                "expected": "reachable",
            },
            {
                "name": "broker_flat_for_watched_tickers",
                "ok": not watched_positions,
                "actual": watched_positions,
                "expected": [],
            },
            {
                "name": "no_watched_open_orders",
                "ok": not watched_orders,
                "actual": watched_orders,
                "expected": [],
            },
            {
                "name": "app_broker_lifecycle_block_clear",
                "ok": not (status.get("broker_lifecycle_block") if isinstance(status, dict) else None),
                "actual": status.get("broker_lifecycle_block") if isinstance(status, dict) else None,
                "expected": None,
            },
            {
                "name": "app_broker_lifecycle_gate_no_critical_issues",
                "ok": bool((lifecycle or {}).get("ok")) and int((lifecycle or {}).get("critical_count") or 0) == 0,
                "actual": {
                    "ok": (lifecycle or {}).get("ok"),
                    "critical_count": (lifecycle or {}).get("critical_count"),
                    "verdict": (lifecycle or {}).get("verdict"),
                    "enforced": (lifecycle or {}).get("enforced"),
                },
                "expected": "ok with zero critical issues",
            },
        ]
        return {
            "ok": all(row["ok"] for row in checks),
            "command_name": "readiness_direct_broker",
            "day": day,
            "created_at_ct": datetime.now(CT).isoformat(timespec="seconds"),
            "watched": watched,
            "broker_lifecycle_gate": lifecycle,
            "checks": checks,
        }
    except Exception as exc:
        return {
            "ok": False,
            "command_name": "readiness_direct_broker",
            "day": day,
            "created_at_ct": datetime.now(CT).isoformat(timespec="seconds"),
            "checks": [
                {
                    "name": "direct_broker_reachable",
                    "ok": False,
                    "actual": str(exc),
                    "expected": "reachable Alpaca paper API",
                }
            ],
        }


def _run_readiness(day: str, opts: dict) -> dict:
    checks = [
        ("smoke_no_surprises", [PYTHON, "smoke_check.py", "--mode", "no-surprises"], 240),
        ("architecture_drift", [PYTHON, "architecture_drift_gate.py", "--json"], 240),
        ("pre_market_parity", [PYTHON, "pre_market_parity_gate.py", day, "--json"], 300),
        ("promotion_preflight", [PYTHON, "promotion_preflight_bundle.py", "--no-write", "--json"], 240),
        ("rollback_drill", [PYTHON, "rollback_drill.py", "--no-write", "--json"], 240),
    ]
    results = []
    for name, cmd, default_timeout in checks:
        results.append(
            run_supervisor.run_and_track(
                cmd,
                cwd=HERE,
                timeout=opts.get("timeout_sec") or default_timeout,
                name=f"readiness_{name}",
                workers=1,
            )
        )
    try:
        import step2_void_registry
        day_voided = step2_void_registry.is_day_voided(day)
    except Exception:
        day_voided = False
    if day_voided:
        results.append({
            "ok": True,
            "command_name": "readiness_frozen_identity",
            "day": day,
            "skipped": True,
            "reason": "voided_day_excluded_from_readiness_identity_evidence",
        })
    else:
        results.append(_run_frozen_identity_check(day))
    results.append(_run_process_singleton_check(day))
    if opts.get("require_direct_broker"):
        results.append(_run_direct_broker_check(day))
    payload = {
        "ok": all(bool(row.get("ok")) for row in results),
        "command_name": "readiness",
        "day": day,
        "created_at_ct": datetime.now(CT).isoformat(timespec="seconds"),
        "checks": results,
        "require_direct_broker": bool(opts.get("require_direct_broker")),
        "worker_policy": worker_policy.describe_policy(),
    }
    return _write_payload("readiness", day, payload)


def _run_rollback_drill(day: str, opts: dict) -> dict:
    cmd = [PYTHON, "rollback_drill.py"]
    if opts.get("execute"):
        cmd.append("--execute")
    else:
        cmd.append("--no-write")
    if opts.get("restart"):
        cmd.append("--restart")
    cmd.append("--json")
    return _tracked(
        name="rollback-drill",
        day=day,
        command=cmd,
        timeout_sec=opts.get("timeout_sec") or 300,
        workers=1,
    )


def _commands_payload() -> dict:
    payload = canonical_command_registry.catalog()
    payload.update(
        {
            "ok": True,
            "created_at_ct": datetime.now(CT).isoformat(timespec="seconds"),
            "deduction": (
                "Use ops.py as the front door. Deprecated direct scripts either route here "
                "or require --allow-direct-legacy for forensic work."
            ),
        }
    )
    return payload


def run_command(command: str, day: str | None = None, force: bool = False, **opts) -> dict:
    canonical = canonical_command_registry.resolve(command)
    day = day or _today()
    if canonical == "commands":
        return _write_payload("commands", day, _commands_payload())
    if canonical == "status":
        return _write_payload(canonical, day, _compact_status())
    if canonical == "app-start":
        payload = ensure_app_running()
        payload.update({"command": canonical, "day": day, "created_at_ct": datetime.now(CT).isoformat(timespec="seconds")})
        return _write_payload(canonical, day, payload)
    if canonical == "monitor":
        payload = start_live_monitor(day)
        payload.update({"command": canonical, "day": day, "created_at_ct": datetime.now(CT).isoformat(timespec="seconds")})
        return _write_payload(canonical, day, payload)
    if canonical == "context":
        return _write_payload(canonical, day, _write_codex_context(day))
    if canonical in {"pre-open", "post-open", "intraday", "pre-flat", "post-close"}:
        return run_phase(canonical, day, force=force)
    if canonical == "hunt":
        return _run_hunt(day, opts)
    if canonical == "promote":
        return _run_promote(day, opts)
    if canonical == "parity":
        return _run_parity(day, opts)
    if canonical == "scorecard":
        return _run_scorecard(day, opts)
    if canonical == "step3-audit":
        return _run_step3_audit(day, opts)
    if canonical == "readiness":
        return _run_readiness(day, opts)
    if canonical == "rollback-drill":
        return _run_rollback_drill(day, opts)
    raise ValueError(f"unknown ops command: {command}")


def _add_day(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("day", nargs="?", default=None)


def _add_json(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--json", action="store_true")


def _add_timeout_workers(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--timeout-sec", type=float, default=0.0)
    parser.add_argument("--workers", type=int, default=worker_policy.DEFAULT_MAX_WORKERS)


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Canonical operator entrypoint for the trading bot.")
    sub = ap.add_subparsers(dest="command", required=True)

    for name in ("status", "app-start", "monitor", "context", "pre-open", "post-open", "intraday", "pre-flat", "post-close"):
        spec = canonical_command_registry.COMMANDS[name]
        parser = sub.add_parser(name, aliases=list(spec.aliases), help=spec.purpose)
        _add_day(parser)
        _add_json(parser)
        if name in {"pre-open", "post-open", "intraday", "pre-flat", "post-close"}:
            parser.add_argument("--force", action="store_true")

    hunt = sub.add_parser("hunt", aliases=list(canonical_command_registry.COMMANDS["hunt"].aliases), help=canonical_command_registry.COMMANDS["hunt"].purpose)
    _add_day(hunt)
    _add_json(hunt)
    _add_timeout_workers(hunt)
    hunt.add_argument("--beat-pct", type=float, default=5.0)
    hunt.add_argument("--target-count", type=int, default=1)
    hunt.add_argument("--batch-size", type=int, default=500)
    hunt.add_argument("--max-batches", type=int, default=200)
    hunt.add_argument("--start-balance", type=float, default=100000.0)
    hunt.add_argument("--seed", type=int, default=20260507)
    hunt.add_argument("--seed-json", action="append", default=[])
    hunt.add_argument("--compiled-decision-tape", default="")
    hunt.add_argument("--out-dir", default="")
    hunt.add_argument("--name", default="")
    hunt.add_argument("--allow-data-integrity-fail", action="store_true")

    promote = sub.add_parser("promote", aliases=list(canonical_command_registry.COMMANDS["promote"].aliases), help=canonical_command_registry.COMMANDS["promote"].purpose)
    _add_day(promote)
    _add_json(promote)
    _add_timeout_workers(promote)
    promote.add_argument("--candidate-json", default="")
    promote.add_argument("--variant", default="")
    promote.add_argument("--rank", type=int, default=1)
    promote.add_argument("--reason", default="")
    promote.add_argument("--record-current", action="store_true")
    promote.add_argument("--dry-run", action="store_true")
    promote.add_argument("--restart", dest="restart", action="store_true", default=True)
    promote.add_argument("--no-restart", dest="restart", action="store_false")
    promote.add_argument("--evidence-days", nargs="*", default=[])
    promote.add_argument("--allow-execution-change", action="store_true")
    promote.add_argument("--allow-unapproved-candidate", action="store_true")
    promote.add_argument("--allow-preflight-failure", action="store_true")

    parity = sub.add_parser("parity", aliases=list(canonical_command_registry.COMMANDS["parity"].aliases), help=canonical_command_registry.COMMANDS["parity"].purpose)
    _add_day(parity)
    _add_json(parity)
    _add_timeout_workers(parity)
    parity.add_argument("--out-dir", default="")

    scorecard = sub.add_parser("scorecard", help=canonical_command_registry.COMMANDS["scorecard"].purpose)
    _add_day(scorecard)
    _add_json(scorecard)
    _add_timeout_workers(scorecard)
    scorecard.add_argument("--refresh-report", action="store_true")

    audit = sub.add_parser("step3-audit", aliases=list(canonical_command_registry.COMMANDS["step3-audit"].aliases), help=canonical_command_registry.COMMANDS["step3-audit"].purpose)
    _add_json(audit)
    _add_timeout_workers(audit)
    audit.add_argument("--start", default="")
    audit.add_argument("--end", default="")
    audit.add_argument("--tickers", nargs="*", default=["CLSK", "MARA", "RIOT"])
    audit.add_argument("--scoring-profile", default="")
    audit.add_argument("--out-dir", default="")
    audit.add_argument("--start-balance", type=float, default=100000.0)
    audit.add_argument("--actual-sizing", action="store_true")
    audit.add_argument("--require-complete-days", action="store_true")
    audit.add_argument("day", nargs="?", default=None)

    readiness = sub.add_parser("readiness", aliases=list(canonical_command_registry.COMMANDS["readiness"].aliases), help=canonical_command_registry.COMMANDS["readiness"].purpose)
    _add_day(readiness)
    _add_json(readiness)
    readiness.add_argument("--timeout-sec", type=float, default=0.0)
    readiness.add_argument("--require-direct-broker", action="store_true")

    rollback = sub.add_parser("rollback-drill", aliases=list(canonical_command_registry.COMMANDS["rollback-drill"].aliases), help=canonical_command_registry.COMMANDS["rollback-drill"].purpose)
    _add_day(rollback)
    _add_json(rollback)
    rollback.add_argument("--timeout-sec", type=float, default=0.0)
    rollback.add_argument("--execute", action="store_true")
    rollback.add_argument("--restart", action="store_true")

    commands = sub.add_parser("commands", aliases=list(canonical_command_registry.COMMANDS["commands"].aliases), help=canonical_command_registry.COMMANDS["commands"].purpose)
    _add_json(commands)
    commands.add_argument("day", nargs="?", default=None)
    return ap


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    opts = vars(args).copy()
    command = opts.pop("command")
    day = opts.pop("day", None)
    json_output = bool(opts.pop("json", False))
    force = bool(opts.pop("force", False))

    payload = run_command(command, day, force=force, **opts)
    if json_output:
        print(json.dumps(payload, indent=2, default=str))
    else:
        canonical = canonical_command_registry.resolve(command)
        print(f"ops command={canonical} day={day or _today()} ok={payload.get('ok')}")
        if payload.get("artifact"):
            print(payload["artifact"])
        if payload.get("log_path"):
            print(payload["log_path"])
    return 0 if payload.get("ok") is not False else 1


if __name__ == "__main__":
    sys.exit(main())
