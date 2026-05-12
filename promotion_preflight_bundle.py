"""Promotion preflight bundle.

This is the final pre-promotion receipt: candidate identity, Step 2 evidence,
contract hashes, config hash, data hashes, latency model, rollback snapshot,
and config-journal state before Live is changed.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import candidate_profile_schema
import config_change_journal
import contract_gate
import execution_kernel
import market_data_integrity_gate
import promotion_evidence_packet
import step2_execution_contract
import step2_latency_model
import step2_parity_contract
import tournament_safety


HERE = Path(__file__).resolve().parent
CONFIG_PATH = HERE / "trading_config.json"
POSTMORTEM_DIR = HERE / "postmortem"
OUT_DIR = POSTMORTEM_DIR / "promotion_preflight"
CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


def _stamp() -> str:
    return datetime.now(CT).strftime("%Y%m%d_%H%M%S")


def _slug(text: str) -> str:
    return (re.sub(r"[^A-Za-z0-9_.+-]+", "_", text or "candidate").strip("_") or "candidate")[:96]


def _read_json(path: str | os.PathLike[str], default: Any = None) -> Any:
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except Exception:
        return default


def _write_json(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.{int(time.time() * 1000)}.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
        f.write("\n")
    os.replace(tmp, path)
    return str(path.resolve())


def _write_text(path: Path, text: str) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.{int(time.time() * 1000)}.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)
    return str(path.resolve())


def _stable_hash(payload: Any) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def _file_meta(path: str | os.PathLike[str]) -> dict[str, Any]:
    raw = str(path or "")
    exists = bool(raw and os.path.exists(raw))
    return {
        "path": os.path.abspath(raw) if raw else "",
        "exists": exists,
        "bytes": os.path.getsize(raw) if exists else 0,
        "sha256": tournament_safety._file_sha256(raw) if exists else None,
    }


def _select_candidate(candidate_json: str = "", rank: int = 1, variant: str = "") -> tuple[dict[str, Any], dict[str, Any]]:
    source_payload = _read_json(candidate_json, {}) if candidate_json else {}
    if not candidate_json:
        profile = step2_parity_contract.active_profile_snapshot(_read_json(CONFIG_PATH, {}) or {}, include_weights=True)
        row = {
            "name": profile.get("name") or "current_live_active_profile",
            "variant": profile.get("name") or "current_live_active_profile",
            "bias": profile.get("bias") or 0.0,
            "weights": profile.get("weights") or {},
        }
    else:
        row = candidate_profile_schema.select(source_payload, rank=rank, variant=variant)
    standard = candidate_profile_schema.normalize(
        row,
        context={"artifact": os.path.abspath(candidate_json) if candidate_json else "current_live_config", "rank": rank},
        source_payload=source_payload,
    )
    return standard, source_payload


def _market_data(days: list[str]) -> dict[str, Any]:
    reports = {}
    for day in days:
        try:
            payload = market_data_integrity_gate.build(day, source="canonical", write=True, use_cached=True)
        except Exception as exc:
            payload = {"ok": False, "promotion_safe": False, "error": repr(exc)}
        reports[day] = {
            "ok": payload.get("ok"),
            "promotion_safe": payload.get("promotion_safe"),
            "verdict": payload.get("verdict"),
            "critical_count": payload.get("critical_count"),
            "warning_count": payload.get("warning_count"),
            "source_tape_hash": (payload.get("source_tape") or {}).get("sha256"),
            "path": payload.get("path") or market_data_integrity_gate.output_paths(day)["json"],
        }
    return reports


def build(candidate_json: str = "", rank: int = 1, variant: str = "",
          days: list[str] | None = None, rollback_snapshot_path: str = "",
          promotion_evidence: dict[str, Any] | None = None,
          write: bool = True) -> dict[str, Any]:
    days = days or []
    cfg = _read_json(CONFIG_PATH, {}) or {}
    candidate, source_payload = _select_candidate(candidate_json, rank=rank, variant=variant)
    profile_hash = promotion_evidence_packet.candidate_profile_hash(candidate)
    evidence = promotion_evidence or promotion_evidence_packet.build(
        candidate_json=candidate_json,
        rank=rank,
        variant=variant,
        days=days,
        write=write,
    )
    contract = contract_gate.check(cfg, day=datetime.now(CT).date().isoformat())
    data = _market_data(days)
    if not rollback_snapshot_path:
        latest = config_change_journal.latest_pointer()
        rollback_snapshot_path = str(latest.get("rollback_snapshot_path") or "")
    rollback_meta = _file_meta(rollback_snapshot_path)
    config_validation = config_change_journal.validate_live_config(cfg)
    latency_meta = _file_meta(step2_latency_model.DEFAULT_MODEL_PATH)
    exec_kernel = execution_kernel.contract_from_config(cfg)
    source_is_step2 = bool(candidate_json and "step2_evaluation_envelope" in source_payload)
    source_envelope = source_payload.get("step2_evaluation_envelope") if isinstance(source_payload.get("step2_evaluation_envelope"), dict) else {}
    source_compiled = source_envelope.get("compiled_decision_tape") if isinstance(source_envelope.get("compiled_decision_tape"), dict) else {}
    source_lineage = source_compiled.get("lineage_validation") if isinstance(source_compiled.get("lineage_validation"), dict) else {}

    checks = [
        {"name": "candidate_weights_present", "ok": bool(candidate.get("weights")), "severity": "critical"},
        {"name": "candidate_profile_hash_present", "ok": bool(profile_hash), "severity": "critical", "detail": profile_hash},
        {"name": "contract_gate_ok", "ok": bool(contract.get("ok")), "severity": "critical", "detail": contract.get("critical_failure_count")},
        {"name": "config_change_journal_ok", "ok": bool(config_validation.get("ok")), "severity": "critical", "detail": config_validation.get("status")},
        {"name": "rollback_snapshot_exists", "ok": bool(rollback_meta.get("exists")), "severity": "critical", "detail": rollback_meta.get("path")},
        {"name": "promotion_evidence_packet_ok", "ok": bool(evidence.get("ok")), "severity": "critical", "detail": (evidence.get("output") or {}).get("json_path")},
        {
            "name": "market_data_promotion_safe",
            "ok": all(bool(row.get("promotion_safe")) for row in data.values()) if data else True,
            "severity": "critical",
            "detail": {day: {"verdict": row.get("verdict"), "promotion_safe": row.get("promotion_safe")} for day, row in data.items()},
            "skipped": not bool(data),
        },
        {
            "name": "latency_model_present_for_evidence_days",
            "ok": bool(latency_meta.get("exists") and latency_meta.get("sha256")) if days else True,
            "severity": "warning",
            "detail": latency_meta.get("path"),
            "skipped": not bool(days),
        },
        {
            "name": "step2_source_envelope_present",
            "ok": bool(source_envelope) if source_is_step2 else True,
            "severity": "critical",
            "detail": source_envelope.get("envelope_hash") if source_is_step2 else "not_step2_payload",
            "skipped": not source_is_step2,
        },
        {
            "name": "step2_source_compiled_tape_lineage_safe",
            "ok": (
                bool(source_lineage)
                and source_lineage.get("status") == "CERTIFIED_MATCH"
                and source_lineage.get("certified") is True
                and source_lineage.get("status") != "UNSAFE_SCORE_DRIFT"
                and not bool(source_lineage.get("rebuild_required"))
                and source_lineage.get("quick_score_allowed") is not False
            ) if source_is_step2 else True,
            "severity": "critical",
            "detail": {
                "status": source_lineage.get("status"),
                "certified": source_lineage.get("certified"),
                "rebuild_required": source_lineage.get("rebuild_required"),
                "quick_score_allowed": source_lineage.get("quick_score_allowed"),
                "recommended_action": source_lineage.get("recommended_action"),
            } if source_is_step2 else "not_step2_payload",
            "skipped": not source_is_step2,
        },
    ]
    critical_failures = [row for row in checks if row.get("severity") == "critical" and not row.get("ok")]
    warning_failures = [row for row in checks if row.get("severity") == "warning" and not row.get("ok")]
    payload = {
        "schema_version": SCHEMA_VERSION,
        "source": "promotion_preflight_bundle",
        "created_at_ct": _now_ct(),
        "ok": not critical_failures,
        "critical_failure_count": len(critical_failures),
        "warning_failure_count": len(warning_failures),
        "checks": checks,
        "candidate": {
            **candidate,
            "profile_hash": profile_hash,
            "source_artifact": os.path.abspath(candidate_json) if candidate_json else "current_live_config",
            "rank": rank,
            "variant": variant,
        },
        "live": {
            "config_path": str(CONFIG_PATH.resolve()),
            "config_hash": config_change_journal.config_hash(cfg),
            "config_file_sha256": tournament_safety._file_sha256(CONFIG_PATH),
            "active_profile": step2_parity_contract.active_profile_snapshot(cfg, include_weights=False),
            "execution_kernel_hash": exec_kernel.get("execution_kernel_hash"),
            "step2_parity_contract_hash": step2_parity_contract.contract_hash(step2_parity_contract.contract(cfg)),
            "step2_execution_contract_hash": step2_execution_contract.execution_contract_hash(cfg),
            "same_ticker_reentry_cooldown_sec": exec_kernel.get("same_ticker_reentry_cooldown_sec"),
            "trade_size_pct": exec_kernel.get("trade_size_pct"),
        },
        "rollback_snapshot": rollback_meta,
        "promotion_evidence_packet": {
            "ok": evidence.get("ok"),
            "packet_hash": evidence.get("packet_hash"),
            "output": evidence.get("output"),
        },
        "market_data_integrity": data,
        "latency_model": latency_meta,
        "config_change_journal": config_validation,
        "contract_gate": {
            "ok": contract.get("ok"),
            "critical_failure_count": contract.get("critical_failure_count"),
            "hashes": contract.get("hashes"),
        },
        "deduction": (
            "Promotion may proceed only when this preflight can connect candidate identity, "
            "Step 2 evidence, data integrity, current config hash, rollback snapshot, and Live/Step2 contracts."
        ),
    }
    payload["preflight_hash"] = _stable_hash({k: v for k, v in payload.items() if k not in {"preflight_hash", "output"}})
    if write:
        stem = f"{_stamp()}_{_slug(str(candidate.get('name') or candidate.get('variant') or 'candidate'))}"
        json_path = OUT_DIR / f"{stem}.json"
        txt_path = OUT_DIR / f"{stem}.txt"
        payload["output"] = {
            "json_path": _write_json(json_path, payload),
            "txt_path": _write_text(txt_path, render_text(payload)),
        }
    return payload


def render_text(payload: dict[str, Any]) -> str:
    cand = payload.get("candidate") or {}
    live = payload.get("live") or {}
    lines = [
        f"Promotion Preflight Bundle - {cand.get('name') or cand.get('variant')}",
        f"Created CT: {payload.get('created_at_ct')}",
        "",
        "Verdict",
        f"- OK: {payload.get('ok')}",
        f"- Critical/warnings: {payload.get('critical_failure_count')}/{payload.get('warning_failure_count')}",
        "",
        "Identity",
        f"- Candidate profile hash: {cand.get('profile_hash')}",
        f"- Live config hash: {live.get('config_hash')}",
        f"- Step2 parity/execution: {live.get('step2_parity_contract_hash')}/{live.get('step2_execution_contract_hash')}",
        f"- Rollback snapshot: {(payload.get('rollback_snapshot') or {}).get('path')}",
        "",
        "Checks",
    ]
    for check in payload.get("checks") or []:
        lines.append(f"- {'OK' if check.get('ok') else 'FAIL'} {check.get('name')}: {check.get('detail')}")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="Build promotion preflight bundle.")
    ap.add_argument("--candidate-json", default="")
    ap.add_argument("--rank", type=int, default=1)
    ap.add_argument("--variant", default="")
    ap.add_argument("--days", nargs="*", default=[])
    ap.add_argument("--rollback-snapshot-path", default="")
    ap.add_argument("--no-write", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    payload = build(
        candidate_json=args.candidate_json,
        rank=args.rank,
        variant=args.variant,
        days=args.days,
        rollback_snapshot_path=args.rollback_snapshot_path,
        write=not args.no_write,
    )
    print(json.dumps(payload, indent=2, sort_keys=True, default=str) if args.json else render_text(payload))
    return 0 if payload.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
