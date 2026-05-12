"""Last-known-good profile and rollback recovery helpers.

This module keeps recovery state separate from promotion selection. Promotion
selection decides what should go live; this module records when the running Live
process has proven it actually picked that config up cleanly.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.request import urlopen

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import config_change_journal
import contract_gate
import promotion_manifest
import promotion_safety
import step2_parity_contract


HERE = Path(__file__).resolve().parent
CONFIG_PATH = HERE / "trading_config.json"
OUT_DIR = HERE / "postmortem" / "promotions" / "recovery"
LKG_PATH = HERE / "postmortem" / "promotions" / "LAST_KNOWN_GOOD_PROFILE.json"
LATEST_PROOF_PATH = OUT_DIR / "PROMOTION_SAFETY_PROOF_LATEST.json"
STATUS_URL = "http://127.0.0.1:5000/mock/status"
CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


def _stamp() -> str:
    return datetime.now(CT).strftime("%Y%m%d_%H%M%S")


def _read_json(path: str | os.PathLike[str], default: Any = None) -> Any:
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except Exception:
        return default


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.{int(time.time() * 1000)}.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
        f.write("\n")
    os.replace(tmp, path)
    return str(path.resolve())


def _check(name: str, ok: bool, actual: Any = None, expected: Any = None,
           severity: str = "critical") -> dict[str, Any]:
    row = {"name": name, "ok": bool(ok), "severity": severity}
    if actual is not None:
        row["actual"] = actual
    if expected is not None:
        row["expected"] = expected
    return row


def _load_config(config: dict[str, Any] | None = None) -> dict[str, Any]:
    return config if config is not None else (_read_json(CONFIG_PATH, {}) or {})


def _status(timeout: float = 5.0, *, full: bool = True) -> dict[str, Any]:
    try:
        suffix = "?full=1" if full else ""
        with urlopen(STATUS_URL + suffix, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception as exc:
        return {"_read_error": repr(exc)}


def _smoke(timeout: float = 180.0) -> dict[str, Any]:
    try:
        proc = subprocess.run(
            [sys.executable, "smoke_check.py", "--mode", "no-surprises"],
            cwd=str(HERE),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return {
            "ok": proc.returncode == 0,
            "returncode": proc.returncode,
            "stdout": proc.stdout[-6000:],
            "stderr": proc.stderr[-3000:],
        }
    except Exception as exc:
        return {"ok": False, "error": repr(exc)}


def active_profile_summary(config: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = _load_config(config)
    raw = (((cfg.get("smart_entry") or {}).get("active_scoring_profile")) or {})
    profile = step2_parity_contract.active_profile_snapshot(cfg, include_weights=False)
    return {
        "enabled": profile.get("enabled"),
        "name": profile.get("name"),
        "hash": profile.get("hash"),
        "promoted_at": profile.get("promoted_at"),
        "promotion_reason": profile.get("promotion_reason"),
        "weight_count": profile.get("weight_count"),
        "profile_hash_metadata": raw.get("profile_hash"),
        "promotion_manifest_path": raw.get("promotion_manifest_path"),
        "promotion_manifest_status": raw.get("promotion_manifest_status"),
        "promotion_manifest_hash": raw.get("promotion_manifest_hash"),
        "promotion_evidence_packet_path": raw.get("promotion_evidence_packet_path"),
        "promotion_registry_path": raw.get("promotion_registry_path"),
        "rollback_snapshot_path": raw.get("rollback_snapshot_path"),
    }


def running_profile_verification(config: dict[str, Any] | None = None,
                                 status: dict[str, Any] | None = None,
                                 *,
                                 timeout: float = 5.0) -> dict[str, Any]:
    cfg = _load_config(config)
    expected = active_profile_summary(cfg)
    status = status if status is not None else _status(timeout=timeout, full=True)
    startup = status.get("startup_self_check") if isinstance(status.get("startup_self_check"), dict) else {}
    running_profile = status.get("active_scoring_profile") if isinstance(status.get("active_scoring_profile"), dict) else {}
    provenance = status.get("promotion_provenance") if isinstance(status.get("promotion_provenance"), dict) else {}
    startup_manifest = (
        startup.get("promotion_manifest_integrity")
        if isinstance(startup.get("promotion_manifest_integrity"), dict)
        else {}
    )
    running_hash = str(running_profile.get("hash") or (startup.get("active_scoring_profile") or {}).get("hash") or "")
    running_manifest_hash = str(provenance.get("manifest_hash") or startup_manifest.get("manifest_hash") or "")
    expected_hash = str(expected.get("hash") or "")
    expected_manifest_hash = str(expected.get("promotion_manifest_hash") or "")
    checks = [
        _check("live_status_reachable", not status.get("_read_error"), status.get("_read_error"), "reachable"),
        _check("running_profile_hash_matches_config", bool(running_hash and running_hash == expected_hash), running_hash, expected_hash),
        _check(
            "running_manifest_hash_matches_config",
            bool(expected_manifest_hash and running_manifest_hash == expected_manifest_hash),
            running_manifest_hash,
            expected_manifest_hash,
        ),
        _check("startup_self_check_ok", bool(startup.get("ok")), startup.get("ok"), True),
    ]
    critical_failures = [row for row in checks if row.get("severity") == "critical" and not row.get("ok")]
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "live_running_profile_verification",
        "created_at_ct": _now_ct(),
        "ok": not critical_failures,
        "expected_profile": expected,
        "running_profile": running_profile,
        "running_manifest_hash": running_manifest_hash,
        "startup_self_check_ok": startup.get("ok"),
        "checks": checks,
        "failed_checks": critical_failures,
        "critical_failure_count": len(critical_failures),
    }


def promotion_safety_proof(config: dict[str, Any] | None = None,
                           *,
                           status: dict[str, Any] | None = None,
                           require_running_match: bool = True,
                           run_smoke: bool = True,
                           write: bool = True,
                           label: str = "post_promotion") -> dict[str, Any]:
    cfg = _load_config(config)
    profile = active_profile_summary(cfg)
    contract = contract_gate.check(cfg)
    manifest = promotion_manifest.validate_live_profile(cfg)
    evidence = promotion_safety.active_profile_evidence_validation(cfg)
    journal = config_change_journal.validate_live_config(cfg)
    running = (
        running_profile_verification(cfg, status=status)
        if require_running_match else
        {"ok": True, "skipped": True, "reason": "running match not required"}
    )
    smoke = _smoke() if run_smoke else {"ok": True, "skipped": True}
    checks = [
        _check("contract_gate_ok", bool(contract.get("ok")), contract.get("critical_failure_count"), 0),
        _check("promotion_manifest_ok", bool(manifest.get("ok")), manifest.get("status"), "ok"),
        _check("promotion_manifest_not_legacy", not bool(manifest.get("legacy")), manifest.get("legacy"), False),
        _check("promotion_evidence_boot_gate_ok", bool(evidence.get("ok")), evidence.get("failed_count"), 0),
        _check("config_change_journal_ok", bool(journal.get("ok")), journal.get("status"), "ok"),
        _check("running_profile_verification_ok", bool(running.get("ok")), running.get("critical_failure_count"), 0),
        _check("smoke_no_surprises_ok", bool(smoke.get("ok")), smoke.get("returncode"), 0),
    ]
    critical_failures = [row for row in checks if row.get("severity") == "critical" and not row.get("ok")]
    payload = {
        "schema_version": SCHEMA_VERSION,
        "source": "promotion_safety_proof",
        "label": label,
        "created_at_ct": _now_ct(),
        "ok": not critical_failures,
        "profile": profile,
        "config_hash": config_change_journal.config_hash(cfg),
        "checks": checks,
        "failed_checks": critical_failures,
        "critical_failure_count": len(critical_failures),
        "contract_gate": {
            "ok": contract.get("ok"),
            "critical_failure_count": contract.get("critical_failure_count"),
            "hashes": contract.get("hashes"),
        },
        "promotion_manifest": {
            "ok": manifest.get("ok"),
            "legacy": manifest.get("legacy"),
            "status": manifest.get("status"),
            "manifest_path": manifest.get("manifest_path"),
            "manifest_hash": manifest.get("manifest_hash"),
            "critical_failure_count": manifest.get("critical_failure_count"),
        },
        "promotion_evidence": {
            "ok": evidence.get("ok"),
            "failed_count": evidence.get("failed_count"),
            "promotion_evidence_packet_path": evidence.get("promotion_evidence_packet_path"),
            "promotion_registry_path": evidence.get("promotion_registry_path"),
            "rollback_snapshot_path": evidence.get("rollback_snapshot_path"),
        },
        "config_change_journal": {
            "ok": journal.get("ok"),
            "status": journal.get("status"),
            "current_config_hash": journal.get("current_config_hash"),
            "critical_failure_count": journal.get("critical_failure_count"),
        },
        "running_profile_verification": running,
        "smoke": {
            "ok": smoke.get("ok"),
            "returncode": smoke.get("returncode"),
            "skipped": smoke.get("skipped"),
            "error": smoke.get("error"),
        },
    }
    if write:
        path = OUT_DIR / f"promotion_safety_proof_{_stamp()}_{str(profile.get('hash') or '')[:12]}.json"
        payload["path"] = _write_json_atomic(path, payload)
        _write_json_atomic(LATEST_PROOF_PATH, {
            "schema_version": SCHEMA_VERSION,
            "source": "promotion_safety_proof_latest",
            "updated_at_ct": _now_ct(),
            "ok": payload.get("ok"),
            "profile_hash": profile.get("hash"),
            "profile_name": profile.get("name"),
            "proof_path": payload["path"],
            "critical_failure_count": payload.get("critical_failure_count"),
        })
    return payload


def latest_last_known_good() -> dict[str, Any]:
    payload = _read_json(LKG_PATH, {}) or {}
    return payload if isinstance(payload, dict) else {}


def _rollback_command(snapshot_path: str) -> str:
    if not snapshot_path:
        return ""
    return (
        f'python rollback_drill.py --snapshot-path "{snapshot_path}" '
        f'--execute --restart --reason "Restore last-known-good live profile"'
    )


def record_last_known_good(config: dict[str, Any] | None = None,
                           *,
                           proof: dict[str, Any] | None = None,
                           reason: str = "Promotion safety proof passed.",
                           write: bool = True) -> dict[str, Any]:
    cfg = _load_config(config)
    proof = proof or promotion_safety_proof(cfg, write=write)
    if not proof.get("ok"):
        return {
            "ok": False,
            "updated": False,
            "reason": "promotion safety proof failed",
            "critical_failure_count": proof.get("critical_failure_count"),
            "failed_checks": proof.get("failed_checks") or [],
            "proof_path": proof.get("path"),
        }
    pointer = config_change_journal.latest_pointer()
    entry = config_change_journal.latest_entry(pointer)
    profile = active_profile_summary(cfg)
    config_snapshot_path = str(entry.get("after_config_snapshot_path") or "")
    promotion_rollback_path = str(profile.get("rollback_snapshot_path") or entry.get("rollback_snapshot_path") or "")
    rollback_snapshot_path = config_snapshot_path or promotion_rollback_path
    payload = {
        "schema_version": SCHEMA_VERSION,
        "source": "last_known_good_profile",
        "updated_at_ct": _now_ct(),
        "ok": True,
        "reason": reason,
        "profile": profile,
        "config_hash": config_change_journal.config_hash(cfg),
        "config_snapshot_path": config_snapshot_path,
        "promotion_rollback_snapshot_path": promotion_rollback_path,
        "rollback_snapshot_path": rollback_snapshot_path,
        "rollback_command": _rollback_command(rollback_snapshot_path),
        "proof_path": proof.get("path"),
        "proof_hash_profile": (proof.get("profile") or {}).get("hash"),
        "config_change_journal_latest_pointer": pointer,
        "config_change_journal_latest_entry": entry,
        "deduction": (
            "This pointer updates only after the live profile passes promotion manifest, "
            "evidence, config journal, hard contract, smoke, and running startup checks."
        ),
    }
    if write:
        payload["path"] = _write_json_atomic(LKG_PATH, payload)
    return payload


def recovery_recommendation(config: dict[str, Any] | None = None,
                            *,
                            reason: str = "",
                            status: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = _load_config(config)
    lkg = latest_last_known_good()
    current_profile = active_profile_summary(cfg)
    lkg_profile = lkg.get("profile") if isinstance(lkg.get("profile"), dict) else {}
    snapshot_path = str(lkg.get("rollback_snapshot_path") or lkg.get("config_snapshot_path") or "")
    command = str(lkg.get("rollback_command") or _rollback_command(snapshot_path))
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "live_profile_recovery_recommendation",
        "created_at_ct": _now_ct(),
        "ok": bool(lkg and snapshot_path and command),
        "reason": reason,
        "current_profile": current_profile,
        "last_known_good_profile": lkg_profile,
        "last_known_good_updated_at_ct": lkg.get("updated_at_ct"),
        "rollback_snapshot_path": snapshot_path,
        "rollback_command": command,
        "status_context_present": bool(status),
        "deduction": (
            "If Live refuses to start because hard gates failed, keep trading disabled "
            "and restore this last-known-good snapshot through rollback_drill.py."
        ),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Inspect or update Live profile recovery state.")
    sub = ap.add_subparsers(dest="cmd", required=True)
    proof_ap = sub.add_parser("proof")
    proof_ap.add_argument("--no-smoke", action="store_true")
    proof_ap.add_argument("--no-running-match", action="store_true")
    proof_ap.add_argument("--no-write", action="store_true")
    record_ap = sub.add_parser("record-lkg")
    record_ap.add_argument("--reason", default="Promotion safety proof passed.")
    record_ap.add_argument("--no-write", action="store_true")
    sub.add_parser("recommend")
    sub.add_parser("status")
    args = ap.parse_args()

    if args.cmd == "proof":
        payload = promotion_safety_proof(
            require_running_match=not args.no_running_match,
            run_smoke=not args.no_smoke,
            write=not args.no_write,
        )
        ok = bool(payload.get("ok"))
    elif args.cmd == "record-lkg":
        proof = promotion_safety_proof(write=not args.no_write)
        payload = record_last_known_good(proof=proof, reason=args.reason, write=not args.no_write)
        ok = bool(payload.get("ok"))
    elif args.cmd == "recommend":
        payload = recovery_recommendation(reason="manual_request")
        ok = bool(payload.get("ok"))
    else:
        payload = {
            "last_known_good": latest_last_known_good(),
            "latest_proof": _read_json(LATEST_PROOF_PATH, {}) or {},
        }
        ok = bool(payload.get("last_known_good") or payload.get("latest_proof"))
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
