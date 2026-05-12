"""One-command audited rollback drill for the live trading config.

By default this builds the rollback plan without mutating Live. Use
``--execute`` to restore the latest journal rollback snapshot, journal that
config change, and optionally restart/verify Live.
"""
from __future__ import annotations

import argparse
import copy
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
import promotion_manifest
import promotion_safety
import step2_parity_contract


HERE = Path(__file__).resolve().parent
CONFIG_PATH = HERE / "trading_config.json"
OUT_DIR = HERE / "postmortem" / "rollback_drills"
LATEST_PATH = OUT_DIR / "ROLLBACK_DRILL_LATEST.json"
LKG_PATH = HERE / "postmortem" / "promotions" / "LAST_KNOWN_GOOD_PROFILE.json"
CT = ZoneInfo("America/Chicago")
APP_URL = "http://127.0.0.1:5000/mock/status"
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


def _write_config(config: dict[str, Any]) -> None:
    tmp = CONFIG_PATH.with_suffix(CONFIG_PATH.suffix + f".{os.getpid()}.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(config, f, indent=4, default=str)
        f.write("\n")
    os.replace(tmp, CONFIG_PATH)


def _status(timeout: float = 5.0) -> dict[str, Any]:
    try:
        with urlopen(APP_URL, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception as exc:
        return {"_read_error": str(exc)}


def _restart_live(timeout: float = 30.0) -> dict[str, Any]:
    proc = subprocess.run(
        [sys.executable, "live_engine_ops.py", "--timeout", str(timeout), "restart"],
        cwd=str(HERE),
        capture_output=True,
        text=True,
        timeout=max(timeout + 15.0, 30.0),
    )
    return {
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "stdout": proc.stdout[-4000:],
        "stderr": proc.stderr[-4000:],
    }


def _profile_summary(config: dict[str, Any]) -> dict[str, Any]:
    profile = step2_parity_contract.active_profile_snapshot(config, include_weights=False)
    return {
        "enabled": profile.get("enabled"),
        "name": profile.get("name"),
        "hash": profile.get("hash"),
        "weight_count": profile.get("weight_count"),
    }


def _select_snapshot(snapshot_path: str = "") -> tuple[str, dict[str, Any]]:
    pointer = config_change_journal.latest_pointer()
    entry = config_change_journal.latest_entry(pointer)
    lkg = _read_json(LKG_PATH, {}) or {}
    if snapshot_path:
        path = snapshot_path
        source = "explicit_snapshot_path"
    elif lkg.get("rollback_snapshot_path"):
        path = str(lkg.get("rollback_snapshot_path") or "")
        source = "last_known_good_profile"
    elif entry.get("before_config_snapshot_path"):
        path = str(entry.get("before_config_snapshot_path") or "")
        source = "latest_journal_before_config_snapshot"
    else:
        path = str(entry.get("rollback_snapshot_path") or pointer.get("rollback_snapshot_path") or "")
        source = "latest_journal_rollback_snapshot"
    return path, {
        "selection_source": source,
        "latest_pointer": pointer,
        "latest_entry": entry,
        "last_known_good": lkg,
    }


def _looks_like_config(payload: dict[str, Any]) -> bool:
    return bool(
        isinstance(payload, dict)
        and isinstance((payload.get("smart_entry") or {}).get("active_scoring_profile"), dict)
        and ((payload.get("smart_entry") or {}).get("active_scoring_profile") or {}).get("weights")
    )


def _target_config_from_snapshot(current: dict[str, Any],
                                 snapshot: dict[str, Any]) -> tuple[dict[str, Any], str]:
    if _looks_like_config(snapshot):
        return snapshot, "full_config_snapshot"
    prior_profile = snapshot.get("prior_profile") if isinstance(snapshot.get("prior_profile"), dict) else {}
    if prior_profile.get("weights"):
        target = copy.deepcopy(current)
        smart = target.setdefault("smart_entry", {})
        profile = dict(prior_profile)
        profile["enabled"] = True
        smart["active_scoring_profile"] = profile
        return target, "promotion_rollback_profile_snapshot"
    return {}, "unknown_snapshot_shape"


def build(snapshot_path: str = "", *, execute: bool = False, restart: bool = False,
          reason: str = "Rollback drill restore from latest audited config snapshot.",
          actor: str = "codex", restart_timeout: float = 30.0,
          write: bool = True) -> dict[str, Any]:
    current = _read_json(CONFIG_PATH, {}) or {}
    selected_snapshot_path, journal_context = _select_snapshot(snapshot_path)
    raw_snapshot = _read_json(selected_snapshot_path, {}) or {}
    target, snapshot_kind = _target_config_from_snapshot(current, raw_snapshot if isinstance(raw_snapshot, dict) else {})
    checks = []

    def check(name: str, ok: bool, actual: Any = None, expected: Any = None,
              severity: str = "critical") -> None:
        row = {"name": name, "ok": bool(ok), "severity": severity}
        if actual is not None:
            row["actual"] = actual
        if expected is not None:
            row["expected"] = expected
        checks.append(row)

    check("snapshot_path_present", bool(selected_snapshot_path), selected_snapshot_path)
    check("snapshot_exists", bool(selected_snapshot_path and os.path.exists(selected_snapshot_path)), selected_snapshot_path)
    check("snapshot_resolves_to_target_config", bool(target), snapshot_kind, "full_config_or_promotion_profile_snapshot")
    check("snapshot_has_active_profile", bool(((target.get("smart_entry") or {}).get("active_scoring_profile") or {}).get("weights")))
    journal_validation = config_change_journal.validate_live_config(current)
    check("current_config_journal_valid", bool(journal_validation.get("ok")),
          journal_validation.get("status"), "ok")

    current_hash = config_change_journal.config_hash(current)
    target_hash = config_change_journal.config_hash(target) if target else ""
    would_change = bool(target and target_hash != current_hash)
    check("rollback_changes_config", would_change, target_hash, current_hash, severity="warning")
    target_live_parity = step2_parity_contract.live_parity_checks(target) if target else {"ok": False}
    target_manifest = promotion_manifest.validate_live_profile(target) if target else {"ok": False}
    target_evidence = promotion_safety.active_profile_evidence_validation(target) if target else {"ok": False}
    check("target_live_parity_contract_ok", bool(target_live_parity.get("ok")), target_live_parity.get("status"), "ok")
    check("target_promotion_manifest_valid", bool(target_manifest.get("ok")), target_manifest.get("status"), "ok")
    check("target_promotion_manifest_not_legacy", not bool(target_manifest.get("legacy")), target_manifest.get("legacy"), False)
    check("target_promotion_evidence_valid", bool(target_evidence.get("ok")), target_evidence.get("failed_count"), 0)
    lkg = journal_context.get("last_known_good") if isinstance(journal_context.get("last_known_good"), dict) else {}
    target_journal_validation = {}
    if lkg.get("config_change_journal_latest_pointer") and lkg.get("config_change_journal_latest_entry"):
        target_journal_validation = config_change_journal.validate_live_config(
            target,
            pointer=lkg.get("config_change_journal_latest_pointer"),
            entry=lkg.get("config_change_journal_latest_entry"),
        )
        check("target_config_journal_matches_last_known_good",
              bool(target_journal_validation.get("ok")),
              target_journal_validation.get("status"),
              "ok")
    else:
        check("target_config_journal_matches_last_known_good",
              False,
              "no last-known-good journal context",
              "journal context",
              severity="warning")

    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "source": "rollback_drill",
        "created_at_ct": _now_ct(),
        "execute": bool(execute),
        "restart_requested": bool(restart),
        "reason": reason,
        "actor": actor,
        "config_path": str(CONFIG_PATH.resolve()),
        "snapshot_path": os.path.abspath(selected_snapshot_path) if selected_snapshot_path else "",
        "snapshot_kind": snapshot_kind,
        "current_config_hash": current_hash,
        "target_config_hash": target_hash,
        "current_profile": _profile_summary(current),
        "target_profile": _profile_summary(target) if target else {},
        "journal_context": journal_context,
        "current_config_journal_validation": journal_validation,
        "target_live_parity": target_live_parity,
        "target_promotion_manifest": {
            "ok": target_manifest.get("ok"),
            "status": target_manifest.get("status"),
            "legacy": target_manifest.get("legacy"),
            "manifest_path": target_manifest.get("manifest_path"),
            "manifest_hash": target_manifest.get("manifest_hash"),
            "critical_failure_count": target_manifest.get("critical_failure_count"),
        },
        "target_promotion_evidence": {
            "ok": target_evidence.get("ok"),
            "failed_count": target_evidence.get("failed_count"),
            "promotion_evidence_packet_path": target_evidence.get("promotion_evidence_packet_path"),
            "promotion_registry_path": target_evidence.get("promotion_registry_path"),
            "rollback_snapshot_path": target_evidence.get("rollback_snapshot_path"),
        },
        "target_config_journal_validation": target_journal_validation,
        "checks": checks,
        "critical_failure_count": sum(1 for row in checks if row.get("severity") == "critical" and not row.get("ok")),
        "warning_failure_count": sum(1 for row in checks if row.get("severity") == "warning" and not row.get("ok")),
        "steps": [
            "load latest rollback snapshot",
            "resolve full config or promotion-profile rollback snapshot",
            "validate target profile can boot with manifest/evidence/parity gates",
            "write trading_config.json from snapshot",
            "record config_change_journal rollback_drill_restore",
            "restart Live if requested",
            "verify /mock/status profile/config provenance",
        ],
    }
    payload["ok"] = payload["critical_failure_count"] == 0

    if execute and payload["ok"]:
        before = current
        _write_config(target)
        journal_record = config_change_journal.record_change(
            before,
            target,
            action="rollback_drill_restore",
            reason=reason,
            actor=actor,
            artifacts={"rollback_snapshot_path": os.path.abspath(selected_snapshot_path)},
            rollback_snapshot_path=str(journal_context.get("latest_entry", {}).get("after_config_snapshot_path") or ""),
            write=True,
        )
        payload["config_change_journal"] = {
            "event_hash": journal_record.get("event_hash"),
            "journal_path": (journal_record.get("output") or {}).get("journal_path"),
            "latest_path": (journal_record.get("output") or {}).get("latest_path"),
        }
        if restart:
            payload["restart"] = _restart_live(restart_timeout)
        status = _status()
        payload["live_status_verification"] = {
            "read_error": status.get("_read_error"),
            "running": status.get("running"),
            "active_scoring_profile": status.get("active_scoring_profile"),
            "config_provenance": status.get("config_provenance"),
        }
        payload["ok"] = bool(payload["ok"] and not status.get("_read_error") and (not restart or (payload.get("restart") or {}).get("ok")))

    if write:
        path = OUT_DIR / f"rollback_drill_{_stamp()}.json"
        payload["path"] = _write_json_atomic(path, payload)
        _write_json_atomic(LATEST_PATH, {
            "schema_version": SCHEMA_VERSION,
            "source": "rollback_drill_latest",
            "updated_at_ct": _now_ct(),
            "ok": payload.get("ok"),
            "path": payload["path"],
            "snapshot_path": payload.get("snapshot_path"),
            "snapshot_kind": payload.get("snapshot_kind"),
            "target_profile": payload.get("target_profile"),
            "critical_failure_count": payload.get("critical_failure_count"),
        })
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description="Build or execute an audited Live config rollback drill.")
    ap.add_argument("--snapshot-path", default="")
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--restart", action="store_true")
    ap.add_argument("--restart-timeout", type=float, default=30.0)
    ap.add_argument("--reason", default="Rollback drill restore from latest audited config snapshot.")
    ap.add_argument("--actor", default="codex")
    ap.add_argument("--no-write", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    payload = build(
        args.snapshot_path,
        execute=args.execute,
        restart=args.restart,
        reason=args.reason,
        actor=args.actor,
        restart_timeout=args.restart_timeout,
        write=not args.no_write,
    )
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0 if payload.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
