"""Immutable promotion manifest for live scoring profiles.

The manifest is the one-stop audit record for a promotion. Evidence packets,
rollback snapshots, quarantine records, canary reports, and the exact live
config/profile identity are intentionally linked here so a future review does
not need to reconstruct the chain from scattered files.
"""
from __future__ import annotations

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
import step2_parity_contract
import tournament_safety


HERE = Path(__file__).resolve().parent
OUT_DIR = HERE / "postmortem" / "promotions" / "manifests"
LATEST_PATH = OUT_DIR / "PROMOTION_MANIFEST_LATEST.json"
CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1
VALID_LIVE_MANIFEST_STATUSES = {"promoted", "recorded_current", "backfilled_current"}
BACKFILL_STATUS = "backfilled_current"
LEGACY_OVERRIDE_ENV = "CLAUDE_ALLOW_LEGACY_PROMOTION_MANIFEST"


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


def _stamp() -> str:
    return datetime.now(CT).strftime("%Y%m%d_%H%M%S")


def _slug(text: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.+-]+", "_", text or "profile").strip("_")
    return (cleaned or "profile")[:96]


def _stable_hash(payload: Any) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def _read_json(path: str | os.PathLike[str]) -> dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _file_meta(path: str | os.PathLike[str] | None) -> dict[str, Any]:
    if not path:
        return {"path": "", "exists": False}
    p = Path(path)
    exists = p.exists()
    return {
        "path": str(p.resolve()),
        "exists": exists,
        "bytes": p.stat().st_size if exists and p.is_file() else 0,
        "mtime": p.stat().st_mtime if exists else None,
        "sha256": tournament_safety._file_sha256(str(p)) if exists and p.is_file() else None,
    }


def stored_manifest_hash(payload: dict[str, Any]) -> str:
    """Hash a stored manifest using the same immutable fields as build()."""
    return _stable_hash({
        k: v
        for k, v in (payload or {}).items()
        if k not in {"manifest_hash", "output"}
    })


def _active_profile_raw(config: dict[str, Any]) -> dict[str, Any]:
    profile = ((config.get("smart_entry") or {}).get("active_scoring_profile") or {})
    return profile if isinstance(profile, dict) else {}


def _latest_pointer() -> dict[str, Any]:
    return _read_json(LATEST_PATH)


def _profile_manifest_path(config: dict[str, Any]) -> str:
    profile = _active_profile_raw(config)
    return str(profile.get("promotion_manifest_path") or "")


def _check(name: str, ok: bool, actual: Any = None, expected: Any = None,
           severity: str = "critical") -> dict[str, Any]:
    row = {"name": name, "ok": bool(ok), "severity": severity}
    if actual is not None:
        row["actual"] = actual
    if expected is not None:
        row["expected"] = expected
    return row


def _write_json_exclusive(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
        f.write("\n")
    return str(path.resolve())


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.{int(time.time() * 1000)}.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
        f.write("\n")
    os.replace(tmp, path)
    return str(path.resolve())


def _write_live_config_atomic(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.{int(time.time() * 1000)}.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4, default=str)
        f.write("\n")
    os.replace(tmp, path)
    return str(path.resolve())


def _profile_from_config(config: dict[str, Any]) -> dict[str, Any]:
    profile = step2_parity_contract.active_profile_snapshot(config, include_weights=True)
    live_raw = ((config.get("smart_entry") or {}).get("active_scoring_profile") or {})
    return {
        "canonical": profile,
        "metadata": {
            key: value
            for key, value in live_raw.items()
            if key not in {"weights"}
        },
    }


def _promotion_registry_path(entry: dict[str, Any]) -> str:
    return str(entry.get("registry_path") or "")


def _evidence_path(entry: dict[str, Any]) -> str:
    post = entry.get("promotion_evidence_packet") if isinstance(entry.get("promotion_evidence_packet"), dict) else {}
    pre = entry.get("pre_promotion_evidence_packet") if isinstance(entry.get("pre_promotion_evidence_packet"), dict) else {}
    return str(post.get("json_path") or pre.get("json_path") or "")


def _rollback_path(entry: dict[str, Any]) -> str:
    rollback = entry.get("rollback_snapshot") if isinstance(entry.get("rollback_snapshot"), dict) else {}
    return str(rollback.get("path") or "")


def _canary_path(entry: dict[str, Any]) -> str:
    canary = entry.get("post_promotion_canary") if isinstance(entry.get("post_promotion_canary"), dict) else {}
    return str(canary.get("output_path") or "")


def _quarantine_record(entry: dict[str, Any]) -> dict[str, Any]:
    explicit = entry.get("promotion_quarantine_record")
    if isinstance(explicit, dict) and explicit:
        return explicit
    validation = entry.get("validation") if isinstance(entry.get("validation"), dict) else {}
    quarantine = validation.get("promotion_quarantine") if isinstance(validation.get("promotion_quarantine"), dict) else {}
    record = quarantine.get("record") if isinstance(quarantine.get("record"), dict) else {}
    return record


def _candidate(entry: dict[str, Any]) -> dict[str, Any]:
    standard = entry.get("standard_candidate") if isinstance(entry.get("standard_candidate"), dict) else {}
    new_profile = entry.get("new_profile") if isinstance(entry.get("new_profile"), dict) else {}
    source = standard.get("source") if isinstance(standard.get("source"), dict) else {}
    return {
        "source_artifact": entry.get("candidate_source") or source.get("artifact"),
        "selected_rank": entry.get("selected_rank"),
        "selected_variant": entry.get("selected_variant") or standard.get("variant") or new_profile.get("name"),
        "selected_step2_pnl": entry.get("selected_step2_pnl"),
        "step2": standard.get("step2") or {},
        "baseline": standard.get("baseline") or {},
        "model_id": entry.get("model_id") or standard.get("model_id"),
        "family_id": entry.get("family_id") or standard.get("family_id"),
        "profile_hash": new_profile.get("profile_hash"),
        "weight_count": len(new_profile.get("weights") or standard.get("weights") or {}),
    }


def _active_profile_entry(config: dict[str, Any]) -> dict[str, Any]:
    profile = _active_profile_raw(config)
    if not profile.get("enabled") or not isinstance(profile.get("weights"), dict):
        raise RuntimeError("active_scoring_profile is disabled or missing weights")
    evidence_path = str(profile.get("promotion_evidence_packet_path") or "")
    evidence = _read_json(evidence_path) if evidence_path else {}
    registry_path = str(profile.get("promotion_registry_path") or "")
    registry = _read_json(registry_path) if registry_path else {}
    rollback_path = str(profile.get("rollback_snapshot_path") or "")
    standard = (
        (evidence.get("candidate") if isinstance(evidence.get("candidate"), dict) else {})
        or (registry.get("standard_candidate") if isinstance(registry.get("standard_candidate"), dict) else {})
        or {
            "name": profile.get("name"),
            "variant": profile.get("name"),
            "weights": dict(profile.get("weights") or {}),
            "bias": profile.get("bias"),
            "model_id": profile.get("model_id"),
            "family_id": profile.get("family_id"),
            "source": {"artifact": "current_live_config", "script": "promotion_manifest.backfill_live_profile_manifest"},
        }
    )
    full_profile = dict(profile)
    full_profile["enabled"] = True
    full_profile["weights"] = dict(profile.get("weights") or {})
    full_profile.setdefault("profile_hash", step2_parity_contract.active_profile_snapshot(config, include_weights=False).get("hash"))
    return {
        "schema_version": SCHEMA_VERSION,
        "action": "backfill_current_live_profile_promotion_manifest",
        "candidate_source": standard.get("source", {}).get("artifact") if isinstance(standard.get("source"), dict) else "current_live_config",
        "selected_rank": 1,
        "selected_variant": standard.get("variant") or standard.get("name") or profile.get("name"),
        "selected_step2_pnl": (standard.get("step2") or {}).get("pnl") if isinstance(standard.get("step2"), dict) else None,
        "model_id": profile.get("model_id") or standard.get("model_id"),
        "family_id": profile.get("family_id") or standard.get("family_id"),
        "standard_candidate": standard,
        "new_profile": full_profile,
        "prior_profile": full_profile,
        "validation": registry.get("validation") if isinstance(registry.get("validation"), dict) else {"ok": True, "backfilled": True},
        "candidate_lifecycle": registry.get("candidate_lifecycle") or {"backfilled": True},
        "candidate_lifecycle_gate": registry.get("candidate_lifecycle_gate") or {"ok": True, "backfilled": True},
        "promotion_gate": registry.get("promotion_gate") or {"ok": True, "backfilled": True},
        "promotion_evidence_packet": {
            "ok": bool(evidence.get("ok")) if evidence else bool(evidence_path),
            "json_path": evidence_path,
            "packet_hash": profile.get("promotion_evidence_packet_hash") or evidence.get("packet_hash"),
        },
        "pre_promotion_evidence_packet": {
            "ok": bool(evidence.get("ok")) if evidence else bool(evidence_path),
            "json_path": evidence_path,
            "packet_hash": profile.get("promotion_evidence_packet_hash") or evidence.get("packet_hash"),
        },
        "rollback_snapshot": {
            "path": rollback_path,
            "prior_profile_hash": profile.get("rollback_prior_profile_hash"),
            "new_profile_hash": full_profile.get("profile_hash"),
        },
        "post_promotion_canary": registry.get("post_promotion_canary") or {"ok": True, "backfilled": True},
        "registry_path": registry_path,
        "promotion_log_event": {"backfilled": True},
    }


def path_for(entry: dict[str, Any], status: str = "promoted") -> Path:
    profile = entry.get("new_profile") if isinstance(entry.get("new_profile"), dict) else {}
    name = str(profile.get("name") or entry.get("selected_variant") or "profile")
    profile_hash = str(profile.get("profile_hash") or "")[:12]
    suffix = f"_{profile_hash}" if profile_hash else ""
    return OUT_DIR / f"{_stamp()}_{int(time.time() * 1000)}_{_slug(name)}_{_slug(status)}{suffix}.json"


def build(entry: dict[str, Any],
          config: dict[str, Any],
          *,
          status: str = "promoted",
          manifest_path: str | os.PathLike[str] | None = None) -> dict[str, Any]:
    registry_path = _promotion_registry_path(entry)
    evidence_path = _evidence_path(entry)
    rollback_path = _rollback_path(entry)
    canary_path = _canary_path(entry)
    source_artifact = _candidate(entry).get("source_artifact")
    quarantine = _quarantine_record(entry)
    contracts = candidate_profile_schema.contracts(config)
    live_parity = step2_parity_contract.live_parity_checks(config)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "source": "promotion_manifest",
        "created_at_ct": _now_ct(),
        "status": status,
        "action": entry.get("action"),
        "ok": status in {"promoted", "recorded_current"},
        "candidate": _candidate(entry),
        "approval": {
            "quarantine_registry_path": str((HERE / "postmortem" / "promotion_quarantine" / "promotion_candidate_quarantine.json").resolve()),
            "candidate_id": quarantine.get("candidate_id"),
            "status": quarantine.get("status"),
            "approved_by": (quarantine.get("approval") or {}).get("approved_by") if isinstance(quarantine.get("approval"), dict) else None,
            "approved_at_ct": (quarantine.get("approval") or {}).get("approved_at_ct") if isinstance(quarantine.get("approval"), dict) else None,
            "approved_baseline_profile_hash": (quarantine.get("approval") or {}).get("approved_baseline_profile_hash") if isinstance(quarantine.get("approval"), dict) else None,
            "approval_reason": (quarantine.get("approval") or {}).get("reason") if isinstance(quarantine.get("approval"), dict) else None,
            "record": quarantine,
        },
        "evidence": {
            "validation": entry.get("validation") or {},
            "candidate_lifecycle": entry.get("candidate_lifecycle") or {},
            "candidate_lifecycle_gate": entry.get("candidate_lifecycle_gate") or {},
            "promotion_gate": entry.get("promotion_gate") or {},
            "promotion_evidence_packet": entry.get("promotion_evidence_packet") or entry.get("pre_promotion_evidence_packet") or {},
            "post_promotion_canary": entry.get("post_promotion_canary") or {"skipped": True},
        },
        "rollback": {
            "snapshot": entry.get("rollback_snapshot") or {},
            "restore": entry.get("rollback_restore") or {},
        },
        "live": {
            "config_path": str((HERE / "trading_config.json").resolve()),
            "config_hash": _stable_hash(config),
            "active_profile": _profile_from_config(config),
            "live_parity": {
                "ok": live_parity.get("ok"),
                "status": live_parity.get("status"),
                "step2_parity_contract_hash": live_parity.get("step2_parity_contract_hash"),
                "execution_kernel_hash": live_parity.get("execution_kernel_hash"),
            },
            "contracts": {
                "step2_parity_contract_hash": contracts.get("step2_parity_contract_hash"),
                "step2_execution_contract_hash": contracts.get("step2_execution_contract_hash"),
                "compiled_tape_semantic_hash": contracts.get("compiled_tape_semantic_hash"),
                "semantic_config_section_hashes": contracts.get("semantic_config_section_hashes"),
            },
        },
        "artifacts": {
            "source_candidate": _file_meta(source_artifact),
            "promotion_registry": _file_meta(registry_path),
            "promotion_evidence_packet": _file_meta(evidence_path),
            "rollback_snapshot": _file_meta(rollback_path),
            "post_promotion_canary": _file_meta(canary_path),
            "quarantine_registry": _file_meta(HERE / "postmortem" / "promotion_quarantine" / "promotion_candidate_quarantine.json"),
            "live_config": _file_meta(HERE / "trading_config.json"),
        },
        "promotion_log_event": entry.get("promotion_log_event") or {},
        "restart": entry.get("restart") or entry.get("rollback_restart") or {},
        "deduction": (
            "This immutable manifest is the canonical 'why this profile is live' record. "
            "It links the selected candidate, Step 2 score, approval, reproducibility, "
            "promotion evidence, rollback snapshot, canary, and exact live config identity."
        ),
    }
    if manifest_path:
        manifest["planned_path"] = str(Path(manifest_path).resolve())
    manifest["manifest_hash"] = _stable_hash({k: v for k, v in manifest.items() if k != "manifest_hash"})
    return manifest


def write(entry: dict[str, Any],
          config: dict[str, Any],
          *,
          status: str = "promoted",
          path: str | os.PathLike[str] | None = None) -> dict[str, Any]:
    out_path = Path(path).resolve() if path else path_for(entry, status=status).resolve()
    manifest = build(entry, config, status=status, manifest_path=out_path)
    manifest["output"] = {"json_path": _write_json_exclusive(out_path, manifest)}
    _write_json_atomic(LATEST_PATH, {
        "schema_version": SCHEMA_VERSION,
        "source": "promotion_manifest_latest",
        "updated_at_ct": _now_ct(),
        "status": status,
        "manifest_path": manifest["output"]["json_path"],
        "manifest_hash": manifest.get("manifest_hash"),
        "candidate": manifest.get("candidate"),
        "live_profile_hash": ((manifest.get("live") or {}).get("active_profile") or {}).get("canonical", {}).get("hash"),
    })
    return manifest


def _legacy_override_active() -> bool:
    return os.environ.get(LEGACY_OVERRIDE_ENV, "").strip().lower() in {"1", "true", "yes", "on"}


def backfill_live_profile_manifest(*, write_config: bool = True, force: bool = False) -> dict[str, Any]:
    config_path = HERE / "trading_config.json"
    before_config = _read_json(config_path)
    if not before_config:
        raise RuntimeError("could not read trading_config.json")
    current_verdict = validate_live_profile(before_config)
    if current_verdict.get("ok") and not current_verdict.get("legacy") and not force:
        return {
            "ok": True,
            "skipped": True,
            "reason": "active profile already has a valid promotion manifest",
            "verdict": current_verdict,
            "manifest_path": current_verdict.get("manifest_path"),
        }
    entry = _active_profile_entry(before_config)
    out_path = path_for(entry, status=BACKFILL_STATUS).resolve()
    after_config = json.loads(json.dumps(before_config))
    profile = ((after_config.get("smart_entry") or {}).get("active_scoring_profile") or {})
    profile["promotion_manifest_path"] = str(out_path)
    profile["promotion_manifest_status"] = BACKFILL_STATUS
    entry_profile = dict(entry.get("new_profile") or {})
    entry_profile["promotion_manifest_path"] = str(out_path)
    entry_profile["promotion_manifest_status"] = BACKFILL_STATUS
    entry["new_profile"] = entry_profile
    manifest = build(entry, after_config, status=BACKFILL_STATUS, manifest_path=out_path)
    _write_json_exclusive(out_path, manifest)
    profile["promotion_manifest_hash"] = manifest.get("manifest_hash")
    entry["new_profile"]["promotion_manifest_hash"] = manifest.get("manifest_hash")
    _write_json_atomic(LATEST_PATH, {
        "schema_version": SCHEMA_VERSION,
        "source": "promotion_manifest_latest",
        "updated_at_ct": _now_ct(),
        "status": BACKFILL_STATUS,
        "manifest_path": str(out_path),
        "manifest_hash": manifest.get("manifest_hash"),
        "candidate": manifest.get("candidate"),
        "live_profile_hash": ((manifest.get("live") or {}).get("active_profile") or {}).get("canonical", {}).get("hash"),
    })
    output = {
        "ok": True,
        "source": "promotion_manifest_backfill",
        "created_at_ct": _now_ct(),
        "manifest": {
            "json_path": str(out_path),
            "manifest_hash": manifest.get("manifest_hash"),
            "status": BACKFILL_STATUS,
        },
        "write_config": bool(write_config),
    }
    if write_config:
        _write_live_config_atomic(config_path, after_config)
        journal = config_change_journal.record_change(
            before_config,
            after_config,
            action="backfill_current_live_profile_promotion_manifest",
            reason="Backfilled and enforced promotion manifest metadata for the current live scoring profile.",
            actor="promotion_manifest.py",
            artifacts={
                "promotion_manifest_path": str(out_path),
                "promotion_evidence_packet_path": profile.get("promotion_evidence_packet_path"),
                "promotion_registry_path": profile.get("promotion_registry_path"),
                "rollback_snapshot_path": profile.get("rollback_snapshot_path"),
            },
            rollback_snapshot_path=str(profile.get("rollback_snapshot_path") or ""),
            write=True,
        )
        output["config_path"] = str(config_path.resolve())
        output["config_change_journal"] = {
            "ok": journal.get("ok"),
            "event_hash": journal.get("event_hash"),
            "after_config_hash": journal.get("after_config_hash"),
            "path": journal.get("path"),
        }
        output["post_write_verdict"] = validate_live_profile(after_config)
        output["ok"] = bool(output["post_write_verdict"].get("ok"))
    return output


def validate_live_profile(config: dict[str, Any], *, require_manifest: bool = True) -> dict[str, Any]:
    """Validate that the active live profile is backed by a matching manifest.

    Normal Live startup requires the active profile itself to point at a
    manifest. The only legacy escape hatch is the explicit
    CLAUDE_ALLOW_LEGACY_PROMOTION_MANIFEST environment override, intended for
    emergency recovery rather than regular operation.
    """
    profile_raw = _active_profile_raw(config)
    profile = step2_parity_contract.active_profile_snapshot(config, include_weights=False)
    profile_hash = str(profile.get("hash") or "")
    profile_path = _profile_manifest_path(config)
    latest = _latest_pointer()
    latest_path = str(latest.get("manifest_path") or "")
    latest_hash = str(latest.get("manifest_hash") or "")
    latest_profile_hash = str(latest.get("live_profile_hash") or "")
    checks: list[dict[str, Any]] = [
        _check("active_profile_hash_present", bool(profile_hash), profile_hash),
    ]
    manifest_payload: dict[str, Any] = {}
    manifest_path = profile_path or latest_path
    legacy = not profile_path and not latest_path
    if legacy:
        override = _legacy_override_active()
        checks.append(_check(
            "promotion_manifest_path_present",
            override,
            "no active-profile manifest path or latest pointer",
            "active profile promotion_manifest_path",
        ))
        return {
            "schema_version": SCHEMA_VERSION,
            "source": "promotion_manifest_integrity",
            "created_at_ct": _now_ct(),
            "ok": bool(override),
            "status": "legacy_override" if override else "missing_manifest",
            "legacy": True,
            "enforced": not override,
            "override_active": bool(override),
            "active_profile": profile,
            "manifest_path": "",
            "latest_pointer": latest,
            "checks": checks,
            "failed_checks": [row for row in checks if not row.get("ok")],
            "critical_failure_count": 0 if override else 1,
            "warning_failure_count": 0,
        }
    checks.append(_check(
        "active_profile_manifest_path_present",
        bool(profile_path),
        profile_path,
        "active profile promotion_manifest_path",
    ))
    checks.append(_check(
        "promotion_manifest_path_present",
        bool(manifest_path),
        manifest_path,
    ))
    if manifest_path:
        manifest_payload = _read_json(manifest_path)
        checks.append(_check("promotion_manifest_file_exists", bool(manifest_payload), manifest_path, "readable manifest"))
    if profile_path and latest_path:
        checks.append(_check(
            "profile_manifest_matches_latest_pointer",
            os.path.abspath(profile_path) == os.path.abspath(latest_path),
            os.path.abspath(profile_path),
            os.path.abspath(latest_path),
            severity="warning",
        ))
    if manifest_payload:
        computed_hash = stored_manifest_hash(manifest_payload)
        stored_hash = str(manifest_payload.get("manifest_hash") or "")
        manifest_live = ((manifest_payload.get("live") or {}).get("active_profile") or {}).get("canonical") or {}
        manifest_candidate = manifest_payload.get("candidate") or {}
        approval = manifest_payload.get("approval") or {}
        evidence = manifest_payload.get("evidence") or {}
        rollback = manifest_payload.get("rollback") or {}
        artifacts = manifest_payload.get("artifacts") or {}
        status = str(manifest_payload.get("status") or "")
        profile_manifest_hash = str(profile_raw.get("promotion_manifest_hash") or "")
        profile_manifest_status = str(profile_raw.get("promotion_manifest_status") or "")
        evidence_packet = evidence.get("promotion_evidence_packet") if isinstance(evidence.get("promotion_evidence_packet"), dict) else {}
        requires_original_promotion_gates = status == "promoted"
        checks.extend([
            _check("promotion_manifest_hash_present", bool(stored_hash), stored_hash),
            _check("promotion_manifest_hash_valid", stored_hash == computed_hash, stored_hash, computed_hash),
            _check("promotion_manifest_status_promoted_or_recorded",
                   status in VALID_LIVE_MANIFEST_STATUSES,
                   status,
                   "|".join(sorted(VALID_LIVE_MANIFEST_STATUSES))),
            _check("active_profile_manifest_status_matches_manifest",
                   bool(profile_manifest_status and profile_manifest_status == status),
                   profile_manifest_status,
                   status),
            _check("active_profile_manifest_hash_present",
                   bool(profile_manifest_hash),
                   profile_manifest_hash),
            _check("active_profile_manifest_hash_matches_manifest",
                   bool(profile_manifest_hash and profile_manifest_hash == stored_hash),
                   profile_manifest_hash,
                   stored_hash),
            _check("manifest_live_profile_hash_matches_active",
                   str(manifest_live.get("hash") or "") == profile_hash,
                   manifest_live.get("hash"),
                   profile_hash),
            _check("manifest_candidate_profile_hash_matches_active",
                   not manifest_candidate.get("profile_hash") or str(manifest_candidate.get("profile_hash")) == profile_hash,
                   manifest_candidate.get("profile_hash"),
                   profile_hash),
            _check("active_profile_manifest_path_matches_manifest",
                   not profile_path or os.path.abspath(profile_path) == os.path.abspath(manifest_path),
                   os.path.abspath(profile_path) if profile_path else "",
                   os.path.abspath(manifest_path)),
            _check("latest_pointer_hash_matches_manifest",
                   not latest_hash or latest_hash == stored_hash,
                   latest_hash,
                   stored_hash,
                   severity="warning"),
            _check("latest_pointer_profile_hash_matches_active",
                   not latest_profile_hash or latest_profile_hash == profile_hash,
                   latest_profile_hash,
                   profile_hash,
                   severity="warning"),
            _check("promotion_evidence_validation_ok",
                   bool((evidence.get("validation") or {}).get("ok")) or bool(evidence_packet.get("ok")),
                   {
                       "validation_ok": (evidence.get("validation") or {}).get("ok"),
                       "evidence_packet_ok": evidence_packet.get("ok"),
                   },
                   True),
            _check("promotion_gate_ok",
                   bool((evidence.get("promotion_gate") or {}).get("ok")) or not requires_original_promotion_gates,
                   (evidence.get("promotion_gate") or {}).get("ok"),
                   True),
            _check("promotion_lifecycle_gate_ok",
                   bool((evidence.get("candidate_lifecycle_gate") or {}).get("ok")) or not requires_original_promotion_gates,
                   (evidence.get("candidate_lifecycle_gate") or {}).get("ok"),
                   True),
            _check("promotion_quarantine_approval_present",
                   bool(approval.get("approved_by")) or not requires_original_promotion_gates,
                   approval.get("approved_by"),
                   "approval actor"),
            _check("promotion_quarantine_status_final",
                   str(approval.get("status") or "") in {"approved_for_live", "promoted"} or not requires_original_promotion_gates,
                   approval.get("status"),
                   "approved_for_live|promoted"),
            _check("post_promotion_canary_ok",
                   bool((evidence.get("post_promotion_canary") or {}).get("ok")) or not requires_original_promotion_gates,
                   (evidence.get("post_promotion_canary") or {}).get("ok"),
                   True),
            _check("rollback_snapshot_exists",
                   bool(((artifacts.get("rollback_snapshot") or {}).get("exists"))),
                   (artifacts.get("rollback_snapshot") or {}).get("path"),
                   "existing rollback snapshot"),
            _check("promotion_registry_exists",
                   bool(((artifacts.get("promotion_registry") or {}).get("exists"))),
                   (artifacts.get("promotion_registry") or {}).get("path"),
                   "existing promotion registry"),
            _check("promotion_evidence_packet_exists",
                   bool(((artifacts.get("promotion_evidence_packet") or {}).get("exists"))),
                   (artifacts.get("promotion_evidence_packet") or {}).get("path"),
                   "existing evidence packet"),
        ])
    elif latest_path and not profile_path:
        checks.append(_check(
            "latest_manifest_profile_hash_matches_active_without_profile_path",
            latest_profile_hash == profile_hash,
            latest_profile_hash,
            profile_hash,
        ))
    critical_failures = [row for row in checks if row.get("severity") == "critical" and not row.get("ok")]
    warning_failures = [row for row in checks if row.get("severity") != "critical" and not row.get("ok")]
    ok = not critical_failures
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "promotion_manifest_integrity",
        "created_at_ct": _now_ct(),
        "ok": ok,
        "status": "ok" if ok else "failed",
        "legacy": False,
        "enforced": True,
        "active_profile": profile,
        "active_profile_metadata": {
            "promotion_manifest_path": profile_raw.get("promotion_manifest_path"),
            "promotion_manifest_status": profile_raw.get("promotion_manifest_status"),
            "promotion_manifest_hash": profile_raw.get("promotion_manifest_hash"),
        },
        "manifest_path": manifest_path,
        "manifest_hash": manifest_payload.get("manifest_hash") if manifest_payload else latest_hash,
        "latest_pointer": latest,
        "checks": checks,
        "failed_checks": [row for row in checks if not row.get("ok")],
        "critical_failure_count": len(critical_failures),
        "warning_failure_count": len(warning_failures),
    }


def provenance_status(config: dict[str, Any]) -> dict[str, Any]:
    verdict = validate_live_profile(config)
    manifest = _read_json(verdict.get("manifest_path") or "")
    candidate = manifest.get("candidate") or {}
    approval = manifest.get("approval") or {}
    evidence = manifest.get("evidence") or {}
    rollback = manifest.get("rollback") or {}
    artifacts = manifest.get("artifacts") or {}
    profile = verdict.get("active_profile") or {}
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "promotion_manifest_provenance",
        "created_at_ct": _now_ct(),
        "ok": verdict.get("ok"),
        "status": verdict.get("status"),
        "legacy": verdict.get("legacy"),
        "profile_name": profile.get("name"),
        "profile_hash": profile.get("hash"),
        "promoted_at": profile.get("promoted_at"),
        "manifest_path": verdict.get("manifest_path"),
        "manifest_hash": verdict.get("manifest_hash"),
        "selected_variant": candidate.get("selected_variant"),
        "selected_step2_pnl": candidate.get("selected_step2_pnl"),
        "model_id": candidate.get("model_id"),
        "approved_by": approval.get("approved_by"),
        "approval_status": approval.get("status"),
        "evidence_ok": (evidence.get("validation") or {}).get("ok"),
        "promotion_gate_ok": (evidence.get("promotion_gate") or {}).get("ok"),
        "canary_ok": (evidence.get("post_promotion_canary") or {}).get("ok"),
        "rollback_snapshot_path": (rollback.get("snapshot") or {}).get("path"),
        "rollback_snapshot_exists": (artifacts.get("rollback_snapshot") or {}).get("exists"),
        "critical_failure_count": verdict.get("critical_failure_count"),
        "warning_failure_count": verdict.get("warning_failure_count"),
        "failed_checks": [
            {
                "name": row.get("name"),
                "severity": row.get("severity"),
                "actual": row.get("actual"),
                "expected": row.get("expected"),
            }
            for row in (verdict.get("failed_checks") or [])[:10]
        ],
    }


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser(description="Inspect the latest promotion manifest.")
    ap.add_argument("--latest", action="store_true")
    ap.add_argument("--path", default="")
    ap.add_argument("--validate-live", action="store_true")
    ap.add_argument("--provenance", action="store_true")
    ap.add_argument("--backfill-live", action="store_true")
    ap.add_argument("--no-write-config", action="store_true")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()
    if args.backfill_live:
        payload = backfill_live_profile_manifest(write_config=not args.no_write_config, force=args.force)
    elif args.validate_live or args.provenance:
        cfg = _read_json(HERE / "trading_config.json")
        payload = provenance_status(cfg) if args.provenance else validate_live_profile(cfg)
    elif args.path:
        payload = _read_json(args.path)
    else:
        payload = _read_json(LATEST_PATH)
        if args.latest and payload.get("manifest_path"):
            payload = _read_json(payload["manifest_path"])
        elif args.latest and not payload:
            payload = {
                "schema_version": SCHEMA_VERSION,
                "source": "promotion_manifest",
                "exists": False,
                "latest_path": str(LATEST_PATH.resolve()),
                "message": "No promotion manifest has been written yet.",
            }
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0 if payload else 2


if __name__ == "__main__":
    raise SystemExit(main())
