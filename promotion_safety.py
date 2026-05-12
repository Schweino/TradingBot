"""Shared promotion safety helpers.

Promotion scripts should call this module before changing the live engine. Live
startup uses the lightweight active-profile evidence validator here so boot
does not need to run expensive replay checks.
"""
from __future__ import annotations

import json
import os
from typing import Any

import promotion_gate
import step2_execution_contract
import step2_parity_contract


HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(HERE, "trading_config.json")


class PromotionBlocked(RuntimeError):
    """Raised when a candidate fails the promotion safety gate."""


def assert_promotable(candidate: dict[str, Any] | None = None,
                      days: list[str] | None = None) -> dict[str, Any]:
    payload = promotion_gate.evaluate(candidate or {}, days=days)
    if not payload.get("ok"):
        failed = [row for row in payload.get("checks", []) if not row.get("ok")]
        names = ", ".join(str(row.get("name")) for row in failed) or "unknown"
        raise PromotionBlocked(f"promotion safety gate failed: {names}")
    return payload


def promotable(candidate: dict[str, Any] | None = None,
               days: list[str] | None = None) -> bool:
    return bool(promotion_gate.evaluate(candidate or {}, days=days).get("ok"))


def _read_json(path: str) -> dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _abs(path: str) -> str:
    if not path:
        return ""
    return path if os.path.isabs(path) else os.path.abspath(os.path.join(HERE, path))


def active_profile_hash(config: dict[str, Any]) -> str:
    return str(step2_parity_contract.active_profile_snapshot(config, include_weights=False).get("hash") or "")


def candidate_profile_hash(candidate: dict[str, Any]) -> str:
    weights = candidate.get("weights") or {}
    semantic = {
        "enabled": True,
        "name": candidate.get("name") or candidate.get("variant"),
        "bias": candidate.get("bias"),
        "weights": {
            str(key): round(float(value or 0.0), 6)
            for key, value in sorted(weights.items())
            if abs(float(value or 0.0)) > 1e-12
        },
    }
    if candidate.get("routes"):
        semantic["routes"] = list(candidate.get("routes") or [])
        semantic["routed_scoring_profile"] = True
    return step2_parity_contract.contract_hash(semantic)


def active_profile_evidence_validation(config: dict[str, Any] | None = None,
                                       require_artifacts: bool = True) -> dict[str, Any]:
    cfg = config if config is not None else (_read_json(CONFIG_PATH) or {})
    smart = cfg.get("smart_entry") or {}
    router = smart.get("active_scoring_router") if isinstance(smart.get("active_scoring_router"), dict) else {}
    profile = router if router.get("enabled") else ((smart.get("active_scoring_profile")) or {})
    active_hash = active_profile_hash(cfg)
    profile_hash = str(profile.get("profile_hash") or "")
    evidence_path = _abs(str(profile.get("promotion_evidence_packet_path") or ""))
    registry_path = _abs(str(profile.get("promotion_registry_path") or ""))
    rollback_path = _abs(str(profile.get("rollback_snapshot_path") or ""))
    evidence = _read_json(evidence_path) if evidence_path else {}
    registry = _read_json(registry_path) if registry_path else {}
    rollback = _read_json(rollback_path) if rollback_path else {}
    candidate = evidence.get("candidate") if isinstance(evidence.get("candidate"), dict) else {}
    contracts = evidence.get("contracts") if isinstance(evidence.get("contracts"), dict) else {}
    live_parity_hash = step2_parity_contract.contract_hash(step2_parity_contract.contract(cfg))
    live_exec_hash = step2_execution_contract.execution_contract_hash(cfg)
    evidence_parity_hash = contracts.get("step2_parity_contract_hash")
    evidence_exec_hash = contracts.get("step2_execution_contract_hash")
    checks = [
        {
            "name": "active_profile_metadata_hash_present",
            "ok": bool(profile_hash),
            "actual": profile_hash,
        },
        {
            "name": "active_profile_metadata_hash_matches_live_profile",
            "ok": bool(profile_hash and profile_hash == active_hash),
            "actual": profile_hash,
            "expected": active_hash,
        },
        {
            "name": "promotion_evidence_packet_path_present",
            "ok": bool(evidence_path),
            "actual": evidence_path,
        },
        {
            "name": "promotion_evidence_packet_exists",
            "ok": bool(evidence_path and os.path.exists(evidence_path)),
            "actual": evidence_path,
        },
        {
            "name": "promotion_evidence_packet_ok",
            "ok": bool(evidence.get("ok")),
            "actual": evidence.get("ok"),
        },
        {
            "name": "promotion_evidence_packet_hash_matches_metadata",
            "ok": bool(
                not profile.get("promotion_evidence_packet_hash")
                or profile.get("promotion_evidence_packet_hash") == evidence.get("packet_hash")
            ),
            "actual": profile.get("promotion_evidence_packet_hash"),
            "expected": evidence.get("packet_hash"),
        },
        {
            "name": "promotion_evidence_profile_hash_matches_live",
            "ok": bool((candidate.get("profile_hash") or "") == active_hash),
            "actual": candidate.get("profile_hash"),
            "expected": active_hash,
        },
        {
            "name": "promotion_registry_path_present",
            "ok": bool(registry_path),
            "actual": registry_path,
        },
        {
            "name": "promotion_registry_exists",
            "ok": bool(registry_path and os.path.exists(registry_path)),
            "actual": registry_path,
        },
        {
            "name": "promotion_registry_new_profile_hash_matches_live",
            "ok": bool(((registry.get("new_profile") or {}).get("profile_hash")) == active_hash),
            "actual": (registry.get("new_profile") or {}).get("profile_hash"),
            "expected": active_hash,
        },
        {
            "name": "rollback_snapshot_path_present",
            "ok": bool(rollback_path),
            "actual": rollback_path,
        },
        {
            "name": "rollback_snapshot_exists",
            "ok": bool(rollback_path and os.path.exists(rollback_path)),
            "actual": rollback_path,
        },
        {
            "name": "rollback_snapshot_prior_profile_present",
            "ok": bool((rollback.get("prior_profile") or {}).get("weights")),
            "actual": (rollback.get("prior_profile") or {}).get("name"),
        },
        {
            "name": "evidence_step2_parity_contract_hash_matches_live",
            "ok": bool(evidence_parity_hash and evidence_parity_hash == live_parity_hash),
            "actual": evidence_parity_hash,
            "expected": live_parity_hash,
        },
        {
            "name": "evidence_step2_execution_contract_hash_matches_live",
            "ok": bool(evidence_exec_hash and evidence_exec_hash == live_exec_hash),
            "actual": evidence_exec_hash,
            "expected": live_exec_hash,
        },
    ]
    if not require_artifacts:
        for row in checks:
            if row["name"].endswith("_exists"):
                row["ok"] = True
                row["skipped"] = True
    failed = [row for row in checks if not row.get("ok")]
    return {
        "schema_version": 1,
        "ok": not failed,
        "checks": checks,
        "failed_count": len(failed),
        "active_profile_hash": active_hash,
        "promotion_evidence_packet_path": evidence_path,
        "promotion_registry_path": registry_path,
        "rollback_snapshot_path": rollback_path,
        "evidence_packet_hash": evidence.get("packet_hash"),
        "deduction": (
            "Live boot only trusts an active profile when its exact hash is tied to an "
            "evidence packet, promotion registry entry, rollback snapshot, and current "
            "Step 2 execution/parity contract hashes."
        ),
    }
