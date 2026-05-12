"""Promotion safety gate for candidate variants.

The gate keeps promotion decisions tied to the same execution kernel used by
Live/Mock and Step 2. It is deliberately conservative: missing artifacts or a
kernel-hash mismatch fail closed unless the caller explicitly chooses report
only behavior outside this module.
"""
from __future__ import annotations

from output_paths import output_path

import argparse
import json
import os
from datetime import datetime
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import execution_kernel
import candidate_lifecycle
import candidate_profile_schema
import canonical_decision_packet
import contract_gate
import golden_parity_suite
import routed_profile_safety
import step2_execution_contract
import step2_parity_contract
import step2_quote_aware_guard
import step2_deployment_risk
import step2_void_registry
import step2_statistical_validation
import step2_world_class_audit


HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = output_path("postmortem", "promotion_gates")
CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


def _read_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except Exception:
        return default


def _write_json(path: str, payload: dict[str, Any]) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
    os.replace(tmp, path)
    return os.path.abspath(path)


def _candidate_kernel_hash(candidate: dict[str, Any]) -> str | None:
    for key in ("execution_kernel_hash", "kernel_hash"):
        if candidate.get(key):
            return str(candidate.get(key))
    contracts = candidate.get("contracts") if isinstance(candidate.get("contracts"), dict) else {}
    for key in ("execution_kernel_hash",):
        if contracts.get(key):
            return str(contracts.get(key))
    execution_contract = contracts.get("step2_execution_contract") if isinstance(contracts.get("step2_execution_contract"), dict) else {}
    nested_kernel = (
        execution_contract.get("execution_kernel_hash")
        or ((execution_contract.get("execution_kernel_contract") or {}).get("execution_kernel_hash")
            if isinstance(execution_contract.get("execution_kernel_contract"), dict) else None)
    )
    if nested_kernel:
        return str(nested_kernel)
    score = candidate.get("score") if isinstance(candidate.get("score"), dict) else {}
    if score.get("execution_kernel_hash"):
        return str(score.get("execution_kernel_hash"))
    result = candidate.get("result") if isinstance(candidate.get("result"), dict) else {}
    if result.get("execution_kernel_hash"):
        return str(result.get("execution_kernel_hash"))
    return None


def _candidate_summary(candidate: dict[str, Any]) -> dict[str, Any]:
    score = candidate.get("score") if isinstance(candidate.get("score"), dict) else {}
    result = score.get("result") if isinstance(score.get("result"), dict) else candidate.get("result")
    result = result if isinstance(result, dict) else {}
    step2 = candidate.get("step2") if isinstance(candidate.get("step2"), dict) else {}
    return {
        "variant": candidate.get("variant") or candidate.get("name") or score.get("variant"),
        "pnl": step2.get("pnl", result.get("pnl") if result else candidate.get("pnl")),
        "trades": step2.get("trades", result.get("trades") if result else candidate.get("trades")),
        "wins": step2.get("wins", result.get("wins") if result else candidate.get("wins")),
        "losses": step2.get("losses", result.get("losses") if result else candidate.get("losses")),
        "execution_kernel_hash": _candidate_kernel_hash(candidate),
    }


def _config() -> dict[str, Any]:
    return _read_json(os.path.join(HERE, "trading_config.json"), {}) or {}


def _candidate_profile_hash(candidate: dict[str, Any]) -> str:
    weights = candidate.get("weights") or {}
    semantic = {
        "enabled": True,
        "name": candidate.get("name") or candidate.get("variant"),
        "bias": round(float(candidate.get("bias") or 0.0), 6),
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


def _path_exists(path: str) -> bool:
    return bool(path and os.path.exists(path))


def _failed_check_reasons(checks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    reasons: list[dict[str, Any]] = []
    for check in checks:
        if check.get("ok") or check.get("warning_only"):
            continue
        name = str(check.get("name") or "unknown_check")
        item: dict[str, Any] = {
            "reason": name,
            "message": name.replace("_", " "),
        }
        for key in ("actual", "expected", "detail", "skipped"):
            if key in check:
                item[key] = check.get(key)
        reasons.append(item)
    return reasons


def _evidence_checks(candidate: dict[str, Any],
                     evidence_packet: dict[str, Any] | None,
                     days: list[str],
                     require_evidence_packet: bool,
                     require_evidence_artifacts: bool,
                     cfg: dict[str, Any]) -> list[dict[str, Any]]:
    evidence_packet = evidence_packet or {}
    evidence_candidate = evidence_packet.get("candidate") if isinstance(evidence_packet.get("candidate"), dict) else {}
    contracts = evidence_packet.get("contracts") if isinstance(evidence_packet.get("contracts"), dict) else {}
    output = evidence_packet.get("output") if isinstance(evidence_packet.get("output"), dict) else {}
    live_parity_hash = step2_parity_contract.contract_hash(step2_parity_contract.contract(cfg))
    live_exec_hash = step2_execution_contract.execution_contract_hash(cfg)
    expected_profile_hash = _candidate_profile_hash(candidate) if candidate else ""
    evidence_profile_hash = evidence_candidate.get("profile_hash")
    evidence_days = set(str(day) for day in (evidence_packet.get("days") or []))
    requested_days = set(str(day) for day in (days or []))
    parity_verdicts = evidence_packet.get("parity_verdicts") if isinstance(evidence_packet.get("parity_verdicts"), dict) else {}
    market_data_integrity = evidence_packet.get("market_data_integrity") if isinstance(evidence_packet.get("market_data_integrity"), dict) else {}
    step2_scores = evidence_packet.get("step2_scores") if isinstance(evidence_packet.get("step2_scores"), dict) else {}
    artifact_registries = evidence_packet.get("artifact_registries") if isinstance(evidence_packet.get("artifact_registries"), dict) else {}
    step2_envelope = evidence_packet.get("step2_evaluation_envelope") if isinstance(evidence_packet.get("step2_evaluation_envelope"), dict) else {}
    compiled_envelope = step2_envelope.get("compiled_decision_tape") if isinstance(step2_envelope.get("compiled_decision_tape"), dict) else {}
    compiled_lineage = compiled_envelope.get("lineage_validation") if isinstance(compiled_envelope.get("lineage_validation"), dict) else {}
    candidate_reproducibility = evidence_packet.get("candidate_reproducibility") if isinstance(evidence_packet.get("candidate_reproducibility"), dict) else {}
    candidate_quarantine = evidence_packet.get("candidate_quarantine") if isinstance(evidence_packet.get("candidate_quarantine"), dict) else {}
    checks = [
        {
            "name": "promotion_evidence_packet_present",
            "ok": bool(evidence_packet) or not require_evidence_packet,
            "actual": bool(evidence_packet),
            "required": require_evidence_packet,
        },
        {
            "name": "promotion_evidence_packet_ok",
            "ok": bool(evidence_packet.get("ok")) if require_evidence_packet or evidence_packet else True,
            "actual": evidence_packet.get("ok"),
            "skipped": not bool(require_evidence_packet or evidence_packet),
        },
        {
            "name": "promotion_evidence_profile_hash_matches_candidate",
            "ok": bool(evidence_profile_hash and evidence_profile_hash == expected_profile_hash)
            if require_evidence_packet or evidence_packet else True,
            "actual": evidence_profile_hash,
            "expected": expected_profile_hash,
            "skipped": not bool(require_evidence_packet or evidence_packet),
        },
        {
            "name": "promotion_evidence_step2_parity_contract_hash_matches_live",
            "ok": bool(contracts.get("step2_parity_contract_hash") == live_parity_hash)
            if require_evidence_packet or evidence_packet else True,
            "actual": contracts.get("step2_parity_contract_hash"),
            "expected": live_parity_hash,
            "skipped": not bool(require_evidence_packet or evidence_packet),
        },
        {
            "name": "promotion_evidence_step2_execution_contract_hash_matches_live",
            "ok": bool(contracts.get("step2_execution_contract_hash") == live_exec_hash)
            if require_evidence_packet or evidence_packet else True,
            "actual": contracts.get("step2_execution_contract_hash"),
            "expected": live_exec_hash,
            "skipped": not bool(require_evidence_packet or evidence_packet),
        },
        {
            "name": "promotion_evidence_packet_json_exists",
            "ok": _path_exists(str(output.get("json_path") or "")) if require_evidence_artifacts else True,
            "actual": output.get("json_path"),
            "skipped": not require_evidence_artifacts,
        },
        {
            "name": "promotion_evidence_requested_days_covered",
            "ok": requested_days.issubset(evidence_days) if requested_days and (require_evidence_packet or evidence_packet) else True,
            "actual": sorted(evidence_days),
            "expected": sorted(requested_days),
            "skipped": not bool(requested_days),
        },
        {
            "name": "promotion_evidence_step2_scores_present",
            "ok": all(bool((step2_scores.get(day) or {}).get("exists")
                           and (step2_scores.get(day) or {}).get("pnl") is not None)
                      for day in requested_days) if requested_days else True,
            "actual": {day: step2_scores.get(day) for day in sorted(requested_days)},
            "skipped": not bool(requested_days),
        },
        {
            "name": "promotion_evidence_parity_verdicts_safe",
            "ok": all(bool((parity_verdicts.get(day) or {}).get("exists")
                           and (parity_verdicts.get(day) or {}).get("promotion_safe"))
                      for day in requested_days) if requested_days else True,
            "actual": {day: parity_verdicts.get(day) for day in sorted(requested_days)},
            "skipped": not bool(requested_days),
        },
        {
            "name": "promotion_evidence_market_data_integrity_safe",
            "ok": all(bool((market_data_integrity.get(day) or {}).get("exists")
                           and (market_data_integrity.get(day) or {}).get("promotion_safe"))
                      for day in requested_days) if requested_days else True,
            "actual": {day: market_data_integrity.get(day) for day in sorted(requested_days)},
            "skipped": not bool(requested_days),
        },
        {
            "name": "promotion_evidence_artifact_registries_present",
            "ok": all(bool((artifact_registries.get(day) or {}).get("exists")
                           and (artifact_registries.get(day) or {}).get("registry_hash"))
                      for day in requested_days) if requested_days else True,
            "actual": {day: artifact_registries.get(day) for day in sorted(requested_days)},
            "skipped": not bool(requested_days),
        },
        {
            "name": "promotion_evidence_candidate_reproducibility_ok",
            "ok": bool(candidate_reproducibility.get("ok")) if step2_envelope else True,
            "actual": {
                "ok": candidate_reproducibility.get("ok"),
                "output_path": candidate_reproducibility.get("output_path"),
                "failed_checks": candidate_reproducibility.get("failed_checks", [])[:10],
                "error": candidate_reproducibility.get("error"),
            },
            "skipped": not bool(step2_envelope),
        },
        {
            "name": "promotion_evidence_compiled_tape_lineage_safe",
            "ok": (
                bool(compiled_lineage)
                and compiled_lineage.get("status") == "CERTIFIED_MATCH"
                and compiled_lineage.get("certified") is True
                and compiled_lineage.get("status") != "UNSAFE_SCORE_DRIFT"
                and not bool(compiled_lineage.get("rebuild_required"))
                and compiled_lineage.get("quick_score_allowed") is not False
            ) if step2_envelope else True,
            "actual": {
                "status": compiled_lineage.get("status"),
                "certified": compiled_lineage.get("certified"),
                "rebuild_required": compiled_lineage.get("rebuild_required"),
                "quick_score_allowed": compiled_lineage.get("quick_score_allowed"),
                "recommended_action": compiled_lineage.get("recommended_action"),
            },
            "skipped": not bool(step2_envelope),
        },
        {
            "name": "promotion_evidence_candidate_quarantine_approved",
            "ok": bool(candidate_quarantine.get("ok")) if step2_envelope else True,
            "actual": {
                "ok": candidate_quarantine.get("ok"),
                "candidate_id": candidate_quarantine.get("candidate_id"),
                "failed_checks": candidate_quarantine.get("failed_checks", [])[:10],
                "error": candidate_quarantine.get("error"),
            },
            "skipped": not bool(step2_envelope),
        },
    ]
    return checks


def evaluate(candidate: dict[str, Any] | None = None, days: list[str] | None = None,
             require_candidate_kernel: bool = True,
             lifecycle_candidate_id: str = "",
             evidence_packet: dict[str, Any] | None = None,
             require_lifecycle: bool = True,
             require_evidence_packet: bool = False,
             require_evidence_artifacts: bool = False) -> dict[str, Any]:
    candidate = candidate or {}
    evidence_packet = evidence_packet or {}
    evidence_candidate = evidence_packet.get("candidate") if isinstance(evidence_packet.get("candidate"), dict) else {}
    if (
        isinstance(candidate.get("routes"), list)
        and candidate.get("routes")
        and not candidate.get("route_audit")
        and isinstance(evidence_candidate.get("route_audit"), dict)
    ):
        candidate = {**candidate, "route_audit": evidence_candidate["route_audit"]}
    cfg = _config()
    expected_kernel = execution_kernel.contract_from_config(cfg).get("execution_kernel_hash")
    candidate_hash = _candidate_kernel_hash(candidate)
    voided_days = sorted(
        set(day for day in (days or []) if step2_void_registry.is_day_voided(day))
        | set(step2_void_registry.contamination_report(candidate).get("voided_source_days") or [])
        | set(step2_void_registry.contamination_report(evidence_packet).get("voided_source_days") or [])
    )
    void_blocked = bool(voided_days)
    route_safety = routed_profile_safety.evaluate_candidate(candidate)
    quote_aware_gate = step2_quote_aware_guard.candidate_gate(candidate)
    parity = golden_parity_suite.run(days=days)
    packet_checks = []
    for day in days or []:
        try:
            compat = canonical_decision_packet.schema_compatibility(day)
        except Exception as exc:
            compat = {"ok": False, "error": repr(exc), "day": day}
        packet_checks.append(compat)
    lifecycle_eval = {"ok": True, "checks": [], "skipped": True}
    if require_lifecycle and candidate and not void_blocked:
        if lifecycle_candidate_id:
            record = (candidate_lifecycle.load_registry().get("candidates") or {}).get(lifecycle_candidate_id)
        else:
            record = candidate_lifecycle.register_candidate(candidate, status="step2_validated", write=True)
        lifecycle_eval = candidate_lifecycle.evaluate_record(record, evidence_packet=evidence_packet)
    checks = [
        {
            "name": "voided_source_day_block",
            "ok": not void_blocked,
            "actual": voided_days,
            "expected": [],
        },
        {
            "name": "golden_parity_suite",
            "ok": bool(parity.get("ok")),
            "detail": [r.get("day") for r in parity.get("results") or []],
        },
        {
            "name": "candidate_lifecycle_gate",
            "ok": bool(lifecycle_eval.get("ok")),
            "detail": lifecycle_eval.get("candidate_id") or lifecycle_candidate_id,
            "skipped": lifecycle_eval.get("skipped"),
        },
        {
            "name": "canonical_decision_packet_schema",
            "ok": all(bool(row.get("ok")) for row in packet_checks) if packet_checks else True,
            "detail": {row.get("day"): row.get("counts") for row in packet_checks},
            "skipped": not bool(packet_checks),
        },
    ]
    if require_candidate_kernel:
        checks.append({
            "name": "candidate_execution_kernel_hash_present",
            "ok": bool(candidate_hash),
            "actual": candidate_hash,
        })
        checks.append({
            "name": "candidate_execution_kernel_hash_matches_current",
            "ok": bool(candidate_hash and candidate_hash == expected_kernel),
            "actual": candidate_hash,
            "expected": expected_kernel,
        })
    checks.extend(_evidence_checks(
        candidate,
        evidence_packet,
        days or [],
        require_evidence_packet=require_evidence_packet,
        require_evidence_artifacts=require_evidence_artifacts,
        cfg=cfg,
    ))
    checks.append({
        "name": "routed_profile_safety_gate",
        "ok": bool(route_safety.get("ok")),
        "actual": route_safety.get("status"),
        "skipped": bool(route_safety.get("skipped")),
    })
    checks.append({
        "name": "quote_aware_exit_replay_model",
        "ok": bool(quote_aware_gate.get("ok")),
        "actual": quote_aware_gate.get("actual_exit_replay_model"),
        "expected": quote_aware_gate.get("required_exit_replay_model"),
        "detail": quote_aware_gate.get("blockers") or [],
    })
    statistical_validation = (
        candidate.get("statistical_validation")
        if isinstance(candidate.get("statistical_validation"), dict)
        else evidence_packet.get("statistical_validation")
        if isinstance(evidence_packet.get("statistical_validation"), dict)
        else {}
    )
    checks.append({
        "name": "statistical_validation_promotion_grade",
        "ok": bool(statistical_validation.get("promotion_grade")) if statistical_validation else True,
        "actual": statistical_validation.get("score") if statistical_validation else None,
        "expected": step2_statistical_validation.VALIDATION_VERSION,
        "detail": statistical_validation.get("blockers") if statistical_validation else [],
        "skipped": not bool(statistical_validation),
    })
    deployment_risk = (
        candidate.get("deployment_risk")
        if isinstance(candidate.get("deployment_risk"), dict)
        else evidence_packet.get("deployment_risk")
        if isinstance(evidence_packet.get("deployment_risk"), dict)
        else {}
    )
    checks.append({
        "name": "deployment_risk_live_eligible",
        "ok": bool(deployment_risk.get("deployment_allowed")) if deployment_risk else True,
        "actual": deployment_risk.get("tier") if deployment_risk else None,
        "expected": step2_deployment_risk.RISK_VERSION,
        "detail": (deployment_risk.get("blockers") or []) + (deployment_risk.get("warnings") or []) if deployment_risk else [],
        "skipped": not bool(deployment_risk),
    })
    world_class = (
        candidate.get("world_class_readiness")
        if isinstance(candidate.get("world_class_readiness"), dict)
        else candidate.get("world_class_candidate_audit")
        if isinstance(candidate.get("world_class_candidate_audit"), dict)
        else evidence_packet.get("world_class_readiness_report")
        if isinstance(evidence_packet.get("world_class_readiness_report"), dict)
        else {}
    )
    checks.append({
        "name": "world_class_readiness_ok",
        "ok": bool(world_class.get("ok")) if world_class else True,
        "actual": world_class.get("readiness_state") or world_class.get("score") if world_class else None,
        "expected": step2_world_class_audit.AUDIT_VERSION,
        "detail": (world_class.get("blockers") or []) + (world_class.get("warnings") or []) if world_class else [],
        "skipped": not bool(world_class),
    })
    gate = contract_gate.check(cfg, day=datetime.now(CT).date().isoformat())
    checks.append({
        "name": "hard_contract_gate_ok",
        "ok": bool(gate.get("ok")),
        "actual": gate.get("critical_failure_count"),
        "expected": 0,
    })
    ok = all(bool(c.get("ok")) for c in checks)
    reject_reasons = _failed_check_reasons(checks)
    return {
        "schema_version": SCHEMA_VERSION,
        "created_at_ct": _now_ct(),
        "ok": ok,
        "decision": "PASS" if ok else "REJECT",
        "reject_reasons": [item["reason"] for item in reject_reasons],
        "reject_reason_details": reject_reasons,
        "quote_aware_outcome_gate": quote_aware_gate,
        "checks": checks,
        "candidate": _candidate_summary(candidate),
        "candidate_profile_hash": _candidate_profile_hash(candidate) if candidate else None,
        "routed_profile_safety": route_safety,
        "candidate_lifecycle": lifecycle_eval,
        "canonical_decision_packets": packet_checks,
        "expected_execution_kernel_hash": expected_kernel,
        "golden_parity": parity,
        "contract_gate": gate,
        "deduction": (
            "Promotion is blocked if golden parity, lifecycle evidence, immutable "
            "promotion evidence, canonical packets, or execution contract hashes fail."
        ),
    }


def _select_candidate_from_payload(payload: Any, rank: int = 1, variant: str = "") -> dict[str, Any]:
    if isinstance(payload, dict):
        try:
            row = candidate_profile_schema.select(payload, variant=variant, rank=rank)
            return candidate_profile_schema.normalize(
                row,
                context={"rank": rank, "script": "promotion_gate"},
                source_payload=payload,
            )
        except Exception:
            pass
    if isinstance(payload, dict) and isinstance(payload.get("score"), dict):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("candidates"), list) and payload["candidates"]:
        idx = max(0, min(len(payload["candidates"]) - 1, int(rank) - 1))
        return payload["candidates"][idx]
    if isinstance(payload, dict) and isinstance(payload.get("results"), list) and payload["results"]:
        idx = max(0, min(len(payload["results"]) - 1, int(rank) - 1))
        return payload["results"][idx]
    return payload if isinstance(payload, dict) else {}


def evaluate_file(path: str, days: list[str] | None = None, rank: int = 1,
                  variant: str = "", require_lifecycle: bool = True,
                  evidence_packet: dict[str, Any] | None = None,
                  require_evidence_packet: bool = False,
                  require_evidence_artifacts: bool = False) -> dict[str, Any]:
    payload = _read_json(path, {}) or {}
    candidate = _select_candidate_from_payload(payload, rank=rank, variant=variant)
    out = evaluate(
        candidate,
        days=days,
        require_lifecycle=require_lifecycle,
        evidence_packet=evidence_packet,
        require_evidence_packet=require_evidence_packet,
        require_evidence_artifacts=require_evidence_artifacts,
    )
    out["candidate_path"] = os.path.abspath(path)
    out["candidate_rank"] = rank
    if variant:
        out["candidate_variant"] = variant
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Run the promotion safety gate.")
    ap.add_argument("--candidate-json", default="")
    ap.add_argument("--rank", type=int, default=1)
    ap.add_argument("--variant", default="")
    ap.add_argument("--days", nargs="*", default=[])
    ap.add_argument("--out", default="")
    ap.add_argument("--skip-lifecycle", action="store_true",
                    help="Diagnostics only: run promotion checks without candidate lifecycle evidence.")
    ap.add_argument("--evidence-packet", default="")
    ap.add_argument("--require-evidence", action="store_true")
    args = ap.parse_args()
    evidence_packet = _read_json(args.evidence_packet, {}) if args.evidence_packet else None
    payload = evaluate_file(
        args.candidate_json,
        days=args.days,
        rank=args.rank,
        variant=args.variant,
        require_lifecycle=not args.skip_lifecycle,
        evidence_packet=evidence_packet,
        require_evidence_packet=args.require_evidence,
        require_evidence_artifacts=args.require_evidence,
    ) if args.candidate_json else evaluate(
        days=args.days,
        require_lifecycle=not args.skip_lifecycle,
        evidence_packet=evidence_packet,
        require_evidence_packet=args.require_evidence,
        require_evidence_artifacts=args.require_evidence,
    )
    if args.out:
        payload["path"] = _write_json(args.out, payload)
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
