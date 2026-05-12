"""Promotion quarantine for Step 2 candidates.

This is the promotion-side two-key control:

1. Evidence key: the candidate has a current Step 2 envelope, current baseline,
   and a passing reproducibility report.
2. Approval key: a separate explicit approval marks that exact model id as
   approved_for_live under the same baseline hash.

Promotion code may override this gate only with an explicit CLI flag.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import active_engine_baseline
import baseline_drift_sentinel
import candidate_profile_schema
import candidate_reproducibility_gate
import tournament_safety


HERE = Path(__file__).resolve().parent
OUT_DIR = HERE / "postmortem" / "promotion_quarantine"
REGISTRY_PATH = OUT_DIR / "promotion_candidate_quarantine.json"
CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1

STATUS_ORDER = {
    "found": 10,
    "reproduced": 20,
    "ready_for_review": 30,
    "approved_for_live": 40,
    "promoted": 50,
    "rejected": 60,
    "retired": 70,
    "stale": 80,
}


def _now() -> datetime:
    return datetime.now(CT)


def _now_ct() -> str:
    return _now().isoformat(timespec="seconds")


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


def _stable_hash(payload: Any) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def _empty_registry() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "promotion_candidate_quarantine",
        "created_at_ct": _now_ct(),
        "updated_at_ct": _now_ct(),
        "records": {},
        "history": [],
    }


def load_registry(path: Path | None = None) -> dict[str, Any]:
    path = path or REGISTRY_PATH
    payload = _read_json(path, None)
    if not isinstance(payload, dict):
        return _empty_registry()
    payload.setdefault("schema_version", SCHEMA_VERSION)
    payload.setdefault("source", "promotion_candidate_quarantine")
    payload.setdefault("created_at_ct", _now_ct())
    payload.setdefault("records", {})
    payload.setdefault("history", [])
    return payload


def write_registry(registry: dict[str, Any], path: Path | None = None) -> str:
    path = path or REGISTRY_PATH
    registry["updated_at_ct"] = _now_ct()
    return _write_json(path, registry)


def current_baseline_hash() -> str:
    active = active_engine_baseline.active_profile_payload()
    profile = active.get("profile") if isinstance(active.get("profile"), dict) else {}
    return str(profile.get("hash") or "")


def _status_max(a: str | None, b: str | None) -> str:
    a = a or "found"
    b = b or "found"
    return a if STATUS_ORDER.get(a, 0) >= STATUS_ORDER.get(b, 0) else b


def candidate_id(row: dict[str, Any]) -> str:
    name = str(row.get("variant") or row.get("name") or "candidate")
    weights = row.get("weights") or {}
    bias = float(row.get("bias") or 0.0)
    routes = row.get("routes") if isinstance(row.get("routes"), list) else []
    if routes:
        return str(row.get("model_id") or tournament_safety.stable_json_hash({
            "name": name,
            "weights": weights,
            "bias": round(bias, 8),
            "routes": routes,
        }, length=20))
    return str(row.get("model_id") or tournament_safety.model_id(name, weights, bias))


def _profile_hash(row: dict[str, Any]) -> str:
    payload = {
        "name": row.get("variant") or row.get("name"),
        "bias": row.get("bias"),
        "weights": row.get("weights") or {},
    }
    routes = row.get("routes") if isinstance(row.get("routes"), list) else []
    if routes:
        payload["routes"] = routes
        payload["routed_scoring_profile"] = True
    return _stable_hash(payload)


def _select(candidate_json: str, rank: int = 1, variant: str = "") -> tuple[dict[str, Any], dict[str, Any], Path]:
    path = Path(candidate_json).resolve()
    payload = _read_json(path, {}) or {}
    if not isinstance(payload, dict):
        raise RuntimeError(f"candidate artifact must be a JSON object: {path}")
    row = candidate_profile_schema.select(payload, rank=rank, variant=variant)
    return payload, row, path


def _envelope(payload: dict[str, Any]) -> dict[str, Any]:
    value = payload.get("step2_evaluation_envelope")
    return value if isinstance(value, dict) else {}


def evidence_from_artifact(
    candidate_json: str,
    *,
    rank: int = 1,
    variant: str = "",
    run_reproducibility: bool = True,
    write_reproducibility: bool = True,
) -> dict[str, Any]:
    payload, row, path = _select(candidate_json, rank=rank, variant=variant)
    envelope = _envelope(payload)
    drift = baseline_drift_sentinel.evaluate_payload(payload) if baseline_drift_sentinel.source_looks_step2(payload) else {
        "status": "skipped",
        "ok": True,
        "reason": "not_step2_payload",
    }
    reproducibility = {"ok": False, "skipped": True, "reason": "not_run"}
    if run_reproducibility:
        try:
            reproducibility = candidate_reproducibility_gate.evaluate_payload(
                payload,
                row,
                candidate_json=str(path),
                rank=rank,
                variant=variant,
            )
            if write_reproducibility:
                candidate_reproducibility_gate.write_report(reproducibility, label="quarantine_reproducibility")
        except Exception as exc:
            reproducibility = {"ok": False, "error": repr(exc)}
    cid = candidate_id(row)
    checks = [
        {"name": "step2_source_payload", "ok": baseline_drift_sentinel.source_looks_step2(payload)},
        {"name": "step2_evaluation_envelope_present", "ok": bool(envelope), "actual": bool(envelope)},
        {"name": "step2_artifact_promotable", "ok": payload.get("promotable") is not False, "actual": payload.get("promotable")},
        {"name": "baseline_current", "ok": drift.get("status") == "current", "actual": drift.get("status"), "expected": "current"},
        {"name": "candidate_reproducibility_ok", "ok": bool(reproducibility.get("ok")), "actual": reproducibility.get("ok")},
    ]
    return {
        "candidate_id": cid,
        "model_id": cid,
        "variant": row.get("variant") or row.get("name"),
        "rank": rank,
        "candidate_json": str(path),
        "artifact_hash": tournament_safety._file_sha256(str(path)),
        "profile_hash": _profile_hash(row),
        "baseline_profile_hash": envelope.get("baseline_profile_hash") or payload.get("baseline_profile_hash"),
        "step2_evaluation_envelope_hash": envelope.get("envelope_hash") or payload.get("step2_evaluation_envelope_hash"),
        "baseline_drift": drift,
        "reproducibility": {
            "ok": bool(reproducibility.get("ok")),
            "output_path": reproducibility.get("output_path"),
            "report_hash": reproducibility.get("report_hash"),
            "failed_checks": reproducibility.get("failed_checks", [])[:10],
            "error": reproducibility.get("error"),
        },
        "step2": candidate_profile_schema.normalize(row, {"artifact": str(path), "rank": rank}, payload).get("step2"),
        "checks": checks,
        "evidence_key_ok": all(check.get("ok") for check in checks),
    }


def upsert_record(
    evidence: dict[str, Any],
    *,
    status: str,
    approval: dict[str, Any] | None = None,
    promotion: dict[str, Any] | None = None,
    note: str = "",
    registry: dict[str, Any] | None = None,
    write: bool = True,
) -> dict[str, Any]:
    registry = registry or load_registry()
    records = registry.setdefault("records", {})
    cid = str(evidence.get("candidate_id") or evidence.get("model_id") or "")
    if not cid:
        raise RuntimeError("candidate evidence is missing candidate_id")
    existing = records.get(cid) or {}
    record = {
        **existing,
        "candidate_id": cid,
        "model_id": evidence.get("model_id") or cid,
        "variant": evidence.get("variant"),
        "rank": evidence.get("rank"),
        "candidate_json": evidence.get("candidate_json"),
        "artifact_hash": evidence.get("artifact_hash"),
        "profile_hash": evidence.get("profile_hash"),
        "baseline_profile_hash": evidence.get("baseline_profile_hash"),
        "step2_evaluation_envelope_hash": evidence.get("step2_evaluation_envelope_hash"),
        "evidence_key_ok": bool(evidence.get("evidence_key_ok")),
        "baseline_drift": evidence.get("baseline_drift"),
        "reproducibility": evidence.get("reproducibility"),
        "step2": evidence.get("step2"),
        "checks": evidence.get("checks") or [],
        "status": _status_for_new_evidence(existing, status, evidence),
        "first_seen_ct": existing.get("first_seen_ct") or _now_ct(),
        "last_seen_ct": _now_ct(),
    }
    if approval:
        record["approval"] = approval
        record["status"] = "approved_for_live"
    if promotion:
        record["promotion"] = {**(existing.get("promotion") or {}), **promotion}
        record["status"] = promotion.get("status") or "promoted"
    if record.get("status") != "stale":
        record.pop("stale_at_ct", None)
        record.pop("stale_reason", None)
    if note:
        notes = list(existing.get("notes") or [])
        notes.append({"created_at_ct": _now_ct(), "note": note})
        record["notes"] = notes[-50:]
    records[cid] = record
    registry.setdefault("history", []).append({
        "created_at_ct": _now_ct(),
        "candidate_id": cid,
        "event": f"status:{record['status']}",
        "status": record["status"],
        "payload": {
            "candidate_json": evidence.get("candidate_json"),
            "artifact_hash": evidence.get("artifact_hash"),
            "evidence_key_ok": evidence.get("evidence_key_ok"),
            "approval": bool(approval),
            "promotion": bool(promotion),
        },
    })
    registry["history"] = registry["history"][-2000:]
    if write:
        write_registry(registry)
    return record


def record_artifact(
    candidate_json: str,
    *,
    rank: int = 1,
    variant: str = "",
    write: bool = True,
) -> dict[str, Any]:
    evidence = evidence_from_artifact(candidate_json, rank=rank, variant=variant, write_reproducibility=write)
    status = "reproduced" if evidence.get("evidence_key_ok") else "found"
    return upsert_record(evidence, status=status, write=write)


def approve_artifact(
    candidate_json: str,
    *,
    rank: int = 1,
    variant: str = "",
    approved_by: str,
    reason: str,
    ttl_hours: float = 24.0,
    write: bool = True,
) -> dict[str, Any]:
    if not approved_by:
        raise RuntimeError("approved_by is required")
    if not reason:
        raise RuntimeError("approval reason is required")
    evidence = evidence_from_artifact(candidate_json, rank=rank, variant=variant, write_reproducibility=write)
    if not evidence.get("evidence_key_ok"):
        failed = [row for row in evidence.get("checks") or [] if not row.get("ok")]
        raise RuntimeError(f"candidate evidence key failed: {failed}")
    approved_at = _now()
    approval = {
        "approved_by": approved_by,
        "reason": reason,
        "approved_at_ct": approved_at.isoformat(timespec="seconds"),
        "expires_at_ct": (approved_at + timedelta(hours=float(ttl_hours))).isoformat(timespec="seconds"),
        "approved_baseline_profile_hash": evidence.get("baseline_profile_hash"),
        "evidence_key_hash": _stable_hash({
            "candidate_id": evidence.get("candidate_id"),
            "artifact_hash": evidence.get("artifact_hash"),
            "baseline_profile_hash": evidence.get("baseline_profile_hash"),
            "step2_evaluation_envelope_hash": evidence.get("step2_evaluation_envelope_hash"),
            "reproducibility_report_hash": (evidence.get("reproducibility") or {}).get("report_hash"),
        }),
    }
    return upsert_record(evidence, status="approved_for_live", approval=approval, write=write)


def _approval_expired(approval: dict[str, Any]) -> bool:
    raw = approval.get("expires_at_ct")
    if not raw:
        return False
    try:
        expires = datetime.fromisoformat(str(raw))
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=CT)
        return _now() > expires
    except Exception:
        return True


def _status_for_new_evidence(existing: dict[str, Any], incoming_status: str, evidence: dict[str, Any]) -> str:
    """Let fresh current evidence recover records that were only stale by review state."""
    existing_status = str(existing.get("status") or "")
    drift = evidence.get("baseline_drift") if isinstance(evidence.get("baseline_drift"), dict) else {}
    has_approval = isinstance(existing.get("approval"), dict) and bool(existing.get("approval"))
    if (
        existing_status == "stale"
        and incoming_status in {"reproduced", "ready_for_review"}
        and drift.get("status") == "current"
        and not has_approval
    ):
        return incoming_status
    return _status_max(existing_status, incoming_status)


def evaluate_artifact(
    candidate_json: str,
    *,
    rank: int = 1,
    variant: str = "",
    registry: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload, row, path = _select(candidate_json, rank=rank, variant=variant)
    cid = candidate_id(row)
    registry = registry or load_registry()
    record = (registry.get("records") or {}).get(cid) or {}
    approval = record.get("approval") if isinstance(record.get("approval"), dict) else {}
    baseline_hash = current_baseline_hash()
    checks = [
        {"name": "quarantine_record_present", "ok": bool(record), "actual": bool(record)},
        {"name": "quarantine_status_approved_for_live", "ok": record.get("status") == "approved_for_live", "actual": record.get("status"), "expected": "approved_for_live"},
        {"name": "quarantine_evidence_key_ok", "ok": bool(record.get("evidence_key_ok")), "actual": record.get("evidence_key_ok")},
        {"name": "approval_key_present", "ok": bool(approval), "actual": bool(approval)},
        {"name": "approval_has_actor", "ok": bool(approval.get("approved_by")), "actual": approval.get("approved_by")},
        {"name": "approval_has_reason", "ok": bool(approval.get("reason")), "actual": bool(approval.get("reason"))},
        {"name": "approval_not_expired", "ok": not _approval_expired(approval), "actual": approval.get("expires_at_ct")},
        {"name": "approval_baseline_matches_live", "ok": approval.get("approved_baseline_profile_hash") == baseline_hash, "actual": approval.get("approved_baseline_profile_hash"), "expected": baseline_hash},
        {"name": "artifact_hash_matches_approval_record", "ok": record.get("artifact_hash") == tournament_safety._file_sha256(str(path)), "actual": record.get("artifact_hash"), "expected": tournament_safety._file_sha256(str(path))},
    ]
    ok = all(check.get("ok") for check in checks)
    approval_baseline_stale = bool(approval) and any(
        check["name"] == "approval_baseline_matches_live" and not check.get("ok")
        for check in checks
    )
    if record and not ok and approval_baseline_stale:
        record["status"] = "stale"
        record["stale_at_ct"] = _now_ct()
        record["stale_reason"] = "baseline_hash_changed"
        write_registry(registry)
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "promotion_candidate_quarantine",
        "created_at_ct": _now_ct(),
        "ok": ok,
        "candidate_id": cid,
        "candidate_json": str(path),
        "record": record,
        "checks": checks,
        "failed_checks": [check for check in checks if not check.get("ok")],
        "deduction": (
            "Promotion requires both a valid evidence key and an explicit approval key "
            "for the current live baseline hash."
        ),
    }


def record_promotion(
    candidate_json: str,
    *,
    rank: int = 1,
    variant: str = "",
    status: str = "promoted",
    promotion_registry_path: str = "",
    evidence_packet_path: str = "",
    canary_path: str = "",
    rollback_path: str = "",
    write: bool = True,
) -> dict[str, Any]:
    evidence = evidence_from_artifact(
        candidate_json,
        rank=rank,
        variant=variant,
        run_reproducibility=False,
        write_reproducibility=False,
    )
    registry = load_registry()
    existing = (registry.get("records") or {}).get(str(evidence.get("candidate_id"))) or {}
    for key in (
        "evidence_key_ok",
        "baseline_drift",
        "reproducibility",
        "checks",
        "step2_evaluation_envelope_hash",
        "baseline_profile_hash",
    ):
        if existing.get(key) is not None:
            evidence[key] = existing.get(key)
    promotion = {
        "status": status,
        "promoted_at_ct": _now_ct(),
        "promotion_registry_path": promotion_registry_path,
        "promotion_evidence_packet_path": evidence_packet_path,
        "post_promotion_canary_path": canary_path,
        "rollback_snapshot_path": rollback_path,
    }
    return upsert_record(evidence, status=status, promotion=promotion, registry=registry, write=write)


def _main() -> int:
    parser = argparse.ArgumentParser(description="Promotion quarantine approval registry.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    def add_candidate_args(cmd: argparse.ArgumentParser) -> None:
        cmd.add_argument("--candidate-json", required=True)
        cmd.add_argument("--rank", type=int, default=1)
        cmd.add_argument("--variant", default="")

    record_cmd = sub.add_parser("record")
    add_candidate_args(record_cmd)
    record_cmd.add_argument("--json", action="store_true")

    approve_cmd = sub.add_parser("approve")
    add_candidate_args(approve_cmd)
    approve_cmd.add_argument("--approved-by", required=True)
    approve_cmd.add_argument("--reason", required=True)
    approve_cmd.add_argument("--ttl-hours", type=float, default=24.0)
    approve_cmd.add_argument("--json", action="store_true")

    check_cmd = sub.add_parser("check")
    add_candidate_args(check_cmd)
    check_cmd.add_argument("--json", action="store_true")

    args = parser.parse_args()
    if args.cmd == "record":
        payload = record_artifact(args.candidate_json, rank=args.rank, variant=args.variant, write=True)
        ok = bool(payload.get("evidence_key_ok"))
    elif args.cmd == "approve":
        payload = approve_artifact(
            args.candidate_json,
            rank=args.rank,
            variant=args.variant,
            approved_by=args.approved_by,
            reason=args.reason,
            ttl_hours=args.ttl_hours,
            write=True,
        )
        ok = payload.get("status") == "approved_for_live"
    else:
        payload = evaluate_artifact(args.candidate_json, rank=args.rank, variant=args.variant)
        ok = bool(payload.get("ok"))

    if getattr(args, "json", False):
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    else:
        print(f"ok={ok} status={payload.get('status') or ((payload.get('record') or {}).get('status'))} candidate_id={payload.get('candidate_id')}")
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(_main())
