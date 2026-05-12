"""Authoritative lifecycle registry for scoring-profile candidates.

The registry gives every variant a stable identity and records how it moved
through the research pipeline: discovered, Step 2 validated, shadowed, promotion
ready, promoted, rejected, or retired.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import candidate_profile_schema
import contract_gate
import step2_parity_contract
import tournament_safety


HERE = Path(__file__).resolve().parent
POSTMORTEM_DIR = HERE / "postmortem"
OUT_DIR = POSTMORTEM_DIR / "candidate_lifecycle"
REGISTRY_PATH = OUT_DIR / "candidate_lifecycle_registry.json"
CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1

STATUS_ORDER = {
    "discovered": 10,
    "step2_validated": 20,
    "shadow_live": 30,
    "promotion_ready": 40,
    "promoted": 50,
    "rejected": 60,
    "retired": 70,
}


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


def _today() -> str:
    return datetime.now(CT).date().isoformat()


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


def _config() -> dict[str, Any]:
    return _read_json(HERE / "trading_config.json", {}) or {}


def _empty_registry() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "candidate_lifecycle",
        "created_at_ct": _now_ct(),
        "updated_at_ct": _now_ct(),
        "candidates": {},
        "history": [],
    }


def load_registry(path: Path = REGISTRY_PATH) -> dict[str, Any]:
    payload = _read_json(path, None)
    if not isinstance(payload, dict):
        return _empty_registry()
    payload.setdefault("schema_version", SCHEMA_VERSION)
    payload.setdefault("source", "candidate_lifecycle")
    payload.setdefault("created_at_ct", _now_ct())
    payload.setdefault("candidates", {})
    payload.setdefault("history", [])
    return payload


def write_registry(registry: dict[str, Any], path: Path = REGISTRY_PATH) -> str:
    registry["updated_at_ct"] = _now_ct()
    return _write_json(path, registry)


def _status_max(a: str | None, b: str | None) -> str:
    a = a or "discovered"
    b = b or "discovered"
    return a if STATUS_ORDER.get(a, 0) >= STATUS_ORDER.get(b, 0) else b


def _candidate_id(standard: dict[str, Any]) -> str:
    if standard.get("model_id"):
        return str(standard["model_id"])
    routes = standard.get("routes") if isinstance(standard.get("routes"), list) else []
    if routes:
        return tournament_safety.stable_json_hash({
            "name": str(standard.get("name") or standard.get("variant") or "candidate"),
            "weights": standard.get("weights") or {},
            "bias": round(float(standard.get("bias") or 0.0), 8),
            "routes": routes,
        }, length=20)
    return tournament_safety.model_id(
        str(standard.get("name") or standard.get("variant") or "candidate"),
        standard.get("weights") or {},
        float(standard.get("bias") or 0.0),
    )


def _weights_hash(weights: dict[str, Any]) -> str:
    blob = json.dumps(weights or {}, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    import hashlib

    return hashlib.sha256(blob).hexdigest()


def standardize(row: dict[str, Any],
                source_payload: dict[str, Any] | None = None,
                context: dict[str, Any] | None = None) -> dict[str, Any]:
    row = dict(row or {})
    context = dict(context or {})
    source_payload = source_payload or {}
    if row.get("candidate_schema_version") and isinstance(row.get("weights"), dict):
        standard = dict(row)
        standard.setdefault("variant", standard.get("name") or "candidate")
        standard.setdefault("name", standard.get("variant") or "candidate")
        standard.setdefault("contracts", candidate_profile_schema.contracts())
        standard.setdefault("step2", row.get("step2") or {})
        standard.setdefault("baseline", row.get("baseline") or {})
        standard.setdefault("source", row.get("source") or {})
        standard.setdefault("model_id", _candidate_id(standard))
        standard.setdefault("family_id", tournament_safety.variant_family(standard.get("weights") or {}))
        return standard
    return candidate_profile_schema.normalize(row, context=context, source_payload=source_payload)


def _event(candidate_id: str, event_type: str, status: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "created_at_ct": _now_ct(),
        "candidate_id": candidate_id,
        "event": event_type,
        "status": status,
        "payload": payload or {},
    }


def upsert_standard_candidate(standard: dict[str, Any],
                              status: str = "discovered",
                              event: str | None = None,
                              artifacts: dict[str, Any] | None = None,
                              metrics: dict[str, Any] | None = None,
                              note: str = "",
                              registry: dict[str, Any] | None = None,
                              write: bool = True) -> dict[str, Any]:
    registry = registry or load_registry()
    candidate_id = _candidate_id(standard)
    candidates = registry.setdefault("candidates", {})
    existing = candidates.get(candidate_id) or {}
    record = {
        **existing,
        "candidate_id": candidate_id,
        "model_id": standard.get("model_id") or candidate_id,
        "family_id": standard.get("family_id"),
        "name": standard.get("name") or standard.get("variant"),
        "variant": standard.get("variant") or standard.get("name"),
        "bias": standard.get("bias"),
        "weights_hash": _weights_hash(standard.get("weights") or {}),
        "weight_count": len(standard.get("weights") or {}),
        "weights": standard.get("weights") or {},
        "contracts": standard.get("contracts") or {},
        "baseline": standard.get("baseline") or {},
        "source": standard.get("source") or {},
        "status": _status_max(existing.get("status"), status),
        "first_seen_ct": existing.get("first_seen_ct") or _now_ct(),
        "last_seen_ct": _now_ct(),
    }
    step2 = standard.get("step2") or {}
    if step2:
        record["step2"] = {
            **(existing.get("step2") or {}),
            **{k: v for k, v in step2.items() if v is not None},
        }
    if artifacts:
        merged = dict(existing.get("artifacts") or {})
        merged.update({k: v for k, v in artifacts.items() if v})
        record["artifacts"] = merged
    if metrics:
        merged_metrics = dict(existing.get("metrics") or {})
        for key, value in metrics.items():
            if isinstance(value, dict) and isinstance(merged_metrics.get(key), dict):
                merged_metrics[key] = {**merged_metrics[key], **value}
            else:
                merged_metrics[key] = value
        record["metrics"] = merged_metrics
    if note:
        notes = list(existing.get("notes") or [])
        notes.append({"created_at_ct": _now_ct(), "note": note})
        record["notes"] = notes[-50:]
    candidates[candidate_id] = record
    evt = _event(candidate_id, event or f"status:{status}", record["status"], {
        "name": record.get("name"),
        "artifacts": artifacts or {},
        "metrics_keys": sorted((metrics or {}).keys()),
    })
    registry.setdefault("history", []).append(evt)
    registry["history"] = registry["history"][-2000:]
    if write:
        write_registry(registry)
    return record


def register_candidate(row: dict[str, Any],
                       source_payload: dict[str, Any] | None = None,
                       context: dict[str, Any] | None = None,
                       status: str = "discovered",
                       event: str | None = None,
                       artifacts: dict[str, Any] | None = None,
                       metrics: dict[str, Any] | None = None,
                       write: bool = True) -> dict[str, Any]:
    standard = standardize(row, source_payload=source_payload, context=context)
    return upsert_standard_candidate(
        standard,
        status=status,
        event=event,
        artifacts=artifacts,
        metrics=metrics,
        registry=None,
        write=write,
    )


def register_artifact(candidate_json: str,
                      rank: int = 1,
                      variant: str = "",
                      status: str = "step2_validated",
                      write: bool = True) -> dict[str, Any]:
    path = Path(candidate_json).resolve()
    payload = _read_json(path, {}) or {}
    if not isinstance(payload, dict):
        raise RuntimeError(f"candidate artifact must be JSON object: {path}")
    row = candidate_profile_schema.select(payload, variant=variant, rank=rank)
    standard = standardize(
        row,
        source_payload=payload,
        context={"artifact": str(path), "rank": rank},
    )
    return upsert_standard_candidate(
        standard,
        status=status,
        event="candidate_artifact_registered",
        artifacts={"candidate_json": str(path), "rank": rank, "variant": variant or standard.get("variant")},
        write=write,
    )


def _profile_rows_from_shadow_candidates(path: Path) -> dict[str, dict[str, Any]]:
    payload = _read_json(path, {}) or {}
    profiles = payload.get("profiles") if isinstance(payload, dict) else []
    out = {}
    for row in profiles or []:
        if not isinstance(row, dict):
            continue
        model_id = row.get("model_id") or _candidate_id(row)
        out[str(model_id)] = row
    return out


def register_shadow_day(day: str, write: bool = True) -> dict[str, Any]:
    leaderboard_path = POSTMORTEM_DIR / "shadow_variants" / day / f"shadow_variant_leaderboard_{day}.json"
    candidates_path = POSTMORTEM_DIR / "shadow_variants" / "candidates.json"
    leaderboard = _read_json(leaderboard_path, {}) or {}
    profile_by_id = _profile_rows_from_shadow_candidates(candidates_path)
    rows = leaderboard.get("leaderboard") or leaderboard.get("top5") or []
    registered = []
    registry = load_registry()
    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        model_id = str(row.get("model_id") or "")
        profile = dict(profile_by_id.get(model_id) or {})
        if not profile and not row.get("weights"):
            continue
        shadow_metric_keys = {
            "pnl",
            "entered",
            "wins",
            "losses",
            "win_rate_pct",
            "signals_seen",
            "skipped",
            "by_ticker",
            "skip_reasons",
            "outcome_reasons",
            "delta_vs_current_live",
            "delta_pct_vs_current_live",
        }
        candidate_row = {
            **profile,
            **{
                k: v for k, v in row.items()
                if k not in shadow_metric_keys and k not in ("adapter_snapshot", "skip_reasons")
            },
        }
        standard = standardize(
            candidate_row,
            source_payload={"candidate_path": str(candidates_path), "shadow_leaderboard": str(leaderboard_path)},
            context={"artifact": str(candidates_path), "rank": idx + 1},
        )
        rec = upsert_standard_candidate(
            standard,
            status="shadow_live",
            event="shadow_day_registered",
            artifacts={
                "shadow_candidate_path": str(candidates_path),
                "shadow_leaderboard": str(leaderboard_path),
            },
            metrics={
                "shadow_live": {
                    day: {
                        "rank": row.get("rank", idx + 1),
                        "pnl": row.get("pnl"),
                        "entered": row.get("entered"),
                        "wins": row.get("wins"),
                        "losses": row.get("losses"),
                        "win_rate_pct": row.get("win_rate_pct"),
                        "delta_vs_current_live": row.get("delta_vs_current_live"),
                        "delta_pct_vs_current_live": row.get("delta_pct_vs_current_live"),
                    }
                }
            },
            registry=registry,
            write=False,
        )
        registered.append(rec.get("candidate_id"))
    if write:
        write_registry(registry)
    return {
        "day": day,
        "leaderboard_path": str(leaderboard_path.resolve()),
        "leaderboard_exists": leaderboard_path.exists(),
        "registered_count": len(registered),
        "candidate_ids": registered,
    }


def attach_evidence(candidate_id: str,
                    evidence_packet: dict[str, Any],
                    status: str = "promotion_ready",
                    write: bool = True) -> dict[str, Any]:
    registry = load_registry()
    record = registry.setdefault("candidates", {}).get(candidate_id)
    if not record:
        raise RuntimeError(f"candidate not found in lifecycle registry: {candidate_id}")
    packets = list(record.get("promotion_evidence_packets") or [])
    output = evidence_packet.get("output") if isinstance(evidence_packet, dict) else {}
    packets.append({
        "created_at_ct": _now_ct(),
        "ok": bool(evidence_packet.get("ok")) if isinstance(evidence_packet, dict) else False,
        "json_path": (output or {}).get("json_path"),
        "txt_path": (output or {}).get("txt_path"),
        "days": evidence_packet.get("days") if isinstance(evidence_packet, dict) else [],
    })
    record["promotion_evidence_packets"] = packets[-20:]
    record["status"] = _status_max(record.get("status"), status)
    record["last_seen_ct"] = _now_ct()
    registry.setdefault("history", []).append(_event(candidate_id, "promotion_evidence_attached", record["status"], packets[-1]))
    if write:
        write_registry(registry)
    return record


def record_promotion(candidate_id: str,
                     registry_path: str,
                     config_path: str = "",
                     write: bool = True) -> dict[str, Any]:
    registry = load_registry()
    record = registry.setdefault("candidates", {}).get(candidate_id)
    if not record:
        raise RuntimeError(f"candidate not found in lifecycle registry: {candidate_id}")
    promotions = list(record.get("promotions") or [])
    promotions.append({
        "created_at_ct": _now_ct(),
        "registry_path": registry_path,
        "config_path": config_path or str((HERE / "trading_config.json").resolve()),
    })
    record["promotions"] = promotions[-20:]
    record["status"] = "promoted"
    record["last_seen_ct"] = _now_ct()
    registry.setdefault("history", []).append(_event(candidate_id, "promoted", "promoted", promotions[-1]))
    if write:
        write_registry(registry)
    return record


def mark(candidate_id: str, status: str, reason: str = "", write: bool = True) -> dict[str, Any]:
    if status not in STATUS_ORDER:
        raise RuntimeError(f"unknown lifecycle status: {status}")
    registry = load_registry()
    record = registry.setdefault("candidates", {}).get(candidate_id)
    if not record:
        raise RuntimeError(f"candidate not found in lifecycle registry: {candidate_id}")
    record["status"] = status
    record["last_seen_ct"] = _now_ct()
    if reason:
        record.setdefault("notes", []).append({"created_at_ct": _now_ct(), "note": reason})
    registry.setdefault("history", []).append(_event(candidate_id, f"marked:{status}", status, {"reason": reason}))
    if write:
        write_registry(registry)
    return record


def evaluate_record(record: dict[str, Any] | None,
                    evidence_packet: dict[str, Any] | None = None,
                    require_shadow: bool = False) -> dict[str, Any]:
    record = record or {}
    evidence_packet = evidence_packet or {}
    contracts = record.get("contracts") or {}
    cfg = _config()
    live_contract = step2_parity_contract.contract(cfg)
    live_hash = step2_parity_contract.contract_hash(live_contract)
    candidate_hash = contracts.get("step2_parity_contract_hash")
    gate = contract_gate.check(cfg, day=_today())
    checks = [
        {"name": "lifecycle_record_exists", "ok": bool(record), "actual": bool(record)},
        {"name": "candidate_has_weights", "ok": bool(record.get("weights")), "actual": record.get("weight_count")},
        {
            "name": "candidate_step2_pnl_present",
            "ok": ((record.get("step2") or {}).get("pnl") is not None),
            "actual": (record.get("step2") or {}).get("pnl"),
        },
        {
            "name": "candidate_step2_parity_contract_hash_matches_live",
            "ok": bool(candidate_hash and candidate_hash == live_hash),
            "actual": candidate_hash,
            "expected": live_hash,
        },
        {
            "name": "contract_gate_ok",
            "ok": bool(gate.get("ok")),
            "actual": gate.get("critical_failure_count"),
            "expected": 0,
        },
    ]
    if evidence_packet:
        checks.append({
            "name": "promotion_evidence_packet_ok",
            "ok": bool(evidence_packet.get("ok")),
            "actual": evidence_packet.get("ok"),
        })
    else:
        checks.append({
            "name": "promotion_evidence_packet_present",
            "ok": bool(record.get("promotion_evidence_packets")),
            "actual": len(record.get("promotion_evidence_packets") or []),
        })
    if require_shadow:
        checks.append({
            "name": "shadow_live_metrics_present",
            "ok": bool((record.get("metrics") or {}).get("shadow_live")),
            "actual": sorted(((record.get("metrics") or {}).get("shadow_live") or {}).keys()),
        })
    return {
        "schema_version": SCHEMA_VERSION,
        "created_at_ct": _now_ct(),
        "ok": all(bool(c.get("ok")) for c in checks),
        "candidate_id": record.get("candidate_id"),
        "status": record.get("status"),
        "checks": checks,
        "contract_gate": {
            "ok": gate.get("ok"),
            "critical_failure_count": gate.get("critical_failure_count"),
            "hashes": gate.get("hashes"),
        },
        "deduction": (
            "Promotion readiness requires the candidate to exist in the lifecycle registry, "
            "have Step 2 evidence, match the current execution/parity contract, and have a clean evidence packet."
        ),
    }


def evaluate_candidate(candidate_id: str = "",
                       candidate_json: str = "",
                       rank: int = 1,
                       variant: str = "",
                       evidence_packet: dict[str, Any] | None = None,
                       require_shadow: bool = False,
                       write: bool = True) -> dict[str, Any]:
    if candidate_id:
        record = (load_registry().get("candidates") or {}).get(candidate_id)
    elif candidate_json:
        record = register_artifact(candidate_json, rank=rank, variant=variant, write=write)
    else:
        record = None
    return evaluate_record(record, evidence_packet=evidence_packet, require_shadow=require_shadow)


def sync_from_known_artifacts(day: str | None = None, write: bool = True) -> dict[str, Any]:
    cfg = _config()
    profile = step2_parity_contract.active_profile_snapshot(cfg, include_weights=True)
    active_record = None
    if profile.get("weights"):
        active_record = register_candidate(
            {
                "name": profile.get("name") or "current_live_active_profile",
                "variant": profile.get("name") or "current_live_active_profile",
                "bias": profile.get("bias") or 0.0,
                "weights": profile.get("weights") or {},
                "source": "current_live_config",
            },
            source_payload={"source": "current_live_config"},
            status="promoted",
            event="active_profile_synced",
            artifacts={"config": str((HERE / "trading_config.json").resolve())},
            write=write,
        )
    shadow = register_shadow_day(day, write=write) if day else None
    return {
        "day": day,
        "active_candidate_id": (active_record or {}).get("candidate_id"),
        "shadow": shadow,
        "registry_path": str(REGISTRY_PATH.resolve()),
    }


def dashboard(day: str | None = None) -> dict[str, Any]:
    registry = load_registry()
    candidates = list((registry.get("candidates") or {}).values())
    status_counts = Counter(str(c.get("status") or "unknown") for c in candidates)
    by_step2 = sorted(
        candidates,
        key=lambda c: float(((c.get("step2") or {}).get("pnl") if (c.get("step2") or {}).get("pnl") is not None else -10**18)),
        reverse=True,
    )
    promoted = [c for c in candidates if c.get("status") == "promoted"]
    promotion_ready = [c for c in candidates if c.get("status") == "promotion_ready"]
    shadowed = [c for c in candidates if (c.get("metrics") or {}).get("shadow_live")]
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "candidate_lifecycle_dashboard",
        "created_at_ct": _now_ct(),
        "day": day,
        "registry_path": str(REGISTRY_PATH.resolve()),
        "candidate_count": len(candidates),
        "status_counts": dict(sorted(status_counts.items())),
        "top_step2": [_public_record(c) for c in by_step2[:10]],
        "promotion_ready": [_public_record(c) for c in promotion_ready[:10]],
        "promoted": [_public_record(c) for c in promoted[:10]],
        "shadowed_count": len(shadowed),
        "recent_history": (registry.get("history") or [])[-20:],
        "deduction": (
            "This is the single map of candidate state. Promotion should point back here "
            "instead of relying on loose JSON artifacts."
        ),
    }


def _public_record(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "candidate_id": record.get("candidate_id"),
        "name": record.get("name"),
        "status": record.get("status"),
        "family_id": record.get("family_id"),
        "weight_count": record.get("weight_count"),
        "step2_pnl": (record.get("step2") or {}).get("pnl"),
        "step2_trades": (record.get("step2") or {}).get("trades"),
        "baseline": record.get("baseline"),
        "shadow_days": sorted(((record.get("metrics") or {}).get("shadow_live") or {}).keys()),
        "evidence_packets": len(record.get("promotion_evidence_packets") or []),
        "promotions": len(record.get("promotions") or []),
        "first_seen_ct": record.get("first_seen_ct"),
        "last_seen_ct": record.get("last_seen_ct"),
    }


def render_text(payload: dict[str, Any]) -> str:
    lines = [
        f"Candidate Lifecycle Dashboard - {payload.get('day') or 'all'}",
        f"Created CT: {payload.get('created_at_ct')}",
        "",
        f"Candidates: {payload.get('candidate_count')}",
        f"Statuses: {json.dumps(payload.get('status_counts') or {}, sort_keys=True)}",
        "",
        "Top Step 2",
    ]
    for row in payload.get("top_step2") or []:
        lines.append(
            f"- {row.get('name')} id={row.get('candidate_id')} status={row.get('status')} "
            f"step2={row.get('step2_pnl')} evidence={row.get('evidence_packets')}"
        )
    lines.extend(["", "Promotion Ready"])
    for row in payload.get("promotion_ready") or []:
        lines.append(f"- {row.get('name')} id={row.get('candidate_id')} step2={row.get('step2_pnl')}")
    lines.extend(["", "Promoted"])
    for row in payload.get("promoted") or []:
        lines.append(f"- {row.get('name')} id={row.get('candidate_id')} promotions={row.get('promotions')}")
    lines.extend(["", "Use", "- Treat this as the source of truth for variant identity and promotion evidence.", ""])
    return "\n".join(lines)


def write_dashboard(day: str | None = None) -> dict[str, Any]:
    payload = dashboard(day=day)
    suffix = day or "all"
    json_path = OUT_DIR / f"candidate_lifecycle_dashboard_{suffix}.json"
    txt_path = OUT_DIR / f"candidate_lifecycle_dashboard_{suffix}.txt"
    payload["output"] = {
        "json_path": _write_json(json_path, payload),
        "txt_path": _write_text(txt_path, render_text(payload)),
    }
    _write_json(json_path, payload)
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description="Manage candidate lifecycle registry.")
    sub = ap.add_subparsers(dest="command", required=True)
    p_register = sub.add_parser("register-artifact")
    p_register.add_argument("--candidate-json", required=True)
    p_register.add_argument("--rank", type=int, default=1)
    p_register.add_argument("--variant", default="")
    p_register.add_argument("--status", default="step2_validated", choices=sorted(STATUS_ORDER))
    p_shadow = sub.add_parser("register-shadow-day")
    p_shadow.add_argument("day")
    p_sync = sub.add_parser("sync")
    p_sync.add_argument("--day", default="")
    p_eval = sub.add_parser("evaluate")
    p_eval.add_argument("--candidate-id", default="")
    p_eval.add_argument("--candidate-json", default="")
    p_eval.add_argument("--rank", type=int, default=1)
    p_eval.add_argument("--variant", default="")
    p_eval.add_argument("--require-shadow", action="store_true")
    p_dash = sub.add_parser("dashboard")
    p_dash.add_argument("--day", default="")
    p_mark = sub.add_parser("mark")
    p_mark.add_argument("--candidate-id", required=True)
    p_mark.add_argument("--status", required=True, choices=sorted(STATUS_ORDER))
    p_mark.add_argument("--reason", default="")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    if args.command == "register-artifact":
        payload = register_artifact(args.candidate_json, rank=args.rank, variant=args.variant, status=args.status)
    elif args.command == "register-shadow-day":
        payload = register_shadow_day(args.day)
    elif args.command == "sync":
        payload = sync_from_known_artifacts(args.day or None)
    elif args.command == "evaluate":
        payload = evaluate_candidate(
            candidate_id=args.candidate_id,
            candidate_json=args.candidate_json,
            rank=args.rank,
            variant=args.variant,
            require_shadow=args.require_shadow,
        )
    elif args.command == "mark":
        payload = mark(args.candidate_id, args.status, reason=args.reason)
    else:
        payload = write_dashboard(args.day or None)
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0 if payload.get("ok", True) is not False else 2


if __name__ == "__main__":
    raise SystemExit(main())
