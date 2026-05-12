"""Promotion evidence packets for scoring-profile changes.

The packet is deliberately redundant: it ties a candidate to the Step 2 score,
shadow-variant behavior, contract hashes, parity reports, and promotion
registry. Future promotions should leave this breadcrumb automatically.
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
import candidate_lifecycle
import candidate_reproducibility_gate
import baseline_drift_sentinel
import candidate_robustness_report
import promotion_candidate_quarantine
import canonical_decision_packet
import contract_gate
import market_data_integrity_gate
import step2_execution_contract
import step2_parity_contract
import tournament_safety


HERE = Path(__file__).resolve().parent
POSTMORTEM_DIR = HERE / "postmortem"
OUT_DIR = POSTMORTEM_DIR / "promotion_evidence"
CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1


def _stamp() -> str:
    return datetime.now(CT).strftime("%Y%m%d_%H%M%S")


def _slug(text: str) -> str:
    token = re.sub(r"[^A-Za-z0-9_.+-]+", "_", text or "candidate").strip("_")
    return (token or "candidate")[:96]


def _read_json(path: str | os.PathLike[str]) -> dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _file_sha256(path: str | os.PathLike[str]) -> str | None:
    try:
        p = Path(path)
        if not p.exists() or not p.is_file():
            return None
        h = hashlib.sha256()
        with p.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None


def _stable_hash(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def candidate_profile_hash(candidate: dict[str, Any]) -> str:
    weights = {
        str(key): round(float(value or 0.0), 6)
        for key, value in sorted((candidate.get("weights") or {}).items())
        if abs(float(value or 0.0)) > 1e-12
    }
    semantic = {
        "enabled": True,
        "name": candidate.get("name") or candidate.get("variant"),
        "bias": round(float(candidate.get("bias") or 0.0), 6),
        "weights": weights,
    }
    if candidate.get("routes"):
        semantic["routes"] = list(candidate.get("routes") or [])
        semantic["routed_scoring_profile"] = True
    return step2_parity_contract.contract_hash(semantic)


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


def _latest_file(folder: Path, pattern: str = "*.json") -> Path | None:
    if not folder.exists():
        return None
    files = [p for p in folder.glob(pattern) if p.is_file()]
    if not files:
        return None
    return max(files, key=lambda p: p.stat().st_mtime)


def _config() -> dict[str, Any]:
    return _read_json(HERE / "trading_config.json")


def _select_candidate(candidate_json: str = "", rank: int = 1, variant: str = "") -> dict[str, Any]:
    if not candidate_json:
        profile = step2_parity_contract.active_profile_snapshot(_config(), include_weights=True)
        return {
            "variant": profile.get("name") or "current_live_active_profile",
            "name": profile.get("name") or "current_live_active_profile",
            "bias": profile.get("bias") or 0.0,
            "weights": profile.get("weights") or {},
            "routes": profile.get("routes") or [],
            "routed_scoring_profile": bool(profile.get("routes")),
            "source": "current_live_config",
        }
    payload = _read_json(candidate_json)
    row = candidate_profile_schema.select(payload, variant=variant, rank=rank)
    row = dict(row)
    row.setdefault("source", os.path.abspath(candidate_json))
    return row


def _shadow_leaderboards(days: list[str]) -> dict[str, Any]:
    out = {}
    for day in days:
        path = POSTMORTEM_DIR / "shadow_variants" / day / f"shadow_variant_leaderboard_{day}.json"
        out[day] = _read_json(path)
    return out


def _step2_scores(days: list[str]) -> dict[str, Any]:
    out = {}
    for day in days:
        path = POSTMORTEM_DIR / "backtests" / "step2_today_compiled" / f"step2_today_compiled_{day}.json"
        payload = _read_json(path)
        result = ((payload.get("score") or {}).get("result") or {}) if payload else {}
        out[day] = {
            "path": str(path.resolve()),
            "exists": path.exists(),
            "pnl": result.get("pnl"),
            "trades": result.get("trades"),
            "wins": result.get("wins"),
            "losses": result.get("losses"),
            "execution_contract_hash": (((result.get("mock_parity_guards") or {})).get("execution_contract_hash")),
        }
    return out


def _parity_verdicts(days: list[str]) -> dict[str, Any]:
    out = {}
    for day in days:
        path = POSTMORTEM_DIR / "parity_verdict" / f"parity_verdict_{day}.json"
        payload = _read_json(path)
        out[day] = {
            "path": str(path.resolve()),
            "exists": path.exists(),
            "verdict": payload.get("verdict"),
            "promotion_safe": payload.get("promotion_safe"),
            "critical_count": payload.get("critical_count"),
            "warning_count": payload.get("warning_count"),
            "packet_hash": _file_sha256(path),
        }
    return out


def _market_data_integrity(days: list[str]) -> dict[str, Any]:
    out = {}
    for day in days:
        try:
            payload = market_data_integrity_gate.build(day, source="canonical", write=True, use_cached=True)
            path = Path(payload.get("path") or market_data_integrity_gate.output_paths(day)["json"])
            out[day] = {
                "path": str(path.resolve()),
                "exists": path.exists(),
                "verdict": payload.get("verdict"),
                "ok": payload.get("ok"),
                "promotion_safe": payload.get("promotion_safe"),
                "critical_count": payload.get("critical_count"),
                "warning_count": payload.get("warning_count"),
                "source_used": payload.get("source_used"),
                "source_tape_hash": (payload.get("source_tape") or {}).get("sha256"),
                "packet_hash": _file_sha256(path),
            }
        except Exception as exc:
            path = Path(market_data_integrity_gate.output_paths(day)["json"])
            out[day] = {
                "path": str(path.resolve()),
                "exists": path.exists(),
                "ok": False,
                "promotion_safe": False,
                "error": repr(exc),
            }
    return out


def _artifact_registries(days: list[str]) -> dict[str, Any]:
    out = {}
    for day in days:
        path = POSTMORTEM_DIR / "artifact_registry" / f"artifact_registry_{day}.json"
        payload = _read_json(path)
        out[day] = {
            "path": str(path.resolve()),
            "exists": path.exists(),
            "registry_hash": payload.get("registry_hash"),
            "file_hash": _file_sha256(path),
        }
    return out


def _latency_model() -> dict[str, Any]:
    path = POSTMORTEM_DIR / "latency_model" / "step2_latency_model.json"
    payload = _read_json(path)
    return {
        "path": str(path.resolve()),
        "exists": path.exists(),
        "file_hash": _file_sha256(path),
        "model_hash": payload.get("model_hash") or payload.get("hash"),
        "created_at_ct": payload.get("created_at_ct"),
    }


def _canonical_packet_evidence(days: list[str]) -> dict[str, Any]:
    out = {}
    for day in days:
        try:
            out[day] = canonical_decision_packet.schema_compatibility(day)
        except Exception as exc:
            out[day] = {"ok": False, "error": repr(exc), "day": day}
    return out


def _latest_promotion_registry() -> dict[str, Any]:
    path = _latest_file(POSTMORTEM_DIR / "promotions" / "active_scoring_profiles")
    if not path:
        return {"exists": False}
    payload = _read_json(path)
    return {"exists": True, "path": str(path.resolve()), "payload": payload}


def render_text(packet: dict[str, Any]) -> str:
    candidate = packet.get("candidate") or {}
    lifecycle = packet.get("candidate_lifecycle") or {}
    checks = packet.get("checks") or []
    lines = [
        f"Promotion Evidence Packet - {candidate.get('name') or candidate.get('variant')}",
        f"Created CT: {packet.get('created_at_ct')}",
        "",
        "Candidate",
        f"- Model id: {candidate.get('model_id')}",
        f"- Family id: {candidate.get('family_id')}",
        f"- Profile hash: {candidate.get('profile_hash')}",
        f"- Source: {candidate.get('source')}",
        f"- Step 2 P/L: {(candidate.get('step2') or {}).get('pnl')}",
        f"- Lifecycle status: {lifecycle.get('status')} id={lifecycle.get('candidate_id')}",
        "",
        "Checks",
    ]
    for check in checks:
        lines.append(f"- {check.get('name')}: {check.get('ok')} {check.get('detail') or ''}")
    lines.extend([
        "",
        "Contracts",
        f"- Step 2 parity hash: {packet.get('contracts', {}).get('step2_parity_contract_hash')}",
        f"- Step 2 execution hash: {packet.get('contracts', {}).get('step2_execution_contract_hash')}",
        f"- Contract gate ok: {(packet.get('contract_gate') or {}).get('ok')}",
        f"- Latency model hash: {(packet.get('latency_model') or {}).get('file_hash')}",
        f"- Source baseline drift: {(packet.get('baseline_drift') or {}).get('status')}",
        f"- Candidate reproduced: {(packet.get('candidate_reproducibility') or {}).get('ok')}",
        f"- Candidate quarantine: {(packet.get('candidate_quarantine') or {}).get('ok')}",
        f"- Candidate robustness: {((packet.get('candidate_robustness') or {}).get('selected') or {}).get('robustness_score')}"
        f" grade={((packet.get('candidate_robustness') or {}).get('selected') or {}).get('robustness_grade')}",
        f"- Robustness flags: {', '.join((((packet.get('candidate_robustness') or {}).get('selected') or {}).get('red_flags') or []))}",
        "",
        "Parity Verdicts",
    ])
    for day, row in (packet.get("parity_verdicts") or {}).items():
        lines.append(f"- {day}: verdict={row.get('verdict')} promotion_safe={row.get('promotion_safe')} critical={row.get('critical_count')}")
    lines.extend([
        "",
        "Market Data Integrity",
    ])
    for day, row in (packet.get("market_data_integrity") or {}).items():
        lines.append(f"- {day}: verdict={row.get('verdict')} promotion_safe={row.get('promotion_safe')} critical={row.get('critical_count')} source={row.get('source_used')}")
    lines.extend([
        "",
        "Shadow Leaderboards",
    ])
    for day, board in (packet.get("shadow_leaderboards") or {}).items():
        top = (board.get("top5") or [{}])[0] if isinstance(board, dict) else {}
        lines.append(f"- {day}: exists={bool(board)} top={top.get('profile_name')} pnl={top.get('pnl')}")
    lines.extend([
        "",
        "Canonical Decision Packets",
    ])
    for day, row in (packet.get("canonical_decision_packets") or {}).items():
        lines.append(f"- {day}: ok={row.get('ok')} counts={row.get('counts')}")
    lines.extend([
        "",
        "Use",
        "- Promote only when this packet has contract-clean evidence and the candidate is not merely a stale artifact.",
        "",
    ])
    return "\n".join(lines)


def build(candidate_json: str = "",
          rank: int = 1,
          variant: str = "",
          days: list[str] | None = None,
          promotion_registry_path: str = "",
          write: bool = True) -> dict[str, Any]:
    days = days or []
    source_payload = _read_json(candidate_json) if candidate_json else {}
    source_is_step2 = bool(candidate_json and baseline_drift_sentinel.source_looks_step2(source_payload))
    source_envelope = source_payload.get("step2_evaluation_envelope") if isinstance(source_payload.get("step2_evaluation_envelope"), dict) else {}
    source_compiled = source_envelope.get("compiled_decision_tape") if isinstance(source_envelope.get("compiled_decision_tape"), dict) else {}
    source_lineage = source_compiled.get("lineage_validation") if isinstance(source_compiled.get("lineage_validation"), dict) else {}
    baseline_drift = (
        baseline_drift_sentinel.evaluate_payload(source_payload)
        if source_is_step2
        else {"status": "skipped", "reason": "not_step2_payload", "ok": True}
    )
    row = _select_candidate(candidate_json, rank=rank, variant=variant)
    candidate_reproducibility = {"ok": True, "skipped": True, "reason": "not_step2_payload"}
    if source_is_step2:
        try:
            candidate_reproducibility = candidate_reproducibility_gate.evaluate_payload(
                source_payload,
                row,
                candidate_json=candidate_json,
                rank=rank,
                variant=variant,
            )
            if write:
                candidate_reproducibility_gate.write_report(candidate_reproducibility, label="promotion_evidence")
        except Exception as exc:
            candidate_reproducibility = {"ok": False, "error": repr(exc), "failed_checks": []}
    candidate_quarantine = {"ok": True, "skipped": True, "reason": "not_step2_payload"}
    if source_is_step2:
        try:
            candidate_quarantine = promotion_candidate_quarantine.evaluate_artifact(
                candidate_json,
                rank=rank,
                variant=variant,
            )
        except Exception as exc:
            candidate_quarantine = {"ok": False, "error": repr(exc), "failed_checks": []}
    candidate_robustness = {"skipped": True, "reason": "not_step2_payload", "report_only": True}
    if source_is_step2:
        try:
            robustness_report = candidate_robustness_report.build_from_payload(
                source_payload,
                top_n=max(50, int(rank)),
                source_path=os.path.abspath(candidate_json),
            )
            selected = None
            target_name = str(row.get("variant") or row.get("name") or "")
            target_model_id = row.get("model_id")
            for item in robustness_report.get("top_by_raw_pnl") or []:
                if target_model_id and item.get("model_id") == target_model_id:
                    selected = item
                    break
                if target_name and item.get("variant") == target_name:
                    selected = item
                    break
            if selected is None:
                selected = candidate_robustness_report.evaluate_candidate(
                    row,
                    active_row=source_payload.get("active") if isinstance(source_payload.get("active"), dict) else {},
                    start_balance=float(source_payload.get("start_balance") or 100000.0),
                    rank=rank,
                )
            candidate_robustness = {
                "summary": robustness_report.get("summary"),
                "selected": selected,
                "report_hash": robustness_report.get("report_hash"),
                "report_only": True,
            }
        except Exception as exc:
            candidate_robustness = {"skipped": False, "error": repr(exc), "report_only": True}
    standard_row = dict(row)
    reproduced_summary = (
        candidate_reproducibility.get("reproduced")
        if isinstance(candidate_reproducibility.get("reproduced"), dict)
        else {}
    )
    if (
        isinstance(standard_row.get("routes"), list)
        and standard_row.get("routes")
        and not standard_row.get("route_audit")
        and isinstance(reproduced_summary.get("route_audit"), dict)
    ):
        standard_row["route_audit"] = reproduced_summary["route_audit"]
    standard = candidate_profile_schema.normalize(
        standard_row,
        context={"artifact": os.path.abspath(candidate_json) if candidate_json else "current_live_config", "rank": rank},
        source_payload=source_payload,
    )
    cfg = _config()
    contracts = candidate_profile_schema.contracts(cfg)
    gate = contract_gate.check(cfg, day=datetime.now(CT).date().isoformat())
    model_id = standard.get("model_id")
    if not model_id and standard.get("routes"):
        model_id = tournament_safety.stable_json_hash({
            "name": standard.get("name") or "candidate",
            "weights": standard.get("weights") or {},
            "bias": float(standard.get("bias") or 0.0),
            "routes": standard.get("routes") or [],
        }, length=20)
    if not model_id:
        model_id = tournament_safety.model_id(
            standard.get("name") or "candidate",
            standard.get("weights") or {},
            float(standard.get("bias") or 0.0),
        )
    registry = _read_json(promotion_registry_path) if promotion_registry_path else _latest_promotion_registry()
    canonical_packets = _canonical_packet_evidence(days)
    step2_scores = _step2_scores(days)
    parity_verdicts = _parity_verdicts(days)
    market_data_integrity = _market_data_integrity(days)
    artifact_registries = _artifact_registries(days)
    latency_model = _latency_model()
    profile_hash = candidate_profile_hash(standard)
    lifecycle_record = {}
    try:
        lifecycle_record = candidate_lifecycle.register_candidate(
            standard,
            source_payload=source_payload if candidate_json else {"source": "current_live_config"},
            context={"artifact": os.path.abspath(candidate_json) if candidate_json else "current_live_config", "rank": rank},
            status="step2_validated" if candidate_json else "promoted",
            event="promotion_evidence_packet_linked",
            artifacts={"candidate_json": os.path.abspath(candidate_json)} if candidate_json else {"config": str(HERE / "trading_config.json")},
            write=write,
        )
    except Exception as exc:
        lifecycle_record = {"error": repr(exc)}
    checks = [
        {
            "name": "candidate_has_weights",
            "ok": bool(standard.get("weights")),
            "detail": f"{len(standard.get('weights') or {})} weights",
        },
        {
            "name": "candidate_profile_hash_present",
            "ok": bool(profile_hash),
            "detail": profile_hash,
        },
        {
            "name": "contract_gate_ok",
            "ok": bool(gate.get("ok")),
            "detail": f"critical={gate.get('critical_failure_count')}",
        },
        {
            "name": "step2_parity_contract_hash_present",
            "ok": bool(contracts.get("step2_parity_contract_hash")),
            "detail": contracts.get("step2_parity_contract_hash"),
        },
        {
            "name": "step2_execution_contract_hash_present",
            "ok": bool(contracts.get("step2_execution_contract_hash")),
            "detail": contracts.get("step2_execution_contract_hash"),
        },
        {
            "name": "candidate_lifecycle_record_present",
            "ok": bool(lifecycle_record.get("candidate_id")),
            "detail": lifecycle_record.get("candidate_id") or lifecycle_record.get("error"),
        },
        {
            "name": "canonical_decision_packet_schema",
            "ok": all(bool(row.get("ok")) for row in canonical_packets.values()) if canonical_packets else True,
            "detail": {day: row.get("counts") for day, row in canonical_packets.items()},
            "skipped": not bool(canonical_packets),
        },
        {
            "name": "step2_scores_present_for_evidence_days",
            "ok": all(bool(row.get("exists") and row.get("pnl") is not None) for row in step2_scores.values()) if step2_scores else True,
            "detail": {day: {"exists": row.get("exists"), "pnl": row.get("pnl"), "trades": row.get("trades")}
                       for day, row in step2_scores.items()},
            "skipped": not bool(step2_scores),
        },
        {
            "name": "parity_verdicts_promotion_safe",
            "ok": all(bool(row.get("exists") and row.get("promotion_safe")) for row in parity_verdicts.values()) if parity_verdicts else True,
            "detail": {day: {"exists": row.get("exists"), "verdict": row.get("verdict"), "promotion_safe": row.get("promotion_safe")}
                       for day, row in parity_verdicts.items()},
            "skipped": not bool(parity_verdicts),
        },
        {
            "name": "market_data_integrity_promotion_safe",
            "ok": all(bool(row.get("exists") and row.get("promotion_safe")) for row in market_data_integrity.values()) if market_data_integrity else True,
            "detail": {day: {"exists": row.get("exists"), "verdict": row.get("verdict"), "promotion_safe": row.get("promotion_safe")}
                       for day, row in market_data_integrity.items()},
            "skipped": not bool(market_data_integrity),
        },
        {
            "name": "artifact_registries_present",
            "ok": all(bool(row.get("exists") and row.get("registry_hash")) for row in artifact_registries.values()) if artifact_registries else True,
            "detail": {day: {"exists": row.get("exists"), "registry_hash": row.get("registry_hash")}
                       for day, row in artifact_registries.items()},
            "skipped": not bool(artifact_registries),
        },
        {
            "name": "latency_model_present",
            "ok": bool(latency_model.get("exists") and latency_model.get("file_hash")) if days else True,
            "detail": latency_model.get("path"),
            "skipped": not bool(days),
        },
        {
            "name": "step2_source_evaluation_envelope_present",
            "ok": bool(source_envelope) if source_is_step2 else True,
            "detail": (source_envelope or {}).get("envelope_hash") if source_is_step2 else "not_step2_payload",
            "skipped": not source_is_step2,
        },
        {
            "name": "step2_source_evaluation_envelope_promotable",
            "ok": bool((source_envelope or {}).get("promotable")) if source_is_step2 else True,
            "detail": (source_envelope or {}).get("non_promotable_reasons") if source_is_step2 else "not_step2_payload",
            "skipped": not source_is_step2,
        },
        {
            "name": "step2_source_compiled_tape_lineage_certified",
            "ok": (
                bool(source_lineage)
                and source_lineage.get("status") == "CERTIFIED_MATCH"
                and source_lineage.get("certified") is True
                and not bool(source_lineage.get("rebuild_required"))
            ) if source_is_step2 else True,
            "detail": {
                "status": source_lineage.get("status"),
                "certified": source_lineage.get("certified"),
                "recommended_action": source_lineage.get("recommended_action"),
            } if source_is_step2 else "not_step2_payload",
            "skipped": not source_is_step2,
        },
        {
            "name": "step2_source_artifact_promotable",
            "ok": source_payload.get("promotable") is not False if source_is_step2 else True,
            "detail": source_payload.get("non_promotable_reason") if source_is_step2 else "not_step2_payload",
            "skipped": not source_is_step2,
        },
        {
            "name": "step2_source_baseline_matches_live",
            "ok": baseline_drift.get("status") == "current" if source_is_step2 else True,
            "detail": baseline_drift,
            "skipped": not source_is_step2,
        },
        {
            "name": "step2_candidate_reproducibility_ok",
            "ok": bool(candidate_reproducibility.get("ok")) if source_is_step2 else True,
            "detail": {
                "output_path": candidate_reproducibility.get("output_path"),
                "failed_checks": candidate_reproducibility.get("failed_checks", [])[:10],
                "error": candidate_reproducibility.get("error"),
            } if source_is_step2 else "not_step2_payload",
            "skipped": not source_is_step2,
        },
        {
            "name": "step2_candidate_quarantine_approved_for_live",
            "ok": bool(candidate_quarantine.get("ok")) if source_is_step2 else True,
            "detail": {
                "candidate_id": candidate_quarantine.get("candidate_id"),
                "failed_checks": candidate_quarantine.get("failed_checks", [])[:10],
                "error": candidate_quarantine.get("error"),
            } if source_is_step2 else "not_step2_payload",
            "skipped": not source_is_step2,
        },
    ]
    packet = {
        "schema_version": SCHEMA_VERSION,
        "source": "promotion_evidence_packet",
        "created_at_ct": datetime.now(CT).isoformat(timespec="seconds"),
        "ok": all(bool(c.get("ok")) for c in checks),
        "candidate": {
            **standard,
            "model_id": model_id,
            "family_id": standard.get("family_id") or tournament_safety.variant_family(standard.get("weights") or {}),
            "profile_hash": profile_hash,
        },
        "checks": checks,
        "contracts": contracts,
        "contract_gate": {
            "ok": gate.get("ok"),
            "critical_failure_count": gate.get("critical_failure_count"),
            "hashes": gate.get("hashes"),
        },
        "days": days,
        "step2_scores": step2_scores,
        "canonical_decision_packets": canonical_packets,
        "parity_verdicts": parity_verdicts,
        "market_data_integrity": market_data_integrity,
        "artifact_registries": artifact_registries,
        "latency_model": latency_model,
        "step2_evaluation_envelope": source_envelope if source_is_step2 else {},
        "baseline_drift": baseline_drift,
        "candidate_reproducibility": candidate_reproducibility,
        "candidate_quarantine": candidate_quarantine,
        "candidate_robustness": candidate_robustness,
        "shadow_leaderboards": _shadow_leaderboards(days),
        "promotion_registry": registry,
        "candidate_lifecycle": {
            "candidate_id": lifecycle_record.get("candidate_id"),
            "status": lifecycle_record.get("status"),
            "registry_path": str(candidate_lifecycle.REGISTRY_PATH.resolve()),
            "error": lifecycle_record.get("error"),
        },
        "deduction": (
            "This packet is promotion evidence, not a new score. It records which artifacts and contracts "
            "existed when a candidate was reviewed or promoted."
        ),
    }
    packet["packet_hash"] = _stable_hash({k: v for k, v in packet.items() if k not in {"packet_hash", "output"}})
    if write:
        stem = f"{_stamp()}_{_slug(str(standard.get('name') or standard.get('variant') or 'candidate'))}"
        json_path = OUT_DIR / f"{stem}.json"
        txt_path = OUT_DIR / f"{stem}.txt"
        packet["output"] = {
            "json_path": _write_json(json_path, packet),
            "txt_path": _write_text(txt_path, render_text(packet)),
        }
        _write_json(json_path, packet)
    return packet


def main() -> int:
    ap = argparse.ArgumentParser(description="Build a promotion evidence packet.")
    ap.add_argument("--candidate-json", default="")
    ap.add_argument("--rank", type=int, default=1)
    ap.add_argument("--variant", default="")
    ap.add_argument("--days", nargs="*", default=[])
    ap.add_argument("--promotion-registry-path", default="")
    ap.add_argument("--no-write", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    packet = build(
        candidate_json=args.candidate_json,
        rank=args.rank,
        variant=args.variant,
        days=args.days,
        promotion_registry_path=args.promotion_registry_path,
        write=not args.no_write,
    )
    print(json.dumps(packet, indent=2, sort_keys=True, default=str))
    return 0 if packet.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
