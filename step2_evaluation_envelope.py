from __future__ import annotations

import argparse
import copy
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import active_engine_baseline
import candidate_profile_schema
import compiled_tape_lineage
import step2_latency_model
import step2_parity_contract
import tournament_safety
import worker_policy
from zoneinfo import ZoneInfo


CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1
HERE = Path(__file__).resolve().parent
OUT_DIR = HERE / "postmortem" / "step2_evaluation_envelopes"
CONFIG_PATH = HERE / "trading_config.json"


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


def _read_json(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    return payload if isinstance(payload, dict) else {}


def _stable_hash(payload: Any) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def _without_hash(payload: dict[str, Any]) -> dict[str, Any]:
    clone = copy.deepcopy(payload)
    clone.pop("envelope_hash", None)
    clone.pop("output_path", None)
    return clone


def _file_sha256(path: str | Path | None) -> str:
    if not path:
        return ""
    try:
        return tournament_safety._file_sha256(Path(path)) or ""
    except Exception:
        return ""


def _file_meta(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {"path": "", "exists": False, "sha256": ""}
    p = Path(path)
    return {
        "path": str(p),
        "exists": p.exists(),
        "sha256": _file_sha256(p) if p.exists() else "",
    }


def _simple_args(args: Any) -> dict[str, Any]:
    if args is None:
        return {}
    raw = vars(args) if not isinstance(args, dict) else dict(args)
    allowed = {
        "batch_size",
        "max_batches",
        "target_count",
        "target_beats_live_pct",
        "beat_pct",
        "seed",
        "start_balance",
        "max_workers",
        "weight_limit",
        "max_trades_per_day",
        "max_trades_per_ticker_day",
        "compiled_decision_tape",
        "skip_data_integrity_gate",
        "refresh_data_integrity",
        "allow_data_integrity_fail",
    }
    result: dict[str, Any] = {}
    for key, value in raw.items():
        if key not in allowed:
            continue
        if isinstance(value, (str, int, float, bool)) or value is None:
            result[key] = value
        elif isinstance(value, (list, tuple)):
            result[key] = list(value)
        elif isinstance(value, dict):
            result[key] = value
        else:
            result[key] = str(value)
    return result


def _manifest_from_compiled(
    compiled: dict[str, Any] | None,
    compiled_manifest_path: str | Path | None,
) -> dict[str, Any]:
    if isinstance(compiled, dict) and isinstance(compiled.get("manifest"), dict):
        return dict(compiled["manifest"])
    if compiled_manifest_path and Path(compiled_manifest_path).exists():
        return _read_json(compiled_manifest_path)
    return {}


def _source_days_from_manifest(manifest: dict[str, Any]) -> list[str]:
    day_map = manifest.get("day_map")
    if isinstance(day_map, dict):
        return sorted(str(day) for day in day_map)
    days = manifest.get("source_days")
    if isinstance(days, list):
        return sorted(str(day) for day in days)
    return []


def _compiled_tape_summary(
    compiled: dict[str, Any] | None,
    compiled_manifest_path: str | Path | None,
) -> dict[str, Any]:
    manifest = _manifest_from_compiled(compiled, compiled_manifest_path)
    manifest_path = str(compiled_manifest_path or manifest.get("manifest_path") or "")
    source_days = _source_days_from_manifest(manifest)
    source_paths = manifest.get("source_paths") if isinstance(manifest.get("source_paths"), dict) else {}
    source_hashes = manifest.get("source_hashes") if isinstance(manifest.get("source_hashes"), dict) else {}
    lineage = manifest.get("lineage_validation") if isinstance(manifest.get("lineage_validation"), dict) else {}
    if not lineage and manifest:
        manifest_for_lineage = dict(manifest)
        if manifest_path:
            manifest_for_lineage["manifest_path"] = manifest_path
        try:
            lineage = compiled_tape_lineage.evaluate_manifest(
                manifest_for_lineage,
                code_hash_inputs=compiled_tape_lineage.DEFAULT_COMPILED_TAPE_CODE_HASH_INPUTS,
            )
        except Exception as exc:
            lineage = {
                "status": "LINEAGE_EVALUATION_FAILED",
                "certified": False,
                "quick_score_allowed": False,
                "rebuild_required": True,
                "recommended_action": "full_signal_rebuild",
                "error": repr(exc),
            }
    return {
        "manifest": _file_meta(manifest_path),
        "compiled_tape_hash": str(manifest.get("compiled_tape_hash") or ""),
        "arrays_sha256": str(manifest.get("arrays_sha256") or manifest.get("array_payload_hash") or ""),
        "arrays_path": str(manifest.get("arrays_path") or ""),
        "source_days": source_days,
        "row_count": manifest.get("row_count") or manifest.get("rows"),
        "min_ts": manifest.get("min_ts") or manifest.get("first_ts"),
        "max_ts": manifest.get("max_ts") or manifest.get("last_ts"),
        "indicator_mode": manifest.get("indicator_mode"),
        "source_paths": source_paths,
        "source_hashes": source_hashes,
        "step2_execution_contract_hash": str(manifest.get("step2_execution_contract_hash") or ""),
        "semantic_hashes": manifest.get("semantic_hashes") if isinstance(manifest.get("semantic_hashes"), dict) else {},
        "lineage_validation": lineage,
        "lineage_status": lineage.get("status"),
        "lineage_certified": lineage.get("certified"),
        "lineage_quick_score_allowed": lineage.get("quick_score_allowed"),
        "lineage_rebuild_required": lineage.get("rebuild_required"),
        "lineage_recommended_action": lineage.get("recommended_action"),
    }


def _safe_count(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def _market_data_summary(data_integrity: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(data_integrity, dict):
        return {
            "checked": False,
            "promotion_safe": False,
            "critical_count": 0,
            "warning_count": 0,
            "reports": [],
        }

    reports: list[dict[str, Any]] = []
    raw_reports = data_integrity.get("reports")
    if isinstance(raw_reports, dict):
        iterator = raw_reports.items()
    elif isinstance(raw_reports, list):
        iterator = enumerate(raw_reports)
    else:
        iterator = []

    for key, report in iterator:
        if not isinstance(report, dict):
            continue
        report_path = report.get("output_path") or report.get("path") or ""
        reports.append(
            {
                "day": str(report.get("day") or key),
                "verdict": report.get("verdict"),
                "promotion_safe": bool(report.get("promotion_safe", False)),
                "critical_count": _safe_count(report.get("critical_count")),
                "warning_count": _safe_count(report.get("warning_count")),
                "source": report.get("source"),
                "source_used": report.get("source_used"),
                "source_tape_hash": report.get("source_tape_hash"),
                "path": str(report_path),
                "report_sha256": _file_sha256(report_path) if report_path else "",
            }
        )

    if reports:
        promotion_safe = all(item.get("promotion_safe") for item in reports)
        critical_count = sum(_safe_count(item.get("critical_count")) for item in reports)
        warning_count = sum(_safe_count(item.get("warning_count")) for item in reports)
    else:
        promotion_safe = bool(data_integrity.get("promotion_safe", False))
        critical_count = _safe_count(data_integrity.get("critical_count"))
        warning_count = _safe_count(data_integrity.get("warning_count"))

    return {
        "checked": bool(data_integrity.get("checked", True)),
        "promotion_safe": promotion_safe,
        "critical_count": critical_count,
        "warning_count": warning_count,
        "aggregate_hash": _stable_hash(reports) if reports else "",
        "reports": reports,
    }


def _active_baseline_score(active_row: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(active_row, dict):
        return {}
    keys = (
        "variant_id",
        "pnl",
        "step2_pnl",
        "trades",
        "wins",
        "losses",
        "win_rate",
        "avg_pnl",
        "score",
    )
    return {key: active_row.get(key) for key in keys if key in active_row}


def _latency_model_summary() -> dict[str, Any]:
    path = step2_latency_model.DEFAULT_MODEL_PATH
    payload: dict[str, Any] = {}
    if Path(path).exists():
        try:
            payload = _read_json(path)
        except Exception:
            payload = {}
    return {
        "path": str(path),
        "exists": Path(path).exists(),
        "sha256": _file_sha256(path) if Path(path).exists() else "",
        "model_hash": _stable_hash(payload) if payload else "",
        "schema_version": payload.get("schema_version"),
        "created_at_ct": payload.get("created_at_ct"),
        "sample_count": payload.get("sample_count"),
        "source": payload.get("source"),
    }


def _contracts(config: Any | None = None) -> dict[str, Any]:
    if config is None:
        config = _read_json(CONFIG_PATH) if CONFIG_PATH.exists() else {}
    return candidate_profile_schema.contracts(config)


def _non_promotable_reasons(envelope: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    data = envelope.get("market_data_integrity") if isinstance(envelope.get("market_data_integrity"), dict) else {}
    if data and not data.get("promotion_safe", False):
        reasons.append("market_data_integrity_failed")
    compiled = envelope.get("compiled_decision_tape") if isinstance(envelope.get("compiled_decision_tape"), dict) else {}
    if not compiled.get("source_days"):
        reasons.append("source_days_missing")
    if not (compiled.get("compiled_tape_hash") or compiled.get("arrays_sha256")):
        reasons.append("compiled_tape_hash_missing")
    lineage = compiled.get("lineage_validation") if isinstance(compiled.get("lineage_validation"), dict) else {}
    if lineage:
        if lineage.get("status") != "CERTIFIED_MATCH" or lineage.get("certified") is not True:
            reasons.append("compiled_tape_not_certified")
        if lineage.get("status") == "UNSAFE_SCORE_DRIFT" or lineage.get("rebuild_required"):
            reasons.append("compiled_tape_unsafe_score_drift")
        if lineage.get("quick_score_allowed") is False:
            reasons.append("compiled_tape_quick_score_not_allowed")
    else:
        reasons.append("compiled_tape_lineage_missing")
    active = envelope.get("active_baseline") if isinstance(envelope.get("active_baseline"), dict) else {}
    if not (active.get("profile_hash") or (isinstance(active.get("profile"), dict) and active["profile"].get("hash"))):
        reasons.append("active_baseline_hash_missing")
    contracts = envelope.get("contracts") if isinstance(envelope.get("contracts"), dict) else {}
    current_exec_hash = contracts.get("step2_execution_contract_hash")
    if not current_exec_hash:
        reasons.append("step2_execution_contract_hash_missing")
    compiled_exec_hash = compiled.get("step2_execution_contract_hash")
    if compiled_exec_hash and current_exec_hash and compiled_exec_hash != current_exec_hash:
        reasons.append("compiled_execution_contract_hash_mismatch")
    if not contracts.get("step2_parity_contract_hash"):
        reasons.append("step2_parity_contract_hash_missing")
    cache_certification = envelope.get("cache_certification") if isinstance(envelope.get("cache_certification"), dict) else {}
    if cache_certification and cache_certification.get("ok") is not True:
        reasons.append("step2_cache_certification_failed")
    return sorted(set(reasons))


def build(
    *,
    compiled: dict[str, Any] | None = None,
    compiled_manifest_path: str | Path | None = None,
    args: Any | None = None,
    active_row: dict[str, Any] | None = None,
    data_integrity: dict[str, Any] | None = None,
    sim_config: dict[str, Any] | None = None,
    contracts: dict[str, Any] | None = None,
    run_context: dict[str, Any] | None = None,
    cache_certification: dict[str, Any] | None = None,
    name: str = "",
    write: bool = False,
) -> dict[str, Any]:
    active_payload = active_engine_baseline.active_profile_payload()
    profile = active_payload.get("profile") if isinstance(active_payload.get("profile"), dict) else {}
    compiled_summary = _compiled_tape_summary(compiled, compiled_manifest_path)
    contract_payload = contracts or _contracts()

    envelope: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "script": "step2_adaptive_hunter.py",
        "name": name,
        "created_at_ct": _now_ct(),
        "active_baseline": {
            **active_payload,
            "profile_hash": profile.get("hash", ""),
            "score": _active_baseline_score(active_row),
        },
        "baseline_profile_hash": profile.get("hash", ""),
        "compiled_decision_tape": compiled_summary,
        "source_days": compiled_summary.get("source_days", []),
        "contracts": {
            "step2_parity_contract_hash": contract_payload.get("step2_parity_contract_hash"),
            "step2_execution_contract_hash": contract_payload.get("step2_execution_contract_hash"),
            "step2_parity_contract_semantic_hash": contract_payload.get("step2_parity_contract_semantic_hash"),
            "step2_execution_contract_semantic_hash": contract_payload.get("step2_execution_contract_semantic_hash"),
        },
        "latency_model": _latency_model_summary(),
        "sim_config": sim_config or {},
        "run_args": _simple_args(args),
        "run_context": run_context or {},
        "cache_certification": cache_certification or {},
        "worker_policy": worker_policy.describe_policy(),
        "market_data_integrity": _market_data_summary(data_integrity),
    }
    reasons = _non_promotable_reasons(envelope)
    envelope["promotable"] = not reasons
    envelope["non_promotable_reasons"] = reasons
    envelope["envelope_hash"] = _stable_hash(_without_hash(envelope))

    if write:
        write_envelope(envelope, name=name)
    return envelope


def write_envelope(envelope: dict[str, Any], *, name: str = "") -> Path:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    suffix = name or envelope.get("name") or envelope.get("envelope_hash", "")[:12] or "step2"
    safe_suffix = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in str(suffix))
    path = OUT_DIR / f"step2_evaluation_envelope_{safe_suffix}.json"
    payload = dict(envelope)
    payload["output_path"] = str(path)
    payload["envelope_hash"] = _stable_hash(_without_hash(payload))
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    envelope.update(payload)
    return path


def attach(payload: dict[str, Any], envelope: dict[str, Any]) -> dict[str, Any]:
    payload["step2_evaluation_envelope"] = envelope
    payload["step2_evaluation_envelope_hash"] = envelope.get("envelope_hash", "")
    payload["baseline_profile_hash"] = envelope.get("baseline_profile_hash", "")

    existing_reasons = payload.get("non_promotable_reasons")
    if isinstance(existing_reasons, list):
        reasons = [str(item) for item in existing_reasons]
    else:
        reason = payload.get("non_promotable_reason")
        reasons = [str(reason)] if reason else []
    reasons.extend(str(item) for item in envelope.get("non_promotable_reasons", []) if item)
    reasons = sorted(set(reasons))

    if envelope.get("promotable") is False:
        payload["promotable"] = False
    elif "promotable" not in payload:
        payload["promotable"] = True
    if reasons:
        payload["non_promotable_reasons"] = reasons
        payload["non_promotable_reason"] = ",".join(reasons)
    return payload


def _main() -> int:
    parser = argparse.ArgumentParser(description="Build or inspect a Step 2 evaluation envelope.")
    parser.add_argument("--compiled-decision-tape", default="")
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--name", default="")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    compiled = None
    if args.compiled_decision_tape and Path(args.compiled_decision_tape).exists():
        compiled = {"manifest": _read_json(args.compiled_decision_tape)}
    envelope = build(compiled=compiled, compiled_manifest_path=args.compiled_decision_tape, args=args, write=args.write, name=args.name)
    if args.json:
        print(json.dumps(envelope, indent=2, sort_keys=True))
    else:
        print(
            f"promotable={envelope.get('promotable')} "
            f"hash={envelope.get('envelope_hash')} "
            f"baseline={envelope.get('baseline_profile_hash')}"
        )
    return 0 if envelope.get("promotable") else 2


if __name__ == "__main__":
    raise SystemExit(_main())
