"""Single-page promotion decision brief for Step 2 candidates.

This is a synthesis layer. It does not approve, promote, block trading, or
mutate candidate state. It pulls the existing Step 2 score, robustness report,
contract hashes, reproducibility check, and quarantine registry status into one
plain-English review artifact.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import baseline_drift_sentinel
import candidate_profile_schema
import candidate_reproducibility_gate
import candidate_robustness_report
import promotion_candidate_quarantine
import step2_execution_contract
import step2_parity_contract
import tournament_safety


HERE = Path(__file__).resolve().parent
OUT_DIR = HERE / "postmortem" / "candidate_decision_briefs"
CONFIG_PATH = HERE / "trading_config.json"
CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


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


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except Exception:
        return default


def _config() -> dict[str, Any]:
    payload = _read_json(CONFIG_PATH, {}) or {}
    return payload if isinstance(payload, dict) else {}


def _row_id(row: dict[str, Any]) -> str:
    routes = row.get("routes") if isinstance(row.get("routes"), list) else []
    if routes:
        return str(row.get("model_id") or tournament_safety.stable_json_hash({
            "name": str(row.get("variant") or row.get("name") or "candidate"),
            "weights": row.get("weights") or {},
            "bias": round(float(row.get("bias") or 0.0), 8),
            "routes": routes,
        }, length=20))
    return str(row.get("model_id") or tournament_safety.model_id(
        str(row.get("variant") or row.get("name") or "candidate"),
        row.get("weights") or {},
        float(row.get("bias") or 0.0),
    ))


def _dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("weights"), dict):
            continue
        key = _row_id(row)
        old = best.get(key)
        if old is None or candidate_profile_schema.score(row) > candidate_profile_schema.score(old):
            best[key] = row
    return list(best.values())


def _select_row(payload: dict[str, Any], rank: int = 1, variant: str = "") -> dict[str, Any]:
    if variant:
        for row in _dedupe_rows(candidate_profile_schema.rows_from_payload(payload)):
            if str(row.get("variant") or row.get("name") or "") == variant:
                return row
        return candidate_profile_schema.select(payload, rank=rank, variant=variant)
    return candidate_profile_schema.select(payload, rank=rank)


def _rankings(payload: dict[str, Any], selected: dict[str, Any],
              active_row: dict[str, Any], start_balance: float) -> dict[str, Any]:
    selected_id = _row_id(selected)
    rows = _dedupe_rows(candidate_profile_schema.rows_from_payload(payload))
    raw_rows = sorted(rows, key=candidate_profile_schema.score, reverse=True)
    reports = [
        candidate_robustness_report.evaluate_candidate(
            row,
            active_row=active_row,
            start_balance=start_balance,
            rank=idx + 1,
        )
        for idx, row in enumerate(raw_rows)
    ]
    adjusted = sorted(
        reports,
        key=lambda item: (_num(item.get("robustness_adjusted_score")), _num(item.get("step2_pnl"))),
        reverse=True,
    )
    recommended = [item for item in adjusted if item.get("recommended")]
    raw_rank = next((idx + 1 for idx, row in enumerate(raw_rows) if _row_id(row) == selected_id), None)
    adjusted_rank = next((idx + 1 for idx, report in enumerate(adjusted) if report.get("model_id") == selected_id), None)
    recommended_rank = next((idx + 1 for idx, report in enumerate(recommended) if report.get("model_id") == selected_id), None)
    return {
        "raw_rank": raw_rank,
        "robustness_adjusted_rank": adjusted_rank,
        "recommended_rank": recommended_rank,
        "row_count": len(raw_rows),
        "top_raw_variant": raw_rows[0].get("variant") if raw_rows else None,
        "top_adjusted_variant": adjusted[0].get("variant") if adjusted else None,
        "best_recommended_variant": recommended[0].get("variant") if recommended else None,
        "recommended_count": len(recommended),
    }


def _artifact_contracts(payload: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
    row_contracts = row.get("contracts") if isinstance(row.get("contracts"), dict) else {}
    payload_contracts = payload.get("candidate_contracts") if isinstance(payload.get("candidate_contracts"), dict) else {}
    candidates = [row_contracts, payload_contracts, row, payload]

    def first(key: str) -> Any:
        for source in candidates:
            if isinstance(source, dict) and source.get(key):
                return source.get(key)
        return None

    cfg = _config()
    live_parity = step2_parity_contract.contract_hash(step2_parity_contract.contract(cfg))
    live_exec = step2_execution_contract.execution_contract_hash(cfg)
    artifact_parity = first("step2_parity_contract_hash")
    artifact_exec = first("step2_execution_contract_hash")
    checks = [
        {
            "name": "step2_parity_contract_hash_matches_live",
            "ok": bool(artifact_parity and artifact_parity == live_parity),
            "actual": artifact_parity,
            "expected": live_parity,
        },
        {
            "name": "step2_execution_contract_hash_matches_live",
            "ok": bool(artifact_exec and artifact_exec == live_exec),
            "actual": artifact_exec,
            "expected": live_exec,
        },
    ]
    return {
        "ok": all(check["ok"] for check in checks),
        "checks": checks,
        "artifact_step2_parity_contract_hash": artifact_parity,
        "live_step2_parity_contract_hash": live_parity,
        "artifact_step2_execution_contract_hash": artifact_exec,
        "live_step2_execution_contract_hash": live_exec,
    }


def _baseline_drift(payload: dict[str, Any]) -> dict[str, Any]:
    if not baseline_drift_sentinel.source_looks_step2(payload):
        return {"ok": True, "status": "skipped", "reason": "not_step2_payload"}
    try:
        return baseline_drift_sentinel.evaluate_payload(payload)
    except Exception as exc:
        return {"ok": False, "status": "error", "error": repr(exc)}


def _reproducibility(payload: dict[str, Any], row: dict[str, Any], *,
                     candidate_json: str, rank: int, variant: str,
                     run: bool) -> dict[str, Any]:
    if not run:
        return {"ok": None, "skipped": True, "reason": "disabled"}
    if not baseline_drift_sentinel.source_looks_step2(payload):
        return {"ok": None, "skipped": True, "reason": "not_step2_payload"}
    try:
        report = candidate_reproducibility_gate.evaluate_payload(
            payload,
            row,
            candidate_json=candidate_json,
            rank=rank,
            variant=variant,
        )
        return {
            "ok": bool(report.get("ok")),
            "error": report.get("error"),
            "failed_count": len(report.get("failed_checks") or []),
            "failed_checks": (report.get("failed_checks") or [])[:8],
            "expected": report.get("expected"),
            "reproduced": report.get("reproduced"),
            "compiled_manifest_path": report.get("compiled_manifest_path"),
            "deduction": report.get("deduction"),
        }
    except Exception as exc:
        return {"ok": False, "error": repr(exc), "failed_count": 1, "failed_checks": []}


def _quarantine_status(row: dict[str, Any], candidate_json: str, run: bool) -> dict[str, Any]:
    if not run:
        return {"ok": None, "skipped": True, "reason": "disabled"}
    try:
        registry = promotion_candidate_quarantine.load_registry()
        cid = promotion_candidate_quarantine.candidate_id(row)
        record = (registry.get("records") or {}).get(cid) or {}
        approval = record.get("approval") if isinstance(record.get("approval"), dict) else {}
        baseline_hash = promotion_candidate_quarantine.current_baseline_hash()
        artifact_hash = tournament_safety._file_sha256(candidate_json) if candidate_json and os.path.exists(candidate_json) else ""
        checks = [
            {"name": "quarantine_record_present", "ok": bool(record), "actual": bool(record)},
            {
                "name": "quarantine_status_approved_for_live",
                "ok": record.get("status") == "approved_for_live",
                "actual": record.get("status"),
                "expected": "approved_for_live",
            },
            {"name": "quarantine_evidence_key_ok", "ok": bool(record.get("evidence_key_ok")), "actual": record.get("evidence_key_ok")},
            {"name": "approval_key_present", "ok": bool(approval), "actual": bool(approval)},
            {
                "name": "approval_baseline_matches_live",
                "ok": bool(approval and approval.get("approved_baseline_profile_hash") == baseline_hash),
                "actual": approval.get("approved_baseline_profile_hash"),
                "expected": baseline_hash,
            },
            {
                "name": "artifact_hash_matches_approval_record",
                "ok": bool(record and artifact_hash and record.get("artifact_hash") == artifact_hash),
                "actual": record.get("artifact_hash"),
                "expected": artifact_hash,
            },
        ]
        return {
            "ok": all(check["ok"] for check in checks),
            "candidate_id": cid,
            "record_status": record.get("status"),
            "approval_present": bool(approval),
            "checks": checks,
            "failed_checks": [check for check in checks if not check.get("ok")],
            "read_only": True,
        }
    except Exception as exc:
        return {"ok": False, "error": repr(exc), "read_only": True}


def _metric_summary(row: dict[str, Any], robustness: dict[str, Any]) -> dict[str, Any]:
    step2 = row.get("step2") if isinstance(row.get("step2"), dict) else {}
    pnl = candidate_profile_schema.score(row)
    if pnl == float("-inf"):
        pnl = _num(row.get("step2_pnl"), _num(step2.get("pnl")))
    active_pnl = robustness.get("active_step2_pnl")
    delta = robustness.get("delta_vs_active")
    delta_pct = robustness.get("delta_pct_vs_active")
    return {
        "step2_pnl": round(float(pnl), 6),
        "active_step2_pnl": active_pnl,
        "delta_vs_active": delta,
        "delta_pct_vs_active": delta_pct,
        "trades": row.get("step2_trades") or step2.get("trades") or robustness.get("trades"),
        "win_rate_pct": row.get("step2_win_rate_pct") or step2.get("win_rate_pct") or robustness.get("win_rate_pct"),
        "holdout": robustness.get("train_holdout"),
        "worst_day": (robustness.get("day_profile") or {}).get("worst_day"),
        "best_day": (robustness.get("day_profile") or {}).get("best_day"),
        "day_concentration": ((robustness.get("day_profile") or {}).get("concentration") or {}),
        "ticker_concentration": ((robustness.get("ticker_profile") or {}).get("concentration") or {}),
        "trade_sample": robustness.get("trade_sample"),
    }


def _decision(metrics: dict[str, Any], robustness: dict[str, Any],
              repro: dict[str, Any], quarantine: dict[str, Any],
              contracts: dict[str, Any], drift: dict[str, Any],
              payload: dict[str, Any]) -> dict[str, Any]:
    reject_reasons: list[str] = []
    promotion_blockers: list[str] = []
    warnings: list[str] = []

    recommendation = robustness.get("recommendation") if isinstance(robustness.get("recommendation"), dict) else {}
    if metrics.get("delta_vs_active") is not None and _num(metrics.get("delta_vs_active")) <= 0:
        reject_reasons.append("does_not_beat_active_step2")
    if recommendation.get("blockers"):
        reject_reasons.extend(str(item) for item in recommendation.get("blockers") or [])
    if payload.get("promotable") is False:
        promotion_blockers.append(str(payload.get("non_promotable_reason") or "source_artifact_not_promotable"))
    if contracts.get("ok") is False:
        promotion_blockers.append("contract_hash_mismatch")
    if drift.get("status") not in (None, "current", "skipped") and drift.get("ok") is not True:
        promotion_blockers.append(f"baseline_drift_{drift.get('status')}")
    if repro.get("ok") is False:
        promotion_blockers.append("candidate_reproducibility_failed")
    elif repro.get("skipped"):
        warnings.append(f"reproducibility_{repro.get('reason')}")
    if quarantine.get("ok") is not True:
        warnings.append("quarantine_not_approved_yet")

    reject_reasons = sorted(set(reject_reasons))
    promotion_blockers = sorted(set(promotion_blockers))
    warnings = sorted(set(warnings))
    analytically_recommended = bool(robustness.get("recommended")) and not reject_reasons
    if reject_reasons:
        label = "REJECT"
    elif analytically_recommended and not promotion_blockers and quarantine.get("ok") is True:
        label = "PROMOTE"
    else:
        label = "REVIEW"
    confidence = "high" if label == "PROMOTE" else ("medium" if analytically_recommended else "low")
    return {
        "label": label,
        "confidence": confidence,
        "analytically_recommended": analytically_recommended,
        "promotion_ready": label == "PROMOTE",
        "reject_reasons": reject_reasons,
        "reject_reason_details": _reject_reason_details(reject_reasons, metrics, robustness),
        "promotion_blockers": promotion_blockers,
        "warnings": warnings,
        "deduction": (
            "PROMOTE requires a robust candidate, clean contracts/reproducibility, and an approved "
            "quarantine record. REVIEW means the candidate may be good but is missing evidence or "
            "manual approval. REJECT means the candidate has analytical robustness blockers."
        ),
    }


def _reject_reason_details(
    reasons: list[str],
    metrics: dict[str, Any],
    robustness: dict[str, Any],
) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    recommendation = robustness.get("recommendation") if isinstance(robustness.get("recommendation"), dict) else {}
    for reason in reasons:
        item: dict[str, Any] = {"reason": reason}
        if reason == "does_not_beat_active_step2":
            item.update({
                "message": "Candidate does not beat the current Live Step 2 baseline.",
                "delta_vs_active": metrics.get("delta_vs_active"),
                "candidate_pnl": metrics.get("step2_pnl"),
                "active_pnl": metrics.get("active_step2_pnl"),
            })
        elif reason == "robustness_score_below_70":
            diagnosis = _robustness_failure_diagnosis(metrics, robustness)
            item.update({
                "message": diagnosis.get("summary") or "Robustness score is below the promotion review threshold.",
                "robustness_score": robustness.get("robustness_score"),
                "robustness_adjusted_score": robustness.get("robustness_adjusted_score"),
                "threshold": 70,
                "grade": robustness.get("robustness_grade"),
                "tier": recommendation.get("tier"),
                "main_issues": diagnosis.get("main_issues") or [],
                "component_breakdown": diagnosis.get("component_breakdown") or [],
            })
        else:
            item["message"] = reason.replace("_", " ")
        details.append(item)
    return details


def _money(value: Any) -> str:
    try:
        return f"${float(value):,.2f}"
    except Exception:
        return "n/a"


def _pct(value: Any) -> str:
    try:
        return f"{float(value):+.2f}%"
    except Exception:
        return "n/a"


def _component_breakdown(robustness: dict[str, Any]) -> list[dict[str, Any]]:
    max_points = {
        "total_delta": 20.0,
        "day_consistency": 25.0,
        "holdout": 20.0,
        "ticker_balance": 10.0,
        "side_balance": 10.0,
        "drawdown_control": 10.0,
        "trade_sample": 5.0,
    }
    labels = {
        "total_delta": "Overall edge vs Live",
        "day_consistency": "Day-by-day consistency",
        "holdout": "Recent holdout strength",
        "ticker_balance": "Ticker breadth",
        "side_balance": "Long/short breadth",
        "drawdown_control": "Drawdown control",
        "trade_sample": "Trade sample size",
    }
    components = robustness.get("components") if isinstance(robustness.get("components"), dict) else {}
    rows = []
    for key, maximum in max_points.items():
        earned = _num(components.get(key))
        rows.append({
            "component": key,
            "label": labels[key],
            "earned": round(earned, 4),
            "max": maximum,
            "lost": round(maximum - earned, 4),
        })
    return sorted(rows, key=lambda item: item["lost"], reverse=True)


def _robustness_failure_diagnosis(metrics: dict[str, Any], robustness: dict[str, Any]) -> dict[str, Any]:
    breakdown = _component_breakdown(robustness)
    main = [row for row in breakdown if row["lost"] >= 5.0][:3]
    holdout = robustness.get("train_holdout") if isinstance(robustness.get("train_holdout"), dict) else {}
    day_profile = robustness.get("day_profile") if isinstance(robustness.get("day_profile"), dict) else {}
    total_days = day_profile.get("total_days")
    beats_days = day_profile.get("beats_active_days")
    holdout_delta = holdout.get("holdout_delta_vs_active")
    active_holdout = holdout.get("active_holdout_pnl")
    holdout_days = holdout.get("holdout_days") or []

    issues: list[str] = []
    component_by_name = {row["component"]: row for row in breakdown}
    total_delta_row = component_by_name.get("total_delta") or {}
    holdout_row = component_by_name.get("holdout") or {}
    day_row = component_by_name.get("day_consistency") or {}

    if holdout_row and holdout_row.get("lost", 0.0) >= 5.0:
        issues.append(
            f"Recent holdout edge is too thin: the last {len(holdout_days)} day(s) beat Live by "
            f"{_money(holdout_delta)} on a Live holdout base of {_money(active_holdout)}, earning "
            f"{holdout_row.get('earned')}/{holdout_row.get('max')} holdout points."
        )
    if total_delta_row and total_delta_row.get("lost", 0.0) >= 5.0:
        issues.append(
            f"Total edge is small for promotion: overall delta is {_money(metrics.get('delta_vs_active'))} "
            f"({_pct(metrics.get('delta_pct_vs_active'))}), earning {total_delta_row.get('earned')}/"
            f"{total_delta_row.get('max')} edge points."
        )
    if day_row and day_row.get("lost", 0.0) >= 5.0 and total_days:
        issues.append(
            f"Day consistency is good but not perfect: it beat Live on {beats_days}/{total_days} days, "
            f"earning {day_row.get('earned')}/{day_row.get('max')} day-consistency points."
        )
    if not issues and main:
        issues = [
            f"{row['label']} lost {row['lost']} point(s), earning {row['earned']}/{row['max']}."
            for row in main
        ]

    summary = (
        "Robustness is below threshold mainly because the candidate's edge is too thin in the recent "
        "holdout and overall delta, even though route safety, drawdown, ticker/side breadth, and trade "
        "sample are acceptable."
    )
    if issues:
        summary = " ".join(issues)
    return {
        "summary": summary,
        "main_issues": issues,
        "component_breakdown": breakdown,
    }


def _plain_english(candidate: dict[str, Any], metrics: dict[str, Any],
                   robustness: dict[str, Any], decision: dict[str, Any]) -> str:
    name = candidate.get("variant") or candidate.get("name") or "Candidate"
    delta_pct = metrics.get("delta_pct_vs_active")
    delta_text = f"beats Live by {float(delta_pct):+.2f}%" if delta_pct is not None else "has no Live delta available"
    score = robustness.get("robustness_score")
    adjusted = robustness.get("robustness_adjusted_score")
    flags = robustness.get("red_flags") or []
    if decision.get("label") == "REJECT":
        details = decision.get("reject_reason_details") or []
        digest = ""
        if details and isinstance(details[0], dict):
            digest = str(details[0].get("message") or "")
        reason = digest or ", ".join(decision.get("reject_reasons") or flags or ["analytical blockers"])
        return f"{name} {delta_text}, but is rejected: {reason} Do not promote without more data or changes."
    if decision.get("label") == "PROMOTE":
        return f"{name} {delta_text}, has robustness {score} adjusted {adjusted}, clean evidence, and approval is present. Promotion-ready."
    blockers = decision.get("promotion_blockers") or decision.get("warnings") or ["manual review required"]
    return f"{name} {delta_text}, has robustness {score} adjusted {adjusted}. Recommended for review; remaining item(s): {', '.join(blockers)}."


def build(
    *,
    payload: dict[str, Any],
    rank: int = 1,
    variant: str = "",
    candidate_json: str = "",
    run_reproducibility: bool = True,
    run_quarantine: bool = True,
) -> dict[str, Any]:
    row = _select_row(payload, rank=rank, variant=variant)
    active = payload.get("active") if isinstance(payload.get("active"), dict) else {}
    start_balance = _num(payload.get("start_balance"), 100000.0)
    robustness = candidate_robustness_report.evaluate_candidate(
        row,
        active_row=active,
        start_balance=start_balance,
        rank=rank,
    )
    metrics = _metric_summary(row, robustness)
    rankings = _rankings(payload, row, active, start_balance)
    contracts = _artifact_contracts(payload, row)
    drift = _baseline_drift(payload)
    repro = _reproducibility(
        payload,
        row,
        candidate_json=candidate_json,
        rank=rank,
        variant=variant,
        run=run_reproducibility,
    )
    quarantine = _quarantine_status(row, candidate_json, run_quarantine)
    standard_row = dict(row)
    reproduced_summary = repro.get("reproduced") if isinstance(repro.get("reproduced"), dict) else {}
    if (
        isinstance(standard_row.get("routes"), list)
        and standard_row.get("routes")
        and not standard_row.get("route_audit")
        and isinstance(reproduced_summary.get("route_audit"), dict)
    ):
        standard_row["route_audit"] = reproduced_summary["route_audit"]
    standard = candidate_profile_schema.normalize(
        standard_row,
        context={"rank": rank, "artifact": candidate_json, "script": "candidate_decision_brief.py"},
        source_payload=payload,
    )
    decision = _decision(metrics, robustness, repro, quarantine, contracts, drift, payload)
    checks = [
        {"name": "candidate_has_weights", "ok": bool(row.get("weights"))},
        {"name": "candidate_beats_active_step2", "ok": _num(metrics.get("delta_vs_active")) > 0.0, "actual": metrics.get("delta_vs_active")},
        {"name": "candidate_robustness_recommended", "ok": bool(robustness.get("recommended")), "actual": robustness.get("recommendation")},
        {"name": "contracts_current", "ok": bool(contracts.get("ok")), "actual": contracts.get("checks")},
        {"name": "baseline_drift_current_or_skipped", "ok": drift.get("status") in ("current", "skipped") or drift.get("ok") is True, "actual": drift.get("status")},
        {"name": "reproducibility_ok_or_reviewable", "ok": repro.get("ok") is not False, "actual": repro.get("ok"), "skipped": repro.get("skipped")},
        {"name": "quarantine_approved_for_live", "ok": quarantine.get("ok") is True, "actual": quarantine.get("record_status"), "warning_only": True},
    ]
    brief = {
        "schema_version": SCHEMA_VERSION,
        "source": "candidate_decision_brief",
        "created_at_ct": _now_ct(),
        "candidate_json": os.path.abspath(candidate_json) if candidate_json else "",
        "selection": {
            "rank": rank,
            "variant": variant or row.get("variant") or row.get("name"),
            "model_id": standard.get("model_id"),
        },
        "decision": decision,
        "promotion_blockers": decision.get("promotion_blockers") or [],
        "reject_reasons": decision.get("reject_reasons") or [],
        "plain_english": _plain_english(standard, metrics, robustness, decision),
        "candidate": standard,
        "metrics": metrics,
        "rankings": rankings,
        "robustness": {
            "score": robustness.get("robustness_score"),
            "adjusted_score": robustness.get("robustness_adjusted_score"),
            "grade": robustness.get("robustness_grade"),
            "recommended": robustness.get("recommended"),
            "recommendation": robustness.get("recommendation"),
            "red_flags": robustness.get("red_flags"),
            "components": robustness.get("components"),
            "train_holdout": robustness.get("train_holdout"),
            "drawdown": robustness.get("drawdown"),
        },
        "evidence": {
            "reproducibility": repro,
            "quarantine": quarantine,
            "contracts": contracts,
            "baseline_drift": drift,
            "compiled_lineage_validation": payload.get("compiled_lineage_validation"),
            "quote_aware_guard": payload.get("quote_aware_guard") or payload.get("quote_aware_outcome_gate"),
            "score_only_contract": payload.get("score_only_contract"),
            "score_cache": payload.get("score_cache") or payload.get("score_cache_observed"),
            "frontier_report": payload.get("frontier_report"),
            "selection_explanation": payload.get("selection_explanation"),
            "risk_concentration_report": payload.get("risk_concentration_report"),
            "toxic_addon_report": payload.get("toxic_addon_report"),
            "route_marginal_report": payload.get("route_marginal_report"),
            "route_learning_registry": payload.get("route_learning_registry"),
            "adaptive_gate_manifest": payload.get("adaptive_gate_manifest"),
        },
        "checks": checks,
        "report_only": True,
        "deduction": (
            "This brief is an operator review artifact. It does not approve, promote, block, "
            "or mutate live trading state."
        ),
    }
    brief["brief_hash"] = _stable_hash({k: v for k, v in brief.items() if k not in {"brief_hash", "output_path"}})
    return brief


def build_from_file(
    candidate_json: str,
    *,
    rank: int = 1,
    variant: str = "",
    run_reproducibility: bool = True,
    run_quarantine: bool = True,
) -> dict[str, Any]:
    payload = _read_json(candidate_json, {}) or {}
    if not isinstance(payload, dict):
        raise RuntimeError(f"candidate artifact must be a JSON object: {candidate_json}")
    return build(
        payload=payload,
        rank=rank,
        variant=variant,
        candidate_json=str(Path(candidate_json).resolve()),
        run_reproducibility=run_reproducibility,
        run_quarantine=run_quarantine,
    )


def write_report(payload: dict[str, Any], path: str | os.PathLike[str] | None = None) -> str:
    if path is None:
        selected = (payload.get("selection") or {}).get("variant") or "candidate"
        safe = "".join(ch if ch.isalnum() or ch in "._+-" else "_" for ch in str(selected))[:96] or "candidate"
        path = OUT_DIR / f"{datetime.now(CT).strftime('%Y%m%d_%H%M%S')}_{safe}.json"
    out = _write_json(Path(path), payload)
    payload["output_path"] = out
    _write_json(Path(out), payload)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Build a promotion decision brief for a Step 2 candidate.")
    ap.add_argument("--candidate-json", required=True)
    ap.add_argument("--rank", type=int, default=1)
    ap.add_argument("--variant", default="")
    ap.add_argument("--out", default="")
    ap.add_argument("--no-reproducibility", action="store_true")
    ap.add_argument("--no-quarantine", action="store_true")
    ap.add_argument("--no-write", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    brief = build_from_file(
        args.candidate_json,
        rank=args.rank,
        variant=args.variant,
        run_reproducibility=not args.no_reproducibility,
        run_quarantine=not args.no_quarantine,
    )
    if not args.no_write:
        write_report(brief, args.out or None)
    if args.json:
        print(json.dumps(brief, indent=2, sort_keys=True, default=str))
    else:
        print(json.dumps({
            "decision": (brief.get("decision") or {}).get("label"),
            "reject_reasons": (brief.get("decision") or {}).get("reject_reasons") or [],
            "reject_reason_details": (brief.get("decision") or {}).get("reject_reason_details") or [],
            "promotion_blockers": (brief.get("decision") or {}).get("promotion_blockers") or [],
            "warnings": (brief.get("decision") or {}).get("warnings") or [],
            "variant": (brief.get("selection") or {}).get("variant"),
            "step2_pnl": (brief.get("metrics") or {}).get("step2_pnl"),
            "delta_pct_vs_active": (brief.get("metrics") or {}).get("delta_pct_vs_active"),
            "robustness_adjusted_score": ((brief.get("robustness") or {}).get("adjusted_score")),
            "plain_english": brief.get("plain_english"),
            "output_path": brief.get("output_path"),
        }, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
