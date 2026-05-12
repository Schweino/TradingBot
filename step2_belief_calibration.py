"""Belief calibration and outcome scoring for Step 2 learning artifacts."""
from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from typing import Any


CALIBRATION_VERSION = "step2_belief_calibration_v1"
PENDING_VALUES = {None, "", "pending", "pending_future_outcome", "unknown"}


def _stable_hash(payload: Any, length: int = 32) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length]


def clamp_probability(value: Any) -> float:
    try:
        prob = float(value)
    except Exception:
        return 0.5
    if prob < 0.0:
        return 0.0
    if prob > 1.0:
        return 1.0
    return prob


def split_belief_subject(belief: Any) -> tuple[str, str]:
    raw = str(belief or "")
    if ":" in raw:
        belief_type, subject = raw.split(":", 1)
        return belief_type or "unknown", subject or raw
    return "unknown", raw


def score_prediction(predicted_probability: Any, actual_numeric: Any) -> dict[str, Any]:
    predicted = clamp_probability(predicted_probability)
    actual = 1.0 if float(actual_numeric or 0.0) >= 0.5 else 0.0
    signed_error = predicted - actual
    return {
        "predicted_probability": round(predicted, 6),
        "actual_numeric": actual,
        "calibration_error": round(abs(signed_error), 6),
        "signed_error": round(signed_error, 6),
        "brier_score": round((predicted - actual) ** 2, 6),
        "confidence_bucket": probability_bucket(predicted),
        "confidence_bias": "overconfident" if signed_error > 0.05 else "underconfident" if signed_error < -0.05 else "well_calibrated",
    }


def probability_bucket(probability: Any) -> str:
    prob = clamp_probability(probability)
    low = int(prob * 10) * 10
    if low == 100:
        low = 90
    return f"{low:02d}_{low + 10:02d}"


def prediction_rows_from_beliefs(
    belief_update_report: dict[str, Any] | None,
    *,
    run_id: str = "",
    validation_rows: list[dict[str, Any]] | None = None,
    feedback_rows: list[dict[str, Any]] | None = None,
    command_outcomes: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    report = belief_update_report if isinstance(belief_update_report, dict) else {}
    predictions = []
    for row in report.get("beliefs") or report.get("updates") or []:
        belief = row.get("belief") or row.get("subject") or row.get("name")
        belief_type, subject = split_belief_subject(belief)
        probability = clamp_probability(row.get("posterior_confidence", row.get("predicted_probability", row.get("confidence", 0.5))))
        pred = {
            "schema_version": 1,
            "calibration_version": CALIBRATION_VERSION,
            "run_id": run_id,
            "belief": str(belief or subject),
            "belief_type": belief_type,
            "subject": subject,
            "prediction_type": f"{belief_type}_future_success_probability",
            "predicted_probability": probability,
            "state": row.get("state"),
            "evidence": row.get("evidence") or {},
            "status": "pending",
            "actual": "pending_future_outcome",
            "actual_numeric": None,
        }
        pred["belief_id"] = _stable_hash({
            "run_id": run_id,
            "belief": pred["belief"],
            "prediction_type": pred["prediction_type"],
        }, length=32)
        resolved = resolve_prediction_outcome(
            pred,
            validation_rows=validation_rows or [],
            feedback_rows=feedback_rows or [],
            command_outcomes=command_outcomes or [],
        )
        pred.update(resolved)
        predictions.append(pred)
    return predictions


def resolve_prediction_outcome(
    prediction: dict[str, Any],
    *,
    validation_rows: list[dict[str, Any]] | None = None,
    feedback_rows: list[dict[str, Any]] | None = None,
    command_outcomes: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    subject = str(prediction.get("subject") or "")
    belief_type = str(prediction.get("belief_type") or "")
    if belief_type == "route_prior" and subject:
        feedback = [row for row in feedback_rows or [] if str(row.get("route_key") or "") == subject]
        if feedback:
            rejected = any(str(row.get("status") or row.get("decision") or "").lower() in {"reject", "rejected", "blocked", "failed"} for row in feedback)
            actual_numeric = 0.0 if rejected else 1.0
            scored = score_prediction(prediction.get("predicted_probability"), actual_numeric)
            return {
                "status": "resolved",
                "actual": "rejected_by_feedback" if rejected else "approved_by_feedback",
                "resolution_source": "promotion_feedback",
                **scored,
            }
        validations = [row for row in validation_rows or [] if str(row.get("route_key") or "") == subject]
        if validations:
            pass_rate = sum(1 for row in validations if bool(row.get("passed"))) / max(1, len(validations))
            actual_numeric = 1.0 if pass_rate >= 0.5 else 0.0
            scored = score_prediction(prediction.get("predicted_probability"), actual_numeric)
            return {
                "status": "resolved",
                "actual": "validation_passed" if actual_numeric >= 0.5 else "validation_failed",
                "resolution_source": "validation_results",
                "resolution_sample_count": len(validations),
                "resolution_pass_rate": round(pass_rate, 6),
                **scored,
            }
    if belief_type in {"action", "command_action"} and subject:
        outcomes = [row for row in command_outcomes or [] if str(row.get("action") or "") == subject]
        if outcomes:
            success_rate = sum(1 for row in outcomes if bool(row.get("worked"))) / max(1, len(outcomes))
            actual_numeric = 1.0 if success_rate >= 0.5 else 0.0
            scored = score_prediction(prediction.get("predicted_probability"), actual_numeric)
            return {
                "status": "resolved",
                "actual": "action_worked" if actual_numeric >= 0.5 else "action_failed",
                "resolution_source": "command_outcomes",
                "resolution_sample_count": len(outcomes),
                "resolution_success_rate": round(success_rate, 6),
                **scored,
            }
    actual = prediction.get("actual")
    if actual not in PENDING_VALUES and prediction.get("actual_numeric") is not None:
        scored = score_prediction(prediction.get("predicted_probability"), prediction.get("actual_numeric"))
        return {"status": "resolved", **scored}
    return {
        "status": "pending",
        "actual": "pending_future_outcome",
        "actual_numeric": None,
        "calibration_error": None,
        "brier_score": None,
        "confidence_bucket": probability_bucket(prediction.get("predicted_probability")),
    }


def calibration_report(
    predictions: list[dict[str, Any]] | None,
    *,
    source: str = "step2_belief_calibration",
) -> dict[str, Any]:
    rows = list(predictions or [])
    resolved = [row for row in rows if row.get("status") == "resolved"]
    pending = [row for row in rows if row.get("status") != "resolved"]
    avg_abs = sum(float(row.get("calibration_error") or 0.0) for row in resolved) / max(1, len(resolved))
    avg_brier = sum(float(row.get("brier_score") or 0.0) for row in resolved) / max(1, len(resolved))
    by_type: dict[str, dict[str, Any]] = {}
    buckets: dict[str, dict[str, Any]] = defaultdict(lambda: {"count": 0, "resolved": 0, "predicted_sum": 0.0, "actual_sum": 0.0})
    for row in rows:
        belief_type = str(row.get("belief_type") or "unknown")
        item = by_type.setdefault(belief_type, {"count": 0, "resolved": 0, "pending": 0, "error_sum": 0.0, "brier_sum": 0.0})
        item["count"] += 1
        bucket = buckets[probability_bucket(row.get("predicted_probability"))]
        bucket["count"] += 1
        if row.get("status") == "resolved":
            item["resolved"] += 1
            item["error_sum"] += float(row.get("calibration_error") or 0.0)
            item["brier_sum"] += float(row.get("brier_score") or 0.0)
            bucket["resolved"] += 1
            bucket["predicted_sum"] += float(row.get("predicted_probability") or 0.0)
            bucket["actual_sum"] += float(row.get("actual_numeric") or 0.0)
        else:
            item["pending"] += 1
    type_summary = []
    for belief_type, item in sorted(by_type.items()):
        type_summary.append({
            "belief_type": belief_type,
            "count": item["count"],
            "resolved": item["resolved"],
            "pending": item["pending"],
            "avg_calibration_error": round(item["error_sum"] / max(1, item["resolved"]), 6),
            "avg_brier_score": round(item["brier_sum"] / max(1, item["resolved"]), 6),
        })
    reliability = []
    expected_calibration_error = 0.0
    total_resolved = max(1, len(resolved))
    for bucket_name, item in sorted(buckets.items()):
        if item["resolved"]:
            avg_pred = item["predicted_sum"] / item["resolved"]
            empirical = item["actual_sum"] / item["resolved"]
            gap = abs(avg_pred - empirical)
            expected_calibration_error += gap * (item["resolved"] / total_resolved)
        else:
            avg_pred = 0.0
            empirical = None
            gap = None
        reliability.append({
            "bucket": bucket_name,
            "count": item["count"],
            "resolved": item["resolved"],
            "avg_predicted_probability": round(avg_pred, 6),
            "empirical_success_rate": round(empirical, 6) if empirical is not None else None,
            "calibration_gap": round(gap, 6) if gap is not None else None,
        })
    overconfident = sum(1 for row in resolved if str(row.get("confidence_bias")) == "overconfident")
    underconfident = sum(1 for row in resolved if str(row.get("confidence_bias")) == "underconfident")
    return {
        "schema_version": 1,
        "calibration_version": CALIBRATION_VERSION,
        "source": source,
        "belief_count": len(rows),
        "resolved_predictions": len(resolved),
        "pending_predictions": len(pending),
        "avg_calibration_error": round(avg_abs, 6),
        "brier_score": round(avg_brier, 6),
        "expected_calibration_error": round(expected_calibration_error, 6),
        "overconfidence_rate": round(overconfident / max(1, len(resolved)), 6),
        "underconfidence_rate": round(underconfident / max(1, len(resolved)), 6),
        "by_type": type_summary,
        "reliability_buckets": reliability,
        "learning_adjustments": learning_adjustments(avg_abs, avg_brier, len(resolved), len(pending)),
        "predictions": rows[:200],
    }


def report_from_beliefs(
    belief_update_report: dict[str, Any] | None,
    *,
    run_id: str = "",
    validation_rows: list[dict[str, Any]] | None = None,
    feedback_rows: list[dict[str, Any]] | None = None,
    command_outcomes: list[dict[str, Any]] | None = None,
    additional_predictions: list[dict[str, Any]] | None = None,
    source: str = "step2_belief_calibration",
) -> dict[str, Any]:
    rows = prediction_rows_from_beliefs(
        belief_update_report,
        run_id=run_id,
        validation_rows=validation_rows or [],
        feedback_rows=feedback_rows or [],
        command_outcomes=command_outcomes or [],
    )
    for prediction in additional_predictions or []:
        if not isinstance(prediction, dict):
            continue
        row = dict(prediction)
        if row.get("status") == "resolved" and row.get("calibration_error") is None and row.get("actual_numeric") is not None:
            row.update(score_prediction(row.get("predicted_probability"), row.get("actual_numeric")))
        rows.append(row)
    return calibration_report(rows, source=source)


def learning_adjustments(avg_abs_error: float, avg_brier: float, resolved_count: int, pending_count: int) -> dict[str, Any]:
    reliability_multiplier = max(0.25, min(1.25, 1.0 - float(avg_abs_error) + 0.1))
    if resolved_count <= 0:
        state = "await_outcomes"
    elif avg_abs_error <= 0.12 and avg_brier <= 0.08:
        state = "trusted"
    elif avg_abs_error <= 0.25:
        state = "usable_with_caution"
    else:
        state = "recalibrate_before_scaling"
    return {
        "state": state,
        "belief_weight_multiplier": round(reliability_multiplier, 6),
        "resolved_count": int(resolved_count),
        "pending_count": int(pending_count),
        "recommended_controls": [
            "keep_predictions_pending_until_outcome_evidence_arrives" if pending_count else "continue_scoring_resolved_beliefs",
            "downweight_overconfident_beliefs" if avg_abs_error > 0.25 else "allow_calibrated_beliefs_to_influence_search",
        ],
    }
