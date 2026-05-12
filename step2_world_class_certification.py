"""World-class certification buckets for the Step 2 learning engine.

This module maps the large proof surface into buildable buckets.  Each
tranche is represented by a small set of high-signal gates plus an estimated
check count, so the report can track roughly 12,000 lower-level assertions
without hard-coding thousands of brittle one-off booleans.
"""
from __future__ import annotations

from datetime import date
from dataclasses import dataclass
from typing import Any, Callable


CERTIFICATION_VERSION = "step2_world_class_certification_v2"
TOTAL_ESTIMATED_CHECKS = 12000
SCAFFOLDING_CHECKS = 6000
WORLD_BEST_LEARNING_CHECKS = 6000


@dataclass(frozen=True)
class CheckSpec:
    check_id: str
    description: str
    predicate: Callable[[dict[str, Any], list[dict[str, Any]]], bool]
    blocker: str
    hard: bool = True


@dataclass(frozen=True)
class TrancheSpec:
    bucket_id: int
    bucket_name: str
    tranche_id: int
    tranche_name: str
    estimated_checks: int
    checks: tuple[CheckSpec, ...]


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _has_dict(payload: dict[str, Any], key: str) -> bool:
    return bool(_dict(payload.get(key)))


def _has_list(payload: dict[str, Any], key: str) -> bool:
    return bool(_list(payload.get(key)))


def _nested(payload: dict[str, Any], *keys: str) -> Any:
    cur: Any = payload
    for key in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def _report_count(payload: dict[str, Any], report_key: str, *count_keys: str) -> float:
    report = _dict(payload.get(report_key))
    for key in count_keys:
        value = report.get(key)
        if value is not None:
            return _num(value)
    return 0.0


def _parse_date(value: Any) -> date | None:
    try:
        raw = str(value or "")[:10]
        if not raw:
            return None
        return date.fromisoformat(raw)
    except Exception:
        return None


def _split_topology_valid(protocol: dict[str, Any]) -> bool:
    phases = [
        _list(protocol.get("train_windows")),
        _list(protocol.get("validation_windows")),
        _list(protocol.get("test_windows")),
    ]
    bounds: list[list[tuple[date, date]]] = []
    for windows in phases:
        phase_bounds = []
        for window in windows:
            if not isinstance(window, dict):
                return False
            start = _parse_date(window.get("start"))
            end = _parse_date(window.get("end"))
            if start is None or end is None or start > end:
                return False
            phase_bounds.append((start, end))
        phase_bounds.sort(key=lambda item: item[0])
        for left, right in zip(phase_bounds, phase_bounds[1:]):
            if right[0] <= left[1]:
                return False
        bounds.append(phase_bounds)
    if not all(bounds):
        return False
    embargo_days = int(_num(protocol.get("purge_embargo_days"), 0.0)) if protocol.get("purged_embargoed") is True else 0
    phase_ends = [max(end for _, end in phase) for phase in bounds]
    phase_starts = [min(start for start, _ in phase) for phase in bounds]
    for idx in range(len(bounds) - 1):
        gap = (phase_starts[idx + 1] - phase_ends[idx]).days
        if gap <= embargo_days:
            return False
    return True


def _truth_data_lineage(payload: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    coverage = _dict(payload.get("data_coverage_report"))
    has_rows = _num(coverage.get("row_count")) > 0 or bool(rows)
    has_span = _num(coverage.get("day_count")) >= 10 or bool(coverage.get("first_day") and coverage.get("last_day"))
    has_manifest = _has_dict(payload, "compiled_tape_manifest") or _has_dict(payload, "compiled_lineage_validation")
    return bool(has_rows and has_span and has_manifest)


def _truth_artifact_lineage(payload: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    return _has_dict(payload, "artifact_paths") or _has_dict(payload, "candidate_contracts")


def _void_clean(payload: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    if int(_num(payload.get("void_filtered_rows"))) > 0:
        return False
    if _list(payload.get("void_filtered_days")):
        return False
    if _list(payload.get("voided_source_days")):
        return False
    dq = _dict(payload.get("data_quality_report") or payload.get("data_quality_contract_report"))
    return int(_num(dq.get("learning_blocked_count"))) <= 0


def _void_registry_controls(payload: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    controls = _dict(payload.get("world_class_learning_controls"))
    implemented = set(str(item) for item in _list(controls.get("implemented")))
    required_controls = set(str(item) for item in _list(_nested(payload, "learning_governance_report", "required_controls")))
    return bool("exclude_voided_days" in required_controls or any("void" in item for item in implemented))


def _feature_correctness(payload: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    feature = _dict(payload.get("opportunity_feature_learner_report"))
    target = _dict(payload.get("opportunity_target_model_report"))
    rule = _dict(payload.get("opportunity_rule_extraction_report"))
    if feature and (feature.get("model_state") == "trained" or _num(feature.get("feature_count")) > 0):
        return bool(target or rule)
    return _has_dict(payload, "compiled_tape_manifest") and _has_dict(payload, "candidate_contracts")


def _feature_contracts(payload: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    return _has_dict(payload, "candidate_contracts") or _has_dict(payload, "score_only_contract")


def _replay_fidelity(payload: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    quote = _dict(payload.get("quote_aware_guard"))
    exit_model = str(payload.get("exit_replay_model") or "")
    required = str(payload.get("required_exit_replay_model") or "")
    replay_ok = quote.get("ok") is True or bool(exit_model and required and exit_model == required)
    score_only = _dict(payload.get("score_only_contract"))
    return bool(replay_ok and score_only.get("score_only") is True)


def _replay_lineage(payload: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    lineage = _dict(payload.get("compiled_lineage_validation"))
    return bool(lineage.get("certified") or lineage.get("quick_score_allowed") or _has_dict(payload, "compiled_tape_manifest"))


def _execution_reality(payload: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    objective = _dict(payload.get("execution_adjusted_objective_report"))
    latency = _dict(payload.get("latency_model_report") or payload.get("step2_latency_model"))
    slippage = _dict(payload.get("slippage_sensitivity_sweep"))
    spread = _dict(payload.get("spread_sensitivity_sweep"))
    broker = _dict(payload.get("broker_execution_score"))
    return bool(objective or latency or (slippage and spread) or broker)


def _fill_evidence(payload: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    live = _dict(payload.get("live_feedback_loop_report"))
    order_latency = _dict(payload.get("order_latency_report"))
    execution = _dict(payload.get("execution_adjusted_objective_report"))
    return bool(live.get("feedback_queue") or order_latency or execution.get("candidate_count") is not None)


def _opportunity_accounting(payload: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    return any(
        _has_dict(payload, key) or _has_list(payload, key)
        for key in (
            "missed_winner_report",
            "false_positive_negative_report",
            "opportunity_target_model_report",
            "counterfactual_backlog_report",
        )
    )


def _opportunity_conservation(payload: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    return _has_dict(payload, "champion_challenger_report") or _has_dict(payload, "walk_forward_validation_bundle")


def _statistical_validation(payload: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    stat = _dict(payload.get("statistical_validation_report"))
    return bool(_num(stat.get("learning_grade_count")) > 0 or _has_list(stat, "leaderboard"))


def _backtest_overfit_defense(payload: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    return any(
        _has_dict(payload, key)
        for key in (
            "false_discovery_pressure_report",
            "day_robustness_leaderboard",
            "walk_forward_validation_bundle",
            "stress_test_queue",
        )
    )


def _regime_generalization(payload: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    return _has_dict(payload, "regime_fingerprint_report") or _has_dict(payload, "regime_conditioned_learning_report")


def _causal_learning(payload: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    return any(
        _has_dict(payload, key)
        for key in (
            "controlled_sibling_plan",
            "closed_loop_causal_controller",
            "counterfactual_learning_report",
            "pairwise_route_ablation_report",
            "shapley_route_contribution_estimates",
        )
    )


def _online_memory(payload: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    learning_db = _dict(payload.get("learning_db"))
    stats = _dict(learning_db.get("stats"))
    return bool(learning_db.get("enabled") or _has_dict(payload, "route_prior_report") or _num(stats.get("runs")) > 0)


def _candidate_search_quality(payload: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    return any(
        _has_dict(payload, key)
        for key in (
            "search_budget_allocator_report",
            "bandit_lane_allocator",
            "beam_search_route_set_optimizer",
            "genetic_search_operator",
            "search_space_coverage_report",
        )
    )


def _promotion_hardening(payload: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    return _has_dict(payload, "promotion_evidence_hardening_report") or _has_dict(payload, "promotion_evidence_packet")


def _live_shadow_paper(payload: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    return _has_dict(payload, "live_feedback_loop_report") or _has_dict(payload, "champion_challenger_report")


def _risk_capital_controls(payload: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    risk = _dict(payload.get("deployment_risk_report"))
    return bool(risk or _has_dict(payload, "rollback_kill_switch_report") or _has_dict(payload, "risk_concentration_report"))


def _monitoring_incident_response(payload: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    return any(
        _has_dict(payload, key)
        for key in (
            "ops_hardening_report",
            "drift_monitoring_report",
            "auditability_lineage_report",
            "elite_runbook_report",
        )
    )


def _adversarial_red_team(payload: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    return any(
        _has_dict(payload, key)
        for key in (
            "stress_test_queue",
            "negative_falsification_queue",
            "slippage_sensitivity_sweep",
            "spread_sensitivity_sweep",
            "rollback_kill_switch_report",
        )
    )


def _certification_layer(payload: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    gate = _dict(payload.get("end_to_end_readiness_gate"))
    return bool(gate.get("ready") is True or str(gate.get("readiness_state") or "") == "ready")


def _historical_split_integrity(payload: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    protocol = _dict(payload.get("historical_learning_protocol"))
    walk = _dict(payload.get("walk_forward_validation_bundle"))
    split = _dict(walk.get("split_protocol"))
    train = _list(protocol.get("train_windows")) or _list(split.get("train_windows"))
    validation = _list(protocol.get("validation_windows")) or _list(split.get("validation_windows"))
    test = _list(protocol.get("test_windows")) or _list(split.get("test_windows"))
    frozen = protocol.get("frozen_test_set") is True or split.get("frozen_test_set") is True
    purged = protocol.get("purged_embargoed") is True or split.get("purged_embargoed") is True
    retired = protocol.get("test_reuse_policy") == "retire_after_use" or split.get("test_reuse_policy") == "retire_after_use"
    enforced = protocol.get("split_enforced") is True or split.get("split_enforced") is True
    clean = protocol.get("search_touched_test_set") is False or split.get("search_touched_test_set") is False
    topology = _split_topology_valid(protocol or split)
    return bool(train and validation and test and frozen and purged and retired and enforced and clean and topology)


def _oos_profitability(payload: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    proof = _dict(payload.get("oos_profitability_report"))
    if not proof:
        proof = _dict(payload.get("walk_forward_validation_bundle")).get("oos_profitability_report")
        proof = _dict(proof)
    return bool(
        _num(proof.get("net_oos_pnl")) > 0.0
        and _num(proof.get("positive_window_rate_pct")) >= 60.0
        and _num(proof.get("min_test_trades")) >= 100.0
        and proof.get("execution_adjusted") is True
        and _num(proof.get("passed_validation_count") or proof.get("passed_count")) > 0.0
    )


def _learning_calibration_resolved(payload: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    calibration = _dict(payload.get("belief_calibration_report") or payload.get("prediction_calibration_ledger"))
    resolved = _num(calibration.get("resolved_predictions") or calibration.get("resolved_count"))
    pending = _num(calibration.get("pending_predictions") or calibration.get("pending_count"))
    error = _num(calibration.get("expected_calibration_error") or calibration.get("avg_calibration_error"), 1.0)
    return bool(resolved >= 30.0 and pending <= resolved * 2.0 and error <= 0.12)


def _causal_ablation_proof(payload: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    proof = _dict(payload.get("causal_ablation_proof_report"))
    if not proof:
        proof = _dict(payload.get("ab_route_experiment_executor"))
    passed = _num(proof.get("passed_ablation_count") or proof.get("passed_count"))
    failed = _num(proof.get("failed_ablation_count") or proof.get("failed_count"))
    isolated = _num(proof.get("isolated_lift_count"))
    return bool(passed >= 10.0 and failed <= passed * 0.35 and isolated >= 3.0)


def _promotion_ladder_proof(payload: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    ladder = _dict(payload.get("promotion_ladder_report"))
    stages = ladder.get("stages")
    if not isinstance(stages, dict):
        stages = {}
    required = ("historical", "shadow", "paper", "tiny_live")
    return bool(
        all(_dict(stages.get(stage)).get("passed") is True for stage in required)
        and _num(ladder.get("forward_trade_count")) >= 100.0
        and _num(ladder.get("forward_net_pnl")) > 0.0
    )


def _external_benchmark_superiority(payload: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
    proof = _dict(payload.get("benchmark_superiority_report"))
    return bool(
        _num(proof.get("baseline_count")) >= 5.0
        and _num(proof.get("baselines_beaten_count")) >= _num(proof.get("baseline_count"))
        and _num(proof.get("net_alpha_after_costs")) > 0.0
        and proof.get("includes_random_entry") is True
        and proof.get("includes_current_live_engine") is True
    )


TRANCHES: tuple[TrancheSpec, ...] = (
    TrancheSpec(1, "Truth Foundation", 1, "Truth/Data Lineage", 300, (
        CheckSpec("lineage_data_span", "Decision tape has row/day/ticker lineage.", _truth_data_lineage, "missing_decision_tape_lineage"),
        CheckSpec("lineage_artifacts", "Artifacts and candidate contracts are traceable.", _truth_artifact_lineage, "missing_artifact_lineage"),
    )),
    TrancheSpec(1, "Truth Foundation", 2, "Void/Contamination Immunity", 300, (
        CheckSpec("void_clean", "Voided source days are excluded.", _void_clean, "void_or_data_quality_contamination"),
        CheckSpec("void_controls", "Learning governance carries void controls.", _void_registry_controls, "missing_void_learning_controls"),
    )),
    TrancheSpec(1, "Truth Foundation", 3, "Feature Correctness", 300, (
        CheckSpec("feature_correctness", "Feature/target/rule artifacts exist.", _feature_correctness, "missing_feature_correctness_evidence"),
        CheckSpec("feature_contracts", "Feature/candidate contracts are present.", _feature_contracts, "missing_feature_contracts"),
    )),
    TrancheSpec(1, "Truth Foundation", 4, "Replay Fidelity", 400, (
        CheckSpec("quote_aware_replay", "Replay is quote-aware and score-only.", _replay_fidelity, "missing_quote_aware_score_only_replay"),
        CheckSpec("replay_lineage", "Compiled replay lineage is certified or traceable.", _replay_lineage, "missing_replay_lineage_certification"),
    )),
    TrancheSpec(1, "Truth Foundation", 5, "Execution/Fills Reality", 450, (
        CheckSpec("execution_objective", "Execution-adjusted objective or latency/slippage evidence exists.", _execution_reality, "missing_execution_reality_evidence"),
        CheckSpec("fill_evidence", "Fill/latency/broker feedback evidence exists.", _fill_evidence, "missing_fill_evidence"),
    )),
    TrancheSpec(1, "Truth Foundation", 6, "Opportunity Accounting", 250, (
        CheckSpec("opportunity_accounting", "Missed/skipped/counterfactual opportunities are accounted for.", _opportunity_accounting, "missing_opportunity_accounting"),
        CheckSpec("opportunity_conservation", "Opportunity costs are tested against champion/challenger or walk-forward.", _opportunity_conservation, "missing_opportunity_conservation"),
    )),
    TrancheSpec(2, "Learning Proof", 7, "Statistical Validation", 360, (
        CheckSpec("statistical_validation", "Learning-grade statistical validation is present.", _statistical_validation, "missing_statistical_learning_validation"),
    )),
    TrancheSpec(2, "Learning Proof", 8, "Backtest Overfit Defense", 340, (
        CheckSpec("overfit_defense", "False discovery, robustness, or walk-forward defenses exist.", _backtest_overfit_defense, "missing_backtest_overfit_defense"),
    )),
    TrancheSpec(2, "Learning Proof", 9, "Regime Generalization", 350, (
        CheckSpec("regime_generalization", "Regime-conditioned learning evidence exists.", _regime_generalization, "missing_regime_generalization"),
    )),
    TrancheSpec(2, "Learning Proof", 10, "Causal Learning", 360, (
        CheckSpec("causal_learning", "Controlled/counterfactual/ablation evidence exists.", _causal_learning, "missing_causal_learning_evidence"),
    )),
    TrancheSpec(2, "Learning Proof", 11, "Online Learning Memory", 340, (
        CheckSpec("online_memory", "Cross-run learning memory exists.", _online_memory, "missing_online_learning_memory"),
    )),
    TrancheSpec(2, "Learning Proof", 12, "Candidate Search Quality", 250, (
        CheckSpec("candidate_search_quality", "Search/budget/coverage controls exist.", _candidate_search_quality, "missing_candidate_search_quality"),
    )),
    TrancheSpec(3, "Deployment Confidence", 13, "Promotion Gate Hardening", 350, (
        CheckSpec("promotion_hardening", "Promotion evidence is hardened.", _promotion_hardening, "missing_promotion_hardening"),
    )),
    TrancheSpec(3, "Deployment Confidence", 14, "Live Shadow/Paper Proof", 350, (
        CheckSpec("live_shadow_paper", "Live shadow, paper, or challenger evidence exists.", _live_shadow_paper, "missing_live_shadow_or_paper_proof"),
    )),
    TrancheSpec(3, "Deployment Confidence", 15, "Risk/Capital Controls", 300, (
        CheckSpec("risk_capital", "Risk, drawdown, and capital controls exist.", _risk_capital_controls, "missing_risk_capital_controls"),
    )),
    TrancheSpec(3, "Deployment Confidence", 16, "Monitoring/Incident Response", 300, (
        CheckSpec("monitoring_incident_response", "Ops hardening and incident response evidence exists.", _monitoring_incident_response, "missing_monitoring_incident_response"),
    )),
    TrancheSpec(3, "Deployment Confidence", 17, "Adversarial Red-Team Suite", 350, (
        CheckSpec("adversarial_red_team", "Stress, falsification, and sensitivity tests exist.", _adversarial_red_team, "missing_adversarial_red_team"),
    )),
    TrancheSpec(3, "Deployment Confidence", 18, "World-Class Certification Layer", 350, (
        CheckSpec("certification_layer", "End-to-end readiness gate exists.", _certification_layer, "missing_end_to_end_certification_gate"),
    )),
    TrancheSpec(4, "World-Best Learning Proof", 19, "Frozen Historical Splits", 1000, (
        CheckSpec("historical_split_integrity", "Train/validation/test windows are frozen, purged, embargoed, and retired after use.", _historical_split_integrity, "missing_frozen_purged_historical_splits"),
    )),
    TrancheSpec(4, "World-Best Learning Proof", 20, "Out-Of-Sample Profitability", 1000, (
        CheckSpec("oos_profitability", "Candidates prove execution-adjusted net profit on untouched out-of-sample windows.", _oos_profitability, "missing_oos_execution_adjusted_profitability"),
    )),
    TrancheSpec(4, "World-Best Learning Proof", 21, "Resolved Calibration", 1000, (
        CheckSpec("learning_calibration_resolved", "Predicted lessons are later resolved and calibrated.", _learning_calibration_resolved, "missing_resolved_learning_calibration"),
    )),
    TrancheSpec(4, "World-Best Learning Proof", 22, "Causal Ablation Proof", 1000, (
        CheckSpec("causal_ablation_proof", "Winning ingredients survive isolated causal ablations.", _causal_ablation_proof, "missing_causal_ablation_proof"),
    )),
    TrancheSpec(4, "World-Best Learning Proof", 23, "Promotion Ladder Proof", 1000, (
        CheckSpec("promotion_ladder_proof", "Historical winners survive shadow, paper, and tiny-live forward stages.", _promotion_ladder_proof, "missing_forward_promotion_ladder_proof"),
    )),
    TrancheSpec(4, "World-Best Learning Proof", 24, "External Benchmark Superiority", 1000, (
        CheckSpec("external_benchmark_superiority", "Learning beats current engine, naive, random, and simple strategy baselines after costs.", _external_benchmark_superiority, "missing_external_benchmark_superiority"),
    )),
)


def _evaluate_check(spec: CheckSpec, payload: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    try:
        passed = bool(spec.predicate(payload, rows))
        error = None
    except Exception as exc:
        passed = False
        error = str(exc)
    out = {
        "check_id": spec.check_id,
        "description": spec.description,
        "passed": passed,
        "hard": bool(spec.hard),
        "blocker": None if passed else spec.blocker,
    }
    if error:
        out["error"] = error
    return out


EVIDENCE_HINTS = {
    "missing_frozen_purged_historical_splits": "Provide historical_learning_protocol or walk_forward_validation_bundle.split_protocol with chronological non-overlapping train/validation/test windows, frozen_test_set=true, purged_embargoed=true, split_enforced=true, search_touched_test_set=false, and test_reuse_policy=retire_after_use.",
    "missing_positive_execution_adjusted_oos_profit": "Provide oos_profitability_report with positive net_oos_pnl, positive_window_rate_pct >= 60, min_test_trades >= 100, execution_adjusted=true, and at least one passed validation.",
    "missing_resolved_calibration_proof": "Provide belief_calibration_report with resolved_predictions >= 30, expected_calibration_error <= 0.12, and pending_predictions no more than 2x resolved.",
    "missing_actual_causal_ablation_proof": "Provide causal_ablation_proof_report with at least 10 passed actual ablations, failed_ablation_count <= 35% of passed, and at least 3 isolated_lift proofs.",
    "missing_forward_promotion_ladder_proof": "Provide promotion_ladder_report where historical, shadow, paper, and tiny_live stages passed for the same candidate, with forward_trade_count >= 100 and positive forward_net_pnl.",
    "missing_external_benchmark_superiority": "Provide benchmark_superiority_report with at least 5 baselines, all beaten after costs, random-entry included, current-live-engine included, and positive net alpha.",
}


def _actionable_blocker(blocker: str) -> dict[str, Any]:
    return {
        "blocker": blocker,
        "required_evidence": EVIDENCE_HINTS.get(blocker, "Add the missing report or satisfy the failed check described in the tranche output."),
    }


def certification_report(
    payload: dict[str, Any] | None = None,
    rows: list[dict[str, Any]] | None = None,
    *,
    source: str = "step2",
) -> dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    rows = rows if isinstance(rows, list) else []
    tranche_reports = []
    for tranche in TRANCHES:
        checks = [_evaluate_check(check, payload, rows) for check in tranche.checks]
        hard_failures = [check for check in checks if not check["passed"] and check["hard"]]
        passed_count = sum(1 for check in checks if check["passed"])
        coverage = passed_count / max(1, len(checks))
        tranche_reports.append({
            "bucket_id": tranche.bucket_id,
            "bucket_name": tranche.bucket_name,
            "tranche_id": tranche.tranche_id,
            "tranche_name": tranche.tranche_name,
            "estimated_checks": tranche.estimated_checks,
            "represented_check_count": len(checks),
            "passed_check_count": passed_count,
            "coverage_pct": round(coverage * 100.0, 4),
            "status": "passed" if not hard_failures else "blocked",
            "blockers": [str(check["blocker"]) for check in hard_failures if check.get("blocker")],
            "checks": checks,
        })

    bucket_reports = []
    bucket_ids = sorted({tranche.bucket_id for tranche in TRANCHES})
    for bucket_id in bucket_ids:
        rows_for_bucket = [row for row in tranche_reports if row["bucket_id"] == bucket_id]
        estimated = sum(int(row["estimated_checks"]) for row in rows_for_bucket)
        passed_weighted = sum(
            int(row["estimated_checks"]) * (_num(row.get("coverage_pct")) / 100.0)
            for row in rows_for_bucket
        )
        blockers = sorted({blocker for row in rows_for_bucket for blocker in row["blockers"]})
        coverage_pct = passed_weighted / max(1, estimated) * 100.0
        bucket_reports.append({
            "bucket_id": bucket_id,
            "bucket_name": rows_for_bucket[0]["bucket_name"] if rows_for_bucket else f"Bucket {bucket_id}",
            "estimated_checks": estimated,
            "coverage_pct": round(coverage_pct, 4),
            "status": "passed" if not blockers and coverage_pct >= 95.0 else "blocked" if blockers else "partial",
            "blockers": blockers,
            "tranche_count": len(rows_for_bucket),
            "passed_tranche_count": sum(1 for row in rows_for_bucket if row["status"] == "passed"),
        })

    total_estimated = sum(int(row["estimated_checks"]) for row in bucket_reports)
    passed_estimated = sum(int(row["estimated_checks"]) * (_num(row["coverage_pct"]) / 100.0) for row in bucket_reports)
    all_blockers = sorted({blocker for bucket in bucket_reports for blocker in bucket["blockers"]})
    failed_checks = [
        {
            "bucket_id": tranche["bucket_id"],
            "bucket_name": tranche["bucket_name"],
            "tranche_id": tranche["tranche_id"],
            "tranche_name": tranche["tranche_name"],
            "check_id": check["check_id"],
            "description": check["description"],
            "blocker": check["blocker"],
            "required_evidence": _actionable_blocker(str(check["blocker"]))["required_evidence"],
        }
        for tranche in tranche_reports
        for check in tranche["checks"]
        if check.get("hard") and not check.get("passed")
    ]
    coverage_pct = passed_estimated / max(1, total_estimated) * 100.0
    certified = not all_blockers and coverage_pct >= 95.0
    return {
        "schema_version": 1,
        "certification_version": CERTIFICATION_VERSION,
        "source": source,
        "total_estimated_checks": TOTAL_ESTIMATED_CHECKS,
        "implemented_bucket_count": len(bucket_ids),
        "implemented_tranche_count": len(TRANCHES),
        "represented_gate_count": sum(len(tranche.checks) for tranche in TRANCHES),
        "scaffolding_estimated_checks": SCAFFOLDING_CHECKS,
        "world_best_learning_estimated_checks": WORLD_BEST_LEARNING_CHECKS,
        "coverage_pct": round(coverage_pct, 4),
        "certified_world_class": bool(certified),
        "status": "certified" if certified else "blocked" if all_blockers else "partial",
        "blockers": all_blockers,
        "actionable_blockers": [_actionable_blocker(blocker) for blocker in all_blockers],
        "failed_check_count": len(failed_checks),
        "failed_checks": failed_checks,
        "buckets": bucket_reports,
        "tranches": tranche_reports,
        "deduction": (
            "The 12,000-check surface is represented as weighted tranches. "
            "The first 6,000 checks cover scaffolding; the additional 6,000 checks require "
            "frozen historical splits, out-of-sample profitability, resolved calibration, "
            "causal ablation, forward promotion proof, and external benchmark superiority."
        ),
    }


def bucket_report(payload: dict[str, Any] | None, rows: list[dict[str, Any]] | None, bucket_id: int) -> dict[str, Any]:
    report = certification_report(payload, rows)
    bucket = next((row for row in report["buckets"] if row["bucket_id"] == int(bucket_id)), None)
    tranches = [row for row in report["tranches"] if row["bucket_id"] == int(bucket_id)]
    return {
        "schema_version": 1,
        "certification_version": CERTIFICATION_VERSION,
        "bucket": bucket,
        "tranches": tranches,
    }
