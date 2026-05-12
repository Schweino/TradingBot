import step2_world_class_audit
import step2_world_class_certification as cert


def _row():
    return {
        "variant": "steady",
        "weights": {"ema": 1.0},
        "step2_pnl": 1000.0,
        "step2_trades": 180,
        "by_day": {
            "2026-04-06": {"pnl": 250.0, "trades": 45},
            "2026-04-07": {"pnl": 250.0, "trades": 45},
            "2026-04-08": {"pnl": 250.0, "trades": 45},
            "2026-04-09": {"pnl": 250.0, "trades": 45},
        },
    }


def _full_payload():
    return {
        "script": "step2_profit_combo_hunter.py",
        "score_only_contract": {
            "score_only": True,
            "no_signal_rebuild": True,
            "no_compiled_tape_rebuild": True,
        },
        "compiled_decision_tape": "compiled.json",
        "compiled_tape_manifest": {"sha256": "abc", "row_count": 1000},
        "compiled_lineage_validation": {"certified": True},
        "artifact_paths": {"summary": "summary.json"},
        "candidate_contracts": {"schema_version": 1},
        "data_coverage_report": {
            "row_count": 206618,
            "day_count": 25,
            "ticker_count": 3,
            "first_day": "2026-04-06",
            "last_day": "2026-05-08",
        },
        "data_quality_report": {"learning_blocked_count": 0},
        "void_filtered_rows": 0,
        "void_filtered_days": [],
        "quote_aware_guard": {"ok": True},
        "exit_replay_model": "quote_aware_exit_v1",
        "required_exit_replay_model": "quote_aware_exit_v1",
        "opportunity_feature_learner_report": {"model_state": "trained", "feature_count": 12},
        "opportunity_target_model_report": {"targets": {"expected_skip_value_pct": {}}},
        "opportunity_rule_extraction_report": {"rules": [{"rule": "x"}]},
        "execution_adjusted_objective_report": {"candidate_count": 1},
        "slippage_sensitivity_sweep": {"scenarios": [{"survivor_count": 1}]},
        "spread_sensitivity_sweep": {"scenarios": [{"survivor_count": 1}]},
        "live_feedback_loop_report": {"feedback_queue": [{"route_key": "CLSK|x|open"}]},
        "order_latency_report": {"samples": 100},
        "missed_winner_report": {"missed": [{"variant": "m"}]},
        "counterfactual_backlog_report": {"backlog": [{"subject": "m"}]},
        "champion_challenger_report": {"promotion_challenger_count": 2},
        "walk_forward_validation_bundle": {"validations": [{"passed": True}]},
        "statistical_validation_report": {"learning_grade_count": 1, "promotion_grade_count": 1},
        "false_discovery_pressure_report": {"pressure_state": "ok"},
        "day_robustness_leaderboard": {"leaderboard": [{"variant": "steady"}]},
        "stress_test_queue": {"tests": [{"test_id": "stress"}]},
        "regime_fingerprint_report": {"fingerprints": [{"regime_state": "expand"}]},
        "regime_conditioned_learning_report": {"regimes": [{"regime_key": "open"}]},
        "controlled_sibling_plan": {"tests": [{"experiment_id": "x"}]},
        "closed_loop_causal_controller": {
            "next_run_directives": {"scale_routes": ["CLSK|x|open"]},
            "route_controller": {"allocation": [{"route_key": "CLSK|x|open"}]},
        },
        "pairwise_route_ablation_report": {"interactions": [{"route_set": ["a", "b"]}]},
        "shapley_route_contribution_estimates": {"routes": [{"route_key": "a"}]},
        "learning_db": {"enabled": True, "stats": {"runs": 2}},
        "route_prior_report": {"priors": [{"route_key": "a"}]},
        "search_budget_allocator_report": {"allocations": [{"variant_budget": 10}]},
        "bandit_lane_allocator": {"allocation": [{"route_key": "a"}]},
        "beam_search_route_set_optimizer": {"optimizer_state": "ready"},
        "genetic_search_operator": {"enabled_for_next_generation": True},
        "search_space_coverage_report": {"lane_coverage": [{"lane": "frontier"}]},
        "promotion_evidence_hardening_report": {"candidates": [{"variant": "steady"}]},
        "deployment_risk_report": {"live_eligible_count": 1, "top": [{"risk_score": 90}]},
        "risk_concentration_report": {"ticker_concentration": 0.3},
        "ops_hardening_report": {"tranches": [19, 20]},
        "drift_monitoring_report": {"monitors": [{"monitor_id": "risk"}]},
        "auditability_lineage_report": {"lineage_entries": [{"artifact": "summary"}]},
        "elite_runbook_report": {"steps": [{"step": "verify"}]},
        "negative_falsification_queue": {"queue": [{"test_id": "neg"}]},
        "rollback_kill_switch_report": {"kill_switches": [{"name": "daily_loss_stop"}]},
        "end_to_end_readiness_gate": {"ready": True, "readiness_state": "ready"},
        "artifact_trust_report": {"trust_state": "trusted"},
        "learning_ops_readiness_report": {"status": "ready"},
        "learning_governance_report": {"required_controls": ["exclude_voided_days"]},
    }


def _world_best_payload():
    payload = _full_payload()
    payload.update({
        "historical_learning_protocol": {
            "train_windows": [{"start": "2026-01-01", "end": "2026-03-31"}],
            "validation_windows": [{"start": "2026-04-01", "end": "2026-04-30"}],
            "test_windows": [{"start": "2026-05-01", "end": "2026-05-08"}],
            "frozen_test_set": True,
            "purged_embargoed": True,
            "test_reuse_policy": "retire_after_use",
            "split_enforced": True,
            "search_touched_test_set": False,
        },
        "oos_profitability_report": {
            "net_oos_pnl": 1200.0,
            "positive_window_rate_pct": 72.0,
            "min_test_trades": 140,
            "execution_adjusted": True,
            "passed_validation_count": 3,
        },
        "belief_calibration_report": {
            "resolved_predictions": 42,
            "pending_predictions": 12,
            "expected_calibration_error": 0.08,
        },
        "causal_ablation_proof_report": {
            "passed_ablation_count": 14,
            "failed_ablation_count": 2,
            "isolated_lift_count": 5,
        },
        "promotion_ladder_report": {
            "stages": {
                "historical": {"passed": True},
                "shadow": {"passed": True},
                "paper": {"passed": True},
                "tiny_live": {"passed": True},
            },
            "forward_trade_count": 140,
            "forward_net_pnl": 640.0,
        },
        "benchmark_superiority_report": {
            "baseline_count": 5,
            "baselines_beaten_count": 5,
            "net_alpha_after_costs": 340.0,
            "includes_random_entry": True,
            "includes_current_live_engine": True,
        },
    })
    return payload


def test_certification_defines_four_buckets_and_twenty_four_tranches():
    report = cert.certification_report(_full_payload(), [_row()], source="test")

    assert report["total_estimated_checks"] == 12000
    assert report["scaffolding_estimated_checks"] == 6000
    assert report["world_best_learning_estimated_checks"] == 6000
    assert report["implemented_bucket_count"] == 4
    assert report["implemented_tranche_count"] == 24
    assert [bucket["estimated_checks"] for bucket in report["buckets"]] == [2000, 2000, 2000, 6000]
    assert report["certified_world_class"] is False
    assert "missing_frozen_purged_historical_splits" in report["blockers"]
    assert report["failed_check_count"] >= 1
    assert any(
        item["blocker"] == "missing_frozen_purged_historical_splits"
        and "split_enforced=true" in item["required_evidence"]
        for item in report["actionable_blockers"]
    )
    assert any(item["tranche_id"] == 19 for item in report["failed_checks"])


def test_certification_passes_only_with_world_best_learning_proof():
    report = cert.certification_report(_world_best_payload(), [_row()], source="test")

    assert report["certified_world_class"] is True
    assert report["blockers"] == []


def test_certification_rejects_overlapping_historical_split_windows():
    payload = _world_best_payload()
    payload["historical_learning_protocol"]["validation_windows"] = [{"start": "2026-03-15", "end": "2026-04-30"}]

    report = cert.certification_report(payload, [_row()], source="test")

    assert report["certified_world_class"] is False
    assert "missing_frozen_purged_historical_splits" in report["blockers"]


def test_bucket_one_blocks_contaminated_or_missing_truth_foundation():
    payload = _full_payload()
    payload["void_filtered_days"] = ["2026-05-11"]
    payload["void_filtered_rows"] = 14

    bucket = cert.bucket_report(payload, [_row()], 1)

    assert bucket["bucket"]["status"] == "blocked"
    assert "void_or_data_quality_contamination" in bucket["bucket"]["blockers"]


def test_bucket_two_blocks_missing_learning_proof():
    payload = _full_payload()
    for key in (
        "statistical_validation_report",
        "false_discovery_pressure_report",
        "day_robustness_leaderboard",
        "walk_forward_validation_bundle",
        "stress_test_queue",
        "regime_fingerprint_report",
        "regime_conditioned_learning_report",
        "controlled_sibling_plan",
        "closed_loop_causal_controller",
        "pairwise_route_ablation_report",
        "shapley_route_contribution_estimates",
        "learning_db",
        "route_prior_report",
        "search_budget_allocator_report",
        "bandit_lane_allocator",
        "beam_search_route_set_optimizer",
        "genetic_search_operator",
        "search_space_coverage_report",
    ):
        payload.pop(key, None)

    bucket = cert.bucket_report(payload, [_row()], 2)

    assert bucket["bucket"]["status"] == "blocked"
    assert len(bucket["bucket"]["blockers"]) == 6


def test_bucket_three_blocks_missing_deployment_confidence():
    payload = _full_payload()
    for key in (
        "promotion_evidence_hardening_report",
        "live_feedback_loop_report",
        "champion_challenger_report",
        "deployment_risk_report",
        "risk_concentration_report",
        "ops_hardening_report",
        "drift_monitoring_report",
        "auditability_lineage_report",
        "elite_runbook_report",
        "stress_test_queue",
        "negative_falsification_queue",
        "slippage_sensitivity_sweep",
        "spread_sensitivity_sweep",
        "rollback_kill_switch_report",
        "end_to_end_readiness_gate",
    ):
        payload.pop(key, None)

    bucket = cert.bucket_report(payload, [_row()], 3)

    assert bucket["bucket"]["status"] == "blocked"
    assert len(bucket["bucket"]["blockers"]) == 6


def test_existing_audit_embeds_certification_without_changing_readiness_contract():
    payload = _full_payload()
    payload["leaderboard"] = [_row()]

    report = step2_world_class_audit.payload_readiness(payload)

    assert report["ok"] is True
    assert report["world_class_certification"]["implemented_tranche_count"] == 24
    assert report["world_class_certified"] is False


def test_existing_audit_certifies_when_world_best_learning_proof_is_present():
    payload = _world_best_payload()
    payload["leaderboard"] = [_row()]

    report = step2_world_class_audit.payload_readiness(payload)

    assert report["ok"] is True
    assert report["world_class_certified"] is True
