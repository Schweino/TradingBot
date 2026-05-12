import step2_learning_control_plane as control


def _row(name, pnl, holdout, trades=120):
    return {
        "variant": name,
        "step2_pnl": pnl,
        "step2_trades": trades,
        "holdout_gate": {"holdout_pnl": holdout},
        "routes": [
            {
                "action": "force_short",
                "match": {
                    "ticker": "CLSK",
                    "setup_type": "trend_pullback",
                    "session_phase": "open",
                    "side": "SHORT",
                },
            }
        ],
    }


def test_learning_control_plane_builds_tranches_9_to_13():
    rows = [_row("winner", 300.0, 120.0), _row("loser", -80.0, -40.0)]
    shapley = {
        "routes": [
            {
                "route_key": "force_short|CLSK|trend_pullback|open|SHORT",
                "observations": 4,
                "avg_pnl_credit": 75.0,
                "avg_holdout_credit": 30.0,
            }
        ]
    }
    pairwise = {
        "interactions": [
            {
                "route_set": ["a", "b"],
                "observations": 2,
                "avg_pnl": 90.0,
                "avg_holdout_pnl": 20.0,
                "interaction_state": "promising_interaction",
            }
        ]
    }
    feature = {"features": [{"feature": "momentum", "importance": 0.7, "direction": "positive"}]}
    backlog = {"backlog": [{"counterfactual_id": "cf1", "subject": "winner", "question": "isolate winner", "priority_score": 80.0, "required_change": "single_variable_repair"}]}
    missed = {"missed": [{"variant": "missed", "reason": "under_gate", "holdout_pnl": 50.0, "pnl": 90.0, "recommended_action": "scale_sample"}]}
    stress = {"tests": [{"test_id": "stress1", "variant": "winner", "stress_type": "leave_worst_day_out", "min_pass_condition": "positive"}]}
    regime = {"fingerprints": [{"bucket_type": "session_phase", "bucket": "open", "variant_count": 3, "avg_pnl": 100.0, "holdout_positive_rate_pct": 66.0, "regime_state": "expand"}]}
    belief = {"learning_adjustments": {"state": "trusted", "belief_weight_multiplier": 1.05}}
    sequencer = {"sequence": [{"order": 1, "subject": "winner", "question": "test winner", "variant_budget": 20, "success_gate": "positive_holdout"}]}

    report = control.learning_control_plane_report(
        rows,
        shapley_report=shapley,
        pairwise_report=pairwise,
        feature_learner_report=feature,
        counterfactual_backlog=backlog,
        missed_winner_report=missed,
        stress_test_queue=stress,
        regime_fingerprint_report=regime,
        belief_calibration_report=belief,
        experiment_sequencer_report=sequencer,
        data_quality_report={"learning_allowed": True, "quality_score": 95.0},
        statistical_validation_report={"false_discovery_pressure": 0.2},
        artifact_trust_report={"trust_state": "trusted"},
        deployment_risk_report={"deployment_state": "paper"},
        batch_size=100,
    )

    assert report["tranches"] == [9, 10, 11, 12, 13]
    assert report["outcome_attribution_report"]["top_route"]["state"] == "causal_candidate"
    assert report["counterfactual_learning_report"]["candidate_count"] >= 3
    assert report["regime_conditioned_learning_report"]["top_regime"]["policy"] == "exploit_with_small_cap"
    assert report["active_experiment_design_report"]["experiment_count"] > 0
    assert report["learning_governance_report"]["decision"] == "allow_learning_to_steer_next_batch"
    assert report["next_batch_directive"]["top_experiment"]


def test_learning_governance_blocks_uncalibrated_or_low_trust_lessons():
    governance = control.learning_governance_report(
        data_quality_report={"learning_allowed": True, "quality_score": 95.0},
        belief_calibration_report={"learning_adjustments": {"state": "recalibrate_before_scaling"}},
        artifact_trust_report={"trust_state": "needs_repair"},
        active_experiment_design_report={"experiments": []},
    )

    assert governance["decision"] == "block_learning_influence"
    assert "belief_calibration_requires_recalibration" in governance["blockers"]
    assert governance["max_learning_influence"] == 0.0
