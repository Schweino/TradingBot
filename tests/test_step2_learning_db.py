import json
from pathlib import Path
from uuid import uuid4

import step2_learning_db


def _workspace_tmp(name):
    path = Path("runtime") / "unit_test_tmp" / f"{name}_{uuid4().hex}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_learning_db_ingests_run_and_feedback():
    tmp_path = _workspace_tmp("learning_db")
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "final_summary.json").write_text(
        json.dumps(
            {
                "source": "test",
                "started_at_ct": "2026-05-10T00:00:00-05:00",
                "finished_at_ct": "2026-05-10T01:00:00-05:00",
                "cycles": [{"cycle": 0, "hunter": "router", "ok": True, "stdout_tail": '{"scored_total":100,"winners":2}'}],
                "top100": [
                    {
                        "rank": 1,
                        "variant": "router_1_CLSK|trend_pullback|late",
                        "behavior_key": "b1",
                        "config_key": "c1",
                        "route_key": "CLSK|trend_pullback|late",
                        "step2_pnl": 120.0,
                        "step2_delta_vs_active": 5.0,
                    }
                ],
                "artifact_paths": {"finalists": str(run_dir / "finalists.json")},
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "causal_experiment_registry.json").write_text(
        json.dumps(
            {
                "experiments": [
                    {
                        "experiment_id": "exp1",
                        "kind": "controlled_parent_sibling",
                        "route_key": "CLSK|trend_pullback|late",
                        "family_key": "fam1",
                        "hypothesis": "test sibling treatments",
                        "status": "planned",
                        "verdict": "planned",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "experiment_debt_queue.json").write_text(
        json.dumps(
            {
                "queue": [
                    {
                        "question": "Does holdout repair work here?",
                        "route_key": "CLSK|trend_pullback|late",
                        "family_key": "fam1",
                        "priority_score": 90.0,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "prediction_calibration_ledger.json").write_text(
        json.dumps(
            {
                "entries": [
                    {
                        "prediction_type": "promotion_approve_probability",
                        "variant": "router_1_CLSK|trend_pullback|late",
                        "predicted_probability": 0.8,
                        "actual": "pending",
                    }
                ],
                "treatment_entries": [
                    {
                        "prediction_type": "treatment_decision",
                        "treatment": "holdout_repair",
                        "posterior_success_mean": 0.7,
                        "actual": "pending_future_outcome",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "belief_update_report.json").write_text(
        json.dumps(
            {
                "beliefs": [
                    {
                        "belief": "route_prior:CLSK|trend_pullback|late",
                        "posterior_confidence": 0.75,
                        "state": "scaling",
                        "evidence": {"observations": 3},
                    },
                    {
                        "belief": "positive_gene:holdout_repair",
                        "posterior_confidence": 0.6,
                        "state": "gene_supports_future_search",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "learning_control_plane.json").write_text(
        json.dumps(
            {
                "outcome_attribution_report": {
                    "route_attribution": [
                        {
                            "route_key": "CLSK|trend_pullback|late",
                            "attribution_score": 42.0,
                            "state": "causal_candidate",
                        }
                    ]
                },
                "counterfactual_learning_report": {
                    "counterfactuals": [
                        {
                            "counterfactual_id": "cf1",
                            "subject": "CLSK|trend_pullback|late",
                            "expected_learning_value": 50.0,
                            "source": "outcome_attribution",
                        }
                    ]
                },
                "regime_conditioned_learning_report": {
                    "regimes": [
                        {
                            "regime_key": "session_phase:late",
                            "regime_score": 70.0,
                            "regime_state": "expand",
                        }
                    ]
                },
                "active_experiment_design_report": {
                    "experiments": [
                        {
                            "experiment_id": "exp-control-1",
                            "subject": "CLSK|trend_pullback|late",
                            "expected_value_of_information": 85.0,
                            "success_gate": "positive_holdout",
                        }
                    ]
                },
                "learning_governance_report": {
                    "decision": "allow_learning_to_steer_next_batch",
                    "max_learning_influence": 0.75,
                },
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "learning_depth_report.json").write_text(
        json.dumps(
            {
                "enhanced_target_label_report": {
                    "top_labels": [
                        {
                            "variant": "router_1_CLSK|trend_pullback|late",
                            "target_label": "promotion_survivor",
                            "learning_weight": 1.2,
                        }
                    ]
                },
                "lesson_survival_decay_report": {
                    "lessons": [
                        {
                            "subject": "CLSK|trend_pullback|late",
                            "survival_score": 0.8,
                            "decay_action": "retain",
                        }
                    ]
                },
                "portfolio_learning_report": {
                    "top_portfolio_candidates": [
                        {
                            "variant": "router_1_CLSK|trend_pullback|late",
                            "portfolio_score": 120.0,
                            "role": "core",
                        }
                    ]
                },
                "uncertainty_risk_pricing_report": {
                    "priced_cells": [
                        {
                            "subject": "route:CLSK|trend_pullback|late",
                            "uncertainty_price": 0.25,
                            "recommended_action": "controlled_sibling_test",
                        }
                    ]
                },
                "promotion_evidence_hardening_report": {
                    "candidates": [
                        {
                            "variant": "router_1_CLSK|trend_pullback|late",
                            "hardening_score": 90.0,
                            "promotion_evidence_status": "hardened",
                        }
                    ]
                },
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "ops_hardening_report.json").write_text(
        json.dumps(
            {
                "live_feedback_loop_report": {
                    "feedback_queue": [
                        {
                            "route_key": "CLSK|trend_pullback|late",
                            "action": "scale",
                            "source": "closed_loop_controller",
                        }
                    ]
                },
                "drift_monitoring_report": {
                    "monitors": [
                        {
                            "monitor_id": "risk_budget_state",
                            "subject": "learning_depth",
                            "severity": "medium",
                        }
                    ]
                },
                "rollback_kill_switch_report": {
                    "kill_switches": [
                        {
                            "name": "disable_if_void_registry_flags_source_day",
                            "armed": True,
                            "severity": "critical",
                        }
                    ]
                },
                "auditability_lineage_report": {
                    "lineage_entries": [
                        {
                            "artifact": "learning_depth_report",
                            "present": True,
                        }
                    ]
                },
                "end_to_end_readiness_gate": {
                    "ready": False,
                    "readiness_state": "blocked",
                },
                "elite_runbook_report": {
                    "steps": [
                        {
                            "order": 1,
                            "step": "verify_void_registry_and_data_quality",
                            "owner": "pre_run_gate",
                        }
                    ]
                },
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "route_regime_half_life.json").write_text(
        json.dumps(
            {
                "routes": [
                    {
                        "route_key": "CLSK|trend_pullback|late",
                        "freshness": 0.4,
                        "freshness_score": 40.0,
                        "decayed_expected_promotable_pnl": 25.0,
                        "recommended_action": "retest",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "policy_tournament.json").write_text(
        json.dumps(
            {
                "champion": {"policy_name": "truth_first_conservative", "score": 22.0},
                "challengers": [{"policy_name": "high_voi_aggressive", "score": 18.0}],
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "route_state_machine.json").write_text(
        json.dumps(
            {
                "states": [
                    {
                        "route_key": "CLSK|trend_pullback|late",
                        "state": "scaling",
                        "next_action": "increase_budget",
                        "freshness_score": 80.0,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "negative_knowledge_bank.json").write_text(
        json.dumps(
            {
                "patterns": [
                    {
                        "pattern_key": "neg1",
                        "route_key": "BAD|route|x",
                        "reason": "promotion_rejected",
                        "severity": "hard_avoid",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "mutation_grammar_learner.json").write_text(
        json.dumps(
            {
                "grammar": [
                    {
                        "grammar_id": "gram1",
                        "route_key": "CLSK|trend_pullback|late",
                        "treatment": "holdout_repair",
                        "mutation_radius": "tight",
                        "learning_score": 42.0,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "self_competition_league.json").write_text(
        json.dumps(
            {
                "leaderboard": [
                    {
                        "rank": 1,
                        "participant_key": "policy:truth_first_conservative",
                        "participant_type": "policy",
                        "name": "truth_first_conservative",
                        "rating": 1550.0,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "false_lesson_detector.json").write_text(
        json.dumps(
            {
                "lessons": [
                    {
                        "variant": "router_1_CLSK|trend_pullback|late",
                        "route_key": "CLSK|trend_pullback|late",
                        "false_lesson_risk_score": 45.0,
                        "advice": "treat_as_hypothesis_not_rule",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "uncertainty_budgeting.json").write_text(
        json.dumps({"budgets": [{"kind": "route", "route_key": "CLSK|trend_pullback|late", "uncertainty": 0.6, "recommended_budget": "learn"}]}),
        encoding="utf-8",
    )
    (run_dir / "evidence_contract_engine.json").write_text(
        json.dumps(
            {
                "contracts": [
                    {
                        "contract_id": "contract1",
                        "belief_type": "route_state",
                        "belief": "Route is scaling",
                        "status": "scaling",
                        "scope": {"route_key": "CLSK|trend_pullback|late"},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "research_trace_ledger.json").write_text(
        json.dumps(
            {
                "entries": [
                    {
                        "trace_id": "trace1",
                        "kind": "resolved_action",
                        "subject": "CLSK|trend_pullback|late",
                        "decision": "scale",
                        "confidence": 0.75,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "command_replay_ledger.json").write_text(
        json.dumps(
            {
                "entries": [
                    {
                        "command_id": "cmd1",
                        "action": "revalidate",
                        "route_key": "CLSK|trend_pullback|late",
                        "outcome": {"reward_score": 12.0, "worked": True},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "action_elo_league.json").write_text(
        json.dumps(
            {
                "leaderboard": [
                    {
                        "action": "revalidate",
                        "rating": 1542.0,
                        "sample_count": 1,
                        "success_rate": 1.0,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "command_outcome_backfill.json").write_text(
        json.dumps(
            {
                "outcomes": [
                    {
                        "command_id": "cmd1",
                        "action": "revalidate",
                        "route_key": "CLSK|trend_pullback|late",
                        "reward_score": 9.5,
                        "worked": True,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "action_reward_calibration.json").write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "action": "revalidate",
                        "predicted_reward": 6.0,
                        "observed_reward": 9.5,
                        "abs_error": 3.5,
                        "sample_count": 1,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "walk_forward_validation_bundle.json").write_text(
        json.dumps(
            {
                "source": "step2_profit_combo_hunter",
                "validations": [
                    {
                        "variant": "router_1_CLSK|trend_pullback|late",
                        "route_key": "CLSK|trend_pullback|late",
                        "pnl": 120.0,
                        "passed": True,
                        "checks": {
                            "beats_active_pnl": True,
                            "leave_one_day_ok": True,
                            "ticker_rotation_ok": False,
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "ab_route_experiment_executor.json").write_text(
        json.dumps(
            {
                "assignments": [
                    {
                        "assignment_id": "ab1",
                        "experiment_id": "exp1",
                        "worker": "worker_1",
                        "route_key": "CLSK|trend_pullback|late",
                        "treatment": "narrow_mutation",
                        "priority_score": 12.0,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "learning_rate_controller.json").write_text(
        json.dumps({"mode": "exploit_with_challenger", "batch_size_multiplier": 1.15}),
        encoding="utf-8",
    )
    (run_dir / "recovery_playbook_generator.json").write_text(
        json.dumps(
            {
                "interventions": [
                    {
                        "intervention_id": "rec1",
                        "action": "force_structural_mutation",
                        "route_key": "CLSK|trend_pullback|late",
                        "priority_score": 88.0,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "alias_trap_detector.json").write_text(
        json.dumps({"traps": [{"route_key": "CLSK|trend_pullback|late", "alias_pressure": 0.9}]}),
        encoding="utf-8",
    )
    (run_dir / "bandit_allocation.json").write_text(
        json.dumps({"allocation": [{"route_key": "CLSK|trend_pullback|late", "recommended_budget_pct": 80, "reward_score": 10}]}),
        encoding="utf-8",
    )
    feedback = tmp_path / "feedback.json"
    feedback.write_text(
        json.dumps({"feedback": [{"variant": "router_1_CLSK|trend_pullback|late", "route_key": "CLSK|trend_pullback|late", "status": "rejected"}]}),
        encoding="utf-8",
    )
    validation = tmp_path / "validation.json"
    validation.write_text(
        json.dumps(
            {
                "kind": "counterfactual",
                "results": [
                    {
                        "variant": "router_1_CLSK|trend_pullback|late",
                        "route_key": "CLSK|trend_pullback|late",
                        "baseline_step2_pnl": 120.0,
                        "route_disabled_delta": -10.0,
                        "causal_route_signal": True,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    db = step2_learning_db.Step2LearningDB(tmp_path / "learning.sqlite")
    try:
        stats = db.ingest_run_dir(run_dir)
        fb = db.ingest_feedback_file(feedback)
        vr = db.ingest_validation_results(validation)
        final = db.stats(run_id=run_dir.name)
        memory = db.cross_run_route_memory()
        priors = db.route_prior_model()
        controls = db.adaptive_mutation_controller()
        drift = db.drift_detection()
        lineage = db.candidate_lineage_graph()
        regime = db.regime_aware_bandit()
        packet = db.next_experiment_packet()
        controller = db.closed_loop_controller()
        experiments = db.causal_experiment_registry()
        debt = db.experiment_debt_queue()
        strategy = db.meta_strategy_learner()
        global_memory = db.global_learning_memory()
        calibration = db.calibration_persistence()
        belief_calibration = db.belief_calibration_persistence()
        learning_control = db.learning_control_plane_memory()
        learning_depth = db.learning_depth_memory()
        ops_hardening = db.ops_hardening_memory()
        temporal = db.temporal_decay_memory()
        revalidation = db.revalidation_candidates()
        lab_memory = db.research_lab_memory()
        correction_memory = db.self_correction_memory()
        pressure_memory = db.pressure_science_memory()
        closed_loop = db.closed_loop_execution_memory()
        opening = db.next_run_opening_controls()
        reconciled = db.outcome_reconciliation()
        cycle_write = db.record_cycle(run_dir.name, {"cycle": 1, "hunter": "router", "ok": True})
        event = {"event_type": "hunter_batch_telemetry", "route_key": "CLSK|trend_pullback|late", "batch": 1}
        event_write = db.record_online_event(run_dir.name, event)
        event_write_again = db.record_online_event(run_dir.name, dict(event))
        with_online = db.stats(run_id=run_dir.name)
    finally:
        db.close()

    assert stats["candidates"] == 1
    assert stats["route_arms"] == 1
    assert stats["run_data_quality"] == 1
    assert fb["inserted"] == 1
    assert vr["inserted"] == 1
    assert final["promotion_feedback"] == 1
    assert final["validation_results"] == 2
    assert memory["routes"][0]["route_key"] == "CLSK|trend_pullback|late"
    assert priors["priors"][0]["route_key"] == "CLSK|trend_pullback|late"
    assert controls["controls"]
    assert "routes" in drift
    assert lineage["nodes"]
    assert regime["regimes"]
    assert "hunt_next" in packet
    assert "closed_loop_controller" in packet
    assert controller["route_controller"]["route_count"] >= 1
    assert "closed_loop_controller" in global_memory
    assert "belief_calibration_persistence" in global_memory
    assert "learning_control_plane_memory" in global_memory
    assert "learning_depth_memory" in global_memory
    assert "ops_hardening_memory" in global_memory
    assert experiments["experiments"][0]["experiment_id"] == "exp1"
    assert debt["queue"][0]["question"] == "Does holdout repair work here?"
    assert strategy["strategies"]
    assert "next_experiment_packet" in global_memory
    assert global_memory["temporal_decay_memory"]["routes"]
    assert calibration["pending_predictions"] >= 1
    assert belief_calibration["belief_count"] == 2
    assert belief_calibration["resolved_predictions"] >= 1
    assert belief_calibration["learning_adjustments"]["state"] in {"trusted", "usable_with_caution", "recalibrate_before_scaling"}
    assert learning_control["item_count"] >= 5
    assert any(row["artifact_type"] == "active_experiment_design" for row in learning_control["top_items"])
    assert learning_depth["item_count"] >= 5
    assert any(row["artifact_type"] == "portfolio_learning" for row in learning_depth["top_items"])
    assert ops_hardening["item_count"] >= 6
    assert any(row["artifact_type"] == "rollback_kill_switch" for row in ops_hardening["top_items"])
    assert temporal["routes"][0]["route_key"] == "CLSK|trend_pullback|late"
    assert temporal["policies"][0]["policy_name"] == "truth_first_conservative"
    assert revalidation["queue"]
    assert lab_memory["route_lifecycle"][0]["state"] == "scaling"
    assert lab_memory["negative_knowledge"][0]["pattern_key"] == "neg1"
    assert lab_memory["mutation_grammar"][0]["grammar_id"] == "gram1"
    assert correction_memory["policy_league"][0]["participant_key"] == "policy:truth_first_conservative"
    assert correction_memory["self_corrections"]
    assert pressure_memory["evidence_contracts"][0]["contract_id"] == "contract1"
    assert pressure_memory["research_traces"][0]["trace_id"] == "trace1"
    assert closed_loop["commands"][0]["command_id"] == "cmd1"
    assert closed_loop["action_league"][0]["action"] == "revalidate"
    assert closed_loop["command_outcomes"][0]["command_id"] == "cmd1"
    assert closed_loop["reward_calibration"][0]["action"] == "revalidate"
    assert closed_loop["runtime_experiments"]
    assert opening["memory_reliability_scorer"]["scores"]
    assert opening["memory_provenance_explorer"]["entries"]
    assert opening["memory_qa_smoke_test"]["passed"] is True
    assert opening["pre_hunt_strategy_selector"]["strategy"]
    assert "hunt_opening_playbook" in opening
    assert "cross_run_learning_regression_test" in opening
    assert reconciled["reconciled"] >= 1
    assert stats["causal_experiments"] == 1
    assert stats["experiment_debt"] == 1
    assert stats["calibration_predictions"] >= 1
    assert stats["belief_calibration_memory"] == 2
    assert stats["learning_control_memory"] >= 5
    assert stats["learning_depth_memory"] >= 5
    assert stats["ops_hardening_memory"] >= 6
    assert stats["temporal_route_memory"] == 1
    assert stats["temporal_policy_memory"] >= 2
    assert stats["route_lifecycle_memory"] == 1
    assert stats["negative_knowledge"] == 1
    assert stats["mutation_grammar_memory"] == 1
    assert stats["policy_league_memory"] == 1
    assert stats["self_correction_memory"] >= 1
    assert stats["evidence_contract_memory"] == 1
    assert stats["research_trace_memory"] == 1
    assert stats["command_replay_memory"] == 1
    assert stats["action_league_memory"] == 1
    assert stats["command_outcome_memory"] == 1
    assert stats["action_reward_calibration_memory"] == 1
    assert stats["runtime_experiment_memory"] >= 4
    assert cycle_write["recorded"] is True
    assert event_write["recorded"] is True
    assert event_write_again["recorded"] is True
    assert with_online["cycles"] == 2
    assert with_online["online_events"] == 1


def test_learning_db_ingests_lesson_half_life_temporal_memory():
    tmp_path = _workspace_tmp("learning_db_lesson_half_life")
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "summary.json").write_text(json.dumps({"source": "test"}), encoding="utf-8")
    (run_dir / "lesson_half_life_report.json").write_text(
        json.dumps(
            {
                "source": "step2_profit_combo_hunter",
                "lessons": [
                    {
                        "subject": "route:score:MARA|trend_pullback|open|SHORT",
                        "memory_type": "route_prior",
                        "confidence": 0.75,
                        "half_life_state": "fresh",
                        "refresh_action": "keep_in_memory",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    db = step2_learning_db.Step2LearningDB(tmp_path / "learning.sqlite")
    try:
        stats = db.ingest_run_dir(run_dir)
        temporal = db.temporal_decay_memory()
    finally:
        db.close()

    assert stats["temporal_route_memory"] == 1
    assert temporal["routes"][0]["route_key"] == "MARA|trend_pullback|open|SHORT"
    assert temporal["routes"][0]["decayed_confidence"] > 0
