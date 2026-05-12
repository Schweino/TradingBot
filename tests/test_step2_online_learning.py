import step2_hunt_intelligence as hunt_intel
import step2_online_learning
from pathlib import Path
from uuid import uuid4


def _workspace_tmp(name):
    path = Path("runtime") / "unit_test_tmp" / f"{name}_{uuid4().hex}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_online_state_updates_route_arms_and_triage():
    tmp_path = _workspace_tmp("online_state")
    row = {
        "variant": "router_1_CLSK|trend_pullback|late",
        "weights": {"ema": 1.0, "setup_trend_pullback": 3.0},
        "bias": 0.0,
        "routes": [{"match": {"ticker": "CLSK", "setup_type": "trend_pullback", "session_phase": "late"}}],
        "step2_pnl": 120.0,
        "step2_delta_vs_active": 5.0,
        "step2_trades": 100,
        "step2_win_rate_pct": 60.0,
        "by_day": {"2026-05-01": {"pnl": 120.0, "trades": 100}},
        "by_ticker": {"CLSK": {"pnl": 120.0, "trades": 100}},
    }
    ranked = hunt_intel.rank_rows([row], live_only=True, behavioral_dedupe=True, limit=10)
    state = step2_online_learning.update_online_state(
        run_dir=tmp_path,
        rankings=ranked,
        cycles=[{"cycle": 0, "hunter": "router", "stdout_tail": '{"scored_total":100,"winners":1}'}],
        novelty_budget_pct=15.0,
    )

    assert state["route_arms"][0]["route_key"] == "CLSK|trend_pullback|late"
    assert state["candidate_triage"][0]["status"] in {"validate_now", "keep_hunting", "likely_alias", "likely_overfit"}
    assert (tmp_path / "online_state.json").exists()
    assert (tmp_path / "learning_events.jsonl").exists()


def test_streaming_telemetry_updates_controls_and_interrupts():
    tmp_path = _workspace_tmp("streaming_telemetry")
    events = [
        {
            "event": "router_batch_telemetry",
            "batch": 1,
            "best_pnl": 10.0,
            "best_delta_vs_active": 0.0,
            "live_beaters": 0,
            "novelty_yield": 0,
            "alias_rate": 0.9,
            "alias_count": 9,
            "route_seeds": ["CLSK|trend_pullback|late"],
            "top_failure_reason": "no_live_beaters",
        },
        {
            "event": "router_batch_telemetry",
            "batch": 2,
            "best_pnl": 11.0,
            "best_delta_vs_active": 0.0,
            "live_beaters": 0,
            "novelty_yield": 0,
            "alias_rate": 0.95,
            "alias_count": 10,
            "route_seeds": ["CLSK|trend_pullback|late"],
            "top_failure_reason": "no_live_beaters",
        },
        {
            "event": "router_batch_telemetry",
            "batch": 3,
            "best_pnl": 12.0,
            "best_delta_vs_active": 0.0,
            "live_beaters": 0,
            "novelty_yield": 0,
            "alias_rate": 0.91,
            "alias_count": 11,
            "route_seeds": ["CLSK|trend_pullback|late"],
            "top_failure_reason": "no_live_beaters",
        },
    ]

    state = step2_online_learning.update_state_from_telemetry(
        run_dir=tmp_path,
        telemetry_events=events,
        novelty_budget_pct=15.0,
    )
    state["batch_size_multiplier"] = step2_online_learning.batch_size_multiplier(state)
    should_stop, reason = step2_online_learning.should_interrupt_from_telemetry(state, events, min_events=3)
    quarantine = step2_online_learning.quarantine_payload(state)

    assert state["route_arms"][0]["route_key"] == "CLSK|trend_pullback|late"
    assert state["streaming_telemetry"]["alias_rate"] > 0.9
    assert state["batch_size_multiplier"] == 0.6
    assert should_stop is True
    assert reason in {"no_live_or_novelty_yield", "alias_rate_high_without_novelty", "active_basin_exhausted"}
    assert "CLSK|trend_pullback|late" in quarantine["skip_routes"]


def test_promotion_review_feedback_drives_repair_directives():
    state = step2_online_learning.apply_promotion_review_feedback(
        {
            "route_arms": [
                {
                    "route_key": "CLSK|trend_pullback|late",
                    "attempts": 1,
                    "alias_count": 0,
                    "novelty_yield": 1,
                    "best_delta_vs_active": 100.0,
                }
            ]
        },
        [
            {
                "variant": "candidate",
                "route_key": "CLSK|trend_pullback|late",
                "status": "reject",
                "reject_reasons": ["robustness_score_below_70"],
                "learning_tags": ["thin_holdout_edge", "small_total_edge"],
            }
        ],
    )
    controls = step2_online_learning._mutation_controls(state["route_arms"], state)
    quarantine = step2_online_learning.quarantine_payload(state)

    assert state["promotion_review_directives"][0]["action"] == "repair_holdout"
    assert controls["CLSK|trend_pullback|late"]["reason"] == "promotion_review_holdout_repair"
    assert "CLSK|trend_pullback|late" in quarantine["holdout_repair_routes"]


def test_family_rejection_memory_marks_descendants_for_caution():
    row = {
        "variant": "child",
        "weights": {"ema": 1.0},
        "bias": 0.0,
        "step2_pnl": 120.0,
        "step2_delta_vs_active": 20.0,
        "step2_trades": 100,
        "by_day": {"2026-05-01": {"pnl": 120.0, "trades": 100}},
        "by_ticker": {"CLSK": {"pnl": 120.0, "trades": 100}},
        "family_key": "fam1",
    }
    ranked = hunt_intel.rank_rows([row], live_only=True, behavioral_dedupe=True, limit=10)
    ranked["raw_leaderboard"][0]["family_key"] = "fam1"
    state = step2_online_learning.update_online_state(
        run_dir=_workspace_tmp("family_feedback"),
        rankings=ranked,
        cycles=[],
        promotion_feedback_rows=[{
            "variant": "rejected_parent",
            "family_key": "fam1",
            "route_key": "CLSK|trend_pullback|late",
            "status": "reject",
            "reject_reasons": ["robustness_score_below_70"],
        }],
    )

    assert state["family_review_directives"][0]["action"] == "downweight_family"
    assert state["candidate_triage"][0]["status"] == "family_caution"
    assert "fam1" in step2_online_learning.quarantine_payload(state)["downweight_families"]


def test_experiment_plan_becomes_online_directive():
    state = step2_online_learning.apply_experiment_plan(
        {},
        {
            "experiments": [
                {
                    "experiment_id": "exp1",
                    "kind": "ab_mutation_radius",
                    "hypothesis": "test narrow vs wide",
                    "route_key": "CLSK|trend_pullback|late",
                    "family_key": "fam1",
                    "worker_assignment": "worker_1",
                    "treatments": [{"name": "holdout", "mutation_lane": "holdout_repair"}],
                    "success_metric": "readiness",
                    "stop_rule": "stop later",
                }
            ]
        },
    )
    controls = step2_online_learning._mutation_controls(
        [{"route_key": "CLSK|trend_pullback|late", "attempts": 1, "alias_count": 0, "novelty_yield": 1}],
        state,
    )
    quarantine = step2_online_learning.quarantine_payload(state)

    assert state["experiment_directive"]["experiment_id"] == "exp1"
    assert controls["CLSK|trend_pullback|late"]["reason"] == "active_experiment_holdout_repair"
    assert quarantine["experiment_focus_families"] == ["fam1"]


def test_treatment_priors_become_online_budget_and_controls():
    state = step2_online_learning.apply_experiment_plan(
        {},
        {
            "experiments": [
                {
                    "experiment_id": "exp1",
                    "route_key": "CLSK|trend_pullback|late",
                    "family_key": "fam1",
                    "treatments": [{"name": "holdout", "mutation_lane": "holdout_repair"}],
                }
            ]
        },
    )
    state = step2_online_learning.apply_treatment_priors(
        state,
        {
            "best_treatment": "holdout_repair",
            "worst_treatment": "wide_mutation",
            "priors": [{"treatment": "holdout_repair", "prior_weight": 1.4, "confidence": 0.7}],
        },
        {
            "best_treatment": "holdout_repair",
            "worst_treatment": "wide_mutation",
            "workers": [{"worker": "worker_1", "treatment": "holdout_repair", "recommended_budget_pct": 60.0}],
        },
    )
    controls = step2_online_learning._mutation_controls(
        [{"route_key": "CLSK|trend_pullback|late", "attempts": 1, "alias_count": 0, "novelty_yield": 1}],
        state,
    )
    quarantine = step2_online_learning.quarantine_payload(state)

    assert state["best_treatment"] == "holdout_repair"
    assert quarantine["treatment_budget"]["best_treatment"] == "holdout_repair"
    assert controls["CLSK|trend_pullback|late"]["reason"] == "best_treatment_holdout_repair"
    assert controls["CLSK|trend_pullback|late"]["treatment_prior"]["prior_weight"] == 1.4


def test_learning_upgrade_controls_reach_quarantine_payload():
    state = step2_online_learning.apply_learning_upgrade_controls(
        {},
        treatment_confidence={
            "scale": [{"treatment": "holdout_repair"}],
            "abandon": [{"treatment": "wide_mutation"}],
        },
        controlled_sibling_experiments={
            "experiments": [{"experiment_id": "sib1", "kind": "controlled_parent_sibling"}],
        },
        worker_specialization={
            "workers": [{"worker": "worker_2", "recommended_role": "holdout_repair"}],
        },
        regime_learning={"regimes": [{"regime_key": "CLSK|trend_pullback|late|stable"}]},
        promotion_reject_simulator={"predictions": [{"variant": "candidate", "predicted_reject_probability": 0.7}]},
        search_portfolio={"allocations": [{"bucket": "repair_near_misses", "budget_pct": 35.0}]},
        causal_experiment_registry={"experiments": [{"experiment_id": "exp1"}]},
        experiment_debt_queue={"queue": [{"question": "repair this family"}]},
        information_gain_scoring={"actions": [{"action": "probe", "target": "holdout_repair"}]},
        value_of_information_planner={"plans": [{"action": "probe", "target": "holdout_repair"}]},
        decision_change_tracker={"changes": [{"type": "treatment_decision"}]},
        hypothesis_quality_scoring={"hypotheses": [{"experiment_id": "exp1", "quality_score": 80.0}]},
        evidence_sufficiency_gate={"scale_allowed": [{"treatment": "holdout_repair"}]},
        counterfactual_shadow_board={"tasks": [{"check": "disable_route"}]},
        learning_velocity_dashboard={"learning_velocity_score": 12.0},
        prediction_calibration_ledger={"calibration_confidence": 0.7},
        belief_revision_engine={"truth_calibration_confidence": 0.65},
        adversarial_red_team_learner={"tasks": [{"attack": "ood_stress_replay"}]},
        out_of_distribution_detector={"detections": [{"variant": "candidate", "ood_score": 50.0}]},
        memory_compression_distiller={"durable_rules": ["Prefer holdout repair"]},
        self_audit_score={"self_audit_score": 88.0},
        truth_first_promotion_objective={"leaderboard": [{"variant": "candidate", "truth_first_promotable_pnl": 123.0}]},
        compiled_hunt_policy={"objective": "maximize_truth_first_promotable_pnl"},
        policy_executor={"assignments": [{"kind": "value_of_information", "target": "holdout_repair"}], "focus_routes": ["CLSK|trend_pullback|late"]},
        adaptive_worker_assignment={"assignments": [{"worker": "worker_1", "recommended_role": "value_of_information"}]},
        policy_backtester={"verdict": "policy_improves_selection"},
        policy_mutation_engine={"policies": [{"policy_name": "truth_first_conservative"}]},
        policy_tournament={"champion": {"policy_name": "truth_first_conservative"}},
        champion_challenger_memory={"champion": {"policy_name": "truth_first_conservative"}},
        regime_specific_policies={"policies": [{"regime_key": "CLSK|trend_pullback|late|stable", "policy_name": "repair_heavy"}]},
        causal_graph_of_learning={"nodes": [{"id": "policy:x"}], "edges": []},
        policy_safety_rail={"ok": True},
        auto_promoted_field_manual={"rules": [{"rule": "Default to truth-first", "status": "promoted_rule"}]},
        policy_drift_detector={"status": "watch", "reasons": ["calibration_confidence_low"]},
        route_regime_half_life={"routes": [{"route_key": "CLSK|trend_pullback|late", "recommended_action": "boost"}]},
        learning_market_map={"cells": [{"cell": "CLSK|trend_pullback|late::holdout_repair", "learning_score": 42.0}]},
        concept_drift_alarms={"alarm_count": 1, "alarms": [{"alarm": "policy_champion_drift"}]},
        revalidation_scheduler={"queue": [{"task": "revalidate_champion_policy"}]},
        temporal_ensemble_policy={
            "primary_policy": "truth_first_conservative",
            "components": [{"policy_name": "truth_first_conservative", "weight": 0.7}],
        },
        active_experiment_governor={"top_decision": {"decision": "expand"}, "decisions": [{"decision": "expand"}]},
        route_state_machine={"focus_routes": ["CLSK|trend_pullback|late"], "states": [{"route_key": "CLSK|trend_pullback|late", "state": "scaling"}]},
        negative_knowledge_bank={"avoid_routes": ["BAD|route|x"], "patterns": [{"pattern_key": "p1"}]},
        promotion_survivor_model={"promotion_survivor_top10": [{"variant": "candidate", "survival_probability": 0.8}]},
        mutation_grammar_learner={"top_templates": [{"template": "tight_mutation::CLSK"}]},
        real_time_worker_rebalancer={"assignments": [{"worker": "worker_1", "role": "repair"}]},
        hunt_replay_simulator={"recommended_policy": "truth_first_conservative", "simulations": [{"policy_name": "truth_first_conservative"}]},
        resurrection_engine={"top_resurrection": {"variant": "candidate"}, "repair_queue": [{"variant": "candidate"}]},
        causal_mutation_attribution={"top_parameter_moves": [{"route_key": "CLSK|trend_pullback|late", "expected_pnl_lift": 12.0}]},
        uncertainty_budgeting={"top_budget": {"kind": "route", "route_key": "CLSK|trend_pullback|late"}, "budgets": [{"kind": "route"}]},
        promotability_pareto_frontier={"frontier": [{"variant": "candidate", "frontier_score": 99.0}]},
        false_lesson_detector={"highest_risk": {"variant": "candidate", "false_lesson_risk_score": 50.0}},
        experiment_graduation_system={"stage_counts": {"confirmed": 1}, "experiments": [{"experiment_id": "exp1", "stage": "confirmed"}]},
        candidate_genealogy_diff_engine={"diffs": [{"child_variant": "candidate", "human_summary": "tighten x"}]},
        off_policy_hunt_evaluator={"recommended_policy": "truth_first_conservative", "evaluations": [{"policy_name": "truth_first_conservative"}]},
        self_competition_league={"champion": {"name": "truth_first_conservative"}, "leaderboard": [{"participant_key": "policy:truth_first_conservative"}]},
        evidence_contract_engine={"contract_count": 1, "contracts": [{"contract_id": "c1"}]},
        live_beater_quality_decomposer={"top_quality": {"variant": "candidate"}, "candidates": [{"variant": "candidate"}]},
        contradiction_detector={"contradiction_count": 1, "contradictions": [{"kind": "scale_vs_false_lesson"}]},
        learning_conflict_resolver={"top_action": {"final_action": "revalidate"}, "actions": [{"final_action": "revalidate"}]},
        cohort_based_memory={"top_cohort": {"cohort_key": "cohort1"}, "cohorts": [{"cohort_key": "cohort1"}]},
        adaptive_hunt_throttle={"controls": {"batch_size_multiplier": 0.65, "mutation_width": "tight"}},
        promotion_readiness_simulator={"highest_reject_risk": {"variant": "candidate"}, "simulations": [{"variant": "candidate"}]},
        research_trace_ledger={"entry_count": 2, "entries": [{"trace_id": "t1"}]},
        runtime_decision_kernel={
            "command_packet": {"live_only": True, "focus_routes": ["CLSK|trend_pullback|late"], "commands": [{"command_id": "cmd1", "action": "revalidate", "route_key": "CLSK|trend_pullback|late"}]},
            "commands": [{"command_id": "cmd1", "action": "revalidate", "route_key": "CLSK|trend_pullback|late"}],
        },
        action_outcome_tracker={"outcomes": [{"command_id": "cmd1", "action": "revalidate", "worked": True}]},
        closed_loop_reward_model={"best_action": {"action": "revalidate"}, "action_rewards": [{"action": "revalidate"}]},
        autonomous_hunt_planner={"next_job": {"action": "revalidate", "route_key": "CLSK|trend_pullback|late"}, "jobs": [{"action": "revalidate"}]},
        runtime_guardrails={"ok": True, "guarded_command_packet": {"live_only": True, "focus_routes": ["CLSK|trend_pullback|late"], "commands": []}},
        command_replay_ledger={"entry_count": 1, "entries": [{"command_id": "cmd1"}]},
        action_elo_league={"champion": {"action": "revalidate"}, "leaderboard": [{"action": "revalidate"}]},
        human_readable_hunt_brief={"brief": "Next job: revalidate on CLSK|trend_pullback|late."},
        meta_hunt_strategy={"strategies": [{"strategy": "router_heavy"}]},
        run_to_run_postmortem={"memo": ["learned something"]},
    )
    state.update({
        "ab_route_experiment_executor": {"assignment_count": 1, "focus_routes": ["CLSK|trend_pullback|late"], "assignments": [{"assignment_id": "ab1", "worker": "worker_1", "route_key": "CLSK|trend_pullback|late", "treatment": "narrow_mutation"}]},
        "champion_challenger_runtime_slots": {"slots": [{"slot": "champion", "worker": "worker_1", "command": {"action": "revalidate", "route_key": "CLSK|trend_pullback|late"}}]},
        "adaptive_experiment_stopping": {"stop_count": 0, "decisions": [{"experiment_id": "exp1", "decision": "continue"}]},
        "counterfactual_command_replay": {"best_counterfactual": {"counterfactual_action": "repair"}, "simulations": [{"counterfactual_action": "repair"}]},
        "experiment_contamination_guard": {"ok": True, "safe_focus_routes": ["CLSK|trend_pullback|late"], "risks": []},
        "learning_rate_controller": {"mode": "exploit_with_challenger", "batch_size_multiplier": 1.1},
        "worker_learning_report_cards": {"top_worker": {"worker": "worker_1"}, "cards": [{"worker": "worker_1"}]},
        "experiment_to_promotion_trace": {"top_trace": {"variant": "candidate"}, "traces": [{"variant": "candidate"}]},
        "zero_yield_autopsy_engine": {"zero_yield_count": 1, "top_autopsy": {"primary_reason": "alias_trap"}, "autopsies": [{"primary_reason": "alias_trap", "route_seeds": ["CLSK|trend_pullback|late"]}]},
        "stuck_loop_breaker": {"reset_count": 1, "force_widen_routes": ["CLSK|trend_pullback|late"], "resets": [{"kind": "route_reset"}]},
        "opportunity_cost_meter": {"highest_cost": {"route_key": "OLD|route|x"}, "reduce_budget_routes": ["OLD|route|x"], "costs": [{"route_key": "OLD|route|x"}]},
        "search_space_coverage_map": {"blind_spots": [{"route_key": "NEW|route|x"}], "route_coverage": {"CLSK|trend_pullback|late": 3}},
        "live_beater_scarcity_mode": {"enabled": True, "mode": "broad_discovery", "mutation_width": "wide", "batch_size_multiplier": 1.25, "focus_routes": ["NEW|route|x"]},
        "alias_trap_detector": {"trap_count": 1, "structural_mutation_routes": ["CLSK|trend_pullback|late"], "traps": [{"route_key": "CLSK|trend_pullback|late"}]},
        "route_seed_quality_score": {"top_seed": {"route_key": "CLSK|trend_pullback|late"}, "clone_routes": ["CLSK|trend_pullback|late"], "retire_routes": ["OLD|route|x"], "scores": [{"route_key": "CLSK|trend_pullback|late"}]},
        "recovery_playbook_generator": {"top_intervention": {"action": "force_structural_mutation"}, "focus_routes": ["NEW|route|x"], "skip_routes": ["OLD|route|x"], "structural_mutation_routes": ["CLSK|trend_pullback|late"], "interventions": [{"intervention_id": "rec1", "action": "force_structural_mutation", "route_key": "CLSK|trend_pullback|late", "mutation_width": "wide"}]},
    })
    state["runtime_command_adapter"] = step2_online_learning.runtime_command_adapter(state)
    state["worker_job_contracts"] = step2_online_learning.planner_worker_contracts(state)
    quarantine = step2_online_learning.quarantine_payload(state)

    assert state["learning_upgrade_directive"]["scale_treatments"] == ["holdout_repair"]
    assert state["learning_upgrade_directive"]["abandon_treatments"] == ["wide_mutation"]
    assert state["learning_upgrade_directive"]["next_information_gain_action"]["action"] == "probe"
    assert state["learning_upgrade_directive"]["next_value_of_information_plan"]["action"] == "probe"
    assert state["learning_upgrade_directive"]["compiled_policy_objective"] == "maximize_truth_first_promotable_pnl"
    assert state["learning_upgrade_directive"]["next_policy_job"]["kind"] == "value_of_information"
    assert state["learning_upgrade_directive"]["policy_backtest_verdict"] == "policy_improves_selection"
    assert state["learning_upgrade_directive"]["policy_champion"] == "truth_first_conservative"
    assert state["learning_upgrade_directive"]["policy_safety_ok"] is True
    assert state["learning_upgrade_directive"]["promoted_rules"][0]["status"] == "promoted_rule"
    assert state["learning_upgrade_directive"]["truth_calibration_confidence"] == 0.65
    assert state["learning_upgrade_directive"]["truth_first_rank1"]["variant"] == "candidate"
    assert state["learning_upgrade_directive"]["next_experiment_debt"]["question"] == "repair this family"
    assert state["learning_upgrade_directive"]["policy_drift_status"] == "watch"
    assert state["learning_upgrade_directive"]["next_revalidation"]["task"] == "revalidate_champion_policy"
    assert state["learning_upgrade_directive"]["temporal_ensemble_primary"] == "truth_first_conservative"
    assert state["learning_upgrade_directive"]["experiment_governor_top_decision"]["decision"] == "expand"
    assert state["learning_upgrade_directive"]["route_state_focus"] == ["CLSK|trend_pullback|late"]
    assert state["learning_upgrade_directive"]["negative_avoid_routes"] == ["BAD|route|x"]
    assert state["learning_upgrade_directive"]["replay_recommended_policy"] == "truth_first_conservative"
    assert state["learning_upgrade_directive"]["top_resurrection"]["variant"] == "candidate"
    assert state["learning_upgrade_directive"]["uncertainty_top_budget"]["route_key"] == "CLSK|trend_pullback|late"
    assert state["learning_upgrade_directive"]["pareto_frontier_top"]["variant"] == "candidate"
    assert state["learning_upgrade_directive"]["false_lesson_highest_risk"]["variant"] == "candidate"
    assert state["learning_upgrade_directive"]["experiment_stage_counts"]["confirmed"] == 1
    assert state["learning_upgrade_directive"]["off_policy_recommended_policy"] == "truth_first_conservative"
    assert state["learning_upgrade_directive"]["self_competition_champion"]["name"] == "truth_first_conservative"
    assert state["learning_upgrade_directive"]["evidence_contract_count"] == 1
    assert state["learning_upgrade_directive"]["top_quality_decomposition"]["variant"] == "candidate"
    assert state["learning_upgrade_directive"]["contradiction_count"] == 1
    assert state["learning_upgrade_directive"]["top_conflict_resolution"]["final_action"] == "revalidate"
    assert state["learning_upgrade_directive"]["top_cohort"]["cohort_key"] == "cohort1"
    assert state["learning_upgrade_directive"]["adaptive_throttle"]["mutation_width"] == "tight"
    assert state["learning_upgrade_directive"]["promotion_readiness_highest_risk"]["variant"] == "candidate"
    assert state["learning_upgrade_directive"]["research_trace_count"] == 2
    assert state["learning_upgrade_directive"]["runtime_command_count"] == 1
    assert state["learning_upgrade_directive"]["runtime_guardrails_ok"] is True
    assert state["learning_upgrade_directive"]["next_runtime_job"]["action"] == "revalidate"
    assert state["learning_upgrade_directive"]["best_closed_loop_action"]["action"] == "revalidate"
    assert state["learning_upgrade_directive"]["action_league_champion"]["action"] == "revalidate"
    assert "Next job" in state["learning_upgrade_directive"]["hunt_brief"]
    assert state["learning_upgrade_directive"]["top_hypothesis"]["hypothesis_id"]
    assert state["learning_upgrade_directive"]["top_hypothesis_quote"]["hypothesis_id"]
    assert "Believed:" in state["learning_upgrade_directive"]["hunt_narrative"]
    assert state["learning_upgrade_directive"]["attention_regret"]
    assert "Spent" in state["learning_upgrade_directive"]["spend_efficiency"]
    assert state["learning_upgrade_directive"]["fresh_idea_count"] >= 0
    assert state["learning_upgrade_directive"]["novelty_budget_pct"] is not None
    assert "Novelty budget" in state["learning_upgrade_directive"]["creative_brief"]
    assert state["learning_upgrade_directive"]["active_learning_modules"]
    assert state["learning_upgrade_directive"]["learning_system_health"] is not None
    assert "Pulling weight" in state["learning_upgrade_directive"]["meta_learning_brief"]
    assert state["learning_upgrade_directive"]["top_causal_intervention"]
    assert state["learning_upgrade_directive"]["experiment_power_score"] is not None
    assert state["learning_upgrade_directive"]["search_temperature"]
    assert state["learning_upgrade_directive"]["hunt_strategy_v2"]
    assert state["learning_upgrade_directive"]["top_surviving_lesson"]
    assert state["learning_upgrade_directive"]["promotion_rejection_backprop_count"] is not None
    assert state["learning_upgrade_directive"]["promotion_aware_top_candidate"]
    assert "Belief:" in state["learning_upgrade_directive"]["scientific_run_brief"]
    assert state["learning_upgrade_directive"]["strategy_genome_champion"]
    assert state["learning_upgrade_directive"]["top_strategy_challenger"]
    assert state["learning_upgrade_directive"]["strategy_tournament_champion"]
    assert state["learning_upgrade_directive"]["selected_strategy_regime"]
    assert state["learning_upgrade_directive"]["meta_objective"]
    assert state["learning_upgrade_directive"]["exploration_debt_count"] is not None
    assert state["learning_upgrade_directive"]["strategy_red_team_top_attack"]
    assert state["learning_upgrade_directive"]["autonomous_pivot"] is not None
    assert state["learning_upgrade_directive"]["top_learning_roi"]
    assert state["learning_upgrade_directive"]["decision_trace_count"] is not None
    assert state["learning_upgrade_directive"]["control_conflict_count"] is not None
    assert state["learning_upgrade_directive"]["simplified_runtime_width"]
    assert state["learning_upgrade_directive"]["ablation_replay_count"] >= 1
    assert state["learning_upgrade_directive"]["architecture_fitness_verdict"]
    assert state["learning_upgrade_directive"]["schema_registry_valid"] is not None
    assert state["learning_upgrade_directive"]["artifact_dependency_root_count"] >= 1
    assert state["learning_upgrade_directive"]["incremental_cache_reuse_count"] is not None
    assert state["learning_upgrade_directive"]["dashboard_status"] in {"healthy", "pivoting"}
    assert state["learning_upgrade_directive"]["runbook_worker_count"] >= 1
    assert state["learning_upgrade_directive"]["learning_failure_count"] is not None
    assert state["learning_upgrade_directive"]["warehouse_export_count"] >= 1
    assert state["learning_upgrade_directive"]["pre_hunt_ready"] is not None
    assert state["learning_upgrade_directive"]["online_causal_top_lane"]
    assert state["learning_upgrade_directive"]["online_causal_allocations"]
    assert state["learning_upgrade_directive"]["variant_dna_top_gene"]
    assert state["learning_upgrade_directive"]["negative_gene_suppressed_count"] is not None
    assert state["learning_upgrade_directive"]["winner_family_count"] >= 1
    assert state["learning_upgrade_directive"]["exploration_frontier_count"] >= 1
    assert state["learning_upgrade_directive"]["worker_personality_assignments"]
    assert state["learning_upgrade_directive"]["cycle_learning_delta"]
    assert "risk_score" in state["learning_upgrade_directive"]["promotion_rejection_v2_highest_risk"]
    assert "expected_live_lift" in state["learning_upgrade_directive"]["top_counterfactual"]
    assert state["learning_upgrade_directive"]["missed_winner_count"] is not None
    assert state["learning_upgrade_directive"]["top_causal_regret"] is not None
    assert state["learning_upgrade_directive"]["search_grammar_top_templates"] is not None
    assert state["learning_upgrade_directive"]["hypothesis_court_counts"]["scale"] is not None
    assert state["learning_upgrade_directive"]["route_interaction_v2_top"] is not None
    assert state["learning_upgrade_directive"]["promotion_shadow_top"] is not None
    assert state["learning_upgrade_directive"]["autopilot_policy_packet"]["live_only"] is True
    assert state["learning_upgrade_directive"]["verified_learning_claim_count"] is not None
    assert state["learning_upgrade_directive"]["causal_confidence_top"] is not None
    assert state["learning_upgrade_directive"]["false_discovery_warning_count"] is not None
    assert state["learning_upgrade_directive"]["evidence_threshold_mode"] in {"normal", "cautious", "strict"}
    assert state["learning_upgrade_directive"]["self_debate_resolution"] is not None
    assert state["learning_upgrade_directive"]["compressed_memory_rule_count"] is not None
    assert state["learning_upgrade_directive"]["learning_drift_count"] is not None
    assert state["learning_upgrade_directive"]["promotion_first_policy_packet"]["live_only"] is True
    assert state["learning_upgrade_directive"]["memory_horizon_summary"]
    assert state["learning_upgrade_directive"]["lesson_half_life_top"]
    assert state["learning_upgrade_directive"]["cross_hunt_replay_top"] is not None
    assert state["learning_upgrade_directive"]["temporal_regime_top"] is not None
    assert state["learning_upgrade_directive"]["longitudinal_survival_top"] is not None
    assert state["learning_upgrade_directive"]["memory_conflict_v2_count"] is not None
    assert state["learning_upgrade_directive"]["strategy_aging_counts"]
    assert state["learning_upgrade_directive"]["next_hunt_opening_policy"]["live_only"] is True
    assert state["learning_upgrade_directive"]["question_planner_top"]
    assert state["learning_upgrade_directive"]["expected_information_gain_top"] is not None
    assert state["learning_upgrade_directive"]["uncertainty_heatmap_top"] is not None
    assert state["learning_upgrade_directive"]["adaptive_experiment_next_step"] is not None
    assert state["learning_upgrade_directive"]["learning_value_stop_count"] is not None
    assert state["learning_upgrade_directive"]["causal_question_top"] is not None
    assert state["learning_upgrade_directive"]["epistemic_role_assignments"]
    assert state["learning_upgrade_directive"]["compiled_hypotheses"]
    assert state["learning_upgrade_directive"]["experiment_contract_top"]
    assert state["learning_upgrade_directive"]["control_route_top"] is not None
    assert state["learning_upgrade_directive"]["sequential_test_top"] is not None
    assert state["learning_upgrade_directive"]["causal_effect_top"] is not None
    assert state["learning_upgrade_directive"]["false_positive_pressure"]["mode"] in {"normal", "cautious_control", "strict_control"}
    assert state["learning_upgrade_directive"]["exploration_paydown_top"] is not None
    assert state["learning_upgrade_directive"]["promotion_power_top"] is not None
    assert state["learning_upgrade_directive"]["scientific_executive_summary"]
    assert state["learning_upgrade_directive"]["scientific_executive_top_command"] is not None
    assert state["learning_upgrade_directive"]["live_candidate_evidence_top"]
    assert state["learning_upgrade_directive"]["promotion_failure_v3_highest_risk"] is not None
    assert state["learning_upgrade_directive"]["evidence_gap_top_task"] is not None
    assert state["learning_upgrade_directive"]["review_ready_v2_top"] is not None
    assert state["learning_upgrade_directive"]["promotion_scorecard_top"] is not None
    assert state["learning_upgrade_directive"]["candidate_lineage_top"] is not None
    assert state["learning_upgrade_directive"]["live_control_differential_top"] is not None
    assert state["learning_upgrade_directive"]["promotion_packet_executive_summary"]
    assert state["learning_upgrade_directive"]["promotion_packet_top_decision"] is not None
    assert quarantine["search_portfolio"]["allocations"][0]["bucket"] == "repair_near_misses"
    assert quarantine["worker_specialization"]["workers"][0]["recommended_role"] == "holdout_repair"
    assert quarantine["causal_experiment_registry"]["experiments"][0]["experiment_id"] == "exp1"
    assert quarantine["compiled_hunt_policy"]["objective"] == "maximize_truth_first_promotable_pnl"
    assert quarantine["policy_executor"]["focus_routes"] == ["CLSK|trend_pullback|late"]
    assert quarantine["policy_tournament"]["champion"]["policy_name"] == "truth_first_conservative"
    assert quarantine["auto_promoted_field_manual"]["rules"][0]["rule"] == "Default to truth-first"
    assert quarantine["policy_drift_detector"]["status"] == "watch"
    assert quarantine["route_regime_half_life"]["routes"][0]["route_key"] == "CLSK|trend_pullback|late"
    assert quarantine["temporal_ensemble_policy"]["primary_policy"] == "truth_first_conservative"
    assert quarantine["route_state_machine"]["focus_routes"] == ["CLSK|trend_pullback|late"]
    assert quarantine["negative_knowledge_bank"]["avoid_routes"] == ["BAD|route|x"]
    assert quarantine["self_competition_league"]["champion"]["name"] == "truth_first_conservative"
    assert quarantine["uncertainty_budgeting"]["top_budget"]["route_key"] == "CLSK|trend_pullback|late"
    assert quarantine["learning_conflict_resolver"]["top_action"]["final_action"] == "revalidate"
    assert quarantine["adaptive_hunt_throttle"]["controls"]["batch_size_multiplier"] == 0.65
    assert quarantine["runtime_guardrails"]["guarded_command_packet"]["live_only"] is True
    assert quarantine["hypothesis_factory"]["hypotheses"]
    assert quarantine["hypothesis_market_maker"]["quotes"]
    assert quarantine["hunt_narrative_memory"]["memo"]
    assert quarantine["attention_ledger"]["entries"]
    assert quarantine["budget_reallocator"]["recommendation"]
    assert quarantine["idea_novelty_ledger"]["ideas"]
    assert quarantine["creative_brief_compiler"]["summary"]
    assert quarantine["learning_module_registry"]["modules"]
    assert quarantine["module_budget_governor"]["allocations"]
    assert "Pulling weight" in quarantine["meta_learning_brief"]["summary"]
    assert quarantine["causal_intervention_scheduler"]["interventions"]
    assert quarantine["experiment_power_calculator"]["interventions"] is not None
    assert quarantine["winner_fragility_profiler"]["profiles"]
    assert quarantine["live_beater_source_attribution"]["source_counts"]
    assert quarantine["adaptive_search_temperature_controller"]["temperature"]
    assert quarantine["route_interaction_learner"]["families"]
    assert quarantine["false_discovery_firewall"]["flag_count"] >= 0
    assert quarantine["hunt_strategy_compiler_v2"]["strategy"]
    assert quarantine["lesson_survival_tracker"]["lessons"]
    assert quarantine["lesson_decay_model"]["lessons"]
    assert quarantine["cross_hunt_causal_memory"]["records"]
    assert quarantine["evidence_chain_ledger"]["chains"]
    assert quarantine["learning_disagreement_court"]["case_count"] >= 0
    assert quarantine["promotion_aware_search_objective"]["leaderboard"]
    assert quarantine["scientific_run_brief_v2"]["summary"]
    assert quarantine["strategy_genome_registry"]["champion"]
    assert quarantine["strategy_mutation_engine"]["challengers"]
    assert quarantine["strategy_tournament_memory"]["champion"]
    assert quarantine["regime_conditioned_strategy_selector"]["selected_strategy"]
    assert quarantine["meta_objective_optimizer"]["objective"]
    assert quarantine["exploration_debt_ledger"]["debt_count"] >= 0
    assert quarantine["adversarial_strategy_red_team"]["attacks"]
    assert "pivot" in quarantine["autonomous_pivot_governor"]
    assert quarantine["learning_roi_ledger"]["rows"]
    assert quarantine["artifact_usefulness_pruner"]["artifacts"]
    assert quarantine["decision_trace_explainer"]["trace"]
    assert quarantine["control_surface_conflict_auditor"]["conflict_count"] >= 0
    assert quarantine["runtime_control_simplifier"]["final_command_packet"]["live_only"] is True
    assert quarantine["learning_cost_meter"]["artifact_count"] >= 1
    assert quarantine["ablation_replay_harness"]["tests"]
    assert quarantine["architecture_fitness_brief"]["summary"]
    assert quarantine["learning_artifact_schema_registry"]["validation"]
    assert quarantine["artifact_dependency_graph"]["edges"]
    assert quarantine["incremental_learning_cache"]["cache_key"]
    assert quarantine["live_learning_dashboard_feed"]["status"] in {"healthy", "pivoting"}
    assert quarantine["hunt_runbook_compiler"]["review_gates"]
    assert "failure_count" in quarantine["learning_failure_sentinel"]
    assert quarantine["cross_run_artifact_warehouse"]["exports"]
    assert quarantine["pre_hunt_readiness_gate"]["checks"]
    assert quarantine["online_causal_bandit"]["allocations"]
    assert quarantine["variant_dna_attribution"]["top_positive_genes"]
    assert "suppressed_count" in quarantine["negative_gene_suppression"]
    assert quarantine["live_winner_family_tree"]["families"]
    assert quarantine["exploration_frontier_map"]["frontier_cells"]
    assert quarantine["adaptive_worker_personalities"]["assignments"]
    assert quarantine["cycle_level_learning_delta"]["summary"]
    assert quarantine["promotion_rejection_predictor_v2"]["predictions"]
    assert quarantine["counterfactual_hunt_simulator"]["simulations"] is not None
    assert "missed_count" in quarantine["missed_winner_detector"]
    assert "rows" in quarantine["causal_regret_ledger"]
    assert quarantine["adaptive_search_grammar_generator"]["templates"] is not None
    assert quarantine["live_hypothesis_kill_scale_court"]["cases"] is not None
    assert "matrix" in quarantine["route_interaction_matrix_v2"]
    assert quarantine["promotion_survival_shadow_scoring"]["scores"] is not None
    assert quarantine["hunt_autopilot_policy_compiler"]["policy_packet"]["live_only"] is True
    assert quarantine["learning_claim_verifier"]["claims"]
    assert "calibrations" in quarantine["causal_confidence_calibration"]
    assert "warning_count" in quarantine["false_discovery_early_warning"]
    assert quarantine["adaptive_evidence_thresholds"]["thresholds"]
    assert quarantine["self_debate_search_council"]["recommendations"]
    assert "rule_count" in quarantine["experiment_memory_compression"]
    assert "drift_count" in quarantine["learning_drift_monitor"]
    assert quarantine["promotion_first_autopilot_v2"]["policy_packet"]["objective"] == "maximize_promotion_survivable_live_pnl"
    assert quarantine["multi_horizon_memory_stack"]["horizons"]
    assert quarantine["lesson_half_life_engine_v2"]["lessons"]
    assert "replays" in quarantine["cross_hunt_strategy_replay"]
    assert quarantine["temporal_regime_fingerprinting"]["fingerprints"]
    assert quarantine["longitudinal_promotion_survival_model"]["features"]
    assert "case_count" in quarantine["memory_conflict_court_v2"]
    assert quarantine["strategy_aging_dashboard"]["strategies"]
    assert quarantine["next_hunt_opening_policy_compiler"]["policy_packet"]["live_only"] is True
    assert quarantine["question_driven_hunt_planner"]["questions"]
    assert quarantine["expected_information_gain_scorer_v2"]["scores"]
    assert quarantine["uncertainty_heatmap"]["cells"]
    assert quarantine["adaptive_experiment_sequencer"]["steps"]
    assert "stop_count" in quarantine["learning_value_stop_loss"]
    assert quarantine["causal_question_ledger"]["questions"]
    assert quarantine["worker_epistemic_roles_v2"]["assignments"]
    assert quarantine["hunt_hypothesis_compiler"]["hypotheses"]
    assert quarantine["experiment_contract_compiler"]["contracts"]
    assert quarantine["control_route_matcher"]["matches"]
    assert quarantine["sequential_test_monitor"]["decisions"]
    assert quarantine["causal_effect_size_ledger"]["effects"]
    assert "pressure_score" in quarantine["false_positive_pressure_gauge"]
    assert "plans" in quarantine["exploration_debt_paydown_planner"]
    assert quarantine["promotion_aware_power_planner"]["plans"]
    assert quarantine["scientific_hunt_executive"]["command_packet"]["live_only"] is True
    assert quarantine["live_candidate_evidence_builder"]["packets"]
    assert quarantine["promotion_failure_predictor_v3"]["predictions"]
    assert "tasks" in quarantine["evidence_gap_router"]
    assert "near_ready" in quarantine["review_ready_queue_v2"]
    assert quarantine["promotion_evidence_scorecard"]["scorecards"]
    assert quarantine["candidate_lineage_explainer_v2"]["explanations"]
    assert "reports" in quarantine["live_vs_control_differential_report"]
    assert quarantine["promotion_packet_executive"]["decisions"]
    assert quarantine["runtime_command_adapter"]["live_only"] is True
    assert quarantine["runtime_command_adapter"]["focus_routes"]
    assert "NEW|route|x" in quarantine["runtime_command_adapter"]["focus_routes"]
    assert quarantine["runtime_command_adapter"]["learning_rate_mode"] == "exploit_with_challenger"
    assert quarantine["worker_job_contracts"]["contract_count"] >= 1
    assert quarantine["ab_route_experiment_executor"]["assignment_count"] == 1
    assert quarantine["champion_challenger_runtime_slots"]["slots"][0]["slot"] == "champion"
    assert quarantine["learning_rate_controller"]["mode"] == "exploit_with_challenger"
    assert quarantine["runtime_command_adapter"]["scarcity_mode"] == "broad_discovery"
    assert "CLSK|trend_pullback|late" in quarantine["runtime_command_adapter"]["structural_mutation_routes"]
    assert "OLD|route|x" in quarantine["runtime_command_adapter"]["retire_routes"]
    assert quarantine["recovery_playbook_generator"]["top_intervention"]["action"] == "force_structural_mutation"
    assert quarantine["alias_trap_detector"]["trap_count"] == 1
    assert any(job.get("action") == "experiment_arm" for contract in quarantine["worker_job_contracts"]["contracts"] for job in contract.get("jobs", []))
    assert state["learning_upgrade_directive"]["runtime_adapter_summary"]["mutation_width"] in {"wide", "tight"}
    assert state["learning_upgrade_directive"]["worker_contract_count"] >= 1
    assert quarantine["runtime_decision_kernel"]["commands"][0]["action"] == "revalidate"
    assert quarantine["counterfactual_shadow_board"]["tasks"][0]["check"] == "disable_route"
    assert quarantine["self_audit_score"]["self_audit_score"] == 88.0
    assert quarantine["adversarial_red_team_learner"]["tasks"][0]["attack"] == "ood_stress_replay"


def test_runtime_command_adapter_degrades_and_backfills_outcomes():
    previous = {
        "top_candidates": [{"variant": "old", "step2_pnl": 100.0}],
        "runtime_decision_kernel": {
            "command_packet": {
                "live_only": True,
                "max_workers": 8,
                "focus_routes": ["CLSK|trend_pullback|late"],
                "avoid_routes": [],
                "mutation_width": "wide",
                "batch_size_multiplier": 1.4,
                "commands": [
                    {
                        "command_id": "cmd-scale",
                        "action": "scale",
                        "route_key": "CLSK|trend_pullback|late",
                        "mutation_width": "wide",
                        "batch_size_multiplier": 1.4,
                    }
                ],
            },
            "commands": [{"command_id": "cmd-scale", "action": "scale", "route_key": "CLSK|trend_pullback|late"}],
        },
        "runtime_guardrails": {
            "ok": False,
            "failures": ["max_workers_exceeded"],
            "guarded_command_packet": {
                "live_only": True,
                "max_workers": 4,
                "focus_routes": ["CLSK|trend_pullback|late"],
                "avoid_routes": [],
                "mutation_width": "wide",
                "batch_size_multiplier": 1.4,
                "commands": [
                    {
                        "command_id": "cmd-scale",
                        "action": "scale",
                        "route_key": "CLSK|trend_pullback|late",
                    }
                ],
            },
        },
        "closed_loop_reward_model": {
            "action_rewards": [{"action": "revalidate", "avg_reward_score": 6.0}]
        },
    }
    rankings = {
        "raw_leaderboard": [
            {
                "variant": "new",
                "ticker": "CLSK",
                "setup_type": "trend_pullback",
                "session_phase": "late",
                "route_key": "CLSK|trend_pullback|late",
                "step2_pnl": 140.0,
                "step2_delta_vs_active": 20.0,
            }
        ]
    }

    adapter = step2_online_learning.runtime_command_adapter(previous)
    backfill = step2_online_learning.command_outcome_backfill(previous, rankings, [{"scored_total": 100, "winners": 2}])
    calibration = step2_online_learning.action_reward_calibration(previous, backfill)
    contracts = step2_online_learning.planner_worker_contracts(previous)

    assert adapter["degrade_mode"] is True
    assert adapter["mutation_width"] == "tight"
    assert adapter["commands"][0]["action"] == "revalidate"
    assert adapter["batch_size_multiplier"] == 0.65
    assert step2_online_learning.batch_size_multiplier(previous) == 0.65
    assert step2_online_learning.mutation_scale_for_route(previous) == 0.72
    assert backfill["outcomes"][0]["worked"] is True
    assert calibration["rows"][0]["action"] == "revalidate"
    assert contracts["contracts"][0]["guardrails"]["degrade_mode"] is True


def test_hypothesis_learning_suite_drives_runtime_focus_and_jobs():
    state = {
        "top_candidates": [
            {
                "variant": "candidate",
                "route_key": "CLSK|trend_pullback|late",
                "step2_pnl": 180.0,
                "step2_delta_vs_active": 40.0,
                "mutation_lane": "holdout_repair",
            },
            {
                "variant": "old_candidate",
                "route_key": "OLD|route|x",
                "step2_pnl": 4.0,
                "mutation_lane": "wide_probe",
            },
        ],
        "promotion_survivor_model": {
            "promotion_survivor_top10": [
                {"route_key": "CLSK|trend_pullback|late", "survival_probability": 0.8},
                {"route_key": "OLD|route|x", "survival_probability": 0.1},
            ]
        },
        "memory_falsification_queue": {
            "queue": [
                {
                    "subject": "CLSK|trend_pullback|late",
                    "route_key": "CLSK|trend_pullback|late",
                    "priority_score": 75.0,
                }
            ]
        },
        "belief_retirement_engine": {"retire_routes": ["OLD|route|x"]},
        "opportunity_cost_meter": {"reduce_budget_routes": ["OLD|route|x"]},
    }
    state.update(step2_online_learning.hypothesis_learning_suite(state))
    adapter = step2_online_learning.runtime_command_adapter(state)
    contracts = step2_online_learning.planner_worker_contracts(state)
    quarantine = step2_online_learning.quarantine_payload(state)

    assert state["hypothesis_factory"]["hypotheses"]
    assert state["hypothesis_market_maker"]["quotes"][0]["market_action"] in {"buy", "watch"}
    assert state["real_time_bet_sizer"]["allocations"]
    assert state["contrarian_generator"]["experiments"]
    assert "OLD|route|x" in state["learning_stop_loss"]["avoid_routes"]
    assert state["breakthrough_detector"]["breakthroughs"]
    assert state["pattern_to_recipe_compiler"]["recipes"]
    assert "Believed:" in state["hunt_narrative_memory"]["summary"]
    assert "CLSK|trend_pullback|late" in adapter["hypothesis_focus_routes"]
    assert "OLD|route|x" in adapter["avoid_routes"]
    assert any(job.get("action") == "hypothesis_bet" for contract in contracts["contracts"] for job in contract.get("jobs", []))
    assert quarantine["real_time_bet_sizer"]["allocations"]


def test_attention_economics_suite_reallocates_budget_and_time_plan():
    state = {
        "cycles_completed": 5,
        "remaining_sec": 1200,
        "cycle_yield": {"recent_winner_yield_per_10k": 0.0},
        "top_candidates": [{"route_key": "WIN|route|x", "step2_pnl": 120.0}],
        "online_allocation": {
            "allocation": [
                {"route_key": "WASTE|route|x", "recommended_budget_pct": 60.0},
                {"route_key": "WIN|route|x", "recommended_budget_pct": 20.0},
            ]
        },
        "real_time_bet_sizer": {
            "allocations": [
                {
                    "hypothesis_id": "h1",
                    "route_key": "WIN|route|x",
                    "treatment": "holdout_repair",
                    "budget_pct": 30.0,
                    "worker": "worker_1",
                    "batch_size_multiplier": 1.1,
                }
            ]
        },
        "learning_stop_loss": {"avoid_routes": ["WASTE|route|x"]},
        "worker_learning_report_cards": {
            "cards": [{"worker": "worker_1", "score": 15.0, "recommended_role": "repair"}]
        },
    }
    state.update(step2_online_learning.attention_economics_suite(state))
    adapter = step2_online_learning.runtime_command_adapter(state)
    contracts = step2_online_learning.planner_worker_contracts(state)
    quarantine = step2_online_learning.quarantine_payload(state)

    assert state["attention_ledger"]["route_spend"]
    assert "WASTE|route|x" in state["wasted_spend_autopsy"]["avoid_routes"]
    assert "WASTE|route|x" in state["marginal_yield_curve"]["plateau_routes"]
    assert state["explore_exploit_regret_tracker"]["recommendation"]
    assert state["worker_alpha_attribution"]["top_worker"]["worker"] == "worker_1"
    assert "WASTE|route|x" in state["budget_reallocator"]["avoid_routes"]
    assert state["time_aware_hunt_plan"]["phase"] == "late"
    assert "Spent" in state["spend_efficiency_narrative"]["summary"]
    assert "WASTE|route|x" in adapter["avoid_routes"]
    assert adapter["time_plan_phase"] == "late"
    assert any(job.get("rationale") == "budget_reallocator" for contract in contracts["contracts"] for job in contract.get("jobs", []))
    assert quarantine["spend_efficiency_narrative"]["summary"]


def test_creative_imagination_suite_governs_novelty_budget():
    state = {
        "top_candidates": [{"variant": "parent_win", "route_key": "WIN|route|x", "step2_pnl": 120.0}],
        "hypothesis_factory": {
            "hypotheses": [
                {
                    "hypothesis_id": "h1",
                    "route_key": "WIN|route|x",
                    "treatment": "holdout_repair",
                    "claim": "WIN route improves with holdout repair",
                    "source": "top_candidate",
                },
                {
                    "hypothesis_id": "h2",
                    "route_key": "ODD|route|x",
                    "treatment": "indicator_shuffle",
                    "claim": "ODD route might work with indicator shuffle",
                    "source": "contrarian",
                },
            ]
        },
        "hypothesis_market_maker": {
            "quotes": [
                {"hypothesis_id": "h2", "route_key": "ODD|route|x", "edge": 80.0},
                {"hypothesis_id": "h1", "route_key": "WIN|route|x", "edge": 30.0},
            ]
        },
        "contrarian_generator": {
            "experiments": [
                {
                    "experiment_id": "c1",
                    "route_key": "ODD|route|x",
                    "treatment": "indicator_shuffle",
                    "contrarian_action": "indicator_shuffle",
                    "priority_score": 70.0,
                }
            ]
        },
        "time_aware_hunt_plan": {"phase": "early"},
    }
    state.update(step2_online_learning.creative_imagination_suite(state))
    adapter = step2_online_learning.runtime_command_adapter(state)
    contracts = step2_online_learning.planner_worker_contracts(state)
    quarantine = step2_online_learning.quarantine_payload(state)

    assert state["idea_novelty_ledger"]["ideas"]
    assert state["idea_saturation_detector"]["saturation_count"] >= 0
    assert state["creative_leap_scorer"]["leaps"]
    assert state["failed_imagination_autopsy"]["autopsies"] is not None
    assert state["mutation_grammar_gap_finder"]["gaps"]
    assert state["novelty_budget_governor"]["novelty_budget_pct"] == 28.0
    assert state["idea_lineage_map"]["nodes"]
    assert "Novelty budget" in state["creative_brief_compiler"]["summary"]
    assert adapter["novelty_budget_pct"] == 28.0
    assert any(job.get("rationale") == "novelty_budget_governor" for contract in contracts["contracts"] for job in contract.get("jobs", []))
    assert quarantine["creative_brief_compiler"]["summary"]


def test_meta_learning_suite_audits_modules_and_adds_ablation_jobs():
    state = {
        "top_candidates": [{"variant": "winner", "route_key": "WIN|route|x", "step2_pnl": 160.0}],
        "memory_reliability_scorer": {"scores": [{"subject": "WIN|route|x", "reliability_score": 80.0}]},
        "memory_falsification_queue": {"focus_routes": ["WIN|route|x"]},
        "belief_retirement_engine": {"retire_routes": ["KILL|route|x"]},
        "hypothesis_factory": {"hypotheses": [{"hypothesis_id": "h1", "route_key": "WIN|route|x"}]},
        "hypothesis_market_maker": {"focus_routes": ["WIN|route|x"], "quotes": [{"route_key": "WIN|route|x"}]},
        "learning_stop_loss": {"avoid_routes": ["KILL|route|x"]},
        "attention_ledger": {"route_spend": [{"route_key": "WIN|route|x", "spent": 4}]},
        "wasted_spend_autopsy": {"avoid_routes": ["KILL|route|x"]},
        "budget_reallocator": {
            "focus_routes": ["WIN|route|x"],
            "avoid_routes": ["KILL|route|x"],
            "batch_size_multiplier": 0.8,
        },
        "idea_novelty_ledger": {"ideas": [{"route_key": "ODD|route|x"}]},
        "creative_leap_scorer": {"focus_routes": ["ODD|route|x"], "leaps": [{"route_key": "ODD|route|x"}]},
        "novelty_budget_governor": {"focus_routes": ["ODD|route|x"], "batch_size_multiplier": 1.1},
        "runtime_command_adapter": {"focus_routes": ["KILL|route|x"], "avoid_routes": []},
    }
    state.update(step2_online_learning.meta_learning_suite(state))
    adapter = step2_online_learning.runtime_command_adapter(state)
    contracts = step2_online_learning.planner_worker_contracts(state)
    quarantine = step2_online_learning.quarantine_payload(state)

    assert state["learning_module_registry"]["active_modules"]
    assert state["module_contribution_attribution"]["top_module"]["module_id"]
    assert state["module_conflict_detector"]["conflict_count"] >= 1
    assert state["module_reliability_scorer"]["scores"]
    assert state["module_ablation_planner"]["tests"]
    assert state["module_budget_governor"]["allocations"]
    assert state["learning_system_self_audit"]["health_score"] is not None
    assert "Pulling weight" in state["meta_learning_brief"]["summary"]
    assert "KILL|route|x" in adapter["avoid_routes"]
    assert adapter["module_conflict_count"] >= 1
    assert any(
        job.get("action") == "module_ablation_shadow"
        for contract in contracts["contracts"]
        for job in contract.get("jobs", [])
    )
    assert quarantine["learning_module_registry"]["modules"]
    assert quarantine["module_budget_governor"]["allocations"]
    assert quarantine["meta_learning_brief"]["summary"]


def test_runtime_science_suite_controls_power_fragility_and_strategy():
    state = {
        "top_candidates": [
            {
                "variant": "winner_a",
                "route_key": "WIN|route|late",
                "step2_pnl": 190.0,
                "step2_delta_vs_active": 60.0,
                "promotion_readiness_score": 82.0,
                "learning_tags": [],
            },
            {
                "variant": "fragile_b",
                "route_key": "THIN|route|late",
                "step2_pnl": 142.0,
                "step2_delta_vs_active": 5.0,
                "promotion_readiness_score": 42.0,
                "learning_tags": ["thin_sample", "high_overfit_risk"],
            },
        ],
        "cycle_yield": {"recent_scored_total": 120, "recent_reported_winners": 1},
        "hypothesis_market_maker": {"focus_routes": ["WIN|route|late"]},
        "creative_leap_scorer": {"focus_routes": ["ODD|route|x"]},
        "budget_reallocator": {"focus_routes": ["WIN|route|late"]},
        "module_conflict_detector": {"conflict_count": 1, "downweight_routes": ["THIN|route|late"], "pause_routes": ["THIN|route|late"]},
        "search_space_coverage_map": {"blind_spots": [{"route_key": "BLIND|route|x"}]},
        "live_beater_scarcity_mode": {"enabled": True, "focus_routes": ["BLIND|route|x"]},
    }
    state.update(step2_online_learning.runtime_science_suite(state))
    adapter = step2_online_learning.runtime_command_adapter(state)
    contracts = step2_online_learning.planner_worker_contracts(state)
    quarantine = step2_online_learning.quarantine_payload(state)

    assert state["causal_intervention_scheduler"]["interventions"]
    assert state["experiment_power_calculator"]["underpowered_experiments"]
    assert state["winner_fragility_profiler"]["fragile_variants"] == ["fragile_b"]
    assert state["live_beater_source_attribution"]["top_source"]["source"]
    assert state["adaptive_search_temperature_controller"]["temperature"] == "hot"
    assert state["route_interaction_learner"]["families"]
    assert "fragile_b" in state["false_discovery_firewall"]["quarantine_variants"]
    assert state["hunt_strategy_compiler_v2"]["strategy"] == "controlled_hot_discovery"
    assert "THIN|route|late" in adapter["avoid_routes"]
    assert adapter["runtime_science_temperature"] == "hot"
    assert any(
        job.get("action") == "winner_fragility_stress"
        for contract in contracts["contracts"]
        for job in contract.get("jobs", [])
    )
    assert quarantine["hunt_strategy_compiler_v2"]["summary"]


def test_lesson_accountability_suite_backprops_rejections_and_promotes_objective():
    state = {
        "top_candidates": [
            {
                "variant": "durable_a",
                "route_key": "GOOD|route|late",
                "step2_pnl": 210.0,
                "step2_delta_vs_active": 70.0,
                "promotion_readiness_score": 88.0,
            },
            {
                "variant": "rejected_b",
                "route_key": "BAD|route|late",
                "step2_pnl": 240.0,
                "step2_delta_vs_active": 12.0,
                "promotion_readiness_score": 38.0,
                "learning_tags": ["thin_sample", "high_overfit_risk"],
            },
        ],
        "promotion_feedback": [
            {"variant": "rejected_b", "decision": "reject", "reason": "fragile low robustness"},
            {"variant": "durable_a", "decision": "promote", "reason": "survived review"},
        ],
        "module_conflict_detector": {
            "conflicts": [
                {
                    "route_key": "BAD|route|late",
                    "focus_modules": ["creativity"],
                    "avoid_modules": ["memory", "hypothesis"],
                    "resolution": "pause_or_shadow_test",
                }
            ],
            "conflict_count": 1,
            "pause_routes": ["BAD|route|late"],
            "downweight_routes": ["BAD|route|late"],
        },
        "hypothesis_market_maker": {"focus_routes": ["GOOD|route|late"]},
        "policy_executor": {"focus_routes": ["GOOD|route|late"]},
        "cycle_yield": {"recent_scored_total": 1000, "recent_reported_winners": 2},
    }
    state.update(step2_online_learning.runtime_science_suite(state))
    state.update(step2_online_learning.lesson_accountability_suite(state))
    adapter = step2_online_learning.runtime_command_adapter(state)
    contracts = step2_online_learning.planner_worker_contracts(state)
    quarantine = step2_online_learning.quarantine_payload(state)

    assert state["lesson_survival_tracker"]["survivors"][0]["variant"] == "durable_a"
    assert state["promotion_rejection_backpropagation"]["rejection_count"] == 1
    assert "BAD|route|late" in state["lesson_decay_model"]["decay_routes"]
    assert state["cross_hunt_causal_memory"]["records"]
    assert state["evidence_chain_ledger"]["top_chain"]["variant"] == "rejected_b"
    assert "BAD|route|late" in state["learning_disagreement_court"]["pause_routes"]
    assert state["promotion_aware_search_objective"]["top_candidate"]["variant"] == "durable_a"
    assert "Belief:" in state["scientific_run_brief_v2"]["summary"]
    assert "BAD|route|late" in adapter["avoid_routes"]
    assert adapter["promotion_aware_top"]["variant"] == "durable_a"
    assert any(
        job.get("action") == "promotion_aware_probe"
        for contract in contracts["contracts"]
        for job in contract.get("jobs", [])
    )
    assert quarantine["scientific_run_brief_v2"]["summary"]


def test_strategy_evolution_suite_mutates_selects_and_pivots_strategies():
    state = {
        "top_candidates": [
            {
                "variant": "fragile_winner",
                "route_key": "RISK|route|late",
                "step2_pnl": 250.0,
                "step2_delta_vs_active": 8.0,
                "promotion_readiness_score": 35.0,
                "learning_tags": ["thin_sample", "high_overfit_risk"],
            },
            {
                "variant": "durable_winner",
                "route_key": "GOOD|route|late",
                "step2_pnl": 210.0,
                "step2_delta_vs_active": 65.0,
                "promotion_readiness_score": 90.0,
            },
        ],
        "promotion_feedback": [{"variant": "durable_winner", "decision": "promote"}],
        "cycle_yield": {"recent_scored_total": 150, "recent_reported_winners": 0},
        "search_space_coverage_map": {"blind_spots": [{"route_key": "BLIND|route|x"}]},
        "creative_leap_scorer": {"focus_routes": ["ODD|route|x"]},
        "module_conflict_detector": {"conflict_count": 1, "pause_routes": ["RISK|route|late"], "downweight_routes": ["RISK|route|late"], "conflicts": []},
    }
    state.update(step2_online_learning.runtime_science_suite(state))
    state.update(step2_online_learning.lesson_accountability_suite(state))
    state.update(step2_online_learning.strategy_evolution_suite(state))
    adapter = step2_online_learning.runtime_command_adapter(state)
    contracts = step2_online_learning.planner_worker_contracts(state)
    quarantine = step2_online_learning.quarantine_payload(state)

    assert state["strategy_genome_registry"]["champion"]["genome_id"]
    assert len(state["strategy_mutation_engine"]["challengers"]) == 4
    assert state["strategy_tournament_memory"]["champion"]["strategy_name"]
    assert state["regime_conditioned_strategy_selector"]["regime"] in {"false_discovery_pressure", "scarcity_or_underpowered", "survivor_memory", "balanced"}
    assert state["meta_objective_optimizer"]["objective"] in {"fragility_reduction", "information_gain", "promotable_pnl"}
    assert state["exploration_debt_ledger"]["focus_routes"]
    assert state["adversarial_strategy_red_team"]["top_attack"]["attack"]
    assert state["autonomous_pivot_governor"]["pivot"] is True
    assert adapter["autonomous_pivot"] is True
    assert adapter["meta_objective"]
    assert contracts["contract_count"] >= 1
    assert state["strategy_mutation_engine"]["top_challenger"]["strategy_name"]
    assert quarantine["autonomous_pivot_governor"]["pivot"] is True


def test_governance_suite_measures_roi_simplifies_controls_and_plans_ablation():
    state = {
        "top_candidates": [
            {
                "variant": "durable_winner",
                "route_key": "GOOD|route|late",
                "step2_pnl": 210.0,
                "step2_delta_vs_active": 65.0,
                "promotion_readiness_score": 90.0,
            }
        ],
        "promotion_feedback": [{"variant": "durable_winner", "decision": "promote"}],
        "cycle_yield": {"recent_scored_total": 800, "recent_reported_winners": 2},
        "search_space_coverage_map": {"blind_spots": [{"route_key": "BLIND|route|x"}]},
        "creative_leap_scorer": {"focus_routes": ["ODD|route|x"]},
        "runtime_decision_kernel": {
            "command_packet": {
                "live_only": True,
                "focus_routes": ["GOOD|route|late"],
                "mutation_width": "medium",
                "batch_size_multiplier": 1.0,
                "commands": [],
            }
        },
        "budget_reallocator": {"focus_routes": ["GOOD|route|late"], "mutation_width": "tight", "batch_size_multiplier": 0.8},
        "novelty_budget_governor": {"focus_routes": ["ODD|route|x"], "mutation_width": "wide", "batch_size_multiplier": 1.2},
    }
    state.update(step2_online_learning.runtime_science_suite(state))
    state.update(step2_online_learning.lesson_accountability_suite(state))
    state.update(step2_online_learning.strategy_evolution_suite(state))
    state.update(step2_online_learning.governance_suite(state))
    adapter = step2_online_learning.runtime_command_adapter(state)
    contracts = step2_online_learning.planner_worker_contracts(state)
    quarantine = step2_online_learning.quarantine_payload(state)

    assert state["learning_roi_ledger"]["top_roi"]["artifact"]
    assert state["artifact_usefulness_pruner"]["keep_artifacts"]
    assert state["decision_trace_explainer"]["trace_count"] >= 1
    assert state["control_surface_conflict_auditor"]["conflict_count"] >= 1
    assert state["runtime_control_simplifier"]["final_command_packet"]["focus_routes"]
    assert state["learning_cost_meter"]["highest_cost"]["artifact"]
    assert state["ablation_replay_harness"]["test_count"] == 4
    assert state["architecture_fitness_brief"]["verdict"] in {"healthy", "simplify"}
    assert adapter["architecture_fitness_verdict"] in {"healthy", "simplify"}
    assert contracts["contract_count"] >= 1
    assert state["ablation_replay_harness"]["top_test"]["action"] == "shadow_replay_without_layer"
    assert quarantine["runtime_control_simplifier"]["final_command_packet"]["live_only"] is True


def test_learning_ops_suite_builds_schema_cache_runbook_and_readiness_gate():
    state = {
        "top_candidates": [
            {
                "variant": "ops_winner",
                "route_key": "OPS|route|late",
                "step2_pnl": 220.0,
                "step2_delta_vs_active": 70.0,
                "promotion_readiness_score": 92.0,
            }
        ],
        "promotion_feedback": [{"variant": "ops_winner", "decision": "promote"}],
        "cycle_yield": {"recent_scored_total": 700, "recent_reported_winners": 3},
        "runtime_decision_kernel": {
            "command_packet": {
                "live_only": True,
                "focus_routes": ["OPS|route|late"],
                "mutation_width": "medium",
                "batch_size_multiplier": 1.0,
                "commands": [{"action": "probe", "route_key": "OPS|route|late"}],
            }
        },
    }
    state.update(step2_online_learning.runtime_science_suite(state))
    state.update(step2_online_learning.lesson_accountability_suite(state))
    state.update(step2_online_learning.strategy_evolution_suite(state))
    state.update(step2_online_learning.governance_suite(state))
    state["runtime_command_adapter"] = step2_online_learning.runtime_command_adapter(state)
    state["worker_job_contracts"] = step2_online_learning.planner_worker_contracts(state)
    state.update(step2_online_learning.learning_ops_suite(state))
    adapter = step2_online_learning.runtime_command_adapter(state)
    quarantine = step2_online_learning.quarantine_payload(state)

    assert state["learning_artifact_schema_registry"]["validation"]
    assert "runtime_command_adapter" in state["learning_artifact_schema_registry"]["schemas"]
    assert state["artifact_dependency_graph"]["edges"]
    assert state["incremental_learning_cache"]["reuse_count"] >= 0
    assert state["live_learning_dashboard_feed"]["status"] in {"healthy", "pivoting"}
    assert state["hunt_runbook_compiler"]["worker_count"] >= 1
    assert "failure_count" in state["learning_failure_sentinel"]
    assert state["cross_run_artifact_warehouse"]["export_count"] >= 8
    assert state["pre_hunt_readiness_gate"]["checks"]
    assert adapter["schema_registry_valid"] is not None
    assert adapter["runbook_worker_count"] >= 1
    assert quarantine["pre_hunt_readiness_gate"]["checks"]


def test_learning_failure_sentinel_treats_soft_pivot_attack_as_warning():
    sentinel = step2_online_learning.learning_failure_sentinel({
        "learning_artifact_schema_registry": {"valid": True},
        "hunt_runbook_compiler": {"batch_size_multiplier": 1.0, "avoid_routes": []},
        "runtime_control_simplifier": {"batch_size_multiplier": 1.0, "avoid_routes": []},
        "autonomous_pivot_governor": {"pivot": True, "focus_routes": ["SOFT|route|x"]},
        "adversarial_strategy_red_team": {
            "top_attack": {"attack_score": 70.0, "route_key": "SOFT|route|x"},
            "avoid_routes": ["SOFT|route|x"],
        },
    })

    assert sentinel["ok"] is True
    assert sentinel["failure_count"] == 0
    assert sentinel["warning_count"] == 1
    assert sentinel["control_posture"] == "observe_and_repair"
    assert sentinel["batch_size_multiplier"] is None
    assert "shadow_validate_pivot_without_global_throttle" in sentinel["recommended_actions"]


def test_learning_failure_sentinel_hard_blocks_critical_pivot_attack():
    sentinel = step2_online_learning.learning_failure_sentinel({
        "learning_artifact_schema_registry": {"valid": True},
        "hunt_runbook_compiler": {"batch_size_multiplier": 1.0, "avoid_routes": ["HARD|route|x"]},
        "runtime_control_simplifier": {"batch_size_multiplier": 1.0, "avoid_routes": []},
        "autonomous_pivot_governor": {"pivot": True, "focus_routes": ["HARD|route|x"]},
        "adversarial_strategy_red_team": {
            "top_attack": {"severity": "critical", "route_key": "HARD|route|x"},
        },
    })

    assert sentinel["ok"] is False
    assert sentinel["failure_count"] == 1
    assert sentinel["control_posture"] == "throttle"
    assert sentinel["batch_size_multiplier"] == 0.65


def test_real_time_causal_search_suite_steers_genes_frontier_workers_and_rejection_risk():
    state = {
        "top_candidates": [
            {
                "variant": "causal_win",
                "route_key": "CAUSE|trend|late",
                "family_key": "cause_family",
                "mutation_lane": "edge_expansion",
                "indicator": "rsi",
                "step2_pnl": 260.0,
                "step2_delta_vs_active": 85.0,
                "promotion_readiness_score": 88.0,
                "learning_tags": ["route_narrow"],
            },
            {
                "variant": "fragile_win",
                "route_key": "FRAGILE|mean|open",
                "family_key": "fragile_family",
                "mutation_lane": "holdout_repair",
                "indicator": "vwap",
                "step2_pnl": 190.0,
                "step2_delta_vs_active": 25.0,
                "promotion_readiness_score": 45.0,
                "learning_tags": ["thin_holdout_edge", "high_overfit_risk"],
            },
        ],
        "promotion_feedback": [{"variant": "fragile_win", "decision": "reject"}],
        "search_space_coverage_map": {"blind_spots": [{"route_key": "BLIND|route|x"}]},
    }
    state.update(step2_online_learning.real_time_causal_search_suite(state))
    adapter = step2_online_learning.runtime_command_adapter(state)
    state["runtime_command_adapter"] = adapter
    state["worker_job_contracts"] = step2_online_learning.planner_worker_contracts(state)
    quarantine = step2_online_learning.quarantine_payload(state)

    assert state["online_causal_bandit"]["top_lane"] in {"edge_expansion", "holdout_repair"}
    assert state["variant_dna_attribution"]["top_gene"]
    assert state["negative_gene_suppression"]["suppressed_count"] >= 1
    assert state["live_winner_family_tree"]["family_count"] == 2
    assert "BLIND|route|x" in state["exploration_frontier_map"]["focus_routes"]
    assert state["adaptive_worker_personalities"]["worker_count"] == 4
    assert "top_lane=" in state["cycle_level_learning_delta"]["summary"]
    assert state["promotion_rejection_predictor_v2"]["highest_risk"]["variant"] == "fragile_win"
    assert "FRAGILE|mean|open" in adapter["avoid_routes"]
    assert any(
        job.get("rationale", "").startswith("adaptive_worker_personalities")
        for contract in state["worker_job_contracts"]["contracts"]
        for job in contract.get("jobs", [])
    )
    assert quarantine["promotion_rejection_predictor_v2"]["high_risk_variants"]


def test_counterfactual_opportunity_suite_compiles_autopilot_policy():
    state = {
        "top_candidates": [
            {
                "variant": "durable_win",
                "route_key": "DURABLE|trend|late",
                "family_key": "durable",
                "mutation_lane": "edge_expansion",
                "indicator": "rsi",
                "step2_pnl": 280.0,
                "step2_delta_vs_active": 95.0,
                "promotion_readiness_score": 90.0,
            },
            {
                "variant": "fragile_win",
                "route_key": "FRAGILE|mean|open",
                "family_key": "fragile",
                "mutation_lane": "holdout_repair",
                "indicator": "vwap",
                "step2_pnl": 230.0,
                "step2_delta_vs_active": 30.0,
                "promotion_readiness_score": 42.0,
                "learning_tags": ["thin_holdout_edge", "high_overfit_risk"],
            },
        ],
        "promotion_feedback": [{"variant": "fragile_win", "decision": "reject"}],
        "search_space_coverage_map": {"blind_spots": [{"route_key": "MISSED|frontier|x"}]},
        "runtime_decision_kernel": {
            "command_packet": {
                "live_only": True,
                "focus_routes": ["DURABLE|trend|late"],
                "mutation_width": "tight",
                "batch_size_multiplier": 1.0,
                "commands": [{"action": "probe", "route_key": "DURABLE|trend|late"}],
            }
        },
    }
    state.update(step2_online_learning.real_time_causal_search_suite(state))
    state["runtime_command_adapter"] = step2_online_learning.runtime_command_adapter(state)
    state.update(step2_online_learning.counterfactual_opportunity_suite(state))
    adapter = step2_online_learning.runtime_command_adapter(state)
    state["runtime_command_adapter"] = adapter
    contracts = step2_online_learning.planner_worker_contracts(state)
    quarantine = step2_online_learning.quarantine_payload(state)

    assert state["counterfactual_hunt_simulator"]["top_counterfactual"]
    assert state["missed_winner_detector"]["missed_count"] >= 1
    assert state["causal_regret_ledger"]["rows"]
    assert state["adaptive_search_grammar_generator"]["templates"]
    assert state["live_hypothesis_kill_scale_court"]["cases"]
    assert "matrix" in state["route_interaction_matrix_v2"]
    assert state["promotion_survival_shadow_scoring"]["top_shadow"]
    assert state["hunt_autopilot_policy_compiler"]["policy_packet"]["live_only"] is True
    assert "MISSED|frontier|x" in adapter["focus_routes"]
    assert any(
        job.get("rationale") == "hunt_autopilot_policy_compiler"
        for contract in contracts["contracts"]
        for job in contract.get("jobs", [])
    )
    assert quarantine["hunt_autopilot_policy_compiler"]["worker_policy"]


def test_truth_maintenance_suite_verifies_claims_and_builds_promotion_first_policy():
    state = {
        "top_candidates": [
            {
                "variant": "truth_win",
                "route_key": "TRUTH|trend|late",
                "family_key": "truth",
                "mutation_lane": "edge_expansion",
                "indicator": "rsi",
                "step2_pnl": 310.0,
                "step2_delta_vs_active": 110.0,
                "promotion_readiness_score": 91.0,
            },
            {
                "variant": "fake_win",
                "route_key": "FAKE|mean|open",
                "family_key": "fake",
                "mutation_lane": "wild_shuffle",
                "indicator": "vwap",
                "step2_pnl": 250.0,
                "step2_delta_vs_active": 18.0,
                "promotion_readiness_score": 35.0,
                "learning_tags": ["thin_sample", "high_overfit_risk"],
            },
        ],
        "promotion_feedback": [{"variant": "fake_win", "decision": "reject"}],
        "runtime_decision_kernel": {
            "command_packet": {
                "live_only": True,
                "focus_routes": ["TRUTH|trend|late", "FAKE|mean|open"],
                "mutation_width": "medium",
                "batch_size_multiplier": 1.0,
                "commands": [{"action": "probe", "route_key": "TRUTH|trend|late"}],
            }
        },
    }
    state.update(step2_online_learning.real_time_causal_search_suite(state))
    state["runtime_command_adapter"] = step2_online_learning.runtime_command_adapter(state)
    state.update(step2_online_learning.counterfactual_opportunity_suite(state))
    state.update(step2_online_learning.truth_maintenance_suite(state))
    adapter = step2_online_learning.runtime_command_adapter(state)
    state["runtime_command_adapter"] = adapter
    contracts = step2_online_learning.planner_worker_contracts(state)
    quarantine = step2_online_learning.quarantine_payload(state)

    assert state["learning_claim_verifier"]["claims"]
    assert "confidence" in state["causal_confidence_calibration"]["top_confidence"]
    assert state["false_discovery_early_warning"]["warning_count"] >= 1
    assert state["adaptive_evidence_thresholds"]["mode"] in {"cautious", "strict"}
    assert state["self_debate_search_council"]["recommendations"]
    assert "rule_count" in state["experiment_memory_compression"]
    assert "drift_count" in state["learning_drift_monitor"]
    assert state["promotion_first_autopilot_v2"]["policy_packet"]["objective"] == "maximize_promotion_survivable_live_pnl"
    assert "FAKE|mean|open" in adapter["avoid_routes"]
    assert any(
        job.get("rationale") == "promotion_first_autopilot_v2"
        for contract in contracts["contracts"]
        for job in contract.get("jobs", [])
    )
    assert quarantine["promotion_first_autopilot_v2"]["worker_policy"]


def test_temporal_memory_suite_compiles_next_hunt_opening_policy():
    state = {
        "top_candidates": [
            {
                "variant": "temporal_win",
                "route_key": "TEMP|breakout|late",
                "family_key": "temporal",
                "mutation_lane": "edge_expansion",
                "indicator": "ema",
                "ticker": "TEMP",
                "step2_pnl": 420.0,
                "step2_delta_vs_active": 160.0,
                "promotion_readiness_score": 94.0,
            },
            {
                "variant": "temporal_risk",
                "route_key": "RISK|mean|open",
                "family_key": "risk",
                "mutation_lane": "wild_shuffle",
                "indicator": "vwap",
                "step2_pnl": 260.0,
                "step2_delta_vs_active": 40.0,
                "promotion_readiness_score": 34.0,
                "learning_tags": ["thin_sample", "high_overfit_risk"],
            },
        ],
        "promotion_feedback": [{"variant": "temporal_risk", "decision": "reject"}],
        "runtime_decision_kernel": {
            "command_packet": {
                "live_only": True,
                "focus_routes": ["TEMP|breakout|late", "RISK|mean|open"],
                "commands": [{"action": "probe", "route_key": "TEMP|breakout|late"}],
            }
        },
    }
    state.update(step2_online_learning.real_time_causal_search_suite(state))
    state["runtime_command_adapter"] = step2_online_learning.runtime_command_adapter(state)
    state.update(step2_online_learning.counterfactual_opportunity_suite(state))
    state.update(step2_online_learning.truth_maintenance_suite(state))
    state.update(step2_online_learning.temporal_memory_suite(state))
    adapter = step2_online_learning.runtime_command_adapter(state)
    state["runtime_command_adapter"] = adapter
    contracts = step2_online_learning.planner_worker_contracts(state)
    quarantine = step2_online_learning.quarantine_payload(state)

    assert state["multi_horizon_memory_stack"]["summary"]["next_hunt_routes"] >= 1
    assert state["lesson_half_life_engine_v2"]["lessons"]
    assert state["cross_hunt_strategy_replay"]["replays"]
    assert state["temporal_regime_fingerprinting"]["top_fingerprint"]["ticker"] == "TEMP"
    assert state["longitudinal_promotion_survival_model"]["top_survival_feature"]
    assert "case_count" in state["memory_conflict_court_v2"]
    assert state["strategy_aging_dashboard"]["aging_counts"]
    assert state["next_hunt_opening_policy_compiler"]["policy_packet"]["objective"] == "maximize_temporally_survivable_live_pnl"
    assert "TEMP|breakout|late" in adapter["focus_routes"]
    assert adapter["next_hunt_opening_policy"]["live_only"] is True
    assert any(
        job.get("rationale") == "next_hunt_opening_policy_compiler"
        for contract in contracts["contracts"]
        for job in contract.get("jobs", [])
    )
    assert quarantine["next_hunt_opening_policy_compiler"]["worker_roles"]


def test_active_uncertainty_learning_suite_assigns_question_driven_work():
    state = {
        "top_candidates": [
            {
                "variant": "question_win",
                "route_key": "QUESTION|trend|late",
                "family_key": "question",
                "mutation_lane": "edge_expansion",
                "indicator": "macd",
                "ticker": "QUESTION",
                "step2_pnl": 380.0,
                "step2_delta_vs_active": 140.0,
                "promotion_readiness_score": 82.0,
                "novelty_score": 33.0,
            },
            {
                "variant": "question_repair",
                "route_key": "REPAIRQ|mean|open",
                "family_key": "repairq",
                "mutation_lane": "holdout_repair",
                "step2_pnl": 245.0,
                "step2_delta_vs_active": 32.0,
                "promotion_readiness_score": 42.0,
            },
        ],
        "runtime_decision_kernel": {
            "command_packet": {
                "live_only": True,
                "focus_routes": ["QUESTION|trend|late"],
                "commands": [{"action": "probe", "route_key": "QUESTION|trend|late"}],
            }
        },
    }
    state.update(step2_online_learning.real_time_causal_search_suite(state))
    state.update(step2_online_learning.counterfactual_opportunity_suite(state))
    state.update(step2_online_learning.truth_maintenance_suite(state))
    state.update(step2_online_learning.temporal_memory_suite(state))
    state.update(step2_online_learning.active_uncertainty_learning_suite(state))
    adapter = step2_online_learning.runtime_command_adapter(state)
    state["runtime_command_adapter"] = adapter
    contracts = step2_online_learning.planner_worker_contracts(state)
    quarantine = step2_online_learning.quarantine_payload(state)

    assert state["question_driven_hunt_planner"]["questions"]
    assert state["expected_information_gain_scorer_v2"]["top_score"]["route_key"] == "QUESTION|trend|late"
    assert state["uncertainty_heatmap"]["top_cell"]
    assert state["adaptive_experiment_sequencer"]["next_step"]
    assert "stop_count" in state["learning_value_stop_loss"]
    assert state["causal_question_ledger"]["open_question_count"] >= 1
    assert len(state["worker_epistemic_roles_v2"]["assignments"]) == 4
    assert state["hunt_hypothesis_compiler"]["top_hypothesis"]["route_key"] == "QUESTION|trend|late"
    assert "QUESTION|trend|late" in adapter["focus_routes"]
    assert adapter["question_planner_top"]["route_key"] == "QUESTION|trend|late"
    assert any(
        job.get("rationale") in {"question_driven_hunt_planner", "hunt_hypothesis_compiler"}
        for contract in contracts["contracts"]
        for job in contract.get("jobs", [])
    )
    assert quarantine["worker_epistemic_roles_v2"]["assignments"]


def test_closed_loop_scientific_execution_suite_builds_controlled_contracts():
    state = {
        "top_candidates": [
            {
                "variant": "science_win",
                "route_key": "SCI|breakout|late",
                "family_key": "science",
                "mutation_lane": "edge_expansion",
                "ticker": "SCI",
                "step2_pnl": 440.0,
                "step2_delta_vs_active": 150.0,
                "promotion_readiness_score": 88.0,
            },
            {
                "variant": "science_control",
                "route_key": "CTRL|breakout|late",
                "family_key": "science",
                "mutation_lane": "edge_expansion",
                "ticker": "CTRL",
                "step2_pnl": 300.0,
                "step2_delta_vs_active": 58.0,
                "promotion_readiness_score": 84.0,
            },
        ],
        "runtime_decision_kernel": {
            "command_packet": {
                "live_only": True,
                "focus_routes": ["SCI|breakout|late"],
                "commands": [{"action": "probe", "route_key": "SCI|breakout|late"}],
            }
        },
    }
    state.update(step2_online_learning.real_time_causal_search_suite(state))
    state.update(step2_online_learning.counterfactual_opportunity_suite(state))
    state.update(step2_online_learning.truth_maintenance_suite(state))
    state.update(step2_online_learning.temporal_memory_suite(state))
    state.update(step2_online_learning.active_uncertainty_learning_suite(state))
    state.update(step2_online_learning.closed_loop_scientific_execution_suite(state))
    adapter = step2_online_learning.runtime_command_adapter(state)
    state["runtime_command_adapter"] = adapter
    contracts = step2_online_learning.planner_worker_contracts(state)
    quarantine = step2_online_learning.quarantine_payload(state)

    assert state["experiment_contract_compiler"]["contracts"]
    assert state["control_route_matcher"]["top_match"]["control_route"] == "CTRL|breakout|late"
    assert state["sequential_test_monitor"]["top_decision"]["decision"] in {"scale", "continue", "repair", "stop"}
    assert state["causal_effect_size_ledger"]["top_effect"]["route_key"] == "SCI|breakout|late"
    assert state["false_positive_pressure_gauge"]["mode"] in {"normal", "cautious_control", "strict_control"}
    assert "plans" in state["exploration_debt_paydown_planner"]
    assert state["promotion_aware_power_planner"]["top_plan"]["route_key"] == "SCI|breakout|late"
    assert state["scientific_hunt_executive"]["command_packet"]["objective"] == "run_controlled_scientific_hunt"
    assert "SCI|breakout|late" in adapter["focus_routes"]
    assert adapter["scientific_executive_summary"]["contract_count"] >= 1
    assert any(
        job.get("rationale") == "scientific_hunt_executive"
        for contract in contracts["contracts"]
        for job in contract.get("jobs", [])
    )
    assert quarantine["scientific_hunt_executive"]["commands"]


def test_promotion_grade_evidence_suite_routes_review_packets():
    state = {
        "top_candidates": [
            {
                "variant": "packet_win",
                "route_key": "PACKET|trend|late",
                "family_key": "packet",
                "mutation_lane": "edge_expansion",
                "indicator": "rsi",
                "parent_variant": "live",
                "step2_pnl": 510.0,
                "step2_delta_vs_active": 180.0,
                "promotion_readiness_score": 90.0,
            },
            {
                "variant": "packet_control",
                "route_key": "PACKCTRL|trend|late",
                "family_key": "packet",
                "mutation_lane": "edge_expansion",
                "step2_pnl": 320.0,
                "step2_delta_vs_active": 62.0,
                "promotion_readiness_score": 82.0,
            },
        ],
        "runtime_decision_kernel": {
            "command_packet": {
                "live_only": True,
                "focus_routes": ["PACKET|trend|late"],
                "commands": [{"action": "probe", "route_key": "PACKET|trend|late"}],
            }
        },
    }
    state.update(step2_online_learning.real_time_causal_search_suite(state))
    state.update(step2_online_learning.counterfactual_opportunity_suite(state))
    state.update(step2_online_learning.truth_maintenance_suite(state))
    state.update(step2_online_learning.temporal_memory_suite(state))
    state.update(step2_online_learning.active_uncertainty_learning_suite(state))
    state.update(step2_online_learning.closed_loop_scientific_execution_suite(state))
    state.update(step2_online_learning.promotion_grade_evidence_suite(state))
    adapter = step2_online_learning.runtime_command_adapter(state)
    state["runtime_command_adapter"] = adapter
    contracts = step2_online_learning.planner_worker_contracts(state)
    quarantine = step2_online_learning.quarantine_payload(state)

    assert state["live_candidate_evidence_builder"]["top_packet"]["variant"] == "packet_win"
    assert state["promotion_failure_predictor_v3"]["predictions"]
    assert "tasks" in state["evidence_gap_router"]
    assert "near_ready" in state["review_ready_queue_v2"]
    assert state["promotion_evidence_scorecard"]["top_scorecard"]["route_key"] == "PACKET|trend|late"
    assert state["candidate_lineage_explainer_v2"]["top_explanation"]["parent_variant"] == "live"
    assert state["live_vs_control_differential_report"]["top_report"]["route_key"] == "PACKET|trend|late"
    assert state["promotion_packet_executive"]["top_decision"]["decision"] in {"review", "repair", "shadow", "reject"}
    assert "PACKET|trend|late" in adapter["focus_routes"]
    assert adapter["promotion_packet_executive_summary"]
    assert any(
        job.get("rationale") == "promotion_packet_executive"
        for contract in contracts["contracts"]
        for job in contract.get("jobs", [])
    )
    assert quarantine["promotion_packet_executive"]["commands"]


def test_world_class_meta_learning_suite_governs_learning_system():
    state = {
        "top_candidates": [
            {
                "variant": "world_win",
                "route_key": "WORLD|trend|late",
                "family_key": "world",
                "mutation_lane": "holdout_repair",
                "indicator": "rsi_macd",
                "parent_variant": "live",
                "step2_pnl": 1440.0,
                "step2_delta_vs_active": 840.0,
                "promotion_readiness_score": 92.0,
                "promotion_quality_score": 88.0,
                "novelty_score": 24.0,
                "overfit_risk_score": 12.0,
            },
            {
                "variant": "world_repair",
                "route_key": "REPAIR|mean|open",
                "family_key": "repair",
                "mutation_lane": "day_consistency_repair",
                "indicator": "vwap_adx",
                "parent_variant": "live",
                "step2_pnl": 920.0,
                "step2_delta_vs_active": 210.0,
                "promotion_readiness_score": 58.0,
                "promotion_quality_score": 52.0,
                "novelty_score": 3.0,
                "overfit_risk_score": 68.0,
            },
        ],
        "runtime_decision_kernel": {
            "command_packet": {
                "live_only": True,
                "focus_routes": ["WORLD|trend|late"],
                "commands": [{"action": "probe", "route_key": "WORLD|trend|late"}],
            }
        },
        "search_space_coverage_map": {"blind_spots": [{"route_key": "ODD|shuffle|close"}]},
        "live_beater_scarcity_mode": {"enabled": True},
    }
    state.update(step2_online_learning.active_uncertainty_learning_suite(state))
    state.update(step2_online_learning.closed_loop_scientific_execution_suite(state))
    state.update(step2_online_learning.promotion_grade_evidence_suite(state))
    state.update(step2_online_learning.world_class_meta_learning_suite(state))
    state.update(step2_online_learning.elite_world_class_learning_suite(state))
    state.update(step2_online_learning.proof_oriented_learning_suite(state))
    state.update(step2_online_learning.scientific_control_learning_suite(state))
    state.update(step2_online_learning.world_model_nervous_system_suite(state))
    state.update(step2_online_learning.orchestration_learning_suite(state))
    state.update(step2_online_learning.fitness_selection_suite(state))
    adapter = step2_online_learning.runtime_command_adapter(state)
    state["runtime_command_adapter"] = adapter
    contracts = step2_online_learning.planner_worker_contracts(state)
    quarantine = step2_online_learning.quarantine_payload(state)

    for key in step2_online_learning.WORLD_CLASS_META_LEARNING_ARTIFACTS:
        assert key in state
        assert key in quarantine
    for key in step2_online_learning.ELITE_LEARNING_ARTIFACTS:
        assert key in state
        assert key in quarantine
        assert state[key]["artifact"] == key
    assert len(step2_online_learning.ELITE_LEARNING_ARTIFACTS) == 50
    for key in step2_online_learning.PROOF_LEARNING_ARTIFACTS:
        assert key in state
        assert key in quarantine
        assert state[key]["artifact"] == key
    assert len(step2_online_learning.PROOF_LEARNING_ARTIFACTS) == 15
    for key in step2_online_learning.CLOSED_LOOP_CONTROL_ARTIFACTS:
        assert key in state
        assert key in quarantine
        assert state[key]["artifact"] == key
    assert len(step2_online_learning.CLOSED_LOOP_CONTROL_ARTIFACTS) == 12
    for key in step2_online_learning.WORLD_MODEL_NERVOUS_SYSTEM_ARTIFACTS:
        assert key in state
        assert key in quarantine
        assert state[key]["artifact"] == key
    assert len(step2_online_learning.WORLD_MODEL_NERVOUS_SYSTEM_ARTIFACTS) == 80
    for key in step2_online_learning.ORCHESTRATION_LEARNING_ARTIFACTS:
        assert key in state
        assert key in quarantine
        assert state[key]["artifact"] == key
    assert len(step2_online_learning.ORCHESTRATION_LEARNING_ARTIFACTS) == 80
    for key in step2_online_learning.FITNESS_SELECTION_ARTIFACTS:
        assert key in state
        assert key in quarantine
        assert state[key]["artifact"] == key
    assert len(step2_online_learning.FITNESS_SELECTION_ARTIFACTS) == 8
    assert state["unified_learning_state_reducer"]["top_belief"]["route_key"] == "WORLD|trend|late"
    assert state["candidate_survival_simulator"]["top_simulation"]["route_key"] == "WORLD|trend|late"
    assert state["learning_budget_optimizer"]["top_allocation"]["bucket"] in {"promotion_repair", "novelty_floor"}
    assert state["world_state_next_best_question_engine"]["top_record"]["question"]
    assert state["elite_learning_system_summary"]["summary"]["artifact_count"] == 50
    assert state["meta_learning_governor"]["summary"]["mode"]
    assert adapter["meta_learning_governor_summary"]["mode"] == state["meta_learning_governor"]["mode"]
    assert adapter["elite_learning_summary"]["artifact_count"] == 50
    assert adapter["elite_next_best_question"]["question"]
    assert adapter["proof_learning_summary"]["artifact_count"] == 15
    assert adapter["truth_ledger_top_claim"]["claim"]
    assert adapter["research_agenda_top_question"]["question"]
    assert adapter["review_board_decision"]["board_vote"]
    assert adapter["closed_loop_control_summary"]["artifact_count"] == 12
    assert adapter["bayesian_top_belief"]["posterior_probability"] > 0
    assert adapter["active_experiment_next"]["selected_experiment"]
    assert adapter["learning_status_feed"]["testing_next"]
    assert adapter["world_model_nervous_system_summary"]["artifact_count"] == 80
    assert adapter["nervous_system_top_truth"]["truth_score"] > 0
    assert adapter["nervous_system_top_promotion"]["promotion_survival_probability"] > 0
    assert adapter["nervous_system_world_audit"]["objective"] == "world_model_self_audit_loop"
    assert adapter["orchestration_learning_summary"]["artifact_count"] == 80
    assert adapter["orchestration_top_artifact_governance"]["trust_score"] > 0
    assert adapter["orchestration_top_hunt_exec"]["executive_decision"] == "hunt_executive_controller"
    assert adapter["fitness_selection_summary"]["artifact_count"] == 8
    assert adapter["fitness_top_scorecard"]["fitness_score"]
    assert adapter["fitness_trust_policy"]["policy"] in {"scale", "shadow", "demote"}
    assert adapter["fitness_world_report"]["best_layer"]
    assert "WORLD|trend|late" in adapter["focus_routes"]
    assert any(
        job.get("rationale") in {"meta_learning_governor", "candidate_repair_recipe_generator", "learning_budget_optimizer", "closed_loop_control_learning_summary", "world_model_nervous_system_summary", "orchestration_learning_summary", "fitness_selection_summary"}
        for contract in contracts["contracts"]
        for job in contract.get("jobs", [])
    )


def test_cross_run_opening_controls_seed_runtime_adapter():
    global_memory = {
        "route_prior_model": {
            "priors": [
                {
                    "route_key": "CLSK|trend_pullback|late",
                    "prior_score": 500.0,
                    "reject_rate": 0.0,
                    "mutation_radius": "balanced",
                },
                {
                    "route_key": "BAD|route|x",
                    "prior_score": -500.0,
                    "reject_rate": 0.8,
                    "mutation_radius": "widen",
                },
            ]
        },
        "closed_loop_execution_memory": {
            "runtime_experiments": [
                {"priority": 20.0, "raw": {"treatment": "holdout_repair"}}
            ]
        },
        "failure_memory_feedback": {"directives": [{"route_key": "BAD|route|x", "action": "skip_route"}]},
        "revalidation_candidates": {"queue": [{"route_key": "CLSK|trend_pullback|late"}]},
    }

    controls = step2_online_learning.cross_run_opening_controls(global_memory)
    state = {
        key: value
        for key, value in controls.items()
        if isinstance(value, dict)
    }
    adapter = step2_online_learning.runtime_command_adapter(state)
    contracts = step2_online_learning.planner_worker_contracts(state)
    quarantine = step2_online_learning.quarantine_payload(state)

    assert controls["cross_run_hunt_memory_compiler"]["favor_routes"] == ["CLSK|trend_pullback|late"]
    assert controls["pre_hunt_strategy_selector"]["strategy"] == "validation_heavy"
    assert controls["cold_start_route_pack_generator"]["focus_routes"] == ["CLSK|trend_pullback|late"]
    assert controls["longitudinal_treatment_decay"]["favor_treatments"] == ["holdout_repair"]
    assert "BAD|route|x" in controls["memory_conflict_arbiter"]["avoid_routes"]
    assert controls["memory_provenance_explorer"]["entries"]
    route_scores = [
        row for row in controls["memory_reliability_scorer"]["scores"]
        if row.get("subject_type") == "route"
    ]
    assert route_scores[0]["route_key"] == "CLSK|trend_pullback|late"
    assert "BAD|route|x" in controls["belief_retirement_engine"]["retire_routes"]
    assert controls["memory_falsification_queue"]["queue"]
    assert controls["memory_stress_test_pack"]["tasks"]
    assert controls["durable_memory_compression"]["rules"]
    assert controls["memory_qa_smoke_test"]["passed"] is True
    assert controls["hunt_opening_playbook"]["worker_roles"]
    assert controls["cross_run_learning_regression_test"]["passed"] is True
    assert "CLSK|trend_pullback|late" in adapter["focus_routes"]
    assert "BAD|route|x" in adapter["avoid_routes"]
    assert adapter["memory_falsification_routes"]
    assert adapter["memory_stress_test_routes"]
    assert "BAD|route|x" in adapter["memory_retire_routes"]
    assert any(job.get("rationale", "").startswith("pre_hunt_opening_playbook") for contract in contracts["contracts"] for job in contract.get("jobs", []))
    assert quarantine["pre_hunt_strategy_selector"]["strategy"] == "validation_heavy"
    assert quarantine["memory_qa_smoke_test"]["passed"] is True
def test_telemetry_summary_separates_live_beaters_from_reported_winners():
    summary = step2_online_learning.telemetry_summary([
        {"live_beaters": 0, "winners": 7, "best_pnl": 100.0, "novelty_yield": 2, "alias_rate": 0.0}
    ])

    assert summary["live_beaters"] == 0
    assert summary["reported_winners"] == 7

