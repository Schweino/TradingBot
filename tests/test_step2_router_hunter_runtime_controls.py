import argparse
import json
from pathlib import Path

import step2_router_hunter as router


def test_router_consumes_runtime_command_packet():
    quarantine = {
        "runtime_command_adapter": {
            "live_only": True,
            "focus_routes": ["CLSK|trend_pullback|late", "BAD|route|x"],
            "avoid_routes": ["BAD|route|x"],
            "mutation_width": "tight",
            "batch_size_multiplier": 0.65,
            "commands": [
                {
                    "command_id": "cmd1",
                    "action": "repair",
                    "route_key": "CLSK|trend_pullback|late",
                    "mutation_width": "tight",
                },
                {
                    "command_id": "cmd2",
                    "action": "scale",
                    "route_key": "BAD|route|x",
                },
            ],
            "structural_mutation_routes": ["CLSK|trend_pullback|late"],
            "retire_routes": ["BAD|route|x"],
            "route_budget_caps": {
                "route_caps": [
                    {
                        "route_key": "CLSK|trend_pullback|late",
                        "cap_required": True,
                        "current_repair_share_pct": 80.0,
                        "max_budget_share_pct": 40.0,
                    }
                ]
            },
        }
    }
    path = Path("runtime_router_quarantine_test.json")
    try:
        path.write_text(json.dumps(quarantine), encoding="utf-8")
        args = argparse.Namespace(focus_route=[], focus_plan_json="", quarantine_json=str(path))

        focus = router._load_focus_routes(args)
        seeds = [
            router._focus_seed("CLSK|trend_pullback|late"),
            router._focus_seed("BAD|route|x"),
        ]
        kept = router._filter_quarantined_seeds(seeds, quarantine)
    finally:
        path.unlink(missing_ok=True)

    assert focus == ["CLSK|trend_pullback|late"]
    assert router._parse_route_bucket("MARA|trend_pullback|*") == {"ticker": "MARA", "setup_type": "trend_pullback"}
    assert [router._route_key_from_seed(seed) for seed in kept] == ["CLSK|trend_pullback|late"]
    assert kept[0]["runtime_action"] == "repair"
    assert kept[0]["force_structural_mutation"] is True
    assert kept[0]["budget_cap_multiplier"] == 0.5
    assert kept[0]["route_budget_cap"]["cap_required"] is True
    assert kept[0]["sample_weight"] > seeds[0]["sample_weight"]


def test_router_enforces_promotion_ready_repair_envelope_without_blackholing_anchor():
    quarantine = {
        "runtime_command_adapter": {
            "avoid_routes": ["CLSK|vwap_reclaim_breakdown|*"],
            "focus_routes": ["CLSK|vwap_reclaim_breakdown|*", "RIOT|vwap_reclaim_breakdown|*"],
            "quality_protection_mode": "medium",
            "promotion_ready_repair_envelope": {
                "active": True,
                "target_failures": ["promotion_readiness_below_floor", "route_breadth", "robustness_below_floor"],
                "edge_preservation_routes": ["CLSK|vwap_reclaim_breakdown|*"],
                "close_sibling_routes": ["RIOT|vwap_reclaim_breakdown|*"],
                "allowed_routes": ["CLSK|vwap_reclaim_breakdown|*", "RIOT|vwap_reclaim_breakdown|*"],
                "outside_envelope_sample_weight_multiplier": 0.08,
                "generic_route_deprioritize_patterns": ["*|*|*"],
            },
            "edge_preservation_routes": ["CLSK|vwap_reclaim_breakdown|*"],
            "close_sibling_routes": ["RIOT|vwap_reclaim_breakdown|*"],
            "commands": [
                {
                    "action": "repair",
                    "route_key": "CLSK|vwap_reclaim_breakdown|*",
                    "target_failures": ["promotion_readiness_below_floor", "route_breadth", "robustness_below_floor"],
                    "mutation_width": "micro",
                    "edge_preservation": True,
                },
                {
                    "action": "revalidate",
                    "route_key": "RIOT|vwap_reclaim_breakdown|*",
                    "target_failures": ["promotion_readiness_below_floor", "route_breadth", "robustness_below_floor"],
                    "mutation_width": "tight",
                    "close_sibling_only": True,
                },
            ],
        }
    }
    seeds = [
        router._focus_seed("CLSK|vwap_reclaim_breakdown|*"),
        router._focus_seed("RIOT|vwap_reclaim_breakdown|*"),
        router._focus_seed("MARA|trend_pullback|late"),
    ]

    kept = router._filter_quarantined_seeds(seeds, quarantine)
    by_route = {router._route_key_from_seed(seed): seed for seed in kept}
    weighted_anchor = router._seed_weight(by_route["CLSK|vwap_reclaim_breakdown|*"], quarantine)

    assert "CLSK|vwap_reclaim_breakdown|*" in by_route
    assert by_route["CLSK|vwap_reclaim_breakdown|*"]["edge_preservation_repair"] is True
    assert by_route["RIOT|vwap_reclaim_breakdown|*"]["close_sibling_repair"] is True
    assert by_route["MARA|trend_pullback|late"]["outside_promotion_ready_envelope"] is True
    assert by_route["MARA|trend_pullback|late"]["sample_weight"] < seeds[2]["sample_weight"]
    assert weighted_anchor > by_route["CLSK|vwap_reclaim_breakdown|*"]["sample_weight"] * 0.5


def test_router_observe_mode_does_not_skip_or_throttle():
    quarantine = {
        "runtime_command_adapter": {
            "avoid_routes": ["BAD|route|x"],
            "retire_routes": ["RETIRE|route|x"],
            "mutation_width": "tight",
            "batch_size_multiplier": 0.65,
            "degrade_mode": True,
        }
    }
    seeds = [
        router._focus_seed("BAD|route|x"),
        router._focus_seed("RETIRE|route|x"),
        router._focus_seed("GOOD|route|x"),
    ]

    kept = router._filter_quarantined_seeds(seeds, quarantine, enforce_runtime_controls=False)
    adapter = router._runtime_adapter_for_execution(
        quarantine["runtime_command_adapter"],
        enforce_runtime_controls=False,
        exact_variant_count=True,
    )

    assert [router._route_key_from_seed(seed) for seed in kept] == [
        "BAD|route|x",
        "RETIRE|route|x",
        "GOOD|route|x",
    ]
    assert adapter["control_mode"] == "observe"
    assert adapter["avoid_routes"] == []
    assert adapter["retire_routes"] == []
    assert adapter["degrade_mode"] is False
    assert adapter["batch_size_multiplier"] == 1.0
    assert adapter["observed_batch_size_multiplier"] == 0.65
    assert adapter["exact_variant_count"] is True


def test_router_scored_manifest_persists_every_scored_row():
    path = Path("runtime") / "test_router_scored_manifest.jsonl"
    rows = [
        {"variant": "router_a", "weights": {"ema": 1.0}, "step2_pnl": 101.0},
        {"variant": "router_b", "weights": {"ema": 2.0}, "step2_pnl": 102.0},
    ]
    try:
        path.unlink(missing_ok=True)
        router._append_scored_manifest(path, rows, batch_idx=3)
        loaded = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    finally:
        path.unlink(missing_ok=True)

    assert [row["variant"] for row in loaded] == ["router_a", "router_b"]
    assert {row["manifest_batch"] for row in loaded} == {3}
    assert {row["manifest_source"] for row in loaded} == {"step2_router_hunter"}


def test_router_fallback_consumes_memory_quality_controls():
    quarantine = {
        "degraded_runtime_command_packet": {
            "focus_routes": ["OLD|route|x"],
            "avoid_routes": [],
            "max_workers": 4,
        },
        "memory_falsification_queue": {"focus_routes": ["TEST|route|x"]},
        "memory_stress_test_pack": {"focus_routes": ["STRESS|route|x"]},
        "belief_retirement_engine": {"retire_routes": ["OLD|route|x"]},
        "current_vs_historical_disagreement_monitor": {
            "falsify_routes": ["DISAGREE|route|x"],
            "override_history_routes": ["BLOCK|route|x"],
        },
        "hypothesis_market_maker": {"focus_routes": ["HYP|route|x"]},
        "real_time_bet_sizer": {"focus_routes": ["BET|route|x"]},
        "contrarian_generator": {"focus_routes": ["ODD|route|x"]},
        "learning_stop_loss": {"avoid_routes": ["KILL|route|x"]},
        "breakthrough_detector": {"focus_routes": ["WOW|route|x"]},
        "pattern_to_recipe_compiler": {"focus_routes": ["RECIPE|route|x"]},
        "budget_reallocator": {
            "focus_routes": ["BUDGET|route|x"],
            "avoid_routes": ["SPENDKILL|route|x"],
            "batch_size_multiplier": 0.8,
            "mutation_width": "tight",
        },
        "time_aware_hunt_plan": {
            "focus_routes": ["TIME|route|x"],
            "avoid_routes": ["TIMEKILL|route|x"],
            "batch_size_multiplier": 0.75,
            "mutation_width": "wide",
        },
        "creative_leap_scorer": {"focus_routes": ["LEAP|route|x"]},
        "novelty_budget_governor": {
            "focus_routes": ["NOVEL|route|x"],
            "batch_size_multiplier": 1.2,
            "mutation_width": "wide",
        },
        "creative_brief_compiler": {"focus_routes": ["BRIEF|route|x"]},
        "module_conflict_detector": {"pause_routes": ["METAKILL|route|x"]},
        "module_budget_governor": {
            "pause_routes": ["METABUDGETKILL|route|x"],
            "batch_size_multiplier": 0.9,
        },
        "causal_intervention_scheduler": {"focus_routes": ["SCIENCE|route|x"]},
        "adaptive_search_temperature_controller": {
            "focus_routes": ["TEMP|route|x"],
            "avoid_routes": ["TEMPKILL|route|x"],
            "mutation_width": "tight",
            "batch_size_multiplier": 0.8,
        },
        "route_interaction_learner": {"focus_routes": ["PAIR|route|x"]},
        "false_discovery_firewall": {"avoid_routes": ["FAKE|route|x"]},
        "hunt_strategy_compiler_v2": {
            "focus_routes": ["STRATEGY|route|x"],
            "avoid_routes": ["STRATEGYKILL|route|x"],
            "mutation_width": "wide",
        },
        "learning_disagreement_court": {
            "retest_routes": ["COURT|route|x"],
            "pause_routes": ["COURTKILL|route|x"],
        },
        "promotion_aware_search_objective": {
            "focus_routes": ["PROMO|route|x"],
            "avoid_routes": ["PROMOKILL|route|x"],
            "batch_size_multiplier": 0.88,
        },
        "regime_conditioned_strategy_selector": {
            "focus_routes": ["REGIME|route|x"],
            "avoid_routes": ["REGIMEKILL|route|x"],
            "mutation_width": "tight",
        },
        "exploration_debt_ledger": {"focus_routes": ["DEBT|route|x"]},
        "adversarial_strategy_red_team": {"avoid_routes": ["REDKILL|route|x"]},
        "autonomous_pivot_governor": {
            "focus_routes": ["PIVOT|route|x"],
            "avoid_routes": ["PIVOTKILL|route|x"],
        },
        "runtime_control_simplifier": {
            "focus_routes": ["SIMPLE|route|x"],
            "avoid_routes": ["SIMPLEKILL|route|x"],
            "mutation_width": "tight",
            "batch_size_multiplier": 0.9,
        },
        "live_learning_dashboard_feed": {
            "status": "healthy",
            "focus_routes": ["DASH|route|x"],
            "avoid_routes": ["DASHKILL|route|x"],
        },
        "hunt_runbook_compiler": {
            "focus_routes": ["RUNBOOK|route|x"],
            "avoid_routes": ["RUNBOOKKILL|route|x"],
            "worker_count": 4,
        },
        "learning_failure_sentinel": {
            "ok": False,
            "failure_count": 1,
            "pause_routes": ["SENTINELKILL|route|x"],
        },
        "pre_hunt_readiness_gate": {
            "ready": False,
            "block_routes": ["READYKILL|route|x"],
        },
        "online_causal_bandit": {
            "top_lane": "edge_expansion",
            "focus_routes": ["BANDIT|route|x"],
            "mutation_width": "wide",
        },
        "variant_dna_attribution": {
            "top_gene": {"gene": "route:DNA|route|x"},
            "focus_routes": ["DNA|route|x"],
        },
        "negative_gene_suppression": {"avoid_routes": ["GENEKILL|route|x"]},
        "live_winner_family_tree": {"focus_routes": ["FAMILY|route|x"]},
        "exploration_frontier_map": {"focus_routes": ["FRONTIER|route|x"], "mutation_width": "wide"},
        "adaptive_worker_personalities": {"focus_routes": ["PERSONA|route|x"]},
        "cycle_level_learning_delta": {
            "summary": "top_lane=edge_expansion",
            "focus_routes": ["CYCLE|route|x"],
            "avoid_routes": ["CYCLEKILL|route|x"],
        },
        "promotion_rejection_predictor_v2": {
            "repair_routes": ["REPAIR|route|x"],
            "avoid_routes": ["REJECTKILL|route|x"],
            "highest_risk": {"variant": "bad", "risk_score": 88.0},
        },
        "counterfactual_hunt_simulator": {
            "focus_routes": ["COUNT|route|x"],
            "top_counterfactual": {"route_key": "COUNT|route|x", "expected_live_lift": 44.0},
        },
        "missed_winner_detector": {"focus_routes": ["MISS|route|x"], "missed_count": 1},
        "causal_regret_ledger": {
            "focus_routes": ["REGRET|route|x"],
            "avoid_routes": ["REGRETKILL|route|x"],
            "top_regret": {"decision": "focus_route_omission"},
        },
        "adaptive_search_grammar_generator": {"focus_routes": ["GRAMMAR|route|x"]},
        "live_hypothesis_kill_scale_court": {
            "focus_routes": ["COURTSCALE|route|x"],
            "avoid_routes": ["COURTKILL2|route|x"],
        },
        "promotion_survival_shadow_scoring": {"focus_routes": ["SHADOW|route|x"]},
        "hunt_autopilot_policy_compiler": {
            "focus_routes": ["AUTO|route|x"],
            "avoid_routes": ["AUTOKILL|route|x"],
            "batch_size_multiplier": 1.05,
            "policy_packet": {"live_only": True},
        },
        "causal_confidence_calibration": {
            "focus_routes": ["CONF|route|x"],
            "pause_routes": ["CONFKILL|route|x"],
        },
        "false_discovery_early_warning": {
            "warning_count": 2,
            "avoid_routes": ["WARNKILL|route|x"],
        },
        "self_debate_search_council": {
            "focus_routes": ["DEBATE|route|x"],
            "avoid_routes": ["DEBATEKILL|route|x"],
        },
        "learning_drift_monitor": {"retire_routes": ["DRIFTKILL|route|x"]},
        "promotion_first_autopilot_v2": {
            "focus_routes": ["PROMOFIRST|route|x"],
            "avoid_routes": ["PROMOFIRSTKILL|route|x"],
            "policy_packet": {"live_only": True, "objective": "maximize_promotion_survivable_live_pnl"},
        },
        "multi_horizon_memory_stack": {
            "summary": {"next_hunt_routes": 2},
            "focus_routes": ["HORIZON|route|x"],
            "avoid_routes": ["HORIZONKILL|route|x"],
        },
        "lesson_half_life_engine_v2": {
            "top_lesson": {"lesson": "refresh this route"},
            "refresh_routes": ["HALFLIFE|route|x"],
            "decay_routes": ["HALFLIFEKILL|route|x"],
        },
        "cross_hunt_strategy_replay": {
            "top_replay": {"route_key": "REPLAY|route|x"},
            "focus_routes": ["REPLAY|route|x"],
            "avoid_routes": ["REPLAYKILL|route|x"],
        },
        "temporal_regime_fingerprinting": {
            "top_fingerprint": {"fingerprint": "REGIMEFP"},
            "focus_routes": ["REGIMEFP|route|x"],
        },
        "longitudinal_promotion_survival_model": {
            "top_survival_feature": {"feature": "route:SURVIVE|route|x"},
            "survival_focus_routes": ["SURVIVE|route|x"],
            "risk_avoid_routes": ["SURVIVEKILL|route|x"],
        },
        "memory_conflict_court_v2": {
            "case_count": 2,
            "focus_routes": ["COURTV2|route|x"],
            "falsify_routes": ["FALSIFYV2|route|x"],
            "avoid_routes": ["COURTV2KILL|route|x"],
        },
        "strategy_aging_dashboard": {
            "aging_counts": {"fresh": 1},
            "resurrect_routes": ["AGING|route|x"],
            "retire_routes": ["AGINGKILL|route|x"],
        },
        "next_hunt_opening_policy_compiler": {
            "policy_packet": {"live_only": True, "opening_mode": "promotion_survival_opening"},
            "focus_routes": ["OPENINGV2|route|x"],
            "avoid_routes": ["OPENINGV2KILL|route|x"],
            "mutation_width": "medium",
            "batch_size_multiplier": 1.1,
        },
        "question_driven_hunt_planner": {
            "top_question": {"route_key": "QUESTION|route|x", "question": "Does it survive?"},
            "focus_routes": ["QUESTION|route|x"],
        },
        "expected_information_gain_scorer_v2": {
            "top_score": {"route_key": "EIG|route|x", "expected_information_gain": 77.0},
            "focus_routes": ["EIG|route|x"],
        },
        "uncertainty_heatmap": {
            "top_cell": {"route_key": "HEAT|route|x", "uncertainty_score": 66.0},
            "probe_routes": ["HEAT|route|x"],
        },
        "adaptive_experiment_sequencer": {
            "next_step": {"route_key": "SEQ|route|x"},
            "focus_routes": ["SEQ|route|x"],
            "avoid_routes": ["SEQKILL|route|x"],
        },
        "learning_value_stop_loss": {
            "stop_count": 1,
            "avoid_routes": ["LEARNKILL|route|x"],
        },
        "causal_question_ledger": {
            "top_question": {"route_key": "LEDGER|route|x"},
            "focus_routes": ["LEDGER|route|x"],
        },
        "worker_epistemic_roles_v2": {
            "assignments": [{"worker": "worker_1", "epistemic_role": "skeptic"}],
            "focus_routes": ["ROLE|route|x"],
        },
        "hunt_hypothesis_compiler": {
            "hypotheses": [{"hypothesis_id": "h1", "route_key": "HYPO|route|x"}],
            "focus_routes": ["HYPO|route|x"],
        },
        "experiment_contract_compiler": {
            "top_contract": {"contract_id": "c1", "route_key": "CONTRACT|route|x"},
            "focus_routes": ["CONTRACT|route|x"],
        },
        "control_route_matcher": {
            "top_match": {"contract_id": "c1", "control_route": "CONTROL|route|x"},
            "control_routes": ["CONTROL|route|x"],
        },
        "sequential_test_monitor": {
            "top_decision": {"route_key": "SEQTEST|route|x", "decision": "scale"},
            "focus_routes": ["SEQTEST|route|x"],
            "avoid_routes": ["SEQTESTKILL|route|x"],
        },
        "causal_effect_size_ledger": {
            "top_effect": {"route_key": "EFFECT|route|x", "effect_size": 42.0},
            "focus_routes": ["EFFECT|route|x"],
        },
        "false_positive_pressure_gauge": {
            "pressure_score": 55.0,
            "mode": "cautious_control",
            "avoid_routes": ["PRESSUREKILL|route|x"],
            "batch_size_multiplier": 0.88,
        },
        "exploration_debt_paydown_planner": {
            "top_plan": {"route_key": "PAYDOWN|route|x"},
            "focus_routes": ["PAYDOWN|route|x"],
        },
        "promotion_aware_power_planner": {
            "top_plan": {"route_key": "POWER|route|x"},
            "focus_routes": ["POWER|route|x"],
        },
        "scientific_hunt_executive": {
            "summary": {"contract_count": 1, "pressure_mode": "cautious_control"},
            "top_command": {"route_key": "SCIEXEC|route|x"},
            "focus_routes": ["SCIEXEC|route|x"],
            "avoid_routes": ["SCIEXECKILL|route|x"],
            "command_packet": {"live_only": True, "mutation_width": "tight", "batch_size_multiplier": 0.82},
            "commands": [{"command_id": "sc1", "route_key": "SCIEXEC|route|x"}],
        },
        "live_candidate_evidence_builder": {
            "top_packet": {"route_key": "EVIDENCE|route|x", "evidence_score": 80.0},
            "focus_routes": ["EVIDENCE|route|x"],
        },
        "promotion_failure_predictor_v3": {
            "highest_risk": {"route_key": "RISK3|route|x", "predicted_reject_probability": 0.7},
            "repair_routes": ["RISK3|route|x"],
            "avoid_routes": ["RISK3KILL|route|x"],
        },
        "evidence_gap_router": {
            "top_task": {"route_key": "GAP|route|x", "gap": "controlled_lift"},
            "focus_routes": ["GAP|route|x"],
        },
        "review_ready_queue_v2": {
            "top_ready": {"route_key": "READY2|route|x"},
            "focus_routes": ["READY2|route|x"],
        },
        "promotion_evidence_scorecard": {
            "top_scorecard": {"route_key": "SCORECARD|route|x", "grade": "A"},
        },
        "candidate_lineage_explainer_v2": {
            "top_explanation": {"route_key": "LINEAGE|route|x", "parent_variant": "live"},
        },
        "live_vs_control_differential_report": {
            "top_report": {"route_key": "DIFF|route|x", "differential": 33.0},
            "focus_routes": ["DIFF|route|x"],
        },
        "promotion_packet_executive": {
            "summary": {"review": 1, "repair": 0, "shadow": 0, "reject": 0},
            "top_decision": {"route_key": "PACKEXEC|route|x", "decision": "review"},
            "focus_routes": ["PACKEXEC|route|x"],
            "avoid_routes": ["PACKKILL|route|x"],
        },
        "unified_learning_state_reducer": {
            "summary": {"top_route": "WORLD|route|x"},
            "top_belief": {"route_key": "WORLD|route|x"},
            "focus_routes": ["WORLD|route|x"],
            "avoid_routes": ["WORLDKILL|route|x"],
        },
        "artifact_priority_arbitration_engine": {
            "top_artifact": {"artifact": "promotion_packet_executive"},
            "focus_routes": ["PRIORITY|route|x"],
            "avoid_routes": ["PRIORITYKILL|route|x"],
        },
        "evidence_provenance_graph": {"top_node": {"id": "route:WORLD|route|x"}},
        "counterfactual_promotion_replay": {
            "top_replay": {"route_key": "REPLAY2|route|x"},
            "repair_routes": ["REPLAY2|route|x"],
            "avoid_routes": ["REPLAY2KILL|route|x"],
        },
        "candidate_survival_simulator": {
            "top_simulation": {"route_key": "SURV2|route|x"},
            "focus_routes": ["SURV2|route|x"],
            "avoid_routes": ["SURV2KILL|route|x"],
        },
        "live_regime_shift_detector_v2": {
            "mode": "new_regime_explore",
            "shift_score": 0.7,
            "focus_routes": ["SHIFT|route|x"],
            "discount_routes": ["SHIFTKILL|route|x"],
        },
        "adaptive_trust_weights_per_module": {
            "top_module": {"module": "promotion_packet_executive"},
            "focus_routes": ["TRUST|route|x"],
        },
        "worker_skill_elo_v2": {"top_worker": {"worker": "worker_1", "elo": 1100.0}},
        "route_genome_knowledge_graph": {
            "summary": {"route_count": 1},
            "focus_routes": ["GRAPH|route|x"],
        },
        "causal_feature_interaction_miner": {
            "top_interaction": {"genes": ["a", "b"]},
            "focus_routes": ["INTERACT|route|x"],
        },
        "overfit_signature_library": {
            "top_signature": {"signature": "thin_holdout"},
            "avoid_routes": ["OVERFITKILL|route|x"],
        },
        "review_rejection_memory_bank_v2": {
            "top_memory": {"route_key": "REJMEM|route|x"},
            "repair_routes": ["REJMEM|route|x"],
            "avoid_routes": ["REJMEMKILL|route|x"],
        },
        "learning_budget_optimizer": {
            "top_allocation": {"bucket": "promotion_repair"},
            "focus_routes": ["BUDGET2|route|x"],
            "batch_size_multiplier": 1.05,
        },
        "search_novelty_floor": {
            "summary": {"floor_pct": 28.0},
            "focus_routes": ["NOVEL2|route|x"],
            "mutation_width": "wide",
        },
        "breakthrough_escalation_protocol": {
            "top_breakthrough": {"route_key": "BREAK2|route|x"},
            "focus_routes": ["BREAK2|route|x"],
        },
        "false_discovery_backpressure_controller": {
            "pressure_score": 80.0,
            "mode": "strict_backpressure",
            "mutation_width": "tight",
            "batch_size_multiplier": 0.9,
            "avoid_routes": ["BACKKILL|route|x"],
        },
        "multi_armed_strategy_portfolio": {
            "top_arm": {"arm": "promotion_packet"},
            "focus_routes": ["PORT2|route|x"],
        },
        "historical_lesson_ab_harness": {"top_test": {"lesson": "test lesson"}},
        "promotion_packet_diff_engine": {"top_diff": {"left_route": "WORLD|route|x"}},
        "candidate_repair_recipe_generator": {
            "top_recipe": {"route_key": "RECIPE2|route|x"},
            "focus_routes": ["RECIPE2|route|x"],
        },
        "automated_red_team_reviewer": {
            "top_objection": {"route_key": "RED2|route|x"},
            "avoid_routes": ["RED2KILL|route|x"],
        },
        "learning_compression_field_manual": {"top_rule": {"rule": "repair before review"}},
        "hunt_outcome_attribution_v2": {
            "top_attribution": {"route_key": "ATTR|route|x"},
            "focus_routes": ["ATTR|route|x"],
        },
        "world_state_dashboard_artifact": {
            "summary": {"backpressure": "strict_backpressure"},
            "focus_routes": ["DASH2|route|x"],
            "avoid_routes": ["DASH2KILL|route|x"],
        },
        "meta_learning_governor": {
            "summary": {"mode": "truth_first_repair"},
            "focus_routes": ["META2|route|x"],
            "avoid_routes": ["META2KILL|route|x"],
            "mutation_width": "tight",
            "batch_size_multiplier": 0.9,
        },
        "world_state_next_best_question_engine": {
            "artifact": "world_state_next_best_question_engine",
            "top_record": {"question": "What should we repair next?", "route_key": "QUESTION2|route|x"},
            "focus_routes": ["QUESTION2|route|x"],
            "mutation_width": "medium",
            "batch_size_multiplier": 1.0,
        },
        "elite_learning_system_summary": {
            "summary": {"artifact_count": 50, "top_action": {"route_key": "QUESTION2|route|x"}},
        },
        "learning_truth_ledger": {
            "artifact": "learning_truth_ledger",
            "top_record": {"claim": "candidate_has_live_edge_worth_testing", "route_key": "TRUTH|route|x"},
            "focus_routes": ["TRUTH|route|x"],
            "mutation_width": "tight",
            "batch_size_multiplier": 0.72,
        },
        "adaptive_research_agenda": {
            "artifact": "adaptive_research_agenda",
            "top_record": {"question": "What proves this edge?", "route_key": "AGENDA|route|x"},
            "focus_routes": ["AGENDA|route|x"],
        },
        "autonomous_hunt_review_board": {
            "artifact": "autonomous_hunt_review_board",
            "top_record": {"board_vote": {"skeptic": "test"}, "route_key": "BOARD|route|x"},
            "focus_routes": ["BOARD|route|x"],
        },
        "proof_learning_system_summary": {
            "summary": {"artifact_count": 15, "top_action": {"route_key": "TRUTH|route|x"}},
        },
        "bayesian_belief_engine": {
            "artifact": "bayesian_belief_engine",
            "top_record": {"posterior_probability": 0.81, "route_key": "BAYES|route|x"},
            "focus_routes": ["BAYES|route|x"],
            "mutation_width": "tight",
            "batch_size_multiplier": 0.78,
        },
        "active_experiment_selector": {
            "artifact": "active_experiment_selector",
            "top_record": {"selected_experiment": "controlled_trait_shuffle", "route_key": "EIGCTRL|route|x"},
            "focus_routes": ["EIGCTRL|route|x"],
        },
        "real_time_learning_dashboard_feed": {
            "artifact": "real_time_learning_dashboard_feed",
            "top_record": {"testing_next": "controlled_trait_shuffle", "route_key": "FEED|route|x"},
            "focus_routes": ["FEED|route|x"],
        },
        "closed_loop_control_learning_summary": {
            "summary": {"artifact_count": 12, "top_action": {"route_key": "BAYES|route|x"}},
        },
        "hypothesis_dependency_graph": {
            "artifact": "hypothesis_dependency_graph",
            "top_record": {"truth_score": 88.0, "route_key": "NERVOUS|truth|x"},
            "focus_routes": ["NERVOUS|truth|x"],
            "mutation_width": "tight",
            "batch_size_multiplier": 0.76,
        },
        "promotion_packet_completeness_optimizer": {
            "artifact": "promotion_packet_completeness_optimizer",
            "top_record": {"promotion_survival_probability": 0.74, "route_key": "NERVOUS|promo|x"},
            "focus_routes": ["NERVOUS|promo|x"],
        },
        "world_model_self_audit_loop": {
            "artifact": "world_model_self_audit_loop",
            "top_record": {"objective": "world_model_self_audit_loop", "route_key": "NERVOUS|audit|x"},
            "focus_routes": ["NERVOUS|audit|x"],
        },
        "world_model_nervous_system_summary": {
            "summary": {"artifact_count": 80, "top_action": {"route_key": "NERVOUS|truth|x"}},
        },
        "artifact_governance_dashboard": {
            "artifact": "artifact_governance_dashboard",
            "top_record": {"trust_score": 91.0, "route_key": "ORCH|artifact|x"},
            "focus_routes": ["ORCH|artifact|x"],
            "mutation_width": "tight",
            "batch_size_multiplier": 0.74,
        },
        "hunt_executive_controller": {
            "artifact": "hunt_executive_controller",
            "top_record": {"executive_decision": "hunt_executive_controller", "route_key": "ORCH|hunt|x"},
            "focus_routes": ["ORCH|hunt|x"],
        },
        "orchestration_learning_summary": {
            "summary": {"artifact_count": 80, "top_action": {"route_key": "ORCH|artifact|x"}},
        },
        "artifact_fitness_scorecard": {
            "artifact": "artifact_fitness_scorecard",
            "top_record": {"fitness_score": 123.0, "layer": "closed_loop_control", "route_key": "FIT|score|x"},
            "focus_routes": ["FIT|score|x"],
            "mutation_width": "tight",
            "batch_size_multiplier": 0.78,
        },
        "learning_layer_trust_policy": {
            "artifact": "learning_layer_trust_policy",
            "top_record": {"policy": "scale", "layer": "closed_loop_control", "route_key": "FIT|trust|x"},
            "focus_routes": ["FIT|trust|x"],
        },
        "world_model_fitness_report": {
            "artifact": "world_model_fitness_report",
            "top_record": {"best_layer": "closed_loop_control", "route_key": "FIT|report|x"},
            "focus_routes": ["FIT|report|x"],
        },
        "fitness_selection_summary": {
            "summary": {"artifact_count": 8, "top_action": {"route_key": "FIT|score|x"}},
        },
    }

    adapter = router._runtime_adapter_from_quarantine(quarantine)

    assert "TEST|route|x" in adapter["focus_routes"]
    assert "STRESS|route|x" in adapter["focus_routes"]
    assert "DISAGREE|route|x" in adapter["focus_routes"]
    assert "HYP|route|x" in adapter["focus_routes"]
    assert "BET|route|x" in adapter["focus_routes"]
    assert "ODD|route|x" in adapter["focus_routes"]
    assert "WOW|route|x" in adapter["focus_routes"]
    assert "RECIPE|route|x" in adapter["focus_routes"]
    assert "BUDGET|route|x" in adapter["focus_routes"]
    assert "TIME|route|x" in adapter["focus_routes"]
    assert "LEAP|route|x" in adapter["focus_routes"]
    assert "NOVEL|route|x" in adapter["focus_routes"]
    assert "BRIEF|route|x" in adapter["focus_routes"]
    assert "SCIENCE|route|x" in adapter["focus_routes"]
    assert "TEMP|route|x" in adapter["focus_routes"]
    assert "PAIR|route|x" in adapter["focus_routes"]
    assert "STRATEGY|route|x" in adapter["focus_routes"]
    assert "COURT|route|x" in adapter["focus_routes"]
    assert "PROMO|route|x" in adapter["focus_routes"]
    assert "REGIME|route|x" in adapter["focus_routes"]
    assert "DEBT|route|x" in adapter["focus_routes"]
    assert "PIVOT|route|x" in adapter["focus_routes"]
    assert "SIMPLE|route|x" in adapter["focus_routes"]
    assert "DASH|route|x" in adapter["focus_routes"]
    assert "RUNBOOK|route|x" in adapter["focus_routes"]
    assert "BANDIT|route|x" in adapter["focus_routes"]
    assert "DNA|route|x" in adapter["focus_routes"]
    assert "FAMILY|route|x" in adapter["focus_routes"]
    assert "FRONTIER|route|x" in adapter["focus_routes"]
    assert "PERSONA|route|x" in adapter["focus_routes"]
    assert "CYCLE|route|x" in adapter["focus_routes"]
    assert "REPAIR|route|x" in adapter["focus_routes"]
    assert "COUNT|route|x" in adapter["focus_routes"]
    assert "MISS|route|x" in adapter["focus_routes"]
    assert "REGRET|route|x" in adapter["focus_routes"]
    assert "GRAMMAR|route|x" in adapter["focus_routes"]
    assert "COURTSCALE|route|x" in adapter["focus_routes"]
    assert "SHADOW|route|x" in adapter["focus_routes"]
    assert "AUTO|route|x" in adapter["focus_routes"]
    assert "CONF|route|x" in adapter["focus_routes"]
    assert "DEBATE|route|x" in adapter["focus_routes"]
    assert "PROMOFIRST|route|x" in adapter["focus_routes"]
    assert "HORIZON|route|x" in adapter["focus_routes"]
    assert "HALFLIFE|route|x" in adapter["focus_routes"]
    assert "REPLAY|route|x" in adapter["focus_routes"]
    assert "REGIMEFP|route|x" in adapter["focus_routes"]
    assert "SURVIVE|route|x" in adapter["focus_routes"]
    assert "COURTV2|route|x" in adapter["focus_routes"]
    assert "FALSIFYV2|route|x" in adapter["focus_routes"]
    assert "AGING|route|x" in adapter["focus_routes"]
    assert "OPENINGV2|route|x" in adapter["focus_routes"]
    assert "QUESTION|route|x" in adapter["focus_routes"]
    assert "EIG|route|x" in adapter["focus_routes"]
    assert "HEAT|route|x" in adapter["focus_routes"]
    assert "SEQ|route|x" in adapter["focus_routes"]
    assert "LEDGER|route|x" in adapter["focus_routes"]
    assert "ROLE|route|x" in adapter["focus_routes"]
    assert "HYPO|route|x" in adapter["focus_routes"]
    assert "CONTRACT|route|x" in adapter["focus_routes"]
    assert "CONTROL|route|x" in adapter["focus_routes"]
    assert "SEQTEST|route|x" in adapter["focus_routes"]
    assert "EFFECT|route|x" in adapter["focus_routes"]
    assert "PAYDOWN|route|x" in adapter["focus_routes"]
    assert "POWER|route|x" in adapter["focus_routes"]
    assert "SCIEXEC|route|x" in adapter["focus_routes"]
    assert "EVIDENCE|route|x" in adapter["focus_routes"]
    assert "RISK3|route|x" in adapter["focus_routes"]
    assert "GAP|route|x" in adapter["focus_routes"]
    assert "READY2|route|x" in adapter["focus_routes"]
    assert "DIFF|route|x" in adapter["focus_routes"]
    assert "PACKEXEC|route|x" in adapter["focus_routes"]
    assert "WORLD|route|x" in adapter["focus_routes"]
    assert "BUDGET2|route|x" in adapter["focus_routes"]
    assert "META2|route|x" in adapter["focus_routes"]
    assert "QUESTION2|route|x" in adapter["focus_routes"]
    assert "TRUTH|route|x" in adapter["focus_routes"]
    assert "AGENDA|route|x" in adapter["focus_routes"]
    assert "BAYES|route|x" in adapter["focus_routes"]
    assert "EIGCTRL|route|x" in adapter["focus_routes"]
    assert "NERVOUS|truth|x" in adapter["focus_routes"]
    assert "NERVOUS|promo|x" in adapter["focus_routes"]
    assert "ORCH|artifact|x" in adapter["focus_routes"]
    assert "ORCH|hunt|x" in adapter["focus_routes"]
    assert "FIT|score|x" in adapter["focus_routes"]
    assert "FIT|trust|x" in adapter["focus_routes"]
    assert "OLD|route|x" not in adapter["focus_routes"]
    assert "OLD|route|x" in adapter["avoid_routes"]
    assert "BLOCK|route|x" in adapter["avoid_routes"]
    assert "KILL|route|x" in adapter["avoid_routes"]
    assert "SPENDKILL|route|x" in adapter["avoid_routes"]
    assert "TIMEKILL|route|x" in adapter["avoid_routes"]
    assert "METAKILL|route|x" in adapter["avoid_routes"]
    assert "METABUDGETKILL|route|x" in adapter["avoid_routes"]
    assert "TEMPKILL|route|x" in adapter["avoid_routes"]
    assert "FAKE|route|x" in adapter["avoid_routes"]
    assert "STRATEGYKILL|route|x" in adapter["avoid_routes"]
    assert "COURTKILL|route|x" in adapter["avoid_routes"]
    assert "PROMOKILL|route|x" in adapter["avoid_routes"]
    assert "REGIMEKILL|route|x" in adapter["avoid_routes"]
    assert "REDKILL|route|x" in adapter["avoid_routes"]
    assert "PIVOTKILL|route|x" in adapter["avoid_routes"]
    assert "SIMPLEKILL|route|x" in adapter["avoid_routes"]
    assert "DASHKILL|route|x" in adapter["avoid_routes"]
    assert "RUNBOOKKILL|route|x" in adapter["avoid_routes"]
    assert "SENTINELKILL|route|x" in adapter["avoid_routes"]
    assert "READYKILL|route|x" in adapter["avoid_routes"]
    assert "GENEKILL|route|x" in adapter["avoid_routes"]
    assert "CYCLEKILL|route|x" in adapter["avoid_routes"]
    assert "REJECTKILL|route|x" in adapter["avoid_routes"]
    assert "REGRETKILL|route|x" in adapter["avoid_routes"]
    assert "COURTKILL2|route|x" in adapter["avoid_routes"]
    assert "AUTOKILL|route|x" in adapter["avoid_routes"]
    assert "CONFKILL|route|x" in adapter["avoid_routes"]
    assert "WARNKILL|route|x" in adapter["avoid_routes"]
    assert "DEBATEKILL|route|x" in adapter["avoid_routes"]
    assert "DRIFTKILL|route|x" in adapter["avoid_routes"]
    assert "PROMOFIRSTKILL|route|x" in adapter["avoid_routes"]
    assert "HORIZONKILL|route|x" in adapter["avoid_routes"]
    assert "HALFLIFEKILL|route|x" in adapter["avoid_routes"]
    assert "REPLAYKILL|route|x" in adapter["avoid_routes"]
    assert "SURVIVEKILL|route|x" in adapter["avoid_routes"]
    assert "COURTV2KILL|route|x" in adapter["avoid_routes"]
    assert "AGINGKILL|route|x" in adapter["avoid_routes"]
    assert "OPENINGV2KILL|route|x" in adapter["avoid_routes"]
    assert "SEQKILL|route|x" in adapter["avoid_routes"]
    assert "LEARNKILL|route|x" in adapter["avoid_routes"]
    assert "SEQTESTKILL|route|x" in adapter["avoid_routes"]
    assert "PRESSUREKILL|route|x" in adapter["avoid_routes"]
    assert "SCIEXECKILL|route|x" in adapter["avoid_routes"]
    assert "RISK3KILL|route|x" in adapter["avoid_routes"]
    assert "PACKKILL|route|x" in adapter["avoid_routes"]
    assert "WORLDKILL|route|x" in adapter["avoid_routes"]
    assert "BACKKILL|route|x" in adapter["avoid_routes"]
    assert "META2KILL|route|x" in adapter["avoid_routes"]
    assert adapter["mutation_width"] == "tight"
    assert adapter["batch_size_multiplier"] == 0.72
    assert adapter["learning_ops_status"] == "healthy"
    assert adapter["pre_hunt_ready"] is False
    assert adapter["learning_failure_count"] == 1
    assert adapter["online_causal_top_lane"] == "edge_expansion"
    assert adapter["variant_dna_top_gene"] == "route:DNA|route|x"
    assert adapter["cycle_learning_delta"] == "top_lane=edge_expansion"
    assert adapter["promotion_rejection_v2_highest_risk"]["risk_score"] == 88.0
    assert adapter["top_counterfactual"]["expected_live_lift"] == 44.0
    assert adapter["missed_winner_count"] == 1
    assert adapter["top_causal_regret"]["decision"] == "focus_route_omission"
    assert adapter["autopilot_policy_packet"]["live_only"] is True
    assert adapter["promotion_first_policy_packet"]["objective"] == "maximize_promotion_survivable_live_pnl"
    assert adapter["false_discovery_warning_count"] == 2
    assert adapter["memory_horizon_summary"]["next_hunt_routes"] == 2
    assert adapter["lesson_half_life_top"]["lesson"] == "refresh this route"
    assert adapter["cross_hunt_replay_top"]["route_key"] == "REPLAY|route|x"
    assert adapter["temporal_regime_top"]["fingerprint"] == "REGIMEFP"
    assert adapter["longitudinal_survival_top"]["feature"] == "route:SURVIVE|route|x"
    assert adapter["memory_conflict_v2_count"] == 2
    assert adapter["strategy_aging_counts"]["fresh"] == 1
    assert adapter["next_hunt_opening_policy"]["opening_mode"] == "promotion_survival_opening"
    assert adapter["question_planner_top"]["question"] == "Does it survive?"
    assert adapter["expected_information_gain_top"]["expected_information_gain"] == 77.0
    assert adapter["uncertainty_heatmap_top"]["uncertainty_score"] == 66.0
    assert adapter["adaptive_experiment_next_step"]["route_key"] == "SEQ|route|x"
    assert adapter["learning_value_stop_count"] == 1
    assert adapter["causal_question_top"]["route_key"] == "LEDGER|route|x"
    assert adapter["epistemic_role_assignments"][0]["epistemic_role"] == "skeptic"
    assert adapter["compiled_hypotheses"][0]["hypothesis_id"] == "h1"
    assert adapter["experiment_contract_top"]["contract_id"] == "c1"
    assert adapter["control_route_top"]["control_route"] == "CONTROL|route|x"
    assert adapter["sequential_test_top"]["decision"] == "scale"
    assert adapter["causal_effect_top"]["effect_size"] == 42.0
    assert adapter["false_positive_pressure"]["mode"] == "cautious_control"
    assert adapter["exploration_paydown_top"]["route_key"] == "PAYDOWN|route|x"
    assert adapter["promotion_power_top"]["route_key"] == "POWER|route|x"
    assert adapter["scientific_executive_summary"]["contract_count"] == 1
    assert adapter["scientific_executive_top_command"]["route_key"] == "SCIEXEC|route|x"
    assert adapter["scientific_commands"][0]["command_id"] == "sc1"
    assert adapter["live_candidate_evidence_top"]["evidence_score"] == 80.0
    assert adapter["promotion_failure_v3_highest_risk"]["predicted_reject_probability"] == 0.7
    assert adapter["evidence_gap_top_task"]["gap"] == "controlled_lift"
    assert adapter["review_ready_v2_top"]["route_key"] == "READY2|route|x"
    assert adapter["promotion_scorecard_top"]["grade"] == "A"
    assert adapter["candidate_lineage_top"]["parent_variant"] == "live"
    assert adapter["live_control_differential_top"]["differential"] == 33.0
    assert adapter["promotion_packet_executive_summary"]["review"] == 1
    assert adapter["promotion_packet_top_decision"]["decision"] == "review"
    assert adapter["unified_learning_summary"]["top_route"] == "WORLD|route|x"
    assert adapter["learning_budget_top"]["bucket"] == "promotion_repair"
    assert adapter["false_discovery_backpressure"]["mode"] == "strict_backpressure"
    assert adapter["meta_learning_governor_summary"]["mode"] == "truth_first_repair"
    assert adapter["elite_learning_summary"]["artifact_count"] == 50
    assert adapter["elite_next_best_question"]["question"] == "What should we repair next?"
    assert adapter["proof_learning_summary"]["artifact_count"] == 15
    assert adapter["truth_ledger_top_claim"]["claim"] == "candidate_has_live_edge_worth_testing"
    assert adapter["research_agenda_top_question"]["question"] == "What proves this edge?"
    assert adapter["review_board_decision"]["board_vote"]["skeptic"] == "test"
    assert adapter["closed_loop_control_summary"]["artifact_count"] == 12
    assert adapter["bayesian_top_belief"]["posterior_probability"] == 0.81
    assert adapter["active_experiment_next"]["selected_experiment"] == "controlled_trait_shuffle"
    assert adapter["learning_status_feed"]["testing_next"] == "controlled_trait_shuffle"
    assert adapter["world_model_nervous_system_summary"]["artifact_count"] == 80
    assert adapter["nervous_system_top_truth"]["truth_score"] == 88.0
    assert adapter["nervous_system_top_promotion"]["promotion_survival_probability"] == 0.74
    assert adapter["nervous_system_world_audit"]["objective"] == "world_model_self_audit_loop"
    assert adapter["orchestration_learning_summary"]["artifact_count"] == 80
    assert adapter["orchestration_top_artifact_governance"]["trust_score"] == 91.0
    assert adapter["orchestration_top_hunt_exec"]["executive_decision"] == "hunt_executive_controller"
    assert adapter["fitness_selection_summary"]["artifact_count"] == 8
    assert adapter["fitness_top_scorecard"]["fitness_score"] == 123.0
    assert adapter["fitness_trust_policy"]["policy"] == "scale"
    assert adapter["fitness_world_report"]["best_layer"] == "closed_loop_control"
