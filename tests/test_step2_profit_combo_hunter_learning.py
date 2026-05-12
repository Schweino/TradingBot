from argparse import Namespace
from datetime import date, timedelta
import json
from pathlib import Path
import shutil
import uuid

import numpy as np

import step2_profit_combo_hunter as hunter
import step2_learning_db


def _row(name, pnl, trades, holdout=0.0, gate_ok=None):
    if gate_ok is None:
        gate_ok = trades >= 100
    return {
        "variant": name,
        "step2_pnl": pnl,
        "step2_trades": trades,
        "step2_wins": max(0, trades - 1),
        "step2_losses": 1 if trades else 0,
        "profit_hunt_rank_score": pnl if gate_ok else -1_000_000_000 + pnl,
        "min_trade_gate": {"ok": gate_ok, "min_trades": 100, "actual_trades": trades},
        "holdout_gate": {"ok": holdout > 0.0, "holdout_pnl": holdout, "holdout_days": ["2026-05-01"]},
        "by_day": {"2026-05-01": {"pnl": pnl, "trades": trades}},
        "by_ticker": {"CLSK": {"pnl": pnl, "trades": trades}},
        "by_side": {"LONG": {"pnl": pnl, "trades": trades, "wins": max(0, trades - 1), "losses": 1 if trades else 0}},
    }


def _route(action="score", ticker="CLSK", setup="vwap_reclaim_breakdown", phase="midday", side="SHORT"):
    return {
        "name": f"{action}_{ticker}_{setup}_{phase}_{side}",
        "action": action,
        "match": {
            "ticker": ticker,
            "setup_type": setup,
            "session_phase": phase,
            "side": side,
        },
        "weights": {},
        "bias": 0.0,
    }


def _compiled_features():
    feature_count = len(hunter.routed.FEATURE_INDEX)
    features = np.zeros((12, feature_count), dtype=float)
    ema_idx = hunter.routed.FEATURE_INDEX.get("ema", 0)
    vwap_idx = hunter.routed.FEATURE_INDEX.get("vwap", 1)
    momentum_idx = hunter.routed.FEATURE_INDEX.get("momentum", 2)
    features[:, ema_idx] = np.linspace(-1.0, 1.0, 12)
    features[:, vwap_idx] = np.array([0.8, 0.7, 0.9, 0.6, -0.2, -0.4, -0.5, -0.7, 0.2, 0.3, -0.1, 0.4])
    features[:, momentum_idx] = np.array([0.9, 0.8, 0.7, 0.6, -0.4, -0.6, -0.8, -0.9, 0.1, 0.2, -0.2, 0.3])
    return {
        "features": features,
        "long_pnl_pct": np.array([0.9, 0.7, 0.8, 0.5, -0.2, -0.4, -0.5, -0.8, 0.1, 0.2, -0.1, 0.3]),
        "short_pnl_pct": np.array([-0.7, -0.6, -0.8, -0.4, 0.4, 0.5, 0.7, 0.9, -0.1, -0.2, 0.2, -0.1]),
        "long_held": np.array([60, 55, 50, 65, 45, 40, 35, 30, 70, 75, 42, 58]),
        "short_held": np.array([45, 40, 35, 30, 60, 65, 70, 75, 38, 36, 55, 44]),
        "long_reason_code": np.array([1, 1, 1, 1, 4, 4, 7, 7, 1, 1, 4, 1]),
        "short_reason_code": np.array([4, 4, 7, 4, 1, 1, 1, 1, 4, 4, 1, 4]),
        "ticker_code": np.array([0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2]),
        "ticker_map": {"CLSK": 0, "RIOT": 1, "MARA": 2},
        "setup_code": np.array([0, 0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 1]),
        "setup_map": {"vwap_reclaim_breakdown": 0, "trend_pullback": 1},
        "day_code": np.array([0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2]),
        "day_map": {"2026-05-01": 0, "2026-05-02": 1, "2026-05-03": 2},
        "original_side": np.array([1, 1, 1, 1, -1, -1, -1, -1, 1, -1, 1, -1]),
    }


def test_failed_run_selects_low_sample_profit_before_negative_diagnostic():
    low_sample = _row("low_sample_profit", 125.0, 80, holdout=40.0, gate_ok=False)
    negative_gate = _row("negative_high_sample", -500.0, 400, holdout=-100.0, gate_ok=True)

    roles = hunter._select_candidate_roles(
        winners=[],
        holdout_winners=[],
        low_sample_positive=[low_sample],
        leaderboard=[negative_gate],
        raw_leaderboard=[low_sample, negative_gate],
    )

    assert roles["selected"]["variant"] == "low_sample_profit"
    assert roles["selected_role"] == "low_sample_positive"
    assert roles["best_diagnostic_failure"]["variant"] == "negative_high_sample"


def test_gate_break_diagnosis_explains_nearest_profitable_gap():
    args = Namespace(min_trades=170)
    near = _row("near_frontier", 210.0, 169, holdout=100.0, gate_ok=False)
    rows = [near, _row("bad_gate", -10.0, 200, holdout=-5.0, gate_ok=True)]
    frontier = {
        "highest_holdout_positive_gate": 165,
        "first_failed_gate_at_or_above_requested": 170,
    }

    report = hunter._gate_break_diagnosis(rows, frontier, args)

    assert report["additional_trades_needed_for_closest"] == 1
    assert report["closest_profitable_below_requested"]["variant"] == "near_frontier"
    assert "requested_trade_gate_has_no_holdout_positive_profit_candidate" in report["blockers"]


def test_gate_break_diagnosis_labels_next_higher_gate_when_requested_gate_passed():
    args = Namespace(min_trades=160)
    near = _row("near_frontier", 263.0, 175, holdout=100.0, gate_ok=True)
    rows = [near, _row("failed_higher_gate", -567.0, 258, holdout=-54.0, gate_ok=True)]
    frontier = {
        "highest_holdout_positive_gate": 175,
        "first_failed_gate_at_or_above_requested": 200,
    }

    report = hunter._gate_break_diagnosis(rows, frontier, args)

    assert report["requested_gate_passed"] is True
    assert report["diagnostic_target_gate"] == 200
    assert "next_higher_trade_gate_has_no_holdout_positive_profit_candidate" in report["blockers"]
    assert "requested_trade_gate_has_no_holdout_positive_profit_candidate" not in report["blockers"]


def test_true_profit_target_report_tracks_30_pct_distance():
    rows = [
        _row("raw", 490.0, 134, holdout=200.0, gate_ok=False),
        _row("gate_holdout", 300.0, 175, holdout=175.0, gate_ok=True),
    ]

    report = hunter._true_profit_target_report(
        rows,
        Namespace(start_balance=100000.0, true_profit_target_pct=30.0),
    )

    assert report["target_pnl"] == 30000.0
    assert report["best_holdout_gate"]["variant"] == "gate_holdout"
    assert report["best_holdout_gate"]["progress_to_target_pct"] == 1.0
    assert report["target_reached"] is False


def test_failure_taxonomy_and_next_command_packet_become_closed_loop_controls():
    args = Namespace(min_trades=160, true_profit_target_pct=30.0)
    fragile = _row("profit_frontier_fragile", 300.0, 175, holdout=-25.0, gate_ok=True)
    fragile["by_day"] = {
        "2026-05-01": {"pnl": 350.0, "trades": 100},
        "2026-05-02": {"pnl": -50.0, "trades": 75},
    }
    low_sample = _row("profit_micro_low", 120.0, 80, holdout=40.0, gate_ok=False)
    loss = _row("profit_mixed_loss", -800.0, 320, holdout=-200.0, gate_ok=True)
    rows = [fragile, low_sample, loss]
    lane_report = hunter._lane_report(rows)
    negative = hunter._profit_combo_negative_knowledge(rows, lane_report)
    true_profit = hunter._true_profit_target_report(rows, Namespace(start_balance=100000.0, true_profit_target_pct=30.0))
    failure = hunter._failure_taxonomy_report(rows, args)
    packet = hunter._next_hunt_command_packet(
        {"highest_holdout_positive_gate": 175, "first_failed_gate_at_or_above_requested": 200},
        lane_report,
        negative,
        true_profit,
        failure,
        args,
    )

    classes = {row["failure_class"] for row in failure["classes"]}
    assert "positive_total_but_holdout_failed" in classes
    assert "positive_but_low_trade_sample" in classes
    assert packet["score_only"] is True
    assert packet["no_compiled_tape_rebuild"] is True
    assert packet["suggested_min_trades"] == 180
    assert "failure_taxonomy_before_next_batch" in packet["required_controls"]


def test_route_priors_sibling_contracts_and_learning_debt_are_actionable():
    winner = _row("profit_frontier_winner", 400.0, 180, holdout=200.0, gate_ok=True)
    winner["routes"] = [_route("score"), _route("force_short", ticker="RIOT")]
    second_winner = _row("profit_frontier_winner_2", 300.0, 170, holdout=120.0, gate_ok=True)
    second_winner["routes"] = [_route("score")]
    loser = _row("profit_mixed_loss", -900.0, 340, holdout=-300.0, gate_ok=True)
    loser["routes"] = [_route("force_long", ticker="MARA", side="LONG")]
    rows = [winner, second_winner, loser]
    args = Namespace(min_trades=160)
    lane_report = hunter._lane_report(rows)
    failure = hunter._failure_taxonomy_report(rows, args)
    priors = hunter._route_prior_report(rows)
    siblings = hunter._controlled_sibling_plan(rows, priors)
    contracts = hunter._experiment_contract_report(rows, failure, priors, args)
    debt = hunter._learning_debt_report(lane_report, priors, failure)

    assert priors["prior_count"] >= 3
    assert any(route.startswith("route:score:CLSK|vwap_reclaim_breakdown|midday|SHORT") for route in priors["focus_routes"])
    assert siblings["parent_count"] >= 1
    assert siblings["tests"][0]["success_criteria"]["holdout_positive"] is True
    assert contracts["contract_count"] >= 1
    assert contracts["contracts"][0]["success_metrics"]
    assert debt["debt_count"] >= 1
    assert any(item["debt_type"] == "unrepaired_failure_class" for item in debt["debts"])


def test_meta_quality_reports_track_dna_family_pressure_and_promotion_gaps():
    args = Namespace(min_trades=160, target_pnl=0.0, true_profit_target_pct=30.0)
    winner = _row("profit_frontier_parent", 420.0, 180, holdout=220.0, gate_ok=True)
    winner["routes"] = [_route("score"), _route("force_short", ticker="RIOT")]
    winner["by_day"] = {
        "2026-05-01": {"pnl": 250.0, "trades": 90},
        "2026-05-02": {"pnl": 170.0, "trades": 90},
    }
    sibling = _row("profit_frontier_sibling", 360.0, 175, holdout=150.0, gate_ok=True)
    sibling["routes"] = [_route("score"), _route("force_short", ticker="RIOT")]
    sibling["by_day"] = {
        "2026-05-01": {"pnl": 180.0, "trades": 85},
        "2026-05-02": {"pnl": 180.0, "trades": 90},
    }
    fragile = _row("profit_micro_fragile", 120.0, 80, holdout=-20.0, gate_ok=False)
    fragile["routes"] = [_route("force_long", ticker="MARA", side="LONG")]
    duplicate_fragile = dict(fragile)
    duplicate_fragile["variant"] = "profit_micro_fragile_duplicate"
    rows = [winner, sibling, fragile, duplicate_fragile]

    dna = hunter._variant_dna_report(rows)
    family = hunter._family_survival_report(rows)
    pressure = hunter._false_discovery_pressure_report(rows, args)
    day_board = hunter._day_robustness_leaderboard(rows)
    true_profit = hunter._true_profit_target_report(rows, Namespace(start_balance=100000.0, true_profit_target_pct=30.0))
    gaps = hunter._promotion_evidence_gap_report(rows, true_profit, args)
    packet = hunter._next_hunt_command_packet(
        {"highest_holdout_positive_gate": 175, "first_failed_gate_at_or_above_requested": 200},
        hunter._lane_report(rows),
        {"avoid_routes": [], "caution_routes": []},
        true_profit,
        hunter._failure_taxonomy_report(rows, args),
        args,
        pressure,
        gaps,
    )

    assert dna["gene_count"] > 0
    assert any(row["gene"] == "lane:frontier" for row in dna["top_positive_genes"])
    assert family["surviving_families"]
    assert pressure["positive_count"] == 4
    assert pressure["raw_positive_without_holdout_count"] == 2
    assert day_board["leaderboard"][0]["variant"] in {"profit_frontier_parent", "profit_frontier_sibling"}
    assert gaps["top_candidates"][0]["gap_count"] >= 1
    assert packet["false_discovery_mode"] == pressure["mode"]
    assert "promotion_evidence_gap_check" in packet["required_controls"]


def test_uncertainty_repair_budget_belief_and_quality_reports_are_machine_readable():
    args = Namespace(min_trades=160, batch_size=500, target_pnl=0.0, true_profit_target_pct=30.0)
    winner = _row("profit_frontier_parent", 420.0, 180, holdout=220.0, gate_ok=True)
    winner["routes"] = [_route("score"), _route("force_short", ticker="RIOT")]
    loser = _row("profit_mixed_loss", -900.0, 340, holdout=-300.0, gate_ok=True)
    loser["routes"] = [_route("force_long", ticker="MARA", side="LONG")]
    low = _row("profit_micro_low", 100.0, 80, holdout=-10.0, gate_ok=False)
    low["routes"] = [_route("score", ticker="CLSK")]
    rows = [winner, loser, low]

    lane = hunter._lane_report(rows)
    failure = hunter._failure_taxonomy_report(rows, args)
    priors = hunter._route_prior_report(rows)
    true_profit = hunter._true_profit_target_report(rows, Namespace(start_balance=100000.0, true_profit_target_pct=30.0))
    siblings = hunter._controlled_sibling_plan(rows, priors)
    debt = hunter._learning_debt_report(lane, priors, failure)
    gaps = hunter._promotion_evidence_gap_report(rows, true_profit, args)
    pressure = hunter._false_discovery_pressure_report(rows, args)
    uncertainty = hunter._uncertainty_heatmap_report(priors, failure, debt, gaps)
    recipes = hunter._repair_recipe_report(failure, gaps, siblings, priors)
    budget = hunter._search_budget_allocator_report(args, priors, pressure, debt, uncertainty)
    dna = hunter._variant_dna_report(rows)
    family = hunter._family_survival_report(rows)
    beliefs = hunter._belief_update_report(priors, dna, family)
    belief_calibration = hunter.step2_belief_calibration.report_from_beliefs(
        beliefs,
        validation_rows=[{"route_key": "CLSK|score|open", "passed": True}],
        source="test",
    )
    quality = hunter._learning_quality_scorecard({
        "score_only_contract": {"score_only": True},
        "quote_aware_guard": {"ok": True},
        "true_profit_target_report": true_profit,
        "failure_taxonomy_report": failure,
        "route_prior_report": priors,
        "experiment_contract_report": hunter._experiment_contract_report(rows, failure, priors, args),
        "repair_recipe_report": recipes,
        "search_budget_allocator_report": budget,
        "next_hunt_command_packet": {"score_only": True},
    })

    assert uncertainty["cell_count"] > 0
    assert uncertainty["cells"][0]["recommended_action"] in {"test_first_next_batch", "allocate_small_probe", "monitor"}
    assert recipes["recipe_count"] > 0
    assert all(sum(row["variant_budget"] for row in budget["allocations"]) == budget["batch_size"] for _ in [0])
    assert beliefs["belief_count"] > 0
    assert belief_calibration["belief_count"] == beliefs["belief_count"]
    assert "learning_adjustments" in belief_calibration
    assert quality["status"] == "healthy"
    assert quality["learning_quality_score"] == 100.0


def test_half_life_negative_falsification_power_questions_and_governor():
    args = Namespace(min_trades=160, batch_size=500, start_balance=100000.0, target_pnl=0.0, true_profit_target_pct=30.0)
    winner = _row("profit_frontier_parent", 420.0, 180, holdout=220.0, gate_ok=True)
    winner["routes"] = [_route("score"), _route("force_short", ticker="RIOT")]
    winner["by_day"] = {
        "2026-05-01": {"pnl": 250.0, "trades": 90},
        "2026-05-02": {"pnl": 170.0, "trades": 90},
    }
    loser = _row("profit_mixed_loss", -1200.0, 520, holdout=-350.0, gate_ok=True)
    loser["routes"] = [_route("force_long", ticker="MARA", side="LONG")]
    rows = [winner, loser]

    lane = hunter._lane_report(rows)
    failure = hunter._failure_taxonomy_report(rows, args)
    negative = hunter._profit_combo_negative_knowledge(rows, lane)
    priors = hunter._route_prior_report(rows)
    family = hunter._family_survival_report(rows)
    half_life = hunter._lesson_half_life_report(rows, priors, family)
    falsify = hunter._negative_falsification_queue(priors, negative, failure)
    true_profit = hunter._true_profit_target_report(rows, args)
    power = hunter._promotion_power_report(rows, true_profit, args)
    debt = hunter._learning_debt_report(lane, priors, failure)
    gaps = hunter._promotion_evidence_gap_report(rows, true_profit, args)
    uncertainty = hunter._uncertainty_heatmap_report(priors, failure, debt, gaps)
    recipes = hunter._repair_recipe_report(failure, gaps, hunter._controlled_sibling_plan(rows, priors), priors)
    pressure = hunter._false_discovery_pressure_report(rows, args)
    questions = hunter._next_best_question_report(uncertainty, recipes, power, pressure)
    quality = hunter._learning_quality_scorecard({
        "score_only_contract": {"score_only": True},
        "quote_aware_guard": {"ok": True},
        "true_profit_target_report": true_profit,
        "failure_taxonomy_report": failure,
        "route_prior_report": priors,
        "experiment_contract_report": hunter._experiment_contract_report(rows, failure, priors, args),
        "repair_recipe_report": recipes,
        "search_budget_allocator_report": hunter._search_budget_allocator_report(args, priors, pressure, debt, uncertainty),
        "next_hunt_command_packet": {"score_only": True},
    })
    governor = hunter._hunt_governor_report(pressure, quality, power, debt, questions)

    assert half_life["lesson_count"] > 0
    assert falsify["test_count"] > 0
    assert power["power_state"] == "far_from_target"
    assert questions["top_question"] is not None
    assert governor["decision"] in {
        "continue_with_replication_backpressure",
        "continue_with_learning_debt_paydown",
        "continue_with_scalability_discovery",
        "continue_balanced_hunt",
    }
    assert "score_only_no_rebuild" in governor["guardrails"]


def test_regime_lineage_counterfactual_coverage_and_trust_reports():
    args = Namespace(min_trades=160, batch_size=500, start_balance=100000.0, target_pnl=0.0, true_profit_target_pct=30.0)
    winner = _row("profit_frontier_parent", 420.0, 180, holdout=220.0, gate_ok=True)
    winner["routes"] = [_route("score"), _route("force_short", ticker="RIOT", phase="open")]
    winner["by_day"] = {
        "2026-05-01": {"pnl": 250.0, "trades": 90},
        "2026-05-02": {"pnl": 170.0, "trades": 90},
    }
    missed = _row("profit_micro_scale_candidate", 160.0, 80, holdout=75.0, gate_ok=False)
    missed["routes"] = [_route("score", ticker="CLSK", phase="late")]
    holdout_repair = _row("profit_scaleup_holdout_repair", 260.0, 180, holdout=-45.0, gate_ok=True)
    holdout_repair["routes"] = [_route("force_long", ticker="MARA", side="LONG", phase="midday")]
    rows = [winner, missed, holdout_repair]

    lane = hunter._lane_report(rows)
    failure = hunter._failure_taxonomy_report(rows, args)
    priors = hunter._route_prior_report(rows)
    true_profit = hunter._true_profit_target_report(rows, args)
    siblings = hunter._controlled_sibling_plan(rows, priors)
    debt = hunter._learning_debt_report(lane, priors, failure)
    gaps = hunter._promotion_evidence_gap_report(rows, true_profit, args)
    uncertainty = hunter._uncertainty_heatmap_report(priors, failure, debt, gaps)
    recipes = hunter._repair_recipe_report(failure, gaps, siblings, priors)
    pressure = hunter._false_discovery_pressure_report(rows, args)
    budget = hunter._search_budget_allocator_report(args, priors, pressure, debt, uncertainty)
    questions = hunter._next_best_question_report(
        uncertainty,
        recipes,
        hunter._promotion_power_report(rows, true_profit, args),
        pressure,
    )
    quality = hunter._learning_quality_scorecard({
        "score_only_contract": {"score_only": True},
        "quote_aware_guard": {"ok": True},
        "true_profit_target_report": true_profit,
        "failure_taxonomy_report": failure,
        "route_prior_report": priors,
        "experiment_contract_report": hunter._experiment_contract_report(rows, failure, priors, args),
        "repair_recipe_report": recipes,
        "search_budget_allocator_report": budget,
        "next_hunt_command_packet": {"score_only": True},
    })
    governor = hunter._hunt_governor_report(
        pressure,
        quality,
        hunter._promotion_power_report(rows, true_profit, args),
        debt,
        questions,
    )
    regime = hunter._regime_fingerprint_report(rows)
    lineage = hunter._candidate_lineage_report(rows)
    missed_report = hunter._missed_winner_report(rows, args)
    coverage = hunter._search_space_coverage_report(rows)
    backlog = hunter._counterfactual_backlog_report(missed_report, priors, recipes, regime)
    pairwise = hunter._route_interaction_report(rows, 2, "pairwise_route_ablation")
    triple = hunter._route_interaction_report(rows, 3, "triple_route_ablation")
    shapley = hunter._shapley_route_contribution_report(rows)
    causal = hunter._causal_ablation_proof_report(pairwise, triple, shapley)
    target_model = hunter._opportunity_target_model_report(_compiled_features())
    feature_learner = hunter._opportunity_feature_learner_report(_compiled_features())
    rule_extractor = hunter._opportunity_rule_extraction_report(_compiled_features(), feature_learner)
    meta_model = hunter._variant_meta_model_report(rows, rule_extractor, priors, args)
    uncertainty_model = hunter._uncertainty_model_report(feature_learner, meta_model, priors)
    teacher_student = hunter._teacher_student_packet(feature_learner, rule_extractor, meta_model, uncertainty_model, {"sequence": []}, args)
    walk_forward = hunter._walk_forward_validation_bundle(
        rows,
        {"step2_pnl": 100.0, "step2_trades": 120},
        {"tests": []},
        hunter._pnl_haircut_sensitivity_report(rows, "slippage_sensitivity_sweep", [0.01]),
        hunter._pnl_haircut_sensitivity_report(rows, "spread_sensitivity_sweep", [0.02]),
    )
    historical_protocol = hunter._historical_learning_protocol(rows, {}, args.min_trades)
    oos_profitability = hunter._oos_profitability_report(walk_forward, historical_protocol, args.min_trades)
    champion = hunter._champion_challenger_report(rows, {"step2_pnl": 100.0, "step2_trades": 120}, args)
    promotion_ladder = hunter._promotion_ladder_report(historical_protocol, oos_profitability, champion)
    benchmark_superiority = hunter._benchmark_superiority_report(rows, {"step2_pnl": 100.0, "step2_trades": 120})
    proof_manifest = hunter._proof_artifact_manifest({}, {}, [], [], {})
    freshness = hunter._proof_freshness_report(proof_manifest)
    regime_match = hunter._proof_regime_match_report(regime, {})
    fill_quality = hunter._fill_quality_proof_report({}, "profit_frontier_parent")
    dossier = {"candidate_variant": "profit_frontier_parent", "ready_for_world_best_certification": False}
    trust = hunter._artifact_trust_report({
        "route_prior_report": priors,
        "failure_taxonomy_report": failure,
        "promotion_evidence_gap_report": gaps,
        "search_budget_allocator_report": budget,
        "hunt_governor_report": governor,
        "regime_fingerprint_report": regime,
        "candidate_lineage_report": lineage,
        "counterfactual_backlog_report": backlog,
        "missed_winner_report": missed_report,
        "search_space_coverage_report": coverage,
        "pairwise_route_ablation_report": pairwise,
        "triple_route_ablation_report": triple,
        "shapley_route_contribution_estimates": shapley,
        "causal_ablation_proof_report": causal,
        "genetic_search_operator": hunter._genetic_search_operator_report(lineage, priors, args),
        "bandit_lane_allocator": hunter._bandit_lane_allocator_report(rows, args),
        "beam_search_route_set_optimizer": hunter._beam_search_route_set_optimizer_report(priors, shapley, args),
        "slippage_sensitivity_sweep": hunter._pnl_haircut_sensitivity_report(rows, "slippage_sensitivity_sweep", [0.01]),
        "spread_sensitivity_sweep": hunter._pnl_haircut_sensitivity_report(rows, "spread_sensitivity_sweep", [0.02]),
        "worker_pool_parallel_batch_runner": hunter._worker_pool_parallel_batch_runner_report(args),
        "opportunity_target_model_report": target_model,
        "opportunity_feature_learner_report": feature_learner,
        "opportunity_rule_extraction_report": rule_extractor,
        "variant_meta_model_report": meta_model,
        "uncertainty_model_report": uncertainty_model,
        "teacher_student_learning_packet": teacher_student,
        "walk_forward_validation_bundle": walk_forward,
        "historical_learning_protocol": historical_protocol,
        "oos_profitability_report": oos_profitability,
        "promotion_ladder_report": promotion_ladder,
        "benchmark_superiority_report": benchmark_superiority,
        "proof_artifact_manifest": proof_manifest,
        "proof_freshness_report": freshness,
        "proof_regime_match_report": regime_match,
        "fill_quality_proof_report": fill_quality,
        "candidate_proof_dossier": dossier,
    })

    assert regime["fingerprint_count"] > 0
    assert any(item["bucket_type"] == "session_phase" and item["bucket"] == "open" for item in regime["fingerprints"])
    assert lineage["lineage"][0]["behavior_key"]
    assert missed_report["missed"][0]["reason"] == "positive_holdout_but_under_trade_gate"
    assert coverage["unique_route_combo_count"] >= 3
    assert "ticker" in coverage["field_coverage"]
    assert backlog["counterfactual_count"] > 0
    assert trust["trust_state"] == "trusted"
    assert trust["trust_score"] == 100.0


def test_experiment_sequence_overlap_efficiency_stress_and_readiness_reports():
    args = Namespace(min_trades=160, batch_size=120, start_balance=100000.0, target_pnl=0.0, true_profit_target_pct=30.0)
    winner = _row("profit_frontier_parent", 480.0, 180, holdout=240.0, gate_ok=True)
    winner["routes"] = [_route("score"), _route("force_short", ticker="RIOT", phase="open")]
    winner["by_day"] = {
        "2026-05-01": {"pnl": 300.0, "trades": 90},
        "2026-05-02": {"pnl": 180.0, "trades": 90},
    }
    winner["by_ticker"] = {
        "CLSK": {"pnl": 280.0, "trades": 90},
        "RIOT": {"pnl": 200.0, "trades": 90},
    }
    challenger = _row("profit_frontier_sibling", 360.0, 170, holdout=120.0, gate_ok=True)
    challenger["routes"] = [_route("score"), _route("force_short", ticker="RIOT", phase="open")]
    missed = _row("profit_micro_scale_candidate", 140.0, 80, holdout=80.0, gate_ok=False)
    missed["routes"] = [_route("score", ticker="CLSK", phase="late")]
    rows = [winner, challenger, missed]

    lane = hunter._lane_report(rows)
    failure = hunter._failure_taxonomy_report(rows, args)
    priors = hunter._route_prior_report(rows)
    true_profit = hunter._true_profit_target_report(rows, args)
    siblings = hunter._controlled_sibling_plan(rows, priors)
    debt = hunter._learning_debt_report(lane, priors, failure)
    gaps = hunter._promotion_evidence_gap_report(rows, true_profit, args)
    uncertainty = hunter._uncertainty_heatmap_report(priors, failure, debt, gaps)
    recipes = hunter._repair_recipe_report(failure, gaps, siblings, priors)
    pressure = hunter._false_discovery_pressure_report(rows, args)
    budget = hunter._search_budget_allocator_report(args, priors, pressure, debt, uncertainty)
    power = hunter._promotion_power_report(rows, true_profit, args)
    questions = hunter._next_best_question_report(uncertainty, recipes, power, pressure)
    quality = hunter._learning_quality_scorecard({
        "score_only_contract": {"score_only": True},
        "quote_aware_guard": {"ok": True},
        "true_profit_target_report": true_profit,
        "failure_taxonomy_report": failure,
        "route_prior_report": priors,
        "experiment_contract_report": hunter._experiment_contract_report(rows, failure, priors, args),
        "repair_recipe_report": recipes,
        "search_budget_allocator_report": budget,
        "next_hunt_command_packet": {"score_only": True},
    })
    governor = hunter._hunt_governor_report(pressure, quality, power, debt, questions)
    regime = hunter._regime_fingerprint_report(rows)
    missed_report = hunter._missed_winner_report(rows, args)
    backlog = hunter._counterfactual_backlog_report(missed_report, priors, recipes, regime)
    sequencer = hunter._experiment_sequencer_report(backlog, questions, budget, governor, args)
    overlap = hunter._route_overlap_matrix(rows)
    efficiency = hunter._sample_efficiency_report(rows, args)
    stress = hunter._stress_test_queue(rows, regime, gaps)
    champion = hunter._champion_challenger_report(rows, {"step2_pnl": 100.0, "step2_trades": 120}, args)
    pairwise = hunter._route_interaction_report(rows, 2, "pairwise_route_ablation")
    triple = hunter._route_interaction_report(rows, 3, "triple_route_ablation")
    shapley = hunter._shapley_route_contribution_report(rows)
    causal = hunter._causal_ablation_proof_report(pairwise, triple, shapley)
    walk_forward = hunter._walk_forward_validation_bundle(
        rows,
        {"step2_pnl": 100.0, "step2_trades": 120},
        stress,
        hunter._pnl_haircut_sensitivity_report(rows, "slippage_sensitivity_sweep", [0.01]),
        hunter._pnl_haircut_sensitivity_report(rows, "spread_sensitivity_sweep", [0.02]),
    )
    historical_protocol = hunter._historical_learning_protocol(rows, {}, args.min_trades)
    oos_profitability = hunter._oos_profitability_report(walk_forward, historical_protocol, args.min_trades)
    promotion_ladder = hunter._promotion_ladder_report(historical_protocol, oos_profitability, champion)
    benchmark_superiority = hunter._benchmark_superiority_report(rows, {"step2_pnl": 100.0, "step2_trades": 120})
    proof_manifest = hunter._proof_artifact_manifest({}, {}, [], [], {})
    freshness = hunter._proof_freshness_report(proof_manifest)
    regime_match = hunter._proof_regime_match_report(regime, {})
    fill_quality = hunter._fill_quality_proof_report({}, "profit_frontier_parent")
    dossier = {"candidate_variant": "profit_frontier_parent", "ready_for_world_best_certification": False}
    trust = hunter._artifact_trust_report({
        "route_prior_report": priors,
        "failure_taxonomy_report": failure,
        "promotion_evidence_gap_report": gaps,
        "search_budget_allocator_report": budget,
        "hunt_governor_report": governor,
        "regime_fingerprint_report": regime,
        "candidate_lineage_report": hunter._candidate_lineage_report(rows),
        "counterfactual_backlog_report": backlog,
        "missed_winner_report": missed_report,
        "search_space_coverage_report": hunter._search_space_coverage_report(rows),
        "pairwise_route_ablation_report": pairwise,
        "triple_route_ablation_report": triple,
        "shapley_route_contribution_estimates": shapley,
        "causal_ablation_proof_report": causal,
        "genetic_search_operator": hunter._genetic_search_operator_report(hunter._candidate_lineage_report(rows), priors, args),
        "bandit_lane_allocator": hunter._bandit_lane_allocator_report(rows, args),
        "beam_search_route_set_optimizer": hunter._beam_search_route_set_optimizer_report(priors, hunter._shapley_route_contribution_report(rows), args),
        "slippage_sensitivity_sweep": hunter._pnl_haircut_sensitivity_report(rows, "slippage_sensitivity_sweep", [0.01]),
        "spread_sensitivity_sweep": hunter._pnl_haircut_sensitivity_report(rows, "spread_sensitivity_sweep", [0.02]),
        "worker_pool_parallel_batch_runner": hunter._worker_pool_parallel_batch_runner_report(args),
        "opportunity_target_model_report": hunter._opportunity_target_model_report(_compiled_features()),
        "opportunity_feature_learner_report": hunter._opportunity_feature_learner_report(_compiled_features()),
        "opportunity_rule_extraction_report": hunter._opportunity_rule_extraction_report(
            _compiled_features(),
            hunter._opportunity_feature_learner_report(_compiled_features()),
        ),
        "variant_meta_model_report": hunter._variant_meta_model_report(
            rows,
            hunter._opportunity_rule_extraction_report(_compiled_features(), hunter._opportunity_feature_learner_report(_compiled_features())),
            priors,
            args,
        ),
        "uncertainty_model_report": hunter._uncertainty_model_report(
            hunter._opportunity_feature_learner_report(_compiled_features()),
            hunter._variant_meta_model_report(rows, {"rules": []}, priors, args),
            priors,
        ),
        "teacher_student_learning_packet": hunter._teacher_student_packet(
            hunter._opportunity_feature_learner_report(_compiled_features()),
            hunter._opportunity_rule_extraction_report(_compiled_features(), hunter._opportunity_feature_learner_report(_compiled_features())),
            hunter._variant_meta_model_report(rows, {"rules": []}, priors, args),
            hunter._uncertainty_model_report(
                hunter._opportunity_feature_learner_report(_compiled_features()),
                hunter._variant_meta_model_report(rows, {"rules": []}, priors, args),
                priors,
            ),
            {"sequence": []},
            args,
        ),
            "walk_forward_validation_bundle": walk_forward,
            "historical_learning_protocol": historical_protocol,
            "oos_profitability_report": oos_profitability,
            "promotion_ladder_report": promotion_ladder,
            "benchmark_superiority_report": benchmark_superiority,
            "proof_artifact_manifest": proof_manifest,
            "proof_freshness_report": freshness,
            "proof_regime_match_report": regime_match,
            "fill_quality_proof_report": fill_quality,
            "candidate_proof_dossier": dossier,
        })
    readiness = hunter._learning_ops_readiness_report(trust, governor, hunter._search_space_coverage_report(rows), stress)

    assert sequencer["sequence_count"] > 0
    assert sum(row["variant_budget"] for row in sequencer["sequence"]) == args.batch_size
    assert overlap["crowded_pair_count"] >= 1
    assert efficiency["top_efficiency"][0]["holdout_pnl_per_trade"] > 0
    assert any(row["stress_type"] == "leave_worst_day_out_and_recheck" for row in stress["tests"])
    assert champion["promotion_challenger_count"] >= 2
    assert readiness["artifact_trust_state"] == "trusted"
    assert "no_stress_tests_generated" not in readiness["blockers"]


def test_remaining_score_only_world_class_items_are_now_reported():
    args = Namespace(min_trades=100, batch_size=90, start_balance=100000.0, target_pnl=0.0, true_profit_target_pct=30.0)
    parent = _row("profit_portfolio_parent", 600.0, 180, holdout=300.0, gate_ok=True)
    parent["routes"] = [
        _route("score", ticker="CLSK", phase="open"),
        _route("force_short", ticker="RIOT", phase="midday"),
        _route("force_long", ticker="MARA", side="LONG", phase="late"),
    ]
    sibling = _row("profit_portfolio_sibling", 330.0, 120, holdout=90.0, gate_ok=True)
    sibling["routes"] = [
        _route("score", ticker="CLSK", phase="open"),
        _route("force_short", ticker="RIOT", phase="midday"),
    ]
    loser = _row("profit_mixed_loss", -240.0, 130, holdout=-80.0, gate_ok=True)
    loser["routes"] = [_route("force_long", ticker="MARA", side="LONG", phase="late")]
    rows = [parent, sibling, loser]

    priors = hunter._route_prior_report(rows)
    lineage = hunter._candidate_lineage_report(rows)
    pairwise = hunter._route_interaction_report(rows, 2, "pairwise_route_ablation")
    triple = hunter._route_interaction_report(rows, 3, "triple_route_ablation")
    shapley = hunter._shapley_route_contribution_report(rows)
    genetic = hunter._genetic_search_operator_report(lineage, priors, args)
    bandit = hunter._bandit_lane_allocator_report(rows, args)
    beam = hunter._beam_search_route_set_optimizer_report(priors, shapley, args)
    slippage = hunter._pnl_haircut_sensitivity_report(rows, "slippage_sensitivity_sweep", [0.01, 0.10])
    spread = hunter._pnl_haircut_sensitivity_report(rows, "spread_sensitivity_sweep", [0.02, 0.20])
    worker = hunter._worker_pool_parallel_batch_runner_report(args)
    controls = hunter._world_class_learning_controls()

    assert pairwise["interaction_count"] >= 1
    assert triple["interaction_count"] >= 1
    assert shapley["route_count"] >= 3
    assert genetic["enabled_for_next_generation"] is True
    assert sum(row["variant_budget"] for row in bandit["allocations"]) == args.batch_size
    assert beam["optimizer_state"] == "ready"
    assert slippage["scenarios"][0]["survivor_count"] >= 1
    assert spread["model_note"].startswith("score_only_pnl_haircut_proxy")
    assert worker["runner_state"] == "contract_ready"
    assert controls["planned_not_blocking_score_only_count"] == 0
    assert "future_out_of_sample_market_days" in controls["external_data_required"]


def test_direct_indicator_model_stack_trains_from_compiled_features():
    args = Namespace(min_trades=100, batch_size=100, start_balance=100000.0, target_pnl=0.0, true_profit_target_pct=30.0)
    compiled = _compiled_features()
    winner = _row("profit_frontier_indicator_parent", 300.0, 120, holdout=140.0, gate_ok=True)
    winner["routes"] = [_route("score", ticker="CLSK", phase="open")]
    loser = _row("profit_mixed_indicator_loss", -120.0, 140, holdout=-60.0, gate_ok=True)
    loser["routes"] = [_route("force_short", ticker="RIOT", phase="late")]
    rows = [winner, loser]
    priors = hunter._route_prior_report(rows)

    targets = hunter._opportunity_target_model_report(compiled)
    feature_learner = hunter._opportunity_feature_learner_report(compiled)
    rules = hunter._opportunity_rule_extraction_report(compiled, feature_learner)
    meta = hunter._variant_meta_model_report(rows, rules, priors, args)
    uncertainty = hunter._uncertainty_model_report(feature_learner, meta, priors)
    teacher = hunter._teacher_student_packet(feature_learner, rules, meta, uncertainty, {"sequence": [{"order": 1}]}, args)
    walk = hunter._walk_forward_validation_bundle(
        rows,
        {"step2_pnl": 100.0, "step2_trades": 50},
        {"tests": [{"test_id": "stress_001"}]},
        hunter._pnl_haircut_sensitivity_report(rows, "slippage_sensitivity_sweep", [0.01]),
        hunter._pnl_haircut_sensitivity_report(rows, "spread_sensitivity_sweep", [0.02]),
    )

    assert targets["targets"]["expected_skip_value_pct"]["mean"] == 0.0
    assert "quote_aware_survival_probability" in targets["targets"]
    assert "downside_tail_risk_pct" in targets["targets"]
    assert feature_learner["model_state"] == "trained"
    assert feature_learner["feature_count"] > 0
    assert rules["rule_count"] > 0
    assert rules["supports_context_feature_rules"] is True
    assert rules["rules"][0]["route_hint"]["feature_filters"]
    assert meta["model_type"] == "variant_meta_ranker"
    assert meta["lane_scores"]
    assert "route_combination_scores" in meta
    assert "operator_budget_worth" in meta
    assert uncertainty["cell_count"] > 0
    assert uncertainty["cells"][0]["confidence_band"] in {"low", "medium", "high"}
    assert teacher["teacher"] == "quote_aware_step2_simulator"
    assert teacher["proposal_count"] > 0
    assert teacher["candidate_variant_count"] == args.batch_size
    assert teacher["candidate_variants"][0]["what_would_falsify_it"]
    assert "student_never_replaces_quote_aware_teacher" in teacher["non_negotiable"]
    assert walk["validation_count"] == len(rows)
    assert walk["validations"][0]["route_keys"]
    assert "slippage_sensitivity_sweep" in walk


def test_walk_forward_route_keys_resolve_route_prior_calibration():
    row = _row("candidate", 300.0, 150, holdout=120.0, gate_ok=True)
    row["routes"] = [_route("score", ticker="CLSK", phase="open")]
    row["by_day"] = {
        "2026-05-01": {"pnl": 180.0, "trades": 75},
        "2026-05-02": {"pnl": 120.0, "trades": 75},
    }
    walk = hunter._walk_forward_validation_bundle(
        [row],
        {"step2_pnl": 100.0, "step2_trades": 100},
        {"tests": []},
        hunter._pnl_haircut_sensitivity_report([row], "slippage_sensitivity_sweep", [0.01]),
        hunter._pnl_haircut_sensitivity_report([row], "spread_sensitivity_sweep", [0.02]),
    )
    validation_rows = hunter._calibration_validation_rows(walk)
    route_key = validation_rows[0]["route_key"]

    calibration = hunter.step2_belief_calibration.report_from_beliefs(
        {"beliefs": [{"belief": f"route_prior:{route_key}", "posterior_confidence": 0.8}]},
        validation_rows=validation_rows,
        source="test",
    )

    assert validation_rows[0]["passed"] is True
    assert calibration["resolved_predictions"] == 1
    assert calibration["pending_predictions"] == 0
    assert calibration["predictions"][0]["resolution_source"] == "validation_results"


def test_calibration_feedback_loader_adds_resolved_prior_predictions():
    tmp_dir = Path("runtime") / f"test_calibration_feedback_{uuid.uuid4().hex}"
    tmp_dir.mkdir(parents=True)
    try:
        path = tmp_dir / "calibration_feedback.json"
        path.write_text(json.dumps({
            "resolved_predictions": [
                {
                    "belief": "route_prior:CLSK|score|open",
                    "belief_type": "route_prior",
                    "subject": "CLSK|score|open",
                    "predicted_probability": 0.8,
                    "status": "resolved",
                    "actual": "validation_passed",
                    "actual_numeric": 1.0,
                }
            ],
            "feedback_rows": [
                {"route_key": "RIOT|force_short|open", "status": "approved"}
            ],
        }), encoding="utf-8")

        feedback = hunter._load_calibration_feedback(path)
        calibration = hunter.step2_belief_calibration.report_from_beliefs(
            {"beliefs": []},
            feedback_rows=feedback["feedback_rows"],
            additional_predictions=feedback["additional_predictions"],
            source="test",
        )

        assert feedback["external_artifact_sha256"]
        assert len(feedback["additional_predictions"]) == 1
        assert calibration["resolved_predictions"] == 1
        assert calibration["pending_predictions"] == 0
        assert calibration["expected_calibration_error"] == 0.2
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_historical_learning_protocol_exposes_split_without_overclaiming_enforcement():
    rows = []
    for idx in range(10):
        day = f"2026-05-{idx + 1:02d}"
        row = _row(f"candidate_{idx}", 100.0 + idx, 120, holdout=40.0, gate_ok=True)
        row["by_day"] = {day: {"pnl": 20.0 + idx, "trades": 25}}
        rows.append(row)

    protocol = hunter._historical_learning_protocol(rows, {}, min_trades=40)

    assert protocol["available_day_count"] == 10
    assert protocol["train_windows"][0]["day_count"] == 6
    assert protocol["validation_windows"][0]["day_count"] == 2
    assert protocol["test_windows"][0]["day_count"] == 2
    assert protocol["frozen_test_set"] is True
    assert protocol["purged_embargoed"] is True
    assert protocol["test_reuse_policy"] == "retire_after_use"
    assert protocol["split_enforced"] is False
    assert protocol["search_touched_test_set"] is True
    assert "search_not_precommitted_to_split" in protocol["blockers"]


def test_external_historical_split_protocol_can_prove_precommitted_search():
    tmp_dir = Path("runtime") / f"test_split_protocol_{uuid.uuid4().hex}"
    tmp_dir.mkdir(parents=True)
    try:
        path = tmp_dir / "split_protocol.json"
        path.write_text(json.dumps({
            "historical_learning_protocol": {
                "train_windows": [{"start": "2026-01-01", "end": "2026-03-31"}],
                "validation_windows": [{"start": "2026-04-01", "end": "2026-04-30"}],
                "test_windows": [{"start": "2026-05-01", "end": "2026-05-08"}],
                "frozen_test_set": True,
                "purged_embargoed": True,
                "purge_embargo_days": 0,
                "test_reuse_policy": "retire_after_use",
                "split_enforced": True,
                "search_touched_test_set": False,
            }
        }), encoding="utf-8")

        external = hunter._load_historical_split_protocol(path)
        protocol = hunter._historical_learning_protocol([], {}, min_trades=100, external_protocol=external)

        assert protocol["external_protocol_loaded"] is True
        assert protocol["external_artifact_sha256"]
        assert protocol["split_enforced"] is True
        assert protocol["search_touched_test_set"] is False
        assert protocol["ready_for_world_best_certification"] is True
        assert protocol["blockers"] == []
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_historical_split_protocol_blocks_overlap_and_embargo_violations():
    external = {
        "train_windows": [{"start": "2026-01-01", "end": "2026-03-31"}],
        "validation_windows": [{"start": "2026-03-20", "end": "2026-04-30"}],
        "test_windows": [{"start": "2026-05-01", "end": "2026-05-08"}],
        "frozen_test_set": True,
        "purged_embargoed": True,
        "purge_embargo_days": 1,
        "test_reuse_policy": "retire_after_use",
        "split_enforced": True,
        "search_touched_test_set": False,
    }

    protocol = hunter._historical_learning_protocol([], {}, min_trades=100, external_protocol=external)

    assert protocol["ready_for_world_best_certification"] is False
    assert "train_to_validation_embargo_violation" in protocol["blockers"]
    assert "validation_to_test_embargo_violation" in protocol["blockers"]


def test_oos_profitability_report_requires_positive_cost_adjusted_passed_split():
    walk = {
        "validations": [
            {"holdout_pnl": 220.0, "trades": 140, "passed": True},
            {"holdout_pnl": 80.0, "trades": 130, "passed": True},
            {"holdout_pnl": -20.0, "trades": 125, "passed": False},
        ],
        "slippage_sensitivity_sweep": {"scenarios": [{"survivor_count": 2}]},
        "spread_sensitivity_sweep": {"scenarios": [{"survivor_count": 2}]},
    }
    unenforced_split = {"split_enforced": False}

    blocked = hunter._oos_profitability_report(walk, unenforced_split, min_trades=100)

    assert blocked["net_oos_pnl"] == 280.0
    assert blocked["positive_window_rate_pct"] == 66.6667
    assert blocked["passed_validation_count"] == 2
    assert blocked["ready_for_world_best_certification"] is False
    assert "historical_split_not_enforced_before_search" in blocked["blockers"]

    enforced_split = {"split_enforced": True}
    ready = hunter._oos_profitability_report(walk, enforced_split, min_trades=100)

    assert ready["ready_for_world_best_certification"] is True
    assert ready["blockers"] == []


def test_oos_profitability_report_can_be_bound_to_selected_candidate():
    walk = {
        "validations": [
            {"variant": "candidate_a", "holdout_pnl": 220.0, "trades": 140, "passed": True},
            {"variant": "candidate_b", "holdout_pnl": -500.0, "trades": 140, "passed": False},
        ],
        "slippage_sensitivity_sweep": {"scenarios": [{"survivor_count": 1}]},
        "spread_sensitivity_sweep": {"scenarios": [{"survivor_count": 1}]},
    }
    split = {"split_enforced": True}

    candidate_a = hunter._oos_profitability_report(walk, split, min_trades=100, candidate_variant="candidate_a")
    candidate_b = hunter._oos_profitability_report(walk, split, min_trades=100, candidate_variant="candidate_b")
    missing = hunter._oos_profitability_report(walk, split, min_trades=100, candidate_variant="candidate_c")

    assert candidate_a["candidate_variant"] == "candidate_a"
    assert candidate_a["validation_count"] == 1
    assert candidate_a["ready_for_world_best_certification"] is True
    assert candidate_b["ready_for_world_best_certification"] is False
    assert "non_positive_net_oos_pnl" in candidate_b["blockers"]
    assert "missing_candidate_walk_forward_validation_rows" in missing["blockers"]


def test_causal_ablation_report_requires_actual_removed_route_rescores():
    rows = [
        _row("candidate_a", 300.0, 160, holdout=120.0, gate_ok=True),
        _row("candidate_b", 220.0, 150, holdout=90.0, gate_ok=True),
    ]
    for row in rows:
        row["routes"] = [_route("score", ticker="CLSK"), _route("force_short", ticker="RIOT", phase="open")]
    pairwise = hunter._route_interaction_report(rows, 2, "pairwise_route_ablation")
    triple = hunter._route_interaction_report(rows, 3, "triple_route_ablation")
    shapley = hunter._shapley_route_contribution_report(rows)

    blocked = hunter._causal_ablation_proof_report(pairwise, triple, shapley)

    assert blocked["observed_candidate_count"] > 0
    assert blocked["isolated_candidate_count"] > 0
    assert blocked["passed_ablation_count"] == 0
    assert blocked["ready_for_world_best_certification"] is False
    assert "missing_removed_route_counterfactual_rescores" in blocked["blockers"]

    actual_tests = [
        {"test_id": f"ab_{idx}", "passed": True, "isolated_lift": idx < 3, "lift_after_costs": 25.0}
        for idx in range(10)
    ]
    actual_tests.extend([
        {"test_id": "failed_1", "passed": False, "isolated_lift": False, "lift_after_costs": -5.0},
        {"test_id": "failed_2", "passed": False, "isolated_lift": False, "lift_after_costs": -8.0},
    ])
    ready = hunter._causal_ablation_proof_report(
        {"actual_ablation_tests": actual_tests},
        {},
        {},
    )

    assert ready["passed_ablation_count"] == 10
    assert ready["failed_ablation_count"] == 2
    assert ready["isolated_lift_count"] == 3
    assert ready["ready_for_world_best_certification"] is True
    assert ready["blockers"] == []


def test_actual_ablation_loader_populates_causal_proof():
    tmp_dir = Path("runtime") / f"test_actual_ablation_{uuid.uuid4().hex}"
    tmp_dir.mkdir(parents=True)
    try:
        path = tmp_dir / "ablations.json"
        path.write_text(json.dumps({
            "actual_ablation_tests": [
                {"test_id": f"ab_{idx}", "passed": True, "isolated_lift": idx < 4, "lift_after_costs": 20.0}
                for idx in range(11)
            ]
        }), encoding="utf-8")

        tests = hunter._load_actual_ablation_tests(path)
        proof = hunter._causal_ablation_proof_report({}, {}, {}, tests)

        assert len(tests) == 11
        assert tests[0]["external_artifact_sha256"]
        assert proof["passed_ablation_count"] == 11
        assert proof["isolated_lift_count"] == 4
        assert proof["ready_for_world_best_certification"] is True
        assert proof["blockers"] == []
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_promotion_ladder_requires_forward_shadow_paper_and_tiny_live():
    historical = {"ready_for_world_best_certification": True}
    oos = {"ready_for_world_best_certification": True, "min_test_trades": 160, "net_oos_pnl": 480.0}
    champion = {
        "promotion_challenger_count": 2,
        "challengers": [{"variant": "candidate_a", "challenge_state": "promotion_challenger"}],
    }

    blocked = hunter._promotion_ladder_report(historical, oos, champion)

    assert blocked["stages"]["historical"]["passed"] is True
    assert blocked["stages"]["historical"]["candidate_bound"] is True
    assert blocked["stages"]["shadow"]["passed"] is False
    assert blocked["ready_for_world_best_certification"] is False
    assert "shadow_stage_not_passed" in blocked["blockers"]
    assert "shadow_stage_not_bound_to_candidate" in blocked["blockers"]

    live = {
        "promotion_stages": {
            "shadow": {"variant": "candidate_a", "passed": True, "trade_count": 40, "net_pnl": 120.0},
            "paper": {"variant": "candidate_a", "passed": True, "trade_count": 40, "net_pnl": 100.0},
            "tiny_live": {"variant": "candidate_a", "passed": True, "trade_count": 30, "net_pnl": 25.0},
        }
    }
    ready = hunter._promotion_ladder_report(historical, oos, champion, live)

    assert ready["candidate_variant"] == "candidate_a"
    assert ready["forward_trade_count"] == 110
    assert ready["forward_net_pnl"] == 245.0
    assert ready["ready_for_world_best_certification"] is True
    assert ready["blockers"] == []


def test_promotion_ladder_rejects_cross_candidate_forward_evidence():
    champion = {
        "promotion_challenger_count": 1,
        "challengers": [{"variant": "candidate_a", "challenge_state": "promotion_challenger"}],
    }
    live = {
        "promotion_stages": {
            "shadow": {"variant": "candidate_b", "passed": True, "trade_count": 100, "net_pnl": 300.0},
            "paper": {"variant": "candidate_b", "passed": True, "trade_count": 100, "net_pnl": 300.0},
            "tiny_live": {"variant": "candidate_b", "passed": True, "trade_count": 100, "net_pnl": 300.0},
        }
    }

    report = hunter._promotion_ladder_report(
        {"ready_for_world_best_certification": True},
        {"ready_for_world_best_certification": True, "min_test_trades": 160, "net_oos_pnl": 480.0},
        champion,
        live,
    )

    assert report["ready_for_world_best_certification"] is False
    assert "shadow_stage_not_bound_to_candidate" in report["blockers"]
    assert "paper_stage_not_bound_to_candidate" in report["blockers"]
    assert "tiny_live_stage_not_bound_to_candidate" in report["blockers"]


def test_promotion_ladder_allows_explicit_family_identity_binding():
    champion = {
        "promotion_challenger_count": 1,
        "challengers": [{
            "variant": "candidate_a_v3",
            "family_key": "family_a",
            "lineage_id": "lineage_a",
            "challenge_state": "promotion_challenger",
        }],
    }
    live = {
        "promotion_stages": {
            "shadow": {"family_key": "family_a", "passed": True, "trade_count": 40, "net_pnl": 100.0},
            "paper": {"lineage_id": "lineage_a", "passed": True, "trade_count": 40, "net_pnl": 100.0},
            "tiny_live": {"candidate_variant": "candidate_a_v3", "passed": True, "trade_count": 30, "net_pnl": 50.0},
        }
    }

    report = hunter._promotion_ladder_report(
        {"ready_for_world_best_certification": True},
        {"ready_for_world_best_certification": True, "min_test_trades": 160, "net_oos_pnl": 480.0},
        champion,
        live,
    )

    assert report["ready_for_world_best_certification"] is True
    assert report["blockers"] == []


def test_benchmark_superiority_requires_random_and_current_live_baselines():
    rows = [_row("candidate", 500.0, 180, holdout=250.0, gate_ok=True)]

    blocked = hunter._benchmark_superiority_report(rows, {"step2_pnl": 100.0, "step2_trades": 120})

    assert blocked["baseline_count"] == 2
    assert blocked["includes_current_live_engine"] is True
    assert blocked["includes_random_entry"] is False
    assert blocked["ready_for_world_best_certification"] is False
    assert "missing_random_entry_baseline" in blocked["blockers"]

    external = [
        {"name": "random_seed_a", "type": "random_entry", "net_pnl": -10.0},
        {"name": "buy_and_hold_proxy", "type": "market_proxy", "net_pnl": 40.0},
        {"name": "simple_momentum", "type": "external_rule", "net_pnl": 80.0},
        {"name": "mean_reversion", "type": "external_rule", "net_pnl": 90.0},
    ]
    ready = hunter._benchmark_superiority_report(rows, {"step2_pnl": 100.0, "step2_trades": 120}, external)

    assert ready["baseline_count"] == 6
    assert ready["baselines_beaten_count"] == 6
    assert ready["net_alpha_after_costs"] == 400.0
    assert ready["ready_for_world_best_certification"] is True
    assert ready["blockers"] == []


def test_benchmark_superiority_can_be_bound_to_selected_candidate():
    rows = [
        _row("candidate_a", 500.0, 180, holdout=250.0, gate_ok=True),
        _row("candidate_b", -50.0, 180, holdout=-25.0, gate_ok=True),
    ]
    external = [
        {"name": "random_seed_a", "type": "random_entry", "net_pnl": -10.0},
        {"name": "buy_and_hold_proxy", "type": "market_proxy", "net_pnl": 40.0},
        {"name": "simple_momentum", "type": "external_rule", "net_pnl": 80.0},
        {"name": "mean_reversion", "type": "external_rule", "net_pnl": 90.0},
    ]

    candidate_a = hunter._benchmark_superiority_report(rows, {"step2_pnl": 100.0}, external, "candidate_a")
    candidate_b = hunter._benchmark_superiority_report(rows, {"step2_pnl": 100.0}, external, "candidate_b")
    missing = hunter._benchmark_superiority_report(rows, {"step2_pnl": 100.0}, external, "candidate_c")

    assert candidate_a["candidate_variant"] == "candidate_a"
    assert candidate_a["ready_for_world_best_certification"] is True
    assert candidate_b["candidate_net_pnl"] == -50.0
    assert candidate_b["ready_for_world_best_certification"] is False
    assert "not_all_baselines_beaten" in candidate_b["blockers"]
    assert "missing_candidate_benchmark_row" in missing["blockers"]


def test_forward_promotion_and_external_benchmark_loaders():
    tmp_dir = Path("runtime") / f"test_external_proofs_{uuid.uuid4().hex}"
    tmp_dir.mkdir(parents=True)
    try:
        promotion_path = tmp_dir / "promotion.json"
        promotion_path.write_text(json.dumps({
            "stages": {
                "shadow": {"passed": True, "trade_count": 45, "net_pnl": 140.0},
                "paper": {"passed": True, "trade_count": 45, "net_pnl": 130.0},
                "tiny_live": {"passed": True, "trade_count": 20, "net_pnl": 30.0},
            }
        }), encoding="utf-8")
        benchmark_path = tmp_dir / "benchmarks.json"
        benchmark_path.write_text(json.dumps({
            "benchmarks": [
                {"name": "random_seed_a", "type": "random_entry", "net_pnl": -10.0},
                {"name": "market_proxy", "type": "market_proxy", "net_pnl": 20.0},
            ]
        }), encoding="utf-8")

        promotion = hunter._load_forward_promotion_feedback(promotion_path)
        benchmarks = hunter._load_external_benchmarks(benchmark_path)

        assert promotion["promotion_stages"]["tiny_live"]["passed"] is True
        assert promotion["external_artifact_sha256"]
        assert len(benchmarks) == 2
        assert benchmarks[0]["external_artifact_path"].endswith("benchmarks.json")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_fill_quality_loader_and_proof_require_execution_survival():
    tmp_dir = Path("runtime") / f"test_fill_quality_{uuid.uuid4().hex}"
    tmp_dir.mkdir(parents=True)
    try:
        path = tmp_dir / "fills.json"
        fills = [
            {
                "candidate_variant": "candidate_a",
                "status": "filled",
                "fill_state": "filled",
                "slippage_bps": 4.0,
                "latency_ms": 35.0,
                "net_pnl": 3.0,
            }
            for _ in range(100)
        ]
        path.write_text(json.dumps({"fills": fills}), encoding="utf-8")

        feedback = hunter._load_fill_quality_feedback(path)
        proof = hunter._fill_quality_proof_report(feedback, "candidate_a")
        missing = hunter._fill_quality_proof_report(feedback, "candidate_b")

        assert feedback["external_artifact_sha256"]
        assert proof["fill_count"] == 100
        assert proof["net_pnl_after_fills"] == 300.0
        assert proof["ready_for_world_best_certification"] is True
        assert missing["ready_for_world_best_certification"] is False
        assert "missing_candidate_fill_rows" in missing["blockers"]
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def test_proof_artifact_manifest_tracks_missing_and_hashed_inputs():
    manifest = hunter._proof_artifact_manifest({}, {}, [], [], {})

    assert manifest["artifact_count"] == 0
    assert manifest["ready_for_world_best_certification"] is False
    assert "historical_split_protocol" in manifest["missing_roles"]

    loaded = hunter._proof_artifact_manifest(
        {"external_artifact_path": "split.json", "external_artifact_sha256": "a"},
        {"external_artifact_path": "promotion.json", "external_artifact_sha256": "b", "promotion_stages": {"shadow": {}}},
        [{"external_artifact_path": "bench.json", "external_artifact_sha256": "c"}],
        [{"external_artifact_path": "ab.json", "external_artifact_sha256": "d"}],
        {"external_artifact_path": "cal.json", "external_artifact_sha256": "e", "additional_predictions": [{}]},
        {"external_artifact_path": "fills.json", "external_artifact_sha256": "f", "fills": [{}, {}]},
    )

    assert loaded["artifact_count"] == 6
    assert loaded["record_count"] == 7
    assert loaded["tamper_evident"] is True
    assert loaded["missing_roles"] == []
    assert loaded["ready_for_world_best_certification"] is True


def test_proof_freshness_report_expires_stale_artifacts():
    today = date(2026, 5, 11)
    fresh_day = (today - timedelta(days=5)).isoformat()
    stale_day = (today - timedelta(days=45)).isoformat()
    manifest = {
        "entries": [
            {"role": "fresh", "loaded": True, "artifact_path": "fresh.json", "artifact_mtime": fresh_day},
            {"role": "stale", "loaded": True, "artifact_path": "stale.json", "artifact_mtime": stale_day},
        ]
    }

    report = hunter._proof_freshness_report(manifest, max_age_days=30, today=today)

    assert report["checked_artifact_count"] == 2
    assert report["fresh_artifact_count"] == 1
    assert report["ready_for_world_best_certification"] is False
    assert "stale" in report["stale_roles"]


def test_proof_regime_match_blocks_mismatched_external_proof():
    regime = {
        "fingerprints": [
            {"bucket_type": "session_phase", "bucket": "open", "regime_state": "expand"},
            {"bucket_type": "ticker", "bucket": "CLSK", "regime_state": "expand"},
        ]
    }
    matched = hunter._proof_regime_match_report(regime, {
        "split": {"regime_keys": ["session_phase:open", "ticker:CLSK"]},
    })
    mismatched = hunter._proof_regime_match_report(regime, {
        "split": {"regime_keys": ["session_phase:late"]},
    })
    untagged = hunter._proof_regime_match_report(regime, {
        "split": {"external_artifact_path": "split.json"},
    })

    assert matched["ready_for_world_best_certification"] is True
    assert matched["matched_proof_count"] == 1
    assert mismatched["ready_for_world_best_certification"] is False
    assert "split_regime_mismatch" in mismatched["blockers"]
    assert "split_missing_regime_keys" in untagged["blockers"]


def test_candidate_proof_dossier_summarizes_ready_and_blocked_sections():
    ready_report = {"ready_for_world_best_certification": True, "blockers": []}
    calibration = {
        "resolved_predictions": 35,
        "pending_predictions": 10,
        "expected_calibration_error": 0.08,
    }
    ready = hunter._candidate_proof_dossier(
        "candidate_a",
        historical_protocol=ready_report,
        oos_report=ready_report,
        causal_report=ready_report,
        promotion_ladder=ready_report,
        benchmark_report=ready_report,
        fill_quality_report=ready_report,
        proof_manifest=ready_report,
        proof_freshness=ready_report,
        proof_regime_match=ready_report,
        calibration_report=calibration,
    )
    blocked = hunter._candidate_proof_dossier(
        "candidate_a",
        historical_protocol={"ready_for_world_best_certification": False, "blockers": ["bad_split"]},
        oos_report=ready_report,
        causal_report=ready_report,
        promotion_ladder=ready_report,
        benchmark_report=ready_report,
        fill_quality_report=ready_report,
        proof_manifest=ready_report,
        proof_freshness=ready_report,
        proof_regime_match=ready_report,
        calibration_report={"resolved_predictions": 2, "pending_predictions": 100, "expected_calibration_error": 0.4},
    )

    assert ready["ready_for_world_best_certification"] is True
    assert ready["proof_score"] == 100.0
    assert blocked["ready_for_world_best_certification"] is False
    assert "historical_split:bad_split" in blocked["blockers"]
    assert "calibration:not_enough_resolved_low_error_predictions" in blocked["blockers"]


def test_score_cache_report_summarizes_hits_and_duplicate_hashes():
    rows = [
        {"score_cache": {"status": "hit", "decision_hash": "same"}},
        {"score_cache": {"status": "miss", "decision_hash": "same"}},
        {"score_cache": {"status": "hit", "decision_hash": "other"}},
    ]

    report = hunter._score_cache_report(rows, Namespace(use_score_cache=True, score_cache_db="cache.sqlite"))

    assert report["cache_hit_count"] == 2
    assert report["cache_miss_count"] == 1
    assert report["cache_hit_rate_pct"] == 66.6667
    assert report["duplicate_decision_hash_count"] == 1
    assert report["top_duplicate_decision_hashes"] == {"same": 2}


def test_validation_matrix_reports_chronological_holdout_and_leave_one_day():
    row = _row("candidate", 10.0, 3)
    row["by_day"] = {
        "2026-05-01": {"pnl": 10.0, "trades": 1},
        "2026-05-02": {"pnl": -5.0, "trades": 1},
        "2026-05-03": {"pnl": 20.0, "trades": 1},
    }

    report = hunter._validation_matrix(row)

    assert report["available"] is True
    assert report["day_count"] == 3
    assert report["chronological_holdout_pnl"] == 20.0
    assert report["leave_one_day_count"] == 3


def test_build_variants_promotes_low_sample_holdout_parent_into_boost_lane():
    compiled = _compiled_features()
    compiled["rows"] = len(compiled["original_side"])
    base = _row("profit_registry_good_low_sample", 400.0, 138, holdout=300.0, gate_ok=False)
    base["routes"] = [{"name": "force_short_CLSK", "action": "force_short", "match": {"ticker": "CLSK"}, "weights": {}, "bias": 0.0}]
    support = _row("profit_support_incremental", 120.0, 80, holdout=60.0, gate_ok=False)
    support["routes"] = [{"name": "force_short_MARA", "action": "force_short", "match": {"ticker": "MARA"}, "weights": {}, "bias": 0.0}]
    temp_dir = Path("runtime") / f"test_profit_combo_builder_{uuid.uuid4().hex}"
    temp_dir.mkdir(parents=True)
    seed_summary = temp_dir / "summary.json"
    try:
        seed_summary.write_text(
            json.dumps({"low_sample_positive": [base, support], "leaderboard": [base, support]}),
            encoding="utf-8",
        )
        active = hunter.routed.routed_variant("active", {}, 0.0, [])
        args = Namespace(
            seed=7,
            batch_size=80,
            min_trades=250,
            min_bucket_opportunities=1,
            seed_summary=[str(seed_summary)],
        )

        variants, stats = hunter._build_variants(compiled, active, args)
        report = stats["variant_builder_report"]

        assert report["near_gate_trade_floor"] <= 138
        assert report["near_gate_candidate_rows"] >= 1
        assert report["frontier_added"] + report["boost_added"] >= 1
        assert any(
            str(getattr(variant, "name", "")).startswith(("profit_frontier_", "profit_boost_"))
            for variant in variants
        )
        assert any("138base" in str(getattr(variant, "name", "")) for variant in variants)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_seed_positive_rows_interleaves_edge_density_candidates():
    dense = _row("dense_edge", 120.0, 10, holdout=40.0, gate_ok=False)
    dense["routes"] = [_route("force_short", ticker="CLSK")]
    large = _row("large_total", 300.0, 300, holdout=80.0, gate_ok=True)
    large["routes"] = [_route("force_short", ticker="MARA")]
    temp_dir = Path("runtime") / f"test_profit_combo_density_{uuid.uuid4().hex}"
    temp_dir.mkdir(parents=True)
    seed_summary = temp_dir / "summary.json"
    try:
        seed_summary.write_text(
            json.dumps({"low_sample_positive": [large, dense], "leaderboard": [large, dense]}),
            encoding="utf-8",
        )

        rows = hunter._seed_positive_rows([str(seed_summary)], limit=2)

        assert {row["variant"] for row in rows} == {"dense_edge", "large_total"}
        assert rows[0]["variant"] in {"dense_edge", "large_total"}
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_execution_viability_gap_report_requires_cost_adjusted_gate_profit():
    args = Namespace(min_trades=250, target_pnl=0.0)
    row = _row("near_gate_low_edge", 2.25, 244, holdout=184.0, gate_ok=False)
    row["execution_adjusted_objective"] = {
        "estimated_execution_costs": {"estimated_total_cost": 2897.5},
        "fill_probability": 0.92,
        "execution_adjusted_pnl": -2663.83,
    }
    row["execution_adjusted_pnl"] = -2663.83

    report = hunter._execution_viability_gap_report([row], args)

    assert report["ready_for_live_learning_promotion"] is False
    assert "no_execution_adjusted_positive_candidate" in report["blockers"]
    assert "best_gross_per_trade_below_estimated_cost_per_trade" in report["blockers"]
    assert report["top_execution_adjusted"][0]["execution_gap_to_positive"] > 0


def test_seed_positive_rows_reuses_execution_positive_candidates_from_gap_report():
    candidate = _row("edge_positive", 199.26, 13, holdout=62.09, gate_ok=False)
    candidate["execution_adjusted_pnl"] = 41.29
    candidate["routes"] = [_route("force_short", ticker="RIOT")]
    temp_dir = Path("runtime") / f"test_profit_combo_execution_seed_{uuid.uuid4().hex}"
    temp_dir.mkdir(parents=True)
    seed_summary = temp_dir / "summary.json"
    try:
        seed_summary.write_text(
            json.dumps({"execution_viability_gap_report": {"execution_positive_candidates": [candidate]}}),
            encoding="utf-8",
        )

        rows = hunter._seed_positive_rows([str(seed_summary)], limit=5)

        assert rows
        assert rows[0]["variant"] == "edge_positive"
        assert rows[0]["execution_adjusted_pnl"] == 41.29
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_edge_density_routes_mine_feature_slices_from_historical_outcomes():
    compiled = _compiled_features()
    compiled["rows"] = len(compiled["original_side"])
    active = hunter.routed.routed_variant("active", {}, 0.0, [])
    positive = [{
        "route_key": "CLSK|*|*|*",
        "preferred_action": "force_long",
        "preferred_avg_pct": 0.5,
        "opportunities": 4,
    }]

    routes = hunter._edge_density_routes(
        compiled,
        positive,
        active,
        Namespace(min_bucket_opportunities=1, min_trades=250),
        limit=5,
    )

    assert routes
    assert all(route.action in {"force_long", "force_short"} for route in routes)
    assert any((route.match or {}).get("feature_filters") for route in routes)


def test_profit_combo_learning_report_emits_closed_loop_sections():
    rows = [
        _row("profit_frontier_0001", 200.0, 120, holdout=80.0, gate_ok=True),
        _row("profit_micro_0001", 50.0, 20, holdout=10.0, gate_ok=False),
        _row("profit_mixed_bad", -100.0, 300, holdout=-50.0, gate_ok=True),
    ]
    report = hunter._profit_combo_learning_report(
        rows,
        rows,
        sorted(rows, key=lambda row: row["step2_pnl"], reverse=True),
        [rows[0]],
        [rows[0]],
        [rows[1]],
        {"weights": {"ema": 1.0}, "bias": 0.0},
        Namespace(seed=7, min_trades=100, target_pnl=0.0),
    )

    assert report["profit_combo_learning_context"]["hunter"] == "profit_combo"
    assert "bandit_allocation" in report
    assert "active_experiment_plan" in report
    assert "negative_knowledge_bank" in report
    assert "autonomous_hunt_planner" in report


def test_audit_payload_flags_duplicate_and_lane_backpressure():
    payload = {
        "score_only_contract": {
            "score_only": True,
            "no_signal_rebuild": True,
            "no_compiled_tape_rebuild": True,
        },
        "exit_replay_model": "quote_aware_exit_v1",
        "quote_aware_guard": {"ok": True},
        "compiled_lineage_validation": {"certified": True, "quick_score_allowed": True},
        "score_cache": {"hit": True},
        "data_coverage_report": {"row_count": 1},
        "true_profit_target_report": {"target_pnl": 1},
        "failure_taxonomy_report": {"classes": []},
        "next_hunt_command_packet": {"score_only": True},
        "route_prior_report": {"priors": []},
        "controlled_sibling_plan": {"siblings": []},
        "experiment_contract_report": {"contracts": []},
        "learning_debt_report": {"debt": []},
        "variant_dna_report": {"variants": []},
        "family_survival_report": {"families": []},
        "false_discovery_pressure_report": {"pressure_state": "ok"},
        "day_robustness_leaderboard": {"leaderboard": []},
        "promotion_evidence_gap_report": {"top_candidates": []},
        "uncertainty_heatmap_report": {"cells": []},
        "repair_recipe_report": {"recipes": []},
        "search_budget_allocator_report": {"allocations": []},
        "belief_update_report": {"updates": []},
        "belief_calibration_report": {"predictions": []},
        "learning_control_plane": {"tranches": [9, 10, 11, 12, 13]},
        "outcome_attribution_report": {"route_attribution": []},
        "counterfactual_learning_report": {"counterfactuals": []},
        "regime_conditioned_learning_report": {"regimes": []},
        "active_experiment_design_report": {"experiments": []},
        "learning_governance_report": {"decision": "allow_probe_only"},
        "learning_depth_report": {"tranches": [14, 15, 16, 17, 18]},
        "enhanced_target_label_report": {"top_labels": []},
        "lesson_survival_decay_report": {"lessons": []},
        "portfolio_learning_report": {"top_portfolio_candidates": []},
        "uncertainty_risk_pricing_report": {"priced_cells": []},
        "promotion_evidence_hardening_report": {"candidates": []},
        "ops_hardening_report": {"tranches": [19, 20, 21, 22, 23, 24]},
        "live_feedback_loop_report": {"feedback_queue": []},
        "drift_monitoring_report": {"monitors": []},
        "rollback_kill_switch_report": {"kill_switches": []},
        "auditability_lineage_report": {"lineage_entries": []},
        "end_to_end_readiness_gate": {"readiness_state": "ready"},
        "elite_runbook_report": {"steps": []},
        "learning_quality_scorecard": {"status": "healthy"},
        "lesson_half_life_report": {"lessons": []},
        "negative_falsification_queue": {"queue": []},
        "promotion_power_report": {"state": "building"},
        "next_best_question_report": {"questions": []},
        "hunt_governor_report": {"decision": "continue"},
        "regime_fingerprint_report": {"fingerprints": []},
        "candidate_lineage_report": {"lineage": []},
        "missed_winner_report": {"missed": []},
        "search_space_coverage_report": {
            "lane_coverage": [{"lane": "mixed", "coverage_state": "over_concentrated", "coverage_pct": 55.0}]
        },
        "duplicate_behavior_report": {
            "low_sample_positive": {
                "variant_count": 100,
                "duplicate_behavior_count": 40,
            }
        },
        "counterfactual_backlog_report": {"backlog": []},
        "artifact_trust_report": {"trust_state": "trusted"},
        "experiment_sequencer_report": {"sequence": []},
        "route_overlap_matrix": {"rows": []},
        "sample_efficiency_report": {"rows": []},
        "stress_test_queue": {"queue": []},
        "champion_challenger_report": {"champion": {}},
        "learning_ops_readiness_report": {"status": "ready"},
        "pairwise_route_ablation_report": {"interactions": []},
        "triple_route_ablation_report": {"interactions": []},
        "shapley_route_contribution_estimates": {"contributions": []},
        "genetic_search_operator": {"operators": []},
        "bandit_lane_allocator": {"allocation": []},
        "beam_search_route_set_optimizer": {"beams": []},
        "slippage_sensitivity_sweep": {"scenarios": []},
        "spread_sensitivity_sweep": {"scenarios": []},
        "statistical_validation_report": {"leaderboard": []},
        "closed_loop_causal_controller": {"route_controller": {"allocation": []}},
        "deployment_risk_report": {"top": []},
        "world_class_readiness_report": {"ok": True, "readiness_state": "ready"},
        "worker_pool_parallel_batch_runner": {"recommended_workers": 1},
        "opportunity_feature_learner_report": {"trained": True},
        "opportunity_target_model_report": {"trained": True},
        "opportunity_rule_extraction_report": {"rules": []},
        "variant_meta_model_report": {"trained": True},
        "uncertainty_model_report": {"cells": []},
        "teacher_student_learning_packet": {"candidates": []},
        "walk_forward_validation_bundle": {"validations": []},
        "world_class_learning_controls": {"controls": []},
        "profit_combo_learning_report": {"status": "ok"},
        "artifact_paths": {"summary": "summary.json"},
        "requested_variants": 500,
        "scored_total": 500,
        "winners": [],
        "leaderboard": [],
        "winner_count": 0,
        "low_sample_positive_count": 0,
    }

    issues = hunter._audit_payload(payload)

    assert any(issue["issue"] == "search_lane_over_concentrated" for issue in issues)
    assert any(issue["issue"] == "duplicate_behavior_pressure" for issue in issues)


def test_learning_db_ingests_profit_combo_summary_json():
    tmp_path = Path("runtime") / f"test_profit_combo_learning_db_{uuid.uuid4().hex}"
    if tmp_path.exists():
        shutil.rmtree(tmp_path)
    tmp_path.mkdir(parents=True)
    run_dir = tmp_path / "quote_aware_profit_combo"
    run_dir.mkdir()
    row = _row("profit_frontier_0001", 200.0, 120, holdout=80.0, gate_ok=True)
    (run_dir / "summary.json").write_text(
        __import__("json").dumps({
            "source": "step2_profit_combo_hunter",
            "leaderboard": [row],
            "cycles": [{"cycle": 0, "hunter": "profit_combo", "ok": True, "scored_total": 1, "winners": 1}],
            "bandit_allocation": {"allocation": [{"route_key": "CLSK|*|*|*", "recommended_budget_pct": 50, "reward_score": 10}]},
        }),
        encoding="utf-8",
    )
    db_path = tmp_path / "learning.sqlite"
    try:
        db = step2_learning_db.Step2LearningDB(db_path)
        try:
            stats = db.ingest_run_dir(run_dir)
            packet = db.next_experiment_packet()
        finally:
            db.close()

        assert stats["runs"] == 1
        assert stats["candidates"] == 1
        assert "hunt_next" in packet
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def _touch_market_file(root: Path, relative: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("[]", encoding="utf-8")


def test_data_coverage_report_flags_per_second_when_full_quote_ticks_exist():
    root = Path("runtime") / f"test_market_data_fidelity_{uuid.uuid4().hex}"
    try:
        days = ["2026-05-07", "2026-05-08"]
        tickers = ["CLSK", "MARA"]
        for day in days:
            for mode in ("per-second", "all"):
                _touch_market_file(root, f"data_cache/alpaca_engine_replay_tapes/sip_{mode}_bars_CLSK-MARA_{day}.events.json.gz")
                folder = "quotes_all" if mode == "all" else "quotes_per-second"
                for ticker in tickers:
                    _touch_market_file(root, f"data_cache/alpaca_engine_replay/sip/{folder}/{ticker}_{day}.json.gz")
            for ticker in tickers:
                _touch_market_file(root, f"data_cache/alpaca_engine_replay/sip/trades/{ticker}_{day}.json.gz")
        _touch_market_file(root, "postmortem/backtests/decision_tapes/decision_tape_sip_all_bars_live_CLSK-MARA_2026-05-07.jsonl.gz")

        old_here = hunter.HERE
        hunter.HERE = root
        try:
            report = hunter._data_coverage_report(
                {"rows": 10, "day_map": {day: idx for idx, day in enumerate(days)}, "ticker_map": {"CLSK": 0, "MARA": 1}},
                {"source_paths": ["decision_tape_sip_per-second_bars_live_CLSK-MARA_2026-05-07.jsonl.gz"]},
            )
        finally:
            hunter.HERE = old_here

        fidelity = report["market_data_fidelity"]
        assert fidelity["compiled_quote_mode"] == "per-second"
        assert fidelity["full_quote_tick_available_for_scope"] is True
        progress = fidelity["all_quote_decision_tape_progress"]
        assert progress["completed_days"] == ["2026-05-07"]
        assert progress["missing_days"] == ["2026-05-08"]
        assert progress["resume_start_day"] == "2026-05-08"
        assert "compiled_not_using_highest_available_quote_fidelity" in report["promotion_data_fidelity_blockers"]
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_data_coverage_report_accepts_full_quote_compiled_scope():
    root = Path("runtime") / f"test_market_data_fidelity_{uuid.uuid4().hex}"
    try:
        days = ["2026-05-07"]
        tickers = ["CLSK"]
        for day in days:
            _touch_market_file(root, f"data_cache/alpaca_engine_replay_tapes/sip_all_bars_CLSK_{day}.events.json.gz")
            _touch_market_file(root, f"data_cache/alpaca_engine_replay/sip/quotes_all/CLSK_{day}.json.gz")
            _touch_market_file(root, f"data_cache/alpaca_engine_replay/sip/trades/CLSK_{day}.json.gz")

        old_here = hunter.HERE
        hunter.HERE = root
        try:
            report = hunter._data_coverage_report(
                {"rows": 3, "day_map": {"2026-05-07": 0}, "ticker_map": {"CLSK": 0}},
                {"source_paths": ["decision_tape_sip_all_bars_live_CLSK_2026-05-07.jsonl.gz"]},
            )
        finally:
            hunter.HERE = old_here

        fidelity = report["market_data_fidelity"]
        assert fidelity["compiled_quote_mode"] == "all"
        assert fidelity["ready_for_world_best_certification"] is True
        assert report["promotion_data_fidelity_blockers"] == []
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_next_hunt_command_packet_requires_all_quote_compare_when_available():
    args = Namespace(min_trades=160, true_profit_target_pct=30.0)
    packet = hunter._next_hunt_command_packet(
        {"highest_holdout_positive_gate": 150},
        {},
        {},
        {"best_holdout_gate": {"progress_to_target_pct": 0.5}},
        {"classes": []},
        args,
        data_coverage_report={
            "promotion_data_fidelity_blockers": ["compiled_not_using_highest_available_quote_fidelity"],
            "market_data_fidelity": {
                "compiled_quote_mode": "per-second",
                "highest_available_quote_mode": "all",
                "full_quote_tick_available_for_scope": True,
                "all_quote_decision_tape_progress": {
                    "resume_command": ["python", "prepare_step2_live_cache.py", "--start", "2026-05-08"],
                    "resume_start_day": "2026-05-08",
                },
            },
        },
    )

    action = packet["data_fidelity_action_packet"]
    assert action["pre_hunt_rebuild_required"] is True
    assert action["required_action"] == "build_and_compare_quote_mode_all_compiled_tape"
    assert action["resume_command"][:3] == ["python", "prepare_step2_live_cache.py", "--start"]
    assert "market_data_fidelity_check" in packet["required_controls"]
