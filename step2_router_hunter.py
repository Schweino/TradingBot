"""Route-aware Step 2 hunter.

This hunter searches a different surface than the linear adaptive hunter:
fallback scoring stays equal to the active live profile, while ordered route
overrides are added for specific ticker/setup/session buckets identified by the
oracle gap report.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import time
from collections import Counter
from pathlib import Path
from typing import Any

import active_engine_baseline
import baseline_drift_sentinel
import candidate_profile_schema
import decision_tape_compiled
import market_data_integrity_gate
import routed_scoring_profile as routed
import routed_profile_safety
import scoring_variant_lab as lab
import scoring_variant_lab_fast as fast
import step2_evaluation_envelope
import step2_manifest_resolver
import step2_oracle_gap_report
import step2_parity_contract
import step2_quote_aware_guard
import step2_score_cache
import unified_decision_ledger


HERE = Path(__file__).resolve().parent
DEFAULT_OUT = HERE / 'postmortem' / 'backtests' / 'step2_router_hunter'
ELITE_LEARNING_KEYS = (
    'cross_run_candidate_resurrection_scoring', 'live_edge_decay_curves_by_route',
    'promotion_failure_causal_backtrace', 'route_crowding_detector', 'worker_disagreement_logging',
    'automatic_win_explainer', 'automatic_loss_explainer', 'variant_mutation_ancestry_scoring',
    'parent_child_edge_retention_model', 'live_review_mismatch_detector', 'candidate_fragility_heatmap',
    'robustness_repair_prioritizer', 'indicator_family_saturation_meter', 'ticker_concentration_budgeter',
    'time_of_day_route_memory', 'market_regime_conditioned_mutation_widths',
    'promotion_readiness_lift_estimator', 'almost_promoted_near_miss_league',
    'false_confidence_detector', 'lesson_contradiction_detector_v3', 'route_hypothesis_decay_scheduler',
    'dead_route_resurrection_calendar', 'novelty_usefulness_scorer', 'search_entropy_monitor',
    'exploit_overfit_warning_system', 'discovery_overload_warning_system',
    'live_beater_quality_percentile_model', 'candidate_control_similarity_scorer',
    'promotion_packet_completeness_gate', 'adaptive_holdout_demand_model', 'evidence_debt_ledger_v2',
    'candidate_repair_outcome_memory', 'wild_shuffle_safety_governor', 'strategy_drift_fingerprinting',
    'route_family_tournament_brackets', 'worker_role_switching_policy', 'hunt_cycle_objective_selector',
    'stop_wasting_cycles_detector', 'route_interaction_veto_rules', 'meta_learning_ablation_scheduler',
    'historical_lesson_replay_simulator', 'synthetic_challenger_generator', 'candidate_ensemble_builder',
    'live_leaderboard_stability_tracker', 'shock_event_memory_tagging', 'promotion_review_rehearsal_mode',
    'human_readable_hunt_diary_compiler', 'top100_memory_compression_by_theme',
    'self_auditing_learning_scorecard', 'world_state_next_best_question_engine',
)
PROOF_LEARNING_KEYS = (
    'learning_truth_ledger', 'causal_credit_assignment_engine', 'live_hunt_simulator',
    'experiment_promotion_ladder', 'persistent_cross_hunt_memory_warehouse',
    'learning_roi_accounting', 'adaptive_research_agenda', 'self_falsification_mode',
    'counterfactual_budget_replay', 'candidate_lifecycle_state_machine',
    'regime_aware_lesson_validity', 'world_model_dashboard',
    'autonomous_hunt_review_board', 'memory_garbage_collector',
    'proof_carrying_promotion_packets',
)
CLOSED_LOOP_CONTROL_KEYS = (
    'bayesian_belief_engine', 'active_experiment_selector', 'uncertainty_aware_top100',
    'live_search_drift_detector', 'exploration_exploitation_governor',
    'variant_lineage_genetics', 'feature_interaction_miner_v2', 'regret_accounting_engine',
    'adversarial_robustness_generator', 'hunt_curriculum_engine',
    'meta_strategy_bandits', 'real_time_learning_dashboard_feed',
)
WORLD_MODEL_NERVOUS_KEYS = (
    'hypothesis_dependency_graph', 'belief_contradiction_resolver',
    'evidence_freshness_scorer', 'route_level_confidence_intervals',
    'candidate_luck_adjusted_ranking', 'live_beater_persistence_tracker',
    'statistical_power_gate', 'minimum_evidence_calculator',
    'adaptive_holdout_allocator', 'false_positive_tax_model',
    'trait_inheritance_scoreboard', 'parent_child_lift_attribution',
    'mutation_distance_optimizer', 'gene_recombination_planner',
    'dead_gene_suppression_engine', 'rare_gene_exploration_budget',
    'family_overcrowding_detector', 'lineage_diversity_governor',
    'mutation_genealogy_replay', 'candidate_clone_detector_v2',
    'worker_strategy_specialization', 'worker_disagreement_arbitration',
    'worker_boredom_staleness_detector', 'worker_exploration_quota',
    'worker_exploit_quota', 'worker_role_tournament', 'worker_error_memory',
    'worker_route_fit_model', 'worker_risk_appetite_scheduler',
    'worker_promotion_readiness_focus_mode', 'multi_armed_route_portfolio',
    'route_retirement_court', 'route_resurrection_market',
    'route_volatility_memory', 'route_time_decay_model',
    'route_overfit_signature_tracker', 'route_stress_test_ladder',
    'route_novelty_saturation_monitor', 'route_interaction_veto_engine',
    'route_promotion_survival_simulator', 'indicator_family_causal_map',
    'indicator_pair_lift_miner', 'indicator_redundancy_detector',
    'indicator_regime_fit_scorer', 'indicator_shuffle_generator',
    'indicator_fragility_profiler', 'indicator_decay_monitor',
    'indicator_conflict_graph', 'indicator_substitution_recommender',
    'indicator_ensemble_builder', 'regime_fingerprint_memory',
    'regime_specific_top100_ranking', 'regime_drift_early_warning',
    'regime_replay_harness', 'regime_transferability_scorer',
    'regime_mismatch_firewall', 'regime_aware_mutation_width',
    'regime_specific_promotion_threshold', 'regime_lesson_half_life',
    'regime_opening_playbook', 'promotion_rejection_reason_backprop',
    'promotion_packet_completeness_optimizer', 'promotion_shadow_review_scheduler',
    'promotion_failure_simulator', 'promotion_evidence_gap_router_v2',
    'promotion_ready_queue_optimizer', 'promotion_confidence_decomposer',
    'promotion_risk_heatmap', 'promotion_reviewer_ensemble',
    'promotion_survival_memory_bank', 'hunt_objective_optimizer',
    'search_entropy_governor', 'breakthrough_detector_v2',
    'exploration_debt_accounting', 'opportunity_cost_simulator',
    'scientific_ablation_runner', 'counterfactual_hunt_replay_v2',
    'live_learning_executive_summary', 'autonomous_research_agenda_v2',
    'world_model_self_audit_loop',
)
ORCHESTRATION_LEARNING_KEYS = (
    'unified_artifact_dependency_resolver', 'artifact_conflict_priority_court',
    'artifact_trust_score_optimizer', 'artifact_duplication_detector',
    'artifact_cost_benefit_ledger', 'artifact_aging_and_retirement_policy',
    'artifact_schema_migration_validator', 'artifact_output_contract_tester',
    'artifact_runtime_contribution_tracker', 'artifact_governance_dashboard',
    'belief_graph_versioning', 'belief_provenance_tracer',
    'belief_confidence_calibration_curve', 'belief_contradiction_heatmap',
    'belief_overconfidence_penalty', 'belief_uncertainty_debt_queue',
    'belief_expiry_scheduler', 'belief_replay_validator',
    'belief_to_action_explainability_map', 'belief_promotion_impact_tracker',
    'experiment_queue_optimizer', 'experiment_interference_detector',
    'experiment_dependency_scheduler', 'experiment_expected_value_frontier',
    'experiment_sequential_stopping_v2', 'experiment_shadow_control_allocator',
    'experiment_sample_size_auto_planner', 'experiment_result_reproducibility_scorer',
    'experiment_contamination_replay', 'experiment_promotion_readiness_bridge',
    'worker_learning_rate_model', 'worker_fatigue_novelty_balancer',
    'worker_specialization_drift_detector', 'worker_task_fit_recommender_v2',
    'worker_result_quality_auditor', 'worker_debate_allocator',
    'worker_adversarial_assignment_rotator', 'worker_throughput_quality_frontier',
    'worker_promotion_packet_closer', 'worker_autonomy_guardrail',
    'search_space_topology_mapper', 'search_basin_depth_estimator',
    'search_local_optimum_escape_detector', 'search_novelty_exhaustion_forecaster',
    'search_mutation_radius_controller_v2', 'search_grammar_coverage_auditor',
    'search_route_pair_exploration_planner', 'search_wild_shuffle_governor_v2',
    'search_exploitation_saturation_gauge', 'search_unexplored_cell_bounty_market',
    'promotion_proof_burden_allocator', 'promotion_failure_taxonomy_learner',
    'promotion_reviewer_disagreement_resolver', 'promotion_causal_evidence_builder',
    'promotion_minimum_viable_proof_gate', 'promotion_anti_overfit_checklist_generator',
    'promotion_robustness_replay_scheduler', 'promotion_economic_value_decomposer',
    'promotion_review_dry_run_tournament', 'promotion_live_readiness_governor',
    'memory_entropy_monitor', 'memory_contradiction_replay',
    'memory_lesson_clustering_engine', 'memory_stale_prior_quarantiner',
    'memory_regime_transfer_validator', 'memory_negative_result_compressor',
    'memory_high_value_lesson_pinboard', 'memory_retrieval_quality_benchmark',
    'memory_catastrophic_forgetting_guard', 'memory_cross_run_research_agenda_compiler',
    'hunt_executive_controller', 'hunt_cycle_objective_negotiator',
    'hunt_interrupt_reason_classifier', 'hunt_pivot_quality_scorer',
    'hunt_budget_auctioneer', 'hunt_post_action_truth_updater',
    'hunt_online_ablation_switchboard', 'hunt_scientific_method_enforcer',
    'hunt_live_status_narrative_critic', 'hunt_self_improvement_backlog_generator',
)
FITNESS_SELECTION_KEYS = (
    'learning_layer_ablation_harness', 'artifact_fitness_scorecard',
    'layer_conflict_matrix', 'artifact_duplicate_clusterer',
    'artifact_runtime_roi_ranker', 'learning_layer_trust_policy',
    'artifact_demote_retire_queue', 'world_model_fitness_report',
)
FEATURES = list(fast.FEATURE_NAMES)
CORE_ROUTE_FEATURES = [
    'ema',
    'vwap',
    'momentum',
    'btc',
    'relative',
    'miner',
    'burst',
    'flow_contra',
    'btc_chop',
    'exec_penalty',
    'open_phase',
    'midday_phase',
    'brs_open_low_range_040',
    'brs_open_low_range_045',
    'setup_btc_relative_strength',
]


def _load_trading_config() -> dict:
    path = HERE / 'trading_config.json'
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return {}


def _sim_config(args: argparse.Namespace) -> dict:
    sim = step2_parity_contract.sim_config(_load_trading_config())
    sim['max_trades_per_day'] = int(args.max_trades_per_day)
    sim['max_trades_per_ticker_day'] = int(args.max_trades_per_ticker_day)
    return sim


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(payload, sort_keys=True, separators=(',', ':'), default=str), encoding='utf-8')
    os.replace(tmp, path)


def _read_json(path: str | Path) -> dict:
    try:
        return json.loads(Path(path).read_text(encoding='utf-8-sig'))
    except Exception:
        return {}


def _compiled_days(compiled: dict) -> list[str]:
    return sorted(str(day) for day in (compiled.get('day_map') or {}).keys())


def _data_integrity_preflight(compiled: dict) -> dict:
    days = _compiled_days(compiled)
    if not days:
        return {
            'skipped': True,
            'reason': 'no_compiled_days',
            'days': days,
            'ok': True,
            'promotion_safe': True,
        }
    payload = market_data_integrity_gate.build_many(
        days,
        source='canonical',
        write=True,
        use_cached=True,
    )
    payload['skipped'] = False
    return payload


def _attach_evaluation_guardrails(payload: dict, envelope: dict) -> dict:
    step2_evaluation_envelope.attach(payload, envelope)
    baseline_drift_sentinel.annotate_payload(payload)
    return payload


def _append_jsonl(path: str | Path, payload: dict) -> None:
    if not path:
        return
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a', encoding='utf-8') as f:
        f.write(json.dumps(payload, sort_keys=True, separators=(',', ':'), default=str) + '\n')


def _append_scored_manifest(path: str | Path, rows: list[dict], *, batch_idx: int) -> None:
    """Persist every scored variant so exact-count smoke runs remain auditable."""
    if not path:
        return
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a', encoding='utf-8') as f:
        for row in rows:
            payload = dict(row)
            payload['manifest_batch'] = int(batch_idx)
            payload['manifest_source'] = 'step2_router_hunter'
            f.write(json.dumps(payload, sort_keys=True, separators=(',', ':'), default=str) + '\n')


def _route_key_from_seed(seed: dict) -> str:
    match = seed.get('match') if isinstance(seed.get('match'), dict) else {}
    if not match:
        return ''
    return '|'.join([
        str(match.get('ticker') or '*'),
        str(match.get('setup_type') or '*'),
        str(match.get('session_phase') or '*'),
    ])


def _route_pattern_matches(pattern: str, route: str) -> bool:
    if not pattern or not route:
        return False
    if pattern == route:
        return True
    pattern_parts = str(pattern).split('|')
    route_parts = str(route).split('|')
    if len(pattern_parts) != len(route_parts):
        return False
    return all(p == '*' or p == r for p, r in zip(pattern_parts, route_parts))


def _route_in_patterns(route: str, patterns) -> bool:
    return any(_route_pattern_matches(str(pattern), route) for pattern in (patterns or []))


def _route_value(mapping: dict, route: str, default=None):
    if not isinstance(mapping, dict) or not route:
        return default
    if route in mapping:
        return mapping[route]
    best_key = ''
    best_value = default
    best_specificity = -1
    for key, value in mapping.items():
        key = str(key)
        if not _route_pattern_matches(key, route):
            continue
        specificity = sum(1 for part in key.split('|') if part and part != '*')
        if specificity > best_specificity:
            best_key = key
            best_value = value
            best_specificity = specificity
    return best_value if best_key else default


def _route_command(commands: dict, route: str) -> dict:
    command = _route_value(commands, route, {})
    return command if isinstance(command, dict) else {}


def _online_controls(args: argparse.Namespace) -> dict:
    return _read_json(args.online_state_json) if getattr(args, 'online_state_json', '') else {}


def _quarantine(args: argparse.Namespace) -> dict:
    return _read_json(args.quarantine_json) if getattr(args, 'quarantine_json', '') else {}


def _runtime_adapter_from_quarantine(quarantine: dict) -> dict:
    adapter = quarantine.get('runtime_command_adapter') if isinstance(quarantine.get('runtime_command_adapter'), dict) else {}
    if adapter:
        return adapter
    packet = quarantine.get('degraded_runtime_command_packet') if isinstance(quarantine.get('degraded_runtime_command_packet'), dict) else {}
    if not packet:
        guardrails = quarantine.get('runtime_guardrails') if isinstance(quarantine.get('runtime_guardrails'), dict) else {}
        kernel = quarantine.get('runtime_decision_kernel') if isinstance(quarantine.get('runtime_decision_kernel'), dict) else {}
        packet = guardrails.get('guarded_command_packet') if isinstance(guardrails.get('guarded_command_packet'), dict) else {}
        if not packet:
            packet = kernel.get('command_packet') if isinstance(kernel.get('command_packet'), dict) else {}
    ab_executor = quarantine.get('ab_route_experiment_executor') if isinstance(quarantine.get('ab_route_experiment_executor'), dict) else {}
    contamination = quarantine.get('experiment_contamination_guard') if isinstance(quarantine.get('experiment_contamination_guard'), dict) else {}
    cold_start = quarantine.get('cold_start_route_pack_generator') if isinstance(quarantine.get('cold_start_route_pack_generator'), dict) else {}
    opening = quarantine.get('hunt_opening_playbook') if isinstance(quarantine.get('hunt_opening_playbook'), dict) else {}
    arbiter = quarantine.get('memory_conflict_arbiter') if isinstance(quarantine.get('memory_conflict_arbiter'), dict) else {}
    falsification = quarantine.get('memory_falsification_queue') if isinstance(quarantine.get('memory_falsification_queue'), dict) else {}
    stress_pack = quarantine.get('memory_stress_test_pack') if isinstance(quarantine.get('memory_stress_test_pack'), dict) else {}
    retirement = quarantine.get('belief_retirement_engine') if isinstance(quarantine.get('belief_retirement_engine'), dict) else {}
    disagreement = quarantine.get('current_vs_historical_disagreement_monitor') if isinstance(quarantine.get('current_vs_historical_disagreement_monitor'), dict) else {}
    hypothesis_market = quarantine.get('hypothesis_market_maker') if isinstance(quarantine.get('hypothesis_market_maker'), dict) else {}
    bet_sizer = quarantine.get('real_time_bet_sizer') if isinstance(quarantine.get('real_time_bet_sizer'), dict) else {}
    contrarian = quarantine.get('contrarian_generator') if isinstance(quarantine.get('contrarian_generator'), dict) else {}
    stop_loss = quarantine.get('learning_stop_loss') if isinstance(quarantine.get('learning_stop_loss'), dict) else {}
    breakthroughs = quarantine.get('breakthrough_detector') if isinstance(quarantine.get('breakthrough_detector'), dict) else {}
    recipes = quarantine.get('pattern_to_recipe_compiler') if isinstance(quarantine.get('pattern_to_recipe_compiler'), dict) else {}
    budget_reallocator = quarantine.get('budget_reallocator') if isinstance(quarantine.get('budget_reallocator'), dict) else {}
    time_plan = quarantine.get('time_aware_hunt_plan') if isinstance(quarantine.get('time_aware_hunt_plan'), dict) else {}
    creative_leap = quarantine.get('creative_leap_scorer') if isinstance(quarantine.get('creative_leap_scorer'), dict) else {}
    novelty_governor = quarantine.get('novelty_budget_governor') if isinstance(quarantine.get('novelty_budget_governor'), dict) else {}
    creative_brief = quarantine.get('creative_brief_compiler') if isinstance(quarantine.get('creative_brief_compiler'), dict) else {}
    module_conflicts = quarantine.get('module_conflict_detector') if isinstance(quarantine.get('module_conflict_detector'), dict) else {}
    module_budget = quarantine.get('module_budget_governor') if isinstance(quarantine.get('module_budget_governor'), dict) else {}
    causal_scheduler = quarantine.get('causal_intervention_scheduler') if isinstance(quarantine.get('causal_intervention_scheduler'), dict) else {}
    temperature_controller = quarantine.get('adaptive_search_temperature_controller') if isinstance(quarantine.get('adaptive_search_temperature_controller'), dict) else {}
    route_interactions = quarantine.get('route_interaction_learner') if isinstance(quarantine.get('route_interaction_learner'), dict) else {}
    false_firewall = quarantine.get('false_discovery_firewall') if isinstance(quarantine.get('false_discovery_firewall'), dict) else {}
    strategy_v2 = quarantine.get('hunt_strategy_compiler_v2') if isinstance(quarantine.get('hunt_strategy_compiler_v2'), dict) else {}
    disagreement_court = quarantine.get('learning_disagreement_court') if isinstance(quarantine.get('learning_disagreement_court'), dict) else {}
    promotion_objective = quarantine.get('promotion_aware_search_objective') if isinstance(quarantine.get('promotion_aware_search_objective'), dict) else {}
    strategy_selector = quarantine.get('regime_conditioned_strategy_selector') if isinstance(quarantine.get('regime_conditioned_strategy_selector'), dict) else {}
    meta_objective = quarantine.get('meta_objective_optimizer') if isinstance(quarantine.get('meta_objective_optimizer'), dict) else {}
    exploration_debt = quarantine.get('exploration_debt_ledger') if isinstance(quarantine.get('exploration_debt_ledger'), dict) else {}
    red_team = quarantine.get('adversarial_strategy_red_team') if isinstance(quarantine.get('adversarial_strategy_red_team'), dict) else {}
    pivot = quarantine.get('autonomous_pivot_governor') if isinstance(quarantine.get('autonomous_pivot_governor'), dict) else {}
    simplifier = quarantine.get('runtime_control_simplifier') if isinstance(quarantine.get('runtime_control_simplifier'), dict) else {}
    dashboard = quarantine.get('live_learning_dashboard_feed') if isinstance(quarantine.get('live_learning_dashboard_feed'), dict) else {}
    runbook = quarantine.get('hunt_runbook_compiler') if isinstance(quarantine.get('hunt_runbook_compiler'), dict) else {}
    sentinel = quarantine.get('learning_failure_sentinel') if isinstance(quarantine.get('learning_failure_sentinel'), dict) else {}
    readiness = quarantine.get('pre_hunt_readiness_gate') if isinstance(quarantine.get('pre_hunt_readiness_gate'), dict) else {}
    causal_bandit = quarantine.get('online_causal_bandit') if isinstance(quarantine.get('online_causal_bandit'), dict) else {}
    dna_attribution = quarantine.get('variant_dna_attribution') if isinstance(quarantine.get('variant_dna_attribution'), dict) else {}
    gene_suppression = quarantine.get('negative_gene_suppression') if isinstance(quarantine.get('negative_gene_suppression'), dict) else {}
    family_tree = quarantine.get('live_winner_family_tree') if isinstance(quarantine.get('live_winner_family_tree'), dict) else {}
    frontier_map = quarantine.get('exploration_frontier_map') if isinstance(quarantine.get('exploration_frontier_map'), dict) else {}
    worker_personalities = quarantine.get('adaptive_worker_personalities') if isinstance(quarantine.get('adaptive_worker_personalities'), dict) else {}
    cycle_delta = quarantine.get('cycle_level_learning_delta') if isinstance(quarantine.get('cycle_level_learning_delta'), dict) else {}
    rejection_v2 = quarantine.get('promotion_rejection_predictor_v2') if isinstance(quarantine.get('promotion_rejection_predictor_v2'), dict) else {}
    counterfactual_sim = quarantine.get('counterfactual_hunt_simulator') if isinstance(quarantine.get('counterfactual_hunt_simulator'), dict) else {}
    missed_winners = quarantine.get('missed_winner_detector') if isinstance(quarantine.get('missed_winner_detector'), dict) else {}
    regret_ledger = quarantine.get('causal_regret_ledger') if isinstance(quarantine.get('causal_regret_ledger'), dict) else {}
    grammar_generator = quarantine.get('adaptive_search_grammar_generator') if isinstance(quarantine.get('adaptive_search_grammar_generator'), dict) else {}
    hypothesis_court = quarantine.get('live_hypothesis_kill_scale_court') if isinstance(quarantine.get('live_hypothesis_kill_scale_court'), dict) else {}
    shadow_scoring = quarantine.get('promotion_survival_shadow_scoring') if isinstance(quarantine.get('promotion_survival_shadow_scoring'), dict) else {}
    autopilot = quarantine.get('hunt_autopilot_policy_compiler') if isinstance(quarantine.get('hunt_autopilot_policy_compiler'), dict) else {}
    confidence_calibration = quarantine.get('causal_confidence_calibration') if isinstance(quarantine.get('causal_confidence_calibration'), dict) else {}
    false_warning = quarantine.get('false_discovery_early_warning') if isinstance(quarantine.get('false_discovery_early_warning'), dict) else {}
    debate_council = quarantine.get('self_debate_search_council') if isinstance(quarantine.get('self_debate_search_council'), dict) else {}
    drift_monitor = quarantine.get('learning_drift_monitor') if isinstance(quarantine.get('learning_drift_monitor'), dict) else {}
    promotion_first = quarantine.get('promotion_first_autopilot_v2') if isinstance(quarantine.get('promotion_first_autopilot_v2'), dict) else {}
    horizon_memory = quarantine.get('multi_horizon_memory_stack') if isinstance(quarantine.get('multi_horizon_memory_stack'), dict) else {}
    half_life_v2 = quarantine.get('lesson_half_life_engine_v2') if isinstance(quarantine.get('lesson_half_life_engine_v2'), dict) else {}
    strategy_replay = quarantine.get('cross_hunt_strategy_replay') if isinstance(quarantine.get('cross_hunt_strategy_replay'), dict) else {}
    regime_fingerprint = quarantine.get('temporal_regime_fingerprinting') if isinstance(quarantine.get('temporal_regime_fingerprinting'), dict) else {}
    longitudinal_survival = quarantine.get('longitudinal_promotion_survival_model') if isinstance(quarantine.get('longitudinal_promotion_survival_model'), dict) else {}
    conflict_court_v2 = quarantine.get('memory_conflict_court_v2') if isinstance(quarantine.get('memory_conflict_court_v2'), dict) else {}
    strategy_aging = quarantine.get('strategy_aging_dashboard') if isinstance(quarantine.get('strategy_aging_dashboard'), dict) else {}
    next_opening = quarantine.get('next_hunt_opening_policy_compiler') if isinstance(quarantine.get('next_hunt_opening_policy_compiler'), dict) else {}
    question_planner = quarantine.get('question_driven_hunt_planner') if isinstance(quarantine.get('question_driven_hunt_planner'), dict) else {}
    eig_v2 = quarantine.get('expected_information_gain_scorer_v2') if isinstance(quarantine.get('expected_information_gain_scorer_v2'), dict) else {}
    uncertainty_map = quarantine.get('uncertainty_heatmap') if isinstance(quarantine.get('uncertainty_heatmap'), dict) else {}
    experiment_sequencer = quarantine.get('adaptive_experiment_sequencer') if isinstance(quarantine.get('adaptive_experiment_sequencer'), dict) else {}
    learning_value_stop = quarantine.get('learning_value_stop_loss') if isinstance(quarantine.get('learning_value_stop_loss'), dict) else {}
    causal_ledger = quarantine.get('causal_question_ledger') if isinstance(quarantine.get('causal_question_ledger'), dict) else {}
    epistemic_roles = quarantine.get('worker_epistemic_roles_v2') if isinstance(quarantine.get('worker_epistemic_roles_v2'), dict) else {}
    hypothesis_compiler = quarantine.get('hunt_hypothesis_compiler') if isinstance(quarantine.get('hunt_hypothesis_compiler'), dict) else {}
    experiment_contracts = quarantine.get('experiment_contract_compiler') if isinstance(quarantine.get('experiment_contract_compiler'), dict) else {}
    control_matcher = quarantine.get('control_route_matcher') if isinstance(quarantine.get('control_route_matcher'), dict) else {}
    sequential_monitor = quarantine.get('sequential_test_monitor') if isinstance(quarantine.get('sequential_test_monitor'), dict) else {}
    effect_ledger = quarantine.get('causal_effect_size_ledger') if isinstance(quarantine.get('causal_effect_size_ledger'), dict) else {}
    false_pressure = quarantine.get('false_positive_pressure_gauge') if isinstance(quarantine.get('false_positive_pressure_gauge'), dict) else {}
    debt_paydown = quarantine.get('exploration_debt_paydown_planner') if isinstance(quarantine.get('exploration_debt_paydown_planner'), dict) else {}
    promotion_power = quarantine.get('promotion_aware_power_planner') if isinstance(quarantine.get('promotion_aware_power_planner'), dict) else {}
    scientific_exec = quarantine.get('scientific_hunt_executive') if isinstance(quarantine.get('scientific_hunt_executive'), dict) else {}
    live_evidence = quarantine.get('live_candidate_evidence_builder') if isinstance(quarantine.get('live_candidate_evidence_builder'), dict) else {}
    failure_v3 = quarantine.get('promotion_failure_predictor_v3') if isinstance(quarantine.get('promotion_failure_predictor_v3'), dict) else {}
    gap_router = quarantine.get('evidence_gap_router') if isinstance(quarantine.get('evidence_gap_router'), dict) else {}
    review_queue_v2 = quarantine.get('review_ready_queue_v2') if isinstance(quarantine.get('review_ready_queue_v2'), dict) else {}
    scorecard = quarantine.get('promotion_evidence_scorecard') if isinstance(quarantine.get('promotion_evidence_scorecard'), dict) else {}
    lineage_v2 = quarantine.get('candidate_lineage_explainer_v2') if isinstance(quarantine.get('candidate_lineage_explainer_v2'), dict) else {}
    control_diff = quarantine.get('live_vs_control_differential_report') if isinstance(quarantine.get('live_vs_control_differential_report'), dict) else {}
    packet_exec = quarantine.get('promotion_packet_executive') if isinstance(quarantine.get('promotion_packet_executive'), dict) else {}
    unified_learning = quarantine.get('unified_learning_state_reducer') if isinstance(quarantine.get('unified_learning_state_reducer'), dict) else {}
    artifact_priority = quarantine.get('artifact_priority_arbitration_engine') if isinstance(quarantine.get('artifact_priority_arbitration_engine'), dict) else {}
    provenance_graph = quarantine.get('evidence_provenance_graph') if isinstance(quarantine.get('evidence_provenance_graph'), dict) else {}
    counterfactual_replay_v2 = quarantine.get('counterfactual_promotion_replay') if isinstance(quarantine.get('counterfactual_promotion_replay'), dict) else {}
    survival_sim = quarantine.get('candidate_survival_simulator') if isinstance(quarantine.get('candidate_survival_simulator'), dict) else {}
    regime_shift_v2 = quarantine.get('live_regime_shift_detector_v2') if isinstance(quarantine.get('live_regime_shift_detector_v2'), dict) else {}
    module_trust_v2 = quarantine.get('adaptive_trust_weights_per_module') if isinstance(quarantine.get('adaptive_trust_weights_per_module'), dict) else {}
    worker_elo_v2 = quarantine.get('worker_skill_elo_v2') if isinstance(quarantine.get('worker_skill_elo_v2'), dict) else {}
    knowledge_graph = quarantine.get('route_genome_knowledge_graph') if isinstance(quarantine.get('route_genome_knowledge_graph'), dict) else {}
    causal_interactions = quarantine.get('causal_feature_interaction_miner') if isinstance(quarantine.get('causal_feature_interaction_miner'), dict) else {}
    overfit_library = quarantine.get('overfit_signature_library') if isinstance(quarantine.get('overfit_signature_library'), dict) else {}
    rejection_memory_v2 = quarantine.get('review_rejection_memory_bank_v2') if isinstance(quarantine.get('review_rejection_memory_bank_v2'), dict) else {}
    learning_budget = quarantine.get('learning_budget_optimizer') if isinstance(quarantine.get('learning_budget_optimizer'), dict) else {}
    novelty_floor = quarantine.get('search_novelty_floor') if isinstance(quarantine.get('search_novelty_floor'), dict) else {}
    breakthrough_protocol = quarantine.get('breakthrough_escalation_protocol') if isinstance(quarantine.get('breakthrough_escalation_protocol'), dict) else {}
    discovery_backpressure = quarantine.get('false_discovery_backpressure_controller') if isinstance(quarantine.get('false_discovery_backpressure_controller'), dict) else {}
    strategy_portfolio = quarantine.get('multi_armed_strategy_portfolio') if isinstance(quarantine.get('multi_armed_strategy_portfolio'), dict) else {}
    lesson_ab_harness = quarantine.get('historical_lesson_ab_harness') if isinstance(quarantine.get('historical_lesson_ab_harness'), dict) else {}
    packet_diff_engine = quarantine.get('promotion_packet_diff_engine') if isinstance(quarantine.get('promotion_packet_diff_engine'), dict) else {}
    repair_recipes_v2 = quarantine.get('candidate_repair_recipe_generator') if isinstance(quarantine.get('candidate_repair_recipe_generator'), dict) else {}
    red_team_v2 = quarantine.get('automated_red_team_reviewer') if isinstance(quarantine.get('automated_red_team_reviewer'), dict) else {}
    field_manual_v2 = quarantine.get('learning_compression_field_manual') if isinstance(quarantine.get('learning_compression_field_manual'), dict) else {}
    outcome_attribution_v2 = quarantine.get('hunt_outcome_attribution_v2') if isinstance(quarantine.get('hunt_outcome_attribution_v2'), dict) else {}
    world_dashboard = quarantine.get('world_state_dashboard_artifact') if isinstance(quarantine.get('world_state_dashboard_artifact'), dict) else {}
    meta_governor = quarantine.get('meta_learning_governor') if isinstance(quarantine.get('meta_learning_governor'), dict) else {}
    elite_artifacts = [
        quarantine.get(key)
        for key in ELITE_LEARNING_KEYS
        if isinstance(quarantine.get(key), dict)
    ]
    elite_summary = quarantine.get('elite_learning_system_summary') if isinstance(quarantine.get('elite_learning_system_summary'), dict) else {}
    proof_artifacts = [
        quarantine.get(key)
        for key in PROOF_LEARNING_KEYS
        if isinstance(quarantine.get(key), dict)
    ]
    proof_summary = quarantine.get('proof_learning_system_summary') if isinstance(quarantine.get('proof_learning_system_summary'), dict) else {}
    control_artifacts = [
        quarantine.get(key)
        for key in CLOSED_LOOP_CONTROL_KEYS
        if isinstance(quarantine.get(key), dict)
    ]
    control_summary = quarantine.get('closed_loop_control_learning_summary') if isinstance(quarantine.get('closed_loop_control_learning_summary'), dict) else {}
    nervous_artifacts = [
        quarantine.get(key)
        for key in WORLD_MODEL_NERVOUS_KEYS
        if isinstance(quarantine.get(key), dict)
    ]
    nervous_summary = quarantine.get('world_model_nervous_system_summary') if isinstance(quarantine.get('world_model_nervous_system_summary'), dict) else {}
    orchestration_artifacts = [
        quarantine.get(key)
        for key in ORCHESTRATION_LEARNING_KEYS
        if isinstance(quarantine.get(key), dict)
    ]
    orchestration_summary = quarantine.get('orchestration_learning_summary') if isinstance(quarantine.get('orchestration_learning_summary'), dict) else {}
    fitness_artifacts = [
        quarantine.get(key)
        for key in FITNESS_SELECTION_KEYS
        if isinstance(quarantine.get(key), dict)
    ]
    fitness_summary = quarantine.get('fitness_selection_summary') if isinstance(quarantine.get('fitness_selection_summary'), dict) else {}
    focus_routes = (
        list((packet or {}).get('focus_routes') or [])
        + list(ab_executor.get('focus_routes') or [])
        + list(contamination.get('safe_focus_routes') or [])
        + list(cold_start.get('focus_routes') or [])
        + list(opening.get('focus_routes') or [])
        + list(arbiter.get('safe_focus_routes') or [])
        + list(falsification.get('focus_routes') or [])
        + list(stress_pack.get('focus_routes') or [])
        + list(disagreement.get('falsify_routes') or [])
        + list(hypothesis_market.get('focus_routes') or [])
        + list(bet_sizer.get('focus_routes') or [])
        + list(contrarian.get('focus_routes') or [])
        + list(breakthroughs.get('focus_routes') or [])
        + list(recipes.get('focus_routes') or [])
        + list(budget_reallocator.get('focus_routes') or [])
        + list(time_plan.get('focus_routes') or [])
        + list(creative_leap.get('focus_routes') or [])
        + list(novelty_governor.get('focus_routes') or [])
        + list(creative_brief.get('focus_routes') or [])
        + list(causal_scheduler.get('focus_routes') or [])
        + list(temperature_controller.get('focus_routes') or [])
        + list(route_interactions.get('focus_routes') or [])
        + list(strategy_v2.get('focus_routes') or [])
        + list(disagreement_court.get('retest_routes') or [])
        + list(promotion_objective.get('focus_routes') or [])
        + list(strategy_selector.get('focus_routes') or [])
        + list(exploration_debt.get('focus_routes') or [])
        + list(pivot.get('focus_routes') or [])
        + list(simplifier.get('focus_routes') or [])
        + list(dashboard.get('focus_routes') or [])
        + list(runbook.get('focus_routes') or [])
        + list(causal_bandit.get('focus_routes') or [])
        + list(dna_attribution.get('focus_routes') or [])
        + list(family_tree.get('focus_routes') or [])
        + list(frontier_map.get('focus_routes') or [])
        + list(worker_personalities.get('focus_routes') or [])
        + list(cycle_delta.get('focus_routes') or [])
        + list(rejection_v2.get('repair_routes') or [])
        + list(counterfactual_sim.get('focus_routes') or [])
        + list(missed_winners.get('focus_routes') or [])
        + list(regret_ledger.get('focus_routes') or [])
        + list(grammar_generator.get('focus_routes') or [])
        + list(hypothesis_court.get('focus_routes') or [])
        + list(shadow_scoring.get('focus_routes') or [])
        + list(autopilot.get('focus_routes') or [])
        + list(confidence_calibration.get('focus_routes') or [])
        + list(debate_council.get('focus_routes') or [])
        + list(promotion_first.get('focus_routes') or [])
        + list(horizon_memory.get('focus_routes') or [])
        + list(half_life_v2.get('refresh_routes') or [])
        + list(strategy_replay.get('focus_routes') or [])
        + list(regime_fingerprint.get('focus_routes') or [])
        + list(longitudinal_survival.get('survival_focus_routes') or [])
        + list(conflict_court_v2.get('focus_routes') or [])
        + list(conflict_court_v2.get('falsify_routes') or [])
        + list(strategy_aging.get('resurrect_routes') or [])
        + list(next_opening.get('focus_routes') or [])
        + list(question_planner.get('focus_routes') or [])
        + list(eig_v2.get('focus_routes') or [])
        + list(uncertainty_map.get('probe_routes') or [])
        + list(experiment_sequencer.get('focus_routes') or [])
        + list(causal_ledger.get('focus_routes') or [])
        + list(epistemic_roles.get('focus_routes') or [])
        + list(hypothesis_compiler.get('focus_routes') or [])
        + list(experiment_contracts.get('focus_routes') or [])
        + list(control_matcher.get('control_routes') or [])
        + list(sequential_monitor.get('focus_routes') or [])
        + list(effect_ledger.get('focus_routes') or [])
        + list(debt_paydown.get('focus_routes') or [])
        + list(promotion_power.get('focus_routes') or [])
        + list(scientific_exec.get('focus_routes') or [])
        + list(live_evidence.get('focus_routes') or [])
        + list(failure_v3.get('repair_routes') or [])
        + list(gap_router.get('focus_routes') or [])
        + list(review_queue_v2.get('focus_routes') or [])
        + list(control_diff.get('focus_routes') or [])
        + list(packet_exec.get('focus_routes') or [])
        + list(unified_learning.get('focus_routes') or [])
        + list(artifact_priority.get('focus_routes') or [])
        + list(counterfactual_replay_v2.get('repair_routes') or [])
        + list(survival_sim.get('focus_routes') or [])
        + list(regime_shift_v2.get('focus_routes') or [])
        + list(module_trust_v2.get('focus_routes') or [])
        + list(knowledge_graph.get('focus_routes') or [])
        + list(causal_interactions.get('focus_routes') or [])
        + list(rejection_memory_v2.get('repair_routes') or [])
        + list(learning_budget.get('focus_routes') or [])
        + list(novelty_floor.get('focus_routes') or [])
        + list(breakthrough_protocol.get('focus_routes') or [])
        + list(strategy_portfolio.get('focus_routes') or [])
        + list(repair_recipes_v2.get('focus_routes') or [])
        + list(red_team_v2.get('repair_routes') or [])
        + list(outcome_attribution_v2.get('focus_routes') or [])
        + list(world_dashboard.get('focus_routes') or [])
        + list(meta_governor.get('focus_routes') or [])
        + [route for artifact in elite_artifacts for route in (artifact.get('focus_routes') or artifact.get('repair_routes') or [])]
        + [route for artifact in proof_artifacts for route in (artifact.get('focus_routes') or artifact.get('repair_routes') or [])]
        + [route for artifact in control_artifacts for route in (artifact.get('focus_routes') or artifact.get('repair_routes') or [])]
        + [route for artifact in nervous_artifacts for route in (artifact.get('focus_routes') or artifact.get('repair_routes') or [])]
        + [route for artifact in orchestration_artifacts for route in (artifact.get('focus_routes') or artifact.get('repair_routes') or [])]
        + [route for artifact in fitness_artifacts for route in (artifact.get('focus_routes') or artifact.get('repair_routes') or [])]
    )
    blocked_routes = {str(route) for route in (contamination.get('blocked_routes') or []) if str(route)}
    avoid_routes = (
        list((packet or {}).get('avoid_routes') or [])
        + list(blocked_routes)
        + list(cold_start.get('avoid_routes') or [])
        + list(opening.get('avoid_routes') or [])
        + list(arbiter.get('avoid_routes') or [])
        + list(retirement.get('retire_routes') or [])
        + list(disagreement.get('override_history_routes') or [])
        + list(stop_loss.get('avoid_routes') or [])
        + list(budget_reallocator.get('avoid_routes') or [])
        + list(time_plan.get('avoid_routes') or [])
        + list(module_conflicts.get('pause_routes') or [])
        + list(module_budget.get('pause_routes') or [])
        + list(temperature_controller.get('avoid_routes') or [])
        + list(false_firewall.get('avoid_routes') or [])
        + list(strategy_v2.get('avoid_routes') or [])
        + list(disagreement_court.get('pause_routes') or [])
        + list(promotion_objective.get('avoid_routes') or [])
        + list(strategy_selector.get('avoid_routes') or [])
        + list(red_team.get('avoid_routes') or [])
        + list(pivot.get('avoid_routes') or [])
        + list(simplifier.get('avoid_routes') or [])
        + list(dashboard.get('avoid_routes') or [])
        + list(runbook.get('avoid_routes') or [])
        + list(sentinel.get('pause_routes') or [])
        + list(readiness.get('block_routes') or [])
        + list(gene_suppression.get('avoid_routes') or [])
        + list(cycle_delta.get('avoid_routes') or [])
        + list(rejection_v2.get('avoid_routes') or [])
        + list(regret_ledger.get('avoid_routes') or [])
        + list(hypothesis_court.get('avoid_routes') or [])
        + list(autopilot.get('avoid_routes') or [])
        + list(confidence_calibration.get('pause_routes') or [])
        + list(false_warning.get('avoid_routes') or [])
        + list(debate_council.get('avoid_routes') or [])
        + list(drift_monitor.get('retire_routes') or [])
        + list(promotion_first.get('avoid_routes') or [])
        + list(horizon_memory.get('avoid_routes') or [])
        + list(half_life_v2.get('decay_routes') or [])
        + list(strategy_replay.get('avoid_routes') or [])
        + list(longitudinal_survival.get('risk_avoid_routes') or [])
        + list(conflict_court_v2.get('avoid_routes') or [])
        + list(strategy_aging.get('retire_routes') or [])
        + list(next_opening.get('avoid_routes') or [])
        + list(experiment_sequencer.get('avoid_routes') or [])
        + list(learning_value_stop.get('avoid_routes') or [])
        + list(sequential_monitor.get('avoid_routes') or [])
        + list(false_pressure.get('avoid_routes') or [])
        + list(scientific_exec.get('avoid_routes') or [])
        + list(failure_v3.get('avoid_routes') or [])
        + list(packet_exec.get('avoid_routes') or [])
        + list(unified_learning.get('avoid_routes') or [])
        + list(artifact_priority.get('avoid_routes') or [])
        + list(counterfactual_replay_v2.get('avoid_routes') or [])
        + list(survival_sim.get('avoid_routes') or [])
        + list(regime_shift_v2.get('discount_routes') or [])
        + list(overfit_library.get('avoid_routes') or [])
        + list(rejection_memory_v2.get('avoid_routes') or [])
        + list(discovery_backpressure.get('avoid_routes') or [])
        + list(red_team_v2.get('avoid_routes') or [])
        + list(world_dashboard.get('avoid_routes') or [])
        + list(meta_governor.get('avoid_routes') or [])
        + [route for artifact in elite_artifacts for route in (artifact.get('avoid_routes') or artifact.get('retire_routes') or artifact.get('block_routes') or [])]
        + [route for artifact in proof_artifacts for route in (artifact.get('avoid_routes') or artifact.get('retire_routes') or artifact.get('block_routes') or [])]
        + [route for artifact in control_artifacts for route in (artifact.get('avoid_routes') or artifact.get('retire_routes') or artifact.get('block_routes') or [])]
        + [route for artifact in nervous_artifacts for route in (artifact.get('avoid_routes') or artifact.get('retire_routes') or artifact.get('block_routes') or [])]
        + [route for artifact in orchestration_artifacts for route in (artifact.get('avoid_routes') or artifact.get('retire_routes') or artifact.get('block_routes') or [])]
        + [route for artifact in fitness_artifacts for route in (artifact.get('avoid_routes') or artifact.get('retire_routes') or artifact.get('block_routes') or [])]
    )
    focus_routes = [str(route) for route in focus_routes if str(route) and str(route) not in set(str(item) for item in avoid_routes)]
    return {
        'live_only': True,
        'max_workers': min(4, max(1, int(packet.get('max_workers') or 4))) if packet else 4,
        'focus_routes': list(dict.fromkeys(focus_routes)),
        'avoid_routes': list(dict.fromkeys(str(route) for route in avoid_routes if str(route))),
        'mutation_width': (sentinel.get('mutation_width') or next((artifact.get('mutation_width') for artifact in fitness_artifacts if artifact.get('mutation_width') == 'tight'), '') or next((artifact.get('mutation_width') for artifact in orchestration_artifacts if artifact.get('mutation_width') == 'tight'), '') or next((artifact.get('mutation_width') for artifact in nervous_artifacts if artifact.get('mutation_width') == 'tight'), '') or next((artifact.get('mutation_width') for artifact in control_artifacts if artifact.get('mutation_width') == 'tight'), '') or next((artifact.get('mutation_width') for artifact in elite_artifacts if artifact.get('mutation_width') == 'tight'), '') or meta_governor.get('mutation_width') or discovery_backpressure.get('mutation_width') or novelty_floor.get('mutation_width') or simplifier.get('mutation_width') or promotion_first.get('mutation_width') or (scientific_exec.get('command_packet') or {}).get('mutation_width') or next_opening.get('mutation_width') or autopilot.get('mutation_width') or frontier_map.get('mutation_width') or causal_bandit.get('mutation_width') or runbook.get('mutation_width') or dashboard.get('mutation_width') or pivot.get('mutation_width') or strategy_selector.get('mutation_width') or strategy_v2.get('mutation_width') or temperature_controller.get('mutation_width') or novelty_governor.get('mutation_width') or time_plan.get('mutation_width') or budget_reallocator.get('mutation_width') or opening.get('mutation_width') or (packet or {}).get('mutation_width') or 'medium'),
        'batch_size_multiplier': round(min(
            [float(simplifier.get('batch_size_multiplier') or promotion_first.get('batch_size_multiplier') or meta_governor.get('batch_size_multiplier') or discovery_backpressure.get('batch_size_multiplier') or false_pressure.get('batch_size_multiplier') or (scientific_exec.get('command_packet') or {}).get('batch_size_multiplier') or next_opening.get('batch_size_multiplier') or autopilot.get('batch_size_multiplier') or (float((packet or {}).get('batch_size_multiplier') or 1.0) * float(learning_budget.get('batch_size_multiplier') or 1.0) * float(budget_reallocator.get('batch_size_multiplier') or 1.0) * float(time_plan.get('batch_size_multiplier') or 1.0) * float(novelty_governor.get('batch_size_multiplier') or 1.0) * float(module_budget.get('batch_size_multiplier') or 1.0) * float(temperature_controller.get('batch_size_multiplier') or 1.0) * float(promotion_objective.get('batch_size_multiplier') or 1.0) * float(strategy_selector.get('batch_size_multiplier') or 1.0) * float(meta_objective.get('batch_size_multiplier') or 1.0) * float(pivot.get('batch_size_multiplier') or 1.0)))]
            + [float(artifact.get('batch_size_multiplier')) for artifact in elite_artifacts if artifact.get('batch_size_multiplier') is not None]
            + [float(artifact.get('batch_size_multiplier')) for artifact in proof_artifacts if artifact.get('batch_size_multiplier') is not None]
            + [float(artifact.get('batch_size_multiplier')) for artifact in control_artifacts if artifact.get('batch_size_multiplier') is not None]
            + [float(artifact.get('batch_size_multiplier')) for artifact in nervous_artifacts if artifact.get('batch_size_multiplier') is not None]
            + [float(artifact.get('batch_size_multiplier')) for artifact in orchestration_artifacts if artifact.get('batch_size_multiplier') is not None]
            + [float(artifact.get('batch_size_multiplier')) for artifact in fitness_artifacts if artifact.get('batch_size_multiplier') is not None]
        ), 4),
        'route_breadth': (packet or {}).get('route_breadth') or 'balanced',
        'commands': list((packet or {}).get('commands') or []),
        'scientific_commands': list(scientific_exec.get('commands') or [])[:12],
        'degrade_mode': bool((packet or {}).get('degraded')),
        'learning_ops_status': dashboard.get('status'),
        'pre_hunt_ready': readiness.get('ready'),
        'learning_failure_count': sentinel.get('failure_count'),
        'online_causal_top_lane': causal_bandit.get('top_lane'),
        'variant_dna_top_gene': (dna_attribution.get('top_gene') or {}).get('gene'),
        'cycle_learning_delta': cycle_delta.get('summary'),
        'promotion_rejection_v2_highest_risk': rejection_v2.get('highest_risk') or {},
        'top_counterfactual': counterfactual_sim.get('top_counterfactual') or {},
        'missed_winner_count': missed_winners.get('missed_count'),
        'top_causal_regret': regret_ledger.get('top_regret') or {},
        'autopilot_policy_packet': autopilot.get('policy_packet') or {},
        'promotion_first_policy_packet': promotion_first.get('policy_packet') or {},
        'false_discovery_warning_count': false_warning.get('warning_count'),
        'memory_horizon_summary': horizon_memory.get('summary') or {},
        'lesson_half_life_top': half_life_v2.get('top_lesson') or {},
        'cross_hunt_replay_top': strategy_replay.get('top_replay') or {},
        'temporal_regime_top': regime_fingerprint.get('top_fingerprint') or {},
        'longitudinal_survival_top': longitudinal_survival.get('top_survival_feature') or {},
        'memory_conflict_v2_count': conflict_court_v2.get('case_count'),
        'strategy_aging_counts': strategy_aging.get('aging_counts') or {},
        'next_hunt_opening_policy': next_opening.get('policy_packet') or {},
        'question_planner_top': question_planner.get('top_question') or {},
        'expected_information_gain_top': eig_v2.get('top_score') or {},
        'uncertainty_heatmap_top': uncertainty_map.get('top_cell') or {},
        'adaptive_experiment_next_step': experiment_sequencer.get('next_step') or {},
        'learning_value_stop_count': learning_value_stop.get('stop_count'),
        'causal_question_top': causal_ledger.get('top_question') or {},
        'epistemic_role_assignments': list(epistemic_roles.get('assignments') or [])[:4],
        'compiled_hypotheses': list(hypothesis_compiler.get('hypotheses') or [])[:5],
        'experiment_contract_top': experiment_contracts.get('top_contract') or {},
        'control_route_top': control_matcher.get('top_match') or {},
        'sequential_test_top': sequential_monitor.get('top_decision') or {},
        'causal_effect_top': effect_ledger.get('top_effect') or {},
        'false_positive_pressure': {
            'score': false_pressure.get('pressure_score'),
            'mode': false_pressure.get('mode'),
        },
        'exploration_paydown_top': debt_paydown.get('top_plan') or {},
        'promotion_power_top': promotion_power.get('top_plan') or {},
        'scientific_executive_summary': scientific_exec.get('summary') or {},
        'scientific_executive_top_command': scientific_exec.get('top_command') or {},
        'live_candidate_evidence_top': live_evidence.get('top_packet') or {},
        'promotion_failure_v3_highest_risk': failure_v3.get('highest_risk') or {},
        'evidence_gap_top_task': gap_router.get('top_task') or {},
        'review_ready_v2_top': review_queue_v2.get('top_ready') or {},
        'promotion_scorecard_top': scorecard.get('top_scorecard') or {},
        'candidate_lineage_top': lineage_v2.get('top_explanation') or {},
        'live_control_differential_top': control_diff.get('top_report') or {},
        'promotion_packet_executive_summary': packet_exec.get('summary') or {},
        'promotion_packet_top_decision': packet_exec.get('top_decision') or {},
        'unified_learning_summary': unified_learning.get('summary') or {},
        'artifact_priority_top': artifact_priority.get('top_artifact') or {},
        'evidence_provenance_top': provenance_graph.get('top_node') or {},
        'counterfactual_promotion_top': counterfactual_replay_v2.get('top_replay') or {},
        'candidate_survival_top': survival_sim.get('top_simulation') or {},
        'regime_shift_v2': {'mode': regime_shift_v2.get('mode'), 'shift_score': regime_shift_v2.get('shift_score')},
        'module_trust_top': module_trust_v2.get('top_module') or {},
        'worker_skill_top': worker_elo_v2.get('top_worker') or {},
        'knowledge_graph_summary': knowledge_graph.get('summary') or {},
        'causal_interaction_top': causal_interactions.get('top_interaction') or {},
        'overfit_signature_top': overfit_library.get('top_signature') or {},
        'rejection_memory_top': rejection_memory_v2.get('top_memory') or {},
        'learning_budget_top': learning_budget.get('top_allocation') or {},
        'novelty_floor_top': novelty_floor.get('summary') or {},
        'breakthrough_top': breakthrough_protocol.get('top_breakthrough') or {},
        'false_discovery_backpressure': {'score': discovery_backpressure.get('pressure_score'), 'mode': discovery_backpressure.get('mode')},
        'strategy_portfolio_top': strategy_portfolio.get('top_arm') or {},
        'historical_lesson_ab_top': lesson_ab_harness.get('top_test') or {},
        'promotion_packet_diff_top': packet_diff_engine.get('top_diff') or {},
        'repair_recipe_top': repair_recipes_v2.get('top_recipe') or {},
        'red_team_top': red_team_v2.get('top_objection') or {},
        'field_manual_top_rule': field_manual_v2.get('top_rule') or {},
        'outcome_attribution_top': outcome_attribution_v2.get('top_attribution') or {},
        'world_state_summary': world_dashboard.get('summary') or {},
        'meta_learning_governor_summary': meta_governor.get('summary') or {},
        'elite_learning_summary': elite_summary.get('summary') or {},
        'elite_next_best_question': (quarantine.get('world_state_next_best_question_engine') or {}).get('top_record') if isinstance(quarantine.get('world_state_next_best_question_engine'), dict) else {},
        'proof_learning_summary': proof_summary.get('summary') or {},
        'truth_ledger_top_claim': (quarantine.get('learning_truth_ledger') or {}).get('top_record') if isinstance(quarantine.get('learning_truth_ledger'), dict) else {},
        'research_agenda_top_question': (quarantine.get('adaptive_research_agenda') or {}).get('top_record') if isinstance(quarantine.get('adaptive_research_agenda'), dict) else {},
        'review_board_decision': (quarantine.get('autonomous_hunt_review_board') or {}).get('top_record') if isinstance(quarantine.get('autonomous_hunt_review_board'), dict) else {},
        'closed_loop_control_summary': control_summary.get('summary') or {},
        'bayesian_top_belief': (quarantine.get('bayesian_belief_engine') or {}).get('top_record') if isinstance(quarantine.get('bayesian_belief_engine'), dict) else {},
        'active_experiment_next': (quarantine.get('active_experiment_selector') or {}).get('top_record') if isinstance(quarantine.get('active_experiment_selector'), dict) else {},
        'learning_status_feed': (quarantine.get('real_time_learning_dashboard_feed') or {}).get('top_record') if isinstance(quarantine.get('real_time_learning_dashboard_feed'), dict) else {},
        'world_model_nervous_system_summary': nervous_summary.get('summary') or {},
        'nervous_system_top_truth': (quarantine.get('hypothesis_dependency_graph') or {}).get('top_record') if isinstance(quarantine.get('hypothesis_dependency_graph'), dict) else {},
        'nervous_system_top_promotion': (quarantine.get('promotion_packet_completeness_optimizer') or {}).get('top_record') if isinstance(quarantine.get('promotion_packet_completeness_optimizer'), dict) else {},
        'nervous_system_world_audit': (quarantine.get('world_model_self_audit_loop') or {}).get('top_record') if isinstance(quarantine.get('world_model_self_audit_loop'), dict) else {},
        'orchestration_learning_summary': orchestration_summary.get('summary') or {},
        'orchestration_top_artifact_governance': (quarantine.get('artifact_governance_dashboard') or {}).get('top_record') if isinstance(quarantine.get('artifact_governance_dashboard'), dict) else {},
        'orchestration_top_hunt_exec': (quarantine.get('hunt_executive_controller') or {}).get('top_record') if isinstance(quarantine.get('hunt_executive_controller'), dict) else {},
        'fitness_selection_summary': fitness_summary.get('summary') or {},
        'fitness_top_scorecard': (quarantine.get('artifact_fitness_scorecard') or {}).get('top_record') if isinstance(quarantine.get('artifact_fitness_scorecard'), dict) else {},
        'fitness_trust_policy': (quarantine.get('learning_layer_trust_policy') or {}).get('top_record') if isinstance(quarantine.get('learning_layer_trust_policy'), dict) else {},
        'fitness_world_report': (quarantine.get('world_model_fitness_report') or {}).get('top_record') if isinstance(quarantine.get('world_model_fitness_report'), dict) else {},
    }


def _stop_requested(args: argparse.Namespace) -> bool:
    payload = _read_json(args.stop_signal_json) if getattr(args, 'stop_signal_json', '') else {}
    return bool(payload.get('stop'))


def _filter_quarantined_seeds(seeds: list[dict], quarantine: dict, *, enforce_runtime_controls: bool = True) -> list[dict]:
    if not enforce_runtime_controls:
        return list(seeds)
    adapter = _runtime_adapter_from_quarantine(quarantine)
    skip = {str(route) for route in (quarantine.get('skip_routes') or []) if str(route)}
    skip.update(str(route) for route in (adapter.get('avoid_routes') or []) if str(route))
    widen = {str(route) for route in (quarantine.get('widen_routes') or []) if str(route)}
    narrow = {str(route) for route in (quarantine.get('narrow_routes') or []) if str(route)}
    holdout_repair = {str(route) for route in (quarantine.get('holdout_repair_routes') or []) if str(route)}
    day_repair = {str(route) for route in (quarantine.get('day_consistency_repair_routes') or []) if str(route)}
    treatment_budget = quarantine.get('treatment_budget') if isinstance(quarantine.get('treatment_budget'), dict) else {}
    policy_exec = quarantine.get('policy_executor') if isinstance(quarantine.get('policy_executor'), dict) else {}
    policy_routes = {str(route) for route in (policy_exec.get('focus_routes') or []) if str(route)}
    runtime_focus = {str(route) for route in (adapter.get('focus_routes') or []) if str(route)}
    structural_routes = {str(route) for route in (adapter.get('structural_mutation_routes') or []) if str(route)}
    throttle_routes = {str(route) for route in (adapter.get('throttle_routes') or []) if str(route)}
    throttle_multipliers = adapter.get('throttle_multipliers') if isinstance(adapter.get('throttle_multipliers'), dict) else {}
    quality_controls = adapter.get('quality_protection_controls') if isinstance(adapter.get('quality_protection_controls'), dict) else {}
    quality_routes = {str(route) for route in (quality_controls.get('protected_routes') or []) if str(route)}
    quality_mode = str(adapter.get('quality_protection_mode') or quality_controls.get('quality_protection_mode') or '')
    quality_failures = {str(item) for item in (quality_controls.get('target_failures') or []) if str(item)}
    repair_envelope = adapter.get('promotion_ready_repair_envelope') if isinstance(adapter.get('promotion_ready_repair_envelope'), dict) else {}
    envelope_active = bool(repair_envelope.get('active'))
    edge_preservation_routes = {
        str(route) for route in (
            list(adapter.get('edge_preservation_routes') or [])
            + list(repair_envelope.get('edge_preservation_routes') or [])
        )
        if str(route)
    }
    close_sibling_routes = {
        str(route) for route in (
            list(adapter.get('close_sibling_routes') or [])
            + list(repair_envelope.get('close_sibling_routes') or [])
        )
        if str(route)
    }
    envelope_allowed_routes = {
        str(route) for route in (
            list(repair_envelope.get('allowed_routes') or [])
            + list(edge_preservation_routes)
            + list(close_sibling_routes)
        )
        if str(route)
    }
    generic_deprioritize_patterns = {
        str(route) for route in (
            list(adapter.get('generic_route_deprioritize_patterns') or [])
            + list(repair_envelope.get('generic_route_deprioritize_patterns') or [])
        )
        if str(route)
    }
    outside_envelope_multiplier = max(
        0.01,
        min(1.0, float(adapter.get('outside_repair_envelope_multiplier') or repair_envelope.get('outside_envelope_sample_weight_multiplier') or 0.08)),
    )
    route_budget_caps = adapter.get('route_budget_caps') if isinstance(adapter.get('route_budget_caps'), dict) else {}
    cap_by_route = {
        str(row.get('route_key') or ''): row
        for row in (route_budget_caps.get('route_caps') or [])
        if isinstance(row, dict) and row.get('route_key')
    }
    retire_routes = {str(route) for route in (adapter.get('retire_routes') or []) if str(route)}
    skip.update(retire_routes)
    command_by_route = {
        str(command.get('route_key') or ''): command
        for command in (adapter.get('commands') or [])
        if isinstance(command, dict) and command.get('route_key')
    }
    protected_route_patterns = set(quality_routes) | set(edge_preservation_routes) | set(close_sibling_routes) | {
        route
        for route, command in command_by_route.items()
        if str(command.get('action') or '') in {'repair', 'revalidate', 'probe'}
    }
    treatment_priors = {
        str(row.get('treatment')): row
        for row in (treatment_budget.get('priors') or [])
        if isinstance(row, dict) and row.get('treatment')
    }
    best_treatment = str(quarantine.get('best_treatment') or treatment_budget.get('best_treatment') or '')
    worst_treatment = str(quarantine.get('worst_treatment') or treatment_budget.get('worst_treatment') or '')
    kept = []
    for seed in seeds:
        route = _route_key_from_seed(seed)
        protected_route = _route_in_patterns(route, protected_route_patterns)
        if (route in skip or _route_in_patterns(route, skip)) and not protected_route:
            continue
        seed = dict(seed)
        if protected_route:
            seed['protected_runtime_focus'] = True
        if _route_in_patterns(route, holdout_repair) or _route_in_patterns(route, day_repair):
            seed['sample_weight'] = float(seed.get('sample_weight') or 1.0) * 1.35
            seed['promotion_repair_lane'] = 'holdout_repair' if _route_in_patterns(route, holdout_repair) else 'day_consistency_repair'
        if _route_in_patterns(route, widen):
            match = dict(seed.get('match') or {})
            if match.get('session_phase'):
                match.pop('session_phase', None)
            seed['match'] = match
            seed['sample_weight'] = float(seed.get('sample_weight') or 1.0) * 1.15
            seed['promotion_repair_lane'] = 'edge_expansion'
        if _route_in_patterns(route, narrow):
            seed['sample_weight'] = float(seed.get('sample_weight') or 1.0) * 0.80
            seed['promotion_repair_lane'] = 'route_safety_repair'
        lane = str(seed.get('promotion_repair_lane') or '')
        treatment = {
            'holdout_repair': 'holdout_repair',
            'day_consistency_repair': 'day_consistency_repair',
            'edge_expansion': 'route_widen',
            'route_safety_repair': 'route_narrow',
        }.get(lane, '')
        if treatment:
            prior = treatment_priors.get(treatment, {})
            seed['treatment_prior'] = prior
            if treatment == best_treatment:
                seed['sample_weight'] = float(seed.get('sample_weight') or 1.0) * 1.20
            elif treatment == worst_treatment:
                seed['sample_weight'] = float(seed.get('sample_weight') or 1.0) * 0.65
        if _route_in_patterns(route, policy_routes):
            seed['sample_weight'] = float(seed.get('sample_weight') or 1.0) * 1.30
            seed['policy_focus'] = True
        in_edge_envelope = _route_in_patterns(route, edge_preservation_routes)
        in_close_sibling_envelope = _route_in_patterns(route, close_sibling_routes)
        in_allowed_envelope = _route_in_patterns(route, envelope_allowed_routes)
        if envelope_active:
            envelope_targets = {str(item) for item in (repair_envelope.get('target_failures') or []) if str(item)}
            if in_edge_envelope:
                seed['sample_weight'] = float(seed.get('sample_weight') or 1.0) * 2.05
                seed['promotion_repair_lane'] = 'edge_preservation_repair'
                seed['edge_preservation_repair'] = True
                seed['quality_protection'] = True
                seed['quality_target_failures'] = sorted(envelope_targets or quality_failures)
            elif in_close_sibling_envelope:
                seed['sample_weight'] = float(seed.get('sample_weight') or 1.0) * 1.65
                seed['promotion_repair_lane'] = 'close_sibling_revalidation'
                seed['close_sibling_repair'] = True
                seed['quality_protection'] = True
                seed['quality_target_failures'] = sorted(envelope_targets or quality_failures)
            else:
                seed['sample_weight'] = float(seed.get('sample_weight') or 1.0) * outside_envelope_multiplier
                seed['outside_promotion_ready_envelope'] = True
            if generic_deprioritize_patterns and _route_in_patterns(route, generic_deprioritize_patterns) and not in_allowed_envelope:
                seed['sample_weight'] = float(seed.get('sample_weight') or 1.0) * 0.25
                seed['generic_route_deprioritized_by_repair_envelope'] = True
        if _route_in_patterns(route, runtime_focus):
            seed['sample_weight'] = float(seed.get('sample_weight') or 1.0) * 1.45
            seed['runtime_focus'] = True
        elif runtime_focus and (quality_mode in {'hard', 'medium'} or command_by_route):
            seed['sample_weight'] = float(seed.get('sample_weight') or 1.0) * 0.20
            seed['off_focus_repair_deprioritized'] = True
        cap = _route_value(cap_by_route, route, None)
        if cap:
            current_share = float(cap.get('current_repair_share_pct') or 0.0)
            max_share = float(cap.get('max_budget_share_pct') or 100.0)
            if cap.get('cap_required') and current_share > 0:
                multiplier = max(0.20, min(0.95, max_share / current_share))
                seed['sample_weight'] = float(seed.get('sample_weight') or 1.0) * multiplier
                seed['budget_cap_multiplier'] = round(multiplier, 6)
            seed['route_budget_cap'] = cap
        if _route_in_patterns(route, structural_routes):
            seed['sample_weight'] = float(seed.get('sample_weight') or 1.0) * 1.55
            seed['promotion_repair_lane'] = 'structural_mutation'
            seed['force_structural_mutation'] = True
        if _route_in_patterns(route, quality_routes) or (quality_mode == 'hard' and not quality_routes):
            seed['quality_protection'] = True
            seed['quality_target_failures'] = sorted(quality_failures)
            if 'robustness_below_floor' in quality_failures or 'holdout_positive_but_not_promotion_grade' in quality_failures:
                seed['sample_weight'] = float(seed.get('sample_weight') or 1.0) * 1.15
                seed['promotion_repair_lane'] = 'robustness_holdout_repair'
            if 'promotion_readiness_below_floor' in quality_failures:
                seed['sample_weight'] = float(seed.get('sample_weight') or 1.0) * 1.10
            if 'route_breadth' in quality_failures and seed.get('match', {}).get('session_phase'):
                match = dict(seed.get('match') or {})
                match.pop('session_phase', None)
                seed['match'] = match
        if _route_in_patterns(route, throttle_routes):
            multiplier = max(0.01, min(1.0, float(_route_value(throttle_multipliers, route, 0.15) or 0.15)))
            seed['sample_weight'] = float(seed.get('sample_weight') or 1.0) * multiplier
            seed['runtime_throttled'] = True
            seed['runtime_throttle_multiplier'] = round(multiplier, 6)
        command = _route_command(command_by_route, route)
        if command:
            action = str(command.get('action') or '')
            target_failures = {str(item) for item in (command.get('target_failures') or []) if str(item)}
            envelope_lane = str(command.get('primary_repair_lane') or '')
            secondary_lanes = {str(item) for item in (command.get('secondary_repair_lanes') or []) if str(item)}
            preserve_envelope_lane = bool(seed.get('edge_preservation_repair') or seed.get('close_sibling_repair') or envelope_lane)
            seed['runtime_command'] = command
            seed['runtime_action'] = action
            if envelope_lane:
                seed['promotion_repair_lane'] = envelope_lane
            elif not preserve_envelope_lane:
                seed['promotion_repair_lane'] = {
                    'repair': 'runtime_repair',
                    'revalidate': 'runtime_revalidation',
                    'scale': 'runtime_scale',
                    'probe': 'runtime_probe',
                    'quarantine': 'runtime_quarantine',
                }.get(action, seed.get('promotion_repair_lane') or '')
            seed['sample_weight'] = float(seed.get('sample_weight') or 1.0) * {
                'scale': 1.35,
                'repair': 1.30,
                'revalidate': 1.20,
                'probe': 1.10,
                'quarantine': 0.25,
            }.get(action, 1.0)
            if target_failures:
                seed['quality_target_failures'] = sorted(set(seed.get('quality_target_failures') or []) | target_failures)
            if secondary_lanes:
                seed['secondary_repair_lanes'] = sorted(set(seed.get('secondary_repair_lanes') or []) | secondary_lanes)
                if 'promotion_readiness_repair' in secondary_lanes:
                    seed['readiness_first_repair'] = True
                    seed['sample_weight'] = float(seed.get('sample_weight') or 1.0) * 1.35
                if {'robustness_holdout_repair', 'robustness_floor_repair', 'holdout_grade_repair'} & secondary_lanes:
                    seed['robustness_holdout_focus'] = True
                    seed['robustness_floor_focus'] = 'robustness_floor_repair' in secondary_lanes
                    seed['holdout_grade_focus'] = 'holdout_grade_repair' in secondary_lanes
                    seed['sample_weight'] = float(seed.get('sample_weight') or 1.0) * 1.30
            if envelope_lane in {'promotion_ready_finish', 'promotion_readiness_repair'} or command.get('finish_mode'):
                seed['readiness_first_repair'] = True
                seed['quality_protection'] = True
            if envelope_lane in {'robustness_floor_repair', 'robustness_holdout_repair'} or command.get('preserve_robustness_floor'):
                seed['robustness_holdout_focus'] = True
                seed['robustness_floor_focus'] = True
                seed['quality_protection'] = True
            if envelope_lane in {'holdout_grade_repair', 'robustness_holdout_repair'} or command.get('preserve_holdout_grade'):
                seed['robustness_holdout_focus'] = True
                seed['holdout_grade_focus'] = True
                seed['quality_protection'] = True
            if command.get('finish_mode'):
                seed['finish_mode_repair'] = True
                seed['sample_weight'] = float(seed.get('sample_weight') or 1.0) * 1.25
            if command.get('mutation_width') == 'tight':
                seed['sample_weight'] = float(seed.get('sample_weight') or 1.0) * 0.90
            if command.get('mutation_width') == 'micro':
                seed['sample_weight'] = float(seed.get('sample_weight') or 1.0) * 0.95
            if 'promotion_readiness_below_floor' in target_failures:
                seed['sample_weight'] = float(seed.get('sample_weight') or 1.0) * 1.45
                seed['readiness_first_repair'] = True
                if not preserve_envelope_lane:
                    seed['promotion_repair_lane'] = 'promotion_readiness_repair'
                seed['quality_protection'] = True
            if 'route_breadth' in target_failures:
                seed['sample_weight'] = float(seed.get('sample_weight') or 1.0) * 1.35
                if not preserve_envelope_lane:
                    seed['promotion_repair_lane'] = 'route_breadth_repair'
                if seed.get('match', {}).get('session_phase'):
                    match = dict(seed.get('match') or {})
                    match.pop('session_phase', None)
                    seed['match'] = match
            if 'robustness_below_floor' in target_failures or {'holdout_positive_but_not_promotion_grade', 'missing_holdout_credit', 'promotion_grade_holdout_credit'} & target_failures:
                seed['sample_weight'] = float(seed.get('sample_weight') or 1.0) * 1.40
                seed['robustness_holdout_focus'] = True
                if 'robustness_below_floor' in target_failures:
                    seed['robustness_floor_focus'] = True
                if {'holdout_positive_but_not_promotion_grade', 'missing_holdout_credit', 'promotion_grade_holdout_credit'} & target_failures:
                    seed['holdout_grade_focus'] = True
                if not preserve_envelope_lane:
                    seed['promotion_repair_lane'] = 'robustness_holdout_repair'
                seed['quality_protection'] = True
            if str(adapter.get('clone_prevention_mode') or '') in {'high', 'hard'}:
                seed['force_structural_mutation'] = True
        kept.append(seed)
    return kept or ([] if skip else seeds)


def _runtime_adapter_for_execution(adapter: dict, *, enforce_runtime_controls: bool, exact_variant_count: bool) -> dict:
    effective = dict(adapter or {})
    if not enforce_runtime_controls:
        effective["control_mode"] = "observe"
        effective["observed_batch_size_multiplier"] = effective.get("batch_size_multiplier")
        effective["observed_mutation_width"] = effective.get("mutation_width")
        effective["observed_avoid_routes"] = list(effective.get("avoid_routes") or [])
        effective["observed_retire_routes"] = list(effective.get("retire_routes") or [])
        effective["avoid_routes"] = []
        effective["retire_routes"] = []
        effective["degrade_mode"] = False
        effective["mutation_width"] = "medium"
        effective["batch_size_multiplier"] = 1.0
    else:
        effective["control_mode"] = "enforce"
    if exact_variant_count:
        effective["exact_variant_count"] = True
        if "observed_batch_size_multiplier" not in effective:
            effective["observed_batch_size_multiplier"] = effective.get("batch_size_multiplier")
        effective["batch_size_multiplier"] = 1.0
    return effective


def _seed_weight(seed: dict, online_state: dict) -> float:
    base = float(seed.get('sample_weight') or (8.0 if seed.get('focus') else 1.0))
    route = _route_key_from_seed(seed)
    adapter = online_state.get('runtime_command_adapter') if isinstance(online_state.get('runtime_command_adapter'), dict) else {}
    route_actions = adapter.get('route_actions') if isinstance(adapter.get('route_actions'), dict) else {}
    if route and _route_value(route_actions, route, None) is not None:
        base *= 1.25
    quality_controls = adapter.get('quality_protection_controls') if isinstance(adapter.get('quality_protection_controls'), dict) else {}
    protected_patterns = {str(item) for item in (quality_controls.get('protected_routes') or []) if str(item)}
    repair_envelope = adapter.get('promotion_ready_repair_envelope') if isinstance(adapter.get('promotion_ready_repair_envelope'), dict) else {}
    protected_patterns.update(str(item) for item in (adapter.get('edge_preservation_routes') or []) if str(item))
    protected_patterns.update(str(item) for item in (adapter.get('close_sibling_routes') or []) if str(item))
    protected_patterns.update(str(item) for item in (repair_envelope.get('allowed_routes') or []) if str(item))
    for command in adapter.get('commands') or []:
        if isinstance(command, dict) and str(command.get('action') or '') in {'repair', 'revalidate', 'probe'} and command.get('route_key'):
            protected_patterns.add(str(command.get('route_key')))
    protected_route = route and _route_in_patterns(route, protected_patterns)
    if route and _route_in_patterns(route, set(str(item) for item in (adapter.get('avoid_routes') or []))) and not protected_route:
        base *= 0.05
    throttle_routes = {str(item) for item in (adapter.get('throttle_routes') or []) if str(item)}
    throttle_multipliers = adapter.get('throttle_multipliers') if isinstance(adapter.get('throttle_multipliers'), dict) else {}
    if route and _route_in_patterns(route, throttle_routes):
        base *= max(0.01, min(1.0, float(_route_value(throttle_multipliers, route, 0.15) or 0.15)))
    allocation = ((online_state.get('online_allocation') or {}).get('allocation') or [])
    for arm in allocation:
        if str(arm.get('route_key') or '') == route:
            base *= max(0.25, float(arm.get('recommended_budget_pct') or 1.0) / 10.0)
            break
    return base


def _load_or_build_gap_report(compiled: dict, args: argparse.Namespace) -> dict:
    if args.gap_report:
        payload = _read_json(args.gap_report)
        if payload:
            return payload
    report_args = argparse.Namespace(
        compiled_decision_tape=args.compiled_decision_tape,
        start_balance=args.start_balance,
        top_rows=args.gap_top_rows,
        max_trades_per_day=args.max_trades_per_day,
        max_trades_per_ticker_day=args.max_trades_per_ticker_day,
    )
    return step2_oracle_gap_report.build_report(compiled, report_args)


def _active_seed() -> lab.Variant:
    active = active_engine_baseline.reference_variant()
    return lab.Variant(active.name, dict(active.weights), float(active.bias or 0.0))


def _score(compiled: dict, variants: list, args: argparse.Namespace, lineage_by_name: dict[str, dict] | None = None) -> list[dict]:
    if getattr(args, 'use_score_cache', True):
        rows = step2_score_cache.score_variants_cached(
            compiled,
            variants,
            float(args.start_balance),
            gate=None,
            sim_config=_sim_config(args),
            cache_db=getattr(args, 'score_cache_db', str(step2_score_cache.DEFAULT_CACHE_DB)),
        )
    else:
        rows = decision_tape_compiled.simulate_variants(
            compiled,
            variants,
            float(args.start_balance),
            gate=None,
            sim_config=_sim_config(args),
        )
    if rows is None:
        raise RuntimeError('compiled Step 2 simulation unavailable')
    lineage_by_name = lineage_by_name or {}
    out = []
    for row in rows:
        full = row.get('decision_full') or {}
        lineage = lineage_by_name.get(str(row.get('variant') or ''), {})
        out.append({
            'variant': row.get('variant'),
            'weights': row.get('weights') or {},
            'bias': float(row.get('bias') or 0.0),
            'routes': row.get('routes') or [],
            'lineage': lineage,
            'parent_variant': lineage.get('parent_variant') if isinstance(lineage, dict) else '',
            'parent_behavior_key': lineage.get('parent_behavior_key') if isinstance(lineage, dict) else '',
            'mutation_lane': lineage.get('mutation_lane') if isinstance(lineage, dict) else '',
            'mutation_reason': lineage.get('mutation_reason') if isinstance(lineage, dict) else '',
            'worker_role': lineage.get('worker_role') if isinstance(lineage, dict) else '',
            'generation': lineage.get('generation') if isinstance(lineage, dict) else 0,
            'route_seed': lineage.get('route_seed') if isinstance(lineage, dict) else '',
            'mutation_scale': lineage.get('mutation_scale') if isinstance(lineage, dict) else 0.0,
            'route_audit': row.get('route_audit') or {},
            'routed_scoring_profile': bool(row.get('routed_scoring_profile')),
            'step2_pnl': float(full.get('pnl') or 0.0),
            'step2_trades': int(full.get('trades') or 0),
            'step2_win_rate_pct': full.get('win_rate_pct'),
            'by_ticker': full.get('by_ticker'),
            'by_day': full.get('by_day'),
            'by_side': full.get('by_side'),
            'skipped': full.get('skipped'),
            'exit_replay_model': row.get('exit_replay_model') or full.get('exit_replay_model'),
            'required_exit_replay_model': row.get('required_exit_replay_model'),
            'score_cache': row.get('score_cache') or {},
        })
    return out


def _route_key(variant: Any) -> str:
    payload = routed.variant_to_dict(variant) if routed.is_routed_variant(variant) else {
        'name': getattr(variant, 'name', ''),
        'weights': dict(getattr(variant, 'weights', {}) or {}),
        'bias': float(getattr(variant, 'bias', 0.0) or 0.0),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()[:24]


def _route_bucket_key(variant: Any) -> str:
    payload = routed.variant_to_dict(variant) if routed.is_routed_variant(variant) else {}
    routes = payload.get('routes') if isinstance(payload.get('routes'), list) else []
    for route in routes:
        if not isinstance(route, dict):
            continue
        match = route.get('match') if isinstance(route.get('match'), dict) else {}
        ticker = str(match.get('ticker') or '*')
        setup = str(match.get('setup_type') or '*')
        phase = str(match.get('session_phase') or '*')
        if ticker != '*' or setup != '*' or phase != '*':
            return '|'.join([ticker, setup, phase])
    return 'unrouted'


def _dedupe_rows(rows: list[dict]) -> list[dict]:
    seen = set()
    out = []
    for row in sorted(rows, key=lambda item: float(item.get('step2_pnl') or -1e18), reverse=True):
        key = json.dumps({
            'weights': row.get('weights') or {},
            'bias': float(row.get('bias') or 0.0),
            'routes': row.get('routes') or [],
        }, sort_keys=True, separators=(',', ':'))
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _parse_route_bucket(raw: str) -> dict:
    parts = str(raw or '').split('|')
    match = {}
    if len(parts) >= 1 and parts[0] and parts[0] != '*':
        match['ticker'] = parts[0]
    if len(parts) >= 2 and parts[1] and parts[1] != '*':
        match['setup_type'] = parts[1]
    if len(parts) >= 3 and parts[2] and parts[2] != '*':
        match['session_phase'] = parts[2]
    return match


def _focus_seed(raw: str, source: str = 'manual_focus') -> dict:
    return {
        'name': str(raw or '').strip(),
        'match': _parse_route_bucket(str(raw or '').strip()),
        'source': {'focus_route': str(raw or '').strip(), 'source': source},
        'focus': True,
        'sample_weight': 12.0,
    }


def _load_focus_routes(args: argparse.Namespace) -> list[str]:
    routes = [str(route).strip() for route in (args.focus_route or []) if str(route).strip()]
    if args.focus_plan_json:
        payload = _read_json(args.focus_plan_json)
        for job in payload.get('route_jobs') or []:
            focus = str(job.get('focus') or '').strip()
            if focus:
                routes.append(focus)
            for sibling in job.get('sibling_routes') or []:
                sibling = str(sibling or '').strip()
                if sibling:
                    routes.append(sibling)
    quarantine = _quarantine(args)
    adapter = _runtime_adapter_from_quarantine(quarantine)
    avoid = {str(route) for route in (adapter.get('avoid_routes') or []) if str(route)}
    for route in adapter.get('focus_routes') or []:
        route = str(route or '').strip()
        if route and route not in avoid:
            routes.append(route)
    for command in adapter.get('commands') or []:
        if not isinstance(command, dict):
            continue
        route = str(command.get('route_key') or '').strip()
        if route and route not in avoid and command.get('action') != 'quarantine':
            routes.append(route)
    out = []
    seen = set()
    for route in routes:
        if route in seen:
            continue
        seen.add(route)
        out.append(route)
    return out


def _route_seeds(gap_report: dict, limit: int, focus_routes: list[str] | None = None) -> list[dict]:
    seeds = []
    for route in focus_routes or []:
        seed = _focus_seed(route)
        if seed.get('match'):
            seeds.append(seed)
    for row in gap_report.get('route_seed_buckets') or []:
        if float(row.get('gap_pct_sum') or 0.0) <= 0.0:
            continue
        seeds.append({
            'name': str(row.get('bucket') or ''),
            'match': _parse_route_bucket(str(row.get('bucket') or '')),
            'source': row,
        })
    for row in gap_report.get('by_ticker') or []:
        seeds.append({
            'name': f"ticker_{row.get('bucket')}",
            'match': {'ticker': row.get('bucket')},
            'source': row,
        })
    for row in gap_report.get('by_setup') or []:
        seeds.append({
            'name': f"setup_{row.get('bucket')}",
            'match': {'setup_type': row.get('bucket')},
            'source': row,
        })
    clean = []
    seen = set()
    for seed in seeds:
        match = {k: v for k, v in (seed.get('match') or {}).items() if v not in (None, '')}
        if not match:
            continue
        key = json.dumps(match, sort_keys=True)
        if key in seen and not seed.get('focus'):
            continue
        seen.add(key)
        seed['match'] = match
        clean.append(seed)
    return clean[:limit]


def _mutated_weights(base: dict[str, float], rng: random.Random, scale: float,
                     weight_limit: float, anchor_features: list[str] | None = None) -> dict[str, float]:
    weights = dict(base)
    pool = list(dict.fromkeys((anchor_features or []) + CORE_ROUTE_FEATURES + rng.sample(FEATURES, min(8, len(FEATURES)))))
    for feature in pool:
        cur = float(weights.get(feature, 0.0))
        if rng.random() < 0.70:
            cur += rng.gauss(0.0, scale)
        if rng.random() < 0.12:
            cur *= rng.choice([-1.0, 0.0, 1.5])
        cur = max(-float(weight_limit), min(float(weight_limit), cur))
        if abs(cur) > 1e-9:
            weights[feature] = round(cur, 6)
        elif feature in weights:
            del weights[feature]
    return weights


def _route_variant(active: lab.Variant, seed: dict, rng: random.Random, idx: int,
                   scale: float, weight_limit: float) -> routed.RoutedVariant:
    source = seed.get('source') or {}
    active_losing = int(source.get('active_losing_rows') or 0)
    wrong_side = int(source.get('wrong_side_rows') or 0)
    rows = max(1, int(source.get('rows') or 1))
    skip_bias = active_losing / rows
    match = dict(seed.get('match') or {})
    lane = str(seed.get('promotion_repair_lane') or '')
    target_failures = {str(item) for item in (seed.get('quality_target_failures') or []) if str(item)}
    quality_protection = bool(seed.get('quality_protection')) or lane in {'robustness_holdout_repair', 'robustness_floor_repair', 'holdout_grade_repair', 'promotion_ready_finish'}
    breadth_repair = lane == 'route_breadth_repair' or 'route_breadth' in target_failures
    readiness_repair = bool(seed.get('readiness_first_repair')) or lane in {'promotion_readiness_repair', 'promotion_ready_finish'} or 'promotion_readiness_below_floor' in target_failures
    robustness_repair = bool(seed.get('robustness_holdout_focus')) or lane in {'robustness_holdout_repair', 'robustness_floor_repair', 'holdout_grade_repair'} or bool({'robustness_below_floor', 'holdout_positive_but_not_promotion_grade', 'missing_holdout_credit', 'promotion_grade_holdout_credit'} & target_failures)
    finish_mode_repair = bool(seed.get('finish_mode_repair')) or lane == 'promotion_ready_finish'
    robustness_floor_focus = bool(seed.get('robustness_floor_focus')) or lane == 'robustness_floor_repair'
    holdout_grade_focus = bool(seed.get('holdout_grade_focus')) or lane == 'holdout_grade_repair'
    edge_preservation = bool(seed.get('edge_preservation_repair')) or lane == 'edge_preservation_repair'
    close_sibling_repair = bool(seed.get('close_sibling_repair')) or lane == 'close_sibling_revalidation'
    force_structural = bool(seed.get('force_structural_mutation')) or lane in {'structural_mutation', 'anti_alias_structural_mutation'} or breadth_repair
    if force_structural:
        if match.get('session_phase') and not edge_preservation and rng.random() < (0.55 if breadth_repair and close_sibling_repair else 0.65 if breadth_repair else 0.45):
            match.pop('session_phase', None)
        if match.get('ticker') and not edge_preservation and rng.random() < (0.45 if breadth_repair and close_sibling_repair else 0.35 if breadth_repair else 0.25):
            match.pop('ticker', None)
        if match.get('setup_type') and not (edge_preservation or close_sibling_repair or breadth_repair) and rng.random() < 0.10:
            match.pop('setup_type', None)
    route_items = []
    if skip_bias >= 0.35 and rng.random() < 0.45:
        route_items.append(routed.route(
            f"skip_{idx}_{seed.get('name')}",
            match,
            action='skip',
        ))
    else:
        anchor = []
        phase = str(match.get('session_phase') or '')
        if phase == 'open':
            anchor.extend(['open_phase', 'brs_open_low_range_040', 'brs_open_low_range_045'])
        if match.get('setup_type') == 'btc_relative_strength':
            anchor.extend(['setup_btc_relative_strength', 'relative', 'btc'])
        structural_scale = scale * (0.68 if quality_protection else 1.25 if force_structural else 1.0)
        if force_structural and not quality_protection:
            anchor.extend(rng.sample(FEATURES, min(4, len(FEATURES))))
        if quality_protection:
            anchor.extend(['vwap', 'ema', 'rsi', 'relative_strength', 'volume'])
        if edge_preservation:
            anchor.extend(['vwap', 'ema', 'rsi', 'relative_strength', 'volume', 'exec_penalty', 'flow_contra'])
            structural_scale *= 0.55
        elif close_sibling_repair:
            anchor.extend(['vwap', 'ema', 'momentum', 'relative_strength', 'volume'])
            structural_scale *= 0.65
        if readiness_repair:
            anchor.extend(['vwap', 'ema', 'rsi', 'momentum', 'relative', 'btc', 'exec_penalty', 'volume', 'flow_contra'])
            structural_scale *= 0.78 if finish_mode_repair else 0.92
        if robustness_repair:
            anchor.extend(['vwap', 'ema', 'rsi', 'relative_strength', 'volume', 'exec_penalty', 'flow_contra', 'btc_chop'])
            structural_scale *= 0.72
        if robustness_floor_focus:
            anchor.extend(['rsi', 'ema', 'volume', 'exec_penalty', 'flow_contra', 'btc_chop'])
            structural_scale *= 0.82
        if holdout_grade_focus:
            anchor.extend(['vwap', 'relative_strength', 'volume', 'flow_contra', 'btc'])
            structural_scale *= 0.86
        if breadth_repair:
            anchor.extend(['vwap', 'ema', 'momentum', 'btc', 'relative'])
            if not edge_preservation:
                structural_scale *= 0.82
        weights = _mutated_weights(dict(active.weights), rng, structural_scale, weight_limit, anchor)
        bias_scale = structural_scale / (6.0 if readiness_repair or quality_protection else 4.0)
        bias = float(active.bias or 0.0) + rng.gauss(0.0, bias_scale)
        route_items.append(routed.route(
            f"score_{idx}_{seed.get('name')}",
            match,
            weights,
            bias=round(bias, 6),
            action='score',
        ))
    if wrong_side / rows >= 0.50 and rng.random() < 0.20:
        route_items.append(routed.route(
            f"force_flip_{idx}_{seed.get('name')}",
            match,
            action=rng.choice(['force_long', 'force_short']),
        ))
    name = f"router_{idx:09d}_{str(seed.get('name') or 'route')[:48]}"
    return routed.routed_variant(name, dict(active.weights), float(active.bias or 0.0), route_items)


def _lineage_for_route_variant(seed: dict, variant: routed.RoutedVariant, batch_idx: int, scale: float) -> dict:
    route_seed = _route_key_from_seed(seed)
    lane = str(seed.get('promotion_repair_lane') or ('route_focus' if seed.get('focus') else 'route_gap_mutation'))
    return {
        'parent_variant': str(seed.get('parent_variant') or 'active_live_fallback'),
        'parent_behavior_key': str(seed.get('parent_behavior_key') or ''),
        'mutation_lane': lane,
        'mutation_reason': str(seed.get('promotion_repair_lane') or seed.get('source', {}).get('source') or 'oracle_gap_route_mutation'),
        'quality_protection': bool(seed.get('quality_protection')),
        'quality_target_failures': list(seed.get('quality_target_failures') or []),
        'edge_preservation_repair': bool(seed.get('edge_preservation_repair')),
        'close_sibling_repair': bool(seed.get('close_sibling_repair')),
        'readiness_first_repair': bool(seed.get('readiness_first_repair')),
        'robustness_holdout_focus': bool(seed.get('robustness_holdout_focus')),
        'robustness_floor_focus': bool(seed.get('robustness_floor_focus')),
        'holdout_grade_focus': bool(seed.get('holdout_grade_focus')),
        'finish_mode_repair': bool(seed.get('finish_mode_repair')),
        'secondary_repair_lanes': list(seed.get('secondary_repair_lanes') or []),
        'outside_promotion_ready_envelope': bool(seed.get('outside_promotion_ready_envelope')),
        'worker_role': 'router',
        'generation': int(seed.get('generation') or 1),
        'route_seed': route_seed,
        'mutation_scale': round(float(scale), 6),
        'ancestor_path': [str(seed.get('parent_variant') or 'active_live_fallback')],
        'cycle': int(batch_idx),
        'route_match': dict(seed.get('match') or {}),
        'variant_name': variant.name,
    }


def _candidates(active: lab.Variant, seeds: list[dict], rng: random.Random, batch_idx: int,
                batch_size: int, scale: float, weight_limit: float,
                online_state: dict | None = None) -> tuple[list[routed.RoutedVariant], dict[str, dict]]:
    variants = []
    lineage_by_name: dict[str, dict] = {}
    seen = set()
    online_state = online_state or {}
    adapter = online_state.get('runtime_command_adapter') if isinstance(online_state.get('runtime_command_adapter'), dict) else {}
    route_counts: Counter = Counter()
    max_share_by_route = adapter.get('max_sample_share_by_route') if isinstance(adapter.get('max_sample_share_by_route'), dict) else {}
    max_share_pct_by_route = adapter.get('max_sample_share_pct_by_route') if isinstance(adapter.get('max_sample_share_pct_by_route'), dict) else {}
    max_winner_share_pct_by_route = adapter.get('max_winner_share_pct_by_route') if isinstance(adapter.get('max_winner_share_pct_by_route'), dict) else {}
    default_share = float(adapter.get('default_max_sample_share_pct') or (28.0 if str(adapter.get('clone_prevention_mode') or '') in {'high', 'hard'} else 0.0) or 0.0)

    def cap_for(seed: dict) -> int:
        route = _route_key_from_seed(seed)
        raw = _route_value(max_share_pct_by_route, route, _route_value(max_share_by_route, route, default_share))
        winner_raw = _route_value(max_winner_share_pct_by_route, route, None)
        if winner_raw not in (None, '', 0, 0.0):
            raw = min(float(raw or winner_raw), float(winner_raw))
        if raw in (None, '', 0, 0.0):
            return int(batch_size)
        value = float(raw)
        share = value / 100.0 if value > 1.0 else value
        if share <= 0.0:
            return int(batch_size)
        return max(1, int(round(batch_size * min(1.0, share))))

    base_weights = [_seed_weight(seed, online_state) for seed in seeds]
    while len(variants) < batch_size:
        available_pairs = [
            (seed, weight)
            for seed, weight in zip(seeds, base_weights)
            if route_counts[_route_key_from_seed(seed)] < cap_for(seed)
        ]
        if not available_pairs:
            available_pairs = list(zip(seeds, base_weights))
        available_seeds = [seed for seed, _weight in available_pairs]
        available_weights = [weight for _seed, weight in available_pairs]
        seed = rng.choices(available_seeds, weights=available_weights, k=1)[0]
        variant = _route_variant(
            active,
            seed,
            rng,
            batch_idx * 1_000_000 + len(variants),
            scale,
            weight_limit,
        )
        key = _route_key(variant)
        if key in seen:
            continue
        seen.add(key)
        route_counts[_route_key_from_seed(seed)] += 1
        lineage_by_name[variant.name] = _lineage_for_route_variant(seed, variant, batch_idx, scale)
        variants.append(variant)
    return variants, lineage_by_name


def _decorate_rows(rows: list[dict], active_pnl: float, target_pnl: float,
                   milestone_floor: float) -> None:
    for row in rows:
        pnl = float(row.get('step2_pnl') or 0.0)
        row['step2_delta_vs_active'] = round(pnl - active_pnl, 4)
        row['step2_delta_pct_vs_active'] = round((pnl - active_pnl) / abs(active_pnl) * 100.0, 4) if active_pnl else None
        row['beats_target_pnl'] = pnl >= target_pnl
        row['beats_current_milestone_floor'] = pnl >= milestone_floor
        row['routed_profile_safety'] = routed_profile_safety.evaluate_candidate(row)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Route-aware direct Step 2 hunter.')
    ap.add_argument('--compiled-decision-tape', default='',
                    help='Optional explicit compiled manifest. Defaults to the best certified current Step 2 cache.')
    ap.add_argument('--no-cache-resolver', dest='cache_resolver', action='store_false',
                    help='Diagnostics only: require/use --compiled-decision-tape exactly as provided.')
    ap.set_defaults(cache_resolver=True)
    ap.add_argument('--cache-start', default=step2_manifest_resolver.DEFAULT_START)
    ap.add_argument('--cache-end', default=step2_manifest_resolver.DEFAULT_END)
    ap.add_argument('--cache-tickers', nargs='*', default=step2_manifest_resolver.DEFAULT_TICKERS)
    ap.add_argument('--gap-report', default='',
                    help='Optional oracle-gap report. If omitted, one is built in memory.')
    ap.add_argument('--out-dir', default=str(DEFAULT_OUT))
    ap.add_argument('--name', default='router_step2_hunt')
    ap.add_argument('--batch-size', type=int, default=300)
    ap.add_argument('--max-batches', type=int, default=200)
    ap.add_argument('--seed', type=int, default=20260509)
    ap.add_argument('--route-seed-limit', type=int, default=120)
    ap.add_argument('--focus-route', action='append', default=[],
                    help='Prioritize a route key like CLSK|trend_pullback|late during candidate generation.')
    ap.add_argument('--focus-plan-json', default='',
                    help='Optional next_hunt_plan JSON with route_jobs to prioritize.')
    ap.add_argument('--gap-top-rows', type=int, default=100)
    ap.add_argument('--start-balance', type=float, default=100000.0)
    ap.add_argument('--target-pnl', type=float, default=100000.0)
    ap.add_argument('--beat-pct', type=float, default=5.0,
                    help='Milestone chain threshold. A new milestone is recorded only after beating the current leader by this percent.')
    ap.add_argument('--weight-limit', type=float, default=4.0)
    ap.add_argument('--mutation-scale-multiplier', type=float, default=1.0,
                    help='Online-learning control: multiply route mutation scale by this value.')
    ap.add_argument('--online-state-json', default='',
                    help='Optional online_state.json to consume between batches.')
    ap.add_argument('--stream-telemetry-jsonl', default='',
                    help='Optional JSONL path for streaming batch telemetry.')
    ap.add_argument('--stop-signal-json', default='',
                    help='Optional stop signal JSON; if {\"stop\": true}, hunter exits after current batch.')
    ap.add_argument('--quarantine-json', default='',
                    help='Optional quarantine JSON with skip_routes.')
    ap.add_argument('--min-batch-size', type=int, default=0)
    ap.add_argument('--max-batch-size', type=int, default=0)
    ap.add_argument('--exact-variant-count', action='store_true',
                    help='Score exactly --batch-size variants per batch, ignoring online batch multipliers and runtime throttles.')
    ap.add_argument('--runtime-control-mode', choices=['enforce', 'observe'], default='enforce',
                    help='Use observe to record runtime controls without applying route skips, throttles, or degrade mode.')
    ap.add_argument('--score-cache-db', default=str(step2_score_cache.DEFAULT_CACHE_DB))
    ap.add_argument('--no-score-cache', dest='use_score_cache', action='store_false')
    ap.set_defaults(use_score_cache=True)
    ap.add_argument('--max-trades-per-day', type=int, default=0)
    ap.add_argument('--max-trades-per-ticker-day', type=int, default=0)
    ap.add_argument('--allow-uncertified-cache', action='store_true',
                    help='Diagnostics only: skip compiled lineage validation.')
    ap.add_argument('--skip-best-trace', action='store_true',
                    help='Skip final per-route trace attribution for the best routed candidate.')
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    rng = random.Random(args.seed)
    out_dir = Path(args.out_dir) / args.name
    checkpoint = out_dir / 'checkpoint.json'
    rounds_path = out_dir / 'rounds.jsonl'
    scored_manifest_path = out_dir / 'scored_variants.jsonl'
    out_dir.mkdir(parents=True, exist_ok=True)

    cache_resolution = {'ok': True, 'source': 'cache_resolver_disabled', 'manifest_path': args.compiled_decision_tape}
    if args.cache_resolver:
        payload = step2_manifest_resolver.resolve_best_manifest(
            compiled_manifest=args.compiled_decision_tape or None,
            start=args.cache_start,
            end=args.cache_end,
            tickers=args.cache_tickers,
            require_certified=not args.allow_uncertified_cache,
            write_certification=True,
            write_receipt=True,
            label=args.name,
        )
        cache_resolution = step2_manifest_resolver.compact_resolution(payload)
        step2_manifest_resolver.write_json(out_dir / 'cache_manifest_resolution.json', payload)
        if not payload.get('ok') and not args.allow_uncertified_cache:
            summary = {
                'schema_version': 1,
                'script': 'step2_router_hunter.py',
                'completed': False,
                'promotable': False,
                'non_promotable_reason': 'step2_cache_resolution_failed',
                'compiled_decision_tape': args.compiled_decision_tape,
                'cache_manifest_resolution': cache_resolution,
            }
            _write_json(out_dir / 'summary.json', summary)
            print(json.dumps(summary, indent=2, sort_keys=True), flush=True)
            return 4
        if payload.get('manifest_path'):
            args.compiled_decision_tape = str(payload['manifest_path'])
    elif not args.compiled_decision_tape:
        raise SystemExit('--compiled-decision-tape is required when --no-cache-resolver is used')

    compiled = decision_tape_compiled.load_compiled(
        args.compiled_decision_tape,
        mmap=True,
        validate_sources=not args.allow_uncertified_cache,
    )
    quote_aware_gate = step2_quote_aware_guard.manifest_gate(compiled.get('manifest') if isinstance(compiled.get('manifest'), dict) else {})
    if not quote_aware_gate.get('ok'):
        raise RuntimeError(f"compiled tape rejected by quote-aware gate: {quote_aware_gate}")
    gap_report = _load_or_build_gap_report(compiled, args)
    focus_routes = _load_focus_routes(args)
    seeds = _route_seeds(gap_report, int(args.route_seed_limit), focus_routes)
    if not seeds:
        raise RuntimeError('oracle gap report did not produce any route seeds')

    data_integrity = _data_integrity_preflight(compiled)
    active = _active_seed()
    active_row = _score(compiled, [active], args)[0]
    evaluation_envelope = step2_evaluation_envelope.build(
        compiled=compiled,
        compiled_manifest_path=args.compiled_decision_tape,
        args=args,
        active_row=active_row,
        data_integrity=data_integrity,
        sim_config=_sim_config(args),
        run_context={'mode': 'router_step2_hunt', 'cache_manifest_resolution': cache_resolution},
        cache_certification=cache_resolution,
        name=args.name,
    )
    active_pnl = float(active_row['step2_pnl'])
    beat_floor = active_pnl + abs(active_pnl) * float(args.beat_pct) / 100.0
    target_pnl = max(float(args.target_pnl), beat_floor)
    milestone_floor = beat_floor
    milestones = []
    leaderboard: list[dict] = []
    winners: list[dict] = []
    started = time.perf_counter()
    scored_total = 0

    with rounds_path.open('a', encoding='utf-8') as round_log:
        for batch_idx in range(1, int(args.max_batches) + 1):
            if _stop_requested(args):
                print(json.dumps({'event': 'stop_requested', 'batch': batch_idx}, sort_keys=True), flush=True)
                break
            online_state = _online_controls(args)
            quarantine = _quarantine(args)
            observed_runtime_adapter = _runtime_adapter_from_quarantine(quarantine)
            enforce_runtime_controls = str(args.runtime_control_mode or 'enforce') == 'enforce'
            runtime_adapter = _runtime_adapter_for_execution(
                observed_runtime_adapter,
                enforce_runtime_controls=enforce_runtime_controls,
                exact_variant_count=bool(args.exact_variant_count),
            )
            execution_state = dict(online_state or {})
            execution_state['runtime_command_adapter'] = runtime_adapter
            active_seeds = _filter_quarantined_seeds(
                seeds,
                quarantine,
                enforce_runtime_controls=enforce_runtime_controls,
            )
            if not active_seeds:
                print(json.dumps({'event': 'all_route_seeds_filtered_by_runtime_controls', 'batch': batch_idx}, sort_keys=True), flush=True)
                break
            online_scale = float(args.mutation_scale_multiplier)
            width = str(runtime_adapter.get('mutation_width') or 'medium')
            online_scale *= {'tight': 0.72, 'medium': 1.0, 'wide': 1.28}.get(width, 1.0)
            if runtime_adapter.get('degrade_mode'):
                online_scale = min(online_scale, 0.72)
            controls = online_state.get('mutation_controls') if isinstance(online_state.get('mutation_controls'), dict) else {}
            focus_route = _route_key_from_seed(active_seeds[0]) if active_seeds else ''
            if focus_route and focus_route in controls:
                online_scale *= float((controls.get(focus_route) or {}).get('scale') or 1.0)
            batch_multiplier = 1.0 if bool(args.exact_variant_count) else float((online_state.get('batch_size_multiplier') or 1.0))
            if runtime_adapter.get('batch_size_multiplier'):
                batch_multiplier *= float(runtime_adapter.get('batch_size_multiplier') or 1.0)
            if runtime_adapter.get('degrade_mode'):
                batch_multiplier = min(batch_multiplier, 0.65)
            current_batch_size = int(args.batch_size)
            if bool(args.exact_variant_count):
                current_batch_size = int(args.batch_size)
            elif batch_multiplier and batch_multiplier > 0:
                current_batch_size = max(1, int(round(current_batch_size * batch_multiplier)))
            if not bool(args.exact_variant_count) and int(args.min_batch_size or 0) > 0:
                current_batch_size = max(int(args.min_batch_size), current_batch_size)
            if not bool(args.exact_variant_count) and int(args.max_batch_size or 0) > 0:
                current_batch_size = min(int(args.max_batch_size), current_batch_size)
            scale = max(0.05, 0.75 * (0.988 ** batch_idx) * online_scale)
            variants, lineage_by_name = _candidates(
                active,
                active_seeds,
                rng,
                batch_idx,
                current_batch_size,
                scale,
                float(args.weight_limit),
                execution_state,
            )
            sampled_route_distribution = Counter()
            for variant in variants:
                try:
                    sampled_route_distribution[_route_bucket_key(variant)] += 1
                except Exception:
                    sampled_route_distribution['unknown'] += 1
            route_cap_application_count = sum(1 for seed in active_seeds if seed.get('route_budget_cap'))
            route_cap_multiplier_by_route = {
                _route_key_from_seed(seed): seed.get('budget_cap_multiplier')
                for seed in active_seeds
                if seed.get('budget_cap_multiplier') is not None
            }
            scored = _score(compiled, variants, args, lineage_by_name=lineage_by_name)
            scored_total += len(scored)
            _decorate_rows(scored, active_pnl, target_pnl, milestone_floor)
            scored.sort(key=lambda row: float(row.get('step2_pnl') or -1e18), reverse=True)
            _append_scored_manifest(scored_manifest_path, scored, batch_idx=batch_idx)
            leaderboard = _dedupe_rows(leaderboard + scored)[:500]
            winners = _dedupe_rows(winners + [row for row in scored if row['step2_pnl'] >= target_pnl])[:100]

            best = leaderboard[0]
            if float(best['step2_pnl']) >= milestone_floor:
                milestone = {
                    'batch': batch_idx,
                    'scored_total': scored_total,
                    'floor_pnl': round(milestone_floor, 2),
                    'variant': best['variant'],
                    'step2_pnl': best['step2_pnl'],
                    'step2_delta_pct_vs_active': best.get('step2_delta_pct_vs_active'),
                    'routes': best.get('routes') or [],
                }
                milestones.append(milestone)
                milestone_floor = float(best['step2_pnl']) + abs(float(best['step2_pnl'])) * float(args.beat_pct) / 100.0
                print(json.dumps({'event': 'milestone', **milestone}, sort_keys=True), flush=True)

            promotion_surface_ok = bool(data_integrity.get('promotion_safe') and cache_resolution.get('ok'))
            payload = {
                'schema_version': 1,
                'script': 'step2_router_hunter.py',
                'compiled_decision_tape': args.compiled_decision_tape,
                'exit_replay_model': step2_quote_aware_guard.manifest_exit_replay_model(compiled.get('manifest') if isinstance(compiled.get('manifest'), dict) else {}),
                'required_exit_replay_model': step2_quote_aware_guard.REQUIRED_EXIT_REPLAY_MODEL,
                'cache_manifest_resolution': cache_resolution,
                'gap_report': args.gap_report or 'built_in_memory',
                'start_balance': args.start_balance,
                'market_data_integrity': data_integrity,
                'active': active_row,
                'active_step2_pnl': active_pnl,
                'target_pnl': target_pnl,
                'target_formula': 'max(requested_target_pnl, active_pnl + abs(active_pnl) * beat_pct / 100)',
                'requested_target_pnl': float(args.target_pnl),
                'beat_pct': float(args.beat_pct),
                'completed_batches': batch_idx,
                'scored_total': scored_total,
                'route_seed_count': len(seeds),
                'focus_routes': focus_routes,
                'runtime_command_adapter': runtime_adapter,
                'observed_runtime_command_adapter': observed_runtime_adapter,
                'runtime_control_mode': str(args.runtime_control_mode or 'enforce'),
                'exact_variant_count': bool(args.exact_variant_count),
                'sampled_route_distribution': dict(sampled_route_distribution.most_common(25)),
                'sampled_route_distribution_full': dict(sampled_route_distribution.most_common()),
                'route_cap_application_count': route_cap_application_count,
                'route_cap_multiplier_by_route': route_cap_multiplier_by_route,
                'requested_batch_size': int(args.batch_size),
                'effective_batch_size': int(current_batch_size),
                'scored_manifest_path': str(scored_manifest_path.resolve()),
                'scored_manifest_rows': scored_total,
                'elapsed_sec': round(time.perf_counter() - started, 3),
                'milestone_floor': round(milestone_floor, 2),
                'milestones': milestones,
                'winners': winners[:50],
                'leaderboard': leaderboard[:50],
                'target_reached': bool(winners),
                'completed': bool(winners or batch_idx >= int(args.max_batches)),
                'stop_reason': 'target_pnl_reached' if winners else 'max_batches_exhausted',
                'promotion_surface_ok': promotion_surface_ok,
                'promotable': bool(promotion_surface_ok and winners),
                'non_promotable_reason': None if promotion_surface_ok and winners else ('no_profitable_candidate' if promotion_surface_ok else 'promotion_surface_failed'),
                'best': best,
                'notes': [
                    'Routed variants keep the active profile as fallback and only override matched slices.',
                    'Promote only after a fresh certified compiled cache and normal robustness review.',
                ],
            }
            _attach_evaluation_guardrails(payload, evaluation_envelope)
            candidate_profile_schema.decorate_payload(payload, context={
                'script': 'step2_router_hunter.py',
                'compiled_decision_tape': args.compiled_decision_tape,
                'exit_replay_model': step2_quote_aware_guard.manifest_exit_replay_model(compiled.get('manifest') if isinstance(compiled.get('manifest'), dict) else {}),
                'active_step2_pnl': active_pnl,
                'target_pnl': target_pnl,
                'scored_total': payload['scored_total'],
            })
            _write_json(checkpoint, payload)
            round_log.write(json.dumps({
                'batch': batch_idx,
                'elapsed_sec': payload['elapsed_sec'],
                'best_pnl': best['step2_pnl'],
                'winners': len(winners),
                'milestones': len(milestones),
                'batch_size': current_batch_size,
                'requested_batch_size': int(args.batch_size),
                'mutation_scale': scale,
                'sampled_route_distribution': dict(sampled_route_distribution.most_common(25)),
                'sampled_route_distribution_full': dict(sampled_route_distribution.most_common()),
                'route_cap_application_count': route_cap_application_count,
                'route_cap_multiplier_by_route': route_cap_multiplier_by_route,
            }, sort_keys=True) + '\n')
            round_log.flush()
            telemetry = {
                'event': 'router_batch_telemetry',
                'batch': batch_idx,
                'hunter': 'router',
                'batch_size': current_batch_size,
                'requested_batch_size': int(args.batch_size),
                'scored_total': payload['scored_total'],
                'best_pnl': best['step2_pnl'],
                'best_variant': best.get('variant') or best.get('name'),
                'best_routes': best.get('routes') or [],
                'best_delta_vs_active': best.get('step2_delta_vs_active'),
                'live_beaters': sum(1 for row in scored if float(row.get('step2_delta_vs_active') or 0.0) > 0.0),
                'winners': len(winners),
                'novelty_yield': len({json.dumps({'pnl': row.get('step2_pnl'), 'trades': row.get('step2_trades')}, sort_keys=True) for row in scored[:50]}),
                'alias_count': max(0, len(scored) - len(_dedupe_rows(scored))),
                'alias_rate': round(max(0, len(scored) - len(_dedupe_rows(scored))) / max(1, len(scored)), 4),
                'route_seeds': [_route_key_from_seed(seed) for seed in active_seeds[:10]],
                'mutation_scale': scale,
                'runtime_command_count': len(runtime_adapter.get('commands') or []),
                'runtime_degrade_mode': bool(runtime_adapter.get('degrade_mode')),
                'runtime_primary_action': ((runtime_adapter.get('commands') or [{}])[0] or {}).get('action'),
                'sampled_route_distribution': dict(sampled_route_distribution.most_common(25)),
                'sampled_route_distribution_full': dict(sampled_route_distribution.most_common()),
                'route_cap_application_count': route_cap_application_count,
                'route_cap_multiplier_by_route': route_cap_multiplier_by_route,
                'runtime_control_mode': str(args.runtime_control_mode or 'enforce'),
                'exact_variant_count': bool(args.exact_variant_count),
                'top_failure_reason': 'no_live_beaters' if not any(float(row.get('step2_delta_vs_active') or 0.0) > 0.0 for row in scored) else '',
            }
            _append_jsonl(args.stream_telemetry_jsonl, telemetry)
            print(json.dumps({
                'event': 'batch_done',
                'batch': batch_idx,
                'scored_total': payload['scored_total'],
                'best_pnl': best['step2_pnl'],
                'target_pnl': round(target_pnl, 2),
                'winners': len(winners),
                'milestones': len(milestones),
                'batch_size': current_batch_size,
                'requested_batch_size': int(args.batch_size),
                'mutation_scale': round(scale, 6),
                'runtime_degrade_mode': bool(runtime_adapter.get('degrade_mode')),
            }, sort_keys=True), flush=True)
            if winners:
                break

    final = _read_json(checkpoint)
    if not final:
        final = {
            'schema_version': 1,
            'script': 'step2_router_hunter.py',
            'compiled_decision_tape': args.compiled_decision_tape,
            'exit_replay_model': step2_quote_aware_guard.manifest_exit_replay_model(compiled.get('manifest') if isinstance(compiled.get('manifest'), dict) else {}),
            'required_exit_replay_model': step2_quote_aware_guard.REQUIRED_EXIT_REPLAY_MODEL,
            'cache_manifest_resolution': cache_resolution,
            'gap_report': args.gap_report or 'built_in_memory',
            'start_balance': args.start_balance,
            'market_data_integrity': data_integrity,
            'active': active_row,
            'active_step2_pnl': active_pnl,
            'target_pnl': target_pnl,
            'target_formula': 'max(requested_target_pnl, active_pnl + abs(active_pnl) * beat_pct / 100)',
            'requested_target_pnl': float(args.target_pnl),
            'beat_pct': float(args.beat_pct),
            'completed_batches': 0,
            'scored_total': 0,
            'route_seed_count': len(seeds),
            'focus_routes': focus_routes,
            'scored_manifest_path': str(scored_manifest_path.resolve()),
            'scored_manifest_rows': 0,
            'elapsed_sec': round(time.perf_counter() - started, 3),
            'milestone_floor': round(milestone_floor, 2),
            'milestones': [],
            'winners': [],
            'leaderboard': [],
            'completed': False,
            'promotable': False,
            'non_promotable_reason': 'stopped_before_first_batch',
            'best': None,
        }
        _attach_evaluation_guardrails(final, evaluation_envelope)
    if not args.skip_best_trace and isinstance(final.get('best'), dict) and (final.get('best') or {}).get('routes'):
        best = final['best']
        best_variant = routed.variant_from_dict({
            'name': best.get('variant') or best.get('name') or 'best_routed_candidate',
            'weights': best.get('weights') or {},
            'bias': float(best.get('bias') or 0.0),
            'routes': best.get('routes') or [],
        })
        trace = unified_decision_ledger.trace_step2_decisions(
            compiled,
            best_variant,
            float(args.start_balance),
            gate=None,
            sim_config=_sim_config(args),
            include_rejected=True,
        )
        audit = routed.route_audit(compiled, best_variant, trace_rows=trace.get('rows') or [])
        final['best']['route_audit'] = audit
        final['best']['routed_profile_safety'] = routed_profile_safety.evaluate_candidate(final['best'])
        final['best_route_trace_summary'] = trace.get('summary') or {}
        _attach_evaluation_guardrails(final, evaluation_envelope)
        candidate_profile_schema.decorate_payload(final, context={
            'script': 'step2_router_hunter.py',
            'compiled_decision_tape': args.compiled_decision_tape,
            'exit_replay_model': step2_quote_aware_guard.manifest_exit_replay_model(compiled.get('manifest') if isinstance(compiled.get('manifest'), dict) else {}),
            'active_step2_pnl': active_pnl,
            'target_pnl': target_pnl,
            'scored_total': final.get('scored_total'),
        })
    _write_json(out_dir / 'summary.json', final)
    print(json.dumps({
        'out': str(out_dir / 'summary.json'),
        'completed': bool(final.get('completed')),
        'best_pnl': (final.get('best') or {}).get('step2_pnl'),
        'winners': len(final.get('winners') or []),
        'milestones': len(final.get('milestones') or []),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
