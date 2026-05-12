"""Online learning state for long Step 2 hunt runs."""
from __future__ import annotations

import json
import hashlib
import os
import time
from pathlib import Path
from typing import Any

import step2_hunt_intelligence as hunt_intel


def _now_epoch() -> float:
    return time.time()


def read_state(path: str | Path) -> dict[str, Any]:
    payload = hunt_intel.read_json(path, {}) or {}
    return payload if isinstance(payload, dict) else {}


def append_event(run_dir: str | Path, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    run_dir = Path(run_dir)
    event = {
        "ts": _now_epoch(),
        "event_type": event_type,
        **payload,
    }
    event.setdefault("event_id", _stable_hash({k: v for k, v in event.items() if k != "ts"}, length=24))
    path = run_dir / "learning_events.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, sort_keys=True, separators=(",", ":"), default=str) + "\n")
    return event


def append_jsonl(path: str | Path, payload: dict[str, Any]) -> dict[str, Any]:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    row = {"ts": _now_epoch(), **payload}
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, sort_keys=True, separators=(",", ":"), default=str) + "\n")
    return row


def _stable_hash(payload: Any, *, length: int = 20) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()[:length]


def _ordered_unique(values: list[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        item = str(value or "").strip()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def runtime_command_packet(state: dict[str, Any]) -> dict[str, Any]:
    guardrails = state.get("runtime_guardrails") if isinstance(state.get("runtime_guardrails"), dict) else {}
    kernel = state.get("runtime_decision_kernel") if isinstance(state.get("runtime_decision_kernel"), dict) else {}
    packet = guardrails.get("guarded_command_packet") if isinstance(guardrails.get("guarded_command_packet"), dict) else {}
    if not packet:
        packet = kernel.get("command_packet") if isinstance(kernel.get("command_packet"), dict) else {}
    packet = dict(packet or {})
    packet["live_only"] = True
    packet["max_workers"] = min(4, max(1, int(packet.get("max_workers") or 4)))
    avoid = _ordered_unique(list(packet.get("avoid_routes") or []) + list((state.get("negative_knowledge_bank") or {}).get("avoid_routes") or []))
    packet["avoid_routes"] = avoid
    packet["focus_routes"] = [route for route in _ordered_unique(list(packet.get("focus_routes") or [])) if route not in set(avoid)][:12]
    packet["commands"] = [
        dict(command)
        for command in (packet.get("commands") or [])
        if isinstance(command, dict) and (not command.get("route_key") or str(command.get("route_key")) not in set(avoid) or command.get("action") in {"repair", "quarantine"})
    ][:20]
    packet["mutation_width"] = str(packet.get("mutation_width") or "medium")
    packet["route_breadth"] = str(packet.get("route_breadth") or "balanced")
    packet["batch_size_multiplier"] = float(packet.get("batch_size_multiplier") or 1.0)
    return packet


def degraded_runtime_command_packet(state: dict[str, Any]) -> dict[str, Any]:
    packet = runtime_command_packet(state)
    guardrails = state.get("runtime_guardrails") if isinstance(state.get("runtime_guardrails"), dict) else {}
    if guardrails.get("ok", True) is not False:
        return {}
    allowed = {"repair", "revalidate", "quarantine"}
    commands = []
    for command in packet.get("commands") or []:
        command = dict(command)
        if command.get("action") not in allowed:
            command["original_action"] = command.get("action")
            command["action"] = "revalidate"
            command["reason"] = f"guardrail_degrade_mode; {command.get('reason') or ''}".strip("; ")
        command["mutation_width"] = "tight"
        command["batch_size_multiplier"] = min(0.65, float(command.get("batch_size_multiplier") or 1.0))
        commands.append(command)
    if not commands:
        for route in packet.get("focus_routes") or []:
            commands.append({
                "command_id": _stable_hash({"degraded_revalidate": route}),
                "action": "revalidate",
                "route_key": route,
                "mutation_width": "tight",
                "batch_size_multiplier": 0.65,
                "reason": "guardrail_degrade_mode",
            })
    degraded = dict(packet)
    degraded.update({
        "degraded": True,
        "degrade_reason": ",".join(guardrails.get("failures") or ["guardrail_failure"]),
        "mutation_width": "tight",
        "route_breadth": "narrow_revalidation",
        "batch_size_multiplier": min(0.65, float(packet.get("batch_size_multiplier") or 1.0)),
        "commands": commands[:20],
    })
    return degraded


def active_runtime_command_packet(state: dict[str, Any]) -> dict[str, Any]:
    return degraded_runtime_command_packet(state) or runtime_command_packet(state)


def runtime_command_adapter(state: dict[str, Any]) -> dict[str, Any]:
    packet = active_runtime_command_packet(state)
    commands = [dict(command) for command in packet.get("commands") or [] if isinstance(command, dict)]
    ab_executor = state.get("ab_route_experiment_executor") if isinstance(state.get("ab_route_experiment_executor"), dict) else {}
    champion_slots = state.get("champion_challenger_runtime_slots") if isinstance(state.get("champion_challenger_runtime_slots"), dict) else {}
    contamination = state.get("experiment_contamination_guard") if isinstance(state.get("experiment_contamination_guard"), dict) else {}
    learning_rate = state.get("learning_rate_controller") if isinstance(state.get("learning_rate_controller"), dict) else {}
    recovery = state.get("recovery_playbook_generator") if isinstance(state.get("recovery_playbook_generator"), dict) else {}
    scarcity = state.get("live_beater_scarcity_mode") if isinstance(state.get("live_beater_scarcity_mode"), dict) else {}
    alias_traps = state.get("alias_trap_detector") if isinstance(state.get("alias_trap_detector"), dict) else {}
    seed_quality = state.get("route_seed_quality_score") if isinstance(state.get("route_seed_quality_score"), dict) else {}
    opening_pack = state.get("cold_start_route_pack_generator") if isinstance(state.get("cold_start_route_pack_generator"), dict) else {}
    opening_playbook = state.get("hunt_opening_playbook") if isinstance(state.get("hunt_opening_playbook"), dict) else {}
    memory_arbiter = state.get("memory_conflict_arbiter") if isinstance(state.get("memory_conflict_arbiter"), dict) else {}
    falsification = state.get("memory_falsification_queue") if isinstance(state.get("memory_falsification_queue"), dict) else {}
    retirement = state.get("belief_retirement_engine") if isinstance(state.get("belief_retirement_engine"), dict) else {}
    disagreement = state.get("current_vs_historical_disagreement_monitor") if isinstance(state.get("current_vs_historical_disagreement_monitor"), dict) else {}
    stress_pack = state.get("memory_stress_test_pack") if isinstance(state.get("memory_stress_test_pack"), dict) else {}
    hypothesis_market = state.get("hypothesis_market_maker") if isinstance(state.get("hypothesis_market_maker"), dict) else {}
    bet_sizer = state.get("real_time_bet_sizer") if isinstance(state.get("real_time_bet_sizer"), dict) else {}
    contrarian = state.get("contrarian_generator") if isinstance(state.get("contrarian_generator"), dict) else {}
    stop_loss = state.get("learning_stop_loss") if isinstance(state.get("learning_stop_loss"), dict) else {}
    breakthroughs = state.get("breakthrough_detector") if isinstance(state.get("breakthrough_detector"), dict) else {}
    recipes = state.get("pattern_to_recipe_compiler") if isinstance(state.get("pattern_to_recipe_compiler"), dict) else {}
    budget_reallocator = state.get("budget_reallocator") if isinstance(state.get("budget_reallocator"), dict) else {}
    time_plan = state.get("time_aware_hunt_plan") if isinstance(state.get("time_aware_hunt_plan"), dict) else {}
    creative_leaps = state.get("creative_leap_scorer") if isinstance(state.get("creative_leap_scorer"), dict) else {}
    novelty_governor = state.get("novelty_budget_governor") if isinstance(state.get("novelty_budget_governor"), dict) else {}
    creative_brief = state.get("creative_brief_compiler") if isinstance(state.get("creative_brief_compiler"), dict) else {}
    module_conflicts = state.get("module_conflict_detector") if isinstance(state.get("module_conflict_detector"), dict) else {}
    module_budget = state.get("module_budget_governor") if isinstance(state.get("module_budget_governor"), dict) else {}
    causal_scheduler = state.get("causal_intervention_scheduler") if isinstance(state.get("causal_intervention_scheduler"), dict) else {}
    temperature_controller = state.get("adaptive_search_temperature_controller") if isinstance(state.get("adaptive_search_temperature_controller"), dict) else {}
    route_interactions = state.get("route_interaction_learner") if isinstance(state.get("route_interaction_learner"), dict) else {}
    false_firewall = state.get("false_discovery_firewall") if isinstance(state.get("false_discovery_firewall"), dict) else {}
    strategy_v2 = state.get("hunt_strategy_compiler_v2") if isinstance(state.get("hunt_strategy_compiler_v2"), dict) else {}
    disagreement_court = state.get("learning_disagreement_court") if isinstance(state.get("learning_disagreement_court"), dict) else {}
    promotion_objective = state.get("promotion_aware_search_objective") if isinstance(state.get("promotion_aware_search_objective"), dict) else {}
    strategy_selector = state.get("regime_conditioned_strategy_selector") if isinstance(state.get("regime_conditioned_strategy_selector"), dict) else {}
    meta_objective = state.get("meta_objective_optimizer") if isinstance(state.get("meta_objective_optimizer"), dict) else {}
    exploration_debt = state.get("exploration_debt_ledger") if isinstance(state.get("exploration_debt_ledger"), dict) else {}
    strategy_red_team = state.get("adversarial_strategy_red_team") if isinstance(state.get("adversarial_strategy_red_team"), dict) else {}
    pivot_governor = state.get("autonomous_pivot_governor") if isinstance(state.get("autonomous_pivot_governor"), dict) else {}
    simplifier = state.get("runtime_control_simplifier") if isinstance(state.get("runtime_control_simplifier"), dict) else {}
    pruner = state.get("artifact_usefulness_pruner") if isinstance(state.get("artifact_usefulness_pruner"), dict) else {}
    fitness = state.get("architecture_fitness_brief") if isinstance(state.get("architecture_fitness_brief"), dict) else {}
    dashboard = state.get("live_learning_dashboard_feed") if isinstance(state.get("live_learning_dashboard_feed"), dict) else {}
    runbook = state.get("hunt_runbook_compiler") if isinstance(state.get("hunt_runbook_compiler"), dict) else {}
    sentinel = state.get("learning_failure_sentinel") if isinstance(state.get("learning_failure_sentinel"), dict) else {}
    readiness = state.get("pre_hunt_readiness_gate") if isinstance(state.get("pre_hunt_readiness_gate"), dict) else {}
    schema_registry = state.get("learning_artifact_schema_registry") if isinstance(state.get("learning_artifact_schema_registry"), dict) else {}
    causal_bandit = state.get("online_causal_bandit") if isinstance(state.get("online_causal_bandit"), dict) else {}
    dna_attribution = state.get("variant_dna_attribution") if isinstance(state.get("variant_dna_attribution"), dict) else {}
    gene_suppression = state.get("negative_gene_suppression") if isinstance(state.get("negative_gene_suppression"), dict) else {}
    family_tree = state.get("live_winner_family_tree") if isinstance(state.get("live_winner_family_tree"), dict) else {}
    frontier_map = state.get("exploration_frontier_map") if isinstance(state.get("exploration_frontier_map"), dict) else {}
    worker_personalities = state.get("adaptive_worker_personalities") if isinstance(state.get("adaptive_worker_personalities"), dict) else {}
    cycle_delta = state.get("cycle_level_learning_delta") if isinstance(state.get("cycle_level_learning_delta"), dict) else {}
    rejection_v2 = state.get("promotion_rejection_predictor_v2") if isinstance(state.get("promotion_rejection_predictor_v2"), dict) else {}
    counterfactual_sim = state.get("counterfactual_hunt_simulator") if isinstance(state.get("counterfactual_hunt_simulator"), dict) else {}
    missed_winners = state.get("missed_winner_detector") if isinstance(state.get("missed_winner_detector"), dict) else {}
    regret_ledger = state.get("causal_regret_ledger") if isinstance(state.get("causal_regret_ledger"), dict) else {}
    grammar_generator = state.get("adaptive_search_grammar_generator") if isinstance(state.get("adaptive_search_grammar_generator"), dict) else {}
    hypothesis_court = state.get("live_hypothesis_kill_scale_court") if isinstance(state.get("live_hypothesis_kill_scale_court"), dict) else {}
    shadow_scoring = state.get("promotion_survival_shadow_scoring") if isinstance(state.get("promotion_survival_shadow_scoring"), dict) else {}
    autopilot = state.get("hunt_autopilot_policy_compiler") if isinstance(state.get("hunt_autopilot_policy_compiler"), dict) else {}
    claim_verifier = state.get("learning_claim_verifier") if isinstance(state.get("learning_claim_verifier"), dict) else {}
    confidence_calibration = state.get("causal_confidence_calibration") if isinstance(state.get("causal_confidence_calibration"), dict) else {}
    false_warning = state.get("false_discovery_early_warning") if isinstance(state.get("false_discovery_early_warning"), dict) else {}
    debate_council = state.get("self_debate_search_council") if isinstance(state.get("self_debate_search_council"), dict) else {}
    drift_monitor = state.get("learning_drift_monitor") if isinstance(state.get("learning_drift_monitor"), dict) else {}
    promotion_autopilot = state.get("promotion_first_autopilot_v2") if isinstance(state.get("promotion_first_autopilot_v2"), dict) else {}
    horizon_memory = state.get("multi_horizon_memory_stack") if isinstance(state.get("multi_horizon_memory_stack"), dict) else {}
    half_life_v2 = state.get("lesson_half_life_engine_v2") if isinstance(state.get("lesson_half_life_engine_v2"), dict) else {}
    strategy_replay = state.get("cross_hunt_strategy_replay") if isinstance(state.get("cross_hunt_strategy_replay"), dict) else {}
    regime_fingerprint = state.get("temporal_regime_fingerprinting") if isinstance(state.get("temporal_regime_fingerprinting"), dict) else {}
    longitudinal_survival = state.get("longitudinal_promotion_survival_model") if isinstance(state.get("longitudinal_promotion_survival_model"), dict) else {}
    conflict_court_v2 = state.get("memory_conflict_court_v2") if isinstance(state.get("memory_conflict_court_v2"), dict) else {}
    strategy_aging = state.get("strategy_aging_dashboard") if isinstance(state.get("strategy_aging_dashboard"), dict) else {}
    next_hunt_opening = state.get("next_hunt_opening_policy_compiler") if isinstance(state.get("next_hunt_opening_policy_compiler"), dict) else {}
    question_planner = state.get("question_driven_hunt_planner") if isinstance(state.get("question_driven_hunt_planner"), dict) else {}
    eig_v2 = state.get("expected_information_gain_scorer_v2") if isinstance(state.get("expected_information_gain_scorer_v2"), dict) else {}
    uncertainty_map = state.get("uncertainty_heatmap") if isinstance(state.get("uncertainty_heatmap"), dict) else {}
    experiment_sequencer = state.get("adaptive_experiment_sequencer") if isinstance(state.get("adaptive_experiment_sequencer"), dict) else {}
    learning_value_stop = state.get("learning_value_stop_loss") if isinstance(state.get("learning_value_stop_loss"), dict) else {}
    causal_ledger = state.get("causal_question_ledger") if isinstance(state.get("causal_question_ledger"), dict) else {}
    epistemic_roles = state.get("worker_epistemic_roles_v2") if isinstance(state.get("worker_epistemic_roles_v2"), dict) else {}
    hypothesis_compiler = state.get("hunt_hypothesis_compiler") if isinstance(state.get("hunt_hypothesis_compiler"), dict) else {}
    experiment_contracts = state.get("experiment_contract_compiler") if isinstance(state.get("experiment_contract_compiler"), dict) else {}
    control_matcher = state.get("control_route_matcher") if isinstance(state.get("control_route_matcher"), dict) else {}
    sequential_monitor = state.get("sequential_test_monitor") if isinstance(state.get("sequential_test_monitor"), dict) else {}
    effect_ledger = state.get("causal_effect_size_ledger") if isinstance(state.get("causal_effect_size_ledger"), dict) else {}
    false_positive_pressure = state.get("false_positive_pressure_gauge") if isinstance(state.get("false_positive_pressure_gauge"), dict) else {}
    debt_paydown = state.get("exploration_debt_paydown_planner") if isinstance(state.get("exploration_debt_paydown_planner"), dict) else {}
    promotion_power = state.get("promotion_aware_power_planner") if isinstance(state.get("promotion_aware_power_planner"), dict) else {}
    scientific_executive = state.get("scientific_hunt_executive") if isinstance(state.get("scientific_hunt_executive"), dict) else {}
    live_evidence = state.get("live_candidate_evidence_builder") if isinstance(state.get("live_candidate_evidence_builder"), dict) else {}
    failure_v3 = state.get("promotion_failure_predictor_v3") if isinstance(state.get("promotion_failure_predictor_v3"), dict) else {}
    gap_router = state.get("evidence_gap_router") if isinstance(state.get("evidence_gap_router"), dict) else {}
    review_queue_v2 = state.get("review_ready_queue_v2") if isinstance(state.get("review_ready_queue_v2"), dict) else {}
    evidence_scorecard = state.get("promotion_evidence_scorecard") if isinstance(state.get("promotion_evidence_scorecard"), dict) else {}
    lineage_v2 = state.get("candidate_lineage_explainer_v2") if isinstance(state.get("candidate_lineage_explainer_v2"), dict) else {}
    control_diff_report = state.get("live_vs_control_differential_report") if isinstance(state.get("live_vs_control_differential_report"), dict) else {}
    promotion_packet_exec = state.get("promotion_packet_executive") if isinstance(state.get("promotion_packet_executive"), dict) else {}
    unified_learning = state.get("unified_learning_state_reducer") if isinstance(state.get("unified_learning_state_reducer"), dict) else {}
    artifact_priority = state.get("artifact_priority_arbitration_engine") if isinstance(state.get("artifact_priority_arbitration_engine"), dict) else {}
    provenance_graph = state.get("evidence_provenance_graph") if isinstance(state.get("evidence_provenance_graph"), dict) else {}
    counterfactual_replay = state.get("counterfactual_promotion_replay") if isinstance(state.get("counterfactual_promotion_replay"), dict) else {}
    survival_sim = state.get("candidate_survival_simulator") if isinstance(state.get("candidate_survival_simulator"), dict) else {}
    regime_shift_v2 = state.get("live_regime_shift_detector_v2") if isinstance(state.get("live_regime_shift_detector_v2"), dict) else {}
    module_trust_v2 = state.get("adaptive_trust_weights_per_module") if isinstance(state.get("adaptive_trust_weights_per_module"), dict) else {}
    worker_elo_v2 = state.get("worker_skill_elo_v2") if isinstance(state.get("worker_skill_elo_v2"), dict) else {}
    knowledge_graph = state.get("route_genome_knowledge_graph") if isinstance(state.get("route_genome_knowledge_graph"), dict) else {}
    causal_interactions = state.get("causal_feature_interaction_miner") if isinstance(state.get("causal_feature_interaction_miner"), dict) else {}
    overfit_library = state.get("overfit_signature_library") if isinstance(state.get("overfit_signature_library"), dict) else {}
    rejection_memory_v2 = state.get("review_rejection_memory_bank_v2") if isinstance(state.get("review_rejection_memory_bank_v2"), dict) else {}
    learning_budget = state.get("learning_budget_optimizer") if isinstance(state.get("learning_budget_optimizer"), dict) else {}
    novelty_floor = state.get("search_novelty_floor") if isinstance(state.get("search_novelty_floor"), dict) else {}
    breakthrough_protocol = state.get("breakthrough_escalation_protocol") if isinstance(state.get("breakthrough_escalation_protocol"), dict) else {}
    discovery_backpressure = state.get("false_discovery_backpressure_controller") if isinstance(state.get("false_discovery_backpressure_controller"), dict) else {}
    strategy_portfolio = state.get("multi_armed_strategy_portfolio") if isinstance(state.get("multi_armed_strategy_portfolio"), dict) else {}
    lesson_ab_harness = state.get("historical_lesson_ab_harness") if isinstance(state.get("historical_lesson_ab_harness"), dict) else {}
    packet_diff_engine = state.get("promotion_packet_diff_engine") if isinstance(state.get("promotion_packet_diff_engine"), dict) else {}
    repair_recipes_v2 = state.get("candidate_repair_recipe_generator") if isinstance(state.get("candidate_repair_recipe_generator"), dict) else {}
    red_team_v2 = state.get("automated_red_team_reviewer") if isinstance(state.get("automated_red_team_reviewer"), dict) else {}
    field_manual_v2 = state.get("learning_compression_field_manual") if isinstance(state.get("learning_compression_field_manual"), dict) else {}
    outcome_attribution_v2 = state.get("hunt_outcome_attribution_v2") if isinstance(state.get("hunt_outcome_attribution_v2"), dict) else {}
    world_dashboard = state.get("world_state_dashboard_artifact") if isinstance(state.get("world_state_dashboard_artifact"), dict) else {}
    meta_governor = state.get("meta_learning_governor") if isinstance(state.get("meta_learning_governor"), dict) else {}
    elite_artifacts = [
        state.get(key)
        for key in globals().get("ELITE_LEARNING_ARTIFACTS", ())
        if isinstance(state.get(key), dict)
    ]
    proof_artifacts = [
        state.get(key)
        for key in globals().get("PROOF_LEARNING_ARTIFACTS", ())
        if isinstance(state.get(key), dict)
    ]
    control_artifacts = [
        state.get(key)
        for key in globals().get("CLOSED_LOOP_CONTROL_ARTIFACTS", ())
        if isinstance(state.get(key), dict)
    ]
    nervous_artifacts = [
        state.get(key)
        for key in globals().get("WORLD_MODEL_NERVOUS_SYSTEM_ARTIFACTS", ())
        if isinstance(state.get(key), dict)
    ]
    orchestration_artifacts = [
        state.get(key)
        for key in globals().get("ORCHESTRATION_LEARNING_ARTIFACTS", ())
        if isinstance(state.get(key), dict)
    ]
    fitness_artifacts = [
        state.get(key)
        for key in globals().get("FITNESS_SELECTION_ARTIFACTS", ())
        if isinstance(state.get(key), dict)
    ]
    width = str(packet.get("mutation_width") or "medium")
    width_scale = {"tight": 0.72, "medium": 1.0, "wide": 1.28}.get(width, 1.0)
    action_counts: dict[str, int] = {}
    route_actions: dict[str, list[str]] = {}
    for command in commands:
        action = str(command.get("action") or "probe")
        route = str(command.get("route_key") or "")
        action_counts[action] = action_counts.get(action, 0) + 1
        if route:
            route_actions.setdefault(route, []).append(action)
    focus_routes = _ordered_unique(
        list(packet.get("focus_routes") or [])
        + list(ab_executor.get("focus_routes") or [])
        + list(contamination.get("safe_focus_routes") or [])
        + list(recovery.get("focus_routes") or [])
        + list(scarcity.get("focus_routes") or [])
        + list(seed_quality.get("clone_routes") or [])
        + list(opening_pack.get("focus_routes") or [])
        + list(opening_playbook.get("focus_routes") or [])
        + list(memory_arbiter.get("safe_focus_routes") or [])
        + list(falsification.get("focus_routes") or [])
        + list(disagreement.get("falsify_routes") or [])
        + list(stress_pack.get("focus_routes") or [])
        + list(hypothesis_market.get("focus_routes") or [])
        + list(bet_sizer.get("focus_routes") or [])
        + list(contrarian.get("focus_routes") or [])
        + list(breakthroughs.get("focus_routes") or [])
        + list(recipes.get("focus_routes") or [])
        + list(budget_reallocator.get("focus_routes") or [])
        + list(time_plan.get("focus_routes") or [])
        + list(creative_leaps.get("focus_routes") or [])
        + list(novelty_governor.get("focus_routes") or [])
        + list(creative_brief.get("focus_routes") or [])
        + list(causal_scheduler.get("focus_routes") or [])
        + list(temperature_controller.get("focus_routes") or [])
        + list(route_interactions.get("focus_routes") or [])
        + list(strategy_v2.get("focus_routes") or [])
        + list(disagreement_court.get("retest_routes") or [])
        + list(promotion_objective.get("focus_routes") or [])
        + list(strategy_selector.get("focus_routes") or [])
        + list(exploration_debt.get("focus_routes") or [])
        + list(pivot_governor.get("focus_routes") or [])
        + list(simplifier.get("focus_routes") or [])
        + list(dashboard.get("focus_routes") or [])
        + list(runbook.get("focus_routes") or [])
        + list(causal_bandit.get("focus_routes") or [])
        + list(dna_attribution.get("focus_routes") or [])
        + list(family_tree.get("focus_routes") or [])
        + list(frontier_map.get("focus_routes") or [])
        + list(worker_personalities.get("focus_routes") or [])
        + list(cycle_delta.get("focus_routes") or [])
        + list(rejection_v2.get("repair_routes") or [])
        + list(counterfactual_sim.get("focus_routes") or [])
        + list(missed_winners.get("focus_routes") or [])
        + list(regret_ledger.get("focus_routes") or [])
        + list(grammar_generator.get("focus_routes") or [])
        + list(hypothesis_court.get("focus_routes") or [])
        + list(shadow_scoring.get("focus_routes") or [])
        + list(autopilot.get("focus_routes") or [])
        + list(confidence_calibration.get("focus_routes") or [])
        + list(debate_council.get("focus_routes") or [])
        + list(promotion_autopilot.get("focus_routes") or [])
        + list(horizon_memory.get("focus_routes") or [])
        + list(half_life_v2.get("refresh_routes") or [])
        + list(strategy_replay.get("focus_routes") or [])
        + list(regime_fingerprint.get("focus_routes") or [])
        + list(longitudinal_survival.get("survival_focus_routes") or [])
        + list(conflict_court_v2.get("focus_routes") or [])
        + list(conflict_court_v2.get("falsify_routes") or [])
        + list(strategy_aging.get("resurrect_routes") or [])
        + list(next_hunt_opening.get("focus_routes") or [])
        + list(question_planner.get("focus_routes") or [])
        + list(eig_v2.get("focus_routes") or [])
        + list(uncertainty_map.get("probe_routes") or [])
        + list(experiment_sequencer.get("focus_routes") or [])
        + list(causal_ledger.get("focus_routes") or [])
        + list(epistemic_roles.get("focus_routes") or [])
        + list(hypothesis_compiler.get("focus_routes") or [])
        + list(experiment_contracts.get("focus_routes") or [])
        + list(control_matcher.get("control_routes") or [])
        + list(sequential_monitor.get("focus_routes") or [])
        + list(effect_ledger.get("focus_routes") or [])
        + list(debt_paydown.get("focus_routes") or [])
        + list(promotion_power.get("focus_routes") or [])
        + list(scientific_executive.get("focus_routes") or [])
        + list(live_evidence.get("focus_routes") or [])
        + list(failure_v3.get("repair_routes") or [])
        + list(gap_router.get("focus_routes") or [])
        + list(review_queue_v2.get("focus_routes") or [])
        + list(control_diff_report.get("focus_routes") or [])
        + list(promotion_packet_exec.get("focus_routes") or [])
        + list(unified_learning.get("focus_routes") or [])
        + list(artifact_priority.get("focus_routes") or [])
        + list(counterfactual_replay.get("repair_routes") or [])
        + list(survival_sim.get("focus_routes") or [])
        + list(regime_shift_v2.get("focus_routes") or [])
        + list(module_trust_v2.get("focus_routes") or [])
        + list(knowledge_graph.get("focus_routes") or [])
        + list(causal_interactions.get("focus_routes") or [])
        + list(rejection_memory_v2.get("repair_routes") or [])
        + list(learning_budget.get("focus_routes") or [])
        + list(novelty_floor.get("focus_routes") or [])
        + list(breakthrough_protocol.get("focus_routes") or [])
        + list(strategy_portfolio.get("focus_routes") or [])
        + list(repair_recipes_v2.get("focus_routes") or [])
        + list(red_team_v2.get("repair_routes") or [])
        + list(outcome_attribution_v2.get("focus_routes") or [])
        + list(world_dashboard.get("focus_routes") or [])
        + list(meta_governor.get("focus_routes") or [])
        + [route for artifact in elite_artifacts for route in (artifact.get("focus_routes") or artifact.get("repair_routes") or [])]
        + [route for artifact in proof_artifacts for route in (artifact.get("focus_routes") or artifact.get("repair_routes") or [])]
        + [route for artifact in control_artifacts for route in (artifact.get("focus_routes") or artifact.get("repair_routes") or [])]
        + [route for artifact in nervous_artifacts for route in (artifact.get("focus_routes") or artifact.get("repair_routes") or [])]
        + [route for artifact in orchestration_artifacts for route in (artifact.get("focus_routes") or artifact.get("repair_routes") or [])]
        + [route for artifact in fitness_artifacts for route in (artifact.get("focus_routes") or artifact.get("repair_routes") or [])]
        + [str(((slot.get("command") or {}).get("route_key") or "")) for slot in (champion_slots.get("slots") or []) if isinstance(slot, dict)]
    )
    blocked_routes = {str(route) for route in (contamination.get("blocked_routes") or []) if str(route)}
    avoid_routes = _ordered_unique(
        list(packet.get("avoid_routes") or [])
        + list(blocked_routes)
        + list(recovery.get("skip_routes") or [])
        + list(seed_quality.get("retire_routes") or [])
        + list(opening_pack.get("avoid_routes") or [])
        + list(opening_playbook.get("avoid_routes") or [])
        + list(memory_arbiter.get("avoid_routes") or [])
        + list(retirement.get("retire_routes") or [])
        + list(disagreement.get("override_history_routes") or [])
        + list(stop_loss.get("avoid_routes") or [])
        + list(budget_reallocator.get("avoid_routes") or [])
        + list(time_plan.get("avoid_routes") or [])
        + list(module_conflicts.get("pause_routes") or [])
        + list(module_budget.get("pause_routes") or [])
        + list(temperature_controller.get("avoid_routes") or [])
        + list(false_firewall.get("avoid_routes") or [])
        + list(strategy_v2.get("avoid_routes") or [])
        + list(disagreement_court.get("pause_routes") or [])
        + list(promotion_objective.get("avoid_routes") or [])
        + list(strategy_selector.get("avoid_routes") or [])
        + list(strategy_red_team.get("avoid_routes") or [])
        + list(pivot_governor.get("avoid_routes") or [])
        + list(simplifier.get("avoid_routes") or [])
        + list(dashboard.get("avoid_routes") or [])
        + list(runbook.get("avoid_routes") or [])
        + list(sentinel.get("pause_routes") or [])
        + list(readiness.get("block_routes") or [])
        + list(gene_suppression.get("avoid_routes") or [])
        + list(cycle_delta.get("avoid_routes") or [])
        + list(rejection_v2.get("avoid_routes") or [])
        + list(regret_ledger.get("avoid_routes") or [])
        + list(hypothesis_court.get("avoid_routes") or [])
        + list(autopilot.get("avoid_routes") or [])
        + list(confidence_calibration.get("pause_routes") or [])
        + list(false_warning.get("avoid_routes") or [])
        + list(debate_council.get("avoid_routes") or [])
        + list(drift_monitor.get("retire_routes") or [])
        + list(promotion_autopilot.get("avoid_routes") or [])
        + list(horizon_memory.get("avoid_routes") or [])
        + list(half_life_v2.get("decay_routes") or [])
        + list(strategy_replay.get("avoid_routes") or [])
        + list(longitudinal_survival.get("risk_avoid_routes") or [])
        + list(conflict_court_v2.get("avoid_routes") or [])
        + list(strategy_aging.get("retire_routes") or [])
        + list(next_hunt_opening.get("avoid_routes") or [])
        + list(experiment_sequencer.get("avoid_routes") or [])
        + list(learning_value_stop.get("avoid_routes") or [])
        + list(sequential_monitor.get("avoid_routes") or [])
        + list(false_positive_pressure.get("avoid_routes") or [])
        + list(scientific_executive.get("avoid_routes") or [])
        + list(failure_v3.get("avoid_routes") or [])
        + list(promotion_packet_exec.get("avoid_routes") or [])
        + list(unified_learning.get("avoid_routes") or [])
        + list(artifact_priority.get("avoid_routes") or [])
        + list(counterfactual_replay.get("avoid_routes") or [])
        + list(survival_sim.get("avoid_routes") or [])
        + list(regime_shift_v2.get("discount_routes") or [])
        + list(overfit_library.get("avoid_routes") or [])
        + list(rejection_memory_v2.get("avoid_routes") or [])
        + list(discovery_backpressure.get("avoid_routes") or [])
        + list(red_team_v2.get("avoid_routes") or [])
        + list(world_dashboard.get("avoid_routes") or [])
        + list(meta_governor.get("avoid_routes") or [])
        + [route for artifact in elite_artifacts for route in (artifact.get("avoid_routes") or artifact.get("retire_routes") or artifact.get("block_routes") or [])]
        + [route for artifact in proof_artifacts for route in (artifact.get("avoid_routes") or artifact.get("retire_routes") or artifact.get("block_routes") or [])]
        + [route for artifact in control_artifacts for route in (artifact.get("avoid_routes") or artifact.get("retire_routes") or artifact.get("block_routes") or [])]
        + [route for artifact in nervous_artifacts for route in (artifact.get("avoid_routes") or artifact.get("retire_routes") or artifact.get("block_routes") or [])]
        + [route for artifact in orchestration_artifacts for route in (artifact.get("avoid_routes") or artifact.get("retire_routes") or artifact.get("block_routes") or [])]
        + [route for artifact in fitness_artifacts for route in (artifact.get("avoid_routes") or artifact.get("retire_routes") or artifact.get("block_routes") or [])]
    )
    focus_routes = [route for route in focus_routes if route not in set(avoid_routes)]
    batch_mult = float(packet.get("batch_size_multiplier") or 1.0)
    if learning_rate.get("batch_size_multiplier"):
        batch_mult *= float(learning_rate.get("batch_size_multiplier") or 1.0)
    if bet_sizer.get("batch_size_multiplier"):
        batch_mult *= float(bet_sizer.get("batch_size_multiplier") or 1.0)
    if budget_reallocator.get("batch_size_multiplier"):
        batch_mult *= float(budget_reallocator.get("batch_size_multiplier") or 1.0)
    if time_plan.get("batch_size_multiplier"):
        batch_mult *= float(time_plan.get("batch_size_multiplier") or 1.0)
    if novelty_governor.get("batch_size_multiplier"):
        batch_mult *= float(novelty_governor.get("batch_size_multiplier") or 1.0)
    if module_budget.get("batch_size_multiplier"):
        batch_mult *= float(module_budget.get("batch_size_multiplier") or 1.0)
    if temperature_controller.get("batch_size_multiplier"):
        batch_mult *= float(temperature_controller.get("batch_size_multiplier") or 1.0)
    if promotion_objective.get("batch_size_multiplier"):
        batch_mult *= float(promotion_objective.get("batch_size_multiplier") or 1.0)
    if strategy_selector.get("batch_size_multiplier"):
        batch_mult *= float(strategy_selector.get("batch_size_multiplier") or 1.0)
    if meta_objective.get("batch_size_multiplier"):
        batch_mult *= float(meta_objective.get("batch_size_multiplier") or 1.0)
    if pivot_governor.get("batch_size_multiplier"):
        batch_mult *= float(pivot_governor.get("batch_size_multiplier") or 1.0)
    if causal_bandit.get("batch_size_multiplier"):
        batch_mult *= float(causal_bandit.get("batch_size_multiplier") or 1.0)
    if autopilot.get("batch_size_multiplier"):
        batch_mult = float(autopilot.get("batch_size_multiplier") or batch_mult)
    if promotion_autopilot.get("batch_size_multiplier"):
        batch_mult = float(promotion_autopilot.get("batch_size_multiplier") or batch_mult)
    if next_hunt_opening.get("batch_size_multiplier") and not promotion_autopilot.get("batch_size_multiplier"):
        batch_mult = float(next_hunt_opening.get("batch_size_multiplier") or batch_mult)
    if learning_value_stop.get("batch_size_multiplier"):
        batch_mult = min(batch_mult, float(learning_value_stop.get("batch_size_multiplier") or batch_mult))
    if false_positive_pressure.get("batch_size_multiplier"):
        batch_mult = min(batch_mult, float(false_positive_pressure.get("batch_size_multiplier") or batch_mult))
    if (scientific_executive.get("command_packet") or {}).get("batch_size_multiplier"):
        batch_mult = min(batch_mult, float((scientific_executive.get("command_packet") or {}).get("batch_size_multiplier") or batch_mult))
    if learning_budget.get("batch_size_multiplier"):
        batch_mult *= float(learning_budget.get("batch_size_multiplier") or 1.0)
    if discovery_backpressure.get("batch_size_multiplier"):
        batch_mult = min(batch_mult, float(discovery_backpressure.get("batch_size_multiplier") or batch_mult))
    if meta_governor.get("batch_size_multiplier"):
        batch_mult = min(batch_mult, float(meta_governor.get("batch_size_multiplier") or batch_mult))
    elite_batch_limits = [
        float(artifact.get("batch_size_multiplier"))
        for artifact in elite_artifacts
        if artifact.get("batch_size_multiplier") is not None
    ]
    if elite_batch_limits:
        batch_mult = min(batch_mult, min(elite_batch_limits))
    proof_batch_limits = [
        float(artifact.get("batch_size_multiplier"))
        for artifact in proof_artifacts
        if artifact.get("batch_size_multiplier") is not None
    ]
    if proof_batch_limits:
        batch_mult = min(batch_mult, min(proof_batch_limits))
    control_batch_limits = [
        float(artifact.get("batch_size_multiplier"))
        for artifact in control_artifacts
        if artifact.get("batch_size_multiplier") is not None
    ]
    if control_batch_limits:
        batch_mult = min(batch_mult, min(control_batch_limits))
    nervous_batch_limits = [
        float(artifact.get("batch_size_multiplier"))
        for artifact in nervous_artifacts
        if artifact.get("batch_size_multiplier") is not None
    ]
    if nervous_batch_limits:
        batch_mult = min(batch_mult, min(nervous_batch_limits))
    orchestration_batch_limits = [
        float(artifact.get("batch_size_multiplier"))
        for artifact in orchestration_artifacts
        if artifact.get("batch_size_multiplier") is not None
    ]
    if orchestration_batch_limits:
        batch_mult = min(batch_mult, min(orchestration_batch_limits))
    fitness_batch_limits = [
        float(artifact.get("batch_size_multiplier"))
        for artifact in fitness_artifacts
        if artifact.get("batch_size_multiplier") is not None
    ]
    if fitness_batch_limits:
        batch_mult = min(batch_mult, min(fitness_batch_limits))
    if false_warning.get("batch_size_multiplier"):
        batch_mult = min(batch_mult, float(false_warning.get("batch_size_multiplier") or batch_mult))
    if simplifier.get("batch_size_multiplier"):
        batch_mult = float(simplifier.get("batch_size_multiplier") or batch_mult)
    if sentinel.get("batch_size_multiplier"):
        batch_mult = min(batch_mult, float(sentinel.get("batch_size_multiplier") or batch_mult))
    if contamination and contamination.get("ok") is False:
        batch_mult = min(batch_mult, 0.65)
    if scarcity.get("enabled"):
        batch_mult = max(batch_mult, float(scarcity.get("batch_size_multiplier") or 1.0))
        width = str(scarcity.get("mutation_width") or width)
        width_scale = max(width_scale, 1.28 if width == "wide" else width_scale)
    if opening_playbook.get("mutation_width") and not scarcity.get("enabled"):
        width = str(opening_playbook.get("mutation_width") or width)
        width_scale = {"tight": 0.72, "medium": 1.0, "wide": 1.28}.get(width, width_scale)
    if budget_reallocator.get("mutation_width"):
        width = str(budget_reallocator.get("mutation_width") or width)
        width_scale = {"tight": 0.72, "medium": 1.0, "wide": 1.28}.get(width, width_scale)
    if time_plan.get("mutation_width"):
        width = str(time_plan.get("mutation_width") or width)
        width_scale = {"tight": 0.72, "medium": 1.0, "wide": 1.28}.get(width, width_scale)
    if novelty_governor.get("mutation_width") and (time_plan.get("phase") or "") != "late":
        width = str(novelty_governor.get("mutation_width") or width)
        width_scale = {"tight": 0.72, "medium": 1.0, "wide": 1.28}.get(width, width_scale)
    if temperature_controller.get("mutation_width"):
        width = str(temperature_controller.get("mutation_width") or width)
        width_scale = {"tight": 0.72, "medium": 1.0, "wide": 1.28}.get(width, width_scale)
    if strategy_v2.get("mutation_width"):
        width = str(strategy_v2.get("mutation_width") or width)
        width_scale = {"tight": 0.72, "medium": 1.0, "wide": 1.28}.get(width, width_scale)
    if strategy_selector.get("mutation_width"):
        width = str(strategy_selector.get("mutation_width") or width)
        width_scale = {"tight": 0.72, "medium": 1.0, "wide": 1.28}.get(width, width_scale)
    if pivot_governor.get("mutation_width"):
        width = str(pivot_governor.get("mutation_width") or width)
        width_scale = {"tight": 0.72, "medium": 1.0, "wide": 1.28}.get(width, width_scale)
    if frontier_map.get("mutation_width"):
        width = str(frontier_map.get("mutation_width") or width)
        width_scale = {"tight": 0.72, "medium": 1.0, "wide": 1.28}.get(width, width_scale)
    if causal_bandit.get("mutation_width"):
        width = str(causal_bandit.get("mutation_width") or width)
        width_scale = {"tight": 0.72, "medium": 1.0, "wide": 1.28}.get(width, width_scale)
    if autopilot.get("mutation_width"):
        width = str(autopilot.get("mutation_width") or width)
        width_scale = {"tight": 0.72, "medium": 1.0, "wide": 1.28}.get(width, width_scale)
    if promotion_autopilot.get("mutation_width"):
        width = str(promotion_autopilot.get("mutation_width") or width)
        width_scale = {"tight": 0.72, "medium": 1.0, "wide": 1.28}.get(width, width_scale)
    if next_hunt_opening.get("mutation_width") and not promotion_autopilot.get("mutation_width"):
        width = str(next_hunt_opening.get("mutation_width") or width)
        width_scale = {"tight": 0.72, "medium": 1.0, "wide": 1.28}.get(width, width_scale)
    if (scientific_executive.get("command_packet") or {}).get("mutation_width") and not promotion_autopilot.get("mutation_width"):
        width = str((scientific_executive.get("command_packet") or {}).get("mutation_width") or width)
        width_scale = {"tight": 0.72, "medium": 1.0, "wide": 1.28}.get(width, width_scale)
    if novelty_floor.get("mutation_width") and not promotion_autopilot.get("mutation_width"):
        width = str(novelty_floor.get("mutation_width") or width)
        width_scale = {"tight": 0.72, "medium": 1.0, "wide": 1.28}.get(width, width_scale)
    if discovery_backpressure.get("mutation_width"):
        width = str(discovery_backpressure.get("mutation_width") or width)
        width_scale = {"tight": 0.72, "medium": 1.0, "wide": 1.28}.get(width, width_scale)
    if meta_governor.get("mutation_width"):
        width = str(meta_governor.get("mutation_width") or width)
        width_scale = {"tight": 0.72, "medium": 1.0, "wide": 1.28}.get(width, width_scale)
    elite_widths = [str(artifact.get("mutation_width") or "") for artifact in elite_artifacts if artifact.get("mutation_width")]
    if "tight" in elite_widths:
        width = "tight"
        width_scale = {"tight": 0.72, "medium": 1.0, "wide": 1.28}.get(width, width_scale)
    elif "wide" in elite_widths and width != "tight":
        width = "wide"
        width_scale = {"tight": 0.72, "medium": 1.0, "wide": 1.28}.get(width, width_scale)
    proof_widths = [str(artifact.get("mutation_width") or "") for artifact in proof_artifacts if artifact.get("mutation_width")]
    if "tight" in proof_widths:
        width = "tight"
        width_scale = {"tight": 0.72, "medium": 1.0, "wide": 1.28}.get(width, width_scale)
    control_widths = [str(artifact.get("mutation_width") or "") for artifact in control_artifacts if artifact.get("mutation_width")]
    if "tight" in control_widths:
        width = "tight"
        width_scale = {"tight": 0.72, "medium": 1.0, "wide": 1.28}.get(width, width_scale)
    elif "wide" in control_widths and width != "tight":
        width = "wide"
        width_scale = {"tight": 0.72, "medium": 1.0, "wide": 1.28}.get(width, width_scale)
    nervous_widths = [str(artifact.get("mutation_width") or "") for artifact in nervous_artifacts if artifact.get("mutation_width")]
    if "tight" in nervous_widths:
        width = "tight"
        width_scale = {"tight": 0.72, "medium": 1.0, "wide": 1.28}.get(width, width_scale)
    elif "wide" in nervous_widths and width != "tight":
        width = "wide"
        width_scale = {"tight": 0.72, "medium": 1.0, "wide": 1.28}.get(width, width_scale)
    orchestration_widths = [str(artifact.get("mutation_width") or "") for artifact in orchestration_artifacts if artifact.get("mutation_width")]
    if "tight" in orchestration_widths:
        width = "tight"
        width_scale = {"tight": 0.72, "medium": 1.0, "wide": 1.28}.get(width, width_scale)
    elif "wide" in orchestration_widths and width != "tight":
        width = "wide"
        width_scale = {"tight": 0.72, "medium": 1.0, "wide": 1.28}.get(width, width_scale)
    fitness_widths = [str(artifact.get("mutation_width") or "") for artifact in fitness_artifacts if artifact.get("mutation_width")]
    if "tight" in fitness_widths:
        width = "tight"
        width_scale = {"tight": 0.72, "medium": 1.0, "wide": 1.28}.get(width, width_scale)
    if simplifier.get("mutation_width"):
        width = str(simplifier.get("mutation_width") or width)
        width_scale = {"tight": 0.72, "medium": 1.0, "wide": 1.28}.get(width, width_scale)
    if sentinel.get("mutation_width"):
        width = str(sentinel.get("mutation_width") or width)
        width_scale = {"tight": 0.72, "medium": 1.0, "wide": 1.28}.get(width, width_scale)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Normalized runtime controls consumed by hunt workers and router batches.",
        "live_only": True,
        "max_workers": min(4, max(1, int(packet.get("max_workers") or 4))),
        "degrade_mode": bool(packet.get("degraded")),
        "degrade_reason": packet.get("degrade_reason") or "",
        "focus_routes": focus_routes[:12],
        "avoid_routes": avoid_routes,
        "mutation_width": width,
        "mutation_scale_multiplier": round(width_scale, 4),
        "batch_size_multiplier": round(batch_mult, 4),
        "route_breadth": packet.get("route_breadth") or "balanced",
        "commands": commands,
        "scientific_commands": list(scientific_executive.get("commands") or [])[:12],
        "ab_assignments": list(ab_executor.get("assignments") or [])[:8],
        "champion_challenger_slots": list(champion_slots.get("slots") or [])[:4],
        "learning_rate_mode": learning_rate.get("mode"),
        "contamination_ok": contamination.get("ok"),
        "scarcity_mode": scarcity.get("mode"),
        "recovery_interventions": list(recovery.get("interventions") or [])[:12],
        "structural_mutation_routes": _ordered_unique(list(recovery.get("structural_mutation_routes") or []) + list(alias_traps.get("structural_mutation_routes") or []))[:12],
        "retire_routes": _ordered_unique(list(seed_quality.get("retire_routes") or []) + list(recovery.get("skip_routes") or []))[:12],
        "pre_hunt_strategy": (state.get("pre_hunt_strategy_selector") or {}).get("strategy"),
        "cross_run_opening_focus": list(opening_playbook.get("focus_routes") or opening_pack.get("focus_routes") or [])[:12],
        "memory_falsification_routes": list(falsification.get("focus_routes") or [])[:12],
        "memory_stress_test_routes": list(stress_pack.get("focus_routes") or [])[:12],
        "memory_retire_routes": list(retirement.get("retire_routes") or [])[:12],
        "memory_disagreement_count": disagreement.get("disagreement_count"),
        "hypothesis_focus_routes": list(hypothesis_market.get("focus_routes") or [])[:12],
        "hypothesis_bet_routes": list(bet_sizer.get("focus_routes") or [])[:12],
        "contrarian_routes": list(contrarian.get("focus_routes") or [])[:12],
        "learning_stop_loss_routes": list(stop_loss.get("avoid_routes") or [])[:12],
        "breakthrough_routes": list(breakthroughs.get("focus_routes") or [])[:12],
        "recipe_routes": list(recipes.get("focus_routes") or [])[:12],
        "attention_budget_focus_routes": list(budget_reallocator.get("focus_routes") or [])[:12],
        "attention_budget_avoid_routes": list(budget_reallocator.get("avoid_routes") or [])[:12],
        "time_plan_phase": time_plan.get("phase"),
        "time_plan_action": time_plan.get("primary_action"),
        "creative_focus_routes": list(creative_leaps.get("focus_routes") or [])[:12],
        "novelty_budget_focus_routes": list(novelty_governor.get("focus_routes") or [])[:12],
        "novelty_budget_pct": novelty_governor.get("novelty_budget_pct"),
        "creative_brief": creative_brief.get("summary"),
        "module_conflict_count": module_conflicts.get("conflict_count"),
        "module_pause_routes": list(module_conflicts.get("pause_routes") or module_budget.get("pause_routes") or [])[:12],
        "module_budget_top": module_budget.get("top_module") or {},
        "runtime_science_temperature": temperature_controller.get("temperature"),
        "runtime_science_top_intervention": causal_scheduler.get("top_intervention") or {},
        "false_discovery_flag_count": false_firewall.get("flag_count"),
        "hunt_strategy_v2": strategy_v2.get("strategy"),
        "learning_court_case_count": disagreement_court.get("case_count"),
        "promotion_aware_top": promotion_objective.get("top_candidate") or {},
        "strategy_regime": strategy_selector.get("regime"),
        "meta_objective": meta_objective.get("objective"),
        "exploration_debt_count": exploration_debt.get("debt_count"),
        "strategy_red_team_top_attack": strategy_red_team.get("top_attack") or {},
        "autonomous_pivot": bool(pivot_governor.get("pivot")),
        "architecture_fitness_verdict": fitness.get("verdict"),
        "pruned_artifacts": list(pruner.get("prune_candidates") or [])[:12],
        "simplified_control_packet": simplifier.get("final_command_packet") or {},
        "learning_ops_status": dashboard.get("status"),
        "schema_registry_valid": schema_registry.get("valid"),
        "pre_hunt_ready": readiness.get("ready"),
        "learning_failure_count": sentinel.get("failure_count"),
        "runbook_worker_count": runbook.get("worker_count"),
        "online_causal_top_lane": causal_bandit.get("top_lane"),
        "variant_dna_top_gene": (dna_attribution.get("top_gene") or {}).get("gene"),
        "negative_gene_suppressed_count": gene_suppression.get("suppressed_count"),
        "winner_family_count": family_tree.get("family_count"),
        "exploration_frontier_count": frontier_map.get("frontier_count"),
        "adaptive_worker_personalities": list(worker_personalities.get("assignments") or [])[:4],
        "cycle_learning_delta": cycle_delta.get("summary"),
        "promotion_rejection_v2_highest_risk": rejection_v2.get("highest_risk") or {},
        "top_counterfactual": counterfactual_sim.get("top_counterfactual") or {},
        "missed_winner_count": missed_winners.get("missed_count"),
        "top_causal_regret": regret_ledger.get("top_regret") or {},
        "search_grammar_top_templates": list(grammar_generator.get("top_templates") or [])[:8],
        "hypothesis_court_scale_count": hypothesis_court.get("scale_count"),
        "route_interaction_v2_top": (state.get("route_interaction_matrix_v2") or {}).get("top_interaction") if isinstance(state.get("route_interaction_matrix_v2"), dict) else {},
        "promotion_shadow_top": shadow_scoring.get("top_shadow") or {},
        "autopilot_policy_packet": autopilot.get("policy_packet") or {},
        "verified_learning_claim_count": claim_verifier.get("verified_count"),
        "causal_confidence_top": confidence_calibration.get("top_confidence") or {},
        "false_discovery_warning_count": false_warning.get("warning_count"),
        "evidence_threshold_mode": (state.get("adaptive_evidence_thresholds") or {}).get("mode") if isinstance(state.get("adaptive_evidence_thresholds"), dict) else None,
        "self_debate_resolution": debate_council.get("resolution") or {},
        "compressed_memory_rule_count": (state.get("experiment_memory_compression") or {}).get("rule_count") if isinstance(state.get("experiment_memory_compression"), dict) else None,
        "learning_drift_count": drift_monitor.get("drift_count"),
        "promotion_first_policy_packet": promotion_autopilot.get("policy_packet") or {},
        "memory_horizon_summary": horizon_memory.get("summary") or {},
        "lesson_half_life_top": half_life_v2.get("top_lesson") or {},
        "cross_hunt_replay_top": strategy_replay.get("top_replay") or {},
        "temporal_regime_top": regime_fingerprint.get("top_fingerprint") or {},
        "longitudinal_survival_top": longitudinal_survival.get("top_survival_feature") or {},
        "memory_conflict_v2_count": conflict_court_v2.get("case_count"),
        "strategy_aging_counts": strategy_aging.get("aging_counts") or {},
        "next_hunt_opening_policy": next_hunt_opening.get("policy_packet") or {},
        "question_planner_top": question_planner.get("top_question") or {},
        "expected_information_gain_top": eig_v2.get("top_score") or {},
        "uncertainty_heatmap_top": uncertainty_map.get("top_cell") or {},
        "adaptive_experiment_next_step": experiment_sequencer.get("next_step") or {},
        "learning_value_stop_count": learning_value_stop.get("stop_count"),
        "causal_question_top": causal_ledger.get("top_question") or {},
        "epistemic_role_assignments": list(epistemic_roles.get("assignments") or [])[:4],
        "compiled_hypotheses": list(hypothesis_compiler.get("hypotheses") or [])[:5],
        "experiment_contract_top": experiment_contracts.get("top_contract") or {},
        "control_route_top": control_matcher.get("top_match") or {},
        "sequential_test_top": sequential_monitor.get("top_decision") or {},
        "causal_effect_top": effect_ledger.get("top_effect") or {},
        "false_positive_pressure": {
            "score": false_positive_pressure.get("pressure_score"),
            "mode": false_positive_pressure.get("mode"),
        },
        "exploration_paydown_top": debt_paydown.get("top_plan") or {},
        "promotion_power_top": promotion_power.get("top_plan") or {},
        "scientific_executive_summary": scientific_executive.get("summary") or {},
        "scientific_executive_top_command": scientific_executive.get("top_command") or {},
        "live_candidate_evidence_top": live_evidence.get("top_packet") or {},
        "promotion_failure_v3_highest_risk": failure_v3.get("highest_risk") or {},
        "evidence_gap_top_task": gap_router.get("top_task") or {},
        "review_ready_v2_top": review_queue_v2.get("top_ready") or {},
        "promotion_scorecard_top": evidence_scorecard.get("top_scorecard") or {},
        "candidate_lineage_top": lineage_v2.get("top_explanation") or {},
        "live_control_differential_top": control_diff_report.get("top_report") or {},
        "promotion_packet_executive_summary": promotion_packet_exec.get("summary") or {},
        "promotion_packet_top_decision": promotion_packet_exec.get("top_decision") or {},
        "unified_learning_summary": unified_learning.get("summary") or {},
        "artifact_priority_top": artifact_priority.get("top_artifact") or {},
        "evidence_provenance_top": provenance_graph.get("top_node") or {},
        "counterfactual_promotion_top": counterfactual_replay.get("top_replay") or {},
        "candidate_survival_top": survival_sim.get("top_simulation") or {},
        "regime_shift_v2": {"mode": regime_shift_v2.get("mode"), "shift_score": regime_shift_v2.get("shift_score")},
        "module_trust_top": module_trust_v2.get("top_module") or {},
        "worker_skill_top": worker_elo_v2.get("top_worker") or {},
        "knowledge_graph_summary": knowledge_graph.get("summary") or {},
        "causal_interaction_top": causal_interactions.get("top_interaction") or {},
        "overfit_signature_top": overfit_library.get("top_signature") or {},
        "rejection_memory_top": rejection_memory_v2.get("top_memory") or {},
        "learning_budget_top": learning_budget.get("top_allocation") or {},
        "novelty_floor_top": novelty_floor.get("summary") or {},
        "breakthrough_top": breakthrough_protocol.get("top_breakthrough") or {},
        "false_discovery_backpressure": {
            "score": discovery_backpressure.get("pressure_score"),
            "mode": discovery_backpressure.get("mode"),
        },
        "strategy_portfolio_top": strategy_portfolio.get("top_arm") or {},
        "historical_lesson_ab_top": lesson_ab_harness.get("top_test") or {},
        "promotion_packet_diff_top": packet_diff_engine.get("top_diff") or {},
        "repair_recipe_top": repair_recipes_v2.get("top_recipe") or {},
        "red_team_top": red_team_v2.get("top_objection") or {},
        "field_manual_top_rule": field_manual_v2.get("top_rule") or {},
        "outcome_attribution_top": outcome_attribution_v2.get("top_attribution") or {},
        "world_state_summary": world_dashboard.get("summary") or {},
        "meta_learning_governor_summary": meta_governor.get("summary") or {},
        "elite_learning_summary": (state.get("elite_learning_system_summary") or {}).get("summary") if isinstance(state.get("elite_learning_system_summary"), dict) else {},
        "elite_next_best_question": (state.get("world_state_next_best_question_engine") or {}).get("top_record") if isinstance(state.get("world_state_next_best_question_engine"), dict) else {},
        "elite_top_actions": [
            {
                "artifact": artifact.get("artifact"),
                "action": (artifact.get("top_record") or {}).get("recommended_action"),
                "route_key": (artifact.get("top_record") or {}).get("route_key"),
            }
            for artifact in elite_artifacts[:8]
        ],
        "proof_learning_summary": (state.get("proof_learning_system_summary") or {}).get("summary") if isinstance(state.get("proof_learning_system_summary"), dict) else {},
        "truth_ledger_top_claim": (state.get("learning_truth_ledger") or {}).get("top_record") if isinstance(state.get("learning_truth_ledger"), dict) else {},
        "research_agenda_top_question": (state.get("adaptive_research_agenda") or {}).get("top_record") if isinstance(state.get("adaptive_research_agenda"), dict) else {},
        "review_board_decision": (state.get("autonomous_hunt_review_board") or {}).get("top_record") if isinstance(state.get("autonomous_hunt_review_board"), dict) else {},
        "closed_loop_control_summary": (state.get("closed_loop_control_learning_summary") or {}).get("summary") if isinstance(state.get("closed_loop_control_learning_summary"), dict) else {},
        "bayesian_top_belief": (state.get("bayesian_belief_engine") or {}).get("top_record") if isinstance(state.get("bayesian_belief_engine"), dict) else {},
        "active_experiment_next": (state.get("active_experiment_selector") or {}).get("top_record") if isinstance(state.get("active_experiment_selector"), dict) else {},
        "learning_status_feed": (state.get("real_time_learning_dashboard_feed") or {}).get("top_record") if isinstance(state.get("real_time_learning_dashboard_feed"), dict) else {},
        "world_model_nervous_system_summary": (state.get("world_model_nervous_system_summary") or {}).get("summary") if isinstance(state.get("world_model_nervous_system_summary"), dict) else {},
        "nervous_system_top_truth": (state.get("hypothesis_dependency_graph") or {}).get("top_record") if isinstance(state.get("hypothesis_dependency_graph"), dict) else {},
        "nervous_system_top_promotion": (state.get("promotion_packet_completeness_optimizer") or {}).get("top_record") if isinstance(state.get("promotion_packet_completeness_optimizer"), dict) else {},
        "nervous_system_world_audit": (state.get("world_model_self_audit_loop") or {}).get("top_record") if isinstance(state.get("world_model_self_audit_loop"), dict) else {},
        "orchestration_learning_summary": (state.get("orchestration_learning_summary") or {}).get("summary") if isinstance(state.get("orchestration_learning_summary"), dict) else {},
        "orchestration_top_artifact_governance": (state.get("artifact_governance_dashboard") or {}).get("top_record") if isinstance(state.get("artifact_governance_dashboard"), dict) else {},
        "orchestration_top_hunt_exec": (state.get("hunt_executive_controller") or {}).get("top_record") if isinstance(state.get("hunt_executive_controller"), dict) else {},
        "fitness_selection_summary": (state.get("fitness_selection_summary") or {}).get("summary") if isinstance(state.get("fitness_selection_summary"), dict) else {},
        "fitness_top_scorecard": (state.get("artifact_fitness_scorecard") or {}).get("top_record") if isinstance(state.get("artifact_fitness_scorecard"), dict) else {},
        "fitness_trust_policy": (state.get("learning_layer_trust_policy") or {}).get("top_record") if isinstance(state.get("learning_layer_trust_policy"), dict) else {},
        "fitness_world_report": (state.get("world_model_fitness_report") or {}).get("top_record") if isinstance(state.get("world_model_fitness_report"), dict) else {},
        "action_counts": action_counts,
        "route_actions": route_actions,
        "primary_command": commands[0] if commands else {},
    }


def planner_worker_contracts(state: dict[str, Any], *, max_workers: int = 4) -> dict[str, Any]:
    adapter = runtime_command_adapter(state)
    planner = state.get("autonomous_hunt_planner") if isinstance(state.get("autonomous_hunt_planner"), dict) else {}
    jobs = [dict(job) for job in (planner.get("jobs") or []) if isinstance(job, dict)]
    next_hunt_opening = state.get("next_hunt_opening_policy_compiler") if isinstance(state.get("next_hunt_opening_policy_compiler"), dict) else {}
    for role in next_hunt_opening.get("worker_roles") or []:
        if not isinstance(role, dict):
            continue
        jobs.append({
            "job_id": _stable_hash({"next_hunt_opening_policy_compiler": role}),
            "worker": role.get("worker") or f"worker_{(len(jobs) % max_workers) + 1}",
            "action": role.get("role") or "temporal_opening_probe",
            "route_key": role.get("route_key") or "",
            "mutation_width": role.get("mutation_width") or next_hunt_opening.get("mutation_width") or adapter.get("mutation_width"),
            "batch_size_multiplier": role.get("batch_size_multiplier") or next_hunt_opening.get("batch_size_multiplier") or adapter.get("batch_size_multiplier"),
            "rationale": "next_hunt_opening_policy_compiler",
        })
    question_planner = state.get("question_driven_hunt_planner") if isinstance(state.get("question_driven_hunt_planner"), dict) else {}
    for question in question_planner.get("worker_questions") or []:
        if not isinstance(question, dict):
            continue
        jobs.append({
            "job_id": question.get("question_id") or _stable_hash({"question_job": question}),
            "worker": question.get("worker") or f"worker_{(len(jobs) % max_workers) + 1}",
            "action": question.get("action") or "answer_hunt_question",
            "route_key": question.get("route_key") or "",
            "question": question.get("question"),
            "mutation_width": question.get("mutation_width") or adapter.get("mutation_width"),
            "batch_size_multiplier": adapter.get("batch_size_multiplier"),
            "rationale": "question_driven_hunt_planner",
        })
    sequencer = state.get("adaptive_experiment_sequencer") if isinstance(state.get("adaptive_experiment_sequencer"), dict) else {}
    for step in sequencer.get("steps") or []:
        if not isinstance(step, dict):
            continue
        jobs.append({
            "job_id": step.get("step_id") or _stable_hash({"adaptive_sequence": step}),
            "worker": step.get("worker") or f"worker_{(len(jobs) % max_workers) + 1}",
            "action": step.get("action") or "sequenced_question_probe",
            "route_key": step.get("route_key") or "",
            "mutation_width": step.get("mutation_width") or adapter.get("mutation_width"),
            "batch_size_multiplier": adapter.get("batch_size_multiplier"),
            "rationale": "adaptive_experiment_sequencer",
        })
    epistemic = state.get("worker_epistemic_roles_v2") if isinstance(state.get("worker_epistemic_roles_v2"), dict) else {}
    for assignment in epistemic.get("assignments") or []:
        if not isinstance(assignment, dict):
            continue
        jobs.append({
            "job_id": _stable_hash({"epistemic_role_v2": assignment}),
            "worker": assignment.get("worker") or f"worker_{(len(jobs) % max_workers) + 1}",
            "action": assignment.get("action") or "epistemic_probe",
            "route_key": assignment.get("route_key") or "",
            "mutation_width": assignment.get("mutation_width") or adapter.get("mutation_width"),
            "batch_size_multiplier": adapter.get("batch_size_multiplier"),
            "rationale": f"worker_epistemic_roles_v2::{assignment.get('epistemic_role') or 'role'}",
        })
    hypothesis_compiler = state.get("hunt_hypothesis_compiler") if isinstance(state.get("hunt_hypothesis_compiler"), dict) else {}
    for assignment in hypothesis_compiler.get("worker_assignments") or []:
        if not isinstance(assignment, dict):
            continue
        jobs.append({
            "job_id": _stable_hash({"hunt_hypothesis_compiler": assignment}),
            "worker": assignment.get("worker") or f"worker_{(len(jobs) % max_workers) + 1}",
            "action": assignment.get("action") or "hypothesis_test",
            "route_key": assignment.get("route_key") or "",
            "hypothesis_id": assignment.get("hypothesis_id"),
            "mutation_width": assignment.get("mutation_width") or adapter.get("mutation_width"),
            "batch_size_multiplier": adapter.get("batch_size_multiplier"),
            "rationale": "hunt_hypothesis_compiler",
        })
    scientific = state.get("scientific_hunt_executive") if isinstance(state.get("scientific_hunt_executive"), dict) else {}
    for command in scientific.get("commands") or []:
        if not isinstance(command, dict):
            continue
        jobs.append({
            "job_id": command.get("command_id") or _stable_hash({"scientific_hunt_executive": command}),
            "worker": command.get("worker") or f"worker_{(len(jobs) % max_workers) + 1}",
            "action": command.get("action") or "run_controlled_contract",
            "route_key": command.get("route_key") or "",
            "control_route": command.get("control_route"),
            "contract_id": command.get("contract_id"),
            "mutation_width": command.get("mutation_width") or adapter.get("mutation_width"),
            "batch_size_multiplier": command.get("batch_size_multiplier") or adapter.get("batch_size_multiplier"),
            "rationale": "scientific_hunt_executive",
        })
    contracts = state.get("experiment_contract_compiler") if isinstance(state.get("experiment_contract_compiler"), dict) else {}
    for contract in contracts.get("contracts") or []:
        if not isinstance(contract, dict):
            continue
        jobs.append({
            "job_id": _stable_hash({"experiment_contract_compiler": contract.get("contract_id")}),
            "worker": contract.get("worker") or f"worker_{(len(jobs) % max_workers) + 1}",
            "action": "run_experiment_contract",
            "route_key": contract.get("route_key") or "",
            "contract_id": contract.get("contract_id"),
            "mutation_width": (contract.get("treatment") or {}).get("mutation_width") or adapter.get("mutation_width"),
            "batch_size_multiplier": adapter.get("batch_size_multiplier"),
            "rationale": "experiment_contract_compiler",
        })
    gap_router = state.get("evidence_gap_router") if isinstance(state.get("evidence_gap_router"), dict) else {}
    for task in gap_router.get("tasks") or []:
        if not isinstance(task, dict):
            continue
        jobs.append({
            "job_id": task.get("task_id") or _stable_hash({"evidence_gap_router": task}),
            "worker": task.get("worker") or f"worker_{(len(jobs) % max_workers) + 1}",
            "action": task.get("action") or "fill_promotion_evidence_gap",
            "route_key": task.get("route_key") or "",
            "variant": task.get("variant"),
            "gap": task.get("gap"),
            "mutation_width": task.get("mutation_width") or adapter.get("mutation_width"),
            "batch_size_multiplier": adapter.get("batch_size_multiplier"),
            "rationale": "evidence_gap_router",
        })
    packet_exec = state.get("promotion_packet_executive") if isinstance(state.get("promotion_packet_executive"), dict) else {}
    for command in packet_exec.get("commands") or []:
        if not isinstance(command, dict):
            continue
        jobs.append({
            "job_id": command.get("command_id") or _stable_hash({"promotion_packet_executive": command}),
            "worker": command.get("worker") or f"worker_{(len(jobs) % max_workers) + 1}",
            "action": command.get("action") or "promotion_packet_repair",
            "route_key": command.get("route_key") or "",
            "variant": command.get("variant"),
            "mutation_width": command.get("mutation_width") or adapter.get("mutation_width"),
            "batch_size_multiplier": adapter.get("batch_size_multiplier"),
            "rationale": "promotion_packet_executive",
        })
    for assignment in adapter.get("ab_assignments") or []:
        if not isinstance(assignment, dict):
            continue
        jobs.append({
            "job_id": assignment.get("assignment_id") or _stable_hash({"ab_assignment": assignment}),
            "worker": assignment.get("worker") or f"worker_{(len(jobs) % max_workers) + 1}",
            "action": "experiment_arm",
            "route_key": assignment.get("route_key") or "",
            "experiment_id": assignment.get("experiment_id"),
            "treatment": assignment.get("treatment"),
            "mutation_width": adapter.get("mutation_width"),
            "batch_size_multiplier": assignment.get("mutation_scale_multiplier") or adapter.get("batch_size_multiplier"),
            "rationale": f"A/B {assignment.get('arm') or 'arm'}: {assignment.get('treatment') or 'treatment'}",
        })
    for slot in adapter.get("champion_challenger_slots") or []:
        if not isinstance(slot, dict):
            continue
        command = slot.get("command") if isinstance(slot.get("command"), dict) else {}
        jobs.append({
            "job_id": _stable_hash({"slot": slot.get("slot"), "command": command}),
            "worker": slot.get("worker") or f"worker_{(len(jobs) % max_workers) + 1}",
            "action": slot.get("slot") or command.get("action") or "probe",
            "route_key": command.get("route_key") or "",
            "mutation_width": command.get("mutation_width") or adapter.get("mutation_width"),
            "batch_size_multiplier": command.get("batch_size_multiplier") or adapter.get("batch_size_multiplier"),
            "rationale": slot.get("objective") or command.get("reason") or "champion_challenger_runtime_slot",
        })
    for intervention in adapter.get("recovery_interventions") or []:
        if not isinstance(intervention, dict):
            continue
        jobs.append({
            "job_id": intervention.get("intervention_id") or _stable_hash({"recovery": intervention}),
            "worker": f"worker_{(len(jobs) % max_workers) + 1}",
            "action": intervention.get("action") or "recovery_probe",
            "route_key": intervention.get("route_key") or "",
            "mutation_width": intervention.get("mutation_width") or adapter.get("mutation_width"),
            "batch_size_multiplier": adapter.get("batch_size_multiplier"),
            "rationale": intervention.get("reason") or "recovery_playbook_intervention",
        })
    bet_sizer = state.get("real_time_bet_sizer") if isinstance(state.get("real_time_bet_sizer"), dict) else {}
    for allocation in bet_sizer.get("allocations") or []:
        if not isinstance(allocation, dict):
            continue
        jobs.append({
            "job_id": _stable_hash({"hypothesis_bet": allocation}),
            "worker": allocation.get("worker") or f"worker_{(len(jobs) % max_workers) + 1}",
            "action": "hypothesis_bet",
            "route_key": allocation.get("route_key") or "",
            "treatment": allocation.get("treatment"),
            "mutation_width": allocation.get("mutation_width") or adapter.get("mutation_width"),
            "batch_size_multiplier": allocation.get("batch_size_multiplier") or adapter.get("batch_size_multiplier"),
            "rationale": allocation.get("reason") or "real_time_bet_sizer",
        })
    contrarian = state.get("contrarian_generator") if isinstance(state.get("contrarian_generator"), dict) else {}
    for experiment in contrarian.get("experiments") or []:
        if not isinstance(experiment, dict):
            continue
        jobs.append({
            "job_id": experiment.get("experiment_id") or _stable_hash({"contrarian": experiment}),
            "worker": f"worker_{(len(jobs) % max_workers) + 1}",
            "action": experiment.get("contrarian_action") or "contrarian_probe",
            "route_key": experiment.get("route_key") or "",
            "treatment": experiment.get("treatment"),
            "mutation_width": experiment.get("mutation_width") or adapter.get("mutation_width"),
            "batch_size_multiplier": adapter.get("batch_size_multiplier"),
            "rationale": experiment.get("reason") or "contrarian_generator",
        })
    recipes = state.get("pattern_to_recipe_compiler") if isinstance(state.get("pattern_to_recipe_compiler"), dict) else {}
    for recipe in recipes.get("recipes") or []:
        if not isinstance(recipe, dict):
            continue
        jobs.append({
            "job_id": recipe.get("recipe_id") or _stable_hash({"recipe": recipe}),
            "worker": f"worker_{(len(jobs) % max_workers) + 1}",
            "action": "recipe_probe",
            "route_key": recipe.get("route_key") or "",
            "treatment": recipe.get("treatment"),
            "mutation_width": recipe.get("mutation_width") or adapter.get("mutation_width"),
            "batch_size_multiplier": adapter.get("batch_size_multiplier"),
            "rationale": "pattern_to_recipe_compiler",
        })
    budget = state.get("budget_reallocator") if isinstance(state.get("budget_reallocator"), dict) else {}
    for assignment in budget.get("assignments") or []:
        if not isinstance(assignment, dict):
            continue
        jobs.append({
            "job_id": _stable_hash({"budget_reallocator": assignment}),
            "worker": assignment.get("worker") or f"worker_{(len(jobs) % max_workers) + 1}",
            "action": assignment.get("action") or "redeploy_probe",
            "route_key": assignment.get("route_key") or "",
            "mutation_width": assignment.get("mutation_width") or adapter.get("mutation_width"),
            "batch_size_multiplier": assignment.get("batch_size_multiplier") or adapter.get("batch_size_multiplier"),
            "rationale": "budget_reallocator",
        })
    novelty = state.get("novelty_budget_governor") if isinstance(state.get("novelty_budget_governor"), dict) else {}
    for assignment in novelty.get("assignments") or []:
        if not isinstance(assignment, dict):
            continue
        jobs.append({
            "job_id": _stable_hash({"novelty_budget": assignment}),
            "worker": assignment.get("worker") or f"worker_{(len(jobs) % max_workers) + 1}",
            "action": assignment.get("action") or "creative_probe",
            "route_key": assignment.get("route_key") or "",
            "treatment": assignment.get("treatment"),
            "mutation_width": assignment.get("mutation_width") or adapter.get("mutation_width"),
            "batch_size_multiplier": adapter.get("batch_size_multiplier"),
            "rationale": "novelty_budget_governor",
        })
    ablation = state.get("module_ablation_planner") if isinstance(state.get("module_ablation_planner"), dict) else {}
    for test in ablation.get("tests") or []:
        if not isinstance(test, dict):
            continue
        jobs.append({
            "job_id": test.get("test_id") or _stable_hash({"module_ablation": test}),
            "worker": f"worker_{(len(jobs) % max_workers) + 1}",
            "action": "module_ablation_shadow",
            "route_key": ((test.get("focus_routes") or [""])[0] or ""),
            "mutation_width": adapter.get("mutation_width"),
            "batch_size_multiplier": min(0.75, float(adapter.get("batch_size_multiplier") or 1.0)),
            "rationale": f"module_ablation_planner::{test.get('module_id') or 'module'}",
        })
    scheduler = state.get("causal_intervention_scheduler") if isinstance(state.get("causal_intervention_scheduler"), dict) else {}
    for intervention in scheduler.get("interventions") or []:
        if not isinstance(intervention, dict):
            continue
        jobs.append({
            "job_id": intervention.get("intervention_id") or _stable_hash({"causal_intervention": intervention}),
            "worker": f"worker_{(len(jobs) % max_workers) + 1}",
            "action": intervention.get("action") or "causal_intervention_probe",
            "route_key": intervention.get("route_key") or "",
            "treatment": intervention.get("treatment"),
            "mutation_width": adapter.get("mutation_width"),
            "batch_size_multiplier": adapter.get("batch_size_multiplier"),
            "rationale": f"causal_intervention_scheduler::{intervention.get('source') or 'source'}",
        })
    fragility = state.get("winner_fragility_profiler") if isinstance(state.get("winner_fragility_profiler"), dict) else {}
    for test in fragility.get("stress_tests") or []:
        if not isinstance(test, dict):
            continue
        jobs.append({
            "job_id": test.get("test_id") or _stable_hash({"fragility": test}),
            "worker": f"worker_{(len(jobs) % max_workers) + 1}",
            "action": test.get("action") or "winner_fragility_stress",
            "route_key": test.get("route_key") or "",
            "variant": test.get("variant"),
            "mutation_width": test.get("mutation_width") or "tight",
            "batch_size_multiplier": min(0.85, float(adapter.get("batch_size_multiplier") or 1.0)),
            "rationale": "winner_fragility_profiler",
        })
    strategy_v2 = state.get("hunt_strategy_compiler_v2") if isinstance(state.get("hunt_strategy_compiler_v2"), dict) else {}
    for job in strategy_v2.get("jobs") or []:
        if not isinstance(job, dict):
            continue
        jobs.append({
            "job_id": job.get("job_id") or _stable_hash({"strategy_v2": job}),
            "worker": f"worker_{(len(jobs) % max_workers) + 1}",
            "action": job.get("action") or "strategy_v2_probe",
            "route_key": job.get("route_key") or "",
            "mutation_width": job.get("mutation_width") or adapter.get("mutation_width"),
            "batch_size_multiplier": job.get("batch_size_multiplier") or adapter.get("batch_size_multiplier"),
            "rationale": job.get("rationale") or "hunt_strategy_compiler_v2",
        })
    court = state.get("learning_disagreement_court") if isinstance(state.get("learning_disagreement_court"), dict) else {}
    for case in court.get("cases") or []:
        if not isinstance(case, dict):
            continue
        if case.get("ruling") not in {"retest", "challenge"}:
            continue
        jobs.append({
            "job_id": case.get("case_id") or _stable_hash({"learning_court": case}),
            "worker": f"worker_{(len(jobs) % max_workers) + 1}",
            "action": "learning_court_retest",
            "route_key": case.get("route_key") or "",
            "mutation_width": "tight",
            "batch_size_multiplier": min(0.85, float(adapter.get("batch_size_multiplier") or 1.0)),
            "rationale": f"learning_disagreement_court::{case.get('ruling') or 'ruling'}",
        })
    objective = state.get("promotion_aware_search_objective") if isinstance(state.get("promotion_aware_search_objective"), dict) else {}
    for row in (objective.get("leaderboard") or [])[:4]:
        if not isinstance(row, dict):
            continue
        jobs.append({
            "job_id": _stable_hash({"promotion_aware_probe": row.get("variant"), "route": row.get("route_key")}),
            "worker": f"worker_{(len(jobs) % max_workers) + 1}",
            "action": "promotion_aware_probe",
            "route_key": row.get("route_key") or "",
            "variant": row.get("variant"),
            "mutation_width": adapter.get("mutation_width"),
            "batch_size_multiplier": adapter.get("batch_size_multiplier"),
            "rationale": "promotion_aware_search_objective",
        })
    mutations = state.get("strategy_mutation_engine") if isinstance(state.get("strategy_mutation_engine"), dict) else {}
    for challenger in mutations.get("challengers") or []:
        if not isinstance(challenger, dict):
            continue
        jobs.append({
            "job_id": challenger.get("genome_id") or _stable_hash({"strategy_challenger": challenger}),
            "worker": f"worker_{(len(jobs) % max_workers) + 1}",
            "action": "strategy_challenger_probe",
            "route_key": ((challenger.get("focus_routes") or [""])[0] or ""),
            "strategy_name": challenger.get("strategy_name"),
            "mutation_width": challenger.get("mutation_width") or adapter.get("mutation_width"),
            "batch_size_multiplier": challenger.get("batch_size_multiplier") or adapter.get("batch_size_multiplier"),
            "rationale": challenger.get("mutation_reason") or "strategy_mutation_engine",
        })
    exploration = state.get("exploration_debt_ledger") if isinstance(state.get("exploration_debt_ledger"), dict) else {}
    for debt in (exploration.get("debts") or [])[:8]:
        if not isinstance(debt, dict):
            continue
        jobs.append({
            "job_id": _stable_hash({"exploration_debt": debt}),
            "worker": f"worker_{(len(jobs) % max_workers) + 1}",
            "action": debt.get("recommended_action") or "exploration_debt_probe",
            "route_key": debt.get("route_key") or "",
            "mutation_width": "wide",
            "batch_size_multiplier": adapter.get("batch_size_multiplier"),
            "rationale": "exploration_debt_ledger",
        })
    red_team = state.get("adversarial_strategy_red_team") if isinstance(state.get("adversarial_strategy_red_team"), dict) else {}
    for test in red_team.get("counter_tests") or []:
        if not isinstance(test, dict):
            continue
        jobs.append({
            "job_id": test.get("test_id") or _stable_hash({"strategy_red_team": test}),
            "worker": f"worker_{(len(jobs) % max_workers) + 1}",
            "action": test.get("action") or "strategy_red_team_counter_test",
            "route_key": "",
            "mutation_width": test.get("mutation_width") or "tight",
            "batch_size_multiplier": min(0.82, float(adapter.get("batch_size_multiplier") or 1.0)),
            "rationale": "adversarial_strategy_red_team",
        })
    pivot = state.get("autonomous_pivot_governor") if isinstance(state.get("autonomous_pivot_governor"), dict) else {}
    if pivot.get("pivot"):
        jobs.append({
            "job_id": _stable_hash({"autonomous_pivot": pivot.get("pivot_reason"), "strategy": pivot.get("selected_strategy")}),
            "worker": f"worker_{(len(jobs) % max_workers) + 1}",
            "action": "strategy_pivot",
            "route_key": ((pivot.get("focus_routes") or [""])[0] or ""),
            "mutation_width": pivot.get("mutation_width") or adapter.get("mutation_width"),
            "batch_size_multiplier": pivot.get("batch_size_multiplier") or adapter.get("batch_size_multiplier"),
            "rationale": f"autonomous_pivot_governor::{pivot.get('pivot_reason') or 'pivot'}",
        })
    worker_personalities = state.get("adaptive_worker_personalities") if isinstance(state.get("adaptive_worker_personalities"), dict) else {}
    for assignment in worker_personalities.get("assignments") or []:
        if not isinstance(assignment, dict):
            continue
        jobs.append({
            "job_id": _stable_hash({"adaptive_worker_personality": assignment}),
            "worker": assignment.get("worker") or f"worker_{(len(jobs) % max_workers) + 1}",
            "action": assignment.get("action") or "personality_probe",
            "route_key": assignment.get("route_key") or "",
            "mutation_width": assignment.get("mutation_width") or adapter.get("mutation_width"),
            "batch_size_multiplier": assignment.get("batch_size_multiplier") or adapter.get("batch_size_multiplier"),
            "rationale": f"adaptive_worker_personalities::{assignment.get('personality') or 'worker'}",
        })
    bandit = state.get("online_causal_bandit") if isinstance(state.get("online_causal_bandit"), dict) else {}
    for allocation in bandit.get("allocations") or []:
        if not isinstance(allocation, dict):
            continue
        route = ((allocation.get("focus_routes") or [""])[0] or "")
        jobs.append({
            "job_id": _stable_hash({"online_causal_bandit": allocation}),
            "worker": allocation.get("worker") or f"worker_{(len(jobs) % max_workers) + 1}",
            "action": allocation.get("recommended_action") or "causal_lane_probe",
            "route_key": route,
            "lane": allocation.get("lane"),
            "mutation_width": adapter.get("mutation_width"),
            "batch_size_multiplier": adapter.get("batch_size_multiplier"),
            "rationale": "online_causal_bandit",
        })
    rejection_v2 = state.get("promotion_rejection_predictor_v2") if isinstance(state.get("promotion_rejection_predictor_v2"), dict) else {}
    for row in rejection_v2.get("repair_variants") or []:
        if not isinstance(row, dict):
            continue
        jobs.append({
            "job_id": _stable_hash({"promotion_rejection_v2_repair": row}),
            "worker": f"worker_{(len(jobs) % max_workers) + 1}",
            "action": "promotion_rejection_repair",
            "route_key": row.get("route_key") or "",
            "variant": row.get("variant"),
            "mutation_width": "tight",
            "batch_size_multiplier": min(0.82, float(adapter.get("batch_size_multiplier") or 1.0)),
            "rationale": "promotion_rejection_predictor_v2",
        })
    autopilot = state.get("hunt_autopilot_policy_compiler") if isinstance(state.get("hunt_autopilot_policy_compiler"), dict) else {}
    for assignment in autopilot.get("worker_policy") or []:
        if not isinstance(assignment, dict):
            continue
        jobs.append({
            "job_id": _stable_hash({"autopilot_policy": assignment}),
            "worker": assignment.get("worker") or f"worker_{(len(jobs) % max_workers) + 1}",
            "action": assignment.get("role") or "autopilot_probe",
            "route_key": assignment.get("route_key") or "",
            "template": assignment.get("template"),
            "mutation_width": assignment.get("mutation_width") or adapter.get("mutation_width"),
            "batch_size_multiplier": adapter.get("batch_size_multiplier"),
            "rationale": "hunt_autopilot_policy_compiler",
        })
    promotion_autopilot = state.get("promotion_first_autopilot_v2") if isinstance(state.get("promotion_first_autopilot_v2"), dict) else {}
    for assignment in promotion_autopilot.get("worker_policy") or []:
        if not isinstance(assignment, dict):
            continue
        jobs.append({
            "job_id": _stable_hash({"promotion_first_autopilot_v2": assignment}),
            "worker": assignment.get("worker") or f"worker_{(len(jobs) % max_workers) + 1}",
            "action": assignment.get("role") or "promotion_first_probe",
            "route_key": assignment.get("route_key") or "",
            "mutation_width": promotion_autopilot.get("mutation_width") or adapter.get("mutation_width"),
            "batch_size_multiplier": promotion_autopilot.get("batch_size_multiplier") or adapter.get("batch_size_multiplier"),
            "rationale": "promotion_first_autopilot_v2",
        })
    ablation_replay = state.get("ablation_replay_harness") if isinstance(state.get("ablation_replay_harness"), dict) else {}
    for test in ablation_replay.get("tests") or []:
        if not isinstance(test, dict):
            continue
        jobs.append({
            "job_id": test.get("test_id") or _stable_hash({"ablation_replay": test}),
            "worker": f"worker_{(len(jobs) % max_workers) + 1}",
            "action": test.get("action") or "shadow_replay_without_layer",
            "route_key": "",
            "layer": test.get("layer"),
            "mutation_width": "tight",
            "batch_size_multiplier": min(0.70, float(adapter.get("batch_size_multiplier") or 1.0)),
            "rationale": "ablation_replay_harness",
        })
    meta_governor = state.get("meta_learning_governor") if isinstance(state.get("meta_learning_governor"), dict) else {}
    for command in meta_governor.get("commands") or []:
        if not isinstance(command, dict):
            continue
        jobs.append({
            "job_id": command.get("command_id") or _stable_hash({"meta_learning_governor": command}),
            "worker": command.get("worker") or f"worker_{(len(jobs) % max_workers) + 1}",
            "action": command.get("action") or "meta_governed_probe",
            "route_key": command.get("route_key") or "",
            "mutation_width": command.get("mutation_width") or adapter.get("mutation_width"),
            "batch_size_multiplier": command.get("batch_size_multiplier") or adapter.get("batch_size_multiplier"),
            "rationale": "meta_learning_governor",
        })
    repair_recipes = state.get("candidate_repair_recipe_generator") if isinstance(state.get("candidate_repair_recipe_generator"), dict) else {}
    for recipe in repair_recipes.get("recipes") or []:
        if not isinstance(recipe, dict):
            continue
        jobs.append({
            "job_id": recipe.get("recipe_id") or _stable_hash({"candidate_repair_recipe_generator": recipe}),
            "worker": recipe.get("worker") or f"worker_{(len(jobs) % max_workers) + 1}",
            "action": recipe.get("action") or "candidate_repair_recipe",
            "route_key": recipe.get("route_key") or "",
            "variant": recipe.get("variant"),
            "mutation_width": recipe.get("mutation_width") or adapter.get("mutation_width"),
            "batch_size_multiplier": min(0.92, float(adapter.get("batch_size_multiplier") or 1.0)),
            "rationale": "candidate_repair_recipe_generator",
        })
    budget_optimizer = state.get("learning_budget_optimizer") if isinstance(state.get("learning_budget_optimizer"), dict) else {}
    for allocation in (budget_optimizer.get("allocations") or [])[:4]:
        if not isinstance(allocation, dict):
            continue
        route = ((budget_optimizer.get("focus_routes") or [""])[0] or "")
        jobs.append({
            "job_id": _stable_hash({"learning_budget_optimizer": allocation}),
            "worker": f"worker_{(len(jobs) % max_workers) + 1}",
            "action": allocation.get("action") or "learning_budget_probe",
            "route_key": route,
            "budget_bucket": allocation.get("bucket"),
            "mutation_width": adapter.get("mutation_width"),
            "batch_size_multiplier": adapter.get("batch_size_multiplier"),
            "rationale": "learning_budget_optimizer",
        })
    breakthrough = state.get("breakthrough_escalation_protocol") if isinstance(state.get("breakthrough_escalation_protocol"), dict) else {}
    for command in breakthrough.get("commands") or []:
        if not isinstance(command, dict):
            continue
        jobs.append({
            "job_id": command.get("command_id") or _stable_hash({"breakthrough_escalation_protocol": command}),
            "worker": command.get("worker") or f"worker_{(len(jobs) % max_workers) + 1}",
            "action": command.get("action") or "breakthrough_escalation_review",
            "route_key": command.get("route_key") or "",
            "variant": command.get("variant"),
            "mutation_width": command.get("mutation_width") or "tight",
            "batch_size_multiplier": min(0.90, float(adapter.get("batch_size_multiplier") or 1.0)),
            "rationale": "breakthrough_escalation_protocol",
        })
    elite_summary = state.get("elite_learning_system_summary") if isinstance(state.get("elite_learning_system_summary"), dict) else {}
    elite_actions = list(elite_summary.get("top_actions") or [])
    next_question = state.get("world_state_next_best_question_engine") if isinstance(state.get("world_state_next_best_question_engine"), dict) else {}
    if next_question.get("top_record"):
        elite_actions.insert(0, {"artifact": "world_state_next_best_question_engine", **(next_question.get("top_record") or {})})
    for action in elite_actions[:8]:
        if not isinstance(action, dict):
            continue
        route = str(action.get("route_key") or "")
        jobs.append({
            "job_id": _stable_hash({"elite_learning_action": action}),
            "worker": f"worker_{(len(jobs) % max_workers) + 1}",
            "action": action.get("action") or action.get("recommended_action") or "elite_learning_probe",
            "route_key": route,
            "artifact": action.get("artifact"),
            "mutation_width": adapter.get("mutation_width"),
            "batch_size_multiplier": adapter.get("batch_size_multiplier"),
            "rationale": "elite_learning_system_summary",
        })
    proof_summary = state.get("proof_learning_system_summary") if isinstance(state.get("proof_learning_system_summary"), dict) else {}
    proof_actions = list(proof_summary.get("top_actions") or [])
    for action in proof_actions[:8]:
        if not isinstance(action, dict):
            continue
        jobs.append({
            "job_id": _stable_hash({"proof_learning_action": action}),
            "worker": f"worker_{(len(jobs) % max_workers) + 1}",
            "action": action.get("action") or action.get("recommended_action") or "proof_learning_probe",
            "route_key": action.get("route_key") or "",
            "artifact": action.get("artifact"),
            "mutation_width": adapter.get("mutation_width"),
            "batch_size_multiplier": adapter.get("batch_size_multiplier"),
            "rationale": "proof_learning_system_summary",
        })
    control_summary = state.get("closed_loop_control_learning_summary") if isinstance(state.get("closed_loop_control_learning_summary"), dict) else {}
    control_actions = list(control_summary.get("top_actions") or [])
    for action in control_actions[:8]:
        if not isinstance(action, dict):
            continue
        jobs.append({
            "job_id": _stable_hash({"closed_loop_control_action": action}),
            "worker": f"worker_{(len(jobs) % max_workers) + 1}",
            "action": action.get("action") or action.get("recommended_action") or "closed_loop_control_probe",
            "route_key": action.get("route_key") or "",
            "artifact": action.get("artifact"),
            "mutation_width": adapter.get("mutation_width"),
            "batch_size_multiplier": adapter.get("batch_size_multiplier"),
            "rationale": "closed_loop_control_learning_summary",
        })
    nervous_summary = state.get("world_model_nervous_system_summary") if isinstance(state.get("world_model_nervous_system_summary"), dict) else {}
    nervous_actions = list(nervous_summary.get("top_actions") or [])
    for action in nervous_actions[:10]:
        if not isinstance(action, dict):
            continue
        jobs.append({
            "job_id": _stable_hash({"world_model_nervous_action": action}),
            "worker": f"worker_{(len(jobs) % max_workers) + 1}",
            "action": action.get("action") or action.get("recommended_action") or "world_model_nervous_probe",
            "route_key": action.get("route_key") or "",
            "artifact": action.get("artifact"),
            "kind": action.get("kind"),
            "mutation_width": adapter.get("mutation_width"),
            "batch_size_multiplier": adapter.get("batch_size_multiplier"),
            "rationale": "world_model_nervous_system_summary",
        })
    orchestration_summary = state.get("orchestration_learning_summary") if isinstance(state.get("orchestration_learning_summary"), dict) else {}
    orchestration_actions = list(orchestration_summary.get("top_actions") or [])
    for action in orchestration_actions[:10]:
        if not isinstance(action, dict):
            continue
        jobs.append({
            "job_id": _stable_hash({"orchestration_action": action}),
            "worker": f"worker_{(len(jobs) % max_workers) + 1}",
            "action": action.get("action") or action.get("recommended_action") or "orchestration_probe",
            "route_key": action.get("route_key") or "",
            "artifact": action.get("artifact"),
            "kind": action.get("kind"),
            "mutation_width": adapter.get("mutation_width"),
            "batch_size_multiplier": adapter.get("batch_size_multiplier"),
            "rationale": "orchestration_learning_summary",
        })
    fitness_summary = state.get("fitness_selection_summary") if isinstance(state.get("fitness_selection_summary"), dict) else {}
    fitness_actions = list(fitness_summary.get("top_actions") or [])
    for action in fitness_actions[:8]:
        if not isinstance(action, dict):
            continue
        jobs.append({
            "job_id": _stable_hash({"fitness_selection_action": action}),
            "worker": f"worker_{(len(jobs) % max_workers) + 1}",
            "action": action.get("action") or action.get("recommended_action") or "fitness_selection_probe",
            "route_key": action.get("route_key") or "",
            "artifact": action.get("artifact"),
            "mutation_width": adapter.get("mutation_width"),
            "batch_size_multiplier": adapter.get("batch_size_multiplier"),
            "rationale": "fitness_selection_summary",
        })
    opening = state.get("hunt_opening_playbook") if isinstance(state.get("hunt_opening_playbook"), dict) else {}
    for role in opening.get("worker_roles") or []:
        if not isinstance(role, dict):
            continue
        jobs.append({
            "job_id": _stable_hash({"opening_role": role}),
            "worker": role.get("worker") or f"worker_{(len(jobs) % max_workers) + 1}",
            "action": role.get("role") or "opening_role",
            "route_key": role.get("route_key") or "",
            "mutation_width": role.get("mutation_width") or adapter.get("mutation_width"),
            "batch_size_multiplier": adapter.get("batch_size_multiplier"),
            "rationale": f"pre_hunt_opening_playbook::{opening.get('strategy') or 'strategy'}",
        })
    if not jobs:
        for idx, command in enumerate(adapter.get("commands") or []):
            jobs.append({
                "job_id": command.get("command_id") or _stable_hash({"worker_job": idx, "command": command}),
                "worker": f"worker_{(idx % max_workers) + 1}",
                "action": command.get("action") or "probe",
                "route_key": command.get("route_key") or "",
                "mutation_width": command.get("mutation_width") or adapter.get("mutation_width"),
                "batch_size_multiplier": command.get("batch_size_multiplier") or adapter.get("batch_size_multiplier"),
                "rationale": command.get("reason") or "runtime_command_adapter",
            })
    if not jobs and adapter.get("focus_routes"):
        for idx, route in enumerate(adapter.get("focus_routes")[:max_workers]):
            jobs.append({
                "job_id": _stable_hash({"focus_probe": route}),
                "worker": f"worker_{idx + 1}",
                "action": "probe",
                "route_key": route,
                "mutation_width": adapter.get("mutation_width"),
                "batch_size_multiplier": adapter.get("batch_size_multiplier"),
                "rationale": "focus_route_probe",
            })
    contracts = []
    workers = [f"worker_{idx + 1}" for idx in range(min(max_workers, max(1, int(adapter.get("max_workers") or max_workers))))]
    for idx, worker in enumerate(workers):
        worker_jobs = [job for job in jobs if str(job.get("worker") or "") == worker]
        if not worker_jobs and jobs:
            worker_jobs = [jobs[idx % len(jobs)]]
        visible_jobs = worker_jobs[:3]
        priority_labels = ("experiment_arm", "scientific_hunt_executive", "promotion_packet_executive", "meta_learning_governor", "candidate_repair_recipe_generator", "elite_learning_system_summary", "proof_learning_system_summary", "closed_loop_control_learning_summary", "world_model_nervous_system_summary", "orchestration_learning_summary", "fitness_selection_summary")
        for priority_action in priority_labels:
            priority_job = next((
                job for job in worker_jobs
                if job.get("action") == priority_action or job.get("rationale") == priority_action
            ), None)
            if priority_job and priority_job not in visible_jobs:
                if len(visible_jobs) >= 3:
                    replace_idx = next((
                        idx for idx, job in enumerate(visible_jobs)
                        if job.get("action") not in priority_labels and job.get("rationale") not in priority_labels
                    ), len(visible_jobs) - 1)
                    visible_jobs[replace_idx] = priority_job
                else:
                    visible_jobs.append(priority_job)
        contracts.append({
            "worker": worker,
            "live_only": True,
            "action": (worker_jobs[0] if worker_jobs else {}).get("action") or "probe",
            "route_key": (worker_jobs[0] if worker_jobs else {}).get("route_key") or "",
            "mutation_width": (worker_jobs[0] if worker_jobs else {}).get("mutation_width") or adapter.get("mutation_width"),
            "batch_size_multiplier": (worker_jobs[0] if worker_jobs else {}).get("batch_size_multiplier") or adapter.get("batch_size_multiplier"),
            "focus_routes": adapter.get("focus_routes") or [],
            "avoid_routes": adapter.get("avoid_routes") or [],
            "guardrails": {
                "only_keep_live_beaters": True,
                "max_workers": min(4, int(adapter.get("max_workers") or 4)),
                "degrade_mode": bool(adapter.get("degrade_mode")),
            },
            "jobs": visible_jobs,
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Per-worker contract translating planner commands into live-only hunt responsibilities.",
        "contract_count": len(contracts),
        "contracts": contracts,
    }


def command_delta_status(previous_state: dict[str, Any] | None, state: dict[str, Any]) -> dict[str, Any]:
    previous_packet = active_runtime_command_packet(previous_state or {}) if previous_state else {}
    current_packet = active_runtime_command_packet(state or {})
    previous_commands = previous_packet.get("commands") or []
    current_commands = current_packet.get("commands") or []
    prev_ids = {str(row.get("command_id") or _stable_hash(row)) for row in previous_commands if isinstance(row, dict)}
    cur_ids = {str(row.get("command_id") or _stable_hash(row)) for row in current_commands if isinstance(row, dict)}
    changed_fields = []
    for field in ("mutation_width", "route_breadth", "batch_size_multiplier", "focus_routes", "avoid_routes"):
        if previous_packet.get(field) != current_packet.get(field):
            changed_fields.append(field)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Shows how the executable command packet changed since the prior cycle.",
        "changed": bool(changed_fields or prev_ids != cur_ids),
        "changed_fields": changed_fields,
        "added_command_ids": sorted(cur_ids - prev_ids),
        "removed_command_ids": sorted(prev_ids - cur_ids),
        "current_primary_action": ((current_commands or [{}])[0] or {}).get("action"),
        "current_focus_routes": list(current_packet.get("focus_routes") or [])[:8],
        "degrade_mode": bool(current_packet.get("degraded")),
    }


def command_outcome_backfill(
    previous_state: dict[str, Any] | None,
    rankings: dict[str, Any],
    cycles: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    previous_state = previous_state or {}
    packet = active_runtime_command_packet(previous_state)
    rows = list(rankings.get("raw_leaderboard") or rankings.get("promotion_readiness_leaderboard") or [])
    recent = list(cycles or [])[-3:]
    scored = sum(hunt_intel._cycle_scored_total(cycle) for cycle in recent)
    winners = sum(hunt_intel._cycle_winners(cycle) for cycle in recent)
    previous_best = max([float(row.get("step2_pnl") or 0.0) for row in (previous_state.get("top_candidates") or [])[:10]] or [0.0])
    global_best = max([float(row.get("step2_pnl") or 0.0) for row in rows[:20]] or [0.0])
    outcomes = []
    for command in packet.get("commands") or []:
        if not isinstance(command, dict):
            continue
        route = str(command.get("route_key") or "")
        route_rows = [row for row in rows if route and hunt_intel.route_key_from_row(row) == route]
        route_best = max([float(row.get("step2_pnl") or 0.0) for row in route_rows[:20]] or [0.0])
        route_delta = max([float(row.get("step2_delta_vs_active") or 0.0) for row in route_rows[:20]] or [0.0])
        action = str(command.get("action") or "probe")
        avoided_bad_route = action == "quarantine" and not route_rows
        reward = route_delta * 0.02 + max(0.0, global_best - previous_best) * 0.01 + winners * 2.0
        if action in {"repair", "revalidate"}:
            reward += 3.0 if route_rows else -1.0
        if avoided_bad_route:
            reward += 5.0
        worked = bool(route_delta > 0.0 or global_best > previous_best or avoided_bad_route)
        outcomes.append({
            "command_id": command.get("command_id") or _stable_hash(command),
            "action": action,
            "route_key": route,
            "previous_global_best_pnl": round(previous_best, 4),
            "current_global_best_pnl": round(global_best, 4),
            "route_best_pnl": round(route_best, 4),
            "route_best_delta_vs_active": round(route_delta, 4),
            "recent_scored": int(scored),
            "recent_winners": int(winners),
            "reward_score": round(reward, 4),
            "worked": worked,
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Backfills outcomes for the previous cycle's issued commands using the current leaderboard.",
        "outcomes": outcomes,
        "outcome_count": len(outcomes),
    }


def action_reward_calibration(state: dict[str, Any], outcomes: dict[str, Any] | None = None) -> dict[str, Any]:
    outcome_rows = list((outcomes or {}).get("outcomes") or (state.get("command_outcome_backfill") or {}).get("outcomes") or (state.get("action_outcome_tracker") or {}).get("outcomes") or [])
    predicted = {
        str(row.get("action")): row
        for row in ((state.get("closed_loop_reward_model") or {}).get("action_rewards") or [])
        if isinstance(row, dict) and row.get("action")
    }
    by_action: dict[str, list[float]] = {}
    for row in outcome_rows:
        action = str(row.get("action") or "unknown")
        by_action.setdefault(action, []).append(float(row.get("reward_score") or 0.0))
    rows = []
    for action, observed_values in by_action.items():
        observed = sum(observed_values) / max(1, len(observed_values))
        pred = float((predicted.get(action) or {}).get("avg_reward_score") or 0.0)
        rows.append({
            "action": action,
            "predicted_reward": round(pred, 4),
            "observed_reward": round(observed, 4),
            "error": round(observed - pred, 4),
            "abs_error": round(abs(observed - pred), 4),
            "sample_count": len(observed_values),
            "calibrated_use": "scale" if observed >= 10.0 else "probe" if observed >= 0.0 else "deprioritize",
        })
    rows.sort(key=lambda row: float(row.get("abs_error") or 0.0), reverse=True)
    mae = sum(float(row.get("abs_error") or 0.0) for row in rows) / max(1, len(rows))
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Compares predicted action rewards with backfilled observed rewards.",
        "mean_abs_error": round(mae, 4),
        "rows": rows,
        "worst_calibration": rows[0] if rows else {},
    }


def cross_run_hunt_memory_compiler(global_memory: dict[str, Any] | None = None) -> dict[str, Any]:
    memory = global_memory or {}
    route_priors = list(((memory.get("route_prior_model") or {}).get("priors") or []))
    closed_loop = memory.get("closed_loop_execution_memory") if isinstance(memory.get("closed_loop_execution_memory"), dict) else {}
    runtime_experiments = list(closed_loop.get("runtime_experiments") or [])
    failure = memory.get("failure_memory_feedback") if isinstance(memory.get("failure_memory_feedback"), dict) else {}
    feedback_directives = list(failure.get("directives") or [])
    revalidation = list((memory.get("revalidation_candidates") or {}).get("queue") or [])
    favor_routes = []
    retire_routes = []
    treatment_scores: dict[str, float] = {}
    for prior in route_priors:
        route = str(prior.get("route_key") or "")
        if not route:
            continue
        reject_rate = float(prior.get("reject_rate") or 0.0)
        score = float(prior.get("prior_score") or 0.0)
        if reject_rate >= 0.50 or score < -250.0:
            retire_routes.append(route)
        else:
            favor_routes.append(route)
        radius = str(prior.get("mutation_radius") or "balanced")
        treatment_scores[radius] = treatment_scores.get(radius, 0.0) + max(1.0, score)
    for item in runtime_experiments:
        raw = item.get("raw") if isinstance(item.get("raw"), dict) else {}
        treatment = str(raw.get("treatment") or raw.get("action") or "")
        if treatment:
            treatment_scores[treatment] = treatment_scores.get(treatment, 0.0) + float(item.get("priority") or 0.0)
    for directive in feedback_directives:
        route = str(directive.get("route_key") or "")
        if route and directive.get("action") in {"skip_route", "downweight_route"}:
            retire_routes.append(route)
    compiled = {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Compiled durable cross-run hunt memory for routes, treatments, failures, and runtime experiment results.",
        "favor_routes": _ordered_unique(favor_routes)[:20],
        "retire_routes": _ordered_unique(retire_routes)[:20],
        "revalidate_routes": _ordered_unique([row.get("route_key") for row in revalidation if row.get("route_key")])[:12],
        "treatment_priors": [
            {"treatment": treatment, "cross_run_score": round(score, 4)}
            for treatment, score in sorted(treatment_scores.items(), key=lambda item: item[1], reverse=True)
            if treatment
        ][:12],
        "failure_patterns": feedback_directives[:20],
        "route_prior_count": len(route_priors),
        "runtime_experiment_count": len(runtime_experiments),
    }
    return compiled


def pre_hunt_strategy_selector(compiled_memory: dict[str, Any] | None = None) -> dict[str, Any]:
    compiled = compiled_memory or {}
    treatments = list(compiled.get("treatment_priors") or [])
    revalidate = list(compiled.get("revalidate_routes") or [])
    favor = list(compiled.get("favor_routes") or [])
    retire = list(compiled.get("retire_routes") or [])
    top_treatment = str(((treatments or [{}])[0] or {}).get("treatment") or "")
    strategy = "router_heavy"
    reason = "durable route priors are available"
    if revalidate:
        strategy = "validation_heavy"
        reason = "stale or high-value routes need immediate revalidation"
    elif top_treatment in {"holdout_repair", "day_consistency_repair", "route_narrow", "route_widen"}:
        strategy = "repair_heavy"
        reason = f"cross-run treatment prior favors {top_treatment}"
    elif len(favor) < 3 and not revalidate:
        strategy = "scarcity_discovery"
        reason = "few durable favored routes exist"
    elif len(retire) >= max(3, len(favor)):
        strategy = "champion_challenger"
        reason = "long-run memory has many retired routes, so controlled challengers are safer"
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Selects the opening hunt strategy from durable cross-run memory.",
        "strategy": strategy,
        "reason": reason,
        "top_treatment": top_treatment,
        "recommended_hunters": {
            "router_heavy": ["router", "adaptive", "local"],
            "repair_heavy": ["router", "local", "adaptive"],
            "scarcity_discovery": ["adaptive", "router", "local"],
            "validation_heavy": ["router", "adaptive", "local"],
            "champion_challenger": ["router", "local", "adaptive"],
        }.get(strategy, ["router", "adaptive", "local"]),
    }


def cold_start_route_pack_generator(
    compiled_memory: dict[str, Any] | None = None,
    strategy: dict[str, Any] | None = None,
    *,
    limit: int = 8,
) -> dict[str, Any]:
    compiled = compiled_memory or {}
    strat = (strategy or {}).get("strategy") or "router_heavy"
    routes = []
    routes.extend(compiled.get("revalidate_routes") or [])
    routes.extend(compiled.get("favor_routes") or [])
    if strat == "scarcity_discovery":
        routes = list(compiled.get("favor_routes") or []) + list(compiled.get("revalidate_routes") or [])
    retired = set(str(route) for route in compiled.get("retire_routes") or [])
    routes = [route for route in routes if str(route) not in retired]
    route_pack = []
    for idx, route in enumerate(_ordered_unique(routes)[: max(1, int(limit))], 1):
        route_pack.append({
            "route_key": route,
            "rank": idx,
            "source": "cross_run_memory",
            "opening_action": "revalidate" if route in set(compiled.get("revalidate_routes") or []) else "probe",
            "mutation_width": "wide" if strat == "scarcity_discovery" else "tight" if strat == "validation_heavy" else "medium",
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "First-cycle route pack generated from durable memory so the run starts warm.",
        "route_pack": route_pack,
        "focus_routes": [row.get("route_key") for row in route_pack],
        "avoid_routes": list(compiled.get("retire_routes") or [])[:12],
    }


def longitudinal_treatment_decay(global_memory: dict[str, Any] | None = None, *, decay: float = 0.82) -> dict[str, Any]:
    memory = global_memory or {}
    treatments: dict[str, dict[str, float]] = {}
    for row in ((memory.get("closed_loop_execution_memory") or {}).get("runtime_experiments") or []):
        raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
        treatment = str(raw.get("treatment") or raw.get("action") or row.get("subject") or "")
        if not treatment:
            continue
        item = treatments.setdefault(treatment, {"score": 0.0, "seen": 0.0})
        item["score"] += float(row.get("priority") or 0.0)
        item["seen"] += 1.0
    decayed = []
    for treatment, item in treatments.items():
        seen = max(1.0, item["seen"])
        score = (item["score"] / seen) * (float(decay) ** max(0.0, seen - 1.0))
        decayed.append({
            "treatment": treatment,
            "seen_count": int(seen),
            "decayed_score": round(score, 4),
            "trust": "keep" if score >= 10.0 else "probe" if score >= 0.0 else "decay_out",
        })
    decayed.sort(key=lambda row: float(row.get("decayed_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Decays treatment trust across runs unless treatments keep producing useful evidence.",
        "treatments": decayed[:30],
        "favor_treatments": [row.get("treatment") for row in decayed if row.get("trust") == "keep"][:8],
        "decay_out_treatments": [row.get("treatment") for row in decayed if row.get("trust") == "decay_out"][:8],
    }


def run_level_promotion_survival_feedback(global_memory: dict[str, Any] | None = None) -> dict[str, Any]:
    feedback = (global_memory or {}).get("failure_memory_feedback") if isinstance((global_memory or {}).get("failure_memory_feedback"), dict) else {}
    directives = list(feedback.get("directives") or [])
    route_feedback: dict[str, dict[str, Any]] = {}
    for directive in directives:
        route = str(directive.get("route_key") or "")
        if not route:
            continue
        item = route_feedback.setdefault(route, {"rejects": 0, "reasons": []})
        if directive.get("action") in {"skip_route", "downweight_route", "repair_holdout", "repair_day_consistency"}:
            item["rejects"] += 1
        item["reasons"].extend(directive.get("reasons") or directive.get("top_reasons") or [])
    routes = []
    for route, item in route_feedback.items():
        routes.append({
            "route_key": route,
            "reject_count": int(item["rejects"]),
            "survival_penalty": round(min(1.0, int(item["rejects"]) / 5.0), 4),
            "reasons": item["reasons"][:8],
            "next_action": "repair_before_hunt" if int(item["rejects"]) <= 2 else "avoid_opening",
        })
    routes.sort(key=lambda row: int(row.get("reject_count") or 0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Feeds run-level promotion/review survival outcomes into next-run route priors.",
        "routes": routes[:50],
        "avoid_opening_routes": [row.get("route_key") for row in routes if row.get("next_action") == "avoid_opening"][:12],
        "repair_opening_routes": [row.get("route_key") for row in routes if row.get("next_action") == "repair_before_hunt"][:12],
    }


def memory_provenance_explorer(
    global_memory: dict[str, Any] | None = None,
    compiled_memory: dict[str, Any] | None = None,
) -> dict[str, Any]:
    memory = global_memory or {}
    compiled = compiled_memory or {}
    route_priors = list(((memory.get("route_prior_model") or {}).get("priors") or []))
    experiments = list(((memory.get("causal_experiment_registry") or {}).get("experiments") or []))
    runtime_experiments = list(((memory.get("closed_loop_execution_memory") or {}).get("runtime_experiments") or []))
    feedback = list(((memory.get("failure_memory_feedback") or {}).get("directives") or []))
    subjects = _ordered_unique(
        list(compiled.get("favor_routes") or [])
        + list(compiled.get("retire_routes") or [])
        + list(compiled.get("revalidate_routes") or [])
        + [str(row.get("route_key") or "") for row in route_priors]
    )
    entries = []
    for subject in subjects:
        prior = next((row for row in route_priors if str(row.get("route_key") or "") == subject), {})
        exp_hits = [row for row in experiments if str(row.get("route_key") or "") == subject]
        runtime_hits = [
            row for row in runtime_experiments
            if str((row.get("raw") or {}).get("route_key") or row.get("subject") or "") == subject
        ]
        feedback_hits = [row for row in feedback if str(row.get("route_key") or "") == subject]
        entries.append({
            "subject_type": "route",
            "subject": subject,
            "route_prior": prior,
            "experiment_count": len(exp_hits),
            "runtime_experiment_count": len(runtime_hits),
            "feedback_count": len(feedback_hits),
            "evidence": {
                "route_prior": prior,
                "experiments": exp_hits[:6],
                "runtime_experiments": runtime_hits[:6],
                "feedback": feedback_hits[:6],
            },
            "summary": f"{subject}: prior={round(float(prior.get('prior_score') or 0.0), 4)}, experiments={len(exp_hits)}, feedback={len(feedback_hits)}",
        })
    for treatment in compiled.get("treatment_priors") or []:
        treatment_name = str(treatment.get("treatment") or "")
        if not treatment_name:
            continue
        hits = [
            row for row in runtime_experiments
            if str((row.get("raw") or {}).get("treatment") or (row.get("raw") or {}).get("action") or "") == treatment_name
        ]
        entries.append({
            "subject_type": "treatment",
            "subject": treatment_name,
            "route_prior": {},
            "experiment_count": 0,
            "runtime_experiment_count": len(hits),
            "feedback_count": 0,
            "evidence": {"runtime_experiments": hits[:6]},
            "summary": f"{treatment_name}: cross-run score={treatment.get('cross_run_score')}, runtime experiments={len(hits)}",
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Explains which runs, candidates, experiments, and reviews caused durable memory to be trusted.",
        "entries": entries[:150],
        "subjects_with_provenance": [row.get("subject") for row in entries if row.get("evidence")][:100],
    }


def memory_reliability_scorer(
    global_memory: dict[str, Any] | None = None,
    compiled_memory: dict[str, Any] | None = None,
    provenance: dict[str, Any] | None = None,
) -> dict[str, Any]:
    memory = global_memory or {}
    compiled = compiled_memory or {}
    provenance_by_subject = {
        str(row.get("subject")): row
        for row in (provenance or {}).get("entries") or []
        if row.get("subject")
    }
    scores = []
    for prior in ((memory.get("route_prior_model") or {}).get("priors") or []):
        route = str(prior.get("route_key") or "")
        if not route:
            continue
        pass_rate = float(prior.get("validation_pass_rate") or 0.0)
        reject_rate = float(prior.get("reject_rate") or 0.0)
        confidence = float(prior.get("confidence") or 0.0)
        prior_score = float(prior.get("prior_score") or 0.0)
        prov = provenance_by_subject.get(route, {})
        evidence_count = int(prov.get("experiment_count") or 0) + int(prov.get("runtime_experiment_count") or 0) + int(prov.get("feedback_count") or 0)
        reliability = (
            min(40.0, max(0.0, prior_score / 25.0))
            + pass_rate * 30.0
            + confidence * 20.0
            + min(10.0, evidence_count * 2.0)
            - reject_rate * 45.0
        )
        decision = "use_opening"
        if reject_rate >= 0.55 or reliability < 20.0:
            decision = "downweight"
        elif evidence_count <= 1 or pass_rate < 0.35:
            decision = "challenge"
        scores.append({
            "subject_type": "route",
            "subject": route,
            "route_key": route,
            "reliability_score": round(reliability, 4),
            "decision": decision,
            "validation_pass_rate": round(pass_rate, 4),
            "reject_rate": round(reject_rate, 4),
            "confidence": round(confidence, 4),
            "evidence_count": evidence_count,
            "provenance_summary": prov.get("summary") or "",
        })
    for treatment in compiled.get("treatment_priors") or []:
        name = str(treatment.get("treatment") or "")
        if not name:
            continue
        score = float(treatment.get("cross_run_score") or 0.0)
        prov = provenance_by_subject.get(name, {})
        reliability = min(100.0, max(0.0, score * 1.5 + int(prov.get("runtime_experiment_count") or 0) * 5.0))
        scores.append({
            "subject_type": "treatment",
            "subject": name,
            "reliability_score": round(reliability, 4),
            "decision": "use_opening" if reliability >= 30.0 else "challenge",
            "evidence_count": int(prov.get("runtime_experiment_count") or 0),
            "provenance_summary": prov.get("summary") or "",
        })
    scores.sort(key=lambda row: float(row.get("reliability_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Scores durable memories by validation, promotion survival, evidence count, confidence, and prior strength.",
        "scores": scores,
        "use_opening_subjects": [row.get("subject") for row in scores if row.get("decision") == "use_opening"][:20],
        "challenge_subjects": [row.get("subject") for row in scores if row.get("decision") == "challenge"][:20],
        "downweight_subjects": [row.get("subject") for row in scores if row.get("decision") == "downweight"][:20],
    }


def memory_falsification_queue(
    reliability: dict[str, Any] | None = None,
    provenance: dict[str, Any] | None = None,
) -> dict[str, Any]:
    prov_by_subject = {
        str(row.get("subject")): row
        for row in (provenance or {}).get("entries") or []
        if row.get("subject")
    }
    queue = []
    for row in (reliability or {}).get("scores") or []:
        subject = str(row.get("subject") or "")
        if not subject:
            continue
        score = float(row.get("reliability_score") or 0.0)
        evidence_count = int(row.get("evidence_count") or 0)
        should_test = row.get("decision") == "challenge" or (score >= 70.0 and evidence_count <= 2)
        if not should_test:
            continue
        queue.append({
            "task_id": _stable_hash({"falsify": subject, "score": score}),
            "subject_type": row.get("subject_type"),
            "subject": subject,
            "route_key": row.get("route_key") or (subject if row.get("subject_type") == "route" else ""),
            "test": "disable_or_invert_route" if row.get("subject_type") == "route" else "run_competing_treatment_arm",
            "reason": "high confidence needs challenge" if score >= 70.0 else "low evidence or weak validation",
            "priority_score": round(score + max(0, 4 - evidence_count) * 8.0, 4),
            "provenance": prov_by_subject.get(subject, {}),
        })
    queue.sort(key=lambda item: float(item.get("priority_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Schedules small tests against durable memories that are high-confidence or under-challenged.",
        "queue": queue[:30],
        "focus_routes": [row.get("route_key") for row in queue if row.get("route_key")][:12],
    }


def belief_retirement_engine(
    reliability: dict[str, Any] | None = None,
    promotion_feedback: dict[str, Any] | None = None,
) -> dict[str, Any]:
    avoid_routes = set(str(route) for route in (promotion_feedback or {}).get("avoid_opening_routes") or [])
    retire = []
    for row in (reliability or {}).get("scores") or []:
        subject = str(row.get("subject") or "")
        if not subject:
            continue
        if row.get("decision") == "downweight" or (row.get("subject_type") == "route" and subject in avoid_routes):
            retire.append({
                "subject_type": row.get("subject_type"),
                "subject": subject,
                "route_key": row.get("route_key") or (subject if row.get("subject_type") == "route" else ""),
                "reason": "promotion_survival_or_reliability_failure",
                "reliability_score": row.get("reliability_score"),
                "retirement_action": "avoid_opening" if row.get("subject_type") == "route" else "decay_treatment",
            })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Retires durable beliefs that repeatedly fail validation, promotion survival, or reliability checks.",
        "retirements": retire[:50],
        "retire_routes": [row.get("route_key") for row in retire if row.get("route_key")][:20],
        "retire_treatments": [row.get("subject") for row in retire if row.get("subject_type") == "treatment"][:20],
    }


def current_vs_historical_disagreement_monitor(
    compiled_memory: dict[str, Any] | None = None,
    current_state: dict[str, Any] | None = None,
    reliability: dict[str, Any] | None = None,
) -> dict[str, Any]:
    compiled = compiled_memory or {}
    state = current_state or {}
    current_retire = set(str(route) for route in (state.get("recovery_playbook_generator") or {}).get("skip_routes") or [])
    current_retire.update(str(route) for route in (state.get("route_seed_quality_score") or {}).get("retire_routes") or [])
    historical_favor = set(str(route) for route in compiled.get("favor_routes") or [])
    historical_retire = set(str(route) for route in compiled.get("retire_routes") or [])
    current_focus = set(str(route) for route in (state.get("pre_hunt_focus_routes") or state.get("cold_start_route_pack_generator", {}).get("focus_routes") or []))
    disagreements = []
    for route in sorted(historical_favor & current_retire):
        disagreements.append({"route_key": route, "kind": "history_favors_current_retiring", "resolution": "pause_and_falsify"})
    for route in sorted(historical_retire & current_focus):
        disagreements.append({"route_key": route, "kind": "history_retires_current_focuses", "resolution": "remove_from_focus"})
    reliability_down = set(str(item) for item in (reliability or {}).get("downweight_subjects") or [])
    for route in sorted(historical_favor & reliability_down):
        disagreements.append({"route_key": route, "kind": "history_favors_low_reliability", "resolution": "challenge_before_use"})
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Detects early contradictions between durable memory and current-run evidence.",
        "disagreements": disagreements,
        "disagreement_count": len(disagreements),
        "override_history_routes": [row.get("route_key") for row in disagreements if row.get("resolution") == "remove_from_focus"][:12],
        "falsify_routes": [row.get("route_key") for row in disagreements if row.get("resolution") != "remove_from_focus"][:12],
    }


def memory_stress_test_pack(
    reliability: dict[str, Any] | None = None,
    falsification_queue: dict[str, Any] | None = None,
) -> dict[str, Any]:
    tasks = []
    for row in (falsification_queue or {}).get("queue") or []:
        route = str(row.get("route_key") or "")
        tasks.append({
            "task_id": row.get("task_id") or _stable_hash(row),
            "subject": row.get("subject"),
            "route_key": route,
            "probe": row.get("test") or "falsify_memory",
            "mutation_width": "wide" if row.get("subject_type") == "route" else "medium",
            "expected_to_disprove": "durable_memory_assumption",
            "priority_score": row.get("priority_score"),
        })
    if not tasks:
        for row in (reliability or {}).get("scores") or []:
            if row.get("subject_type") == "route" and float(row.get("reliability_score") or 0.0) >= 60.0:
                tasks.append({
                    "task_id": _stable_hash({"stress": row.get("subject")}),
                    "subject": row.get("subject"),
                    "route_key": row.get("route_key") or row.get("subject"),
                    "probe": "disable_route_shadow_check",
                    "mutation_width": "tight",
                    "expected_to_disprove": "route_is_necessary",
                    "priority_score": row.get("reliability_score"),
                })
    tasks.sort(key=lambda item: float(item.get("priority_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Adversarial first-cycle probes designed to disprove top durable assumptions.",
        "tasks": tasks[:20],
        "focus_routes": [row.get("route_key") for row in tasks if row.get("route_key")][:12],
    }


def durable_memory_compression(
    reliability: dict[str, Any] | None = None,
    provenance: dict[str, Any] | None = None,
    retirement: dict[str, Any] | None = None,
) -> dict[str, Any]:
    retire_subjects = set(str(row.get("subject")) for row in (retirement or {}).get("retirements") or [])
    prov = {
        str(row.get("subject")): row
        for row in (provenance or {}).get("entries") or []
        if row.get("subject")
    }
    rules = []
    for row in (reliability or {}).get("scores") or []:
        subject = str(row.get("subject") or "")
        if not subject or subject in retire_subjects:
            continue
        confidence = max(0.0, min(1.0, float(row.get("reliability_score") or 0.0) / 100.0))
        rules.append({
            "rule_id": _stable_hash({"memory_rule": subject, "type": row.get("subject_type")}),
            "subject_type": row.get("subject_type"),
            "subject": subject,
            "rule": f"Use {subject} only while reliability remains above challenge threshold.",
            "confidence": round(confidence, 4),
            "falsifiers": [
                "promotion_survival_failure",
                "validation_pass_rate_drop",
                "current_run_retires_route",
            ],
            "provenance_summary": (prov.get(subject) or {}).get("summary") or row.get("provenance_summary") or "",
        })
    rules.sort(key=lambda item: float(item.get("confidence") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Compresses durable memory into a small rule set with confidence and falsifiers.",
        "rules": rules[:40],
        "rule_count": len(rules),
    }


def memory_qa_smoke_test(controls: dict[str, Any]) -> dict[str, Any]:
    reliability = controls.get("memory_reliability_scorer") or {}
    retirement = controls.get("belief_retirement_engine") or {}
    provenance = controls.get("memory_provenance_explorer") or {}
    opening = controls.get("hunt_opening_playbook") or {}
    compressed = controls.get("durable_memory_compression") or {}
    downweighted = set(str(item) for item in reliability.get("downweight_subjects") or [])
    opening_focus = set(str(route) for route in opening.get("focus_routes") or [])
    provenance_subjects = set(str(item) for item in provenance.get("subjects_with_provenance") or [])
    retired = bool(retirement.get("retirements") or retirement.get("retire_routes") or retirement.get("retire_treatments"))
    checks = [
        {"check": "bad_memory_downweighted_or_retired", "passed": bool(downweighted or retired or retirement.get("retirements") == [])},
        {"check": "opening_memories_have_provenance", "passed": all(route in provenance_subjects for route in opening_focus) if opening_focus else True},
        {"check": "compressed_rules_have_falsifiers", "passed": all(bool(row.get("falsifiers")) for row in compressed.get("rules") or [])},
        {"check": "retired_routes_not_opening_focus", "passed": not (set(retirement.get("retire_routes") or []) & opening_focus)},
    ]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "QA smoke test for stale-memory downweighting, provenance, and falsifiers.",
        "checks": checks,
        "passed": all(bool(row.get("passed")) for row in checks),
    }


def _route_from_subject(subject: str) -> str:
    subject = str(subject or "")
    return subject if "|" in subject else ""


def hypothesis_factory(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    hypotheses: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add(route: str, treatment: str, claim: str, source: str, support: float, uncertainty: float, crazy: float = 0.0) -> None:
        route = str(route or "")
        treatment = str(treatment or "probe")
        key = _stable_hash({"route": route, "treatment": treatment, "claim": claim, "source": source})
        if key in seen:
            return
        seen.add(key)
        hypotheses.append({
            "hypothesis_id": key,
            "route_key": route,
            "treatment": treatment,
            "claim": claim,
            "source": source,
            "support_score": round(float(support), 4),
            "uncertainty_score": round(max(0.0, min(1.0, float(uncertainty))), 4),
            "crazy_factor": round(max(0.0, min(1.0, float(crazy))), 4),
        })

    for row in state.get("top_candidates") or []:
        if not isinstance(row, dict):
            continue
        route = str(row.get("route_key") or "")
        pnl = float(row.get("step2_pnl") or row.get("pnl") or row.get("step2_delta_vs_active") or 0.0)
        treatment = str(row.get("treatment") or row.get("mutation_lane") or row.get("setup_type") or "winner_dna")
        if route:
            add(route, treatment, f"{route} can produce live-beating variants when {treatment} is emphasized.", "top_candidate", pnl, 0.35, 0.15)

    for experiment in (state.get("active_experiment_plan") or {}).get("experiments") or []:
        if not isinstance(experiment, dict):
            continue
        route = str(experiment.get("route_key") or "")
        treatment = str(((experiment.get("treatments") or [{}])[0] or {}).get("mutation_lane") or experiment.get("kind") or "experiment")
        add(route, treatment, str(experiment.get("hypothesis") or f"{treatment} improves {route or 'current search space'}."), "active_experiment_plan", 40.0, 0.55, 0.25)

    for row in (state.get("value_of_information_planner") or {}).get("plans") or []:
        if not isinstance(row, dict):
            continue
        target = str(row.get("target") or row.get("route_key") or row.get("treatment") or "")
        route = str(row.get("route_key") or _route_from_subject(target))
        add(route, target or "voi_probe", f"Testing {target or route} has enough information value to change the hunt.", "value_of_information", float(row.get("expected_value_of_information") or 25.0), 0.7, 0.35)

    for row in (state.get("memory_falsification_queue") or {}).get("queue") or []:
        if not isinstance(row, dict):
            continue
        route = str(row.get("route_key") or "")
        add(route, "falsify_memory", f"Durable memory for {row.get('subject') or route} may be wrong and should be stress-tested.", "memory_falsification", float(row.get("priority_score") or 30.0), 0.8, 0.65)

    for row in (state.get("mutation_grammar_learner") or {}).get("top_templates") or []:
        if not isinstance(row, dict):
            continue
        route = str(row.get("route_key") or "")
        treatment = str(row.get("template") or row.get("treatment") or "mutation_recipe")
        add(route, treatment, f"{treatment} may be reusable beyond the parent candidate.", "mutation_grammar", float(row.get("learning_score") or 35.0), 0.45, 0.3)

    hypotheses.sort(key=lambda row: (float(row.get("support_score") or 0.0) * (0.8 + float(row.get("uncertainty_score") or 0.0))), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Converts live hunt evidence into explicit testable hypotheses.",
        "hypotheses": hypotheses[:40],
        "top_hypothesis": (hypotheses or [{}])[0],
        "focus_routes": _ordered_unique([row.get("route_key") for row in hypotheses if row.get("route_key")])[:12],
    }


def hypothesis_market_maker(hypothesis_factory_payload: dict[str, Any] | None = None, state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    survivor_by_route = {
        str(row.get("route_key") or row.get("route") or ""): float(row.get("survival_probability") or row.get("approve_probability") or 0.5)
        for row in (state.get("promotion_survivor_model") or {}).get("promotion_survivor_top10") or []
        if isinstance(row, dict)
    }
    quotes = []
    for row in (hypothesis_factory_payload or {}).get("hypotheses") or []:
        if not isinstance(row, dict):
            continue
        route = str(row.get("route_key") or "")
        support = float(row.get("support_score") or 0.0)
        uncertainty = float(row.get("uncertainty_score") or 0.0)
        crazy = float(row.get("crazy_factor") or 0.0)
        survival = survivor_by_route.get(route, 0.55 if route else 0.45)
        expected_value = support * (0.65 + survival) + 35.0 * uncertainty + 22.0 * crazy
        cost = 12.0 + 10.0 * max(0.0, 1.0 - survival)
        edge = expected_value - cost
        quotes.append({
            "hypothesis_id": row.get("hypothesis_id"),
            "route_key": route,
            "treatment": row.get("treatment"),
            "expected_value": round(expected_value, 4),
            "cost": round(cost, 4),
            "edge": round(edge, 4),
            "survival_probability": round(survival, 4),
            "uncertainty_score": round(uncertainty, 4),
            "market_action": "buy" if edge >= 45.0 else "watch" if edge >= 12.0 else "sell",
            "claim": row.get("claim"),
        })
    quotes.sort(key=lambda item: float(item.get("edge") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Prices hunt hypotheses by upside, uncertainty, cost, and promotion-survival odds.",
        "quotes": quotes[:40],
        "buy": [row for row in quotes if row.get("market_action") == "buy"][:12],
        "watch": [row for row in quotes if row.get("market_action") == "watch"][:12],
        "sell": [row for row in quotes if row.get("market_action") == "sell"][:12],
        "focus_routes": _ordered_unique([row.get("route_key") for row in quotes if row.get("market_action") in {"buy", "watch"} and row.get("route_key")])[:12],
    }


def real_time_bet_sizer(hypothesis_market: dict[str, Any] | None = None, state: dict[str, Any] | None = None, *, max_workers: int = 4) -> dict[str, Any]:
    quotes = [row for row in (hypothesis_market or {}).get("quotes") or [] if isinstance(row, dict) and row.get("market_action") != "sell"]
    total_edge = sum(max(1.0, float(row.get("edge") or 0.0)) for row in quotes) or 1.0
    allocations = []
    for idx, row in enumerate(quotes[:12]):
        pct = max(3.0, min(55.0, 100.0 * max(1.0, float(row.get("edge") or 0.0)) / total_edge))
        allocations.append({
            "hypothesis_id": row.get("hypothesis_id"),
            "route_key": row.get("route_key"),
            "treatment": row.get("treatment"),
            "budget_pct": round(pct, 2),
            "worker": f"worker_{(idx % max_workers) + 1}",
            "batch_size_multiplier": round(0.75 + min(0.75, pct / 100.0), 4),
            "mutation_width": "wide" if float(row.get("uncertainty_score") or 0.0) >= 0.65 else "medium",
            "reason": f"hypothesis_edge={row.get('edge')}",
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Allocates live hunt budget to hypotheses using edge-weighted sizing.",
        "allocations": allocations,
        "focus_routes": _ordered_unique([row.get("route_key") for row in allocations if row.get("route_key")])[:12],
        "batch_size_multiplier": round(max([float(row.get("batch_size_multiplier") or 1.0) for row in allocations] or [1.0]), 4),
    }


def contrarian_generator(hypothesis_factory_payload: dict[str, Any] | None = None, hypothesis_market: dict[str, Any] | None = None) -> dict[str, Any]:
    market_by_id = {str(row.get("hypothesis_id")): row for row in (hypothesis_market or {}).get("quotes") or [] if isinstance(row, dict)}
    experiments = []
    for row in (hypothesis_factory_payload or {}).get("hypotheses") or []:
        if not isinstance(row, dict):
            continue
        quote = market_by_id.get(str(row.get("hypothesis_id")), {})
        if quote.get("market_action") == "sell":
            continue
        route = str(row.get("route_key") or "")
        experiments.append({
            "experiment_id": _stable_hash({"contrarian": row.get("hypothesis_id")}),
            "parent_hypothesis_id": row.get("hypothesis_id"),
            "route_key": route,
            "treatment": row.get("treatment"),
            "contrarian_action": "indicator_shuffle" if float(row.get("crazy_factor") or 0.0) >= 0.5 else "invert_mutation_width",
            "mutation_width": "wide" if quote.get("uncertainty_score", row.get("uncertainty_score")) else "medium",
            "reason": "try the opposite before trusting the belief",
            "priority_score": round(float(quote.get("edge") or row.get("support_score") or 0.0) + 25.0 * float(row.get("crazy_factor") or 0.0), 4),
        })
    experiments.sort(key=lambda item: float(item.get("priority_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Creates opposite-side tests for current best beliefs, including weird scoring and indicator shuffles.",
        "experiments": experiments[:20],
        "focus_routes": _ordered_unique([row.get("route_key") for row in experiments if row.get("route_key")])[:12],
    }


def learning_stop_loss(hypothesis_market: dict[str, Any] | None = None, state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    retired_routes = set(str(route) for route in (state.get("belief_retirement_engine") or {}).get("retire_routes") or [])
    retired_routes.update(str(route) for route in (state.get("opportunity_cost_meter") or {}).get("reduce_budget_routes") or [])
    zero_routes = {
        str(route)
        for row in (state.get("zero_yield_autopsy_engine") or {}).get("autopsies") or []
        for route in (row.get("route_seeds") or [])
    }
    stops = []
    for row in (hypothesis_market or {}).get("quotes") or []:
        if not isinstance(row, dict):
            continue
        route = str(row.get("route_key") or "")
        reasons = []
        if row.get("market_action") == "sell":
            reasons.append("negative_or_low_edge")
        if route in retired_routes:
            reasons.append("retired_or_high_opportunity_cost_route")
        if route in zero_routes:
            reasons.append("recent_zero_yield_route")
        if reasons:
            stops.append({
                "hypothesis_id": row.get("hypothesis_id"),
                "route_key": route,
                "action": "kill" if "retired_or_high_opportunity_cost_route" in reasons else "shrink",
                "reasons": reasons,
                "edge": row.get("edge"),
            })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Stops or shrinks hypotheses when live evidence says they are wasting hunt budget.",
        "stops": stops[:30],
        "avoid_routes": _ordered_unique([row.get("route_key") for row in stops if row.get("action") == "kill" and row.get("route_key")])[:12],
        "shrink_routes": _ordered_unique([row.get("route_key") for row in stops if row.get("action") == "shrink" and row.get("route_key")])[:12],
    }


def breakthrough_detector(hypothesis_market: dict[str, Any] | None = None, contrarian: dict[str, Any] | None = None, state: dict[str, Any] | None = None) -> dict[str, Any]:
    contrarian_routes = {str(row.get("route_key") or "") for row in (contrarian or {}).get("experiments") or [] if isinstance(row, dict)}
    breakthroughs = []
    for row in (hypothesis_market or {}).get("quotes") or []:
        if not isinstance(row, dict):
            continue
        route = str(row.get("route_key") or "")
        score = float(row.get("edge") or 0.0) + (18.0 if route in contrarian_routes else 0.0) + 20.0 * float(row.get("uncertainty_score") or 0.0)
        if score < 35.0:
            continue
        breakthroughs.append({
            "hypothesis_id": row.get("hypothesis_id"),
            "route_key": route,
            "treatment": row.get("treatment"),
            "breakthrough_score": round(score, 4),
            "signal": "new_reusable_pattern" if row.get("market_action") == "buy" else "watch_for_pattern",
            "claim": row.get("claim"),
        })
    breakthroughs.sort(key=lambda item: float(item.get("breakthrough_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Detects when a weird or strong hypothesis looks like reusable hunt knowledge.",
        "breakthroughs": breakthroughs[:20],
        "top_breakthrough": (breakthroughs or [{}])[0],
        "focus_routes": _ordered_unique([row.get("route_key") for row in breakthroughs if row.get("route_key")])[:12],
    }


def pattern_to_recipe_compiler(breakthroughs: dict[str, Any] | None = None, contrarian: dict[str, Any] | None = None) -> dict[str, Any]:
    contrarian_by_route = {str(row.get("route_key") or ""): row for row in (contrarian or {}).get("experiments") or [] if isinstance(row, dict)}
    recipes = []
    for row in (breakthroughs or {}).get("breakthroughs") or []:
        if not isinstance(row, dict):
            continue
        route = str(row.get("route_key") or "")
        contra = contrarian_by_route.get(route, {})
        recipes.append({
            "recipe_id": _stable_hash({"recipe": row.get("hypothesis_id"), "route": route}),
            "route_key": route,
            "treatment": row.get("treatment") or contra.get("treatment") or "probe",
            "mutation_width": contra.get("mutation_width") or "medium",
            "steps": [
                "seed from the parent route",
                "apply the hypothesized treatment",
                "run one contrarian arm beside it",
                "keep only variants beating current live",
            ],
            "promotion_caution": "treat as recipe candidate until promotion review survives robustness",
            "source_hypothesis_id": row.get("hypothesis_id"),
            "recipe_score": row.get("breakthrough_score"),
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Compiles breakthrough hypotheses into reusable mutation recipes.",
        "recipes": recipes[:20],
        "focus_routes": _ordered_unique([row.get("route_key") for row in recipes if row.get("route_key")])[:12],
        "top_recipe": (recipes or [{}])[0],
    }


def hunt_narrative_memory(
    factory: dict[str, Any] | None = None,
    market: dict[str, Any] | None = None,
    stop_loss: dict[str, Any] | None = None,
    breakthroughs: dict[str, Any] | None = None,
    recipes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    top = (market or {}).get("quotes") or []
    stopped = (stop_loss or {}).get("stops") or []
    breakthrough = (breakthroughs or {}).get("top_breakthrough") or {}
    recipe = (recipes or {}).get("top_recipe") or {}
    memo = [
        f"Believed: {(factory or {}).get('top_hypothesis', {}).get('claim') or 'no dominant hypothesis yet'}",
        f"Priced: {(top[0] if top else {}).get('hypothesis_id', '')} edge={(top[0] if top else {}).get('edge', '')}",
        f"Stopped: {len(stopped)} weak or stale hypotheses",
        f"Breakthrough: {breakthrough.get('route_key') or 'none'}",
        f"Recipe: {recipe.get('recipe_id') or 'none'}",
    ]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Short running narrative of what the hunt believed, tested, killed, and promoted to recipe memory.",
        "memo": memo,
        "summary": " | ".join(memo),
    }


def hypothesis_learning_suite(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    factory = hypothesis_factory(state)
    market = hypothesis_market_maker(factory, state)
    bet_sizer = real_time_bet_sizer(market, state)
    contrarian = contrarian_generator(factory, market)
    stop_loss = learning_stop_loss(market, state)
    breakthroughs = breakthrough_detector(market, contrarian, state)
    recipes = pattern_to_recipe_compiler(breakthroughs, contrarian)
    narrative = hunt_narrative_memory(factory, market, stop_loss, breakthroughs, recipes)
    return {
        "hypothesis_factory": factory,
        "hypothesis_market_maker": market,
        "real_time_bet_sizer": bet_sizer,
        "contrarian_generator": contrarian,
        "learning_stop_loss": stop_loss,
        "breakthrough_detector": breakthroughs,
        "pattern_to_recipe_compiler": recipes,
        "hunt_narrative_memory": narrative,
    }


def attention_ledger(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    entries: list[dict[str, Any]] = []

    def add(kind: str, route: str, treatment: str, worker: str, spend: float, source: str, action: str = "") -> None:
        route = str(route or "")
        treatment = str(treatment or "")
        worker = str(worker or "")
        entries.append({
            "entry_id": _stable_hash({"kind": kind, "route": route, "treatment": treatment, "worker": worker, "source": source, "action": action}),
            "kind": kind,
            "route_key": route,
            "treatment": treatment,
            "worker": worker,
            "action": action,
            "spend_units": round(max(0.1, float(spend)), 4),
            "source": source,
        })

    for row in ((state.get("online_allocation") or {}).get("allocation") or []):
        if isinstance(row, dict):
            add("route_budget", row.get("route_key"), "", "", float(row.get("recommended_budget_pct") or 0.0), "online_allocation", "route_probe")
    for row in ((state.get("real_time_bet_sizer") or {}).get("allocations") or []):
        if isinstance(row, dict):
            add("hypothesis_budget", row.get("route_key"), row.get("treatment"), row.get("worker"), float(row.get("budget_pct") or 0.0), "real_time_bet_sizer", "hypothesis_bet")
    for row in ((state.get("contrarian_generator") or {}).get("experiments") or []):
        if isinstance(row, dict):
            add("contrarian_budget", row.get("route_key"), row.get("treatment"), "", float(row.get("priority_score") or 10.0), "contrarian_generator", row.get("contrarian_action") or "contrarian_probe")
    for row in ((state.get("pattern_to_recipe_compiler") or {}).get("recipes") or []):
        if isinstance(row, dict):
            add("recipe_budget", row.get("route_key"), row.get("treatment"), "", float(row.get("recipe_score") or 10.0), "pattern_to_recipe_compiler", "recipe_probe")
    for contract in ((state.get("worker_job_contracts") or {}).get("contracts") or []):
        if not isinstance(contract, dict):
            continue
        for job in contract.get("jobs") or []:
            if isinstance(job, dict):
                add("worker_attention", job.get("route_key"), job.get("treatment"), contract.get("worker") or job.get("worker"), float(job.get("batch_size_multiplier") or 1.0) * 10.0, "worker_job_contracts", job.get("action") or "")

    by_route: dict[str, float] = {}
    by_worker: dict[str, float] = {}
    by_kind: dict[str, float] = {}
    for row in entries:
        route = str(row.get("route_key") or "")
        worker = str(row.get("worker") or "")
        kind = str(row.get("kind") or "")
        spend = float(row.get("spend_units") or 0.0)
        if route:
            by_route[route] = by_route.get(route, 0.0) + spend
        if worker:
            by_worker[worker] = by_worker.get(worker, 0.0) + spend
        by_kind[kind] = by_kind.get(kind, 0.0) + spend

    route_spend = [{"route_key": route, "spend_units": round(spend, 4)} for route, spend in by_route.items()]
    route_spend.sort(key=lambda row: float(row.get("spend_units") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Tracks where hunt attention went by route, hypothesis, treatment, and worker.",
        "entries": entries[:120],
        "route_spend": route_spend[:40],
        "worker_spend": [{"worker": worker, "spend_units": round(spend, 4)} for worker, spend in sorted(by_worker.items(), key=lambda item: item[1], reverse=True)],
        "kind_spend": [{"kind": kind, "spend_units": round(spend, 4)} for kind, spend in sorted(by_kind.items(), key=lambda item: item[1], reverse=True)],
        "total_spend_units": round(sum(float(row.get("spend_units") or 0.0) for row in entries), 4),
    }


def wasted_spend_autopsy(attention: dict[str, Any] | None = None, state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    winning_routes = {str(row.get("route_key") or "") for row in state.get("top_candidates") or [] if isinstance(row, dict)}
    winning_routes.update(str(row.get("route_key") or "") for row in ((state.get("breakthrough_detector") or {}).get("breakthroughs") or []) if isinstance(row, dict))
    stop_routes = set((state.get("learning_stop_loss") or {}).get("avoid_routes") or [])
    rows = []
    for row in (attention or {}).get("route_spend") or []:
        route = str(row.get("route_key") or "")
        spend = float(row.get("spend_units") or 0.0)
        if not route:
            continue
        productive = route in winning_routes and route not in stop_routes
        waste_score = spend * (0.25 if productive else 1.0) + (18.0 if route in stop_routes else 0.0)
        rows.append({
            "route_key": route,
            "spend_units": round(spend, 4),
            "productive": productive,
            "waste_score": round(waste_score, 4),
            "reason": "stop_loss_or_retired" if route in stop_routes else "no_live_beater_or_breakthrough_seen" if not productive else "productive_spend",
            "recommendation": "kill_or_pause" if route in stop_routes else "shrink" if not productive and spend >= 20.0 else "keep",
        })
    rows.sort(key=lambda item: float(item.get("waste_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Finds routes and hypotheses that consumed budget without enough useful output.",
        "waste": rows[:40],
        "avoid_routes": _ordered_unique([row.get("route_key") for row in rows if row.get("recommendation") == "kill_or_pause"])[:12],
        "shrink_routes": _ordered_unique([row.get("route_key") for row in rows if row.get("recommendation") == "shrink"])[:12],
        "top_waste": (rows or [{}])[0],
    }


def marginal_yield_curve(attention: dict[str, Any] | None = None, state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    cycle_yield = state.get("cycle_yield") if isinstance(state.get("cycle_yield"), dict) else {}
    global_yield = float(cycle_yield.get("recent_winner_yield_per_10k") or 0.0)
    live_routes = {str(row.get("route_key") or "") for row in state.get("top_candidates") or [] if isinstance(row, dict)}
    curves = []
    for row in (attention or {}).get("route_spend") or []:
        route = str(row.get("route_key") or "")
        spend = float(row.get("spend_units") or 0.0)
        output = 1.0 if route in live_routes else 0.0
        marginal_yield = (output / max(1.0, spend)) * 100.0
        plateau = spend >= 25.0 and output <= 0.0
        curves.append({
            "route_key": route,
            "spend_units": round(spend, 4),
            "marginal_yield": round(marginal_yield, 4),
            "global_recent_yield_per_10k": round(global_yield, 4),
            "status": "plateau" if plateau else "scale" if marginal_yield >= 4.0 else "probe",
            "next_action": "stop_extra_batches" if plateau else "add_batches" if marginal_yield >= 4.0 else "small_probe",
        })
    curves.sort(key=lambda item: (item.get("status") == "plateau", -float(item.get("marginal_yield") or 0.0)), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Estimates when additional batches stop improving route or hypothesis yield.",
        "curves": curves[:40],
        "plateau_routes": _ordered_unique([row.get("route_key") for row in curves if row.get("status") == "plateau"])[:12],
        "scale_routes": _ordered_unique([row.get("route_key") for row in curves if row.get("status") == "scale"])[:12],
    }


def explore_exploit_regret_tracker(attention: dict[str, Any] | None = None, wasted: dict[str, Any] | None = None, yield_curve: dict[str, Any] | None = None) -> dict[str, Any]:
    kind_spend = {str(row.get("kind")): float(row.get("spend_units") or 0.0) for row in (attention or {}).get("kind_spend") or []}
    total = max(1.0, float((attention or {}).get("total_spend_units") or 0.0))
    explore = kind_spend.get("contrarian_budget", 0.0) + kind_spend.get("recipe_budget", 0.0)
    exploit = kind_spend.get("route_budget", 0.0) + kind_spend.get("hypothesis_budget", 0.0)
    waste_count = len((wasted or {}).get("shrink_routes") or []) + len((wasted or {}).get("avoid_routes") or [])
    plateau_count = len((yield_curve or {}).get("plateau_routes") or [])
    exploration_regret = max(0.0, 0.22 - explore / total) * 100.0
    exploitation_regret = max(0.0, 0.45 - exploit / total) * 100.0
    waste_regret = min(100.0, (waste_count + plateau_count) * 8.0)
    if waste_regret >= max(exploration_regret, exploitation_regret):
        recommendation = "cut_waste_and_redeploy"
    elif exploration_regret > exploitation_regret:
        recommendation = "increase_exploration"
    else:
        recommendation = "increase_exploitation"
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Measures whether attention was under-spent on exploration, exploitation, validation, or cleanup.",
        "explore_pct": round(explore / total * 100.0, 4),
        "exploit_pct": round(exploit / total * 100.0, 4),
        "exploration_regret": round(exploration_regret, 4),
        "exploitation_regret": round(exploitation_regret, 4),
        "waste_regret": round(waste_regret, 4),
        "recommendation": recommendation,
    }


def worker_alpha_attribution(attention: dict[str, Any] | None = None, state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    card_by_worker = {
        str(row.get("worker") or ""): row
        for row in ((state.get("worker_learning_report_cards") or {}).get("cards") or [])
        if isinstance(row, dict)
    }
    rows = []
    for row in (attention or {}).get("worker_spend") or []:
        worker = str(row.get("worker") or "")
        spend = float(row.get("spend_units") or 0.0)
        card = card_by_worker.get(worker, {})
        score = float(card.get("score") or card.get("alpha_score") or 0.0)
        alpha = score + max(0.0, 20.0 - spend) * 0.2
        role = str(card.get("recommended_role") or card.get("role") or "route_probe")
        rows.append({
            "worker": worker,
            "spend_units": round(spend, 4),
            "alpha_score": round(alpha, 4),
            "best_role": role,
            "recommendation": "give_more_budget" if alpha >= 10.0 else "keep_small_probe",
        })
    if not rows:
        for idx in range(4):
            rows.append({"worker": f"worker_{idx + 1}", "spend_units": 0.0, "alpha_score": 0.0, "best_role": "route_probe", "recommendation": "keep_small_probe"})
    rows.sort(key=lambda item: float(item.get("alpha_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Attributes useful hunt output to worker-role pairs.",
        "workers": rows[:4],
        "top_worker": (rows or [{}])[0],
    }


def budget_reallocator(
    attention: dict[str, Any] | None = None,
    wasted: dict[str, Any] | None = None,
    yield_curve: dict[str, Any] | None = None,
    regret: dict[str, Any] | None = None,
    worker_alpha: dict[str, Any] | None = None,
) -> dict[str, Any]:
    avoid = _ordered_unique(list((wasted or {}).get("avoid_routes") or []) + list((yield_curve or {}).get("plateau_routes") or []))
    shrink = _ordered_unique(list((wasted or {}).get("shrink_routes") or []))
    scale = [route for route in _ordered_unique(list((yield_curve or {}).get("scale_routes") or [])) if route not in set(avoid)]
    recommendation = (regret or {}).get("recommendation") or "balanced"
    batch_mult = 1.0
    mutation_width = "medium"
    if recommendation == "increase_exploration":
        batch_mult = 1.12
        mutation_width = "wide"
    elif recommendation == "increase_exploitation":
        batch_mult = 1.08
        mutation_width = "medium"
    elif recommendation == "cut_waste_and_redeploy":
        batch_mult = 0.82
        mutation_width = "tight"
    assignments = []
    top_workers = (worker_alpha or {}).get("workers") or []
    focus_pool = scale or [
        row.get("route_key")
        for row in (attention or {}).get("route_spend") or []
        if row.get("route_key") not in set(avoid)
    ]
    for idx, route in enumerate(_ordered_unique(focus_pool)[:4]):
        worker = ((top_workers[idx % len(top_workers)] or {}).get("worker") if top_workers else f"worker_{idx + 1}") or f"worker_{idx + 1}"
        assignments.append({
            "worker": worker,
            "route_key": route,
            "action": "scale_efficient_route" if route in scale else "redeploy_probe",
            "mutation_width": mutation_width,
            "batch_size_multiplier": batch_mult,
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Converts attention efficiency into next-cycle budget moves.",
        "focus_routes": _ordered_unique(scale + [row.get("route_key") for row in assignments])[:12],
        "avoid_routes": avoid[:12],
        "shrink_routes": shrink[:12],
        "assignments": assignments,
        "batch_size_multiplier": round(batch_mult, 4),
        "mutation_width": mutation_width,
        "recommendation": recommendation,
    }


def time_aware_hunt_plan(state: dict[str, Any] | None = None, budget: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    remaining = float(state.get("remaining_sec") or state.get("estimated_remaining_sec") or 0.0)
    cycles_done = int(state.get("cycles_completed") or 0)
    if remaining <= 0:
        phase = "middle" if cycles_done >= 2 else "early"
    elif remaining >= 5400:
        phase = "early"
    elif remaining >= 1800:
        phase = "middle"
    else:
        phase = "late"
    action_by_phase = {
        "early": "explore_contrarian_and_recipe_space",
        "middle": "scale_best_yield_and_kill_plateaus",
        "late": "confirm_promotion_quality_and_reduce_noise",
    }
    width_by_phase = {"early": "wide", "middle": (budget or {}).get("mutation_width") or "medium", "late": "tight"}
    batch_by_phase = {"early": 1.08, "middle": float((budget or {}).get("batch_size_multiplier") or 1.0), "late": 0.72}
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Adjusts hunt controls based on remaining wall-clock time.",
        "phase": phase,
        "primary_action": action_by_phase[phase],
        "mutation_width": width_by_phase[phase],
        "batch_size_multiplier": round(batch_by_phase[phase], 4),
        "focus_routes": (budget or {}).get("focus_routes") or [],
        "avoid_routes": (budget or {}).get("avoid_routes") or [],
    }


def spend_efficiency_narrative(
    attention: dict[str, Any] | None = None,
    wasted: dict[str, Any] | None = None,
    regret: dict[str, Any] | None = None,
    budget: dict[str, Any] | None = None,
) -> dict[str, Any]:
    top_spend = ((attention or {}).get("route_spend") or [{}])[0] or {}
    top_waste = (wasted or {}).get("top_waste") or {}
    memo = [
        f"Spent {attention.get('total_spend_units') if isinstance(attention, dict) else 0} attention units.",
        f"Most attention: {top_spend.get('route_key') or 'none'} ({top_spend.get('spend_units') or 0}).",
        f"Biggest waste: {top_waste.get('route_key') or 'none'} ({top_waste.get('reason') or 'n/a'}).",
        f"Regret: {(regret or {}).get('recommendation') or 'balanced'}.",
        f"Next budget: {(budget or {}).get('recommendation') or 'balanced'}; focus={','.join((budget or {}).get('focus_routes') or []) or 'none'}.",
    ]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Human-readable memo explaining attention efficiency and next spend moves.",
        "memo": memo,
        "summary": " | ".join(memo),
    }


def attention_economics_suite(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    attention = attention_ledger(state)
    wasted = wasted_spend_autopsy(attention, state)
    yield_curve = marginal_yield_curve(attention, state)
    regret = explore_exploit_regret_tracker(attention, wasted, yield_curve)
    worker_alpha = worker_alpha_attribution(attention, state)
    budget = budget_reallocator(attention, wasted, yield_curve, regret, worker_alpha)
    time_plan = time_aware_hunt_plan(state, budget)
    narrative = spend_efficiency_narrative(attention, wasted, regret, budget)
    return {
        "attention_ledger": attention,
        "wasted_spend_autopsy": wasted,
        "marginal_yield_curve": yield_curve,
        "explore_exploit_regret_tracker": regret,
        "worker_alpha_attribution": worker_alpha,
        "budget_reallocator": budget,
        "time_aware_hunt_plan": time_plan,
        "spend_efficiency_narrative": narrative,
    }


def _idea_tokens(*parts: Any) -> set[str]:
    blob = " ".join(str(part or "").replace("|", " ").replace("::", " ") for part in parts)
    return {token.lower() for token in blob.replace("_", " ").replace("-", " ").split() if token}


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    return len(a & b) / max(1, len(a | b))


def idea_novelty_ledger(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    raw_ideas: list[dict[str, Any]] = []
    for row in (state.get("hypothesis_factory") or {}).get("hypotheses") or []:
        if isinstance(row, dict):
            raw_ideas.append({
                "idea_id": row.get("hypothesis_id"),
                "kind": "hypothesis",
                "route_key": row.get("route_key") or "",
                "treatment": row.get("treatment") or "",
                "text": row.get("claim") or "",
                "source": row.get("source") or "hypothesis_factory",
            })
    for row in (state.get("contrarian_generator") or {}).get("experiments") or []:
        if isinstance(row, dict):
            raw_ideas.append({
                "idea_id": row.get("experiment_id"),
                "kind": "contrarian",
                "route_key": row.get("route_key") or "",
                "treatment": row.get("treatment") or row.get("contrarian_action") or "",
                "text": row.get("reason") or row.get("contrarian_action") or "",
                "source": "contrarian_generator",
            })
    for row in (state.get("pattern_to_recipe_compiler") or {}).get("recipes") or []:
        if isinstance(row, dict):
            raw_ideas.append({
                "idea_id": row.get("recipe_id"),
                "kind": "recipe",
                "route_key": row.get("route_key") or "",
                "treatment": row.get("treatment") or "",
                "text": " ".join(row.get("steps") or []),
                "source": "pattern_to_recipe_compiler",
            })
    recent_tokens = [
        set(row.get("tokens") or [])
        for row in ((state.get("idea_novelty_ledger") or {}).get("ideas") or [])[:40]
        if isinstance(row, dict)
    ]
    ideas = []
    prior_tokens: list[set[str]] = list(recent_tokens)
    for row in raw_ideas:
        tokens = _idea_tokens(row.get("route_key"), row.get("treatment"), row.get("text"), row.get("kind"))
        max_similarity = max([_jaccard(tokens, prior) for prior in prior_tokens] or [0.0])
        novelty = 1.0 - max_similarity
        row = {
            **row,
            "idea_id": row.get("idea_id") or _stable_hash(row),
            "tokens": sorted(tokens),
            "max_similarity": round(max_similarity, 4),
            "novelty_score": round(novelty, 4),
            "novelty_band": "fresh" if novelty >= 0.55 else "familiar" if novelty >= 0.25 else "repeat",
        }
        ideas.append(row)
        prior_tokens.append(tokens)
    ideas.sort(key=lambda item: float(item.get("novelty_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Tracks how different each hypothesis, contrarian probe, and recipe is from recent ideas.",
        "ideas": ideas[:80],
        "fresh_ideas": [row for row in ideas if row.get("novelty_band") == "fresh"][:20],
        "repeat_ideas": [row for row in ideas if row.get("novelty_band") == "repeat"][:20],
    }


def idea_saturation_detector(novelty: dict[str, Any] | None = None) -> dict[str, Any]:
    buckets: dict[str, dict[str, Any]] = {}
    for row in (novelty or {}).get("ideas") or []:
        if not isinstance(row, dict):
            continue
        key = "|".join([str(row.get("route_key") or "*"), str(row.get("treatment") or "*"), str(row.get("kind") or "*")])
        item = buckets.setdefault(key, {"signature": key, "count": 0, "ideas": [], "avg_novelty": 0.0})
        item["count"] += 1
        item["ideas"].append(row.get("idea_id"))
        item["avg_novelty"] += float(row.get("novelty_score") or 0.0)
    clusters = []
    for item in buckets.values():
        count = max(1, int(item.get("count") or 0))
        avg = float(item.get("avg_novelty") or 0.0) / count
        clusters.append({
            "signature": item.get("signature"),
            "count": count,
            "idea_ids": item.get("ideas") or [],
            "avg_novelty": round(avg, 4),
            "status": "saturated" if count >= 2 and avg < 0.45 else "ok",
        })
    clusters.sort(key=lambda row: (row.get("status") == "saturated", int(row.get("count") or 0)), reverse=True)
    saturated = [row for row in clusters if row.get("status") == "saturated"]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Detects when the hunt keeps generating the same idea in different clothes.",
        "clusters": clusters[:40],
        "saturated_clusters": saturated[:20],
        "saturation_count": len(saturated),
    }


def creative_leap_scorer(novelty: dict[str, Any] | None = None, state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    edge_by_route = {
        str(row.get("route_key") or ""): float(row.get("edge") or 0.0)
        for row in (state.get("hypothesis_market_maker") or {}).get("quotes") or []
        if isinstance(row, dict)
    }
    leaps = []
    for row in (novelty or {}).get("ideas") or []:
        if not isinstance(row, dict):
            continue
        route = str(row.get("route_key") or "")
        novelty_score = float(row.get("novelty_score") or 0.0)
        plausibility = max(0.15, min(1.0, 0.45 + edge_by_route.get(route, 0.0) / 120.0))
        useful_weirdness = novelty_score * plausibility
        leaps.append({
            "idea_id": row.get("idea_id"),
            "route_key": route,
            "kind": row.get("kind"),
            "treatment": row.get("treatment"),
            "novelty_score": round(novelty_score, 4),
            "plausibility_score": round(plausibility, 4),
            "creative_leap_score": round(useful_weirdness * 100.0, 4),
            "decision": "fund" if useful_weirdness >= 0.35 else "watch" if useful_weirdness >= 0.18 else "too_timid_or_too_far",
        })
    leaps.sort(key=lambda row: float(row.get("creative_leap_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Scores useful weirdness: plausible distance from current winners, not random chaos.",
        "leaps": leaps[:40],
        "fund": [row for row in leaps if row.get("decision") == "fund"][:12],
        "focus_routes": _ordered_unique([row.get("route_key") for row in leaps if row.get("decision") in {"fund", "watch"} and row.get("route_key")])[:12],
    }


def failed_imagination_autopsy(
    novelty: dict[str, Any] | None = None,
    saturation: dict[str, Any] | None = None,
    state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    state = state or {}
    winner_routes = {str(row.get("route_key") or "") for row in state.get("top_candidates") or [] if isinstance(row, dict)}
    saturated_ids = {
        str(idea_id)
        for row in (saturation or {}).get("saturated_clusters") or []
        for idea_id in (row.get("idea_ids") or [])
    }
    autopsies = []
    for row in (novelty or {}).get("ideas") or []:
        if not isinstance(row, dict):
            continue
        route = str(row.get("route_key") or "")
        if route in winner_routes and row.get("novelty_band") != "repeat":
            continue
        reason = "too_close_to_existing_idea" if str(row.get("idea_id")) in saturated_ids or row.get("novelty_band") == "repeat" else "too_far_without_plausibility" if float(row.get("novelty_score") or 0.0) >= 0.8 else "wrong_timing_or_route"
        autopsies.append({
            "idea_id": row.get("idea_id"),
            "route_key": route,
            "kind": row.get("kind"),
            "failure_reason": reason,
            "next_action": "stop_rephrasing" if reason == "too_close_to_existing_idea" else "pair_with_parent_winner" if reason == "too_far_without_plausibility" else "retry_in_different_time_phase",
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Classifies failed ideas as too close, too far, wrong treatment, stale route, or bad timing.",
        "autopsies": autopsies[:40],
        "stop_rephrasing_ideas": [row.get("idea_id") for row in autopsies if row.get("next_action") == "stop_rephrasing"][:20],
    }


def mutation_grammar_gap_finder(novelty: dict[str, Any] | None = None, state: dict[str, Any] | None = None) -> dict[str, Any]:
    tried = set()
    for row in (novelty or {}).get("ideas") or []:
        tried.update(_idea_tokens(row.get("treatment"), row.get("text"), row.get("kind")) if isinstance(row, dict) else set())
    expected = {
        "indicator_shuffle": {"indicator", "shuffle"},
        "session_phase_shift": {"session", "phase", "time"},
        "inverse_route": {"invert", "opposite"},
        "volatility_gate": {"volatility", "gate"},
        "holdout_repair": {"holdout", "repair"},
        "wide_structural": {"wide", "structural"},
        "tight_confirmation": {"tight", "confirm"},
        "recipe_crossbreed": {"recipe", "crossbreed"},
    }
    gaps = []
    for name, tokens in expected.items():
        coverage = len(tokens & tried) / max(1, len(tokens))
        if coverage < 0.5:
            gaps.append({
                "grammar": name,
                "missing_tokens": sorted(tokens - tried),
                "coverage": round(coverage, 4),
                "suggested_action": f"try_{name}",
                "route_key": ((state or {}).get("breakthrough_detector") or {}).get("top_breakthrough", {}).get("route_key") or "",
            })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Finds mutation shapes the hunt has not tried enough yet.",
        "gaps": gaps,
        "top_gap": (gaps or [{}])[0],
    }


def novelty_budget_governor(
    leaps: dict[str, Any] | None = None,
    gaps: dict[str, Any] | None = None,
    time_plan: dict[str, Any] | None = None,
) -> dict[str, Any]:
    phase = (time_plan or {}).get("phase") or "middle"
    base_pct = {"early": 28.0, "middle": 18.0, "late": 8.0}.get(str(phase), 18.0)
    funded = list((leaps or {}).get("fund") or [])
    gap = (gaps or {}).get("top_gap") or {}
    focus_routes = _ordered_unique([row.get("route_key") for row in funded if row.get("route_key")] + [gap.get("route_key")])
    assignments = []
    for idx, row in enumerate(funded[:3]):
        assignments.append({
            "worker": f"worker_{idx + 1}",
            "route_key": row.get("route_key") or gap.get("route_key") or "",
            "action": "creative_leap_probe",
            "treatment": row.get("treatment") or gap.get("suggested_action"),
            "mutation_width": "wide" if phase != "late" else "medium",
            "budget_pct": round(base_pct / max(1, min(3, len(funded))), 2),
        })
    if gap and len(assignments) < 4:
        assignments.append({
            "worker": f"worker_{len(assignments) + 1}",
            "route_key": gap.get("route_key") or "",
            "action": gap.get("suggested_action") or "creative_gap_probe",
            "treatment": gap.get("grammar"),
            "mutation_width": "wide" if phase != "late" else "medium",
            "budget_pct": round(max(4.0, base_pct * 0.35), 2),
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Reserves a controlled worker/batch slice for genuinely new ideas.",
        "novelty_budget_pct": round(base_pct, 4),
        "phase": phase,
        "focus_routes": focus_routes[:12],
        "assignments": assignments,
        "mutation_width": "wide" if phase != "late" else "medium",
        "batch_size_multiplier": round(1.0 + base_pct / 100.0, 4),
    }


def idea_lineage_map(novelty: dict[str, Any] | None = None, state: dict[str, Any] | None = None) -> dict[str, Any]:
    nodes = []
    edges = []
    parent_by_route = {
        str(row.get("route_key") or ""): str(row.get("variant") or row.get("hypothesis_id") or "")
        for row in (state or {}).get("top_candidates") or []
        if isinstance(row, dict) and row.get("route_key")
    }
    for row in (novelty or {}).get("ideas") or []:
        if not isinstance(row, dict):
            continue
        idea_id = str(row.get("idea_id") or "")
        route = str(row.get("route_key") or "")
        nodes.append({"id": idea_id, "kind": row.get("kind"), "route_key": route, "novelty_score": row.get("novelty_score")})
        parent = parent_by_route.get(route)
        if parent:
            nodes.append({"id": parent, "kind": "parent_candidate", "route_key": route})
            edges.append({"source": parent, "target": idea_id, "relationship": "inspired"})
        source = str(row.get("source") or "")
        if source:
            edges.append({"source": source, "target": idea_id, "relationship": "generated"})
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Tracks which hypotheses and recipes descended from prior ideas, winners, or failed probes.",
        "nodes": nodes[:120],
        "edges": edges[:160],
        "node_count": len(nodes),
        "edge_count": len(edges),
    }


def creative_brief_compiler(
    saturation: dict[str, Any] | None = None,
    autopsy: dict[str, Any] | None = None,
    gaps: dict[str, Any] | None = None,
    governor: dict[str, Any] | None = None,
    lineage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    top_gap = (gaps or {}).get("top_gap") or {}
    memo = [
        f"Stop rephrasing: {len((autopsy or {}).get('stop_rephrasing_ideas') or [])} saturated ideas.",
        f"Saturation clusters: {(saturation or {}).get('saturation_count') or 0}.",
        f"Try shape: {top_gap.get('grammar') or 'none'} via {top_gap.get('suggested_action') or 'standard_probe'}.",
        f"Novelty budget: {(governor or {}).get('novelty_budget_pct') or 0}% in {(governor or {}).get('phase') or 'middle'} phase.",
        f"Lineage: {(lineage or {}).get('node_count') or 0} idea nodes.",
    ]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Short next-cycle creative brief: avoid repetition, name useful weirdness, reserve novelty budget.",
        "memo": memo,
        "summary": " | ".join(memo),
        "focus_routes": (governor or {}).get("focus_routes") or [],
    }


def creative_imagination_suite(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    novelty = idea_novelty_ledger(state)
    saturation = idea_saturation_detector(novelty)
    leaps = creative_leap_scorer(novelty, state)
    autopsy = failed_imagination_autopsy(novelty, saturation, state)
    gaps = mutation_grammar_gap_finder(novelty, state)
    governor = novelty_budget_governor(leaps, gaps, state.get("time_aware_hunt_plan") if isinstance(state.get("time_aware_hunt_plan"), dict) else {})
    lineage = idea_lineage_map(novelty, state)
    brief = creative_brief_compiler(saturation, autopsy, gaps, governor, lineage)
    return {
        "idea_novelty_ledger": novelty,
        "idea_saturation_detector": saturation,
        "creative_leap_scorer": leaps,
        "failed_imagination_autopsy": autopsy,
        "mutation_grammar_gap_finder": gaps,
        "novelty_budget_governor": governor,
        "idea_lineage_map": lineage,
        "creative_brief_compiler": brief,
    }


def learning_module_registry(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    specs = [
        ("memory", ["memory_reliability_scorer", "memory_falsification_queue", "belief_retirement_engine", "memory_conflict_arbiter"], "cross-run trust, falsification, and retire/avoid decisions"),
        ("hypothesis", ["hypothesis_factory", "hypothesis_market_maker", "real_time_bet_sizer", "learning_stop_loss"], "hypothesis pricing, focus, and stop-loss decisions"),
        ("attention", ["attention_ledger", "wasted_spend_autopsy", "budget_reallocator", "time_aware_hunt_plan"], "spend efficiency, budget reallocation, and timing decisions"),
        ("creativity", ["idea_novelty_ledger", "creative_leap_scorer", "novelty_budget_governor", "creative_brief_compiler"], "novelty, creative leap, and mutation-gap decisions"),
        ("policy", ["compiled_hunt_policy", "policy_executor", "policy_tournament", "policy_safety_rail"], "policy selection and execution decisions"),
        ("runtime", ["runtime_decision_kernel", "runtime_guardrails", "worker_job_contracts", "runtime_command_adapter"], "worker commands and guardrailed runtime execution"),
    ]
    modules = []
    for module_id, artifacts, role in specs:
        present = [name for name in artifacts if isinstance(state.get(name), dict) and state.get(name)]
        focus_routes: list[str] = []
        avoid_routes: list[str] = []
        for name in present:
            artifact = state.get(name) if isinstance(state.get(name), dict) else {}
            focus_routes.extend(artifact.get("focus_routes") or artifact.get("safe_focus_routes") or [])
            avoid_routes.extend(artifact.get("avoid_routes") or artifact.get("retire_routes") or [])
        modules.append({
            "module_id": module_id,
            "artifacts": artifacts,
            "present_artifacts": present,
            "coverage": round(len(present) / max(1, len(artifacts)), 4),
            "influences": role,
            "focus_routes": _ordered_unique(focus_routes)[:12],
            "avoid_routes": _ordered_unique(avoid_routes)[:12],
            "status": "active" if present else "missing",
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Registry of learning modules and which runtime decisions they influence.",
        "modules": modules,
        "active_modules": [row.get("module_id") for row in modules if row.get("status") == "active"],
    }


def module_contribution_attribution(registry: dict[str, Any] | None = None, state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    winner_routes = {str(row.get("route_key") or "") for row in state.get("top_candidates") or [] if isinstance(row, dict)}
    saved_routes = set((state.get("wasted_spend_autopsy") or {}).get("avoid_routes") or [])
    saved_routes.update((state.get("learning_stop_loss") or {}).get("avoid_routes") or [])
    rows = []
    for module in (registry or {}).get("modules") or []:
        if not isinstance(module, dict):
            continue
        focus = set(str(route) for route in module.get("focus_routes") or [])
        avoid = set(str(route) for route in module.get("avoid_routes") or [])
        winner_hits = len(focus & winner_routes)
        budget_saves = len(avoid & saved_routes)
        contribution = winner_hits * 35.0 + budget_saves * 22.0 + float(module.get("coverage") or 0.0) * 20.0
        rows.append({
            "module_id": module.get("module_id"),
            "winner_route_hits": winner_hits,
            "budget_save_hits": budget_saves,
            "coverage": module.get("coverage"),
            "contribution_score": round(contribution, 4),
            "explanation": f"focus_hits={winner_hits}; save_hits={budget_saves}; coverage={module.get('coverage')}",
        })
    rows.sort(key=lambda row: float(row.get("contribution_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Attributes useful focus, stop-loss, winner discovery, and budget-save decisions to modules.",
        "attribution": rows,
        "top_module": (rows or [{}])[0],
    }


def module_conflict_detector(registry: dict[str, Any] | None = None) -> dict[str, Any]:
    focus_by_route: dict[str, list[str]] = {}
    avoid_by_route: dict[str, list[str]] = {}
    for module in (registry or {}).get("modules") or []:
        if not isinstance(module, dict):
            continue
        module_id = str(module.get("module_id") or "")
        for route in module.get("focus_routes") or []:
            focus_by_route.setdefault(str(route), []).append(module_id)
        for route in module.get("avoid_routes") or []:
            avoid_by_route.setdefault(str(route), []).append(module_id)
    conflicts = []
    for route in sorted(set(focus_by_route) & set(avoid_by_route)):
        conflicts.append({
            "route_key": route,
            "focus_modules": sorted(set(focus_by_route.get(route) or [])),
            "avoid_modules": sorted(set(avoid_by_route.get(route) or [])),
            "resolution": "pause_or_shadow_test" if len(set(avoid_by_route.get(route) or [])) >= 2 else "downweight_focus",
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Detects disagreements between modules such as novelty funding a route that memory or stop-loss wants killed.",
        "conflicts": conflicts[:40],
        "conflict_count": len(conflicts),
        "pause_routes": _ordered_unique([row.get("route_key") for row in conflicts if row.get("resolution") == "pause_or_shadow_test"])[:12],
        "downweight_routes": _ordered_unique([row.get("route_key") for row in conflicts])[:12],
    }


def module_reliability_scorer(
    registry: dict[str, Any] | None = None,
    attribution: dict[str, Any] | None = None,
    conflicts: dict[str, Any] | None = None,
) -> dict[str, Any]:
    conflict_routes = set(str(row.get("route_key") or "") for row in (conflicts or {}).get("conflicts") or [])
    attribution_by_module = {
        str(row.get("module_id")): row
        for row in (attribution or {}).get("attribution") or []
        if isinstance(row, dict)
    }
    rows = []
    for module in (registry or {}).get("modules") or []:
        if not isinstance(module, dict):
            continue
        module_id = str(module.get("module_id") or "")
        attr = attribution_by_module.get(module_id, {})
        focus_conflict = len(set(module.get("focus_routes") or []) & conflict_routes)
        avoid_conflict = len(set(module.get("avoid_routes") or []) & conflict_routes)
        reliability = (
            float(module.get("coverage") or 0.0) * 25.0
            + float(attr.get("contribution_score") or 0.0)
            - (focus_conflict + avoid_conflict) * 12.0
        )
        rows.append({
            "module_id": module_id,
            "reliability_score": round(reliability, 4),
            "decision": "trust" if reliability >= 45.0 else "challenge" if reliability >= 18.0 else "downweight",
            "conflict_count": focus_conflict + avoid_conflict,
            "contribution_score": attr.get("contribution_score") or 0.0,
        })
    rows.sort(key=lambda row: float(row.get("reliability_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Scores learning modules by contribution, coverage, and conflict rate.",
        "scores": rows,
        "trust_modules": [row.get("module_id") for row in rows if row.get("decision") == "trust"],
        "challenge_modules": [row.get("module_id") for row in rows if row.get("decision") == "challenge"],
        "downweight_modules": [row.get("module_id") for row in rows if row.get("decision") == "downweight"],
    }


def module_ablation_planner(registry: dict[str, Any] | None = None, reliability: dict[str, Any] | None = None) -> dict[str, Any]:
    challenge = set(str(module) for module in (reliability or {}).get("challenge_modules") or [])
    downweight = set(str(module) for module in (reliability or {}).get("downweight_modules") or [])
    tests = []
    for module in (registry or {}).get("modules") or []:
        if not isinstance(module, dict):
            continue
        module_id = str(module.get("module_id") or "")
        if module_id not in challenge and module_id not in downweight:
            continue
        tests.append({
            "test_id": _stable_hash({"module_ablation": module_id}),
            "module_id": module_id,
            "shadow_test": f"compare_runtime_focus_without_{module_id}",
            "focus_routes": module.get("focus_routes") or [],
            "avoid_routes": module.get("avoid_routes") or [],
            "reason": "low_or_uncertain_module_reliability",
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Plans shadow ablations to test whether a learning module is actually improving runtime choices.",
        "tests": tests[:20],
        "test_count": len(tests),
    }


def module_budget_governor(reliability: dict[str, Any] | None = None, conflicts: dict[str, Any] | None = None) -> dict[str, Any]:
    scores = [row for row in (reliability or {}).get("scores") or [] if isinstance(row, dict)]
    total = sum(max(5.0, float(row.get("reliability_score") or 0.0)) for row in scores) or 1.0
    allocations = []
    for row in scores:
        score = max(5.0, float(row.get("reliability_score") or 0.0))
        pct = score / total * 100.0
        allocations.append({
            "module_id": row.get("module_id"),
            "influence_pct": round(pct, 4),
            "decision": row.get("decision"),
            "runtime_weight": round(max(0.25, min(1.35, pct / 20.0)), 4),
        })
    allocations.sort(key=lambda row: float(row.get("influence_pct") or 0.0), reverse=True)
    trusted = {str(row.get("module_id")) for row in allocations if row.get("decision") == "trust"}
    batch_mult = 1.0 + min(0.18, len(trusted) * 0.025) - min(0.18, int((conflicts or {}).get("conflict_count") or 0) * 0.03)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Allocates influence budget among memory, hypothesis, attention, creativity, policy, and runtime modules.",
        "allocations": allocations,
        "top_module": (allocations or [{}])[0],
        "batch_size_multiplier": round(max(0.72, min(1.18, batch_mult)), 4),
        "pause_routes": (conflicts or {}).get("pause_routes") or [],
        "downweight_routes": (conflicts or {}).get("downweight_routes") or [],
    }


def learning_system_self_audit(
    registry: dict[str, Any] | None = None,
    reliability: dict[str, Any] | None = None,
    conflicts: dict[str, Any] | None = None,
    budget: dict[str, Any] | None = None,
) -> dict[str, Any]:
    modules = (registry or {}).get("modules") or []
    scores = (reliability or {}).get("scores") or []
    top_alloc = float(((budget or {}).get("top_module") or {}).get("influence_pct") or 0.0)
    missing = [row.get("module_id") for row in modules if row.get("status") != "active"]
    downweight = (reliability or {}).get("downweight_modules") or []
    conflict_count = int((conflicts or {}).get("conflict_count") or 0)
    dominance_risk = top_alloc >= 45.0
    stale_risk = bool(missing or downweight)
    health = 100.0 - conflict_count * 7.0 - len(downweight) * 9.0 - (15.0 if dominance_risk else 0.0) - len(missing) * 4.0
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Self-audit of the learning system: dominance, conflicts, stale modules, blind spots, and overfit risk.",
        "health_score": round(max(0.0, min(100.0, health)), 4),
        "module_count": len(modules),
        "active_module_count": len([row for row in modules if row.get("status") == "active"]),
        "conflict_density": round(conflict_count / max(1, len(modules)), 4),
        "dominance_risk": dominance_risk,
        "stale_or_missing_modules": missing,
        "downweight_modules": downweight,
        "top_reliability": (scores or [{}])[0],
        "verdict": "healthy" if health >= 75.0 else "watch" if health >= 50.0 else "intervene",
    }


def meta_learning_brief(
    attribution: dict[str, Any] | None = None,
    conflicts: dict[str, Any] | None = None,
    reliability: dict[str, Any] | None = None,
    audit: dict[str, Any] | None = None,
) -> dict[str, Any]:
    top = (attribution or {}).get("top_module") or {}
    memo = [
        f"Pulling weight: {top.get('module_id') or 'none'} ({top.get('contribution_score') or 0}).",
        f"Trust: {','.join((reliability or {}).get('trust_modules') or []) or 'none'}.",
        f"Challenge: {','.join((reliability or {}).get('challenge_modules') or []) or 'none'}.",
        f"Conflicts: {(conflicts or {}).get('conflict_count') or 0}.",
        f"Health: {(audit or {}).get('health_score') or 0} / verdict={(audit or {}).get('verdict') or 'unknown'}.",
    ]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Human-readable meta-learning brief: which learning layers pull weight, which are noisy, and what to change.",
        "memo": memo,
        "summary": " | ".join(memo),
    }


def meta_learning_suite(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    registry = learning_module_registry(state)
    attribution = module_contribution_attribution(registry, state)
    conflicts = module_conflict_detector(registry)
    reliability = module_reliability_scorer(registry, attribution, conflicts)
    ablation = module_ablation_planner(registry, reliability)
    budget = module_budget_governor(reliability, conflicts)
    audit = learning_system_self_audit(registry, reliability, conflicts, budget)
    brief = meta_learning_brief(attribution, conflicts, reliability, audit)
    return {
        "learning_module_registry": registry,
        "module_contribution_attribution": attribution,
        "module_conflict_detector": conflicts,
        "module_reliability_scorer": reliability,
        "module_ablation_planner": ablation,
        "module_budget_governor": budget,
        "learning_system_self_audit": audit,
        "meta_learning_brief": brief,
    }


def _candidate_rows(state: dict[str, Any] | None = None, *, limit: int = 100) -> list[dict[str, Any]]:
    state = state or {}
    rows = (
        state.get("top_candidates")
        or state.get("top100")
        or state.get("raw_leaderboard")
        or state.get("promotion_readiness_top100")
        or (state.get("truth_first_promotion_objective") or {}).get("leaderboard")
        or (state.get("promotability_pareto_frontier") or {}).get("frontier")
        or (state.get("promotion_survivor_model") or {}).get("promotion_survivor_top10")
        or []
    )
    return [dict(row) for row in rows[:limit] if isinstance(row, dict)]


def _row_route_key(row: dict[str, Any]) -> str:
    route = str(row.get("route_key") or "")
    if route:
        return route
    try:
        return str(hunt_intel.route_key_from_row(row) or "")
    except Exception:
        return ""


def causal_intervention_scheduler(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    candidates = _candidate_rows(state, limit=40)
    sources = [
        ("winner_revalidation", [_row_route_key(row) for row in candidates[:8]], "revalidate_top_live_beater_route"),
        ("hypothesis_probe", (state.get("hypothesis_market_maker") or {}).get("focus_routes") or [], "controlled_hypothesis_probe"),
        ("creative_leap", (state.get("creative_leap_scorer") or {}).get("focus_routes") or [], "wild_indicator_shuffle_probe"),
        ("policy_focus", (state.get("policy_executor") or {}).get("focus_routes") or [], "policy_controlled_probe"),
        ("runtime_focus", (state.get("runtime_command_adapter") or {}).get("focus_routes") or [], "runtime_controlled_probe"),
        ("module_conflict", (state.get("module_conflict_detector") or {}).get("downweight_routes") or [], "pause_vs_probe_shadow_test"),
        ("route_gap", [row.get("route_key") for row in ((state.get("search_space_coverage_map") or {}).get("blind_spots") or [])], "coverage_gap_probe"),
    ]
    interventions = []
    seen: set[tuple[str, str]] = set()
    for source, routes, action in sources:
        for route in routes or []:
            route = str(route or "")
            if not route or (source, route) in seen:
                continue
            seen.add((source, route))
            score = 30.0
            if route in {_row_route_key(row) for row in candidates[:10]}:
                score += 35.0
            if source == "module_conflict":
                score += 20.0
            if source == "creative_leap":
                score += 12.0
            interventions.append({
                "intervention_id": _stable_hash({"runtime_science_intervention": source, "route": route}),
                "source": source,
                "route_key": route,
                "action": action,
                "control": "hold_current_live_baseline",
                "treatment": source,
                "priority_score": round(score, 4),
                "success_metric": "live_beater_pnl_and_promotion_readiness",
                "failure_metric": "alias_or_fragile_winner_rate",
            })
    interventions.sort(key=lambda row: float(row.get("priority_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Schedules controlled causal interventions to test which hunt moves create live-beating, promotion-ready variants.",
        "interventions": interventions[:24],
        "focus_routes": _ordered_unique([row.get("route_key") for row in interventions])[:12],
        "top_intervention": (interventions or [{}])[0],
        "intervention_count": len(interventions),
    }


def experiment_power_calculator(
    state: dict[str, Any] | None = None,
    scheduler: dict[str, Any] | None = None,
) -> dict[str, Any]:
    state = state or {}
    telemetry = state.get("streaming_telemetry") if isinstance(state.get("streaming_telemetry"), dict) else {}
    cycle_yield = state.get("cycle_yield") if isinstance(state.get("cycle_yield"), dict) else {}
    scored = int(telemetry.get("scored_total") or cycle_yield.get("recent_scored_total") or 0)
    winners = int(telemetry.get("live_beaters") or telemetry.get("winners") or cycle_yield.get("recent_reported_winners") or 0)
    base_power = min(100.0, scored / 600.0 * 55.0 + winners * 12.0)
    rows = []
    for intervention in (scheduler or {}).get("interventions") or []:
        if not isinstance(intervention, dict):
            continue
        route = str(intervention.get("route_key") or "")
        route_attempts = 0
        for arm in state.get("route_arms") or []:
            if isinstance(arm, dict) and str(arm.get("route_key") or "") == route:
                route_attempts = int(arm.get("attempts") or 0)
                break
        power = min(100.0, base_power + route_attempts * 8.0)
        rows.append({
            "intervention_id": intervention.get("intervention_id"),
            "route_key": route,
            "power_score": round(power, 4),
            "sample_status": "powered" if power >= 70.0 else "thin" if power >= 35.0 else "underpowered",
            "recommended_min_scored": max(300, int(900 - min(600, scored))),
            "reason": f"scored={scored}; winners={winners}; route_attempts={route_attempts}",
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Estimates whether active hunt lessons have enough sample power to trust or need more data.",
        "scored_total": scored,
        "live_beaters": winners,
        "overall_power_score": round(base_power, 4),
        "interventions": rows[:24],
        "underpowered_experiments": [row for row in rows if row.get("sample_status") == "underpowered"][:12],
        "ready_experiments": [row for row in rows if row.get("sample_status") == "powered"][:12],
    }


def winner_fragility_profiler(state: dict[str, Any] | None = None) -> dict[str, Any]:
    profiles = []
    for row in _candidate_rows(state, limit=40):
        route = _row_route_key(row)
        delta = float(row.get("step2_delta_vs_active") or row.get("delta_vs_active") or 0.0)
        pnl = float(row.get("step2_pnl") or 0.0)
        readiness = float(row.get("promotion_readiness_score") or row.get("robustness_score") or 50.0)
        tags = [str(tag) for tag in (row.get("learning_tags") or row.get("risk_tags") or [])]
        risk_tag_count = len([tag for tag in tags if any(token in tag for token in ("thin", "weak", "overfit", "concentrated", "alias"))])
        fragility = 100.0 - min(45.0, max(0.0, delta) * 0.60) - min(40.0, readiness * 0.45) + risk_tag_count * 9.0
        fragility = max(0.0, min(100.0, fragility))
        profiles.append({
            "variant": row.get("variant"),
            "route_key": route,
            "step2_pnl": pnl,
            "delta_vs_active": delta,
            "promotion_readiness_score": readiness,
            "fragility_score": round(fragility, 4),
            "status": "stress_first" if fragility >= 65.0 else "watch" if fragility >= 42.0 else "robust_candidate",
            "why": "thin_edge_or_proxy_risk" if fragility >= 65.0 else "moderate_sensitivity" if fragility >= 42.0 else "large_edge_or_readiness_support",
        })
    profiles.sort(key=lambda row: float(row.get("fragility_score") or 0.0), reverse=True)
    tests = [
        {
            "test_id": _stable_hash({"fragility_test": row.get("variant"), "route": row.get("route_key")}),
            "variant": row.get("variant"),
            "route_key": row.get("route_key"),
            "action": "winner_fragility_stress",
            "mutation_width": "tight",
            "reason": row.get("why"),
        }
        for row in profiles
        if row.get("status") in {"stress_first", "watch"}
    ]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Profiles which live-beating winners look fragile under small perturbations before promotion review.",
        "profiles": profiles[:40],
        "stress_tests": tests[:16],
        "fragile_variants": [row.get("variant") for row in profiles if row.get("status") == "stress_first"][:12],
        "top_fragility": (profiles or [{}])[0],
    }


def live_beater_source_attribution(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    source_routes = {
        "memory": set((state.get("memory_falsification_queue") or {}).get("focus_routes") or []),
        "hypothesis": set((state.get("hypothesis_market_maker") or {}).get("focus_routes") or []),
        "attention": set((state.get("budget_reallocator") or {}).get("focus_routes") or []),
        "creativity": set((state.get("creative_leap_scorer") or {}).get("focus_routes") or []),
        "policy": set((state.get("policy_executor") or {}).get("focus_routes") or []),
        "runtime": set((state.get("runtime_command_adapter") or {}).get("focus_routes") or []),
    }
    rows = []
    counts: dict[str, int] = {}
    for candidate in _candidate_rows(state, limit=100):
        route = _row_route_key(candidate)
        matched = [source for source, routes in source_routes.items() if route in routes]
        if not matched:
            matched = [str(candidate.get("mutation_lane") or candidate.get("source") or "unknown")]
        for source in matched:
            counts[source] = counts.get(source, 0) + 1
        rows.append({
            "variant": candidate.get("variant"),
            "route_key": route,
            "sources": matched,
            "step2_pnl": candidate.get("step2_pnl"),
            "delta_vs_active": candidate.get("step2_delta_vs_active"),
        })
    if not rows:
        for source, routes in source_routes.items():
            for route in routes:
                route = str(route or "")
                if not route:
                    continue
                counts[source] = counts.get(source, 0) + 1
                rows.append({
                    "variant": "",
                    "route_key": route,
                    "sources": [source],
                    "step2_pnl": None,
                    "delta_vs_active": None,
                })
    source_counts = [
        {"source": source, "live_beater_count": count}
        for source, count in sorted(counts.items(), key=lambda item: item[1], reverse=True)
    ]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Attributes live-beating variants to the learning sources that suggested their routes or treatments.",
        "attributions": rows[:100],
        "source_counts": source_counts,
        "top_source": (source_counts or [{}])[0],
        "focus_routes": _ordered_unique([row.get("route_key") for row in rows if row.get("sources") and row.get("sources")[0] != "unknown"])[:12],
    }


def adaptive_search_temperature_controller(
    state: dict[str, Any] | None = None,
    power: dict[str, Any] | None = None,
    fragility: dict[str, Any] | None = None,
) -> dict[str, Any]:
    state = state or {}
    scarcity = state.get("live_beater_scarcity_mode") if isinstance(state.get("live_beater_scarcity_mode"), dict) else {}
    conflict_count = int((state.get("module_conflict_detector") or {}).get("conflict_count") or 0)
    power_score = float((power or {}).get("overall_power_score") or 0.0)
    fragile_count = len((fragility or {}).get("fragile_variants") or [])
    if scarcity.get("enabled") or power_score < 25.0:
        temperature = "hot"
        width = "wide"
        batch_mult = 1.18
        exploration_pct = 38.0
    elif conflict_count >= 2 or fragile_count >= 2:
        temperature = "cool"
        width = "tight"
        batch_mult = 0.82
        exploration_pct = 14.0
    else:
        temperature = "warm"
        width = "medium"
        batch_mult = 1.04
        exploration_pct = 24.0
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Controls hunt search temperature from scarcity, statistical power, module conflict, and winner fragility.",
        "temperature": temperature,
        "mutation_width": width,
        "batch_size_multiplier": batch_mult,
        "exploration_pct": exploration_pct,
        "focus_routes": (state.get("live_beater_scarcity_mode") or {}).get("focus_routes") or (state.get("causal_intervention_scheduler") or {}).get("focus_routes") or [],
        "avoid_routes": (state.get("module_conflict_detector") or {}).get("pause_routes") or [],
        "reasons": {
            "scarcity_enabled": bool(scarcity.get("enabled")),
            "power_score": power_score,
            "module_conflict_count": conflict_count,
            "fragile_count": fragile_count,
        },
    }


def route_interaction_learner(state: dict[str, Any] | None = None) -> dict[str, Any]:
    buckets: dict[str, dict[str, Any]] = {}
    for row in _candidate_rows(state, limit=100):
        route = _row_route_key(row)
        if not route:
            continue
        parts = route.split("|")
        family = "|".join(parts[:2]) if len(parts) >= 2 else route
        bucket = buckets.setdefault(family, {"family": family, "routes": set(), "count": 0, "best_pnl": 0.0, "delta_sum": 0.0})
        bucket["routes"].add(route)
        bucket["count"] += 1
        bucket["best_pnl"] = max(float(bucket.get("best_pnl") or 0.0), float(row.get("step2_pnl") or 0.0))
        bucket["delta_sum"] += float(row.get("step2_delta_vs_active") or 0.0)
    if not buckets:
        focus_routes = _ordered_unique(
            list((state.get("policy_executor") or {}).get("focus_routes") or [])
            + list((state.get("runtime_command_adapter") or {}).get("focus_routes") or [])
            + list((state.get("hypothesis_market_maker") or {}).get("focus_routes") or [])
            + list((state.get("causal_intervention_scheduler") or {}).get("focus_routes") or [])
        )
        for route in focus_routes:
            parts = route.split("|")
            family = "|".join(parts[:2]) if len(parts) >= 2 else route
            bucket = buckets.setdefault(family, {"family": family, "routes": set(), "count": 0, "best_pnl": 0.0, "delta_sum": 0.0})
            bucket["routes"].add(route)
            bucket["count"] += 1
    interactions = []
    families = list(buckets.values())
    for idx, left in enumerate(families[:16]):
        for right in families[idx + 1:16]:
            left_avg = float(left.get("delta_sum") or 0.0) / max(1, int(left.get("count") or 0))
            right_avg = float(right.get("delta_sum") or 0.0) / max(1, int(right.get("count") or 0))
            synergy = (left_avg + right_avg) / 2.0 + min(20.0, (len(left.get("routes") or []) + len(right.get("routes") or [])) * 2.0)
            interactions.append({
                "left_family": left.get("family"),
                "right_family": right.get("family"),
                "interaction_score": round(synergy, 4),
                "decision": "pair_probe" if synergy >= 20.0 else "separate_budget",
            })
    interactions.sort(key=lambda row: float(row.get("interaction_score") or 0.0), reverse=True)
    focus_routes = []
    for family in sorted(families, key=lambda row: float(row.get("best_pnl") or 0.0), reverse=True):
        focus_routes.extend(sorted(family.get("routes") or []))
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Learns route family interactions so routes can be paired, separated, or budgeted together during hunts.",
        "families": [
            {**row, "routes": sorted(row.get("routes") or [])}
            for row in sorted(families, key=lambda item: float(item.get("best_pnl") or 0.0), reverse=True)[:20]
        ],
        "interactions": interactions[:24],
        "top_interaction": (interactions or [{}])[0],
        "focus_routes": _ordered_unique(focus_routes)[:12],
        "avoid_routes": [],
    }


def false_discovery_firewall(
    state: dict[str, Any] | None = None,
    power: dict[str, Any] | None = None,
    fragility: dict[str, Any] | None = None,
) -> dict[str, Any]:
    underpowered_routes = {str(row.get("route_key") or "") for row in (power or {}).get("underpowered_experiments") or []}
    fragile_by_variant = {
        str(row.get("variant") or ""): row
        for row in (fragility or {}).get("profiles") or []
        if isinstance(row, dict)
    }
    flags = []
    for row in _candidate_rows(state, limit=100):
        variant = str(row.get("variant") or "")
        route = _row_route_key(row)
        fragility_score = float((fragile_by_variant.get(variant) or {}).get("fragility_score") or 0.0)
        reasons = []
        if route in underpowered_routes:
            reasons.append("underpowered_route_lesson")
        if fragility_score >= 65.0:
            reasons.append("high_winner_fragility")
        if float(row.get("promotion_readiness_score") or 100.0) < 55.0:
            reasons.append("low_promotion_readiness_proxy")
        if reasons:
            flags.append({
                "variant": variant,
                "route_key": route,
                "risk_score": round(fragility_score + len(reasons) * 12.0, 4),
                "reasons": reasons,
                "decision": "quarantine_until_stressed" if "high_winner_fragility" in reasons else "require_more_power",
            })
    flags.sort(key=lambda row: float(row.get("risk_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Blocks likely false discoveries before they consume hunt budget or enter promotion review.",
        "flags": flags[:40],
        "flag_count": len(flags),
        "quarantine_variants": [row.get("variant") for row in flags if row.get("decision") == "quarantine_until_stressed"][:20],
        "avoid_routes": _ordered_unique([row.get("route_key") for row in flags if row.get("decision") == "quarantine_until_stressed"])[:12],
        "firewall_ok": not flags,
    }


def hunt_strategy_compiler_v2(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    scheduler = state.get("causal_intervention_scheduler") if isinstance(state.get("causal_intervention_scheduler"), dict) else {}
    temperature = state.get("adaptive_search_temperature_controller") if isinstance(state.get("adaptive_search_temperature_controller"), dict) else {}
    interactions = state.get("route_interaction_learner") if isinstance(state.get("route_interaction_learner"), dict) else {}
    firewall = state.get("false_discovery_firewall") if isinstance(state.get("false_discovery_firewall"), dict) else {}
    attribution = state.get("live_beater_source_attribution") if isinstance(state.get("live_beater_source_attribution"), dict) else {}
    focus = _ordered_unique(
        list(scheduler.get("focus_routes") or [])
        + list(interactions.get("focus_routes") or [])
        + list(attribution.get("focus_routes") or [])
        + list(temperature.get("focus_routes") or [])
    )
    avoid = _ordered_unique(list(firewall.get("avoid_routes") or []) + list(temperature.get("avoid_routes") or []))
    focus = [route for route in focus if route not in set(avoid)]
    jobs = []
    for intervention in (scheduler.get("interventions") or [])[:8]:
        if not isinstance(intervention, dict):
            continue
        jobs.append({
            "job_id": intervention.get("intervention_id"),
            "action": intervention.get("action"),
            "route_key": intervention.get("route_key"),
            "mutation_width": temperature.get("mutation_width") or "medium",
            "batch_size_multiplier": temperature.get("batch_size_multiplier") or 1.0,
            "rationale": f"hunt_strategy_compiler_v2::{intervention.get('source')}",
        })
    strategy = "controlled_hot_discovery" if temperature.get("temperature") == "hot" else "fragility_first_validation" if avoid else "powered_exploit_with_challengers"
    summary = (
        f"Strategy={strategy}; temp={temperature.get('temperature')}; "
        f"focus={','.join(focus[:3]) or 'none'}; avoid={','.join(avoid[:3]) or 'none'}; "
        f"top_source={(attribution.get('top_source') or {}).get('source') or 'unknown'}."
    )
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Compiles live learning artifacts into one explicit runtime hunt strategy with focus, avoidance, budget, and evidence rules.",
        "strategy": strategy,
        "focus_routes": focus[:12],
        "avoid_routes": avoid[:12],
        "mutation_width": temperature.get("mutation_width") or "medium",
        "batch_size_multiplier": temperature.get("batch_size_multiplier") or 1.0,
        "jobs": jobs,
        "top_job": (jobs or [{}])[0],
        "summary": summary,
        "evidence_rule": "trust powered interventions; stress fragile winners; quarantine high-fragility underpowered routes",
    }


def runtime_science_suite(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    scheduler = causal_intervention_scheduler(state)
    power = experiment_power_calculator(state, scheduler)
    fragility = winner_fragility_profiler(state)
    attribution = live_beater_source_attribution(state)
    temperature = adaptive_search_temperature_controller(state, power, fragility)
    interactions = route_interaction_learner(state)
    firewall = false_discovery_firewall(state, power, fragility)
    strategy_state = {
        **state,
        "causal_intervention_scheduler": scheduler,
        "experiment_power_calculator": power,
        "winner_fragility_profiler": fragility,
        "live_beater_source_attribution": attribution,
        "adaptive_search_temperature_controller": temperature,
        "route_interaction_learner": interactions,
        "false_discovery_firewall": firewall,
    }
    strategy = hunt_strategy_compiler_v2(strategy_state)
    return {
        "causal_intervention_scheduler": scheduler,
        "experiment_power_calculator": power,
        "winner_fragility_profiler": fragility,
        "live_beater_source_attribution": attribution,
        "adaptive_search_temperature_controller": temperature,
        "route_interaction_learner": interactions,
        "false_discovery_firewall": firewall,
        "hunt_strategy_compiler_v2": strategy,
    }


def _promotion_feedback_rows(state: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    state = state or {}
    rows = (
        state.get("promotion_feedback")
        or state.get("promotion_review_feedback")
        or state.get("promotion_review_directives")
        or state.get("feedback_rows")
        or []
    )
    out = []
    for row in rows:
        if isinstance(row, dict):
            out.append(dict(row))
    return out


def lesson_survival_tracker(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    feedback = _promotion_feedback_rows(state)
    rejected_variants = {
        str(row.get("variant") or row.get("candidate") or "")
        for row in feedback
        if str(row.get("decision") or row.get("action") or "").lower() in {"reject", "rejected", "skip_route", "downweight_family"}
    }
    promoted_variants = {
        str(row.get("variant") or row.get("candidate") or "")
        for row in feedback
        if str(row.get("decision") or row.get("action") or "").lower() in {"promote", "approved", "promote_to_full_review"}
    }
    lessons = []
    for row in _candidate_rows(state, limit=100):
        variant = str(row.get("variant") or "")
        route = _row_route_key(row)
        source_rows = [
            attribution
            for attribution in ((state.get("live_beater_source_attribution") or {}).get("attributions") or [])
            if isinstance(attribution, dict) and str(attribution.get("variant") or "") == variant
        ]
        sources = _ordered_unique([source for item in source_rows for source in (item.get("sources") or [])]) or [str(row.get("mutation_lane") or "unknown")]
        survived = variant in promoted_variants
        rejected = variant in rejected_variants
        stage = "promotion_survivor" if survived else "promotion_rejected" if rejected else "hunt_live_beater"
        survival_score = float(row.get("promotion_readiness_score") or row.get("robustness_score") or 50.0)
        survival_score += max(0.0, float(row.get("step2_delta_vs_active") or 0.0)) * 0.10
        if rejected:
            survival_score -= 35.0
        if survived:
            survival_score += 25.0
        lessons.append({
            "lesson_id": _stable_hash({"lesson": variant, "route": route, "sources": sources}),
            "variant": variant,
            "route_key": route,
            "sources": sources,
            "stage": stage,
            "survival_score": round(max(0.0, min(100.0, survival_score)), 4),
            "step2_pnl": row.get("step2_pnl"),
            "promotion_readiness_score": row.get("promotion_readiness_score"),
        })
    lessons.sort(key=lambda row: float(row.get("survival_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Tracks lessons from hunt birth through promotion pressure and later survival evidence.",
        "lessons": lessons[:100],
        "survivors": [row for row in lessons if row.get("stage") == "promotion_survivor"][:20],
        "rejected": [row for row in lessons if row.get("stage") == "promotion_rejected"][:20],
        "top_lesson": (lessons or [{}])[0],
    }


def promotion_rejection_backpropagation(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    lessons = (state.get("lesson_survival_tracker") or {}).get("lessons") or []
    feedback = _promotion_feedback_rows(state)
    feedback_by_variant = {
        str(row.get("variant") or row.get("candidate") or ""): row
        for row in feedback
        if isinstance(row, dict)
    }
    updates = []
    module_penalties: dict[str, float] = {}
    avoid_routes = []
    for lesson in lessons:
        if not isinstance(lesson, dict) or lesson.get("stage") != "promotion_rejected":
            continue
        variant = str(lesson.get("variant") or "")
        route = str(lesson.get("route_key") or "")
        fb = feedback_by_variant.get(variant, {})
        reason = str(fb.get("reason") or fb.get("reject_reason") or fb.get("action") or "promotion_rejected")
        sources = lesson.get("sources") or []
        for source in sources:
            module_penalties[str(source)] = module_penalties.get(str(source), 0.0) + 18.0
        avoid_routes.append(route)
        updates.append({
            "variant": variant,
            "route_key": route,
            "reason": reason,
            "sources": sources,
            "action": "reduce_source_influence_and_retest_route",
            "penalty": 18.0 * max(1, len(sources)),
        })
    module_updates = [
        {"source": source, "penalty": round(penalty, 4), "decision": "challenge" if penalty < 35.0 else "downweight"}
        for source, penalty in sorted(module_penalties.items(), key=lambda item: item[1], reverse=True)
    ]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Backpropagates promotion rejection reasons into the learning sources, routes, and assumptions that produced them.",
        "updates": updates[:40],
        "module_updates": module_updates,
        "avoid_routes": _ordered_unique(avoid_routes)[:12],
        "rejection_count": len(updates),
        "top_update": (updates or [{}])[0],
    }


def lesson_decay_model(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    lessons = (state.get("lesson_survival_tracker") or {}).get("lessons") or []
    backprop = state.get("promotion_rejection_backpropagation") if isinstance(state.get("promotion_rejection_backpropagation"), dict) else {}
    penalized_routes = set(backprop.get("avoid_routes") or [])
    rows = []
    for lesson in lessons:
        if not isinstance(lesson, dict):
            continue
        route = str(lesson.get("route_key") or "")
        survival = float(lesson.get("survival_score") or 0.0)
        decay = 0.88 if route not in penalized_routes else 0.55
        if lesson.get("stage") == "promotion_survivor":
            decay = 1.05
        influence = max(0.0, min(120.0, survival * decay))
        rows.append({
            "lesson_id": lesson.get("lesson_id"),
            "variant": lesson.get("variant"),
            "route_key": route,
            "decay_factor": round(decay, 4),
            "influence_score": round(influence, 4),
            "decision": "retain" if influence >= 65.0 else "retest" if influence >= 35.0 else "decay",
        })
    rows.sort(key=lambda row: float(row.get("influence_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Decays stale or rejection-tainted lessons unless they keep surviving promotion pressure.",
        "lessons": rows[:100],
        "retain_routes": _ordered_unique([row.get("route_key") for row in rows if row.get("decision") == "retain"])[:12],
        "retest_routes": _ordered_unique([row.get("route_key") for row in rows if row.get("decision") == "retest"])[:12],
        "decay_routes": _ordered_unique([row.get("route_key") for row in rows if row.get("decision") == "decay"])[:12],
        "top_lesson": (rows or [{}])[0],
    }


def cross_hunt_causal_memory(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    records = []
    scheduler = state.get("causal_intervention_scheduler") if isinstance(state.get("causal_intervention_scheduler"), dict) else {}
    power_by_route = {
        str(row.get("route_key") or ""): row
        for row in ((state.get("experiment_power_calculator") or {}).get("interventions") or [])
        if isinstance(row, dict)
    }
    for intervention in scheduler.get("interventions") or []:
        if not isinstance(intervention, dict):
            continue
        route = str(intervention.get("route_key") or "")
        power = power_by_route.get(route, {})
        records.append({
            "memory_id": _stable_hash({"cross_hunt_causal": intervention.get("intervention_id"), "route": route}),
            "condition": {
                "route_key": route,
                "source": intervention.get("source"),
                "temperature": (state.get("adaptive_search_temperature_controller") or {}).get("temperature"),
            },
            "intervention": intervention.get("action"),
            "observed_result": "pending_current_hunt",
            "power_score": power.get("power_score"),
            "confidence": "usable" if float(power.get("power_score") or 0.0) >= 70.0 else "thin",
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Durable causal memory: under condition Y, intervention X produced or is testing result Z.",
        "records": records[:80],
        "record_count": len(records),
        "usable_records": [row for row in records if row.get("confidence") == "usable"][:20],
        "top_record": (records or [{}])[0],
    }


def evidence_chain_ledger(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    fragility_by_variant = {
        str(row.get("variant") or ""): row
        for row in ((state.get("winner_fragility_profiler") or {}).get("profiles") or [])
        if isinstance(row, dict)
    }
    firewall_by_variant = {
        str(row.get("variant") or ""): row
        for row in ((state.get("false_discovery_firewall") or {}).get("flags") or [])
        if isinstance(row, dict)
    }
    attribution_by_variant = {
        str(row.get("variant") or ""): row
        for row in ((state.get("live_beater_source_attribution") or {}).get("attributions") or [])
        if isinstance(row, dict)
    }
    chains = []
    for row in _candidate_rows(state, limit=100):
        variant = str(row.get("variant") or "")
        route = _row_route_key(row)
        source = attribution_by_variant.get(variant, {})
        fragility = fragility_by_variant.get(variant, {})
        firewall = firewall_by_variant.get(variant, {})
        chains.append({
            "chain_id": _stable_hash({"evidence_chain": variant, "route": route}),
            "variant": variant,
            "route_key": route,
            "step2_pnl": row.get("step2_pnl"),
            "delta_vs_active": row.get("step2_delta_vs_active"),
            "sources": source.get("sources") or [row.get("mutation_lane") or "unknown"],
            "power_score": (state.get("experiment_power_calculator") or {}).get("overall_power_score"),
            "fragility_score": fragility.get("fragility_score"),
            "firewall_decision": firewall.get("decision") or "clear",
            "promotion_readiness_score": row.get("promotion_readiness_score"),
            "promotion_outcome": "pending",
        })
    chains.sort(key=lambda row: float(row.get("step2_pnl") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Full evidence chain for each top candidate: source, route, power, fragility, firewall, and promotion status.",
        "chains": chains[:100],
        "top_chain": (chains or [{}])[0],
        "chain_count": len(chains),
    }


def learning_disagreement_court(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    cases = []
    pause_routes = []
    retest_routes = []
    module_conflicts = (state.get("module_conflict_detector") or {}).get("conflicts") or []
    firewall_routes = set((state.get("false_discovery_firewall") or {}).get("avoid_routes") or [])
    decay_routes = set((state.get("lesson_decay_model") or {}).get("decay_routes") or [])
    for conflict in module_conflicts:
        if not isinstance(conflict, dict):
            continue
        route = str(conflict.get("route_key") or "")
        ruling = "quarantine" if route in firewall_routes or route in decay_routes else "retest" if conflict.get("resolution") == "pause_or_shadow_test" else "challenge"
        if ruling == "quarantine":
            pause_routes.append(route)
        elif ruling == "retest":
            retest_routes.append(route)
        cases.append({
            "case_id": _stable_hash({"learning_court": route, "conflict": conflict}),
            "route_key": route,
            "focus_modules": conflict.get("focus_modules") or [],
            "avoid_modules": conflict.get("avoid_modules") or [],
            "evidence": {
                "firewall_flagged": route in firewall_routes,
                "lesson_decayed": route in decay_routes,
            },
            "ruling": ruling,
        })
    for route in sorted(firewall_routes - {case.get("route_key") for case in cases}):
        pause_routes.append(route)
        cases.append({
            "case_id": _stable_hash({"learning_court_firewall": route}),
            "route_key": route,
            "focus_modules": [],
            "avoid_modules": ["false_discovery_firewall"],
            "evidence": {"firewall_flagged": True},
            "ruling": "quarantine",
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Evidence court for learning disagreements: trust, challenge, quarantine, ablate, or retest.",
        "cases": cases[:40],
        "case_count": len(cases),
        "pause_routes": _ordered_unique(pause_routes)[:12],
        "retest_routes": _ordered_unique(retest_routes)[:12],
        "top_case": (cases or [{}])[0],
    }


def promotion_aware_search_objective(state: dict[str, Any] | None = None) -> dict[str, Any]:
    rows = []
    firewall_routes = set((state or {}).get("false_discovery_firewall", {}).get("avoid_routes") or [])
    decay_routes = set((state or {}).get("lesson_decay_model", {}).get("decay_routes") or [])
    for row in _candidate_rows(state, limit=100):
        route = _row_route_key(row)
        pnl = float(row.get("step2_pnl") or 0.0)
        readiness = float(row.get("promotion_readiness_score") or row.get("robustness_score") or 50.0)
        fragility = 0.0
        for profile in ((state or {}).get("winner_fragility_profiler") or {}).get("profiles") or []:
            if isinstance(profile, dict) and str(profile.get("variant") or "") == str(row.get("variant") or ""):
                fragility = float(profile.get("fragility_score") or 0.0)
                break
        penalty = (fragility * 0.65) + (35.0 if route in firewall_routes else 0.0) + (22.0 if route in decay_routes else 0.0)
        score = pnl * (0.50 + readiness / 200.0) - penalty
        rows.append({
            "variant": row.get("variant"),
            "route_key": route,
            "step2_pnl": pnl,
            "promotion_readiness_score": readiness,
            "promotion_aware_score": round(score, 4),
            "penalty": round(penalty, 4),
        })
    rows.sort(key=lambda row: float(row.get("promotion_aware_score") or 0.0), reverse=True)
    focus_routes = _ordered_unique([row.get("route_key") for row in rows[:12]])
    avoid_routes = _ordered_unique(list(firewall_routes | decay_routes))[:12]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Scores hunt output for promotable P/L, not raw P/L that is likely to die in review.",
        "leaderboard": rows[:100],
        "top_candidate": (rows or [{}])[0],
        "focus_routes": [route for route in focus_routes if route not in set(avoid_routes)][:12],
        "avoid_routes": avoid_routes,
        "batch_size_multiplier": 0.88 if avoid_routes else 1.04,
    }


def scientific_run_brief_v2(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    power = state.get("experiment_power_calculator") if isinstance(state.get("experiment_power_calculator"), dict) else {}
    court = state.get("learning_disagreement_court") if isinstance(state.get("learning_disagreement_court"), dict) else {}
    objective = state.get("promotion_aware_search_objective") if isinstance(state.get("promotion_aware_search_objective"), dict) else {}
    decay = state.get("lesson_decay_model") if isinstance(state.get("lesson_decay_model"), dict) else {}
    beliefs = [
        f"Top promotable candidate: {(objective.get('top_candidate') or {}).get('variant') or 'none'}",
        f"Search temp: {(state.get('adaptive_search_temperature_controller') or {}).get('temperature') or 'unknown'}",
        f"Lesson influence: {(decay.get('top_lesson') or {}).get('decision') or 'unknown'}",
    ]
    testing = [
        ((state.get("causal_intervention_scheduler") or {}).get("top_intervention") or {}).get("action") or "none",
        ((state.get("hunt_strategy_compiler_v2") or {}).get("top_job") or {}).get("action") or "none",
    ]
    memo = [
        f"Belief: {'; '.join(beliefs)}.",
        f"Underpowered: {len(power.get('underpowered_experiments') or [])}.",
        f"Testing now: {', '.join([item for item in testing if item and item != 'none']) or 'none'}.",
        f"Court cases: {court.get('case_count') or 0}; pause={','.join(court.get('pause_routes') or []) or 'none'}.",
        f"Pivot if firewall flags grow or powered interventions fail to improve promotion-aware score.",
    ]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Status-update brief for live scientific hunting: beliefs, changes, underpowered tests, active tests, and pivot rules.",
        "beliefs": beliefs,
        "testing_now": [item for item in testing if item and item != "none"],
        "underpowered_count": len(power.get("underpowered_experiments") or []),
        "pivot_rules": [
            "pivot on rising false-discovery flags",
            "pivot when powered interventions fail promotion-aware scoring",
            "retest any court-paused route before increasing budget",
        ],
        "summary": " | ".join(memo),
    }


def lesson_accountability_suite(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    survival = lesson_survival_tracker(state)
    backprop = promotion_rejection_backpropagation({**state, "lesson_survival_tracker": survival})
    decay = lesson_decay_model({**state, "lesson_survival_tracker": survival, "promotion_rejection_backpropagation": backprop})
    causal_memory = cross_hunt_causal_memory(state)
    evidence = evidence_chain_ledger(state)
    court_state = {
        **state,
        "lesson_survival_tracker": survival,
        "promotion_rejection_backpropagation": backprop,
        "lesson_decay_model": decay,
    }
    court = learning_disagreement_court(court_state)
    objective_state = {
        **court_state,
        "learning_disagreement_court": court,
    }
    objective = promotion_aware_search_objective(objective_state)
    brief = scientific_run_brief_v2({
        **objective_state,
        "cross_hunt_causal_memory": causal_memory,
        "evidence_chain_ledger": evidence,
        "promotion_aware_search_objective": objective,
    })
    return {
        "lesson_survival_tracker": survival,
        "promotion_rejection_backpropagation": backprop,
        "lesson_decay_model": decay,
        "cross_hunt_causal_memory": causal_memory,
        "evidence_chain_ledger": evidence,
        "learning_disagreement_court": court,
        "promotion_aware_search_objective": objective,
        "scientific_run_brief_v2": brief,
    }


def strategy_genome_registry(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    strategy = state.get("hunt_strategy_compiler_v2") if isinstance(state.get("hunt_strategy_compiler_v2"), dict) else {}
    objective = state.get("promotion_aware_search_objective") if isinstance(state.get("promotion_aware_search_objective"), dict) else {}
    adapter = state.get("runtime_command_adapter") if isinstance(state.get("runtime_command_adapter"), dict) else {}
    worker_contracts = state.get("worker_job_contracts") if isinstance(state.get("worker_job_contracts"), dict) else {}
    focus = _ordered_unique(list(strategy.get("focus_routes") or []) + list(objective.get("focus_routes") or []) + list(adapter.get("focus_routes") or []))
    avoid = _ordered_unique(list(strategy.get("avoid_routes") or []) + list(objective.get("avoid_routes") or []) + list(adapter.get("avoid_routes") or []))
    genome = {
        "genome_id": _stable_hash({"strategy_genome": strategy.get("strategy") or "current", "focus": focus, "avoid": avoid}),
        "strategy_name": strategy.get("strategy") or "current_runtime_strategy",
        "focus_routes": [route for route in focus if route not in set(avoid)][:12],
        "avoid_routes": avoid[:12],
        "mutation_width": strategy.get("mutation_width") or adapter.get("mutation_width") or "medium",
        "scoring_objective": "promotion_aware_score" if objective.get("leaderboard") else "raw_live_beating_pnl",
        "worker_mix": [contract.get("action") for contract in (worker_contracts.get("contracts") or []) if isinstance(contract, dict)][:8],
        "novelty_budget_pct": (state.get("novelty_budget_governor") or {}).get("novelty_budget_pct"),
        "stop_rules": (state.get("scientific_run_brief_v2") or {}).get("pivot_rules") or [],
        "fitness_score": float((objective.get("top_candidate") or {}).get("promotion_aware_score") or 0.0),
    }
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Registry of hunt strategies as genomes: routes, width, objective, worker mix, novelty budget, and stop rules.",
        "genomes": [genome],
        "champion": genome,
        "genome_count": 1,
    }


def strategy_mutation_engine(
    registry: dict[str, Any] | None = None,
    state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    state = state or {}
    champion = (registry or {}).get("champion") or {}
    base_focus = champion.get("focus_routes") or (state.get("causal_intervention_scheduler") or {}).get("focus_routes") or []
    avoid = set(champion.get("avoid_routes") or [])
    route_pairs = (state.get("route_interaction_learner") or {}).get("interactions") or []
    templates = [
        ("high_novelty_burst", "wide", 1.16, "information_gain"),
        ("ultra_tight_validation", "tight", 0.74, "fragility_reduction"),
        ("inverted_filter_probe", "wide", 0.92, "false_discovery_probe"),
        ("route_pair_bet", "medium", 1.04, "route_interaction_synergy"),
    ]
    challengers = []
    for name, width, batch, objective in templates:
        focus = list(base_focus)
        if name == "inverted_filter_probe":
            focus = list((state.get("lesson_decay_model") or {}).get("retest_routes") or base_focus)
        if name == "route_pair_bet" and route_pairs:
            top = route_pairs[0]
            focus = _ordered_unique([top.get("left_family"), top.get("right_family")] + list(base_focus))
        focus = [route for route in _ordered_unique(focus) if route and route not in avoid][:12]
        challengers.append({
            "genome_id": _stable_hash({"strategy_mutation": champion.get("genome_id"), "name": name, "focus": focus}),
            "parent_genome_id": champion.get("genome_id"),
            "strategy_name": name,
            "focus_routes": focus,
            "avoid_routes": list(avoid)[:12],
            "mutation_width": width,
            "batch_size_multiplier": batch,
            "scoring_objective": objective,
            "mutation_reason": f"mutate_champion::{name}",
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Creates challenger strategy genomes by mutating the current champion strategy.",
        "challengers": challengers,
        "challenger_count": len(challengers),
        "focus_routes": _ordered_unique([route for row in challengers for route in (row.get("focus_routes") or [])])[:12],
        "top_challenger": (challengers or [{}])[0],
    }


def strategy_tournament_memory(
    registry: dict[str, Any] | None = None,
    mutations: dict[str, Any] | None = None,
    state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    state = state or {}
    genomes = list((registry or {}).get("genomes") or []) + list((mutations or {}).get("challengers") or [])
    survivor_bonus = len((state.get("lesson_survival_tracker") or {}).get("survivors") or []) * 12.0
    false_penalty = int((state.get("false_discovery_firewall") or {}).get("flag_count") or 0) * 8.0
    power = float((state.get("experiment_power_calculator") or {}).get("overall_power_score") or 0.0)
    rows = []
    for genome in genomes:
        if not isinstance(genome, dict):
            continue
        score = float(genome.get("fitness_score") or 0.0) + survivor_bonus + power * 0.25 - false_penalty
        if genome.get("scoring_objective") in {"promotion_aware_score", "fragility_reduction"}:
            score += 12.0
        rows.append({
            "genome_id": genome.get("genome_id"),
            "strategy_name": genome.get("strategy_name"),
            "tournament_score": round(score, 4),
            "mutation_width": genome.get("mutation_width"),
            "batch_size_multiplier": genome.get("batch_size_multiplier") or 1.0,
            "focus_routes": genome.get("focus_routes") or [],
            "avoid_routes": genome.get("avoid_routes") or [],
            "scoring_objective": genome.get("scoring_objective"),
        })
    rows.sort(key=lambda row: float(row.get("tournament_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Tracks which strategy genomes are winning across hunt evidence, survival, power, and false-discovery pressure.",
        "leaderboard": rows,
        "champion": (rows or [{}])[0],
        "challenger": (rows[1:] or [{}])[0],
    }


def regime_conditioned_strategy_selector(
    tournament: dict[str, Any] | None = None,
    state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    state = state or {}
    champion = (tournament or {}).get("champion") or {}
    challenger = (tournament or {}).get("challenger") or champion
    scarcity = bool((state.get("live_beater_scarcity_mode") or {}).get("enabled"))
    false_flags = int((state.get("false_discovery_firewall") or {}).get("flag_count") or 0)
    underpowered = len((state.get("experiment_power_calculator") or {}).get("underpowered_experiments") or [])
    survivors = len((state.get("lesson_survival_tracker") or {}).get("survivors") or [])
    if false_flags:
        selected = challenger if challenger.get("scoring_objective") == "fragility_reduction" else champion
        regime = "false_discovery_pressure"
    elif scarcity or underpowered >= 2:
        selected = challenger if challenger.get("scoring_objective") == "information_gain" else champion
        regime = "scarcity_or_underpowered"
    elif survivors:
        selected = champion
        regime = "survivor_memory"
    else:
        selected = champion
        regime = "balanced"
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Selects strategy by current regime: scarcity, conflicts, false-discovery risk, underpowered evidence, or survivor memory.",
        "regime": regime,
        "selected_strategy": selected,
        "focus_routes": selected.get("focus_routes") or [],
        "avoid_routes": selected.get("avoid_routes") or [],
        "mutation_width": selected.get("mutation_width") or "medium",
        "batch_size_multiplier": selected.get("batch_size_multiplier") or 1.0,
    }


def meta_objective_optimizer(
    state: dict[str, Any] | None = None,
    selector: dict[str, Any] | None = None,
) -> dict[str, Any]:
    state = state or {}
    false_flags = int((state.get("false_discovery_firewall") or {}).get("flag_count") or 0)
    underpowered = len((state.get("experiment_power_calculator") or {}).get("underpowered_experiments") or [])
    fragile = len((state.get("winner_fragility_profiler") or {}).get("fragile_variants") or [])
    if false_flags or fragile:
        objective = "fragility_reduction"
        weights = {"promotable_pnl": 0.45, "fragility": 0.35, "information_gain": 0.20}
    elif underpowered:
        objective = "information_gain"
        weights = {"promotable_pnl": 0.30, "fragility": 0.15, "information_gain": 0.55}
    else:
        objective = "promotable_pnl"
        weights = {"promotable_pnl": 0.62, "fragility": 0.18, "information_gain": 0.20}
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Optimizes the meta-objective for this hunt phase: P/L, promotability, information gain, or fragility reduction.",
        "objective": objective,
        "weights": weights,
        "selected_strategy": (selector or {}).get("selected_strategy") or {},
        "batch_size_multiplier": 0.92 if objective == "fragility_reduction" else 1.08 if objective == "information_gain" else 1.0,
    }


def exploration_debt_ledger(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    focused = set((state.get("runtime_command_adapter") or {}).get("focus_routes") or [])
    focused.update((state.get("strategy_genome_registry") or {}).get("champion", {}).get("focus_routes") or [])
    blind = [str(row.get("route_key") or "") for row in ((state.get("search_space_coverage_map") or {}).get("blind_spots") or []) if isinstance(row, dict)]
    novelty = (state.get("creative_leap_scorer") or {}).get("focus_routes") or []
    debts = []
    for route in _ordered_unique(blind + list(novelty)):
        if not route:
            continue
        debt = 35.0 + (25.0 if route not in focused else 0.0)
        debts.append({
            "route_key": route,
            "debt_score": debt,
            "reason": "blind_spot_or_underexplored_novelty",
            "recommended_action": "allocate_exploration_budget",
        })
    debts.sort(key=lambda row: float(row.get("debt_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Tracks exploration debt so exploitation does not collapse search onto familiar routes.",
        "debts": debts[:40],
        "debt_count": len(debts),
        "focus_routes": _ordered_unique([row.get("route_key") for row in debts])[:12],
        "top_debt": (debts or [{}])[0],
    }


def adversarial_strategy_red_team(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    selected = (state.get("regime_conditioned_strategy_selector") or {}).get("selected_strategy") or {}
    attacks = []
    if int((state.get("false_discovery_firewall") or {}).get("flag_count") or 0):
        attacks.append({"attack": "fake_winner_amplification", "severity": 85.0, "counter_test": "tight_fragility_replay"})
    if len(selected.get("focus_routes") or []) <= 1:
        attacks.append({"attack": "route_overfocus", "severity": 62.0, "counter_test": "route_breadth_probe"})
    if len((state.get("experiment_power_calculator") or {}).get("underpowered_experiments") or []):
        attacks.append({"attack": "underpowered_lesson_overfit", "severity": 72.0, "counter_test": "increase_sample_before_trust"})
    if not attacks:
        attacks.append({"attack": "baseline_strategy_smoke", "severity": 20.0, "counter_test": "shadow_challenger"})
    attacks.sort(key=lambda row: float(row.get("severity") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Adversarially attacks the selected strategy to reveal how it could find fake winners.",
        "attacks": attacks,
        "counter_tests": [
            {
                "test_id": _stable_hash({"strategy_red_team": row.get("attack")}),
                "action": row.get("counter_test"),
                "severity": row.get("severity"),
                "mutation_width": "tight" if float(row.get("severity") or 0.0) >= 70.0 else "medium",
            }
            for row in attacks
        ],
        "avoid_routes": (state.get("false_discovery_firewall") or {}).get("avoid_routes") or [],
        "top_attack": attacks[0],
    }


def autonomous_pivot_governor(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    selector = state.get("regime_conditioned_strategy_selector") if isinstance(state.get("regime_conditioned_strategy_selector"), dict) else {}
    red_team = state.get("adversarial_strategy_red_team") if isinstance(state.get("adversarial_strategy_red_team"), dict) else {}
    objective = state.get("meta_objective_optimizer") if isinstance(state.get("meta_objective_optimizer"), dict) else {}
    top_attack = red_team.get("top_attack") or {}
    severity = float(top_attack.get("severity") or 0.0)
    pivot = severity >= 70.0 or selector.get("regime") in {"false_discovery_pressure", "scarcity_or_underpowered"}
    selected = selector.get("selected_strategy") or {}
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Decides mid-hunt when to switch strategy based on live evidence, red-team risk, and meta-objective.",
        "pivot": bool(pivot),
        "pivot_reason": top_attack.get("attack") if pivot else "stay_on_selected_strategy",
        "selected_strategy": selected,
        "objective": objective.get("objective"),
        "focus_routes": selector.get("focus_routes") or [],
        "avoid_routes": _ordered_unique(list(selector.get("avoid_routes") or []) + list(red_team.get("avoid_routes") or []))[:12],
        "mutation_width": "tight" if severity >= 70.0 else selector.get("mutation_width") or "medium",
        "batch_size_multiplier": min(float(selector.get("batch_size_multiplier") or 1.0), 0.82) if severity >= 70.0 else float(selector.get("batch_size_multiplier") or 1.0),
    }


def strategy_evolution_suite(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    registry = strategy_genome_registry(state)
    mutations = strategy_mutation_engine(registry, state)
    tournament = strategy_tournament_memory(registry, mutations, state)
    selector = regime_conditioned_strategy_selector(tournament, state)
    meta_objective = meta_objective_optimizer(state, selector)
    debt = exploration_debt_ledger({**state, "strategy_genome_registry": registry})
    red_team_state = {
        **state,
        "strategy_genome_registry": registry,
        "strategy_mutation_engine": mutations,
        "strategy_tournament_memory": tournament,
        "regime_conditioned_strategy_selector": selector,
        "meta_objective_optimizer": meta_objective,
        "exploration_debt_ledger": debt,
    }
    red_team = adversarial_strategy_red_team(red_team_state)
    pivot = autonomous_pivot_governor({**red_team_state, "adversarial_strategy_red_team": red_team})
    return {
        "strategy_genome_registry": registry,
        "strategy_mutation_engine": mutations,
        "strategy_tournament_memory": tournament,
        "regime_conditioned_strategy_selector": selector,
        "meta_objective_optimizer": meta_objective,
        "exploration_debt_ledger": debt,
        "adversarial_strategy_red_team": red_team,
        "autonomous_pivot_governor": pivot,
    }


def learning_roi_ledger(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    candidates = _candidate_rows(state, limit=100)
    live_beaters = len(candidates)
    survivors = len((state.get("lesson_survival_tracker") or {}).get("survivors") or [])
    false_flags = int((state.get("false_discovery_firewall") or {}).get("flag_count") or 0)
    artifact_specs = [
        ("meta_learning", "learning_module_registry", (state.get("module_contribution_attribution") or {}).get("top_module") or {}),
        ("runtime_science", "hunt_strategy_compiler_v2", (state.get("causal_intervention_scheduler") or {}).get("top_intervention") or {}),
        ("lesson_accountability", "promotion_aware_search_objective", (state.get("promotion_aware_search_objective") or {}).get("top_candidate") or {}),
        ("strategy_evolution", "strategy_tournament_memory", (state.get("strategy_tournament_memory") or {}).get("champion") or {}),
    ]
    rows = []
    for layer, artifact, top in artifact_specs:
        present = bool(state.get(artifact))
        contribution = float(top.get("contribution_score") or top.get("promotion_aware_score") or top.get("tournament_score") or top.get("priority_score") or 0.0)
        roi = contribution + survivors * 20.0 + live_beaters * 3.0 - false_flags * 10.0
        rows.append({
            "layer": layer,
            "artifact": artifact,
            "present": present,
            "roi_score": round(max(0.0, roi if present else 0.0), 4),
            "live_beater_count": live_beaters,
            "promotion_survivor_count": survivors,
            "false_discovery_flags": false_flags,
            "decision": "earn_keep" if present and roi >= 35.0 else "watch" if present else "missing",
        })
    rows.sort(key=lambda row: float(row.get("roi_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "ROI ledger for learning artifacts: live beaters, promotion survival, false discoveries, and convergence signal.",
        "rows": rows,
        "top_roi": (rows or [{}])[0],
        "low_roi_artifacts": [row.get("artifact") for row in rows if row.get("decision") != "earn_keep"],
    }


def artifact_usefulness_pruner(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    watched = [
        "learning_module_registry",
        "causal_intervention_scheduler",
        "hunt_strategy_compiler_v2",
        "lesson_survival_tracker",
        "promotion_aware_search_objective",
        "strategy_genome_registry",
        "strategy_mutation_engine",
        "autonomous_pivot_governor",
    ]
    roi_by_artifact = {
        str(row.get("artifact")): row
        for row in ((state.get("learning_roi_ledger") or {}).get("rows") or [])
        if isinstance(row, dict)
    }
    rows = []
    for artifact in watched:
        payload = state.get(artifact) if isinstance(state.get(artifact), dict) else {}
        empty = not bool(payload)
        key_count = len(payload.keys()) if isinstance(payload, dict) else 0
        roi = float((roi_by_artifact.get(artifact) or {}).get("roi_score") or 0.0)
        useful = (not empty) and (roi >= 20.0 or artifact not in roi_by_artifact)
        rows.append({
            "artifact": artifact,
            "empty": empty,
            "key_count": key_count,
            "roi_score": roi,
            "decision": "keep" if useful else "prune_candidate" if empty else "downweight",
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Detects empty, stale, redundant, or low-ROI artifacts that should be downweighted or pruned.",
        "artifacts": rows,
        "downweight_artifacts": [row.get("artifact") for row in rows if row.get("decision") == "downweight"],
        "prune_candidates": [row.get("artifact") for row in rows if row.get("decision") == "prune_candidate"],
        "keep_artifacts": [row.get("artifact") for row in rows if row.get("decision") == "keep"],
    }


def decision_trace_explainer(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    trace = []
    sources = [
        ("runtime_packet", active_runtime_command_packet(state)),
        ("strategy", state.get("hunt_strategy_compiler_v2") or {}),
        ("promotion_objective", state.get("promotion_aware_search_objective") or {}),
        ("strategy_selector", state.get("regime_conditioned_strategy_selector") or {}),
        ("pivot_governor", state.get("autonomous_pivot_governor") or {}),
        ("firewall", state.get("false_discovery_firewall") or {}),
        ("learning_court", state.get("learning_disagreement_court") or {}),
        ("exploration_debt", state.get("exploration_debt_ledger") or {}),
    ]
    for source, payload in sources:
        if not isinstance(payload, dict) or not payload:
            continue
        trace.append({
            "source": source,
            "focus_routes": payload.get("focus_routes") or payload.get("retest_routes") or [],
            "avoid_routes": payload.get("avoid_routes") or payload.get("pause_routes") or [],
            "mutation_width": payload.get("mutation_width"),
            "batch_size_multiplier": payload.get("batch_size_multiplier"),
            "primary_reason": payload.get("strategy") or payload.get("regime") or payload.get("pivot_reason") or payload.get("description"),
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Explains which artifacts caused runtime focus, avoid, width, batch, pivot, and worker decisions.",
        "trace": trace,
        "trace_count": len(trace),
        "top_trace": (trace or [{}])[0],
    }


def control_surface_conflict_auditor(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    providers = {
        "runtime_packet": active_runtime_command_packet(state),
        "budget_reallocator": state.get("budget_reallocator") or {},
        "novelty_budget": state.get("novelty_budget_governor") or {},
        "temperature": state.get("adaptive_search_temperature_controller") or {},
        "strategy": state.get("hunt_strategy_compiler_v2") or {},
        "promotion_objective": state.get("promotion_aware_search_objective") or {},
        "strategy_selector": state.get("regime_conditioned_strategy_selector") or {},
        "pivot": state.get("autonomous_pivot_governor") or {},
    }
    width_votes = [
        {"source": key, "value": value.get("mutation_width")}
        for key, value in providers.items()
        if isinstance(value, dict) and value.get("mutation_width")
    ]
    batch_votes = [
        {"source": key, "value": float(value.get("batch_size_multiplier") or 1.0)}
        for key, value in providers.items()
        if isinstance(value, dict) and value.get("batch_size_multiplier") is not None
    ]
    conflicts = []
    if len({row.get("value") for row in width_votes}) > 1:
        conflicts.append({"control": "mutation_width", "votes": width_votes, "resolution": (width_votes[-1] or {}).get("value")})
    if batch_votes and (max(row["value"] for row in batch_votes) - min(row["value"] for row in batch_votes)) >= 0.35:
        conflicts.append({"control": "batch_size_multiplier", "votes": batch_votes, "resolution": "multiply_and_clamp"})
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Audits crowded control surfaces and reports who touched width, batch, focus, and avoid controls.",
        "width_votes": width_votes,
        "batch_votes": batch_votes,
        "conflicts": conflicts,
        "conflict_count": len(conflicts),
    }


def runtime_control_simplifier(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    trace = state.get("decision_trace_explainer") if isinstance(state.get("decision_trace_explainer"), dict) else {}
    focus = []
    avoid = []
    width = str(active_runtime_command_packet(state).get("mutation_width") or "medium")
    batch = float(active_runtime_command_packet(state).get("batch_size_multiplier") or 1.0)
    provenance = []
    for row in trace.get("trace") or []:
        if not isinstance(row, dict):
            continue
        source = row.get("source")
        focus.extend(row.get("focus_routes") or [])
        avoid.extend(row.get("avoid_routes") or [])
        if row.get("mutation_width"):
            width = str(row.get("mutation_width"))
        if row.get("batch_size_multiplier"):
            batch *= float(row.get("batch_size_multiplier") or 1.0)
        provenance.append({"source": source, "reason": row.get("primary_reason")})
    avoid = _ordered_unique(avoid)
    focus = [route for route in _ordered_unique(focus) if route not in set(avoid)]
    packet = {
        "live_only": True,
        "max_workers": 4,
        "focus_routes": focus[:12],
        "avoid_routes": avoid[:20],
        "mutation_width": width,
        "batch_size_multiplier": round(max(0.45, min(1.65, batch)), 4),
        "commands": active_runtime_command_packet(state).get("commands") or [],
    }
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Compiles layered controls into one clean final command packet with provenance.",
        "final_command_packet": packet,
        "focus_routes": packet["focus_routes"],
        "avoid_routes": packet["avoid_routes"],
        "mutation_width": packet["mutation_width"],
        "batch_size_multiplier": packet["batch_size_multiplier"],
        "provenance": provenance,
    }


def learning_cost_meter(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    artifacts = [
        key for key, value in state.items()
        if isinstance(value, dict) and key not in {"runtime_command_adapter", "learning_upgrade_directive"}
    ]
    worker_jobs = sum(
        len(contract.get("jobs") or [])
        for contract in ((state.get("worker_job_contracts") or {}).get("contracts") or [])
        if isinstance(contract, dict)
    )
    rows = []
    for artifact in artifacts:
        payload = state.get(artifact) or {}
        key_count = len(payload.keys()) if isinstance(payload, dict) else 0
        cost = key_count * 1.5 + (worker_jobs * 0.25 if artifact in {"worker_job_contracts", "strategy_mutation_engine", "autonomous_pivot_governor"} else 0.0)
        rows.append({
            "artifact": artifact,
            "key_count": key_count,
            "estimated_cost": round(cost, 4),
            "decision": "expensive" if cost >= 24.0 else "normal",
        })
    rows.sort(key=lambda row: float(row.get("estimated_cost") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Estimates computational and architecture cost per learning artifact and worker-job surface.",
        "artifact_count": len(artifacts),
        "worker_job_count": worker_jobs,
        "costs": rows[:80],
        "highest_cost": (rows or [{}])[0],
    }


def ablation_replay_harness(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    candidates = [
        "runtime_science_suite",
        "lesson_accountability_suite",
        "strategy_evolution_suite",
        "governance_suite",
    ]
    pruner = state.get("artifact_usefulness_pruner") if isinstance(state.get("artifact_usefulness_pruner"), dict) else {}
    low_roi = set((state.get("learning_roi_ledger") or {}).get("low_roi_artifacts") or [])
    tests = []
    for layer in candidates:
        priority = 50.0
        if layer == "governance_suite":
            priority += 10.0
        if any(item for item in pruner.get("downweight_artifacts") or [] if layer.split("_suite")[0] in str(item)):
            priority += 20.0
        if low_roi:
            priority += 10.0
        tests.append({
            "test_id": _stable_hash({"ablation_replay": layer}),
            "layer": layer,
            "action": "shadow_replay_without_layer",
            "priority_score": round(priority, 4),
            "success_metric": "promotion_aware_score_without_false_discovery_increase",
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Plans replay ablations with selected learning layers disabled to prove whether they helped or hurt.",
        "tests": tests,
        "test_count": len(tests),
        "top_test": (tests or [{}])[0],
    }


def architecture_fitness_brief(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    roi = state.get("learning_roi_ledger") if isinstance(state.get("learning_roi_ledger"), dict) else {}
    pruner = state.get("artifact_usefulness_pruner") if isinstance(state.get("artifact_usefulness_pruner"), dict) else {}
    conflicts = state.get("control_surface_conflict_auditor") if isinstance(state.get("control_surface_conflict_auditor"), dict) else {}
    cost = state.get("learning_cost_meter") if isinstance(state.get("learning_cost_meter"), dict) else {}
    helped = [row.get("artifact") for row in (roi.get("rows") or []) if row.get("decision") == "earn_keep"]
    hurt = list(pruner.get("downweight_artifacts") or []) + list(pruner.get("prune_candidates") or [])
    memo = [
        f"Helped: {','.join(helped[:4]) or 'none'}",
        f"Prune/watch: {','.join(hurt[:4]) or 'none'}",
        f"Control conflicts: {conflicts.get('conflict_count') or 0}",
        f"Highest cost: {(cost.get('highest_cost') or {}).get('artifact') or 'none'}",
    ]
    verdict = "simplify" if hurt or int(conflicts.get("conflict_count") or 0) >= 2 else "healthy"
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Compact governance brief: what helped, hurt, duplicated controls, cost too much, and should be pruned.",
        "helped": helped,
        "hurt_or_prune": hurt,
        "verdict": verdict,
        "summary": " | ".join(memo),
    }


def governance_suite(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    roi = learning_roi_ledger(state)
    pruner = artifact_usefulness_pruner({**state, "learning_roi_ledger": roi})
    trace = decision_trace_explainer(state)
    conflicts = control_surface_conflict_auditor(state)
    simplifier = runtime_control_simplifier({
        **state,
        "decision_trace_explainer": trace,
        "control_surface_conflict_auditor": conflicts,
    })
    cost = learning_cost_meter(state)
    ablation = ablation_replay_harness({
        **state,
        "learning_roi_ledger": roi,
        "artifact_usefulness_pruner": pruner,
    })
    brief = architecture_fitness_brief({
        **state,
        "learning_roi_ledger": roi,
        "artifact_usefulness_pruner": pruner,
        "control_surface_conflict_auditor": conflicts,
        "learning_cost_meter": cost,
    })
    return {
        "learning_roi_ledger": roi,
        "artifact_usefulness_pruner": pruner,
        "decision_trace_explainer": trace,
        "control_surface_conflict_auditor": conflicts,
        "runtime_control_simplifier": simplifier,
        "learning_cost_meter": cost,
        "ablation_replay_harness": ablation,
        "architecture_fitness_brief": brief,
    }


def learning_artifact_schema_registry(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    schemas = {
        "runtime_command_adapter": ["live_only", "focus_routes", "avoid_routes", "mutation_width", "batch_size_multiplier"],
        "worker_job_contracts": ["contracts", "contract_count"],
        "learning_roi_ledger": ["rows", "top_roi"],
        "runtime_control_simplifier": ["final_command_packet", "provenance"],
        "architecture_fitness_brief": ["verdict", "summary"],
        "strategy_genome_registry": ["genomes", "champion"],
        "promotion_aware_search_objective": ["top_candidate", "focus_routes", "avoid_routes"],
        "hunt_strategy_compiler_v2": ["strategy", "summary", "focus_routes", "avoid_routes"],
    }
    validation = []
    failures = []
    for artifact, required in schemas.items():
        payload = state.get(artifact) if isinstance(state.get(artifact), dict) else {}
        missing = [field for field in required if field not in payload]
        empty = [
            field for field in required
            if field in payload and payload.get(field) in (None, "", [], {})
            and field not in {"focus_routes", "avoid_routes"}
        ]
        ok = bool(payload) and not missing and not empty
        row = {
            "artifact": artifact,
            "required_fields": required,
            "present": bool(payload),
            "missing_fields": missing,
            "empty_fields": empty,
            "ok": ok,
        }
        validation.append(row)
        if not ok:
            failures.append(row)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Registry of critical learning artifacts and their minimum runtime contract fields.",
        "schemas": schemas,
        "validation": validation,
        "valid": not failures,
        "failure_count": len(failures),
        "failures": failures,
    }


def artifact_dependency_graph(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    schema = state.get("learning_artifact_schema_registry") if isinstance(state.get("learning_artifact_schema_registry"), dict) else {}
    pruner = state.get("artifact_usefulness_pruner") if isinstance(state.get("artifact_usefulness_pruner"), dict) else {}
    nodes = [
        "runtime_science_suite",
        "lesson_accountability_suite",
        "strategy_evolution_suite",
        "governance_suite",
        "runtime_command_adapter",
        "worker_job_contracts",
        "learning_ops_suite",
        "hunt_runner",
        "router_hunter",
    ]
    edges = [
        {"from": "runtime_science_suite", "to": "lesson_accountability_suite"},
        {"from": "runtime_science_suite", "to": "strategy_evolution_suite"},
        {"from": "lesson_accountability_suite", "to": "strategy_evolution_suite"},
        {"from": "strategy_evolution_suite", "to": "governance_suite"},
        {"from": "governance_suite", "to": "runtime_command_adapter"},
        {"from": "governance_suite", "to": "learning_ops_suite"},
        {"from": "runtime_command_adapter", "to": "worker_job_contracts"},
        {"from": "learning_ops_suite", "to": "hunt_runner"},
        {"from": "learning_ops_suite", "to": "router_hunter"},
        {"from": "worker_job_contracts", "to": "hunt_runner"},
    ]
    dirty = [str(row.get("artifact")) for row in schema.get("failures") or [] if row.get("artifact")]
    dirty += [str(item) for item in pruner.get("downweight_artifacts") or []]
    dirty += [str(item) for item in pruner.get("prune_candidates") or []]
    downstream = []
    edge_map: dict[str, list[str]] = {}
    for edge in edges:
        edge_map.setdefault(str(edge.get("from")), []).append(str(edge.get("to")))
    frontier = list(dict.fromkeys(dirty))
    seen = set(frontier)
    while frontier:
        current = frontier.pop(0)
        for child in edge_map.get(current, []):
            if child not in seen:
                seen.add(child)
                downstream.append(child)
                frontier.append(child)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Dependency map for deciding what learning artifacts can be reused versus refreshed.",
        "nodes": nodes,
        "edges": edges,
        "root_nodes": ["runtime_science_suite"],
        "dirty_artifacts": _ordered_unique(dirty),
        "downstream_dirty": _ordered_unique(downstream),
    }


def incremental_learning_cache(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    graph = state.get("artifact_dependency_graph") if isinstance(state.get("artifact_dependency_graph"), dict) else {}
    dirty = set(str(item) for item in (graph.get("dirty_artifacts") or []) + (graph.get("downstream_dirty") or []))
    artifact_names = [
        "runtime_science_suite",
        "lesson_accountability_suite",
        "strategy_evolution_suite",
        "governance_suite",
        "runtime_command_adapter",
        "worker_job_contracts",
        "learning_ops_suite",
        "hunt_runner",
        "router_hunter",
    ]
    refresh = [name for name in artifact_names if name in dirty]
    reuse = [name for name in artifact_names if name not in dirty]
    simplifier = state.get("runtime_control_simplifier") if isinstance(state.get("runtime_control_simplifier"), dict) else {}
    top = (state.get("promotion_aware_search_objective") or {}).get("top_candidate") if isinstance(state.get("promotion_aware_search_objective"), dict) else {}
    cache_key = _stable_hash({
        "packet": simplifier.get("final_command_packet") or {},
        "top_candidate": top or {},
        "dirty": sorted(dirty),
    })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Cache hint for reusing clean learning artifacts and refreshing only dirty downstream products.",
        "cache_key": cache_key,
        "reuse_artifacts": reuse,
        "refresh_artifacts": refresh,
        "reuse_count": len(reuse),
        "refresh_count": len(refresh),
        "recommendation": "reuse_clean_refresh_dirty" if refresh else "reuse_all_artifacts",
    }


def live_learning_dashboard_feed(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    simplifier = state.get("runtime_control_simplifier") if isinstance(state.get("runtime_control_simplifier"), dict) else {}
    packet = simplifier.get("final_command_packet") if isinstance(simplifier.get("final_command_packet"), dict) else {}
    pivot = state.get("autonomous_pivot_governor") if isinstance(state.get("autonomous_pivot_governor"), dict) else {}
    roi = state.get("learning_roi_ledger") if isinstance(state.get("learning_roi_ledger"), dict) else {}
    firewall = state.get("false_discovery_firewall") if isinstance(state.get("false_discovery_firewall"), dict) else {}
    ablation = state.get("ablation_replay_harness") if isinstance(state.get("ablation_replay_harness"), dict) else {}
    pruner = state.get("artifact_usefulness_pruner") if isinstance(state.get("artifact_usefulness_pruner"), dict) else {}
    status = "pivoting" if pivot.get("pivot") else "healthy"
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Compact feed designed for live five-minute hunt status updates.",
        "status": status,
        "strategy": (state.get("hunt_strategy_compiler_v2") or {}).get("strategy") if isinstance(state.get("hunt_strategy_compiler_v2"), dict) else None,
        "pivot": bool(pivot.get("pivot")),
        "pivot_reason": pivot.get("pivot_reason"),
        "top_roi": roi.get("top_roi") or {},
        "false_discovery_flags": firewall.get("flag_count") or 0,
        "active_tests": ablation.get("test_count") or 0,
        "pruning_advice": pruner.get("prune_candidates") or [],
        "focus_routes": list(packet.get("focus_routes") or [])[:12],
        "avoid_routes": list(packet.get("avoid_routes") or [])[:20],
        "mutation_width": simplifier.get("mutation_width") or packet.get("mutation_width"),
        "batch_size_multiplier": simplifier.get("batch_size_multiplier") or packet.get("batch_size_multiplier"),
    }


def hunt_runbook_compiler(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    contracts = state.get("worker_job_contracts") if isinstance(state.get("worker_job_contracts"), dict) else {}
    dashboard = state.get("live_learning_dashboard_feed") if isinstance(state.get("live_learning_dashboard_feed"), dict) else {}
    simplifier = state.get("runtime_control_simplifier") if isinstance(state.get("runtime_control_simplifier"), dict) else {}
    packet = simplifier.get("final_command_packet") if isinstance(simplifier.get("final_command_packet"), dict) else {}
    worker_steps = []
    for contract in list(contracts.get("contracts") or [])[:12]:
        if not isinstance(contract, dict):
            continue
        worker_steps.append({
            "worker": contract.get("worker"),
            "action": contract.get("action"),
            "route_key": contract.get("route_key"),
            "mutation_width": contract.get("mutation_width") or simplifier.get("mutation_width"),
            "rationale": contract.get("rationale"),
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Concrete hunt runbook that turns learning state into worker steps, guardrails, and review gates.",
        "worker_steps": worker_steps,
        "worker_count": contracts.get("contract_count") or len(worker_steps),
        "focus_routes": list(dashboard.get("focus_routes") or packet.get("focus_routes") or [])[:12],
        "avoid_routes": list(dashboard.get("avoid_routes") or packet.get("avoid_routes") or [])[:20],
        "mutation_width": dashboard.get("mutation_width") or simplifier.get("mutation_width") or packet.get("mutation_width") or "medium",
        "batch_size_multiplier": dashboard.get("batch_size_multiplier") or simplifier.get("batch_size_multiplier") or packet.get("batch_size_multiplier") or 1.0,
        "review_gates": ["live_only_filter", "promotion_review_on_rank1", "schema_registry_check", "failure_sentinel_check"],
        "pivot_triggers": ["no_live_beaters_after_status_window", "schema_failure", "sentinel_failure", "false_discovery_spike"],
        "stop_rules": ["stop_or_shrink_routes_marked_by_sentinel", "do_not_keep_variants_below_live"],
    }


def learning_failure_sentinel(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    schema = state.get("learning_artifact_schema_registry") if isinstance(state.get("learning_artifact_schema_registry"), dict) else {}
    simplifier = state.get("runtime_control_simplifier") if isinstance(state.get("runtime_control_simplifier"), dict) else {}
    runbook = state.get("hunt_runbook_compiler") if isinstance(state.get("hunt_runbook_compiler"), dict) else {}
    pruner = state.get("artifact_usefulness_pruner") if isinstance(state.get("artifact_usefulness_pruner"), dict) else {}
    pivot = state.get("autonomous_pivot_governor") if isinstance(state.get("autonomous_pivot_governor"), dict) else {}
    red_team = state.get("adversarial_strategy_red_team") if isinstance(state.get("adversarial_strategy_red_team"), dict) else {}
    failures = []
    warnings = []
    recommended_actions = []
    if schema and not schema.get("valid"):
        failures.append({"kind": "schema_registry_invalid", "detail": f"{schema.get('failure_count') or 0} critical artifacts failed minimum contract validation"})
        recommended_actions.append("repair_schema_registry_before_enforced_hunt")
    batch = float(runbook.get("batch_size_multiplier") or simplifier.get("batch_size_multiplier") or 1.0)
    if batch < 0.45 or batch > 1.75:
        failures.append({"kind": "batch_multiplier_out_of_bounds", "detail": f"batch_size_multiplier={round(batch, 4)}"})
        recommended_actions.append("clamp_runtime_batch_multiplier")
    avoid_routes = _ordered_unique(list(runbook.get("avoid_routes") or []) + list(simplifier.get("avoid_routes") or []))
    if len(avoid_routes) > 20:
        failures.append({"kind": "avoid_route_overload", "detail": f"{len(avoid_routes)} routes are blocked; learning may be over-constraining the hunt"})
        recommended_actions.append("compress_avoid_routes_to_high_confidence_blockers")
    prune_count = len(pruner.get("prune_candidates") or [])
    if prune_count > 4:
        warnings.append({"kind": "artifact_sprawl", "detail": f"{prune_count} artifacts are prune candidates"})
        recommended_actions.append("prune_or_ablate_low_usefulness_artifacts")
    top_attack = red_team.get("top_attack") if isinstance(red_team.get("top_attack"), dict) else {}
    if pivot.get("pivot") and top_attack:
        severity = str(top_attack.get("severity") or top_attack.get("risk") or "").lower()
        score = float(
            top_attack.get("attack_score")
            or top_attack.get("risk_score")
            or top_attack.get("confidence")
            or 0.0
        )
        detail = "autonomous pivot has active red-team counter-evidence; shadow-test before scaling the pivot"
        if bool(top_attack.get("hard_block")) or severity in {"critical", "block", "hard_block"} or score >= 90.0:
            failures.append({"kind": "pivot_under_attack", "detail": detail, "attack": top_attack})
            recommended_actions.append("pause_pivot_and_run_counter_test")
        else:
            warnings.append({"kind": "pivot_under_attack", "detail": detail, "attack": top_attack})
            recommended_actions.append("shadow_validate_pivot_without_global_throttle")
    pause_routes = avoid_routes[:12] if failures else []
    repair_routes = _ordered_unique(
        list(pivot.get("focus_routes") or [])
        + list(red_team.get("focus_routes") or [])
        + list(red_team.get("avoid_routes") or [])
    )[:12]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Runtime sentinel for learning-system failures that should slow, pause, or narrow a hunt.",
        "ok": not failures,
        "failure_count": len(failures),
        "failures": failures,
        "warning_count": len(warnings),
        "warnings": warnings,
        "pause_routes": pause_routes,
        "repair_routes": repair_routes,
        "recommended_actions": _ordered_unique(recommended_actions),
        "control_posture": "throttle" if failures else ("observe_and_repair" if warnings else "clear"),
        "mutation_width": "tight" if failures else None,
        "batch_size_multiplier": 0.65 if failures else None,
    }


def cross_run_artifact_warehouse(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    artifact_names = [
        "learning_artifact_schema_registry",
        "artifact_dependency_graph",
        "incremental_learning_cache",
        "live_learning_dashboard_feed",
        "hunt_runbook_compiler",
        "learning_failure_sentinel",
        "learning_roi_ledger",
        "architecture_fitness_brief",
    ]
    exports = []
    for name in artifact_names:
        payload = state.get(name) if isinstance(state.get(name), dict) else {}
        exports.append({
            "artifact": name,
            "warehouse_key": f"{name}:{_stable_hash(payload)[:12]}",
            "summary_hash": _stable_hash({"artifact": name, "payload": payload})[:16],
            "present": bool(payload),
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Manifest of learning artifacts worth persisting for cross-run comparison and querying.",
        "exports": exports,
        "export_count": len(exports),
        "top_export": exports[0] if exports else {},
        "query_hints": ["compare_schema_failures_by_run", "join_runbook_steps_to_live_beaters", "track_sentinel_failures_before_rejects"],
    }


def pre_hunt_readiness_gate(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    schema = state.get("learning_artifact_schema_registry") if isinstance(state.get("learning_artifact_schema_registry"), dict) else {}
    sentinel = state.get("learning_failure_sentinel") if isinstance(state.get("learning_failure_sentinel"), dict) else {}
    runbook = state.get("hunt_runbook_compiler") if isinstance(state.get("hunt_runbook_compiler"), dict) else {}
    simplifier = state.get("runtime_control_simplifier") if isinstance(state.get("runtime_control_simplifier"), dict) else {}
    packet = simplifier.get("final_command_packet") if isinstance(simplifier.get("final_command_packet"), dict) else {}
    checks = [
        {"check": "schema_registry_valid", "passed": bool(schema.get("valid"))},
        {"check": "failure_sentinel_ok", "passed": bool(sentinel.get("ok", True))},
        {"check": "runbook_has_workers", "passed": int(runbook.get("worker_count") or 0) > 0},
        {"check": "live_only_packet", "passed": bool(packet.get("live_only", True))},
        {"check": "route_controls_bounded", "passed": len(runbook.get("avoid_routes") or []) <= 20},
    ]
    failures = [row for row in checks if not row.get("passed")]
    ready = not failures
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Pre-hunt gate that confirms the learning layer is coherent enough to steer live-only hunting.",
        "ready": ready,
        "checks": checks,
        "failures": failures,
        "block_routes": list(sentinel.get("pause_routes") or []) if not ready else [],
        "recommendation": "hunt_ready" if ready else "repair_learning_artifacts_before_hunt",
    }


def learning_ops_suite(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    schema = learning_artifact_schema_registry(state)
    graph = artifact_dependency_graph({**state, "learning_artifact_schema_registry": schema})
    cache = incremental_learning_cache({**state, "learning_artifact_schema_registry": schema, "artifact_dependency_graph": graph})
    dashboard = live_learning_dashboard_feed(state)
    runbook = hunt_runbook_compiler({**state, "live_learning_dashboard_feed": dashboard})
    sentinel = learning_failure_sentinel({
        **state,
        "learning_artifact_schema_registry": schema,
        "hunt_runbook_compiler": runbook,
    })
    warehouse = cross_run_artifact_warehouse({
        **state,
        "learning_artifact_schema_registry": schema,
        "artifact_dependency_graph": graph,
        "incremental_learning_cache": cache,
        "live_learning_dashboard_feed": dashboard,
        "hunt_runbook_compiler": runbook,
        "learning_failure_sentinel": sentinel,
    })
    readiness = pre_hunt_readiness_gate({
        **state,
        "learning_artifact_schema_registry": schema,
        "hunt_runbook_compiler": runbook,
        "learning_failure_sentinel": sentinel,
    })
    return {
        "learning_artifact_schema_registry": schema,
        "artifact_dependency_graph": graph,
        "incremental_learning_cache": cache,
        "live_learning_dashboard_feed": dashboard,
        "hunt_runbook_compiler": runbook,
        "learning_failure_sentinel": sentinel,
        "cross_run_artifact_warehouse": warehouse,
        "pre_hunt_readiness_gate": readiness,
    }


def _search_candidate_rows(state: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    state = state or {}
    rows = state.get("top_candidates") if isinstance(state.get("top_candidates"), list) else []
    if not rows:
        rows = state.get("raw_leaderboard") if isinstance(state.get("raw_leaderboard"), list) else []
    return [row for row in rows if isinstance(row, dict)]


def _row_route(row: dict[str, Any]) -> str:
    return str(row.get("route_key") or hunt_intel.route_key_from_row(row) or "").strip()


def _row_lane(row: dict[str, Any]) -> str:
    lane = row.get("mutation_lane") or row.get("treatment") or row.get("strategy") or row.get("family_key")
    if lane:
        return str(lane)
    tags = [str(tag) for tag in row.get("learning_tags") or []]
    if "thin_holdout_edge" in tags or "no_holdout_credit" in tags:
        return "holdout_repair"
    if "weak_day_consistency" in tags:
        return "day_consistency_repair"
    if "route_narrow" in tags:
        return "edge_expansion"
    return "standard_mutation"


def _row_gene_tokens(row: dict[str, Any]) -> list[str]:
    route = _row_route(row)
    lane = _row_lane(row)
    tokens = [
        f"route:{route}" if route else "",
        f"lane:{lane}" if lane else "",
        f"family:{row.get('family_key')}" if row.get("family_key") else "",
        f"side:{row.get('side')}" if row.get("side") else "",
        f"ticker:{row.get('ticker')}" if row.get("ticker") else "",
    ]
    for field in ("indicator", "entry_signal", "exit_signal", "score_model", "timeframe", "regime"):
        if row.get(field):
            tokens.append(f"{field}:{row.get(field)}")
    for tag in row.get("learning_tags") or []:
        if tag:
            tokens.append(f"tag:{tag}")
    return _ordered_unique(tokens)


def online_causal_bandit(state: dict[str, Any] | None = None) -> dict[str, Any]:
    rows = _search_candidate_rows(state)
    lane_stats: dict[str, dict[str, Any]] = {}
    for row in rows:
        lane = _row_lane(row)
        stats = lane_stats.setdefault(lane, {"lane": lane, "attempts": 0, "live_winners": 0, "total_reward": 0.0, "routes": []})
        pnl = float(row.get("step2_pnl") or row.get("pnl") or 0.0)
        delta = float(row.get("step2_delta_vs_active") or 0.0)
        readiness = float(row.get("promotion_readiness_score") or 50.0)
        reward = max(0.0, delta) + max(0.0, pnl) * 0.04 + max(0.0, readiness - 60.0)
        stats["attempts"] += 1
        stats["live_winners"] += 1 if delta > 0.0 or bool(row.get("beats_live")) else 0
        stats["total_reward"] += reward
        route = _row_route(row)
        if route:
            stats["routes"].append(route)
    arms = []
    total_score = 0.0
    for lane, stats in lane_stats.items():
        attempts = max(1, int(stats.get("attempts") or 0))
        win_rate = float(stats.get("live_winners") or 0) / attempts
        avg_reward = float(stats.get("total_reward") or 0.0) / attempts
        uncertainty_bonus = 1.0 / (attempts ** 0.5)
        score = avg_reward * 0.7 + win_rate * 120.0 + uncertainty_bonus * 20.0
        stats["score"] = round(score, 4)
        stats["avg_reward"] = round(avg_reward, 4)
        stats["win_rate"] = round(win_rate, 4)
        stats["routes"] = _ordered_unique(stats.get("routes") or [])[:8]
        arms.append(stats)
        total_score += max(1.0, score)
    if not arms:
        packet = active_runtime_command_packet(state or {})
        routes = _ordered_unique(list(packet.get("focus_routes") or []))
        if routes:
            arms.append({
                "lane": "standard_mutation",
                "attempts": 0,
                "live_winners": 0,
                "total_reward": 0.0,
                "routes": routes[:8],
                "score": 20.0,
                "avg_reward": 0.0,
                "win_rate": 0.0,
            })
            total_score = 20.0
    arms = sorted(arms, key=lambda row: float(row.get("score") or 0.0), reverse=True)
    allocations = []
    for idx, arm in enumerate(arms[:6]):
        pct = round(100.0 * max(1.0, float(arm.get("score") or 0.0)) / max(1.0, total_score), 2)
        allocations.append({
            "lane": arm.get("lane"),
            "budget_pct": pct,
            "worker": f"worker_{(idx % 4) + 1}",
            "focus_routes": arm.get("routes") or [],
            "recommended_action": "exploit_and_mutate" if idx == 0 else "challenge_or_probe",
        })
    focus = []
    for allocation in allocations:
        focus.extend(allocation.get("focus_routes") or [])
    top_lane = (arms[0] if arms else {}).get("lane") or "standard_mutation"
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Online causal bandit for reallocating hunt budget toward mutation lanes causing live-beating lift.",
        "arms": arms,
        "allocations": allocations,
        "top_lane": top_lane,
        "focus_routes": _ordered_unique(focus)[:12],
        "mutation_width": "wide" if top_lane in {"edge_expansion", "wild_shuffle"} else "tight" if top_lane in {"holdout_repair", "day_consistency_repair"} else "medium",
        "batch_size_multiplier": 1.18 if arms and float(arms[0].get("win_rate") or 0.0) >= 0.5 else 0.92,
    }


def variant_dna_attribution(state: dict[str, Any] | None = None) -> dict[str, Any]:
    rows = _search_candidate_rows(state)
    gene_stats: dict[str, dict[str, Any]] = {}
    for row in rows:
        delta = float(row.get("step2_delta_vs_active") or 0.0)
        pnl = float(row.get("step2_pnl") or row.get("pnl") or 0.0)
        readiness = float(row.get("promotion_readiness_score") or 50.0)
        contribution = max(0.0, delta) + max(0.0, pnl) * 0.03 + max(0.0, readiness - 65.0)
        for gene in _row_gene_tokens(row):
            stats = gene_stats.setdefault(gene, {"gene": gene, "count": 0, "live_winner_count": 0, "contribution": 0.0, "variants": []})
            stats["count"] += 1
            stats["live_winner_count"] += 1 if delta > 0.0 else 0
            stats["contribution"] += contribution
            if row.get("variant"):
                stats["variants"].append(row.get("variant"))
    genes = []
    for stats in gene_stats.values():
        count = max(1, int(stats.get("count") or 0))
        stats["score"] = round(float(stats.get("contribution") or 0.0) / count, 4)
        stats["variants"] = _ordered_unique(stats.get("variants") or [])[:6]
        genes.append(stats)
    if not genes:
        packet = active_runtime_command_packet(state or {})
        for route in _ordered_unique(list(packet.get("focus_routes") or [])):
            genes.append({
                "gene": f"route:{route}",
                "count": 0,
                "live_winner_count": 0,
                "contribution": 0.0,
                "variants": [],
                "score": 10.0,
            })
    genes = sorted(genes, key=lambda row: float(row.get("score") or 0.0), reverse=True)
    focus_routes = [str(row.get("gene")).split("route:", 1)[1] for row in genes if str(row.get("gene") or "").startswith("route:")]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Attributes live-beating variants to reusable genes: route, lane, indicator, tags, side, ticker, and regime.",
        "genes": genes[:50],
        "top_positive_genes": genes[:12],
        "focus_routes": _ordered_unique(focus_routes)[:12],
        "top_gene": genes[0] if genes else {},
    }


def negative_gene_suppression(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    rows = _search_candidate_rows(state)
    feedback = state.get("promotion_feedback") if isinstance(state.get("promotion_feedback"), list) else []
    rejected = {str(row.get("variant")) for row in feedback if isinstance(row, dict) and str(row.get("decision") or "").lower() in {"reject", "rejected"}}
    bad_stats: dict[str, dict[str, Any]] = {}
    for row in rows:
        tags = {str(tag) for tag in row.get("learning_tags") or []}
        bad = (
            str(row.get("variant")) in rejected
            or float(row.get("step2_delta_vs_active") or 0.0) <= 0.0
            or float(row.get("promotion_readiness_score") or 100.0) < 55.0
            or bool(tags.intersection({"high_overfit_risk", "thin_holdout_edge", "weak_day_consistency", "ticker_concentrated", "side_concentrated"}))
        )
        if not bad:
            continue
        for gene in _row_gene_tokens(row):
            stats = bad_stats.setdefault(gene, {"gene": gene, "bad_count": 0, "variants": [], "routes": []})
            stats["bad_count"] += 1
            if row.get("variant"):
                stats["variants"].append(row.get("variant"))
            route = _row_route(row)
            if route:
                stats["routes"].append(route)
    suppressed = sorted(bad_stats.values(), key=lambda row: int(row.get("bad_count") or 0), reverse=True)
    avoid_routes = []
    for row in suppressed:
        if str(row.get("gene") or "").startswith("route:"):
            avoid_routes.append(str(row.get("gene")).split("route:", 1)[1])
        avoid_routes.extend(row.get("routes") or [])
        row["variants"] = _ordered_unique(row.get("variants") or [])[:6]
        row["routes"] = _ordered_unique(row.get("routes") or [])[:6]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Suppresses genes repeatedly associated with reject-prone, non-live-beating, or fragile candidates.",
        "suppressed_genes": suppressed[:30],
        "suppressed_count": len(suppressed),
        "avoid_routes": _ordered_unique(avoid_routes)[:12],
        "suppression_policy": "pause only when a gene is repeatedly tied to fragility or below-live outcomes",
    }


def live_winner_family_tree(state: dict[str, Any] | None = None) -> dict[str, Any]:
    rows = _search_candidate_rows(state)
    families: dict[str, dict[str, Any]] = {}
    for row in rows:
        route = _row_route(row)
        family = str(row.get("family_key") or route or "unknown")
        parent = str(row.get("parent_variant") or row.get("seed_variant") or "")
        node = {
            "variant": row.get("variant"),
            "parent": parent,
            "route_key": route,
            "pnl": row.get("step2_pnl"),
            "delta_vs_live": row.get("step2_delta_vs_active"),
            "readiness": row.get("promotion_readiness_score"),
        }
        fam = families.setdefault(family, {"family_key": family, "nodes": [], "best_delta": -1e18, "champion": {}})
        fam["nodes"].append(node)
        delta = float(row.get("step2_delta_vs_active") or 0.0)
        if delta > float(fam.get("best_delta") or -1e18):
            fam["best_delta"] = delta
            fam["champion"] = node
    family_rows = sorted(families.values(), key=lambda row: float(row.get("best_delta") or 0.0), reverse=True)
    if not family_rows:
        packet = active_runtime_command_packet(state or {})
        for route in _ordered_unique(list(packet.get("focus_routes") or [])):
            family_rows.append({
                "family_key": route,
                "nodes": [],
                "best_delta": 0.0,
                "champion": {"route_key": route, "variant": ""},
            })
    focus_routes = [((row.get("champion") or {}).get("route_key")) for row in family_rows if (row.get("champion") or {}).get("route_key")]
    for row in family_rows:
        row["node_count"] = len(row.get("nodes") or [])
        row["lineage_hash"] = _stable_hash(row.get("nodes") or [], length=14)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Tracks live winner parent-child lineages to distinguish true improvement from alias rediscovery.",
        "families": family_rows[:20],
        "family_count": len(family_rows),
        "champion_family": family_rows[0] if family_rows else {},
        "focus_routes": _ordered_unique(focus_routes)[:12],
    }


def exploration_frontier_map(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    rows = _search_candidate_rows(state)
    coverage = state.get("search_space_coverage_map") if isinstance(state.get("search_space_coverage_map"), dict) else {}
    route_counts: dict[str, int] = {}
    route_best: dict[str, float] = {}
    for row in rows:
        route = _row_route(row)
        if not route:
            continue
        route_counts[route] = route_counts.get(route, 0) + 1
        route_best[route] = max(route_best.get(route, -1e18), float(row.get("step2_delta_vs_active") or 0.0))
    blind_routes = [str(row.get("route_key") or row.get("route") or "") for row in coverage.get("blind_spots") or [] if isinstance(row, dict)]
    cells = []
    for route in _ordered_unique(blind_routes + list(route_counts.keys())):
        attempts = route_counts.get(route, 0)
        best = route_best.get(route, 0.0)
        frontier_score = (60.0 if attempts == 0 else 20.0 / attempts) + max(0.0, best) * 0.15
        cells.append({
            "route_key": route,
            "attempts": attempts,
            "best_delta_vs_live": round(best, 4),
            "frontier_score": round(frontier_score, 4),
            "recommended_action": "explore_unsearched" if attempts == 0 else "expand_edge" if best > 0 else "probe_sparse",
        })
    if not cells:
        packet = active_runtime_command_packet(state or {})
        for route in _ordered_unique(list(packet.get("focus_routes") or [])):
            cells.append({
                "route_key": route,
                "attempts": 0,
                "best_delta_vs_live": 0.0,
                "frontier_score": 20.0,
                "recommended_action": "probe_sparse",
            })
    cells = sorted(cells, key=lambda row: float(row.get("frontier_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Live map of searched versus under-searched strategy space with frontier routes to probe next.",
        "frontier_cells": cells[:30],
        "focus_routes": [row.get("route_key") for row in cells[:12] if row.get("route_key")],
        "mutation_width": "wide" if any(int(row.get("attempts") or 0) == 0 for row in cells[:3]) else "medium",
        "frontier_count": len(cells),
    }


def adaptive_worker_personalities(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    bandit = state.get("online_causal_bandit") if isinstance(state.get("online_causal_bandit"), dict) else {}
    frontier = state.get("exploration_frontier_map") if isinstance(state.get("exploration_frontier_map"), dict) else {}
    rejection = state.get("promotion_rejection_predictor_v2") if isinstance(state.get("promotion_rejection_predictor_v2"), dict) else {}
    routes = _ordered_unique(list(bandit.get("focus_routes") or []) + list(frontier.get("focus_routes") or []) + list(rejection.get("repair_routes") or []))
    personalities = [
        ("worker_1", "causal_exploiter", "exploit_and_mutate", "medium"),
        ("worker_2", "promotion_repairer", "repair_fragility", "tight"),
        ("worker_3", "frontier_scout", "explore_frontier", "wide"),
        ("worker_4", "adversarial_breaker", "stress_and_falsify", "tight"),
    ]
    assignments = []
    for idx, (worker, personality, action, width) in enumerate(personalities):
        assignments.append({
            "worker": worker,
            "personality": personality,
            "action": action,
            "route_key": (routes[idx % len(routes)] if routes else ""),
            "mutation_width": width,
            "batch_size_multiplier": 1.1 if personality == "causal_exploiter" and bandit.get("top_lane") else 0.85 if personality == "adversarial_breaker" else 1.0,
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Assigns distinct search personalities to workers and lets live causal evidence steer their routes.",
        "assignments": assignments,
        "focus_routes": routes[:12],
        "worker_count": len(assignments),
        "top_personality": assignments[0] if assignments else {},
    }


def cycle_level_learning_delta(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    bandit = state.get("online_causal_bandit") if isinstance(state.get("online_causal_bandit"), dict) else {}
    dna = state.get("variant_dna_attribution") if isinstance(state.get("variant_dna_attribution"), dict) else {}
    suppression = state.get("negative_gene_suppression") if isinstance(state.get("negative_gene_suppression"), dict) else {}
    frontier = state.get("exploration_frontier_map") if isinstance(state.get("exploration_frontier_map"), dict) else {}
    rejection = state.get("promotion_rejection_predictor_v2") if isinstance(state.get("promotion_rejection_predictor_v2"), dict) else {}
    learned = [
        f"top_lane={bandit.get('top_lane') or 'unknown'}",
        f"top_gene={(dna.get('top_gene') or {}).get('gene') or 'unknown'}",
        f"suppressed_genes={suppression.get('suppressed_count') or 0}",
        f"frontier_routes={len(frontier.get('focus_routes') or [])}",
        f"promotion_high_risk={len(rejection.get('high_risk_variants') or [])}",
    ]
    focus = _ordered_unique(list(bandit.get("focus_routes") or []) + list(dna.get("focus_routes") or []) + list(frontier.get("focus_routes") or []))
    avoid = _ordered_unique(list(suppression.get("avoid_routes") or []) + list(rejection.get("avoid_routes") or []))
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Cycle heartbeat: what changed this cycle and how the next cycle should adapt.",
        "learned": learned,
        "summary": " | ".join(learned),
        "next_actions": [
            {"action": "amplify_lane", "target": bandit.get("top_lane") or "standard_mutation"},
            {"action": "reuse_positive_gene", "target": (dna.get("top_gene") or {}).get("gene") or ""},
            {"action": "probe_frontier", "target": ((frontier.get("frontier_cells") or [{}])[0] or {}).get("route_key") or ""},
        ],
        "focus_routes": focus[:12],
        "avoid_routes": avoid[:12],
    }


def promotion_rejection_predictor_v2(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    rows = _search_candidate_rows(state)
    suppression = state.get("negative_gene_suppression") if isinstance(state.get("negative_gene_suppression"), dict) else {}
    suppressed_genes = {str(row.get("gene")) for row in suppression.get("suppressed_genes") or [] if isinstance(row, dict)}
    predictions = []
    for row in rows:
        tags = {str(tag) for tag in row.get("learning_tags") or []}
        risk = 0.0
        reasons = []
        readiness = float(row.get("promotion_readiness_score") or 65.0)
        if readiness < 70.0:
            risk += (70.0 - readiness) * 1.2
            reasons.append("low_promotion_readiness")
        for tag, weight in {
            "thin_holdout_edge": 18.0,
            "no_holdout_credit": 18.0,
            "weak_day_consistency": 15.0,
            "ticker_concentrated": 12.0,
            "side_concentrated": 10.0,
            "thin_sample": 12.0,
            "high_overfit_risk": 25.0,
        }.items():
            if tag in tags:
                risk += weight
                reasons.append(tag)
        gene_hits = [gene for gene in _row_gene_tokens(row) if gene in suppressed_genes]
        if gene_hits:
            risk += 18.0
            reasons.append("suppressed_gene_overlap")
        risk = min(100.0, round(risk, 2))
        action = "reject_like" if risk >= 70.0 else "repair_before_review" if risk >= 45.0 else "promotion_viable"
        predictions.append({
            "variant": row.get("variant"),
            "route_key": _row_route(row),
            "risk_score": risk,
            "reasons": reasons,
            "recommended_action": action,
            "readiness": readiness,
        })
    if not predictions:
        prior_predictions = state.get("promotion_reject_predictions") if isinstance(state.get("promotion_reject_predictions"), dict) else {}
        for row in prior_predictions.get("predictions") or []:
            if not isinstance(row, dict):
                continue
            risk = float(row.get("predicted_reject_probability") or row.get("reject_probability") or 0.0) * 100.0
            predictions.append({
                "variant": row.get("variant"),
                "route_key": row.get("route_key") or "",
                "risk_score": round(risk, 2),
                "reasons": ["prior_promotion_reject_prediction"],
                "recommended_action": "reject_like" if risk >= 70.0 else "repair_before_review" if risk >= 45.0 else "promotion_viable",
                "readiness": row.get("promotion_readiness_score"),
            })
    predictions = sorted(predictions, key=lambda row: float(row.get("risk_score") or 0.0), reverse=True)
    high_risk = [row for row in predictions if row.get("recommended_action") == "reject_like"]
    repair = [row for row in predictions if row.get("recommended_action") == "repair_before_review"]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Promotion rejection predictor that downranks high-P/L candidates likely to die in review while suggesting repair lanes.",
        "predictions": predictions[:50],
        "high_risk_variants": high_risk[:20],
        "repair_variants": repair[:20],
        "avoid_routes": _ordered_unique([row.get("route_key") for row in high_risk if row.get("route_key")])[:12],
        "repair_routes": _ordered_unique([row.get("route_key") for row in repair if row.get("route_key")])[:12],
        "highest_risk": predictions[0] if predictions else {},
    }


def real_time_causal_search_suite(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    bandit = online_causal_bandit(state)
    dna = variant_dna_attribution(state)
    suppression = negative_gene_suppression({**state, "variant_dna_attribution": dna})
    family_tree = live_winner_family_tree(state)
    frontier = exploration_frontier_map(state)
    rejection = promotion_rejection_predictor_v2({**state, "negative_gene_suppression": suppression})
    workers = adaptive_worker_personalities({
        **state,
        "online_causal_bandit": bandit,
        "exploration_frontier_map": frontier,
        "promotion_rejection_predictor_v2": rejection,
    })
    delta = cycle_level_learning_delta({
        **state,
        "online_causal_bandit": bandit,
        "variant_dna_attribution": dna,
        "negative_gene_suppression": suppression,
        "exploration_frontier_map": frontier,
        "promotion_rejection_predictor_v2": rejection,
    })
    return {
        "online_causal_bandit": bandit,
        "variant_dna_attribution": dna,
        "negative_gene_suppression": suppression,
        "live_winner_family_tree": family_tree,
        "exploration_frontier_map": frontier,
        "adaptive_worker_personalities": workers,
        "cycle_level_learning_delta": delta,
        "promotion_rejection_predictor_v2": rejection,
    }


def counterfactual_hunt_simulator(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    rows = _search_candidate_rows(state)
    bandit = state.get("online_causal_bandit") if isinstance(state.get("online_causal_bandit"), dict) else {}
    frontier = state.get("exploration_frontier_map") if isinstance(state.get("exploration_frontier_map"), dict) else {}
    candidate_routes = _ordered_unique(
        [row.get("route_key") for row in bandit.get("arms") or [] for _ in []]
        + list(bandit.get("focus_routes") or [])
        + list(frontier.get("focus_routes") or [])
        + [_row_route(row) for row in rows]
    )
    observed_best = {
        _row_route(row): max(0.0, float(row.get("step2_delta_vs_active") or 0.0))
        for row in rows
        if _row_route(row)
    }
    simulations = []
    for idx, route in enumerate(candidate_routes[:20]):
        attempts = sum(1 for row in rows if _row_route(row) == route)
        frontier_bonus = 25.0 if route in set(frontier.get("focus_routes") or []) else 0.0
        bandit_bonus = 20.0 if route in set(bandit.get("focus_routes") or []) else 0.0
        expected_lift = round(observed_best.get(route, 0.0) * 0.55 + frontier_bonus + bandit_bonus + max(0, 4 - attempts) * 6.0, 4)
        simulations.append({
            "counterfactual_id": _stable_hash({"route": route, "idx": idx}, length=14),
            "route_key": route,
            "hypothetical_action": "assign_worker_next_cycle",
            "observed_attempts": attempts,
            "expected_live_lift": expected_lift,
            "confidence": round(0.35 + min(0.45, attempts * 0.08), 4),
        })
    simulations = sorted(simulations, key=lambda row: float(row.get("expected_live_lift") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Counterfactual simulation of where workers might have found more live-beating lift.",
        "simulations": simulations,
        "top_counterfactual": simulations[0] if simulations else {},
        "focus_routes": [row.get("route_key") for row in simulations[:8] if row.get("route_key")],
    }


def missed_winner_detector(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    rows = _search_candidate_rows(state)
    counterfactual = state.get("counterfactual_hunt_simulator") if isinstance(state.get("counterfactual_hunt_simulator"), dict) else {}
    frontier = state.get("exploration_frontier_map") if isinstance(state.get("exploration_frontier_map"), dict) else {}
    attempted = {_row_route(row) for row in rows if _row_route(row)}
    missed = []
    for sim in counterfactual.get("simulations") or []:
        route = str(sim.get("route_key") or "")
        if not route:
            continue
        expected = float(sim.get("expected_live_lift") or 0.0)
        if route not in attempted or expected >= 30.0:
            missed.append({
                "route_key": route,
                "expected_live_lift": round(expected, 4),
                "reason": "unsearched_frontier" if route not in attempted else "under_allocated_high_expected_lift",
                "rescue_action": "assign_frontier_scout",
            })
    for route in frontier.get("focus_routes") or []:
        if route and route not in {row.get("route_key") for row in missed}:
            missed.append({"route_key": route, "expected_live_lift": 20.0, "reason": "frontier_starved", "rescue_action": "small_rescue_batch"})
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Detects routes/genes that looked promising but were starved by the current allocator.",
        "missed": missed[:20],
        "missed_count": len(missed),
        "focus_routes": [row.get("route_key") for row in missed[:12] if row.get("route_key")],
    }


def causal_regret_ledger(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    adapter = state.get("runtime_command_adapter") if isinstance(state.get("runtime_command_adapter"), dict) else {}
    missed = state.get("missed_winner_detector") if isinstance(state.get("missed_winner_detector"), dict) else {}
    rejection = state.get("promotion_rejection_predictor_v2") if isinstance(state.get("promotion_rejection_predictor_v2"), dict) else {}
    current_focus = set(adapter.get("focus_routes") or [])
    rows = []
    for item in missed.get("missed") or []:
        route = str(item.get("route_key") or "")
        regret = float(item.get("expected_live_lift") or 0.0)
        if route and route not in current_focus:
            rows.append({"decision": "focus_route_omission", "route_key": route, "regret_score": round(regret, 4), "repair": "add_to_focus"})
    for item in rejection.get("high_risk_variants") or []:
        route = str(item.get("route_key") or "")
        if route and route in current_focus:
            rows.append({"decision": "fragile_route_overfocus", "route_key": route, "regret_score": float(item.get("risk_score") or 0.0), "repair": "shift_to_repair_or_avoid"})
    if adapter.get("mutation_width") == "tight" and missed.get("missed_count"):
        rows.append({"decision": "mutation_width_too_tight", "route_key": "", "regret_score": 18.0, "repair": "widen_frontier_routes"})
    rows = sorted(rows, key=lambda row: float(row.get("regret_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Regret ledger for opportunity-cost decisions: omitted focus, wrong width, fragile overfocus, and worker allocation.",
        "rows": rows[:30],
        "top_regret": rows[0] if rows else {},
        "focus_routes": [row.get("route_key") for row in rows if row.get("repair") in {"add_to_focus", "widen_frontier_routes"} and row.get("route_key")][:12],
        "avoid_routes": [row.get("route_key") for row in rows if row.get("repair") == "shift_to_repair_or_avoid" and row.get("route_key")][:12],
    }


def adaptive_search_grammar_generator(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    dna = state.get("variant_dna_attribution") if isinstance(state.get("variant_dna_attribution"), dict) else {}
    missed = state.get("missed_winner_detector") if isinstance(state.get("missed_winner_detector"), dict) else {}
    rejection = state.get("promotion_rejection_predictor_v2") if isinstance(state.get("promotion_rejection_predictor_v2"), dict) else {}
    templates = []
    for gene in dna.get("top_positive_genes") or []:
        token = str(gene.get("gene") or "")
        if not token:
            continue
        templates.append({
            "template_id": _stable_hash({"gene_template": token}, length=14),
            "template": f"amplify::{token}",
            "mutation_width": "medium",
            "source_gene": token,
        })
    for route in missed.get("focus_routes") or []:
        templates.append({
            "template_id": _stable_hash({"frontier_template": route}, length=14),
            "template": f"frontier_rescue::{route}",
            "route_key": route,
            "mutation_width": "wide",
        })
    for row in rejection.get("repair_variants") or []:
        route = row.get("route_key") or ""
        templates.append({
            "template_id": _stable_hash({"repair_template": row.get("variant"), "route": route}, length=14),
            "template": f"promotion_repair::{route or row.get('variant')}",
            "route_key": route,
            "mutation_width": "tight",
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Generates fresh mutation grammar templates from winners, near-misses, and promotion repair targets.",
        "templates": templates[:40],
        "top_templates": templates[:8],
        "focus_routes": _ordered_unique([row.get("route_key") for row in templates if row.get("route_key")])[:12],
    }


def live_hypothesis_kill_scale_court(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    bandit = state.get("online_causal_bandit") if isinstance(state.get("online_causal_bandit"), dict) else {}
    regret = state.get("causal_regret_ledger") if isinstance(state.get("causal_regret_ledger"), dict) else {}
    rejection = state.get("promotion_rejection_predictor_v2") if isinstance(state.get("promotion_rejection_predictor_v2"), dict) else {}
    cases = []
    for arm in bandit.get("arms") or []:
        lane = arm.get("lane")
        win_rate = float(arm.get("win_rate") or 0.0)
        decision = "scale" if win_rate >= 0.45 else "retest" if win_rate >= 0.15 else "kill"
        cases.append({"hypothesis": f"lane:{lane}", "decision": decision, "evidence": f"win_rate={win_rate}", "focus_routes": arm.get("routes") or []})
    for row in regret.get("rows") or []:
        if row.get("decision") == "focus_route_omission":
            cases.append({"hypothesis": f"missed:{row.get('route_key')}", "decision": "scale", "evidence": "counterfactual_regret", "focus_routes": [row.get("route_key")]})
    for row in rejection.get("high_risk_variants") or []:
        cases.append({"hypothesis": f"variant:{row.get('variant')}", "decision": "kill", "evidence": ",".join(row.get("reasons") or []), "avoid_routes": [row.get("route_key")]})
    focus = []
    avoid = []
    for case in cases:
        if case.get("decision") == "scale":
            focus.extend(case.get("focus_routes") or [])
        if case.get("decision") == "kill":
            avoid.extend(case.get("avoid_routes") or [])
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Cycle court that explicitly kills, scales, or retests active hunt hypotheses from live evidence.",
        "cases": cases[:40],
        "scale_count": sum(1 for row in cases if row.get("decision") == "scale"),
        "kill_count": sum(1 for row in cases if row.get("decision") == "kill"),
        "retest_count": sum(1 for row in cases if row.get("decision") == "retest"),
        "focus_routes": _ordered_unique(focus)[:12],
        "avoid_routes": _ordered_unique(avoid)[:12],
    }


def route_interaction_matrix_v2(state: dict[str, Any] | None = None) -> dict[str, Any]:
    rows = _search_candidate_rows(state)
    by_family: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        route = _row_route(row)
        if not route:
            continue
        family = route.split("|", 1)[0]
        by_family.setdefault(family, []).append(row)
    matrix = []
    families = sorted(by_family)
    for left in families:
        for right in families:
            if left >= right:
                continue
            left_best = max(float(row.get("step2_delta_vs_active") or 0.0) for row in by_family[left])
            right_best = max(float(row.get("step2_delta_vs_active") or 0.0) for row in by_family[right])
            interaction = round((left_best + right_best) / 2.0, 4)
            matrix.append({
                "left_family": left,
                "right_family": right,
                "interaction_score": interaction,
                "relationship": "complement" if interaction > 25.0 else "neutral" if interaction >= 0.0 else "poison",
            })
    matrix = sorted(matrix, key=lambda row: float(row.get("interaction_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Learns which route families help or poison each other when hunted in the same window.",
        "matrix": matrix[:40],
        "top_interaction": matrix[0] if matrix else {},
        "avoid_pairs": [row for row in matrix if row.get("relationship") == "poison"][:10],
    }


def promotion_survival_shadow_scoring(state: dict[str, Any] | None = None) -> dict[str, Any]:
    rows = _search_candidate_rows(state)
    rejection = state.get("promotion_rejection_predictor_v2") if isinstance(state.get("promotion_rejection_predictor_v2"), dict) else {}
    risk_by_variant = {str(row.get("variant")): float(row.get("risk_score") or 0.0) for row in rejection.get("predictions") or []}
    scored = []
    for row in rows:
        pnl = float(row.get("step2_pnl") or 0.0)
        delta = float(row.get("step2_delta_vs_active") or 0.0)
        readiness = float(row.get("promotion_readiness_score") or 65.0)
        risk = risk_by_variant.get(str(row.get("variant")), max(0.0, 70.0 - readiness))
        survival_score = round(max(0.0, delta) + pnl * 0.03 + readiness - risk * 0.8, 4)
        scored.append({
            "variant": row.get("variant"),
            "route_key": _row_route(row),
            "raw_pnl": pnl,
            "delta_vs_live": delta,
            "promotion_risk": round(risk, 4),
            "shadow_survival_score": survival_score,
            "disagreement": "raw_high_survival_low" if pnl > 0 and survival_score < 40.0 else "aligned",
        })
    scored = sorted(scored, key=lambda row: float(row.get("shadow_survival_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Scores live beaters by both raw P/L and likely promotion survival, then highlights disagreements.",
        "scores": scored[:50],
        "top_shadow": scored[0] if scored else {},
        "disagreements": [row for row in scored if row.get("disagreement") != "aligned"][:20],
        "focus_routes": [row.get("route_key") for row in scored[:12] if row.get("route_key")],
    }


def hunt_autopilot_policy_compiler(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    adapter = state.get("runtime_command_adapter") if isinstance(state.get("runtime_command_adapter"), dict) else {}
    court = state.get("live_hypothesis_kill_scale_court") if isinstance(state.get("live_hypothesis_kill_scale_court"), dict) else {}
    grammar = state.get("adaptive_search_grammar_generator") if isinstance(state.get("adaptive_search_grammar_generator"), dict) else {}
    regret = state.get("causal_regret_ledger") if isinstance(state.get("causal_regret_ledger"), dict) else {}
    shadow = state.get("promotion_survival_shadow_scoring") if isinstance(state.get("promotion_survival_shadow_scoring"), dict) else {}
    focus = _ordered_unique(
        list(court.get("focus_routes") or [])
        + list(grammar.get("focus_routes") or [])
        + list(regret.get("focus_routes") or [])
        + list(shadow.get("focus_routes") or [])
        + list(adapter.get("focus_routes") or [])
    )[:12]
    avoid = _ordered_unique(list(court.get("avoid_routes") or []) + list(regret.get("avoid_routes") or []) + list(adapter.get("avoid_routes") or []))[:24]
    worker_policy = []
    templates = grammar.get("top_templates") or []
    for idx in range(4):
        worker_policy.append({
            "worker": f"worker_{idx + 1}",
            "role": ["autopilot_scaler", "counterfactual_rescuer", "grammar_mutator", "promotion_shadow_reviewer"][idx],
            "route_key": (focus[idx % len(focus)] if focus else ""),
            "template": ((templates[idx % len(templates)] or {}).get("template") if templates else ""),
            "mutation_width": "wide" if idx in {1, 2} else "tight" if idx == 3 else adapter.get("mutation_width") or "medium",
        })
    packet = {
        "live_only": True,
        "focus_routes": focus,
        "avoid_routes": avoid,
        "mutation_width": "wide" if regret.get("top_regret") else adapter.get("mutation_width") or "medium",
        "batch_size_multiplier": min(1.35, max(0.65, float(adapter.get("batch_size_multiplier") or 1.0))),
        "worker_policy": worker_policy,
        "stopping_rules": ["stop keeping variants below live", "kill court-rejected hypotheses", "repair raw-high survival-low candidates"],
        "evidence_change_rules": ["scale if shadow survival improves", "rescue missed route if counterfactual lift stays high"],
    }
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Compiles counterfactual, regret, grammar, route interaction, and shadow survival learning into one policy packet.",
        "policy_packet": packet,
        "worker_policy": worker_policy,
        "focus_routes": focus,
        "avoid_routes": avoid,
        "mutation_width": packet["mutation_width"],
        "batch_size_multiplier": packet["batch_size_multiplier"],
    }


def counterfactual_opportunity_suite(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    simulator = counterfactual_hunt_simulator(state)
    missed = missed_winner_detector({**state, "counterfactual_hunt_simulator": simulator})
    regret = causal_regret_ledger({**state, "counterfactual_hunt_simulator": simulator, "missed_winner_detector": missed})
    grammar = adaptive_search_grammar_generator({**state, "missed_winner_detector": missed})
    court = live_hypothesis_kill_scale_court({**state, "causal_regret_ledger": regret})
    interactions = route_interaction_matrix_v2(state)
    shadow = promotion_survival_shadow_scoring(state)
    autopilot = hunt_autopilot_policy_compiler({
        **state,
        "causal_regret_ledger": regret,
        "adaptive_search_grammar_generator": grammar,
        "live_hypothesis_kill_scale_court": court,
        "promotion_survival_shadow_scoring": shadow,
    })
    return {
        "counterfactual_hunt_simulator": simulator,
        "missed_winner_detector": missed,
        "causal_regret_ledger": regret,
        "adaptive_search_grammar_generator": grammar,
        "live_hypothesis_kill_scale_court": court,
        "route_interaction_matrix_v2": interactions,
        "promotion_survival_shadow_scoring": shadow,
        "hunt_autopilot_policy_compiler": autopilot,
    }


def learning_claim_verifier(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    rows = _search_candidate_rows(state)
    cycle_delta = state.get("cycle_level_learning_delta") if isinstance(state.get("cycle_level_learning_delta"), dict) else {}
    bandit = state.get("online_causal_bandit") if isinstance(state.get("online_causal_bandit"), dict) else {}
    claims = []
    for item in cycle_delta.get("learned") or []:
        text = str(item or "")
        evidence = len(rows)
        if "top_lane=" in text:
            lane = text.split("top_lane=", 1)[1].split("|", 1)[0]
            evidence = sum(1 for arm in bandit.get("arms") or [] if str(arm.get("lane")) == lane and int(arm.get("attempts") or 0) > 0)
        confidence = min(95.0, 35.0 + evidence * 12.0)
        claims.append({
            "claim": text,
            "evidence_count": evidence,
            "confidence": round(confidence, 2),
            "verdict": "verified" if confidence >= 65.0 else "weak_evidence",
        })
    if not claims:
        top_lane = bandit.get("top_lane") or "standard_mutation"
        claims.append({"claim": f"top_lane={top_lane}", "evidence_count": len(rows), "confidence": 50.0, "verdict": "weak_evidence"})
    weak = [row for row in claims if row.get("verdict") != "verified"]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Evidence-checks live learning claims before they are allowed to steer scaling decisions.",
        "claims": claims,
        "verified_count": len(claims) - len(weak),
        "weak_claims": weak,
        "weak_claim_count": len(weak),
    }


def causal_confidence_calibration(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    bandit = state.get("online_causal_bandit") if isinstance(state.get("online_causal_bandit"), dict) else {}
    verifier = state.get("learning_claim_verifier") if isinstance(state.get("learning_claim_verifier"), dict) else {}
    rejection = state.get("promotion_rejection_predictor_v2") if isinstance(state.get("promotion_rejection_predictor_v2"), dict) else {}
    high_risk_routes = {str(row.get("route_key")) for row in rejection.get("high_risk_variants") or [] if row.get("route_key")}
    calibrations = []
    for arm in bandit.get("arms") or []:
        attempts = int(arm.get("attempts") or 0)
        win_rate = float(arm.get("win_rate") or 0.0)
        routes = [str(route) for route in arm.get("routes") or []]
        contradiction = bool(set(routes).intersection(high_risk_routes))
        confidence = min(98.0, 30.0 + attempts * 10.0 + win_rate * 45.0 - (25.0 if contradiction else 0.0))
        calibrations.append({
            "lane": arm.get("lane"),
            "confidence": round(max(0.0, confidence), 2),
            "state": "promotion_fragile" if contradiction else "strong" if confidence >= 70.0 else "noisy" if attempts else "underpowered",
            "routes": routes,
        })
    calibrations = sorted(calibrations, key=lambda row: float(row.get("confidence") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Calibrates whether causal lessons are strong, noisy, underpowered, contradicted, or promotion-fragile.",
        "calibrations": calibrations,
        "top_confidence": calibrations[0] if calibrations else {},
        "weak_claim_count": verifier.get("weak_claim_count") or 0,
        "focus_routes": _ordered_unique([route for row in calibrations if row.get("state") == "strong" for route in row.get("routes") or []])[:12],
        "pause_routes": _ordered_unique([route for row in calibrations if row.get("state") in {"promotion_fragile", "underpowered"} for route in row.get("routes") or []])[:12],
    }


def false_discovery_early_warning(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    rows = _search_candidate_rows(state)
    verifier = state.get("learning_claim_verifier") if isinstance(state.get("learning_claim_verifier"), dict) else {}
    rejection = state.get("promotion_rejection_predictor_v2") if isinstance(state.get("promotion_rejection_predictor_v2"), dict) else {}
    firewall = state.get("false_discovery_firewall") if isinstance(state.get("false_discovery_firewall"), dict) else {}
    warnings = []
    if verifier.get("weak_claim_count"):
        warnings.append({"kind": "weak_learning_claims", "severity": "medium", "detail": f"{verifier.get('weak_claim_count')} claims need more evidence"})
    if int(firewall.get("flag_count") or 0) > 0:
        warnings.append({"kind": "existing_false_discovery_flags", "severity": "high", "detail": f"{firewall.get('flag_count')} firewall flags"})
    for row in rows:
        tags = {str(tag) for tag in row.get("learning_tags") or []}
        if tags.intersection({"thin_sample", "high_overfit_risk", "thin_holdout_edge", "ticker_concentrated"}):
            warnings.append({"kind": "fragile_live_beater", "severity": "high", "route_key": _row_route(row), "variant": row.get("variant")})
    for row in rejection.get("high_risk_variants") or []:
        warnings.append({"kind": "promotion_reject_likely", "severity": "high", "route_key": row.get("route_key"), "variant": row.get("variant")})
    avoid_routes = _ordered_unique([row.get("route_key") for row in warnings if row.get("severity") == "high" and row.get("route_key")])[:12]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Early warning system for lucky P/L, aliases, small samples, regime flukes, and promotion-fragile discoveries.",
        "warnings": warnings[:40],
        "warning_count": len(warnings),
        "avoid_routes": avoid_routes,
        "batch_size_multiplier": 0.72 if warnings else None,
    }


def adaptive_evidence_thresholds(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    warnings = state.get("false_discovery_early_warning") if isinstance(state.get("false_discovery_early_warning"), dict) else {}
    calibration = state.get("causal_confidence_calibration") if isinstance(state.get("causal_confidence_calibration"), dict) else {}
    warning_count = int(warnings.get("warning_count") or 0)
    weak_count = int(calibration.get("weak_claim_count") or 0)
    thresholds = {
        "explore": 35 + min(15, weak_count * 3),
        "scale": 62 + min(20, warning_count * 4),
        "promotion_track": 82 + min(12, warning_count * 2),
    }
    mode = "strict" if warning_count >= 3 else "cautious" if warning_count else "normal"
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Live evidence thresholds: low for exploration, higher for scale, highest for promotion-track decisions.",
        "thresholds": thresholds,
        "mode": mode,
        "min_scale_confidence": thresholds["scale"],
        "min_promotion_confidence": thresholds["promotion_track"],
    }


def self_debate_search_council(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    bandit = state.get("online_causal_bandit") if isinstance(state.get("online_causal_bandit"), dict) else {}
    warnings = state.get("false_discovery_early_warning") if isinstance(state.get("false_discovery_early_warning"), dict) else {}
    frontier = state.get("exploration_frontier_map") if isinstance(state.get("exploration_frontier_map"), dict) else {}
    shadow = state.get("promotion_survival_shadow_scoring") if isinstance(state.get("promotion_survival_shadow_scoring"), dict) else {}
    recommendations = [
        {"voice": "exploiter", "action": "scale_top_lane", "focus_routes": bandit.get("focus_routes") or [], "confidence": 70},
        {"voice": "skeptic", "action": "pause_fragile_routes", "avoid_routes": warnings.get("avoid_routes") or [], "confidence": 75 if warnings.get("warning_count") else 40},
        {"voice": "frontier_scout", "action": "probe_frontier", "focus_routes": frontier.get("focus_routes") or [], "confidence": 60},
        {"voice": "promotion_reviewer", "action": "promote_survivable", "focus_routes": shadow.get("focus_routes") or [], "confidence": 82},
    ]
    focus = []
    avoid = []
    for rec in recommendations:
        if float(rec.get("confidence") or 0.0) >= 55:
            focus.extend(rec.get("focus_routes") or [])
            avoid.extend(rec.get("avoid_routes") or [])
    avoid_set = set(_ordered_unique(avoid))
    focus = [route for route in _ordered_unique(focus) if route not in avoid_set]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Internal search council: exploiter, skeptic, frontier scout, and promotion reviewer debate the next policy.",
        "recommendations": recommendations,
        "resolution": {"focus_routes": focus[:12], "avoid_routes": list(avoid_set)[:12]},
        "focus_routes": focus[:12],
        "avoid_routes": list(avoid_set)[:12],
    }


def experiment_memory_compression(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    verifier = state.get("learning_claim_verifier") if isinstance(state.get("learning_claim_verifier"), dict) else {}
    grammar = state.get("adaptive_search_grammar_generator") if isinstance(state.get("adaptive_search_grammar_generator"), dict) else {}
    shadow = state.get("promotion_survival_shadow_scoring") if isinstance(state.get("promotion_survival_shadow_scoring"), dict) else {}
    rules = []
    for claim in verifier.get("claims") or []:
        if claim.get("verdict") == "verified":
            rules.append({"rule": f"Trust only with evidence: {claim.get('claim')}", "source": "learning_claim_verifier"})
    for template in grammar.get("top_templates") or []:
        rules.append({"rule": f"Reusable grammar: {template.get('template')}", "source": "adaptive_search_grammar_generator"})
    top_shadow = shadow.get("top_shadow") or {}
    if top_shadow.get("route_key"):
        rules.append({"rule": f"Promotion-survivable route to revisit: {top_shadow.get('route_key')}", "source": "promotion_survival_shadow_scoring"})
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Compresses repeated experiment learning into durable rules for cleaner future runs.",
        "rules": rules[:30],
        "rule_count": len(rules),
    }


def learning_drift_monitor(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    rows = _search_candidate_rows(state)
    memory = state.get("experiment_memory_compression") if isinstance(state.get("experiment_memory_compression"), dict) else {}
    regret = state.get("causal_regret_ledger") if isinstance(state.get("causal_regret_ledger"), dict) else {}
    current_routes = {_row_route(row) for row in rows if _row_route(row)}
    drifted = []
    for rule in memory.get("rules") or []:
        text = str(rule.get("rule") or "")
        route = text.rsplit(":", 1)[-1].strip() if ":" in text else ""
        if route and "|" in route and route not in current_routes:
            drifted.append({"rule": text, "reason": "rule_route_absent_from_current_winners", "route_key": route})
    for row in regret.get("rows") or []:
        if row.get("decision") == "fragile_route_overfocus":
            drifted.append({"rule": f"Overfocused fragile route {row.get('route_key')}", "reason": "current_regret", "route_key": row.get("route_key")})
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Detects stale lessons whose routes or effects no longer hold in the current hunt.",
        "drifted_rules": drifted[:30],
        "drift_count": len(drifted),
        "retire_routes": _ordered_unique([row.get("route_key") for row in drifted if row.get("route_key")])[:12],
    }


def promotion_first_autopilot_v2(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    autopilot = state.get("hunt_autopilot_policy_compiler") if isinstance(state.get("hunt_autopilot_policy_compiler"), dict) else {}
    council = state.get("self_debate_search_council") if isinstance(state.get("self_debate_search_council"), dict) else {}
    thresholds = state.get("adaptive_evidence_thresholds") if isinstance(state.get("adaptive_evidence_thresholds"), dict) else {}
    drift = state.get("learning_drift_monitor") if isinstance(state.get("learning_drift_monitor"), dict) else {}
    shadow = state.get("promotion_survival_shadow_scoring") if isinstance(state.get("promotion_survival_shadow_scoring"), dict) else {}
    base = autopilot.get("policy_packet") if isinstance(autopilot.get("policy_packet"), dict) else {}
    focus = _ordered_unique(list(shadow.get("focus_routes") or []) + list(council.get("focus_routes") or []) + list(base.get("focus_routes") or []))[:12]
    avoid = _ordered_unique(list(drift.get("retire_routes") or []) + list(council.get("avoid_routes") or []) + list(base.get("avoid_routes") or []))[:24]
    worker_policy = []
    for idx in range(4):
        worker_policy.append({
            "worker": f"worker_{idx + 1}",
            "role": ["promotion_survival_scaler", "evidence_verifier", "frontier_rescuer", "fragility_repairer"][idx],
            "route_key": (focus[idx % len(focus)] if focus else ""),
            "min_confidence": (thresholds.get("thresholds") or {}).get("promotion_track", 82),
        })
    packet = {
        **base,
        "live_only": True,
        "objective": "maximize_promotion_survivable_live_pnl",
        "focus_routes": [route for route in focus if route not in set(avoid)],
        "avoid_routes": avoid,
        "worker_policy": worker_policy,
        "evidence_thresholds": thresholds.get("thresholds") or {},
        "mutation_width": "tight" if thresholds.get("mode") == "strict" else base.get("mutation_width") or "medium",
        "batch_size_multiplier": min(float(base.get("batch_size_multiplier") or 1.0), 0.85) if thresholds.get("mode") == "strict" else float(base.get("batch_size_multiplier") or 1.0),
    }
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Promotion-first autopilot that optimizes for live beaters likely to survive review, not raw P/L alone.",
        "policy_packet": packet,
        "worker_policy": worker_policy,
        "focus_routes": packet["focus_routes"],
        "avoid_routes": avoid,
        "mutation_width": packet["mutation_width"],
        "batch_size_multiplier": packet["batch_size_multiplier"],
    }


def truth_maintenance_suite(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    verifier = learning_claim_verifier(state)
    calibration = causal_confidence_calibration({**state, "learning_claim_verifier": verifier})
    warning = false_discovery_early_warning({**state, "learning_claim_verifier": verifier})
    thresholds = adaptive_evidence_thresholds({**state, "false_discovery_early_warning": warning, "causal_confidence_calibration": calibration})
    council = self_debate_search_council({**state, "false_discovery_early_warning": warning})
    compression = experiment_memory_compression({**state, "learning_claim_verifier": verifier})
    drift = learning_drift_monitor({**state, "experiment_memory_compression": compression})
    autopilot_v2 = promotion_first_autopilot_v2({
        **state,
        "adaptive_evidence_thresholds": thresholds,
        "self_debate_search_council": council,
        "experiment_memory_compression": compression,
        "learning_drift_monitor": drift,
    })
    return {
        "learning_claim_verifier": verifier,
        "causal_confidence_calibration": calibration,
        "false_discovery_early_warning": warning,
        "adaptive_evidence_thresholds": thresholds,
        "self_debate_search_council": council,
        "experiment_memory_compression": compression,
        "learning_drift_monitor": drift,
        "promotion_first_autopilot_v2": autopilot_v2,
    }


def _temporal_candidate_rows(state: dict[str, Any] | None = None, *, limit: int = 80) -> list[dict[str, Any]]:
    state = state or {}
    rows = list(_search_candidate_rows(state))
    if not rows:
        for arm in state.get("route_arms") or []:
            if not isinstance(arm, dict):
                continue
            rows.append({
                "variant": arm.get("best_variant") or arm.get("route_key"),
                "route_key": arm.get("route_key"),
                "mutation_lane": arm.get("lane") or arm.get("treatment") or "route_arm",
                "step2_pnl": arm.get("best_step2_pnl") or 0.0,
                "step2_delta_vs_active": arm.get("best_delta_vs_active") or arm.get("online_score") or 0.0,
                "promotion_readiness_score": arm.get("promotion_readiness_score") or 50.0,
            })
    if not rows:
        packet = active_runtime_command_packet(state)
        for route in packet.get("focus_routes") or []:
            rows.append({
                "variant": route,
                "route_key": route,
                "mutation_lane": "runtime_focus",
                "step2_pnl": 0.0,
                "step2_delta_vs_active": 0.0,
                "promotion_readiness_score": 50.0,
            })
    scored = []
    for row in rows:
        route = _row_route(row)
        if not route:
            continue
        scored.append(dict(row, route_key=route))
    scored.sort(
        key=lambda row: (
            float(row.get("step2_delta_vs_active") or 0.0),
            float(row.get("promotion_readiness_score") or 0.0),
            float(row.get("step2_pnl") or 0.0),
        ),
        reverse=True,
    )
    return scored[:limit]


def multi_horizon_memory_stack(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    adapter = state.get("runtime_command_adapter") if isinstance(state.get("runtime_command_adapter"), dict) else runtime_command_adapter(dict(state))
    promotion = state.get("promotion_first_autopilot_v2") if isinstance(state.get("promotion_first_autopilot_v2"), dict) else {}
    bandit = state.get("online_causal_bandit") if isinstance(state.get("online_causal_bandit"), dict) else {}
    compression = state.get("experiment_memory_compression") if isinstance(state.get("experiment_memory_compression"), dict) else {}
    shadow = state.get("promotion_survival_shadow_scoring") if isinstance(state.get("promotion_survival_shadow_scoring"), dict) else {}
    verifier = state.get("learning_claim_verifier") if isinstance(state.get("learning_claim_verifier"), dict) else {}
    rows = _temporal_candidate_rows(state, limit=50)
    durable_rules = list(compression.get("rules") or [])[:8]
    next_cycle = _ordered_unique(list(adapter.get("focus_routes") or []) + list(promotion.get("focus_routes") or []))[:8]
    next_hour = _ordered_unique(
        next_cycle
        + list(bandit.get("focus_routes") or [])
        + [row.get("route_key") for row in rows[:8]]
    )[:12]
    next_hunt = _ordered_unique(
        next_hour
        + [row.get("route_key") for row in (shadow.get("scores") or [])[:8] if isinstance(row, dict)]
        + list((state.get("hunt_opening_playbook") or {}).get("focus_routes") or [])
    )[:16]
    avoid = _ordered_unique(
        list(adapter.get("avoid_routes") or [])
        + list(promotion.get("avoid_routes") or [])
        + list((state.get("learning_drift_monitor") or {}).get("retire_routes") or [])
    )[:20]
    horizons = {
        "next_cycle": {
            "intent": "exploit the freshest verified runtime signal",
            "focus_routes": [route for route in next_cycle if route not in set(avoid)],
            "memory_weight": 0.34,
        },
        "next_hour": {
            "intent": "blend hot routes with causal lane learning",
            "focus_routes": [route for route in next_hour if route not in set(avoid)],
            "memory_weight": 0.28,
        },
        "next_hunt": {
            "intent": "carry forward promotion-survivable openings",
            "focus_routes": [route for route in next_hunt if route not in set(avoid)],
            "memory_weight": 0.23,
        },
        "durable": {
            "intent": "rules that survived claim verification",
            "rules": durable_rules,
            "verified_claim_count": verifier.get("verified_count") or 0,
            "memory_weight": 0.15,
        },
    }
    summary = {
        "next_cycle_routes": len(horizons["next_cycle"]["focus_routes"]),
        "next_hour_routes": len(horizons["next_hour"]["focus_routes"]),
        "next_hunt_routes": len(horizons["next_hunt"]["focus_routes"]),
        "durable_rules": len(durable_rules),
    }
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Separates fast, medium, next-hunt, and durable memory so fresh signal does not overwrite institutional memory.",
        "horizons": horizons,
        "summary": summary,
        "focus_routes": horizons["next_cycle"]["focus_routes"] + horizons["next_hour"]["focus_routes"][:4],
        "avoid_routes": avoid,
    }


def lesson_half_life_engine_v2(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    verifier = state.get("learning_claim_verifier") if isinstance(state.get("learning_claim_verifier"), dict) else {}
    compression = state.get("experiment_memory_compression") if isinstance(state.get("experiment_memory_compression"), dict) else {}
    false_warning = state.get("false_discovery_early_warning") if isinstance(state.get("false_discovery_early_warning"), dict) else {}
    drift = state.get("learning_drift_monitor") if isinstance(state.get("learning_drift_monitor"), dict) else {}
    warning_routes = set(str(route) for route in false_warning.get("avoid_routes") or [])
    drift_routes = set(str(route) for route in drift.get("retire_routes") or [])
    lessons: list[dict[str, Any]] = []
    for claim in verifier.get("claims") or []:
        if not isinstance(claim, dict):
            continue
        route = str(claim.get("route_key") or "")
        confidence = float(claim.get("confidence") or claim.get("claim_confidence") or 0.5)
        half_life = 3.0 + confidence * 8.0 - (3.0 if route in warning_routes else 0.0) - (4.0 if route in drift_routes else 0.0)
        lessons.append({
            "lesson_id": claim.get("claim_id") or _stable_hash({"claim": claim}),
            "route_key": route,
            "lesson": claim.get("claim") or claim.get("summary") or "verified claim",
            "confidence": round(confidence, 4),
            "half_life_cycles": round(max(1.0, half_life), 2),
            "recommended_action": "refresh" if half_life < 4.5 else "trust_but_monitor",
        })
    for rule in compression.get("rules") or []:
        if not isinstance(rule, dict):
            rule = {"rule": str(rule)}
        route = str(rule.get("route_key") or "")
        confidence = float(rule.get("confidence") or 0.66)
        lessons.append({
            "lesson_id": rule.get("rule_id") or _stable_hash({"rule": rule}),
            "route_key": route,
            "lesson": rule.get("rule") or rule.get("summary") or "compressed rule",
            "confidence": round(confidence, 4),
            "half_life_cycles": round(max(2.0, 6.0 + confidence * 6.0), 2),
            "recommended_action": "durable",
        })
    if not lessons:
        for row in _temporal_candidate_rows(state, limit=8):
            lessons.append({
                "lesson_id": _stable_hash({"candidate_lesson": row.get("variant"), "route": row.get("route_key")}),
                "route_key": row.get("route_key"),
                "lesson": "candidate edge needs temporal refresh",
                "confidence": round(float(row.get("promotion_readiness_score") or 50.0) / 100.0, 4),
                "half_life_cycles": 4.0,
                "recommended_action": "refresh",
            })
    lessons.sort(key=lambda row: (float(row.get("half_life_cycles") or 0.0), float(row.get("confidence") or 0.0)), reverse=True)
    refresh = _ordered_unique([row.get("route_key") for row in lessons if row.get("recommended_action") == "refresh"])[:10]
    decay = _ordered_unique([row.get("route_key") for row in lessons if float(row.get("half_life_cycles") or 0.0) <= 3.0] + list(drift_routes))[:12]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Assigns decay rates to lessons so stale rules are refreshed or retired during the hunt.",
        "lessons": lessons[:30],
        "top_lesson": lessons[0] if lessons else {},
        "refresh_routes": refresh,
        "decay_routes": decay,
        "median_half_life_cycles": round(sorted([float(row.get("half_life_cycles") or 0.0) for row in lessons])[len(lessons) // 2], 2) if lessons else 0.0,
    }


def cross_hunt_strategy_replay(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    current_policy = (state.get("promotion_first_autopilot_v2") or {}).get("policy_packet") if isinstance(state.get("promotion_first_autopilot_v2"), dict) else {}
    if not isinstance(current_policy, dict):
        current_policy = {}
    replay = state.get("hunt_replay_simulator") if isinstance(state.get("hunt_replay_simulator"), dict) else {}
    postmortem = state.get("run_to_run_postmortem") if isinstance(state.get("run_to_run_postmortem"), dict) else {}
    rows = _temporal_candidate_rows(state, limit=20)
    focus = _ordered_unique(list(current_policy.get("focus_routes") or []) + [row.get("route_key") for row in rows[:6]])[:10]
    replays = []
    for idx, route in enumerate(focus):
        recent_score = max(0.0, float((rows[idx] if idx < len(rows) else {}).get("step2_delta_vs_active") or 0.0))
        replay_score = float(((replay.get("policy_scores") or {}).get(route) if isinstance(replay.get("policy_scores"), dict) else 0.0) or 0.0)
        postmortem_penalty = 12.0 if route in set(postmortem.get("failed_routes") or []) else 0.0
        delta = recent_score + replay_score - postmortem_penalty
        replays.append({
            "route_key": route,
            "current_policy": current_policy.get("objective") or "promotion_first",
            "replay_score": round(delta, 4),
            "decision": "open_with_route" if delta >= 0.0 else "shadow_only",
            "reason": "recent live edge plus replay memory" if delta >= 0.0 else "postmortem or replay penalty",
        })
    replays.sort(key=lambda row: float(row.get("replay_score") or 0.0), reverse=True)
    adjustments = [
        {"route_key": row.get("route_key"), "action": row.get("decision"), "reason": row.get("reason")}
        for row in replays[:8]
    ]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Replays the current policy against cross-hunt memory and asks what would have changed the opening.",
        "replays": replays,
        "top_replay": replays[0] if replays else {},
        "would_have_changed": any(row.get("decision") == "shadow_only" for row in replays),
        "recommended_adjustments": adjustments,
        "focus_routes": _ordered_unique([row.get("route_key") for row in replays if row.get("decision") == "open_with_route"])[:8],
        "avoid_routes": _ordered_unique([row.get("route_key") for row in replays if row.get("decision") == "shadow_only"])[:8],
    }


def temporal_regime_fingerprinting(state: dict[str, Any] | None = None) -> dict[str, Any]:
    rows = _temporal_candidate_rows(state, limit=80)
    cells: dict[str, dict[str, Any]] = {}
    for row in rows:
        route = _row_route(row)
        parts = route.split("|")
        ticker = str(row.get("ticker") or (parts[0] if parts else "") or "unknown")
        setup = str(row.get("setup") or row.get("regime") or (parts[1] if len(parts) > 1 else "") or _row_lane(row))
        phase = str(row.get("session_phase") or row.get("phase") or (parts[2] if len(parts) > 2 else "") or "all")
        key = f"{ticker}|{setup}|{phase}"
        cell = cells.setdefault(key, {
            "fingerprint": key,
            "ticker": ticker,
            "setup": setup,
            "phase": phase,
            "routes": [],
            "count": 0,
            "best_delta": 0.0,
            "readiness_total": 0.0,
        })
        cell["routes"].append(route)
        cell["count"] += 1
        cell["best_delta"] = max(float(cell.get("best_delta") or 0.0), float(row.get("step2_delta_vs_active") or 0.0))
        cell["readiness_total"] += float(row.get("promotion_readiness_score") or 0.0)
    fingerprints = []
    for cell in cells.values():
        avg_readiness = float(cell.get("readiness_total") or 0.0) / max(1, int(cell.get("count") or 0))
        score = float(cell.get("best_delta") or 0.0) + avg_readiness * 0.35 + min(8, int(cell.get("count") or 0)) * 2.5
        routes = _ordered_unique(cell.get("routes") or [])
        fingerprints.append({
            "fingerprint": cell["fingerprint"],
            "ticker": cell["ticker"],
            "setup": cell["setup"],
            "phase": cell["phase"],
            "routes": routes[:8],
            "count": cell["count"],
            "best_delta": round(float(cell.get("best_delta") or 0.0), 4),
            "avg_readiness": round(avg_readiness, 2),
            "score": round(score, 4),
        })
    fingerprints.sort(key=lambda row: float(row.get("score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Groups live-beating variants by ticker/setup/phase so the hunt learns when a route works.",
        "fingerprints": fingerprints[:30],
        "top_fingerprint": fingerprints[0] if fingerprints else {},
        "focus_routes": _ordered_unique([route for fp in fingerprints[:4] for route in fp.get("routes") or []])[:10],
    }


def longitudinal_promotion_survival_model(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    rows = _temporal_candidate_rows(state, limit=80)
    feedback = [row for row in (state.get("promotion_feedback") or []) if isinstance(row, dict)]
    rejected = {str(row.get("variant") or row.get("candidate") or "") for row in feedback if str(row.get("decision") or "").lower() == "reject"}
    shadow = state.get("promotion_survival_shadow_scoring") if isinstance(state.get("promotion_survival_shadow_scoring"), dict) else {}
    rejection = state.get("promotion_rejection_predictor_v2") if isinstance(state.get("promotion_rejection_predictor_v2"), dict) else {}
    risk_routes = set(str(route) for route in rejection.get("avoid_routes") or [])
    features: dict[str, dict[str, Any]] = {}
    for row in rows:
        route = _row_route(row)
        tokens = [f"route:{route}", f"lane:{_row_lane(row)}"] + _row_gene_tokens(row)
        for token in _ordered_unique(tokens):
            feat = features.setdefault(token, {"feature": token, "count": 0, "survival_score": 0.0, "routes": []})
            readiness = float(row.get("promotion_readiness_score") or 50.0)
            delta = max(0.0, float(row.get("step2_delta_vs_active") or 0.0))
            penalty = 35.0 if str(row.get("variant") or "") in rejected else 0.0
            penalty += 20.0 if route in risk_routes else 0.0
            feat["count"] += 1
            feat["survival_score"] += readiness * 0.55 + min(80.0, delta) * 0.45 - penalty
            feat["routes"].append(route)
    for row in shadow.get("scores") or []:
        if not isinstance(row, dict):
            continue
        route = str(row.get("route_key") or "")
        token = f"route:{route}"
        feat = features.setdefault(token, {"feature": token, "count": 0, "survival_score": 0.0, "routes": []})
        feat["count"] += 1
        feat["survival_score"] += float(row.get("survival_score") or row.get("score") or 0.0)
        feat["routes"].append(route)
    modeled = []
    for feat in features.values():
        count = max(1, int(feat.get("count") or 0))
        score = float(feat.get("survival_score") or 0.0) / count
        routes = _ordered_unique(feat.get("routes") or [])
        modeled.append({
            "feature": feat["feature"],
            "count": count,
            "survival_score": round(score, 4),
            "routes": routes[:8],
            "recommendation": "scale" if score >= 55.0 else "repair" if score >= 35.0 else "avoid",
        })
    modeled.sort(key=lambda row: float(row.get("survival_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Learns which routes, lanes, and genes keep surviving promotion review over time.",
        "features": modeled[:40],
        "top_survival_feature": modeled[0] if modeled else {},
        "survival_focus_routes": _ordered_unique([route for row in modeled if row.get("recommendation") == "scale" for route in row.get("routes") or []])[:10],
        "risk_avoid_routes": _ordered_unique(list(risk_routes) + [route for row in modeled if row.get("recommendation") == "avoid" for route in row.get("routes") or []])[:12],
    }


def memory_conflict_court_v2(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    memory_stack = state.get("multi_horizon_memory_stack") if isinstance(state.get("multi_horizon_memory_stack"), dict) else {}
    half_life = state.get("lesson_half_life_engine_v2") if isinstance(state.get("lesson_half_life_engine_v2"), dict) else {}
    survival = state.get("longitudinal_promotion_survival_model") if isinstance(state.get("longitudinal_promotion_survival_model"), dict) else {}
    current_focus = set(str(route) for route in memory_stack.get("focus_routes") or [])
    durable_focus = set(str(route) for route in survival.get("survival_focus_routes") or [])
    stale = set(str(route) for route in half_life.get("decay_routes") or [])
    risk = set(str(route) for route in survival.get("risk_avoid_routes") or [])
    cases = []
    for route in sorted(current_focus | durable_focus | stale | risk):
        if not route:
            continue
        signals = {
            "current_focus": route in current_focus,
            "durable_survival": route in durable_focus,
            "stale_or_decaying": route in stale,
            "promotion_risk": route in risk,
        }
        if route in risk and route in current_focus:
            ruling = "avoid_until_repaired"
            action = "avoid"
        elif route in stale and route in durable_focus:
            ruling = "falsify_before_scaling"
            action = "falsify"
        elif route in durable_focus or route in current_focus:
            ruling = "allow"
            action = "focus"
        else:
            ruling = "monitor"
            action = "monitor"
        cases.append({
            "case_id": _stable_hash({"memory_conflict_v2": route, "signals": signals}),
            "route_key": route,
            "signals": signals,
            "ruling": ruling,
            "action": action,
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Arbitrates short-term memory, durable survival memory, stale lessons, and promotion risk.",
        "cases": cases[:40],
        "case_count": len(cases),
        "focus_routes": _ordered_unique([row.get("route_key") for row in cases if row.get("action") == "focus"])[:10],
        "avoid_routes": _ordered_unique([row.get("route_key") for row in cases if row.get("action") == "avoid"])[:10],
        "falsify_routes": _ordered_unique([row.get("route_key") for row in cases if row.get("action") == "falsify"])[:10],
    }


def strategy_aging_dashboard(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    genome = state.get("strategy_genome_registry") if isinstance(state.get("strategy_genome_registry"), dict) else {}
    selector = state.get("regime_conditioned_strategy_selector") if isinstance(state.get("regime_conditioned_strategy_selector"), dict) else {}
    strategy_v2 = state.get("hunt_strategy_compiler_v2") if isinstance(state.get("hunt_strategy_compiler_v2"), dict) else {}
    half_life = state.get("lesson_half_life_engine_v2") if isinstance(state.get("lesson_half_life_engine_v2"), dict) else {}
    strategies = []
    candidates = []
    if genome.get("champion"):
        candidates.append(("genome_champion", genome.get("champion")))
    if selector.get("selected_strategy"):
        candidates.append(("regime_selected", selector.get("selected_strategy")))
    if strategy_v2.get("strategy"):
        candidates.append(("strategy_v2", {"name": strategy_v2.get("strategy"), "focus_routes": strategy_v2.get("focus_routes") or []}))
    if not candidates:
        candidates.append(("temporal_default", {"name": "promotion_survivable_temporal_opening", "focus_routes": (state.get("runtime_command_adapter") or {}).get("focus_routes") or []}))
    median_half_life = float(half_life.get("median_half_life_cycles") or 4.0)
    for source, strategy in candidates:
        if not isinstance(strategy, dict):
            strategy = {"name": str(strategy)}
        name = str(strategy.get("strategy_name") or strategy.get("name") or source)
        routes = _ordered_unique(strategy.get("focus_routes") or strategy.get("routes") or [])
        freshness = min(100.0, median_half_life * 12.0 + len(routes) * 3.0)
        status = "fresh" if freshness >= 70 else "maturing" if freshness >= 45 else "stale" if freshness >= 25 else "decaying"
        strategies.append({
            "strategy_id": _stable_hash({"strategy_aging": source, "name": name}),
            "name": name,
            "source": source,
            "freshness_score": round(freshness, 2),
            "status": status,
            "routes": routes[:8],
        })
    counts: dict[str, int] = {}
    for row in strategies:
        counts[row["status"]] = counts.get(row["status"], 0) + 1
    strategies.sort(key=lambda row: float(row.get("freshness_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Tracks whether strategies are fresh, maturing, stale, decaying, or worth resurrection.",
        "strategies": strategies,
        "aging_counts": counts,
        "top_strategy": strategies[0] if strategies else {},
        "resurrect_routes": _ordered_unique([route for row in strategies if row.get("status") in {"maturing", "stale"} for route in row.get("routes") or []])[:8],
        "retire_routes": _ordered_unique([route for row in strategies if row.get("status") == "decaying" for route in row.get("routes") or []])[:8],
    }


def next_hunt_opening_policy_compiler(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    memory_stack = state.get("multi_horizon_memory_stack") if isinstance(state.get("multi_horizon_memory_stack"), dict) else {}
    replay = state.get("cross_hunt_strategy_replay") if isinstance(state.get("cross_hunt_strategy_replay"), dict) else {}
    regime = state.get("temporal_regime_fingerprinting") if isinstance(state.get("temporal_regime_fingerprinting"), dict) else {}
    survival = state.get("longitudinal_promotion_survival_model") if isinstance(state.get("longitudinal_promotion_survival_model"), dict) else {}
    court = state.get("memory_conflict_court_v2") if isinstance(state.get("memory_conflict_court_v2"), dict) else {}
    aging = state.get("strategy_aging_dashboard") if isinstance(state.get("strategy_aging_dashboard"), dict) else {}
    avoid = _ordered_unique(
        list(memory_stack.get("avoid_routes") or [])
        + list(replay.get("avoid_routes") or [])
        + list(survival.get("risk_avoid_routes") or [])
        + list(court.get("avoid_routes") or [])
        + list(aging.get("retire_routes") or [])
    )[:20]
    focus = _ordered_unique(
        list(court.get("focus_routes") or [])
        + list(survival.get("survival_focus_routes") or [])
        + list(replay.get("focus_routes") or [])
        + list(regime.get("focus_routes") or [])
        + list(memory_stack.get("focus_routes") or [])
        + list(aging.get("resurrect_routes") or [])
    )
    focus = [route for route in focus if route not in set(avoid)][:12]
    if not focus:
        focus = _ordered_unique([row.get("route_key") for row in _temporal_candidate_rows(state, limit=6)])[:6]
    mode = "promotion_survival_opening"
    if court.get("falsify_routes"):
        mode = "falsify_then_scale_opening"
    elif not focus:
        mode = "broad_discovery_opening"
    width = "tight" if mode.startswith("falsify") else "medium" if focus else "wide"
    batch = 0.82 if mode.startswith("falsify") else 1.0 if focus else 1.18
    worker_roles = []
    role_names = ["survival_exploit", "regime_probe", "replay_challenger", "falsification_shadow"]
    route_pool = focus + list(court.get("falsify_routes") or [])
    for idx, role in enumerate(role_names):
        route = (route_pool[idx:] or route_pool or [""])[0]
        worker_roles.append({
            "worker": f"worker_{idx + 1}",
            "role": role,
            "route_key": route,
            "mutation_width": "tight" if role == "falsification_shadow" else width,
            "batch_size_multiplier": min(batch, 0.82) if role == "falsification_shadow" else batch,
        })
    packet = {
        "live_only": True,
        "objective": "maximize_temporally_survivable_live_pnl",
        "opening_mode": mode,
        "focus_routes": focus,
        "avoid_routes": avoid,
        "falsify_routes": list(court.get("falsify_routes") or [])[:8],
        "mutation_width": width,
        "batch_size_multiplier": batch,
    }
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Compiles temporal memory, replay, regime, survival, conflict, and aging into the next hunt opening.",
        "policy_packet": packet,
        "worker_roles": worker_roles,
        "focus_routes": focus,
        "avoid_routes": avoid,
        "mutation_width": width,
        "batch_size_multiplier": batch,
    }


def temporal_memory_suite(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    horizons = multi_horizon_memory_stack(state)
    half_life = lesson_half_life_engine_v2({**state, "multi_horizon_memory_stack": horizons})
    replay = cross_hunt_strategy_replay({**state, "multi_horizon_memory_stack": horizons, "lesson_half_life_engine_v2": half_life})
    regime = temporal_regime_fingerprinting(state)
    survival = longitudinal_promotion_survival_model({**state, "temporal_regime_fingerprinting": regime})
    court = memory_conflict_court_v2({
        **state,
        "multi_horizon_memory_stack": horizons,
        "lesson_half_life_engine_v2": half_life,
        "longitudinal_promotion_survival_model": survival,
    })
    aging = strategy_aging_dashboard({**state, "lesson_half_life_engine_v2": half_life})
    opening = next_hunt_opening_policy_compiler({
        **state,
        "multi_horizon_memory_stack": horizons,
        "lesson_half_life_engine_v2": half_life,
        "cross_hunt_strategy_replay": replay,
        "temporal_regime_fingerprinting": regime,
        "longitudinal_promotion_survival_model": survival,
        "memory_conflict_court_v2": court,
        "strategy_aging_dashboard": aging,
    })
    return {
        "multi_horizon_memory_stack": horizons,
        "lesson_half_life_engine_v2": half_life,
        "cross_hunt_strategy_replay": replay,
        "temporal_regime_fingerprinting": regime,
        "longitudinal_promotion_survival_model": survival,
        "memory_conflict_court_v2": court,
        "strategy_aging_dashboard": aging,
        "next_hunt_opening_policy_compiler": opening,
    }


def question_driven_hunt_planner(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    rows = _temporal_candidate_rows(state, limit=40)
    uncertainty = state.get("uncertainty_budgeting") if isinstance(state.get("uncertainty_budgeting"), dict) else {}
    temporal = state.get("next_hunt_opening_policy_compiler") if isinstance(state.get("next_hunt_opening_policy_compiler"), dict) else {}
    focus_pool = _ordered_unique(
        list((temporal.get("policy_packet") or {}).get("focus_routes") or [])
        + [row.get("route_key") for row in rows[:10]]
        + [((uncertainty.get("top_budget") or {}).get("route_key") or "")]
    )
    templates = [
        "Does this edge survive holdout/day-parity stress?",
        "Is this route a ticker-specific artifact or a reusable setup?",
        "Which gene is carrying the live delta?",
        "Does a wider indicator/exit shuffle preserve the edge?",
        "Can promotion readiness improve without giving back P/L?",
    ]
    questions = []
    for idx, route in enumerate(focus_pool[:12]):
        row = next((candidate for candidate in rows if candidate.get("route_key") == route), {})
        question = templates[idx % len(templates)]
        if float(row.get("promotion_readiness_score") or 0.0) < 60.0:
            question = "Can this live beater be repaired into promotion-survivable shape?"
        questions.append({
            "question_id": _stable_hash({"active_question": route, "idx": idx}),
            "route_key": route,
            "question": question,
            "variant": row.get("variant"),
            "worker": f"worker_{(idx % 4) + 1}",
            "action": "answer_hunt_question",
            "mutation_width": "tight" if "survive" in question or "repair" in question else "medium",
            "success_evidence": ["beats current live", "improves or preserves promotion readiness", "adds non-duplicate causal evidence"],
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Turns the next hunt into explicit answerable questions instead of route-only exploration.",
        "questions": questions,
        "top_question": questions[0] if questions else {},
        "focus_routes": _ordered_unique([row.get("route_key") for row in questions])[:12],
        "worker_questions": questions[:4],
    }


def expected_information_gain_scorer_v2(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    rows = _temporal_candidate_rows(state, limit=60)
    heatmap = state.get("uncertainty_heatmap") if isinstance(state.get("uncertainty_heatmap"), dict) else {}
    question_plan = state.get("question_driven_hunt_planner") if isinstance(state.get("question_driven_hunt_planner"), dict) else {}
    question_routes = set(str(row.get("route_key") or "") for row in question_plan.get("questions") or [] if isinstance(row, dict))
    heat_routes = {str(row.get("route_key") or ""): float(row.get("uncertainty_score") or 0.0) for row in heatmap.get("cells") or [] if isinstance(row, dict)}
    scored = []
    for row in rows:
        route = _row_route(row)
        delta = max(0.0, float(row.get("step2_delta_vs_active") or 0.0))
        readiness = float(row.get("promotion_readiness_score") or 50.0)
        uncertainty_score = heat_routes.get(route, max(0.0, 100.0 - readiness))
        novelty = float(row.get("novelty_score") or 0.0)
        question_bonus = 18.0 if route in question_routes else 0.0
        info_gain = uncertainty_score * 0.45 + min(delta, 120.0) * 0.25 + novelty * 0.15 + question_bonus
        scored.append({
            "route_key": route,
            "variant": row.get("variant"),
            "expected_information_gain": round(info_gain, 4),
            "uncertainty_score": round(uncertainty_score, 4),
            "pnl_delta": round(delta, 4),
            "recommended_action": "probe_for_learning" if info_gain >= 35.0 else "exploit_only",
        })
    scored.sort(key=lambda row: float(row.get("expected_information_gain") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Ranks candidates by expected learning value, not only immediate P/L.",
        "scores": scored[:40],
        "top_score": scored[0] if scored else {},
        "focus_routes": _ordered_unique([row.get("route_key") for row in scored if row.get("recommended_action") == "probe_for_learning"])[:10],
        "exploration_budget_pct": 22.0 if scored else 0.0,
    }


def uncertainty_heatmap(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    rows = _temporal_candidate_rows(state, limit=80)
    memory = state.get("multi_horizon_memory_stack") if isinstance(state.get("multi_horizon_memory_stack"), dict) else {}
    survival = state.get("longitudinal_promotion_survival_model") if isinstance(state.get("longitudinal_promotion_survival_model"), dict) else {}
    durable_routes = set(str(route) for route in (memory.get("horizons") or {}).get("durable", {}).get("focus_routes") or [])
    survival_routes = set(str(route) for route in survival.get("survival_focus_routes") or [])
    cells = []
    for row in rows:
        route = _row_route(row)
        evidence_count = 1
        if route in durable_routes:
            evidence_count += 3
        if route in survival_routes:
            evidence_count += 2
        readiness = float(row.get("promotion_readiness_score") or 50.0)
        promise = max(0.0, float(row.get("step2_delta_vs_active") or 0.0)) + readiness * 0.25
        uncertainty_score = max(0.0, promise / max(1.0, evidence_count) + max(0.0, 70.0 - readiness) * 0.35)
        cells.append({
            "cell_id": _stable_hash({"uncertainty": route, "lane": _row_lane(row)}),
            "route_key": route,
            "lane": _row_lane(row),
            "regime": row.get("regime") or row.get("session_phase") or "",
            "promise_score": round(promise, 4),
            "evidence_count": evidence_count,
            "uncertainty_score": round(uncertainty_score, 4),
        })
    cells.sort(key=lambda row: float(row.get("uncertainty_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Highlights promising but under-evidenced routes, genes, and regimes that deserve diagnostic probes.",
        "cells": cells[:40],
        "top_cell": cells[0] if cells else {},
        "probe_routes": _ordered_unique([row.get("route_key") for row in cells[:10]])[:10],
    }


def adaptive_experiment_sequencer(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    question_plan = state.get("question_driven_hunt_planner") if isinstance(state.get("question_driven_hunt_planner"), dict) else {}
    eig = state.get("expected_information_gain_scorer_v2") if isinstance(state.get("expected_information_gain_scorer_v2"), dict) else {}
    stop_loss = state.get("learning_value_stop_loss") if isinstance(state.get("learning_value_stop_loss"), dict) else {}
    stopped = set(str(route) for route in stop_loss.get("avoid_routes") or [])
    steps = []
    source_rows = list(question_plan.get("questions") or [])
    if not source_rows:
        source_rows = [{"route_key": row.get("route_key"), "question": "What is the highest-value uncertainty probe?"} for row in eig.get("scores") or []]
    for idx, question in enumerate(source_rows[:10]):
        if not isinstance(question, dict):
            continue
        route = str(question.get("route_key") or "")
        if not route or route in stopped:
            continue
        steps.append({
            "step_id": _stable_hash({"sequencer": route, "idx": idx}),
            "route_key": route,
            "question_id": question.get("question_id"),
            "if_success": "scale_to_promotion_survival_probe",
            "if_failure": "run_counter_test_or_retire_question",
            "worker": question.get("worker") or f"worker_{(idx % 4) + 1}",
            "action": "sequenced_question_probe",
            "mutation_width": question.get("mutation_width") or "medium",
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Chains hunt experiments so each answer determines the next diagnostic step.",
        "steps": steps,
        "next_step": steps[0] if steps else {},
        "focus_routes": _ordered_unique([row.get("route_key") for row in steps])[:10],
        "avoid_routes": _ordered_unique(list(stopped))[:12],
    }


def learning_value_stop_loss(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    rows = _temporal_candidate_rows(state, limit=80)
    heatmap = state.get("uncertainty_heatmap") if isinstance(state.get("uncertainty_heatmap"), dict) else {}
    heat = {str(row.get("route_key") or ""): float(row.get("uncertainty_score") or 0.0) for row in heatmap.get("cells") or [] if isinstance(row, dict)}
    stops = []
    for row in rows:
        route = _row_route(row)
        delta = float(row.get("step2_delta_vs_active") or 0.0)
        readiness = float(row.get("promotion_readiness_score") or 50.0)
        learning_value = heat.get(route, 0.0) + max(0.0, delta) * 0.12 + readiness * 0.08
        if learning_value < 18.0 and delta < 35.0:
            stops.append({
                "route_key": route,
                "variant": row.get("variant"),
                "learning_value": round(learning_value, 4),
                "reason": "low information gain and weak live delta",
                "action": "pause_lane",
            })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Pauses lanes that are no longer teaching useful lessons, even if they still produce mediocre candidates.",
        "stops": stops[:20],
        "stop_count": len(stops),
        "avoid_routes": _ordered_unique([row.get("route_key") for row in stops])[:12],
        "batch_size_multiplier": 0.82 if stops else None,
    }


def causal_question_ledger(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    question_plan = state.get("question_driven_hunt_planner") if isinstance(state.get("question_driven_hunt_planner"), dict) else {}
    hypothesis = state.get("hunt_hypothesis_compiler") if isinstance(state.get("hunt_hypothesis_compiler"), dict) else {}
    rows = []
    for question in question_plan.get("questions") or []:
        if not isinstance(question, dict):
            continue
        rows.append({
            "ledger_id": _stable_hash({"causal_question": question.get("question_id")}),
            "route_key": question.get("route_key"),
            "question": question.get("question"),
            "status": "open",
            "needed_evidence": question.get("success_evidence") or [],
        })
    for item in hypothesis.get("hypotheses") or []:
        if not isinstance(item, dict):
            continue
        rows.append({
            "ledger_id": _stable_hash({"causal_hypothesis": item.get("hypothesis_id")}),
            "route_key": item.get("route_key"),
            "question": item.get("hypothesis"),
            "status": "open",
            "needed_evidence": item.get("tests") or [],
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Live backlog of causal questions the hunt should answer before trusting or scaling lessons.",
        "questions": rows[:40],
        "top_question": rows[0] if rows else {},
        "open_question_count": len(rows),
        "focus_routes": _ordered_unique([row.get("route_key") for row in rows])[:12],
    }


def worker_epistemic_roles_v2(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    question_plan = state.get("question_driven_hunt_planner") if isinstance(state.get("question_driven_hunt_planner"), dict) else {}
    heatmap = state.get("uncertainty_heatmap") if isinstance(state.get("uncertainty_heatmap"), dict) else {}
    hypothesis = state.get("hunt_hypothesis_compiler") if isinstance(state.get("hunt_hypothesis_compiler"), dict) else {}
    focus = _ordered_unique(
        list(question_plan.get("focus_routes") or [])
        + list(heatmap.get("probe_routes") or [])
        + list(hypothesis.get("focus_routes") or [])
    )
    roles = [
        ("worker_1", "exploiter", "scale the highest-confidence live beater without breaking promotion readiness"),
        ("worker_2", "skeptic", "try to falsify the leading hypothesis with tight counter-tests"),
        ("worker_3", "regime_mapper", "map ticker/setup/phase boundaries for the edge"),
        ("worker_4", "chaos_mutator", "try controlled crazy indicator/score shuffles on high-uncertainty routes"),
    ]
    assignments = []
    for idx, (worker, role, mission) in enumerate(roles):
        route = (focus[idx:] or focus or [""])[0]
        assignments.append({
            "worker": worker,
            "epistemic_role": role,
            "mission": mission,
            "route_key": route,
            "action": f"{role}_probe",
            "mutation_width": "wide" if role == "chaos_mutator" else "tight" if role == "skeptic" else "medium",
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Assigns workers by epistemic function so the hunt learns from exploitation, skepticism, regime mapping, and creative mutation.",
        "assignments": assignments,
        "focus_routes": _ordered_unique([row.get("route_key") for row in assignments])[:8],
    }


def hunt_hypothesis_compiler(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    rows = _temporal_candidate_rows(state, limit=20)
    regime = state.get("temporal_regime_fingerprinting") if isinstance(state.get("temporal_regime_fingerprinting"), dict) else {}
    survival = state.get("longitudinal_promotion_survival_model") if isinstance(state.get("longitudinal_promotion_survival_model"), dict) else {}
    eig = state.get("expected_information_gain_scorer_v2") if isinstance(state.get("expected_information_gain_scorer_v2"), dict) else {}
    hypotheses = []
    for idx, row in enumerate(rows[:5]):
        route = _row_route(row)
        lane = _row_lane(row)
        top_regime = (regime.get("top_fingerprint") or {}).get("fingerprint") or route
        survival_feature = (survival.get("top_survival_feature") or {}).get("feature") or f"route:{route}"
        eig_score = next((score for score in eig.get("scores") or [] if isinstance(score, dict) and score.get("route_key") == route), {})
        hypotheses.append({
            "hypothesis_id": _stable_hash({"hypothesis": route, "lane": lane, "idx": idx}),
            "route_key": route,
            "hypothesis": f"{route} is live-beating because {lane} interacts with {top_regime}, and {survival_feature} determines promotion survival.",
            "confidence": round(min(0.95, (float(row.get("promotion_readiness_score") or 50.0) / 100.0) * 0.7 + 0.15), 4),
            "expected_information_gain": eig_score.get("expected_information_gain"),
            "tests": [
                "tight sibling around current winner",
                "holdout/day parity counter-test",
                "one controlled indicator or exit shuffle",
            ],
            "worker": f"worker_{(idx % 4) + 1}",
        })
    assignments = [
        {
            "worker": row.get("worker"),
            "route_key": row.get("route_key"),
            "action": "hypothesis_test",
            "hypothesis_id": row.get("hypothesis_id"),
            "mutation_width": "tight",
        }
        for row in hypotheses[:4]
    ]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Compiles the hunt's current beliefs into explicit hypotheses with tests.",
        "hypotheses": hypotheses,
        "top_hypothesis": hypotheses[0] if hypotheses else {},
        "worker_assignments": assignments,
        "focus_routes": _ordered_unique([row.get("route_key") for row in hypotheses])[:8],
    }


def active_uncertainty_learning_suite(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    questions = question_driven_hunt_planner(state)
    heatmap = uncertainty_heatmap({**state, "question_driven_hunt_planner": questions})
    eig = expected_information_gain_scorer_v2({**state, "question_driven_hunt_planner": questions, "uncertainty_heatmap": heatmap})
    stop = learning_value_stop_loss({**state, "uncertainty_heatmap": heatmap, "expected_information_gain_scorer_v2": eig})
    sequencer = adaptive_experiment_sequencer({
        **state,
        "question_driven_hunt_planner": questions,
        "expected_information_gain_scorer_v2": eig,
        "learning_value_stop_loss": stop,
    })
    hypotheses = hunt_hypothesis_compiler({
        **state,
        "uncertainty_heatmap": heatmap,
        "expected_information_gain_scorer_v2": eig,
    })
    ledger = causal_question_ledger({
        **state,
        "question_driven_hunt_planner": questions,
        "hunt_hypothesis_compiler": hypotheses,
    })
    roles = worker_epistemic_roles_v2({
        **state,
        "question_driven_hunt_planner": questions,
        "uncertainty_heatmap": heatmap,
        "hunt_hypothesis_compiler": hypotheses,
    })
    return {
        "question_driven_hunt_planner": questions,
        "expected_information_gain_scorer_v2": eig,
        "uncertainty_heatmap": heatmap,
        "adaptive_experiment_sequencer": sequencer,
        "learning_value_stop_loss": stop,
        "causal_question_ledger": ledger,
        "worker_epistemic_roles_v2": roles,
        "hunt_hypothesis_compiler": hypotheses,
    }


def experiment_contract_compiler(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    questions = state.get("question_driven_hunt_planner") if isinstance(state.get("question_driven_hunt_planner"), dict) else {}
    hypotheses = state.get("hunt_hypothesis_compiler") if isinstance(state.get("hunt_hypothesis_compiler"), dict) else {}
    power = state.get("promotion_aware_power_planner") if isinstance(state.get("promotion_aware_power_planner"), dict) else {}
    rows = _temporal_candidate_rows(state, limit=60)
    by_route = {str(row.get("route_key") or _row_route(row)): row for row in rows}
    source_items = list(hypotheses.get("hypotheses") or [])
    if not source_items:
        source_items = list(questions.get("questions") or [])
    contracts = []
    for idx, item in enumerate(source_items[:12]):
        if not isinstance(item, dict):
            continue
        route = str(item.get("route_key") or "")
        if not route:
            continue
        row = by_route.get(route, {})
        needed = next((p for p in power.get("plans") or [] if isinstance(p, dict) and p.get("route_key") == route), {})
        contract = {
            "contract_id": _stable_hash({"experiment_contract": route, "idx": idx, "hypothesis": item.get("hypothesis_id") or item.get("question_id")}),
            "route_key": route,
            "variant": item.get("variant") or row.get("variant"),
            "hypothesis_id": item.get("hypothesis_id"),
            "question_id": item.get("question_id"),
            "question": item.get("hypothesis") or item.get("question") or "Does this mutation causally improve live P/L without hurting promotion readiness?",
            "treatment": {
                "action": "mutate_under_contract",
                "mutation_width": "tight" if float(row.get("promotion_readiness_score") or 50.0) < 65.0 else "medium",
                "lane": _row_lane(row) if row else "contract_probe",
            },
            "control": {
                "route_key": route,
                "action": "hold_current_profile_constant",
                "sample_weight": 0.35,
            },
            "success_metric": "delta_vs_current_live_and_promotion_readiness",
            "min_live_delta": max(10.0, min(45.0, float(row.get("step2_delta_vs_active") or 0.0) * 0.25)),
            "min_readiness": max(65.0, float(needed.get("required_readiness") or 70.0)),
            "stop_rule": "stop_if_no_live_beater_or_readiness_regresses_after_controlled_probe",
            "worker": item.get("worker") or f"worker_{(idx % 4) + 1}",
        }
        contracts.append(contract)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Converts active hunt questions and hypotheses into strict treatment/control experiment contracts.",
        "contracts": contracts,
        "top_contract": contracts[0] if contracts else {},
        "focus_routes": _ordered_unique([row.get("route_key") for row in contracts])[:12],
    }


def control_route_matcher(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    contracts = state.get("experiment_contract_compiler") if isinstance(state.get("experiment_contract_compiler"), dict) else {}
    rows = _temporal_candidate_rows(state, limit=80)
    matches = []
    for contract in contracts.get("contracts") or []:
        if not isinstance(contract, dict):
            continue
        route = str(contract.get("route_key") or "")
        treatment_row = next((row for row in rows if row.get("route_key") == route), {})
        lane = _row_lane(treatment_row) if treatment_row else str((contract.get("treatment") or {}).get("lane") or "")
        candidates = []
        for row in rows:
            candidate_route = str(row.get("route_key") or "")
            if not candidate_route or candidate_route == route:
                continue
            lane_match = 1.0 if _row_lane(row) == lane else 0.55
            readiness_gap = abs(float(row.get("promotion_readiness_score") or 50.0) - float(treatment_row.get("promotion_readiness_score") or 50.0))
            delta_gap = abs(float(row.get("step2_delta_vs_active") or 0.0) - float(treatment_row.get("step2_delta_vs_active") or 0.0))
            score = lane_match * 70.0 - readiness_gap * 0.35 - delta_gap * 0.08
            candidates.append((score, row))
        candidates.sort(key=lambda item: item[0], reverse=True)
        control = candidates[0][1] if candidates else treatment_row
        matches.append({
            "contract_id": contract.get("contract_id"),
            "treatment_route": route,
            "control_route": control.get("route_key") or route,
            "control_variant": control.get("variant"),
            "match_score": round(float(candidates[0][0]) if candidates else 50.0, 4),
            "match_reason": "same lane/readiness nearest neighbor" if candidates else "self-control fallback",
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Finds fair controls so contract outcomes can be interpreted causally.",
        "matches": matches,
        "top_match": matches[0] if matches else {},
        "control_routes": _ordered_unique([row.get("control_route") for row in matches])[:12],
    }


def sequential_test_monitor(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    contracts = state.get("experiment_contract_compiler") if isinstance(state.get("experiment_contract_compiler"), dict) else {}
    controls = state.get("control_route_matcher") if isinstance(state.get("control_route_matcher"), dict) else {}
    rows = _temporal_candidate_rows(state, limit=80)
    by_route = {str(row.get("route_key") or ""): row for row in rows}
    control_by_contract = {str(row.get("contract_id")): row for row in controls.get("matches") or [] if isinstance(row, dict)}
    decisions = []
    for contract in contracts.get("contracts") or []:
        if not isinstance(contract, dict):
            continue
        route = str(contract.get("route_key") or "")
        row = by_route.get(route, {})
        match = control_by_contract.get(str(contract.get("contract_id")), {})
        control_row = by_route.get(str(match.get("control_route") or ""), {})
        treatment_delta = float(row.get("step2_delta_vs_active") or 0.0)
        control_delta = float(control_row.get("step2_delta_vs_active") or 0.0)
        readiness = float(row.get("promotion_readiness_score") or 0.0)
        lift = treatment_delta - control_delta
        if readiness < float(contract.get("min_readiness") or 65.0):
            decision = "repair"
            reason = "readiness below contract threshold"
        elif lift >= float(contract.get("min_live_delta") or 10.0):
            decision = "scale"
            reason = "treatment lift cleared control-adjusted threshold"
        elif treatment_delta <= 0.0:
            decision = "stop"
            reason = "no live edge under contract"
        else:
            decision = "continue"
            reason = "edge exists but causal lift is not decisive yet"
        decisions.append({
            "contract_id": contract.get("contract_id"),
            "route_key": route,
            "control_route": match.get("control_route"),
            "treatment_delta": round(treatment_delta, 4),
            "control_delta": round(control_delta, 4),
            "estimated_lift": round(lift, 4),
            "readiness": round(readiness, 2),
            "decision": decision,
            "reason": reason,
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Sequentially monitors contracts and decides continue, scale, stop, repair, or invert.",
        "decisions": decisions,
        "top_decision": decisions[0] if decisions else {},
        "focus_routes": _ordered_unique([row.get("route_key") for row in decisions if row.get("decision") in {"scale", "continue", "repair"}])[:12],
        "avoid_routes": _ordered_unique([row.get("route_key") for row in decisions if row.get("decision") == "stop"])[:12],
    }


def causal_effect_size_ledger(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    monitor = state.get("sequential_test_monitor") if isinstance(state.get("sequential_test_monitor"), dict) else {}
    contracts = state.get("experiment_contract_compiler") if isinstance(state.get("experiment_contract_compiler"), dict) else {}
    contract_by_id = {str(row.get("contract_id")): row for row in contracts.get("contracts") or [] if isinstance(row, dict)}
    effects = []
    for decision in monitor.get("decisions") or []:
        if not isinstance(decision, dict):
            continue
        contract = contract_by_id.get(str(decision.get("contract_id")), {})
        lift = float(decision.get("estimated_lift") or 0.0)
        readiness = float(decision.get("readiness") or 0.0)
        confidence = min(0.95, max(0.15, 0.35 + abs(lift) / 160.0 + readiness / 300.0))
        effects.append({
            "effect_id": _stable_hash({"effect": decision.get("contract_id"), "route": decision.get("route_key")}),
            "contract_id": decision.get("contract_id"),
            "route_key": decision.get("route_key"),
            "lane": (contract.get("treatment") or {}).get("lane"),
            "effect_size": round(lift, 4),
            "confidence": round(confidence, 4),
            "sample_quality": "controlled" if decision.get("control_route") else "fallback",
            "decision": decision.get("decision"),
        })
    effects.sort(key=lambda row: (float(row.get("effect_size") or 0.0), float(row.get("confidence") or 0.0)), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Records estimated causal lift by route/lane/gene with confidence and sample quality.",
        "effects": effects,
        "top_effect": effects[0] if effects else {},
        "focus_routes": _ordered_unique([row.get("route_key") for row in effects if float(row.get("effect_size") or 0.0) > 0])[:12],
    }


def false_positive_pressure_gauge(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    rows = _temporal_candidate_rows(state, limit=100)
    warning = state.get("false_discovery_early_warning") if isinstance(state.get("false_discovery_early_warning"), dict) else {}
    heatmap = state.get("uncertainty_heatmap") if isinstance(state.get("uncertainty_heatmap"), dict) else {}
    candidate_count = len(rows)
    low_readiness = sum(1 for row in rows if float(row.get("promotion_readiness_score") or 0.0) < 60.0)
    warning_count = int(warning.get("warning_count") or 0)
    high_uncertainty = len(heatmap.get("cells") or [])
    pressure = min(100.0, warning_count * 12.0 + (low_readiness / max(1, candidate_count)) * 55.0 + min(25.0, high_uncertainty * 0.7))
    mode = "strict_control" if pressure >= 70.0 else "cautious_control" if pressure >= 40.0 else "normal"
    avoid = list(warning.get("avoid_routes") or [])
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Measures how much aggressive search is likely selecting noise and tightens controls when pressure rises.",
        "pressure_score": round(pressure, 4),
        "mode": mode,
        "low_readiness_count": low_readiness,
        "warning_count": warning_count,
        "avoid_routes": _ordered_unique(avoid)[:12],
        "batch_size_multiplier": 0.75 if mode == "strict_control" else 0.88 if mode == "cautious_control" else None,
    }


def exploration_debt_paydown_planner(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    heatmap = state.get("uncertainty_heatmap") if isinstance(state.get("uncertainty_heatmap"), dict) else {}
    ledger = state.get("exploration_debt_ledger") if isinstance(state.get("exploration_debt_ledger"), dict) else {}
    cells = list(heatmap.get("cells") or [])
    debts = list(ledger.get("debts") or ledger.get("queue") or [])
    plans = []
    for idx, cell in enumerate(cells[:8]):
        if not isinstance(cell, dict):
            continue
        plans.append({
            "plan_id": _stable_hash({"paydown_cell": cell.get("cell_id"), "idx": idx}),
            "route_key": cell.get("route_key"),
            "reason": "high uncertainty cell needs controlled coverage",
            "budget_pct": 4.0 + min(6.0, float(cell.get("uncertainty_score") or 0.0) / 20.0),
            "worker": f"worker_{(idx % 4) + 1}",
            "action": "exploration_debt_paydown",
            "mutation_width": "medium",
        })
    for idx, debt in enumerate(debts[:4]):
        if not isinstance(debt, dict):
            continue
        route = str(debt.get("route_key") or "")
        if route and route not in {str(row.get("route_key")) for row in plans}:
            plans.append({
                "plan_id": _stable_hash({"paydown_debt": debt, "idx": idx}),
                "route_key": route,
                "reason": debt.get("reason") or "legacy exploration debt",
                "budget_pct": 5.0,
                "worker": f"worker_{(len(plans) % 4) + 1}",
                "action": "exploration_debt_paydown",
                "mutation_width": "wide",
            })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Pays down neglected high-uncertainty search cells without letting exploitation starve coverage.",
        "plans": plans[:12],
        "top_plan": plans[0] if plans else {},
        "focus_routes": _ordered_unique([row.get("route_key") for row in plans])[:12],
        "paydown_budget_pct": round(sum(float(row.get("budget_pct") or 0.0) for row in plans[:6]), 2),
    }


def promotion_aware_power_planner(state: dict[str, Any] | None = None) -> dict[str, Any]:
    rows = _temporal_candidate_rows(state, limit=60)
    plans = []
    for row in rows[:30]:
        route = _row_route(row)
        readiness = float(row.get("promotion_readiness_score") or 50.0)
        delta = max(0.0, float(row.get("step2_delta_vs_active") or 0.0))
        evidence_gap = max(0.0, 75.0 - readiness) + max(0.0, 35.0 - delta) * 0.4
        required_tests = 1 + int(evidence_gap // 15.0)
        plans.append({
            "route_key": route,
            "variant": row.get("variant"),
            "current_readiness": round(readiness, 2),
            "current_delta": round(delta, 4),
            "required_readiness": 72.0 if delta >= 80.0 else 78.0,
            "required_controlled_tests": max(1, min(5, required_tests)),
            "power_status": "review_ready" if readiness >= 78.0 and delta >= 35.0 else "needs_more_evidence",
        })
    plans.sort(key=lambda row: (row.get("power_status") == "review_ready", float(row.get("current_delta") or 0.0)), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Estimates how much controlled evidence a candidate needs before promotion review is worth running.",
        "plans": plans,
        "top_plan": plans[0] if plans else {},
        "review_ready_routes": _ordered_unique([row.get("route_key") for row in plans if row.get("power_status") == "review_ready"])[:10],
        "focus_routes": _ordered_unique([row.get("route_key") for row in plans[:10]])[:10],
    }


def scientific_hunt_executive(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    contracts = state.get("experiment_contract_compiler") if isinstance(state.get("experiment_contract_compiler"), dict) else {}
    controls = state.get("control_route_matcher") if isinstance(state.get("control_route_matcher"), dict) else {}
    monitor = state.get("sequential_test_monitor") if isinstance(state.get("sequential_test_monitor"), dict) else {}
    effects = state.get("causal_effect_size_ledger") if isinstance(state.get("causal_effect_size_ledger"), dict) else {}
    pressure = state.get("false_positive_pressure_gauge") if isinstance(state.get("false_positive_pressure_gauge"), dict) else {}
    paydown = state.get("exploration_debt_paydown_planner") if isinstance(state.get("exploration_debt_paydown_planner"), dict) else {}
    power = state.get("promotion_aware_power_planner") if isinstance(state.get("promotion_aware_power_planner"), dict) else {}
    control_by_contract = {str(row.get("contract_id")): row for row in controls.get("matches") or [] if isinstance(row, dict)}
    decision_by_contract = {str(row.get("contract_id")): row for row in monitor.get("decisions") or [] if isinstance(row, dict)}
    commands = []
    for idx, contract in enumerate(contracts.get("contracts") or []):
        if not isinstance(contract, dict):
            continue
        decision = decision_by_contract.get(str(contract.get("contract_id")), {})
        match = control_by_contract.get(str(contract.get("contract_id")), {})
        commands.append({
            "command_id": _stable_hash({"scientific_command": contract.get("contract_id"), "idx": idx}),
            "worker": contract.get("worker") or f"worker_{(idx % 4) + 1}",
            "action": decision.get("decision") or "run_controlled_contract",
            "route_key": contract.get("route_key"),
            "control_route": match.get("control_route"),
            "contract_id": contract.get("contract_id"),
            "mutation_width": (contract.get("treatment") or {}).get("mutation_width") or "medium",
            "batch_size_multiplier": 0.82 if pressure.get("mode") == "strict_control" else 0.95,
            "reason": decision.get("reason") or "scientific_hunt_executive",
        })
    for plan in paydown.get("plans") or []:
        if not isinstance(plan, dict):
            continue
        commands.append({
            "command_id": plan.get("plan_id") or _stable_hash({"paydown_command": plan}),
            "worker": plan.get("worker") or f"worker_{(len(commands) % 4) + 1}",
            "action": plan.get("action") or "exploration_debt_paydown",
            "route_key": plan.get("route_key"),
            "mutation_width": plan.get("mutation_width") or "medium",
            "batch_size_multiplier": 0.7,
            "reason": plan.get("reason") or "exploration_debt_paydown",
        })
    focus = _ordered_unique(
        list(monitor.get("focus_routes") or [])
        + list(effects.get("focus_routes") or [])
        + list(power.get("review_ready_routes") or [])
        + list(paydown.get("focus_routes") or [])
    )[:12]
    avoid = _ordered_unique(list(monitor.get("avoid_routes") or []) + list(pressure.get("avoid_routes") or []))[:12]
    packet = {
        "live_only": True,
        "objective": "run_controlled_scientific_hunt",
        "focus_routes": [route for route in focus if route not in set(avoid)],
        "avoid_routes": avoid,
        "commands": commands[:20],
        "mutation_width": "tight" if pressure.get("mode") == "strict_control" else "medium",
        "batch_size_multiplier": 0.82 if pressure.get("mode") == "strict_control" else 0.95,
        "pressure_mode": pressure.get("mode") or "normal",
    }
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Top-level scientific controller that converts contracts, controls, power, and evidence into worker-ready commands.",
        "command_packet": packet,
        "commands": commands[:20],
        "focus_routes": packet["focus_routes"],
        "avoid_routes": avoid,
        "top_command": commands[0] if commands else {},
        "summary": {
            "contract_count": len(contracts.get("contracts") or []),
            "control_count": len(controls.get("matches") or []),
            "pressure_mode": pressure.get("mode") or "normal",
            "review_ready_count": len(power.get("review_ready_routes") or []),
            "positive_effect_count": len(effects.get("focus_routes") or []),
        },
    }


def closed_loop_scientific_execution_suite(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    power = promotion_aware_power_planner(state)
    contracts = experiment_contract_compiler({**state, "promotion_aware_power_planner": power})
    controls = control_route_matcher({**state, "experiment_contract_compiler": contracts})
    monitor = sequential_test_monitor({
        **state,
        "experiment_contract_compiler": contracts,
        "control_route_matcher": controls,
    })
    effects = causal_effect_size_ledger({
        **state,
        "experiment_contract_compiler": contracts,
        "sequential_test_monitor": monitor,
    })
    pressure = false_positive_pressure_gauge(state)
    paydown = exploration_debt_paydown_planner(state)
    executive = scientific_hunt_executive({
        **state,
        "experiment_contract_compiler": contracts,
        "control_route_matcher": controls,
        "sequential_test_monitor": monitor,
        "causal_effect_size_ledger": effects,
        "false_positive_pressure_gauge": pressure,
        "exploration_debt_paydown_planner": paydown,
        "promotion_aware_power_planner": power,
    })
    return {
        "experiment_contract_compiler": contracts,
        "control_route_matcher": controls,
        "sequential_test_monitor": monitor,
        "causal_effect_size_ledger": effects,
        "false_positive_pressure_gauge": pressure,
        "exploration_debt_paydown_planner": paydown,
        "promotion_aware_power_planner": power,
        "scientific_hunt_executive": executive,
    }


def live_candidate_evidence_builder(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    rows = _temporal_candidate_rows(state, limit=100)
    effects = state.get("causal_effect_size_ledger") if isinstance(state.get("causal_effect_size_ledger"), dict) else {}
    monitor = state.get("sequential_test_monitor") if isinstance(state.get("sequential_test_monitor"), dict) else {}
    power = state.get("promotion_aware_power_planner") if isinstance(state.get("promotion_aware_power_planner"), dict) else {}
    effect_by_route = {str(row.get("route_key") or ""): row for row in effects.get("effects") or [] if isinstance(row, dict)}
    decision_by_route = {str(row.get("route_key") or ""): row for row in monitor.get("decisions") or [] if isinstance(row, dict)}
    power_by_route = {str(row.get("route_key") or ""): row for row in power.get("plans") or [] if isinstance(row, dict)}
    packets = []
    for row in rows[:40]:
        route = _row_route(row)
        readiness = float(row.get("promotion_readiness_score") or 0.0)
        delta = float(row.get("step2_delta_vs_active") or 0.0)
        effect = effect_by_route.get(route, {})
        decision = decision_by_route.get(route, {})
        power_plan = power_by_route.get(route, {})
        categories = {
            "live_edge": "pass" if delta > 0 else "fail",
            "readiness": "pass" if readiness >= 75 else "watch" if readiness >= 60 else "fail",
            "controlled_lift": "pass" if float(effect.get("effect_size") or 0.0) > 0 else "watch" if decision else "missing",
            "power": power_plan.get("power_status") or "missing",
            "lineage": "present" if row.get("parent_variant") or row.get("family_key") else "missing",
        }
        evidence_score = (
            min(45.0, max(0.0, delta) * 0.25)
            + min(30.0, readiness * 0.30)
            + max(0.0, min(20.0, float(effect.get("effect_size") or 0.0) * 0.20))
            + (5.0 if power_plan.get("power_status") == "review_ready" else 0.0)
        )
        packets.append({
            "packet_id": _stable_hash({"live_evidence_packet": row.get("variant"), "route": route}),
            "variant": row.get("variant"),
            "route_key": route,
            "family_key": row.get("family_key"),
            "step2_pnl": row.get("step2_pnl"),
            "step2_delta_vs_active": round(delta, 4),
            "promotion_readiness_score": round(readiness, 2),
            "controlled_effect": effect,
            "sequential_decision": decision,
            "power_plan": power_plan,
            "categories": categories,
            "evidence_score": round(evidence_score, 4),
        })
    packets.sort(key=lambda item: float(item.get("evidence_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Builds promotion evidence packets for live-beating candidates during the hunt.",
        "packets": packets,
        "top_packet": packets[0] if packets else {},
        "focus_routes": _ordered_unique([row.get("route_key") for row in packets[:12]])[:12],
    }


def promotion_failure_predictor_v3(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    evidence = state.get("live_candidate_evidence_builder") if isinstance(state.get("live_candidate_evidence_builder"), dict) else {}
    rows = evidence.get("packets") or []
    predictions = []
    for packet in rows[:60]:
        if not isinstance(packet, dict):
            continue
        categories = packet.get("categories") if isinstance(packet.get("categories"), dict) else {}
        readiness = float(packet.get("promotion_readiness_score") or 0.0)
        delta = float(packet.get("step2_delta_vs_active") or 0.0)
        effect = packet.get("controlled_effect") if isinstance(packet.get("controlled_effect"), dict) else {}
        reasons = []
        if readiness < 70.0:
            reasons.append({"reason": "readiness_below_review_bar", "detail": f"readiness {readiness:.1f} below 70"})
        if delta < 25.0:
            reasons.append({"reason": "small_live_delta", "detail": f"live delta {delta:.1f} may not clear review"})
        if categories.get("controlled_lift") in {"missing", "watch"}:
            reasons.append({"reason": "control_weakness", "detail": "controlled lift is missing or not decisive"})
        if categories.get("lineage") == "missing":
            reasons.append({"reason": "lineage_gap", "detail": "parent/current/live change explanation is incomplete"})
        if float(effect.get("effect_size") or 0.0) < 0.0:
            reasons.append({"reason": "negative_control_adjusted_lift", "detail": "treatment underperformed matched control"})
        risk = min(0.98, 0.12 + len(reasons) * 0.16 + max(0.0, 70.0 - readiness) / 140.0 + (0.12 if delta < 25 else 0.0))
        predictions.append({
            "variant": packet.get("variant"),
            "route_key": packet.get("route_key"),
            "predicted_reject_probability": round(risk, 4),
            "risk_band": "high" if risk >= 0.62 else "medium" if risk >= 0.35 else "low",
            "specific_reasons": reasons,
        })
    predictions.sort(key=lambda row: float(row.get("predicted_reject_probability") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Predicts promotion rejection with human-readable, category-specific failure reasons.",
        "predictions": predictions,
        "highest_risk": predictions[0] if predictions else {},
        "repair_routes": _ordered_unique([row.get("route_key") for row in predictions if row.get("risk_band") in {"medium", "high"}])[:12],
        "avoid_routes": _ordered_unique([row.get("route_key") for row in predictions if row.get("risk_band") == "high" and len(row.get("specific_reasons") or []) >= 3])[:12],
    }


def evidence_gap_router(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    evidence = state.get("live_candidate_evidence_builder") if isinstance(state.get("live_candidate_evidence_builder"), dict) else {}
    v3 = state.get("promotion_failure_predictor_v3") if isinstance(state.get("promotion_failure_predictor_v3"), dict) else {}
    risk_by_route = {str(row.get("route_key") or ""): row for row in v3.get("predictions") or [] if isinstance(row, dict)}
    tasks = []
    for idx, packet in enumerate(evidence.get("packets") or []):
        if not isinstance(packet, dict):
            continue
        route = str(packet.get("route_key") or "")
        categories = packet.get("categories") if isinstance(packet.get("categories"), dict) else {}
        missing = [name for name, value in categories.items() if value in {"missing", "fail", "watch", "needs_more_evidence"}]
        risk = risk_by_route.get(route, {})
        for gap in missing[:3]:
            tasks.append({
                "task_id": _stable_hash({"evidence_gap": route, "gap": gap, "idx": idx}),
                "variant": packet.get("variant"),
                "route_key": route,
                "gap": gap,
                "action": "fill_promotion_evidence_gap",
                "worker": f"worker_{((len(tasks)) % 4) + 1}",
                "mutation_width": "tight" if gap in {"readiness", "controlled_lift"} else "medium",
                "reason": ((risk.get("specific_reasons") or [{}])[0] or {}).get("detail") or f"{gap} is incomplete",
            })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Routes workers to fill exact missing promotion evidence for each promising candidate.",
        "tasks": tasks[:40],
        "top_task": tasks[0] if tasks else {},
        "focus_routes": _ordered_unique([row.get("route_key") for row in tasks])[:12],
    }


def review_ready_queue_v2(state: dict[str, Any] | None = None) -> dict[str, Any]:
    evidence = state.get("live_candidate_evidence_builder") if isinstance(state, dict) and isinstance(state.get("live_candidate_evidence_builder"), dict) else {}
    failure = state.get("promotion_failure_predictor_v3") if isinstance(state, dict) and isinstance(state.get("promotion_failure_predictor_v3"), dict) else {}
    risk_by_route = {str(row.get("route_key") or ""): row for row in failure.get("predictions") or [] if isinstance(row, dict)}
    queue = []
    near = []
    for packet in evidence.get("packets") or []:
        if not isinstance(packet, dict):
            continue
        risk = risk_by_route.get(str(packet.get("route_key") or ""), {})
        reject_prob = float(risk.get("predicted_reject_probability") or 0.0)
        categories = packet.get("categories") if isinstance(packet.get("categories"), dict) else {}
        final_tests = [name for name, value in categories.items() if value in {"watch", "missing", "needs_more_evidence"}]
        item = {
            "variant": packet.get("variant"),
            "route_key": packet.get("route_key"),
            "evidence_score": packet.get("evidence_score"),
            "predicted_reject_probability": reject_prob,
            "final_tests": final_tests[:5],
            "status": "review_ready" if float(packet.get("evidence_score") or 0.0) >= 68.0 and reject_prob < 0.35 and not final_tests else "near_ready",
        }
        if item["status"] == "review_ready":
            queue.append(item)
        elif float(packet.get("evidence_score") or 0.0) >= 45.0:
            near.append(item)
    queue.sort(key=lambda row: float(row.get("evidence_score") or 0.0), reverse=True)
    near.sort(key=lambda row: float(row.get("evidence_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Maintains candidates that are ready or nearly ready for promotion review with final required tests.",
        "queue": queue[:20],
        "near_ready": near[:20],
        "top_ready": (queue or near or [{}])[0],
        "focus_routes": _ordered_unique([row.get("route_key") for row in (queue + near)])[:12],
    }


def promotion_evidence_scorecard(state: dict[str, Any] | None = None) -> dict[str, Any]:
    evidence = state.get("live_candidate_evidence_builder") if isinstance(state, dict) and isinstance(state.get("live_candidate_evidence_builder"), dict) else {}
    scorecards = []
    weights = {"live_edge": 25.0, "readiness": 25.0, "controlled_lift": 25.0, "power": 15.0, "lineage": 10.0}
    value_score = {"pass": 1.0, "review_ready": 1.0, "present": 1.0, "watch": 0.55, "needs_more_evidence": 0.45, "missing": 0.0, "fail": 0.0}
    for packet in evidence.get("packets") or []:
        if not isinstance(packet, dict):
            continue
        categories = packet.get("categories") if isinstance(packet.get("categories"), dict) else {}
        category_scores = {
            name: round(weights.get(name, 10.0) * value_score.get(str(value), 0.35), 2)
            for name, value in categories.items()
        }
        total = round(sum(category_scores.values()), 2)
        scorecards.append({
            "variant": packet.get("variant"),
            "route_key": packet.get("route_key"),
            "category_scores": category_scores,
            "total_score": total,
            "grade": "A" if total >= 82 else "B" if total >= 68 else "C" if total >= 50 else "D",
            "summary": f"live={categories.get('live_edge')}, readiness={categories.get('readiness')}, control={categories.get('controlled_lift')}, power={categories.get('power')}",
        })
    scorecards.sort(key=lambda row: float(row.get("total_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Human-readable promotion scorecards by evidence category.",
        "scorecards": scorecards,
        "top_scorecard": scorecards[0] if scorecards else {},
    }


def candidate_lineage_explainer_v2(state: dict[str, Any] | None = None) -> dict[str, Any]:
    rows = _temporal_candidate_rows(state, limit=60)
    explanations = []
    for row in rows[:30]:
        route = _row_route(row)
        parent = row.get("parent_variant") or row.get("source_variant") or "current_live"
        changes = []
        for field in ("indicator", "entry_signal", "exit_signal", "score_model", "timeframe", "mutation_lane"):
            if row.get(field):
                changes.append(f"{field}={row.get(field)}")
        delta = float(row.get("step2_delta_vs_active") or 0.0)
        explanations.append({
            "variant": row.get("variant"),
            "route_key": route,
            "parent_variant": parent,
            "changes": changes[:8],
            "why_it_helped": "controlled/live delta improved" if delta > 0 else "no proven lift yet",
            "lineage_confidence": round(min(0.95, 0.35 + len(changes) * 0.08 + max(0.0, delta) / 250.0), 4),
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Explains what changed from parent/live/current and why it likely helped.",
        "explanations": explanations,
        "top_explanation": explanations[0] if explanations else {},
    }


def live_vs_control_differential_report(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    monitor = state.get("sequential_test_monitor") if isinstance(state.get("sequential_test_monitor"), dict) else {}
    effects = state.get("causal_effect_size_ledger") if isinstance(state.get("causal_effect_size_ledger"), dict) else {}
    effect_by_contract = {str(row.get("contract_id") or ""): row for row in effects.get("effects") or [] if isinstance(row, dict)}
    reports = []
    for decision in monitor.get("decisions") or []:
        if not isinstance(decision, dict):
            continue
        effect = effect_by_contract.get(str(decision.get("contract_id") or ""), {})
        reports.append({
            "contract_id": decision.get("contract_id"),
            "route_key": decision.get("route_key"),
            "control_route": decision.get("control_route"),
            "treatment_delta": decision.get("treatment_delta"),
            "control_delta": decision.get("control_delta"),
            "differential": decision.get("estimated_lift"),
            "confidence": effect.get("confidence"),
            "verdict": "supports_promotion_evidence" if float(decision.get("estimated_lift") or 0.0) > 0 else "does_not_support_yet",
        })
    reports.sort(key=lambda row: float(row.get("differential") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Connects scientific contracts directly to promotion evidence using live-vs-control differentials.",
        "reports": reports,
        "top_report": reports[0] if reports else {},
        "focus_routes": _ordered_unique([row.get("route_key") for row in reports if row.get("verdict") == "supports_promotion_evidence"])[:12],
    }


def promotion_packet_executive(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = state or {}
    evidence = state.get("live_candidate_evidence_builder") if isinstance(state.get("live_candidate_evidence_builder"), dict) else {}
    failure = state.get("promotion_failure_predictor_v3") if isinstance(state.get("promotion_failure_predictor_v3"), dict) else {}
    gaps = state.get("evidence_gap_router") if isinstance(state.get("evidence_gap_router"), dict) else {}
    queue = state.get("review_ready_queue_v2") if isinstance(state.get("review_ready_queue_v2"), dict) else {}
    scorecards = state.get("promotion_evidence_scorecard") if isinstance(state.get("promotion_evidence_scorecard"), dict) else {}
    diff = state.get("live_vs_control_differential_report") if isinstance(state.get("live_vs_control_differential_report"), dict) else {}
    risk_by_route = {str(row.get("route_key") or ""): row for row in failure.get("predictions") or [] if isinstance(row, dict)}
    score_by_route = {str(row.get("route_key") or ""): row for row in scorecards.get("scorecards") or [] if isinstance(row, dict)}
    diff_routes = set(str(row.get("route_key") or "") for row in diff.get("reports") or [] if isinstance(row, dict) and row.get("verdict") == "supports_promotion_evidence")
    ready_routes = set(str(row.get("route_key") or "") for row in queue.get("queue") or [] if isinstance(row, dict))
    decisions = []
    commands = []
    for idx, packet in enumerate(evidence.get("packets") or []):
        if not isinstance(packet, dict):
            continue
        route = str(packet.get("route_key") or "")
        risk = risk_by_route.get(route, {})
        scorecard = score_by_route.get(route, {})
        reject_prob = float(risk.get("predicted_reject_probability") or 0.0)
        total_score = float(scorecard.get("total_score") or packet.get("evidence_score") or 0.0)
        if route in ready_routes or (total_score >= 78.0 and reject_prob < 0.30 and route in diff_routes):
            decision = "review"
        elif reject_prob >= 0.70:
            decision = "reject"
        elif total_score >= 50.0:
            decision = "repair"
        else:
            decision = "shadow"
        decisions.append({
            "variant": packet.get("variant"),
            "route_key": route,
            "decision": decision,
            "score": round(total_score, 2),
            "predicted_reject_probability": reject_prob,
            "reason": "promotion packet evidence and control differential",
        })
        commands.append({
            "command_id": _stable_hash({"promotion_packet_command": route, "decision": decision, "idx": idx}),
            "worker": f"worker_{(idx % 4) + 1}",
            "action": f"promotion_packet_{decision}",
            "route_key": route,
            "variant": packet.get("variant"),
            "mutation_width": "tight" if decision in {"repair", "review"} else "medium",
            "reason": decision,
        })
    focus = _ordered_unique([row.get("route_key") for row in decisions if row.get("decision") in {"review", "repair"}])[:12]
    avoid = _ordered_unique([row.get("route_key") for row in decisions if row.get("decision") == "reject"])[:12]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Decides whether each promising candidate should be repaired, reviewed, shadowed, or rejected.",
        "decisions": decisions,
        "top_decision": decisions[0] if decisions else {},
        "commands": commands[:20],
        "focus_routes": focus,
        "avoid_routes": avoid,
        "gap_task_count": len(gaps.get("tasks") or []),
        "summary": {
            "review": sum(1 for row in decisions if row.get("decision") == "review"),
            "repair": sum(1 for row in decisions if row.get("decision") == "repair"),
            "shadow": sum(1 for row in decisions if row.get("decision") == "shadow"),
            "reject": sum(1 for row in decisions if row.get("decision") == "reject"),
        },
    }


def promotion_grade_evidence_suite(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    evidence = live_candidate_evidence_builder(state)
    failure = promotion_failure_predictor_v3({**state, "live_candidate_evidence_builder": evidence})
    gaps = evidence_gap_router({
        **state,
        "live_candidate_evidence_builder": evidence,
        "promotion_failure_predictor_v3": failure,
    })
    queue = review_ready_queue_v2({
        **state,
        "live_candidate_evidence_builder": evidence,
        "promotion_failure_predictor_v3": failure,
    })
    scorecard = promotion_evidence_scorecard({**state, "live_candidate_evidence_builder": evidence})
    lineage = candidate_lineage_explainer_v2(state)
    differential = live_vs_control_differential_report(state)
    executive = promotion_packet_executive({
        **state,
        "live_candidate_evidence_builder": evidence,
        "promotion_failure_predictor_v3": failure,
        "evidence_gap_router": gaps,
        "review_ready_queue_v2": queue,
        "promotion_evidence_scorecard": scorecard,
        "candidate_lineage_explainer_v2": lineage,
        "live_vs_control_differential_report": differential,
    })
    return {
        "live_candidate_evidence_builder": evidence,
        "promotion_failure_predictor_v3": failure,
        "evidence_gap_router": gaps,
        "review_ready_queue_v2": queue,
        "promotion_evidence_scorecard": scorecard,
        "candidate_lineage_explainer_v2": lineage,
        "live_vs_control_differential_report": differential,
        "promotion_packet_executive": executive,
    }


WORLD_CLASS_META_LEARNING_ARTIFACTS = (
    "unified_learning_state_reducer",
    "artifact_priority_arbitration_engine",
    "evidence_provenance_graph",
    "counterfactual_promotion_replay",
    "candidate_survival_simulator",
    "live_regime_shift_detector_v2",
    "adaptive_trust_weights_per_module",
    "worker_skill_elo_v2",
    "route_genome_knowledge_graph",
    "causal_feature_interaction_miner",
    "overfit_signature_library",
    "review_rejection_memory_bank_v2",
    "learning_budget_optimizer",
    "search_novelty_floor",
    "breakthrough_escalation_protocol",
    "false_discovery_backpressure_controller",
    "multi_armed_strategy_portfolio",
    "historical_lesson_ab_harness",
    "promotion_packet_diff_engine",
    "candidate_repair_recipe_generator",
    "automated_red_team_reviewer",
    "learning_compression_field_manual",
    "hunt_outcome_attribution_v2",
    "world_state_dashboard_artifact",
    "meta_learning_governor",
)


ELITE_LEARNING_ARTIFACTS = (
    "cross_run_candidate_resurrection_scoring",
    "live_edge_decay_curves_by_route",
    "promotion_failure_causal_backtrace",
    "route_crowding_detector",
    "worker_disagreement_logging",
    "automatic_win_explainer",
    "automatic_loss_explainer",
    "variant_mutation_ancestry_scoring",
    "parent_child_edge_retention_model",
    "live_review_mismatch_detector",
    "candidate_fragility_heatmap",
    "robustness_repair_prioritizer",
    "indicator_family_saturation_meter",
    "ticker_concentration_budgeter",
    "time_of_day_route_memory",
    "market_regime_conditioned_mutation_widths",
    "promotion_readiness_lift_estimator",
    "almost_promoted_near_miss_league",
    "false_confidence_detector",
    "lesson_contradiction_detector_v3",
    "route_hypothesis_decay_scheduler",
    "dead_route_resurrection_calendar",
    "novelty_usefulness_scorer",
    "search_entropy_monitor",
    "exploit_overfit_warning_system",
    "discovery_overload_warning_system",
    "live_beater_quality_percentile_model",
    "candidate_control_similarity_scorer",
    "promotion_packet_completeness_gate",
    "adaptive_holdout_demand_model",
    "evidence_debt_ledger_v2",
    "candidate_repair_outcome_memory",
    "wild_shuffle_safety_governor",
    "strategy_drift_fingerprinting",
    "route_family_tournament_brackets",
    "worker_role_switching_policy",
    "hunt_cycle_objective_selector",
    "stop_wasting_cycles_detector",
    "route_interaction_veto_rules",
    "meta_learning_ablation_scheduler",
    "historical_lesson_replay_simulator",
    "synthetic_challenger_generator",
    "candidate_ensemble_builder",
    "live_leaderboard_stability_tracker",
    "shock_event_memory_tagging",
    "promotion_review_rehearsal_mode",
    "human_readable_hunt_diary_compiler",
    "top100_memory_compression_by_theme",
    "self_auditing_learning_scorecard",
    "world_state_next_best_question_engine",
)


PROOF_LEARNING_ARTIFACTS = (
    "learning_truth_ledger",
    "causal_credit_assignment_engine",
    "live_hunt_simulator",
    "experiment_promotion_ladder",
    "persistent_cross_hunt_memory_warehouse",
    "learning_roi_accounting",
    "adaptive_research_agenda",
    "self_falsification_mode",
    "counterfactual_budget_replay",
    "candidate_lifecycle_state_machine",
    "regime_aware_lesson_validity",
    "world_model_dashboard",
    "autonomous_hunt_review_board",
    "memory_garbage_collector",
    "proof_carrying_promotion_packets",
)


CLOSED_LOOP_CONTROL_ARTIFACTS = (
    "bayesian_belief_engine",
    "active_experiment_selector",
    "uncertainty_aware_top100",
    "live_search_drift_detector",
    "exploration_exploitation_governor",
    "variant_lineage_genetics",
    "feature_interaction_miner_v2",
    "regret_accounting_engine",
    "adversarial_robustness_generator",
    "hunt_curriculum_engine",
    "meta_strategy_bandits",
    "real_time_learning_dashboard_feed",
)


WORLD_MODEL_NERVOUS_SYSTEM_ARTIFACTS = (
    "hypothesis_dependency_graph",
    "belief_contradiction_resolver",
    "evidence_freshness_scorer",
    "route_level_confidence_intervals",
    "candidate_luck_adjusted_ranking",
    "live_beater_persistence_tracker",
    "statistical_power_gate",
    "minimum_evidence_calculator",
    "adaptive_holdout_allocator",
    "false_positive_tax_model",
    "trait_inheritance_scoreboard",
    "parent_child_lift_attribution",
    "mutation_distance_optimizer",
    "gene_recombination_planner",
    "dead_gene_suppression_engine",
    "rare_gene_exploration_budget",
    "family_overcrowding_detector",
    "lineage_diversity_governor",
    "mutation_genealogy_replay",
    "candidate_clone_detector_v2",
    "worker_strategy_specialization",
    "worker_disagreement_arbitration",
    "worker_boredom_staleness_detector",
    "worker_exploration_quota",
    "worker_exploit_quota",
    "worker_role_tournament",
    "worker_error_memory",
    "worker_route_fit_model",
    "worker_risk_appetite_scheduler",
    "worker_promotion_readiness_focus_mode",
    "multi_armed_route_portfolio",
    "route_retirement_court",
    "route_resurrection_market",
    "route_volatility_memory",
    "route_time_decay_model",
    "route_overfit_signature_tracker",
    "route_stress_test_ladder",
    "route_novelty_saturation_monitor",
    "route_interaction_veto_engine",
    "route_promotion_survival_simulator",
    "indicator_family_causal_map",
    "indicator_pair_lift_miner",
    "indicator_redundancy_detector",
    "indicator_regime_fit_scorer",
    "indicator_shuffle_generator",
    "indicator_fragility_profiler",
    "indicator_decay_monitor",
    "indicator_conflict_graph",
    "indicator_substitution_recommender",
    "indicator_ensemble_builder",
    "regime_fingerprint_memory",
    "regime_specific_top100_ranking",
    "regime_drift_early_warning",
    "regime_replay_harness",
    "regime_transferability_scorer",
    "regime_mismatch_firewall",
    "regime_aware_mutation_width",
    "regime_specific_promotion_threshold",
    "regime_lesson_half_life",
    "regime_opening_playbook",
    "promotion_rejection_reason_backprop",
    "promotion_packet_completeness_optimizer",
    "promotion_shadow_review_scheduler",
    "promotion_failure_simulator",
    "promotion_evidence_gap_router_v2",
    "promotion_ready_queue_optimizer",
    "promotion_confidence_decomposer",
    "promotion_risk_heatmap",
    "promotion_reviewer_ensemble",
    "promotion_survival_memory_bank",
    "hunt_objective_optimizer",
    "search_entropy_governor",
    "breakthrough_detector_v2",
    "exploration_debt_accounting",
    "opportunity_cost_simulator",
    "scientific_ablation_runner",
    "counterfactual_hunt_replay_v2",
    "live_learning_executive_summary",
    "autonomous_research_agenda_v2",
    "world_model_self_audit_loop",
)


ORCHESTRATION_LEARNING_ARTIFACTS = (
    "unified_artifact_dependency_resolver",
    "artifact_conflict_priority_court",
    "artifact_trust_score_optimizer",
    "artifact_duplication_detector",
    "artifact_cost_benefit_ledger",
    "artifact_aging_and_retirement_policy",
    "artifact_schema_migration_validator",
    "artifact_output_contract_tester",
    "artifact_runtime_contribution_tracker",
    "artifact_governance_dashboard",
    "belief_graph_versioning",
    "belief_provenance_tracer",
    "belief_confidence_calibration_curve",
    "belief_contradiction_heatmap",
    "belief_overconfidence_penalty",
    "belief_uncertainty_debt_queue",
    "belief_expiry_scheduler",
    "belief_replay_validator",
    "belief_to_action_explainability_map",
    "belief_promotion_impact_tracker",
    "experiment_queue_optimizer",
    "experiment_interference_detector",
    "experiment_dependency_scheduler",
    "experiment_expected_value_frontier",
    "experiment_sequential_stopping_v2",
    "experiment_shadow_control_allocator",
    "experiment_sample_size_auto_planner",
    "experiment_result_reproducibility_scorer",
    "experiment_contamination_replay",
    "experiment_promotion_readiness_bridge",
    "worker_learning_rate_model",
    "worker_fatigue_novelty_balancer",
    "worker_specialization_drift_detector",
    "worker_task_fit_recommender_v2",
    "worker_result_quality_auditor",
    "worker_debate_allocator",
    "worker_adversarial_assignment_rotator",
    "worker_throughput_quality_frontier",
    "worker_promotion_packet_closer",
    "worker_autonomy_guardrail",
    "search_space_topology_mapper",
    "search_basin_depth_estimator",
    "search_local_optimum_escape_detector",
    "search_novelty_exhaustion_forecaster",
    "search_mutation_radius_controller_v2",
    "search_grammar_coverage_auditor",
    "search_route_pair_exploration_planner",
    "search_wild_shuffle_governor_v2",
    "search_exploitation_saturation_gauge",
    "search_unexplored_cell_bounty_market",
    "promotion_proof_burden_allocator",
    "promotion_failure_taxonomy_learner",
    "promotion_reviewer_disagreement_resolver",
    "promotion_causal_evidence_builder",
    "promotion_minimum_viable_proof_gate",
    "promotion_anti_overfit_checklist_generator",
    "promotion_robustness_replay_scheduler",
    "promotion_economic_value_decomposer",
    "promotion_review_dry_run_tournament",
    "promotion_live_readiness_governor",
    "memory_entropy_monitor",
    "memory_contradiction_replay",
    "memory_lesson_clustering_engine",
    "memory_stale_prior_quarantiner",
    "memory_regime_transfer_validator",
    "memory_negative_result_compressor",
    "memory_high_value_lesson_pinboard",
    "memory_retrieval_quality_benchmark",
    "memory_catastrophic_forgetting_guard",
    "memory_cross_run_research_agenda_compiler",
    "hunt_executive_controller",
    "hunt_cycle_objective_negotiator",
    "hunt_interrupt_reason_classifier",
    "hunt_pivot_quality_scorer",
    "hunt_budget_auctioneer",
    "hunt_post_action_truth_updater",
    "hunt_online_ablation_switchboard",
    "hunt_scientific_method_enforcer",
    "hunt_live_status_narrative_critic",
    "hunt_self_improvement_backlog_generator",
)


FITNESS_SELECTION_ARTIFACTS = (
    "learning_layer_ablation_harness",
    "artifact_fitness_scorecard",
    "layer_conflict_matrix",
    "artifact_duplicate_clusterer",
    "artifact_runtime_roi_ranker",
    "learning_layer_trust_policy",
    "artifact_demote_retire_queue",
    "world_model_fitness_report",
)


def _world_candidate_packets(state: dict[str, Any]) -> list[dict[str, Any]]:
    evidence = state.get("live_candidate_evidence_builder") if isinstance(state.get("live_candidate_evidence_builder"), dict) else {}
    packets = [dict(row) for row in (evidence.get("packets") or []) if isinstance(row, dict)]
    if packets:
        for packet in packets:
            packet["delta_vs_live"] = float(packet.get("delta_vs_live") or packet.get("step2_delta_vs_active") or packet.get("delta_vs_active") or 0.0)
            packet["gene_tokens"] = packet.get("gene_tokens") or _row_gene_tokens(packet)
            packet["novelty_score"] = float(packet.get("novelty_score") or 0.0)
            packet["overfit_risk_score"] = float(packet.get("overfit_risk_score") or 0.0)
        return packets[:80]
    packets = []
    for row in _temporal_candidate_rows(state, limit=80):
        route = _row_route(row)
        packets.append({
            "variant": row.get("variant"),
            "route_key": route,
            "step2_pnl": hunt_intel.pnl(row),
            "delta_vs_live": hunt_intel.live_delta(row),
            "evidence_score": float(row.get("promotion_quality_score") or 0.0) + max(0.0, hunt_intel.live_delta(row)) / 250.0,
            "novelty_score": float(row.get("novelty_score") or 0.0),
            "overfit_risk_score": float(row.get("overfit_risk_score") or 0.0),
            "gene_tokens": _row_gene_tokens(row),
        })
    return packets


def _world_routes_from_packets(packets: list[dict[str, Any]], *, min_score: float = 0.0, limit: int = 12) -> list[str]:
    routes = []
    for packet in sorted(packets, key=lambda row: float(row.get("evidence_score") or row.get("delta_vs_live") or 0.0), reverse=True):
        if float(packet.get("evidence_score") or 0.0) < min_score and float(packet.get("delta_vs_live") or 0.0) <= 0.0:
            continue
        routes.append(str(packet.get("route_key") or ""))
    return _ordered_unique(routes)[:limit]


def unified_learning_state_reducer(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    packets = _world_candidate_packets(state)
    focus = _world_routes_from_packets(packets, limit=10)
    avoid = _ordered_unique(
        list((state.get("promotion_failure_predictor_v3") or {}).get("avoid_routes") or [])
        + list((state.get("false_positive_pressure_gauge") or {}).get("avoid_routes") or [])
    )[:12]
    beliefs = []
    for idx, packet in enumerate(packets[:12], 1):
        route = str(packet.get("route_key") or "")
        score = float(packet.get("evidence_score") or 0.0)
        risk = float(packet.get("predicted_reject_probability") or packet.get("overfit_risk_score") or 0.0)
        beliefs.append({
            "belief_id": _stable_hash({"unified_belief": route, "variant": packet.get("variant")}),
            "rank": idx,
            "route_key": route,
            "variant": packet.get("variant"),
            "belief": "promotion_candidate" if score >= 60.0 else "learning_probe",
            "confidence": round(max(0.05, min(0.98, score / 100.0 * (1.0 - min(0.85, risk / 100.0)))), 4),
            "evidence_score": round(score, 2),
            "risk_score": round(risk, 2),
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Reduces all active learning evidence into the current route/candidate belief state.",
        "beliefs": beliefs,
        "top_belief": beliefs[0] if beliefs else {},
        "focus_routes": [route for route in focus if route not in set(avoid)],
        "avoid_routes": avoid,
        "summary": {
            "belief_count": len(beliefs),
            "focus_count": len(focus),
            "avoid_count": len(avoid),
            "top_route": focus[0] if focus else "",
        },
    }


def artifact_priority_arbitration_engine(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    modules = [
        ("promotion_packet_executive", state.get("promotion_packet_executive") or {}, 1.20),
        ("scientific_hunt_executive", state.get("scientific_hunt_executive") or {}, 1.12),
        ("unified_learning_state_reducer", state.get("unified_learning_state_reducer") or {}, 1.10),
        ("expected_information_gain_scorer_v2", state.get("expected_information_gain_scorer_v2") or {}, 1.00),
        ("learning_value_stop_loss", state.get("learning_value_stop_loss") or {}, 0.95),
        ("false_positive_pressure_gauge", state.get("false_positive_pressure_gauge") or {}, 0.92),
        ("live_candidate_evidence_builder", state.get("live_candidate_evidence_builder") or {}, 1.05),
    ]
    priorities = []
    for name, artifact, base in modules:
        if not isinstance(artifact, dict) or not artifact:
            continue
        focus_count = len(artifact.get("focus_routes") or artifact.get("repair_routes") or [])
        avoid_count = len(artifact.get("avoid_routes") or artifact.get("pause_routes") or [])
        command_count = len(artifact.get("commands") or artifact.get("tasks") or [])
        score = base * 50.0 + focus_count * 8.0 + command_count * 5.0 - avoid_count * 3.0
        priorities.append({
            "artifact": name,
            "priority_score": round(score, 2),
            "focus_routes": _ordered_unique(list(artifact.get("focus_routes") or artifact.get("repair_routes") or []))[:6],
            "avoid_routes": _ordered_unique(list(artifact.get("avoid_routes") or artifact.get("pause_routes") or []))[:6],
            "reason": "high_signal_runtime_artifact",
        })
    priorities.sort(key=lambda row: float(row.get("priority_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Arbitrates which learning artifacts should steer the next hunt window first.",
        "priorities": priorities,
        "top_artifact": priorities[0] if priorities else {},
        "focus_routes": _ordered_unique([route for row in priorities[:4] for route in (row.get("focus_routes") or [])])[:12],
        "avoid_routes": _ordered_unique([route for row in priorities[:4] for route in (row.get("avoid_routes") or [])])[:12],
    }


def evidence_provenance_graph(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    packets = _world_candidate_packets(state)
    nodes = []
    edges = []
    for packet in packets[:18]:
        route = str(packet.get("route_key") or "")
        variant = str(packet.get("variant") or "")
        if route:
            nodes.append({"id": f"route:{route}", "type": "route", "label": route})
        if variant:
            nodes.append({"id": f"variant:{variant}", "type": "variant", "label": variant})
            edges.append({"from": f"variant:{variant}", "to": f"route:{route}", "kind": "belongs_to"})
        for gene in (packet.get("gene_tokens") or [])[:5]:
            nodes.append({"id": f"gene:{gene}", "type": "gene", "label": gene})
            edges.append({"from": f"route:{route}", "to": f"gene:{gene}", "kind": "uses_gene"})
    for artifact_name in ("promotion_packet_executive", "scientific_hunt_executive", "evidence_gap_router"):
        artifact = state.get(artifact_name) if isinstance(state.get(artifact_name), dict) else {}
        if not artifact:
            continue
        nodes.append({"id": f"artifact:{artifact_name}", "type": "artifact", "label": artifact_name})
        for route in _ordered_unique(list(artifact.get("focus_routes") or artifact.get("avoid_routes") or []))[:8]:
            edges.append({"from": f"artifact:{artifact_name}", "to": f"route:{route}", "kind": "supports_or_warns"})
    node_map = {node["id"]: node for node in nodes if node.get("id")}
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Connects candidate, route, gene, and artifact evidence so steering decisions have provenance.",
        "nodes": list(node_map.values())[:80],
        "edges": edges[:120],
        "node_count": len(node_map),
        "edge_count": len(edges),
        "top_node": (list(node_map.values()) or [{}])[0],
    }


def counterfactual_promotion_replay(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    packets = _world_candidate_packets(state)
    failures = state.get("promotion_failure_predictor_v3") if isinstance(state.get("promotion_failure_predictor_v3"), dict) else {}
    risk_by_route = {str(row.get("route_key") or ""): row for row in (failures.get("risks") or []) if isinstance(row, dict)}
    replays = []
    for packet in packets[:16]:
        route = str(packet.get("route_key") or "")
        risk = risk_by_route.get(route, {})
        reject_prob = float(risk.get("predicted_reject_probability") or packet.get("predicted_reject_probability") or 0.0)
        score = float(packet.get("evidence_score") or 0.0)
        missing = list(risk.get("top_failure_reasons") or packet.get("missing_evidence") or [])
        replays.append({
            "replay_id": _stable_hash({"counterfactual_promotion_replay": route, "variant": packet.get("variant")}),
            "route_key": route,
            "variant": packet.get("variant"),
            "decision_if_reviewed_now": "likely_reject" if reject_prob >= 0.55 else "likely_pass" if score >= 75.0 else "needs_more_evidence",
            "predicted_reject_probability": round(reject_prob, 4),
            "missing_or_weak_evidence": missing[:4],
            "counterfactual_repair": "tight robustness/day consistency replay" if reject_prob >= 0.55 else "promotion packet completion",
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Replays what would probably happen if candidates were sent through promotion review now.",
        "replays": replays,
        "top_replay": replays[0] if replays else {},
        "repair_routes": _ordered_unique([row["route_key"] for row in replays if row.get("decision_if_reviewed_now") != "likely_pass"])[:12],
        "avoid_routes": _ordered_unique([row["route_key"] for row in replays if row.get("decision_if_reviewed_now") == "likely_reject"])[:12],
    }


def candidate_survival_simulator(state: dict[str, Any] | None = None) -> dict[str, Any]:
    packets = _world_candidate_packets(dict(state or {}))
    simulations = []
    for packet in packets[:20]:
        score = float(packet.get("evidence_score") or 0.0)
        delta = max(0.0, float(packet.get("delta_vs_live") or 0.0))
        novelty = float(packet.get("novelty_score") or 0.0)
        risk = float(packet.get("predicted_reject_probability") or packet.get("overfit_risk_score") or 0.0)
        survival = max(0.02, min(0.95, 0.18 + score / 150.0 + min(delta, 2000.0) / 6500.0 + novelty / 400.0 - risk / 140.0))
        simulations.append({
            "route_key": packet.get("route_key"),
            "variant": packet.get("variant"),
            "survival_probability": round(survival, 4),
            "expected_promotable_edge": round(delta * survival, 2),
            "recommended_action": "review" if survival >= 0.70 else "repair" if survival >= 0.42 else "shadow",
        })
    simulations.sort(key=lambda row: float(row.get("expected_promotable_edge") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Simulates which live-beating candidates are most likely to survive promotion review.",
        "simulations": simulations,
        "top_simulation": simulations[0] if simulations else {},
        "focus_routes": _ordered_unique([row["route_key"] for row in simulations if row.get("recommended_action") in {"review", "repair"}])[:12],
        "avoid_routes": _ordered_unique([row["route_key"] for row in simulations if row.get("recommended_action") == "shadow"])[:12],
    }


def live_regime_shift_detector_v2(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    current = set(_world_routes_from_packets(_world_candidate_packets(state), limit=20))
    historical = set((state.get("multi_horizon_memory_stack") or {}).get("focus_routes") or [])
    temporal = state.get("temporal_regime_fingerprinting") if isinstance(state.get("temporal_regime_fingerprinting"), dict) else {}
    shift = 0.0 if not current and not historical else 1.0 - (len(current & historical) / max(1, len(current | historical)))
    if temporal.get("shift_score") is not None:
        shift = max(shift, float(temporal.get("shift_score") or 0.0))
    mode = "new_regime_explore" if shift >= 0.65 else "mixed_regime" if shift >= 0.35 else "stable_regime"
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Detects whether current live beaters imply a shift away from learned historical priors.",
        "shift_score": round(shift, 4),
        "mode": mode,
        "focus_routes": sorted(current - historical)[:12],
        "discount_routes": sorted(historical - current)[:12],
        "summary": {"current_routes": len(current), "historical_routes": len(historical), "mode": mode},
    }


def adaptive_trust_weights_per_module(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    artifact_priority = state.get("artifact_priority_arbitration_engine") if isinstance(state.get("artifact_priority_arbitration_engine"), dict) else {}
    weights = []
    for row in artifact_priority.get("priorities") or []:
        if not isinstance(row, dict):
            continue
        score = float(row.get("priority_score") or 0.0)
        weights.append({
            "module": row.get("artifact"),
            "trust_weight": round(max(0.15, min(1.75, score / 70.0)), 4),
            "reason": "priority_arbitration_score",
            "focus_routes": row.get("focus_routes") or [],
        })
    if not weights:
        weights = [{"module": "promotion_packet_executive", "trust_weight": 1.0, "reason": "default_promotion_first", "focus_routes": []}]
    weights.sort(key=lambda row: float(row.get("trust_weight") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Assigns adaptive trust weights to learning modules based on current usefulness.",
        "weights": weights,
        "top_module": weights[0] if weights else {},
        "focus_routes": _ordered_unique([route for row in weights[:4] for route in (row.get("focus_routes") or [])])[:12],
    }


def worker_skill_elo_v2(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    cards = (state.get("worker_learning_report_cards") or {}).get("cards") if isinstance(state.get("worker_learning_report_cards"), dict) else []
    workers: dict[str, dict[str, Any]] = {}
    for idx, card in enumerate(cards or [], 1):
        if not isinstance(card, dict):
            continue
        worker = str(card.get("worker") or f"worker_{idx}")
        wins = int(card.get("wins") or card.get("successful_commands") or 0)
        losses = int(card.get("losses") or card.get("failed_commands") or 0)
        workers[worker] = {
            "worker": worker,
            "elo": round(1000.0 + wins * 22.0 - losses * 16.0 + idx, 2),
            "best_role": card.get("recommended_role") or card.get("role") or "adaptive_probe",
            "sample_size": wins + losses,
        }
    for idx in range(1, 5):
        workers.setdefault(f"worker_{idx}", {"worker": f"worker_{idx}", "elo": 1000.0, "best_role": "adaptive_probe", "sample_size": 0})
    leaderboard = sorted(workers.values(), key=lambda row: float(row.get("elo") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Tracks worker skill by role so the hunt can route harder probes to better-fit workers.",
        "leaderboard": leaderboard,
        "top_worker": leaderboard[0] if leaderboard else {},
    }


def route_genome_knowledge_graph(state: dict[str, Any] | None = None) -> dict[str, Any]:
    packets = _world_candidate_packets(dict(state or {}))
    route_genes: dict[str, set[str]] = {}
    for packet in packets[:40]:
        route = str(packet.get("route_key") or "")
        if route:
            route_genes.setdefault(route, set()).update(str(gene) for gene in (packet.get("gene_tokens") or [])[:8])
    nodes = [{"id": f"route:{route}", "type": "route", "label": route, "gene_count": len(genes)} for route, genes in route_genes.items()]
    edges = [{"from": f"route:{route}", "to": f"gene:{gene}", "kind": "has_gene"} for route, genes in route_genes.items() for gene in sorted(genes)]
    top = max(nodes, key=lambda node: int(node.get("gene_count") or 0), default={})
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Builds a lightweight route/genome graph for discovering reusable scoring and indicator DNA.",
        "nodes": nodes[:80],
        "edges": edges[:160],
        "summary": {"route_count": len(route_genes), "edge_count": len(edges)},
        "top_node": top,
        "focus_routes": [str(top.get("label"))] if top.get("label") else [],
    }


def causal_feature_interaction_miner(state: dict[str, Any] | None = None) -> dict[str, Any]:
    packets = _world_candidate_packets(dict(state or {}))
    pairs: dict[tuple[str, str], dict[str, Any]] = {}
    for packet in packets[:60]:
        genes = _ordered_unique(list(packet.get("gene_tokens") or []))[:8]
        score = float(packet.get("evidence_score") or packet.get("delta_vs_live") or 0.0)
        route = str(packet.get("route_key") or "")
        for i, left in enumerate(genes):
            for right in genes[i + 1:]:
                key = tuple(sorted((left, right)))
                row = pairs.setdefault(key, {"genes": list(key), "score": 0.0, "routes": []})
                row["score"] = float(row.get("score") or 0.0) + max(0.0, score)
                row["routes"].append(route)
    interactions = []
    for row in pairs.values():
        routes = _ordered_unique(row.get("routes") or [])
        interactions.append({
            "interaction_id": _stable_hash({"interaction": row.get("genes")}),
            "genes": row.get("genes"),
            "interaction_score": round(float(row.get("score") or 0.0) / max(1, len(routes)), 2),
            "routes": routes[:6],
            "recommended_action": "shuffle_pair" if len(routes) >= 2 else "probe_pair",
        })
    interactions.sort(key=lambda row: float(row.get("interaction_score") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Mines feature/gene combinations that repeatedly appear in live-beating candidates.",
        "interactions": interactions[:30],
        "top_interaction": interactions[0] if interactions else {},
        "focus_routes": _ordered_unique([route for row in interactions[:5] for route in (row.get("routes") or [])])[:12],
    }


def overfit_signature_library(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    failures = state.get("promotion_failure_predictor_v3") if isinstance(state.get("promotion_failure_predictor_v3"), dict) else {}
    signatures: dict[str, dict[str, Any]] = {}
    for row in failures.get("risks") or []:
        if not isinstance(row, dict):
            continue
        route = str(row.get("route_key") or "")
        reasons = list(row.get("top_failure_reasons") or row.get("reasons") or ["promotion_risk"])
        for reason in reasons:
            key = str(reason)
            sig = signatures.setdefault(key, {"signature": key, "count": 0, "routes": [], "severity": 0.0})
            sig["count"] += 1
            sig["routes"].append(route)
            sig["severity"] = max(float(sig.get("severity") or 0.0), float(row.get("predicted_reject_probability") or 0.0))
    rows = sorted(signatures.values(), key=lambda row: (float(row.get("severity") or 0.0), int(row.get("count") or 0)), reverse=True)
    for row in rows:
        row["routes"] = _ordered_unique(row.get("routes") or [])[:8]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Stores reusable overfit and promotion-failure signatures seen during hunts.",
        "signatures": rows[:30],
        "top_signature": rows[0] if rows else {},
        "avoid_routes": _ordered_unique([route for row in rows[:5] for route in (row.get("routes") or [])])[:12],
    }


def review_rejection_memory_bank_v2(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    feedback = [row for row in (state.get("promotion_review_feedback") or []) if isinstance(row, dict)]
    failures = (state.get("promotion_failure_predictor_v3") or {}).get("risks") if isinstance(state.get("promotion_failure_predictor_v3"), dict) else []
    memories = []
    for row in list(feedback) + list(failures or []):
        route = str(row.get("route_key") or "")
        reasons = row.get("reject_reasons") or row.get("top_failure_reasons") or row.get("reasons") or []
        if not route and not reasons:
            continue
        memories.append({
            "memory_id": _stable_hash({"review_rejection_memory_v2": route, "reasons": reasons}),
            "route_key": route,
            "variant": row.get("variant"),
            "reasons": list(reasons)[:5],
            "plain_english": row.get("plain_english") or "Repair weak robustness, holdout, or reproducibility evidence before promotion review.",
            "recommended_repair_lane": "holdout_repair" if "thin_holdout_edge" in set(reasons) else "day_consistency_repair",
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Turns promotion rejects and predicted rejects into durable human-readable repair memory.",
        "memories": memories[:40],
        "top_memory": memories[0] if memories else {},
        "repair_routes": _ordered_unique([row["route_key"] for row in memories if row.get("route_key")])[:12],
        "avoid_routes": _ordered_unique([row["route_key"] for row in memories if row.get("recommended_repair_lane") == "day_consistency_repair"])[:12],
    }


def learning_budget_optimizer(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    scarcity = bool((state.get("live_beater_scarcity_mode") or {}).get("enabled")) if isinstance(state.get("live_beater_scarcity_mode"), dict) else False
    backpressure = (state.get("false_positive_pressure_gauge") or {}).get("pressure_score") if isinstance(state.get("false_positive_pressure_gauge"), dict) else 0.0
    allocations = [
        {"bucket": "promotion_repair", "budget_pct": 32.0 if float(backpressure or 0.0) < 60.0 else 42.0, "action": "repair_review_candidates"},
        {"bucket": "controlled_science", "budget_pct": 20.0, "action": "run_controlled_contracts"},
        {"bucket": "uncertainty_questions", "budget_pct": 18.0, "action": "answer_high_eig_questions"},
        {"bucket": "novelty_floor", "budget_pct": 20.0 if scarcity else 12.0, "action": "wide_indicator_shuffle"},
        {"bucket": "wild_breakthrough", "budget_pct": 10.0 if scarcity else 6.0, "action": "crazy_scoring_shuffle"},
    ]
    allocations.sort(key=lambda row: float(row.get("budget_pct") or 0.0), reverse=True)
    focus = _ordered_unique(
        list((state.get("promotion_packet_executive") or {}).get("focus_routes") or [])
        + list((state.get("question_driven_hunt_planner") or {}).get("focus_routes") or [])
        + list((state.get("scientific_hunt_executive") or {}).get("focus_routes") or [])
    )[:12]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Optimizes the next hunt budget across promotion repair, experiments, novelty, and wild exploration.",
        "allocations": allocations,
        "top_allocation": allocations[0] if allocations else {},
        "focus_routes": focus,
        "batch_size_multiplier": 1.18 if scarcity and float(backpressure or 0.0) < 65.0 else 0.82 if float(backpressure or 0.0) >= 75.0 else 1.0,
    }


def search_novelty_floor(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    coverage = state.get("search_space_coverage_map") if isinstance(state.get("search_space_coverage_map"), dict) else {}
    blind = [row for row in (coverage.get("blind_spots") or []) if isinstance(row, dict)]
    scarcity = bool((state.get("live_beater_scarcity_mode") or {}).get("enabled")) if isinstance(state.get("live_beater_scarcity_mode"), dict) else False
    floor = 28.0 if scarcity else 18.0
    routes = _ordered_unique([row.get("route_key") for row in blind] + list((state.get("uncertainty_heatmap") or {}).get("probe_routes") or []))[:12]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Keeps a minimum share of hunt work pointed at genuinely new route/gene space.",
        "floor_pct": floor,
        "focus_routes": routes,
        "mutation_width": "wide" if floor >= 25.0 else "medium",
        "summary": {"blind_spots": len(blind), "floor_pct": floor},
    }


def breakthrough_escalation_protocol(state: dict[str, Any] | None = None) -> dict[str, Any]:
    packets = _world_candidate_packets(dict(state or {}))
    commands = []
    for idx, packet in enumerate(packets[:8]):
        delta = float(packet.get("delta_vs_live") or 0.0)
        score = float(packet.get("evidence_score") or 0.0)
        if delta >= 1000.0 or score >= 82.0:
            commands.append({
                "command_id": _stable_hash({"breakthrough_escalation": packet.get("route_key"), "variant": packet.get("variant")}),
                "worker": f"worker_{(idx % 4) + 1}",
                "action": "breakthrough_escalation_review",
                "route_key": packet.get("route_key"),
                "variant": packet.get("variant"),
                "mutation_width": "tight",
                "reason": "large live edge or high promotion evidence",
            })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Escalates unusually strong live beaters into immediate review/repair work.",
        "commands": commands,
        "top_breakthrough": commands[0] if commands else {},
        "focus_routes": _ordered_unique([cmd.get("route_key") for cmd in commands])[:12],
    }


def false_discovery_backpressure_controller(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    pressure = state.get("false_positive_pressure_gauge") if isinstance(state.get("false_positive_pressure_gauge"), dict) else {}
    failures = state.get("promotion_failure_predictor_v3") if isinstance(state.get("promotion_failure_predictor_v3"), dict) else {}
    score = max(float(pressure.get("pressure_score") or 0.0), 100.0 * float((failures.get("highest_risk") or {}).get("predicted_reject_probability") or 0.0))
    mode = "strict_backpressure" if score >= 75.0 else "moderate_backpressure" if score >= 45.0 else "normal"
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Slows or narrows the hunt when false discovery risk is accumulating.",
        "pressure_score": round(score, 2),
        "mode": mode,
        "batch_size_multiplier": 0.68 if mode == "strict_backpressure" else 0.86 if mode == "moderate_backpressure" else 1.0,
        "mutation_width": "tight" if mode != "normal" else "medium",
        "avoid_routes": _ordered_unique(list(pressure.get("avoid_routes") or []) + list(failures.get("avoid_routes") or []))[:12],
    }


def multi_armed_strategy_portfolio(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    budget = state.get("learning_budget_optimizer") if isinstance(state.get("learning_budget_optimizer"), dict) else {}
    budget_by_bucket = {str(row.get("bucket")): float(row.get("budget_pct") or 0.0) for row in (budget.get("allocations") or []) if isinstance(row, dict)}
    arms = [
        {"arm": "promotion_packet", "allocation_pct": budget_by_bucket.get("promotion_repair", 32.0), "source": "promotion_packet_executive"},
        {"arm": "scientific_contracts", "allocation_pct": budget_by_bucket.get("controlled_science", 20.0), "source": "scientific_hunt_executive"},
        {"arm": "high_eig_questions", "allocation_pct": budget_by_bucket.get("uncertainty_questions", 18.0), "source": "question_driven_hunt_planner"},
        {"arm": "novelty_floor", "allocation_pct": budget_by_bucket.get("novelty_floor", 12.0), "source": "search_novelty_floor"},
        {"arm": "wild_shuffle", "allocation_pct": budget_by_bucket.get("wild_breakthrough", 6.0), "source": "breakthrough_escalation_protocol"},
    ]
    arms.sort(key=lambda row: float(row.get("allocation_pct") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Maintains a live strategy portfolio so hunt workers do not collapse into one tactic.",
        "arms": arms,
        "top_arm": arms[0] if arms else {},
        "focus_routes": _ordered_unique(list((state.get("promotion_packet_executive") or {}).get("focus_routes") or []) + list((state.get("search_novelty_floor") or {}).get("focus_routes") or []))[:12],
    }


def historical_lesson_ab_harness(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    lessons = (state.get("learning_compression_field_manual") or {}).get("rules") if isinstance(state.get("learning_compression_field_manual"), dict) else []
    if not lessons:
        lessons = (state.get("auto_promoted_field_manual") or {}).get("rules") if isinstance(state.get("auto_promoted_field_manual"), dict) else []
    tests = []
    for idx, lesson in enumerate(lessons or [], 1):
        if not isinstance(lesson, dict):
            continue
        tests.append({
            "test_id": _stable_hash({"historical_lesson_ab": lesson, "idx": idx}),
            "lesson": lesson.get("rule") or lesson.get("plain_english") or str(lesson)[:80],
            "arm_a": "lesson_applied",
            "arm_b": "lesson_withheld_shadow",
            "success_metric": "live beating candidate survival",
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Creates shadow A/B tests to verify historical lessons still help.",
        "tests": tests[:12],
        "top_test": tests[0] if tests else {},
    }


def promotion_packet_diff_engine(state: dict[str, Any] | None = None) -> dict[str, Any]:
    packets = _world_candidate_packets(dict(state or {}))
    diffs = []
    for left, right in zip(packets[:8], packets[1:9]):
        left_genes = set(left.get("gene_tokens") or [])
        right_genes = set(right.get("gene_tokens") or [])
        diffs.append({
            "diff_id": _stable_hash({"promotion_packet_diff": [left.get("variant"), right.get("variant")]}),
            "left_variant": left.get("variant"),
            "right_variant": right.get("variant"),
            "left_route": left.get("route_key"),
            "right_route": right.get("route_key"),
            "delta_gap": round(float(left.get("delta_vs_live") or 0.0) - float(right.get("delta_vs_live") or 0.0), 2),
            "gene_only_left": sorted(left_genes - right_genes)[:8],
            "gene_only_right": sorted(right_genes - left_genes)[:8],
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Diffs promotion packets so repairs target the exact evidence and gene gaps.",
        "diffs": diffs,
        "top_diff": diffs[0] if diffs else {},
    }


def candidate_repair_recipe_generator(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    gaps = state.get("evidence_gap_router") if isinstance(state.get("evidence_gap_router"), dict) else {}
    replays = state.get("counterfactual_promotion_replay") if isinstance(state.get("counterfactual_promotion_replay"), dict) else {}
    recipes = []
    sources = list(gaps.get("tasks") or []) + list(replays.get("replays") or [])
    for idx, row in enumerate(sources[:20]):
        if not isinstance(row, dict):
            continue
        route = str(row.get("route_key") or "")
        lane = row.get("gap") or row.get("counterfactual_repair") or "promotion_packet_completion"
        recipes.append({
            "recipe_id": _stable_hash({"candidate_repair_recipe": route, "lane": lane, "idx": idx}),
            "worker": row.get("worker") or f"worker_{(idx % 4) + 1}",
            "route_key": route,
            "variant": row.get("variant"),
            "action": "candidate_repair_recipe",
            "repair_lane": lane,
            "mutation_width": "tight" if "holdout" in str(lane) or "robustness" in str(lane) else "medium",
            "plain_english": f"Repair {lane} before promotion review.",
        })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Converts evidence gaps and replay failures into worker-ready repair recipes.",
        "recipes": recipes,
        "top_recipe": recipes[0] if recipes else {},
        "focus_routes": _ordered_unique([row.get("route_key") for row in recipes])[:12],
    }


def automated_red_team_reviewer(state: dict[str, Any] | None = None) -> dict[str, Any]:
    packets = _world_candidate_packets(dict(state or {}))
    objections = []
    for packet in packets[:16]:
        reasons = []
        risk = float(packet.get("predicted_reject_probability") or packet.get("overfit_risk_score") or 0.0)
        risk_pct = risk * 100.0 if risk <= 1.0 else risk
        novelty = float(packet.get("novelty_score") or 0.0)
        if risk_pct >= 55.0:
            reasons.append("promotion risk is high")
        if novelty < 8.0:
            reasons.append("novelty evidence is thin")
        if float(packet.get("evidence_score") or 0.0) < 55.0:
            reasons.append("evidence score is not review-grade yet")
        if reasons:
            action = "repair_before_review" if risk_pct < 55.0 else "block_until_repaired"
            objections.append({
                "objection_id": _stable_hash({"red_team": packet.get("route_key"), "variant": packet.get("variant")}),
                "route_key": packet.get("route_key"),
                "variant": packet.get("variant"),
                "objections": reasons,
                "recommended_action": action,
            })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Red-teams candidates before promotion so the hunt learns from likely review objections immediately.",
        "objections": objections,
        "top_objection": objections[0] if objections else {},
        "repair_routes": _ordered_unique([row.get("route_key") for row in objections if row.get("recommended_action") == "repair_before_review"])[:12],
        "avoid_routes": _ordered_unique([row.get("route_key") for row in objections if row.get("recommended_action") == "block_until_repaired"])[:12],
    }


def learning_compression_field_manual(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    rules = []
    top_signature = (state.get("overfit_signature_library") or {}).get("top_signature") if isinstance(state.get("overfit_signature_library"), dict) else {}
    if top_signature:
        rules.append({
            "rule_id": _stable_hash({"field_manual": "overfit", "signature": top_signature.get("signature")}),
            "rule": f"Before scaling routes with {top_signature.get('signature')}, force a tight robustness repair.",
            "confidence": round(min(0.95, 0.45 + float(top_signature.get("severity") or 0.0)), 4),
            "status": "active_rule",
        })
    budget = state.get("learning_budget_optimizer") if isinstance(state.get("learning_budget_optimizer"), dict) else {}
    top_alloc = budget.get("top_allocation") or {}
    if top_alloc:
        rules.append({
            "rule_id": _stable_hash({"field_manual": "budget", "bucket": top_alloc.get("bucket")}),
            "rule": f"Prioritize {top_alloc.get('bucket')} when it is the top budget allocation.",
            "confidence": 0.72,
            "status": "active_rule",
        })
    if not rules:
        rules.append({"rule_id": "default_promotion_first", "rule": "Only scale live-beating candidates with enough promotion evidence to explain the edge.", "confidence": 0.6, "status": "active_rule"})
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Compresses current learning into a small field manual the next hunt can apply while running.",
        "rules": rules[:12],
        "top_rule": rules[0] if rules else {},
        "rule_count": len(rules),
    }


def hunt_outcome_attribution_v2(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    packets = _world_candidate_packets(state)
    rows = []
    for packet in packets[:20]:
        route = str(packet.get("route_key") or "")
        source = "promotion_packet" if route in set((state.get("promotion_packet_executive") or {}).get("focus_routes") or []) else "search_candidate"
        rows.append({
            "attribution_id": _stable_hash({"hunt_outcome_attribution_v2": route, "variant": packet.get("variant")}),
            "route_key": route,
            "variant": packet.get("variant"),
            "source": source,
            "pnl_contribution": round(float(packet.get("step2_pnl") or 0.0), 2),
            "delta_contribution": round(float(packet.get("delta_vs_live") or 0.0), 2),
        })
    rows.sort(key=lambda row: float(row.get("delta_contribution") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Attributes live-beating outcomes to routes, modules, and candidate sources.",
        "attributions": rows,
        "top_attribution": rows[0] if rows else {},
        "focus_routes": _ordered_unique([row.get("route_key") for row in rows[:8]])[:12],
    }


def world_state_dashboard_artifact(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    summary = {
        "top_belief": (state.get("unified_learning_state_reducer") or {}).get("top_belief") if isinstance(state.get("unified_learning_state_reducer"), dict) else {},
        "top_budget": (state.get("learning_budget_optimizer") or {}).get("top_allocation") if isinstance(state.get("learning_budget_optimizer"), dict) else {},
        "backpressure": (state.get("false_discovery_backpressure_controller") or {}).get("mode") if isinstance(state.get("false_discovery_backpressure_controller"), dict) else None,
        "regime": (state.get("live_regime_shift_detector_v2") or {}).get("mode") if isinstance(state.get("live_regime_shift_detector_v2"), dict) else None,
        "top_repair": (state.get("candidate_repair_recipe_generator") or {}).get("top_recipe") if isinstance(state.get("candidate_repair_recipe_generator"), dict) else {},
    }
    focus = _ordered_unique(
        list((state.get("unified_learning_state_reducer") or {}).get("focus_routes") or [])
        + list((state.get("learning_budget_optimizer") or {}).get("focus_routes") or [])
        + list((state.get("candidate_repair_recipe_generator") or {}).get("focus_routes") or [])
    )[:12]
    avoid = _ordered_unique(
        list((state.get("false_discovery_backpressure_controller") or {}).get("avoid_routes") or [])
        + list((state.get("automated_red_team_reviewer") or {}).get("avoid_routes") or [])
    )[:12]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Single dashboard artifact summarizing what the learning system currently believes and will do.",
        "summary": summary,
        "focus_routes": [route for route in focus if route not in set(avoid)],
        "avoid_routes": avoid,
    }


def meta_learning_governor(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    budget = state.get("learning_budget_optimizer") if isinstance(state.get("learning_budget_optimizer"), dict) else {}
    backpressure = state.get("false_discovery_backpressure_controller") if isinstance(state.get("false_discovery_backpressure_controller"), dict) else {}
    novelty = state.get("search_novelty_floor") if isinstance(state.get("search_novelty_floor"), dict) else {}
    dashboard = state.get("world_state_dashboard_artifact") if isinstance(state.get("world_state_dashboard_artifact"), dict) else {}
    mode = "truth_first_repair" if backpressure.get("mode") in {"strict_backpressure", "moderate_backpressure"} else "adaptive_hunt"
    if float(novelty.get("floor_pct") or 0.0) >= 25.0 and mode == "adaptive_hunt":
        mode = "discovery_with_promotion_guardrails"
    commands = []
    for idx, route in enumerate(dashboard.get("focus_routes") or budget.get("focus_routes") or []):
        commands.append({
            "command_id": _stable_hash({"meta_learning_governor": mode, "route": route, "idx": idx}),
            "worker": f"worker_{(idx % 4) + 1}",
            "action": "meta_governed_probe" if mode != "truth_first_repair" else "meta_governed_repair",
            "route_key": route,
            "mutation_width": backpressure.get("mutation_width") or novelty.get("mutation_width") or "medium",
            "batch_size_multiplier": min(float(budget.get("batch_size_multiplier") or 1.0), float(backpressure.get("batch_size_multiplier") or 1.0)),
            "reason": mode,
        })
    focus = _ordered_unique(list(dashboard.get("focus_routes") or []) + list(novelty.get("focus_routes") or []))[:12]
    avoid = _ordered_unique(list(dashboard.get("avoid_routes") or []) + list(backpressure.get("avoid_routes") or []))[:12]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Governors all meta-learning outputs into one runtime policy for live-only hunting.",
        "mode": mode,
        "commands": commands[:16],
        "top_command": commands[0] if commands else {},
        "focus_routes": [route for route in focus if route not in set(avoid)],
        "avoid_routes": avoid,
        "mutation_width": (commands[0] or {}).get("mutation_width") if commands else backpressure.get("mutation_width") or novelty.get("mutation_width") or "medium",
        "batch_size_multiplier": round(min(float(budget.get("batch_size_multiplier") or 1.0), float(backpressure.get("batch_size_multiplier") or 1.0)), 4),
        "summary": {
            "mode": mode,
            "command_count": len(commands),
            "focus_count": len(focus),
            "avoid_count": len(avoid),
        },
    }


def world_class_meta_learning_suite(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    unified = unified_learning_state_reducer(state)
    priority = artifact_priority_arbitration_engine({**state, "unified_learning_state_reducer": unified})
    provenance = evidence_provenance_graph({**state, "unified_learning_state_reducer": unified})
    replay = counterfactual_promotion_replay(state)
    survival = candidate_survival_simulator(state)
    regime = live_regime_shift_detector_v2(state)
    trust = adaptive_trust_weights_per_module({**state, "artifact_priority_arbitration_engine": priority})
    worker_elo = worker_skill_elo_v2(state)
    graph = route_genome_knowledge_graph(state)
    interactions = causal_feature_interaction_miner(state)
    overfit = overfit_signature_library(state)
    rejection_memory = review_rejection_memory_bank_v2(state)
    budget = learning_budget_optimizer(state)
    novelty = search_novelty_floor(state)
    breakthrough = breakthrough_escalation_protocol(state)
    backpressure = false_discovery_backpressure_controller(state)
    portfolio = multi_armed_strategy_portfolio({**state, "learning_budget_optimizer": budget, "search_novelty_floor": novelty})
    field_manual_seed = learning_compression_field_manual({**state, "overfit_signature_library": overfit, "learning_budget_optimizer": budget})
    lesson_ab = historical_lesson_ab_harness({**state, "learning_compression_field_manual": field_manual_seed})
    diff = promotion_packet_diff_engine(state)
    repair = candidate_repair_recipe_generator({**state, "counterfactual_promotion_replay": replay})
    red_team = automated_red_team_reviewer(state)
    field_manual = learning_compression_field_manual({**state, "overfit_signature_library": overfit, "learning_budget_optimizer": budget})
    attribution = hunt_outcome_attribution_v2(state)
    dashboard = world_state_dashboard_artifact({
        **state,
        "unified_learning_state_reducer": unified,
        "learning_budget_optimizer": budget,
        "false_discovery_backpressure_controller": backpressure,
        "live_regime_shift_detector_v2": regime,
        "candidate_repair_recipe_generator": repair,
        "automated_red_team_reviewer": red_team,
    })
    governor = meta_learning_governor({
        **state,
        "learning_budget_optimizer": budget,
        "false_discovery_backpressure_controller": backpressure,
        "search_novelty_floor": novelty,
        "world_state_dashboard_artifact": dashboard,
    })
    return {
        "unified_learning_state_reducer": unified,
        "artifact_priority_arbitration_engine": priority,
        "evidence_provenance_graph": provenance,
        "counterfactual_promotion_replay": replay,
        "candidate_survival_simulator": survival,
        "live_regime_shift_detector_v2": regime,
        "adaptive_trust_weights_per_module": trust,
        "worker_skill_elo_v2": worker_elo,
        "route_genome_knowledge_graph": graph,
        "causal_feature_interaction_miner": interactions,
        "overfit_signature_library": overfit,
        "review_rejection_memory_bank_v2": rejection_memory,
        "learning_budget_optimizer": budget,
        "search_novelty_floor": novelty,
        "breakthrough_escalation_protocol": breakthrough,
        "false_discovery_backpressure_controller": backpressure,
        "multi_armed_strategy_portfolio": portfolio,
        "historical_lesson_ab_harness": lesson_ab,
        "promotion_packet_diff_engine": diff,
        "candidate_repair_recipe_generator": repair,
        "automated_red_team_reviewer": red_team,
        "learning_compression_field_manual": field_manual,
        "hunt_outcome_attribution_v2": attribution,
        "world_state_dashboard_artifact": dashboard,
        "meta_learning_governor": governor,
    }


ELITE_LEARNING_ARTIFACT_SPECS = {
    "cross_run_candidate_resurrection_scoring": ("resurrection", "Scores old or rejected live beaters that deserve another controlled attempt.", "resurrect_candidate"),
    "live_edge_decay_curves_by_route": ("decay", "Estimates whether each route's live edge is fresh, decaying, or stale.", "refresh_or_decay_route"),
    "promotion_failure_causal_backtrace": ("failure", "Backtraces promotion failure risk into route, evidence, control, and robustness causes.", "repair_failure_cause"),
    "route_crowding_detector": ("crowding", "Detects when too many candidates share the same route/family DNA.", "diversify_route_crowding"),
    "worker_disagreement_logging": ("worker", "Logs worker/module disagreement so uncertainty becomes an explicit hunt task.", "assign_disagreement_retest"),
    "automatic_win_explainer": ("explain_win", "Explains why current live beaters are winning.", "clone_winning_dna"),
    "automatic_loss_explainer": ("explain_loss", "Explains why weak or rejected candidates are failing.", "repair_losing_dna"),
    "variant_mutation_ancestry_scoring": ("ancestry", "Scores whether child variants improved enough over parent lineage.", "prefer_productive_ancestry"),
    "parent_child_edge_retention_model": ("retention", "Models whether parent edge survives child mutation.", "retain_parent_edge"),
    "live_review_mismatch_detector": ("mismatch", "Finds candidates with live P/L strength but review weakness.", "close_live_review_gap"),
    "candidate_fragility_heatmap": ("fragility", "Highlights candidates likely to break under holdout, day, ticker, or side stress.", "stress_fragile_candidate"),
    "robustness_repair_prioritizer": ("repair", "Ranks robustness repairs by expected promotion lift.", "prioritize_robustness_repair"),
    "indicator_family_saturation_meter": ("saturation", "Measures when indicator families are overused and losing discovery value.", "rotate_indicator_family"),
    "ticker_concentration_budgeter": ("concentration", "Budgets search away from over-concentrated tickers.", "rebalance_ticker_budget"),
    "time_of_day_route_memory": ("temporal", "Tracks which routes appear strongest by time-of-day bucket.", "time_condition_route_probe"),
    "market_regime_conditioned_mutation_widths": ("regime", "Adapts mutation width to the current market/regime signal.", "regime_condition_width"),
    "promotion_readiness_lift_estimator": ("readiness", "Estimates how much promotion readiness a repair could add.", "maximize_readiness_lift"),
    "almost_promoted_near_miss_league": ("near_miss", "Keeps a league of candidates just below promotion quality.", "repair_near_miss"),
    "false_confidence_detector": ("confidence", "Finds high-confidence looking candidates with thin evidence.", "challenge_false_confidence"),
    "lesson_contradiction_detector_v3": ("contradiction", "Finds contradictions between current hunt evidence and stored lessons.", "retest_contradicted_lesson"),
    "route_hypothesis_decay_scheduler": ("hypothesis", "Schedules route hypotheses for refresh before they become stale.", "refresh_route_hypothesis"),
    "dead_route_resurrection_calendar": ("dead_route", "Schedules previously dead routes for occasional resurrection when regime shifts.", "calendar_resurrection_probe"),
    "novelty_usefulness_scorer": ("novelty", "Separates useful novelty from decorative novelty.", "scale_useful_novelty"),
    "search_entropy_monitor": ("entropy", "Measures route/gene diversity so the search does not collapse too early.", "restore_search_entropy"),
    "exploit_overfit_warning_system": ("overfit", "Warns when exploit pressure is likely becoming overfit.", "slow_exploit_overfit"),
    "discovery_overload_warning_system": ("overload", "Warns when novelty is too broad to learn efficiently.", "narrow_discovery_overload"),
    "live_beater_quality_percentile_model": ("quality", "Percentiles live beaters by edge, evidence, and fragility quality.", "promote_top_quality_percentile"),
    "candidate_control_similarity_scorer": ("control", "Scores whether treatment candidates have suitable matched controls.", "improve_control_similarity"),
    "promotion_packet_completeness_gate": ("packet", "Blocks promotion packets that are missing required evidence categories.", "complete_promotion_packet"),
    "adaptive_holdout_demand_model": ("holdout", "Raises holdout demand when candidates look fragile or concentrated.", "demand_more_holdout"),
    "evidence_debt_ledger_v2": ("debt", "Tracks unpaid evidence debt by route and repair lane.", "pay_evidence_debt"),
    "candidate_repair_outcome_memory": ("repair_memory", "Remembers which repair recipes actually helped candidates later.", "reuse_working_repair"),
    "wild_shuffle_safety_governor": ("wild", "Lets crazy scoring/indicator shuffles run while enforcing promotion safety.", "safe_wild_shuffle"),
    "strategy_drift_fingerprinting": ("strategy", "Fingerprints strategy drift during the hunt.", "correct_strategy_drift"),
    "route_family_tournament_brackets": ("tournament", "Runs route families as brackets so winners and challengers are explicit.", "advance_family_winner"),
    "worker_role_switching_policy": ("worker_switch", "Switches worker roles when current role/value fit decays.", "switch_worker_role"),
    "hunt_cycle_objective_selector": ("objective", "Selects the next cycle objective from evidence pressure and opportunity.", "select_cycle_objective"),
    "stop_wasting_cycles_detector": ("waste", "Detects routes or tactics consuming cycles without live-learning payoff.", "stop_wasting_cycles"),
    "route_interaction_veto_rules": ("interaction_veto", "Vetoes route/gene interactions that repeatedly create false discoveries.", "veto_bad_interaction"),
    "meta_learning_ablation_scheduler": ("ablation", "Schedules small ablations to prove which learning modules are helping.", "run_meta_ablation"),
    "historical_lesson_replay_simulator": ("lesson_replay", "Replays old lessons against current candidates before trusting them.", "replay_historical_lesson"),
    "synthetic_challenger_generator": ("challenger", "Generates challenger ideas from the gaps between winners and near misses.", "generate_synthetic_challenger"),
    "candidate_ensemble_builder": ("ensemble", "Combines complementary candidates/routes into ensemble hypotheses.", "build_candidate_ensemble"),
    "live_leaderboard_stability_tracker": ("stability", "Tracks whether top live beaters remain stable across refreshes.", "stabilize_leaderboard"),
    "shock_event_memory_tagging": ("shock", "Tags hunt evidence that may be explained by unusual market/shock events.", "tag_shock_sensitive_memory"),
    "promotion_review_rehearsal_mode": ("rehearsal", "Runs a pre-review rehearsal and lists exact objections before promotion.", "rehearse_promotion_review"),
    "human_readable_hunt_diary_compiler": ("diary", "Compiles a human-readable diary of what the hunt learned in-flight.", "write_hunt_diary"),
    "top100_memory_compression_by_theme": ("compression", "Compresses the top 100 into reusable themes instead of isolated variants.", "compress_top100_theme"),
    "self_auditing_learning_scorecard": ("audit", "Scores whether the learning system is producing usable evidence.", "audit_learning_system"),
    "world_state_next_best_question_engine": ("question", "Chooses the next best question to answer while the hunt is still running.", "answer_next_best_question"),
}


def _elite_candidate_rows(state: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    failures = state.get("promotion_failure_predictor_v3") if isinstance(state.get("promotion_failure_predictor_v3"), dict) else {}
    risk_by_route = {str(row.get("route_key") or ""): row for row in (failures.get("predictions") or failures.get("risks") or []) if isinstance(row, dict)}
    for idx, packet in enumerate(_world_candidate_packets(state)[:100], 1):
        route = str(packet.get("route_key") or "")
        risk = risk_by_route.get(route, {})
        readiness = float(packet.get("promotion_readiness_score") or packet.get("promotion_quality_score") or 0.0)
        delta = float(packet.get("delta_vs_live") or packet.get("step2_delta_vs_active") or 0.0)
        evidence = float(packet.get("evidence_score") or 0.0)
        reject_prob = float(risk.get("predicted_reject_probability") or packet.get("predicted_reject_probability") or 0.0)
        rows.append({
            **packet,
            "rank": idx,
            "route_key": route,
            "delta_vs_live": delta,
            "readiness": readiness,
            "evidence_score": evidence,
            "reject_probability": reject_prob,
            "fragility_score": round(max(0.0, min(100.0, (100.0 - readiness) * 0.42 + reject_prob * 70.0 + max(0.0, 12.0 - float(packet.get("novelty_score") or 0.0)) * 1.5)), 4),
            "gene_tokens": packet.get("gene_tokens") or _row_gene_tokens(packet),
            "ticker": str(packet.get("ticker") or (route.split("|")[0] if route else "")),
            "indicator": str(packet.get("indicator") or packet.get("family_key") or packet.get("mutation_lane") or "unknown"),
        })
    if not rows:
        for idx, row in enumerate(_temporal_candidate_rows(state, limit=100), 1):
            route = _row_route(row)
            rows.append({
                **row,
                "rank": idx,
                "route_key": route,
                "delta_vs_live": hunt_intel.live_delta(row),
                "readiness": float(row.get("promotion_readiness_score") or row.get("promotion_quality_score") or 0.0),
                "evidence_score": float(row.get("promotion_quality_score") or 0.0),
                "reject_probability": 0.0,
                "fragility_score": float(row.get("overfit_risk_score") or 0.0),
                "gene_tokens": _row_gene_tokens(row),
                "ticker": str(row.get("ticker") or (route.split("|")[0] if route else "")),
                "indicator": str(row.get("indicator") or row.get("family_key") or row.get("mutation_lane") or "unknown"),
            })
    return rows


def _elite_route_stats(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    stats: dict[str, dict[str, Any]] = {}
    for row in rows:
        route = str(row.get("route_key") or "")
        if not route:
            continue
        stat = stats.setdefault(route, {"route_key": route, "count": 0, "best_delta": 0.0, "avg_readiness": 0.0, "avg_fragility": 0.0, "variants": []})
        stat["count"] += 1
        stat["best_delta"] = max(float(stat.get("best_delta") or 0.0), float(row.get("delta_vs_live") or 0.0))
        stat["avg_readiness"] += float(row.get("readiness") or 0.0)
        stat["avg_fragility"] += float(row.get("fragility_score") or 0.0)
        stat["variants"].append(row.get("variant"))
    for stat in stats.values():
        count = max(1, int(stat.get("count") or 1))
        stat["avg_readiness"] = round(float(stat.get("avg_readiness") or 0.0) / count, 4)
        stat["avg_fragility"] = round(float(stat.get("avg_fragility") or 0.0) / count, 4)
        stat["variants"] = [variant for variant in stat.get("variants") or [] if variant][:5]
    return stats


def _elite_records(kind: str, rows: list[dict[str, Any]], route_stats: dict[str, dict[str, Any]], state: dict[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    top_rows = sorted(rows, key=lambda row: (float(row.get("delta_vs_live") or 0.0), float(row.get("evidence_score") or 0.0)), reverse=True)
    weak_rows = sorted(rows, key=lambda row: (float(row.get("fragility_score") or 0.0), -float(row.get("readiness") or 0.0)), reverse=True)
    route_rows = sorted(route_stats.values(), key=lambda row: (int(row.get("count") or 0), float(row.get("best_delta") or 0.0)), reverse=True)
    if kind in {"crowding", "saturation", "concentration", "entropy", "overload", "tournament", "compression"}:
        for stat in route_rows[:12]:
            score = int(stat.get("count") or 0) * 10.0 + float(stat.get("best_delta") or 0.0) / 50.0
            records.append({**stat, "signal_score": round(score, 4), "recommended_action": "diversify" if int(stat.get("count") or 0) >= 3 else "monitor"})
    elif kind in {"failure", "fragility", "repair", "mismatch", "confidence", "packet", "holdout", "debt", "rehearsal"}:
        for row in weak_rows[:12]:
            records.append({
                "route_key": row.get("route_key"),
                "variant": row.get("variant"),
                "signal_score": round(float(row.get("fragility_score") or 0.0) + max(0.0, 70.0 - float(row.get("readiness") or 0.0)), 4),
                "readiness": row.get("readiness"),
                "reject_probability": row.get("reject_probability"),
                "recommended_action": "repair_before_review",
                "why": "promotion fragility, review mismatch, or missing evidence",
            })
    elif kind in {"resurrection", "near_miss", "dead_route", "decay", "hypothesis", "lesson_replay"}:
        for row in sorted(rows, key=lambda item: (float(item.get("readiness") or 0.0), float(item.get("delta_vs_live") or 0.0)), reverse=True)[:12]:
            records.append({
                "route_key": row.get("route_key"),
                "variant": row.get("variant"),
                "signal_score": round(float(row.get("readiness") or 0.0) + max(0.0, float(row.get("delta_vs_live") or 0.0)) / 40.0, 4),
                "recommended_action": "resurrect_or_revalidate",
                "why": "near-promotion edge worth another controlled pass",
            })
    elif kind in {"worker", "worker_switch", "diary", "audit", "ablation"}:
        contracts = (state.get("worker_job_contracts") or {}).get("contracts") if isinstance(state.get("worker_job_contracts"), dict) else []
        workers = contracts or [{"worker": f"worker_{idx}", "jobs": []} for idx in range(1, 5)]
        for idx, worker in enumerate(workers[:4], 1):
            records.append({
                "worker": worker.get("worker") or f"worker_{idx}",
                "route_key": ((worker.get("focus_routes") or [""])[0] if isinstance(worker, dict) else ""),
                "signal_score": round(70.0 - idx, 4),
                "recommended_action": "switch_or_retest_role" if kind == "worker_switch" else "log_disagreement",
                "why": "worker/module role needs explicit evidence trail",
            })
    elif kind in {"wild", "challenger", "ensemble", "interaction_veto"}:
        for row in top_rows[:12]:
            records.append({
                "route_key": row.get("route_key"),
                "variant": row.get("variant"),
                "gene_tokens": row.get("gene_tokens") or [],
                "signal_score": round(max(0.0, float(row.get("delta_vs_live") or 0.0)) / 25.0 + float(row.get("evidence_score") or 0.0), 4),
                "recommended_action": "generate_controlled_challenger",
                "why": "winning DNA can be safely shuffled with controls",
            })
    else:
        for row in top_rows[:12]:
            records.append({
                "route_key": row.get("route_key"),
                "variant": row.get("variant"),
                "signal_score": round(float(row.get("evidence_score") or 0.0) + max(0.0, float(row.get("delta_vs_live") or 0.0)) / 35.0 - float(row.get("fragility_score") or 0.0) * 0.25, 4),
                "readiness": row.get("readiness"),
                "delta_vs_live": row.get("delta_vs_live"),
                "fragility_score": row.get("fragility_score"),
                "recommended_action": "scale_or_repair",
                "why": "best combined live edge, evidence, and fragility tradeoff",
            })
    records.sort(key=lambda row: float(row.get("signal_score") or 0.0), reverse=True)
    return records


def _elite_artifact(name: str, state: dict[str, Any], rows: list[dict[str, Any]], route_stats: dict[str, dict[str, Any]]) -> dict[str, Any]:
    kind, description, default_action = ELITE_LEARNING_ARTIFACT_SPECS[name]
    records = _elite_records(kind, rows, route_stats, state)
    top = records[0] if records else {}
    focus = _ordered_unique([row.get("route_key") for row in records if row.get("route_key") and row.get("recommended_action") not in {"diversify", "repair_before_review"}])[:12]
    repair = _ordered_unique([row.get("route_key") for row in records if row.get("route_key") and row.get("recommended_action") in {"repair_before_review", "resurrect_or_revalidate", "switch_or_retest_role"}])[:12]
    avoid = _ordered_unique([
        row.get("route_key")
        for row in records
        if row.get("route_key")
        and (
            row.get("recommended_action") == "diversify"
            or (kind in {"overfit", "waste", "interaction_veto"} and float(row.get("fragility_score") or 0.0) >= 50.0)
        )
    ])[:12]
    mutation_width = "tight" if kind in {"failure", "fragility", "repair", "mismatch", "packet", "holdout", "debt", "rehearsal", "overfit", "waste", "interaction_veto"} else "wide" if kind in {"wild", "challenger", "novelty", "resurrection", "dead_route"} else "medium"
    batch = 0.78 if mutation_width == "tight" else 1.12 if mutation_width == "wide" else 1.0
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "artifact": name,
        "description": description,
        "kind": kind,
        "records": records[:20],
        "top_record": top,
        "focus_routes": [route for route in focus if route not in set(avoid)],
        "repair_routes": repair,
        "avoid_routes": avoid,
        "recommended_action": (top.get("recommended_action") if isinstance(top, dict) else None) or default_action,
        "mutation_width": mutation_width,
        "batch_size_multiplier": batch,
        "summary": {
            "record_count": len(records),
            "top_route": top.get("route_key") if isinstance(top, dict) else "",
            "top_action": (top.get("recommended_action") if isinstance(top, dict) else None) or default_action,
        },
    }


def elite_learning_system_summary(state: dict[str, Any]) -> dict[str, Any]:
    artifacts = [
        state.get(key)
        for key in ELITE_LEARNING_ARTIFACTS
        if isinstance(state.get(key), dict)
    ]
    focus = _ordered_unique([route for artifact in artifacts for route in (artifact.get("focus_routes") or [])])[:12]
    repair = _ordered_unique([route for artifact in artifacts for route in (artifact.get("repair_routes") or [])])[:12]
    avoid = _ordered_unique([route for artifact in artifacts for route in (artifact.get("avoid_routes") or [])])[:12]
    top_actions = [
        {
            "artifact": artifact.get("artifact"),
            "route_key": (artifact.get("top_record") or {}).get("route_key"),
            "action": artifact.get("recommended_action"),
        }
        for artifact in artifacts
        if artifact.get("top_record")
    ][:12]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Executive summary for the 50-artifact elite learning layer.",
        "summary": {
            "artifact_count": len(artifacts),
            "focus_count": len(focus),
            "repair_count": len(repair),
            "avoid_count": len(avoid),
            "top_action": (top_actions[0] if top_actions else {}),
        },
        "focus_routes": [route for route in focus if route not in set(avoid)],
        "repair_routes": repair,
        "avoid_routes": avoid,
        "top_actions": top_actions,
    }


def elite_world_class_learning_suite(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    rows = _elite_candidate_rows(state)
    route_stats = _elite_route_stats(rows)
    artifacts: dict[str, Any] = {}
    for name in ELITE_LEARNING_ARTIFACTS:
        artifacts[name] = _elite_artifact(name, {**state, **artifacts}, rows, route_stats)
    # The next-best-question artifact should explicitly point at the strongest unresolved repair/focus tension.
    question_artifact = artifacts.get("world_state_next_best_question_engine") or {}
    top_repair = next((route for artifact in artifacts.values() for route in artifact.get("repair_routes") or []), "")
    top_focus = next((route for artifact in artifacts.values() for route in artifact.get("focus_routes") or []), "")
    question_artifact["top_record"] = {
        "question": "Which live-beating route has the highest promotion-lift if repaired in the next cycle?",
        "route_key": top_repair or top_focus,
        "recommended_action": "answer_next_best_question",
        "signal_score": 100.0 if (top_repair or top_focus) else 0.0,
    }
    question_artifact["focus_routes"] = _ordered_unique([top_repair or top_focus] + list(question_artifact.get("focus_routes") or []))[:12]
    artifacts["world_state_next_best_question_engine"] = question_artifact
    artifacts["elite_learning_system_summary"] = elite_learning_system_summary({**state, **artifacts})
    return artifacts


PROOF_LEARNING_ARTIFACT_SPECS = {
    "learning_truth_ledger": ("truth", "Tracks every active lesson, claim, prediction, and whether later evidence supports it.", "verify_or_falsify_claim"),
    "causal_credit_assignment_engine": ("credit", "Assigns live edge credit to route, indicator, lane, worker, regime, and repair ingredients.", "assign_causal_credit"),
    "live_hunt_simulator": ("simulator", "Simulates next-cycle choices before spending hunt budget.", "simulate_next_cycle"),
    "experiment_promotion_ladder": ("ladder", "Stages candidates from idea to probe to controlled test to repair to review candidate.", "advance_candidate_stage"),
    "persistent_cross_hunt_memory_warehouse": ("warehouse", "Stores compact cross-hunt lessons with proof, timestamps, regime, and survival metadata.", "persist_validated_memory"),
    "learning_roi_accounting": ("roi", "Measures which learning modules produce useful live-beating or promotion-survivable evidence.", "rebalance_learning_roi"),
    "adaptive_research_agenda": ("agenda", "Chooses the next unresolved question the hunt should answer.", "answer_research_question"),
    "self_falsification_mode": ("falsification", "Spends budget trying to disprove the system's strongest assumptions.", "falsify_strong_belief"),
    "counterfactual_budget_replay": ("budget_replay", "Replays alternate budget allocations to estimate opportunity cost.", "replay_budget_counterfactual"),
    "candidate_lifecycle_state_machine": ("lifecycle", "Assigns every candidate a lifecycle stage and next allowed action.", "advance_lifecycle_state"),
    "regime_aware_lesson_validity": ("regime_validity", "Scopes lesson validity to current and historical market regimes.", "validate_lesson_regime"),
    "world_model_dashboard": ("world_model", "Single belief dashboard of top beliefs, weak beliefs, contradictions, tests, and risks.", "update_world_model"),
    "autonomous_hunt_review_board": ("review_board", "Lets promoter, skeptic, scientist, operator, and explorer roles vote before steering changes.", "review_board_vote"),
    "memory_garbage_collector": ("garbage", "Retires stale, contradicted, low-ROI, or single-use lessons.", "retire_bad_memory"),
    "proof_carrying_promotion_packets": ("proof_packet", "Attaches proof, kill risks, and supporting evidence to every promotion packet.", "build_proof_packet"),
}


def _proof_base_rows(state: dict[str, Any]) -> list[dict[str, Any]]:
    rows = _elite_candidate_rows(state)
    if rows:
        return rows
    return [
        {
            "variant": row.get("variant"),
            "route_key": _row_route(row),
            "delta_vs_live": hunt_intel.live_delta(row),
            "readiness": float(row.get("promotion_readiness_score") or row.get("promotion_quality_score") or 0.0),
            "evidence_score": float(row.get("promotion_quality_score") or 0.0),
            "fragility_score": float(row.get("overfit_risk_score") or 0.0),
            "gene_tokens": _row_gene_tokens(row),
        }
        for row in _temporal_candidate_rows(state, limit=80)
    ]


def _proof_records(kind: str, state: dict[str, Any], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    elite_summary = state.get("elite_learning_system_summary") if isinstance(state.get("elite_learning_system_summary"), dict) else {}
    meta_summary = state.get("meta_learning_governor") if isinstance(state.get("meta_learning_governor"), dict) else {}
    top_rows = sorted(rows, key=lambda row: (float(row.get("delta_vs_live") or 0.0), float(row.get("evidence_score") or 0.0)), reverse=True)
    fragile_rows = sorted(rows, key=lambda row: (float(row.get("fragility_score") or 0.0), -float(row.get("readiness") or 0.0)), reverse=True)
    records: list[dict[str, Any]] = []
    if kind in {"truth", "world_model", "proof_packet"}:
        for row in top_rows[:16]:
            support = float(row.get("evidence_score") or 0.0)
            risk = float(row.get("fragility_score") or 0.0)
            records.append({
                "claim_id": _stable_hash({"proof_claim": [row.get("variant"), row.get("route_key")]}),
                "route_key": row.get("route_key"),
                "variant": row.get("variant"),
                "claim": "candidate_has_live_edge_worth_testing",
                "proof_status": "supported" if support >= 65.0 and risk < 45.0 else "needs_proof" if support >= 35.0 else "weak",
                "support_score": round(support, 4),
                "risk_score": round(risk, 4),
                "signal_score": round(support + max(0.0, float(row.get("delta_vs_live") or 0.0)) / 30.0 - risk * 0.25, 4),
                "recommended_action": "build_proof_packet" if kind == "proof_packet" else "verify_or_falsify_claim",
            })
    elif kind in {"credit", "roi"}:
        modules = ["promotion_packet_executive", "elite_learning_system_summary", "scientific_hunt_executive", "active_uncertainty_learning_suite"]
        for idx, module in enumerate(modules, 1):
            top = top_rows[(idx - 1) % max(1, len(top_rows))] if top_rows else {}
            records.append({
                "module": module,
                "route_key": top.get("route_key"),
                "credit_score": round(max(0.0, float(top.get("delta_vs_live") or 0.0)) / 25.0 + 100.0 / idx, 4),
                "signal_score": round(max(0.0, float(top.get("delta_vs_live") or 0.0)) / 25.0 + 100.0 / idx, 4),
                "recommended_action": "increase_budget" if idx <= 2 else "shadow_measure",
            })
    elif kind in {"simulator", "budget_replay"}:
        actions = ["promotion_repair", "controlled_science", "novelty_shuffle", "self_falsification"]
        for idx, action in enumerate(actions):
            top = top_rows[idx % max(1, len(top_rows))] if top_rows else {}
            expected = max(0.0, float(top.get("delta_vs_live") or 0.0)) * (0.35 + idx * 0.05) + float(top.get("evidence_score") or 0.0)
            records.append({
                "scenario": action,
                "route_key": top.get("route_key"),
                "expected_learning_value": round(expected, 4),
                "signal_score": round(expected, 4),
                "recommended_action": f"simulate_{action}",
            })
    elif kind in {"ladder", "lifecycle"}:
        for row in top_rows[:20]:
            readiness = float(row.get("readiness") or 0.0)
            stage = "review_candidate" if readiness >= 80.0 else "repair" if readiness >= 55.0 else "probe"
            records.append({
                "route_key": row.get("route_key"),
                "variant": row.get("variant"),
                "stage": stage,
                "allowed_next_action": "promotion_review" if stage == "review_candidate" else "repair_candidate" if stage == "repair" else "probe_candidate",
                "signal_score": round(readiness + max(0.0, float(row.get("delta_vs_live") or 0.0)) / 40.0, 4),
                "recommended_action": "advance_candidate_stage",
            })
    elif kind in {"agenda", "falsification", "review_board"}:
        target = (elite_summary.get("top_actions") or [{}])[0]
        route = target.get("route_key") or ((top_rows or [{}])[0]).get("route_key")
        question = "What evidence would disprove the strongest current live-edge belief?"
        records.append({
            "question": question,
            "route_key": route,
            "board_vote": {"promoter": "yes", "skeptic": "test", "scientist": "control", "operator": "safe", "explorer": "challenge"} if kind == "review_board" else {},
            "signal_score": 100.0 if route else 0.0,
            "recommended_action": "run_falsification_test" if kind == "falsification" else "answer_research_question",
        })
    elif kind in {"warehouse", "regime_validity", "garbage"}:
        for row in top_rows[:12]:
            stale = float(row.get("fragility_score") or 0.0) >= 70.0
            records.append({
                "memory_id": _stable_hash({"proof_memory": [row.get("route_key"), row.get("variant")]}),
                "route_key": row.get("route_key"),
                "variant": row.get("variant"),
                "memory_status": "retire" if stale and kind == "garbage" else "valid_current_regime",
                "regime": (state.get("live_regime_shift_detector_v2") or {}).get("mode") if isinstance(state.get("live_regime_shift_detector_v2"), dict) else "unknown",
                "signal_score": round(float(row.get("evidence_score") or 0.0) - float(row.get("fragility_score") or 0.0) * 0.35, 4),
                "recommended_action": "retire_bad_memory" if stale and kind == "garbage" else "persist_validated_memory",
            })
    else:
        for row in fragile_rows[:12]:
            records.append({
                "route_key": row.get("route_key"),
                "variant": row.get("variant"),
                "signal_score": round(float(row.get("fragility_score") or 0.0) + max(0.0, 70.0 - float(row.get("readiness") or 0.0)), 4),
                "recommended_action": "proof_required",
            })
    records.sort(key=lambda row: float(row.get("signal_score") or row.get("credit_score") or row.get("expected_learning_value") or 0.0), reverse=True)
    return records


def _proof_artifact(name: str, state: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    kind, description, default_action = PROOF_LEARNING_ARTIFACT_SPECS[name]
    records = _proof_records(kind, state, rows)
    top = records[0] if records else {}
    focus = _ordered_unique([row.get("route_key") for row in records if row.get("route_key") and row.get("recommended_action") not in {"retire_bad_memory", "proof_required"}])[:12]
    repair = _ordered_unique([row.get("route_key") for row in records if row.get("route_key") and row.get("recommended_action") in {"proof_required", "run_falsification_test", "repair_candidate", "advance_candidate_stage"}])[:12]
    avoid = _ordered_unique([row.get("route_key") for row in records if row.get("route_key") and row.get("recommended_action") == "retire_bad_memory"])[:12]
    width = "tight" if kind in {"truth", "falsification", "review_board", "proof_packet", "garbage"} else "wide" if kind in {"simulator", "budget_replay"} else "medium"
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "artifact": name,
        "description": description,
        "kind": kind,
        "records": records[:25],
        "top_record": top,
        "focus_routes": [route for route in focus if route not in set(avoid)],
        "repair_routes": repair,
        "avoid_routes": avoid,
        "recommended_action": (top.get("recommended_action") if isinstance(top, dict) else None) or default_action,
        "mutation_width": width,
        "batch_size_multiplier": 0.72 if width == "tight" else 1.08 if width == "wide" else 0.95,
        "summary": {
            "record_count": len(records),
            "top_route": top.get("route_key") if isinstance(top, dict) else "",
            "top_action": (top.get("recommended_action") if isinstance(top, dict) else None) or default_action,
        },
    }


def proof_learning_system_summary(state: dict[str, Any]) -> dict[str, Any]:
    artifacts = [state.get(key) for key in PROOF_LEARNING_ARTIFACTS if isinstance(state.get(key), dict)]
    focus = _ordered_unique([route for artifact in artifacts for route in (artifact.get("focus_routes") or [])])[:12]
    repair = _ordered_unique([route for artifact in artifacts for route in (artifact.get("repair_routes") or [])])[:12]
    avoid = _ordered_unique([route for artifact in artifacts for route in (artifact.get("avoid_routes") or [])])[:12]
    top_actions = [
        {
            "artifact": artifact.get("artifact"),
            "route_key": (artifact.get("top_record") or {}).get("route_key"),
            "action": artifact.get("recommended_action"),
        }
        for artifact in artifacts
        if artifact.get("top_record")
    ][:12]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Executive summary for the proof-oriented learning layer.",
        "summary": {
            "artifact_count": len(artifacts),
            "focus_count": len(focus),
            "repair_count": len(repair),
            "avoid_count": len(avoid),
            "top_action": top_actions[0] if top_actions else {},
        },
        "focus_routes": [route for route in focus if route not in set(avoid)],
        "repair_routes": repair,
        "avoid_routes": avoid,
        "top_actions": top_actions,
    }


def proof_oriented_learning_suite(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    rows = _proof_base_rows(state)
    artifacts: dict[str, Any] = {}
    for name in PROOF_LEARNING_ARTIFACTS:
        artifacts[name] = _proof_artifact(name, {**state, **artifacts}, rows)
    artifacts["proof_learning_system_summary"] = proof_learning_system_summary({**state, **artifacts})
    return artifacts


CLOSED_LOOP_CONTROL_ARTIFACT_SPECS = {
    "bayesian_belief_engine": ("belief", "Maintains probabilistic hypotheses and updates route confidence from live edge, evidence, novelty, and fragility.", "update_belief_probability"),
    "active_experiment_selector": ("experiment", "Chooses the next experiment by expected information gain plus expected live-edge value.", "run_high_eig_experiment"),
    "uncertainty_aware_top100": ("uncertainty_rank", "Ranks live-beating candidates with confidence intervals so lucky winners do not crowd out reliable winners.", "rank_with_uncertainty"),
    "live_search_drift_detector": ("drift", "Detects stale search regions and pivots workers while the hunt is still running.", "pivot_stale_search"),
    "exploration_exploitation_governor": ("budget", "Continuously reallocates hunt budget across crazy exploration, near-winner exploitation, repair, falsification, and promotion polish.", "rebalance_explore_exploit"),
    "variant_lineage_genetics": ("lineage", "Learns which parent traits and genes repeatedly produce live-beating children.", "breed_productive_lineage"),
    "feature_interaction_miner_v2": ("interaction", "Mines route, indicator, lane, and gene combinations whose joint effect matters more than each feature alone.", "probe_feature_interaction"),
    "regret_accounting_engine": ("regret", "Estimates where hunt budget was over-spent or under-spent and converts regret into next-cycle work.", "pay_down_regret"),
    "adversarial_robustness_generator": ("adversarial", "Generates attack tests intended to break promising variants before promotion review does.", "run_adversarial_attack"),
    "hunt_curriculum_engine": ("curriculum", "Moves the hunt through discovery, clustering, exploitation, falsification, repair, and promotion-packet stages.", "advance_hunt_curriculum"),
    "meta_strategy_bandits": ("bandit", "Runs strategy-level bandits over mutation shuffling, indicator swaps, regime slicing, repair, and ensemble seeding.", "allocate_strategy_bandit"),
    "real_time_learning_dashboard_feed": ("status_feed", "Compiles each status update into learned, changed, testing-next, and risk statements.", "emit_learning_status_feed"),
}


def _closed_loop_control_rows(state: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    packets = _world_candidate_packets(state)
    for packet in packets[:100]:
        route = str(packet.get("route_key") or "")
        delta = float(packet.get("delta_vs_live") or packet.get("step2_delta_vs_active") or packet.get("delta_vs_active") or 0.0)
        evidence = float(packet.get("evidence_score") or packet.get("promotion_quality_score") or packet.get("promotion_readiness_score") or 0.0)
        readiness = float(packet.get("promotion_readiness_score") or packet.get("readiness") or evidence)
        risk = float(packet.get("predicted_reject_probability") or packet.get("overfit_risk_score") or packet.get("fragility_score") or 0.0)
        novelty = float(packet.get("novelty_score") or 0.0)
        sample = max(1.0, float(packet.get("sample_size") or packet.get("trade_count") or packet.get("evidence_count") or 1.0))
        rows.append({
            "variant": packet.get("variant"),
            "route_key": route,
            "family_key": packet.get("family_key") or route.split("|", 1)[0] if route else "",
            "parent_variant": packet.get("parent_variant") or packet.get("parent") or "",
            "mutation_lane": packet.get("mutation_lane") or packet.get("lane") or "unknown",
            "indicator": packet.get("indicator") or packet.get("indicator_family") or "",
            "delta_vs_live": delta,
            "evidence_score": evidence,
            "readiness": readiness,
            "risk_score": risk,
            "novelty_score": novelty,
            "sample_size": sample,
            "gene_tokens": packet.get("gene_tokens") or _row_gene_tokens(packet),
        })
    rows.sort(key=lambda row: (float(row.get("delta_vs_live") or 0.0), float(row.get("evidence_score") or 0.0)), reverse=True)
    return rows


def _closed_loop_strategy_rows(state: dict[str, Any], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    lanes: dict[str, dict[str, Any]] = {}
    for row in rows:
        lane = str(row.get("mutation_lane") or "unknown")
        lane_row = lanes.setdefault(lane, {"strategy": lane, "attempts": 0, "live_edge": 0.0, "routes": []})
        lane_row["attempts"] = int(lane_row.get("attempts") or 0) + 1
        lane_row["live_edge"] = float(lane_row.get("live_edge") or 0.0) + max(0.0, float(row.get("delta_vs_live") or 0.0))
        if row.get("route_key"):
            lane_row["routes"].append(row.get("route_key"))
    if not lanes:
        lanes = {
            "wild_indicator_shuffle": {"strategy": "wild_indicator_shuffle", "attempts": 0, "live_edge": 0.0, "routes": []},
            "promotion_repair": {"strategy": "promotion_repair", "attempts": 0, "live_edge": 0.0, "routes": []},
            "regime_slice": {"strategy": "regime_slice", "attempts": 0, "live_edge": 0.0, "routes": []},
        }
    strategy_rows = []
    for lane, lane_row in lanes.items():
        attempts = max(1, int(lane_row.get("attempts") or 0))
        avg = float(lane_row.get("live_edge") or 0.0) / attempts
        exploration_bonus = 20.0 / (attempts ** 0.5)
        strategy_rows.append({
            "strategy": lane,
            "attempts": attempts,
            "avg_live_edge": round(avg, 4),
            "bandit_score": round(avg * 0.35 + exploration_bonus, 4),
            "focus_routes": _ordered_unique(lane_row.get("routes") or [])[:6],
        })
    strategy_rows.sort(key=lambda row: float(row.get("bandit_score") or 0.0), reverse=True)
    return strategy_rows


def _closed_loop_control_records(kind: str, state: dict[str, Any], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    top_rows = rows[:25]
    if kind == "belief":
        for row in top_rows:
            delta = max(0.0, float(row.get("delta_vs_live") or 0.0))
            evidence = float(row.get("evidence_score") or 0.0)
            risk = float(row.get("risk_score") or 0.0)
            novelty = float(row.get("novelty_score") or 0.0)
            posterior = max(0.03, min(0.97, 0.24 + evidence / 180.0 + min(delta, 2000.0) / 5000.0 + novelty / 260.0 - risk / 170.0))
            records.append({
                "hypothesis_id": _stable_hash({"bayes": row.get("variant"), "route": row.get("route_key")}),
                "hypothesis": "route_has_repeatable_live_edge",
                "route_key": row.get("route_key"),
                "variant": row.get("variant"),
                "posterior_probability": round(posterior, 4),
                "confidence": round(1.0 - min(0.92, risk / 120.0), 4),
                "signal_score": round(posterior * 100.0 + delta / 40.0, 4),
                "recommended_action": "scale_belief_test" if posterior >= 0.65 else "collect_more_evidence",
            })
    elif kind == "experiment":
        heat = {str(cell.get("route_key") or ""): float(cell.get("uncertainty_score") or 0.0) for cell in ((state.get("uncertainty_heatmap") or {}).get("cells") or []) if isinstance(cell, dict)}
        for row in top_rows:
            route = str(row.get("route_key") or "")
            uncertainty = heat.get(route, max(5.0, 80.0 - float(row.get("readiness") or 0.0)))
            eig = uncertainty * 0.55 + max(0.0, float(row.get("delta_vs_live") or 0.0)) / 35.0 + float(row.get("novelty_score") or 0.0) * 0.35
            records.append({
                "experiment_id": _stable_hash({"active_selector": route, "variant": row.get("variant")}),
                "selected_experiment": "controlled_trait_shuffle",
                "route_key": route,
                "variant": row.get("variant"),
                "expected_information_gain": round(eig, 4),
                "signal_score": round(eig, 4),
                "recommended_action": "run_high_eig_experiment",
            })
    elif kind == "uncertainty_rank":
        for idx, row in enumerate(top_rows, 1):
            delta = float(row.get("delta_vs_live") or 0.0)
            risk = float(row.get("risk_score") or 0.0)
            n = max(1.0, float(row.get("sample_size") or 1.0))
            half_width = max(25.0, (risk + 40.0) * (1.0 / (n ** 0.5)))
            lower = delta - half_width
            records.append({
                "rank": idx,
                "route_key": row.get("route_key"),
                "variant": row.get("variant"),
                "delta_vs_live": round(delta, 4),
                "confidence_interval": [round(lower, 4), round(delta + half_width, 4)],
                "uncertainty_penalty": round(half_width, 4),
                "signal_score": round(lower + float(row.get("evidence_score") or 0.0) * 0.4, 4),
                "recommended_action": "rank_with_uncertainty" if lower > 0 else "repair_uncertain_winner",
            })
    elif kind == "drift":
        current = {str(row.get("route_key") or "") for row in rows[:20] if row.get("route_key")}
        previous = set((state.get("runtime_command_adapter") or {}).get("focus_routes") or [])
        overlap = len(current & previous) / max(1, len(current | previous))
        drift_score = 1.0 - overlap
        stale = sorted(previous - current)
        route = (stale or sorted(current) or [""])[0]
        records.append({
            "route_key": route,
            "drift_status": "pivot_now" if drift_score >= 0.68 else "watch" if drift_score >= 0.38 else "stable",
            "drift_score": round(drift_score, 4),
            "stale_routes": stale[:12],
            "signal_score": round(drift_score * 100.0, 4),
            "recommended_action": "pivot_stale_search" if drift_score >= 0.38 else "continue_current_search",
        })
    elif kind == "budget":
        live_count = len([row for row in rows if float(row.get("delta_vs_live") or 0.0) > 0.0])
        fragile_count = len([row for row in rows[:25] if float(row.get("risk_score") or 0.0) >= 55.0])
        explore = 20.0 if live_count >= 8 else 38.0
        repair = min(35.0, fragile_count * 4.0 + 12.0)
        falsify = 12.0 if live_count else 20.0
        exploit = max(18.0, 100.0 - explore - repair - falsify - 10.0)
        records.append({
            "budget_mix": {
                "crazy_exploration_pct": round(explore, 2),
                "near_winner_exploitation_pct": round(exploit, 2),
                "robustness_repair_pct": round(repair, 2),
                "self_falsification_pct": round(falsify, 2),
                "promotion_polish_pct": 10.0,
            },
            "route_key": (rows[0].get("route_key") if rows else ""),
            "signal_score": round(explore + exploit + repair, 4),
            "recommended_action": "rebalance_explore_exploit",
        })
    elif kind == "lineage":
        families: dict[str, dict[str, Any]] = {}
        for row in rows:
            family = str(row.get("family_key") or row.get("route_key") or "unknown")
            item = families.setdefault(family, {"family_key": family, "children": 0, "edge": 0.0, "genes": [], "routes": []})
            item["children"] = int(item.get("children") or 0) + 1
            item["edge"] = float(item.get("edge") or 0.0) + max(0.0, float(row.get("delta_vs_live") or 0.0))
            item["genes"].extend(row.get("gene_tokens") or [])
            if row.get("route_key"):
                item["routes"].append(row.get("route_key"))
        for item in families.values():
            genes = _ordered_unique(item.get("genes") or [])[:8]
            records.append({
                "family_key": item.get("family_key"),
                "route_key": ((item.get("routes") or [""])[0] or ""),
                "children": item.get("children"),
                "productive_genes": genes,
                "lineage_edge": round(float(item.get("edge") or 0.0), 4),
                "signal_score": round(float(item.get("edge") or 0.0) + len(genes) * 5.0, 4),
                "recommended_action": "breed_productive_lineage",
            })
    elif kind == "interaction":
        pairs: dict[tuple[str, str], dict[str, Any]] = {}
        for row in rows:
            genes = _ordered_unique([str(g) for g in (row.get("gene_tokens") or []) if str(g)])[:8]
            route = row.get("route_key")
            for left_idx, left in enumerate(genes):
                for right in genes[left_idx + 1:]:
                    key = tuple(sorted((left, right)))
                    item = pairs.setdefault(key, {"features": list(key), "edge": 0.0, "routes": []})
                    item["edge"] = float(item.get("edge") or 0.0) + max(0.0, float(row.get("delta_vs_live") or 0.0))
                    if route:
                        item["routes"].append(route)
        for item in pairs.values():
            records.append({
                "interaction": item.get("features"),
                "route_key": ((item.get("routes") or [""])[0] or ""),
                "joint_edge": round(float(item.get("edge") or 0.0), 4),
                "supporting_routes": _ordered_unique(item.get("routes") or [])[:6],
                "signal_score": round(float(item.get("edge") or 0.0), 4),
                "recommended_action": "probe_feature_interaction",
            })
    elif kind == "regret":
        focus = set((state.get("runtime_command_adapter") or {}).get("focus_routes") or [])
        for row in top_rows:
            route = str(row.get("route_key") or "")
            omitted = route and route not in focus
            risk = float(row.get("risk_score") or 0.0)
            regret = max(0.0, float(row.get("delta_vs_live") or 0.0)) / (25.0 if omitted else 60.0) + max(0.0, risk - 60.0)
            records.append({
                "route_key": route,
                "decision": "under_invested_live_beater" if omitted else "monitor_overfit_spend",
                "regret_score": round(regret, 4),
                "signal_score": round(regret, 4),
                "recommended_action": "pay_down_regret" if omitted else "measure_regret",
            })
    elif kind == "adversarial":
        for row in top_rows[:16]:
            risk = float(row.get("risk_score") or 0.0)
            records.append({
                "attack_id": _stable_hash({"attack": row.get("variant"), "route": row.get("route_key")}),
                "route_key": row.get("route_key"),
                "variant": row.get("variant"),
                "attack": "holdout_day_shuffle_and_cost_stress" if risk >= 45.0 else "sibling_control_and_session_flip",
                "kill_criterion": "edge disappears vs live under stricter cost/day controls",
                "signal_score": round(risk + max(0.0, float(row.get("delta_vs_live") or 0.0)) / 50.0, 4),
                "recommended_action": "run_adversarial_attack",
            })
    elif kind == "curriculum":
        live_count = len([row for row in rows if float(row.get("delta_vs_live") or 0.0) > 0.0])
        fragile_count = len([row for row in rows[:20] if float(row.get("risk_score") or 0.0) >= 55.0])
        phase = "broad_discovery" if live_count < 3 else "trait_clustering" if live_count < 8 else "falsification_repair" if fragile_count else "promotion_packet_generation"
        records.append({
            "curriculum_phase": phase,
            "route_key": (rows[0].get("route_key") if rows else ""),
            "stage_order": ["broad_discovery", "trait_clustering", "focused_exploitation", "falsification_repair", "promotion_packet_generation"],
            "next_stage": "focused_exploitation" if phase == "trait_clustering" else "falsification_repair" if phase == "focused_exploitation" else phase,
            "signal_score": 100.0 if rows else 0.0,
            "recommended_action": "advance_hunt_curriculum",
        })
    elif kind == "bandit":
        for strategy in _closed_loop_strategy_rows(state, rows):
            records.append({
                "strategy": strategy.get("strategy"),
                "route_key": ((strategy.get("focus_routes") or [""])[0] or ""),
                "bandit_score": strategy.get("bandit_score"),
                "allocation_pct": round(max(8.0, min(45.0, float(strategy.get("bandit_score") or 0.0))), 2),
                "signal_score": strategy.get("bandit_score"),
                "recommended_action": "allocate_strategy_bandit",
            })
    elif kind == "status_feed":
        belief = state.get("bayesian_belief_engine") if isinstance(state.get("bayesian_belief_engine"), dict) else {}
        selector = state.get("active_experiment_selector") if isinstance(state.get("active_experiment_selector"), dict) else {}
        governor = state.get("exploration_exploitation_governor") if isinstance(state.get("exploration_exploitation_governor"), dict) else {}
        records.append({
            "route_key": ((selector.get("top_record") or {}).get("route_key") or (rows[0].get("route_key") if rows else "")),
            "learned": f"top belief posterior={(belief.get('top_record') or {}).get('posterior_probability', 'n/a')}",
            "changed": f"budget mix={(governor.get('top_record') or {}).get('budget_mix', {})}",
            "testing_next": (selector.get("top_record") or {}).get("selected_experiment") or "high_information_probe",
            "risk": (state.get("live_search_drift_detector") or {}).get("top_record") or {},
            "signal_score": 100.0 if rows else 0.0,
            "recommended_action": "emit_learning_status_feed",
        })
    records.sort(key=lambda row: float(row.get("signal_score") or row.get("bandit_score") or row.get("regret_score") or 0.0), reverse=True)
    return records


def _closed_loop_control_artifact(name: str, state: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    kind, description, default_action = CLOSED_LOOP_CONTROL_ARTIFACT_SPECS[name]
    records = _closed_loop_control_records(kind, state, rows)
    top = records[0] if records else {}
    avoid_actions = {"pivot_stale_search"} if kind == "drift" else set()
    focus = _ordered_unique([row.get("route_key") for row in records if row.get("route_key") and row.get("recommended_action") not in avoid_actions])[:12]
    repair = _ordered_unique([row.get("route_key") for row in records if row.get("route_key") and row.get("recommended_action") in {"repair_uncertain_winner", "run_adversarial_attack", "pay_down_regret", "collect_more_evidence"}])[:12]
    avoid = _ordered_unique([route for row in records for route in (row.get("stale_routes") or [])])[:12]
    width = "wide" if kind in {"experiment", "budget", "bandit", "curriculum"} else "tight" if kind in {"uncertainty_rank", "adversarial", "belief"} else "medium"
    batch = 1.12 if width == "wide" else 0.78 if width == "tight" else 0.94
    if kind == "drift" and (top.get("drift_status") == "pivot_now"):
        width = "wide"
        batch = 0.88
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "artifact": name,
        "description": description,
        "kind": kind,
        "records": records[:50],
        "top_record": top,
        "focus_routes": [route for route in focus if route not in set(avoid)],
        "repair_routes": repair,
        "avoid_routes": avoid,
        "recommended_action": (top.get("recommended_action") if isinstance(top, dict) else None) or default_action,
        "mutation_width": width,
        "batch_size_multiplier": batch,
        "summary": {
            "record_count": len(records),
            "top_route": top.get("route_key") if isinstance(top, dict) else "",
            "top_action": (top.get("recommended_action") if isinstance(top, dict) else None) or default_action,
        },
    }


def closed_loop_control_learning_summary(state: dict[str, Any]) -> dict[str, Any]:
    artifacts = [state.get(key) for key in CLOSED_LOOP_CONTROL_ARTIFACTS if isinstance(state.get(key), dict)]
    focus = _ordered_unique([route for artifact in artifacts for route in (artifact.get("focus_routes") or [])])[:12]
    repair = _ordered_unique([route for artifact in artifacts for route in (artifact.get("repair_routes") or [])])[:12]
    avoid = _ordered_unique([route for artifact in artifacts for route in (artifact.get("avoid_routes") or [])])[:12]
    top_actions = [
        {
            "artifact": artifact.get("artifact"),
            "route_key": (artifact.get("top_record") or {}).get("route_key"),
            "action": artifact.get("recommended_action"),
        }
        for artifact in artifacts
        if artifact.get("top_record")
    ][:12]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Executive summary for the closed-loop scientific control layer.",
        "summary": {
            "artifact_count": len(artifacts),
            "focus_count": len(focus),
            "repair_count": len(repair),
            "avoid_count": len(avoid),
            "top_action": top_actions[0] if top_actions else {},
        },
        "focus_routes": [route for route in focus if route not in set(avoid)],
        "repair_routes": repair,
        "avoid_routes": avoid,
        "top_actions": top_actions,
        "status_feed": (state.get("real_time_learning_dashboard_feed") or {}).get("records") or [],
    }


def scientific_control_learning_suite(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    rows = _closed_loop_control_rows(state)
    artifacts: dict[str, Any] = {}
    for name in CLOSED_LOOP_CONTROL_ARTIFACTS:
        artifacts[name] = _closed_loop_control_artifact(name, {**state, **artifacts}, rows)
    artifacts["closed_loop_control_learning_summary"] = closed_loop_control_learning_summary({**state, **artifacts})
    return artifacts


def _nervous_system_kind(name: str) -> str:
    if name.startswith(("hypothesis_", "belief_", "evidence_", "statistical_", "minimum_", "false_positive_", "candidate_luck", "live_beater_")):
        return "truth"
    if name.startswith(("trait_", "parent_", "mutation_", "gene_", "dead_gene", "rare_gene", "family_", "lineage_", "candidate_clone")):
        return "genetics"
    if name.startswith("worker_"):
        return "worker"
    if name.startswith("route_") or name == "multi_armed_route_portfolio":
        return "route"
    if name.startswith("indicator_"):
        return "indicator"
    if name.startswith("regime_"):
        return "regime"
    if name.startswith("promotion_"):
        return "promotion"
    return "hunt_objective"


def _nervous_system_action(kind: str, name: str) -> str:
    action_by_kind = {
        "truth": "tighten_truth_evidence",
        "genetics": "breed_and_prune_variant_genes",
        "worker": "rebalance_worker_learning_role",
        "route": "rebalance_route_portfolio",
        "indicator": "shuffle_indicator_with_causal_guardrails",
        "regime": "condition_search_on_regime",
        "promotion": "repair_promotion_survival_evidence",
        "hunt_objective": "optimize_hunt_learning_objective",
    }
    if "resurrection" in name:
        return "resurrect_route_or_gene"
    if "retirement" in name or "dead_gene" in name or "firewall" in name or "veto" in name:
        return "evaluate_suppression_risk"
    if "ablation" in name or "replay" in name or "simulator" in name:
        return "run_counterfactual_replay"
    if "agenda" in name:
        return "answer_next_research_question"
    return action_by_kind.get(kind, "optimize_hunt_learning_objective")


def _nervous_system_description(name: str, kind: str) -> str:
    readable = name.replace("_", " ")
    return f"World-model nervous-system artifact for {readable}; updates {kind} learning while the hunt is running."


def _nervous_records(kind: str, name: str, state: dict[str, Any], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    top_rows = rows[:30]
    records: list[dict[str, Any]] = []
    base_action = _nervous_system_action(kind, name)
    if kind == "truth":
        for row in top_rows:
            delta = float(row.get("delta_vs_live") or 0.0)
            evidence = float(row.get("evidence_score") or 0.0)
            risk = float(row.get("risk_score") or 0.0)
            n = max(1.0, float(row.get("sample_size") or 1.0))
            uncertainty = (risk + 40.0) / (n ** 0.5)
            power = max(0.0, min(1.0, (evidence + max(0.0, delta) / 20.0) / 120.0))
            records.append({
                "route_key": row.get("route_key"),
                "variant": row.get("variant"),
                "truth_score": round(evidence + max(0.0, delta) / 30.0 - risk * 0.25, 4),
                "confidence_interval": [round(delta - uncertainty, 4), round(delta + uncertainty, 4)],
                "power_estimate": round(power, 4),
                "evidence_gap": round(max(0.0, 0.72 - power) * 100.0, 4),
                "signal_score": round(evidence + max(0.0, delta) / 28.0 + (1.0 - power) * 20.0, 4),
                "recommended_action": base_action,
            })
    elif kind == "genetics":
        gene_scores: dict[str, dict[str, Any]] = {}
        for row in rows:
            for gene in (row.get("gene_tokens") or [])[:10]:
                key = str(gene)
                item = gene_scores.setdefault(key, {"gene": key, "edge": 0.0, "risk": 0.0, "routes": [], "count": 0})
                item["edge"] = float(item.get("edge") or 0.0) + max(0.0, float(row.get("delta_vs_live") or 0.0))
                item["risk"] = float(item.get("risk") or 0.0) + float(row.get("risk_score") or 0.0)
                item["count"] = int(item.get("count") or 0) + 1
                if row.get("route_key"):
                    item["routes"].append(row.get("route_key"))
        for item in gene_scores.values():
            count = max(1, int(item.get("count") or 0))
            avg_risk = float(item.get("risk") or 0.0) / count
            score = float(item.get("edge") or 0.0) / count - avg_risk * 0.2
            records.append({
                "gene": item.get("gene"),
                "route_key": ((item.get("routes") or [""])[0] or ""),
                "edge_per_child": round(float(item.get("edge") or 0.0) / count, 4),
                "risk_per_child": round(avg_risk, 4),
                "supporting_routes": _ordered_unique(item.get("routes") or [])[:8],
                "signal_score": round(score, 4),
                "recommended_action": "suppress_or_retire_risk" if avg_risk >= 70.0 else base_action,
            })
    elif kind == "worker":
        worker_rows = []
        cards = (state.get("worker_learning_report_cards") or {}).get("cards") if isinstance(state.get("worker_learning_report_cards"), dict) else []
        for idx in range(4):
            card = cards[idx] if idx < len(cards or []) and isinstance(cards[idx], dict) else {}
            focus = (state.get("runtime_command_adapter") or {}).get("focus_routes") or []
            wins = int(card.get("wins") or card.get("successful_commands") or 0)
            losses = int(card.get("losses") or card.get("failed_commands") or 0)
            worker_rows.append({
                "worker": card.get("worker") or f"worker_{idx + 1}",
                "route_key": (focus[idx % max(1, len(focus))] if focus else (top_rows[idx % max(1, len(top_rows))].get("route_key") if top_rows else "")),
                "role": ["explorer", "exploiter", "skeptic", "promotion_repair"][idx],
                "skill_score": round(50.0 + wins * 12.0 - losses * 9.0 + idx, 4),
                "signal_score": round(50.0 + wins * 12.0 - losses * 9.0 + idx, 4),
                "recommended_action": base_action,
            })
        records.extend(worker_rows)
    elif kind in {"route", "regime"}:
        route_scores: dict[str, dict[str, Any]] = {}
        for row in rows:
            route = str(row.get("route_key") or "")
            if not route:
                continue
            item = route_scores.setdefault(route, {"route_key": route, "edge": 0.0, "risk": 0.0, "novelty": 0.0, "count": 0})
            item["edge"] = float(item.get("edge") or 0.0) + max(0.0, float(row.get("delta_vs_live") or 0.0))
            item["risk"] = float(item.get("risk") or 0.0) + float(row.get("risk_score") or 0.0)
            item["novelty"] = float(item.get("novelty") or 0.0) + float(row.get("novelty_score") or 0.0)
            item["count"] = int(item.get("count") or 0) + 1
        for item in route_scores.values():
            count = max(1, int(item.get("count") or 0))
            risk = float(item.get("risk") or 0.0) / count
            score = float(item.get("edge") or 0.0) / count + float(item.get("novelty") or 0.0) / count - risk * 0.2
            records.append({
                "route_key": item.get("route_key"),
                "route_edge": round(float(item.get("edge") or 0.0) / count, 4),
                "route_risk": round(risk, 4),
                "sample_count": count,
                "regime": (state.get("temporal_regime_fingerprinting") or {}).get("top_fingerprint", {}).get("fingerprint") if isinstance(state.get("temporal_regime_fingerprinting"), dict) else "unknown",
                "signal_score": round(score, 4),
                "recommended_action": "suppress_or_retire_risk" if risk >= 75.0 and ("retirement" in name or "firewall" in name or "veto" in name) else base_action,
            })
    elif kind == "indicator":
        indicator_scores: dict[str, dict[str, Any]] = {}
        for row in rows:
            indicator = str(row.get("indicator") or (row.get("gene_tokens") or ["unknown"])[0] or "unknown")
            item = indicator_scores.setdefault(indicator, {"indicator": indicator, "edge": 0.0, "risk": 0.0, "routes": [], "count": 0})
            item["edge"] = float(item.get("edge") or 0.0) + max(0.0, float(row.get("delta_vs_live") or 0.0))
            item["risk"] = float(item.get("risk") or 0.0) + float(row.get("risk_score") or 0.0)
            item["count"] = int(item.get("count") or 0) + 1
            if row.get("route_key"):
                item["routes"].append(row.get("route_key"))
        for item in indicator_scores.values():
            count = max(1, int(item.get("count") or 0))
            risk = float(item.get("risk") or 0.0) / count
            score = float(item.get("edge") or 0.0) / count - risk * 0.18
            records.append({
                "indicator": item.get("indicator"),
                "route_key": ((item.get("routes") or [""])[0] or ""),
                "indicator_edge": round(float(item.get("edge") or 0.0) / count, 4),
                "indicator_risk": round(risk, 4),
                "supporting_routes": _ordered_unique(item.get("routes") or [])[:8],
                "signal_score": round(score, 4),
                "recommended_action": "suppress_or_retire_risk" if risk >= 72.0 and ("fragility" in name or "conflict" in name or "decay" in name) else base_action,
            })
    elif kind == "promotion":
        for row in top_rows:
            readiness = float(row.get("readiness") or 0.0)
            risk = float(row.get("risk_score") or 0.0)
            delta = max(0.0, float(row.get("delta_vs_live") or 0.0))
            survival = max(0.02, min(0.98, readiness / 120.0 + delta / 6500.0 - risk / 180.0))
            records.append({
                "route_key": row.get("route_key"),
                "variant": row.get("variant"),
                "promotion_survival_probability": round(survival, 4),
                "missing_evidence_score": round(max(0.0, 78.0 - readiness) + risk * 0.25, 4),
                "signal_score": round(survival * 100.0 + delta / 45.0, 4),
                "recommended_action": base_action,
            })
    else:
        control = state.get("closed_loop_control_learning_summary") if isinstance(state.get("closed_loop_control_learning_summary"), dict) else {}
        top_action = (control.get("top_actions") or [{}])[0]
        route = top_action.get("route_key") or (top_rows[0].get("route_key") if top_rows else "")
        records.append({
            "route_key": route,
            "objective": name,
            "learning_value": round(100.0 if route else 0.0, 4),
            "top_control_action": top_action,
            "signal_score": 100.0 if route else 0.0,
            "recommended_action": base_action,
        })
    records.sort(key=lambda row: float(row.get("signal_score") or row.get("learning_value") or 0.0), reverse=True)
    return records


def _nervous_system_artifact(name: str, state: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    kind = _nervous_system_kind(name)
    records = _nervous_records(kind, name, state, rows)
    top = records[0] if records else {}
    focus = _ordered_unique([row.get("route_key") for row in records if row.get("route_key") and row.get("recommended_action") not in {"suppress_or_retire_risk"}])[:12]
    repair = _ordered_unique([row.get("route_key") for row in records if row.get("route_key") and row.get("recommended_action") in {"tighten_truth_evidence", "repair_promotion_survival_evidence", "run_counterfactual_replay"}])[:12]
    avoid = _ordered_unique([row.get("route_key") for row in records if row.get("route_key") and row.get("recommended_action") == "suppress_or_retire_risk"])[:12]
    width = "tight" if kind in {"truth", "promotion"} or "firewall" in name or "veto" in name else "wide" if kind in {"genetics", "indicator", "hunt_objective"} or "exploration" in name else "medium"
    batch = 0.76 if width == "tight" else 1.1 if width == "wide" else 0.94
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "artifact": name,
        "description": _nervous_system_description(name, kind),
        "kind": kind,
        "records": records[:40],
        "top_record": top,
        "focus_routes": [route for route in focus if route not in set(avoid)],
        "repair_routes": repair,
        "avoid_routes": avoid,
        "recommended_action": (top.get("recommended_action") if isinstance(top, dict) else None) or _nervous_system_action(kind, name),
        "mutation_width": width,
        "batch_size_multiplier": batch,
        "summary": {
            "record_count": len(records),
            "top_route": top.get("route_key") if isinstance(top, dict) else "",
            "top_action": (top.get("recommended_action") if isinstance(top, dict) else None) or _nervous_system_action(kind, name),
        },
    }


def world_model_nervous_system_summary(state: dict[str, Any]) -> dict[str, Any]:
    artifacts = [state.get(key) for key in WORLD_MODEL_NERVOUS_SYSTEM_ARTIFACTS if isinstance(state.get(key), dict)]
    focus = _ordered_unique([route for artifact in artifacts for route in (artifact.get("focus_routes") or [])])[:16]
    repair = _ordered_unique([route for artifact in artifacts for route in (artifact.get("repair_routes") or [])])[:16]
    avoid = _ordered_unique([route for artifact in artifacts for route in (artifact.get("avoid_routes") or [])])[:16]
    top_actions = [
        {
            "artifact": artifact.get("artifact"),
            "kind": artifact.get("kind"),
            "route_key": (artifact.get("top_record") or {}).get("route_key"),
            "action": artifact.get("recommended_action"),
        }
        for artifact in artifacts
        if artifact.get("top_record")
    ][:16]
    kind_counts: dict[str, int] = {}
    for artifact in artifacts:
        kind = str(artifact.get("kind") or "unknown")
        kind_counts[kind] = kind_counts.get(kind, 0) + 1
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Executive summary for the 80-artifact world-model nervous system.",
        "summary": {
            "artifact_count": len(artifacts),
            "kind_counts": kind_counts,
            "focus_count": len(focus),
            "repair_count": len(repair),
            "avoid_count": len(avoid),
            "top_action": top_actions[0] if top_actions else {},
        },
        "focus_routes": [route for route in focus if route not in set(avoid)],
        "repair_routes": repair,
        "avoid_routes": avoid,
        "top_actions": top_actions,
    }


def world_model_nervous_system_suite(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    rows = _closed_loop_control_rows(state)
    artifacts: dict[str, Any] = {}
    for name in WORLD_MODEL_NERVOUS_SYSTEM_ARTIFACTS:
        artifacts[name] = _nervous_system_artifact(name, {**state, **artifacts}, rows)
    artifacts["world_model_nervous_system_summary"] = world_model_nervous_system_summary({**state, **artifacts})
    return artifacts


def _orchestration_kind(name: str) -> str:
    if name.startswith("artifact_") or name.startswith("unified_artifact"):
        return "artifact_governance"
    if name.startswith("belief_"):
        return "belief_governance"
    if name.startswith("experiment_"):
        return "experiment_orchestration"
    if name.startswith("worker_"):
        return "worker_orchestration"
    if name.startswith("search_"):
        return "search_orchestration"
    if name.startswith("promotion_"):
        return "promotion_orchestration"
    if name.startswith("memory_"):
        return "memory_orchestration"
    return "hunt_executive"


def _orchestration_action(kind: str, name: str) -> str:
    if "conflict" in name or "contradiction" in name or "disagreement" in name:
        return "resolve_conflict_before_spend"
    if "retirement" in name or "stale" in name or "expiry" in name:
        return "evaluate_staleness_for_quarantine"
    if "proof" in name or "promotion" in name:
        return "raise_promotion_proof_quality"
    if "ablation" in name or "replay" in name or "dry_run" in name:
        return "run_shadow_replay"
    if "budget" in name or "auction" in name or "bounty" in name:
        return "rebalance_hunt_budget"
    return {
        "artifact_governance": "govern_learning_artifacts",
        "belief_governance": "govern_belief_lifecycle",
        "experiment_orchestration": "schedule_best_experiment",
        "worker_orchestration": "assign_best_worker_role",
        "search_orchestration": "reshape_search_topology",
        "promotion_orchestration": "raise_promotion_proof_quality",
        "memory_orchestration": "improve_memory_quality",
        "hunt_executive": "execute_hunt_control_decision",
    }.get(kind, "execute_hunt_control_decision")


def _orchestration_description(name: str, kind: str) -> str:
    return f"Orchestration artifact for {name.replace('_', ' ')}; governs {kind} decisions during live hunts."


def _orchestration_records(kind: str, name: str, state: dict[str, Any], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    top_rows = rows[:30]
    action = _orchestration_action(kind, name)
    records: list[dict[str, Any]] = []
    summaries = [
        state.get("elite_learning_system_summary") if isinstance(state.get("elite_learning_system_summary"), dict) else {},
        state.get("proof_learning_system_summary") if isinstance(state.get("proof_learning_system_summary"), dict) else {},
        state.get("closed_loop_control_learning_summary") if isinstance(state.get("closed_loop_control_learning_summary"), dict) else {},
        state.get("world_model_nervous_system_summary") if isinstance(state.get("world_model_nervous_system_summary"), dict) else {},
    ]
    summary_focus = _ordered_unique([route for summary in summaries for route in (summary.get("focus_routes") or [])])
    summary_avoid = _ordered_unique([route for summary in summaries for route in (summary.get("avoid_routes") or [])])
    if kind == "artifact_governance":
        artifacts = []
        for key in (
            list(globals().get("ELITE_LEARNING_ARTIFACTS", ()))[:8]
            + list(globals().get("PROOF_LEARNING_ARTIFACTS", ()))[:6]
            + list(globals().get("CLOSED_LOOP_CONTROL_ARTIFACTS", ()))[:6]
            + list(globals().get("WORLD_MODEL_NERVOUS_SYSTEM_ARTIFACTS", ()))[:10]
        ):
            artifact = state.get(key) if isinstance(state.get(key), dict) else {}
            if not artifact:
                continue
            focus_count = len(artifact.get("focus_routes") or [])
            avoid_count = len(artifact.get("avoid_routes") or [])
            record_count = len(artifact.get("records") or [])
            trust = 50.0 + focus_count * 5.0 + min(record_count, 20) - avoid_count * 3.0
            records.append({
                "artifact_name": key,
                "route_key": ((artifact.get("focus_routes") or artifact.get("repair_routes") or [""])[0] or ""),
                "trust_score": round(trust, 4),
                "record_count": record_count,
                "conflict_count": avoid_count,
                "signal_score": round(trust, 4),
                "recommended_action": action,
            })
    elif kind == "belief_governance":
        belief = state.get("bayesian_belief_engine") if isinstance(state.get("bayesian_belief_engine"), dict) else {}
        source_records = belief.get("records") or []
        for record in source_records[:20]:
            posterior = float(record.get("posterior_probability") or record.get("confidence") or 0.0)
            route = str(record.get("route_key") or "")
            contradiction = route in set(summary_avoid)
            score = posterior * 100.0 - (25.0 if contradiction else 0.0)
            records.append({
                "belief_id": record.get("hypothesis_id") or _stable_hash({"belief": route, "name": name}),
                "route_key": route,
                "posterior_probability": round(posterior, 4),
                "contradiction": contradiction,
                "signal_score": round(score, 4),
                "recommended_action": "resolve_conflict_before_spend" if contradiction else action,
            })
    elif kind == "experiment_orchestration":
        selector = state.get("active_experiment_selector") if isinstance(state.get("active_experiment_selector"), dict) else {}
        for record in (selector.get("records") or [])[:20]:
            eig = float(record.get("expected_information_gain") or record.get("signal_score") or 0.0)
            route = str(record.get("route_key") or "")
            interference = route in set(summary_avoid)
            records.append({
                "experiment_id": record.get("experiment_id") or _stable_hash({"experiment": route, "name": name}),
                "route_key": route,
                "expected_value": round(eig, 4),
                "interference_risk": interference,
                "signal_score": round(eig - (30.0 if interference else 0.0), 4),
                "recommended_action": "resolve_conflict_before_spend" if interference else action,
            })
    elif kind == "worker_orchestration":
        focus = summary_focus or [row.get("route_key") for row in top_rows if row.get("route_key")]
        for idx in range(4):
            route = (focus[idx % max(1, len(focus))] if focus else "")
            role = ["artifact_governor", "belief_skeptic", "experiment_operator", "promotion_closer"][idx]
            score = 80.0 - idx * 4.0 + (8.0 if route else 0.0)
            records.append({
                "worker": f"worker_{idx + 1}",
                "route_key": route,
                "role": role,
                "task_fit_score": round(score, 4),
                "signal_score": round(score, 4),
                "recommended_action": action,
            })
    elif kind == "search_orchestration":
        for row in top_rows:
            route = str(row.get("route_key") or "")
            novelty = float(row.get("novelty_score") or 0.0)
            edge = max(0.0, float(row.get("delta_vs_live") or 0.0))
            risk = float(row.get("risk_score") or 0.0)
            saturation = 100.0 - novelty + risk * 0.2
            records.append({
                "route_key": route,
                "search_cell": route or row.get("family_key") or "unknown",
                "novelty_score": round(novelty, 4),
                "saturation_score": round(saturation, 4),
                "signal_score": round(edge / 35.0 + novelty - risk * 0.15, 4),
                "recommended_action": "rebalance_hunt_budget" if "bounty" in name else action,
            })
    elif kind == "promotion_orchestration":
        for row in top_rows:
            readiness = float(row.get("readiness") or 0.0)
            risk = float(row.get("risk_score") or 0.0)
            edge = max(0.0, float(row.get("delta_vs_live") or 0.0))
            burden = max(0.0, 85.0 - readiness) + risk * 0.35
            records.append({
                "route_key": row.get("route_key"),
                "variant": row.get("variant"),
                "proof_burden": round(burden, 4),
                "economic_value": round(edge * max(0.1, readiness / 100.0), 4),
                "signal_score": round(edge / 40.0 + readiness - burden * 0.25, 4),
                "recommended_action": action,
            })
    elif kind == "memory_orchestration":
        for row in top_rows:
            risk = float(row.get("risk_score") or 0.0)
            evidence = float(row.get("evidence_score") or 0.0)
            route = str(row.get("route_key") or "")
            entropy = max(0.0, 100.0 - evidence + risk)
            stale = route in set(summary_avoid)
            records.append({
                "route_key": route,
                "memory_entropy": round(entropy, 4),
                "stale_prior": stale,
                "lesson_value": round(evidence + max(0.0, float(row.get("delta_vs_live") or 0.0)) / 35.0, 4),
                "signal_score": round(evidence - risk * 0.2 + (20.0 if stale else 0.0), 4),
                "recommended_action": "quarantine_stale_learning" if stale else action,
            })
    else:
        executive = state.get("real_time_learning_dashboard_feed") if isinstance(state.get("real_time_learning_dashboard_feed"), dict) else {}
        route = ((executive.get("top_record") or {}).get("route_key") or (top_rows[0].get("route_key") if top_rows else ""))
        records.append({
            "route_key": route,
            "executive_decision": name,
            "status_feed": executive.get("top_record") or {},
            "signal_score": 100.0 if route else 0.0,
            "recommended_action": action,
        })
    if not records:
        records.append({
            "route_key": (top_rows[0].get("route_key") if top_rows else ""),
            "signal_score": 0.0,
            "recommended_action": action,
        })
    records.sort(key=lambda row: float(row.get("signal_score") or row.get("trust_score") or row.get("expected_value") or 0.0), reverse=True)
    return records


def _orchestration_artifact(name: str, state: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    kind = _orchestration_kind(name)
    records = _orchestration_records(kind, name, state, rows)
    top = records[0] if records else {}
    focus = _ordered_unique([row.get("route_key") for row in records if row.get("route_key") and row.get("recommended_action") not in {"quarantine_stale_learning"}])[:12]
    repair = _ordered_unique([row.get("route_key") for row in records if row.get("route_key") and row.get("recommended_action") in {"resolve_conflict_before_spend", "raise_promotion_proof_quality", "run_shadow_replay", "improve_memory_quality"}])[:12]
    avoid = _ordered_unique([row.get("route_key") for row in records if row.get("route_key") and row.get("recommended_action") == "quarantine_stale_learning"])[:12]
    width = "tight" if kind in {"artifact_governance", "belief_governance", "promotion_orchestration", "memory_orchestration"} else "wide" if kind in {"search_orchestration", "hunt_executive"} else "medium"
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "artifact": name,
        "description": _orchestration_description(name, kind),
        "kind": kind,
        "records": records[:40],
        "top_record": top,
        "focus_routes": [route for route in focus if route not in set(avoid)],
        "repair_routes": repair,
        "avoid_routes": avoid,
        "recommended_action": (top.get("recommended_action") if isinstance(top, dict) else None) or _orchestration_action(kind, name),
        "mutation_width": width,
        "batch_size_multiplier": 0.74 if width == "tight" else 1.08 if width == "wide" else 0.92,
        "summary": {
            "record_count": len(records),
            "top_route": top.get("route_key") if isinstance(top, dict) else "",
            "top_action": (top.get("recommended_action") if isinstance(top, dict) else None) or _orchestration_action(kind, name),
        },
    }


def orchestration_learning_summary(state: dict[str, Any]) -> dict[str, Any]:
    artifacts = [state.get(key) for key in ORCHESTRATION_LEARNING_ARTIFACTS if isinstance(state.get(key), dict)]
    focus = _ordered_unique([route for artifact in artifacts for route in (artifact.get("focus_routes") or [])])[:16]
    repair = _ordered_unique([route for artifact in artifacts for route in (artifact.get("repair_routes") or [])])[:16]
    avoid = _ordered_unique([route for artifact in artifacts for route in (artifact.get("avoid_routes") or [])])[:16]
    top_actions = [
        {
            "artifact": artifact.get("artifact"),
            "kind": artifact.get("kind"),
            "route_key": (artifact.get("top_record") or {}).get("route_key"),
            "action": artifact.get("recommended_action"),
        }
        for artifact in artifacts
        if artifact.get("top_record")
    ][:16]
    kind_counts: dict[str, int] = {}
    for artifact in artifacts:
        kind = str(artifact.get("kind") or "unknown")
        kind_counts[kind] = kind_counts.get(kind, 0) + 1
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Executive summary for the 80-artifact orchestration layer.",
        "summary": {
            "artifact_count": len(artifacts),
            "kind_counts": kind_counts,
            "focus_count": len(focus),
            "repair_count": len(repair),
            "avoid_count": len(avoid),
            "top_action": top_actions[0] if top_actions else {},
        },
        "focus_routes": [route for route in focus if route not in set(avoid)],
        "repair_routes": repair,
        "avoid_routes": avoid,
        "top_actions": top_actions,
    }


def orchestration_learning_suite(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    rows = _closed_loop_control_rows(state)
    artifacts: dict[str, Any] = {}
    for name in ORCHESTRATION_LEARNING_ARTIFACTS:
        artifacts[name] = _orchestration_artifact(name, {**state, **artifacts}, rows)
    artifacts["orchestration_learning_summary"] = orchestration_learning_summary({**state, **artifacts})
    return artifacts


FITNESS_LAYER_KEYS = (
    ("elite_world_class", "elite_learning_system_summary", "ELITE_LEARNING_ARTIFACTS"),
    ("proof_oriented", "proof_learning_system_summary", "PROOF_LEARNING_ARTIFACTS"),
    ("closed_loop_control", "closed_loop_control_learning_summary", "CLOSED_LOOP_CONTROL_ARTIFACTS"),
    ("world_model_nervous", "world_model_nervous_system_summary", "WORLD_MODEL_NERVOUS_SYSTEM_ARTIFACTS"),
    ("orchestration", "orchestration_learning_summary", "ORCHESTRATION_LEARNING_ARTIFACTS"),
)


def _fitness_layer_rows(state: dict[str, Any]) -> list[dict[str, Any]]:
    rows = _closed_loop_control_rows(state)
    live_count = len([row for row in rows if float(row.get("delta_vs_live") or 0.0) > 0.0])
    rank1_delta = max([float(row.get("delta_vs_live") or 0.0) for row in rows] or [0.0])
    avg_readiness = sum(float(row.get("readiness") or 0.0) for row in rows[:20]) / max(1, len(rows[:20]))
    layer_rows: list[dict[str, Any]] = []
    for idx, (layer, summary_key, artifacts_key) in enumerate(FITNESS_LAYER_KEYS, 1):
        summary = state.get(summary_key) if isinstance(state.get(summary_key), dict) else {}
        artifact_names = list(globals().get(artifacts_key, ()))
        artifacts = [state.get(key) for key in artifact_names if isinstance(state.get(key), dict)]
        focus = _ordered_unique(list(summary.get("focus_routes") or []) + [route for artifact in artifacts[:12] for route in (artifact.get("focus_routes") or [])])
        avoid = _ordered_unique(list(summary.get("avoid_routes") or []) + [route for artifact in artifacts[:12] for route in (artifact.get("avoid_routes") or [])])
        repair = _ordered_unique(list(summary.get("repair_routes") or []) + [route for artifact in artifacts[:12] for route in (artifact.get("repair_routes") or [])])
        conflict = len(set(focus) & set(avoid))
        duplicate = max(0, sum(len(artifact.get("focus_routes") or []) for artifact in artifacts[:12]) - len(set(focus)))
        contribution = live_count * 8.0 + rank1_delta / 35.0 + avg_readiness * 0.4 + len(focus) * 2.0 + len(repair) * 1.5
        cost = len(artifacts) * 0.8 + conflict * 10.0 + duplicate * 2.5 + len(avoid) * 1.2
        fitness = contribution - cost
        layer_rows.append({
            "layer": layer,
            "summary_key": summary_key,
            "artifact_count": len(artifact_names),
            "live_beating_top100_count": live_count,
            "rank1_delta_vs_live": round(rank1_delta, 4),
            "avg_promotion_readiness": round(avg_readiness, 4),
            "focus_routes": focus[:12],
            "repair_routes": repair[:12],
            "avoid_routes": avoid[:12],
            "conflict_count": conflict,
            "duplicate_route_pressure": duplicate,
            "runtime_contribution_score": round(contribution, 4),
            "runtime_cost_score": round(cost, 4),
            "fitness_score": round(fitness, 4),
            "trust_weight": round(max(0.1, min(1.6, 0.75 + fitness / 250.0)), 4),
            "recommended_action": "keep" if fitness >= 60.0 else "tune" if fitness >= 15.0 else "demote",
            "rank": idx,
        })
    layer_rows.sort(key=lambda row: float(row.get("fitness_score") or 0.0), reverse=True)
    return layer_rows


def _fitness_records(name: str, state: dict[str, Any], layers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    if name == "learning_layer_ablation_harness":
        for layer in layers:
            records.append({
                "layer": layer.get("layer"),
                "route_key": ((layer.get("focus_routes") or layer.get("repair_routes") or [""])[0] or ""),
                "shadow_disable_delta": round(-float(layer.get("fitness_score") or 0.0), 4),
                "metrics": {
                    "live_beating_top100_count": layer.get("live_beating_top100_count"),
                    "rank1_delta_vs_live": layer.get("rank1_delta_vs_live"),
                    "avg_promotion_readiness": layer.get("avg_promotion_readiness"),
                    "conflict_count": layer.get("conflict_count"),
                },
                "signal_score": layer.get("fitness_score"),
                "recommended_action": "shadow_ablate" if float(layer.get("fitness_score") or 0.0) < 25.0 else "keep_enabled",
            })
    elif name == "artifact_fitness_scorecard":
        for layer in layers:
            records.append({
                "layer": layer.get("layer"),
                "route_key": ((layer.get("focus_routes") or [""])[0] or ""),
                "fitness_score": layer.get("fitness_score"),
                "trust_weight": layer.get("trust_weight"),
                "recommended_decision": layer.get("recommended_action"),
                "signal_score": layer.get("fitness_score"),
                "recommended_action": "apply_fitness_score",
            })
    elif name == "layer_conflict_matrix":
        for left in layers:
            for right in layers:
                if left.get("layer") >= right.get("layer"):
                    continue
                conflict_routes = sorted(set(left.get("focus_routes") or []) & set(right.get("avoid_routes") or []))
                conflict_routes += sorted(set(right.get("focus_routes") or []) & set(left.get("avoid_routes") or []))
                if not conflict_routes:
                    continue
                records.append({
                    "left_layer": left.get("layer"),
                    "right_layer": right.get("layer"),
                    "route_key": conflict_routes[0],
                    "conflict_routes": _ordered_unique(conflict_routes)[:10],
                    "conflict_score": len(conflict_routes) * 10.0,
                    "signal_score": len(conflict_routes) * 10.0,
                    "recommended_action": "resolve_layer_conflict",
                })
        if not records:
            records.append({"route_key": "", "conflict_score": 0.0, "signal_score": 0.0, "recommended_action": "no_conflict"})
    elif name == "artifact_duplicate_clusterer":
        seen: dict[str, list[str]] = {}
        for layer in layers:
            for route in layer.get("focus_routes") or []:
                seen.setdefault(str(route), []).append(str(layer.get("layer")))
        for route, owners in seen.items():
            if len(owners) < 2:
                continue
            records.append({
                "route_key": route,
                "duplicate_layers": owners,
                "duplicate_score": len(owners) * 8.0,
                "signal_score": len(owners) * 8.0,
                "recommended_action": "deduplicate_artifact_routes",
            })
        if not records:
            records.append({"route_key": "", "duplicate_score": 0.0, "signal_score": 0.0, "recommended_action": "no_duplicate"})
    elif name == "artifact_runtime_roi_ranker":
        for layer in layers:
            roi = float(layer.get("runtime_contribution_score") or 0.0) / max(1.0, float(layer.get("runtime_cost_score") or 0.0))
            records.append({
                "layer": layer.get("layer"),
                "route_key": ((layer.get("focus_routes") or [""])[0] or ""),
                "runtime_roi": round(roi, 4),
                "fitness_score": layer.get("fitness_score"),
                "signal_score": round(roi * 100.0, 4),
                "recommended_action": "increase_trust" if roi >= 1.25 else "measure_more" if roi >= 0.75 else "reduce_trust",
            })
    elif name == "learning_layer_trust_policy":
        for layer in layers:
            trust = float(layer.get("trust_weight") or 1.0)
            records.append({
                "layer": layer.get("layer"),
                "route_key": ((layer.get("focus_routes") or [""])[0] or ""),
                "trust_weight": round(trust, 4),
                "policy": "scale" if trust >= 1.05 else "shadow" if trust >= 0.65 else "demote",
                "signal_score": round(trust * 100.0, 4),
                "recommended_action": "apply_layer_trust_policy",
            })
    elif name == "artifact_demote_retire_queue":
        for layer in layers:
            if str(layer.get("recommended_action")) not in {"demote", "tune"}:
                continue
            records.append({
                "layer": layer.get("layer"),
                "route_key": ((layer.get("avoid_routes") or layer.get("focus_routes") or [""])[0] or ""),
                "decision": layer.get("recommended_action"),
                "reason": "low_fitness_or_conflict_pressure",
                "fitness_score": layer.get("fitness_score"),
                "signal_score": round(max(0.0, 80.0 - float(layer.get("fitness_score") or 0.0)), 4),
                "recommended_action": "queue_layer_tune_or_demote",
            })
        if not records:
            top = layers[0] if layers else {}
            records.append({
                "layer": top.get("layer"),
                "route_key": ((top.get("focus_routes") or [""])[0] or ""),
                "decision": "none",
                "reason": "all_layers_above_demote_threshold",
                "signal_score": 0.0,
                "recommended_action": "no_demote",
            })
    else:
        top = layers[0] if layers else {}
        records.append({
            "route_key": ((top.get("focus_routes") or [""])[0] or ""),
            "best_layer": top.get("layer"),
            "best_fitness_score": top.get("fitness_score"),
            "keep_layers": [row.get("layer") for row in layers if str(row.get("recommended_action")) == "keep"],
            "tune_layers": [row.get("layer") for row in layers if str(row.get("recommended_action")) == "tune"],
            "demote_layers": [row.get("layer") for row in layers if str(row.get("recommended_action")) == "demote"],
            "signal_score": top.get("fitness_score") or 0.0,
            "recommended_action": "publish_world_model_fitness_report",
        })
    records.sort(key=lambda row: float(row.get("signal_score") or row.get("fitness_score") or row.get("runtime_roi") or 0.0), reverse=True)
    return records


def _fitness_artifact(name: str, state: dict[str, Any], layers: list[dict[str, Any]]) -> dict[str, Any]:
    descriptions = {
        "learning_layer_ablation_harness": "Runs shadow enable/disable comparisons for major learning layers.",
        "artifact_fitness_scorecard": "Scores each learning layer by contribution, cost, conflict, and duplicate pressure.",
        "layer_conflict_matrix": "Finds layers whose focus and avoid decisions contradict each other.",
        "artifact_duplicate_clusterer": "Clusters redundant artifact outputs so duplicate learning does not consume budget.",
        "artifact_runtime_roi_ranker": "Ranks learning layers by runtime return on attention and computation.",
        "learning_layer_trust_policy": "Turns fitness evidence into trust weights and scale/shadow/demote policies.",
        "artifact_demote_retire_queue": "Queues weak layers for tuning, shadowing, demotion, or retirement.",
        "world_model_fitness_report": "Executive report showing which learning layers should be kept, tuned, or demoted.",
    }
    records = _fitness_records(name, state, layers)
    top = records[0] if records else {}
    focus = _ordered_unique([row.get("route_key") for row in records if row.get("route_key") and row.get("recommended_action") not in {"queue_layer_tune_or_demote", "reduce_trust"}])[:12]
    repair = _ordered_unique([row.get("route_key") for row in records if row.get("route_key") and row.get("recommended_action") in {"shadow_ablate", "resolve_layer_conflict", "deduplicate_artifact_routes", "queue_layer_tune_or_demote"}])[:12]
    avoid: list[str] = []
    width = "tight" if name in {"learning_layer_ablation_harness", "layer_conflict_matrix", "artifact_demote_retire_queue"} else "medium"
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "artifact": name,
        "description": descriptions[name],
        "kind": "fitness_selection",
        "records": records[:40],
        "top_record": top,
        "focus_routes": [route for route in focus if route not in set(avoid)],
        "repair_routes": repair,
        "avoid_routes": avoid,
        "recommended_action": (top.get("recommended_action") if isinstance(top, dict) else None) or "measure_learning_layer_fitness",
        "mutation_width": width,
        "batch_size_multiplier": 0.78 if width == "tight" else 0.9,
        "summary": {
            "record_count": len(records),
            "top_route": top.get("route_key") if isinstance(top, dict) else "",
            "top_action": (top.get("recommended_action") if isinstance(top, dict) else None) or "measure_learning_layer_fitness",
        },
    }


def fitness_selection_summary(state: dict[str, Any]) -> dict[str, Any]:
    artifacts = [state.get(key) for key in FITNESS_SELECTION_ARTIFACTS if isinstance(state.get(key), dict)]
    focus = _ordered_unique([route for artifact in artifacts for route in (artifact.get("focus_routes") or [])])[:12]
    repair = _ordered_unique([route for artifact in artifacts for route in (artifact.get("repair_routes") or [])])[:12]
    avoid = _ordered_unique([route for artifact in artifacts for route in (artifact.get("avoid_routes") or [])])[:12]
    top_actions = [
        {
            "artifact": artifact.get("artifact"),
            "route_key": (artifact.get("top_record") or {}).get("route_key"),
            "action": artifact.get("recommended_action"),
        }
        for artifact in artifacts
        if artifact.get("top_record")
    ][:12]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Executive summary for ablation and fitness selection across learning layers.",
        "summary": {
            "artifact_count": len(artifacts),
            "focus_count": len(focus),
            "repair_count": len(repair),
            "avoid_count": len(avoid),
            "top_action": top_actions[0] if top_actions else {},
        },
        "focus_routes": [route for route in focus if route not in set(avoid)],
        "repair_routes": repair,
        "avoid_routes": avoid,
        "top_actions": top_actions,
    }


def fitness_selection_suite(state: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    layers = _fitness_layer_rows(state)
    artifacts: dict[str, Any] = {}
    for name in FITNESS_SELECTION_ARTIFACTS:
        artifacts[name] = _fitness_artifact(name, {**state, **artifacts}, layers)
    artifacts["fitness_selection_summary"] = fitness_selection_summary({**state, **artifacts})
    return artifacts


def memory_conflict_arbiter(
    compiled_memory: dict[str, Any] | None = None,
    route_pack: dict[str, Any] | None = None,
    promotion_feedback: dict[str, Any] | None = None,
) -> dict[str, Any]:
    compiled = compiled_memory or {}
    pack = route_pack or {}
    feedback = promotion_feedback or {}
    avoid = set(str(route) for route in compiled.get("retire_routes") or [])
    avoid.update(str(route) for route in feedback.get("avoid_opening_routes") or [])
    conflicts = []
    safe_focus = []
    for route in pack.get("focus_routes") or []:
        route = str(route or "")
        if route in avoid:
            conflicts.append({
                "route_key": route,
                "kind": "focus_vs_retire_or_reject_memory",
                "resolution": "remove_from_opening_focus",
            })
        elif route:
            safe_focus.append(route)
    for route in feedback.get("repair_opening_routes") or []:
        route = str(route or "")
        if route and route not in avoid and route not in safe_focus:
            safe_focus.append(route)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Resolves contradictions between durable memory, promotion survival feedback, and opening route focus.",
        "conflicts": conflicts,
        "conflict_count": len(conflicts),
        "safe_focus_routes": _ordered_unique(safe_focus)[:12],
        "avoid_routes": _ordered_unique(list(avoid))[:20],
        "resolution_policy": "current promotion survival failures override stale positive route priors",
    }


def hunt_opening_playbook(
    strategy: dict[str, Any] | None = None,
    route_pack: dict[str, Any] | None = None,
    treatment_decay: dict[str, Any] | None = None,
    conflict_arbiter: dict[str, Any] | None = None,
) -> dict[str, Any]:
    strat = (strategy or {}).get("strategy") or "router_heavy"
    focus = list((conflict_arbiter or {}).get("safe_focus_routes") or (route_pack or {}).get("focus_routes") or [])
    width = "wide" if strat == "scarcity_discovery" else "tight" if strat == "validation_heavy" else "medium"
    roles = [
        {"worker": "worker_1", "role": "route_probe", "route_key": (focus or [""])[0], "mutation_width": width},
        {"worker": "worker_2", "role": "repair_or_revalidate", "route_key": (focus[1:] or focus or [""])[0], "mutation_width": "tight"},
        {"worker": "worker_3", "role": "challenger", "route_key": (focus[2:] or focus or [""])[0], "mutation_width": "medium"},
        {"worker": "worker_4", "role": "coverage", "route_key": (focus[3:] or focus or [""])[0], "mutation_width": "wide"},
    ]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Concrete first-30-minutes hunt plan from durable cross-run memory.",
        "strategy": strat,
        "focus_routes": focus[:8],
        "avoid_routes": (conflict_arbiter or {}).get("avoid_routes") or [],
        "worker_roles": roles,
        "mutation_width": width,
        "validation_cadence": "early_revalidation" if strat == "validation_heavy" else "standard_micro_validation",
        "stop_pivot_rules": [
            "pivot if no live beaters after first useful telemetry window",
            "retire opening routes that conflict with promotion survival memory",
            "switch to scarcity discovery if focus route pack produces aliases only",
        ],
        "favor_treatments": (treatment_decay or {}).get("favor_treatments") or [],
    }


def cross_run_learning_regression_test(
    controls: dict[str, Any],
    previous_baseline: dict[str, Any] | None = None,
) -> dict[str, Any]:
    opening = controls.get("hunt_opening_playbook") or {}
    route_pack = controls.get("cold_start_route_pack_generator") or {}
    compiler = controls.get("cross_run_hunt_memory_compiler") or {}
    baseline_focus = set((previous_baseline or {}).get("focus_routes") or [])
    current_focus = set(opening.get("focus_routes") or route_pack.get("focus_routes") or [])
    checks = [
        {"check": "has_compiled_memory", "passed": bool(compiler.get("route_prior_count") or compiler.get("runtime_experiment_count"))},
        {"check": "has_opening_focus", "passed": bool(current_focus)},
        {"check": "opening_changed_from_baseline", "passed": current_focus != baseline_focus if baseline_focus else True},
        {"check": "has_worker_roles", "passed": bool(opening.get("worker_roles"))},
    ]
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Smoke test that durable memory changes next-run opening controls.",
        "checks": checks,
        "passed": all(bool(row.get("passed")) for row in checks),
        "current_focus_routes": sorted(current_focus),
        "baseline_focus_routes": sorted(baseline_focus),
    }


def cross_run_opening_controls(
    global_memory: dict[str, Any] | None = None,
    previous_baseline: dict[str, Any] | None = None,
) -> dict[str, Any]:
    compiled = cross_run_hunt_memory_compiler(global_memory)
    provenance = memory_provenance_explorer(global_memory, compiled)
    reliability = memory_reliability_scorer(global_memory, compiled, provenance)
    strategy = pre_hunt_strategy_selector(compiled)
    route_pack = cold_start_route_pack_generator(compiled, strategy)
    treatment_decay = longitudinal_treatment_decay(global_memory)
    promotion_feedback = run_level_promotion_survival_feedback(global_memory)
    retirement = belief_retirement_engine(reliability, promotion_feedback)
    compiled = {
        **compiled,
        "retire_routes": _ordered_unique(list(compiled.get("retire_routes") or []) + list(retirement.get("retire_routes") or [])),
        "favor_routes": [route for route in (compiled.get("favor_routes") or []) if route not in set(retirement.get("retire_routes") or [])],
    }
    route_pack = cold_start_route_pack_generator(compiled, strategy)
    falsification = memory_falsification_queue(reliability, provenance)
    disagreement = current_vs_historical_disagreement_monitor(compiled, previous_baseline or {}, reliability)
    stress = memory_stress_test_pack(reliability, falsification)
    arbiter = memory_conflict_arbiter(compiled, route_pack, promotion_feedback)
    opening = hunt_opening_playbook(strategy, route_pack, treatment_decay, arbiter)
    compressed = durable_memory_compression(reliability, provenance, retirement)
    controls = {
        "schema_version": 1,
        "source": "step2_online_learning",
        "description": "Cross-run controls that warm-start the next hunt from durable institutional memory.",
        "cross_run_hunt_memory_compiler": compiled,
        "memory_reliability_scorer": reliability,
        "memory_falsification_queue": falsification,
        "belief_retirement_engine": retirement,
        "memory_provenance_explorer": provenance,
        "current_vs_historical_disagreement_monitor": disagreement,
        "memory_stress_test_pack": stress,
        "durable_memory_compression": compressed,
        "pre_hunt_strategy_selector": strategy,
        "cold_start_route_pack_generator": route_pack,
        "longitudinal_treatment_decay": treatment_decay,
        "run_level_promotion_survival_feedback": promotion_feedback,
        "memory_conflict_arbiter": arbiter,
        "hunt_opening_playbook": opening,
    }
    controls["cross_run_learning_regression_test"] = cross_run_learning_regression_test(controls, previous_baseline)
    controls["memory_qa_smoke_test"] = memory_qa_smoke_test(controls)
    return controls


def read_jsonl_since(path: str | Path, offset: int = 0) -> tuple[list[dict[str, Any]], int]:
    path = Path(path)
    if not path.exists():
        return [], offset
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        f.seek(max(0, int(offset)))
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
        return rows, f.tell()


def _write_json(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n"
    try:
        if path.exists() and path.read_text(encoding="utf-8") == rendered:
            return str(path.resolve())
    except Exception:
        pass
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(rendered, encoding="utf-8")
    os.replace(tmp, path)
    return str(path.resolve())


def _cycle_yield(cycles: list[dict[str, Any]]) -> dict[str, Any]:
    recent = cycles[-20:]
    scored = sum(hunt_intel._cycle_scored_total(cycle) for cycle in recent)
    winners = sum(hunt_intel._cycle_winners(cycle) for cycle in recent)
    return {
        "recent_cycles": len(recent),
        "recent_scored_total": scored,
        "recent_reported_winners": winners,
        "recent_winner_yield_per_10k": round(winners / max(1, scored) * 10000.0, 4),
    }


def telemetry_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    if not events:
        return {
            "events": 0,
            "best_pnl": 0.0,
            "live_beaters": 0,
            "reported_winners": 0,
            "novelty_yield": 0,
            "alias_rate": 0.0,
            "top_failure_reason": "",
        }
    best_pnl = max(float(event.get("best_pnl") or 0.0) for event in events)
    live_beaters = sum(int(event.get("live_beaters") or 0) for event in events)
    reported_winners = sum(int(event.get("winners") or 0) for event in events)
    novelty = sum(int(event.get("novelty_yield") or 0) for event in events)
    alias_rates = [float(event.get("alias_rate") or 0.0) for event in events if event.get("alias_rate") is not None]
    failures: dict[str, int] = {}
    for event in events:
        reason = str(event.get("top_failure_reason") or "")
        if reason:
            failures[reason] = failures.get(reason, 0) + 1
    top_failure = max(failures, key=failures.get) if failures else ""
    return {
        "events": len(events),
        "best_pnl": best_pnl,
        "live_beaters": live_beaters,
        "reported_winners": reported_winners,
        "novelty_yield": novelty,
        "alias_rate": round(sum(alias_rates) / max(1, len(alias_rates)), 4),
        "top_failure_reason": top_failure,
    }


def _route_arm_updates(rows: list[dict[str, Any]], prior_state: dict[str, Any]) -> list[dict[str, Any]]:
    prior_arms = {
        str(arm.get("route_key")): dict(arm)
        for arm in (prior_state.get("route_arms") or [])
        if arm.get("route_key")
    }
    clusters = hunt_intel.route_clusters(rows, limit=30)
    out = []
    for cluster in clusters:
        route = str(cluster.get("route_key") or "")
        old = prior_arms.get(route, {})
        attempts = int(old.get("attempts") or 0) + 1
        best_delta = max(float(old.get("best_delta_vs_active") or 0.0), float(cluster.get("best_delta_vs_active") or 0.0))
        alias_count = int(cluster.get("alias_count") or cluster.get("count") or 0)
        novelty_rows = [row for row in rows if hunt_intel.route_key_from_row(row) == route and float(row.get("novelty_score") or 0.0) >= 25.0]
        novelty_yield = len(novelty_rows)
        old_score = float(old.get("online_score") or 0.0)
        score = old_score * 0.65 + max(0.0, best_delta) * 0.20 + alias_count * 20.0 + novelty_yield * 25.0
        out.append({
            "route_key": route,
            "attempts": attempts,
            "best_variant": cluster.get("best_variant"),
            "best_step2_pnl": cluster.get("best_step2_pnl"),
            "best_delta_vs_active": round(best_delta, 4),
            "alias_count": alias_count,
            "novelty_yield": novelty_yield,
            "online_score": round(score, 4),
        })
    old_only = [arm for key, arm in prior_arms.items() if key not in {a["route_key"] for a in out}]
    for arm in old_only:
        arm = dict(arm)
        arm["online_score"] = round(float(arm.get("online_score") or 0.0) * 0.92, 4)
        out.append(arm)
    out.sort(key=lambda arm: float(arm.get("online_score") or 0.0), reverse=True)
    return out[:30]


def update_state_from_telemetry(
    *,
    run_dir: str | Path,
    telemetry_events: list[dict[str, Any]],
    previous_state: dict[str, Any] | None = None,
    novelty_budget_pct: float = 15.0,
) -> dict[str, Any]:
    run_dir = Path(run_dir)
    state = dict(previous_state or read_state(run_dir / "online_state.json"))
    summary = telemetry_summary(telemetry_events)
    arms = {
        str(arm.get("route_key")): dict(arm)
        for arm in (state.get("route_arms") or [])
        if arm.get("route_key")
    }
    for event in telemetry_events:
        for route in event.get("route_seeds") or event.get("focus_routes") or []:
            route = str(route or "")
            if not route:
                continue
            arm = arms.setdefault(route, {"route_key": route, "attempts": 0, "online_score": 0.0})
            arm["attempts"] = int(arm.get("attempts") or 0) + 1
            arm["online_score"] = round(float(arm.get("online_score") or 0.0) * 0.85 + float(event.get("best_delta_vs_active") or 0.0) * 0.15 + int(event.get("novelty_yield") or 0) * 25.0, 4)
            arm["last_batch"] = event.get("batch")
            arm["best_step2_pnl"] = max(float(arm.get("best_step2_pnl") or 0.0), float(event.get("best_pnl") or 0.0))
            arm["best_delta_vs_active"] = max(float(arm.get("best_delta_vs_active") or 0.0), float(event.get("best_delta_vs_active") or 0.0))
            arm["novelty_yield"] = int(arm.get("novelty_yield") or 0) + int(event.get("novelty_yield") or 0)
            arm["alias_count"] = int(event.get("alias_count") or arm.get("alias_count") or 0)
    route_arms = sorted(arms.values(), key=lambda arm: float(arm.get("online_score") or 0.0), reverse=True)[:30]
    mutation_controls = _mutation_controls(route_arms, state)
    basin_status = _basin_status(route_arms, mutation_controls)
    allocation = online_allocation(route_arms, basin_status, novelty_budget_pct)
    state.update({
        "updated_at": _now_epoch(),
        "streaming_telemetry": summary,
        "route_arms": route_arms,
        "mutation_controls": mutation_controls,
        "basin_status": basin_status,
        "online_allocation": allocation,
    })
    _write_json(run_dir / "online_state.json", state)
    if telemetry_events:
        append_event(run_dir, "streaming_telemetry_ingested", summary)
    return state


def quarantine_payload(state: dict[str, Any]) -> dict[str, Any]:
    skip_routes = [
        row.get("route_key")
        for row in (state.get("basin_status") or [])
        if row.get("status") in {"exhausted", "rotate", "shrink_or_rotate"}
    ]
    directives = [
        row for row in (state.get("promotion_review_directives") or [])
        if isinstance(row, dict) and row.get("route_key")
    ]
    family_directives = [
        row for row in (state.get("family_review_directives") or [])
        if isinstance(row, dict) and row.get("family_key")
    ]
    experiment = state.get("active_experiment") if isinstance(state.get("active_experiment"), dict) else {}
    experiment_families = [
        str(experiment.get("family_key") or "")
    ] if experiment else []
    directive_routes: dict[str, list[str]] = {}
    for directive in directives:
        action = str(directive.get("action") or "")
        route = str(directive.get("route_key") or "")
        if not action or not route:
            continue
        directive_routes.setdefault(action, []).append(route)
        if action == "skip_route":
            skip_routes.append(route)
    likely_bad = [
        row for row in (state.get("candidate_triage") or [])
        if row.get("status") in {"likely_alias", "likely_overfit", "reject_ignore"}
    ]
    adapter = runtime_command_adapter(state)
    worker_contracts = planner_worker_contracts(state)
    degraded_packet = degraded_runtime_command_packet(state)
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "skip_routes": sorted({route for route in skip_routes if route}),
        "route_directives": directives,
        "widen_routes": sorted(set(directive_routes.get("widen_route", []))),
        "narrow_routes": sorted(set(directive_routes.get("narrow_route", []))),
        "holdout_repair_routes": sorted(set(directive_routes.get("repair_holdout", []))),
        "day_consistency_repair_routes": sorted(set(directive_routes.get("repair_day_consistency", []))),
        "promotion_ready_routes": sorted(set(directive_routes.get("promote_to_full_review", []))),
        "downweight_families": sorted({
            str(row.get("family_key"))
            for row in family_directives
            if row.get("action") == "downweight_family"
        }),
        "active_experiment": experiment,
        "experiment_focus_families": [family for family in experiment_families if family],
        "treatment_budget": state.get("treatment_budget") or {},
        "treatment_confidence": state.get("treatment_confidence") or {},
        "controlled_sibling_experiments": state.get("controlled_sibling_experiments") or {},
        "worker_specialization": state.get("worker_specialization") or {},
        "regime_learning": state.get("regime_learning") or {},
        "promotion_reject_predictions": state.get("promotion_reject_predictions") or {},
        "search_portfolio": state.get("search_portfolio") or {},
        "causal_experiment_registry": state.get("causal_experiment_registry") or {},
        "experiment_debt_queue": state.get("experiment_debt_queue") or {},
        "information_gain_scoring": state.get("information_gain_scoring") or {},
        "value_of_information_planner": state.get("value_of_information_planner") or {},
        "decision_change_tracker": state.get("decision_change_tracker") or {},
        "hypothesis_quality_scoring": state.get("hypothesis_quality_scoring") or {},
        "evidence_sufficiency_gate": state.get("evidence_sufficiency_gate") or {},
        "counterfactual_shadow_board": state.get("counterfactual_shadow_board") or {},
        "prediction_calibration_ledger": state.get("prediction_calibration_ledger") or {},
        "belief_revision_engine": state.get("belief_revision_engine") or {},
        "adversarial_red_team_learner": state.get("adversarial_red_team_learner") or {},
        "out_of_distribution_detector": state.get("out_of_distribution_detector") or {},
        "memory_compression_distiller": state.get("memory_compression_distiller") or {},
        "self_audit_score": state.get("self_audit_score") or {},
        "truth_first_promotion_objective": state.get("truth_first_promotion_objective") or {},
        "learning_velocity_dashboard": state.get("learning_velocity_dashboard") or {},
        "compiled_hunt_policy": state.get("compiled_hunt_policy") or {},
        "policy_executor": state.get("policy_executor") or {},
        "adaptive_worker_assignment": state.get("adaptive_worker_assignment") or {},
        "policy_backtester": state.get("policy_backtester") or {},
        "policy_mutation_engine": state.get("policy_mutation_engine") or {},
        "policy_tournament": state.get("policy_tournament") or {},
        "champion_challenger_memory": state.get("champion_challenger_memory") or {},
        "regime_specific_policies": state.get("regime_specific_policies") or {},
        "causal_graph_of_learning": state.get("causal_graph_of_learning") or {},
        "policy_safety_rail": state.get("policy_safety_rail") or {},
        "auto_promoted_field_manual": state.get("auto_promoted_field_manual") or {},
        "policy_drift_detector": state.get("policy_drift_detector") or {},
        "route_regime_half_life": state.get("route_regime_half_life") or {},
        "learning_market_map": state.get("learning_market_map") or {},
        "concept_drift_alarms": state.get("concept_drift_alarms") or {},
        "revalidation_scheduler": state.get("revalidation_scheduler") or {},
        "temporal_ensemble_policy": state.get("temporal_ensemble_policy") or {},
        "active_experiment_governor": state.get("active_experiment_governor") or {},
        "route_state_machine": state.get("route_state_machine") or {},
        "negative_knowledge_bank": state.get("negative_knowledge_bank") or {},
        "promotion_survivor_model": state.get("promotion_survivor_model") or {},
        "mutation_grammar_learner": state.get("mutation_grammar_learner") or {},
        "real_time_worker_rebalancer": state.get("real_time_worker_rebalancer") or {},
        "hunt_replay_simulator": state.get("hunt_replay_simulator") or {},
        "resurrection_engine": state.get("resurrection_engine") or {},
        "causal_mutation_attribution": state.get("causal_mutation_attribution") or {},
        "uncertainty_budgeting": state.get("uncertainty_budgeting") or {},
        "promotability_pareto_frontier": state.get("promotability_pareto_frontier") or {},
        "false_lesson_detector": state.get("false_lesson_detector") or {},
        "experiment_graduation_system": state.get("experiment_graduation_system") or {},
        "candidate_genealogy_diff_engine": state.get("candidate_genealogy_diff_engine") or {},
        "off_policy_hunt_evaluator": state.get("off_policy_hunt_evaluator") or {},
        "self_competition_league": state.get("self_competition_league") or {},
        "evidence_contract_engine": state.get("evidence_contract_engine") or {},
        "live_beater_quality_decomposer": state.get("live_beater_quality_decomposer") or {},
        "contradiction_detector": state.get("contradiction_detector") or {},
        "learning_conflict_resolver": state.get("learning_conflict_resolver") or {},
        "cohort_based_memory": state.get("cohort_based_memory") or {},
        "adaptive_hunt_throttle": state.get("adaptive_hunt_throttle") or {},
        "promotion_readiness_simulator": state.get("promotion_readiness_simulator") or {},
        "research_trace_ledger": state.get("research_trace_ledger") or {},
        "runtime_decision_kernel": state.get("runtime_decision_kernel") or {},
        "action_outcome_tracker": state.get("action_outcome_tracker") or {},
        "closed_loop_reward_model": state.get("closed_loop_reward_model") or {},
        "autonomous_hunt_planner": state.get("autonomous_hunt_planner") or {},
        "runtime_guardrails": state.get("runtime_guardrails") or {},
        "runtime_command_adapter": state.get("runtime_command_adapter") or adapter,
        "degraded_runtime_command_packet": state.get("degraded_runtime_command_packet") or degraded_packet,
        "worker_job_contracts": state.get("worker_job_contracts") or worker_contracts,
        "command_delta_status": state.get("command_delta_status") or {},
        "command_outcome_backfill": state.get("command_outcome_backfill") or {},
        "action_reward_calibration": state.get("action_reward_calibration") or {},
        "ab_route_experiment_executor": state.get("ab_route_experiment_executor") or {},
        "champion_challenger_runtime_slots": state.get("champion_challenger_runtime_slots") or {},
        "adaptive_experiment_stopping": state.get("adaptive_experiment_stopping") or {},
        "counterfactual_command_replay": state.get("counterfactual_command_replay") or {},
        "experiment_contamination_guard": state.get("experiment_contamination_guard") or {},
        "learning_rate_controller": state.get("learning_rate_controller") or {},
        "worker_learning_report_cards": state.get("worker_learning_report_cards") or {},
        "experiment_to_promotion_trace": state.get("experiment_to_promotion_trace") or {},
        "zero_yield_autopsy_engine": state.get("zero_yield_autopsy_engine") or {},
        "stuck_loop_breaker": state.get("stuck_loop_breaker") or {},
        "opportunity_cost_meter": state.get("opportunity_cost_meter") or {},
        "search_space_coverage_map": state.get("search_space_coverage_map") or {},
        "live_beater_scarcity_mode": state.get("live_beater_scarcity_mode") or {},
        "alias_trap_detector": state.get("alias_trap_detector") or {},
        "route_seed_quality_score": state.get("route_seed_quality_score") or {},
        "recovery_playbook_generator": state.get("recovery_playbook_generator") or {},
        "cross_run_hunt_memory_compiler": state.get("cross_run_hunt_memory_compiler") or {},
        "memory_reliability_scorer": state.get("memory_reliability_scorer") or {},
        "memory_falsification_queue": state.get("memory_falsification_queue") or {},
        "belief_retirement_engine": state.get("belief_retirement_engine") or {},
        "memory_provenance_explorer": state.get("memory_provenance_explorer") or {},
        "current_vs_historical_disagreement_monitor": state.get("current_vs_historical_disagreement_monitor") or {},
        "memory_stress_test_pack": state.get("memory_stress_test_pack") or {},
        "durable_memory_compression": state.get("durable_memory_compression") or {},
        "memory_qa_smoke_test": state.get("memory_qa_smoke_test") or {},
        "pre_hunt_strategy_selector": state.get("pre_hunt_strategy_selector") or {},
        "cold_start_route_pack_generator": state.get("cold_start_route_pack_generator") or {},
        "longitudinal_treatment_decay": state.get("longitudinal_treatment_decay") or {},
        "run_level_promotion_survival_feedback": state.get("run_level_promotion_survival_feedback") or {},
        "memory_conflict_arbiter": state.get("memory_conflict_arbiter") or {},
        "hunt_opening_playbook": state.get("hunt_opening_playbook") or {},
        "cross_run_learning_regression_test": state.get("cross_run_learning_regression_test") or {},
        "hypothesis_factory": state.get("hypothesis_factory") or {},
        "hypothesis_market_maker": state.get("hypothesis_market_maker") or {},
        "real_time_bet_sizer": state.get("real_time_bet_sizer") or {},
        "contrarian_generator": state.get("contrarian_generator") or {},
        "learning_stop_loss": state.get("learning_stop_loss") or {},
        "breakthrough_detector": state.get("breakthrough_detector") or {},
        "pattern_to_recipe_compiler": state.get("pattern_to_recipe_compiler") or {},
        "hunt_narrative_memory": state.get("hunt_narrative_memory") or {},
        "attention_ledger": state.get("attention_ledger") or {},
        "wasted_spend_autopsy": state.get("wasted_spend_autopsy") or {},
        "marginal_yield_curve": state.get("marginal_yield_curve") or {},
        "explore_exploit_regret_tracker": state.get("explore_exploit_regret_tracker") or {},
        "worker_alpha_attribution": state.get("worker_alpha_attribution") or {},
        "budget_reallocator": state.get("budget_reallocator") or {},
        "time_aware_hunt_plan": state.get("time_aware_hunt_plan") or {},
        "spend_efficiency_narrative": state.get("spend_efficiency_narrative") or {},
        "idea_novelty_ledger": state.get("idea_novelty_ledger") or {},
        "idea_saturation_detector": state.get("idea_saturation_detector") or {},
        "creative_leap_scorer": state.get("creative_leap_scorer") or {},
        "failed_imagination_autopsy": state.get("failed_imagination_autopsy") or {},
        "mutation_grammar_gap_finder": state.get("mutation_grammar_gap_finder") or {},
        "novelty_budget_governor": state.get("novelty_budget_governor") or {},
        "idea_lineage_map": state.get("idea_lineage_map") or {},
        "creative_brief_compiler": state.get("creative_brief_compiler") or {},
        "learning_module_registry": state.get("learning_module_registry") or {},
        "module_contribution_attribution": state.get("module_contribution_attribution") or {},
        "module_conflict_detector": state.get("module_conflict_detector") or {},
        "module_reliability_scorer": state.get("module_reliability_scorer") or {},
        "module_ablation_planner": state.get("module_ablation_planner") or {},
        "module_budget_governor": state.get("module_budget_governor") or {},
        "learning_system_self_audit": state.get("learning_system_self_audit") or {},
        "meta_learning_brief": state.get("meta_learning_brief") or {},
        "causal_intervention_scheduler": state.get("causal_intervention_scheduler") or {},
        "experiment_power_calculator": state.get("experiment_power_calculator") or {},
        "winner_fragility_profiler": state.get("winner_fragility_profiler") or {},
        "live_beater_source_attribution": state.get("live_beater_source_attribution") or {},
        "adaptive_search_temperature_controller": state.get("adaptive_search_temperature_controller") or {},
        "route_interaction_learner": state.get("route_interaction_learner") or {},
        "false_discovery_firewall": state.get("false_discovery_firewall") or {},
        "hunt_strategy_compiler_v2": state.get("hunt_strategy_compiler_v2") or {},
        "lesson_survival_tracker": state.get("lesson_survival_tracker") or {},
        "promotion_rejection_backpropagation": state.get("promotion_rejection_backpropagation") or {},
        "lesson_decay_model": state.get("lesson_decay_model") or {},
        "cross_hunt_causal_memory": state.get("cross_hunt_causal_memory") or {},
        "evidence_chain_ledger": state.get("evidence_chain_ledger") or {},
        "learning_disagreement_court": state.get("learning_disagreement_court") or {},
        "promotion_aware_search_objective": state.get("promotion_aware_search_objective") or {},
        "scientific_run_brief_v2": state.get("scientific_run_brief_v2") or {},
        "strategy_genome_registry": state.get("strategy_genome_registry") or {},
        "strategy_mutation_engine": state.get("strategy_mutation_engine") or {},
        "strategy_tournament_memory": state.get("strategy_tournament_memory") or {},
        "regime_conditioned_strategy_selector": state.get("regime_conditioned_strategy_selector") or {},
        "meta_objective_optimizer": state.get("meta_objective_optimizer") or {},
        "exploration_debt_ledger": state.get("exploration_debt_ledger") or {},
        "adversarial_strategy_red_team": state.get("adversarial_strategy_red_team") or {},
        "autonomous_pivot_governor": state.get("autonomous_pivot_governor") or {},
        "learning_roi_ledger": state.get("learning_roi_ledger") or {},
        "artifact_usefulness_pruner": state.get("artifact_usefulness_pruner") or {},
        "decision_trace_explainer": state.get("decision_trace_explainer") or {},
        "control_surface_conflict_auditor": state.get("control_surface_conflict_auditor") or {},
        "runtime_control_simplifier": state.get("runtime_control_simplifier") or {},
        "learning_cost_meter": state.get("learning_cost_meter") or {},
        "ablation_replay_harness": state.get("ablation_replay_harness") or {},
        "architecture_fitness_brief": state.get("architecture_fitness_brief") or {},
        "learning_artifact_schema_registry": state.get("learning_artifact_schema_registry") or {},
        "artifact_dependency_graph": state.get("artifact_dependency_graph") or {},
        "incremental_learning_cache": state.get("incremental_learning_cache") or {},
        "live_learning_dashboard_feed": state.get("live_learning_dashboard_feed") or {},
        "hunt_runbook_compiler": state.get("hunt_runbook_compiler") or {},
        "learning_failure_sentinel": state.get("learning_failure_sentinel") or {},
        "cross_run_artifact_warehouse": state.get("cross_run_artifact_warehouse") or {},
        "pre_hunt_readiness_gate": state.get("pre_hunt_readiness_gate") or {},
        "online_causal_bandit": state.get("online_causal_bandit") or {},
        "variant_dna_attribution": state.get("variant_dna_attribution") or {},
        "negative_gene_suppression": state.get("negative_gene_suppression") or {},
        "live_winner_family_tree": state.get("live_winner_family_tree") or {},
        "exploration_frontier_map": state.get("exploration_frontier_map") or {},
        "adaptive_worker_personalities": state.get("adaptive_worker_personalities") or {},
        "cycle_level_learning_delta": state.get("cycle_level_learning_delta") or {},
        "promotion_rejection_predictor_v2": state.get("promotion_rejection_predictor_v2") or {},
        "counterfactual_hunt_simulator": state.get("counterfactual_hunt_simulator") or {},
        "missed_winner_detector": state.get("missed_winner_detector") or {},
        "causal_regret_ledger": state.get("causal_regret_ledger") or {},
        "adaptive_search_grammar_generator": state.get("adaptive_search_grammar_generator") or {},
        "live_hypothesis_kill_scale_court": state.get("live_hypothesis_kill_scale_court") or {},
        "route_interaction_matrix_v2": state.get("route_interaction_matrix_v2") or {},
        "promotion_survival_shadow_scoring": state.get("promotion_survival_shadow_scoring") or {},
        "hunt_autopilot_policy_compiler": state.get("hunt_autopilot_policy_compiler") or {},
        "learning_claim_verifier": state.get("learning_claim_verifier") or {},
        "causal_confidence_calibration": state.get("causal_confidence_calibration") or {},
        "false_discovery_early_warning": state.get("false_discovery_early_warning") or {},
        "adaptive_evidence_thresholds": state.get("adaptive_evidence_thresholds") or {},
        "self_debate_search_council": state.get("self_debate_search_council") or {},
        "experiment_memory_compression": state.get("experiment_memory_compression") or {},
        "learning_drift_monitor": state.get("learning_drift_monitor") or {},
        "promotion_first_autopilot_v2": state.get("promotion_first_autopilot_v2") or {},
        "multi_horizon_memory_stack": state.get("multi_horizon_memory_stack") or {},
        "lesson_half_life_engine_v2": state.get("lesson_half_life_engine_v2") or {},
        "cross_hunt_strategy_replay": state.get("cross_hunt_strategy_replay") or {},
        "temporal_regime_fingerprinting": state.get("temporal_regime_fingerprinting") or {},
        "longitudinal_promotion_survival_model": state.get("longitudinal_promotion_survival_model") or {},
        "memory_conflict_court_v2": state.get("memory_conflict_court_v2") or {},
        "strategy_aging_dashboard": state.get("strategy_aging_dashboard") or {},
        "next_hunt_opening_policy_compiler": state.get("next_hunt_opening_policy_compiler") or {},
        "question_driven_hunt_planner": state.get("question_driven_hunt_planner") or {},
        "expected_information_gain_scorer_v2": state.get("expected_information_gain_scorer_v2") or {},
        "uncertainty_heatmap": state.get("uncertainty_heatmap") or {},
        "adaptive_experiment_sequencer": state.get("adaptive_experiment_sequencer") or {},
        "learning_value_stop_loss": state.get("learning_value_stop_loss") or {},
        "causal_question_ledger": state.get("causal_question_ledger") or {},
        "worker_epistemic_roles_v2": state.get("worker_epistemic_roles_v2") or {},
        "hunt_hypothesis_compiler": state.get("hunt_hypothesis_compiler") or {},
        "experiment_contract_compiler": state.get("experiment_contract_compiler") or {},
        "control_route_matcher": state.get("control_route_matcher") or {},
        "sequential_test_monitor": state.get("sequential_test_monitor") or {},
        "causal_effect_size_ledger": state.get("causal_effect_size_ledger") or {},
        "false_positive_pressure_gauge": state.get("false_positive_pressure_gauge") or {},
        "exploration_debt_paydown_planner": state.get("exploration_debt_paydown_planner") or {},
        "promotion_aware_power_planner": state.get("promotion_aware_power_planner") or {},
        "scientific_hunt_executive": state.get("scientific_hunt_executive") or {},
        "live_candidate_evidence_builder": state.get("live_candidate_evidence_builder") or {},
        "promotion_failure_predictor_v3": state.get("promotion_failure_predictor_v3") or {},
        "evidence_gap_router": state.get("evidence_gap_router") or {},
        "review_ready_queue_v2": state.get("review_ready_queue_v2") or {},
        "promotion_evidence_scorecard": state.get("promotion_evidence_scorecard") or {},
        "candidate_lineage_explainer_v2": state.get("candidate_lineage_explainer_v2") or {},
        "live_vs_control_differential_report": state.get("live_vs_control_differential_report") or {},
        "promotion_packet_executive": state.get("promotion_packet_executive") or {},
        "unified_learning_state_reducer": state.get("unified_learning_state_reducer") or {},
        "artifact_priority_arbitration_engine": state.get("artifact_priority_arbitration_engine") or {},
        "evidence_provenance_graph": state.get("evidence_provenance_graph") or {},
        "counterfactual_promotion_replay": state.get("counterfactual_promotion_replay") or {},
        "candidate_survival_simulator": state.get("candidate_survival_simulator") or {},
        "live_regime_shift_detector_v2": state.get("live_regime_shift_detector_v2") or {},
        "adaptive_trust_weights_per_module": state.get("adaptive_trust_weights_per_module") or {},
        "worker_skill_elo_v2": state.get("worker_skill_elo_v2") or {},
        "route_genome_knowledge_graph": state.get("route_genome_knowledge_graph") or {},
        "causal_feature_interaction_miner": state.get("causal_feature_interaction_miner") or {},
        "overfit_signature_library": state.get("overfit_signature_library") or {},
        "review_rejection_memory_bank_v2": state.get("review_rejection_memory_bank_v2") or {},
        "learning_budget_optimizer": state.get("learning_budget_optimizer") or {},
        "search_novelty_floor": state.get("search_novelty_floor") or {},
        "breakthrough_escalation_protocol": state.get("breakthrough_escalation_protocol") or {},
        "false_discovery_backpressure_controller": state.get("false_discovery_backpressure_controller") or {},
        "multi_armed_strategy_portfolio": state.get("multi_armed_strategy_portfolio") or {},
        "historical_lesson_ab_harness": state.get("historical_lesson_ab_harness") or {},
        "promotion_packet_diff_engine": state.get("promotion_packet_diff_engine") or {},
        "candidate_repair_recipe_generator": state.get("candidate_repair_recipe_generator") or {},
        "automated_red_team_reviewer": state.get("automated_red_team_reviewer") or {},
        "learning_compression_field_manual": state.get("learning_compression_field_manual") or {},
        "hunt_outcome_attribution_v2": state.get("hunt_outcome_attribution_v2") or {},
        "world_state_dashboard_artifact": state.get("world_state_dashboard_artifact") or {},
        "meta_learning_governor": state.get("meta_learning_governor") or {},
        **{key: state.get(key) or {} for key in ELITE_LEARNING_ARTIFACTS},
        "elite_learning_system_summary": state.get("elite_learning_system_summary") or {},
        **{key: state.get(key) or {} for key in PROOF_LEARNING_ARTIFACTS},
        "proof_learning_system_summary": state.get("proof_learning_system_summary") or {},
        **{key: state.get(key) or {} for key in CLOSED_LOOP_CONTROL_ARTIFACTS},
        "closed_loop_control_learning_summary": state.get("closed_loop_control_learning_summary") or {},
        **{key: state.get(key) or {} for key in WORLD_MODEL_NERVOUS_SYSTEM_ARTIFACTS},
        "world_model_nervous_system_summary": state.get("world_model_nervous_system_summary") or {},
        **{key: state.get(key) or {} for key in ORCHESTRATION_LEARNING_ARTIFACTS},
        "orchestration_learning_summary": state.get("orchestration_learning_summary") or {},
        **{key: state.get(key) or {} for key in FITNESS_SELECTION_ARTIFACTS},
        "fitness_selection_summary": state.get("fitness_selection_summary") or {},
        "command_replay_ledger": state.get("command_replay_ledger") or {},
        "action_elo_league": state.get("action_elo_league") or {},
        "human_readable_hunt_brief": state.get("human_readable_hunt_brief") or {},
        "meta_hunt_strategy": state.get("meta_hunt_strategy") or {},
        "run_to_run_postmortem": state.get("run_to_run_postmortem") or {},
        "best_treatment": state.get("best_treatment"),
        "worst_treatment": state.get("worst_treatment"),
        "quarantined_variants": [row.get("variant") for row in likely_bad if row.get("variant")],
        "quarantined_behavior_keys": [row.get("behavior_key") for row in likely_bad if row.get("behavior_key")],
        "reasons": likely_bad[:50],
    }


def should_interrupt_from_telemetry(state: dict[str, Any], telemetry_events: list[dict[str, Any]], *, min_events: int = 3) -> tuple[bool, str]:
    if len(telemetry_events) < int(min_events):
        return False, ""
    recent = telemetry_events[-int(min_events):]
    live = sum(int(event.get("live_beaters") or event.get("winners") or 0) for event in recent)
    novelty = sum(int(event.get("novelty_yield") or 0) for event in recent)
    alias = sum(float(event.get("alias_rate") or 0.0) for event in recent) / max(1, len(recent))
    if live == 0 and novelty == 0:
        return True, "no_live_or_novelty_yield"
    if alias >= 0.85 and novelty == 0:
        return True, "alias_rate_high_without_novelty"
    exhausted = [row for row in (state.get("basin_status") or []) if row.get("status") in {"exhausted", "rotate"}]
    if exhausted and live == 0:
        return True, "active_basin_exhausted"
    return False, ""


def batch_size_multiplier(state: dict[str, Any]) -> float:
    adapter = runtime_command_adapter(state)
    telemetry = state.get("streaming_telemetry") if isinstance(state.get("streaming_telemetry"), dict) else {}
    yield_per = float((state.get("cycle_yield") or {}).get("recent_winner_yield_per_10k") or 0.0)
    alias_rate = float(telemetry.get("alias_rate") or 0.0)
    runtime_mult = float(adapter.get("batch_size_multiplier") or 1.0)
    if adapter.get("degrade_mode"):
        runtime_mult = min(runtime_mult, 0.65)
    if yield_per >= 20.0 and alias_rate < 0.50:
        return round(min(1.5, max(runtime_mult, 1.0)), 4)
    if alias_rate >= 0.80:
        return round(min(0.6, runtime_mult), 4)
    if float(telemetry.get("live_beaters") or 0.0) == 0.0 and int(telemetry.get("events") or 0) >= 3:
        return round(min(0.75, runtime_mult), 4)
    return round(runtime_mult, 4)


def _mutation_controls(route_arms: list[dict[str, Any]], previous: dict[str, Any]) -> dict[str, dict[str, Any]]:
    previous_controls = previous.get("mutation_controls") if isinstance(previous.get("mutation_controls"), dict) else {}
    directives_by_route: dict[str, list[dict[str, Any]]] = {}
    for directive in previous.get("promotion_review_directives") or []:
        if isinstance(directive, dict) and directive.get("route_key"):
            directives_by_route.setdefault(str(directive.get("route_key")), []).append(directive)
    active_experiment = previous.get("active_experiment") if isinstance(previous.get("active_experiment"), dict) else {}
    experiment_route = str(active_experiment.get("route_key") or "")
    experiment_treatments = active_experiment.get("treatments") if isinstance(active_experiment.get("treatments"), list) else []
    treatment_lane = str(((experiment_treatments or [{}])[0] or {}).get("mutation_lane") or "")
    active_treatment = hunt_intel._row_treatment({"mutation_lane": treatment_lane}) if treatment_lane else ""
    treatment_priors = {
        str(row.get("treatment")): row
        for row in ((previous.get("treatment_prior_model") or {}).get("priors") or [])
        if isinstance(row, dict) and row.get("treatment")
    }
    best_treatment = str(previous.get("best_treatment") or "")
    worst_treatment = str(previous.get("worst_treatment") or "")
    controls: dict[str, dict[str, Any]] = {}
    for arm in route_arms:
        route = str(arm.get("route_key") or "")
        old = previous_controls.get(route, {}) if isinstance(previous_controls, dict) else {}
        alias_count = int(arm.get("alias_count") or 0)
        novelty = int(arm.get("novelty_yield") or 0)
        attempts = int(arm.get("attempts") or 1)
        old_scale = float(old.get("scale") or 1.0)
        reason = "balanced"
        scale = old_scale
        if novelty >= 2 and alias_count <= 4:
            scale = max(0.45, old_scale * 0.88)
            reason = "steady_improvement_shrink"
        elif alias_count >= 6 and novelty == 0:
            scale = min(1.75, old_scale * 1.18)
            reason = "aliasing_without_novelty_widen"
        elif attempts >= 5 and float(arm.get("best_delta_vs_active") or 0.0) <= 0.0:
            scale = min(1.90, old_scale * 1.25)
            reason = "stagnant_widen_or_rotate"
        actions = {str(item.get("action") or "") for item in directives_by_route.get(route, [])}
        if "repair_holdout" in actions:
            scale = max(0.55, min(scale, 0.85))
            reason = "promotion_review_holdout_repair"
        elif "repair_day_consistency" in actions:
            scale = max(0.50, min(scale, 0.75))
            reason = "promotion_review_day_consistency_repair"
        elif "widen_route" in actions:
            scale = min(1.65, max(scale, old_scale * 1.20))
            reason = "promotion_review_edge_expansion"
        elif "narrow_route" in actions:
            scale = max(0.40, min(scale, 0.65))
            reason = "promotion_review_route_safety_narrow"
        prior = {}
        if active_experiment and route == experiment_route and treatment_lane:
            if treatment_lane in {"holdout_repair", "day_consistency_repair", "robustness_first"}:
                scale = max(0.45, min(scale, 0.75))
            elif treatment_lane in {"wild_shuffle", "edge_expansion"}:
                scale = min(1.60, max(scale, 1.20))
            reason = f"active_experiment_{treatment_lane}"
            prior = treatment_priors.get(active_treatment, {})
            if active_treatment and active_treatment == best_treatment:
                scale = min(1.70, max(scale, 1.08))
                reason = f"best_treatment_{active_treatment}"
            elif active_treatment and active_treatment == worst_treatment:
                scale = max(0.42, min(scale, 0.72))
                reason = f"worst_treatment_caution_{active_treatment}"
        controls[route] = {
            "scale": round(scale, 4),
            "reason": reason,
            "attempts": attempts,
            "promotion_review_actions": sorted(actions),
            "active_experiment_id": active_experiment.get("experiment_id") if route == experiment_route else None,
            "active_treatment": active_treatment if route == experiment_route else "",
            "treatment_prior": prior if route == experiment_route and active_experiment else {},
        }
    return controls


def _basin_status(route_arms: list[dict[str, Any]], mutation_controls: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    statuses = []
    for arm in route_arms:
        route = str(arm.get("route_key") or "")
        control = mutation_controls.get(route, {})
        status = "continue"
        reasons = []
        if int(arm.get("attempts") or 0) >= 6 and int(arm.get("novelty_yield") or 0) == 0:
            status = "exhausted"
            reasons.append("no_novelty_after_repeated_attempts")
        if int(arm.get("alias_count") or 0) >= 8 and float(arm.get("best_delta_vs_active") or 0.0) < 500.0:
            status = "shrink_or_rotate"
            reasons.append("alias_count_high_without_large_delta")
        if float(control.get("scale") or 1.0) >= 1.75:
            status = "rotate"
            reasons.append("mutation_radius_already_wide")
        statuses.append({
            "route_key": route,
            "status": status,
            "reasons": reasons,
        })
    return statuses


def triage_candidates(rows: list[dict[str, Any]], limit: int = 50,
                      downweight_families: set[str] | None = None) -> list[dict[str, Any]]:
    downweight_families = downweight_families or set()
    triaged = []
    for idx, row in enumerate(rows[:limit], 1):
        alias_count = int(row.get("behavior_alias_count") or 1)
        overfit = float(row.get("overfit_risk_score") or 0.0)
        novelty = float(row.get("novelty_score") or 0.0)
        quality = float(row.get("promotion_quality_score") or 0.0)
        status = "keep_hunting"
        reasons = []
        if alias_count >= 3:
            status = "likely_alias"
            reasons.append("behavior_alias_count_high")
        if overfit >= 65.0:
            status = "likely_overfit"
            reasons.append("overfit_risk_high")
        if quality > 0 and novelty >= 25.0 and overfit < 60.0:
            status = "validate_now"
            reasons.append("quality_plus_novelty")
        if hunt_intel.live_delta(row) <= 0:
            status = "reject_ignore"
            reasons.append("does_not_beat_live")
        family = str(row.get("family_key") or hunt_intel.candidate_family_key(row))
        if family in downweight_families:
            status = "family_caution"
            reasons.append("family_rejection_memory")
        triaged.append({
            "rank": idx,
            "variant": row.get("variant"),
            "route_key": hunt_intel.route_key_from_row(row),
            "status": status,
            "reasons": reasons,
            "step2_pnl": hunt_intel.pnl(row),
            "delta_vs_active": hunt_intel.live_delta(row),
            "novelty_score": novelty,
            "overfit_risk_score": overfit,
            "behavior_alias_count": alias_count,
            "family_key": family,
        })
    return triaged


def _directive_from_feedback(row: dict[str, Any]) -> dict[str, Any]:
    route = str(row.get("route_key") or "unrouted")
    status = str(row.get("status") or row.get("decision") or "").lower()
    reasons = {str(reason) for reason in (row.get("reject_reasons") or [])}
    tags = {str(tag) for tag in (row.get("learning_tags") or [])}
    action = "continue"
    lane = "robustness_first"
    if status in {"approve", "approved", "pass"}:
        action = "promote_to_full_review"
        lane = "promotion_ready"
    elif reasons & {"reproducibility_failed", "reproducibility_mismatch", "micro_promotion_review_error"}:
        action = "skip_route"
        lane = "reproducibility_repair"
    elif reasons & {"route_safety_failed", "route_contract_failed"}:
        action = "narrow_route"
        lane = "route_safety_repair"
    elif "robustness_score_below_70" in reasons and ("thin_holdout_edge" in tags or "no_holdout_credit" in tags):
        action = "repair_holdout"
        lane = "holdout_repair"
    elif "robustness_score_below_70" in reasons or "weak_day_consistency" in tags:
        action = "repair_day_consistency"
        lane = "day_consistency_repair"
    elif "small_total_edge" in tags:
        action = "widen_route"
        lane = "edge_expansion"
    return {
        "route_key": route,
        "variant": row.get("variant"),
        "action": action,
        "lane": lane,
        "reasons": sorted(reasons),
        "learning_tags": sorted(tags),
        "plain_english": row.get("plain_english"),
    }


def apply_promotion_review_feedback(
    state: dict[str, Any],
    feedback_rows: list[dict[str, Any]] | None = None,
    directives: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    state = dict(state or {})
    feedback_rows = [row for row in (feedback_rows or []) if isinstance(row, dict)]
    directives = [row for row in (directives or []) if isinstance(row, dict)]
    generated = [_directive_from_feedback(row) for row in feedback_rows]
    merged: dict[tuple[str, str], dict[str, Any]] = {}
    for directive in list(state.get("promotion_review_directives") or []) + generated + directives:
        if not isinstance(directive, dict):
            continue
        route = str(directive.get("route_key") or "")
        action = str(directive.get("action") or "")
        if not route or not action:
            continue
        merged[(route, action)] = directive
    state["promotion_review_feedback"] = feedback_rows[-100:]
    state["promotion_review_directives"] = list(merged.values())[-100:]
    family_memory = hunt_intel.family_rejection_memory(feedback_rows)
    existing_family = {
        str(row.get("family_key")): row
        for row in (state.get("family_review_directives") or [])
        if isinstance(row, dict) and row.get("family_key")
    }
    for family in family_memory.get("families") or []:
        if float(family.get("reject_rate") or 0.0) >= 0.5 and int(family.get("rejects") or 0) >= 1:
            existing_family[str(family.get("family_key"))] = {
                "family_key": family.get("family_key"),
                "action": "downweight_family",
                "reject_rate": family.get("reject_rate"),
                "top_reasons": family.get("top_reasons") or [],
                "sample_variants": family.get("sample_variants") or [],
            }
    state["family_review_directives"] = list(existing_family.values())[-100:]
    state["family_rejection_memory"] = family_memory
    lane_counts: dict[str, int] = {}
    for directive in state["promotion_review_directives"]:
        lane = str(directive.get("lane") or "unknown")
        lane_counts[lane] = lane_counts.get(lane, 0) + 1
    state["promotion_review_lane_pressure"] = lane_counts
    return state


def apply_experiment_plan(state: dict[str, Any], experiment_plan: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(state or {})
    experiments = list((experiment_plan or {}).get("experiments") or [])
    active = experiments[0] if experiments else {}
    state["active_experiment"] = active
    state["experiment_queue"] = experiments[:20]
    if active:
        state["experiment_directive"] = {
            "experiment_id": active.get("experiment_id"),
            "kind": active.get("kind"),
            "hypothesis": active.get("hypothesis"),
            "route_key": active.get("route_key"),
            "family_key": active.get("family_key"),
            "worker_assignment": active.get("worker_assignment"),
            "primary_treatment": ((active.get("treatments") or [{}])[0] or {}),
            "success_metric": active.get("success_metric"),
            "stop_rule": active.get("stop_rule"),
        }
    else:
        state["experiment_directive"] = {}
    return state


def apply_treatment_priors(
    state: dict[str, Any],
    treatment_prior_model: dict[str, Any] | None = None,
    treatment_worker_budget: dict[str, Any] | None = None,
) -> dict[str, Any]:
    state = dict(state or {})
    prior_model = treatment_prior_model if isinstance(treatment_prior_model, dict) else {}
    worker_budget = treatment_worker_budget if isinstance(treatment_worker_budget, dict) else {}
    if not prior_model and isinstance(state.get("treatment_prior_model"), dict):
        prior_model = state.get("treatment_prior_model") or {}
    if not worker_budget and isinstance(state.get("treatment_worker_budget"), dict):
        worker_budget = state.get("treatment_worker_budget") or {}
    priors = list(prior_model.get("priors") or [])
    workers = list(worker_budget.get("workers") or [])
    state["treatment_prior_model"] = prior_model
    state["treatment_worker_budget"] = worker_budget
    state["best_treatment"] = prior_model.get("best_treatment") or worker_budget.get("best_treatment")
    state["worst_treatment"] = prior_model.get("worst_treatment") or worker_budget.get("worst_treatment")
    state["treatment_budget"] = {
        "priors": priors[:12],
        "workers": workers[:4],
        "best_treatment": state.get("best_treatment"),
        "worst_treatment": state.get("worst_treatment"),
    }
    return state


def apply_learning_upgrade_controls(
    state: dict[str, Any],
    *,
    treatment_confidence: dict[str, Any] | None = None,
    controlled_sibling_experiments: dict[str, Any] | None = None,
    worker_specialization: dict[str, Any] | None = None,
    regime_learning: dict[str, Any] | None = None,
    promotion_reject_simulator: dict[str, Any] | None = None,
    search_portfolio: dict[str, Any] | None = None,
    causal_experiment_registry: dict[str, Any] | None = None,
    experiment_debt_queue: dict[str, Any] | None = None,
    information_gain_scoring: dict[str, Any] | None = None,
    value_of_information_planner: dict[str, Any] | None = None,
    decision_change_tracker: dict[str, Any] | None = None,
    hypothesis_quality_scoring: dict[str, Any] | None = None,
    evidence_sufficiency_gate: dict[str, Any] | None = None,
    counterfactual_shadow_board: dict[str, Any] | None = None,
    prediction_calibration_ledger: dict[str, Any] | None = None,
    belief_revision_engine: dict[str, Any] | None = None,
    adversarial_red_team_learner: dict[str, Any] | None = None,
    out_of_distribution_detector: dict[str, Any] | None = None,
    memory_compression_distiller: dict[str, Any] | None = None,
    self_audit_score: dict[str, Any] | None = None,
    truth_first_promotion_objective: dict[str, Any] | None = None,
    learning_velocity_dashboard: dict[str, Any] | None = None,
    compiled_hunt_policy: dict[str, Any] | None = None,
    policy_executor: dict[str, Any] | None = None,
    adaptive_worker_assignment: dict[str, Any] | None = None,
    policy_backtester: dict[str, Any] | None = None,
    policy_mutation_engine: dict[str, Any] | None = None,
    policy_tournament: dict[str, Any] | None = None,
    champion_challenger_memory: dict[str, Any] | None = None,
    regime_specific_policies: dict[str, Any] | None = None,
    causal_graph_of_learning: dict[str, Any] | None = None,
    policy_safety_rail: dict[str, Any] | None = None,
    auto_promoted_field_manual: dict[str, Any] | None = None,
    policy_drift_detector: dict[str, Any] | None = None,
    route_regime_half_life: dict[str, Any] | None = None,
    learning_market_map: dict[str, Any] | None = None,
    concept_drift_alarms: dict[str, Any] | None = None,
    revalidation_scheduler: dict[str, Any] | None = None,
    temporal_ensemble_policy: dict[str, Any] | None = None,
    active_experiment_governor: dict[str, Any] | None = None,
    route_state_machine: dict[str, Any] | None = None,
    negative_knowledge_bank: dict[str, Any] | None = None,
    promotion_survivor_model: dict[str, Any] | None = None,
    mutation_grammar_learner: dict[str, Any] | None = None,
    real_time_worker_rebalancer: dict[str, Any] | None = None,
    hunt_replay_simulator: dict[str, Any] | None = None,
    resurrection_engine: dict[str, Any] | None = None,
    causal_mutation_attribution: dict[str, Any] | None = None,
    uncertainty_budgeting: dict[str, Any] | None = None,
    promotability_pareto_frontier: dict[str, Any] | None = None,
    false_lesson_detector: dict[str, Any] | None = None,
    experiment_graduation_system: dict[str, Any] | None = None,
    candidate_genealogy_diff_engine: dict[str, Any] | None = None,
    off_policy_hunt_evaluator: dict[str, Any] | None = None,
    self_competition_league: dict[str, Any] | None = None,
    evidence_contract_engine: dict[str, Any] | None = None,
    live_beater_quality_decomposer: dict[str, Any] | None = None,
    contradiction_detector: dict[str, Any] | None = None,
    learning_conflict_resolver: dict[str, Any] | None = None,
    cohort_based_memory: dict[str, Any] | None = None,
    adaptive_hunt_throttle: dict[str, Any] | None = None,
    promotion_readiness_simulator: dict[str, Any] | None = None,
    research_trace_ledger: dict[str, Any] | None = None,
    runtime_decision_kernel: dict[str, Any] | None = None,
    action_outcome_tracker: dict[str, Any] | None = None,
    closed_loop_reward_model: dict[str, Any] | None = None,
    autonomous_hunt_planner: dict[str, Any] | None = None,
    runtime_guardrails: dict[str, Any] | None = None,
    command_replay_ledger: dict[str, Any] | None = None,
    action_elo_league: dict[str, Any] | None = None,
    human_readable_hunt_brief: dict[str, Any] | None = None,
    meta_hunt_strategy: dict[str, Any] | None = None,
    run_to_run_postmortem: dict[str, Any] | None = None,
) -> dict[str, Any]:
    state = dict(state or {})
    if isinstance(treatment_confidence, dict):
        state["treatment_confidence"] = treatment_confidence
    if isinstance(controlled_sibling_experiments, dict):
        state["controlled_sibling_experiments"] = controlled_sibling_experiments
    if isinstance(worker_specialization, dict):
        state["worker_specialization"] = worker_specialization
    if isinstance(regime_learning, dict):
        state["regime_learning"] = regime_learning
    if isinstance(promotion_reject_simulator, dict):
        state["promotion_reject_predictions"] = promotion_reject_simulator
    if isinstance(search_portfolio, dict):
        state["search_portfolio"] = search_portfolio
    if isinstance(causal_experiment_registry, dict):
        state["causal_experiment_registry"] = causal_experiment_registry
    if isinstance(experiment_debt_queue, dict):
        state["experiment_debt_queue"] = experiment_debt_queue
    if isinstance(information_gain_scoring, dict):
        state["information_gain_scoring"] = information_gain_scoring
    if isinstance(value_of_information_planner, dict):
        state["value_of_information_planner"] = value_of_information_planner
    if isinstance(decision_change_tracker, dict):
        state["decision_change_tracker"] = decision_change_tracker
    if isinstance(hypothesis_quality_scoring, dict):
        state["hypothesis_quality_scoring"] = hypothesis_quality_scoring
    if isinstance(evidence_sufficiency_gate, dict):
        state["evidence_sufficiency_gate"] = evidence_sufficiency_gate
    if isinstance(counterfactual_shadow_board, dict):
        state["counterfactual_shadow_board"] = counterfactual_shadow_board
    if isinstance(prediction_calibration_ledger, dict):
        state["prediction_calibration_ledger"] = prediction_calibration_ledger
    if isinstance(belief_revision_engine, dict):
        state["belief_revision_engine"] = belief_revision_engine
    if isinstance(adversarial_red_team_learner, dict):
        state["adversarial_red_team_learner"] = adversarial_red_team_learner
    if isinstance(out_of_distribution_detector, dict):
        state["out_of_distribution_detector"] = out_of_distribution_detector
    if isinstance(memory_compression_distiller, dict):
        state["memory_compression_distiller"] = memory_compression_distiller
    if isinstance(self_audit_score, dict):
        state["self_audit_score"] = self_audit_score
    if isinstance(truth_first_promotion_objective, dict):
        state["truth_first_promotion_objective"] = truth_first_promotion_objective
    if isinstance(learning_velocity_dashboard, dict):
        state["learning_velocity_dashboard"] = learning_velocity_dashboard
    if isinstance(compiled_hunt_policy, dict):
        state["compiled_hunt_policy"] = compiled_hunt_policy
    if isinstance(policy_executor, dict):
        state["policy_executor"] = policy_executor
    if isinstance(adaptive_worker_assignment, dict):
        state["adaptive_worker_assignment"] = adaptive_worker_assignment
    if isinstance(policy_backtester, dict):
        state["policy_backtester"] = policy_backtester
    if isinstance(policy_mutation_engine, dict):
        state["policy_mutation_engine"] = policy_mutation_engine
    if isinstance(policy_tournament, dict):
        state["policy_tournament"] = policy_tournament
    if isinstance(champion_challenger_memory, dict):
        state["champion_challenger_memory"] = champion_challenger_memory
    if isinstance(regime_specific_policies, dict):
        state["regime_specific_policies"] = regime_specific_policies
    if isinstance(causal_graph_of_learning, dict):
        state["causal_graph_of_learning"] = causal_graph_of_learning
    if isinstance(policy_safety_rail, dict):
        state["policy_safety_rail"] = policy_safety_rail
    if isinstance(auto_promoted_field_manual, dict):
        state["auto_promoted_field_manual"] = auto_promoted_field_manual
    if isinstance(policy_drift_detector, dict):
        state["policy_drift_detector"] = policy_drift_detector
    if isinstance(route_regime_half_life, dict):
        state["route_regime_half_life"] = route_regime_half_life
    if isinstance(learning_market_map, dict):
        state["learning_market_map"] = learning_market_map
    if isinstance(concept_drift_alarms, dict):
        state["concept_drift_alarms"] = concept_drift_alarms
    if isinstance(revalidation_scheduler, dict):
        state["revalidation_scheduler"] = revalidation_scheduler
    if isinstance(temporal_ensemble_policy, dict):
        state["temporal_ensemble_policy"] = temporal_ensemble_policy
    if isinstance(active_experiment_governor, dict):
        state["active_experiment_governor"] = active_experiment_governor
    if isinstance(route_state_machine, dict):
        state["route_state_machine"] = route_state_machine
    if isinstance(negative_knowledge_bank, dict):
        state["negative_knowledge_bank"] = negative_knowledge_bank
    if isinstance(promotion_survivor_model, dict):
        state["promotion_survivor_model"] = promotion_survivor_model
    if isinstance(mutation_grammar_learner, dict):
        state["mutation_grammar_learner"] = mutation_grammar_learner
    if isinstance(real_time_worker_rebalancer, dict):
        state["real_time_worker_rebalancer"] = real_time_worker_rebalancer
    if isinstance(hunt_replay_simulator, dict):
        state["hunt_replay_simulator"] = hunt_replay_simulator
    if isinstance(resurrection_engine, dict):
        state["resurrection_engine"] = resurrection_engine
    if isinstance(causal_mutation_attribution, dict):
        state["causal_mutation_attribution"] = causal_mutation_attribution
    if isinstance(uncertainty_budgeting, dict):
        state["uncertainty_budgeting"] = uncertainty_budgeting
    if isinstance(promotability_pareto_frontier, dict):
        state["promotability_pareto_frontier"] = promotability_pareto_frontier
    if isinstance(false_lesson_detector, dict):
        state["false_lesson_detector"] = false_lesson_detector
    if isinstance(experiment_graduation_system, dict):
        state["experiment_graduation_system"] = experiment_graduation_system
    if isinstance(candidate_genealogy_diff_engine, dict):
        state["candidate_genealogy_diff_engine"] = candidate_genealogy_diff_engine
    if isinstance(off_policy_hunt_evaluator, dict):
        state["off_policy_hunt_evaluator"] = off_policy_hunt_evaluator
    if isinstance(self_competition_league, dict):
        state["self_competition_league"] = self_competition_league
    if isinstance(evidence_contract_engine, dict):
        state["evidence_contract_engine"] = evidence_contract_engine
    if isinstance(live_beater_quality_decomposer, dict):
        state["live_beater_quality_decomposer"] = live_beater_quality_decomposer
    if isinstance(contradiction_detector, dict):
        state["contradiction_detector"] = contradiction_detector
    if isinstance(learning_conflict_resolver, dict):
        state["learning_conflict_resolver"] = learning_conflict_resolver
    if isinstance(cohort_based_memory, dict):
        state["cohort_based_memory"] = cohort_based_memory
    if isinstance(adaptive_hunt_throttle, dict):
        state["adaptive_hunt_throttle"] = adaptive_hunt_throttle
    if isinstance(promotion_readiness_simulator, dict):
        state["promotion_readiness_simulator"] = promotion_readiness_simulator
    if isinstance(research_trace_ledger, dict):
        state["research_trace_ledger"] = research_trace_ledger
    if isinstance(runtime_decision_kernel, dict):
        state["runtime_decision_kernel"] = runtime_decision_kernel
    if isinstance(action_outcome_tracker, dict):
        state["action_outcome_tracker"] = action_outcome_tracker
    if isinstance(closed_loop_reward_model, dict):
        state["closed_loop_reward_model"] = closed_loop_reward_model
    if isinstance(autonomous_hunt_planner, dict):
        state["autonomous_hunt_planner"] = autonomous_hunt_planner
    if isinstance(runtime_guardrails, dict):
        state["runtime_guardrails"] = runtime_guardrails
    if isinstance(command_replay_ledger, dict):
        state["command_replay_ledger"] = command_replay_ledger
    if isinstance(action_elo_league, dict):
        state["action_elo_league"] = action_elo_league
    if isinstance(human_readable_hunt_brief, dict):
        state["human_readable_hunt_brief"] = human_readable_hunt_brief
    if isinstance(meta_hunt_strategy, dict):
        state["meta_hunt_strategy"] = meta_hunt_strategy
    if isinstance(run_to_run_postmortem, dict):
        state["run_to_run_postmortem"] = run_to_run_postmortem
    state.update(hypothesis_learning_suite(state))
    state.update(attention_economics_suite(state))
    state.update(creative_imagination_suite(state))
    state.update(meta_learning_suite(state))
    state.update(runtime_science_suite(state))
    state.update(lesson_accountability_suite(state))
    state.update(strategy_evolution_suite(state))
    state.update(governance_suite(state))
    adapter = runtime_command_adapter(state)
    state["runtime_command_adapter"] = adapter
    state.update(real_time_causal_search_suite(state))
    state.update(counterfactual_opportunity_suite(state))
    state.update(truth_maintenance_suite(state))
    state.update(temporal_memory_suite(state))
    state.update(active_uncertainty_learning_suite(state))
    state.update(closed_loop_scientific_execution_suite(state))
    state.update(promotion_grade_evidence_suite(state))
    state.update(world_class_meta_learning_suite(state))
    state.update(elite_world_class_learning_suite(state))
    state.update(proof_oriented_learning_suite(state))
    state.update(scientific_control_learning_suite(state))
    state.update(world_model_nervous_system_suite(state))
    state.update(orchestration_learning_suite(state))
    state.update(fitness_selection_suite(state))
    adapter = runtime_command_adapter(state)
    state["runtime_command_adapter"] = adapter
    worker_contracts = planner_worker_contracts(state)
    reward_calibration = action_reward_calibration(state)
    state["degraded_runtime_command_packet"] = degraded_runtime_command_packet(state)
    state["worker_job_contracts"] = worker_contracts
    state["action_reward_calibration"] = reward_calibration
    state.update(learning_ops_suite(state))
    adapter = runtime_command_adapter(state)
    state["runtime_command_adapter"] = adapter
    state["worker_job_contracts"] = planner_worker_contracts(state)

    portfolio_allocations = list((state.get("search_portfolio") or {}).get("allocations") or [])
    info_actions = list((state.get("information_gain_scoring") or {}).get("actions") or [])
    voi_plans = list((state.get("value_of_information_planner") or {}).get("plans") or [])
    debt = list((state.get("experiment_debt_queue") or {}).get("queue") or [])
    scale = list((state.get("treatment_confidence") or {}).get("scale") or [])
    abandon = list((state.get("treatment_confidence") or {}).get("abandon") or [])
    state["learning_upgrade_directive"] = {
        "scale_treatments": [row.get("treatment") for row in scale[:5]],
        "abandon_treatments": [row.get("treatment") for row in abandon[:5]],
        "portfolio_allocations": portfolio_allocations,
        "next_controlled_sibling": ((state.get("controlled_sibling_experiments") or {}).get("experiments") or [{}])[0],
        "next_information_gain_action": (info_actions or [{}])[0],
        "next_value_of_information_plan": (voi_plans or [{}])[0],
        "next_experiment_debt": (debt or [{}])[0],
        "evidence_scale_allowed": (state.get("evidence_sufficiency_gate") or {}).get("scale_allowed") or [],
        "compiled_policy_objective": (state.get("compiled_hunt_policy") or {}).get("objective"),
        "next_policy_job": ((state.get("policy_executor") or {}).get("assignments") or [{}])[0],
        "policy_backtest_verdict": (state.get("policy_backtester") or {}).get("verdict"),
        "policy_champion": ((state.get("policy_tournament") or {}).get("champion") or {}).get("policy_name"),
        "policy_safety_ok": (state.get("policy_safety_rail") or {}).get("ok"),
        "promoted_rules": (state.get("auto_promoted_field_manual") or {}).get("rules") or [],
        "policy_drift_status": (state.get("policy_drift_detector") or {}).get("status"),
        "policy_drift_reasons": (state.get("policy_drift_detector") or {}).get("reasons") or [],
        "stale_or_retest_routes": [
            row.get("route_key")
            for row in ((state.get("route_regime_half_life") or {}).get("routes") or [])[:10]
            if row.get("recommended_action") in {"retest", "probe"}
        ],
        "top_learning_market_cells": ((state.get("learning_market_map") or {}).get("cells") or [])[:10],
        "concept_alarm_count": (state.get("concept_drift_alarms") or {}).get("alarm_count")
            or len((state.get("concept_drift_alarms") or {}).get("alarms") or []),
        "next_revalidation": (((state.get("revalidation_scheduler") or {}).get("queue")
                                or (state.get("revalidation_scheduler") or {}).get("tasks") or [{}])[0]),
        "temporal_ensemble_primary": (state.get("temporal_ensemble_policy") or {}).get("primary_policy"),
        "temporal_ensemble_components": (state.get("temporal_ensemble_policy") or {}).get("components")
            or (state.get("temporal_ensemble_policy") or {}).get("ensemble") or [],
        "experiment_governor_top_decision": (state.get("active_experiment_governor") or {}).get("top_decision") or {},
        "route_state_focus": (state.get("route_state_machine") or {}).get("focus_routes") or [],
        "negative_avoid_routes": (state.get("negative_knowledge_bank") or {}).get("avoid_routes") or [],
        "promotion_survivor_top10": (state.get("promotion_survivor_model") or {}).get("promotion_survivor_top10") or [],
        "mutation_grammar_top_templates": (state.get("mutation_grammar_learner") or {}).get("top_templates") or [],
        "worker_rebalance_assignments": (state.get("real_time_worker_rebalancer") or {}).get("assignments") or [],
        "replay_recommended_policy": (state.get("hunt_replay_simulator") or {}).get("recommended_policy"),
        "top_resurrection": (state.get("resurrection_engine") or {}).get("top_resurrection") or {},
        "top_causal_mutation": ((state.get("causal_mutation_attribution") or {}).get("top_parameter_moves") or [{}])[0],
        "uncertainty_top_budget": (state.get("uncertainty_budgeting") or {}).get("top_budget") or {},
        "pareto_frontier_top": ((state.get("promotability_pareto_frontier") or {}).get("frontier") or [{}])[0],
        "false_lesson_highest_risk": (state.get("false_lesson_detector") or {}).get("highest_risk") or {},
        "experiment_stage_counts": (state.get("experiment_graduation_system") or {}).get("stage_counts") or {},
        "top_genealogy_diff": ((state.get("candidate_genealogy_diff_engine") or {}).get("diffs") or [{}])[0],
        "off_policy_recommended_policy": (state.get("off_policy_hunt_evaluator") or {}).get("recommended_policy"),
        "self_competition_champion": (state.get("self_competition_league") or {}).get("champion") or {},
        "evidence_contract_count": (state.get("evidence_contract_engine") or {}).get("contract_count"),
        "top_quality_decomposition": (state.get("live_beater_quality_decomposer") or {}).get("top_quality") or {},
        "contradiction_count": (state.get("contradiction_detector") or {}).get("contradiction_count"),
        "top_conflict_resolution": (state.get("learning_conflict_resolver") or {}).get("top_action") or {},
        "top_cohort": (state.get("cohort_based_memory") or {}).get("top_cohort") or {},
        "adaptive_throttle": (state.get("adaptive_hunt_throttle") or {}).get("controls") or {},
        "promotion_readiness_highest_risk": (state.get("promotion_readiness_simulator") or {}).get("highest_reject_risk") or {},
        "research_trace_count": (state.get("research_trace_ledger") or {}).get("entry_count"),
        "runtime_command_count": len((state.get("runtime_decision_kernel") or {}).get("commands") or []),
        "runtime_guardrails_ok": (state.get("runtime_guardrails") or {}).get("ok"),
        "runtime_degrade_mode": adapter.get("degrade_mode"),
        "runtime_adapter_summary": {
            "focus_routes": adapter.get("focus_routes") or [],
            "avoid_routes": adapter.get("avoid_routes") or [],
            "mutation_width": adapter.get("mutation_width"),
            "batch_size_multiplier": adapter.get("batch_size_multiplier"),
            "primary_command": adapter.get("primary_command") or {},
        },
        "worker_contract_count": worker_contracts.get("contract_count"),
        "reward_calibration_mae": reward_calibration.get("mean_abs_error"),
        "worst_reward_calibration": reward_calibration.get("worst_calibration") or {},
        "ab_experiment_assignment_count": (state.get("ab_route_experiment_executor") or {}).get("assignment_count"),
        "champion_challenger_slots": (state.get("champion_challenger_runtime_slots") or {}).get("slots") or [],
        "adaptive_experiment_stop_count": (state.get("adaptive_experiment_stopping") or {}).get("stop_count"),
        "best_counterfactual_command": (state.get("counterfactual_command_replay") or {}).get("best_counterfactual") or {},
        "experiment_contamination_ok": (state.get("experiment_contamination_guard") or {}).get("ok"),
        "learning_rate_mode": (state.get("learning_rate_controller") or {}).get("mode"),
        "top_worker_report_card": (state.get("worker_learning_report_cards") or {}).get("top_worker") or {},
        "top_experiment_promotion_trace": (state.get("experiment_to_promotion_trace") or {}).get("top_trace") or {},
        "zero_yield_count": (state.get("zero_yield_autopsy_engine") or {}).get("zero_yield_count"),
        "top_zero_yield_autopsy": (state.get("zero_yield_autopsy_engine") or {}).get("top_autopsy") or {},
        "stuck_reset_count": (state.get("stuck_loop_breaker") or {}).get("reset_count"),
        "highest_opportunity_cost": (state.get("opportunity_cost_meter") or {}).get("highest_cost") or {},
        "coverage_blind_spots": (state.get("search_space_coverage_map") or {}).get("blind_spots") or [],
        "live_beater_scarcity_mode": (state.get("live_beater_scarcity_mode") or {}).get("mode"),
        "alias_trap_count": (state.get("alias_trap_detector") or {}).get("trap_count"),
        "top_route_seed_quality": (state.get("route_seed_quality_score") or {}).get("top_seed") or {},
        "top_recovery_intervention": (state.get("recovery_playbook_generator") or {}).get("top_intervention") or {},
        "cross_run_favor_routes": (state.get("cross_run_hunt_memory_compiler") or {}).get("favor_routes") or [],
        "memory_reliability_top": ((state.get("memory_reliability_scorer") or {}).get("scores") or [{}])[0],
        "memory_falsification_count": len((state.get("memory_falsification_queue") or {}).get("queue") or []),
        "belief_retire_routes": (state.get("belief_retirement_engine") or {}).get("retire_routes") or [],
        "memory_provenance_subject_count": len((state.get("memory_provenance_explorer") or {}).get("entries") or []),
        "historical_disagreement_count": (state.get("current_vs_historical_disagreement_monitor") or {}).get("disagreement_count"),
        "memory_stress_task_count": len((state.get("memory_stress_test_pack") or {}).get("tasks") or []),
        "durable_memory_rule_count": (state.get("durable_memory_compression") or {}).get("rule_count"),
        "memory_qa_passed": (state.get("memory_qa_smoke_test") or {}).get("passed"),
        "top_hypothesis": (state.get("hypothesis_factory") or {}).get("top_hypothesis") or {},
        "top_hypothesis_quote": (((state.get("hypothesis_market_maker") or {}).get("quotes") or [{}])[0]),
        "hypothesis_bet_allocations": (state.get("real_time_bet_sizer") or {}).get("allocations") or [],
        "top_contrarian_experiment": (((state.get("contrarian_generator") or {}).get("experiments") or [{}])[0]),
        "learning_stop_loss_count": len((state.get("learning_stop_loss") or {}).get("stops") or []),
        "top_breakthrough": (state.get("breakthrough_detector") or {}).get("top_breakthrough") or {},
        "top_recipe": (state.get("pattern_to_recipe_compiler") or {}).get("top_recipe") or {},
        "hunt_narrative": (state.get("hunt_narrative_memory") or {}).get("summary"),
        "attention_total_spend": (state.get("attention_ledger") or {}).get("total_spend_units"),
        "top_wasted_spend": (state.get("wasted_spend_autopsy") or {}).get("top_waste") or {},
        "plateau_routes": (state.get("marginal_yield_curve") or {}).get("plateau_routes") or [],
        "attention_regret": (state.get("explore_exploit_regret_tracker") or {}).get("recommendation"),
        "top_worker_alpha": (state.get("worker_alpha_attribution") or {}).get("top_worker") or {},
        "budget_reallocation": {
            "focus_routes": (state.get("budget_reallocator") or {}).get("focus_routes") or [],
            "avoid_routes": (state.get("budget_reallocator") or {}).get("avoid_routes") or [],
            "recommendation": (state.get("budget_reallocator") or {}).get("recommendation"),
        },
        "time_aware_phase": (state.get("time_aware_hunt_plan") or {}).get("phase"),
        "spend_efficiency": (state.get("spend_efficiency_narrative") or {}).get("summary"),
        "fresh_idea_count": len((state.get("idea_novelty_ledger") or {}).get("fresh_ideas") or []),
        "idea_saturation_count": (state.get("idea_saturation_detector") or {}).get("saturation_count"),
        "top_creative_leap": (((state.get("creative_leap_scorer") or {}).get("leaps") or [{}])[0]),
        "failed_imagination_count": len((state.get("failed_imagination_autopsy") or {}).get("autopsies") or []),
        "top_mutation_gap": (state.get("mutation_grammar_gap_finder") or {}).get("top_gap") or {},
        "novelty_budget_pct": (state.get("novelty_budget_governor") or {}).get("novelty_budget_pct"),
        "idea_lineage_nodes": (state.get("idea_lineage_map") or {}).get("node_count"),
        "creative_brief": (state.get("creative_brief_compiler") or {}).get("summary"),
        "active_learning_modules": (state.get("learning_module_registry") or {}).get("active_modules") or [],
        "top_module_contribution": (state.get("module_contribution_attribution") or {}).get("top_module") or {},
        "module_conflict_count": (state.get("module_conflict_detector") or {}).get("conflict_count"),
        "top_module_reliability": (((state.get("module_reliability_scorer") or {}).get("scores") or [{}])[0]),
        "module_ablation_count": (state.get("module_ablation_planner") or {}).get("test_count"),
        "module_budget_top": (state.get("module_budget_governor") or {}).get("top_module") or {},
        "learning_system_health": (state.get("learning_system_self_audit") or {}).get("health_score"),
        "meta_learning_brief": (state.get("meta_learning_brief") or {}).get("summary"),
        "top_causal_intervention": (state.get("causal_intervention_scheduler") or {}).get("top_intervention") or {},
        "experiment_power_score": (state.get("experiment_power_calculator") or {}).get("overall_power_score"),
        "underpowered_experiment_count": len((state.get("experiment_power_calculator") or {}).get("underpowered_experiments") or []),
        "top_winner_fragility": (state.get("winner_fragility_profiler") or {}).get("top_fragility") or {},
        "top_live_beater_source": (state.get("live_beater_source_attribution") or {}).get("top_source") or {},
        "search_temperature": (state.get("adaptive_search_temperature_controller") or {}).get("temperature"),
        "top_route_interaction": (state.get("route_interaction_learner") or {}).get("top_interaction") or {},
        "false_discovery_flag_count": (state.get("false_discovery_firewall") or {}).get("flag_count"),
        "hunt_strategy_v2": (state.get("hunt_strategy_compiler_v2") or {}).get("strategy"),
        "hunt_strategy_v2_summary": (state.get("hunt_strategy_compiler_v2") or {}).get("summary"),
        "top_surviving_lesson": (state.get("lesson_survival_tracker") or {}).get("top_lesson") or {},
        "promotion_rejection_backprop_count": (state.get("promotion_rejection_backpropagation") or {}).get("rejection_count"),
        "lesson_decay_top": (state.get("lesson_decay_model") or {}).get("top_lesson") or {},
        "cross_hunt_causal_record_count": (state.get("cross_hunt_causal_memory") or {}).get("record_count"),
        "top_evidence_chain": (state.get("evidence_chain_ledger") or {}).get("top_chain") or {},
        "learning_court_case_count": (state.get("learning_disagreement_court") or {}).get("case_count"),
        "promotion_aware_top_candidate": (state.get("promotion_aware_search_objective") or {}).get("top_candidate") or {},
        "scientific_run_brief": (state.get("scientific_run_brief_v2") or {}).get("summary"),
        "strategy_genome_champion": (state.get("strategy_genome_registry") or {}).get("champion") or {},
        "top_strategy_challenger": (state.get("strategy_mutation_engine") or {}).get("top_challenger") or {},
        "strategy_tournament_champion": (state.get("strategy_tournament_memory") or {}).get("champion") or {},
        "selected_strategy_regime": (state.get("regime_conditioned_strategy_selector") or {}).get("regime"),
        "meta_objective": (state.get("meta_objective_optimizer") or {}).get("objective"),
        "exploration_debt_count": (state.get("exploration_debt_ledger") or {}).get("debt_count"),
        "strategy_red_team_top_attack": (state.get("adversarial_strategy_red_team") or {}).get("top_attack") or {},
        "autonomous_pivot": (state.get("autonomous_pivot_governor") or {}).get("pivot"),
        "autonomous_pivot_reason": (state.get("autonomous_pivot_governor") or {}).get("pivot_reason"),
        "top_learning_roi": (state.get("learning_roi_ledger") or {}).get("top_roi") or {},
        "prune_candidate_count": len((state.get("artifact_usefulness_pruner") or {}).get("prune_candidates") or []),
        "decision_trace_count": (state.get("decision_trace_explainer") or {}).get("trace_count"),
        "control_conflict_count": (state.get("control_surface_conflict_auditor") or {}).get("conflict_count"),
        "simplified_runtime_width": (state.get("runtime_control_simplifier") or {}).get("mutation_width"),
        "learning_cost_highest": (state.get("learning_cost_meter") or {}).get("highest_cost") or {},
        "ablation_replay_count": (state.get("ablation_replay_harness") or {}).get("test_count"),
        "architecture_fitness_verdict": (state.get("architecture_fitness_brief") or {}).get("verdict"),
        "architecture_fitness_summary": (state.get("architecture_fitness_brief") or {}).get("summary"),
        "schema_registry_valid": (state.get("learning_artifact_schema_registry") or {}).get("valid"),
        "artifact_dependency_root_count": len((state.get("artifact_dependency_graph") or {}).get("root_nodes") or []),
        "incremental_cache_reuse_count": (state.get("incremental_learning_cache") or {}).get("reuse_count"),
        "dashboard_status": (state.get("live_learning_dashboard_feed") or {}).get("status"),
        "runbook_worker_count": (state.get("hunt_runbook_compiler") or {}).get("worker_count"),
        "learning_failure_count": (state.get("learning_failure_sentinel") or {}).get("failure_count"),
        "warehouse_export_count": (state.get("cross_run_artifact_warehouse") or {}).get("export_count"),
        "pre_hunt_ready": (state.get("pre_hunt_readiness_gate") or {}).get("ready"),
        "online_causal_top_lane": (state.get("online_causal_bandit") or {}).get("top_lane"),
        "online_causal_allocations": (state.get("online_causal_bandit") or {}).get("allocations") or [],
        "variant_dna_top_gene": ((state.get("variant_dna_attribution") or {}).get("top_gene") or {}).get("gene"),
        "negative_gene_suppressed_count": (state.get("negative_gene_suppression") or {}).get("suppressed_count"),
        "winner_family_count": (state.get("live_winner_family_tree") or {}).get("family_count"),
        "exploration_frontier_count": (state.get("exploration_frontier_map") or {}).get("frontier_count"),
        "worker_personality_assignments": (state.get("adaptive_worker_personalities") or {}).get("assignments") or [],
        "cycle_learning_delta": (state.get("cycle_level_learning_delta") or {}).get("summary"),
        "promotion_rejection_v2_highest_risk": (state.get("promotion_rejection_predictor_v2") or {}).get("highest_risk") or {},
        "top_counterfactual": (state.get("counterfactual_hunt_simulator") or {}).get("top_counterfactual") or {},
        "missed_winner_count": (state.get("missed_winner_detector") or {}).get("missed_count"),
        "top_causal_regret": (state.get("causal_regret_ledger") or {}).get("top_regret") or {},
        "search_grammar_top_templates": (state.get("adaptive_search_grammar_generator") or {}).get("top_templates") or [],
        "hypothesis_court_counts": {
            "scale": (state.get("live_hypothesis_kill_scale_court") or {}).get("scale_count"),
            "kill": (state.get("live_hypothesis_kill_scale_court") or {}).get("kill_count"),
            "retest": (state.get("live_hypothesis_kill_scale_court") or {}).get("retest_count"),
        },
        "route_interaction_v2_top": (state.get("route_interaction_matrix_v2") or {}).get("top_interaction") or {},
        "promotion_shadow_top": (state.get("promotion_survival_shadow_scoring") or {}).get("top_shadow") or {},
        "autopilot_policy_packet": (state.get("hunt_autopilot_policy_compiler") or {}).get("policy_packet") or {},
        "verified_learning_claim_count": (state.get("learning_claim_verifier") or {}).get("verified_count"),
        "causal_confidence_top": (state.get("causal_confidence_calibration") or {}).get("top_confidence") or {},
        "false_discovery_warning_count": (state.get("false_discovery_early_warning") or {}).get("warning_count"),
        "evidence_threshold_mode": (state.get("adaptive_evidence_thresholds") or {}).get("mode"),
        "self_debate_resolution": (state.get("self_debate_search_council") or {}).get("resolution") or {},
        "compressed_memory_rule_count": (state.get("experiment_memory_compression") or {}).get("rule_count"),
        "learning_drift_count": (state.get("learning_drift_monitor") or {}).get("drift_count"),
        "promotion_first_policy_packet": (state.get("promotion_first_autopilot_v2") or {}).get("policy_packet") or {},
        "memory_horizon_summary": (state.get("multi_horizon_memory_stack") or {}).get("summary") or {},
        "lesson_half_life_top": (state.get("lesson_half_life_engine_v2") or {}).get("top_lesson") or {},
        "cross_hunt_replay_top": (state.get("cross_hunt_strategy_replay") or {}).get("top_replay") or {},
        "temporal_regime_top": (state.get("temporal_regime_fingerprinting") or {}).get("top_fingerprint") or {},
        "longitudinal_survival_top": (state.get("longitudinal_promotion_survival_model") or {}).get("top_survival_feature") or {},
        "memory_conflict_v2_count": (state.get("memory_conflict_court_v2") or {}).get("case_count"),
        "strategy_aging_counts": (state.get("strategy_aging_dashboard") or {}).get("aging_counts") or {},
        "next_hunt_opening_policy": (state.get("next_hunt_opening_policy_compiler") or {}).get("policy_packet") or {},
        "question_planner_top": (state.get("question_driven_hunt_planner") or {}).get("top_question") or {},
        "expected_information_gain_top": (state.get("expected_information_gain_scorer_v2") or {}).get("top_score") or {},
        "uncertainty_heatmap_top": (state.get("uncertainty_heatmap") or {}).get("top_cell") or {},
        "adaptive_experiment_next_step": (state.get("adaptive_experiment_sequencer") or {}).get("next_step") or {},
        "learning_value_stop_count": (state.get("learning_value_stop_loss") or {}).get("stop_count"),
        "causal_question_top": (state.get("causal_question_ledger") or {}).get("top_question") or {},
        "epistemic_role_assignments": (state.get("worker_epistemic_roles_v2") or {}).get("assignments") or [],
        "compiled_hypotheses": (state.get("hunt_hypothesis_compiler") or {}).get("hypotheses") or [],
        "experiment_contract_top": (state.get("experiment_contract_compiler") or {}).get("top_contract") or {},
        "control_route_top": (state.get("control_route_matcher") or {}).get("top_match") or {},
        "sequential_test_top": (state.get("sequential_test_monitor") or {}).get("top_decision") or {},
        "causal_effect_top": (state.get("causal_effect_size_ledger") or {}).get("top_effect") or {},
        "false_positive_pressure": {
            "score": (state.get("false_positive_pressure_gauge") or {}).get("pressure_score"),
            "mode": (state.get("false_positive_pressure_gauge") or {}).get("mode"),
        },
        "exploration_paydown_top": (state.get("exploration_debt_paydown_planner") or {}).get("top_plan") or {},
        "promotion_power_top": (state.get("promotion_aware_power_planner") or {}).get("top_plan") or {},
        "scientific_executive_summary": (state.get("scientific_hunt_executive") or {}).get("summary") or {},
        "scientific_executive_top_command": (state.get("scientific_hunt_executive") or {}).get("top_command") or {},
        "live_candidate_evidence_top": (state.get("live_candidate_evidence_builder") or {}).get("top_packet") or {},
        "promotion_failure_v3_highest_risk": (state.get("promotion_failure_predictor_v3") or {}).get("highest_risk") or {},
        "evidence_gap_top_task": (state.get("evidence_gap_router") or {}).get("top_task") or {},
        "review_ready_v2_top": (state.get("review_ready_queue_v2") or {}).get("top_ready") or {},
        "promotion_scorecard_top": (state.get("promotion_evidence_scorecard") or {}).get("top_scorecard") or {},
        "candidate_lineage_top": (state.get("candidate_lineage_explainer_v2") or {}).get("top_explanation") or {},
        "live_control_differential_top": (state.get("live_vs_control_differential_report") or {}).get("top_report") or {},
        "promotion_packet_executive_summary": (state.get("promotion_packet_executive") or {}).get("summary") or {},
        "promotion_packet_top_decision": (state.get("promotion_packet_executive") or {}).get("top_decision") or {},
        "unified_learning_summary": (state.get("unified_learning_state_reducer") or {}).get("summary") or {},
        "artifact_priority_top": (state.get("artifact_priority_arbitration_engine") or {}).get("top_artifact") or {},
        "evidence_provenance_top": (state.get("evidence_provenance_graph") or {}).get("top_node") or {},
        "counterfactual_promotion_top": (state.get("counterfactual_promotion_replay") or {}).get("top_replay") or {},
        "candidate_survival_top": (state.get("candidate_survival_simulator") or {}).get("top_simulation") or {},
        "regime_shift_v2": {
            "mode": (state.get("live_regime_shift_detector_v2") or {}).get("mode"),
            "shift_score": (state.get("live_regime_shift_detector_v2") or {}).get("shift_score"),
        },
        "module_trust_top": (state.get("adaptive_trust_weights_per_module") or {}).get("top_module") or {},
        "worker_skill_top": (state.get("worker_skill_elo_v2") or {}).get("top_worker") or {},
        "knowledge_graph_summary": (state.get("route_genome_knowledge_graph") or {}).get("summary") or {},
        "causal_interaction_top": (state.get("causal_feature_interaction_miner") or {}).get("top_interaction") or {},
        "overfit_signature_top": (state.get("overfit_signature_library") or {}).get("top_signature") or {},
        "rejection_memory_top": (state.get("review_rejection_memory_bank_v2") or {}).get("top_memory") or {},
        "learning_budget_top": (state.get("learning_budget_optimizer") or {}).get("top_allocation") or {},
        "novelty_floor_top": (state.get("search_novelty_floor") or {}).get("summary") or {},
        "breakthrough_top": (state.get("breakthrough_escalation_protocol") or {}).get("top_breakthrough") or {},
        "false_discovery_backpressure": {
            "score": (state.get("false_discovery_backpressure_controller") or {}).get("pressure_score"),
            "mode": (state.get("false_discovery_backpressure_controller") or {}).get("mode"),
        },
        "strategy_portfolio_top": (state.get("multi_armed_strategy_portfolio") or {}).get("top_arm") or {},
        "historical_lesson_ab_top": (state.get("historical_lesson_ab_harness") or {}).get("top_test") or {},
        "promotion_packet_diff_top": (state.get("promotion_packet_diff_engine") or {}).get("top_diff") or {},
        "repair_recipe_top": (state.get("candidate_repair_recipe_generator") or {}).get("top_recipe") or {},
        "red_team_top": (state.get("automated_red_team_reviewer") or {}).get("top_objection") or {},
        "field_manual_top_rule": (state.get("learning_compression_field_manual") or {}).get("top_rule") or {},
        "outcome_attribution_top": (state.get("hunt_outcome_attribution_v2") or {}).get("top_attribution") or {},
        "world_state_summary": (state.get("world_state_dashboard_artifact") or {}).get("summary") or {},
        "meta_learning_governor_summary": (state.get("meta_learning_governor") or {}).get("summary") or {},
        "elite_learning_summary": (state.get("elite_learning_system_summary") or {}).get("summary") or {},
        "elite_next_best_question": (state.get("world_state_next_best_question_engine") or {}).get("top_record") or {},
        "elite_artifact_count": len(ELITE_LEARNING_ARTIFACTS),
        "proof_learning_summary": (state.get("proof_learning_system_summary") or {}).get("summary") or {},
        "truth_ledger_top_claim": (state.get("learning_truth_ledger") or {}).get("top_record") or {},
        "research_agenda_top_question": (state.get("adaptive_research_agenda") or {}).get("top_record") or {},
        "review_board_decision": (state.get("autonomous_hunt_review_board") or {}).get("top_record") or {},
        "proof_artifact_count": len(PROOF_LEARNING_ARTIFACTS),
        "closed_loop_control_summary": (state.get("closed_loop_control_learning_summary") or {}).get("summary") or {},
        "bayesian_top_belief": (state.get("bayesian_belief_engine") or {}).get("top_record") or {},
        "active_experiment_next": (state.get("active_experiment_selector") or {}).get("top_record") or {},
        "uncertainty_rank_top": (state.get("uncertainty_aware_top100") or {}).get("top_record") or {},
        "search_drift_status": (state.get("live_search_drift_detector") or {}).get("top_record") or {},
        "explore_exploit_budget_mix": (state.get("exploration_exploitation_governor") or {}).get("top_record") or {},
        "meta_strategy_bandit_top": (state.get("meta_strategy_bandits") or {}).get("top_record") or {},
        "learning_status_feed": (state.get("real_time_learning_dashboard_feed") or {}).get("top_record") or {},
        "closed_loop_control_artifact_count": len(CLOSED_LOOP_CONTROL_ARTIFACTS),
        "world_model_nervous_system_summary": (state.get("world_model_nervous_system_summary") or {}).get("summary") or {},
        "nervous_system_top_truth": (state.get("hypothesis_dependency_graph") or {}).get("top_record") or {},
        "nervous_system_top_genetics": (state.get("trait_inheritance_scoreboard") or {}).get("top_record") or {},
        "nervous_system_top_worker": (state.get("worker_strategy_specialization") or {}).get("top_record") or {},
        "nervous_system_top_route": (state.get("multi_armed_route_portfolio") or {}).get("top_record") or {},
        "nervous_system_top_indicator": (state.get("indicator_family_causal_map") or {}).get("top_record") or {},
        "nervous_system_top_regime": (state.get("regime_fingerprint_memory") or {}).get("top_record") or {},
        "nervous_system_top_promotion": (state.get("promotion_packet_completeness_optimizer") or {}).get("top_record") or {},
        "nervous_system_world_audit": (state.get("world_model_self_audit_loop") or {}).get("top_record") or {},
        "world_model_nervous_artifact_count": len(WORLD_MODEL_NERVOUS_SYSTEM_ARTIFACTS),
        "orchestration_learning_summary": (state.get("orchestration_learning_summary") or {}).get("summary") or {},
        "orchestration_artifact_governance_top": (state.get("artifact_governance_dashboard") or {}).get("top_record") or {},
        "orchestration_belief_top": (state.get("belief_to_action_explainability_map") or {}).get("top_record") or {},
        "orchestration_experiment_top": (state.get("experiment_queue_optimizer") or {}).get("top_record") or {},
        "orchestration_worker_top": (state.get("worker_task_fit_recommender_v2") or {}).get("top_record") or {},
        "orchestration_search_top": (state.get("search_space_topology_mapper") or {}).get("top_record") or {},
        "orchestration_promotion_top": (state.get("promotion_proof_burden_allocator") or {}).get("top_record") or {},
        "orchestration_memory_top": (state.get("memory_entropy_monitor") or {}).get("top_record") or {},
        "orchestration_hunt_exec_top": (state.get("hunt_executive_controller") or {}).get("top_record") or {},
        "orchestration_artifact_count": len(ORCHESTRATION_LEARNING_ARTIFACTS),
        "fitness_selection_summary": (state.get("fitness_selection_summary") or {}).get("summary") or {},
        "fitness_ablation_top": (state.get("learning_layer_ablation_harness") or {}).get("top_record") or {},
        "fitness_scorecard_top": (state.get("artifact_fitness_scorecard") or {}).get("top_record") or {},
        "fitness_conflict_top": (state.get("layer_conflict_matrix") or {}).get("top_record") or {},
        "fitness_duplicate_top": (state.get("artifact_duplicate_clusterer") or {}).get("top_record") or {},
        "fitness_roi_top": (state.get("artifact_runtime_roi_ranker") or {}).get("top_record") or {},
        "fitness_trust_policy": (state.get("learning_layer_trust_policy") or {}).get("top_record") or {},
        "fitness_demote_queue_top": (state.get("artifact_demote_retire_queue") or {}).get("top_record") or {},
        "fitness_world_report": (state.get("world_model_fitness_report") or {}).get("top_record") or {},
        "fitness_selection_artifact_count": len(FITNESS_SELECTION_ARTIFACTS),
        "pre_hunt_strategy": (state.get("pre_hunt_strategy_selector") or {}).get("strategy"),
        "cold_start_focus_routes": (state.get("cold_start_route_pack_generator") or {}).get("focus_routes") or [],
        "decayed_favor_treatments": (state.get("longitudinal_treatment_decay") or {}).get("favor_treatments") or [],
        "promotion_survival_avoid_routes": (state.get("run_level_promotion_survival_feedback") or {}).get("avoid_opening_routes") or [],
        "memory_conflict_count": (state.get("memory_conflict_arbiter") or {}).get("conflict_count"),
        "opening_playbook_strategy": (state.get("hunt_opening_playbook") or {}).get("strategy"),
        "cross_run_regression_passed": (state.get("cross_run_learning_regression_test") or {}).get("passed"),
        "next_runtime_job": (state.get("autonomous_hunt_planner") or {}).get("next_job") or {},
        "best_closed_loop_action": (state.get("closed_loop_reward_model") or {}).get("best_action") or {},
        "action_league_champion": (state.get("action_elo_league") or {}).get("champion") or {},
        "hunt_brief": (state.get("human_readable_hunt_brief") or {}).get("brief"),
        "truth_calibration_confidence": (state.get("belief_revision_engine") or {}).get("truth_calibration_confidence"),
        "self_audit_score": (state.get("self_audit_score") or {}).get("self_audit_score"),
        "truth_first_rank1": ((state.get("truth_first_promotion_objective") or {}).get("leaderboard") or [{}])[0],
        "top_worker_specialties": (state.get("worker_specialization") or {}).get("workers") or [],
    }
    return state


def online_allocation(route_arms: list[dict[str, Any]], basin_status: list[dict[str, Any]], novelty_budget_pct: float) -> dict[str, Any]:
    blocked = {row["route_key"] for row in basin_status if row.get("status") in {"exhausted", "rotate"}}
    usable = [arm for arm in route_arms if arm.get("route_key") not in blocked]
    total = sum(max(1.0, float(arm.get("online_score") or 0.0)) for arm in usable) or 1.0
    exploit = max(0.0, 100.0 - float(novelty_budget_pct) - 10.0)
    allocation = []
    for arm in usable[:12]:
        allocation.append({
            "route_key": arm.get("route_key"),
            "recommended_budget_pct": round(exploit * max(1.0, float(arm.get("online_score") or 0.0)) / total, 2),
            "online_score": arm.get("online_score"),
            "best_variant": arm.get("best_variant"),
        })
    allocation.append({
        "route_key": "novelty_exploration",
        "recommended_budget_pct": round(float(novelty_budget_pct), 2),
        "online_score": 0.0,
        "best_variant": None,
    })
    allocation.append({
        "route_key": "broad_exploration",
        "recommended_budget_pct": 10.0,
        "online_score": 0.0,
        "best_variant": None,
    })
    return {
        "schema_version": 1,
        "source": "step2_online_learning",
        "allocation": allocation,
    }


def update_online_state(
    *,
    run_dir: str | Path,
    rankings: dict[str, Any],
    cycles: list[dict[str, Any]],
    previous_state: dict[str, Any] | None = None,
    novelty_budget_pct: float = 15.0,
    promotion_feedback_rows: list[dict[str, Any]] | None = None,
    promotion_review_directives: list[dict[str, Any]] | None = None,
    experiment_plan: dict[str, Any] | None = None,
    treatment_prior_model: dict[str, Any] | None = None,
    treatment_worker_budget: dict[str, Any] | None = None,
    treatment_confidence: dict[str, Any] | None = None,
    controlled_sibling_experiments: dict[str, Any] | None = None,
    worker_specialization: dict[str, Any] | None = None,
    regime_learning: dict[str, Any] | None = None,
    promotion_reject_simulator: dict[str, Any] | None = None,
    search_portfolio: dict[str, Any] | None = None,
    causal_experiment_registry: dict[str, Any] | None = None,
    experiment_debt_queue: dict[str, Any] | None = None,
    information_gain_scoring: dict[str, Any] | None = None,
    value_of_information_planner: dict[str, Any] | None = None,
    decision_change_tracker: dict[str, Any] | None = None,
    hypothesis_quality_scoring: dict[str, Any] | None = None,
    evidence_sufficiency_gate: dict[str, Any] | None = None,
    counterfactual_shadow_board: dict[str, Any] | None = None,
    prediction_calibration_ledger: dict[str, Any] | None = None,
    belief_revision_engine: dict[str, Any] | None = None,
    adversarial_red_team_learner: dict[str, Any] | None = None,
    out_of_distribution_detector: dict[str, Any] | None = None,
    memory_compression_distiller: dict[str, Any] | None = None,
    self_audit_score: dict[str, Any] | None = None,
    truth_first_promotion_objective: dict[str, Any] | None = None,
    learning_velocity_dashboard: dict[str, Any] | None = None,
    compiled_hunt_policy: dict[str, Any] | None = None,
    policy_executor: dict[str, Any] | None = None,
    adaptive_worker_assignment: dict[str, Any] | None = None,
    policy_backtester: dict[str, Any] | None = None,
    policy_mutation_engine: dict[str, Any] | None = None,
    policy_tournament: dict[str, Any] | None = None,
    champion_challenger_memory: dict[str, Any] | None = None,
    regime_specific_policies: dict[str, Any] | None = None,
    causal_graph_of_learning: dict[str, Any] | None = None,
    policy_safety_rail: dict[str, Any] | None = None,
    auto_promoted_field_manual: dict[str, Any] | None = None,
    policy_drift_detector: dict[str, Any] | None = None,
    route_regime_half_life: dict[str, Any] | None = None,
    learning_market_map: dict[str, Any] | None = None,
    concept_drift_alarms: dict[str, Any] | None = None,
    revalidation_scheduler: dict[str, Any] | None = None,
    temporal_ensemble_policy: dict[str, Any] | None = None,
    active_experiment_governor: dict[str, Any] | None = None,
    route_state_machine: dict[str, Any] | None = None,
    negative_knowledge_bank: dict[str, Any] | None = None,
    promotion_survivor_model: dict[str, Any] | None = None,
    mutation_grammar_learner: dict[str, Any] | None = None,
    real_time_worker_rebalancer: dict[str, Any] | None = None,
    hunt_replay_simulator: dict[str, Any] | None = None,
    resurrection_engine: dict[str, Any] | None = None,
    causal_mutation_attribution: dict[str, Any] | None = None,
    uncertainty_budgeting: dict[str, Any] | None = None,
    promotability_pareto_frontier: dict[str, Any] | None = None,
    false_lesson_detector: dict[str, Any] | None = None,
    experiment_graduation_system: dict[str, Any] | None = None,
    candidate_genealogy_diff_engine: dict[str, Any] | None = None,
    off_policy_hunt_evaluator: dict[str, Any] | None = None,
    self_competition_league: dict[str, Any] | None = None,
    evidence_contract_engine: dict[str, Any] | None = None,
    live_beater_quality_decomposer: dict[str, Any] | None = None,
    contradiction_detector: dict[str, Any] | None = None,
    learning_conflict_resolver: dict[str, Any] | None = None,
    cohort_based_memory: dict[str, Any] | None = None,
    adaptive_hunt_throttle: dict[str, Any] | None = None,
    promotion_readiness_simulator: dict[str, Any] | None = None,
    research_trace_ledger: dict[str, Any] | None = None,
    runtime_decision_kernel: dict[str, Any] | None = None,
    action_outcome_tracker: dict[str, Any] | None = None,
    closed_loop_reward_model: dict[str, Any] | None = None,
    autonomous_hunt_planner: dict[str, Any] | None = None,
    runtime_guardrails: dict[str, Any] | None = None,
    command_replay_ledger: dict[str, Any] | None = None,
    action_elo_league: dict[str, Any] | None = None,
    human_readable_hunt_brief: dict[str, Any] | None = None,
    meta_hunt_strategy: dict[str, Any] | None = None,
    run_to_run_postmortem: dict[str, Any] | None = None,
) -> dict[str, Any]:
    run_dir = Path(run_dir)
    previous_state = previous_state or read_state(run_dir / "online_state.json")
    rows = list(rankings.get("raw_leaderboard") or [])
    previous_feedback = previous_state.get("promotion_review_feedback") if isinstance(previous_state.get("promotion_review_feedback"), list) else []
    family_memory = hunt_intel.family_rejection_memory(previous_feedback + list(promotion_feedback_rows or []))
    downweight_families = {
        str(row.get("family_key"))
        for row in family_memory.get("families") or []
        if row.get("recommended_action") == "downweight_family"
    }
    route_arms = _route_arm_updates(rows, previous_state)
    mutation_controls = _mutation_controls(route_arms, previous_state)
    basin_status = _basin_status(route_arms, mutation_controls)
    triage = triage_candidates(rows, downweight_families=downweight_families)
    allocation = online_allocation(route_arms, basin_status, novelty_budget_pct)
    learning = hunt_intel.learning_report(
        rows=rows,
        cycles=cycles,
        all_rows=list(rankings.get("diagnostic_rows") or rankings.get("decorated_rows") or rows),
        source="step2_online_learning",
    )
    state = {
        "schema_version": 1,
        "source": "step2_online_learning",
        "updated_at": _now_epoch(),
        "cycles_completed": len(cycles),
        "cycle_yield": _cycle_yield(cycles),
        "recent_cycles": list(cycles[-20:]),
        "current_phase": (learning.get("curriculum_state") or {}).get("current_phase"),
        "route_arms": route_arms,
        "online_allocation": allocation,
        "mutation_controls": mutation_controls,
        "basin_status": basin_status,
        "candidate_triage": triage,
        "validation_queue": (learning.get("auto_validation_scheduler") or {}).get("queue") or [],
        "stop_continue": learning.get("stop_continue_criteria") or {},
        "top_candidates": rows[:10],
    }
    state = apply_promotion_review_feedback(
        {**previous_state, **state},
        promotion_feedback_rows,
        promotion_review_directives,
    )
    state = apply_experiment_plan(state, experiment_plan)
    state = apply_treatment_priors(state, treatment_prior_model, treatment_worker_budget)
    state = apply_learning_upgrade_controls(
        state,
        treatment_confidence=treatment_confidence,
        controlled_sibling_experiments=controlled_sibling_experiments,
        worker_specialization=worker_specialization,
        regime_learning=regime_learning,
        promotion_reject_simulator=promotion_reject_simulator,
        search_portfolio=search_portfolio,
        causal_experiment_registry=causal_experiment_registry,
        experiment_debt_queue=experiment_debt_queue,
        information_gain_scoring=information_gain_scoring,
        value_of_information_planner=value_of_information_planner,
        decision_change_tracker=decision_change_tracker,
        hypothesis_quality_scoring=hypothesis_quality_scoring,
        evidence_sufficiency_gate=evidence_sufficiency_gate,
        counterfactual_shadow_board=counterfactual_shadow_board,
        prediction_calibration_ledger=prediction_calibration_ledger,
        belief_revision_engine=belief_revision_engine,
        adversarial_red_team_learner=adversarial_red_team_learner,
        out_of_distribution_detector=out_of_distribution_detector,
        memory_compression_distiller=memory_compression_distiller,
        self_audit_score=self_audit_score,
        truth_first_promotion_objective=truth_first_promotion_objective,
        learning_velocity_dashboard=learning_velocity_dashboard,
        compiled_hunt_policy=compiled_hunt_policy,
        policy_executor=policy_executor,
        adaptive_worker_assignment=adaptive_worker_assignment,
        policy_backtester=policy_backtester,
        policy_mutation_engine=policy_mutation_engine,
        policy_tournament=policy_tournament,
        champion_challenger_memory=champion_challenger_memory,
        regime_specific_policies=regime_specific_policies,
        causal_graph_of_learning=causal_graph_of_learning,
        policy_safety_rail=policy_safety_rail,
        auto_promoted_field_manual=auto_promoted_field_manual,
        policy_drift_detector=policy_drift_detector,
        route_regime_half_life=route_regime_half_life,
        learning_market_map=learning_market_map,
        concept_drift_alarms=concept_drift_alarms,
        revalidation_scheduler=revalidation_scheduler,
        temporal_ensemble_policy=temporal_ensemble_policy,
        active_experiment_governor=active_experiment_governor,
        route_state_machine=route_state_machine,
        negative_knowledge_bank=negative_knowledge_bank,
        promotion_survivor_model=promotion_survivor_model,
        mutation_grammar_learner=mutation_grammar_learner,
        real_time_worker_rebalancer=real_time_worker_rebalancer,
        hunt_replay_simulator=hunt_replay_simulator,
        resurrection_engine=resurrection_engine,
        causal_mutation_attribution=causal_mutation_attribution,
        uncertainty_budgeting=uncertainty_budgeting,
        promotability_pareto_frontier=promotability_pareto_frontier,
        false_lesson_detector=false_lesson_detector,
        experiment_graduation_system=experiment_graduation_system,
        candidate_genealogy_diff_engine=candidate_genealogy_diff_engine,
        off_policy_hunt_evaluator=off_policy_hunt_evaluator,
        self_competition_league=self_competition_league,
        evidence_contract_engine=evidence_contract_engine,
        live_beater_quality_decomposer=live_beater_quality_decomposer,
        contradiction_detector=contradiction_detector,
        learning_conflict_resolver=learning_conflict_resolver,
        cohort_based_memory=cohort_based_memory,
        adaptive_hunt_throttle=adaptive_hunt_throttle,
        promotion_readiness_simulator=promotion_readiness_simulator,
        research_trace_ledger=research_trace_ledger,
        runtime_decision_kernel=runtime_decision_kernel,
        action_outcome_tracker=action_outcome_tracker,
        closed_loop_reward_model=closed_loop_reward_model,
        autonomous_hunt_planner=autonomous_hunt_planner,
        runtime_guardrails=runtime_guardrails,
        command_replay_ledger=command_replay_ledger,
        action_elo_league=action_elo_league,
        human_readable_hunt_brief=human_readable_hunt_brief,
        meta_hunt_strategy=meta_hunt_strategy,
        run_to_run_postmortem=run_to_run_postmortem,
    )
    state["command_outcome_backfill"] = command_outcome_backfill(previous_state, rankings, cycles)
    state["action_reward_calibration"] = action_reward_calibration(state, state["command_outcome_backfill"])
    state["runtime_command_adapter"] = runtime_command_adapter(state)
    state["degraded_runtime_command_packet"] = degraded_runtime_command_packet(state)
    state["worker_job_contracts"] = planner_worker_contracts(state)
    state["command_delta_status"] = command_delta_status(previous_state, state)
    directive = state.get("learning_upgrade_directive") if isinstance(state.get("learning_upgrade_directive"), dict) else {}
    directive.update({
        "runtime_degrade_mode": bool((state.get("runtime_command_adapter") or {}).get("degrade_mode")),
        "runtime_adapter_summary": {
            "focus_routes": (state.get("runtime_command_adapter") or {}).get("focus_routes") or [],
            "avoid_routes": (state.get("runtime_command_adapter") or {}).get("avoid_routes") or [],
            "mutation_width": (state.get("runtime_command_adapter") or {}).get("mutation_width"),
            "batch_size_multiplier": (state.get("runtime_command_adapter") or {}).get("batch_size_multiplier"),
            "primary_command": (state.get("runtime_command_adapter") or {}).get("primary_command") or {},
        },
        "worker_contract_count": (state.get("worker_job_contracts") or {}).get("contract_count"),
        "command_delta": state.get("command_delta_status") or {},
        "command_outcome_count": (state.get("command_outcome_backfill") or {}).get("outcome_count"),
        "reward_calibration_mae": (state.get("action_reward_calibration") or {}).get("mean_abs_error"),
        "worst_reward_calibration": (state.get("action_reward_calibration") or {}).get("worst_calibration") or {},
    })
    state["learning_upgrade_directive"] = directive
    state["mutation_controls"] = _mutation_controls(route_arms, state)
    state["basin_status"] = _basin_status(route_arms, state["mutation_controls"])
    state["online_allocation"] = online_allocation(route_arms, state["basin_status"], novelty_budget_pct)
    old_best = ((previous_state.get("top_candidates") or [{}])[0] or {}).get("behavior_key") if previous_state else None
    new_best = ((rows or [{}])[0] or {}).get("behavior_key")
    if new_best and new_best != old_best:
        append_event(run_dir, "new_behavior_unique_leader", {
            "variant": (rows[0] or {}).get("variant"),
            "route_key": hunt_intel.route_key_from_row(rows[0]),
            "step2_pnl": hunt_intel.pnl(rows[0]),
        })
    old_alloc = previous_state.get("online_allocation", {}).get("allocation") if isinstance(previous_state.get("online_allocation"), dict) else []
    new_alloc = allocation.get("allocation") or []
    if old_alloc[:3] != new_alloc[:3]:
        append_event(run_dir, "route_allocation_changed", {"allocation": new_alloc[:5]})
    for status in basin_status:
        if status.get("status") in {"exhausted", "rotate", "shrink_or_rotate"}:
            append_event(run_dir, "basin_status_changed", status)
    _write_json(run_dir / "online_state.json", state)
    return state


def focus_routes_from_state(state: dict[str, Any], limit: int = 6) -> list[str]:
    allocation = (state.get("online_allocation") or {}).get("allocation") or []
    routes = []
    packet = active_runtime_command_packet(state)
    for route in packet.get("focus_routes") or []:
        route = str(route or "")
        if route and route not in routes:
            routes.append(route)
        if len(routes) >= limit:
            return routes
    for route in (state.get("route_state_machine") or {}).get("focus_routes") or []:
        route = str(route or "")
        if route and route not in routes:
            routes.append(route)
        if len(routes) >= limit:
            return routes
    for route_row in (state.get("route_regime_half_life") or {}).get("routes") or []:
        if route_row.get("recommended_action") not in {"boost", "probe", "retest"}:
            continue
        route = str(route_row.get("route_key") or "")
        if route and route not in routes:
            routes.append(route)
        if len(routes) >= limit:
            return routes
    for arm in allocation:
        route = str(arm.get("route_key") or "")
        if route and route not in {"novelty_exploration", "broad_exploration", "exploration"}:
            routes.append(route)
        if len(routes) >= limit:
            break
    return routes


def mutation_scale_for_route(state: dict[str, Any], route: str | None = None) -> float:
    adapter = runtime_command_adapter(state)
    adapter_scale = float(adapter.get("mutation_scale_multiplier") or 1.0)
    controls = state.get("mutation_controls") if isinstance(state.get("mutation_controls"), dict) else {}
    if route and route in controls:
        return round(float((controls.get(route) or {}).get("scale") or 1.0) * adapter_scale, 4)
    values = [float((item or {}).get("scale") or 1.0) for item in controls.values() if isinstance(item, dict)]
    if not values:
        return round(adapter_scale, 4)
    return round((sum(values[:5]) / max(1, min(5, len(values)))) * adapter_scale, 4)
