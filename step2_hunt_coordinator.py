"""Coordinate Step 2 hunts against one certified cache and shared score cache."""
from __future__ import annotations

import argparse
import json
import os
import random
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import step2_manifest_resolver
import step2_candidate_work_queue
import step2_score_cache
import step2_hunt_intelligence as hunt_intel
import step2_learning_db
import active_engine_baseline
import baseline_drift_sentinel
import step2_evaluation_envelope
import tournament_safety


HERE = Path(__file__).resolve().parent
CT = ZoneInfo("America/Chicago")
DEFAULT_OUT = HERE / "postmortem" / "backtests" / "step2_hunt_coordinator"
HUNTER_SCRIPTS = {
    "adaptive": "step2_adaptive_hunter.py",
    "local": "step2_local_hill_hunter.py",
    "router": "step2_router_hunter.py",
}


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


def _write_json(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n"
    try:
        if path.exists() and path.read_text(encoding="utf-8") == rendered:
            return str(path.resolve())
    except Exception:
        pass
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.{int(time.time() * 1000)}.tmp")
    tmp.write_text(rendered, encoding="utf-8")
    os.replace(tmp, path)
    return str(path.resolve())


def _read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return default


def _compact_coordinator_payload(payload: dict[str, Any]) -> dict[str, Any]:
    leaderboard = list(payload.get("leaderboard") or [])
    winners = list(payload.get("winners") or [])
    runs = list(payload.get("runs") or [])
    score_cache = payload.get("score_cache") if isinstance(payload.get("score_cache"), dict) else {}
    return {
        "ok": payload.get("ok"),
        "path": payload.get("path"),
        "compiled_decision_tape": payload.get("compiled_decision_tape"),
        "hunters": payload.get("hunters"),
        "winner_count": len(winners),
        "leaderboard_count": len(leaderboard),
        "run_count": len(runs),
        "active_step2_pnl": payload.get("active_step2_pnl"),
        "top_candidate": _compact_row(leaderboard[0] if leaderboard else {}),
        "runs": [
            {
                "hunter": row.get("hunter"),
                "ok": row.get("ok"),
                "returncode": row.get("returncode"),
                "elapsed_sec": row.get("elapsed_sec"),
                "summary_path": row.get("summary_path"),
                "stdout_path": row.get("stdout_path"),
                "stderr_path": row.get("stderr_path"),
            }
            for row in runs[:10]
            if isinstance(row, dict)
        ],
        "score_cache": {
            key: score_cache.get(key)
            for key in ("entries", "hits", "decision_entries", "decision_hits", "behavior_entries", "behavior_hits", "db")
            if key in score_cache
        },
    }


def _compact_row(row: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(row, dict) or not row:
        return {}
    return {
        "variant": row.get("variant") or row.get("name"),
        "step2_pnl": row.get("step2_pnl"),
        "step2_delta_vs_active": row.get("step2_delta_vs_active") or row.get("delta_vs_active"),
        "promotion_readiness_score": row.get("promotion_readiness_score"),
        "route_key": hunt_intel.route_key_from_row(row),
    }


def _minimal_artifact_mode(args: argparse.Namespace) -> bool:
    return str(getattr(args, "coordinator_artifact_mode", "full") or "full") == "minimal"


def _score_cache_stats(args: argparse.Namespace) -> dict[str, Any]:
    cache = step2_score_cache.ScoreCache(args.score_cache_db)
    try:
        if str(getattr(args, "score_cache_stats_mode", "fast") or "fast") == "full":
            return cache.stats()
        return cache.fast_stats()
    finally:
        cache.close()


def _tail_text(path: Path, max_chars: int = 4000) -> str:
    try:
        if not path.exists():
            return ""
        size = path.stat().st_size
        with path.open("rb") as handle:
            handle.seek(max(0, size - max_chars * 4))
            data = handle.read()
        return data.decode("utf-8", errors="replace")[-max_chars:]
    except Exception:
        return ""


def _terminate_process_tree(proc: subprocess.Popen, *, grace_sec: float = 2.0) -> None:
    if proc.poll() is not None:
        return
    if os.name == "nt" and proc.pid:
        try:
            subprocess.run(
                ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
            proc.wait(timeout=max(0.1, float(grace_sec)))
            return
        except Exception:
            pass
    try:
        proc.terminate()
        proc.wait(timeout=max(0.1, float(grace_sec)))
        return
    except Exception:
        pass
    try:
        proc.kill()
    except Exception:
        pass


def _first_step2_envelope(summary_paths: list[Path]) -> dict[str, Any]:
    for path in summary_paths:
        payload = _read_json(path, {}) or {}
        envelope = payload.get("step2_evaluation_envelope")
        if isinstance(envelope, dict) and envelope:
            return envelope
    return {}


def _attach_evaluation_guardrails(payload: dict[str, Any], envelope: dict[str, Any]) -> None:
    if not envelope:
        return
    step2_evaluation_envelope.attach(payload, envelope)
    baseline_drift_sentinel.annotate_payload(payload)


def _row_key(row: dict[str, Any]) -> str:
    return tournament_safety.stable_json_hash({
        "weights": row.get("weights") or {},
        "bias": float(row.get("bias") or 0.0),
        "routes": row.get("routes") or [],
    }, length=32)


def _walk_rows(payload: Any):
    if isinstance(payload, dict):
        if isinstance(payload.get("weights"), dict) and ("step2_pnl" in payload or "decision_full" in payload):
            yield payload
        for value in payload.values():
            yield from _walk_rows(value)
    elif isinstance(payload, list):
        for item in payload:
            yield from _walk_rows(item)


def _collect_rows(paths: list[Path], args: argparse.Namespace, active_pnl: float | None = None) -> dict[str, Any]:
    ranked = hunt_intel.collect_ranked_from_paths(
        paths,
        active_pnl=active_pnl,
        live_only=bool(args.live_only),
        behavioral_dedupe=bool(args.behavioral_dedupe),
        limit=int(args.leaderboard_limit),
    )
    diagnostic = hunt_intel.collect_ranked_from_paths(
        paths,
        active_pnl=active_pnl,
        live_only=False,
        behavioral_dedupe=False,
        limit=max(int(args.leaderboard_limit), 500),
    )
    ranked["diagnostic_rows"] = diagnostic.get("decorated_rows") or []
    return ranked


def _hunter_command(args: argparse.Namespace, hunter: str, manifest_path: str, run_name: str) -> list[str]:
    script = HUNTER_SCRIPTS[hunter]
    cmd = [
        sys.executable,
        script,
        "--compiled-decision-tape", manifest_path,
        "--out-dir", str(Path(args.out_dir).resolve()),
        "--name", run_name,
        "--score-cache-db", str(Path(args.score_cache_db).resolve()),
        "--batch-size", str(int(args.batch_size)),
        "--start-balance", str(float(args.start_balance)),
        "--beat-pct", str(float(args.beat_pct)),
    ]
    if hunter == "adaptive":
        cmd.extend(["--max-batches", str(int(args.max_batches))])
        cmd.extend(["--target-count", str(int(args.target_count))])
    elif hunter == "local":
        cmd.extend(["--generations", str(int(args.max_batches))])
    elif hunter == "router":
        cmd.extend(["--max-batches", str(int(args.max_batches)), "--target-pnl", str(float(args.target_pnl))])
        cmd.extend(["--mutation-scale-multiplier", str(float(args.mutation_scale_multiplier))])
        if args.online_state_json:
            cmd.extend(["--online-state-json", str(Path(args.online_state_json).resolve())])
        if args.stream_telemetry_jsonl:
            cmd.extend(["--stream-telemetry-jsonl", str(Path(args.stream_telemetry_jsonl).resolve())])
        if args.stop_signal_json:
            cmd.extend(["--stop-signal-json", str(Path(args.stop_signal_json).resolve())])
        if args.quarantine_json:
            cmd.extend(["--quarantine-json", str(Path(args.quarantine_json).resolve())])
        if int(args.min_batch_size or 0) > 0:
            cmd.extend(["--min-batch-size", str(int(args.min_batch_size))])
        if int(args.max_batch_size or 0) > 0:
            cmd.extend(["--max-batch-size", str(int(args.max_batch_size))])
        if bool(args.exact_variant_count):
            cmd.append("--exact-variant-count")
        if str(args.runtime_control_mode or "enforce") != "enforce":
            cmd.extend(["--runtime-control-mode", str(args.runtime_control_mode)])
        if bool(getattr(args, "allow_uncertified_cache", False)):
            cmd.append("--allow-uncertified-cache")
        for route in args.focus_route or []:
            cmd.extend(["--focus-route", route])
        if args.focus_plan_json:
            cmd.extend(["--focus-plan-json", str(Path(args.focus_plan_json).resolve())])
    for seed in args.seed_json or []:
        if hunter in {"adaptive", "local"}:
            cmd.extend(["--seed-json", seed])
    return [str(part) for part in cmd]


def _payload_from_variant(variant: Any) -> dict[str, Any]:
    return step2_score_cache.variant_payload(variant)


def _queue_candidates(count: int, seed: int) -> list[dict[str, Any]]:
    active = active_engine_baseline.active_variant()
    base = _payload_from_variant(active)
    rng = random.Random(int(seed))
    keys = list((base.get("weights") or {}).keys())
    rows = [base]
    for idx in range(max(0, int(count) - 1)):
        weights = dict(base.get("weights") or {})
        touch = rng.sample(keys, k=min(len(keys), rng.choice([1, 2, 3, 5]))) if keys else []
        for key in touch:
            weights[key] = round(float(weights.get(key) or 0.0) + rng.uniform(-0.20, 0.20), 6)
        rows.append({
            "name": f"queue_mutation_{idx:05d}",
            "weights": weights,
            "bias": round(float(base.get("bias") or 0.0) + rng.uniform(-0.02, 0.02), 6),
            "base_weights": dict(base.get("weights") or {}),
            "base_bias": float(base.get("bias") or 0.0),
            "routes": [],
        })
    return rows


def _run_queue_mode(args: argparse.Namespace, out_dir: Path, resolved: dict[str, Any], manifest_path: str) -> dict[str, Any]:
    queue_db = str(Path(args.queue_db).resolve())
    queue = step2_candidate_work_queue.CandidateWorkQueue(queue_db)
    try:
        submit_rows = _queue_candidates(int(args.queue_candidates), int(args.seed))
        for seed_path in args.seed_json or []:
            try:
                submit_rows.extend(step2_candidate_work_queue.load_variants(seed_path))
            except Exception:
                pass
        submitted = queue.submit(submit_rows, source=args.name)
        queue_stats_before = queue.stats()
    finally:
        queue.close()
    if args.dry_run:
        score_payload = {"dry_run": True, "scored": 0, "leaderboard": [], "stats": queue_stats_before}
    else:
        score_payload = step2_candidate_work_queue.score_pending(
            queue_db=queue_db,
            manifest_path=manifest_path,
            batch_size=int(args.queue_batch_size),
            limit=int(args.queue_score_limit or args.queue_candidates),
            cache_db=str(Path(args.score_cache_db).resolve()),
            start_balance=float(args.start_balance),
            workers=int(args.queue_workers),
            summary=not bool(args.queue_full),
            full_finalists=int(args.queue_full_finalists),
            progressive_day_count=int(args.queue_progressive_day_count),
            promote_fraction=float(args.queue_promote_fraction),
        )
    raw_rows = list(score_payload.get("leaderboard") or [])
    active_pnl = None
    for row in raw_rows:
        if row.get("variant") == active_engine_baseline.active_variant().name:
            active_pnl = float(row.get("step2_pnl") or 0.0)
            break
    ranked = hunt_intel.rank_rows(
        raw_rows,
        active_pnl=active_pnl,
        live_only=bool(args.live_only),
        behavioral_dedupe=bool(args.behavioral_dedupe),
        limit=int(args.leaderboard_limit),
    )
    leaderboard = list(ranked.get("raw_leaderboard") or [])
    target = max(float(args.target_pnl), (active_pnl or 0.0) * (1.0 + float(args.beat_pct) / 100.0))
    winners = [row for row in leaderboard if float(row.get("step2_pnl") or 0.0) >= target]
    route_clusters = hunt_intel.route_clusters(leaderboard)
    learning = hunt_intel.learning_report(
        rows=leaderboard,
        cycles=[],
        all_rows=list(ranked.get("diagnostic_rows") or ranked.get("decorated_rows") or leaderboard),
        feedback_rows=list((hunt_intel.read_json(args.feedback_json, {}) or {}).get("feedback") or []) if args.feedback_json else [],
        source="step2_hunt_coordinator",
    )
    artifact_paths = {
        "finalists": _write_json(out_dir / "finalists.json", hunt_intel.finalists_payload(leaderboard, source="step2_hunt_coordinator")),
        "annotated_top100": _write_json(out_dir / "annotated_top100.json", learning.get("annotated_top100") or {}),
        "near_miss_archive": _write_json(out_dir / "near_miss_archive.json", learning.get("near_miss_archive") or {}),
        "adaptive_mutation_lanes": _write_json(out_dir / "adaptive_mutation_lanes.json", learning.get("adaptive_mutation_lanes") or {}),
        "worker_role_plan": _write_json(out_dir / "worker_role_plan.json", learning.get("worker_role_plan") or {}),
        "candidate_lineage_graph": _write_json(out_dir / "candidate_lineage_graph.json", learning.get("candidate_lineage_graph") or {}),
        "family_rejection_memory": _write_json(out_dir / "family_rejection_memory.json", learning.get("family_rejection_memory") or {}),
        "active_experiment_plan": _write_json(out_dir / "active_experiment_plan.json", learning.get("active_experiment_plan") or {}),
        "experiment_outcome_ledger": _write_json(out_dir / "experiment_outcome_ledger.json", learning.get("experiment_outcome_ledger") or {}),
        "causal_experiment_registry": _write_json(out_dir / "causal_experiment_registry.json", learning.get("causal_experiment_registry") or {}),
        "experiment_debt_queue": _write_json(out_dir / "experiment_debt_queue.json", learning.get("experiment_debt_queue") or {}),
        "information_gain_scoring": _write_json(out_dir / "information_gain_scoring.json", learning.get("information_gain_scoring") or {}),
        "value_of_information_planner": _write_json(out_dir / "value_of_information_planner.json", learning.get("value_of_information_planner") or {}),
        "decision_change_tracker": _write_json(out_dir / "decision_change_tracker.json", learning.get("decision_change_tracker") or {}),
        "hypothesis_quality_scoring": _write_json(out_dir / "hypothesis_quality_scoring.json", learning.get("hypothesis_quality_scoring") or {}),
        "evidence_sufficiency_gate": _write_json(out_dir / "evidence_sufficiency_gate.json", learning.get("evidence_sufficiency_gate") or {}),
        "counterfactual_shadow_board": _write_json(out_dir / "counterfactual_shadow_board.json", learning.get("counterfactual_shadow_board") or {}),
        "prediction_calibration_ledger": _write_json(out_dir / "prediction_calibration_ledger.json", learning.get("prediction_calibration_ledger") or {}),
        "belief_revision_engine": _write_json(out_dir / "belief_revision_engine.json", learning.get("belief_revision_engine") or {}),
        "adversarial_red_team_learner": _write_json(out_dir / "adversarial_red_team_learner.json", learning.get("adversarial_red_team_learner") or {}),
        "out_of_distribution_detector": _write_json(out_dir / "out_of_distribution_detector.json", learning.get("out_of_distribution_detector") or {}),
        "memory_compression_distiller": _write_json(out_dir / "memory_compression_distiller.json", learning.get("memory_compression_distiller") or {}),
        "self_audit_score": _write_json(out_dir / "self_audit_score.json", learning.get("self_audit_score") or {}),
        "truth_first_promotion_objective": _write_json(out_dir / "truth_first_promotion_objective.json", learning.get("truth_first_promotion_objective") or {}),
        "learning_velocity_dashboard": _write_json(out_dir / "learning_velocity_dashboard.json", learning.get("learning_velocity_dashboard") or {}),
        "compiled_hunt_policy": _write_json(out_dir / "compiled_hunt_policy.json", learning.get("compiled_hunt_policy") or {}),
        "policy_executor": _write_json(out_dir / "policy_executor.json", learning.get("policy_executor") or {}),
        "adaptive_worker_assignment": _write_json(out_dir / "adaptive_worker_assignment.json", learning.get("adaptive_worker_assignment") or {}),
        "policy_backtester": _write_json(out_dir / "policy_backtester.json", learning.get("policy_backtester") or {}),
        "policy_mutation_engine": _write_json(out_dir / "policy_mutation_engine.json", learning.get("policy_mutation_engine") or {}),
        "policy_tournament": _write_json(out_dir / "policy_tournament.json", learning.get("policy_tournament") or {}),
        "champion_challenger_memory": _write_json(out_dir / "champion_challenger_memory.json", learning.get("champion_challenger_memory") or {}),
        "regime_specific_policies": _write_json(out_dir / "regime_specific_policies.json", learning.get("regime_specific_policies") or {}),
        "causal_graph_of_learning": _write_json(out_dir / "causal_graph_of_learning.json", learning.get("causal_graph_of_learning") or {}),
        "policy_safety_rail": _write_json(out_dir / "policy_safety_rail.json", learning.get("policy_safety_rail") or {}),
        "auto_promoted_field_manual": _write_json(out_dir / "auto_promoted_field_manual.json", learning.get("auto_promoted_field_manual") or {}),
        "policy_drift_detector": _write_json(out_dir / "policy_drift_detector.json", learning.get("policy_drift_detector") or {}),
        "route_regime_half_life": _write_json(out_dir / "route_regime_half_life.json", learning.get("route_regime_half_life") or {}),
        "learning_market_map": _write_json(out_dir / "learning_market_map.json", learning.get("learning_market_map") or {}),
        "concept_drift_alarms": _write_json(out_dir / "concept_drift_alarms.json", learning.get("concept_drift_alarms") or {}),
        "revalidation_scheduler": _write_json(out_dir / "revalidation_scheduler.json", learning.get("revalidation_scheduler") or {}),
        "temporal_ensemble_policy": _write_json(out_dir / "temporal_ensemble_policy.json", learning.get("temporal_ensemble_policy") or {}),
        "active_experiment_governor": _write_json(out_dir / "active_experiment_governor.json", learning.get("active_experiment_governor") or {}),
        "route_state_machine": _write_json(out_dir / "route_state_machine.json", learning.get("route_state_machine") or {}),
        "negative_knowledge_bank": _write_json(out_dir / "negative_knowledge_bank.json", learning.get("negative_knowledge_bank") or {}),
        "promotion_survivor_model": _write_json(out_dir / "promotion_survivor_model.json", learning.get("promotion_survivor_model") or {}),
        "mutation_grammar_learner": _write_json(out_dir / "mutation_grammar_learner.json", learning.get("mutation_grammar_learner") or {}),
        "real_time_worker_rebalancer": _write_json(out_dir / "real_time_worker_rebalancer.json", learning.get("real_time_worker_rebalancer") or {}),
        "hunt_replay_simulator": _write_json(out_dir / "hunt_replay_simulator.json", learning.get("hunt_replay_simulator") or {}),
        "resurrection_engine": _write_json(out_dir / "resurrection_engine.json", learning.get("resurrection_engine") or {}),
        "causal_mutation_attribution": _write_json(out_dir / "causal_mutation_attribution.json", learning.get("causal_mutation_attribution") or {}),
        "uncertainty_budgeting": _write_json(out_dir / "uncertainty_budgeting.json", learning.get("uncertainty_budgeting") or {}),
        "promotability_pareto_frontier": _write_json(out_dir / "promotability_pareto_frontier.json", learning.get("promotability_pareto_frontier") or {}),
        "false_lesson_detector": _write_json(out_dir / "false_lesson_detector.json", learning.get("false_lesson_detector") or {}),
        "experiment_graduation_system": _write_json(out_dir / "experiment_graduation_system.json", learning.get("experiment_graduation_system") or {}),
        "candidate_genealogy_diff_engine": _write_json(out_dir / "candidate_genealogy_diff_engine.json", learning.get("candidate_genealogy_diff_engine") or {}),
        "off_policy_hunt_evaluator": _write_json(out_dir / "off_policy_hunt_evaluator.json", learning.get("off_policy_hunt_evaluator") or {}),
        "self_competition_league": _write_json(out_dir / "self_competition_league.json", learning.get("self_competition_league") or {}),
        "evidence_contract_engine": _write_json(out_dir / "evidence_contract_engine.json", learning.get("evidence_contract_engine") or {}),
        "live_beater_quality_decomposer": _write_json(out_dir / "live_beater_quality_decomposer.json", learning.get("live_beater_quality_decomposer") or {}),
        "contradiction_detector": _write_json(out_dir / "contradiction_detector.json", learning.get("contradiction_detector") or {}),
        "learning_conflict_resolver": _write_json(out_dir / "learning_conflict_resolver.json", learning.get("learning_conflict_resolver") or {}),
        "cohort_based_memory": _write_json(out_dir / "cohort_based_memory.json", learning.get("cohort_based_memory") or {}),
        "adaptive_hunt_throttle": _write_json(out_dir / "adaptive_hunt_throttle.json", learning.get("adaptive_hunt_throttle") or {}),
        "promotion_readiness_simulator": _write_json(out_dir / "promotion_readiness_simulator.json", learning.get("promotion_readiness_simulator") or {}),
        "research_trace_ledger": _write_json(out_dir / "research_trace_ledger.json", learning.get("research_trace_ledger") or {}),
        "runtime_decision_kernel": _write_json(out_dir / "runtime_decision_kernel.json", learning.get("runtime_decision_kernel") or {}),
        "action_outcome_tracker": _write_json(out_dir / "action_outcome_tracker.json", learning.get("action_outcome_tracker") or {}),
        "closed_loop_reward_model": _write_json(out_dir / "closed_loop_reward_model.json", learning.get("closed_loop_reward_model") or {}),
        "autonomous_hunt_planner": _write_json(out_dir / "autonomous_hunt_planner.json", learning.get("autonomous_hunt_planner") or {}),
        "runtime_guardrails": _write_json(out_dir / "runtime_guardrails.json", learning.get("runtime_guardrails") or {}),
        "command_replay_ledger": _write_json(out_dir / "command_replay_ledger.json", learning.get("command_replay_ledger") or {}),
        "action_elo_league": _write_json(out_dir / "action_elo_league.json", learning.get("action_elo_league") or {}),
        "human_readable_hunt_brief": _write_json(out_dir / "human_readable_hunt_brief.json", learning.get("human_readable_hunt_brief") or {}),
        "meta_hunt_strategy_learner": _write_json(out_dir / "meta_hunt_strategy_learner.json", learning.get("meta_hunt_strategy_learner") or {}),
        "run_to_run_postmortem": _write_json(out_dir / "run_to_run_postmortem.json", learning.get("run_to_run_postmortem") or {}),
        "treatment_effects": _write_json(out_dir / "treatment_effects.json", learning.get("treatment_effects") or {}),
        "treatment_prior_model": _write_json(out_dir / "treatment_prior_model.json", learning.get("treatment_prior_model") or {}),
        "treatment_worker_budget": _write_json(out_dir / "treatment_worker_budget.json", learning.get("treatment_worker_budget") or {}),
        "treatment_confidence_model": _write_json(out_dir / "treatment_confidence_model.json", learning.get("treatment_confidence_model") or {}),
        "controlled_parent_sibling_experiments": _write_json(out_dir / "controlled_parent_sibling_experiments.json", learning.get("controlled_parent_sibling_experiments") or {}),
        "live_beater_failure_autopsy": _write_json(out_dir / "live_beater_failure_autopsy.json", learning.get("live_beater_failure_autopsy") or {}),
        "worker_specialization_memory": _write_json(out_dir / "worker_specialization_memory.json", learning.get("worker_specialization_memory") or {}),
        "regime_aware_learning": _write_json(out_dir / "regime_aware_learning.json", learning.get("regime_aware_learning") or {}),
        "promotion_reject_simulator": _write_json(out_dir / "promotion_reject_simulator.json", learning.get("promotion_reject_simulator") or {}),
        "search_portfolio_manager": _write_json(out_dir / "search_portfolio_manager.json", learning.get("search_portfolio_manager") or {}),
        "diversity_finalists": _write_json(out_dir / "diversity_finalists.json", learning.get("diversity_constrained_finalists") or {}),
        "oos_replay_queue": _write_json(out_dir / "oos_replay_queue.json", hunt_intel.oos_replay_queue(ranked.get("promotion_quality_leaderboard") or leaderboard, source="step2_hunt_coordinator")),
        "behavior_cache": _write_json(out_dir / "behavior_cache.json", hunt_intel.behavior_cache_payload(leaderboard, source="step2_hunt_coordinator")),
        "next_hunt_plan": _write_json(out_dir / "next_hunt_plan.json", hunt_intel.next_hunt_plan(leaderboard)),
        "learning_report": _write_json(out_dir / "learning_report.json", learning),
        "bandit_allocation": _write_json(out_dir / "bandit_allocation.json", learning.get("bandit_allocation") or {}),
        "validation_aware_bandit": _write_json(out_dir / "validation_aware_bandit.json", learning.get("validation_aware_bandit") or {}),
        "auto_validation_scheduler": _write_json(out_dir / "auto_validation_scheduler.json", learning.get("auto_validation_scheduler") or {}),
        "candidate_family_clusters": _write_json(out_dir / "candidate_family_clusters.json", learning.get("candidate_family_clusters") or {}),
        "counterfactual_route_attribution_plan": _write_json(out_dir / "counterfactual_route_attribution_plan.json", learning.get("counterfactual_route_attribution_plan") or {}),
        "temporal_generalization_map": _write_json(out_dir / "temporal_generalization_map.json", learning.get("temporal_generalization_map") or {}),
        "feature_interactions": _write_json(out_dir / "feature_interactions.json", learning.get("feature_interactions") or {}),
        "adversarial_perturbation_plan": _write_json(out_dir / "adversarial_perturbation_plan.json", learning.get("adversarial_perturbation_plan") or {}),
    }
    payload = {
        "schema_version": 1,
        "source": "step2_hunt_coordinator_queue_mode",
        "created_at_ct": _now_ct(),
        "ok": True,
        "dry_run": bool(args.dry_run),
        "queue_mode": True,
        "cache_resolution": step2_manifest_resolver.compact_resolution(resolved),
        "compiled_decision_tape": manifest_path,
        "queue": {
            "db": queue_db,
            "submitted": submitted,
            "score_payload": score_payload,
        },
        "active_step2_pnl": active_pnl,
        "target_pnl": target,
        "beat_pct": float(args.beat_pct),
        "live_only": bool(args.live_only),
        "behavioral_dedupe": bool(args.behavioral_dedupe),
        "ranking_stats": {k: ranked.get(k) for k in (
            "input_rows",
            "live_filtered_rows",
            "config_unique_rows",
            "behavior_unique_rows",
        )},
        "leaderboard": leaderboard,
        "promotion_quality_leaderboard": ranked.get("promotion_quality_leaderboard") or [],
        "promotion_readiness_leaderboard": ranked.get("promotion_readiness_leaderboard") or [],
        "winners": winners,
        "route_clusters": route_clusters,
        "next_hunt_plan": hunt_intel.next_hunt_plan(leaderboard),
        "learning_report": learning,
        "artifact_paths": artifact_paths,
    }
    payload["path"] = _write_json(out_dir / "coordinator_summary.json", payload)
    if args.learning_db:
        db = step2_learning_db.Step2LearningDB(args.learning_db)
        try:
            payload["learning_db"] = db.ingest_run_dir(out_dir)
            if args.feedback_json:
                payload["learning_db"]["feedback_ingest"] = db.ingest_feedback_file(args.feedback_json)
            payload["learning_db"]["validation_ingest"] = [
                db.ingest_validation_results(path) for path in (args.validation_results_json or [])
            ]
            payload["learning_db"]["cross_run_route_memory"] = db.cross_run_route_memory(limit=20)
            payload["learning_db"]["route_prior_model"] = db.route_prior_model(limit=20)
            payload["learning_db"]["adaptive_mutation_controller"] = db.adaptive_mutation_controller(limit=20)
            payload["learning_db"]["drift_detection"] = db.drift_detection(limit=20)
            payload["learning_db"]["regime_aware_bandit"] = db.regime_aware_bandit(limit=50)
            payload["learning_db"]["temporal_decay_memory"] = db.temporal_decay_memory(limit=50)
            payload["learning_db"]["revalidation_candidates"] = db.revalidation_candidates(limit=50)
            payload["learning_db"]["research_lab_memory"] = db.research_lab_memory(limit=50)
            payload["learning_db"]["self_correction_memory"] = db.self_correction_memory(limit=50)
            payload["learning_db"]["pressure_science_memory"] = db.pressure_science_memory(limit=50)
            payload["learning_db"]["closed_loop_execution_memory"] = db.closed_loop_execution_memory(limit=50)
            packet = db.next_experiment_packet(limit=20)
            payload["learning_db"]["next_experiment_packet"] = packet
            _write_json(out_dir / "next_experiment_packet.json", packet)
        finally:
            db.close()
        payload["path"] = _write_json(out_dir / "coordinator_summary.json", payload)
    return payload


def run(args: argparse.Namespace) -> dict[str, Any]:
    out_dir = Path(args.out_dir) / args.name
    out_dir.mkdir(parents=True, exist_ok=True)
    resolved = step2_manifest_resolver.resolve_best_manifest(
        start=args.cache_start,
        end=args.cache_end,
        tickers=args.cache_tickers,
        require_certified=not bool(getattr(args, "allow_uncertified_cache", False)),
        write_receipt=True,
        label=args.name,
    )
    if not resolved.get("ok"):
        payload = {
            "schema_version": 1,
            "source": "step2_hunt_coordinator",
            "created_at_ct": _now_ct(),
            "ok": False,
            "reason": "cache_resolution_failed",
            "cache_resolution": step2_manifest_resolver.compact_resolution(resolved),
        }
        payload["path"] = _write_json(out_dir / "coordinator_summary.json", payload)
        return payload

    manifest_path = str(resolved["manifest_path"])
    if getattr(args, "queue_mode", False):
        return _run_queue_mode(args, out_dir, resolved, manifest_path)
    runs = []
    summary_paths: list[Path] = []
    rank_paths: list[Path] = []
    started = time.perf_counter()
    for hunter in args.hunters:
        if hunter not in HUNTER_SCRIPTS:
            runs.append({"hunter": hunter, "ok": False, "error": "unknown_hunter"})
            continue
        run_name = f"{args.name}_{hunter}"
        cmd = _hunter_command(args, hunter, manifest_path, run_name)
        row = {
            "hunter": hunter,
            "run_name": run_name,
            "cmd": cmd,
            "started_at_ct": _now_ct(),
        }
        if args.dry_run:
            row.update({"ok": True, "dry_run": True})
            runs.append(row)
            continue
        t0 = time.perf_counter()
        log_dir = out_dir / "hunter_logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        stdout_path = log_dir / f"{hunter}.stdout.log"
        stderr_path = log_dir / f"{hunter}.stderr.log"
        try:
            with stdout_path.open("a", encoding="utf-8") as stdout_handle, stderr_path.open("a", encoding="utf-8") as stderr_handle:
                proc = subprocess.Popen(
                    cmd,
                    cwd=HERE,
                    text=True,
                    stdout=stdout_handle,
                    stderr=stderr_handle,
                )
                try:
                    proc.wait(timeout=(float(args.hunter_timeout_sec) if float(args.hunter_timeout_sec or 0) > 0 else None))
                except subprocess.TimeoutExpired:
                    _terminate_process_tree(proc)
                    raise
            row.update({
                "ok": proc.returncode == 0,
                "returncode": proc.returncode,
                "elapsed_sec": round(time.perf_counter() - t0, 3),
                "stdout_path": str(stdout_path.resolve()),
                "stderr_path": str(stderr_path.resolve()),
                "stdout_tail": _tail_text(stdout_path, 4000),
                "stderr_tail": _tail_text(stderr_path, 4000),
            })
        except subprocess.TimeoutExpired:
            row.update({
                "ok": False,
                "timeout": True,
                "returncode": None,
                "elapsed_sec": round(time.perf_counter() - t0, 3),
                "stdout_path": str(stdout_path.resolve()),
                "stderr_path": str(stderr_path.resolve()),
                "stdout_tail": _tail_text(stdout_path, 4000),
                "stderr_tail": _tail_text(stderr_path, 4000),
            })
        summary = Path(args.out_dir) / run_name / "summary.json"
        if summary.exists():
            row["summary_path"] = str(summary.resolve())
            summary_paths.append(summary)
            scored_path = summary.parent / "scored_variants.jsonl"
            rank_paths.append(scored_path if scored_path.exists() else summary)
        runs.append(row)

    active_pnl = None
    for path in summary_paths:
        payload = _read_json(path, {}) or {}
        if payload.get("active_step2_pnl") is not None:
            active_pnl = float(payload.get("active_step2_pnl"))
            break
    ranked = _collect_rows(rank_paths or summary_paths, args, active_pnl=active_pnl)
    evaluation_envelope = _first_step2_envelope(summary_paths)
    leaderboard = list(ranked.get("raw_leaderboard") or [])
    target = max(float(args.target_pnl), (active_pnl or 0.0) * (1.0 + float(args.beat_pct) / 100.0))
    winners = [row for row in leaderboard if float(row.get("step2_pnl") or 0.0) >= target]
    milestone_floor = (active_pnl or 0.0) * (1.0 + float(args.beat_pct) / 100.0)
    milestones = []
    for idx, row in enumerate(leaderboard):
        pnl = float(row.get("step2_pnl") or 0.0)
        if milestone_floor and pnl >= milestone_floor:
            milestones.append({
                "rank": idx + 1,
                "variant": row.get("variant"),
                "step2_pnl": pnl,
                "floor_pnl": round(milestone_floor, 2),
            })
            milestone_floor = pnl * (1.0 + float(args.beat_pct) / 100.0)
    route_clusters = hunt_intel.route_clusters(leaderboard)
    if _minimal_artifact_mode(args):
        cache_stats = _score_cache_stats(args)
        artifact_paths = {
            "hunter_summaries": [str(path.resolve()) for path in summary_paths],
            "ranking_sources": [str(path.resolve()) for path in (rank_paths or summary_paths)],
        }
        payload = {
            "schema_version": 1,
            "source": "step2_hunt_coordinator",
            "created_at_ct": _now_ct(),
            "ok": all(row.get("ok") for row in runs) if runs else True,
            "dry_run": bool(args.dry_run),
            "artifact_mode": "minimal",
            "cache_resolution": step2_manifest_resolver.compact_resolution(resolved),
            "compiled_decision_tape": manifest_path,
            "score_cache": cache_stats,
            "hunters": args.hunters,
            "runs": runs,
            "active_step2_pnl": active_pnl,
            "target_pnl": target,
            "beat_pct": float(args.beat_pct),
            "live_only": bool(args.live_only),
            "behavioral_dedupe": bool(args.behavioral_dedupe),
            "ranking_stats": {k: ranked.get(k) for k in (
                "input_rows",
                "live_filtered_rows",
                "config_unique_rows",
                "behavior_unique_rows",
            )},
            "leaderboard": leaderboard[:int(args.leaderboard_limit)],
            "promotion_quality_leaderboard": (ranked.get("promotion_quality_leaderboard") or [])[:int(args.leaderboard_limit)],
            "promotion_readiness_leaderboard": (ranked.get("promotion_readiness_leaderboard") or [])[:int(args.leaderboard_limit)],
            "winners": winners[:100],
            "milestones": milestones,
            "route_clusters": route_clusters,
            "next_hunt_plan": hunt_intel.next_hunt_plan(leaderboard),
            "artifact_paths": artifact_paths,
            "elapsed_sec": round(time.perf_counter() - started, 3),
        }
        _attach_evaluation_guardrails(payload, evaluation_envelope)
        summary_path = out_dir / "coordinator_summary.json"
        payload["path"] = str(summary_path.resolve())
        _write_json(summary_path, payload)
        return payload
    learning = hunt_intel.learning_report(
        rows=leaderboard,
        cycles=runs,
        all_rows=list(ranked.get("diagnostic_rows") or ranked.get("decorated_rows") or leaderboard),
        feedback_rows=list((hunt_intel.read_json(args.feedback_json, {}) or {}).get("feedback") or []) if args.feedback_json else [],
        source="step2_hunt_coordinator",
    )
    artifact_paths = {
        "finalists": _write_json(out_dir / "finalists.json", hunt_intel.finalists_payload(leaderboard, source="step2_hunt_coordinator")),
        "annotated_top100": _write_json(out_dir / "annotated_top100.json", learning.get("annotated_top100") or {}),
        "near_miss_archive": _write_json(out_dir / "near_miss_archive.json", learning.get("near_miss_archive") or {}),
        "adaptive_mutation_lanes": _write_json(out_dir / "adaptive_mutation_lanes.json", learning.get("adaptive_mutation_lanes") or {}),
        "worker_role_plan": _write_json(out_dir / "worker_role_plan.json", learning.get("worker_role_plan") or {}),
        "candidate_lineage_graph": _write_json(out_dir / "candidate_lineage_graph.json", learning.get("candidate_lineage_graph") or {}),
        "family_rejection_memory": _write_json(out_dir / "family_rejection_memory.json", learning.get("family_rejection_memory") or {}),
        "active_experiment_plan": _write_json(out_dir / "active_experiment_plan.json", learning.get("active_experiment_plan") or {}),
        "experiment_outcome_ledger": _write_json(out_dir / "experiment_outcome_ledger.json", learning.get("experiment_outcome_ledger") or {}),
        "causal_experiment_registry": _write_json(out_dir / "causal_experiment_registry.json", learning.get("causal_experiment_registry") or {}),
        "experiment_debt_queue": _write_json(out_dir / "experiment_debt_queue.json", learning.get("experiment_debt_queue") or {}),
        "information_gain_scoring": _write_json(out_dir / "information_gain_scoring.json", learning.get("information_gain_scoring") or {}),
        "value_of_information_planner": _write_json(out_dir / "value_of_information_planner.json", learning.get("value_of_information_planner") or {}),
        "decision_change_tracker": _write_json(out_dir / "decision_change_tracker.json", learning.get("decision_change_tracker") or {}),
        "hypothesis_quality_scoring": _write_json(out_dir / "hypothesis_quality_scoring.json", learning.get("hypothesis_quality_scoring") or {}),
        "evidence_sufficiency_gate": _write_json(out_dir / "evidence_sufficiency_gate.json", learning.get("evidence_sufficiency_gate") or {}),
        "counterfactual_shadow_board": _write_json(out_dir / "counterfactual_shadow_board.json", learning.get("counterfactual_shadow_board") or {}),
        "prediction_calibration_ledger": _write_json(out_dir / "prediction_calibration_ledger.json", learning.get("prediction_calibration_ledger") or {}),
        "belief_revision_engine": _write_json(out_dir / "belief_revision_engine.json", learning.get("belief_revision_engine") or {}),
        "adversarial_red_team_learner": _write_json(out_dir / "adversarial_red_team_learner.json", learning.get("adversarial_red_team_learner") or {}),
        "out_of_distribution_detector": _write_json(out_dir / "out_of_distribution_detector.json", learning.get("out_of_distribution_detector") or {}),
        "memory_compression_distiller": _write_json(out_dir / "memory_compression_distiller.json", learning.get("memory_compression_distiller") or {}),
        "self_audit_score": _write_json(out_dir / "self_audit_score.json", learning.get("self_audit_score") or {}),
        "truth_first_promotion_objective": _write_json(out_dir / "truth_first_promotion_objective.json", learning.get("truth_first_promotion_objective") or {}),
        "learning_velocity_dashboard": _write_json(out_dir / "learning_velocity_dashboard.json", learning.get("learning_velocity_dashboard") or {}),
        "compiled_hunt_policy": _write_json(out_dir / "compiled_hunt_policy.json", learning.get("compiled_hunt_policy") or {}),
        "policy_executor": _write_json(out_dir / "policy_executor.json", learning.get("policy_executor") or {}),
        "adaptive_worker_assignment": _write_json(out_dir / "adaptive_worker_assignment.json", learning.get("adaptive_worker_assignment") or {}),
        "policy_backtester": _write_json(out_dir / "policy_backtester.json", learning.get("policy_backtester") or {}),
        "policy_mutation_engine": _write_json(out_dir / "policy_mutation_engine.json", learning.get("policy_mutation_engine") or {}),
        "policy_tournament": _write_json(out_dir / "policy_tournament.json", learning.get("policy_tournament") or {}),
        "champion_challenger_memory": _write_json(out_dir / "champion_challenger_memory.json", learning.get("champion_challenger_memory") or {}),
        "regime_specific_policies": _write_json(out_dir / "regime_specific_policies.json", learning.get("regime_specific_policies") or {}),
        "causal_graph_of_learning": _write_json(out_dir / "causal_graph_of_learning.json", learning.get("causal_graph_of_learning") or {}),
        "policy_safety_rail": _write_json(out_dir / "policy_safety_rail.json", learning.get("policy_safety_rail") or {}),
        "auto_promoted_field_manual": _write_json(out_dir / "auto_promoted_field_manual.json", learning.get("auto_promoted_field_manual") or {}),
        "policy_drift_detector": _write_json(out_dir / "policy_drift_detector.json", learning.get("policy_drift_detector") or {}),
        "route_regime_half_life": _write_json(out_dir / "route_regime_half_life.json", learning.get("route_regime_half_life") or {}),
        "learning_market_map": _write_json(out_dir / "learning_market_map.json", learning.get("learning_market_map") or {}),
        "concept_drift_alarms": _write_json(out_dir / "concept_drift_alarms.json", learning.get("concept_drift_alarms") or {}),
        "revalidation_scheduler": _write_json(out_dir / "revalidation_scheduler.json", learning.get("revalidation_scheduler") or {}),
        "temporal_ensemble_policy": _write_json(out_dir / "temporal_ensemble_policy.json", learning.get("temporal_ensemble_policy") or {}),
        "active_experiment_governor": _write_json(out_dir / "active_experiment_governor.json", learning.get("active_experiment_governor") or {}),
        "route_state_machine": _write_json(out_dir / "route_state_machine.json", learning.get("route_state_machine") or {}),
        "negative_knowledge_bank": _write_json(out_dir / "negative_knowledge_bank.json", learning.get("negative_knowledge_bank") or {}),
        "promotion_survivor_model": _write_json(out_dir / "promotion_survivor_model.json", learning.get("promotion_survivor_model") or {}),
        "mutation_grammar_learner": _write_json(out_dir / "mutation_grammar_learner.json", learning.get("mutation_grammar_learner") or {}),
        "real_time_worker_rebalancer": _write_json(out_dir / "real_time_worker_rebalancer.json", learning.get("real_time_worker_rebalancer") or {}),
        "hunt_replay_simulator": _write_json(out_dir / "hunt_replay_simulator.json", learning.get("hunt_replay_simulator") or {}),
        "resurrection_engine": _write_json(out_dir / "resurrection_engine.json", learning.get("resurrection_engine") or {}),
        "causal_mutation_attribution": _write_json(out_dir / "causal_mutation_attribution.json", learning.get("causal_mutation_attribution") or {}),
        "uncertainty_budgeting": _write_json(out_dir / "uncertainty_budgeting.json", learning.get("uncertainty_budgeting") or {}),
        "promotability_pareto_frontier": _write_json(out_dir / "promotability_pareto_frontier.json", learning.get("promotability_pareto_frontier") or {}),
        "false_lesson_detector": _write_json(out_dir / "false_lesson_detector.json", learning.get("false_lesson_detector") or {}),
        "experiment_graduation_system": _write_json(out_dir / "experiment_graduation_system.json", learning.get("experiment_graduation_system") or {}),
        "candidate_genealogy_diff_engine": _write_json(out_dir / "candidate_genealogy_diff_engine.json", learning.get("candidate_genealogy_diff_engine") or {}),
        "off_policy_hunt_evaluator": _write_json(out_dir / "off_policy_hunt_evaluator.json", learning.get("off_policy_hunt_evaluator") or {}),
        "self_competition_league": _write_json(out_dir / "self_competition_league.json", learning.get("self_competition_league") or {}),
        "evidence_contract_engine": _write_json(out_dir / "evidence_contract_engine.json", learning.get("evidence_contract_engine") or {}),
        "live_beater_quality_decomposer": _write_json(out_dir / "live_beater_quality_decomposer.json", learning.get("live_beater_quality_decomposer") or {}),
        "contradiction_detector": _write_json(out_dir / "contradiction_detector.json", learning.get("contradiction_detector") or {}),
        "learning_conflict_resolver": _write_json(out_dir / "learning_conflict_resolver.json", learning.get("learning_conflict_resolver") or {}),
        "cohort_based_memory": _write_json(out_dir / "cohort_based_memory.json", learning.get("cohort_based_memory") or {}),
        "adaptive_hunt_throttle": _write_json(out_dir / "adaptive_hunt_throttle.json", learning.get("adaptive_hunt_throttle") or {}),
        "promotion_readiness_simulator": _write_json(out_dir / "promotion_readiness_simulator.json", learning.get("promotion_readiness_simulator") or {}),
        "research_trace_ledger": _write_json(out_dir / "research_trace_ledger.json", learning.get("research_trace_ledger") or {}),
        "runtime_decision_kernel": _write_json(out_dir / "runtime_decision_kernel.json", learning.get("runtime_decision_kernel") or {}),
        "action_outcome_tracker": _write_json(out_dir / "action_outcome_tracker.json", learning.get("action_outcome_tracker") or {}),
        "closed_loop_reward_model": _write_json(out_dir / "closed_loop_reward_model.json", learning.get("closed_loop_reward_model") or {}),
        "autonomous_hunt_planner": _write_json(out_dir / "autonomous_hunt_planner.json", learning.get("autonomous_hunt_planner") or {}),
        "runtime_guardrails": _write_json(out_dir / "runtime_guardrails.json", learning.get("runtime_guardrails") or {}),
        "command_replay_ledger": _write_json(out_dir / "command_replay_ledger.json", learning.get("command_replay_ledger") or {}),
        "action_elo_league": _write_json(out_dir / "action_elo_league.json", learning.get("action_elo_league") or {}),
        "human_readable_hunt_brief": _write_json(out_dir / "human_readable_hunt_brief.json", learning.get("human_readable_hunt_brief") or {}),
        "meta_hunt_strategy_learner": _write_json(out_dir / "meta_hunt_strategy_learner.json", learning.get("meta_hunt_strategy_learner") or {}),
        "run_to_run_postmortem": _write_json(out_dir / "run_to_run_postmortem.json", learning.get("run_to_run_postmortem") or {}),
        "treatment_effects": _write_json(out_dir / "treatment_effects.json", learning.get("treatment_effects") or {}),
        "treatment_prior_model": _write_json(out_dir / "treatment_prior_model.json", learning.get("treatment_prior_model") or {}),
        "treatment_worker_budget": _write_json(out_dir / "treatment_worker_budget.json", learning.get("treatment_worker_budget") or {}),
        "treatment_confidence_model": _write_json(out_dir / "treatment_confidence_model.json", learning.get("treatment_confidence_model") or {}),
        "controlled_parent_sibling_experiments": _write_json(out_dir / "controlled_parent_sibling_experiments.json", learning.get("controlled_parent_sibling_experiments") or {}),
        "live_beater_failure_autopsy": _write_json(out_dir / "live_beater_failure_autopsy.json", learning.get("live_beater_failure_autopsy") or {}),
        "worker_specialization_memory": _write_json(out_dir / "worker_specialization_memory.json", learning.get("worker_specialization_memory") or {}),
        "regime_aware_learning": _write_json(out_dir / "regime_aware_learning.json", learning.get("regime_aware_learning") or {}),
        "promotion_reject_simulator": _write_json(out_dir / "promotion_reject_simulator.json", learning.get("promotion_reject_simulator") or {}),
        "search_portfolio_manager": _write_json(out_dir / "search_portfolio_manager.json", learning.get("search_portfolio_manager") or {}),
        "diversity_finalists": _write_json(out_dir / "diversity_finalists.json", learning.get("diversity_constrained_finalists") or {}),
        "oos_replay_queue": _write_json(out_dir / "oos_replay_queue.json", hunt_intel.oos_replay_queue(ranked.get("promotion_quality_leaderboard") or leaderboard, source="step2_hunt_coordinator")),
        "behavior_cache": _write_json(out_dir / "behavior_cache.json", hunt_intel.behavior_cache_payload(leaderboard, source="step2_hunt_coordinator")),
        "next_hunt_plan": _write_json(out_dir / "next_hunt_plan.json", hunt_intel.next_hunt_plan(leaderboard)),
        "learning_report": _write_json(out_dir / "learning_report.json", learning),
        "bandit_allocation": _write_json(out_dir / "bandit_allocation.json", learning.get("bandit_allocation") or {}),
        "validation_aware_bandit": _write_json(out_dir / "validation_aware_bandit.json", learning.get("validation_aware_bandit") or {}),
        "auto_validation_scheduler": _write_json(out_dir / "auto_validation_scheduler.json", learning.get("auto_validation_scheduler") or {}),
        "candidate_family_clusters": _write_json(out_dir / "candidate_family_clusters.json", learning.get("candidate_family_clusters") or {}),
        "counterfactual_route_attribution_plan": _write_json(out_dir / "counterfactual_route_attribution_plan.json", learning.get("counterfactual_route_attribution_plan") or {}),
        "temporal_generalization_map": _write_json(out_dir / "temporal_generalization_map.json", learning.get("temporal_generalization_map") or {}),
        "feature_interactions": _write_json(out_dir / "feature_interactions.json", learning.get("feature_interactions") or {}),
        "adversarial_perturbation_plan": _write_json(out_dir / "adversarial_perturbation_plan.json", learning.get("adversarial_perturbation_plan") or {}),
    }

    cache_stats = _score_cache_stats(args)
    payload = {
        "schema_version": 1,
        "source": "step2_hunt_coordinator",
        "created_at_ct": _now_ct(),
        "ok": all(row.get("ok") for row in runs) if runs else True,
        "dry_run": bool(args.dry_run),
        "cache_resolution": step2_manifest_resolver.compact_resolution(resolved),
        "compiled_decision_tape": manifest_path,
        "score_cache": cache_stats,
        "hunters": args.hunters,
        "runs": runs,
        "active_step2_pnl": active_pnl,
        "target_pnl": target,
        "beat_pct": float(args.beat_pct),
        "live_only": bool(args.live_only),
        "behavioral_dedupe": bool(args.behavioral_dedupe),
        "ranking_stats": {k: ranked.get(k) for k in (
            "input_rows",
            "live_filtered_rows",
            "config_unique_rows",
            "behavior_unique_rows",
        )},
        "leaderboard": leaderboard[:int(args.leaderboard_limit)],
        "promotion_quality_leaderboard": (ranked.get("promotion_quality_leaderboard") or [])[:int(args.leaderboard_limit)],
        "promotion_readiness_leaderboard": (ranked.get("promotion_readiness_leaderboard") or [])[:int(args.leaderboard_limit)],
        "winners": winners[:100],
        "milestones": milestones,
        "route_clusters": route_clusters,
        "next_hunt_plan": hunt_intel.next_hunt_plan(leaderboard),
        "learning_report": learning,
        "artifact_paths": artifact_paths,
        "elapsed_sec": round(time.perf_counter() - started, 3),
    }
    _attach_evaluation_guardrails(payload, evaluation_envelope)
    payload["path"] = _write_json(out_dir / "coordinator_summary.json", payload)
    if args.learning_db:
        db = step2_learning_db.Step2LearningDB(args.learning_db)
        try:
            payload["learning_db"] = db.ingest_run_dir(out_dir)
            if args.feedback_json:
                payload["learning_db"]["feedback_ingest"] = db.ingest_feedback_file(args.feedback_json)
            payload["learning_db"]["validation_ingest"] = [
                db.ingest_validation_results(path) for path in (args.validation_results_json or [])
            ]
            payload["learning_db"]["cross_run_route_memory"] = db.cross_run_route_memory(limit=20)
            payload["learning_db"]["route_prior_model"] = db.route_prior_model(limit=20)
            payload["learning_db"]["adaptive_mutation_controller"] = db.adaptive_mutation_controller(limit=20)
            payload["learning_db"]["drift_detection"] = db.drift_detection(limit=20)
            payload["learning_db"]["regime_aware_bandit"] = db.regime_aware_bandit(limit=50)
            payload["learning_db"]["temporal_decay_memory"] = db.temporal_decay_memory(limit=50)
            payload["learning_db"]["revalidation_candidates"] = db.revalidation_candidates(limit=50)
            payload["learning_db"]["research_lab_memory"] = db.research_lab_memory(limit=50)
            payload["learning_db"]["self_correction_memory"] = db.self_correction_memory(limit=50)
            payload["learning_db"]["pressure_science_memory"] = db.pressure_science_memory(limit=50)
            payload["learning_db"]["closed_loop_execution_memory"] = db.closed_loop_execution_memory(limit=50)
            packet = db.next_experiment_packet(limit=20)
            payload["learning_db"]["next_experiment_packet"] = packet
            _write_json(out_dir / "next_experiment_packet.json", packet)
        finally:
            db.close()
        _attach_evaluation_guardrails(payload, evaluation_envelope)
        payload["path"] = _write_json(out_dir / "coordinator_summary.json", payload)
    return payload


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Coordinate Step 2 hunters with a shared certified cache and score cache.")
    ap.add_argument("--name", default="coordinated_step2_hunt")
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT))
    ap.add_argument("--hunters", nargs="*", choices=sorted(HUNTER_SCRIPTS), default=["adaptive", "local", "router"])
    ap.add_argument("--cache-start", default=step2_manifest_resolver.DEFAULT_START)
    ap.add_argument("--cache-end", default=step2_manifest_resolver.DEFAULT_END)
    ap.add_argument("--cache-tickers", nargs="*", default=step2_manifest_resolver.DEFAULT_TICKERS)
    ap.add_argument("--score-cache-db", default=str(step2_score_cache.DEFAULT_CACHE_DB))
    ap.add_argument("--score-cache-stats-mode", choices=["fast", "full"], default="fast",
                    help="Fast records DB file metadata; full runs exact aggregate cache scans.")
    ap.add_argument("--seed-json", action="append", default=[])
    ap.add_argument("--seed", type=int, default=20260509)
    ap.add_argument("--batch-size", type=int, default=500)
    ap.add_argument("--max-batches", type=int, default=20)
    ap.add_argument("--target-count", type=int, default=5)
    ap.add_argument("--start-balance", type=float, default=100000.0)
    ap.add_argument("--beat-pct", type=float, default=5.0)
    ap.add_argument("--target-pnl", type=float, default=100000.0)
    ap.add_argument("--focus-route", action="append", default=[],
                    help="Route key to prioritize for router hunts, e.g. CLSK|trend_pullback|late.")
    ap.add_argument("--focus-plan-json", default="",
                    help="Optional next_hunt_plan JSON to feed route jobs into router hunts.")
    ap.add_argument("--mutation-scale-multiplier", type=float, default=1.0,
                    help="Online-learning control passed through to router hunts.")
    ap.add_argument("--online-state-json", default="")
    ap.add_argument("--stream-telemetry-jsonl", default="")
    ap.add_argument("--stop-signal-json", default="")
    ap.add_argument("--quarantine-json", default="")
    ap.add_argument("--min-batch-size", type=int, default=0)
    ap.add_argument("--max-batch-size", type=int, default=0)
    ap.add_argument("--exact-variant-count", action="store_true",
                    help="For router hunts, score exactly --batch-size variants per batch, ignoring learning batch throttles.")
    ap.add_argument("--runtime-control-mode", choices=["enforce", "observe"], default="enforce",
                    help="For router hunts, observe records runtime controls without applying route skips, throttles, or degrade mode.")
    ap.add_argument("--allow-uncertified-cache", action="store_true",
                    help="Diagnostics only: allow an uncertified Step 2 cache so smoke tests can exercise plumbing.")
    ap.add_argument("--learning-db", default=str(step2_learning_db.DEFAULT_DB))
    ap.add_argument("--feedback-json", default="",
                    help="Optional promotion/robustness feedback JSON to fold into learning and persist.")
    ap.add_argument("--validation-results-json", action="append", default=[],
                    help="Optional counterfactual/adversarial validation results JSON to ingest into learning DB.")
    ap.add_argument("--leaderboard-limit", type=int, default=100)
    ap.add_argument("--live-only", dest="live_only", action="store_true", default=True,
                    help="Only keep variants that beat the current Live profile in coordinator leaderboards.")
    ap.add_argument("--include-non-live", dest="live_only", action="store_false",
                    help="Diagnostics only: allow rows that do not beat current Live.")
    ap.add_argument("--behavioral-dedupe", dest="behavioral_dedupe", action="store_true", default=True,
                    help="Collapse variants with identical behavior signatures and keep aliases under the leader.")
    ap.add_argument("--config-dedupe-only", dest="behavioral_dedupe", action="store_false",
                    help="Diagnostics only: keep config-unique rows even when behavior is identical.")
    ap.add_argument("--queue-mode", action="store_true",
                    help="Use the central candidate queue and resident scorer instead of launching hunter scripts.")
    ap.add_argument("--queue-db", default=str(step2_candidate_work_queue.DEFAULT_QUEUE_DB))
    ap.add_argument("--queue-candidates", type=int, default=100)
    ap.add_argument("--queue-batch-size", type=int, default=64)
    ap.add_argument("--queue-score-limit", type=int, default=0)
    ap.add_argument("--queue-workers", type=int, default=1)
    ap.add_argument("--queue-full", action="store_true",
                    help="Store full diagnostics for every queue-scored candidate instead of summary-first rows.")
    ap.add_argument("--queue-full-finalists", type=int, default=20)
    ap.add_argument("--queue-progressive-day-count", type=int, default=0)
    ap.add_argument("--queue-promote-fraction", type=float, default=0.5)
    ap.add_argument("--hunter-timeout-sec", type=float, default=0.0,
                    help="Optional per-hunter timeout; 0 lets each hunter run to completion.")
    ap.add_argument("--coordinator-artifact-mode", choices=["full", "minimal"], default="full",
                    help="Use minimal when a wrapper will create the full final analysis artifacts.")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--json-verbosity", choices=["digest", "full"], default="digest",
                    help="Digest prints only run pointers/counts; full prints the complete coordinator payload.")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    payload = run(args)
    if args.json:
        out = payload if str(args.json_verbosity) == "full" else _compact_coordinator_payload(payload)
        print(json.dumps(out, indent=2, sort_keys=True, default=str))
    else:
        print(json.dumps({
            "path": payload.get("path"),
            "ok": payload.get("ok"),
            "dry_run": payload.get("dry_run"),
            "compiled_decision_tape": payload.get("compiled_decision_tape"),
            "hunters": payload.get("hunters"),
            "winners": len(payload.get("winners") or []),
            "milestones": len(payload.get("milestones") or []),
            "score_cache": _compact_coordinator_payload(payload).get("score_cache"),
        }, indent=2, sort_keys=True, default=str))
    return 0 if payload.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
