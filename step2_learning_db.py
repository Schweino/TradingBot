"""SQLite learning store for Step 2 hunt artifacts."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any

import step2_hunt_intelligence as hunt_intel
import step2_belief_calibration
import step2_closed_loop_controller
import step2_data_quality
import step2_learning_control_plane
import step2_learning_depth
import step2_ops_hardening
import step2_void_registry


HERE = Path(__file__).resolve().parent
DEFAULT_DB = HERE / "postmortem" / "learning" / "step2_learning.sqlite"
SCHEMA_VERSION = 1


def _now_epoch() -> float:
    return time.time()


def _read_json(path: str | os.PathLike[str], default: Any = None) -> Any:
    return hunt_intel.read_json(path, default)


def _stable_hash(payload: Any, length: int = 32) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length]


def _compact_cycle_payload(cycle: dict[str, Any]) -> dict[str, Any]:
    compact = dict(cycle or {})
    for key in ("stdout_tail", "stderr_tail"):
        if key in compact and compact[key] is not None:
            compact[key] = str(compact[key])[-1200:]
    for key in ("coordinator_payload", "learning_report", "online_state"):
        if key in compact:
            compact[key] = {"omitted": True, "reason": "stored_as_artifact"}
    return compact


def _lesson_subject_key(row: dict[str, Any]) -> str:
    subject = str(row.get("subject") or row.get("lesson") or "")
    if subject.startswith("route:"):
        parts = subject.split(":", 2)
        if len(parts) == 3 and parts[2]:
            return parts[2]
    return subject


class Step2LearningDB:
    def __init__(self, path: str | os.PathLike[str] = DEFAULT_DB):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.path), timeout=30.0)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def close(self) -> None:
        self.conn.close()

    def _init_db(self) -> None:
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS learning_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                run_dir TEXT NOT NULL,
                started_at_ct TEXT,
                finished_at_ct TEXT,
                source TEXT,
                created_at REAL NOT NULL,
                summary_json TEXT NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS cycles (
                run_id TEXT NOT NULL,
                cycle INTEGER NOT NULL,
                hunter TEXT,
                seed INTEGER,
                ok INTEGER,
                elapsed_sec REAL,
                scored_total INTEGER,
                winners INTEGER,
                raw_json TEXT NOT NULL,
                PRIMARY KEY (run_id, cycle)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS candidates (
                run_id TEXT NOT NULL,
                behavior_key TEXT NOT NULL,
                config_key TEXT,
                variant TEXT,
                route_key TEXT,
                rank INTEGER,
                step2_pnl REAL,
                delta_vs_active REAL,
                promotion_quality_score REAL,
                novelty_score REAL,
                overfit_risk_score REAL,
                alias_count INTEGER,
                source TEXT,
                raw_json TEXT NOT NULL,
                PRIMARY KEY (run_id, behavior_key)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS route_arms (
                run_id TEXT NOT NULL,
                route_key TEXT NOT NULL,
                recommended_budget_pct REAL,
                reward_score REAL,
                best_variant TEXT,
                raw_json TEXT NOT NULL,
                PRIMARY KEY (run_id, route_key)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS artifacts (
                run_id TEXT NOT NULL,
                artifact_name TEXT NOT NULL,
                path TEXT NOT NULL,
                sha_key TEXT,
                created_at REAL NOT NULL,
                PRIMARY KEY (run_id, artifact_name)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS promotion_feedback (
                feedback_id TEXT PRIMARY KEY,
                variant TEXT,
                behavior_key TEXT,
                route_key TEXT,
                status TEXT,
                reject_reasons_json TEXT,
                source_path TEXT,
                created_at REAL NOT NULL,
                raw_json TEXT NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS validation_results (
                result_id TEXT PRIMARY KEY,
                kind TEXT,
                variant TEXT,
                route_key TEXT,
                baseline_step2_pnl REAL,
                metric_name TEXT,
                metric_value REAL,
                passed INTEGER,
                source_path TEXT,
                created_at REAL NOT NULL,
                raw_json TEXT NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS online_events (
                event_id TEXT PRIMARY KEY,
                run_id TEXT,
                event_type TEXT,
                route_key TEXT,
                created_at REAL NOT NULL,
                raw_json TEXT NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS causal_experiments (
                run_id TEXT NOT NULL,
                experiment_id TEXT NOT NULL,
                kind TEXT,
                route_key TEXT,
                family_key TEXT,
                hypothesis TEXT,
                status TEXT,
                verdict TEXT,
                raw_json TEXT NOT NULL,
                PRIMARY KEY (run_id, experiment_id)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS experiment_debt (
                debt_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                question TEXT,
                route_key TEXT,
                family_key TEXT,
                priority_score REAL,
                status TEXT,
                raw_json TEXT NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS calibration_predictions (
                prediction_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                prediction_type TEXT,
                variant TEXT,
                treatment TEXT,
                action TEXT,
                target TEXT,
                predicted_probability REAL,
                actual TEXT,
                calibration_error REAL,
                status TEXT,
                raw_json TEXT NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS belief_calibration_memory (
                belief_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                belief_type TEXT,
                subject TEXT,
                predicted_probability REAL NOT NULL DEFAULT 0.5,
                actual TEXT,
                actual_numeric REAL,
                calibration_error REAL,
                brier_score REAL,
                status TEXT,
                resolution_source TEXT,
                updated_at REAL NOT NULL,
                raw_json TEXT NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS temporal_route_memory (
                route_key TEXT PRIMARY KEY,
                last_seen_run_id TEXT,
                seen_count INTEGER NOT NULL DEFAULT 0,
                decayed_confidence REAL NOT NULL DEFAULT 0.0,
                decayed_score REAL NOT NULL DEFAULT 0.0,
                last_seen_at REAL NOT NULL,
                raw_json TEXT NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS temporal_policy_memory (
                policy_name TEXT PRIMARY KEY,
                last_seen_run_id TEXT,
                champion_count INTEGER NOT NULL DEFAULT 0,
                seen_count INTEGER NOT NULL DEFAULT 0,
                last_score REAL NOT NULL DEFAULT 0.0,
                decayed_score REAL NOT NULL DEFAULT 0.0,
                last_seen_at REAL NOT NULL,
                raw_json TEXT NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS route_lifecycle_memory (
                route_key TEXT PRIMARY KEY,
                last_seen_run_id TEXT,
                state TEXT,
                next_action TEXT,
                confidence REAL NOT NULL DEFAULT 0.0,
                updated_at REAL NOT NULL,
                raw_json TEXT NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS negative_knowledge (
                pattern_key TEXT PRIMARY KEY,
                route_key TEXT,
                reason TEXT,
                severity TEXT,
                source_run_id TEXT,
                seen_count INTEGER NOT NULL DEFAULT 0,
                updated_at REAL NOT NULL,
                raw_json TEXT NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS mutation_grammar_memory (
                grammar_id TEXT PRIMARY KEY,
                route_key TEXT,
                treatment TEXT,
                mutation_radius TEXT,
                learning_score REAL NOT NULL DEFAULT 0.0,
                source_run_id TEXT,
                updated_at REAL NOT NULL,
                raw_json TEXT NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS policy_league_memory (
                participant_key TEXT PRIMARY KEY,
                participant_type TEXT,
                name TEXT,
                rating REAL NOT NULL DEFAULT 1500.0,
                wins INTEGER NOT NULL DEFAULT 0,
                losses INTEGER NOT NULL DEFAULT 0,
                draws INTEGER NOT NULL DEFAULT 0,
                last_seen_run_id TEXT,
                updated_at REAL NOT NULL,
                raw_json TEXT NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS self_correction_memory (
                memory_key TEXT PRIMARY KEY,
                kind TEXT,
                route_key TEXT,
                policy_name TEXT,
                risk_score REAL NOT NULL DEFAULT 0.0,
                status TEXT,
                source_run_id TEXT,
                updated_at REAL NOT NULL,
                raw_json TEXT NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS evidence_contract_memory (
                contract_id TEXT PRIMARY KEY,
                belief_type TEXT,
                belief TEXT,
                status TEXT,
                scope_json TEXT,
                source_run_id TEXT,
                updated_at REAL NOT NULL,
                raw_json TEXT NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS research_trace_memory (
                trace_id TEXT PRIMARY KEY,
                kind TEXT,
                subject TEXT,
                decision TEXT,
                confidence REAL NOT NULL DEFAULT 0.0,
                source_run_id TEXT,
                updated_at REAL NOT NULL,
                raw_json TEXT NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS command_replay_memory (
                command_id TEXT PRIMARY KEY,
                run_id TEXT,
                action TEXT,
                route_key TEXT,
                reward_score REAL NOT NULL DEFAULT 0.0,
                worked INTEGER NOT NULL DEFAULT 0,
                updated_at REAL NOT NULL,
                raw_json TEXT NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS action_league_memory (
                action TEXT PRIMARY KEY,
                rating REAL NOT NULL DEFAULT 1500.0,
                sample_count INTEGER NOT NULL DEFAULT 0,
                success_rate REAL NOT NULL DEFAULT 0.0,
                last_seen_run_id TEXT,
                updated_at REAL NOT NULL,
                raw_json TEXT NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS command_outcome_memory (
                outcome_id TEXT PRIMARY KEY,
                run_id TEXT,
                command_id TEXT,
                action TEXT,
                route_key TEXT,
                reward_score REAL NOT NULL DEFAULT 0.0,
                worked INTEGER NOT NULL DEFAULT 0,
                updated_at REAL NOT NULL,
                raw_json TEXT NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS action_reward_calibration_memory (
                calibration_id TEXT PRIMARY KEY,
                run_id TEXT,
                action TEXT,
                predicted_reward REAL NOT NULL DEFAULT 0.0,
                observed_reward REAL NOT NULL DEFAULT 0.0,
                abs_error REAL NOT NULL DEFAULT 0.0,
                sample_count INTEGER NOT NULL DEFAULT 0,
                updated_at REAL NOT NULL,
                raw_json TEXT NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS runtime_experiment_memory (
                memory_id TEXT PRIMARY KEY,
                run_id TEXT,
                artifact_name TEXT,
                subject TEXT,
                priority REAL NOT NULL DEFAULT 0.0,
                updated_at REAL NOT NULL,
                raw_json TEXT NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS learning_control_memory (
                control_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                artifact_type TEXT,
                subject TEXT,
                score REAL NOT NULL DEFAULT 0.0,
                status TEXT,
                updated_at REAL NOT NULL,
                raw_json TEXT NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS learning_depth_memory (
                depth_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                artifact_type TEXT,
                subject TEXT,
                score REAL NOT NULL DEFAULT 0.0,
                status TEXT,
                updated_at REAL NOT NULL,
                raw_json TEXT NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS ops_hardening_memory (
                ops_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                artifact_type TEXT,
                subject TEXT,
                score REAL NOT NULL DEFAULT 0.0,
                status TEXT,
                updated_at REAL NOT NULL,
                raw_json TEXT NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS run_data_quality (
                run_id TEXT PRIMARY KEY,
                contract_id TEXT,
                learning_allowed INTEGER NOT NULL,
                score REAL NOT NULL DEFAULT 0.0,
                blockers_json TEXT NOT NULL,
                warnings_json TEXT NOT NULL,
                raw_json TEXT NOT NULL
            )
        """)
        self.conn.execute(
            "INSERT OR REPLACE INTO learning_meta(key, value) VALUES (?, ?)",
            ("schema_version", str(SCHEMA_VERSION)),
        )
        self.conn.commit()

    def ingest_run_dir(self, run_dir: str | os.PathLike[str]) -> dict[str, Any]:
        run_path = Path(run_dir)
        summary_path = run_path / "final_summary.json"
        if not summary_path.exists():
            summary_path = run_path / "coordinator_summary.json"
        if not summary_path.exists():
            summary_path = run_path / "summary.json"
        summary = _read_json(summary_path, {}) or {}
        run_id = run_path.name
        data_quality_contract = step2_data_quality.run_provenance(run_path, summary)
        void_report = step2_void_registry.contamination_report(summary, source_path=run_path)
        if void_report["voided_source_days"]:
            return {
                "run_id": run_id,
                "skipped": True,
                "skip_reason": "voided_source_day",
                "voided_source_days": void_report["voided_source_days"],
                "candidates": 0,
                "route_arms": 0,
                "cycles": 0,
            }
        self.conn.execute(
            """
            INSERT OR REPLACE INTO runs(run_id, run_dir, started_at_ct, finished_at_ct, source, created_at, summary_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                str(run_path.resolve()),
                summary.get("started_at_ct") or summary.get("created_at_ct"),
                summary.get("finished_at_ct"),
                summary.get("source"),
                _now_epoch(),
                json.dumps(summary, sort_keys=True, separators=(",", ":"), default=str),
            ),
        )
        self._record_run_data_quality(run_id, data_quality_contract)
        cycles = list(summary.get("cycles") or summary.get("runs") or [])
        for idx, cycle in enumerate(cycles):
            self.conn.execute(
                """
                INSERT OR REPLACE INTO cycles(run_id, cycle, hunter, seed, ok, elapsed_sec, scored_total, winners, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    int(cycle.get("cycle") if cycle.get("cycle") is not None else idx),
                    cycle.get("hunter"),
                    int(cycle.get("seed") or 0),
                    1 if cycle.get("ok") else 0,
                    float(cycle.get("elapsed_sec") or 0.0),
                    hunt_intel._cycle_scored_total(cycle),
                    hunt_intel._cycle_winners(cycle),
                    json.dumps(_compact_cycle_payload(cycle), sort_keys=True, separators=(",", ":"), default=str),
                ),
            )
        finalists = _read_json(run_path / "finalists.json", {}) or {}
        rows = finalists.get("finalists") or summary.get("top100") or summary.get("leaderboard") or []
        for idx, row in enumerate(rows):
            bkey = str(row.get("behavior_key") or hunt_intel.behavior_key(row))
            self.conn.execute(
                """
                INSERT OR REPLACE INTO candidates(
                    run_id, behavior_key, config_key, variant, route_key, rank, step2_pnl,
                    delta_vs_active, promotion_quality_score, novelty_score, overfit_risk_score,
                    alias_count, source, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    bkey,
                    row.get("config_key"),
                    row.get("variant"),
                    row.get("route_key") or hunt_intel.route_key_from_row(row),
                    int(row.get("rank") or idx + 1),
                    float(row.get("step2_pnl") or 0.0),
                    float(row.get("step2_delta_vs_active") or 0.0),
                    float(row.get("promotion_quality_score") or 0.0),
                    float(row.get("novelty_score") or 0.0),
                    float(row.get("overfit_risk_score") or 0.0),
                    int(row.get("behavior_alias_count") or 1),
                    row.get("source"),
                    json.dumps(row, sort_keys=True, separators=(",", ":"), default=str),
                ),
            )
        bandit = _read_json(run_path / "bandit_allocation.json", {}) or {}
        for arm in bandit.get("allocation") or []:
            route_key = str(arm.get("route_key") or "")
            if not route_key:
                continue
            self.conn.execute(
                """
                INSERT OR REPLACE INTO route_arms(run_id, route_key, recommended_budget_pct, reward_score, best_variant, raw_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    route_key,
                    float(arm.get("recommended_budget_pct") or 0.0),
                    float(arm.get("reward_score") or 0.0),
                    arm.get("best_variant"),
                    json.dumps(arm, sort_keys=True, separators=(",", ":"), default=str),
                ),
            )
        artifact_paths = summary.get("artifact_paths") if isinstance(summary.get("artifact_paths"), dict) else {}
        for name, path in artifact_paths.items():
            self.conn.execute(
                """
                INSERT OR REPLACE INTO artifacts(run_id, artifact_name, path, sha_key, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (run_id, str(name), str(path), "", _now_epoch()),
            )
        registry = _read_json(run_path / "causal_experiment_registry.json", {}) or {}
        if not registry and isinstance(summary.get("causal_experiment_registry"), dict):
            registry = summary.get("causal_experiment_registry") or {}
        for exp in registry.get("experiments") or []:
            exp_id = str(exp.get("experiment_id") or _stable_hash(exp, length=20))
            self.conn.execute(
                """
                INSERT OR REPLACE INTO causal_experiments(
                    run_id, experiment_id, kind, route_key, family_key, hypothesis, status, verdict, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    exp_id,
                    exp.get("kind"),
                    exp.get("route_key"),
                    exp.get("family_key"),
                    exp.get("hypothesis"),
                    exp.get("status"),
                    exp.get("verdict"),
                    json.dumps(exp, sort_keys=True, separators=(",", ":"), default=str),
                ),
            )
        debt = _read_json(run_path / "experiment_debt_queue.json", {}) or {}
        if not debt and isinstance(summary.get("experiment_debt_queue"), dict):
            debt = summary.get("experiment_debt_queue") or {}
        for row in debt.get("queue") or []:
            debt_id = _stable_hash({"run_id": run_id, "question": row.get("question"), "route": row.get("route_key"), "family": row.get("family_key")})
            self.conn.execute(
                """
                INSERT OR REPLACE INTO experiment_debt(
                    debt_id, run_id, question, route_key, family_key, priority_score, status, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    debt_id,
                    run_id,
                    row.get("question"),
                    row.get("route_key"),
                    row.get("family_key"),
                    float(row.get("priority_score") or 0.0),
                    row.get("status") or "open",
                    json.dumps(row, sort_keys=True, separators=(",", ":"), default=str),
                ),
            )
        calibration = _read_json(run_path / "prediction_calibration_ledger.json", {}) or {}
        if not calibration and isinstance(summary.get("prediction_calibration_ledger"), dict):
            calibration = summary.get("prediction_calibration_ledger") or {}
        self._ingest_calibration_payload(run_id, calibration)
        half_life = _read_json(run_path / "route_regime_half_life.json", {}) or {}
        if not half_life and isinstance(summary.get("route_regime_half_life"), dict):
            half_life = summary.get("route_regime_half_life") or {}
        if not half_life:
            half_life = _read_json(run_path / "lesson_half_life_report.json", {}) or {}
        if not half_life and isinstance(summary.get("lesson_half_life_report"), dict):
            half_life = summary.get("lesson_half_life_report") or {}
        self._ingest_temporal_route_payload(run_id, half_life)
        tournament = _read_json(run_path / "policy_tournament.json", {}) or {}
        if not tournament and isinstance(summary.get("policy_tournament"), dict):
            tournament = summary.get("policy_tournament") or {}
        self._ingest_temporal_policy_payload(run_id, tournament)
        route_lifecycle = _read_json(run_path / "route_state_machine.json", {}) or {}
        if not route_lifecycle and isinstance(summary.get("route_state_machine"), dict):
            route_lifecycle = summary.get("route_state_machine") or {}
        self._ingest_route_lifecycle_payload(run_id, route_lifecycle)
        negative = _read_json(run_path / "negative_knowledge_bank.json", {}) or {}
        if not negative and isinstance(summary.get("negative_knowledge_bank"), dict):
            negative = summary.get("negative_knowledge_bank") or {}
        self._ingest_negative_knowledge_payload(run_id, negative)
        grammar = _read_json(run_path / "mutation_grammar_learner.json", {}) or {}
        if not grammar and isinstance(summary.get("mutation_grammar_learner"), dict):
            grammar = summary.get("mutation_grammar_learner") or {}
        self._ingest_mutation_grammar_payload(run_id, grammar)
        league = _read_json(run_path / "self_competition_league.json", {}) or {}
        if not league and isinstance(summary.get("self_competition_league"), dict):
            league = summary.get("self_competition_league") or {}
        self._ingest_policy_league_payload(run_id, league)
        self._ingest_self_correction_payload(run_id, run_path, summary)
        contracts = _read_json(run_path / "evidence_contract_engine.json", {}) or {}
        if not contracts and isinstance(summary.get("evidence_contract_engine"), dict):
            contracts = summary.get("evidence_contract_engine") or {}
        self._ingest_evidence_contract_payload(run_id, contracts)
        trace = _read_json(run_path / "research_trace_ledger.json", {}) or {}
        if not trace and isinstance(summary.get("research_trace_ledger"), dict):
            trace = summary.get("research_trace_ledger") or {}
        self._ingest_research_trace_payload(run_id, trace)
        command_replay = _read_json(run_path / "command_replay_ledger.json", {}) or {}
        if not command_replay and isinstance(summary.get("command_replay_ledger"), dict):
            command_replay = summary.get("command_replay_ledger") or {}
        self._ingest_command_replay_payload(run_id, command_replay)
        action_league = _read_json(run_path / "action_elo_league.json", {}) or {}
        if not action_league and isinstance(summary.get("action_elo_league"), dict):
            action_league = summary.get("action_elo_league") or {}
        self._ingest_action_league_payload(run_id, action_league)
        command_backfill = _read_json(run_path / "command_outcome_backfill.json", {}) or {}
        if not command_backfill and isinstance(summary.get("command_outcome_backfill"), dict):
            command_backfill = summary.get("command_outcome_backfill") or {}
        self._ingest_command_outcome_backfill(run_id, command_backfill)
        reward_calibration = _read_json(run_path / "action_reward_calibration.json", {}) or {}
        if not reward_calibration and isinstance(summary.get("action_reward_calibration"), dict):
            reward_calibration = summary.get("action_reward_calibration") or {}
        self._ingest_action_reward_calibration(run_id, reward_calibration)
        walk_forward_validation = _read_json(run_path / "walk_forward_validation_bundle.json", {}) or {}
        if not walk_forward_validation and isinstance(summary.get("walk_forward_validation_bundle"), dict):
            walk_forward_validation = summary.get("walk_forward_validation_bundle") or {}
        self._ingest_validation_payload(run_id, walk_forward_validation, run_path / "walk_forward_validation_bundle.json")
        belief_update = _read_json(run_path / "belief_update_report.json", {}) or {}
        if not belief_update and isinstance(summary.get("belief_update_report"), dict):
            belief_update = summary.get("belief_update_report") or {}
        belief_calibration = _read_json(run_path / "belief_calibration_report.json", {}) or {}
        if not belief_calibration and isinstance(summary.get("belief_calibration_report"), dict):
            belief_calibration = summary.get("belief_calibration_report") or {}
        self._ingest_belief_calibration_payload(run_id, belief_update, belief_calibration)
        learning_control = _read_json(run_path / "learning_control_plane.json", {}) or {}
        if not learning_control and isinstance(summary.get("learning_control_plane"), dict):
            learning_control = summary.get("learning_control_plane") or {}
        self._ingest_learning_control_plane_payload(run_id, learning_control)
        learning_depth = _read_json(run_path / "learning_depth_report.json", {}) or {}
        if not learning_depth and isinstance(summary.get("learning_depth_report"), dict):
            learning_depth = summary.get("learning_depth_report") or {}
        self._ingest_learning_depth_payload(run_id, learning_depth)
        ops_hardening = _read_json(run_path / "ops_hardening_report.json", {}) or {}
        if not ops_hardening and isinstance(summary.get("ops_hardening_report"), dict):
            ops_hardening = summary.get("ops_hardening_report") or {}
        self._ingest_ops_hardening_payload(run_id, ops_hardening)
        self._ingest_runtime_experiment_memory(run_id, run_path, summary)
        self.conn.commit()
        return self.stats(run_id=run_id)

    def _record_run_data_quality(self, run_id: str, contract: dict[str, Any]) -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO run_data_quality(
                run_id, contract_id, learning_allowed, score, blockers_json, warnings_json, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                contract.get("contract_id"),
                1 if contract.get("learning_allowed") else 0,
                float(contract.get("score") or 0.0),
                json.dumps(contract.get("blockers") or [], sort_keys=True, separators=(",", ":"), default=str),
                json.dumps(contract.get("warnings") or [], sort_keys=True, separators=(",", ":"), default=str),
                json.dumps(contract, sort_keys=True, separators=(",", ":"), default=str),
            ),
        )

    def _ingest_command_replay_payload(self, run_id: str, payload: dict[str, Any]) -> int:
        inserted = 0
        now = _now_epoch()
        for row in payload.get("entries") or []:
            command_id = str(row.get("command_id") or _stable_hash(row, length=20))
            outcome = row.get("outcome") if isinstance(row.get("outcome"), dict) else {}
            self.conn.execute(
                """
                INSERT OR REPLACE INTO command_replay_memory(
                    command_id, run_id, action, route_key, reward_score, worked, updated_at, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    command_id,
                    run_id,
                    row.get("action"),
                    row.get("route_key"),
                    float(outcome.get("reward_score") or 0.0),
                    1 if outcome.get("worked") else 0,
                    now,
                    json.dumps(row, sort_keys=True, separators=(",", ":"), default=str),
                ),
            )
            inserted += 1
        return inserted

    def _ingest_action_league_payload(self, run_id: str, payload: dict[str, Any]) -> int:
        inserted = 0
        now = _now_epoch()
        for row in payload.get("leaderboard") or []:
            action = str(row.get("action") or "")
            if not action:
                continue
            self.conn.execute(
                """
                INSERT OR REPLACE INTO action_league_memory(
                    action, rating, sample_count, success_rate, last_seen_run_id, updated_at, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    action,
                    float(row.get("rating") or 1500.0),
                    int(row.get("sample_count") or 0),
                    float(row.get("success_rate") or 0.0),
                    run_id,
                    now,
                    json.dumps(row, sort_keys=True, separators=(",", ":"), default=str),
                ),
            )
            inserted += 1
        return inserted

    def _ingest_command_outcome_backfill(self, run_id: str, payload: dict[str, Any]) -> int:
        inserted = 0
        now = _now_epoch()
        for row in payload.get("outcomes") or []:
            command_id = str(row.get("command_id") or _stable_hash(row, length=20))
            outcome_id = _stable_hash({"run_id": run_id, "command_id": command_id, "route": row.get("route_key"), "action": row.get("action")}, length=24)
            self.conn.execute(
                """
                INSERT OR REPLACE INTO command_outcome_memory(
                    outcome_id, run_id, command_id, action, route_key, reward_score, worked, updated_at, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    outcome_id,
                    run_id,
                    command_id,
                    row.get("action"),
                    row.get("route_key"),
                    float(row.get("reward_score") or 0.0),
                    1 if row.get("worked") else 0,
                    now,
                    json.dumps(row, sort_keys=True, separators=(",", ":"), default=str),
                ),
            )
            inserted += 1
        return inserted

    def _ingest_action_reward_calibration(self, run_id: str, payload: dict[str, Any]) -> int:
        inserted = 0
        now = _now_epoch()
        for row in payload.get("rows") or []:
            action = str(row.get("action") or "")
            if not action:
                continue
            calibration_id = _stable_hash({"run_id": run_id, "action": action, "sample_count": row.get("sample_count")}, length=24)
            self.conn.execute(
                """
                INSERT OR REPLACE INTO action_reward_calibration_memory(
                    calibration_id, run_id, action, predicted_reward, observed_reward, abs_error, sample_count, updated_at, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    calibration_id,
                    run_id,
                    action,
                    float(row.get("predicted_reward") or 0.0),
                    float(row.get("observed_reward") or 0.0),
                    float(row.get("abs_error") or 0.0),
                    int(row.get("sample_count") or 0),
                    now,
                    json.dumps(row, sort_keys=True, separators=(",", ":"), default=str),
                ),
            )
            inserted += 1
        return inserted

    def _ingest_runtime_experiment_memory(self, run_id: str, run_path: Path, summary: dict[str, Any]) -> int:
        inserted = 0
        now = _now_epoch()
        specs = {
            "ab_route_experiment_executor": ("assignments", "assignment_id"),
            "champion_challenger_runtime_slots": ("slots", "slot"),
            "adaptive_experiment_stopping": ("decisions", "experiment_id"),
            "counterfactual_command_replay": ("simulations", "base_command_id"),
            "experiment_contamination_guard": ("risks", "kind"),
            "learning_rate_controller": ("mode", "mode"),
            "worker_learning_report_cards": ("cards", "worker"),
            "experiment_to_promotion_trace": ("traces", "variant"),
            "zero_yield_autopsy_engine": ("autopsies", "primary_reason"),
            "stuck_loop_breaker": ("resets", "kind"),
            "opportunity_cost_meter": ("costs", "route_key"),
            "search_space_coverage_map": ("blind_spots", "route_key"),
            "live_beater_scarcity_mode": ("mode", "mode"),
            "alias_trap_detector": ("traps", "route_key"),
            "route_seed_quality_score": ("scores", "route_key"),
            "recovery_playbook_generator": ("interventions", "intervention_id"),
        }
        for artifact, (row_key, subject_key) in specs.items():
            payload = _read_json(run_path / f"{artifact}.json", {}) or {}
            if not payload and isinstance(summary.get(artifact), dict):
                payload = summary.get(artifact) or {}
            if not payload:
                continue
            rows = payload.get(row_key) if isinstance(payload.get(row_key), list) else []
            if row_key == "mode" and payload.get("mode"):
                rows = [payload]
            if not rows:
                rows = [payload]
            for idx, row in enumerate(rows[:100]):
                if not isinstance(row, dict):
                    continue
                subject = str(row.get(subject_key) or row.get("route_key") or row.get("experiment_id") or row.get("action") or artifact)
                memory_id = _stable_hash({"run_id": run_id, "artifact": artifact, "subject": subject, "idx": idx}, length=24)
                self.conn.execute(
                    """
                    INSERT OR REPLACE INTO runtime_experiment_memory(
                        memory_id, run_id, artifact_name, subject, priority, updated_at, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        memory_id,
                        run_id,
                        artifact,
                        subject,
                        float(row.get("priority_score") or row.get("learning_score") or row.get("estimated_lift_vs_observed") or 0.0),
                        now,
                        json.dumps(row, sort_keys=True, separators=(",", ":"), default=str),
                    ),
                )
                inserted += 1
        return inserted

    def _ingest_evidence_contract_payload(self, run_id: str, payload: dict[str, Any]) -> int:
        inserted = 0
        now = _now_epoch()
        for row in payload.get("contracts") or []:
            contract_id = str(row.get("contract_id") or _stable_hash(row, length=20))
            self.conn.execute(
                """
                INSERT OR REPLACE INTO evidence_contract_memory(
                    contract_id, belief_type, belief, status, scope_json, source_run_id, updated_at, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    contract_id,
                    row.get("belief_type"),
                    row.get("belief"),
                    row.get("status"),
                    json.dumps(row.get("scope") or {}, sort_keys=True, separators=(",", ":"), default=str),
                    run_id,
                    now,
                    json.dumps(row, sort_keys=True, separators=(",", ":"), default=str),
                ),
            )
            inserted += 1
        return inserted

    def _ingest_research_trace_payload(self, run_id: str, payload: dict[str, Any]) -> int:
        inserted = 0
        now = _now_epoch()
        for row in payload.get("entries") or []:
            trace_id = str(row.get("trace_id") or _stable_hash(row, length=24))
            self.conn.execute(
                """
                INSERT OR REPLACE INTO research_trace_memory(
                    trace_id, kind, subject, decision, confidence, source_run_id, updated_at, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trace_id,
                    row.get("kind"),
                    row.get("subject"),
                    row.get("decision"),
                    float(row.get("confidence") or 0.0),
                    run_id,
                    now,
                    json.dumps(row, sort_keys=True, separators=(",", ":"), default=str),
                ),
            )
            inserted += 1
        return inserted

    def _ingest_policy_league_payload(self, run_id: str, payload: dict[str, Any]) -> int:
        inserted = 0
        now = _now_epoch()
        for row in payload.get("leaderboard") or []:
            participant_key = str(row.get("participant_key") or _stable_hash(row, length=24))
            rating = float(row.get("rating") or 1500.0)
            existing = self.conn.execute(
                "SELECT wins, losses, draws FROM policy_league_memory WHERE participant_key=?",
                (participant_key,),
            ).fetchone()
            wins = int(existing["wins"] if existing else 0)
            losses = int(existing["losses"] if existing else 0)
            draws = int(existing["draws"] if existing else 0)
            rank = int(row.get("rank") or 999)
            if rank == 1:
                wins += 1
            elif rank <= 5:
                draws += 1
            else:
                losses += 1
            self.conn.execute(
                """
                INSERT OR REPLACE INTO policy_league_memory(
                    participant_key, participant_type, name, rating, wins, losses, draws,
                    last_seen_run_id, updated_at, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    participant_key,
                    row.get("participant_type"),
                    row.get("name"),
                    rating,
                    wins,
                    losses,
                    draws,
                    run_id,
                    now,
                    json.dumps(row, sort_keys=True, separators=(",", ":"), default=str),
                ),
            )
            inserted += 1
        return inserted

    def _ingest_self_correction_payload(self, run_id: str, run_path: Path, summary: dict[str, Any]) -> int:
        inserted = 0
        now = _now_epoch()
        sources = [
            ("false_lesson", _read_json(run_path / "false_lesson_detector.json", {}) or summary.get("false_lesson_detector") or {}, "lessons"),
            ("uncertainty_budget", _read_json(run_path / "uncertainty_budgeting.json", {}) or summary.get("uncertainty_budgeting") or {}, "budgets"),
            ("pareto_frontier", _read_json(run_path / "promotability_pareto_frontier.json", {}) or summary.get("promotability_pareto_frontier") or {}, "frontier"),
            ("causal_mutation", _read_json(run_path / "causal_mutation_attribution.json", {}) or summary.get("causal_mutation_attribution") or {}, "attributions"),
            ("experiment_graduation", _read_json(run_path / "experiment_graduation_system.json", {}) or summary.get("experiment_graduation_system") or {}, "experiments"),
        ]
        for kind, payload, key in sources:
            rows = payload.get(key) if isinstance(payload, dict) else []
            for row in rows or []:
                memory_key = _stable_hash({"kind": kind, "run_id": run_id, "row": row}, length=28)
                risk = float(row.get("false_lesson_risk_score") or row.get("uncertainty") or row.get("frontier_score") or row.get("expected_pnl_lift") or 0.0)
                self.conn.execute(
                    """
                    INSERT OR REPLACE INTO self_correction_memory(
                        memory_key, kind, route_key, policy_name, risk_score, status,
                        source_run_id, updated_at, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        memory_key,
                        kind,
                        row.get("route_key"),
                        row.get("policy_name") or row.get("recommended_policy"),
                        risk,
                        row.get("advice") or row.get("stage") or row.get("recommended_budget") or row.get("verdict"),
                        run_id,
                        now,
                        json.dumps(row, sort_keys=True, separators=(",", ":"), default=str),
                    ),
                )
                inserted += 1
        return inserted

    def _ingest_route_lifecycle_payload(self, run_id: str, payload: dict[str, Any]) -> int:
        inserted = 0
        now = _now_epoch()
        for row in payload.get("states") or []:
            route_key = str(row.get("route_key") or "")
            if not route_key:
                continue
            confidence = max(0.0, min(1.0, float(row.get("freshness_score") or 0.0) / 100.0))
            self.conn.execute(
                """
                INSERT OR REPLACE INTO route_lifecycle_memory(
                    route_key, last_seen_run_id, state, next_action, confidence, updated_at, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    route_key,
                    run_id,
                    row.get("state"),
                    row.get("next_action"),
                    confidence,
                    now,
                    json.dumps(row, sort_keys=True, separators=(",", ":"), default=str),
                ),
            )
            inserted += 1
        return inserted

    def _ingest_negative_knowledge_payload(self, run_id: str, payload: dict[str, Any]) -> int:
        inserted = 0
        now = _now_epoch()
        for row in payload.get("patterns") or []:
            pattern_key = str(row.get("pattern_key") or _stable_hash(row, length=20))
            existing = self.conn.execute(
                "SELECT seen_count FROM negative_knowledge WHERE pattern_key=?",
                (pattern_key,),
            ).fetchone()
            seen_count = int(existing["seen_count"] if existing else 0) + 1
            self.conn.execute(
                """
                INSERT OR REPLACE INTO negative_knowledge(
                    pattern_key, route_key, reason, severity, source_run_id, seen_count, updated_at, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    pattern_key,
                    row.get("route_key"),
                    row.get("reason"),
                    row.get("severity"),
                    run_id,
                    seen_count,
                    now,
                    json.dumps(row, sort_keys=True, separators=(",", ":"), default=str),
                ),
            )
            inserted += 1
        return inserted

    def _ingest_mutation_grammar_payload(self, run_id: str, payload: dict[str, Any]) -> int:
        inserted = 0
        now = _now_epoch()
        for row in payload.get("grammar") or payload.get("top_templates") or []:
            grammar_id = str(row.get("grammar_id") or _stable_hash(row, length=18))
            self.conn.execute(
                """
                INSERT OR REPLACE INTO mutation_grammar_memory(
                    grammar_id, route_key, treatment, mutation_radius, learning_score, source_run_id, updated_at, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    grammar_id,
                    row.get("route_key"),
                    row.get("treatment"),
                    row.get("mutation_radius"),
                    float(row.get("learning_score") or 0.0),
                    run_id,
                    now,
                    json.dumps(row, sort_keys=True, separators=(",", ":"), default=str),
                ),
            )
            inserted += 1
        return inserted

    def _ingest_temporal_route_payload(self, run_id: str, payload: dict[str, Any]) -> int:
        inserted = 0
        now = _now_epoch()
        rows = list(payload.get("routes") or payload.get("lessons") or [])
        for row in rows:
            route_key = str(row.get("route_key") or _lesson_subject_key(row))
            if not route_key:
                continue
            existing = self.conn.execute(
                "SELECT seen_count, decayed_confidence, decayed_score FROM temporal_route_memory WHERE route_key=?",
                (route_key,),
            ).fetchone()
            seen_count = int(existing["seen_count"] if existing else 0) + 1
            old_conf = float(existing["decayed_confidence"] if existing else 0.0)
            old_score = float(existing["decayed_score"] if existing else 0.0)
            freshness = float(row.get("freshness") or row.get("freshness_score") or row.get("confidence") or 0.0)
            if freshness > 1.0:
                freshness /= 100.0
            score = float(
                row.get("decayed_expected_promotable_pnl")
                or row.get("decayed_score")
                or (float(row.get("confidence") or 0.0) * 100.0)
                or 0.0
            )
            decayed_conf = old_conf * 0.85 + freshness * 0.15
            decayed_score = old_score * 0.85 + score * 0.15
            self.conn.execute(
                """
                INSERT OR REPLACE INTO temporal_route_memory(
                    route_key, last_seen_run_id, seen_count, decayed_confidence,
                    decayed_score, last_seen_at, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    route_key,
                    run_id,
                    seen_count,
                    decayed_conf,
                    decayed_score,
                    now,
                    json.dumps(row, sort_keys=True, separators=(",", ":"), default=str),
                ),
            )
            inserted += 1
        return inserted

    def _ingest_temporal_policy_payload(self, run_id: str, payload: dict[str, Any]) -> int:
        inserted = 0
        now = _now_epoch()
        champion_name = str(((payload.get("champion") or {}).get("policy_name")) or "")
        rows = []
        if isinstance(payload.get("champion"), dict):
            rows.append(payload.get("champion") or {})
        rows.extend(payload.get("challengers") or [])
        rows.extend(payload.get("results") or [])
        seen_names: set[str] = set()
        for row in rows:
            policy_name = str(row.get("policy_name") or "")
            if not policy_name or policy_name in seen_names:
                continue
            seen_names.add(policy_name)
            existing = self.conn.execute(
                "SELECT seen_count, champion_count, decayed_score FROM temporal_policy_memory WHERE policy_name=?",
                (policy_name,),
            ).fetchone()
            seen_count = int(existing["seen_count"] if existing else 0) + 1
            champion_count = int(existing["champion_count"] if existing else 0) + (1 if policy_name == champion_name else 0)
            score = float(row.get("score") or row.get("avg_expected_promotable_pnl") or 0.0)
            old_score = float(existing["decayed_score"] if existing else 0.0)
            decayed_score = old_score * 0.85 + score * 0.15
            self.conn.execute(
                """
                INSERT OR REPLACE INTO temporal_policy_memory(
                    policy_name, last_seen_run_id, champion_count, seen_count, last_score,
                    decayed_score, last_seen_at, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    policy_name,
                    run_id,
                    champion_count,
                    seen_count,
                    score,
                    decayed_score,
                    now,
                    json.dumps(row, sort_keys=True, separators=(",", ":"), default=str),
                ),
            )
            inserted += 1
        return inserted

    def _ingest_calibration_payload(self, run_id: str, payload: dict[str, Any]) -> int:
        inserted = 0
        rows = []
        rows.extend(payload.get("entries") or [])
        rows.extend(payload.get("treatment_entries") or [])
        rows.extend(payload.get("value_of_information_entries") or [])
        for row in rows:
            prediction_id = _stable_hash({"run_id": run_id, "row": row})
            self.conn.execute(
                """
                INSERT OR REPLACE INTO calibration_predictions(
                    prediction_id, run_id, prediction_type, variant, treatment, action, target,
                    predicted_probability, actual, calibration_error, status, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    prediction_id,
                    run_id,
                    row.get("prediction_type"),
                    row.get("variant"),
                    row.get("treatment"),
                    row.get("action"),
                    row.get("target"),
                    float(row.get("predicted_probability") or row.get("posterior_success_mean") or row.get("predicted_decision_change_probability") or 0.0),
                    row.get("actual"),
                    float(row.get("calibration_error") or 0.0),
                    "resolved" if row.get("actual") not in {None, "", "pending", "pending_future_outcome"} else "pending",
                    json.dumps(row, sort_keys=True, separators=(",", ":"), default=str),
                ),
            )
            inserted += 1
        return inserted

    def _ingest_belief_calibration_payload(
        self,
        run_id: str,
        belief_update_report: dict[str, Any],
        belief_calibration_report: dict[str, Any] | None = None,
    ) -> int:
        report = belief_calibration_report if isinstance(belief_calibration_report, dict) else {}
        rows = list(report.get("predictions") or [])
        if not rows:
            rows = step2_belief_calibration.prediction_rows_from_beliefs(
                belief_update_report,
                run_id=run_id,
                validation_rows=self.validation_rows(limit=5000),
                feedback_rows=self.feedback_rows(limit=5000),
                command_outcomes=self._command_outcome_rows(limit=5000),
            )
        inserted = 0
        now = _now_epoch()
        for row in rows:
            belief_id = str(row.get("belief_id") or _stable_hash({
                "run_id": run_id,
                "belief": row.get("belief"),
                "subject": row.get("subject"),
                "prediction_type": row.get("prediction_type"),
            }, length=32))
            status = str(row.get("status") or "pending")
            self.conn.execute(
                """
                INSERT OR REPLACE INTO belief_calibration_memory(
                    belief_id, run_id, belief_type, subject, predicted_probability, actual,
                    actual_numeric, calibration_error, brier_score, status, resolution_source,
                    updated_at, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    belief_id,
                    run_id,
                    row.get("belief_type"),
                    row.get("subject"),
                    float(row.get("predicted_probability") if row.get("predicted_probability") is not None else 0.5),
                    row.get("actual"),
                    row.get("actual_numeric"),
                    row.get("calibration_error"),
                    row.get("brier_score"),
                    status,
                    row.get("resolution_source"),
                    now,
                    json.dumps(row, sort_keys=True, separators=(",", ":"), default=str),
                ),
            )
            inserted += 1
        return inserted

    def _ingest_learning_control_plane_payload(self, run_id: str, payload: dict[str, Any]) -> int:
        if not isinstance(payload, dict) or not payload:
            return 0
        rows = []
        for row in ((payload.get("outcome_attribution_report") or {}).get("route_attribution") or []):
            rows.append(("outcome_attribution", row.get("route_key"), row.get("attribution_score"), row.get("state"), row))
        for row in ((payload.get("counterfactual_learning_report") or {}).get("counterfactuals") or []):
            rows.append(("counterfactual_learning", row.get("subject"), row.get("expected_learning_value"), row.get("source"), row))
        for row in ((payload.get("regime_conditioned_learning_report") or {}).get("regimes") or []):
            rows.append(("regime_conditioned_learning", row.get("regime_key"), row.get("regime_score"), row.get("regime_state"), row))
        for row in ((payload.get("active_experiment_design_report") or {}).get("experiments") or []):
            rows.append(("active_experiment_design", row.get("subject"), row.get("expected_value_of_information"), row.get("success_gate"), row))
        governance = payload.get("learning_governance_report") if isinstance(payload.get("learning_governance_report"), dict) else {}
        if governance:
            rows.append(("learning_governance", "governance", governance.get("max_learning_influence"), governance.get("decision"), governance))
        inserted = 0
        now = _now_epoch()
        for artifact_type, subject, score, status, row in rows:
            control_id = _stable_hash({
                "run_id": run_id,
                "artifact_type": artifact_type,
                "subject": subject,
                "row": row,
            }, length=28)
            self.conn.execute(
                """
                INSERT OR REPLACE INTO learning_control_memory(
                    control_id, run_id, artifact_type, subject, score, status, updated_at, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    control_id,
                    run_id,
                    artifact_type,
                    str(subject or ""),
                    float(score or 0.0),
                    str(status or ""),
                    now,
                    json.dumps(row, sort_keys=True, separators=(",", ":"), default=str),
                ),
            )
            inserted += 1
        return inserted

    def _ingest_learning_depth_payload(self, run_id: str, payload: dict[str, Any]) -> int:
        if not isinstance(payload, dict) or not payload:
            return 0
        rows = []
        for row in ((payload.get("enhanced_target_label_report") or {}).get("top_labels") or []):
            rows.append(("enhanced_target_label", row.get("variant"), row.get("learning_weight"), row.get("target_label"), row))
        for row in ((payload.get("lesson_survival_decay_report") or {}).get("lessons") or []):
            rows.append(("lesson_survival_decay", row.get("subject"), row.get("survival_score"), row.get("decay_action"), row))
        for row in ((payload.get("portfolio_learning_report") or {}).get("top_portfolio_candidates") or []):
            rows.append(("portfolio_learning", row.get("variant"), row.get("portfolio_score"), row.get("role"), row))
        for row in ((payload.get("uncertainty_risk_pricing_report") or {}).get("priced_cells") or []):
            rows.append(("uncertainty_risk_pricing", row.get("subject"), row.get("uncertainty_price"), row.get("recommended_action"), row))
        for row in ((payload.get("promotion_evidence_hardening_report") or {}).get("candidates") or []):
            rows.append(("promotion_evidence_hardening", row.get("variant"), row.get("hardening_score"), row.get("promotion_evidence_status"), row))
        inserted = 0
        now = _now_epoch()
        for artifact_type, subject, score, status, row in rows:
            depth_id = _stable_hash({
                "run_id": run_id,
                "artifact_type": artifact_type,
                "subject": subject,
                "row": row,
            }, length=28)
            self.conn.execute(
                """
                INSERT OR REPLACE INTO learning_depth_memory(
                    depth_id, run_id, artifact_type, subject, score, status, updated_at, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    depth_id,
                    run_id,
                    artifact_type,
                    str(subject or ""),
                    float(score or 0.0),
                    str(status or ""),
                    now,
                    json.dumps(row, sort_keys=True, separators=(",", ":"), default=str),
                ),
            )
            inserted += 1
        return inserted

    def _ingest_ops_hardening_payload(self, run_id: str, payload: dict[str, Any]) -> int:
        if not isinstance(payload, dict) or not payload:
            return 0
        rows = []
        for row in ((payload.get("live_feedback_loop_report") or {}).get("feedback_queue") or []):
            rows.append(("live_feedback_loop", row.get("route_key") or row.get("variant") or row.get("action"), 1.0, row.get("action"), row))
        for row in ((payload.get("drift_monitoring_report") or {}).get("monitors") or []):
            rows.append(("drift_monitoring", row.get("subject") or row.get("monitor_id"), 100.0 if row.get("severity") == "critical" else 50.0, row.get("severity"), row))
        for row in ((payload.get("rollback_kill_switch_report") or {}).get("kill_switches") or []):
            rows.append(("rollback_kill_switch", row.get("name"), 100.0 if row.get("severity") == "critical" else 70.0, "armed" if row.get("armed") else "inactive", row))
        for row in ((payload.get("auditability_lineage_report") or {}).get("lineage_entries") or []):
            rows.append(("auditability_lineage", row.get("artifact"), 1.0 if row.get("present") else 0.0, "present" if row.get("present") else "missing", row))
        gate = payload.get("end_to_end_readiness_gate") if isinstance(payload.get("end_to_end_readiness_gate"), dict) else {}
        if gate:
            rows.append(("end_to_end_readiness_gate", "readiness", 1.0 if gate.get("ready") else 0.0, gate.get("readiness_state"), gate))
        for row in ((payload.get("elite_runbook_report") or {}).get("steps") or []):
            rows.append(("elite_runbook", row.get("step"), row.get("order"), row.get("owner"), row))
        inserted = 0
        now = _now_epoch()
        for artifact_type, subject, score, status, row in rows:
            ops_id = _stable_hash({
                "run_id": run_id,
                "artifact_type": artifact_type,
                "subject": subject,
                "row": row,
            }, length=28)
            self.conn.execute(
                """
                INSERT OR REPLACE INTO ops_hardening_memory(
                    ops_id, run_id, artifact_type, subject, score, status, updated_at, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ops_id,
                    run_id,
                    artifact_type,
                    str(subject or ""),
                    float(score or 0.0),
                    str(status or ""),
                    now,
                    json.dumps(row, sort_keys=True, separators=(",", ":"), default=str),
                ),
            )
            inserted += 1
        return inserted

    def ingest_feedback_file(self, path: str | os.PathLike[str]) -> dict[str, Any]:
        payload = _read_json(path, {}) or {}
        void_report = step2_void_registry.contamination_report(payload, source_path=path)
        if void_report["voided_source_days"]:
            return {
                "inserted": 0,
                "path": str(Path(path).resolve()),
                "skipped": True,
                "skip_reason": "voided_source_day",
                "voided_source_days": void_report["voided_source_days"],
            }
        rows = payload.get("feedback") or payload.get("rows") or payload.get("queue") or []
        inserted = 0
        for row in rows:
            fid = _stable_hash({
                "variant": row.get("variant"),
                "behavior_key": row.get("behavior_key"),
                "route_key": row.get("route_key"),
                "status": row.get("status") or row.get("decision"),
                "reject_reasons": row.get("reject_reasons") or [],
                "weights": row.get("weights") or {},
                "bias": row.get("bias") or 0.0,
                "routes": row.get("routes") or [],
            })
            self.conn.execute(
                """
                INSERT OR REPLACE INTO promotion_feedback(
                    feedback_id, variant, behavior_key, route_key, status, reject_reasons_json,
                    source_path, created_at, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fid,
                    row.get("variant"),
                    row.get("behavior_key"),
                    row.get("route_key"),
                    row.get("status") or row.get("decision"),
                    json.dumps(row.get("reject_reasons") or [], sort_keys=True, separators=(",", ":"), default=str),
                    str(Path(path).resolve()),
                    _now_epoch(),
                    json.dumps(row, sort_keys=True, separators=(",", ":"), default=str),
                ),
            )
            inserted += 1
        self.conn.commit()
        return {"inserted": inserted, "path": str(Path(path).resolve())}

    def _ingest_validation_payload(
        self,
        run_id: str,
        payload: dict[str, Any],
        source_path: str | os.PathLike[str],
    ) -> int:
        if not isinstance(payload, dict) or not payload:
            return 0
        kind = str(payload.get("kind") or "validation")
        rows = list(payload.get("results") or payload.get("validations") or [])
        if not rows:
            return 0
        inserted = 0
        source = str(Path(source_path).resolve())
        for row in rows:
            if kind == "counterfactual":
                metric_name = "route_disabled_delta"
                metric_value = float(row.get("route_disabled_delta") or 0.0)
                passed = bool(row.get("causal_route_signal"))
            elif kind == "adversarial":
                metric_name = "worst_delta_vs_baseline"
                metric_value = float(row.get("worst_delta_vs_baseline") or 0.0)
                passed = bool(row.get("stability_pass"))
            elif payload.get("source") == "step2_profit_combo_hunter" and "checks" in row:
                checks = row.get("checks") if isinstance(row.get("checks"), dict) else {}
                metric_name = "walk_forward_check_pass_rate"
                if checks:
                    metric_value = sum(1 for ok in checks.values() if ok) / max(1, len(checks))
                else:
                    metric_value = 1.0 if row.get("passed") else 0.0
                passed = bool(row.get("passed"))
                kind = "walk_forward"
            else:
                metric_name = "metric"
                metric_value = 0.0
                passed = False
            route_key = row.get("route_key") or hunt_intel.route_key_from_row(row)
            result_id = _stable_hash({
                "kind": kind,
                "variant": row.get("variant"),
                "route_key": route_key,
                "metric_name": metric_name,
                "source": source,
            })
            self.conn.execute(
                """
                INSERT OR REPLACE INTO validation_results(
                    result_id, kind, variant, route_key, baseline_step2_pnl, metric_name,
                    metric_value, passed, source_path, created_at, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    result_id,
                    kind,
                    row.get("variant"),
                    route_key,
                    float(row.get("baseline_step2_pnl") or row.get("pnl") or 0.0),
                    metric_name,
                    metric_value,
                    1 if passed else 0,
                    source,
                    _now_epoch(),
                    json.dumps(row, sort_keys=True, separators=(",", ":"), default=str),
                ),
            )
            inserted += 1
        return inserted

    def ingest_validation_results(self, path: str | os.PathLike[str]) -> dict[str, Any]:
        payload = _read_json(path, {}) or {}
        void_report = step2_void_registry.contamination_report(payload, source_path=path)
        if void_report["voided_source_days"]:
            return {
                "inserted": 0,
                "kind": str(payload.get("kind") or "validation"),
                "path": str(Path(path).resolve()),
                "skipped": True,
                "skip_reason": "voided_source_day",
                "voided_source_days": void_report["voided_source_days"],
            }
        inserted = self._ingest_validation_payload("", payload, path)
        self.conn.commit()
        return {
            "inserted": inserted,
            "kind": str(payload.get("kind") or "validation"),
            "path": str(Path(path).resolve()),
        }

    def record_cycle(self, run_id: str, cycle: dict[str, Any]) -> dict[str, Any]:
        idx = int(cycle.get("cycle") or 0)
        self.conn.execute(
            """
            INSERT OR REPLACE INTO cycles(run_id, cycle, hunter, seed, ok, elapsed_sec, scored_total, winners, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(run_id),
                idx,
                cycle.get("hunter"),
                int(cycle.get("seed") or 0),
                1 if cycle.get("ok") else 0,
                float(cycle.get("elapsed_sec") or 0.0),
                hunt_intel._cycle_scored_total(cycle),
                hunt_intel._cycle_winners(cycle),
                json.dumps(_compact_cycle_payload(cycle), sort_keys=True, separators=(",", ":"), default=str),
            ),
        )
        self.conn.commit()
        return {"run_id": str(run_id), "cycle": idx, "recorded": True}

    def record_online_event(self, run_id: str, event: dict[str, Any]) -> dict[str, Any]:
        event_id = str(event.get("event_id") or _stable_hash({
            "run_id": run_id,
            "event_type": event.get("event_type"),
            "route_key": event.get("route_key"),
            "batch": event.get("batch"),
            "cycle": event.get("cycle"),
            "payload_hash": _stable_hash({k: v for k, v in event.items() if k != "ts"}, length=20),
        }))
        route = str(event.get("route_key") or "")
        if not route and isinstance(event.get("allocation"), list) and event["allocation"]:
            route = str((event["allocation"][0] or {}).get("route_key") or "")
        self.conn.execute(
            """
            INSERT OR REPLACE INTO online_events(event_id, run_id, event_type, route_key, created_at, raw_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                str(run_id),
                event.get("event_type"),
                route,
                float(event.get("ts") or _now_epoch()),
                json.dumps(event, sort_keys=True, separators=(",", ":"), default=str),
            ),
        )
        self.conn.commit()
        return {"event_id": event_id, "recorded": True}

    def feedback_rows(self, limit: int = 1000) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT raw_json FROM promotion_feedback ORDER BY created_at DESC LIMIT ?",
            (int(limit),),
        ).fetchall()
        return [json.loads(row["raw_json"]) for row in rows]

    def _command_outcome_rows(self, limit: int = 1000) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT raw_json FROM command_outcome_memory ORDER BY updated_at DESC LIMIT ?",
            (int(limit),),
        ).fetchall()
        return [json.loads(row["raw_json"]) for row in rows]

    def cross_run_route_memory(self, limit: int = 50) -> dict[str, Any]:
        rows = self.conn.execute(
            """
            SELECT
              c.route_key AS route_key,
              COUNT(DISTINCT c.run_id) AS runs,
              COUNT(*) AS candidates,
              AVG(c.step2_pnl) AS avg_pnl,
              MAX(c.step2_pnl) AS best_pnl,
              AVG(c.promotion_quality_score) AS avg_quality,
              AVG(c.overfit_risk_score) AS avg_overfit,
              COALESCE(SUM(CASE WHEN v.passed=1 THEN 1 ELSE 0 END), 0) AS validation_passes,
              COALESCE(COUNT(v.result_id), 0) AS validation_results
            FROM candidates c
            LEFT JOIN validation_results v ON v.route_key = c.route_key
            GROUP BY c.route_key
            ORDER BY best_pnl DESC, candidates DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        return {
            "schema_version": 1,
            "source": "step2_learning_db",
            "routes": [dict(row) for row in rows],
        }

    def route_prior_model(self, limit: int = 50) -> dict[str, Any]:
        memory = self.cross_run_route_memory(limit=limit).get("routes") or []
        feedback_rows = self.conn.execute(
            """
            SELECT route_key, status FROM promotion_feedback
            """
        ).fetchall()
        rejects: dict[str, int] = {}
        totals: dict[str, int] = {}
        for row in feedback_rows:
            route = str(row["route_key"] or "unknown")
            status = str(row["status"] or "").lower()
            totals[route] = totals.get(route, 0) + 1
            if status in {"failed", "reject", "rejected", "blocked"}:
                rejects[route] = rejects.get(route, 0) + 1
        priors = []
        for row in memory:
            route = str(row.get("route_key") or "unknown")
            validations = int(row.get("validation_results") or 0)
            passes = int(row.get("validation_passes") or 0)
            pass_rate = passes / max(1, validations)
            reject_rate = rejects.get(route, 0) / max(1, totals.get(route, 0))
            expected_delta = float(row.get("best_pnl") or 0.0) - float(row.get("avg_pnl") or 0.0)
            overfit = float(row.get("avg_overfit") or 0.0)
            confidence = min(1.0, (int(row.get("runs") or 0) + validations) / 10.0)
            prior_score = (
                float(row.get("avg_quality") or 0.0)
                + pass_rate * 1200.0
                - reject_rate * 1500.0
                - overfit * 20.0
                + confidence * 300.0
            )
            priors.append({
                "route_key": route,
                "expected_live_delta_proxy": round(expected_delta, 4),
                "validation_pass_rate": round(pass_rate, 4),
                "reject_rate": round(reject_rate, 4),
                "avg_overfit": round(overfit, 4),
                "confidence": round(confidence, 4),
                "prior_score": round(prior_score, 4),
                "mutation_radius": (
                    "narrow" if pass_rate >= 0.65 and overfit < 45.0
                    else "widen" if pass_rate < 0.30 and validations >= 3
                    else "balanced"
                ),
            })
        priors.sort(key=lambda row: row["prior_score"], reverse=True)
        return {
            "schema_version": 1,
            "source": "step2_learning_db",
            "priors": priors[:limit],
        }

    def causal_experiment_registry(self, limit: int = 200) -> dict[str, Any]:
        rows = self.conn.execute(
            """
            SELECT run_id, experiment_id, kind, route_key, family_key, hypothesis, status, verdict, raw_json
            FROM causal_experiments
            ORDER BY run_id DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        experiments = []
        for row in rows:
            payload = json.loads(row["raw_json"])
            payload.setdefault("run_id", row["run_id"])
            payload.setdefault("experiment_id", row["experiment_id"])
            payload.setdefault("verdict", row["verdict"])
            experiments.append(payload)
        verdict_counts: dict[str, int] = {}
        for exp in experiments:
            verdict = str(exp.get("verdict") or "unknown")
            verdict_counts[verdict] = verdict_counts.get(verdict, 0) + 1
        return {
            "schema_version": 1,
            "source": "step2_learning_db",
            "experiments": experiments,
            "verdict_counts": verdict_counts,
        }

    def experiment_debt_queue(self, limit: int = 100) -> dict[str, Any]:
        rows = self.conn.execute(
            """
            SELECT debt_id, run_id, question, route_key, family_key, priority_score, status, raw_json
            FROM experiment_debt
            WHERE COALESCE(status, 'open') != 'closed'
            ORDER BY priority_score DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        queue = []
        for row in rows:
            payload = json.loads(row["raw_json"])
            payload.setdefault("debt_id", row["debt_id"])
            payload.setdefault("run_id", row["run_id"])
            payload.setdefault("status", row["status"] or "open")
            queue.append(payload)
        return {
            "schema_version": 1,
            "source": "step2_learning_db",
            "queue": queue,
        }

    def meta_strategy_learner(self, limit: int = 50) -> dict[str, Any]:
        rows = self.conn.execute(
            """
            SELECT hunter, COUNT(*) AS cycles, SUM(scored_total) AS scored, SUM(winners) AS winners
            FROM cycles
            GROUP BY hunter
            ORDER BY winners DESC, cycles DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        strategies = []
        for row in rows:
            scored = float(row["scored"] or 0.0)
            winners = float(row["winners"] or 0.0)
            yield_per_10k = winners / max(1.0, scored) * 10000.0
            strategies.append({
                "strategy": f"{row['hunter'] or 'unknown'}_heavy",
                "cycles": int(row["cycles"] or 0),
                "winner_yield_per_10k": round(yield_per_10k, 4),
                "recommended_budget_pct": round(min(60.0, 15.0 + yield_per_10k), 2),
            })
        return {
            "schema_version": 1,
            "source": "step2_learning_db",
            "strategies": strategies,
        }

    def temporal_decay_memory(self, limit: int = 100) -> dict[str, Any]:
        route_rows = self.conn.execute(
            """
            SELECT route_key, last_seen_run_id, seen_count, decayed_confidence, decayed_score, last_seen_at, raw_json
            FROM temporal_route_memory
            ORDER BY decayed_score DESC, decayed_confidence DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        policy_rows = self.conn.execute(
            """
            SELECT policy_name, last_seen_run_id, champion_count, seen_count, last_score, decayed_score, last_seen_at, raw_json
            FROM temporal_policy_memory
            ORDER BY decayed_score DESC, champion_count DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        return {
            "schema_version": 1,
            "source": "step2_learning_db",
            "description": "Durable temporal memory with old route and policy evidence decayed over ingest events.",
            "routes": [
                {
                    "route_key": row["route_key"],
                    "last_seen_run_id": row["last_seen_run_id"],
                    "seen_count": int(row["seen_count"] or 0),
                    "decayed_confidence": round(float(row["decayed_confidence"] or 0.0), 4),
                    "decayed_score": round(float(row["decayed_score"] or 0.0), 4),
                    "last_seen_at": row["last_seen_at"],
                    "raw": json.loads(row["raw_json"]),
                }
                for row in route_rows
            ],
            "policies": [
                {
                    "policy_name": row["policy_name"],
                    "last_seen_run_id": row["last_seen_run_id"],
                    "champion_count": int(row["champion_count"] or 0),
                    "seen_count": int(row["seen_count"] or 0),
                    "last_score": round(float(row["last_score"] or 0.0), 4),
                    "decayed_score": round(float(row["decayed_score"] or 0.0), 4),
                    "last_seen_at": row["last_seen_at"],
                    "raw": json.loads(row["raw_json"]),
                }
                for row in policy_rows
            ],
        }

    def revalidation_candidates(self, limit: int = 50) -> dict[str, Any]:
        routes = self.conn.execute(
            """
            SELECT route_key, last_seen_run_id, seen_count, decayed_confidence, decayed_score, last_seen_at, raw_json
            FROM temporal_route_memory
            WHERE decayed_confidence < 0.55 OR decayed_score <= 0
            ORDER BY decayed_confidence ASC, decayed_score ASC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        policies = self.conn.execute(
            """
            SELECT policy_name, last_seen_run_id, champion_count, seen_count, last_score, decayed_score, last_seen_at, raw_json
            FROM temporal_policy_memory
            WHERE seen_count >= 1
            ORDER BY champion_count ASC, decayed_score ASC
            LIMIT ?
            """,
            (max(1, int(limit) // 2),),
        ).fetchall()
        queue = []
        for row in routes:
            queue.append({
                "task": "revalidate_temporal_route",
                "route_key": row["route_key"],
                "last_seen_run_id": row["last_seen_run_id"],
                "priority_score": round(80.0 - min(50.0, float(row["decayed_confidence"] or 0.0) * 50.0), 4),
                "reason": "decayed confidence is low or decayed score is non-positive",
                "decayed_confidence": round(float(row["decayed_confidence"] or 0.0), 4),
                "decayed_score": round(float(row["decayed_score"] or 0.0), 4),
            })
        for row in policies:
            queue.append({
                "task": "revalidate_temporal_policy",
                "policy_name": row["policy_name"],
                "last_seen_run_id": row["last_seen_run_id"],
                "priority_score": round(55.0 + max(0.0, 5.0 - float(row["champion_count"] or 0.0)) * 4.0, 4),
                "reason": "policy has weak champion persistence or stale tournament evidence",
                "champion_count": int(row["champion_count"] or 0),
                "decayed_score": round(float(row["decayed_score"] or 0.0), 4),
            })
        queue.sort(key=lambda row: float(row.get("priority_score") or 0.0), reverse=True)
        return {
            "schema_version": 1,
            "source": "step2_learning_db",
            "queue": queue[:limit],
        }

    def research_lab_memory(self, limit: int = 100) -> dict[str, Any]:
        route_rows = self.conn.execute(
            """
            SELECT route_key, last_seen_run_id, state, next_action, confidence, updated_at, raw_json
            FROM route_lifecycle_memory
            ORDER BY confidence DESC, updated_at DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        negative_rows = self.conn.execute(
            """
            SELECT pattern_key, route_key, reason, severity, source_run_id, seen_count, updated_at, raw_json
            FROM negative_knowledge
            ORDER BY seen_count DESC, updated_at DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        grammar_rows = self.conn.execute(
            """
            SELECT grammar_id, route_key, treatment, mutation_radius, learning_score, source_run_id, updated_at, raw_json
            FROM mutation_grammar_memory
            ORDER BY learning_score DESC, updated_at DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        return {
            "schema_version": 1,
            "source": "step2_learning_db",
            "description": "Durable autonomous research-lab memory: route lifecycle, negative knowledge, and mutation grammar.",
            "route_lifecycle": [
                {
                    "route_key": row["route_key"],
                    "state": row["state"],
                    "next_action": row["next_action"],
                    "confidence": round(float(row["confidence"] or 0.0), 4),
                    "last_seen_run_id": row["last_seen_run_id"],
                    "raw": json.loads(row["raw_json"]),
                }
                for row in route_rows
            ],
            "negative_knowledge": [
                {
                    "pattern_key": row["pattern_key"],
                    "route_key": row["route_key"],
                    "reason": row["reason"],
                    "severity": row["severity"],
                    "seen_count": int(row["seen_count"] or 0),
                    "source_run_id": row["source_run_id"],
                    "raw": json.loads(row["raw_json"]),
                }
                for row in negative_rows
            ],
            "mutation_grammar": [
                {
                    "grammar_id": row["grammar_id"],
                    "route_key": row["route_key"],
                    "treatment": row["treatment"],
                    "mutation_radius": row["mutation_radius"],
                    "learning_score": round(float(row["learning_score"] or 0.0), 4),
                    "source_run_id": row["source_run_id"],
                    "raw": json.loads(row["raw_json"]),
                }
                for row in grammar_rows
            ],
        }

    def self_correction_memory(self, limit: int = 100) -> dict[str, Any]:
        league_rows = self.conn.execute(
            """
            SELECT participant_key, participant_type, name, rating, wins, losses, draws, last_seen_run_id, raw_json
            FROM policy_league_memory
            ORDER BY rating DESC, wins DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        correction_rows = self.conn.execute(
            """
            SELECT memory_key, kind, route_key, policy_name, risk_score, status, source_run_id, raw_json
            FROM self_correction_memory
            ORDER BY risk_score DESC, rowid DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        return {
            "schema_version": 1,
            "source": "step2_learning_db",
            "description": "Durable self-correction memory for policy league standings, false lessons, uncertainty, Pareto, and causal attributions.",
            "policy_league": [
                {
                    "participant_key": row["participant_key"],
                    "participant_type": row["participant_type"],
                    "name": row["name"],
                    "rating": round(float(row["rating"] or 0.0), 4),
                    "wins": int(row["wins"] or 0),
                    "losses": int(row["losses"] or 0),
                    "draws": int(row["draws"] or 0),
                    "last_seen_run_id": row["last_seen_run_id"],
                    "raw": json.loads(row["raw_json"]),
                }
                for row in league_rows
            ],
            "self_corrections": [
                {
                    "memory_key": row["memory_key"],
                    "kind": row["kind"],
                    "route_key": row["route_key"],
                    "policy_name": row["policy_name"],
                    "risk_score": round(float(row["risk_score"] or 0.0), 4),
                    "status": row["status"],
                    "source_run_id": row["source_run_id"],
                    "raw": json.loads(row["raw_json"]),
                }
                for row in correction_rows
            ],
        }

    def pressure_science_memory(self, limit: int = 100) -> dict[str, Any]:
        contracts = self.conn.execute(
            """
            SELECT contract_id, belief_type, belief, status, scope_json, source_run_id, raw_json
            FROM evidence_contract_memory
            ORDER BY rowid DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        traces = self.conn.execute(
            """
            SELECT trace_id, kind, subject, decision, confidence, source_run_id, raw_json
            FROM research_trace_memory
            ORDER BY confidence DESC, rowid DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        return {
            "schema_version": 1,
            "source": "step2_learning_db",
            "description": "Durable pressure-science memory for evidence contracts and research trace provenance.",
            "evidence_contracts": [
                {
                    "contract_id": row["contract_id"],
                    "belief_type": row["belief_type"],
                    "belief": row["belief"],
                    "status": row["status"],
                    "scope": json.loads(row["scope_json"] or "{}"),
                    "source_run_id": row["source_run_id"],
                    "raw": json.loads(row["raw_json"]),
                }
                for row in contracts
            ],
            "research_traces": [
                {
                    "trace_id": row["trace_id"],
                    "kind": row["kind"],
                    "subject": row["subject"],
                    "decision": row["decision"],
                    "confidence": round(float(row["confidence"] or 0.0), 4),
                    "source_run_id": row["source_run_id"],
                    "raw": json.loads(row["raw_json"]),
                }
                for row in traces
            ],
        }

    def closed_loop_execution_memory(self, limit: int = 100) -> dict[str, Any]:
        commands = self.conn.execute(
            """
            SELECT command_id, run_id, action, route_key, reward_score, worked, raw_json
            FROM command_replay_memory
            ORDER BY rowid DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        actions = self.conn.execute(
            """
            SELECT action, rating, sample_count, success_rate, last_seen_run_id, raw_json
            FROM action_league_memory
            ORDER BY rating DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        outcomes = self.conn.execute(
            """
            SELECT command_id, run_id, action, route_key, reward_score, worked, raw_json
            FROM command_outcome_memory
            ORDER BY rowid DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        calibrations = self.conn.execute(
            """
            SELECT action, predicted_reward, observed_reward, abs_error, sample_count, raw_json
            FROM action_reward_calibration_memory
            ORDER BY abs_error DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        runtime_experiments = self.conn.execute(
            """
            SELECT artifact_name, subject, priority, run_id, raw_json
            FROM runtime_experiment_memory
            ORDER BY priority DESC, rowid DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        return {
            "schema_version": 1,
            "source": "step2_learning_db",
            "description": "Durable closed-loop execution memory for issued commands, outcomes, and action ELO ratings.",
            "commands": [
                {
                    "command_id": row["command_id"],
                    "run_id": row["run_id"],
                    "action": row["action"],
                    "route_key": row["route_key"],
                    "reward_score": round(float(row["reward_score"] or 0.0), 4),
                    "worked": bool(row["worked"]),
                    "raw": json.loads(row["raw_json"]),
                }
                for row in commands
            ],
            "action_league": [
                {
                    "action": row["action"],
                    "rating": round(float(row["rating"] or 0.0), 4),
                    "sample_count": int(row["sample_count"] or 0),
                    "success_rate": round(float(row["success_rate"] or 0.0), 4),
                    "last_seen_run_id": row["last_seen_run_id"],
                    "raw": json.loads(row["raw_json"]),
                }
                for row in actions
            ],
            "command_outcomes": [
                {
                    "command_id": row["command_id"],
                    "run_id": row["run_id"],
                    "action": row["action"],
                    "route_key": row["route_key"],
                    "reward_score": round(float(row["reward_score"] or 0.0), 4),
                    "worked": bool(row["worked"]),
                    "raw": json.loads(row["raw_json"]),
                }
                for row in outcomes
            ],
            "reward_calibration": [
                {
                    "action": row["action"],
                    "predicted_reward": round(float(row["predicted_reward"] or 0.0), 4),
                    "observed_reward": round(float(row["observed_reward"] or 0.0), 4),
                    "abs_error": round(float(row["abs_error"] or 0.0), 4),
                    "sample_count": int(row["sample_count"] or 0),
                    "raw": json.loads(row["raw_json"]),
                }
                for row in calibrations
            ],
            "runtime_experiments": [
                {
                    "artifact_name": row["artifact_name"],
                    "subject": row["subject"],
                    "priority": round(float(row["priority"] or 0.0), 4),
                    "run_id": row["run_id"],
                    "raw": json.loads(row["raw_json"]),
                }
                for row in runtime_experiments
            ],
        }

    def global_learning_memory(self, limit: int = 50) -> dict[str, Any]:
        route_priors = self.route_prior_model(limit=limit)
        experiments = self.causal_experiment_registry(limit=limit)
        debt = self.experiment_debt_queue(limit=limit)
        strategy = self.meta_strategy_learner(limit=limit)
        feedback = self.feedback_rows(limit=limit)
        failure_memory = hunt_intel.failure_memory_feedback(feedback)
        closed_loop_memory = self.closed_loop_execution_memory(limit=limit)
        controller = self.closed_loop_controller(limit=limit)
        return {
            "schema_version": 1,
            "source": "step2_learning_db",
            "description": "Durable cross-run memory: routes, experiments, debt, reject patterns, and hunt strategy.",
            "route_prior_model": route_priors,
            "causal_experiment_registry": experiments,
            "experiment_debt_queue": debt,
            "meta_strategy_learner": strategy,
            "failure_memory_feedback": failure_memory,
            "next_experiment_packet": self.next_experiment_packet(limit=limit),
            "temporal_decay_memory": self.temporal_decay_memory(limit=limit),
            "revalidation_candidates": self.revalidation_candidates(limit=limit),
            "research_lab_memory": self.research_lab_memory(limit=limit),
            "self_correction_memory": self.self_correction_memory(limit=limit),
            "pressure_science_memory": self.pressure_science_memory(limit=limit),
            "closed_loop_execution_memory": closed_loop_memory,
            "closed_loop_controller": controller,
            "belief_calibration_persistence": self.belief_calibration_persistence(limit=limit),
            "learning_control_plane_memory": self.learning_control_plane_memory(limit=limit),
            "learning_depth_memory": self.learning_depth_memory(limit=limit),
            "ops_hardening_memory": self.ops_hardening_memory(limit=limit),
        }

    def next_run_opening_controls(self, limit: int = 50) -> dict[str, Any]:
        import step2_online_learning

        memory = self.global_learning_memory(limit=limit)
        controls = step2_online_learning.cross_run_opening_controls(memory)
        return {
            "schema_version": 1,
            "source": "step2_learning_db",
            "description": "Next-run opening controls compiled from durable cross-run learning memory.",
            **controls,
        }

    def calibration_persistence(self, limit: int = 200) -> dict[str, Any]:
        rows = self.conn.execute(
            """
            SELECT prediction_type, actual, calibration_error, status, raw_json
            FROM calibration_predictions
            ORDER BY rowid DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        items = [json.loads(row["raw_json"]) for row in rows]
        resolved = [row for row in rows if row["status"] == "resolved"]
        avg_error = sum(float(row["calibration_error"] or 0.0) for row in resolved) / max(1, len(resolved))
        by_type: dict[str, dict[str, Any]] = {}
        for row in rows:
            typ = str(row["prediction_type"] or "unknown")
            item = by_type.setdefault(typ, {"count": 0, "resolved": 0, "error_sum": 0.0})
            item["count"] += 1
            if row["status"] == "resolved":
                item["resolved"] += 1
                item["error_sum"] += float(row["calibration_error"] or 0.0)
        summaries = []
        for typ, item in by_type.items():
            summaries.append({
                "prediction_type": typ,
                "count": item["count"],
                "resolved": item["resolved"],
                "avg_error": round(float(item["error_sum"]) / max(1, int(item["resolved"])), 4),
            })
        return {
            "schema_version": 1,
            "source": "step2_learning_db",
            "resolved_predictions": len(resolved),
            "pending_predictions": sum(1 for row in rows if row["status"] == "pending"),
            "avg_calibration_error": round(avg_error, 4),
            "by_type": summaries,
            "recent": items,
        }

    def belief_calibration_persistence(self, limit: int = 200) -> dict[str, Any]:
        rows = self.conn.execute(
            """
            SELECT raw_json, status
            FROM belief_calibration_memory
            ORDER BY updated_at DESC, rowid DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        predictions = []
        for row in rows:
            payload = json.loads(row["raw_json"])
            payload.setdefault("status", row["status"])
            predictions.append(payload)
        report = step2_belief_calibration.calibration_report(
            predictions,
            source="step2_learning_db",
        )
        report["description"] = "Durable belief calibration memory: prior beliefs, resolved outcomes, reliability buckets, and belief-weight controls."
        return report

    def learning_control_plane_memory(self, limit: int = 200) -> dict[str, Any]:
        rows = self.conn.execute(
            """
            SELECT run_id, artifact_type, subject, score, status, raw_json
            FROM learning_control_memory
            ORDER BY score DESC, updated_at DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        by_type: dict[str, dict[str, Any]] = {}
        items = []
        for row in rows:
            artifact_type = str(row["artifact_type"] or "unknown")
            summary = by_type.setdefault(artifact_type, {"artifact_type": artifact_type, "count": 0, "top_score": None})
            summary["count"] += 1
            score = float(row["score"] or 0.0)
            if summary["top_score"] is None or score > float(summary["top_score"]):
                summary["top_score"] = score
            items.append({
                "run_id": row["run_id"],
                "artifact_type": artifact_type,
                "subject": row["subject"],
                "score": round(score, 6),
                "status": row["status"],
                "raw": json.loads(row["raw_json"]),
            })
        top_by_type = []
        for artifact_type in sorted(by_type):
            typed = [item for item in items if item["artifact_type"] == artifact_type]
            top_by_type.append({
                **by_type[artifact_type],
                "top": typed[:10],
                "top_score": round(float(by_type[artifact_type]["top_score"] or 0.0), 6),
            })
        return {
            "schema_version": 1,
            "control_plane_version": step2_learning_control_plane.CONTROL_PLANE_VERSION,
            "source": "step2_learning_db",
            "description": "Durable memory for Tranches 9-13: attribution, counterfactuals, regimes, active experiments, and learning governance.",
            "item_count": len(items),
            "by_type": top_by_type,
            "top_items": items[:50],
        }

    def learning_depth_memory(self, limit: int = 200) -> dict[str, Any]:
        rows = self.conn.execute(
            """
            SELECT run_id, artifact_type, subject, score, status, raw_json
            FROM learning_depth_memory
            ORDER BY score DESC, updated_at DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        items = []
        by_type: dict[str, dict[str, Any]] = {}
        for row in rows:
            artifact_type = str(row["artifact_type"] or "unknown")
            score = float(row["score"] or 0.0)
            summary = by_type.setdefault(artifact_type, {"artifact_type": artifact_type, "count": 0, "top_score": 0.0})
            summary["count"] += 1
            summary["top_score"] = max(float(summary["top_score"] or 0.0), score)
            items.append({
                "run_id": row["run_id"],
                "artifact_type": artifact_type,
                "subject": row["subject"],
                "score": round(score, 6),
                "status": row["status"],
                "raw": json.loads(row["raw_json"]),
            })
        return {
            "schema_version": 1,
            "learning_depth_version": step2_learning_depth.LEARNING_DEPTH_VERSION,
            "source": "step2_learning_db",
            "description": "Durable memory for Tranches 14-18: labels, lesson survival, portfolio learning, risk pricing, and promotion hardening.",
            "item_count": len(items),
            "by_type": [
                {
                    **summary,
                    "top_score": round(float(summary["top_score"] or 0.0), 6),
                    "top": [item for item in items if item["artifact_type"] == artifact_type][:10],
                }
                for artifact_type, summary in sorted(by_type.items())
            ],
            "top_items": items[:50],
        }

    def ops_hardening_memory(self, limit: int = 200) -> dict[str, Any]:
        rows = self.conn.execute(
            """
            SELECT run_id, artifact_type, subject, score, status, raw_json
            FROM ops_hardening_memory
            ORDER BY score DESC, updated_at DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        items = []
        by_type: dict[str, dict[str, Any]] = {}
        for row in rows:
            artifact_type = str(row["artifact_type"] or "unknown")
            score = float(row["score"] or 0.0)
            summary = by_type.setdefault(artifact_type, {"artifact_type": artifact_type, "count": 0, "top_score": 0.0})
            summary["count"] += 1
            summary["top_score"] = max(float(summary["top_score"] or 0.0), score)
            items.append({
                "run_id": row["run_id"],
                "artifact_type": artifact_type,
                "subject": row["subject"],
                "score": round(score, 6),
                "status": row["status"],
                "raw": json.loads(row["raw_json"]),
            })
        return {
            "schema_version": 1,
            "ops_hardening_version": step2_ops_hardening.OPS_HARDENING_VERSION,
            "source": "step2_learning_db",
            "description": "Durable memory for Tranches 19-24: feedback loops, drift monitors, kill switches, audit lineage, readiness gates, and runbooks.",
            "item_count": len(items),
            "by_type": [
                {
                    **summary,
                    "top_score": round(float(summary["top_score"] or 0.0), 6),
                    "top": [item for item in items if item["artifact_type"] == artifact_type][:10],
                }
                for artifact_type, summary in sorted(by_type.items())
            ],
            "top_items": items[:50],
        }

    def outcome_reconciliation(self, limit: int = 200) -> dict[str, Any]:
        rows = self.conn.execute(
            """
            SELECT prediction_id, raw_json
            FROM calibration_predictions
            WHERE status='pending'
            ORDER BY rowid DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        feedback = self.feedback_rows(limit=5000)
        feedback_by_variant: dict[str, list[dict[str, Any]]] = {}
        for row in feedback:
            if row.get("variant"):
                feedback_by_variant.setdefault(str(row.get("variant")), []).append(row)
        reconciled = []
        for dbrow in rows:
            payload = json.loads(dbrow["raw_json"])
            variant = str(payload.get("variant") or "")
            if not variant or variant not in feedback_by_variant:
                continue
            frows = feedback_by_variant[variant]
            rejected = any(str(item.get("status") or item.get("decision") or "").lower() in {"reject", "rejected", "blocked", "failed"} for item in frows)
            actual = "rejected" if rejected else "approved"
            predicted = float(payload.get("predicted_probability") or 0.0)
            actual_numeric = 0.0 if rejected else 1.0
            error = abs(predicted - actual_numeric)
            payload["actual"] = actual
            payload["calibration_error"] = round(error, 4)
            self.conn.execute(
                """
                UPDATE calibration_predictions
                SET actual=?, calibration_error=?, status=?, raw_json=?
                WHERE prediction_id=?
                """,
                (actual, error, "resolved", json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str), dbrow["prediction_id"]),
            )
            reconciled.append(payload)
        belief_rows = self.conn.execute(
            """
            SELECT belief_id, raw_json
            FROM belief_calibration_memory
            WHERE status='pending'
            ORDER BY updated_at DESC, rowid DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        validation_rows = self.validation_rows(limit=5000)
        command_outcomes = self._command_outcome_rows(limit=5000)
        belief_reconciled = []
        for dbrow in belief_rows:
            payload = json.loads(dbrow["raw_json"])
            resolved = step2_belief_calibration.resolve_prediction_outcome(
                payload,
                validation_rows=validation_rows,
                feedback_rows=feedback,
                command_outcomes=command_outcomes,
            )
            if resolved.get("status") != "resolved":
                continue
            payload.update(resolved)
            self.conn.execute(
                """
                UPDATE belief_calibration_memory
                SET actual=?, actual_numeric=?, calibration_error=?, brier_score=?, status=?,
                    resolution_source=?, updated_at=?, raw_json=?
                WHERE belief_id=?
                """,
                (
                    payload.get("actual"),
                    payload.get("actual_numeric"),
                    payload.get("calibration_error"),
                    payload.get("brier_score"),
                    "resolved",
                    payload.get("resolution_source"),
                    _now_epoch(),
                    json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str),
                    dbrow["belief_id"],
                ),
            )
            belief_reconciled.append(payload)
        self.conn.commit()
        return {
            "schema_version": 1,
            "source": "step2_learning_db",
            "reconciled": len(reconciled),
            "belief_reconciled": len(belief_reconciled),
            "items": reconciled,
            "belief_items": belief_reconciled,
            "calibration": self.calibration_persistence(limit=limit),
            "belief_calibration": self.belief_calibration_persistence(limit=limit),
        }

    def adaptive_mutation_controller(self, limit: int = 20) -> dict[str, Any]:
        memory = self.cross_run_route_memory(limit=limit).get("routes") or []
        controls = []
        for row in memory:
            validations = int(row.get("validation_results") or 0)
            passes = int(row.get("validation_passes") or 0)
            pass_rate = passes / max(1, validations)
            avg_overfit = float(row.get("avg_overfit") or 0.0)
            candidates = int(row.get("candidates") or 0)
            if pass_rate >= 0.65 and avg_overfit < 45.0:
                radius = "narrow"
                scale = 0.45
            elif candidates >= 10 and pass_rate < 0.30:
                radius = "rotate_or_widen"
                scale = 1.25
            elif avg_overfit >= 65.0:
                radius = "widen_with_robustness_pressure"
                scale = 0.90
            else:
                radius = "balanced"
                scale = 0.75
            controls.append({
                "route_key": row.get("route_key"),
                "mutation_radius": radius,
                "recommended_scale": scale,
                "validation_pass_rate": round(pass_rate, 4),
                "avg_overfit": avg_overfit,
                "candidates": candidates,
            })
        return {
            "schema_version": 1,
            "source": "step2_learning_db",
            "controls": controls,
        }

    def drift_detection(self, limit: int = 50) -> dict[str, Any]:
        rows = self.conn.execute(
            """
            SELECT run_id, route_key, AVG(delta_vs_active) AS avg_delta, MAX(step2_pnl) AS best_pnl
            FROM candidates
            GROUP BY run_id, route_key
            ORDER BY run_id ASC
            """
        ).fetchall()
        by_route: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            by_route.setdefault(str(row["route_key"] or "unknown"), []).append(dict(row))
        drift_rows = []
        for route, items in by_route.items():
            if len(items) < 2:
                continue
            midpoint = len(items) // 2
            old = items[:midpoint]
            new = items[midpoint:]
            old_delta = sum(float(item.get("avg_delta") or 0.0) for item in old) / max(1, len(old))
            new_delta = sum(float(item.get("avg_delta") or 0.0) for item in new) / max(1, len(new))
            drift = new_delta - old_delta
            status = "stable"
            if drift < -500.0:
                status = "stale_or_degrading"
            elif drift > 500.0:
                status = "improving"
            drift_rows.append({
                "route_key": route,
                "old_avg_delta": round(old_delta, 4),
                "new_avg_delta": round(new_delta, 4),
                "drift_delta": round(drift, 4),
                "status": status,
                "runs_seen": len(items),
            })
        drift_rows.sort(key=lambda row: abs(float(row["drift_delta"])), reverse=True)
        return {
            "schema_version": 1,
            "source": "step2_learning_db",
            "routes": drift_rows[:limit],
        }

    def candidate_lineage_graph(self, limit: int = 200) -> dict[str, Any]:
        rows = self.conn.execute(
            """
            SELECT run_id, behavior_key, config_key, variant, route_key, source, step2_pnl
            FROM candidates
            ORDER BY step2_pnl DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        nodes = []
        edges = []
        for row in rows:
            d = dict(row)
            candidate_id = f"candidate:{d.get('behavior_key')}"
            route_id = f"route:{d.get('route_key')}"
            run_id = f"run:{d.get('run_id')}"
            nodes.extend([
                {"id": run_id, "type": "run", "label": d.get("run_id")},
                {"id": route_id, "type": "route", "label": d.get("route_key")},
                {"id": candidate_id, "type": "candidate", "label": d.get("variant"), "pnl": d.get("step2_pnl")},
            ])
            edges.extend([
                {"from": run_id, "to": candidate_id, "type": "produced"},
                {"from": route_id, "to": candidate_id, "type": "route"},
            ])
        unique_nodes = {node["id"]: node for node in nodes}
        return {
            "schema_version": 1,
            "source": "step2_learning_db",
            "nodes": list(unique_nodes.values()),
            "edges": edges,
        }

    def regime_aware_bandit(self, limit: int = 50) -> dict[str, Any]:
        rows = self.conn.execute(
            """
            SELECT raw_json FROM candidates
            ORDER BY step2_pnl DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        candidates = [json.loads(row["raw_json"]) for row in rows]
        by_session: dict[str, list[dict[str, Any]]] = {}
        for row in candidates:
            match = hunt_intel.route_match(row)
            session = match.get("session_phase") or "unknown"
            by_session.setdefault(session, []).append(row)
        allocations = {
            session: hunt_intel.bandit_allocation(items, limit=10)
            for session, items in by_session.items()
        }
        return {
            "schema_version": 1,
            "source": "step2_learning_db",
            "regimes": allocations,
        }

    def validation_rows(self, limit: int = 5000) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT kind, variant, route_key, metric_name, metric_value, passed, raw_json FROM validation_results ORDER BY created_at DESC LIMIT ?",
            (int(limit),),
        ).fetchall()
        out = []
        for row in rows:
            payload = json.loads(row["raw_json"])
            payload.setdefault("kind", row["kind"])
            payload.setdefault("variant", row["variant"])
            payload.setdefault("route_key", row["route_key"])
            payload.setdefault("passed", bool(row["passed"]))
            payload.setdefault(row["metric_name"], row["metric_value"])
            out.append(payload)
        return out

    def next_experiment_packet(self, limit: int = 20) -> dict[str, Any]:
        priors = self.route_prior_model(limit=limit).get("priors") or []
        memory = self.cross_run_route_memory(limit=limit).get("routes") or []
        mutation = self.adaptive_mutation_controller(limit=limit).get("controls") or []
        drift = self.drift_detection(limit=limit).get("routes") or []
        regime = self.regime_aware_bandit(limit=limit)
        abandon = [
            row for row in priors
            if float(row.get("reject_rate") or 0.0) >= 0.5 or (
                float(row.get("validation_pass_rate") or 0.0) < 0.25 and float(row.get("confidence") or 0.0) >= 0.4
            )
        ]
        hunt_next = [
            {
                "route_key": row.get("route_key"),
                "why": "high_route_prior_score",
                "prior_score": row.get("prior_score"),
                "mutation_radius": row.get("mutation_radius"),
            }
            for row in priors[:10]
            if row not in abandon
        ]
        validate_next = [
            {
                "route_key": row.get("route_key"),
                "why": "high_reward_low_validation_confidence",
                "confidence": row.get("confidence"),
                "validation_pass_rate": row.get("validation_pass_rate"),
            }
            for row in priors
            if float(row.get("confidence") or 0.0) < 0.5 and row not in abandon
        ][:10]
        return {
            "schema_version": 1,
            "source": "step2_learning_db",
            "hunt_next": hunt_next,
            "validate_next": validate_next,
            "abandon_or_downweight": abandon[:10],
            "belief_calibration": self.belief_calibration_persistence(limit=limit),
            "learning_control_plane": self.learning_control_plane_memory(limit=limit),
            "learning_depth": self.learning_depth_memory(limit=limit),
            "ops_hardening": self.ops_hardening_memory(limit=limit),
            "closed_loop_controller": self.closed_loop_controller(limit=limit),
            "mutation_controls": mutation,
            "route_memory": memory,
            "drift_detection": drift,
            "regime_aware_bandit": regime,
            "evidence": {
                "route_prior_model": priors,
                "selection_policy": [
                    "hunt high prior score unless validation/reject evidence says abandon",
                    "validate high reward routes with low confidence",
                    "downweight high reject-rate or repeated validation failures",
                ],
            },
        }

    def closed_loop_controller(self, limit: int = 25) -> dict[str, Any]:
        return step2_closed_loop_controller.controller_report(
            route_prior_model=self.route_prior_model(limit=limit),
            experiment_registry=self.causal_experiment_registry(limit=limit),
            experiment_debt=self.experiment_debt_queue(limit=limit),
            closed_loop_memory=self.closed_loop_execution_memory(limit=limit),
            validation_rows=self.validation_rows(limit=limit * 25),
            feedback_rows=self.feedback_rows(limit=limit * 10),
            batch_size=500,
            limit=limit,
        )

    def stats(self, run_id: str | None = None) -> dict[str, Any]:
        where = "WHERE run_id=?" if run_id else ""
        params = (run_id,) if run_id else ()
        runs = self.conn.execute("SELECT COUNT(*) AS n FROM runs").fetchone()
        cycles = self.conn.execute(f"SELECT COUNT(*) AS n FROM cycles {where}", params).fetchone()
        candidates = self.conn.execute(f"SELECT COUNT(*) AS n FROM candidates {where}", params).fetchone()
        routes = self.conn.execute(f"SELECT COUNT(*) AS n FROM route_arms {where}", params).fetchone()
        feedback = self.conn.execute("SELECT COUNT(*) AS n FROM promotion_feedback").fetchone()
        validation = self.conn.execute("SELECT COUNT(*) AS n FROM validation_results").fetchone()
        online = self.conn.execute("SELECT COUNT(*) AS n FROM online_events").fetchone()
        experiments = self.conn.execute(f"SELECT COUNT(*) AS n FROM causal_experiments {where}", params).fetchone()
        debt = self.conn.execute(f"SELECT COUNT(*) AS n FROM experiment_debt {where}", params).fetchone()
        calibration = self.conn.execute(f"SELECT COUNT(*) AS n FROM calibration_predictions {where}", params).fetchone()
        belief_calibration = self.conn.execute(f"SELECT COUNT(*) AS n FROM belief_calibration_memory {where}", params).fetchone()
        temporal_routes = self.conn.execute("SELECT COUNT(*) AS n FROM temporal_route_memory").fetchone()
        temporal_policies = self.conn.execute("SELECT COUNT(*) AS n FROM temporal_policy_memory").fetchone()
        route_lifecycle = self.conn.execute("SELECT COUNT(*) AS n FROM route_lifecycle_memory").fetchone()
        negative = self.conn.execute("SELECT COUNT(*) AS n FROM negative_knowledge").fetchone()
        grammar = self.conn.execute("SELECT COUNT(*) AS n FROM mutation_grammar_memory").fetchone()
        league = self.conn.execute("SELECT COUNT(*) AS n FROM policy_league_memory").fetchone()
        corrections = self.conn.execute("SELECT COUNT(*) AS n FROM self_correction_memory").fetchone()
        contracts = self.conn.execute("SELECT COUNT(*) AS n FROM evidence_contract_memory").fetchone()
        traces = self.conn.execute("SELECT COUNT(*) AS n FROM research_trace_memory").fetchone()
        commands = self.conn.execute("SELECT COUNT(*) AS n FROM command_replay_memory").fetchone()
        action_league = self.conn.execute("SELECT COUNT(*) AS n FROM action_league_memory").fetchone()
        command_outcomes = self.conn.execute("SELECT COUNT(*) AS n FROM command_outcome_memory").fetchone()
        reward_calibration = self.conn.execute("SELECT COUNT(*) AS n FROM action_reward_calibration_memory").fetchone()
        runtime_experiments = self.conn.execute("SELECT COUNT(*) AS n FROM runtime_experiment_memory").fetchone()
        learning_control = self.conn.execute(f"SELECT COUNT(*) AS n FROM learning_control_memory {where}", params).fetchone()
        learning_depth = self.conn.execute(f"SELECT COUNT(*) AS n FROM learning_depth_memory {where}", params).fetchone()
        ops_hardening = self.conn.execute(f"SELECT COUNT(*) AS n FROM ops_hardening_memory {where}", params).fetchone()
        data_quality = self.conn.execute(f"SELECT COUNT(*) AS n FROM run_data_quality {where}", params).fetchone()
        return {
            "db": str(self.path.resolve()),
            "run_id": run_id,
            "runs": int(runs["n"] if runs else 0),
            "cycles": int(cycles["n"] if cycles else 0),
            "candidates": int(candidates["n"] if candidates else 0),
            "route_arms": int(routes["n"] if routes else 0),
            "promotion_feedback": int(feedback["n"] if feedback else 0),
            "validation_results": int(validation["n"] if validation else 0),
            "online_events": int(online["n"] if online else 0),
            "causal_experiments": int(experiments["n"] if experiments else 0),
            "experiment_debt": int(debt["n"] if debt else 0),
            "calibration_predictions": int(calibration["n"] if calibration else 0),
            "belief_calibration_memory": int(belief_calibration["n"] if belief_calibration else 0),
            "temporal_route_memory": int(temporal_routes["n"] if temporal_routes else 0),
            "temporal_policy_memory": int(temporal_policies["n"] if temporal_policies else 0),
            "route_lifecycle_memory": int(route_lifecycle["n"] if route_lifecycle else 0),
            "negative_knowledge": int(negative["n"] if negative else 0),
            "mutation_grammar_memory": int(grammar["n"] if grammar else 0),
            "policy_league_memory": int(league["n"] if league else 0),
            "self_correction_memory": int(corrections["n"] if corrections else 0),
            "evidence_contract_memory": int(contracts["n"] if contracts else 0),
            "research_trace_memory": int(traces["n"] if traces else 0),
            "command_replay_memory": int(commands["n"] if commands else 0),
            "action_league_memory": int(action_league["n"] if action_league else 0),
            "command_outcome_memory": int(command_outcomes["n"] if command_outcomes else 0),
            "action_reward_calibration_memory": int(reward_calibration["n"] if reward_calibration else 0),
            "runtime_experiment_memory": int(runtime_experiments["n"] if runtime_experiments else 0),
            "learning_control_memory": int(learning_control["n"] if learning_control else 0),
            "learning_depth_memory": int(learning_depth["n"] if learning_depth else 0),
            "ops_hardening_memory": int(ops_hardening["n"] if ops_hardening else 0),
            "run_data_quality": int(data_quality["n"] if data_quality else 0),
        }


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Ingest and inspect Step 2 hunt learning artifacts.")
    ap.add_argument("--db", default=str(DEFAULT_DB))
    sub = ap.add_subparsers(dest="command")
    ingest = sub.add_parser("ingest-run")
    ingest.add_argument("run_dir")
    feedback = sub.add_parser("ingest-feedback")
    feedback.add_argument("path")
    validation = sub.add_parser("ingest-validation")
    validation.add_argument("path")
    sub.add_parser("route-memory")
    sub.add_parser("route-priors")
    sub.add_parser("mutation-controller")
    sub.add_parser("lineage-graph")
    sub.add_parser("regime-bandit")
    sub.add_parser("drift-detection")
    sub.add_parser("next-experiment")
    sub.add_parser("causal-experiments")
    sub.add_parser("experiment-debt")
    sub.add_parser("meta-strategy")
    sub.add_parser("global-memory")
    sub.add_parser("calibration")
    sub.add_parser("belief-calibration")
    sub.add_parser("learning-control-plane")
    sub.add_parser("learning-depth")
    sub.add_parser("ops-hardening")
    sub.add_parser("temporal-memory")
    sub.add_parser("revalidation-candidates")
    sub.add_parser("research-lab-memory")
    sub.add_parser("self-correction-memory")
    sub.add_parser("pressure-science-memory")
    sub.add_parser("closed-loop-execution-memory")
    sub.add_parser("next-run-opening")
    sub.add_parser("reconcile-outcomes")
    sub.add_parser("stats")
    ap.set_defaults(command="stats")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    db = Step2LearningDB(args.db)
    try:
        if args.command == "ingest-run":
            payload = db.ingest_run_dir(args.run_dir)
        elif args.command == "ingest-feedback":
            payload = db.ingest_feedback_file(args.path)
        elif args.command == "ingest-validation":
            payload = db.ingest_validation_results(args.path)
        elif args.command == "route-memory":
            payload = db.cross_run_route_memory()
        elif args.command == "route-priors":
            payload = db.route_prior_model()
        elif args.command == "mutation-controller":
            payload = db.adaptive_mutation_controller()
        elif args.command == "lineage-graph":
            payload = db.candidate_lineage_graph()
        elif args.command == "regime-bandit":
            payload = db.regime_aware_bandit()
        elif args.command == "drift-detection":
            payload = db.drift_detection()
        elif args.command == "next-experiment":
            payload = db.next_experiment_packet()
        elif args.command == "causal-experiments":
            payload = db.causal_experiment_registry()
        elif args.command == "experiment-debt":
            payload = db.experiment_debt_queue()
        elif args.command == "meta-strategy":
            payload = db.meta_strategy_learner()
        elif args.command == "global-memory":
            payload = db.global_learning_memory()
        elif args.command == "calibration":
            payload = db.calibration_persistence()
        elif args.command == "belief-calibration":
            payload = db.belief_calibration_persistence()
        elif args.command == "learning-control-plane":
            payload = db.learning_control_plane_memory()
        elif args.command == "learning-depth":
            payload = db.learning_depth_memory()
        elif args.command == "ops-hardening":
            payload = db.ops_hardening_memory()
        elif args.command == "temporal-memory":
            payload = db.temporal_decay_memory()
        elif args.command == "revalidation-candidates":
            payload = db.revalidation_candidates()
        elif args.command == "research-lab-memory":
            payload = db.research_lab_memory()
        elif args.command == "self-correction-memory":
            payload = db.self_correction_memory()
        elif args.command == "pressure-science-memory":
            payload = db.pressure_science_memory()
        elif args.command == "closed-loop-execution-memory":
            payload = db.closed_loop_execution_memory()
        elif args.command == "next-run-opening":
            payload = db.next_run_opening_controls()
        elif args.command == "reconcile-outcomes":
            payload = db.outcome_reconciliation()
        else:
            payload = db.stats()
    finally:
        db.close()
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
