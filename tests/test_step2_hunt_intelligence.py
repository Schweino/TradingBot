import json
import shutil
from argparse import Namespace
from pathlib import Path

import step2_hunt_intelligence as hunt_intel
import run_step2_three_hour_hunt as hunt_runner
import step2_router_hunter


def _row(name, pnl, delta, trades=10, by_day=None):
    return {
        "variant": name,
        "weights": {"ema": 1.0, "vwap": 2.0},
        "bias": 0.1,
        "step2_pnl": pnl,
        "step2_delta_vs_active": delta,
        "step2_trades": trades,
        "step2_win_rate_pct": 60.0,
        "by_day": by_day or {
            "2026-05-01": {"pnl": pnl, "trades": trades},
        },
        "by_ticker": {
            "CLSK": {"pnl": pnl, "trades": trades},
        },
    }


def test_live_only_ranking_excludes_rows_that_do_not_beat_live():
    rows = [
        _row("loser", 99.0, -1.0),
        _row("winner", 101.0, 1.0),
    ]

    ranked = hunt_intel.rank_rows(rows, live_only=True, behavioral_dedupe=True, limit=10)

    assert [row["variant"] for row in ranked["raw_leaderboard"]] == ["winner"]
    assert ranked["input_rows"] == 2
    assert ranked["live_filtered_rows"] == 1


def test_status_line_separates_raw_and_promotion_rank1():
    raw = {
        "variant": "raw_v",
        "step2_pnl": 120.0,
        "step2_delta_vs_active": 20.0,
        "promotion_readiness_score": 10.0,
        "promotion_distance": {"passed": False, "gates": [{"gate": "holdout", "passed": False}]},
    }
    promo = {
        "variant": "promo_v",
        "step2_pnl": 110.0,
        "step2_delta_vs_active": 10.0,
        "promotion_readiness_score": 80.0,
        "promotion_distance": {"passed": True},
    }

    line = hunt_runner._compact_status_line(
        phase="cycle_running",
        cycle_idx=2,
        hunter="router",
        top_count=10,
        rank1=raw,
        promotion_rank1=promo,
        next_lane="validate",
        remaining_sec=120,
    )

    assert "raw#1=raw_v" in line
    assert "promo#1=promo_v" in line
    assert "blockers=promotion: holdout" in line
    assert len(line) < 700


def test_native_window_seconds_prefers_stop_after_and_window_minutes():
    args = Namespace(hours=4, window_minutes=0, stop_after_sec=90)
    assert hunt_runner._effective_run_seconds(args) == 90
    args.stop_after_sec = 0
    args.window_minutes = 2
    assert hunt_runner._effective_run_seconds(args) == 120


def test_hunt_cli_accepts_compatibility_aliases():
    args = hunt_runner.parse_args([
        "--max-variants", "250",
        "--bootstrap-controls", "handoff.json",
        "--route-focus", "RIOT|trend_pullback|late",
        "--status-event-verbosity", "digest",
        "--artifact-profile", "compact",
    ])

    assert args.target_count == 250
    assert args.runtime_bootstrap_json == "handoff.json"
    assert args.focus_route == ["RIOT|trend_pullback|late"]
    assert args.status_event_verbosity == "digest"
    assert args.artifact_profile == "compact"
    assert args.json_verbosity == "digest"
    assert not args.write_full_running_top100
    assert args.archive_completed_artifacts
    assert args.okay_to_delete_dir == "okay_to_delete"


def test_completed_run_archive_moves_cold_artifacts_and_keeps_receipts():
    run_dir = Path("runtime") / "test_completed_run_archive" / "loop500_r99_20260511_000000"
    root = run_dir.parent
    archive_root = Path("runtime") / "test_completed_run_archive_root"
    if root.exists():
        shutil.rmtree(root)
    if archive_root.exists():
        shutil.rmtree(archive_root)
    cycle_dir = run_dir / "loop500_r99_cycle_0000_router"
    cycle_dir.mkdir(parents=True)
    (run_dir / "final_summary.json").write_text("{}", encoding="utf-8")
    (run_dir / "runtime_handoff_controls.json").write_text("{}", encoding="utf-8")
    (run_dir / "variant_funnel.json").write_text("{}", encoding="utf-8")
    (run_dir / "online_state.json").write_text('{"bulky": true}', encoding="utf-8")
    (cycle_dir / "scored_variants.jsonl").write_text('{"variant": "x"}\n', encoding="utf-8")
    args = hunt_runner.parse_args(["--name", "archive_test", "--okay-to-delete-dir", str(archive_root)])

    report = hunt_runner._archive_completed_run_artifacts(args, run_dir, {"final_summary": str(run_dir / "final_summary.json")})
    archive_dir = hunt_runner._completed_run_archive_dir(args, run_dir)

    assert report["enabled"] is True
    assert report["moved_count"] == 2
    assert (run_dir / "final_summary.json").exists()
    assert (run_dir / "runtime_handoff_controls.json").exists()
    assert (run_dir / "variant_funnel.json").exists()
    assert not (run_dir / "online_state.json").exists()
    assert not cycle_dir.exists()
    assert (archive_dir / "online_state.json").exists()
    assert (archive_dir / "loop500_r99_cycle_0000_router" / "scored_variants.jsonl").exists()
    assert (run_dir / "completed_artifact_archive_manifest.json").exists()
    shutil.rmtree(root)
    shutil.rmtree(archive_root)


def test_completed_run_archive_remaps_cycle_log_cold_paths():
    run_dir = Path("runtime") / "test_completed_run_archive_remaps_cycle_log" / "loop500_r99_20260511_000000"
    root = run_dir.parent
    archive_root = Path("runtime") / "test_completed_run_archive_remaps_cycle_log_root"
    shutil.rmtree(root, ignore_errors=True)
    shutil.rmtree(archive_root, ignore_errors=True)
    try:
        child = run_dir / "loop500_r99_cycle_0000_router"
        logs = run_dir / "cycle_process_logs"
        child.mkdir(parents=True)
        logs.mkdir(parents=True)
        (child / "coordinator_summary.json").write_text("{}", encoding="utf-8")
        stdout = logs / "cycle_0000_router.stdout.log"
        stderr = logs / "cycle_0000_router.stderr.log"
        stdout.write_text("out", encoding="utf-8")
        stderr.write_text("err", encoding="utf-8")
        (run_dir / "final_summary.json").write_text("{}", encoding="utf-8")
        (run_dir / "cycle_log.json").write_text(json.dumps({
            "cycles": [{
                "stdout_path": str(stdout.resolve()),
                "stderr_path": str(stderr.resolve()),
                "coordinator_summary": {"path": str((child / "coordinator_summary.json").resolve())},
            }]
        }), encoding="utf-8")
        args = hunt_runner.parse_args(["--name", "archive_test", "--okay-to-delete-dir", str(archive_root)])

        report = hunt_runner._archive_completed_run_artifacts(args, run_dir, {"final_summary": str(run_dir / "final_summary.json")})
        remap = hunt_runner._remap_archived_cycle_log_paths(run_dir, report)
        cycle = json.loads((run_dir / "cycle_log.json").read_text(encoding="utf-8"))["cycles"][0]

        assert remap["updated"] is True
        assert Path(cycle["stdout_path"]).exists()
        assert Path(cycle["stderr_path"]).exists()
        assert Path(cycle["coordinator_summary"]["path"]).exists()
        assert str(archive_root.resolve()) in cycle["stdout_path"]
    finally:
        shutil.rmtree(root, ignore_errors=True)
        shutil.rmtree(archive_root, ignore_errors=True)


def test_latest_previous_run_uses_name_timestamp_not_archive_mtime():
    root = Path("runtime") / "test_latest_previous_run_uses_name_timestamp"
    if root.exists():
        shutil.rmtree(root)
    older = root / "step2_old_20260510_235959"
    newer = root / "step2_new_20260511_000001"
    current = root / "step2_current_20260511_000500"
    older.mkdir(parents=True)
    newer.mkdir(parents=True)
    current.mkdir(parents=True)
    (older / "final_summary.json").write_text(json.dumps({"run_dir": str(older), "variant_funnel": {"scored_total": 1}}), encoding="utf-8")
    (newer / "final_summary.json").write_text(json.dumps({"run_dir": str(newer), "variant_funnel": {"scored_total": 2}}), encoding="utf-8")
    (older / "variant_funnel.json").write_text(json.dumps({"scored_total": 1}), encoding="utf-8")
    (newer / "variant_funnel.json").write_text(json.dumps({"scored_total": 2}), encoding="utf-8")
    # Simulate archive work touching the older folder after the newer one completed.
    (older / "okay_to_delete").mkdir()
    original_default = hunt_runner.DEFAULT_OUT
    try:
        hunt_runner.DEFAULT_OUT = root
        latest = hunt_runner._latest_previous_run_summary(current)
        latest_dir = hunt_runner._latest_previous_run_dir(current)
    finally:
        hunt_runner.DEFAULT_OUT = original_default
        shutil.rmtree(root)

    assert latest["variant_funnel"]["scored_total"] == 2
    assert latest_dir.name == newer.name


def test_cycle_command_uses_minimal_nested_coordinator_artifacts():
    run_dir = Path("runtime") / "test_cycle_command_uses_minimal_nested_coordinator_artifacts"
    args = hunt_runner.parse_args([
        "--hunters", "router",
        "--batch-size", "100",
        "--max-batches", "1",
        "--max-cycles", "1",
        "--exact-variant-count",
        "--allow-uncertified-cache",
    ])
    cmd = hunt_runner._cycle_command(args, run_dir, 0, 123, "router", {})

    assert "--coordinator-artifact-mode" in cmd
    mode_idx = cmd.index("--coordinator-artifact-mode")
    assert cmd[mode_idx + 1] == "minimal"
    assert "--score-cache-stats-mode" in cmd
    stats_idx = cmd.index("--score-cache-stats-mode")
    assert cmd[stats_idx + 1] == "fast"


def test_compact_mode_omits_duplicate_full_running_top100_by_default():
    args = Namespace(artifact_profile="compact", write_full_running_top100=False)
    assert hunt_runner._write_full_running_top100(args) is False

    args.write_full_running_top100 = True
    assert hunt_runner._write_full_running_top100(args) is True

    args.artifact_profile = "full"
    args.write_full_running_top100 = False
    assert hunt_runner._write_full_running_top100(args) is True


def test_digest_run_payload_is_small_and_points_to_full_artifacts():
    payload = {
        "ok": True,
        "path": "run/final_summary.json",
        "run_dir": "run",
        "artifact_paths": {
            "operator_digest": "run/operator_digest.json",
            "final_summary": "run/final_summary.json",
            "variant_funnel": "run/variant_funnel.json",
        },
        "variant_funnel": {
            "requested_variants": 100,
            "scored_total": 100,
            "behavior_unique_rows": 23,
            "count_contract_ok": True,
        },
        "top100": [_row("winner", 10.0, 5.0)],
        "promotion_survival_top100": [_row("promo", 9.0, 4.0)],
        "top_failure_summary": {
            "human_summary": "repair",
            "promotion_ready_count": 1,
            "repair_task_count": 3,
            "top_failure_reasons": [{"reason": "robustness", "count": 2}],
        },
    }

    digest = hunt_runner._digest_run_payload(payload)

    assert digest["operator_digest"] == "run/operator_digest.json"
    assert digest["variant_funnel"]["scored_total"] == 100
    assert digest["top_candidate"]["variant"] == "winner"
    assert "top100" not in digest


def test_operator_digest_and_artifact_budget_are_compact_start_here():
    root = Path("runtime") / "test_operator_digest_and_artifact_budget_are_compact_start_here"
    shutil.rmtree(root, ignore_errors=True)
    try:
        root.mkdir(parents=True)
        row = _row("digest_winner", 150.0, 50.0)
        row["promotion_distance"] = {"passed": False, "gates": [{"gate": "holdout", "passed": False}]}
        row["promotion_survival_score"] = 72.5
        (root / "hunt_brain_summary.json").write_text(
            json.dumps({"what_worked": ["router"], "what_failed": ["alias"], "hunt_next": ["repair"]}),
            encoding="utf-8",
        )
        (root / "top_failure_summary.json").write_text(
            json.dumps({"human_summary": "repair holdout", "top_failure_reasons": [{"reason": "holdout"}]}),
            encoding="utf-8",
        )
        (root / "big.json").write_text("x" * 2048, encoding="utf-8")
        rankings = {"raw_leaderboard": [row], "promotion_readiness_leaderboard": [row]}
        args = type("Args", (), {"artifact_profile": "compact", "artifact_size_warning_mb": 0.001})()

        budget = hunt_runner._artifact_budget_report(root, warning_mb=0.001)
        hunt_runner._write_json(root / "artifact_budget_report.json", budget)
        digest = hunt_runner._operator_digest(args, root, rankings, [{"cycle": 0}], {"artifact_budget_report": str(root / "artifact_budget_report.json")})
        out = root / "operator_digest.json"
        hunt_runner._write_compact_json(out, digest)

        assert budget["warning_count"] >= 1
        assert digest["top_candidate"]["variant"] == "digest_winner"
        assert digest["learning"]["what_worked"] == ["router"]
        assert out.stat().st_size < 20_000
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_post_window_health_marks_partial_valid_cycles():
    run_dir = Path("runtime") / "test_post_window_health_marks_partial_valid_cycles"
    shutil.rmtree(run_dir, ignore_errors=True)
    run_dir.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "variant": "v1_RIOT|trend_pullback|late",
            "step2_pnl": 120.0,
            "step2_delta_vs_active": 20.0,
            "promotion_distance": {"passed": True},
            "routed_profile_safety": {"ok": True},
        }
    ]
    args = Namespace(stop_after_sec=60, window_minutes=0)
    report = hunt_runner._post_window_health_report(
        args,
        run_dir,
        [{"cycle": 0, "ok": False, "partial_valid_artifact": True, "interrupt_reason": "run_window_elapsed"}],
        rows,
        {"top100_count": 1},
        {},
    )

    assert report["native_window_control"]["used_native_window"] is True
    assert report["partial_valid_cycle_count"] == 1
    assert report["top100_health"]["promotion_distance_passed_count"] == 1
    assert report["recommendation"] == "review_best_promotion_candidate"


def test_run_collector_uses_one_scorer_output_per_hunter():
    root = Path("runtime") / "test_run_collector_uses_one_scorer_output_per_hunter"
    if root.exists():
        shutil.rmtree(root)
    try:
        cycle = root / "cycle_0000"
        router = root / "cycle_0000_router"
        cycle.mkdir(parents=True)
        router.mkdir(parents=True)
        winner = _row("router_001_WIN", 110.0, 10.0)
        lower = _row("router_002_LOWER", 105.0, 5.0)
        coordinator = {"winners": [winner], "leaderboard": [winner]}
        checkpoint = {"winners": [winner], "leaderboard": [winner, lower]}
        summary = {"winners": [winner], "leaderboard": [winner, lower]}
        (cycle / "coordinator_summary.json").write_text(json.dumps(coordinator), encoding="utf-8")
        (router / "checkpoint.json").write_text(json.dumps(checkpoint), encoding="utf-8")
        (router / "summary.json").write_text(json.dumps(summary), encoding="utf-8")

        paths = hunt_runner._scorer_summary_paths(root)
        ranked = hunt_runner.collect_rankings(
            type("Args", (), {"live_only": True, "behavioral_dedupe": False, "leaderboard_limit": 10})(),
            root,
        )

        assert [path.name for path in paths] == ["summary.json"]
        assert ranked["input_rows"] == 3
        assert ranked["diagnostic_source_paths"]
        assert len(ranked["diagnostic_rows"]) > len(ranked["decorated_rows"])
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_run_collector_prefers_full_scored_manifest_when_present():
    root = Path("runtime") / "test_run_collector_prefers_full_scored_manifest_when_present"
    if root.exists():
        shutil.rmtree(root)
    try:
        router = root / "cycle_0000_router"
        router.mkdir(parents=True)
        rows = [_row(f"router_{idx:03d}_WIN", 100.0 + idx, float(idx)) for idx in range(1, 6)]
        manifest = router / "scored_variants.jsonl"
        manifest.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
        (router / "summary.json").write_text(
            json.dumps({"leaderboard": rows[:2], "active": _row("active", 100.0, 0.0), "active_step2_pnl": 100.0}),
            encoding="utf-8",
        )

        paths = hunt_runner._scorer_summary_paths(root)
        ranked = hunt_runner.collect_rankings(
            type("Args", (), {"live_only": True, "behavioral_dedupe": False, "leaderboard_limit": 10})(),
            root,
        )

        assert [path.name for path in paths] == ["scored_variants.jsonl"]
        assert ranked["input_rows"] == 5
        assert ranked["active_step2_pnl"] == 100.0
        assert ranked["raw_leaderboard"][0]["variant"] == "router_005_WIN"
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_runner_promotion_survival_repair_gate_and_tiny_guard_are_compact():
    raw = _row("raw_big", 200.0, 100.0)
    raw["learning_tags"] = ["beats_live", "promotion_weak", "no_holdout_credit", "route_narrow", "small_total_edge"]
    raw["promotion_readiness_score"] = 5.0
    ready = _row("survivor", 160.0, 60.0)
    ready["learning_tags"] = ["beats_live", "day_consistent", "low_overfit_risk", "route_broader", "sample_ok"]
    ready["promotion_readiness_score"] = 65.0
    ready["promotion_quality_score"] = 20.0

    survival = hunt_runner._promotion_survival_leaderboard([raw, ready], limit=10)
    repair = hunt_runner._promotion_evidence_repair_queue([raw, ready], limit=10)
    directives = hunt_runner._repair_worker_directives(repair, worker_count=2)
    args = type(
        "Args",
        (),
        {
            "smoke_validation_mode": True,
            "tiny_run_threshold": 1000,
            "persist_tiny_run_learning": False,
            "batch_size": 20,
            "max_batches": 1,
            "runtime_control_mode": "observe",
            "live_only": True,
            "ranking_objective": "promotion_survival",
        },
    )()
    guard = hunt_runner._tiny_run_calibration_guard(args, {"requested_variants": 20})
    limited_guard = hunt_runner._tiny_run_calibration_guard(args, {"requested_variants": 500})
    floor = hunt_runner._quality_floor_leaderboard([raw, ready], limit=10)
    actions = hunt_runner._route_action_report([raw, ready])

    assert survival[0]["variant"] == "survivor"
    assert survival[0]["promotion_minimum_bar"]["passed"] is True
    assert survival[-1]["promotion_minimum_bar"]["passed"] is False
    assert repair["task_count"] == 2
    assert directives["directive_count"] == 2
    assert "evidence_validation_holdout_day_split" in {row["action"] for row in directives["directives"]}
    assert guard["is_tiny_diagnostic"] is True
    assert guard["persist_to_learning_db"] is False
    assert limited_guard["is_tiny_diagnostic"] is False
    assert limited_guard["learning_tier"] == "limited_learning"
    assert limited_guard["persist_to_learning_db"] is True
    assert [row["variant"] for row in floor["leaderboard"]] == ["survivor"]
    assert actions["action_counts"]["global_weight_shift"] == 2

    weak_evidence = {
        "robustness_score": 43.0,
        "red_flags": ["weak_day_consistency"],
        "train_holdout": {"holdout_delta_vs_active": 12.0, "holdout_pnl": 100.0},
    }
    bar = hunt_runner._promotion_minimum_bar(raw, evidence=weak_evidence)
    auto = hunt_runner._auto_hunt_recommendation(
        [raw],
        {"kept_count": 0},
        {"task_count": 1},
        {"crowded_routes": []},
    )
    loosen = hunt_runner._auto_hunt_recommendation(
        [raw],
        {"kept_count": 0},
        {"task_count": 1},
        {"crowded_routes": []},
        variant_funnel={"scored_to_live_stream_yield_pct": 0.6},
        args=type("RepairArgs", (), {"hunt_mode": "promotion_repair", "repair_yield_floor_pct": 1.0})(),
    )
    loosen_exploration = hunt_runner._auto_hunt_recommendation(
        [raw],
        {"kept_count": 0},
        {"task_count": 1},
        {"crowded_routes": []},
        variant_funnel={"scored_to_live_stream_yield_pct": 0.6},
        args=type("RepairArgs", (), {"hunt_mode": "repair_exploration", "repair_yield_floor_pct": 1.0})(),
    )
    tier = hunt_runner._cycle_learning_tiers(
        args,
        [{
            "cycle": 0,
            "hunter": "router",
            "ok": True,
            "cmd": ["runner", "--batch-size", "500", "--max-batches", "1", "--exact-variant-count"],
            "streaming_telemetry_summary": {"events": 1},
        }],
    )
    alias = hunt_runner._anti_alias_pressure_report(
        {"live_filtered_rows": 35, "behavior_unique_rows": 3},
        [raw, ready],
    )
    ready_evidence = {
        "variant": "survivor",
        "route_key": hunt_intel.route_key_from_row(ready),
        "robustness_score": 82.0,
        "red_flags": [],
        "train_holdout": {"holdout_delta_vs_active": 20.0, "holdout_pnl": 120.0},
        "day_profile": {"beats_active_day_rate": 1.0},
        "recommendation": {"blockers": []},
    }
    weak_evidence_report = dict(weak_evidence, variant="raw_big", route_key=hunt_intel.route_key_from_row(raw), day_profile={"beats_active_day_rate": 0.5})
    evidence_report = {
        "reports": [weak_evidence_report, ready_evidence],
        "by_variant": {"raw_big": weak_evidence_report, "survivor": ready_evidence},
    }
    evidence_survival = hunt_runner._promotion_survival_leaderboard(
        [raw, ready],
        limit=10,
        evidence_by_variant=evidence_report["by_variant"],
        sort_mode="evidence_tier",
    )
    near_queue = hunt_runner._near_promotion_evidence_queue([raw, ready], evidence_report, limit=10)
    lanes = hunt_runner._evidence_repair_lanes(evidence_report, [raw, ready])
    route_distance = hunt_runner._route_promotion_distance_cards([raw, ready], evidence_report["by_variant"])
    repair_alloc = hunt_runner._repair_allocation_by_evidence_weakness(lanes, {"overconcentrated": True})
    caps = hunt_runner._repair_route_budget_caps(
        {"tasks": [{"route_key": "CLSK|vwap|open", "candidate_count": 10}, {"route_key": "MARA|vwap|open", "candidate_count": 1}]},
        {"pressure": "high"},
    )
    next_plan = hunt_runner._next_500_plan(
        [raw],
        {
            "tasks": [{
                "route_key": "CLSK|vwap|open",
                "candidate_count": 1,
                "gaps": ["holdout_evidence", "promotion_readiness", "route_breadth", "edge_size"],
            }],
            "focus_routes": ["CLSK|vwap|open"],
        },
        {"avoid_routes": []},
        {"kept_count": 0},
        anti_alias={"pressure": "low", "structural_mutation_routes": ["CLSK|vwap|open"]},
        route_budget_caps={
            "route_caps": [{"route_key": "CLSK|vwap|open", "max_variants_per_500": 225, "cap_required": True}],
            "overconcentrated": True,
        },
    )
    rank_reason = hunt_runner._why_raw_rank1_not_promotion_rank1([raw, ready], evidence_survival, evidence_by_variant=evidence_report["by_variant"])
    severity = hunt_runner._post_test_recommendation_severity(auto, {"kept_count": 0}, {"pressure": "low"}, {"task_count": 4})
    recipe = hunt_runner._next_command_recipe(args, Path("."), {"recommended_hunt_mode": "promotion_repair", "recommended_focus_routes": ["CLSK|vwap|open"]}, caps)

    assert "holdout_positive_but_not_promotion_grade" in bar["failures"]
    assert "robustness_below_floor" in bar["failures"]
    assert auto["recommended_action"] == "repair_first"
    assert loosen["recommended_action"] == "loosen_repair"
    assert loosen["next_mode"] == "repair_exploration"
    assert loosen_exploration["recommended_action"] == "loosen_repair"
    assert tier[0]["scored_total"] == 500
    assert alias["pressure"] == "high"
    assert alias["hard_behavior_unique_warning"] is True
    assert evidence_survival[0]["variant"] == "survivor"
    assert near_queue["best_evidence_candidate"]["variant"] == "survivor"
    assert lanes["lane_counts"]["robustness_below_70"] == 1
    assert lanes["lane_counts"]["weak_day_consistency"] == 1
    assert route_distance["closest_route"]["route_key"]
    assert repair_alloc["route_budget_caps_active"] is True
    assert sum(row["variants"] for row in next_plan["allocation"]) == 500
    assert next_plan["route_cap_compliance"][0]["cap_respected"] is True
    assert next_plan["objective"] == "repair_promotion_readiness_and_route_breadth"
    assert "sibling_route_expansion" in {row["bucket"] for row in next_plan["allocation"]}
    assert "robustness_floor_repair" in {row["bucket"] for row in next_plan["allocation"]}
    assert "robustness_grade_target_repair" in {row["bucket"] for row in next_plan["allocation"]}
    assert sum(row["variants"] for row in next_plan["allocation"] if row["bucket"] == "weird_exploration") <= 15
    assert "edge_cushion_repair" in {row["bucket"] for row in next_plan["allocation"]}
    assert len(next_plan["broadened_repair_routes"]) >= 3
    assert caps["overconcentrated"] is True
    assert rank_reason["same_candidate"] is False
    assert severity["severity"] == "red"
    assert "anti-alias pressure is high" not in severity["why"]
    assert "--hunt-mode" in recipe["command"]
    assert "--max-cycles" in recipe["command"]
    assert "--focus-route" in recipe["command"]


def test_next_command_recipe_preserves_diagnostic_cache_flags():
    args = hunt_runner.parse_args([
        "--allow-uncertified-cache",
        "--runtime-control-mode",
        "observe",
    ])

    recipe = hunt_runner._next_command_recipe(args, Path("."), {"recommended_hunt_mode": "promotion_repair"}, {})

    assert "--allow-uncertified-cache" in recipe["command"]
    mode_idx = recipe["command"].index("--runtime-control-mode")
    assert recipe["command"][mode_idx + 1] == "observe"
    assert "diagnostic" in " ".join(recipe["notes"])


def test_compact_candidate_digest_derives_promotion_blockers_when_missing():
    row = _row("raw_big", 200.0, 100.0)
    row["promotion_readiness_score"] = 0.0

    digest = hunt_runner._compact_candidate_digest(row)

    assert digest["promotion_distance_passed"] is False
    assert "promotion_readiness_floor" in digest["blocking_gates"]


def test_promotion_distance_blocks_stale_route_audit_identity():
    row = _row("router", 200.0, 100.0)
    row["learning_tags"] = ["beats_live", "day_consistent", "low_overfit_risk", "route_broader"]
    row["promotion_readiness_score"] = 80.0
    row["routes"] = [
        {"name": "current_route", "match": {"ticker": "CLSK"}, "action": "force_short"}
    ]
    row["route_audit"] = {
        "routes": [
            {"route": "stale_route", "matched_opportunities": 100, "opportunity_share_pct": 10.0, "skipped_sides": 0}
        ]
    }
    evidence = {
        "robustness_score": 80.0,
        "red_flags": [],
        "train_holdout": {"holdout_delta_vs_active": 50.0, "holdout_pnl": 100.0},
    }

    bar = hunt_runner._promotion_minimum_bar(row, evidence=evidence)
    distance = hunt_runner._promotion_distance(row, evidence=evidence)

    assert "routed_profile_safety_gate" in bar["failures"]
    assert distance["passed"] is False
    assert any(gate["gate"] == "routed_profile_safety" and gate["passed"] is False for gate in distance["gates"])


def test_promotion_targets_and_precheck_distinguish_blockers_from_goals():
    row = _row("near_ready", 110.0, 10.0)
    row["promotion_readiness_score"] = 60.0
    row["learning_tags"] = ["sample_ok"]
    evidence = {
        "robustness_score": 60.0,
        "red_flags": [],
        "train_holdout": {"holdout_delta_vs_active": 10.0, "holdout_pnl": 100.0},
    }

    distance = hunt_runner._promotion_distance(row, evidence)
    summary = hunt_runner._top_failure_summary([row], {"task_count": 0}, {"flagged_count": 0}, {"near_ready": evidence})

    assert distance["passed"] is True
    assert distance["open_gate_count"] == 0
    assert distance["report_only_target_count"] == 1
    assert summary["promotion_ready_count"] == 1
    assert summary["hard_blockers"] == []
    assert summary["promotion_grade_target_misses"][0]["target"] == "robustness_promotion_grade_target"

    narrow = _row("narrow", 112.0, 12.0)
    narrow["promotion_readiness_score"] = 20.0
    narrow["learning_tags"] = ["route_narrow"]
    precheck = hunt_runner._promotion_review_precheck(narrow, evidence)

    assert precheck["decision"] == "do_not_review_yet_repair_first"
    assert precheck["do_not_run_full_review_yet"] is True
    assert set(precheck["repair_first_reasons"]) == {"promotion_readiness_below_floor", "route_too_narrow"}


def test_route_concentration_and_actual_allocation_reports():
    root = Path("runtime") / "test_route_concentration_and_actual_allocation_reports"
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)
    rows = [_row(f"winner_{idx}", 110.0 + idx, 10.0 + idx) for idx in range(10)]
    try:
        for row in rows:
            row["routes"] = [{"match": {"ticker": "MARA", "setup_type": "vwap_reclaim_breakdown", "session_phase": "midday"}, "action": "score"}]
        warning = hunt_runner._route_concentration_warning(rows, {"route_count": 1, "min_distinct_repair_routes": 4, "route_caps": []})
        alias = hunt_runner._anti_alias_pressure_report({"live_filtered_rows": 16, "behavior_unique_rows": 10}, rows)
        telemetry = root / "streaming_telemetry.jsonl"
        manifest = root / "cycle_0000_router" / "scored_variants.jsonl"
        manifest.parent.mkdir(parents=True)
        manifest.write_text(
            "\n".join(json.dumps(row) for row in rows) + "\n",
            encoding="utf-8",
        )
        telemetry.write_text(
            json.dumps({"summary": {"scored_total": 10, "sampled_route_distribution": {"1a7d1b8a7e4a1052421426a3": 7}}}) + "\n",
            encoding="utf-8",
        )
        actual = hunt_runner._actual_route_allocation(root)
        comparison = hunt_runner._plan_vs_actual_route_allocation(
            {"route_budget_usage": {"MARA|vwap_reclaim_breakdown|midday": 175}},
            actual,
        )

        assert warning["active"] is True
        assert warning["cap_warning"] is True
        assert alias["recommended_action"] == "force_structural_mutation"
        assert actual["telemetry_present"] is True
        assert actual["allocation_source"] == "scored_manifest_fallback"
        assert actual["opaque_route_key_count"] == 0
        assert actual["sample_coverage_ok"] is True
        assert comparison["rows"][0]["route_key"] == "MARA|vwap_reclaim_breakdown|midday"
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_evidence_aware_reports_do_not_contradict_promotion_ready_candidate():
    row = _row("ready", 114000.0, 120.0)
    row["promotion_readiness_score"] = 5.0
    row["learning_tags"] = ["beats_live", "promotion_weak", "no_holdout_credit", "sample_ok"]
    row["route_breadth_credit"] = {"passed": True}
    evidence = {
        "variant": "ready",
        "robustness_score": 60.0,
        "red_flags": [],
        "train_holdout": {"holdout_delta_vs_active": 40.0, "holdout_pnl": 100.0},
        "day_profile": {"beats_active_day_rate": 0.95},
    }

    bar = hunt_runner._promotion_minimum_bar(row, evidence)
    suspicious = hunt_runner._suspicious_winner(row, evidence)
    tradeoff = hunt_runner._tradeoff_frontier([row], evidence_by_variant={"ready": evidence})

    assert bar["passed"] is True
    assert suspicious["evidence_gaps"] == []
    assert suspicious["evidence_adjusted_readiness_score"] >= 45.0
    assert tradeoff["points"][0]["promotion_bar_passed"] is True


def test_actual_route_allocation_marks_unrouted_telemetry_as_partial():
    root = Path("runtime") / "test_actual_route_allocation_marks_unrouted_telemetry_as_partial"
    if root.exists():
        shutil.rmtree(root)
    try:
        root.mkdir(parents=True)
        (root / "streaming_telemetry.jsonl").write_text(
            json.dumps({
                "summary": {
                    "scored_total": 100,
                    "sampled_route_distribution": {
                        "CLSK|vwap_reclaim_breakdown|open": 99,
                        "unrouted": 1,
                    },
                }
            }) + "\n",
            encoding="utf-8",
        )

        actual = hunt_runner._actual_route_allocation(root)

        assert actual["sample_coverage_ok"] is True
        assert actual["human_route_key_coverage_ok"] is False
        assert actual["human_route_key_coverage_status"] == "partial"
        assert actual["unrouted_count"] == 1
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_next_command_and_truth_summary_are_big_run_coherent():
    args = type(
        "Args",
        (),
        {
            "name": "measure_100",
            "hours": 0.03,
            "batch_size": 100,
            "max_batches": 1,
            "target_count": 100,
            "max_focus_routes": 12,
        },
    )()
    recipe = hunt_runner._next_command_recipe(
        args,
        Path("runtime"),
        {"run_repair_cycle_now": True, "recommended_hunt_mode": "promotion_repair", "recommended_focus_routes": ["MARA|vwap_reclaim_breakdown|*"]},
        {"overconcentrated": True},
    )
    truth = hunt_runner._big_run_truth_summary(
        {"top100_count": 8, "behavior_unique_rows": 8},
        {"promotion_ready_count": 1},
        {"blockers": ["route_throttles_still_needed"], "decision": "run_targeted_controlled_repair_first"},
        {"decision": "run_targeted_500_first", "ready_with_controls": False},
        {"severity": "yellow"},
        {"recommended_run_size": "targeted_500"},
    )

    assert recipe["recommended_run_size"] == "targeted_500"
    assert recipe["command"][recipe["command"].index("--batch-size") + 1] == "500"
    assert "targeted500" in recipe["command"][recipe["command"].index("--name") + 1]
    assert "--focus-route 'MARA|vwap_reclaim_breakdown|*'" in recipe["command_string"]
    assert truth["verdict"] == "targeted_repair_before_big_run"
    assert truth["launch_decision"] == "run_targeted_500_first"
    assert truth["truth_table"]["has_promotion_ready_candidate"] is True


def test_final_summary_embeds_scale_control_artifacts():
    root = Path("runtime") / "test_final_summary_embeds_scale_control_artifacts"
    if root.exists():
        shutil.rmtree(root)
    try:
        root.mkdir(parents=True)
        (root / "big_run_launch_controls.json").write_text(
            json.dumps({"decision": "run_targeted_500_first", "ready_with_controls": False}),
            encoding="utf-8",
        )
        (root / "big_run_truth_summary.json").write_text(
            json.dumps({"verdict": "targeted_repair_before_big_run", "launch_decision": "run_targeted_500_first"}),
            encoding="utf-8",
        )

        payloads = hunt_runner._final_summary_control_payloads(root)

        assert payloads["big_run_launch_controls"]["decision"] == "run_targeted_500_first"
        assert payloads["big_run_truth_summary"]["verdict"] == "targeted_repair_before_big_run"
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_issue_register_hides_conditions_that_are_already_satisfied():
    register = hunt_runner._before_big_run_issue_register(
        {
            "decision": "run_targeted_controlled_repair_first",
            "checks": {"route_throttles_still_needed": False},
            "promotion_ready_count": 1,
            "estimated_big_run_behavior_unique_rows": 40,
        },
        {"top_failure_reasons": []},
        {"reason_counts": [], "universal_blockers": []},
        {"pressure": "low", "collapse_rate": 0.0},
        {"active": False},
        {"near_promotion_delta": {"improvement_points": 2.0}},
    )

    assert "promotion_ready_count > 0" not in register["no_big_run_until"]
    assert "promotion_ready_count > 0" in register["satisfied_conditions"]
    assert register["issues"][0]["issue"] == "route_throttles_still_needed"


def test_big_run_preflight_blocks_scale_on_concentration_and_focus_but_accepts_next_command_focus():
    root = Path("runtime") / "test_big_run_preflight_blocks_scale_on_concentration"
    if root.exists():
        shutil.rmtree(root)
    try:
        root.mkdir(parents=True)
        (root / "status_policy.json").write_text("{}", encoding="utf-8")
        (root / "cycle_log.json").write_text("{}", encoding="utf-8")
        repair_queue = {"focus_routes": ["CLSK|vwap_reclaim_breakdown|*", "RIOT|flow_exhaustion_fade|midday"]}
        route_distance = {"closest_route": {"route_key": "CLSK|vwap_reclaim_breakdown|*"}}
        recipe = {"focus_routes": ["CLSK|vwap_reclaim_breakdown|*", "RIOT|flow_exhaustion_fade|midday"]}
        stale = hunt_runner._stale_focus_audit([], repair_queue, route_distance, recipe)
        warning = {"active": True}
        preflight = hunt_runner._big_run_preflight_gate(
            type("Args", (), {"exact_variant_count": True})(),
            root,
            {"count_contract_ok": True, "top100_count": 34},
            {"allocation_total": 500, "route_cap_compliance": []},
            {
                "telemetry_present": True,
                "human_route_key_coverage_ok": True,
                "sample_coverage_ok": True,
            },
            {"promotion_ready_count": 0},
            warning,
            stale,
            {"suspicious_winner_count": 1},
        )

        assert stale["status"] == "next_command_ready"
        assert "stale_focus_update_needed" not in preflight["blockers"]
        assert "route_concentration_below_scale_threshold" in preflight["blockers"]
        assert "missing_promotion_ready_candidates" in preflight["blockers"]
        assert "no_suspicious_winners" in preflight["blockers"]
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_repair_regression_and_route_admission_preserve_original_objective():
    rows = [_row(f"winner_{idx}", 114000.0 + idx, 100.0 + idx) for idx in range(10)]
    for row in rows:
        row["variant"] = f"{row['variant']}_MARA|trend_pullback|open"
        row["routes"] = [{"match": {"ticker": "MARA", "setup_type": "trend_pullback", "session_phase": "open"}}]
    intended = ["CLSK|vwap_reclaim_breakdown|*", "RIOT|vwap_reclaim_breakdown|*"]
    concentration = hunt_runner._route_concentration_warning(rows, {})
    regression = {
        "active": True,
        "recommended_next_mode": "repair_exploration",
        "reasons": ["live_yield_regressed", "promotion_distance_widened"],
    }
    admission = hunt_runner._route_admission_gate(rows, intended, concentration, regression)
    decision = hunt_runner._repair_cycle_now_decision(
        {"decision": "repair_first"},
        {"recommended_action": "repair_first", "next_mode": "promotion_repair"},
        {"focus_routes": ["MARA|trend_pullback|open"], "task_count": 1},
        {"kept_count": 0},
        {"pressure": "medium"},
        route_admission=admission,
        repair_regression=regression,
    )

    assert admission["requires_admission_packet"] is True
    assert admission["admitted_as_new_center"] is False
    assert admission["throttle_routes"] == ["MARA|trend_pullback|open"]
    assert decision["recommended_hunt_mode"] == "repair_exploration"
    assert "MARA|trend_pullback|open" not in decision["recommended_focus_routes"]
    assert "CLSK|vwap_reclaim_breakdown|*" in decision["recommended_focus_routes"]


def test_big_run_learning_gates_explain_alias_sample_and_promotion_blockers():
    root = Path("runtime") / "test_big_run_learning_gates_explain_alias_sample_and_promotion_blockers"
    if root.exists():
        shutil.rmtree(root)
    try:
        root.mkdir(parents=True)
        (root / "status_policy.json").write_text("{}", encoding="utf-8")
        (root / "cycle_log.json").write_text("{}", encoding="utf-8")
        rows = [_row(f"blocked_{idx}", 114000.0 + idx, 100.0 + idx) for idx in range(6)]
        for row in rows:
            row["promotion_readiness_score"] = 5.0
            row["learning_tags"] = ["beats_live", "promotion_weak", "no_holdout_credit", "route_narrow"]
            row["routes"] = [{"match": {"ticker": "RIOT", "setup_type": "flow_exhaustion_fade", "session_phase": "midday"}}]
        evidence = {
            row["variant"]: {
                "variant": row["variant"],
                "robustness_score": 40.0,
                "red_flags": [],
                "train_holdout": {"holdout_delta_vs_active": 10.0, "holdout_pnl": 100.0},
            }
            for row in rows
        }
        blocker_digest = hunt_runner._promotion_blocker_digest(rows, evidence)
        anti_alias = hunt_runner._anti_alias_pressure_report({"live_filtered_rows": 23, "behavior_unique_rows": 6}, rows)
        actual = {
            "telemetry_present": True,
            "human_route_key_coverage_ok": True,
            "sample_coverage_ok": True,
            "sampled_route_total": 500,
            "top_route": "RIOT|flow_exhaustion_fade|midday",
            "top_route_share_pct": 45.0,
            "top_route_family": "RIOT|flow_exhaustion_fade",
            "top_route_family_share_pct": 80.0,
            "route_distribution": {"RIOT|flow_exhaustion_fade|midday": 225},
        }
        sampled = hunt_runner._sampled_route_concentration_report(actual)
        preflight = hunt_runner._big_run_preflight_gate(
            type("Args", (), {"exact_variant_count": True})(),
            root,
            {"count_contract_ok": True, "top100_count": 6, "behavior_unique_rows": 6},
            {"allocation_total": 500, "route_cap_compliance": []},
            actual,
            {"promotion_ready_count": 0},
            {"active": False},
            {"requires_next_run_update": False},
            {"suspicious_winner_count": 0},
            {"soft_active": True},
            {"requires_admission_packet": True, "admitted_as_new_center": False, "throttle_routes": ["RIOT|flow_exhaustion_fade|midday"]},
            anti_alias=anti_alias,
            sampled_concentration=sampled,
            repair_progress={"near_promotion_delta": {"improvement_points": -1.0}},
            blocker_digest=blocker_digest,
            winner_distribution={"route_counts": {"RIOT|flow_exhaustion_fade|midday": 6}},
        )
        register = hunt_runner._before_big_run_issue_register(
            preflight,
            {"top_failure_reasons": [{"reason": "robustness_below_floor", "count": 6}]},
            blocker_digest,
            anti_alias,
            sampled,
            {"near_promotion_delta": {"improvement_points": -1.0}},
        )

        assert "robustness_below_floor" in blocker_digest["universal_blockers"]
        assert sampled["active"] is True
        assert "sampled_route_budget_under_big_run_cap" in preflight["blockers"]
        assert "anti_alias_pressure_not_low" in preflight["blockers"]
        assert "hard_blockers_are_universal" in preflight["blockers"]
        assert register["blocker_count"] >= 3
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_behavior_unique_handoff_controls_bootstrap_next_repair_run():
    anti_alias = {
        "pressure": "high",
        "minimum_big_run_behavior_unique_target": 20,
        "structural_mutation_routes": ["CLSK|vwap_reclaim_breakdown|late"],
    }
    winners = {"top_route": "CLSK|vwap_reclaim_breakdown|late", "route_counts": {"CLSK|vwap_reclaim_breakdown|late": 4}}
    admission = {
        "preserved_focus_routes": ["CLSK|vwap_reclaim_breakdown|*", "MARA|flow_exhaustion_fade|open"],
        "throttle_routes": ["CLSK|vwap_reclaim_breakdown|late"],
        "throttle_multipliers": {"CLSK|vwap_reclaim_breakdown|late": 0.35},
        "avoid_as_center_routes": ["CLSK|vwap_reclaim_breakdown|late"],
    }
    blocker_digest = {"universal_blockers": ["promotion_readiness_below_floor", "route_too_narrow"]}

    controls = hunt_runner._behavior_unique_generation_controls(anti_alias, winners, admission, blocker_digest)
    handoff = hunt_runner._runtime_handoff_controls(
        admission,
        anti_alias,
        controls,
        {"active": True, "quality_protection_mode": "hard", "target_failures": ["robustness_below_floor"], "protected_routes": ["CLSK|vwap_reclaim_breakdown|*"]},
        {"recommended_focus_routes": ["CLSK|vwap_reclaim_breakdown|*"]},
        {"route_caps": []},
        {"allocation": []},
    )
    stale = hunt_runner._stale_focus_audit(
        [],
        {"focus_routes": ["CLSK|vwap_reclaim_breakdown|late", "MARA|flow_exhaustion_fade|open"]},
        {"closest_route": {"route_key": "CLSK|vwap_reclaim_breakdown|late"}},
        {"focus_routes": ["MARA|flow_exhaustion_fade|open"]},
        route_admission=admission,
    )

    assert controls["active"] is True
    assert controls["clone_prevention_mode"] == "hard"
    assert controls["max_sample_share_pct_by_route"]["CLSK|vwap_reclaim_breakdown|late"] == 12.0
    assert handoff["runtime_command_adapter"]["target_behavior_unique_count"] == 20
    assert handoff["runtime_command_adapter"]["quality_protection_mode"] == "hard"
    assert "CLSK|vwap_reclaim_breakdown|late" in handoff["throttle_routes"]
    assert stale["status"] == "next_command_ready"


def test_router_candidate_generation_respects_runtime_sample_caps():
    active = step2_router_hunter.lab.Variant("active", {"ema": 1.0, "vwap": 1.0}, 0.0)
    seeds = [
        {"name": "CLSK|vwap_reclaim_breakdown|late", "match": {"ticker": "CLSK", "setup_type": "vwap_reclaim_breakdown", "session_phase": "late"}, "sample_weight": 100.0},
        {"name": "MARA|flow_exhaustion_fade|open", "match": {"ticker": "MARA", "setup_type": "flow_exhaustion_fade", "session_phase": "open"}, "sample_weight": 1.0},
    ]
    variants, _lineage = step2_router_hunter._candidates(
        active,
        seeds,
        step2_router_hunter.random.Random(7),
        0,
        20,
        0.1,
        4.0,
        {"runtime_command_adapter": {"max_sample_share_pct_by_route": {"CLSK|vwap_reclaim_breakdown|late": 10.0}}},
    )
    sampled = {}
    for variant in variants:
        route = step2_router_hunter._route_bucket_key(variant)
        sampled[route] = sampled.get(route, 0) + 1

    assert len(variants) == 20
    assert sampled.get("CLSK|vwap_reclaim_breakdown|late", 0) <= 2


def test_dominant_quality_controls_cap_winner_route_before_big_run():
    variant_funnel = {"scored_to_live_stream_yield_pct": 12.2}
    repair_progress = {"near_promotion_delta": {"improvement_points": -10.1}}
    blocker_digest = {"universal_blockers": ["promotion_readiness_below_floor", "route_too_narrow"]}
    winners = {
        "top_route": "RIOT|vwap_reclaim_breakdown|open",
        "top_route_share_pct": 52.9,
        "route_counts": {"RIOT|vwap_reclaim_breakdown|open": 18, "CLSK|flow_exhaustion_fade|*": 10},
    }
    admission = {
        "preserved_focus_routes": ["RIOT|vwap_reclaim_breakdown|*", "CLSK|flow_exhaustion_fade|*"],
        "throttle_routes": ["RIOT|vwap_reclaim_breakdown|open"],
        "throttle_multipliers": {"RIOT|vwap_reclaim_breakdown|open": 0.12},
    }

    quality = hunt_runner._promotion_quality_protection_controls(
        variant_funnel,
        repair_progress,
        blocker_digest,
        winners,
        admission,
    )
    behavior = hunt_runner._behavior_unique_generation_controls(
        {"pressure": "medium", "minimum_big_run_behavior_unique_target": 30},
        winners,
        admission,
        blocker_digest,
    )
    handoff = hunt_runner._runtime_handoff_controls(
        admission,
        {"pressure": "medium"},
        behavior,
        quality,
        {"recommended_focus_routes": ["CLSK|flow_exhaustion_fade|*"]},
        {"route_caps": []},
        {"allocation": []},
    )

    adapter = handoff["runtime_command_adapter"]
    assert quality["quality_protection_mode"] == "hard"
    assert quality["target_failures"] == ["promotion_readiness_below_floor", "route_breadth"]
    assert adapter["max_sample_share_pct_by_route"]["RIOT|vwap_reclaim_breakdown|open"] == 8.0
    assert adapter["max_winner_share_pct_by_route"]["RIOT|vwap_reclaim_breakdown|open"] == 18.0
    assert adapter["route_breadth_required"] is True
    assert adapter["prefer_promotion_readiness"] is True


def test_route_breadth_context_credits_live_sibling_setup_support():
    rows = [
        dict(
            _row("clsk_vwap", 114100.0, 100.0),
            routes=[{"match": {"ticker": "CLSK", "setup_type": "vwap_reclaim_breakdown"}}],
            learning_tags=["beats_live", "route_narrow", "promotion_weak"],
        ),
        dict(
            _row("riot_vwap", 114050.0, 50.0),
            routes=[{"match": {"ticker": "RIOT", "setup_type": "vwap_reclaim_breakdown"}}],
            learning_tags=["beats_live", "route_narrow", "promotion_weak"],
        ),
    ]

    enriched = hunt_runner._apply_route_breadth_context(rows)
    bar = hunt_runner._promotion_minimum_bar(enriched[0])

    assert enriched[0]["route_breadth_credit"]["passed"] is True
    assert "route_breadth_supported" in enriched[0]["learning_tags"]
    assert "route_too_narrow" not in bar["failures"]


def test_promotion_ready_repair_envelope_anchors_edge_and_close_siblings():
    blocker_digest = {
        "universal_blockers": [
            "promotion_readiness_below_floor",
            "route_too_narrow",
            "robustness_below_floor",
            "holdout_positive_but_not_promotion_grade",
        ]
    }
    winners = {
        "top_route": "CLSK|vwap_reclaim_breakdown|*",
        "top_route_share_pct": 25.0,
        "route_counts": {
            "CLSK|vwap_reclaim_breakdown|*": 9,
            "RIOT|vwap_reclaim_breakdown|late": 3,
        },
    }
    admission = {"preserved_focus_routes": ["CLSK|vwap_reclaim_breakdown|*"]}
    quality = hunt_runner._promotion_quality_protection_controls(
        {"scored_to_live_stream_yield_pct": 12.8},
        {"near_promotion_delta": {"improvement_points": 1.0}},
        blocker_digest,
        winners,
        admission,
    )
    handoff = hunt_runner._runtime_handoff_controls(
        admission,
        {"pressure": "low"},
        {"diversity_focus_routes": [], "max_sample_share_pct_by_route": {}, "max_winner_share_pct_by_route": {}},
        quality,
        {"recommended_focus_routes": ["CLSK|vwap_reclaim_breakdown|*"]},
        {"route_caps": []},
        {"allocation": []},
        {"promotion_ready_count": 0},
        winners,
        blocker_digest,
    )

    adapter = handoff["runtime_command_adapter"]
    envelope = adapter["promotion_ready_repair_envelope"]
    assert envelope["active"] is True
    assert envelope["edge_preservation_routes"] == ["CLSK|vwap_reclaim_breakdown|*"]
    assert "RIOT|vwap_reclaim_breakdown|*" in envelope["close_sibling_routes"]
    assert "promotion_readiness_below_floor" in envelope["target_failures"]
    assert "route_breadth" in envelope["target_failures"]
    assert adapter["max_sample_share_pct_by_route"]["CLSK|vwap_reclaim_breakdown|*"] == 18.0
    assert adapter["outside_repair_envelope_multiplier"] == 0.08
    assert adapter["commands"][0]["edge_preservation"] is True


def test_promotion_repair_envelope_prefers_promotion_survival_anchor():
    blocker_digest = {"universal_blockers": ["promotion_readiness_below_floor", "robustness_below_floor"]}
    winners = {
        "top_route": "CLSK|vwap_reclaim_breakdown|*",
        "route_counts": {
            "CLSK|vwap_reclaim_breakdown|*": 9,
            "*|vwap_reclaim_breakdown|*": 7,
        },
    }
    handoff = hunt_runner._runtime_handoff_controls(
        {"preserved_focus_routes": ["CLSK|vwap_reclaim_breakdown|*"]},
        {"pressure": "medium"},
        {"diversity_focus_routes": [], "max_sample_share_pct_by_route": {}, "max_winner_share_pct_by_route": {}},
        {
            "active": True,
            "quality_protection_mode": "medium",
            "target_failures": ["promotion_readiness_below_floor", "robustness_below_floor"],
            "protected_routes": ["CLSK|vwap_reclaim_breakdown|*"],
        },
        {"recommended_focus_routes": ["CLSK|vwap_reclaim_breakdown|*"]},
        {"route_caps": []},
        {"allocation": []},
        {"promotion_ready_count": 0},
        winners,
        blocker_digest,
        {
            "route_key": "*|vwap_reclaim_breakdown|*",
            "failures": ["promotion_readiness_below_floor"],
            "promotion_readiness_score": 34.8,
            "robustness_score": 58.9,
        },
    )

    envelope = handoff["runtime_command_adapter"]["promotion_ready_repair_envelope"]
    assert envelope["edge_preservation_routes"] == ["*|vwap_reclaim_breakdown|*"]
    assert envelope["near_candidate_finish_mode"] is True
    assert envelope["commands"][0]["primary_repair_lane"] == "promotion_ready_finish"
    assert "robustness_holdout_repair" in envelope["commands"][0]["secondary_repair_lanes"]
    assert "robustness_floor_repair" in envelope["commands"][0]["secondary_repair_lanes"]


def test_winner_distribution_treats_wildcard_siblings_as_on_objective():
    rows = [
        dict(
            _row("wild", 114100.0, 100.0),
            routes=[{"match": {"setup_type": "vwap_reclaim_breakdown"}}],
        ),
        dict(
            _row("clsk_open", 114050.0, 50.0),
            routes=[{"match": {"ticker": "CLSK", "setup_type": "vwap_reclaim_breakdown", "session_phase": "open"}}],
        ),
    ]

    distribution = hunt_runner._winner_route_distribution(rows, ["*|vwap_reclaim_breakdown|*"])

    assert distribution["off_objective_winner_count"] == 0
    assert distribution["off_objective_routes"] == []


def test_winner_distribution_uses_strict_cli_focus_for_split_workers():
    rows = [
        dict(
            _row("clsk_open", 114050.0, 50.0),
            routes=[{"match": {"ticker": "CLSK", "setup_type": "vwap_reclaim_breakdown", "session_phase": "open"}}],
        ),
        dict(
            _row("riot_midday", 114040.0, 40.0),
            routes=[{"match": {"ticker": "RIOT", "setup_type": "vwap_reclaim_breakdown", "session_phase": "midday"}}],
        ),
    ]

    distribution = hunt_runner._winner_route_distribution(
        rows,
        ["CLSK|vwap_reclaim_breakdown|open", "RIOT|vwap_reclaim_breakdown|midday"],
        strict_focus_routes=["RIOT|vwap_reclaim_breakdown|midday"],
    )

    assert distribution["off_objective_basis"] == "strict_cli_focus"
    assert distribution["off_objective_winner_count"] == 1
    assert distribution["off_objective_routes"] == ["CLSK|vwap_reclaim_breakdown|open"]


def test_readiness_component_diagnostics_explains_floor_bridge():
    row = _row("router_000001_CLSK|vwap_reclaim_breakdown|open", 114200.0, 320.0, trades=1300)
    row["promotion_readiness_score"] = 8.0
    row["learning_tags"] = ["beats_live", "promotion_weak", "sample_ok", "day_consistent", "ticker_balanced", "side_balanced", "route_broader"]
    row["routes"] = [{"match": {"ticker": "CLSK", "setup_type": "vwap_reclaim_breakdown", "session_phase": "open"}}]
    evidence = {
        "variant": row["variant"],
        "route_key": "CLSK|vwap_reclaim_breakdown|open",
        "robustness_score": 59.0,
        "red_flags": [],
        "train_holdout": {"holdout_delta_vs_active": 150.0, "holdout_pnl": 200.0},
        "day_profile": {"beats_active_day_rate": 0.90},
    }

    breakdown = hunt_runner._readiness_component_breakdown(row, evidence)
    diagnostics = hunt_runner._readiness_component_diagnostics([row], {row["variant"]: evidence})

    assert breakdown["passed_floor"] is True
    assert breakdown["components"]["positive_holdout"] == 12.0
    assert diagnostics["passed_floor_count"] == 1
    assert diagnostics["closest_to_floor"][0]["variant"] == row["variant"]


def test_no_winner_diagnostic_does_not_erase_quality_repair_targets():
    controls = {
        "promotion_quality_protection_controls": {
            "active": True,
            "quality_protection_mode": "medium",
            "target_failures": [],
        },
        "runtime_command_adapter": {
            "quality_protection_mode": "medium",
            "quality_protection_controls": {
                "active": True,
                "quality_protection_mode": "medium",
                "target_failures": [],
            },
            "commands": [
                {"route_key": "CLSK|vwap_reclaim_breakdown|*", "action": "repair", "target_failures": []}
            ],
        },
    }

    repaired = hunt_runner._repair_empty_quality_targets(controls)
    adapter = repaired["runtime_command_adapter"]

    assert adapter["quality_protection_controls"]["target_failures"] == [
        "promotion_readiness_below_floor",
        "robustness_below_floor",
    ]
    assert adapter["commands"][0]["target_failures"] == [
        "promotion_readiness_below_floor",
        "robustness_below_floor",
    ]
    assert adapter["route_breadth_required"] is False
    assert adapter["prefer_promotion_readiness"] is True


def test_router_candidate_generation_respects_wildcard_runtime_caps():
    active = step2_router_hunter.lab.Variant("active", {"ema": 1.0, "vwap": 1.0}, 0.0)
    seeds = [
        {"name": "RIOT|vwap_reclaim_breakdown|open", "match": {"ticker": "RIOT", "setup_type": "vwap_reclaim_breakdown", "session_phase": "open"}, "sample_weight": 100.0},
        {"name": "MARA|flow_exhaustion_fade|open", "match": {"ticker": "MARA", "setup_type": "flow_exhaustion_fade", "session_phase": "open"}, "sample_weight": 1.0},
    ]

    variants, _lineage = step2_router_hunter._candidates(
        active,
        seeds,
        step2_router_hunter.random.Random(9),
        0,
        50,
        0.1,
        4.0,
        {"runtime_command_adapter": {"max_sample_share_pct_by_route": {"RIOT|vwap_reclaim_breakdown|*": 8.0}}},
    )
    sampled = {}
    for variant in variants:
        route = step2_router_hunter._route_bucket_key(variant)
        sampled[route] = sampled.get(route, 0) + 1

    assert len(variants) == 50
    assert sampled.get("RIOT|vwap_reclaim_breakdown|open", 0) <= 4


def test_router_repair_controls_deprioritize_off_focus_generic_routes():
    seeds = [
        {"name": "focused", "match": {"ticker": "RIOT", "setup_type": "vwap_reclaim_breakdown", "session_phase": "open"}, "sample_weight": 10.0},
        {"name": "generic", "match": {"setup_type": "trend_pullback"}, "sample_weight": 10.0},
    ]
    quarantine = {
        "runtime_command_adapter": {
            "quality_protection_mode": "medium",
            "focus_routes": ["RIOT|vwap_reclaim_breakdown|*"],
            "quality_protection_controls": {
                "active": True,
                "quality_protection_mode": "medium",
                "protected_routes": ["RIOT|vwap_reclaim_breakdown|*"],
                "target_failures": ["promotion_readiness_below_floor", "robustness_below_floor"],
            },
            "commands": [
                {
                    "route_key": "RIOT|vwap_reclaim_breakdown|*",
                    "action": "repair",
                    "target_failures": ["promotion_readiness_below_floor", "robustness_below_floor"],
                }
            ],
        }
    }

    filtered = step2_router_hunter._filter_quarantined_seeds(seeds, quarantine)
    by_name = {seed["name"]: seed for seed in filtered}

    assert by_name["focused"]["quality_protection"] is True
    assert by_name["focused"]["sample_weight"] > by_name["generic"]["sample_weight"]
    assert by_name["generic"]["off_focus_repair_deprioritized"] is True
    assert not by_name["generic"].get("quality_protection")


def test_router_protected_focus_routes_override_conflicting_avoid_rules():
    seeds = [
        {"name": "focused", "match": {"ticker": "RIOT", "setup_type": "vwap_reclaim_breakdown"}, "sample_weight": 10.0},
        {"name": "generic", "match": {"ticker": "MARA"}, "sample_weight": 10.0},
    ]
    quarantine = {
        "runtime_command_adapter": {
            "avoid_routes": ["RIOT|vwap_reclaim_breakdown|*", "MARA|*|*"],
            "focus_routes": ["RIOT|vwap_reclaim_breakdown|*"],
            "quality_protection_mode": "medium",
            "quality_protection_controls": {
                "active": True,
                "quality_protection_mode": "medium",
                "protected_routes": ["RIOT|vwap_reclaim_breakdown|*"],
                "target_failures": ["promotion_readiness_below_floor"],
            },
            "commands": [
                {
                    "route_key": "RIOT|vwap_reclaim_breakdown|*",
                    "action": "repair",
                    "target_failures": ["promotion_readiness_below_floor"],
                }
            ],
        }
    }

    filtered = step2_router_hunter._filter_quarantined_seeds(seeds, quarantine)

    assert [seed["name"] for seed in filtered] == ["focused"]
    assert filtered[0]["protected_runtime_focus"] is True
    assert filtered[0]["quality_protection"] is True


def test_router_seed_weight_honors_runtime_throttle_routes():
    seed = {"match": {"ticker": "MARA", "setup_type": "trend_pullback", "session_phase": "open"}, "sample_weight": 10.0}
    state = {
        "runtime_command_adapter": {
            "throttle_routes": ["MARA|trend_pullback|open"],
            "throttle_multipliers": {"MARA|trend_pullback|open": 0.15},
        }
    }

    assert step2_router_hunter._seed_weight(seed, state) == 1.5


def test_variant_funnel_reads_scored_total_from_coordinator_child_runs():
    root = Path("runtime") / "test_variant_funnel_reads_scored_total_from_coordinator_child_runs"
    if root.exists():
        shutil.rmtree(root)
    try:
        child_summary = root / "child" / "summary.json"
        child_summary.parent.mkdir(parents=True)
        child_summary.write_text(json.dumps({"scored_total": 500, "winners": []}), encoding="utf-8")
        coordinator = root / "cycle" / "coordinator_summary.json"
        coordinator.parent.mkdir()
        coordinator.write_text(
            json.dumps({
                "runs": [{
                    "hunter": "router",
                    "summary_path": str(child_summary),
                    "stdout_tail": '{"scored_total": 500, "winners": 0}',
                }]
            }),
            encoding="utf-8",
        )
        args = type(
            "Args",
            (),
            {
                "exact_variant_count": True,
                "batch_size": 500,
                "max_batches": 1,
                "runtime_control_mode": "enforce",
                "smoke_validation_mode": False,
                "tiny_run_threshold": 1000,
                "limited_learning_min_variants": 500,
            },
        )()
        cycle = {
            "cycle": 0,
            "hunter": "router",
            "hunt_mode": "repair_exploration",
            "cmd": ["python", "step2_hunt_coordinator.py", "--batch-size", "500", "--max-batches", "1"],
            "stdout_tail": json.dumps({"path": str(coordinator)}),
        }

        funnel = hunt_runner._variant_funnel(args, root, [cycle], {"input_rows": 0})

        assert funnel["requested_variants"] == 500
        assert funnel["scored_total"] == 500
        assert funnel["count_contract_ok"] is True
        assert "reported_winners" in funnel["count_definitions"]
        assert funnel["cycle_learning_tiers"][0]["scored_total"] == 500
        log = hunt_runner._cycle_log_payload([cycle])
        assert "coordinator_payload" not in log["cycles"][0]
        assert log["cycles"][0]["reported_scored_total"] == 500
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_behavioral_dedupe_keeps_aliases_under_behavior_leader():
    rows = [
        _row("alias_a", 101.0, 1.0),
        _row("alias_b", 101.0, 1.0),
        _row(
            "distinct",
            100.5,
            0.5,
            by_day={"2026-05-01": {"pnl": 50.0, "trades": 10}, "2026-05-02": {"pnl": 50.5, "trades": 10}},
        ),
    ]
    rows[1]["weights"] = {"ema": 1.1, "vwap": 2.0}
    rows[2]["weights"] = {"ema": 1.0, "vwap": 2.2}

    ranked = hunt_intel.rank_rows(rows, live_only=True, behavioral_dedupe=True, limit=10)

    assert ranked["config_unique_rows"] == 3
    assert ranked["behavior_unique_rows"] == 2
    assert ranked["raw_leaderboard"][0]["behavior_alias_count"] == 2


def test_learning_report_includes_bandit_curriculum_and_failure_taxonomy():
    winner = _row("router_000001_CLSK|trend_pullback|late", 110.0, 10.0)
    winner["routes"] = [{"match": {"ticker": "CLSK", "setup_type": "trend_pullback", "session_phase": "late"}}]
    loser = _row("loser", 90.0, -10.0)
    cycles = [{"hunter": "router", "stdout_tail": '{"scored_total": 1000, "winners": 3}'}]

    ranked = hunt_intel.rank_rows([winner, loser], live_only=True, behavioral_dedupe=True, limit=10)
    report = hunt_intel.learning_report(
        rows=ranked["raw_leaderboard"],
        cycles=cycles,
        all_rows=[hunt_intel.decorate_row(winner), hunt_intel.decorate_row(loser)],
        source="test",
    )

    assert report["bandit_allocation"]["allocation"][0]["route_key"] == "CLSK|trend_pullback|late"
    assert report["curriculum_state"]["behavior_unique_live_beaters"] == 1
    assert report["failure_taxonomy"]["failure_counts"]["does_not_beat_live"] == 1
    assert report["experiment_memory"]["hunter_memory"][0]["hunter"] == "router"


def test_validation_aware_learning_adds_scheduler_and_family_clusters():
    winner = _row("router_000001_CLSK|trend_pullback|late", 110.0, 10.0)
    winner["routes"] = [{"match": {"ticker": "CLSK", "setup_type": "trend_pullback", "session_phase": "late"}}]
    ranked = hunt_intel.rank_rows([winner], live_only=True, behavioral_dedupe=True, limit=10)
    report = hunt_intel.learning_report(
        rows=ranked["raw_leaderboard"],
        validation_rows=[
            {
                "kind": "counterfactual",
                "route_key": "CLSK|trend_pullback|late",
                "route_disabled_delta": -20.0,
                "causal_route_signal": True,
                "passed": True,
            }
        ],
        source="test",
    )

    assert report["validation_aware_bandit"]["allocation"][0]["route_key"] == "CLSK|trend_pullback|late"
    assert report["auto_validation_scheduler"]["queue"][0]["route_key"] == "CLSK|trend_pullback|late"
    assert report["candidate_family_clusters"]["families"]


def test_promotion_readiness_tags_near_misses_and_worker_roles():
    row = _row("router_000001_CLSK|trend_pullback|late", 120.0, 20.0, trades=1200)
    row["routes"] = [{"match": {"ticker": "CLSK", "setup_type": "trend_pullback", "session_phase": "late"}}]
    row["robustness"] = {
        "score": 58.0,
        "adjusted_score": 57.0,
        "components": {
            "holdout": 0.5,
            "day_consistency": 20.0,
            "ticker_balance": 10.0,
            "side_balance": 10.0,
            "trade_sample": 5.0,
        },
        "red_flags": [],
    }

    ranked = hunt_intel.rank_rows([row], live_only=True, behavioral_dedupe=True, limit=10)
    leader = ranked["raw_leaderboard"][0]
    report = hunt_intel.learning_report(rows=ranked["raw_leaderboard"], all_rows=ranked["decorated_rows"], source="test")

    assert leader["promotion_readiness_score"] > 0
    assert "thin_holdout_edge" in leader["learning_tags"]
    assert report["annotated_top100"]["top"][0]["promotion_readiness_score"] == leader["promotion_readiness_score"]
    assert report["near_miss_archive"]["near_misses"]
    assert report["adaptive_mutation_lanes"]["lanes"]
    assert len(report["worker_role_plan"]["roles"]) == 4


def test_lineage_graph_and_family_rejection_memory():
    row = _row("child", 130.0, 30.0, trades=1200)
    row["lineage"] = {
        "parent_variant": "parent",
        "parent_behavior_key": "parent_behavior",
        "mutation_lane": "holdout_repair",
        "worker_role": "worker_2",
        "generation": 2,
        "route_seed": "CLSK|trend_pullback|late",
        "ancestor_path": ["root", "parent"],
    }
    ranked = hunt_intel.rank_rows([row], live_only=True, behavioral_dedupe=True, limit=10)
    leader = ranked["raw_leaderboard"][0]
    feedback = [{
        "variant": "child",
        "family_key": leader["family_key"],
        "status": "reject",
        "reject_reasons": ["robustness_score_below_70"],
    }]
    report = hunt_intel.learning_report(rows=ranked["raw_leaderboard"], feedback_rows=feedback, source="test")
    graph = report["candidate_lineage_graph"]
    memory = report["family_rejection_memory"]

    assert leader["lineage"]["parent_variant"] == "parent"
    assert any(edge["type"] == "parent_of" for edge in graph["edges"])
    assert memory["families"][0]["recommended_action"] == "downweight_family"


def test_active_experiment_planner_and_outcome_ledger():
    row = _row("near_miss", 125.0, 25.0, trades=1200)
    row["routes"] = [{"match": {"ticker": "CLSK", "setup_type": "trend_pullback", "session_phase": "late"}}]
    row["robustness"] = {
        "score": 58.0,
        "adjusted_score": 57.0,
        "components": {
            "holdout": 0.5,
            "day_consistency": 20.0,
            "ticker_balance": 10.0,
            "side_balance": 10.0,
            "trade_sample": 5.0,
        },
        "red_flags": [],
    }
    ranked = hunt_intel.rank_rows([row], live_only=True, behavioral_dedupe=True, limit=10)
    leader = ranked["raw_leaderboard"][0]
    feedback = [{
        "variant": "near_miss",
        "family_key": leader["family_key"],
        "status": "reject",
        "reject_reasons": ["robustness_score_below_70"],
        "learning_tags": leader["learning_tags"],
    }]
    report = hunt_intel.learning_report(rows=ranked["raw_leaderboard"], feedback_rows=feedback, source="test")
    plan = report["active_experiment_plan"]
    ledger = report["experiment_outcome_ledger"]

    assert plan["experiments"]
    assert any(exp["kind"] in {"ab_mutation_radius", "counterfactual_family_repair"} for exp in plan["experiments"])
    assert ledger["outcomes"]
    assert ledger["outcomes"][0]["next_action"] in {"run_experiment", "downweight_or_change_treatment", "continue_small_probe", "scale_successful_treatment"}


def test_treatment_effects_update_priors_and_worker_budget():
    holdout = _row("holdout_child", 140.0, 40.0, trades=1200)
    holdout["lineage"] = {"mutation_lane": "holdout_repair", "route_seed": "CLSK|trend_pullback|late"}
    holdout["robustness"] = {
        "score": 75.0,
        "adjusted_score": 74.0,
        "components": {"holdout": 18.0, "day_consistency": 20.0, "ticker_balance": 10.0, "side_balance": 10.0, "trade_sample": 5.0},
        "red_flags": [],
    }
    wide = _row("wide_child", 115.0, 15.0, trades=1200)
    wide["lineage"] = {"mutation_lane": "wild_shuffle", "route_seed": "CLSK|trend_pullback|late"}
    wide["robustness"] = {
        "score": 60.0,
        "adjusted_score": 59.0,
        "components": {"holdout": 3.0, "day_consistency": 12.0, "ticker_balance": 10.0, "side_balance": 10.0, "trade_sample": 5.0},
        "red_flags": [],
    }
    ranked = hunt_intel.rank_rows([holdout, wide], live_only=True, behavioral_dedupe=True, limit=10)
    report = hunt_intel.learning_report(
        rows=ranked["raw_leaderboard"],
        feedback_rows=[{
            "variant": "wide_child",
            "status": "reject",
            "reject_reasons": ["robustness_score_below_70"],
            "learning_tags": ["high_overfit_risk"],
            "lineage": {"mutation_lane": "wild_shuffle"},
        }],
        source="test",
    )

    treatments = {row["treatment"]: row for row in report["treatment_effects"]["treatments"]}
    assert "holdout_repair" in treatments
    assert report["treatment_prior_model"]["priors"]
    assert report["treatment_worker_budget"]["workers"]
    assert report["treatment_effects"]["best_treatment"] in treatments


def test_learning_report_adds_confidence_siblings_autopsy_regime_and_portfolio():
    row = _row(
        "router_000002_CLSK|trend_pullback|late",
        150.0,
        50.0,
        trades=1200,
        by_day={
            "2026-05-01": {"pnl": 120.0, "trades": 600},
            "2026-05-02": {"pnl": 30.0, "trades": 600},
        },
    )
    row["routes"] = [{"match": {"ticker": "CLSK", "setup_type": "trend_pullback", "session_phase": "late"}}]
    row["lineage"] = {"mutation_lane": "holdout_repair", "worker_role": "worker_2", "route_seed": "CLSK|trend_pullback|late"}
    row["robustness"] = {
        "score": 62.0,
        "adjusted_score": 61.0,
        "components": {"holdout": 2.0, "day_consistency": 9.0, "ticker_balance": 10.0, "side_balance": 10.0, "trade_sample": 5.0},
        "red_flags": [],
    }
    ranked = hunt_intel.rank_rows([row], live_only=True, behavioral_dedupe=True, limit=10)
    leader = ranked["raw_leaderboard"][0]
    feedback = [{
        "variant": leader["variant"],
        "route_key": "CLSK|trend_pullback|late",
        "status": "reject",
        "reject_reasons": ["robustness_score_below_70"],
        "learning_tags": leader["learning_tags"],
    }]
    report = hunt_intel.learning_report(rows=ranked["raw_leaderboard"], feedback_rows=feedback, source="test")

    assert report["treatment_confidence_model"]["decisions"]
    assert report["controlled_parent_sibling_experiments"]["experiments"][0]["kind"] == "controlled_parent_sibling"
    assert report["live_beater_failure_autopsy"]["autopsies"][0]["recommended_repair_lane"] in {"holdout_repair", "day_consistency_repair", "robustness_first"}
    assert report["worker_specialization_memory"]["workers"][0]["worker"] == "worker_2"
    assert report["regime_aware_learning"]["regimes"][0]["regime_key"].startswith("CLSK|trend_pullback|late")
    assert report["promotion_reject_simulator"]["predictions"][0]["predicted_reject_probability"] > 0
    assert {row["bucket"] for row in report["search_portfolio_manager"]["allocations"]} == {
        "exploit_best_treatments",
        "repair_near_misses",
        "controlled_experiments",
        "weird_exploration",
    }


def test_expected_promotable_objective_and_research_lab_artifacts():
    ready = _row("ready", 130.0, 30.0, trades=1500)
    ready["weights"] = {"ema": 1.0, "vwap": 2.0}
    ready["robustness"] = {
        "score": 82.0,
        "adjusted_score": 82.0,
        "components": {"holdout": 18.0, "day_consistency": 24.0, "ticker_balance": 10.0, "side_balance": 10.0, "trade_sample": 5.0},
        "red_flags": [],
    }
    risky = _row("risky", 180.0, 80.0, trades=100)
    risky["weights"] = {"ema": 1.5, "vwap": 2.0}
    risky["robustness"] = {
        "score": 35.0,
        "adjusted_score": 35.0,
        "components": {"holdout": 0.0, "day_consistency": 3.0, "ticker_balance": 2.0, "side_balance": 2.0, "trade_sample": 1.0},
        "red_flags": ["thin_holdout"],
    }

    ranked = hunt_intel.rank_rows([ready, risky], live_only=True, behavioral_dedupe=True, limit=10)
    report = hunt_intel.learning_report(rows=ranked["raw_leaderboard"], cycles=[{"hunter": "adaptive", "stdout_tail": '{"scored_total":100,"winners":2}'}], source="test")

    assert ranked["expected_promotable_leaderboard"][0]["variant"] == "ready"
    assert ranked["expected_promotable_leaderboard"][0]["expected_promotable_pnl"] > 0
    assert report["causal_experiment_registry"]["experiments"]
    assert "queue" in report["experiment_debt_queue"]
    assert "actions" in report["information_gain_scoring"]
    assert "plans" in report["value_of_information_planner"]
    assert "changes" in report["decision_change_tracker"]
    assert "hypotheses" in report["hypothesis_quality_scoring"]
    assert "gates" in report["evidence_sufficiency_gate"]
    assert report["counterfactual_shadow_board"]["tasks"]
    assert report["learning_velocity_dashboard"]["learning_velocity_score"] >= 0
    assert report["prediction_calibration_ledger"]["calibration_confidence"] > 0
    assert report["belief_revision_engine"]["truth_calibration_confidence"] > 0
    assert report["out_of_distribution_detector"]["detections"]
    assert report["adversarial_red_team_learner"]["tasks"]
    assert "durable_rules" in report["memory_compression_distiller"]
    assert report["self_audit_score"]["self_audit_score"] >= 0
    assert report["truth_first_promotion_objective"]["leaderboard"]
    assert report["compiled_hunt_policy"]["objective"] == "maximize_truth_first_promotable_pnl"
    assert report["policy_executor"]["jobs"]
    assert report["adaptive_worker_assignment"]["assignments"]
    assert report["policy_backtester"]["verdict"] in {"policy_improves_selection", "policy_needs_review"}
    assert report["policy_mutation_engine"]["policies"]
    assert report["policy_tournament"]["champion"]
    assert report["champion_challenger_memory"]["champion"]
    assert "policies" in report["regime_specific_policies"]
    assert report["causal_graph_of_learning"]["nodes"]
    assert report["policy_safety_rail"]["ok"] is True
    assert report["auto_promoted_field_manual"]["rules"]
    assert report["policy_drift_detector"]["status"] in {"stable", "watch", "drifting"}
    assert report["route_regime_half_life"]["routes"]
    assert report["learning_market_map"]["cells"]
    assert "alarms" in report["concept_drift_alarms"]
    assert "queue" in report["revalidation_scheduler"]
    assert report["temporal_ensemble_policy"]["components"]
    assert report["active_experiment_governor"]["decisions"]
    assert report["route_state_machine"]["states"]
    assert "patterns" in report["negative_knowledge_bank"]
    assert report["promotion_survivor_model"]["predictions"]
    assert report["mutation_grammar_learner"]["grammar"]
    assert report["real_time_worker_rebalancer"]["assignments"]
    assert report["hunt_replay_simulator"]["simulations"]
    assert "repair_queue" in report["resurrection_engine"]
    assert "attributions" in report["causal_mutation_attribution"]
    assert report["uncertainty_budgeting"]["budgets"]
    assert report["promotability_pareto_frontier"]["frontier"]
    assert "lessons" in report["false_lesson_detector"]
    assert report["experiment_graduation_system"]["experiments"]
    assert "diffs" in report["candidate_genealogy_diff_engine"]
    assert report["off_policy_hunt_evaluator"]["evaluations"]
    assert report["self_competition_league"]["leaderboard"]
    assert "contracts" in report["evidence_contract_engine"]
    assert report["live_beater_quality_decomposer"]["candidates"]
    assert "contradictions" in report["contradiction_detector"]
    assert report["learning_conflict_resolver"]["actions"]
    assert report["cohort_based_memory"]["cohorts"]
    assert report["adaptive_hunt_throttle"]["controls"]
    assert report["promotion_readiness_simulator"]["simulations"]
    assert report["research_trace_ledger"]["entries"]
    assert report["runtime_decision_kernel"]["command_packet"]["live_only"] is True
    assert "outcomes" in report["action_outcome_tracker"]
    assert report["closed_loop_reward_model"]["action_rewards"]
    assert "jobs" in report["autonomous_hunt_planner"]
    assert report["runtime_guardrails"]["guarded_command_packet"]["live_only"] is True
    assert "entries" in report["command_replay_ledger"]
    assert report["action_elo_league"]["leaderboard"]
    assert report["human_readable_hunt_brief"]["brief"]
    assert "assignments" in report["ab_route_experiment_executor"]
    assert report["champion_challenger_runtime_slots"]["slots"]
    assert "decisions" in report["adaptive_experiment_stopping"]
    assert "simulations" in report["counterfactual_command_replay"]
    assert "risks" in report["experiment_contamination_guard"]
    assert report["learning_rate_controller"]["mode"]
    assert report["worker_learning_report_cards"]["cards"]
    assert "traces" in report["experiment_to_promotion_trace"]
    assert "autopsies" in report["zero_yield_autopsy_engine"]
    assert "resets" in report["stuck_loop_breaker"]
    assert "costs" in report["opportunity_cost_meter"]
    assert "route_coverage" in report["search_space_coverage_map"]
    assert report["live_beater_scarcity_mode"]["mode"] in {"broad_discovery", "normal_truth_first"}
    assert "traps" in report["alias_trap_detector"]
    assert "scores" in report["route_seed_quality_score"]
    assert "interventions" in report["recovery_playbook_generator"]
    assert report["meta_hunt_strategy_learner"]["strategies"]
    assert "memo" in report["run_to_run_postmortem"]
