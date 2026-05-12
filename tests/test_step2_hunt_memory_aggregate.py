import json
import shutil
from pathlib import Path

import aggregate_step2_hunt_memory as memory


def test_aggregate_promotion_ready_requires_current_route_safety():
    base_dir = Path("runtime") / "test_step2_hunt_memory_aggregate"
    shutil.rmtree(base_dir, ignore_errors=True)
    run_dir = base_dir / "step2_4h_livelearn_seg001_slice001_20260510_000000"
    run_dir.mkdir(parents=True, exist_ok=True)
    row = {
        "variant": "router",
        "step2_pnl": 200.0,
        "step2_delta_vs_active": 100.0,
        "beats_current_live": True,
        "promotion_distance": {"passed": True},
        "routes": [
            {"name": "current_route", "match": {"ticker": "CLSK"}, "action": "force_short"}
        ],
        "route_audit": {
            "routes": [
                {"route": "stale_route", "matched_opportunities": 100, "opportunity_share_pct": 10.0, "skipped_sides": 0}
            ]
        },
    }
    (run_dir / "promotion_survival_top100.json").write_text(json.dumps([row]), encoding="utf-8")

    result = memory.aggregate_top100(base_dir, ["step2_4h_livelearn_"], limit=100)

    assert result["top100_count"] == 1
    assert result["promotion_ready_in_top100"] == 0
    assert result["leaderboard"][0]["memory_routed_profile_safety_gate"]["ok"] is False
    assert result["leaderboard"][0]["memory_route_audit_identity_status"] == "stale_audit_removed"
    assert "route_audit" not in result["leaderboard"][0]
    assert result["route_audit_sanitized_in_top100"] == 1


def test_aggregate_writes_operator_summary_fields_and_lanes():
    base_dir = Path("runtime") / "test_step2_hunt_memory_operator_summary"
    shutil.rmtree(base_dir, ignore_errors=True)
    run_dir = base_dir / "step2_4h_livelearn_seg001_slice001_20260510_000000"
    run_dir.mkdir(parents=True, exist_ok=True)
    row = {
        "variant": "router_001_RIOT|trend_pullback|late",
        "step2_pnl": 300.0,
        "step2_delta_vs_active": 120.0,
        "beats_current_live": True,
        "promotion_readiness_score": 77.0,
        "evidence_adjusted_promotion_readiness_score": 80.0,
        "promotion_distance": {"passed": True, "gates": [{"gate": "beats_current_live", "passed": True}]},
        "routes": [
            {"name": "score_RIOT", "match": {"ticker": "RIOT", "setup_type": "trend_pullback", "session_phase": "late"}, "action": "score"}
        ],
        "route_audit": {
            "routes": [
                {"route": "score_RIOT", "matched_opportunities": 100, "opportunity_share_pct": 10.0, "skipped_sides": 0}
            ]
        },
    }
    non_live = dict(row, variant="router_002_RIOT|trend_pullback|late", step2_pnl=100.0, step2_delta_vs_active=-1.0, beats_current_live=False)
    (run_dir / "promotion_survival_top100.json").write_text(json.dumps([row, non_live]), encoding="utf-8")
    (run_dir / "final_summary.json").write_text("{}", encoding="utf-8")

    result = memory.aggregate_top100(base_dir, ["step2_4h_livelearn_"], limit=100)

    assert result["schema_version"] == 2
    assert result["rank1"] == "router_001_RIOT|trend_pullback|late"
    assert result["rank1_pnl"] == 300.0
    assert result["clean_rank1"]["variant"] == "router_001_RIOT|trend_pullback|late"
    assert result["best_promotion_ready"]["variant"] == "router_001_RIOT|trend_pullback|late"
    assert result["leaderboard"][0]["promotion_distance_passed"] is True
    assert result["leaderboard"][0]["safety_status"] == "ok"
    assert result["leaderboard"][0]["candidate_status"] == "promotion_candidate"
    assert result["leaderboard"][0]["route_key"] == "RIOT|trend_pullback|late"
    assert result["rejected_non_live_rows"] == 1
    assert result["lane_summary"][0]["route_key"] == "RIOT|trend_pullback|late"
    assert result["promotion_queue"][0]["variant"] == "router_001_RIOT|trend_pullback|late"


def test_companion_outputs_are_written():
    out_dir = Path("runtime") / "test_step2_hunt_memory_companions"
    shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    result = {
        "schema_version": 2,
        "source": "test",
        "rank1": "v",
        "rank1_pnl": 1,
        "top100_count": 1,
        "promotion_ready_in_top100": 1,
        "routed_safety_blocked_in_top100": 0,
        "compact_top100": [{"variant": "v"}],
        "lane_summary": [{"route_key": "R|s|p"}],
        "promotion_queue": [{"variant": "v"}],
        "memory_health": {"top100_full": False},
        "source_run_health": [],
    }

    written = memory._write_companion_outputs(out_dir / "memory.json", result)

    assert set(written) == {"compact", "by_lane", "promotion_queue", "health"}
    assert json.loads(Path(written["compact"]).read_text(encoding="utf-8"))["top100"][0]["variant"] == "v"
