from unittest.mock import patch

import promotion_gate
import step2_hunt_intelligence as hunt_intel
import step2_quote_aware_guard
import step2_world_class_audit


def _row(name="steady", pnl=5000.0):
    return {
        "variant": name,
        "weights": {"ema": 1.0},
        "bias": 0.0,
        "step2_pnl": pnl,
        "step2_delta_vs_active": pnl,
        "step2_trades": 180,
        "by_day": {
            "2026-05-06": {"pnl": pnl * 0.25, "trades": 45},
            "2026-05-07": {"pnl": pnl * 0.25, "trades": 45},
            "2026-05-08": {"pnl": pnl * 0.25, "trades": 45},
            "2026-05-10": {"pnl": pnl * 0.25, "trades": 45},
        },
        "by_ticker": {
            "CLSK": {"pnl": pnl * 0.34, "trades": 60},
            "MARA": {"pnl": pnl * 0.33, "trades": 60},
            "RIOT": {"pnl": pnl * 0.33, "trades": 60},
        },
        "by_side": {
            "LONG": {"pnl": pnl * 0.5, "trades": 90},
            "SHORT": {"pnl": pnl * 0.5, "trades": 90},
        },
        "holdout_gate": {"ok": True, "holdout_pnl": pnl * 0.25},
        "exit_replay_model": step2_quote_aware_guard.REQUIRED_EXIT_REPLAY_MODEL,
    }


def test_readiness_report_blocks_missing_required_sections():
    report = step2_world_class_audit.readiness_report([_row()], payload={}, required_sections=("data_quality_report",))

    assert report["ok"] is False
    assert "missing_required_readiness_sections" in report["blockers"]
    assert report["missing_sections"] == ["data_quality_report"]


def test_rank_rows_exposes_world_class_readiness():
    ranked = hunt_intel.rank_rows([_row()], live_only=True, behavioral_dedupe=False, limit=10)

    assert ranked["world_class_readiness_report"]["audit_version"] == step2_world_class_audit.AUDIT_VERSION
    assert ranked["world_class_readiness_report"]["candidate_count"] == 1
    assert "world_class_candidate_audit" in hunt_intel.finalist_row(ranked["raw_leaderboard"][0], 1)


def test_payload_readiness_uses_profit_combo_required_sections():
    payload = {
        "script": "step2_profit_combo_hunter.py",
        "leaderboard": [_row()],
        "data_quality_report": {"learning_blocked_count": 0},
        "statistical_validation_report": {"learning_grade_count": 1},
        "deployment_risk_report": {"live_eligible_count": 1},
        "execution_adjusted_objective_report": {"candidate_count": 1},
        "closed_loop_causal_controller": {"next_run_directives": {"scale_routes": ["CLSK|x|late"]}},
        "artifact_trust_report": {"trust_state": "trusted"},
        "learning_ops_readiness_report": {"status": "ready"},
    }

    report = step2_world_class_audit.payload_readiness(payload)

    assert report["ok"] is True
    assert report["missing_sections"] == []


def test_promotion_gate_blocks_present_world_class_readiness_failure():
    candidate = {
        "name": "candidate",
        "weights": {"ema": 1.0},
        "bias": 0.0,
        "exit_replay_model": step2_quote_aware_guard.REQUIRED_EXIT_REPLAY_MODEL,
        "world_class_readiness": {"ok": False, "readiness_state": "blocked", "blockers": ["missing_required_readiness_sections"]},
    }
    with patch.object(promotion_gate.golden_parity_suite, "run", return_value={"ok": True, "results": []}), \
            patch.object(promotion_gate.contract_gate, "check", return_value={"ok": True, "critical_failure_count": 0}):
        payload = promotion_gate.evaluate(candidate, days=[], require_lifecycle=False)

    failed = {row["name"] for row in payload["checks"] if not row.get("ok")}
    assert "world_class_readiness_ok" in failed
