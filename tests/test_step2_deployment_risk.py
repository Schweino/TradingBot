from unittest.mock import patch

import promotion_gate
import step2_deployment_risk
import step2_hunt_intelligence as hunt_intel
import step2_quote_aware_guard


def _row(name="steady", pnl=5000.0, trades=180):
    return {
        "variant": name,
        "weights": {"ema": 1.0},
        "bias": 0.0,
        "step2_pnl": pnl,
        "step2_delta_vs_active": pnl,
        "step2_trades": trades,
        "by_day": {
            "2026-05-06": {"pnl": pnl * 0.25, "trades": trades // 4},
            "2026-05-07": {"pnl": pnl * 0.25, "trades": trades // 4},
            "2026-05-08": {"pnl": pnl * 0.25, "trades": trades // 4},
            "2026-05-10": {"pnl": pnl * 0.25, "trades": trades - 3 * (trades // 4)},
        },
        "by_ticker": {
            "CLSK": {"pnl": pnl * 0.35, "trades": trades // 3},
            "MARA": {"pnl": pnl * 0.35, "trades": trades // 3},
            "RIOT": {"pnl": pnl * 0.30, "trades": trades - 2 * (trades // 3)},
        },
        "by_side": {
            "LONG": {"pnl": pnl * 0.50, "trades": trades // 2},
            "SHORT": {"pnl": pnl * 0.50, "trades": trades // 2},
        },
        "holdout_gate": {"ok": True, "holdout_pnl": pnl * 0.25},
        "exit_replay_model": step2_quote_aware_guard.REQUIRED_EXIT_REPLAY_MODEL,
    }


def test_candidate_risk_allows_small_live_deployment_for_clean_candidate():
    ranked = hunt_intel.rank_rows([_row()], live_only=True, behavioral_dedupe=False, limit=10)
    leader = ranked["raw_leaderboard"][0]

    risk = step2_deployment_risk.candidate_risk(leader, start_balance=100000.0)

    assert risk["deployment_allowed"] is True
    assert risk["tier"] == "live_eligible"
    assert 0.0 < risk["max_capital_pct"] <= 5.0
    assert risk["exposure_caps"]["one_open_position_per_ticker"] is True


def test_candidate_risk_blocks_failed_statistics_and_closed_loop_retire():
    ranked = hunt_intel.rank_rows([_row()], live_only=True, behavioral_dedupe=False, limit=10)
    leader = ranked["raw_leaderboard"][0]
    leader["statistical_validation"] = {"promotion_grade": False, "score": 10.0, "blockers": ["multiple_testing_adjusted"]}

    risk = step2_deployment_risk.candidate_risk(
        leader,
        closed_loop_decision={"decision": "retire_or_quarantine"},
    )

    assert risk["deployment_allowed"] is False
    assert risk["tier"] == "blocked"
    assert "statistical_validation_not_promotion_grade" in risk["blockers"]
    assert "closed_loop_retire_or_quarantine" in risk["blockers"]


def test_rank_rows_exposes_deployment_risk_leaderboard_and_report():
    steady = _row("steady", pnl=5000.0)
    concentrated = _row("concentrated", pnl=5500.0)
    concentrated["by_ticker"] = {"CLSK": {"pnl": 5500.0, "trades": 180}}

    ranked = hunt_intel.rank_rows([concentrated, steady], live_only=True, behavioral_dedupe=False, limit=10)

    assert ranked["deployment_risk_report"]["candidate_count"] == 2
    assert ranked["raw_leaderboard"][0]["variant"] == "concentrated"
    assert ranked["deployment_risk_leaderboard"][0]["variant"] == "steady"
    assert "deployment_risk" in hunt_intel.finalist_row(ranked["deployment_risk_leaderboard"][0], 1)


def test_promotion_gate_blocks_present_deployment_risk_failure():
    candidate = {
        "name": "candidate",
        "weights": {"ema": 1.0},
        "bias": 0.0,
        "exit_replay_model": step2_quote_aware_guard.REQUIRED_EXIT_REPLAY_MODEL,
        "deployment_risk": {
            "deployment_allowed": False,
            "tier": "blocked",
            "blockers": ["execution_adjusted_pnl_not_positive"],
            "warnings": [],
        },
    }
    with patch.object(promotion_gate.golden_parity_suite, "run", return_value={"ok": True, "results": []}), \
            patch.object(promotion_gate.contract_gate, "check", return_value={"ok": True, "critical_failure_count": 0}):
        payload = promotion_gate.evaluate(candidate, days=[], require_lifecycle=False)

    failed = {row["name"] for row in payload["checks"] if not row.get("ok")}
    assert "deployment_risk_live_eligible" in failed
