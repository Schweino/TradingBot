from argparse import Namespace

import promotion_gate
import step2_hunt_intelligence as hunt_intel
import step2_quote_aware_guard
import step2_statistical_validation
import step2_profit_combo_hunter as hunter


def _row(name, pnls, trades=180):
    total = sum(pnls)
    return {
        "variant": name,
        "weights": {"ema": 1.0},
        "bias": 0.0,
        "step2_pnl": total,
        "step2_delta_vs_active": total,
        "step2_trades": trades,
        "by_day": {
            f"2026-05-{idx + 1:02d}": {"pnl": pnl, "trades": max(1, trades // max(1, len(pnls)))}
            for idx, pnl in enumerate(pnls)
        },
        "by_ticker": {
            "CLSK": {"pnl": total * 0.45, "trades": trades // 2},
            "MARA": {"pnl": total * 0.35, "trades": trades // 3},
            "RIOT": {"pnl": total * 0.20, "trades": max(1, trades - (trades // 2) - (trades // 3))},
        },
        "by_side": {"LONG": {"pnl": total * 0.55, "trades": trades // 2}, "SHORT": {"pnl": total * 0.45, "trades": trades // 2}},
        "holdout_gate": {"ok": total > 0, "holdout_pnl": pnls[-1], "holdout_days": ["2026-05-04"]},
        "exit_replay_model": step2_quote_aware_guard.REQUIRED_EXIT_REPLAY_MODEL,
    }


def test_candidate_validation_promotes_broad_repeatable_edge():
    row = _row("steady", [100.0, 105.0, 95.0, 110.0, 102.0, 98.0, 107.0, 103.0, 101.0, 109.0], trades=240)

    validation = step2_statistical_validation.candidate_validation(row, tested_count=5, min_trades=100)

    assert validation["promotion_grade"] is True
    assert validation["score"] >= 75.0
    assert validation["multiple_testing_adjusted_p_value"] <= 0.05
    assert validation["promotion_checks"]["promotion_min_day_sample"] is True
    assert validation["promotion_checks"]["lower_95_day_mean_positive"] is True
    assert validation["blockers"] == []


def test_candidate_validation_blocks_one_day_wonder_after_multiple_testing():
    row = _row("fragile", [500.0, -20.0, -30.0, -40.0])

    validation = step2_statistical_validation.candidate_validation(row, tested_count=500, min_trades=100)

    assert validation["promotion_grade"] is False
    assert "positive_day_rate" in validation["blockers"]
    assert "multiple_testing_adjusted" in validation["blockers"]
    assert validation["leave_one_day_min_pnl"] < 0


def test_short_clean_edge_is_research_grade_not_promotion_grade():
    row = _row("short_clean", [100.0, 105.0, 95.0, 110.0], trades=220)

    validation = step2_statistical_validation.candidate_validation(row, tested_count=5, min_trades=100)

    assert validation["research_grade"] is True
    assert validation["promotion_grade"] is False
    assert "promotion_min_day_sample" in validation["promotion_blockers"]
    assert "promotion_min_day_sample" in validation["blockers"]


def test_rank_rows_exposes_statistical_validation_leaderboard():
    steady = _row("steady", [30.0, 32.0, 29.0, 31.0, 33.0, 30.0, 28.0, 32.0, 31.0, 29.0], trades=240)
    flashy = _row("flashy", [500.0, -20.0, -30.0, -40.0])

    ranked = hunt_intel.rank_rows([flashy, steady], live_only=True, behavioral_dedupe=False, limit=10)

    assert ranked["statistical_validation_version"] == step2_statistical_validation.VALIDATION_VERSION
    assert ranked["raw_leaderboard"][0]["variant"] == "flashy"
    assert ranked["statistical_validation_leaderboard"][0]["variant"] == "steady"
    assert ranked["statistical_validation_report"]["candidate_count"] == 2


def test_profit_combo_report_uses_multiple_testing_count():
    rows = [
        _row("steady", [30.0, 32.0, 29.0, 31.0, 33.0, 30.0, 28.0, 32.0, 31.0, 29.0], trades=240),
        _row("fragile", [500.0, -20.0, -30.0, -40.0]),
    ]
    hunter._decorate(rows, active_pnl=0.0, args=Namespace(min_trades=100, start_balance=100000.0, target_pnl=0.0))

    report = step2_statistical_validation.report(rows, min_trades=100, source="test")

    assert report["candidate_count"] == 2
    assert report["leaderboard"][0]["variant"] == "steady"
    assert rows[0]["statistical_validation"]["tested_count"] == 2


def test_promotion_gate_blocks_present_failed_statistical_validation():
    candidate = {
        "name": "candidate",
        "weights": {"ema": 1.0},
        "bias": 0.0,
        "exit_replay_model": step2_quote_aware_guard.REQUIRED_EXIT_REPLAY_MODEL,
        "statistical_validation": {"promotion_grade": False, "score": 12.0, "blockers": ["multiple_testing_adjusted"]},
    }

    payload = promotion_gate.evaluate(candidate, days=[], require_lifecycle=False)

    failed = {row["name"] for row in payload["checks"] if not row.get("ok")}
    assert "statistical_validation_promotion_grade" in failed
