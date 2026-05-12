import step2_hunt_intelligence as hunt_intel
import step2_objective


def _row(name, pnl, trades, exit_model="quote_aware_exit_v1"):
    return {
        "variant": name,
        "weights": {"ema": 1.0},
        "bias": 0.0,
        "step2_pnl": pnl,
        "step2_delta_vs_active": pnl,
        "step2_trades": trades,
        "step2_win_rate_pct": 70.0,
        "by_day": {"2026-05-08": {"pnl": pnl, "trades": trades}},
        "by_ticker": {"CLSK": {"pnl": pnl, "trades": trades}},
        "exit_replay_model": exit_model,
    }


def test_execution_adjusted_objective_discounts_costs_and_requires_quote_aware():
    clean = _row("clean", 500.0, 10)
    invalid = _row("invalid", 500.0, 10, exit_model="legacy_midpoint_exit")

    clean_obj = step2_objective.candidate_objective(clean, start_balance=100000.0)
    invalid_obj = step2_objective.candidate_objective(invalid, start_balance=100000.0)

    assert clean_obj["replay_validity"]["ok"] is True
    assert clean_obj["execution_adjusted_pnl"] < clean_obj["gross_pnl"]
    assert invalid_obj["replay_validity"]["ok"] is False
    assert invalid_obj["execution_adjusted_pnl"] == 0.0


def test_rank_rows_exposes_execution_adjusted_leaderboard():
    high_gross_thin = _row("high_gross_thin", 500.0, 100)
    lower_gross_cleaner = _row("lower_gross_cleaner", 220.0, 5)

    ranked = hunt_intel.rank_rows(
        [high_gross_thin, lower_gross_cleaner],
        live_only=True,
        behavioral_dedupe=False,
        limit=10,
    )

    assert ranked["objective_version"] == step2_objective.OBJECTIVE_VERSION
    assert ranked["raw_leaderboard"][0]["variant"] == "high_gross_thin"
    assert ranked["execution_adjusted_leaderboard"][0]["variant"] == "lower_gross_cleaner"
    assert "execution_adjusted_objective" in ranked["execution_adjusted_leaderboard"][0]
