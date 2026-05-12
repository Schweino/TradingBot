import unittest

import candidate_decision_brief as brief
import candidate_profile_schema


def _row(name, pnl, by_day, by_ticker, by_side, trades=1000):
    wins = int(trades * 0.72)
    losses = trades - wins
    return {
        "variant": name,
        "weights": {"ema": 1.0, "btc": -0.5},
        "bias": 0.0,
        "step2_pnl": pnl,
        "step2_trades": trades,
        "step2_win_rate_pct": round(wins / trades * 100, 2),
        "by_day": {day: {"pnl": value, "trades": max(1, trades // max(1, len(by_day)))} for day, value in by_day.items()},
        "by_ticker": {ticker: {"pnl": value, "trades": max(1, trades // max(1, len(by_ticker)))} for ticker, value in by_ticker.items()},
        "by_side": {
            side: {
                "pnl": value,
                "trades": max(1, trades // max(1, len(by_side))),
                "wins": max(1, wins // max(1, len(by_side))),
                "losses": max(1, losses // max(1, len(by_side))),
            }
            for side, value in by_side.items()
        },
    }


class CandidateDecisionBriefTests(unittest.TestCase):
    def setUp(self):
        self.active = _row(
            "live",
            1000,
            {"2026-04-06": 250, "2026-04-07": 250, "2026-04-08": 250, "2026-04-09": 250},
            {"CLSK": 350, "MARA": 350, "RIOT": 300},
            {"LONG": 550, "SHORT": 450},
            trades=800,
        )
        self.good = _row(
            "balanced",
            1600,
            {"2026-04-06": 400, "2026-04-07": 380, "2026-04-08": 410, "2026-04-09": 410},
            {"CLSK": 520, "MARA": 540, "RIOT": 540},
            {"LONG": 780, "SHORT": 820},
            trades=900,
        )
        self.fragile = _row(
            "fragile",
            1700,
            {"2026-04-06": 1900, "2026-04-07": -80, "2026-04-08": -60, "2026-04-09": -60},
            {"CLSK": 1850, "MARA": -70, "RIOT": -80},
            {"LONG": 1800, "SHORT": -100},
            trades=900,
        )

    def test_good_candidate_gets_review_when_not_approved(self):
        payload = {
            "script": "step2_adaptive_hunter.py",
            "start_balance": 100000,
            "active": self.active,
            "leaderboard": [self.good],
            "promotable": True,
            "candidate_contracts": candidate_profile_schema.contracts({}),
        }

        result = brief.build(
            payload=payload,
            variant="balanced",
            run_reproducibility=False,
            run_quarantine=False,
        )

        self.assertEqual(result["decision"]["label"], "REVIEW")
        self.assertTrue(result["decision"]["analytically_recommended"])
        self.assertIn("balanced", result["plain_english"])

    def test_fragile_candidate_gets_reject(self):
        payload = {
            "script": "step2_adaptive_hunter.py",
            "start_balance": 100000,
            "active": self.active,
            "leaderboard": [self.fragile],
            "promotable": True,
            "candidate_contracts": candidate_profile_schema.contracts({}),
        }

        result = brief.build(
            payload=payload,
            variant="fragile",
            run_reproducibility=False,
            run_quarantine=False,
        )

        self.assertEqual(result["decision"]["label"], "REJECT")
        self.assertIn("high_day_or_ticker_concentration", result["decision"]["reject_reasons"])

    def test_rankings_include_recommended_leaderboard_rows(self):
        payload = {
            "script": "step2_adaptive_hunter.py",
            "start_balance": 100000,
            "active": self.active,
            "leaderboard": [self.fragile],
            "recommended_leaderboard": [self.good],
            "promotable": True,
            "candidate_contracts": candidate_profile_schema.contracts({}),
        }

        result = brief.build(
            payload=payload,
            variant="balanced",
            run_reproducibility=False,
            run_quarantine=False,
        )

        self.assertEqual(result["selection"]["variant"], "balanced")
        self.assertEqual(result["rankings"]["best_recommended_variant"], "balanced")


if __name__ == "__main__":
    unittest.main()
