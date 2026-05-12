import unittest
from pathlib import Path

import candidate_robustness_report as robustness


def _row(name, pnl, by_day, by_ticker, by_side, trades=1000):
    wins = int(trades * 0.7)
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


class CandidateRobustnessReportTests(unittest.TestCase):
    def setUp(self):
        self.active = _row(
            "live",
            1000,
            {"2026-04-06": 250, "2026-04-07": 250, "2026-04-08": 250, "2026-04-09": 250},
            {"CLSK": 350, "MARA": 350, "RIOT": 300},
            {"LONG": 550, "SHORT": 450},
            trades=800,
        )

    def test_balanced_candidate_scores_above_fragile_candidate(self):
        balanced = _row(
            "balanced",
            1600,
            {"2026-04-06": 400, "2026-04-07": 380, "2026-04-08": 410, "2026-04-09": 410},
            {"CLSK": 520, "MARA": 540, "RIOT": 540},
            {"LONG": 780, "SHORT": 820},
            trades=900,
        )
        fragile = _row(
            "fragile",
            1700,
            {"2026-04-06": 1900, "2026-04-07": -80, "2026-04-08": -60, "2026-04-09": -60},
            {"CLSK": 1850, "MARA": -70, "RIOT": -80},
            {"LONG": 1800, "SHORT": -100},
            trades=900,
        )

        balanced_report = robustness.evaluate_candidate(balanced, active_row=self.active)
        fragile_report = robustness.evaluate_candidate(fragile, active_row=self.active)

        self.assertGreater(balanced_report["robustness_score"], fragile_report["robustness_score"])
        self.assertTrue(balanced_report["recommendation"]["recommended"])
        self.assertFalse(fragile_report["recommendation"]["recommended"])
        self.assertIn("high_day_or_ticker_concentration", fragile_report["red_flags"])
        self.assertEqual(balanced_report["side_profile"]["available"], True)

    def test_report_separates_raw_and_adjusted_leaders(self):
        balanced = _row(
            "balanced",
            1600,
            {"2026-04-06": 400, "2026-04-07": 380, "2026-04-08": 410, "2026-04-09": 410},
            {"CLSK": 520, "MARA": 540, "RIOT": 540},
            {"LONG": 780, "SHORT": 820},
            trades=900,
        )
        fragile = _row(
            "fragile",
            1700,
            {"2026-04-06": 1900, "2026-04-07": -80, "2026-04-08": -60, "2026-04-09": -60},
            {"CLSK": 1850, "MARA": -70, "RIOT": -80},
            {"LONG": 1800, "SHORT": -100},
            trades=900,
        )

        report = robustness.build(rows=[fragile, balanced], active_row=self.active, top_n=2)

        self.assertEqual(report["summary"]["top_raw_variant"], "fragile")
        self.assertEqual(report["summary"]["top_adjusted_variant"], "balanced")
        self.assertEqual(report["summary"]["best_recommended_variant"], "balanced")
        self.assertEqual([row["variant"] for row in report["recommended"]], ["balanced"])

    def test_annotate_rows_attaches_compact_summary(self):
        rows = [_row(
            "candidate",
            1200,
            {"2026-04-06": 300, "2026-04-07": 300, "2026-04-08": 300, "2026-04-09": 300},
            {"CLSK": 400, "MARA": 400, "RIOT": 400},
            {"LONG": 600, "SHORT": 600},
        )]

        robustness.annotate_rows(rows, active_row=self.active)

        self.assertIn("robustness", rows[0])
        self.assertIn("score", rows[0]["robustness"])
        self.assertIn("components", rows[0]["robustness"])

    def test_build_from_payload_and_write_report(self):
        payload = {
            "script": "step2_adaptive_hunter.py",
            "start_balance": 100000,
            "active": self.active,
            "leaderboard": [
                _row(
                    "candidate",
                    1300,
                    {"2026-04-06": 330, "2026-04-07": 320, "2026-04-08": 310, "2026-04-09": 340},
                    {"CLSK": 430, "MARA": 430, "RIOT": 440},
                    {"LONG": 640, "SHORT": 660},
                )
            ],
        }

        report = robustness.build_from_payload(payload, top_n=1)
        self.assertEqual(report["row_count"], 1)
        self.assertEqual(report["summary"]["top_raw_variant"], "candidate")

        path = Path(__file__).resolve().parent / "__tmp_candidate_robustness.json"
        self.addCleanup(lambda: path.exists() and path.unlink())
        out = robustness.write_report(report, path)
        self.assertTrue(Path(out).exists())


if __name__ == "__main__":
    unittest.main()
