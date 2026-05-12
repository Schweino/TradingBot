import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import step2_adaptive_hunter as hunter


def _row(name, pnl, adjusted, recommended):
    return {
        "variant": name,
        "weights": {"ema": 1.0},
        "bias": 0.0,
        "step2_pnl": pnl,
        "robustness": {
            "score": adjusted,
            "adjusted_score": adjusted,
            "recommended": recommended,
            "recommendation_tier": "review" if recommended else "watch",
            "recommendation_blockers": [] if recommended else ["weak_day_consistency"],
            "red_flags": [] if recommended else ["weak_day_consistency"],
        },
    }


class Step2AdaptiveHunterRobustnessTests(unittest.TestCase):
    def test_recommended_rows_prefer_adjusted_score_not_raw_pnl(self):
        raw_high = _row("raw_high", 2000, 60, False)
        recommended = _row("recommended", 1800, 92, True)
        adjusted = _row("adjusted", 1900, 88, False)

        self.assertEqual(hunter._sort_adjusted([raw_high, recommended, adjusted])[0]["variant"], "recommended")
        self.assertEqual([row["variant"] for row in hunter._recommended_rows([raw_high, recommended, adjusted])], ["recommended"])

    def test_review_exposes_recommendation_context(self):
        row = _row("candidate", 1800, 92, True)

        review = hunter._review(row)

        self.assertTrue(review["recommended"])
        self.assertEqual(review["robustness_adjusted_score"], 92)
        self.assertEqual(review["recommendation_tier"], "review")

    def test_infers_single_manifest_day_for_certification(self):
        args = SimpleNamespace(certify_day="")

        self.assertEqual(
            hunter._infer_certification_day(args, {"day_map": {"2026-05-08": [0, 10]}}),
            "2026-05-08",
        )
        self.assertEqual(
            hunter._infer_certification_day(args, {"day_map": {"2026-05-07": [0, 1], "2026-05-08": [1, 2]}}),
            "",
        )

    def test_certify_cache_invokes_certifier_and_updates_manifest_path(self):
        root = Path.cwd()
        manifest = root / "compiled_one_day" / "manifest.json"
        certified = root / "compiled_one_day" / "manifest.certified.json"
        args = SimpleNamespace(
            certify_cache=True,
            compiled_decision_tape=str(manifest),
            certify_day="",
            certify_tickers=[],
            certify_feed="sip",
            certify_quote_mode="per-second",
            certify_btc_mode="bars",
            certify_indicator_mode="live",
            certify_workers=6,
            certify_overlap_sec=120,
            certify_step2_latency_mode="entry-exit",
            certify_step2_latency_model="C:/fake/latency.json",
            certify_step2_latency_percentile=0.75,
            certify_use_state_checkpoints=True,
        )
        receipt = {
            "ok": True,
            "source": "certify_step2_cache",
            "day": "2026-05-08",
            "tickers": ["CLSK", "MARA"],
            "selected_action": "score_only",
            "compiled_manifest": {"path": str(certified), "exists": True},
            "certification_hash": "hash",
        }
        with patch.object(hunter, "_read_json", return_value={"day_map": {"2026-05-08": [0, 10]}, "ticker_map": {"CLSK": 0, "MARA": 1}}), \
                patch.object(hunter, "_write_json", return_value=None), \
                patch.object(hunter.certify_step2_cache, "certify", return_value=receipt) as certify:
            payload = hunter._certify_cache_for_hunter(args, root)

        self.assertTrue(payload["ok"])
        self.assertEqual(str(certified), args.compiled_decision_tape)
        certify.assert_called_once()

    def test_disabled_certification_is_not_promotion_safe(self):
        args = SimpleNamespace(certify_cache=False)

        payload = hunter._certify_cache_for_hunter(args, Path("C:/fake/out"))

        self.assertFalse(payload["ok"])
        self.assertIn("cache_certification_disabled", payload["failure_reasons"])

    def test_resolver_updates_missing_manifest_path_to_certified_cache(self):
        args = SimpleNamespace(
            compiled_decision_tape="",
            cache_resolver=True,
            cache_tickers=["CLSK", "MARA", "RIOT"],
            cache_day="2026-05-08",
            cache_start="",
            cache_end="",
            certify_day="",
            certify_tickers=[],
            allow_uncertified_cache=False,
        )
        resolved = {
            "ok": True,
            "source": "step2_cache_catalog",
            "manifest_path": "C:/fake/compiled/certified/manifest.json",
            "selected": {
                "name": "certified",
                "start_day": "2026-05-08",
                "end_day": "2026-05-08",
                "source_days": ["2026-05-08"],
                "row_count": 100,
                "manifest": {"sha256": "sha"},
                "certification": {"valid": True, "ok": True, "path": "C:/fake/cert.json"},
            },
            "blockers": [],
            "requested": {"tickers": ["CLSK", "MARA", "RIOT"], "day": "2026-05-08"},
        }
        with patch.object(hunter.step2_cache_catalog, "resolve", return_value=resolved) as resolve, \
                patch.object(hunter, "_write_json", return_value=None):
            payload = hunter._resolve_compiled_cache_for_hunter(args, Path.cwd())

        self.assertTrue(payload["ok"])
        self.assertEqual(args.compiled_decision_tape, "C:/fake/compiled/certified/manifest.json")
        resolve.assert_called_once()

    def test_resolver_failure_is_non_ok_when_no_certified_cache_exists(self):
        args = SimpleNamespace(
            compiled_decision_tape="",
            cache_resolver=True,
            cache_tickers=["CLSK", "MARA", "RIOT"],
            cache_day="2026-05-08",
            cache_start="",
            cache_end="",
            certify_day="",
            certify_tickers=[],
            allow_uncertified_cache=False,
        )
        resolved = {
            "ok": False,
            "source": "step2_cache_catalog",
            "manifest_path": "",
            "selected": {},
            "blockers": ["no_certified_cache_for_requested_scope"],
            "requested": {"tickers": ["CLSK", "MARA", "RIOT"], "day": "2026-05-08"},
            "certification_hint": {"command": "python certify_step2_cache.py --day 2026-05-08 --tickers CLSK MARA RIOT --workers 6"},
        }
        with patch.object(hunter.step2_cache_catalog, "resolve", return_value=resolved), \
                patch.object(hunter, "_write_json", return_value=None):
            payload = hunter._resolve_compiled_cache_for_hunter(args, Path.cwd())

        self.assertFalse(payload["ok"])
        self.assertIn("no_certified_cache_for_requested_scope", payload["blockers"])


if __name__ == "__main__":
    unittest.main()
