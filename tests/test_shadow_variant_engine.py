from __future__ import annotations

import unittest

import shadow_variant_engine


class ShadowVariantEngineTests(unittest.TestCase):
    def test_score_live_row_uses_profile_and_live_features(self):
        profile = {
            "name": "unit_profile",
            "bias": 0.0,
            "weights": {"momentum": 2.0},
        }
        row = {
            "ticker": "CLSK",
            "side": "SHORT",
            "price": 10.0,
            "score": -5.0,
            "setup_type": "momentum_breakout",
            "feature_snapshot": {
                "indicators": {
                    "mom_5s": 0.1,
                    "mom_15s": 0.2,
                    "ema_stack": "bull",
                },
                "btc_indicators": {"mom_60s": 0.0},
            },
        }
        score = shadow_variant_engine.score_live_row(profile, row)
        self.assertEqual(score["side"], "LONG")
        self.assertGreater(score["score"], 0)
        self.assertEqual(score["source_side"], "SHORT")

    def test_candidates_always_include_active_profile(self):
        profiles = shadow_variant_engine.load_candidates(path="does_not_exist.json", include_active=True)
        self.assertTrue(profiles)
        self.assertTrue(any(p.get("baseline") == "current_live" for p in profiles))


if __name__ == "__main__":
    unittest.main()
