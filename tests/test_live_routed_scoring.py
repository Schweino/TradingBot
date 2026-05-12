import unittest

import routed_profile_safety
import scoring_profiles
import step2_parity_contract


class LiveRoutedScoringTests(unittest.TestCase):
    def _signal(self) -> dict:
        return {
            "ticker": "CLSK",
            "side": "LONG",
            "score": 6,
            "setup_type": "btc_relative_strength",
            "session_phase": "open",
            "conviction": "HIGH",
            "btc_context": {"mom_60s": 1.0, "regime": "bull"},
            "signal_quality": {"score_components": {}},
        }

    def test_routed_profile_can_skip_matching_live_signal(self):
        profile = {
            "name": "router",
            "weights": {"btc": 1.0},
            "routes": [
                {
                    "name": "skip_open_brs",
                    "match": {"setup_type": "btc_relative_strength", "session_phase": "open"},
                    "action": "skip",
                }
            ],
        }

        scored = scoring_profiles.apply_profile(profile, self._signal(), {}, {}, include_details=True)

        self.assertTrue(scored["scoring_profile_rejected"])
        self.assertEqual("SKIP", scored["scoring_profile"]["side"])
        self.assertEqual("skip_open_brs", scored["scoring_profile"]["selected_route"]["route"])

    def test_active_profile_snapshot_prefers_enabled_router(self):
        cfg = {
            "smart_entry": {
                "active_scoring_profile": {
                    "enabled": True,
                    "name": "linear",
                    "weights": {"btc": 1.0},
                    "bias": 0.0,
                },
                "active_scoring_router": {
                    "enabled": True,
                    "name": "router",
                    "weights": {"btc": 1.0},
                    "bias": 0.0,
                    "routes": [
                        {"name": "force", "match": {"ticker": "CLSK"}, "action": "force_short"}
                    ],
                },
            }
        }

        snapshot = step2_parity_contract.active_profile_snapshot(cfg)

        self.assertEqual("routed", snapshot["profile_type"])
        self.assertEqual(1, snapshot["route_count"])
        self.assertEqual("router", snapshot["name"])

    def test_routed_profile_safety_requires_audit(self):
        candidate = {
            "variant": "router",
            "weights": {"btc": 1.0},
            "routes": [
                {"name": "force", "match": {"ticker": "CLSK"}, "action": "force_short"}
            ],
        }

        safety = routed_profile_safety.evaluate_candidate(candidate)

        self.assertFalse(safety["ok"])
        self.assertIn("route_audit_present", {row["name"] for row in safety["checks"] if not row["ok"]})

    def test_routed_profile_safety_blocks_stale_audit_identity(self):
        candidate = {
            "variant": "router",
            "weights": {"btc": 1.0},
            "routes": [
                {"name": "current_route", "match": {"ticker": "CLSK"}, "action": "force_short"}
            ],
            "route_audit": {
                "routes": [
                    {"route": "stale_route", "matched_opportunities": 100, "opportunity_share_pct": 10.0, "skipped_sides": 0}
                ]
            },
        }

        safety = routed_profile_safety.evaluate_candidate(candidate)

        self.assertFalse(safety["ok"])
        self.assertIn("route_audit_matches_current_routes", {row["name"] for row in safety["checks"] if not row["ok"]})


if __name__ == "__main__":
    unittest.main()
