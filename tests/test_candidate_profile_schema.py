import unittest

import candidate_profile_schema


class CandidateProfileSchemaTests(unittest.TestCase):
    def test_select_reads_shadow_profiles_by_step2_score(self):
        payload = {
            "profiles": [
                {
                    "name": "current_live",
                    "weights": {"btc": 1.0},
                    "bias": 0.0,
                },
                {
                    "name": "winner",
                    "weights": {"btc": 2.0},
                    "bias": 0.1,
                    "step2": {"pnl": 123.45},
                },
            ]
        }

        selected = candidate_profile_schema.select(payload, rank=1)

        self.assertEqual(selected["name"], "winner")

    def test_decorate_payload_preserves_routed_profile_contract(self):
        route = {
            "name": "skip_open_brs",
            "match": {"setup_type": "btc_relative_strength", "session_phase": "open"},
            "weights": {},
            "bias": 0.0,
            "action": "skip",
        }
        payload = {
            "leaderboard": [
                {
                    "variant": "routed_winner",
                    "weights": {"btc": 1.0},
                    "routes": [route],
                    "step2_pnl": 101000.0,
                }
            ]
        }

        candidate_profile_schema.decorate_payload(payload)
        standard = payload["standard_leaderboard"][0]

        self.assertTrue(standard["routed_scoring_profile"])
        self.assertEqual([route], standard["routes"])
        self.assertTrue(standard["model_id"])


if __name__ == "__main__":
    unittest.main()
