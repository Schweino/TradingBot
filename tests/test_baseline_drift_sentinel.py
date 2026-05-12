import unittest

import baseline_drift_sentinel


class BaselineDriftSentinelTests(unittest.TestCase):
    def test_current_baseline_is_ok(self):
        current = {
            "profile_hash": "live-hash",
            "profile_name": "live",
            "config_sha256": "config-hash",
        }
        payload = {
            "script": "step2_adaptive_hunter.py",
            "step2_evaluation_envelope": {
                "baseline_profile_hash": "live-hash",
                "promotable": True,
            },
            "promotable": True,
        }

        drift = baseline_drift_sentinel.evaluate_payload(payload, current=current)
        self.assertEqual("current", drift["status"])
        annotated = baseline_drift_sentinel.annotate_payload(payload, current=current)
        self.assertTrue(annotated["promotable"])

    def test_stale_baseline_blocks_promotion(self):
        current = {
            "profile_hash": "live-hash",
            "profile_name": "live",
            "config_sha256": "config-hash",
        }
        payload = {
            "script": "step2_adaptive_hunter.py",
            "step2_evaluation_envelope": {
                "baseline_profile_hash": "old-hash",
                "promotable": True,
            },
            "promotable": True,
        }

        annotated = baseline_drift_sentinel.annotate_payload(payload, current=current)
        self.assertFalse(annotated["promotable"])
        self.assertEqual("stale", annotated["baseline_drift"]["status"])
        self.assertIn("baseline_drift_stale", annotated["non_promotable_reasons"])
        self.assertFalse(annotated["step2_evaluation_envelope"]["promotable"])

    def test_missing_baseline_hash_is_unknown(self):
        drift = baseline_drift_sentinel.evaluate_payload(
            {"script": "step2_adaptive_hunter.py"},
            current={"profile_hash": "live-hash"},
        )
        self.assertEqual("unknown", drift["status"])
        self.assertFalse(drift["ok"])


if __name__ == "__main__":
    unittest.main()
