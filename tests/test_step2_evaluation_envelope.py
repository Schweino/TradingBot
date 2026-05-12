import unittest

import candidate_profile_schema
import step2_evaluation_envelope


class Step2EvaluationEnvelopeTests(unittest.TestCase):
    def _compiled(self):
        return {
            "manifest": {
                "compiled_tape_hash": "compiled-hash",
                "arrays_sha256": "arrays-hash",
                "day_map": {"2026-04-06": [0, 10]},
                "row_count": 10,
                "lineage_validation": {
                    "status": "CERTIFIED_MATCH",
                    "certified": True,
                    "quick_score_allowed": True,
                    "rebuild_required": False,
                    "recommended_action": "score_only",
                },
            }
        }

    def test_builds_promotable_envelope_with_baseline_and_tape_identity(self):
        envelope = step2_evaluation_envelope.build(
            compiled=self._compiled(),
            active_row={"variant": "active", "step2_pnl": 123.45, "step2_trades": 9},
            data_integrity={
                "promotion_safe": True,
                "reports": {
                    "2026-04-06": {
                        "day": "2026-04-06",
                        "promotion_safe": True,
                        "verdict": "DATA_OK",
                        "critical_count": 0,
                    }
                },
            },
            contracts=candidate_profile_schema.contracts(),
        )

        self.assertTrue(envelope["promotable"])
        self.assertTrue(envelope["baseline_profile_hash"])
        self.assertEqual(["2026-04-06"], envelope["source_days"])
        self.assertEqual("compiled-hash", envelope["compiled_decision_tape"]["compiled_tape_hash"])
        self.assertTrue(envelope["envelope_hash"])

    def test_market_data_failure_marks_envelope_non_promotable(self):
        envelope = step2_evaluation_envelope.build(
            compiled=self._compiled(),
            data_integrity={
                "promotion_safe": False,
                "reports": {
                    "2026-04-06": {
                        "day": "2026-04-06",
                        "promotion_safe": False,
                        "verdict": "DATA_FAIL",
                        "critical_count": 1,
                    }
                },
            },
            contracts=candidate_profile_schema.contracts(),
        )

        self.assertFalse(envelope["promotable"])
        self.assertIn("market_data_integrity_failed", envelope["non_promotable_reasons"])

    def test_unsafe_lineage_marks_envelope_non_promotable(self):
        compiled = self._compiled()
        compiled["manifest"]["lineage_validation"] = {
            "status": "UNSAFE_SCORE_DRIFT",
            "certified": False,
            "quick_score_allowed": False,
            "rebuild_required": True,
            "recommended_action": "full_signal_rebuild",
        }
        envelope = step2_evaluation_envelope.build(
            compiled=compiled,
            data_integrity={"promotion_safe": True, "reports": {}},
            contracts=candidate_profile_schema.contracts(),
        )

        self.assertFalse(envelope["promotable"])
        self.assertIn("compiled_tape_unsafe_score_drift", envelope["non_promotable_reasons"])
        self.assertEqual("UNSAFE_SCORE_DRIFT", envelope["compiled_decision_tape"]["lineage_status"])


if __name__ == "__main__":
    unittest.main()
