from __future__ import annotations

import unittest

import candidate_lifecycle


class CandidateLifecycleTests(unittest.TestCase):
    def test_standard_candidate_gets_stable_id(self):
        row = {
            "name": "unit_variant",
            "bias": 0.1,
            "weights": {"btc": 1.0, "vwap": -0.5},
            "step2_pnl": 123.45,
        }
        standard = candidate_lifecycle.standardize(row)
        first = candidate_lifecycle._candidate_id(standard)
        second = candidate_lifecycle._candidate_id(candidate_lifecycle.standardize(dict(row)))
        self.assertEqual(first, second)
        self.assertEqual(standard["step2"]["pnl"], 123.45)

    def test_evaluate_requires_step2_and_evidence(self):
        standard = candidate_lifecycle.standardize({
            "name": "unit_variant_no_step2",
            "bias": 0.0,
            "weights": {"btc": 1.0},
        })
        record = candidate_lifecycle.upsert_standard_candidate(standard, write=False)
        result = candidate_lifecycle.evaluate_record(record, evidence_packet={"ok": True})
        failed = {row["name"] for row in result["checks"] if not row["ok"]}
        self.assertIn("candidate_step2_pnl_present", failed)


if __name__ == "__main__":
    unittest.main()
