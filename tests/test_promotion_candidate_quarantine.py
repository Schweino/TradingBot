import json
import unittest
import copy
from pathlib import Path
from unittest.mock import patch

import promotion_candidate_quarantine


class PromotionCandidateQuarantineTests(unittest.TestCase):
    def _candidate_payload_and_row(self):
        payload = {
            "script": "step2_adaptive_hunter.py",
            "promotable": True,
            "step2_evaluation_envelope_hash": "env-hash",
            "baseline_profile_hash": "base-hash",
            "step2_evaluation_envelope": {
                "envelope_hash": "env-hash",
                "baseline_profile_hash": "base-hash",
                "promotable": True,
                "compiled_decision_tape": {"manifest": {"path": "C:/fake/manifest.json"}},
            },
            "winners": [
                {
                    "variant": "winner",
                    "weights": {"btc": 1.0, "vwap": -0.5},
                    "bias": 0.125,
                    "step2_pnl": 100.0,
                    "step2_trades": 10,
                }
            ],
        }
        return payload, payload["winners"][0], Path("C:/fake/candidate.json")

    def _patches(self, registry_store: dict):
        payload, row, path = self._candidate_payload_and_row()

        def load_registry():
            return copy.deepcopy(registry_store) if registry_store else promotion_candidate_quarantine._empty_registry()

        def write_registry(registry):
            registry_store.clear()
            registry_store.update(copy.deepcopy(registry))
            return "C:/fake/quarantine.json"

        return (
            patch.object(promotion_candidate_quarantine, "_select", return_value=(payload, row, path)),
            patch.object(promotion_candidate_quarantine, "load_registry", side_effect=load_registry),
            patch.object(promotion_candidate_quarantine, "write_registry", side_effect=write_registry),
            patch.object(
                promotion_candidate_quarantine.active_engine_baseline,
                "active_profile_payload",
                return_value={"profile": {"hash": "base-hash"}},
            ),
            patch.object(
                promotion_candidate_quarantine.baseline_drift_sentinel,
                "evaluate_payload",
                return_value={"status": "current", "ok": True, "reason": "baseline_current"},
            ),
            patch.object(
                promotion_candidate_quarantine.candidate_reproducibility_gate,
                "evaluate_payload",
                return_value={"ok": True, "output_path": "C:/fake/repro.json", "report_hash": "repro-hash"},
            ),
            patch.object(
                promotion_candidate_quarantine.candidate_reproducibility_gate,
                "write_report",
                return_value="C:/fake/repro.json",
            ),
            patch.object(promotion_candidate_quarantine.tournament_safety, "_file_sha256", return_value="artifact-hash"),
        )

    def test_approve_then_evaluate_allows_promotion(self):
        registry_store = {}
        patches = self._patches(registry_store)
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7]:
            record = promotion_candidate_quarantine.approve_artifact(
                "C:/fake/candidate.json",
                approved_by="tester",
                reason="unit test approval",
            )
            verdict = promotion_candidate_quarantine.evaluate_artifact("C:/fake/candidate.json")

        self.assertEqual("approved_for_live", record["status"])
        self.assertTrue(verdict["ok"])
        self.assertEqual([], verdict["failed_checks"])

    def test_missing_approval_blocks_promotion(self):
        registry_store = {}
        patches = self._patches(registry_store)
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7]:
            promotion_candidate_quarantine.record_artifact("C:/fake/candidate.json")
            verdict = promotion_candidate_quarantine.evaluate_artifact("C:/fake/candidate.json")

        self.assertFalse(verdict["ok"])
        failed = {row["name"] for row in verdict["failed_checks"]}
        self.assertIn("quarantine_status_approved_for_live", failed)

    def test_baseline_change_marks_record_stale(self):
        registry_store = {}
        patches = self._patches(registry_store)
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7]:
            promotion_candidate_quarantine.approve_artifact(
                "C:/fake/candidate.json",
                approved_by="tester",
                reason="unit test approval",
            )
        payload, row, path = self._candidate_payload_and_row()

        def load_registry():
            return copy.deepcopy(registry_store)

        def write_registry(registry):
            registry_store.clear()
            registry_store.update(copy.deepcopy(registry))
            return "C:/fake/quarantine.json"

        with patch.object(promotion_candidate_quarantine, "_select", return_value=(payload, row, path)), \
                patch.object(promotion_candidate_quarantine, "load_registry", side_effect=load_registry), \
                patch.object(promotion_candidate_quarantine, "write_registry", side_effect=write_registry), \
                patch.object(
                    promotion_candidate_quarantine.active_engine_baseline,
                    "active_profile_payload",
                    return_value={"profile": {"hash": "new-base-hash"}},
                ), \
                patch.object(promotion_candidate_quarantine.tournament_safety, "_file_sha256", return_value="artifact-hash"):
            verdict = promotion_candidate_quarantine.evaluate_artifact("C:/fake/candidate.json")

        self.assertFalse(verdict["ok"])
        failed = {row["name"] for row in verdict["failed_checks"]}
        self.assertIn("approval_baseline_matches_live", failed)


if __name__ == "__main__":
    unittest.main()
