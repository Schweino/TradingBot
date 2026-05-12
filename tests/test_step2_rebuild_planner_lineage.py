import unittest
from unittest.mock import patch

import step2_rebuild_planner


class Step2RebuildPlannerLineageTests(unittest.TestCase):
    def _plan_for_action(self, action: str) -> dict:
        cache_report = {
            "recommended_action": action,
            "stale_layers": ["compiled_scoring_math"] if action != "score_only" else [],
            "compiled_tape_lineage": {
                "status": "CERTIFIED_MATCH" if action == "score_only" else "UNSAFE_SCORE_DRIFT",
                "certified": action == "score_only",
                "quick_score_allowed": action in ("score_only", "score_only_uncertified"),
                "rebuild_required": action not in ("score_only", "score_only_uncertified"),
                "recommended_action": action,
            },
        }
        with patch.object(step2_rebuild_planner.step2_cache_layers, "report", return_value=cache_report), \
                patch.object(step2_rebuild_planner.compiled_chunk_store, "changed_buckets_from_market_store", return_value=[]), \
                patch.object(step2_rebuild_planner, "_decision_tape_watermark", return_value={"exists": True, "rows": 10, "max_ts": 1000}), \
                patch.object(step2_rebuild_planner, "_write_json", return_value="C:/fake/plan.json"):
            return step2_rebuild_planner.plan("2026-05-08", ["CLSK", "MARA", "RIOT"])

    def test_non_score_lineage_allows_uncertified_score_only(self):
        payload = self._plan_for_action("score_only_uncertified")

        self.assertEqual("score_only_uncertified", payload["action"])
        self.assertTrue(payload["score_only"])
        self.assertTrue(payload["uncertified_score_only"])

    def test_path_outcome_lineage_refreshes_outcomes(self):
        payload = self._plan_for_action("refresh_outcomes_compile")

        self.assertEqual("refresh_outcomes_compile", payload["action"])
        self.assertTrue(payload["reuse_existing_signals"])
        self.assertFalse(payload["full_rebuild"])

    def test_score_math_lineage_recompiles_only(self):
        payload = self._plan_for_action("recompile_only")

        self.assertEqual("recompile_only", payload["action"])
        self.assertTrue(payload["recompile_only"])
        self.assertFalse(payload["reuse_existing_signals"])

    def test_scorer_recertification_uses_uncertified_score_only(self):
        payload = self._plan_for_action("scorer_recertification")

        self.assertEqual("score_only_uncertified", payload["action"])
        self.assertTrue(payload["score_only"])
        self.assertTrue(payload["uncertified_score_only"])

    def test_source_or_signal_lineage_forces_full_signal_rebuild(self):
        payload = self._plan_for_action("full_signal_rebuild")

        self.assertEqual("full_signal_rebuild", payload["action"])
        self.assertTrue(payload["full_rebuild"])


if __name__ == "__main__":
    unittest.main()
