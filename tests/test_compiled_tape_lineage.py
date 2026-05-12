import unittest
import shutil
import uuid
from contextlib import contextmanager
from pathlib import Path

import compiled_tape_lineage as lineage
import step2_artifact_identity


WORKSPACE_TMP = Path(__file__).resolve().parents[1] / ".test_tmp"


@contextmanager
def _tempdir():
    WORKSPACE_TMP.mkdir(exist_ok=True)
    path = WORKSPACE_TMP / f"case_{uuid.uuid4().hex}"
    path.mkdir()
    try:
        yield str(path)
    finally:
        shutil.rmtree(path, ignore_errors=True)


class CompiledTapeLineageTests(unittest.TestCase):
    def test_reporting_only_drift_allows_quick_score_but_not_certified(self):
        payload = lineage.evaluate_mismatches(
            [{"path": "candidate_decision_brief.py", "reason": "code_hash_mismatch"}],
            code_hash_inputs=["candidate_decision_brief.py"],
        )

        self.assertEqual(payload["status"], "UNCERTIFIED_NON_SCORE_DRIFT")
        self.assertTrue(payload["quick_score_allowed"])
        self.assertFalse(payload["certified"])
        self.assertFalse(payload["rebuild_required"])
        self.assertEqual(payload["recommended_action"], "score_only_uncertified")

    def test_perf_only_event_cache_drift_is_non_score(self):
        payload = lineage.evaluate_mismatches(
            [{"path": "decision_tape_event_cache.py", "reason": "code_hash_mismatch"}],
            code_hash_inputs=["decision_tape_event_cache.py"],
        )

        self.assertEqual(payload["status"], "UNCERTIFIED_NON_SCORE_DRIFT")
        self.assertEqual(payload["recommended_action"], "score_only_uncertified")
        self.assertFalse(payload["rebuild_required"])
        self.assertEqual(payload["non_score_drift"][0]["layer"], "cache_storage")

    def test_scorer_runtime_drift_requires_recertification_not_rebuild(self):
        payload = lineage.evaluate_mismatches(
            [{"path": "decision_tape_compiled.py", "reason": "code_hash_mismatch"}],
            code_hash_inputs=["decision_tape_compiled.py"],
        )

        self.assertEqual(payload["status"], "UNCERTIFIED_SCORER_DRIFT")
        self.assertTrue(payload["quick_score_allowed"])
        self.assertFalse(payload["rebuild_required"])
        self.assertTrue(payload["scorer_recertification_required"])
        self.assertEqual(payload["score_affecting_drift"][0]["layer"], "scoring_runtime")
        self.assertEqual(payload["recommended_action"], "scorer_recertification")

    def test_source_tape_drift_requires_rebuild(self):
        payload = lineage.evaluate_mismatches(
            [{"path": "C:/data/day.jsonl.gz", "reason": "source_hash_mismatch", "score_affecting": True, "layer": "source_market_data"}],
            code_hash_inputs=[],
        )

        self.assertEqual(payload["status"], "UNSAFE_SCORE_DRIFT")
        self.assertTrue(payload["rebuild_required"])
        self.assertEqual(payload["recommended_action"], "full_signal_rebuild")

    def test_path_outcome_drift_refreshes_outcomes_without_signal_rebuild(self):
        payload = lineage.evaluate_mismatches(
            [{"path": "step2_latency_model.py", "reason": "code_hash_mismatch"}],
            code_hash_inputs=["step2_latency_model.py"],
        )

        self.assertEqual(payload["status"], "UNSAFE_SCORE_DRIFT")
        self.assertEqual(payload["recommended_action"], "refresh_outcomes_compile")

    def test_semantic_path_outcomes_drift_refreshes_outcomes_without_signal_rebuild(self):
        payload = lineage.evaluate_mismatches(
            [{"section": "path_outcomes", "reason": "semantic_hash_mismatch"}],
            code_hash_inputs=[],
        )

        self.assertEqual(payload["status"], "UNCERTIFIED_HISTORICAL_OUTCOME_DRIFT")
        self.assertTrue(payload["quick_score_allowed"])
        self.assertFalse(payload["rebuild_required"])
        self.assertEqual(payload["non_score_drift"][0]["layer"], "path_outcomes")
        self.assertTrue(payload["non_score_drift"][0]["historical_score_allowed"])
        self.assertEqual(payload["recommended_action"], "score_only_uncertified")

    def test_unknown_dependency_fails_closed(self):
        payload = lineage.evaluate_mismatches(
            [{"path": "new_scoring_thing.py", "reason": "code_hash_mismatch"}],
            code_hash_inputs=["new_scoring_thing.py"],
        )

        self.assertEqual(payload["status"], "UNSAFE_SCORE_DRIFT")
        self.assertEqual(payload["unknown_count"], 1)
        self.assertTrue(payload["score_affecting_drift"][0]["score_affecting"])

    def test_registry_hash_is_stable(self):
        one = lineage.registry_for(["decision_tape_compiled.py", "candidate_decision_brief.py"])
        two = lineage.registry_for(["decision_tape_compiled.py", "candidate_decision_brief.py"])

        self.assertEqual(one["registry_hash"], two["registry_hash"])
        self.assertTrue(one["dependencies"]["decision_tape_compiled.py"]["score_affecting"])
        self.assertEqual("scoring_runtime", one["dependencies"]["decision_tape_compiled.py"]["layer"])
        self.assertFalse(one["dependencies"]["candidate_decision_brief.py"]["score_affecting"])

    def test_default_registry_tracks_perf_only_event_cache(self):
        self.assertIn("decision_tape_event_cache.py", lineage.DEFAULT_COMPILED_TAPE_CODE_HASH_INPUTS)
        dep = lineage.classify_path("decision_tape_event_cache.py")
        self.assertEqual("cache_storage", dep["layer"])
        self.assertFalse(dep["score_affecting"])

    def test_default_registry_tracks_artifact_identity_as_non_score(self):
        self.assertIn("step2_artifact_identity.py", lineage.DEFAULT_COMPILED_TAPE_CODE_HASH_INPUTS)
        dep = lineage.classify_path("step2_artifact_identity.py")
        self.assertEqual("cache_storage", dep["layer"])
        self.assertFalse(dep["score_affecting"])

    def test_default_registry_tracks_range_linker_as_non_score(self):
        self.assertIn("step2_range_linker.py", lineage.DEFAULT_COMPILED_TAPE_CODE_HASH_INPUTS)
        dep = lineage.classify_path("step2_range_linker.py")
        self.assertEqual("cache_storage", dep["layer"])
        self.assertFalse(dep["score_affecting"])

    def test_source_physical_drift_is_allowed_when_semantic_hash_matches(self):
        with _tempdir() as tmp:
            path = Path(tmp) / "decision_tape.jsonl.gz"
            rows = [{"ts": 1, "ticker": "CLSK", "score": 1.0}]
            source = step2_artifact_identity.write_canonical_jsonl_gz(path, rows)
            manifest = {
                "source_hashes": {str(path): "old-physical-hash"},
                "source_semantic_hashes": {str(path): source["semantic_sha256"]},
                "code_hashes": {},
            }

            payload = lineage.evaluate_manifest(manifest, code_hash_inputs=[])

            self.assertEqual("CERTIFIED_MATCH", payload["status"])
            self.assertTrue(payload["certified"])


if __name__ == "__main__":
    unittest.main()
