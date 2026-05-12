import artifact_version_registry


def test_registry_exposes_learning_ready_artifact_aliases():
    paths = artifact_version_registry._artifact_paths("2026-05-08", ["CLSK", "MARA", "RIOT"])

    assert paths["step2_today_compiled"] == paths["step2_score"]
    assert paths["cache_certification"] == paths["step2_cache_certification"]
    assert paths["canonical_opportunities_summary"].endswith("canonical_opportunities_2026-05-08.summary.json")
    assert paths["step2_decision_parity_summary"].endswith("step2_decision_parity_2026-05-08.summary.json")
