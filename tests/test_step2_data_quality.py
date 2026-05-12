import json
from uuid import uuid4
from pathlib import Path

import step2_data_quality
import step2_hunt_intelligence as hunt_intel
import step2_quote_aware_guard


def _workspace_tmp(name):
    path = Path("runtime") / "unit_test_tmp" / f"{name}_{uuid4().hex}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _row(name="candidate", *, exit_model=None, by_day=None):
    row = {
        "variant": name,
        "weights": {"ema": 1.0},
        "bias": 0.0,
        "step2_pnl": 100.0,
        "step2_delta_vs_active": 10.0,
        "step2_trades": 8,
        "by_day": by_day if by_day is not None else {"2026-05-08": {"pnl": 100.0, "trades": 8}},
        "by_ticker": {"CLSK": {"pnl": 100.0, "trades": 8}},
    }
    if exit_model is not None:
        row["exit_replay_model"] = exit_model
    return row


def test_row_contract_allows_clean_learning_and_requires_quote_model_for_promotion():
    row = _row(exit_model=step2_quote_aware_guard.REQUIRED_EXIT_REPLAY_MODEL)

    contract = step2_data_quality.row_contract(row)

    assert contract["learning_allowed"] is True
    assert contract["promotion_allowed"] is True
    assert contract["blockers"] == []
    assert contract["score"] == 100.0


def test_row_contract_blocks_missing_decision_shape_but_only_warns_on_quote_metadata():
    row = _row(exit_model=None)
    row.pop("weights")

    contract = step2_data_quality.row_contract(row)

    assert contract["learning_allowed"] is False
    assert "missing_decision_shape" in contract["blockers"]
    assert "exit_replay_model_missing" in contract["warnings"]
    assert contract["promotion_allowed"] is False


def test_rank_rows_exposes_data_quality_report_and_tags_warnings():
    ranked = hunt_intel.rank_rows([_row(exit_model=None)], live_only=True, behavioral_dedupe=False, limit=10)
    leader = ranked["raw_leaderboard"][0]

    assert ranked["data_quality_report"]["candidate_count"] == 1
    assert leader["data_quality_contract"]["learning_allowed"] is True
    assert "data_quality_warning:exit_replay_model_missing" in leader["learning_tags"]
    assert "data_quality_contract" in hunt_intel.finalist_row(leader, 1)


def test_run_provenance_fingerprints_summary_and_declared_artifacts():
    run_dir = _workspace_tmp("data_quality") / "run"
    run_dir.mkdir()
    declared = run_dir / "finalists.json"
    declared.write_text(json.dumps({"finalists": []}), encoding="utf-8")
    summary = {
        "source": "test",
        "top100": [_row(exit_model=step2_quote_aware_guard.REQUIRED_EXIT_REPLAY_MODEL)],
        "artifact_paths": {"finalists": str(declared)},
    }
    (run_dir / "final_summary.json").write_text(json.dumps(summary), encoding="utf-8")

    contract = step2_data_quality.run_provenance(run_dir, summary)

    assert contract["learning_allowed"] is True
    assert contract["candidate_count"] == 1
    assert contract["required_artifacts"]["final_summary.json"]["sha256"]
    assert contract["declared_artifacts"]["finalists"]["exists"] is True
