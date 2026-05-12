import json
from pathlib import Path

import execution_kernel
import promotion_gate
import step2_hunt_intelligence as hunt_intel
import step2_learning_db
import step2_void_registry


def _workspace_tmp(name):
    import uuid

    path = Path("runtime") / "unit_test_tmp" / f"{name}_{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_registry_marks_20260511_voided():
    assert step2_void_registry.is_day_voided("2026-05-11")
    assert step2_void_registry.is_day_voided("run_20260511")
    report = step2_void_registry.contamination_report({"by_day": {"2026-05-11": {"pnl": 1.0}}})
    assert report["contamination_status"] == "tainted"
    assert report["strategy_learning_allowed"] is False


def test_rank_rows_excludes_voided_day_candidates():
    rows = [
        {
            "variant": "voided",
            "weights": {"ema": 1.0},
            "bias": 0.0,
            "step2_pnl": 200.0,
            "step2_delta_vs_active": 100.0,
            "step2_trades": 10,
            "step2_win_rate_pct": 80.0,
            "by_day": {"2026-05-11": {"pnl": 200.0, "trades": 10}},
            "by_ticker": {"CLSK": {"pnl": 200.0, "trades": 10}},
        },
        {
            "variant": "clean",
            "weights": {"ema": 1.0},
            "bias": 0.0,
            "step2_pnl": 100.0,
            "step2_delta_vs_active": 50.0,
            "step2_trades": 10,
            "step2_win_rate_pct": 70.0,
            "by_day": {"2026-05-08": {"pnl": 100.0, "trades": 10}},
            "by_ticker": {"CLSK": {"pnl": 100.0, "trades": 10}},
        },
    ]

    ranked = hunt_intel.rank_rows(rows, live_only=True)

    assert ranked["void_filtered_rows"] == 1
    assert ranked["void_filtered_days"] == ["2026-05-11"]
    assert [row["variant"] for row in ranked["raw_leaderboard"]] == ["clean"]


def test_learning_db_skips_voided_run_dir():
    tmp_path = _workspace_tmp("void_learning_db")
    run_dir = tmp_path / "quote_aware_learning_20260511"
    run_dir.mkdir()
    (run_dir / "summary.json").write_text(
        json.dumps(
            {
                "leaderboard": [
                    {
                        "variant": "voided",
                        "weights": {"ema": 1.0},
                        "bias": 0.0,
                        "step2_pnl": 100.0,
                        "step2_delta_vs_active": 50.0,
                        "by_day": {"2026-05-08": {"pnl": 100.0, "trades": 5}},
                        "by_ticker": {"CLSK": {"pnl": 100.0, "trades": 5}},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    db = step2_learning_db.Step2LearningDB(tmp_path / "learning.sqlite")
    try:
        stats = db.ingest_run_dir(run_dir)
        final = db.stats(run_id=run_dir.name)
    finally:
        db.close()

    assert stats["skipped"] is True
    assert stats["voided_source_days"] == ["2026-05-11"]
    assert final["candidates"] == 0
    assert final["route_arms"] == 0


def test_promotion_gate_rejects_voided_evidence(monkeypatch):
    monkeypatch.setattr(promotion_gate.golden_parity_suite, "run", lambda days=None: {"ok": True, "results": []})
    monkeypatch.setattr(promotion_gate.contract_gate, "check", lambda cfg, day=None: {"ok": True, "critical_failure_count": 0})
    kernel_hash = execution_kernel.contract_from_config({}).get("execution_kernel_hash")
    candidate = {
        "execution_kernel_hash": kernel_hash,
        "weights": {},
        "bias": 0.0,
        "by_day": {"2026-05-11": {"pnl": 10.0, "trades": 1}},
    }

    result = promotion_gate.evaluate(candidate=candidate, days=["2026-05-11"], require_lifecycle=True)
    check = next(row for row in result["checks"] if row["name"] == "voided_source_day_block")

    assert result["decision"] == "REJECT"
    assert check["ok"] is False
    assert check["actual"] == ["2026-05-11"]
    assert result["candidate_lifecycle"]["skipped"] is True
