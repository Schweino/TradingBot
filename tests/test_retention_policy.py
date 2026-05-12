import shutil
import uuid
from pathlib import Path

import retention_policy


def _configure_tmp_retention(monkeypatch, tmp_path: Path, keep_market_days: int = 2) -> None:
    tick_dir = tmp_path / "tick_logs"
    out_dir = tmp_path / "postmortem"
    config_path = tmp_path / "trading_config.json"
    out_dir.mkdir()
    config_path.write_text(
        (
            '{"retention": {'
            f'"raw_tick_keep_market_days": {keep_market_days}, '
            '"delete_enabled": true, '
            '"raw_tick_delete_enabled": true'
            "}}"
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(retention_policy, "HERE", str(tmp_path))
    monkeypatch.setattr(retention_policy, "TICK_DIR", str(tick_dir))
    monkeypatch.setattr(retention_policy, "OUT_DIR", str(out_dir))
    monkeypatch.setattr(retention_policy, "CONFIG_PATH", str(config_path))


def _new_test_root() -> Path:
    root = Path(__file__).resolve().parents[1] / "runtime" / "retention_policy_tests" / uuid.uuid4().hex
    root.mkdir(parents=True)
    return root


def _write_tick_day(root: Path, day: str, name: str = "CLSK.jsonl") -> Path:
    folder = root / day
    folder.mkdir(parents=True)
    (folder / name).write_text('{"event":"trade","price":1}\n', encoding="utf-8")
    return folder


def test_retention_plan_keeps_newest_market_data_days(monkeypatch):
    test_root = _new_test_root()
    try:
        _configure_tmp_retention(monkeypatch, test_root, keep_market_days=2)
        tick_root = test_root / "tick_logs"
        for day in ("2026-04-29", "2026-05-01", "2026-05-06", "2026-05-11"):
            _write_tick_day(tick_root, day)

        plan = retention_policy.build_retention_plan("2026-05-11")
        raw_plan = plan["raw_tick_market_day_retention"]

        assert raw_plan["raw_tick_market_days_found"] == 4
        assert [row["day"] for row in raw_plan["kept_market_days"]] == ["2026-05-06", "2026-05-11"]
        assert [row["day"] for row in raw_plan["delete_market_days"]] == ["2026-04-29", "2026-05-01"]
        assert plan["totals"]["raw_tick_market_day_delete_candidates"] == 2
    finally:
        shutil.rmtree(test_root, ignore_errors=True)


def test_apply_raw_tick_retention_deletes_only_old_date_folders(monkeypatch):
    test_root = _new_test_root()
    try:
        _configure_tmp_retention(monkeypatch, test_root, keep_market_days=1)
        tick_root = test_root / "tick_logs"
        old_day = _write_tick_day(tick_root, "2026-05-01")
        kept_day = _write_tick_day(tick_root, "2026-05-06")
        scratch = tick_root / "scratch"
        scratch.mkdir()
        (scratch / "keep.jsonl").write_text("{}", encoding="utf-8")

        result = retention_policy.apply_raw_tick_retention("2026-05-11")

        assert result["ok"] is True
        assert result["applied"] is True
        assert result["deleted_market_days_count"] == 1
        assert not old_day.exists()
        assert kept_day.exists()
        assert scratch.exists()
    finally:
        shutil.rmtree(test_root, ignore_errors=True)


def test_apply_raw_tick_retention_honors_delete_toggles(monkeypatch):
    test_root = _new_test_root()
    try:
        _configure_tmp_retention(monkeypatch, test_root, keep_market_days=1)
        config_path = test_root / "trading_config.json"
        config_path.write_text(
            '{"retention": {"raw_tick_keep_market_days": 1, "delete_enabled": false, "raw_tick_delete_enabled": false}}',
            encoding="utf-8",
        )
        tick_root = test_root / "tick_logs"
        old_day = _write_tick_day(tick_root, "2026-05-01")
        _write_tick_day(tick_root, "2026-05-06")

        result = retention_policy.apply_raw_tick_retention("2026-05-11")

        assert result["ok"] is True
        assert result["applied"] is False
        assert old_day.exists()
    finally:
        shutil.rmtree(test_root, ignore_errors=True)
