import shutil
import unittest
import uuid
import json
from pathlib import Path

import numpy as np
import compiled_tape_lineage

import decision_tape_compiled
import step2_artifact_identity
import step2_range_linker


WORKSPACE_TMP = Path(__file__).resolve().parents[1] / ".test_work"


def _case_dir() -> Path:
    path = WORKSPACE_TMP / f"case_{uuid.uuid4().hex}"
    path.mkdir(parents=True)
    return path


def _row(day: str, ts: int, ticker: str = "CLSK") -> dict:
    return {
        "schema_version": 2,
        "type": "decision_tape_row",
        "indicator_mode": "live",
        "opportunity_id": f"{day}:{ticker}:{ts}",
        "day": day,
        "ts": ts,
        "ticker": ticker,
        "original_side": "LONG",
        "setup_type": "unit_setup",
        "price": 10.0,
        "score": 1.0,
        "conviction": "HIGH",
        "source_decision": "accepted",
        "model_features": {},
        "gate_features": {},
        "outcomes": {
            "LONG": {"pnl_pct": 1.0, "held_sec": 10, "reason": "take_profit"},
            "SHORT": {"pnl_pct": -1.0, "held_sec": 10, "reason": "stop_loss"},
        },
    }


class Step2RangeLinkerTests(unittest.TestCase):
    def test_linked_range_loader_assembles_day_shards_exactly(self):
        root = _case_dir()
        try:
            tape_dir = root / "tapes"
            out_dir = root / "compiled"
            days = ["2026-05-07", "2026-05-08"]
            source_paths = []
            rows_by_day = {}
            for idx, day in enumerate(days):
                rows = [_row(day, 1000 + idx * 100), _row(day, 1010 + idx * 100, "MARA")]
                rows_by_day[day] = rows
                path = tape_dir / f"decision_tape_sip_per-second_bars_live_CLSK-MARA_{day}.jsonl.gz"
                step2_artifact_identity.write_canonical_jsonl_gz(path, rows, sort_rows=True)
                source_paths.append(str(path))

            compiled = decision_tape_compiled.compile_rows(rows_by_day, ["CLSK", "MARA"], indicator_mode="live")
            source_manifest = decision_tape_compiled.save_compiled(compiled, str(out_dir), "source", source_paths)
            linked = step2_range_linker.link_range(
                source_manifest=source_manifest["manifest_path"],
                out_dir=str(out_dir),
                name="linked",
            )

            monolith = decision_tape_compiled.load_compiled(source_manifest["manifest_path"], mmap=False)
            linked_loaded = decision_tape_compiled.load_compiled(linked["manifest_path"], mmap=False)

            self.assertTrue(linked_loaded["loaded_from_day_shards"])
            self.assertEqual(monolith["rows"], linked_loaded["rows"])
            self.assertEqual(linked["day_map"], linked_loaded["day_map"])
            np.testing.assert_array_equal(monolith["ts"], linked_loaded["ts"])
            np.testing.assert_array_equal(monolith["ticker_code"], linked_loaded["ticker_code"])
            np.testing.assert_array_equal(monolith["features"], linked_loaded["features"])
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_historical_day_shards_survive_current_lineage_drift(self):
        root = _case_dir()
        try:
            tape_dir = root / "tapes"
            out_dir = root / "compiled"
            days = ["2026-05-07", "2026-05-08"]
            source_paths = []
            rows_by_day = {}
            for idx, day in enumerate(days):
                rows = [_row(day, 1000 + idx * 100), _row(day, 1010 + idx * 100, "MARA")]
                rows_by_day[day] = rows
                path = tape_dir / f"decision_tape_sip_per-second_bars_live_CLSK-MARA_{day}.jsonl.gz"
                step2_artifact_identity.write_canonical_jsonl_gz(path, rows, sort_rows=True)
                source_paths.append(str(path))

            compiled = decision_tape_compiled.compile_rows(rows_by_day, ["CLSK", "MARA"], indicator_mode="live")
            manifest = decision_tape_compiled.save_compiled(compiled, str(out_dir), "source", source_paths)
            manifest_path = Path(manifest["manifest_path"])
            payload = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
            payload["code_hashes"]["build_decision_tape.py"] = "stale_hash"
            manifest_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")

            with self.assertRaises(compiled_tape_lineage.CompiledTapeLineageError):
                decision_tape_compiled.load_compiled(str(manifest_path), mmap=False, prefer_day_shards=True)

            loaded = decision_tape_compiled.load_compiled(
                str(manifest_path),
                mmap=False,
                prefer_day_shards=True,
                allow_historical_day_shard_score=True,
                historical_cutoff_day="2026-05-08",
            )

            self.assertTrue(loaded["loaded_from_day_shards"])
            self.assertEqual(2, loaded["rows"])
            self.assertEqual({"2026-05-07": 0}, loaded["day_map"])
            lineage = loaded["manifest"]["lineage_validation"]
            self.assertEqual("PARTIAL_HISTORICAL_DAY_SHARD_SCORE_ALLOWED", lineage["status"])
            self.assertEqual(["2026-05-07"], lineage["allowed_days"])
            self.assertTrue(lineage["quick_score_allowed"])
            self.assertFalse(lineage["rebuild_required"])
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_linked_range_can_merge_multiple_source_manifests(self):
        root = _case_dir()
        try:
            tape_dir = root / "tapes"
            out_dir = root / "compiled"
            source_manifests = []
            expected_ts = []
            for idx, day in enumerate(["2026-05-07", "2026-05-08"]):
                rows = [_row(day, 1000 + idx * 100), _row(day, 1010 + idx * 100, "MARA")]
                expected_ts.extend(row["ts"] for row in rows)
                path = tape_dir / f"decision_tape_sip_per-second_bars_live_CLSK-MARA_{day}.jsonl.gz"
                step2_artifact_identity.write_canonical_jsonl_gz(path, rows, sort_rows=True)
                compiled = decision_tape_compiled.compile_rows({day: rows}, ["CLSK", "MARA"], indicator_mode="live")
                manifest = decision_tape_compiled.save_compiled(compiled, str(out_dir), f"source_{idx}", [str(path)])
                source_manifests.append(manifest["manifest_path"])

            linked = step2_range_linker.link_ranges(
                source_manifests=source_manifests,
                out_dir=str(out_dir),
                name="linked_multi",
            )
            linked_loaded = decision_tape_compiled.load_compiled(linked["manifest_path"], mmap=False)

            self.assertTrue(linked_loaded["loaded_from_day_shards"])
            self.assertEqual(4, linked_loaded["rows"])
            self.assertEqual({"2026-05-07": 0, "2026-05-08": 1}, linked["day_map"])
            self.assertEqual(source_manifests, linked["source_manifests"])
            np.testing.assert_array_equal(sorted(expected_ts), sorted(linked_loaded["ts"].tolist()))
        finally:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
