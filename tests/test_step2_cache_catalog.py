import unittest
from pathlib import Path
from unittest.mock import patch

import step2_cache_catalog


def _entry(name, start, end, certified=True, rows=100):
    return {
        "name": name,
        "certified": certified,
        "manifest": {
            "path": f"C:/fake/compiled/{name}/manifest.json",
            "sha256": f"sha-{name}",
            "mtime": rows,
        },
        "tickers": ["CLSK", "MARA", "RIOT"],
        "source_days": [start] if start == end else [start, end],
        "start_day": start,
        "end_day": end,
        "row_count": rows,
        "certification": {
            "valid": certified,
            "ok": certified,
            "path": f"C:/fake/certs/{name}.json" if certified else "",
            "failure_reasons": [] if certified else ["certification_receipt_missing"],
        },
    }


class Step2CacheCatalogTests(unittest.TestCase):
    def test_build_catalog_marks_receipted_manifest_certified(self):
        manifest_path = Path("C:/fake/compiled/compiled_step2_live_mockparity_CLSK-MARA-RIOT_2026-05-08_intraday/manifest.json")
        receipt_path = Path("C:/fake/certs/step2_cache_certification_2026-05-08_CLSK-MARA-RIOT.json")
        manifest_resolved = str(manifest_path.resolve())
        receipt = {
            "ok": True,
            "day": "2026-05-08",
            "tickers": ["CLSK", "MARA", "RIOT"],
            "selected_action": "score_only",
            "compiled_manifest": {"path": manifest_resolved, "sha256": "manifest-sha"},
            "after": {"lineage_status": "CERTIFIED_MATCH", "lineage_certified": True},
        }
        manifest = {
            "day_map": {"2026-05-08": [0, 10]},
            "ticker_map": {"CLSK": 0, "MARA": 1, "RIOT": 2},
            "rows": 10,
            "compiled_tape_hash": "tape-hash",
            "arrays_sha256": "arrays-hash",
        }

        def read_json(path, default=None):
            if Path(path) == manifest_path:
                return manifest
            if Path(path) == receipt_path:
                return receipt
            return default

        def file_meta(path):
            return {
                "path": str(Path(path).resolve()),
                "exists": True,
                "bytes": 123,
                "mtime": 10.0,
                "sha256": "manifest-sha",
            }

        with patch.object(step2_cache_catalog, "_iter_manifest_paths", return_value=[manifest_path]), \
                patch.object(step2_cache_catalog, "_iter_receipt_paths", return_value=[receipt_path]), \
                patch.object(step2_cache_catalog, "_read_json", side_effect=read_json), \
                patch.object(step2_cache_catalog, "_file_meta", side_effect=file_meta), \
                patch.object(step2_cache_catalog.tournament_safety, "_file_sha256", return_value="receipt-sha"):
            catalog = step2_cache_catalog.build_catalog()

        self.assertEqual(catalog["certified_entries"], 1)
        self.assertTrue(catalog["entries"][0]["certified"])
        self.assertEqual(catalog["entries"][0]["certification"]["selected_action"], "score_only")

    def test_resolve_selects_latest_certified_cache_covering_scope(self):
        catalog = {
            "entries": [
                _entry("older", "2026-04-06", "2026-05-01", certified=True, rows=100),
                _entry("newer", "2026-04-06", "2026-05-08", certified=True, rows=200),
            ],
            "total_entries": 2,
            "certified_entries": 2,
            "uncertified_entries": 0,
        }

        payload = step2_cache_catalog.resolve(
            tickers=["RIOT", "CLSK", "MARA"],
            day="2026-05-08",
            catalog=catalog,
        )

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["selected"]["name"], "newer")
        self.assertEqual(payload["manifest_path"], "C:/fake/compiled/newer/manifest.json")

    def test_resolve_fails_closed_when_only_uncertified_cache_matches(self):
        catalog = {
            "entries": [_entry("uncertified", "2026-05-08", "2026-05-08", certified=False)],
            "total_entries": 1,
            "certified_entries": 0,
            "uncertified_entries": 1,
        }

        payload = step2_cache_catalog.resolve(
            tickers=["CLSK", "MARA", "RIOT"],
            day="2026-05-08",
            catalog=catalog,
        )

        self.assertFalse(payload["ok"])
        self.assertIn("no_certified_cache_for_requested_scope", payload["blockers"])
        self.assertIn("python certify_step2_cache.py --day 2026-05-08", payload["certification_hint"]["command"])

    def test_resolve_can_allow_uncertified_for_diagnostics(self):
        catalog = {
            "entries": [_entry("uncertified", "2026-05-08", "2026-05-08", certified=False)],
            "total_entries": 1,
            "certified_entries": 0,
            "uncertified_entries": 1,
        }

        payload = step2_cache_catalog.resolve(
            tickers=["CLSK", "MARA", "RIOT"],
            day="2026-05-08",
            require_certified=False,
            catalog=catalog,
        )

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["selected"]["name"], "uncertified")

    def test_resolve_does_not_match_entries_with_unknown_tickers(self):
        unknown = _entry("unknown", "2026-05-08", "2026-05-08", certified=True)
        unknown["tickers"] = []
        catalog = {"entries": [unknown], "total_entries": 1, "certified_entries": 1}

        payload = step2_cache_catalog.resolve(
            tickers=["CLSK", "MARA", "RIOT"],
            day="2026-05-08",
            catalog=catalog,
        )

        self.assertFalse(payload["ok"])
        self.assertIn("no_cache_for_requested_tickers", payload["blockers"])

    def test_range_certification_hint_points_to_exact_manifest(self):
        catalog = {
            "entries": [_entry("range_uncertified", "2026-04-06", "2026-05-08", certified=False)],
            "total_entries": 1,
            "certified_entries": 0,
            "uncertified_entries": 1,
        }

        payload = step2_cache_catalog.resolve(
            tickers=["CLSK", "MARA", "RIOT"],
            start="2026-04-06",
            end="2026-05-08",
            catalog=catalog,
        )

        self.assertFalse(payload["ok"])
        command = payload["certification_hint"]["command"]
        self.assertIn("--start 2026-04-06 --end 2026-05-08", command)
        self.assertIn('--compiled-manifest "C:/fake/compiled/range_uncertified/manifest.json"', command)


if __name__ == "__main__":
    unittest.main()
