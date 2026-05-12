import shutil
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

import step2_manifest_resolver


WORKSPACE_TMP = Path(__file__).resolve().parents[1] / ".test_work"


def _case_dir() -> Path:
    path = WORKSPACE_TMP / f"resolver_{uuid.uuid4().hex}"
    path.mkdir(parents=True)
    return path


def _write_manifest(path: Path, *, day_shards: bool = True, linked: bool = False) -> None:
    payload = {
        "schema_version": 3,
        "range_linked": linked,
        "rows": 10,
        "source_days": ["2026-05-07", "2026-05-08"],
        "tickers": ["CLSK", "MARA", "RIOT"],
        "array_payload_sha256": "payload-hash",
        "compiled_tape_hash": "compiled-hash",
    }
    if day_shards:
        payload["day_shards"] = {
            "shards": [
                {"day": "2026-05-07", "rows": 5, "array_payload_sha256": "a", "source_identities": {}},
                {"day": "2026-05-08", "rows": 5, "array_payload_sha256": "b", "source_identities": {}},
            ],
        }
    step2_manifest_resolver.write_json(path, payload)


def _ok_receipt(path: Path, selected_action: str = "score_only") -> dict:
    return {
        "ok": True,
        "source": "certify_step2_cache",
        "certification_mode": "range",
        "selected_action": selected_action,
        "compiled_manifest": {"path": str(path), "sha256": "sha"},
        "after": {
            "lineage_status": "CERTIFIED_MATCH",
            "lineage_certified": True,
            "lineage_rebuild_required": False,
            "lineage_quick_score_allowed": True,
        },
        "failure_reasons": [],
    }


class Step2ManifestResolverTests(unittest.TestCase):
    def test_prefers_existing_linked_manifest(self):
        root = _case_dir()
        try:
            base = root / "compiled_step2_current_live_CLSK-MARA-RIOT_2026-05-07_2026-05-08" / "manifest.json"
            linked = root / "compiled_step2_current_live_CLSK-MARA-RIOT_2026-05-07_2026-05-08_linked" / "manifest.json"
            _write_manifest(base)
            _write_manifest(linked, linked=True)
            with patch.object(step2_manifest_resolver, "certify_manifest", side_effect=lambda path, **_: _ok_receipt(Path(path))):
                payload = step2_manifest_resolver.resolve_best_manifest(
                    start="2026-05-07",
                    end="2026-05-08",
                    tickers=["CLSK", "MARA", "RIOT"],
                    compiled_dir=root,
                    write_certification=False,
                )
            self.assertTrue(payload["ok"])
            self.assertEqual("linked", payload["manifest_kind"])
            self.assertEqual(str(linked.resolve()), payload["manifest_path"])
            self.assertEqual("existing_certified_linked", payload["resolver_action"])
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_links_certified_monolith_when_linked_manifest_missing(self):
        root = _case_dir()
        try:
            base_name = "compiled_step2_current_live_CLSK-MARA-RIOT_2026-05-07_2026-05-08"
            base = root / base_name / "manifest.json"
            linked = root / f"{base_name}_linked" / "manifest.json"
            _write_manifest(base)

            def fake_link_range(*, source_manifest, out_dir, name, days=None):
                self.assertEqual(str(base.resolve()), source_manifest)
                _write_manifest(linked, linked=True)
                return {"manifest_path": str(linked.resolve()), "compiled_tape_hash": "linked-hash"}

            with patch.object(step2_manifest_resolver, "certify_manifest", side_effect=lambda path, **_: _ok_receipt(Path(path))), \
                    patch.object(step2_manifest_resolver.step2_range_linker, "link_range", side_effect=fake_link_range):
                payload = step2_manifest_resolver.resolve_best_manifest(
                    start="2026-05-07",
                    end="2026-05-08",
                    tickers=["CLSK", "MARA", "RIOT"],
                    compiled_dir=root,
                    write_certification=False,
                )
            self.assertTrue(payload["ok"])
            self.assertEqual("range_link_certify", payload["resolver_action"])
            self.assertEqual(str(linked.resolve()), payload["manifest_path"])
            self.assertTrue(payload["link_attempt"]["ok"])
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_fails_closed_on_rebuild_required_certification(self):
        root = _case_dir()
        try:
            base = root / "compiled_step2_current_live_CLSK-MARA-RIOT_2026-05-07_2026-05-08" / "manifest.json"
            _write_manifest(base)
            bad_receipt = {
                "ok": False,
                "selected_action": "range_certification_rebuild_required",
                "after": {
                    "lineage_status": "UNCERTIFIED_SCORE_DRIFT",
                    "lineage_certified": False,
                    "lineage_rebuild_required": True,
                    "lineage_quick_score_allowed": False,
                },
                "failure_reasons": ["compiled_tape_not_certified"],
            }
            with patch.object(step2_manifest_resolver, "certify_manifest", return_value=bad_receipt):
                payload = step2_manifest_resolver.resolve_best_manifest(
                    start="2026-05-07",
                    end="2026-05-08",
                    tickers=["CLSK", "MARA", "RIOT"],
                    compiled_dir=root,
                    write_certification=False,
                )
            self.assertFalse(payload["ok"])
            self.assertIn("no_safe_certified_manifest", payload["blockers"])
        finally:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
