import unittest
from unittest.mock import patch

import promotion_manifest
import step2_parity_contract


class PromotionManifestTests(unittest.TestCase):
    def _config(self):
        return {
            "execution_mode": step2_parity_contract.LIVE_PARITY_EXECUTION_MODE,
            "smart_entry": {
                "active_scoring_profile": {
                    "enabled": True,
                    "name": "winner",
                    "bias": 0.25,
                    "weights": {"btc": 2.0, "vwap": -1.0},
                    "promotion_manifest_path": "C:/fake/manifest.json",
                    "promotion_manifest_status": "promoted",
                }
            },
            "step2_parity": {
                "same_ticker_reentry_cooldown_sec": 5,
                "max_trades_per_day": 0,
                "max_trades_per_ticker_day": 0,
            },
        }

    def _entry(self):
        return {
            "action": "promote_active_scoring_profile",
            "candidate_source": "C:/fake/candidate.json",
            "selected_rank": 1,
            "selected_variant": "winner",
            "selected_step2_pnl": 12345.67,
            "model_id": "model-1",
            "family_id": "family-1",
            "standard_candidate": {
                "name": "winner",
                "variant": "winner",
                "model_id": "model-1",
                "family_id": "family-1",
                "weights": {"btc": 2.0, "vwap": -1.0},
                "bias": 0.25,
                "step2": {"pnl": 12345.67, "trades": 10, "wins": 8, "losses": 2},
                "baseline": {"active_step2_pnl": 10000.0, "delta_vs_active": 2345.67},
            },
            "new_profile": {
                "enabled": True,
                "name": "winner",
                "bias": 0.25,
                "weights": {"btc": 2.0, "vwap": -1.0},
                "profile_hash": "profile-hash",
            },
            "validation": {
                "ok": True,
                "candidate_reproducibility": {"ok": True, "output_path": "C:/fake/repro.json"},
                "promotion_quarantine": {
                    "ok": True,
                    "record": {
                        "candidate_id": "model-1",
                        "status": "promoted",
                        "approval": {
                            "approved_by": "tester",
                            "approved_at_ct": "2026-05-09T10:00:00-05:00",
                            "approved_baseline_profile_hash": "baseline-hash",
                            "reason": "unit test approval",
                        },
                    },
                },
            },
            "candidate_lifecycle_gate": {"ok": True},
            "promotion_gate": {"ok": True},
            "promotion_evidence_packet": {"json_path": "C:/fake/evidence.json", "packet_hash": "evidence-hash"},
            "rollback_snapshot": {"path": "C:/fake/rollback.json", "prior_profile_hash": "prior-hash"},
            "post_promotion_canary": {"ok": True, "output_path": "C:/fake/canary.json", "report_hash": "canary-hash"},
            "registry_path": "C:/fake/registry.json",
        }

    def test_manifest_links_promotion_chain_and_live_identity(self):
        manifest = promotion_manifest.build(
            self._entry(),
            self._config(),
            status="promoted",
            manifest_path="C:/fake/manifest.json",
        )

        self.assertEqual("promotion_manifest", manifest["source"])
        self.assertEqual("promoted", manifest["status"])
        self.assertEqual("C:/fake/candidate.json", manifest["candidate"]["source_artifact"])
        self.assertEqual(12345.67, manifest["candidate"]["step2"]["pnl"])
        self.assertEqual("tester", manifest["approval"]["approved_by"])
        self.assertTrue(manifest["evidence"]["validation"]["ok"])
        self.assertTrue(manifest["evidence"]["post_promotion_canary"]["ok"])
        self.assertEqual("C:/fake/rollback.json", manifest["rollback"]["snapshot"]["path"])
        self.assertTrue(manifest["live"]["config_hash"])
        self.assertTrue(manifest["live"]["active_profile"]["canonical"]["hash"])
        self.assertTrue(manifest["manifest_hash"])

    def _valid_manifest(self):
        cfg = self._config()
        profile_hash = step2_parity_contract.active_profile_snapshot(cfg, include_weights=False)["hash"]
        entry = self._entry()
        entry["new_profile"]["profile_hash"] = profile_hash
        manifest = promotion_manifest.build(
            entry,
            cfg,
            status="promoted",
            manifest_path="C:/fake/manifest.json",
        )
        manifest["artifacts"]["rollback_snapshot"]["exists"] = True
        manifest["artifacts"]["promotion_registry"]["exists"] = True
        manifest["artifacts"]["promotion_evidence_packet"]["exists"] = True
        manifest["manifest_hash"] = promotion_manifest.stored_manifest_hash(manifest)
        cfg["smart_entry"]["active_scoring_profile"]["promotion_manifest_hash"] = manifest["manifest_hash"]
        return cfg, manifest

    def test_validate_live_profile_blocks_pre_manifest_live_profile(self):
        cfg = self._config()
        cfg["smart_entry"]["active_scoring_profile"].pop("promotion_manifest_path", None)
        cfg["smart_entry"]["active_scoring_profile"].pop("promotion_manifest_status", None)
        with patch.object(promotion_manifest, "_read_json", return_value={}):
            verdict = promotion_manifest.validate_live_profile(cfg)

        self.assertFalse(verdict["ok"])
        self.assertTrue(verdict["legacy"])
        self.assertEqual("missing_manifest", verdict["status"])

    def test_validate_live_profile_allows_explicit_legacy_override_only(self):
        cfg = self._config()
        cfg["smart_entry"]["active_scoring_profile"].pop("promotion_manifest_path", None)
        cfg["smart_entry"]["active_scoring_profile"].pop("promotion_manifest_status", None)
        with patch.object(promotion_manifest, "_read_json", return_value={}), \
                patch.dict("os.environ", {"CLAUDE_ALLOW_LEGACY_PROMOTION_MANIFEST": "1"}):
            verdict = promotion_manifest.validate_live_profile(cfg)

        self.assertTrue(verdict["ok"])
        self.assertTrue(verdict["legacy"])
        self.assertEqual("legacy_override", verdict["status"])

    def test_validate_live_profile_requires_manifest_hash_on_active_profile(self):
        cfg, manifest = self._valid_manifest()
        cfg["smart_entry"]["active_scoring_profile"].pop("promotion_manifest_hash", None)
        pointer = {
            "manifest_path": "C:/fake/manifest.json",
            "manifest_hash": manifest["manifest_hash"],
            "live_profile_hash": manifest["live"]["active_profile"]["canonical"]["hash"],
        }

        def read_json(path):
            raw = str(path)
            if raw.endswith("PROMOTION_MANIFEST_LATEST.json"):
                return pointer
            if raw.endswith("manifest.json"):
                return manifest
            return {}

        with patch.object(promotion_manifest, "_read_json", side_effect=read_json):
            verdict = promotion_manifest.validate_live_profile(cfg)

        self.assertFalse(verdict["ok"])
        failed = {row["name"] for row in verdict["failed_checks"]}
        self.assertIn("active_profile_manifest_hash_present", failed)

    def test_validate_live_profile_accepts_matching_manifest(self):
        cfg, manifest = self._valid_manifest()
        pointer = {
            "manifest_path": "C:/fake/manifest.json",
            "manifest_hash": manifest["manifest_hash"],
            "live_profile_hash": manifest["live"]["active_profile"]["canonical"]["hash"],
        }

        def read_json(path):
            raw = str(path)
            if raw.endswith("PROMOTION_MANIFEST_LATEST.json"):
                return pointer
            if raw.endswith("manifest.json"):
                return manifest
            return {}

        with patch.object(promotion_manifest, "_read_json", side_effect=read_json):
            verdict = promotion_manifest.validate_live_profile(cfg)

        self.assertTrue(verdict["ok"])
        self.assertFalse(verdict["legacy"])
        self.assertEqual(0, verdict["critical_failure_count"])

    def test_validate_live_profile_blocks_manifest_profile_mismatch(self):
        cfg, manifest = self._valid_manifest()
        cfg["smart_entry"]["active_scoring_profile"]["weights"] = {"btc": 9.0}
        pointer = {
            "manifest_path": "C:/fake/manifest.json",
            "manifest_hash": manifest["manifest_hash"],
            "live_profile_hash": manifest["live"]["active_profile"]["canonical"]["hash"],
        }

        def read_json(path):
            raw = str(path)
            if raw.endswith("PROMOTION_MANIFEST_LATEST.json"):
                return pointer
            if raw.endswith("manifest.json"):
                return manifest
            return {}

        with patch.object(promotion_manifest, "_read_json", side_effect=read_json):
            verdict = promotion_manifest.validate_live_profile(cfg)

        self.assertFalse(verdict["ok"])
        failed = {row["name"] for row in verdict["failed_checks"]}
        self.assertIn("manifest_live_profile_hash_matches_active", failed)


if __name__ == "__main__":
    unittest.main()
