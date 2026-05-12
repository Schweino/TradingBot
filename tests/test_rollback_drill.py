import unittest
from unittest.mock import patch

import rollback_drill


class RollbackDrillTests(unittest.TestCase):
    def _current_config(self):
        return {
            "execution_mode": "step2_signal_scan_live",
            "smart_entry": {
                "active_scoring_profile": {
                    "enabled": True,
                    "name": "new",
                    "bias": 0.2,
                    "weights": {"btc": 2.0},
                    "promotion_manifest_path": "C:/fake/new_manifest.json",
                    "promotion_manifest_hash": "new-manifest",
                    "promotion_manifest_status": "promoted",
                }
            },
            "step2_parity": {"same_ticker_reentry_cooldown_sec": 5},
        }

    def test_promotion_rollback_snapshot_resolves_to_target_config(self):
        snapshot_path = "C:/fake/promotion_rollback.json"
        current = self._current_config()
        rollback_snapshot = {
            "action": "rollback_active_scoring_profile",
            "prior_profile": {
                "enabled": True,
                "name": "prior",
                "bias": 0.1,
                "weights": {"btc": 1.0},
                "promotion_manifest_path": "C:/fake/prior_manifest.json",
                "promotion_manifest_hash": "prior-manifest",
                "promotion_manifest_status": "promoted",
            },
        }

        def read_json(path, default=None):
            raw = str(path)
            if raw.endswith("trading_config.json"):
                return current
            if raw == snapshot_path:
                return rollback_snapshot
            return default

        with patch.object(rollback_drill, "_read_json", side_effect=read_json), \
                patch.object(rollback_drill.os.path, "exists", return_value=True), \
                patch.object(rollback_drill.config_change_journal, "validate_live_config", return_value={"ok": True, "status": "ok"}), \
                patch.object(rollback_drill.step2_parity_contract, "live_parity_checks", return_value={"ok": True, "status": "ok"}), \
                patch.object(rollback_drill.promotion_manifest, "validate_live_profile", return_value={"ok": True, "status": "ok", "legacy": False}), \
                patch.object(rollback_drill.promotion_safety, "active_profile_evidence_validation", return_value={"ok": True, "failed_count": 0}):
            payload = rollback_drill.build(snapshot_path=snapshot_path, write=False)

        self.assertTrue(payload["ok"])
        self.assertEqual("promotion_rollback_profile_snapshot", payload["snapshot_kind"])
        self.assertEqual("prior", payload["target_profile"]["name"])


if __name__ == "__main__":
    unittest.main()
