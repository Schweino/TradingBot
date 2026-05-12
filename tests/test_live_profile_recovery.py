import copy
import unittest
from unittest.mock import patch

import live_profile_recovery
import step2_parity_contract


class LiveProfileRecoveryTests(unittest.TestCase):
    def _config(self):
        return {
            "execution_mode": "step2_signal_scan_live",
            "trade_size_pct": 0.25,
            "smart_entry": {
                "active_scoring_profile": {
                    "enabled": True,
                    "name": "winner",
                    "bias": 0.1,
                    "weights": {"btc": 1.0},
                    "promotion_manifest_path": "C:/fake/manifest.json",
                    "promotion_manifest_status": "promoted",
                    "promotion_manifest_hash": "manifest-hash",
                    "promotion_evidence_packet_path": "C:/fake/evidence.json",
                    "promotion_registry_path": "C:/fake/registry.json",
                    "rollback_snapshot_path": "C:/fake/profile_rollback.json",
                }
            },
            "step2_parity": {"same_ticker_reentry_cooldown_sec": 5},
        }

    def test_running_profile_verification_matches_profile_and_manifest(self):
        cfg = self._config()
        profile_hash = step2_parity_contract.active_profile_snapshot(cfg, include_weights=False)["hash"]
        status = {
            "active_scoring_profile": {"hash": profile_hash},
            "promotion_provenance": {"manifest_hash": "manifest-hash"},
            "startup_self_check": {"ok": True},
        }

        verdict = live_profile_recovery.running_profile_verification(cfg, status=status)

        self.assertTrue(verdict["ok"])
        self.assertEqual(0, verdict["critical_failure_count"])

    def test_record_last_known_good_uses_after_config_snapshot_for_restore(self):
        cfg = self._config()
        written = {}
        pointer = {"event_hash": "event-1"}
        entry = {
            "after_config_snapshot_path": "C:/fake/after_config.json",
            "rollback_snapshot_path": "C:/fake/profile_rollback.json",
            "event_hash": "event-1",
        }
        proof = {
            "ok": True,
            "path": "C:/fake/proof.json",
            "profile": {"hash": step2_parity_contract.active_profile_snapshot(cfg, include_weights=False)["hash"]},
        }

        def write_json(_path, payload):
            written.clear()
            written.update(copy.deepcopy(payload))
            return "C:/fake/LAST_KNOWN_GOOD_PROFILE.json"

        with patch.object(live_profile_recovery.config_change_journal, "latest_pointer", return_value=pointer), \
                patch.object(live_profile_recovery.config_change_journal, "latest_entry", return_value=entry), \
                patch.object(live_profile_recovery, "_write_json_atomic", side_effect=write_json):
            payload = live_profile_recovery.record_last_known_good(cfg, proof=proof, write=True)

        self.assertTrue(payload["ok"])
        self.assertEqual("C:/fake/after_config.json", payload["rollback_snapshot_path"])
        self.assertIn("--execute --restart", payload["rollback_command"])
        self.assertEqual("winner", written["profile"]["name"])

    def test_recovery_recommendation_reports_last_known_good_command(self):
        cfg = self._config()
        lkg = {
            "ok": True,
            "updated_at_ct": "2026-05-09T12:00:00-05:00",
            "profile": {"name": "safe", "hash": "safe-hash"},
            "rollback_snapshot_path": "C:/fake/safe_config.json",
            "rollback_command": 'python rollback_drill.py --snapshot-path "C:/fake/safe_config.json" --execute --restart',
        }

        with patch.object(live_profile_recovery, "latest_last_known_good", return_value=lkg):
            payload = live_profile_recovery.recovery_recommendation(cfg, reason="unit")

        self.assertTrue(payload["ok"])
        self.assertEqual("safe", payload["last_known_good_profile"]["name"])
        self.assertIn("safe_config.json", payload["rollback_command"])


if __name__ == "__main__":
    unittest.main()
