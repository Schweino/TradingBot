import copy
import unittest

import config_change_journal


class ConfigChangeJournalTests(unittest.TestCase):
    def _config(self):
        return {
            "execution_mode": "step2_signal_scan_live",
            "tickers": ["CLSK", "MARA", "RIOT"],
            "trade_size_pct": 0.25,
            "smart_entry": {
                "active_scoring_profile": {
                    "enabled": True,
                    "name": "winner",
                    "bias": 0.1,
                    "weights": {"btc": 1.0},
                }
            },
            "step2_parity": {"same_ticker_reentry_cooldown_sec": 5},
        }

    def test_build_record_captures_changed_paths_and_sections(self):
        before = self._config()
        after = copy.deepcopy(before)
        after["trade_size_pct"] = 0.3
        after["smart_entry"]["active_scoring_profile"]["bias"] = 0.2

        record = config_change_journal.build_record(
            before,
            after,
            action="unit_test_change",
            reason="exercise journal",
            actor="tester",
            write_snapshots=False,
        )

        paths = {row["path"] for row in record["changed_paths"]}
        sections = {row["section"] for row in record["semantic_section_deltas"]}
        self.assertIn("trade_size_pct", paths)
        self.assertIn("smart_entry.active_scoring_profile.bias", paths)
        self.assertIn("active_scoring_profile", sections)
        self.assertTrue(record["event_hash"])

    def test_validate_live_config_accepts_matching_latest_entry(self):
        cfg = self._config()
        record = config_change_journal.build_record(
            cfg,
            cfg,
            action="record_current_baseline",
            reason="unit baseline",
            actor="tester",
            rollback_snapshot_path="",
            write_snapshots=False,
        )
        pointer = {
            "event_hash": record["event_hash"],
            "current_config_hash": record["current_config_hash"],
            "after_config_hash": record["after_config_hash"],
        }

        verdict = config_change_journal.validate_live_config(cfg, pointer=pointer, entry=record)

        self.assertTrue(verdict["ok"])
        self.assertEqual("ok", verdict["status"])

    def test_validate_live_config_blocks_unaudited_drift(self):
        cfg = self._config()
        record = config_change_journal.build_record(
            cfg,
            cfg,
            action="record_current_baseline",
            reason="unit baseline",
            actor="tester",
            write_snapshots=False,
        )
        drifted = copy.deepcopy(cfg)
        drifted["trade_size_pct"] = 0.5
        pointer = {
            "event_hash": record["event_hash"],
            "current_config_hash": record["current_config_hash"],
            "after_config_hash": record["after_config_hash"],
        }

        verdict = config_change_journal.validate_live_config(drifted, pointer=pointer, entry=record)

        self.assertFalse(verdict["ok"])
        failed = {row["name"] for row in verdict["failed_checks"]}
        self.assertIn("config_hash_matches_latest_journal", failed)


if __name__ == "__main__":
    unittest.main()
