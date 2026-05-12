import json
import unittest
import copy
from pathlib import Path
from unittest.mock import patch

import promote_active_profile


class PromoteActiveProfileRollbackTests(unittest.TestCase):
    def test_post_canary_failure_restore_writes_prior_profile(self):
        cfg_store = {
            "smart_entry": {
                "active_scoring_profile": {
                    "enabled": True,
                    "name": "new",
                    "bias": 0.2,
                    "weights": {"btc": 2.0},
                }
            }
        }
        rollback = {
            "prior_profile": {
                "enabled": True,
                "name": "prior",
                "bias": 0.1,
                "weights": {"btc": 1.0},
                "promotion_evidence_packet_path": "C:/fake/evidence.json",
            }
        }
        entry = {"new_profile": {"profile_hash": "new-hash"}}
        canary = {"output_path": "C:/fake/canary.json"}

        def load_json(_path):
            return copy.deepcopy(cfg_store)

        def write_json(_path, payload):
            cfg_store.clear()
            cfg_store.update(copy.deepcopy(payload))

        with patch.object(promote_active_profile, "_load_json", side_effect=load_json), \
                patch.object(promote_active_profile, "_write_json_atomic", side_effect=write_json):
            result = promote_active_profile._restore_from_rollback_snapshot(rollback, entry, canary)
            cfg = copy.deepcopy(cfg_store)

        profile = cfg["smart_entry"]["active_scoring_profile"]
        self.assertTrue(result["ok"])
        self.assertEqual("prior", profile["name"])
        self.assertEqual({"btc": 1.0}, profile["weights"])
        self.assertEqual("post_promotion_canary_failed", profile["rollback_reason"])
        self.assertEqual("new-hash", profile["rollback_failed_profile_hash"])


if __name__ == "__main__":
    unittest.main()
