import json
import unittest
from unittest.mock import patch

import candidate_profile_schema
import promotion_gate
import promotion_safety
import step2_quote_aware_guard
import step2_parity_contract


class PromotionHardGateTests(unittest.TestCase):
    def _candidate_and_evidence(self):
        cfg = promotion_gate._config()
        contracts = candidate_profile_schema.contracts(cfg)
        candidate = {
            "name": "unit_candidate",
            "bias": 0.125,
            "weights": {"btc": 1.0, "vwap": -0.5},
            "contracts": contracts,
            "step2": {
                "pnl": 100.0,
                "trades": 10,
                "wins": 8,
                "losses": 2,
                "exit_replay_model": step2_quote_aware_guard.REQUIRED_EXIT_REPLAY_MODEL,
            },
        }
        evidence = {
            "ok": True,
            "candidate": {"profile_hash": promotion_gate._candidate_profile_hash(candidate)},
            "contracts": contracts,
            "output": {"json_path": "C:/fake/evidence.json"},
            "days": [],
        }
        return candidate, evidence

    def test_require_evidence_blocks_profile_hash_mismatch(self):
        candidate, evidence = self._candidate_and_evidence()
        evidence["candidate"]["profile_hash"] = "wrong"
        with patch.object(promotion_gate.golden_parity_suite, "run", return_value={"ok": True, "results": []}):
            payload = promotion_gate.evaluate(
                candidate,
                days=[],
                require_lifecycle=False,
                evidence_packet=evidence,
                require_evidence_packet=True,
                require_evidence_artifacts=False,
            )
        failed = {row["name"] for row in payload["checks"] if not row.get("ok")}
        self.assertIn("promotion_evidence_profile_hash_matches_candidate", failed)
        self.assertFalse(payload["ok"])

    def test_require_evidence_allows_matching_packet(self):
        candidate, evidence = self._candidate_and_evidence()
        with patch.object(promotion_gate.golden_parity_suite, "run", return_value={"ok": True, "results": []}), \
                patch.object(promotion_gate.contract_gate, "check", return_value={"ok": True, "critical_failure_count": 0}):
            payload = promotion_gate.evaluate(
                candidate,
                days=[],
                require_lifecycle=False,
                evidence_packet=evidence,
                require_evidence_packet=True,
                require_evidence_artifacts=False,
            )
        failed = [row for row in payload["checks"] if not row.get("ok")]
        self.assertEqual([], failed)
        self.assertTrue(payload["ok"])

    def test_require_evidence_blocks_unsafe_market_data(self):
        candidate, evidence = self._candidate_and_evidence()
        day = "2026-04-06"
        evidence["days"] = [day]
        evidence["step2_scores"] = {day: {"exists": True, "pnl": 100.0}}
        evidence["parity_verdicts"] = {day: {"exists": True, "promotion_safe": True}}
        evidence["market_data_integrity"] = {day: {"exists": True, "promotion_safe": False, "verdict": "DATA_FAIL"}}
        evidence["artifact_registries"] = {day: {"exists": True, "registry_hash": "hash"}}
        with patch.object(promotion_gate.golden_parity_suite, "run", return_value={"ok": True, "results": []}), \
                patch.object(promotion_gate.canonical_decision_packet, "schema_compatibility", return_value={"ok": True, "counts": {}}):
            payload = promotion_gate.evaluate(
                candidate,
                days=[day],
                require_lifecycle=False,
                evidence_packet=evidence,
                require_evidence_packet=True,
                require_evidence_artifacts=False,
            )
        failed = {row["name"] for row in payload["checks"] if not row.get("ok")}
        self.assertIn("promotion_evidence_market_data_integrity_safe", failed)
        self.assertFalse(payload["ok"])

    def test_require_evidence_blocks_unsafe_compiled_lineage(self):
        candidate, evidence = self._candidate_and_evidence()
        evidence["step2_evaluation_envelope"] = {
            "compiled_decision_tape": {
                "lineage_validation": {
                    "status": "UNSAFE_SCORE_DRIFT",
                    "certified": False,
                    "quick_score_allowed": False,
                    "rebuild_required": True,
                    "recommended_action": "full_signal_rebuild",
                }
            }
        }
        with patch.object(promotion_gate.golden_parity_suite, "run", return_value={"ok": True, "results": []}), \
                patch.object(promotion_gate.contract_gate, "check", return_value={"ok": True, "critical_failure_count": 0}):
            payload = promotion_gate.evaluate(
                candidate,
                days=[],
                require_lifecycle=False,
                evidence_packet=evidence,
                require_evidence_packet=True,
                require_evidence_artifacts=False,
            )
        failed = {row["name"] for row in payload["checks"] if not row.get("ok")}
        self.assertIn("promotion_evidence_compiled_tape_lineage_safe", failed)
        self.assertFalse(payload["ok"])

    def test_live_boot_evidence_validation_requires_exact_active_hash(self):
        cfg = promotion_gate._config()
        active_hash = step2_parity_contract.active_profile_snapshot(cfg, include_weights=False)["hash"]
        contracts = candidate_profile_schema.contracts(cfg)
        evidence_path = "C:/fake/evidence.json"
        registry_path = "C:/fake/registry.json"
        rollback_path = "C:/fake/rollback.json"
        fake_json = {
            evidence_path: {
                    "ok": True,
                    "candidate": {"profile_hash": active_hash},
                    "contracts": contracts,
                    "packet_hash": "packet",
            },
            registry_path: {"new_profile": {"profile_hash": active_hash}},
            rollback_path: {"prior_profile": {"name": "old", "weights": {"btc": 1.0}}},
        }
        profile = cfg.setdefault("smart_entry", {}).setdefault("active_scoring_profile", {})
        profile["profile_hash"] = active_hash
        profile["promotion_evidence_packet_path"] = evidence_path
        profile["promotion_evidence_packet_hash"] = "packet"
        profile["promotion_registry_path"] = registry_path
        profile["rollback_snapshot_path"] = rollback_path
        with patch.object(promotion_safety, "_read_json", side_effect=lambda path: fake_json.get(path, {})), \
                patch.object(promotion_safety.os.path, "exists", return_value=True):
            payload = promotion_safety.active_profile_evidence_validation(cfg)
        failed = [row for row in payload["checks"] if not row.get("ok")]
        self.assertEqual([], failed)
        self.assertTrue(payload["ok"])


if __name__ == "__main__":
    unittest.main()
