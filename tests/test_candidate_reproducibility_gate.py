import unittest
from unittest.mock import patch

import candidate_reproducibility_gate
import scoring_variant_lab as lab


class CandidateReproducibilityGateTests(unittest.TestCase):
    def _payload_and_row(self):
        payload = {
            "script": "step2_adaptive_hunter.py",
            "step2_evaluation_envelope": {
                "envelope_hash": "env-hash",
                "baseline_profile_hash": "base-hash",
                "compiled_decision_tape": {
                    "manifest": {"path": "C:/fake/manifest.json"},
                    "compiled_tape_hash": "compiled-hash",
                },
                "run_args": {"start_balance": 100000},
                "sim_config": {"same_ticker_reentry_cooldown_sec": 5},
            },
        }
        row = {
            "variant": "winner",
            "weights": {"btc": 1.0, "vwap": -0.5},
            "bias": 0.125,
            "step2_pnl": 100.0,
            "step2_trades": 10,
            "step2_win_rate_pct": 80.0,
            "by_ticker": {"CLSK": {"pnl": 60.0, "trades": 6}},
        }
        return payload, row

    def _compiled(self):
        return {
            "manifest": {
                "compiled_tape_hash": "compiled-hash",
                "lineage_validation": {
                    "status": "CERTIFIED_MATCH",
                    "certified": True,
                    "quick_score_allowed": True,
                    "rebuild_required": False,
                    "recommended_action": "score_only",
                },
            }
        }

    def _row(self, pnl=100.0):
        return {
            "decision_full": {
                "pnl": pnl,
                "trades": 10,
                "wins": 8,
                "losses": 2,
                "win_rate_pct": 80.0,
                "by_ticker": {"CLSK": {"pnl": 60.0, "trades": 6}},
            }
        }

    def test_reproduces_matching_candidate(self):
        payload, row = self._payload_and_row()
        with patch.object(candidate_reproducibility_gate.decision_tape_compiled, "load_compiled", return_value=self._compiled()), \
                patch.object(candidate_reproducibility_gate.decision_tape_compiled, "simulate_variants", return_value=[self._row()]):
            report = candidate_reproducibility_gate.evaluate_payload(payload, row)

        self.assertTrue(report["ok"])
        self.assertEqual([], report["failed_checks"])
        self.assertEqual(100.0, report["reproduced"]["pnl"])

    def test_blocks_score_mismatch(self):
        payload, row = self._payload_and_row()
        with patch.object(candidate_reproducibility_gate.decision_tape_compiled, "load_compiled", return_value=self._compiled()), \
                patch.object(candidate_reproducibility_gate.decision_tape_compiled, "simulate_variants", return_value=[self._row(pnl=110.0)]):
            report = candidate_reproducibility_gate.evaluate_payload(payload, row)

        self.assertFalse(report["ok"])
        failed = {check["name"] for check in report["failed_checks"]}
        self.assertIn("pnl", failed)

    def test_live_canary_requires_live_weights_to_match_candidate(self):
        payload, row = self._payload_and_row()
        with patch.object(candidate_reproducibility_gate.active_engine_baseline, "active_variant", return_value=lab.Variant("live", {"btc": 1.0, "vwap": -0.5}, 0.125)), \
                patch.object(candidate_reproducibility_gate.decision_tape_compiled, "load_compiled", return_value=self._compiled()), \
                patch.object(candidate_reproducibility_gate.decision_tape_compiled, "simulate_variants", return_value=[self._row()]):
            report = candidate_reproducibility_gate.evaluate_payload(payload, row, use_live_profile=True)

        self.assertTrue(report["ok"])
        names = {check["name"] for check in report["checks"]}
        self.assertIn("live_profile_weights_match_candidate", names)

    def test_blocks_unsafe_compiled_tape_lineage(self):
        payload, row = self._payload_and_row()
        unsafe = {
            "manifest": {
                "compiled_tape_hash": "compiled-hash",
                "lineage_validation": {
                    "status": "UNSAFE_SCORE_DRIFT",
                    "certified": False,
                    "quick_score_allowed": False,
                    "rebuild_required": True,
                    "recommended_action": "full_signal_rebuild",
                },
            }
        }
        with patch.object(candidate_reproducibility_gate.decision_tape_compiled, "load_compiled", return_value=unsafe), \
                patch.object(candidate_reproducibility_gate.decision_tape_compiled, "simulate_variants", return_value=[self._row()]):
            report = candidate_reproducibility_gate.evaluate_payload(payload, row)

        self.assertFalse(report["ok"])
        failed = {check["name"] for check in report["failed_checks"]}
        self.assertIn("compiled_tape_lineage_not_unsafe", failed)


if __name__ == "__main__":
    unittest.main()
