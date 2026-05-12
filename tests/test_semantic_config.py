import unittest

import semantic_config
import semantic_diagnostics


class SemanticConfigDiagnosticsTests(unittest.TestCase):
    def test_path_outcomes_mismatch_reports_latency_field_diagnostics(self):
        cfg = {
            "tickers": ["CLSK"],
            "ticker_cfg": {"CLSK": {"tp": 0.01, "sl": 0.01}},
            "trade_size_pct": 0.25,
            "min_balance": 50,
        }
        current_payload = semantic_config.sections(cfg)["path_outcomes"]
        recorded_payload = dict(current_payload)
        recorded_payload["latency_model_hash"] = "old-latency-hash"

        rows = semantic_diagnostics.enrich_semantic_mismatches(
            semantic_config.mismatches(
                {"path_outcomes": "old-section-hash"},
                ["path_outcomes"],
                cfg,
            ),
            config=cfg,
            recorded_payloads={"path_outcomes": recorded_payload},
        )

        self.assertEqual(1, len(rows))
        diagnostics = rows[0]["diagnostics"]
        self.assertTrue(diagnostics["field_level_diff_available"])
        self.assertIn("latency_model_hash", diagnostics["changed_fields"])
        self.assertTrue(diagnostics["latency_model"]["changed"])
        self.assertEqual("confirmed", diagnostics["suspected_invalidators"][0]["confidence"])


if __name__ == "__main__":
    unittest.main()
