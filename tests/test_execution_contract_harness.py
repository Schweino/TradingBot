from __future__ import annotations

import unittest

import live_step2_execution_harness


class LiveStep2ExecutionHarnessTests(unittest.TestCase):
    def test_harness_scenarios_pass_without_writing(self):
        payload = live_step2_execution_harness.run(day="2026-05-08", write=False, config={})

        self.assertTrue(payload["ok"])
        self.assertEqual(12, payload["scenario_count"])
        self.assertEqual([], payload["failed_scenarios"])

    def test_orphan_broker_guard_scenario_is_expected_block(self):
        payload = live_step2_execution_harness.run(day="2026-05-08", write=False, config={})
        row = next(
            item for item in payload["results"]
            if item["name"] == "orphan_broker_bracket_startup_guard_blocks"
        )

        self.assertTrue(row["ok"])
        self.assertFalse(row["broker_guard"]["ok"])
        kinds = {issue["kind"] for issue in row["broker_guard"]["issues"]}
        self.assertIn("broker_position_untracked", kinds)
        self.assertIn("broker_open_order_unmapped", kinds)


if __name__ == "__main__":
    unittest.main()

