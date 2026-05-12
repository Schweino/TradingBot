import argparse
import unittest
from unittest.mock import patch

import step2_fast_rebuild


def _args(**overrides):
    values = dict(
        start="2026-05-07",
        end="2026-05-08",
        tickers=["CLSK", "MARA", "RIOT"],
        feed="sip",
        quote_mode="per-second",
        btc_mode="bars",
        indicator_mode="live",
        workers=6,
        cache_dir="C:/cache",
        prepared_cache_dir="C:/prepared",
        tape_dir="C:/tapes",
        compiled_dir="C:/compiled",
        artifact_dir="C:/artifacts",
        name="compiled_test",
        source="signal-scan",
        no_outcome_shards=False,
        no_event_cache=False,
        overlap_sec=120,
        use_incremental_market_store=False,
        force_full=False,
        force_reuse_existing_signals=False,
        force_compile=False,
        compile_only=False,
        no_certify=False,
        plan_only=True,
        use_state_checkpoints=True,
        checkpoint_bucket_sec=300,
        step2_latency_mode="entry-exit",
        step2_latency_model="C:/latency.json",
        step2_latency_percentile=0.75,
        json=True,
    )
    values.update(overrides)
    return argparse.Namespace(**values)


def _day_plan(day, action, decision_start_ts=0):
    return {
        "day": day,
        "action": action,
        "decision_start_ts": decision_start_ts,
        "score_only": action.startswith("score_only"),
        "full_rebuild": action == "full_signal_rebuild",
        "recompile_only": action == "recompile_only",
        "reuse_existing_signals": action == "refresh_outcomes_compile",
    }


class Step2FastRebuildTests(unittest.TestCase):
    def test_certified_range_plans_certify_only(self):
        args = _args()
        with patch.object(step2_fast_rebuild, "_days", return_value=["2026-05-07", "2026-05-08"]), \
                patch.object(step2_fast_rebuild.step2_rebuild_planner, "plan", side_effect=[
                    _day_plan("2026-05-07", "score_only"),
                    _day_plan("2026-05-08", "score_only"),
                ]), \
                patch.object(step2_fast_rebuild, "_write_json", return_value="C:/plan.json"):
            payload = step2_fast_rebuild.plan(args)

        self.assertEqual("certify_only", payload["final_action"])
        self.assertFalse(payload["compile_needed"])
        self.assertEqual([], payload["build_commands"])
        self.assertTrue(payload["certify_command"])

    def test_mixed_range_rebuilds_only_needed_days_then_compiles_once(self):
        args = _args()
        with patch.object(step2_fast_rebuild, "_days", return_value=["2026-05-07", "2026-05-08"]), \
                patch.object(step2_fast_rebuild.step2_rebuild_planner, "plan", side_effect=[
                    _day_plan("2026-05-07", "score_only"),
                    _day_plan("2026-05-08", "refresh_outcomes_compile"),
                ]), \
                patch.object(step2_fast_rebuild, "_write_json", return_value="C:/plan.json"):
            payload = step2_fast_rebuild.plan(args)

        self.assertEqual("partial_rebuild_compile_certify", payload["final_action"])
        self.assertTrue(payload["compile_needed"])
        self.assertEqual(1, payload["build_command_count"])
        self.assertEqual(["2026-05-08"], payload["build_commands"][0]["days"])
        self.assertIn("--reuse-existing-signals", payload["build_commands"][0]["cmd"])

    def test_incremental_append_uses_planned_watermark_and_checkpoints(self):
        args = _args()
        with patch.object(step2_fast_rebuild, "_days", return_value=["2026-05-08"]), \
                patch.object(step2_fast_rebuild.step2_rebuild_planner, "plan", return_value=_day_plan("2026-05-08", "incremental_append", 12345)), \
                patch.object(step2_fast_rebuild, "_write_json", return_value="C:/plan.json"):
            payload = step2_fast_rebuild.plan(args)

        cmd = payload["build_commands"][0]["cmd"]
        self.assertIn("--append-existing", cmd)
        self.assertIn("--use-state-checkpoints", cmd)
        self.assertIn("12345", cmd)

    def test_compile_only_skips_planned_build_commands(self):
        args = _args(compile_only=True)
        with patch.object(step2_fast_rebuild, "_days", return_value=["2026-05-08"]), \
                patch.object(step2_fast_rebuild.step2_rebuild_planner, "plan", return_value=_day_plan("2026-05-08", "full_signal_rebuild")), \
                patch.object(step2_fast_rebuild, "_write_json", return_value="C:/plan.json"):
            payload = step2_fast_rebuild.plan(args)

        self.assertEqual([], payload["build_commands"])
        self.assertTrue(payload["compile_needed"])
        self.assertEqual("compile_certify", payload["final_action"])


if __name__ == "__main__":
    unittest.main()
