import argparse
import unittest
from unittest.mock import patch

import certify_step2_cache


def _args():
    return argparse.Namespace(
        day="2026-05-08",
        start="",
        end="",
        tickers=["CLSK", "MARA", "RIOT"],
        feed="sip",
        quote_mode="per-second",
        btc_mode="bars",
        indicator_mode="live",
        name="",
        compiled_manifest="",
        workers=6,
        no_existing=False,
        overlap_sec=120,
        step2_latency_mode="entry-exit",
        step2_latency_model="C:/fake/latency.json",
        step2_latency_percentile=0.75,
        use_state_checkpoints=True,
        check_only=False,
        no_recertify_non_score=False,
    )


def _report(status="CERTIFIED_MATCH", certified=True, action="score_only", rebuild=False, quick=True):
    return {
        "recommended_action": action,
        "stale_layers": [] if certified else ["compiled_scoring_math"],
        "compiled_tape_lineage": {
            "status": status,
            "certified": certified,
            "rebuild_required": rebuild,
            "quick_score_allowed": quick,
            "recommended_action": action,
            "score_affecting_count": 0 if certified else 1,
            "non_score_count": 0,
        },
    }


class CertifyStep2CacheTests(unittest.TestCase):
    def test_refresh_args_normalizes_latency_percentile_for_refresh_cli(self):
        args = _args()
        args.step2_latency_percentile = 0.75

        payload = certify_step2_cache._refresh_args(args, ["CLSK"], "compiled_name", "full_signal_rebuild")

        self.assertEqual("p75", payload.step2_latency_percentile)

    def test_already_certified_writes_receipt_without_refresh(self):
        args = _args()
        with patch.object(certify_step2_cache.step2_cache_layers, "report", return_value=_report()), \
                patch.object(certify_step2_cache.refresh_intraday_step2, "refresh_once") as refresh, \
                patch.object(certify_step2_cache, "_write_json", return_value="C:/fake/cert.json"):
            payload = certify_step2_cache.certify(args, write=True)

        self.assertTrue(payload["ok"])
        self.assertEqual("score_only", payload["selected_action"])
        refresh.assert_not_called()

    def test_unsafe_cache_runs_refresh_then_certifies(self):
        args = _args()
        unsafe = _report("UNSAFE_SCORE_DRIFT", False, "full_signal_rebuild", True, False)
        certified = _report()
        with patch.object(certify_step2_cache.step2_cache_layers, "report", side_effect=[unsafe, certified]), \
                patch.object(certify_step2_cache.refresh_intraday_step2, "refresh_once", return_value={"compiled_manifest": "C:/fake/manifest.json"}) as refresh, \
                patch.object(certify_step2_cache, "_write_json", return_value="C:/fake/cert.json"):
            payload = certify_step2_cache.certify(args, write=True)

        self.assertTrue(payload["ok"])
        self.assertEqual("full_signal_rebuild", payload["selected_action"])
        refresh.assert_called_once()

    def test_non_score_drift_recertifies_without_refresh(self):
        args = _args()
        non_score = _report("UNCERTIFIED_NON_SCORE_DRIFT", False, "score_only_uncertified", False, True)
        non_score["compiled_tape_lineage"]["non_score_count"] = 1
        certified = _report()
        with patch.object(certify_step2_cache.step2_cache_layers, "report", side_effect=[non_score, certified]), \
                patch.object(certify_step2_cache, "_recertify_non_score_manifest", return_value={"attempted": True, "ok": True}) as recertify, \
                patch.object(certify_step2_cache.refresh_intraday_step2, "refresh_once") as refresh, \
                patch.object(certify_step2_cache, "_write_json", return_value="C:/fake/cert.json"):
            payload = certify_step2_cache.certify(args, write=True)

        self.assertTrue(payload["ok"])
        self.assertEqual("recertify_non_score_drift", payload["selected_action"])
        recertify.assert_called_once()
        refresh.assert_not_called()

    def test_check_only_unsafe_reports_not_certified(self):
        args = _args()
        args.check_only = True
        unsafe = _report("UNSAFE_SCORE_DRIFT", False, "full_signal_rebuild", True, False)
        with patch.object(certify_step2_cache.step2_cache_layers, "report", return_value=unsafe), \
                patch.object(certify_step2_cache.refresh_intraday_step2, "refresh_once") as refresh, \
                patch.object(certify_step2_cache, "_write_json", return_value="C:/fake/cert.json"):
            payload = certify_step2_cache.certify(args, write=True)

        self.assertFalse(payload["ok"])
        self.assertIn("compiled_tape_not_certified", payload["failure_reasons"])
        refresh.assert_not_called()

    def test_range_certifies_exact_manifest_without_refresh(self):
        args = _args()
        args.day = ""
        args.start = "2026-04-06"
        args.end = "2026-05-08"
        args.compiled_manifest = "C:/fake/range/manifest.json"
        manifest = {
            "day_map": {"2026-04-06": [0, 1], "2026-05-08": [1, 2]},
            "ticker_map": {"CLSK": 0, "MARA": 1, "RIOT": 2},
            "rows": 200,
            "arrays_sha256": "arrays",
            "compiled_tape_hash": "tape",
            "step2_execution_contract_hash": "contract",
        }
        with patch.object(certify_step2_cache.Path, "exists", return_value=True), \
                patch.object(certify_step2_cache, "_read_json", return_value=manifest), \
                patch.object(certify_step2_cache, "_lineage_report_for_manifest", return_value=_report()), \
                patch.object(certify_step2_cache.refresh_intraday_step2, "refresh_once") as refresh, \
                patch.object(certify_step2_cache, "_file_meta", return_value={"path": args.compiled_manifest, "sha256": "manifest-sha", "exists": True}), \
                patch.object(certify_step2_cache, "_write_json", return_value="C:/fake/cert.json"):
            payload = certify_step2_cache.certify(args, write=True)

        self.assertTrue(payload["ok"])
        self.assertEqual("range", payload["certification_mode"])
        self.assertEqual("2026-04-06", payload["start"])
        self.assertEqual("2026-05-08", payload["end"])
        self.assertEqual("tape", payload["compiled_tape_hash"])
        refresh.assert_not_called()

    def test_range_refuses_score_affecting_drift_without_one_day_refresh(self):
        args = _args()
        args.day = ""
        args.start = "2026-04-06"
        args.end = "2026-05-08"
        args.compiled_manifest = "C:/fake/range/manifest.json"
        manifest = {
            "day_map": {"2026-04-06": [0, 1], "2026-05-08": [1, 2]},
            "ticker_map": {"CLSK": 0, "MARA": 1, "RIOT": 2},
        }
        with patch.object(certify_step2_cache.Path, "exists", return_value=True), \
                patch.object(certify_step2_cache, "_read_json", return_value=manifest), \
                patch.object(certify_step2_cache, "_lineage_report_for_manifest", return_value=_report("UNSAFE_SCORE_DRIFT", False, "full_signal_rebuild", True, False)), \
                patch.object(certify_step2_cache.refresh_intraday_step2, "refresh_once") as refresh, \
                patch.object(certify_step2_cache, "_file_meta", return_value={"path": args.compiled_manifest, "sha256": "manifest-sha", "exists": True}), \
                patch.object(certify_step2_cache, "_write_json", return_value="C:/fake/cert.json"):
            payload = certify_step2_cache.certify(args, write=True)

        self.assertFalse(payload["ok"])
        self.assertEqual("range_certification_rebuild_required", payload["selected_action"])
        self.assertIn("lineage_rebuild_required", payload["failure_reasons"])
        refresh.assert_not_called()

    def test_range_no_write_does_not_write_day_shard_registry(self):
        args = _args()
        args.day = ""
        args.start = "2026-04-06"
        args.end = "2026-05-08"
        args.compiled_manifest = "C:/fake/range/manifest.json"
        manifest = {
            "day_map": {"2026-04-06": [0, 1], "2026-05-08": [1, 2]},
            "ticker_map": {"CLSK": 0, "MARA": 1, "RIOT": 2},
        }
        with patch.object(certify_step2_cache.Path, "exists", return_value=True), \
                patch.object(certify_step2_cache, "_read_json", return_value=manifest), \
                patch.object(certify_step2_cache, "_lineage_report_for_manifest", return_value=_report()), \
                patch.object(certify_step2_cache.step2_shard_cert_registry, "write_registry_for_manifest") as registry_write, \
                patch.object(certify_step2_cache, "_file_meta", return_value={"path": args.compiled_manifest, "sha256": "manifest-sha", "exists": True}):
            payload = certify_step2_cache.certify(args, write=False)

        self.assertEqual({"written": False, "reason": "no_write"}, payload["day_shard_registry"])
        registry_write.assert_not_called()


if __name__ == "__main__":
    unittest.main()
