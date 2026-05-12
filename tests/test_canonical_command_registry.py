import unittest
from unittest import mock

import canonical_command_registry
import ops


class CanonicalCommandRegistryTests(unittest.TestCase):
    def test_aliases_resolve_to_canonical_commands(self):
        self.assertEqual("post-close", canonical_command_registry.resolve("eod"))
        self.assertEqual("hunt", canonical_command_registry.resolve("variant-hunt"))
        self.assertEqual("parity", canonical_command_registry.resolve("parity-review"))
        self.assertEqual("step3-audit", canonical_command_registry.resolve("step3"))

    def test_catalog_has_required_operator_commands(self):
        catalog = canonical_command_registry.catalog()
        names = {row["name"] for row in catalog["commands"]}
        for name in ("hunt", "promote", "parity", "post-close", "readiness", "rollback-drill"):
            self.assertIn(name, names)

    def test_deprecated_direct_script_points_to_ops(self):
        msg = canonical_command_registry.deprecated_script_message("variant_tournament_runner.py")
        self.assertIn("deprecated direct entrypoint", msg)
        self.assertIn("python ops.py hunt", msg)

    def test_ops_parser_accepts_canonical_aliases(self):
        parser = ops.build_parser()
        parsed = parser.parse_args(["eod", "2026-05-08", "--json", "--force"])
        self.assertEqual("eod", parsed.command)
        self.assertEqual("2026-05-08", parsed.day)
        self.assertTrue(parsed.json)
        self.assertTrue(parsed.force)

    def test_readiness_runs_pre_market_parity_gate_for_target_day(self):
        calls = []

        def fake_run_and_track(command, **kwargs):
            calls.append(command)
            return {"ok": True, "command": command}

        with mock.patch.object(ops.run_supervisor, "run_and_track", side_effect=fake_run_and_track), \
             mock.patch.object(ops, "_run_process_singleton_check", return_value={"ok": True}):
            payload = ops.run_command("readiness", "2026-05-11")

        self.assertTrue(payload["ok"])
        self.assertIn(
            [ops.PYTHON, "pre_market_parity_gate.py", "2026-05-11", "--json"],
            calls,
        )

    def test_readiness_strict_broker_adds_hard_broker_gate(self):
        with mock.patch.object(ops.run_supervisor, "run_and_track", return_value={"ok": True}), \
             mock.patch.object(ops, "_run_process_singleton_check", return_value={"ok": True}), \
             mock.patch.object(ops, "_run_direct_broker_check", return_value={"ok": False, "command_name": "readiness_direct_broker"}):
            payload = ops.run_command("readiness", "2026-05-11", require_direct_broker=True)

        self.assertFalse(payload["ok"])
        self.assertTrue(payload["require_direct_broker"])


if __name__ == "__main__":
    unittest.main()
