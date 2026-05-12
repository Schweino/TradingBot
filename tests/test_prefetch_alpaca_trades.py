import shutil
import unittest
import uuid
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import prefetch_alpaca_trades as prefetch


WORKSPACE_TMP = Path(__file__).resolve().parents[1] / ".test_work"


def _case_dir() -> Path:
    path = WORKSPACE_TMP / f"case_{uuid.uuid4().hex}"
    path.mkdir(parents=True)
    return path


def _args(tmp: str, require_online: bool = False) -> SimpleNamespace:
    return SimpleNamespace(
        cache_dir=tmp,
        feed="sip",
        refresh=True,
        start_hour=8,
        start_minute=30,
        end_hour=15,
        end_minute=0,
        limit=10000,
        max_pages=10,
        timeout=1,
        retries=1,
        require_online=require_online,
    )


class PrefetchAlpacaTradesTests(unittest.TestCase):
    def test_offline_fallback_is_labeled_offline(self):
        root = _case_dir()
        try:
            args = _args(str(root))
            with patch.object(prefetch, "alpaca_get", side_effect=OSError("network blocked")):
                with patch.object(prefetch, "_offline_stock_rows", return_value=[{"t": 1, "p": 10.0, "s": 1}]):
                    result = prefetch.fetch_stock_kind("trades", "CLSK", date(2026, 5, 11), args, {})

            self.assertEqual("offline", result["status"])
            self.assertEqual(1, result["rows"])
            self.assertTrue(Path(result["path"]).exists())
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_require_online_refuses_offline_fallback(self):
        root = _case_dir()
        try:
            args = _args(str(root), require_online=True)
            with patch.object(prefetch, "alpaca_get", side_effect=OSError("network blocked")):
                with patch.object(prefetch, "_offline_stock_rows", return_value=[{"t": 1, "p": 10.0, "s": 1}]):
                    with self.assertRaisesRegex(RuntimeError, "--require-online"):
                        prefetch.fetch_stock_kind("trades", "CLSK", date(2026, 5, 11), args, {})
        finally:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
