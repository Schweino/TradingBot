import unittest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import market_data_integrity_gate


CT = ZoneInfo("America/Chicago")


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _row(symbol: str, kind: str, ts: datetime) -> dict:
    return {"symbol": symbol, "kind": kind, "t": _ms(ts), "row": {"t": _ms(ts), "p": 10.0}}


def _complete_rows(tickers=("CLSK", "MARA", "RIOT"), step_sec=60) -> list[dict]:
    start = datetime(2026, 4, 6, 8, 30, tzinfo=CT)
    end = datetime(2026, 4, 6, 15, 0, 59, tzinfo=CT)
    rows = []
    ts = start
    while ts <= end:
        for ticker in tickers:
            rows.append(_row(ticker, "stock_trade", ts))
            rows.append(_row(ticker, "stock_quote", ts))
        rows.append(_row("BTC/USD", "btc_synth_trade", ts))
        ts += timedelta(seconds=step_sec)
    return rows


class MarketDataIntegrityGateTests(unittest.TestCase):
    def test_complete_tape_is_promotion_safe(self):
        payload = market_data_integrity_gate.evaluate_events(
            "2026-04-06",
            _complete_rows(),
            tickers=["CLSK", "MARA", "RIOT"],
            source_used="canonical",
            source_exists=True,
        )
        self.assertEqual("DATA_OK", payload["verdict"])
        self.assertTrue(payload["promotion_safe"])
        self.assertEqual(0, payload["critical_count"])

    def test_missing_ticker_series_fails_closed(self):
        payload = market_data_integrity_gate.evaluate_events(
            "2026-04-06",
            _complete_rows(tickers=("CLSK", "MARA")),
            tickers=["CLSK", "MARA", "RIOT"],
            source_used="canonical",
            source_exists=True,
        )
        self.assertEqual("DATA_FAIL", payload["verdict"])
        self.assertFalse(payload["promotion_safe"])
        kinds = {issue["kind"] for issue in payload["issues"]}
        self.assertIn("missing_market_series", kinds)

    def test_large_quote_gap_fails_closed(self):
        rows = _complete_rows()
        rows = [
            row for row in rows
            if not (
                row["symbol"] == "CLSK"
                and row["kind"] == "stock_quote"
                and datetime.fromtimestamp(row["t"] / 1000, CT).hour in (10, 11, 12)
            )
        ]
        payload = market_data_integrity_gate.evaluate_events(
            "2026-04-06",
            rows,
            tickers=["CLSK", "MARA", "RIOT"],
            source_used="canonical",
            source_exists=True,
        )
        self.assertEqual("DATA_FAIL", payload["verdict"])
        self.assertFalse(payload["promotion_safe"])
        kinds = {issue["kind"] for issue in payload["issues"]}
        self.assertIn("large_market_data_gap", kinds)


if __name__ == "__main__":
    unittest.main()
