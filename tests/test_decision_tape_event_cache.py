import unittest

import decision_tape_event_cache


class DecisionTapeEventCacheTests(unittest.TestCase):
    def test_cache_returns_exact_loader_payload_without_transforming(self):
        cache = decision_tape_event_cache.DayEventCache(max_entries=2)
        key = decision_tape_event_cache.event_cache_key(
            day="2026-05-08",
            tickers=["clsk", "MARA"],
            feed="sip",
            quote_mode="per-second",
            btc_mode="bars",
            cache_dir="C:/cache",
            prepared_cache_dir="C:/prepared",
        )
        calls = []
        rows = [{"t": 1, "symbol": "CLSK"}, {"t": 2, "symbol": "MARA"}]

        def loader():
            calls.append("load")
            return rows

        first = cache.get_or_load(key, loader)
        second = cache.get_or_load(key, loader)

        self.assertIs(first, rows)
        self.assertIs(second, rows)
        self.assertEqual(["load"], calls)
        self.assertEqual({"enabled": True, "hits": 1, "misses": 1, "resident_entries": 1, "max_entries": 2}, cache.summary())

    def test_disabled_cache_loads_every_time(self):
        cache = decision_tape_event_cache.DayEventCache(enabled=False)
        calls = []

        def loader():
            calls.append("load")
            return [{"seq": len(calls)}]

        one = cache.get_or_load(("k",), loader)
        two = cache.get_or_load(("k",), loader)

        self.assertNotEqual(one, two)
        self.assertEqual(["load", "load"], calls)
        self.assertEqual(0, cache.summary()["resident_entries"])

    def test_cache_evicts_oldest_key(self):
        cache = decision_tape_event_cache.DayEventCache(max_entries=1)

        cache.get_or_load(("a",), lambda: [{"a": 1}])
        cache.get_or_load(("b",), lambda: [{"b": 1}])

        self.assertEqual(1, cache.summary()["resident_entries"])
        self.assertEqual([("b",)], list(cache._items.keys()))


if __name__ == "__main__":
    unittest.main()
