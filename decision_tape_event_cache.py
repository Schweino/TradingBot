"""Process-local event tape cache helpers for future decision-tape builds.

This module is intentionally isolated from ``build_decision_tape.py`` so cache
performance work can be classified and tested separately from signal semantics.
It does not transform events; it only memoizes the exact object returned by the
caller-provided loader for a stable day/feed/ticker/cache key.
"""
from __future__ import annotations

import os
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Callable, Hashable


def event_cache_key(
    *,
    day: str,
    tickers: list[str] | tuple[str, ...],
    feed: str,
    quote_mode: str,
    btc_mode: str,
    cache_dir: str,
    prepared_cache_dir: str,
) -> tuple[Hashable, ...]:
    return (
        str(day),
        tuple(str(ticker).upper() for ticker in tickers),
        str(feed),
        str(quote_mode),
        str(btc_mode),
        os.path.abspath(str(cache_dir or "")),
        os.path.abspath(str(prepared_cache_dir or "")),
    )


@dataclass
class DayEventCache:
    max_entries: int = 2
    enabled: bool = True
    _items: OrderedDict[tuple[Hashable, ...], list[dict[str, Any]]] = field(default_factory=OrderedDict)
    hits: int = 0
    misses: int = 0

    def get_or_load(
        self,
        key: tuple[Hashable, ...],
        loader: Callable[[], list[dict[str, Any]]],
    ) -> list[dict[str, Any]]:
        if not self.enabled:
            self.misses += 1
            return loader()
        if key in self._items:
            self.hits += 1
            self._items.move_to_end(key)
            return self._items[key]
        self.misses += 1
        events = loader()
        self._items[key] = events
        self._items.move_to_end(key)
        while len(self._items) > max(1, int(self.max_entries or 1)):
            self._items.popitem(last=False)
        return events

    def clear(self) -> None:
        self._items.clear()
        self.hits = 0
        self.misses = 0

    def summary(self) -> dict[str, Any]:
        return {
            "enabled": bool(self.enabled),
            "hits": int(self.hits),
            "misses": int(self.misses),
            "resident_entries": len(self._items),
            "max_entries": int(self.max_entries),
        }
