"""
Prefetch Alpaca market data cache files for engine replay.

This intentionally fills the same gzip JSON files used by backtest_30d_engine:
  data_cache/alpaca_engine_replay/<feed>/trades/<SYMBOL>_<YYYY-MM-DD>.json.gz
  data_cache/alpaca_engine_replay/<feed>/quotes_per-second/<SYMBOL>_<YYYY-MM-DD>.json.gz
  data_cache/alpaca_engine_replay/crypto-us/crypto_bars_1Min/BTC-USD_<YYYY-MM-DD>.json.gz
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from requests import exceptions as requests_exceptions

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore


HERE = Path(__file__).resolve().parent
DEFAULT_CACHE_DIR = HERE / "data_cache" / "alpaca_engine_replay"
CT = ZoneInfo("America/Chicago")


def parse_day(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def market_days(start: date, end: date) -> list[date]:
    days: list[date] = []
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            days.append(cur)
        cur += timedelta(days=1)
    return days


def session_bounds_utc(day: date, start_hour: int, start_minute: int, end_hour: int, end_minute: int) -> tuple[str, str]:
    start_ct = datetime.combine(day, dt_time(start_hour, start_minute), tzinfo=CT)
    end_ct = datetime.combine(day, dt_time(end_hour, end_minute), tzinfo=CT)
    return (
        start_ct.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        end_ct.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    )


def iso_to_ms(ts: str) -> int:
    return int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1000)


def cache_path(cache_dir: Path, feed: str, kind: str, symbol: str, day: date) -> Path:
    safe_symbol = symbol.replace("/", "-")
    return cache_dir / feed / kind / f"{safe_symbol}_{day.isoformat()}.json.gz"


def read_cached_count(path: Path) -> int | None:
    if not path.exists():
        return None
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            data = json.load(f)
        return len(data) if isinstance(data, list) else None
    except Exception:
        return None


def alpaca_get(session: requests.Session, url: str, params: dict[str, Any], headers: dict[str, str],
               timeout: int, retries: int) -> dict[str, Any]:
    delay = 2.0
    for attempt in range(retries):
        resp = session.get(url, params=params, headers=headers, timeout=timeout)
        if resp.status_code == 429 or 500 <= resp.status_code < 600:
            retry_after = resp.headers.get("Retry-After")
            if retry_after:
                try:
                    sleep_for = max(delay, float(retry_after))
                except ValueError:
                    sleep_for = delay
            else:
                sleep_for = delay
            print(f"[prefetch] retry status={resp.status_code} sleep={sleep_for:.1f}s url={url}", flush=True)
            time.sleep(sleep_for)
            delay = min(delay * 1.8, 60.0)
            continue
        if resp.status_code >= 400:
            try:
                msg = resp.json().get("message", resp.text)
            except Exception:
                msg = resp.text
            raise RuntimeError(f"Alpaca API error ({resp.status_code}): {msg}")
        return resp.json()
    raise RuntimeError(f"Alpaca API exhausted retries after {retries} attempts")


def thin_quotes_per_second(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest: dict[int, dict[str, Any]] = {}
    for row in rows:
        try:
            sec = int(row["t"]) // 1000
        except Exception:
            continue
        latest[sec] = row
    return [latest[k] for k in sorted(latest)]


def _iter_incremental_partition_events(day: date):
    """
    Best-effort offline fallback: iterate locally captured events for the day.

    Reads: data_cache/incremental_market_store/<YYYY-MM-DD>/partitions/*.events.json.gz
    """
    root = HERE / "data_cache" / "incremental_market_store" / day.isoformat() / "partitions"
    if not root.is_dir():
        return iter(())

    def _gen():
        for path in sorted(root.glob("*.events.json.gz")):
            try:
                with gzip.open(path, "rt", encoding="utf-8") as f:
                    payload = json.load(f)
                if not isinstance(payload, list):
                    continue
                for event in payload:
                    if isinstance(event, dict):
                        yield event
            except Exception:
                continue

    return _gen()


def _offline_stock_rows(kind: str, symbol: str, day: date, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
    want_kind = "stock_trade" if kind == "trades" else "stock_quote"
    sym = symbol.upper()
    rows: list[dict[str, Any]] = []
    for ev in _iter_incremental_partition_events(day):
        if ev.get("kind") != want_kind:
            continue
        if (ev.get("symbol") or "").upper() != sym:
            continue
        row = ev.get("row") or {}
        try:
            ts = int(row.get("t") or ev.get("t") or 0)
        except Exception:
            continue
        if ts < start_ms or ts >= end_ms:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _offline_btc_bars(day: date, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
    # Aggregate btc_synth_trade ticks into 1-minute OHLCV bars.
    buckets: dict[int, dict[str, Any]] = {}
    for ev in _iter_incremental_partition_events(day):
        if ev.get("kind") != "btc_synth_trade":
            continue
        if ev.get("symbol") not in ("BTC/USD", "BTC-USD"):
            continue
        row = ev.get("row") or {}
        try:
            ts = int(row.get("t") or ev.get("t") or 0)
            price = float(row.get("p"))
            size = float(row.get("s") or 0.0)
        except Exception:
            continue
        if ts < start_ms or ts >= end_ms:
            continue
        minute_start = (ts // 60000) * 60000
        bar = buckets.get(minute_start)
        if bar is None:
            buckets[minute_start] = {
                "t": int(minute_start),
                "o": price,
                "h": price,
                "l": price,
                "c": price,
                "v": size,
                "_last_ts": ts,
            }
        else:
            bar["h"] = max(float(bar["h"]), price)
            bar["l"] = min(float(bar["l"]), price)
            if ts >= int(bar["_last_ts"]):
                bar["c"] = price
                bar["_last_ts"] = ts
            bar["v"] = float(bar.get("v") or 0.0) + size

    bars = [buckets[k] for k in sorted(buckets)]
    for bar in bars:
        bar.pop("_last_ts", None)
        bar["o"] = float(bar["o"])
        bar["h"] = float(bar["h"])
        bar["l"] = float(bar["l"])
        bar["c"] = float(bar["c"])
        bar["v"] = float(bar.get("v") or 0.0)
    return bars


def fetch_stock_kind(kind: str, symbol: str, day: date, args: argparse.Namespace, headers: dict[str, str]) -> dict[str, Any]:
    cache_kind = "trades" if kind == "trades" else "quotes_per-second"
    path = cache_path(Path(args.cache_dir), args.feed, cache_kind, symbol, day)
    if path.exists() and not args.refresh:
        count = read_cached_count(path)
        if count is not None:
            return {"kind": cache_kind, "symbol": symbol, "day": day.isoformat(), "status": "cached", "rows": count, "pages": 0, "path": str(path)}

    start_iso, end_iso = session_bounds_utc(day, args.start_hour, args.start_minute, args.end_hour, args.end_minute)
    start_ms = iso_to_ms(start_iso)
    end_ms = iso_to_ms(end_iso)
    url = f"https://data.alpaca.markets/v2/stocks/{symbol}/{kind}"
    params: dict[str, Any] = {
        "start": start_iso,
        "end": end_iso,
        "limit": args.limit,
        "feed": args.feed,
        "sort": "asc",
    }
    rows: list[dict[str, Any]] = []
    page_token = None
    pages = 0
    try:
        with requests.Session() as session:
            while pages < args.max_pages:
                if page_token:
                    params["page_token"] = page_token
                else:
                    params.pop("page_token", None)
                data = alpaca_get(session, url, params, headers, args.timeout, args.retries)
                pages += 1
                if kind == "trades":
                    for trade in data.get("trades") or []:
                        rows.append({
                            "t": iso_to_ms(trade["t"]),
                            "p": trade.get("p"),
                            "s": trade.get("s"),
                            "x": trade.get("x"),
                            "c": trade.get("c") or [],
                            "i": trade.get("i"),
                            "z": trade.get("z"),
                        })
                else:
                    for quote in data.get("quotes") or []:
                        rows.append({
                            "t": iso_to_ms(quote["t"]),
                            "bp": quote.get("bp"),
                            "ap": quote.get("ap"),
                            "bs": quote.get("bs"),
                            "as": quote.get("as"),
                            "bx": quote.get("bx"),
                            "ax": quote.get("ax"),
                            "c": quote.get("c") or [],
                            "z": quote.get("z"),
                        })
                page_token = data.get("next_page_token")
                if not page_token:
                    break
            if page_token:
                raise RuntimeError(f"{symbol} {day.isoformat()} hit max_pages={args.max_pages} before completion")
    except (requests_exceptions.RequestException, OSError, PermissionError) as exc:
        if args.require_online:
            raise RuntimeError(f"online Alpaca fetch failed with --require-online: {exc}") from exc
        offline_rows = _offline_stock_rows(kind, symbol, day, start_ms, end_ms)
        if kind == "quotes":
            offline_rows = thin_quotes_per_second(offline_rows)
        if not offline_rows:
            raise
        rows = offline_rows
        pages = 0
        page_token = None

    if kind == "quotes":
        rows = thin_quotes_per_second(rows)

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp-{os.getpid()}")
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        json.dump(rows, f, separators=(",", ":"))
    os.replace(tmp, path)
    status = "fetched" if pages else "offline"
    return {"kind": cache_kind, "symbol": symbol, "day": day.isoformat(), "status": status, "rows": len(rows), "pages": pages, "path": str(path)}


def fetch_btc_bars(day: date, args: argparse.Namespace, headers: dict[str, str]) -> dict[str, Any]:
    symbol = args.btc_symbol
    path = cache_path(Path(args.cache_dir), "crypto-us", "crypto_bars_1Min", symbol, day)
    if path.exists() and not args.refresh:
        count = read_cached_count(path)
        if count is not None:
            return {"kind": "crypto_bars_1Min", "symbol": symbol, "day": day.isoformat(), "status": "cached", "rows": count, "pages": 0, "path": str(path)}

    start_iso, end_iso = session_bounds_utc(day, args.start_hour, args.start_minute, args.end_hour, args.end_minute)
    start_ms = iso_to_ms(start_iso)
    end_ms = iso_to_ms(end_iso)
    url = "https://data.alpaca.markets/v1beta3/crypto/us/bars"
    params: dict[str, Any] = {
        "symbols": symbol,
        "timeframe": "1Min",
        "start": start_iso,
        "end": end_iso,
        "limit": args.limit,
        "sort": "asc",
    }
    rows: list[dict[str, Any]] = []
    page_token = None
    pages = 0
    try:
        with requests.Session() as session:
            while pages < args.max_pages:
                if page_token:
                    params["page_token"] = page_token
                else:
                    params.pop("page_token", None)
                data = alpaca_get(session, url, params, headers, args.timeout, args.retries)
                pages += 1
                bars_map = data.get("bars") or {}
                for bar in bars_map.get(symbol) or []:
                    rows.append({
                        "t": iso_to_ms(bar["t"]),
                        "o": bar.get("o"),
                        "h": bar.get("h"),
                        "l": bar.get("l"),
                        "c": bar.get("c"),
                        "v": bar.get("v"),
                    })
                page_token = data.get("next_page_token")
                if not page_token:
                    break
            if page_token:
                raise RuntimeError(f"{symbol} {day.isoformat()} hit max_pages={args.max_pages} before completion")
    except (requests_exceptions.RequestException, OSError, PermissionError) as exc:
        if args.require_online:
            raise RuntimeError(f"online Alpaca fetch failed with --require-online: {exc}") from exc
        offline_rows = _offline_btc_bars(day, start_ms, end_ms)
        if not offline_rows:
            raise
        rows = offline_rows
        pages = 0
        page_token = None

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp-{os.getpid()}")
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        json.dump(rows, f, separators=(",", ":"))
    os.replace(tmp, path)
    status = "fetched" if pages else "offline"
    return {"kind": "crypto_bars_1Min", "symbol": symbol, "day": day.isoformat(), "status": status, "rows": len(rows), "pages": pages, "path": str(path)}


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Prefetch Alpaca market data cache files for engine replay.")
    ap.add_argument("--start", required=True, help="Start date YYYY-MM-DD.")
    ap.add_argument("--end", required=True, help="End date YYYY-MM-DD.")
    ap.add_argument("--tickers", nargs="+", default=["CLSK", "MARA", "RIOT"])
    ap.add_argument("--kinds", nargs="+", default=["trades"], choices=["trades", "quotes", "btc"])
    ap.add_argument("--feed", default="sip", choices=["sip", "iex", "delayed_sip"])
    ap.add_argument("--cache-dir", default=str(DEFAULT_CACHE_DIR))
    ap.add_argument("--workers", type=int, default=2, help="Concurrent symbol/day fetches. Keep modest to respect rate limits.")
    ap.add_argument("--limit", type=int, default=10000)
    ap.add_argument("--max-pages", type=int, default=2000)
    ap.add_argument("--timeout", type=int, default=30)
    ap.add_argument("--retries", type=int, default=10)
    ap.add_argument("--refresh", action="store_true")
    ap.add_argument("--require-online", action="store_true",
                    help="Fail instead of falling back to locally captured incremental rows when Alpaca is unreachable.")
    ap.add_argument("--start-hour", type=int, default=8)
    ap.add_argument("--start-minute", type=int, default=30)
    ap.add_argument("--end-hour", type=int, default=15)
    ap.add_argument("--end-minute", type=int, default=0)
    ap.add_argument("--btc-symbol", default="BTC/USD")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    load_dotenv()
    key = os.getenv("ALPACA_API_KEY")
    secret = os.getenv("ALPACA_SECRET_KEY")
    if not key or not secret:
        raise SystemExit("ALPACA_API_KEY and ALPACA_SECRET_KEY are required")
    headers = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}
    days = market_days(parse_day(args.start), parse_day(args.end))
    jobs: list[tuple[str, str, date]] = []
    for day in days:
        for kind in args.kinds:
            if kind == "btc":
                jobs.append((kind, args.btc_symbol, day))
            else:
                for symbol in args.tickers:
                    jobs.append((kind, symbol.upper(), day))
    print(f"[prefetch] jobs={len(jobs)} days={len(days)} kinds={','.join(args.kinds)} tickers={','.join(args.tickers)} feed={args.feed} workers={args.workers}", flush=True)

    fetched = cached = offline = rows = pages = 0
    failures: list[str] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {}
        for kind, symbol, day in jobs:
            if kind == "btc":
                futures[pool.submit(fetch_btc_bars, day, args, headers)] = (kind, symbol, day)
            else:
                futures[pool.submit(fetch_stock_kind, kind, symbol, day, args, headers)] = (kind, symbol, day)
        for future in as_completed(futures):
            kind, symbol, day = futures[future]
            try:
                result = future.result()
                rows += int(result["rows"])
                pages += int(result["pages"])
                if result["status"] == "cached":
                    cached += 1
                elif result["status"] == "offline":
                    offline += 1
                else:
                    fetched += 1
                print(f"[prefetch] {result['status']} {result['kind']} {result['day']} {result['symbol']} rows={result['rows']} pages={result['pages']}", flush=True)
            except Exception as exc:
                failures.append(f"{kind} {day.isoformat()} {symbol}: {exc}")
                print(f"[prefetch] failed {kind} {day.isoformat()} {symbol}: {exc}", flush=True)

    summary = {
        "jobs": len(jobs),
        "fetched": fetched,
        "cached": cached,
        "offline": offline,
        "rows": rows,
        "pages": pages,
        "failures": failures,
    }
    print(json.dumps(summary, indent=2), flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
