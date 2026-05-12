"""Incrementally refresh Step 2 outcomes on an existing decision tape.

This utility leaves signal/features rows intact and refreshes only rows inside
the requested timestamp window. It is called by the fast rebuild planner for
future outcome-only refreshes so the certified current cache is not invalidated
by changing the canonical decision-tape builder.
"""
from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict

import build_decision_tape as bdt
import opportunity_outcome_cache
import replay_artifacts
import step2_latency_model
import worker_policy


def refresh_day(day, args) -> dict:
    out_path = bdt._tape_path(args, day)
    existing_rows = bdt._read_jsonl_gz(out_path) if os.path.exists(out_path) else []
    if not existing_rows:
        return {"day": day.isoformat(), "status": "missing_existing_tape", "out": out_path, "rows": 0}

    tape_args = argparse.Namespace(
        tickers=args.tickers,
        feed=args.feed,
        quote_mode=args.quote_mode,
        btc_mode=args.btc_mode,
        cache_dir=args.cache_dir,
        prepared_cache_dir=args.prepared_cache_dir,
        use_prepared_events=True,
        refresh_prepared_events=False,
        refresh=False,
        max_pages=1000,
    )
    stats = bdt.replay.ReplayStats()
    events = bdt.replay._load_day_events(day, tape_args, stats)
    _start_iso, _end_iso, start_sec, end_sec = bdt.replay._session_bounds_utc(day)
    flatten_ts = bdt.replay._flatten_ts(day)
    refresh_start_ts = int(getattr(args, "decision_start_ts", 0) or 0)
    refresh_end_ts = int(getattr(args, "decision_end_ts", 0) or 0)
    artifact_key = replay_artifacts.key_from_args(args)

    outcome_shards = {}
    for ticker in args.tickers:
        if getattr(args, "use_outcome_shards", True):
            if getattr(args, "step2_latency_mode", "off") != "off":
                outcome_shards[ticker] = replay_artifacts.read_latency_outcome_shard(
                    args.artifact_dir,
                    artifact_key,
                    day,
                    ticker,
                    getattr(args, "step2_latency_mode", "off"),
                    getattr(args, "step2_latency_percentile", "p75"),
                    getattr(args, "step2_latency_model", None),
                )
            else:
                outcome_shards[ticker] = replay_artifacts.read_outcome_shard(args.artifact_dir, artifact_key, day, ticker)

    prices = {}
    price_hashes = {}
    for ticker in args.tickers:
        _ts_values, prices[ticker] = replay_artifacts.exit_series_from_events(events, ticker, start_sec, end_sec)
        price_hashes[ticker] = opportunity_outcome_cache.price_series_hash(prices[ticker])

    outcome_sources = defaultdict(int)
    refreshed = []
    for row in existing_rows:
        ticker = str(row.get("ticker") or "").upper()
        entry_price = bdt.replay._float(row.get("price"))
        entry_ts = int(row.get("ts") or 0)
        if refresh_start_ts > 0 and entry_ts < refresh_start_ts:
            refreshed.append(row)
            outcome_sources["kept_existing_before_refresh_window"] += 1
            continue
        if refresh_end_ts > 0 and entry_ts > refresh_end_ts:
            refreshed.append(row)
            outcome_sources["kept_existing_after_refresh_window"] += 1
            continue
        if ticker not in prices or not entry_price or entry_ts < start_sec:
            refreshed.append(row)
            outcome_sources["missing_price_context"] += 1
            continue
        shard_row = (outcome_shards.get(ticker) or {}).get(entry_ts)
        shard_price = bdt.replay._float((shard_row or {}).get("price"))
        entry_matches_shard = (
            shard_price is not None
            and abs(float(shard_price) - float(entry_price)) <= 0.0001
        )
        if shard_row and entry_matches_shard and shard_row.get("LONG") and shard_row.get("SHORT"):
            outcomes = {"LONG": shard_row["LONG"], "SHORT": shard_row["SHORT"]}
            outcome_sources["outcome_shard"] += 1
        else:
            outcomes, cache_meta = opportunity_outcome_cache.get_or_compute(
                day.isoformat(),
                ticker,
                entry_ts,
                float(entry_price),
                prices[ticker],
                start_sec,
                flatten_ts,
                end_sec,
                args,
                lambda: {
                    "LONG": bdt._outcome("LONG", ticker, entry_ts, float(entry_price), prices[ticker], start_sec, flatten_ts, end_sec, args),
                    "SHORT": bdt._outcome("SHORT", ticker, entry_ts, float(entry_price), prices[ticker], start_sec, flatten_ts, end_sec, args),
                },
                precomputed_price_hash=price_hashes.get(ticker),
            )
            if shard_row and not entry_matches_shard:
                outcome_sources["computed_inline_price_mismatch"] += 1
            else:
                outcome_sources[str(cache_meta.get("source") or "computed_inline")] += 1
        new_row = dict(row)
        new_row["outcomes"] = outcomes
        notes = list(new_row.get("notes") or [])
        notes.append("Outcomes refreshed incrementally from existing signal/features tape.")
        new_row["notes"] = notes
        new_row["outcome_refresh"] = {
            "mode": "incremental_reuse_existing_signals",
            "start_ts": refresh_start_ts,
            "end_ts": refresh_end_ts,
            "step2_latency_mode": getattr(args, "step2_latency_mode", "off"),
            "step2_latency_percentile": getattr(args, "step2_latency_percentile", "p75"),
            "exit_replay_model": replay_artifacts.EXIT_REPLAY_MODEL_VERSION,
        }
        refreshed.append(new_row)

    tape_identity = bdt._write_jsonl_gz(out_path, refreshed)
    manifest = replay_artifacts.write_manifest(
        args.artifact_dir,
        args,
        day,
        {
            "decision_tape": out_path,
            "decision_tape_rows": len(refreshed),
            "decision_tape_schema_version": 2,
            "indicator_mode": getattr(args, "indicator_mode", "live"),
            "step2_latency_mode": getattr(args, "step2_latency_mode", "off"),
            "step2_latency_model": os.path.abspath(getattr(args, "step2_latency_model", step2_latency_model.DEFAULT_MODEL_PATH)),
            "step2_latency_percentile": getattr(args, "step2_latency_percentile", "p75"),
            "exit_replay_model": replay_artifacts.EXIT_REPLAY_MODEL_VERSION,
            "outcome_sources": dict(outcome_sources),
            "signal_source": "incremental_reused_existing_decision_tape",
            "outcome_refresh_window": {
                "start_ts": refresh_start_ts,
                "end_ts": refresh_end_ts,
                "incremental": bool(refresh_start_ts or refresh_end_ts),
            },
            "decision_tape_identity": tape_identity,
        },
    )
    return {
        "day": day.isoformat(),
        "rows": len(refreshed),
        "status": "ok",
        "out": out_path,
        "prepared_events": len(events),
        "artifact_manifest": replay_artifacts.manifest_path(args.artifact_dir, artifact_key, day),
        "fingerprint": manifest.get("fingerprint"),
        "decision_tape_identity": tape_identity,
        "outcome_sources": dict(outcome_sources),
        "outcome_refresh_window": {
            "start_ts": refresh_start_ts,
            "end_ts": refresh_end_ts,
            "incremental": bool(refresh_start_ts or refresh_end_ts),
        },
        "min_ts": min((int(row.get("ts") or 0) for row in refreshed), default=0),
        "max_ts": max((int(row.get("ts") or 0) for row in refreshed), default=0),
    }


def parse_args() -> argparse.Namespace:
    ap = bdt.parse_args()
    ap.use_outcome_shards = not ap.no_outcome_shards
    return ap


def main() -> int:
    args = parse_args()
    args.tickers = [ticker.upper() for ticker in args.tickers]
    days = list(bdt.replay._market_days(bdt.replay._parse_day(args.start), bdt.replay._parse_day(args.end)))
    workers = worker_policy.clamp_workers(args.workers, available=len(days) or 1)
    if workers > 1 and len(days) > 1:
        args_dict = vars(args).copy()
        from concurrent.futures import ProcessPoolExecutor, as_completed
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(refresh_day, day, argparse.Namespace(**args_dict)): day for day in days}
            results = []
            for fut in as_completed(futures):
                row = fut.result()
                results.append(row)
                print(f"[incremental-outcomes] {row.get('day')} rows={row.get('rows')} status={row.get('status')}", flush=True)
        results.sort(key=lambda row: row.get("day") or "")
    else:
        results = [refresh_day(day, args) for day in days]
    print(json.dumps({"results": results}, indent=2, sort_keys=True, default=str))
    return 0 if all(row.get("status") == "ok" for row in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
