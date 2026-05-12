"""Benchmark direct, shared-cache, resident, and queue Step 2 scoring paths."""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import active_engine_baseline
import decision_tape_compiled
import step2_candidate_work_queue
import step2_manifest_resolver
import step2_parity_contract
import step2_score_cache
from step2_scorer_client import ResidentScorerClient


HERE = Path(__file__).resolve().parent
CT = ZoneInfo("America/Chicago")
OUT_DIR = HERE / "postmortem" / "cache_benchmarks"
SCRATCH_DIR = OUT_DIR / "_scratch"


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


def _write_json(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    os.replace(tmp, path)
    return str(path.resolve())


def _scratch_db(label: str) -> str:
    SCRATCH_DIR.mkdir(parents=True, exist_ok=True)
    stamp = int(time.time() * 1000)
    return str((SCRATCH_DIR / f"{label}_{os.getpid()}_{stamp}.sqlite").resolve())


def _time(label: str, fn: Callable[[], Any]) -> tuple[dict[str, Any], Any]:
    started = time.perf_counter()
    try:
        value = fn()
        return {"label": label, "ok": True, "elapsed_sec": round(time.perf_counter() - started, 6)}, value
    except Exception as exc:
        return {"label": label, "ok": False, "elapsed_sec": round(time.perf_counter() - started, 6), "error": repr(exc)}, None


def _config() -> dict[str, Any]:
    try:
        return json.loads((HERE / "trading_config.json").read_text(encoding="utf-8"))
    except Exception:
        return {}


def _active_score(row: dict[str, Any] | None) -> dict[str, Any]:
    if not row:
        return {}
    full = row.get("decision_full") if isinstance(row.get("decision_full"), dict) else {}
    return {
        "variant": row.get("variant"),
        "step2_pnl": row.get("step2_pnl") if row.get("step2_pnl") is not None else full.get("pnl"),
        "trades": row.get("step2_trades") if row.get("step2_trades") is not None else full.get("trades"),
        "score_cache": row.get("score_cache") or {},
    }


def benchmark(args: argparse.Namespace) -> dict[str, Any]:
    SCRATCH_DIR.mkdir(parents=True, exist_ok=True)
    timings: list[dict[str, Any]] = []
    timing, resolved = _time("resolve_best_manifest", lambda: step2_manifest_resolver.resolve_best_manifest(
        start=args.start,
        end=args.end,
        tickers=args.tickers,
        write_receipt=False,
    ))
    timings.append(timing)
    manifest = str((resolved or {}).get("manifest_path") or args.compiled_manifest)
    if not manifest:
        raise RuntimeError("no compiled manifest resolved")

    timing, compiled = _time("load_compiled", lambda: decision_tape_compiled.load_compiled(manifest, mmap=True))
    timings.append(timing)
    variant = active_engine_baseline.active_variant()
    sim_config = step2_parity_contract.sim_config(_config())
    scores: dict[str, Any] = {}

    timing, direct_rows = _time("direct_score_active", lambda: decision_tape_compiled.simulate_variants(
        compiled,
        [variant],
        args.start_balance,
        gate=None,
        sim_config=sim_config,
    ))
    timings.append(timing)
    scores["direct"] = _active_score((direct_rows or [{}])[0] if direct_rows else None)

    cold_db = _scratch_db("stack_score_cache")
    timing, cold_rows = _time("score_cache_cold_active", lambda: step2_score_cache.score_variants_cached(
        compiled,
        [variant],
        args.start_balance,
        sim_config=sim_config,
        cache_db=cold_db,
    ))
    timings.append(timing)
    scores["score_cache_cold"] = _active_score((cold_rows or [{}])[0] if cold_rows else None)

    timing, warm_rows = _time("score_cache_warm_active", lambda: step2_score_cache.score_variants_cached(
        compiled,
        [variant],
        args.start_balance,
        sim_config=sim_config,
        cache_db=cold_db,
    ))
    timings.append(timing)
    scores["score_cache_warm"] = _active_score((warm_rows or [{}])[0] if warm_rows else None)

    queue_db = _scratch_db("stack_queue")
    queue = step2_candidate_work_queue.CandidateWorkQueue(queue_db)
    try:
        queue.submit([step2_score_cache.variant_payload(variant)], source="stack_benchmark")
    finally:
        queue.close()
    timing, queue_payload = _time("queue_resident_score_active", lambda: step2_candidate_work_queue.score_pending(
        queue_db=queue_db,
        manifest_path=manifest,
        batch_size=1,
        limit=1,
        cache_db=cold_db,
        start_balance=args.start_balance,
        workers=1,
        summary=True,
        full_finalists=0,
    ))
    timings.append(timing)
    scores["queue_resident"] = _active_score(((queue_payload or {}).get("leaderboard") or [{}])[0])

    timing, resident_payload = _time("resident_start_and_active", lambda: _resident_once(manifest, args))
    timings.append(timing)
    scores["resident"] = resident_payload or {}

    payload = {
        "schema_version": 1,
        "source": "step2_stack_benchmark",
        "created_at_ct": _now_ct(),
        "manifest_path": manifest,
        "resolved": step2_manifest_resolver.compact_resolution(resolved or {}),
        "scores": scores,
        "timings": timings,
    }
    stamp = datetime.now(CT).strftime("%Y%m%d_%H%M%S")
    payload["path"] = _write_json(OUT_DIR / f"step2_stack_benchmark_{stamp}.json", payload)
    return payload


def _resident_once(manifest: str, args: argparse.Namespace) -> dict[str, Any]:
    cache_db = _scratch_db("resident_score_cache")
    with ResidentScorerClient(
        manifest_path=manifest,
        cache_db=cache_db,
        start_balance=args.start_balance,
    ) as client:
        cold = client.active()
        warm = client.active()
        return {
            "cold": _active_score(cold.get("row") or {}),
            "warm": _active_score(warm.get("row") or {}),
            "stats": warm.get("stats") or {},
        }


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Benchmark Step 2 scoring stack variants.")
    ap.add_argument("--compiled-manifest", default="")
    ap.add_argument("--start", default=step2_manifest_resolver.DEFAULT_START)
    ap.add_argument("--end", default=step2_manifest_resolver.DEFAULT_END)
    ap.add_argument("--tickers", nargs="*", default=step2_manifest_resolver.DEFAULT_TICKERS)
    ap.add_argument("--start-balance", type=float, default=100000.0)
    ap.add_argument("--json", action="store_true")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    payload = benchmark(args)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    else:
        print(json.dumps({
            "path": payload.get("path"),
            "scores": payload.get("scores"),
            "timings": payload.get("timings"),
        }, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
