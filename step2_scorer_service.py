"""Resident Step 2 scorer.

The daemon mode speaks newline-delimited JSON over stdin/stdout. It loads the
certified compiled cache once, keeps compiled kernels warm, and scores batches
through the shared Step 2 score cache.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import active_engine_baseline
import decision_tape_compiled
import routed_scoring_profile
import scoring_variant_lab as lab
import step2_manifest_resolver
import step2_parity_contract
import step2_score_cache


def _read_config() -> dict[str, Any]:
    try:
        return json.loads((Path(__file__).resolve().parent / "trading_config.json").read_text(encoding="utf-8"))
    except Exception:
        return {}


def _variant_from_payload(payload: dict[str, Any]) -> Any:
    if payload.get("routes"):
        return routed_scoring_profile.variant_from_dict(payload)
    if isinstance(payload.get("base_weights"), dict):
        return SimpleNamespace(
            name=str(payload.get("name") or payload.get("variant") or "variant"),
            weights=dict(payload.get("weights") or {}),
            bias=float(payload.get("bias") or 0.0),
            base_weights=dict(payload.get("base_weights") or {}),
            base_bias=float(payload.get("base_bias") or 0.0),
        )
    return lab.Variant(
        str(payload.get("name") or payload.get("variant") or "variant"),
        dict(payload.get("weights") or {}),
        float(payload.get("bias") or 0.0),
    )


def _variant_payload_key(payload: dict[str, Any]) -> str:
    return step2_score_cache._stable_hash({
        "name": str(payload.get("name") or payload.get("variant") or "candidate"),
        "weights": dict(payload.get("weights") or {}),
        "bias": float(payload.get("bias") or 0.0),
        "routes": list(payload.get("routes") or []),
    }, length=32)


def _compact_row(row: dict[str, Any], *, summary: bool = False) -> dict[str, Any]:
    full = row.get("decision_full") if isinstance(row.get("decision_full"), dict) else {}
    cache = row.get("score_cache") or {}
    if summary:
        return {
            "variant": row.get("variant"),
            "step2_pnl": float(full.get("pnl") or 0.0),
            "step2_trades": int(full.get("trades") or 0),
            "step2_win_rate_pct": full.get("win_rate_pct"),
            "score_cache": cache,
        }
    return {
        "variant": row.get("variant"),
        "weights": row.get("weights") or {},
        "bias": float(row.get("bias") or 0.0),
        "routes": row.get("routes") or [],
        "routed_scoring_profile": bool(row.get("routed_scoring_profile")),
        "step2_pnl": float(full.get("pnl") or 0.0),
        "step2_trades": int(full.get("trades") or 0),
        "step2_win_rate_pct": full.get("win_rate_pct"),
        "by_day": full.get("by_day") or {},
        "by_ticker": full.get("by_ticker") or {},
        "by_side": full.get("by_side") or {},
        "skipped": full.get("skipped") or {},
        "score_cache": cache,
    }


class ResidentStep2Scorer:
    def __init__(
        self,
        *,
        manifest_path: str = "",
        start: str = step2_manifest_resolver.DEFAULT_START,
        end: str = step2_manifest_resolver.DEFAULT_END,
        tickers: list[str] | None = None,
        cache_db: str = str(step2_score_cache.DEFAULT_CACHE_DB),
        start_balance: float = 100000.0,
        mmap: bool = True,
        day_subset: list[str] | None = None,
    ):
        started = time.perf_counter()
        self.cache_db = cache_db
        self.start_balance = float(start_balance)
        self.config = _read_config()
        self.sim_config = step2_parity_contract.sim_config(self.config)
        if manifest_path:
            self.resolution = {
                "ok": True,
                "manifest_path": str(Path(manifest_path).resolve()),
                "source": "explicit_manifest",
            }
            self.compiled = decision_tape_compiled.load_compiled(str(manifest_path), mmap=mmap, day_subset=day_subset or None)
        else:
            self.compiled, self.resolution = step2_manifest_resolver.load_best_compiled(
                start=start,
                end=end,
                tickers=tickers or step2_manifest_resolver.DEFAULT_TICKERS,
                mmap=mmap,
                validate_sources=True,
                write_receipt=True,
                label="resident_scorer",
            )
            if day_subset:
                self.compiled = decision_tape_compiled.select_days(self.compiled, day_subset)
        self.loaded_at = time.time()
        self.load_elapsed_sec = round(time.perf_counter() - started, 6)
        self.score_calls = 0
        self.variants_scored = 0

    def score(
        self,
        variants: list[Any],
        *,
        compact: bool = True,
        summary: bool = False,
        sort_results: bool = True,
    ) -> list[dict[str, Any]]:
        started = time.perf_counter()
        rows = step2_score_cache.score_variants_cached(
            self.compiled,
            variants,
            self.start_balance,
            gate=None,
            sim_config=self.sim_config,
            cache_db=self.cache_db,
            use_cache=True,
        )
        if rows is None:
            raise RuntimeError("compiled Step 2 simulation unavailable")
        self.score_calls += 1
        self.variants_scored += len(variants)
        out = [_compact_row(row, summary=summary) for row in rows] if compact else rows
        if sort_results:
            out.sort(key=lambda row: float(row.get("step2_pnl") or ((row.get("decision_full") or {}).get("pnl") or -1e18)), reverse=True)
        for row in out:
            row.setdefault("resident_scorer", {})["score_elapsed_sec"] = round(time.perf_counter() - started, 6)
        return out

    def active(self) -> dict[str, Any]:
        return self.score([active_engine_baseline.active_variant()], compact=True)[0]

    def stats(self) -> dict[str, Any]:
        cache = step2_score_cache.ScoreCache(self.cache_db)
        try:
            cache_stats = cache.stats()
        finally:
            cache.close()
        manifest = self.compiled.get("manifest") if isinstance(self.compiled.get("manifest"), dict) else {}
        return {
            "loaded_at": self.loaded_at,
            "load_elapsed_sec": self.load_elapsed_sec,
            "manifest_path": self.resolution.get("manifest_path"),
            "compiled_tape_hash": manifest.get("compiled_tape_hash"),
            "rows": self.compiled.get("rows"),
            "loaded_from_day_shards": bool(self.compiled.get("loaded_from_day_shards")),
            "score_calls": self.score_calls,
            "variants_scored": self.variants_scored,
            "cache": cache_stats,
        }


def _handle_command(scorer: ResidentStep2Scorer, payload: dict[str, Any]) -> dict[str, Any]:
    cmd = str(payload.get("cmd") or "score")
    if cmd == "score":
        payload_rows = [row for row in (payload.get("variants") or []) if isinstance(row, dict)]
        variants = [_variant_from_payload(row) for row in payload_rows]
        preserve_order = bool(payload.get("preserve_order"))
        rows = scorer.score(
            variants,
            compact=not bool(payload.get("raw")),
            summary=bool(payload.get("summary")),
            sort_results=not preserve_order,
        )
        if preserve_order:
            for row, source in zip(rows, payload_rows):
                row["variant_key"] = source.get("variant_key") or _variant_payload_key(source)
        return {"ok": True, "cmd": cmd, "rows": rows, "stats": scorer.stats()}
    if cmd == "active":
        return {"ok": True, "cmd": cmd, "row": scorer.active(), "stats": scorer.stats()}
    if cmd == "stats":
        return {"ok": True, "cmd": cmd, "stats": scorer.stats()}
    if cmd == "shutdown":
        return {"ok": True, "cmd": cmd, "shutdown": True, "stats": scorer.stats()}
    return {"ok": False, "cmd": cmd, "error": "unknown_command"}


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run a resident Step 2 scorer daemon.")
    ap.add_argument("--compiled-manifest", default="")
    ap.add_argument("--start", default=step2_manifest_resolver.DEFAULT_START)
    ap.add_argument("--end", default=step2_manifest_resolver.DEFAULT_END)
    ap.add_argument("--tickers", nargs="*", default=step2_manifest_resolver.DEFAULT_TICKERS)
    ap.add_argument("--cache-db", default=str(step2_score_cache.DEFAULT_CACHE_DB))
    ap.add_argument("--start-balance", type=float, default=100000.0)
    ap.add_argument("--day-subset", nargs="*", default=[])
    ap.add_argument("--once", default="", help="Score one JSON file containing {variants:[...]}.")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    scorer = ResidentStep2Scorer(
        manifest_path=args.compiled_manifest,
        start=args.start,
        end=args.end,
        tickers=args.tickers,
        cache_db=args.cache_db,
        start_balance=args.start_balance,
        day_subset=args.day_subset or None,
    )
    if args.once:
        payload = json.loads(Path(args.once).read_text(encoding="utf-8-sig"))
        print(json.dumps(_handle_command(scorer, {"cmd": "score", **payload}), indent=2, sort_keys=True, default=str))
        return 0

    print(json.dumps({"ok": True, "event": "ready", "stats": scorer.stats()}, sort_keys=True, default=str), flush=True)
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
            response = _handle_command(scorer, payload if isinstance(payload, dict) else {})
        except Exception as exc:
            response = {"ok": False, "error": repr(exc)}
        print(json.dumps(response, sort_keys=True, default=str), flush=True)
        if response.get("shutdown"):
            break
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
