"""Benchmark Step 2 cache gate, load, and active scoring paths."""
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
import step2_manifest_resolver
import step2_parity_contract


HERE = Path(__file__).resolve().parent
CT = ZoneInfo("America/Chicago")
OUT_DIR = HERE / "postmortem" / "cache_benchmarks"


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


def _write_json(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    os.replace(tmp, path)
    return str(path.resolve())


def _load_config() -> dict[str, Any]:
    try:
        return json.loads((HERE / "trading_config.json").read_text(encoding="utf-8"))
    except Exception:
        return {}


def _time(label: str, fn: Callable[[], Any]) -> tuple[dict[str, Any], Any]:
    started = time.perf_counter()
    try:
        value = fn()
        return {
            "label": label,
            "ok": True,
            "elapsed_sec": round(time.perf_counter() - started, 6),
        }, value
    except Exception as exc:
        return {
            "label": label,
            "ok": False,
            "elapsed_sec": round(time.perf_counter() - started, 6),
            "error": repr(exc),
        }, None


def _manifest_summary(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    try:
        manifest = json.loads(p.read_text(encoding="utf-8-sig"))
    except Exception:
        manifest = {}
    return {
        "path": str(p.resolve()) if p.exists() else str(p),
        "exists": p.exists(),
        "name": p.parent.name,
        "range_linked": bool(manifest.get("range_linked")),
        "rows": manifest.get("rows") or manifest.get("row_count"),
        "day_count": manifest.get("day_count") or len(manifest.get("day_map") or {}),
        "array_payload_sha256": manifest.get("array_payload_sha256") or manifest.get("arrays_sha256"),
        "compiled_tape_hash": manifest.get("compiled_tape_hash"),
    }


def _score_active(compiled: dict[str, Any], start_balance: float) -> dict[str, Any]:
    cfg = _load_config()
    sim_config = step2_parity_contract.sim_config(cfg)
    row = decision_tape_compiled.simulate_variants(
        compiled,
        [active_engine_baseline.active_variant()],
        float(start_balance),
        gate=None,
        sim_config=sim_config,
    )
    if row is None:
        raise RuntimeError("compiled Step 2 simulation unavailable")
    return row[0]


def benchmark(args: argparse.Namespace) -> dict[str, Any]:
    timings: list[dict[str, Any]] = []
    base_name = args.name or step2_manifest_resolver.default_name(args.start, args.end, args.tickers)
    linked_name = args.linked_name or step2_manifest_resolver.linked_name(base_name)
    monolithic_manifest = step2_manifest_resolver.manifest_path(base_name, args.compiled_dir)
    linked_manifest = step2_manifest_resolver.manifest_path(linked_name, args.compiled_dir)

    timing, resolved = _time("resolve_best_manifest", lambda: step2_manifest_resolver.resolve_best_manifest(
        start=args.start,
        end=args.end,
        tickers=args.tickers,
        compiled_dir=args.compiled_dir,
        name=base_name,
        linked_manifest_name=linked_name,
        allow_link=not args.no_link,
        write_certification=not args.no_write_certification,
        write_receipt=args.write_receipt,
        label=args.label or "cache_benchmark",
        workers=args.workers,
    ))
    timings.append(timing)

    loaded: dict[str, Any] = {}
    for label, path in (("monolithic", monolithic_manifest), ("linked", linked_manifest)):
        if not Path(path).exists():
            timings.append({"label": f"load_{label}", "ok": False, "elapsed_sec": 0.0, "error": "manifest_missing"})
            continue
        timing, compiled = _time(f"load_{label}", lambda p=path: decision_tape_compiled.load_compiled(str(p), mmap=True))
        timings.append(timing)
        if compiled is not None:
            loaded[label] = {
                "rows": compiled.get("rows"),
                "loaded_from_day_shards": bool(compiled.get("loaded_from_day_shards")),
                "manifest": _manifest_summary(path),
            }
            timing, active_row = _time(f"score_active_{label}", lambda c=compiled: _score_active(c, args.start_balance))
            timings.append(timing)
            if active_row is not None:
                pnl = active_row.get("step2_pnl")
                if pnl is None:
                    pnl = active_row.get("pnl")
                decision_full = active_row.get("decision_full") if isinstance(active_row.get("decision_full"), dict) else {}
                if pnl is None:
                    pnl = decision_full.get("pnl")
                loaded[label]["active_score"] = {
                    "variant": active_row.get("variant"),
                    "step2_pnl": pnl,
                    "trades": decision_full.get("trades"),
                }

    payload = {
        "schema_version": 1,
        "source": "step2_cache_benchmark",
        "created_at_ct": _now_ct(),
        "requested": {
            "start": args.start,
            "end": args.end,
            "tickers": args.tickers,
            "compiled_dir": str(Path(args.compiled_dir).resolve()),
            "name": base_name,
            "linked_name": linked_name,
        },
        "resolved": step2_manifest_resolver.compact_resolution(resolved or {}) if isinstance(resolved, dict) else {},
        "manifests": {
            "monolithic": _manifest_summary(monolithic_manifest),
            "linked": _manifest_summary(linked_manifest),
        },
        "loaded": loaded,
        "timings": timings,
        "notes": [
            "The first score timing in a fresh process may include compiled-kernel warmup; rerun for steady-state scoring comparisons.",
        ],
    }
    stamp = datetime.now(CT).strftime("%Y%m%d_%H%M%S")
    payload["path"] = _write_json(OUT_DIR / f"step2_cache_benchmark_{stamp}.json", payload)
    return payload


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Benchmark Step 2 cache resolve/load/score timings.")
    ap.add_argument("--start", default=step2_manifest_resolver.DEFAULT_START)
    ap.add_argument("--end", default=step2_manifest_resolver.DEFAULT_END)
    ap.add_argument("--tickers", nargs="*", default=step2_manifest_resolver.DEFAULT_TICKERS)
    ap.add_argument("--compiled-dir", default=str(step2_manifest_resolver.COMPILED_DIR))
    ap.add_argument("--name", default="")
    ap.add_argument("--linked-name", default="")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--start-balance", type=float, default=100000.0)
    ap.add_argument("--no-link", action="store_true")
    ap.add_argument("--no-write-certification", action="store_true")
    ap.add_argument("--write-receipt", action="store_true")
    ap.add_argument("--label", default="")
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
            "resolved": payload.get("resolved"),
            "timings": payload.get("timings"),
            "loaded": payload.get("loaded"),
        }, indent=2, sort_keys=True, default=str))
    return 0 if (payload.get("resolved") or {}).get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
