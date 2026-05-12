"""Execute Step 2 validation plans emitted by hunt learning artifacts."""
from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any

import decision_tape_compiled
import routed_scoring_profile as routed
import scoring_variant_lab as lab
import step2_manifest_resolver
import step2_parity_contract
import step2_score_cache


HERE = Path(__file__).resolve().parent
DEFAULT_OUT = HERE / "postmortem" / "backtests" / "step2_validation_executor"


def _read_json(path: str | os.PathLike[str], default: Any = None) -> Any:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except Exception:
        return default


def _write_json(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    os.replace(tmp, path)
    return str(path.resolve())


def _load_trading_config() -> dict[str, Any]:
    return _read_json(HERE / "trading_config.json", {}) or {}


def _sim_config() -> dict[str, Any]:
    return step2_parity_contract.sim_config(_load_trading_config())


def _variant_from_task(task: dict[str, Any], name: str | None = None) -> Any:
    payload = {
        "name": name or task.get("variant") or "validation_variant",
        "weights": task.get("weights") or {},
        "bias": float(task.get("bias") or 0.0),
        "routes": task.get("routes") or [],
    }
    if payload["routes"]:
        return routed.variant_from_dict(payload)
    return lab.Variant(str(payload["name"]), dict(payload["weights"] or {}), float(payload["bias"] or 0.0))


def _route_dicts(task: dict[str, Any]) -> list[dict[str, Any]]:
    return [dict(route) for route in (task.get("routes") or []) if isinstance(route, dict)]


def _counterfactual_variant(task: dict[str, Any], spec: dict[str, Any]) -> Any:
    base_routes = _route_dicts(task)
    action = str(spec.get("action") or "")
    routes = []
    for route in base_routes:
        route = dict(route)
        match = dict(route.get("match") or {})
        if action == "remove_matching_route":
            continue
        if action == "keep_only_match_keys":
            keep = set(spec.get("match_keys") or [])
            route["match"] = {k: v for k, v in match.items() if k in keep}
        elif action == "exact_current_match":
            route["match"] = dict(spec.get("match") or match)
        elif action == "replace_route_action":
            route["action"] = str(spec.get("route_action") or route.get("action") or "score")
        routes.append(route)
    return routed.variant_from_dict({
        "name": f"{task.get('variant')}_{spec.get('name')}",
        "weights": task.get("weights") or {},
        "bias": float(task.get("bias") or 0.0),
        "routes": routes,
    })


def _scale_weights(weights: dict[str, Any], scale: float, features: list[str] | None = None) -> dict[str, float]:
    out = {}
    keep = set(features or [])
    for key, value in (weights or {}).items():
        if features and key not in keep:
            out[key] = float(value or 0.0)
        else:
            out[key] = round(float(value or 0.0) * float(scale), 6)
    return out


def _adversarial_variant(task: dict[str, Any], spec: dict[str, Any]) -> Any:
    weights = dict(task.get("weights") or {})
    bias = float(task.get("bias") or 0.0)
    routes = _route_dicts(task)
    if spec.get("bias_delta") is not None:
        bias += float(spec.get("bias_delta") or 0.0)
    if spec.get("weight_scale") is not None:
        features = list(spec.get("features") or []) or None
        weights = _scale_weights(weights, float(spec.get("weight_scale") or 1.0), features)
        scaled_routes = []
        for route in routes:
            route = dict(route)
            route["weights"] = _scale_weights(dict(route.get("weights") or {}), float(spec.get("weight_scale") or 1.0), features)
            scaled_routes.append(route)
        routes = scaled_routes
    return routed.variant_from_dict({
        "name": f"{task.get('variant')}_{spec.get('name')}",
        "weights": weights,
        "bias": round(bias, 6),
        "routes": routes,
    }) if routes else lab.Variant(f"{task.get('variant')}_{spec.get('name')}", weights, round(bias, 6))


def _score(
    compiled: dict[str, Any],
    variants: list[Any],
    args: argparse.Namespace,
    sim_config_overlay: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    sim_config = _sim_config()
    if sim_config_overlay:
        sim_config.update(dict(sim_config_overlay))
    rows = step2_score_cache.score_variants_cached(
        compiled,
        variants,
        float(args.start_balance),
        gate=None,
        sim_config=sim_config,
        cache_db=args.score_cache_db,
    )
    if rows is None:
        raise RuntimeError("compiled Step 2 simulation unavailable")
    out = []
    for row in rows:
        full = row.get("decision_full") or {}
        out.append({
            "variant": row.get("variant"),
            "weights": row.get("weights") or {},
            "bias": float(row.get("bias") or 0.0),
            "routes": row.get("routes") or [],
            "step2_pnl": float(full.get("pnl") or 0.0),
            "step2_trades": int(full.get("trades") or 0),
            "step2_win_rate_pct": full.get("win_rate_pct"),
            "by_day": full.get("by_day") or {},
            "by_ticker": full.get("by_ticker") or {},
            "by_side": full.get("by_side") or {},
            "score_cache": row.get("score_cache") or {},
        })
    return out


def _resolve_manifest(args: argparse.Namespace) -> str:
    if args.compiled_decision_tape:
        return str(Path(args.compiled_decision_tape).resolve())
    resolved = step2_manifest_resolver.resolve_best_manifest(
        start=args.cache_start,
        end=args.cache_end,
        tickers=args.cache_tickers,
        require_certified=True,
        write_receipt=True,
        label=args.name,
    )
    if not resolved.get("ok"):
        raise RuntimeError(f"cache resolution failed: {resolved.get('reason') or resolved.get('blockers')}")
    return str(resolved["manifest_path"])


def execute_counterfactual_plan(compiled: dict[str, Any], plan: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    results = []
    for task in (plan.get("tasks") or [])[: int(args.limit)]:
        baseline = _variant_from_task(task, name=f"{task.get('variant')}_baseline")
        specs = list(task.get("counterfactuals") or [])
        variants = [baseline] + [_counterfactual_variant(task, spec) for spec in specs]
        scored = _score(compiled, variants, args)
        baseline_pnl = float(scored[0].get("step2_pnl") or 0.0) if scored else 0.0
        rows = []
        for spec, row in zip([{"name": "baseline"}] + specs, scored):
            row = dict(row)
            row["validation_name"] = spec.get("name")
            row["delta_vs_baseline"] = round(float(row.get("step2_pnl") or 0.0) - baseline_pnl, 4)
            rows.append(row)
        disabled = next((row for row in rows if row.get("validation_name") == "route_disabled"), None)
        results.append({
            "variant": task.get("variant"),
            "route_key": task.get("route_key"),
            "baseline_step2_pnl": baseline_pnl,
            "route_disabled_delta": (disabled or {}).get("delta_vs_baseline"),
            "causal_route_signal": bool(disabled and float(disabled.get("delta_vs_baseline") or 0.0) < -abs(baseline_pnl) * 0.005),
            "rows": rows,
        })
    return {"kind": "counterfactual", "result_count": len(results), "results": results}


def execute_adversarial_plan(compiled: dict[str, Any], plan: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    results = []
    for task in (plan.get("tasks") or [])[: int(args.limit)]:
        baseline = _variant_from_task(task, name=f"{task.get('variant')}_baseline")
        specs = list(task.get("perturbations") or [])
        scored = _score(compiled, [baseline], args)
        baseline_pnl = float(scored[0].get("step2_pnl") or 0.0) if scored else 0.0
        rows = []
        worst_delta = 0.0
        baseline_row = dict(scored[0])
        baseline_row["validation_name"] = "baseline"
        baseline_row["delta_vs_baseline"] = 0.0
        rows.append(baseline_row)
        for spec in specs:
            row = _score(
                compiled,
                [_adversarial_variant(task, spec)],
                args,
                sim_config_overlay=spec.get("sim_config_overlay") if isinstance(spec.get("sim_config_overlay"), dict) else None,
            )[0]
            row = dict(row)
            row["validation_name"] = spec.get("name")
            row["delta_vs_baseline"] = round(float(row.get("step2_pnl") or 0.0) - baseline_pnl, 4)
            worst_delta = min(worst_delta, float(row["delta_vs_baseline"]))
            rows.append(row)
        results.append({
            "variant": task.get("variant"),
            "route_key": task.get("route_key"),
            "baseline_step2_pnl": baseline_pnl,
            "worst_delta_vs_baseline": round(worst_delta, 4),
            "stability_pass": bool(worst_delta > -max(500.0, abs(baseline_pnl) * 0.02)),
            "rows": rows,
        })
    return {"kind": "adversarial", "result_count": len(results), "results": results}


def run(args: argparse.Namespace) -> dict[str, Any]:
    manifest_path = _resolve_manifest(args)
    compiled = decision_tape_compiled.load_compiled(manifest_path, mmap=True)
    plan = _read_json(args.plan_json, {}) or {}
    started = time.perf_counter()
    kind = args.kind
    if kind == "auto":
        if plan.get("tasks") and (plan.get("tasks")[0] or {}).get("counterfactuals") is not None:
            kind = "counterfactual"
        elif plan.get("tasks") and (plan.get("tasks")[0] or {}).get("perturbations") is not None:
            kind = "adversarial"
        else:
            raise RuntimeError("could not infer plan kind")
    if kind == "counterfactual":
        payload = execute_counterfactual_plan(compiled, plan, args)
    elif kind == "adversarial":
        payload = execute_adversarial_plan(compiled, plan, args)
    else:
        raise RuntimeError(f"unsupported validation kind: {kind}")
    payload.update({
        "schema_version": 1,
        "source": "step2_validation_executor",
        "compiled_decision_tape": manifest_path,
        "plan_json": str(Path(args.plan_json).resolve()),
        "elapsed_sec": round(time.perf_counter() - started, 3),
    })
    out_dir = Path(args.out_dir) / args.name
    payload["path"] = _write_json(out_dir / f"{kind}_validation_results.json", payload)
    return payload


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Execute Step 2 counterfactual/adversarial validation plans.")
    ap.add_argument("--plan-json", required=True)
    ap.add_argument("--kind", choices=["auto", "counterfactual", "adversarial"], default="auto")
    ap.add_argument("--name", default="step2_validation")
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT))
    ap.add_argument("--compiled-decision-tape", default="")
    ap.add_argument("--cache-start", default=step2_manifest_resolver.DEFAULT_START)
    ap.add_argument("--cache-end", default=step2_manifest_resolver.DEFAULT_END)
    ap.add_argument("--cache-tickers", nargs="*", default=step2_manifest_resolver.DEFAULT_TICKERS)
    ap.add_argument("--score-cache-db", default=str(step2_score_cache.DEFAULT_CACHE_DB))
    ap.add_argument("--start-balance", type=float, default=100000.0)
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument("--json", action="store_true")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    payload = run(args)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    else:
        print(json.dumps({"path": payload.get("path"), "kind": payload.get("kind"), "result_count": payload.get("result_count")}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
