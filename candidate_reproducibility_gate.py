"""Reproduce a selected Step 2 candidate before/after promotion.

The point of this gate is simple: a candidate is not promotable merely because
an old JSON row says it won. We reload the compiled tape named by the Step 2
evaluation envelope, rerun the selected weights through the compiled Step 2
kernel, and compare the reproduced score to the artifact row.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import active_engine_baseline
import baseline_drift_sentinel
import candidate_profile_schema
import compiled_tape_lineage
import decision_tape_compiled
import routed_scoring_profile
import scoring_variant_lab as lab
import step2_parity_contract
import tournament_safety


HERE = Path(__file__).resolve().parent
POSTMORTEM_DIR = HERE / "postmortem"
OUT_DIR = POSTMORTEM_DIR / "candidate_reproducibility"
CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1
PNL_TOLERANCE = 0.01
NUMERIC_TOLERANCE = 1e-6


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


def _read_json(path: str | os.PathLike[str]) -> dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _write_json(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.{int(time.time() * 1000)}.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
        f.write("\n")
    os.replace(tmp, path)
    return str(path.resolve())


def _stable_hash(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _num(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            return None
        return number
    except Exception:
        return None


def _int(value: Any) -> int | None:
    try:
        if value in (None, ""):
            return None
        return int(value)
    except Exception:
        return None


def _first(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _decision(row: dict[str, Any]) -> dict[str, Any]:
    for key in ("decision_full", "result"):
        if isinstance(row.get(key), dict):
            return row[key]
    score = row.get("score") if isinstance(row.get("score"), dict) else {}
    if isinstance(score.get("result"), dict):
        return score["result"]
    return {}


def _expected_summary(row: dict[str, Any]) -> dict[str, Any]:
    decision = _decision(row)
    step2 = row.get("step2") if isinstance(row.get("step2"), dict) else {}
    out: dict[str, Any] = {
        "pnl": _first(row.get("step2_pnl"), row.get("pnl"), decision.get("pnl"), step2.get("pnl")),
        "trades": _first(row.get("step2_trades"), row.get("trades"), decision.get("trades"), step2.get("trades")),
        "win_rate_pct": _first(row.get("step2_win_rate_pct"), row.get("win_rate_pct"), decision.get("win_rate_pct"), step2.get("win_rate_pct")),
        "by_ticker": _first(row.get("by_ticker"), decision.get("by_ticker"), step2.get("by_ticker")),
        "by_day": _first(row.get("by_day"), decision.get("by_day"), step2.get("by_day")),
        "skipped": _first(row.get("skipped"), decision.get("skipped"), step2.get("skipped")),
    }
    for key in ("wins", "losses"):
        direct = _first(row.get(f"step2_{key}"), row.get(key), decision.get(key))
        step_value = step2.get(key)
        if direct is not None:
            out[key] = direct
        elif step_value not in (None, 0, 0.0):
            out[key] = step_value
    return {key: value for key, value in out.items() if value is not None}


def _reproduced_summary(row: dict[str, Any]) -> dict[str, Any]:
    full = row.get("decision_full") if isinstance(row.get("decision_full"), dict) else {}
    summary = {
        "pnl": _first(full.get("pnl"), row.get("pnl")),
        "trades": _first(full.get("trades"), row.get("trades")),
        "wins": full.get("wins"),
        "losses": full.get("losses"),
        "win_rate_pct": full.get("win_rate_pct"),
        "by_ticker": full.get("by_ticker"),
        "by_day": full.get("by_day"),
        "skipped": full.get("skipped"),
    }
    if isinstance(row.get("route_audit"), dict):
        summary["route_audit"] = row["route_audit"]
    if isinstance(row.get("routes"), list):
        summary["routes"] = row["routes"]
        summary["routed_scoring_profile"] = bool(row.get("routed_scoring_profile") or row.get("routes"))
    return summary


def _clean_weights(weights: Any) -> dict[str, float]:
    if not isinstance(weights, dict) or not weights:
        return {}
    return {
        str(key): round(float(value), 8)
        for key, value in weights.items()
        if abs(float(value or 0.0)) > 1e-12
    }


def _route_payloads(row: dict[str, Any]) -> list[dict[str, Any]]:
    routes = row.get("routes")
    if not isinstance(routes, list):
        return []
    return [dict(route) for route in routes if isinstance(route, dict)]


def _variant_from_row(row: dict[str, Any], *, name: str | None = None) -> Any:
    weights = row.get("weights")
    if not isinstance(weights, dict) or not weights:
        raise RuntimeError("candidate row has no weights")
    variant_name = name or str(row.get("variant") or row.get("name") or "candidate")
    clean = _clean_weights(weights)
    routes = _route_payloads(row)
    if routes or row.get("routed_scoring_profile"):
        return routed_scoring_profile.variant_from_dict({
            "name": variant_name,
            "weights": clean,
            "bias": round(float(row.get("bias") or 0.0), 8),
            "routes": routes,
        })
    return lab.Variant(
        variant_name,
        clean,
        round(float(row.get("bias") or 0.0), 8),
    )


def _candidate_model_id(row: dict[str, Any]) -> str:
    variant = _variant_from_row(row)
    if routed_scoring_profile.is_routed_variant(variant):
        return tournament_safety.stable_json_hash({
            "name": variant.name,
            "weights": dict(variant.weights),
            "bias": round(float(variant.bias or 0.0), 8),
            "routes": routed_scoring_profile.routes_to_dicts(variant),
        }, length=20)
    return tournament_safety.model_id(variant.name, variant.weights, variant.bias)


def _route_semantics(variant: Any) -> list[dict[str, Any]]:
    if not routed_scoring_profile.is_routed_variant(variant):
        return []
    return routed_scoring_profile.routes_to_dicts(variant)


def _envelope(payload: dict[str, Any]) -> dict[str, Any]:
    value = payload.get("step2_evaluation_envelope")
    return value if isinstance(value, dict) else {}


def _compiled_manifest_path(payload: dict[str, Any], row: dict[str, Any]) -> str:
    envelope = _envelope(payload)
    compiled = envelope.get("compiled_decision_tape") if isinstance(envelope.get("compiled_decision_tape"), dict) else {}
    manifest = compiled.get("manifest") if isinstance(compiled.get("manifest"), dict) else {}
    candidates = [
        manifest.get("path"),
        payload.get("compiled_decision_tape"),
        payload.get("compiled_tape_manifest"),
    ]
    data = row.get("data") if isinstance(row.get("data"), dict) else {}
    candidates.append(data.get("compiled_decision_tape"))
    for value in candidates:
        if value:
            return str(value)
    return ""


def _starting_balance(payload: dict[str, Any], row: dict[str, Any]) -> float:
    envelope = _envelope(payload)
    run_args = envelope.get("run_args") if isinstance(envelope.get("run_args"), dict) else {}
    data = row.get("data") if isinstance(row.get("data"), dict) else {}
    for value in (
        run_args.get("start_balance"),
        payload.get("start_balance"),
        data.get("start_balance"),
        100000.0,
    ):
        number = _num(value)
        if number is not None:
            return float(number)
    return 100000.0


def _sim_config(payload: dict[str, Any]) -> dict[str, Any]:
    envelope = _envelope(payload)
    sim = envelope.get("sim_config") if isinstance(envelope.get("sim_config"), dict) else {}
    if sim:
        return dict(sim)
    cfg = _read_json(HERE / "trading_config.json")
    return dict(step2_parity_contract.sim_config(cfg))


def _compare_scalar(name: str, expected: Any, actual: Any) -> dict[str, Any]:
    expected_num = _num(expected)
    actual_num = _num(actual)
    if expected_num is not None or actual_num is not None:
        tolerance = PNL_TOLERANCE if name.endswith("pnl") or name == "pnl" else NUMERIC_TOLERANCE
        diff = None if expected_num is None or actual_num is None else round(actual_num - expected_num, 10)
        return {
            "name": name,
            "ok": expected_num is not None and actual_num is not None and abs(actual_num - expected_num) <= tolerance,
            "expected": expected,
            "actual": actual,
            "delta": diff,
            "tolerance": tolerance,
        }
    return {
        "name": name,
        "ok": expected == actual,
        "expected": expected,
        "actual": actual,
    }


def _compare_nested(name: str, expected: Any, actual: Any, checks: list[dict[str, Any]]) -> None:
    if expected is None:
        return
    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            checks.append({"name": name, "ok": False, "expected_type": "dict", "actual": actual})
            return
        for key in sorted(expected):
            _compare_nested(f"{name}.{key}", expected.get(key), actual.get(key), checks)
        return
    if isinstance(expected, list):
        if not isinstance(actual, list):
            checks.append({"name": name, "ok": False, "expected_type": "list", "actual": actual})
            return
        checks.append({"name": f"{name}.length", "ok": len(expected) == len(actual), "expected": len(expected), "actual": len(actual)})
        for idx, item in enumerate(expected[: min(len(expected), len(actual))]):
            _compare_nested(f"{name}[{idx}]", item, actual[idx], checks)
        return
    checks.append(_compare_scalar(name, expected, actual))


def _compare(expected: dict[str, Any], reproduced: dict[str, Any]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    for key in ("pnl", "trades", "wins", "losses", "win_rate_pct", "by_ticker", "by_day", "skipped"):
        if key in expected:
            _compare_nested(key, expected.get(key), reproduced.get(key), checks)
    return checks


def _run_variant(
    *,
    compiled_path: str,
    variant: Any,
    starting_balance: float,
    sim_config: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    compiled = decision_tape_compiled.load_compiled(compiled_path, mmap=True)
    rows = decision_tape_compiled.simulate_variants(
        compiled,
        [variant],
        starting_balance,
        gate=None,
        sim_config=sim_config,
    )
    if rows is None or not rows:
        raise RuntimeError("compiled Step 2 simulation unavailable")
    return rows[0], compiled.get("manifest") or {}


def evaluate_payload(
    payload: dict[str, Any],
    row: dict[str, Any],
    *,
    candidate_json: str = "",
    rank: int = 1,
    variant: str = "",
    use_live_profile: bool = False,
) -> dict[str, Any]:
    created = _now_ct()
    source_is_step2 = baseline_drift_sentinel.source_looks_step2(payload)
    envelope = _envelope(payload)
    expected = _expected_summary(row)
    compiled_path = _compiled_manifest_path(payload, row)
    sim_config = _sim_config(payload)
    starting_balance = _starting_balance(payload, row)
    checks: list[dict[str, Any]] = []

    def add(name: str, ok: bool, actual: Any = None, expected_value: Any = None) -> None:
        item = {"name": name, "ok": bool(ok)}
        if actual is not None:
            item["actual"] = actual
        if expected_value is not None:
            item["expected"] = expected_value
        checks.append(item)

    add("source_is_step2_payload", source_is_step2, source_is_step2, True)
    add("step2_evaluation_envelope_present", bool(envelope), bool(envelope), True)
    add("compiled_manifest_path_present", bool(compiled_path), compiled_path, "non-empty")
    add("candidate_expected_pnl_present", expected.get("pnl") is not None, expected.get("pnl"), "non-empty")

    reproduced: dict[str, Any] = {}
    compiled_manifest: dict[str, Any] = {}
    error = None
    try:
        selected_variant = active_engine_baseline.active_variant() if use_live_profile else _variant_from_row(row)
        if use_live_profile:
            expected_variant = _variant_from_row(row)
            add("live_profile_weights_match_candidate", selected_variant.weights == expected_variant.weights, selected_variant.weights, expected_variant.weights)
            add("live_profile_bias_matches_candidate", abs(float(selected_variant.bias) - float(expected_variant.bias)) <= NUMERIC_TOLERANCE, selected_variant.bias, expected_variant.bias)
            add("live_profile_routes_match_candidate", _route_semantics(selected_variant) == _route_semantics(expected_variant), _route_semantics(selected_variant), _route_semantics(expected_variant))
        raw, compiled_manifest = _run_variant(
            compiled_path=compiled_path,
            variant=selected_variant,
            starting_balance=starting_balance,
            sim_config=sim_config,
        )
        reproduced = _reproduced_summary(raw)
        lineage = compiled_manifest.get("lineage_validation") if isinstance(compiled_manifest.get("lineage_validation"), dict) else {}
        add(
            "compiled_tape_lineage_present",
            bool(lineage),
            lineage.get("status") if lineage else None,
            "lineage_validation",
        )
        if lineage:
            add(
                "compiled_tape_lineage_certified",
                lineage.get("status") == "CERTIFIED_MATCH" and lineage.get("certified") is True,
                lineage.get("status"),
                "CERTIFIED_MATCH",
            )
            add(
                "compiled_tape_lineage_quick_score_allowed",
                bool(lineage.get("quick_score_allowed")),
                lineage.get("status"),
                "quick_score_allowed",
            )
            add(
                "compiled_tape_lineage_not_unsafe",
                not bool(lineage.get("rebuild_required")) and lineage.get("status") != "UNSAFE_SCORE_DRIFT",
                lineage.get("status"),
                "not UNSAFE_SCORE_DRIFT",
            )
        compiled_expected = ((envelope.get("compiled_decision_tape") or {}).get("compiled_tape_hash")
                             if isinstance(envelope.get("compiled_decision_tape"), dict) else "")
        compiled_actual = compiled_manifest.get("compiled_tape_hash")
        if compiled_expected:
            add("compiled_tape_hash_matches_envelope", compiled_actual == compiled_expected, compiled_actual, compiled_expected)
        checks.extend(_compare(expected, reproduced))
    except compiled_tape_lineage.CompiledTapeLineageError as exc:
        error = repr(exc)
        lineage = exc.lineage if isinstance(getattr(exc, "lineage", None), dict) else {}
        add("compiled_tape_lineage_not_unsafe", False, lineage.get("status") or error, "not UNSAFE_SCORE_DRIFT")
        add("candidate_reproduction_ran", False, error, "successful rerun")
    except Exception as exc:
        error = repr(exc)
        add("candidate_reproduction_ran", False, error, "successful rerun")
    else:
        add("candidate_reproduction_ran", True, "successful rerun", "successful rerun")

    ok = bool(checks) and all(check.get("ok") for check in checks)
    mode = "live_canary" if use_live_profile else "pre_promotion_reproducibility"
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "candidate_reproducibility_gate",
        "mode": mode,
        "created_at_ct": created,
        "ok": ok,
        "candidate_json": os.path.abspath(candidate_json) if candidate_json else "",
        "rank": rank,
        "variant": variant,
        "candidate_model_id": _candidate_model_id(row) if isinstance(row.get("weights"), dict) else "",
        "source_is_step2": source_is_step2,
        "step2_evaluation_envelope_hash": envelope.get("envelope_hash") or payload.get("step2_evaluation_envelope_hash"),
        "baseline_profile_hash": envelope.get("baseline_profile_hash") or payload.get("baseline_profile_hash"),
        "compiled_manifest_path": compiled_path,
        "compiled_manifest_hash": tournament_safety._file_sha256(compiled_path) if compiled_path else None,
        "compiled_tape_hash": compiled_manifest.get("compiled_tape_hash"),
        "starting_balance": starting_balance,
        "sim_config_hash": _stable_hash(sim_config),
        "sim_config": sim_config,
        "expected": expected,
        "reproduced": reproduced,
        "checks": checks,
        "failed_checks": [check for check in checks if not check.get("ok")],
        "error": error,
        "deduction": (
            "Promotion is blocked unless this exact selected candidate reproduces on the compiled "
            "Step 2 tape referenced by its evaluation envelope."
        ),
    }


def evaluate_file(
    candidate_json: str,
    *,
    rank: int = 1,
    variant: str = "",
    write: bool = False,
    label: str = "",
    use_live_profile: bool = False,
) -> dict[str, Any]:
    payload = _read_json(candidate_json)
    if not payload:
        raise RuntimeError(f"candidate artifact is missing or invalid: {candidate_json}")
    row = candidate_profile_schema.select(payload, rank=rank, variant=variant)
    report = evaluate_payload(
        payload,
        row,
        candidate_json=candidate_json,
        rank=rank,
        variant=variant,
        use_live_profile=use_live_profile,
    )
    if write:
        write_report(report, label=label)
    return report


def write_report(report: dict[str, Any], *, label: str = "") -> str:
    suffix = label or report.get("mode") or "candidate"
    candidate = report.get("variant") or report.get("candidate_model_id") or "candidate"
    safe = "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in f"{suffix}_{candidate}")[:140]
    path = OUT_DIR / f"{datetime.now(CT).strftime('%Y%m%d_%H%M%S')}_{safe}.json"
    report["output_path"] = str(path.resolve())
    report["report_hash"] = _stable_hash({k: v for k, v in report.items() if k not in {"report_hash", "output_path"}})
    return _write_json(path, report)


def _main() -> int:
    parser = argparse.ArgumentParser(description="Rerun a selected Step 2 candidate and compare it to its artifact score.")
    parser.add_argument("--candidate-json", required=True)
    parser.add_argument("--rank", type=int, default=1)
    parser.add_argument("--variant", default="")
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--label", default="")
    parser.add_argument("--live-canary", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    report = evaluate_file(
        args.candidate_json,
        rank=args.rank,
        variant=args.variant,
        write=args.write,
        label=args.label,
        use_live_profile=args.live_canary,
    )
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True, default=str))
    else:
        print(
            f"ok={report.get('ok')} mode={report.get('mode')} "
            f"pnl={((report.get('reproduced') or {}).get('pnl'))} "
            f"expected={((report.get('expected') or {}).get('pnl'))}"
        )
    return 0 if report.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(_main())
