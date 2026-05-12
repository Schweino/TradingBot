from __future__ import annotations

import argparse
import json
import random
import time
from pathlib import Path
from types import SimpleNamespace

import active_engine_baseline
import decision_tape_compiled
import scoring_variant_lab as lab
import step2_adaptive_hunter as hunter
import step2_parity_contract
import tournament_safety


HERE = Path(__file__).resolve().parent
DEFAULT_MANIFEST = HERE / "postmortem" / "backtests" / "compiled_decision_tapes" / (
    "compiled_step2_current_live_CLSK-MARA-RIOT_2026-04-06_2026-05-08"
) / "manifest.json"
DEFAULT_OUT = HERE / "postmortem" / "backtests" / "step2_adaptive_hunter" / "hunt_100k_live_chain_20260509"


def _read_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return default


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def _load_config() -> dict:
    return _read_json(HERE / "trading_config.json", {})


def _score(compiled: dict, variants: list[lab.Variant], args: SimpleNamespace) -> list[dict]:
    return hunter._score(compiled, variants, args)


def _row_id(row: dict) -> str:
    return tournament_safety.model_id(
        str(row.get("variant") or "candidate"),
        row.get("weights") or {},
        float(row.get("bias") or 0.0),
    )


def _variant_from_row(row: dict) -> lab.Variant:
    return hunter._variant(str(row["variant"]), dict(row.get("weights") or {}), float(row.get("bias") or 0.0))


def _dedupe_sort(rows: list[dict], limit: int) -> list[dict]:
    by_id: dict[str, dict] = {}
    for row in rows:
        if not row.get("weights"):
            continue
        key = _row_id(row)
        old = by_id.get(key)
        if old is None or float(row.get("step2_pnl") or 0.0) > float(old.get("step2_pnl") or 0.0):
            by_id[key] = row
    return sorted(by_id.values(), key=lambda r: float(r.get("step2_pnl") or 0.0), reverse=True)[:limit]


def _decorate(row: dict, active_pnl: float, champion_pnl: float) -> dict:
    pnl = float(row.get("step2_pnl") or 0.0)
    row["step2_delta_vs_active"] = round(pnl - active_pnl, 4)
    row["step2_delta_pct_vs_active"] = round((pnl / active_pnl - 1.0) * 100.0, 4) if active_pnl else None
    row["step2_delta_vs_prior_champion"] = round(pnl - champion_pnl, 4)
    row["step2_delta_pct_vs_prior_champion"] = round((pnl / champion_pnl - 1.0) * 100.0, 4) if champion_pnl else None
    return row


def _load_seed_rows(paths: list[str], limit: int) -> list[lab.Variant]:
    seeds: list[lab.Variant] = [hunter._active_seed()]
    seen = {tournament_safety.model_id(seeds[0].name, seeds[0].weights, seeds[0].bias)}
    for raw in paths:
        payload = _read_json(Path(raw), {})
        candidates = []
        if isinstance(payload.get("leaderboard"), list):
            candidates.extend(payload["leaderboard"])
        if isinstance(payload.get("top"), list):
            candidates.extend(payload["top"])
        for row in sorted(candidates, key=lambda r: float(r.get("step2_pnl") or r.get("pnl") or 0.0), reverse=True)[:limit]:
            if not isinstance(row, dict) or not row.get("weights"):
                continue
            name = str(row.get("variant") or row.get("name") or f"seed_{len(seeds)}")
            bias = float(row.get("bias") or 0.0)
            key = tournament_safety.model_id(name, row.get("weights") or {}, bias)
            if key in seen:
                continue
            seen.add(key)
            seeds.append(hunter._variant(name, dict(row.get("weights") or {}), bias))
    return seeds


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--compiled-decision-tape", default=str(DEFAULT_MANIFEST))
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT))
    ap.add_argument("--batches", type=int, default=10)
    ap.add_argument("--batch-size", type=int, default=1000)
    ap.add_argument("--seed", type=int, default=2026050915)
    ap.add_argument("--seed-json", action="append", default=[])
    ap.add_argument("--seed-limit", type=int, default=250)
    ap.add_argument("--weight-limit", type=float, default=10.0)
    ap.add_argument("--start-balance", type=float, default=100000.0)
    ap.add_argument("--target-pnl", type=float, default=100000.0)
    ap.add_argument("--milestone-pct", type=float, default=5.0)
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    checkpoint_path = out_dir / "chain_checkpoint.json"
    checkpoint = _read_json(checkpoint_path, {})
    started = time.perf_counter()

    compiled = decision_tape_compiled.load_compiled(args.compiled_decision_tape, mmap=True)
    sim_config = step2_parity_contract.sim_config(_load_config())
    score_args = SimpleNamespace(
        start_balance=args.start_balance,
        max_trades_per_day=0,
        max_trades_per_ticker_day=0,
    )
    active = active_engine_baseline.active_variant()
    active_row = _score(compiled, [active], score_args)[0]
    active_pnl = float(active_row["step2_pnl"])

    if checkpoint:
        batch_idx = int(checkpoint.get("completed_batches") or 0)
        elites = list(checkpoint.get("leaderboard") or [])
        champions = list(checkpoint.get("milestones") or [])
        champion_pnl = float((champions[-1] if champions else active_row).get("step2_pnl") or active_pnl)
        seen_ids = set(checkpoint.get("seen_ids") or [])
    else:
        batch_idx = 0
        elites = [active_row]
        champions = []
        champion_pnl = active_pnl
        seen_ids = {_row_id(active_row)}

    seed_variants = _load_seed_rows(args.seed_json, int(args.seed_limit))
    new_milestones = []
    target_reached = False
    rng = random.Random(int(args.seed) + batch_idx * 99991)

    for _ in range(int(args.batches)):
        batch_idx += 1
        variants: list[lab.Variant] = []
        scale = max(0.05, 1.1 * (0.992 ** batch_idx))
        while len(variants) < int(args.batch_size):
            roll = rng.random()
            source_rows = elites[: min(len(elites), 200)]
            if source_rows and roll < 0.82:
                seed = _variant_from_row(rng.choice(source_rows))
                variant = hunter._mutate(
                    seed,
                    rng,
                    batch_idx * 1_000_000 + len(variants),
                    scale,
                    float(args.weight_limit),
                )
            elif seed_variants and roll < 0.96:
                variant = hunter._mutate(
                    rng.choice(seed_variants),
                    rng,
                    batch_idx * 1_000_000 + len(variants),
                    scale,
                    float(args.weight_limit),
                )
            else:
                variant = hunter._random_sparse(rng, batch_idx * 1_000_000 + len(variants))
            key = tournament_safety.model_id(variant.name, variant.weights, variant.bias)
            if key in seen_ids:
                continue
            seen_ids.add(key)
            variants.append(variant)

        scored = _score(compiled, variants, score_args)
        scored = [_decorate(row, active_pnl, champion_pnl) for row in scored]
        elites = _dedupe_sort(elites + scored, 3000)
        best = elites[0]
        threshold = champion_pnl * (1.0 + float(args.milestone_pct) / 100.0)
        if float(best["step2_pnl"]) >= threshold:
            milestone = _decorate(dict(best), active_pnl, champion_pnl)
            milestone["milestone_number"] = len(champions) + 1
            milestone["prior_champion_pnl"] = round(champion_pnl, 4)
            milestone["threshold_pnl"] = round(threshold, 4)
            champions.append(milestone)
            new_milestones.append(milestone)
            champion_pnl = float(best["step2_pnl"])
        if float(best["step2_pnl"]) >= float(args.target_pnl):
            target_reached = True
            break

        checkpoint = {
            "completed_batches": batch_idx,
            "scored_total": batch_idx * int(args.batch_size),
            "active_step2_pnl": active_pnl,
            "target_pnl": float(args.target_pnl),
            "champion_pnl": champion_pnl,
            "milestones": champions,
            "leaderboard": elites[:100],
            "seen_ids": list(seen_ids)[-200000:],
            "compiled_decision_tape": str(Path(args.compiled_decision_tape).resolve()),
        }
        _write_json(checkpoint_path, checkpoint)

    checkpoint = {
        "completed_batches": batch_idx,
        "scored_total": batch_idx * int(args.batch_size),
        "active_step2_pnl": active_pnl,
        "target_pnl": float(args.target_pnl),
        "target_reached": target_reached or bool(elites and float(elites[0]["step2_pnl"]) >= float(args.target_pnl)),
        "champion_pnl": champion_pnl,
        "milestones": champions,
        "new_milestones": new_milestones,
        "leaderboard": elites[:100],
        "seen_ids": list(seen_ids)[-200000:],
        "compiled_decision_tape": str(Path(args.compiled_decision_tape).resolve()),
        "elapsed_sec_this_run": round(time.perf_counter() - started, 3),
    }
    _write_json(checkpoint_path, checkpoint)
    print(json.dumps({
        "checkpoint": str(checkpoint_path),
        "completed_batches": checkpoint["completed_batches"],
        "scored_total": checkpoint["scored_total"],
        "active_step2_pnl": active_pnl,
        "best_pnl": float(elites[0]["step2_pnl"]) if elites else active_pnl,
        "champion_pnl": champion_pnl,
        "new_milestones": [
            {
                "milestone_number": row.get("milestone_number"),
                "variant": row.get("variant"),
                "step2_pnl": row.get("step2_pnl"),
                "delta_pct_vs_prior": row.get("step2_delta_pct_vs_prior_champion"),
            }
            for row in new_milestones
        ],
        "target_reached": checkpoint["target_reached"],
        "elapsed_sec_this_run": checkpoint["elapsed_sec_this_run"],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
