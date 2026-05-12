"""Multi-profile shadow runner for Live signal rows.

The live trader writes every signal decision to ``live_signal_parity``. This
module lets candidate scoring profiles ride on that same stream without ever
submitting broker orders. Intraday appends are decision-only and intentionally
cheap; day builds replay outcomes from prepared market data and reduce state
through the same execution adapter used by Step 2.
"""
from __future__ import annotations

import argparse
import copy
import json
import os
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import candidate_profile_schema
import execution_adapters
import live_signal_step2_parity
import live_step2_decision_kernel
import scoring_profiles
import step2_execution_contract
import step2_latency_model
import step2_parity_contract
import tournament_safety


HERE = Path(__file__).resolve().parent
POSTMORTEM_DIR = HERE / "postmortem"
LIVE_SIGNAL_DIR = POSTMORTEM_DIR / "live_signal_parity"
OUT_DIR = POSTMORTEM_DIR / "shadow_variants"
CANDIDATE_PATH = OUT_DIR / "candidates.json"
CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1
DEFAULT_TICKERS = ["CLSK", "MARA", "RIOT"]
DEFAULT_START_BALANCE = 100000.0
DEFAULT_MAX_PROFILES = 100

_LIVE_CACHE: dict[str, Any] = {"mtime": None, "profiles": None}
_LIVE_DECISION_STATE: dict[str, dict[str, Any]] = {}


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


def _day(ts: int | float | None = None) -> str:
    return datetime.fromtimestamp(float(ts or time.time()), CT).date().isoformat()


def _write_json(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.{int(time.time() * 1000)}.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
        f.write("\n")
    os.replace(tmp, path)
    return str(path.resolve())


def _append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, sort_keys=True, separators=(",", ":"), default=str) + "\n")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8-sig") as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows


def _num(value: Any, default: float | None = None) -> float | None:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except Exception:
        return default


def _int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return int(default)
        return int(float(value))
    except Exception:
        return int(default)


def _clean_reasons(reasons: Any) -> list[str]:
    out = []
    for reason in reasons or []:
        text = str(reason)
        if text.startswith("scoring_profile:"):
            continue
        out.append(text)
    return out


def _profile_id(profile: dict[str, Any]) -> str:
    weights = profile.get("weights") or {}
    return str(profile.get("model_id") or tournament_safety.model_id(
        str(profile.get("name") or "profile"),
        weights,
        float(profile.get("bias") or 0.0),
    ))


def _normalize_profile(row: dict[str, Any], source: str = "") -> dict[str, Any]:
    weights = row.get("weights") if isinstance(row.get("weights"), dict) else {}
    clean = {
        str(k): round(float(v), 8)
        for k, v in sorted(weights.items())
        if abs(float(v or 0.0)) > 1e-12
    }
    profile = {
        "enabled": bool(row.get("enabled", True)),
        "name": str(row.get("variant") or row.get("name") or "shadow_profile"),
        "bias": round(float(row.get("bias") or 0.0), 8),
        "weights": clean,
        "source": source or row.get("source"),
    }
    profile["model_id"] = _profile_id(profile)
    profile["family_id"] = row.get("family_id") or tournament_safety.variant_family(clean)
    if row.get("step2") is not None:
        profile["step2"] = row.get("step2")
    elif candidate_profile_schema.score(row) != float("-inf"):
        profile["step2"] = {"pnl": candidate_profile_schema.score(row)}
    return profile


def active_profile() -> dict[str, Any]:
    cfg = _read_json(HERE / "trading_config.json")
    profile = step2_parity_contract.active_profile_snapshot(cfg, include_weights=True)
    out = _normalize_profile(
        {
            "enabled": profile.get("enabled", True),
            "name": profile.get("name") or "current_live_active_profile",
            "bias": profile.get("bias"),
            "weights": profile.get("weights") or {},
        },
        source="current_live_config",
    )
    out["baseline"] = "current_live"
    out["profile_hash"] = profile.get("hash")
    return out


def _profile_key(profile: dict[str, Any]) -> tuple[str, str]:
    return (str(profile.get("model_id") or ""), str(profile.get("name") or ""))


def _dedupe_profiles(profiles: list[dict[str, Any]], limit: int = DEFAULT_MAX_PROFILES) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, Any]] = []
    for profile in profiles:
        is_current_live = profile.get("baseline") == "current_live"
        if not is_current_live and not profile.get("enabled", True):
            continue
        if not is_current_live and (not isinstance(profile.get("weights"), dict) or not profile.get("weights")):
            continue
        key = _profile_key(profile)
        if key in seen:
            continue
        seen.add(key)
        out.append(profile)
        if len(out) >= limit:
            break
    return out


def load_candidates(path: str | os.PathLike[str] | None = None,
                    include_active: bool = True,
                    max_profiles: int = DEFAULT_MAX_PROFILES,
                    use_cache: bool = False) -> list[dict[str, Any]]:
    candidate_path = Path(path or CANDIDATE_PATH)
    mtime = candidate_path.stat().st_mtime if candidate_path.exists() else None
    if use_cache and _LIVE_CACHE.get("mtime") == mtime and _LIVE_CACHE.get("profiles") is not None:
        return list(_LIVE_CACHE["profiles"])
    profiles: list[dict[str, Any]] = []
    if include_active:
        profiles.append(active_profile())
    payload = _read_json(candidate_path)
    rows = []
    if isinstance(payload.get("profiles"), list):
        rows.extend(row for row in payload["profiles"] if isinstance(row, dict))
    rows.extend(candidate_profile_schema.rows_from_payload(payload))
    for row in rows:
        profiles.append(_normalize_profile(row, source=str(candidate_path)))
    profiles = _dedupe_profiles(profiles, max_profiles)
    _LIVE_CACHE["mtime"] = mtime
    _LIVE_CACHE["profiles"] = list(profiles)
    return profiles


def discover_candidate_profiles(max_profiles: int = DEFAULT_MAX_PROFILES,
                                max_files: int = 250) -> list[dict[str, Any]]:
    roots = [
        POSTMORTEM_DIR / "promotions" / "active_scoring_profiles",
        POSTMORTEM_DIR / "backtests" / "step2_adaptive_hunter",
        POSTMORTEM_DIR / "backtests" / "pipeline_accelerator",
        POSTMORTEM_DIR / "backtests" / "profiles",
        POSTMORTEM_DIR / "backtests",
    ]
    files: list[Path] = []
    for root in roots:
        if root.exists():
            files.extend(root.rglob("*.json"))
    files = sorted(set(files), key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)[:max_files]
    profiles = [active_profile()]
    for path in files:
        payload = _read_json(path)
        if not payload:
            continue
        for row in candidate_profile_schema.rows_from_payload(payload):
            try:
                profiles.append(_normalize_profile(row, source=str(path)))
            except Exception:
                continue
            if len(profiles) >= max_profiles * 4:
                break
        if len(profiles) >= max_profiles * 4:
            break
    return _dedupe_profiles(profiles, max_profiles)


def write_candidates(profiles: list[dict[str, Any]],
                     path: str | os.PathLike[str] | None = None,
                     source: str = "manual") -> dict[str, Any]:
    profiles = _dedupe_profiles(profiles, DEFAULT_MAX_PROFILES)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "source": source,
        "created_at_ct": _now_ct(),
        "profile_count": len(profiles),
        "profiles": profiles,
        "deduction": (
            "These profiles are passive candidates only. The shadow runner may score and replay them, "
            "but it never submits broker orders."
        ),
    }
    payload["path"] = _write_json(Path(path or CANDIDATE_PATH), payload)
    return payload


def bootstrap_candidates(max_profiles: int = DEFAULT_MAX_PROFILES) -> dict[str, Any]:
    return write_candidates(discover_candidate_profiles(max_profiles=max_profiles), source="auto_discovery")


def signal_from_live_row(row: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    feature = row.get("feature_snapshot") if isinstance(row.get("feature_snapshot"), dict) else {}
    ind = feature.get("indicators") if isinstance(feature.get("indicators"), dict) else {}
    btc_ind = feature.get("btc_indicators") if isinstance(feature.get("btc_indicators"), dict) else {}
    side = str(row.get("side") or "LONG").upper()
    signal = {
        "ticker": str(row.get("ticker") or "").upper(),
        "side": side if side in ("LONG", "SHORT") else "LONG",
        "price": _num(row.get("price")),
        "score": row.get("score"),
        "conviction": row.get("conviction"),
        "setup_type": row.get("setup_type"),
        "session_phase": feature.get("session_phase") or ind.get("session_phase"),
        "reasons": _clean_reasons(row.get("reasons") or []),
        "indicators": ind,
        "btc_indicators": btc_ind,
        "btc_context": feature.get("btc_context") if isinstance(feature.get("btc_context"), dict) else {},
        "signal_quality": feature.get("signal_quality") if isinstance(feature.get("signal_quality"), dict) else {},
        "relative_strength": feature.get("relative_strength") if isinstance(feature.get("relative_strength"), dict) else {},
        "miner_basket": feature.get("miner_basket") if isinstance(feature.get("miner_basket"), dict) else {},
        "lead_lag": feature.get("lead_lag") if isinstance(feature.get("lead_lag"), dict) else {},
    }
    return signal, ind, btc_ind


def score_live_row(profile: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
    signal, ind, btc_ind = signal_from_live_row(row)
    result = scoring_profiles.score_signal(profile, signal, ind, btc_ind, include_details=True)
    side = str(result.get("side") or signal.get("side")).upper()
    abs_score = abs(float(signal.get("score") or 0.0))
    signed_score = abs_score if side == "LONG" else -abs_score
    return {
        "schema_version": SCHEMA_VERSION,
        "profile_name": profile.get("name"),
        "profile_model_id": profile.get("model_id"),
        "score": result.get("score"),
        "side": side,
        "engine_score": signed_score,
        "features": result.get("features") or {},
        "contributions": result.get("contributions") or {},
        "source_side": signal.get("side"),
        "source_score": signal.get("score"),
    }


def _contract() -> dict[str, Any]:
    cfg = _read_json(HERE / "trading_config.json")
    return step2_execution_contract.execution_contract(cfg)


def _shadow_row_for_outcome(row: dict[str, Any], score: dict[str, Any],
                            start_balance: float, tickers: list[str]) -> dict[str, Any]:
    price = _num(row.get("price"), 0.0) or 0.0
    side = str(score.get("side") or row.get("side") or "LONG").upper()
    extra = copy.deepcopy(row.get("extra") if isinstance(row.get("extra"), dict) else {})
    bracket = extra.get("bracket_policy") if isinstance(extra.get("bracket_policy"), dict) else {}
    sl_pct = _num(bracket.get("sl_pct"), 0.004) or 0.004
    tp_pct = _num(bracket.get("tp_pct"), 0.003) or 0.003
    brackets = live_step2_decision_kernel.bracket_prices(side, price, sl_pct, tp_pct)
    alloc = _num(extra.get("alloc"))
    if alloc is None:
        trade_size = float((_contract().get("execution_kernel_contract") or {}).get("trade_size_pct") or 0.25)
        alloc = float(start_balance) / max(1, len(tickers)) * trade_size
    qty = _num(extra.get("qty"))
    if qty is None and price > 0:
        qty = float(alloc or 0.0) / price
    extra.update({
        "alloc": round(float(alloc or 0.0), 2),
        "qty": qty,
        "sl": brackets.get("sl"),
        "tp": brackets.get("tp"),
        "sl_send": brackets.get("sl"),
        "tp_send": brackets.get("tp"),
        "bracket_policy": {**bracket, "sl_pct": sl_pct, "tp_pct": tp_pct, "shadow_variant": True},
    })
    out = copy.deepcopy(row)
    out.update({
        "decision": "entered",
        "reason": "entered",
        "side": side,
        "score": score.get("engine_score"),
        "extra": extra,
    })
    return out


def _decision_path(day: str) -> Path:
    return OUT_DIR / day / f"shadow_variant_decisions_{day}.jsonl"


def _leaderboard_path(day: str) -> Path:
    return OUT_DIR / day / f"shadow_variant_leaderboard_{day}.json"


def _live_decision_path(day: str) -> Path:
    return OUT_DIR / day / f"shadow_variant_live_decisions_{day}.jsonl"


def _prepared_path(day: str, tickers: list[str], feed: str, quote_mode: str,
                   btc_mode: str, prepared_cache_dir: str) -> str:
    return live_signal_step2_parity._prepared_path(day, tickers, feed, quote_mode, btc_mode, prepared_cache_dir)


def _load_price_paths(day: str, tickers: list[str], feed: str, quote_mode: str,
                      btc_mode: str, prepared_cache_dir: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    path = _prepared_path(day, tickers, feed, quote_mode, btc_mode, prepared_cache_dir)
    events = live_signal_step2_parity._load_prepared(path)
    return events, live_signal_step2_parity._exit_path(events)


def build_day(day: str,
              tickers: list[str] | None = None,
              profiles: list[dict[str, Any]] | None = None,
              candidate_path: str | None = None,
              feed: str = "sip",
              quote_mode: str = "per-second",
              btc_mode: str = "bars",
              prepared_cache_dir: str | None = None,
              start_balance: float = DEFAULT_START_BALANCE,
              latency_model_path: str | None = None,
              latency_percentile: str = "p75",
              max_profiles: int = DEFAULT_MAX_PROFILES) -> dict[str, Any]:
    tickers = [str(t).upper() for t in (tickers or DEFAULT_TICKERS)]
    prepared_cache_dir = prepared_cache_dir or str(HERE / "data_cache" / "live_intraday_tapes")
    profiles = profiles or load_candidates(candidate_path, include_active=True, max_profiles=max_profiles)
    signal_path = LIVE_SIGNAL_DIR / f"live_signal_parity_{day}.jsonl"
    signals = [
        row for row in _read_jsonl(signal_path)
        if str(row.get("ticker") or "").upper() in tickers
    ]
    signals.sort(key=lambda r: (_int(r.get("created_at")), str(r.get("parity_key") or "")))
    events, price_paths = _load_price_paths(day, tickers, feed, quote_mode, btc_mode, prepared_cache_dir)
    latency_model = step2_latency_model.load_model(latency_model_path)
    contract = _contract()
    decision_path = _decision_path(day)
    decision_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = decision_path.with_suffix(decision_path.suffix + f".{os.getpid()}.tmp")
    summaries: dict[str, dict[str, Any]] = {}
    adapters: dict[str, execution_adapters.ReplayExecutionAdapter] = {}
    with tmp.open("w", encoding="utf-8") as f:
        for profile in profiles:
            model_id = _profile_id(profile)
            runtime_state = {"positions": {}, "pending_entries": {}, "trades": []}
            adapter = execution_adapters.ReplayExecutionAdapter(day=day, runtime_state=runtime_state)
            adapters[model_id] = adapter
            scheduled_closes: list[dict[str, Any]] = []

            def apply_due_closes(until_ts: int) -> None:
                due = [row for row in scheduled_closes if int(row["close_ts"]) <= until_ts]
                if not due:
                    return
                remaining = [row for row in scheduled_closes if int(row["close_ts"]) > until_ts]
                scheduled_closes[:] = remaining
                for close in sorted(due, key=lambda row: (int(row["close_ts"]), str(row["trade_id"]))):
                    adapter.close_position(
                        str(close["ticker"]),
                        str(close["trade_id"]),
                        str(close["side"]),
                        str(close["reason"]),
                        float(close["exit_price"]),
                        pnl=float(close["pnl"]),
                        ts=int(close["close_ts"]),
                    )

            summaries[model_id] = {
                "profile_name": profile.get("name"),
                "model_id": model_id,
                "source": profile.get("source"),
                "baseline": profile.get("baseline"),
                "signals_seen": 0,
                "entered": 0,
                "skipped": 0,
                "wins": 0,
                "losses": 0,
                "pnl": 0.0,
                "skip_reasons": Counter(),
                "outcome_reasons": Counter(),
                "by_ticker": defaultdict(lambda: {"entered": 0, "pnl": 0.0, "wins": 0, "losses": 0}),
            }
            for idx, signal_row in enumerate(signals):
                ts = _int(signal_row.get("created_at"))
                apply_due_closes(ts)
                parity_key = str(signal_row.get("parity_key") or f"{day}:{idx}")
                score = score_live_row(profile, signal_row)
                block_reason = adapter.entry_block_reason(
                    str(signal_row.get("ticker") or "").upper(),
                    ts,
                    contract,
                    signal_scan_live_mode=True,
                )
                summary = summaries[model_id]
                summary["signals_seen"] += 1
                if block_reason:
                    summary["skipped"] += 1
                    summary["skip_reasons"][block_reason] += 1
                    decision = {
                        "schema_version": SCHEMA_VERSION,
                        "source": "shadow_variant_engine",
                        "day": day,
                        "created_at": ts,
                        "created_at_ct": signal_row.get("created_at_ct"),
                        "parity_key": parity_key,
                        "profile_name": profile.get("name"),
                        "profile_model_id": model_id,
                        "ticker": signal_row.get("ticker"),
                        "side": score.get("side"),
                        "decision": "skipped",
                        "reason": block_reason,
                        "score": score.get("score"),
                        "source_live_decision": signal_row.get("decision"),
                        "source_live_reason": signal_row.get("reason"),
                    }
                    f.write(json.dumps(decision, sort_keys=True, separators=(",", ":"), default=str) + "\n")
                    continue
                shadow_row = _shadow_row_for_outcome(signal_row, score, start_balance, tickers)
                outcome = live_signal_step2_parity._simulate_outcome(
                    shadow_row,
                    price_paths,
                    trade=None,
                    model=latency_model,
                    latency_percentile=latency_percentile,
                )
                ticker = str(signal_row.get("ticker") or "").upper()
                trade_id = f"shadow-{model_id[:12]}-{parity_key}"
                extra = shadow_row.get("extra") or {}
                adapter.commit_entry(
                    ticker,
                    trade_id,
                    str(score.get("side") or ""),
                    float(shadow_row.get("price") or 0.0),
                    qty=_num(extra.get("qty")),
                    alloc=_num(extra.get("alloc")),
                    tp=_num(extra.get("tp")),
                    sl=_num(extra.get("sl")),
                    ts=ts,
                )
                pnl = _num((outcome or {}).get("pnl"), 0.0) or 0.0
                held = _int((outcome or {}).get("held_sec"), 0)
                reason = str((outcome or {}).get("reason") or "no_outcome")
                scheduled_closes.append({
                    "close_ts": ts + max(0, held),
                    "ticker": ticker,
                    "trade_id": trade_id,
                    "side": str(score.get("side") or ""),
                    "reason": reason,
                    "exit_price": float((outcome or {}).get("exit") or shadow_row.get("price") or 0.0),
                    "pnl": pnl,
                })
                summary["entered"] += 1
                summary["pnl"] = round(float(summary["pnl"]) + pnl, 6)
                summary["outcome_reasons"][reason] += 1
                if pnl > 0:
                    summary["wins"] += 1
                elif pnl < 0:
                    summary["losses"] += 1
                bucket = summary["by_ticker"][ticker]
                bucket["entered"] += 1
                bucket["pnl"] = round(float(bucket["pnl"]) + pnl, 6)
                if pnl > 0:
                    bucket["wins"] += 1
                elif pnl < 0:
                    bucket["losses"] += 1
                decision = {
                    "schema_version": SCHEMA_VERSION,
                    "source": "shadow_variant_engine",
                    "day": day,
                    "created_at": ts,
                    "created_at_ct": signal_row.get("created_at_ct"),
                    "parity_key": parity_key,
                    "profile_name": profile.get("name"),
                    "profile_model_id": model_id,
                    "ticker": ticker,
                    "side": score.get("side"),
                    "decision": "entered",
                    "reason": "entered",
                    "score": score.get("score"),
                    "source_side": score.get("source_side"),
                    "source_live_decision": signal_row.get("decision"),
                    "source_live_reason": signal_row.get("reason"),
                    "outcome": outcome,
                }
                f.write(json.dumps(decision, sort_keys=True, separators=(",", ":"), default=str) + "\n")
            apply_due_closes(2_147_483_647)
    os.replace(tmp, decision_path)
    leaderboard = []
    for model_id, summary in summaries.items():
        entered = int(summary["entered"] or 0)
        wins = int(summary["wins"] or 0)
        losses = int(summary["losses"] or 0)
        item = {
            "profile_name": summary["profile_name"],
            "model_id": model_id,
            "source": summary.get("source"),
            "baseline": summary.get("baseline"),
            "signals_seen": summary["signals_seen"],
            "entered": entered,
            "skipped": summary["skipped"],
            "wins": wins,
            "losses": losses,
            "win_rate_pct": round(wins / entered * 100.0, 4) if entered else None,
            "pnl": round(float(summary["pnl"]), 4),
            "skip_reasons": dict(summary["skip_reasons"].most_common(20)),
            "outcome_reasons": dict(summary["outcome_reasons"].most_common(20)),
            "by_ticker": dict(sorted(summary["by_ticker"].items())),
            "adapter_snapshot": adapters[model_id].snapshot(),
        }
        leaderboard.append(item)
    leaderboard.sort(key=lambda row: float(row.get("pnl") or 0.0), reverse=True)
    active = next((row for row in leaderboard if row.get("baseline") == "current_live"), None)
    active_pnl = float((active or {}).get("pnl") or 0.0)
    for idx, row in enumerate(leaderboard, start=1):
        row["rank"] = idx
        row["delta_vs_current_live"] = round(float(row.get("pnl") or 0.0) - active_pnl, 4) if active else None
        row["delta_pct_vs_current_live"] = (
            round((float(row.get("pnl") or 0.0) - active_pnl) / abs(active_pnl) * 100.0, 4)
            if active and abs(active_pnl) > 1e-12 else None
        )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "source": "shadow_variant_engine",
        "created_at_ct": _now_ct(),
        "day": day,
        "tickers": tickers,
        "profile_count": len(profiles),
        "signal_rows": len(signals),
        "prepared_event_rows": len(events),
        "decision_path": str(decision_path.resolve()),
        "candidate_path": str(Path(candidate_path or CANDIDATE_PATH).resolve()),
        "run_context": {
            "feed": feed,
            "quote_mode": quote_mode,
            "btc_mode": btc_mode,
            "prepared_cache_dir": str(Path(prepared_cache_dir).resolve()),
            "start_balance": start_balance,
            "latency_model_path": str(Path(latency_model_path or step2_latency_model.DEFAULT_MODEL_PATH).resolve()),
            "latency_percentile": latency_percentile,
            "execution_contract_hash": step2_execution_contract.execution_contract_hash(_read_json(HERE / "trading_config.json")),
        },
        "current_live_baseline": active,
        "leaderboard": leaderboard,
        "top5": leaderboard[:5],
        "deduction": (
            "This is passive shadow scoring against Live signal rows. Offline day builds include replayed "
            "TP/SL outcomes and same-ticker lifecycle locks; intraday appends are decision telemetry only."
        ),
    }
    payload["path"] = _write_json(_leaderboard_path(day), payload)
    return payload


def append_live_row(row: dict[str, Any], max_profiles: int = DEFAULT_MAX_PROFILES) -> dict[str, Any]:
    day = _day(row.get("created_at"))
    profiles = load_candidates(include_active=True, max_profiles=max_profiles, use_cache=True)
    out_rows = []
    for profile in profiles:
        model_id = _profile_id(profile)
        key = f"{day}:{model_id}"
        state = _LIVE_DECISION_STATE.setdefault(key, {
            "positions": {},
            "pending_entries": {},
            "trades": [],
        })
        score = score_live_row(profile, row)
        decision = {
            "schema_version": SCHEMA_VERSION,
            "source": "shadow_variant_engine_live_append",
            "day": day,
            "created_at": row.get("created_at"),
            "created_at_ct": row.get("created_at_ct"),
            "parity_key": row.get("parity_key"),
            "profile_name": profile.get("name"),
            "profile_model_id": model_id,
            "ticker": row.get("ticker"),
            "side": score.get("side"),
            "score": score.get("score"),
            "decision": "observed",
            "reason": "decision_only_intraday_append",
            "source_live_decision": row.get("decision"),
            "source_live_reason": row.get("reason"),
            "state_hash": (state.get("execution_state") or {}).get("state_hash"),
        }
        out_rows.append(decision)
        _append_jsonl(_live_decision_path(day), decision)
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "shadow_variant_engine_live_append",
        "day": day,
        "rows": len(out_rows),
        "path": str(_live_decision_path(day).resolve()),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Run passive shadow variants on Live signal rows.")
    sub = ap.add_subparsers(dest="cmd")
    boot = sub.add_parser("bootstrap-candidates")
    boot.add_argument("--max-profiles", type=int, default=DEFAULT_MAX_PROFILES)
    boot.add_argument("--json", action="store_true")
    run = sub.add_parser("run-day")
    run.add_argument("day")
    run.add_argument("--candidate-path", default=str(CANDIDATE_PATH))
    run.add_argument("--max-profiles", type=int, default=DEFAULT_MAX_PROFILES)
    run.add_argument("--tickers", nargs="+", default=DEFAULT_TICKERS)
    run.add_argument("--feed", default="sip")
    run.add_argument("--quote-mode", default="per-second")
    run.add_argument("--btc-mode", default="bars")
    run.add_argument("--prepared-cache-dir", default=str(HERE / "data_cache" / "live_intraday_tapes"))
    run.add_argument("--start-balance", type=float, default=DEFAULT_START_BALANCE)
    run.add_argument("--latency-model", default=step2_latency_model.DEFAULT_MODEL_PATH)
    run.add_argument("--latency-percentile", default="p75")
    run.add_argument("--json", action="store_true")
    args = ap.parse_args()
    if args.cmd == "bootstrap-candidates":
        payload = bootstrap_candidates(args.max_profiles)
    elif args.cmd == "run-day":
        payload = build_day(
            args.day,
            tickers=args.tickers,
            candidate_path=args.candidate_path,
            max_profiles=args.max_profiles,
            feed=args.feed,
            quote_mode=args.quote_mode,
            btc_mode=args.btc_mode,
            prepared_cache_dir=args.prepared_cache_dir,
            start_balance=args.start_balance,
            latency_model_path=args.latency_model,
            latency_percentile=args.latency_percentile,
        )
    else:
        ap.print_help()
        return 2
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
