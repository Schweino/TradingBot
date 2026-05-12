"""Market-day runtime state isolation for the mock/live engine.

The durable ledger is allowed to span days. Decision state is not. This module
keeps that boundary explicit so Monday morning cannot inherit Friday's pending
locks, setup pauses, or stale runtime latches.
"""
from __future__ import annotations

from output_paths import output_path

import argparse
import copy
import hashlib
import json
import os
import time
from datetime import datetime
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

try:
    import execution_state_reducer
except Exception:  # pragma: no cover
    execution_state_reducer = None


HERE = os.path.dirname(os.path.abspath(__file__))
STATE_PATH = output_path("mock_trader_state.json")
OUT_DIR = output_path("postmortem", "live_state_rollover")
LATEST_PATH = os.path.join(OUT_DIR, "LIVE_STATE_ROLLOVER_LATEST.json")
CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1

DAILY_BUDGET_KEYS = ("daily_budget_date", "daily_budget_cash", "daily_per_ticker_budget")
BROKER_PENDING_KEYS = (
    "alpaca_order_id",
    "broker_order_id",
    "order_id",
    "client_order_id",
    "broker_client_order_id",
)


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


def _today() -> str:
    return datetime.now(CT).date().isoformat()


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def _day_from_ts(value: Any) -> str | None:
    ts = _int(value, 0)
    if ts <= 0:
        return None
    try:
        return datetime.fromtimestamp(ts, CT).date().isoformat()
    except Exception:
        return None


def _item_day(item: dict[str, Any]) -> str | None:
    for key in (
        "created_at",
        "entry_ts",
        "opened_at",
        "closed_at",
        "last_trade_closed_at",
        "until",
        "ts",
    ):
        day = _day_from_ts(item.get(key))
        if day:
            return day
    return None


def _stable_hash(payload: Any) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _trade_history_hash(state: dict[str, Any]) -> str:
    return _stable_hash(state.get("trades") or [])


def _has_broker_pending_identity(row: dict[str, Any]) -> bool:
    return any(bool(row.get(key)) for key in BROKER_PENDING_KEYS)


def _is_stale_day(item: dict[str, Any], target_day: str, state_day: str | None) -> bool:
    day = _item_day(item)
    if day:
        return day != target_day
    if state_day:
        return state_day != target_day
    return True


def _summarize_state(state: dict[str, Any], target_day: str, now_ts: int) -> dict[str, Any]:
    pending = state.get("pending_entries") or {}
    positions = state.get("positions") or {}
    setup_pauses = state.get("setup_pauses") or {}
    skipped = state.get("skipped_signals") or []
    broker_degraded = state.get("broker_api_degraded")
    state_day = state.get("state_market_day") or state.get("runtime_day")

    def stale_map(rows: dict[str, Any]) -> list[str]:
        out = []
        for key, value in rows.items():
            if isinstance(value, dict) and _is_stale_day(value, target_day, state_day):
                out.append(str(key))
        return sorted(out)

    expired_pauses = []
    for key, value in setup_pauses.items():
        row = value if isinstance(value, dict) else {}
        until = _int(row.get("until"), 0)
        if until and until <= now_ts:
            expired_pauses.append(str(key))

    broker_degraded_day = _item_day(broker_degraded) if isinstance(broker_degraded, dict) else None
    return {
        "state_market_day": state_day,
        "target_day": target_day,
        "position_count": len(positions),
        "pending_count": len(pending),
        "setup_pause_count": len(setup_pauses),
        "skipped_signal_cache_count": len(skipped),
        "trade_count": len(state.get("trades") or []),
        "trade_history_hash": _trade_history_hash(state),
        "balance": state.get("balance"),
        "start_balance": state.get("start_balance"),
        "daily_budget_date": state.get("daily_budget_date"),
        "broker_api_degraded_present": bool(broker_degraded),
        "broker_api_degraded_day": broker_degraded_day,
        "stale_pending_entries": stale_map(pending),
        "stale_open_positions": stale_map(positions),
        "stale_setup_pauses": sorted(set(stale_map(setup_pauses) + expired_pauses)),
    }


def rollover_state(
    state: dict[str, Any],
    target_day: str | None = None,
    *,
    apply: bool = True,
    reason: str = "market_day_rollover",
    actor: str = "live_state_rollover.py",
    now_ts: int | None = None,
) -> dict[str, Any]:
    """Clear stale decision state while preserving durable trade history.

    Positions are never silently cleared. Stale pending entries with broker
    identities are also preserved and reported as a blocker, because clearing
    those would hide an order that may still exist at the broker.
    """
    target_day = target_day or _today()
    now_ts = int(now_ts or time.time())
    before = _summarize_state(state, target_day, now_ts)
    state_day = before.get("state_market_day")

    actions: list[dict[str, Any]] = []
    blocked_pending: list[str] = []

    pending = dict(state.get("pending_entries") or {})
    pending_to_clear = []
    for ticker, row in pending.items():
        row = row if isinstance(row, dict) else {}
        if not _is_stale_day(row, target_day, state_day):
            continue
        if _has_broker_pending_identity(row):
            blocked_pending.append(str(ticker))
        else:
            pending_to_clear.append(str(ticker))

    setup_pauses = dict(state.get("setup_pauses") or {})
    setup_to_clear = []
    for key, row in setup_pauses.items():
        row = row if isinstance(row, dict) else {}
        until = _int(row.get("until"), 0)
        if _is_stale_day(row, target_day, state_day) or (until and until <= now_ts):
            setup_to_clear.append(str(key))

    skipped = list(state.get("skipped_signals") or [])
    skipped_keep = []
    skipped_drop = 0
    for row in skipped:
        if not isinstance(row, dict):
            skipped_drop += 1
            continue
        day = _item_day(row)
        if day == target_day:
            skipped_keep.append(row)
        else:
            skipped_drop += 1

    broker_degraded = state.get("broker_api_degraded")
    broker_degraded_stale = False
    if broker_degraded:
        if isinstance(broker_degraded, dict):
            day = _item_day(broker_degraded)
            broker_degraded_stale = day != target_day
        else:
            broker_degraded_stale = True

    budget_reset = state.get("daily_budget_date") != target_day

    if apply:
        for ticker in pending_to_clear:
            state.setdefault("pending_entries", {}).pop(ticker, None)
        if pending_to_clear:
            actions.append({"action": "cleared_stale_pending_entries", "symbols": sorted(pending_to_clear)})

        for key in setup_to_clear:
            state.setdefault("setup_pauses", {}).pop(key, None)
        if setup_to_clear:
            actions.append({"action": "cleared_stale_setup_pauses", "keys": sorted(setup_to_clear)})

        if skipped_drop:
            state["skipped_signals"] = skipped_keep
            actions.append({"action": "trimmed_skipped_signal_cache", "dropped": skipped_drop})

        if broker_degraded_stale:
            state.pop("broker_api_degraded", None)
            actions.append({"action": "cleared_stale_broker_api_degraded"})

        if budget_reset:
            for key in DAILY_BUDGET_KEYS:
                state.pop(key, None)
            actions.append({"action": "reset_daily_budget_snapshot", "prior_day": before.get("daily_budget_date")})

        state["state_market_day"] = target_day
        state["runtime_day"] = target_day
        state["state_rollover"] = {
            "schema_version": SCHEMA_VERSION,
            "ok": True,
            "target_day": target_day,
            "applied_at": now_ts,
            "applied_at_ct": _now_ct(),
            "reason": reason,
            "actor": actor,
            "actions": actions,
            "blocked_stale_broker_pending_entries": sorted(blocked_pending),
        }
        if execution_state_reducer is not None:
            execution_state_reducer.bootstrap_runtime_state(state, day=target_day)

    after_state = state if apply else copy.deepcopy(state)
    if not apply:
        simulated = copy.deepcopy(state)
        rollover_state(simulated, target_day, apply=True, reason=reason, actor=actor, now_ts=now_ts)
        after = _summarize_state(simulated, target_day, now_ts)
    else:
        after = _summarize_state(after_state, target_day, now_ts)

    checks = []

    def add(name: str, ok: bool, actual: Any = None, expected: Any = None, severity: str = "critical") -> None:
        row = {"name": name, "ok": bool(ok), "severity": severity}
        if actual is not None:
            row["actual"] = actual
        if expected is not None:
            row["expected"] = expected
        checks.append(row)

    add("durable_trade_count_preserved", after.get("trade_count") == before.get("trade_count"), after.get("trade_count"), before.get("trade_count"))
    add("durable_trade_history_hash_preserved", after.get("trade_history_hash") == before.get("trade_history_hash"), after.get("trade_history_hash"), before.get("trade_history_hash"))
    add("balance_preserved", after.get("balance") == before.get("balance"), after.get("balance"), before.get("balance"))
    add("start_balance_preserved", after.get("start_balance") == before.get("start_balance"), after.get("start_balance"), before.get("start_balance"))
    add("state_market_day_matches_target", after.get("state_market_day") == target_day, after.get("state_market_day"), target_day)
    add("no_stale_pending_entries_after_rollover", not after.get("stale_pending_entries"), after.get("stale_pending_entries"), [])
    add("no_stale_setup_pauses_after_rollover", not after.get("stale_setup_pauses"), after.get("stale_setup_pauses"), [])
    add("daily_budget_reset_or_current", after.get("daily_budget_date") in (None, target_day), after.get("daily_budget_date"), f"None|{target_day}")
    add("no_prior_day_open_positions", not before.get("stale_open_positions"), before.get("stale_open_positions"), [])
    add("no_stale_broker_identified_pending_entries", not blocked_pending, sorted(blocked_pending), [])

    critical_failures = [row for row in checks if not row.get("ok") and row.get("severity") == "critical"]
    proof = {
        "schema_version": SCHEMA_VERSION,
        "source": "live_state_rollover",
        "created_at_ct": _now_ct(),
        "target_day": target_day,
        "reason": reason,
        "actor": actor,
        "applied": bool(apply),
        "ok": not critical_failures,
        "critical_failure_count": len(critical_failures),
        "checks": checks,
        "failed_checks": critical_failures,
        "before": before,
        "after": after,
        "actions": actions,
        "blocked_stale_broker_pending_entries": sorted(blocked_pending),
    }
    if apply and isinstance(state.get("state_rollover"), dict):
        state["state_rollover"]["ok"] = proof["ok"]
        state["state_rollover"]["critical_failure_count"] = proof["critical_failure_count"]
    return proof


def _read_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except Exception:
        return default


def _write_json(path: str, payload: dict[str, Any]) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
    os.replace(tmp, path)
    return os.path.abspath(path)


def write_proof(proof: dict[str, Any]) -> str:
    target_day = str(proof.get("target_day") or _today())
    stamp = datetime.now(CT).strftime("%Y%m%d_%H%M%S")
    path = os.path.join(OUT_DIR, f"live_state_rollover_{target_day}_{stamp}.json")
    proof = dict(proof)
    proof["path"] = os.path.abspath(path)
    written = _write_json(path, proof)
    _write_json(LATEST_PATH, {
        "schema_version": SCHEMA_VERSION,
        "source": "live_state_rollover_latest",
        "updated_at_ct": _now_ct(),
        "target_day": target_day,
        "ok": proof.get("ok"),
        "critical_failure_count": proof.get("critical_failure_count"),
        "path": written,
    })
    return written


def check_state_file(path: str = STATE_PATH, target_day: str | None = None) -> dict[str, Any]:
    state = _read_json(path, {}) or {}
    return rollover_state(state, target_day, apply=False, reason="state_file_check")


def apply_state_file(path: str = STATE_PATH, target_day: str | None = None) -> dict[str, Any]:
    state = _read_json(path, {}) or {}
    proof = rollover_state(state, target_day, apply=True, reason="state_file_apply")
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, separators=(",", ":"), default=str)
    os.replace(tmp, path)
    proof["path"] = write_proof(proof)
    return proof


def main() -> int:
    ap = argparse.ArgumentParser(description="Check/apply Live market-day state rollover.")
    ap.add_argument("cmd", choices=("check", "apply"))
    ap.add_argument("day", nargs="?", default="")
    ap.add_argument("--state-path", default=STATE_PATH)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    if args.cmd == "check":
        payload = check_state_file(args.state_path, args.day or None)
    else:
        payload = apply_state_file(args.state_path, args.day or None)
    print(json.dumps(payload if args.json else {"ok": payload.get("ok"), "path": payload.get("path")},
                     indent=2, sort_keys=True, default=str))
    return 0 if payload.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
