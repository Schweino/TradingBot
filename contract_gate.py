"""Hard contract gate for Live/Step 2 parity-critical configuration."""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import execution_kernel
import config_change_journal
import promotion_manifest
import step2_execution_contract
import step2_parity_contract


HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(HERE, "trading_config.json")
OUT_DIR = os.path.join(HERE, "postmortem", "contract_gate")
CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1


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


def _check(name: str, ok: bool, actual: Any = None, expected: Any = None,
           severity: str = "critical") -> dict[str, Any]:
    row = {"name": name, "ok": bool(ok), "severity": severity}
    if actual is not None:
        row["actual"] = actual
    if expected is not None:
        row["expected"] = expected
    return row


def check(config: dict[str, Any] | None = None, day: str | None = None) -> dict[str, Any]:
    cfg = config if config is not None else (_read_json(CONFIG_PATH, {}) or {})
    live_kernel = execution_kernel.contract_from_config(cfg)
    parity = step2_parity_contract.contract(cfg)
    step2_exec = step2_execution_contract.execution_contract(cfg)
    parity_kernel = parity.get("execution_kernel_contract") or {}
    step2_kernel = step2_exec.get("execution_kernel_contract") or {}
    profile = step2_parity_contract.active_profile_snapshot(cfg, include_weights=False)
    manifest_integrity = promotion_manifest.validate_live_profile(cfg)
    config_journal_integrity = config_change_journal.validate_live_config(cfg)
    allow_manifest_legacy_override = os.environ.get("CLAUDE_ALLOW_LEGACY_PROMOTION_MANIFEST", "").strip().lower() in {"1", "true", "yes", "on"}
    checks = [
        _check("active_profile_enabled", bool(profile.get("enabled")), profile.get("enabled"), True),
        _check("active_profile_weights_present", int(profile.get("weight_count") or 0) > 0,
               profile.get("weight_count"), "> 0"),
        _check("live_kernel_hash_present", bool(live_kernel.get("execution_kernel_hash")),
               live_kernel.get("execution_kernel_hash")),
        _check("parity_kernel_hash_present", bool(parity_kernel.get("execution_kernel_hash")),
               parity_kernel.get("execution_kernel_hash")),
        _check("step2_kernel_hash_present", bool(step2_kernel.get("execution_kernel_hash")),
               step2_kernel.get("execution_kernel_hash")),
        _check("live_vs_parity_kernel_hash",
               live_kernel.get("execution_kernel_hash") == parity_kernel.get("execution_kernel_hash"),
               live_kernel.get("execution_kernel_hash"), parity_kernel.get("execution_kernel_hash")),
        _check("live_vs_step2_execution_kernel_hash",
               live_kernel.get("execution_kernel_hash") == step2_kernel.get("execution_kernel_hash"),
               live_kernel.get("execution_kernel_hash"), step2_kernel.get("execution_kernel_hash")),
        _check("trade_size_pct_contract_match",
               live_kernel.get("trade_size_pct") == parity_kernel.get("trade_size_pct") == step2_kernel.get("trade_size_pct"),
               {
                   "live": live_kernel.get("trade_size_pct"),
                   "parity": parity_kernel.get("trade_size_pct"),
                   "step2_execution": step2_kernel.get("trade_size_pct"),
               }),
        _check("same_ticker_cooldown_contract_match",
               live_kernel.get("same_ticker_reentry_cooldown_sec")
               == parity_kernel.get("same_ticker_reentry_cooldown_sec")
               == step2_kernel.get("same_ticker_reentry_cooldown_sec"),
               {
                   "live": live_kernel.get("same_ticker_reentry_cooldown_sec"),
                   "parity": parity_kernel.get("same_ticker_reentry_cooldown_sec"),
                   "step2_execution": step2_kernel.get("same_ticker_reentry_cooldown_sec"),
               }),
        _check("one_open_ticker_contract_match",
               live_kernel.get("one_open_trade_per_ticker")
               == parity_kernel.get("one_open_trade_per_ticker")
               == step2_kernel.get("one_open_trade_per_ticker"),
               {
                   "live": live_kernel.get("one_open_trade_per_ticker"),
                   "parity": parity_kernel.get("one_open_trade_per_ticker"),
                   "step2_execution": step2_kernel.get("one_open_trade_per_ticker"),
               }),
        _check("promotion_manifest_integrity",
               bool(manifest_integrity.get("ok")),
               {
                   "status": manifest_integrity.get("status"),
                   "legacy": manifest_integrity.get("legacy"),
                   "critical_failure_count": manifest_integrity.get("critical_failure_count"),
                   "manifest_path": manifest_integrity.get("manifest_path"),
               },
               "ok"),
        _check("promotion_manifest_not_legacy",
               (not manifest_integrity.get("legacy")) or allow_manifest_legacy_override,
               {
                   "status": manifest_integrity.get("status"),
                   "legacy": manifest_integrity.get("legacy"),
                   "override_env": "CLAUDE_ALLOW_LEGACY_PROMOTION_MANIFEST",
                   "override_active": allow_manifest_legacy_override,
               },
               "non-legacy manifest"),
        _check("config_change_journal_integrity",
               bool(config_journal_integrity.get("ok")),
               {
                   "status": config_journal_integrity.get("status"),
                   "legacy": config_journal_integrity.get("legacy"),
                   "critical_failure_count": config_journal_integrity.get("critical_failure_count"),
                   "current_config_hash": config_journal_integrity.get("current_config_hash"),
               },
               "ok"),
    ]
    critical_failures = [row for row in checks if row.get("severity") == "critical" and not row.get("ok")]
    allow_override = os.environ.get("CLAUDE_ALLOW_CONTRACT_DRIFT", "").strip().lower() in {"1", "true", "yes", "on"}
    return {
        "schema_version": SCHEMA_VERSION,
        "created_at_ct": datetime.now(CT).isoformat(timespec="seconds"),
        "day": day,
        "ok": not critical_failures or allow_override,
        "enforced": True,
        "override_active": allow_override,
        "critical_failure_count": len(critical_failures),
        "checks": checks,
        "active_profile": profile,
        "promotion_manifest_integrity": manifest_integrity,
        "config_change_journal_integrity": config_journal_integrity,
        "hashes": {
            "live_execution_kernel_hash": live_kernel.get("execution_kernel_hash"),
            "step2_parity_contract_hash": step2_parity_contract.contract_hash(parity),
            "step2_execution_contract_hash": step2_execution_contract.execution_contract_hash(cfg),
        },
        "contracts": {
            "live_execution_kernel": live_kernel,
            "step2_parity": parity,
            "step2_execution": step2_exec,
        },
    }


def write(config: dict[str, Any] | None = None, day: str | None = None) -> dict[str, Any]:
    payload = check(config=config, day=day)
    label = day or "latest"
    payload["path"] = _write_json(os.path.join(OUT_DIR, f"contract_gate_{label}.json"), payload)
    _write_json(os.path.join(OUT_DIR, "CONTRACT_GATE_LATEST.json"), payload)
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate hard Live/Step 2 contract parity.")
    ap.add_argument("--day", default="")
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    payload = write(day=args.day or None) if args.write else check(day=args.day or None)
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
