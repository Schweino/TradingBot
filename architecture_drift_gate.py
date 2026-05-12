"""Architecture drift gate for Live/Step 2 parity.

This gate is intentionally conservative. It catches the easy-to-miss kind of
drift: Live forgot to call the reducer, Step 2 stopped using the shared
adapter, shadow variants are not wired, or contract hashes no longer line up.
"""
from __future__ import annotations

import argparse
import json
import os
import py_compile
import time
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import contract_gate


HERE = Path(__file__).resolve().parent
POSTMORTEM_DIR = HERE / "postmortem"
OUT_DIR = POSTMORTEM_DIR / "architecture_drift"
CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1


COMPILE_TARGETS = [
    "mock_trader.py",
    "backtest_30d_engine.py",
    "step2_execution_contract.py",
    "execution_action_engine.py",
    "execution_intent_engine.py",
    "live_step2_decision_kernel.py",
    "execution_state_reducer.py",
    "execution_adapters.py",
    "contract_gate.py",
    "shadow_variant_engine.py",
    "candidate_lifecycle.py",
    "certify_step2_cache.py",
    "step2_cache_catalog.py",
    "canonical_decision_packet.py",
    "parity_verdict_engine.py",
    "market_data_integrity_gate.py",
    "market_data_freshness_guard.py",
    "prepare_step2_live_cache.py",
    "refresh_intraday_step2.py",
    "config_change_journal.py",
    "rollback_drill.py",
    "order_lifecycle_reconciliation.py",
    "daily_parity_scorecard.py",
    "promotion_preflight_bundle.py",
    "canonical_command_registry.py",
    "ops.py",
    "step2_evaluation_envelope.py",
    "candidate_decision_brief.py",
    "candidate_reproducibility_gate.py",
    "candidate_robustness_report.py",
    "promotion_candidate_quarantine.py",
    "promotion_manifest.py",
    "baseline_drift_sentinel.py",
    "step2_adaptive_hunter.py",
    "promote_active_profile.py",
    "deterministic_live_replay.py",
    "promotion_evidence_packet.py",
    "promotion_gate.py",
    "promotion_safety.py",
    "compiled_tape_lineage.py",
]

TEXT_CONTRACTS = [
    {
        "file": "mock_trader.py",
        "tokens": [
            "execution_state_reducer",
            "execution_action_engine",
            "execution_intent_engine",
            "contract_gate",
            "execution_adapters",
            "shadow_variant_engine",
            "canonical_decision_packet",
            "hard_contract_gate_ok",
            "promotion_evidence_boot_gate_ok",
        ],
    },
    {
        "file": "backtest_30d_engine.py",
        "tokens": [
            "execution_adapters.ReplayExecutionAdapter",
            "execution_action_engine.plan_entry",
            "execution_action_engine.plan_exit",
            "execution_intent_engine",
        ],
    },
    {
        "file": "step2_execution_contract.py",
        "tokens": [
            "execution_state_reducer.entry_block_reason",
        ],
    },
    {
        "file": "live_step2_decision_kernel.py",
        "tokens": [
            "execution_action_engine.plan_entry",
            "execution_action_engine.plan_exit",
        ],
    },
    {
        "file": "automation_ops.py",
        "tokens": [
            "shadow_variant_engine",
            "candidate_lifecycle",
            "canonical_decision_packet",
            "parity_verdict_engine",
            "market_data_integrity_gate",
            "market_data_freshness_guard",
            "baseline_drift_sentinel",
            "certify_step2_cache",
            "order_lifecycle_reconciliation",
            "daily_parity_scorecard",
            "deterministic_live_replay",
            "architecture_drift_gate",
        ],
    },
]


def _write_json(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.{int(time.time() * 1000)}.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
        f.write("\n")
    os.replace(tmp, path)
    return str(path.resolve())


def _compile_check(name: str) -> dict[str, Any]:
    path = HERE / name
    try:
        py_compile.compile(str(path), doraise=True)
        return {"name": f"compile:{name}", "ok": True, "file": str(path.resolve())}
    except Exception as exc:
        return {"name": f"compile:{name}", "ok": False, "file": str(path.resolve()), "error": str(exc)}


def _text_check(contract: dict[str, Any]) -> list[dict[str, Any]]:
    path = HERE / str(contract.get("file"))
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except Exception as exc:
        return [{
            "name": f"text_contract:{path.name}",
            "ok": False,
            "file": str(path.resolve()),
            "error": str(exc),
        }]
    checks = []
    for token in contract.get("tokens") or []:
        checks.append({
            "name": f"text_contract:{path.name}:{token}",
            "ok": token in text,
            "file": str(path.resolve()),
            "token": token,
        })
    return checks


def _deterministic_replay_check(day: str | None) -> dict[str, Any] | None:
    if not day:
        return None
    try:
        import deterministic_live_replay
        payload = deterministic_live_replay.build(day, write=True)
        return {
            "name": f"deterministic_live_replay:{day}",
            "ok": bool(payload.get("ok")),
            "day": day,
            "path": payload.get("path"),
            "blockers": payload.get("blockers"),
            "decision_mismatch_count": payload.get("decision_mismatch_count"),
        }
    except Exception as exc:
        return {
            "name": f"deterministic_live_replay:{day}",
            "ok": False,
            "day": day,
            "error": str(exc),
        }


def build(day: str | None = None, write: bool = True) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    for name in COMPILE_TARGETS:
        checks.append(_compile_check(name))
    for contract in TEXT_CONTRACTS:
        checks.extend(_text_check(contract))
    gate = contract_gate.check(day=day)
    checks.append({
        "name": "contract_gate",
        "ok": bool(gate.get("ok")),
        "critical_failure_count": gate.get("critical_failure_count"),
        "hashes": gate.get("hashes"),
    })
    replay_check = _deterministic_replay_check(day)
    if replay_check is not None:
        checks.append(replay_check)
    failed = [row for row in checks if not row.get("ok")]
    payload = {
        "schema_version": SCHEMA_VERSION,
        "source": "architecture_drift_gate",
        "created_at_ct": datetime.now(CT).isoformat(timespec="seconds"),
        "day": day,
        "ok": not failed,
        "failed_count": len(failed),
        "failed_checks": failed[:50],
        "checks": checks,
        "contract_gate": gate,
        "deduction": (
            "This is a structural parity gate. It catches code-path drift before a profitable Step 2 "
            "candidate is trusted by Live."
        ),
    }
    if write:
        suffix = day or datetime.now(CT).date().isoformat()
        payload["path"] = _write_json(OUT_DIR / f"architecture_drift_gate_{suffix}.json", payload)
    return payload


def main() -> int:
    ap = argparse.ArgumentParser(description="Run the Live/Step 2 architecture drift gate.")
    ap.add_argument("--day", default="")
    ap.add_argument("--no-write", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    payload = build(day=args.day or None, write=not args.no_write)
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0 if payload.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
