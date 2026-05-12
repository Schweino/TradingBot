from __future__ import annotations

import argparse
import copy
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import active_engine_baseline
from zoneinfo import ZoneInfo


CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1
HERE = Path(__file__).resolve().parent
DEFAULT_ROOT = HERE / "postmortem" / "backtests" / "step2_adaptive_hunter"
OUT_DIR = HERE / "postmortem" / "baseline_drift"


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


def _read_json(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    return payload if isinstance(payload, dict) else {}


def _write_json(path: str | Path, payload: dict[str, Any]) -> None:
    p = Path(path)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(p)


def _stable_hash(payload: Any) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def _envelope_hash(envelope: dict[str, Any]) -> str:
    clone = copy.deepcopy(envelope)
    clone.pop("envelope_hash", None)
    clone.pop("output_path", None)
    return _stable_hash(clone)


def current_baseline() -> dict[str, Any]:
    payload = active_engine_baseline.active_profile_payload()
    profile = payload.get("profile") if isinstance(payload.get("profile"), dict) else {}
    return {
        "profile_hash": profile.get("hash", ""),
        "profile_name": profile.get("name", ""),
        "config_path": payload.get("config_path", ""),
        "config_sha256": payload.get("config_sha256", ""),
        "promoted_at": profile.get("promoted_at", ""),
    }


def source_looks_step2(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    if isinstance(payload.get("step2_evaluation_envelope"), dict):
        return True
    if payload.get("compiled_decision_tape"):
        return True
    script = str(payload.get("script") or payload.get("source_script") or "")
    return script.startswith("step2_") or "step2_adaptive_hunter" in script


def extract_baseline_hash(payload: dict[str, Any] | None) -> str:
    if not isinstance(payload, dict):
        return ""
    envelope = payload.get("step2_evaluation_envelope")
    if isinstance(envelope, dict):
        value = envelope.get("baseline_profile_hash")
        if value:
            return str(value)
        active = envelope.get("active_baseline")
        if isinstance(active, dict):
            value = active.get("profile_hash")
            if value:
                return str(value)
            profile = active.get("profile")
            if isinstance(profile, dict) and profile.get("hash"):
                return str(profile["hash"])
    for key in ("baseline_profile_hash", "active_profile_hash", "current_profile_hash"):
        if payload.get(key):
            return str(payload[key])
    active = payload.get("active_baseline")
    if isinstance(active, dict):
        if active.get("profile_hash"):
            return str(active["profile_hash"])
        profile = active.get("profile")
        if isinstance(profile, dict) and profile.get("hash"):
            return str(profile["hash"])
    active_profile = payload.get("active_profile")
    if isinstance(active_profile, dict) and active_profile.get("hash"):
        return str(active_profile["hash"])
    baseline = payload.get("baseline")
    if isinstance(baseline, dict):
        for key in ("profile_hash", "active_profile_hash", "hash"):
            if baseline.get(key):
                return str(baseline[key])
    return ""


def evaluate_payload(payload: dict[str, Any] | None, *, current: dict[str, Any] | None = None) -> dict[str, Any]:
    current = current or current_baseline()
    artifact_hash = extract_baseline_hash(payload)
    current_hash = str(current.get("profile_hash") or "")
    if not artifact_hash:
        status = "unknown"
        reason = "baseline_hash_missing"
    elif artifact_hash == current_hash:
        status = "current"
        reason = "baseline_current"
    else:
        status = "stale"
        reason = "baseline_hash_mismatch"
    return {
        "schema_version": SCHEMA_VERSION,
        "checked_at_ct": _now_ct(),
        "status": status,
        "ok": status == "current",
        "stale": status == "stale",
        "reason": reason,
        "artifact_baseline_hash": artifact_hash,
        "current_profile_hash": current_hash,
        "current_profile_name": current.get("profile_name", ""),
        "current_config_sha256": current.get("config_sha256", ""),
    }


def _reasons(payload: dict[str, Any]) -> list[str]:
    raw = payload.get("non_promotable_reasons")
    if isinstance(raw, list):
        reasons = [str(item) for item in raw if item]
    else:
        reason = payload.get("non_promotable_reason")
        reasons = [str(reason)] if reason else []
    return reasons


def annotate_payload(
    payload: dict[str, Any],
    *,
    current: dict[str, Any] | None = None,
    path: str | Path | None = None,
    write: bool = False,
) -> dict[str, Any]:
    drift = evaluate_payload(payload, current=current)
    drift["artifact_path"] = str(path or "")
    payload["baseline_drift"] = drift

    reasons = _reasons(payload)
    if drift["status"] != "current":
        payload["promotable"] = False
        reasons.append("baseline_drift_stale" if drift["status"] == "stale" else "baseline_hash_missing")

    if reasons:
        reasons = sorted(set(reasons))
        payload["non_promotable_reasons"] = reasons
        payload["non_promotable_reason"] = ",".join(reasons)

    envelope = payload.get("step2_evaluation_envelope")
    if isinstance(envelope, dict):
        envelope["baseline_drift"] = drift
        if drift["status"] != "current":
            envelope["promotable"] = False
            env_reasons = envelope.get("non_promotable_reasons")
            env_reason_list = [str(item) for item in env_reasons] if isinstance(env_reasons, list) else []
            env_reason_list.append("baseline_drift_stale" if drift["status"] == "stale" else "baseline_hash_missing")
            envelope["non_promotable_reasons"] = sorted(set(env_reason_list))
        envelope["envelope_hash"] = _envelope_hash(envelope)
        payload["step2_evaluation_envelope_hash"] = envelope["envelope_hash"]

    if write and path:
        _write_json(path, payload)
    return payload


def _summary_paths(root: Path, pattern: str) -> list[Path]:
    if root.is_file():
        return [root]
    return sorted(root.rglob(pattern))


def scan(
    *,
    root: str | Path = DEFAULT_ROOT,
    pattern: str = "summary.json",
    mutate: bool = False,
    write_report: bool = True,
    label: str = "",
) -> dict[str, Any]:
    root_path = Path(root)
    current = current_baseline()
    rows: list[dict[str, Any]] = []
    mutated_count = 0
    for path in _summary_paths(root_path, pattern):
        try:
            payload = _read_json(path)
        except Exception as exc:
            rows.append({"path": str(path), "status": "error", "ok": False, "reason": str(exc)})
            continue
        if not source_looks_step2(payload):
            rows.append({"path": str(path), "status": "skipped", "ok": True, "reason": "not_step2_payload"})
            continue
        drift = evaluate_payload(payload, current=current)
        row = {
            "path": str(path),
            **drift,
            "promotable_before": payload.get("promotable"),
        }
        if mutate and drift["status"] != "current":
            before = json.dumps(payload, sort_keys=True, default=str)
            annotate_payload(payload, current=current, path=path, write=True)
            after = json.dumps(payload, sort_keys=True, default=str)
            if before != after:
                mutated_count += 1
            row["promotable_after"] = payload.get("promotable")
        rows.append(row)

    counts = {
        "current": sum(1 for row in rows if row.get("status") == "current"),
        "stale": sum(1 for row in rows if row.get("status") == "stale"),
        "unknown": sum(1 for row in rows if row.get("status") == "unknown"),
        "skipped": sum(1 for row in rows if row.get("status") == "skipped"),
        "error": sum(1 for row in rows if row.get("status") == "error"),
    }
    report = {
        "schema_version": SCHEMA_VERSION,
        "created_at_ct": _now_ct(),
        "root": str(root_path),
        "pattern": pattern,
        "mutate": mutate,
        "current_baseline": current,
        "counts": counts,
        "mutated_count": mutated_count,
        "rows": rows,
        "promotion_safe": counts["error"] == 0,
    }
    if write_report:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        suffix = label or datetime.now(CT).strftime("%Y-%m-%d")
        safe_suffix = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in str(suffix))
        path = OUT_DIR / f"baseline_drift_sentinel_{safe_suffix}.json"
        report["output_path"] = str(path)
        _write_json(path, report)
    return report


def _main() -> int:
    parser = argparse.ArgumentParser(description="Label Step 2 artifacts stale when their baseline no longer matches Live.")
    sub = parser.add_subparsers(dest="cmd")

    scan_cmd = sub.add_parser("scan")
    scan_cmd.add_argument("--root", default=str(DEFAULT_ROOT))
    scan_cmd.add_argument("--pattern", default="summary.json")
    scan_cmd.add_argument("--write-labels", action="store_true")
    scan_cmd.add_argument("--no-report", action="store_true")
    scan_cmd.add_argument("--label", default="")
    scan_cmd.add_argument("--json", action="store_true")

    check_cmd = sub.add_parser("check")
    check_cmd.add_argument("path")
    check_cmd.add_argument("--write-label", action="store_true")
    check_cmd.add_argument("--json", action="store_true")

    args = parser.parse_args()
    if args.cmd == "check":
        payload = _read_json(args.path)
        annotate_payload(payload, path=args.path, write=args.write_label)
        drift = payload.get("baseline_drift", {})
        if args.json:
            print(json.dumps(drift, indent=2, sort_keys=True))
        else:
            print(f"status={drift.get('status')} reason={drift.get('reason')}")
        return 0 if drift.get("status") == "current" else 2

    report = scan(
        root=args.root if args.cmd else DEFAULT_ROOT,
        pattern=args.pattern if args.cmd else "summary.json",
        mutate=bool(getattr(args, "write_labels", False)),
        write_report=not bool(getattr(args, "no_report", False)),
        label=getattr(args, "label", ""),
    )
    if getattr(args, "json", False):
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        counts = report["counts"]
        print(
            f"current={counts['current']} stale={counts['stale']} "
            f"unknown={counts['unknown']} mutated={report['mutated_count']}"
        )
    return 0 if report.get("promotion_safe") else 2


if __name__ == "__main__":
    raise SystemExit(_main())
