"""Audited journal for live trading-config changes.

This tracks why the current ``trading_config.json`` is live. Promotion manifests
answer "why is this scoring profile live"; this journal answers "why is this
entire live config live". If the config hash changes without a matching latest
journal entry, gates can flag unaudited drift.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import semantic_config
import tournament_safety


HERE = Path(__file__).resolve().parent
CONFIG_PATH = HERE / "trading_config.json"
OUT_DIR = HERE / "postmortem" / "config_change_journal"
SNAPSHOT_DIR = OUT_DIR / "snapshots"
JOURNAL_PATH = OUT_DIR / "config_change_journal.jsonl"
LATEST_PATH = OUT_DIR / "CONFIG_CHANGE_JOURNAL_LATEST.json"
CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


def _stamp() -> str:
    return datetime.now(CT).strftime("%Y%m%d_%H%M%S")


def _read_json(path: str | os.PathLike[str], default: Any = None) -> Any:
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except Exception:
        return default


def _stable_hash(payload: Any) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def config_hash(config: dict[str, Any]) -> str:
    return _stable_hash(config or {})


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.{int(time.time() * 1000)}.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
        f.write("\n")
    os.replace(tmp, path)
    return str(path.resolve())


def _append_jsonl(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str) + "\n")
    return str(path.resolve())


def _write_snapshot(config: dict[str, Any], label: str) -> str:
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    path = SNAPSHOT_DIR / f"{_stamp()}_{label}_{config_hash(config)[:12]}.json"
    return _write_json_atomic(path, config or {})


def _changed_paths(before: Any, after: Any, prefix: str = "", limit: int = 500) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    if len(changes) >= limit:
        return changes
    if isinstance(before, dict) and isinstance(after, dict):
        keys = sorted(set(before) | set(after), key=str)
        for key in keys:
            if len(changes) >= limit:
                break
            path = f"{prefix}.{key}" if prefix else str(key)
            if key not in before:
                changes.append({"path": path, "kind": "added", "after": after.get(key)})
            elif key not in after:
                changes.append({"path": path, "kind": "removed", "before": before.get(key)})
            else:
                changes.extend(_changed_paths(before.get(key), after.get(key), path, limit - len(changes)))
        return changes[:limit]
    if before != after:
        return [{"path": prefix or "$", "kind": "changed", "before": before, "after": after}]
    return []


def _section_deltas(before_config: dict[str, Any], after_config: dict[str, Any]) -> list[dict[str, Any]]:
    before = semantic_config.section_hashes(before_config)
    after = semantic_config.section_hashes(after_config)
    deltas = []
    for section in sorted(set(before) | set(after)):
        if before.get(section) != after.get(section):
            deltas.append({
                "section": section,
                "before": before.get(section),
                "after": after.get(section),
            })
    return deltas


def event_hash(entry: dict[str, Any]) -> str:
    return _stable_hash({
        key: value
        for key, value in (entry or {}).items()
        if key not in {"event_hash", "output"}
    })


def build_record(before_config: dict[str, Any],
                 after_config: dict[str, Any],
                 *,
                 action: str,
                 reason: str,
                 actor: str = "codex",
                 artifacts: dict[str, Any] | None = None,
                 rollback_snapshot_path: str = "",
                 write_snapshots: bool = False) -> dict[str, Any]:
    before_config = before_config or {}
    after_config = after_config or {}
    before_hash = config_hash(before_config)
    after_hash = config_hash(after_config)
    changed_paths = _changed_paths(before_config, after_config)
    record = {
        "schema_version": SCHEMA_VERSION,
        "source": "config_change_journal",
        "created_at_ct": _now_ct(),
        "action": action,
        "actor": actor,
        "reason": reason,
        "config_path": str(CONFIG_PATH.resolve()),
        "before_config_hash": before_hash,
        "after_config_hash": after_hash,
        "current_config_hash": after_hash,
        "before_config_file_sha256": tournament_safety._file_sha256(CONFIG_PATH),
        "after_config_file_sha256": tournament_safety._file_sha256(CONFIG_PATH),
        "semantic_section_deltas": _section_deltas(before_config, after_config),
        "changed_path_count": len(changed_paths),
        "changed_paths": changed_paths[:200],
        "artifacts": artifacts or {},
        "rollback_snapshot_path": rollback_snapshot_path,
        "deduction": (
            "This record explains why the current trading_config.json hash is live. "
            "If the live config hash differs from the latest journal after_config_hash, "
            "the config changed outside the audited path."
        ),
    }
    if write_snapshots:
        record["before_config_snapshot_path"] = _write_snapshot(before_config, "before")
        record["after_config_snapshot_path"] = _write_snapshot(after_config, "after")
        record["rollback_snapshot_path"] = record.get("rollback_snapshot_path") or record["before_config_snapshot_path"]
    record["event_hash"] = event_hash(record)
    return record


def write_record(record: dict[str, Any]) -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    _append_jsonl(JOURNAL_PATH, record)
    pointer = {
        "schema_version": SCHEMA_VERSION,
        "source": "config_change_journal_latest",
        "updated_at_ct": _now_ct(),
        "journal_path": str(JOURNAL_PATH.resolve()),
        "event_hash": record.get("event_hash"),
        "action": record.get("action"),
        "actor": record.get("actor"),
        "reason": record.get("reason"),
        "current_config_hash": record.get("current_config_hash"),
        "after_config_hash": record.get("after_config_hash"),
        "changed_path_count": record.get("changed_path_count"),
        "semantic_section_deltas": record.get("semantic_section_deltas") or [],
        "rollback_snapshot_path": record.get("rollback_snapshot_path"),
    }
    pointer["path"] = _write_json_atomic(LATEST_PATH, pointer)
    record["output"] = {
        "journal_path": str(JOURNAL_PATH.resolve()),
        "latest_path": str(LATEST_PATH.resolve()),
    }
    return record


def record_change(before_config: dict[str, Any],
                  after_config: dict[str, Any],
                  *,
                  action: str,
                  reason: str,
                  actor: str = "codex",
                  artifacts: dict[str, Any] | None = None,
                  rollback_snapshot_path: str = "",
                  write: bool = True) -> dict[str, Any]:
    record = build_record(
        before_config,
        after_config,
        action=action,
        reason=reason,
        actor=actor,
        artifacts=artifacts,
        rollback_snapshot_path=rollback_snapshot_path,
        write_snapshots=write,
    )
    return write_record(record) if write else record


def record_current(*, reason: str, actor: str = "codex", write: bool = True) -> dict[str, Any]:
    cfg = _read_json(CONFIG_PATH, {}) or {}
    return record_change(
        cfg,
        cfg,
        action="record_current_baseline",
        reason=reason,
        actor=actor,
        artifacts={"config": str(CONFIG_PATH.resolve())},
        write=write,
    )


def latest_pointer() -> dict[str, Any]:
    payload = _read_json(LATEST_PATH, {}) or {}
    return payload if isinstance(payload, dict) else {}


def latest_entry(pointer: dict[str, Any] | None = None) -> dict[str, Any]:
    pointer = pointer or latest_pointer()
    journal_path = pointer.get("journal_path")
    target_hash = pointer.get("event_hash")
    if not journal_path or not os.path.exists(str(journal_path)):
        return {}
    try:
        with open(journal_path, "r", encoding="utf-8-sig") as f:
            rows = [json.loads(line) for line in f if line.strip()]
    except Exception:
        return {}
    if target_hash:
        for row in reversed(rows):
            if row.get("event_hash") == target_hash:
                return row
    return rows[-1] if rows else {}


def _check(name: str, ok: bool, actual: Any = None, expected: Any = None,
           severity: str = "critical") -> dict[str, Any]:
    row = {"name": name, "ok": bool(ok), "severity": severity}
    if actual is not None:
        row["actual"] = actual
    if expected is not None:
        row["expected"] = expected
    return row


def validate_live_config(config: dict[str, Any] | None = None,
                         *,
                         require_journal: bool = False,
                         pointer: dict[str, Any] | None = None,
                         entry: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = config if config is not None else (_read_json(CONFIG_PATH, {}) or {})
    current_hash = config_hash(cfg)
    pointer = pointer if pointer is not None else latest_pointer()
    entry = entry if entry is not None else latest_entry(pointer)
    checks = [
        _check("current_config_hash_present", bool(current_hash), current_hash),
    ]
    if not pointer and not entry and not require_journal:
        checks.append(_check("config_journal_legacy_grandfathered", True, "no config journal exists yet", severity="warning"))
        return {
            "schema_version": SCHEMA_VERSION,
            "source": "config_change_journal_integrity",
            "created_at_ct": _now_ct(),
            "ok": True,
            "status": "legacy_grandfathered",
            "legacy": True,
            "enforced": False,
            "current_config_hash": current_hash,
            "latest_pointer": {},
            "latest_entry": {},
            "checks": checks,
            "failed_checks": [],
            "critical_failure_count": 0,
            "warning_failure_count": 0,
        }
    checks.extend([
        _check("config_journal_latest_pointer_present", bool(pointer), bool(pointer), True),
        _check("config_journal_entry_present", bool(entry), bool(entry), True),
    ])
    if entry:
        stored_hash = str(entry.get("event_hash") or "")
        computed_hash = event_hash(entry)
        rollback_path = str(entry.get("rollback_snapshot_path") or "")
        checks.extend([
            _check("config_journal_event_hash_valid", stored_hash == computed_hash, stored_hash, computed_hash),
            _check("config_hash_matches_latest_journal",
                   str(entry.get("after_config_hash") or "") == current_hash,
                   entry.get("after_config_hash"),
                   current_hash),
            _check("config_journal_has_reason", bool(entry.get("reason")), entry.get("reason")),
            _check("config_journal_has_actor", bool(entry.get("actor")), entry.get("actor")),
            _check("config_journal_rollback_snapshot_exists",
                   bool(rollback_path and os.path.exists(rollback_path)),
                   rollback_path,
                   "existing rollback snapshot",
                   severity="warning" if entry.get("action") == "record_current_baseline" else "critical"),
        ])
    if pointer:
        checks.extend([
            _check("latest_pointer_hash_matches_entry",
                   not entry or pointer.get("event_hash") == entry.get("event_hash"),
                   pointer.get("event_hash"),
                   (entry or {}).get("event_hash"),
                   severity="warning"),
            _check("latest_pointer_config_hash_matches_current",
                   pointer.get("current_config_hash") == current_hash or pointer.get("after_config_hash") == current_hash,
                   pointer.get("current_config_hash") or pointer.get("after_config_hash"),
                   current_hash),
        ])
    critical_failures = [row for row in checks if row.get("severity") == "critical" and not row.get("ok")]
    warning_failures = [row for row in checks if row.get("severity") != "critical" and not row.get("ok")]
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "config_change_journal_integrity",
        "created_at_ct": _now_ct(),
        "ok": not critical_failures,
        "status": "ok" if not critical_failures else "failed",
        "legacy": False,
        "enforced": True,
        "current_config_hash": current_hash,
        "latest_pointer": pointer,
        "latest_entry": {
            "action": entry.get("action"),
            "actor": entry.get("actor"),
            "reason": entry.get("reason"),
            "created_at_ct": entry.get("created_at_ct"),
            "event_hash": entry.get("event_hash"),
            "before_config_hash": entry.get("before_config_hash"),
            "after_config_hash": entry.get("after_config_hash"),
            "changed_path_count": entry.get("changed_path_count"),
            "semantic_section_deltas": entry.get("semantic_section_deltas") or [],
            "rollback_snapshot_path": entry.get("rollback_snapshot_path"),
            "artifacts": entry.get("artifacts") or {},
        } if entry else {},
        "checks": checks,
        "failed_checks": [row for row in checks if not row.get("ok")],
        "critical_failure_count": len(critical_failures),
        "warning_failure_count": len(warning_failures),
    }


def provenance_status(config: dict[str, Any] | None = None) -> dict[str, Any]:
    verdict = validate_live_config(config)
    entry = verdict.get("latest_entry") or {}
    pointer = verdict.get("latest_pointer") or {}
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "config_change_journal_provenance",
        "created_at_ct": _now_ct(),
        "ok": verdict.get("ok"),
        "status": verdict.get("status"),
        "legacy": verdict.get("legacy"),
        "current_config_hash": verdict.get("current_config_hash"),
        "journal_path": pointer.get("journal_path"),
        "event_hash": entry.get("event_hash") or pointer.get("event_hash"),
        "action": entry.get("action") or pointer.get("action"),
        "actor": entry.get("actor") or pointer.get("actor"),
        "reason": entry.get("reason") or pointer.get("reason"),
        "created_at_ct": entry.get("created_at_ct"),
        "changed_path_count": entry.get("changed_path_count") or pointer.get("changed_path_count"),
        "semantic_section_deltas": entry.get("semantic_section_deltas") or pointer.get("semantic_section_deltas") or [],
        "rollback_snapshot_path": entry.get("rollback_snapshot_path") or pointer.get("rollback_snapshot_path"),
        "critical_failure_count": verdict.get("critical_failure_count"),
        "warning_failure_count": verdict.get("warning_failure_count"),
        "failed_checks": [
            {
                "name": row.get("name"),
                "severity": row.get("severity"),
                "actual": row.get("actual"),
                "expected": row.get("expected"),
            }
            for row in (verdict.get("failed_checks") or [])[:10]
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit and validate trading_config.json changes.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    current_cmd = sub.add_parser("record-current")
    current_cmd.add_argument("--reason", required=True)
    current_cmd.add_argument("--actor", default="codex")
    current_cmd.add_argument("--json", action="store_true")

    validate_cmd = sub.add_parser("validate-live")
    validate_cmd.add_argument("--json", action="store_true")

    provenance_cmd = sub.add_parser("provenance")
    provenance_cmd.add_argument("--json", action="store_true")

    latest_cmd = sub.add_parser("latest")
    latest_cmd.add_argument("--json", action="store_true")

    args = parser.parse_args()
    if args.cmd == "record-current":
        payload = record_current(reason=args.reason, actor=args.actor, write=True)
        ok = True
    elif args.cmd == "validate-live":
        payload = validate_live_config()
        ok = bool(payload.get("ok"))
    elif args.cmd == "provenance":
        payload = provenance_status()
        ok = bool(payload.get("ok"))
    else:
        payload = {"latest_pointer": latest_pointer(), "latest_entry": latest_entry()}
        ok = bool(payload.get("latest_pointer") or payload.get("latest_entry"))

    if getattr(args, "json", False):
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    else:
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
