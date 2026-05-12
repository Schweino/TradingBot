"""Hard gates for Step 2 quote-aware outcome artifacts."""
from __future__ import annotations

from typing import Any


REQUIRED_EXIT_REPLAY_MODEL = "quote_aware_exit_v1"


class QuoteAwareModelError(RuntimeError):
    """Raised when a Step 2 artifact cannot prove quote-aware outcomes."""


def manifest_exit_replay_model(manifest: dict[str, Any] | None) -> str:
    manifest = manifest or {}
    direct = manifest.get("exit_replay_model")
    if direct not in (None, ""):
        return str(direct)

    source_models = manifest.get("source_exit_replay_models")
    if isinstance(source_models, dict) and source_models:
        values = {str(value) for value in source_models.values() if value not in (None, "")}
        if len(values) == 1:
            return next(iter(values))
        if values:
            return "mixed:" + ",".join(sorted(values))

    shards = ((manifest.get("day_shards") or {}).get("shards")
              if isinstance(manifest.get("day_shards"), dict) else [])
    if isinstance(shards, list) and shards:
        values = {str(row.get("exit_replay_model")) for row in shards
                  if isinstance(row, dict) and row.get("exit_replay_model") not in (None, "")}
        if len(values) == 1:
            return next(iter(values))
        if values:
            return "mixed:" + ",".join(sorted(values))
    return ""


def manifest_gate(manifest: dict[str, Any] | None) -> dict[str, Any]:
    actual = manifest_exit_replay_model(manifest)
    blockers: list[str] = []
    if actual != REQUIRED_EXIT_REPLAY_MODEL:
        blockers.append("exit_replay_model_mismatch" if actual else "exit_replay_model_missing")
    return {
        "ok": not blockers,
        "required_exit_replay_model": REQUIRED_EXIT_REPLAY_MODEL,
        "actual_exit_replay_model": actual,
        "blockers": blockers,
    }


def assert_manifest(manifest: dict[str, Any] | None, *, context: str = "compiled_manifest") -> None:
    gate = manifest_gate(manifest)
    if gate["ok"]:
        return
    raise QuoteAwareModelError(
        f"{context} rejected: expected exit_replay_model="
        f"{REQUIRED_EXIT_REPLAY_MODEL}, got {gate['actual_exit_replay_model'] or '<missing>'}"
    )


def candidate_exit_replay_model(candidate: dict[str, Any] | None) -> str:
    candidate = candidate or {}
    value = candidate.get("exit_replay_model")
    if value not in (None, ""):
        return str(value)

    step2 = candidate.get("step2") if isinstance(candidate.get("step2"), dict) else {}
    value = step2.get("exit_replay_model")
    if value not in (None, ""):
        return str(value)

    data = candidate.get("data") if isinstance(candidate.get("data"), dict) else {}
    value = data.get("exit_replay_model")
    if value not in (None, ""):
        return str(value)

    manifest = candidate.get("compiled_manifest") if isinstance(candidate.get("compiled_manifest"), dict) else {}
    value = manifest_exit_replay_model(manifest)
    if value:
        return value

    certification = candidate.get("cache_certification") if isinstance(candidate.get("cache_certification"), dict) else {}
    scope = certification.get("scope") if isinstance(certification.get("scope"), dict) else {}
    actual = scope.get("actual") if isinstance(scope.get("actual"), dict) else {}
    value = actual.get("exit_replay_model")
    return str(value) if value not in (None, "") else ""


def candidate_gate(candidate: dict[str, Any] | None) -> dict[str, Any]:
    actual = candidate_exit_replay_model(candidate)
    blockers: list[str] = []
    if actual != REQUIRED_EXIT_REPLAY_MODEL:
        blockers.append("candidate_exit_replay_model_mismatch" if actual else "candidate_exit_replay_model_missing")
    return {
        "ok": not blockers,
        "required_exit_replay_model": REQUIRED_EXIT_REPLAY_MODEL,
        "actual_exit_replay_model": actual,
        "blockers": blockers,
    }
