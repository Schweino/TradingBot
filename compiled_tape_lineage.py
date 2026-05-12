"""Lineage classifier for compiled Step 2 decision tapes.

Compiled tapes used to treat every dependency hash mismatch the same way. That
is safe, but it is not very helpful: a source tape or execution-math change is
not equivalent to a reporting-only script changing. This module classifies tape
dependencies so callers can distinguish score-affecting drift from certification
or reporting drift.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import tournament_safety
import step2_artifact_identity


HERE = Path(__file__).resolve().parent
CT = ZoneInfo("America/Chicago")
SCHEMA_VERSION = 1

DEFAULT_COMPILED_TAPE_CODE_HASH_INPUTS = (
    "ws_scalp.py",
    "backtest_30d_engine.py",
    "bracket_rounding.py",
    "build_decision_tape.py",
    "compiled_chunk_store.py",
    "decision_tape_event_cache.py",
    "decision_tape_compiled.py",
    "decision_tape_gates.py",
    "opportunity_outcome_cache.py",
    "replay_artifacts.py",
    "routed_scoring_profile.py",
    "replay_state_checkpoints.py",
    "simulate_decision_tape.py",
    "scoring_profiles.py",
    "scoring_variant_lab_fast.py",
    "scoring_variant_lab_massive.py",
    "step2_latency_model.py",
    "step2_execution_contract.py",
    "step2_parity_contract.py",
    "step2_range_linker.py",
    "semantic_config.py",
    "step2_artifact_identity.py",
)

SCORE_AFFECTING_LAYERS = {
    "source_market_data",
    "signal_features",
    "path_outcomes",
    "compiled_execution",
    "compiled_scoring_math",
    "scoring_runtime",
    "scoring_features",
    "semantic_config",
    "execution_contract",
    "unknown",
}

REBUILD_ACTION_ORDER = {
    "score_only": 0,
    "score_only_uncertified": 1,
    "scorer_recertification": 1,
    "recompile_only": 2,
    "refresh_outcomes_compile": 3,
    "full_signal_rebuild": 4,
}

LAYER_REBUILD_ACTION = {
    "source_market_data": "full_signal_rebuild",
    "signal_features": "full_signal_rebuild",
    "semantic_config": "full_signal_rebuild",
    "path_outcomes": "refresh_outcomes_compile",
    "execution_contract": "refresh_outcomes_compile",
    "compiled_execution": "refresh_outcomes_compile",
    "compiled_scoring_math": "recompile_only",
    "scoring_runtime": "scorer_recertification",
    "scoring_features": "recompile_only",
    "cache_storage": "score_only_uncertified",
    "reporting_only": "score_only_uncertified",
    "validation_ops": "score_only_uncertified",
    "unknown": "full_signal_rebuild",
}

SEMANTIC_SECTION_LAYERS = {
    "active_scoring_profile": "scoring_features",
    "signal_generation": "signal_features",
    "path_outcomes": "path_outcomes",
    "compiled_execution": "compiled_execution",
    "market_data_contract": "source_market_data",
    "live_runtime": "execution_contract",
    "full_config": "semantic_config",
}

HISTORICAL_PROVENANCE_SECTIONS = {
    "path_outcomes",
}


DEPENDENCY_REGISTRY: dict[str, dict[str, Any]] = {
    "ws_scalp.py": {
        "layer": "signal_features",
        "score_affecting": True,
        "reason": "Signal generation, setup cooldowns, feature gates, and source decisions can change.",
    },
    "backtest_30d_engine.py": {
        "layer": "path_outcomes",
        "score_affecting": True,
        "reason": "Replay/path-outcome constants and trade sizing can change compiled outcomes.",
    },
    "bracket_rounding.py": {
        "layer": "execution_contract",
        "score_affecting": True,
        "reason": "TP/SL rounding can change exit outcomes and P/L.",
    },
    "build_decision_tape.py": {
        "layer": "signal_features",
        "score_affecting": True,
        "reason": "Decision tape materialization can change compiled rows or features.",
    },
    "compiled_chunk_store.py": {
        "layer": "cache_storage",
        "score_affecting": False,
        "reason": "Chunk storage affects cache layout, not already-built arrays used for scoring.",
    },
    "decision_tape_event_cache.py": {
        "layer": "cache_storage",
        "score_affecting": False,
        "reason": "Process-local event memoization is performance-only when equivalence tests pass; it must not transform event payloads.",
    },
    "decision_tape_compiled.py": {
        "layer": "scoring_runtime",
        "score_affecting": True,
        "scorer_recertification": True,
        "reason": "Compiled Step 2 scoring runtime can change acceptance, sizing, and P/L math without invalidating the already-built tape arrays.",
    },
    "decision_tape_gates.py": {
        "layer": "signal_features",
        "score_affecting": True,
        "reason": "Gate definitions feed compiled acceptance logic.",
    },
    "opportunity_outcome_cache.py": {
        "layer": "path_outcomes",
        "score_affecting": True,
        "reason": "Opportunity outcome cache changes can alter TP/SL/held-second outcomes.",
    },
    "replay_artifacts.py": {
        "layer": "source_market_data",
        "score_affecting": True,
        "reason": "Replay artifact selection/fingerprints define the source tape inputs.",
    },
    "routed_scoring_profile.py": {
        "layer": "scoring_runtime",
        "score_affecting": True,
        "scorer_recertification": True,
        "reason": "Route matching and routed side/skip selection affect scores; compiled tape arrays remain reusable but scorer certification must be refreshed.",
    },
    "replay_state_checkpoints.py": {
        "layer": "compiled_execution",
        "score_affecting": True,
        "reason": "Replay checkpoint semantics can affect stateful admission/order timing.",
    },
    "simulate_decision_tape.py": {
        "layer": "path_outcomes",
        "score_affecting": True,
        "reason": "Decision-tape simulation can change compiled trade outcomes.",
    },
    "scoring_profiles.py": {
        "layer": "scoring_features",
        "score_affecting": True,
        "reason": "Scoring feature definitions can change variant side selection.",
    },
    "scoring_variant_lab_fast.py": {
        "layer": "scoring_features",
        "score_affecting": True,
        "reason": "Feature name/order changes can change score matrix interpretation.",
    },
    "scoring_variant_lab_massive.py": {
        "layer": "scoring_runtime",
        "score_affecting": True,
        "scorer_recertification": True,
        "reason": "Variant matrix construction can change side selection; compiled tape arrays remain reusable.",
    },
    "step2_latency_model.py": {
        "layer": "path_outcomes",
        "score_affecting": True,
        "reason": "Fill latency model changes can alter entry/exit outcome windows.",
    },
    "step2_execution_contract.py": {
        "layer": "execution_contract",
        "score_affecting": True,
        "reason": "Execution contract changes can alter admission, exits, and cooldowns.",
    },
    "step2_parity_contract.py": {
        "layer": "compiled_execution",
        "score_affecting": True,
        "reason": "Step 2 parity config controls compiled admission/cooldown behavior.",
    },
    "step2_range_linker.py": {
        "layer": "cache_storage",
        "score_affecting": False,
        "reason": "Range linking changes how certified day shards are referenced, not the scoring arrays inside those shards.",
    },
    "semantic_config.py": {
        "layer": "semantic_config",
        "score_affecting": True,
        "reason": "Semantic cache sections decide when score-affecting config changed.",
    },
    "step2_artifact_identity.py": {
        "layer": "cache_storage",
        "score_affecting": False,
        "reason": "Artifact identity helpers control deterministic writes and semantic hash bookkeeping without changing scoring math.",
    },
    "trading_config.json": {
        "layer": "semantic_config",
        "score_affecting": True,
        "reason": "Trading config changes can alter scoring weights, ticker settings, sizing, or execution behavior.",
    },
    "candidate_decision_brief.py": {
        "layer": "reporting_only",
        "score_affecting": False,
        "reason": "Decision brief reports on candidates; it does not build or score compiled arrays.",
    },
    "candidate_robustness_report.py": {
        "layer": "reporting_only",
        "score_affecting": False,
        "reason": "Robustness report summarizes already-scored rows.",
    },
    "promotion_evidence_packet.py": {
        "layer": "validation_ops",
        "score_affecting": False,
        "reason": "Promotion evidence records artifacts and contracts after scoring.",
    },
    "smoke_check.py": {
        "layer": "validation_ops",
        "score_affecting": False,
        "reason": "Smoke checks validate readiness; they do not affect compiled Step 2 math.",
    },
    "architecture_drift_gate.py": {
        "layer": "validation_ops",
        "score_affecting": False,
        "reason": "Architecture drift checks validate structure; they do not affect scoring.",
    },
}


class CompiledTapeLineageError(ValueError):
    """Raised when a compiled tape has score-affecting drift."""

    def __init__(self, lineage: dict[str, Any]):
        self.lineage = lineage
        status = lineage.get("status")
        examples = lineage.get("score_affecting_drift", [])[:3]
        super().__init__(
            f"compiled decision tape has {status}; rebuild required before certified scoring: {examples}"
        )


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


def _stable_hash(payload: Any) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def _norm(path: str | os.PathLike[str] | None) -> str:
    if not path:
        return ""
    text = str(path).replace("\\", "/")
    return os.path.basename(text)


def classify_path(path: str | os.PathLike[str] | None) -> dict[str, Any]:
    name = _norm(path)
    row = dict(DEPENDENCY_REGISTRY.get(name) or {})
    if not row:
        row = {
            "layer": "unknown",
            "score_affecting": True,
            "reason": "Unknown compiled-tape dependency; fail closed until classified.",
        }
    row["path"] = str(path or "")
    row["name"] = name
    row["known"] = name in DEPENDENCY_REGISTRY
    return row


def registry_for(paths: list[str] | tuple[str, ...]) -> dict[str, Any]:
    dependencies = {str(path): classify_path(path) for path in paths}
    payload = {
        "schema_version": SCHEMA_VERSION,
        "source": "compiled_tape_lineage_registry",
        "dependencies": dependencies,
        "score_affecting_layers": sorted(SCORE_AFFECTING_LAYERS),
    }
    payload["registry_hash"] = _stable_hash(payload)
    return payload


def _max_action(actions: list[str]) -> str:
    if not actions:
        return "score_only"
    return max(actions, key=lambda value: REBUILD_ACTION_ORDER.get(value, 999))


def _choose_rebuild_action(
    status: str,
    score_affecting: list[dict[str, Any]],
    non_score: list[dict[str, Any]],
) -> str:
    if status == "CERTIFIED_MATCH":
        return "score_only"
    if status == "UNCERTIFIED_NON_SCORE_DRIFT":
        return "score_only_uncertified"
    actions = [
        LAYER_REBUILD_ACTION.get(str(row.get("layer") or "unknown"), "full_signal_rebuild")
        for row in score_affecting
    ]
    if not actions and non_score:
        return "score_only_uncertified"
    return _max_action(actions)


def _annotate_mismatch(row: dict[str, Any]) -> dict[str, Any]:
    item = dict(row)
    path = item.get("path") or item.get("file") or item.get("name") or item.get("section")
    if item.get("section") and not item.get("path"):
        section = str(item.get("section") or "")
        layer = SEMANTIC_SECTION_LAYERS.get(section, "semantic_config")
        item["path"] = f"semantic_config:{item.get('section')}"
        item["dependency_name"] = item["path"]
        item["layer"] = layer
        historical_provenance = (
            section in HISTORICAL_PROVENANCE_SECTIONS
            and item.get("reason") == "semantic_hash_mismatch"
        )
        item["score_affecting"] = not historical_provenance
        item["dependency_known"] = True
        item["historical_score_allowed"] = bool(historical_provenance)
        if historical_provenance:
            item["lineage_reason"] = (
                f"The current {section} config section changed after this compiled tape was built. "
                "The compiled tape's historical outcome arrays remain reusable for score-only research, "
                "but the artifact is not certified against the refreshed current outcome model."
            )
        else:
            item["lineage_reason"] = f"The compiled-tape {section or 'semantic'} config section changed."
        return item
    dep = classify_path(str(path or ""))
    item["path"] = str(path or "")
    item.setdefault("dependency_name", dep.get("name"))
    item.setdefault("layer", dep.get("layer"))
    if "score_affecting" not in item:
        item["score_affecting"] = bool(dep.get("score_affecting"))
    if "dependency_known" not in item:
        item["dependency_known"] = bool(dep.get("known"))
    item.setdefault("lineage_reason", dep.get("reason"))
    return item


def evaluate_mismatches(
    mismatches: list[dict[str, Any]],
    *,
    manifest_path: str = "",
    code_hash_inputs: list[str] | tuple[str, ...] = (),
) -> dict[str, Any]:
    annotated = [_annotate_mismatch(row) for row in (mismatches or [])]
    score_affecting = [row for row in annotated if row.get("score_affecting")]
    scorer_drift = [
        row for row in score_affecting
        if row.get("layer") == "scoring_runtime" or row.get("scorer_recertification")
    ]
    rebuild_drift = [row for row in score_affecting if row not in scorer_drift]
    non_score = [row for row in annotated if not row.get("score_affecting")]
    unknown = [row for row in annotated if not row.get("dependency_known")]
    if rebuild_drift:
        status = "UNSAFE_SCORE_DRIFT"
        quick_score_allowed = False
        certified = False
        rebuild_required = True
        scorer_recertification_required = bool(scorer_drift)
    elif scorer_drift:
        status = "UNCERTIFIED_SCORER_DRIFT"
        quick_score_allowed = True
        certified = False
        rebuild_required = False
        scorer_recertification_required = True
    elif non_score:
        if any(row.get("historical_score_allowed") for row in non_score):
            status = "UNCERTIFIED_HISTORICAL_OUTCOME_DRIFT"
        else:
            status = "UNCERTIFIED_NON_SCORE_DRIFT"
        quick_score_allowed = True
        certified = False
        rebuild_required = False
        scorer_recertification_required = False
    else:
        status = "CERTIFIED_MATCH"
        quick_score_allowed = True
        certified = True
        rebuild_required = False
        scorer_recertification_required = False
    layer_counts: dict[str, int] = {}
    reason_counts: dict[str, int] = {}
    for row in annotated:
        layer = str(row.get("layer") or "unknown")
        reason = str(row.get("reason") or "unknown")
        layer_counts[layer] = layer_counts.get(layer, 0) + 1
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
    recommended_action = _choose_rebuild_action(status, rebuild_drift + scorer_drift, non_score)
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "compiled_tape_lineage",
        "created_at_ct": _now_ct(),
        "manifest_path": manifest_path,
        "status": status,
        "certified": certified,
        "rebuild_required": rebuild_required,
        "scorer_recertification_required": scorer_recertification_required,
        "quick_score_allowed": quick_score_allowed,
        "recommended_action": recommended_action,
        "rebuild_strategy": recommended_action,
        "score_affecting_count": len(score_affecting),
        "scorer_drift_count": len(scorer_drift),
        "non_score_count": len(non_score),
        "unknown_count": len(unknown),
        "layer_counts": dict(sorted(layer_counts.items())),
        "reason_counts": dict(sorted(reason_counts.items())),
        "score_affecting_drift": score_affecting,
        "scorer_drift": scorer_drift,
        "rebuild_drift": rebuild_drift,
        "non_score_drift": non_score,
        "unknown_drift": unknown,
        "all_mismatches": annotated,
        "lineage_registry": registry_for(list(code_hash_inputs or [])),
        "deduction": (
            "Score-affecting drift requires a rebuild before certified scoring. "
            "Scorer-runtime drift can reuse the compiled arrays for quick scoring, "
            "but remains uncertified until scorer recertification records current hashes. "
            "Historical outcome provenance drift means the compiled tape can still be used "
            "for score-only research against its embedded outcome arrays, but it is not "
            "certified against the refreshed current outcome model. Non-score drift can be "
            "used for quick estimates, but remains uncertified."
        ),
    }


def manifest_mismatches(manifest: dict[str, Any], code_hash_inputs: list[str] | tuple[str, ...]) -> list[dict[str, Any]]:
    mismatches: list[dict[str, Any]] = []
    source_semantic = manifest.get("source_semantic_hashes") or {}
    source_identities = manifest.get("source_identities") or {}
    for path, expected in (manifest.get("source_hashes") or {}).items():
        if not os.path.exists(path):
            mismatches.append({
                "path": path,
                "reason": "source_missing",
                "layer": "source_market_data",
                "score_affecting": True,
                "dependency_known": True,
                "lineage_reason": "A source decision tape is missing.",
            })
            continue
        actual = tournament_safety._file_sha256(path)
        if actual != expected:
            expected_semantic = source_semantic.get(path)
            if expected_semantic is None and isinstance(source_identities.get(path), dict):
                expected_semantic = source_identities[path].get("semantic_sha256")
            if expected_semantic:
                try:
                    current_identity = step2_artifact_identity.jsonl_gz_identity(path)
                    actual_semantic = current_identity.get("semantic_sha256")
                except Exception as exc:
                    actual_semantic = None
                    current_identity = {"error": repr(exc)}
                if actual_semantic == expected_semantic:
                    continue
                mismatches.append({
                    "path": path,
                    "reason": "source_semantic_hash_mismatch",
                    "expected": expected_semantic,
                    "actual": actual_semantic,
                    "expected_physical": expected,
                    "actual_physical": actual,
                    "current_identity": current_identity,
                    "layer": "source_market_data",
                    "score_affecting": True,
                    "dependency_known": True,
                    "lineage_reason": "Decoded source decision-tape content changed after this compiled tape was built.",
                })
                continue
            mismatches.append({
                "path": path,
                "reason": "source_hash_mismatch",
                "expected": expected,
                "actual": actual,
                "layer": "source_market_data",
                "score_affecting": True,
                "dependency_known": True,
                "lineage_reason": "A source decision tape changed after this compiled tape was built.",
            })
    recorded = manifest.get("code_hashes") or {}
    for path, expected in recorded.items():
        abs_path = path if os.path.isabs(path) else str(HERE / path)
        if not os.path.exists(abs_path):
            mismatches.append({"path": path, "reason": "code_missing"})
            continue
        actual = tournament_safety._file_sha256(abs_path)
        if actual != expected:
            mismatches.append({"path": path, "reason": "code_hash_mismatch", "expected": expected, "actual": actual})
    for path in code_hash_inputs:
        if path not in recorded:
            mismatches.append({"path": path, "reason": "code_hash_not_recorded"})
    return mismatches


def evaluate_manifest(
    manifest: dict[str, Any],
    *,
    code_hash_inputs: list[str] | tuple[str, ...],
    semantic_mismatches: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if not code_hash_inputs:
        code_hash_inputs = list((manifest.get("code_hashes") or {}).keys())
    mismatches = manifest_mismatches(manifest, code_hash_inputs)
    mismatches.extend(semantic_mismatches or [])
    return evaluate_mismatches(
        mismatches,
        manifest_path=str(manifest.get("manifest_path") or ""),
        code_hash_inputs=code_hash_inputs,
    )


def build_report(manifest_path: str, code_hash_inputs: list[str] | tuple[str, ...]) -> dict[str, Any]:
    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)
    manifest["manifest_path"] = manifest_path
    if not code_hash_inputs:
        code_hash_inputs = list((manifest.get("code_hashes") or {}).keys())
    return evaluate_manifest(manifest, code_hash_inputs=code_hash_inputs)


def main() -> int:
    parser = argparse.ArgumentParser(description="Classify compiled Step 2 tape lineage drift.")
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--code-hash-input", action="append", default=[])
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    code_inputs = args.code_hash_input or []
    payload = build_report(args.manifest, code_inputs)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    else:
        print(json.dumps({
            "status": payload.get("status"),
            "certified": payload.get("certified"),
            "quick_score_allowed": payload.get("quick_score_allowed"),
            "rebuild_required": payload.get("rebuild_required"),
            "recommended_action": payload.get("recommended_action"),
            "score_affecting_count": payload.get("score_affecting_count"),
            "non_score_count": payload.get("non_score_count"),
            "layer_counts": payload.get("layer_counts"),
        }, indent=2, sort_keys=True))
    return 0 if payload.get("quick_score_allowed") else 2


if __name__ == "__main__":
    raise SystemExit(main())
