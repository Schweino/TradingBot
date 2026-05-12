"""Reporting-only diagnostics for semantic cache drift.

This module is intentionally kept out of compiled-tape code-hash inputs. It can
make cache receipts more explainable without invalidating existing compiled
artifacts just because diagnostics improved.
"""
from __future__ import annotations

import json
import os
from typing import Any

import semantic_config
import step2_latency_model
import tournament_safety


def _stable_hash(payload: Any) -> str:
    return tournament_safety.stable_json_hash(payload, length=64)


def _read_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return json.load(f)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _value_summary(value: Any) -> dict[str, Any]:
    summary = {
        "type": type(value).__name__,
        "semantic_hash": _stable_hash(value),
    }
    if isinstance(value, dict):
        summary["keys"] = sorted(str(key) for key in value.keys())
        summary["size"] = len(value)
    elif isinstance(value, (list, tuple)):
        summary["size"] = len(value)
        if len(value) <= 10:
            summary["value"] = list(value)
    else:
        summary["value"] = value
    return summary


def latency_model_fingerprint(path: str | None = None) -> dict[str, Any]:
    model_path = path or step2_latency_model.DEFAULT_MODEL_PATH
    exists = bool(model_path and os.path.exists(model_path))
    model = _read_json(model_path, {}) if exists else {}
    if not isinstance(model, dict):
        model = {}
    sample_count = _safe_int(model.get("sample_count"), 0)
    unique_trade_sample_count = _safe_int(model.get("unique_trade_sample_count"), 0)
    source = str(model.get("source") or "")
    warnings: list[str] = []
    if not exists:
        warnings.append("latency_model_missing")
    if source == "default_latency_model":
        warnings.append("latency_model_default_source")
    if sample_count <= 0:
        warnings.append("latency_model_zero_sample")
    return {
        "path": os.path.abspath(model_path) if model_path else "",
        "exists": exists,
        "sha256": tournament_safety._file_sha256(model_path) if exists else None,
        "bytes": os.path.getsize(model_path) if exists else 0,
        "mtime": os.path.getmtime(model_path) if exists else None,
        "source": source or None,
        "created_at_ct": model.get("created_at_ct"),
        "days": model.get("days") if isinstance(model.get("days"), list) else [],
        "sample_count": sample_count,
        "unique_trade_sample_count": unique_trade_sample_count,
        "groups_count": len(model.get("groups") or {}) if isinstance(model.get("groups"), dict) else 0,
        "groups_v2_count": len(model.get("groups_v2") or {}) if isinstance(model.get("groups_v2"), dict) else 0,
        "is_default_or_empty": bool((source == "default_latency_model") or sample_count <= 0),
        "learning_warnings": warnings,
    }


def _section_field_diffs(recorded_payload: Any, current_payload: Any) -> list[dict[str, Any]]:
    if not isinstance(recorded_payload, dict) or not isinstance(current_payload, dict):
        return []
    fields = sorted({str(key) for key in recorded_payload.keys()} | {str(key) for key in current_payload.keys()})
    diffs = []
    for field in fields:
        expected = recorded_payload.get(field)
        actual = current_payload.get(field)
        if _stable_hash(expected) == _stable_hash(actual):
            continue
        diffs.append({
            "field": field,
            "expected": _value_summary(expected),
            "actual": _value_summary(actual),
        })
    return diffs


def section_diagnostics(
    section: str,
    *,
    expected_hash: Any = None,
    actual_hash: Any = None,
    recorded_payload: Any = None,
    current_payload: Any = None,
) -> dict[str, Any]:
    current_payload = current_payload if current_payload is not None else semantic_config.sections().get(section)
    field_diffs = _section_field_diffs(recorded_payload, current_payload)
    out: dict[str, Any] = {
        "section": section,
        "expected_section_hash": expected_hash,
        "actual_section_hash": actual_hash,
        "field_level_diff_available": isinstance(recorded_payload, dict),
        "changed_fields": [row.get("field") for row in field_diffs],
        "field_diffs": field_diffs,
    }
    if section == "path_outcomes":
        current_latency = latency_model_fingerprint()
        recorded_latency_hash = (
            recorded_payload.get("latency_model_hash")
            if isinstance(recorded_payload, dict)
            else None
        )
        actual_latency_hash = (
            current_payload.get("latency_model_hash")
            if isinstance(current_payload, dict)
            else current_latency.get("sha256")
        )
        confirmed = bool(recorded_latency_hash and recorded_latency_hash != actual_latency_hash)
        out["latency_model"] = {
            "recorded_hash": recorded_latency_hash,
            "current_hash": actual_latency_hash,
            "changed": confirmed if recorded_latency_hash else None,
            "current": current_latency,
        }
        out["suspected_invalidators"] = [{
            "field": "latency_model_hash",
            "confidence": "confirmed" if confirmed else "needs_recorded_section_payload",
            "reason": (
                "The compiled tape recorded a different latency model hash."
                if confirmed
                else "path_outcomes includes latency_model_hash, but this manifest did not store the old section payload."
            ),
        }]
        out["learning_warnings"] = list(current_latency.get("learning_warnings") or [])
    return out


def enrich_semantic_mismatches(
    mismatches: list[dict[str, Any]],
    *,
    config: dict[str, Any] | None = None,
    recorded_payloads: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    recorded_payloads = recorded_payloads or {}
    current_sections = semantic_config.sections(config)
    enriched = []
    for row in mismatches or []:
        item = dict(row)
        section = str(item.get("section") or "")
        if section:
            item["diagnostics"] = section_diagnostics(
                section,
                expected_hash=item.get("expected"),
                actual_hash=item.get("actual"),
                recorded_payload=recorded_payloads.get(section),
                current_payload=current_sections.get(section),
            )
        enriched.append(item)
    return enriched
