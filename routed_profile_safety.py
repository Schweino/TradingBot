"""Promotion safety checks for routed scoring profiles."""
from __future__ import annotations

from datetime import datetime
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore


CT = ZoneInfo("America/Chicago")
DEFAULTS = {
    "max_routes": 20,
    "min_route_opportunities": 10,
    "min_route_trades": 0,
    "max_route_opportunity_share_pct": 85.0,
    "max_skip_share_pct": 70.0,
}


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec="seconds")


def _route_rows(candidate: dict[str, Any]) -> list[dict[str, Any]]:
    audit = candidate.get("route_audit") if isinstance(candidate.get("route_audit"), dict) else {}
    rows = audit.get("routes") if isinstance(audit.get("routes"), list) else []
    if rows:
        return [row for row in rows if isinstance(row, dict)]
    nested = candidate.get("routed_profile_audit") if isinstance(candidate.get("routed_profile_audit"), dict) else {}
    rows = nested.get("routes") if isinstance(nested.get("routes"), list) else []
    return [row for row in rows if isinstance(row, dict)]


def _thresholds(candidate: dict[str, Any]) -> dict[str, float]:
    config = candidate.get("route_safety") if isinstance(candidate.get("route_safety"), dict) else {}
    out = dict(DEFAULTS)
    for key in out:
        if config.get(key) is not None:
            out[key] = float(config.get(key))
    return out


def has_routes(candidate: dict[str, Any] | None) -> bool:
    candidate = candidate or {}
    return bool(candidate.get("routes")) or bool(candidate.get("routed_scoring_profile"))


def evaluate_candidate(candidate: dict[str, Any] | None, *, require_audit: bool = True) -> dict[str, Any]:
    candidate = candidate or {}
    if not has_routes(candidate):
        return {
            "schema_version": 1,
            "created_at_ct": _now_ct(),
            "ok": True,
            "status": "not_routed",
            "skipped": True,
            "checks": [],
        }
    routes = candidate.get("routes") if isinstance(candidate.get("routes"), list) else []
    audit_rows = _route_rows(candidate)
    thresholds = _thresholds(candidate)
    checks: list[dict[str, Any]] = []

    def add(name: str, ok: bool, actual: Any = None, expected: Any = None) -> None:
        row = {"name": name, "ok": bool(ok)}
        if actual is not None:
            row["actual"] = actual
        if expected is not None:
            row["expected"] = expected
        checks.append(row)

    add("routes_present", bool(routes), len(routes), "> 0")
    add("route_count_within_cap", len(routes) <= int(thresholds["max_routes"]), len(routes), thresholds["max_routes"])
    for idx, route in enumerate(routes):
        if not isinstance(route, dict):
            add(f"route_{idx}_is_object", False, type(route).__name__, "object")
            continue
        add(f"route_{idx}_has_name", bool(route.get("name")), route.get("name"), "non-empty")
        add(f"route_{idx}_has_action", str(route.get("action") or "score") in {"score", "skip", "veto", "force_long", "force_short", "fallback"}, route.get("action"), "known action")
        add(f"route_{idx}_has_match", isinstance(route.get("match"), dict) and bool(route.get("match")), route.get("match"), "non-empty match")

    add("route_audit_present", bool(audit_rows) or not require_audit, len(audit_rows), "> 0")
    route_names = {str(route.get("name") or "") for route in routes if isinstance(route, dict) and route.get("name")}
    named_audit = [row for row in audit_rows if row.get("route") != "fallback"]
    audit_route_names = {str(row.get("route") or "") for row in named_audit if row.get("route")}
    if audit_rows:
        missing_audits = sorted(route_names - audit_route_names)
        stale_audits = sorted(audit_route_names - route_names)
        add("route_audit_matches_current_routes", not missing_audits and not stale_audits, {
            "missing_audits": missing_audits,
            "stale_audits": stale_audits,
        }, "audit route names match current routes")
    for row in named_audit:
        route = str(row.get("route") or "unknown")
        matched = int(row.get("matched_opportunities") or 0)
        share = float(row.get("opportunity_share_pct") or 0.0)
        skipped = int(row.get("skipped_sides") or 0)
        trades = row.get("accepted_trades")
        skip_share = (100.0 * skipped / matched) if matched else 0.0
        add(f"route_{route}_min_opportunities", matched >= int(thresholds["min_route_opportunities"]), matched, thresholds["min_route_opportunities"])
        add(f"route_{route}_max_opportunity_share", share <= float(thresholds["max_route_opportunity_share_pct"]), round(share, 4), thresholds["max_route_opportunity_share_pct"])
        add(f"route_{route}_max_skip_share", skip_share <= float(thresholds["max_skip_share_pct"]), round(skip_share, 4), thresholds["max_skip_share_pct"])
        if trades is not None and int(thresholds["min_route_trades"]) > 0:
            add(f"route_{route}_min_trades", int(trades or 0) >= int(thresholds["min_route_trades"]), trades, thresholds["min_route_trades"])

    ok = all(row.get("ok") for row in checks)
    return {
        "schema_version": 1,
        "created_at_ct": _now_ct(),
        "ok": ok,
        "status": "ok" if ok else "blocked",
        "skipped": False,
        "thresholds": thresholds,
        "route_count": len(routes),
        "audited_route_count": len(named_audit),
        "checks": checks,
    }
