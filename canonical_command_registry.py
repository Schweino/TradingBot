from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class CanonicalCommand:
    name: str
    purpose: str
    canonical_invocation: str
    owner_script: str
    category: str
    aliases: tuple[str, ...] = field(default_factory=tuple)
    notes: tuple[str, ...] = field(default_factory=tuple)

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "purpose": self.purpose,
            "canonical_invocation": self.canonical_invocation,
            "owner_script": self.owner_script,
            "category": self.category,
            "aliases": list(self.aliases),
            "notes": list(self.notes),
        }


COMMANDS: dict[str, CanonicalCommand] = {
    "status": CanonicalCommand(
        name="status",
        purpose="Read the live mock engine status without mutating anything.",
        canonical_invocation="python ops.py status [day] --json",
        owner_script="ops.py",
        category="live",
    ),
    "app-start": CanonicalCommand(
        name="app-start",
        purpose="Ensure the Flask/mock app is reachable.",
        canonical_invocation="python ops.py app-start [day] --json",
        owner_script="automation_ops.py",
        category="live",
        aliases=("start",),
    ),
    "monitor": CanonicalCommand(
        name="monitor",
        purpose="Start the intraday live monitor for the trading day.",
        canonical_invocation="python ops.py monitor [day] --json",
        owner_script="automation_ops.py",
        category="live",
    ),
    "context": CanonicalCommand(
        name="context",
        purpose="Refresh Codex review context and artifact index.",
        canonical_invocation="python ops.py context [day] --json",
        owner_script="ops.py",
        category="review",
    ),
    "pre-open": CanonicalCommand(
        name="pre-open",
        purpose="Run the pre-open automation phase.",
        canonical_invocation="python ops.py pre-open [day] --json",
        owner_script="automation_ops.py",
        category="automation",
    ),
    "post-open": CanonicalCommand(
        name="post-open",
        purpose="Run the post-open automation phase.",
        canonical_invocation="python ops.py post-open [day] --json",
        owner_script="automation_ops.py",
        category="automation",
    ),
    "intraday": CanonicalCommand(
        name="intraday",
        purpose="Refresh intraday Step 2/parity artifacts.",
        canonical_invocation="python ops.py intraday [day] --json",
        owner_script="automation_ops.py",
        category="automation",
    ),
    "pre-flat": CanonicalCommand(
        name="pre-flat",
        purpose="Run the pre-flatten safety phase.",
        canonical_invocation="python ops.py pre-flat [day] --json",
        owner_script="automation_ops.py",
        category="automation",
    ),
    "post-close": CanonicalCommand(
        name="post-close",
        purpose="Run the full end-of-day automation chain.",
        canonical_invocation="python ops.py post-close [day] --json",
        owner_script="automation_ops.py",
        category="automation",
        aliases=("eod", "post-market"),
    ),
    "hunt": CanonicalCommand(
        name="hunt",
        purpose="Search direct Step 2 variants against the canonical compiled Step 2 surface.",
        canonical_invocation="python ops.py hunt --beat-pct 10 --target-count 1 --json",
        owner_script="step2_adaptive_hunter.py",
        category="research",
        aliases=("variant-hunt", "step2-hunt", "variants"),
        notes=(
            "This is the canonical replacement for old Step 1/2/2R direct experiments.",
            "Worker count is clamped by worker_policy.DEFAULT_MAX_WORKERS.",
        ),
    ),
    "promote": CanonicalCommand(
        name="promote",
        purpose="Promote a Step 2 candidate through preflight, evidence, rollback snapshot, and restart.",
        canonical_invocation="python ops.py promote --candidate-json PATH --rank 1 --json",
        owner_script="promote_active_profile.py",
        category="promotion",
        aliases=("promotion",),
        notes=("Promotion is blocked if the preflight bundle fails unless explicitly overridden.",),
    ),
    "parity": CanonicalCommand(
        name="parity",
        purpose="Build the Live-vs-Step2 parity report for a day.",
        canonical_invocation="python ops.py parity [day] --json",
        owner_script="live_step2_parity_report.py",
        category="review",
        aliases=("parity-review", "review-parity"),
    ),
    "scorecard": CanonicalCommand(
        name="scorecard",
        purpose="Build the compact daily parity scorecard for a day.",
        canonical_invocation="python ops.py scorecard [day] --json",
        owner_script="daily_parity_scorecard.py",
        category="review",
    ),
    "step3-audit": CanonicalCommand(
        name="step3-audit",
        purpose="Run the current mock-parity Step 3 audit on actual mock trade corpora.",
        canonical_invocation="python ops.py step3-audit --start YYYY-MM-DD --end YYYY-MM-DD --start-balance 100000 --json",
        owner_script="mock_replay.py",
        category="review",
        aliases=("mock-audit", "step3"),
        notes=("This is not the old synthetic full replay path.",),
    ),
    "readiness": CanonicalCommand(
        name="readiness",
        purpose="Run no-surprises smoke, drift, pre-market parity, promotion preflight, process singleton, optional strict broker, and rollback dry-run checks.",
        canonical_invocation="python ops.py readiness [day] --require-direct-broker --json",
        owner_script="ops.py",
        category="safety",
        aliases=("ready",),
    ),
    "rollback-drill": CanonicalCommand(
        name="rollback-drill",
        purpose="Verify the current rollback snapshot and optionally perform an explicit rollback restore.",
        canonical_invocation="python ops.py rollback-drill --json",
        owner_script="rollback_drill.py",
        category="safety",
        aliases=("rollback-check",),
    ),
    "commands": CanonicalCommand(
        name="commands",
        purpose="List the canonical command surface and deprecated direct-script routes.",
        canonical_invocation="python ops.py commands --json",
        owner_script="canonical_command_registry.py",
        category="meta",
        aliases=("catalog", "help-commands"),
    ),
}

ALIASES: dict[str, str] = {
    alias: name
    for name, command in COMMANDS.items()
    for alias in command.aliases
}

DEPRECATED_DIRECT_SCRIPTS: dict[str, dict[str, str]] = {
    "backtest_variants.py": {
        "replacement": "python ops.py hunt --beat-pct 5 --target-count 1 --json",
        "reason": "Legacy hand-built variant experiment; not part of the current Step 2/Live parity surface.",
    },
    "decision_tape_validate_top.py": {
        "replacement": "python ops.py hunt --beat-pct 5 --target-count 1 --json",
        "reason": "Older decision-tape finalist promotion path; Step 2 adaptive hunt is now canonical.",
    },
    "variant_tournament_runner.py": {
        "replacement": "python ops.py hunt --beat-pct 5 --target-count 1 --json",
        "reason": "Legacy multi-stage tournament labels Step 3 as full replay; current promotion scoring is Step 2.",
    },
    "multi_profile_full_replay.py": {
        "replacement": "python ops.py step3-audit --start YYYY-MM-DD --end YYYY-MM-DD --start-balance 100000 --json",
        "reason": "Legacy synthetic multi-profile replay; current Step 3 is mock-parity audit.",
    },
    "full_replay_finalists.py": {
        "replacement": "python ops.py step3-audit --start YYYY-MM-DD --end YYYY-MM-DD --start-balance 100000 --json",
        "reason": "Direct finalist replay is no longer the default promotion gate; use canonical Step 3 audit.",
    },
}


def resolve(name: str) -> str:
    raw = str(name or "").strip()
    return ALIASES.get(raw, raw)


def command(name: str) -> CanonicalCommand:
    resolved = resolve(name)
    if resolved not in COMMANDS:
        raise KeyError(resolved)
    return COMMANDS[resolved]


def catalog() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "commands": [COMMANDS[name].as_dict() for name in sorted(COMMANDS)],
        "aliases": dict(sorted(ALIASES.items())),
        "deprecated_direct_scripts": dict(sorted(DEPRECATED_DIRECT_SCRIPTS.items())),
    }


def deprecated_script_entry(path: str) -> dict[str, str] | None:
    return DEPRECATED_DIRECT_SCRIPTS.get(os.path.basename(path))


def deprecated_script_message(path: str) -> str:
    entry = deprecated_script_entry(path)
    if not entry:
        return ""
    script = os.path.basename(path)
    return (
        f"{script} is a deprecated direct entrypoint.\n"
        f"Reason: {entry['reason']}\n"
        f"Use: {entry['replacement']}\n"
        "If you are intentionally doing a forensic legacy run, pass --allow-direct-legacy."
    )


def enforce_direct_script_allowed(path: str) -> None:
    entry = deprecated_script_entry(path)
    if not entry:
        return
    if "--allow-direct-legacy" in sys.argv:
        sys.argv.remove("--allow-direct-legacy")
        return
    print(deprecated_script_message(path), file=sys.stderr)
    raise SystemExit(2)
