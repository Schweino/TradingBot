"""Verify a voided day is not currently visible in core learning stores."""
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

import step2_void_registry


HERE = Path(__file__).resolve().parent
DEFAULT_DB = HERE / "postmortem" / "learning" / "step2_learning.sqlite"


def _count_sqlite_hits(db_path: Path, day: str) -> dict[str, int]:
    if not db_path.exists():
        return {}
    compact = day.replace("-", "")
    hits: dict[str, int] = {}
    conn = sqlite3.connect(str(db_path))
    try:
        tables = [
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            if row and row[0]
        ]
        for table in tables:
            columns = [row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]
            text_columns = [name for name in columns if name.endswith("json") or name in {"run_id", "run_dir", "source_path"}]
            if not text_columns:
                continue
            clauses = " OR ".join([f"{name} LIKE ? OR {name} LIKE ?" for name in text_columns])
            params: list[str] = []
            for _ in text_columns:
                params.extend([f"%{day}%", f"%{compact}%"])
            count = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {clauses}", params).fetchone()[0]
            if count:
                hits[table] = int(count)
    finally:
        conn.close()
    return hits


def verify(day: str, *, db_path: str | Path = DEFAULT_DB) -> dict[str, Any]:
    normalized = step2_void_registry.normalize_day(day)
    if not normalized:
        raise ValueError(f"Invalid day: {day}")
    sqlite_hits = _count_sqlite_hits(Path(db_path), normalized)
    registry_has_day = step2_void_registry.is_day_voided(normalized)
    return {
        "day": normalized,
        "registry_has_day": registry_has_day,
        "ok": registry_has_day and not sqlite_hits,
        "sqlite_hits": sqlite_hits,
        "deduction": "ok means the day is voided and no core SQLite learning table references it.",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify voided-day exclusion from core learning stores.")
    parser.add_argument("day")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = verify(args.day, db_path=args.db)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(f"voided={result['registry_has_day']} sqlite_hits={result['sqlite_hits']}")
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
