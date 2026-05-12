"""Purge voided-day rows from core Step 2 learning SQLite stores.

The purge is intentionally narrow: it deletes rows whose text identifiers or
JSON payloads reference the voided day token. A backup is written first.
"""
from __future__ import annotations

import argparse
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

import step2_void_registry


HERE = Path(__file__).resolve().parent
DEFAULT_DB = HERE / "postmortem" / "learning" / "step2_learning.sqlite"
DEFAULT_BACKUP_DIR = HERE / "postmortem" / "voided"


def _candidate_text_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    columns = conn.execute(f"PRAGMA table_info({table})").fetchall()
    names = [row[1] for row in columns]
    return [
        name
        for name in names
        if name.endswith("json")
        or name in {
            "run_id",
            "run_dir",
            "source_path",
            "path",
            "variant",
            "artifact_name",
            "experiment_id",
            "debt_id",
            "result_id",
            "memory_id",
        }
    ]


def _table_names(conn: sqlite3.Connection) -> list[str]:
    return [
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        if row and row[0] and not str(row[0]).startswith("sqlite_")
    ]


def _where_clause(columns: list[str]) -> tuple[str, list[str]]:
    clause = " OR ".join([f"{name} LIKE ? OR {name} LIKE ?" for name in columns])
    return clause, []


def purge_sqlite_day(
    day: str,
    *,
    db_path: str | Path = DEFAULT_DB,
    backup_dir: str | Path = DEFAULT_BACKUP_DIR,
    dry_run: bool = False,
) -> dict[str, Any]:
    normalized = step2_void_registry.normalize_day(day)
    if not normalized:
        raise ValueError(f"Invalid day: {day}")
    if not step2_void_registry.is_day_voided(normalized):
        raise ValueError(f"{normalized} is not in the void registry")

    db_path = Path(db_path)
    if not db_path.exists():
        return {"day": normalized, "db": str(db_path), "exists": False, "deleted": {}, "total_deleted": 0}

    backup_path = ""
    if not dry_run:
        backup_root = Path(backup_dir) / normalized
        backup_root.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup = backup_root / f"{db_path.stem}_before_void_purge_{stamp}{db_path.suffix}"
        shutil.copy2(db_path, backup)
        backup_path = str(backup.resolve())

    compact = normalized.replace("-", "")
    deleted: dict[str, int] = {}
    conn = sqlite3.connect(str(db_path))
    try:
        for table in _table_names(conn):
            columns = _candidate_text_columns(conn, table)
            if not columns:
                continue
            clause, _ = _where_clause(columns)
            params: list[str] = []
            for _column in columns:
                params.extend([f"%{normalized}%", f"%{compact}%"])
            count = int(conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {clause}", params).fetchone()[0])
            if count:
                deleted[table] = count
                if not dry_run:
                    conn.execute(f"DELETE FROM {table} WHERE {clause}", params)
        if dry_run:
            conn.rollback()
        else:
            conn.commit()
            conn.execute("VACUUM")
    finally:
        conn.close()

    return {
        "day": normalized,
        "db": str(db_path.resolve()),
        "backup_path": backup_path,
        "dry_run": dry_run,
        "deleted": deleted,
        "total_deleted": sum(deleted.values()),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Purge voided-day rows from Step 2 learning SQLite.")
    parser.add_argument("day")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--backup-dir", default=str(DEFAULT_BACKUP_DIR))
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = purge_sqlite_day(args.day, db_path=args.db, backup_dir=args.backup_dir, dry_run=args.dry_run)
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
