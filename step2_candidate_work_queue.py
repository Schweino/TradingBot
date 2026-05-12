"""Deduplicating Step 2 candidate work queue."""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock
from typing import Any

import step2_manifest_resolver
import step2_score_cache
from step2_scorer_client import ResidentScorerClient


HERE = Path(__file__).resolve().parent
DEFAULT_QUEUE_DB = HERE / "postmortem" / "score_cache" / "step2_candidate_queue.sqlite"
SCHEMA_VERSION = 2


def _now_epoch() -> float:
    return time.time()


def _variant_payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "name": str(row.get("name") or row.get("variant") or "candidate"),
        "weights": dict(row.get("weights") or {}),
        "bias": float(row.get("bias") or 0.0),
        "routes": list(row.get("routes") or []),
    }
    if isinstance(row.get("base_weights"), dict):
        payload["base_weights"] = dict(row.get("base_weights") or {})
        payload["base_bias"] = float(row.get("base_bias") or 0.0)
    return payload


def _variant_key(row: dict[str, Any]) -> str:
    payload = _variant_payload(row)
    payload.pop("base_weights", None)
    payload.pop("base_bias", None)
    return step2_score_cache._stable_hash(payload, length=32)


def _walk_variants(payload: Any):
    if isinstance(payload, dict):
        if isinstance(payload.get("weights"), dict):
            yield _variant_payload(payload)
        for key in ("variants", "candidates", "leaderboard", "winners", "rows", "results"):
            if key in payload:
                yield from _walk_variants(payload[key])
    elif isinstance(payload, list):
        for item in payload:
            yield from _walk_variants(item)


def load_variants(path: str | os.PathLike[str]) -> list[dict[str, Any]]:
    payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    seen = set()
    out = []
    for row in _walk_variants(payload):
        key = _variant_key(row)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


class CandidateWorkQueue:
    def __init__(self, path: str | os.PathLike[str] = DEFAULT_QUEUE_DB):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.path), timeout=30.0)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def close(self) -> None:
        self.conn.close()

    def _init_db(self) -> None:
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS candidate_queue (
                variant_key TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                variant_json TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at REAL NOT NULL,
                claimed_at REAL,
                claimed_by TEXT,
                screened_at REAL,
                screened_pnl REAL,
                scored_at REAL,
                step2_pnl REAL,
                result_json TEXT,
                decision_hash TEXT,
                error TEXT
            )
        """)
        self._ensure_columns()
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS queue_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        self.conn.execute(
            "INSERT OR REPLACE INTO queue_meta(key, value) VALUES (?, ?)",
            ("schema_version", str(SCHEMA_VERSION)),
        )
        self.conn.commit()

    def _ensure_columns(self) -> None:
        existing = {
            str(row["name"])
            for row in self.conn.execute("PRAGMA table_info(candidate_queue)").fetchall()
        }
        for name, decl in (
            ("claimed_by", "TEXT"),
            ("screened_at", "REAL"),
            ("screened_pnl", "REAL"),
        ):
            if name not in existing:
                self.conn.execute(f"ALTER TABLE candidate_queue ADD COLUMN {name} {decl}")

    def submit(self, variants: list[dict[str, Any]], *, source: str = "manual") -> dict[str, Any]:
        inserted = 0
        duplicates = 0
        for row in variants:
            payload = _variant_payload(row)
            key = _variant_key(payload)
            try:
                self.conn.execute(
                    """
                    INSERT INTO candidate_queue(variant_key, source, variant_json, status, created_at)
                    VALUES (?, ?, ?, 'pending', ?)
                    """,
                    (key, source, json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str), _now_epoch()),
                )
                inserted += 1
            except sqlite3.IntegrityError:
                duplicates += 1
        self.conn.commit()
        return {"inserted": inserted, "duplicates": duplicates, "source": source}

    def pending(self, limit: int, *, worker_id: str = "") -> list[dict[str, Any]]:
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            rows = self.conn.execute(
                """
                SELECT variant_key, variant_json FROM candidate_queue
                WHERE status='pending'
                ORDER BY created_at ASC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
            now = _now_epoch()
            keys = [row["variant_key"] for row in rows]
            if keys:
                self.conn.executemany(
                    """
                    UPDATE candidate_queue
                    SET status='claimed', claimed_at=?, claimed_by=?
                    WHERE variant_key=? AND status='pending'
                    """,
                    [(now, str(worker_id), key) for key in keys],
                )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        out = []
        for row in rows:
            payload = json.loads(row["variant_json"])
            payload["variant_key"] = row["variant_key"]
            out.append(payload)
        return out

    def mark_scored(self, rows: list[dict[str, Any]]) -> None:
        updates = []
        for row in rows:
            key = row.get("variant_key") or _variant_key(row)
            cache = row.get("score_cache") if isinstance(row.get("score_cache"), dict) else {}
            updates.append((
                "scored",
                _now_epoch(),
                float(row.get("step2_pnl") or 0.0),
                json.dumps(row, sort_keys=True, separators=(",", ":"), default=str),
                cache.get("decision_hash") or "",
                "",
                key,
            ))
        self.conn.executemany(
            """
            UPDATE candidate_queue
            SET status=?, scored_at=?, step2_pnl=?, result_json=?, decision_hash=?, error=?
            WHERE variant_key=?
            """,
            updates,
        )
        self.conn.commit()

    def mark_screened(self, rows: list[dict[str, Any]]) -> None:
        updates = []
        for row in rows:
            key = row.get("variant_key") or _variant_key(row)
            updates.append((
                "screened",
                _now_epoch(),
                float(row.get("step2_pnl") or 0.0),
                json.dumps(row, sort_keys=True, separators=(",", ":"), default=str),
                key,
            ))
        self.conn.executemany(
            """
            UPDATE candidate_queue
            SET status=?, screened_at=?, screened_pnl=?, result_json=?
            WHERE variant_key=?
            """,
            updates,
        )
        self.conn.commit()

    def mark_error(self, variants: list[dict[str, Any]], error: str) -> None:
        self.conn.executemany(
            "UPDATE candidate_queue SET status='error', error=? WHERE variant_key=?",
            [(error, row.get("variant_key") or _variant_key(row)) for row in variants],
        )
        self.conn.commit()

    def stats(self) -> dict[str, Any]:
        rows = self.conn.execute("""
            SELECT status, COUNT(*) AS count FROM candidate_queue GROUP BY status ORDER BY status
        """).fetchall()
        total = self.conn.execute("SELECT COUNT(*) AS n FROM candidate_queue").fetchone()
        return {
            "db": str(self.path.resolve()),
            "bytes": self.path.stat().st_size if self.path.exists() else 0,
            "total": int(total["n"] if total else 0),
            "by_status": {str(row["status"]): int(row["count"]) for row in rows},
        }

    def leaderboard(self, limit: int = 100) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT variant_key, variant_json, result_json FROM candidate_queue
            WHERE status='scored' AND result_json IS NOT NULL
            ORDER BY step2_pnl DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        out = []
        for row in rows:
            result = json.loads(row["result_json"])
            variant = json.loads(row["variant_json"])
            result.setdefault("variant", variant.get("name") or variant.get("variant"))
            result.setdefault("weights", variant.get("weights") or {})
            result.setdefault("bias", float(variant.get("bias") or 0.0))
            result.setdefault("routes", variant.get("routes") or [])
            result["variant_key"] = row["variant_key"]
            out.append(result)
        return out

    def top_variants(self, limit: int = 20, *, statuses: tuple[str, ...] = ("scored",)) -> list[dict[str, Any]]:
        placeholders = ",".join("?" for _ in statuses)
        rows = self.conn.execute(
            f"""
            SELECT variant_key, variant_json FROM candidate_queue
            WHERE status IN ({placeholders})
            ORDER BY COALESCE(step2_pnl, screened_pnl, -1e99) DESC
            LIMIT ?
            """,
            (*statuses, int(limit)),
        ).fetchall()
        out = []
        for row in rows:
            payload = json.loads(row["variant_json"])
            payload["variant_key"] = row["variant_key"]
            out.append(payload)
        return out


def _attach_variant_keys(rows: list[dict[str, Any]], batch: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row, source in zip(rows, batch):
        row = dict(row)
        row["variant_key"] = source.get("variant_key") or _variant_key(source)
        out.append(row)
    return out


def _refresh_full_finalists(
    *,
    queue_db: str | os.PathLike[str],
    manifest_path: str,
    cache_db: str,
    start_balance: float,
    limit: int,
) -> int:
    if int(limit or 0) <= 0:
        return 0
    queue = CandidateWorkQueue(queue_db)
    try:
        finalists = queue.top_variants(int(limit), statuses=("scored",))
        if not finalists:
            return 0
        with ResidentScorerClient(
            manifest_path=manifest_path,
            cache_db=cache_db,
            start_balance=start_balance,
        ) as client:
            response = client.score(finalists, summary=False, preserve_order=True)
        if not response.get("ok"):
            return 0
        queue.mark_scored(_attach_variant_keys(response.get("rows") or [], finalists))
        return len(response.get("rows") or [])
    finally:
        queue.close()


def _score_pending_worker(
    *,
    queue_db: str | os.PathLike[str],
    manifest_path: str,
    batch_size: int,
    limit: int,
    cache_db: str,
    start_balance: float,
    summary: bool,
    worker_id: str,
    claim_lock: Lock,
    claimed_counter: dict[str, int],
) -> dict[str, Any]:
    queue = CandidateWorkQueue(queue_db)
    scored = 0
    errors = 0
    batches = 0
    try:
        with ResidentScorerClient(
            manifest_path=manifest_path,
            cache_db=cache_db,
            start_balance=start_balance,
        ) as client:
            while True:
                with claim_lock:
                    if limit and claimed_counter["claimed"] >= int(limit):
                        break
                    claim_size = int(batch_size)
                    if limit:
                        claim_size = min(claim_size, int(limit) - claimed_counter["claimed"])
                    claimed_counter["claimed"] += claim_size
                if claim_size <= 0:
                    break
                batch = queue.pending(claim_size, worker_id=worker_id)
                if len(batch) < claim_size:
                    with claim_lock:
                        claimed_counter["claimed"] -= claim_size - len(batch)
                if not batch:
                    break
                try:
                    response = client.score(batch, summary=summary, preserve_order=True)
                    if not response.get("ok"):
                        errors += len(batch)
                        queue.mark_error(batch, response.get("error") or "resident_scorer_error")
                        continue
                    scored_rows = _attach_variant_keys(response.get("rows") or [], batch)
                    queue.mark_scored(scored_rows)
                    scored += len(scored_rows)
                    batches += 1
                except Exception as exc:
                    errors += len(batch)
                    queue.mark_error(batch, repr(exc))
    finally:
        queue.close()
    return {"worker_id": worker_id, "scored": scored, "errors": errors, "batches": batches}


def _recent_manifest_days(manifest_path: str, count: int) -> list[str]:
    try:
        payload = json.loads(Path(manifest_path).read_text(encoding="utf-8-sig"))
    except Exception:
        return []
    days = []
    scope = payload.get("source_days")
    if isinstance(scope, list):
        days.extend(str(day) for day in scope)
    shards = ((payload.get("day_shards") or {}).get("shards") or []) if isinstance(payload.get("day_shards"), dict) else []
    days.extend(str(row.get("day")) for row in shards if row.get("day"))
    unique = sorted({day for day in days if day})
    return unique[-int(count):] if count and unique else []


def _score_pending_progressive(
    *,
    queue_db: str | os.PathLike[str],
    manifest_path: str,
    batch_size: int,
    limit: int,
    cache_db: str,
    start_balance: float,
    summary: bool,
    full_finalists: int,
    progressive_days: list[str],
    promote_fraction: float,
) -> dict[str, Any]:
    queue = CandidateWorkQueue(queue_db)
    scored = 0
    screened = 0
    errors = 0
    batches = 0
    try:
        partial_cache = str(Path(cache_db).with_suffix(".progressive.sqlite"))
        with ResidentScorerClient(
            manifest_path=manifest_path,
            cache_db=partial_cache,
            start_balance=start_balance,
            day_subset=progressive_days,
        ) as partial_client, ResidentScorerClient(
            manifest_path=manifest_path,
            cache_db=cache_db,
            start_balance=start_balance,
        ) as full_client:
            while True:
                remaining = int(limit or 0) - (scored + screened) if limit else int(batch_size)
                if limit and remaining <= 0:
                    break
                batch = queue.pending(min(int(batch_size), remaining if limit else int(batch_size)), worker_id="progressive")
                if not batch:
                    break
                try:
                    partial = partial_client.score(batch, summary=True, preserve_order=True)
                    if not partial.get("ok"):
                        errors += len(batch)
                        queue.mark_error(batch, partial.get("error") or "resident_scorer_error")
                        continue
                    partial_rows = _attach_variant_keys(partial.get("rows") or [], batch)
                    ranked = sorted(zip(batch, partial_rows), key=lambda pair: float(pair[1].get("step2_pnl") or -1e99), reverse=True)
                    promote_count = max(1, int(round(len(ranked) * max(0.0, min(1.0, float(promote_fraction))))))
                    promoted = [pair[0] for pair in ranked[:promote_count]]
                    screened_rows = [pair[1] for pair in ranked[promote_count:]]
                    if screened_rows:
                        queue.mark_screened(screened_rows)
                        screened += len(screened_rows)
                    full = full_client.score(promoted, summary=summary, preserve_order=True)
                    if not full.get("ok"):
                        errors += len(promoted)
                        queue.mark_error(promoted, full.get("error") or "resident_scorer_error")
                        continue
                    full_rows = _attach_variant_keys(full.get("rows") or [], promoted)
                    queue.mark_scored(full_rows)
                    scored += len(full_rows)
                    batches += 1
                except Exception as exc:
                    errors += len(batch)
                    queue.mark_error(batch, repr(exc))
    finally:
        queue.close()
    refreshed = _refresh_full_finalists(
        queue_db=queue_db,
        manifest_path=manifest_path,
        cache_db=cache_db,
        start_balance=start_balance,
        limit=full_finalists if summary else 0,
    )
    queue = CandidateWorkQueue(queue_db)
    try:
        stats = queue.stats()
        leaderboard = queue.leaderboard(20)
    finally:
        queue.close()
    return {
        "queue_db": str(Path(queue_db).resolve()),
        "scored": scored,
        "screened": screened,
        "errors": errors,
        "batches": batches,
        "workers": 1,
        "summary_mode": bool(summary),
        "full_finalists_refreshed": refreshed,
        "progressive": {"enabled": True, "days": progressive_days, "promote_fraction": promote_fraction},
        "stats": stats,
        "leaderboard": leaderboard,
    }


def score_pending(
    *,
    queue_db: str | os.PathLike[str] = DEFAULT_QUEUE_DB,
    manifest_path: str = "",
    batch_size: int = 256,
    limit: int = 0,
    cache_db: str = str(step2_score_cache.DEFAULT_CACHE_DB),
    start_balance: float = 100000.0,
    workers: int = 1,
    summary: bool = True,
    full_finalists: int = 20,
    progressive_days: list[str] | None = None,
    progressive_day_count: int = 0,
    promote_fraction: float = 0.5,
) -> dict[str, Any]:
    if not progressive_days and progressive_day_count:
        progressive_days = _recent_manifest_days(manifest_path, progressive_day_count)
    if progressive_days:
        return _score_pending_progressive(
            queue_db=queue_db,
            manifest_path=manifest_path,
            batch_size=batch_size,
            limit=limit,
            cache_db=cache_db,
            start_balance=start_balance,
            summary=summary,
            full_finalists=full_finalists,
            progressive_days=progressive_days,
            promote_fraction=promote_fraction,
        )

    worker_count = max(1, int(workers or 1))
    claim_lock = Lock()
    claimed_counter = {"claimed": 0}
    results = []
    if worker_count == 1:
        results.append(_score_pending_worker(
            queue_db=queue_db,
            manifest_path=manifest_path,
            batch_size=batch_size,
            limit=limit,
            cache_db=cache_db,
            start_balance=start_balance,
            summary=summary,
            worker_id="worker_0",
            claim_lock=claim_lock,
            claimed_counter=claimed_counter,
        ))
    else:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = [
                executor.submit(
                    _score_pending_worker,
                    queue_db=queue_db,
                    manifest_path=manifest_path,
                    batch_size=batch_size,
                    limit=limit,
                    cache_db=cache_db,
                    start_balance=start_balance,
                    summary=summary,
                    worker_id=f"worker_{idx}",
                    claim_lock=claim_lock,
                    claimed_counter=claimed_counter,
                )
                for idx in range(worker_count)
            ]
            for future in as_completed(futures):
                results.append(future.result())

    refreshed = _refresh_full_finalists(
        queue_db=queue_db,
        manifest_path=manifest_path,
        cache_db=cache_db,
        start_balance=start_balance,
        limit=full_finalists if summary else 0,
    )
    queue = CandidateWorkQueue(queue_db)
    try:
        stats = queue.stats()
        leaderboard = queue.leaderboard(20)
    finally:
        queue.close()
    return {
        "queue_db": str(Path(queue_db).resolve()),
        "scored": sum(int(row.get("scored") or 0) for row in results),
        "errors": sum(int(row.get("errors") or 0) for row in results),
        "batches": sum(int(row.get("batches") or 0) for row in results),
        "workers": worker_count,
        "worker_results": results,
        "summary_mode": bool(summary),
        "full_finalists_refreshed": refreshed,
        "progressive": {"enabled": False},
        "stats": stats,
        "leaderboard": leaderboard,
    }


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Manage and score a deduplicating Step 2 candidate queue.")
    ap.add_argument("--queue-db", default=str(DEFAULT_QUEUE_DB))
    sub = ap.add_subparsers(dest="command")
    submit = sub.add_parser("submit")
    submit.add_argument("--input", required=True)
    submit.add_argument("--source", default="manual")
    score = sub.add_parser("score")
    score.add_argument("--compiled-manifest", default="")
    score.add_argument("--batch-size", type=int, default=256)
    score.add_argument("--limit", type=int, default=0)
    score.add_argument("--cache-db", default=str(step2_score_cache.DEFAULT_CACHE_DB))
    score.add_argument("--start-balance", type=float, default=100000.0)
    score.add_argument("--workers", type=int, default=1)
    score.add_argument("--full", action="store_true", help="Store full result payloads for every scored candidate.")
    score.add_argument("--full-finalists", type=int, default=20)
    score.add_argument("--progressive-days", nargs="*", default=[])
    score.add_argument("--progressive-day-count", type=int, default=0)
    score.add_argument("--promote-fraction", type=float, default=0.5)
    leaderboard = sub.add_parser("leaderboard")
    leaderboard.add_argument("--limit", type=int, default=100)
    sub.add_parser("stats")
    ap.set_defaults(command="stats")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    queue = CandidateWorkQueue(args.queue_db)
    try:
        if args.command == "submit":
            payload = queue.submit(load_variants(args.input), source=args.source)
        elif args.command == "score":
            queue.close()
            payload = score_pending(
                queue_db=args.queue_db,
                manifest_path=args.compiled_manifest,
                batch_size=args.batch_size,
                limit=args.limit,
                cache_db=args.cache_db,
                start_balance=args.start_balance,
                workers=args.workers,
                summary=not bool(args.full),
                full_finalists=args.full_finalists,
                progressive_days=args.progressive_days or None,
                progressive_day_count=args.progressive_day_count,
                promote_fraction=args.promote_fraction,
            )
            print(json.dumps(payload, indent=2, sort_keys=True, default=str))
            return 0
        elif args.command == "leaderboard":
            payload = {"leaderboard": queue.leaderboard(args.limit), "stats": queue.stats()}
        else:
            payload = queue.stats()
    finally:
        try:
            queue.close()
        except Exception:
            pass
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
