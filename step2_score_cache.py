"""Shared Step 2 variant score cache.

This cache stores the expensive parts of Step 2 scoring keyed by:

* compiled tape identity;
* simulation config and gate;
* starting balance;
* full variant payload, including routed variants.

Rows include the final score payload plus the side vector decision hash. The
side vector is stored compressed so future tooling can reuse it without
recomputing the feature dot product or routed masks.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
import zlib
from pathlib import Path
from typing import Any

import numpy as np

import decision_tape_compiled
import routed_scoring_profile
import routed_profile_safety
import step2_quote_aware_guard
import tournament_safety


HERE = Path(__file__).resolve().parent
DEFAULT_CACHE_DB = HERE / "postmortem" / "score_cache" / "step2_score_cache.sqlite"
SCHEMA_VERSION = 2


def _now_epoch() -> float:
    return time.time()


def _stable_hash(payload: Any, length: int = 32) -> str:
    return tournament_safety.stable_json_hash(payload, length=length)


def variant_payload(variant: Any) -> dict[str, Any]:
    if routed_scoring_profile.is_routed_variant(variant):
        return routed_scoring_profile.variant_to_dict(variant)
    return {
        "name": str(getattr(variant, "name", "variant")),
        "weights": dict(getattr(variant, "weights", {}) or {}),
        "bias": float(getattr(variant, "bias", 0.0) or 0.0),
        "routes": [],
    }


def variant_key(variant: Any) -> str:
    return _stable_hash(variant_payload(variant), length=32)


def compiled_cache_key(compiled: dict[str, Any]) -> str:
    manifest = compiled.get("manifest") if isinstance(compiled.get("manifest"), dict) else {}
    payload = {
        "compiled_tape_hash": manifest.get("compiled_tape_hash"),
        "array_payload_sha256": manifest.get("array_payload_sha256") or manifest.get("arrays_sha256"),
        "step2_execution_contract_hash": manifest.get("step2_execution_contract_hash"),
        "exit_replay_model": step2_quote_aware_guard.manifest_exit_replay_model(manifest),
        "rows": compiled.get("rows") or manifest.get("rows"),
        "day_map": compiled.get("day_map") or manifest.get("day_map"),
        "range_linked": bool(manifest.get("range_linked")),
    }
    if manifest.get("day_subset"):
        payload["day_subset"] = manifest.get("day_subset")
    return _stable_hash(payload, length=32)


def sim_config_key(sim_config: dict[str, Any] | None) -> str:
    return _stable_hash(sim_config or {}, length=32)


def gate_key(gate: dict[str, Any] | None) -> str:
    return _stable_hash(gate or {}, length=32)


def _side_blob(side_row: np.ndarray) -> bytes:
    arr = np.asarray(side_row, dtype=np.int8)
    return zlib.compress(arr.tobytes(order="C"), level=3)


def _restore_side(blob: bytes, rows: int) -> np.ndarray:
    raw = zlib.decompress(blob)
    arr = np.frombuffer(raw, dtype=np.int8)
    if arr.size != int(rows):
        raise ValueError(f"cached side vector length mismatch: {arr.size} != {rows}")
    return arr.reshape(1, int(rows))


def _behavior_key_from_result(result: dict[str, Any]) -> str:
    full = result.get("decision_full") if isinstance(result.get("decision_full"), dict) else {}
    payload = {
        "pnl": round(float(full.get("pnl") or result.get("step2_pnl") or 0.0), 2),
        "trades": int(full.get("trades") or result.get("step2_trades") or 0),
        "win_rate_pct": round(float(full.get("win_rate_pct") or result.get("step2_win_rate_pct") or 0.0), 4),
        "by_day": full.get("by_day") or result.get("by_day") or {},
        "by_ticker": full.get("by_ticker") or result.get("by_ticker") or {},
    }
    return _stable_hash(payload, length=32)


def _strip_variant_scoped_route_fields(result: dict[str, Any]) -> None:
    for key in ("route_audit", "routed_profile_audit", "routed_profile_safety"):
        result.pop(key, None)
    result["route_audit_identity_status"] = "sanitized_for_variant_identity_rewrite"


def _refresh_variant_scoped_route_fields(
    result: dict[str, Any],
    compiled: dict[str, Any] | None,
    variant: Any,
    side_row: np.ndarray | None,
) -> None:
    _strip_variant_scoped_route_fields(result)
    if compiled is None or side_row is None or not result.get("routes"):
        return
    try:
        result["route_audit"] = routed_scoring_profile.route_audit(compiled, variant, sides=side_row)
        result["routed_profile_safety"] = routed_profile_safety.evaluate_candidate(result)
        result["route_audit_identity_status"] = "recomputed_for_variant_identity_rewrite"
    except Exception as exc:  # pragma: no cover - defensive cache hygiene
        result["route_audit_error"] = str(exc)


def _result_with_variant_identity(result: dict[str, Any], variant: Any) -> dict[str, Any]:
    payload = variant_payload(variant)
    out = json.loads(json.dumps(result, default=str))
    out["variant"] = payload.get("name") or out.get("variant")
    out["weights"] = dict(payload.get("weights") or {})
    out["bias"] = float(payload.get("bias") or 0.0)
    if payload.get("routes"):
        out["routes"] = list(payload.get("routes") or [])
        out["routed_scoring_profile"] = True
    else:
        out.setdefault("routes", [])
    _strip_variant_scoped_route_fields(out)
    return out


class ScoreCache:
    def __init__(self, path: str | os.PathLike[str] = DEFAULT_CACHE_DB):
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
            CREATE TABLE IF NOT EXISTS score_cache (
                compiled_key TEXT NOT NULL,
                sim_config_key TEXT NOT NULL,
                gate_key TEXT NOT NULL,
                start_balance REAL NOT NULL,
                variant_key TEXT NOT NULL,
                variant_json TEXT NOT NULL,
                decision_hash TEXT NOT NULL,
                side_rows INTEGER NOT NULL,
                side_blob BLOB NOT NULL,
                result_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                last_hit_at REAL,
                hits INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (compiled_key, sim_config_key, gate_key, start_balance, variant_key)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS decision_score_cache (
                compiled_key TEXT NOT NULL,
                sim_config_key TEXT NOT NULL,
                gate_key TEXT NOT NULL,
                start_balance REAL NOT NULL,
                decision_hash TEXT NOT NULL,
                result_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                last_hit_at REAL,
                hits INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (compiled_key, sim_config_key, gate_key, start_balance, decision_hash)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS behavior_score_cache (
                compiled_key TEXT NOT NULL,
                sim_config_key TEXT NOT NULL,
                gate_key TEXT NOT NULL,
                start_balance REAL NOT NULL,
                behavior_key TEXT NOT NULL,
                leader_variant_key TEXT NOT NULL,
                leader_decision_hash TEXT NOT NULL,
                leader_result_json TEXT NOT NULL,
                alias_count INTEGER NOT NULL DEFAULT 1,
                aliases_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                last_hit_at REAL,
                hits INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (compiled_key, sim_config_key, gate_key, start_balance, behavior_key)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS score_cache_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        self.conn.execute(
            "INSERT OR REPLACE INTO score_cache_meta(key, value) VALUES (?, ?)",
            ("schema_version", str(SCHEMA_VERSION)),
        )
        self.conn.commit()

    def get(
        self,
        *,
        compiled_key_value: str,
        sim_config_key_value: str,
        gate_key_value: str,
        start_balance: float,
        variant_key_value: str,
        include_side: bool = False,
        rows: int = 0,
    ) -> dict[str, Any] | None:
        row = self.conn.execute(
            """
            SELECT result_json, decision_hash, side_blob, side_rows
            FROM score_cache
            WHERE compiled_key=? AND sim_config_key=? AND gate_key=?
              AND start_balance=? AND variant_key=?
            """,
            (compiled_key_value, sim_config_key_value, gate_key_value, float(start_balance), variant_key_value),
        ).fetchone()
        if not row:
            return None
        self.conn.execute(
            """
            UPDATE score_cache
            SET hits=hits+1, last_hit_at=?
            WHERE compiled_key=? AND sim_config_key=? AND gate_key=?
              AND start_balance=? AND variant_key=?
            """,
            (_now_epoch(), compiled_key_value, sim_config_key_value, gate_key_value, float(start_balance), variant_key_value),
        )
        self.conn.commit()
        result = json.loads(row["result_json"])
        result["score_cache"] = {
            "hit": True,
            "decision_hash": row["decision_hash"],
            "db": str(self.path.resolve()),
        }
        out = {
            "result": result,
            "decision_hash": row["decision_hash"],
        }
        if include_side:
            out["side"] = _restore_side(row["side_blob"], rows or int(row["side_rows"]))
        return out

    def get_by_decision_hash(
        self,
        *,
        compiled_key_value: str,
        sim_config_key_value: str,
        gate_key_value: str,
        start_balance: float,
        decision_hash: str,
        variant: Any,
    ) -> dict[str, Any] | None:
        row = self.conn.execute(
            """
            SELECT result_json
            FROM decision_score_cache
            WHERE compiled_key=? AND sim_config_key=? AND gate_key=?
              AND start_balance=? AND decision_hash=?
            """,
            (compiled_key_value, sim_config_key_value, gate_key_value, float(start_balance), str(decision_hash)),
        ).fetchone()
        if not row:
            return None
        self.conn.execute(
            """
            UPDATE decision_score_cache
            SET hits=hits+1, last_hit_at=?
            WHERE compiled_key=? AND sim_config_key=? AND gate_key=?
              AND start_balance=? AND decision_hash=?
            """,
            (_now_epoch(), compiled_key_value, sim_config_key_value, gate_key_value, float(start_balance), str(decision_hash)),
        )
        self.conn.commit()
        result = _result_with_variant_identity(json.loads(row["result_json"]), variant)
        result["score_cache"] = {
            "hit": True,
            "decision_hash": str(decision_hash),
            "decision_hash_reused": True,
            "db": str(self.path.resolve()),
        }
        return {
            "result": result,
            "decision_hash": str(decision_hash),
        }

    def put(
        self,
        *,
        compiled_key_value: str,
        sim_config_key_value: str,
        gate_key_value: str,
        start_balance: float,
        variant: Any,
        decision_hash: str,
        side_row: np.ndarray,
        result: dict[str, Any],
    ) -> None:
        result_payload = dict(result)
        result_payload["score_cache"] = {
            "hit": False,
            "decision_hash": decision_hash,
            "db": str(self.path.resolve()),
        }
        self.conn.execute(
            """
            INSERT OR REPLACE INTO score_cache (
                compiled_key, sim_config_key, gate_key, start_balance, variant_key,
                variant_json, decision_hash, side_rows, side_blob, result_json,
                created_at, last_hit_at, hits
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0)
            """,
            (
                compiled_key_value,
                sim_config_key_value,
                gate_key_value,
                float(start_balance),
                variant_key(variant),
                json.dumps(variant_payload(variant), sort_keys=True, separators=(",", ":"), default=str),
                str(decision_hash),
                int(np.asarray(side_row).size),
                _side_blob(side_row),
                json.dumps(result_payload, sort_keys=True, separators=(",", ":"), default=str),
                _now_epoch(),
            ),
        )
        self.conn.execute(
            """
            INSERT OR IGNORE INTO decision_score_cache (
                compiled_key, sim_config_key, gate_key, start_balance, decision_hash,
                result_json, created_at, last_hit_at, hits
            ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, 0)
            """,
            (
                compiled_key_value,
                sim_config_key_value,
                gate_key_value,
                float(start_balance),
                str(decision_hash),
                json.dumps(result_payload, sort_keys=True, separators=(",", ":"), default=str),
                _now_epoch(),
            ),
        )
        self.put_behavior(
            compiled_key_value=compiled_key_value,
            sim_config_key_value=sim_config_key_value,
            gate_key_value=gate_key_value,
            start_balance=float(start_balance),
            behavior_key_value=_behavior_key_from_result(result_payload),
            variant_key_value=variant_key(variant),
            decision_hash=str(decision_hash),
            result=result_payload,
        )
        self.conn.commit()

    def put_behavior(
        self,
        *,
        compiled_key_value: str,
        sim_config_key_value: str,
        gate_key_value: str,
        start_balance: float,
        behavior_key_value: str,
        variant_key_value: str,
        decision_hash: str,
        result: dict[str, Any],
    ) -> None:
        existing = self.conn.execute(
            """
            SELECT aliases_json, alias_count
            FROM behavior_score_cache
            WHERE compiled_key=? AND sim_config_key=? AND gate_key=?
              AND start_balance=? AND behavior_key=?
            """,
            (
                compiled_key_value,
                sim_config_key_value,
                gate_key_value,
                float(start_balance),
                behavior_key_value,
            ),
        ).fetchone()
        alias = {"variant_key": variant_key_value, "decision_hash": str(decision_hash)}
        if existing:
            try:
                aliases = json.loads(existing["aliases_json"])
            except Exception:
                aliases = []
            if alias not in aliases:
                aliases.append(alias)
            self.conn.execute(
                """
                UPDATE behavior_score_cache
                SET alias_count=?, aliases_json=?, last_hit_at=?
                WHERE compiled_key=? AND sim_config_key=? AND gate_key=?
                  AND start_balance=? AND behavior_key=?
                """,
                (
                    len(aliases),
                    json.dumps(aliases[:250], sort_keys=True, separators=(",", ":"), default=str),
                    _now_epoch(),
                    compiled_key_value,
                    sim_config_key_value,
                    gate_key_value,
                    float(start_balance),
                    behavior_key_value,
                ),
            )
            return
        self.conn.execute(
            """
            INSERT OR REPLACE INTO behavior_score_cache (
                compiled_key, sim_config_key, gate_key, start_balance, behavior_key,
                leader_variant_key, leader_decision_hash, leader_result_json,
                alias_count, aliases_json, created_at, last_hit_at, hits
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, NULL, 0)
            """,
            (
                compiled_key_value,
                sim_config_key_value,
                gate_key_value,
                float(start_balance),
                behavior_key_value,
                variant_key_value,
                str(decision_hash),
                json.dumps(result, sort_keys=True, separators=(",", ":"), default=str),
                json.dumps([alias], sort_keys=True, separators=(",", ":"), default=str),
                _now_epoch(),
            ),
        )

    def stats(self) -> dict[str, Any]:
        row = self.conn.execute("""
            SELECT
              COUNT(*) AS n,
              COALESCE(SUM(hits), 0) AS hits,
              MIN(created_at) AS oldest_created,
              MAX(created_at) AS newest_created,
              MAX(last_hit_at) AS newest_hit,
              COALESCE(SUM(LENGTH(side_blob)), 0) AS side_blob_bytes,
              COALESCE(SUM(LENGTH(result_json)), 0) AS result_json_bytes
            FROM score_cache
        """).fetchone()
        drow = self.conn.execute("""
            SELECT
              COUNT(*) AS n,
              COALESCE(SUM(hits), 0) AS hits,
              MIN(created_at) AS oldest_created,
              MAX(created_at) AS newest_created,
              MAX(last_hit_at) AS newest_hit,
              COALESCE(SUM(LENGTH(result_json)), 0) AS result_json_bytes
            FROM decision_score_cache
        """).fetchone()
        brow = self.conn.execute("""
            SELECT
              COUNT(*) AS n,
              COALESCE(SUM(alias_count), 0) AS aliases,
              COALESCE(SUM(hits), 0) AS hits,
              COALESCE(SUM(LENGTH(leader_result_json)), 0) AS result_json_bytes
            FROM behavior_score_cache
        """).fetchone()
        return {
            "db": str(self.path.resolve()),
            "bytes": self.path.stat().st_size if self.path.exists() else 0,
            "entries": int(row["n"] if row else 0),
            "hits": int(row["hits"] if row else 0),
            "decision_entries": int(drow["n"] if drow else 0),
            "decision_hits": int(drow["hits"] if drow else 0),
            "behavior_entries": int(brow["n"] if brow else 0),
            "behavior_aliases": int(brow["aliases"] if brow else 0),
            "behavior_hits": int(brow["hits"] if brow else 0),
            "oldest_created": float(row["oldest_created"] or 0.0) if row else 0.0,
            "newest_created": float(row["newest_created"] or 0.0) if row else 0.0,
            "newest_hit": float(row["newest_hit"] or 0.0) if row else 0.0,
            "side_blob_bytes": int(row["side_blob_bytes"] or 0) if row else 0,
            "result_json_bytes": int(row["result_json_bytes"] or 0) if row else 0,
            "decision_result_json_bytes": int(drow["result_json_bytes"] or 0) if drow else 0,
            "behavior_result_json_bytes": int(brow["result_json_bytes"] or 0) if brow else 0,
        }

    def fast_stats(self) -> dict[str, Any]:
        wal = self.path.with_name(self.path.name + "-wal")
        shm = self.path.with_name(self.path.name + "-shm")
        try:
            page = self.conn.execute("PRAGMA page_count").fetchone()
            page_count = int(page[0] or 0) if page else 0
        except Exception:
            page_count = 0
        try:
            page = self.conn.execute("PRAGMA page_size").fetchone()
            page_size = int(page[0] or 0) if page else 0
        except Exception:
            page_size = 0
        return {
            "db": str(self.path.resolve()),
            "stats_mode": "fast_file_metadata",
            "bytes": self.path.stat().st_size if self.path.exists() else 0,
            "wal_bytes": wal.stat().st_size if wal.exists() else 0,
            "shm_bytes": shm.stat().st_size if shm.exists() else 0,
            "sqlite_page_count": page_count,
            "sqlite_page_size": page_size,
            "sqlite_estimated_bytes": page_count * page_size,
            "exact_counts": False,
            "full_stats_note": "Run with --score-cache-stats-mode full when exact cache aggregate counts are needed.",
        }

    def detailed_stats(self, limit: int = 10) -> dict[str, Any]:
        out = self.stats()
        compiled_rows = self.conn.execute("""
            SELECT compiled_key, COUNT(*) AS entries, COALESCE(SUM(hits), 0) AS hits,
                   MAX(COALESCE(last_hit_at, created_at)) AS last_used_at
            FROM score_cache
            GROUP BY compiled_key
            ORDER BY entries DESC, hits DESC
            LIMIT ?
        """, (int(limit),)).fetchall()
        decision_rows = self.conn.execute("""
            SELECT compiled_key, COUNT(*) AS entries, COALESCE(SUM(hits), 0) AS hits,
                   MAX(COALESCE(last_hit_at, created_at)) AS last_used_at
            FROM decision_score_cache
            GROUP BY compiled_key
            ORDER BY entries DESC, hits DESC
            LIMIT ?
        """, (int(limit),)).fetchall()
        hot_rows = self.conn.execute("""
            SELECT variant_key, decision_hash, hits, created_at, last_hit_at,
                   SUBSTR(variant_json, 1, 240) AS variant_json_preview
            FROM score_cache
            ORDER BY hits DESC, COALESCE(last_hit_at, created_at) DESC
            LIMIT ?
        """, (int(limit),)).fetchall()
        out["by_compiled_key"] = [dict(row) for row in compiled_rows]
        out["decision_by_compiled_key"] = [dict(row) for row in decision_rows]
        out["hot_entries"] = [dict(row) for row in hot_rows]
        return out

    def prune(self, *, older_than_days: float = 0.0, max_entries: int = 0) -> dict[str, Any]:
        before = self.stats()
        deleted_old = 0
        if older_than_days and older_than_days > 0:
            cutoff = _now_epoch() - float(older_than_days) * 86400.0
            cur = self.conn.execute(
                "DELETE FROM score_cache WHERE COALESCE(last_hit_at, created_at) < ?",
                (cutoff,),
            )
            deleted_old = int(cur.rowcount or 0)
            self.conn.execute(
                "DELETE FROM decision_score_cache WHERE COALESCE(last_hit_at, created_at) < ?",
                (cutoff,),
            )
        deleted_overflow = 0
        if max_entries and max_entries > 0:
            cur = self.conn.execute("""
                DELETE FROM score_cache
                WHERE rowid IN (
                    SELECT rowid FROM score_cache
                    ORDER BY COALESCE(last_hit_at, created_at) DESC, hits DESC
                    LIMIT -1 OFFSET ?
                )
            """, (int(max_entries),))
            deleted_overflow = int(cur.rowcount or 0)
            self.conn.execute("""
                DELETE FROM decision_score_cache
                WHERE rowid IN (
                    SELECT rowid FROM decision_score_cache
                    ORDER BY COALESCE(last_hit_at, created_at) DESC, hits DESC
                    LIMIT -1 OFFSET ?
                )
            """, (int(max_entries),))
        self.conn.commit()
        after = self.stats()
        return {
            "db": str(self.path.resolve()),
            "deleted_old": deleted_old,
            "deleted_overflow": deleted_overflow,
            "before": before,
            "after": after,
        }

    def vacuum(self) -> dict[str, Any]:
        before_bytes = self.path.stat().st_size if self.path.exists() else 0
        self.conn.execute("VACUUM")
        self.conn.commit()
        after_bytes = self.path.stat().st_size if self.path.exists() else 0
        return {
            "db": str(self.path.resolve()),
            "before_bytes": before_bytes,
            "after_bytes": after_bytes,
            "saved_bytes": max(0, before_bytes - after_bytes),
        }


def score_variants_cached(
    compiled: dict[str, Any],
    variants: list[Any],
    starting_balance: float,
    *,
    gate: dict[str, Any] | None = None,
    sim_config: dict[str, Any] | None = None,
    cache_db: str | os.PathLike[str] = DEFAULT_CACHE_DB,
    use_cache: bool = True,
) -> list[dict[str, Any]] | None:
    manifest = compiled.get("manifest") if isinstance(compiled.get("manifest"), dict) else {}
    step2_quote_aware_guard.assert_manifest(manifest, context="step2_score_cache")
    if not use_cache:
        return decision_tape_compiled.simulate_variants(compiled, variants, starting_balance, gate=gate, sim_config=sim_config)
    if not variants:
        return []

    cache = ScoreCache(cache_db)
    try:
        ckey = compiled_cache_key(compiled)
        skey = sim_config_key(sim_config)
        gkey = gate_key(gate)
        ts_array = compiled.get("ts")
        rows = int(compiled.get("rows") or (len(ts_array) if ts_array is not None else 0))
        output: list[dict[str, Any] | None] = [None] * len(variants)
        misses: list[Any] = []
        miss_indices: list[int] = []
        seen_in_batch: dict[str, int] = {}
        keys = [variant_key(variant) for variant in variants]

        for idx, (variant, vkey) in enumerate(zip(variants, keys)):
            if vkey in seen_in_batch:
                prior = output[seen_in_batch[vkey]]
                if prior is not None:
                    clone = json.loads(json.dumps(prior, default=str))
                    clone.setdefault("score_cache", {})["batch_duplicate"] = True
                    output[idx] = clone
                    continue
            hit = cache.get(
                compiled_key_value=ckey,
                sim_config_key_value=skey,
                gate_key_value=gkey,
                start_balance=float(starting_balance),
                variant_key_value=vkey,
                rows=rows,
            )
            if hit:
                output[idx] = hit["result"]
                seen_in_batch[vkey] = idx
            else:
                misses.append(variant)
                miss_indices.append(idx)
                seen_in_batch[vkey] = idx

        if misses:
            sides = decision_tape_compiled.side_matrix(compiled, misses)
            decision_hashes = decision_tape_compiled.decision_hashes_from_sides(sides)
            simulated_by_hash: dict[str, dict[str, Any]] = {}
            first_local_for_hash: dict[str, int] = {}
            simulate_local_indices: list[int] = []

            for local_idx, (global_idx, variant, decision_hash) in enumerate(zip(miss_indices, misses, decision_hashes)):
                cached_by_decision = cache.get_by_decision_hash(
                    compiled_key_value=ckey,
                    sim_config_key_value=skey,
                    gate_key_value=gkey,
                    start_balance=float(starting_balance),
                    decision_hash=decision_hash,
                    variant=variant,
                )
                if cached_by_decision:
                    row = cached_by_decision["result"]
                    _refresh_variant_scoped_route_fields(row, compiled, variant, sides[local_idx])
                    cache.put(
                        compiled_key_value=ckey,
                        sim_config_key_value=skey,
                        gate_key_value=gkey,
                        start_balance=float(starting_balance),
                        variant=variant,
                        decision_hash=decision_hash,
                        side_row=sides[local_idx],
                        result=row,
                    )
                    row["score_cache"] = {
                        "hit": True,
                        "decision_hash": decision_hash,
                        "decision_hash_reused": True,
                        "db": str(Path(cache_db).resolve()),
                    }
                    output[global_idx] = row
                    simulated_by_hash[decision_hash] = row
                    continue
                if decision_hash in first_local_for_hash:
                    continue
                first_local_for_hash[decision_hash] = local_idx
                simulate_local_indices.append(local_idx)

            if simulate_local_indices:
                sim_variants = [misses[i] for i in simulate_local_indices]
                sim_sides = sides[np.asarray(simulate_local_indices, dtype=np.int64)]
                miss_rows = decision_tape_compiled.simulate_side_matrix(
                    compiled,
                    sim_variants,
                    sim_sides,
                    starting_balance,
                    gate=gate,
                    sim_config=sim_config,
                )
                if miss_rows is None:
                    return None
                for sim_pos, local_idx in enumerate(simulate_local_indices):
                    variant = misses[local_idx]
                    decision_hash = decision_hashes[local_idx]
                    row = dict(miss_rows[sim_pos])
                    cache.put(
                        compiled_key_value=ckey,
                        sim_config_key_value=skey,
                        gate_key_value=gkey,
                        start_balance=float(starting_balance),
                        variant=variant,
                        decision_hash=decision_hash,
                        side_row=sides[local_idx],
                        result=row,
                    )
                    row["score_cache"] = {
                        "hit": False,
                        "decision_hash": decision_hash,
                        "db": str(Path(cache_db).resolve()),
                    }
                    simulated_by_hash[decision_hash] = row
                    output[miss_indices[local_idx]] = row

            for local_idx, (global_idx, variant, decision_hash) in enumerate(zip(miss_indices, misses, decision_hashes)):
                if output[global_idx] is not None:
                    continue
                source = simulated_by_hash.get(decision_hash)
                if source is None:
                    raise RuntimeError(f"decision hash was neither simulated nor cached: {decision_hash}")
                row = _result_with_variant_identity(source, variant)
                _refresh_variant_scoped_route_fields(row, compiled, variant, sides[local_idx])
                row["score_cache"] = {
                    "hit": True,
                    "decision_hash": decision_hash,
                    "batch_decision_duplicate": True,
                    "db": str(Path(cache_db).resolve()),
                }
                cache.put(
                    compiled_key_value=ckey,
                    sim_config_key_value=skey,
                    gate_key_value=gkey,
                    start_balance=float(starting_balance),
                    variant=variant,
                    decision_hash=decision_hash,
                    side_row=sides[local_idx],
                    result=row,
                )
                output[global_idx] = row
        return [row for row in output if row is not None]
    finally:
        cache.close()


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Inspect and maintain the shared Step 2 score cache.")
    ap.add_argument("--db", default=str(DEFAULT_CACHE_DB))
    sub = ap.add_subparsers(dest="command")
    stats = sub.add_parser("stats", help="Show score-cache stats.")
    stats.add_argument("--limit", type=int, default=10)
    prune = sub.add_parser("prune", help="Delete old or overflow cache rows.")
    prune.add_argument("--older-than-days", type=float, default=0.0)
    prune.add_argument("--max-entries", type=int, default=0)
    sub.add_parser("vacuum", help="Run SQLite VACUUM.")
    ap.set_defaults(command="stats")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    cache = ScoreCache(args.db)
    try:
        if args.command == "prune":
            payload = cache.prune(older_than_days=args.older_than_days, max_entries=args.max_entries)
        elif args.command == "vacuum":
            payload = cache.vacuum()
        else:
            payload = cache.detailed_stats(limit=getattr(args, "limit", 10))
    finally:
        cache.close()
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
