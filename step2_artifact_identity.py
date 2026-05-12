"""Deterministic artifact identity helpers for Step 2 cache rebuilds.

The rebuild planner needs to distinguish "the bytes changed" from "the
scoring content changed." This module keeps those concepts separate and gives
long-running rebuilds a staging/promotion primitive that can be shared by the
builder, compiler, and certifier.
"""
from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import shutil
import time
import zipfile
from pathlib import Path
from typing import Any, Iterable

import numpy as np


SCHEMA_VERSION = 1
IDENTITY_SIDECAR_VERSION = 1


def stable_json_bytes(payload: Any) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")


def stable_json_hash(payload: Any) -> str:
    return hashlib.sha256(stable_json_bytes(payload)).hexdigest()


def file_sha256(path: str | os.PathLike[str]) -> str | None:
    p = Path(path)
    if not p.exists():
        return None
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def identity_sidecar_path(path: str | os.PathLike[str]) -> Path:
    p = Path(path)
    return p.with_name(f"{p.name}.identity.json")


def _read_json(path: str | os.PathLike[str], default: Any = None) -> Any:
    try:
        with Path(path).open("r", encoding="utf-8-sig") as f:
            return json.load(f)
    except Exception:
        return default


def _sidecar_is_fresh(sidecar: dict[str, Any], path: Path, *, verify_hash: bool = False) -> bool:
    if not isinstance(sidecar, dict) or not path.exists():
        return False
    if int(sidecar.get("identity_sidecar_version") or 0) != IDENTITY_SIDECAR_VERSION:
        return False
    stat = path.stat()
    if int(sidecar.get("bytes") or -1) != int(stat.st_size):
        return False
    recorded_mtime_ns = sidecar.get("mtime_ns")
    current_mtime_ns = int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000)))
    if recorded_mtime_ns is not None and int(recorded_mtime_ns) != current_mtime_ns:
        return False
    if verify_hash and sidecar.get("physical_sha256") != file_sha256(path):
        return False
    return True


def write_identity_sidecar(path: str | os.PathLike[str], identity: dict[str, Any]) -> str:
    p = Path(path)
    sidecar = dict(identity)
    sidecar["identity_sidecar_version"] = IDENTITY_SIDECAR_VERSION
    sidecar["artifact_path"] = str(p.resolve())
    if p.exists():
        stat = p.stat()
        sidecar["bytes"] = int(stat.st_size)
        sidecar["mtime"] = stat.st_mtime
        sidecar["mtime_ns"] = int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000)))
    sidecar["sidecar_path"] = str(identity_sidecar_path(p).resolve())
    return write_json(identity_sidecar_path(p), sidecar)


def _canonical_json_line(row: Any) -> bytes:
    return stable_json_bytes(row) + b"\n"


def jsonl_semantic_hash(rows: Iterable[Any]) -> str:
    h = hashlib.sha256()
    row_count = 0
    for row in rows:
        h.update(_canonical_json_line(row))
        row_count += 1
    h.update(f"rows={row_count}".encode("ascii"))
    return h.hexdigest()


def read_jsonl_gz(path: str | os.PathLike[str]) -> list[dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return []
    rows: list[dict[str, Any]] = []
    with gzip.open(p, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                row = json.loads(line)
                if isinstance(row, dict):
                    rows.append(row)
    return rows


def jsonl_gz_identity(
    path: str | os.PathLike[str],
    *,
    use_sidecar: bool = True,
    verify_sidecar_hash: bool = False,
) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {
            "schema_version": SCHEMA_VERSION,
            "path": str(p.resolve()),
            "exists": False,
            "rows": 0,
            "min_ts": 0,
            "max_ts": 0,
            "physical_sha256": None,
            "semantic_sha256": None,
            "bytes": 0,
            "mtime": None,
            "sidecar_path": str(identity_sidecar_path(p).resolve()),
            "sidecar_used": False,
        }
    if use_sidecar:
        sidecar = _read_json(identity_sidecar_path(p), {})
        if _sidecar_is_fresh(sidecar, p, verify_hash=verify_sidecar_hash):
            out = dict(sidecar)
            out["sidecar_used"] = True
            return out
    rows = read_jsonl_gz(p)
    ts_values = [int(row.get("ts") or 0) for row in rows if int(row.get("ts") or 0)]
    stat = p.stat()
    return {
        "schema_version": SCHEMA_VERSION,
        "path": str(p.resolve()),
        "exists": True,
        "rows": len(rows),
        "min_ts": min(ts_values, default=0),
        "max_ts": max(ts_values, default=0),
        "physical_sha256": file_sha256(p),
        "semantic_sha256": jsonl_semantic_hash(rows),
        "bytes": stat.st_size,
        "mtime": stat.st_mtime,
        "mtime_ns": int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000))),
        "sidecar_path": str(identity_sidecar_path(p).resolve()),
        "sidecar_used": False,
    }


def _gzip_bytes(data: bytes) -> bytes:
    out = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=out, mtime=0) as gz:
        gz.write(data)
    return out.getvalue()


def write_canonical_jsonl_gz(
    path: str | os.PathLike[str],
    rows: Iterable[dict[str, Any]],
    *,
    sort_rows: bool = False,
) -> dict[str, Any]:
    p = Path(path)
    row_list = list(rows)
    if sort_rows:
        row_list.sort(key=lambda row: (
            str(row.get("day") or ""),
            int(row.get("ts") or 0),
            str(row.get("ticker") or ""),
            str(row.get("opportunity_id") or ""),
        ))
    payload = b"".join(_canonical_json_line(row) for row in row_list)
    gz_payload = _gzip_bytes(payload)
    new_hash = hashlib.sha256(gz_payload).hexdigest()
    existing_hash = file_sha256(p)
    replaced = False
    skipped_identical = existing_hash == new_hash
    if not skipped_identical:
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_name(f"{p.name}.{os.getpid()}.{int(time.time() * 1000)}.tmp")
        tmp.write_bytes(gz_payload)
        os.replace(tmp, p)
        replaced = True
    identity = jsonl_gz_identity(p)
    identity.update({
        "replaced": replaced,
        "skipped_identical": skipped_identical,
        "canonical_write": True,
    })
    write_identity_sidecar(p, identity)
    return identity


def write_json(path: str | os.PathLike[str], payload: Any, *, indent: int | None = 2) -> str:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=indent, sort_keys=True, default=str)
    if indent is not None:
        text += "\n"
    tmp = p.with_name(f"{p.name}.{os.getpid()}.{int(time.time() * 1000)}.tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, p)
    return str(p.resolve())


def write_canonical_json_gz(path: str | os.PathLike[str], payload: Any) -> dict[str, Any]:
    p = Path(path)
    gz_payload = _gzip_bytes(stable_json_bytes(payload))
    new_hash = hashlib.sha256(gz_payload).hexdigest()
    existing_hash = file_sha256(p)
    replaced = False
    skipped_identical = existing_hash == new_hash
    if not skipped_identical:
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_name(f"{p.name}.{os.getpid()}.{int(time.time() * 1000)}.tmp")
        tmp.write_bytes(gz_payload)
        os.replace(tmp, p)
        replaced = True
    return {
        "schema_version": SCHEMA_VERSION,
        "path": str(p.resolve()),
        "exists": p.exists(),
        "physical_sha256": file_sha256(p),
        "semantic_sha256": stable_json_hash(payload),
        "bytes": p.stat().st_size if p.exists() else 0,
        "mtime": p.stat().st_mtime if p.exists() else None,
        "replaced": replaced,
        "skipped_identical": skipped_identical,
        "canonical_write": True,
    }


def array_payload_hash(arrays: dict[str, Any]) -> str:
    h = hashlib.sha256()
    for name in sorted(arrays):
        arr = np.asarray(arrays[name])
        h.update(name.encode("utf-8"))
        h.update(str(arr.dtype).encode("ascii"))
        h.update(str(tuple(arr.shape)).encode("ascii"))
        h.update(np.ascontiguousarray(arr).tobytes())
    return h.hexdigest()


def _npy_bytes(array: Any) -> bytes:
    out = io.BytesIO()
    np.save(out, np.asarray(array), allow_pickle=False)
    return out.getvalue()


def write_deterministic_npz(path: str | os.PathLike[str], arrays: dict[str, Any]) -> dict[str, Any]:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(f"{p.name}.{os.getpid()}.{int(time.time() * 1000)}.tmp")
    with zipfile.ZipFile(tmp, mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for name in sorted(arrays):
            info = zipfile.ZipInfo(f"{name}.npy", date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o600 << 16
            zf.writestr(info, _npy_bytes(arrays[name]))
    new_hash = file_sha256(tmp)
    old_hash = file_sha256(p)
    replaced = old_hash != new_hash
    if replaced:
        os.replace(tmp, p)
    else:
        tmp.unlink(missing_ok=True)
    return {
        "schema_version": SCHEMA_VERSION,
        "path": str(p.resolve()),
        "physical_sha256": file_sha256(p),
        "array_payload_sha256": array_payload_hash(arrays),
        "bytes": p.stat().st_size if p.exists() else 0,
        "mtime": p.stat().st_mtime if p.exists() else None,
        "replaced": replaced,
        "skipped_identical": not replaced,
        "deterministic_npz": True,
    }


def copy_if_exists(src: str | os.PathLike[str], dst: str | os.PathLike[str]) -> bool:
    src_path = Path(src)
    if not src_path.exists():
        return False
    dst_path = Path(dst)
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    if file_sha256(src_path) == file_sha256(dst_path):
        return False
    shutil.copy2(src_path, dst_path)
    return True


def promote_file(staged: str | os.PathLike[str], final: str | os.PathLike[str]) -> dict[str, Any]:
    staged_path = Path(staged)
    final_path = Path(final)
    if not staged_path.exists():
        return {"promoted": False, "reason": "staged_file_missing", "path": str(final_path)}
    final_path.parent.mkdir(parents=True, exist_ok=True)
    staged_hash = file_sha256(staged_path)
    final_hash = file_sha256(final_path)
    if staged_hash == final_hash:
        return {
            "promoted": False,
            "reason": "identical",
            "path": str(final_path.resolve()),
            "sha256": final_hash,
        }
    tmp = final_path.with_name(f"{final_path.name}.{os.getpid()}.{int(time.time() * 1000)}.promote")
    shutil.copy2(staged_path, tmp)
    os.replace(tmp, final_path)
    return {
        "promoted": True,
        "reason": "replaced",
        "path": str(final_path.resolve()),
        "old_sha256": final_hash,
        "sha256": staged_hash,
    }


def promote_tree(staged_dir: str | os.PathLike[str], final_dir: str | os.PathLike[str]) -> dict[str, Any]:
    staged = Path(staged_dir)
    final = Path(final_dir)
    if not staged.exists():
        return {"promoted": False, "reason": "staged_tree_missing", "path": str(final)}
    final.parent.mkdir(parents=True, exist_ok=True)
    backup = final.with_name(f"{final.name}.backup.{os.getpid()}.{int(time.time() * 1000)}")
    moved_existing = False
    try:
        if final.exists():
            os.replace(final, backup)
            moved_existing = True
        os.replace(staged, final)
        if moved_existing and backup.exists():
            shutil.rmtree(backup)
        return {"promoted": True, "path": str(final.resolve()), "backup_removed": moved_existing}
    except Exception:
        if final.exists() and not staged.exists():
            os.replace(final, staged)
        if moved_existing and backup.exists() and not final.exists():
            os.replace(backup, final)
        raise
