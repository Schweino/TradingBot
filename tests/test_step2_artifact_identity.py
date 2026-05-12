import gzip
import json
import shutil
import uuid
import unittest
from contextlib import contextmanager
from pathlib import Path

import numpy as np

import step2_artifact_identity as identity


WORKSPACE_TMP = Path(__file__).resolve().parents[1] / ".test_tmp"


@contextmanager
def _tempdir():
    WORKSPACE_TMP.mkdir(exist_ok=True)
    path = WORKSPACE_TMP / f"case_{uuid.uuid4().hex}"
    path.mkdir()
    try:
        yield str(path)
    finally:
        shutil.rmtree(path, ignore_errors=True)


class Step2ArtifactIdentityTests(unittest.TestCase):
    def test_canonical_jsonl_gz_rewrite_is_byte_stable(self):
        with _tempdir() as tmp:
            path = Path(tmp) / "day.jsonl.gz"
            rows = [{"ts": 2, "ticker": "MARA", "b": 1}, {"ts": 1, "ticker": "CLSK", "a": 2}]

            first = identity.write_canonical_jsonl_gz(path, rows, sort_rows=True)
            second = identity.write_canonical_jsonl_gz(path, rows, sort_rows=True)

            self.assertTrue(first["replaced"])
            self.assertTrue(second["skipped_identical"])
            self.assertEqual(first["physical_sha256"], second["physical_sha256"])
            self.assertEqual(first["semantic_sha256"], second["semantic_sha256"])
            self.assertTrue(Path(first["sidecar_path"]).exists())

            from_sidecar = identity.jsonl_gz_identity(path)
            self.assertTrue(from_sidecar["sidecar_used"])
            self.assertEqual(first["semantic_sha256"], from_sidecar["semantic_sha256"])

    def test_semantic_hash_survives_noncanonical_gzip_bytes(self):
        with _tempdir() as tmp:
            path = Path(tmp) / "day.jsonl.gz"
            rows = [{"ts": 1, "ticker": "CLSK", "nested": {"b": 2, "a": 1}}]
            canonical = identity.write_canonical_jsonl_gz(path, rows)

            with gzip.open(path, "wt", encoding="utf-8") as f:
                for row in rows:
                    f.write(json.dumps(row, sort_keys=False, indent=None) + "\n")

            changed = identity.jsonl_gz_identity(path)
            self.assertNotEqual(canonical["physical_sha256"], changed["physical_sha256"])
            self.assertEqual(canonical["semantic_sha256"], changed["semantic_sha256"])

    def test_deterministic_npz_tracks_payload_hash(self):
        with _tempdir() as tmp:
            path = Path(tmp) / "arrays.npz"
            arrays = {"b": np.asarray([2, 3], dtype=np.int64), "a": np.asarray([1.25], dtype=np.float64)}

            first = identity.write_deterministic_npz(path, arrays)
            second = identity.write_deterministic_npz(path, arrays)

            self.assertTrue(first["replaced"])
            self.assertTrue(second["skipped_identical"])
            self.assertEqual(first["physical_sha256"], second["physical_sha256"])
            self.assertEqual(first["array_payload_sha256"], second["array_payload_sha256"])


if __name__ == "__main__":
    unittest.main()
