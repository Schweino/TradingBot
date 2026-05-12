from __future__ import annotations

from pathlib import Path

import conftest


def test_pytest_scratch_cleanup_removes_workspace_and_runtime_dirs():
    created = [
        conftest.WORKSPACE_ROOT / "pytest-cache-files-cleanup-test",
        conftest.WORKSPACE_ROOT / "runtime" / "pytest-cache-files-cleanup-test",
    ]
    for path in created:
        path.mkdir(parents=True, exist_ok=True)
        (path / "scratch.txt").write_text("temporary", encoding="utf-8")

    conftest._remove_pytest_scratch_dirs()

    for path in created:
        assert not path.exists()
