from __future__ import annotations

import os
import shutil
import stat
from pathlib import Path


WORKSPACE_ROOT = Path(__file__).resolve().parent
PYTEST_SCRATCH_PREFIX = "pytest-cache-files-"
PYTEST_SCRATCH_ROOTS = (
    WORKSPACE_ROOT,
    WORKSPACE_ROOT / "runtime",
)
PYTEST_SCRATCH_QUARANTINE = WORKSPACE_ROOT / "okay_to_delete" / "pytest_scratch"


def _make_writable(path: str) -> None:
    try:
        os.chmod(path, stat.S_IWRITE | stat.S_IREAD)
    except OSError:
        pass


def _remove_pytest_scratch_dirs() -> None:
    for root in PYTEST_SCRATCH_ROOTS:
        if not root.exists():
            continue
        for path in root.glob(f"{PYTEST_SCRATCH_PREFIX}*"):
            if not path.is_dir():
                continue
            try:
                shutil.rmtree(path, onerror=lambda func, target, exc_info: (_make_writable(target), func(target)))
            except OSError:
                _quarantine_pytest_scratch_dir(path)


def _quarantine_pytest_scratch_dir(path: Path) -> None:
    try:
        PYTEST_SCRATCH_QUARANTINE.mkdir(parents=True, exist_ok=True)
        dest = PYTEST_SCRATCH_QUARANTINE / path.name
        counter = 1
        while dest.exists():
            counter += 1
            dest = PYTEST_SCRATCH_QUARANTINE / f"{path.name}_{counter}"
        shutil.move(str(path), str(dest))
    except OSError:
        # If Windows still has a handle or ACL lock open, the next test teardown/session finish will retry.
        pass


def pytest_runtest_teardown(item, nextitem) -> None:  # noqa: ANN001
    _remove_pytest_scratch_dirs()


def pytest_sessionfinish(session, exitstatus) -> None:  # noqa: ANN001
    _remove_pytest_scratch_dirs()
