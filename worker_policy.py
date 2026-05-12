from __future__ import annotations

import os
from typing import Any


DEFAULT_MAX_WORKERS = 6
WORKER_ENV_VAR = 'CLAUDE_MAX_WORKERS'


def _as_positive_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        return int(default)
    return parsed if parsed > 0 else int(default)


def configured_max_workers() -> int:
    """Return the project-wide worker ceiling.

    The environment variable can lower the cap for a fragile session, but it
    cannot raise it above DEFAULT_MAX_WORKERS. Raise DEFAULT_MAX_WORKERS when
    the machine upgrade is complete.
    """
    requested = _as_positive_int(os.environ.get(WORKER_ENV_VAR), DEFAULT_MAX_WORKERS)
    return max(1, min(DEFAULT_MAX_WORKERS, requested))


def clamp_workers(requested: Any = None, available: Any = None) -> int:
    workers = _as_positive_int(requested, configured_max_workers())
    workers = min(workers, configured_max_workers())
    if available is not None:
        workers = min(workers, _as_positive_int(available, workers))
    return max(1, workers)


def describe_policy() -> dict:
    return {
        'default_max_workers': DEFAULT_MAX_WORKERS,
        'configured_max_workers': configured_max_workers(),
        'env_var': WORKER_ENV_VAR,
        'env_value': os.environ.get(WORKER_ENV_VAR),
        'note': (
            'CLAUDE_MAX_WORKERS may lower the session cap. It cannot raise the '
            'cap above DEFAULT_MAX_WORKERS until the project default changes.'
        ),
    }
