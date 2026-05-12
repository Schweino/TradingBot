Research tools
==============

These scripts are intentionally outside the live engine path. They are allowed
to generate candidate profiles, run Step 2 sweeps, and write artifacts under
``postmortem/backtests``. They should not be imported by ``mock_trader.py`` or
called by market-day automation unless a script is explicitly promoted into the
automation flow.

Root-level entry points are kept in place for backward-compatible commands, but
this manifest is the boundary: live-critical code is ``mock_trader.py``,
``ws_scalp.py``, ``trading_config.json``, the Step 2 parity/execution contract
modules, and ``automation_ops.py``.

Current research-only entry points:

- ``step2_adaptive_hunter.py``
- ``step2_targeted_probe.py``
- ``step2_gradient_probe.py``
- ``step2_local_hill_hunter.py``
- ``step2_ensemble_probe.py``
- ``step2_confidence_gate_probe.py``
- ``step2_pair_sweep.py``
- ``step2_feature_grid.py``
- ``step2_cooldown_sweep.py``
