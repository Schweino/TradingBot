# System Map

## Live Path

- `app.py` exposes the Flask app and `/mock/status`.
- `mock_trader.py` owns paper execution, broker safety, state, exits, and trade logging.
- `ws_scalp.py` is the only signal engine. It emits live signals, near-signal logs, and BTC-aware scoring context.
- `alpaca_trading.py` is the broker adapter.
- `trading_config.json` is the strategy/config source of truth.

## Runtime Safety

- Hard flat cutoff is 14:55 CT.
- Broker exposure block prevents trading when local state and Alpaca exposure disagree.
- Manual/session/conditional closes should verify broker flatness before clearing local positions.
- `smoke_check.py --mode no-surprises` is the main readiness gate.
- `postmortem/NOW_STATUS.json` is the compact live status file to inspect first.

## Data Captured

- Closed trades live in `mock_trader_state.json` and postmortem JSON.
- Lifecycle audit logs live under `audit/trade_lifecycle_YYYY-MM-DD.jsonl`.
- Skipped signals live under `postmortem/skipped_signals/`.
- Near signals live under `postmortem/near_signals/`.
- Decision audits live under `postmortem/decision_audits/`.
- Health heartbeats live under `postmortem/health_heartbeats/`.
- Broker safety snapshots live under `postmortem/broker_safety_snapshots/`.

## Review Artifacts

Open these first:

- `postmortem/NOW_STATUS.json`
- `postmortem/DAILY_REVIEW_START_HERE_YYYY-MM-DD.json`
- `postmortem/daily_review_gate_YYYY-MM-DD.json`
- `postmortem/world_class_dashboard_YYYY-MM-DD.json`
- `postmortem/trade_alerts_YYYY-MM-DD.json`
- `postmortem/current_engine_regrade_YYYY-MM-DD.json`
- `postmortem/false_positive_negative_YYYY-MM-DD.json`
- `postmortem/rule_lifecycle_dashboard_YYYY-MM-DD.json`

## Learning Discipline

- A day can be `safe_to_learn`, `watch_only`, `unsafe_to_learn_strategy`, or `insufficient_data`.
- Candidate changes move through `proven`, `promising`, `watch_only`, `rejected`, or `insufficient_data`.
- No artifact automatically changes live strategy.
- Entry sizing is intentionally not changed.
- Replay is diagnostic only, not a fill-accurate backtest.
