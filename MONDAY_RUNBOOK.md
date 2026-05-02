# Monday Trading Runbook

This bot trades the paper Alpaca account for `CLSK`, `MARA`, and `RIOT` using `ws_scalp` only. Live rule changes are manual. The promotion queue can recommend candidates, but it must not update `trading_config.json` automatically.

## Pre-Market

Run this before the open:

```powershell
python ops.py pre-open
```

Required green checks:

- Flask `/mock/status` reachable.
- Broker flat for watched tickers.
- No watched open orders.
- No local positions or pending entries.
- No broker exposure block.
- Kill switch disabled.
- Alpaca equity visible.
- `flatten_cutoff_ct` is `14:55`.
- Core modules compile.
- Readiness artifacts are written.

Key files:

- `postmortem/pre_market_checklist_YYYY-MM-DD.json`
- `postmortem/config_freeze_YYYY-MM-DD_pre_open.json`
- `postmortem/known_risks_YYYY-MM-DD.json`
- `postmortem/monday_command_menu_YYYY-MM-DD.json`
- `MONDAY_READY.txt`
- `MONDAY_COMMAND_MENU.txt`
- `postmortem/broker_safety_snapshots/broker_safety_YYYY-MM-DD_weekend_readiness_HHMMSS.json`
- `postmortem/monday_live_review_YYYY-MM-DD.json`
- `postmortem/why_no_trade_YYYY-MM-DD.json`
- `postmortem/market_open_monitor_YYYY-MM-DD.json`
- `postmortem/trade_alerts_YYYY-MM-DD.json`
- `postmortem/DAILY_REVIEW_START_HERE_YYYY-MM-DD.json`
- `postmortem/MONDAY_REVIEW_START_HERE_YYYY-MM-DD.json`
- `postmortem/notes_YYYY-MM-DD.json`

## During Market

Use compact status first, raw logs second:

- `/mock/status` for positions, P&L, exposure block, kill switch, and freshness.
- `postmortem/monday_live_review_YYYY-MM-DD.json` for a compact intraday read.
- `postmortem/market_open_monitor_YYYY-MM-DD.json` for first-15-minute open health.
- `postmortem/why_no_trade_YYYY-MM-DD.json` if the bot is quiet.
- `postmortem/trade_alerts_YYYY-MM-DD.json` for passive warnings.
- `postmortem/config_change_watch_YYYY-MM-DD.json` to catch config drift after freeze.
- `audit/trade_lifecycle_YYYY-MM-DD.jsonl` only when diagnosing a specific trade lifecycle.

To auto-refresh compact intraday artifacts:

```powershell
python ops.py monitor
```

For the command menu:

```powershell
python monday_ops.py menu
python daily_ops.py menu
```

Around 08:35 CT, run:

```powershell
python ops.py post-open
```

Do not change live rules intraday unless there is a broker/process safety issue. Strategy changes should wait for postmortem evidence.

## If Something Looks Wrong

1. Check `/mock/status`.
2. If broker exposure is blocked, do not restart blindly.
3. Run:

```powershell
python ops.py status
```

4. If positions are open, verify they are broker-matched and protected before taking manual action.
5. If manual stop is needed, use the app stop path so broker-confirmed close verification runs.

## Post-Market

Run:

```powershell
python ops.py post-close
```

Review these first:

- `postmortem/review_index_YYYY-MM-DD.json`
- `postmortem/DAILY_REVIEW_START_HERE_YYYY-MM-DD.json`
- `postmortem/MONDAY_REVIEW_START_HERE_YYYY-MM-DD.json`
- `postmortem/monday_review_packet_YYYY-MM-DD.json`
- `postmortem/multi_day_scorecard_YYYY-MM-DD_10d.json`
- `postmortem/market_regime_day_YYYY-MM-DD.json`
- `postmortem/edge_quality_activity_YYYY-MM-DD_10d.json`
- `postmortem/order_latency_YYYY-MM-DD.json`
- `postmortem/promotion_review_YYYY-MM-DD.json`
- `postmortem/loser_clusters_YYYY-MM-DD.json`
- `postmortem/strategy_operations_split_YYYY-MM-DD.json`
- `postmortem/postmarket_artifact_validation_YYYY-MM-DD.json`
- `postmortem/change_impact_ledger.json`
- `postmortem/promotion_queue.json`

## Promotion Discipline

Promotion candidates must have:

- Multiple evidence days.
- Enough matching trades to matter.
- Positive estimated delta versus actual behavior.
- Acceptable winner damage.
- A clear explanation of what loser pattern it fixes.

If a candidate is eligible, update `trading_config.json` manually and record the reason in `postmortem/config_changes_YYYY-MM-DD.json`. No automatic promotion.

## Weekend Cleanup

To see duplicate generated snapshots/checkpoints that can be archived:

```powershell
python monday_ops.py archive-plan
python daily_ops.py archive-plan
```

This is dry-run by default and never moves core postmortems, audit logs, config files, or reports.

Daily helper aliases:

```powershell
python ops.py status
python ops.py intraday
python ops.py pre-flat
python ops.py post-close
python daily_ops.py setup
python promotion_review.py
python schedule_helpers.py
```
