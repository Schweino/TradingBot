# Review Checklist

Use this for fast future reviews without rereading the whole folder.

## Pre-Market

- Confirm exactly one `pythonw app.py` process is running.
- Confirm `/mock/status` returns `running: true`.
- Confirm latest `mock_trader.log` boot line says `signal_source=ws_scalp`.
- Confirm no open internal positions unless expected.
- Run `python smoke_check.py --pre-market`.
- Confirm `broker_exposure_block` is clear and Alpaca reports no watched-symbol positions.
- Confirm `kill_switch` is disabled unless intentionally pausing entries.
- Confirm `broker_api_degraded` is clear.
- Confirm Alpaca orphan reconcile did not leave unmanaged positions.

## Signal And Entry

- Review `ws_scalp.detect_signal` scoring changes.
- Review BTC conflict veto in `mock_trader.py`.
- Confirm `trading_config.json` matches intended tickers, SL/TP, betas, and trading window.
- Confirm `pending_entries` is empty before open.
- Review the latest `audit/trade_lifecycle_YYYY-MM-DD.jsonl` tail for duplicate `signal_seen`, missing `entry_submitted`, or refused starts.

## Exit And Risk

- Confirm `COND_STOP_MIN` and per-ticker SL/TP are intended.
- Confirm session hard-flat still fires at 14:55 CT.
- Review any `cond_time_stop`, `stop_loss`, or manual-stop entries from the prior day.
- Confirm local closes include broker fill metadata when Alpaca exit fills are available.
- Review `entry_fill_timeout_cancelled` events if any parent entry orders failed to fill promptly.

## Daily Postmortem

- Confirm report uses Chicago dates.
- Confirm forward returns are anchored to target timestamps.
- Confirm `hypotheses.json` does not increment `evidence_days` on same-day reruns.
- Compare loser themes against skipped-signal opportunities, especially filters with positive +5m/+15m forward returns.
- Confirm skipped-signal forward returns came from timestamped bars, not live last-price snapshots.
- Review decision-quality labels on losers: valid decision/bad outcome, bad entry context, bad execution context, bad exit.
- Review counterfactual rules: losers avoided, winners sacrificed, and net estimated impact.
- Check daily verdict, data-quality score, confidence labels, and pre-entry actionability before trusting any recommendation.
- Separate today-only evidence from rolling multi-day evidence.
- Compare missed winners vs taken losers to see whether filters reject better setups than the bot accepts.
- Review `postmortem/candidate_rules_YYYY-MM-DD.json` for targeted backtest ideas.
- Add `postmortem/notes_YYYY-MM-DD.json` when human market context or known catalysts matter.
- Read `**STRONGLY CONSIDER THE BELOW CHANGES**` only as advisory, not automatic.

## End Of Day

- Run `python eod_integrity_check.py` after postmortem generation.
- Verify Alpaca watched tickers are flat and open orders are clear.
- Verify postmortem JSON/TXT and daily audit file expectations.

## Token-Saving Scope

Default review files:

- `SYSTEM_MAP.md`
- `REVIEW_CHECKLIST.md`
- `trading_config.json`
- `mock_trader.py`
- `ws_scalp.py`
- `daily_postmortem.py`
- `postmortem_hypotheses.py`
- latest `mock_trader_state.json` summary
- latest `audit/trade_lifecycle_YYYY-MM-DD.jsonl` tail
- latest `postmortem/skipped_signals/skipped_signals_YYYY-MM-DD.jsonl` summary
