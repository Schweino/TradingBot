# Review Flow

## During Market

1. Open `postmortem/NOW_STATUS.json`.
2. If state is `critical`, inspect `trade_alerts_YYYY-MM-DD.json` before anything else.
3. If there are no trades, inspect `why_no_trade_YYYY-MM-DD.json`.
4. If trades are open, inspect open trade quality in `NOW_STATUS.json` and the latest session checkpoint.
5. Do not change live rules intraday unless it is a safety issue.

## After Close

1. Run `python monday_close_packet.py` or the daily close packet for non-Monday sessions.
2. Open `DAILY_REVIEW_START_HERE_YYYY-MM-DD.json`.
3. Check `daily_review_gate_YYYY-MM-DD.json`.
4. If the gate is not `safe_to_learn`, only make operational/process fixes.
5. If the gate is `safe_to_learn`, review loser clusters, exit quality, near misses, and rule lifecycle.

## Research Order

1. `daily_review_gate_YYYY-MM-DD.json`
2. `current_engine_regrade_YYYY-MM-DD.json`
3. `false_positive_negative_YYYY-MM-DD.json`
4. `replay_confidence_YYYY-MM-DD.json`
5. `regime_specific_scorecards_YYYY-MM-DD.json`
6. `exit_quality_score_YYYY-MM-DD.json`
7. `rule_lifecycle_dashboard_YYYY-MM-DD.json`

## Promotion Rules

- `proven`: eligible for human review, never auto-promoted.
- `promising`: keep collecting and replay-testing.
- `watch_only`: interesting, not actionable.
- `rejected`: evidence is negative or winner damage is too high.
- `insufficient_data`: sample or cleanliness is too weak.

## Token-Saving Rule

Start every future review from `NOW_STATUS.json`, `DAILY_REVIEW_START_HERE_YYYY-MM-DD.json`, and `daily_review_gate_YYYY-MM-DD.json`. Avoid raw JSONL unless the gate points to a specific forensic question.
