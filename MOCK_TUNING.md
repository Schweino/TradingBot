# Mock Trader tuning notes — review after 4/20 first session

## Hour-13 CT skip added (wired 4/19, effective 4/20 open)
Forensic on 180-day trades at SL=5%/TP=1% showed hour 13 (1pm CT) was the
ONLY net-losing hour: 80 trades, 8.8% SL rate, -$67.09 P/L. Skipping it
in backtest: +$99.58 P/L delta (+9.96pp) with -37 fewer trades.
Tested late-entry cutoffs (14:00, 13:30, 13:00, 12:30) — all made it WORSE
than just skip h13, because hour 14 entries ARE net profitable even though
many close at session_end.

## TP widened to 1.0% (wired 4/19, effective 4/20 open)
180-day sweep at SL=5% showed TP=1.0% beats TP=0.6%: +39.56% vs +22.42%
(+$171 over 180 days, +76% improvement). Both longs and shorts peak at
TP=1.0%. Trade count drops 1,263 -> 903 but revenue/trade rises 2.4x.
Avg winner now holds ~51 min (was ~31 min) — still intraday, session-flat
still catches stragglers. WR drops 84.5% -> 77% but absolute P/L rises.

## SL widened to 5% (wired 4/19, effective 4/20 open)
180-day sweep (2025-10 → 2026-04) showed SL=5% + session-flat at 15:00 CT
beats the 0.4% baseline: +22.42% vs +14.20%. With 5% SL the 15:00 daily
force-flat becomes the effective stop (catches 139 positions vs. 64 that
hit the literal 5% SL). WR jumps 42% → 85%. Tradeoff: worst intraday
adverse excursion on a winner grows from -2.01% to -7.78% — expect to
watch trades sit down 4-7% during the day more often before resolving.

## V2 is LIVE (wired 4/19, effective 4/20 open)
Three surgical changes vs. original baseline:
1. **Buy-pressure INVERTED** in `ws_scalp.detect_signal` — flow_30s/120s now fades extremes (mean-reversion), not continues them.
2. **EMA stack alignment gate** — no LONG unless `ema_stack=='bull'`, no SHORT unless `ema_stack=='bear'`. No counter-trend entries.
3. **Hour-9 CT skip** in `mock_trader._on_signal` — 9:00-9:59 CT is a proven dead zone in Feb backtest; skip all entries.

Feb 2026 backtest: Baseline -1.40% → V2 +2.26% (shorts drove it; LONG side still barely positive but no longer bleeding).

## Design intent
- **High trade count is good.** Target 10-40 trades/day across CLSK+MARA combined.
- Once SL or TP hits → exit is a green light to immediately search for the next setup.
- Entries follow the model — **do not adjust SL/TP mid-trade**.

## Current knobs (top of `mock_trader.py`)
| Constant         | Value    | Meaning                                          |
| ---------------- | -------- | ------------------------------------------------ |
| `TRADE_SIZE_PCT` | 0.25     | 25% of balance per trade                         |
| `SL_PCT`         | 0.05     | 5.00% stop-loss (wide — session-flat is real stop)|
| `TP_PCT`         | 0.010    | 1.00% take-profit                                |
| `MIN_CONVICTION` | MED+HIGH | Entry requires score ≥ 5 from scalp signal       |
| `MIN_BALANCE`    | 50       | Stop opening new trades below $50                |

## Current engine knobs (in `ws_scalp.py`)
- Signal cooldown: **30s per (ticker, direction)** — reduced from 60s on 4/19 to support higher trade frequency.
- On position close, `mock_trader._close_position` now clears that ticker's cooldown so the next qualifying signal fires immediately.

## Things to eyeball after Monday's session
1. **Win rate vs. R:R** — need >40% wins for 1:1.5 R:R to be profitable.
   - If wins <35%: tighten entries (raise MIN_CONVICTION to HIGH-only, or raise the detect_signal score bar in `ws_scalp.py`).
   - If wins >55% but too few trades: loosen entries (try MIN_CONVICTION = LOW+MED+HIGH, or reduce conviction threshold in detect_signal).
2. **SL stop-out pattern** — if most losers are hitting SL within 10-20s of entry, SL is too tight relative to 1s noise. Try SL_PCT = 0.005 (0.5%) + TP_PCT = 0.0075 (0.75%) to keep R:R.
3. **Time-stop frequency** — if `reason: time_stop` dominates, price isn't moving enough in 5 min → either bump MAX_HOLD_SEC to 600 or the signal is too weak; raise MIN_CONVICTION.
4. **CLSK vs MARA skew** — if one ticker is all the trades, the signal detector may be biased to that volatility profile. Note but don't immediately "fix" — could just reflect reality.
5. **Trades during chop (no real trend)** — flow/EMA signals fire on both sides in a 5-min range. Mitigation: require `vwap_dist_sigma` > 0.5 absolute to enter (i.e. meaningful distance from VWAP).

## Feb 2026 backtest (1-min resolution, 20 trading days)
Time stops **removed** per user rule — exits only on SL, TP, or 15:00 CT force-flat.

| Metric        | With time stops | No time stops (current) |
| ------------- | --------------- | ----------------------- |
| Trades        | 1,684           | 1,455                   |
| Win rate      | 44.5%           | 40.4%                   |
| End balance   | $988.54         | **$1,013.96**           |
| P/L           | -$11.46 (-1.15%)| **+$13.96 (+1.40%)**    |
| MARA          | +$16.96         | +$36.92                 |
| CLSK          | -$28.42         | -$22.96                 |
| Exits (TP/SL/other) | 401/748/535 | 582/861/12        |

Takeaway: removing time stops flipped the result positive. At 40.4% WR × 1:1.5 R:R
the expected edge is ~+1% per trade, and it's realizing. MARA is the engine;
CLSK still drags.

- DO NOT re-tune based on backtest alone — 1-min vs 1-sec resolution gap is
  large. Let Monday's live 1-sec data be the first real signal.
- Candidates to watch after 4/20:
  (a) Only enter when `|vwap_dist_sigma| >= 0.5` (avoid chop)
  (b) Require `vol_z_60s >= 1.0` to enter (avoid dead-tape trades)
  (c) Investigate CLSK-specific underperformance vs MARA
  (d) Reconsider MIN_CONVICTION if live produces too few trades

## Things explicitly NOT to change
- Don't adjust SL/TP after entry (user requirement).
- Don't vary position size — 25% flat regardless of conviction.
- Don't skip MEDIUM signals automatically during "bad" market conditions; let the data tell us.
