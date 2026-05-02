# Live trading integration plan

**Status: module built, NOT wired into `mock_trader.py`.** This is the hand-off
doc for when we decide to flip the switch.

## What's built

- **`alpaca_trading.py`** — paper-default Alpaca Trading API v2 wrapper.
  - `AlpacaTrader.from_env(paper=True)` — reads keys from `.env`.
  - Account, clock, asset info, bracket orders, order lifecycle, positions.
  - Hard-defaults to paper. Live requires `paper=False` AND env flag
    `ALPACA_ALLOW_LIVE=1` — belt + suspenders.
- **`test_alpaca_trading.py`** — smoke test against paper env.
  - Read-only by default: `python test_alpaca_trading.py`
  - Full order cycle: `RUN_ORDER_TEST=1 python test_alpaca_trading.py`

## Env var layout (in `.env`)

Current (shared keys — used for Data API):
```
ALPACA_API_KEY=...
ALPACA_SECRET_KEY=...
```

**Recommended before going paper-live:** Alpaca paper trading uses its own
dashboard at https://app.alpaca.markets/paper/dashboard — generate a paper
key pair there and add:
```
ALPACA_PAPER_KEY=...
ALPACA_PAPER_SECRET=...
```
The module prefers these when `paper=True`. If you don't set them it falls
back to the shared keys (fine if the shared keys already have paper access).

For eventual real-money trading, separate keys (live dashboard):
```
ALPACA_LIVE_KEY=...
ALPACA_LIVE_SECRET=...
```
Plus `ALPACA_ALLOW_LIVE=1` at runtime.

## Integration into `mock_trader.py` (when ready)

The mock trader's internal state (`positions`, `balance`) has to become a
view of Alpaca's actual state — not a ledger we mirror. Key changes:

### 1. Constructor

```python
# mock_trader.py __init__
from alpaca_trading import AlpacaTrader, alloc_to_qty
self.trader = AlpacaTrader.from_env(paper=True)   # paper first
```

### 2. `_on_signal` — replace internal bookkeeping with a bracket order

```python
# replace the block that computes alloc/qty/sl/tp and writes to state
qty = alloc_to_qty(alloc, price)  # whole shares — brackets require int
if qty < 1: return

side_alpaca = 'buy' if sig['side'] == 'LONG' else 'sell'
if side_alpaca == 'sell':
    asset = self.trader.get_asset(tkr)
    if not asset.get('shortable'):
        log.info(f'{tkr} not shortable, skip SHORT signal'); return

order = self.trader.submit_bracket_order(
    symbol=tkr, qty=qty, side=side_alpaca,
    tp_price=tp, sl_price=sl,
    client_order_id=f'scalp-{tkr}-{int(time.time()*1000)}',
)
# Persist mapping: order.id → (ticker, side, qty, tp, sl, entry_ts)
self.state['positions'][tkr] = {
    'alpaca_order_id': order['id'],
    'side': sig['side'], 'qty': qty,
    'tp': tp, 'sl': sl, 'entry_ts': time.time(),
    'status': order.get('status'),   # 'new' | 'accepted' | 'filled' | ...
}
```

### 3. `_monitor_loop` — become a reconciler, not a SL/TP checker

Alpaca handles TP/SL server-side via the bracket. The monitor just needs
to reconcile state:

```python
# every few seconds:
for tkr, pos in list(self.state['positions'].items()):
    o = self.trader.get_order(pos['alpaca_order_id'], nested=True)
    if o['status'] == 'filled':
        # Entry filled; check legs for TP/SL exit
        for leg in o.get('legs', []):
            if leg['status'] == 'filled':
                # child TP or SL filled → position is closed
                self._record_close(tkr, leg, o)
                del self.state['positions'][tkr]
                break
    elif o['status'] in ('canceled', 'rejected', 'expired'):
        log.warning(f'entry {pos["alpaca_order_id"]} {o["status"]}')
        del self.state['positions'][tkr]
```

### 4. `_in_trading_window` session-flat → call `close_all_positions`

At 15:00 CT:
```python
self.trader.cancel_all_orders()
self.trader.close_all_positions(cancel_orders=False)
```

### 5. Balance / equity

Read from Alpaca directly, don't maintain:
```python
def status(self):
    acct = self.trader.get_account()
    return {
        'balance':     float(acct['cash']),
        'equity':      float(acct['equity']),
        'buying_power': float(acct['buying_power']),
        'positions':   self.trader.list_positions(),
        ...
    }
```

## Gotchas and open questions

1. **Bracket orders don't support fractional shares.** `alloc_to_qty` rounds
   down to int — at $10/share and 25% of $1000, that's 25 shares ($250),
   fine. As balance grows, granularity improves.

2. **Short entries need `shortable: True`.** Check `get_asset(symbol)` before
   submitting. Also HTB (hard-to-borrow) tickers can reject shorts even if
   marked shortable. Add a retry-as-skip handler.

3. **Extended-hours.** `time_in_force='day'` is fine for our 8:30–15:00 CT
   window. If you ever want pre/post, use `tif='day'` + `extended_hours=true`
   and switch entry to a limit (market orders not allowed after-hours).

4. **PDT rule.** Paper accounts enforce it too: >3 day-trades in 5 rolling
   days on an account below $25k triggers the flag. With 5 trades/day we
   will hit this in a week. Mitigations: (a) start with >$25k paper balance,
   (b) accept the flag on paper (it's instructive), (c) hold overnight
   some days (breaks our "force-flat at 15:00" rule).

5. **Wash-sale tracking** doesn't matter on paper, but becomes a concern
   if we ever go live with real capital. Alpaca reports via 1099-B.

6. **Rate limits.** Trading API is 200 req/min. Our `_monitor_loop` should
   poll every 5-10 seconds max, or use the Alpaca streaming API for
   order updates (`wss://paper-api.alpaca.markets/stream`) to avoid
   hammering REST.

7. **State reconciliation on restart.** If Flask restarts mid-session,
   `self.state['positions']` could disagree with Alpaca. On boot, call
   `list_positions()` + `list_orders(status='open')` and rebuild state
   from truth.

8. **Tick-size rules.** Prices >= $1 must be rounded to $0.01. `_fmt()`
   in the module handles this. Sub-dollar (penny stocks) allow $0.0001.

## Roll-out sequence

1. ✅ Build module + smoke test (done).
2. Generate paper-specific keys at app.alpaca.markets/paper/dashboard
   and add `ALPACA_PAPER_KEY` / `ALPACA_PAPER_SECRET` to `.env`.
3. Run `python test_alpaca_trading.py` — read-only. Confirm account exists
   and CLSK+MARA are tradable/shortable.
4. Run `RUN_ORDER_TEST=1 python test_alpaca_trading.py` during market
   hours. Confirm a bracket fills, position appears, close works.
5. Wire the 5 changes above into `mock_trader.py` behind a flag:
   `USE_ALPACA_EXECUTION = os.getenv('USE_ALPACA_EXECUTION') == '1'`
6. Run for 1-2 weeks paper-Alpaca-executed. Compare to internal-simulated
   mock trader running in parallel.
7. If paper results match backtest expectations, graduate to live with
   ALPACA_ALLOW_LIVE=1 and separate live keys. Start tiny.
