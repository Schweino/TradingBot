"""
Smoke test for alpaca_trading.py. PAPER ENVIRONMENT ONLY.

Run order:
  1. Read-only checks (account, clock, asset info) — always safe.
  2. Opt-in order cycle (set RUN_ORDER_TEST=1 env var):
     - Submit a $20 bracket-long on MARA at current price
     - Poll order status for 10s
     - Close the position
     - Verify flat

Usage:
    python test_alpaca_trading.py              # read-only
    RUN_ORDER_TEST=1 python test_alpaca_trading.py   # full cycle

Safety: Always uses `paper=True`. Will refuse to run if keys accidentally
resolve to live (the module itself blocks that; this script doubles up).
"""
from __future__ import annotations
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from alpaca_trading import AlpacaTrader, alloc_to_qty, AlpacaTradingError


def section(title):
    print(f'\n{"=" * 60}\n{title}\n{"=" * 60}')


def main():
    t = AlpacaTrader.from_env(paper=True)
    assert t.paper, 'refusing to run test on live env'
    print(f'base: {t.base}')

    section('1. Account')
    acct = t.get_account()
    for k in ('status', 'currency', 'cash', 'equity', 'buying_power',
              'pattern_day_trader', 'shorting_enabled',
              'trading_blocked', 'account_blocked'):
        print(f'  {k:<25}: {acct.get(k)}')

    section('2. Market clock')
    clk = t.get_clock()
    for k in ('is_open', 'timestamp', 'next_open', 'next_close'):
        print(f'  {k:<25}: {clk.get(k)}')

    section('3. Asset info (CLSK, MARA)')
    for sym in ['CLSK', 'MARA']:
        a = t.get_asset(sym)
        print(f'  {sym}: tradable={a.get("tradable")}  '
              f'shortable={a.get("shortable")}  '
              f'fractionable={a.get("fractionable")}  '
              f'easy_to_borrow={a.get("easy_to_borrow")}')

    section('4. Current positions & open orders')
    pos = t.list_positions()
    print(f'  positions: {len(pos)}')
    for p in pos:
        print(f'    {p.get("symbol")} qty={p.get("qty")} '
              f'side={p.get("side")} avg={p.get("avg_entry_price")} '
              f'mark={p.get("current_price")} upl=${p.get("unrealized_pl")}')
    orders = t.list_orders(status='open')
    print(f'  open orders: {len(orders)}')
    for o in orders:
        print(f'    {o.get("symbol")} {o.get("side")} {o.get("qty")} '
              f'{o.get("type")} {o.get("order_class")} '
              f'status={o.get("status")}')

    if os.getenv('RUN_ORDER_TEST') != '1':
        section('DONE (read-only)')
        print('Set RUN_ORDER_TEST=1 to exercise the order lifecycle.')
        return

    # ── Opt-in: live order cycle on paper ──────────────────────────────
    section('5. Submit bracket order (PAPER)')
    if not clk.get('is_open'):
        print('  market closed — bracket order will queue; submitting anyway.')

    symbol = 'MARA'
    # Get a recent mark from position snapshot, or fall back to last-trade REST
    asset = t.get_asset(symbol)
    if not asset.get('tradable'):
        print(f'  {symbol} not tradable, bailing.')
        return

    # Pull a recent price from the snapshot endpoint (trading API hosts /v2/stocks/{sym}/snapshot)
    import requests
    snap_url = f'https://data.alpaca.markets/v2/stocks/{symbol}/snapshot'
    r = requests.get(snap_url, headers=t.headers, timeout=10)
    mark = r.json().get('latestTrade', {}).get('p') or \
           r.json().get('minuteBar', {}).get('c')
    if not mark:
        print('  could not fetch mark, bailing.')
        return
    print(f'  {symbol} mark ≈ ${mark:.2f}')

    # Tiny size — $20 alloc
    qty = alloc_to_qty(20.0, mark)
    if qty < 1:
        print(f'  $20 alloc cannot afford 1 share at ${mark:.2f}, bailing.')
        return
    tp = round(mark * 1.01, 2)   # +1%
    sl = round(mark * 0.95, 2)   # -5%
    print(f'  submitting BUY {qty} {symbol}  TP=${tp}  SL=${sl}')
    order = t.submit_bracket_order(symbol, qty=qty, side='buy',
                                   tp_price=tp, sl_price=sl)
    oid = order['id']
    print(f'  order id: {oid}  status: {order.get("status")}')

    section('6. Poll order status (10s)')
    for i in range(10):
        time.sleep(1)
        o = t.get_order(oid, nested=True)
        legs = o.get('legs') or []
        print(f'  t+{i+1}s  parent={o.get("status")}  '
              f'filled_qty={o.get("filled_qty")}  legs={len(legs)}')
        if o.get('status') in ('filled', 'canceled', 'rejected', 'expired'):
            break

    section('7. Close position')
    pos = t.get_position(symbol)
    if pos:
        print(f'  closing {symbol} qty={pos.get("qty")}')
        try:
            close_res = t.close_position(symbol)
            print(f'  close submitted: {close_res.get("id")}')
        except AlpacaTradingError as e:
            print(f'  close error: {e}')
    else:
        print(f'  no open {symbol} position (bracket may not have filled yet)')
        # Cancel the parent bracket if still open
        try: t.cancel_order(oid); print(f'  canceled parent order {oid}')
        except AlpacaTradingError: pass

    section('8. Final state')
    print(f'  positions: {len(t.list_positions())}')
    print(f'  open orders: {len(t.list_orders(status="open"))}')


if __name__ == '__main__':
    main()
