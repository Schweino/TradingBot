"""
Side-by-side backtest of 4 rule variants on Feb 2026 data:
  - Baseline: current live rules (buy-press continuation, score>=4 medium)
  - V1: invert buy-pressure (fade strong BP, buy weak BP)
  - V2: V1 + skip hour 9 CT + require EMA stack alignment with side
  - V3: V2 + shorts only

All same SL/TP/session-end/no-time-stop rules. Fetches data once, runs
4 independent simulations, prints a comparison table.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from backtest_feb import (
    alpaca_stock_bars, alpaca_crypto_bars,
    compute_indicators, compute_btc_ind,
    utc_date_iso, in_session,
    START_DATE, END_DATE, TICKERS, BTC_SYM,
    START_BALANCE, TRADE_SIZE_PCT, SL_PCT, TP_PCT,
    MIN_CONVICTION, MIN_BALANCE,
)

# ── Variant-aware signal detector ───────────────────────────────────────
def detect_signal(ticker, ind, btc, cfg):
    if ind is None:
        return None
    score = 0

    st = ind.get('ema_stack')
    if st == 'bull':   score += 2
    elif st == 'bear': score -= 2

    vd = ind.get('vwap_dist')
    if vd is not None:
        score += 1 if vd > 0 else -1

    bp = ind.get('buy_pressure')
    if bp is not None:
        if cfg['invert_bp']:
            # Mean-reversion: fade extremes
            if bp >= 0.65:   score -= 2
            elif bp <= 0.35: score += 2
        else:
            # Continuation (current live logic)
            if bp >= 0.65:   score += 2
            elif bp <= 0.35: score -= 2

    m1, m5 = ind.get('mom_1'), ind.get('mom_5')
    if m1 is not None and m5 is not None:
        if m1 > 0.10 and m5 > 0.10:     score += 1
        elif m1 < -0.10 and m5 < -0.10: score -= 1

    if btc:
        bs, bm = btc.get('stack'), btc.get('mom_5')
        if bs == 'bull' and bm and bm > 0:
            if score > 0:   score += 1
            elif score < 0: score += 1
        elif bs == 'bear' and bm and bm < 0:
            if score < 0:   score -= 1
            elif score > 0: score -= 1

    # RSI exhaustion penalty: don't chase into overbought LONGs or oversold SHORTs
    if cfg.get('use_rsi_penalty'):
        rsi = ind.get('rsi')
        if rsi is not None:
            if score > 0 and rsi > 70:   score -= 2  # overbought — LONG may be exhausted
            elif score < 0 and rsi < 30: score += 2  # oversold  — SHORT may be exhausted

    # OBV: cumulative volume flow confirms or contradicts direction
    if cfg.get('use_obv'):
        obv_dir = ind.get('obv_trend')
        if obv_dir is not None and obv_dir != 0:
            if (score > 0 and obv_dir > 0) or (score < 0 and obv_dir < 0):
                score += 1  # volume flow confirms direction
            else:
                score -= 1  # volume flow contradicts direction

    # MACD: histogram sign confirms or contradicts momentum direction
    if cfg.get('use_macd'):
        mh = ind.get('macd_hist')
        if mh is not None:
            if (score > 0 and mh > 0) or (score < 0 and mh < 0):
                score += 1  # momentum confirms direction
            else:
                score -= 1  # momentum contradicts direction

    # ADX gate: skip trades when market is choppy (no trend)
    if cfg.get('use_adx_gate'):
        adx = ind.get('adx')
        if adx is not None and adx < 20:
            return None

    abs_s = abs(score)
    if abs_s < 4:
        return None
    conv = 'HIGH' if abs_s >= 7 else 'MEDIUM' if abs_s >= 5 else 'LOW'
    side = 'LONG' if score > 0 else 'SHORT'

    # Entry filters
    if cfg['require_stack_align']:
        if side == 'LONG'  and st != 'bull': return None
        if side == 'SHORT' and st != 'bear': return None
    if cfg['shorts_only'] and side == 'LONG':
        return None

    return {'side': side, 'conviction': conv, 'score': score, 'price': ind['price']}


def open_position(tkr, sig, bar, balance):
    entry = bar['c']
    alloc = round(balance * TRADE_SIZE_PCT, 2)
    qty   = alloc / entry if entry > 0 else 0
    if qty <= 0:
        return None
    side = sig['side']
    if side == 'LONG':
        sl = round(entry * (1 - SL_PCT), 4)
        tp = round(entry * (1 + TP_PCT), 4)
    else:
        sl = round(entry * (1 + SL_PCT), 4)
        tp = round(entry * (1 - TP_PCT), 4)
    return {'ticker': tkr, 'side': side, 'entry': entry, 'entry_ts': bar['t'],
            'sl': sl, 'tp': tp, 'qty': qty, 'alloc': alloc,
            'conviction': sig['conviction']}


def check_exit_on_bar(pos, bar):
    side = pos['side']
    if side == 'LONG':
        sl_hit = bar['l'] <= pos['sl']
        tp_hit = bar['h'] >= pos['tp']
    else:
        sl_hit = bar['h'] >= pos['sl']
        tp_hit = bar['l'] <= pos['tp']
    if sl_hit:            return pos['sl'], 'stop_loss'
    if tp_hit:            return pos['tp'], 'take_profit'
    return None, None


def close_position(pos, exit_price, reason, exit_ts):
    side = pos['side']
    if side == 'LONG':
        pnl = (exit_price - pos['entry']) * pos['qty']
    else:
        pnl = (pos['entry'] - exit_price) * pos['qty']
    return {
        'ticker': pos['ticker'], 'side': side,
        'entry': round(pos['entry'], 4), 'exit': round(exit_price, 4),
        'sl': pos['sl'], 'tp': pos['tp'],
        'pnl': round(pnl, 2),
        'result': 'WIN' if pnl > 0 else ('LOSS' if pnl < 0 else 'FLAT'),
        'reason': reason, 'entry_ts': pos['entry_ts'], 'exit_ts': exit_ts,
        'conviction': pos['conviction'],
    }


def utc_ts_hour_ct(ts_ms):
    """Return hour-of-day in CT (winter UTC-6) for filtering."""
    dt = datetime.fromtimestamp(ts_ms / 1000 - 6 * 3600, tz=timezone.utc)
    return dt.hour


# ── Simulation ──────────────────────────────────────────────────────────
def run_simulation(bars_by_tkr, btc_bars, cfg):
    bar_idx = {(t, b['t']): b for t, lst in bars_by_tkr.items() for b in lst}
    btc_idx = {b['t']: b for b in btc_bars}
    all_ts  = sorted(set(b['t'] for t in TICKERS for b in bars_by_tkr[t]
                         if in_session(b['t'])))

    history   = {t: [] for t in TICKERS}
    btc_hist  = []
    positions = {}
    balance   = START_BALANCE
    trades    = []
    prev_day  = None
    skipped_hr9 = 0

    for idx, ts in enumerate(all_ts):
        day = utc_date_iso(ts)
        next_day = utc_date_iso(all_ts[idx + 1]) if idx + 1 < len(all_ts) else None

        if prev_day and day != prev_day:
            for tkr in list(positions.keys()):
                if history[tkr]:
                    last = history[tkr][-1]
                    tr = close_position(positions[tkr], last['c'],
                                        'session_end_rollover', last['t'])
                    balance = round(balance + tr['pnl'], 2)
                    trades.append(tr)
                    del positions[tkr]
            history = {t: [] for t in TICKERS}
            btc_hist = []
        prev_day = day

        bb = btc_idx.get(ts)
        if bb is not None:
            btc_hist.append(bb)

        for tkr in TICKERS:
            bar = bar_idx.get((tkr, ts))
            if bar is None:
                continue
            history[tkr].append(bar)

            # Manage position
            if tkr in positions:
                pos = positions[tkr]
                if bar['t'] > pos['entry_ts']:
                    ep, reason = check_exit_on_bar(pos, bar)
                    if ep is not None:
                        tr = close_position(pos, ep, reason, bar['t'])
                        balance = round(balance + tr['pnl'], 2)
                        trades.append(tr)
                        del positions[tkr]

            is_last_minute = (next_day != day)
            if is_last_minute and tkr in positions:
                pos = positions[tkr]
                tr = close_position(pos, bar['c'], 'session_end', bar['t'])
                balance = round(balance + tr['pnl'], 2)
                trades.append(tr)
                del positions[tkr]

            # Entry
            if tkr not in positions and not is_last_minute:
                if balance < MIN_BALANCE:
                    continue
                # Hour-9 CT filter
                if cfg['skip_hour_9']:
                    if utc_ts_hour_ct(ts) == 9:
                        skipped_hr9 += 1
                        continue
                ind     = compute_indicators(history[tkr])
                btc_ind = compute_btc_ind(btc_hist)
                sig = detect_signal(tkr, ind, btc_ind, cfg)
                if sig and sig['conviction'] in MIN_CONVICTION:
                    pos = open_position(tkr, sig, bar, balance)
                    if pos:
                        positions[tkr] = pos

    for tkr in list(positions.keys()):
        if history[tkr]:
            last = history[tkr][-1]
            tr = close_position(positions[tkr], last['c'],
                                'end_of_data', last['t'])
            balance = round(balance + tr['pnl'], 2)
            trades.append(tr)
            del positions[tkr]

    return balance, trades, skipped_hr9


# ── Main ────────────────────────────────────────────────────────────────
def summarize(name, balance, trades):
    w = sum(1 for t in trades if t['result'] == 'WIN')
    l = sum(1 for t in trades if t['result'] == 'LOSS')
    wr = (100 * w / (w + l)) if (w + l) else 0
    pnl = round(balance - START_BALANCE, 2)
    longs  = [t for t in trades if t['side'] == 'LONG']
    shorts = [t for t in trades if t['side'] == 'SHORT']
    long_pnl  = round(sum(t['pnl'] for t in longs), 2)
    short_pnl = round(sum(t['pnl'] for t in shorts), 2)
    return {
        'name':   name,
        'n':      len(trades),
        'wins':   w,
        'losses': l,
        'wr':     wr,
        'pnl':    pnl,
        'pct':    pnl / START_BALANCE * 100,
        'longs':  len(longs),
        'shorts': len(shorts),
        'long_pnl':  long_pnl,
        'short_pnl': short_pnl,
    }


def main():
    print(f'[variants] fetching {START_DATE}..{END_DATE}')
    start_iso = f'{START_DATE}T00:00:00Z'
    end_iso   = f'{END_DATE}T23:59:59Z'
    bars_by_tkr = {}
    for t in TICKERS:
        bars = alpaca_stock_bars(t, start_iso, end_iso, timeframe='1Min')
        bars.sort(key=lambda b: b['t'])
        bars_by_tkr[t] = bars
        print(f'  {t:>6}: {len(bars)} bars')
    btc_bars = alpaca_crypto_bars(BTC_SYM, start_iso, end_iso, timeframe='1Min')
    btc_bars.sort(key=lambda b: b['t'])
    print(f'  {BTC_SYM:>6}: {len(btc_bars)} bars')
    print()

    configs = [
        ('Baseline (current rules)',
         dict(invert_bp=False, skip_hour_9=False,
              require_stack_align=False, shorts_only=False)),
        ('V1: invert buy-pressure',
         dict(invert_bp=True,  skip_hour_9=False,
              require_stack_align=False, shorts_only=False)),
        ('V2: V1 + skip hr9 CT + stack-align',
         dict(invert_bp=True,  skip_hour_9=True,
              require_stack_align=True,  shorts_only=False)),
        ('V3: V2 + shorts only',
         dict(invert_bp=True,  skip_hour_9=True,
              require_stack_align=True,  shorts_only=True)),
        ('V4: V2 + RSI exhaustion penalty',
         dict(invert_bp=True,  skip_hour_9=True,
              require_stack_align=True,  shorts_only=False,
              use_rsi_penalty=True)),
        ('V5: V4 + OBV confirmation',
         dict(invert_bp=True,  skip_hour_9=True,
              require_stack_align=True,  shorts_only=False,
              use_rsi_penalty=True, use_obv=True)),
        ('V6: V4 + MACD momentum',
         dict(invert_bp=True,  skip_hour_9=True,
              require_stack_align=True,  shorts_only=False,
              use_rsi_penalty=True, use_macd=True)),
        ('V7: V4 + ADX gate (>20)',
         dict(invert_bp=True,  skip_hour_9=True,
              require_stack_align=True,  shorts_only=False,
              use_rsi_penalty=True, use_adx_gate=True)),
        ('V8: V4 + OBV + MACD + ADX',
         dict(invert_bp=True,  skip_hour_9=True,
              require_stack_align=True,  shorts_only=False,
              use_rsi_penalty=True, use_obv=True, use_macd=True,
              use_adx_gate=True)),
    ]

    results = []
    for name, cfg in configs:
        print(f'running: {name}...')
        balance, trades, skipped = run_simulation(bars_by_tkr, btc_bars, cfg)
        r = summarize(name, balance, trades)
        r['skipped_hr9'] = skipped
        results.append(r)

    print()
    print('=' * 100)
    print('Feb 2026 backtest variants')
    print('=' * 100)
    hdr = f'{"Variant":<42} {"Trades":>7} {"WR%":>6} {"P/L$":>9} {"P/L%":>7} {"Long$":>9} {"Short$":>9}'
    print(hdr); print('-' * len(hdr))
    for r in results:
        print(f'{r["name"]:<42} {r["n"]:>7} {r["wr"]:>5.1f}% '
              f'{r["pnl"]:>+8.2f} {r["pct"]:>+6.2f}% '
              f'{r["long_pnl"]:>+8.2f} {r["short_pnl"]:>+8.2f}')
    print()
    print(f'Long/Short split (trades):')
    for r in results:
        print(f'  {r["name"]:<42} longs={r["longs"]:>4}  shorts={r["shorts"]:>4}')

if __name__ == '__main__':
    main()
