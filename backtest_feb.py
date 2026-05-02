"""
Walk-forward backtest of the mock-trader logic on Feb 2026 data.

Honest constraints:
  - Alpaca historical API → 1-minute bars (not 1-second). Signals are
    adapted 1-min analogues of ws_scalp.py's 1-sec detectors.
  - At each decision T, only bars[0..T] are visible. Entry fills at
    bars[T].close (the price you could see). SL/TP checked only against
    bars[T+1..]. If a single bar has both SL and TP in its h/l range,
    we conservatively assume SL hit first.
  - Indicator history resets at each day's session open (no overnight
    mixing). BTC alignment uses only BTC bars with t <= current_t.
  - CLSK and MARA share a single $1,000 balance (same as live).
  - 25% allocation per trade, SL 0.40%, TP 0.60%, 5-bar time stop,
    force-flat at session end (15:00 CT).
Output: backtest_feb_trades.csv
"""
from __future__ import annotations

import csv
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from data_fetch import alpaca_stock_bars, alpaca_crypto_bars

# ── Config ──────────────────────────────────────────────────────────────
START_DATE      = '2026-02-01'
END_DATE        = '2026-02-28'
TICKERS         = ['CLSK', 'MARA']
BTC_SYM         = 'BTC/USD'
# Feb 2026 = CST (UTC-6). 8:30 CT = 14:30 UTC ; 15:00 CT = 21:00 UTC.
SESSION_START_M = 14 * 60 + 30
SESSION_END_M   = 21 * 60
START_BALANCE   = 1000.0
TRADE_SIZE_PCT  = 0.25
SL_PCT          = 0.004       # 0.40 %
TP_PCT          = 0.006       # 0.60 %
MIN_CONVICTION  = ('MEDIUM', 'HIGH')
MIN_BALANCE     = 50.0
MIN_HISTORY     = 21          # need 21 bars in session before signalling
OUT_CSV         = 'backtest_feb_trades.csv'

# ── Time helpers ────────────────────────────────────────────────────────
def utc_minute_of_day(ts_ms: int) -> int:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.hour * 60 + dt.minute

def utc_date_iso(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).date().isoformat()

def in_session(ts_ms: int) -> bool:
    m = utc_minute_of_day(ts_ms)
    return SESSION_START_M <= m < SESSION_END_M

def ct_iso(ts_ms: int) -> str:
    """Format as CT (CST, UTC-6) for the CSV — matches user's mental model."""
    dt = datetime.fromtimestamp(ts_ms / 1000 - 6 * 3600, tz=timezone.utc)
    return dt.strftime('%Y-%m-%d %H:%M')

# ── Indicator library (1-min analogues) ─────────────────────────────────
def ema(arr, span):
    if len(arr) < span:
        return None
    k = 2 / (span + 1)
    e = arr[-span]
    for v in arr[-span + 1:]:
        e = v * k + e * (1 - k)
    return e

def rsi14(arr):
    """Simple 14-period RSI (SMA-smoothed). Returns None if insufficient data."""
    if len(arr) < 15:
        return None
    deltas = [arr[i] - arr[i - 1] for i in range(len(arr) - 14, len(arr))]
    gains  = [max(0.0, d) for d in deltas]
    losses = [max(0.0, -d) for d in deltas]
    ag = sum(gains)  / 14
    al = sum(losses) / 14
    if al == 0:
        return 100.0
    return round(100 - (100 / (1 + ag / al)), 2)

def obv_trend(bars, lookback=5):
    """OBV slope direction over last `lookback` bars. Returns +1, -1, or 0."""
    if len(bars) < lookback + 1:
        return None
    obv = 0
    obv_series = []
    prev_c = bars[0]['c']
    for b in bars:
        if b['c'] > prev_c:   obv += b['v']
        elif b['c'] < prev_c: obv -= b['v']
        obv_series.append(obv)
        prev_c = b['c']
    slope = obv_series[-1] - obv_series[-lookback - 1]
    return 1 if slope > 0 else (-1 if slope < 0 else 0)


def macd_histogram(closes):
    """Returns (macd_line, histogram). None pair if < 35 bars."""
    if len(closes) < 35:
        return None, None
    # MACD line at each of the last 9 positions, then current
    macd_series = []
    for offset in range(9, -1, -1):
        sub = closes[:len(closes) - offset] if offset > 0 else closes
        e12 = ema(sub, 12)
        e26 = ema(sub, 26)
        if e12 is None or e26 is None:
            return None, None
        macd_series.append(e12 - e26)
    signal = ema(macd_series[:9], 9)
    if signal is None:
        return None, None
    ml = macd_series[-1]
    return ml, ml - signal


def adx14(bars, period=14):
    """Wilder's ADX. Returns None if < 2*period+1 bars."""
    if len(bars) < 2 * period + 1:
        return None
    trs, pdms, mdms = [], [], []
    for i in range(1, len(bars)):
        h, l, c   = bars[i]['h'],   bars[i]['l'],   bars[i]['c']
        ph, pl, pc = bars[i-1]['h'], bars[i-1]['l'], bars[i-1]['c']
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
        up, dn = h - ph, pl - l
        pdms.append(up if up > dn and up > 0 else 0.0)
        mdms.append(dn if dn > up and dn > 0 else 0.0)

    def wilder(arr):
        s = [sum(arr[:period])]
        for v in arr[period:]:
            s.append(s[-1] - s[-1] / period + v)
        return s

    atr  = wilder(trs)
    apdm = wilder(pdms)
    amdm = wilder(mdms)

    dx_series = []
    for i in range(len(atr)):
        if atr[i] == 0:
            continue
        pdi = 100 * apdm[i] / atr[i]
        mdi = 100 * amdm[i] / atr[i]
        denom = pdi + mdi
        if denom > 0:
            dx_series.append(100 * abs(pdi - mdi) / denom)

    if len(dx_series) < period:
        return None
    adx_vals = wilder(dx_series)
    return round(adx_vals[-1], 2) if adx_vals else None


def compute_indicators(bars_today):
    """Compute signal inputs from today's in-session bars up to (and
    including) the latest. Must not touch any future bar."""
    if len(bars_today) < MIN_HISTORY:
        return None
    closes = [b['c'] for b in bars_today]
    vols   = [b['v'] for b in bars_today]
    price  = closes[-1]

    ef, em, es = ema(closes, 3), ema(closes, 8), ema(closes, 21)
    stack = None
    if ef and em and es:
        if ef > em > es:   stack = 'bull'
        elif ef < em < es: stack = 'bear'
        else:              stack = 'mixed'

    # Session VWAP from today's bars
    pv = sum(((b['h'] + b['l'] + b['c']) / 3.0) * b['v'] for b in bars_today)
    vv = sum(b['v'] for b in bars_today)
    vwap = (pv / vv) if vv > 0 else None

    # σ for VWAP distance: stdev of the last 20 closes
    window = closes[-20:]
    mu = sum(window) / len(window)
    sd = (sum((x - mu) ** 2 for x in window) / len(window)) ** 0.5
    vwap_dist_sigma = ((price - vwap) / sd) if (vwap and sd > 0) else None

    def pct(n):
        if len(closes) < n + 1 or closes[-n - 1] == 0:
            return None
        return (closes[-1] - closes[-n - 1]) / closes[-n - 1] * 100
    mom_1 = pct(1)
    mom_5 = pct(5)

    # Volume z-score: latest bar vs previous 20
    if len(vols) >= 21:
        prev = vols[-21:-1]
        cur  = vols[-1]
        mm   = sum(prev) / len(prev)
        ss   = (sum((x - mm) ** 2 for x in prev) / len(prev)) ** 0.5
        vol_z = ((cur - mm) / ss) if ss > 0 else 0.0
    else:
        vol_z = None

    # Buy-pressure proxy: avg position of close within bar range, last 5 bars
    bps = []
    for b in bars_today[-5:]:
        rng = b['h'] - b['l']
        if rng > 0:
            bps.append((b['c'] - b['l']) / rng)
    buy_pressure = (sum(bps) / len(bps)) if bps else 0.5

    ml, mh = macd_histogram(closes)
    return {
        'price': price,
        'ema_stack': stack,
        'vwap': vwap,
        'vwap_dist': (price - vwap) if vwap else None,
        'vwap_dist_sigma': vwap_dist_sigma,
        'mom_1': mom_1,
        'mom_5': mom_5,
        'vol_z': vol_z,
        'buy_pressure': buy_pressure,
        'rsi':       rsi14(closes),
        'obv_trend': obv_trend(bars_today),
        'macd_line': ml,
        'macd_hist': mh,
        'adx':       adx14(bars_today),
    }

def compute_btc_ind(btc_session_bars):
    if len(btc_session_bars) < MIN_HISTORY:
        return None
    closes = [b['c'] for b in btc_session_bars]
    ef, em, es = ema(closes, 3), ema(closes, 8), ema(closes, 21)
    stack = None
    if ef and em and es:
        if ef > em > es:   stack = 'bull'
        elif ef < em < es: stack = 'bear'
        else:              stack = 'mixed'
    mom_5 = None
    if len(closes) >= 6 and closes[-6] != 0:
        mom_5 = (closes[-1] - closes[-6]) / closes[-6] * 100
    return {'stack': stack, 'mom_5': mom_5}

def detect_signal(ticker, ind, btc):
    if ind is None:
        return None
    score = 0
    r_long, r_short = [], []

    st = ind.get('ema_stack')
    if st == 'bull':   score += 2; r_long.append('EMA 3>8>21')
    elif st == 'bear': score -= 2; r_short.append('EMA 3<8<21')

    vd  = ind.get('vwap_dist')
    vds = ind.get('vwap_dist_sigma')
    if vd is not None:
        if vd > 0:
            score += 1
            r_long.append(f'above VWAP {vds:+.2f}σ' if vds is not None else 'above VWAP')
        else:
            score -= 1
            r_short.append(f'below VWAP {vds:+.2f}σ' if vds is not None else 'below VWAP')

    bp = ind.get('buy_pressure')
    if bp is not None:
        if bp >= 0.65:
            score += 2; r_long.append(f'buy-press {bp:.2f}')
        elif bp <= 0.35:
            score -= 2; r_short.append(f'buy-press {bp:.2f}')

    m1, m5 = ind.get('mom_1'), ind.get('mom_5')
    if m1 is not None and m5 is not None:
        if m1 > 0.10 and m5 > 0.10:
            score += 1; r_long.append(f'mom 1m:{m1:+.2f}% 5m:{m5:+.2f}%')
        elif m1 < -0.10 and m5 < -0.10:
            score -= 1; r_short.append(f'mom 1m:{m1:+.2f}% 5m:{m5:+.2f}%')

    vz = ind.get('vol_z')
    if vz is not None and vz >= 2.0:
        if score > 0:   r_long.append(f'vol z={vz:.1f}')
        elif score < 0: r_short.append(f'vol z={vz:.1f}')

    if btc:
        bs, bm = btc.get('stack'), btc.get('mom_5')
        if bs == 'bull' and bm and bm > 0:
            if score > 0:    score += 1; r_long.append(f'BTC bull 5m:{bm:+.2f}%')
            elif score < 0:  score += 1
        elif bs == 'bear' and bm and bm < 0:
            if score < 0:    score -= 1; r_short.append(f'BTC bear 5m:{bm:+.2f}%')
            elif score > 0:  score -= 1

    abs_s = abs(score)
    if abs_s < 4:
        return None
    conv = 'HIGH' if abs_s >= 7 else 'MEDIUM' if abs_s >= 5 else 'LOW'
    side = 'LONG' if score > 0 else 'SHORT'
    return {
        'side': side,
        'conviction': conv,
        'score': score,
        'price': ind['price'],
        'reasons': (r_long if side == 'LONG' else r_short)[:3],
    }

# ── Position management ─────────────────────────────────────────────────
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
    return {
        'ticker':       tkr,
        'side':         side,
        'entry':        entry,
        'entry_ts':     bar['t'],
        'sl':           sl,
        'tp':           tp,
        'qty':          qty,
        'alloc':        alloc,
        'conviction':   sig['conviction'],
        'reasons':      sig['reasons'],
        'bars_held':    0,
    }

def check_exit_on_bar(pos, bar):
    """Return (exit_price, reason) or (None, None). Conservatively assumes
    SL hit first when both SL and TP fall in the bar's range."""
    side = pos['side']
    if side == 'LONG':
        sl_hit = bar['l'] <= pos['sl']
        tp_hit = bar['h'] >= pos['tp']
        if sl_hit and tp_hit:  return pos['sl'], 'stop_loss'   # pessimistic
        if sl_hit:             return pos['sl'], 'stop_loss'
        if tp_hit:             return pos['tp'], 'take_profit'
    else:  # SHORT
        sl_hit = bar['h'] >= pos['sl']
        tp_hit = bar['l'] <= pos['tp']
        if sl_hit and tp_hit:  return pos['sl'], 'stop_loss'
        if sl_hit:             return pos['sl'], 'stop_loss'
        if tp_hit:             return pos['tp'], 'take_profit'
    return None, None

def close_position(pos, exit_price, reason, exit_ts):
    side = pos['side']
    if side == 'LONG':
        pnl = (exit_price - pos['entry']) * pos['qty']
    else:
        pnl = (pos['entry'] - exit_price) * pos['qty']
    return {
        'ticker':     pos['ticker'],
        'side':       side,
        'entry':      round(pos['entry'], 4),
        'sl':         pos['sl'],
        'tp':         pos['tp'],
        'exit':       round(exit_price, 4),
        'qty':        round(pos['qty'], 4),
        'alloc':      pos['alloc'],
        'pnl':        round(pnl, 2),
        'result':     'WIN' if pnl > 0 else ('LOSS' if pnl < 0 else 'FLAT'),
        'reason':     reason,
        'entry_ts':   pos['entry_ts'],
        'exit_ts':    exit_ts,
        'conviction': pos['conviction'],
        'reasons':    ' · '.join(pos['reasons']) if pos['reasons'] else '',
    }

# ── Main ────────────────────────────────────────────────────────────────
def main():
    print(f'[backtest] fetching {START_DATE}..{END_DATE}')
    start_iso = f'{START_DATE}T00:00:00Z'
    end_iso   = f'{END_DATE}T23:59:59Z'

    bars_by_tkr = {}
    for t in TICKERS:
        bars = alpaca_stock_bars(t, start_iso, end_iso, timeframe='1Min')
        bars.sort(key=lambda b: b['t'])
        bars_by_tkr[t] = bars
        print(f'  {t:>6}: {len(bars):>6} raw bars')

    btc_bars = alpaca_crypto_bars(BTC_SYM, start_iso, end_iso, timeframe='1Min')
    btc_bars.sort(key=lambda b: b['t'])
    print(f'  {BTC_SYM:>6}: {len(btc_bars):>6} raw bars')

    # Build per-minute indexes
    bar_idx = {(t, b['t']): b for t, lst in bars_by_tkr.items() for b in lst}
    btc_idx = {b['t']: b for b in btc_bars}

    # All session minutes across both tickers
    all_ts = sorted(set(b['t'] for t in TICKERS for b in bars_by_tkr[t]
                        if in_session(b['t'])))
    print(f'  session minutes across tickers: {len(all_ts)}')

    # Per-ticker session-day history + BTC session-day history
    history  = {t: [] for t in TICKERS}
    btc_hist = []
    positions = {}
    balance   = START_BALANCE
    trades    = []
    prev_day  = None

    for idx, ts in enumerate(all_ts):
        day = utc_date_iso(ts)
        next_day = utc_date_iso(all_ts[idx + 1]) if idx + 1 < len(all_ts) else None

        # ── Day rollover: reset per-day history (VWAP resets) ──────────
        if prev_day and day != prev_day:
            # Before resetting, any positions still open at end of prev_day
            # should have been closed on the prev iteration's session_end.
            # Safety: force-close any dangling positions now at last known bar.
            for tkr in list(positions.keys()):
                if history[tkr]:
                    last_bar = history[tkr][-1]
                    tr = close_position(positions[tkr], last_bar['c'],
                                        'session_end_rollover', last_bar['t'])
                    balance = round(balance + tr['pnl'], 2)
                    tr['balance_after'] = balance
                    trades.append(tr)
                    del positions[tkr]
            history  = {t: [] for t in TICKERS}
            btc_hist = []
        prev_day = day

        # Append BTC bar (if present) to today's BTC history
        bb = btc_idx.get(ts)
        if bb is not None:
            btc_hist.append(bb)

        # ── Per ticker this minute ─────────────────────────────────────
        for tkr in TICKERS:
            bar = bar_idx.get((tkr, ts))
            if bar is None:
                continue
            history[tkr].append(bar)

            # 1) Manage open position against THIS bar (fresh minute of price action)
            if tkr in positions:
                pos = positions[tkr]
                # Only use bars strictly after entry; skip the entry bar itself
                if bar['t'] > pos['entry_ts']:
                    pos['bars_held'] += 1
                    ep, reason = check_exit_on_bar(pos, bar)
                    # No time stops — positions only exit on SL, TP, or session end.
                    if ep is not None:
                        tr = close_position(pos, ep, reason, bar['t'])
                        balance = round(balance + tr['pnl'], 2)
                        tr['balance_after'] = balance
                        trades.append(tr)
                        del positions[tkr]

            # 2) Session-end force-flat: if this is the last session-minute
            #    of the day for this ticker, close the position at bar close.
            is_last_minute_of_day = (next_day != day)
            if is_last_minute_of_day and tkr in positions:
                pos = positions[tkr]
                tr = close_position(pos, bar['c'], 'session_end', bar['t'])
                balance = round(balance + tr['pnl'], 2)
                tr['balance_after'] = balance
                trades.append(tr)
                del positions[tkr]

            # 3) Try to open a new position (unless it's literally the last
            #    minute of the session — nothing to manage on tomorrow's bars)
            if tkr not in positions and not is_last_minute_of_day:
                if balance < MIN_BALANCE:
                    continue
                ind     = compute_indicators(history[tkr])
                btc_ind = compute_btc_ind(btc_hist)
                sig = detect_signal(tkr, ind, btc_ind)
                if sig and sig['conviction'] in MIN_CONVICTION:
                    pos = open_position(tkr, sig, bar, balance)
                    if pos:
                        positions[tkr] = pos

    # End of dataset — close anything dangling
    for tkr in list(positions.keys()):
        if history[tkr]:
            last_bar = history[tkr][-1]
            tr = close_position(positions[tkr], last_bar['c'],
                                'end_of_data', last_bar['t'])
            balance = round(balance + tr['pnl'], 2)
            tr['balance_after'] = balance
            trades.append(tr)
            del positions[tkr]

    # ── Summary ────────────────────────────────────────────────────────
    wins   = sum(1 for t in trades if t['result'] == 'WIN')
    losses = sum(1 for t in trades if t['result'] == 'LOSS')
    flats  = sum(1 for t in trades if t['result'] == 'FLAT')
    total_pnl = round(balance - START_BALANCE, 2)
    win_rate  = (100 * wins / (wins + losses)) if (wins + losses) else 0

    print()
    print('========== Backtest Summary ==========')
    print(f'  period:     {START_DATE} .. {END_DATE}')
    print(f'  trades:     {len(trades)} (W:{wins}  L:{losses}  F:{flats})')
    print(f'  win rate:   {win_rate:.1f}%')
    print(f'  start bal:  ${START_BALANCE:.2f}')
    print(f'  end   bal:  ${balance:.2f}')
    print(f'  total P/L:  {"+" if total_pnl >= 0 else ""}${total_pnl:.2f}  ({total_pnl/START_BALANCE*100:+.2f}%)')
    # Per-ticker breakdown
    for tkr in TICKERS:
        tk_tr = [t for t in trades if t['ticker'] == tkr]
        tk_pnl = round(sum(t['pnl'] for t in tk_tr), 2)
        tk_w   = sum(1 for t in tk_tr if t['result'] == 'WIN')
        tk_l   = sum(1 for t in tk_tr if t['result'] == 'LOSS')
        tk_wr  = (100 * tk_w / (tk_w + tk_l)) if (tk_w + tk_l) else 0
        print(f'  {tkr}: {len(tk_tr)} trades · W:{tk_w} L:{tk_l} · wr {tk_wr:.1f}% · P/L ${tk_pnl:+.2f}')
    # Exit-reason breakdown
    reasons = {}
    for t in trades:
        reasons[t['reason']] = reasons.get(t['reason'], 0) + 1
    print(f'  exit reasons: {reasons}')

    # ── CSV ────────────────────────────────────────────────────────────
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), OUT_CSV)
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow([
            '#', 'open_time_ct', 'close_time_ct', 'ticker', 'side',
            'entry', 'stop', 'target', 'exit', 'qty', 'alloc',
            'result', 'pnl', 'balance_after', 'exit_reason',
            'conviction', 'signal_reasons',
        ])
        for i, t in enumerate(trades, 1):
            w.writerow([
                i,
                ct_iso(t['entry_ts']),
                ct_iso(t['exit_ts']),
                t['ticker'], t['side'],
                f"{t['entry']:.4f}", f"{t['sl']:.4f}", f"{t['tp']:.4f}",
                f"{t['exit']:.4f}",
                f"{t['qty']:.4f}", f"{t['alloc']:.2f}",
                t['result'], f"{t['pnl']:+.2f}",
                f"{t.get('balance_after', '')}",
                t['reason'], t['conviction'], t['reasons'],
            ])
    print(f'\n  -> wrote {len(trades)} rows to {out_path}')

if __name__ == '__main__':
    main()
