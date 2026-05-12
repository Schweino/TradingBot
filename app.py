import os
import time
import threading
import json
from flask import Flask, render_template, request, jsonify
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore

try:
    from win11toast import toast as _win_toast
except Exception:
    _win_toast = None

load_dotenv()

_ET = ZoneInfo('America/New_York')
_CT = ZoneInfo('America/Chicago')

app = Flask(__name__)
ALPACA_API_KEY    = os.getenv('ALPACA_API_KEY')
ALPACA_SECRET_KEY = os.getenv('ALPACA_SECRET_KEY')

from data_fetch import alpaca_stock_bars, alpaca_crypto_bars, alpaca_crypto_latest  # noqa: E402


def _et_day_bounds(date_str):
    """Return (start_iso_utc, end_iso_utc) for 04:00 ET → 20:00 ET on the given date."""
    d = datetime.strptime(date_str, '%Y-%m-%d').date()
    start = datetime(d.year, d.month, d.day, 4,  0, 0, tzinfo=_ET)
    end   = datetime(d.year, d.month, d.day, 20, 0, 0, tzinfo=_ET)
    return (start.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
            end.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'))


def get_intraday_data(ticker):
    """Pre-market (04:00 ET) through post-market, scanning back up to 6 days."""
    for days_back in range(0, 6):
        day_dt = datetime.now(timezone.utc) - timedelta(days=days_back)
        date   = day_dt.strftime('%Y-%m-%d')
        start_iso, end_iso = _et_day_bounds(date)
        results = alpaca_stock_bars(ticker, start_iso, end_iso, timeframe='1Min')
        if results and len(results) >= 3:
            df = pd.DataFrame(results)
            df = df.rename(columns={
                'o': 'open', 'h': 'high', 'l': 'low',
                'c': 'close', 'v': 'volume', 't': 'timestamp'
            })
            df['dt_utc'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
            df = df.sort_values('timestamp').reset_index(drop=True)
            return df, date
    raise ValueError(f"No recent intraday data found for {ticker}. Check the ticker symbol.")


def _premarket_mask(dt_series):
    """True for bars that started before 09:30 ET (pre-market)."""
    et = dt_series.dt.tz_convert(_ET)
    h, m = et.dt.hour, et.dt.minute
    return (h < 9) | ((h == 9) & (m < 30))


def get_daily_context(ticker):
    end_date   = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    start_date = (datetime.now(timezone.utc) - timedelta(days=40)).strftime('%Y-%m-%d')
    try:
        results = alpaca_stock_bars(ticker, start_date, end_date, timeframe='1Day')
    except Exception:
        return None
    if not results:
        return None
    df = pd.DataFrame(results)
    df = df.rename(columns={'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume'})
    # Drop today's in-progress bar if present — once the market opens, Alpaca
    # returns a partial daily bar whose close = live price. That would make
    # prev_close track the live price instead of yesterday's real close.
    if 't' in df.columns and len(df) > 1:
        today_et = datetime.now(_ET).strftime('%Y-%m-%d')
        last_ts_ms = int(df['t'].iloc[-1])
        last_bar_et = datetime.fromtimestamp(last_ts_ms/1000, tz=_ET).strftime('%Y-%m-%d')
        if last_bar_et == today_et:
            df = df.iloc[:-1].reset_index(drop=True)
    closes = df['close'].tolist()
    ema9  = pd.Series(closes).ewm(span=9,  adjust=False).mean()
    ema21 = pd.Series(closes).ewm(span=21, adjust=False).mean()
    return {
        'prev_close':       round(float(df['close'].iloc[-1]), 2),
        'prev_high':        round(float(df['high'].iloc[-1]),  2),
        'prev_low':         round(float(df['low'].iloc[-1]),   2),
        'avg_daily_volume': int(df['volume'].mean()),
        'daily_trend':      'bullish' if ema9.iloc[-1] > ema21.iloc[-1] else 'bearish',
        'pct_5d': round(((closes[-1] - closes[-5]) / closes[-5]) * 100, 2) if len(closes) >= 5 else None,
    }


def get_btc_data(last_stock_close_date):
    close_d = datetime.strptime(last_stock_close_date, '%Y-%m-%d').date()
    close_utc = datetime(close_d.year, close_d.month, close_d.day,
                         16, 0, 0, tzinfo=_ET).astimezone(timezone.utc)
    # If today's 4pm ET hasn't happened yet (pre-market / mid-session),
    # anchor to the prior TRADING day's close so btc_pct_change reflects
    # the move since the last real close — not zero.
    now_utc = datetime.now(timezone.utc)
    if close_utc > now_utc:
        probe = close_d - timedelta(days=1)
        while probe.weekday() >= 5:  # skip Sat/Sun
            probe -= timedelta(days=1)
        close_d = probe
        close_utc = datetime(close_d.year, close_d.month, close_d.day,
                             16, 0, 0, tzinfo=_ET).astimezone(timezone.utc)
        last_stock_close_date = close_d.strftime('%Y-%m-%d')

    # BTC price at the ~4 PM ET stock close (5-min bar window around that moment)
    btc_at_close = None
    window_start = (close_utc - timedelta(minutes=10)).strftime('%Y-%m-%dT%H:%M:%SZ')
    window_end   = (close_utc + timedelta(minutes=5)).strftime('%Y-%m-%dT%H:%M:%SZ')
    try:
        bars = alpaca_crypto_bars('BTC/USD', window_start, window_end, timeframe='5Min')
        if bars:
            btc_at_close = round(float(bars[-1]['c']), 2)
    except Exception:
        pass
    if btc_at_close is None:
        # Fallback: daily bar for that date
        try:
            dbars = alpaca_crypto_bars('BTC/USD', last_stock_close_date,
                                        (close_d + timedelta(days=1)).strftime('%Y-%m-%d'),
                                        timeframe='1Day')
            if dbars:
                btc_at_close = round(float(dbars[0]['c']), 2)
        except Exception:
            pass
    if btc_at_close is None:
        raise ValueError("Could not fetch BTC close price from Alpaca.")

    # 5-day trend from daily bars
    btc_trend_5d = None
    try:
        end_date   = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        start_date = (datetime.now(timezone.utc) - timedelta(days=10)).strftime('%Y-%m-%d')
        dbars = alpaca_crypto_bars('BTC/USD', start_date, end_date, timeframe='1Day')
        daily_closes = [b['c'] for b in dbars]
        if len(daily_closes) >= 5:
            btc_trend_5d = round(((daily_closes[-1] - daily_closes[-5]) / daily_closes[-5]) * 100, 2)
    except Exception:
        pass

    # Current BTC price (real-time from Alpaca)
    btc_current = alpaca_crypto_latest('BTC/USD')
    btc_price_source = 'alpaca'
    if btc_current is None:
        btc_current = btc_at_close
        btc_price_source = 'fallback'
    btc_current = round(float(btc_current), 2)
    btc_pct_change = round(((btc_current - btc_at_close) / btc_at_close) * 100, 2)

    now_et = datetime.now(_ET)
    hour_et = now_et.hour
    minute_et = now_et.minute
    now_weekday = now_et.weekday()
    market_is_open = (now_weekday < 5) and (hour_et > 9 or (hour_et == 9 and minute_et >= 30)) and (hour_et < 16)

    if now_weekday >= 5:
        days_to_open = 2 - (now_weekday - 5)
        context_label = f"weekend — market opens in {days_to_open} day{'s' if days_to_open > 1 else ''}"
    elif not market_is_open:
        context_label = "market closed (pre/after-hours)"
    else:
        context_label = "intraday (market open)"

    return {
        'btc_at_close': btc_at_close,
        'btc_current': btc_current,
        'btc_pct_change': btc_pct_change,
        'btc_trend_5d': btc_trend_5d,
        'context_label': context_label,
        'last_stock_close_date': last_stock_close_date,
        'close_label': f"{last_stock_close_date} 3:00 PM CT",
        'price_source': btc_price_source,
    }


# ── Indicator calculations ────────────────────────────────────────────────

def calculate_rsi(closes, period=14):
    s = pd.Series(closes)
    delta = s.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return [round(x, 2) if not np.isnan(x) else None for x in rsi]


def calculate_ema(closes, period):
    result = pd.Series(closes).ewm(span=period, adjust=False).mean()
    return [round(x, 4) for x in result]


def calculate_atr(df, period=14):
    prev_close = df['close'].shift(1)
    tr = pd.concat([
        df['high'] - df['low'],
        (df['high'] - prev_close).abs(),
        (df['low'] - prev_close).abs()
    ], axis=1).max(axis=1)
    atr = tr.ewm(com=period - 1, min_periods=period).mean()
    return [round(x, 4) if not np.isnan(x) else None for x in atr]


def calculate_macd(closes, fast=12, slow=26, signal=9):
    s = pd.Series(closes)
    ema_fast = s.ewm(span=fast, adjust=False).mean()
    ema_slow = s.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return {
        'macd': [round(x, 4) for x in macd_line],
        'signal': [round(x, 4) for x in signal_line],
        'histogram': [round(x, 4) for x in histogram],
    }


def calculate_vwap_bands(df):
    """VWAP ±1σ and ±2σ bands. Pass session-only bars."""
    typical = (df['high'] + df['low'] + df['close']) / 3
    vol = df['volume']
    cum_vol = vol.cumsum()
    cum_pv = (typical * vol).cumsum()
    cum_pv2 = (typical ** 2 * vol).cumsum()
    vwap = cum_pv / cum_vol
    variance = (cum_pv2 / cum_vol) - vwap ** 2
    sd = np.sqrt(variance.clip(lower=0))
    to_list = lambda s: [round(x, 4) for x in s]
    return {
        'vwap':   to_list(vwap),
        'upper1': to_list(vwap + sd),
        'lower1': to_list(vwap - sd),
        'upper2': to_list(vwap + 2 * sd),
        'lower2': to_list(vwap - 2 * sd),
    }


def calculate_cvd(df):
    """Cumulative Volume Delta — approximated from OHLCV bars."""
    hl = (df['high'] - df['low']).replace(0, np.nan)
    buy_ratio = ((df['close'] - df['low']) / hl).fillna(0.5).clip(0, 1)
    buy_vol   = df['volume'] * buy_ratio
    sell_vol  = df['volume'] * (1 - buy_ratio)
    delta     = buy_vol - sell_vol
    cvd       = delta.cumsum()
    return {
        'delta': [round(float(x)) for x in delta],
        'cvd':   [round(float(x)) for x in cvd],
    }


def calculate_volume_profile(df, n_bins=24):
    """Intraday volume profile with POC and Value Area (70%)."""
    if df.empty:
        return {'prices': [], 'volumes': [], 'poc': None, 'poc_idx': 0, 'vah': None, 'val': None}
    price_min = float(df['low'].min())
    price_max = float(df['high'].max())
    if price_max == price_min:
        price_max += 0.01
    bin_size = (price_max - price_min) / n_bins
    vols = [0.0] * n_bins
    for row in df[['low', 'high', 'volume']].itertuples(index=False):
        lo = max(0, min(n_bins - 1, int((float(row.low)  - price_min) / bin_size)))
        hi = max(0, min(n_bins - 1, int((float(row.high) - price_min) / bin_size)))
        v  = float(row.volume) / max(1, hi - lo + 1)
        for b in range(lo, hi + 1):
            vols[b] += v
    prices  = [round(price_min + (i + 0.5) * bin_size, 4) for i in range(n_bins)]
    volumes = [round(v) for v in vols]
    poc_idx = volumes.index(max(volumes))
    poc     = prices[poc_idx]
    # Value area: expand outward from POC until 70% of total volume is covered
    total, target = sum(volumes), sum(volumes) * 0.70
    va, above, below, cum = {poc_idx}, poc_idx + 1, poc_idx - 1, volumes[poc_idx]
    while cum < target:
        a = volumes[above] if above < n_bins else -1
        b = volumes[below] if below >= 0   else -1
        if a <= 0 and b <= 0:
            break
        if a >= b:
            va.add(above); cum += a; above += 1
        else:
            va.add(below); cum += b; below -= 1
    va_s = sorted(va)
    return {
        'prices':  prices,
        'volumes': volumes,
        'poc':     round(poc, 4),
        'poc_idx': poc_idx,
        'vah':     round(prices[max(va_s)], 4),
        'val':     round(prices[min(va_s)], 4),
    }


def calculate_pivots(prev_high, prev_low, prev_close):
    p = (prev_high + prev_low + prev_close) / 3
    return {
        'p':  round(p, 2),
        'r1': round(2 * p - prev_low, 2),
        'r2': round(p + (prev_high - prev_low), 2),
        's1': round(2 * p - prev_high, 2),
        's2': round(p - (prev_high - prev_low), 2),
    }


def calculate_bollinger(closes, period=20, mult=2.0):
    """Returns dict with mid/upper/lower arrays (SMA ± mult·stdev)."""
    s = pd.Series(closes, dtype='float64')
    mid = s.rolling(window=period, min_periods=period).mean()
    sd = s.rolling(window=period, min_periods=period).std(ddof=0)
    upper = mid + mult * sd
    lower = mid - mult * sd
    to_list = lambda series: [None if pd.isna(x) else float(x) for x in series]
    return {'mid': to_list(mid), 'upper': to_list(upper), 'lower': to_list(lower)}


def calculate_keltner(df, period=20, mult=1.5):
    """ATR-based envelope around EMA of typical price. Returns upper/lower arrays."""
    closes = df['close'].tolist()
    ema = calculate_ema(closes, period)
    atr = calculate_atr(df, period)
    upper, lower = [], []
    for i in range(len(closes)):
        if ema[i] is None or atr[i] is None:
            upper.append(None); lower.append(None); continue
        upper.append(ema[i] + mult * atr[i])
        lower.append(ema[i] - mult * atr[i])
    return {'upper': upper, 'lower': lower}


def detect_squeeze(bb, kc):
    """TTM-style squeeze: BB inside Keltner = compression. Returns last-bar state."""
    n = len(bb['upper']) - 1
    if n < 0 or bb['upper'][n] is None or kc['upper'][n] is None:
        return None
    return bb['upper'][n] < kc['upper'][n] and bb['lower'][n] > kc['lower'][n]


def detect_candle_pattern(df):
    """Classify the last bar. Returns (pattern_name, bias) or (None, None)."""
    if len(df) < 2:
        return (None, None)
    last = df.iloc[-1]; prev = df.iloc[-2]
    o, h, l, c = float(last['open']), float(last['high']), float(last['low']), float(last['close'])
    po, pc = float(prev['open']), float(prev['close'])
    rng = h - l
    if rng <= 0:
        return (None, None)
    body = abs(c - o)
    upper_wick = h - max(o, c)
    lower_wick = min(o, c) - l

    # Shooting star: small body near low, long upper wick (bearish reversal)
    if upper_wick >= 2 * body and lower_wick <= body and body / rng < 0.3 and c < o:
        return ('Shooting Star', 'bearish')
    # Hammer: small body near high, long lower wick (bullish reversal)
    if lower_wick >= 2 * body and upper_wick <= body and body / rng < 0.3 and c > o:
        return ('Hammer', 'bullish')
    # Doji: very small body relative to range
    if body / rng < 0.1:
        return ('Doji', 'neutral')
    # Bearish engulfing: prev green, current red, body covers prev body
    if pc > po and c < o and o >= pc and c <= po:
        return ('Bearish Engulfing', 'bearish')
    # Bullish engulfing
    if pc < po and c > o and o <= pc and c >= po:
        return ('Bullish Engulfing', 'bullish')
    return (None, None)


def compute_volume_climax(df, lookback=20, mult=3.0):
    """Last-bar volume vs trailing avg. Returns (ratio, is_climax)."""
    if len(df) < lookback + 1:
        return (None, False)
    last_vol = float(df['volume'].iloc[-1])
    avg_vol = float(df['volume'].iloc[-(lookback + 1):-1].mean())
    if avg_vol <= 0:
        return (None, False)
    ratio = last_vol / avg_vol
    return (round(ratio, 2), ratio >= mult)


# ── Post-mortem builder ───────────────────────────────────────────────────

def build_postmortem(direction, analysis, outcome, daily_ctx, btc_data, latest_e, entry_price=None):
    score = analysis['score']
    sig   = analysis['decision']
    profitable = outcome['profitable'] if outcome else None
    price = latest_e.get('price', 0) or 0  # reconstructed market price at entry bar

    # Collect individual signals
    bearish, bullish = [], []
    execution_warnings = []  # execution-quality issues (separate from signal warnings)

    # ── Execution quality checks ──────────────────────────────────
    sig_target = analysis.get('target')
    sig_entry  = analysis.get('entry')
    sig_stop   = analysis.get('stop')
    sig_rr     = analysis.get('rr')

    if entry_price and price and abs(entry_price - price) / price > 0.003:
        chase_pct = (entry_price - price) / price * 100
        if direction == 'LONG' and chase_pct > 0:
            execution_warnings.append(('Chased Entry', f'You paid ${entry_price:.2f} but the bar close was ${price:.2f} (+{chase_pct:.2f}%). Entering above the signal price reduces R/R and means you\'re buying into strength rather than value.'))
        elif direction == 'SHORT' and chase_pct < 0:
            execution_warnings.append(('Chased Entry', f'You sold short at ${entry_price:.2f} but the bar close was ${price:.2f} ({chase_pct:.2f}%). Shorting below the signal price reduces R/R.'))

    if entry_price and sig_target:
        if direction == 'LONG' and entry_price >= sig_target:
            execution_warnings.append(('Entered Above Target', f'You entered at ${entry_price:.2f} but the signal\'s target was only ${sig_target:.2f} — you were already past the profit objective before the trade started. R/R was negative from the moment you filled.'))
        elif direction == 'SHORT' and entry_price <= sig_target:
            execution_warnings.append(('Entered Below Target', f'You entered at ${entry_price:.2f} but the signal\'s target was ${sig_target:.2f} — already past the profit objective. R/R was negative from the moment you filled.'))

    if sig_rr is not None and sig_rr < 1.0:
        execution_warnings.append(('Poor Risk/Reward', f'The signal R/R was only {sig_rr}:1 — professional traders typically require at least 2:1 before entering. Setups with sub-1:1 R/R require a very high win rate (>50%) just to break even.'))

    # ── Signal-level checks ───────────────────────────────────────
    vwap = latest_e.get('vwap')
    if vwap:
        if price < vwap:
            bearish.append(('Below VWAP', f'Price ${price:.2f} was under VWAP ${vwap:.2f} — intraday structure is bearish; sellers control the tape'))
        else:
            bullish.append(('Above VWAP', f'Price ${price:.2f} was above VWAP ${vwap:.2f} — intraday structure is bullish'))

    ema9 = latest_e.get('ema9'); ema21 = latest_e.get('ema21')
    if ema9 and ema21:
        if ema9 < ema21:
            bearish.append(('EMA Bearish Cross', f'EMA9 (${ema9:.2f}) < EMA21 (${ema21:.2f}) — short-term momentum has turned down'))
        else:
            bullish.append(('EMA Bullish', f'EMA9 (${ema9:.2f}) > EMA21 (${ema21:.2f}) — short-term momentum is up'))

    rsi = latest_e.get('rsi')
    if rsi is not None:
        if rsi > 70:
            bearish.append(('RSI Overbought', f'RSI at {rsi:.1f} — extended; reversal risk is elevated'))
        elif rsi >= 55:
            bullish.append(('RSI Bullish Zone', f'RSI at {rsi:.1f} — momentum favors longs'))
        elif rsi <= 30:
            bullish.append(('RSI Oversold', f'RSI at {rsi:.1f} — potential bounce territory'))
        elif rsi < 45:
            bearish.append(('RSI Weak', f'RSI at {rsi:.1f} — momentum is weak, below midpoint'))

    ml = latest_e.get('macd_line'); ms = latest_e.get('macd_signal')
    if ml is not None and ms is not None:
        if ml < ms:
            bearish.append(('MACD Bearish', 'MACD line below signal line — bearish momentum crossover'))
        else:
            bullish.append(('MACD Bullish', 'MACD line above signal line — bullish momentum'))

    cvd_div = latest_e.get('cvd_divergence')
    if cvd_div == 'bearish_div':
        bearish.append(('CVD Bearish Divergence', 'Price moved up but cumulative volume delta declined — hidden selling pressure; the move lacked real buying support'))
    elif cvd_div == 'bullish_div':
        bullish.append(('CVD Bullish Divergence', 'Price dipped but volume delta rose — accumulation underway beneath the surface'))

    poc = latest_e.get('poc')
    if poc and price:
        if price < poc:
            bearish.append(('Below Volume POC', f'Price ${price:.2f} below Point of Control ${poc:.2f} — most session volume traded above; sellers have the advantage'))
        else:
            bullish.append(('Above Volume POC', f'Price ${price:.2f} above POC ${poc:.2f} — trading above the fair value area'))

    # ── Overextension / exhaustion (mean-reversion shorts on long-side spikes) ──
    vwap_u1 = latest_e.get('vwap_upper1'); vwap_u2 = latest_e.get('vwap_upper2')
    vwap_l1 = latest_e.get('vwap_lower1'); vwap_l2 = latest_e.get('vwap_lower2')
    if vwap and price:
        vwap_dist_pct = (price - vwap) / vwap * 100
        if vwap_u2 and price >= vwap_u2:
            bearish.append(('Price at Upper VWAP 2σ Band', f'Price ${price:.2f} ≥ VWAP +2σ (${vwap_u2:.2f}) — statistically stretched far above intraday mean. Mean-reversion shorts fire here; longs are chasing extremes.'))
        elif vwap_u1 and price >= vwap_u1:
            bearish.append(('Extended Above VWAP', f'Price ${price:.2f} ≥ VWAP +1σ (${vwap_u1:.2f}), {vwap_dist_pct:+.2f}% over VWAP — trading outside the normal intraday range, reversion risk is elevated.'))
        elif vwap_l2 and price <= vwap_l2:
            bullish.append(('Price at Lower VWAP 2σ Band', f'Price ${price:.2f} ≤ VWAP -2σ (${vwap_l2:.2f}) — statistically stretched below mean; mean-reversion longs fire here.'))
        elif vwap_l1 and price <= vwap_l1:
            bullish.append(('Extended Below VWAP', f'Price ${price:.2f} ≤ VWAP -1σ (${vwap_l1:.2f}), {vwap_dist_pct:+.2f}% under VWAP — reversion to the mean is likely.'))

    # Extension from EMA9 (short-term moving average)
    if ema9 and price:
        ema9_dist_pct = (price - ema9) / ema9 * 100
        if ema9_dist_pct > 0.5:
            bearish.append(('Stretched From EMA9', f'Price ${price:.2f} is {ema9_dist_pct:+.2f}% above EMA9 (${ema9:.2f}) — rubber-band effect; short-term reversion to EMA is the higher-probability move.'))
        elif ema9_dist_pct < -0.5:
            bullish.append(('Stretched Below EMA9', f'Price ${price:.2f} is {ema9_dist_pct:+.2f}% below EMA9 (${ema9:.2f}) — bounce back toward EMA is likely.'))

    # Above Value Area High
    vah = latest_e.get('vah'); val = latest_e.get('val')
    if vah and price and price > vah:
        bearish.append(('Above Value Area High', f'Price ${price:.2f} above VAH ${vah:.2f} — trading outside the 70% value zone. Statistically, price reverts to value; longs here chase the edge of distribution.'))
    elif val and price and price < val:
        bullish.append(('Below Value Area Low', f'Price ${price:.2f} below VAL ${val:.2f} — trading outside the 70% value zone; reversion to value area typical.'))

    # Near session high (resistance) / session low (support)
    sess_high = latest_e.get('session_high'); sess_low = latest_e.get('session_low')
    if sess_high and price and (sess_high - price) / sess_high < 0.003 and price > 0:
        bearish.append(('Near Session High', f'Price ${price:.2f} is within 0.3% of the session high (${sess_high:.2f}) — entering at resistance; sellers typically defend highs on first test.'))
    if sess_low and price and (price - sess_low) / sess_low < 0.003 and price > 0:
        bullish.append(('Near Session Low', f'Price ${price:.2f} is within 0.3% of the session low (${sess_low:.2f}) — buyers typically defend lows.'))

    # Consecutive-green / consecutive-red exhaustion
    consec_green = latest_e.get('consec_green') or 0
    consec_red   = latest_e.get('consec_red') or 0
    if consec_green >= 4:
        bearish.append(('Exhaustion — Consecutive Green Bars', f'{consec_green} straight green bars into entry — short-term exhaustion; pullback probability rises sharply after 4+ consecutive up bars.'))
    if consec_red >= 4:
        bullish.append(('Exhaustion — Consecutive Red Bars', f'{consec_red} straight red bars into entry — short-term exhaustion; bounce probability rises sharply after 4+ consecutive down bars.'))

    # Short-window velocity (last 5 bars) — identifies parabolic moves
    last5_pct = latest_e.get('last5_pct')
    if last5_pct is not None:
        if last5_pct >= 0.8:
            bearish.append(('Parabolic 5-Bar Ramp', f'Price up {last5_pct:+.2f}% in the prior 5 bars — steep ramps like this typically give back 50%+ within the next 5-10 bars. Fade territory, not chase territory.'))
        elif last5_pct <= -0.8:
            bullish.append(('Steep 5-Bar Drop', f'Price down {last5_pct:+.2f}% in the prior 5 bars — steep drops often see mechanical bounces within 5-10 bars.'))

    # Bollinger Bands
    bb_mid = latest_e.get('bb_mid'); bb_up = latest_e.get('bb_upper'); bb_lo = latest_e.get('bb_lower')
    if bb_up and price and price >= bb_up:
        bearish.append(('Above Bollinger Upper', f'Price ${price:.2f} at/above BB upper (${bb_up:.2f}) — statistically 2σ above 20-bar mean. Strong mean-reversion zone.'))
    elif bb_lo and price and price <= bb_lo:
        bullish.append(('Below Bollinger Lower', f'Price ${price:.2f} at/below BB lower (${bb_lo:.2f}) — 2σ below mean; reversion setup.'))

    # TTM Squeeze — compression = impending breakout
    if latest_e.get('squeeze') is True:
        bearish.append(('Squeeze Active', 'Bollinger Bands are inside Keltner Channels — consolidation / volatility compression. Breakout direction is not yet confirmed; entering mid-squeeze is a coin flip until the band expansion fires.'))

    # Candlestick pattern on entry bar
    cpat = latest_e.get('candle_pattern'); cbias = latest_e.get('candle_bias')
    if cpat:
        if cbias == 'bearish':
            bearish.append((f'{cpat}', f'The entry bar printed a {cpat.lower()} pattern — classic short-term reversal candle.'))
        elif cbias == 'bullish':
            bullish.append((f'{cpat}', f'The entry bar printed a {cpat.lower()} pattern — classic short-term bullish reversal.'))

    # Prior-day levels
    pdh = latest_e.get('prev_day_high'); pdl = latest_e.get('prev_day_low')
    if pdh and price:
        dist_pct = abs(price - pdh) / pdh * 100
        if dist_pct < 0.2 and price <= pdh * 1.002:
            bearish.append(('At Prior-Day High', f'Price ${price:.2f} is near yesterday\'s high (${pdh:.2f}) — heavily-watched resistance; first test typically rejects.'))
        elif price > pdh:
            bullish.append(('Above Prior-Day High', f'Price ${price:.2f} has broken yesterday\'s high (${pdh:.2f}) — bullish structural breakout.'))
    if pdl and price:
        dist_pct = abs(price - pdl) / pdl * 100
        if dist_pct < 0.2 and price >= pdl * 0.998:
            bullish.append(('At Prior-Day Low', f'Price ${price:.2f} is near yesterday\'s low (${pdl:.2f}) — key support; first test typically bounces.'))
        elif price < pdl:
            bearish.append(('Below Prior-Day Low', f'Price ${price:.2f} has broken yesterday\'s low (${pdl:.2f}) — bearish structural breakdown.'))

    # Volume climax (last bar ≥ 3× trailing avg)
    vol_ratio = latest_e.get('vol_climax_ratio')
    if vol_ratio and vol_ratio >= 3.0:
        # On a rally, climax volume = likely exhaustion (fade); on a drop, capitulation (bounce)
        if last5_pct is not None and last5_pct > 0.3:
            bearish.append(('Volume Climax (Blow-off)', f'Entry bar volume was {vol_ratio:.1f}× the 20-bar average into a rally — classic exhaustion / blow-off top signature. Large players are unloading into retail chasers.'))
        elif last5_pct is not None and last5_pct < -0.3:
            bullish.append(('Volume Climax (Capitulation)', f'Entry bar volume was {vol_ratio:.1f}× the 20-bar average into a drop — capitulation signature; mechanical bounces typical.'))

    if daily_ctx:
        if daily_ctx.get('daily_trend') == 'bearish':
            bearish.append(('Bearish Daily Trend', 'Higher-timeframe daily chart is in a downtrend — short setups have macro backing; longs are fighting the tide'))
        elif daily_ctx.get('daily_trend') == 'bullish':
            bullish.append(('Bullish Daily Trend', 'Daily chart is in an uptrend — longs have higher-timeframe confirmation'))

    if btc_data:
        btc_pct = btc_data.get('btc_pct_change', 0) or 0
        if btc_pct <= -3:
            bearish.append(('Major BTC Selloff', f'BTC down {abs(btc_pct):.1f}% since prior close — severe macro headwind; crypto-correlated names typically follow'))
        elif btc_pct <= -1.5:
            bearish.append(('BTC Headwind', f'BTC down {abs(btc_pct):.1f}% — meaningful macro drag on momentum names'))
        elif btc_pct >= 2:
            bullish.append(('BTC Tailwind', f'BTC up {btc_pct:.1f}% — positive macro backdrop for risk assets'))

    # ── Determine verdict ─────────────────────────────────────────
    has_exec_problems = len(execution_warnings) > 0

    if outcome is None:
        # No exit provided
        if direction != sig and sig != 'FLAT':
            verdict = 'against_signal'
            label   = f'Setup Error — Signal Was {sig}, You Went {direction}'
            summary = f"Indicators were pointing {sig} (score {score:+}) at your entry but you went {direction}. See the warning signals below."
        elif sig == 'FLAT':
            verdict = 'no_edge'
            label   = 'No Edge — No Confirmed Setup at Entry'
            summary = f"The system found no clear directional edge at your entry time (score {score:+}). Entering here was speculation without a confirmed setup."
        elif has_exec_problems:
            verdict = 'ignored_warnings'
            label   = 'Signal Matched — But Execution Had Critical Problems'
            summary = (f"The {sig} signal agreed with your {direction} (score {score:+}), but there were execution-level issues at this entry. "
                       f"See the execution warnings below — these alone can turn a valid setup into a losing trade.")
        else:
            verdict = 'valid_setup'
            label   = 'Valid Setup — Entry Had Technical Merit'
            summary = (f"Your {direction} entry aligned with the {sig} signal (score {score:+}). "
                       f"Add the exit price to see whether the trade was profitable and get a full post-mortem.")
    elif profitable:
        if direction == sig:
            verdict = 'valid_win'
            label   = 'Winning Trade — Setup Executed Well'
            summary = f"Your {direction} trade aligned with the {sig} signal (score {score:+}) and closed profitably. Good execution."
        elif sig == 'FLAT':
            verdict = 'lucky_win'
            label   = 'Won With No Confirmed Edge'
            summary = f"The signal was flat (score {score:+}) but your {direction} trade was profitable. Be careful not to over-learn — there was no confirmed setup."
        else:
            verdict = 'lucky_win'
            label   = f'Won Against the Signal — {sig} Signal, {direction} Trade'
            summary = f"The signal pointed {sig} (score {score:+}) but your {direction} trade was profitable. Do not use this outcome to validate the entry approach."
    else:
        # Loss
        if sig != direction and sig != 'FLAT':
            verdict = 'against_signal'
            label   = f'Setup Error — Signal Was {sig}, You Went {direction}'
            summary = (f"The indicators were pointing {sig} (score {score:+}) at your exact entry time, but you went {direction}. "
                       f"You were trading against the signal from the start.")
        elif sig == 'FLAT':
            verdict = 'no_edge'
            label   = 'No Edge — Speculative Entry Without Confirmed Setup'
            summary = (f"There was no confirmed directional setup at entry (score {score:+}). "
                       f"Going {direction} here was a directional bet without technical backing — "
                       f"{len(bearish)} bearish and {len(bullish)} bullish factor(s) were present, roughly even odds.")
        elif has_exec_problems:
            verdict = 'ignored_warnings'
            label   = 'Execution Error — Entered At the Wrong Price Level'
            summary = (f"The {sig} signal agreed with your {direction} (score {score:+}), but the execution had critical problems. "
                       f"The warning(s) below show why this specific entry was unviable regardless of the signal direction.")
        elif len(bearish) >= 2:
            verdict = 'ignored_warnings'
            label   = 'Warning Signs Were Present — Setup Was Riskier Than It Appeared'
            summary = (f"The signal agreed with your {direction} (score {score:+}), but {len(bearish)} bearish factor(s) "
                       f"were active at entry. These warning signs elevated the risk and likely contributed to the adverse move.")
        else:
            verdict = 'signal_failed'
            label   = 'Valid Setup Failed — Adverse Outcome'
            summary = (f"The {sig} signal was valid at entry (score {score:+}) and aligned with your {direction} trade, "
                       f"but the setup failed. "
                       + (f"One warning sign was present (see below). " if bearish else "No major warning signs — this is within normal trade variance. ")
                       + "Review your stop-loss placement.")

    # ── What the trader should have seen / acted on ───────────────
    is_loss_or_unknown = (not profitable) or (outcome is None and (has_exec_problems or (direction != sig and sig != 'FLAT')))
    all_warnings = execution_warnings + (bearish if direction == 'LONG' else bullish)
    missed = all_warnings if is_loss_or_unknown else []

    return {
        'verdict':              verdict,
        'label':                label,
        'summary':              summary,
        'execution_warnings':   execution_warnings,
        'bearish_signals':      bearish,
        'bullish_signals':      bullish,
        'missed_signals':       missed,
    }


# ── Signal analysis ───────────────────────────────────────────────────────

def build_playbook(latest, daily_ctx=None, btc=None):
    """
    Generate LONG and SHORT scenario playbooks regardless of the main decision.
    Returns {'long': {...}, 'short': {...}} — each side has trigger, entry conditions,
    stop, target, R/R, supporting factors, and risks.
    """
    price   = latest.get('price')
    vwap    = latest.get('vwap')
    vwap_u1 = latest.get('vwap_upper1'); vwap_l1 = latest.get('vwap_lower1')
    vwap_u2 = latest.get('vwap_upper2'); vwap_l2 = latest.get('vwap_lower2')
    ema9    = latest.get('ema9');  ema21 = latest.get('ema21')
    rsi     = latest.get('rsi')
    atr     = latest.get('atr') or 0
    or_high = latest.get('or_high'); or_low = latest.get('or_low')
    pdh     = latest.get('prev_day_high'); pdl = latest.get('prev_day_low')
    pmh     = latest.get('premarket_high'); pml = latest.get('premarket_low')
    sess_hi = latest.get('session_high'); sess_lo = latest.get('session_low')
    poc     = latest.get('poc'); vah = latest.get('vah'); val = latest.get('val')
    r1 = latest.get('r1'); r2 = latest.get('r2')
    s1 = latest.get('s1'); s2 = latest.get('s2')
    bb_up = latest.get('bb_upper'); bb_lo = latest.get('bb_lower')
    rvol = latest.get('rvol')

    def upside_levels(ref=None):
        r = ref if ref is not None else price
        levels = [lv for lv in [vwap_u1, vah, r1, or_high, pmh, pdh, sess_hi, bb_up, r2] if lv and lv > r]
        return sorted(set(round(lv, 2) for lv in levels))

    def downside_levels(ref=None):
        r = ref if ref is not None else price
        levels = [lv for lv in [vwap_l1, val, s1, or_low, pml, pdl, sess_lo, bb_lo, s2] if lv and lv < r]
        return sorted(set(round(lv, 2) for lv in levels), reverse=True)

    ups = upside_levels()
    downs = downside_levels()
    far_threshold = (atr * 1.5) if atr else 0.30

    def label_of(lv):
        """Human-readable label for a price level."""
        tags = []
        if vwap_u1 and abs(lv - vwap_u1) < 0.01: tags.append('VWAP +1σ')
        if vwap_u2 and abs(lv - vwap_u2) < 0.01: tags.append('VWAP +2σ')
        if vwap_l1 and abs(lv - vwap_l1) < 0.01: tags.append('VWAP -1σ')
        if vwap_l2 and abs(lv - vwap_l2) < 0.01: tags.append('VWAP -2σ')
        if vah and abs(lv - vah) < 0.01: tags.append('VAH')
        if val and abs(lv - val) < 0.01: tags.append('VAL')
        if r1 and abs(lv - r1) < 0.01: tags.append('R1 pivot')
        if r2 and abs(lv - r2) < 0.01: tags.append('R2 pivot')
        if s1 and abs(lv - s1) < 0.01: tags.append('S1 pivot')
        if s2 and abs(lv - s2) < 0.01: tags.append('S2 pivot')
        if or_high and abs(lv - or_high) < 0.01: tags.append('ORB high')
        if or_low and abs(lv - or_low) < 0.01: tags.append('ORB low')
        if pmh and abs(lv - pmh) < 0.01: tags.append('premarket high')
        if pml and abs(lv - pml) < 0.01: tags.append('premarket low')
        if pdh and abs(lv - pdh) < 0.01: tags.append('prior-day high')
        if pdl and abs(lv - pdl) < 0.01: tags.append('prior-day low')
        if sess_hi and abs(lv - sess_hi) < 0.01: tags.append('session high')
        if sess_lo and abs(lv - sess_lo) < 0.01: tags.append('session low')
        if bb_up and abs(lv - bb_up) < 0.01: tags.append('BB upper')
        if bb_lo and abs(lv - bb_lo) < 0.01: tags.append('BB lower')
        return ' / '.join(tags) if tags else 'level'

    # ── LONG playbook ─────────────────────────────────────────────
    # Candidate breakout triggers (price must be below level)
    long_break_candidates = []
    if vwap and price < vwap:
        long_break_candidates.append((vwap + 0.02, f'VWAP reclaim — enter on first 1-min close above ${vwap + 0.02:.2f} (VWAP ${vwap:.2f}). Reclaiming VWAP flips intraday control back to buyers.'))
    if or_high and price < or_high:
        long_break_candidates.append((or_high + 0.02, f'ORB high break — enter on break above ${or_high + 0.02:.2f} (opening range high ${or_high:.2f}). Momentum buyers chase the ORB break.'))
    if pdh and price < pdh:
        long_break_candidates.append((pdh + 0.02, f'Prior-day high break — enter on break above ${pdh + 0.02:.2f}. Cleanest structural breakout level; attracts trend-follower flow.'))
    if sess_hi and price < sess_hi:
        long_break_candidates.append((sess_hi + 0.02, f'Session high break — enter on break above ${sess_hi + 0.02:.2f}. Breakout confirms bulls absorbed overhead supply.'))
    # Pick nearest breakout
    long_break = min(long_break_candidates, key=lambda x: x[0] - price) if long_break_candidates else None

    # Bounce-at-support fallback: if price is above most support, look for dip-buy level below
    long_bounce = None
    support_below = [lv for lv in [vwap, val, s1, or_low, pdl, sess_lo] if lv and lv < price]
    if support_below:
        nearest_sup = max(support_below)
        long_bounce = (round(nearest_sup + 0.02, 2),
                       f'Bounce long — enter on reclaim of ${nearest_sup + 0.02:.2f} after test of support (${nearest_sup:.2f}). Use when no clean breakout is near.')

    # Prefer breakout if close; otherwise bounce-at-support
    if long_break and (long_break[0] - price) <= far_threshold:
        long_trigger = round(long_break[0], 2); long_trigger_reason = long_break[1]
    elif long_bounce:
        long_trigger = long_bounce[0]; long_trigger_reason = long_bounce[1]
    elif long_break:
        long_trigger = round(long_break[0], 2); long_trigger_reason = long_break[1] + f' (note: >{far_threshold:.2f} from spot — patience required)'
    else:
        long_trigger = round(price + max(0.05, (atr or 0.10) * 0.3), 2)
        long_trigger_reason = 'No structural trigger visible — wait for a pullback to VWAP or a fresh consolidation.'

    long_stop = None; long_stop_note = None
    long_stop_candidates = []
    if sess_lo: long_stop_candidates.append((sess_lo, 'session low'))
    if vwap:    long_stop_candidates.append((vwap - atr * 0.3, f'VWAP - 0.3·ATR'))
    if val:     long_stop_candidates.append((val, 'VAL'))
    if or_low:  long_stop_candidates.append((or_low, 'ORB low'))
    long_stop_candidates = [(lv, n) for lv, n in long_stop_candidates if lv and lv < (long_trigger or price)]
    if long_stop_candidates:
        long_stop_candidates.sort(key=lambda x: -x[0])  # tightest first
        long_stop, long_stop_note = round(long_stop_candidates[0][0], 2), long_stop_candidates[0][1]

    long_target = None; long_target_note = None
    long_ups = upside_levels(long_trigger) if long_trigger else ups
    if long_ups:
        # Target floors: must be >= 0.5% of price AND R:R >= 1.5 above trigger.
        # Walk levels in ascending order and pick the first that clears both.
        # Fallback: highest available level if none qualify (preserves a
        # real technical anchor rather than inventing a synthetic number).
        pct_floor = 0.005 * (long_trigger or price or 0)
        risk = (long_trigger - long_stop) if (long_trigger and long_stop) else 0
        rr_floor = 1.5 * risk if risk > 0 else 0
        min_distance = max(pct_floor, rr_floor)
        qualified = [lv for lv in long_ups
                     if long_trigger and (lv - long_trigger) >= min_distance]
        if qualified:
            long_target = qualified[0]
        else:
            long_target = long_ups[-1]  # furthest level available
        long_target_note = label_of(long_target)

    long_rr = None
    if long_trigger and long_stop and long_target and (long_trigger - long_stop) > 0:
        long_rr = round((long_target - long_trigger) / (long_trigger - long_stop), 2)

    long_confirmations, long_risks = [], []
    if rsi is not None:
        if rsi >= 55: long_confirmations.append(f'RSI {rsi:.1f} — momentum supports longs')
        elif rsi <= 30: long_confirmations.append(f'RSI {rsi:.1f} oversold — reversion longs fire here')
        elif rsi >= 70: long_risks.append(f'RSI {rsi:.1f} overbought — chasing risk; prefer pullback entry')
        else: long_risks.append(f'RSI {rsi:.1f} — no momentum tailwind, setup is weaker')
    if ema9 and ema21:
        if ema9 > ema21: long_confirmations.append(f'EMA9 > EMA21 — short-term trend up')
        else: long_risks.append(f'EMA9 < EMA21 — short-term trend still down; wait for cross before going long')
    if latest.get('macd_line') is not None and latest.get('macd_signal') is not None:
        if latest['macd_line'] > latest['macd_signal']:
            long_confirmations.append('MACD above signal line — momentum bullish')
        else:
            long_risks.append('MACD below signal line — momentum still bearish')
    if poc and price:
        if price > poc: long_confirmations.append(f'Above POC ${poc:.2f} — trading in bullish half of value area')
        else: long_risks.append(f'Below POC ${poc:.2f} — sellers have value-area edge')
    if rvol and rvol >= 1.5: long_confirmations.append(f'RVOL {rvol:.1f}× — volume confirms participation')
    elif rvol and rvol < 0.8: long_risks.append(f'RVOL {rvol:.1f}× — low participation; breakouts often fail without volume')
    if latest.get('cvd_divergence') == 'bearish_div':
        long_risks.append('CVD bearish divergence — price rising but tape is selling; longs fighting order flow')
    if btc:
        bp = btc.get('btc_pct_change', 0) or 0
        if bp >= 1.5: long_confirmations.append(f'BTC +{bp:.1f}% — macro tailwind')
        elif bp <= -1.5: long_risks.append(f'BTC {bp:.1f}% — macro headwind; longs need extra confirmation')
    if latest.get('consec_green', 0) >= 4:
        long_risks.append(f'{latest["consec_green"]} consecutive green bars — exhaustion risk; avoid chasing')

    # ── SHORT playbook ────────────────────────────────────────────
    short_break_candidates = []
    if vwap and price > vwap:
        short_break_candidates.append((vwap - 0.02, f'VWAP loss — enter on first 1-min close below ${vwap - 0.02:.2f} (VWAP ${vwap:.2f}). Losing VWAP flips intraday control to sellers.'))
    if or_low and price > or_low:
        short_break_candidates.append((or_low - 0.02, f'ORB low break — enter on break below ${or_low - 0.02:.2f} (opening range low ${or_low:.2f}).'))
    if pdl and price > pdl:
        short_break_candidates.append((pdl - 0.02, f'Prior-day low break — enter on break below ${pdl - 0.02:.2f}. Structural breakdown; attracts trend-follower shorts.'))
    if sess_lo and price > sess_lo:
        short_break_candidates.append((sess_lo - 0.02, f'Session low break — enter on break below ${sess_lo - 0.02:.2f}.'))
    short_break = min(short_break_candidates, key=lambda x: price - x[0]) if short_break_candidates else None

    # Fade-at-resistance fallback: short on rejection of nearest resistance above
    short_fade = None
    resistance_above = [lv for lv in [vwap, vah, r1, or_high, pdh, sess_hi, bb_up] if lv and lv > price]
    if resistance_above:
        nearest_res = min(resistance_above)
        short_fade = (round(nearest_res - 0.02, 2),
                      f'Fade short — enter on rejection of ${nearest_res:.2f} (short below ${nearest_res - 0.02:.2f} after a failed test). Use when no clean breakdown is near.')

    if short_break and (price - short_break[0]) <= far_threshold:
        short_trigger = round(short_break[0], 2); short_trigger_reason = short_break[1]
    elif short_fade:
        short_trigger = short_fade[0]; short_trigger_reason = short_fade[1]
    elif short_break:
        short_trigger = round(short_break[0], 2); short_trigger_reason = short_break[1] + f' (note: >{far_threshold:.2f} from spot — patience required)'
    else:
        short_trigger = round(price - max(0.05, (atr or 0.10) * 0.3), 2)
        short_trigger_reason = 'No structural trigger visible — wait for rally to VWAP or fade at session high rejection.'

    short_stop = None; short_stop_note = None
    short_stop_candidates = []
    if sess_hi: short_stop_candidates.append((sess_hi, 'session high'))
    if vwap:    short_stop_candidates.append((vwap + atr * 0.3, 'VWAP + 0.3·ATR'))
    if vah:     short_stop_candidates.append((vah, 'VAH'))
    if or_high: short_stop_candidates.append((or_high, 'ORB high'))
    short_stop_candidates = [(lv, n) for lv, n in short_stop_candidates if lv and lv > (short_trigger or price)]
    if short_stop_candidates:
        short_stop_candidates.sort(key=lambda x: x[0])  # tightest first
        short_stop, short_stop_note = round(short_stop_candidates[0][0], 2), short_stop_candidates[0][1]

    short_target = None; short_target_note = None
    # downside_levels returns sorted descending (nearest-below first); walk from
    # nearest outward and pick the first that clears both floors.
    short_downs = downside_levels(short_trigger) if short_trigger else downs
    if short_downs:
        pct_floor = 0.005 * (short_trigger or price or 0)
        risk = (short_stop - short_trigger) if (short_trigger and short_stop) else 0
        rr_floor = 1.5 * risk if risk > 0 else 0
        min_distance = max(pct_floor, rr_floor)
        qualified = [lv for lv in short_downs
                     if short_trigger and (short_trigger - lv) >= min_distance]
        if qualified:
            short_target = qualified[0]
        else:
            short_target = short_downs[-1]  # furthest level available
        short_target_note = label_of(short_target)

    short_rr = None
    if short_trigger and short_stop and short_target and (short_stop - short_trigger) > 0:
        short_rr = round((short_trigger - short_target) / (short_stop - short_trigger), 2)

    short_confirmations, short_risks = [], []
    if rsi is not None:
        if rsi >= 70: short_confirmations.append(f'RSI {rsi:.1f} overbought — reversal zone')
        elif rsi <= 45: short_confirmations.append(f'RSI {rsi:.1f} — momentum favors shorts')
        elif rsi >= 55: short_risks.append(f'RSI {rsi:.1f} — momentum still with longs; shorts fighting the tape')
    if ema9 and ema21:
        if ema9 < ema21: short_confirmations.append('EMA9 < EMA21 — short-term trend down')
        else: short_risks.append('EMA9 > EMA21 — short-term trend still up; wait for bearish cross')
    if latest.get('macd_line') is not None and latest.get('macd_signal') is not None:
        if latest['macd_line'] < latest['macd_signal']:
            short_confirmations.append('MACD below signal line — momentum bearish')
        else:
            short_risks.append('MACD above signal line — momentum still bullish')
    if poc and price:
        if price < poc: short_confirmations.append(f'Below POC ${poc:.2f} — in bearish half of value area')
        else: short_risks.append(f'Above POC ${poc:.2f} — buyers have value-area edge')
    if rvol and rvol >= 1.5 and (latest.get('last5_pct') or 0) > 0.3:
        short_confirmations.append(f'RVOL {rvol:.1f}× into a rally — look for climax / blow-off fade')
    if latest.get('vol_climax_ratio') and latest['vol_climax_ratio'] >= 3.0:
        short_confirmations.append(f'Volume climax ({latest["vol_climax_ratio"]:.1f}× avg) — often marks exhaustion')
    if latest.get('cvd_divergence') == 'bearish_div':
        short_confirmations.append('CVD bearish divergence — sellers absorbing the rally')
    if btc:
        bp = btc.get('btc_pct_change', 0) or 0
        if bp <= -1.5: short_confirmations.append(f'BTC {bp:.1f}% — macro tailwind for shorts')
        elif bp >= 1.5: short_risks.append(f'BTC +{bp:.1f}% — macro headwind; shorts need extra confirmation')
    if latest.get('consec_red', 0) >= 4:
        short_risks.append(f'{latest["consec_red"]} consecutive red bars — bounce risk; avoid chasing')

    return {
        'long': {
            'trigger': long_trigger,
            'trigger_reason': long_trigger_reason,
            'stop': long_stop,
            'stop_note': long_stop_note,
            'target': long_target,
            'target_note': long_target_note,
            'rr': long_rr,
            'confirmations': long_confirmations,
            'risks': long_risks,
        },
        'short': {
            'trigger': short_trigger,
            'trigger_reason': short_trigger_reason,
            'stop': short_stop,
            'stop_note': short_stop_note,
            'target': short_target,
            'target_note': short_target_note,
            'rr': short_rr,
            'confirmations': short_confirmations,
            'risks': short_risks,
        },
    }


# Empirically measured overnight-gap betas vs BTC (180-day OLS regression).
# Computed via /compute_beta endpoint. Rerun periodically (quarterly) as betas drift.
# Values: beta, R² (strength of fit). Tickers not in this map use DEFAULT_MINER_BETA.
TICKER_BTC_BETAS = {
    # Auto-refitted daily by refit_betas.py. Asymmetric β — miners react differently to BTC rallies vs sells.
    'CLSK': {'beta': 1.066, 'beta_up': 1.117, 'beta_down': 1.02, 'r2': 0.702},  # n=124, refit 2026-04-20
    'MARA': {'beta': 0.967, 'beta_up': 1.09, 'beta_down': 0.855, 'r2': 0.551},  # n=124, refit 2026-04-20
}
DEFAULT_MINER_BETA = 1.20  # fallback for other miners until measured
HIGH_BTC_CORRELATION = {'CLSK', 'MARA', 'RIOT', 'HUT', 'BITF', 'CIFR', 'CORZ', 'IREN', 'BTDR', 'WULF'}


def _fmt_price(value):
    if value is None:
        return 'N/A'
    try:
        return f"${float(value):.2f}"
    except Exception:
        return str(value)


def _fmt_pct(value):
    if value is None:
        return 'N/A'
    try:
        return f"{float(value):+.2f}%"
    except Exception:
        return str(value)


def _plain_signal_read(ticker, latest, analysis, daily_ctx=None, btc=None, btc_intraday=None):
    """Human-first explanation of the same indicator data used by the Analyzer."""
    price = latest.get('price')
    decision = analysis.get('decision')
    confidence = analysis.get('confidence')
    score = analysis.get('score')
    forced = analysis.get('forced_side')
    forced_conv = analysis.get('forced_conviction')

    if decision == 'LONG':
        headline = f"{ticker} leans upward right now, but treat the strength as {confidence.lower()}."
        summary = (
            f"The current read favors buyers at about {_fmt_price(price)}. "
            "That does not mean the stock must go up; it means the evidence is currently better for an upside move than a downside move."
        )
    elif decision == 'SHORT':
        headline = f"{ticker} leans downward right now, with {confidence.lower()} confidence."
        summary = (
            f"The current read favors sellers at about {_fmt_price(price)}. "
            "For a newer trader, that usually means avoid buying here unless the stock quickly recovers key levels."
        )
    else:
        headline = f"{ticker} does not have a clean edge right now."
        summary = (
            f"At about {_fmt_price(price)}, the evidence is mixed. "
            f"If forced to choose, the weaker directional lean is {forced} with {str(forced_conv).lower()} conviction, but patience is the cleaner decision."
        )

    rows = []

    rsi = latest.get('rsi')
    if rsi is not None:
        if rsi >= 70:
            meaning = "The stock has been pushed up hard enough that late buyers may be chasing."
            bias = "bear"
        elif rsi >= 55:
            meaning = "Buyers have control of recent movement, but it is not yet extremely stretched."
            bias = "bull"
        elif rsi <= 30:
            meaning = "Selling has been heavy enough that a bounce can happen, even if the larger trend is still weak."
            bias = "bull"
        elif rsi <= 45:
            meaning = "Sellers have control of recent movement, so quick bounces may fail."
            bias = "bear"
        else:
            meaning = "Momentum is balanced; RSI is not giving a strong reason by itself."
            bias = "neutral"
        rows.append({
            'label': 'RSI',
            'value': f"{rsi:.1f}",
            'bias': bias,
            'meaning': meaning,
            'plain': 'RSI is a speedometer for recent buying vs selling pressure.',
        })

    ema9 = latest.get('ema9')
    ema21 = latest.get('ema21')
    if ema9 is not None and ema21 is not None:
        up = ema9 > ema21
        rows.append({
            'label': 'Short-term trend',
            'value': f"EMA9 {_fmt_price(ema9)} / EMA21 {_fmt_price(ema21)}",
            'bias': 'bull' if up else 'bear',
            'meaning': (
                "The faster average is above the slower one, so the short-term path is rising."
                if up else
                "The faster average is below the slower one, so the short-term path is slipping."
            ),
            'plain': 'EMA lines smooth out price so you can see whether the recent path is tilting up or down.',
        })

    vwap = latest.get('vwap')
    if vwap is not None and price is not None:
        above = price > vwap
        dist = abs(float(price) - float(vwap))
        rows.append({
            'label': 'VWAP',
            'value': _fmt_price(vwap),
            'bias': 'bull' if above else 'bear',
            'meaning': (
                f"Price is ${dist:.2f} above the day's average traded price, which means buyers are paying up."
                if above else
                f"Price is ${dist:.2f} below the day's average traded price, which means sellers have the upper hand."
            ),
            'plain': 'VWAP is the average price where the stock has traded today, weighted by volume.',
        })

    macd_h = latest.get('macd_histogram')
    macd_prev = latest.get('macd_prev_histogram')
    if macd_h is not None:
        if macd_h > 0 and (macd_prev is None or macd_h >= macd_prev):
            bias = 'bull'
            meaning = "Momentum is improving; the recent push upward is still building."
        elif macd_h > 0:
            bias = 'neutral'
            meaning = "Momentum is still positive, but it is cooling off."
        elif macd_h < 0 and (macd_prev is None or macd_h <= macd_prev):
            bias = 'bear'
            meaning = "Momentum is worsening; sellers are pressing harder."
        else:
            bias = 'neutral'
            meaning = "Momentum is still negative, but the selling pressure is easing."
        rows.append({
            'label': 'MACD',
            'value': f"{macd_h:.4f}",
            'bias': bias,
            'meaning': meaning,
            'plain': 'MACD compares faster and slower momentum to show whether a move is gaining or losing energy.',
        })

    rvol = latest.get('rvol')
    if rvol is not None:
        if rvol >= 1.5:
            meaning = "More people are participating than usual, so moves are more likely to matter."
            bias = 'neutral'
        elif rvol < 0.7:
            meaning = "Volume is light, so price moves are easier to fake out."
            bias = 'watch'
        else:
            meaning = "Volume is normal; it is not strongly confirming or rejecting the move."
            bias = 'neutral'
        rows.append({
            'label': 'Volume',
            'value': f"{rvol}x normal",
            'bias': bias,
            'meaning': meaning,
            'plain': 'Relative volume compares today\'s activity with normal activity for this point in the session.',
        })

    pivot = latest.get('pivot')
    if pivot is not None and price is not None:
        rows.append({
            'label': 'Daily pivot',
            'value': _fmt_price(pivot),
            'bias': 'bull' if price >= pivot else 'bear',
            'meaning': (
                "Price is above a common reference level, so the day has a slightly stronger tone."
                if price >= pivot else
                "Price is below a common reference level, so rallies may run into resistance."
            ),
            'plain': 'The pivot is a simple level built from yesterday\'s high, low, and close.',
        })

    poc = latest.get('poc')
    if poc is not None and price is not None:
        rows.append({
            'label': 'Volume profile',
            'value': f"POC {_fmt_price(poc)}",
            'bias': 'bull' if price >= poc else 'bear',
            'meaning': (
                "Price is above the busiest traded price of the day, which means buyers are holding value."
                if price >= poc else
                "Price is below the busiest traded price of the day, which means that busy area may act like a ceiling."
            ),
            'plain': 'POC is the price where the most shares traded today.',
        })

    cvd = latest.get('cvd_value')
    cvd_div = latest.get('cvd_divergence')
    if cvd is not None:
        if cvd_div == 'bear':
            bias = 'bear'
            meaning = "Price is rising while buying pressure is weakening. That is an early warning sign."
        elif cvd_div == 'bull':
            bias = 'bull'
            meaning = "Price is falling while buyers are quietly stepping in. That can precede a bounce."
        elif cvd > 0:
            bias = 'bull'
            meaning = "More aggressive buying than selling has shown up today."
        else:
            bias = 'bear'
            meaning = "More aggressive selling than buying has shown up today."
        rows.append({
            'label': 'Buying vs selling pressure',
            'value': f"{cvd:+,.0f}",
            'bias': bias,
            'meaning': meaning,
            'plain': 'CVD estimates whether trades are mostly hitting the ask (buying) or bid (selling).',
        })

    watch = []
    if analysis.get('entry') is not None:
        watch.append(f"Planned entry area: {_fmt_price(analysis.get('entry'))}.")
    if analysis.get('stop') is not None:
        watch.append(f"Invalidation/stop area: {_fmt_price(analysis.get('stop'))}. If price gets there, this read is probably wrong.")
    if analysis.get('target') is not None:
        watch.append(f"First target area: {_fmt_price(analysis.get('target'))}.")
    if decision == 'FLAT':
        watch.append("A cleaner trade would need price to reclaim or reject VWAP with stronger volume.")
    elif decision == 'LONG':
        watch.append("For the long idea to stay healthy, price should hold above VWAP or quickly reclaim it after a dip.")
    elif decision == 'SHORT':
        watch.append("For the short idea to stay healthy, price should stay below VWAP or fail quickly if it bounces into it.")

    if btc:
        btc_bits = []
        if btc.get('btc_pct_change') is not None:
            btc_bits.append(f"BTC move since stock close: {_fmt_pct(btc.get('btc_pct_change'))}")
        if analysis.get('btc_adj'):
            btc_bits.append(f"BTC adjustment to score: {analysis.get('btc_adj'):+.2f}")
        if btc_intraday:
            btc_bits.append(f"BTC intraday bias: {btc_intraday.get('bias')} ({btc_intraday.get('score'):+.1f})")
        if btc_bits:
            watch.append("Bitcoin context: " + "; ".join(btc_bits) + ".")

    return {
        'headline': headline,
        'summary': summary,
        'rows': rows,
        'watch': watch,
        'score_label': f"Engine score {score:+.1f}" if score is not None else None,
    }


def get_btc_beta(ticker, btc_move_pct=None):
    """
    Return empirical β for a BTC-correlated ticker. If btc_move_pct is supplied
    and per-direction betas exist, return the asymmetric β for that direction.
    """
    t = ticker.upper()
    if t in TICKER_BTC_BETAS:
        rec = TICKER_BTC_BETAS[t]
        if btc_move_pct is not None and abs(btc_move_pct) >= 0.3:
            if btc_move_pct > 0 and 'beta_up' in rec:
                return rec['beta_up']
            if btc_move_pct < 0 and 'beta_down' in rec:
                return rec['beta_down']
        return rec['beta']
    if t in HIGH_BTC_CORRELATION:
        return DEFAULT_MINER_BETA
    return None


def rule_based_analysis(ticker, latest, daily_ctx=None, btc=None, btc_intraday=None):
    price  = latest['price']
    rsi    = latest['rsi']
    ema9   = latest['ema9']
    ema21  = latest['ema21']
    vwap   = latest['vwap']
    vwap_u1 = latest.get('vwap_upper1')
    vwap_l1 = latest.get('vwap_lower1')
    vwap_u2 = latest.get('vwap_upper2')
    vwap_l2 = latest.get('vwap_lower2')
    rvol   = latest['rvol']
    atr    = latest['atr']
    pm_high = latest['premarket_high']
    pm_low  = latest['premarket_low']
    or_high = latest['or_high']
    or_low  = latest['or_low']
    macd_h  = latest.get('macd_histogram')
    macd_h_prev = latest.get('macd_prev_histogram')
    pivot  = latest.get('pivot')
    r1 = latest.get('r1');  r2 = latest.get('r2')
    s1 = latest.get('s1');  s2 = latest.get('s2')
    prev_close = latest.get('prev_close')
    poc = latest.get('poc'); vah = latest.get('vah'); val = latest.get('val')
    cvd_val = latest.get('cvd_value'); cvd_div = latest.get('cvd_divergence')

    score = 0.0       # positive = bullish, negative = bearish
    sentences = []    # narrative sentences

    # ── RSI ───────────────────────────────────────────────────────────────
    if rsi is not None:
        if rsi < 30:
            score += 2
            sentences.append(
                f"RSI at {rsi} has fallen into oversold territory — sellers are exhausted "
                f"and a mean-reversion bounce is statistically likely; watch for price to "
                f"stabilize and reclaim VWAP before entering long")
        elif rsi > 70:
            score -= 2
            sentences.append(
                f"RSI at {rsi} is overbought — buyers are overextended and a pullback or "
                f"reversal is likely; chasing here is high-risk")
        elif rsi > 55:
            score += 1
            sentences.append(
                f"RSI at {rsi} is in bullish momentum territory with room to run before "
                f"reaching overbought; buyers currently have the edge on the 5-min")
        elif rsi < 45:
            score -= 1
            sentences.append(
                f"RSI at {rsi} is in bearish momentum territory — sellers have the edge; "
                f"bounces are likely to be weak and short-lived")
        else:
            sentences.append(
                f"RSI at {rsi} is neutral, sitting in the 45–55 no-man's-land — "
                f"no directional momentum edge from RSI alone right now")

    # ── EMA 9 / 21 ────────────────────────────────────────────────────────
    ema_diff = round(abs(ema9 - ema21), 2)
    if ema9 > ema21:
        score += 1
        sentences.append(
            f"EMA 9 (${ema9:.2f}) is ${ema_diff} above EMA 21 (${ema21:.2f}), "
            f"confirming the 5-min intraday trend is bullish — pullbacks to EMA 9 are buy "
            f"opportunities as long as EMA 9 holds above EMA 21")
    else:
        score -= 1
        sentences.append(
            f"EMA 9 (${ema9:.2f}) is ${ema_diff} below EMA 21 (${ema21:.2f}), "
            f"confirming the 5-min intraday trend is bearish — bounces to EMA 9 are short "
            f"opportunities; a cross above EMA 21 would flip the bias")

    # ── VWAP ──────────────────────────────────────────────────────────────
    if vwap:
        vd = round(abs(price - vwap), 2)
        if vwap_u2 and price >= vwap_u2:
            score -= 2
            sentences.append(
                f"Price has reached VWAP +2σ (${vwap_u2:.2f}) — a statistically rare "
                f"extreme extension; mean reversion back toward VWAP (${vwap:.2f}) is the "
                f"high-probability outcome; this is a dangerous spot to initiate longs")
        elif vwap_l2 and price <= vwap_l2:
            score += 2
            sentences.append(
                f"Price has fallen to VWAP −2σ (${vwap_l2:.2f}) — a statistically rare "
                f"oversold extreme; a bounce back toward VWAP (${vwap:.2f}) is the "
                f"high-probability outcome here")
        elif vwap_u1 and price >= vwap_u1:
            score -= 0.5
            sentences.append(
                f"Price is near VWAP +1σ (${vwap_u1:.2f}), a mild resistance zone where "
                f"momentum may slow; watch for a rejection back toward VWAP (${vwap:.2f})")
        elif vwap_l1 and price <= vwap_l1:
            score += 0.5
            sentences.append(
                f"Price is near VWAP −1σ (${vwap_l1:.2f}), a mild support zone where "
                f"buyers may step in; a hold here could produce a bounce back to VWAP (${vwap:.2f})")
        elif price > vwap:
            score += 1
            sentences.append(
                f"Price is ${vd} above VWAP (${vwap:.2f}) — buyers are in control of the "
                f"session; VWAP is the key support to defend; a pullback to VWAP that holds "
                f"is a long entry, a break below it flips the intraday bias bearish")
        else:
            score -= 1
            sentences.append(
                f"Price is ${vd} below VWAP (${vwap:.2f}) — sellers have controlled the "
                f"session; VWAP is now overhead resistance; a bounce to VWAP that gets "
                f"rejected is a short entry")

    # ── MACD ──────────────────────────────────────────────────────────────
    if macd_h is not None:
        just_bull = macd_h_prev is not None and macd_h_prev < 0 <= macd_h
        just_bear = macd_h_prev is not None and macd_h_prev > 0 >= macd_h
        expanding = macd_h_prev is not None and abs(macd_h) > abs(macd_h_prev)

        if just_bull:
            score += 2
            sentences.append(
                f"MACD histogram just crossed from negative ({macd_h_prev:.4f}) to positive "
                f"({macd_h:.4f}) — a bullish crossover has fired on the 5-min; momentum has "
                f"definitively shifted from sellers to buyers and this is a high-conviction "
                f"entry signal when combined with price holding above VWAP")
        elif just_bear:
            score -= 2
            sentences.append(
                f"MACD histogram just crossed from positive ({macd_h_prev:.4f}) to negative "
                f"({macd_h:.4f}) — a bearish crossover on the 5-min; momentum has shifted "
                f"from buyers to sellers, making this a short-entry signal")
        elif macd_h > 0 and expanding:
            score += 1.5
            sentences.append(
                f"MACD histogram is positive ({macd_h:.4f}) and expanding — bullish momentum "
                f"is accelerating, not fading; buyers have conviction and dips are likely to "
                f"be shallow and brief")
        elif macd_h > 0:
            score += 0.5
            sentences.append(
                f"MACD histogram is positive ({macd_h:.4f}) but shrinking — the uptrend "
                f"exists but momentum is fading; tighten stops if long and watch for a "
                f"potential bearish crossover")
        elif macd_h < 0 and expanding:
            score -= 1.5
            sentences.append(
                f"MACD histogram is negative ({macd_h:.4f}) and more negative than the prior "
                f"bar — bearish momentum is accelerating; sellers have conviction and bounces "
                f"are likely to be brief")
        else:
            score -= 0.5
            sentences.append(
                f"MACD histogram is negative ({macd_h:.4f}) but shrinking — bearish momentum "
                f"is fading; a bullish crossover may be forming but is not confirmed yet")

    # ── RVOL ──────────────────────────────────────────────────────────────
    if rvol is not None:
        if rvol >= 2.0:
            if vwap and price >= vwap:
                score += 1
                sentences.append(
                    f"Volume is running at {rvol}x the daily average — this is a "
                    f"high-participation up move; elevated volume on breakouts dramatically "
                    f"increases follow-through probability")
            else:
                score -= 1
                sentences.append(
                    f"Volume is running at {rvol}x the daily average on a down move — heavy "
                    f"institutional selling pressure; this is not a thin-market drift lower, "
                    f"it has real conviction behind it")
        elif rvol < 0.6:
            sentences.append(
                f"Volume is thin at only {rvol}x normal — low participation means any "
                f"breakout or breakdown is more likely to be a false move; be skeptical of "
                f"signals until volume confirms")
        else:
            sentences.append(
                f"Volume is running at {rvol}x normal — average participation; "
                f"setups are technically valid but don't expect explosive follow-through")

    # ── Pivot levels ──────────────────────────────────────────────────────
    if pivot:
        if r2 and price >= r2:
            score -= 1.5
            sentences.append(
                f"Price has pushed all the way to R2 (${r2}) — this is a full-range up day "
                f"relative to yesterday's session; the risk/reward of adding longs here is "
                f"very poor; R2 is a high-probability reversal zone")
        elif r1 and price >= r1:
            score -= 1
            sentences.append(
                f"Price is testing R1 (${r1}), the first resistance level derived from "
                f"yesterday's range — many algorithmic systems place sell orders here; "
                f"expect supply; a clean break with volume would target R2 (${r2}), "
                f"but a rejection is the more common outcome")
        elif s2 and price <= s2:
            score += 1.5
            sentences.append(
                f"Price has fallen to S2 (${s2}) — an extended sell-off for the session; "
                f"statistically oversold relative to yesterday's range; high bounce probability "
                f"back toward S1 (${s1}) or the pivot (${pivot})")
        elif s1 and price <= s1:
            score += 1
            sentences.append(
                f"Price is at S1 (${s1}), the first support level from yesterday's range — "
                f"institutional algos are programmed to defend this level; a bounce back "
                f"toward the pivot (${pivot}) is the base case; a break below S1 on volume "
                f"shifts the target to S2 (${s2})")
        else:
            above = price >= pivot
            score += 0.5 if above else -0.5
            sentences.append(
                f"Price is {'above' if above else 'below'} the daily pivot (${pivot}) — "
                f"{'bullish session bias; the pivot is support and a pullback to it that holds is a long entry' if above else 'bearish session bias; the pivot is overhead resistance and a bounce to it that fails is a short entry'}")

    # ── Opening range breakout ────────────────────────────────────────────
    if or_high is not None and or_low is not None:
        if price > or_high:
            score += 1.5
            sentences.append(
                f"Price broke above the 15-min opening range high (${or_high}) — the ORB "
                f"long is one of the most statistically reliable intraday setups; "
                f"${or_high} is now support; the next targets are PM high "
                + (f"(${pm_high})" if pm_high else "") +
                f" and R1 (${r1})" if r1 else "")
        elif price < or_low:
            score -= 1.5
            sentences.append(
                f"Price broke below the 15-min opening range low (${or_low}) — ORB short "
                f"setup is active; ${or_low} is now resistance; targets are PM low "
                + (f"(${pm_low})" if pm_low else "") +
                f" and S1 (${s1})" if s1 else "")
        else:
            sentences.append(
                f"Price is inside the opening range (${or_low}–${or_high}) — no ORB setup "
                f"has triggered yet; wait for a high-volume break of either end before "
                f"committing to a direction")

    # ── Premarket levels ──────────────────────────────────────────────────
    if pm_high and price > pm_high:
        score += 1
        sentences.append(
            f"Price has cleared the premarket high (${pm_high}), absorbing all premarket "
            f"supply — bullish continuation signal; ${pm_high} is now support on pullbacks")
    elif pm_low and price < pm_low:
        score -= 1
        sentences.append(
            f"Price broke below the premarket low (${pm_low}), meaning even premarket "
            f"buyers are underwater — capitulation signal; ${pm_low} is now resistance")

    # ── Daily trend ───────────────────────────────────────────────────────
    if daily_ctx:
        trend = daily_ctx['daily_trend']
        p5 = daily_ctx.get('pct_5d')
        pct_str = f" ({p5:+.1f}% over the past 5 days)" if p5 else ""
        if trend == 'bullish':
            score += 0.5
            sentences.append(
                f"The daily chart is in an uptrend{pct_str} — intraday longs are aligned "
                f"with the higher-timeframe bias, which increases the probability of follow-through")
        else:
            score -= 0.5
            sentences.append(
                f"The daily chart is in a downtrend{pct_str} — the macro backdrop is a "
                f"headwind for longs; short setups on the 5-min have higher-timeframe backing "
                f"and should be favored when signals are mixed")

    # ── Volume Profile: Point of Control ─────────────────────────────────
    if poc:
        if price > poc:
            score += 0.5
            sentences.append(
                f"Price (${price}) is above the session Point of Control (${poc:.2f}), the "
                f"highest-volume price level of the day — buyers are controlling value; POC acts "
                f"as dynamic support on pullbacks, derived from actual volume activity rather than formulas "
                f"(Value Area: ${val:.2f}–${vah:.2f})")
        elif price < poc:
            score -= 0.5
            sentences.append(
                f"Price (${price}) is below the session Point of Control (${poc:.2f}) — sellers "
                f"are controlling value; POC is overhead resistance; a reclaim of ${poc:.2f} on "
                f"volume would shift the intraday balance back to buyers "
                f"(Value Area: ${val:.2f}–${vah:.2f})")
        else:
            sentences.append(
                f"Price is sitting at the Point of Control (${poc:.2f}), the highest-volume price "
                f"of the session — expect two-sided chop and indecision here; both sides are equally "
                f"active; a decisive break with volume in either direction is the trade")

    # ── Cumulative Volume Delta ───────────────────────────────────────────
    if cvd_val is not None:
        if cvd_div == 'bear':
            score -= 1.5
            sentences.append(
                f"CVD DIVERGENCE WARNING: Price is trending up but Cumulative Volume Delta is "
                f"falling — sellers are absorbing the buying pressure behind the scenes; this is "
                f"one of the most reliable early warnings of an intraday top; be very cautious "
                f"adding to longs here")
        elif cvd_div == 'bull':
            score += 1.5
            sentences.append(
                f"CVD DIVERGENCE: Price is trending down but Cumulative Volume Delta is rising — "
                f"buyers are quietly stepping in despite surface-level weakness; high-probability "
                f"bounce signal; look for a price stabilization and VWAP reclaim to confirm")
        elif cvd_val > 0:
            score += 0.5
            sentences.append(
                f"Cumulative Volume Delta is positive ({cvd_val:+,.0f}) — net buying aggression "
                f"has dominated the session; the underlying order flow confirms the bullish bias "
                f"and adds conviction to long setups")
        else:
            score -= 0.5
            sentences.append(
                f"Cumulative Volume Delta is negative ({cvd_val:+,.0f}) — net selling aggression "
                f"has dominated the session; the underlying order flow is bearish regardless of "
                f"where price sits; be cautious with longs")

    # ── BTC score adjustment (applied before decision) ───────────────────
    # When btc_intraday is present, it drives the adjustment. Otherwise fall back
    # to the legacy macro (24h % + 5-day trend) scoring.
    btc_adj = 0.0
    high_corr = ticker.upper() in HIGH_BTC_CORRELATION
    market_closed = bool(btc and 'intraday' not in (btc.get('context_label') or ''))

    if btc_intraday and btc:
        # Scale intraday BTC score (-10..+10) into an adjustment. Heavy weight for
        # high-correlation names (CLSK, MARA, etc.), moderate otherwise.
        weight = 0.75 if high_corr else 0.40
        btc_adj = round(btc_intraday['score'] * weight, 2)
        # Blend in a small portion of the 24h macro move so gap moves still register
        if btc.get('btc_pct_change') is not None:
            btc_adj += max(-1.5, min(1.5, btc['btc_pct_change'] * 0.25))
        btc_t5 = btc.get('btc_trend_5d')
        if btc_t5 is not None:
            if btc_t5 <= -5:   btc_adj -= 0.5
            elif btc_t5 >= 5:  btc_adj += 0.5
    elif btc:
        btc_pct = btc['btc_pct_change']
        btc_t5  = btc.get('btc_trend_5d')
        if btc_pct >= 5:      btc_adj =  5.0
        elif btc_pct >= 3:    btc_adj =  3.0
        elif btc_pct >= 1:    btc_adj =  1.5
        elif btc_pct >= 0.5:  btc_adj =  0.5
        elif btc_pct > -0.5:  btc_adj =  0.0
        elif btc_pct > -1:    btc_adj = -1.5
        elif btc_pct > -2:    btc_adj = -2.5
        elif btc_pct > -4:    btc_adj = -5.0
        else:                  btc_adj = -6.5
        if btc_t5 is not None:
            if btc_t5 <= -5:   btc_adj -= 0.5
            elif btc_t5 >= 5:  btc_adj += 0.5

    # ── Gap Prediction (high-corr miners, market closed) ──────────────────
    # CLSK/MARA trade with a beta of ~2.5 to BTC — every 1% BTC move translates
    # roughly to a 2.5% move on the stock at Monday's open. This dominates stale
    # Friday technicals, so when market is closed we both:
    #  (1) downweight stock indicators (they're frozen in time), and
    #  (2) add a heavy gap adjustment based on BTC since the prior close.
    expected_gap_pct = None
    predicted_open_price = None
    beta_used = None
    gap_adj = 0.0
    if market_closed and btc and high_corr and btc.get('btc_pct_change') is not None:
        btc_move = btc['btc_pct_change']
        # Asymmetric β: CLSK reacts harder to rallies (1.26) than sells (0.92).
        beta_used = get_btc_beta(ticker, btc_move) or DEFAULT_MINER_BETA
        raw_predicted = btc_move * beta_used
        # Weekend bias: only on Mondays, and only when BTC didn't already move big.
        # Big weekend BTC moves already price in risk-on; additional +1.1% over-fires.
        is_weekend = 'weekend' in (btc.get('context_label') or '')
        if is_weekend and abs(btc_move) < 2.0:
            weekend_bias = 1.1
        elif is_weekend:
            # Taper: small residual bias for moderate moves, zero past ±4%
            weekend_bias = max(0.0, 1.1 * (1 - (abs(btc_move) - 2) / 2))
        else:
            weekend_bias = 0.0
        expected_gap_pct = round(raw_predicted + weekend_bias, 2)
        # Anchor to prev_close (last completed stock session), not current premarket price.
        # BTC move since last close × β projects where stock "should" open relative to that close.
        anchor_px = prev_close if prev_close else price
        if anchor_px:
            predicted_open_price = round(anchor_px * (1 + expected_gap_pct / 100.0), 2)
        g = expected_gap_pct
        if   g <= -5:   gap_adj = -7.0
        elif g <= -3:   gap_adj = -5.0
        elif g <= -1.5: gap_adj = -3.0
        elif g <= -0.5: gap_adj = -1.5
        elif g >=  5:   gap_adj =  7.0
        elif g >=  3:   gap_adj =  5.0
        elif g >=  1.5: gap_adj =  3.0
        elif g >=  0.5: gap_adj =  1.5

    # ── BTC-Correlated Implied Stock Price (always computed for high-corr miners) ──
    # Uses beta to project what the stock price would be if it perfectly tracked
    # BTC's % move since yesterday's close. Divergence from actual price reveals
    # whether the stock is lagging/leading BTC.
    btc_implied_price = None
    btc_implied_beta = None
    btc_current_px = None
    btc_pct_now = None
    if high_corr and btc and prev_close and btc.get('btc_pct_change') is not None:
        btc_pct_now = btc['btc_pct_change']
        btc_current_px = btc.get('btc_current')
        btc_implied_beta = get_btc_beta(ticker, btc_pct_now) or DEFAULT_MINER_BETA
        btc_implied_price = round(prev_close * (1 + btc_implied_beta * btc_pct_now / 100.0), 2)

    stock_score = score
    # Downweight stale stock technicals when the market is closed
    effective_stock = score * (0.3 if market_closed else 1.0)
    final_score = round(effective_stock + btc_adj + gap_adj, 1)

    # ── Forced Side (always LONG or SHORT — ignores the flat zone) ───────
    # Uses the same final_score but eliminates the ±1 neutral zone. Ties break
    # toward the stock's raw bias; if that's flat too, default to SHORT (since
    # low-conviction longs have the worst expected value in chop).
    forced_margin = abs(final_score)
    if final_score > 0:
        forced_side = 'LONG'
    elif final_score < 0:
        forced_side = 'SHORT'
    elif stock_score > 0:
        forced_side = 'LONG'
    elif stock_score < 0:
        forced_side = 'SHORT'
    else:
        forced_side = 'SHORT'
    # Thresholds align with the 0-10 confidence_score scale:
    #   |final_score| * 2, clamped [0,10]  →  HIGH≥8, MEDIUM 5-7, LOW 2-4.
    if forced_margin >= 4: forced_conviction = 'HIGH'
    elif forced_margin >= 2.5: forced_conviction = 'MEDIUM'
    elif forced_margin >= 1: forced_conviction = 'LOW'
    else: forced_conviction = 'VERY LOW'

    # Build forced-side reasoning
    forced_reasons = []
    if expected_gap_pct is not None and abs(expected_gap_pct) >= 0.5:
        direction = 'down' if expected_gap_pct < 0 else 'up'
        open_txt = f" to ~${predicted_open_price:.2f}" if predicted_open_price else ''
        is_weekend = 'weekend' in (btc.get('context_label') or '')
        bias_txt = ' +1.1% Mon-open bias' if is_weekend else ''
        forced_reasons.append(
            f"Predicted next-open gap: {expected_gap_pct:+.1f}% (BTC {btc['btc_pct_change']:+.2f}% × β={beta_used:.2f}{bias_txt}) — "
            f"{ticker} will likely open sharply {direction}{open_txt}. Stale technicals don't apply while market closed."
        )
    if btc_intraday and high_corr:
        forced_reasons.append(
            f"BTC intraday bias is {btc_intraday['bias'].upper()} ({btc_intraday['score']:+.1f}) and {ticker} is heavily "
            f"BTC-correlated — this is the dominant input"
        )
    elif btc_intraday:
        forced_reasons.append(f"BTC intraday bias: {btc_intraday['bias']} ({btc_intraday['score']:+.1f})")
    if rsi is not None:
        if forced_side == 'LONG' and rsi >= 55: forced_reasons.append(f'RSI {rsi:.1f} supports longs')
        elif forced_side == 'LONG' and rsi <= 30: forced_reasons.append(f'RSI {rsi:.1f} oversold — reversion long')
        elif forced_side == 'SHORT' and rsi <= 45: forced_reasons.append(f'RSI {rsi:.1f} supports shorts')
        elif forced_side == 'SHORT' and rsi >= 70: forced_reasons.append(f'RSI {rsi:.1f} overbought — fade')
    if ema9 and ema21:
        if forced_side == 'LONG' and ema9 > ema21: forced_reasons.append('EMA9 > EMA21 trend up')
        elif forced_side == 'SHORT' and ema9 < ema21: forced_reasons.append('EMA9 < EMA21 trend down')
    if vwap and price:
        if forced_side == 'LONG' and price > vwap: forced_reasons.append(f'price above VWAP ${vwap:.2f}')
        elif forced_side == 'SHORT' and price < vwap: forced_reasons.append(f'price below VWAP ${vwap:.2f}')
    if not forced_reasons:
        forced_reasons.append('No strong individual signals — pick reflects weakest net direction; treat as low conviction')

    # ── Decision ──────────────────────────────────────────────────────────
    # Conviction bands match the 0-10 confidence_score scale below
    # (|s|*2 clamped to 10): HIGH≥8 (|s|≥4), MEDIUM 5-7 (|s|≥2.5), LOW 2-4 (|s|≥1).
    def _classify(s):
        if s >= 4:    return 'LONG',  'HIGH'
        if s >= 2.5:  return 'LONG',  'MEDIUM'
        if s >= 1:    return 'LONG',  'LOW'
        if s <= -4:   return 'SHORT', 'HIGH'
        if s <= -2.5: return 'SHORT', 'MEDIUM'
        if s <= -1:   return 'SHORT', 'LOW'
        return 'FLAT', 'N/A'

    base_decision,  base_conf  = _classify(stock_score)
    decision,       confidence = _classify(final_score)

    # Unsigned 0-10 confidence for UI display. Direction comes from
    # `decision` / `forced_side`; the score just expresses how strong
    # the conviction is. Linearly maps |final_score| → 0..10 (capped).
    confidence_score = round(min(10.0, max(0.0, abs(final_score) * 2.0)), 1)

    # ── Entry / Stop / Target ─────────────────────────────────────────────
    entry = price
    stop = stop_note = target = target_note = entry_note = None
    rr = None

    if decision in ('LONG', 'SHORT') and atr:
        if decision == 'LONG':
            pull_lvl = min(filter(None, [
                ema9 if ema9 < price else None,
                vwap if vwap and vwap < price else None,
            ]), default=None)
            if pull_lvl:
                entry_note = (f"Enter at market (${price}) or on a pullback to "
                              f"${pull_lvl:.2f} for better risk/reward")
            else:
                entry_note = f"Enter at market (${price})"

            stop_cands = []
            for lvl, lbl in [
                (s1,      f"just below S1 support (${s1})"),
                (val,     f"just below Value Area Low (${val:.2f})" if val else None),
                (poc,     f"just below Point of Control (${poc:.2f})" if poc else None),
                (or_low,  f"just below opening range low (${or_low})"),
                (pm_low,  f"just below premarket low (${pm_low})"),
                (vwap_l1, f"just below VWAP −1σ (${vwap_l1:.2f})" if vwap_l1 else None),
                (vwap,    f"just below VWAP (${vwap:.2f})" if vwap else None),
            ]:
                if lvl and lbl and lvl < price and (price - lvl) >= 0.4 * atr:
                    stop_cands.append((lvl, lbl))
            if stop_cands:
                sv, sl = max(stop_cands, key=lambda x: x[0])
                stop, stop_note = round(sv - 0.01, 2), sl
            else:
                stop = round(price - 1.5 * atr, 2)
                stop_note = f"1.5×ATR below entry (no nearby support level)"

            tgt_cands = []
            for lvl, lbl in [
                (poc,     f"Point of Control (${poc:.2f})" if poc and poc > price else None),
                (vah,     f"Value Area High (${vah:.2f})"  if vah else None),
                (r1,      f"R1 resistance (${r1})"),
                (or_high, f"opening range high (${or_high})"),
                (pm_high, f"premarket high (${pm_high})"),
                (vwap_u1, f"VWAP +1σ (${vwap_u1:.2f})" if vwap_u1 else None),
                (r2,      f"R2 resistance (${r2})"),
            ]:
                if lvl and lbl and lvl > price:
                    tgt_cands.append((lvl, lbl))
            if tgt_cands:
                tv, tl = min(tgt_cands, key=lambda x: x[0])
                target, target_note = tv, tl
            else:
                target = round(price + 2.5 * atr, 2)
                target_note = f"2.5×ATR above entry (no nearby resistance level)"

        else:  # SHORT
            bounce_lvl = max(filter(None, [
                ema9  if ema9  > price else None,
                vwap  if vwap  and vwap > price else None,
            ]), default=None)
            if bounce_lvl:
                entry_note = (f"Enter at market (${price}) or on a bounce to "
                              f"${bounce_lvl:.2f} for better risk/reward")
            else:
                entry_note = f"Enter at market (${price})"

            stop_cands = []
            for lvl, lbl in [
                (r1,      f"just above R1 resistance (${r1})"),
                (vah,     f"just above Value Area High (${vah:.2f})" if vah else None),
                (poc,     f"just above Point of Control (${poc:.2f})" if poc else None),
                (or_high, f"just above opening range high (${or_high})"),
                (pm_high, f"just above premarket high (${pm_high})"),
                (vwap_u1, f"just above VWAP +1σ (${vwap_u1:.2f})" if vwap_u1 else None),
                (vwap,    f"just above VWAP (${vwap:.2f})" if vwap else None),
            ]:
                if lvl and lbl and lvl > price and (lvl - price) >= 0.4 * atr:
                    stop_cands.append((lvl, lbl))
            if stop_cands:
                sv, sl = min(stop_cands, key=lambda x: x[0])
                stop, stop_note = round(sv + 0.01, 2), sl
            else:
                stop = round(price + 1.5 * atr, 2)
                stop_note = f"1.5×ATR above entry (no nearby resistance level)"

            tgt_cands = []
            for lvl, lbl in [
                (poc,     f"Point of Control (${poc:.2f})" if poc and poc < price else None),
                (val,     f"Value Area Low (${val:.2f})"   if val else None),
                (s1,      f"S1 support (${s1})"),
                (or_low,  f"opening range low (${or_low})"),
                (pm_low,  f"premarket low (${pm_low})"),
                (vwap_l1, f"VWAP −1σ (${vwap_l1:.2f})" if vwap_l1 else None),
                (s2,      f"S2 support (${s2})"),
            ]:
                if lvl and lbl and lvl < price:
                    tgt_cands.append((lvl, lbl))
            if tgt_cands:
                tv, tl = max(tgt_cands, key=lambda x: x[0])
                target, target_note = tv, tl
            else:
                target = round(price - 2.5 * atr, 2)
                target_note = f"2.5×ATR below entry (no nearby support level)"

        if stop and target:
            risk   = abs(entry - stop)
            reward = abs(target - entry)
            rr = round(reward / risk, 1) if risk > 0 else None

    # ── BTC overlay narrative ─────────────────────────────────────────────
    btc_line = None
    if btc:
        pct         = btc['btc_pct_change']
        context     = btc['context_label']
        current     = btc['btc_current']
        at_close    = btc['btc_at_close']
        trend5      = btc['btc_trend_5d']
        close_label = btc.get('close_label', btc['last_stock_close_date'])
        abs_pct     = abs(pct)
        is_open     = 'intraday' in context

        # Verdict-change suffix — only show when BTC actually moved the needle
        if base_decision != decision or base_conf != confidence:
            _from = f"{base_decision}" + (f" {base_conf}" if base_conf != 'N/A' else "")
            _to   = f"{decision}"      + (f" {confidence}" if confidence != 'N/A' else "")
            verdict_note = f" ⚑ Verdict adjusted by BTC: {_from} → {_to}."
        else:
            verdict_note = ""

        if pct >= 0:
            if abs_pct < 0.5:
                btc_line = (
                    f"⚡ BTC ({context}): Flat since {close_label} (${at_close:,} → ${current:,}, {pct:+.2f}%) — "
                    f"neutral macro backdrop, no BTC-driven edge.{verdict_note}")
            elif abs_pct < 2:
                btc_line = (
                    f"⚡ BTC TAILWIND ({context}): Up {abs_pct:.2f}% since {close_label} (${at_close:,} → ${current:,}) — "
                    f"mild macro support for longs. "
                    + ('Manage size normally and trail your stop.' if is_open
                       else 'Expect a slightly positive open — confirm price holds after the first 5-min candle before adding size.')
                    + verdict_note)
            else:
                btc_line = (
                    f"⚡ BTC STRONG TAILWIND ({context}): Up {abs_pct:.2f}% since {close_label} (${at_close:,} → ${current:,}) — "
                    f"significant macro support for longs. "
                    + ('Ride bullish setups with normal size — keep stops tight in case BTC fades.' if is_open
                       else 'Expect a gap-up open. Wait for the first 5-min candle to confirm before entering.')
                    + verdict_note)
        else:
            if abs_pct < 0.5:
                btc_line = (
                    f"⚡ BTC ({context}): Essentially flat since {close_label} (${at_close:,} → ${current:,}, {pct:+.2f}%) — "
                    f"no meaningful macro headwind.{verdict_note}")
            elif abs_pct < 1:
                if decision == 'LONG':
                    btc_line = (
                        f"⚠️ BTC MILD HEADWIND ({context}): Down {abs_pct:.2f}% since {close_label} (${at_close:,} → ${current:,}). "
                        f"Stock technicals still favor long but reduce position size and keep stops tight — "
                        + ('be ready to flip short if price loses VWAP.' if is_open
                           else f'wait 10–15 min at open to confirm VWAP holds before entering.')
                        + verdict_note)
                elif decision == 'FLAT':
                    btc_line = (
                        f"⚠️ BTC HEADWIND NEUTRALIZES SETUP ({context}): Down {abs_pct:.2f}% since {close_label} (${at_close:,} → ${current:,}). "
                        f"BTC drag has offset the stock technicals — no clear edge, staying flat is correct."
                        + verdict_note)
                else:
                    btc_line = (
                        f"⚠️ BTC HEADWIND ({context}): Down {abs_pct:.2f}% since {close_label} (${at_close:,} → ${current:,}) — "
                        f"reinforces bearish setup. "
                        + ('Short bias valid — trail stops above VWAP.' if is_open
                           else f'Expect downside pressure at open. Short on {ticker} favored if it opens below prior close.')
                        + verdict_note)
            elif abs_pct < 3:
                if decision == 'LONG':
                    btc_line = (
                        f"⚠️ BTC DRAG ({context}): Down {abs_pct:.2f}% since {close_label} (${at_close:,} → ${current:,}). "
                        f"Stock technicals remain bullish but BTC is a significant headwind — reduce position size, tighten your stop, "
                        f"and be prepared to exit quickly if price loses VWAP."
                        + verdict_note)
                elif decision == 'FLAT':
                    btc_line = (
                        f"⛔ BTC DRAG OVERRIDES SETUP ({context}): BTC down {abs_pct:.2f}% since {close_label} (${at_close:,} → ${current:,}). "
                        f"The BTC headwind has cancelled out the {'bullish' if base_decision == 'LONG' else 'bearish'} technical setup. "
                        + (f'Wait for price to resolve above/below VWAP with volume, or for BTC to stabilize, before entering.' if is_open
                           else f'Stay flat at open. Do not trade {ticker} until BTC finds direction.')
                        + verdict_note)
                else:
                    btc_line = (
                        f"⛔ BTC CONFIRMS BEARISH ({context}): BTC down {abs_pct:.2f}% since {close_label} (${at_close:,} → ${current:,}) — "
                        f"{ticker} is under dual macro and technical pressure. "
                        + (f'Short setup is high-conviction — stop above VWAP.' if is_open
                           else f'Short on {ticker} is high-conviction. Enter after first 5-min candle confirms, stop above the open.')
                        + verdict_note)
            else:
                if decision == 'SHORT':
                    btc_line = (
                        f"🚨 MAJOR BTC SELLOFF + BEARISH TECHNICALS ({context}): BTC down {abs_pct:.2f}% since {close_label} (${at_close:,} → ${current:,}). "
                        f"Both macro and technicals are fully aligned to the downside. "
                        + (f'Short {ticker} with conviction — keep stops tight; panic moves can reverse sharply.' if is_open
                           else f'{ticker} will likely gap down hard. Short after first 5-min candle confirms — stop above the open.')
                        + verdict_note)
                elif decision == 'FLAT':
                    btc_line = (
                        f"🚨 MAJOR BTC SELLOFF ({context}): BTC down {abs_pct:.2f}% since {close_label} (${at_close:,} → ${current:,}). "
                        f"This macro event {'has overridden the bullish technical setup' if base_decision == 'LONG' else 'dominates the session'}. "
                        + ('Do not chase longs into this macro move. Wait for BTC to find a base before re-engaging.' if is_open
                           else f'Do not trade {ticker} at open. Wait for BTC to stabilize first.')
                        + verdict_note)
                else:
                    # LONG survives major selloff — stock score must be enormous
                    btc_line = (
                        f"🚨 WARNING — MAJOR BTC SELLOFF ({context}): BTC down {abs_pct:.2f}% since {close_label} (${at_close:,} → ${current:,}). "
                        f"Despite strong technical signals, this is a severe macro headwind. If taking this long, size down significantly and use an extremely tight stop."
                        + verdict_note)

        if trend5 is not None:
            if trend5 <= -10:
                btc_line += f" BTC 5-day: {trend5:+.1f}% — macro headwind is structural, not just today's move."
            elif trend5 >= 10:
                btc_line += f" BTC 5-day: {trend5:+.1f}% — strong multi-day tailwind behind this move."

    narrative = ". ".join(sentences) + ("." if sentences else "")

    analysis_payload = {
        'narrative':       narrative,
        'btc_line':        btc_line,
        'decision':        decision,
        'confidence':      confidence,
        'score':           final_score,
        'confidence_score': confidence_score,  # 0-10 UI score (unsigned)
        'stock_score':     round(stock_score, 1),
        'btc_adj':         round(btc_adj, 2),
        'base_decision':   base_decision,
        'base_confidence': base_conf,
        'entry':           entry,
        'entry_note':      entry_note,
        'stop':            stop,
        'stop_note':       stop_note,
        'target':          target,
        'target_note':     target_note,
        'rr':              rr,
        'forced_side':       forced_side,
        'forced_conviction': forced_conviction,
        'forced_reasons':    forced_reasons,
        'high_btc_correlation': high_corr,
        'market_closed':     market_closed,
        'expected_gap_pct':  expected_gap_pct,
        'gap_adj':           round(gap_adj, 2),
        'predicted_open_price': predicted_open_price,
        'beta_used':         beta_used,
        'btc_implied_price': btc_implied_price,
        'btc_implied_beta':  btc_implied_beta,
        'btc_current':       btc_current_px,
        'btc_pct_change':    btc_pct_now,
    }
    analysis_payload['human_read'] = _plain_signal_read(
        ticker,
        latest,
        analysis_payload,
        daily_ctx=daily_ctx,
        btc=btc,
        btc_intraday=btc_intraday,
    )
    return analysis_payload


def get_btc_intraday_bias():
    """
    Fetch BTC 1-min bars for the last ~4 hours and derive a directional bias from
    full indicator stack (RSI, MACD, EMA9/21, VWAP, recent momentum).
    Returns {score: -10..+10, bias: 'bullish'/'neutral'/'bearish', indicators: {...},
             signals: [str], price, pct_4h}
    BTC trades 24/7 so we don't need market-hour gating.
    """
    try:
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(hours=4)
        bars = alpaca_crypto_bars(
            'BTC/USD',
            start_dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
            end_dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
            timeframe='1Min',
        )
        if not bars or len(bars) < 30:
            return None
        df = pd.DataFrame(bars).rename(columns={'o':'open','h':'high','l':'low','c':'close','v':'volume'})
        closes = df['close'].tolist()

        rsi_v = calculate_rsi(closes)
        macd = calculate_macd(closes)
        ema9_v = calculate_ema(closes, 9)
        ema21_v = calculate_ema(closes, 21)
        vwap_bands = calculate_vwap_bands(df)
        vwap_v = vwap_bands['vwap'][-1] if vwap_bands['vwap'] else None
        atr_v = calculate_atr(df)
        price = round(float(closes[-1]), 2)
        pct_4h = round(((closes[-1] - closes[0]) / closes[0]) * 100, 2) if closes[0] else 0
        # last 15-min momentum
        pct_15m = round(((closes[-1] - closes[-15]) / closes[-15]) * 100, 2) if len(closes) >= 16 and closes[-15] else 0

        score = 0.0
        signals = []

        # RSI (weight ±2)
        if rsi_v is not None:
            if rsi_v >= 70: score -= 2; signals.append(f'RSI {rsi_v:.1f} overbought — BTC exhausted')
            elif rsi_v >= 55: score += 1.5; signals.append(f'RSI {rsi_v:.1f} — bullish momentum')
            elif rsi_v <= 30: score += 2; signals.append(f'RSI {rsi_v:.1f} oversold — bounce likely')
            elif rsi_v <= 45: score -= 1.5; signals.append(f'RSI {rsi_v:.1f} — bearish momentum')

        # MACD histogram (weight ±2)
        hist = macd['histogram']
        if len(hist) >= 2 and hist[-1] is not None and hist[-2] is not None:
            if hist[-1] > 0 and hist[-1] > hist[-2]:
                score += 2; signals.append('MACD histogram rising and positive — momentum bullish')
            elif hist[-1] > 0:
                score += 1; signals.append('MACD histogram positive')
            elif hist[-1] < 0 and hist[-1] < hist[-2]:
                score -= 2; signals.append('MACD histogram falling and negative — momentum bearish')
            else:
                score -= 1; signals.append('MACD histogram negative')

        # EMA stack (weight ±1.5)
        if ema9_v and ema21_v:
            if ema9_v > ema21_v: score += 1.5; signals.append('EMA9 > EMA21 — trend up')
            else: score -= 1.5; signals.append('EMA9 < EMA21 — trend down')

        # VWAP (weight ±1.5)
        if vwap_v:
            if price > vwap_v * 1.002: score += 1.5; signals.append(f'Price above VWAP ${vwap_v:.0f} — intraday bulls in control')
            elif price < vwap_v * 0.998: score -= 1.5; signals.append(f'Price below VWAP ${vwap_v:.0f} — intraday bears in control')

        # Short-term momentum (weight ±2)
        if pct_15m >= 0.5: score += 2; signals.append(f'Last 15m: {pct_15m:+.2f}% — strong upside thrust')
        elif pct_15m >= 0.15: score += 1; signals.append(f'Last 15m: {pct_15m:+.2f}% — drifting up')
        elif pct_15m <= -0.5: score -= 2; signals.append(f'Last 15m: {pct_15m:+.2f}% — strong downside thrust')
        elif pct_15m <= -0.15: score -= 1; signals.append(f'Last 15m: {pct_15m:+.2f}% — drifting down')

        # Clamp to ±10
        score = max(-10.0, min(10.0, score))
        if score >= 3: bias = 'bullish'
        elif score <= -3: bias = 'bearish'
        else: bias = 'neutral'

        return {
            'score': round(score, 1),
            'bias': bias,
            'price': price,
            'pct_4h': pct_4h,
            'pct_15m': pct_15m,
            'indicators': {
                'rsi': round(rsi_v, 1) if rsi_v is not None else None,
                'macd_hist': round(hist[-1], 2) if hist and hist[-1] is not None else None,
                'macd_hist_prev': round(hist[-2], 2) if len(hist) >= 2 and hist[-2] is not None else None,
                'ema9': round(ema9_v, 2) if ema9_v else None,
                'ema21': round(ema21_v, 2) if ema21_v else None,
                'vwap': round(vwap_v, 2) if vwap_v else None,
                'atr': round(atr_v, 2) if atr_v else None,
            },
            'signals': signals,
        }
    except Exception:
        return None


def get_daily_context_for_date(ticker, session_date_str):
    """Daily context using only data available before session_date_str (for historical review)."""
    session_dt = datetime.strptime(session_date_str, '%Y-%m-%d')
    end_date   = (session_dt - timedelta(days=1)).strftime('%Y-%m-%d')
    start_date = (session_dt - timedelta(days=40)).strftime('%Y-%m-%d')
    try:
        results = alpaca_stock_bars(ticker, start_date, end_date, timeframe='1Day')
    except Exception:
        return None
    if not results:
        return None
    df = pd.DataFrame(results)
    df = df.rename(columns={'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume'})
    closes = df['close'].tolist()
    ema9  = pd.Series(closes).ewm(span=9,  adjust=False).mean()
    ema21 = pd.Series(closes).ewm(span=21, adjust=False).mean()
    return {
        'prev_close':       round(float(df['close'].iloc[-1]), 2),
        'prev_high':        round(float(df['high'].iloc[-1]),  2),
        'prev_low':         round(float(df['low'].iloc[-1]),   2),
        'avg_daily_volume': int(df['volume'].mean()),
        'daily_trend':      'bullish' if ema9.iloc[-1] > ema21.iloc[-1] else 'bearish',
        'pct_5d': round(((closes[-1] - closes[-5]) / closes[-5]) * 100, 2) if len(closes) >= 5 else None,
    }


def get_btc_context_at_time(entry_ts_ms, session_date_str, prev_session_date_str):
    """Historical BTC context: BTC price at entry time vs the prior stock session close."""
    close_d   = datetime.strptime(prev_session_date_str, '%Y-%m-%d').date()
    close_utc = datetime(close_d.year, close_d.month, close_d.day,
                         16, 0, 0, tzinfo=_ET).astimezone(timezone.utc)

    # BTC price at the prior stock-session close (~4 PM ET)
    btc_at_close = None
    win_start = (close_utc - timedelta(minutes=10)).strftime('%Y-%m-%dT%H:%M:%SZ')
    win_end   = (close_utc + timedelta(minutes=5)).strftime('%Y-%m-%dT%H:%M:%SZ')
    try:
        bars = alpaca_crypto_bars('BTC/USD', win_start, win_end, timeframe='5Min')
        if bars:
            btc_at_close = round(float(bars[-1]['c']), 2)
    except Exception:
        pass
    if btc_at_close is None:
        try:
            dbars = alpaca_crypto_bars('BTC/USD', prev_session_date_str,
                                        (close_d + timedelta(days=1)).strftime('%Y-%m-%d'),
                                        timeframe='1Day')
            if dbars:
                btc_at_close = round(float(dbars[0]['c']), 2)
        except Exception:
            return None
    if btc_at_close is None:
        return None

    # BTC price at the exact entry time (5-min bar closest to entry_ts_ms)
    entry_dt = datetime.fromtimestamp(entry_ts_ms / 1000, tz=timezone.utc)
    entry_win_start = (entry_dt - timedelta(minutes=30)).strftime('%Y-%m-%dT%H:%M:%SZ')
    entry_win_end   = (entry_dt + timedelta(minutes=10)).strftime('%Y-%m-%dT%H:%M:%SZ')
    btc_at_entry = None
    try:
        ebars = alpaca_crypto_bars('BTC/USD', entry_win_start, entry_win_end, timeframe='5Min')
        if ebars:
            closest = min(ebars, key=lambda b: abs(b['t'] - entry_ts_ms))
            btc_at_entry = round(float(closest['c']), 2)
    except Exception:
        pass
    if btc_at_entry is None:
        btc_at_entry = btc_at_close

    btc_pct_change = round(((btc_at_entry - btc_at_close) / btc_at_close) * 100, 2)

    # 5-day trend ending at session_date
    btc_trend_5d = None
    try:
        s_dt       = datetime.strptime(session_date_str, '%Y-%m-%d')
        start_date = (s_dt - timedelta(days=10)).strftime('%Y-%m-%d')
        dbars = alpaca_crypto_bars('BTC/USD', start_date, session_date_str, timeframe='1Day')
        dc = [b['c'] for b in dbars]
        if len(dc) >= 5:
            btc_trend_5d = round(((dc[-1] - dc[-5]) / dc[-5]) * 100, 2)
    except Exception:
        pass

    return {
        'btc_at_close':          btc_at_close,
        'btc_current':           btc_at_entry,
        'btc_pct_change':        btc_pct_change,
        'btc_trend_5d':          btc_trend_5d,
        'context_label':         'at time of entry',
        'last_stock_close_date': prev_session_date_str,
        'close_label':           f"{prev_session_date_str} 4PM ET",
        'price_source':          'alpaca historical',
    }


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/analyze', methods=['POST'])
def analyze():
    body = request.json
    ticker = body.get('ticker', '').upper().strip()
    btc_correlated = body.get('btc_correlated', False)

    if not ticker:
        return jsonify({'error': 'Ticker is required'}), 400

    try:
        df, session_date = get_intraday_data(ticker)
        daily_ctx = get_daily_context(ticker)

        premarket_mask = _premarket_mask(df['dt_utc'])
        premarket_df = df[premarket_mask]
        session_df = df[~premarket_mask].reset_index(drop=True)

        premarket_high = round(float(premarket_df['high'].max()), 2) if not premarket_df.empty else None
        premarket_low = round(float(premarket_df['low'].min()), 2) if not premarket_df.empty else None

        or_df = session_df.head(3)
        or_high = round(float(or_df['high'].max()), 2) if len(or_df) >= 1 else None
        or_low = round(float(or_df['low'].min()), 2) if len(or_df) >= 1 else None

        # Session bars for indicators; fall back to all bars if session not started
        ind_df = session_df if len(session_df) >= 3 else df.reset_index(drop=True)
        n_pad = len(premarket_df) if len(session_df) >= 3 else 0

        # VWAP bands (session bars only)
        vwap_result = calculate_vwap_bands(ind_df)
        vwap_pad = [None] * n_pad

        # MACD on all bars for chart continuity; latest values from last bar
        all_closes = df['close'].tolist()
        macd_all = calculate_macd(all_closes)
        macd_h_last = macd_all['histogram'][-1] if macd_all['histogram'] else None
        macd_h_prev = macd_all['histogram'][-2] if len(macd_all['histogram']) >= 2 else None

        # RSI / EMA / ATR
        closes_ind = ind_df['close'].tolist()
        rsi_ind = calculate_rsi(closes_ind)
        ema9_ind = calculate_ema(closes_ind, 9)
        ema21_ind = calculate_ema(closes_ind, 21)
        atr_ind = calculate_atr(ind_df)
        all_rsi = calculate_rsi(all_closes)
        all_ema9 = calculate_ema(all_closes, 9)
        all_ema21 = calculate_ema(all_closes, 21)
        all_atr = calculate_atr(df)

        # RVOL
        rvol = None
        if daily_ctx and daily_ctx['avg_daily_volume'] > 0:
            bars_elapsed = min(len(ind_df), 390)
            expected = daily_ctx['avg_daily_volume'] * (bars_elapsed / 390)
            if expected > 0:
                rvol = round(float(ind_df['volume'].sum()) / expected, 2)

        # Pivot points from previous day OHLC
        pivots = None
        if daily_ctx and daily_ctx.get('prev_high') and daily_ctx.get('prev_low'):
            pivots = calculate_pivots(
                daily_ctx['prev_high'], daily_ctx['prev_low'], daily_ctx['prev_close']
            )

        # CVD and Volume Profile (session bars only)
        cvd_result = calculate_cvd(ind_df)
        vp = calculate_volume_profile(ind_df)
        cvd_pad = [None] * n_pad

        # CVD divergence: compare price trend vs CVD trend over last 6 bars (~30 min)
        cvd_divergence = None
        if len(cvd_result['cvd']) >= 6:
            rp = closes_ind[-6:];  rc = cvd_result['cvd'][-6:]
            price_up = rp[-1] > rp[0];  cvd_up = rc[-1] > rc[0]
            if price_up and not cvd_up:   cvd_divergence = 'bear'
            elif not price_up and cvd_up: cvd_divergence = 'bull'

        # Overextension / exhaustion context
        sess_high_l = float(ind_df['high'].max()) if not ind_df.empty else None
        sess_low_l  = float(ind_df['low'].min())  if not ind_df.empty else None
        consec_green_l = 0; consec_red_l = 0
        for _, row in ind_df.iloc[::-1].iterrows():
            if row['close'] > row['open']:
                if consec_red_l > 0: break
                consec_green_l += 1
            elif row['close'] < row['open']:
                if consec_green_l > 0: break
                consec_red_l += 1
            else:
                break
        last5_pct_l = None
        if len(ind_df) >= 6:
            p0 = float(ind_df['close'].iloc[-6]); p1 = float(ind_df['close'].iloc[-1])
            if p0: last5_pct_l = round((p1 - p0) / p0 * 100, 2)

        bb_l  = calculate_bollinger(closes_ind, period=20, mult=2.0)
        kc_l  = calculate_keltner(ind_df, period=20, mult=1.5)
        bb_mid_l = bb_l['mid'][-1]   if bb_l['mid']   else None
        bb_up_l  = bb_l['upper'][-1] if bb_l['upper'] else None
        bb_lo_l  = bb_l['lower'][-1] if bb_l['lower'] else None
        squeeze_l = detect_squeeze(bb_l, kc_l)
        cpat_l, cbias_l = detect_candle_pattern(ind_df)
        vol_ratio_l, _  = compute_volume_climax(ind_df, lookback=20, mult=3.0)
        pdh_l = daily_ctx.get('prev_high') if daily_ctx else None
        pdl_l = daily_ctx.get('prev_low')  if daily_ctx else None

        latest = {
            'price': round(float(ind_df['close'].iloc[-1]), 2),
            'rsi': rsi_ind[-1],
            'ema9': ema9_ind[-1],
            'ema21': ema21_ind[-1],
            'vwap': vwap_result['vwap'][-1],
            'vwap_upper1': vwap_result['upper1'][-1],
            'vwap_lower1': vwap_result['lower1'][-1],
            'vwap_upper2': vwap_result['upper2'][-1],
            'vwap_lower2': vwap_result['lower2'][-1],
            'atr': atr_ind[-1],
            'rvol': rvol,
            'macd_line': macd_all['macd'][-1],
            'macd_signal': macd_all['signal'][-1],
            'macd_histogram': macd_h_last,
            'macd_prev_histogram': macd_h_prev,
            'premarket_high': premarket_high,
            'premarket_low': premarket_low,
            'or_high': or_high,
            'or_low': or_low,
            'prev_close': daily_ctx['prev_close'] if daily_ctx else None,
            'pivot': pivots['p'] if pivots else None,
            'r1': pivots['r1'] if pivots else None,
            'r2': pivots['r2'] if pivots else None,
            's1': pivots['s1'] if pivots else None,
            's2': pivots['s2'] if pivots else None,
            'poc': round(vp['poc'], 2) if vp['poc'] else None,
            'vah': round(vp['vah'], 2) if vp['vah'] else None,
            'val': round(vp['val'], 2) if vp['val'] else None,
            'cvd_value': cvd_result['cvd'][-1] if cvd_result['cvd'] else None,
            'cvd_divergence': cvd_divergence,
            'session_high':   round(sess_high_l, 2) if sess_high_l else None,
            'session_low':    round(sess_low_l,  2) if sess_low_l  else None,
            'consec_green':   consec_green_l,
            'consec_red':     consec_red_l,
            'last5_pct':      last5_pct_l,
            'bb_mid':         round(bb_mid_l, 2) if bb_mid_l is not None else None,
            'bb_upper':       round(bb_up_l,  2) if bb_up_l  is not None else None,
            'bb_lower':       round(bb_lo_l,  2) if bb_lo_l  is not None else None,
            'squeeze':        squeeze_l,
            'candle_pattern': cpat_l,
            'candle_bias':    cbias_l,
            'vol_climax_ratio': vol_ratio_l,
            'prev_day_high':  pdh_l,
            'prev_day_low':   pdl_l,
        }

        # RVOL series for chart
        rvol_chart = []
        sess_vol, sess_bars = 0, 0
        for is_pm, row in zip(premarket_mask.tolist(), df.itertuples(index=False)):
            if is_pm:
                rvol_chart.append(None)
            else:
                sess_vol += row.volume
                sess_bars += 1
                if daily_ctx and daily_ctx['avg_daily_volume'] > 0:
                    exp = daily_ctx['avg_daily_volume'] * (min(sess_bars, 390) / 390)
                    rvol_chart.append(round(sess_vol / exp, 2) if exp > 0 else None)
                else:
                    rvol_chart.append(None)

        def ts_to_et(ts_ms):
            return datetime.fromtimestamp(ts_ms / 1000, tz=_ET).strftime('%H:%M')

        times = [ts_to_et(ts) for ts in df['timestamp'].tolist()]

        btc_data = None
        btc_error = None
        btc_intraday = None
        if btc_correlated:
            try:
                btc_data = get_btc_data(session_date)
            except Exception as e:
                btc_error = f"BTC data unavailable: {str(e)}"
            btc_intraday = get_btc_intraday_bias()

        analysis = rule_based_analysis(ticker, latest, daily_ctx=daily_ctx, btc=btc_data, btc_intraday=btc_intraday)
        playbook = build_playbook(latest, daily_ctx=daily_ctx, btc=btc_data)

        return jsonify({
            'ticker': ticker,
            'session_date': session_date,
            'latest': latest,
            'daily_ctx': daily_ctx,
            'analysis': analysis,
            'playbook': playbook,
            'btc': btc_data,
            'btc_intraday': btc_intraday,
            'btc_error': btc_error,
            'chart_data': {
                'times': times,
                'close': all_closes,
                'volume': df['volume'].tolist(),
                'ema9': all_ema9,
                'ema21': all_ema21,
                'vwap':    vwap_pad + vwap_result['vwap'],
                'vwap_u1': vwap_pad + vwap_result['upper1'],
                'vwap_l1': vwap_pad + vwap_result['lower1'],
                'vwap_u2': vwap_pad + vwap_result['upper2'],
                'vwap_l2': vwap_pad + vwap_result['lower2'],
                'rsi': all_rsi,
                'atr': all_atr,
                'rvol': rvol_chart,
                'macd': macd_all['macd'],
                'macd_signal': macd_all['signal'],
                'macd_histogram': macd_all['histogram'],
                'premarket_high': premarket_high,
                'premarket_low': premarket_low,
                'or_high': or_high,
                'or_low': or_low,
                'prev_close': daily_ctx['prev_close'] if daily_ctx else None,
                'pivot': pivots['p'] if pivots else None,
                'r1': pivots['r1'] if pivots else None,
                'r2': pivots['r2'] if pivots else None,
                's1': pivots['s1'] if pivots else None,
                's2': pivots['s2'] if pivots else None,
                'cvd_delta': cvd_pad + cvd_result['delta'],
                'cvd_line':  cvd_pad + cvd_result['cvd'],
                'vp_prices':  vp['prices'],
                'vp_volumes': vp['volumes'],
                'vp_poc':     round(vp['poc'], 2) if vp['poc'] else None,
                'vp_poc_idx': vp['poc_idx'],
                'vp_vah':     round(vp['vah'], 2) if vp['vah'] else None,
                'vp_val':     round(vp['val'], 2) if vp['val'] else None,
            }
        })

    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': f'Server error: {str(e)}'}), 500


@app.route('/review', methods=['POST'])
def review():
    body          = request.json
    ticker        = body.get('ticker', '').upper().strip()
    entry_dt_str  = body.get('entry_dt', '')
    entry_price   = body.get('entry_price')
    direction     = body.get('direction', 'LONG').upper()
    exit_dt_str   = body.get('exit_dt', '')
    exit_price_raw = body.get('exit_price')
    btc_correlated = body.get('btc_correlated', False)

    if not ticker:
        return jsonify({'error': 'Ticker is required'}), 400
    if not entry_dt_str or entry_price is None:
        return jsonify({'error': 'Entry datetime and price are required'}), 400

    try:
        entry_price = float(entry_price)
        exit_price  = float(exit_price_raw) if exit_price_raw else None

        # User inputs times in CT; localise and convert to UTC.
        entry_dt_naive = datetime.fromisoformat(entry_dt_str)
        session_date   = entry_dt_naive.strftime('%Y-%m-%d')
        entry_utc_dt   = entry_dt_naive.replace(tzinfo=_CT).astimezone(timezone.utc)
        entry_ts_ms    = int(entry_utc_dt.timestamp() * 1000)

        exit_ts_ms = None
        exit_date  = None
        if exit_dt_str:
            exit_dt_naive = datetime.fromisoformat(exit_dt_str)
            exit_utc_dt   = exit_dt_naive.replace(tzinfo=_CT).astimezone(timezone.utc)
            exit_ts_ms    = int(exit_utc_dt.timestamp() * 1000)
            exit_date     = exit_dt_naive.strftime('%Y-%m-%d')

        # Fetch entry session bars (pre-market through post-market in ET)
        start_iso, end_iso = _et_day_bounds(session_date)
        results = alpaca_stock_bars(ticker, start_iso, end_iso, timeframe='1Min')
        if not results or len(results) < 3:
            return jsonify({'error': f'No data for {ticker} on {session_date}. Market may have been closed or the date is too far back for the free tier.'}), 400

        df = pd.DataFrame(results)
        df = df.rename(columns={'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume', 't': 'timestamp'})
        df['dt_utc'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        df = df.sort_values('timestamp').reset_index(drop=True)

        # Fetch exit-date bars if exit is on a different day
        post_df = pd.DataFrame()
        if exit_date and exit_date != session_date:
            p_start, p_end = _et_day_bounds(exit_date)
            try:
                p_results = alpaca_stock_bars(ticker, p_start, p_end, timeframe='1Min')
                if p_results:
                    post_df = pd.DataFrame(p_results)
                    post_df = post_df.rename(columns={'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume', 't': 'timestamp'})
                    post_df['dt_utc'] = pd.to_datetime(post_df['timestamp'], unit='ms', utc=True)
                    post_df = post_df.sort_values('timestamp').reset_index(drop=True)
            except Exception:
                pass

        premarket_mask = _premarket_mask(df['dt_utc'])
        premarket_df   = df[premarket_mask]
        session_df     = df[~premarket_mask].reset_index(drop=True)
        n_pre          = len(premarket_df)

        premarket_high = round(float(premarket_df['high'].max()), 2) if not premarket_df.empty else None
        premarket_low  = round(float(premarket_df['low'].min()),  2) if not premarket_df.empty else None
        or_df   = session_df.head(3)
        or_high = round(float(or_df['high'].max()), 2) if len(or_df) >= 1 else None
        or_low  = round(float(or_df['low'].min()),  2) if len(or_df) >= 1 else None

        # Slice session bars to bars that COMPLETED BEFORE entry.
        # Alpaca bar timestamps mark the START of the window, so a bar ts == entry_ts_ms
        # is still forming at entry — including it would leak 5 min of post-entry data.
        entry_session_df = session_df[session_df['timestamp'] < entry_ts_ms].reset_index(drop=True)
        if entry_session_df.empty:
            entry_session_df = df[df['timestamp'] < entry_ts_ms].reset_index(drop=True)
        if len(entry_session_df) < 2:
            entry_session_df = session_df.head(3) if len(session_df) >= 3 else df.head(3)
        entry_session_df = entry_session_df.reset_index(drop=True)

        # Bar index of entry in full df (for chart marker)
        entry_bar_idx = max(0, len(df[df['timestamp'] < entry_ts_ms]) - 1)

        # Bar index of exit
        exit_bar_idx = None
        if exit_ts_ms is not None:
            if not post_df.empty:
                exit_bar_idx = len(df) + max(0, len(post_df[post_df['timestamp'] <= exit_ts_ms]) - 1)
            else:
                exit_bar_idx = max(0, len(df[df['timestamp'] <= exit_ts_ms]) - 1)

        daily_ctx = get_daily_context_for_date(ticker, session_date)

        # Indicators on entry slice
        vwap_e   = calculate_vwap_bands(entry_session_df)
        closes_e = entry_session_df['close'].tolist()
        rsi_e    = calculate_rsi(closes_e)
        ema9_e   = calculate_ema(closes_e, 9)
        ema21_e  = calculate_ema(closes_e, 21)
        atr_e    = calculate_atr(entry_session_df)

        all_closes_e = df[df['timestamp'] < entry_ts_ms]['close'].tolist()
        macd_e       = calculate_macd(all_closes_e)
        macd_h_e     = macd_e['histogram'][-1] if macd_e['histogram'] else None
        macd_hp_e    = macd_e['histogram'][-2] if len(macd_e['histogram']) >= 2 else None

        rvol_e = None
        if daily_ctx and daily_ctx['avg_daily_volume'] > 0:
            bars_el = min(len(entry_session_df), 390)
            exp = daily_ctx['avg_daily_volume'] * (bars_el / 390)
            if exp > 0:
                rvol_e = round(float(entry_session_df['volume'].sum()) / exp, 2)

        pivots_e = None
        if daily_ctx and daily_ctx.get('prev_high'):
            pivots_e = calculate_pivots(daily_ctx['prev_high'], daily_ctx['prev_low'], daily_ctx['prev_close'])

        cvd_e  = calculate_cvd(entry_session_df)
        vp_e   = calculate_volume_profile(entry_session_df)

        cvd_div_e = None
        if len(cvd_e['cvd']) >= 6:
            rp = closes_e[-6:]; rc = cvd_e['cvd'][-6:]
            pu = rp[-1] > rp[0]; cu = rc[-1] > rc[0]
            if pu and not cu:   cvd_div_e = 'bear'
            elif not pu and cu: cvd_div_e = 'bull'

        n_pre_entry = max(0, len(df[df['timestamp'] < entry_ts_ms]) - len(entry_session_df))

        # Overextension / exhaustion context
        sess_high_e = float(entry_session_df['high'].max()) if not entry_session_df.empty else None
        sess_low_e  = float(entry_session_df['low'].min())  if not entry_session_df.empty else None
        consec_green_e = 0; consec_red_e = 0
        for _, row in entry_session_df.iloc[::-1].iterrows():
            if row['close'] > row['open']:
                if consec_red_e > 0: break
                consec_green_e += 1
            elif row['close'] < row['open']:
                if consec_green_e > 0: break
                consec_red_e += 1
            else:
                break
        last5_pct_e = None
        if len(entry_session_df) >= 6:
            p0 = float(entry_session_df['close'].iloc[-6])
            p1 = float(entry_session_df['close'].iloc[-1])
            if p0: last5_pct_e = round((p1 - p0) / p0 * 100, 2)

        # Bollinger / Keltner / Squeeze
        bb_e = calculate_bollinger(closes_e, period=20, mult=2.0)
        kc_e = calculate_keltner(entry_session_df, period=20, mult=1.5)
        bb_mid_v   = bb_e['mid'][-1]   if bb_e['mid']   else None
        bb_up_v    = bb_e['upper'][-1] if bb_e['upper'] else None
        bb_lo_v    = bb_e['lower'][-1] if bb_e['lower'] else None
        squeeze_e  = detect_squeeze(bb_e, kc_e)

        # Candlestick pattern on entry bar
        cpat_e, cbias_e = detect_candle_pattern(entry_session_df)

        # Volume climax
        vol_ratio_e, _ = compute_volume_climax(entry_session_df, lookback=20, mult=3.0)

        # Prior-day high / low
        pdh_e = daily_ctx.get('prev_high') if daily_ctx else None
        pdl_e = daily_ctx.get('prev_low')  if daily_ctx else None

        latest_e = {
            'price': round(float(entry_session_df['close'].iloc[-1]), 2),
            'rsi': rsi_e[-1], 'ema9': ema9_e[-1], 'ema21': ema21_e[-1],
            'vwap': vwap_e['vwap'][-1],
            'vwap_upper1': vwap_e['upper1'][-1], 'vwap_lower1': vwap_e['lower1'][-1],
            'vwap_upper2': vwap_e['upper2'][-1], 'vwap_lower2': vwap_e['lower2'][-1],
            'atr': atr_e[-1], 'rvol': rvol_e,
            'macd_line': macd_e['macd'][-1], 'macd_signal': macd_e['signal'][-1],
            'macd_histogram': macd_h_e, 'macd_prev_histogram': macd_hp_e,
            'premarket_high': premarket_high, 'premarket_low': premarket_low,
            'or_high': or_high, 'or_low': or_low,
            'prev_close': daily_ctx['prev_close'] if daily_ctx else None,
            'pivot': pivots_e['p']  if pivots_e else None,
            'r1':    pivots_e['r1'] if pivots_e else None,
            'r2':    pivots_e['r2'] if pivots_e else None,
            's1':    pivots_e['s1'] if pivots_e else None,
            's2':    pivots_e['s2'] if pivots_e else None,
            'poc': round(vp_e['poc'], 2) if vp_e['poc'] else None,
            'vah': round(vp_e['vah'], 2) if vp_e['vah'] else None,
            'val': round(vp_e['val'], 2) if vp_e['val'] else None,
            'cvd_value':      cvd_e['cvd'][-1] if cvd_e['cvd'] else None,
            'cvd_divergence': cvd_div_e,
            'session_high':   round(sess_high_e, 2) if sess_high_e else None,
            'session_low':    round(sess_low_e,  2) if sess_low_e  else None,
            'consec_green':   consec_green_e,
            'consec_red':     consec_red_e,
            'last5_pct':      last5_pct_e,
            'bb_mid':         round(bb_mid_v, 2) if bb_mid_v is not None else None,
            'bb_upper':       round(bb_up_v,  2) if bb_up_v  is not None else None,
            'bb_lower':       round(bb_lo_v,  2) if bb_lo_v  is not None else None,
            'squeeze':        squeeze_e,
            'candle_pattern': cpat_e,
            'candle_bias':    cbias_e,
            'vol_climax_ratio': vol_ratio_e,
            'prev_day_high':  pdh_e,
            'prev_day_low':   pdl_e,
        }

        btc_data  = None
        btc_error = None
        if btc_correlated:
            prev_date = (entry_dt_naive - timedelta(days=1)).strftime('%Y-%m-%d')
            try:
                btc_data = get_btc_context_at_time(entry_ts_ms, session_date, prev_date)
            except Exception as e:
                btc_error = f"BTC data unavailable: {str(e)}"

        analysis = rule_based_analysis(ticker, latest_e, daily_ctx=daily_ctx, btc=btc_data)

        # Post-mortem verdict (computed after outcome so we can be outcome-aware)

        # Outcome
        outcome = None
        if exit_price is not None:
            raw_pnl   = exit_price - entry_price
            pnl_dollar = raw_pnl if direction == 'LONG' else -raw_pnl
            pnl_pct    = round(pnl_dollar / entry_price * 100, 2)

            after_mask = df['timestamp'] > entry_ts_ms
            after_df   = df[after_mask]
            if not post_df.empty:
                sliced = post_df[post_df['timestamp'] <= exit_ts_ms] if exit_ts_ms else post_df
                after_df = pd.concat([after_df, sliced])
            elif exit_ts_ms:
                after_df = df[(df['timestamp'] > entry_ts_ms) & (df['timestamp'] <= exit_ts_ms)]

            mae = mfe = 0.0
            if not after_df.empty:
                if direction == 'LONG':
                    mae = round(float(after_df['low'].min())  - entry_price, 3)
                    mfe = round(float(after_df['high'].max()) - entry_price, 3)
                else:
                    mae = round(entry_price - float(after_df['high'].max()), 3)
                    mfe = round(entry_price - float(after_df['low'].min()),  3)

            outcome = {
                'exit_price': exit_price,
                'pnl_dollar': round(pnl_dollar, 3),
                'pnl_pct':    pnl_pct,
                'mae':        mae,
                'mfe':        mfe,
                'profitable': pnl_dollar > 0,
            }

        postmortem = build_postmortem(direction, analysis, outcome, daily_ctx, btc_data, latest_e, entry_price=entry_price)

        # Chart data (full session + optional exit-day bars)
        full_df          = pd.concat([df, post_df], ignore_index=True) if not post_df.empty else df
        all_closes_full  = full_df['close'].tolist()
        all_ema9_full    = calculate_ema(all_closes_full, 9)
        all_ema21_full   = calculate_ema(all_closes_full, 21)
        all_rsi_full     = calculate_rsi(all_closes_full)
        macd_full        = calculate_macd(all_closes_full)

        n_sess = len(session_df)
        n_post = len(post_df)
        vwap_sess     = calculate_vwap_bands(session_df) if n_sess >= 2 else None
        vwap_full     = ([None]*n_pre + (vwap_sess['vwap']    if vwap_sess else [None]*n_sess) + [None]*n_post)
        vwap_u1_full  = ([None]*n_pre + (vwap_sess['upper1']  if vwap_sess else [None]*n_sess) + [None]*n_post)
        vwap_l1_full  = ([None]*n_pre + (vwap_sess['lower1']  if vwap_sess else [None]*n_sess) + [None]*n_post)
        vwap_u2_full  = ([None]*n_pre + (vwap_sess['upper2']  if vwap_sess else [None]*n_sess) + [None]*n_post)
        vwap_l2_full  = ([None]*n_pre + (vwap_sess['lower2']  if vwap_sess else [None]*n_sess) + [None]*n_post)

        def ts_to_et_r(ts_ms):
            return datetime.fromtimestamp(ts_ms / 1000, tz=_ET).strftime('%m/%d %H:%M')

        times_full = [ts_to_et_r(ts) for ts in full_df['timestamp'].tolist()]

        entry_marker = [None] * len(full_df)
        if 0 <= entry_bar_idx < len(entry_marker):
            entry_marker[entry_bar_idx] = entry_price
        exit_marker = [None] * len(full_df)
        if exit_bar_idx is not None and exit_price is not None and 0 <= exit_bar_idx < len(exit_marker):
            exit_marker[exit_bar_idx] = exit_price

        cvd_pad_e = [None] * n_pre_entry
        vp_e_poc  = round(vp_e['poc'], 2) if vp_e['poc'] else None

        return jsonify({
            'ticker':        ticker,
            'session_date':  session_date,
            'entry_dt':      entry_dt_str,
            'entry_price':   entry_price,
            'direction':     direction,
            'latest':        latest_e,
            'daily_ctx':     daily_ctx,
            'analysis':      analysis,
            'btc':           btc_data,
            'btc_error':     btc_error,
            'postmortem':    postmortem,
            'outcome':       outcome,
            'chart_data': {
                'times':          times_full,
                'close':          all_closes_full,
                'volume':         full_df['volume'].tolist(),
                'ema9':           all_ema9_full,
                'ema21':          all_ema21_full,
                'vwap':           vwap_full,
                'vwap_u1':        vwap_u1_full,
                'vwap_l1':        vwap_l1_full,
                'vwap_u2':        vwap_u2_full,
                'vwap_l2':        vwap_l2_full,
                'rsi':            all_rsi_full,
                'macd':           macd_full['macd'],
                'macd_signal':    macd_full['signal'],
                'macd_histogram': macd_full['histogram'],
                'entry_marker':   entry_marker,
                'exit_marker':    exit_marker,
                'entry_bar_idx':  entry_bar_idx,
                'exit_bar_idx':   exit_bar_idx,
                'premarket_high': premarket_high,
                'premarket_low':  premarket_low,
                'or_high':        or_high,
                'or_low':         or_low,
                'prev_close':     daily_ctx['prev_close'] if daily_ctx else None,
                'pivot':          pivots_e['p']  if pivots_e else None,
                'r1':             pivots_e['r1'] if pivots_e else None,
                'r2':             pivots_e['r2'] if pivots_e else None,
                's1':             pivots_e['s1'] if pivots_e else None,
                's2':             pivots_e['s2'] if pivots_e else None,
                'vp_poc':         vp_e_poc,
                'vp_vah':         round(vp_e['vah'], 2) if vp_e['vah'] else None,
                'vp_val':         round(vp_e['val'], 2) if vp_e['val'] else None,
                'cvd_delta':      cvd_pad_e + cvd_e['delta'],
                'cvd_line':       cvd_pad_e + cvd_e['cvd'],
            }
        })

    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': f'Server error: {str(e)}'}), 500


# ─────────────────── Watcher (background auto-analysis) ───────────────────
watcher_state = {
    'running': False,
    'ticker': None,
    'btc_correlated': False,
    'signal_source': 'ws_scalp',
    'min_alert_conviction': ['MEDIUM', 'HIGH'],
    'interval': 30,
    'last_check': None,
    'last_price': None,
    'last_decision': None,
    'last_score': None,
    'last_error': None,
    'next_check_ts': None,
    'long_setup': None,   # {trigger, stop, target}
    'short_setup': None,
    'long_trigger_hit': False,
    'short_trigger_hit': False,
    'position': None,     # {side, entry, stop, target, opened_ts, status, pnl}
    'events': [],         # recent alerts
}
_watcher_lock = threading.Lock()
_watcher_stop_evt = threading.Event()
_watcher_thread = None
_WATCHER_MIN_ALERT_CONVICTION = {'MEDIUM', 'HIGH'}


def _notify(title, msg):
    if _win_toast:
        try:
            _win_toast(title, msg, duration='short')
        except Exception:
            pass


def _push_event(kind, text, price=None):
    evt = {'ts': int(time.time()), 'kind': kind, 'text': text, 'price': price}
    watcher_state['events'] = ([evt] + watcher_state['events'])[:30]


def _watcher_signal_rationale(sig):
    reasons = sig.get('reasons') or []
    bits = []
    setup = sig.get('setup_type')
    if setup:
        bits.append(f"setup={setup}")
    if sig.get('score') is not None:
        bits.append(f"score={sig.get('score')}")
    if sig.get('execution_quality'):
        eq = sig.get('execution_quality') or {}
        bits.append(f"execution={eq.get('score')}")
    if sig.get('btc_regime'):
        bits.append(f"BTC={sig.get('btc_regime')}")
    return ' · '.join(bits + reasons[:2])


def _watcher_on_signal(sig):
    """Watcher alerts use the same ws_scalp signal stream as MockTrader."""
    try:
        ticker = (sig.get('ticker') or '').upper()
        side = (sig.get('side') or '').upper()
        conviction = sig.get('conviction')
        if conviction not in _WATCHER_MIN_ALERT_CONVICTION:
            return
        if side not in ('LONG', 'SHORT'):
            return
        with _watcher_lock:
            if not watcher_state.get('running'):
                return
            if ticker != (watcher_state.get('ticker') or '').upper():
                return
            price = sig.get('price')
            watcher_state['last_check'] = int(time.time())
            watcher_state['next_check_ts'] = int(time.time()) + int(watcher_state.get('interval') or 30)
            watcher_state['last_error'] = None
            watcher_state['last_price'] = price
            watcher_state['last_decision'] = side
            watcher_state['last_confidence'] = conviction
            watcher_state['last_score'] = sig.get('score')
            watcher_state['last_confidence_score'] = min(10, abs(float(sig.get('score') or 0)) * 2)
            watcher_state['last_forced_side'] = side
            watcher_state['last_forced_conviction'] = conviction
            watcher_state['last_forced_reasons'] = sig.get('reasons') or []
            watcher_state['last_signal'] = {
                'ticker': ticker,
                'side': side,
                'conviction': conviction,
                'score': sig.get('score'),
                'price': price,
                'setup_type': sig.get('setup_type'),
                'reasons': sig.get('reasons') or [],
                'execution_quality': sig.get('execution_quality') or {},
                'signal_quality': sig.get('signal_quality') or {},
                'btc_regime': sig.get('btc_regime'),
                'tp_price': sig.get('tp_price'),
                'sl_price': sig.get('sl_price'),
                'source': 'ws_scalp',
            }
            if side == 'LONG':
                watcher_state['long_trigger_hit'] = True
                watcher_state['long_setup'] = {
                    'trigger': price,
                    'stop': sig.get('sl_price'),
                    'target': sig.get('tp_price'),
                    'trigger_reason': _watcher_signal_rationale(sig),
                    'source': 'ws_scalp',
                    'conviction': conviction,
                }
            else:
                watcher_state['short_trigger_hit'] = True
                watcher_state['short_setup'] = {
                    'trigger': price,
                    'stop': sig.get('sl_price'),
                    'target': sig.get('tp_price'),
                    'trigger_reason': _watcher_signal_rationale(sig),
                    'source': 'ws_scalp',
                    'conviction': conviction,
                }
            rationale = _watcher_signal_rationale(sig)
            tp_txt = f"TP ${sig.get('tp_price'):.2f}" if sig.get('tp_price') is not None else 'TP n/a'
            sl_txt = f"SL ${sig.get('sl_price'):.2f}" if sig.get('sl_price') is not None else 'SL n/a'
            main = f'{side} {conviction} engine signal @ ${float(price or 0):.2f} · {tp_txt} · {sl_txt}'
            _push_event(
                f'{side.lower()}_trigger',
                main + (f'<br><span class="evt-why">Why: {rationale}</span>' if rationale else ''),
                price,
            )
        _notify(
            f'{ticker} — {side} {conviction} signal',
            f'${float(sig.get("price") or 0):.2f} · {tp_txt} · {sl_txt}' + (f' — {rationale}' if rationale else ''),
        )
    except Exception:
        pass


def _in_rth():
    """Regular Trading Hours gate: Mon-Fri 9:30 AM - 4:00 PM ET."""
    now_et = datetime.now(_ET)
    if now_et.weekday() >= 5:
        return False
    m = now_et.hour * 60 + now_et.minute
    return (9*60 + 30) <= m < (16*60)


def _trigger_rationale(side):
    """Build a 1-2 sentence 'why take this' blurb from current watcher state.
    Uses forced_reasons (already narrative), overall decision, score, BTC intraday,
    and BTC-correlated implied price divergence."""
    side = side.upper()
    decision = (watcher_state.get('last_decision') or '').upper()
    score = watcher_state.get('last_score')
    conv  = watcher_state.get('last_forced_conviction') or ''
    reasons = watcher_state.get('last_forced_reasons') or []
    bi = watcher_state.get('last_btc_intraday') or {}
    implied = watcher_state.get('last_btc_implied_price')
    actual = watcher_state.get('last_price')
    prev_close = watcher_state.get('last_prev_close')
    bits = []
    # Stance vs overall decision
    if decision == side:
        bits.append(f"Overall read agrees ({decision}, score {score:+.1f}/10, {conv.lower()} conviction).")
    elif decision == 'FLAT':
        bits.append(f"Overall read is FLAT (score {score:+.1f}/10) — this is a forced-side {conv.lower()}-conviction setup.")
    elif decision and decision != side:
        bits.append(f"Caution: overall read is {decision} (score {score:+.1f}/10), but level triggered a {side}.")
    # BTC intraday + implied divergence
    if bi and bi.get('bias'):
        bias = bi['bias']
        if (side == 'LONG' and bias == 'bullish') or (side == 'SHORT' and bias == 'bearish'):
            bits.append(f"BTC intraday is {bias} ({bi.get('score',0):+.1f}) — macro tailwind for this {side}.")
        elif (side == 'LONG' and bias == 'bearish') or (side == 'SHORT' and bias == 'bullish'):
            bits.append(f"BTC intraday is {bias} — expect a macro headwind; size small or wait for BTC to turn.")
    if implied and actual and prev_close:
        div_pct = (actual - implied) / implied * 100
        if side == 'LONG' and div_pct < -0.5:
            bits.append(f"Stock is {abs(div_pct):.1f}% below its BTC-implied fair (${implied:.2f}) — mean-reversion long has room.")
        elif side == 'SHORT' and div_pct > 0.5:
            bits.append(f"Stock is {div_pct:.1f}% above its BTC-implied fair (${implied:.2f}) — mean-reversion short has room.")
    # Fill with a top forced reason if we have room
    if len(bits) < 2 and reasons:
        # Pick a reason that isn't redundant with the BTC line above
        for r in reasons:
            if 'BTC' not in r.upper() or not any('BTC' in b.upper() for b in bits):
                bits.append(r.rstrip('.') + '.')
                break
    if not bits:
        return ''
    return ' '.join(bits[:2])


def _check_crossings(price, prev_price):
    """Detect trigger / target / stop crossings. Mutates watcher_state; caller holds lock."""
    if price is None:
        return
    ticker = watcher_state['ticker']
    long_s = watcher_state.get('long_setup') or {}
    short_s = watcher_state.get('short_setup') or {}
    pos = watcher_state.get('position')

    # ── trigger alerts (only during RTH, only if no position open) ──
    # Pre-market volume is too thin — triggers fire on 100-share prints that
    # don't survive the open. Gate alerts to regular trading hours.
    if not pos or pos.get('status') != 'open':
      if _in_rth():
        lt = long_s.get('trigger')
        if lt and prev_price is not None and prev_price < lt <= price and not watcher_state['long_trigger_hit']:
            watcher_state['long_trigger_hit'] = True
            l_tp = long_s.get('target'); l_sl = long_s.get('stop')
            tp_txt = f'TP ${l_tp:.2f}' if l_tp else 'TP n/a'
            sl_txt = f'SL ${l_sl:.2f}' if l_sl else 'SL n/a'
            rationale = _trigger_rationale('LONG')
            main = f'LONG trigger ${lt:.2f} hit — price ${price:.2f} · {tp_txt} · {sl_txt}'
            _push_event('long_trigger',
                        main + (f'<br><span class="evt-why">Why: {rationale}</span>' if rationale else ''),
                        price)
            _notify(f'{ticker} — LONG trigger hit',
                    f'${lt:.2f} crossed · {tp_txt} · {sl_txt}' + (f' — {rationale}' if rationale else ''))
        st = short_s.get('trigger')
        if st and prev_price is not None and prev_price > st >= price and not watcher_state['short_trigger_hit']:
            watcher_state['short_trigger_hit'] = True
            s_tp = short_s.get('target'); s_sl = short_s.get('stop')
            tp_txt = f'TP ${s_tp:.2f}' if s_tp else 'TP n/a'
            sl_txt = f'SL ${s_sl:.2f}' if s_sl else 'SL n/a'
            rationale = _trigger_rationale('SHORT')
            main = f'SHORT trigger ${st:.2f} hit — price ${price:.2f} · {tp_txt} · {sl_txt}'
            _push_event('short_trigger',
                        main + (f'<br><span class="evt-why">Why: {rationale}</span>' if rationale else ''),
                        price)
            _notify(f'{ticker} — SHORT trigger hit',
                    f'${st:.2f} crossed · {tp_txt} · {sl_txt}' + (f' — {rationale}' if rationale else ''))

    # ── position target/stop ──
    if pos and pos.get('status') == 'open':
        side = pos['side']; entry = pos['entry']; stop = pos.get('stop'); target = pos.get('target')
        hit = None
        if side == 'long':
            if target and price >= target: hit = ('target', target)
            elif stop and price <= stop:   hit = ('stop', stop)
            pos['pnl'] = round(price - entry, 2)
        else:  # short
            if target and price <= target: hit = ('target', target)
            elif stop and price >= stop:   hit = ('stop', stop)
            pos['pnl'] = round(entry - price, 2)
        if hit:
            kind, lv = hit
            pnl = (price - entry) if side == 'long' else (entry - price)
            pnl_txt = f'{"+" if pnl >= 0 else ""}${pnl:.2f}/share'
            pos['status'] = f'{kind}_hit'
            pos['closed_price'] = price
            pos['closed_ts'] = int(time.time())
            emoji = '🟢' if kind == 'target' else '🔴'
            _push_event(f'{side}_{kind}', f'{emoji} {side.upper()} {kind.upper()} ${lv:.2f} hit · {pnl_txt}', price)
            _notify(f'{ticker} — {side.upper()} {kind.upper()} HIT', f'${lv:.2f} · {pnl_txt}')


def _check_crossings_unlocked(price, prev_price):
    """Watcher crossing checks that avoid holding the shared lock during toast notifications."""
    if price is None:
        return

    notifications = []
    with _watcher_lock:
        ticker = watcher_state['ticker']
        long_s = watcher_state.get('long_setup') or {}
        short_s = watcher_state.get('short_setup') or {}
        pos = watcher_state.get('position')

        if (not pos or pos.get('status') != 'open') and _in_rth():
            lt = long_s.get('trigger')
            if lt and prev_price is not None and prev_price < lt <= price and not watcher_state['long_trigger_hit']:
                watcher_state['long_trigger_hit'] = True
                l_tp = long_s.get('target'); l_sl = long_s.get('stop')
                tp_txt = f'TP ${l_tp:.2f}' if l_tp else 'TP n/a'
                sl_txt = f'SL ${l_sl:.2f}' if l_sl else 'SL n/a'
                rationale = _trigger_rationale('LONG')
                main = f'LONG trigger ${lt:.2f} hit — price ${price:.2f} · {tp_txt} · {sl_txt}'
                _push_event('long_trigger',
                            main + (f'<br><span class="evt-why">Why: {rationale}</span>' if rationale else ''),
                            price)
                notifications.append((
                    f'{ticker} — LONG trigger hit',
                    f'${lt:.2f} crossed · {tp_txt} · {sl_txt}' + (f' — {rationale}' if rationale else '')
                ))

            st = short_s.get('trigger')
            if st and prev_price is not None and prev_price > st >= price and not watcher_state['short_trigger_hit']:
                watcher_state['short_trigger_hit'] = True
                s_tp = short_s.get('target'); s_sl = short_s.get('stop')
                tp_txt = f'TP ${s_tp:.2f}' if s_tp else 'TP n/a'
                sl_txt = f'SL ${s_sl:.2f}' if s_sl else 'SL n/a'
                rationale = _trigger_rationale('SHORT')
                main = f'SHORT trigger ${st:.2f} hit — price ${price:.2f} · {tp_txt} · {sl_txt}'
                _push_event('short_trigger',
                            main + (f'<br><span class="evt-why">Why: {rationale}</span>' if rationale else ''),
                            price)
                notifications.append((
                    f'{ticker} — SHORT trigger hit',
                    f'${st:.2f} crossed · {tp_txt} · {sl_txt}' + (f' — {rationale}' if rationale else '')
                ))

        if pos and pos.get('status') == 'open':
            side = pos['side']; entry = pos['entry']; stop = pos.get('stop'); target = pos.get('target')
            hit = None
            if side == 'long':
                if target and price >= target: hit = ('target', target)
                elif stop and price <= stop:   hit = ('stop', stop)
                pos['pnl'] = round(price - entry, 2)
            else:
                if target and price <= target: hit = ('target', target)
                elif stop and price >= stop:   hit = ('stop', stop)
                pos['pnl'] = round(entry - price, 2)
            if hit:
                kind, lv = hit
                pnl = (price - entry) if side == 'long' else (entry - price)
                pnl_txt = f'{"+" if pnl >= 0 else ""}${pnl:.2f}/share'
                pos['status'] = f'{kind}_hit'
                pos['closed_price'] = price
                pos['closed_ts'] = int(time.time())
                emoji = '🟢' if kind == 'target' else '🔴'
                _push_event(f'{side}_{kind}', f'{emoji} {side.upper()} {kind.upper()} ${lv:.2f} hit · {pnl_txt}', price)
                notifications.append((
                    f'{ticker} — {side.upper()} {kind.upper()} HIT',
                    f'${lv:.2f} · {pnl_txt}'
                ))

    for title, msg in notifications:
        _notify(title, msg)


def _check_watcher_position_unlocked(price):
    """Track manually-recorded Watcher positions without emitting old playbook entry alerts."""
    if price is None:
        return

    notifications = []
    with _watcher_lock:
        ticker = watcher_state['ticker']
        pos = watcher_state.get('position')
        if pos and pos.get('status') == 'open':
            side = pos['side']; entry = pos['entry']; stop = pos.get('stop'); target = pos.get('target')
            hit = None
            if side == 'long':
                if target and price >= target: hit = ('target', target)
                elif stop and price <= stop:   hit = ('stop', stop)
                pos['pnl'] = round(price - entry, 2)
            else:
                if target and price <= target: hit = ('target', target)
                elif stop and price >= stop:   hit = ('stop', stop)
                pos['pnl'] = round(entry - price, 2)
            if hit:
                kind, lv = hit
                pnl = (price - entry) if side == 'long' else (entry - price)
                pnl_txt = f'{"+" if pnl >= 0 else ""}${pnl:.2f}/share'
                pos['status'] = f'{kind}_hit'
                pos['closed_price'] = price
                pos['closed_ts'] = int(time.time())
                emoji = '🟢' if kind == 'target' else '🔴'
                _push_event(f'{side}_{kind}', f'{emoji} {side.upper()} {kind.upper()} ${lv:.2f} hit · {pnl_txt}', price)
                notifications.append((
                    f'{ticker} — {side.upper()} {kind.upper()} HIT',
                    f'${lv:.2f} · {pnl_txt}'
                ))

    for title, msg in notifications:
        _notify(title, msg)


def _watcher_loop(ticker, btc_correlated, interval):
    prev_price = None
    # Let /watcher/start finish its HTTP response before the watcher makes a
    # loopback request into /analyze on the same Flask process.
    if _watcher_stop_evt.wait(0.25):
        return
    while not _watcher_stop_evt.is_set():
        try:
            r = requests.post(
                'http://127.0.0.1:5000/analyze',
                json={'ticker': ticker, 'btc_correlated': btc_correlated},
                timeout=20,
            )
            data = r.json() if r.ok else {'error': f'HTTP {r.status_code}'}
            now_ts = int(time.time())
            with _watcher_lock:
                watcher_state['last_check'] = now_ts
                watcher_state['next_check_ts'] = now_ts + interval
                if 'error' in data:
                    watcher_state['last_error'] = data['error']
                else:
                    watcher_state['last_error'] = None
                    # Watcher alerts now come only from the shared ws_scalp
                    # signal callback used by MockTrader. This polling loop is
                    # retained for price/BTC context and manual position tracking.
                    ana = data.get('analysis') or {}
                    watcher_state['last_btc_intraday'] = data.get('btc_intraday')
                    watcher_state['last_market_closed'] = ana.get('market_closed')
                    watcher_state['last_expected_gap_pct'] = ana.get('expected_gap_pct')
                    watcher_state['last_predicted_open_price'] = ana.get('predicted_open_price')
                    watcher_state['last_beta_used'] = ana.get('beta_used')
                    watcher_state['last_btc_implied_price'] = ana.get('btc_implied_price')
                    watcher_state['last_btc_implied_beta']  = ana.get('btc_implied_beta')
                    watcher_state['last_btc_current']       = ana.get('btc_current')
                    watcher_state['last_btc_pct_change']    = ana.get('btc_pct_change')
                    watcher_state['last_high_corr']         = ana.get('high_btc_correlation')
                    watcher_state['last_prev_close']        = (data.get('latest') or {}).get('prev_close')
                    price = (data.get('latest') or {}).get('price')
                    watcher_state['last_price'] = price
                    prev_price_next = price
            if 'error' not in data:
                _check_watcher_position_unlocked(price)
                prev_price = prev_price_next
        except Exception as e:
            with _watcher_lock:
                watcher_state['last_error'] = str(e)[:200]
        _watcher_stop_evt.wait(interval)


def _ensure_watcher_signal_engine(ticker, btc_correlated):
    """Attach Watcher to the shared ws_scalp engine used by MockTrader."""
    eng = _get_scalp_engine()
    eng.on_signal(_watcher_on_signal)
    ticker = ticker.upper().strip()
    need_btc = bool(btc_correlated or ticker in HIGH_BTC_CORRELATION)
    subscribed = set(eng.get_all_subscribed())
    active = getattr(eng, 'active_ticker', None)

    if need_btc and not getattr(eng, 'btc_enabled', False):
        eng.subscribe(active or ticker, btc=True)
        subscribed = set(eng.get_all_subscribed())

    if not subscribed:
        eng.subscribe(ticker, btc=need_btc)
    elif ticker not in subscribed:
        eng.add_ticker(ticker)
    return eng


@app.route('/backtest_report', methods=['POST'])
def backtest_report():
    """
    Detailed per-day backtest: for each trading day, report ticker close,
    BTC close (4pm ET prior day), BTC at 3am CT current day, predicted
    stock price (β × BTC move + weekend bias), and actual stock price at 3am CT.
    Returns CSV-ready JSON.
    """
    body = request.json or {}
    ticker = (body.get('ticker') or '').upper().strip()
    days = int(body.get('days', 180))
    if not ticker:
        return jsonify({'error': 'ticker required'}), 400
    beta = get_btc_beta(ticker)
    if beta is None:
        return jsonify({'error': f'No β on file for {ticker}'}), 400

    try:
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=days + 5)

        # Daily bars for close prices + date list
        daily = alpaca_stock_bars(ticker, start_dt.strftime('%Y-%m-%d'),
                                  end_dt.strftime('%Y-%m-%d'), timeframe='1Day')
        trading_days = []
        for b in daily:
            dt = datetime.fromtimestamp(b['t'] / 1000, tz=timezone.utc)
            et_date = dt.astimezone(_ET).date()
            trading_days.append({'date': et_date, 'close': b['c']})
        trading_days.sort(key=lambda x: x['date'])

        # Stock 15-min bars (includes premarket via feed=sip)
        stock_15m = alpaca_stock_bars(ticker, start_dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
                                      end_dt.strftime('%Y-%m-%dT%H:%M:%SZ'), timeframe='15Min')
        s_ts = np.array([b['t'] for b in stock_15m])
        s_c  = np.array([b['c'] for b in stock_15m])
        def stock_price_at(dt_utc, window_min=45):
            target = int(dt_utc.timestamp() * 1000)
            if len(s_ts) == 0: return None
            idx = int(np.argmin(np.abs(s_ts - target)))
            if abs(s_ts[idx] - target) > window_min * 60 * 1000:
                return None
            return float(s_c[idx])

        # BTC 15-min bars
        btc_bars = alpaca_crypto_bars('BTC/USD',
            start_dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
            end_dt.strftime('%Y-%m-%dT%H:%M:%SZ'), timeframe='15Min')
        b_ts = np.array([b['t'] for b in btc_bars])
        b_c  = np.array([b['c'] for b in btc_bars])
        def btc_price_at(dt_utc):
            target = int(dt_utc.timestamp() * 1000)
            idx = int(np.argmin(np.abs(b_ts - target)))
            if abs(b_ts[idx] - target) > 30 * 60 * 1000:
                return None
            return float(b_c[idx])

        rows = []
        dir_hits = dir_total = 0
        abs_errs = []
        within_1 = within_2 = within_3 = 0
        meaningful = 0
        for i in range(1, len(trading_days)):
            prev = trading_days[i - 1]
            curr = trading_days[i]
            gap_days = (curr['date'] - prev['date']).days
            is_weekend = gap_days > 1

            # 4pm ET on prior day
            d = prev['date']
            prev_close_utc = datetime(d.year, d.month, d.day, 16, 0, 0, tzinfo=_ET).astimezone(timezone.utc)
            # 4am ET on current day (= 3am CT)
            d = curr['date']
            pre_open_utc = datetime(d.year, d.month, d.day, 4, 0, 0, tzinfo=_ET).astimezone(timezone.utc)

            btc_close = btc_price_at(prev_close_utc)
            btc_3am   = btc_price_at(pre_open_utc)
            stock_3am = stock_price_at(pre_open_utc, window_min=120)  # premarket is sparse
            if not btc_close or not btc_3am:
                continue

            btc_move_pct = (btc_3am - btc_close) / btc_close * 100
            # Asymmetric β per-direction (if available)
            beta_dir = get_btc_beta(ticker, btc_move_pct) or beta
            if is_weekend and abs(btc_move_pct) < 2.0:
                weekend_bias = 1.1
            elif is_weekend:
                weekend_bias = max(0.0, 1.1 * (1 - (abs(btc_move_pct) - 2) / 2))
            else:
                weekend_bias = 0.0
            predicted_pct = btc_move_pct * beta_dir + weekend_bias
            predicted_price = prev['close'] * (1 + predicted_pct / 100.0)

            row = {
                'ticker': ticker,
                'prev_trading_day': prev['date'].isoformat(),
                'trading_day': curr['date'].isoformat(),
                'is_monday_after_weekend': is_weekend,
                'ticker_close_prev_day': round(prev['close'], 2),
                'btc_at_close_prev_day': round(btc_close, 2),
                'btc_at_3am_ct': round(btc_3am, 2),
                'btc_move_pct': round(btc_move_pct, 2),
                'beta': round(beta_dir, 3),
                'weekend_bias_applied_pct': round(weekend_bias, 2),
                'predicted_move_pct': round(predicted_pct, 2),
                'predicted_ticker_price_at_3am': round(predicted_price, 2),
                'actual_ticker_price_at_3am': round(stock_3am, 2) if stock_3am else None,
            }
            if stock_3am:
                actual_pct = (stock_3am - prev['close']) / prev['close'] * 100
                err_pct = actual_pct - predicted_pct
                err_abs_price = stock_3am - predicted_price
                row['actual_move_pct'] = round(actual_pct, 2)
                row['error_pct'] = round(err_pct, 2)
                row['abs_error_pct'] = round(abs(err_pct), 2)
                row['error_dollars'] = round(err_abs_price, 2)
                # Direction accuracy (only when BTC moved meaningfully)
                if abs(btc_move_pct) >= 0.3:
                    dir_total += 1
                    if (predicted_pct > 0) == (actual_pct > 0):
                        dir_hits += 1
                abs_errs.append(abs(err_pct))
                if abs(err_pct) <= 1.0: within_1 += 1
                if abs(err_pct) <= 2.0: within_2 += 1
                if abs(err_pct) <= 3.0: within_3 += 1
                if abs(actual_pct) >= 0.5: meaningful += 1
            rows.append(row)

        n = len([r for r in rows if r.get('actual_move_pct') is not None])
        summary = {
            'ticker': ticker, 'beta': beta,
            'n_with_actual': n,
            'direction_accuracy_pct': round(100 * dir_hits / dir_total, 1) if dir_total else None,
            'direction_sample_n': dir_total,
            'within_1pct_pct': round(100 * within_1 / n, 1) if n else None,
            'within_2pct_pct': round(100 * within_2 / n, 1) if n else None,
            'within_3pct_pct': round(100 * within_3 / n, 1) if n else None,
            'mean_abs_error_pct': round(float(np.mean(abs_errs)), 2) if abs_errs else None,
        }
        return jsonify({'summary': summary, 'rows': rows})
    except Exception as e:
        return jsonify({'error': f'Server error: {str(e)}'}), 500


@app.route('/backtest_beta', methods=['POST'])
def backtest_beta():
    """
    Backtest the hardcoded β from get_btc_beta(ticker) against historical
    overnight gaps. For each trading day, predict the open-gap from BTC's
    overnight move × β, compare to actual stock open-gap, and report hit rates.
    """
    body = request.json or {}
    ticker = (body.get('ticker') or '').upper().strip()
    days = int(body.get('days', 180))
    # Thresholds: skip "noise" nights where BTC barely moved
    min_btc_move = float(body.get('min_btc_move', 0.3))   # pct
    if not ticker:
        return jsonify({'error': 'ticker required'}), 400

    beta = get_btc_beta(ticker)
    if beta is None:
        return jsonify({'error': f'No β on file for {ticker}'}), 400

    try:
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=days + 5)
        stock_bars = alpaca_stock_bars(ticker, start_dt.strftime('%Y-%m-%d'),
                                        end_dt.strftime('%Y-%m-%d'), timeframe='1Day')
        if len(stock_bars) < 20:
            return jsonify({'error': f'only {len(stock_bars)} bars'}), 400
        trading_days = []
        for b in stock_bars:
            dt = datetime.fromtimestamp(b['t'] / 1000, tz=timezone.utc)
            et_date = dt.astimezone(_ET).date()
            trading_days.append({'date': et_date, 'open': b['o'], 'close': b['c']})
        trading_days.sort(key=lambda x: x['date'])

        btc_bars = alpaca_crypto_bars('BTC/USD',
            start_dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
            end_dt.strftime('%Y-%m-%dT%H:%M:%SZ'), timeframe='15Min')
        btc_ts = np.array([b['t'] for b in btc_bars])
        btc_closes = np.array([b['c'] for b in btc_bars])
        def btc_price_at(dt_utc):
            target_ms = int(dt_utc.timestamp() * 1000)
            idx = int(np.argmin(np.abs(btc_ts - target_ms)))
            if abs(btc_ts[idx] - target_ms) > 30 * 60 * 1000:
                return None
            return float(btc_closes[idx])

        all_samples = []
        week_samples = []
        weekend_samples = []
        for i in range(1, len(trading_days)):
            prev = trading_days[i - 1]
            curr = trading_days[i]
            gap_days = (curr['date'] - prev['date']).days
            d = prev['date']
            prev_close_utc = datetime(d.year, d.month, d.day, 16, 0, 0, tzinfo=_ET).astimezone(timezone.utc)
            d = curr['date']
            curr_open_utc  = datetime(d.year, d.month, d.day, 9, 30, 0, tzinfo=_ET).astimezone(timezone.utc)
            bac = btc_price_at(prev_close_utc); bao = btc_price_at(curr_open_utc)
            if not bac or not bao: continue
            stock_gap = (curr['open'] - prev['close']) / prev['close'] * 100
            btc_gap   = (bao - bac) / bac * 100
            if abs(stock_gap) > 25 or abs(btc_gap) > 15: continue
            predicted = btc_gap * beta
            err = stock_gap - predicted
            sample = {
                'date': curr['date'].isoformat(),
                'weekend': gap_days > 1,
                'btc_gap': round(btc_gap, 2),
                'actual_stock_gap': round(stock_gap, 2),
                'predicted_stock_gap': round(predicted, 2),
                'error': round(err, 2),
                'abs_error': round(abs(err), 2),
            }
            all_samples.append(sample)
            (weekend_samples if gap_days > 1 else week_samples).append(sample)

        def stats(samples):
            if not samples:
                return None
            # Filter to meaningful-move nights for direction accuracy
            directional = [s for s in samples if abs(s['btc_gap']) >= min_btc_move]
            # Direction hit: sign(predicted) == sign(actual), ignoring near-zero actual
            dir_hits = sum(1 for s in directional
                           if (s['predicted_stock_gap'] > 0) == (s['actual_stock_gap'] > 0))
            dir_total = len(directional)
            # Magnitude hit rates at various tolerances
            tol_1pct = sum(1 for s in samples if s['abs_error'] <= 1.0)
            tol_2pct = sum(1 for s in samples if s['abs_error'] <= 2.0)
            tol_3pct = sum(1 for s in samples if s['abs_error'] <= 3.0)
            # Within-50%-relative: prediction within ±50% of actual magnitude (only when actual is meaningful)
            meaningful = [s for s in samples if abs(s['actual_stock_gap']) >= 0.5]
            within_half = sum(1 for s in meaningful
                              if abs(s['abs_error']) <= 0.5 * abs(s['actual_stock_gap']))
            mae = round(float(np.mean([s['abs_error'] for s in samples])), 2)
            # Bias: avg signed error. Positive = we under-predicted the gap.
            bias = round(float(np.mean([s['error'] for s in samples])), 2)
            return {
                'n': len(samples),
                'direction_accuracy_pct': round(100 * dir_hits / dir_total, 1) if dir_total else None,
                'direction_sample_n': dir_total,
                'within_1pct_pct': round(100 * tol_1pct / len(samples), 1),
                'within_2pct_pct': round(100 * tol_2pct / len(samples), 1),
                'within_3pct_pct': round(100 * tol_3pct / len(samples), 1),
                'within_50pct_relative': round(100 * within_half / len(meaningful), 1) if meaningful else None,
                'within_50pct_n': len(meaningful),
                'mean_abs_error_pct': mae,
                'mean_signed_error_pct': bias,
            }

        # Top 5 worst misses for color
        worst = sorted(all_samples, key=lambda s: s['abs_error'], reverse=True)[:5]
        return jsonify({
            'ticker': ticker, 'beta_tested': beta, 'lookback_days': days,
            'min_btc_move_filter': min_btc_move,
            'overall':  stats(all_samples),
            'weeknight': stats(week_samples),
            'weekend':   stats(weekend_samples),
            'worst_misses': worst,
        })
    except Exception as e:
        return jsonify({'error': f'Server error: {str(e)}'}), 500


@app.route('/compute_beta', methods=['POST'])
def compute_beta():
    """
    Empirical overnight-gap beta: regress stock overnight % move on BTC % move
    across the same absolute time window (prev stock close 16:00 ET → next stock
    open 09:30 ET). Uses ~6 months of daily bars by default.
    """
    body = request.json or {}
    ticker = (body.get('ticker') or '').upper().strip()
    days = int(body.get('days', 180))
    if not ticker:
        return jsonify({'error': 'ticker required'}), 400

    try:
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=days + 5)

        # 1) Stock daily bars
        stock_bars = alpaca_stock_bars(
            ticker,
            start_dt.strftime('%Y-%m-%d'),
            end_dt.strftime('%Y-%m-%d'),
            timeframe='1Day',
        )
        if len(stock_bars) < 20:
            return jsonify({'error': f'Not enough history — got {len(stock_bars)} daily bars'}), 400

        # Daily bars from Alpaca come timestamped at session open UTC. Map each to a
        # calendar date (ET) with open + close.
        trading_days = []
        for b in stock_bars:
            dt = datetime.fromtimestamp(b['t'] / 1000, tz=timezone.utc)
            et_date = dt.astimezone(_ET).date()
            trading_days.append({
                'date': et_date,
                'open': b['o'],
                'close': b['c'],
            })
        trading_days.sort(key=lambda x: x['date'])

        # 2) BTC 15-min bars for entire window (one pull, paginated)
        btc_bars = alpaca_crypto_bars(
            'BTC/USD',
            start_dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
            end_dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
            timeframe='15Min',
        )
        if len(btc_bars) < 100:
            return jsonify({'error': f'Not enough BTC history — got {len(btc_bars)} bars'}), 400

        # Index BTC bars by timestamp for fast lookup
        btc_ts = np.array([b['t'] for b in btc_bars])
        btc_closes = np.array([b['c'] for b in btc_bars])

        def btc_price_at(dt_utc):
            """Find BTC close at the closest 15-min bar within ±30 min."""
            target_ms = int(dt_utc.timestamp() * 1000)
            idx = int(np.argmin(np.abs(btc_ts - target_ms)))
            if abs(btc_ts[idx] - target_ms) > 30 * 60 * 1000:
                return None
            return float(btc_closes[idx])

        # 3) Build (btc_gap, stock_gap, weekend) samples
        samples_week = []   # Mon→Fri overnights
        samples_weekend = []  # Fri→Mon (or similar across non-trading days)
        raw_points = []

        for i in range(1, len(trading_days)):
            prev = trading_days[i - 1]
            curr = trading_days[i]
            gap_days = (curr['date'] - prev['date']).days

            d = prev['date']
            prev_close_utc = datetime(d.year, d.month, d.day, 16, 0, 0, tzinfo=_ET).astimezone(timezone.utc)
            d = curr['date']
            curr_open_utc  = datetime(d.year, d.month, d.day,  9, 30, 0, tzinfo=_ET).astimezone(timezone.utc)

            btc_at_close = btc_price_at(prev_close_utc)
            btc_at_open  = btc_price_at(curr_open_utc)
            if btc_at_close is None or btc_at_open is None or btc_at_close == 0:
                continue

            stock_gap = (curr['open'] - prev['close']) / prev['close'] * 100
            btc_gap   = (btc_at_open - btc_at_close) / btc_at_close * 100

            # Filter extreme outliers (news-driven moves, splits, halts)
            if abs(stock_gap) > 25 or abs(btc_gap) > 15:
                continue

            point = {
                'date': curr['date'].isoformat(),
                'btc_gap': round(btc_gap, 3),
                'stock_gap': round(stock_gap, 3),
                'gap_days': gap_days,
            }
            raw_points.append(point)
            if gap_days > 1:
                samples_weekend.append((btc_gap, stock_gap))
            else:
                samples_week.append((btc_gap, stock_gap))

        def regress(samples):
            if len(samples) < 5:
                return None
            x = np.array([s[0] for s in samples])
            y = np.array([s[1] for s in samples])
            # OLS with intercept
            slope, intercept = np.polyfit(x, y, 1)
            y_pred = slope * x + intercept
            ss_res = np.sum((y - y_pred) ** 2)
            ss_tot = np.sum((y - y.mean()) ** 2)
            r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0
            # Slope through origin (often more faithful to true beta)
            beta_zero = float(np.sum(x * y) / np.sum(x * x)) if np.sum(x * x) > 0 else None
            return {
                'beta': round(float(slope), 3),
                'beta_through_origin': round(beta_zero, 3) if beta_zero is not None else None,
                'intercept': round(float(intercept), 3),
                'r_squared': round(float(r2), 3),
                'n': len(samples),
                'avg_stock_gap': round(float(y.mean()), 3),
                'avg_btc_gap':   round(float(x.mean()), 3),
            }

        all_samples = samples_week + samples_weekend
        return jsonify({
            'ticker': ticker,
            'lookback_days': days,
            'overall':  regress(all_samples),
            'weeknight': regress(samples_week),
            'weekend':   regress(samples_weekend),
            'sample_count': len(all_samples),
            'data_points': raw_points[-60:],  # last 60 for optional scatter
        })

    except Exception as e:
        return jsonify({'error': f'Server error: {str(e)}'}), 500


def _watcher_start_impl():
    global _watcher_thread
    body = request.json or {}
    ticker = (body.get('ticker') or '').upper().strip()
    if not ticker:
        return jsonify({'error': 'Ticker required'}), 400
    btc_correlated = bool(body.get('btc_correlated', False))
    interval = int(body.get('interval', 30))
    try:
        _ensure_watcher_signal_engine(ticker, btc_correlated)
    except Exception as e:
        return jsonify({'error': f'Could not start shared signal engine: {e}'}), 500

    with _watcher_lock:
        if watcher_state['running']:
            return jsonify({'error': 'Already running', 'state': watcher_state}), 409
        _watcher_stop_evt.clear()
        watcher_state.update({
            'running': True, 'ticker': ticker, 'btc_correlated': btc_correlated,
            'signal_source': 'ws_scalp',
            'min_alert_conviction': sorted(_WATCHER_MIN_ALERT_CONVICTION),
            'interval': interval, 'last_check': None, 'last_decision': None,
            'last_price': None, 'last_error': None,
            'next_check_ts': int(time.time()) + 1,
            'long_setup': None, 'short_setup': None,
            'long_trigger_hit': False, 'short_trigger_hit': False,
            'position': None, 'events': [],
        })
    _watcher_thread = threading.Thread(
        target=_watcher_loop, args=(ticker, btc_correlated, interval), daemon=True)
    _watcher_thread.start()
    # Persist so autolaunch-on-wake can restore
    _save_watcher_prefs({
        'running': True, 'ticker': ticker,
        'btc_correlated': btc_correlated, 'interval': interval,
    })
    return jsonify({'ok': True, 'state': watcher_state})


def _watcher_stop_impl():
    with _watcher_lock:
        watcher_state['running'] = False
    _watcher_stop_evt.set()
    _save_watcher_prefs({'running': False})
    return jsonify({'ok': True})


def _watcher_status_impl():
    with _watcher_lock:
        return jsonify(dict(watcher_state))


def _watcher_enter_impl():
    body = request.json or {}
    side = (body.get('side') or '').lower()
    if side not in ('long', 'short'):
        return jsonify({'error': "side must be 'long' or 'short'"}), 400
    try:
        entry = float(body.get('entry'))
        stop = float(body.get('stop')) if body.get('stop') not in (None, '') else None
        target = float(body.get('target')) if body.get('target') not in (None, '') else None
    except (TypeError, ValueError):
        return jsonify({'error': 'entry/stop/target must be numbers'}), 400
    # Sanity: stop and target on correct sides
    if side == 'long':
        if stop is not None and stop >= entry:  return jsonify({'error': 'Long stop must be below entry'}), 400
        if target is not None and target <= entry: return jsonify({'error': 'Long target must be above entry'}), 400
    else:
        if stop is not None and stop <= entry:  return jsonify({'error': 'Short stop must be above entry'}), 400
        if target is not None and target >= entry: return jsonify({'error': 'Short target must be below entry'}), 400
    with _watcher_lock:
        if not watcher_state['running']:
            return jsonify({'error': 'Start the watcher first'}), 400
        watcher_state['position'] = {
            'side': side, 'entry': round(entry, 2),
            'stop': round(stop, 2) if stop is not None else None,
            'target': round(target, 2) if target is not None else None,
            'opened_ts': int(time.time()), 'status': 'open', 'pnl': 0.0,
        }
        stop_s = f'${stop:.2f}' if stop is not None else '—'
        target_s = f'${target:.2f}' if target is not None else '—'
        _push_event('entered', f'{side.upper()} entered @ ${entry:.2f} · stop {stop_s} · target {target_s}')
    _notify(f'{watcher_state["ticker"]} — {side.upper()} ENTERED',
            f'Entry ${entry:.2f} · target ${target if target else "—"} · stop ${stop if stop else "—"}')
    return jsonify({'ok': True, 'position': watcher_state['position']})


def _watcher_exit_impl():
    with _watcher_lock:
        pos = watcher_state.get('position')
        if not pos:
            return jsonify({'error': 'No position'}), 400
        pos['status'] = 'closed'
        pos['closed_ts'] = int(time.time())
        pos['closed_price'] = watcher_state.get('last_price')
        _push_event('exited', f'{pos["side"].upper()} manually closed @ ${pos.get("closed_price") or 0:.2f}')
    return jsonify({'ok': True, 'position': pos})


# ─── Scalp engine (WS-driven 1s bars) ──────────────────────────────────
import ws_scalp
_scalp_engine = None
_SCALP_STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                 'scalp_state.json')
_scalp_state_cache = None
_scalp_state_mtime = None

def _load_scalp_state(force=False):
    global _scalp_state_cache, _scalp_state_mtime
    try:
        mtime = os.path.getmtime(_SCALP_STATE_FILE)
        if not force and _scalp_state_cache is not None and _scalp_state_mtime == mtime:
            return dict(_scalp_state_cache)
        with open(_SCALP_STATE_FILE, 'r', encoding='utf-8') as f:
            raw = f.read()
        state = json.loads(raw) if raw.strip() else {}
        _scalp_state_cache = dict(state)
        _scalp_state_mtime = mtime
        return dict(state)
    except FileNotFoundError:
        _scalp_state_cache = {}
        _scalp_state_mtime = None
        return {}
    except Exception as e:
        print(f'[scalp] load failed: {e}', flush=True)
        return {}

def _save_scalp_state(s):
    global _scalp_state_cache, _scalp_state_mtime
    try:
        with open(_SCALP_STATE_FILE, 'w', encoding='utf-8') as f:
            json.dump(s, f, indent=2)
        _scalp_state_cache = dict(s)
        _scalp_state_mtime = os.path.getmtime(_SCALP_STATE_FILE)
    except Exception as e:
        print(f'[scalp] state save failed: {e}')

def _get_scalp_engine():
    global _scalp_engine
    if _scalp_engine is None:
        _scalp_engine = ws_scalp.get_engine(ALPACA_API_KEY, ALPACA_SECRET_KEY)
        def _on_sig(sig):
            conv = sig.get('conviction')
            if conv in ('HIGH', 'MEDIUM'):
                pref = _load_scalp_state().get('toast_level', 'HIGH')
                if pref == 'HIGH' and conv != 'HIGH':
                    return
                reasons = ' · '.join(sig.get('reasons', [])[:2])
                _notify(
                    f"{sig['side']} {sig['ticker']} @ ${sig.get('price',0):.2f} [{conv}]",
                    reasons or f"score={sig.get('score')}"
                )
        _scalp_engine.on_signal(_on_sig)
    return _scalp_engine


def _is_mock_running():
    try:
        return bool(_mock_trader and _mock_trader.status().get('running'))
    except Exception:
        return False


def _scalp_autostart():
    """On Flask boot, if scalp_state.json says scalp_on=true, reconnect WS."""
    print('[scalp] autostart check...', flush=True)
    s = _load_scalp_state()
    print(f'[scalp] loaded state: {s}', flush=True)
    if not s.get('scalp_on'):
        print('[scalp] scalp_on=false, skipping autostart', flush=True)
        return
    tkr = s.get('ticker')
    if not tkr:
        print('[scalp] no ticker in state, skipping', flush=True)
        return
    try:
        eng = _get_scalp_engine()
        eng.subscribe(tkr, btc=bool(s.get('btc')))
        print(f'[scalp] auto-started for {tkr} (btc={s.get("btc")})', flush=True)
    except Exception as e:
        import traceback
        print(f'[scalp] autostart failed: {e}', flush=True)
        traceback.print_exc()


# ─── Watcher persistence + autostart ────────────────────────────────────
_WATCHER_STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   'watcher_prefs.json')

def _load_watcher_prefs():
    try:
        with open(_WATCHER_STATE_FILE, 'r') as f: return json.load(f)
    except Exception: return {}

def _save_watcher_prefs(prefs):
    try:
        with open(_WATCHER_STATE_FILE, 'w') as f: json.dump(prefs, f, indent=2)
    except Exception as e: print(f'[watcher] prefs save failed: {e}')

def _watcher_autostart():
    """Restore watcher from last-known running state on Flask boot."""
    prefs = _load_watcher_prefs()
    if not prefs.get('running'): return
    tkr = prefs.get('ticker')
    if not tkr: return
    global _watcher_thread
    try:
        _ensure_watcher_signal_engine(tkr, bool(prefs.get('btc_correlated')))
        with _watcher_lock:
            if watcher_state['running']: return  # already running somehow
            _watcher_stop_evt.clear()
            watcher_state.update({
                'running': True, 'ticker': tkr,
                'btc_correlated': bool(prefs.get('btc_correlated')),
                'signal_source': 'ws_scalp',
                'min_alert_conviction': sorted(_WATCHER_MIN_ALERT_CONVICTION),
                'interval': int(prefs.get('interval', 30)),
                'last_check': None, 'last_price': None, 'last_error': None,
                'next_check_ts': int(time.time()) + 5,
                'long_setup': None, 'short_setup': None,
                'long_trigger_hit': False, 'short_trigger_hit': False,
                'position': None, 'events': [],
            })
        _watcher_thread = threading.Thread(
            target=_watcher_loop,
            args=(tkr, bool(prefs.get('btc_correlated')), int(prefs.get('interval', 30))),
            daemon=True)
        _watcher_thread.start()
        print(f'[watcher] auto-started for {tkr}')
    except Exception as e:
        print(f'[watcher] autostart failed: {e}')


def _daily_beta_refit_startup():
    """
    Once per day on Flask startup, recompute BTC betas from last 180 days and
    rewrite TICKER_BTC_BETAS in-place if values drifted. Runs in a background
    thread so Flask boot is not blocked. Skips restarting Flask — new values
    are picked up on the next natural startup (wake/reboot).
    """
    import os, sys, time, threading, subprocess
    def _run():
        time.sleep(60)  # let Flask fully come up (watcher, routes, Alpaca warm)
        try:
            env = os.environ.copy()
            env['CLAUDE_REFIT_NO_RESTART'] = '1'  # don't kill ourselves
            here = os.path.dirname(os.path.abspath(__file__))
            subprocess.run(
                [sys.executable, os.path.join(here, 'refit_betas.py')],
                env=env, cwd=here,
                timeout=180, capture_output=True,
            )
        except Exception as e:
            print(f'[beta-refit] background job failed: {e}')
    threading.Thread(target=_run, daemon=True, name='beta-refit').start()


# ─── Mock Trader (paper-trading on 1s scalp signals) ────────────────────
import mock_trader
_MOCK_STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                'mock_trader_state.json')
_mock_trader = None

def _get_mock_trader():
    global _mock_trader
    if _mock_trader is None:
        eng = _get_scalp_engine()
        _mock_trader = mock_trader.get_trader(eng, _MOCK_STATE_FILE)
    return _mock_trader


from routes_scalp import register_scalp_routes
from routes_mock import register_mock_routes
from routes_watcher import register_watcher_routes

register_watcher_routes(app, _watcher_start_impl, _watcher_stop_impl,
                        _watcher_status_impl, _watcher_enter_impl,
                        _watcher_exit_impl)
register_scalp_routes(app, _get_scalp_engine, _load_scalp_state,
                      _save_scalp_state, _is_mock_running)
register_mock_routes(app, _get_mock_trader)


def _mock_autostart():
    """Boot the mock trader on Flask startup so it's ready at wake-up."""
    print('[mock] autostart...', flush=True)
    try:
        if os.getenv('MOCK_AUTOSTART', '1') != '1':
            print('[mock] MOCK_AUTOSTART!=1, skipping', flush=True)
            return
        mt = _get_mock_trader()
        s = mt.status()
        if not s.get('running'):
            print('[mock] not running; attempting start...', flush=True)
            resp = mt.start()
            print(f'[mock] start => {resp}', flush=True)
            s = mt.status()
        print(f'[mock] running={s["running"]} balance=${s["balance"]:.2f} '
              f'open_positions={len(s.get("positions",{}))} '
              f'trade_count={s["trade_count"]} in_window={s["in_window"]}',
              flush=True)
    except Exception as e:
        import traceback
        print(f'[mock] autostart failed: {e}', flush=True)
        traceback.print_exc()


_BOOT_APP_DONE = False


def boot_app():
    # Install crash/shutdown diagnostics FIRST so they cover the rest of
    # startup. Writes a 10s heartbeat file, installs faulthandler for
    # C-level crashes, excepthooks for unhandled exceptions in any
    # thread, and signal+atexit handlers to distinguish OS-kill from
    # silent-death. Boot logs report downtime since last heartbeat.
    global _BOOT_APP_DONE
    if _BOOT_APP_DONE:
        return
    _BOOT_APP_DONE = True
    import process_monitor
    (getattr(process_monitor, 'install_v2', None) or process_monitor.install)()

    _daily_beta_refit_startup()
    _watcher_autostart()
    _scalp_autostart()
    _mock_autostart()


if __name__ == '__main__':
    from runtime_guard import SingleInstanceLock, rotate_runtime_logs, status_reachable

    lock = SingleInstanceLock()
    if not lock.acquire():
        if status_reachable('http://127.0.0.1:5000/mock/status', timeout=3):
            print('[server] existing app is healthy at http://127.0.0.1:5000', flush=True)
            raise SystemExit(0)
        print('[server] refused to start: app lock is held but status is unreachable', flush=True)
        raise SystemExit(2)
    try:
        rotate_runtime_logs()
        boot_app()
        # use_reloader=False prevents duplicate watcher/scalp/mock threads.
        app.run(debug=False, port=5000, use_reloader=False, threaded=True)
    finally:
        lock.release()
