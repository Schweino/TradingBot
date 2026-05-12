"""
Alpaca Data API helpers.

Standalone — no Flask dependency. Safe to import from any script,
backtest, or live-trading module without triggering Flask startup.
"""
import os
import time
from datetime import datetime

import requests
from dotenv import load_dotenv

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore

load_dotenv()

_API_KEY    = os.getenv('ALPACA_API_KEY')
_SECRET_KEY = os.getenv('ALPACA_SECRET_KEY')
_HEADERS    = {
    'APCA-API-KEY-ID':     _API_KEY,
    'APCA-API-SECRET-KEY': _SECRET_KEY,
}

_ET = ZoneInfo('America/New_York')


def _iso_to_ms(ts: str) -> int:
    """Convert Alpaca ISO8601 timestamp ('2026-04-17T13:30:00Z') to unix ms."""
    return int(datetime.fromisoformat(ts.replace('Z', '+00:00')).timestamp() * 1000)


def _alpaca_get(url, params, timeout=12, retries=3):
    """Alpaca Data API GET with retry on rate limits."""
    for attempt in range(retries):
        resp = requests.get(url, params=params, headers=_HEADERS, timeout=timeout)
        if resp.status_code == 429:
            time.sleep(2 * (attempt + 1))
            continue
        if resp.status_code >= 400:
            try:    msg = resp.json().get('message', resp.text)
            except: msg = resp.text
            raise ValueError(f'Alpaca API error ({resp.status_code}): {msg}')
        return resp.json()
    raise ValueError('Alpaca rate limit — too many requests. Wait a moment and try again.')


def alpaca_stock_bars(ticker, start_iso, end_iso, timeframe='1Min', feed='sip', adjustment='all'):
    """
    Fetch stock bars from Alpaca. Returns list of {t(ms), o, h, l, c, v}.
    start_iso / end_iso: 'YYYY-MM-DDTHH:MM:SSZ' or 'YYYY-MM-DD' strings.
    """
    url = f'https://data.alpaca.markets/v2/stocks/{ticker}/bars'
    params = {
        'timeframe':  timeframe,
        'start':      start_iso,
        'end':        end_iso,
        'limit':      10000,
        'feed':       feed,
        'adjustment': adjustment,
        'sort':       'asc',
    }
    out = []
    page_token = None
    for _ in range(10):  # hard cap on pagination
        if page_token:
            params['page_token'] = page_token
        data = _alpaca_get(url, params)
        for b in (data.get('bars') or []):
            out.append({'t': _iso_to_ms(b['t']), 'o': b['o'], 'h': b['h'],
                        'l': b['l'], 'c': b['c'], 'v': b['v']})
        page_token = data.get('next_page_token')
        if not page_token:
            break
    return out


def alpaca_stock_trades(ticker, start_iso, end_iso, feed='sip', limit=10000, max_pages=1000):
    """
    Fetch historical stock trades from Alpaca.

    Returns normalized rows shaped for ws_scalp replay:
      {t(ms), p, s, x, c, i, z}
    """
    url = f'https://data.alpaca.markets/v2/stocks/{ticker}/trades'
    params = {
        'start': start_iso,
        'end': end_iso,
        'limit': limit,
        'feed': feed,
        'sort': 'asc',
    }
    out = []
    page_token = None
    for _ in range(max_pages):
        if page_token:
            params['page_token'] = page_token
        else:
            params.pop('page_token', None)
        data = _alpaca_get(url, params, timeout=20)
        for t in (data.get('trades') or []):
            out.append({
                't': _iso_to_ms(t['t']),
                'p': t.get('p'),
                's': t.get('s'),
                'x': t.get('x'),
                'c': t.get('c') or [],
                'i': t.get('i'),
                'z': t.get('z'),
            })
        page_token = data.get('next_page_token')
        if not page_token:
            break
    return out


def alpaca_stock_quotes(ticker, start_iso, end_iso, feed='sip', limit=10000, max_pages=1000):
    """
    Fetch historical stock quotes from Alpaca.

    Returns normalized rows shaped for ws_scalp replay:
      {t(ms), bp, ap, bs, as, bx, ax, c, z}
    """
    url = f'https://data.alpaca.markets/v2/stocks/{ticker}/quotes'
    params = {
        'start': start_iso,
        'end': end_iso,
        'limit': limit,
        'feed': feed,
        'sort': 'asc',
    }
    out = []
    page_token = None
    for _ in range(max_pages):
        if page_token:
            params['page_token'] = page_token
        else:
            params.pop('page_token', None)
        data = _alpaca_get(url, params, timeout=20)
        for q in (data.get('quotes') or []):
            out.append({
                't': _iso_to_ms(q['t']),
                'bp': q.get('bp'),
                'ap': q.get('ap'),
                'bs': q.get('bs'),
                'as': q.get('as'),
                'bx': q.get('bx'),
                'ax': q.get('ax'),
                'c': q.get('c') or [],
                'z': q.get('z'),
            })
        page_token = data.get('next_page_token')
        if not page_token:
            break
    return out


def alpaca_crypto_bars(symbol, start_iso, end_iso, timeframe='5Min'):
    """
    Fetch crypto bars. `symbol` is a pair like 'BTC/USD'.
    Returns list of {t(ms), o, h, l, c, v}.
    """
    url = 'https://data.alpaca.markets/v1beta3/crypto/us/bars'
    params = {
        'symbols':   symbol,
        'timeframe': timeframe,
        'start':     start_iso,
        'end':       end_iso,
        'limit':     10000,
        'sort':      'asc',
    }
    out = []
    page_token = None
    # 100 pages * 10k = up to 1M bars (~1.9 years of 1-min crypto)
    for _ in range(100):
        if page_token:
            params['page_token'] = page_token
        data = _alpaca_get(url, params)
        bars_map = data.get('bars') or {}
        for b in (bars_map.get(symbol) or []):
            out.append({'t': _iso_to_ms(b['t']), 'o': b['o'], 'h': b['h'],
                        'l': b['l'], 'c': b['c'], 'v': b['v']})
        page_token = data.get('next_page_token')
        if not page_token:
            break
    return out


def alpaca_crypto_latest(symbol) -> float | None:
    """Fetch the most recent crypto bar close. Returns float or None."""
    url = 'https://data.alpaca.markets/v1beta3/crypto/us/latest/bars'
    try:
        data = _alpaca_get(url, {'symbols': symbol})
        b = (data.get('bars') or {}).get(symbol)
        return float(b['c']) if b else None
    except Exception:
        return None


def et_offset(month: int) -> int:
    """Return ET UTC offset (-4 EDT or -5 EST) for a given month number.

    Uses the 15th of the month as a representative date. Accurate for all
    months except the DST-transition weeks in mid-March and early November,
    where the result may be off by one hour. Prefer dt.astimezone(_ET) when
    you have a full datetime available.
    """
    probe = datetime(datetime.now().year, month, 15, 12, 0, 0, tzinfo=_ET)
    return int(probe.utcoffset().total_seconds() / 3600)
