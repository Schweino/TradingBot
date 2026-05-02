"""
Alpaca trading API wrapper — paper/sandbox by default.

Scope:
  - Account + market clock
  - Asset info (shortable? fractionable?)
  - Submit bracket orders (entry + take-profit + stop-loss as OCO)
  - Submit plain market/limit orders
  - Order lifecycle: get, list, cancel, cancel-all
  - Position lifecycle: list, get, close, close-all

Safety defaults:
  - `paper=True` is the default. Live requires explicit `paper=False` AND
    env var `ALPACA_ALLOW_LIVE=1`. Belt + suspenders.
  - All methods raise AlpacaTradingError on non-2xx.
  - Every order gets a client_order_id (uuid4) unless provided, so retries
    are idempotent.

This module DOES NOT auto-wire into mock_trader.py. See LIVE_TRADING_PLAN.md
for how to integrate.

Usage:
    from alpaca_trading import AlpacaTrader
    trader = AlpacaTrader.from_env(paper=True)
    print(trader.get_account())
    print(trader.is_market_open())

    # Submit bracket long
    order = trader.submit_bracket_order(
        symbol='MARA', qty=10, side='buy',
        tp_price=10.50, sl_price=9.50,
    )
"""
from __future__ import annotations

import os
import uuid
from typing import Optional

import requests
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))
except ImportError:
    pass

PAPER_BASE = 'https://paper-api.alpaca.markets'
LIVE_BASE  = 'https://api.alpaca.markets'


class AlpacaTradingError(RuntimeError):
    """Raised on any Alpaca API error (non-2xx)."""
    def __init__(self, status: int, body: str, url: str):
        super().__init__(f'Alpaca {status} on {url}: {body}')
        self.status = status
        self.body = body
        self.url = url


class AlpacaTrader:
    """Wrapper around Alpaca's Trading API v2.
    Hard-defaults to paper env. Live requires paper=False + env flag."""

    def __init__(self, api_key: str, secret_key: str, paper: bool = True):
        if not paper and os.getenv('ALPACA_ALLOW_LIVE') != '1':
            raise RuntimeError(
                'Refusing to construct live trader. Set env '
                'ALPACA_ALLOW_LIVE=1 and pass paper=False explicitly.'
            )
        if not api_key or not secret_key:
            raise RuntimeError('ALPACA_API_KEY / ALPACA_SECRET_KEY not set')
        self.paper = paper
        self.base = PAPER_BASE if paper else LIVE_BASE
        self.headers = {
            'APCA-API-KEY-ID':     api_key,
            'APCA-API-SECRET-KEY': secret_key,
            'Content-Type':        'application/json',
            'Accept':              'application/json',
        }

    # ── Factories ──────────────────────────────────────────────────────
    @classmethod
    def from_env(cls, paper: bool = True) -> 'AlpacaTrader':
        """Build from env. Prefers paper-specific keys if present:
            ALPACA_PAPER_KEY / ALPACA_PAPER_SECRET  (paper=True)
            ALPACA_LIVE_KEY  / ALPACA_LIVE_SECRET   (paper=False)
        Falls back to the shared ALPACA_API_KEY / ALPACA_SECRET_KEY.
        """
        if paper:
            k = os.getenv('ALPACA_PAPER_KEY')    or os.getenv('ALPACA_API_KEY')
            s = os.getenv('ALPACA_PAPER_SECRET') or os.getenv('ALPACA_SECRET_KEY')
        else:
            k = os.getenv('ALPACA_LIVE_KEY')    or os.getenv('ALPACA_API_KEY')
            s = os.getenv('ALPACA_LIVE_SECRET') or os.getenv('ALPACA_SECRET_KEY')
        return cls(api_key=k, secret_key=s, paper=paper)

    # ── Low-level HTTP ─────────────────────────────────────────────────
    def _request(self, method: str, path: str,
                 params: Optional[dict] = None,
                 json_body: Optional[dict] = None,
                 timeout: int = 15) -> dict:
        url = self.base + path
        resp = requests.request(method, url, headers=self.headers,
                                params=params, json=json_body, timeout=timeout)
        if not resp.ok:
            raise AlpacaTradingError(resp.status_code, resp.text, url)
        if resp.status_code == 204 or not resp.content:
            return {}
        try:
            return resp.json()
        except ValueError:
            return {'_raw': resp.text}

    # ── Account / clock / assets ───────────────────────────────────────
    def get_account(self) -> dict:
        """Returns equity, buying_power, cash, status, pattern_day_trader, ..."""
        return self._request('GET', '/v2/account')

    def get_clock(self) -> dict:
        """Returns {is_open, next_open, next_close, timestamp}."""
        return self._request('GET', '/v2/clock')

    def is_market_open(self) -> bool:
        return bool(self.get_clock().get('is_open'))

    def get_asset(self, symbol: str) -> dict:
        """Returns tradable, shortable, fractionable, easy_to_borrow, ..."""
        return self._request('GET', f'/v2/assets/{symbol.upper()}')

    # ── Orders ─────────────────────────────────────────────────────────
    def submit_market_order(self, symbol: str, qty: float, side: str,
                            time_in_force: str = 'day',
                            client_order_id: Optional[str] = None) -> dict:
        """Plain market order. `side` is 'buy' or 'sell'.
        Supports fractional qty for market orders."""
        body = {
            'symbol':          symbol.upper(),
            'qty':             str(qty),
            'side':            side,
            'type':            'market',
            'time_in_force':   time_in_force,
            'client_order_id': client_order_id or _mkid('mkt'),
        }
        return self._request('POST', '/v2/orders', json_body=body)

    def submit_bracket_order(self, symbol: str, qty: float, side: str,
                             tp_price: float, sl_price: float,
                             time_in_force: str = 'day',
                             sl_limit_price: Optional[float] = None,
                             client_order_id: Optional[str] = None) -> dict:
        """Submit a bracket order = market entry + OCO take-profit + stop-loss.

        - `side`:     'buy' (opens long) or 'sell' (opens short)
        - `qty`:      whole-share integer (brackets don't support fractional)
        - `tp_price`: take-profit limit price
        - `sl_price`: stop-loss stop price (triggers a market exit)
        - `sl_limit_price`: optional — if set, stop-loss is a stop-limit
          (won't fill below this price). Omit for stop-market (recommended
          for noisy names; otherwise you can get filled at any price).

        Alpaca rejects brackets with fractional qty — round up/down at caller.
        """
        if qty != int(qty):
            raise ValueError(f'bracket orders require integer qty (got {qty})')
        qty = int(qty)

        stop_loss = {'stop_price': _fmt(sl_price)}
        if sl_limit_price is not None:
            stop_loss['limit_price'] = _fmt(sl_limit_price)

        body = {
            'symbol':          symbol.upper(),
            'qty':             str(qty),
            'side':            side,
            'type':            'market',
            'time_in_force':   time_in_force,
            'order_class':     'bracket',
            'take_profit':     {'limit_price': _fmt(tp_price)},
            'stop_loss':       stop_loss,
            'client_order_id': client_order_id or _mkid('brk'),
        }
        return self._request('POST', '/v2/orders', json_body=body)

    def get_order(self, order_id: str,
                  nested: bool = True) -> dict:
        """Get order by id. `nested=True` returns child legs of brackets."""
        return self._request('GET', f'/v2/orders/{order_id}',
                             params={'nested': str(nested).lower()})

    def list_orders(self, status: str = 'open',
                    limit: int = 100,
                    symbols: Optional[list] = None,
                    nested: bool = True,
                    after: Optional[str] = None,
                    until: Optional[str] = None,
                    direction: str = 'desc') -> list:
        """status: 'open' (default) | 'closed' | 'all'."""
        params = {'status': status, 'limit': limit,
                  'nested': str(nested).lower(),
                  'direction': direction}
        if symbols:
            params['symbols'] = ','.join(s.upper() for s in symbols)
        if after:
            params['after'] = after
        if until:
            params['until'] = until
        out = self._request('GET', '/v2/orders', params=params)
        return out if isinstance(out, list) else []

    def cancel_order(self, order_id: str) -> None:
        """Cancel a single open order. Raises if already filled."""
        self._request('DELETE', f'/v2/orders/{order_id}')

    def cancel_all_orders(self) -> list:
        """Cancel every open order. Returns list of {id, status}."""
        out = self._request('DELETE', '/v2/orders')
        return out if isinstance(out, list) else []

    # ── Positions ──────────────────────────────────────────────────────
    def list_positions(self) -> list:
        out = self._request('GET', '/v2/positions')
        return out if isinstance(out, list) else []

    def get_position(self, symbol: str) -> Optional[dict]:
        """Returns the position dict or None if flat."""
        try:
            return self._request('GET', f'/v2/positions/{symbol.upper()}')
        except AlpacaTradingError as e:
            if e.status == 404:
                return None
            raise

    def close_position(self, symbol: str,
                       qty: Optional[float] = None,
                       percentage: Optional[float] = None) -> dict:
        """Market-close a position. Pass qty OR percentage OR neither (full)."""
        params = {}
        if qty is not None:        params['qty'] = str(qty)
        if percentage is not None: params['percentage'] = str(percentage)
        return self._request('DELETE', f'/v2/positions/{symbol.upper()}',
                             params=params or None)

    def close_all_positions(self, cancel_orders: bool = True) -> list:
        """Market-close every open position. Cancel open orders first by default."""
        if cancel_orders:
            try: self.cancel_all_orders()
            except AlpacaTradingError: pass
        out = self._request('DELETE', '/v2/positions',
                            params={'cancel_orders': 'true' if cancel_orders else 'false'})
        return out if isinstance(out, list) else []


# ── Helpers ────────────────────────────────────────────────────────────
def _mkid(prefix: str) -> str:
    """Generate a unique client_order_id (<= 48 chars)."""
    return f'{prefix}-{uuid.uuid4().hex[:12]}'


def _fmt(p: float) -> str:
    """Alpaca price formatting. Sub-dollar: up to 4 decimals;
    >=$1: max 2 decimals (tick rule)."""
    return f'{p:.4f}' if p < 1.0 else f'{p:.2f}'


# ── Convenience: position-sizing for bracket from $ alloc ──────────────
def alloc_to_qty(alloc_dollars: float, entry_price: float,
                 round_down: bool = True) -> int:
    """Convert a dollar allocation to a whole-share qty (bracket-friendly).
    Returns 0 if the allocation can't afford even one share."""
    if entry_price <= 0:
        return 0
    q = alloc_dollars / entry_price
    return int(q) if round_down else round(q)


if __name__ == '__main__':
    # Minimal manual sanity check
    t = AlpacaTrader.from_env(paper=True)
    print('account:', t.get_account())
    print('clock:',   t.get_clock())
