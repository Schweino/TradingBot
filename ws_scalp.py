"""
Alpaca WebSocket scalp engine.

Two persistent WS connections:
  - Stocks SIP  : wss://stream.data.alpaca.markets/v2/sip
  - Crypto (BTC): wss://stream.data.alpaca.markets/v1beta3/crypto/us

Each connection runs in its own thread. On every trade + quote we update a
thread-safe per-symbol state and append trades into a ring buffer. A separate
aggregator thread rolls trades into 1-second OHLCV+flow bars.

Scalp indicators + signal engine consume the 1s bar stream and expose current
state via get_snapshot(). Flask polls this snapshot for the UI.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Callable, Deque, Dict, List, Optional

import websocket  # websocket-client 1.x

try:
    from zoneinfo import ZoneInfo
except ImportError:  # py<3.9 fallback
    from backports.zoneinfo import ZoneInfo  # type: ignore

# ────────────────────────────────────────────────────────────────────────
# Config
# ────────────────────────────────────────────────────────────────────────
STOCKS_WS_URL = 'wss://stream.data.alpaca.markets/v2/sip'
CRYPTO_WS_URL = 'wss://stream.data.alpaca.markets/v1beta3/crypto/us'
BTC_SYMBOL = 'BTC/USD'

RING_SECONDS = 4 * 60 * 60   # keep 4h of 1s bars
TRADE_RING   = 50_000        # per-symbol recent trades cap
FLOW_MINUTES = 8 * 60        # per-symbol per-minute flow buckets (8h cap)

# SL/TP targets displayed on signal alerts. Must stay in sync with
# TICKER_CFG in mock_trader.py — these are informational only (the
# live broker brackets use mock_trader's values). Tickers not listed
# here fall back to SCALP_DEFAULT_SL/TP.
SCALP_DEFAULT_SL = 0.05
SCALP_DEFAULT_TP = 0.010
SCALP_TICKER_BRACKETS = {
    # 2026-04-25: CLSK/MARA restored to 5% (from 4%) — matches mock_trader.TICKER_CFG.
    # Keep in sync with mock_trader.TICKER_CFG (informational; actual brackets use those values).
    'CLSK': {'sl': 0.05, 'tp': 0.010},
    'MARA': {'sl': 0.05, 'tp': 0.010},
    'RIOT': {'sl': 0.05, 'tp': 0.020},
}

log = logging.getLogger('scalp')
log.setLevel(logging.INFO)
if not log.handlers:
    h = RotatingFileHandler(os.path.join(os.path.dirname(__file__), 'scalp.log'),
                            maxBytes=2_000_000, backupCount=5, encoding='utf-8')
    h.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    log.addHandler(h)

ET = ZoneInfo('America/New_York')


def _load_trading_config() -> dict:
    try:
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               'trading_config.json'), 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


TRADING_CONFIG = _load_trading_config()
BTC_BETA = TRADING_CONFIG.get('btc_beta', {'CLSK': 1.066, 'MARA': 0.967, 'RIOT': 1.20})
SMART_ENTRY = TRADING_CONFIG.get('smart_entry', {})
MIN_ACTIVITY_Z = float(SMART_ENTRY.get('min_activity_z', 1.0))
VWAP_CHOP_SIGMA = float(SMART_ENTRY.get('vwap_chop_sigma', 0.30))
REL_STRENGTH_MIN = float(SMART_ENTRY.get('relative_strength_min', 0.10))
BTC_CONFLICT_REJECT = bool(SMART_ENTRY.get('btc_conflict_reject', True))
RANGE_EXTENSION_REJECT_PCT = float(SMART_ENTRY.get('range_extension_reject_pct', 0.92))
BTC_MAX_STALE_SEC = float(SMART_ENTRY.get('btc_max_stale_sec', 20))
BTC_MAX_QUOTE_STALE_SEC = float(SMART_ENTRY.get('btc_quote_max_stale_sec', 10))
STOCK_MAX_STALE_SEC = float(SMART_ENTRY.get('stock_max_stale_sec', 10))
FLOW_FADE_MIN_DELTA_PCT = float(SMART_ENTRY.get('flow_fade_min_delta_pct', 4.0))
FLOW_FADE_REQUIRES_EXHAUSTION = bool(SMART_ENTRY.get('flow_fade_requires_exhaustion', True))
STATE_MACHINE_ENABLED = bool(SMART_ENTRY.get('state_machine_enabled', True))
STATE_MACHINE_MIN_CONFIRM_SEC = float(SMART_ENTRY.get('state_machine_min_confirm_sec', 2))
STATE_MACHINE_MAX_CONFIRM_SEC = float(SMART_ENTRY.get('state_machine_max_confirm_sec', 30))
EXECUTION_QUALITY_MIN = float(SMART_ENTRY.get('execution_quality_min', 55))
LIVE_QUALITY_GATE_ENABLED = bool(SMART_ENTRY.get('live_quality_gate_enabled', True))
LIVE_QUALITY_MIN_SCORE = float(SMART_ENTRY.get('live_quality_min_score', 60))
SIDE_SCORE_GAP_MIN = float(SMART_ENTRY.get('side_score_gap_min', 1.0))
BTC_LEAD_LAG_MIN_PCT = float(SMART_ENTRY.get('btc_lead_lag_min_pct', 0.08))
BTC_CHASE_MAX_PCT = float(SMART_ENTRY.get('btc_chase_max_pct', 0.45))
MINER_BASKET_ENABLED = bool(SMART_ENTRY.get('miner_basket_enabled', True))
MINER_BASKET_MIN_CONFIRMING = int(SMART_ENTRY.get('miner_basket_min_confirming', 1))
MINER_BASKET_BONUS = int(SMART_ENTRY.get('miner_basket_bonus', 1))
MINER_BASKET_PENALTY = int(SMART_ENTRY.get('miner_basket_penalty', 1))
BTC_CHOP_PENALTY_ENABLED = bool(SMART_ENTRY.get('btc_chop_penalty_enabled', True))
REALIZED_VOL_CHOP_PENALTY_ENABLED = bool(SMART_ENTRY.get('realized_vol_chop_penalty_enabled', True))
MIN_REALIZED_VOL_60S_PCT = float(SMART_ENTRY.get('min_realized_vol_60s_pct', 0.06))
SETUP_COOLDOWN_SEC = SMART_ENTRY.get('setup_cooldown_sec', {})
SESSION_MIN_SCORE_ADJUST = SMART_ENTRY.get('session_min_score_adjust', {})
MIN_SCORE_BY_SETUP = SMART_ENTRY.get('min_score_by_setup', {
    'momentum_breakout': 4,
    'btc_relative_strength': 4,
    'vwap_reclaim_breakdown': 5,
    'trend_pullback': 5,
    'flow_exhaustion_fade': 5,
})
NEAR_SIGNAL_MIN_SCORE = float(SMART_ENTRY.get('near_signal_min_score', 2))
NEAR_SIGNAL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               'postmortem', 'near_signals')
_near_signal_lock = threading.Lock()
_last_near_signal_log: Dict[str, float] = {}


# ────────────────────────────────────────────────────────────────────────
# Per-symbol live state
# ────────────────────────────────────────────────────────────────────────
@dataclass
class SymbolState:
    symbol: str
    last_trade_price: Optional[float] = None
    last_trade_size: int = 0
    last_trade_ts_ms: int = 0
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    bid_size: Optional[int] = None
    ask_size: Optional[int] = None
    last_quote_ts_ms: int = 0
    quote_history: Deque = field(default_factory=lambda: deque(maxlen=300))
    # Trade ring: each entry is (ts_ms, price, size, side) where side ∈ {+1, -1, 0}
    trades: Deque = field(default_factory=lambda: deque(maxlen=TRADE_RING))
    pending_trades: Deque = field(default_factory=deque)
    # 1s bar ring: each entry is a dict with ts_s, o, h, l, c, v, buy_v, sell_v, n
    bars_1s: Deque = field(default_factory=lambda: deque(maxlen=RING_SECONDS))
    # Per-minute order-flow history: each entry {ts, buy_pct, buy_v, sell_v}.
    # Emitted at minute boundaries by the aggregator. Used for the intraday
    # order-flow line chart so the UI can show buy-pressure over the full day.
    flow_history: Deque = field(default_factory=lambda: deque(maxlen=FLOW_MINUTES))
    # Session VWAP state
    session_pv_sum: float = 0.0   # sum of price*volume since session start
    session_v_sum: float = 0.0
    session_date: Optional[str] = None
    lock: threading.Lock = field(default_factory=threading.Lock)

    def classify_side(self, price: float) -> int:
        """Return +1 if buy-aggressor, -1 if sell-aggressor, 0 if indeterminate."""
        if self.best_ask is not None and price >= self.best_ask:
            return +1
        if self.best_bid is not None and price <= self.best_bid:
            return -1
        # Fallback: tick rule against last trade price
        if self.last_trade_price is not None:
            if price > self.last_trade_price: return +1
            if price < self.last_trade_price: return -1
        return 0


# ────────────────────────────────────────────────────────────────────────
# Engine
# ────────────────────────────────────────────────────────────────────────
class ScalpEngine:
    def __init__(self, api_key: str, secret_key: str):
        self.api_key = api_key
        self.secret_key = secret_key
        self.states: Dict[str, SymbolState] = {}
        self.active_ticker: Optional[str] = None
        self.btc_enabled: bool = False
        self._state_lock = threading.Lock()
        # Extra tickers (e.g. mock-trader tickers) that should stream + signal
        # regardless of the active display ticker.
        self._extra_tickers: set = set()

        # WS connections
        self._stocks_ws: Optional[websocket.WebSocketApp] = None
        self._crypto_ws: Optional[websocket.WebSocketApp] = None
        self._stocks_thread: Optional[threading.Thread] = None
        self._crypto_thread: Optional[threading.Thread] = None
        self._stocks_subscribed: List[str] = []
        self._crypto_subscribed: List[str] = []
        self._shutdown = threading.Event()

        # Aggregator
        self._agg_thread = threading.Thread(
            target=self._aggregator_loop, daemon=True, name='scalp-agg')
        self._agg_thread.start()

        # Signal engine
        self.signals: Deque = deque(maxlen=200)  # recent signal log
        self._last_signal_ts: Dict[str, float] = {}  # for cooldown
        self._setup_states: Dict[str, dict] = {}
        self._on_signal_cbs: List[Callable] = []
        self._cb_lock = threading.Lock()

        # Tick capture: ring-buffer flush on signal + post-entry tail
        self._tick_captures: Dict[str, dict] = {}
        self._captures_lock = threading.Lock()

    # --------------- public API ---------------
    def on_signal(self, cb: Callable):
        cb_self = getattr(cb, '__self__', None)
        cb_func = getattr(cb, '__func__', None)
        with self._cb_lock:
            for existing in self._on_signal_cbs:
                if existing is cb:
                    return
                if cb_func is not None \
                        and getattr(existing, '__self__', None) is cb_self \
                        and getattr(existing, '__func__', None) is cb_func:
                    return
            self._on_signal_cbs.append(cb)

    def subscribe(self, ticker: str, btc: bool = False):
        """Set the active ticker, optionally enabling BTC side-stream."""
        ticker = ticker.upper().strip()
        with self._state_lock:
            prev = self.active_ticker
            self.active_ticker = ticker
            self.btc_enabled = btc
            if ticker not in self.states:
                self.states[ticker] = SymbolState(symbol=ticker)
            if btc and BTC_SYMBOL not in self.states:
                self.states[BTC_SYMBOL] = SymbolState(symbol=BTC_SYMBOL)
        log.info(f'subscribe ticker={ticker} btc={btc} (prev={prev})')

        # Stocks WS: (re)subscribe if ticker changed
        self._ensure_stocks_ws()
        self._stocks_send_sub([ticker], prev=[prev] if prev and prev != ticker else [])

        # Crypto WS: ensure connected + subscribed if btc flag on
        if btc:
            self._ensure_crypto_ws()
            self._crypto_send_sub([BTC_SYMBOL])

    def add_ticker(self, ticker: str):
        """Add an additional ticker to the stocks WS stream without changing
        the active display ticker. Safe to call multiple times."""
        ticker = ticker.upper().strip()
        with self._state_lock:
            if ticker not in self.states:
                self.states[ticker] = SymbolState(symbol=ticker)
            self._extra_tickers.add(ticker)
        log.info(f'add_ticker {ticker}')
        self._ensure_stocks_ws()
        # Subscribe just this symbol (additive)
        self._stocks_send_sub([ticker])

    def remove_ticker(self, ticker: str):
        ticker = ticker.upper().strip()
        with self._state_lock:
            self._extra_tickers.discard(ticker)
            still_active = (ticker == self.active_ticker)
        if not still_active:
            self._stocks_send_sub([], prev=[ticker])
        log.info(f'remove_ticker {ticker}')

    def clear_signal_cooldown(self, ticker: str):
        """Reset signal cooldown for a ticker so the very next qualifying
        signal fires regardless of the 30s throttle. Call this right after
        closing a scalp position to enable immediate re-entry."""
        ticker = ticker.upper().strip()
        for key in list(self._last_signal_ts):
            if key.startswith(f'{ticker}:'):
                self._last_signal_ts.pop(key, None)
        for key in list(self._setup_states):
            if key.startswith(f'{ticker}:'):
                self._setup_states.pop(key, None)

    def get_last_price(self, ticker: str) -> Optional[float]:
        ticker = ticker.upper().strip()
        st = self.states.get(ticker)
        return st.last_trade_price if st else None

    def get_all_subscribed(self) -> List[str]:
        """All stock tickers currently streamed (active + extras)."""
        out = set(self._extra_tickers)
        if self.active_ticker:
            out.add(self.active_ticker)
        return sorted(out)

    def unsubscribe_all(self):
        with self._state_lock:
            prev = self.active_ticker
            self.active_ticker = None
        if prev:
            self._stocks_send_sub([], prev=[prev])
        log.info('unsubscribe_all')

    def get_snapshot(self) -> dict:
        with self._state_lock:
            tkr = self.active_ticker
            btc_on = self.btc_enabled
        if not tkr:
            return {'active': False}
        st = self.states.get(tkr)
        if not st:
            return {'active': False}
        # Pull latest 1s bar + compute indicators
        ind = compute_indicators(st)
        btc_ind = None
        if btc_on and BTC_SYMBOL in self.states:
            btc_ind = compute_indicators(self.states[BTC_SYMBOL])
        # Compact flow history for the intraday chart: list of [ts_s, buy_pct].
        flow_hist = [[e['ts'], e['buy_pct']] for e in st.flow_history]
        return {
            'active': True,
            'ticker': tkr,
            'btc_enabled': btc_on,
            'last_price': st.last_trade_price,
            'last_ts_ms': st.last_trade_ts_ms,
            'best_bid':   st.best_bid,
            'best_ask':   st.best_ask,
            'bid_size':   st.bid_size,
            'ask_size':   st.ask_size,
            'last_quote_ts_ms': st.last_quote_ts_ms,
            'bar_count':  len(st.bars_1s),
            'indicators': ind,
            'btc':        btc_ind,
            'flow_history': flow_hist,
            'signals':    list(self.signals)[-20:],
            'stocks_ws_connected': bool(self._stocks_ws and self._stocks_ws.sock
                                         and self._stocks_ws.sock.connected),
            'crypto_ws_connected': bool(self._crypto_ws and self._crypto_ws.sock
                                         and self._crypto_ws.sock.connected),
        }

    def shutdown(self):
        self._shutdown.set()
        for ws in (self._stocks_ws, self._crypto_ws):
            try:
                if ws: ws.close()
            except Exception:
                pass

    # --------------- stocks WS ---------------
    def _ensure_stocks_ws(self):
        if self._stocks_thread and self._stocks_thread.is_alive():
            return
        self._stocks_thread = threading.Thread(
            target=self._run_ws, args=(STOCKS_WS_URL, 'stocks'),
            daemon=True, name='scalp-stocks-ws')
        self._stocks_thread.start()

    def _ensure_crypto_ws(self):
        if self._crypto_thread and self._crypto_thread.is_alive():
            return
        self._crypto_thread = threading.Thread(
            target=self._run_ws, args=(CRYPTO_WS_URL, 'crypto'),
            daemon=True, name='scalp-crypto-ws')
        self._crypto_thread.start()

    def _run_ws(self, url: str, label: str):
        backoff = 1
        while not self._shutdown.is_set():
            try:
                ws = websocket.WebSocketApp(
                    url,
                    on_open=lambda w, l=label: self._on_open(w, l),
                    on_message=lambda w, m, l=label: self._on_message(w, m, l),
                    on_error=lambda w, e, l=label: log.error(f'{l} ws error: {e}'),
                    on_close=lambda w, c, r, l=label: log.info(f'{l} ws closed: {c} {r}'),
                )
                if label == 'stocks':
                    self._stocks_ws = ws
                else:
                    self._crypto_ws = ws
                ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                log.exception(f'{label} run_forever crashed: {e}')
            if self._shutdown.is_set():
                break
            wait = min(backoff, 30)
            log.info(f'{label} reconnecting in {wait}s')
            time.sleep(wait)
            backoff = min(backoff * 2, 30)

    def _on_open(self, ws, label):
        log.info(f'{label} ws opened, authenticating')
        ws.send(json.dumps({
            'action': 'auth',
            'key':    self.api_key,
            'secret': self.secret_key,
        }))
        # Re-subscribe known symbols on reconnect
        if label == 'stocks':
            all_syms = self.get_all_subscribed()
            if all_syms:
                self._stocks_send_sub(all_syms)
        elif label == 'crypto' and self.btc_enabled:
            self._crypto_send_sub([BTC_SYMBOL])

    def _stocks_send_sub(self, add: List[str], prev: List[str] = None):
        if not self._stocks_ws or not getattr(self._stocks_ws, 'sock', None) \
                or not self._stocks_ws.sock.connected:
            return  # will happen in _on_open on reconnect
        if prev:
            try:
                self._stocks_ws.send(json.dumps({
                    'action': 'unsubscribe', 'trades': prev, 'quotes': prev,
                }))
            except Exception as e:
                log.error(f'unsubscribe failed: {e}')
        if add:
            try:
                self._stocks_ws.send(json.dumps({
                    'action': 'subscribe', 'trades': add, 'quotes': add,
                }))
                self._stocks_subscribed = add
            except Exception as e:
                log.error(f'subscribe failed: {e}')

    def _crypto_send_sub(self, add: List[str]):
        if not self._crypto_ws or not getattr(self._crypto_ws, 'sock', None) \
                or not self._crypto_ws.sock.connected:
            return
        try:
            self._crypto_ws.send(json.dumps({
                'action': 'subscribe', 'trades': add, 'quotes': add,
            }))
            self._crypto_subscribed = add
        except Exception as e:
            log.error(f'crypto subscribe failed: {e}')

    def _on_message(self, ws, message, label):
        try:
            msgs = json.loads(message)
        except Exception:
            return
        if isinstance(msgs, dict):
            msgs = [msgs]
        for m in msgs:
            t = m.get('T')
            if t == 't':           # trade
                self._handle_trade(m)
            elif t == 'q':         # quote
                self._handle_quote(m)
            elif t == 'success':
                log.info(f'{label} success: {m.get("msg")}')
            elif t == 'error':
                log.error(f'{label} error: {m}')
            elif t == 'subscription':
                log.info(f'{label} subs confirmed: {m}')

    # --------------- tick capture ---------------
    def start_tick_capture(self, ticker: str, label: str, signal: Optional[dict] = None,
                           capture_id: Optional[str] = None,
                           pre_seconds: int = 120, post_seconds: int = 360):
        """Snapshot the last pre_seconds of ticks (pre-signal ring buffer) and
        stream the next post_seconds of ticks (post-entry) to a JSONL file.
        Called by mock_trader when a position is opened."""
        st = self.states.get(ticker)
        if not st:
            log.warning(f'tick_capture: no state for {ticker}')
            return
        now_ms = int(time.time() * 1000)
        pre_cutoff_ms = now_ms - pre_seconds * 1000
        end_ms = now_ms + post_seconds * 1000

        with st.lock:
            pre_ticks = [(ts, p, sz, sd) for ts, p, sz, sd in st.trades
                         if ts >= pre_cutoff_ms]
            pre_quotes = [q for q in st.quote_history if q[0] >= pre_cutoff_ms]

        btc_pre_ticks = []
        btc_pre_quotes = []
        btc_state = self.states.get(BTC_SYMBOL)
        if btc_state:
            with btc_state.lock:
                btc_pre_ticks = [(ts, p, sz, sd) for ts, p, sz, sd in btc_state.trades
                                 if ts >= pre_cutoff_ms]
                btc_pre_quotes = [q for q in btc_state.quote_history if q[0] >= pre_cutoff_ms]

        date_str = datetime.now(ET).strftime('%Y-%m-%d')
        ts_str   = datetime.now(ET).strftime('%H%M%S')
        out_dir  = os.path.join(os.path.dirname(__file__), 'tick_logs', date_str)
        os.makedirs(out_dir, exist_ok=True)
        capture_key = capture_id or f'{ticker}-{ts_str}-{int(now_ms)}'
        safe_key = ''.join(ch if ch.isalnum() or ch in ('-', '_') else '_' for ch in capture_key)
        fpath = os.path.join(out_dir, f'{ticker}_{ts_str}_{label}_{safe_key[-24:]}.jsonl')

        with self._captures_lock:
            self._tick_captures[capture_key] = {
                'fpath':      fpath,
                'end_ms':     end_ms,
                'label':      label,
                'ticker':     ticker,
                'capture_id': capture_key,
                'symbols':    {ticker, BTC_SYMBOL},
                'started_ms': now_ms,
                'signal':     signal or {},
                'pre_ticks':  pre_ticks,
                'pre_quotes': pre_quotes,
                'btc_pre_ticks': btc_pre_ticks,
                'btc_pre_quotes': btc_pre_quotes,
                'post_ticks': [],
                'post_quotes': [],
                'btc_post_ticks': [],
                'btc_post_quotes': [],
                'post_events': [],
            }
        log.info(f'tick_capture start {ticker} label={label} '
                 f'pre={len(pre_ticks)} ticks post={post_seconds}s -> {fpath}')

    def _flush_capture(self, cap: dict):
        try:
            with open(cap['fpath'], 'w', encoding='utf-8') as f:
                f.write(json.dumps({
                    'type': 'header', 'label': cap['label'],
                    'ticker': cap.get('ticker'),
                    'capture_id': cap.get('capture_id'),
                    'started_ms': cap.get('started_ms'),
                    'signal': cap.get('signal', {}),
                    'pre_count': len(cap['pre_ticks']),
                    'post_count': len(cap['post_ticks']),
                    'pre_quote_count': len(cap.get('pre_quotes', [])),
                    'post_quote_count': len(cap.get('post_quotes', [])),
                    'btc_pre_count': len(cap.get('btc_pre_ticks', [])),
                    'btc_post_count': len(cap.get('btc_post_ticks', [])),
                }) + '\n')
                for ts, price, size, side in cap['pre_ticks']:
                    f.write(json.dumps({
                        'phase': 'pre', 'ts_ms': ts,
                        'symbol': cap.get('ticker'),
                        'event': 'trade',
                        'price': price, 'size': size, 'side': side,
                    }) + '\n')
                for ts, bid, ask, bid_size, ask_size, imbalance in cap.get('pre_quotes', []):
                    f.write(json.dumps({
                        'phase': 'pre', 'ts_ms': ts,
                        'symbol': cap.get('ticker'),
                        'event': 'quote',
                        'bid': bid, 'ask': ask,
                        'bid_size': bid_size, 'ask_size': ask_size,
                        'imbalance': imbalance,
                    }) + '\n')
                for ts, price, size, side in cap.get('btc_pre_ticks', []):
                    f.write(json.dumps({
                        'phase': 'pre', 'ts_ms': ts,
                        'symbol': BTC_SYMBOL,
                        'event': 'trade',
                        'price': price, 'size': size, 'side': side,
                    }) + '\n')
                for ts, bid, ask, bid_size, ask_size, imbalance in cap.get('btc_pre_quotes', []):
                    f.write(json.dumps({
                        'phase': 'pre', 'ts_ms': ts,
                        'symbol': BTC_SYMBOL,
                        'event': 'quote',
                        'bid': bid, 'ask': ask,
                        'bid_size': bid_size, 'ask_size': ask_size,
                        'imbalance': imbalance,
                    }) + '\n')
                for ts, price, size, side in cap['post_ticks']:
                    f.write(json.dumps({
                        'phase': 'post', 'ts_ms': ts,
                        'symbol': cap.get('ticker'),
                        'event': 'trade',
                        'price': price, 'size': size, 'side': side,
                    }) + '\n')
                for event in cap.get('post_events', []):
                    f.write(json.dumps(event) + '\n')
            log.info(f'tick_capture flushed {cap["fpath"]} '
                     f'({len(cap["pre_ticks"])} pre + {len(cap["post_ticks"])} post ticks)')
        except Exception as e:
            log.error(f'tick_capture flush error: {e}')

    def _append_capture_event(self, sym: str, event: dict):
        if not self._tick_captures:
            return
        with self._captures_lock:
            captures = list(self._tick_captures.values())
        for cap in captures:
            if sym not in cap.get('symbols', set()):
                continue
            if event.get('ts_ms') and event['ts_ms'] > cap.get('end_ms', 0):
                continue
            row = dict(event)
            row.setdefault('phase', 'post')
            row.setdefault('symbol', sym)
            cap.setdefault('post_events', []).append(row)
            if sym == cap.get('ticker') and row.get('event') == 'quote':
                cap.setdefault('post_quotes', []).append(
                    (row.get('ts_ms'), row.get('bid'), row.get('ask'),
                     row.get('bid_size'), row.get('ask_size'), row.get('imbalance'))
                )
            elif sym == BTC_SYMBOL and row.get('event') == 'trade':
                cap.setdefault('btc_post_ticks', []).append(
                    (row.get('ts_ms'), row.get('price'), row.get('size'), row.get('side'))
                )
            elif sym == BTC_SYMBOL and row.get('event') == 'quote':
                cap.setdefault('btc_post_quotes', []).append(
                    (row.get('ts_ms'), row.get('bid'), row.get('ask'),
                     row.get('bid_size'), row.get('ask_size'), row.get('imbalance'))
                )

    def _handle_trade(self, m):
        sym = m.get('S')
        if not sym or sym not in self.states:
            return
        st = self.states[sym]
        price = float(m.get('p', 0))
        size = int(m.get('s', 0))
        ts_ms = _alpaca_ts_to_ms(m.get('t', ''))
        # Skip invalid/odd-lot or trades with problematic conditions
        conds = m.get('c') or []
        # Alpaca condition codes to skip: 'B' (out of sequence), 'W' (corrected), 'Z' (sold)
        if any(c in ('B', 'W', 'Z') for c in conds):
            return
        with st.lock:
            side = st.classify_side(price)
            st.trades.append((ts_ms, price, size, side))
            st.pending_trades.append((ts_ms, price, size, side))
            st.last_trade_price = price
            st.last_trade_size = size
            st.last_trade_ts_ms = ts_ms
            # Session VWAP
            dt = datetime.fromtimestamp(ts_ms / 1000, tz=ET)
            # Reset VWAP at 9:30am ET each trading day
            et_date = dt.date().isoformat()
            if st.session_date != et_date:
                st.session_date = et_date
                st.session_pv_sum = 0.0
                st.session_v_sum = 0.0
            st.session_pv_sum += price * size
            st.session_v_sum += size
        # Post-entry tick capture (outside st.lock — fast append, GIL-safe)
        with self._captures_lock:
            captures = [
                cap for cap in self._tick_captures.values()
                if cap.get('ticker') == sym and ts_ms <= cap.get('end_ms', 0)
            ]
        for cap in captures:
            cap['post_ticks'].append((ts_ms, price, size, side))
        self._append_capture_event(sym, {
            'event': 'trade', 'ts_ms': ts_ms,
            'price': price, 'size': size, 'side': side,
        })

    def _handle_quote(self, m):
        sym = m.get('S')
        if not sym or sym not in self.states:
            return
        st = self.states[sym]
        with st.lock:
            bp = m.get('bp')
            ap = m.get('ap')
            bs = m.get('bs')
            a_s = m.get('as')
            if bp is not None: st.best_bid = float(bp)
            if ap is not None: st.best_ask = float(ap)
            if bs is not None: st.bid_size = int(bs)
            if a_s is not None: st.ask_size = int(a_s)
            quote_ts = _alpaca_ts_to_ms(m.get('t', '')) if m.get('t') else int(time.time() * 1000)
            st.last_quote_ts_ms = quote_ts
            bid_sz = st.bid_size or 0
            ask_sz = st.ask_size or 0
            tot_sz = bid_sz + ask_sz
            imb = round((bid_sz - ask_sz) / tot_sz, 3) if tot_sz > 0 else None
            st.quote_history.append((quote_ts, st.best_bid, st.best_ask, bid_sz, ask_sz, imb))
            if sym == BTC_SYMBOL and st.best_bid is not None and st.best_ask is not None:
                mid = round((st.best_bid + st.best_ask) / 2, 6)
                side = 0
                if st.last_trade_price is not None:
                    if mid > st.last_trade_price:
                        side = +1
                    elif mid < st.last_trade_price:
                        side = -1
                st.trades.append((quote_ts, mid, 0, side))
                st.pending_trades.append((quote_ts, mid, 0, side))
                st.last_trade_price = mid
                st.last_trade_size = 0
                st.last_trade_ts_ms = quote_ts
            event = {
                'event': 'quote', 'ts_ms': quote_ts,
                'bid': st.best_bid, 'ask': st.best_ask,
                'bid_size': bid_sz, 'ask_size': ask_sz,
                'imbalance': imb,
            }
        self._append_capture_event(sym, event)

    # --------------- aggregator ---------------
    def _aggregator_loop(self):
        """Every second, roll trades in the past 1s window into a 1s bar per symbol."""
        while not self._shutdown.is_set():
            time.sleep(1.0)
            now_ms = int(time.time() * 1000)
            sec_start_ms = (now_ms // 1000 - 1) * 1000  # bar for the previous second
            sec_end_ms = sec_start_ms + 1000
            for sym, st in list(self.states.items()):
                with st.lock:
                    o = h = l = c = None
                    v = buy_v = sell_v = n = 0
                    while st.pending_trades and st.pending_trades[0][0] < sec_end_ms:
                        ts_ms, price, size, side = st.pending_trades.popleft()
                        if ts_ms < sec_start_ms:
                            continue
                        if o is None:
                            o = h = l = price
                        h = max(h, price)
                        l = min(l, price)
                        c = price
                        v += size
                        n += 1
                        if side > 0: buy_v += size
                        elif side < 0: sell_v += size
                    if o is None:
                        # No trades this second — carry-forward close from last bar
                        if st.bars_1s:
                            prev = st.bars_1s[-1]
                            o = h = l = c = prev['c']
                        else:
                            continue
                    st.bars_1s.append({
                        'ts_s': sec_start_ms // 1000,
                        'o': o, 'h': h, 'l': l, 'c': c,
                        'v': v, 'buy_v': buy_v, 'sell_v': sell_v, 'n': n,
                    })
                    # Emit a flow-history bucket at each minute boundary.
                    # sec_start_ms/1000 is the ts of the second we just wrote;
                    # when its second-of-minute is 59 we've just completed a
                    # minute (seconds 0..59), so aggregate those 60 bars.
                    sec_of_min = (sec_start_ms // 1000) % 60
                    if sec_of_min == 59 and len(st.bars_1s) >= 60:
                        last60 = list(st.bars_1s)[-60:]
                        bv = sum(b['buy_v'] for b in last60)
                        sv = sum(b['sell_v'] for b in last60)
                        tot = bv + sv
                        if tot > 0:
                            st.flow_history.append({
                                'ts':      (sec_start_ms // 1000) - 59,  # minute start
                                'buy_pct': round(100 * bv / tot, 1),
                                'buy_v':   bv,
                                'sell_v':  sv,
                            })
            # Run signal engine after bars are written for all symbols
            self._run_signal_engine()
            # Flush expired tick captures (2s grace period to drain last ticks)
            now_cap = int(time.time() * 1000)
            with self._captures_lock:
                expired = {key: self._tick_captures.pop(key)
                           for key in list(self._tick_captures)
                           if now_cap > self._tick_captures[key]['end_ms'] + 2000}
            for cap in expired.values():
                self._flush_capture(cap)

    # --------------- signal engine ---------------
    def _run_signal_engine(self):
        # Gate: only emit signals during regular trading hours (9:30 AM - 4:00 PM ET, Mon-Fri).
        # Pre-market / after-hours volume is too thin to trust — the signal fires but the
        # fill quality and follow-through are poor. User explicitly requested RTH-only alerts.
        now_et = datetime.now(ET)
        if now_et.weekday() >= 5:
            return
        minutes_et = now_et.hour * 60 + now_et.minute
        if minutes_et < 9*60 + 30 or minutes_et >= 16*60:
            return
        # Run for every stock ticker currently being streamed (active + extras)
        tickers = self.get_all_subscribed()
        if not tickers:
            return
        btc_ind = None
        if self.btc_enabled and BTC_SYMBOL in self.states:
            btc_ind = compute_indicators(self.states[BTC_SYMBOL])
        stock_indicators = {}
        for tkr in tickers:
            st = self.states.get(tkr)
            if not st or len(st.bars_1s) < 60:
                continue
            stock_indicators[tkr] = compute_indicators(st)
        for tkr, ind in stock_indicators.items():
            st = self.states.get(tkr)
            if not st:
                continue
            sig = detect_signal(tkr, ind, btc_ind, miner_indicators=stock_indicators)
            if not sig:
                continue
            if not self._setup_state_gate(tkr, sig, ind):
                continue
            key = f"{tkr}:{sig['side']}:{sig.get('setup_type', 'unknown')}"
            last = self._last_signal_ts.get(key, 0)
            cooldown = float(SETUP_COOLDOWN_SEC.get(sig.get('setup_type'), 30))
            if time.time() - last < cooldown:
                continue
            self._last_signal_ts[key] = time.time()
            sig['ts'] = int(time.time())
            # Compute SL/TP target prices from per-ticker brackets so the
            # alert shows where the bracket legs will sit.
            brk = SCALP_TICKER_BRACKETS.get(tkr,
                      {'sl': SCALP_DEFAULT_SL, 'tp': SCALP_DEFAULT_TP})
            _price = sig.get('price')
            if _price:
                if sig['side'] == 'LONG':
                    sig['sl_price'] = round(_price * (1 - brk['sl']), 4)
                    sig['tp_price'] = round(_price * (1 + brk['tp']), 4)
                else:
                    sig['sl_price'] = round(_price * (1 + brk['sl']), 4)
                    sig['tp_price'] = round(_price * (1 - brk['tp']), 4)
                sig['sl_pct'] = brk['sl']
                sig['tp_pct'] = brk['tp']
            with st.lock:
                sig['best_bid'] = st.best_bid
                sig['best_ask'] = st.best_ask
                sig['bid_size'] = st.bid_size
                sig['ask_size'] = st.ask_size
                sig['quote_ts'] = st.last_quote_ts_ms
            if sig.get('best_bid') is not None and sig.get('best_ask') is not None:
                sig['spread'] = round(sig['best_ask'] - sig['best_bid'], 4)
                sig['mid_price'] = round((sig['best_ask'] + sig['best_bid']) / 2, 4)
            sig['execution_quality'] = execution_quality(sig)
            sig.setdefault('signal_quality', {}).setdefault('score_model', {})['execution_score'] = sig['execution_quality']['score']
            sig['signal_quality']['score_model']['execution_reasons'] = sig['execution_quality'].get('reasons', [])
            if sig['execution_quality']['score'] < EXECUTION_QUALITY_MIN:
                _log_near_signal(
                    tkr, sig.get('side'), 'execution_quality_low',
                    sig.get('score', 0), ind, sig.get('btc_context'),
                    sig.get('signal_quality', {}).get('score_components', {}),
                )
                continue
            sig['shadow_variants'] = shadow_variants(sig)
            self.signals.append(sig)
            log.info(
                f"SIGNAL {tkr} {sig['side']} {sig.get('conviction')} "
                f"@ ${_price} -> TP ${sig.get('tp_price')} ({brk['tp']*100:.1f}%) "
                f"SL ${sig.get('sl_price')} ({brk['sl']*100:.1f}%) "
                f"score={abs(sig.get('score', 0))}/10"
            )
            with self._cb_lock:
                callbacks = list(self._on_signal_cbs)
            for cb in callbacks:
                try: cb(sig)
                except Exception as e: log.error(f'signal cb error: {e}')

    def _setup_state_gate(self, ticker: str, sig: dict, ind: dict) -> bool:
        if not STATE_MACHINE_ENABLED:
            return True
        setup = sig.get('setup_type')
        if setup not in ('flow_exhaustion_fade', 'vwap_reclaim_breakdown'):
            return True
        now = time.time()
        side = sig.get('side')
        key = f'{ticker}:{side}:{setup}'
        state = self._setup_states.get(key)
        trigger = {
            'flow_exhaustion_fade': sig.get('signal_quality', {}).get('flow_fade_confirmed'),
            'vwap_reclaim_breakdown': abs(ind.get('vwap_dist_sigma') or 0) <= 1.0,
        }.get(setup)
        if not trigger:
            self._setup_states.pop(key, None)
            return False
        if not state:
            self._setup_states[key] = {
                'armed_at': now,
                'price': sig.get('price'),
                'score': sig.get('score'),
            }
            _log_near_signal(
                ticker, side, f'setup_armed:{setup}', sig.get('score', 0),
                ind, sig.get('btc_context'),
                sig.get('signal_quality', {}).get('score_components', {}),
            )
            return False
        age = now - float(state.get('armed_at', now))
        if age > STATE_MACHINE_MAX_CONFIRM_SEC:
            self._setup_states[key] = {
                'armed_at': now,
                'price': sig.get('price'),
                'score': sig.get('score'),
            }
            return False
        if age < STATE_MACHINE_MIN_CONFIRM_SEC:
            return False
        sig['setup_state'] = {
            'confirmed': True,
            'armed_age_sec': round(age, 2),
            'armed_price': state.get('price'),
            'armed_score': state.get('score'),
        }
        self._setup_states.pop(key, None)
        return True


# ────────────────────────────────────────────────────────────────────────
# Indicators (pure functions on SymbolState)
# ────────────────────────────────────────────────────────────────────────
def compute_indicators(st: SymbolState) -> dict:
    with st.lock:
        bars = list(st.bars_1s)
        price = st.last_trade_price
        symbol = st.symbol
        last_trade_ts_ms = st.last_trade_ts_ms
        last_quote_ts_ms = st.last_quote_ts_ms
        best_bid = st.best_bid
        best_ask = st.best_ask
        bid_size = st.bid_size
        ask_size = st.ask_size
        quote_history = list(st.quote_history)
        pv_sum = st.session_pv_sum
        v_sum = st.session_v_sum

    if len(bars) < 5 or price is None:
        return {
            'price': price, 'bars': len(bars), 'ready': False,
            'symbol': symbol,
        }

    closes = [b['c'] for b in bars]
    vols   = [b['v'] for b in bars]
    buy_v  = [b['buy_v'] for b in bars]
    sell_v = [b['sell_v'] for b in bars]
    n_arr  = [b['n'] for b in bars]

    def pct_change(arr, n):
        if len(arr) < n + 1 or arr[-n-1] == 0: return None
        return round((arr[-1] - arr[-n-1]) / arr[-n-1] * 100, 3)

    def ema(arr, span):
        if len(arr) < span: return None
        k = 2 / (span + 1)
        e = arr[-span]
        for v in arr[-span+1:]:
            e = v * k + e * (1 - k)
        return round(e, 4)

    def zscore(arr, window):
        if len(arr) < window + 1: return None
        recent = arr[-window-1:-1]
        cur = arr[-1]
        mean = sum(recent) / len(recent)
        var = sum((x - mean) ** 2 for x in recent) / len(recent)
        sd = var ** 0.5
        return round((cur - mean) / sd, 2) if sd > 0 else 0

    def realized_vol_pct(window):
        if len(closes) < window + 1:
            return None
        vals = closes[-window-1:]
        rets = []
        for prev, cur in zip(vals, vals[1:]):
            if prev:
                rets.append((cur - prev) / prev * 100)
        if not rets:
            return None
        rv = (sum(r * r for r in rets) / len(rets)) ** 0.5
        return round(rv, 4)

    # Order-flow imbalance (30s, 120s)
    def flow(window):
        if len(buy_v) < window: return None
        b = sum(buy_v[-window:])
        s = sum(sell_v[-window:])
        tot = b + s
        if tot == 0: return {'buy_pct': None, 'ratio': None}
        return {'buy_pct': round(100 * b / tot, 1), 'ratio': round(b / max(s, 1), 2)}

    def flow_slice(start, end):
        if len(buy_v) < abs(start):
            return None
        b = sum(buy_v[start:end])
        s = sum(sell_v[start:end])
        tot = b + s
        if tot == 0:
            return {'buy_pct': None, 'ratio': None}
        return {'buy_pct': round(100 * b / tot, 1), 'ratio': round(b / max(s, 1), 2)}

    # VWAP
    vwap = round(pv_sum / v_sum, 4) if v_sum > 0 else None
    # σ-band estimate from recent 5-min stddev of prices
    # 2026-04-20: added two guards after live RIOT produced a bogus
    # "-18.36σ" signal from tiny early-session stddev:
    #   (1) Floor sd at 5 bps of price — prevents div-by-tiny
    #   (2) Clamp output to ±5σ — anything larger is an artifact
    if len(closes) >= 300:
        recent = closes[-300:]
        m = sum(recent) / len(recent)
        sd = (sum((x - m)**2 for x in recent) / len(recent)) ** 0.5
        if price:
            sd = max(sd, price * 0.0005)  # floor: 5 bps
    else:
        sd = 0
    if vwap and sd > 0:
        raw = (price - vwap) / sd
        raw = max(-5.0, min(5.0, raw))   # clamp to ±5σ
        vwap_dist_sigma = round(raw, 2)
    else:
        vwap_dist_sigma = None

    # Volume burst: z-score of latest 1s vol vs last 60s mean
    vol_z = zscore(vols, 60)
    # Tick velocity z-score
    tick_z = zscore(n_arr, 30)
    session_open = bars[0]['o'] if bars else None
    session_high = max(b['h'] for b in bars) if bars else None
    session_low = min(b['l'] for b in bars) if bars else None
    opening_5m = bars[:300]
    opening_15m = bars[:900]
    opening_5m_high = max((b['h'] for b in opening_5m), default=None)
    opening_5m_low = min((b['l'] for b in opening_5m), default=None)
    opening_15m_high = max((b['h'] for b in opening_15m), default=None)
    opening_15m_low = min((b['l'] for b in opening_15m), default=None)
    session_range = (session_high - session_low) if session_high is not None and session_low is not None else None
    session_range_pos = None
    if session_range and session_range > 0 and price is not None:
        session_range_pos = round((price - session_low) / session_range, 3)
    now_ms = int(time.time() * 1000)
    last_trade_age_sec = round((now_ms - last_trade_ts_ms) / 1000, 2) if last_trade_ts_ms else None
    last_quote_age_sec = round((now_ms - last_quote_ts_ms) / 1000, 2) if last_quote_ts_ms else None
    flow_10s = flow(10)
    flow_30s = flow(30)
    flow_30s_prev = flow_slice(-60, -30) if len(buy_v) >= 60 else None
    flow_30s_delta = None
    if flow_30s and flow_30s_prev and flow_30s.get('buy_pct') is not None and flow_30s_prev.get('buy_pct') is not None:
        flow_30s_delta = round(flow_30s['buy_pct'] - flow_30s_prev['buy_pct'], 1)
    quote_imbalance = None
    if bid_size is not None and ask_size is not None and (bid_size + ask_size) > 0:
        quote_imbalance = round((bid_size - ask_size) / (bid_size + ask_size), 3)
    quote_imbalance_5s_ago = None
    if quote_history:
        cutoff = now_ms - 5000
        older = [q for q in quote_history if q[0] <= cutoff and q[5] is not None]
        if older:
            quote_imbalance_5s_ago = older[-1][5]
    quote_imbalance_delta_5s = None
    if quote_imbalance is not None and quote_imbalance_5s_ago is not None:
        quote_imbalance_delta_5s = round(quote_imbalance - quote_imbalance_5s_ago, 3)
    spread_pct = None
    if best_bid is not None and best_ask is not None and price:
        spread_pct = round((float(best_ask) - float(best_bid)) / price * 100, 4)

    ema5  = ema(closes, 5)
    ema15 = ema(closes, 15)
    ema60 = ema(closes, 60)
    stack = None
    if ema5 and ema15 and ema60:
        if ema5 > ema15 > ema60: stack = 'bull'
        elif ema5 < ema15 < ema60: stack = 'bear'
        else: stack = 'mixed'

    return {
        'ready': True,
        'symbol': symbol,
        'price':  price,
        'bars':   len(bars),
        'last_trade_ts_ms': last_trade_ts_ms,
        'last_quote_ts_ms': last_quote_ts_ms,
        'last_trade_age_sec': last_trade_age_sec,
        'last_quote_age_sec': last_quote_age_sec,
        'mom_5s':  pct_change(closes, 5),
        'mom_15s': pct_change(closes, 15),
        'mom_30s': pct_change(closes, 30),
        'mom_60s': pct_change(closes, 60),
        'mom_180s': pct_change(closes, 180),
        'ema_5s':  ema5,
        'ema_15s': ema15,
        'ema_60s': ema60,
        'ema_stack': stack,
        'vwap':      vwap,
        'vwap_dist': round(price - vwap, 4) if vwap else None,
        'vwap_dist_sigma': vwap_dist_sigma,
        'flow_10s':  flow_10s,
        'flow_30s':  flow_30s,
        'flow_30s_prev': flow_30s_prev,
        'flow_30s_delta': flow_30s_delta,
        'flow_120s': flow(120),
        'realized_vol_60s_pct': realized_vol_pct(60),
        'realized_vol_180s_pct': realized_vol_pct(180),
        'vol_z_60s': vol_z,
        'tick_z_30s': tick_z,
        'vol_10s': sum(vols[-10:]) if len(vols) >= 10 else None,
        'vol_60s': sum(vols[-60:]) if len(vols) >= 60 else None,
        'ticks_10s': sum(n_arr[-10:]) if len(n_arr) >= 10 else None,
        'ticks_30s': sum(n_arr[-30:]) if len(n_arr) >= 30 else None,
        'session_open': session_open,
        'session_high': session_high,
        'session_low': session_low,
        'opening_5m_high': opening_5m_high,
        'opening_5m_low': opening_5m_low,
        'opening_15m_high': opening_15m_high,
        'opening_15m_low': opening_15m_low,
        'opening_5m_break_state': _opening_break_state(price, opening_5m_high, opening_5m_low),
        'opening_15m_break_state': _opening_break_state(price, opening_15m_high, opening_15m_low),
        'session_range_pct': round(session_range / price * 100, 3) if session_range and price else None,
        'session_range_pos': session_range_pos,
        'session_return_pct': round((price - session_open) / session_open * 100, 3)
                              if session_open and price else None,
        'best_bid': best_bid,
        'best_ask': best_ask,
        'bid_size': bid_size,
        'ask_size': ask_size,
        'spread_pct': spread_pct,
        'quote_imbalance': quote_imbalance,
        'quote_imbalance_delta_5s': quote_imbalance_delta_5s,
    }


# ────────────────────────────────────────────────────────────────────────
# Signal detection
# ────────────────────────────────────────────────────────────────────────
def _btc_signal_context(ticker: str, ind: dict, btc_ind: Optional[dict]) -> dict:
    stale = False
    stale_reason = None
    if not btc_ind or not btc_ind.get('ready'):
        return {
            'ready': False, 'regime': 'unknown', 'stack': None,
            'mom_5s': None, 'mom_15s': None, 'mom_60s': None, 'mom_180s': None,
            'beta': BTC_BETA.get(ticker), 'implied_stock_mom_60s': None,
            'stock_minus_btc_implied_60s': None, 'acceleration': 'unknown',
            'stale': True, 'stale_reason': 'not_ready',
        }
    age = btc_ind.get('last_trade_age_sec')
    quote_age = btc_ind.get('last_quote_age_sec')
    freshness_source = 'trade'
    if age is not None and age > BTC_MAX_STALE_SEC:
        if quote_age is not None and quote_age <= BTC_MAX_QUOTE_STALE_SEC:
            freshness_source = 'quote_fallback'
        else:
            stale = True
            stale_reason = f'btc_trade_age_{age}_quote_age_{quote_age}'
    elif age is None and quote_age is not None and quote_age <= BTC_MAX_QUOTE_STALE_SEC:
        freshness_source = 'quote_fallback'
    elif age is None:
        stale = True
        stale_reason = f'btc_trade_age_unknown_quote_age_{quote_age}'
    b5 = btc_ind.get('mom_5s')
    b15 = btc_ind.get('mom_15s')
    b30 = btc_ind.get('mom_30s')
    b60 = btc_ind.get('mom_60s')
    b180 = btc_ind.get('mom_180s')
    stack = btc_ind.get('ema_stack')
    regime = 'neutral'
    if stack == 'bull' and (b60 or 0) > 0:
        regime = 'bull'
    elif stack == 'bear' and (b60 or 0) < 0:
        regime = 'bear'
    elif b60 is not None and b60 >= 0.10:
        regime = 'bull_momentum'
    elif b60 is not None and b60 <= -0.10:
        regime = 'bear_momentum'
    accel = 'unknown'
    if b5 is not None and b15 is not None and b60 is not None:
        if b5 > 0 and b15 > 0 and b60 > 0 and b5 >= b15:
            accel = 'bull_accelerating'
        elif b5 < 0 and b15 < 0 and b60 < 0 and b5 <= b15:
            accel = 'bear_accelerating'
        elif abs(b5) < abs(b15) < abs(b60):
            accel = 'decelerating'
        else:
            accel = 'mixed'
    regime_detail = regime
    if b15 is not None and b60 is not None:
        if abs(b60) < 0.05 and abs(b15) < 0.03:
            regime_detail = 'chop'
        elif b15 >= 0.12 and b60 > 0:
            regime_detail = 'impulse_up'
        elif b15 <= -0.12 and b60 < 0:
            regime_detail = 'impulse_down'
        elif b5 is not None and b60 > 0.10 and b5 < -0.03:
            regime_detail = 'bull_reversal_risk'
        elif b5 is not None and b60 < -0.10 and b5 > 0.03:
            regime_detail = 'bear_reversal_risk'
    beta = BTC_BETA.get(ticker)
    stock_mom = ind.get('mom_60s')
    implied = beta * b60 if beta is not None and b60 is not None else None
    rel = float(stock_mom) - implied if stock_mom is not None and implied is not None else None
    lead_lag = {}
    for horizon, stock_key, btc_val in (
            ('15s', 'mom_15s', b15),
            ('30s', 'mom_30s', b30),
            ('60s', 'mom_60s', b60)):
        stock_val = ind.get(stock_key)
        implied_h = beta * btc_val if beta is not None and btc_val is not None else None
        lag_h = float(stock_val) - implied_h if stock_val is not None and implied_h is not None else None
        lead_lag[horizon] = {
            'btc_mom': btc_val,
            'stock_mom': stock_val,
            'btc_implied_stock_mom': round(implied_h, 3) if implied_h is not None else None,
            'stock_minus_btc_implied': round(lag_h, 3) if lag_h is not None else None,
        }
    return {
        'ready': True, 'regime': regime, 'stack': stack,
        'regime_detail': regime_detail,
        'mom_5s': b5, 'mom_15s': b15, 'mom_30s': b30, 'mom_60s': b60, 'mom_180s': b180,
        'beta': beta,
        'implied_stock_mom_60s': round(implied, 3) if implied is not None else None,
        'stock_minus_btc_implied_60s': round(rel, 3) if rel is not None else None,
        'acceleration': accel,
        'lead_lag': lead_lag,
        'stale': stale,
        'stale_reason': stale_reason,
        'last_trade_age_sec': age,
        'last_quote_age_sec': quote_age,
        'freshness_source': freshness_source,
    }


def _classify_setup(side: str, ind: dict, btc_ctx: dict, components: dict) -> str:
    vds = ind.get('vwap_dist_sigma')
    bp = (ind.get('flow_30s') or {}).get('buy_pct')
    rel = btc_ctx.get('stock_minus_btc_implied_60s')
    if bp is not None and ((side == 'SHORT' and bp >= 65) or (side == 'LONG' and bp <= 35)):
        return 'flow_exhaustion_fade'
    if rel is not None and (
        (side == 'LONG' and rel >= REL_STRENGTH_MIN)
        or (side == 'SHORT' and rel <= -REL_STRENGTH_MIN)
    ):
        return 'btc_relative_strength'
    if components.get('burst') and abs(ind.get('mom_15s') or 0) >= 0.08:
        return 'momentum_breakout'
    if vds is not None and abs(vds) <= 1.0 and (
        (side == 'LONG' and (ind.get('vwap_dist') or 0) > 0)
        or (side == 'SHORT' and (ind.get('vwap_dist') or 0) < 0)
    ):
        return 'vwap_reclaim_breakdown'
    return 'trend_pullback'


def _flow_fade_confirmed(side: str, ind: dict) -> bool:
    bp = (ind.get('flow_30s') or {}).get('buy_pct')
    if bp is None:
        return False
    delta = ind.get('flow_30s_delta')
    m5 = ind.get('mom_5s')
    qi = ind.get('quote_imbalance')
    qid = ind.get('quote_imbalance_delta_5s')
    if side == 'SHORT' and bp >= 65:
        return (
            (delta is not None and delta <= -FLOW_FADE_MIN_DELTA_PCT)
            or (m5 is not None and m5 < -0.02)
            or (qi is not None and qi < -0.10)
            or (qid is not None and qid <= -0.15)
        )
    if side == 'LONG' and bp <= 35:
        return (
            (delta is not None and delta >= FLOW_FADE_MIN_DELTA_PCT)
            or (m5 is not None and m5 > 0.02)
            or (qi is not None and qi > 0.10)
            or (qid is not None and qid >= 0.15)
        )
    return True


def _min_score_for_setup(setup_type: str) -> int:
    try:
        return int(MIN_SCORE_BY_SETUP.get(setup_type, 4))
    except Exception:
        return 4


def _session_phase(ind: dict) -> str:
    minutes = (ind.get('bars') or 0) / 60
    if minutes < 45:
        return 'open'
    if minutes >= 300:
        return 'late'
    if 150 <= minutes < 240:
        return 'midday'
    return 'normal'


def _opening_break_state(price: Optional[float], high: Optional[float], low: Optional[float]) -> str:
    if price is None or high is None or low is None:
        return 'unknown'
    if price > high:
        return 'above_opening_range'
    if price < low:
        return 'below_opening_range'
    return 'inside_opening_range'


def _adjusted_min_score(setup_type: str, ind: dict) -> int:
    base = _min_score_for_setup(setup_type)
    phase = _session_phase(ind)
    try:
        return max(4, base + int(SESSION_MIN_SCORE_ADJUST.get(phase, 0)))
    except Exception:
        return base


def passive_regime_profile(ticker: str, side: str, setup_type: str, ind: dict,
                           btc_ctx: dict, score: int) -> dict:
    regime = btc_ctx.get('regime_detail') or btc_ctx.get('regime') or 'unknown'
    profile = {
        'ticker': ticker,
        'side': side,
        'setup_type': setup_type,
        'btc_regime': regime,
        'base_score': score,
        'suggested_score_adjustment': 0,
        'tags': [],
        'mode': 'passive_log_only',
    }
    flow120 = ind.get('flow_120s') or {}
    rel = btc_ctx.get('stock_minus_btc_implied_60s')
    if side == 'SHORT' and flow120.get('buy_pct') is not None and flow120.get('buy_pct') >= 60:
        if btc_ctx.get('regime') not in ('bear', 'bear_momentum'):
            profile['suggested_score_adjustment'] += 1
            profile['tags'].append('short_sustained_buying_btc_not_bearish')
    if side == 'LONG' and rel is not None and rel <= -REL_STRENGTH_MIN:
        profile['suggested_score_adjustment'] -= 1
        profile['tags'].append('long_stock_lagging_btc')
    if regime == 'chop' and setup_type in ('momentum_breakout', 'btc_relative_strength'):
        profile['suggested_score_adjustment'] += -1 if side == 'LONG' else 1
        profile['tags'].append('momentum_setup_in_btc_chop')
    if regime in ('impulse_up', 'bull') and side == 'SHORT':
        profile['suggested_score_adjustment'] += 1
        profile['tags'].append('short_against_btc_regime')
    if regime in ('impulse_down', 'bear') and side == 'LONG':
        profile['suggested_score_adjustment'] -= 1
        profile['tags'].append('long_against_btc_regime')
    return profile


def _lead_lag_bias(side: str, btc_ctx: dict) -> dict:
    lead_lag = btc_ctx.get('lead_lag') or {}
    h30 = lead_lag.get('30s') or {}
    h60 = lead_lag.get('60s') or {}
    lag30 = h30.get('stock_minus_btc_implied')
    lag60 = h60.get('stock_minus_btc_implied')
    btc30 = h30.get('btc_mom')
    out = {'state': 'unknown', 'score': 0, 'lag_30s': lag30, 'lag_60s': lag60}
    if lag30 is None or btc30 is None:
        return out
    if side == 'LONG':
        if btc30 > 0 and lag30 <= -BTC_LEAD_LAG_MIN_PCT:
            out.update({'state': 'btc_leads_stock_lagging_long', 'score': 1})
        elif lag30 >= BTC_CHASE_MAX_PCT or (lag60 is not None and lag60 >= BTC_CHASE_MAX_PCT):
            out.update({'state': 'stock_chasing_btc_long', 'score': -1})
    else:
        if btc30 < 0 and lag30 >= BTC_LEAD_LAG_MIN_PCT:
            out.update({'state': 'btc_leads_stock_lagging_short', 'score': 1})
        elif lag30 <= -BTC_CHASE_MAX_PCT or (lag60 is not None and lag60 <= -BTC_CHASE_MAX_PCT):
            out.update({'state': 'stock_chasing_btc_short', 'score': -1})
    return out


def _side_score_breakdown(components: dict) -> dict:
    long_score = 0.0
    short_score = 0.0
    for key, value in (components or {}).items():
        if isinstance(value, bool) or key in ('alive', 'flow_fade_confirmed'):
            continue
        if isinstance(value, (int, float)):
            if value > 0:
                long_score += float(value)
            elif value < 0:
                short_score += abs(float(value))
    return {
        'long_score': round(long_score, 3),
        'short_score': round(short_score, 3),
        'gap': round(abs(long_score - short_score), 3),
    }


def _miner_basket_context(ticker: str, side: str, miner_indicators: Optional[dict]) -> dict:
    rows = []
    confirming = 0
    opposing = 0
    ready = 0
    for sym, ind in (miner_indicators or {}).items():
        if sym == ticker or sym == BTC_SYMBOL or not isinstance(ind, dict) or not ind.get('ready'):
            continue
        ready += 1
        stack = ind.get('ema_stack')
        mom15 = ind.get('mom_15s')
        mom60 = ind.get('mom_60s')
        bullish = stack == 'bull' and (mom15 or 0) > 0 and (mom60 or 0) >= 0
        bearish = stack == 'bear' and (mom15 or 0) < 0 and (mom60 or 0) <= 0
        confirms = (side == 'LONG' and bullish) or (side == 'SHORT' and bearish)
        opposes = (side == 'LONG' and bearish) or (side == 'SHORT' and bullish)
        if confirms:
            confirming += 1
        if opposes:
            opposing += 1
        rows.append({
            'ticker': sym,
            'ema_stack': stack,
            'mom_15s': mom15,
            'mom_60s': mom60,
            'confirms': confirms,
            'opposes': opposes,
        })
    state = 'unknown'
    score = 0
    if ready:
        if confirming >= MINER_BASKET_MIN_CONFIRMING:
            state = 'confirmed'
            score = MINER_BASKET_BONUS
        elif opposing:
            state = 'opposed'
            score = -MINER_BASKET_PENALTY
        else:
            state = 'mixed'
    return {
        'state': state,
        'score': score,
        'ready_peers': ready,
        'confirming': confirming,
        'opposing': opposing,
        'peers': rows,
    }


def execution_quality(sig: dict) -> dict:
    price = sig.get('price') or 0
    spread = sig.get('spread')
    if spread is None and sig.get('best_bid') is not None and sig.get('best_ask') is not None:
        spread = float(sig['best_ask']) - float(sig['best_bid'])
    spread_pct = (spread / price * 100) if spread is not None and price else None
    side = sig.get('side')
    ind = sig.get('indicators') or {}
    qi = ind.get('quote_imbalance')
    qid = ind.get('quote_imbalance_delta_5s')
    quote_age = ind.get('last_quote_age_sec')
    score = 100
    reasons = []
    if spread_pct is None:
        score -= 15
        reasons.append('spread_unknown')
    elif spread_pct > 0.15:
        score -= 35
        reasons.append('spread_wide')
    elif spread_pct > 0.08:
        score -= 20
        reasons.append('spread_elevated')
    if quote_age is None:
        score -= 10
        reasons.append('quote_age_unknown')
    elif quote_age > 3:
        score -= 20
        reasons.append('quote_stale')
    if qi is not None:
        if side == 'LONG' and qi < -0.25:
            score -= 15
            reasons.append('ask_heavy_book')
        elif side == 'SHORT' and qi > 0.25:
            score -= 15
            reasons.append('bid_heavy_book')
    if qid is not None:
        if side == 'LONG' and qid < -0.20:
            score -= 10
            reasons.append('book_worsening_long')
        elif side == 'SHORT' and qid > 0.20:
            score -= 10
            reasons.append('book_worsening_short')
    return {
        'score': max(0, min(100, round(score, 1))),
        'spread_pct': round(spread_pct, 4) if spread_pct is not None else None,
        'quote_age_sec': quote_age,
        'quote_imbalance': qi,
        'quote_imbalance_delta_5s': qid,
        'reasons': reasons,
    }


def shadow_variants(sig: dict) -> dict:
    setup = sig.get('setup_type')
    score = abs(sig.get('score') or 0)
    quality = sig.get('signal_quality') or {}
    btc = sig.get('btc_context') or {}
    lead_lag = sig.get('lead_lag') or {}
    return {
        'momentum_only': setup in ('momentum_breakout', 'btc_relative_strength') and score >= 5,
        'strict_flow_fade': setup == 'flow_exhaustion_fade'
                            and quality.get('flow_fade_confirmed')
                            and score >= 6,
        'btc_lead_lag_only': (lead_lag.get('score') or 0) > 0 and not btc.get('stale'),
        'high_quality_execution_only': (sig.get('execution_quality') or {}).get('score', 0) >= 75,
    }


def _log_near_signal(ticker: str, side: Optional[str], reason: str, score: float,
                     ind: dict, btc_ctx: Optional[dict] = None,
                     components: Optional[dict] = None):
    if abs(score or 0) < NEAR_SIGNAL_MIN_SCORE:
        return
    now = time.time()
    key = f'{ticker}:{side or "NA"}:{reason}'
    with _near_signal_lock:
        if now - _last_near_signal_log.get(key, 0) < 30:
            return
        _last_near_signal_log[key] = now
    try:
        os.makedirs(NEAR_SIGNAL_DIR, exist_ok=True)
        day = datetime.now(ET).date().isoformat()
        path = os.path.join(NEAR_SIGNAL_DIR, f'near_signals_{day}.jsonl')
        row = {
            'created_at': int(now),
            'ticker': ticker,
            'side': side,
            'reason': reason,
            'score': score,
            'price': ind.get('price'),
            'setup_type': _classify_setup(side, ind, btc_ctx or {}, components or {}) if side else None,
            'components': components or {},
            'indicators': {
                k: v for k, v in (ind or {}).items()
                if isinstance(v, (int, float, str, bool)) or v is None
            },
            'btc_context': btc_ctx or {},
        }
        with open(path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(row, separators=(',', ':'), default=str) + '\n')
    except Exception as e:
        log.warning(f'near signal log failed: {e}')


def detect_signal(ticker: str, ind: dict, btc_ind: Optional[dict],
                  miner_indicators: Optional[dict] = None) -> Optional[dict]:
    """
    Return {side, conviction, reasons} or None.
    Combines: EMA stack + VWAP side + order flow + volume burst + BTC alignment.
    """
    if not ind or not ind.get('ready'):
        return None
    if (ind.get('last_trade_age_sec') is not None
            and ind.get('last_trade_age_sec') > STOCK_MAX_STALE_SEC):
        return None
    reasons_long = []
    reasons_short = []
    components = {}
    score = 0

    stack = ind.get('ema_stack')
    if stack == 'bull':
        score += 2
        components['ema'] = 2
        reasons_long.append('EMA 5s>15s>60s bull stack')
    elif stack == 'bear':
        score -= 2
        components['ema'] = -2
        reasons_short.append('EMA 5s<15s<60s bear stack')
    else:
        return None

    vd = ind.get('vwap_dist')
    vds = ind.get('vwap_dist_sigma')
    near_vwap = vds is not None and abs(vds) < VWAP_CHOP_SIGMA
    if vd is not None:
        if vd > 0:
            score += 1
            components['vwap'] = 1
            reasons_long.append(f'above VWAP {"+%.2f sigma"%vds if vds is not None else ""}')
        else:
            score -= 1
            components['vwap'] = -1
            reasons_short.append(f'below VWAP {"%.2f sigma"%vds if vds is not None else ""}')

    f30 = ind.get('flow_30s') or {}
    bp = f30.get('buy_pct')
    if bp is not None:
        if bp >= 65:
            # Fresh buying pressure is not a SHORT edge by itself. It only
            # becomes actionable after the flow-fade confirmation proves
            # exhaustion/rollover later in this decision path.
            components['flow_30s'] = 0
            reasons_short.append(f'30s buy pressure watch ({bp}% buys)')
        elif bp <= 35:
            components['flow_30s'] = 0
            reasons_long.append(f'30s sell pressure watch ({100-bp:.1f}% sells)')

    f120 = ind.get('flow_120s') or {}
    bp2 = f120.get('buy_pct')
    if bp2 is not None:
        if bp2 >= 60:
            # Sustained buying is only useful to a SHORT after exhaustion is
            # proven. Treat it as context here; flow-fade confirmation later
            # decides whether it is actionable.
            components['flow_120s'] = 0
            reasons_short.append(f'120s buy pressure watch ({bp2}%)')
        elif bp2 <= 40:
            components['flow_120s'] = 0
            reasons_long.append(f'120s sell pressure watch ({100-bp2:.1f}% sells)')

    vz = ind.get('vol_z_60s')
    tz = ind.get('tick_z_30s')
    burst = (vz is not None and vz >= 2) or (tz is not None and tz >= 2)
    alive = (vz is not None and vz >= MIN_ACTIVITY_Z) or (tz is not None and tz >= MIN_ACTIVITY_Z)
    components['burst'] = burst
    components['alive'] = alive
    if burst:
        if score > 0:
            score += 1
            components['burst_score'] = 1
            reasons_long.append(f'vol/tick burst (vol z={vz}, tick z={tz})')
        elif score < 0:
            score -= 1
            components['burst_score'] = -1
            reasons_short.append(f'vol/tick burst (vol z={vz}, tick z={tz})')

    m5 = ind.get('mom_5s')
    m15 = ind.get('mom_15s')
    if m5 is not None and m15 is not None:
        if m5 > 0.05 and m15 > 0.05:
            score += 1
            components['momentum'] = 1
            reasons_long.append(f'5s:{m5:+.2f}% 15s:{m15:+.2f}%')
        elif m5 < -0.05 and m15 < -0.05:
            score -= 1
            components['momentum'] = -1
            reasons_short.append(f'5s:{m5:+.2f}% 15s:{m15:+.2f}%')

    flow_extreme = bp is not None and (bp >= 65 or bp <= 35)
    rv60 = ind.get('realized_vol_60s_pct')
    if (REALIZED_VOL_CHOP_PENALTY_ENABLED and rv60 is not None
            and rv60 < MIN_REALIZED_VOL_60S_PCT and not flow_extreme and not burst):
        if score > 0:
            score -= 1
            components['realized_vol_chop'] = -1
            reasons_long.append(f'low 60s realized vol ({rv60}%)')
        elif score < 0:
            score += 1
            components['realized_vol_chop'] = 1
            reasons_short.append(f'low 60s realized vol ({rv60}%)')
    if near_vwap and not burst and not flow_extreme:
        _log_near_signal(ticker, None, 'near_vwap_chop', score, ind, components=components)
        return None
    if not alive and not flow_extreme:
        _log_near_signal(ticker, None, 'dead_tape', score, ind, components=components)
        return None

    btc_ctx = _btc_signal_context(ticker, ind, btc_ind)
    if (not btc_ctx.get('ready')) and ticker in ('CLSK', 'MARA', 'RIOT'):
        _log_near_signal(ticker, None, 'btc_not_ready', score, ind, btc_ctx, components)
        return None
    if btc_ctx.get('ready'):
        if btc_ctx.get('stale') and ticker in ('CLSK', 'MARA', 'RIOT'):
            _log_near_signal(ticker, None, 'btc_stale', score, ind, btc_ctx, components)
            return None
        b_stack = btc_ctx.get('stack')
        b_mom = btc_ctx.get('mom_60s')
        if b_stack == 'bull' and b_mom and b_mom > 0:
            if score > 0:
                score += 2
                components['btc'] = 2
                reasons_long.append(f'BTC aligned bull, 60s {b_mom:+.2f}%')
            elif score < 0:
                score += 2
                components['btc'] = 2
                reasons_short.append(f'BTC bull conflict, 60s {b_mom:+.2f}%')
        elif b_stack == 'bear' and b_mom and b_mom < 0:
            if score < 0:
                score -= 2
                components['btc'] = -2
                reasons_short.append(f'BTC aligned bear, 60s {b_mom:+.2f}%')
            elif score > 0:
                score -= 2
                components['btc'] = -2
                reasons_long.append(f'BTC bear conflict, 60s {b_mom:+.2f}%')

        rel = btc_ctx.get('stock_minus_btc_implied_60s')
        if rel is not None:
            if score > 0 and rel >= REL_STRENGTH_MIN:
                score += 1
                components['relative_strength'] = 1
                reasons_long.append(f'stock leading BTC-implied move {rel:+.2f}%')
            elif score > 0 and rel <= -REL_STRENGTH_MIN:
                score -= 1
                components['relative_strength'] = -1
                reasons_long.append(f'stock lagging BTC-implied move {rel:+.2f}%')
            elif score < 0 and rel <= -REL_STRENGTH_MIN:
                score -= 1
                components['relative_strength'] = -1
                reasons_short.append(f'stock weak vs BTC-implied move {rel:+.2f}%')
            elif score < 0 and rel >= REL_STRENGTH_MIN:
                score += 1
                components['relative_strength'] = 1
                reasons_short.append(f'stock strong vs BTC-implied move {rel:+.2f}%')

    side = 'LONG' if score > 0 else 'SHORT'
    if side == 'LONG' and stack != 'bull':
        _log_near_signal(ticker, side, 'side_stack_mismatch', score, ind, btc_ctx, components)
        return None
    if side == 'SHORT' and stack != 'bear':
        _log_near_signal(ticker, side, 'side_stack_mismatch', score, ind, btc_ctx, components)
        return None
    rel = btc_ctx.get('stock_minus_btc_implied_60s')
    btc_conflict = (
        (side == 'LONG' and btc_ctx.get('regime') in ('bear', 'bear_momentum') and (rel is None or rel < REL_STRENGTH_MIN))
        or (side == 'SHORT' and btc_ctx.get('regime') in ('bull', 'bull_momentum') and (rel is None or rel > -REL_STRENGTH_MIN))
    )
    if BTC_CONFLICT_REJECT and btc_conflict:
        _log_near_signal(ticker, side, 'btc_conflict', score, ind, btc_ctx, components)
        return None
    if BTC_CHOP_PENALTY_ENABLED and btc_ctx.get('regime_detail') == 'chop' and components.get('momentum'):
        if side == 'LONG':
            score -= 1
            components['btc_chop_penalty'] = -1
        else:
            score += 1
            components['btc_chop_penalty'] = 1
        (reasons_long if side == 'LONG' else reasons_short).append('BTC chop penalty')
    basket = _miner_basket_context(ticker, side, miner_indicators) if MINER_BASKET_ENABLED else {
        'state': 'disabled',
        'score': 0,
    }
    if basket.get('score'):
        signed = int(basket['score']) if side == 'LONG' else -int(basket['score'])
        score += signed
        components['miner_basket'] = signed
        if basket.get('state') == 'confirmed':
            (reasons_long if side == 'LONG' else reasons_short).append(
                f"miner basket confirmed ({basket.get('confirming')}/{basket.get('ready_peers')})"
            )
        elif basket.get('state') == 'opposed':
            (reasons_long if side == 'LONG' else reasons_short).append(
                f"miner basket opposed ({basket.get('opposing')}/{basket.get('ready_peers')})"
            )
    lead_lag = _lead_lag_bias(side, btc_ctx)
    if lead_lag.get('score'):
        score += int(lead_lag['score']) if side == 'LONG' else -int(lead_lag['score'])
        components['btc_lead_lag'] = lead_lag['score']
        if lead_lag['score'] > 0:
            (reasons_long if side == 'LONG' else reasons_short).append(lead_lag['state'])
        else:
            (reasons_long if side == 'LONG' else reasons_short).append(lead_lag['state'])
    if lead_lag.get('score', 0) < 0:
        _log_near_signal(ticker, side, 'btc_chase_penalty', score, ind, btc_ctx, components)
        return None
    side_scores = _side_score_breakdown(components)
    directional_gap = side_scores['long_score'] - side_scores['short_score']
    if SIDE_SCORE_GAP_MIN > 0:
        if side == 'LONG' and directional_gap < SIDE_SCORE_GAP_MIN:
            components['side_score_gap'] = side_scores
            _log_near_signal(ticker, side, 'side_score_gap_too_small', score, ind, btc_ctx, components)
            return None
        if side == 'SHORT' and -directional_gap < SIDE_SCORE_GAP_MIN:
            components['side_score_gap'] = side_scores
            _log_near_signal(ticker, side, 'side_score_gap_too_small', score, ind, btc_ctx, components)
            return None
    components['side_score_gap'] = side_scores
    setup_type = _classify_setup(side, ind, btc_ctx, components)
    flow_fade_confirmed = True
    if setup_type == 'flow_exhaustion_fade' and FLOW_FADE_REQUIRES_EXHAUSTION:
        flow_fade_confirmed = _flow_fade_confirmed(side, ind)
        components['flow_fade_confirmed'] = flow_fade_confirmed
        if not flow_fade_confirmed:
            _log_near_signal(ticker, side, 'flow_fade_unconfirmed', score, ind, btc_ctx, components)
            return None
    range_pos = ind.get('session_range_pos')
    late_extension = (
        range_pos is not None
        and not burst
        and not flow_extreme
        and (
            (side == 'LONG' and range_pos >= RANGE_EXTENSION_REJECT_PCT)
            or (side == 'SHORT' and range_pos <= (1.0 - RANGE_EXTENSION_REJECT_PCT))
        )
    )
    if late_extension:
        _log_near_signal(ticker, side, 'late_session_range_extension', score, ind, btc_ctx, components)
        return None
    abs_s = abs(score)
    min_score = _adjusted_min_score(setup_type, ind)
    if abs_s < min_score:
        _log_near_signal(ticker, side, f'below_setup_min_score:{setup_type}', score, ind, btc_ctx, components)
        return None
    if abs_s >= 7:
        conv = 'HIGH'
    elif abs_s >= 5:
        conv = 'MEDIUM'
    else:
        conv = 'LOW'
    reasons = reasons_long if side == 'LONG' else reasons_short
    btc_stack = (btc_ind or {}).get('ema_stack') if btc_ind and btc_ind.get('ready') else None
    btc_mom = (btc_ind or {}).get('mom_60s') if btc_ind and btc_ind.get('ready') else None

    def _scalarize_new(d):
        out = {}
        for k, v in (d or {}).items():
            if isinstance(v, (int, float, str, bool)) or v is None:
                out[k] = v
            elif isinstance(v, dict):
                out[k] = {kk: vv for kk, vv in v.items()
                          if isinstance(vv, (int, float, str, bool)) or vv is None}
        return out

    return {
        'ticker': ticker,
        'side': side,
        'conviction': conv,
        'score': score,
        'passive_regime_profile': passive_regime_profile(ticker, side, setup_type, ind, btc_ctx, score),
        'price': ind.get('price'),
        'reasons': reasons,
        'btc_stack': btc_stack,
        'btc_mom': btc_mom,
        'setup_type': setup_type,
        'btc_regime': btc_ctx.get('regime'),
        'session_high': ind.get('session_high'),
        'session_low': ind.get('session_low'),
        'session_elapsed_min': round((ind.get('bars') or 0) / 60, 1),
        'btc_context': btc_ctx,
        'miner_basket': basket,
        'session_phase': _session_phase(ind),
        'lead_lag': lead_lag,
        'relative_strength': {
            'btc_beta': btc_ctx.get('beta'),
            'stock_mom_60s': ind.get('mom_60s'),
            'btc_mom_60s': btc_ctx.get('mom_60s'),
            'btc_implied_stock_mom_60s': btc_ctx.get('implied_stock_mom_60s'),
            'stock_minus_btc_implied_60s': btc_ctx.get('stock_minus_btc_implied_60s'),
        },
        'signal_quality': {
            'near_vwap_chop': near_vwap,
            'alive': alive,
            'burst': burst,
            'btc_conflict': btc_conflict,
            'late_session_range_extension': late_extension,
            'flow_fade_confirmed': flow_fade_confirmed,
            'min_score_required': min_score,
            'score_components': components,
            'side_scores': side_scores,
            'score_model': {
                'direction_score': side_scores,
                'regime_score': round(sum(float(v) for k, v in components.items()
                                          if k in ('btc', 'relative_strength', 'btc_lead_lag', 'miner_basket')
                                          and isinstance(v, (int, float))), 3),
                'activity_score': round(sum(float(v) for k, v in components.items()
                                            if k in ('burst_score', 'momentum')
                                            and isinstance(v, (int, float))), 3),
                'execution_score': None,
                'risk_notes': [
                    k for k in ('realized_vol_chop', 'btc_chop_penalty')
                    if k in components
                ],
            },
        },
        'indicators': _scalarize_new(ind),
        'btc_indicators': _scalarize_new(btc_ind) if btc_ind else None,
    }

# helpers
# ────────────────────────────────────────────────────────────────────────
def _alpaca_ts_to_ms(ts: str) -> int:
    """Alpaca WS trade timestamps come as ISO with nanosecond precision."""
    if not ts: return int(time.time() * 1000)
    try:
        from datetime import datetime
        # Strip nanoseconds past microseconds
        if '.' in ts:
            head, tail = ts.split('.')
            tz = ''
            if 'Z' in tail: tail = tail.rstrip('Z'); tz = 'Z'
            elif '+' in tail: tz = tail[tail.index('+'):]; tail = tail[:tail.index('+')]
            tail = tail[:6]  # microseconds
            ts = f'{head}.{tail}{tz}'
        ts = ts.replace('Z', '+00:00')
        return int(datetime.fromisoformat(ts).timestamp() * 1000)
    except Exception:
        return int(time.time() * 1000)


# ────────────────────────────────────────────────────────────────────────
# Module-level singleton
# ────────────────────────────────────────────────────────────────────────
_engine: Optional[ScalpEngine] = None

def get_engine(api_key: Optional[str] = None,
               secret_key: Optional[str] = None) -> ScalpEngine:
    global _engine
    if _engine is None:
        if not api_key or not secret_key:
            raise RuntimeError('First call must pass api_key and secret_key')
        _engine = ScalpEngine(api_key, secret_key)
    return _engine

