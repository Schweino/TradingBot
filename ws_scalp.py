# -*- coding: cp1252 -*-
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

from output_paths import output_path

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

import live_step2_feed
import scoring_profiles
from bracket_rounding import round_exit_brackets

try:
    from zoneinfo import ZoneInfo
except ImportError:  # py<3.9 fallback
    from backports.zoneinfo import ZoneInfo  # type: ignore

# ------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------
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
SCALP_DEFAULT_SL = 0.0045
SCALP_DEFAULT_TP = 0.0035
SCALP_TICKER_BRACKETS = {
    # 2026-05-06: MARA and RIOT ticker-specific brackets promoted from Step 3 replay winners.
    # Keep in sync with mock_trader.TICKER_CFG (informational; actual brackets use those values).
    'CLSK': {'sl': 0.0045, 'tp': 0.0035},
    'MARA': {'sl': 0.0040, 'tp': 0.0030},
    'RIOT': {'sl': 0.0065, 'tp': 0.0018},
}

log = logging.getLogger('scalp')
log.setLevel(logging.INFO)
if not log.handlers:
    h = RotatingFileHandler(output_path('scalp.log'),
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
NEAR_TICK_CAPTURE_ENABLED = bool(SMART_ENTRY.get('near_tick_capture_enabled', True))
NEAR_TICK_CAPTURE_MIN_SCORE = float(SMART_ENTRY.get('near_tick_capture_min_score', 4.0))
NEAR_TICK_CAPTURE_PRE_SEC = int(SMART_ENTRY.get('near_tick_capture_pre_seconds', 60))
NEAR_TICK_CAPTURE_POST_SEC = int(SMART_ENTRY.get('near_tick_capture_post_seconds', 180))
NEAR_SIGNAL_DIR = output_path('postmortem', 'near_signals')
GATE_TIMELINE_DIR = output_path('postmortem', 'gate_timeline')
NO_SIGNAL_SNAPSHOT_DIR = output_path('postmortem', 'no_signal_snapshots')
RUNTIME_EVENT_DIR = output_path('postmortem', 'runtime')
GATE_TIMELINE_INTERVAL_SEC = float(SMART_ENTRY.get('gate_timeline_interval_sec', 15))
NO_SIGNAL_SNAPSHOT_INTERVAL_SEC = float(SMART_ENTRY.get('no_signal_snapshot_interval_sec', 30))
SIGNAL_LOOP_STALE_SEC = float(SMART_ENTRY.get('signal_loop_stale_sec', 20))
SIGNAL_LOOP_WATCHDOG_SEC = float(SMART_ENTRY.get('signal_loop_watchdog_sec', 5))
DUAL_SIDE_SHADOW_ENABLED = bool(SMART_ENTRY.get('dual_side_shadow_enabled', True))
ACTIVE_SCORING_PROFILE = SMART_ENTRY.get('active_scoring_profile') or {}
ACTIVE_SCORING_ROUTER = SMART_ENTRY.get('active_scoring_router') or {}
LONG_ENTRY_QUALITY_GATE = SMART_ENTRY.get('long_entry_quality_gate') or {}
SHORT_RECOVERY_GUARD = SMART_ENTRY.get('short_recovery_guard') or {}
ROLLING_SPREAD_WINDOW_SEC = int(SMART_ENTRY.get('rolling_spread_window_sec', 300))
ROLLING_SPREAD_ABNORMAL_MULTIPLE = float(SMART_ENTRY.get('rolling_spread_abnormal_multiple', 2.0))
QUOTE_LOCKED_CROSSED_PENALTY = int(SMART_ENTRY.get('quote_locked_crossed_penalty', 25))
CONDITION_METADATA_PENALTY = int(SMART_ENTRY.get('condition_metadata_penalty', 10))
_near_signal_lock = threading.Lock()
_last_near_signal_log: Dict[str, float] = {}
_near_signal_capture_cb = None


def _active_scoring_router_profile() -> Optional[dict]:
    if not isinstance(ACTIVE_SCORING_ROUTER, dict):
        return None
    if not ACTIVE_SCORING_ROUTER.get('enabled'):
        return None
    fallback = ACTIVE_SCORING_PROFILE if isinstance(ACTIVE_SCORING_PROFILE, dict) else {}
    weights = ACTIVE_SCORING_ROUTER.get('weights') or fallback.get('weights') or {}
    routes = ACTIVE_SCORING_ROUTER.get('routes') or []
    if not isinstance(weights, dict) or not isinstance(routes, list) or not routes:
        return None
    return {
        'name': ACTIVE_SCORING_ROUTER.get('name') or 'active_scoring_router',
        'bias': ACTIVE_SCORING_ROUTER.get('bias', fallback.get('bias', 0.0)),
        'weights': weights,
        'routes': routes,
        'routed_scoring_profile': True,
        'route_safety': ACTIVE_SCORING_ROUTER.get('route_safety') or {},
    }


def _active_scoring_profile() -> Optional[dict]:
    router = _active_scoring_router_profile()
    if router:
        return router
    if not isinstance(ACTIVE_SCORING_PROFILE, dict):
        return None
    if not ACTIVE_SCORING_PROFILE.get('enabled'):
        return None
    profile = {
        'name': ACTIVE_SCORING_PROFILE.get('name') or 'active_scoring_profile',
        'bias': ACTIVE_SCORING_PROFILE.get('bias', 0.0),
        'weights': ACTIVE_SCORING_PROFILE.get('weights') or {},
    }
    return profile if profile['weights'] else None


def _long_entry_quality_gate_reason(sig: dict, ind: dict) -> Optional[str]:
    if not isinstance(LONG_ENTRY_QUALITY_GATE, dict):
        return None
    if not LONG_ENTRY_QUALITY_GATE.get('enabled'):
        return None
    if sig.get('side') != 'LONG':
        return None
    ticker_return = _float_or_none(ind.get('session_return_pct'))
    vwap_dist = _float_or_none(ind.get('vwap_dist'))
    btc_regime = sig.get('btc_regime') or ((sig.get('btc_context') or {}).get('regime'))
    threshold = float(LONG_ENTRY_QUALITY_GATE.get('ticker_session_return_below_pct', -0.5))
    bullish_regimes = set(LONG_ENTRY_QUALITY_GATE.get('btc_bullish_regimes') or [
        'bull',
        'bull_momentum',
        'impulse_up',
    ])
    if ticker_return is None or vwap_dist is None:
        return None
    if ticker_return < threshold and vwap_dist < 0 and btc_regime not in bullish_regimes:
        return (
            f"long_entry_quality_gate:ticker_return={ticker_return:.3f}%"
            f"<{threshold:.3f}% below_vwap btc_regime={btc_regime or 'unknown'}"
        )
    return None


def _short_recovery_guard_reason(sig: dict, ind: dict, btc_ind: Optional[dict]) -> Optional[str]:
    if not isinstance(SHORT_RECOVERY_GUARD, dict):
        return None
    if not SHORT_RECOVERY_GUARD.get('enabled'):
        return None
    if sig.get('side') != 'SHORT':
        return None

    dual = sig.get('shadow_dual_side_score') or (
        ((sig.get('signal_quality') or {}).get('score_model') or {}).get('dual_side_shadow')
    ) or {}
    shadow_gap_min = float(SHORT_RECOVERY_GUARD.get('shadow_long_gap_min', 6.0))
    shadow_long = (
        dual.get('enabled')
        and dual.get('chosen_side_by_shadow') == 'LONG'
        and float(dual.get('side_gap') or 0.0) >= shadow_gap_min
    )

    price = _float_or_none(ind.get('price'))
    ema15 = _float_or_none(ind.get('ema_15s'))
    ema60 = _float_or_none(ind.get('ema_60s'))
    m15 = _float_or_none(ind.get('mom_15s'))
    m60 = _float_or_none(ind.get('mom_60s'))
    vwap_dist = _float_or_none(ind.get('vwap_dist'))
    range_pos = _float_or_none(ind.get('session_range_pos'))
    stack = ind.get('ema_stack')
    btc_ctx = sig.get('btc_context') or {}
    btc_regime = btc_ctx.get('regime')
    btc_stack = btc_ctx.get('stack') or ((btc_ind or {}).get('ema_stack') if btc_ind else None)
    btc_m15 = _float_or_none(btc_ctx.get('mom_15s'))
    btc_m60 = _float_or_none(btc_ctx.get('mom_60s'))

    bullish_flags = []
    if stack == 'bull':
        bullish_flags.append('ema_stack_bull')
    if price is not None and ema15 is not None and ema60 is not None and price > ema15 and price > ema60:
        bullish_flags.append('price_above_ema15_60')
    if m15 is not None and m60 is not None and m15 > 0 and m60 >= 0:
        bullish_flags.append('positive_15s_60s_momentum')
    if vwap_dist is not None and vwap_dist > 0:
        bullish_flags.append('above_vwap')
    if range_pos is not None and range_pos >= float(SHORT_RECOVERY_GUARD.get('min_session_range_pos', 0.25)):
        bullish_flags.append('off_session_lows')
    if btc_regime in ('bull', 'bull_momentum', 'impulse_up') or (
            btc_stack == 'bull' and (btc_m15 is None or btc_m15 >= 0) and (btc_m60 is None or btc_m60 >= 0)):
        bullish_flags.append('btc_not_bearish')
    if shadow_long:
        bullish_flags.append('shadow_prefers_long')

    btc_bearish = (
        btc_regime in ('bear', 'bear_momentum', 'impulse_down')
        or (btc_stack == 'bear' and (btc_m15 is None or btc_m15 <= 0) and (btc_m60 is None or btc_m60 <= 0))
    )
    if SHORT_RECOVERY_GUARD.get('require_btc_not_bearish', True) and btc_bearish and not shadow_long:
        return None

    min_flags = int(SHORT_RECOVERY_GUARD.get('min_bullish_flags', 4))
    if shadow_long or len(bullish_flags) >= min_flags:
        return (
            'short_recovery_guard:'
            f"flags={len(bullish_flags)}[{','.join(bullish_flags[:6])}]"
            f" shadow_gap={float(dual.get('side_gap') or 0.0):.3f}"
            f" btc_regime={btc_regime or 'unknown'}"
        )
    return None


def _apply_active_scoring_profile(sig: dict, ind: dict, btc_ind: Optional[dict]) -> Optional[dict]:
    profile = _active_scoring_profile()
    final_sig = scoring_profiles.apply_profile(profile, sig, ind, btc_ind, include_details=True) if profile else sig
    if final_sig.get('scoring_profile_rejected'):
        result = final_sig.get('scoring_profile') or {}
        route = result.get('selected_route') or {}
        _log_near_signal(
            final_sig.get('ticker') or '',
            final_sig.get('side'),
            f"scoring_profile_route_skip:{route.get('route') or 'unknown'}",
            final_sig.get('score'),
            ind,
            final_sig.get('btc_context') or {},
            (final_sig.get('signal_quality') or {}).get('score_components') or {},
        )
        return None
    short_guard_reason = _short_recovery_guard_reason(final_sig, ind, btc_ind)
    if short_guard_reason:
        _log_near_signal(
            final_sig.get('ticker') or '',
            final_sig.get('side'),
            short_guard_reason,
            final_sig.get('score'),
            ind,
            final_sig.get('btc_context') or {},
            (final_sig.get('signal_quality') or {}).get('score_components') or {},
        )
        return None
    gate_reason = _long_entry_quality_gate_reason(final_sig, ind)
    if gate_reason:
        _log_near_signal(
            final_sig.get('ticker') or '',
            final_sig.get('side'),
            gate_reason,
            final_sig.get('score'),
            ind,
            final_sig.get('btc_context') or {},
            (final_sig.get('signal_quality') or {}).get('score_components') or {},
        )
        return None
    return final_sig


# ------------------------------------------------------------------------
# Per-symbol live state
# ------------------------------------------------------------------------
@dataclass
class SymbolState:
    symbol: str
    last_trade_price: Optional[float] = None
    last_trade_size: int = 0
    last_trade_ts_ms: int = 0
    last_trade_exchange: Optional[str] = None
    last_trade_conditions: List[str] = field(default_factory=list)
    last_trade_tape: Optional[str] = None
    last_trade_id: Optional[str] = None
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    bid_size: Optional[int] = None
    ask_size: Optional[int] = None
    last_quote_ts_ms: int = 0
    last_quote_bid_exchange: Optional[str] = None
    last_quote_ask_exchange: Optional[str] = None
    last_quote_conditions: List[str] = field(default_factory=list)
    last_quote_tape: Optional[str] = None
    quote_history: Deque = field(default_factory=lambda: deque(maxlen=300))
    # Trade ring: each entry is (ts_ms, price, size, side, meta) where side is {+1, -1, 0}.
    # Older in-memory/test rows may still be four-tuples; helpers below accept both.
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


def _clean_codes(value) -> List[str]:
    if value in (None, '', []):
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(v) for v in value if v not in (None, '')]
    return [str(value)]


def _compact_meta(meta: Optional[dict]) -> dict:
    out = {}
    for k, v in (meta or {}).items():
        if isinstance(v, (int, float, str, bool)) or v is None:
            out[k] = v
        elif isinstance(v, (list, tuple, set)):
            out[k] = [str(x) for x in v if x not in (None, '')]
    return out


def _median(vals: List[float]) -> Optional[float]:
    vals = sorted(float(v) for v in vals if v is not None)
    if not vals:
        return None
    mid = len(vals) // 2
    if len(vals) % 2:
        return vals[mid]
    return (vals[mid - 1] + vals[mid]) / 2


def _percentile(vals: List[float], pct: float) -> Optional[float]:
    vals = sorted(float(v) for v in vals if v is not None)
    if not vals:
        return None
    idx = int(round((len(vals) - 1) * max(0.0, min(100.0, pct)) / 100.0))
    return vals[idx]


def _float_or_none(value) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _quote_state(best_bid, best_ask, price, quote_age_sec=None, spread_pct=None) -> dict:
    state = 'unknown'
    tags = []
    if best_bid is None and best_ask is None:
        state = 'missing'
        tags.append('missing_quote')
    elif best_bid is None or best_ask is None:
        state = 'one_sided'
        tags.append('one_sided_quote')
    else:
        try:
            bid = float(best_bid)
            ask = float(best_ask)
            if bid > ask:
                state = 'crossed'
                tags.append('crossed_quote')
            elif bid == ask:
                state = 'locked'
                tags.append('locked_quote')
            elif bid <= 0 or ask <= 0:
                state = 'invalid'
                tags.append('invalid_quote')
            else:
                state = 'normal'
        except Exception:
            state = 'invalid'
            tags.append('invalid_quote')
    if quote_age_sec is None:
        tags.append('quote_age_unknown')
    else:
        try:
            if float(quote_age_sec) > 3:
                tags.append('quote_stale')
        except Exception:
            tags.append('quote_age_invalid')
    if spread_pct is not None:
        try:
            if float(spread_pct) > 0.15:
                tags.append('spread_wide')
            elif float(spread_pct) > 0.08:
                tags.append('spread_elevated')
        except Exception:
            pass
    score = 100
    if state in ('crossed', 'locked'):
        score -= QUOTE_LOCKED_CROSSED_PENALTY
    elif state in ('missing', 'one_sided', 'invalid'):
        score -= 20
    if 'quote_stale' in tags:
        score -= 20
    if 'spread_wide' in tags:
        score -= 25
    elif 'spread_elevated' in tags:
        score -= 12
    return {
        'state': state,
        'tags': tags,
        'score': max(0, min(100, score)),
        'bid': best_bid,
        'ask': best_ask,
        'price_ref': price,
    }


def _condition_quality(trade_conditions=None, quote_conditions=None,
                       trade_exchange=None, quote_bid_exchange=None,
                       quote_ask_exchange=None) -> dict:
    tconds = _clean_codes(trade_conditions)
    qconds = _clean_codes(quote_conditions)
    tags = []
    if tconds:
        tags.append('trade_conditions_present')
    if qconds:
        tags.append('quote_conditions_present')
    odd = sorted({c for c in tconds + qconds if c in ('B', 'W', 'Z', '4', '7', '9')})
    if odd:
        tags.append('odd_condition_codes')
    if not trade_exchange:
        tags.append('trade_exchange_missing')
    if not quote_bid_exchange or not quote_ask_exchange:
        tags.append('quote_exchange_missing')
    score = 100
    if tconds or qconds:
        score -= CONDITION_METADATA_PENALTY
    if odd:
        score -= 35
    if 'trade_exchange_missing' in tags:
        score -= 5
    if 'quote_exchange_missing' in tags:
        score -= 5
    return {
        'score': max(0, min(100, score)),
        'tags': tags,
        'trade_conditions': tconds,
        'quote_conditions': qconds,
        'trade_exchange': trade_exchange,
        'quote_bid_exchange': quote_bid_exchange,
        'quote_ask_exchange': quote_ask_exchange,
        'metadata_present': bool(tconds or qconds or trade_exchange or quote_bid_exchange or quote_ask_exchange),
    }


def _trade_parts(row):
    ts, price, size, side = row[:4]
    meta = row[4] if len(row) > 4 and isinstance(row[4], dict) else {}
    return ts, price, size, side, _compact_meta(meta)


def _quote_parts(row):
    ts, bid, ask, bid_size, ask_size, imbalance = row[:6]
    meta = row[6] if len(row) > 6 and isinstance(row[6], dict) else {}
    return ts, bid, ask, bid_size, ask_size, imbalance, _compact_meta(meta)


def _trade_event_dict(symbol: str, phase: str, row) -> dict:
    ts, price, size, side, meta = _trade_parts(row)
    out = {
        'schema_version': 2,
        'phase': phase,
        'ts_ms': ts,
        'symbol': symbol,
        'event': 'trade',
        'price': price,
        'size': size,
        'side': side,
    }
    out.update(meta)
    return out


def _quote_event_dict(symbol: str, phase: str, row) -> dict:
    ts, bid, ask, bid_size, ask_size, imbalance, meta = _quote_parts(row)
    out = {
        'schema_version': 2,
        'phase': phase,
        'ts_ms': ts,
        'symbol': symbol,
        'event': 'quote',
        'bid': bid,
        'ask': ask,
        'bid_size': bid_size,
        'ask_size': ask_size,
        'imbalance': imbalance,
    }
    out.update(meta)
    return out


# ------------------------------------------------------------------------
# Engine
# ------------------------------------------------------------------------
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

        # Signal engine
        self.signals: Deque = deque(maxlen=200)  # recent signal log
        self._last_signal_ts: Dict[str, float] = {}  # for cooldown
        self._setup_states: Dict[str, dict] = {}
        self._on_signal_cbs: List[Callable] = []
        self._cb_lock = threading.Lock()
        self._agg_lock = threading.Lock()
        self._agg_thread: Optional[threading.Thread] = None
        self._watchdog_thread: Optional[threading.Thread] = None
        self._last_signal_loop_at: float = 0.0
        self._last_signal_loop_ok_at: float = 0.0
        self._last_signal_loop_error_at: float = 0.0
        self._signal_loop_errors: Deque = deque(maxlen=20)
        self._agg_restart_count = 0
        self._agg_generation = 0

        # Tick capture: ring-buffer flush on signal + post-entry tail
        self._tick_captures: Dict[str, dict] = {}
        self._captures_lock = threading.Lock()
        self._near_tick_capture_seen: Dict[str, float] = {}
        self._last_gate_timeline_log: Dict[str, float] = {}
        self._last_no_signal_snapshot_log: Dict[str, float] = {}
        global _near_signal_capture_cb
        _near_signal_capture_cb = self._maybe_capture_near_signal_ticks
        live_step2_feed.start_materializer_loop()
        self._start_aggregator_thread(reason='init')
        self._watchdog_thread = threading.Thread(
            target=self._signal_loop_watchdog, daemon=True, name='scalp-signal-watchdog')
        self._watchdog_thread.start()

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
            'signal_loop_health': self._signal_loop_health(),
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

    def _start_aggregator_thread(self, reason: str, force: bool = False):
        with self._agg_lock:
            if not force and self._agg_thread and self._agg_thread.is_alive():
                return
            self._last_signal_loop_at = time.time()
            self._agg_restart_count += 1
            self._agg_generation += 1
            generation = self._agg_generation
            self._agg_thread = threading.Thread(
                target=self._aggregator_loop, args=(generation,),
                daemon=True, name=f'scalp-agg-{generation}')
            self._agg_thread.start()
        log.warning(f'signal aggregator thread started reason={reason} restart_count={self._agg_restart_count}')
        self._append_runtime_event('signal_loop_started', {
            'reason': reason,
            'restart_count': self._agg_restart_count,
        })

    def _signal_loop_health(self) -> dict:
        now = time.time()
        thread_alive = bool(self._agg_thread and self._agg_thread.is_alive())
        age = None if not self._last_signal_loop_at else round(now - self._last_signal_loop_at, 3)
        ok_age = None if not self._last_signal_loop_ok_at else round(now - self._last_signal_loop_ok_at, 3)
        stale = (not thread_alive) or (age is not None and age > SIGNAL_LOOP_STALE_SEC)
        return {
            'status': 'stale' if stale else 'ok',
            'thread_alive': thread_alive,
            'last_loop_age_sec': age,
            'last_ok_age_sec': ok_age,
            'last_error_age_sec': None if not self._last_signal_loop_error_at else round(now - self._last_signal_loop_error_at, 3),
            'restart_count': self._agg_restart_count,
            'recent_errors': list(self._signal_loop_errors)[-5:],
        }

    def _append_runtime_event(self, kind: str, payload: Optional[dict] = None):
        row = {
            'ts': int(time.time()),
            'created_at_ct': datetime.now(ZoneInfo('America/Chicago')).isoformat(),
            'kind': kind,
            'payload': payload or {},
        }
        try:
            self._append_postmortem_jsonl(RUNTIME_EVENT_DIR, 'runtime_events', row)
        except Exception as e:
            log.warning(f'runtime event write failed: {e}')

    def _signal_loop_watchdog(self):
        while not self._shutdown.is_set():
            time.sleep(max(1.0, SIGNAL_LOOP_WATCHDOG_SEC))
            now = time.time()
            thread_dead = not (self._agg_thread and self._agg_thread.is_alive())
            stale = self._last_signal_loop_at and now - self._last_signal_loop_at > SIGNAL_LOOP_STALE_SEC
            if thread_dead or stale:
                reason = 'thread_dead' if thread_dead else 'stale'
                age = None if not self._last_signal_loop_at else round(now - self._last_signal_loop_at, 3)
                log.error(f'signal loop watchdog restart reason={reason} last_loop_age_sec={age}')
                self._append_runtime_event('signal_loop_watchdog_restart', {
                    'reason': reason,
                    'last_loop_age_sec': age,
                    'thread_alive': not thread_dead,
                })
                self._start_aggregator_thread(reason=f'watchdog_{reason}', force=stale and not thread_dead)

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
    def _maybe_capture_near_signal_ticks(self, row: dict):
        if not NEAR_TICK_CAPTURE_ENABLED:
            return
        ticker = row.get('ticker')
        side = row.get('side')
        if not ticker or side not in ('LONG', 'SHORT'):
            return
        try:
            if abs(float(row.get('score') or 0)) < NEAR_TICK_CAPTURE_MIN_SCORE:
                return
        except Exception:
            return
        reason = str(row.get('reason') or 'near_signal')
        safe_reason = ''.join(ch if ch.isalnum() or ch in ('-', '_') else '_' for ch in reason)[:40]
        key = f'{ticker}:{side}:{safe_reason}'
        now = time.time()
        if now - float(self._near_tick_capture_seen.get(key, 0) or 0) < 60:
            return
        self._near_tick_capture_seen[key] = now
        signal = {
            'capture_type': 'near_signal',
            'ticker': ticker,
            'side': side,
            'price': row.get('price'),
            'score': row.get('score'),
            'setup_type': row.get('setup_type'),
            'near_signal_reason': reason,
            'components': row.get('components') or {},
            'indicators': row.get('indicators') or {},
            'btc_context': row.get('btc_context') or {},
        }
        self.start_tick_capture(
            ticker,
            label=f'NEAR_{side}_{safe_reason}',
            signal=signal,
            capture_id=f'near-{ticker}-{side}-{safe_reason}-{int(now * 1000)}',
            pre_seconds=NEAR_TICK_CAPTURE_PRE_SEC,
            post_seconds=NEAR_TICK_CAPTURE_POST_SEC,
        )

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
            pre_ticks = [row for row in st.trades if row[0] >= pre_cutoff_ms]
            pre_quotes = [row for row in st.quote_history if row[0] >= pre_cutoff_ms]

        btc_pre_ticks = []
        btc_pre_quotes = []
        peer_pre_ticks = {}
        peer_pre_quotes = {}
        btc_state = self.states.get(BTC_SYMBOL)
        if btc_state:
            with btc_state.lock:
                btc_pre_ticks = [row for row in btc_state.trades if row[0] >= pre_cutoff_ms]
                btc_pre_quotes = [row for row in btc_state.quote_history if row[0] >= pre_cutoff_ms]
        peer_symbols = [
            sym for sym in self.get_all_subscribed()
            if sym != ticker and sym in self.states
        ]
        for peer in peer_symbols:
            pst = self.states.get(peer)
            if not pst:
                continue
            with pst.lock:
                peer_pre_ticks[peer] = [row for row in pst.trades if row[0] >= pre_cutoff_ms]
                peer_pre_quotes[peer] = [row for row in pst.quote_history if row[0] >= pre_cutoff_ms]

        date_str = datetime.now(ET).strftime('%Y-%m-%d')
        ts_str   = datetime.now(ET).strftime('%H%M%S')
        out_dir  = output_path('tick_logs', date_str)
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
                'symbols':    {ticker, BTC_SYMBOL, *peer_symbols},
                'started_ms': now_ms,
                'signal':     signal or {},
                'pre_ticks':  pre_ticks,
                'pre_quotes': pre_quotes,
                'btc_pre_ticks': btc_pre_ticks,
                'btc_pre_quotes': btc_pre_quotes,
                'peer_pre_ticks': peer_pre_ticks,
                'peer_pre_quotes': peer_pre_quotes,
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
                    'peer_symbols': sorted((cap.get('symbols') or set()) - {cap.get('ticker'), BTC_SYMBOL}),
                    'peer_pre_counts': {
                        sym: len(rows) for sym, rows in (cap.get('peer_pre_ticks') or {}).items()
                    },
                }) + '\n')
                for row in cap['pre_ticks']:
                    f.write(json.dumps(_trade_event_dict(cap.get('ticker'), 'pre', row)) + '\n')
                for row in cap.get('pre_quotes', []):
                    f.write(json.dumps(_quote_event_dict(cap.get('ticker'), 'pre', row)) + '\n')
                for row in cap.get('btc_pre_ticks', []):
                    f.write(json.dumps(_trade_event_dict(BTC_SYMBOL, 'pre', row)) + '\n')
                for row in cap.get('btc_pre_quotes', []):
                    f.write(json.dumps(_quote_event_dict(BTC_SYMBOL, 'pre', row)) + '\n')
                for sym, rows in (cap.get('peer_pre_ticks') or {}).items():
                    for row in rows:
                        f.write(json.dumps(_trade_event_dict(sym, 'pre', row)) + '\n')
                for sym, rows in (cap.get('peer_pre_quotes') or {}).items():
                    for row in rows:
                        f.write(json.dumps(_quote_event_dict(sym, 'pre', row)) + '\n')
                for row in cap['post_ticks']:
                    f.write(json.dumps(_trade_event_dict(cap.get('ticker'), 'post', row)) + '\n')
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
            # Stock post-entry trades are already written through post_ticks.
            # Keep post_events for quotes and cross-symbol BTC tape so captures
            # stay compact without losing chronological context.
            if (sym == cap.get('ticker') and row.get('event') == 'trade'
                    and not row.get('skipped_by_engine')):
                continue
            cap.setdefault('post_events', []).append(row)
            if sym == cap.get('ticker') and row.get('event') == 'quote':
                cap.setdefault('post_quotes', []).append(
                    (row.get('ts_ms'), row.get('bid'), row.get('ask'),
                     row.get('bid_size'), row.get('ask_size'), row.get('imbalance'), {
                         'bid_exchange': row.get('bid_exchange'),
                         'ask_exchange': row.get('ask_exchange'),
                         'quote_conditions': row.get('quote_conditions') or [],
                         'quote_tape': row.get('quote_tape'),
                     })
                )
            elif sym == BTC_SYMBOL and row.get('event') == 'trade':
                cap.setdefault('btc_post_ticks', []).append(
                    (row.get('ts_ms'), row.get('price'), row.get('size'), row.get('side'), {
                        'trade_exchange': row.get('trade_exchange'),
                        'trade_conditions': row.get('trade_conditions') or [],
                        'trade_tape': row.get('trade_tape'),
                        'trade_id': row.get('trade_id'),
                    })
                )
            elif sym == BTC_SYMBOL and row.get('event') == 'quote':
                cap.setdefault('btc_post_quotes', []).append(
                    (row.get('ts_ms'), row.get('bid'), row.get('ask'),
                     row.get('bid_size'), row.get('ask_size'), row.get('imbalance'), {
                         'bid_exchange': row.get('bid_exchange'),
                         'ask_exchange': row.get('ask_exchange'),
                         'quote_conditions': row.get('quote_conditions') or [],
                         'quote_tape': row.get('quote_tape'),
                     })
                )

    def _handle_trade(self, m):
        sym = m.get('S')
        if not sym or sym not in self.states:
            return
        st = self.states[sym]
        price = float(m.get('p', 0))
        size = int(m.get('s', 0))
        ts_ms = _alpaca_ts_to_ms(m.get('t', ''))
        if sym != BTC_SYMBOL:
            live_step2_feed.record_stock_trade(sym, m, ts_ms)
        meta = {
            'trade_exchange': m.get('x'),
            'trade_conditions': _clean_codes(m.get('c') or []),
            'trade_tape': m.get('z'),
            'trade_id': m.get('i'),
        }
        # Skip invalid/odd-lot or trades with problematic conditions
        conds = meta['trade_conditions']
        # Alpaca condition codes to skip: 'B' (out of sequence), 'W' (corrected), 'Z' (sold)
        if any(c in ('B', 'W', 'Z') for c in conds):
            self._append_capture_event(sym, {
                'event': 'trade', 'ts_ms': ts_ms,
                'price': price, 'size': size, 'side': 0,
                'skipped_by_engine': 'condition_filter',
                **meta,
            })
            return
        with st.lock:
            side = st.classify_side(price)
            st.trades.append((ts_ms, price, size, side, meta))
            st.pending_trades.append((ts_ms, price, size, side, meta))
            st.last_trade_price = price
            st.last_trade_size = size
            st.last_trade_ts_ms = ts_ms
            st.last_trade_exchange = meta.get('trade_exchange')
            st.last_trade_conditions = meta.get('trade_conditions') or []
            st.last_trade_tape = meta.get('trade_tape')
            st.last_trade_id = meta.get('trade_id')
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
            cap['post_ticks'].append((ts_ms, price, size, side, meta))
        self._append_capture_event(sym, {
            'event': 'trade', 'ts_ms': ts_ms,
            'price': price, 'size': size, 'side': side,
            **meta,
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
            quote_meta = {
                'bid_exchange': m.get('bx'),
                'ask_exchange': m.get('ax'),
                'quote_conditions': _clean_codes(m.get('c') or []),
                'quote_tape': m.get('z'),
            }
            if bp is not None: st.best_bid = float(bp)
            if ap is not None: st.best_ask = float(ap)
            if bs is not None: st.bid_size = int(bs)
            if a_s is not None: st.ask_size = int(a_s)
            quote_ts = _alpaca_ts_to_ms(m.get('t', '')) if m.get('t') else int(time.time() * 1000)
            if sym != BTC_SYMBOL:
                live_step2_feed.record_stock_quote(sym, m, quote_ts)
            st.last_quote_ts_ms = quote_ts
            st.last_quote_bid_exchange = quote_meta.get('bid_exchange')
            st.last_quote_ask_exchange = quote_meta.get('ask_exchange')
            st.last_quote_conditions = quote_meta.get('quote_conditions') or []
            st.last_quote_tape = quote_meta.get('quote_tape')
            bid_sz = st.bid_size or 0
            ask_sz = st.ask_size or 0
            tot_sz = bid_sz + ask_sz
            imb = round((bid_sz - ask_sz) / tot_sz, 3) if tot_sz > 0 else None
            st.quote_history.append((quote_ts, st.best_bid, st.best_ask, bid_sz, ask_sz, imb, quote_meta))
            if sym == BTC_SYMBOL and st.best_bid is not None and st.best_ask is not None:
                mid = round((st.best_bid + st.best_ask) / 2, 6)
                side = 0
                if st.last_trade_price is not None:
                    if mid > st.last_trade_price:
                        side = +1
                    elif mid < st.last_trade_price:
                        side = -1
                synth_meta = {
                    'trade_exchange': 'crypto_quote_mid',
                    'trade_conditions': ['synthetic_mid_from_quote'],
                    'trade_tape': quote_meta.get('quote_tape'),
                    'bid_exchange': quote_meta.get('bid_exchange'),
                    'ask_exchange': quote_meta.get('ask_exchange'),
                    'quote_conditions': quote_meta.get('quote_conditions') or [],
                }
                st.trades.append((quote_ts, mid, 0, side, synth_meta))
                st.pending_trades.append((quote_ts, mid, 0, side, synth_meta))
                st.last_trade_price = mid
                st.last_trade_size = 0
                st.last_trade_ts_ms = quote_ts
                st.last_trade_exchange = synth_meta.get('trade_exchange')
                st.last_trade_conditions = synth_meta.get('trade_conditions') or []
                st.last_trade_tape = synth_meta.get('trade_tape')
                live_step2_feed.record_btc_synth(sym, mid, quote_ts)
            event = {
                'event': 'quote', 'ts_ms': quote_ts,
                'bid': st.best_bid, 'ask': st.best_ask,
                'bid_size': bid_sz, 'ask_size': ask_sz,
                'imbalance': imb,
                **quote_meta,
            }
        self._append_capture_event(sym, event)

    # --------------- aggregator ---------------
    def _aggregator_loop(self, generation: int):
        """Every second, roll trades in the past 1s window into a 1s bar per symbol."""
        while not self._shutdown.is_set():
            if generation != self._agg_generation:
                log.warning(f'signal aggregator generation exiting old={generation} current={self._agg_generation}')
                return
            try:
                time.sleep(1.0)
                self._run_aggregator_iteration()
                self._last_signal_loop_ok_at = time.time()
            except Exception as e:
                now = time.time()
                self._last_signal_loop_error_at = now
                err = {
                    'ts': int(now),
                    'error': repr(e),
                }
                self._signal_loop_errors.append(err)
                log.exception(f'signal aggregator iteration failed: {e}')
                self._append_runtime_event('signal_loop_iteration_failed', err)
                time.sleep(1.0)

    def _run_aggregator_iteration(self):
        self._last_signal_loop_at = time.time()
        now_ms = int(time.time() * 1000)
        sec_start_ms = (now_ms // 1000 - 1) * 1000  # bar for the previous second
        sec_end_ms = sec_start_ms + 1000
        for sym, st in list(self.states.items()):
            try:
                self._roll_symbol_1s_bar(sym, st, sec_start_ms, sec_end_ms)
            except Exception as e:
                now = time.time()
                err = {'ts': int(now), 'symbol': sym, 'error': repr(e)}
                self._last_signal_loop_error_at = now
                self._signal_loop_errors.append(err)
                log.exception(f'signal aggregator symbol roll failed sym={sym}: {e}')
                self._append_runtime_event('signal_loop_symbol_failed', err)
        self._run_signal_engine()
        self._flush_expired_tick_captures()

    def _roll_symbol_1s_bar(self, sym: str, st: SymbolState, sec_start_ms: int, sec_end_ms: int):
        with st.lock:
            o = h = l = c = None
            v = buy_v = sell_v = n = 0
            while st.pending_trades and st.pending_trades[0][0] < sec_end_ms:
                ts_ms, price, size, side, _meta = _trade_parts(st.pending_trades.popleft())
                if ts_ms < sec_start_ms:
                    continue
                if o is None:
                    o = h = l = price
                h = max(h, price)
                l = min(l, price)
                c = price
                v += size
                n += 1
                if side > 0:
                    buy_v += size
                elif side < 0:
                    sell_v += size
            if o is None:
                # No trades this second — carry-forward close from last bar.
                if st.bars_1s:
                    prev = st.bars_1s[-1]
                    o = h = l = c = prev['c']
                else:
                    return
            st.bars_1s.append({
                'ts_s': sec_start_ms // 1000,
                'o': o, 'h': h, 'l': l, 'c': c,
                'v': v, 'buy_v': buy_v, 'sell_v': sell_v, 'n': n,
            })
            # Emit a flow-history bucket at each minute boundary.
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

    def _flush_expired_tick_captures(self):
        now_cap = int(time.time() * 1000)
        with self._captures_lock:
            expired = {key: self._tick_captures.pop(key)
                       for key in list(self._tick_captures)
                       if now_cap > self._tick_captures[key]['end_ms'] + 2000}
        for cap in expired.values():
            try:
                self._flush_capture(cap)
            except Exception as e:
                now = time.time()
                err = {'ts': int(now), 'error': repr(e), 'capture': cap.get('capture_type')}
                self._last_signal_loop_error_at = now
                self._signal_loop_errors.append(err)
                log.exception(f'tick capture flush failed: {e}')
                self._append_runtime_event('signal_loop_capture_flush_failed', err)

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
                self._maybe_log_gate_timeline(
                    tkr, ind, btc_ind, stock_indicators, None,
                    'no_signal', 'detect_signal_none',
                )
                self._maybe_log_no_signal_snapshot(
                    tkr, ind, btc_ind, stock_indicators, 'detect_signal_none',
                )
                continue
            if not self._setup_state_gate(tkr, sig, ind):
                self._maybe_log_gate_timeline(
                    tkr, ind, btc_ind, stock_indicators, sig,
                    'blocked', f'setup_state_pending:{sig.get("setup_type")}',
                )
                continue
            key = f"{tkr}:{sig['side']}:{sig.get('setup_type', 'unknown')}"
            last = self._last_signal_ts.get(key, 0)
            cooldown = float(SETUP_COOLDOWN_SEC.get(sig.get('setup_type'), 30))
            if time.time() - last < cooldown:
                self._maybe_log_gate_timeline(
                    tkr, ind, btc_ind, stock_indicators, sig,
                    'blocked', f'cooldown:{sig.get("setup_type")}',
                )
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
                    sl_raw = round(_price * (1 - brk['sl']), 4)
                    tp_raw = round(_price * (1 + brk['tp']), 4)
                else:
                    sl_raw = round(_price * (1 + brk['sl']), 4)
                    tp_raw = round(_price * (1 - brk['tp']), 4)
                sl_rounded, tp_rounded = round_exit_brackets(sig['side'], sl_raw, tp_raw)
                sig['sl_price'] = sl_rounded if sl_rounded is not None else sl_raw
                sig['tp_price'] = tp_rounded if tp_rounded is not None else tp_raw
                sig['sl_pct'] = brk['sl']
                sig['tp_pct'] = brk['tp']
            with st.lock:
                sig['best_bid'] = st.best_bid
                sig['best_ask'] = st.best_ask
                sig['bid_size'] = st.bid_size
                sig['ask_size'] = st.ask_size
                sig['quote_ts'] = st.last_quote_ts_ms
                sig['last_trade_exchange'] = st.last_trade_exchange
                sig['last_trade_conditions'] = list(st.last_trade_conditions or [])
                sig['last_trade_tape'] = st.last_trade_tape
                sig['last_quote_bid_exchange'] = st.last_quote_bid_exchange
                sig['last_quote_ask_exchange'] = st.last_quote_ask_exchange
                sig['last_quote_conditions'] = list(st.last_quote_conditions or [])
                sig['last_quote_tape'] = st.last_quote_tape
                sig['quote_state'] = ind.get('quote_state') or {}
                sig['condition_quality'] = ind.get('condition_quality') or {}
                sig['spread_vs_rolling_median'] = ind.get('spread_vs_rolling_median')
                sig['spread_abnormal'] = ind.get('spread_abnormal')
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
                self._maybe_log_gate_timeline(
                    tkr, ind, btc_ind, stock_indicators, sig,
                    'blocked', 'execution_quality_low',
                )
                continue
            sig['shadow_variants'] = shadow_variants(sig)
            self._maybe_log_gate_timeline(
                tkr, ind, btc_ind, stock_indicators, sig,
                'signal_emitted', None,
            )
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

    def _interval_due(self, bucket: Dict[str, float], key: str, interval_sec: float) -> bool:
        if interval_sec <= 0:
            return False
        now = time.time()
        last = float(bucket.get(key, 0) or 0)
        if now - last < interval_sec:
            return False
        bucket[key] = now
        return True

    def _append_postmortem_jsonl(self, dir_path: str, stem: str, row: dict):
        try:
            os.makedirs(dir_path, exist_ok=True)
            created = float(row.get('created_at') or time.time())
            day = datetime.fromtimestamp(created, ET).date().isoformat()
            path = os.path.join(dir_path, f'{stem}_{day}.jsonl')
            with open(path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(row, separators=(',', ':'), default=str) + '\n')
        except Exception as e:
            log.warning(f'{stem} write failed: {e}')

    def _gate_state_snapshot(self, ticker: str, ind: dict, btc_ind: Optional[dict],
                             stock_indicators: dict, sig: Optional[dict],
                             stage: str, reason: Optional[str] = None) -> dict:
        btc_ctx = sig.get('btc_context') if isinstance(sig, dict) else _btc_signal_context(ticker, ind, btc_ind)
        flow30 = ind.get('flow_30s') or {}
        flow120 = ind.get('flow_120s') or {}
        quote_age = ind.get('last_quote_age_sec')
        trade_age = ind.get('last_trade_age_sec')
        spread_pct = ind.get('spread_pct')
        quote_state = ind.get('quote_state') or {}
        condition_quality = ind.get('condition_quality') or {}
        dual_side = sig.get('shadow_dual_side_score') if isinstance(sig, dict) else None
        if not dual_side:
            dual_side = shadow_dual_side_score(ticker, ind, btc_ind, stock_indicators)
        vol_z = ind.get('vol_z_60s')
        tick_z = ind.get('tick_z_30s')
        vwap_sigma = ind.get('vwap_dist_sigma')
        realized = ind.get('realized_vol_60s_pct')
        basket = sig.get('miner_basket') if isinstance(sig, dict) else {}
        if not basket and sig and sig.get('side') in ('LONG', 'SHORT'):
            basket = _miner_basket_context(ticker, sig.get('side'), stock_indicators)
        condition_quality_score = _float_or_none(condition_quality.get('score'))
        spread_pct_f = _float_or_none(spread_pct)
        vol_z_f = _float_or_none(vol_z)
        tick_z_f = _float_or_none(tick_z)
        vwap_sigma_f = _float_or_none(vwap_sigma)
        realized_f = _float_or_none(realized)

        def fresh(age, max_age):
            age_f = _float_or_none(age)
            max_age_f = _float_or_none(max_age)
            return age_f is not None and max_age_f is not None and age_f <= max_age_f

        activity_ok = any(
            v is not None and v >= MIN_ACTIVITY_Z
            for v in (vol_z_f, tick_z_f)
        )
        gates = {
            'stock_ready': bool(ind.get('ready')),
            'btc_ready': bool(btc_ctx.get('ready')),
            'btc_stale': bool(btc_ctx.get('stale')),
            'stock_trade_fresh': fresh(trade_age, STOCK_MAX_STALE_SEC),
            'stock_quote_fresh': fresh(quote_age, STOCK_MAX_STALE_SEC),
            'spread_ok': spread_pct_f is None or spread_pct_f <= float(SMART_ENTRY.get('pre_submit_max_spread_pct', 0.12)),
            'quote_state_ok': quote_state.get('state') in (None, 'unknown', 'normal'),
            'condition_quality_ok': condition_quality_score is None or condition_quality_score >= 80,
            'spread_baseline_ok': not bool(ind.get('spread_abnormal')),
            'activity_ok': activity_ok,
            'near_vwap_chop': vwap_sigma_f is not None and abs(vwap_sigma_f) <= VWAP_CHOP_SIGMA,
            'realized_vol_ok': realized_f is None or realized_f >= MIN_REALIZED_VOL_60S_PCT,
            'miner_basket_state': (basket or {}).get('state'),
        }
        blockers = []
        for name in ('stock_ready', 'btc_ready', 'stock_trade_fresh',
                     'stock_quote_fresh', 'spread_ok', 'activity_ok',
                     'realized_vol_ok', 'quote_state_ok',
                     'condition_quality_ok', 'spread_baseline_ok'):
            value = gates.get(name)
            if value is False:
                blockers.append(name)
        if gates.get('btc_stale'):
            blockers.append('btc_stale')
        if reason:
            blockers.append(str(reason))
        return {
            'schema_version': 1,
            'created_at': int(time.time()),
            'created_at_et': datetime.now(ET).isoformat(timespec='seconds'),
            'ticker': ticker,
            'stage': stage,
            'reason': reason,
            'side': sig.get('side') if isinstance(sig, dict) else None,
            'setup_type': sig.get('setup_type') if isinstance(sig, dict) else None,
            'score': sig.get('score') if isinstance(sig, dict) else None,
            'price': ind.get('price'),
            'gates': gates,
            'blocked_by': blockers,
            'indicators': {
                'ema_stack': ind.get('ema_stack'),
                'mom_5s': ind.get('mom_5s'),
                'mom_15s': ind.get('mom_15s'),
                'mom_60s': ind.get('mom_60s'),
                'flow_30s': flow30,
                'flow_120s': flow120,
                'flow_30s_delta': ind.get('flow_30s_delta'),
                'spread_pct': spread_pct,
                'rolling_spread_median_pct': ind.get('rolling_spread_median_pct'),
                'rolling_spread_p95_pct': ind.get('rolling_spread_p95_pct'),
                'spread_vs_rolling_median': ind.get('spread_vs_rolling_median'),
                'spread_abnormal': ind.get('spread_abnormal'),
                'quote_state': quote_state,
                'condition_quality': condition_quality,
                'quote_age_sec': quote_age,
                'trade_age_sec': trade_age,
                'vwap_dist_sigma': vwap_sigma,
                'realized_vol_60s_pct': realized,
                'quote_imbalance': ind.get('quote_imbalance'),
                'last_trade_exchange': ind.get('last_trade_exchange'),
                'last_trade_conditions': ind.get('last_trade_conditions') or [],
                'last_quote_bid_exchange': ind.get('last_quote_bid_exchange'),
                'last_quote_ask_exchange': ind.get('last_quote_ask_exchange'),
                'last_quote_conditions': ind.get('last_quote_conditions') or [],
            },
            'btc_context': btc_ctx,
            'miner_basket': basket or {},
            'shadow_dual_side_score': dual_side,
        }

    def _maybe_log_gate_timeline(self, ticker: str, ind: dict, btc_ind: Optional[dict],
                                 stock_indicators: dict, sig: Optional[dict],
                                 stage: str, reason: Optional[str] = None):
        key = f'{ticker}:{stage}'
        if not self._interval_due(self._last_gate_timeline_log, key, GATE_TIMELINE_INTERVAL_SEC):
            return
        row = self._gate_state_snapshot(ticker, ind, btc_ind, stock_indicators, sig, stage, reason)
        self._append_postmortem_jsonl(GATE_TIMELINE_DIR, 'gate_timeline', row)

    def _maybe_log_no_signal_snapshot(self, ticker: str, ind: dict, btc_ind: Optional[dict],
                                      stock_indicators: dict, reason: str):
        if not self._interval_due(self._last_no_signal_snapshot_log, ticker, NO_SIGNAL_SNAPSHOT_INTERVAL_SEC):
            return
        row = self._gate_state_snapshot(ticker, ind, btc_ind, stock_indicators, None, 'no_signal', reason)
        row['snapshot_type'] = 'periodic_no_signal'
        self._append_postmortem_jsonl(NO_SIGNAL_SNAPSHOT_DIR, 'no_signal_snapshots', row)


# ------------------------------------------------------------------------
# Indicators (pure functions on SymbolState)
# ------------------------------------------------------------------------
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
        last_trade_exchange = st.last_trade_exchange
        last_trade_conditions = list(st.last_trade_conditions or [])
        last_trade_tape = st.last_trade_tape
        last_quote_bid_exchange = st.last_quote_bid_exchange
        last_quote_ask_exchange = st.last_quote_ask_exchange
        last_quote_conditions = list(st.last_quote_conditions or [])
        last_quote_tape = st.last_quote_tape
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

    def chop_metrics(window):
        if len(closes) < window + 1 or price is None:
            return {}
        vals = closes[-window-1:]
        base = vals[0]
        if not base:
            return {}
        net = abs(vals[-1] - vals[0]) / base * 100
        path = sum(abs(cur - prev) for prev, cur in zip(vals, vals[1:])) / base * 100
        rng = (max(vals) - min(vals)) / base * 100
        signs = []
        for prev, cur in zip(vals, vals[1:]):
            move = (cur - prev) / base * 100
            if abs(move) >= 0.01:
                signs.append(1 if move > 0 else -1)
        flips = sum(1 for i in range(1, len(signs)) if signs[i] != signs[i - 1])
        return {
            f'chop_range_{window}s_pct': round(rng, 3),
            f'chop_path_{window}s_pct': round(path, 3),
            f'chop_efficiency_{window}s': round(net / path, 4) if path > 0 else None,
            f'chop_flips_{window}s': flips,
        }

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
    # s-band estimate from recent 5-min stddev of prices
    # 2026-04-20: added two guards after live RIOT produced a bogus
    # "-18.36s" signal from tiny early-session stddev:
    #   (1) Floor sd at 5 bps of price — prevents div-by-tiny
    #   (2) Clamp output to ±5s — anything larger is an artifact
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
        raw = max(-5.0, min(5.0, raw))   # clamp to ±5s
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
    rolling_spreads = []
    spread_cutoff = now_ms - max(1, ROLLING_SPREAD_WINDOW_SEC) * 1000
    for q in quote_history:
        qts, bid, ask, *_rest = q
        if qts is None or qts < spread_cutoff or bid is None or ask is None:
            continue
        try:
            bid_f = float(bid)
            ask_f = float(ask)
            mid = (bid_f + ask_f) / 2.0
            if mid > 0 and ask_f > bid_f:
                rolling_spreads.append((ask_f - bid_f) / mid * 100.0)
        except Exception:
            continue
    rolling_spread_median = _median(rolling_spreads)
    rolling_spread_p95 = _percentile(rolling_spreads, 95)
    spread_vs_rolling_median = None
    spread_abnormal = False
    if spread_pct is not None and rolling_spread_median and rolling_spread_median > 0:
        spread_vs_rolling_median = round(float(spread_pct) / rolling_spread_median, 3)
        spread_abnormal = spread_vs_rolling_median >= ROLLING_SPREAD_ABNORMAL_MULTIPLE
    quote_state = _quote_state(best_bid, best_ask, price, last_quote_age_sec, spread_pct)
    condition_quality = _condition_quality(
        last_trade_conditions,
        last_quote_conditions,
        last_trade_exchange,
        last_quote_bid_exchange,
        last_quote_ask_exchange,
    )
    session_elapsed_sec = len(bars)
    if session_elapsed_sec < 15 * 60:
        tod_bucket = 'open_0_15'
    elif session_elapsed_sec < 30 * 60:
        tod_bucket = 'open_15_30'
    elif session_elapsed_sec < 150 * 60:
        tod_bucket = 'morning'
    elif session_elapsed_sec < 330 * 60:
        tod_bucket = 'midday'
    else:
        tod_bucket = 'late'
    activity_vals = [float(v) for v in (vol_z, tick_z) if isinstance(v, (int, float))]
    activity_score = round(sum(activity_vals) / len(activity_vals), 2) if activity_vals else None
    if activity_score is None:
        tod_activity_label = 'unknown'
    elif activity_score >= 3:
        tod_activity_label = 'extreme'
    elif activity_score >= 1.5:
        tod_activity_label = 'high'
    elif activity_score <= -1:
        tod_activity_label = 'low'
    else:
        tod_activity_label = 'normal'

    ema5  = ema(closes, 5)
    ema15 = ema(closes, 15)
    ema60 = ema(closes, 60)
    stack = None
    if ema5 and ema15 and ema60:
        if ema5 > ema15 > ema60: stack = 'bull'
        elif ema5 < ema15 < ema60: stack = 'bear'
        else: stack = 'mixed'

    chop_180s = chop_metrics(180)
    chop_300s = chop_metrics(300)

    return {
        'ready': True,
        'symbol': symbol,
        'price':  price,
        'bars':   len(bars),
        'session_elapsed_sec': session_elapsed_sec,
        'time_of_day_bucket': tod_bucket,
        'tod_activity_score': activity_score,
        'tod_activity_label': tod_activity_label,
        'tod_activity_method': 'same_session_rolling_zscore',
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
        **chop_180s,
        **chop_300s,
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
        'last_trade_exchange': last_trade_exchange,
        'last_trade_conditions': last_trade_conditions,
        'last_trade_tape': last_trade_tape,
        'last_quote_bid_exchange': last_quote_bid_exchange,
        'last_quote_ask_exchange': last_quote_ask_exchange,
        'last_quote_conditions': last_quote_conditions,
        'last_quote_tape': last_quote_tape,
        'spread_pct': spread_pct,
        'rolling_spread_median_pct': round(rolling_spread_median, 4) if rolling_spread_median is not None else None,
        'rolling_spread_p95_pct': round(rolling_spread_p95, 4) if rolling_spread_p95 is not None else None,
        'spread_vs_rolling_median': spread_vs_rolling_median,
        'spread_abnormal': spread_abnormal,
        'quote_state': quote_state,
        'condition_quality': condition_quality,
        'quote_imbalance': quote_imbalance,
        'quote_imbalance_delta_5s': quote_imbalance_delta_5s,
    }


# ------------------------------------------------------------------------
# Signal detection
# ------------------------------------------------------------------------
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
    realized_beta_60s = None
    realized_beta_180s = None
    if b60 not in (None, 0) and stock_mom is not None and abs(float(b60)) >= 0.03:
        realized_beta_60s = round(float(stock_mom) / float(b60), 3)
    stock_mom_180 = ind.get('mom_180s')
    if b180 not in (None, 0) and stock_mom_180 is not None and abs(float(b180)) >= 0.05:
        realized_beta_180s = round(float(stock_mom_180) / float(b180), 3)
    beta_delta = None
    if beta is not None and realized_beta_60s is not None:
        beta_delta = round(float(realized_beta_60s) - float(beta), 3)
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
        'realized_beta_60s': realized_beta_60s,
        'realized_beta_180s': realized_beta_180s,
        'beta_delta_vs_config_60s': beta_delta,
        'beta_source': 'config_static_with_passive_intraday_estimate',
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


def _setup_tags(side: str, ind: dict, btc_ctx: dict, components: dict) -> List[str]:
    vds = ind.get('vwap_dist_sigma')
    bp = (ind.get('flow_30s') or {}).get('buy_pct')
    rel = btc_ctx.get('stock_minus_btc_implied_60s')
    tags = []
    if rel is not None and (
        (side == 'LONG' and rel >= REL_STRENGTH_MIN)
        or (side == 'SHORT' and rel <= -REL_STRENGTH_MIN)
    ):
        tags.append('btc_relative_strength')
    if components.get('burst') and abs(ind.get('mom_15s') or 0) >= 0.08:
        tags.append('momentum_breakout')
    if vds is not None and abs(vds) <= 1.0 and (
        (side == 'LONG' and (ind.get('vwap_dist') or 0) > 0)
        or (side == 'SHORT' and (ind.get('vwap_dist') or 0) < 0)
    ):
        tags.append('vwap_reclaim_breakdown')
    if bp is not None and ((side == 'SHORT' and bp >= 65) or (side == 'LONG' and bp <= 35)):
        tags.append('flow_exhaustion_fade')
    if not tags:
        tags.append('trend_pullback')
    return tags


def _classify_setup(side: str, ind: dict, btc_ctx: dict, components: dict) -> str:
    tags = _setup_tags(side, ind, btc_ctx, components)
    # Prefer directional/context edges first. Flow exhaustion remains a setup,
    # but it should not relabel a cleaner BTC-relative or momentum trade just
    # because short-window flow is extreme.
    for setup in (
        'btc_relative_strength',
        'momentum_breakout',
        'vwap_reclaim_breakdown',
        'flow_exhaustion_fade',
        'trend_pullback',
    ):
        if setup in tags:
            return setup
    return 'trend_pullback'


def _flow_fade_confirmed(side: str, ind: dict) -> bool:
    bp = (ind.get('flow_30s') or {}).get('buy_pct')
    if bp is None:
        return False
    delta = ind.get('flow_30s_delta')
    m5 = ind.get('mom_5s')
    qi = ind.get('quote_imbalance')
    qid = ind.get('quote_imbalance_delta_5s')
    f120 = ind.get('flow_120s') or {}
    bp120 = f120.get('buy_pct')
    if side == 'SHORT' and bp >= 65:
        flow_rollover = delta is not None and delta <= -FLOW_FADE_MIN_DELTA_PCT
        micro_confirm = (
            (m5 is not None and m5 < -0.02)
            or (qi is not None and qi < -0.10)
            or (qid is not None and qid <= -0.15)
        )
        sustained_pressure = bp120 is not None and bp120 >= 65 and not flow_rollover
        return bool(flow_rollover and micro_confirm and not sustained_pressure)
    if side == 'LONG' and bp <= 35:
        flow_rollover = delta is not None and delta >= FLOW_FADE_MIN_DELTA_PCT
        micro_confirm = (
            (m5 is not None and m5 > 0.02)
            or (qi is not None and qi > 0.10)
            or (qid is not None and qid >= 0.15)
        )
        sustained_pressure = bp120 is not None and bp120 <= 35 and not flow_rollover
        return bool(flow_rollover and micro_confirm and not sustained_pressure)
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


def shadow_dual_side_score(ticker: str, ind: dict, btc_ind: Optional[dict],
                           miner_indicators: Optional[dict] = None) -> dict:
    """Passive LONG-vs-SHORT scorecard for postmortem side-selection review."""
    if not DUAL_SIDE_SHADOW_ENABLED:
        return {'enabled': False, 'mode': 'disabled'}
    btc_ctx = _btc_signal_context(ticker, ind, btc_ind)

    def score_side(side: str) -> dict:
        score = 0.0
        reasons = []
        blockers = []
        components = {}
        stack = ind.get('ema_stack')
        if side == 'LONG':
            if stack == 'bull':
                score += 2
                components['ema_stack'] = 2
                reasons.append('ema_stack_supports_long')
            elif stack == 'bear':
                score -= 2
                blockers.append('ema_stack_opposes_long')
        else:
            if stack == 'bear':
                score += 2
                components['ema_stack'] = 2
                reasons.append('ema_stack_supports_short')
            elif stack == 'bull':
                score -= 2
                blockers.append('ema_stack_opposes_short')

        vd = ind.get('vwap_dist')
        if vd is not None:
            supports = (side == 'LONG' and vd > 0) or (side == 'SHORT' and vd < 0)
            score += 1 if supports else -1
            components['vwap_side'] = 1 if supports else -1
            (reasons if supports else blockers).append('vwap_supports_side' if supports else 'vwap_opposes_side')

        m5 = ind.get('mom_5s')
        m15 = ind.get('mom_15s')
        if m5 is not None and m15 is not None:
            supports = (side == 'LONG' and m5 > 0.05 and m15 > 0.05) \
                or (side == 'SHORT' and m5 < -0.05 and m15 < -0.05)
            opposes = (side == 'LONG' and m5 < -0.05 and m15 < -0.05) \
                or (side == 'SHORT' and m5 > 0.05 and m15 > 0.05)
            if supports:
                score += 1
                components['short_momentum'] = 1
                reasons.append('5s_15s_momentum_supports_side')
            elif opposes:
                score -= 1
                components['short_momentum'] = -1
                blockers.append('5s_15s_momentum_opposes_side')

        bp30 = (ind.get('flow_30s') or {}).get('buy_pct')
        bp120 = (ind.get('flow_120s') or {}).get('buy_pct')
        if bp30 is not None:
            pressure_supports = (side == 'LONG' and bp30 >= 55) or (side == 'SHORT' and bp30 <= 45)
            pressure_opposes = (side == 'LONG' and bp30 <= 35) or (side == 'SHORT' and bp30 >= 65)
            if pressure_supports:
                score += 1
                components['flow_pressure'] = 1
                reasons.append('flow_supports_side')
            elif pressure_opposes:
                if _flow_fade_confirmed(side, ind):
                    score += 0.5
                    components['flow_fade'] = 0.5
                    reasons.append('opposing_flow_rollover_confirmed')
                else:
                    score -= 1
                    components['flow_pressure'] = -1
                    blockers.append('opposing_flow_not_rolled_over')
        if bp120 is not None:
            sustained_supports = (side == 'LONG' and bp120 >= 58) or (side == 'SHORT' and bp120 <= 42)
            sustained_opposes = (side == 'LONG' and bp120 <= 40) or (side == 'SHORT' and bp120 >= 60)
            if sustained_supports:
                score += 0.5
                components['sustained_flow'] = 0.5
                reasons.append('120s_flow_supports_side')
            elif sustained_opposes:
                score -= 0.5
                components['sustained_flow'] = -0.5
                blockers.append('120s_flow_opposes_side')

        if btc_ctx.get('ready') and not btc_ctx.get('stale'):
            regime = btc_ctx.get('regime')
            rel = btc_ctx.get('stock_minus_btc_implied_60s')
            if (side == 'LONG' and regime in ('bull', 'bull_momentum')) \
                    or (side == 'SHORT' and regime in ('bear', 'bear_momentum')):
                score += 2
                components['btc_regime'] = 2
                reasons.append('btc_regime_supports_side')
            elif (side == 'LONG' and regime in ('bear', 'bear_momentum')) \
                    or (side == 'SHORT' and regime in ('bull', 'bull_momentum')):
                score -= 2
                components['btc_regime'] = -2
                blockers.append('btc_regime_opposes_side')
            if rel is not None:
                if (side == 'LONG' and rel >= REL_STRENGTH_MIN) or (side == 'SHORT' and rel <= -REL_STRENGTH_MIN):
                    score += 1
                    components['btc_relative_strength'] = 1
                    reasons.append('relative_strength_supports_side')
                elif (side == 'LONG' and rel <= -REL_STRENGTH_MIN) or (side == 'SHORT' and rel >= REL_STRENGTH_MIN):
                    score -= 1
                    components['btc_relative_strength'] = -1
                    blockers.append('relative_strength_opposes_side')
        else:
            blockers.append('btc_not_ready_or_stale')

        basket = _miner_basket_context(ticker, side, miner_indicators) if MINER_BASKET_ENABLED else {}
        if basket.get('state') == 'confirmed':
            score += 1
            components['miner_basket'] = 1
            reasons.append('miner_basket_confirms_side')
        elif basket.get('state') in ('opposed', 'mixed_conflict'):
            score -= 1
            components['miner_basket'] = -1
            blockers.append(f"miner_basket_{basket.get('state')}")

        lead = _lead_lag_bias(side, btc_ctx)
        if lead.get('score'):
            score += float(lead['score'])
            components['btc_lead_lag'] = lead['score']
            (reasons if lead['score'] > 0 else blockers).append(lead.get('state'))

        eq = execution_quality({
            'side': side,
            'price': ind.get('price'),
            'best_bid': ind.get('best_bid'),
            'best_ask': ind.get('best_ask'),
            'indicators': ind,
        })
        if eq.get('score') is not None and float(eq.get('score') or 0) < 75:
            penalty = 2 if float(eq.get('score') or 0) < 65 else 1
            score -= penalty
            components['execution_quality'] = -penalty
            blockers.extend(eq.get('reasons') or ['execution_quality_low'])
        qstate = (ind.get('quote_state') or {}).get('state')
        if qstate in ('locked', 'crossed', 'one_sided', 'missing', 'invalid'):
            blockers.append(f'quote_state_{qstate}')
        if ind.get('spread_abnormal'):
            blockers.append('spread_abnormal_vs_ticker_baseline')

        setup = _classify_setup(side, ind, btc_ctx, components)
        min_score = _adjusted_min_score(setup, ind)
        return {
            'side': side,
            'score': round(score, 3),
            'abs_score': round(abs(score), 3),
            'setup_type': setup,
            'min_score_required': min_score,
            'passes_shadow_threshold': score >= min_score,
            'components': components,
            'reasons': reasons[:10],
            'blockers': sorted(set(str(b) for b in blockers))[:12],
            'execution_quality': eq,
            'miner_basket': basket,
            'lead_lag': lead,
        }

    long_row = score_side('LONG')
    short_row = score_side('SHORT')
    chosen = 'LONG' if long_row['score'] >= short_row['score'] else 'SHORT'
    chosen_row = long_row if chosen == 'LONG' else short_row
    opposite_row = short_row if chosen == 'LONG' else long_row
    return {
        'enabled': True,
        'mode': 'passive_shadow_only',
        'ticker': ticker,
        'long': long_row,
        'short': short_row,
        'chosen_side_by_shadow': chosen,
        'opposite_side': opposite_row['side'],
        'side_gap': round(abs(long_row['score'] - short_row['score']), 3),
        'chosen_score': chosen_row['score'],
        'opposite_score': opposite_row['score'],
        'why_opposite_failed': opposite_row.get('blockers') or ['lower_shadow_score'],
        'btc_context': btc_ctx,
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
        if confirming and opposing:
            state = 'mixed_conflict'
        elif confirming >= MINER_BASKET_MIN_CONFIRMING:
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
    quote_state = ind.get('quote_state') or {}
    condition_quality = ind.get('condition_quality') or {}
    spread_vs_rolling = ind.get('spread_vs_rolling_median')
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
    qstate = quote_state.get('state')
    if qstate in ('locked', 'crossed'):
        score -= QUOTE_LOCKED_CROSSED_PENALTY
        reasons.append(f'quote_{qstate}')
    elif qstate in ('missing', 'one_sided', 'invalid'):
        score -= 15
        reasons.append(f'quote_{qstate}')
    if spread_vs_rolling is not None:
        try:
            if float(spread_vs_rolling) >= ROLLING_SPREAD_ABNORMAL_MULTIPLE:
                score -= 15
                reasons.append('spread_abnormal_vs_ticker_baseline')
        except Exception:
            pass
    cq_score = _float_or_none(condition_quality.get('score'))
    if cq_score is not None and cq_score < 90:
        score -= min(20, int((90 - cq_score) / 2))
        reasons.extend(condition_quality.get('tags') or [])
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
        'quote_state': quote_state,
        'condition_quality': condition_quality,
        'spread_vs_rolling_median': spread_vs_rolling,
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


def _scalarize_snapshot(d: Optional[dict]) -> dict:
    out = {}
    for k, v in (d or {}).items():
        if isinstance(v, (int, float, str, bool)) or v is None:
            out[k] = v
        elif isinstance(v, dict):
            out[k] = {
                kk: vv for kk, vv in v.items()
                if isinstance(vv, (int, float, str, bool)) or vv is None
            }
    return out


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
            'indicators': _scalarize_snapshot(ind),
            'btc_context': btc_ctx or {},
        }
        with open(path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(row, separators=(',', ':'), default=str) + '\n')
        cb = _near_signal_capture_cb
        if cb:
            try:
                cb(row)
            except Exception as capture_err:
                log.warning(f'near signal tick capture failed: {capture_err}')
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
    pre_exec_quality = execution_quality({
        'side': side,
        'price': ind.get('price'),
        'best_bid': ind.get('best_bid'),
        'best_ask': ind.get('best_ask'),
        'indicators': ind,
    })
    exec_score = float(pre_exec_quality.get('score') or 0)
    exec_penalty = 0
    if exec_score < 65:
        exec_penalty = 2
    elif exec_score < 75:
        exec_penalty = 1
    if exec_penalty:
        if side == 'LONG':
            score -= exec_penalty
            components['execution_quality_penalty'] = -exec_penalty
        else:
            score += exec_penalty
            components['execution_quality_penalty'] = exec_penalty
        (reasons_long if side == 'LONG' else reasons_short).append(
            f"execution quality penalty score={exec_score:.0f}"
        )
        if (side == 'LONG' and score <= 0) or (side == 'SHORT' and score >= 0):
            _log_near_signal(ticker, side, 'execution_quality_erased_edge', score, ind, btc_ctx, components)
            return None
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
    dual_side = shadow_dual_side_score(ticker, ind, btc_ind, miner_indicators)
    setup_tags = _setup_tags(side, ind, btc_ctx, components)
    setup_type = _classify_setup(side, ind, btc_ctx, components)
    flow_fade_confirmed = True
    flow_fade_required = 'flow_exhaustion_fade' in setup_tags
    if flow_fade_required and FLOW_FADE_REQUIRES_EXHAUSTION:
        flow_fade_confirmed = _flow_fade_confirmed(side, ind)
        components['flow_fade_confirmed'] = flow_fade_confirmed
        components['opposing_flow_requires_confirmation'] = True
        if not flow_fade_confirmed:
            reason = (
                'flow_fade_unconfirmed'
                if setup_type == 'flow_exhaustion_fade'
                else 'opposing_flow_unconfirmed'
            )
            _log_near_signal(ticker, side, reason, score, ind, btc_ctx, components)
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

    sig = {
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
        'execution_quality': pre_exec_quality,
        'shadow_dual_side_score': dual_side,
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
            'setup_tags': setup_tags,
            'min_score_required': min_score,
            'score_components': components,
            'side_scores': side_scores,
            'score_model': {
                'direction_score': side_scores,
                'dual_side_shadow': dual_side,
                'edge_after_friction_score': round(abs(score) - float(exec_penalty or 0), 3),
                'friction_penalty_score': exec_penalty,
                'regime_score': round(sum(float(v) for k, v in components.items()
                                          if k in ('btc', 'relative_strength', 'btc_lead_lag', 'miner_basket')
                                          and isinstance(v, (int, float))), 3),
                'activity_score': round(sum(float(v) for k, v in components.items()
                                            if k in ('burst_score', 'momentum')
                                            and isinstance(v, (int, float))), 3),
                'execution_score': pre_exec_quality.get('score'),
                'execution_reasons': pre_exec_quality.get('reasons', []),
                'risk_notes': [
                    k for k in ('realized_vol_chop', 'btc_chop_penalty')
                    if k in components
                ],
            },
        },
        'indicators': _scalarize_snapshot(ind),
        'btc_indicators': _scalarize_snapshot(btc_ind) if btc_ind else None,
    }
    return _apply_active_scoring_profile(sig, ind, btc_ind)

# helpers
# ------------------------------------------------------------------------
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


# ------------------------------------------------------------------------
# Module-level singleton
# ------------------------------------------------------------------------
_engine: Optional[ScalpEngine] = None

def get_engine(api_key: Optional[str] = None,
               secret_key: Optional[str] = None) -> ScalpEngine:
    global _engine
    if _engine is None:
        if not api_key or not secret_key:
            raise RuntimeError('First call must pass api_key and secret_key')
        _engine = ScalpEngine(api_key, secret_key)
    return _engine

