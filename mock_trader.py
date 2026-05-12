"""
Mock paper-trading engine.

Live entries come from ws_scalp only. Minute-bar fetches are still used for
relative-volume and broad-market context, but they do not fire trades.

- Starting balance: Alpaca paper equity when available; otherwise $1,000 fallback
- Entry window: Mon-Fri 08:30-14:45 America/Chicago
- Hard-flat cutoff: 14:55 America/Chicago
- Tickers: CLSK, MARA, RIOT (subscribed via engine.add_ticker on boot)
- Position sizing: 25% of current balance per entry
- Entry: MEDIUM or HIGH conviction signal
- Exit: per-ticker SL/TP, hard flat at 14:55 CT
- Persistence: compact mock_trader_state.json plus append-only postmortem/audit corpuses
"""
from __future__ import annotations

from output_paths import output_path

import json
import logging
import os
import hashlib
import glob
import threading
import time
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from typing import Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:  # py<3.9 fallback
    from backports.zoneinfo import ZoneInfo  # type: ignore

try:
    from alpaca_trading import AlpacaTrader, alloc_to_qty, AlpacaTradingError
except Exception:
    AlpacaTrader = None
    alloc_to_qty = None
    AlpacaTradingError = Exception

try:
    import execution_kernel
    import step2_parity_contract
except Exception:
    execution_kernel = None
    step2_parity_contract = None

import live_step2_decision_kernel
import execution_action_engine
import execution_intent_engine
import step2_execution_contract
try:
    import live_decision_ledger
except Exception:
    live_decision_ledger = None
try:
    import execution_lifecycle
except Exception:
    execution_lifecycle = None
try:
    import execution_state_reducer
except Exception:
    execution_state_reducer = None
try:
    import contract_gate
except Exception:
    contract_gate = None
try:
    import promotion_safety
except Exception:
    promotion_safety = None
try:
    import promotion_manifest
except Exception:
    promotion_manifest = None
try:
    import config_change_journal
except Exception:
    config_change_journal = None
try:
    import live_profile_recovery
except Exception:
    live_profile_recovery = None
try:
    import live_state_rollover
except Exception:
    live_state_rollover = None
try:
    import broker_lifecycle_guard
except Exception:
    broker_lifecycle_guard = None
try:
    import execution_adapters
except Exception:
    execution_adapters = None
try:
    import intraday_shadow_step2
except Exception:
    intraday_shadow_step2 = None
try:
    import shadow_variant_engine
except Exception:
    shadow_variant_engine = None
try:
    import canonical_opportunity_ledger
except Exception:
    canonical_opportunity_ledger = None
try:
    import canonical_decision_packet
except Exception:
    canonical_decision_packet = None

# Minute-bar fetches for RVOL and market-tape context.
try:
    from data_fetch import (
        alpaca_stock_bars as _bar_fetch_stock,
    )
    BAR_DATA_AVAILABLE = True
except Exception as e:
    BAR_DATA_AVAILABLE = False
    _bar_data_import_err = e

CT = ZoneInfo('America/Chicago')
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           'trading_config.json')
AUDIT_DIR = output_path('audit')
SKIPPED_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           'postmortem', 'skipped_signals')
SHADOW_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          'postmortem', 'shadow_decisions')
SHADOW_EXIT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               'postmortem', 'shadow_exits')
DECISION_AUDIT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  'postmortem', 'decision_audits')
ENTRY_RETRY_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               'postmortem', 'entry_retry_candidates')
RULE_DRY_RUN_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                'postmortem', 'rule_dry_run')
FILL_ATTRIBUTION_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                    'postmortem', 'fill_attribution')
LATENCY_ATTRIBUTION_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                       'postmortem', 'latency_attribution')
LIVE_SIGNAL_PARITY_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                      'postmortem', 'live_signal_parity')
TRADE_CORPUS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                'postmortem', 'trades')
HEALTH_HEARTBEAT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                     'postmortem', 'health_heartbeats')
EMERGENCY_STATE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   'postmortem', 'emergency_state')
EMERGENCY_STATE_BLOCK_PATH = os.path.join(EMERGENCY_STATE_DIR, 'critical_state_block.json')
KILL_SWITCH_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                'kill_switch.json')
CURRENT_ERA_START = '2026-05-04'
CURRENT_ERA_LABEL = 'vNext_2026_05_04'
BASELINE_ERA_LABEL = 'pre_2026_05_04'


class StatePersistenceError(RuntimeError):
    pass


def _load_trading_config() -> dict:
    try:
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


TRADING_CONFIG = _load_trading_config()
EXECUTION_MODE = str(TRADING_CONFIG.get('execution_mode') or 'live_guarded')
STEP3_PARITY_LIVE_MODE = EXECUTION_MODE == 'step3_parity_live'
STEP2_SIGNAL_SCAN_LIVE_MODE = EXECUTION_MODE == 'step2_signal_scan_live'
STEP_PARITY_LIVE_MODE = STEP3_PARITY_LIVE_MODE or STEP2_SIGNAL_SCAN_LIVE_MODE
if execution_kernel is not None:
    EXECUTION_KERNEL_CONTRACT = execution_kernel.contract_from_config(TRADING_CONFIG)
    EXECUTION_KERNEL_HASH = EXECUTION_KERNEL_CONTRACT.get('execution_kernel_hash')
else:
    EXECUTION_KERNEL_CONTRACT = {}
    EXECUTION_KERNEL_HASH = None
if step2_parity_contract is not None:
    STEP2_PARITY_CONTRACT = step2_parity_contract.contract(TRADING_CONFIG)
    STEP2_PARITY_CONTRACT_HASH = step2_parity_contract.contract_hash(STEP2_PARITY_CONTRACT)
else:
    STEP2_PARITY_CONTRACT = {}
    STEP2_PARITY_CONTRACT_HASH = None


def _file_sha256(path: str) -> Optional[str]:
    try:
        h = hashlib.sha256()
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(65536), b''):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None


STRATEGY_CONFIG_HASH = _file_sha256(CONFIG_PATH)


def _config_snapshot() -> dict:
    smart = TRADING_CONFIG.get('smart_entry', {}) or {}
    adaptive = TRADING_CONFIG.get('adaptive_management', {}) or {}
    session = TRADING_CONFIG.get('session', {}) or {}
    return {
        'config_sha256': STRATEGY_CONFIG_HASH,
        'execution_mode': EXECUTION_MODE,
        'execution_kernel_hash': EXECUTION_KERNEL_HASH,
        'execution_kernel_contract': EXECUTION_KERNEL_CONTRACT,
        'step3_parity_live_mode': STEP3_PARITY_LIVE_MODE,
        'step2_signal_scan_live_mode': STEP2_SIGNAL_SCAN_LIVE_MODE,
        'step_parity_live_mode': STEP_PARITY_LIVE_MODE,
        'step2_parity_contract_hash': STEP2_PARITY_CONTRACT_HASH,
        'step2_parity_contract': STEP2_PARITY_CONTRACT,
        'step2_execution_contract_hash': STEP2_EXECUTION_CONTRACT_HASH,
        'step2_execution_contract': STEP2_EXECUTION_CONTRACT,
        'trade_size_pct': TRADING_CONFIG.get('trade_size_pct'),
        'min_conviction': TRADING_CONFIG.get('min_conviction'),
        'max_entry_spread_pct': TRADING_CONFIG.get('max_entry_spread_pct'),
        'quote_stale_sec': TRADING_CONFIG.get('quote_stale_sec'),
        'session': {
            'entry_cutoff_hour': session.get('entry_cutoff_hour'),
            'entry_cutoff_minute': session.get('entry_cutoff_minute'),
            'flatten_hour': session.get('flatten_hour'),
            'flatten_minute': session.get('flatten_minute'),
        },
        'smart_entry': {
            'live_quality_gate_enabled': smart.get('live_quality_gate_enabled'),
            'live_quality_min_score': smart.get('live_quality_min_score'),
            'pre_submit_check_enabled': smart.get('pre_submit_check_enabled'),
            'pre_submit_max_adverse_move_pct': smart.get('pre_submit_max_adverse_move_pct'),
            'pre_submit_max_spread_pct': smart.get('pre_submit_max_spread_pct'),
            'pre_submit_max_quote_age_sec': smart.get('pre_submit_max_quote_age_sec'),
            'btc_max_stale_sec': smart.get('btc_max_stale_sec'),
            'stock_max_stale_sec': smart.get('stock_max_stale_sec'),
            'dual_side_shadow_enabled': smart.get('dual_side_shadow_enabled'),
            'rolling_spread_window_sec': smart.get('rolling_spread_window_sec'),
            'rolling_spread_abnormal_multiple': smart.get('rolling_spread_abnormal_multiple'),
            'min_score_by_setup': smart.get('min_score_by_setup'),
            'setup_cooldown_sec': smart.get('setup_cooldown_sec'),
        },
        'adaptive_management': {
            'profit_protect_enabled': adaptive.get('profit_protect_enabled'),
            'profit_protect_trigger_pct': adaptive.get('profit_protect_trigger_pct'),
            'profit_protect_giveback_pct': adaptive.get('profit_protect_giveback_pct'),
            'breakeven_after_sec': adaptive.get('breakeven_after_sec'),
            'breakeven_min_green_pct': adaptive.get('breakeven_min_green_pct'),
            'adverse_exit_pct': adaptive.get('adverse_exit_pct'),
            'adverse_exit_pct_by_ticker': adaptive.get('adverse_exit_pct_by_ticker'),
            'failed_followthrough_after_sec': adaptive.get('failed_followthrough_after_sec'),
            'failed_followthrough_min_mfe_pct': adaptive.get('failed_followthrough_min_mfe_pct'),
            'short_conviction_decay_enabled': adaptive.get('short_conviction_decay_enabled'),
            'long_conviction_decay_enabled': adaptive.get('long_conviction_decay_enabled'),
            'shadow_exit_policies_enabled': adaptive.get('shadow_exit_policies_enabled'),
            'shadow_exit_policies': adaptive.get('shadow_exit_policies'),
            'execution_risk_pause_enabled': adaptive.get('execution_risk_pause_enabled'),
            'execution_risk_pause_min': adaptive.get('execution_risk_pause_min'),
        },
        'ticker_cfg': TRADING_CONFIG.get('ticker_cfg'),
    }


def _trade_day(ts) -> Optional[str]:
    try:
        return datetime.fromtimestamp(float(ts or 0), CT).date().isoformat()
    except Exception:
        return None


def _era_for_day(day: Optional[str]) -> str:
    if not day:
        return 'unknown'
    return CURRENT_ERA_LABEL if day >= CURRENT_ERA_START else BASELINE_ERA_LABEL

# Flip to '0' to revert to pure-internal simulation.
USE_ALPACA_EXECUTION = os.getenv('USE_ALPACA_EXECUTION', '1') == '1'

# ── config ───────────────────────────────────────────────────────────────
TICKERS         = TRADING_CONFIG.get('tickers', ['CLSK', 'MARA', 'RIOT'])
START_BALANCE   = 1000.0
TRADE_SIZE_PCT  = float(TRADING_CONFIG.get('trade_size_pct', 0.25))
BTC_BETA        = TRADING_CONFIG.get('btc_beta', {'CLSK': 1.066, 'MARA': 0.967, 'RIOT': 1.20})

# Per-ticker tuning. SL=5% / TP=1% — the validated 180d config (856 trades,
# 77.0% win, +$491). 2026-04-25: restored from 4%→5% after diagnosis showed
# 4% prematurely stopped winners that needed the full ~5% adverse excursion
# before mean-reverting. The 4% setting was a misapplication of an older sweep
# that used different signal source / time-stop combination.
TICKER_CFG = {
    'CLSK': {'sl': 0.05, 'tp': 0.010, 'btc_mode': 'baseline'},
    'MARA': {'sl': 0.05, 'tp': 0.010, 'btc_mode': 'baseline'},
    # RIOT — wider brackets + BTC-agreement filter (180d +$186, OOS +$112)
    'RIOT': {'sl': 0.05, 'tp': 0.020, 'btc_mode': 'agree'},
}
TICKER_CFG = TRADING_CONFIG.get('ticker_cfg', TICKER_CFG)
# Back-compat defaults (used only if a ticker slips through without a cfg)
SL_PCT          = 0.05
TP_PCT          = 0.010
STEP2_MAX_TRADES_PER_DAY = int(STEP2_PARITY_CONTRACT.get('max_trades_per_day') or 0)
STEP2_MAX_TRADES_PER_TICKER_DAY = int(STEP2_PARITY_CONTRACT.get('max_trades_per_ticker_day') or 0)
STEP2_SAME_TICKER_REENTRY_COOLDOWN_SEC = int(
    STEP2_PARITY_CONTRACT.get('same_ticker_reentry_cooldown_sec') or 0
)
STEP2_EXECUTION_CONTRACT = step2_execution_contract.execution_contract(TRADING_CONFIG)
STEP2_EXECUTION_CONTRACT_HASH = step2_execution_contract.execution_contract_hash(TRADING_CONFIG)
MIN_CONVICTION  = tuple(TRADING_CONFIG.get('min_conviction', ['MEDIUM', 'HIGH']))
MIN_BALANCE     = float(TRADING_CONFIG.get('min_balance', 50.0))
_SESSION_CFG    = TRADING_CONFIG.get('session', {})
SESSION_START_H = int(_SESSION_CFG.get('start_hour', 8))
SESSION_START_M = int(_SESSION_CFG.get('start_minute', 30))
SESSION_END_H   = int(_SESSION_CFG.get('end_hour', 15))
SESSION_END_M   = int(_SESSION_CFG.get('end_minute', 0))
SESSION_FLATTEN_H = int(_SESSION_CFG.get('flatten_hour', 14))
SESSION_FLATTEN_M = int(_SESSION_CFG.get('flatten_minute', 55))
ENTRY_CUTOFF_H = int(_SESSION_CFG.get('entry_cutoff_hour', 14))
ENTRY_CUTOFF_M = int(_SESSION_CFG.get('entry_cutoff_minute', 45))
CONDITIONAL_TIME_STOP_ENABLED = bool(
    STEP2_PARITY_CONTRACT.get(
        'conditional_time_stop_enabled',
        TRADING_CONFIG.get('conditional_time_stop_enabled', False),
    )
)
COND_STOP_MIN   = int(
    STEP2_PARITY_CONTRACT.get(
        'conditional_stop_min',
        TRADING_CONFIG.get('conditional_stop_min', 15),
    )
)
# Relative-volume filter: require today's projected full-day volume to be at
# least this multiple of the 20-day average before a signal is allowed. Filters
# thin/choppy days where momentum signals are unreliable.
RVOL_MIN        = float(TRADING_CONFIG.get('rvol_min', 1.5))
RVOL_LOOKBACK   = int(TRADING_CONFIG.get('rvol_lookback', 20))
ENTRY_FILL_TIMEOUT_SEC = int(TRADING_CONFIG.get('entry_fill_timeout_sec', 60))
MAX_ENTRY_SPREAD_PCT = float(TRADING_CONFIG.get('max_entry_spread_pct', 0.0) or 0.0)
QUOTE_STALE_SEC = float(TRADING_CONFIG.get('quote_stale_sec', 5.0))
BROKER_API_DEGRADED_THRESHOLD = int(TRADING_CONFIG.get('broker_api_degraded_threshold', 3))
ADAPTIVE_CFG = TRADING_CONFIG.get('adaptive_management', {})
SMART_ENTRY_CFG = TRADING_CONFIG.get('smart_entry', {})
MIN_SCORE_BY_SETUP = SMART_ENTRY_CFG.get('min_score_by_setup', {
    'momentum_breakout': 4,
    'btc_relative_strength': 4,
    'vwap_reclaim_breakdown': 5,
    'trend_pullback': 5,
    'flow_exhaustion_fade': 5,
})
SPREAD_QUALITY_GATE_ENABLED = bool(SMART_ENTRY_CFG.get('spread_quality_gate_enabled', True))
SPREAD_GATE_ELEVATED_PCT = float(SMART_ENTRY_CFG.get('spread_gate_elevated_pct', 0.08))
SPREAD_GATE_HARD_PCT = float(SMART_ENTRY_CFG.get('spread_gate_hard_pct', MAX_ENTRY_SPREAD_PCT or 0.12))
SPREAD_GATE_MIN_ENTRY_QUALITY_SCORE = float(SMART_ENTRY_CFG.get('spread_gate_min_entry_quality_score', 70))
SPREAD_GATE_MIN_SIGNAL_SCORE = float(SMART_ENTRY_CFG.get('spread_gate_min_signal_score', 6))
ADAPTIVE_BRACKETS_ENABLED = bool(ADAPTIVE_CFG.get('adaptive_brackets_enabled', True))
FIXED_BRACKETS_ENABLED = bool(ADAPTIVE_CFG.get('fixed_brackets_enabled', False))
FIXED_BRACKETS = ADAPTIVE_CFG.get('fixed_brackets', {}) or {}
PROFIT_PROTECT_ENABLED = bool(ADAPTIVE_CFG.get('profit_protect_enabled', True))
PROFIT_PROTECT_TRIGGER_PCT = float(ADAPTIVE_CFG.get('profit_protect_trigger_pct', 0.45))
PROFIT_PROTECT_GIVEBACK_PCT = float(ADAPTIVE_CFG.get('profit_protect_giveback_pct', 0.55))
BREAKEVEN_AFTER_SEC = int(ADAPTIVE_CFG.get('breakeven_after_sec', 180))
BREAKEVEN_MIN_GREEN_PCT = float(ADAPTIVE_CFG.get('breakeven_min_green_pct', 0.20))
ADVERSE_EXIT_PCT = float(ADAPTIVE_CFG.get('adverse_exit_pct', 0.55))
ADVERSE_EXIT_BY_TICKER = ADAPTIVE_CFG.get('adverse_exit_pct_by_ticker', {})
BROKER_DISASTER_STOP_BY_TICKER = ADAPTIVE_CFG.get('broker_disaster_stop_pct_by_ticker', {})
FAILED_FOLLOWTHROUGH_AFTER_SEC = int(ADAPTIVE_CFG.get('failed_followthrough_after_sec', 120))
FAILED_FOLLOWTHROUGH_MIN_MFE_PCT = float(ADAPTIVE_CFG.get('failed_followthrough_min_mfe_pct', 0.12))
PATH_FAILURE_REDUCE_ENABLED = bool(ADAPTIVE_CFG.get('path_failure_reduce_enabled', False))
PATH_FAILURE_REDUCE_SETUP = str(ADAPTIVE_CFG.get('path_failure_reduce_setup', 'all') or 'all')
PATH_FAILURE_REDUCE_SESSION_PHASE = str(ADAPTIVE_CFG.get('path_failure_reduce_session_phase', 'all') or 'all')
PATH_FAILURE_REDUCE_AFTER_SEC = int(ADAPTIVE_CFG.get('path_failure_reduce_after_sec', 0) or 0)
PATH_FAILURE_REDUCE_WINDOW_SEC = int(ADAPTIVE_CFG.get('path_failure_reduce_window_sec', 0) or 0)
PATH_FAILURE_REDUCE_EDGE_THRESHOLD_PCT = float(ADAPTIVE_CFG.get('path_failure_reduce_edge_threshold_pct', 0.0))
PATH_FAILURE_REDUCE_REL_THRESHOLD_PCT = ADAPTIVE_CFG.get('path_failure_reduce_rel_threshold_pct')
if PATH_FAILURE_REDUCE_REL_THRESHOLD_PCT is not None:
    PATH_FAILURE_REDUCE_REL_THRESHOLD_PCT = float(PATH_FAILURE_REDUCE_REL_THRESHOLD_PCT)
PATH_FAILURE_REDUCE_FRACTION = float(ADAPTIVE_CFG.get('path_failure_reduce_fraction', 1.0) or 1.0)
SHORT_CONVICTION_DECAY_ENABLED = bool(ADAPTIVE_CFG.get('short_conviction_decay_enabled', True))
SHORT_CONVICTION_MIN_AGE_SEC = int(ADAPTIVE_CFG.get('short_conviction_min_age_sec', 20))
SHORT_CONVICTION_MIN_SCORE = int(ADAPTIVE_CFG.get('short_conviction_min_score', 3))
SHORT_CONVICTION_MAX_GREEN_PCT = float(ADAPTIVE_CFG.get('short_conviction_max_green_pct', 0.06))
SHORT_CONVICTION_MIN_ADVERSE_PCT = float(ADAPTIVE_CFG.get('short_conviction_min_adverse_pct', -0.08))
SHORT_CONVICTION_LOW_MFE_PCT = float(ADAPTIVE_CFG.get('short_conviction_low_mfe_pct', 0.12))
SHORT_CONVICTION_FLOW_BUY_PCT = float(ADAPTIVE_CFG.get('short_conviction_flow_buy_pct', 62.0))
SHORT_CONVICTION_FLOW_DELTA_PCT = float(ADAPTIVE_CFG.get('short_conviction_flow_delta_pct', 4.0))
SHORT_CONVICTION_BTC_MOM15_PCT = float(ADAPTIVE_CFG.get('short_conviction_btc_mom15_pct', 0.08))
SHORT_CONVICTION_BTC_MOM60_PCT = float(ADAPTIVE_CFG.get('short_conviction_btc_mom60_pct', 0.12))
LONG_CONVICTION_DECAY_ENABLED = bool(ADAPTIVE_CFG.get('long_conviction_decay_enabled', True))
LONG_CONVICTION_MIN_AGE_SEC = int(ADAPTIVE_CFG.get('long_conviction_min_age_sec', 30))
LONG_CONVICTION_MIN_SCORE = int(ADAPTIVE_CFG.get('long_conviction_min_score', 3))
LONG_CONVICTION_MAX_GREEN_PCT = float(ADAPTIVE_CFG.get('long_conviction_max_green_pct', 0.05))
LONG_CONVICTION_MIN_ADVERSE_PCT = float(ADAPTIVE_CFG.get('long_conviction_min_adverse_pct', -0.08))
LONG_CONVICTION_LOW_MFE_PCT = float(ADAPTIVE_CFG.get('long_conviction_low_mfe_pct', 0.12))
LONG_CONVICTION_FLOW_BUY_PCT = float(ADAPTIVE_CFG.get('long_conviction_flow_buy_pct', 40.0))
LONG_CONVICTION_FLOW_DELTA_PCT = float(ADAPTIVE_CFG.get('long_conviction_flow_delta_pct', -4.0))
LONG_CONVICTION_BTC_MOM15_PCT = float(ADAPTIVE_CFG.get('long_conviction_btc_mom15_pct', -0.08))
LONG_CONVICTION_BTC_MOM60_PCT = float(ADAPTIVE_CFG.get('long_conviction_btc_mom60_pct', -0.12))
SETUP_LOSS_PAUSE_COUNT = int(ADAPTIVE_CFG.get('setup_loss_pause_count', 2))
SETUP_LOSS_PAUSE_MIN = int(ADAPTIVE_CFG.get('setup_loss_pause_min', 90))
MARKET_REGIME_FILTER_ENABLED = bool(ADAPTIVE_CFG.get('market_regime_filter_enabled', True))
MAX_CONSECUTIVE_LOSSES = int(ADAPTIVE_CFG.get('max_consecutive_losses', 2))
RISK_OFF_COOLDOWN_MIN = int(ADAPTIVE_CFG.get('risk_off_cooldown_min', 30))
RISK_OFF_IGNORE_REASONS = set(ADAPTIVE_CFG.get('risk_off_ignore_reasons', [
    'manual_stop',
    'session_end',
    'external_broker_exit',
]))
MAX_DAILY_LOSS_PCT = float(ADAPTIVE_CFG.get('max_daily_loss_pct', 1.0))
DRAWDOWN_SIZE_CUT_PCT = float(ADAPTIVE_CFG.get('drawdown_size_cut_pct', 0.5))
DRAWDOWN_SIZE_CUT_TRADE_SIZE_PCT = float(ADAPTIVE_CFG.get('drawdown_size_cut_trade_size_pct', 0.12))
SETUP_SIZE_MULTIPLIER = ADAPTIVE_CFG.get('setup_size_multiplier', {})
LIVE_QUALITY_GATE_ENABLED = bool(SMART_ENTRY_CFG.get('live_quality_gate_enabled', True))
LIVE_QUALITY_MIN_SCORE = float(SMART_ENTRY_CFG.get('live_quality_min_score', 60))
PRE_SUBMIT_CHECK_ENABLED = bool(SMART_ENTRY_CFG.get('pre_submit_check_enabled', True))
PRE_SUBMIT_MAX_ADVERSE_MOVE_PCT = float(SMART_ENTRY_CFG.get('pre_submit_max_adverse_move_pct', 0.12))
PRE_SUBMIT_MAX_SPREAD_PCT = float(SMART_ENTRY_CFG.get('pre_submit_max_spread_pct', 0.12))
PRE_SUBMIT_MAX_QUOTE_AGE_SEC = float(SMART_ENTRY_CFG.get('pre_submit_max_quote_age_sec', 3.0))
BTC_IMPULSE_ABORT_ENABLED = bool(SMART_ENTRY_CFG.get('btc_impulse_abort_enabled', True))
BTC_IMPULSE_ABORT_15S_PCT = float(SMART_ENTRY_CFG.get('btc_impulse_abort_15s_pct', 0.18))
BTC_IMPULSE_ABORT_60S_PCT = float(SMART_ENTRY_CFG.get('btc_impulse_abort_60s_pct', 0.35))
BAD_SLIPPAGE_REDUCE_ENABLED = bool(ADAPTIVE_CFG.get('bad_slippage_reduce_enabled', True))
BAD_SLIPPAGE_THRESHOLD_PCT = float(ADAPTIVE_CFG.get('bad_slippage_threshold_pct', 0.12))
BAD_SLIPPAGE_COUNT = int(ADAPTIVE_CFG.get('bad_slippage_count', 2))
BAD_SLIPPAGE_LOOKBACK_TRADES = int(ADAPTIVE_CFG.get('bad_slippage_lookback_trades', 8))
BAD_SLIPPAGE_SIZE_MULTIPLIER = float(ADAPTIVE_CFG.get('bad_slippage_size_multiplier', 0.5))
EXECUTION_RISK_PAUSE_ENABLED = bool(ADAPTIVE_CFG.get('execution_risk_pause_enabled', True))
EXECUTION_RISK_PAUSE_MIN = int(ADAPTIVE_CFG.get('execution_risk_pause_min', 15))
EXECUTION_RISK_PAUSE_COUNT = int(ADAPTIVE_CFG.get('execution_risk_pause_count', 2))
EXECUTION_RISK_PAUSE_LOOKBACK_TRADES = int(ADAPTIVE_CFG.get('execution_risk_pause_lookback_trades', 6))
EXECUTION_RISK_PAUSE_ADVERSE_FILL_PCT = float(ADAPTIVE_CFG.get('execution_risk_pause_adverse_fill_pct', 0.12))
SHADOW_EXIT_POLICIES_ENABLED = bool(ADAPTIVE_CFG.get('shadow_exit_policies_enabled', True))
SHADOW_EXIT_POLICIES = ADAPTIVE_CFG.get('shadow_exit_policies', {})
HEALTH_HEARTBEAT_ENABLED = bool(TRADING_CONFIG.get('health_heartbeat_enabled', True))
HEALTH_HEARTBEAT_SEC = int(TRADING_CONFIG.get('health_heartbeat_sec', 30))
TICK_CAPTURE_CFG = TRADING_CONFIG.get('tick_capture', {}) or {}
FORENSIC_TICK_CAPTURE_ENABLED = bool(TICK_CAPTURE_CFG.get('enabled', True))
EXIT_TICK_CAPTURE_ENABLED = bool(TICK_CAPTURE_CFG.get('exit_enabled', True))
SKIPPED_TICK_CAPTURE_ENABLED = bool(TICK_CAPTURE_CFG.get('skipped_enabled', True))
SKIPPED_TICK_CAPTURE_MIN_SCORE = float(TICK_CAPTURE_CFG.get('skipped_min_score', 4.5))
TICK_CAPTURE_EXIT_PRE_SEC = int(TICK_CAPTURE_CFG.get('exit_pre_seconds', 120))
TICK_CAPTURE_EXIT_POST_SEC = int(TICK_CAPTURE_CFG.get('exit_post_seconds', 180))
TICK_CAPTURE_SKIPPED_PRE_SEC = int(TICK_CAPTURE_CFG.get('skipped_pre_seconds', 90))
TICK_CAPTURE_SKIPPED_POST_SEC = int(TICK_CAPTURE_CFG.get('skipped_post_seconds', 240))
MICROSTRUCTURE_CFG = TRADING_CONFIG.get('market_microstructure', {}) or {}
HALT_STALE_SEC = float(MICROSTRUCTURE_CFG.get('halt_stale_sec', 30.0))
LULD_SPREAD_RISK_PCT = float(MICROSTRUCTURE_CFG.get('luld_spread_risk_pct', 0.5))
LULD_STALE_SEC = float(MICROSTRUCTURE_CFG.get('luld_stale_sec', 12.0))
SSR_DROP_PCT = float(MICROSTRUCTURE_CFG.get('ssr_drop_pct', -10.0))
ASSET_INFO_TTL_SEC = float(MICROSTRUCTURE_CFG.get('asset_info_ttl_sec', 300.0))
BTC_PROXY_SYMBOLS = tuple(TRADING_CONFIG.get('btc_proxy_symbols', []))
MARKET_CONTEXT_SYMBOLS = ('SPY', 'QQQ', 'IWM') if MARKET_REGIME_FILTER_ENABLED else ()
CATALYST_FLAGS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   'postmortem', 'catalyst_flags.json')
TRANSACTION_COST_CFG = TRADING_CONFIG.get('transaction_costs', {}) or {}
SEC_FEE_RATE = float(TRANSACTION_COST_CFG.get('sec_fee_rate', 0.0000278))
FINRA_TAF_PER_SHARE = float(TRANSACTION_COST_CFG.get('finra_taf_per_share', 0.000166))
FINRA_TAF_CAP = float(TRANSACTION_COST_CFG.get('finra_taf_cap', 8.30))

# Wash-sale blackout windows (inclusive, America/Chicago dates).
# Last trading day before each window = normal hard-flat cutoff.
# No entries allowed, and any open positions get force-flat at window start.
# Unwinds ~11 months of deferred wash-sale losses so they recognize in-year.
WASH_SALE_BLACKOUTS = [
    ('2026-12-01', '2026-12-31'),  # last trade 2026-11-30, resume 2027-01-01+
]
WASH_SALE_BLACKOUTS = TRADING_CONFIG.get('wash_sale_blackouts', WASH_SALE_BLACKOUTS)

log = logging.getLogger('mock_trader')
log.setLevel(logging.INFO)
if not log.handlers:
    h = RotatingFileHandler(
        output_path('mock_trader.log'),
        maxBytes=2_000_000,
        backupCount=5,
        encoding='utf-8',
    )
    h.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    log.addHandler(h)


class MockTrader:
    def __init__(self, engine, state_path: str):
        self.engine      = engine
        self.state_path  = state_path
        self.lock        = threading.RLock()
        self.trader      = None

        # Alpaca paper executor (optional; controlled by USE_ALPACA_EXECUTION)
        if USE_ALPACA_EXECUTION and AlpacaTrader is not None:
            try:
                self.trader = AlpacaTrader.from_env(paper=True)
                acct = self.trader.get_account()
                log.info(f'Alpaca paper executor ready: status={acct.get("status")} '
                         f'cash=${acct.get("cash")} equity=${acct.get("equity")} '
                         f'shorting_enabled={acct.get("shorting_enabled")}')
                # Reconcile any orphaned Alpaca state on boot
            except Exception as e:
                log.error(f'Alpaca init failed, falling back to internal sim: {e}')
                self.trader = None
        else:
            log.info('Alpaca execution disabled — internal simulation only')

        self.state       = self._load() or self._fresh_state()
        self.state.setdefault('skipped_signals', [])
        self.state.setdefault('pending_entries', {})
        self.state.setdefault('setup_pauses', {})
        self.rollover_market_day_state(reason='startup')
        if execution_state_reducer is not None:
            try:
                execution_state_reducer.bootstrap_runtime_state(
                    self.state,
                    day=datetime.now(CT).date().isoformat(),
                )
            except Exception as e:
                log.warning(f'execution_state bootstrap failed: {e}')
        self._apply_emergency_state_block()
        self.state['startup_self_check'] = self._startup_self_check()
        if not self.state['startup_self_check'].get('ok'):
            failed = [
                row.get('name')
                for row in self.state['startup_self_check'].get('checks') or []
                if not row.get('ok')
            ]
            log.warning(f'Step2/live startup self-check WARN failed={failed}')
            if self.state['startup_self_check'].get('contract_gate', {}).get('enforced'):
                recovery = (
                    live_profile_recovery.recovery_recommendation(
                        TRADING_CONFIG,
                        reason='startup_contract_gate_failed',
                        status={'startup_self_check': self.state.get('startup_self_check')},
                    )
                    if live_profile_recovery is not None else None
                )
                self.state['running'] = False
                self.state['startup_contract_gate_block'] = {
                    'created_at': int(time.time()),
                    'failed_checks': failed,
                    'override': 'CLAUDE_ALLOW_CONTRACT_DRIFT=1',
                    'recovery': recovery,
                }
        else:
            self.state.pop('startup_contract_gate_block', None)
            profile = self.state['startup_self_check'].get('active_scoring_profile') or {}
            log.info(f'Step2/live startup self-check OK profile={profile.get("name")} '
                     f'hash={profile.get("hash")}')
        self._save()
        self._acct_cache: dict = {'ts': 0.0, 'data': None}  # 15 s TTL cache for get_account()
        self.execution_adapter = (
            execution_adapters.LiveExecutionAdapter(
                self.state,
                day=datetime.now(CT).date().isoformat(),
            )
            if execution_adapters is not None else None
        )
        self._avg_vol_cache: dict = {}   # tkr -> {'avg': float, 'prior_close': float, 'date': str}
        self._rvol_live:     dict = {}   # tkr -> float, updated each bar-loop tick
        self._spy_ind:       dict = {}   # latest SPY session indicators, updated each bar-loop tick
        self._market_tape:   dict = {}   # SPY/QQQ/IWM + BTC-proxy session tape, refreshed by monitor
        self._market_tape_bars: dict = {}  # sym -> {bar_ms -> bar}; avoids full-session refetches
        self._asset_info_cache: dict = {}  # tkr -> {'ts': float, 'data': dict}
        self._broker_api_failures = 0
        self._skipped_tick_capture_seen: dict = {}

        if self.trader is not None:
            self._reconcile_on_boot()
            self._recover_pending_broker_orders_on_boot()
            self._write_broker_safety_snapshot('startup')
            self.broker_lifecycle_gate(label='startup', enforce=True)

        # Subscribe the tickers via the shared engine.
        # First call uses subscribe(btc=True) so BTC crypto WS is enabled
        # (V2 signals require BTC correlation for full score); subsequent
        # tickers use add_ticker (additive, doesn't change active ticker).
        try:
            self.engine.subscribe(TICKERS[0], btc=True)
        except Exception as e:
            log.error(f'subscribe({TICKERS[0]}, btc=True) failed: {e}')
        for tkr in TICKERS[1:]:
            try:
                self.engine.add_ticker(tkr)
            except Exception as e:
                log.error(f'add_ticker({tkr}) failed: {e}')

        # Hook signal callback
        self.engine.on_signal(self._on_signal)

        # Monitor loop (SL/TP/time-stop/session-end)
        self._stop_evt = threading.Event()
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop, daemon=True, name='mock-monitor')
        self._monitor_thread.start()

        log.info(f'MockTrader started: balance=${self.state["balance"]:.2f} '
                 f'running={self.state["running"]} '
                 f'signal_source=ws_scalp')

    # ── state persistence ─────────────────────────────────────────────
    def _fresh_state(self) -> dict:
        # When live-trading against Alpaca, the realistic starting balance is
        # the actual account equity at reset — not the $1000 default.
        # total_pnl = current_balance - start_balance, so this must be
        # recorded at reset or total_pnl reports a garbage number.
        start = START_BALANCE
        if self.trader is not None:
            try:
                acct = self.trader.get_account()
                eq = float(acct.get('equity', 0) or 0)
                if eq > 0:
                    start = round(eq, 2)
            except Exception as e:
                log.warning(f'_fresh_state: Alpaca equity fetch failed, using default: {e}')
        state = {
            'running':          True,
            'balance':          start,
            'start_balance':    start,
            'positions':        {},   # ticker -> position dict
            'pending_entries':  {},   # reserved while broker order submit is in flight
            'trades':           [],   # closed trades, most recent appended
            'skipped_signals':   [],   # skipped setups with later fwd returns
            'setup_pauses':      {},   # setup key -> pause metadata
            'started_at':       int(time.time()),
        }
        if execution_state_reducer is not None:
            state['execution_state'] = execution_state_reducer.from_runtime_state(
                state,
                day=datetime.now(CT).date().isoformat(),
            )
        return state

    def _compact_skipped_row(self, row: dict) -> dict:
        keys = (
            'created_at', 'ticker', 'side', 'price', 'reason', 'conviction',
            'score', 'fwd_targets', 'fwd', 'strategy_config_hash',
        )
        return {k: row.get(k) for k in keys if k in row}

    def _state_for_disk(self) -> dict:
        """Persist a compact runtime state; full analytics rows live in JSONL/SQLite."""
        disk = dict(self.state)
        skipped = list(disk.get('skipped_signals') or [])
        trades = list(disk.get('trades') or [])
        disk['skipped_signals'] = [self._compact_skipped_row(r) for r in skipped[-200:]]
        disk['trades'] = trades[-500:]
        disk['_state_persistence'] = {
            'schema': 2,
            'saved_at': int(time.time()),
            'compact_skipped_cache_count': len(disk['skipped_signals']),
            'compact_trade_cache_count': len(disk['trades']),
            'full_skipped_corpus_dir': SKIPPED_DIR,
            'full_trade_corpus_dir': TRADE_CORPUS_DIR,
            'event_store': 'postmortem/trading_events.sqlite',
        }
        return disk

    def _load(self) -> Optional[dict]:
        try:
            with open(self.state_path, 'r', encoding='utf-8') as f:
                raw = f.read()
            if not raw.strip():
                return None
            data = json.loads(raw)
            data.setdefault('trades', [])
            data.setdefault('skipped_signals', [])
            return data
        except FileNotFoundError:
            return None
        except Exception as e:
            log.error(f'state load failed: {e}')
            return None

    def rollover_market_day_state(self, day_iso: Optional[str] = None,
                                  reason: str = 'manual',
                                  dry_run: bool = False) -> dict:
        """Apply the hard market-day boundary to transient decision state."""
        target_day = day_iso or datetime.now(CT).date().isoformat()
        if live_state_rollover is None:
            return {
                'ok': False,
                'reason': 'live_state_rollover_unavailable',
                'target_day': target_day,
            }
        with self.lock:
            proof = live_state_rollover.rollover_state(
                self.state,
                target_day,
                apply=not dry_run,
                reason=reason,
                actor='mock_trader.py',
            )
            cleared = []
            if not dry_run:
                for tkr in list(TICKERS):
                    try:
                        if hasattr(self.engine, 'clear_signal_cooldown'):
                            self.engine.clear_signal_cooldown(tkr)
                            cleared.append(tkr)
                    except Exception as e:
                        proof.setdefault('warnings', []).append(
                            f'clear_signal_cooldown_failed:{tkr}:{e}'
                        )
                proof['engine_cooldowns_cleared'] = cleared
                self.state.setdefault('state_rollover', {})['engine_cooldowns_cleared'] = cleared
                self._save()
        if not dry_run and target_day == datetime.now(CT).date().isoformat():
            self._maybe_snapshot_daily_budget()
            with self.lock:
                proof['verification_after_budget'] = live_state_rollover.rollover_state(
                    self.state,
                    target_day,
                    apply=False,
                    reason=f'{reason}_verify',
                    actor='mock_trader.py',
                )
        if not dry_run:
            try:
                proof_path = live_state_rollover.write_proof(proof)
                with self.lock:
                    self.state.setdefault('state_rollover', {})['proof_path'] = proof_path
                    self._save()
                proof['path'] = proof_path
            except Exception as e:
                proof.setdefault('warnings', []).append(f'write_proof_failed:{e}')
            try:
                self._audit_event('market_day_state_rollover', data={
                    'target_day': target_day,
                    'reason': reason,
                    'ok': proof.get('ok'),
                    'critical_failure_count': proof.get('critical_failure_count'),
                    'actions': proof.get('actions') or [],
                    'path': proof.get('path'),
                })
            except Exception:
                pass
        return proof

    def _save(self, strict: bool = False) -> bool:
        try:
            tmp = self.state_path + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as f:
                json.dump(self._state_for_disk(), f, separators=(',', ':'), default=str)
            os.replace(tmp, self.state_path)
            return True
        except Exception as e:
            log.error(f'state save failed: {e}')
            if strict:
                raise StatePersistenceError(str(e))
            return False

    def _write_emergency_state_block(self, payload: dict) -> None:
        os.makedirs(EMERGENCY_STATE_DIR, exist_ok=True)
        with open(EMERGENCY_STATE_BLOCK_PATH, 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=2, default=str)

    def _apply_emergency_state_block(self) -> None:
        if self.state.get('broker_exposure_block') or not os.path.exists(EMERGENCY_STATE_BLOCK_PATH):
            return
        try:
            with open(EMERGENCY_STATE_BLOCK_PATH, 'r', encoding='utf-8') as f:
                payload = json.load(f)
        except Exception as e:
            payload = {'reason': 'emergency_state_block_unreadable', 'error': str(e)}
        symbols = payload.get('symbols') if isinstance(payload, dict) else None
        with self.lock:
            self.state['running'] = False
            self.state['broker_exposure_block'] = {
                'created_at': int(time.time()),
                'symbols': sorted(set(symbols or TICKERS)),
                'reason': 'recovered_emergency_state_block',
                'source': EMERGENCY_STATE_BLOCK_PATH,
                'payload': payload,
            }

    def _critical_save(self, reason: str, symbols=None):
        try:
            self._save(strict=True)
            return
        except Exception as e:
            log.critical(f'CRITICAL_STATE_SAVE_FAILED {reason}: {e}')
            remaining = sorted(set(symbols or []))
            block_payload = {
                'created_at': int(time.time()),
                'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
                'symbols': remaining,
                'reason': f'state_persistence_failed:{reason}',
                'save_error': str(e),
                'state_path': self.state_path,
            }
            try:
                self._write_emergency_state_block(block_payload)
            except Exception as sidecar_err:
                log.critical(f'EMERGENCY_STATE_BLOCK_WRITE_FAILED {sidecar_err}')
            with self.lock:
                self.state['running'] = False
                self.state['broker_exposure_block'] = {
                    'created_at': int(time.time()),
                    'symbols': remaining,
                    'reason': f'state_persistence_failed:{reason}',
                    'save_error': str(e),
                }
                self._save()
            self._audit_event('critical_state_save_failed', data={
                'reason': reason,
                'symbols': remaining,
                'error': str(e),
            })
            raise

    def _audit_event(self, event: str, symbol: Optional[str] = None,
                     data: Optional[dict] = None):
        """Append a compact lifecycle event for broker/state forensics."""
        row = {
            'ts': int(time.time()),
            'ts_ct': datetime.now(CT).isoformat(timespec='seconds'),
            'event': event,
        }
        if symbol:
            row['symbol'] = symbol
        if data:
            row['data'] = data
        try:
            os.makedirs(AUDIT_DIR, exist_ok=True)
            audit_path = os.path.join(
                AUDIT_DIR,
                f'trade_lifecycle_{datetime.now(CT).date().isoformat()}.jsonl',
            )
            with open(audit_path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(row, separators=(',', ':'), default=str) + '\n')
            try:
                from event_store import record_event
                record_event('audit', row, symbol=symbol,
                             trade_id=(data or {}).get('trade_id') if isinstance(data, dict) else None,
                             ts=row['ts'])
            except Exception:
                pass
            if execution_lifecycle is not None:
                try:
                    execution_lifecycle.append_from_audit(row)
                except Exception:
                    pass
            if execution_state_reducer is not None:
                try:
                    execution_state_reducer.apply_audit_row(self.state, row)
                except Exception:
                    pass
        except Exception as e:
            log.warning(f'audit event write failed: {e}')

    def _stable_json_hash(self, payload: dict) -> str:
        try:
            blob = json.dumps(payload or {}, sort_keys=True, separators=(',', ':'), default=str)
        except Exception:
            blob = str(payload)
        return hashlib.sha256(blob.encode('utf-8')).hexdigest()

    def _active_profile_snapshot(self) -> dict:
        if step2_parity_contract is not None:
            return step2_parity_contract.active_profile_snapshot(TRADING_CONFIG, include_weights=True)
        profile = ((TRADING_CONFIG.get('smart_entry') or {}).get('active_scoring_profile') or {})
        weights = dict(profile.get('weights') or {})
        payload = {
            'enabled': bool(profile.get('enabled')),
            'name': profile.get('name'),
            'bias': profile.get('bias'),
            'weights': weights,
        }
        payload['hash'] = self._stable_json_hash(payload)
        payload['weight_count'] = len(weights)
        payload['promoted_at'] = profile.get('promoted_at')
        payload['promotion_reason'] = profile.get('promotion_reason')
        return payload

    def _active_profile_status_snapshot(self, full: bool = False) -> dict:
        profile = self._active_profile_snapshot()
        out = {
            'enabled': profile.get('enabled'),
            'name': profile.get('name'),
            'bias': profile.get('bias'),
            'hash': profile.get('hash'),
            'weight_count': profile.get('weight_count', len(profile.get('weights') or {})),
            'promoted_at': profile.get('promoted_at'),
            'promotion_reason': profile.get('promotion_reason'),
        }
        if full:
            out['weights'] = profile.get('weights') or {}
        return out

    def _promotion_provenance_status(self) -> dict:
        if promotion_manifest is None:
            return {
                'ok': False,
                'status': 'unavailable',
                'error': 'promotion_manifest module not imported',
            }
        try:
            return promotion_manifest.provenance_status(TRADING_CONFIG)
        except Exception as e:
            return {
                'ok': False,
                'status': 'error',
                'error': str(e),
            }

    def _config_provenance_status(self) -> dict:
        if config_change_journal is None:
            return {
                'ok': False,
                'status': 'unavailable',
                'error': 'config_change_journal module not imported',
            }
        try:
            return config_change_journal.provenance_status(TRADING_CONFIG)
        except Exception as e:
            return {
                'ok': False,
                'status': 'error',
                'error': str(e),
            }

    def _startup_self_check(self) -> dict:
        if step2_parity_contract is None:
            return {
                'schema_version': 1,
                'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
                'ok': False,
                'status': 'warn',
                'checks': [{'name': 'step2_parity_contract_imported', 'ok': False}],
            }
        payload = step2_parity_contract.live_parity_checks(TRADING_CONFIG)
        checks = list(payload.get('checks') or [])

        def add(name: str, ok: bool, actual=None, expected=None):
            row = {'name': name, 'ok': bool(ok)}
            if actual is not None:
                row['actual'] = actual
            if expected is not None:
                row['expected'] = expected
            checks.append(row)

        add(
            'live_constant.same_ticker_reentry_cooldown_matches_contract',
            STEP2_SAME_TICKER_REENTRY_COOLDOWN_SEC == int(
                STEP2_PARITY_CONTRACT.get('same_ticker_reentry_cooldown_sec') or 0
            ),
            STEP2_SAME_TICKER_REENTRY_COOLDOWN_SEC,
            STEP2_PARITY_CONTRACT.get('same_ticker_reentry_cooldown_sec'),
        )
        add(
            'live_constant.max_trades_per_day_matches_contract',
            STEP2_MAX_TRADES_PER_DAY == int(STEP2_PARITY_CONTRACT.get('max_trades_per_day') or 0),
            STEP2_MAX_TRADES_PER_DAY,
            STEP2_PARITY_CONTRACT.get('max_trades_per_day'),
        )
        add(
            'live_constant.max_trades_per_ticker_day_matches_contract',
            STEP2_MAX_TRADES_PER_TICKER_DAY == int(STEP2_PARITY_CONTRACT.get('max_trades_per_ticker_day') or 0),
            STEP2_MAX_TRADES_PER_TICKER_DAY,
            STEP2_PARITY_CONTRACT.get('max_trades_per_ticker_day'),
        )
        add('strategy_config_hash_present', bool(STRATEGY_CONFIG_HASH), STRATEGY_CONFIG_HASH)
        add('step2_execution_contract_hash_present', bool(STEP2_EXECUTION_CONTRACT_HASH), STEP2_EXECUTION_CONTRACT_HASH)
        reducer_check = None
        if execution_state_reducer is not None:
            try:
                reducer_check = execution_state_reducer.compare_runtime_view(self.state)
                add(
                    'execution_state_reducer_runtime_view_match',
                    bool(reducer_check.get('ok')),
                    reducer_check.get('mismatches'),
                    [],
                )
            except Exception as e:
                reducer_check = {'ok': False, 'error': str(e)}
                add('execution_state_reducer_runtime_view_match', False, str(e), 'no mismatch')
        gate = None
        if contract_gate is not None:
            try:
                gate = contract_gate.check(TRADING_CONFIG, day=datetime.now(CT).date().isoformat())
                add(
                    'hard_contract_gate_ok',
                    bool(gate.get('ok')),
                    gate.get('critical_failure_count'),
                    0,
                )
            except Exception as e:
                gate = {'ok': False, 'enforced': True, 'error': str(e)}
                add('hard_contract_gate_ok', False, str(e), 'ok')
        promotion_boot_gate = None
        if promotion_safety is not None:
            try:
                promotion_boot_gate = promotion_safety.active_profile_evidence_validation(TRADING_CONFIG)
                add(
                    'promotion_evidence_boot_gate_ok',
                    bool(promotion_boot_gate.get('ok')),
                    promotion_boot_gate.get('failed_count'),
                    0,
                )
            except Exception as e:
                promotion_boot_gate = {'ok': False, 'error': str(e)}
                add('promotion_evidence_boot_gate_ok', False, str(e), 'ok')
        promotion_manifest_gate = None
        if promotion_manifest is not None:
            try:
                promotion_manifest_gate = promotion_manifest.validate_live_profile(TRADING_CONFIG)
                add(
                    'promotion_manifest_integrity_ok',
                    bool(promotion_manifest_gate.get('ok')),
                    {
                        'status': promotion_manifest_gate.get('status'),
                        'legacy': promotion_manifest_gate.get('legacy'),
                        'critical_failure_count': promotion_manifest_gate.get('critical_failure_count'),
                    },
                    'ok',
                )
                add(
                    'promotion_manifest_not_legacy',
                    (not bool(promotion_manifest_gate.get('legacy')))
                    or os.environ.get('CLAUDE_ALLOW_LEGACY_PROMOTION_MANIFEST', '').strip().lower() in {'1', 'true', 'yes', 'on'},
                    {
                        'status': promotion_manifest_gate.get('status'),
                        'legacy': promotion_manifest_gate.get('legacy'),
                        'override_env': 'CLAUDE_ALLOW_LEGACY_PROMOTION_MANIFEST',
                    },
                    'non-legacy manifest',
                )
            except Exception as e:
                promotion_manifest_gate = {'ok': False, 'error': str(e)}
                add('promotion_manifest_integrity_ok', False, str(e), 'ok')
        config_journal_gate = None
        if config_change_journal is not None:
            try:
                config_journal_gate = config_change_journal.validate_live_config(TRADING_CONFIG)
                add(
                    'config_change_journal_integrity_ok',
                    bool(config_journal_gate.get('ok')),
                    {
                        'status': config_journal_gate.get('status'),
                        'legacy': config_journal_gate.get('legacy'),
                        'critical_failure_count': config_journal_gate.get('critical_failure_count'),
                    },
                    'ok',
                )
            except Exception as e:
                config_journal_gate = {'ok': False, 'error': str(e)}
                add('config_change_journal_integrity_ok', False, str(e), 'ok')
        ok = all(row.get('ok') for row in checks)
        payload['checks'] = checks
        payload['ok'] = ok
        payload['status'] = 'ok' if ok else 'warn'
        payload['strategy_config_hash'] = STRATEGY_CONFIG_HASH
        payload['step2_execution_contract_hash'] = STEP2_EXECUTION_CONTRACT_HASH
        payload['active_scoring_profile'] = self._active_profile_status_snapshot(full=False)
        if reducer_check is not None:
            payload['execution_state_reducer'] = reducer_check
        if gate is not None:
            payload['contract_gate'] = gate
        if promotion_boot_gate is not None:
            payload['promotion_evidence_boot_gate'] = promotion_boot_gate
        if promotion_manifest_gate is not None:
            payload['promotion_manifest_integrity'] = promotion_manifest_gate
        if config_journal_gate is not None:
            payload['config_change_journal_integrity'] = config_journal_gate
        return payload

    def _signal_parity_key(self, sig: dict, created_at: Optional[int] = None) -> str:
        ts = int(created_at or time.time())
        payload = {
            'ts': ts,
            'ticker': sig.get('ticker'),
            'side': sig.get('side'),
            'setup_type': sig.get('setup_type') or self._setup_type(sig),
            'score': sig.get('score'),
        }
        return self._stable_json_hash(payload)[:24]

    def _market_freshness_snapshot(self, tkr: str) -> dict:
        stock = self._state_snapshot(tkr) if tkr else {}
        btc = self._state_snapshot('BTC/USD')
        btc_ind = btc.get('indicators') or {}
        return {
            'stock': {
                'price': stock.get('price'),
                'bid': stock.get('bid'),
                'ask': stock.get('ask'),
                'spread_pct': stock.get('spread_pct'),
                'trade_age_sec': stock.get('age_sec') or stock.get('trade_age_sec'),
                'quote_age_sec': stock.get('quote_age_sec'),
                'last_price_source': stock.get('last_price_source') or stock.get('source'),
            },
            'btc': {
                'price': btc.get('price'),
                'trade_age_sec': btc.get('age_sec') or btc_ind.get('last_trade_age_sec'),
                'quote_age_sec': btc.get('quote_age_sec') or btc_ind.get('last_quote_age_sec'),
                'mom_15s': btc_ind.get('mom_15s'),
                'mom_60s': btc_ind.get('mom_60s'),
            },
        }

    def _signal_feature_snapshot(self, sig: dict, forensics: Optional[dict] = None) -> dict:
        payload = {
            'score': sig.get('score'),
            'conviction': sig.get('conviction'),
            'components': sig.get('components') or {},
            'indicators': sig.get('indicators') or {},
            'btc_indicators': sig.get('btc_indicators') or {},
            'btc_context': sig.get('btc_context') or {},
            'signal_quality': sig.get('signal_quality') or {},
            'relative_strength': sig.get('relative_strength') or {},
            'miner_basket': sig.get('miner_basket') or {},
            'lead_lag': sig.get('lead_lag') or {},
            'forensics': forensics or {},
        }
        return {'hash': self._stable_json_hash(payload), 'payload': payload}

    def _append_live_signal_parity(self, sig: dict, decision: str, reason: str,
                                   created_at: Optional[int] = None,
                                   trade_id: Optional[str] = None,
                                   client_order_id: Optional[str] = None,
                                   broker_order_id: Optional[str] = None,
                                   forensics: Optional[dict] = None,
                                   latency_chain: Optional[dict] = None,
                                   extra: Optional[dict] = None) -> Optional[dict]:
        try:
            tkr = sig.get('ticker')
            side = sig.get('side')
            price = sig.get('price')
            if tkr not in TICKERS or side not in ('LONG', 'SHORT') or not price:
                return None
            ts = int(created_at or time.time())
            profile = self._active_profile_snapshot()
            features = self._signal_feature_snapshot(sig, forensics)
            row = {
                'schema_version': 1,
                'created_at': ts,
                'created_at_ct': datetime.fromtimestamp(ts, CT).isoformat(timespec='seconds'),
                'parity_key': self._signal_parity_key(sig, ts),
                'ticker': tkr,
                'side': side,
                'setup_type': sig.get('setup_type') or self._setup_type(sig),
                'decision': decision,
                'reason': reason,
                'price': round(float(price), 4),
                'score': sig.get('score'),
                'conviction': sig.get('conviction'),
                'reasons': sig.get('reasons', []),
                'trade_id': trade_id,
                'client_order_id': client_order_id,
                'broker_order_id': broker_order_id,
                'execution_mode': EXECUTION_MODE,
                'strategy_config_hash': STRATEGY_CONFIG_HASH,
                'execution_kernel_hash': EXECUTION_KERNEL_HASH,
                'step2_parity_contract_hash': STEP2_PARITY_CONTRACT_HASH,
                'step2_execution_contract_hash': STEP2_EXECUTION_CONTRACT_HASH,
                'active_profile_name': profile.get('name'),
                'active_profile_hash': profile.get('hash'),
                'active_profile_bias': profile.get('bias'),
                'active_profile_weights': profile.get('weights'),
                'feature_snapshot_hash': features.get('hash'),
                'feature_snapshot': features.get('payload'),
                'market_freshness': self._market_freshness_snapshot(tkr),
                'latency_chain': latency_chain or {},
                'operational_state': {
                    'running': bool(self.state.get('running')),
                    'kill_switch': self.state.get('kill_switch') or {},
                    'broker_api_degraded': self.state.get('broker_api_degraded'),
                    'positions': sorted((self.state.get('positions') or {}).keys()),
                    'pending_entries': sorted((self.state.get('pending_entries') or {}).keys()),
                    'in_entry_window': self._in_entry_window(),
                    'in_trading_window': self._in_trading_window(),
                },
                'join_hint': {
                    'ticker': tkr,
                    'side': side,
                    'timestamp_second': ts,
                    'setup_type': sig.get('setup_type') or self._setup_type(sig),
                    'score': sig.get('score'),
                },
            }
            if extra:
                row['extra'] = extra
            if canonical_decision_packet is not None:
                try:
                    packet = canonical_decision_packet.from_live_signal_parity(row)
                    row['canonical_decision_packet_id'] = packet.get('packet_id')
                    canonical_decision_packet.append_packet(packet)
                except Exception as packet_err:
                    log.warning(f'canonical live decision packet write failed: {packet_err}')
            os.makedirs(LIVE_SIGNAL_PARITY_DIR, exist_ok=True)
            day = datetime.fromtimestamp(ts, CT).date().isoformat()
            path = os.path.join(LIVE_SIGNAL_PARITY_DIR, f'live_signal_parity_{day}.jsonl')
            with open(path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(row, separators=(',', ':'), default=str) + '\n')
            try:
                from event_store import record_event
                record_event('live_signal_parity', row, symbol=tkr, trade_id=trade_id, ts=ts)
            except Exception:
                pass
            if canonical_opportunity_ledger is not None:
                try:
                    canonical_opportunity_ledger.append_live_decision(row)
                except Exception:
                    pass
            if live_decision_ledger is not None:
                try:
                    live_decision_ledger.append(live_decision_ledger.from_live_signal_parity(row))
                except Exception:
                    pass
            if intraday_shadow_step2 is not None:
                try:
                    row['shadow_step2'] = intraday_shadow_step2.append_live_row(row)
                except Exception:
                    pass
            if shadow_variant_engine is not None:
                try:
                    row['shadow_variants'] = shadow_variant_engine.append_live_row(row)
                except Exception:
                    pass
            return row
        except Exception as e:
            log.warning(f'live signal parity write failed: {e}')
            return None

    def _safe_reason_token(self, reason: Optional[str], max_len: int = 42) -> str:
        raw = str(reason or 'unknown')
        token = ''.join(ch if ch.isalnum() or ch in ('-', '_') else '_' for ch in raw)
        return token[:max_len] or 'unknown'

    def _start_forensic_tick_capture(self, tkr: str, label: str, signal: dict,
                                     capture_id: str, pre_seconds: int,
                                     post_seconds: int):
        if not FORENSIC_TICK_CAPTURE_ENABLED:
            return
        try:
            if not hasattr(self.engine, 'start_tick_capture'):
                return
            self.engine.start_tick_capture(
                tkr,
                label=label,
                signal=signal,
                capture_id=capture_id,
                pre_seconds=pre_seconds,
                post_seconds=post_seconds,
            )
        except Exception as e:
            log.warning(f'forensic tick capture start failed {tkr} {label}: {e}')

    def _start_exit_tick_capture(self, tkr: str, pos: Optional[dict],
                                 price: Optional[float], reason: str):
        if pos is not None:
            self._mark_exit_latency(pos, 'exit_trigger_ms')
        if not EXIT_TICK_CAPTURE_ENABLED or not pos:
            return
        if pos.get('exit_tick_capture_started'):
            return
        pos['exit_tick_capture_started'] = True
        side = pos.get('side')
        reason_token = self._safe_reason_token(reason)
        capture_id = (
            f"exit-{pos.get('trade_id') or tkr}-{reason_token}-"
            f"{int(time.time() * 1000)}"
        )
        signal = {
            'capture_type': 'exit',
            'ticker': tkr,
            'side': side,
            'price': price or pos.get('entry'),
            'exit_reason': reason,
            'trade_id': pos.get('trade_id'),
            'entry_price': pos.get('entry'),
            'entry_ts': pos.get('entry_ts'),
            'exit_decision_context': pos.get('exit_decision_context'),
            'exit_reason_hierarchy': pos.get('exit_reason_hierarchy'),
            'entry_thesis': pos.get('entry_thesis'),
            'market_tape_at_entry': pos.get('market_tape_at_entry'),
            'strategy_config_hash': pos.get('strategy_config_hash') or STRATEGY_CONFIG_HASH,
        }
        self._start_forensic_tick_capture(
            tkr,
            label=f'EXIT_{side}_{reason_token}',
            signal=signal,
            capture_id=capture_id,
            pre_seconds=TICK_CAPTURE_EXIT_PRE_SEC,
            post_seconds=TICK_CAPTURE_EXIT_POST_SEC,
        )
        self._audit_event('exit_tick_capture_started', tkr, {
            'trade_id': pos.get('trade_id'),
            'side': side,
            'reason': reason,
            'capture_id': capture_id,
        })

    def _entry_rule_near_misses(self, sig: dict, forensics: dict,
                                entry_quality_tier: dict) -> list[dict]:
        out = []
        side = sig.get('side')
        setup = sig.get('setup_type') or self._setup_type(sig)
        score_abs = abs(float(sig.get('score') or 0))
        try:
            min_score = float(MIN_SCORE_BY_SETUP.get(setup, 4))
            if 0 <= score_abs - min_score <= 1.0:
                out.append({
                    'rule': 'setup_min_score',
                    'distance': round(score_abs - min_score, 3),
                    'threshold': min_score,
                    'value': score_abs,
                    'deduction': 'Entry barely cleared setup score threshold.',
                })
        except Exception:
            pass
        eq = forensics.get('entry_quality') or {}
        exec_score = ((sig.get('execution_quality') or {}).get('score')
                      or entry_quality_tier.get('score'))
        try:
            if exec_score is not None:
                exec_score_f = float(exec_score)
                if 0 <= exec_score_f - LIVE_QUALITY_MIN_SCORE <= 10:
                    out.append({
                        'rule': 'live_quality_gate',
                        'distance': round(exec_score_f - LIVE_QUALITY_MIN_SCORE, 3),
                        'threshold': LIVE_QUALITY_MIN_SCORE,
                        'value': exec_score_f,
                        'deduction': 'Execution quality was close to minimum acceptable quality.',
                    })
        except Exception:
            pass
        spread_pct = eq.get('spread_pct')
        try:
            if spread_pct is not None:
                spread_pct_f = float(spread_pct)
                for rule, threshold in (
                    ('spread_elevated_gate', SPREAD_GATE_ELEVATED_PCT),
                    ('spread_hard_gate', SPREAD_GATE_HARD_PCT),
                    ('pre_submit_spread_gate', PRE_SUBMIT_MAX_SPREAD_PCT),
                ):
                    if threshold and 0 <= threshold - spread_pct_f <= 0.025:
                        out.append({
                            'rule': rule,
                            'distance': round(threshold - spread_pct_f, 4),
                            'threshold': threshold,
                            'value': spread_pct_f,
                            'deduction': 'Spread was close to a block/retry threshold.',
                        })
        except Exception:
            pass
        quote_age = eq.get('quote_age_sec')
        try:
            if quote_age is not None:
                quote_age_f = float(quote_age)
                if 0 <= PRE_SUBMIT_MAX_QUOTE_AGE_SEC - quote_age_f <= 1.0:
                    out.append({
                        'rule': 'pre_submit_quote_age',
                        'distance': round(PRE_SUBMIT_MAX_QUOTE_AGE_SEC - quote_age_f, 3),
                        'threshold': PRE_SUBMIT_MAX_QUOTE_AGE_SEC,
                        'value': quote_age_f,
                        'deduction': 'Quote freshness was close to stale threshold.',
                    })
        except Exception:
            pass
        btc = sig.get('btc_context') or {}
        b15 = btc.get('mom_15s')
        b60 = btc.get('mom_60s')
        try:
            if side == 'LONG':
                if b15 is not None and 0 <= float(b15) + BTC_IMPULSE_ABORT_15S_PCT <= 0.05:
                    out.append({'rule': 'btc_impulse_abort_15s', 'value': b15,
                                'threshold': -BTC_IMPULSE_ABORT_15S_PCT,
                                'deduction': 'BTC was near the 15s adverse impulse abort line.'})
                if b60 is not None and 0 <= float(b60) + BTC_IMPULSE_ABORT_60S_PCT <= 0.08:
                    out.append({'rule': 'btc_impulse_abort_60s', 'value': b60,
                                'threshold': -BTC_IMPULSE_ABORT_60S_PCT,
                                'deduction': 'BTC was near the 60s adverse impulse abort line.'})
            elif side == 'SHORT':
                if b15 is not None and 0 <= BTC_IMPULSE_ABORT_15S_PCT - float(b15) <= 0.05:
                    out.append({'rule': 'btc_impulse_abort_15s', 'value': b15,
                                'threshold': BTC_IMPULSE_ABORT_15S_PCT,
                                'deduction': 'BTC was near the 15s adverse impulse abort line.'})
                if b60 is not None and 0 <= BTC_IMPULSE_ABORT_60S_PCT - float(b60) <= 0.08:
                    out.append({'rule': 'btc_impulse_abort_60s', 'value': b60,
                                'threshold': BTC_IMPULSE_ABORT_60S_PCT,
                                'deduction': 'BTC was near the 60s adverse impulse abort line.'})
        except Exception:
            pass
        quality = sig.get('signal_quality') or {}
        if quality.get('btc_conflict') is False and quality.get('flow_fade_confirmed') is False:
            out.append({
                'rule': 'stronger_confirmation',
                'value': 'flow_fade_unconfirmed',
                'deduction': 'Entry was valid, but one stronger confirmation flag was not present.',
            })
        return out[:10]

    def _maybe_capture_skipped_signal_ticks(self, row: dict, sig: dict, reason: str):
        if not SKIPPED_TICK_CAPTURE_ENABLED or not FORENSIC_TICK_CAPTURE_ENABLED:
            return
        try:
            score = abs(float(row.get('score') or 0))
        except Exception:
            score = 0.0
        if score < SKIPPED_TICK_CAPTURE_MIN_SCORE:
            return
        tkr = row.get('ticker')
        side = row.get('side')
        if tkr not in TICKERS or side not in ('LONG', 'SHORT'):
            return
        now = time.time()
        reason_token = self._safe_reason_token(reason)
        key = f'{tkr}:{side}:{reason_token}'
        if now - float(self._skipped_tick_capture_seen.get(key, 0) or 0) < 60:
            return
        self._skipped_tick_capture_seen[key] = now
        capture_id = f"skip-{tkr}-{side}-{reason_token}-{int(now * 1000)}"
        signal = dict(sig)
        signal.update({
            'capture_type': 'skipped',
            'skip_reason': reason,
            'skip_row': {
                'created_at': row.get('created_at'),
                'score': row.get('score'),
                'conviction': row.get('conviction'),
                'reason': reason,
            },
            'strategy_config_hash': STRATEGY_CONFIG_HASH,
        })
        self._start_forensic_tick_capture(
            tkr,
            label=f'SKIP_{side}_{reason_token}',
            signal=signal,
            capture_id=capture_id,
            pre_seconds=TICK_CAPTURE_SKIPPED_PRE_SEC,
            post_seconds=TICK_CAPTURE_SKIPPED_POST_SEC,
        )

    def _append_skipped_corpus(self, row: dict):
        try:
            os.makedirs(SKIPPED_DIR, exist_ok=True)
            day = datetime.fromtimestamp(row.get('created_at', time.time()), CT).date().isoformat()
            path = os.path.join(SKIPPED_DIR, f'skipped_signals_{day}.jsonl')
            with open(path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(row, separators=(',', ':'), default=str) + '\n')
        except Exception as e:
            log.warning(f'skipped corpus write failed: {e}')

    def _append_trade_corpus(self, trade: dict):
        try:
            os.makedirs(TRADE_CORPUS_DIR, exist_ok=True)
            day = datetime.fromtimestamp(trade.get('closed_at') or time.time(), CT).date().isoformat()
            path = os.path.join(TRADE_CORPUS_DIR, f'trades_{day}.jsonl')
            with open(path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(trade, separators=(',', ':'), default=str) + '\n')
        except Exception as e:
            log.warning(f'trade corpus write failed: {e}')

    def _append_shadow_decision(self, sig: dict, decision: str, reason: str,
                                forensics: Optional[dict] = None,
                                extra: Optional[dict] = None):
        try:
            price = sig.get('price')
            tkr = sig.get('ticker')
            side = sig.get('side')
            if tkr not in TICKERS or side not in ('LONG', 'SHORT') or not price:
                return
            now = int(time.time())
            row = {
                'created_at': now,
                'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
                'ticker': tkr,
                'side': side,
                'decision': decision,
                'reason': reason,
                'price': round(float(price), 4),
                'conviction': sig.get('conviction'),
                'score': sig.get('score'),
                'setup_type': sig.get('setup_type') or self._setup_type(sig),
                'strategy_config_hash': STRATEGY_CONFIG_HASH,
                'reasons': sig.get('reasons', []),
                'signal_quality': sig.get('signal_quality') or {},
                'relative_strength': sig.get('relative_strength') or {},
                'forensics': forensics,
                'fwd_targets': {
                    '1m': now + 60,
                    '3m': now + 180,
                    '5m': now + 300,
                    '10m': now + 600,
                    '15m': now + 900,
                },
            }
            if extra:
                row['extra'] = extra
            os.makedirs(SHADOW_DIR, exist_ok=True)
            day = datetime.fromtimestamp(now, CT).date().isoformat()
            path = os.path.join(SHADOW_DIR, f'shadow_decisions_{day}.jsonl')
            with open(path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(row, separators=(',', ':'), default=str) + '\n')
        except Exception as e:
            log.warning(f'shadow decision write failed: {e}')

    def _append_decision_audit(self, row: dict):
        try:
            os.makedirs(DECISION_AUDIT_DIR, exist_ok=True)
            day = datetime.fromtimestamp(row.get('created_at', time.time()), CT).date().isoformat()
            path = os.path.join(DECISION_AUDIT_DIR, f'decision_audits_{day}.jsonl')
            with open(path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(row, separators=(',', ':'), default=str) + '\n')
        except Exception as e:
            log.warning(f'decision audit write failed: {e}')

    def _append_shadow_exit(self, row: dict):
        try:
            os.makedirs(SHADOW_EXIT_DIR, exist_ok=True)
            day = datetime.fromtimestamp(row.get('created_at', time.time()), CT).date().isoformat()
            path = os.path.join(SHADOW_EXIT_DIR, f'shadow_exits_{day}.jsonl')
            with open(path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(row, separators=(',', ':'), default=str) + '\n')
            try:
                from event_store import record_event
                record_event('shadow_exit', row, symbol=row.get('ticker'),
                             ts=row.get('created_at'))
            except Exception:
                pass
        except Exception as e:
            log.warning(f'shadow exit write failed: {e}')

    def _append_entry_retry_candidate(self, sig: dict, reason: str, signal_price: float):
        try:
            now = int(time.time())
            tkr = sig.get('ticker')
            side = sig.get('side')
            stock = self._state_snapshot(tkr)
            row = {
                'created_at': now,
                'created_at_ct': datetime.fromtimestamp(now, CT).isoformat(timespec='seconds'),
                'ticker': tkr,
                'side': side,
                'reason': reason,
                'signal_price': round(float(signal_price), 4) if signal_price else None,
                'live_price': stock.get('price'),
                'spread_pct': stock.get('spread_pct'),
                'quote_age_sec': stock.get('quote_age_sec'),
                'score': sig.get('score'),
                'conviction': sig.get('conviction'),
                'setup_type': sig.get('setup_type') or self._setup_type(sig),
                'btc_context': sig.get('btc_context') or {},
                'indicators': sig.get('indicators') or {},
                'suggested_recheck_window_sec': [3, 10],
                'mode': 'passive_log_only',
                'note': 'Signal was blocked by transient execution quality; no retry order was submitted.',
            }
            os.makedirs(ENTRY_RETRY_DIR, exist_ok=True)
            day = datetime.fromtimestamp(now, CT).date().isoformat()
            path = os.path.join(ENTRY_RETRY_DIR, f'entry_retry_candidates_{day}.jsonl')
            with open(path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(row, separators=(',', ':'), default=str) + '\n')
            try:
                from event_store import record_event
                record_event('entry_retry_candidate', row, symbol=tkr, ts=now)
            except Exception:
                pass
        except Exception as e:
            log.warning(f'entry retry candidate write failed: {e}')

    def _append_health_heartbeat(self):
        if not HEALTH_HEARTBEAT_ENABLED:
            return
        try:
            now = int(time.time())
            ticker_freshness = {}
            for sym in list(TICKERS) + ['BTC/USD']:
                try:
                    st = getattr(self.engine, 'states', {}).get(sym)
                    ts_ms = getattr(st, 'last_trade_ts_ms', None) if st else None
                    qts_ms = getattr(st, 'last_quote_ts_ms', None) if st else None
                    ticker_freshness[sym] = {
                        'price': self.engine.get_last_price(sym),
                        'age_sec': round((now * 1000 - ts_ms) / 1000, 3) if ts_ms else None,
                        'quote_age_sec': round((now * 1000 - qts_ms) / 1000, 3) if qts_ms else None,
                    }
                except Exception:
                    ticker_freshness[sym] = {'price': None, 'age_sec': None, 'quote_age_sec': None}
            feed_health = self._feed_health(ticker_freshness)
            signal_loop_health = {}
            try:
                signal_loop_health = self.engine._signal_loop_health()
            except Exception:
                signal_loop_health = {'status': 'unknown'}
            with self.lock:
                positions = list((self.state.get('positions') or {}).keys())
                pending = list((self.state.get('pending_entries') or {}).keys())
                broker_block = self.state.get('broker_exposure_block')
                broker_lifecycle_block = self.state.get('broker_lifecycle_block')
                running = self.state.get('running')
                broker_api_degraded = self.state.get('broker_api_degraded')
                shadow_fired = sum(
                    len((p or {}).get('shadow_exit_fired') or {})
                    for p in (self.state.get('positions') or {}).values()
                )
            row = {
                'created_at': now,
                'created_at_ct': datetime.fromtimestamp(now, CT).isoformat(timespec='seconds'),
                'running': running,
                'in_window': self._in_trading_window(),
                'positions': positions,
                'pending_entries': pending,
                'broker_exposure_block': broker_block,
                'broker_lifecycle_block': broker_lifecycle_block,
                'broker_api_degraded': broker_api_degraded,
                'kill_switch': self._kill_switch_state(),
                'ticker_freshness': ticker_freshness,
                'feed_health': feed_health,
                'signal_loop_health': signal_loop_health,
                'shadow_exit_count_open_positions': shadow_fired,
                'strategy_config_hash': STRATEGY_CONFIG_HASH,
            }
            os.makedirs(HEALTH_HEARTBEAT_DIR, exist_ok=True)
            day = datetime.fromtimestamp(now, CT).date().isoformat()
            path = os.path.join(HEALTH_HEARTBEAT_DIR, f'health_heartbeats_{day}.jsonl')
            with open(path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(row, separators=(',', ':'), default=str) + '\n')
        except Exception as e:
            log.warning(f'health heartbeat write failed: {e}')

    def _feed_health(self, ticker_freshness: Optional[dict] = None) -> dict:
        now = int(time.time())
        rows = ticker_freshness or {}
        in_window = self._in_trading_window()
        if not rows:
            for sym in list(TICKERS) + ['BTC/USD']:
                snap = self._state_snapshot(sym)
                rows[sym] = {
                    'price': snap.get('price'),
                    'age_sec': snap.get('trade_age_sec'),
                    'quote_age_sec': snap.get('quote_age_sec'),
                }
        stale = []
        missing = []
        for sym, row in rows.items():
            age = row.get('age_sec')
            quote_age = row.get('quote_age_sec')
            if row.get('price') is None:
                missing.append(sym)
            elif age is not None and age > (20 if sym == 'BTC/USD' else 10):
                if quote_age is None or quote_age > (10 if sym == 'BTC/USD' else 10):
                    stale.append(sym)
        status = 'ok' if not stale and not missing else 'degraded'
        reason = 'all_symbols_fresh'
        if status == 'degraded':
            reason = 'missing_or_stale_symbols'
        if not in_window and status == 'degraded':
            stock_symbols = set(TICKERS)
            stock_missing = [sym for sym in missing if sym in stock_symbols]
            stock_stale = [sym for sym in stale if sym in stock_symbols]
            non_stock_issues = [
                sym for sym in missing + stale
                if sym not in stock_symbols
            ]
            if (stock_missing or stock_stale) and not non_stock_issues:
                status = 'outside_session_quiet'
                reason = 'stock_feed_inactive_outside_trading_window'
        return {
            'created_at': now,
            'status': status,
            'reason': reason,
            'in_window': in_window,
            'missing': missing,
            'stale': stale,
            'symbols': rows,
            'broker_api_degraded': self.state.get('broker_api_degraded'),
        }

    def _write_broker_safety_snapshot(self, label: str):
        if self.trader is None:
            return
        try:
            from weekend_readiness import write_broker_safety_snapshot
            day = datetime.now(CT).date().isoformat()
            path, payload = write_broker_safety_snapshot(day, label=label)
            log.info(f'broker safety snapshot written: {path}')
            self._clear_block_if_snapshot_flat(payload, f'snapshot:{label}', path)
        except Exception as e:
            log.warning(f'broker safety snapshot failed ({label}): {e}')

    def broker_lifecycle_gate(self, label: str = 'manual', enforce: bool = False) -> dict:
        """Verify watched broker orders/positions map to local lifecycle state."""
        if broker_lifecycle_guard is None:
            payload = {
                'schema_version': 1,
                'source': 'mock_trader.broker_lifecycle_gate',
                'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
                'label': label,
                'ok': True,
                'verdict': 'BROKER_LIFECYCLE_SKIPPED',
                'warning_count': 1,
                'critical_count': 0,
                'issues': [{'level': 'warning', 'kind': 'broker_lifecycle_guard_import_failed', 'detail': {}}],
                'enforced': bool(enforce),
            }
        else:
            with self.lock:
                state_snapshot = {
                    'positions': json.loads(json.dumps(self.state.get('positions') or {})),
                    'pending_entries': json.loads(json.dumps(self.state.get('pending_entries') or {})),
                }
            broker = broker_lifecycle_guard.fetch_broker_snapshot(self.trader, list(TICKERS))
            payload = broker_lifecycle_guard.evaluate_runtime_broker_state(
                state_snapshot,
                broker_positions=broker.get('watched_positions') or [],
                broker_orders=broker.get('watched_open_orders') or [],
                watched=list(TICKERS),
                day=datetime.now(CT).date().isoformat(),
                broker_reachable=bool(broker.get('reachable')),
                broker_error=broker.get('error'),
                label=label,
            )
            payload['enforced'] = bool(enforce)

        slim = {
            'created_at_ct': payload.get('created_at_ct'),
            'label': label,
            'ok': bool(payload.get('ok')),
            'verdict': payload.get('verdict'),
            'critical_count': payload.get('critical_count'),
            'warning_count': payload.get('warning_count'),
            'issue_kind_counts': payload.get('issue_kind_counts') or {},
            'scorecard': payload.get('scorecard') or {},
            'issues': (payload.get('issues') or [])[:20],
            'repair_recommendations': (payload.get('repair_recommendations') or [])[:10],
            'enforced': bool(enforce),
        }
        block = None
        if enforce and not payload.get('ok'):
            symbols = sorted({
                str((issue.get('detail') or {}).get('symbol') or '').upper()
                for issue in (payload.get('issues') or [])
                if (issue.get('detail') or {}).get('symbol')
            }) or list(TICKERS)
            block = {
                'created_at': int(time.time()),
                'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
                'label': label,
                'symbols': symbols,
                'reason': 'broker_lifecycle_gate_failed',
                'verdict': payload.get('verdict'),
                'issues': (payload.get('issues') or [])[:20],
                'repair_recommendations': (payload.get('repair_recommendations') or [])[:10],
            }
        with self.lock:
            self.state['broker_lifecycle_gate'] = slim
            if block:
                self.state['running'] = False
                self.state['broker_lifecycle_block'] = block
            elif payload.get('ok'):
                self.state.pop('broker_lifecycle_block', None)
            self._save()
        self._audit_event(
            'broker_lifecycle_block' if block else 'broker_lifecycle_gate',
            data={'label': label, 'ok': payload.get('ok'), 'verdict': payload.get('verdict'), 'block': block},
        )
        if block:
            log.error(f'BROKER_LIFECYCLE_BLOCK {label}: {block}')
        return payload

    def _snapshot_proves_flat(self, payload: dict, symbols: Optional[list[str]] = None) -> bool:
        """Return True only when a broker safety snapshot proves watched exposure is flat."""
        if not isinstance(payload, dict):
            return False
        broker = payload.get('broker') or {}
        local = payload.get('local') or {}
        verdict = payload.get('verdict') or {}
        if broker.get('reachable') is not True:
            return False
        if verdict.get('flat_watched') is not True or verdict.get('no_watched_open_orders') is not True:
            return False
        if verdict.get('no_local_positions') is not True or verdict.get('no_pending_entries') is not True:
            return False
        watched_positions = broker.get('watched_positions') or []
        watched_orders = broker.get('watched_open_orders') or []
        local_positions = local.get('positions') or {}
        pending_entries = local.get('pending_entries') or {}
        if watched_positions or watched_orders or local_positions or pending_entries:
            return False
        wanted = set(symbols or [])
        if wanted:
            watched = set(payload.get('watched') or [])
            if not wanted.issubset(watched):
                return False
        return True

    def _clear_block_if_snapshot_flat(self, payload: dict, source: str, path: Optional[str] = None) -> bool:
        with self.lock:
            block = dict(self.state.get('broker_exposure_block') or {})
        if not block:
            return False
        symbols = sorted(set(block.get('symbols') or [])) or list(TICKERS)
        if not self._snapshot_proves_flat(payload, symbols):
            return False
        self._clear_broker_exposure_block()
        self._audit_event('broker_exposure_block_cleared', data={
            'force': False,
            'symbols': symbols,
            'prior_block': block,
            'source': source,
            'snapshot_path': path,
        })
        log.warning(f'broker_exposure_block cleared from flat broker safety snapshot '
                    f'source={source} symbols={symbols}')
        return True

    def _latest_flat_broker_safety_snapshot(self, symbols: list[str],
                                            max_age_sec: int = 900) -> Optional[dict]:
        """Find a recent same-day broker safety snapshot that proves flat exposure."""
        snapshot_dir = output_path('postmortem', 'broker_safety_snapshots')
        day = datetime.now(CT).date().isoformat()
        pattern = os.path.join(snapshot_dir, f'broker_safety_{day}_*.json')
        try:
            paths = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
        except Exception:
            paths = []
        now = time.time()
        for path in paths:
            try:
                if now - os.path.getmtime(path) > max_age_sec:
                    continue
                with open(path, 'r', encoding='utf-8') as f:
                    payload = json.load(f)
                if self._snapshot_proves_flat(payload, symbols):
                    return {'path': path, 'payload': payload}
            except Exception as e:
                log.warning(f'broker safety snapshot flat fallback read failed {path}: {e}')
        return None

    def _kill_switch_state(self) -> dict:
        try:
            with open(KILL_SWITCH_PATH, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except FileNotFoundError:
            return {}
        except Exception as e:
            log.warning(f'kill switch read failed: {e}')
            return {}

    def _kill_switch_active(self) -> bool:
        return bool(self._kill_switch_state().get('enabled'))

    def set_kill_switch(self, enabled: bool, reason: str = 'manual'):
        data = {
            'enabled': bool(enabled),
            'reason': reason,
            'updated_at': int(time.time()),
            'updated_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        }
        with open(KILL_SWITCH_PATH, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        self._audit_event('kill_switch_updated', data=data)

    def _record_broker_api_success(self):
        if self._broker_api_failures:
            self._audit_event('broker_api_recovered', data={'failures': self._broker_api_failures})
        self._broker_api_failures = 0
        with self.lock:
            if self.state.get('broker_api_degraded'):
                self.state.pop('broker_api_degraded', None)
                self._save()

    def _record_broker_api_failure(self, where: str, error: Exception):
        self._broker_api_failures += 1
        data = {
            'where': where,
            'error': str(error),
            'failure_count': self._broker_api_failures,
        }
        self._audit_event('broker_api_failure', data=data)
        if self._broker_api_failures >= BROKER_API_DEGRADED_THRESHOLD:
            with self.lock:
                self.state['running'] = False
                self.state['broker_api_degraded'] = {
                    'created_at': int(time.time()),
                    'where': where,
                    'error': str(error),
                    'failure_count': self._broker_api_failures,
                }
                self._save()
            log.error(f'BROKER_API_DEGRADED {where}: {error}')

    def _get_account_cached(self) -> dict:
        """Return Alpaca account dict, refreshed at most once per 15 seconds."""
        if self.trader is None:
            return {}
        now = time.time()
        if now - self._acct_cache['ts'] < 15 and self._acct_cache['data'] is not None:
            return self._acct_cache['data']
        try:
            data = self.trader.get_account()
            self._acct_cache = {'ts': now, 'data': data}
            self._record_broker_api_success()
            return data
        except Exception as e:
            log.warning(f'get_account failed: {e}')
            self._record_broker_api_failure('get_account', e)
            return self._acct_cache['data'] or {}

    def _watched_broker_positions(self) -> dict:
        if self.trader is None:
            return {}
        out = {}
        for p in self.trader.list_positions():
            sym = p.get('symbol')
            if sym in TICKERS:
                out[sym] = p
        return out

    def _position_mismatches(self, internal: dict, broker_positions: dict) -> list:
        mismatches = []
        for sym, pos in internal.items():
            bp = broker_positions.get(sym)
            if not bp:
                continue
            broker_side = str(bp.get('side') or '').upper()
            expected_side = 'LONG' if pos.get('side') == 'LONG' else 'SHORT'
            try:
                broker_qty = abs(float(bp.get('qty') or 0))
                local_qty = abs(float(pos.get('qty') or 0))
            except Exception:
                broker_qty = local_qty = None
            reasons = []
            if broker_side and broker_side != expected_side:
                reasons.append(f'side broker={broker_side} local={expected_side}')
            if broker_qty is not None and local_qty is not None and abs(broker_qty - local_qty) > 0.01:
                reasons.append(f'qty broker={broker_qty} local={local_qty}')
            if reasons:
                mismatches.append({'symbol': sym, 'reasons': reasons})
        return mismatches

    def _step2_density_counts_locked(self, day_iso: str) -> dict:
        counts = step2_execution_contract.density_counts(self.state, day_iso, CT)
        counts['by_ticker'] = {t: int((counts.get('by_ticker') or {}).get(t, 0)) for t in TICKERS}
        return counts

    def _step2_density_cap_reason_locked(self, ticker: str, day_iso: str) -> Optional[str]:
        if not STEP2_SIGNAL_SCAN_LIVE_MODE:
            return None
        return step2_execution_contract.density_cap_reason(
            self.state,
            ticker,
            day_iso,
            STEP2_MAX_TRADES_PER_DAY,
            STEP2_MAX_TRADES_PER_TICKER_DAY,
            CT,
        )

    def _step2_reentry_cooldown_reason_locked(self, ticker: str, signal_ts: int) -> Optional[str]:
        if not STEP2_SIGNAL_SCAN_LIVE_MODE or STEP2_SAME_TICKER_REENTRY_COOLDOWN_SEC <= 0:
            return None
        return step2_execution_contract.reentry_cooldown_reason(
            self.state,
            ticker,
            signal_ts,
            STEP2_SAME_TICKER_REENTRY_COOLDOWN_SEC,
        )

    # ── public API ─────────────────────────────────────────────────────
    def start(self):
        rollover = self.rollover_market_day_state(reason='start')
        if not rollover.get('ok'):
            with self.lock:
                self.state['running'] = False
                self.state['state_rollover_block'] = {
                    'created_at': int(time.time()),
                    'target_day': rollover.get('target_day'),
                    'failed_checks': [row.get('name') for row in rollover.get('failed_checks') or []],
                    'path': rollover.get('path'),
                }
                self._save()
            self._audit_event('start_refused', data={
                'reason': 'state_rollover_failed',
                'rollover': {
                    'target_day': rollover.get('target_day'),
                    'critical_failure_count': rollover.get('critical_failure_count'),
                    'failed_checks': [row.get('name') for row in rollover.get('failed_checks') or []],
                    'path': rollover.get('path'),
                },
            })
            return {'ok': False, 'reason': 'state_rollover_failed', 'rollover': rollover}
        with self.lock:
            block = self.state.get('broker_exposure_block')
        if block:
            # Safety latch can become stale after a delayed broker fill.
            # Before refusing, confirm the broker is still showing exposure.
            symbols = sorted(set((block or {}).get('symbols') or [])) or list(TICKERS)
            if self.trader is not None:
                try:
                    broker_map = self._watched_broker_positions()
                    still_open = sorted(sym for sym in symbols if sym in broker_map)
                    if not still_open:
                        self._clear_broker_exposure_block()
                        block = None
                        log.warning('broker_exposure_block cleared on start: broker is flat')
                except Exception as e:
                    fallback = self._latest_flat_broker_safety_snapshot(symbols)
                    if fallback:
                        self._clear_block_if_snapshot_flat(
                            fallback.get('payload') or {},
                            'start_flat_check_failed_snapshot_fallback',
                            fallback.get('path'),
                        )
                        block = None
                        log.warning('broker_exposure_block cleared on start from recent flat '
                                    'broker safety snapshot after flat check failed')
                    else:
                        log.error(f'mock trader START refused: broker_exposure_block '
                                  f'and flat check failed: {e} block={block}')
                        self._audit_event('start_refused', data={
                            'reason': 'broker_exposure_block_flat_check_failed',
                            'error': str(e),
                            'block': block,
                        })
                        return {'ok': False, 'reason': 'broker_exposure_block_flat_check_failed', 'error': str(e), 'block': block}
            else:
                fallback = self._latest_flat_broker_safety_snapshot(symbols)
                if fallback:
                    self._clear_block_if_snapshot_flat(
                        fallback.get('payload') or {},
                        'start_no_trader_snapshot_fallback',
                        fallback.get('path'),
                    )
                    block = None
                    log.warning('broker_exposure_block cleared on start from recent flat '
                                'broker safety snapshot without broker client')
                else:
                    log.error(f'mock trader START refused: broker_exposure_block '
                              f'and no broker client/snapshot fallback block={block}')
                    self._audit_event('start_refused', data={
                        'reason': 'broker_exposure_block_no_broker_client',
                        'block': block,
                    })
                    return {'ok': False, 'reason': 'broker_exposure_block_no_broker_client', 'block': block}
            if block:
                log.error(f'mock trader START refused: broker_exposure_block={block}')
                self._audit_event('start_refused', data={
                    'reason': 'broker_exposure_block',
                    'block': block,
                })
                return {'ok': False, 'reason': 'broker_exposure_block', 'block': block}
        if self.trader is not None:
            try:
                broker_map = self._watched_broker_positions()
            except Exception as e:
                log.error(f'mock trader START refused: broker flat check failed: {e}')
                self._set_broker_exposure_block(TICKERS, 'start_broker_flat_check_failed')
                self._audit_event('start_refused', data={
                    'reason': 'flat_check_failed',
                    'error': str(e),
                })
                return {'ok': False, 'reason': 'flat_check_failed', 'error': str(e)}
            with self.lock:
                internal = dict(self.state.get('positions') or {})
            mismatches = self._position_mismatches(internal, broker_map)
            untracked = sorted(sym for sym in broker_map if sym not in internal)
            if mismatches or untracked:
                blocked = sorted(set(untracked + [m['symbol'] for m in mismatches]))
                log.error(f'mock trader START refused: broker exposure mismatch '
                          f'untracked={untracked} mismatches={mismatches}')
                self._set_broker_exposure_block(blocked, 'start_refused_broker_exposure_mismatch')
                self._audit_event('start_refused', data={
                    'reason': 'broker_exposure_mismatch',
                    'untracked': untracked,
                    'mismatches': mismatches,
                })
                return {
                    'ok': False,
                    'reason': 'broker_exposure_mismatch',
                    'untracked': untracked,
                    'mismatches': mismatches,
                }
        lifecycle_gate = self.broker_lifecycle_gate(label='start', enforce=True)
        if not lifecycle_gate.get('ok'):
            self._audit_event('start_refused', data={
                'reason': 'broker_lifecycle_gate_failed',
                'verdict': lifecycle_gate.get('verdict'),
                'issues': (lifecycle_gate.get('issues') or [])[:20],
            })
            return {
                'ok': False,
                'reason': 'broker_lifecycle_gate_failed',
                'verdict': lifecycle_gate.get('verdict'),
                'issues': (lifecycle_gate.get('issues') or [])[:20],
                'repair_recommendations': (lifecycle_gate.get('repair_recommendations') or [])[:10],
            }
        startup_check = self._startup_self_check()
        synthetic_instance = not hasattr(self, 'engine') or not hasattr(self, 'state_path')
        if not startup_check.get('ok') and not synthetic_instance:
            failed = [
                row.get('name')
                for row in startup_check.get('checks') or []
                if not row.get('ok')
            ]
            recovery = (
                live_profile_recovery.recovery_recommendation(
                    TRADING_CONFIG,
                    reason='manual_start_refused_hard_contract_gate_failed',
                    status={'startup_self_check': startup_check},
                )
                if live_profile_recovery is not None else None
            )
            with self.lock:
                self.state['startup_self_check'] = startup_check
                self.state['running'] = False
                self.state['startup_contract_gate_block'] = {
                    'created_at': int(time.time()),
                    'failed_checks': failed,
                    'override': 'CLAUDE_ALLOW_CONTRACT_DRIFT=1',
                    'recovery': recovery,
                }
                self._save()
            self._audit_event('start_refused', data={
                'reason': 'hard_contract_gate_failed',
                'failed_checks': failed,
                'override': 'CLAUDE_ALLOW_CONTRACT_DRIFT=1',
                'recovery': recovery,
            })
            return {'ok': False, 'reason': 'hard_contract_gate_failed', 'failed_checks': failed}
        with self.lock:
            self.state['startup_self_check'] = startup_check
            self.state['running'] = True
            self.state.pop('startup_contract_gate_block', None)
            self._save()
        # Relaunch threads if a previous stop() killed them.
        if not self._monitor_thread.is_alive():
            self._stop_evt = threading.Event()
            self._monitor_thread = threading.Thread(
                target=self._monitor_loop, daemon=True, name='mock-monitor')
            self._monitor_thread.start()
        self._audit_event('start')
        log.info('mock trader STARTED')
        return {'ok': True, 'reason': 'started'}

    def stop(self):
        """Manually stop. Flatten any open positions at last price."""
        with self.lock:
            self.state['running'] = False
            open_tkrs = list(self.state['positions'].keys())
            self._save()
        remaining = set()
        for tkr in open_tkrs:
            with self.lock:
                pos = self.state.get('positions', {}).get(tkr)
            price = self.engine.get_last_price(tkr) or (pos or {}).get('entry')
            self._start_exit_tick_capture(tkr, pos, price, 'manual_stop')
        if self.trader is not None and open_tkrs:
            try:
                remaining = self._confirmed_broker_close(open_tkrs, 'manual_stop')
            except Exception as e:
                log.error(f'manual_stop: broker close workflow failed: {e}')
                remaining = set(open_tkrs)
            if remaining:
                self._set_broker_exposure_block(
                    remaining,
                    'manual_stop_positions_not_flat_after_close_attempt',
                )
        for tkr in [s for s in open_tkrs if s not in remaining]:
            with self.lock:
                pos = self.state['positions'].get(tkr)
            price = self.engine.get_last_price(tkr) or (pos or {}).get('entry')
            if price:
                self._close_position(tkr, price, 'manual_stop')
        self._stop_evt.set()  # wake + exit monitor loop
        self._audit_event('stop', data={'remaining': sorted(remaining)})
        self._write_broker_safety_snapshot('shutdown')
        log.info('mock trader STOPPED')
        return {
            'ok': not bool(remaining),
            'reason': 'stopped' if not remaining else 'broker_positions_not_flat',
            'remaining': sorted(remaining),
        }

    def reset(self):
        with self.lock:
            internal_open = sorted(self.state.get('positions', {}).keys())
        broker_open = []
        if self.trader is not None:
            try:
                broker_open = sorted(self._watched_broker_positions().keys())
            except Exception as e:
                log.error(f'mock trader RESET refused: broker flat check failed: {e}')
                self._set_broker_exposure_block(TICKERS, 'reset_broker_flat_check_failed')
                return {'ok': False, 'reason': 'broker_flat_check_failed', 'error': str(e)}
        if internal_open or broker_open:
            symbols = sorted(set(internal_open + broker_open))
            log.error(f'mock trader RESET refused: open exposure internal={internal_open} broker={broker_open}')
            self._set_broker_exposure_block(symbols, 'reset_refused_open_exposure')
            self._audit_event('reset_refused', data={
                'internal_open': internal_open,
                'broker_open': broker_open,
            })
            return {
                'ok': False,
                'reason': 'open_exposure',
                'internal_open': internal_open,
                'broker_open': broker_open,
            }
        with self.lock:
            self.state = self._fresh_state()
            self._critical_save('reset_state')
        self._audit_event('reset')
        log.info(f'mock trader RESET to ${self.state["start_balance"]:.2f}')
        return {'ok': True, 'reason': 'reset'}

    def clear_broker_exposure_block(self, force: bool = False) -> dict:
        """
        Clear broker_exposure_block *only* if broker is flat for the blocked
        symbols (unless force=True).

        This block is a safety latch: it prevents trading when Alpaca exposure
        might still be open or mismatched.
        """
        with self.lock:
            block = dict(self.state.get('broker_exposure_block') or {})
        if not block:
            return {'ok': True, 'reason': 'no_block'}
        symbols = sorted(set(block.get('symbols') or [])) or list(TICKERS)
        if force or self.trader is None:
            self._clear_broker_exposure_block()
            self._audit_event('broker_exposure_block_cleared', data={
                'force': bool(force),
                'symbols': symbols,
                'prior_block': block,
            })
            log.warning(f'broker exposure block CLEARED (force={bool(force)}) symbols={symbols}')
            return {'ok': True, 'reason': 'cleared', 'force': bool(force), 'symbols': symbols}
        try:
            broker_map = self._watched_broker_positions()
        except Exception as e:
            log.error(f'clear_broker_exposure_block refused: broker flat check failed: {e}')
            return {'ok': False, 'reason': 'broker_flat_check_failed', 'error': str(e), 'block': block}
        still_open = sorted(sym for sym in symbols if sym in broker_map)
        if still_open:
            return {
                'ok': False,
                'reason': 'broker_positions_still_open',
                'symbols': still_open,
                'block': block,
            }
        self._clear_broker_exposure_block()
        self._audit_event('broker_exposure_block_cleared', data={
            'force': False,
            'symbols': symbols,
            'prior_block': block,
        })
        log.warning(f'broker exposure block CLEARED symbols={symbols}')
        return {'ok': True, 'reason': 'cleared', 'force': False, 'symbols': symbols}

    def status(self, full: bool = False) -> dict:
        def _trade_summary(rows: list[dict]) -> dict:
            pnl = round(sum(float(t.get('pnl') or 0) for t in rows), 2)
            wins = sum(1 for t in rows if t.get('result') == 'WIN' or (t.get('pnl') or 0) > 0)
            losses = sum(1 for t in rows if t.get('result') == 'LOSS' or (t.get('pnl') or 0) < 0)
            gross_w = sum(float(t.get('pnl') or 0) for t in rows if (t.get('pnl') or 0) > 0)
            gross_l = abs(sum(float(t.get('pnl') or 0) for t in rows if (t.get('pnl') or 0) < 0))
            op_reasons = {'manual_stop', 'session_end', 'external_broker_exit'}
            ops = [
                t for t in rows
                if str(t.get('reason') or '') in op_reasons
                or str(t.get('reason') or '').startswith('external_')
            ]
            return {
                'trades': len(rows),
                'wins': wins,
                'losses': losses,
                'win_rate': round(wins / len(rows) * 100, 1) if rows else None,
                'pnl': pnl,
                'profit_factor': round(gross_w / gross_l, 3) if gross_l else None,
                'largest_loss': min([float(t.get('pnl') or 0) for t in rows], default=0.0),
                'ops_contaminated_trades': len(ops),
            }

        def _slim_trade(t: dict) -> dict:
            keys = (
                'trade_id', 'ticker', 'side', 'entry', 'exit', 'qty', 'pnl',
                'result', 'reason', 'opened_at', 'closed_at', 'conviction',
                'setup', 'entry_score', 'exit_score',
            )
            return {k: t.get(k) for k in keys if k in t}

        with self.lock:
            if full:
                s = json.loads(json.dumps(self.state))  # full deep-copy for UI/history views
            else:
                trades = list(self.state.get('trades', []))
                s = {
                    'running': self.state.get('running'),
                    'balance': self.state.get('balance'),
                    'start_balance': self.state.get('start_balance'),
                    'positions': json.loads(json.dumps(self.state.get('positions', {}))),
                    'pending_entries': json.loads(json.dumps(self.state.get('pending_entries', {}))),
                    'setup_pauses': json.loads(json.dumps(self.state.get('setup_pauses', {}))),
                    'started_at': self.state.get('started_at'),
                    'daily_budget_date': self.state.get('daily_budget_date'),
                    'daily_per_ticker_budget': self.state.get('daily_per_ticker_budget'),
                    'broker_exposure_block': self.state.get('broker_exposure_block'),
                    'broker_lifecycle_block': self.state.get('broker_lifecycle_block'),
                    'broker_lifecycle_gate': self.state.get('broker_lifecycle_gate'),
                    'broker_api_degraded': self.state.get('broker_api_degraded'),
                    'state_market_day': self.state.get('state_market_day'),
                    'state_rollover': self.state.get('state_rollover'),
                    'state_rollover_block': self.state.get('state_rollover_block'),
                    # Compact callers get enough recent context without hauling
                    # the full multi-MB state blob every poll.
                    'recent_trades': json.loads(json.dumps([_slim_trade(t) for t in trades[-20:]])),
                }
                s['_compact'] = True
                s['_full_available'] = True
        # Augment with current P/L on open positions
        unreal = 0.0
        for tkr, pos in s.get('positions', {}).items():
            price = self.engine.get_last_price(tkr)
            if price:
                if pos['side'] == 'LONG':
                    pos['cur_price']   = price
                    pos['unrealized']  = round((price - pos['entry']) * pos['qty'], 2)
                else:
                    pos['cur_price']   = price
                    pos['unrealized']  = round((pos['entry'] - price) * pos['qty'], 2)
                unreal += pos['unrealized']
        s['unrealized']    = round(unreal, 2)
        # total_pnl = realized P/L since reset. Use recorded start_balance
        # (falls back to START_BALANCE for legacy state files).
        start_bal = s.get('start_balance', START_BALANCE)
        s['total_pnl']     = round(s['balance'] - start_bal, 2)
        today_iso = datetime.now(CT).date().isoformat()
        s['in_window']     = self._in_trading_window()
        s['blackout']      = self._in_blackout()
        s['kill_switch']   = self._kill_switch_state()
        s['strategy_config_hash'] = STRATEGY_CONFIG_HASH
        s['execution_kernel_hash'] = EXECUTION_KERNEL_HASH
        s['execution_kernel_contract'] = EXECUTION_KERNEL_CONTRACT
        s['step2_parity_contract_hash'] = STEP2_PARITY_CONTRACT_HASH
        s['step2_parity_contract'] = STEP2_PARITY_CONTRACT
        s['step2_execution_contract_hash'] = STEP2_EXECUTION_CONTRACT_HASH
        s['step2_execution_contract'] = STEP2_EXECUTION_CONTRACT
        s['active_scoring_profile'] = self._active_profile_status_snapshot(full=full)
        s['promotion_provenance'] = self._promotion_provenance_status()
        s['config_provenance'] = self._config_provenance_status()
        with self.lock:
            s['startup_self_check'] = json.loads(json.dumps(self.state.get('startup_self_check')))
        with self.lock:
            s['step2_density_counts_today'] = self._step2_density_counts_locked(today_iso)
        s['broker_api_degraded'] = self.state.get('broker_api_degraded')
        s['regime_score'] = self._regime_score()
        s['feed_health'] = self._feed_health()
        try:
            s['signal_loop_health'] = self.engine._signal_loop_health()
        except Exception:
            s['signal_loop_health'] = {'status': 'unknown'}
        # Live Alpaca-equity-based sizing snapshot.
        # When Alpaca is live, the "balance" and "equity" the UI shows
        # reflect the real account (cash and equity), not the internal
        # $1000 P/L tracker. `total_pnl` continues to show realized P/L
        # from trades (what the model has earned since start).
        s['equity']        = round(s['balance'] + unreal, 2)  # fallback
        try:
            if self.trader is not None:
                acct = self._get_account_cached()
                s['alpaca_equity']     = float(acct.get('equity', 0) or 0)
                s['alpaca_cash']       = float(acct.get('cash', 0) or 0)
                # If today's frozen budget snapshot exists, expose it as
                # the authoritative sizing base; otherwise fall back to
                # live-equity/len(TICKERS).
                if STEP2_SIGNAL_SCAN_LIVE_MODE:
                    s['per_ticker_budget'] = self._get_per_ticker_budget()
                elif s.get('daily_budget_date') == today_iso and s.get('daily_per_ticker_budget'):
                    s['per_ticker_budget'] = float(s['daily_per_ticker_budget'])
                else:
                    s['per_ticker_budget'] = round(s['alpaca_equity'] / max(1, len(TICKERS)), 2)
                s['next_trade_size_pct'] = self._trade_size_pct_now({})
                s['next_alloc']        = round(s['per_ticker_budget'] * s['next_trade_size_pct'], 2)
                # Note: do NOT override s['balance']/s['equity'] with Alpaca
                # values. The UI's "Balance" KPI must equal start_balance plus
                # the sum of simulated trade P/Ls (what the trade history
                # ending-balance column shows). Alpaca cash/equity can differ
                # due to real spreads/fees/pending orders — surface those as
                # separate fields instead.
        except Exception:
            pass
        with self.lock:
            all_trades = list(self.state.get('trades', []))
        today_iso = datetime.now(CT).date().isoformat()
        today_trades = [t for t in all_trades if _trade_day(t.get('closed_at') or t.get('opened_at')) == today_iso]
        current_era_trades = [
            t for t in all_trades
            if _era_for_day(_trade_day(t.get('closed_at') or t.get('opened_at'))) == CURRENT_ERA_LABEL
        ]
        clean_current_era_trades = [
            t for t in current_era_trades
            if str(t.get('reason') or '') not in {'manual_stop', 'session_end', 'external_broker_exit'}
            and not str(t.get('reason') or '').startswith('external_')
        ]
        s['era'] = {
            'current_label': CURRENT_ERA_LABEL,
            'current_start': CURRENT_ERA_START,
            'baseline_label': BASELINE_ERA_LABEL,
        }
        s['summaries'] = {
            'today': _trade_summary(today_trades),
            'current_era': _trade_summary(current_era_trades),
            'current_era_clean_strategy': _trade_summary(clean_current_era_trades),
            'all_time': _trade_summary(all_trades),
        }
        s['trade_count']   = len(all_trades)
        s['wins']          = sum(1 for t in all_trades if t.get('result') == 'WIN')
        s['losses']        = sum(1 for t in all_trades if t.get('result') == 'LOSS')
        s['tickers']       = TICKERS
        s['flatten_cutoff_ct'] = f'{SESSION_FLATTEN_H:02d}:{SESSION_FLATTEN_M:02d}'
        s['entry_cutoff_ct'] = f'{ENTRY_CUTOFF_H:02d}:{ENTRY_CUTOFF_M:02d}'
        # Expose live last prices for each ticker (header widget).
        tp = {}
        for tkr in TICKERS:
            try:
                px = self.engine.get_last_price(tkr)
                if px is not None:
                    tp[tkr] = round(float(px), 4)
            except Exception:
                pass
        s['ticker_prices'] = tp
        return s

    def trade_history(
        self,
        limit: Optional[int] = None,
        day_iso: Optional[str] = None,
        full: bool = False,
    ) -> dict:
        def _slim_trade(t: dict) -> dict:
            keys = (
                'trade_id', 'ticker', 'side', 'entry', 'exit', 'qty', 'pnl',
                'result', 'reason', 'reasons', 'opened_at', 'closed_at',
                'conviction', 'setup',
            )
            return {k: t.get(k) for k in keys if k in t}

        with self.lock:
            trades = list(self.state.get('trades', []))
            start_balance = self.state.get('start_balance', START_BALANCE)
        if day_iso:
            filtered = []
            for trade in trades:
                ts = trade.get('opened_at') or trade.get('closed_at')
                if ts and datetime.fromtimestamp(ts, CT).date().isoformat() == day_iso:
                    filtered.append(trade)
            trades = filtered
        total = len(trades)
        if limit is not None and limit >= 0:
            trades = trades[-int(limit):]
        if not full:
            trades = [_slim_trade(t) for t in trades]
        return {
            'count': total,
            'returned': len(trades),
            'compact': not full,
            'start_balance': start_balance,
            'trades': json.loads(json.dumps(trades)),
        }

    def _state_snapshot(self, symbol: str) -> dict:
        st = getattr(self.engine, 'states', {}).get(symbol)
        if not st:
            return {'streamed': False}
        try:
            from ws_scalp import compute_indicators
            ind = compute_indicators(st)
        except Exception:
            ind = {}
        with st.lock:
            now_ms = int(time.time() * 1000)
            quote_age = round((now_ms - st.last_quote_ts_ms) / 1000, 3) if st.last_quote_ts_ms else None
            trade_age = round((now_ms - st.last_trade_ts_ms) / 1000, 3) if st.last_trade_ts_ms else None
            spread = (st.best_ask - st.best_bid) if st.best_bid is not None and st.best_ask is not None else None
            return {
                'streamed': True,
                'price': st.last_trade_price,
                'bid': st.best_bid,
                'ask': st.best_ask,
                'spread': spread,
                'spread_pct': round(spread / st.last_trade_price * 100, 4)
                              if spread is not None and st.last_trade_price else None,
                'quote_age_sec': quote_age,
                'trade_age_sec': trade_age,
                'last_trade_exchange': getattr(st, 'last_trade_exchange', None),
                'last_trade_conditions': list(getattr(st, 'last_trade_conditions', []) or []),
                'last_trade_tape': getattr(st, 'last_trade_tape', None),
                'last_quote_bid_exchange': getattr(st, 'last_quote_bid_exchange', None),
                'last_quote_ask_exchange': getattr(st, 'last_quote_ask_exchange', None),
                'last_quote_conditions': list(getattr(st, 'last_quote_conditions', []) or []),
                'last_quote_tape': getattr(st, 'last_quote_tape', None),
                'quote_state': ind.get('quote_state') or {},
                'condition_quality': ind.get('condition_quality') or {},
                'rolling_spread_median_pct': ind.get('rolling_spread_median_pct'),
                'rolling_spread_p95_pct': ind.get('rolling_spread_p95_pct'),
                'spread_vs_rolling_median': ind.get('spread_vs_rolling_median'),
                'spread_abnormal': ind.get('spread_abnormal'),
                'indicators': ind,
            }

    def _prior_close_context(self, tkr: str, price: Optional[float],
                             allow_fetch: bool = False) -> dict:
        prior_close = None
        try:
            if not BAR_DATA_AVAILABLE:
                return {'prior_close': None, 'source': 'avg_daily_volume_cache', 'error': 'bar_data_unavailable'}
            cached = self._avg_vol_cache.get(tkr)
            if not cached or cached.get('date') != datetime.now(CT).date().isoformat():
                if allow_fetch:
                    self._get_avg_daily_vol(tkr)
                else:
                    return {'prior_close': None, 'source': 'avg_daily_volume_cache', 'status': 'not_warmed'}
                cached = self._avg_vol_cache.get(tkr)
            prior_close = cached.get('prior_close') if cached else None
        except Exception as e:
            return {'prior_close': None, 'error': str(e), 'source': 'avg_daily_volume_cache'}
        gap_pct = None
        if prior_close and price:
            gap_pct = round((float(price) - float(prior_close)) / float(prior_close) * 100, 3)
        btc = self._state_snapshot('BTC/USD').get('indicators') or {}
        btc_session = btc.get('session_return_pct')
        beta = BTC_BETA.get(tkr)
        btc_implied_gap = round(float(beta) * float(btc_session), 3) if beta is not None and btc_session is not None else None
        stock_minus_btc_gap = (
            round(float(gap_pct) - float(btc_implied_gap), 3)
            if gap_pct is not None and btc_implied_gap is not None else None
        )
        return {
            'prior_close': self._num(prior_close),
            'gap_pct_from_prior_close': gap_pct,
            'btc_session_return_pct': self._num(btc_session),
            'btc_beta': beta,
            'btc_implied_gap_pct': btc_implied_gap,
            'stock_minus_btc_implied_gap_pct': stock_minus_btc_gap,
            'source': 'avg_daily_volume_cache_and_live_btc_session_return',
        }

    def _market_microstructure_context(self, tkr: str, side: Optional[str],
                                       price: Optional[float],
                                       ind: Optional[dict] = None) -> dict:
        stock = self._state_snapshot(tkr)
        ind = ind or stock.get('indicators') or {}
        quote_age = stock.get('quote_age_sec')
        trade_age = stock.get('trade_age_sec')
        spread_pct = stock.get('spread_pct')
        session_return = ind.get('session_return_pct')
        gap = self._prior_close_context(tkr, price)
        prior_gap = gap.get('gap_pct_from_prior_close')
        drop_pct = session_return if session_return is not None else prior_gap
        halt_suspected = (
            bool(self._in_trading_window())
            and ((quote_age is None or quote_age >= HALT_STALE_SEC)
                 and (trade_age is None or trade_age >= HALT_STALE_SEC))
        )
        luld_like_risk = bool(
            (spread_pct is not None and float(spread_pct) >= LULD_SPREAD_RISK_PCT)
            or (quote_age is not None and quote_age >= LULD_STALE_SEC
                and trade_age is not None and trade_age >= LULD_STALE_SEC)
        )
        ssr_approx_active = drop_pct is not None and float(drop_pct) <= SSR_DROP_PCT
        return {
            'halt_suspected': halt_suspected,
            'luld_like_risk': luld_like_risk,
            'ssr_approx_active': ssr_approx_active,
            'quote_age_sec': self._num(quote_age),
            'trade_age_sec': self._num(trade_age),
            'spread_pct': self._num(spread_pct),
            'session_return_pct': self._num(session_return),
            'prior_close_context': gap,
            'side': side,
            'deduction': (
                'Approximate halt/LULD/SSR awareness from live quote/trade freshness, spread, '
                'and prior-close/session move. It is a trading safety and postmortem feature, '
                'not an official exchange halt feed.'
            ),
        }

    def _asset_info_snapshot(self, tkr: str, force: bool = False,
                             critical: bool = False,
                             allow_fetch: bool = True) -> dict:
        now = time.time()
        cached = self._asset_info_cache.get(tkr)
        if (not force and cached
                and now - float(cached.get('ts') or 0) <= ASSET_INFO_TTL_SEC):
            data = dict(cached.get('data') or {})
            data['cache_hit'] = True
            return data
        if not allow_fetch and not critical:
            return {'available': False, 'symbol': tkr, 'status': 'not_warmed', 'source': 'alpaca_asset_snapshot'}
        if self.trader is None:
            return {'available': False, 'reason': 'alpaca_executor_disabled'}
        try:
            asset = self.trader.get_asset(tkr) or {}
            data = {
                'available': True,
                'symbol': asset.get('symbol') or tkr,
                'tradable': asset.get('tradable'),
                'shortable': asset.get('shortable'),
                'easy_to_borrow': asset.get('easy_to_borrow'),
                'fractionable': asset.get('fractionable'),
                'marginable': asset.get('marginable'),
                'status': asset.get('status'),
                'asset_class': asset.get('asset_class'),
                'maintenance_margin_requirement': asset.get('maintenance_margin_requirement'),
                'source': 'alpaca_asset_snapshot',
                'cache_hit': False,
                'captured_at': int(now),
            }
            self._asset_info_cache[tkr] = {'ts': now, 'data': data}
            return data
        except Exception as e:
            if critical:
                self._record_broker_api_failure(f'get_asset:{tkr}', e)
            return {
                'available': False,
                'symbol': tkr,
                'error': str(e),
                'source': 'alpaca_asset_snapshot',
                'captured_at': int(now),
            }

    def _catalyst_context(self, tkr: str) -> dict:
        day = datetime.now(CT).date().isoformat()
        template = {
            '_instructions': (
                'Optional manual catalyst flags. The bot reads this for context only. '
                'Use YYYY-MM-DD keys with market, BTC, CLSK, MARA, RIOT arrays.'
            ),
            day: {'market': [], 'BTC': [], 'CLSK': [], 'MARA': [], 'RIOT': []},
        }
        try:
            os.makedirs(os.path.dirname(CATALYST_FLAGS_PATH), exist_ok=True)
            if not os.path.exists(CATALYST_FLAGS_PATH):
                with open(CATALYST_FLAGS_PATH, 'w', encoding='utf-8') as f:
                    json.dump(template, f, indent=2)
            with open(CATALYST_FLAGS_PATH, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            return {'has_catalyst': False, 'items': [], 'error': str(e), 'file': CATALYST_FLAGS_PATH}
        day_rows = data.get(day) if isinstance(data, dict) else {}
        items = []
        if isinstance(day_rows, dict):
            for key in ('market', 'BTC', tkr):
                vals = day_rows.get(key) or []
                if isinstance(vals, str):
                    vals = [vals]
                for value in vals:
                    if value:
                        items.append({'scope': key, 'note': str(value)})
        return {
            'has_catalyst': bool(items),
            'items': items,
            'day': day,
            'file': CATALYST_FLAGS_PATH,
            'mode': 'manual_flags_context_only',
        }

    def _btc_proxy_context(self) -> dict:
        rows = []
        ready = 0
        above = 0
        day_sum = 0.0
        for sym in BTC_PROXY_SYMBOLS:
            tape = self._market_tape.get(sym) or {}
            if not tape:
                rows.append({'symbol': sym, 'ready': False})
                continue
            day_pct = tape.get('day_pct')
            if day_pct is not None:
                ready += 1
                day_sum += float(day_pct)
            if tape.get('above_vwap') is True:
                above += 1
            rows.append({
                'symbol': sym,
                'ready': bool(tape),
                'day_pct': self._num(day_pct),
                'above_vwap': tape.get('above_vwap'),
                'price': self._num(tape.get('price')),
                'updated_at': tape.get('updated_at'),
            })
        avg_day = round(day_sum / ready, 3) if ready else None
        if ready and avg_day is not None and avg_day >= 0.5 and above >= max(1, ready // 2):
            state = 'proxy_risk_on'
        elif ready and avg_day is not None and avg_day <= -0.5 and above == 0:
            state = 'proxy_risk_off'
        elif ready:
            state = 'mixed'
        else:
            state = 'not_ready'
        return {
            'state': state,
            'symbols': list(BTC_PROXY_SYMBOLS),
            'ready': ready,
            'avg_day_pct': avg_day,
            'above_vwap_count': above,
            'rows': rows,
            'deduction': 'BTC-proxy equity basket context only; BTC/USD remains the primary driver.',
        }

    def _liquidity_impact(self, sig: dict, price: Optional[float],
                          side: Optional[str], qty: Optional[float] = None) -> dict:
        ind = sig.get('indicators') or {}
        bid_size = sig.get('bid_size') or ind.get('bid_size')
        ask_size = sig.get('ask_size') or ind.get('ask_size')
        spread_pct = sig.get('spread_pct') or ind.get('spread_pct')
        entry_display_size = ask_size if side == 'LONG' else bid_size
        qty_vs_display = None
        if qty is not None and entry_display_size:
            try:
                qty_vs_display = round(float(qty) / max(1.0, float(entry_display_size)), 3)
            except Exception:
                qty_vs_display = None
        risk = 'unknown'
        if spread_pct is not None:
            try:
                sp = float(spread_pct)
                if sp >= 0.12 or (qty_vs_display is not None and qty_vs_display >= 1.0):
                    risk = 'high'
                elif sp >= 0.08 or (qty_vs_display is not None and qty_vs_display >= 0.5):
                    risk = 'elevated'
                else:
                    risk = 'normal'
            except Exception:
                pass
        return {
            'bid_size': self._num(bid_size),
            'ask_size': self._num(ask_size),
            'entry_side_display_size': self._num(entry_display_size),
            'qty': self._num(qty),
            'qty_vs_displayed_size': qty_vs_display,
            'spread_pct': self._num(spread_pct),
            'impact_risk': risk,
            'note': 'Uses top-of-book display size only; hidden liquidity and queue position are unknown.',
        }

    def _estimated_transaction_costs(self, side: str, qty: float,
                                     entry_price: float, exit_price: float) -> dict:
        try:
            qty_f = float(qty)
            entry_notional = abs(qty_f * float(entry_price))
            exit_notional = abs(qty_f * float(exit_price))
            sell_notional = exit_notional if side == 'LONG' else entry_notional
            sec_fee = round(sell_notional * SEC_FEE_RATE, 4)
            taf_fee = round(min(FINRA_TAF_CAP, abs(qty_f) * FINRA_TAF_PER_SHARE), 4)
            total = round(sec_fee + taf_fee, 4)
            return {
                'estimated_total': total,
                'sec_fee_estimate': sec_fee,
                'finra_taf_estimate': taf_fee,
                'commission_estimate': 0.0,
                'borrow_fee_estimate': None,
                'entry_notional': round(entry_notional, 2),
                'exit_notional': round(exit_notional, 2),
                'config': {
                    'sec_fee_rate': SEC_FEE_RATE,
                    'finra_taf_per_share': FINRA_TAF_PER_SHARE,
                    'finra_taf_cap': FINRA_TAF_CAP,
                },
                'deduction': 'Configurable estimate for review only; broker statements remain source of truth.',
            }
        except Exception as e:
            return {'estimated_total': None, 'error': str(e)}

    def _append_rule_dry_run_event(self, event: str, row: dict):
        try:
            now = int(row.get('created_at') or time.time())
            day = datetime.fromtimestamp(now, CT).date().isoformat()
            payload = dict(row)
            payload['event'] = event
            payload['created_at'] = now
            payload['strategy_config_hash'] = payload.get('strategy_config_hash') or STRATEGY_CONFIG_HASH
            os.makedirs(RULE_DRY_RUN_DIR, exist_ok=True)
            path = os.path.join(RULE_DRY_RUN_DIR, f'rule_dry_run_{day}.jsonl')
            with open(path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(payload, separators=(',', ':'), default=str) + '\n')
        except Exception as e:
            log.warning(f'rule dry-run write failed: {e}')

    def _append_fill_attribution(self, row: dict):
        try:
            now = int(row.get('created_at') or time.time())
            day = datetime.fromtimestamp(now, CT).date().isoformat()
            os.makedirs(FILL_ATTRIBUTION_DIR, exist_ok=True)
            path = os.path.join(FILL_ATTRIBUTION_DIR, f'fill_attribution_{day}.jsonl')
            with open(path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(row, separators=(',', ':'), default=str) + '\n')
            try:
                from event_store import record_event
                record_event('fill_attribution', row, symbol=row.get('ticker'),
                             trade_id=row.get('trade_id'), ts=now)
            except Exception:
                pass
        except Exception as e:
            log.warning(f'fill attribution write failed: {e}')

    def _fill_attribution_snapshot(self, tkr: str, side: str, stage: str,
                                   price_ref: Optional[float] = None,
                                   qty: Optional[float] = None,
                                   trade_id: Optional[str] = None,
                                   client_order_id: Optional[str] = None,
                                   broker_order_id: Optional[str] = None,
                                   fill_price: Optional[float] = None,
                                   broker_fill: Optional[dict] = None) -> dict:
        stock = self._state_snapshot(tkr)
        bid = stock.get('bid')
        ask = stock.get('ask')
        mid = None
        spread = stock.get('spread')
        spread_pct = stock.get('spread_pct')
        if bid is not None and ask is not None:
            try:
                mid = round((float(bid) + float(ask)) / 2, 5)
            except Exception:
                mid = None
        fill_vs_mid_pct = None
        adverse_fill_vs_mid_pct = None
        if fill_price is not None and mid:
            try:
                raw = (float(fill_price) - float(mid)) / float(mid) * 100
                fill_vs_mid_pct = round(raw, 4)
                is_exit = str(stage).startswith('exit') or 'exit' in str(stage)
                if is_exit:
                    adverse = -raw if side == 'LONG' else raw
                else:
                    adverse = raw if side == 'LONG' else -raw
                adverse_fill_vs_mid_pct = round(adverse, 4)
            except Exception:
                pass
        ref_vs_mid_pct = None
        if price_ref is not None and mid:
            try:
                ref_vs_mid_pct = round((float(price_ref) - float(mid)) / float(mid) * 100, 4)
            except Exception:
                pass
        return {
            'schema_version': 1,
            'created_at': int(time.time()),
            'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
            'ticker': tkr,
            'side': side,
            'stage': stage,
            'trade_id': trade_id,
            'client_order_id': client_order_id,
            'broker_order_id': broker_order_id,
            'qty': qty,
            'price_ref': self._num(price_ref),
            'fill_price': self._num(fill_price),
            'fill_vs_mid_pct': fill_vs_mid_pct,
            'adverse_fill_vs_mid_pct': adverse_fill_vs_mid_pct,
            'price_ref_vs_mid_pct': ref_vs_mid_pct,
                'quote': {
                    'bid': self._num(bid),
                    'ask': self._num(ask),
                    'mid': self._num(mid),
                    'spread': self._num(spread),
                    'spread_pct': self._num(spread_pct),
                    'rolling_spread_median_pct': stock.get('rolling_spread_median_pct'),
                    'rolling_spread_p95_pct': stock.get('rolling_spread_p95_pct'),
                    'spread_vs_rolling_median': stock.get('spread_vs_rolling_median'),
                    'spread_abnormal': stock.get('spread_abnormal'),
                    'quote_state': stock.get('quote_state') or {},
                    'quote_age_sec': stock.get('quote_age_sec'),
                    'trade_age_sec': stock.get('trade_age_sec'),
                    'bid_exchange': stock.get('last_quote_bid_exchange'),
                    'ask_exchange': stock.get('last_quote_ask_exchange'),
                    'quote_conditions': stock.get('last_quote_conditions') or [],
                'quote_tape': stock.get('last_quote_tape'),
            },
            'last_trade': {
                'price': self._num(stock.get('price')),
                'exchange': stock.get('last_trade_exchange'),
                'conditions': stock.get('last_trade_conditions') or [],
                'tape': stock.get('last_trade_tape'),
            },
            'condition_quality': stock.get('condition_quality') or {},
            'broker_fill': broker_fill or None,
            'deduction': (
                'Execution attribution snapshot: separates idea quality from spread, quote age, '
                'venue/condition context, and actual fill-vs-mid slippage.'
            ),
        }

    def _record_fill_attribution(self, tkr: str, side: str, stage: str,
                                 price_ref: Optional[float] = None,
                                 qty: Optional[float] = None,
                                 trade_id: Optional[str] = None,
                                 client_order_id: Optional[str] = None,
                                 broker_order_id: Optional[str] = None,
                                 fill_price: Optional[float] = None,
                                 broker_fill: Optional[dict] = None,
                                 pos: Optional[dict] = None) -> dict:
        row = self._fill_attribution_snapshot(
            tkr, side, stage,
            price_ref=price_ref, qty=qty, trade_id=trade_id,
            client_order_id=client_order_id, broker_order_id=broker_order_id,
            fill_price=fill_price, broker_fill=broker_fill,
        )
        if pos is not None:
            pos.setdefault('fill_attribution', []).append(row)
        self._append_fill_attribution(row)
        return row

    def _append_latency_attribution(self, row: dict):
        try:
            now = int(row.get('created_at') or time.time())
            day = datetime.fromtimestamp(now, CT).date().isoformat()
            os.makedirs(LATENCY_ATTRIBUTION_DIR, exist_ok=True)
            path = os.path.join(LATENCY_ATTRIBUTION_DIR, f'latency_attribution_{day}.jsonl')
            with open(path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(row, separators=(',', ':'), default=str) + '\n')
            try:
                from event_store import record_event
                record_event('latency_attribution', row, symbol=row.get('ticker'),
                             trade_id=row.get('trade_id'), ts=now)
            except Exception:
                pass
        except Exception as e:
            log.warning(f'latency attribution write failed: {e}')

    def _latency_attribution_row(self, tkr: str, side: str, trade_id: str,
                                 client_order_id: Optional[str],
                                 broker_order_id: Optional[str],
                                 latency_chain: dict,
                                 status: str) -> dict:
        chain = dict(latency_chain or {})
        signal_ms = chain.get('signal_seen_ms')
        ordered = [
            'signal_seen_ms',
            'reserved_ms',
            'pre_submit_start_ms',
            'pre_submit_end_ms',
            'broker_submit_start_ms',
            'broker_submit_end_ms',
            'broker_recovered_ms',
            'committed_ms',
            'entry_filled_ms',
            'exit_trigger_ms',
            'exit_submit_start_ms',
            'exit_submit_end_ms',
            'exit_broker_filled_ms',
            'exit_verified_flat_ms',
            'exit_local_closed_ms',
        ]
        deltas = {}
        if signal_ms:
            for key in ordered:
                val = chain.get(key)
                if val:
                    deltas[key.replace('_ms', '_from_signal_ms')] = int(val) - int(signal_ms)
        adjacent = {}
        prev_key = None
        for key in ordered:
            if not chain.get(key):
                continue
            if prev_key:
                adjacent[f'{prev_key}_to_{key}_ms'] = int(chain[key]) - int(chain[prev_key])
            prev_key = key
        return {
            'schema_version': 1,
            'created_at': int(time.time()),
            'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
            'ticker': tkr,
            'side': side,
            'trade_id': trade_id,
            'client_order_id': client_order_id,
            'broker_order_id': broker_order_id,
            'status': status,
            'chain_ms': chain,
            'from_signal_ms': deltas,
            'adjacent_ms': adjacent,
            'entry_time_to_submit_ms': self._ms_delta(chain, 'signal_seen_ms', 'broker_submit_start_ms'),
            'entry_time_to_fill_ms': self._ms_delta(chain, 'signal_seen_ms', 'entry_filled_ms'),
            'entry_broker_submit_ms': self._ms_delta(chain, 'broker_submit_start_ms', 'broker_submit_end_ms'),
            'exit_time_to_submit_ms': self._ms_delta(chain, 'exit_trigger_ms', 'exit_submit_start_ms'),
            'exit_broker_submit_ms': self._ms_delta(chain, 'exit_submit_start_ms', 'exit_submit_end_ms'),
            'exit_time_to_fill_ms': self._ms_delta(chain, 'exit_trigger_ms', 'exit_broker_filled_ms'),
            'exit_time_to_flat_ms': self._ms_delta(chain, 'exit_trigger_ms', 'exit_verified_flat_ms'),
            'deduction': (
                'Latency chain for entry signal -> broker submit/fill and exit trigger -> broker submit/fill/flat. '
                'Use it to quantify whether execution delay damaged the trade.'
            ),
        }

    def _ms_delta(self, chain: dict, start_key: str, end_key: str) -> Optional[int]:
        try:
            start = chain.get(start_key)
            end = chain.get(end_key)
            if start is None or end is None:
                return None
            return max(0, int(end) - int(start))
        except Exception:
            return None

    def _parse_broker_time_ms(self, value) -> Optional[int]:
        if not value:
            return None
        try:
            if isinstance(value, (int, float)):
                v = float(value)
                return int(v if v > 10_000_000_000 else v * 1000)
            text = str(value).strip()
            if not text:
                return None
            return int(datetime.fromisoformat(text.replace('Z', '+00:00')).timestamp() * 1000)
        except Exception:
            return None

    def _mark_exit_latency(self, pos: dict, key: str, value_ms: Optional[int] = None) -> dict:
        chain = dict((pos or {}).get('latency_chain') or {})
        chain[key] = int(value_ms if value_ms is not None else time.time() * 1000)
        pos['latency_chain'] = chain
        return chain

    def _regime_score(self) -> dict:
        btc = self._state_snapshot('BTC/USD').get('indicators') or {}
        spy = self._market_tape.get('SPY') or {}
        qqq = self._market_tape.get('QQQ') or {}
        proxy = self._btc_proxy_context()
        score = 0
        reasons = []
        b60 = btc.get('mom_60s')
        b15 = btc.get('mom_15s')
        if btc.get('ema_stack') == 'bull' and (b60 or 0) > 0:
            score += 2
            reasons.append('btc_bull')
        elif btc.get('ema_stack') == 'bear' and (b60 or 0) < 0:
            score -= 2
            reasons.append('btc_bear')
        if b15 is not None and b60 is not None:
            if b15 > 0.08 and b60 > 0:
                score += 1
                reasons.append('btc_positive_impulse')
            elif b15 < -0.08 and b60 < 0:
                score -= 1
                reasons.append('btc_negative_impulse')
        for name, tape in (('SPY', spy), ('QQQ', qqq)):
            day_pct = tape.get('day_pct') if isinstance(tape, dict) else None
            above_vwap = tape.get('above_vwap') if isinstance(tape, dict) else None
            if day_pct is not None and above_vwap is True and day_pct > 0:
                score += 1
                reasons.append(f'{name}_risk_on')
            elif day_pct is not None and above_vwap is False and day_pct < 0:
                score -= 1
                reasons.append(f'{name}_risk_off')
        if proxy.get('state') == 'proxy_risk_on':
            score += 1
            reasons.append('btc_proxy_risk_on')
        elif proxy.get('state') == 'proxy_risk_off':
            score -= 1
            reasons.append('btc_proxy_risk_off')
        if score >= 3:
            posture = 'long_favored'
        elif score <= -3:
            posture = 'short_favored'
        elif abs(score) <= 1:
            posture = 'mixed_selective'
        else:
            posture = 'slight_bias'
        return {'score': score, 'posture': posture, 'reasons': reasons, 'btc_mom_15s': b15, 'btc_mom_60s': b60}

    # ── session window ─────────────────────────────────────────────────
    def _in_blackout(self) -> bool:
        today = datetime.now(CT).date().isoformat()
        for lo, hi in WASH_SALE_BLACKOUTS:
            if lo <= today <= hi:
                return True
        return False

    def _in_trading_window(self) -> bool:
        now = datetime.now(CT)
        if now.weekday() >= 5:   # Sat/Sun
            return False
        # Wash-sale blackout (e.g. entire December to unwind deferred losses)
        if self._in_blackout():
            return False
        start = now.replace(hour=SESSION_START_H, minute=SESSION_START_M,
                            second=0, microsecond=0)
        end = now.replace(hour=SESSION_FLATTEN_H, minute=SESSION_FLATTEN_M,
                          second=0, microsecond=0)
        return start <= now <= end

    def _in_entry_window(self) -> bool:
        now = datetime.now(CT)
        if now.weekday() >= 5 or self._in_blackout():
            return False
        start = now.replace(hour=SESSION_START_H, minute=SESSION_START_M,
                            second=0, microsecond=0)
        entry_end = now.replace(hour=ENTRY_CUTOFF_H, minute=ENTRY_CUTOFF_M,
                                second=0, microsecond=0)
        flatten = now.replace(hour=SESSION_FLATTEN_H, minute=SESSION_FLATTEN_M,
                              second=0, microsecond=0)
        return start <= now <= min(entry_end, flatten)

    # ── daily budget snapshot (once per trading day at 08:15 CT) ──────
    def _maybe_snapshot_daily_budget(self):
        """At/after 08:15 CT on weekdays, snapshot Alpaca cash and freeze
        cash/len(TICKERS) as the per-ticker budget for the rest of the
        day. All trade sizing for the day then uses this frozen value, so
        intraday P/L drift doesn't change later trades' sizes.

        Idempotent: records the date of the snapshot; repeated calls on
        the same day (or restart mid-day) are no-ops. The morning
        snapshot stands — we don't re-snapshot after a restart.

        Uses cash (not equity) because all positions flatten at the hard-flat cutoff
        the prior day, so cash ≈ equity by 08:15 CT the next morning.
        """
        now = datetime.now(CT)
        if now.weekday() >= 5:  # Sat/Sun
            return
        if now.hour < 8 or (now.hour == 8 and now.minute < 15):
            return
        today = now.date().isoformat()
        if self.state.get('daily_budget_date') == today:
            return
        cash = 0.0
        if self.trader is not None:
            try:
                acct = self.trader.get_account()
                cash = float(acct.get('cash', 0) or 0)
            except Exception as e:
                log.error(f'daily budget snapshot: get_account failed: {e}')
                return
        else:
            # Internal-sim mode: use tracked balance
            cash = float(self.state.get('balance', 0) or 0)
        if cash <= 0:
            log.warning(f'daily budget snapshot {today}: cash=${cash:.2f}, skipping')
            return
        per_ticker = round(cash / max(1, len(TICKERS)), 2)
        with self.lock:
            self.state['daily_budget_date']       = today
            self.state['daily_budget_cash']       = round(cash, 2)
            self.state['daily_per_ticker_budget'] = per_ticker
            self._save()
        log.info(f'DAILY BUDGET {today}: cash=${cash:.2f} '
                 f'per_ticker=${per_ticker:.2f} '
                 f'(/{len(TICKERS)} tickers, 25%/trade=${per_ticker*TRADE_SIZE_PCT:.2f})')

    # ── sizing ────────────────────────────────────────────────────────
    def _get_per_ticker_budget(self) -> float:
        """Per-ticker budget for today's trades.

        Preferred path: use the frozen snapshot taken at 08:15 CT
        (`daily_per_ticker_budget`). This keeps every trade on a given
        day sized off the same base, so intraday P/L drift doesn't shrink
        or inflate later trades' sizes.

        Fallback (no snapshot for today — e.g. first trade of the day
        fires before the 08:15 timer, which shouldn't happen in practice
        since the trading window opens at 08:30): use live Alpaca equity
        divided by len(TICKERS), same as the pre-snapshot behavior.
        """
        if STEP2_SIGNAL_SCAN_LIVE_MODE:
            return float(self.state.get('balance', 0) or 0) / max(1, len(TICKERS))
        today = datetime.now(CT).date().isoformat()
        if self.state.get('daily_budget_date') == today:
            ptb = self.state.get('daily_per_ticker_budget')
            if ptb and ptb > 0:
                return float(ptb)
        # Fallback path
        if self.trader is not None:
            acct = self._get_account_cached()
            equity = float(acct.get('equity', 0) or 0)
            if equity > 0:
                return equity / len(TICKERS)
        return self.state['balance'] / max(1, len(TICKERS))

    # ── entry (signal callback) ────────────────────────────────────────
    def _num(self, value):
        try:
            if value is None:
                return None
            return round(float(value), 6)
        except Exception:
            return None

    def _ind_value(self, data: Optional[dict], *keys):
        data = data or {}
        for key in keys:
            if key in data and data.get(key) is not None:
                return data.get(key)
        return None

    def _btc_context(self, sig: dict, side: Optional[str] = None) -> dict:
        btc = sig.get('btc_indicators') or {}
        stack = sig.get('btc_stack') or btc.get('ema_stack') or btc.get('stack')
        mom_5 = self._ind_value(btc, 'mom_5s', 'mom_5')
        mom_15 = self._ind_value(btc, 'mom_15s', 'mom_15')
        mom_60 = sig.get('btc_mom')
        if mom_60 is None:
            mom_60 = self._ind_value(btc, 'mom_60s', 'mom_60')

        regime = 'neutral'
        if stack == 'bull' and (mom_60 or 0) > 0:
            regime = 'bull'
        elif stack == 'bear' and (mom_60 or 0) < 0:
            regime = 'bear'
        elif mom_60 is not None and mom_60 >= 0.10:
            regime = 'bull_momentum'
        elif mom_60 is not None and mom_60 <= -0.10:
            regime = 'bear_momentum'

        conflict = False
        agreement = False
        if side and mom_60 is not None:
            if side == 'LONG':
                conflict = (stack == 'bear' and mom_60 < 0) or mom_60 <= -0.10
                agreement = stack == 'bull' and mom_60 > 0
            else:
                conflict = (stack == 'bull' and mom_60 > 0) or mom_60 >= 0.10
                agreement = stack == 'bear' and mom_60 < 0

        return {
            'stack': stack,
            'regime': regime,
            'agreement': agreement,
            'conflict': conflict,
            'mom_5s': self._num(mom_5),
            'mom_15s': self._num(mom_15),
            'mom_60s': self._num(mom_60),
        }

    def _setup_type(self, sig: dict) -> str:
        if sig.get('setup_type'):
            return str(sig.get('setup_type'))
        text = ' '.join(str(r).lower() for r in sig.get('reasons', []) or [])
        ind = sig.get('indicators') or {}
        if 'fade strong flow' in text or 'fade weak flow' in text:
            return 'flow_exhaustion_fade'
        if 'vwap' in text and abs(float(ind.get('vwap_dist_sigma') or 0)) >= 1.5:
            return 'vwap_extension_fade'
        if 'bull stack' in text or 'bear stack' in text:
            return 'trend_continuation'
        if 'vol burst' in text:
            return 'breakout_breakdown'
        return 'mixed'

    def _forensics(self, sig: dict, price: float) -> dict:
        tkr = sig.get('ticker')
        side = sig.get('side')
        ind = sig.get('indicators') or {}
        btc = self._btc_context(sig, side)
        session_high = sig.get('session_high')
        session_low = sig.get('session_low')
        range_pct = None
        if session_high is not None and session_low is not None and session_high != session_low:
            if side == 'SHORT':
                range_pct = (session_high - price) / (session_high - session_low)
            else:
                range_pct = (price - session_low) / (session_high - session_low)

        stock_mom_60 = self._ind_value(ind, 'mom_60s', 'mom_60')
        btc_mom_60 = btc.get('mom_60s')
        beta = BTC_BETA.get(tkr)
        implied = beta * btc_mom_60 if beta is not None and btc_mom_60 is not None else None
        rel = None
        if stock_mom_60 is not None and implied is not None:
            rel = float(stock_mom_60) - implied

        best_bid = sig.get('best_bid')
        best_ask = sig.get('best_ask')
        spread = sig.get('spread')
        mid = sig.get('mid_price')
        quote_ts = sig.get('quote_ts') or sig.get('quote_time') or sig.get('last_quote_ts')
        if spread is None and best_bid is not None and best_ask is not None:
            spread = float(best_ask) - float(best_bid)
        if mid is None and best_bid is not None and best_ask is not None:
            mid = (float(best_bid) + float(best_ask)) / 2
        quote_age_sec = None
        if quote_ts:
            try:
                qts = float(quote_ts)
                if qts > 10_000_000_000:
                    qts /= 1000.0
                quote_age_sec = max(0.0, time.time() - qts)
            except Exception:
                quote_age_sec = None

        signal_btc = sig.get('btc_context') if isinstance(sig.get('btc_context'), dict) else None
        signal_rs = sig.get('relative_strength') if isinstance(sig.get('relative_strength'), dict) else None
        market_microstructure = self._market_microstructure_context(tkr, side, price, ind)
        gap_context = market_microstructure.get('prior_close_context') or self._prior_close_context(tkr, price)
        borrow = self._asset_info_snapshot(tkr, critical=False, allow_fetch=False) if side == 'SHORT' else {}
        catalyst = self._catalyst_context(tkr)
        liquidity = self._liquidity_impact(sig, price, side)
        btc_proxy = self._btc_proxy_context()

        return {
            'captured_at': int(time.time()),
            'source': sig.get('_source', 'ws_scalp'),
            'setup_type': self._setup_type(sig),
            'signal': {
                'price': self._num(price),
                'score': sig.get('score'),
                'conviction': sig.get('conviction'),
                'reasons': sig.get('reasons', []),
            },
            'entry_quality': {
                'bid': self._num(best_bid),
                'ask': self._num(best_ask),
                'mid': self._num(mid),
                'spread': self._num(spread),
                'spread_pct': self._num((spread / price * 100) if spread is not None and price else None),
                'quote_age_sec': self._num(quote_age_sec),
                'quote_stale': quote_age_sec is not None and quote_age_sec > QUOTE_STALE_SEC,
                'last_trade_exchange': sig.get('last_trade_exchange'),
                'last_trade_conditions': sig.get('last_trade_conditions') or ind.get('last_trade_conditions') or [],
                'last_trade_tape': sig.get('last_trade_tape'),
                'last_quote_bid_exchange': sig.get('last_quote_bid_exchange') or ind.get('last_quote_bid_exchange'),
                'last_quote_ask_exchange': sig.get('last_quote_ask_exchange') or ind.get('last_quote_ask_exchange'),
                'last_quote_conditions': sig.get('last_quote_conditions') or ind.get('last_quote_conditions') or [],
                'last_quote_tape': sig.get('last_quote_tape') or ind.get('last_quote_tape'),
                'quote_state': sig.get('quote_state') or ind.get('quote_state') or {},
                'condition_quality': sig.get('condition_quality') or ind.get('condition_quality') or {},
                'rolling_spread_median_pct': ind.get('rolling_spread_median_pct'),
                'rolling_spread_p95_pct': ind.get('rolling_spread_p95_pct'),
                'spread_vs_rolling_median': sig.get('spread_vs_rolling_median') or ind.get('spread_vs_rolling_median'),
                'spread_abnormal': bool(sig.get('spread_abnormal') or ind.get('spread_abnormal')),
            },
            'strategy': {
                'config_hash': STRATEGY_CONFIG_HASH,
                'entry_fill_timeout_sec': ENTRY_FILL_TIMEOUT_SEC,
                'max_entry_spread_pct': MAX_ENTRY_SPREAD_PCT,
            },
            'btc': signal_btc or btc,
            'miner_basket': sig.get('miner_basket') or {},
            'relative_strength': signal_rs or {
                'btc_beta': beta,
                'stock_mom_60s': self._num(stock_mom_60),
                'btc_mom_60s': btc_mom_60,
                'btc_implied_stock_mom_60s': self._num(implied),
                'stock_minus_btc_implied_60s': self._num(rel),
            },
            'market_microstructure': market_microstructure,
            'gap_context': gap_context,
            'liquidity_impact': liquidity,
            'short_borrow_snapshot': borrow,
            'catalyst_context': catalyst,
            'btc_proxy_basket': btc_proxy,
            'signal_quality': sig.get('signal_quality') or {},
            'shadow_dual_side_score': sig.get('shadow_dual_side_score')
                                      or (sig.get('signal_quality') or {}).get('score_model', {}).get('dual_side_shadow'),
            'location': {
                'session_elapsed_min': sig.get('session_elapsed_min'),
                'session_high': self._num(session_high),
                'session_low': self._num(session_low),
                'range_pct_from_side': self._num(range_pct),
                'opening_5m_high': self._num(ind.get('opening_5m_high')),
                'opening_5m_low': self._num(ind.get('opening_5m_low')),
                'opening_15m_high': self._num(ind.get('opening_15m_high')),
                'opening_15m_low': self._num(ind.get('opening_15m_low')),
                'opening_5m_break_state': ind.get('opening_5m_break_state'),
                'opening_15m_break_state': ind.get('opening_15m_break_state'),
                'dist_from_session_high_pct': self._num(((price - session_high) / session_high * 100)
                                                        if session_high else None),
                'dist_from_session_low_pct': self._num(((price - session_low) / session_low * 100)
                                                       if session_low else None),
                'vwap': self._num(ind.get('vwap')),
                'vwap_dist': self._num(ind.get('vwap_dist')),
                'vwap_dist_sigma': self._num(ind.get('vwap_dist_sigma')),
                'gap_pct': self._num(sig.get('gap_pct')),
            },
            'market_tape': {
                'SPY': self._market_tape.get('SPY') or {
                    'above_vwap': sig.get('spy_above_vwap'),
                    'day_pct': self._num(sig.get('spy_day_pct')),
                },
                'QQQ': self._market_tape.get('QQQ') or sig.get('qqq_tape'),
                'IWM': self._market_tape.get('IWM') or sig.get('iwm_tape'),
            },
            'execution_quality': sig.get('execution_quality') or {},
            'shadow_variants': sig.get('shadow_variants') or {},
            'lead_lag': sig.get('lead_lag') or {},
            'session_phase': sig.get('session_phase'),
            'setup_state': sig.get('setup_state') or {},
        }

    def _entry_quality_tier(self, sig: dict, forensics: dict) -> dict:
        side = sig.get('side')
        score = 50
        tags = []
        try:
            raw_score = abs(float(sig.get('score') or 0))
            if raw_score >= 8:
                score += 15
                tags.append('strong_signal_score')
            elif raw_score >= 6:
                score += 8
                tags.append('solid_signal_score')
            else:
                score -= 8
                tags.append('thin_signal_score')
        except Exception:
            tags.append('score_unknown')
        btc = forensics.get('btc') or {}
        if btc.get('conflict'):
            score -= 25
            tags.append('btc_conflict')
        elif btc.get('agreement') is True:
            score += 12
            tags.append('btc_aligned')
        elif btc.get('agreement') is False:
            score -= 8
            tags.append('btc_not_confirming')
        eq = forensics.get('entry_quality') or {}
        spread_pct = eq.get('spread_pct')
        try:
            if spread_pct is not None:
                spread_pct = float(spread_pct)
                if spread_pct <= 0.04:
                    score += 8
                    tags.append('tight_spread')
                elif spread_pct >= 0.08:
                    score -= 12
                    tags.append('wide_spread')
        except Exception:
            pass
        if eq.get('quote_stale'):
            score -= 20
            tags.append('quote_stale')
        qstate = (eq.get('quote_state') or {}).get('state')
        if qstate in ('locked', 'crossed'):
            score -= 20
            tags.append(f'quote_{qstate}')
        elif qstate in ('missing', 'one_sided', 'invalid'):
            score -= 12
            tags.append(f'quote_{qstate}')
        if eq.get('spread_abnormal'):
            score -= 10
            tags.append('spread_abnormal_vs_ticker_baseline')
        cq = eq.get('condition_quality') or {}
        try:
            if cq.get('score') is not None and float(cq.get('score')) < 90:
                score -= 8
                tags.append('condition_quality_watch')
        except Exception:
            pass
        rel = forensics.get('relative_strength') or {}
        rs = rel.get('stock_minus_btc_implied_60s')
        try:
            if rs is not None:
                rs = float(rs)
                if side == 'LONG' and rs > 0:
                    score += 7
                    tags.append('stock_confirmed_vs_btc')
                elif side == 'SHORT' and rs < 0:
                    score += 7
                    tags.append('stock_confirmed_vs_btc')
                elif side == 'LONG' and rs < -0.10:
                    score -= 10
                    tags.append('stock_lagged_btc')
                elif side == 'SHORT' and rs > 0.10:
                    score -= 10
                    tags.append('stock_outperformed_btc')
        except Exception:
            pass
        signal_quality = forensics.get('signal_quality') or {}
        if signal_quality.get('flow_confirmed') or signal_quality.get('burst') or signal_quality.get('followthrough'):
            score += 6
            tags.append('flow_or_followthrough_confirmed')
        score = max(0, min(100, int(round(score))))
        tier = 'A' if score >= 75 else ('B' if score >= 58 else 'C')
        return {
            'tier': tier,
            'grade': tier,
            'score': score,
            'tags': tags,
            'label': (
                'high_quality_entry'
                if tier == 'A' else
                'acceptable_entry'
                if tier == 'B' else
                'marginal_entry'
            ),
        }

    def _record_skipped_signal(self, sig: dict, reason: str):
        try:
            price = sig.get('price')
            tkr = sig.get('ticker')
            side = sig.get('side')
            if tkr not in TICKERS or side not in ('LONG', 'SHORT') or not price:
                return
            row = {
                'created_at': int(time.time()),
                'ticker': tkr,
                'side': side,
                'price': round(float(price), 4),
                'reason': reason,
                'conviction': sig.get('conviction'),
                'score': sig.get('score'),
                'reasons': sig.get('reasons', []),
                'strategy_config_hash': STRATEGY_CONFIG_HASH,
                'indicators': sig.get('indicators'),
                'btc_indicators': sig.get('btc_indicators'),
                'btc_context': sig.get('btc_context') or {},
                'shadow_dual_side_score': sig.get('shadow_dual_side_score')
                                          or (sig.get('signal_quality') or {}).get('score_model', {}).get('dual_side_shadow'),
                'forensics': self._forensics(sig, float(price)),
                'fwd_targets': {
                    '5m': int(time.time()) + 300,
                    '15m': int(time.time()) + 900,
                    '30m': int(time.time()) + 1800,
                },
                'fwd': {},
            }
            parity_row = self._append_live_signal_parity(
                sig,
                decision='skipped',
                reason=reason,
                created_at=row['created_at'],
                forensics=row.get('forensics') or {},
                extra={
                    'skip_source': '_record_skipped_signal',
                    'fwd_targets': row.get('fwd_targets') or {},
                },
            )
            if parity_row:
                row['live_signal_parity_key'] = parity_row.get('parity_key')
                row['live_signal_parity_feature_hash'] = parity_row.get('feature_snapshot_hash')
            self._append_skipped_corpus(row)
            self._maybe_capture_skipped_signal_ticks(row, sig, reason)
            self._append_rule_dry_run_event('signal_skipped', {
                'created_at': row['created_at'],
                'ticker': tkr,
                'side': side,
                'decision': 'skipped',
                'reason': reason,
                'score': sig.get('score'),
                'setup_type': sig.get('setup_type') or self._setup_type(sig),
                'near_miss_rules': self._entry_rule_near_misses(
                    sig,
                    row.get('forensics') or {},
                    (row.get('forensics') or {}).get('entry_quality_tier') or {},
                ),
                'forensics': row.get('forensics') or {},
                'mode': 'passive_rule_dry_run',
            })
            try:
                from event_store import record_event
                record_event('skipped_signal', row, symbol=tkr, ts=row.get('created_at'))
            except Exception:
                pass
            self._append_shadow_decision(sig, 'skipped', reason, row.get('forensics'))
            with self.lock:
                skipped = self.state.setdefault('skipped_signals', [])
                skipped.append(row)
                if len(skipped) > 2000:
                    del skipped[:-2000]
                self._save()
            self._audit_event('signal_skipped', tkr, {
                'side': side,
                'reason': reason,
                'price': round(float(price), 4),
                'score': sig.get('score'),
                'conviction': sig.get('conviction'),
            })
        except Exception as e:
            log.warning(f'record skipped signal failed: {e}')

    def _update_skipped_forwards(self):
        # Forward returns for skipped signals are now computed from timestamped
        # 1-minute bars in daily_postmortem.py. Live last-price snapshots were
        # too easy to skew when the monitor loop lagged or a quote went stale.
        return

    def _refresh_market_tape(self):
        if not BAR_DATA_AVAILABLE or not self._in_trading_window():
            return
        tape_symbols = tuple(dict.fromkeys((*MARKET_CONTEXT_SYMBOLS, *BTC_PROXY_SYMBOLS)))
        if not tape_symbols:
            self._market_tape = {}
            self._market_tape_bars = {}
            self._spy_ind = {}
            return
        now_ct = datetime.now(CT)
        sess_start_ct = now_ct.replace(
            hour=SESSION_START_H, minute=SESSION_START_M,
            second=0, microsecond=0)
        end_iso = (now_ct + timedelta(minutes=1)).astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        tape = {}
        for sym in tape_symbols:
            try:
                cache = self._market_tape_bars.setdefault(sym, {})
                if cache:
                    last_ms = max(cache)
                    start_dt = datetime.fromtimestamp(last_ms / 1000, tz=timezone.utc) - timedelta(minutes=2)
                else:
                    start_dt = sess_start_ct.astimezone(timezone.utc)
                start_iso = start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                bars = _bar_fetch_stock(sym, start_iso, end_iso, '1Min')
                for b in bars:
                    cache[b['t']] = b
                session_start_ms = int(sess_start_ct.astimezone(timezone.utc).timestamp() * 1000)
                bars = sorted((b for ts, b in cache.items() if ts >= session_start_ms),
                              key=lambda b: b['t'])
                if not bars:
                    continue
                vol = sum(b.get('v', 0) for b in bars) or 1
                vwap = sum((b['h'] + b['l'] + b['c']) / 3 * b.get('v', 0) for b in bars) / vol
                px = bars[-1]['c']
                op = bars[0]['o']
                tape[sym] = {
                    'price': round(px, 4),
                    'vwap': round(vwap, 4),
                    'above_vwap': px > vwap,
                    'day_pct': round((px - op) / op * 100, 3) if op else None,
                    'bars': len(bars),
                    'updated_at': int(time.time()),
                }
            except Exception as e:
                log.warning(f'market tape refresh failed {sym}: {e}')
        if tape:
            self._market_tape.update(tape)
            self._spy_ind = self._market_tape.get('SPY', self._spy_ind)
        for tkr in TICKERS:
            try:
                self._get_avg_daily_vol(tkr)
            except Exception:
                pass
            if self.trader is not None:
                try:
                    self._asset_info_snapshot(tkr, critical=False, allow_fetch=True)
                except Exception:
                    pass

    def _setup_key(self, sig_or_pos: dict) -> str:
        tkr = sig_or_pos.get('ticker') or sig_or_pos.get('symbol') or '?'
        side = sig_or_pos.get('side') or '?'
        setup = sig_or_pos.get('setup_type')
        if not setup:
            fx = sig_or_pos.get('forensics') or {}
            setup = fx.get('setup_type')
        return f'{tkr}:{side}:{setup or "unknown"}'

    def _setup_pause_active(self, sig: dict) -> Optional[dict]:
        key = self._setup_key(sig)
        now = int(time.time())
        with self.lock:
            pause = (self.state.get('setup_pauses') or {}).get(key)
            if pause and int(pause.get('until', 0)) > now:
                return dict(pause)
            if pause:
                self.state.setdefault('setup_pauses', {}).pop(key, None)
                self._save()
        return None

    def _market_regime_conflict(self, side: str) -> Optional[str]:
        if not MARKET_REGIME_FILTER_ENABLED:
            return None
        tape = self._market_tape or {}
        qqq = tape.get('QQQ') or {}
        spy = tape.get('SPY') or {}
        iwm = tape.get('IWM') or {}
        qqq_pct = qqq.get('day_pct')
        spy_pct = spy.get('day_pct')
        iwm_pct = iwm.get('day_pct')
        qqq_above = qqq.get('above_vwap')
        spy_above = spy.get('above_vwap')
        iwm_above = iwm.get('above_vwap')
        if side == 'LONG':
            if qqq_pct is not None and qqq_pct <= -0.45 and qqq_above is False:
                return 'qqq_downtrend'
            if spy_pct is not None and spy_pct <= -0.35 and spy_above is False:
                return 'spy_downtrend'
            if iwm_pct is not None and iwm_pct <= -0.40 and iwm_above is False:
                return 'iwm_downtrend'
        else:
            if qqq_pct is not None and qqq_pct >= 0.45 and qqq_above is True:
                return 'qqq_uptrend'
            if spy_pct is not None and spy_pct >= 0.35 and spy_above is True:
                return 'spy_uptrend'
            if iwm_pct is not None and iwm_pct >= 0.40 and iwm_above is True:
                return 'iwm_uptrend'
        return None

    def _open_unrealized_pnl(self) -> float:
        with self.lock:
            positions = json.loads(json.dumps(self.state.get('positions', {})))
        unreal = 0.0
        for tkr, pos in positions.items():
            try:
                price = self.engine.get_last_price(tkr)
                if not price:
                    continue
                if pos.get('side') == 'LONG':
                    unreal += (float(price) - float(pos.get('entry') or 0)) * float(pos.get('qty') or 0)
                else:
                    unreal += (float(pos.get('entry') or 0) - float(price)) * float(pos.get('qty') or 0)
            except Exception:
                continue
        return round(unreal, 2)

    def _risk_off_reason(self) -> Optional[str]:
        now_day = datetime.now(CT).date().isoformat()
        with self.lock:
            trades = [
                t for t in self.state.get('trades', [])
                if t.get('closed_at')
                and datetime.fromtimestamp(t.get('closed_at'), CT).date().isoformat() == now_day
            ]
            start_balance = float(self.state.get('start_balance') or START_BALANCE)
            balance = float(self.state.get('balance') or start_balance)
        unreal = self._open_unrealized_pnl()
        effective_balance = balance + unreal
        if MAX_CONSECUTIVE_LOSSES > 0:
            streak = 0
            last_loss_closed_at = None
            for t in reversed(trades):
                if t.get('reason') in RISK_OFF_IGNORE_REASONS:
                    continue
                if float(t.get('pnl') or 0) < 0:
                    streak += 1
                    last_loss_closed_at = last_loss_closed_at or int(t.get('closed_at') or 0)
                else:
                    break
            if streak >= MAX_CONSECUTIVE_LOSSES:
                if RISK_OFF_COOLDOWN_MIN > 0 and last_loss_closed_at:
                    elapsed_min = (int(time.time()) - last_loss_closed_at) / 60
                    if elapsed_min < RISK_OFF_COOLDOWN_MIN:
                        return f'consecutive_losses_{streak}'
                elif RISK_OFF_COOLDOWN_MIN <= 0:
                    return f'consecutive_losses_{streak}'
        if MAX_DAILY_LOSS_PCT > 0 and start_balance > 0:
            dd_pct = max(0.0, (start_balance - effective_balance) / start_balance * 100)
            if dd_pct >= MAX_DAILY_LOSS_PCT:
                return f'daily_drawdown_{dd_pct:.2f}pct_including_open_unrealized'
        return None

    def _live_quality_gate_reason(self) -> Optional[str]:
        if not LIVE_QUALITY_GATE_ENABLED:
            return None
        try:
            from elite_control import live_entry_gate
            gate = live_entry_gate(self, min_score=LIVE_QUALITY_MIN_SCORE)
            if not gate.get('allowed'):
                reasons = gate.get('reasons') or []
                reason_txt = ','.join(str(r) for r in reasons[:4]) or gate.get('posture') or 'low_quality'
                return f"score_{gate.get('score')}_lt_{gate.get('min_score')}:{reason_txt}"
        except Exception as e:
            log.warning(f'live quality gate unavailable: {e}')
            return 'live_quality_gate_unavailable'
        return None

    def _trade_size_pct_now(self, sig: Optional[dict] = None) -> float:
        if STEP_PARITY_LIVE_MODE:
            return round(max(0.01, TRADE_SIZE_PCT), 4)
        if DRAWDOWN_SIZE_CUT_PCT <= 0:
            base = TRADE_SIZE_PCT
        else:
            with self.lock:
                start_balance = float(self.state.get('start_balance') or START_BALANCE)
                balance = float(self.state.get('balance') or start_balance)
            dd_pct = max(0.0, (start_balance - balance) / start_balance * 100) if start_balance else 0.0
            base = min(TRADE_SIZE_PCT, DRAWDOWN_SIZE_CUT_TRADE_SIZE_PCT) \
                if dd_pct >= DRAWDOWN_SIZE_CUT_PCT else TRADE_SIZE_PCT
        setup = (sig or {}).get('setup_type')
        try:
            mult = float(SETUP_SIZE_MULTIPLIER.get(setup, 1.0))
        except Exception:
            mult = 1.0
        if BAD_SLIPPAGE_REDUCE_ENABLED:
            try:
                with self.lock:
                    recent = list(self.state.get('trades', []))[-BAD_SLIPPAGE_LOOKBACK_TRADES:]
                bad = [
                    t for t in recent
                    if t.get('entry_slippage_pct') is not None
                    and float(t.get('entry_slippage_pct') or 0) >= BAD_SLIPPAGE_THRESHOLD_PCT
                ]
                if len(bad) >= BAD_SLIPPAGE_COUNT:
                    mult *= BAD_SLIPPAGE_SIZE_MULTIPLIER
            except Exception:
                pass
        return round(max(0.01, min(TRADE_SIZE_PCT, base * mult)), 4)

    def _adaptive_brackets(self, tkr: str, side: str, price: float,
                           sig: dict, base_sl: float, base_tp: float) -> dict:
        sl_pct = float(base_sl)
        tp_pct = float(base_tp)
        if FIXED_BRACKETS_ENABLED:
            fixed = FIXED_BRACKETS.get(tkr) if isinstance(FIXED_BRACKETS, dict) else None
            if isinstance(fixed, dict):
                sl_pct = float(fixed.get('sl', sl_pct))
                tp_pct = float(fixed.get('tp', tp_pct))
                return {
                    'sl_pct': round(sl_pct, 5),
                    'tp_pct': round(tp_pct, 5),
                    'notes': ['fixed_brackets'],
                }
        if not ADAPTIVE_BRACKETS_ENABLED:
            return {'sl_pct': sl_pct, 'tp_pct': tp_pct, 'notes': ['static']}
        setup = sig.get('setup_type') or self._setup_type(sig)
        quality = sig.get('signal_quality') or {}
        btc = sig.get('btc_context') or {}
        notes = []
        if setup in ('momentum_breakout', 'btc_relative_strength'):
            tp_pct *= 1.20
            notes.append('momentum_tp_expand')
            if quality.get('burst'):
                sl_pct *= 0.90
                notes.append('burst_sl_tighten')
        elif setup in ('flow_exhaustion_fade', 'vwap_reclaim_breakdown'):
            tp_pct *= 0.85
            sl_pct *= 0.85
            notes.append('mean_reversion_tighter_bracket')
            if setup == 'flow_exhaustion_fade':
                tp_pct *= 0.90
                notes.append('fast_fade_take_profit')
        if btc.get('acceleration') in ('bull_accelerating', 'bear_accelerating'):
            aligned = ((side == 'LONG' and str(btc.get('acceleration')).startswith('bull'))
                       or (side == 'SHORT' and str(btc.get('acceleration')).startswith('bear')))
            if aligned:
                tp_pct *= 1.10
                notes.append('btc_accel_tp_expand')
        try:
            disaster_cap = float(BROKER_DISASTER_STOP_BY_TICKER.get(tkr, 0) or 0)
        except Exception:
            disaster_cap = 0.0
        if disaster_cap > 0:
            sl_pct = min(sl_pct, disaster_cap)
            notes.append(f'broker_disaster_stop_cap_{disaster_cap:.3f}')
        # Hard guardrails: do not let adaptive behavior get clever enough to be reckless.
        sl_floor = 0.006 if disaster_cap > 0 else 0.025
        sl_pct = min(max(sl_pct, sl_floor), 0.055)
        tp_pct = min(max(tp_pct, 0.006), 0.030)
        return {
            'sl_pct': round(sl_pct, 5),
            'tp_pct': round(tp_pct, 5),
            'notes': notes or ['base'],
        }

    def _shadow_exit_reason(self, policy: str, cfg: dict, elapsed: int,
                            signed_pct: float, mfe_pct: float) -> Optional[str]:
        hard = cfg.get('hard_adverse_pct')
        if hard is not None and signed_pct <= -float(hard):
            return f'shadow_{policy}:hard_adverse_{hard}'
        failed_after = cfg.get('failed_after_sec')
        if failed_after is not None and elapsed >= int(failed_after):
            min_mfe = float(cfg.get('failed_min_mfe_pct', 0.12))
            if mfe_pct < min_mfe and signed_pct <= 0:
                return f'shadow_{policy}:failed_followthrough_{failed_after}s'
        trigger = cfg.get('profit_trigger_pct')
        if trigger is not None and mfe_pct >= float(trigger):
            giveback = 1.0 - (signed_pct / max(mfe_pct, 0.0001))
            if giveback >= float(cfg.get('profit_giveback_pct', 0.55)):
                return f'shadow_{policy}:profit_giveback_{trigger}'
        return None

    def _check_shadow_exit_policies(self, tkr: str, pos: dict, price: float, elapsed: int,
                                    signed_pct: float, mfe_pct: float):
        if not SHADOW_EXIT_POLICIES_ENABLED or not isinstance(SHADOW_EXIT_POLICIES, dict):
            return
        fired = pos.setdefault('shadow_exit_fired', {})
        for policy, cfg in SHADOW_EXIT_POLICIES.items():
            if not isinstance(cfg, dict) or policy in fired:
                continue
            reason = self._shadow_exit_reason(policy, cfg, elapsed, signed_pct, mfe_pct)
            if not reason:
                continue
            now = int(time.time())
            fired[policy] = {
                'created_at': now,
                'reason': reason,
                'price': round(float(price), 4),
                'signed_pct': round(signed_pct, 4),
                'mfe_pct': round(mfe_pct, 4),
                'elapsed_sec': elapsed,
            }
            self._append_shadow_exit({
                'created_at': now,
                'created_at_ct': datetime.fromtimestamp(now, CT).isoformat(timespec='seconds'),
                'ticker': tkr,
                'trade_id': pos.get('trade_id'),
                'side': pos.get('side'),
                'policy': policy,
                'reason': reason,
                'price': round(float(price), 4),
                'entry': round(float(pos.get('entry') or 0), 4),
                'qty': pos.get('qty'),
                'elapsed_sec': elapsed,
                'signed_pct': round(signed_pct, 4),
                'mfe_pct': round(mfe_pct, 4),
                'policy_config': cfg,
                'live_exit_reason': None,
                'note': 'Shadow-only candidate exit; live position was not closed by this event.',
            })

    def _smart_exit_reason(self, tkr: str, pos: dict, price: float) -> Optional[str]:
        if STEP_PARITY_LIVE_MODE:
            return None
        if self.trader is not None and not pos.get('alpaca_filled_entry'):
            return None
        side = pos.get('side')
        entry = float(pos.get('entry') or 0)
        if not entry or side not in ('LONG', 'SHORT'):
            return None
        now = int(time.time())
        elapsed = max(0, now - int(pos.get('entry_ts') or now))
        signed = (price - entry) if side == 'LONG' else (entry - price)
        signed_pct = signed / entry * 100
        best_px = float(pos.get('_best_price') or entry)
        worst_px = float(pos.get('_worst_price') or entry)
        mfe = (best_px - entry) if side == 'LONG' else (entry - best_px)
        mfe_pct = mfe / entry * 100 if entry else 0
        mae = (entry - worst_px) if side == 'LONG' else (worst_px - entry)
        mae_pct = mae / entry * 100 if entry else 0
        tkr = pos.get('ticker') or tkr
        self._check_shadow_exit_policies(tkr, pos, price, elapsed, signed_pct, mfe_pct)
        candidates = []

        def _candidate(reason: str, kind: str, thresholds: Optional[dict] = None,
                       context: Optional[dict] = None) -> None:
            candidates.append({
                'reason': reason,
                'kind': kind,
                'priority': len(candidates) + 1,
                'signed_pct': round(signed_pct, 4),
                'mfe_pct': round(mfe_pct, 4),
                'elapsed_sec': elapsed,
                'thresholds': thresholds or {},
                'context': context or {},
            })

        def _finish() -> Optional[str]:
            if not candidates:
                pos['exit_reason_hierarchy'] = {
                    'created_at': now,
                    'selected': None,
                    'candidates': [],
                    'signed_pct': round(signed_pct, 4),
                    'mfe_pct': round(mfe_pct, 4),
                    'elapsed_sec': elapsed,
                }
                return None
            selected = candidates[0]
            if selected.get('context'):
                pos['exit_decision_context'] = selected['context']
            pos['exit_reason_hierarchy'] = {
                'created_at': now,
                'selected': selected['reason'],
                'candidates': list(candidates),
                'note': 'All live exit candidates were evaluated; first candidate by live priority is selected.',
            }
            return selected['reason']

        adverse_limit = float(ADVERSE_EXIT_BY_TICKER.get(tkr, ADVERSE_EXIT_PCT))
        if signed_pct <= -adverse_limit:
            context = {
                'kind': 'hard_adverse_exit',
                'score': None,
                'tags': ['adverse_limit'],
                'signed_pct': round(signed_pct, 4),
                'mfe_pct': round(mfe_pct, 4),
                'elapsed_sec': elapsed,
                'thresholds': {'adverse_limit_pct': adverse_limit},
            }
            _candidate(
                f'hard_adverse_exit_{signed_pct:.3f}pct',
                'hard_adverse_exit',
                {'adverse_limit_pct': adverse_limit},
                context,
            )
        conviction_decay = self._short_conviction_decay_reason(
            tkr, pos, price, elapsed, signed_pct, mfe_pct,
        ) or self._long_conviction_decay_reason(tkr, pos, price, elapsed, signed_pct, mfe_pct)
        if conviction_decay:
            _candidate(conviction_decay, 'conviction_decay', context=pos.get('exit_decision_context') or {})
        setup_type = ((pos.get('entry_thesis') or {}).get('setup_type') or pos.get('setup_type'))
        session_phase = pos.get('session_phase') or ((pos.get('entry_thesis') or {}).get('session_phase'))
        path_failure_due = (
            PATH_FAILURE_REDUCE_ENABLED
            and PATH_FAILURE_REDUCE_AFTER_SEC > 0
            and elapsed >= PATH_FAILURE_REDUCE_AFTER_SEC
            and (
                PATH_FAILURE_REDUCE_WINDOW_SEC <= 0
                or elapsed <= PATH_FAILURE_REDUCE_AFTER_SEC + PATH_FAILURE_REDUCE_WINDOW_SEC
            )
            and not pos.get('path_failure_reduced')
            and (PATH_FAILURE_REDUCE_SETUP == 'all' or setup_type == PATH_FAILURE_REDUCE_SETUP)
            and (PATH_FAILURE_REDUCE_SESSION_PHASE == 'all' or session_phase == PATH_FAILURE_REDUCE_SESSION_PHASE)
        )
        if path_failure_due:
            edge_pct = mfe_pct - mae_pct
            btc_entry = float(pos.get('path_failure_entry_btc_price') or 0.0)
            btc_now = float(self.engine.get_last_price('BTC/USD') or 0.0)
            btc_move = 0.0
            if btc_entry > 0 and btc_now > 0:
                btc_raw = (btc_now - btc_entry) / btc_entry * 100.0
                btc_move = btc_raw if side == 'LONG' else -btc_raw
            rel_pct = signed_pct - btc_move
            rel_bad = (
                PATH_FAILURE_REDUCE_REL_THRESHOLD_PCT is None
                or rel_pct <= PATH_FAILURE_REDUCE_REL_THRESHOLD_PCT
            )
            if edge_pct <= PATH_FAILURE_REDUCE_EDGE_THRESHOLD_PCT and rel_bad:
                pos['path_failure_reduced'] = True
                context = {
                    'kind': 'path_failure_reduce',
                    'score': None,
                    'tags': ['path_failure', 'edge', 'relative'],
                    'signed_pct': round(signed_pct, 4),
                    'mfe_pct': round(mfe_pct, 4),
                    'mae_pct': round(mae_pct, 4),
                    'edge_pct': round(edge_pct, 4),
                    'rel_pct': round(rel_pct, 4),
                    'btc_move_pct': round(btc_move, 4),
                    'elapsed_sec': elapsed,
                    'thresholds': {
                        'after_sec': PATH_FAILURE_REDUCE_AFTER_SEC,
                        'window_sec': PATH_FAILURE_REDUCE_WINDOW_SEC,
                        'edge_threshold_pct': PATH_FAILURE_REDUCE_EDGE_THRESHOLD_PCT,
                        'rel_threshold_pct': PATH_FAILURE_REDUCE_REL_THRESHOLD_PCT,
                        'reduce_fraction': PATH_FAILURE_REDUCE_FRACTION,
                    },
                }
                _candidate(
                    'path_failure_reduce',
                    'path_failure_reduce',
                    context['thresholds'],
                    context,
                )
        if (elapsed >= FAILED_FOLLOWTHROUGH_AFTER_SEC
                and mfe_pct < FAILED_FOLLOWTHROUGH_MIN_MFE_PCT
                and signed_pct <= 0):
            context = {
                'kind': 'failed_followthrough',
                'score': None,
                'tags': ['low_mfe', 'not_green'],
                'signed_pct': round(signed_pct, 4),
                'mfe_pct': round(mfe_pct, 4),
                'elapsed_sec': elapsed,
                'thresholds': {
                    'after_sec': FAILED_FOLLOWTHROUGH_AFTER_SEC,
                    'min_mfe_pct': FAILED_FOLLOWTHROUGH_MIN_MFE_PCT,
                },
            }
            _candidate(
                f'failed_followthrough_{elapsed}s_mfe={mfe_pct:.3f}_pnl={signed_pct:.3f}',
                'failed_followthrough',
                {
                    'after_sec': FAILED_FOLLOWTHROUGH_AFTER_SEC,
                    'min_mfe_pct': FAILED_FOLLOWTHROUGH_MIN_MFE_PCT,
                },
                context,
            )
        if PROFIT_PROTECT_ENABLED and mfe_pct >= PROFIT_PROTECT_TRIGGER_PCT:
            giveback = 1.0 - (signed_pct / max(mfe_pct, 0.0001))
            if giveback >= PROFIT_PROTECT_GIVEBACK_PCT:
                context = {
                    'kind': 'profit_protect_giveback',
                    'score': None,
                    'tags': ['giveback'],
                    'signed_pct': round(signed_pct, 4),
                    'mfe_pct': round(mfe_pct, 4),
                    'elapsed_sec': elapsed,
                    'thresholds': {
                        'trigger_pct': PROFIT_PROTECT_TRIGGER_PCT,
                        'giveback_pct': PROFIT_PROTECT_GIVEBACK_PCT,
                    },
                }
                _candidate(
                    'profit_protect_giveback',
                    'profit_protect_giveback',
                    {
                        'trigger_pct': PROFIT_PROTECT_TRIGGER_PCT,
                        'giveback_pct': PROFIT_PROTECT_GIVEBACK_PCT,
                    },
                    context,
                )
        if (PROFIT_PROTECT_ENABLED and elapsed >= BREAKEVEN_AFTER_SEC
                and mfe_pct >= BREAKEVEN_MIN_GREEN_PCT and signed_pct <= 0.03):
            context = {
                'kind': 'profit_protect_breakeven',
                'score': None,
                'tags': ['breakeven'],
                'signed_pct': round(signed_pct, 4),
                'mfe_pct': round(mfe_pct, 4),
                'elapsed_sec': elapsed,
                'thresholds': {
                    'after_sec': BREAKEVEN_AFTER_SEC,
                    'min_green_pct': BREAKEVEN_MIN_GREEN_PCT,
                },
            }
            _candidate(
                'profit_protect_breakeven',
                'profit_protect_breakeven',
                {
                    'after_sec': BREAKEVEN_AFTER_SEC,
                    'min_green_pct': BREAKEVEN_MIN_GREEN_PCT,
                },
                context,
            )
        return _finish()

    def _short_conviction_decay_reason(self, tkr: str, pos: dict, price: float, elapsed: int,
                                       signed_pct: float, mfe_pct: float) -> Optional[str]:
        if not SHORT_CONVICTION_DECAY_ENABLED:
            return None
        if pos.get('side') != 'SHORT':
            return None
        if elapsed < SHORT_CONVICTION_MIN_AGE_SEC:
            return None
        # If the short is meaningfully green, let profit-protection manage it.
        if signed_pct > SHORT_CONVICTION_MAX_GREEN_PCT:
            return None
        # Avoid tiny-noise exits unless the trade also failed to produce MFE.
        if signed_pct > SHORT_CONVICTION_MIN_ADVERSE_PCT and mfe_pct >= SHORT_CONVICTION_LOW_MFE_PCT:
            return None

        tkr = pos.get('ticker') or tkr
        stock = self._state_snapshot(tkr)
        ind = stock.get('indicators') or {}
        btc = self._state_snapshot('BTC/USD').get('indicators') or {}
        score = 0
        tags = []

        stack = ind.get('ema_stack')
        if stack in ('mixed', 'bull'):
            score += 1
            tags.append(f'stack={stack}')

        mom5 = self._ind_value(ind, 'mom_5s')
        mom15 = self._ind_value(ind, 'mom_15s')
        mom60 = self._ind_value(ind, 'mom_60s')
        if mom5 is not None and mom5 > 0.05:
            score += 1
            tags.append(f'm5={mom5:+.3f}')
        if mom15 is not None and mom15 > 0.03:
            score += 1
            tags.append(f'm15={mom15:+.3f}')
        if mom60 is not None and mom60 > 0:
            score += 1
            tags.append(f'm60={mom60:+.3f}')

        f30 = ind.get('flow_30s') or {}
        flow_buy = f30.get('buy_pct')
        flow_delta = ind.get('flow_30s_delta')
        if (flow_buy is not None and flow_buy >= SHORT_CONVICTION_FLOW_BUY_PCT
                and (flow_delta is None or flow_delta >= SHORT_CONVICTION_FLOW_DELTA_PCT)):
            score += 1
            tags.append(f'flow30={flow_buy:.1f}_d={flow_delta}')

        b15 = self._ind_value(btc, 'mom_15s')
        b60 = self._ind_value(btc, 'mom_60s')
        btc_stack = btc.get('ema_stack') or btc.get('stack')
        if b15 is not None and b15 >= SHORT_CONVICTION_BTC_MOM15_PCT:
            score += 1
            tags.append(f'btc15={b15:+.3f}')
        if b60 is not None and b60 >= SHORT_CONVICTION_BTC_MOM60_PCT:
            score += 1
            tags.append(f'btc60={b60:+.3f}')
        if btc_stack == 'bull':
            score += 1
            tags.append('btc_stack=bull')

        ema15 = ind.get('ema_15s')
        ema60 = ind.get('ema_60s')
        if ema15 is not None and price > float(ema15):
            score += 1
            tags.append('px>ema15')
        if ema60 is not None and price > float(ema60):
            score += 1
            tags.append('px>ema60')

        qimb = ind.get('quote_imbalance')
        if qimb is not None and qimb > 0.20:
            score += 1
            tags.append(f'qimb={qimb:+.3f}')

        if mfe_pct < SHORT_CONVICTION_LOW_MFE_PCT and signed_pct <= 0:
            score += 1
            tags.append(f'low_mfe={mfe_pct:.3f}')

        if score >= SHORT_CONVICTION_MIN_SCORE:
            pos['exit_decision_context'] = {
                'kind': 'short_conviction_decay',
                'score': score,
                'tags': tags,
                'signed_pct': round(signed_pct, 4),
                'mfe_pct': round(mfe_pct, 4),
                'elapsed_sec': elapsed,
                'price': round(float(price), 4),
                'snapshot': {
                    'stock': {
                        'ema_stack': ind.get('ema_stack'),
                        'mom_5s': mom5,
                        'mom_15s': mom15,
                        'mom_60s': mom60,
                        'flow_30s': ind.get('flow_30s'),
                        'flow_30s_delta': flow_delta,
                        'ema_15s': ema15,
                        'ema_60s': ema60,
                        'quote_imbalance': qimb,
                    },
                    'btc': {
                        'ema_stack': btc_stack,
                        'mom_15s': b15,
                        'mom_60s': b60,
                    },
                },
                'thresholds': {
                    'min_age_sec': SHORT_CONVICTION_MIN_AGE_SEC,
                    'min_score': SHORT_CONVICTION_MIN_SCORE,
                    'max_green_pct': SHORT_CONVICTION_MAX_GREEN_PCT,
                    'min_adverse_pct': SHORT_CONVICTION_MIN_ADVERSE_PCT,
                    'low_mfe_pct': SHORT_CONVICTION_LOW_MFE_PCT,
                },
            }
            return (
                f'short_conviction_decay_{elapsed}s_score={score}_'
                f'pnl={signed_pct:.3f}_mfe={mfe_pct:.3f}_'
                f'{";".join(tags[:5])}'
            )
        return None

    def _long_conviction_decay_reason(self, tkr: str, pos: dict, price: float, elapsed: int,
                                      signed_pct: float, mfe_pct: float) -> Optional[str]:
        if not LONG_CONVICTION_DECAY_ENABLED:
            return None
        if pos.get('side') != 'LONG':
            return None
        if elapsed < LONG_CONVICTION_MIN_AGE_SEC:
            return None
        # Longs were profitable today, so only decay-exit when the trade has
        # not paid meaningfully and is red or barely green.
        if signed_pct > LONG_CONVICTION_MAX_GREEN_PCT:
            return None
        if signed_pct > LONG_CONVICTION_MIN_ADVERSE_PCT and mfe_pct >= LONG_CONVICTION_LOW_MFE_PCT:
            return None

        tkr = pos.get('ticker') or tkr
        stock = self._state_snapshot(tkr)
        ind = stock.get('indicators') or {}
        btc = self._state_snapshot('BTC/USD').get('indicators') or {}
        score = 0
        tags = []

        stack = ind.get('ema_stack')
        if stack in ('mixed', 'bear'):
            score += 1
            tags.append(f'stack={stack}')

        mom5 = self._ind_value(ind, 'mom_5s')
        mom15 = self._ind_value(ind, 'mom_15s')
        mom60 = self._ind_value(ind, 'mom_60s')
        if mom5 is not None and mom5 < -0.05:
            score += 1
            tags.append(f'm5={mom5:+.3f}')
        if mom15 is not None and mom15 < -0.03:
            score += 1
            tags.append(f'm15={mom15:+.3f}')
        if mom60 is not None and mom60 < 0:
            score += 1
            tags.append(f'm60={mom60:+.3f}')

        f30 = ind.get('flow_30s') or {}
        flow_buy = f30.get('buy_pct')
        flow_delta = ind.get('flow_30s_delta')
        if (flow_buy is not None and flow_buy <= LONG_CONVICTION_FLOW_BUY_PCT
                and (flow_delta is None or flow_delta <= LONG_CONVICTION_FLOW_DELTA_PCT)):
            score += 1
            tags.append(f'flow30={flow_buy:.1f}_d={flow_delta}')

        b15 = self._ind_value(btc, 'mom_15s')
        b60 = self._ind_value(btc, 'mom_60s')
        btc_stack = btc.get('ema_stack') or btc.get('stack')
        if b15 is not None and b15 <= LONG_CONVICTION_BTC_MOM15_PCT:
            score += 1
            tags.append(f'btc15={b15:+.3f}')
        if b60 is not None and b60 <= LONG_CONVICTION_BTC_MOM60_PCT:
            score += 1
            tags.append(f'btc60={b60:+.3f}')
        if btc_stack == 'bear':
            score += 1
            tags.append('btc_stack=bear')

        ema15 = ind.get('ema_15s')
        ema60 = ind.get('ema_60s')
        if ema15 is not None and price < float(ema15):
            score += 1
            tags.append('px<ema15')
        if ema60 is not None and price < float(ema60):
            score += 1
            tags.append('px<ema60')

        qimb = ind.get('quote_imbalance')
        if qimb is not None and qimb < -0.20:
            score += 1
            tags.append(f'qimb={qimb:+.3f}')

        if mfe_pct < LONG_CONVICTION_LOW_MFE_PCT and signed_pct <= 0:
            score += 1
            tags.append(f'low_mfe={mfe_pct:.3f}')

        if score >= LONG_CONVICTION_MIN_SCORE:
            pos['exit_decision_context'] = {
                'kind': 'long_conviction_decay',
                'score': score,
                'tags': tags,
                'signed_pct': round(signed_pct, 4),
                'mfe_pct': round(mfe_pct, 4),
                'elapsed_sec': elapsed,
                'price': round(float(price), 4),
                'snapshot': {
                    'stock': {
                        'ema_stack': ind.get('ema_stack'),
                        'mom_5s': mom5,
                        'mom_15s': mom15,
                        'mom_60s': mom60,
                        'flow_30s': ind.get('flow_30s'),
                        'flow_30s_delta': flow_delta,
                        'ema_15s': ema15,
                        'ema_60s': ema60,
                        'quote_imbalance': qimb,
                    },
                    'btc': {
                        'ema_stack': btc_stack,
                        'mom_15s': b15,
                        'mom_60s': b60,
                    },
                },
                'thresholds': {
                    'min_age_sec': LONG_CONVICTION_MIN_AGE_SEC,
                    'min_score': LONG_CONVICTION_MIN_SCORE,
                    'max_green_pct': LONG_CONVICTION_MAX_GREEN_PCT,
                    'min_adverse_pct': LONG_CONVICTION_MIN_ADVERSE_PCT,
                    'low_mfe_pct': LONG_CONVICTION_LOW_MFE_PCT,
                },
            }
            return (
                f'long_conviction_decay_{elapsed}s_score={score}_'
                f'pnl={signed_pct:.3f}_mfe={mfe_pct:.3f}_'
                f'{";".join(tags[:5])}'
            )
        return None

    def _smart_exit_position(self, tkr: str, price: float, reason: str):
        with self.lock:
            pos = self.state['positions'].get(tkr)
            if not pos or pos.get('smart_exit_triggered'):
                return
            pos['smart_exit_triggered'] = reason
            self._save()
        self._start_exit_tick_capture(tkr, pos, price, reason)
        if self.trader is not None:
            try:
                remaining = self._confirmed_broker_close(
                    [tkr], reason,
                    parent_order_ids={tkr: pos.get('alpaca_order_id')},
                )
            except Exception as e:
                log.error(f'{reason} {tkr}: broker close workflow failed: {e}')
                remaining = {tkr}
            if remaining:
                self._set_broker_exposure_block(
                    remaining,
                    f'{reason}_position_not_flat_after_close_attempt',
                )
                return
        self._close_position(tkr, price, reason)

    def _spread_quality_gate_reason(self, sig: dict, forensics: dict,
                                    entry_quality_tier: dict) -> Optional[str]:
        if not SPREAD_QUALITY_GATE_ENABLED:
            return None
        entry_quality = forensics.get('entry_quality') or {}
        spread_pct = entry_quality.get('spread_pct')
        if spread_pct is None:
            return None
        try:
            spread_pct_f = float(spread_pct)
        except Exception:
            return None
        signal_score = abs(float(sig.get('score') or 0))
        quality_score = float(entry_quality_tier.get('score') or 0)
        if SPREAD_GATE_HARD_PCT > 0 and spread_pct_f > SPREAD_GATE_HARD_PCT:
            return f'spread_hard_gate_{spread_pct_f:.3f}pct'
        if spread_pct_f >= SPREAD_GATE_ELEVATED_PCT:
            if quality_score < SPREAD_GATE_MIN_ENTRY_QUALITY_SCORE or signal_score < SPREAD_GATE_MIN_SIGNAL_SCORE:
                return (
                    f'spread_elevated_quality_gate_{spread_pct_f:.3f}pct_'
                    f'quality={quality_score:.0f}_signal={signal_score:.1f}'
                )
        return None

    def _pre_submit_revalidation(self, sig: dict, signal_price: float) -> Optional[str]:
        if not PRE_SUBMIT_CHECK_ENABLED:
            return None
        tkr = sig.get('ticker')
        side = sig.get('side')
        stock = self._state_snapshot(tkr)
        px = stock.get('price')
        if not px:
            return 'pre_submit_no_live_price'
        adverse = ((signal_price - px) / signal_price * 100) if side == 'LONG' \
            else ((px - signal_price) / signal_price * 100)
        if adverse > PRE_SUBMIT_MAX_ADVERSE_MOVE_PCT:
            return f'pre_submit_adverse_move_{adverse:.3f}pct'
        spread_pct = stock.get('spread_pct')
        if spread_pct is not None and spread_pct > PRE_SUBMIT_MAX_SPREAD_PCT:
            return f'pre_submit_spread_widened_{spread_pct:.3f}pct'
        quote_age = stock.get('quote_age_sec')
        if quote_age is None or quote_age > PRE_SUBMIT_MAX_QUOTE_AGE_SEC:
            return f'pre_submit_quote_stale_{quote_age}'
        btc = self._state_snapshot('BTC/USD').get('indicators') or {}
        btc_age = btc.get('last_trade_age_sec')
        btc_quote_age = btc.get('last_quote_age_sec')
        btc_max_stale = float(SMART_ENTRY_CFG.get('btc_max_stale_sec', 20))
        btc_quote_max_stale = float(SMART_ENTRY_CFG.get('btc_quote_max_stale_sec', 10))
        if btc_age is not None and btc_age > btc_max_stale:
            if btc_quote_age is None or btc_quote_age > btc_quote_max_stale:
                return f'pre_submit_btc_stale_trade={btc_age}_quote={btc_quote_age}'
        b15 = btc.get('mom_15s')
        b60 = btc.get('mom_60s')
        if side == 'LONG' and (
            (b15 is not None and b15 <= -BTC_IMPULSE_ABORT_15S_PCT)
            or (b60 is not None and b60 <= -BTC_IMPULSE_ABORT_60S_PCT)
        ):
            return f'pre_submit_btc_flip_long_b15={b15}_b60={b60}'
        if side == 'SHORT' and (
            (b15 is not None and b15 >= BTC_IMPULSE_ABORT_15S_PCT)
            or (b60 is not None and b60 >= BTC_IMPULSE_ABORT_60S_PCT)
        ):
            return f'pre_submit_btc_flip_short_b15={b15}_b60={b60}'
        sig.setdefault('pre_submit', {}).update({
            'checked_at': int(time.time()),
            'price': px,
            'adverse_move_pct': round(adverse, 4),
            'spread_pct': spread_pct,
            'quote_age_sec': quote_age,
            'btc_mom_15s': b15,
            'btc_mom_60s': b60,
        })
        return None

    def _btc_impulse_exit_reason(self, pos: dict) -> Optional[str]:
        if not BTC_IMPULSE_ABORT_ENABLED or self.trader is None:
            return None
        if not pos.get('alpaca_filled_entry'):
            return None
        side = pos.get('side')
        btc = self._state_snapshot('BTC/USD').get('indicators') or {}
        if not btc.get('ready'):
            return None
        b15 = btc.get('mom_15s')
        b60 = btc.get('mom_60s')
        if side == 'LONG' and (
            (b15 is not None and b15 <= -BTC_IMPULSE_ABORT_15S_PCT)
            or (b60 is not None and b60 <= -BTC_IMPULSE_ABORT_60S_PCT)
        ):
            return f'btc_impulse_abort_long_b15={b15}_b60={b60}'
        if side == 'SHORT' and (
            (b15 is not None and b15 >= BTC_IMPULSE_ABORT_15S_PCT)
            or (b60 is not None and b60 >= BTC_IMPULSE_ABORT_60S_PCT)
        ):
            return f'btc_impulse_abort_short_b15={b15}_b60={b60}'
        return None

    def _on_signal(self, sig: dict):
        try:
            if not self.state.get('running'):
                return
            today_iso = datetime.now(CT).date().isoformat()
            if self.state.get('state_market_day') != today_iso:
                rollover = self.rollover_market_day_state(today_iso, reason='signal_day_boundary')
                if not rollover.get('ok'):
                    self._record_skipped_signal(sig, 'state_rollover_failed')
                    return
            if self._kill_switch_active():
                self._record_skipped_signal(sig, 'kill_switch')
                return
            if self.state.get('broker_api_degraded'):
                self._record_skipped_signal(sig, 'broker_api_degraded')
                return
            if not self._in_entry_window():
                return
            tkr = sig.get('ticker')
            if tkr not in TICKERS:
                return
            if not STEP_PARITY_LIVE_MODE and sig.get('conviction') not in MIN_CONVICTION:
                self._record_skipped_signal(sig, 'below_min_conviction')
                return
            price = sig.get('price')
            if not price or price <= 0:
                return

            signal_seen_ms = int(time.time() * 1000)
            latency_chain = {'signal_seen_ms': signal_seen_ms}

            cfg = TICKER_CFG.get(tkr, {'sl': SL_PCT, 'tp': TP_PCT, 'btc_mode': 'baseline'})
            sl_pct = cfg['sl']
            tp_pct = cfg['tp']
            per_ticker = self._get_per_ticker_budget()
            bs = sig.get('btc_stack')
            bm = sig.get('btc_mom')
            side = sig['side']
            self._audit_event('signal_seen', tkr, {
                'side': side,
                'price': round(float(price), 4),
                'score': sig.get('score'),
                'conviction': sig.get('conviction'),
            })

            signal_quality = sig.get('signal_quality') if isinstance(sig.get('signal_quality'), dict) else {}
            btc_conflict = signal_quality.get('btc_conflict')
            if btc_conflict is None:
                btc_conflict = False
            if bm is not None and not signal_quality:
                if side == 'LONG':
                    btc_conflict = (bs == 'bear' and bm < 0) or bm <= -0.10
                else:
                    btc_conflict = (bs == 'bull' and bm > 0) or bm >= 0.10
            if btc_conflict and not STEP_PARITY_LIVE_MODE:
                log.info(f'{tkr} {side} skip: BTC conflict (stack={bs} mom={bm})')
                self._record_skipped_signal(sig, 'btc_conflict')
                return

            if cfg['btc_mode'] == 'agree' and not STEP_PARITY_LIVE_MODE:
                if side == 'LONG':
                    if not (bs == 'bull' and bm is not None and bm > 0):
                        log.info(f'{tkr} {side} skip: BTC not bullish (stack={bs} mom={bm})')
                        self._record_skipped_signal(sig, 'btc_not_bullish')
                        return
                else:
                    if not (bs == 'bear' and bm is not None and bm < 0):
                        log.info(f'{tkr} {side} skip: BTC not bearish (stack={bs} mom={bm})')
                        self._record_skipped_signal(sig, 'btc_not_bearish')
                        return

            if not STEP_PARITY_LIVE_MODE:
                risk_off = self._risk_off_reason()
                if risk_off:
                    self._record_skipped_signal(sig, f'risk_off:{risk_off}')
                    return
                live_gate = self._live_quality_gate_reason()
                if live_gate:
                    self._record_skipped_signal(sig, f'live_quality_gate:{live_gate}')
                    return
                pause = self._setup_pause_active(sig)
                if pause:
                    self._record_skipped_signal(sig, f"setup_paused:{pause.get('reason', 'recent_losses')}")
                    return
                market_conflict = self._market_regime_conflict(side)
                if MARKET_REGIME_FILTER_ENABLED and not self._market_tape:
                    self._refresh_market_tape()
                    market_conflict = self._market_regime_conflict(side)
                if market_conflict:
                    self._record_skipped_signal(sig, f'market_regime_conflict:{market_conflict}')
                    return

            bracket = self._adaptive_brackets(tkr, side, float(price), sig, sl_pct, tp_pct)
            sl_pct = bracket['sl_pct']
            tp_pct = bracket['tp_pct']

            bracket_prices = live_step2_decision_kernel.bracket_prices(side, float(price), sl_pct, tp_pct)
            sl = sl_send = bracket_prices['sl']
            tp = tp_send = bracket_prices['tp']
            entry_ts = int(signal_seen_ms / 1000)
            client_order_id = f'scalp-{tkr}-{entry_ts}-{int(time.time() * 1000) % 1000000}'
            trade_id = f'{tkr}-{entry_ts}-{client_order_id}'
            fill_attribution_events = [
                self._record_fill_attribution(
                    tkr, side, 'signal',
                    price_ref=float(price), qty=None,
                    trade_id=trade_id,
                    client_order_id=client_order_id if self.trader is not None else None,
                )
            ]
            forensics = self._forensics(sig, price)
            forensics['fill_attribution'] = fill_attribution_events
            try:
                market_tape_at_signal = json.loads(json.dumps(self._market_tape or {}))
            except Exception:
                market_tape_at_signal = {}
            entry_quality_tier = self._entry_quality_tier(sig, forensics)
            forensics['entry_quality_tier'] = entry_quality_tier
            near_miss_rules = self._entry_rule_near_misses(sig, forensics, entry_quality_tier)
            forensics['entry_rule_near_misses'] = near_miss_rules
            micro = forensics.get('market_microstructure') or {}
            if (not STEP_PARITY_LIVE_MODE
                    and (micro.get('halt_suspected') or micro.get('luld_like_risk'))):
                self._record_skipped_signal(
                    sig,
                    'market_microstructure_risk:'
                    + ('halt_suspected' if micro.get('halt_suspected') else 'luld_like_risk'),
                )
                return
            entry_quality = forensics.get('entry_quality') or {}
            spread_pct = entry_quality.get('spread_pct')
            if (not STEP_PARITY_LIVE_MODE
                    and MAX_ENTRY_SPREAD_PCT > 0
                    and spread_pct is not None
                    and spread_pct > MAX_ENTRY_SPREAD_PCT):
                self._record_skipped_signal(sig, 'spread_too_wide')
                return
            spread_gate = self._spread_quality_gate_reason(sig, forensics, entry_quality_tier)
            if spread_gate and not STEP_PARITY_LIVE_MODE:
                self._append_entry_retry_candidate(sig, spread_gate, float(price))
                self._record_skipped_signal(sig, spread_gate)
                return
            if entry_quality.get('quote_stale') and not STEP_PARITY_LIVE_MODE:
                self._record_skipped_signal(sig, 'quote_stale')
                return

            with self.lock:
                pending = self.state.setdefault('pending_entries', {})
                entry_decision = live_step2_decision_kernel.entry_decision(
                    self.state,
                    tkr,
                    entry_ts,
                    STEP2_EXECUTION_CONTRACT,
                    STEP2_SIGNAL_SCAN_LIVE_MODE,
                    CT,
                    signal=sig,
                    entry_price=float(price),
                    sl_pct=sl_pct,
                    tp_pct=tp_pct,
                    brackets=bracket_prices,
                    trade_id=trade_id,
                    client_order_id=client_order_id if self.trader is not None else None,
                    broker_action='submit_bracket_order' if self.trader is not None else 'commit_sim_position',
                    metadata={
                        'execution_mode': EXECUTION_MODE,
                        'strategy_config_hash': STRATEGY_CONFIG_HASH,
                        'execution_kernel_hash': EXECUTION_KERNEL_HASH,
                        'step2_parity_contract_hash': STEP2_PARITY_CONTRACT_HASH,
                        'step2_execution_contract_hash': STEP2_EXECUTION_CONTRACT_HASH,
                    },
                )
                block_reason = entry_decision.get('reason')
                forensics['step2_entry_decision'] = entry_decision
                if block_reason:
                    self._record_skipped_signal(sig, block_reason)
                    return
                if per_ticker < MIN_BALANCE:
                    log.info(f'per-ticker budget ${per_ticker:.2f} < ${MIN_BALANCE} - skip')
                    self._record_skipped_signal(sig, 'insufficient_per_ticker_budget')
                    return
                trade_size_pct_now = self._trade_size_pct_now(sig)
                alloc = round(per_ticker * trade_size_pct_now, 2)
                if self.trader is not None:
                    acct = self._get_account_cached()
                    buying_power = float(acct.get('buying_power', 0) or 0)
                    if buying_power and buying_power < alloc:
                        log.info(f'buying power ${buying_power:.2f} < alloc ${alloc:.2f} - skip')
                        self._record_skipped_signal(sig, 'insufficient_buying_power')
                        return
                    qty = alloc_to_qty(alloc, price)
                    if qty < 1:
                        log.info(f'alloc ${alloc:.2f} @ ${price:.2f} < 1 share - skip')
                        self._record_skipped_signal(sig, 'alloc_below_one_share')
                        return
                else:
                    qty = alloc / price
                    if qty <= 0:
                        self._record_skipped_signal(sig, 'non_positive_qty')
                        return
                entry_action_plan = (
                    forensics.get('step2_entry_decision') or {}
                ).get('action_plan') or {}
                if entry_action_plan:
                    entry_action_plan.setdefault('execution', {}).update({
                        'qty': qty if self.trader is None else int(qty),
                        'alloc': alloc,
                        'trade_id': trade_id,
                        'client_order_id': client_order_id if self.trader is not None else None,
                    })
                    try:
                        execution_action_engine.rehash_plan(entry_action_plan)
                    except Exception:
                        pass
                side_alpaca = 'buy' if side == 'LONG' else 'sell'
                entry_execution_intent = execution_intent_engine.entry_intent(
                    entry_action_plan,
                    venue='alpaca' if self.trader is not None else 'live_internal_sim',
                    qty=qty if self.trader is None else int(qty),
                    trade_id=trade_id,
                    client_order_id=client_order_id if self.trader is not None else None,
                    tp_price=tp_send,
                    sl_price=sl_send,
                    entry_price=round(float(price), 4),
                    order_kind='bracket_entry' if self.trader is not None else 'simulated_entry',
                    timeout_sec=ENTRY_FILL_TIMEOUT_SEC,
                    metadata={
                        'ticker': tkr,
                        'side': side,
                        'setup_type': sig.get('setup_type') or self._setup_type(sig),
                        'timestamp_second': entry_ts,
                        'opportunity_id': sig.get('opportunity_id'),
                        'trade_id': trade_id,
                        'client_order_id': client_order_id if self.trader is not None else None,
                        'execution_mode': EXECUTION_MODE,
                        'strategy_config_hash': STRATEGY_CONFIG_HASH,
                        'execution_kernel_hash': EXECUTION_KERNEL_HASH,
                        'step2_parity_contract_hash': STEP2_PARITY_CONTRACT_HASH,
                        'step2_execution_contract_hash': STEP2_EXECUTION_CONTRACT_HASH,
                    },
                    ts=entry_ts,
                )
                entry_execution_result = None
                forensics['entry_execution_intent'] = entry_execution_intent
                forensics['liquidity_impact'] = self._liquidity_impact(sig, price, side, qty)
                forensics['estimated_round_trip_costs_at_entry'] = self._estimated_transaction_costs(
                    side, qty, float(price), tp if side == 'LONG' else tp
                )
                concurrent_tickers = list(self.state['positions'].keys())
                pending[tkr] = {
                    'side': side,
                    'price': round(price, 4),
                    'created_at': entry_ts,
                    'client_order_id': client_order_id if self.trader is not None else None,
                    'trade_id': trade_id,
                    'latency_chain': dict(latency_chain),
                    'execution_intent': entry_execution_intent,
                    'execution_intent_id': entry_execution_intent.get('execution_intent_id'),
                }
                if self.trader is not None:
                    pending[tkr]['buying_power_at_signal'] = buying_power
                self._critical_save('entry_pending_reserved', [tkr])
                latency_chain['reserved_ms'] = int(time.time() * 1000)
            self._audit_event('entry_reserved', tkr, {
                'trade_id': trade_id,
                'client_order_id': client_order_id if self.trader is not None else None,
                'side': side,
                'price': round(float(price), 4),
                'qty': qty,
                'alloc': alloc,
                'execution_intent_id': entry_execution_intent.get('execution_intent_id'),
                'semantic_execution_intent_hash': entry_execution_intent.get('semantic_execution_intent_hash'),
            })
            self._audit_event('entry_intent_created', tkr, {
                'trade_id': trade_id,
                'client_order_id': client_order_id if self.trader is not None else None,
                'execution_intent_id': entry_execution_intent.get('execution_intent_id'),
                'semantic_execution_intent_hash': entry_execution_intent.get('semantic_execution_intent_hash'),
                'execution_intent': entry_execution_intent,
            })

            alpaca_order_id = None
            alpaca_status = None
            pre_action_packet_id = None
            if canonical_decision_packet is not None:
                try:
                    profile = self._active_profile_snapshot()
                    pre_features = self._signal_feature_snapshot(sig, forensics)
                    pre_packet = canonical_decision_packet.from_live_pre_action(sig, {
                        'created_at': entry_ts,
                        'parity_key': self._signal_parity_key(sig, entry_ts),
                        'trade_id': trade_id,
                        'client_order_id': client_order_id if self.trader is not None else None,
                        'decision': 'prepared',
                        'reason': 'entry_reserved_pre_action',
                        'entry_price': round(float(price), 4),
                        'qty': qty if self.trader is None else int(qty),
                        'alloc': alloc,
                        'sl': sl,
                        'tp': tp,
                        'sl_send': sl_send,
                        'tp_send': tp_send,
                        'bracket_policy': bracket,
                        'broker_action': 'submit_bracket_order' if self.trader is not None else 'commit_sim_position',
                        'execution_mode': EXECUTION_MODE,
                        'strategy_config_hash': STRATEGY_CONFIG_HASH,
                        'execution_kernel_hash': EXECUTION_KERNEL_HASH,
                        'step2_parity_contract_hash': STEP2_PARITY_CONTRACT_HASH,
                        'step2_execution_contract_hash': STEP2_EXECUTION_CONTRACT_HASH,
                        'profile': {
                            'name': profile.get('name'),
                            'hash': profile.get('hash'),
                            'bias': profile.get('bias'),
                            'weights': profile.get('weights'),
                        },
                        'feature_snapshot_hash': pre_features.get('hash'),
                        'feature_snapshot': pre_features.get('payload') or {},
                        'market_freshness': self._market_freshness_snapshot(tkr),
                        'latency_chain': dict(latency_chain),
                        'action_plan': entry_action_plan,
                        'execution_intent': entry_execution_intent,
                        'operational_state': {
                            'running': bool(self.state.get('running')),
                            'kill_switch': self.state.get('kill_switch') or {},
                            'broker_api_degraded': self.state.get('broker_api_degraded'),
                            'positions': sorted((self.state.get('positions') or {}).keys()),
                            'pending_entries': sorted((self.state.get('pending_entries') or {}).keys()),
                            'in_entry_window': self._in_entry_window(),
                            'in_trading_window': self._in_trading_window(),
                        },
                    })
                    canonical_decision_packet.append_packet(pre_packet)
                    pre_action_packet_id = pre_packet.get('packet_id')
                except Exception as packet_err:
                    log.warning(f'canonical pre-action packet write failed: {packet_err}')
            try:
                if self.trader is not None:
                    latency_chain['pre_submit_start_ms'] = int(time.time() * 1000)
                    pre_submit_block = self._pre_submit_revalidation(sig, float(price))
                    latency_chain['pre_submit_end_ms'] = int(time.time() * 1000)
                    if pre_submit_block and STEP_PARITY_LIVE_MODE:
                        self._audit_event('entry_pre_submit_bypassed_step_parity', tkr, {
                            'reason': pre_submit_block,
                            'side': side,
                            'signal_price': round(float(price), 4),
                            'mode': EXECUTION_MODE,
                        })
                        pre_submit_block = None
                    if pre_submit_block:
                        if pre_submit_block.startswith((
                                'pre_submit_quote_stale',
                                'pre_submit_spread_widened',
                                'pre_submit_no_live_price',
                        )):
                            self._append_entry_retry_candidate(sig, pre_submit_block, float(price))
                        self._record_skipped_signal(sig, pre_submit_block)
                        self._audit_event('entry_pre_submit_blocked', tkr, {
                            'reason': pre_submit_block,
                            'side': side,
                            'signal_price': round(float(price), 4),
                            'retry_candidate_logged': pre_submit_block.startswith((
                                'pre_submit_quote_stale',
                                'pre_submit_spread_widened',
                                'pre_submit_no_live_price',
                            )),
                            'execution_intent_id': entry_execution_intent.get('execution_intent_id'),
                        })
                        entry_execution_result = execution_intent_engine.result_from_intent(
                            entry_execution_intent,
                            'rejected',
                            ok=False,
                            reason=pre_submit_block,
                            error={'kind': 'pre_submit_block'},
                            latency=dict(latency_chain),
                        )
                        self._audit_event('entry_execution_result', tkr, {
                            'trade_id': trade_id,
                            'execution_intent_id': entry_execution_intent.get('execution_intent_id'),
                            'execution_result_id': entry_execution_result.get('execution_result_id'),
                            'execution_result': entry_execution_result,
                        })
                        return
                    if sig.get('pre_submit'):
                        forensics['pre_submit'] = sig.get('pre_submit')
                        fill_attribution_events.append(
                            self._record_fill_attribution(
                                tkr, side, 'pre_submit',
                                price_ref=float(price), qty=qty,
                                trade_id=trade_id,
                                client_order_id=client_order_id,
                            )
                        )
                    if side_alpaca == 'sell':
                        asset = self._asset_info_snapshot(tkr, force=True, critical=True)
                        forensics['short_borrow_snapshot'] = asset
                        if not asset.get('available'):
                            log.info(f'{tkr} shortable check unavailable - skip SHORT')
                            self._record_skipped_signal(sig, 'shortable_check_failed')
                            entry_execution_result = execution_intent_engine.result_from_intent(
                                entry_execution_intent,
                                'rejected',
                                ok=False,
                                reason='shortable_check_failed',
                                error={'kind': 'shortable_check_failed'},
                                latency=dict(latency_chain),
                            )
                            self._audit_event('entry_execution_result', tkr, {
                                'trade_id': trade_id,
                                'execution_intent_id': entry_execution_intent.get('execution_intent_id'),
                                'execution_result_id': entry_execution_result.get('execution_result_id'),
                                'execution_result': entry_execution_result,
                            })
                            return
                        if not asset.get('shortable'):
                            log.info(f'{tkr} not shortable - skip SHORT')
                            self._record_skipped_signal(sig, 'not_shortable')
                            entry_execution_result = execution_intent_engine.result_from_intent(
                                entry_execution_intent,
                                'rejected',
                                ok=False,
                                reason='not_shortable',
                                error={'kind': 'not_shortable'},
                                latency=dict(latency_chain),
                            )
                            self._audit_event('entry_execution_result', tkr, {
                                'trade_id': trade_id,
                                'execution_intent_id': entry_execution_intent.get('execution_intent_id'),
                                'execution_result_id': entry_execution_result.get('execution_result_id'),
                                'execution_result': entry_execution_result,
                            })
                            return
                    try:
                        latency_chain['broker_submit_start_ms'] = int(time.time() * 1000)
                        order = self.trader.submit_bracket_order(
                            symbol=tkr, qty=qty, side=side_alpaca,
                            tp_price=tp_send, sl_price=sl_send,
                            client_order_id=client_order_id,
                        )
                        latency_chain['broker_submit_end_ms'] = int(time.time() * 1000)
                        alpaca_order_id = order.get('id')
                        alpaca_status = order.get('status')
                        with self.lock:
                            pending = self.state.setdefault('pending_entries', {})
                            if tkr in pending:
                                pending[tkr].update({
                                    'alpaca_order_id': alpaca_order_id,
                                    'alpaca_status': alpaca_status,
                                    'client_order_id': client_order_id,
                                    'broker_submitted_at': int(time.time()),
                                    'latency_chain': dict(latency_chain),
                                    'trade_size_pct': trade_size_pct_now,
                                })
                                self._critical_save('broker_order_pending_recorded', [tkr])
                        log.info(f'ALPACA submit ok: {tkr} {side_alpaca} {qty} '
                                 f'id={alpaca_order_id} status={alpaca_status}')
                        fill_attribution_events.append(
                            self._record_fill_attribution(
                                tkr, side, 'submit_response',
                                price_ref=float(price), qty=qty,
                                trade_id=trade_id,
                                client_order_id=client_order_id,
                                broker_order_id=alpaca_order_id,
                            )
                        )
                        self._audit_event('entry_submitted', tkr, {
                            'trade_id': trade_id,
                            'side': side_alpaca,
                            'qty': qty,
                            'order_id': alpaca_order_id,
                            'client_order_id': client_order_id,
                            'status': alpaca_status,
                            'tp': tp_send,
                            'sl': sl_send,
                        })
                        entry_execution_result = execution_intent_engine.result_from_intent(
                            entry_execution_intent,
                            'submitted',
                            ok=True,
                            broker_order_id=alpaca_order_id,
                            broker_status=alpaca_status,
                            raw_response=order,
                            latency=dict(latency_chain),
                        )
                        self._audit_event('entry_execution_result', tkr, {
                            'trade_id': trade_id,
                            'order_id': alpaca_order_id,
                            'execution_intent_id': entry_execution_intent.get('execution_intent_id'),
                            'execution_result_id': entry_execution_result.get('execution_result_id'),
                            'execution_result': entry_execution_result,
                        })
                    except AlpacaTradingError as e:
                        latency_chain.setdefault('broker_submit_end_ms', int(time.time() * 1000))
                        order = None
                        try:
                            order = self._lookup_broker_order_by_client_id(client_order_id)
                        except Exception as lookup_err:
                            log.error(f'ALPACA submit failed and client-id lookup failed '
                                      f'{tkr} {client_order_id}: {lookup_err}')
                        if order:
                            latency_chain['broker_recovered_ms'] = int(time.time() * 1000)
                            alpaca_order_id = order.get('id')
                            alpaca_status = order.get('status')
                            log.warning(f'ALPACA submit error recovered by client_order_id '
                                        f'{client_order_id}: id={alpaca_order_id} status={alpaca_status}')
                            fill_attribution_events.append(
                                self._record_fill_attribution(
                                    tkr, side, 'submit_response_recovered',
                                    price_ref=float(price), qty=qty,
                                    trade_id=trade_id,
                                    client_order_id=client_order_id,
                                    broker_order_id=alpaca_order_id,
                                )
                            )
                        else:
                            log.error(f'ALPACA submit FAILED {tkr} {side_alpaca} {qty}: {e}')
                            self._record_skipped_signal(sig, 'alpaca_submit_failed')
                            entry_execution_result = execution_intent_engine.result_from_intent(
                                entry_execution_intent,
                                'failed',
                                ok=False,
                                reason='alpaca_submit_failed',
                                error={'kind': 'alpaca_submit_failed', 'message': str(e)},
                                latency=dict(latency_chain),
                            )
                            self._audit_event('entry_execution_result', tkr, {
                                'trade_id': trade_id,
                                'execution_intent_id': entry_execution_intent.get('execution_intent_id'),
                                'execution_result_id': entry_execution_result.get('execution_result_id'),
                                'execution_result': entry_execution_result,
                            })
                            return
                    except Exception as e:
                        latency_chain.setdefault('broker_submit_end_ms', int(time.time() * 1000))
                        order = None
                        try:
                            order = self._lookup_broker_order_by_client_id(client_order_id)
                        except Exception as lookup_err:
                            log.error(f'ALPACA submit exception and client-id lookup failed '
                                      f'{tkr} {client_order_id}: {lookup_err}')
                        if order:
                            latency_chain['broker_recovered_ms'] = int(time.time() * 1000)
                            alpaca_order_id = order.get('id')
                            alpaca_status = order.get('status')
                            log.warning(f'ALPACA submit exception recovered by client_order_id '
                                        f'{client_order_id}: id={alpaca_order_id} status={alpaca_status}')
                            fill_attribution_events.append(
                                self._record_fill_attribution(
                                    tkr, side, 'submit_response_recovered',
                                    price_ref=float(price), qty=qty,
                                    trade_id=trade_id,
                                    client_order_id=client_order_id,
                                    broker_order_id=alpaca_order_id,
                                )
                            )
                        else:
                            log.exception(f'ALPACA submit exception: {e}')
                            self._record_skipped_signal(sig, 'alpaca_submit_exception')
                            entry_execution_result = execution_intent_engine.result_from_intent(
                                entry_execution_intent,
                                'failed',
                                ok=False,
                                reason='alpaca_submit_exception',
                                error={'kind': 'alpaca_submit_exception', 'message': str(e)},
                                latency=dict(latency_chain),
                            )
                            self._audit_event('entry_execution_result', tkr, {
                                'trade_id': trade_id,
                                'execution_intent_id': entry_execution_intent.get('execution_intent_id'),
                                'execution_result_id': entry_execution_result.get('execution_result_id'),
                                'execution_result': entry_execution_result,
                            })
                            return
                    if alpaca_order_id or alpaca_status:
                        with self.lock:
                            pending = self.state.setdefault('pending_entries', {})
                            if tkr in pending:
                                pending[tkr].update({
                                    'alpaca_order_id': alpaca_order_id,
                                    'alpaca_status': alpaca_status,
                                    'client_order_id': client_order_id,
                                    'broker_submitted_at': int(time.time()),
                                    'latency_chain': dict(latency_chain),
                                    'trade_size_pct': trade_size_pct_now,
                                })
                                self._critical_save('broker_order_pending_recorded', [tkr])
                        if entry_execution_result is None:
                            entry_execution_result = execution_intent_engine.result_from_intent(
                                entry_execution_intent,
                                'submitted_recovered',
                                ok=True,
                                broker_order_id=alpaca_order_id,
                                broker_status=alpaca_status,
                                raw_response=order,
                                latency=dict(latency_chain),
                            )
                            self._audit_event('entry_execution_result', tkr, {
                                'trade_id': trade_id,
                                'order_id': alpaca_order_id,
                                'execution_intent_id': entry_execution_intent.get('execution_intent_id'),
                                'execution_result_id': entry_execution_result.get('execution_result_id'),
                                'execution_result': entry_execution_result,
                            })
                if self.trader is None and entry_execution_result is None:
                    entry_execution_result = execution_intent_engine.simulated_entry_result(
                        entry_execution_intent,
                        ts=entry_ts,
                        fill_price=round(float(price), 4),
                    )
                    self._audit_event('entry_execution_result', tkr, {
                        'trade_id': trade_id,
                        'execution_intent_id': entry_execution_intent.get('execution_intent_id'),
                        'execution_result_id': entry_execution_result.get('execution_result_id'),
                        'execution_result': entry_execution_result,
                    })

                duplicate_commit = False
                latency_chain['committed_ms'] = int(time.time() * 1000)
                forensics['fill_attribution'] = fill_attribution_events
                forensics['entry_execution_result'] = entry_execution_result
                forensics['latency_attribution'] = self._latency_attribution_row(
                    tkr, side, trade_id,
                    client_order_id if self.trader is not None else None,
                    alpaca_order_id,
                    latency_chain,
                    'entry_committing',
                )
                decision_audit = {
                    'created_at': entry_ts,
                    'trade_id': trade_id,
                    'ticker': tkr,
                    'side': side,
                    'decision': 'entered',
                    'price': round(float(price), 4),
                    'score': sig.get('score'),
                    'conviction': sig.get('conviction'),
                    'setup_type': sig.get('setup_type') or self._setup_type(sig),
                    'reasons': sig.get('reasons', []),
                    'components': sig.get('components') or {},
                    'signal_quality': sig.get('signal_quality') or {},
                    'shadow_dual_side_score': sig.get('shadow_dual_side_score')
                                              or (sig.get('signal_quality') or {}).get('score_model', {}).get('dual_side_shadow'),
                    'entry_quality_tier': entry_quality_tier,
                    'setup_grade_at_entry': entry_quality_tier,
                    'execution_quality': sig.get('execution_quality') or {},
                    'btc_context': sig.get('btc_context') or {},
                    'btc_indicators': sig.get('btc_indicators') or {},
                    'miner_basket': sig.get('miner_basket') or {},
                    'lead_lag': sig.get('lead_lag') or {},
                    'relative_strength': sig.get('relative_strength') or {},
                    'indicators': sig.get('indicators') or {},
                    'market_tape': market_tape_at_signal,
                    'market_microstructure': forensics.get('market_microstructure') or {},
                    'gap_context': forensics.get('gap_context') or {},
                    'liquidity_impact': forensics.get('liquidity_impact') or {},
                    'short_borrow_snapshot': forensics.get('short_borrow_snapshot') or {},
                    'btc_proxy_basket': forensics.get('btc_proxy_basket') or {},
                    'catalyst_context': forensics.get('catalyst_context') or {},
                    'fill_attribution': fill_attribution_events,
                    'latency_attribution': forensics['latency_attribution'],
                    'client_order_id': client_order_id if self.trader is not None else None,
                    'canonical_pre_action_packet_id': pre_action_packet_id,
                    'entry_action_plan': entry_action_plan,
                    'entry_execution_intent_id': entry_execution_intent.get('execution_intent_id'),
                    'entry_execution_result_id': (
                        entry_execution_result.get('execution_result_id')
                        if isinstance(entry_execution_result, dict) else None
                    ),
                    'execution_intent': entry_execution_intent,
                    'execution_result': entry_execution_result,
                    'forensics': forensics,
                    'near_miss_rules': near_miss_rules,
                    'gates': {
                        'risk_off': False,
                        'live_quality_gate': False,
                        'setup_pause': False,
                        'market_regime_conflict': False,
                        'spread_too_wide': False,
                        'quote_stale': False,
                        'pre_submit_block': False,
                    },
                    'strategy_config_hash': STRATEGY_CONFIG_HASH,
                    'execution_kernel_hash': EXECUTION_KERNEL_HASH,
                    'step2_execution_contract_hash': STEP2_EXECUTION_CONTRACT_HASH,
                }
                parity_row = self._append_live_signal_parity(
                    sig,
                    decision='entered',
                    reason='entered',
                    created_at=entry_ts,
                    trade_id=trade_id,
                    client_order_id=client_order_id if self.trader is not None else None,
                    broker_order_id=alpaca_order_id,
                    forensics=forensics,
                    latency_chain=latency_chain,
                    extra={
                        'alloc': alloc,
                        'qty': qty,
                        'sl': sl,
                        'tp': tp,
                        'sl_send': sl_send,
                        'tp_send': tp_send,
                        'bracket_policy': bracket,
                        'entry_quality_tier': entry_quality_tier,
                        'near_miss_rules': near_miss_rules,
                        'market_tape': market_tape_at_signal,
                        'pre_action_packet_id': pre_action_packet_id,
                        'action_plan': entry_action_plan,
                        'execution_intent': entry_execution_intent,
                        'execution_result': entry_execution_result,
                    },
                )
                if parity_row:
                    decision_audit['live_signal_parity_key'] = parity_row.get('parity_key')
                    decision_audit['live_signal_parity_feature_hash'] = parity_row.get('feature_snapshot_hash')
                with self.lock:
                    self.state.setdefault('pending_entries', {}).pop(tkr, None)
                    if tkr in self.state['positions']:
                        log.warning(f'{tkr} position appeared while entry was pending; dropping duplicate commit')
                        self._critical_save('duplicate_entry_commit_cleanup', [tkr])
                        duplicate_commit = True
                    else:
                        self.state['positions'][tkr] = {
                            'ticker': tkr,
                            'trade_id': trade_id,
                            'opportunity_id': sig.get('opportunity_id'),
                            'side': side,
                            'entry': round(price, 4),
                            'sl': sl,
                            'tp': tp,
                            'bracket_policy': bracket,
                            'qty': qty if self.trader is None else int(qty),
                            'alloc': alloc,
                            'trade_size_pct': trade_size_pct_now,
                            'entry_ts': entry_ts,
                            'session_phase': sig.get('session_phase'),
                            'setup_type': sig.get('setup_type') or self._setup_type(sig),
                            'path_failure_entry_btc_price': self.engine.get_last_price('BTC/USD') or 0.0,
                            'reasons': sig.get('reasons', [])[:3],
                            'conviction': sig.get('conviction'),
                            'alpaca_order_id': alpaca_order_id,
                            'client_order_id': client_order_id if self.trader is not None else None,
                            'alpaca_status': alpaca_status,
                            'alpaca_filled_entry': False,
                            'entry_fill_price': None,
                            'entry_filled_ts': None,
                            'fill_attribution': fill_attribution_events,
                            'latency_chain': dict(latency_chain),
                            'latency_attribution': forensics['latency_attribution'],
                            'forensics': forensics,
                            'entry_thesis': {
                                'ticker': tkr,
                                'opportunity_id': sig.get('opportunity_id'),
                                'side': side,
                                'setup_type': sig.get('setup_type') or self._setup_type(sig),
                                'session_phase': sig.get('session_phase'),
                                'score': sig.get('score'),
                                'conviction': sig.get('conviction'),
                                'entry_quality_tier': entry_quality_tier.get('tier'),
                                'entry_quality_score': entry_quality_tier.get('score'),
                                'entry_quality_tags': entry_quality_tier.get('tags'),
                                'setup_grade_at_entry': entry_quality_tier,
                                'reasons': sig.get('reasons', []),
                                'signal_quality': sig.get('signal_quality') or {},
                                'shadow_dual_side_score': sig.get('shadow_dual_side_score')
                                                          or (sig.get('signal_quality') or {}).get('score_model', {}).get('dual_side_shadow'),
                                'btc_context': sig.get('btc_context') or {},
                                'miner_basket': sig.get('miner_basket') or {},
                                'lead_lag': sig.get('lead_lag') or {},
                                'relative_strength': sig.get('relative_strength') or {},
                                'market_tape': market_tape_at_signal,
                                'market_microstructure': forensics.get('market_microstructure') or {},
                                'gap_context': forensics.get('gap_context') or {},
                                'liquidity_impact': forensics.get('liquidity_impact') or {},
                                'short_borrow_snapshot': forensics.get('short_borrow_snapshot') or {},
                                'btc_proxy_basket': forensics.get('btc_proxy_basket') or {},
                                'catalyst_context': forensics.get('catalyst_context') or {},
                                'pre_submit': sig.get('pre_submit') or {},
                                'fill_attribution': fill_attribution_events,
                                'latency_attribution': forensics['latency_attribution'],
                                'client_order_id': client_order_id if self.trader is not None else None,
                                'entry_execution_intent': entry_execution_intent,
                                'entry_execution_result': entry_execution_result,
                                'decision_audit': decision_audit,
                                'near_miss_rules': near_miss_rules,
                            },
                            'decision_audit': decision_audit,
                            'entry_quality_tier': entry_quality_tier.get('tier'),
                            'entry_quality_score': entry_quality_tier.get('score'),
                            'setup_grade_at_entry': entry_quality_tier,
                            'entry_rule_near_misses': near_miss_rules,
                            'indicators': sig.get('indicators'),
                            'btc_indicators': sig.get('btc_indicators'),
                            'entry_score': sig.get('score'),
                            'entry_rvol': sig.get('rvol'),
                            'strategy_config_hash': STRATEGY_CONFIG_HASH,
                            'execution_kernel_hash': EXECUTION_KERNEL_HASH,
                            'step2_execution_contract_hash': STEP2_EXECUTION_CONTRACT_HASH,
                            'canonical_pre_action_packet_id': pre_action_packet_id,
                            'entry_action_plan': entry_action_plan,
                            'entry_execution_intent': entry_execution_intent,
                            'entry_execution_result': entry_execution_result,
                            'step2_execution_contract': STEP2_EXECUTION_CONTRACT,
                            'strategy_config_snapshot': _config_snapshot(),
                            'buying_power_at_signal': buying_power if self.trader is not None else None,
                            'concurrent_tickers': concurrent_tickers,
                            'session_elapsed_min': sig.get('session_elapsed_min'),
                            'session_high_at_entry': sig.get('session_high'),
                            'session_low_at_entry': sig.get('session_low'),
                            'gap_pct': sig.get('gap_pct'),
                            'market_tape_at_entry': market_tape_at_signal,
                            'spy_above_vwap': (market_tape_at_signal.get('SPY') or {}).get('above_vwap'),
                            'spy_day_pct': (market_tape_at_signal.get('SPY') or {}).get('day_pct'),
                            'qqq_above_vwap': (market_tape_at_signal.get('QQQ') or {}).get('above_vwap'),
                            'qqq_day_pct': (market_tape_at_signal.get('QQQ') or {}).get('day_pct'),
                            'iwm_above_vwap': (market_tape_at_signal.get('IWM') or {}).get('above_vwap'),
                            'iwm_day_pct': (market_tape_at_signal.get('IWM') or {}).get('day_pct'),
                            'btc_proxy_basket_at_entry': forensics.get('btc_proxy_basket') or {},
                            'market_microstructure_at_entry': forensics.get('market_microstructure') or {},
                            'gap_context_at_entry': forensics.get('gap_context') or {},
                            'liquidity_impact_at_entry': forensics.get('liquidity_impact') or {},
                            'short_borrow_snapshot': forensics.get('short_borrow_snapshot') or {},
                            'catalyst_context_at_entry': forensics.get('catalyst_context') or {},
                            '_best_price': round(price, 4),
                            '_worst_price': round(price, 4),
                            '_path': {
                                'first_green_ts': None,
                                'first_red_ts': None,
                                'time_to_mfe_sec': 0,
                                'time_to_mae_sec': 0,
                                'checks': {},
                            },
                            '_entry_timing': {
                                'signal_price': round(float(price), 4),
                                'windows_sec': {
                                    str(sec): {'best_entry_px': round(float(price), 4),
                                               'worst_entry_px': round(float(price), 4)}
                                    for sec in (15, 30, 60, 120)
                                },
                            },
                        }
                        self._critical_save('entry_position_committed', [tkr])
                        self._append_latency_attribution(forensics['latency_attribution'])
                        self._audit_event('entry_committed', tkr, {
                            'trade_id': trade_id,
                            'client_order_id': client_order_id if self.trader is not None else None,
                            'side': side,
                            'entry': round(float(price), 4),
                            'qty': qty if self.trader is None else int(qty),
                            'order_id': alpaca_order_id,
                            'bracket_policy': bracket,
                            'decision_audit': decision_audit,
                        })
                        self._append_decision_audit(decision_audit)
                        self._append_rule_dry_run_event('signal_entered', {
                            'created_at': entry_ts,
                            'trade_id': trade_id,
                            'ticker': tkr,
                            'side': side,
                            'decision': 'entered',
                            'score': sig.get('score'),
                            'setup_type': sig.get('setup_type') or self._setup_type(sig),
                            'near_miss_rules': near_miss_rules,
                            'entry_quality_tier': entry_quality_tier,
                            'forensics': {
                                'market_microstructure': forensics.get('market_microstructure') or {},
                                'gap_context': forensics.get('gap_context') or {},
                                'liquidity_impact': forensics.get('liquidity_impact') or {},
                                'short_borrow_snapshot': forensics.get('short_borrow_snapshot') or {},
                                'btc_proxy_basket': forensics.get('btc_proxy_basket') or {},
                                'catalyst_context': forensics.get('catalyst_context') or {},
                            },
                            'mode': 'passive_rule_dry_run',
                        })
                        self._append_shadow_decision(
                            sig, 'entered', 'live_entry', forensics,
                            {'qty': qty if self.trader is None else int(qty),
                             'sl': sl, 'tp': tp, 'bracket_policy': bracket},
                        )
                if duplicate_commit:
                    if alpaca_order_id and self.trader is not None:
                        try:
                            self._cancel_symbol_orders(tkr, alpaca_order_id)
                        except Exception as e:
                            log.error(f'{tkr} duplicate commit cleanup failed: {e}')
                    return
            finally:
                with self.lock:
                    if tkr in self.state.get('pending_entries', {}):
                        self.state['pending_entries'].pop(tkr, None)
                        self._critical_save('entry_pending_finalized', [tkr])

            try:
                self.engine.start_tick_capture(
                    tkr,
                    label=f'{side}_{sig.get("conviction", "?")}',
                    signal=sig,
                    capture_id=trade_id,
                    pre_seconds=120,
                    post_seconds=360,
                )
            except Exception as e:
                log.warning(f'tick capture start failed {tkr}: {e}')
            log.info(f'OPEN {side} {tkr} @ {price} qty={qty} '
                     f'alloc=${alloc:.2f} sl={sl} tp={tp} '
                     f'[{sig.get("conviction")}]'
                     + (f' alpaca={alpaca_order_id}' if alpaca_order_id else ''))
        except Exception as e:
            log.exception(f'_on_signal error: {e}')


    # ── relative volume ───────────────────────────────────────────────
    def _get_avg_daily_vol(self, tkr: str) -> Optional[float]:
        """Return 20-day average daily volume for tkr. Cached once per trading day."""
        today = datetime.now(CT).date().isoformat()
        cached = self._avg_vol_cache.get(tkr)
        if cached and cached['date'] == today:
            return cached['avg']
        try:
            end_dt   = datetime.now(timezone.utc)
            start_dt = end_dt - timedelta(days=35)  # ~25 trading days in 35 cal days
            daily = _bar_fetch_stock(
                tkr,
                start_dt.strftime('%Y-%m-%dT00:00:00Z'),
                end_dt.strftime('%Y-%m-%dT00:00:00Z'),
                '1Day',
            )
            if len(daily) < 5:
                return None
            sorted_daily = sorted(daily, key=lambda b: b['t'])
            vols = [b['v'] for b in sorted_daily[-RVOL_LOOKBACK:]]
            avg  = sum(vols) / len(vols)
            # Prior close = most recent complete day before today
            today_iso = today  # e.g. '2026-04-28'
            prior_bars = [b for b in sorted_daily
                          if datetime.fromtimestamp(b['t']/1000, tz=timezone.utc)
                             .strftime('%Y-%m-%d') != today_iso]
            prior_close = prior_bars[-1]['c'] if prior_bars else None
            self._avg_vol_cache[tkr] = {'avg': avg, 'prior_close': prior_close, 'date': today}
            log.info(f'RVOL cache {tkr}: avg_daily_vol={avg:,.0f} prior_close={prior_close} ({len(vols)}d)')
            return avg
        except Exception as e:
            log.warning(f'_get_avg_daily_vol({tkr}) failed: {e}')
            return None

    # ── monitor (exit conditions) ─────────────────────────────────────
    def _monitor_loop(self):
        # Alpaca reconciler runs on a slower cadence to respect rate limits.
        alpaca_tick = 0
        ALPACA_EVERY = 5  # seconds between Alpaca polls
        budget_tick = 0
        BUDGET_EVERY = 30  # seconds between daily-budget-snapshot checks
        skipped_tick = 0
        SKIPPED_EVERY = 15  # seconds between skipped-signal forward-return checks
        TAPE_EVERY = 60  # seconds between SPY/QQQ/IWM tape refreshes
        tape_tick = TAPE_EVERY  # refresh immediately on first running monitor tick
        blocked_flat_tick = 0
        BLOCKED_FLAT_EVERY = 30  # retry broker-exposure flatten while stopped/blocked
        heartbeat_tick = 0
        while not self._stop_evt.is_set():
            try:
                time.sleep(1.0)
                heartbeat_tick += 1
                if heartbeat_tick >= HEALTH_HEARTBEAT_SEC:
                    heartbeat_tick = 0
                    self._append_health_heartbeat()
                if not self.state.get('running'):
                    if self.trader is not None and self.state.get('positions'):
                        blocked_flat_tick += 1
                        if (not self._in_trading_window()
                                or self.state.get('broker_exposure_block')
                                or blocked_flat_tick >= BLOCKED_FLAT_EVERY):
                            blocked_flat_tick = 0
                            self._session_flat_alpaca()
                    continue
                blocked_flat_tick = 0
                # Daily budget snapshot — noop except once per day at ~08:15 CT
                budget_tick += 1
                if budget_tick >= BUDGET_EVERY:
                    budget_tick = 0
                    self._maybe_snapshot_daily_budget()
                skipped_tick += 1
                if skipped_tick >= SKIPPED_EVERY:
                    skipped_tick = 0
                    self._update_skipped_forwards()
                tape_tick += 1
                if tape_tick >= TAPE_EVERY:
                    tape_tick = 0
                    self._refresh_market_tape()
                if self.trader is not None:
                    alpaca_tick += 1
                    if alpaca_tick >= ALPACA_EVERY:
                        alpaca_tick = 0
                        for tkr in list(self.state['positions'].keys()):
                            self._reconcile_alpaca(tkr)
                            self._check_entry_fill_timeout(tkr)
                    if STEP_PARITY_LIVE_MODE:
                        for tkr in list(self.state['positions'].keys()):
                            self._check_exit(tkr)
                    # Optional conditional time-stop: force-close losing positions
                    # after COND_STOP_MIN. Disabled for Step 2 parity unless
                    # explicitly configured.
                    if (CONDITIONAL_TIME_STOP_ENABLED
                            and not STEP3_PARITY_LIVE_MODE
                            and self._in_trading_window()
                            and self.state['positions']):
                        self._check_cond_stop_alpaca()
                    # Hard flatten at/after the configured cutoff regardless.
                    if not self._in_trading_window() and self.state['positions']:
                        self._session_flat_alpaca()
                else:
                    for tkr in list(self.state['positions'].keys()):
                        self._check_exit(tkr)
                # MAE/MFE tracker — update _best_price/_worst_price every second
                with self.lock:
                    open_positions = list(self.state['positions'].items())
                for tkr, pos in open_positions:
                    px = self.engine.get_last_price(tkr)
                    if px is None:
                        continue
                    smart_exit_reason = None
                    with self.lock:
                        p = self.state['positions'].get(tkr)
                        if p is None:
                            continue
                        side = p['side']
                        entry = p.get('entry') or px
                        now_ts = int(time.time())
                        elapsed = max(0, now_ts - int(p.get('entry_ts') or now_ts))
                        path = p.setdefault('_path', {
                            'first_green_ts': None,
                            'first_red_ts': None,
                            'time_to_mfe_sec': 0,
                            'time_to_mae_sec': 0,
                            'checks': {},
                        })
                        signed_move = (px - entry) if side == 'LONG' else (entry - px)
                        if signed_move > 0 and path.get('first_green_ts') is None:
                            path['first_green_ts'] = now_ts
                        elif signed_move < 0 and path.get('first_red_ts') is None:
                            path['first_red_ts'] = now_ts
                        timing = p.setdefault('_entry_timing', {
                            'signal_price': round(float(entry), 4),
                            'windows_sec': {
                                str(sec): {'best_entry_px': round(float(entry), 4),
                                           'worst_entry_px': round(float(entry), 4)}
                                for sec in (15, 30, 60, 120)
                            },
                        })
                        for sec_s, row in (timing.get('windows_sec') or {}).items():
                            try:
                                sec_i = int(sec_s)
                            except Exception:
                                continue
                            if elapsed > sec_i:
                                continue
                            best_px = row.get('best_entry_px')
                            worst_px = row.get('worst_entry_px')
                            if side == 'LONG':
                                if best_px is None or px < float(best_px):
                                    row['best_entry_px'] = round(px, 4)
                                if worst_px is None or px > float(worst_px):
                                    row['worst_entry_px'] = round(px, 4)
                            else:
                                if best_px is None or px > float(best_px):
                                    row['best_entry_px'] = round(px, 4)
                                if worst_px is None or px < float(worst_px):
                                    row['worst_entry_px'] = round(px, 4)
                        for sec, label in ((30, '30s'), (60, '1m'), (180, '3m'), (300, '5m')):
                            checks = path.setdefault('checks', {})
                            if elapsed >= sec and label not in checks:
                                checks[label] = {
                                    'price': round(px, 4),
                                    'signed_pnl_per_share': round(signed_move, 4),
                                    'signed_return_pct': round((signed_move / entry * 100), 3) if entry else None,
                                }
                        if side == 'LONG':
                            if px > p.get('_best_price', px):
                                p['_best_price'] = round(px, 4)
                                path['time_to_mfe_sec'] = elapsed
                            if px < p.get('_worst_price', px):
                                p['_worst_price'] = round(px, 4)
                                path['time_to_mae_sec'] = elapsed
                        else:
                            if px < p.get('_best_price', px):
                                p['_best_price'] = round(px, 4)
                                path['time_to_mfe_sec'] = elapsed
                            if px > p.get('_worst_price', px):
                                p['_worst_price'] = round(px, 4)
                                path['time_to_mae_sec'] = elapsed
                        smart_exit_reason = self._smart_exit_reason(tkr, p, px)
                        btc_exit_reason = self._btc_impulse_exit_reason(p)
                        if btc_exit_reason:
                            hierarchy = p.setdefault('exit_reason_hierarchy', {
                                'created_at': int(time.time()),
                                'selected': None,
                                'candidates': [],
                            })
                            hierarchy.setdefault('candidates', []).append({
                                'reason': btc_exit_reason,
                                'kind': 'btc_impulse_abort',
                                'priority': len(hierarchy.get('candidates') or []) + 1,
                            })
                            if not smart_exit_reason:
                                hierarchy['selected'] = btc_exit_reason
                                smart_exit_reason = btc_exit_reason
                    if smart_exit_reason:
                        self._smart_exit_position(tkr, px, smart_exit_reason)
            except Exception as e:
                log.error(f'monitor loop error: {e}')

    def _build_exit_execution_pair(
        self,
        tkr: str,
        pos: dict,
        exit_price: float,
        reason: str,
        ts: Optional[int] = None,
        status: Optional[str] = None,
        broker_order_id: Optional[str] = None,
        broker_status: Optional[str] = None,
        raw_response: Optional[dict] = None,
    ) -> tuple[dict, dict, dict]:
        ts_i = int(ts or time.time())
        context = pos.get('exit_decision_context') if isinstance(pos.get('exit_decision_context'), dict) else {}
        action_plan = context.get('action_plan') if isinstance(context.get('action_plan'), dict) else {}
        if not action_plan:
            action_plan = execution_action_engine.plan_exit(
                pos,
                exit_price,
                ts_i,
                self._in_trading_window(),
                contract=STEP2_EXECUTION_CONTRACT,
                include_strict_brackets=True,
                include_conditional_time_stop=False,
                include_session_end=True,
                metadata={
                    'ticker': tkr,
                    'side': pos.get('side'),
                    'trade_id': pos.get('trade_id'),
                    'opportunity_id': pos.get('opportunity_id'),
                    'setup_type': pos.get('setup_type') or (pos.get('entry_thesis') or {}).get('setup_type'),
                    'execution_mode': EXECUTION_MODE,
                    'strategy_config_hash': STRATEGY_CONFIG_HASH,
                    'execution_kernel_hash': EXECUTION_KERNEL_HASH,
                    'step2_parity_contract_hash': STEP2_PARITY_CONTRACT_HASH,
                    'step2_execution_contract_hash': STEP2_EXECUTION_CONTRACT_HASH,
                },
            )
        action_plan.setdefault('identity', {}).update({
            'ticker': tkr,
            'side': pos.get('side'),
            'trade_id': pos.get('trade_id'),
            'client_order_id': pos.get('client_order_id'),
            'opportunity_id': pos.get('opportunity_id'),
            'setup_type': pos.get('setup_type') or (pos.get('entry_thesis') or {}).get('setup_type'),
        })
        action_plan['action'] = 'exit'
        action_plan['decision'] = 'closed'
        action_plan['reason'] = reason
        action_plan.setdefault('market', {})['exit_price'] = round(float(exit_price), 4)
        try:
            execution_action_engine.rehash_plan(action_plan)
        except Exception:
            pass
        venue = 'alpaca' if self.trader is not None else 'live_internal_sim'
        order_kind = (
            'local_strict_exit' if self.trader is not None and STEP_PARITY_LIVE_MODE
            else ('broker_exit' if self.trader is not None else 'simulated_exit')
        )
        metadata = {
            'ticker': tkr,
            'side': pos.get('side'),
            'trade_id': pos.get('trade_id'),
            'client_order_id': pos.get('client_order_id'),
            'setup_type': pos.get('setup_type') or (pos.get('entry_thesis') or {}).get('setup_type'),
            'timestamp_second': ts_i,
            'execution_mode': EXECUTION_MODE,
            'strategy_config_hash': STRATEGY_CONFIG_HASH,
            'execution_kernel_hash': EXECUTION_KERNEL_HASH,
            'step2_parity_contract_hash': STEP2_PARITY_CONTRACT_HASH,
            'step2_execution_contract_hash': STEP2_EXECUTION_CONTRACT_HASH,
        }
        intent = execution_intent_engine.exit_intent(
            action_plan,
            venue=venue,
            qty=pos.get('qty'),
            trade_id=pos.get('trade_id'),
            client_order_id=pos.get('client_order_id'),
            broker_order_id=broker_order_id or pos.get('alpaca_order_id'),
            exit_price=round(float(exit_price), 4),
            reason=reason,
            order_kind=order_kind,
            metadata=metadata,
            ts=ts_i,
        )
        result = execution_intent_engine.result_from_intent(
            intent,
            status or ('local_strict_triggered' if self.trader is not None and STEP_PARITY_LIVE_MODE else 'simulated_closed'),
            ok=True,
            ts=ts_i,
            broker_order_id=broker_order_id,
            broker_status=broker_status,
            fill_price=round(float(exit_price), 4),
            filled_qty=pos.get('qty'),
            reason=reason,
            raw_response=raw_response,
            latency=dict(pos.get('latency_chain') or {}),
            metadata=metadata,
        )
        return action_plan, intent, result

    def _check_exit(self, tkr: str):
        """Pure-internal simulation path (USE_ALPACA_EXECUTION=0)."""
        with self.lock:
            pos = self.state['positions'].get(tkr)
            if not pos:
                return
            side = pos['side']
            sl   = pos['sl']
            tp   = pos['tp']

        price = self.engine.get_last_price(tkr)
        if price is None:
            return

        now_ts = int(time.time())
        exit_decision = live_step2_decision_kernel.exit_decision(
            side,
            float(price),
            float(sl),
            float(tp),
            self._in_trading_window(),
            entry_price=pos.get('entry'),
            entry_ts=pos.get('entry_ts'),
            now_ts=now_ts,
            conditional_time_stop_enabled=bool(CONDITIONAL_TIME_STOP_ENABLED and not STEP3_PARITY_LIVE_MODE),
            conditional_time_stop_min=COND_STOP_MIN,
            contract=STEP2_EXECUTION_CONTRACT,
            metadata={
                'ticker': tkr,
                'trade_id': pos.get('trade_id'),
                'setup_type': pos.get('setup_type'),
                'execution_mode': EXECUTION_MODE,
                'strategy_config_hash': STRATEGY_CONFIG_HASH,
                'execution_kernel_hash': EXECUTION_KERNEL_HASH,
                'step2_parity_contract_hash': STEP2_PARITY_CONTRACT_HASH,
                'step2_execution_contract_hash': STEP2_EXECUTION_CONTRACT_HASH,
            },
        )
        exit_price = exit_decision.get('exit_price')
        reason = exit_decision.get('reason')
        if exit_decision.get('decision') == 'exit':
            with self.lock:
                cur = self.state['positions'].get(tkr)
                if cur is not None:
                    cur['exit_decision_context'] = exit_decision
                    exit_action_plan, exit_execution_intent, exit_execution_result = self._build_exit_execution_pair(
                        tkr,
                        cur,
                        float(exit_price),
                        reason,
                        ts=now_ts,
                    )
                    cur['exit_action_plan'] = exit_action_plan
                    cur['exit_execution_intent'] = exit_execution_intent
                    cur['exit_execution_result'] = exit_execution_result
                    self._audit_event('exit_intent_created', tkr, {
                        'trade_id': cur.get('trade_id'),
                        'execution_intent_id': exit_execution_intent.get('execution_intent_id'),
                        'semantic_execution_intent_hash': exit_execution_intent.get('semantic_execution_intent_hash'),
                        'execution_intent': exit_execution_intent,
                    })
                    self._audit_event('exit_execution_result', tkr, {
                        'trade_id': cur.get('trade_id'),
                        'execution_intent_id': exit_execution_intent.get('execution_intent_id'),
                        'execution_result_id': exit_execution_result.get('execution_result_id'),
                        'execution_result': exit_execution_result,
                    })

        if exit_price is not None:
            parent_order_id = None
            if self.trader is not None and STEP_PARITY_LIVE_MODE:
                with self.lock:
                    cur = self.state['positions'].get(tkr) or {}
                    parent_order_id = cur.get('alpaca_order_id')
                    cur['broker_close'] = {
                        'reason': reason,
                        'submitted_at': int(time.time()),
                        'expected_side': 'sell' if side == 'LONG' else 'buy',
                        'response_status': 'local_strict_tp_sl_triggered',
                        'local_strict_exit_price': round(float(exit_price), 4),
                        'source': EXECUTION_MODE,
                    }
                    self._save()
            self._close_position(tkr, exit_price, reason)
            if self.trader is not None and STEP_PARITY_LIVE_MODE:
                try:
                    self._submit_local_strict_broker_cleanup(tkr, parent_order_id, reason)
                except Exception as e:
                    log.error(f'{tkr} local strict broker cleanup failed after {reason}: {e}')

    def _submit_local_strict_broker_cleanup(self, tkr: str, parent_order_id: Optional[str], reason: str):
        """Best-effort broker cleanup after local strict TP/SL owns the simulated exit."""
        if self.trader is None:
            return
        self._audit_event('local_strict_broker_cleanup_requested', tkr, {
            'reason': reason,
            'parent_order_id': parent_order_id,
        })
        try:
            self._cancel_symbol_orders(tkr, parent_order_id)
            self._record_broker_api_success()
        except Exception as e:
            log.warning(f'{tkr} local strict cleanup cancel failed: {e}')
            self._record_broker_api_failure('local_strict_cleanup_cancel', e)
        try:
            response = self.trader.close_position(tkr)
            self._record_broker_api_success()
            self._audit_event('local_strict_broker_cleanup_submitted', tkr, {
                'reason': reason,
                'response': response,
            })
        except AlpacaTradingError as e:
            if e.status not in (404, 422):
                log.warning(f'{tkr} local strict cleanup close_position failed: {e}')
                self._record_broker_api_failure('local_strict_cleanup_close_position', e)
        except Exception as e:
            log.warning(f'{tkr} local strict cleanup close_position failed: {e}')
            self._record_broker_api_failure('local_strict_cleanup_close_position', e)

    # ── Alpaca reconciler path ────────────────────────────────────────
    def _check_entry_fill_timeout(self, tkr: str):
        with self.lock:
            pos = self.state['positions'].get(tkr)
            if not pos or pos.get('alpaca_filled_entry') or pos.get('entry_timeout_checked'):
                return
            entry_ts = int(pos.get('entry_ts') or time.time())
            oid = pos.get('alpaca_order_id')
            if int(time.time()) - entry_ts < ENTRY_FILL_TIMEOUT_SEC:
                return
            pos['entry_timeout_checked'] = True
            self._save()
        try:
            order = self.trader.get_order(oid, nested=True) if oid else {}
            self._record_broker_api_success()
        except Exception as e:
            self._record_broker_api_failure('entry_fill_timeout_get_order', e)
            return
        if order.get('status') == 'filled':
            return
        try:
            self._cancel_symbol_orders(tkr, oid)
            self._record_broker_api_success()
        except Exception as e:
            self._record_broker_api_failure('entry_fill_timeout_cancel', e)
            return
        with self.lock:
            pos = self.state['positions'].get(tkr)
            if pos and not pos.get('alpaca_filled_entry'):
                self.state['positions'].pop(tkr, None)
                self._critical_save('entry_fill_timeout_cancelled', [tkr])
        self._audit_event('entry_fill_timeout_cancelled', tkr, {
            'order_id': oid,
            'timeout_sec': ENTRY_FILL_TIMEOUT_SEC,
            'last_status': order.get('status'),
        })

    def _reconcile_alpaca(self, tkr: str):
        """Poll Alpaca for this ticker's parent order + child legs. Close
        internal position when a TP/SL child fills (or parent dies)."""
        with self.lock:
            pos = self.state['positions'].get(tkr)
            if not pos or not pos.get('alpaca_order_id'):
                return
            oid = pos['alpaca_order_id']

        try:
            o = self.trader.get_order(oid, nested=True)
            self._record_broker_api_success()
        except Exception as e:
            log.error(f'reconcile {tkr} get_order({oid}) failed: {e}')
            self._record_broker_api_failure('reconcile_get_order', e)
            return

        parent_status = o.get('status')
        # Track entry fill
        if not pos.get('alpaca_filled_entry') and parent_status == 'filled':
            fill_price = float(o.get('filled_avg_price') or pos['entry'])
            entry_ref = float(pos.get('entry') or fill_price)
            side = pos.get('side')
            slip = (fill_price - entry_ref) if side == 'LONG' else (entry_ref - fill_price)
            slip_pct = round(slip / entry_ref * 100, 4) if entry_ref else None
            entry_fill_attr = self._record_fill_attribution(
                tkr, side, 'entry_fill',
                price_ref=entry_ref,
                qty=pos.get('qty'),
                trade_id=pos.get('trade_id'),
                client_order_id=pos.get('client_order_id'),
                broker_order_id=oid,
                fill_price=fill_price,
                broker_fill=o,
            )
            with self.lock:
                if tkr in self.state['positions']:
                    latency_chain = dict(self.state['positions'][tkr].get('latency_chain') or {})
                    latency_chain['entry_filled_ms'] = int(time.time() * 1000)
                    self.state['positions'][tkr]['alpaca_filled_entry'] = True
                    self.state['positions'][tkr]['entry_fill_price'] = fill_price
                    self.state['positions'][tkr]['entry_filled_ts'] = int(time.time())
                    self.state['positions'][tkr]['entry_fill_slippage_pct'] = slip_pct
                    self.state['positions'][tkr]['alpaca_status'] = parent_status
                    self.state['positions'][tkr]['latency_chain'] = latency_chain
                    latency_row = self._latency_attribution_row(
                        tkr, side,
                        self.state['positions'][tkr].get('trade_id'),
                        self.state['positions'][tkr].get('client_order_id'),
                        oid,
                        latency_chain,
                        'entry_filled',
                    )
                    self.state['positions'][tkr]['latency_attribution'] = latency_row
                    self.state['positions'][tkr].setdefault('fill_attribution', []).append(entry_fill_attr)
                    self._save()
                    self._append_latency_attribution(latency_row)
            self._audit_event('entry_filled', tkr, {
                'trade_id': pos.get('trade_id'),
                'order_id': oid,
                'fill_price': o.get('filled_avg_price'),
                'slippage_pct': slip_pct,
                'status': parent_status,
            })

        # Parent dead before fill → drop position (no P/L)
        if parent_status in ('canceled', 'rejected', 'expired') \
                and not pos.get('alpaca_filled_entry'):
            log.warning(f'{tkr} entry {oid} {parent_status} — dropping position')
            with self.lock:
                self.state['positions'].pop(tkr, None)
                self._save()
            return

        # Check child legs for TP/SL fill
        for leg in o.get('legs') or []:
            if leg.get('status') == 'filled':
                leg_type = leg.get('type')  # 'limit'=TP, 'stop'=SL
                reason = 'take_profit' if leg_type == 'limit' else 'stop_loss'
                exit_price = float(leg.get('filled_avg_price')
                                    or leg.get('limit_price')
                                    or leg.get('stop_price') or 0)
                if exit_price > 0:
                    exit_fill = {
                        'order_id': leg.get('id'),
                        'client_order_id': leg.get('client_order_id'),
                        'side': leg.get('side'),
                        'type': leg.get('type'),
                        'filled_avg_price': exit_price,
                        'filled_qty': leg.get('filled_qty'),
                        'filled_at': leg.get('filled_at'),
                    }
                    exit_fill_attr = self._record_fill_attribution(
                        tkr, pos.get('side'), 'exit_fill_bracket',
                        price_ref=exit_price,
                        qty=pos.get('qty'),
                        trade_id=pos.get('trade_id'),
                        client_order_id=pos.get('client_order_id'),
                        broker_order_id=leg.get('id'),
                        fill_price=exit_price,
                        broker_fill=exit_fill,
                    )
                    with self.lock:
                        if tkr in self.state['positions']:
                            pos_live = self.state['positions'][tkr]
                            latency_chain = dict(pos_live.get('latency_chain') or {})
                            latency_chain.setdefault('exit_trigger_ms', int(time.time() * 1000))
                            fill_ms = self._parse_broker_time_ms(exit_fill.get('filled_at'))
                            latency_chain['exit_broker_filled_ms'] = int(fill_ms or time.time() * 1000)
                            latency_chain['exit_verified_flat_ms'] = int(time.time() * 1000)
                            pos_live['latency_chain'] = latency_chain
                            self.state['positions'][tkr]['broker_close'] = {
                                'reason': reason,
                                'submitted_at': int(time.time()),
                                'verified_flat_at': int(time.time()),
                                'expected_side': leg.get('side'),
                                'response_order_id': leg.get('id'),
                                'response_status': leg.get('status'),
                                'fill': exit_fill,
                                'latency_chain': latency_chain,
                            }
                            exit_action_plan, exit_execution_intent, exit_execution_result = self._build_exit_execution_pair(
                                tkr,
                                pos_live,
                                exit_price,
                                reason,
                                ts=int(time.time()),
                                status='broker_filled',
                                broker_order_id=leg.get('id'),
                                broker_status=leg.get('status'),
                                raw_response=exit_fill,
                            )
                            pos_live['exit_action_plan'] = exit_action_plan
                            pos_live['exit_execution_intent'] = exit_execution_intent
                            pos_live['exit_execution_result'] = exit_execution_result
                            self._audit_event('exit_intent_created', tkr, {
                                'trade_id': pos_live.get('trade_id'),
                                'execution_intent_id': exit_execution_intent.get('execution_intent_id'),
                                'semantic_execution_intent_hash': exit_execution_intent.get('semantic_execution_intent_hash'),
                                'execution_intent': exit_execution_intent,
                            })
                            self._audit_event('exit_execution_result', tkr, {
                                'trade_id': pos_live.get('trade_id'),
                                'execution_intent_id': exit_execution_intent.get('execution_intent_id'),
                                'execution_result_id': exit_execution_result.get('execution_result_id'),
                                'execution_result': exit_execution_result,
                            })
                            self.state['positions'][tkr].setdefault('fill_attribution', []).append(exit_fill_attr)
                            self._critical_save('bracket_exit_fill_recorded', [tkr])
                    self._audit_event('bracket_exit_filled', tkr, {
                        'trade_id': pos.get('trade_id'),
                        'reason': reason,
                        'order_id': leg.get('id'),
                        'exit_price': exit_price,
                    })
                    self._close_position(tkr, exit_price, reason)
                return

    def _cancel_symbol_orders(self, tkr: str, parent_order_id: Optional[str] = None):
        """Cancel open orders for one ticker only."""
        if self.trader is None:
            return
        seen = set()

        def cancel_oid(oid):
            if not oid or oid in seen:
                return
            seen.add(oid)
            try:
                self.trader.cancel_order(oid)
                log.info(f'cancelled {tkr} order {oid}')
            except AlpacaTradingError as e:
                if e.status not in (404, 422):
                    log.warning(f'cancel {tkr} order {oid} failed: {e}')
            except Exception as e:
                log.warning(f'cancel {tkr} order {oid} failed: {e}')

        if parent_order_id:
            try:
                parent = self.trader.get_order(parent_order_id, nested=True)
                for leg in parent.get('legs') or []:
                    cancel_oid(leg.get('id'))
            except Exception as e:
                log.warning(f'get_order({parent_order_id}) for scoped cancel failed: {e}')
            cancel_oid(parent_order_id)

        try:
            for order in self.trader.list_orders(status='open', symbols=[tkr], nested=True):
                for leg in order.get('legs') or []:
                    cancel_oid(leg.get('id'))
                cancel_oid(order.get('id'))
        except Exception as e:
            log.warning(f'list_orders({tkr}) for scoped cancel failed: {e}')

    def _verify_symbols_flat(self, symbols, attempts: int = 4, delay_sec: float = 1.0) -> set:
        """Return symbols that still have broker positions after close attempts."""
        remaining = set(symbols)
        if self.trader is None or not remaining:
            return set()
        for attempt in range(attempts):
            try:
                alpaca_positions = {p.get('symbol') for p in self.trader.list_positions()}
                remaining = {s for s in remaining if s in alpaca_positions}
                if not remaining:
                    return set()
            except Exception as e:
                log.error(f'flat verification failed for {sorted(remaining)}: {e}')
            if attempt < attempts - 1:
                time.sleep(delay_sec)
        return remaining

    def _set_broker_exposure_block(self, symbols, reason: str):
        remaining = sorted(set(symbols))
        if not remaining:
            return
        try:
            self._write_emergency_state_block({
                'created_at': int(time.time()),
                'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
                'symbols': remaining,
                'reason': reason,
                'source': 'broker_exposure_block',
            })
        except Exception as e:
            log.critical(f'BROKER_EXPOSURE_BLOCK_SIDECAR_FAILED {reason}: {e}')
        with self.lock:
            self.state['running'] = False
            self.state['broker_exposure_block'] = {
                'created_at': int(time.time()),
                'symbols': remaining,
                'reason': reason,
            }
            self._save()
        log.error(f'BROKER_EXPOSURE_BLOCK {reason}: {remaining}')

    def _clear_broker_exposure_block(self):
        with self.lock:
            if 'broker_exposure_block' in self.state:
                self.state.pop('broker_exposure_block', None)
                self._save()
        try:
            if os.path.exists(EMERGENCY_STATE_BLOCK_PATH):
                os.remove(EMERGENCY_STATE_BLOCK_PATH)
        except Exception as e:
            log.warning(f'failed to clear emergency state block sidecar: {e}')

    def _iso_utc(self, ts: int) -> str:
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    def _filled_order_candidates(self, orders, expected_side: Optional[str]) -> list:
        candidates = []
        for order in orders:
            stack = [order] + list(order.get('legs') or [])
            for item in stack:
                if item.get('status') != 'filled':
                    continue
                if expected_side and item.get('side') != expected_side:
                    continue
                px = item.get('filled_avg_price')
                if not px:
                    continue
                filled_at = item.get('filled_at') or item.get('updated_at') or ''
                candidates.append({
                    'order_id': item.get('id'),
                    'client_order_id': item.get('client_order_id'),
                    'side': item.get('side'),
                    'type': item.get('type'),
                    'filled_avg_price': float(px),
                    'filled_qty': item.get('filled_qty'),
                    'filled_at': filled_at,
                })
        return candidates

    def _latest_broker_fill(self, tkr: str, expected_side: Optional[str],
                            after_ts: int, order_id: Optional[str] = None) -> Optional[dict]:
        """Best-effort lookup of a filled Alpaca close, preferring exact order id."""
        if self.trader is None:
            return None
        if order_id:
            try:
                order = self.trader.get_order(order_id, nested=True)
                exact = self._filled_order_candidates([order], expected_side)
                if exact:
                    exact.sort(key=lambda x: x.get('filled_at') or '', reverse=True)
                    return exact[0]
            except Exception as e:
                log.warning(f'{tkr}: exact broker fill lookup {order_id} failed: {e}')
        try:
            orders = self.trader.list_orders(
                status='closed',
                symbols=[tkr],
                nested=True,
                limit=50,
                after=self._iso_utc(max(0, after_ts - 30)),
                until=self._iso_utc(int(time.time()) + 60),
                direction='desc',
            )
        except Exception as e:
            log.warning(f'{tkr}: broker fill lookup failed: {e}')
            return {'lookup_error': str(e)}

        candidates = self._filled_order_candidates(orders, expected_side)
        if not candidates:
            return None
        candidates.sort(key=lambda x: x.get('filled_at') or '', reverse=True)
        return candidates[0]

    def _attach_broker_close(self, tkr: str, reason: str, submitted_at: int,
                             response: Optional[dict] = None,
                             submit_start_ms: Optional[int] = None,
                             submit_end_ms: Optional[int] = None):
        with self.lock:
            pos = self.state['positions'].get(tkr)
            if not pos:
                return
            expected_side = 'sell' if pos.get('side') == 'LONG' else 'buy'
        response_order_id = (response or {}).get('id')
        fill = self._latest_broker_fill(tkr, expected_side, submitted_at, response_order_id)
        exit_fill_attr = None
        if isinstance(fill, dict) and fill.get('filled_avg_price'):
            exit_fill_attr = self._record_fill_attribution(
                tkr, pos.get('side'), 'exit_fill_verified',
                price_ref=fill.get('filled_avg_price'),
                qty=pos.get('qty'),
                trade_id=pos.get('trade_id'),
                client_order_id=pos.get('client_order_id'),
                broker_order_id=fill.get('order_id') or response_order_id,
                fill_price=fill.get('filled_avg_price'),
                broker_fill=fill,
            )
        with self.lock:
            pos = self.state['positions'].get(tkr)
            if not pos:
                return
            latency_chain = dict(pos.get('latency_chain') or {})
            latency_chain.setdefault('exit_trigger_ms', int((submit_start_ms or time.time() * 1000)))
            if submit_start_ms:
                latency_chain.setdefault('exit_submit_start_ms', int(submit_start_ms))
            if submit_end_ms:
                latency_chain.setdefault('exit_submit_end_ms', int(submit_end_ms))
            if isinstance(fill, dict):
                fill_ms = self._parse_broker_time_ms(fill.get('filled_at'))
                if fill_ms:
                    latency_chain['exit_broker_filled_ms'] = fill_ms
            latency_chain['exit_verified_flat_ms'] = int(time.time() * 1000)
            pos['latency_chain'] = latency_chain
            pos['broker_close'] = {
                'reason': reason,
                'submitted_at': submitted_at,
                'verified_flat_at': int(time.time()),
                'expected_side': expected_side,
                'response_order_id': (response or {}).get('id'),
                'response_status': (response or {}).get('status'),
                'fill': fill,
                'latency_chain': latency_chain,
                'exit_time_to_submit_ms': self._ms_delta(latency_chain, 'exit_trigger_ms', 'exit_submit_start_ms'),
                'exit_time_to_fill_ms': self._ms_delta(latency_chain, 'exit_trigger_ms', 'exit_broker_filled_ms'),
                'exit_time_to_flat_ms': self._ms_delta(latency_chain, 'exit_trigger_ms', 'exit_verified_flat_ms'),
            }
            if exit_fill_attr:
                pos.setdefault('fill_attribution', []).append(exit_fill_attr)
            self._save()
        self._audit_event('broker_flat_verified', tkr, {
            'reason': reason,
            'fill': fill,
            'response_order_id': (response or {}).get('id'),
            'response_status': (response or {}).get('status'),
        })

    def _submit_extended_hours_close(self, sym: str) -> Optional[dict]:
        """Best-effort after-hours limit order to flatten a lingering position."""
        if self.trader is None:
            return None
        broker_pos = self.trader.get_position(sym)
        if not broker_pos:
            return None
        qty = broker_pos.get('qty')
        side = 'sell' if broker_pos.get('side') == 'long' else 'buy'
        ref_px = None
        for key in ('current_price', 'market_value'):
            try:
                val = float(broker_pos.get(key) or 0)
                if key == 'market_value':
                    q = abs(float(qty or 0))
                    val = val / q if q else 0
                if val > 0:
                    ref_px = val
                    break
            except Exception:
                continue
        ref_px = self.engine.get_last_price(sym) or ref_px
        if not ref_px:
            return None
        limit_price = ref_px * (0.985 if side == 'sell' else 1.015)
        body = {
            'symbol': sym,
            'qty': str(int(abs(float(qty)))),
            'side': side,
            'type': 'limit',
            'time_in_force': 'day',
            'limit_price': f'{limit_price:.2f}',
            'extended_hours': True,
            'client_order_id': f'scalp-ah-flat-{sym}-{int(time.time() * 1000)}',
        }
        return self.trader._request('POST', '/v2/orders', json_body=body)

    def _confirmed_broker_close(self, symbols, reason: str,
                                close_all: bool = False,
                                parent_order_ids: Optional[dict] = None) -> set:
        """Submit broker close(s), verify flat, and return symbols still open."""
        symbols = sorted(set(symbols))
        parent_order_ids = parent_order_ids or {}
        if self.trader is None or not symbols:
            return set()

        submitted_at = int(time.time())
        submitted_at_ms = int(time.time() * 1000)
        close_responses = {}
        close_submit_start_ms = {}
        close_submit_end_ms = {}
        self._audit_event('broker_close_requested', data={
            'symbols': symbols,
            'reason': reason,
            'close_all': close_all,
        })
        with self.lock:
            for sym in symbols:
                pos = self.state.get('positions', {}).get(sym)
                if pos:
                    latency_chain = dict(pos.get('latency_chain') or {})
                    latency_chain.setdefault('exit_trigger_ms', submitted_at_ms)
                    pos['latency_chain'] = latency_chain
        if close_all:
            try:
                self.trader.cancel_all_orders()
                self._record_broker_api_success()
            except Exception as e:
                log.error(f'{reason}: cancel_all_orders failed: {e}')
                self._record_broker_api_failure(f'{reason}_cancel_all_orders', e)
            try:
                for sym in symbols:
                    close_submit_start_ms[sym] = int(time.time() * 1000)
                responses = self.trader.close_all_positions(cancel_orders=False)
                end_ms = int(time.time() * 1000)
                self._record_broker_api_success()
                for resp in responses or []:
                    sym = resp.get('symbol')
                    if sym:
                        close_responses[sym] = resp
                        close_submit_end_ms[sym] = end_ms
            except Exception as e:
                log.error(f'{reason}: close_all_positions failed: {e}')
                self._record_broker_api_failure(f'{reason}_close_all_positions', e)
        else:
            for sym in symbols:
                try:
                    self._cancel_symbol_orders(sym, parent_order_ids.get(sym))
                    self._record_broker_api_success()
                except Exception as e:
                    log.error(f'{reason} {sym}: scoped order cancel failed: {e}')
                    self._record_broker_api_failure(f'{reason}_scoped_cancel', e)
                try:
                    close_submit_start_ms[sym] = int(time.time() * 1000)
                    close_responses[sym] = self.trader.close_position(sym)
                    close_submit_end_ms[sym] = int(time.time() * 1000)
                    self._record_broker_api_success()
                except Exception as e:
                    close_submit_end_ms[sym] = int(time.time() * 1000)
                    log.error(f'{reason} {sym}: close_position failed: {e}')
                    self._record_broker_api_failure(f'{reason}_close_position', e)

        remaining = self._verify_symbols_flat(symbols, attempts=5, delay_sec=1.0)
        for sym in sorted(remaining):
            try:
                self._cancel_symbol_orders(sym, parent_order_ids.get(sym))
                close_submit_start_ms[sym] = int(time.time() * 1000)
                close_responses[sym] = self.trader.close_position(sym)
                close_submit_end_ms[sym] = int(time.time() * 1000)
                self._record_broker_api_success()
                log.warning(f'{reason}: retry close submitted for {sym}')
            except Exception as e:
                close_submit_end_ms[sym] = int(time.time() * 1000)
                log.error(f'{reason}: retry close_position({sym}) failed: {e}')
                self._record_broker_api_failure(f'{reason}_retry_close_position', e)
        remaining = self._verify_symbols_flat(symbols, attempts=3, delay_sec=1.0)
        if remaining and not self._in_trading_window():
            for sym in sorted(remaining):
                try:
                    self._cancel_symbol_orders(sym, parent_order_ids.get(sym))
                    close_submit_start_ms[sym] = int(time.time() * 1000)
                    close_responses[sym] = self._submit_extended_hours_close(sym) or close_responses.get(sym)
                    close_submit_end_ms[sym] = int(time.time() * 1000)
                    self._record_broker_api_success()
                    log.warning(f'{reason}: extended-hours flatten submitted for {sym}')
                except Exception as e:
                    close_submit_end_ms[sym] = int(time.time() * 1000)
                    log.error(f'{reason}: extended-hours flatten({sym}) failed: {e}')
                    self._record_broker_api_failure(f'{reason}_extended_hours_flatten', e)
            remaining = self._verify_symbols_flat(symbols, attempts=10, delay_sec=3.0)
        for sym in [s for s in symbols if s not in remaining]:
            self._attach_broker_close(
                sym, reason, submitted_at, close_responses.get(sym),
                close_submit_start_ms.get(sym), close_submit_end_ms.get(sym),
            )
        if remaining:
            self._audit_event('broker_close_not_flat', data={
                'symbols': sorted(remaining),
                'reason': reason,
            })
        return remaining

    def _check_cond_stop_alpaca(self):
        """After COND_STOP_MIN minutes, force-close any position that is currently
        losing (unrealized P&L < 0). Profitable positions are left to run until
        TP / SL / session-end. Cancels bracket legs then market-closes on Alpaca."""
        now = int(time.time())
        with self.lock:
            candidates = [
                (tkr, pos) for tkr, pos in self.state['positions'].items()
                if pos.get('entry_ts')
                and (now - pos['entry_ts']) >= COND_STOP_MIN * 60
                and pos.get('alpaca_filled_entry')  # must be actually filled
                and not pos.get('time_stop_triggered')
            ]
        for tkr, pos in candidates:
            price = self.engine.get_last_price(tkr)
            if price is None:
                continue
            side = pos['side']
            unrealized = (price - pos['entry']) if side == 'LONG' \
                         else (pos['entry'] - price)
            if unrealized >= 0:
                continue  # profitable or flat — let it run
            held_min = (now - pos['entry_ts']) // 60
            log.info(f'COND-STOP {tkr}: held {held_min}min unrealized={unrealized:+.4f}, '
                     f'force-closing at market')
            price = self.engine.get_last_price(tkr) or pos.get('entry')
            self._start_exit_tick_capture(tkr, pos, price, 'cond_time_stop')
            with self.lock:
                if tkr in self.state['positions']:
                    self.state['positions'][tkr]['time_stop_triggered'] = True
                    self._save()
            try:
                remaining = self._confirmed_broker_close(
                    [tkr], 'cond_stop',
                    parent_order_ids={tkr: pos.get('alpaca_order_id')},
                )
            except Exception as e:
                log.error(f'cond_stop {tkr}: broker close workflow failed: {e}')
                remaining = {tkr}
            if remaining:
                self._set_broker_exposure_block(
                    remaining,
                    'cond_stop_position_not_flat_after_close_attempt',
                )
                continue
            self._close_position(tkr, price, 'cond_time_stop')

    def _session_flat_alpaca(self):
        """At the hard-flat cutoff or after: cancel orders and market-close all positions."""
        with self.lock:
            open_tkrs = list(self.state['positions'].keys())
        if not open_tkrs:
            return
        for tkr in open_tkrs:
            with self.lock:
                pos = self.state['positions'].get(tkr)
            price = self.engine.get_last_price(tkr) or (pos or {}).get('entry')
            self._start_exit_tick_capture(tkr, pos, price, 'session_end')
        try:
            remaining = self._confirmed_broker_close(open_tkrs, 'session_flat')
        except Exception as e:
            log.error(f'session_flat: broker close workflow failed: {e}')
            remaining = set(open_tkrs)
        if remaining:
            self._set_broker_exposure_block(
                remaining,
                'session_flat_positions_not_flat_after_close_attempt',
            )
        # Record closes only for symbols verified flat at the broker.
        for tkr in [s for s in open_tkrs if s not in remaining]:
            with self.lock:
                pos = self.state['positions'].get(tkr)
            price = self.engine.get_last_price(tkr) or (pos or {}).get('entry')
            self._close_position(tkr, price, 'session_end')
        self._write_broker_safety_snapshot('post_market_flatten')

    def _lookup_broker_order_by_client_id(self, client_order_id: Optional[str]) -> Optional[dict]:
        if self.trader is None or not client_order_id:
            return None
        if hasattr(self.trader, 'get_order_by_client_order_id'):
            return self.trader.get_order_by_client_order_id(client_order_id, nested=True)
        for order in self.trader.list_orders(status='all', limit=100, nested=True):
            stack = [order] + list(order.get('legs') or [])
            for item in stack:
                if item.get('client_order_id') == client_order_id:
                    return order
        return None

    def _recover_pending_broker_orders_on_boot(self):
        """Fail closed if a saved pending entry maps to a live/filled broker order."""
        if self.trader is None:
            return
        with self.lock:
            pending_snapshot = {
                tkr: dict(row) for tkr, row in (self.state.get('pending_entries') or {}).items()
            }
            internal = set((self.state.get('positions') or {}).keys())
        if not pending_snapshot:
            return
        active_statuses = {
            'new', 'accepted', 'pending_new', 'partially_filled', 'filled',
            'held', 'pending_replace', 'pending_cancel',
        }
        terminal_statuses = {'canceled', 'cancelled', 'expired', 'rejected', 'replaced'}
        recovered = []
        cleared = []
        blocked = []
        for tkr, row in pending_snapshot.items():
            cid = row.get('client_order_id') or row.get('broker_client_order_id')
            order = None
            if cid:
                try:
                    order = self._lookup_broker_order_by_client_id(cid)
                except AlpacaTradingError as e:
                    if e.status != 404:
                        log.error(f'boot pending order lookup failed {tkr} cid={cid}: {e}')
                        blocked.append(tkr)
                        continue
                    order = None
                except Exception as e:
                    log.error(f'boot pending order lookup failed {tkr} cid={cid}: {e}')
                    blocked.append(tkr)
                    continue
            status = str((order or {}).get('status') or row.get('alpaca_status') or '').lower()
            oid = (order or {}).get('id') or row.get('alpaca_order_id')
            if order and status in active_statuses:
                recovered.append({'symbol': tkr, 'client_order_id': cid, 'order_id': oid, 'status': status})
                with self.lock:
                    if tkr in self.state.get('pending_entries', {}):
                        self.state['pending_entries'][tkr].update({
                            'alpaca_order_id': oid,
                            'alpaca_status': status,
                            'broker_recovered_on_boot': int(time.time()),
                        })
                        self._save()
                if tkr not in internal:
                    try:
                        if status not in ('filled', 'partially_filled'):
                            self._cancel_symbol_orders(tkr, oid)
                    except Exception as e:
                        log.error(f'boot pending order cancel failed {tkr}: {e}')
                    blocked.append(tkr)
            elif (not order) or status in terminal_statuses:
                with self.lock:
                    self.state.setdefault('pending_entries', {}).pop(tkr, None)
                    self._save()
                cleared.append({'symbol': tkr, 'client_order_id': cid, 'status': status or 'not_found'})
            else:
                blocked.append(tkr)
        if recovered or cleared or blocked:
            self._audit_event('boot_pending_entry_reconcile', data={
                'recovered': recovered,
                'cleared': cleared,
                'blocked': sorted(set(blocked)),
            })
        if blocked:
            self._set_broker_exposure_block(
                sorted(set(blocked)),
                'pending_entry_broker_order_recovered_on_boot',
            )

    def _recover_orphan_bracket_position_on_boot(self, sym: str, broker_pos: dict) -> bool:
        """Rebuild local state for an Alpaca bracket that survived a restart."""
        if self.trader is None:
            return False
        try:
            orders = self.trader.list_orders(status='open', symbols=[sym], nested=True) or []
        except Exception as e:
            log.error(f'boot: list open orders for orphan {sym} failed: {e}')
            return False

        bracket = None
        for order in orders:
            if order.get('symbol') != sym:
                continue
            client_order_id = str(order.get('client_order_id') or '')
            if client_order_id.startswith('scalp-') and (
                    order.get('order_class') == 'bracket' or order.get('legs')):
                bracket = order
                break
        if not bracket:
            return False

        try:
            qty = int(abs(float(broker_pos.get('qty') or bracket.get('filled_qty') or 0)))
            entry = float(broker_pos.get('avg_entry_price') or bracket.get('filled_avg_price') or 0.0)
        except Exception:
            return False
        if qty <= 0 or entry <= 0:
            return False

        side = 'LONG' if str(broker_pos.get('side') or '').lower() == 'long' else 'SHORT'
        tp = None
        sl = None
        for leg in bracket.get('legs') or []:
            if leg.get('type') == 'limit' and leg.get('limit_price') is not None:
                tp = float(leg.get('limit_price'))
            elif leg.get('type') == 'stop' and leg.get('stop_price') is not None:
                sl = float(leg.get('stop_price'))
        if tp is None or sl is None:
            fixed = FIXED_BRACKETS.get(sym) if isinstance(FIXED_BRACKETS, dict) else None
            cfg = fixed if isinstance(fixed, dict) else TICKER_CFG.get(sym, {})
            sl_pct = float(cfg.get('sl', SL_PCT))
            tp_pct = float(cfg.get('tp', TP_PCT))
            if side == 'LONG':
                sl = entry * (1 - sl_pct)
                tp = entry * (1 + tp_pct)
            else:
                sl = entry * (1 + sl_pct)
                tp = entry * (1 - tp_pct)

        created_at = bracket.get('created_at') or bracket.get('submitted_at')
        entry_ts = int(time.time())
        if created_at:
            try:
                entry_ts = int(datetime.fromisoformat(str(created_at).replace('Z', '+00:00')).timestamp())
            except Exception:
                pass

        client_order_id = bracket.get('client_order_id')
        trade_id = f'{sym}-{entry_ts}-{client_order_id or "broker-recovered"}'
        bracket_policy = {
            'sl_pct': round(abs(float(sl) - entry) / entry, 5),
            'tp_pct': round(abs(float(tp) - entry) / entry, 5),
            'notes': ['broker_orphan_recovered_on_boot', 'strict_tp_sl'],
        }
        recovered_pos = {
            'ticker': sym,
            'trade_id': trade_id,
            'side': side,
            'entry': round(entry, 4),
            'sl': round(float(sl), 4),
            'tp': round(float(tp), 4),
            'bracket_policy': bracket_policy,
            'qty': qty,
            'alloc': round(entry * qty, 2),
            'trade_size_pct': TRADE_SIZE_PCT,
            'entry_ts': entry_ts,
            'session_phase': 'broker_recovered',
            'setup_type': 'broker_recovered',
            'path_failure_entry_btc_price': self.engine.get_last_price('BTC/USD') or 0.0,
            'reasons': ['broker_orphan_recovered_on_boot'],
            'conviction': 'BROKER_RECOVERED',
            'alpaca_order_id': bracket.get('id'),
            'client_order_id': client_order_id,
            'alpaca_status': bracket.get('status'),
            'alpaca_filled_entry': True,
            'entry_fill_price': round(entry, 4),
            'entry_filled_ts': entry_ts,
            'fill_attribution': [],
            'latency_chain': {'broker_recovered_ms': int(time.time() * 1000)},
            'forensics': {
                'source': 'broker_orphan_recovered_on_boot',
                'broker_position': broker_pos,
                'broker_order': {
                    'id': bracket.get('id'),
                    'client_order_id': client_order_id,
                    'status': bracket.get('status'),
                    'filled_qty': bracket.get('filled_qty'),
                    'qty': bracket.get('qty'),
                },
                'strategy': {'config_hash': STRATEGY_CONFIG_HASH},
            },
            'entry_thesis': {
                'ticker': sym,
                'side': side,
                'setup_type': 'broker_recovered',
                'session_phase': 'broker_recovered',
                'score': None,
                'conviction': 'BROKER_RECOVERED',
                'reasons': ['broker_orphan_recovered_on_boot'],
            },
            'strategy_config_hash': STRATEGY_CONFIG_HASH,
            'strategy_config_snapshot': _config_snapshot(),
            '_best_price': round(entry, 4),
            '_worst_price': round(entry, 4),
            '_path': {
                'first_green_ts': None,
                'first_red_ts': None,
                'time_to_mfe_sec': 0,
                'time_to_mae_sec': 0,
                'checks': {},
            },
            '_entry_timing': {
                'signal_price': round(entry, 4),
                'windows_sec': {
                    str(sec): {'best_entry_px': round(entry, 4), 'worst_entry_px': round(entry, 4)}
                    for sec in (15, 30, 60, 120)
                },
            },
        }
        with self.lock:
            if sym in self.state.get('positions', {}):
                return True
            self.state.setdefault('positions', {})[sym] = recovered_pos
            self.state.setdefault('pending_entries', {}).pop(sym, None)
            self._critical_save('broker_orphan_bracket_recovered_on_boot', [sym])
        self._audit_event('broker_orphan_bracket_recovered_on_boot', sym, {
            'order_id': bracket.get('id'),
            'client_order_id': client_order_id,
            'side': side,
            'entry': round(entry, 4),
            'qty': qty,
            'sl': round(float(sl), 4),
            'tp': round(float(tp), 4),
            'bracket_policy': bracket_policy,
        })
        log.warning(f'boot: recovered orphan Alpaca bracket position {sym} '
                    f'{side} qty={qty} entry={entry:.4f}')
        return True

    def _reconcile_on_boot(self):
        """On boot, sanity-check internal vs. Alpaca state. Two cases:
          (1) internal HAS pos, Alpaca FLAT  → drop internal (closed externally)
          (2) Alpaca HAS pos,   internal FLAT → flatten on Alpaca (orphan)

        Case (2) catches positions that didn't process their session_end
        close before market closed (e.g. weekend rollover from 4/24 → 4/27),
        so a stale Alpaca position can't collide with a fresh ws_scalp
        entry on the next session.
        """
        try:
            alpaca_positions = {p.get('symbol'): p for p in self.trader.list_positions()}
        except Exception as e:
            log.error(f'boot reconcile list_positions failed: {e}')
            return
        recovered = []
        dropped = []
        with self.lock:
            missing = [
                (tkr, dict(pos)) for tkr, pos in self.state['positions'].items()
                if tkr not in alpaca_positions
            ]
        for tkr, pos in missing:
            expected_side = 'sell' if pos.get('side') == 'LONG' else 'buy'
            fill = self._latest_broker_fill(
                tkr,
                expected_side,
                int(pos.get('entry_ts') or time.time()),
            )
            if isinstance(fill, dict) and fill.get('filled_avg_price'):
                with self.lock:
                    if tkr in self.state['positions']:
                        self.state['positions'][tkr]['broker_close'] = {
                            'reason': 'external_broker_exit',
                            'submitted_at': int(pos.get('entry_ts') or time.time()),
                            'verified_flat_at': int(time.time()),
                            'expected_side': expected_side,
                            'response_order_id': fill.get('order_id'),
                            'response_status': 'filled',
                            'fill': fill,
                        }
                        self._critical_save('external_broker_exit_recorded', [tkr])
                self._close_position(tkr, float(fill['filled_avg_price']), 'external_broker_exit')
                recovered.append(tkr)
            else:
                dropped.append(tkr)
        # Case 1: drop internal positions Alpaca doesn't know about
        with self.lock:
            for tkr in list(self.state['positions'].keys()):
                if tkr not in alpaca_positions:
                    log.warning(f'boot: internal has {tkr} but Alpaca is flat — dropping')
                    self.state['positions'].pop(tkr, None)
            internal_keys = set(self.state['positions'].keys())
            self._save()
        with self.lock:
            internal_snapshot = dict(self.state.get('positions') or {})
        mismatches = self._position_mismatches(internal_snapshot, alpaca_positions)
        if mismatches:
            log.error(f'BOOT_RECONCILE_MISMATCH: {mismatches}')
            self._set_broker_exposure_block(
                [m['symbol'] for m in mismatches],
                'alpaca_internal_position_mismatch_on_boot',
            )
            self._audit_event('boot_position_mismatch', data={'mismatches': mismatches})
            return
        # Case 2: recover current live brackets, then flatten anything still orphaned.
        orphans = [s for s in alpaca_positions.keys() if s not in internal_keys]
        recovered_orphans = []
        for sym in list(orphans):
            if self._recover_orphan_bracket_position_on_boot(sym, alpaca_positions.get(sym) or {}):
                recovered_orphans.append(sym)
        if recovered_orphans:
            with self.lock:
                internal_keys = set(self.state.get('positions', {}).keys())
            orphans = [s for s in alpaca_positions.keys() if s not in internal_keys]
        if orphans:
            log.warning(f'boot: Alpaca has orphan positions {orphans} — '
                        f'cancelling orders and flattening')
            for sym in orphans:
                try:
                    self._cancel_symbol_orders(sym)
                except Exception as e:
                    log.error(f'boot: scoped cancel({sym}) failed: {e}')
                try:
                    self.trader.close_position(sym)
                    log.info(f'boot: flattened orphan Alpaca position {sym}')
                except Exception as e:
                    log.error(f'boot: close_position({sym}) failed: {e}')
            remaining = self._verify_symbols_flat(orphans)
            for sym in sorted(remaining):
                try:
                    self._cancel_symbol_orders(sym)
                    self.trader.close_position(sym)
                    log.warning(f'boot: retry close submitted for orphan {sym}')
                except Exception as e:
                    log.error(f'boot: retry close_position({sym}) failed: {e}')
            remaining = self._verify_symbols_flat(orphans, attempts=2, delay_sec=1.0)
            if remaining:
                log.error(f'BOOT_RECONCILE_NOT_FLAT: Alpaca still has orphan positions '
                          f'{sorted(remaining)} after retries')
                self._set_broker_exposure_block(
                    remaining,
                    'alpaca_orphan_positions_not_flat_after_boot_reconcile',
                )
            else:
                self._clear_broker_exposure_block()
        else:
            self._clear_broker_exposure_block()
        if recovered or dropped:
            self._audit_event('boot_internal_flat_reconcile', data={
                'recovered': recovered,
                'dropped': dropped,
            })
        log.info(f'boot reconcile: alpaca positions={list(alpaca_positions.keys())} '
                 f'orphans_flattened={orphans} recovered={recovered} '
                 f'bracket_recovered={recovered_orphans} dropped={dropped}')

    def _decision_quality(self, trade: dict) -> dict:
        tags = []
        label = 'valid_decision_bad_outcome' if trade.get('pnl', 0) < 0 else 'profitable_or_flat'
        f = trade.get('forensics') or {}
        eq = f.get('entry_quality') or {}
        btc = f.get('btc') or {}
        thesis = trade.get('entry_thesis') or {}
        signal_quality = thesis.get('signal_quality') or {}
        comps = signal_quality.get('score_components') or {}
        miner = thesis.get('miner_basket') or {}
        lead_lag = thesis.get('lead_lag') or {}
        setup = thesis.get('setup_type') or trade.get('setup_type')
        exit_ctx = trade.get('exit_decision_context') or {}
        if eq.get('quote_stale'):
            tags.append('quote_stale')
            label = 'bad_execution_context'
        if eq.get('spread_pct') is not None and eq.get('spread_pct') > 0.08:
            tags.append('wide_spread')
            label = 'bad_execution_context'
        if eq.get('score') is not None and eq.get('score') < 70:
            tags.append(f"entry_quality_score_{eq.get('score')}")
            label = 'bad_execution_context'
        if btc.get('conflict'):
            tags.append('btc_conflict')
            label = 'bad_entry_context'
        if signal_quality.get('btc_conflict'):
            tags.append('signal_btc_conflict')
            label = 'bad_entry_context'
        if comps.get('btc_chop_penalty') is not None:
            tags.append('btc_chop_penalty_present')
            label = 'bad_entry_context'
        if comps.get('realized_vol_chop') is not None:
            tags.append('realized_vol_chop_present')
            label = 'bad_entry_context'
        if setup == 'flow_exhaustion_fade' and signal_quality.get('flow_fade_confirmed') is False:
            tags.append('flow_fade_unconfirmed')
            label = 'bad_entry_context'
        if miner.get('state') in ('opposed', 'mixed_conflict'):
            tags.append(f"miner_basket_{miner.get('state')}")
            label = 'bad_entry_context'
        if isinstance(lead_lag, dict) and (lead_lag.get('score') or 0) < 0:
            tags.append('btc_lead_lag_chase')
            label = 'bad_entry_context'
        if trade.get('mfe_pct') is not None and trade.get('mfe_pct') >= 0.5 and trade.get('pnl', 0) < 0:
            tags.append('profit_reversal')
            label = 'bad_exit_or_profit_protection'
        if exit_ctx.get('kind'):
            tags.append(f"exit_{exit_ctx.get('kind')}")
        if trade.get('reason') == 'cond_time_stop':
            tags.append('exit_rule_review')
        return {'label': label, 'tags': tags}

    def _trade_replay(self, tkr: str, pos: dict, trade: dict) -> dict:
        return {
            'ticker': tkr,
            'side': trade.get('side'),
            'opened_at': trade.get('opened_at'),
            'closed_at': trade.get('closed_at'),
            'entry': trade.get('entry'),
            'exit': trade.get('exit'),
            'pnl': trade.get('pnl'),
            'reason': trade.get('reason'),
            'entry_forensics': pos.get('forensics'),
            'path': pos.get('_path'),
            'mfe_pct': trade.get('mfe_pct'),
            'mae_pct': trade.get('mae_pct'),
            'broker_close': trade.get('broker_close'),
            'latency_chain': trade.get('latency_chain'),
            'entry_timing': trade.get('entry_timing'),
            'exit_latency_attribution': trade.get('exit_latency_attribution'),
            'strategy_config_hash': trade.get('strategy_config_hash'),
        }

    def _finalize_entry_timing(self, pos: dict) -> dict:
        timing = json.loads(json.dumps(pos.get('_entry_timing') or {}))
        side = pos.get('side')
        entry = float(pos.get('entry') or timing.get('signal_price') or 0)
        for sec, row in (timing.get('windows_sec') or {}).items():
            best_px = row.get('best_entry_px')
            worst_px = row.get('worst_entry_px')
            if not entry:
                continue
            try:
                best_px_f = float(best_px)
                worst_px_f = float(worst_px)
            except Exception:
                continue
            if side == 'LONG':
                saved_vs_actual = (entry - best_px_f) / entry * 100
                risk_vs_actual = (worst_px_f - entry) / entry * 100
            else:
                saved_vs_actual = (best_px_f - entry) / entry * 100
                risk_vs_actual = (entry - worst_px_f) / entry * 100
            row['actual_entry_px'] = round(entry, 4)
            row['could_have_improved_entry_pct'] = round(max(0.0, saved_vs_actual), 4)
            row['could_have_worsened_entry_pct'] = round(max(0.0, risk_vs_actual), 4)
            row['entry_timing_grade'] = (
                'excellent' if saved_vs_actual <= 0.03 else
                'acceptable' if saved_vs_actual <= 0.08 else
                'early_or_late_costly'
            )
        timing['deduction'] = (
            'Compares the actual entry against best/worst observed trade price after entry. '
            'This separates bad idea quality from early/late execution timing.'
        )
        return timing

    def _maybe_pause_for_execution_risk(self, trade: dict, setup_key: str):
        if not EXECUTION_RISK_PAUSE_ENABLED or EXECUTION_RISK_PAUSE_COUNT <= 0:
            return
        def adverse_slip(row: dict) -> Optional[float]:
            vals = []
            if row.get('entry_slippage_pct') is not None:
                try:
                    vals.append(float(row.get('entry_slippage_pct')))
                except Exception:
                    pass
            for attr in row.get('fill_attribution') or []:
                if attr.get('stage') in ('entry_fill', 'submit_response', 'submit_response_recovered'):
                    try:
                        vals.append(float(attr.get('adverse_fill_vs_mid_pct')))
                    except Exception:
                        pass
            return max(vals) if vals else None

        with self.lock:
            recent = list(self.state.get('trades', []))[-EXECUTION_RISK_PAUSE_LOOKBACK_TRADES:]
        seen_keys = set()
        deduped = []
        for row in recent + [trade]:
            key = row.get('trade_id') or (
                row.get('ticker'), row.get('side'), row.get('opened_at'), row.get('closed_at')
            )
            if key in seen_keys:
                continue
            seen_keys.add(key)
            deduped.append(row)
        hits = [
            row for row in deduped
            if self._setup_key({
                'ticker': row.get('ticker'),
                'side': row.get('side'),
                'forensics': row.get('forensics') or {},
            }) == setup_key
            and (adverse_slip(row) is not None
                 and adverse_slip(row) >= EXECUTION_RISK_PAUSE_ADVERSE_FILL_PCT)
        ]
        if len(hits) < EXECUTION_RISK_PAUSE_COUNT:
            return
        until = int(time.time()) + EXECUTION_RISK_PAUSE_MIN * 60
        self.state.setdefault('setup_pauses', {})[setup_key] = {
            'created_at': int(time.time()),
            'until': until,
            'reason': f'execution_risk_{len(hits)}_bad_fills',
            'bad_fill_threshold_pct': EXECUTION_RISK_PAUSE_ADVERSE_FILL_PCT,
            'lookback_trades': EXECUTION_RISK_PAUSE_LOOKBACK_TRADES,
            'mode': 'cooldown_no_size_change',
        }
        self._audit_event('setup_paused_execution_risk', trade.get('ticker'), {
            'setup_key': setup_key,
            'until': until,
            'hit_count': len(hits),
            'threshold_pct': EXECUTION_RISK_PAUSE_ADVERSE_FILL_PCT,
        })

    def _close_position(self, tkr: str, exit_price: float, reason: str):
        with self.lock:
            pos = self.state['positions'].pop(tkr, None)
            if not pos:
                return
            self._start_exit_tick_capture(tkr, pos, exit_price, reason)
            broker_close = pos.get('broker_close') or {}
            broker_fill = broker_close.get('fill') or {}
            if isinstance(broker_fill, dict) and broker_fill.get('filled_avg_price'):
                try:
                    exit_price = float(broker_fill['filled_avg_price'])
                except Exception:
                    pass
            side  = pos['side']
            entry = pos['entry']
            qty   = pos['qty']
            if side == 'LONG':
                pnl = (exit_price - entry) * qty
            else:
                pnl = (entry - exit_price) * qty
            pnl = round(pnl, 2)
            estimated_costs = self._estimated_transaction_costs(side, qty, entry, exit_price)
            net_pnl_after_estimated_costs = None
            if estimated_costs.get('estimated_total') is not None:
                net_pnl_after_estimated_costs = round(pnl - float(estimated_costs['estimated_total']), 2)
            self.state['balance'] = round(self.state['balance'] + pnl, 2)
            closed_at = int(time.time())
            latency_chain = dict(pos.get('latency_chain') or {})
            latency_chain.setdefault('exit_trigger_ms', int(time.time() * 1000))
            latency_chain['exit_local_closed_ms'] = int(time.time() * 1000)
            pos['latency_chain'] = latency_chain
            exit_latency_attribution = self._latency_attribution_row(
                tkr, side, pos.get('trade_id'), pos.get('client_order_id'),
                (broker_fill.get('order_id') if isinstance(broker_fill, dict) else None)
                or (broker_close.get('response_order_id') if isinstance(broker_close, dict) else None),
                latency_chain,
                f'exit_closed:{reason}',
            )
            if not pos.get('exit_execution_intent') or not pos.get('exit_execution_result'):
                fallback_status = (
                    'broker_filled' if broker_fill
                    else ('local_strict_triggered' if self.trader is not None and STEP_PARITY_LIVE_MODE else 'simulated_closed')
                )
                exit_action_plan, exit_execution_intent, exit_execution_result = self._build_exit_execution_pair(
                    tkr,
                    pos,
                    exit_price,
                    reason,
                    ts=closed_at,
                    status=fallback_status,
                    broker_order_id=(
                        (broker_fill.get('order_id') if isinstance(broker_fill, dict) else None)
                        or (broker_close.get('response_order_id') if isinstance(broker_close, dict) else None)
                    ),
                    broker_status=(broker_close.get('response_status') if isinstance(broker_close, dict) else None),
                    raw_response=broker_fill if isinstance(broker_fill, dict) else None,
                )
                pos['exit_action_plan'] = exit_action_plan
                pos['exit_execution_intent'] = exit_execution_intent
                pos['exit_execution_result'] = exit_execution_result
                self._audit_event('exit_intent_created', tkr, {
                    'trade_id': pos.get('trade_id'),
                    'execution_intent_id': exit_execution_intent.get('execution_intent_id'),
                    'semantic_execution_intent_hash': exit_execution_intent.get('semantic_execution_intent_hash'),
                    'execution_intent': exit_execution_intent,
                })
                self._audit_event('exit_execution_result', tkr, {
                    'trade_id': pos.get('trade_id'),
                    'execution_intent_id': exit_execution_intent.get('execution_intent_id'),
                    'execution_result_id': exit_execution_result.get('execution_result_id'),
                    'execution_result': exit_execution_result,
                })
            else:
                exit_execution_intent = pos.get('exit_execution_intent')
                previous_result = pos.get('exit_execution_result') if isinstance(pos.get('exit_execution_result'), dict) else {}
                final_status = previous_result.get('status') or (
                    'broker_filled' if broker_fill
                    else ('local_strict_triggered' if self.trader is not None and STEP_PARITY_LIVE_MODE else 'simulated_closed')
                )
                exit_execution_result = execution_intent_engine.result_from_intent(
                    exit_execution_intent,
                    final_status,
                    ok=True,
                    ts=closed_at,
                    broker_order_id=(
                        (broker_fill.get('order_id') if isinstance(broker_fill, dict) else None)
                        or (broker_close.get('response_order_id') if isinstance(broker_close, dict) else None)
                    ),
                    broker_status=(broker_close.get('response_status') if isinstance(broker_close, dict) else None),
                    fill_price=round(float(exit_price), 4),
                    filled_qty=qty,
                    reason=reason,
                    raw_response=broker_fill if isinstance(broker_fill, dict) else None,
                    latency=dict(latency_chain),
                    metadata=(previous_result.get('metadata') if isinstance(previous_result, dict) else None),
                )
                pos['exit_execution_result'] = exit_execution_result
                self._audit_event('exit_execution_result', tkr, {
                    'trade_id': pos.get('trade_id'),
                    'execution_intent_id': exit_execution_intent.get('execution_intent_id'),
                    'execution_result_id': exit_execution_result.get('execution_result_id'),
                    'execution_result': exit_execution_result,
                })
            bars_held = max(0, closed_at - pos['entry_ts']) // 60
            # MFE / MAE from per-second trackers
            best_px  = pos.get('_best_price',  entry)
            worst_px = pos.get('_worst_price', entry)
            if side == 'LONG':
                mfe     = round(best_px  - entry, 4)
                mae     = round(entry    - worst_px, 4)
            else:
                mfe     = round(entry    - best_px, 4)
                mae     = round(worst_px - entry, 4)
            mfe_pct = round(mfe / entry * 100, 3) if entry else None
            mae_pct = round(mae / entry * 100, 3) if entry else None
            entry_fill = pos.get('entry_fill_price')
            fill_slip = None
            fill_slip_pct = None
            if entry_fill is not None:
                fill_slip = (float(entry_fill) - entry) if side == 'LONG' else (entry - float(entry_fill))
                fill_slip = round(fill_slip, 4)
                fill_slip_pct = round(fill_slip / entry * 100, 3) if entry else None
            seconds_to_fill = None
            if pos.get('entry_filled_ts') and pos.get('entry_ts'):
                seconds_to_fill = max(0, int(pos['entry_filled_ts']) - int(pos['entry_ts']))
            entry_timing = self._finalize_entry_timing(pos)
            trade = {
                'ticker':     tkr,
                'trade_id':   pos.get('trade_id'),
                'side':       side,
                'entry':      entry,
                'sl':         pos['sl'],
                'tp':         pos['tp'],
                'exit':       round(exit_price, 4),
                'qty':        qty,
                'alloc':      pos['alloc'],
                'trade_size_pct': pos.get('trade_size_pct'),
                'pnl':        pnl,
                'estimated_transaction_costs': estimated_costs,
                'net_pnl_after_estimated_costs': net_pnl_after_estimated_costs,
                'result':     'WIN' if pnl > 0 else ('LOSS' if pnl < 0 else 'FLAT'),
                'reason':     reason,
                'opened_at':  pos['entry_ts'],
                'closed_at':  closed_at,
                'conviction': pos.get('conviction'),
                'reasons':    pos.get('reasons', []),
                'forensics':  pos.get('forensics'),
                'entry_thesis': pos.get('entry_thesis'),
                'entry_quality_tier': pos.get('entry_quality_tier') or (pos.get('entry_thesis') or {}).get('entry_quality_tier'),
                'entry_quality_score': pos.get('entry_quality_score') or (pos.get('entry_thesis') or {}).get('entry_quality_score'),
                'setup_grade_at_entry': pos.get('setup_grade_at_entry') or (pos.get('entry_thesis') or {}).get('setup_grade_at_entry'),
                'path':       pos.get('_path'),
                'entry_fill_price': entry_fill,
                'entry_slippage': fill_slip,
                'entry_slippage_pct': fill_slip_pct,
                'seconds_to_fill': seconds_to_fill,
                'latency_chain': latency_chain,
                'latency_attribution': pos.get('latency_attribution') or (pos.get('forensics') or {}).get('latency_attribution'),
                'entry_timing': entry_timing,
                'exit_latency_attribution': exit_latency_attribution,
                'exit_time_to_submit_ms': exit_latency_attribution.get('exit_time_to_submit_ms'),
                'exit_broker_submit_ms': exit_latency_attribution.get('exit_broker_submit_ms'),
                'exit_time_to_fill_ms': exit_latency_attribution.get('exit_time_to_fill_ms'),
                'exit_time_to_flat_ms': exit_latency_attribution.get('exit_time_to_flat_ms'),
                'strategy_config_hash': pos.get('strategy_config_hash') or STRATEGY_CONFIG_HASH,
                'exit_decision_context': pos.get('exit_decision_context'),
                'entry_action_plan': pos.get('entry_action_plan'),
                'exit_action_plan': pos.get('exit_action_plan'),
                'entry_execution_intent': pos.get('entry_execution_intent'),
                'entry_execution_result': pos.get('entry_execution_result'),
                'exit_execution_intent': pos.get('exit_execution_intent'),
                'exit_execution_result': pos.get('exit_execution_result'),
                'bracket_policy': pos.get('bracket_policy'),
                'broker_entry_order_id': pos.get('alpaca_order_id'),
                'broker_exit_order_id': broker_fill.get('order_id') if isinstance(broker_fill, dict) else None,
                'broker_exit_fill_price': broker_fill.get('filled_avg_price') if isinstance(broker_fill, dict) else None,
                'broker_exit_filled_at': broker_fill.get('filled_at') if isinstance(broker_fill, dict) else None,
                'broker_close': broker_close or None,
                'fill_attribution': pos.get('fill_attribution') or [],
                # Forensic snapshot (see _on_signal). Empty dicts if the signal
                # predates this feature — retrospective script fills those in.
                'indicators':     pos.get('indicators'),
                'btc_indicators': pos.get('btc_indicators'),
                'entry_score':    pos.get('entry_score'),
                'entry_rvol':     pos.get('entry_rvol'),
                # Tier 1 session context
                'concurrent_tickers':    pos.get('concurrent_tickers'),
                'session_elapsed_min':   pos.get('session_elapsed_min'),
                'session_high_at_entry': pos.get('session_high_at_entry'),
                'session_low_at_entry':  pos.get('session_low_at_entry'),
                'gap_pct':               pos.get('gap_pct'),
                'market_tape_at_entry':  pos.get('market_tape_at_entry'),
                'spy_above_vwap':        pos.get('spy_above_vwap'),
                'spy_day_pct':           pos.get('spy_day_pct'),
                'qqq_above_vwap':        pos.get('qqq_above_vwap'),
                'qqq_day_pct':           pos.get('qqq_day_pct'),
                'iwm_above_vwap':        pos.get('iwm_above_vwap'),
                'iwm_day_pct':           pos.get('iwm_day_pct'),
                'btc_proxy_basket_at_entry': pos.get('btc_proxy_basket_at_entry'),
                'market_microstructure_at_entry': pos.get('market_microstructure_at_entry'),
                'gap_context_at_entry': pos.get('gap_context_at_entry'),
                'liquidity_impact_at_entry': pos.get('liquidity_impact_at_entry'),
                'short_borrow_snapshot': pos.get('short_borrow_snapshot'),
                'catalyst_context_at_entry': pos.get('catalyst_context_at_entry'),
                # MAE / MFE
                'mfe':      mfe,
                'mfe_pct':  mfe_pct,
                'mae':      mae,
                'mae_pct':  mae_pct,
                'bars_held': bars_held,
            }
            self._append_latency_attribution(exit_latency_attribution)
            trade['decision_quality'] = self._decision_quality(trade)
            trade['replay'] = self._trade_replay(tkr, pos, trade)
            self._append_rule_dry_run_event('trade_closed_outcome', {
                'created_at': closed_at,
                'trade_id': trade.get('trade_id'),
                'ticker': tkr,
                'side': side,
                'decision': 'closed',
                'setup_type': (pos.get('entry_thesis') or {}).get('setup_type'),
                'result': trade.get('result'),
                'pnl': pnl,
                'net_pnl_after_estimated_costs': net_pnl_after_estimated_costs,
                'reason': reason,
                'near_miss_rules': pos.get('entry_rule_near_misses') or [],
                'entry_quality_tier': pos.get('setup_grade_at_entry') or {},
                'forensics': {
                    'market_microstructure': pos.get('market_microstructure_at_entry') or {},
                    'gap_context': pos.get('gap_context_at_entry') or {},
                    'liquidity_impact': pos.get('liquidity_impact_at_entry') or {},
                    'short_borrow_snapshot': pos.get('short_borrow_snapshot') or {},
                    'btc_proxy_basket': pos.get('btc_proxy_basket_at_entry') or {},
                    'catalyst_context': pos.get('catalyst_context_at_entry') or {},
                },
                'mode': 'passive_rule_dry_run',
            })
            self.state['trades'].append(trade)
            self._append_trade_corpus(trade)
            if live_decision_ledger is not None:
                try:
                    live_decision_ledger.append(live_decision_ledger.from_closed_trade(trade))
                except Exception:
                    pass
            if canonical_decision_packet is not None:
                try:
                    packet = canonical_decision_packet.from_closed_trade(trade)
                    trade['canonical_exit_packet_id'] = packet.get('packet_id')
                    canonical_decision_packet.append_packet(packet)
                except Exception as packet_err:
                    log.warning(f'canonical exit packet write failed: {packet_err}')
            try:
                from event_store import record_event
                record_event('trade_closed', trade, symbol=tkr,
                             trade_id=trade.get('trade_id'), ts=trade.get('closed_at'))
            except Exception:
                pass
            setup_key_for_trade = self._setup_key({
                'ticker': tkr,
                'side': side,
                'forensics': pos.get('forensics') or {},
            })
            self._maybe_pause_for_execution_risk(trade, setup_key_for_trade)
            if trade.get('result') == 'LOSS' and SETUP_LOSS_PAUSE_COUNT > 0:
                recent_losses = [
                    tr for tr in self.state['trades'][-12:]
                    if tr.get('result') == 'LOSS'
                    and self._setup_key({
                        'ticker': tr.get('ticker'),
                        'side': tr.get('side'),
                        'forensics': tr.get('forensics') or {},
                    }) == setup_key_for_trade
                ]
                if len(recent_losses) >= SETUP_LOSS_PAUSE_COUNT:
                    until = int(time.time()) + SETUP_LOSS_PAUSE_MIN * 60
                    self.state.setdefault('setup_pauses', {})[setup_key_for_trade] = {
                        'created_at': int(time.time()),
                        'until': until,
                        'reason': f'{len(recent_losses)}_recent_losses',
                        'last_trade_closed_at': closed_at,
                    }
                    self._audit_event('setup_paused', tkr, {
                        'setup_key': setup_key_for_trade,
                        'until': until,
                        'loss_count': len(recent_losses),
                    })
            self._critical_save('position_closed_committed', [tkr])
        # Clear engine cooldown so the next qualifying signal on this ticker
        # can re-enter immediately (intent: many trades per day).
        try:
            self.engine.clear_signal_cooldown(tkr)
        except Exception:
            pass
        self._audit_event('position_closed', tkr, {
            'trade_id': trade.get('trade_id'),
            'side': side,
            'reason': reason,
            'exit': round(float(exit_price), 4),
            'pnl': pnl,
            'broker_exit_order_id': trade.get('broker_exit_order_id'),
            'exit_time_to_submit_ms': trade.get('exit_time_to_submit_ms'),
            'exit_time_to_fill_ms': trade.get('exit_time_to_fill_ms'),
            'exit_time_to_flat_ms': trade.get('exit_time_to_flat_ms'),
            'exit_execution_intent_id': (
                (trade.get('exit_execution_intent') or {}).get('execution_intent_id')
                if isinstance(trade.get('exit_execution_intent'), dict) else None
            ),
            'exit_execution_result_id': (
                (trade.get('exit_execution_result') or {}).get('execution_result_id')
                if isinstance(trade.get('exit_execution_result'), dict) else None
            ),
        })
        log.info(f'CLOSE {side} {tkr} @ {exit_price} pnl=${pnl:+.2f} '
                 f'reason={reason} balance=${self.state["balance"]:.2f}')


# ── module singleton ─────────────────────────────────────────────────────
_trader: Optional[MockTrader] = None

def get_trader(engine=None, state_path: str = None) -> MockTrader:
    global _trader
    if _trader is None:
        if engine is None or state_path is None:
            raise RuntimeError('First call must pass engine and state_path')
        _trader = MockTrader(engine, state_path)
    return _trader
