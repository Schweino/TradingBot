"""
Replay the current ws_scalp engine against Alpaca historical market data.

This is intentionally a harness around the live signal code, not a fork of the
strategy. If ws_scalp.detect_signal or trading_config.json changes, rerun this
script to test the new logic against the same cached historical data.

Default window: last 30 calendar days ending yesterday.
Output:
  postmortem/backtests/engine_replay_<start>_<end>_trades.csv
  postmortem/backtests/engine_replay_<start>_<end>_summary.json
"""
from __future__ import annotations

from output_paths import output_path

import argparse
import csv
import gzip
import hashlib
import json
import os
import platform
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from datetime import date, datetime, time as dt_time, timedelta, timezone
from typing import Any, Iterable, Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from data_fetch import alpaca_crypto_bars, alpaca_stock_quotes, alpaca_stock_trades
import scoring_profiles
import ws_scalp
import step2_decision_parity
import step2_latency_model
import step2_parity_contract
import step2_execution_contract
import execution_action_engine
import execution_adapters
import execution_intent_engine
import worker_policy
from bracket_rounding import round_exit_brackets


ET = ZoneInfo('America/New_York')
CT = ZoneInfo('America/Chicago')
BTC_SYMBOL = ws_scalp.BTC_SYMBOL
DEFAULT_CACHE_DIR = output_path('data_cache', 'alpaca_engine_replay')
DEFAULT_PREPARED_DIR = output_path('data_cache', 'alpaca_engine_replay_tapes')
DEFAULT_OUT_DIR = output_path('postmortem', 'backtests')


class ReplayClock:
    def __init__(self):
        self.now = 0.0

    def time(self) -> float:
        return self.now


@dataclass
class Position:
    ticker: str
    side: str
    entry_ts: int
    entry_price: float
    qty: float
    sl: float
    tp: float
    alloc: float
    signal: dict
    best_price: float
    worst_price: float
    active_ts: int = 0


@dataclass
class ReplayStats:
    fetched: Counter = field(default_factory=Counter)
    cache_hits: Counter = field(default_factory=Counter)
    skipped_signals: Counter = field(default_factory=Counter)
    timing: Counter = field(default_factory=Counter)
    emitted_signals: int = 0
    seconds: int = 0


def _read_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return default


TRADING_CONFIG = _read_json(os.path.join(HERE, 'trading_config.json'), {}) or {}
STEP2_PARITY_CONTRACT = step2_parity_contract.contract(TRADING_CONFIG)
STEP2_EXECUTION_CONTRACT = step2_execution_contract.execution_contract(TRADING_CONFIG)
TICKERS = TRADING_CONFIG.get('tickers', ['CLSK', 'MARA', 'RIOT'])
TICKER_CFG = TRADING_CONFIG.get('ticker_cfg', {})
SESSION = TRADING_CONFIG.get('session', {}) or {}
ADAPTIVE = TRADING_CONFIG.get('adaptive_management', {}) or {}
TRADE_SIZE_PCT = float(TRADING_CONFIG.get('trade_size_pct', 0.25))
MIN_CONVICTION = tuple(TRADING_CONFIG.get('min_conviction', ['MEDIUM', 'HIGH']))
CONDITIONAL_TIME_STOP_ENABLED = bool(STEP2_PARITY_CONTRACT.get('conditional_time_stop_enabled'))
COND_STOP_MIN = float(STEP2_PARITY_CONTRACT.get('conditional_stop_min') or 0)
START_BALANCE = 1000.0
SESSION_START_H = int(SESSION.get('start_hour', 8))
SESSION_START_M = int(SESSION.get('start_minute', 30))
ENTRY_CUTOFF_H = int(SESSION.get('entry_cutoff_hour', 14))
ENTRY_CUTOFF_M = int(SESSION.get('entry_cutoff_minute', 45))
FLATTEN_H = int(SESSION.get('flatten_hour', 14))
FLATTEN_M = int(SESSION.get('flatten_minute', 55))


def _parse_day(value: str) -> date:
    return datetime.strptime(value, '%Y-%m-%d').date()


def _default_dates() -> tuple[date, date]:
    # End yesterday so the historical-data window is complete.
    end = datetime.now(CT).date() - timedelta(days=1)
    start = end - timedelta(days=29)
    return start, end


def _market_days(start: date, end: date) -> list[date]:
    days = []
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            days.append(cur)
        cur += timedelta(days=1)
    return days


def _sample_day_set(value: str | None) -> Optional[set[date]]:
    if not value:
        return None
    days = {
        _parse_day(part.strip())
        for part in str(value).split(',')
        if part.strip()
    }
    return days or None


def _selected_market_days(args, start: date, end: date) -> list[date]:
    days = _market_days(start, end)
    sample = _sample_day_set(getattr(args, 'sample_days', None))
    if sample:
        days = [day for day in days if day in sample]
    if not days:
        raise SystemExit('No market days selected for replay.')
    return days


def _session_bounds_utc(day: date) -> tuple[str, str, int, int]:
    start_ct = datetime.combine(day, dt_time(SESSION_START_H, SESSION_START_M), tzinfo=CT)
    end_ct = datetime.combine(day, dt_time(15, 0), tzinfo=CT)
    start_utc = start_ct.astimezone(timezone.utc)
    end_utc = end_ct.astimezone(timezone.utc)
    return (
        start_utc.strftime('%Y-%m-%dT%H:%M:%SZ'),
        end_utc.strftime('%Y-%m-%dT%H:%M:%SZ'),
        int(start_utc.timestamp()),
        int(end_utc.timestamp()),
    )


def _entry_cutoff_ts(day: date) -> int:
    dt = datetime.combine(day, dt_time(ENTRY_CUTOFF_H, ENTRY_CUTOFF_M), tzinfo=CT)
    return int(dt.astimezone(timezone.utc).timestamp())


def _flatten_ts(day: date) -> int:
    dt = datetime.combine(day, dt_time(FLATTEN_H, FLATTEN_M), tzinfo=CT)
    return int(dt.astimezone(timezone.utc).timestamp())


def _iso_ct(ts: int) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).astimezone(CT).strftime('%Y-%m-%d %H:%M:%S')


def _json_gz_read(path: str) -> Optional[list[dict]]:
    if not os.path.exists(path):
        return None
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        return json.load(f)


def _json_gz_write(path: str, rows: list[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with gzip.open(path, 'wt', encoding='utf-8') as f:
        json.dump(rows, f, separators=(',', ':'))


def _cache_path(cache_dir: str, kind: str, symbol: str, day: date, feed: str) -> str:
    safe_symbol = symbol.replace('/', '-')
    return os.path.join(cache_dir, feed, kind, f'{safe_symbol}_{day.isoformat()}.json.gz')


def _thin_quotes_per_second(rows: list[dict]) -> list[dict]:
    latest: dict[int, dict] = {}
    for row in rows:
        try:
            sec = int(row['t']) // 1000
        except Exception:
            continue
        latest[sec] = row
    return [latest[k] for k in sorted(latest)]


def _load_stock_events(symbol: str, day: date, feed: str, cache_dir: str,
                       quote_mode: str, refresh: bool, max_pages: int,
                       stats: ReplayStats) -> list[dict]:
    start_iso, end_iso, _start_sec, _end_sec = _session_bounds_utc(day)
    events: list[dict] = []

    trade_cache = _cache_path(cache_dir, 'trades', symbol, day, feed)
    trades = None if refresh else _json_gz_read(trade_cache)
    if trades is None:
        print(f"[engine-replay] fetch {day} {symbol} trades feed={feed}", flush=True)
        trades = alpaca_stock_trades(symbol, start_iso, end_iso, feed=feed, max_pages=max_pages)
        _json_gz_write(trade_cache, trades)
        stats.fetched[f'{symbol}:trades'] += len(trades)
    else:
        print(f"[engine-replay] cache {day} {symbol} trades rows={len(trades)}", flush=True)
        stats.cache_hits[f'{symbol}:trades'] += len(trades)
    for row in trades:
        events.append({'kind': 'stock_trade', 'symbol': symbol, 't': int(row['t']), 'row': row})

    if quote_mode != 'off':
        quote_cache = _cache_path(cache_dir, f'quotes_{quote_mode}', symbol, day, feed)
        quotes = None if refresh else _json_gz_read(quote_cache)
        if quotes is None:
            print(f"[engine-replay] fetch {day} {symbol} quotes feed={feed} mode={quote_mode}", flush=True)
            raw_quotes = alpaca_stock_quotes(symbol, start_iso, end_iso, feed=feed, max_pages=max_pages)
            quotes = _thin_quotes_per_second(raw_quotes) if quote_mode == 'per-second' else raw_quotes
            _json_gz_write(quote_cache, quotes)
            stats.fetched[f'{symbol}:quotes'] += len(quotes)
        else:
            print(f"[engine-replay] cache {day} {symbol} quotes rows={len(quotes)}", flush=True)
            stats.cache_hits[f'{symbol}:quotes'] += len(quotes)
        for row in quotes:
            events.append({'kind': 'stock_quote', 'symbol': symbol, 't': int(row['t']), 'row': row})
    return events


def _load_btc_bar_events(day: date, cache_dir: str, refresh: bool, stats: ReplayStats) -> list[dict]:
    start_iso, end_iso, _start_sec, _end_sec = _session_bounds_utc(day)
    cache = _cache_path(cache_dir, 'crypto_bars_1Min', BTC_SYMBOL, day, 'crypto-us')
    bars = None if refresh else _json_gz_read(cache)
    if bars is None:
        print(f"[engine-replay] fetch {day} BTC/USD 1Min bars", flush=True)
        bars = alpaca_crypto_bars(BTC_SYMBOL, start_iso, end_iso, timeframe='1Min')
        _json_gz_write(cache, bars)
        stats.fetched['BTC:bars'] += len(bars)
    else:
        print(f"[engine-replay] cache {day} BTC/USD 1Min bars rows={len(bars)}", flush=True)
        stats.cache_hits['BTC:bars'] += len(bars)
    events: list[dict] = []
    for bar in bars:
        start_ms = int(bar['t'])
        o = float(bar['o'])
        c = float(bar['c'])
        h = float(bar['h'])
        l = float(bar['l'])
        for offset in range(60):
            frac = offset / 59 if offset else 0.0
            price = o + (c - o) * frac
            price = max(l, min(h, price))
            ts_ms = start_ms + offset * 1000
            events.append({
                'kind': 'btc_synth_trade',
                'symbol': BTC_SYMBOL,
                't': ts_ms,
                'row': {'t': ts_ms, 'p': round(price, 2), 's': 0},
            })
    return events


def _prepared_tape_path(args, day: date) -> str:
    tickers = '-'.join(args.tickers)
    stem = f'{args.feed}_{args.quote_mode}_{args.btc_mode}_{tickers}_{day.isoformat()}.events.json.gz'
    return os.path.join(args.prepared_cache_dir, stem)


def _load_prepared_events(args, day: date, stats: ReplayStats) -> Optional[list[dict]]:
    if not getattr(args, 'use_prepared_events', False):
        return None
    if getattr(args, 'refresh_prepared_events', False):
        return None
    path = _prepared_tape_path(args, day)
    events = _json_gz_read(path)
    if events is not None:
        stats.cache_hits[f'prepared_events:{day.isoformat()}'] += len(events)
        print(f"[engine-replay] prepared tape {day.isoformat()} rows={len(events)}", flush=True)
    return events


def _write_prepared_events(args, day: date, events: list[dict]) -> Optional[str]:
    if not getattr(args, 'write_prepared_events', False) and not getattr(args, 'use_prepared_events', False):
        return None
    path = _prepared_tape_path(args, day)
    # Match the raw replay path: sort by timestamp only and rely on Python's
    # stable sort to preserve source-order ties across symbols/kinds.
    events = sorted(events, key=lambda e: int(e.get('t') or 0))
    _json_gz_write(path, events)
    print(f"[engine-replay] wrote prepared tape {day.isoformat()} rows={len(events)} path={path}", flush=True)
    return path


def _build_day_events(day: date, args, stats: ReplayStats) -> list[dict]:
    events: list[dict] = []
    for sym in args.tickers:
        events.extend(_load_stock_events(
            sym, day, args.feed, args.cache_dir, args.quote_mode,
            args.refresh, args.max_pages, stats,
        ))
    if args.btc_mode == 'bars':
        events.extend(_load_btc_bar_events(day, args.cache_dir, args.refresh, stats))
    events = sorted(events, key=lambda e: int(e.get('t') or 0))
    _write_prepared_events(args, day, events)
    return events


def _load_day_events(day: date, args, stats: ReplayStats) -> list[dict]:
    prepared = _load_prepared_events(args, day, stats)
    if prepared is not None:
        return prepared
    return _build_day_events(day, args, stats)


def _replay_throttle_label(args) -> str:
    mode = getattr(args, 'replay_regime_long_throttle', 'off') or 'off'
    if mode == 'off':
        return mode
    threshold = float(getattr(args, 'replay_regime_threshold_pct', 0.0) or 0.0)
    if abs(threshold) < 1e-12:
        return mode
    threshold_label = f'{threshold:+.2f}'.replace('+', 'p').replace('-', 'm').replace('.', 'p')
    return f'{mode}_thr{threshold_label}'


def _brs_failed_followthrough_label(args) -> str:
    after = float(getattr(args, 'brs_failed_followthrough_after_sec', 0.0) or 0.0)
    if after <= 0:
        return 'off'
    mfe = float(getattr(args, 'brs_failed_followthrough_min_mfe_pct', 0.0) or 0.0)
    pnl = float(getattr(args, 'brs_failed_followthrough_max_pnl_pct', 0.0) or 0.0)
    return f'ff{after:g}m{mfe:g}p{pnl:g}'.replace('.', 'p').replace('-', 'n')


def _tp_multiplier_label(args) -> str:
    parts = []
    for attr, prefix in (
        ('brs_tp_multiplier', 'brsTP'),
        ('momentum_tp_multiplier', 'momTP'),
        ('high_conviction_tp_multiplier', 'hiTP'),
    ):
        value = float(getattr(args, attr, 1.0) or 1.0)
        if abs(value - 1.0) > 1e-12:
            parts.append(f'{prefix}{value:g}'.replace('.', 'p').replace('-', 'n'))
    return '_'.join(parts) if parts else 'off'


def _fixed_exit_label(args) -> str:
    tp = getattr(args, 'fixed_tp_pct', None)
    sl = getattr(args, 'fixed_sl_pct', None)
    overrides = getattr(args, 'fixed_ticker_brackets', '') or ''
    if tp is None and sl is None and not overrides:
        return 'off'
    tp_label = 'live' if tp is None else f'tp{float(tp):g}'
    sl_label = 'live' if sl is None else f'sl{float(sl):g}'
    override_label = ''
    if overrides:
        override_label = '_' + str(overrides).replace(',', '_').replace(':', '-')
    return f'fixed_{tp_label}_{sl_label}{override_label}'.replace('.', 'p').replace('-', 'n')


def _fixed_ticker_bracket(args, ticker: str) -> tuple[Optional[float], Optional[float]]:
    raw = getattr(args, 'fixed_ticker_brackets', '') if args is not None else ''
    if not raw:
        return None, None
    ticker = ticker.upper()
    for item in str(raw).split(','):
        parts = [p.strip() for p in item.split(':')]
        if len(parts) != 3 or parts[0].upper() != ticker:
            continue
        tp = None if parts[1].lower() in ('', 'live', 'none') else float(parts[1])
        sl = None if parts[2].lower() in ('', 'live', 'none') else float(parts[2])
        return tp, sl
    return None, None


def _entry_confirmation_label(args) -> str:
    delay = int(float(getattr(args, 'entry_confirmation_delay_sec', 0.0) or 0.0))
    mode = getattr(args, 'entry_confirmation_mode', 'off') or 'off'
    setup = getattr(args, 'entry_confirmation_setup', 'all') or 'all'
    if delay <= 0 or mode == 'off':
        return 'off'
    fav = float(getattr(args, 'entry_confirmation_min_favorable_pct', 0.0) or 0.0)
    adv = float(getattr(args, 'entry_confirmation_adverse_pct', 0.0) or 0.0)
    session = getattr(args, 'entry_confirmation_session_phase', 'all') or 'all'
    edge = getattr(args, 'entry_confirmation_edge_threshold_pct', None)
    rel = getattr(args, 'entry_confirmation_rel_threshold_pct', None)
    edge_label = '' if edge is None else f'_e{float(edge):g}'
    rel_label = '' if rel is None else f'_r{float(rel):g}'
    label = f'ec{mode}_{setup}_{session}_{delay}s_f{fav:g}_a{adv:g}{edge_label}{rel_label}'
    return label.replace('.', 'p').replace('-', 'n')


def _path_failure_label(args) -> str:
    after = int(float(getattr(args, 'path_failure_after_sec', 0.0) or 0.0))
    action = getattr(args, 'path_failure_action', 'off') or 'off'
    if after <= 0 or action == 'off':
        return 'off'
    setup = getattr(args, 'path_failure_setup', 'all') or 'all'
    threshold = float(getattr(args, 'path_failure_edge_threshold_pct', 0.0) or 0.0)
    session = getattr(args, 'path_failure_session_phase', 'all') or 'all'
    rel = getattr(args, 'path_failure_rel_threshold_pct', None)
    window = int(float(getattr(args, 'path_failure_window_sec', 0.0) or 0.0))
    reduce_fraction = float(getattr(args, 'path_failure_reduce_fraction', 0.0) or 0.0)
    rel_label = '' if rel is None else f'_r{float(rel):g}'
    window_label = '' if window <= 0 else f'_w{window:g}'
    reduce_label = '' if action != 'reduce' else f'_q{reduce_fraction:g}'
    label = f'pf{action}_{setup}_{session}_{after}s_e{threshold:g}{rel_label}{window_label}{reduce_label}'
    return label.replace('.', 'p').replace('-', 'n')


def _entry_warning_gate_label(args) -> str:
    mode = getattr(args, 'entry_warning_gate_mode', 'off') or 'off'
    if mode == 'off':
        return 'off'
    action = getattr(args, 'entry_warning_gate_action', 'veto') or 'veto'
    tickers = (getattr(args, 'entry_warning_gate_tickers', '') or '').replace(',', '-')
    session = getattr(args, 'entry_warning_gate_session_phase', 'all') or 'all'
    mom = float(getattr(args, 'entry_warning_gate_stock_mom60_threshold_pct', -0.10) or -0.10)
    rel = float(getattr(args, 'entry_warning_gate_rel60_threshold_pct', -0.10) or -0.10)
    label = f'ewg{action}_{mode}_{tickers}_{session}_m{mom:g}_r{rel:g}'
    if mode == 'ticker_chop_veto':
        rng = float(getattr(args, 'entry_warning_gate_chop_range_threshold_pct', 0.75) or 0.75)
        eff = float(getattr(args, 'entry_warning_gate_chop_efficiency_threshold', 0.20) or 0.20)
        flips = float(getattr(args, 'entry_warning_gate_chop_flips_threshold', 10) or 10)
        max_score = float(getattr(args, 'entry_warning_gate_max_score', 999.0) or 999.0)
        label = f'{label}_cr{rng:g}_ce{eff:g}_cf{flips:g}_s{max_score:g}'
    return label.replace('.', 'p').replace('-', 'n')


def _chop_bracket_label(args) -> str:
    mode = getattr(args, 'chop_bracket_mode', 'off') or 'off'
    if mode == 'off':
        return 'off'
    tickers = (getattr(args, 'chop_bracket_tickers', '') or '').replace(',', '-')
    rng = float(getattr(args, 'chop_bracket_range_threshold_pct', 0.75) or 0.75)
    eff = float(getattr(args, 'chop_bracket_efficiency_threshold', 0.20) or 0.20)
    flips = float(getattr(args, 'chop_bracket_flips_threshold', 10) or 10)
    max_score = float(getattr(args, 'chop_bracket_max_score', 999.0) or 999.0)
    tp = float(getattr(args, 'chop_bracket_tp_pct', 0.0010) or 0.0010)
    sl = float(getattr(args, 'chop_bracket_sl_pct', 0.0010) or 0.0010)
    label = f'cb{mode}_{tickers}_cr{rng:g}_ce{eff:g}_cf{flips:g}_s{max_score:g}_tp{tp:g}_sl{sl:g}'
    return label.replace('.', 'p').replace('-', 'n')


def _giveback_exit_label(args) -> str:
    after = int(float(getattr(args, 'giveback_exit_after_sec', 0.0) or 0.0))
    action = getattr(args, 'giveback_exit_action', 'off') or 'off'
    if after <= 0 or action == 'off':
        return 'off'
    setup = getattr(args, 'giveback_exit_setup', 'all') or 'all'
    tickers = (getattr(args, 'giveback_exit_tickers', '') or '').replace(',', '-')
    min_mfe = float(getattr(args, 'giveback_exit_min_mfe_pct', 0.0) or 0.0)
    giveback = float(getattr(args, 'giveback_exit_giveback_pct', 0.0) or 0.0)
    max_pnl = float(getattr(args, 'giveback_exit_max_pnl_pct', 0.0) or 0.0)
    confirm = int(float(getattr(args, 'giveback_exit_confirm_sec', 0.0) or 0.0))
    cooldown = int(float(getattr(args, 'giveback_exit_cooldown_sec', 0.0) or 0.0))
    label = f'gb{action}_{tickers}_{setup}_{after}s_m{min_mfe:g}_g{giveback:g}_p{max_pnl:g}_q{confirm}s_c{cooldown}s'
    return label.replace('.', 'p').replace('-', 'n')


def _day_checkpoint_path(args, day: date) -> str:
    tickers = '-'.join(args.tickers)
    profile = 'live'
    if getattr(args, 'scoring_profile', None):
        profile = os.path.splitext(os.path.basename(args.scoring_profile))[0]
    throttle = _replay_throttle_label(args)
    if throttle != 'off':
        profile = f'{profile}_{throttle}'
    brs_exit = _brs_failed_followthrough_label(args)
    if brs_exit != 'off':
        profile = f'{profile}_{brs_exit}'
    tp_mult = _tp_multiplier_label(args)
    if tp_mult != 'off':
        profile = f'{profile}_{tp_mult}'
    fixed_exit = _fixed_exit_label(args)
    if fixed_exit != 'off':
        profile = f'{profile}_{fixed_exit}'
    entry_confirm = _entry_confirmation_label(args)
    if entry_confirm != 'off':
        profile = f'{profile}_{entry_confirm}'
    path_failure = _path_failure_label(args)
    if path_failure != 'off':
        profile = f'{profile}_{path_failure}'
    entry_warning = _entry_warning_gate_label(args)
    if entry_warning != 'off':
        profile = f'{profile}_{entry_warning}'
    chop_bracket = _chop_bracket_label(args)
    if chop_bracket != 'off':
        profile = f'{profile}_{chop_bracket}'
    giveback_exit = _giveback_exit_label(args)
    if giveback_exit != 'off':
        profile = f'{profile}_{giveback_exit}'
    validation_mode = getattr(args, 'validation_mode', 'research')
    indicator_mode = getattr(args, 'indicator_mode', 'live')
    return os.path.join(
        args.out_dir,
        'engine_replay_days',
        f'{args.feed}_{args.quote_mode}_{args.btc_mode}_{tickers}_{profile}_{validation_mode}_{indicator_mode}_{args.start_balance:.0f}_{day.isoformat()}.json',
    )


def _file_sha256(path: str) -> str | None:
    if not os.path.exists(path):
        return None
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def _day_checkpoint_fingerprint(args, day: date) -> dict:
    payload = {
        'schema_version': 1,
        'day': day.isoformat(),
        'python': platform.python_version(),
        'replay': {
            'tickers': list(args.tickers),
            'feed': args.feed,
            'quote_mode': args.quote_mode,
            'btc_mode': args.btc_mode,
            'start_balance': float(args.start_balance),
            'validation_mode': getattr(args, 'validation_mode', 'research'),
            'indicator_mode': getattr(args, 'indicator_mode', 'live'),
            'scoring_profile': os.path.abspath(args.scoring_profile) if getattr(args, 'scoring_profile', None) else None,
            'replay_regime_long_throttle': getattr(args, 'replay_regime_long_throttle', 'off'),
            'replay_regime_threshold_pct': float(getattr(args, 'replay_regime_threshold_pct', 0.0) or 0.0),
            'brs_failed_followthrough_after_sec': float(getattr(args, 'brs_failed_followthrough_after_sec', 0.0) or 0.0),
            'brs_failed_followthrough_min_mfe_pct': float(getattr(args, 'brs_failed_followthrough_min_mfe_pct', 0.0) or 0.0),
            'brs_failed_followthrough_max_pnl_pct': float(getattr(args, 'brs_failed_followthrough_max_pnl_pct', 0.0) or 0.0),
            'brs_tp_multiplier': float(getattr(args, 'brs_tp_multiplier', 1.0) or 1.0),
            'momentum_tp_multiplier': float(getattr(args, 'momentum_tp_multiplier', 1.0) or 1.0),
            'high_conviction_tp_multiplier': float(getattr(args, 'high_conviction_tp_multiplier', 1.0) or 1.0),
            'fixed_tp_pct': getattr(args, 'fixed_tp_pct', None),
            'fixed_sl_pct': getattr(args, 'fixed_sl_pct', None),
            'fixed_ticker_brackets': getattr(args, 'fixed_ticker_brackets', ''),
            'entry_confirmation_delay_sec': int(float(getattr(args, 'entry_confirmation_delay_sec', 0.0) or 0.0)),
            'entry_confirmation_mode': getattr(args, 'entry_confirmation_mode', 'off'),
            'entry_confirmation_setup': getattr(args, 'entry_confirmation_setup', 'all'),
            'entry_confirmation_min_favorable_pct': float(getattr(args, 'entry_confirmation_min_favorable_pct', 0.0) or 0.0),
            'entry_confirmation_adverse_pct': float(getattr(args, 'entry_confirmation_adverse_pct', 0.0) or 0.0),
            'entry_confirmation_session_phase': getattr(args, 'entry_confirmation_session_phase', 'all'),
            'entry_confirmation_edge_threshold_pct': getattr(args, 'entry_confirmation_edge_threshold_pct', None),
            'entry_confirmation_rel_threshold_pct': getattr(args, 'entry_confirmation_rel_threshold_pct', None),
            'path_failure_after_sec': int(float(getattr(args, 'path_failure_after_sec', 0.0) or 0.0)),
            'path_failure_edge_threshold_pct': float(getattr(args, 'path_failure_edge_threshold_pct', 0.0) or 0.0),
            'path_failure_action': getattr(args, 'path_failure_action', 'off'),
            'path_failure_setup': getattr(args, 'path_failure_setup', 'all'),
            'path_failure_session_phase': getattr(args, 'path_failure_session_phase', 'all'),
            'path_failure_rel_threshold_pct': getattr(args, 'path_failure_rel_threshold_pct', None),
            'path_failure_window_sec': int(float(getattr(args, 'path_failure_window_sec', 0.0) or 0.0)),
            'path_failure_reduce_fraction': float(getattr(args, 'path_failure_reduce_fraction', 0.0) or 0.0),
            'entry_warning_gate_mode': getattr(args, 'entry_warning_gate_mode', 'off'),
            'entry_warning_gate_action': getattr(args, 'entry_warning_gate_action', 'veto'),
            'entry_warning_gate_tickers': getattr(args, 'entry_warning_gate_tickers', ''),
            'entry_warning_gate_setup': getattr(args, 'entry_warning_gate_setup', 'all'),
            'entry_warning_gate_session_phase': getattr(args, 'entry_warning_gate_session_phase', 'all'),
            'entry_warning_gate_stock_mom60_threshold_pct': float(getattr(args, 'entry_warning_gate_stock_mom60_threshold_pct', -0.10) or -0.10),
            'entry_warning_gate_rel60_threshold_pct': float(getattr(args, 'entry_warning_gate_rel60_threshold_pct', -0.10) or -0.10),
            'entry_warning_gate_chop_range_threshold_pct': float(getattr(args, 'entry_warning_gate_chop_range_threshold_pct', 0.75) or 0.75),
            'entry_warning_gate_chop_efficiency_threshold': float(getattr(args, 'entry_warning_gate_chop_efficiency_threshold', 0.20) or 0.20),
            'entry_warning_gate_chop_flips_threshold': float(getattr(args, 'entry_warning_gate_chop_flips_threshold', 10) or 10),
            'entry_warning_gate_max_score': float(getattr(args, 'entry_warning_gate_max_score', 999.0) or 999.0),
            'chop_bracket_mode': getattr(args, 'chop_bracket_mode', 'off'),
            'chop_bracket_tickers': getattr(args, 'chop_bracket_tickers', ''),
            'chop_bracket_range_threshold_pct': float(getattr(args, 'chop_bracket_range_threshold_pct', 0.75) or 0.75),
            'chop_bracket_efficiency_threshold': float(getattr(args, 'chop_bracket_efficiency_threshold', 0.20) or 0.20),
            'chop_bracket_flips_threshold': float(getattr(args, 'chop_bracket_flips_threshold', 10) or 10),
            'chop_bracket_max_score': float(getattr(args, 'chop_bracket_max_score', 999.0) or 999.0),
            'chop_bracket_tp_pct': float(getattr(args, 'chop_bracket_tp_pct', 0.0010) or 0.0010),
            'chop_bracket_sl_pct': float(getattr(args, 'chop_bracket_sl_pct', 0.0010) or 0.0010),
            'stop_loss_cooldown_tickers': getattr(args, 'stop_loss_cooldown_tickers', ''),
            'stop_loss_cooldown_sec': int(float(getattr(args, 'stop_loss_cooldown_sec', 0) or 0)),
            'giveback_exit_after_sec': int(float(getattr(args, 'giveback_exit_after_sec', 0.0) or 0.0)),
            'giveback_exit_min_mfe_pct': float(getattr(args, 'giveback_exit_min_mfe_pct', 0.0) or 0.0),
            'giveback_exit_giveback_pct': float(getattr(args, 'giveback_exit_giveback_pct', 0.0) or 0.0),
            'giveback_exit_max_pnl_pct': float(getattr(args, 'giveback_exit_max_pnl_pct', 0.0) or 0.0),
            'giveback_exit_confirm_sec': int(float(getattr(args, 'giveback_exit_confirm_sec', 0.0) or 0.0)),
            'giveback_exit_action': getattr(args, 'giveback_exit_action', 'off'),
            'giveback_exit_tickers': getattr(args, 'giveback_exit_tickers', ''),
            'giveback_exit_setup': getattr(args, 'giveback_exit_setup', 'all'),
            'giveback_exit_cooldown_sec': int(float(getattr(args, 'giveback_exit_cooldown_sec', 0.0) or 0.0)),
            'use_prepared_events': bool(getattr(args, 'use_prepared_events', False)),
            'prepared_cache_dir': os.path.abspath(getattr(args, 'prepared_cache_dir', DEFAULT_PREPARED_DIR)),
            'cache_dir': os.path.abspath(getattr(args, 'cache_dir', DEFAULT_CACHE_DIR)),
            'step2_latency_mode': getattr(args, 'step2_latency_mode', 'off'),
            'step2_latency_model': os.path.abspath(getattr(args, 'step2_latency_model', step2_latency_model.DEFAULT_MODEL_PATH)),
            'step2_latency_percentile': getattr(args, 'step2_latency_percentile', 'p75'),
        },
        'session': {
            'start_hour': SESSION_START_H,
            'start_minute': SESSION_START_M,
            'entry_cutoff_hour': ENTRY_CUTOFF_H,
            'entry_cutoff_minute': ENTRY_CUTOFF_M,
            'flatten_hour': FLATTEN_H,
            'flatten_minute': FLATTEN_M,
            'conditional_time_stop_enabled': CONDITIONAL_TIME_STOP_ENABLED,
            'conditional_stop_min': COND_STOP_MIN,
            'trade_size_pct': TRADE_SIZE_PCT,
            'min_conviction': MIN_CONVICTION,
            'ticker_cfg': TICKER_CFG,
        },
        'code_hashes': {
            'backtest_30d_engine.py': _file_sha256(os.path.join(HERE, 'backtest_30d_engine.py')),
            'ws_scalp.py': _file_sha256(os.path.join(HERE, 'ws_scalp.py')),
            'scoring_profiles.py': _file_sha256(os.path.join(HERE, 'scoring_profiles.py')),
            'trading_config.json': _file_sha256(os.path.join(HERE, 'trading_config.json')),
            'scoring_profile': _file_sha256(args.scoring_profile) if getattr(args, 'scoring_profile', None) else None,
        },
        'input_hashes': {
            os.path.relpath(path, HERE).replace('\\', '/'): _file_sha256(path)
            for path in (
                [_cache_path(args.cache_dir, 'trades', ticker, day, args.feed) for ticker in args.tickers]
                + (
                    [_cache_path(args.cache_dir, f'quotes_{args.quote_mode}', ticker, day, args.feed) for ticker in args.tickers]
                    if args.quote_mode != 'off' else []
                )
                + (
                    [_cache_path(args.cache_dir, 'crypto_bars_1Min', BTC_SYMBOL, day, 'crypto-us')]
                    if args.btc_mode == 'bars' else []
                )
                + [_prepared_tape_path(args, day)]
            )
        },
    }
    payload['fingerprint'] = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('utf-8')
    ).hexdigest()
    return payload


def _read_day_checkpoint(args, day: date) -> Optional[dict]:
    if not getattr(args, 'resume_days', False):
        return None
    path = _day_checkpoint_path(args, day)
    data = _read_json(path)
    if isinstance(data, dict) and isinstance(data.get('rows'), list):
        current = _day_checkpoint_fingerprint(args, day)
        if data.get('fingerprint') != current.get('fingerprint'):
            print(f"[engine-replay] checkpoint fingerprint mismatch; rebuilding {day}", flush=True)
            return None
        if (
            getattr(args, 'rebuild_missing_opportunities', False)
            and (
                getattr(args, 'write_opportunity_ledger', False)
                or getattr(args, 'write_step2_decision_parity', False)
            )
            and not data.get('opportunities')
        ):
            print(f"[engine-replay] checkpoint lacks opportunities; rebuilding {day}", flush=True)
            return None
        print(f"[engine-replay] resume checkpoint {day} rows={len(data.get('rows') or [])}", flush=True)
        return data
    return None


def _write_day_checkpoint(args, day: date, rows: list[dict], start_balance: float,
                          ending_balance: float, opportunities: Optional[list[dict]] = None) -> None:
    os.makedirs(os.path.dirname(_day_checkpoint_path(args, day)), exist_ok=True)
    payload = {
        'schema_version': 1,
        'day': day.isoformat(),
        'fingerprint': _day_checkpoint_fingerprint(args, day).get('fingerprint'),
        'start_balance': round(start_balance, 4),
        'ending_balance': round(ending_balance, 4),
        'pnl': round(ending_balance - start_balance, 4),
        'rows': rows,
    }
    if opportunities is not None:
        payload['opportunities'] = opportunities
    with open(_day_checkpoint_path(args, day), 'w', encoding='utf-8') as f:
        json.dump(payload, f, separators=(',', ':'), sort_keys=True)


def _scale_day_rows(rows: list[dict], factor: float) -> list[dict]:
    if abs(factor - 1.0) < 0.000001:
        return rows
    scaled = []
    for row in rows:
        copied = dict(row)
        for key in ('pnl', 'alloc'):
            try:
                copied[key] = round(float(copied.get(key) or 0) * factor, 4)
            except Exception:
                pass
        try:
            copied['qty'] = round(float(copied.get('qty') or 0) * factor, 6)
        except Exception:
            pass
        scaled.append(copied)
    return scaled


def _checkpoint_to_compounded_rows(checkpoint: dict, current_balance: float) -> tuple[list[dict], float]:
    rows = checkpoint.get('rows') or []
    start_balance = float(checkpoint.get('start_balance') or START_BALANCE)
    ending_balance = float(checkpoint.get('ending_balance') or start_balance)
    factor = current_balance / start_balance if start_balance else 1.0
    return _scale_day_rows(rows, factor), current_balance + (ending_balance - start_balance) * factor


def _worker_args_dict(args) -> dict:
    return {
        'start': args.start,
        'end': args.end,
        'tickers': list(args.tickers),
        'trade_tickers': list(getattr(args, 'trade_tickers', None) or args.tickers),
        'feed': args.feed,
        'quote_mode': args.quote_mode,
        'btc_mode': args.btc_mode,
        'cache_dir': args.cache_dir,
        'out_dir': args.out_dir,
        'refresh': args.refresh,
        'max_pages': args.max_pages,
        'resume_days': args.resume_days,
        'rebuild_missing_opportunities': args.rebuild_missing_opportunities,
        'validation_mode': args.validation_mode,
        'indicator_mode': args.indicator_mode,
        'sample_days': getattr(args, 'sample_days', ''),
        'start_balance': args.start_balance,
        'write_opportunity_ledger': args.write_opportunity_ledger,
        'opportunity_dir': args.opportunity_dir,
        'prepared_cache_dir': args.prepared_cache_dir,
        'use_prepared_events': args.use_prepared_events,
        'write_prepared_events': args.write_prepared_events,
        'refresh_prepared_events': args.refresh_prepared_events,
        'scoring_profile': args.scoring_profile,
        'replay_regime_long_throttle': args.replay_regime_long_throttle,
        'replay_regime_threshold_pct': args.replay_regime_threshold_pct,
        'brs_failed_followthrough_after_sec': args.brs_failed_followthrough_after_sec,
        'brs_failed_followthrough_min_mfe_pct': args.brs_failed_followthrough_min_mfe_pct,
        'brs_failed_followthrough_max_pnl_pct': args.brs_failed_followthrough_max_pnl_pct,
        'brs_tp_multiplier': args.brs_tp_multiplier,
        'momentum_tp_multiplier': args.momentum_tp_multiplier,
        'high_conviction_tp_multiplier': args.high_conviction_tp_multiplier,
        'fixed_tp_pct': args.fixed_tp_pct,
        'fixed_sl_pct': args.fixed_sl_pct,
        'fixed_ticker_brackets': args.fixed_ticker_brackets,
        'entry_confirmation_delay_sec': args.entry_confirmation_delay_sec,
        'entry_confirmation_mode': args.entry_confirmation_mode,
        'entry_confirmation_setup': args.entry_confirmation_setup,
        'entry_confirmation_min_favorable_pct': args.entry_confirmation_min_favorable_pct,
        'entry_confirmation_adverse_pct': args.entry_confirmation_adverse_pct,
        'entry_confirmation_session_phase': args.entry_confirmation_session_phase,
        'entry_confirmation_edge_threshold_pct': args.entry_confirmation_edge_threshold_pct,
        'entry_confirmation_rel_threshold_pct': args.entry_confirmation_rel_threshold_pct,
        'path_failure_after_sec': args.path_failure_after_sec,
        'path_failure_edge_threshold_pct': args.path_failure_edge_threshold_pct,
        'path_failure_action': args.path_failure_action,
        'path_failure_setup': args.path_failure_setup,
        'path_failure_session_phase': args.path_failure_session_phase,
        'path_failure_rel_threshold_pct': args.path_failure_rel_threshold_pct,
        'path_failure_window_sec': args.path_failure_window_sec,
        'path_failure_reduce_fraction': args.path_failure_reduce_fraction,
        'entry_warning_gate_mode': args.entry_warning_gate_mode,
        'entry_warning_gate_action': args.entry_warning_gate_action,
        'entry_warning_gate_tickers': args.entry_warning_gate_tickers,
        'entry_warning_gate_setup': args.entry_warning_gate_setup,
        'entry_warning_gate_session_phase': args.entry_warning_gate_session_phase,
        'entry_warning_gate_stock_mom60_threshold_pct': args.entry_warning_gate_stock_mom60_threshold_pct,
        'entry_warning_gate_rel60_threshold_pct': args.entry_warning_gate_rel60_threshold_pct,
        'entry_warning_gate_chop_range_threshold_pct': args.entry_warning_gate_chop_range_threshold_pct,
        'entry_warning_gate_chop_efficiency_threshold': args.entry_warning_gate_chop_efficiency_threshold,
        'entry_warning_gate_chop_flips_threshold': args.entry_warning_gate_chop_flips_threshold,
        'entry_warning_gate_max_score': args.entry_warning_gate_max_score,
        'chop_bracket_mode': args.chop_bracket_mode,
        'chop_bracket_tickers': args.chop_bracket_tickers,
        'chop_bracket_range_threshold_pct': args.chop_bracket_range_threshold_pct,
        'chop_bracket_efficiency_threshold': args.chop_bracket_efficiency_threshold,
        'chop_bracket_flips_threshold': args.chop_bracket_flips_threshold,
        'chop_bracket_max_score': args.chop_bracket_max_score,
        'chop_bracket_tp_pct': args.chop_bracket_tp_pct,
        'chop_bracket_sl_pct': args.chop_bracket_sl_pct,
        'stop_loss_cooldown_tickers': args.stop_loss_cooldown_tickers,
        'stop_loss_cooldown_sec': args.stop_loss_cooldown_sec,
        'loss_cluster_throttle_tickers': args.loss_cluster_throttle_tickers,
        'loss_cluster_window_sec': args.loss_cluster_window_sec,
        'loss_cluster_count': args.loss_cluster_count,
        'loss_cluster_cooldown_sec': args.loss_cluster_cooldown_sec,
        'loss_cluster_scope': args.loss_cluster_scope,
        'giveback_exit_after_sec': args.giveback_exit_after_sec,
        'giveback_exit_min_mfe_pct': args.giveback_exit_min_mfe_pct,
        'giveback_exit_giveback_pct': args.giveback_exit_giveback_pct,
        'giveback_exit_max_pnl_pct': args.giveback_exit_max_pnl_pct,
        'giveback_exit_confirm_sec': args.giveback_exit_confirm_sec,
        'giveback_exit_action': args.giveback_exit_action,
        'giveback_exit_tickers': args.giveback_exit_tickers,
        'giveback_exit_setup': args.giveback_exit_setup,
        'giveback_exit_cooldown_sec': args.giveback_exit_cooldown_sec,
        'profile_replay_timing': args.profile_replay_timing,
        'disable_all_open_signal_skip': args.disable_all_open_signal_skip,
        'workers': 1,
    }


def _run_day_worker(args_dict: dict, day_iso: str) -> dict:
    args = argparse.Namespace(**args_dict)
    day = _parse_day(day_iso)
    checkpoint = _read_day_checkpoint(args, day)
    if checkpoint:
        return checkpoint
    clock = ReplayClock()
    args._scoring_profile_data = scoring_profiles.load_profile(getattr(args, 'scoring_profile', None))
    original_active_scoring_profile = getattr(ws_scalp, 'ACTIVE_SCORING_PROFILE', None)
    if getattr(args, 'scoring_profile', None):
        ws_scalp.ACTIVE_SCORING_PROFILE = {}
    original_time = ws_scalp.time.time
    original_near_signal_logger = getattr(ws_scalp, '_log_near_signal', None)
    ws_scalp.time.time = clock.time
    if original_near_signal_logger is not None:
        ws_scalp._log_near_signal = lambda *a, **k: None
    stats = ReplayStats()
    try:
        rows, ending_balance, opportunities = _run_day(day, args, clock, stats, float(args.start_balance))
        if args.resume_days:
            _write_day_checkpoint(args, day, rows, float(args.start_balance), ending_balance, opportunities)
        _write_opportunity_ledger(args, day, opportunities)
        parity_summary = _write_step2_decision_parity(args, day, opportunities)
        return {
            'day': day.isoformat(),
            'start_balance': round(float(args.start_balance), 4),
            'ending_balance': round(ending_balance, 4),
            'pnl': round(ending_balance - float(args.start_balance), 4),
            'rows': rows,
            'opportunities': opportunities,
            'cache_hits': dict(stats.cache_hits),
            'fetched': dict(stats.fetched),
            'seconds_replayed': stats.seconds,
            'emitted_signals': stats.emitted_signals,
            'skipped_signals': dict(stats.skipped_signals),
            'timing': dict(stats.timing),
            'step2_decision_parity': parity_summary,
        }
    finally:
        ws_scalp.time.time = original_time
        ws_scalp.ACTIVE_SCORING_PROFILE = original_active_scoring_profile
        if original_near_signal_logger is not None:
            ws_scalp._log_near_signal = original_near_signal_logger


def _opportunity_ledger_path(args, day: date) -> str:
    tickers = '-'.join(args.tickers)
    profile = ''
    if getattr(args, 'scoring_profile', None):
        profile = '_' + os.path.splitext(os.path.basename(args.scoring_profile))[0]
    out_dir = args.opportunity_dir or os.path.join(args.out_dir, 'opportunities')
    return os.path.join(
        out_dir,
        f'engine_opportunities_{args.feed}_{args.quote_mode}_{args.btc_mode}_{tickers}{profile}_{day.isoformat()}.jsonl',
    )


def _json_clone(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str, separators=(',', ':')))


def _write_opportunity_ledger(args, day: date, opportunities: list[dict]) -> Optional[str]:
    if not getattr(args, 'write_opportunity_ledger', False):
        return None
    path = _opportunity_ledger_path(args, day)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        for row in opportunities:
            f.write(json.dumps(row, separators=(',', ':'), sort_keys=True, default=str) + '\n')
    print(f"[engine-replay] opportunity ledger {day.isoformat()} rows={len(opportunities)} path={path}", flush=True)
    return path


def _capture_opportunities(args) -> bool:
    return bool(
        getattr(args, 'write_opportunity_ledger', False)
        or getattr(args, 'write_step2_decision_parity', False)
    )


def _write_step2_decision_parity(args, day: date, opportunities: list[dict]) -> Optional[dict]:
    if not getattr(args, 'write_step2_decision_parity', False):
        return None
    run_context = {
        'feed': args.feed,
        'quote_mode': args.quote_mode,
        'btc_mode': args.btc_mode,
        'tickers': list(args.tickers),
        'trade_tickers': list(getattr(args, 'trade_tickers', None) or args.tickers),
        'indicator_mode': getattr(args, 'indicator_mode', 'live'),
        'validation_mode': getattr(args, 'validation_mode', 'research'),
        'start_balance': float(getattr(args, 'start_balance', START_BALANCE) or START_BALANCE),
        'use_prepared_events': bool(getattr(args, 'use_prepared_events', False)),
        'prepared_cache_dir': os.path.abspath(getattr(args, 'prepared_cache_dir', DEFAULT_PREPARED_DIR)),
        'scoring_profile': os.path.abspath(args.scoring_profile) if getattr(args, 'scoring_profile', None) else None,
        'step2_latency_mode': getattr(args, 'step2_latency_mode', 'off'),
        'step2_latency_model': os.path.abspath(getattr(args, 'step2_latency_model', step2_latency_model.DEFAULT_MODEL_PATH)),
        'step2_latency_percentile': getattr(args, 'step2_latency_percentile', 'p75'),
        'execution_kernel_hash': STEP2_EXECUTION_CONTRACT.get('execution_kernel_hash'),
        'step2_execution_contract_hash': step2_execution_contract.execution_contract_hash(TRADING_CONFIG),
        'step2_parity_contract_hash': step2_parity_contract.contract_hash(STEP2_PARITY_CONTRACT),
    }
    summary = step2_decision_parity.write_day(
        day.isoformat(),
        opportunities,
        TRADING_CONFIG,
        run_context=run_context,
        out_dir=getattr(args, 'step2_decision_parity_dir', step2_decision_parity.OUT_DIR),
    )
    print(
        f"[engine-replay] step2 decision parity {day.isoformat()} "
        f"rows={summary.get('rows')} entered={summary.get('entered')} path={summary.get('path')}",
        flush=True,
    )
    return summary


def _finalist_mode(args) -> bool:
    return getattr(args, 'validation_mode', 'research') == 'finalist'


def _avg_session_return(indicators: dict) -> Optional[float]:
    vals = []
    for ind in (indicators or {}).values():
        if not isinstance(ind, dict) or not ind.get('ready'):
            continue
        val = _float(ind.get('session_return_pct'))
        if val is not None:
            vals.append(val)
    return sum(vals) / len(vals) if vals else None


def _long_throttle_reason(args, sig: dict, ind: dict, btc_ind: dict,
                          stock_indicators: dict) -> Optional[str]:
    mode = getattr(args, 'replay_regime_long_throttle', 'off') or 'off'
    if mode == 'off' or sig.get('side') != 'LONG':
        return None
    threshold = float(getattr(args, 'replay_regime_threshold_pct', 0.0) or 0.0)
    ticker_ret = _float(ind.get('session_return_pct'))
    miner_ret = _avg_session_return(stock_indicators)
    btc_ret = _float((btc_ind or {}).get('session_return_pct'))
    setup = sig.get('setup_type')
    profile_original_side = sig.get('profile_original_side')
    vwap_dist = _float(ind.get('vwap_dist'))
    btc_regime = sig.get('btc_regime') or ((sig.get('btc_context') or {}).get('regime'))
    miner_state = ((sig.get('miner_basket') or {}).get('state'))
    rel_strength = _float(((sig.get('relative_strength') or {}).get('stock_minus_btc_implied_60s')))
    flow_buy_pct = _float(((ind.get('flow_30s') or {}).get('buy_pct')))
    session_range_pos = _float(ind.get('session_range_pos'))

    ticker_negative = ticker_ret is not None and ticker_ret < threshold
    miner_negative = miner_ret is not None and miner_ret < threshold
    btc_negative = btc_ret is not None and btc_ret < threshold
    below_vwap = vwap_dist is not None and vwap_dist < 0
    above_vwap = vwap_dist is not None and vwap_dist > 0
    flipped_short_to_long = profile_original_side == 'SHORT'
    btc_bearish = btc_regime in ('bear', 'bear_momentum', 'impulse_down')
    btc_not_bullish = btc_regime not in ('bull', 'bull_momentum', 'impulse_up')
    miner_not_confirmed = miner_state != 'confirmed'
    stock_lagging_btc = rel_strength is not None and rel_strength < 0
    weak_long_flow = flow_buy_pct is not None and flow_buy_pct < 50
    lower_half_range = session_range_pos is not None and session_range_pos < 0.5
    session_phase = sig.get('session_phase') or _session_phase(ind)
    mom5 = _float(ind.get('mom_5s'))
    mom15 = _float(ind.get('mom_15s'))
    followthrough_momentum = (
        mom5 is not None and mom15 is not None
        and mom5 > 0.05 and mom15 > 0.05
    )
    btc_chop_penalty = 'btc_chop_penalty' in (((sig.get('signal_quality') or {}).get('score_components')) or {})

    if mode == 'ticker-negative' and ticker_negative:
        return f'ticker_session_return<{threshold:g}'
    if mode == 'miner-negative' and miner_negative:
        return f'miner_avg_session_return<{threshold:g}'
    if mode == 'miner-btc-negative' and miner_negative and btc_negative:
        return f'miner_and_btc_session_return<{threshold:g}'
    if mode == 'trend-pullback-miner-negative' and setup == 'trend_pullback' and miner_negative:
        return f'trend_pullback_miner_avg_session_return<{threshold:g}'
    if mode == 'btc-negative' and btc_negative:
        return f'btc_session_return<{threshold:g}'
    if mode == 'ticker-negative-below-vwap' and ticker_negative and below_vwap:
        return f'ticker_session_return<{threshold:g}_below_vwap'
    if mode == 'ticker-negative-btc-bear' and ticker_negative and btc_bearish:
        return f'ticker_session_return<{threshold:g}_btc_bearish'
    if mode == 'ticker-negative-not-above-vwap' and ticker_negative and not above_vwap:
        return f'ticker_session_return<{threshold:g}_not_above_vwap'
    if mode == 'ticker-negative-not-momentum' and ticker_negative and setup != 'momentum_breakout':
        return f'ticker_session_return<{threshold:g}_not_momentum'
    if mode == 'ticker-negative-btc-relative' and ticker_negative and setup == 'btc_relative_strength':
        return f'ticker_session_return<{threshold:g}_btc_relative_strength'
    if mode == 'ticker-negative-require-profile-agree' and ticker_negative and profile_original_side != 'LONG':
        return f'ticker_session_return<{threshold:g}_profile_not_long'
    if mode == 'flip-short-long-ticker-negative' and flipped_short_to_long and ticker_negative:
        return f'profile_short_to_long_ticker_session_return<{threshold:g}'
    if mode == 'flip-short-long-below-vwap' and flipped_short_to_long and below_vwap:
        return 'profile_short_to_long_below_vwap'
    if mode == 'flip-short-long-btc-not-bull' and flipped_short_to_long and btc_not_bullish:
        return 'profile_short_to_long_btc_not_bull'
    if mode == 'below-vwap-btc-bear' and below_vwap and btc_bearish:
        return 'below_vwap_btc_bearish'
    if mode == 'ticker-negative-below-vwap-btc-not-bull' and ticker_negative and below_vwap and btc_not_bullish:
        return f'ticker_session_return<{threshold:g}_below_vwap_btc_not_bull'
    if mode == 'ticker-negative-below-vwap-miner-not-confirmed' and ticker_negative and below_vwap and miner_not_confirmed:
        return f'ticker_session_return<{threshold:g}_below_vwap_miner_not_confirmed'
    if mode == 'ticker-negative-below-vwap-not-momentum' and ticker_negative and below_vwap and setup != 'momentum_breakout':
        return f'ticker_session_return<{threshold:g}_below_vwap_not_momentum'
    if mode == 'ticker-negative-below-vwap-stock-lagging-btc' and ticker_negative and below_vwap and stock_lagging_btc:
        return f'ticker_session_return<{threshold:g}_below_vwap_stock_lagging_btc'
    if mode == 'ticker-negative-below-vwap-weak-flow' and ticker_negative and below_vwap and weak_long_flow:
        return f'ticker_session_return<{threshold:g}_below_vwap_weak_flow'
    if mode == 'ticker-negative-below-vwap-lower-half-range' and ticker_negative and below_vwap and lower_half_range:
        return f'ticker_session_return<{threshold:g}_below_vwap_lower_half_range'
    if (
        mode == 'ticker-negative-below-vwap-btc-not-bull-stock-lagging'
        and ticker_negative and below_vwap and btc_not_bullish and stock_lagging_btc
    ):
        return f'ticker_session_return<{threshold:g}_below_vwap_btc_not_bull_stock_lagging_btc'
    if (
        mode == 'ticker-negative-below-vwap-btc-not-bull-weak-flow'
        and ticker_negative and below_vwap and btc_not_bullish and weak_long_flow
    ):
        return f'ticker_session_return<{threshold:g}_below_vwap_btc_not_bull_weak_flow'
    if (
        mode == 'ticker-negative-below-vwap-btc-not-bull-lower-half-range'
        and ticker_negative and below_vwap and btc_not_bullish and lower_half_range
    ):
        return f'ticker_session_return<{threshold:g}_below_vwap_btc_not_bull_lower_half_range'
    if (
        mode == 'ticker-negative-below-vwap-btc-not-bull-not-momentum'
        and ticker_negative and below_vwap and btc_not_bullish and setup != 'momentum_breakout'
    ):
        return f'ticker_session_return<{threshold:g}_below_vwap_btc_not_bull_not_momentum'
    if (
        mode == 'ticker-negative-below-vwap-btc-not-bull-btc-relative'
        and ticker_negative and below_vwap and btc_not_bullish and setup == 'btc_relative_strength'
    ):
        return f'ticker_session_return<{threshold:g}_below_vwap_btc_not_bull_btc_relative_strength'
    if mode in ('btc-relative-normal-long-followthrough', 'brs-follow') and setup == 'btc_relative_strength' and session_phase == 'normal':
        weak = []
        if not above_vwap:
            weak.append('not_above_vwap')
        if btc_not_bullish:
            weak.append('btc_not_bullish')
        if miner_not_confirmed:
            weak.append('miner_not_confirmed')
        if stock_lagging_btc:
            weak.append('stock_lagging_btc')
        if weak_long_flow:
            weak.append('weak_flow')
        if not followthrough_momentum:
            weak.append('momentum_not_confirmed')
        if btc_chop_penalty:
            weak.append('btc_chop_penalty')
        if weak:
            return 'btc_relative_normal_long_weak_followthrough:' + ','.join(weak)
    return None


def _entry_warning_gate_reason(args, ticker: str, sig: dict, ind: dict) -> Optional[str]:
    mode = getattr(args, 'entry_warning_gate_mode', 'off') or 'off'
    if mode == 'off':
        return None
    scoped = {
        t.strip().upper()
        for t in str(getattr(args, 'entry_warning_gate_tickers', '') or '').split(',')
        if t.strip()
    }
    if scoped and ticker.upper() not in scoped:
        return None
    setup = getattr(args, 'entry_warning_gate_setup', 'all') or 'all'
    if setup != 'all' and sig.get('setup_type') != setup:
        return None
    session = getattr(args, 'entry_warning_gate_session_phase', 'all') or 'all'
    if session != 'all' and sig.get('session_phase') != session:
        return None
    side = sig.get('side')
    if side not in ('LONG', 'SHORT'):
        return None
    side_mult = 1.0 if side == 'LONG' else -1.0
    stock_mom_60 = _float((ind or {}).get('mom_60s'))
    rel_60 = _float(((sig.get('relative_strength') or {}).get('stock_minus_btc_implied_60s')))
    if stock_mom_60 is None or rel_60 is None:
        return None
    signed_mom = side_mult * stock_mom_60
    signed_rel = side_mult * rel_60
    mom_threshold = float(getattr(args, 'entry_warning_gate_stock_mom60_threshold_pct', -0.10) or -0.10)
    rel_threshold = float(getattr(args, 'entry_warning_gate_rel60_threshold_pct', -0.10) or -0.10)
    if mode == 'mom60_rel60_veto' and signed_mom <= mom_threshold and signed_rel <= rel_threshold:
        return f'mom60_rel60_veto:mom={signed_mom:+.3f}_rel={signed_rel:+.3f}'
    if mode == 'ticker_chop_veto':
        chop_range = _float((ind or {}).get('chop_range_180s_pct'))
        chop_eff = _float((ind or {}).get('chop_efficiency_180s'))
        chop_flips = _float((ind or {}).get('chop_flips_180s'))
        score = _float(sig.get('score'))
        range_threshold = float(getattr(args, 'entry_warning_gate_chop_range_threshold_pct', 0.75) or 0.75)
        eff_threshold = float(getattr(args, 'entry_warning_gate_chop_efficiency_threshold', 0.20) or 0.20)
        flips_threshold = float(getattr(args, 'entry_warning_gate_chop_flips_threshold', 10) or 10)
        max_score = float(getattr(args, 'entry_warning_gate_max_score', 999.0) or 999.0)
        if (
            chop_range is not None and chop_eff is not None and chop_flips is not None
            and chop_range >= range_threshold
            and chop_eff <= eff_threshold
            and chop_flips >= flips_threshold
            and (score is None or score <= max_score)
        ):
            return (
                f'ticker_chop_veto:range={chop_range:.3f}_eff={chop_eff:.3f}'
                f'_flips={chop_flips:.0f}_score={score if score is not None else "na"}'
            )
    return None


def _state(symbol: str) -> ws_scalp.SymbolState:
    st = ws_scalp.SymbolState(symbol=symbol)
    st._replay_bar_cache = {
        'o': [],
        'h': [],
        'l': [],
        'c': [],
        'v': [],
        'buy_v': [],
        'sell_v': [],
        'n': [],
        'ret_sq': [],
        'idx': -1,
        'high_max': deque(),
        'low_min': deque(),
        'cum': {
            'c': [0.0],
            'c2': [0.0],
            'v': [0.0],
            'v2': [0.0],
            'buy_v': [0.0],
            'sell_v': [0.0],
            'n': [0.0],
            'n2': [0.0],
            'ret_sq': [0.0],
            'ret_sq_count': [0],
        },
    }
    st._replay_quote_spreads = deque(maxlen=300)
    return st


def _append_replay_bar_cache(st: ws_scalp.SymbolState, bar: dict) -> None:
    cache = getattr(st, '_replay_bar_cache', None)
    if not isinstance(cache, dict):
        return
    maxlen = ws_scalp.RING_SECONDS
    idx = int(cache.get('idx') or -1) + 1
    cache['idx'] = idx
    closes = cache.setdefault('c', [])
    prev_close = closes[-1] if closes else None
    for key in ('o', 'h', 'l', 'c', 'v', 'buy_v', 'sell_v', 'n'):
        arr = cache.setdefault(key, [])
        arr.append(bar.get(key))
        if len(arr) > maxlen:
            del arr[0]
    ret_sq = cache.setdefault('ret_sq', [])
    cur_close = bar.get('c')
    if prev_close:
        ret = (cur_close - prev_close) / prev_close * 100
        ret_sq_value = ret * ret
        ret_sq.append(ret_sq_value)
    else:
        ret_sq_value = None
        ret_sq.append(None)
    if len(ret_sq) > maxlen:
        del ret_sq[0]
    cum = cache.setdefault('cum', {})
    for key in ('c', 'v', 'buy_v', 'sell_v', 'n'):
        value = float(bar.get(key) or 0.0)
        cum.setdefault(key, [0.0]).append(cum.setdefault(key, [0.0])[-1] + value)
    for src, dst in (('c', 'c2'), ('v', 'v2'), ('n', 'n2')):
        value = float(bar.get(src) or 0.0)
        cum.setdefault(dst, [0.0]).append(cum.setdefault(dst, [0.0])[-1] + value * value)
    cum.setdefault('ret_sq', [0.0]).append(cum.setdefault('ret_sq', [0.0])[-1] + float(ret_sq_value or 0.0))
    cum.setdefault('ret_sq_count', [0]).append(cum.setdefault('ret_sq_count', [0])[-1] + (1 if ret_sq_value is not None else 0))
    high_max = cache.setdefault('high_max', deque())
    low_min = cache.setdefault('low_min', deque())
    high = bar.get('h')
    low = bar.get('l')
    if high is not None:
        while high_max and high_max[-1][1] <= high:
            high_max.pop()
        high_max.append((idx, high))
    if low is not None:
        while low_min and low_min[-1][1] >= low:
            low_min.pop()
        low_min.append((idx, low))
    min_idx = idx - maxlen + 1
    while high_max and high_max[0][0] < min_idx:
        high_max.popleft()
    while low_min and low_min[0][0] < min_idx:
        low_min.popleft()


def _append_replay_quote_spread_cache(st: ws_scalp.SymbolState, ts_ms: int) -> None:
    cache = getattr(st, '_replay_quote_spreads', None)
    if cache is None:
        return
    bid = st.best_bid
    ask = st.best_ask
    spread = None
    if bid is not None and ask is not None:
        try:
            bid_f = float(bid)
            ask_f = float(ask)
            mid = (bid_f + ask_f) / 2.0
            if mid > 0 and ask_f > bid_f:
                spread = (ask_f - bid_f) / mid * 100.0
        except Exception:
            spread = None
    cache.append((ts_ms, spread))


def _feed_stock_trade(st: ws_scalp.SymbolState, symbol: str, row: dict) -> None:
    price = _float(row.get('p'))
    if price is None or price <= 0:
        return
    size = int(row.get('s') or 0)
    ts_ms = int(row['t'])
    meta = {
        'trade_exchange': row.get('x'),
        'trade_conditions': ws_scalp._clean_codes(row.get('c') or []),
        'trade_tape': row.get('z'),
        'trade_id': row.get('i'),
    }
    if any(c in ('B', 'W', 'Z') for c in meta['trade_conditions']):
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
        et_date = datetime.fromtimestamp(ts_ms / 1000, ET).date().isoformat()
        if st.session_date != et_date:
            st.session_date = et_date
            st.session_pv_sum = 0.0
            st.session_v_sum = 0.0
        st.session_pv_sum += price * size
        st.session_v_sum += size


def _feed_stock_quote(st: ws_scalp.SymbolState, row: dict) -> None:
    ts_ms = int(row['t'])
    with st.lock:
        bp = _float(row.get('bp'))
        ap = _float(row.get('ap'))
        bs = row.get('bs')
        ask_size = row.get('as')
        if bp is not None:
            st.best_bid = bp
        if ap is not None:
            st.best_ask = ap
        if bs is not None:
            st.bid_size = int(bs)
        if ask_size is not None:
            st.ask_size = int(ask_size)
        quote_meta = {
            'bid_exchange': row.get('bx'),
            'ask_exchange': row.get('ax'),
            'quote_conditions': ws_scalp._clean_codes(row.get('c') or []),
            'quote_tape': row.get('z'),
        }
        st.last_quote_ts_ms = ts_ms
        st.last_quote_bid_exchange = quote_meta.get('bid_exchange')
        st.last_quote_ask_exchange = quote_meta.get('ask_exchange')
        st.last_quote_conditions = quote_meta.get('quote_conditions') or []
        st.last_quote_tape = quote_meta.get('quote_tape')
        bid_sz = st.bid_size or 0
        ask_sz = st.ask_size or 0
        total = bid_sz + ask_sz
        imb = round((bid_sz - ask_sz) / total, 3) if total > 0 else None
        st.quote_history.append((ts_ms, st.best_bid, st.best_ask, bid_sz, ask_sz, imb, quote_meta))
        _append_replay_quote_spread_cache(st, ts_ms)


def _feed_btc_synth(st: ws_scalp.SymbolState, row: dict) -> None:
    price = _float(row.get('p'))
    if price is None:
        return
    ts_ms = int(row['t'])
    meta = {'trade_exchange': 'alpaca_crypto_bar_synth', 'trade_conditions': ['synthetic_1m_linear']}
    with st.lock:
        side = 0
        if st.last_trade_price is not None:
            side = 1 if price > st.last_trade_price else (-1 if price < st.last_trade_price else 0)
        st.trades.append((ts_ms, price, 0, side, meta))
        st.pending_trades.append((ts_ms, price, 0, side, meta))
        st.last_trade_price = price
        st.last_trade_size = 0
        st.last_trade_ts_ms = ts_ms
        st.last_trade_exchange = meta['trade_exchange']
        st.last_trade_conditions = meta['trade_conditions']
        spread = max(0.01, price * 0.00005)
        st.best_bid = round(price - spread / 2, 2)
        st.best_ask = round(price + spread / 2, 2)
        st.bid_size = 1
        st.ask_size = 1
        st.last_quote_ts_ms = ts_ms
        st.last_quote_bid_exchange = 'synthetic'
        st.last_quote_ask_exchange = 'synthetic'
        st.quote_history.append((ts_ms, st.best_bid, st.best_ask, 1, 1, 0.0, {
            'bid_exchange': 'synthetic',
            'ask_exchange': 'synthetic',
            'quote_conditions': ['synthetic_1m_linear'],
        }))
        _append_replay_quote_spread_cache(st, ts_ms)


def _roll_bar(st: ws_scalp.SymbolState, sec: int) -> None:
    sec_start_ms = sec * 1000
    sec_end_ms = sec_start_ms + 1000
    with st.lock:
        o = h = l = c = None
        v = buy_v = sell_v = n = 0
        while st.pending_trades and st.pending_trades[0][0] < sec_end_ms:
            ts_ms, price, size, side, _meta = ws_scalp._trade_parts(st.pending_trades.popleft())
            if ts_ms < sec_start_ms:
                continue
            if o is None:
                o = h = l = price
            h = max(h, price)
            l = min(l, price)
            c = price
            v += int(size or 0)
            n += 1
            if side > 0:
                buy_v += int(size or 0)
            elif side < 0:
                sell_v += int(size or 0)
        if o is None:
            if st.bars_1s:
                prev = st.bars_1s[-1]
                o = h = l = c = prev['c']
            else:
                return
        bar = {
            'ts_s': sec,
            'o': o, 'h': h, 'l': l, 'c': c,
            'v': v, 'buy_v': buy_v, 'sell_v': sell_v, 'n': n,
        }
        st.bars_1s.append(bar)
        _append_replay_bar_cache(st, bar)
        if sec % 60 == 59 and len(st.bars_1s) >= 60:
            last60 = list(st.bars_1s)[-60:]
            bv = sum(b['buy_v'] for b in last60)
            sv = sum(b['sell_v'] for b in last60)
            total = bv + sv
            if total > 0:
                st.flow_history.append({
                    'ts': sec - 59,
                    'buy_pct': round(100 * bv / total, 1),
                    'buy_v': bv,
                    'sell_v': sv,
                })


def _float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _step2_brs_long_btc_negative_mom60_reason(sig: dict, btc_ind: Optional[dict] = None) -> Optional[str]:
    """Mock-engine parity guard for Step 2 signal-scan live mode."""
    if sig.get('side') != 'LONG' or sig.get('setup_type') != 'btc_relative_strength':
        return None
    if sig.get('conviction') == 'HIGH':
        return None
    btc_context = sig.get('btc_context') if isinstance(sig.get('btc_context'), dict) else {}
    btc_mom_60 = _float(sig.get('btc_mom'))
    if btc_mom_60 is None:
        btc_mom_60 = _float(btc_context.get('mom_60s'))
    if btc_mom_60 is None and isinstance(btc_ind, dict):
        btc_mom_60 = _float(btc_ind.get('mom_60s'))
    if btc_mom_60 is not None and btc_mom_60 < 0:
        return f'step2_brs_long_btc_negative_mom60:{btc_mom_60:.6f}'
    return None


def _tail_sum(values: list, n: int) -> Optional[float]:
    if len(values) < n:
        return None
    return sum(values[-n:])


def _replay_compute_indicators(st: ws_scalp.SymbolState) -> dict:
    cache = getattr(st, '_replay_bar_cache', None)
    if not isinstance(cache, dict):
        return ws_scalp.compute_indicators(st)
    with st.lock:
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
        quote_spreads = list(getattr(st, '_replay_quote_spreads', []) or [])
        pv_sum = st.session_pv_sum
        v_sum = st.session_v_sum
    closes = cache.get('c') or []
    vols = cache.get('v') or []
    buy_v = cache.get('buy_v') or []
    sell_v = cache.get('sell_v') or []
    n_arr = cache.get('n') or []
    ret_sq = cache.get('ret_sq') or []
    highs = cache.get('h') or []
    lows = cache.get('l') or []
    opens = cache.get('o') or []
    cum = cache.get('cum') or {}
    if len(closes) < 5 or price is None:
        return {'price': price, 'bars': len(closes), 'ready': False, 'symbol': symbol}

    def cum_sum(key, start, end):
        arr = cum.get(key) or []
        if start < 0 or end < start or end >= len(arr):
            return None
        return arr[end] - arr[start]

    def pct_change(arr, n):
        if len(arr) < n + 1 or arr[-n - 1] == 0:
            return None
        return round((arr[-1] - arr[-n - 1]) / arr[-n - 1] * 100, 3)

    def ema(arr, span):
        if len(arr) < span:
            return None
        k = 2 / (span + 1)
        e = arr[-span]
        for v in arr[-span + 1:]:
            e = v * k + e * (1 - k)
        return round(e, 4)

    def zscore(arr, window):
        if len(arr) < window + 1:
            return None
        cur = arr[-1]
        total = len(arr)
        key = 'v' if arr is vols else ('n' if arr is n_arr else None)
        if key:
            full_total = len(cum.get(key) or []) - 1
            recent_sum = cum_sum(key, full_total - window - 1, full_total - 1)
            recent_sq_sum = cum_sum(f'{key}2', full_total - window - 1, full_total - 1)
            if recent_sum is None or recent_sq_sum is None:
                return None
            mean = recent_sum / window
            var = (recent_sq_sum / window) - (mean * mean)
            if var < 0 and var > -1e-9:
                var = 0.0
        else:
            recent = arr[-window - 1:-1]
            mean = sum(recent) / len(recent)
            var = sum((x - mean) ** 2 for x in recent) / len(recent)
        sd = var ** 0.5
        return round((cur - mean) / sd, 2) if sd > 0 else 0

    def realized_vol_pct(window):
        if len(ret_sq) < window:
            return None
        full_total = len(cum.get('ret_sq') or []) - 1
        vals_sum = cum_sum('ret_sq', full_total - window, full_total)
        counts = cum_sum('ret_sq_count', full_total - window, full_total)
        if vals_sum is None or not counts:
            return None
        return round((vals_sum / counts) ** 0.5, 4)

    def chop_metrics(window):
        if len(closes) < window + 1 or price is None:
            return {}
        vals = closes[-window - 1:]
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

    def flow(window):
        if len(buy_v) < window:
            return None
        full_total = len(cum.get('buy_v') or []) - 1
        b = cum_sum('buy_v', full_total - window, full_total)
        s = cum_sum('sell_v', full_total - window, full_total)
        if b is None or s is None:
            return None
        total = b + s
        if total == 0:
            return {'buy_pct': None, 'ratio': None}
        return {'buy_pct': round(100 * b / total, 1), 'ratio': round(b / max(s, 1), 2)}

    def flow_slice(start, end):
        if len(buy_v) < abs(start):
            return None
        full_total = len(cum.get('buy_v') or []) - 1
        abs_start = full_total + start
        abs_end = full_total + end
        b = cum_sum('buy_v', abs_start, abs_end)
        s = cum_sum('sell_v', abs_start, abs_end)
        if b is None or s is None:
            return None
        total = b + s
        if total == 0:
            return {'buy_pct': None, 'ratio': None}
        return {'buy_pct': round(100 * b / total, 1), 'ratio': round(b / max(s, 1), 2)}

    vwap = round(pv_sum / v_sum, 4) if v_sum > 0 else None
    if len(closes) >= 300:
        full_total = len(cum.get('c') or []) - 1
        recent_sum = cum_sum('c', full_total - 300, full_total)
        recent_sq_sum = cum_sum('c2', full_total - 300, full_total)
        if recent_sum is not None and recent_sq_sum is not None:
            m = recent_sum / 300
            var = (recent_sq_sum / 300) - (m * m)
            if var < 0 and var > -1e-9:
                var = 0.0
            sd = var ** 0.5
        else:
            recent = closes[-300:]
            m = sum(recent) / len(recent)
            sd = (sum((x - m) ** 2 for x in recent) / len(recent)) ** 0.5
        if price:
            sd = max(sd, price * 0.0005)
    else:
        sd = 0
    if vwap and sd > 0:
        raw = max(-5.0, min(5.0, (price - vwap) / sd))
        vwap_dist_sigma = round(raw, 2)
    else:
        vwap_dist_sigma = None

    vol_z = zscore(vols, 60)
    tick_z = zscore(n_arr, 30)
    session_open = opens[0] if opens else None
    high_max = cache.get('high_max')
    low_min = cache.get('low_min')
    session_high = high_max[0][1] if high_max else (max(highs) if highs else None)
    session_low = low_min[0][1] if low_min else (min(lows) if lows else None)
    opening_5m_high = max(highs[:300], default=None)
    opening_5m_low = min(lows[:300], default=None)
    opening_15m_high = max(highs[:900], default=None)
    opening_15m_low = min(lows[:900], default=None)
    session_range = (session_high - session_low) if session_high is not None and session_low is not None else None
    session_range_pos = round((price - session_low) / session_range, 3) if session_range and session_range > 0 and price is not None else None

    now_ms = int(ws_scalp.time.time() * 1000)
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
    quote_imbalance_delta_5s = round(quote_imbalance - quote_imbalance_5s_ago, 3) if quote_imbalance is not None and quote_imbalance_5s_ago is not None else None

    spread_pct = None
    if best_bid is not None and best_ask is not None and price:
        spread_pct = round((float(best_ask) - float(best_bid)) / price * 100, 4)
    spread_cutoff = now_ms - max(1, ws_scalp.ROLLING_SPREAD_WINDOW_SEC) * 1000
    rolling_spreads = [
        spread for qts, spread in quote_spreads
        if qts is not None and qts >= spread_cutoff and spread is not None
    ]
    rolling_spread_median = None
    rolling_spread_p95 = None
    if rolling_spreads:
        sorted_spreads = sorted(float(v) for v in rolling_spreads if v is not None)
        if sorted_spreads:
            mid = len(sorted_spreads) // 2
            if len(sorted_spreads) % 2:
                rolling_spread_median = sorted_spreads[mid]
            else:
                rolling_spread_median = (sorted_spreads[mid - 1] + sorted_spreads[mid]) / 2
            idx = int(round((len(sorted_spreads) - 1) * 0.95))
            rolling_spread_p95 = sorted_spreads[idx]
    spread_vs_rolling_median = None
    spread_abnormal = False
    if spread_pct is not None and rolling_spread_median and rolling_spread_median > 0:
        spread_vs_rolling_median = round(float(spread_pct) / rolling_spread_median, 3)
        spread_abnormal = spread_vs_rolling_median >= ws_scalp.ROLLING_SPREAD_ABNORMAL_MULTIPLE
    quote_state = ws_scalp._quote_state(best_bid, best_ask, price, last_quote_age_sec, spread_pct)
    condition_quality = ws_scalp._condition_quality(
        last_trade_conditions,
        last_quote_conditions,
        last_trade_exchange,
        last_quote_bid_exchange,
        last_quote_ask_exchange,
    )
    session_elapsed_sec = len(closes)
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

    ema5 = ema(closes, 5)
    ema15 = ema(closes, 15)
    ema60 = ema(closes, 60)
    stack = None
    if ema5 and ema15 and ema60:
        if ema5 > ema15 > ema60:
            stack = 'bull'
        elif ema5 < ema15 < ema60:
            stack = 'bear'
        else:
            stack = 'mixed'
    chop_180s = chop_metrics(180)
    chop_300s = chop_metrics(300)

    return {
        'ready': True, 'symbol': symbol, 'price': price, 'bars': len(closes),
        'session_elapsed_sec': session_elapsed_sec, 'time_of_day_bucket': tod_bucket,
        'tod_activity_score': activity_score, 'tod_activity_label': tod_activity_label,
        'tod_activity_method': 'same_session_rolling_zscore',
        'last_trade_ts_ms': last_trade_ts_ms, 'last_quote_ts_ms': last_quote_ts_ms,
        'last_trade_age_sec': last_trade_age_sec, 'last_quote_age_sec': last_quote_age_sec,
        'mom_5s': pct_change(closes, 5), 'mom_15s': pct_change(closes, 15),
        'mom_30s': pct_change(closes, 30), 'mom_60s': pct_change(closes, 60),
        'mom_180s': pct_change(closes, 180),
        'ema_5s': ema5, 'ema_15s': ema15, 'ema_60s': ema60, 'ema_stack': stack,
        'vwap': vwap, 'vwap_dist': round(price - vwap, 4) if vwap else None,
        'vwap_dist_sigma': vwap_dist_sigma,
        'flow_10s': flow_10s, 'flow_30s': flow_30s, 'flow_30s_prev': flow_30s_prev,
        'flow_30s_delta': flow_30s_delta, 'flow_120s': flow(120),
        'realized_vol_60s_pct': realized_vol_pct(60), 'realized_vol_180s_pct': realized_vol_pct(180),
        **chop_180s, **chop_300s,
        'vol_z_60s': vol_z, 'tick_z_30s': tick_z,
        'vol_10s': cum_sum('v', (len(cum.get('v') or []) - 1) - 10, len(cum.get('v') or []) - 1) if len(vols) >= 10 else None,
        'vol_60s': cum_sum('v', (len(cum.get('v') or []) - 1) - 60, len(cum.get('v') or []) - 1) if len(vols) >= 60 else None,
        'ticks_10s': cum_sum('n', (len(cum.get('n') or []) - 1) - 10, len(cum.get('n') or []) - 1) if len(n_arr) >= 10 else None,
        'ticks_30s': cum_sum('n', (len(cum.get('n') or []) - 1) - 30, len(cum.get('n') or []) - 1) if len(n_arr) >= 30 else None,
        'session_open': session_open, 'session_high': session_high, 'session_low': session_low,
        'opening_5m_high': opening_5m_high, 'opening_5m_low': opening_5m_low,
        'opening_15m_high': opening_15m_high, 'opening_15m_low': opening_15m_low,
        'opening_5m_break_state': ws_scalp._opening_break_state(price, opening_5m_high, opening_5m_low),
        'opening_15m_break_state': ws_scalp._opening_break_state(price, opening_15m_high, opening_15m_low),
        'session_range_pct': round(session_range / price * 100, 3) if session_range and price else None,
        'session_range_pos': session_range_pos,
        'session_return_pct': round((price - session_open) / session_open * 100, 3) if session_open and price else None,
        'best_bid': best_bid, 'best_ask': best_ask, 'bid_size': bid_size, 'ask_size': ask_size,
        'spread_pct': spread_pct, 'spread_vs_rolling_median': spread_vs_rolling_median,
        'spread_abnormal': spread_abnormal, 'rolling_spread_median_pct': rolling_spread_median,
        'rolling_spread_p95_pct': rolling_spread_p95,
        'quote_imbalance': quote_imbalance, 'quote_imbalance_delta_5s': quote_imbalance_delta_5s,
        'quote_state': quote_state, 'condition_quality': condition_quality,
        'last_trade_exchange': last_trade_exchange, 'last_trade_conditions': last_trade_conditions,
        'last_trade_tape': last_trade_tape, 'last_quote_bid_exchange': last_quote_bid_exchange,
        'last_quote_ask_exchange': last_quote_ask_exchange,
        'last_quote_conditions': last_quote_conditions, 'last_quote_tape': last_quote_tape,
    }


def _setup_state_gate(setup_states: dict, ticker: str, sig: dict, ind: dict, now: float) -> bool:
    if not ws_scalp.STATE_MACHINE_ENABLED:
        return True
    setup = sig.get('setup_type')
    if setup not in ('flow_exhaustion_fade', 'vwap_reclaim_breakdown'):
        return True
    side = sig.get('side')
    key = f'{ticker}:{side}:{setup}'
    state = setup_states.get(key)
    trigger = {
        'flow_exhaustion_fade': sig.get('signal_quality', {}).get('flow_fade_confirmed'),
        'vwap_reclaim_breakdown': abs(ind.get('vwap_dist_sigma') or 0) <= 1.0,
    }.get(setup)
    if not trigger:
        setup_states.pop(key, None)
        return False
    if not state:
        setup_states[key] = {'armed_at': now, 'price': sig.get('price'), 'score': sig.get('score')}
        return False
    age = now - float(state.get('armed_at', now))
    if age > ws_scalp.STATE_MACHINE_MAX_CONFIRM_SEC:
        setup_states[key] = {'armed_at': now, 'price': sig.get('price'), 'score': sig.get('score')}
        return False
    if age < ws_scalp.STATE_MACHINE_MIN_CONFIRM_SEC:
        return False
    sig['setup_state'] = {
        'confirmed': True,
        'armed_age_sec': round(age, 2),
        'armed_price': state.get('price'),
        'armed_score': state.get('score'),
    }
    setup_states.pop(key, None)
    return True


def _decorate_signal(sig: dict, st: ws_scalp.SymbolState, ind: dict, include_shadow: bool = True) -> dict:
    tkr = sig['ticker']
    brk = ws_scalp.SCALP_TICKER_BRACKETS.get(
        tkr, {'sl': ws_scalp.SCALP_DEFAULT_SL, 'tp': ws_scalp.SCALP_DEFAULT_TP}
    )
    price = float(sig.get('price') or 0)
    if sig['side'] == 'LONG':
        sl_raw = round(price * (1 - brk['sl']), 4)
        tp_raw = round(price * (1 + brk['tp']), 4)
    else:
        sl_raw = round(price * (1 + brk['sl']), 4)
        tp_raw = round(price * (1 - brk['tp']), 4)
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
    sig['entry_chop_metrics'] = {
        'range_180s_pct': ind.get('chop_range_180s_pct'),
        'efficiency_180s': ind.get('chop_efficiency_180s'),
        'flips_180s': ind.get('chop_flips_180s'),
        'range_300s_pct': ind.get('chop_range_300s_pct'),
        'efficiency_300s': ind.get('chop_efficiency_300s'),
        'flips_300s': ind.get('chop_flips_300s'),
    }
    if sig.get('best_bid') is not None and sig.get('best_ask') is not None:
        sig['spread'] = round(sig['best_ask'] - sig['best_bid'], 4)
        sig['mid_price'] = round((sig['best_ask'] + sig['best_bid']) / 2, 4)
    sig['execution_quality'] = ws_scalp.execution_quality(sig)
    sig.setdefault('signal_quality', {}).setdefault('score_model', {})['execution_score'] = sig['execution_quality']['score']
    sig['signal_quality']['score_model']['execution_reasons'] = sig['execution_quality'].get('reasons', [])
    if include_shadow:
        sig['shadow_variants'] = ws_scalp.shadow_variants(sig)
    return sig


def _take_profit_multiplier(sig: dict, args=None) -> float:
    if args is None:
        return 1.0
    mult = 1.0
    setup = sig.get('setup_type')
    if setup == 'btc_relative_strength':
        mult *= float(getattr(args, 'brs_tp_multiplier', 1.0) or 1.0)
    elif setup == 'momentum_breakout':
        mult *= float(getattr(args, 'momentum_tp_multiplier', 1.0) or 1.0)
    if sig.get('conviction') == 'HIGH':
        mult *= float(getattr(args, 'high_conviction_tp_multiplier', 1.0) or 1.0)
    return max(0.01, mult)


def _chop_bracket_applies(args, ticker: str, sig: dict) -> Optional[dict]:
    if args is None or (getattr(args, 'chop_bracket_mode', 'off') or 'off') == 'off':
        return None
    scoped = {
        t.strip().upper()
        for t in str(getattr(args, 'chop_bracket_tickers', '') or '').split(',')
        if t.strip()
    }
    if scoped and ticker.upper() not in scoped:
        return None
    metrics = sig.get('entry_chop_metrics') or {}
    chop_range = _float(metrics.get('range_180s_pct'))
    chop_eff = _float(metrics.get('efficiency_180s'))
    chop_flips = _float(metrics.get('flips_180s'))
    score = _float(sig.get('score'))
    range_threshold = float(getattr(args, 'chop_bracket_range_threshold_pct', 0.75) or 0.75)
    eff_threshold = float(getattr(args, 'chop_bracket_efficiency_threshold', 0.20) or 0.20)
    flips_threshold = float(getattr(args, 'chop_bracket_flips_threshold', 10) or 10)
    max_score = float(getattr(args, 'chop_bracket_max_score', 999.0) or 999.0)
    if (
        chop_range is not None and chop_eff is not None and chop_flips is not None
        and chop_range >= range_threshold
        and chop_eff <= eff_threshold
        and chop_flips >= flips_threshold
        and (score is None or score <= max_score)
    ):
        return {
            'range_180s_pct': round(chop_range, 6),
            'efficiency_180s': round(chop_eff, 6),
            'flips_180s': round(chop_flips, 6),
            'score': None if score is None else round(score, 6),
        }
    return None


def _open_position(sig: dict, balance: float, args=None) -> Position:
    tkr = sig['ticker']
    signal_price = float(sig['price'])
    price = signal_price
    bracket_price = signal_price
    entry_ts = int(sig['ts'])
    active_ts = entry_ts
    if args is not None and getattr(args, 'step2_latency_mode', 'off') != 'off':
        model = getattr(args, '_step2_latency_model', None) or step2_latency_model.load_model(
            getattr(args, 'step2_latency_model', None)
        )
        setattr(args, '_step2_latency_model', model)
        pct = getattr(args, 'step2_latency_percentile', 'p75')
        delay_ms = step2_latency_model.latency_ms(model, tkr, sig.get('side'), 'entry_delay_ms', pct, context=sig)
        bps = step2_latency_model.slippage_bps(model, tkr, sig.get('side'), 'entry_slippage_bps', 'p50', context=sig)
        active_ts = entry_ts + int(round(delay_ms / 1000.0))
        if getattr(args, 'step2_latency_mode', 'off') in ('entry', 'entry-exit'):
            price = float(step2_latency_model.apply_slippage(price, sig.get('side'), bps, 'entry'))
            sig['latency_model'] = {
                'mode': getattr(args, 'step2_latency_mode', 'off'),
                'model_path': os.path.abspath(getattr(args, 'step2_latency_model', step2_latency_model.DEFAULT_MODEL_PATH)),
                'entry_delay_ms': delay_ms,
                'entry_slippage_bps': bps,
                'entry_signal_price': sig.get('price'),
                'entry_modeled_price': round(price, 4),
                'entry_active_ts': active_ts,
                'percentile': pct,
            }
            sig['price'] = price
    cfg = TICKER_CFG.get(tkr, {'sl': 0.05, 'tp': 0.01})
    sl_pct = float(cfg.get('sl', sig.get('sl_pct') or 0.05))
    tp_pct = float(cfg.get('tp', sig.get('tp_pct') or 0.01))
    fixed_sl = getattr(args, 'fixed_sl_pct', None) if args is not None else None
    fixed_tp = getattr(args, 'fixed_tp_pct', None) if args is not None else None
    ticker_fixed_tp, ticker_fixed_sl = _fixed_ticker_bracket(args, tkr)
    if ticker_fixed_tp is not None:
        fixed_tp = ticker_fixed_tp
    if ticker_fixed_sl is not None:
        fixed_sl = ticker_fixed_sl
    if fixed_sl is not None:
        sl_pct = float(fixed_sl)
        sig['fixed_sl_pct'] = round(sl_pct, 6)
    if fixed_tp is not None:
        tp_pct = float(fixed_tp)
        sig['fixed_tp_pct'] = round(tp_pct, 6)
    chop_policy = _chop_bracket_applies(args, tkr, sig)
    if chop_policy is not None:
        sl_pct = float(getattr(args, 'chop_bracket_sl_pct', 0.0010) or 0.0010)
        tp_pct = float(getattr(args, 'chop_bracket_tp_pct', 0.0010) or 0.0010)
        sig['chop_bracket_policy'] = {
            'mode': getattr(args, 'chop_bracket_mode', 'tiny'),
            'sl_pct': round(sl_pct, 6),
            'tp_pct': round(tp_pct, 6),
            'trigger': chop_policy,
        }
    tp_multiplier = _take_profit_multiplier(sig, args)
    tp_pct *= tp_multiplier
    if abs(tp_multiplier - 1.0) > 1e-12:
        sig['tp_multiplier'] = round(tp_multiplier, 6)
        sig.setdefault('scoring_profile', {})['tp_multiplier'] = round(tp_multiplier, 6)
    side = sig['side']
    bracket_plan = execution_action_engine.plan_brackets(side, bracket_price, sl_pct, tp_pct)
    sl = bracket_plan['sl']
    tp = bracket_plan['tp']
    sig['entry_action_bracket_plan'] = bracket_plan
    alloc = round(balance * TRADE_SIZE_PCT / max(1, len(TICKERS)), 2)
    qty = alloc / price if price > 0 else 0
    return Position(
        ticker=tkr, side=side, entry_ts=entry_ts, entry_price=price,
        qty=qty, sl=sl, tp=tp, alloc=alloc, signal=sig,
        best_price=price, worst_price=price, active_ts=active_ts,
    )


def _entry_confirmation_enabled(args, sig: dict, ind: Optional[dict] = None, ticker: Optional[str] = None) -> bool:
    delay = int(float(getattr(args, 'entry_confirmation_delay_sec', 0.0) or 0.0))
    if delay <= 0 or (getattr(args, 'entry_confirmation_mode', 'off') or 'off') == 'off':
        return False
    setup = getattr(args, 'entry_confirmation_setup', 'all') or 'all'
    if setup != 'all' and sig.get('setup_type') != setup:
        return False
    session = getattr(args, 'entry_confirmation_session_phase', 'all') or 'all'
    if session != 'all' and sig.get('session_phase') != session:
        return False
    if (getattr(args, 'entry_warning_gate_action', 'veto') or 'veto') == 'confirm_only':
        return _entry_warning_gate_reason(args, ticker or sig.get('ticker') or '', sig, ind or {}) is not None
    return True


def _side_move_pct(side: str, entry_price: float, current_price: float) -> float:
    if entry_price <= 0 or current_price <= 0:
        return 0.0
    move = (current_price - entry_price) / entry_price * 100
    return -move if side == 'SHORT' else move


def _resolve_pending_entry(pending: dict, current_price: Optional[float], args,
                           current_btc_price: Optional[float] = None) -> tuple[str, dict | None]:
    sig = pending['sig']
    if current_price is None:
        return 'skip:no_price', None
    side = str(sig.get('side') or '').upper()
    start_price = float(pending.get('signal_price') or sig.get('price') or current_price)
    move = _side_move_pct(side, start_price, float(current_price))
    mode = getattr(args, 'entry_confirmation_mode', 'off') or 'off'
    fav = float(getattr(args, 'entry_confirmation_min_favorable_pct', 0.0) or 0.0)
    adv = abs(float(getattr(args, 'entry_confirmation_adverse_pct', 0.0) or 0.0))
    if mode == 'confirm':
        if move < fav:
            return 'skip:not_confirmed', None
    elif mode == 'skip_adverse':
        if move <= -adv:
            return 'skip:adverse', None
    elif mode == 'flip_adverse':
        if move <= -adv:
            sig = _json_clone(sig)
            sig['side'] = 'SHORT' if side == 'LONG' else 'LONG'
            sig['scoring_profile_original_side'] = side
            sig['reasons'] = list(sig.get('reasons') or [])
            sig['reasons'].append(f"entry_confirmation:flip_adverse move={move:+.4f}%")
        elif move < fav:
            return 'skip:not_confirmed', None
    elif mode == 'skip_path_failure':
        best = float(pending.get('best_price') or start_price)
        worst = float(pending.get('worst_price') or start_price)
        if side == 'LONG':
            mfe = max(0.0, (best - start_price) / start_price * 100.0) if start_price else 0.0
            mae = max(0.0, (start_price - worst) / start_price * 100.0) if start_price else 0.0
        else:
            mfe = max(0.0, (start_price - worst) / start_price * 100.0) if start_price else 0.0
            mae = max(0.0, (best - start_price) / start_price * 100.0) if start_price else 0.0
        edge = mfe - mae
        btc_start = float(pending.get('signal_btc_price') or 0.0)
        btc_now = float(current_btc_price or 0.0)
        btc_move = 0.0
        if btc_start > 0 and btc_now > 0:
            btc_raw = (btc_now - btc_start) / btc_start * 100.0
            btc_move = btc_raw if side == 'LONG' else -btc_raw
        rel_move = move - btc_move
        sig.setdefault('entry_confirmation', {})
        sig['entry_confirmation'].update({
            'edge_pct': round(edge, 6),
            'relative_pct': round(rel_move, 6),
            'mfe_pct': round(mfe, 6),
            'mae_pct': round(mae, 6),
        })
        edge_threshold = getattr(args, 'entry_confirmation_edge_threshold_pct', None)
        rel_threshold = getattr(args, 'entry_confirmation_rel_threshold_pct', None)
        bad_edge = edge_threshold is not None and edge <= float(edge_threshold)
        bad_rel = rel_threshold is not None and rel_move <= float(rel_threshold)
        if bad_edge and bad_rel:
            return 'skip:path_failure', None
        if move < fav:
            return 'skip:not_confirmed', None
    else:
        return 'skip:mode_off', None
    sig['price'] = float(current_price)
    sig['ts'] = int(pending.get('due_ts') or sig.get('ts') or 0)
    sig['entry_confirmation'] = {
        'mode': mode,
        'delay_sec': int(float(getattr(args, 'entry_confirmation_delay_sec', 0.0) or 0.0)),
        'signal_price': round(start_price, 4),
        'entry_price': round(float(current_price), 4),
        'move_pct': round(move, 6),
    }
    return 'enter', sig


def _flip_signal_from_position(pos: Position, price: float, now: int, reason: str = 'path_failure_flip') -> dict:
    sig = _json_clone(pos.signal or {})
    old_side = str(sig.get('side') or pos.side).upper()
    sig['side'] = 'SHORT' if old_side == 'LONG' else 'LONG'
    sig['price'] = float(price)
    sig['ts'] = int(now)
    if reason == 'giveback_exit_flip':
        sig['giveback_exit_flipped'] = True
    else:
        sig['path_failure_flipped'] = True
    sig['opportunity_id'] = f"{sig.get('opportunity_id', 'unknown')}:{reason}:{now}"
    sig['scoring_profile_original_side'] = old_side
    sig['reasons'] = list(sig.get('reasons') or [])
    sig['reasons'].append(reason)
    return sig


def _mark_position(pos: Position, price: Optional[float]) -> None:
    if price is None:
        return
    if pos.side == 'LONG':
        pos.best_price = max(pos.best_price, price)
        pos.worst_price = min(pos.worst_price, price)
    else:
        pos.best_price = min(pos.best_price, price)
        pos.worst_price = max(pos.worst_price, price)


def _position_pnl_pct(pos: Position, price: float) -> float:
    pnl_pct = (price - pos.entry_price) / pos.entry_price * 100
    return -pnl_pct if pos.side == 'SHORT' else pnl_pct


def _position_mfe_pct(pos: Position) -> float:
    return abs(pos.best_price - pos.entry_price) / pos.entry_price * 100


def _position_mae_pct(pos: Position) -> float:
    return abs(pos.worst_price - pos.entry_price) / pos.entry_price * 100


def _position_exit_mark(st, pos: Position, now: int) -> Optional[float]:
    quote_ts = int(getattr(st, 'last_quote_ts_ms', 0) or 0)
    quote_age_sec = (now - (quote_ts / 1000.0)) if quote_ts else None
    quote_is_fresh = quote_age_sec is not None and quote_age_sec <= 2.0
    if quote_is_fresh:
        if pos.side == 'LONG' and st.best_bid is not None:
            return float(st.best_bid)
        if pos.side == 'SHORT' and st.best_ask is not None:
            return float(st.best_ask)
    return st.last_trade_price


def _exit_management_value(pos: Position, args, name: str, default=None):
    profile_exit = (pos.signal or {}).get('scoring_profile_exit_management') or {}
    if name in profile_exit:
        return profile_exit.get(name)
    return getattr(args, name, default) if args is not None else default


def _path_failure_reason(pos: Position, now: int, args=None) -> Optional[str]:
    if args is None:
        return None
    after_sec = int(float(getattr(args, 'path_failure_after_sec', 0.0) or 0.0))
    action = getattr(args, 'path_failure_action', 'off') or 'off'
    if after_sec <= 0 or action == 'off':
        return None
    if (pos.signal or {}).get('path_failure_flipped') or (pos.signal or {}).get('path_failure_reduced'):
        return None
    age = now - pos.entry_ts
    if age < after_sec:
        return None
    window_sec = int(float(getattr(args, 'path_failure_window_sec', 0.0) or 0.0))
    if window_sec > 0 and age > after_sec + window_sec:
        return None
    setup = getattr(args, 'path_failure_setup', 'all') or 'all'
    if setup != 'all' and (pos.signal or {}).get('setup_type') != setup:
        return None
    session = getattr(args, 'path_failure_session_phase', 'all') or 'all'
    if session != 'all' and (pos.signal or {}).get('session_phase') != session:
        return None
    edge = _position_mfe_pct(pos) - _position_mae_pct(pos)
    threshold = float(getattr(args, 'path_failure_edge_threshold_pct', 0.0) or 0.0)
    if edge > threshold:
        return None
    rel_threshold = getattr(args, 'path_failure_rel_threshold_pct', None)
    if rel_threshold is not None:
        price = float((pos.signal or {}).get('path_failure_current_price') or 0.0)
        btc_price = float((pos.signal or {}).get('path_failure_current_btc_price') or 0.0)
        btc_entry = float((pos.signal or {}).get('path_failure_entry_btc_price') or 0.0)
        stock_move = _side_move_pct(pos.side, pos.entry_price, price) if price > 0 else 0.0
        btc_move = 0.0
        if btc_entry > 0 and btc_price > 0:
            btc_raw = (btc_price - btc_entry) / btc_entry * 100.0
            btc_move = btc_raw if pos.side == 'LONG' else -btc_raw
        rel_move = stock_move - btc_move
        pos.signal['path_failure_rel_pct'] = round(rel_move, 6)
        if rel_move > float(rel_threshold):
            return None
    if action == 'flip' and not (pos.signal or {}).get('path_failure_flipped'):
        return 'path_failure_flip'
    if action == 'reduce':
        return 'path_failure_reduce'
    return 'path_failure_exit'


def _giveback_exit_reason(pos: Position, price: float, now: int, args=None,
                          btc_price: Optional[float] = None,
                          miner_indicators: Optional[dict[str, dict]] = None) -> Optional[str]:
    if args is None:
        return None
    after_sec = int(float(_exit_management_value(pos, args, 'giveback_exit_after_sec', 0.0) or 0.0))
    action = _exit_management_value(pos, args, 'giveback_exit_action', 'off') or 'off'
    if after_sec <= 0 or action == 'off':
        return None
    if (now - pos.entry_ts) < after_sec:
        return None
    scoped = {
        t.strip().upper()
        for t in str(_exit_management_value(pos, args, 'giveback_exit_tickers', '') or '').split(',')
        if t.strip()
    }
    if scoped and pos.ticker.upper() not in scoped:
        return None
    setup = _exit_management_value(pos, args, 'giveback_exit_setup', 'all') or 'all'
    if setup != 'all' and (pos.signal or {}).get('setup_type') != setup:
        return None
    mfe = _position_mfe_pct(pos)
    if mfe < float(_exit_management_value(pos, args, 'giveback_exit_min_mfe_pct', 0.0) or 0.0):
        return None
    pnl = _position_pnl_pct(pos, price)
    giveback = mfe - pnl
    if giveback < float(_exit_management_value(pos, args, 'giveback_exit_giveback_pct', 0.0) or 0.0):
        return None
    confirm_sec = int(float(_exit_management_value(pos, args, 'giveback_exit_confirm_sec', 0.0) or 0.0))
    max_pnl = float(_exit_management_value(pos, args, 'giveback_exit_max_pnl_pct', 0.0) or 0.0)
    if confirm_sec > 0:
        trigger_ts = (pos.signal or {}).get('giveback_exit_trigger_ts')
        if not trigger_ts:
            pos.signal['giveback_exit_trigger_ts'] = now
            pos.signal['giveback_exit_trigger_pnl_pct'] = round(pnl, 6)
            pos.signal['giveback_exit_trigger_mfe_pct'] = round(mfe, 6)
            pos.signal['giveback_exit_trigger_price'] = float(price)
            if btc_price is not None:
                pos.signal['giveback_exit_trigger_btc_price'] = float(btc_price)
            return None
        if (now - int(trigger_ts)) < confirm_sec:
            return None
    if pnl > max_pnl:
        return None
    rel_max = _exit_management_value(pos, args, 'giveback_exit_rel_max_pct', None)
    if rel_max is not None:
        trigger_price = float((pos.signal or {}).get('giveback_exit_trigger_price') or pos.entry_price)
        stock_move = _side_move_pct(pos.side, trigger_price, float(price))
        trigger_btc = float((pos.signal or {}).get('giveback_exit_trigger_btc_price') or 0.0)
        btc_move = 0.0
        if trigger_btc > 0 and btc_price is not None:
            btc_raw = (float(btc_price) - trigger_btc) / trigger_btc * 100.0
            btc_move = btc_raw if pos.side == 'LONG' else -btc_raw
        rel_move = stock_move - btc_move
        pos.signal['giveback_exit_confirm_rel_pct'] = round(rel_move, 6)
        if rel_move > float(rel_max):
            return None
    miner_max = _exit_management_value(pos, args, 'giveback_exit_miner_max_pct', None)
    if miner_max is not None and miner_indicators:
        vals = []
        side_sign = 1.0 if pos.side == 'LONG' else -1.0
        for ticker, ind in (miner_indicators or {}).items():
            if ticker == pos.ticker:
                continue
            try:
                vals.append(side_sign * float(ind.get('mom_60s') or 0.0))
            except Exception:
                vals.append(0.0)
        miner_avg = sum(vals) / len(vals) if vals else 0.0
        pos.signal['giveback_exit_confirm_miner_mom60'] = round(miner_avg, 6)
        if miner_avg > float(miner_max):
            return None
    if action == 'flip' and not (pos.signal or {}).get('giveback_exit_flipped'):
        return 'giveback_exit_flip'
    return 'giveback_exit'


def _exit_reason(pos: Position, price: Optional[float], now: int, flatten_ts: int,
                 args=None, btc_price: Optional[float] = None,
                 miner_indicators: Optional[dict[str, dict]] = None) -> tuple[Optional[str], Optional[float]]:
    if price is None:
        return None, None
    strict_plan = execution_action_engine.plan_exit(
        pos,
        price,
        now,
        now < flatten_ts,
        contract=STEP2_EXECUTION_CONTRACT,
        include_strict_brackets=True,
        include_conditional_time_stop=False,
        include_session_end=False,
        metadata={
            'ticker': pos.ticker,
            'trade_id': (pos.signal or {}).get('opportunity_id'),
            'opportunity_id': (pos.signal or {}).get('opportunity_id'),
            'setup_type': (pos.signal or {}).get('setup_type'),
            'execution_mode': 'step2_replay',
            'step2_execution_contract_hash': step2_execution_contract.execution_contract_hash(TRADING_CONFIG),
        },
    )
    if strict_plan.get('action') == 'exit':
        pos.signal['exit_action_plan'] = strict_plan
        return strict_plan.get('reason'), strict_plan.get('market', {}).get('exit_price')
    if pos.side == 'LONG':
        unrealized = price - pos.entry_price
    else:
        unrealized = pos.entry_price - price
    pos.signal['path_failure_current_price'] = float(price)
    if btc_price is not None:
        pos.signal['path_failure_current_btc_price'] = float(btc_price)
    path_failure = _path_failure_reason(pos, now, args)
    if path_failure:
        return path_failure, price
    giveback_exit = _giveback_exit_reason(pos, price, now, args, btc_price=btc_price, miner_indicators=miner_indicators)
    if giveback_exit:
        return giveback_exit, price
    after_sec = float(getattr(args, 'brs_failed_followthrough_after_sec', 0.0) or 0.0) if args is not None else 0.0
    if (
        after_sec > 0
        and (pos.signal or {}).get('setup_type') == 'btc_relative_strength'
        and (now - pos.entry_ts) >= after_sec
        and _position_mfe_pct(pos) < float(getattr(args, 'brs_failed_followthrough_min_mfe_pct', 0.0) or 0.0)
        and _position_pnl_pct(pos, price) <= float(getattr(args, 'brs_failed_followthrough_max_pnl_pct', 0.0) or 0.0)
    ):
        return 'brs_failed_followthrough', price
    held_min = (now - pos.entry_ts) / 60
    time_plan = execution_action_engine.plan_exit(
        pos,
        price,
        now,
        now < flatten_ts,
        contract=STEP2_EXECUTION_CONTRACT,
        include_strict_brackets=False,
        include_conditional_time_stop=bool(
            CONDITIONAL_TIME_STOP_ENABLED
            and not bool(getattr(args, 'disable_conditional_stop', False))
            and unrealized < 0
        ),
        conditional_time_stop_min=COND_STOP_MIN,
        include_session_end=True,
        metadata={
            'ticker': pos.ticker,
            'trade_id': (pos.signal or {}).get('opportunity_id'),
            'opportunity_id': (pos.signal or {}).get('opportunity_id'),
            'setup_type': (pos.signal or {}).get('setup_type'),
            'execution_mode': 'step2_replay',
            'step2_execution_contract_hash': step2_execution_contract.execution_contract_hash(TRADING_CONFIG),
        },
    )
    if time_plan.get('action') == 'exit':
        pos.signal['exit_action_plan'] = time_plan
        return time_plan.get('reason'), time_plan.get('market', {}).get('exit_price')
    return None, None


def _close_position(pos: Position, reason: str, exit_price: float, exit_ts: int) -> dict:
    if (pos.signal or {}).get('latency_model', {}).get('mode') == 'entry-exit':
        model = step2_latency_model.load_model((pos.signal or {}).get('latency_model', {}).get('model_path'))
        pct = (pos.signal or {}).get('latency_model', {}).get('percentile', 'p75')
        delay_ms = step2_latency_model.latency_ms(model, pos.ticker, pos.side, 'exit_delay_ms', pct, context=pos.signal)
        bps = step2_latency_model.slippage_bps(model, pos.ticker, pos.side, 'exit_slippage_bps', 'p50', context=pos.signal)
        exit_price = float(step2_latency_model.apply_slippage(float(exit_price), pos.side, bps, 'exit'))
        exit_ts = int(exit_ts + round(delay_ms / 1000.0))
        pos.signal.setdefault('latency_model', {})['exit_delay_ms'] = delay_ms
        pos.signal.setdefault('latency_model', {})['exit_slippage_bps'] = bps
    gross = (exit_price - pos.entry_price) * pos.qty
    if pos.side == 'SHORT':
        gross *= -1
    pnl_pct = (exit_price - pos.entry_price) / pos.entry_price * 100
    if pos.side == 'SHORT':
        pnl_pct *= -1
    mfe_pct = abs(pos.best_price - pos.entry_price) / pos.entry_price * 100
    mae_pct = abs(pos.worst_price - pos.entry_price) / pos.entry_price * 100
    sig = pos.signal
    quality = sig.get('signal_quality') or {}
    exit_action_plan = sig.get('exit_action_plan')
    if not isinstance(exit_action_plan, dict) or not exit_action_plan:
        exit_action_plan = execution_action_engine.plan_exit(
            pos,
            exit_price,
            exit_ts,
            False,
            contract=STEP2_EXECUTION_CONTRACT,
            include_strict_brackets=True,
            include_conditional_time_stop=False,
            include_session_end=True,
            metadata={
                'ticker': pos.ticker,
                'side': pos.side,
                'trade_id': sig.get('opportunity_id'),
                'opportunity_id': sig.get('opportunity_id'),
                'setup_type': sig.get('setup_type'),
                'execution_mode': 'step2_replay',
                'step2_execution_contract_hash': step2_execution_contract.execution_contract_hash(TRADING_CONFIG),
            },
        )
        exit_action_plan['action'] = 'exit'
        exit_action_plan['decision'] = 'closed'
        exit_action_plan['reason'] = reason
        exit_action_plan.setdefault('market', {})['exit_price'] = round(float(exit_price), 4)
        try:
            execution_action_engine.rehash_plan(exit_action_plan)
        except Exception:
            pass
        sig['exit_action_plan'] = exit_action_plan
    exit_execution_intent = execution_intent_engine.exit_intent(
        exit_action_plan,
        venue='step2_sim',
        qty=pos.qty,
        trade_id=str(sig.get('opportunity_id') or f'{pos.ticker}:{pos.entry_ts}'),
        exit_price=round(float(exit_price), 4),
        reason=reason,
        order_kind='simulated_exit',
        metadata={
            'ticker': pos.ticker,
            'side': pos.side,
            'trade_id': sig.get('opportunity_id'),
            'opportunity_id': sig.get('opportunity_id'),
            'setup_type': sig.get('setup_type'),
            'timestamp_second': exit_ts,
            'execution_mode': 'step2_replay',
            'step2_execution_contract_hash': step2_execution_contract.execution_contract_hash(TRADING_CONFIG),
        },
        ts=exit_ts,
    )
    exit_execution_result = execution_intent_engine.simulated_exit_result(
        exit_execution_intent,
        ts=exit_ts,
        fill_price=round(float(exit_price), 4),
        reason=reason,
    )
    sig['exit_execution_intent'] = exit_execution_intent
    sig['exit_execution_result'] = exit_execution_result
    return {
        'opportunity_id': sig.get('opportunity_id'),
        'entry_ct': _iso_ct(pos.entry_ts),
        'exit_ct': _iso_ct(exit_ts),
        'ticker': pos.ticker,
        'side': pos.side,
        'entry': round(pos.entry_price, 4),
        'exit': round(exit_price, 4),
        'tp': round(pos.tp, 4),
        'sl': round(pos.sl, 4),
        'qty': round(pos.qty, 6),
        'alloc': pos.alloc,
        'pnl': round(gross, 4),
        'pnl_pct': round(pnl_pct, 4),
        'mfe_pct': round(mfe_pct, 4),
        'mae_pct': round(mae_pct, 4),
        'held_sec': exit_ts - pos.entry_ts,
        'reason': reason,
        'score': sig.get('score'),
        'conviction': sig.get('conviction'),
        'setup_type': sig.get('setup_type'),
        'session_phase': sig.get('session_phase'),
        'btc_regime': sig.get('btc_regime'),
        'exec_score': (sig.get('execution_quality') or {}).get('score'),
        'min_score_required': quality.get('min_score_required'),
        'scoring_profile': (sig.get('scoring_profile') or {}).get('profile'),
        'profile_score': (sig.get('scoring_profile') or {}).get('score'),
        'profile_original_side': sig.get('scoring_profile_original_side'),
        'reasons': ' | '.join(sig.get('reasons') or []),
        'entry_action_plan': sig.get('entry_action_plan'),
        'exit_action_plan': exit_action_plan,
        'entry_execution_intent': sig.get('entry_execution_intent'),
        'entry_execution_result': sig.get('entry_execution_result'),
        'exit_execution_intent': exit_execution_intent,
        'exit_execution_result': exit_execution_result,
    }


def _partial_position(pos: Position, fraction: float) -> Position:
    frac = max(0.0, min(1.0, float(fraction or 0.0)))
    return Position(
        ticker=pos.ticker,
        side=pos.side,
        entry_ts=pos.entry_ts,
        entry_price=pos.entry_price,
        qty=pos.qty * frac,
        sl=pos.sl,
        tp=pos.tp,
        alloc=round(pos.alloc * frac, 2),
        signal=pos.signal,
        best_price=pos.best_price,
        worst_price=pos.worst_price,
    )


def _run_day(day: date, args, clock: ReplayClock, stats: ReplayStats,
             starting_balance: float) -> tuple[list[dict], float, list[dict]]:
    start_iso, end_iso, start_sec, end_sec = _session_bounds_utc(day)
    del start_iso, end_iso
    flatten_ts = _flatten_ts(day)
    entry_cutoff = _entry_cutoff_ts(day)
    states = {sym: _state(sym) for sym in list(args.tickers) + [BTC_SYMBOL]}
    trade_tickers = {
        t.strip().upper()
        for t in (getattr(args, 'trade_tickers', None) or args.tickers)
        if str(t).strip()
    }
    setup_states: dict = {}
    last_signal_ts: dict[str, float] = {}
    open_positions: dict[str, Position] = {}
    pending_entries: dict[str, dict] = {}
    exec_state = {'positions': {}, 'pending_entries': {}, 'trades': []}
    exec_adapter = execution_adapters.ReplayExecutionAdapter(
        day=day.isoformat(),
        runtime_state=exec_state,
    )
    giveback_cooldown_until: dict[str, int] = {}
    stop_loss_cooldown_until: dict[str, int] = {}
    stop_loss_cooldown_tickers = {
        t.strip().upper()
        for t in str(getattr(args, 'stop_loss_cooldown_tickers', '') or '').split(',')
        if t.strip()
    }
    stop_loss_cooldown_sec = int(float(getattr(args, 'stop_loss_cooldown_sec', 0) or 0))
    loss_cluster_until: dict[str, int] = {}
    loss_cluster_stops: dict[str, deque] = defaultdict(deque)
    loss_cluster_tickers = {
        t.strip().upper()
        for t in str(getattr(args, 'loss_cluster_throttle_tickers', '') or '').split(',')
        if t.strip()
    }
    loss_cluster_window_sec = int(float(getattr(args, 'loss_cluster_window_sec', 0) or 0))
    loss_cluster_count = int(float(getattr(args, 'loss_cluster_count', 0) or 0))
    loss_cluster_cooldown_sec = int(float(getattr(args, 'loss_cluster_cooldown_sec', 0) or 0))
    loss_cluster_scope = getattr(args, 'loss_cluster_scope', 'ticker') or 'ticker'

    def _loss_cluster_key(ticker: str, sig: dict | None = None) -> str:
        if loss_cluster_scope == 'ticker_side_setup' and sig:
            return f"{ticker}:{sig.get('side', 'UNKNOWN')}:{sig.get('setup_type', 'unknown')}"
        if loss_cluster_scope == 'ticker_side' and sig:
            return f"{ticker}:{sig.get('side', 'UNKNOWN')}"
        return ticker

    def _entry_action_plan(sig: dict, pos: Position, ts: int) -> dict:
        plan_state = {
            'positions': dict(exec_state.get('positions') or {}),
            'pending_entries': {
                key: value for key, value in (exec_state.get('pending_entries') or {}).items()
                if str(key).upper() != pos.ticker.upper()
            },
            'trades': list(exec_state.get('trades') or []),
        }
        return execution_action_engine.plan_entry(
            plan_state,
            sig,
            ts,
            STEP2_EXECUTION_CONTRACT,
            signal_scan_live_mode=True,
            entry_price=pos.entry_price,
            brackets={
                'side': pos.side,
                'entry_price': pos.entry_price,
                'tp': pos.tp,
                'sl': pos.sl,
                'rounding': 'side_aware_penny_v1',
            },
            qty=pos.qty,
            alloc=pos.alloc,
            trade_id=str(sig.get('opportunity_id') or f'{day.isoformat()}:{pos.ticker}:{ts}'),
            broker_action='simulated_entry',
            metadata={
                'execution_mode': 'step2_replay',
                'step2_execution_contract_hash': step2_execution_contract.execution_contract_hash(TRADING_CONFIG),
            },
        )

    def _attach_entry_execution(sig: dict, pos: Position, plan: dict, ts: int,
                                opportunity: dict | None = None) -> tuple[dict, dict, str]:
        trade_id = str(sig.get('opportunity_id') or f'{day.isoformat()}:{pos.ticker}:{ts}')
        intent, result = exec_adapter.create_simulated_entry(plan, pos.qty, trade_id=trade_id, ts=ts)
        sig['entry_execution_intent'] = intent
        sig['entry_execution_result'] = result
        if opportunity is not None:
            opportunity['execution_intent'] = intent
            opportunity['execution_result'] = result
            opportunity['execution_intent_id'] = intent.get('execution_intent_id')
            opportunity['semantic_execution_intent_hash'] = intent.get('semantic_execution_intent_hash')
            opportunity['execution_result_id'] = result.get('execution_result_id')
        return intent, result, trade_id

    def _emit_exit_execution(row: dict, ts: int) -> None:
        intent = row.get('exit_execution_intent')
        result = row.get('exit_execution_result')
        if isinstance(intent, dict):
            exec_adapter.emit_intent(intent, ts=ts)
        if isinstance(result, dict):
            exec_adapter.emit_result(result, ts=ts)
    closed: list[dict] = []
    opportunities: list[dict] = []
    opportunity_by_id: dict[str, dict] = {}
    opportunity_seq = 0
    balance = float(starting_balance)
    capture_opportunities = _capture_opportunities(args)
    finalist_mode = _finalist_mode(args)
    profile_timing = bool(getattr(args, 'profile_replay_timing', False))

    def _tick() -> float:
        return time.perf_counter() if profile_timing else 0.0

    def _tock(name: str, started: float) -> None:
        if profile_timing:
            stats.timing[name] += time.perf_counter() - started

    started = _tick()
    events = _load_day_events(day, args, stats)
    _tock('load_day_events', started)
    started = _tick()
    by_sec: dict[int, list[dict]] = defaultdict(list)
    for event in events:
        sec = int(event['t']) // 1000
        if start_sec <= sec < end_sec:
            by_sec[sec].append(event)
    _tock('bucket_events', started)

    for sec in range(start_sec, end_sec):
        clock.now = sec + 0.999
        stats.seconds += 1
        started = _tick()
        for event in by_sec.get(sec, []):
            st = states.get(event['symbol'])
            if not st:
                continue
            if event['kind'] == 'stock_trade':
                _feed_stock_trade(st, event['symbol'], event['row'])
            elif event['kind'] == 'stock_quote':
                _feed_stock_quote(st, event['row'])
            elif event['kind'] == 'btc_synth_trade':
                _feed_btc_synth(st, event['row'])
        _tock('feed_events', started)
        started = _tick()
        for st in states.values():
            _roll_bar(st, sec)
        _tock('roll_bars', started)

        compute_indicators = _replay_compute_indicators if getattr(args, 'indicator_mode', 'live') == 'fast' else ws_scalp.compute_indicators
        started = _tick()
        exit_miner_indicators = None
        if any(
            _exit_management_value(pos, args, 'giveback_exit_miner_max_pct', None) is not None
            for pos in open_positions.values()
        ):
            exit_miner_indicators = {
                t: compute_indicators(states[t])
                for t in args.tickers
                if len(states[t].bars_1s) >= 60
            }
        for tkr, pos in list(open_positions.items()):
            if sec < int(pos.active_ts or pos.entry_ts):
                continue
            price = _position_exit_mark(states[tkr], pos, sec)
            _mark_position(pos, price)
            reason, exit_price = _exit_reason(
                pos, price, sec, flatten_ts, args,
                btc_price=states[BTC_SYMBOL].last_trade_price,
                miner_indicators=exit_miner_indicators,
            )
            if reason and exit_price is not None:
                if reason == 'path_failure_reduce':
                    reduce_fraction = max(0.0, min(1.0, float(getattr(args, 'path_failure_reduce_fraction', 0.0) or 0.0)))
                    if reduce_fraction <= 0.0:
                        continue
                    partial = _partial_position(pos, reduce_fraction)
                    row = _close_position(partial, reason, float(exit_price), sec)
                    closed.append(row)
                    balance += row['pnl']
                    _emit_exit_execution(row, sec)
                    pos.qty *= (1.0 - reduce_fraction)
                    pos.alloc = round(pos.alloc * (1.0 - reduce_fraction), 2)
                    pos.signal['path_failure_reduced'] = True
                    pos.signal['path_failure_reduce_fraction'] = round(reduce_fraction, 6)
                    if pos.qty <= 1e-9 or pos.alloc <= 0.0:
                        open_positions.pop(tkr, None)
                    continue
                row = _close_position(pos, reason, float(exit_price), sec)
                closed.append(row)
                balance += row['pnl']
                _emit_exit_execution(row, sec)
                exec_adapter.close_position(
                    tkr,
                    str((pos.signal or {}).get('opportunity_id') or f'{day.isoformat()}:{tkr}:{pos.entry_ts}'),
                    pos.side,
                    reason,
                    float(exit_price),
                    pnl=row.get('pnl'),
                    ts=sec,
                )
                open_positions.pop(tkr, None)
                if reason in ('giveback_exit', 'giveback_exit_flip'):
                    cooldown_sec = int(float(_exit_management_value(pos, args, 'giveback_exit_cooldown_sec', 0.0) or 0.0))
                    if cooldown_sec > 0:
                        giveback_cooldown_until[tkr] = sec + cooldown_sec
                if reason == 'stop_loss' and stop_loss_cooldown_sec > 0 and tkr.upper() in stop_loss_cooldown_tickers:
                    stop_loss_cooldown_until[tkr] = sec + stop_loss_cooldown_sec
                if (
                    reason == 'stop_loss'
                    and loss_cluster_cooldown_sec > 0
                    and loss_cluster_window_sec > 0
                    and loss_cluster_count > 0
                    and tkr.upper() in loss_cluster_tickers
                ):
                    cluster_key = _loss_cluster_key(tkr, pos.signal)
                    recent = loss_cluster_stops[cluster_key]
                    recent.append(sec)
                    cutoff = sec - loss_cluster_window_sec
                    while recent and recent[0] < cutoff:
                        recent.popleft()
                    if len(recent) >= loss_cluster_count:
                        loss_cluster_until[cluster_key] = sec + loss_cluster_cooldown_sec
                        recent.clear()
                if reason in ('path_failure_flip', 'giveback_exit_flip'):
                    flip_sig = _flip_signal_from_position(pos, float(exit_price), sec, reason)
                    flip_sig['path_failure_entry_btc_price'] = float(states[BTC_SYMBOL].last_trade_price or 0.0)
                    open_positions[tkr] = _open_position(flip_sig, balance, args)
                    flip_pos = open_positions[tkr]
                    flip_plan = _entry_action_plan(flip_sig, flip_pos, sec)
                    flip_sig['entry_action_plan'] = flip_plan
                    flip_intent, flip_result, flip_trade_id = _attach_entry_execution(
                        flip_sig, flip_pos, flip_plan, sec
                    )
                    exec_adapter.commit_entry(
                        tkr,
                        str(flip_sig.get('opportunity_id') or f'{day.isoformat()}:{tkr}:{sec}:flip'),
                        flip_pos.side,
                        flip_pos.entry_price,
                        qty=flip_pos.qty,
                        alloc=flip_pos.alloc,
                        tp=flip_pos.tp,
                        sl=flip_pos.sl,
                        ts=sec,
                        extra={'execution_intent': flip_intent, 'execution_result': flip_result},
                    )
        for tkr, pending in list(pending_entries.items()):
            cur_price = states[tkr].last_trade_price
            if cur_price is not None:
                if str((pending.get('sig') or {}).get('side') or '').upper() == 'LONG':
                    pending['best_price'] = max(float(pending.get('best_price') or cur_price), float(cur_price))
                    pending['worst_price'] = min(float(pending.get('worst_price') or cur_price), float(cur_price))
                else:
                    pending['best_price'] = max(float(pending.get('best_price') or cur_price), float(cur_price))
                    pending['worst_price'] = min(float(pending.get('worst_price') or cur_price), float(cur_price))
            if sec < int(pending.get('due_ts') or 0):
                continue
            result, resolved_sig = _resolve_pending_entry(
                pending, states[tkr].last_trade_price, args,
                current_btc_price=states[BTC_SYMBOL].last_trade_price,
            )
            if result == 'enter' and resolved_sig is not None:
                resolved_sig['path_failure_entry_btc_price'] = float(states[BTC_SYMBOL].last_trade_price or 0.0)
                open_positions[tkr] = _open_position(resolved_sig, balance, args)
                pos = open_positions[tkr]
                plan = _entry_action_plan(resolved_sig, pos, sec)
                resolved_sig['entry_action_plan'] = plan
                pending_opp_id = resolved_sig.get('opportunity_id')
                if pending_opp_id and pending_opp_id in opportunity_by_id:
                    opportunity_by_id[pending_opp_id]['action_plan'] = plan
                resolved_opp = opportunity_by_id.get(pending_opp_id) if pending_opp_id else None
                entry_intent, entry_result, resolved_trade_id = _attach_entry_execution(
                    resolved_sig, pos, plan, sec, resolved_opp
                )
                exec_adapter.commit_entry(
                    tkr,
                    str(resolved_sig.get('opportunity_id') or f'{day.isoformat()}:{tkr}:{sec}:confirmed'),
                    pos.side,
                    pos.entry_price,
                    qty=pos.qty,
                    alloc=pos.alloc,
                    tp=pos.tp,
                    sl=pos.sl,
                    ts=sec,
                    extra={'execution_intent': entry_intent, 'execution_result': entry_result},
                )
            else:
                stats.skipped_signals[f'entry_confirmation_{result}'] += 1
                pending_opp_id = ((pending.get('sig') or {}).get('opportunity_id'))
                exec_adapter.emit(
                    'entry_fill_timeout_cancelled',
                    tkr,
                    str(pending_opp_id or f'{day.isoformat()}:{tkr}:{sec}:pending'),
                    {'reason': f'entry_confirmation_{result}'},
                    ts=sec,
                )
                if pending_opp_id and pending_opp_id in opportunity_by_id:
                    opportunity_by_id[pending_opp_id]['decision'] = 'rejected'
                    opportunity_by_id[pending_opp_id]['reject_reason'] = f'entry_confirmation_{result}'
            pending_entries.pop(tkr, None)
        _tock('manage_positions', started)

        if sec > entry_cutoff:
            continue
        if not getattr(args, 'disable_all_open_signal_skip', False) and len(open_positions) + len(pending_entries) >= len(args.tickers):
            stats.skipped_signals['all_tickers_open_skip_signal_pass'] += 1
            continue

        started = _tick()
        btc_ind = compute_indicators(states[BTC_SYMBOL]) if args.btc_mode != 'off' else {
            'ready': True,
            'ema_stack': 'mixed',
            'last_trade_age_sec': 0,
            'last_quote_age_sec': 0,
            'mom_5s': 0,
            'mom_15s': 0,
            'mom_30s': 0,
            'mom_60s': 0,
            'mom_180s': 0,
        }
        stock_indicators = {}
        for tkr in args.tickers:
            if len(states[tkr].bars_1s) >= 60:
                stock_indicators[tkr] = compute_indicators(states[tkr])
        _tock('compute_indicators', started)
        for tkr, ind in stock_indicators.items():
            if tkr.upper() not in trade_tickers:
                continue
            admission_plan = execution_action_engine.plan_entry(
                exec_state,
                {'ticker': tkr},
                sec,
                STEP2_EXECUTION_CONTRACT,
                signal_scan_live_mode=True,
                metadata={
                    'execution_mode': 'step2_replay',
                    'step2_execution_contract_hash': step2_execution_contract.execution_contract_hash(TRADING_CONFIG),
                },
            )
            adapter_block_reason = admission_plan.get('reason') if admission_plan.get('action') == 'skip' else None
            if adapter_block_reason:
                continue
            if tkr in open_positions or tkr in pending_entries:
                continue
            if sec < int(giveback_cooldown_until.get(tkr, 0) or 0):
                stats.skipped_signals['giveback_exit_cooldown'] += 1
                continue
            if sec < int(stop_loss_cooldown_until.get(tkr, 0) or 0):
                stats.skipped_signals['stop_loss_cooldown'] += 1
                continue
            started = _tick()
            sig = ws_scalp.detect_signal(tkr, ind, btc_ind, miner_indicators=stock_indicators)
            _tock('detect_signal', started)
            if not sig:
                continue
            started = _tick()
            sig = scoring_profiles.apply_profile(
                getattr(args, '_scoring_profile_data', None), sig, ind, btc_ind,
                include_details=(capture_opportunities or not finalist_mode),
            )
            sig['ts'] = sec
            opp_id = f"{day.isoformat()}:{tkr}:{sec}:{sig.get('side')}:{sig.get('setup_type', 'unknown')}:{opportunity_seq}"
            opportunity_seq += 1
            sig['opportunity_id'] = opp_id
            signal_cluster_key = _loss_cluster_key(tkr, sig)
            if sec < int(loss_cluster_until.get(signal_cluster_key, 0) or 0):
                stats.skipped_signals['loss_cluster_throttle'] += 1
                continue
            opportunity = None
            if capture_opportunities:
                opportunity = {
                    'schema_version': 1,
                    'type': 'engine_opportunity',
                    'opportunity_id': opp_id,
                    'day': day.isoformat(),
                    'ts': sec,
                    'ts_ct': _iso_ct(sec),
                    'ticker': tkr,
                    'side': sig.get('side'),
                    'setup_type': sig.get('setup_type'),
                    'price': sig.get('price'),
                    'score': sig.get('score'),
                    'conviction': sig.get('conviction'),
                    'session_phase': sig.get('session_phase'),
                    'btc_regime': sig.get('btc_regime'),
                    'decision': 'detected',
                    'reject_reason': None,
                    'signal': _json_clone(sig),
                    'features': {
                        'ticker_indicators': _json_clone(ind),
                        'btc_indicators': _json_clone(btc_ind),
                        'miner_indicators': _json_clone(stock_indicators),
                    },
                    'outcome': None,
                }
            throttle_reason = _long_throttle_reason(args, sig, ind, btc_ind, stock_indicators)
            if throttle_reason:
                stats.skipped_signals[f'regime_long_throttle:{throttle_reason}'] += 1
                _tock('post_signal_gates', started)
                if capture_opportunities and opportunity is not None:
                    opportunity['decision'] = 'rejected'
                    opportunity['reject_reason'] = f'regime_long_throttle:{throttle_reason}'
                    opportunities.append(opportunity)
                    opportunity_by_id[opp_id] = opportunity
                continue
            mock_guard_reason = _step2_brs_long_btc_negative_mom60_reason(sig, btc_ind)
            if mock_guard_reason:
                stats.skipped_signals[mock_guard_reason] += 1
                _tock('post_signal_gates', started)
                if capture_opportunities and opportunity is not None:
                    opportunity['decision'] = 'rejected'
                    opportunity['reject_reason'] = mock_guard_reason
                    opportunities.append(opportunity)
                    opportunity_by_id[opp_id] = opportunity
                continue
            warning_reason = _entry_warning_gate_reason(args, tkr, sig, ind)
            if warning_reason and (getattr(args, 'entry_warning_gate_action', 'veto') or 'veto') == 'veto':
                stats.skipped_signals[f'entry_warning_gate:{warning_reason}'] += 1
                _tock('post_signal_gates', started)
                if capture_opportunities and opportunity is not None:
                    opportunity['decision'] = 'rejected'
                    opportunity['reject_reason'] = f'entry_warning_gate:{warning_reason}'
                    opportunities.append(opportunity)
                    opportunity_by_id[opp_id] = opportunity
                continue
            if bool(getattr(args, 'require_conviction', True)) and sig.get('conviction') not in MIN_CONVICTION:
                stats.skipped_signals['below_min_conviction'] += 1
                _tock('post_signal_gates', started)
                if capture_opportunities and opportunity is not None:
                    opportunity['decision'] = 'rejected'
                    opportunity['reject_reason'] = 'below_min_conviction'
                    opportunities.append(opportunity)
                    opportunity_by_id[opp_id] = opportunity
                continue
            if not _setup_state_gate(setup_states, tkr, sig, ind, clock.now):
                stats.skipped_signals['setup_state_pending'] += 1
                _tock('post_signal_gates', started)
                if capture_opportunities and opportunity is not None:
                    opportunity['decision'] = 'rejected'
                    opportunity['reject_reason'] = 'setup_state_pending'
                    opportunity['signal'] = _json_clone(sig)
                    opportunities.append(opportunity)
                    opportunity_by_id[opp_id] = opportunity
                continue
            key = f"{tkr}:{sig['side']}:{sig.get('setup_type', 'unknown')}"
            cooldown = float(ws_scalp.SETUP_COOLDOWN_SEC.get(sig.get('setup_type'), 30))
            if clock.now - float(last_signal_ts.get(key, 0) or 0) < cooldown:
                stats.skipped_signals['cooldown'] += 1
                _tock('post_signal_gates', started)
                if capture_opportunities and opportunity is not None:
                    opportunity['decision'] = 'rejected'
                    opportunity['reject_reason'] = 'cooldown'
                    opportunity['signal'] = _json_clone(sig)
                    opportunities.append(opportunity)
                    opportunity_by_id[opp_id] = opportunity
                continue
            sig = _decorate_signal(sig, states[tkr], ind, include_shadow=not finalist_mode)
            if capture_opportunities and opportunity is not None:
                opportunity['signal'] = _json_clone(sig)
                opportunity['price'] = sig.get('price')
                opportunity['score'] = sig.get('score')
                opportunity['conviction'] = sig.get('conviction')
            if float((sig.get('execution_quality') or {}).get('score') or 0) < ws_scalp.EXECUTION_QUALITY_MIN:
                stats.skipped_signals['execution_quality_low'] += 1
                _tock('post_signal_gates', started)
                if capture_opportunities and opportunity is not None:
                    opportunity['decision'] = 'rejected'
                    opportunity['reject_reason'] = 'execution_quality_low'
                    opportunities.append(opportunity)
                    opportunity_by_id[opp_id] = opportunity
                continue
            last_signal_ts[key] = clock.now
            stats.emitted_signals += 1
            if capture_opportunities and opportunity is not None:
                opportunity['decision'] = 'accepted'
                opportunities.append(opportunity)
                opportunity_by_id[opp_id] = opportunity
            if _entry_confirmation_enabled(args, sig, ind, tkr):
                pending_entries[tkr] = {
                    'sig': _json_clone(sig),
                    'due_ts': sec + int(float(getattr(args, 'entry_confirmation_delay_sec', 0.0) or 0.0)),
                    'signal_price': float(sig.get('price') or states[tkr].last_trade_price or 0.0),
                    'signal_btc_price': float(states[BTC_SYMBOL].last_trade_price or 0.0),
                    'best_price': float(sig.get('price') or states[tkr].last_trade_price or 0.0),
                    'worst_price': float(sig.get('price') or states[tkr].last_trade_price or 0.0),
                }
                exec_adapter.reserve_entry(
                    tkr,
                    str(sig.get('opportunity_id') or f'{day.isoformat()}:{tkr}:{sec}:pending'),
                    sig.get('side'),
                    float(sig.get('price') or states[tkr].last_trade_price or 0.0),
                    ts=sec,
                )
            else:
                sig['path_failure_entry_btc_price'] = float(states[BTC_SYMBOL].last_trade_price or 0.0)
                open_positions[tkr] = _open_position(sig, balance, args)
                pos = open_positions[tkr]
                plan = _entry_action_plan(sig, pos, sec)
                sig['entry_action_plan'] = plan
                if capture_opportunities and opportunity is not None:
                    opportunity['action_plan'] = plan
                entry_intent, entry_result, entry_trade_id = _attach_entry_execution(
                    sig, pos, plan, sec, opportunity if capture_opportunities else None
                )
                exec_adapter.commit_entry(
                    tkr,
                    str(sig.get('opportunity_id') or f'{day.isoformat()}:{tkr}:{sec}'),
                    pos.side,
                    pos.entry_price,
                    qty=pos.qty,
                    alloc=pos.alloc,
                    tp=pos.tp,
                    sl=pos.sl,
                    ts=sec,
                    extra={'execution_intent': entry_intent, 'execution_result': entry_result},
                )
            _tock('post_signal_gates', started)

    pending_entries.clear()
    for tkr, pos in list(open_positions.items()):
        price = states[tkr].last_trade_price or pos.entry_price
        row = _close_position(pos, 'end_of_data', float(price), end_sec)
        closed.append(row)
        balance += row['pnl']
        _emit_exit_execution(row, end_sec)
        exec_adapter.close_position(
            tkr,
            str((pos.signal or {}).get('opportunity_id') or f'{day.isoformat()}:{tkr}:{pos.entry_ts}:end'),
            pos.side,
            'end_of_data',
            float(price),
            pnl=row.get('pnl'),
            ts=end_sec,
        )
    for row in closed:
        opp_id = row.get('opportunity_id')
        if opp_id and opp_id in opportunity_by_id:
            opportunity_by_id[opp_id]['outcome'] = _json_clone(row)
    return closed, balance, opportunities


def _write_outputs(rows: list[dict], summary: dict, args) -> tuple[str, str]:
    os.makedirs(args.out_dir, exist_ok=True)
    stem = f"engine_replay_{args.start}_{args.end}"
    if getattr(args, 'scoring_profile', None):
        profile = os.path.splitext(os.path.basename(args.scoring_profile))[0]
        stem = f'{stem}_{profile}'
    throttle = _replay_throttle_label(args)
    if throttle != 'off':
        stem = f'{stem}_{throttle}'
    validation_mode = getattr(args, 'validation_mode', 'research')
    indicator_mode = getattr(args, 'indicator_mode', 'live')
    stem = f'{stem}_{validation_mode}_{indicator_mode}'
    csv_path = os.path.join(args.out_dir, f'{stem}_trades.csv')
    json_path = os.path.join(args.out_dir, f'{stem}_summary.json')
    fieldnames = [
        'opportunity_id', 'entry_ct', 'exit_ct', 'ticker', 'side', 'entry',
        'exit', 'qty', 'alloc', 'pnl', 'pnl_pct', 'mfe_pct', 'mae_pct',
        'held_sec', 'reason', 'score', 'conviction', 'setup_type',
        'session_phase', 'btc_regime', 'exec_score', 'min_score_required',
        'scoring_profile', 'profile_score', 'profile_original_side',
        'reasons',
    ]
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k) for k in fieldnames})
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, sort_keys=True)
    return csv_path, json_path


def _summarize(rows: list[dict], stats: ReplayStats, args, ending_balance: float) -> dict:
    wins = [r for r in rows if float(r.get('pnl') or 0) > 0]
    losses = [r for r in rows if float(r.get('pnl') or 0) < 0]
    pnl = round(sum(float(r.get('pnl') or 0) for r in rows), 4)
    by_ticker = {}
    for tkr in args.tickers:
        scoped = [r for r in rows if r.get('ticker') == tkr]
        by_ticker[tkr] = {
            'trades': len(scoped),
            'pnl': round(sum(float(r.get('pnl') or 0) for r in scoped), 4),
            'win_rate_pct': round(100 * len([r for r in scoped if float(r.get('pnl') or 0) > 0]) / len(scoped), 2) if scoped else None,
        }
    by_day = {}
    for row in rows:
        day = str(row.get('entry_ct') or '')[:10]
        if not day:
            continue
        bucket = by_day.setdefault(day, {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0})
        row_pnl = float(row.get('pnl') or 0)
        bucket['trades'] += 1
        bucket['wins'] += 1 if row_pnl > 0 else 0
        bucket['losses'] += 1 if row_pnl < 0 else 0
        bucket['pnl'] += row_pnl
    for bucket in by_day.values():
        trades = int(bucket.get('trades') or 0)
        bucket['pnl'] = round(float(bucket.get('pnl') or 0.0), 4)
        bucket['win_rate_pct'] = round(100 * int(bucket.get('wins') or 0) / trades, 2) if trades else None
    return {
        'start': args.start,
        'end': args.end,
        'tickers': list(args.tickers),
        'feed': args.feed,
        'quote_mode': args.quote_mode,
        'prepared_events': bool(getattr(args, 'use_prepared_events', False)),
        'validation_mode': getattr(args, 'validation_mode', 'research'),
        'indicator_mode': getattr(args, 'indicator_mode', 'live'),
        'sample_days': getattr(args, 'sample_days', ''),
        'replay_regime_long_throttle': getattr(args, 'replay_regime_long_throttle', 'off'),
        'replay_regime_threshold_pct': getattr(args, 'replay_regime_threshold_pct', 0.0),
        'brs_failed_followthrough_after_sec': getattr(args, 'brs_failed_followthrough_after_sec', 0.0),
        'brs_failed_followthrough_min_mfe_pct': getattr(args, 'brs_failed_followthrough_min_mfe_pct', 0.0),
        'brs_failed_followthrough_max_pnl_pct': getattr(args, 'brs_failed_followthrough_max_pnl_pct', 0.0),
        'brs_tp_multiplier': getattr(args, 'brs_tp_multiplier', 1.0),
        'momentum_tp_multiplier': getattr(args, 'momentum_tp_multiplier', 1.0),
        'high_conviction_tp_multiplier': getattr(args, 'high_conviction_tp_multiplier', 1.0),
        'fixed_tp_pct': getattr(args, 'fixed_tp_pct', None),
        'fixed_sl_pct': getattr(args, 'fixed_sl_pct', None),
        'fixed_ticker_brackets': getattr(args, 'fixed_ticker_brackets', ''),
        'entry_confirmation_delay_sec': getattr(args, 'entry_confirmation_delay_sec', 0),
        'entry_confirmation_mode': getattr(args, 'entry_confirmation_mode', 'off'),
        'entry_confirmation_setup': getattr(args, 'entry_confirmation_setup', 'all'),
        'entry_confirmation_min_favorable_pct': getattr(args, 'entry_confirmation_min_favorable_pct', 0.0),
        'entry_confirmation_adverse_pct': getattr(args, 'entry_confirmation_adverse_pct', 0.0),
        'entry_confirmation_session_phase': getattr(args, 'entry_confirmation_session_phase', 'all'),
        'entry_confirmation_edge_threshold_pct': getattr(args, 'entry_confirmation_edge_threshold_pct', None),
        'entry_confirmation_rel_threshold_pct': getattr(args, 'entry_confirmation_rel_threshold_pct', None),
        'path_failure_after_sec': getattr(args, 'path_failure_after_sec', 0),
        'path_failure_edge_threshold_pct': getattr(args, 'path_failure_edge_threshold_pct', 0.0),
        'path_failure_action': getattr(args, 'path_failure_action', 'off'),
        'path_failure_setup': getattr(args, 'path_failure_setup', 'all'),
        'path_failure_session_phase': getattr(args, 'path_failure_session_phase', 'all'),
        'path_failure_rel_threshold_pct': getattr(args, 'path_failure_rel_threshold_pct', None),
        'path_failure_window_sec': getattr(args, 'path_failure_window_sec', 0),
        'entry_warning_gate_mode': getattr(args, 'entry_warning_gate_mode', 'off'),
        'entry_warning_gate_action': getattr(args, 'entry_warning_gate_action', 'veto'),
        'entry_warning_gate_tickers': getattr(args, 'entry_warning_gate_tickers', ''),
        'entry_warning_gate_setup': getattr(args, 'entry_warning_gate_setup', 'all'),
        'entry_warning_gate_session_phase': getattr(args, 'entry_warning_gate_session_phase', 'all'),
        'entry_warning_gate_stock_mom60_threshold_pct': getattr(args, 'entry_warning_gate_stock_mom60_threshold_pct', -0.10),
        'entry_warning_gate_rel60_threshold_pct': getattr(args, 'entry_warning_gate_rel60_threshold_pct', -0.10),
        'chop_bracket_mode': getattr(args, 'chop_bracket_mode', 'off'),
        'chop_bracket_tickers': getattr(args, 'chop_bracket_tickers', ''),
        'chop_bracket_range_threshold_pct': getattr(args, 'chop_bracket_range_threshold_pct', 0.75),
        'chop_bracket_efficiency_threshold': getattr(args, 'chop_bracket_efficiency_threshold', 0.20),
        'chop_bracket_flips_threshold': getattr(args, 'chop_bracket_flips_threshold', 10),
        'chop_bracket_max_score': getattr(args, 'chop_bracket_max_score', 999.0),
        'chop_bracket_tp_pct': getattr(args, 'chop_bracket_tp_pct', 0.0010),
        'chop_bracket_sl_pct': getattr(args, 'chop_bracket_sl_pct', 0.0010),
        'giveback_exit_after_sec': getattr(args, 'giveback_exit_after_sec', 0),
        'giveback_exit_min_mfe_pct': getattr(args, 'giveback_exit_min_mfe_pct', 0.0),
        'giveback_exit_giveback_pct': getattr(args, 'giveback_exit_giveback_pct', 0.0),
        'giveback_exit_max_pnl_pct': getattr(args, 'giveback_exit_max_pnl_pct', 0.0),
        'giveback_exit_confirm_sec': getattr(args, 'giveback_exit_confirm_sec', 0),
        'giveback_exit_action': getattr(args, 'giveback_exit_action', 'off'),
        'giveback_exit_tickers': getattr(args, 'giveback_exit_tickers', ''),
        'giveback_exit_setup': getattr(args, 'giveback_exit_setup', 'all'),
        'giveback_exit_cooldown_sec': getattr(args, 'giveback_exit_cooldown_sec', 0),
        'scoring_profile': (
            (getattr(args, '_scoring_profile_data', None) or {}).get('name')
            if getattr(args, '_scoring_profile_data', None) else None
        ),
        'trades': len(rows),
        'wins': len(wins),
        'losses': len(losses),
        'win_rate_pct': round(100 * len(wins) / len(rows), 2) if rows else None,
        'pnl': pnl,
        'starting_balance': args.start_balance,
        'ending_balance': round(ending_balance, 4),
        'by_ticker': by_ticker,
        'by_day': by_day,
        'exit_reasons': dict(Counter(r.get('reason') for r in rows)),
        'setups': dict(Counter(r.get('setup_type') for r in rows)),
        'emitted_signals': stats.emitted_signals,
        'skipped_signals': dict(stats.skipped_signals),
        'cache_hits': dict(stats.cache_hits),
        'fetched': dict(stats.fetched),
        'seconds_replayed': stats.seconds,
        'timing': {k: round(float(v), 6) for k, v in stats.timing.items()},
        'notes': [
            'Imports current ws_scalp.py at runtime; rerun after engine changes.',
            'Stock trades and quotes come from Alpaca historical endpoints.',
            'BTC context defaults to synthetic 1-second points derived from Alpaca 1-minute BTC/USD bars.',
            'Broker-only gates such as borrow checks and actual fill slippage are approximated.',
            'finalist validation mode skips research opportunity snapshots but preserves signal/gate/position replay logic.',
            'fast indicator mode is replay-only and should be checked against live mode before promotion.',
        ],
    }


def parse_args() -> argparse.Namespace:
    start, end = _default_dates()
    ap = argparse.ArgumentParser(description='Backtest current ws_scalp engine against Alpaca historical data.')
    ap.add_argument('--start', default=start.isoformat(), help='Start date YYYY-MM-DD, default last 30 calendar days.')
    ap.add_argument('--end', default=end.isoformat(), help='End date YYYY-MM-DD, default yesterday.')
    ap.add_argument('--sample-days', default='',
                    help='Comma-separated YYYY-MM-DD list for sampled/live validation. Empty replays every market day in the date range.')
    ap.add_argument('--tickers', nargs='+', default=TICKERS, help='Stock tickers to replay.')
    ap.add_argument('--trade-tickers', nargs='+', default=None,
                    help='Optional subset of tickers allowed to trade while all --tickers still provide signal context.')
    ap.add_argument('--feed', default='sip', choices=['sip', 'iex', 'delayed_sip'], help='Alpaca stock data feed.')
    ap.add_argument('--quote-mode', default='per-second', choices=['per-second', 'all', 'off'],
                    help='Use latest quote per second by default to keep replays tractable.')
    ap.add_argument('--btc-mode', default='bars', choices=['bars', 'off'],
                    help='BTC context source. bars synthesizes 1s BTC from Alpaca 1Min bars; off uses neutral BTC for smoke tests.')
    ap.add_argument('--cache-dir', default=DEFAULT_CACHE_DIR)
    ap.add_argument('--prepared-cache-dir', default=DEFAULT_PREPARED_DIR)
    ap.add_argument('--out-dir', default=DEFAULT_OUT_DIR)
    ap.add_argument('--refresh', action='store_true', help='Ignore cached Alpaca responses and refetch.')
    ap.add_argument('--use-prepared-events', action='store_true',
                    help='Load prebuilt per-day event tapes when present; build them on miss.')
    ap.add_argument('--write-prepared-events', action='store_true',
                    help='Write prebuilt per-day event tapes while replaying.')
    ap.add_argument('--refresh-prepared-events', action='store_true',
                    help='Ignore existing prepared event tapes and rebuild them from raw cached data.')
    ap.add_argument('--max-pages', type=int, default=1000,
                    help='Max Alpaca pages per symbol/day/kind. Lower this for smoke tests.')
    ap.add_argument('--resume-days', action='store_true',
                    help='Reuse day-level checkpoint files and write one checkpoint after each completed day.')
    ap.add_argument('--rebuild-missing-opportunities', action='store_true',
                    help='With --resume-days and --write-opportunity-ledger, rerun day checkpoints that lack stored opportunities.')
    ap.add_argument('--workers', type=int, default=1,
                    help='Run independent trading days in parallel. Uses full cached data; final P/L is compounded in date order.')
    ap.add_argument('--start-balance', type=float, default=START_BALANCE)
    ap.add_argument('--validation-mode', default='research', choices=['research', 'finalist'],
                    help='finalist keeps full replay behavior but skips heavy research opportunity cloning unless ledger writing is requested.')
    ap.add_argument('--indicator-mode', default='live', choices=['live', 'fast'],
                    help='fast uses replay-only cached numeric bar arrays for indicator calculation.')
    ap.add_argument('--write-opportunity-ledger', action='store_true',
                    help='Write per-day JSONL feature/opportunity ledgers for fast scoring-variant research.')
    ap.add_argument('--opportunity-dir', default=None,
                    help='Directory for opportunity ledgers. Defaults to <out-dir>/opportunities.')
    ap.add_argument('--write-step2-decision-parity', action='store_true',
                    help='Write normalized Step 2 decision rows for Live-vs-Step2 parity reporting.')
    ap.add_argument('--step2-decision-parity-dir', default=step2_decision_parity.OUT_DIR)
    ap.add_argument('--step2-latency-mode', default='off', choices=['off', 'entry', 'entry-exit'],
                    help='Apply a Live-derived latency model to Step 2 replay entries/exits.')
    ap.add_argument('--step2-latency-model', default=step2_latency_model.DEFAULT_MODEL_PATH,
                    help='Path to Step 2 latency model JSON.')
    ap.add_argument('--step2-latency-percentile', default='p75', choices=['p50', 'p75', 'p95', 'default'],
                    help='Latency percentile to use for modeled Step 2 fills.')
    ap.add_argument('--scoring-profile', default=None,
                    help='Replay-only scoring profile JSON. Overrides direction after live signal detection.')
    ap.add_argument('--replay-regime-long-throttle', default='off', choices=[
        'off',
        'ticker-negative',
        'miner-negative',
        'miner-btc-negative',
        'trend-pullback-miner-negative',
        'btc-negative',
        'ticker-negative-below-vwap',
        'ticker-negative-btc-bear',
        'ticker-negative-not-above-vwap',
        'ticker-negative-not-momentum',
        'ticker-negative-btc-relative',
        'ticker-negative-require-profile-agree',
        'flip-short-long-ticker-negative',
        'flip-short-long-below-vwap',
        'flip-short-long-btc-not-bull',
        'below-vwap-btc-bear',
        'ticker-negative-below-vwap-btc-not-bull',
        'ticker-negative-below-vwap-miner-not-confirmed',
        'ticker-negative-below-vwap-not-momentum',
        'ticker-negative-below-vwap-stock-lagging-btc',
        'ticker-negative-below-vwap-weak-flow',
        'ticker-negative-below-vwap-lower-half-range',
        'ticker-negative-below-vwap-btc-not-bull-stock-lagging',
        'ticker-negative-below-vwap-btc-not-bull-weak-flow',
        'ticker-negative-below-vwap-btc-not-bull-lower-half-range',
        'ticker-negative-below-vwap-btc-not-bull-not-momentum',
        'ticker-negative-below-vwap-btc-not-bull-btc-relative',
        'brs-follow',
        'btc-relative-normal-long-followthrough',
    ], help='Replay-only causal negative-regime gate for hypothetical long throttles.')
    ap.add_argument('--replay-regime-threshold-pct', type=float, default=0.0,
                    help='Intraday session-return threshold for replay regime long throttles.')
    ap.add_argument('--brs-failed-followthrough-after-sec', type=float, default=0.0,
                    help='Replay-only early exit for btc_relative_strength trades after N seconds when MFE/PnL thresholds fail. 0 disables.')
    ap.add_argument('--brs-failed-followthrough-min-mfe-pct', type=float, default=0.12)
    ap.add_argument('--brs-failed-followthrough-max-pnl-pct', type=float, default=0.0)
    ap.add_argument('--brs-tp-multiplier', type=float, default=1.0,
                    help='Replay-only multiplier for btc_relative_strength take-profit distance.')
    ap.add_argument('--momentum-tp-multiplier', type=float, default=1.0,
                    help='Replay-only multiplier for momentum_breakout take-profit distance.')
    ap.add_argument('--high-conviction-tp-multiplier', type=float, default=1.0,
                    help='Replay-only multiplier for HIGH conviction take-profit distance.')
    ap.add_argument('--require-conviction', action='store_true',
                    help='Replay-only legacy gate requiring configured MEDIUM/HIGH conviction.')
    ap.add_argument('--disable-conditional-stop', action='store_true',
                    help='Replay-only strict TP/SL mode that disables conditional time stop exits.')
    ap.add_argument('--fixed-tp-pct', type=float, default=None,
                    help='Replay-only fixed take-profit pct as decimal, e.g. 0.004 for 0.4%%.')
    ap.add_argument('--fixed-sl-pct', type=float, default=None,
                    help='Replay-only fixed stop-loss pct as decimal, e.g. 0.004 for 0.4%%.')
    ap.add_argument('--fixed-ticker-brackets', default='',
                    help='Comma-separated ticker-specific TP/SL overrides as TICKER:tp:sl, e.g. MARA:0.003:0.0045.')
    ap.add_argument('--entry-confirmation-delay-sec', type=int, default=0,
                    help='Replay-only delayed-entry confirmation. 0 disables.')
    ap.add_argument('--entry-confirmation-mode', choices=['off', 'confirm', 'skip_adverse', 'flip_adverse', 'skip_path_failure'], default='off')
    ap.add_argument('--entry-confirmation-setup', default='all')
    ap.add_argument('--entry-confirmation-min-favorable-pct', type=float, default=0.0)
    ap.add_argument('--entry-confirmation-adverse-pct', type=float, default=0.0)
    ap.add_argument('--entry-confirmation-session-phase', default='all')
    ap.add_argument('--entry-confirmation-edge-threshold-pct', type=float, default=None)
    ap.add_argument('--entry-confirmation-rel-threshold-pct', type=float, default=None)
    ap.add_argument('--path-failure-after-sec', type=int, default=0)
    ap.add_argument('--path-failure-edge-threshold-pct', type=float, default=0.0)
    ap.add_argument('--path-failure-action', choices=['off', 'exit', 'flip', 'reduce'], default='off')
    ap.add_argument('--path-failure-setup', default='all')
    ap.add_argument('--path-failure-session-phase', default='all')
    ap.add_argument('--path-failure-rel-threshold-pct', type=float, default=None)
    ap.add_argument('--path-failure-window-sec', type=int, default=0)
    ap.add_argument('--path-failure-reduce-fraction', type=float, default=0.0)
    ap.add_argument('--entry-warning-gate-mode', choices=['off', 'mom60_rel60_veto', 'ticker_chop_veto'], default='off',
                    help='Replay-only entry veto from decision-time warning signs. off disables.')
    ap.add_argument('--entry-warning-gate-action', choices=['veto', 'confirm_only'], default='veto',
                    help='veto skips warning entries; confirm_only only applies delayed entry confirmation to warning entries.')
    ap.add_argument('--entry-warning-gate-tickers', default='',
                    help='Comma-separated ticker scope for entry warning gate. Empty means all tickers.')
    ap.add_argument('--entry-warning-gate-setup', default='all',
                    help='Setup scope for entry warning gate. all means all setups.')
    ap.add_argument('--entry-warning-gate-session-phase', default='all',
                    help='Session phase scope for entry warning gate. all means all phases.')
    ap.add_argument('--entry-warning-gate-stock-mom60-threshold-pct', type=float, default=-0.10)
    ap.add_argument('--entry-warning-gate-rel60-threshold-pct', type=float, default=-0.10)
    ap.add_argument('--entry-warning-gate-chop-range-threshold-pct', type=float, default=0.75)
    ap.add_argument('--entry-warning-gate-chop-efficiency-threshold', type=float, default=0.20)
    ap.add_argument('--entry-warning-gate-chop-flips-threshold', type=float, default=10)
    ap.add_argument('--entry-warning-gate-max-score', type=float, default=999.0)
    ap.add_argument('--chop-bracket-mode', choices=['off', 'tiny'], default='off',
                    help='Replay-only bracket override: when chop=true at entry, use tiny TP/SL instead of base brackets.')
    ap.add_argument('--chop-bracket-tickers', default='',
                    help='Comma-separated ticker scope for chop bracket override. Empty means all tickers.')
    ap.add_argument('--chop-bracket-range-threshold-pct', type=float, default=0.75)
    ap.add_argument('--chop-bracket-efficiency-threshold', type=float, default=0.20)
    ap.add_argument('--chop-bracket-flips-threshold', type=float, default=10)
    ap.add_argument('--chop-bracket-max-score', type=float, default=999.0)
    ap.add_argument('--chop-bracket-tp-pct', type=float, default=0.0010)
    ap.add_argument('--chop-bracket-sl-pct', type=float, default=0.0010)
    ap.add_argument('--stop-loss-cooldown-tickers', default='',
                    help='Comma-separated ticker scope for post-stop-loss entry cooldown. Empty disables.')
    ap.add_argument('--stop-loss-cooldown-sec', type=int, default=0)
    ap.add_argument('--loss-cluster-throttle-tickers', default='',
                    help='Comma-separated ticker scope for stop-loss cluster throttles. Empty disables.')
    ap.add_argument('--loss-cluster-window-sec', type=int, default=0)
    ap.add_argument('--loss-cluster-count', type=int, default=0)
    ap.add_argument('--loss-cluster-cooldown-sec', type=int, default=0)
    ap.add_argument('--loss-cluster-scope', choices=['ticker', 'ticker_side', 'ticker_side_setup'], default='ticker')
    ap.add_argument('--giveback-exit-after-sec', type=int, default=0)
    ap.add_argument('--giveback-exit-min-mfe-pct', type=float, default=0.0)
    ap.add_argument('--giveback-exit-giveback-pct', type=float, default=0.0)
    ap.add_argument('--giveback-exit-max-pnl-pct', type=float, default=0.0)
    ap.add_argument('--giveback-exit-confirm-sec', type=int, default=0)
    ap.add_argument('--giveback-exit-action', choices=['off', 'exit', 'flip'], default='off')
    ap.add_argument('--giveback-exit-tickers', default='')
    ap.add_argument('--giveback-exit-setup', default='all')
    ap.add_argument('--giveback-exit-cooldown-sec', type=int, default=0)
    ap.add_argument('--profile-replay-timing', action='store_true',
                    help='Collect non-invasive timing buckets for the replay hot path.')
    ap.add_argument('--disable-all-open-signal-skip', action='store_true',
                    help='Parity/debug flag: keep running signal checks even when every ticker already has an open position.')
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    start = _parse_day(args.start)
    end = _parse_day(args.end)
    if end < start:
        raise SystemExit('--end must be on or after --start')
    args.tickers = [t.upper() for t in args.tickers]
    args._scoring_profile_data = scoring_profiles.load_profile(args.scoring_profile)
    original_active_scoring_profile = getattr(ws_scalp, 'ACTIVE_SCORING_PROFILE', None)
    if args._scoring_profile_data:
        ws_scalp.ACTIVE_SCORING_PROFILE = {}
    if args._scoring_profile_data:
        print(f"[engine-replay] scoring profile {args._scoring_profile_data.get('name')}", flush=True)
    days = _selected_market_days(args, start, end)
    if int(getattr(args, 'workers', 1) or 1) > 1:
        all_rows: list[dict] = []
        stats = ReplayStats()
        balance = float(args.start_balance)
        results: dict[str, dict] = {}
        worker_args = _worker_args_dict(args)
        max_workers = worker_policy.clamp_workers(args.workers, len(days))
        print(f"[engine-replay] parallel workers={max_workers} days={len(days)}", flush=True)
        with ProcessPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_run_day_worker, worker_args, day.isoformat()): day for day in days}
            for fut in as_completed(futures):
                day = futures[fut]
                payload = fut.result()
                results[day.isoformat()] = payload
                print(f"[engine-replay] worker done {day.isoformat()} rows={len(payload.get('rows') or [])}", flush=True)
        for day in days:
            payload = results[day.isoformat()]
            rows, balance = _checkpoint_to_compounded_rows(payload, balance)
            all_rows.extend(rows)
            stats.seconds += int(payload.get('seconds_replayed') or 0)
            stats.emitted_signals += int(payload.get('emitted_signals') or 0)
            stats.cache_hits.update(payload.get('cache_hits') or {})
            stats.fetched.update(payload.get('fetched') or {})
            stats.skipped_signals.update(payload.get('skipped_signals') or {})
            stats.timing.update(payload.get('timing') or {})
            print(f"[engine-replay] compound {day.isoformat()} trades={len(rows)} balance={balance:.2f}", flush=True)
        summary = _summarize(all_rows, stats, args, balance)
        csv_path, json_path = _write_outputs(all_rows, summary, args)
        print(json.dumps({
            'trades': summary['trades'],
            'pnl': summary['pnl'],
            'win_rate_pct': summary['win_rate_pct'],
            'csv': csv_path,
            'summary': json_path,
        }, indent=2))
        ws_scalp.ACTIVE_SCORING_PROFILE = original_active_scoring_profile
        return 0
    clock = ReplayClock()
    original_time = ws_scalp.time.time
    original_near_signal_logger = getattr(ws_scalp, '_log_near_signal', None)
    ws_scalp.time.time = clock.time
    if original_near_signal_logger is not None:
        ws_scalp._log_near_signal = lambda *args, **kwargs: None
    all_rows: list[dict] = []
    stats = ReplayStats()
    balance = float(args.start_balance)
    try:
        for day in days:
            checkpoint = _read_day_checkpoint(args, day)
            if checkpoint:
                rows = checkpoint.get('rows') or []
                opportunities = checkpoint.get('opportunities') or []
                balance = float(checkpoint.get('ending_balance', balance) or balance)
            else:
                day_start_balance = balance
                rows, balance, opportunities = _run_day(day, args, clock, stats, balance)
                if args.resume_days:
                    _write_day_checkpoint(args, day, rows, day_start_balance, balance, opportunities)
            _write_opportunity_ledger(args, day, opportunities)
            _write_step2_decision_parity(args, day, opportunities)
            all_rows.extend(rows)
            print(f"[engine-replay] {day.isoformat()} trades={len(rows)} balance={balance:.2f}")
    finally:
        ws_scalp.time.time = original_time
        ws_scalp.ACTIVE_SCORING_PROFILE = original_active_scoring_profile
        if original_near_signal_logger is not None:
            ws_scalp._log_near_signal = original_near_signal_logger
    summary = _summarize(all_rows, stats, args, balance)
    csv_path, json_path = _write_outputs(all_rows, summary, args)
    print(json.dumps({
        'trades': summary['trades'],
        'pnl': summary['pnl'],
        'win_rate_pct': summary['win_rate_pct'],
        'csv': csv_path,
        'summary': json_path,
    }, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
