"""Shared Step 2/live execution contract helpers."""
from __future__ import annotations

import execution_kernel
import execution_state_reducer

from collections import defaultdict
from datetime import datetime, tzinfo
from typing import Any

import step2_parity_contract
from bracket_rounding import round_exit_brackets

CONTRACT_SCHEMA_VERSION = 1


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _ticker(value: Any) -> str:
    return str(value or '').upper()


def execution_contract(config: dict[str, Any]) -> dict[str, Any]:
    parity = step2_parity_contract.contract(config)
    kernel_config = dict(config or {})
    kernel_config['step2'] = parity
    kernel_config['step2_parity'] = parity
    kernel_contract = execution_kernel.contract_from_config(kernel_config)
    return {
        'schema_version': CONTRACT_SCHEMA_VERSION,
        'execution_kernel_hash': kernel_contract.get('execution_kernel_hash'),
        'execution_kernel_contract': kernel_contract,
        'step2_parity_contract_hash': step2_parity_contract.contract_hash(parity),
        'one_open_position_per_ticker': bool(parity.get('one_open_position_per_ticker')),
        'same_ticker_reentry_cooldown_sec': int(parity.get('same_ticker_reentry_cooldown_sec') or 0),
        'max_trades_per_day': int(parity.get('max_trades_per_day') or 0),
        'max_trades_per_ticker_day': int(parity.get('max_trades_per_ticker_day') or 0),
        'conditional_time_stop_enabled': bool(parity.get('conditional_time_stop_enabled')),
        'strict_tp_sl_exits': bool(parity.get('strict_tp_sl_exits')),
        'bracket_rounding': 'side_aware_penny_v1',
        'reentry_anchor': 'closed_at',
        'entry_lock_order': [
            'open_position',
            'pending_entry',
            'same_ticker_reentry_cooldown',
            'density_cap',
        ],
    }


def execution_contract_hash(config: dict[str, Any]) -> str:
    return step2_parity_contract.contract_hash(execution_contract(config))


def exit_brackets(side: str, entry_price: float, sl_pct: float, tp_pct: float) -> dict[str, Any]:
    side = str(side or '').upper()
    price = float(entry_price)
    if side == 'LONG':
        sl_raw = round(price * (1 - float(sl_pct)), 4)
        tp_raw = round(price * (1 + float(tp_pct)), 4)
    elif side == 'SHORT':
        sl_raw = round(price * (1 + float(sl_pct)), 4)
        tp_raw = round(price * (1 - float(tp_pct)), 4)
    else:
        return {'side': side, 'sl_raw': None, 'tp_raw': None, 'sl': None, 'tp': None}
    sl_rounded, tp_rounded = round_exit_brackets(side, sl_raw, tp_raw)
    return {
        'side': side,
        'sl_raw': sl_raw,
        'tp_raw': tp_raw,
        'sl': sl_rounded if sl_rounded is not None else sl_raw,
        'tp': tp_rounded if tp_rounded is not None else tp_raw,
        'rounding': 'side_aware_penny_v1',
    }


def normalize_exit_prices(side: str, sl: float | None, tp: float | None) -> tuple[float | None, float | None]:
    return round_exit_brackets(str(side or '').upper(), sl, tp)


def density_counts(state: dict[str, Any], day_iso: str, tz: tzinfo | None = None) -> dict[str, Any]:
    total = 0
    by_ticker: dict[str, int] = defaultdict(int)

    def add(ticker: Any, ts_value: Any) -> None:
        nonlocal total
        ticker_s = _ticker(ticker)
        if not ticker_s:
            return
        ts = _int(ts_value, 0)
        if ts <= 0:
            return
        if datetime.fromtimestamp(ts, tz).date().isoformat() != day_iso:
            return
        total += 1
        by_ticker[ticker_s] += 1

    for row in state.get('trades', []) or []:
        add(row.get('ticker'), row.get('opened_at') or row.get('entry_ts') or row.get('created_at'))
    for ticker, pos in (state.get('positions') or {}).items():
        add(ticker, pos.get('entry_ts') or pos.get('opened_at') or pos.get('created_at'))
    for ticker, pending in (state.get('pending_entries') or {}).items():
        add(ticker, pending.get('created_at') or pending.get('entry_ts'))
    return {'day': day_iso, 'total': total, 'by_ticker': dict(by_ticker)}


def density_cap_reason(state: dict[str, Any], ticker: str, day_iso: str,
                       max_trades_per_day: int = 0,
                       max_trades_per_ticker_day: int = 0,
                       tz: tzinfo | None = None) -> str | None:
    counts = density_counts(state, day_iso, tz)
    if int(max_trades_per_day or 0) > 0 and counts['total'] >= int(max_trades_per_day):
        return f"step2_density_cap_day:{counts['total']}/{int(max_trades_per_day)}"
    ticker_count = int((counts.get('by_ticker') or {}).get(_ticker(ticker), 0))
    if int(max_trades_per_ticker_day or 0) > 0 and ticker_count >= int(max_trades_per_ticker_day):
        return f"step2_density_cap_ticker:{_ticker(ticker)}={ticker_count}/{int(max_trades_per_ticker_day)}"
    return None


def last_closed_at(state: dict[str, Any], ticker: str) -> int:
    ticker_s = _ticker(ticker)
    for row in reversed(state.get('trades', []) or []):
        if _ticker(row.get('ticker')) != ticker_s:
            continue
        closed_at = _int(row.get('closed_at') or 0, 0)
        if closed_at > 0:
            return closed_at
    return 0


def reentry_cooldown_reason(state: dict[str, Any], ticker: str, signal_ts: int,
                            cooldown_sec: int) -> str | None:
    cooldown = int(cooldown_sec or 0)
    if cooldown <= 0:
        return None
    closed_at = last_closed_at(state, ticker)
    if closed_at <= 0:
        return None
    until = closed_at + cooldown
    ts = int(signal_ts)
    gate = execution_kernel.can_enter_ticker(ts, until)
    if not gate.get('allowed'):
        remaining = max(0, until - ts)
        ticker_s = _ticker(ticker)
        return f"step2_reentry_cooldown:{ticker_s} closed_at={closed_at} until={until} remaining_sec={remaining}"
    return None


def entry_block_reason(state: dict[str, Any], ticker: str, signal_ts: int, day_iso: str,
                       contract: dict[str, Any], signal_scan_live_mode: bool = True,
                       tz: tzinfo | None = None) -> str | None:
    return execution_state_reducer.entry_block_reason(
        state,
        ticker,
        int(signal_ts),
        day_iso,
        contract,
        signal_scan_live_mode=signal_scan_live_mode,
        tz=tz,
    )
