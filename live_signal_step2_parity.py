from __future__ import annotations

from output_paths import output_path

import argparse
import bisect
import gzip
import json
import os
from collections import Counter
from datetime import datetime
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore

import step2_latency_model
import replay_artifacts
import step2_execution_contract
import execution_kernel
import unified_decision_ledger

HERE = os.path.dirname(os.path.abspath(__file__))
POSTMORTEM_DIR = output_path('postmortem')
LIVE_SIGNAL_DIR = os.path.join(POSTMORTEM_DIR, 'live_signal_parity')
OUT_DIR = os.path.join(POSTMORTEM_DIR, 'step2_decision_parity')
DEFAULT_PREPARED_DIR = output_path('data_cache', 'live_intraday_tapes')
CONFIG_PATH = os.path.join(HERE, 'trading_config.json')
CT = ZoneInfo('America/Chicago')
QUOTE_AWARE_EXIT_MODEL_VERSION = 'quote_aware_exit_v1'


def _jsonl_rows(path: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not os.path.exists(path):
        return rows
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows


def _trade_rows(day: str) -> dict[str, dict[str, Any]]:
    path = os.path.join(POSTMORTEM_DIR, 'trades', f'trades_{day}.jsonl')
    return {
        str(row.get('trade_id')): row
        for row in _jsonl_rows(path)
        if row.get('trade_id')
    }


def _load_config() -> dict[str, Any]:
    try:
        with open(CONFIG_PATH, 'r', encoding='utf-8-sig') as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _prepared_path(day: str, tickers: list[str], feed: str, quote_mode: str,
                   btc_mode: str, prepared_cache_dir: str) -> str:
    ticker_part = '-'.join(tickers)
    return os.path.join(
        prepared_cache_dir,
        f'{feed}_{quote_mode}_{btc_mode}_{ticker_part}_{day}.events.json.gz',
    )


def _load_prepared(path: str) -> list[dict[str, Any]]:
    if not os.path.exists(path):
        return []
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        payload = json.load(f)
    return payload if isinstance(payload, list) else []


def _num(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _price_path(events: list[dict[str, Any]]) -> dict[str, tuple[list[int], list[float]]]:
    grouped: dict[str, list[tuple[int, float]]] = {}
    for event in events:
        if event.get('kind') != 'stock_trade':
            continue
        ticker = str(event.get('symbol') or '').upper()
        row = event.get('row') or {}
        if not replay_artifacts.is_executable_stock_trade(row):
            continue
        ts = int(event.get('t') or row.get('t') or 0)
        price = _num(row.get('p'))
        if ticker and ts and price is not None:
            grouped.setdefault(ticker, []).append((ts, float(price)))
    out: dict[str, tuple[list[int], list[float]]] = {}
    for ticker, rows in grouped.items():
        rows.sort(key=lambda item: item[0])
        out[ticker] = ([ts for ts, _ in rows], [price for _, price in rows])
    return out


def _exit_path(events: list[dict[str, Any]]) -> dict[str, tuple[list[int], list[dict[str, Any]]]]:
    grouped: dict[str, list[tuple[int, dict[str, Any]]]] = {}
    quote_counts: Counter[str] = Counter()
    for event in events:
        kind = event.get('kind')
        if kind not in ('stock_trade', 'stock_quote'):
            continue
        ticker = str(event.get('symbol') or '').upper()
        row = event.get('row') or {}
        ts = int(event.get('t') or row.get('t') or 0)
        if not ticker or not ts:
            continue
        if kind == 'stock_quote':
            bid = _num(row.get('bp'))
            ask = _num(row.get('ap'))
            if bid is None and ask is None:
                continue
            quote_counts[ticker] += 1
            grouped.setdefault(ticker, []).append((ts, {
                'kind': 'quote',
                'bid': bid,
                'ask': ask,
                'bid_size': _num(row.get('bs')),
                'ask_size': _num(row.get('as')),
            }))
            continue
        if not replay_artifacts.is_executable_stock_trade(row):
            continue
        price = _num(row.get('p'))
        if price is None:
            continue
        grouped.setdefault(ticker, []).append((ts, {
            'kind': 'trade',
            'price': float(price),
            'size': _num(row.get('s')),
            'conditions': row.get('c') or [],
            'allow_trade_take_profit': False,
        }))
    out: dict[str, tuple[list[int], list[dict[str, Any]]]] = {}
    for ticker, rows in grouped.items():
        allow_trade_take_profit = quote_counts[ticker] == 0
        normalized: list[tuple[int, dict[str, Any]]] = []
        for ts, point in rows:
            if point.get('kind') == 'trade':
                point = dict(point)
                point['allow_trade_take_profit'] = allow_trade_take_profit
            normalized.append((ts, point))
        normalized.sort(key=lambda item: (item[0], 0 if item[1].get('kind') == 'quote' else 1))
        out[ticker] = ([ts for ts, _ in normalized], [point for _, point in normalized])
    return out


def _legacy_exit_point(value: Any) -> dict[str, Any]:
    return {
        'kind': 'trade',
        'price': float(value),
        'allow_trade_take_profit': True,
        'legacy_price_path': True,
    }


def _mark_price(point: dict[str, Any], side: str) -> float | None:
    if point.get('kind') == 'quote':
        bid = _num(point.get('bid'))
        ask = _num(point.get('ask'))
        if side == 'LONG':
            return bid
        if side == 'SHORT':
            return ask
        if bid is not None and ask is not None:
            return (bid + ask) / 2.0
        return bid if bid is not None else ask
    return _num(point.get('price'))


def _exit_trigger(side: str, point: dict[str, Any], sl: float, tp: float) -> tuple[float, str] | None:
    kind = point.get('kind')
    if kind == 'quote':
        bid = _num(point.get('bid'))
        ask = _num(point.get('ask'))
        if side == 'LONG':
            if bid is not None and bid <= sl:
                return sl, 'stop_loss'
            if bid is not None and bid >= tp:
                return tp, 'take_profit'
        else:
            if ask is not None and ask >= sl:
                return sl, 'stop_loss'
            if ask is not None and ask <= tp:
                return tp, 'take_profit'
        return None
    price = _num(point.get('price'))
    if price is None:
        return None
    allow_trade_take_profit = bool(point.get('allow_trade_take_profit'))
    if side == 'LONG':
        if price <= sl:
            return sl, 'stop_loss'
        if allow_trade_take_profit and price >= tp:
            return tp, 'take_profit'
    else:
        if price >= sl:
            return sl, 'stop_loss'
        if allow_trade_take_profit and price <= tp:
            return tp, 'take_profit'
    return None


def _ct_from_ms(ts_ms: int | None) -> str | None:
    if not ts_ms:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, CT).isoformat(timespec='seconds')


def _brackets(row: dict[str, Any], side: str) -> tuple[float | None, float | None]:
    extra = row.get('extra') or {}
    sl = _num(extra.get('sl'))
    tp = _num(extra.get('tp'))
    if sl is not None and tp is not None:
        return step2_execution_contract.normalize_exit_prices(side, sl, tp)
    signal = ((row.get('feature_snapshot') or {}).get('forensics') or {}).get('signal') or {}
    return step2_execution_contract.normalize_exit_prices(
        side,
        _num(signal.get('sl_price')),
        _num(signal.get('tp_price')),
    )


def _latency_chain(row: dict[str, Any], trade: dict[str, Any] | None) -> dict[str, Any]:
    chain: dict[str, Any] = {}
    chain.update((row.get('latency_chain') or {}))
    if trade:
        chain.update((trade.get('latency_chain') or {}))
        broker_close = trade.get('broker_close') or {}
        chain.update((broker_close.get('latency_chain') or {}))
        forensics = trade.get('forensics') or {}
        latency = forensics.get('latency_attribution') or {}
        chain.update((latency.get('chain_ms') or {}))
    return chain


def _actual_entry(row: dict[str, Any], trade: dict[str, Any] | None,
                  model: dict[str, Any], latency_percentile: str) -> tuple[float | None, int, str]:
    signal_ts_ms = int(float(row.get('created_at') or 0) * 1000)
    signal_price = _num(row.get('price'))
    if not trade:
        ticker = str(row.get('ticker') or '').upper()
        side = str(row.get('side') or '').upper()
        delay = step2_latency_model.latency_ms(model, ticker, side, 'entry_delay_ms', latency_percentile, context=row)
        bps = step2_latency_model.slippage_bps(model, ticker, side, 'entry_slippage_bps', 'p50', context=row)
        fill_price = step2_latency_model.apply_slippage(float(signal_price or 0.0), side, bps, 'entry') if signal_price else signal_price
        return fill_price, signal_ts_ms + delay, f'modeled_entry_latency_{latency_percentile}'
    chain = _latency_chain(row, trade)
    entry_ts = int(chain.get('entry_filled_ms') or chain.get('committed_ms') or signal_ts_ms)
    fill_price = (
        _num(trade.get('entry_fill_price'))
        or _num(((trade.get('forensics') or {}).get('entry_fill_price')))
        or _num(trade.get('entry'))
        or signal_price
    )
    source = 'live_entry_fill' if chain.get('entry_filled_ms') else 'live_committed_or_trade_entry'
    return fill_price, entry_ts, source


def _simulate_outcome(row: dict[str, Any], paths: dict[str, tuple[list[int], list[Any]]],
                      trade: dict[str, Any] | None = None,
                      model: dict[str, Any] | None = None,
                      latency_percentile: str = 'p75') -> dict[str, Any] | None:
    if row.get('decision') != 'entered':
        return None
    ticker = str(row.get('ticker') or '').upper()
    side = str(row.get('side') or '').upper()
    signal_entry = _num(row.get('price'))
    model = model or step2_latency_model.load_model()
    entry, ts_ms, entry_source = _actual_entry(row, trade, model, latency_percentile)
    sl, tp = _brackets(row, side)
    if not ticker or side not in ('LONG', 'SHORT') or entry is None or sl is None or tp is None:
        return {
            'reason': 'missing_replay_inputs',
            'entry': entry,
            'exit': None,
            'pnl': None,
        }
    times, points = paths.get(ticker, ([], []))
    idx = bisect.bisect_left(times, ts_ms)
    exit_price = None
    exit_ts = None
    exit_evidence = None
    reason = 'no_exit'
    mfe = 0.0
    mae = 0.0
    last_price = entry
    last_ts = ts_ms
    for i in range(idx, len(times)):
        raw_point = points[i]
        point = raw_point if isinstance(raw_point, dict) else _legacy_exit_point(raw_point)
        price = _mark_price(point, side)
        if price is None:
            continue
        last_price = price
        last_ts = times[i]
        if side == 'LONG':
            pnl_pct = (price - entry) / entry
            mfe = max(mfe, pnl_pct)
            mae = min(mae, pnl_pct)
        else:
            pnl_pct = (entry - price) / entry
            mfe = max(mfe, pnl_pct)
            mae = min(mae, pnl_pct)
        trigger = _exit_trigger(side, point, sl, tp)
        if trigger:
            exit_price, reason = trigger
            exit_ts = times[i]
            exit_evidence = {
                'model': QUOTE_AWARE_EXIT_MODEL_VERSION,
                'kind': point.get('kind'),
                'price': point.get('price'),
                'bid': point.get('bid'),
                'ask': point.get('ask'),
                'bid_size': point.get('bid_size'),
                'ask_size': point.get('ask_size'),
                'allow_trade_take_profit': point.get('allow_trade_take_profit'),
                'legacy_price_path': point.get('legacy_price_path'),
            }
            break
    if exit_price is None:
        exit_price = last_price
        exit_ts = last_ts
    elif not trade:
        exit_delay = step2_latency_model.latency_ms(model, ticker, side, 'exit_delay_ms', latency_percentile, context=row)
        exit_bps = step2_latency_model.slippage_bps(model, ticker, side, 'exit_slippage_bps', 'p50', context=row)
        exit_ts = int(exit_ts or ts_ms) + exit_delay
        exit_price = step2_latency_model.apply_slippage(float(exit_price), side, exit_bps, 'exit')
    qty = _num((row.get('extra') or {}).get('qty'))
    alloc = _num((row.get('extra') or {}).get('alloc'))
    if qty is None and alloc is not None and entry:
        qty = alloc / entry
    qty = qty or 0.0
    pnl = (exit_price - entry) * qty
    if side == 'SHORT':
        pnl *= -1
    return {
        'reason': reason,
        'entry': round(entry, 4),
        'signal_entry': round(signal_entry, 4) if signal_entry is not None else None,
        'entry_source': entry_source,
        'latency_model_percentile': latency_percentile,
        'entry_replay_start_ct': _ct_from_ms(ts_ms),
        'exit': round(exit_price, 4),
        'entry_ct': row.get('created_at_ct'),
        'exit_ct': _ct_from_ms(exit_ts),
        'held_sec': int(round(((exit_ts or ts_ms) - ts_ms) / 1000.0)),
        'pnl': round(pnl, 4),
        'mfe_pct': round(mfe * 100.0, 4),
        'mae_pct': round(mae * 100.0, 4),
        'sl': round(sl, 4),
        'tp': round(tp, 4),
        'qty': round(qty, 6),
        'exit_evidence': exit_evidence,
    }


def _row_from_live(row: dict[str, Any], outcome: dict[str, Any] | None, run_context: dict[str, Any]) -> dict[str, Any]:
    decision = row.get('decision')
    extra = row.get('extra') if isinstance(row.get('extra'), dict) else {}
    return {
        'schema_version': 1,
        'source': 'live_signal_fast_step2_parity',
        'day': run_context.get('day'),
        'created_at': row.get('created_at'),
        'created_at_ct': row.get('created_at_ct'),
        'parity_key': row.get('parity_key'),
        'opportunity_id': row.get('opportunity_id'),
        'ticker': row.get('ticker'),
        'side': row.get('side'),
        'setup_type': row.get('setup_type'),
        'decision': decision,
        'step2_decision': 'accepted' if decision == 'entered' else 'rejected',
        'reason': row.get('reason'),
        'price': row.get('price'),
        'score': row.get('score'),
        'conviction': row.get('conviction'),
        'reasons': row.get('reasons') or [],
        'execution_mode': 'live_signal_fast_step2_parity',
        'strategy_config_hash': row.get('strategy_config_hash'),
        'execution_kernel_hash': row.get('execution_kernel_hash'),
        'step2_parity_contract_hash': row.get('step2_parity_contract_hash'),
        'step2_execution_contract_hash': row.get('step2_execution_contract_hash'),
        'active_profile_name': row.get('active_profile_name'),
        'active_profile_hash': row.get('active_profile_hash'),
        'active_profile_bias': row.get('active_profile_bias'),
        'feature_snapshot_hash': row.get('feature_snapshot_hash'),
        'feature_snapshot': row.get('feature_snapshot'),
        'market_freshness': row.get('market_freshness') or {},
        'latency_chain': row.get('latency_chain') or {},
        'operational_state': row.get('operational_state') or {},
        'extra': extra,
        'bracket_policy': row.get('bracket_policy') or extra.get('bracket_policy') or {},
        'action_plan': row.get('action_plan') or extra.get('action_plan'),
        'execution_intent': row.get('execution_intent') or extra.get('execution_intent'),
        'execution_result': row.get('execution_result') or extra.get('execution_result'),
        'run_context': run_context,
        'outcome': outcome,
        'outcome_summary': {
            'pnl': outcome.get('pnl'),
            'reason': outcome.get('reason'),
            'entry': outcome.get('entry'),
            'exit': outcome.get('exit'),
            'entry_ct': outcome.get('entry_ct'),
            'exit_ct': outcome.get('exit_ct'),
            'held_sec': outcome.get('held_sec'),
        } if outcome else None,
        'trade_id': row.get('trade_id'),
        'client_order_id': row.get('client_order_id'),
        'broker_order_id': row.get('broker_order_id'),
        'join_hint': row.get('join_hint') or {
            'ticker': row.get('ticker'),
            'side': row.get('side'),
            'timestamp_second': row.get('created_at'),
            'setup_type': row.get('setup_type'),
            'score': row.get('score'),
        },
    }


def build(day: str, tickers: list[str], feed: str, quote_mode: str, btc_mode: str,
          prepared_cache_dir: str, out_dir: str,
          latency_model_path: str | None = None,
          latency_percentile: str = 'p75') -> dict[str, Any]:
    tickers = [ticker.upper() for ticker in tickers]
    signal_path = os.path.join(LIVE_SIGNAL_DIR, f'live_signal_parity_{day}.jsonl')
    prepared_path = _prepared_path(day, tickers, feed, quote_mode, btc_mode, prepared_cache_dir)
    signals = [
        row for row in _jsonl_rows(signal_path)
        if str(row.get('ticker') or '').upper() in tickers
    ]
    trades = _trade_rows(day)
    events = _load_prepared(prepared_path)
    paths = _exit_path(events)
    latency_model = step2_latency_model.load_model(latency_model_path)
    config = _load_config()
    execution_contract = execution_kernel.contract_from_config(config)
    step2_contract = step2_execution_contract.execution_contract(config)
    run_context = {
        'execution_kernel_contract': execution_contract,
        'execution_kernel_hash': execution_contract.get('execution_kernel_hash'),
        'step2_parity_contract_hash': step2_contract.get('step2_parity_contract_hash'),
        'step2_execution_contract_hash': step2_execution_contract.execution_contract_hash(config),
        'step2_execution_contract': step2_contract,
        'day': day,
        'method': 'live_signal_fast_step2_parity',
        'feed': feed,
        'quote_mode': quote_mode,
        'btc_mode': btc_mode,
        'tickers': tickers,
        'prepared_cache_dir': os.path.abspath(prepared_cache_dir),
        'prepared_path': os.path.abspath(prepared_path),
        'live_signal_path': os.path.abspath(signal_path),
        'start_balance': 100000.0,
        'latency_model_path': os.path.abspath(latency_model_path or step2_latency_model.DEFAULT_MODEL_PATH),
        'latency_model_source': latency_model.get('source'),
        'latency_model_sample_count': latency_model.get('sample_count'),
        'latency_percentile': latency_percentile,
        'exit_model': QUOTE_AWARE_EXIT_MODEL_VERSION,
        'deduction': (
            'Fast parity starts from Live signal rows, preserving Live parity keys, then replays only '
            'future quote-aware stock exits from the actual Live fill/commit time when available. '
            'Take-profit exits require an executable top-of-book quote when quotes are available, '
            'so isolated odd-lot trade prints cannot create optimistic fills. '
            'It avoids rediscovering signals from the full tape.'
        ),
    }
    rows = [
        _row_from_live(
            row,
            _simulate_outcome(
                row,
                paths,
                trades.get(str(row.get('trade_id'))),
                model=latency_model,
                latency_percentile=latency_percentile,
            ),
            run_context,
        )
        for row in signals
    ]
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f'step2_decision_parity_{day}.jsonl')
    with open(path, 'w', encoding='utf-8') as f:
        for row in rows:
            f.write(json.dumps(row, sort_keys=True, separators=(',', ':'), default=str) + '\n')
    entered = [row for row in rows if row.get('decision') == 'entered']
    skipped = [row for row in rows if row.get('decision') == 'skipped']
    pnl = round(sum(float(((row.get('outcome_summary') or {}).get('pnl')) or 0.0) for row in entered), 4)
    summary = {
        'schema_version': 1,
        'source': 'live_signal_fast_step2_parity',
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'day': day,
        'path': os.path.abspath(path),
        'rows': len(rows),
        'entered': len(entered),
        'skipped': len(skipped),
        'pnl': pnl,
        'prepared_event_rows': len(events),
        'live_signal_rows': len(signals),
        'decisions': dict(Counter(str(row.get('decision') or 'unknown') for row in rows)),
        'outcome_reasons': dict(Counter(str((row.get('outcome_summary') or {}).get('reason') or 'none') for row in entered)),
        'run_context': run_context,
    }
    summary_path = os.path.join(out_dir, f'step2_decision_parity_{day}.summary.json')
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, sort_keys=True)
    summary['summary_path'] = os.path.abspath(summary_path)
    try:
        summary['unified_decision_ledger'] = unified_decision_ledger.write_live_signal_parity(day, rows)
    except Exception as exc:
        summary['unified_decision_ledger_error'] = repr(exc)
    return summary


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Build fast Step 2 parity ledger from actual Live signal rows.')
    ap.add_argument('--day', default=datetime.now(CT).date().isoformat())
    ap.add_argument('--tickers', nargs='+', default=['CLSK', 'MARA', 'RIOT'])
    ap.add_argument('--feed', default='sip')
    ap.add_argument('--quote-mode', default='per-second')
    ap.add_argument('--btc-mode', default='bars')
    ap.add_argument('--prepared-cache-dir', default=DEFAULT_PREPARED_DIR)
    ap.add_argument('--out-dir', default=OUT_DIR)
    ap.add_argument('--latency-model', default=step2_latency_model.DEFAULT_MODEL_PATH)
    ap.add_argument('--latency-percentile', default='p75', choices=['p50', 'p75', 'p95', 'default'])
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    summary = build(
        day=args.day,
        tickers=args.tickers,
        feed=args.feed,
        quote_mode=args.quote_mode,
        btc_mode=args.btc_mode,
        prepared_cache_dir=args.prepared_cache_dir,
        out_dir=args.out_dir,
        latency_model_path=args.latency_model,
        latency_percentile=args.latency_percentile,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
