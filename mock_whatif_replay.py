"""Mock-trader what-if replay from actual entered and skipped signal corpora.

This is a diagnostic bridge between fast Step 3 and Step 4/mock parity. It
starts from the live-observed mock signal stream, preserves actual entered
trades exactly, and can re-admit selected skipped signals under alternate gate
settings.
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore


HERE = os.path.dirname(os.path.abspath(__file__))
CT = ZoneInfo('America/Chicago')
TRADE_DIR = os.path.join(HERE, 'postmortem', 'trades')
SKIP_DIR = os.path.join(HERE, 'postmortem', 'skipped_signals')
DEFAULT_OUT_DIR = os.path.join(HERE, 'postmortem', 'backtests', 'mock_whatif_replay')
EVENT_TAPE_DIR = os.path.join(HERE, 'data_cache', 'alpaca_engine_replay_tapes')
TICKERS = ['CLSK', 'MARA', 'RIOT']
BRACKETS = {
    'CLSK': {'tp': 0.0035, 'sl': 0.0045},
    'MARA': {'tp': 0.0030, 'sl': 0.0040},
    'RIOT': {'tp': 0.0018, 'sl': 0.0065},
}
RISK_OFF_IGNORE_REASONS = {'manual_stop', 'session_end', 'external_broker_exit'}


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ''):
            return default
        return float(value)
    except Exception:
        return default


def _parse_day(value: str) -> date:
    return datetime.strptime(value, '%Y-%m-%d').date()


def _market_days(start: str, end: str) -> list[str]:
    cur = _parse_day(start)
    stop = _parse_day(end)
    out = []
    while cur <= stop:
        if cur.weekday() < 5:
            out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def _read_jsonl(path: str) -> list[dict]:
    rows = []
    if not os.path.exists(path):
        return rows
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    return rows


def _event_tape_path(day: str) -> str:
    return os.path.join(
        EVENT_TAPE_DIR,
        f'sip_per-second_bars_CLSK-MARA-RIOT_{day}.events.json.gz',
    )


def _load_price_series(day: str) -> dict[str, dict[int, float]]:
    path = _event_tape_path(day)
    if not os.path.exists(path):
        raise FileNotFoundError(f'prepared event tape missing: {path}')
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        events = json.load(f)
    raw: dict[str, dict[int, float]] = defaultdict(dict)
    all_secs = []
    for event in events:
        if event.get('kind') != 'stock_trade':
            continue
        sym = str(event.get('symbol') or '').upper()
        if sym not in TICKERS:
            continue
        sec = int(event.get('t') or 0) // 1000
        px = _num((event.get('row') or {}).get('p'))
        if px > 0:
            raw[sym][sec] = px
            all_secs.append(sec)
    if not all_secs:
        return {}
    start, end = min(all_secs), max(all_secs)
    series: dict[str, dict[int, float]] = {}
    for sym, rows in raw.items():
        filled = {}
        last = None
        for sec in range(start, end + 1):
            if sec in rows:
                last = rows[sec]
            if last is not None:
                filled[sec] = last
        series[sym] = filled
    return series


def _load_actual(day: str) -> list[dict]:
    rows = []
    for row in _read_jsonl(os.path.join(TRADE_DIR, f'trades_{day}.jsonl')):
        opened = int(_num(row.get('opened_at')))
        closed = int(_num(row.get('closed_at')))
        if not opened or not closed:
            continue
        rows.append({
            'kind': 'actual',
            'day': day,
            'ts': opened,
            'ticker': row.get('ticker'),
            'side': row.get('side'),
            'price': _num(row.get('entry')),
            'actual_exit_ts': closed,
            'actual_pnl': _num(row.get('pnl')),
            'actual_net_pnl': _num(row.get('net_pnl_after_estimated_costs'), _num(row.get('pnl'))),
            'actual_reason': row.get('reason'),
            'trade_id': row.get('trade_id'),
            'strategy_config_hash': row.get('strategy_config_hash') or ((row.get('forensics') or {}).get('strategy') or {}).get('config_hash'),
        })
    return rows


def _load_skips(day: str) -> list[dict]:
    rows = []
    for row in _read_jsonl(os.path.join(SKIP_DIR, f'skipped_signals_{day}.jsonl')):
        ts = int(_num(row.get('created_at')))
        ticker = str(row.get('ticker') or '').upper()
        side = str(row.get('side') or '').upper()
        if not ts or ticker not in TICKERS or side not in ('LONG', 'SHORT'):
            continue
        rows.append({
            'kind': 'skip',
            'day': day,
            'ts': ts,
            'ticker': ticker,
            'side': side,
            'price': _num(row.get('price')),
            'skip_reason': str(row.get('reason') or ''),
            'score': _num(row.get('score')),
            'conviction': row.get('conviction'),
            'setup_type': row.get('setup_type') or ((row.get('forensics') or {}).get('setup_type')),
            'strategy_config_hash': row.get('strategy_config_hash'),
        })
    return rows


def _risk_streak(trades: list[dict], now: int, max_losses: int, cooldown_min: int) -> str | None:
    if max_losses <= 0:
        return None
    streak = 0
    last_loss_closed_at = None
    for trade in reversed(trades):
        if trade.get('reason') in RISK_OFF_IGNORE_REASONS:
            continue
        if _num(trade.get('pnl')) < 0:
            streak += 1
            last_loss_closed_at = last_loss_closed_at or int(_num(trade.get('exit_ts')))
        else:
            break
    if streak < max_losses:
        return None
    if cooldown_min <= 0:
        return f'consecutive_losses_{streak}'
    if last_loss_closed_at and (now - last_loss_closed_at) / 60.0 < cooldown_min:
        return f'consecutive_losses_{streak}'
    return None


def _hypothetical_outcome(row: dict, prices: dict[str, dict[int, float]],
                          max_hold_sec: int = 900) -> dict | None:
    sym = row['ticker']
    side = row['side']
    ts = int(row['ts'])
    entry = _num(row.get('price')) or _num(prices.get(sym, {}).get(ts))
    if entry <= 0:
        return None
    cfg = BRACKETS.get(sym, BRACKETS['CLSK'])
    tp_px = entry * (1 + cfg['tp']) if side == 'LONG' else entry * (1 - cfg['tp'])
    sl_px = entry * (1 - cfg['sl']) if side == 'LONG' else entry * (1 + cfg['sl'])
    end = max(prices.get(sym, {}) or {ts: entry})
    last = entry
    exit_ts = min(end, ts + max_hold_sec)
    exit_px = entry
    reason = 'timeout'
    for sec in range(ts, min(end, ts + max_hold_sec) + 1):
        px = prices.get(sym, {}).get(sec)
        if px is None:
            continue
        last = px
        if side == 'LONG':
            if px >= tp_px:
                exit_ts, exit_px, reason = sec, tp_px, 'take_profit'
                break
            if px <= sl_px:
                exit_ts, exit_px, reason = sec, sl_px, 'stop_loss'
                break
        else:
            if px <= tp_px:
                exit_ts, exit_px, reason = sec, tp_px, 'take_profit'
                break
            if px >= sl_px:
                exit_ts, exit_px, reason = sec, sl_px, 'stop_loss'
                break
    else:
        exit_px = last
    pnl_pct = (exit_px - entry) / entry * 100.0
    if side == 'SHORT':
        pnl_pct *= -1.0
    return {
        'entry': round(entry, 4),
        'exit': round(exit_px, 4),
        'exit_ts': int(exit_ts),
        'held_sec': int(exit_ts - ts),
        'reason': reason,
        'pnl_pct': round(pnl_pct, 6),
    }


def _allow_skip(row: dict, mode: str, risk_max_losses: int, risk_cooldown_min: int,
                trades: list[dict], now: int) -> tuple[bool, str]:
    reason = str(row.get('skip_reason') or '')
    if mode == 'none':
        return False, 'skip_preserved'
    if mode == 'all_risk':
        prefixes = ('risk_off:', 'setup_paused:', 'market_regime_conflict:')
        return (reason.startswith(prefixes), 'risk_block_removed' if reason.startswith(prefixes) else 'skip_preserved')
    if mode == 'risk_off':
        if not reason.startswith('risk_off:'):
            return False, 'skip_preserved'
        new_block = _risk_streak(trades, now, risk_max_losses, risk_cooldown_min)
        if new_block:
            return False, f'risk_off_still_blocks:{new_block}'
        return True, 'risk_off_removed_or_loosened'
    if mode == 'setup_paused':
        return (reason.startswith('setup_paused:'), 'setup_pause_removed' if reason.startswith('setup_paused:') else 'skip_preserved')
    if mode == 'market_conflict':
        return (reason.startswith('market_regime_conflict:'), 'market_conflict_removed' if reason.startswith('market_regime_conflict:') else 'skip_preserved')
    return False, 'unknown_mode'


def simulate(days: list[str], mode: str, start_balance: float, risk_max_losses: int,
             risk_cooldown_min: int, include_actual: bool = True,
             include_skips: bool = True, actual_exit_mode: str = 'actual') -> dict:
    prices_by_day = {day: _load_price_series(day) for day in days}
    stream = []
    for day in days:
        if include_actual:
            stream.extend(_load_actual(day))
        if include_skips:
            stream.extend(_load_skips(day))
    stream.sort(key=lambda row: (row['ts'], 0 if row['kind'] == 'actual' else 1, row['ticker']))

    open_until: dict[str, int] = defaultdict(int)
    balance = float(start_balance)
    trades = []
    skips = Counter()
    skip_reasons = Counter()
    for row in stream:
        day = row['day']
        ticker = row['ticker']
        ts = int(row['ts'])
        if ts < open_until[ticker]:
            skips['already_open'] += 1
            continue
        if row['kind'] == 'skip':
            allowed, why = _allow_skip(row, mode, risk_max_losses, risk_cooldown_min, trades, ts)
            if not allowed:
                skips[why] += 1
                skip_reasons[row.get('skip_reason')] += 1
                continue
            outcome = _hypothetical_outcome(row, prices_by_day[day])
            if not outcome:
                skips['no_outcome'] += 1
                continue
            alloc = balance * 0.25 / max(1, len(TICKERS))
            pnl = alloc * _num(outcome.get('pnl_pct')) / 100.0
            balance += pnl
            trade = {
                'kind': 'whatif_skip',
                'day': day,
                'ticker': ticker,
                'side': row['side'],
                'entry_ts': ts,
                'entry_ct': datetime.fromtimestamp(ts, timezone.utc).astimezone(CT).strftime('%Y-%m-%d %H:%M:%S'),
                'exit_ts': outcome['exit_ts'],
                'held_sec': outcome['held_sec'],
                'entry': outcome['entry'],
                'exit': outcome['exit'],
                'pnl': round(pnl, 6),
                'pnl_pct': outcome['pnl_pct'],
                'reason': outcome['reason'],
                'source_skip_reason': row.get('skip_reason'),
                'score': row.get('score'),
                'conviction': row.get('conviction'),
            }
        else:
            if actual_exit_mode == 'strict':
                strict_row = dict(row)
                outcome = _hypothetical_outcome(strict_row, prices_by_day[day])
                if not outcome:
                    skips['no_actual_strict_outcome'] += 1
                    continue
                alloc = balance * 0.25 / max(1, len(TICKERS))
                pnl = alloc * _num(outcome.get('pnl_pct')) / 100.0
                balance += pnl
                row_exit_ts = outcome['exit_ts']
                row_held = outcome['held_sec']
                row_reason = outcome['reason']
                row_entry = outcome['entry']
                row_exit = outcome['exit']
                row_pnl_pct = outcome['pnl_pct']
            else:
                pnl = row['actual_pnl']
                balance += pnl
                row_exit_ts = row['actual_exit_ts']
                row_held = int(row['actual_exit_ts'] - ts)
                row_reason = row.get('actual_reason')
                row_entry = row['price']
                row_exit = None
                row_pnl_pct = None
            trade = {
                'kind': 'actual',
                'day': day,
                'ticker': ticker,
                'side': row['side'],
                'entry_ts': ts,
                'entry_ct': datetime.fromtimestamp(ts, timezone.utc).astimezone(CT).strftime('%Y-%m-%d %H:%M:%S'),
                'exit_ts': row_exit_ts,
                'held_sec': row_held,
                'entry': row_entry,
                'exit': row_exit,
                'pnl': round(pnl, 6),
                'pnl_pct': row_pnl_pct,
                'reason': row_reason,
                'source_skip_reason': None,
                'score': None,
                'conviction': None,
            }
        open_until[ticker] = int(trade['exit_ts'])
        trades.append(trade)
    payload = _summary(days, mode, start_balance, balance, trades, skips, skip_reasons, risk_max_losses, risk_cooldown_min)
    payload['actual_exit_mode'] = actual_exit_mode
    return payload


def _summary(days: list[str], mode: str, start_balance: float, balance: float,
             trades: list[dict], skips: Counter, skip_reasons: Counter,
             risk_max_losses: int, risk_cooldown_min: int) -> dict:
    pnl = sum(_num(t.get('pnl')) for t in trades)
    by_day = defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0})
    by_ticker = defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0})
    by_kind = defaultdict(lambda: {'trades': 0, 'pnl': 0.0})
    for t in trades:
        for bucket, key in ((by_day, t['day']), (by_ticker, t['ticker'])):
            item = bucket[key]
            item['trades'] += 1
            item['pnl'] += _num(t.get('pnl'))
            item['wins'] += int(_num(t.get('pnl')) > 0)
            item['losses'] += int(_num(t.get('pnl')) < 0)
        by_kind[t['kind']]['trades'] += 1
        by_kind[t['kind']]['pnl'] += _num(t.get('pnl'))
    for bucket in (by_day, by_ticker, by_kind):
        for item in bucket.values():
            item['pnl'] = round(item['pnl'], 4)
            if 'wins' in item:
                item['win_rate_pct'] = round(item['wins'] / item['trades'] * 100.0, 2) if item['trades'] else None
    wins = sum(1 for t in trades if _num(t.get('pnl')) > 0)
    losses = sum(1 for t in trades if _num(t.get('pnl')) < 0)
    return {
        'method': 'mock_whatif_replay',
        'days': days,
        'mode': mode,
        'risk_max_consecutive_losses': risk_max_losses,
        'risk_cooldown_min': risk_cooldown_min,
        'starting_balance': round(start_balance, 2),
        'ending_balance': round(balance, 4),
        'pnl': round(pnl, 4),
        'trades': len(trades),
        'wins': wins,
        'losses': losses,
        'win_rate_pct': round(wins / len(trades) * 100.0, 2) if trades else None,
        'by_day': dict(sorted(by_day.items())),
        'by_ticker': dict(sorted(by_ticker.items())),
        'by_kind': dict(sorted(by_kind.items())),
        'skipped': dict(skips.most_common()),
        'preserved_skip_reasons': dict(skip_reasons.most_common(25)),
        'trades_rows': trades,
        'notes': [
            'Actual mock trades are preserved with exact actual P/L and exit timestamps.',
            'Re-admitted skipped signals use SIP path fixed-bracket outcomes; conviction-decay/profit-protect exits are not perfectly reconstructed for hypothetical entries.',
            'This is intended for gate diagnostics before live config changes.',
        ],
    }


def _write_outputs(payload: dict, out_dir: str) -> str:
    os.makedirs(out_dir, exist_ok=True)
    stem = f"mock_whatif_{payload['days'][0]}_{payload['days'][-1]}_{payload['mode']}_{payload.get('actual_exit_mode','actual')}_loss{payload['risk_max_consecutive_losses']}_cool{payload['risk_cooldown_min']}"
    path = os.path.join(out_dir, f'{stem}.json')
    summary = dict(payload)
    summary['trades_rows'] = payload['trades_rows'][:5000]
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, sort_keys=True)
    return path


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Run mock-trader what-if replay from live corpora.')
    ap.add_argument('--start', required=True)
    ap.add_argument('--end', required=True)
    ap.add_argument('--mode', choices=['none', 'risk_off', 'all_risk', 'setup_paused', 'market_conflict'], default='none')
    ap.add_argument('--start-balance', type=float, default=100000.0)
    ap.add_argument('--risk-max-consecutive-losses', type=int, default=2)
    ap.add_argument('--risk-cooldown-min', type=int, default=30)
    ap.add_argument('--actual-exit-mode', choices=['actual', 'strict'], default='actual',
                    help='actual preserves real mock exits/P&L; strict replays actual entries with fixed TP/SL path exits.')
    ap.add_argument('--out-dir', default=DEFAULT_OUT_DIR)
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    days = _market_days(args.start, args.end)
    payload = simulate(
        days,
        mode=args.mode,
        start_balance=float(args.start_balance),
        risk_max_losses=int(args.risk_max_consecutive_losses),
        risk_cooldown_min=int(args.risk_cooldown_min),
        actual_exit_mode=args.actual_exit_mode,
    )
    path = _write_outputs(payload, args.out_dir)
    compact = {k: v for k, v in payload.items() if k != 'trades_rows'}
    compact['out'] = os.path.abspath(path)
    print(json.dumps(compact, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
