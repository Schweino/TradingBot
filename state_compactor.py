from __future__ import annotations

from output_paths import output_path

import argparse
import json
import os
import time
from collections import defaultdict
from datetime import datetime

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo


HERE = os.path.dirname(os.path.abspath(__file__))
STATE_PATH = output_path('mock_trader_state.json')
SKIPPED_DIR = output_path('postmortem', 'skipped_signals')
TRADE_CORPUS_DIR = output_path('postmortem', 'trades')
CT = ZoneInfo('America/Chicago')


def _compact_skipped(row: dict) -> dict:
    keys = (
        'created_at', 'ticker', 'side', 'price', 'reason', 'conviction',
        'score', 'fwd_targets', 'fwd', 'strategy_config_hash',
    )
    return {k: row.get(k) for k in keys if k in row}


def compact_state(path: str = STATE_PATH) -> dict:
    with open(path, 'r', encoding='utf-8') as f:
        state = json.load(f)
    skipped = list(state.get('skipped_signals') or [])
    trades = list(state.get('trades') or [])
    state['skipped_signals'] = [_compact_skipped(r) for r in skipped[-200:]]
    state['trades'] = trades[-500:]
    state['_state_persistence'] = {
        'schema': 2,
        'saved_at': int(time.time()),
        'compact_skipped_cache_count': len(state['skipped_signals']),
        'compact_trade_cache_count': len(state['trades']),
        'full_skipped_corpus_dir': SKIPPED_DIR,
        'full_trade_corpus_dir': TRADE_CORPUS_DIR,
        'event_store': 'postmortem/trading_events.sqlite',
    }
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(state, f, separators=(',', ':'), default=str)
    os.replace(tmp, path)
    return state['_state_persistence']


def _day_from_trade(trade: dict) -> str | None:
    ts = trade.get('closed_at') or trade.get('opened_at') or trade.get('entry_ts')
    try:
        ts = float(ts)
        if ts > 10_000_000_000:
            ts /= 1000.0
        return datetime.fromtimestamp(ts, CT).date().isoformat()
    except Exception:
        return None


def _trade_key(trade: dict) -> tuple:
    return (
        trade.get('trade_id'),
        trade.get('ticker'),
        trade.get('side'),
        trade.get('opened_at') or trade.get('entry_ts'),
        trade.get('closed_at'),
        trade.get('pnl'),
    )


def backfill_trade_corpus(path: str = STATE_PATH) -> dict:
    with open(path, 'r', encoding='utf-8') as f:
        state = json.load(f)
    trades = [t for t in (state.get('trades') or []) if isinstance(t, dict)]
    os.makedirs(TRADE_CORPUS_DIR, exist_ok=True)
    by_day = defaultdict(list)
    for trade in trades:
        day = _day_from_trade(trade)
        if day:
            by_day[day].append(trade)

    day_counts = {}
    for day, rows in by_day.items():
        out_path = os.path.join(TRADE_CORPUS_DIR, f'trades_{day}.jsonl')
        existing = set()
        if os.path.exists(out_path):
            with open(out_path, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        existing.add(_trade_key(json.loads(line)))
                    except Exception:
                        continue
        appended = 0
        with open(out_path, 'a', encoding='utf-8') as f:
            for trade in rows:
                key = _trade_key(trade)
                if key in existing:
                    continue
                f.write(json.dumps(trade, separators=(',', ':'), default=str) + '\n')
                existing.add(key)
                appended += 1
        day_counts[day] = appended
    return {
        'source_state': path,
        'trade_corpus_dir': TRADE_CORPUS_DIR,
        'days': dict(sorted(day_counts.items())),
        'total_appended': sum(day_counts.values()),
    }


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--backfill-trades', action='store_true')
    parser.add_argument('--no-compact', action='store_true')
    args = parser.parse_args()
    result = {}
    if not args.no_compact:
        result['compact_state'] = compact_state()
    if args.backfill_trades:
        result['backfill_trade_corpus'] = backfill_trade_corpus()
    print(json.dumps(result, indent=2))
