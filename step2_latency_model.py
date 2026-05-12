from __future__ import annotations

from output_paths import output_path

import argparse
import json
import os
from collections import defaultdict
from datetime import datetime
from statistics import median
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore


HERE = os.path.dirname(os.path.abspath(__file__))
POSTMORTEM_DIR = output_path('postmortem')
OUT_DIR = os.path.join(POSTMORTEM_DIR, 'latency_model')
DEFAULT_MODEL_PATH = os.path.join(OUT_DIR, 'step2_latency_model.json')
CT = ZoneInfo('America/Chicago')


DEFAULT_MODEL = {
    'schema_version': 2,
    'source': 'default_latency_model',
    'created_at_ct': None,
    'sample_count': 0,
    'v2': {
        'enabled': True,
        'min_group_samples': 5,
        'group_order': [
            'ticker_side_session_spread',
            'ticker_side_session',
            'ticker_side_spread',
            'ticker_side',
            'global',
        ],
    },
    'entry_delay_ms': {'p50': 4000, 'p75': 8000, 'p95': 18000, 'default': 8000},
    'exit_delay_ms': {'p50': 0, 'p75': 0, 'p95': 0, 'default': 0},
    'entry_slippage_bps': {'p50': 0.0, 'p75': 0.0, 'p95': 0.0, 'default': 0.0},
    'exit_slippage_bps': {'p50': 0.0, 'p75': 0.0, 'p95': 0.0, 'default': 0.0},
    'groups': {},
    'groups_v2': {},
}


def _jsonl_rows(path: str) -> list[dict[str, Any]]:
    if not os.path.exists(path):
        return []
    rows = []
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


def _num(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _pct(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * q))))
    return float(ordered[idx])


def _stats(values: list[float], default: float = 0.0) -> dict[str, float]:
    clean = [float(v) for v in values if v is not None]
    if not clean:
        return {'p50': default, 'p75': default, 'p95': default, 'default': default}
    return {
        'p50': round(float(median(clean)), 4),
        'p75': round(float(_pct(clean, 0.75)), 4),
        'p95': round(float(_pct(clean, 0.95)), 4),
        'default': round(float(_pct(clean, 0.75)), 4),
    }


def _chain(row: dict[str, Any]) -> dict[str, Any]:
    return row.get('chain_ms') or {}


def _group_key(row: dict[str, Any]) -> str:
    return f"{str(row.get('ticker') or 'UNKNOWN').upper()}:{str(row.get('side') or 'UNKNOWN').upper()}"


def _created_dt_ct(row: dict[str, Any], trade: dict[str, Any] | None = None):
    raw_ct = row.get('created_at_ct') or (trade or {}).get('opened_at_ct')
    if raw_ct:
        try:
            return datetime.fromisoformat(str(raw_ct).replace('Z', '+00:00')).astimezone(CT)
        except Exception:
            pass
    ts = _num(row.get('created_at') or row.get('ts') or (trade or {}).get('opened_at'))
    if ts is not None:
        try:
            return datetime.fromtimestamp(float(ts), CT)
        except Exception:
            return None
    chain = _chain(row)
    signal_ms = _num(chain.get('signal_seen_ms'))
    if signal_ms is not None:
        try:
            return datetime.fromtimestamp(float(signal_ms) / 1000.0, CT)
        except Exception:
            return None
    return None


def _session_bucket_from_dt(dt) -> str:
    if dt is None:
        return 'unknown'
    minutes = dt.hour * 60 + dt.minute
    open_min = 8 * 60 + 30
    if minutes < open_min:
        return 'premarket'
    elapsed = minutes - open_min
    if elapsed < 30:
        return 'open_30m'
    if elapsed < 90:
        return 'morning'
    if elapsed < 270:
        return 'midday'
    return 'late'


def _session_bucket(row: dict[str, Any], trade: dict[str, Any] | None = None) -> str:
    phase = str(row.get('session_phase') or ((trade or {}).get('forensics') or {}).get('session_phase') or '').lower()
    if phase:
        if 'open' in phase:
            return 'open_30m'
        if 'late' in phase or 'power' in phase:
            return 'late'
        if 'mid' in phase:
            return 'midday'
        if 'normal' in phase:
            return 'midday'
    return _session_bucket_from_dt(_created_dt_ct(row, trade))


def _spread_pct(row: dict[str, Any], trade: dict[str, Any] | None = None) -> float | None:
    sources = [
        row.get('entry_quality'),
        row.get('execution_quality'),
        row.get('market_microstructure'),
        ((trade or {}).get('forensics') or {}).get('entry_quality'),
        ((trade or {}).get('forensics') or {}).get('execution_quality'),
        ((trade or {}).get('forensics') or {}).get('market_microstructure'),
    ]
    for src in sources:
        if isinstance(src, dict):
            val = _num(src.get('spread_pct'))
            if val is not None:
                return val
    return None


def _spread_bucket_from_pct(value: float | None) -> str:
    if value is None:
        return 'unknown'
    if value <= 0.08:
        return 'tight'
    if value <= 0.15:
        return 'normal'
    if value <= 0.30:
        return 'wide'
    return 'very_wide'


def _context_buckets(context: dict[str, Any] | None) -> dict[str, str]:
    context = context or {}
    return {
        'session': _session_bucket(context),
        'spread': _spread_bucket_from_pct(_spread_pct(context)),
    }


def build_model(days: list[str] | None = None, out_path: str = DEFAULT_MODEL_PATH) -> dict[str, Any]:
    latency_dir = os.path.join(POSTMORTEM_DIR, 'latency_attribution')
    trade_dir = os.path.join(POSTMORTEM_DIR, 'trades')
    if days is None:
        days = []
        if os.path.isdir(latency_dir):
            for name in os.listdir(latency_dir):
                if name.startswith('latency_attribution_') and name.endswith('.jsonl'):
                    days.append(name[len('latency_attribution_'):-len('.jsonl')])
        days = sorted(set(days))

    latency_by_id: dict[str, dict[str, Any]] = {}
    anonymous_rows: list[dict[str, Any]] = []
    trade_by_id: dict[str, dict[str, Any]] = {}
    for day in days:
        for trade in _jsonl_rows(os.path.join(trade_dir, f'trades_{day}.jsonl')):
            if trade.get('trade_id'):
                trade_by_id[str(trade['trade_id'])] = trade
        for row in _jsonl_rows(os.path.join(latency_dir, f'latency_attribution_{day}.jsonl')):
            trade_id = str(row.get('trade_id') or '')
            if trade_id:
                merged = latency_by_id.setdefault(trade_id, {})
                merged.update({k: v for k, v in row.items() if k != 'chain_ms'})
                chain = dict(merged.get('chain_ms') or {})
                chain.update(row.get('chain_ms') or {})
                merged['chain_ms'] = chain
                merged['day'] = day
            else:
                anonymous_rows.append({**row, 'day': day})

    samples: list[dict[str, Any]] = []
    for row in list(latency_by_id.values()) + anonymous_rows:
            chain = _chain(row)
            signal_ms = _num(chain.get('signal_seen_ms'))
            entry_ms = _num(chain.get('entry_filled_ms') or chain.get('committed_ms'))
            exit_trigger_ms = _num(chain.get('exit_trigger_ms'))
            exit_fill_ms = _num(chain.get('exit_broker_filled_ms') or chain.get('exit_local_closed_ms') or chain.get('exit_verified_flat_ms'))
            trade = trade_by_id.get(str(row.get('trade_id')))
            entry_bps = None
            exit_bps = None
            if trade:
                side = str(row.get('side') or trade.get('side') or '').upper()
                signal_price = _num(trade.get('entry'))
                fill_price = _num(trade.get('entry_fill_price')) or signal_price
                exit_price = _num(trade.get('exit'))
                broker_fill = ((trade.get('broker_close') or {}).get('fill') or {})
                broker_exit = _num(broker_fill.get('filled_avg_price')) or exit_price
                if signal_price and fill_price:
                    raw = (fill_price - signal_price) / signal_price * 10000.0
                    entry_bps = raw if side == 'LONG' else -raw
                if exit_price and broker_exit:
                    raw = (broker_exit - exit_price) / exit_price * 10000.0
                    exit_bps = -raw if side == 'LONG' else raw
            sample = {
                'ticker': str(row.get('ticker') or 'UNKNOWN').upper(),
                'side': str(row.get('side') or 'UNKNOWN').upper(),
                'day': row.get('day'),
                'session_bucket': _session_bucket(row, trade),
                'spread_bucket': _spread_bucket_from_pct(_spread_pct(row, trade)),
                'entry_delay_ms': entry_ms - signal_ms if signal_ms is not None and entry_ms is not None else None,
                'exit_delay_ms': exit_fill_ms - exit_trigger_ms if exit_trigger_ms is not None and exit_fill_ms is not None else None,
                'entry_slippage_bps': entry_bps,
                'exit_slippage_bps': exit_bps,
            }
            if sample['entry_delay_ms'] is not None or sample['exit_delay_ms'] is not None:
                samples.append(sample)

    groups: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    groups_v2: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    all_vals: dict[str, list[float]] = defaultdict(list)
    for sample in samples:
        key = f"{sample['ticker']}:{sample['side']}"
        v2_keys = [
            key,
            f"{key}:session={sample.get('session_bucket') or 'unknown'}",
            f"{key}:spread={sample.get('spread_bucket') or 'unknown'}",
            f"{key}:session={sample.get('session_bucket') or 'unknown'}:spread={sample.get('spread_bucket') or 'unknown'}",
        ]
        for field in ('entry_delay_ms', 'exit_delay_ms', 'entry_slippage_bps', 'exit_slippage_bps'):
            val = sample.get(field)
            if val is None:
                continue
            groups[key][field].append(float(val))
            for v2_key in v2_keys:
                groups_v2[v2_key][field].append(float(val))
            all_vals[field].append(float(val))

    model = dict(DEFAULT_MODEL)
    model.update({
        'source': 'live_latency_attribution',
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'days': days,
        'sample_count': len(samples),
        'unique_trade_sample_count': len(latency_by_id),
        'v2': dict(DEFAULT_MODEL['v2']),
        'entry_delay_ms': _stats(all_vals['entry_delay_ms'], DEFAULT_MODEL['entry_delay_ms']['default']),
        'exit_delay_ms': _stats(all_vals['exit_delay_ms'], DEFAULT_MODEL['exit_delay_ms']['default']),
        'entry_slippage_bps': _stats(all_vals['entry_slippage_bps'], 0.0),
        'exit_slippage_bps': _stats(all_vals['exit_slippage_bps'], 0.0),
        'groups': {},
        'groups_v2': {},
    })
    for key, vals in sorted(groups.items()):
        model['groups'][key] = {
            'sample_count': max(len(v) for v in vals.values()) if vals else 0,
            'entry_delay_ms': _stats(vals['entry_delay_ms'], model['entry_delay_ms']['default']),
            'exit_delay_ms': _stats(vals['exit_delay_ms'], model['exit_delay_ms']['default']),
            'entry_slippage_bps': _stats(vals['entry_slippage_bps'], 0.0),
            'exit_slippage_bps': _stats(vals['exit_slippage_bps'], 0.0),
        }
    for key, vals in sorted(groups_v2.items()):
        model['groups_v2'][key] = {
            'sample_count': max(len(v) for v in vals.values()) if vals else 0,
            'entry_delay_ms': _stats(vals['entry_delay_ms'], model['entry_delay_ms']['default']),
            'exit_delay_ms': _stats(vals['exit_delay_ms'], model['exit_delay_ms']['default']),
            'entry_slippage_bps': _stats(vals['entry_slippage_bps'], 0.0),
            'exit_slippage_bps': _stats(vals['exit_slippage_bps'], 0.0),
        }
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(model, f, indent=2, sort_keys=True)
    return model


def load_model(path: str | None = None) -> dict[str, Any]:
    path = path or DEFAULT_MODEL_PATH
    try:
        with open(path, 'r', encoding='utf-8') as f:
            model = json.load(f)
        return model if isinstance(model, dict) else dict(DEFAULT_MODEL)
    except Exception:
        return dict(DEFAULT_MODEL)


def _bucket(model: dict[str, Any], ticker: str, side: str, field: str) -> dict[str, Any]:
    group = ((model.get('groups') or {}).get(f'{ticker.upper()}:{side.upper()}') or {})
    return group.get(field) or model.get(field) or DEFAULT_MODEL[field]


def _bucket_v2(model: dict[str, Any], ticker: str, side: str, field: str,
               context: dict[str, Any] | None = None) -> dict[str, Any] | None:
    if not (model.get('v2') or {}).get('enabled', True):
        return None
    groups_v2 = model.get('groups_v2') or {}
    if not groups_v2:
        return None
    min_samples = int((model.get('v2') or {}).get('min_group_samples') or 5)
    ticker = str(ticker or 'UNKNOWN').upper()
    side = str(side or 'UNKNOWN').upper()
    base = f'{ticker}:{side}'
    buckets = _context_buckets(context)
    keys = [
        f"{base}:session={buckets['session']}:spread={buckets['spread']}",
        f"{base}:session={buckets['session']}",
        f"{base}:spread={buckets['spread']}",
        base,
    ]
    for key in keys:
        group = groups_v2.get(key)
        if not isinstance(group, dict):
            continue
        if int(group.get('sample_count') or 0) < min_samples and key != base:
            continue
        bucket = group.get(field)
        if bucket:
            return bucket
    return None


def latency_ms(model: dict[str, Any], ticker: str, side: str, field: str, percentile: str = 'p75',
               context: dict[str, Any] | None = None) -> int:
    bucket = _bucket_v2(model, ticker, side, field, context=context) or _bucket(model, ticker, side, field)
    value = bucket.get(percentile)
    if value is None:
        value = bucket.get('default', 0)
    return max(0, int(round(float(value or 0))))


def slippage_bps(model: dict[str, Any], ticker: str, side: str, field: str, percentile: str = 'p50',
                 context: dict[str, Any] | None = None) -> float:
    bucket = _bucket_v2(model, ticker, side, field, context=context) or _bucket(model, ticker, side, field)
    value = bucket.get(percentile)
    if value is None:
        value = bucket.get('default', 0.0)
    return float(value or 0.0)


def apply_slippage(price: float, side: str, bps: float, stage: str) -> float:
    if not price or not bps:
        return price
    adverse = float(bps) / 10000.0
    side = side.upper()
    if stage == 'entry':
        return price * (1.0 + adverse if side == 'LONG' else 1.0 - adverse)
    return price * (1.0 - adverse if side == 'LONG' else 1.0 + adverse)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Build or inspect Step 2 latency model from Live logs.')
    ap.add_argument('command', choices=['build', 'show'])
    ap.add_argument('--days', nargs='*', default=None)
    ap.add_argument('--out', default=DEFAULT_MODEL_PATH)
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == 'build':
        payload = build_model(days=args.days, out_path=args.out)
    else:
        payload = load_model(args.out)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
