from __future__ import annotations

import argparse
import json
import os
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Iterable, Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo


HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, 'postmortem')
TICK_DIR = os.path.join(HERE, 'tick_logs')
CT = ZoneInfo('America/Chicago')
BTC_SYMBOL = 'BTC/USD'
SCHEMA_VERSION = 1


def _num(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _pct(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b in (None, 0):
        return None
    return round((a - b) / b * 100, 4)


def _avg(vals: Iterable[Optional[float]], digits: int = 4) -> Optional[float]:
    clean = [float(v) for v in vals if v is not None]
    if not clean:
        return None
    return round(sum(clean) / len(clean), digits)


def _keyable(value: Any) -> Any:
    if isinstance(value, list):
        return tuple(str(v) for v in value)
    if isinstance(value, dict):
        return tuple(sorted((str(k), _keyable(v)) for k, v in value.items()))
    return value


def _row_key(row: dict) -> tuple:
    return (
        row.get('phase'),
        row.get('symbol'),
        row.get('event'),
        row.get('ts_ms'),
        row.get('price'),
        row.get('size'),
        row.get('side'),
        row.get('bid'),
        row.get('ask'),
        row.get('bid_size'),
        row.get('ask_size'),
        row.get('trade_exchange'),
        _keyable(row.get('trade_conditions')),
        row.get('trade_tape'),
        row.get('bid_exchange'),
        row.get('ask_exchange'),
        _keyable(row.get('quote_conditions')),
        row.get('quote_tape'),
    )


def _iter_jsonl(path: str) -> Iterable[dict]:
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                yield json.loads(line)
            except Exception:
                continue


def _infer_side(header: dict) -> Optional[str]:
    signal = header.get('signal') or {}
    side = str(signal.get('side') or '').upper()
    if side in ('LONG', 'SHORT'):
        return side
    label = str(header.get('label') or '').upper()
    if label.startswith('LONG'):
        return 'LONG'
    if label.startswith('SHORT'):
        return 'SHORT'
    return None


def _capture_type(header: dict) -> str:
    signal = header.get('signal') or {}
    ctype = str(signal.get('capture_type') or '').lower()
    if ctype:
        return ctype
    label = str(header.get('label') or '').upper()
    if label.startswith('EXIT_'):
        return 'exit'
    if label.startswith('SKIP_'):
        return 'skipped'
    if label.startswith('NEAR_'):
        return 'near_signal'
    return 'entry'


def _safe_filename(path: str) -> str:
    try:
        return os.path.relpath(path, HERE)
    except Exception:
        return path


def tick_capture_files(day: str) -> list[str]:
    root = os.path.join(TICK_DIR, day)
    if not os.path.isdir(root):
        return []
    return [
        os.path.join(root, name)
        for name in sorted(os.listdir(root))
        if name.endswith('.jsonl')
    ]


def _partition_rows(path: str) -> tuple[dict, list[dict], int]:
    header: dict = {}
    rows: list[dict] = []
    seen = set()
    dupes = 0
    for row in _iter_jsonl(path):
        if row.get('type') == 'header':
            header = row
            continue
        key = _row_key(row)
        if key in seen:
            dupes += 1
            continue
        seen.add(key)
        rows.append(row)
    return header, rows, dupes


def _price_rows(rows: list[dict], symbol: str, phase: str) -> list[dict]:
    out = []
    for row in rows:
        if row.get('symbol') != symbol or row.get('phase') != phase:
            continue
        price = _num(row.get('price'))
        if row.get('event') == 'quote' and price is None:
            bid = _num(row.get('bid'))
            ask = _num(row.get('ask'))
            if bid is not None and ask is not None and bid > 0 and ask > 0:
                price = round((bid + ask) / 2, 6)
        if price is None or price <= 0:
            continue
        copied = dict(row)
        copied['_price'] = price
        out.append(copied)
    return sorted(out, key=lambda r: int(r.get('ts_ms') or 0))


def _trade_rows(rows: list[dict], symbol: str, phase: str) -> list[dict]:
    out = []
    for row in rows:
        if row.get('symbol') == symbol and row.get('phase') == phase and row.get('event') == 'trade':
            price = _num(row.get('price'))
            if price is not None and price > 0:
                copied = dict(row)
                copied['_price'] = price
                out.append(copied)
    return sorted(out, key=lambda r: int(r.get('ts_ms') or 0))


def _quote_rows(rows: list[dict], symbol: str, phase: str) -> list[dict]:
    return sorted(
        [r for r in rows if r.get('symbol') == symbol and r.get('phase') == phase and r.get('event') == 'quote'],
        key=lambda r: int(r.get('ts_ms') or 0),
    )


def _market_metadata(rows: list[dict], symbol: Optional[str]) -> dict:
    scoped = [r for r in rows if symbol is None or r.get('symbol') == symbol]
    trade_rows = [r for r in scoped if r.get('event') == 'trade']
    quote_rows = [r for r in scoped if r.get('event') == 'quote']
    trade_conditions = Counter()
    quote_conditions = Counter()
    for row in trade_rows:
        codes = row.get('trade_conditions') or row.get('conditions') or []
        if isinstance(codes, str):
            codes = [codes]
        for code in codes:
            trade_conditions[str(code)] += 1
    for row in quote_rows:
        codes = row.get('quote_conditions') or []
        if isinstance(codes, str):
            codes = [codes]
        for code in codes:
            quote_conditions[str(code)] += 1
    return {
        'trade_rows': len(trade_rows),
        'quote_rows': len(quote_rows),
        'trade_exchanges': dict(Counter(str(r.get('trade_exchange') or 'unknown') for r in trade_rows).most_common(12)),
        'trade_conditions': dict(trade_conditions.most_common(20)),
        'trade_tapes': dict(Counter(str(r.get('trade_tape') or 'unknown') for r in trade_rows).most_common(8)),
        'quote_bid_exchanges': dict(Counter(str(r.get('bid_exchange') or 'unknown') for r in quote_rows).most_common(12)),
        'quote_ask_exchanges': dict(Counter(str(r.get('ask_exchange') or 'unknown') for r in quote_rows).most_common(12)),
        'quote_conditions': dict(quote_conditions.most_common(20)),
        'quote_tapes': dict(Counter(str(r.get('quote_tape') or 'unknown') for r in quote_rows).most_common(8)),
        'condition_metadata_present': any(
            r.get('trade_conditions') or r.get('quote_conditions')
            or r.get('trade_exchange') or r.get('bid_exchange') or r.get('ask_exchange')
            for r in scoped
        ),
    }


def _choose_reference(header: dict, symbol: str, pre: list[dict], post: list[dict]) -> Optional[float]:
    signal = header.get('signal') or {}
    if symbol == (header.get('ticker') or signal.get('ticker')):
        price = _num(signal.get('entry_price') or signal.get('price'))
        if price:
            return price
    if symbol == BTC_SYMBOL:
        btc_price = _num((signal.get('btc_indicators') or {}).get('price'))
        if btc_price:
            return btc_price
    if post:
        return _num(post[0].get('_price'))
    if pre:
        return _num(pre[-1].get('_price'))
    return None


def _signed_return_pct(price: float, ref: float, side: Optional[str]) -> float:
    raw = (price - ref) / ref * 100
    if side == 'SHORT':
        raw *= -1
    return round(raw, 4)


def _series_at_or_before(series: list[tuple[float, float]], sec: int) -> Optional[float]:
    best = None
    for t, val in series:
        if t <= sec:
            best = val
        else:
            break
    return best


def _path_metrics(post: list[dict], ref: Optional[float], side: Optional[str],
                  start_ms: Optional[int]) -> dict:
    if not post or not ref or not side:
        return {
            'available': False,
            'reason': 'missing_post_ticks_or_reference',
        }
    base_ms = start_ms or _int(post[0].get('ts_ms')) or 0
    signed_series: list[tuple[float, float]] = []
    price_series: list[tuple[float, float]] = []
    for row in post:
        ts = _int(row.get('ts_ms'))
        price = _num(row.get('_price'))
        if ts is None or price is None:
            continue
        sec = max(0.0, round((ts - base_ms) / 1000, 3))
        signed = _signed_return_pct(price, ref, side)
        signed_series.append((sec, signed))
        price_series.append((sec, price))
    if not signed_series:
        return {
            'available': False,
            'reason': 'missing_valid_post_ticks',
        }
    mfe_sec, mfe = max(signed_series, key=lambda x: x[1])
    mae_sec, mae = min(signed_series, key=lambda x: x[1])
    thresholds = {}
    for threshold in (0.05, 0.10, 0.20, -0.05, -0.10, -0.20):
        hit = next((sec for sec, val in signed_series if val >= threshold), None) if threshold > 0 else \
            next((sec for sec, val in signed_series if val <= threshold), None)
        thresholds[f"time_to_{threshold:+.2f}pct_sec"] = round(hit, 3) if hit is not None else None
    checks = {}
    for sec in (5, 10, 15, 30, 60, 120, 180, 300, 360):
        checks[f'{sec}s'] = _series_at_or_before(signed_series, sec)
    first_10 = [v for sec, v in signed_series if sec <= 10]
    first_30 = [v for sec, v in signed_series if sec <= 30]
    return {
        'available': True,
        'reference_price': round(ref, 6),
        'first_price': round(price_series[0][1], 6),
        'last_price': round(price_series[-1][1], 6),
        'duration_sec': round(signed_series[-1][0], 3),
        'samples': len(signed_series),
        'final_signed_return_pct': signed_series[-1][1],
        'mfe_pct': round(mfe, 4),
        'mae_pct': round(mae, 4),
        'time_to_mfe_sec': round(mfe_sec, 3),
        'time_to_mae_sec': round(mae_sec, 3),
        'checks': checks,
        'first_10s_best_pct': round(max(first_10), 4) if first_10 else None,
        'first_10s_worst_pct': round(min(first_10), 4) if first_10 else None,
        'first_30s_best_pct': round(max(first_30), 4) if first_30 else None,
        'first_30s_worst_pct': round(min(first_30), 4) if first_30 else None,
        'thresholds': thresholds,
    }


def _btc_metrics(pre: list[dict], post: list[dict], ref: Optional[float],
                 side: Optional[str], start_ms: Optional[int]) -> dict:
    if not post or not ref:
        return {
            'available': False,
            'reason': 'missing_btc_ticks_or_reference',
            'pre_samples': len(pre),
            'post_samples': len(post),
        }
    base_ms = start_ms or _int(post[0].get('ts_ms')) or 0
    series = []
    for row in post:
        ts = _int(row.get('ts_ms'))
        price = _num(row.get('_price'))
        if ts is None or price is None:
            continue
        sec = max(0.0, round((ts - base_ms) / 1000, 3))
        series.append((sec, _pct(price, ref)))
    checks = {}
    aligned = {}
    for sec in (15, 30, 60, 120, 180, 300, 360):
        val = _series_at_or_before(series, sec)
        checks[f'{sec}s'] = val
        if val is not None and side in ('LONG', 'SHORT'):
            aligned[f'{sec}s'] = round(val if side == 'LONG' else -val, 4)
    best_aligned = max(aligned.values()) if aligned else None
    worst_aligned = min(aligned.values()) if aligned else None
    path_label = 'btc_unavailable'
    if aligned:
        vals = [v for v in aligned.values() if v is not None]
        if vals:
            if max(vals) - min(vals) < 0.04:
                path_label = 'btc_chop'
            elif (aligned.get('60s') or 0) >= 0.05 and (aligned.get('120s') or aligned.get('60s') or 0) >= 0:
                path_label = 'btc_confirmed_side'
            elif min(vals) <= -0.08 and max(vals) >= 0.03:
                path_label = 'btc_reversed_against_side'
            elif (aligned.get('60s') or 0) < 0:
                path_label = 'btc_faded_against_side'
            else:
                path_label = 'btc_neutral'
    return {
        'available': bool(series),
        'reference_price': round(ref, 6),
        'pre_samples': len(pre),
        'post_samples': len(post),
        'checks': checks,
        'aligned_with_entry_side_pct': aligned,
        'best_aligned_pct': round(best_aligned, 4) if best_aligned is not None else None,
        'worst_aligned_pct': round(worst_aligned, 4) if worst_aligned is not None else None,
        'supports_side_60s': aligned.get('60s') is not None and aligned.get('60s') >= 0,
        'supports_side_120s': aligned.get('120s') is not None and aligned.get('120s') >= 0,
        'path_label': path_label,
    }


def _flow_window(trades: list[dict], start_ms: Optional[int], end_sec: int) -> dict:
    base_ms = start_ms or (_int(trades[0].get('ts_ms')) if trades else None)
    if base_ms is None:
        return {'samples': 0}
    buy_v = sell_v = neutral_v = 0
    samples = 0
    for row in trades:
        ts = _int(row.get('ts_ms'))
        if ts is None or ts < base_ms or ts > base_ms + end_sec * 1000:
            continue
        samples += 1
        size = int(_num(row.get('size')) or 0)
        side = int(_num(row.get('side')) or 0)
        if side > 0:
            buy_v += size
        elif side < 0:
            sell_v += size
        else:
            neutral_v += size
    total = buy_v + sell_v
    return {
        'samples': samples,
        'buy_v': buy_v,
        'sell_v': sell_v,
        'neutral_v': neutral_v,
        'buy_pct': round(100 * buy_v / total, 2) if total else None,
        'sell_pct': round(100 * sell_v / total, 2) if total else None,
    }


def _flow_metrics(post_trades: list[dict], side: Optional[str], start_ms: Optional[int]) -> dict:
    windows = {f'{sec}s': _flow_window(post_trades, start_ms, sec) for sec in (10, 30, 60, 120, 360)}
    support = {}
    for label, row in windows.items():
        if side == 'LONG':
            support[label] = row.get('buy_pct') is not None and row.get('buy_pct') >= 55
        elif side == 'SHORT':
            support[label] = row.get('sell_pct') is not None and row.get('sell_pct') >= 55
    return {
        'windows': windows,
        'side_supported': support,
        'support_30s': support.get('30s'),
        'support_60s': support.get('60s'),
    }


def _raw_return(pre: list[dict], post: list[dict], ref: Optional[float],
                start_ms: Optional[int], horizon_sec: int = 60) -> Optional[float]:
    if not ref:
        return None
    base_ms = start_ms or (_int(post[0].get('ts_ms')) if post else None)
    if base_ms is None:
        return None
    candidates = []
    for row in post:
        ts = _int(row.get('ts_ms'))
        price = _num(row.get('_price'))
        if ts is None or price is None:
            continue
        if ts <= base_ms + horizon_sec * 1000:
            candidates.append((ts, price))
    if not candidates:
        return None
    return _pct(candidates[-1][1], ref)


def _pre_return(rows: list[dict], start_ms: Optional[int], lookback_sec: int = 60) -> Optional[float]:
    if not rows or start_ms is None:
        return None
    window = [
        r for r in rows
        if (_int(r.get('ts_ms')) is not None and start_ms - lookback_sec * 1000 <= _int(r.get('ts_ms')) <= start_ms)
    ]
    if len(window) < 2:
        return None
    first = _num(window[0].get('_price'))
    last = _num(window[-1].get('_price'))
    return _pct(last, first)


def _peer_metrics(rows: list[dict], ticker: str, side: Optional[str],
                  stock_pre: list[dict], stock_post: list[dict],
                  stock_ref: Optional[float], start_ms: Optional[int]) -> dict:
    peers = sorted({
        r.get('symbol') for r in rows
        if r.get('symbol') not in (ticker, BTC_SYMBOL, None)
    })
    stock_pre60 = _pre_return(_price_rows(rows, ticker, 'pre'), start_ms) if ticker else None
    stock_post60 = _raw_return(stock_pre, stock_post, stock_ref, start_ms, 60)
    peer_rows = []
    confirming = opposing = unavailable = 0
    peer_pre_returns = []
    for peer in peers:
        pre = _price_rows(rows, peer, 'pre')
        post = _price_rows(rows, peer, 'post')
        ref = _num(pre[-1].get('_price')) if pre else (_num(post[0].get('_price')) if post else None)
        pre60 = _pre_return(pre, start_ms)
        post60 = _raw_return(pre, post, ref, start_ms, 60)
        aligned = None
        if post60 is not None and side in ('LONG', 'SHORT'):
            aligned = post60 if side == 'LONG' else -post60
            if aligned >= 0.03:
                confirming += 1
            elif aligned <= -0.03:
                opposing += 1
            else:
                unavailable += 1
        else:
            unavailable += 1
        if pre60 is not None:
            peer_pre_returns.append(pre60)
        peer_rows.append({
            'symbol': peer,
            'pre60_return_pct': pre60,
            'post60_return_pct': post60,
            'post60_aligned_pct': round(aligned, 4) if aligned is not None else None,
            'pre_samples': len(pre),
            'post_samples': len(post),
        })
    avg_peer_pre = _avg(peer_pre_returns)
    pre_state = 'unknown'
    if stock_pre60 is not None and avg_peer_pre is not None:
        diff = round(stock_pre60 - avg_peer_pre, 4)
        if diff >= 0.08:
            pre_state = 'ticker_leading_peers'
        elif diff <= -0.08:
            pre_state = 'ticker_lagging_peers'
        else:
            pre_state = 'ticker_in_line_with_peers'
    else:
        diff = None
    post_state = 'unknown'
    if confirming or opposing:
        if confirming and not opposing:
            post_state = 'peers_confirmed_side'
        elif opposing and not confirming:
            post_state = 'peers_opposed_side'
        else:
            post_state = 'mixed_peer_confirmation'
    return {
        'peers': peer_rows,
        'confirming_peers_60s': confirming,
        'opposing_peers_60s': opposing,
        'unavailable_peers_60s': unavailable,
        'post_state': post_state,
        'pre_state': pre_state,
        'stock_pre60_return_pct': stock_pre60,
        'avg_peer_pre60_return_pct': avg_peer_pre,
        'stock_minus_peer_pre60_pct': diff,
        'stock_post60_return_pct': stock_post60,
    }


def _exit_replay(capture_type: str, stock_path: dict) -> Optional[dict]:
    if capture_type != 'exit' or not stock_path.get('available'):
        return None
    checks = stock_path.get('checks') or {}
    rows = {}
    for horizon in ('30s', '60s', '120s', '180s'):
        val = checks.get(horizon)
        if val is None:
            continue
        rows[horizon] = {
            'signed_return_if_held_pct': val,
            'verdict': (
                'exit_left_money' if val >= 0.05 else
                'exit_saved_loss' if val <= -0.05 else
                'exit_neutral'
            ),
        }
    vals = [r['signed_return_if_held_pct'] for r in rows.values()]
    if not vals:
        verdict = 'insufficient_after_exit_ticks'
    elif max(vals) >= 0.10 and min(vals) > -0.05:
        verdict = 'likely_exited_too_early'
    elif min(vals) <= -0.10 and max(vals) < 0.05:
        verdict = 'likely_good_exit'
    elif max(vals) >= 0.10 and min(vals) <= -0.10:
        verdict = 'noisy_after_exit'
    else:
        verdict = 'neutral_after_exit'
    return {
        'verdict': verdict,
        'horizons': rows,
        'mfe_after_exit_pct': stock_path.get('mfe_pct'),
        'mae_after_exit_pct': stock_path.get('mae_pct'),
        'deduction': (
            'Positive signed return after exit means holding longer would have helped; '
            'negative signed return means the exit prevented further damage.'
        ),
    }


def _quote_metrics(quotes: list[dict], start_ms: Optional[int]) -> dict:
    spreads = []
    imbalances = []
    entry_10s_imbalances = []
    last_5s_spreads = []
    spread_jumps = 0
    last_5s_jumps = 0
    prev_spread = None
    prev_last5_spread = None
    for row in quotes:
        bid = _num(row.get('bid'))
        ask = _num(row.get('ask'))
        ts = _int(row.get('ts_ms'))
        if bid is not None and ask is not None and bid > 0 and ask > 0 and ask >= bid:
            mid = (bid + ask) / 2
            spread = (ask - bid) / mid * 100
            spreads.append(spread)
            if prev_spread is not None and abs(spread - prev_spread) >= 0.03:
                spread_jumps += 1
            prev_spread = spread
            if start_ms is not None and ts is not None and start_ms - 5000 <= ts <= start_ms:
                last_5s_spreads.append(spread)
                if prev_last5_spread is not None and abs(spread - prev_last5_spread) >= 0.03:
                    last_5s_jumps += 1
                prev_last5_spread = spread
        imb = _num(row.get('imbalance'))
        if imb is not None:
            imbalances.append(imb)
            if start_ms is not None and ts is not None and start_ms <= ts <= start_ms + 10_000:
                entry_10s_imbalances.append(imb)
    last5_avg = _avg(last_5s_spreads)
    last5_max = round(max(last_5s_spreads), 4) if last_5s_spreads else None
    avg_spread = _avg(spreads)
    max_spread = round(max(spreads), 4) if spreads else None
    unstable = False
    if last5_max is not None and avg_spread is not None and last5_max >= max(0.12, avg_spread * 1.8):
        unstable = True
    if last_5s_jumps >= 3:
        unstable = True
    return {
        'samples': len(quotes),
        'avg_spread_pct': avg_spread,
        'max_spread_pct': max_spread,
        'avg_imbalance': _avg(imbalances),
        'entry_10s_avg_imbalance': _avg(entry_10s_imbalances),
        'last_5s_samples': len(last_5s_spreads),
        'last_5s_avg_spread_pct': last5_avg,
        'last_5s_max_spread_pct': last5_max,
        'spread_flicker_count': spread_jumps,
        'last_5s_spread_flicker_count': last_5s_jumps,
        'unstable_quote': unstable,
    }


def _classify_tick_path(stock: dict, btc: dict, flow: dict,
                        quote_pre: dict, quote_post: dict,
                        peers: Optional[dict] = None) -> list[str]:
    tags = []
    if not stock.get('available'):
        tags.append('missing_stock_tick_path')
        return tags
    if stock.get('first_10s_best_pct') is not None and stock.get('first_10s_best_pct') > 0.05:
        tags.append('instant_followthrough')
    if stock.get('first_10s_worst_pct') is not None and stock.get('first_10s_worst_pct') < -0.05:
        tags.append('instant_rejection')
    if stock.get('mae_pct') is not None and stock.get('mae_pct') <= -0.20:
        tags.append('deep_adverse_move')
    if stock.get('mfe_pct') is not None and stock.get('mfe_pct') >= 0.20:
        tags.append('clean_profit_window')
    if btc.get('available') and btc.get('supports_side_60s') is False:
        tags.append('btc_diverged_after_entry')
    if btc.get('path_label') in ('btc_reversed_against_side', 'btc_faded_against_side'):
        tags.append(btc.get('path_label'))
    if flow.get('support_30s') is False:
        tags.append('post_entry_flow_against_side')
    if quote_pre.get('unstable_quote'):
        tags.append('unstable_quote_before_entry')
    if quote_post.get('avg_spread_pct') is not None and quote_post.get('avg_spread_pct') >= 0.12:
        tags.append('wide_spread_tick_context')
    if (peers or {}).get('post_state') == 'peers_opposed_side':
        tags.append('miner_peers_opposed_after_entry')
    elif (peers or {}).get('post_state') == 'mixed_peer_confirmation':
        tags.append('mixed_miner_peer_confirmation')
    return tags or ['normal_tick_path']


def analyze_tick_file(path: str) -> dict:
    try:
        header, rows, duplicate_rows_removed = _partition_rows(path)
    except Exception as e:
        return {
            'file': _safe_filename(path),
            'error': str(e),
        }
    signal = header.get('signal') or {}
    ticker = header.get('ticker') or signal.get('ticker')
    side = _infer_side(header)
    ctype = _capture_type(header)
    started_ms = _int(header.get('started_ms')) or _int(signal.get('ts'))
    if started_ms and started_ms < 10_000_000_000:
        started_ms *= 1000

    stock_pre = _trade_rows(rows, ticker, 'pre') if ticker else []
    stock_post = _trade_rows(rows, ticker, 'post') if ticker else []
    stock_pre_quotes = _quote_rows(rows, ticker, 'pre') if ticker else []
    stock_post_quotes = _quote_rows(rows, ticker, 'post') if ticker else []
    btc_pre = _price_rows(rows, BTC_SYMBOL, 'pre')
    btc_post = _price_rows(rows, BTC_SYMBOL, 'post')
    stock_ref = _choose_reference(header, ticker, stock_pre, stock_post) if ticker else None
    btc_ref = _choose_reference(header, BTC_SYMBOL, btc_pre, btc_post)
    stock_path = _path_metrics(stock_post, stock_ref, side, started_ms)
    btc_path = _btc_metrics(btc_pre, btc_post, btc_ref, side, started_ms)
    flow = _flow_metrics(stock_post, side, started_ms)
    quote = {
        'pre': _quote_metrics(stock_pre_quotes, started_ms),
        'post': _quote_metrics(stock_post_quotes, started_ms),
    }
    market_metadata = {
        'stock': _market_metadata(rows, ticker),
        'btc': _market_metadata(rows, BTC_SYMBOL),
    }
    peers = _peer_metrics(rows, ticker, side, stock_pre, stock_post, stock_ref, started_ms) if ticker else {}
    exit_replay = _exit_replay(ctype, stock_path)
    tags = _classify_tick_path(stock_path, btc_path, flow, quote['pre'], quote['post'], peers)
    return {
        'file': _safe_filename(path),
        'capture_type': ctype,
        'capture_id': header.get('capture_id'),
        'ticker': ticker,
        'side': side,
        'label': header.get('label'),
        'started_ms': started_ms,
        'started_at_ct': datetime.fromtimestamp(started_ms / 1000, CT).isoformat(timespec='seconds') if started_ms else None,
        'setup_type': signal.get('setup_type'),
        'conviction': signal.get('conviction'),
        'score': signal.get('score'),
        'signal_price': signal.get('price'),
        'skip_reason': signal.get('skip_reason') or signal.get('near_signal_reason'),
        'exit_reason': signal.get('exit_reason'),
        'exit_context': {
            'trade_id': signal.get('trade_id'),
            'exit_decision_context': signal.get('exit_decision_context'),
            'exit_reason_hierarchy': signal.get('exit_reason_hierarchy'),
            'candidate_count': len(((signal.get('exit_reason_hierarchy') or {}).get('candidates') or []))
                               if isinstance(signal.get('exit_reason_hierarchy'), dict) else None,
        } if ctype == 'exit' else None,
        'btc_context': signal.get('btc_context') or {
            'regime': signal.get('btc_regime'),
            'stack': signal.get('btc_stack'),
            'mom_60s': signal.get('btc_mom'),
        },
        'counts': {
            'raw_rows': len(rows) + duplicate_rows_removed,
            'duplicate_rows_removed': duplicate_rows_removed,
            'stock_pre_trades': len(stock_pre),
            'stock_post_trades': len(stock_post),
            'stock_pre_quotes': len(stock_pre_quotes),
            'stock_post_quotes': len(stock_post_quotes),
            'btc_pre_prices': len(btc_pre),
            'btc_post_prices': len(btc_post),
        },
        'stock_path': stock_path,
        'btc_path': btc_path,
        'post_entry_flow': flow,
        'quotes': quote,
        'market_metadata': market_metadata,
        'miner_peer_context': peers,
        'exit_replay': exit_replay,
        'tick_path_tags': tags,
    }


def _bucket_rows(rows: list[dict], key: str) -> list[dict]:
    buckets: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        buckets[str(row.get(key) or 'unknown')].append(row)
    out = []
    for bucket, items in buckets.items():
        paths = [r.get('stock_path') or {} for r in items]
        out.append({
            key: bucket,
            'captures': len(items),
            'avg_mfe_pct': _avg([p.get('mfe_pct') for p in paths]),
            'avg_mae_pct': _avg([p.get('mae_pct') for p in paths]),
            'instant_rejections': sum('instant_rejection' in (r.get('tick_path_tags') or []) for r in items),
            'btc_divergences': sum('btc_diverged_after_entry' in (r.get('tick_path_tags') or []) for r in items),
            'flow_against_side': sum('post_entry_flow_against_side' in (r.get('tick_path_tags') or []) for r in items),
        })
    return sorted(out, key=lambda r: (-r['captures'], str(r.get(key))))


def build_tick_replay(day: str) -> dict:
    files = tick_capture_files(day)
    rows = [analyze_tick_file(path) for path in files]
    good = [r for r in rows if not r.get('error')]
    bad = [r for r in rows if r.get('error')]
    tag_counts = Counter(tag for row in good for tag in row.get('tick_path_tags') or [])
    capture_type_counts = Counter(row.get('capture_type') or 'unknown' for row in good)
    watch_rows = []
    for row in good:
        tags = set(row.get('tick_path_tags') or [])
        exit_verdict = (row.get('exit_replay') or {}).get('verdict')
        if tags & {
            'instant_rejection',
            'deep_adverse_move',
            'btc_diverged_after_entry',
            'btc_reversed_against_side',
            'btc_faded_against_side',
            'post_entry_flow_against_side',
            'wide_spread_tick_context',
            'unstable_quote_before_entry',
            'miner_peers_opposed_after_entry',
            'mixed_miner_peer_confirmation',
        } or exit_verdict in ('likely_exited_too_early', 'likely_good_exit', 'noisy_after_exit'):
            watch_rows.append({
                'capture_type': row.get('capture_type'),
                'started_at_ct': row.get('started_at_ct'),
                'ticker': row.get('ticker'),
                'side': row.get('side'),
                'setup_type': row.get('setup_type'),
                'score': row.get('score'),
                'skip_reason': row.get('skip_reason'),
                'exit_reason': row.get('exit_reason'),
                'exit_replay_verdict': exit_verdict,
                'tags': row.get('tick_path_tags'),
                'mfe_pct': (row.get('stock_path') or {}).get('mfe_pct'),
                'mae_pct': (row.get('stock_path') or {}).get('mae_pct'),
                'btc_60s_aligned_pct': ((row.get('btc_path') or {}).get('aligned_with_entry_side_pct') or {}).get('60s'),
                'btc_path_label': (row.get('btc_path') or {}).get('path_label'),
                'flow_30s_supported': (row.get('post_entry_flow') or {}).get('support_30s'),
                'miner_peer_state': (row.get('miner_peer_context') or {}).get('post_state'),
                'pre_quote_unstable': ((row.get('quotes') or {}).get('pre') or {}).get('unstable_quote'),
                'file': row.get('file'),
            })
    watch_rows = sorted(
        watch_rows,
        key=lambda r: (
            0 if 'deep_adverse_move' in (r.get('tags') or []) else 1,
            r.get('mae_pct') if r.get('mae_pct') is not None else 0,
        ),
    )
    return {
        'day': day,
        'schema_version': SCHEMA_VERSION,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'source_dir': os.path.join(TICK_DIR, day),
        'summary': {
            'files_found': len(files),
            'captures_analyzed': len(good),
            'capture_errors': len(bad),
            'duplicate_rows_removed': sum((r.get('counts') or {}).get('duplicate_rows_removed') or 0 for r in good),
            'captures_with_stock_path': sum((r.get('stock_path') or {}).get('available') is True for r in good),
            'captures_with_btc_path': sum((r.get('btc_path') or {}).get('available') is True for r in good),
            'tick_path_tags': dict(tag_counts),
            'capture_type_counts': dict(capture_type_counts),
        },
        'by_ticker': _bucket_rows(good, 'ticker'),
        'by_capture_type': _bucket_rows(good, 'capture_type'),
        'by_setup_type': _bucket_rows(good, 'setup_type'),
        'by_side': _bucket_rows(good, 'side'),
        'watch_rows': watch_rows[:40],
        'rows': good,
        'errors': bad,
        'learning_note': (
            'Passive tick-by-tick replay. Use this to judge post-entry follow-through, '
            'BTC alignment, flow support, and execution context before promoting rule changes.'
        ),
    }


def write_tick_replay(day: str) -> tuple[str, dict]:
    payload = build_tick_replay(day)
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, f'tick_replay_{day}.json')
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, default=str)
    os.replace(tmp, path)
    return path, payload


def main() -> int:
    ap = argparse.ArgumentParser(description='Build tick-by-tick replay artifact from entry captures.')
    ap.add_argument('day')
    ap.add_argument('--json', action='store_true')
    ap.add_argument('--write', action='store_true')
    args = ap.parse_args()
    if args.write:
        path, payload = write_tick_replay(args.day)
    else:
        payload = build_tick_replay(args.day)
        path = None
    if args.json:
        print(json.dumps({'path': path, 'summary': payload.get('summary')}, indent=2))
    else:
        print(payload.get('summary'))
        if path:
            print(f'written: {path}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
