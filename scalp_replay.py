from __future__ import annotations

from output_paths import output_path

import argparse
import json
import os
from collections import Counter, defaultdict

from ws_scalp import BTC_SYMBOL, SymbolState, compute_indicators, detect_signal, execution_quality, shadow_variants


HERE = os.path.dirname(os.path.abspath(__file__))
TICK_LOG_DIR = output_path('tick_logs')


def _infer_symbol(path: str) -> str:
    return os.path.basename(path).split('_', 1)[0].upper()


def _load_capture(path: str):
    symbol = _infer_symbol(path)
    ticks = []
    quotes = []
    btc_ticks = []
    btc_quotes = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                row = json.loads(line)
            except Exception:
                continue
            if row.get('type') == 'header':
                symbol = (row.get('ticker') or symbol).upper()
                continue
            if not row.get('ts_ms'):
                continue
            event = row.get('event') or ('trade' if row.get('price') is not None else None)
            row_sym = row.get('symbol') or symbol
            if event == 'trade' and row.get('price') is not None:
                if row_sym == BTC_SYMBOL:
                    btc_ticks.append(row)
                elif row_sym == symbol:
                    ticks.append(row)
            elif event == 'quote':
                if row_sym == BTC_SYMBOL:
                    btc_quotes.append(row)
                elif row_sym == symbol:
                    quotes.append(row)
    ticks.sort(key=lambda r: r['ts_ms'])
    quotes.sort(key=lambda r: r['ts_ms'])
    btc_ticks.sort(key=lambda r: r['ts_ms'])
    btc_quotes.sort(key=lambda r: r['ts_ms'])
    return symbol, ticks, quotes, btc_ticks, btc_quotes


def _fwd_stats(ticks, ts_ms, price, side):
    future = [t for t in ticks if t['ts_ms'] >= ts_ms]
    if not future or not price:
        return {}
    prices = [float(t['price']) for t in future]
    if side == 'LONG':
        mfe = (max(prices) - price) / price * 100
        mae = (price - min(prices)) / price * 100
        end_ret = (prices[-1] - price) / price * 100
    else:
        mfe = (price - min(prices)) / price * 100
        mae = (max(prices) - price) / price * 100
        end_ret = (price - prices[-1]) / price * 100
    return {
        'mfe_pct': round(mfe, 3),
        'mae_pct': round(mae, 3),
        'end_return_pct': round(end_ret, 3),
    }


def replay_file(path: str, cooldown_sec: int = 30):
    symbol, ticks, quotes, btc_ticks, btc_quotes = _load_capture(path)
    st = SymbolState(symbol=symbol)
    btc_st = SymbolState(symbol=BTC_SYMBOL)
    pending_by_sec = defaultdict(list)
    for t in ticks:
        pending_by_sec[int(t['ts_ms']) // 1000].append(t)
    quotes_by_sec = defaultdict(list)
    for q in quotes:
        quotes_by_sec[int(q['ts_ms']) // 1000].append(q)
    btc_by_sec = defaultdict(list)
    for t in btc_ticks:
        btc_by_sec[int(t['ts_ms']) // 1000].append(t)
    btc_quotes_by_sec = defaultdict(list)
    for q in btc_quotes:
        btc_quotes_by_sec[int(q['ts_ms']) // 1000].append(q)
    signals = []
    last_signal = {}
    if not ticks:
        return {'path': path, 'symbol': symbol, 'ticks': 0, 'signals': signals}
    for sec in range(int(ticks[0]['ts_ms']) // 1000, int(ticks[-1]['ts_ms']) // 1000 + 1):
        for q in quotes_by_sec.get(sec, []):
            with st.lock:
                st.best_bid = float(q['bid']) if q.get('bid') is not None else st.best_bid
                st.best_ask = float(q['ask']) if q.get('ask') is not None else st.best_ask
                st.bid_size = int(q['bid_size']) if q.get('bid_size') is not None else st.bid_size
                st.ask_size = int(q['ask_size']) if q.get('ask_size') is not None else st.ask_size
                st.last_quote_ts_ms = q['ts_ms']
                st.quote_history.append((
                    q['ts_ms'], st.best_bid, st.best_ask,
                    st.bid_size or 0, st.ask_size or 0, q.get('imbalance'),
                ))
        for q in btc_quotes_by_sec.get(sec, []):
            with btc_st.lock:
                btc_st.best_bid = float(q['bid']) if q.get('bid') is not None else btc_st.best_bid
                btc_st.best_ask = float(q['ask']) if q.get('ask') is not None else btc_st.best_ask
                btc_st.bid_size = int(q['bid_size']) if q.get('bid_size') is not None else btc_st.bid_size
                btc_st.ask_size = int(q['ask_size']) if q.get('ask_size') is not None else btc_st.ask_size
                btc_st.last_quote_ts_ms = q['ts_ms']
                btc_st.quote_history.append((
                    q['ts_ms'], btc_st.best_bid, btc_st.best_ask,
                    btc_st.bid_size or 0, btc_st.ask_size or 0, q.get('imbalance'),
                ))
        btc_rows = btc_by_sec.get(sec, [])
        if btc_rows:
            bo = float(btc_rows[0]['price'])
            bh = max(float(r['price']) for r in btc_rows)
            bl = min(float(r['price']) for r in btc_rows)
            bc = float(btc_rows[-1]['price'])
            bv = sum(int(r.get('size') or 0) for r in btc_rows)
            bbuy_v = sum(int(r.get('size') or 0) for r in btc_rows if int(r.get('side') or 0) > 0)
            bsell_v = sum(int(r.get('size') or 0) for r in btc_rows if int(r.get('side') or 0) < 0)
            with btc_st.lock:
                btc_st.last_trade_price = bc
                btc_st.last_trade_ts_ms = btc_rows[-1]['ts_ms']
                btc_st.session_pv_sum += sum(float(r['price']) * int(r.get('size') or 0) for r in btc_rows)
                btc_st.session_v_sum += bv
                btc_st.bars_1s.append({
                    'ts_s': sec,
                    'o': bo, 'h': bh, 'l': bl, 'c': bc,
                    'v': bv, 'buy_v': bbuy_v, 'sell_v': bsell_v, 'n': len(btc_rows),
                })
        elif btc_st.bars_1s:
            prev_b = btc_st.bars_1s[-1]
            with btc_st.lock:
                btc_st.last_trade_price = prev_b['c']
                btc_st.last_trade_ts_ms = sec * 1000
                btc_st.bars_1s.append({
                    'ts_s': sec,
                    'o': prev_b['c'], 'h': prev_b['c'], 'l': prev_b['c'], 'c': prev_b['c'],
                    'v': 0, 'buy_v': 0, 'sell_v': 0, 'n': 0,
                })
        rows = pending_by_sec.get(sec, [])
        if rows:
            o = float(rows[0]['price'])
            h = max(float(r['price']) for r in rows)
            l = min(float(r['price']) for r in rows)
            c = float(rows[-1]['price'])
            v = sum(int(r.get('size') or 0) for r in rows)
            buy_v = sum(int(r.get('size') or 0) for r in rows if int(r.get('side') or 0) > 0)
            sell_v = sum(int(r.get('size') or 0) for r in rows if int(r.get('side') or 0) < 0)
            n = len(rows)
            with st.lock:
                st.last_trade_price = c
                st.last_trade_ts_ms = rows[-1]['ts_ms']
                st.session_pv_sum += sum(float(r['price']) * int(r.get('size') or 0) for r in rows)
                st.session_v_sum += v
        elif st.bars_1s:
            prev = st.bars_1s[-1]
            o = h = l = c = prev['c']
            v = buy_v = sell_v = n = 0
            with st.lock:
                st.last_trade_price = c
                st.last_trade_ts_ms = sec * 1000
        else:
            continue
        with st.lock:
            st.bars_1s.append({
                'ts_s': sec,
                'o': o, 'h': h, 'l': l, 'c': c,
                'v': v, 'buy_v': buy_v, 'sell_v': sell_v, 'n': n,
            })
        if len(st.bars_1s) < 60:
            continue
        ind = compute_indicators(st)
        ind['last_trade_age_sec'] = 0
        if st.last_quote_ts_ms:
            ind['last_quote_age_sec'] = max(0, sec - int(st.last_quote_ts_ms // 1000))
        else:
            ind['last_quote_age_sec'] = None
        if len(btc_st.bars_1s) >= 60:
            btc_ind = compute_indicators(btc_st)
            btc_ind['last_trade_age_sec'] = 0
        else:
            btc_ind = {
                'ready': True,
                'ema_stack': 'mixed',
                'mom_5s': 0,
                'mom_15s': 0,
                'mom_30s': 0,
                'mom_60s': 0,
                'mom_180s': 0,
                'last_trade_age_sec': 0,
            }
        sig = detect_signal(symbol, ind, btc_ind)
        if not sig:
            continue
        key = f"{sig.get('side')}:{sig.get('setup_type')}"
        if sec - last_signal.get(key, 0) < cooldown_sec:
            continue
        last_signal[key] = sec
        sig['best_bid'] = ind.get('best_bid')
        sig['best_ask'] = ind.get('best_ask')
        sig['execution_quality'] = execution_quality(sig)
        sig['shadow_variants'] = shadow_variants(sig)
        stats = _fwd_stats(ticks, sec * 1000, float(sig.get('price') or 0), sig.get('side'))
        signals.append({
            'sec': sec,
            'side': sig.get('side'),
            'score': sig.get('score'),
            'setup_type': sig.get('setup_type'),
            'price': sig.get('price'),
            'execution_quality': sig.get('execution_quality'),
            'shadow_variants': sig.get('shadow_variants'),
            **stats,
        })
    return {'path': path, 'symbol': symbol, 'ticks': len(ticks), 'signals': signals}


def _paths_for_day(day: str):
    folder = os.path.join(TICK_LOG_DIR, day)
    if not os.path.isdir(folder):
        return []
    return [
        os.path.join(folder, name)
        for name in sorted(os.listdir(folder))
        if name.lower().endswith('.jsonl')
    ]


def replay_day(day: str):
    results = [replay_file(path) for path in _paths_for_day(day)]
    all_signals = [s for r in results for s in r.get('signals', [])]
    by_setup = Counter(s.get('setup_type') for s in all_signals)
    variants = defaultdict(list)
    for sig in all_signals:
        for name, active in (sig.get('shadow_variants') or {}).items():
            if active:
                variants[name].append(sig)
    variant_scoreboard = []
    for name, sigs in variants.items():
        wins = sum(1 for s in sigs if (s.get('end_return_pct') or 0) > 0)
        avg_end = sum(s.get('end_return_pct') or 0 for s in sigs) / len(sigs) if sigs else 0
        avg_mfe = sum(s.get('mfe_pct') or 0 for s in sigs) / len(sigs) if sigs else 0
        avg_mae = sum(s.get('mae_pct') or 0 for s in sigs) / len(sigs) if sigs else 0
        variant_scoreboard.append({
            'variant': name,
            'signals': len(sigs),
            'win_rate': round(wins / len(sigs) * 100, 1) if sigs else None,
            'avg_end_return_pct': round(avg_end, 3),
            'avg_mfe_pct': round(avg_mfe, 3),
            'avg_mae_pct': round(avg_mae, 3),
        })
    return {
        'day': day,
        'files': len(results),
        'ticks': sum(r.get('ticks', 0) for r in results),
        'signals': len(all_signals),
        'by_setup': dict(by_setup),
        'variant_scoreboard': sorted(
            variant_scoreboard,
            key=lambda r: (r['avg_end_return_pct'], r['win_rate'] or 0),
            reverse=True,
        ),
        'files_detail': results,
    }


def main():
    ap = argparse.ArgumentParser(description='Replay tick capture JSONL files through current ws_scalp logic.')
    ap.add_argument('paths', nargs='*')
    ap.add_argument('--day')
    ap.add_argument('--json', action='store_true')
    args = ap.parse_args()
    if args.day:
        result = replay_day(args.day)
        if args.json:
            print(json.dumps(result, indent=2, default=str))
            return
        print(f"Replay day {args.day}: files={result['files']} ticks={result['ticks']} signals={result['signals']}")
        print('Signals by setup:', result['by_setup'])
        if result['variant_scoreboard']:
            print('Variant scoreboard:')
            for row in result['variant_scoreboard']:
                print(f"  {row['variant']}: signals={row['signals']} win={row['win_rate']}% "
                      f"avg_end={row['avg_end_return_pct']}% "
                      f"mfe={row['avg_mfe_pct']}% mae={row['avg_mae_pct']}%")
        return
    results = [replay_file(p) for p in args.paths]
    if args.json:
        print(json.dumps(results, indent=2, default=str))
        return
    for r in results:
        print(f"{r['symbol']} {os.path.basename(r['path'])}: ticks={r['ticks']} signals={len(r['signals'])}")
        for sig in r['signals'][:10]:
            print(f"  sec={sig['sec']} {sig['side']} score={sig['score']} "
                  f"setup={sig['setup_type']} price={sig['price']} "
                  f"mfe={sig.get('mfe_pct')} mae={sig.get('mae_pct')}")


if __name__ == '__main__':
    main()
