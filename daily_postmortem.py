"""
Daily post-mortem for mock_trader. Runs at 15:05 CT after force-flat.

Three-layer report (per session design):
  Layer 1 â€” Health checks (alarm on anomaly): snapshot persistence, signal
            source flag, bar-loop activity, orphan reconcile, time-stop
            firings, unexpected exit reasons.
  Layer 2 â€” Today's tape (record, no action): per-trade entry indicators,
            exit reason + forward returns, ws_scalp context,
            duration, max adverse excursion.
  Layer 3 â€” Rolling-window comparison (NOT YET ENABLED â€” needs â‰¥10 days of
            postmortem JSON corpus to populate). Will activate after the
            corpus is built.

Outputs:
  1. C:\\xampp\\htdocs\\Claude\\postmortem\\postmortem_YYYY-MM-DD.json â€” structured corpus
  2. Same dir, postmortem_YYYY-MM-DD.txt â€” human-readable text body
  3. Appended to the rolling Google Doc via gdocs_writer

Idempotent: re-running on the same date overwrites the JSON/TXT and inserts
a NEW dated section in the Doc (we don't dedup the Doc â€” manual cleanup if
you re-run for testing).
"""
from __future__ import annotations
import sys, os, json, re
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Optional
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

CT = ZoneInfo('America/Chicago')
HERE = os.path.dirname(os.path.abspath(__file__))
STATE_PATH = os.path.join(HERE, 'mock_trader_state.json')
LOG_PATH   = os.path.join(HERE, 'mock_trader.log')
OUT_DIR    = os.path.join(HERE, 'postmortem')
SKIPPED_DIR = os.path.join(OUT_DIR, 'skipped_signals')
SHADOW_DIR = os.path.join(OUT_DIR, 'shadow_decisions')
SHADOW_EXIT_DIR = os.path.join(OUT_DIR, 'shadow_exits')
NEAR_SIGNAL_DIR = os.path.join(OUT_DIR, 'near_signals')
TRADE_CORPUS_DIR = os.path.join(OUT_DIR, 'trades')
AUDIT_DIR = os.path.join(HERE, 'audit')
os.makedirs(OUT_DIR, exist_ok=True)

JSON_DETAIL_ROW_LIMIT = 250
_BAR_DAY_CACHE = {}
_JSONL_DAY_CACHE = {}


# â”€â”€ helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def today_iso(override: Optional[str] = None) -> str:
    if override: return override
    return datetime.now(CT).date().isoformat()


def load_state():
    with open(STATE_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)


def write_json_atomic(path: str, payload: dict) -> None:
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, default=str)
    os.replace(tmp, path)


def load_human_notes(day_iso):
    path = os.path.join(OUT_DIR, f'notes_{day_iso}.json')
    default = {
        'trader_notes': '',
        'market_feel': '',
        'known_catalysts': [],
        'external_context': '',
    }
    if not os.path.exists(path):
        return default
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, dict):
            default.update(data)
    except Exception:
        pass
    return default


def trades_for_day(state, day_iso):
    rows = [t for t in state.get('trades', [])
            if datetime.fromtimestamp(t['closed_at'], CT).strftime('%Y-%m-%d') == day_iso]
    path = os.path.join(TRADE_CORPUS_DIR, f'trades_{day_iso}.jsonl')
    rows.extend(_read_jsonl_day(path, day_iso, ts_field='closed_at'))
    seen = set()
    out = []
    for t in rows:
        key = (
            t.get('trade_id'), t.get('ticker'), t.get('side'),
            t.get('opened_at'), t.get('closed_at'), t.get('pnl'),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(t)
    return out

def _read_jsonl_day(path, day_iso, ts_field='created_at'):
    cache_key = (path, day_iso, ts_field)
    if cache_key in _JSONL_DAY_CACHE:
        return list(_JSONL_DAY_CACHE[cache_key])
    rows = []
    if not os.path.exists(path):
        _JSONL_DAY_CACHE[cache_key] = rows
        return []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                row = json.loads(line)
            except Exception:
                continue
            try:
                row_day = datetime.fromtimestamp(row.get(ts_field, 0), CT).strftime('%Y-%m-%d')
            except Exception:
                row_day = None
            if row_day == day_iso:
                rows.append(row)
    _JSONL_DAY_CACHE[cache_key] = rows
    return list(rows)


def skipped_for_day(state, day_iso):
    rows = [
        s for s in state.get('skipped_signals', [])
        if datetime.fromtimestamp(s.get('created_at', 0), CT).strftime('%Y-%m-%d') == day_iso
    ]
    path = os.path.join(SKIPPED_DIR, f'skipped_signals_{day_iso}.jsonl')
    rows.extend(_read_jsonl_day(path, day_iso))
    seen = set()
    out = []
    for s in rows:
        key = (
            s.get('created_at'), s.get('ticker'), s.get('side'),
            s.get('reason'), s.get('price'), s.get('score'),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(s)
    return out


def audit_events_for_day(day_iso):
    path = os.path.join(AUDIT_DIR, f'trade_lifecycle_{day_iso}.jsonl')
    rows = []
    if not os.path.exists(path):
        return rows
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                row = json.loads(line)
            except Exception:
                continue
            if row.get('ts_ct', '')[:10] == day_iso:
                rows.append(row)
    return rows


def shadow_decisions_for_day(day_iso):
    path = os.path.join(SHADOW_DIR, f'shadow_decisions_{day_iso}.jsonl')
    return _read_jsonl_day(path, day_iso)


def shadow_exits_for_day(day_iso):
    path = os.path.join(SHADOW_EXIT_DIR, f'shadow_exits_{day_iso}.jsonl')
    return _read_jsonl_day(path, day_iso)


def near_signals_for_day(day_iso):
    path = os.path.join(NEAR_SIGNAL_DIR, f'near_signals_{day_iso}.jsonl')
    return _read_jsonl_day(path, day_iso)


def get_path(data, path, default=None):
    cur = data
    for key in path.split('.'):
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def is_expected_exit_reason(reason):
    reason = str(reason or '')
    exact = {
        'take_profit',
        'stop_loss',
        'session_end',
        'cond_time_stop',
        'profit_protect_giveback',
        'profit_protect_breakeven',
        'manual_stop',
        'external_broker_exit',
    }
    prefixes = (
        'failed_followthrough_',
        'hard_adverse_exit_',
        'btc_impulse_abort_',
        'short_conviction_decay_',
        'long_conviction_decay_',
    )
    return reason in exact or any(reason.startswith(p) for p in prefixes)


def slim_detail_row(row):
    if not isinstance(row, dict):
        return row
    keep = (
        'created_at', 'ts_ct', 'ticker', 'side', 'decision', 'reason',
        'reject_reason', 'setup_type', 'score', 'conviction', 'price',
        'entry_price', 'signal_price', 'fwd', 'quality', 'tags',
        'time', 'entry', 'exit', 'pnl', 'duration_min', 'mfe_pct', 'mae_pct',
    )
    out = {k: row.get(k) for k in keep if k in row}
    indicators = row.get('indicators') or row.get('snapshot') or {}
    if isinstance(indicators, dict):
        out['indicators'] = {
            k: indicators.get(k)
            for k in (
                'ema_stack', 'mom_5s', 'mom_15s', 'mom_60s',
                'vwap_dist_sigma', 'spread_pct',
            )
            if k in indicators
        }
    btc = row.get('btc') or row.get('btc_indicators') or {}
    if isinstance(btc, dict):
        out['btc'] = {
            k: btc.get(k)
            for k in ('regime', 'stack', 'ema_stack', 'mom_60s', 'conflict')
            if k in btc
        }
    return out


def compact_rows(rows, limit=JSON_DETAIL_ROW_LIMIT):
    rows = list(rows or [])
    return {
        'count': len(rows),
        'truncated': len(rows) > limit,
        'limit': limit,
        'rows': [slim_detail_row(r) for r in rows[:limit]],
    }


def rollup_rows(rows, fields=('ticker', 'side', 'reason'), top_n=25):
    rows = list(rows or [])
    out = {'count': len(rows), 'by_field': {}, 'top_combos': []}
    for field in fields:
        counts = {}
        for row in rows:
            value = row.get(field)
            if value is None:
                value = 'None'
            value = str(value)
            counts[value] = counts.get(value, 0) + 1
        out['by_field'][field] = [
            {'value': k, 'count': v}
            for k, v in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[:top_n]
        ]
    combo_counts = {}
    for row in rows:
        key = '|'.join(str(row.get(f) if row.get(f) is not None else 'None') for f in fields)
        combo_counts[key] = combo_counts.get(key, 0) + 1
    out['top_combos'] = [
        {'key': k, 'count': v}
        for k, v in sorted(combo_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:top_n]
    ]
    return out


def corpus_file_ref(kind, day_iso):
    dirs = {
        'skipped_signals': SKIPPED_DIR,
        'shadow_decisions': SHADOW_DIR,
        'shadow_exits': SHADOW_EXIT_DIR,
        'near_signals': NEAR_SIGNAL_DIR,
    }
    base = dirs.get(kind, OUT_DIR)
    return os.path.join(base, f'{kind}_{day_iso}.jsonl')


def fmt_num(v, nd=3, suffix=''):
    if v is None:
        return 'n/a'
    try:
        return f'{float(v):+.{nd}f}{suffix}'
    except Exception:
        return str(v)


def fmt_plain(v, nd=3):
    if v is None:
        return 'n/a'
    try:
        return f'{float(v):.{nd}f}'
    except Exception:
        return str(v)


def parse_log_for_day(day_iso, patterns):
    """Return dict pattern_name -> list of matching lines from mock_trader.log
    where the line's timestamp prefix matches day_iso."""
    out = {name: [] for name in patterns}
    if not os.path.exists(LOG_PATH):
        return out
    prefix = day_iso  # log lines start "YYYY-MM-DD HH:MM:SS"
    with open(LOG_PATH, 'r', encoding='utf-8', errors='replace') as f:
        for line in f:
            if not line.startswith(prefix):
                continue
            for name, pat in patterns.items():
                if pat in line:
                    out[name].append(line.rstrip())
    return out


def fwd_returns_from_bars(bars, entry_ms, entry_px, side, horizons=(5, 15, 60)):
    """Forward returns at +5/15/60 min after entry. Signed for the trade side."""
    bars = sorted([b for b in (bars or []) if b['t'] >= entry_ms], key=lambda b: b['t'])
    out = {'_status': 'ok', '_error': None}
    for n in horizons:
        target = entry_ms + n * 60_000
        bar = next((b for b in bars if b['t'] >= target), None)
        if bar is None:
            out[n] = None
            continue
        ret = (bar['c'] - entry_px) / entry_px * 100
        if side == 'SHORT': ret = -ret
        out[n] = round(ret, 3)
    return out


def prior_postmortem_payloads(day_iso, max_days=5):
    payloads = []
    try:
        cutoff = datetime.strptime(day_iso, '%Y-%m-%d').date()
    except Exception:
        return payloads
    for name in sorted(os.listdir(OUT_DIR), reverse=True):
        m = re.match(r'postmortem_(\d{4}-\d{2}-\d{2})\.json$', name)
        if not m:
            continue
        d = datetime.strptime(m.group(1), '%Y-%m-%d').date()
        if d >= cutoff:
            continue
        try:
            with open(os.path.join(OUT_DIR, name), 'r', encoding='utf-8') as f:
                payloads.append(json.load(f))
        except Exception:
            continue
        if len(payloads) >= max_days:
            break
    return payloads


def fwd_returns_from_alpaca(tkr, entry_ms, entry_px, side, bars=None):
    """Forward returns at +5/15/60 min after entry. Signed for the trade side."""
    if bars is not None:
        return fwd_returns_from_bars(bars, entry_ms, entry_px, side)
    try:
        from data_fetch import alpaca_stock_bars
    except Exception as e:
        return {5: None, 15: None, 60: None, '_status': 'import_error', '_error': str(e)}
    end_ms = entry_ms + 75 * 60_000
    iso_s = datetime.fromtimestamp(entry_ms/1000, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    iso_e = datetime.fromtimestamp(end_ms/1000,   tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    try:
        bars = alpaca_stock_bars(tkr, iso_s, iso_e, '1Min')
    except Exception as e:
        return {5: None, 15: None, 60: None, '_status': 'fetch_error', '_error': str(e)}
    return fwd_returns_from_bars(bars, entry_ms, entry_px, side)


def load_forward_bars_for_day(trades, day_iso):
    """Fetch each ticker/day bar set once for forward-return calculations."""
    if not trades:
        return {}, {}
    try:
        from data_fetch import alpaca_stock_bars
    except Exception as e:
        return {}, {t.get('ticker', '?'): ('import_error', str(e)) for t in trades}

    out = {}
    errors = {}
    day_start = datetime.strptime(day_iso, '%Y-%m-%d').replace(tzinfo=CT)
    day_end = day_start + timedelta(days=1)
    iso_s = day_start.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    iso_e = day_end.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    for tkr in sorted({t.get('ticker') for t in trades if t.get('ticker')}):
        cache_key = (tkr, day_iso)
        ticker_trades = [t for t in trades if t.get('ticker') == tkr]
        off_day = [
            t for t in ticker_trades
            if datetime.fromtimestamp(t['opened_at'], CT).strftime('%Y-%m-%d') != day_iso
        ]
        if off_day:
            errors[tkr] = ('day_mismatch', f'{len(off_day)} trade(s) not opened on {day_iso}')
            continue
        if cache_key in _BAR_DAY_CACHE:
            out[tkr] = _BAR_DAY_CACHE[cache_key]
            continue
        try:
            out[tkr] = alpaca_stock_bars(tkr, iso_s, iso_e, '1Min')
            _BAR_DAY_CACHE[cache_key] = out[tkr]
        except Exception as e:
            errors[tkr] = ('fetch_error', str(e))
    return out, errors


def load_bars_for_tickers_day(tickers, day_iso):
    if not tickers:
        return {}, {}
    try:
        from data_fetch import alpaca_stock_bars
    except Exception as e:
        return {}, {t: ('import_error', str(e)) for t in tickers}
    day_start = datetime.strptime(day_iso, '%Y-%m-%d').replace(tzinfo=CT)
    day_end = day_start + timedelta(days=1)
    iso_s = day_start.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    iso_e = day_end.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    bars = {}
    errors = {}
    for tkr in sorted(set(tickers)):
        cache_key = (tkr, day_iso)
        if cache_key in _BAR_DAY_CACHE:
            bars[tkr] = _BAR_DAY_CACHE[cache_key]
            continue
        try:
            bars[tkr] = alpaca_stock_bars(tkr, iso_s, iso_e, '1Min')
            _BAR_DAY_CACHE[cache_key] = bars[tkr]
        except Exception as e:
            errors[tkr] = ('fetch_error', str(e))
    return bars, errors


def skipped_with_bar_forwards(state, day_iso):
    skipped = skipped_for_day(state or {}, day_iso)
    tickers = [s.get('ticker') for s in skipped if s.get('ticker')]
    bars_by_ticker, errors = load_bars_for_tickers_day(tickers, day_iso)
    rows = []
    for s in skipped:
        row = dict(s)
        if row.get('ticker') in errors:
            status, err = errors[row['ticker']]
            row['fwd'] = {'_status': status, '_error': err}
            rows.append(row)
            continue
        if not row.get('created_at') or not row.get('price'):
            rows.append(row)
            continue
        fwd = fwd_returns_from_bars(
            bars_by_ticker.get(row.get('ticker'), []),
            int(row['created_at']) * 1000,
            float(row['price']),
            row.get('side'),
            horizons=(5, 15, 30),
        )
        row['fwd'] = {
            '5m': {'signed_return_pct': fwd.get(5)},
            '15m': {'signed_return_pct': fwd.get(15)},
            '30m': {'signed_return_pct': fwd.get(30)},
            '_status': fwd.get('_status'),
            '_error': fwd.get('_error'),
        }
        rows.append(row)
    return rows


def shadow_decisions_with_bar_forwards(day_iso):
    rows_in = shadow_decisions_for_day(day_iso)
    tickers = [s.get('ticker') for s in rows_in if s.get('ticker')]
    bars_by_ticker, errors = load_bars_for_tickers_day(tickers, day_iso)
    rows = []
    for s in rows_in:
        row = dict(s)
        if row.get('ticker') in errors:
            status, err = errors[row['ticker']]
            row['fwd'] = {'_status': status, '_error': err}
            rows.append(row)
            continue
        if not row.get('created_at') or not row.get('price'):
            rows.append(row)
            continue
        fwd = fwd_returns_from_bars(
            bars_by_ticker.get(row.get('ticker'), []),
            int(row['created_at']) * 1000,
            float(row['price']),
            row.get('side'),
            horizons=(1, 3, 5, 10, 15),
        )
        row['fwd'] = {
            '1m': {'signed_return_pct': fwd.get(1)},
            '3m': {'signed_return_pct': fwd.get(3)},
            '5m': {'signed_return_pct': fwd.get(5)},
            '10m': {'signed_return_pct': fwd.get(10)},
            '15m': {'signed_return_pct': fwd.get(15)},
            '_status': fwd.get('_status'),
            '_error': fwd.get('_error'),
        }
        rows.append(row)
    return rows


# â”€â”€ Layer 1 â€” health checks â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def layer1_health(state, day_iso, log_hits):
    flags = []
    notes = []
    today_trades = trades_for_day(state, day_iso)

    # 1a. snapshot persistence
    if today_trades:
        n_pop = sum(1 for t in today_trades
                    if isinstance(t.get('indicators'), dict) and t['indicators'])
        if n_pop == 0:
            flags.append('SNAPSHOT_EMPTY: no trade today has populated `indicators` '
                         'dict â€” forensic capture is broken')
        elif n_pop < len(today_trades):
            flags.append(f'SNAPSHOT_PARTIAL: {n_pop}/{len(today_trades)} trades '
                         f'have populated indicators')
        else:
            notes.append(f'snapshots OK: {n_pop}/{len(today_trades)} trades populated')
    else:
        notes.append('no trades today (skipping snapshot check)')

    # 1b. signal_source flag
    src_lines = log_hits.get('signal_source', [])
    if src_lines:
        last = src_lines[-1]
        if 'signal_source=ws_scalp' in last:
            notes.append('signal_source=ws_scalp (active)')
        else:
            notes.append(f'signal_source unclear: {last}')
    else:
        notes.append('no MockTrader boot today (process started prior day)')

    # 1c. engine activity
    opens = log_hits.get('open', [])
    closes = log_hits.get('close', [])
    notes.append(f'ws_scalp OPEN log lines today: {len(opens)}')
    notes.append(f'ws_scalp CLOSE log lines today: {len(closes)}')
    if today_trades and not opens:
        flags.append('OPEN_LOG_MISSING: trades closed today but no OPEN log lines found')

    # 1d. legacy flat time-stop firings. Conditional time-stop is expected.
    ts_fires = log_hits.get('time_stop', [])
    if ts_fires:
        flags.append(f'LEGACY_TIME_STOP_FIRED: {len(ts_fires)} TIME-STOP line(s) found')

    # 1e. unexpected exit reasons
    if today_trades:
        reasons = {}
        for t in today_trades:
            reasons[t.get('reason','?')] = reasons.get(t.get('reason','?'), 0) + 1
        notes.append(f'exit reasons: {dict(sorted(reasons.items(), key=lambda x: -x[1]))}')
        unusual = [r for r in reasons if not is_expected_exit_reason(r)]
        if unusual:
            flags.append(f'UNUSUAL_EXIT_REASONS: {unusual}')

    # 1f. orphan reconcile
    orphans = [
        line for line in log_hits.get('orphans', [])
        if 'BOOT_RECONCILE_NOT_FLAT' in line
        or 'Alpaca has orphan positions' in line
        or re.search(r"orphans_flattened=\[(?!\])", line)
    ]
    if orphans:
        notes.append(f'orphan reconcile fired at boot: {len(orphans)} line(s)')

    return flags, notes


# â”€â”€ Layer 2 â€” today's tape â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def layer2_tape(state, day_iso, log_hits):
    today = trades_for_day(state, day_iso)
    bars_by_ticker, bar_errors = load_forward_bars_for_day(today, day_iso)
    rows = []
    for t in today:
        entry_ms = t['opened_at'] * 1000
        if t['ticker'] in bar_errors:
            status, err = bar_errors[t['ticker']]
            fwd = {5: None, 15: None, 60: None, '_status': status, '_error': err}
        else:
            fwd = fwd_returns_from_alpaca(
                t['ticker'], entry_ms, t['entry'], t['side'],
                bars=bars_by_ticker.get(t['ticker'], []),
            )
        rows.append({
            'time':   datetime.fromtimestamp(t['opened_at'], CT).strftime('%H:%M'),
            'ticker': t['ticker'],
            'side':   t['side'],
            'conv':   t.get('conviction','?'),
            'entry':  t['entry'],
            'exit':   t['exit'],
            'pnl':    t['pnl'],
            'reason': t.get('reason','?'),
            'duration_min': round((t['closed_at'] - t['opened_at'])/60, 1),
            'opened_at': t.get('opened_at'),
            'closed_at': t.get('closed_at'),
            'fwd5':  fwd.get(5),
            'fwd15': fwd.get(15),
            'fwd60': fwd.get(60),
            'fwd_status': fwd.get('_status'),
            'fwd_error': fwd.get('_error'),
            'ind':   t.get('indicators') or {},
            'btc':   t.get('btc_indicators') or {},
            'score': t.get('entry_score'),
            'mfe_pct': t.get('mfe_pct'),
            'mae_pct': t.get('mae_pct'),
            'forensics': t.get('forensics') or {},
            'entry_thesis': t.get('entry_thesis') or {},
            'entry_quality_tier': (
                t.get('entry_quality_tier')
                or get_path(t, 'entry_thesis.entry_quality_tier')
                or get_path(t, 'forensics.entry_quality_tier.tier')
            ),
            'entry_quality_score': (
                t.get('entry_quality_score')
                or get_path(t, 'entry_thesis.entry_quality_score')
                or get_path(t, 'forensics.entry_quality_tier.score')
            ),
            'path': t.get('path') or {},
            'entry_slippage_pct': t.get('entry_slippage_pct'),
            'seconds_to_fill': t.get('seconds_to_fill'),
            'strategy_config_hash': t.get('strategy_config_hash'),
            'decision_quality': t.get('decision_quality'),
            'exit_decision_context': t.get('exit_decision_context'),
            'replay': t.get('replay'),
            'broker_exit_order_id': t.get('broker_exit_order_id'),
            'broker_exit_fill_price': t.get('broker_exit_fill_price'),
            'broker_exit_filled_at': t.get('broker_exit_filled_at'),
            'broker_close': t.get('broker_close'),
        })
    return rows


def entry_quality_tier_from_row(row):
    if row.get('entry_quality_tier'):
        return {
            'tier': row.get('entry_quality_tier'),
            'score': row.get('entry_quality_score'),
            'tags': get_path(row, 'entry_thesis.entry_quality_tags', []) or [],
        }
    f = row.get('forensics') or {}
    sig = f.get('signal') or {}
    eq = f.get('entry_quality') or {}
    btc = f.get('btc') or {}
    rel = f.get('relative_strength') or {}
    sq = f.get('signal_quality') or {}
    side = row.get('side')
    score = 50
    tags = []
    raw_score = row.get('score') if row.get('score') is not None else sig.get('score')
    try:
        abs_score = abs(float(raw_score))
        if abs_score >= 8:
            score += 15
            tags.append('strong_signal_score')
        elif abs_score >= 6:
            score += 8
            tags.append('solid_signal_score')
        else:
            score -= 8
            tags.append('thin_signal_score')
    except Exception:
        tags.append('score_unknown')
    if btc.get('conflict'):
        score -= 25
        tags.append('btc_conflict')
    elif btc.get('agreement') is True:
        score += 12
        tags.append('btc_aligned')
    elif btc.get('agreement') is False:
        score -= 8
        tags.append('btc_not_confirming')
    spread_pct = eq.get('spread_pct')
    if spread_pct is not None:
        try:
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
    if sq.get('flow_confirmed') or sq.get('burst') or sq.get('followthrough'):
        score += 6
        tags.append('flow_or_followthrough_confirmed')
    score = max(0, min(100, int(round(score))))
    tier = 'A' if score >= 75 else ('B' if score >= 58 else 'C')
    return {'tier': tier, 'score': score, 'tags': tags}


def loser_archetypes(row, tags=None):
    tags = set(tags or loser_diagnosis(row)[1])
    out = []
    reason = str(row.get('reason') or '')
    if 'btc_conflict' in tags or 'btc_not_confirming' in tags:
        out.append('wrong_side_btc_or_unconfirmed_btc')
    if 'never_green' in tags or 'short_into_positive_60s_mom' in tags or 'long_into_negative_60s_mom' in tags:
        out.append('late_or_poorly_timed_entry')
    if 'stock_lagged_btc' in tags or 'stock_outperformed_btc' in tags:
        out.append('miner_btc_divergence')
    if 'profit_reversal' in tags:
        out.append('failed_to_keep_winner')
    if 'broker_disaster_stop_cap_exit' in tags:
        out.append('broker_disaster_stop_cap_exit')
    if 'wide_spread' in tags or 'bad_fill' in tags or 'exit_fill_unknown' in tags:
        out.append('execution_or_slippage')
    if reason in ('manual_stop', 'session_end', 'external_broker_exit') or reason.startswith('external_'):
        out.append('operational_or_forced_exit')
    if 'short_above_vwap_extension' in tags or 'long_below_vwap_extension' in tags:
        out.append('overextended_vwap_chase_or_fade')
    if not out:
        out.append('valid_stopout_or_normal_variance')
    return out


def thesis_failure_label(row, tags=None):
    tags = set(tags or loser_diagnosis(row)[1])
    reason = str(row.get('reason') or '')
    if reason in ('manual_stop', 'session_end', 'external_broker_exit') or reason.startswith('external_'):
        return {'label': 'operations_or_forced_exit', 'confidence': 'high'}
    if 'btc_conflict' in tags or 'btc_not_confirming' in tags:
        return {'label': 'btc_reversal_or_non_confirmation', 'confidence': 'medium'}
    if 'stock_lagged_btc' in tags or 'stock_outperformed_btc' in tags:
        return {'label': 'miner_btc_relationship_failed', 'confidence': 'medium'}
    if 'short_into_positive_60s_mom' in tags or 'long_into_negative_60s_mom' in tags or 'never_green' in tags:
        return {'label': 'entry_timing_failed', 'confidence': 'medium'}
    if 'profit_reversal' in tags:
        return {'label': 'exit_failed_after_mfe', 'confidence': 'medium'}
    if 'broker_disaster_stop_cap_exit' in tags:
        return {'label': 'broker_disaster_stop_cap_review', 'confidence': 'medium'}
    if 'wide_spread' in tags or 'bad_fill' in tags or 'exit_fill_unknown' in tags:
        return {'label': 'execution_quality_failed', 'confidence': 'high'}
    if 'exit_rule_candidate' in tags:
        return {'label': 'exit_policy_review', 'confidence': 'low-medium'}
    return {'label': 'valid_loss_or_unclassified', 'confidence': 'low'}


def loser_replay_snapshot(row):
    path = row.get('path') or {}
    checks = path.get('checks') or {}
    return {
        'ticker': row.get('ticker'),
        'side': row.get('side'),
        'time': row.get('time'),
        'pnl': row.get('pnl'),
        'reason': row.get('reason'),
        'entry': row.get('entry'),
        'mfe_pct': row.get('mfe_pct'),
        'mae_pct': row.get('mae_pct'),
        'first_green_sec': (
            int(path.get('first_green_ts') - row.get('opened_at'))
            if path.get('first_green_ts') and row.get('opened_at') else None
        ),
        'first_red_sec': (
            int(path.get('first_red_ts') - row.get('opened_at'))
            if path.get('first_red_ts') and row.get('opened_at') else None
        ),
        'time_to_mfe_sec': path.get('time_to_mfe_sec'),
        'time_to_mae_sec': path.get('time_to_mae_sec'),
        'checkpoints': {
            label: {
                'signed_return_pct': (checks.get(label) or {}).get('signed_return_pct'),
                'price': (checks.get(label) or {}).get('price'),
            }
            for label in ('30s', '1m', '3m', '5m')
            if label in checks
        },
    }


def trade_thesis_timeline(row):
    path = row.get('path') or {}
    checks = path.get('checks') or {}
    checkpoints = []
    for label in ('30s', '1m', '3m', '5m'):
        chk = checks.get(label) or {}
        signed = chk.get('signed_return_pct')
        if signed is None:
            status = 'missing'
        elif signed > 0.12:
            status = 'confirmed'
        elif signed < -0.12:
            status = 'invalidated'
        elif signed > 0:
            status = 'slightly_confirmed'
        elif signed < 0:
            status = 'weakened'
        else:
            status = 'flat'
        checkpoints.append({
            'checkpoint': label,
            'price': chk.get('price'),
            'signed_return_pct': signed,
            'status': status,
        })
    final_status = 'winner' if (row.get('pnl') or 0) > 0 else ('loser' if (row.get('pnl') or 0) < 0 else 'flat')
    invalidated = any(c['status'] == 'invalidated' for c in checkpoints)
    confirmed = any(c['status'] == 'confirmed' for c in checkpoints)
    if invalidated and final_status == 'winner':
        verdict = 'lucky_or_late_reversal_winner'
    elif confirmed and final_status == 'loser':
        verdict = 'confirmed_then_failed'
    elif invalidated:
        verdict = 'thesis_invalidated'
    elif confirmed:
        verdict = 'thesis_confirmed'
    else:
        verdict = 'thesis_unclear'
    return {
        'ticker': row.get('ticker'),
        'side': row.get('side'),
        'time': row.get('time'),
        'setup': setup_type(row) if 'setup_type' in globals() else None,
        'entry_quality_tier': entry_quality_tier_from_row(row),
        'final_result': final_status,
        'pnl': row.get('pnl'),
        'mfe_pct': row.get('mfe_pct'),
        'mae_pct': row.get('mae_pct'),
        'verdict': verdict,
        'checkpoints': checkpoints,
    }


def build_trade_thesis_timelines(tape):
    return [trade_thesis_timeline(r) for r in tape]


def winner_quality_label(row):
    tags = []
    f = row.get('forensics') or {}
    eq = f.get('entry_quality') or {}
    btc = f.get('btc') or {}
    timeline = trade_thesis_timeline(row)
    label = 'clean_winner'
    if btc.get('conflict') or btc.get('agreement') is False:
        tags.append('btc_conflict_or_unconfirmed')
    spread = eq.get('spread_pct')
    try:
        if spread is not None and float(spread) >= 0.08:
            tags.append('wide_spread_winner')
    except Exception:
        pass
    if timeline.get('verdict') == 'lucky_or_late_reversal_winner':
        tags.append('lucky_reversal')
    if row.get('reason') and str(row.get('reason')).startswith(('profit_protect_', 'failed_followthrough_', 'hard_adverse_', 'short_conviction_', 'long_conviction_')):
        tags.append('saved_by_exit')
    if 'lucky_reversal' in tags:
        label = 'lucky_reversal'
    elif 'btc_conflict_or_unconfirmed' in tags:
        label = 'btc_conflict_winner'
    elif 'wide_spread_winner' in tags:
        label = 'wide_spread_winner'
    elif 'saved_by_exit' in tags:
        label = 'saved_by_exit'
    return {'label': label, 'tags': tags or ['clean_context']}


def winner_quality_review(tape):
    winners = [r for r in tape if (r.get('pnl') or 0) > 0]
    rows = []
    for r in winners:
        q = winner_quality_label(r)
        rows.append({
            'time': r.get('time'),
            'ticker': r.get('ticker'),
            'side': r.get('side'),
            'pnl': r.get('pnl'),
            'reason': r.get('reason'),
            'quality': q,
            'timeline_verdict': trade_thesis_timeline(r).get('verdict'),
            'entry_quality_tier': entry_quality_tier_from_row(r).get('tier'),
        })
    counts = Counter((r.get('quality') or {}).get('label') for r in rows)
    return {
        'counts': dict(counts),
        'rows': rows,
    }


def loser_diagnosis(row):
    f = row.get('forensics') or {}
    ind = row.get('ind') or {}
    btc = f.get('btc') or {}
    rel = f.get('relative_strength') or {}
    loc = f.get('location') or {}
    eq = f.get('entry_quality') or {}
    path = row.get('path') or {}
    checks = path.get('checks') or {}
    side = row.get('side')
    tags = []
    evidence = []

    if btc.get('conflict'):
        tags.append('btc_conflict')
        evidence.append(f"BTC conflict: stack={btc.get('stack')} mom60={fmt_num(btc.get('mom_60s'), 3, '%')}")
    elif btc.get('agreement') is False and btc.get('regime') not in (None, 'neutral'):
        tags.append('btc_not_confirming')
        evidence.append(f"BTC not confirming: regime={btc.get('regime')} mom60={fmt_num(btc.get('mom_60s'), 3, '%')}")

    rs = rel.get('stock_minus_btc_implied_60s')
    if rs is not None:
        if side == 'LONG' and rs < -0.10:
            tags.append('stock_lagged_btc')
            evidence.append(f"Stock lagged BTC-implied move by {fmt_num(rs, 3, '%')}")
        elif side == 'SHORT' and rs > 0.10:
            tags.append('stock_outperformed_btc')
            evidence.append(f"Stock outperformed BTC-implied move by {fmt_num(rs, 3, '%')} against SHORT")

    mom60 = ind.get('mom_60s')
    if side == 'SHORT' and mom60 is not None and mom60 > 0:
        tags.append('short_into_positive_60s_mom')
        evidence.append(f"SHORT entered with stock mom60={fmt_num(mom60, 3, '%')}")
    elif side == 'LONG' and mom60 is not None and mom60 < 0:
        tags.append('long_into_negative_60s_mom')
        evidence.append(f"LONG entered with stock mom60={fmt_num(mom60, 3, '%')}")

    sigma = loc.get('vwap_dist_sigma')
    if sigma is None:
        sigma = ind.get('vwap_dist_sigma')
    if sigma is not None:
        if side == 'SHORT' and sigma > 2:
            tags.append('short_above_vwap_extension')
            evidence.append(f"SHORT fade above VWAP extension: {fmt_num(sigma, 2)} sigma")
        elif side == 'LONG' and sigma < -2:
            tags.append('long_below_vwap_extension')
            evidence.append(f"LONG fade below VWAP extension: {fmt_num(sigma, 2)} sigma")

    spread_pct = eq.get('spread_pct')
    if spread_pct is not None and spread_pct > 0.08:
        tags.append('wide_spread')
        evidence.append(f"Wide spread at signal: {fmt_num(spread_pct, 3, '%')}")
    slip = row.get('entry_slippage_pct')
    if slip is not None and slip < -0.05:
        tags.append('bad_fill')
        evidence.append(f"Adverse entry slippage: {fmt_num(slip, 3, '%')}")
    if row.get('broker_close') and row.get('broker_exit_fill_price') is None:
        tags.append('exit_fill_unknown')
        evidence.append('Broker flat was verified but no Alpaca exit fill price was recovered')

    first_green = path.get('first_green_ts')
    first_red = path.get('first_red_ts')
    if first_green is None and first_red is not None:
        tags.append('never_green')
        evidence.append('Trade went red before ever going green')
    if row.get('mfe_pct') is not None and row.get('mfe_pct') >= 0.50 and row.get('pnl', 0) < 0:
        tags.append('profit_reversal')
        evidence.append(f"Reached MFE {fmt_num(row.get('mfe_pct'), 2, '%')} then closed red")
    if row.get('reason') == 'cond_time_stop':
        tags.append('exit_rule_candidate')
        f15 = row.get('fwd15')
        evidence.append(f"Conditional time stop; signed fwd15={fmt_num(f15, 2, '%')}")
    bracket = row.get('bracket_policy') or (row.get('entry_thesis') or {}).get('bracket_policy') or {}
    notes = bracket.get('notes') if isinstance(bracket, dict) else []
    note_text = ' '.join(str(n) for n in (notes or []))
    reason = str(row.get('reason') or '')
    if 'broker_disaster_stop_cap' in note_text:
        tags.append('broker_disaster_stop_cap')
        evidence.append(f"Broker disaster stop cap was active: notes={notes}")
        if reason in ('stop_loss', 'broker_stop', 'external_broker_exit') or 'stop' in reason:
            tags.append('broker_disaster_stop_cap_exit')
            evidence.append('Loss closed through a stop-like path while the broker disaster cap was active')
    dq = row.get('decision_quality') or {}
    if dq.get('label') in ('bad_execution_context', 'bad_entry_context', 'bad_exit_or_profit_protection'):
        tags.append(dq.get('label'))
        evidence.append(f"Decision-quality label: {dq.get('label')} tags={dq.get('tags')}")

    if not tags:
        tags.append('inconclusive_or_normal_variance')
        evidence.append('No single logged factor clearly explains the loss')

    primary = tags[0]
    if 'btc_conflict' in tags:
        primary = 'btc_conflict'
    elif 'profit_reversal' in tags:
        primary = 'profit_reversal'
    elif 'broker_disaster_stop_cap_exit' in tags:
        primary = 'broker_disaster_stop_cap_exit'
    elif 'never_green' in tags:
        primary = 'bad_entry_or_timing'
    elif 'exit_rule_candidate' in tags and len(tags) == 1:
        primary = 'exit_rule_candidate'
    return primary, tags, evidence


def build_loser_review(tape):
    losers = [r for r in tape if r['pnl'] < 0]
    rows = []
    for r in losers:
        primary, tags, evidence = loser_diagnosis(r)
        rows.append({
            'row': r,
            'primary': primary,
            'tags': tags,
            'archetypes': loser_archetypes(r, tags),
            'thesis_failure': thesis_failure_label(r, tags),
            'replay_snapshot': loser_replay_snapshot(r),
            'evidence': evidence,
            'decision_grade': decision_grade(r, tags),
            'attribution': loss_attribution(r, tags),
            'preventers': preventer_candidates(r, tags),
        })
    return rows


def decision_grade(row, tags):
    """Grade decision quality, not P&L outcome."""
    bad_entry = {
        'btc_conflict', 'btc_not_confirming', 'stock_lagged_btc',
        'stock_outperformed_btc', 'short_into_positive_60s_mom',
        'long_into_negative_60s_mom', 'bad_entry_context',
        'bad_entry_or_timing',
    }
    bad_exit = {
        'profit_reversal', 'exit_rule_candidate', 'bad_exit_or_profit_protection',
        'broker_disaster_stop_cap_exit',
    }
    bad_exec = {'wide_spread', 'bad_fill', 'exit_fill_unknown', 'bad_execution_context'}
    if any(t in tags for t in bad_entry) and any(t in tags for t in bad_exec):
        return {'grade': 'D', 'label': 'should_not_have_entered_or_executed_poorly'}
    if any(t in tags for t in bad_entry):
        return {'grade': 'C', 'label': 'avoidable_bad_entry'}
    if any(t in tags for t in bad_exec):
        return {'grade': 'C', 'label': 'avoidable_execution_issue'}
    if any(t in tags for t in bad_exit):
        return {'grade': 'B', 'label': 'exit_or_profit_protection_review'}
    if tags == ['inconclusive_or_normal_variance']:
        return {'grade': 'A', 'label': 'valid_loss_or_normal_variance'}
    return {'grade': 'B', 'label': 'minor_context_concern'}


def loss_attribution(row, tags):
    buckets = {
        'entry_quality': 0,
        'exit_quality': 0,
        'execution_quality': 0,
        'market_regime': 0,
        'broker_safety': 0,
    }
    entry_tags = {
        'stock_lagged_btc', 'stock_outperformed_btc',
        'short_into_positive_60s_mom', 'long_into_negative_60s_mom',
        'never_green', 'bad_entry_or_timing', 'bad_entry_context',
    }
    exit_tags = {
        'profit_reversal', 'exit_rule_candidate', 'bad_exit_or_profit_protection',
        'broker_disaster_stop_cap_exit',
    }
    exec_tags = {'wide_spread', 'bad_fill', 'exit_fill_unknown', 'bad_execution_context'}
    regime_tags = {'btc_conflict', 'btc_not_confirming'}
    safety_tags = {'broker_disaster_stop_cap', 'broker_disaster_stop_cap_exit'}
    for tag in tags:
        if tag in entry_tags:
            buckets['entry_quality'] += 1
        if tag in exit_tags:
            buckets['exit_quality'] += 1
        if tag in exec_tags:
            buckets['execution_quality'] += 1
        if tag in regime_tags:
            buckets['market_regime'] += 1
        if tag in safety_tags:
            buckets['broker_safety'] += 1
    if not any(buckets.values()):
        buckets['entry_quality'] = 1
    total = sum(buckets.values()) or 1
    return {k: round(v / total, 2) for k, v in buckets.items()}


def preventer_candidates(row, tags):
    out = []
    tag_to_rule = {
        'btc_conflict': ('block_btc_conflict', 'Block entries when BTC is directly against trade side'),
        'btc_not_confirming': ('require_btc_confirmation', 'Require BTC confirmation for this setup type'),
        'short_into_positive_60s_mom': ('block_short_positive_mom60', 'Block SHORTs while stock mom_60s is positive'),
        'long_into_negative_60s_mom': ('block_long_negative_mom60', 'Block LONGs while stock mom_60s is negative'),
        'wide_spread': ('block_wide_spread', 'Block entries above spread threshold'),
        'bad_fill': ('tighten_execution_slippage', 'Require cleaner entry fills or cancel delayed fills'),
        'profit_reversal': ('test_profit_protection', 'Test profit-protection after meaningful MFE'),
        'exit_rule_candidate': ('review_cond_time_stop', 'Review conditional time-stop behavior for this setup'),
        'stock_lagged_btc': ('block_stock_lagging_btc_long', 'Block LONGs when stock lags BTC-implied move'),
        'stock_outperformed_btc': ('block_stock_outperforming_btc_short', 'Block SHORTs when stock outperforms BTC-implied move'),
        'never_green': ('tighten_entry_timing', 'Tighten entry timing for trades that immediately go red'),
        'bad_entry_context': ('block_bad_entry_context', 'Block entries with repeated bad context tags'),
        'bad_execution_context': ('block_bad_execution_context', 'Block entries with poor execution context'),
        'bad_exit_or_profit_protection': ('test_profit_protection', 'Test profit-protection after meaningful MFE'),
        'broker_disaster_stop_cap_exit': (
            'review_broker_disaster_stop_cap',
            'Review whether the safety stop cap protected the account while preserving enough room for this setup',
        ),
    }
    for tag in tags:
        if tag in tag_to_rule:
            rule, desc = tag_to_rule[tag]
            if rule not in [x['rule'] for x in out]:
                out.append({'rule': rule, 'description': desc})
    if not out:
        out.append({'rule': 'no_change', 'description': 'No obvious simple filter would have prevented this without more evidence'})
    for item in out:
        item['actionable_pre_entry'] = rule_actionable_pre_entry(item['rule'])
    return out


def rule_actionable_pre_entry(rule):
    return rule not in {
        'test_profit_protection',
        'review_cond_time_stop',
        'tighten_execution_slippage',
        'review_broker_disaster_stop_cap',
    }


def aggregate_loser_themes(loser_review):
    counts = {}
    pnl = {}
    for item in loser_review:
        loss = abs(item['row'].get('pnl', 0) or 0)
        for tag in set(item['tags']):
            counts[tag] = counts.get(tag, 0) + 1
            pnl[tag] = pnl.get(tag, 0.0) + loss
    return sorted(counts.items(), key=lambda kv: (-kv[1], -pnl.get(kv[0], 0)))


def row_rule_hits(row):
    primary, tags, _ = loser_diagnosis(row) if row.get('pnl', 0) < 0 else (None, infer_tags(row), [])
    return {p['rule'] for p in preventer_candidates(row, tags)}


def infer_tags(row):
    primary, tags, _ = loser_diagnosis(dict(row, pnl=-0.01))
    return tags


def counterfactual_rules(tape):
    rules = {}
    for item in build_loser_review(tape):
        loss = abs(item['row'].get('pnl', 0) or 0)
        for p in item['preventers']:
            rule = p['rule']
            if rule == 'no_change':
                continue
            r = rules.setdefault(rule, {
                'rule': rule,
                'description': p['description'],
                'actionable_pre_entry': p.get('actionable_pre_entry'),
                'losers_avoided': 0,
                'loss_avoided': 0.0,
                'winners_blocked': 0,
                'winner_pnl_sacrificed': 0.0,
                'net_estimated_impact': 0.0,
                'evidence_trades': [],
            })
            r['losers_avoided'] += 1
            r['loss_avoided'] += loss
            r['evidence_trades'].append(f"{item['row']['time']} {item['row']['ticker']} {item['row']['side']}")

    winners = [r for r in tape if r.get('pnl', 0) > 0]
    for w in winners:
        for rule in row_rule_hits(w):
            if rule in rules:
                rules[rule]['winners_blocked'] += 1
                rules[rule]['winner_pnl_sacrificed'] += float(w.get('pnl') or 0)

    for r in rules.values():
        r['loss_avoided'] = round(r['loss_avoided'], 2)
        r['winner_pnl_sacrificed'] = round(r['winner_pnl_sacrificed'], 2)
        r['net_estimated_impact'] = round(r['loss_avoided'] - r['winner_pnl_sacrificed'], 2)
        r['confidence'] = confidence_for_rule(r)
    return sorted(rules.values(), key=lambda x: (-x['net_estimated_impact'], -x['loss_avoided']))


def confidence_for_rule(rule):
    if rule.get('losers_avoided', 0) >= 3 and rule.get('net_estimated_impact', 0) > 0:
        return 'medium'
    if rule.get('losers_avoided', 0) >= 2 and rule.get('net_estimated_impact', 0) > 0:
        return 'low-medium'
    return 'low'


def similar_past_trades(row, state, limit=5):
    if not state:
        return []
    setup = setup_type(row)
    f = row.get('forensics') or {}
    btc_regime = (f.get('btc') or {}).get('regime')
    out = []
    for t in reversed(state.get('trades', [])[:-1]):
        tf = t.get('forensics') or {}
        if t.get('ticker') != row.get('ticker') or t.get('side') != row.get('side'):
            continue
        if (tf.get('setup_type') or 'unknown') != setup:
            continue
        tb = (tf.get('btc') or {}).get('regime')
        score = 2 + (1 if tb == btc_regime else 0)
        out.append({
            'ticker': t.get('ticker'),
            'side': t.get('side'),
            'pnl': t.get('pnl'),
            'reason': t.get('reason'),
            'closed_day': datetime.fromtimestamp(t.get('closed_at', 0), CT).strftime('%Y-%m-%d'),
            'setup': setup,
            'btc_regime': tb,
            'similarity_score': score,
        })
        if len(out) >= limit:
            break
    return out


def data_quality_score(flags, tape, state, day_iso):
    score = 100
    issues = []
    if flags:
        score -= min(35, 10 * len(flags))
        issues.append(f'{len(flags)} health flag(s)')
    fwd_statuses = {}
    for r in tape:
        status = r.get('fwd_status')
        if status not in (None, 'ok'):
            fwd_statuses[status] = fwd_statuses.get(status, 0) + 1
    fwd_missing = sum(fwd_statuses.values())
    if fwd_missing:
        penalty = 8 if set(fwd_statuses) == {'fetch_error'} else min(20, 5 * fwd_missing)
        score -= penalty
        parts = ', '.join(f'{k}={v}' for k, v in sorted(fwd_statuses.items()))
        issues.append(f'{fwd_missing} trade forward-return issue(s): {parts}')
    losers = [r for r in tape if r.get('pnl', 0) < 0]
    missing_broker = sum(1 for r in losers if r.get('broker_close') and r.get('broker_exit_fill_price') is None)
    if missing_broker:
        score -= min(15, 5 * missing_broker)
        issues.append(f'{missing_broker} loser(s) missing broker exit fill')
    skipped = skipped_for_day(state or {}, day_iso)
    if not skipped:
        score -= 5
        issues.append('no skipped-signal corpus for comparison')
    label = 'high'
    if score < 70:
        label = 'low'
    elif score < 90:
        label = 'medium'
    return {'score': max(0, score), 'label': label, 'issues': issues}


def rolling_evidence(day_iso):
    payloads = prior_postmortem_payloads(day_iso, max_days=5)
    rules = {}
    tags = {}
    skipped_reasons = {}
    days = set()
    for p in payloads:
        d = p.get('date_iso')
        if d:
            days.add(d)
        for rule in p.get('counterfactual_rules', []) or []:
            r = rules.setdefault(rule.get('rule'), {
                'rule': rule.get('rule'),
                'days': set(),
                'losers_avoided': 0,
                'winners_blocked': 0,
                'net_estimated_impact': 0.0,
            })
            if d:
                r['days'].add(d)
            r['losers_avoided'] += int(rule.get('losers_avoided') or 0)
            r['winners_blocked'] += int(rule.get('winners_blocked') or 0)
            r['net_estimated_impact'] += float(rule.get('net_estimated_impact') or 0)
        for item in p.get('loser_decision_review', []) or []:
            for tag in item.get('tags', []) or []:
                t = tags.setdefault(tag, {'tag': tag, 'days': set(), 'count': 0})
                if d:
                    t['days'].add(d)
                t['count'] += 1
        for s in p.get('skipped_opportunity_summary', []) or []:
            reason = s.get('reason')
            row = skipped_reasons.setdefault(reason, {'reason': reason, 'days': set(), 'n': 0, 'would_work_5m': 0})
            if d:
                row['days'].add(d)
            row['n'] += int(s.get('n') or 0)
            row['would_work_5m'] += int(s.get('would_work_5m') or 0)
    for coll in (rules, tags, skipped_reasons):
        for v in coll.values():
            v['evidence_days'] = len(v.pop('days'))
            if 'net_estimated_impact' in v:
                v['net_estimated_impact'] = round(v['net_estimated_impact'], 2)
    return {
        'days': sorted(days),
        'rules': sorted(rules.values(), key=lambda x: (-x['evidence_days'], -x.get('net_estimated_impact', 0)))[:8],
        'loser_tags': sorted(tags.values(), key=lambda x: (-x['evidence_days'], -x['count']))[:8],
        'skipped_reasons': sorted(skipped_reasons.values(), key=lambda x: (-x['would_work_5m'], -x['evidence_days']))[:8],
    }


def daily_verdict(day_iso, flags, tape, state):
    dq = data_quality_score(flags, tape, state, day_iso)
    rules = counterfactual_rules(tape)
    strong = [r for r in rules if r['losers_avoided'] >= 2 and r['net_estimated_impact'] > 0]
    if flags:
        action = 'fix health/data issue before strategy changes'
    elif strong:
        action = f"backtest {strong[0]['rule']}"
    elif any(r.get('pnl', 0) < 0 for r in tape):
        action = 'monitor loser themes; no live rule change yet'
    elif not tape:
        action = 'no trades; collect data'
    else:
        action = 'no rule change'
    return {'action': action, 'data_quality': dq, 'top_rule': strong[0] if strong else None}


def clean_day_score(day_iso, flags, tape, state):
    dq = data_quality_score(flags, tape, state, day_iso)
    score = int(dq.get('score') or 0)
    labels = []
    op_reasons = {'manual_stop', 'session_end', 'external_broker_exit'}
    op_trades = [
        r for r in tape
        if str(r.get('reason') or '') in op_reasons
        or str(r.get('reason') or '').startswith('external_')
    ]
    if op_trades:
        score -= min(30, 8 * len(op_trades))
        labels.append('ops_contaminated')
    if flags:
        labels.append('health_flags')
    if len(tape) < 5:
        labels.append('low_sample')
    if any(r.get('fwd_status') not in (None, 'ok') for r in tape):
        labels.append('forward_data_issue')
    if not labels:
        labels.append('clean_strategy_day')
    score = max(0, min(100, score))
    if score >= 90 and labels == ['clean_strategy_day']:
        verdict = 'clean_strategy_day'
    elif score >= 70:
        verdict = 'usable_with_caveats'
    else:
        verdict = 'do_not_promote_rules_from_this_day'
    return {
        'score': score,
        'verdict': verdict,
        'labels': labels,
        'ops_trade_count': len(op_trades),
        'data_quality': dq,
    }


def entry_quality_summary(tape):
    rows = {}
    for r in tape:
        tier = entry_quality_tier_from_row(r)
        key = tier.get('tier') or 'unknown'
        row = rows.setdefault(key, {'tier': key, 'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0})
        pnl = float(r.get('pnl') or 0)
        row['trades'] += 1
        row['wins'] += 1 if pnl > 0 else 0
        row['losses'] += 1 if pnl < 0 else 0
        row['pnl'] += pnl
    out = []
    for row in rows.values():
        row['pnl'] = round(row['pnl'], 2)
        row['win_rate'] = round(row['wins'] / row['trades'] * 100, 1) if row['trades'] else None
        out.append(row)
    return sorted(out, key=lambda r: r['tier'])


def setup_type(row):
    f = row.get('forensics') or {}
    return f.get('setup_type') or 'unknown'


def aggregate_by_setup(tape):
    groups = {}
    for r in tape:
        key = setup_type(r)
        g = groups.setdefault(key, {'n': 0, 'w': 0, 'l': 0, 'pnl': 0.0,
                                    'gross_w': 0.0, 'gross_l': 0.0})
        pnl = float(r.get('pnl') or 0)
        g['n'] += 1
        g['pnl'] += pnl
        if pnl > 0:
            g['w'] += 1
            g['gross_w'] += pnl
        elif pnl < 0:
            g['l'] += 1
            g['gross_l'] += abs(pnl)
    out = []
    for key, g in groups.items():
        g['setup'] = key
        g['win_pct'] = (100 * g['w'] / g['n']) if g['n'] else 0
        g['profit_factor'] = (g['gross_w'] / g['gross_l']) if g['gross_l'] else None
        out.append(g)
    return sorted(out, key=lambda x: (-x['gross_l'], -abs(x['pnl'])))


def skipped_opportunity_summary(state, day_iso):
    skipped = skipped_with_bar_forwards(state or {}, day_iso)
    rows = []
    by_reason = {}
    for s in skipped:
        fwd5 = get_path(s, 'fwd.5m.signed_return_pct')
        fwd15 = get_path(s, 'fwd.15m.signed_return_pct')
        fwd30 = get_path(s, 'fwd.30m.signed_return_pct')
        best = max([v for v in (fwd5, fwd15, fwd30) if v is not None], default=None)
        reason = s.get('reason', '?')
        b = by_reason.setdefault(reason, {'n': 0, 'would_work_5m': 0,
                                          'avg5_values': [], 'avg15_values': [],
                                          'avg30_values': [], 'best_values': []})
        b['n'] += 1
        if fwd5 is not None:
            b['avg5_values'].append(float(fwd5))
            if fwd5 > 0:
                b['would_work_5m'] += 1
        if fwd15 is not None:
            b['avg15_values'].append(float(fwd15))
        if fwd30 is not None:
            b['avg30_values'].append(float(fwd30))
        if best is not None:
            b['best_values'].append(float(best))
        grade = no_trade_opportunity_grade(reason, fwd5, fwd15, fwd30)
        rows.append({
            'ticker': s.get('ticker'),
            'side': s.get('side'),
            'reason': reason,
            'score': s.get('score'),
            'conviction': s.get('conviction'),
            'fwd5': fwd5,
            'fwd15': fwd15,
            'fwd30': fwd30,
            'best': best,
            'opportunity_grade': grade,
        })
    summary = []
    for reason, b in by_reason.items():
        avg5 = sum(b['avg5_values']) / len(b['avg5_values']) if b['avg5_values'] else None
        avg15 = sum(b['avg15_values']) / len(b['avg15_values']) if b['avg15_values'] else None
        avg30 = sum(b['avg30_values']) / len(b['avg30_values']) if b['avg30_values'] else None
        best_avg = sum(b['best_values']) / len(b['best_values']) if b['best_values'] else None
        summary.append({
            'reason': reason,
            'n': b['n'],
            'would_work_5m': b['would_work_5m'],
            'avg5': avg5,
            'avg15': avg15,
            'avg30': avg30,
            'avg_best': best_avg,
            'grade_counts': dict(Counter(
                r.get('opportunity_grade') for r in rows
                if r.get('reason') == reason
            )),
        })
    summary.sort(key=lambda x: (-(x['avg5'] if x['avg5'] is not None else -999), -x['n']))
    rows.sort(key=lambda x: (-(x['fwd5'] if x['fwd5'] is not None else -999), x['reason']))
    return summary, rows


def no_trade_opportunity_grade(reason, fwd5, fwd15, fwd30):
    vals = [v for v in (fwd5, fwd15, fwd30) if v is not None]
    if not vals:
        return 'unclear'
    best = max(vals)
    worst = min(vals)
    if fwd5 is not None and fwd5 > 0.35:
        return 'missed_winner'
    if best <= 0:
        return 'avoided_loser'
    if fwd5 is not None and fwd5 > 0:
        return 'bad_skip'
    if worst < -0.25:
        return 'good_skip'
    return 'unclear'


def render_trader_review(day_iso, tape, state):
    out = []
    losers = [r for r in tape if r['pnl'] < 0]
    winners = [r for r in tape if r['pnl'] > 0]
    total = sum(r.get('pnl', 0) for r in tape)
    gross_w = sum(r.get('pnl', 0) for r in winners)
    gross_l = abs(sum(r.get('pnl', 0) for r in losers))
    pf = (gross_w / gross_l) if gross_l else None
    loser_review = build_loser_review(tape)
    themes = aggregate_loser_themes(loser_review)

    out.append('Trader review')
    out.append('-' * 78)
    if not tape:
        out.append('  No trades closed today. Review skipped-signal section only.')
        out.append('')
        return out
    out.append(f"  Result: {len(winners)}W / {len(losers)}L  P&L=${total:+.2f}  "
               f"gross W=${gross_w:+.2f} gross L=${gross_l:.2f}  "
               f"PF={fmt_plain(pf, 2)}")
    if loser_review:
        out.append('  Main loser themes:')
        for tag, n in themes[:5]:
            loss = sum(abs(i['row'].get('pnl', 0) or 0)
                       for i in loser_review if tag in i['tags'])
            out.append(f"    - {tag}: {n}/{len(loser_review)} losers, ${loss:.2f} gross loss")
        by_setup = aggregate_by_setup(tape)
        if by_setup:
            out.append('  Setup performance:')
            for g in by_setup[:5]:
                out.append(f"    - {g['setup']}: {g['w']}W/{g['l']}L "
                           f"P&L=${g['pnl']:+.2f} PF={fmt_plain(g['profit_factor'], 2)}")
    else:
        out.append('  No losing trades. Do not infer new filters from a clean day.')
    out.append('')
    return out


def render_decision_review(tape, state):
    out = []
    loser_review = build_loser_review(tape)
    out.append('Decision review - losing trades')
    out.append('-' * 78)
    if not loser_review:
        out.append('  No losing trades to grade.')
        out.append('')
        return out

    grade_counts = {}
    attribution = {'entry_quality': 0.0, 'exit_quality': 0.0,
                   'execution_quality': 0.0, 'market_regime': 0.0,
                   'broker_safety': 0.0}
    for item in loser_review:
        grade = item['decision_grade']['grade']
        grade_counts[grade] = grade_counts.get(grade, 0) + 1
        for k, v in item['attribution'].items():
            attribution[k] = attribution.get(k, 0.0) + v
    n = len(loser_review) or 1
    out.append('  Grade key: A=valid loss, B=minor concern/exit review, C=avoidable, D=should not have entered')
    out.append(f"  Grades: " + ', '.join(f"{k}={grade_counts[k]}" for k in sorted(grade_counts)))
    out.append('  Loss attribution:')
    for k, v in sorted(attribution.items(), key=lambda kv: -kv[1]):
        out.append(f"    - {k}: {v / n:.0%} average share")

    for item in loser_review:
        r = item['row']
        grade = item['decision_grade']
        preventers = ', '.join(p['rule'] for p in item['preventers'])
        out.append(f"  {r['time']} {r['ticker']} {r['side']} pnl=${r['pnl']:+.2f}: "
                   f"grade={grade['grade']} {grade['label']} preventers={preventers}")
        out.append(f"    archetypes: {', '.join(item.get('archetypes') or [])}")
        tf = item.get('thesis_failure') or {}
        snap = item.get('replay_snapshot') or {}
        cp = snap.get('checkpoints') or {}
        cp_txt = ', '.join(
            f"{k}={fmt_num(v.get('signed_return_pct'), 3, '%')}"
            for k, v in cp.items()
        ) or 'n/a'
        out.append(f"    thesis failed: {tf.get('label')} ({tf.get('confidence')}); replay: {cp_txt}")
        sims = similar_past_trades(r, state, limit=3)
        if sims:
            sim_txt = '; '.join(f"{s['closed_day']} {s['pnl']:+.2f} {s['reason']}" for s in sims)
            out.append(f"    similar past: {sim_txt}")
    out.append('')
    return out


def render_daily_verdict(day_iso, flags, tape, state):
    verdict = daily_verdict(day_iso, flags, tape, state)
    dq = verdict['data_quality']
    out = []
    out.append('Daily verdict')
    out.append('-' * 78)
    out.append(f"  Verdict: {verdict['action']}")
    out.append(f"  Data quality: {dq['label']} ({dq['score']}/100)")
    if dq['issues']:
        out.append('  Data caveats: ' + '; '.join(dq['issues']))
    if verdict.get('top_rule'):
        r = verdict['top_rule']
        out.append(f"  Top today-only candidate: {r['rule']} "
                   f"net=${r['net_estimated_impact']:+.2f} "
                   f"confidence={r['confidence']} "
                   f"pre_entry_actionable={r['actionable_pre_entry']}")
    out.append('')
    return out


def render_executive_verdict(day_iso, flags, tape, state):
    out = []
    clean = clean_day_score(day_iso, flags, tape, state)
    loser_review = build_loser_review(tape)
    archetypes = Counter(a for item in loser_review for a in item.get('archetypes', []))
    total = sum(float(r.get('pnl') or 0) for r in tape)
    out.append('Executive verdict')
    out.append('-' * 78)
    out.append(f"  Clean-day score: {clean['score']}/100 ({clean['verdict']}; {', '.join(clean['labels'])})")
    out.append(f"  Primary damage source: {archetypes.most_common(1)[0][0] if archetypes else 'none'}")
    out.append(f"  Net result: {len(tape)} trade(s), P&L=${total:+.2f}")
    tier_rows = entry_quality_summary(tape)
    if tier_rows:
        tier_txt = ', '.join(
            f"{r['tier']}={r['trades']} trades/${r['pnl']:+.2f}/{r['win_rate']}% WR"
            for r in tier_rows
        )
        out.append(f"  Entry quality tiers: {tier_txt}")
    if clean['verdict'] == 'do_not_promote_rules_from_this_day':
        out.append('  Change posture: collect evidence; do not promote strategy rules from this day alone.')
    else:
        out.append('  Change posture: review same-day candidates, then require rolling evidence/backtest before promotion.')
    out.append('')
    return out


def render_entry_quality_review(tape):
    out = []
    out.append('Entry quality tiers')
    out.append('-' * 78)
    rows = entry_quality_summary(tape)
    if not rows:
        out.append('  No closed trades to tier yet.')
        out.append('')
        return out
    for row in rows:
        out.append(f"  Tier {row['tier']}: {row['trades']} trade(s), "
                   f"{row['wins']}W/{row['losses']}L, P&L=${row['pnl']:+.2f}, "
                   f"WR={row['win_rate']}%")
    weak = [r for r in tape if entry_quality_tier_from_row(r).get('tier') == 'C']
    if weak:
        out.append('  Tier C examples to inspect:')
        for r in weak[:5]:
            tier = entry_quality_tier_from_row(r)
            out.append(f"    - {r['time']} {r['ticker']} {r['side']} pnl=${r['pnl']:+.2f} "
                       f"score={tier.get('score')} tags={','.join(tier.get('tags') or [])}")
    out.append('')
    return out


def render_thesis_failure_review(tape):
    out = []
    loser_review = build_loser_review(tape)
    counts = Counter((i.get('thesis_failure') or {}).get('label') for i in loser_review)
    out.append('Thesis failure review')
    out.append('-' * 78)
    if not loser_review:
        out.append('  No losing theses to classify.')
        out.append('')
        return out
    for label, count in counts.most_common():
        loss = sum(abs((i.get('row') or {}).get('pnl') or 0) for i in loser_review
                   if (i.get('thesis_failure') or {}).get('label') == label)
        out.append(f'  - {label}: {count} loser(s), gross loss=${loss:.2f}')
    out.append('')
    return out


def render_trade_thesis_timeline_review(tape):
    out = []
    timelines = build_trade_thesis_timelines(tape)
    counts = Counter(t.get('verdict') for t in timelines)
    out.append('Trade thesis timelines')
    out.append('-' * 78)
    if not timelines:
        out.append('  No closed trades to timeline yet.')
        out.append('')
        return out
    out.append('  Verdict counts: ' + ', '.join(f'{k}={v}' for k, v in counts.most_common()))
    flagged = [
        t for t in timelines
        if t.get('verdict') in ('lucky_or_late_reversal_winner', 'confirmed_then_failed', 'thesis_invalidated')
    ]
    for t in flagged[:8]:
        cp = ', '.join(
            f"{c['checkpoint']}={fmt_num(c.get('signed_return_pct'), 3, '%')}:{c.get('status')}"
            for c in t.get('checkpoints') or []
            if c.get('signed_return_pct') is not None
        ) or 'n/a'
        out.append(f"  - {t.get('time')} {t.get('ticker')} {t.get('side')} "
                   f"pnl=${t.get('pnl'):+.2f} verdict={t.get('verdict')} path={cp}")
    out.append('')
    return out


def render_winner_quality_review(tape):
    review = winner_quality_review(tape)
    out = []
    out.append('Winner quality review')
    out.append('-' * 78)
    if not review.get('rows'):
        out.append('  No winners to classify.')
        out.append('')
        return out
    out.append('  Winner labels: ' + ', '.join(f'{k}={v}' for k, v in Counter(review.get('counts') or {}).items()))
    non_clean = [r for r in review.get('rows') or [] if (r.get('quality') or {}).get('label') != 'clean_winner']
    if non_clean:
        out.append('  Winners to avoid over-crediting:')
        for r in non_clean[:8]:
            q = r.get('quality') or {}
            out.append(f"    - {r.get('time')} {r.get('ticker')} {r.get('side')} "
                       f"pnl=${r.get('pnl'):+.2f} label={q.get('label')} tags={','.join(q.get('tags') or [])}")
    else:
        out.append('  All logged winners were clean by current labels.')
    out.append('')
    return out


def render_hypothesis_lifecycle(day_iso, tape):
    hp = hypothesis_preview(day_iso, tape)
    out = []
    out.append('Hypothesis lifecycle dashboard')
    out.append('-' * 78)
    counts = {
        'added': len(hp.get('added') or []),
        'updated': len(hp.get('updated') or []),
        'active': len(hp.get('active') or []),
        'killed': len(hp.get('retired') or []),
    }
    out.append('  ' + ', '.join(f'{k}={v}' for k, v in counts.items()))
    active = sorted(hp.get('active') or [], key=lambda h: (-int(h.get('evidence_days') or 0), h.get('id', '')))
    for h in active[:6]:
        state = 'stale' if h.get('stale') else 'watching'
        out.append(f"    - [{h.get('id')}] {state}: {h.get('monitor')}")
    if not active:
        out.append('    - No active hypotheses.')
    out.append('')
    return out


def _learning_confidence(evidence):
    evidence = evidence or {}
    days = int(evidence.get('days_seen') or evidence.get('evidence_days') or 0)
    affected = int(evidence.get('affected_trades') or evidence.get('trades') or evidence.get('matched') or evidence.get('blocked') or 0)
    delta = float(evidence.get('estimated_delta_vs_actual') or evidence.get('pnl') or 0)
    winner_damage = abs(float(evidence.get('winner_damage') or evidence.get('hurt_winner_delta') or 0))
    if days >= 3 and affected >= 12 and delta > 0 and winner_damage <= 100:
        return 'high'
    if days >= 2 and affected >= 8 and delta > 0:
        return 'medium'
    return 'low'


def _market_regime_label(day_iso):
    try:
        path = os.path.join(OUT_DIR, f'market_regime_day_{day_iso}.json')
        with open(path, 'r', encoding='utf-8') as f:
            payload = json.load(f)
        parts = []
        for key in ('btc_regime', 'market_regime', 'session_regime', 'dominant_regime'):
            if payload.get(key):
                parts.append(f'{key}={payload.get(key)}')
        if parts:
            return ', '.join(parts)
        if payload.get('deduction'):
            return payload.get('deduction')
    except Exception:
        pass
    return 'regime not classified yet'


def build_learning_lifecycle(day_iso, tape, state=None):
    loser_review = build_loser_review(tape)
    observations = []
    themes = aggregate_loser_themes(loser_review)
    for tag, count in themes[:5]:
        observations.append({
            'type': 'observation',
            'text': f'{tag}: {count} losing trade(s) today',
            'confidence': 'low',
            'promotion_status': 'observation_only',
        })
    rules = counterfactual_rules(tape)
    for rule in rules[:5]:
        observations.append({
            'type': 'observation',
            'text': (
                f"{rule.get('rule')}: today-only counterfactual net="
                f"${rule.get('net_estimated_impact', 0):+.2f}; "
                f"losers_avoided={rule.get('losers_avoided')} "
                f"winners_blocked={rule.get('winners_blocked')}"
            ),
            'confidence': rule.get('confidence') or 'low',
            'promotion_status': 'not_promoted_single_day',
        })

    hp = hypothesis_preview(day_iso, tape)
    hypotheses = []
    for h in sorted(hp.get('active') or [], key=lambda row: (-int(row.get('evidence_days') or 0), row.get('id', '')))[:10]:
        hypotheses.append({
            'type': 'hypothesis',
            'id': h.get('id'),
            'text': h.get('monitor'),
            'evidence_days': h.get('evidence_days'),
            'first_seen': h.get('created'),
            'last_seen': h.get('last_seen'),
            'confidence': 'medium' if int(h.get('evidence_days') or 0) >= 2 else 'low',
            'promotion_status': 'tracking' if not h.get('stale') else 'stale_tracking',
        })

    candidates = []
    approved = []
    try:
        from promotion_queue import update_queue
        queue = update_queue(day_iso)
        for item in (queue.get('items') or {}).values():
            ev = item.get('evidence') or {}
            row = {
                'type': 'candidate_change',
                'key': item.get('key'),
                'kind': item.get('kind'),
                'name': item.get('name'),
                'lifecycle': item.get('lifecycle'),
                'status': item.get('status'),
                'seen_days': len(item.get('seen_days') or []),
                'confidence': _learning_confidence(ev),
                'affected_trades': ev.get('affected_trades'),
                'estimated_delta_vs_actual': ev.get('estimated_delta_vs_actual') or ev.get('pnl'),
                'winner_damage': ev.get('winner_damage') or ev.get('hurt_winner_delta'),
                'false_positive_cost': ev.get('winner_damage') or ev.get('hurt_winner_delta') or 0,
                'promotion_status': (
                    'eligible_for_human_review'
                    if item.get('status') == 'eligible_for_human_review'
                    else 'collect_more_or_reject'
                ),
            }
            if item.get('promoted_manually'):
                approved.append({**row, 'type': 'approved_change', 'promotion_status': 'approved_or_implemented'})
            else:
                candidates.append(row)
    except Exception as e:
        candidates.append({'type': 'candidate_change', 'error': str(e), 'promotion_status': 'queue_unavailable'})

    followups = []
    try:
        path = os.path.join(OUT_DIR, 'change_impact_ledger.json')
        with open(path, 'r', encoding='utf-8') as f:
            ledger = json.load(f)
        for key, item in (ledger.get('items') or {}).items():
            manual = item.get('manual_status') or item.get('status')
            if manual in ('approved', 'implemented', 'promoted', 'approved_or_implemented') or item.get('promoted_manually'):
                obs = item.get('observations') or []
                recent = obs[-5:]
                followups.append({
                    'key': key,
                    'manual_status': manual,
                    'first_seen': item.get('first_seen'),
                    'recent_observations': recent,
                    'review_question': 'Did the approved change reduce losses without blocking too many winners?',
                })
    except Exception:
        pass

    return {
        'day': day_iso,
        'market_regime': _market_regime_label(day_iso),
        'observations': observations[:12],
        'hypotheses': hypotheses,
        'candidate_changes': sorted(
            candidates,
            key=lambda row: (
                row.get('promotion_status') != 'eligible_for_human_review',
                row.get('lifecycle') != 'promising',
                -(float(row.get('estimated_delta_vs_actual') or 0)),
            ),
        )[:12],
        'approved_changes': approved[:12],
        'change_impact_followups': followups[:12],
        'why_not_changing': [
            'Single-day counterfactuals stay observations, not action items.',
            'Candidates need multi-day evidence, acceptable winner damage, and clean operations.',
            'Human approval is required before any live config/code change.',
        ],
    }


def render_learning_lifecycle_referee(day_iso, tape, state=None):
    lifecycle = build_learning_lifecycle(day_iso, tape, state=state)
    out = ['Learning lifecycle referee', '-' * 78]
    out.append(f"  Market/regime context: {lifecycle.get('market_regime')}")
    out.append('  Lifecycle: observation -> hypothesis -> candidate change -> approved/implemented.')
    out.append('  No automatic rule changes are made.')
    out.append('')

    out.append('  Observations (today only; not actionable yet):')
    for row in lifecycle.get('observations') or []:
        out.append(f"    - [{row.get('confidence')}] {row.get('text')}")
    if not lifecycle.get('observations'):
        out.append('    - No same-day observations requiring follow-up.')

    out.append('  Hypotheses being tracked:')
    for row in (lifecycle.get('hypotheses') or [])[:6]:
        out.append(
            f"    - [{row.get('id')}] confidence={row.get('confidence')} "
            f"days={row.get('evidence_days')}: {row.get('text')}"
        )
    if not lifecycle.get('hypotheses'):
        out.append('    - No active hypotheses yet.')

    out.append('  Candidate changes:')
    candidates = lifecycle.get('candidate_changes') or []
    for row in candidates[:8]:
        out.append(
            f"    - {row.get('key')}: lifecycle={row.get('lifecycle')} "
            f"status={row.get('status')} confidence={row.get('confidence')} "
            f"days={row.get('seen_days')} affected={row.get('affected_trades')} "
            f"delta=${float(row.get('estimated_delta_vs_actual') or 0):+.2f} "
            f"false_positive_cost=${float(row.get('false_positive_cost') or 0):+.2f}"
        )
    if not candidates:
        out.append('    - No candidate changes in the promotion queue.')

    out.append('  Approved/implemented change follow-up:')
    followups = lifecycle.get('change_impact_followups') or []
    if followups:
        for row in followups[:5]:
            out.append(
                f"    - {row.get('key')}: status={row.get('manual_status')} "
                f"recent_days={len(row.get('recent_observations') or [])}; "
                f"{row.get('review_question')}"
            )
    else:
        out.append('    - No manually approved/promoted changes currently require impact follow-up.')

    out.append('  Why we are not changing automatically:')
    for line in lifecycle.get('why_not_changing') or []:
        out.append(f'    - {line}')
    out.append('')
    return out


def render_rolling_evidence(day_iso):
    roll = rolling_evidence(day_iso)
    out = []
    out.append('Rolling evidence - prior 5 report days')
    out.append('-' * 78)
    if not roll['days']:
        out.append('  No prior postmortem corpus available yet.')
        out.append('')
        return out
    out.append(f"  prior days included: {', '.join(roll['days'])}")
    if roll['rules']:
        out.append('  Repeated rule candidates:')
        for r in roll['rules'][:5]:
            out.append(f"    - {r['rule']}: {r['evidence_days']} day(s), "
                       f"losers avoided={r['losers_avoided']}, winners blocked={r['winners_blocked']}, "
                       f"net=${r['net_estimated_impact']:+.2f}")
    if roll['loser_tags']:
        out.append('  Repeated loser tags:')
        for t in roll['loser_tags'][:5]:
            out.append(f"    - {t['tag']}: {t['count']} occurrence(s) across {t['evidence_days']} day(s)")
    if roll['skipped_reasons']:
        out.append('  Repeated positive skipped-signal reasons:')
        for s in roll['skipped_reasons'][:5]:
            out.append(f"    - {s['reason']}: {s['would_work_5m']} positive +5m skips "
                       f"across {s['evidence_days']} day(s)")
    out.append('')
    return out


def render_counterfactual_review(tape):
    out = []
    rules = counterfactual_rules(tape)
    out.append('Counterfactual rule review')
    out.append('-' * 78)
    if not rules:
        out.append('  No simple rule candidate had enough same-day evidence.')
        out.append('')
        return out
    out.append('  Rule candidates estimate what would have happened today only.')
    out.append('  rule | actionability | confidence | losers avoided | winners sacrificed | net')
    for r in rules[:8]:
        if (r.get('winner_pnl_sacrificed') or 0) > (r.get('loss_avoided') or 0):
            out.append(f"  ! OVERFIT WARNING: {r['rule']} sacrifices more winner P&L than loser damage avoided.")
        out.append(f"  - {r['rule']} [pre_entry={r['actionable_pre_entry']} confidence={r['confidence']}]: "
                   f"{r['losers_avoided']} loser(s) avoided "
                   f"(+${r['loss_avoided']:.2f}), {r['winners_blocked']} winner(s) blocked "
                   f"(-${r['winner_pnl_sacrificed']:.2f}), net=${r['net_estimated_impact']:+.2f}")
        out.append(f"    why: {r['description']}")
    out.append('')
    return out


def render_missed_vs_taken(tape, state, day_iso):
    out = []
    losers = [r for r in tape if r.get('pnl', 0) < 0]
    _, opportunities = skipped_opportunity_summary(state, day_iso)
    good_skips = [o for o in opportunities if o.get('fwd5') is not None and o.get('fwd5') > 0]
    out.append('Missed winners vs taken losers')
    out.append('-' * 78)
    if not losers and not good_skips:
        out.append('  No taken losers or positive skipped signals to compare.')
        out.append('')
        return out
    out.append(f"  taken losers: {len(losers)}")
    for r in losers[:5]:
        out.append(f"    - took {r['time']} {r['ticker']} {r['side']} pnl=${r['pnl']:+.2f} "
                   f"setup={setup_type(r)} score={r.get('score')}")
    out.append(f"  skipped signals positive at +5m: {len(good_skips)}")
    for s in good_skips[:5]:
        out.append(f"    - skipped {s['ticker']} {s['side']} reason={s['reason']} "
                   f"score={s['score']} fwd5={fmt_num(s['fwd5'], 2, '%')} "
                   f"grade={s.get('opportunity_grade')}")
    out.append('  Review question: are we accepting lower-quality setups than the filters are rejecting?')
    out.append('')
    return out


def render_loser_triage(tape):
    out = []
    loser_review = build_loser_review(tape)
    out.append('Loser triage')
    out.append('-' * 78)
    if not loser_review:
        out.append('  No losers to triage.')
        out.append('')
        return out
    for item in loser_review:
        r = item['row']
        f = r.get('forensics') or {}
        btc = f.get('btc') or {}
        rel = f.get('relative_strength') or {}
        loc = f.get('location') or {}
        setup = f.get('setup_type') or 'unknown'
        out.append(f"  {r['time']} {r['ticker']} {r['side']} {r['conv']} "
                   f"pnl=${r['pnl']:+.2f} reason={r['reason']} primary={item['primary']}")
        out.append(f"    setup={setup} score={r.get('score')} "
                   f"BTC={btc.get('regime', 'n/a')} conflict={btc.get('conflict')} "
                   f"rel60={fmt_num(rel.get('stock_minus_btc_implied_60s'), 3, '%')} "
                   f"vwap_sigma={fmt_num(loc.get('vwap_dist_sigma'), 2)} "
                   f"MFE={fmt_num(r.get('mfe_pct'), 2, '%')} MAE={fmt_num(r.get('mae_pct'), 2, '%')}")
        if r.get('decision_quality'):
            out.append(f"    decision_quality={r['decision_quality'].get('label')} "
                       f"tags={r['decision_quality'].get('tags')}")
        out.append('    why: ' + '; '.join(item['evidence'][:4]))
    out.append('')
    return out


def render_skipped_review(state, day_iso):
    out = []
    skipped = skipped_for_day(state or {}, day_iso)
    opportunity_summary, opportunities = skipped_opportunity_summary(state, day_iso)
    out.append('Skipped-signal audit')
    out.append('-' * 78)
    if not skipped:
        out.append('  No skipped signals logged for this date.')
        out.append('')
        return out
    by_reason = {}
    for s in skipped:
        by_reason.setdefault(s.get('reason', '?'), []).append(s)
    out.append(f'  skipped signals logged: {len(skipped)}')
    for reason, items in sorted(by_reason.items(), key=lambda kv: -len(kv[1]))[:8]:
        f5 = [get_path(x, 'fwd.5m.signed_return_pct') for x in items
              if get_path(x, 'fwd.5m.signed_return_pct') is not None]
        avg5 = sum(f5) / len(f5) if f5 else None
        out.append(f"    - {reason}: {len(items)} skips, avg signed fwd5={fmt_num(avg5, 2, '%')}")
    if opportunity_summary:
        out.append('  Best skipped-opportunity reasons:')
        for row in opportunity_summary[:5]:
            out.append(f"    - {row['reason']}: {row['n']} skips, "
                       f"{row['would_work_5m']} positive at +5m, "
                       f"avg5={fmt_num(row['avg5'], 2, '%')} "
                       f"avg15={fmt_num(row['avg15'], 2, '%')} "
                       f"avg30={fmt_num(row['avg30'], 2, '%')} "
                       f"avg best diagnostic={fmt_num(row['avg_best'], 2, '%')}")
    if opportunities:
        out.append('  Top skipped opportunities:')
        for row in opportunities[:5]:
            out.append(f"    - {row['ticker']} {row['side']} reason={row['reason']} "
                       f"score={row['score']} conv={row['conviction']} "
                       f"fwd5={fmt_num(row['fwd5'], 2, '%')} "
                       f"fwd15={fmt_num(row['fwd15'], 2, '%')} "
                       f"fwd30={fmt_num(row['fwd30'], 2, '%')}")
    out.append('  Use this to validate filters. Positive forward return means the skipped trade would have worked.')
    out.append('')
    return out


def render_shadow_decision_review(day_iso):
    rows = shadow_decisions_with_bar_forwards(day_iso)
    out = []
    out.append('Shadow decision ledger')
    out.append('-' * 78)
    if not rows:
        out.append('  No shadow decision rows logged for this date yet.')
        out.append('')
        return out
    by_decision = {}
    by_reason = {}
    for r in rows:
        decision = r.get('decision', 'unknown')
        reason = r.get('reason', 'unknown')
        by_decision[decision] = by_decision.get(decision, 0) + 1
        by_reason[reason] = by_reason.get(reason, 0) + 1
    out.append(f'  rows logged: {len(rows)}')
    out.append('  decisions: ' + ', '.join(f'{k}={v}' for k, v in sorted(by_decision.items())))
    top = sorted(by_reason.items(), key=lambda kv: -kv[1])[:8]
    if top:
        out.append('  top reasons:')
        for reason, n in top:
            out.append(f'    - {reason}: {n}')
    rejected = [r for r in rows if r.get('decision') == 'skipped']
    entered = [r for r in rows if r.get('decision') == 'entered']
    for label, items in (('entered', entered), ('skipped', rejected)):
        f5 = [get_path(r, 'fwd.5m.signed_return_pct') for r in items
              if get_path(r, 'fwd.5m.signed_return_pct') is not None]
        f10 = [get_path(r, 'fwd.10m.signed_return_pct') for r in items
               if get_path(r, 'fwd.10m.signed_return_pct') is not None]
        if f5 or f10:
            out.append(f"  {label} fixed-horizon edge: "
                       f"avg5={fmt_num(sum(f5)/len(f5) if f5 else None, 2, '%')} "
                       f"avg10={fmt_num(sum(f10)/len(f10) if f10 else None, 2, '%')}")
    out.append('  This is the replay corpus: every entered or rejected setup gets fixed-horizon targets.')
    out.append('')
    return out


def render_shadow_exit_review(day_iso):
    rows = shadow_exits_for_day(day_iso)
    out = []
    out.append('Shadow exit candidates')
    out.append('-' * 78)
    if not rows:
        out.append('  No shadow exit rows logged for this date yet.')
        out.append('')
        return out
    by_policy = {}
    by_reason = {}
    for r in rows:
        by_policy[r.get('policy', 'unknown')] = by_policy.get(r.get('policy', 'unknown'), 0) + 1
        by_reason[r.get('reason', 'unknown')] = by_reason.get(r.get('reason', 'unknown'), 0) + 1
    out.append(f'  rows logged: {len(rows)}')
    out.append('  policies: ' + ', '.join(f'{k}={v}' for k, v in sorted(by_policy.items())))
    top = sorted(by_reason.items(), key=lambda kv: -kv[1])[:8]
    if top:
        out.append('  top shadow exit reasons:')
        for reason, n in top:
            out.append(f'    - {reason}: {n}')
    out.append('  These are shadow-only would-close events; compare against actual exits before promotion.')
    out.append('')
    return out


def render_near_signal_review(day_iso):
    rows = near_signals_for_day(day_iso)
    out = []
    out.append('Near-signal engine rejects')
    out.append('-' * 78)
    if not rows:
        out.append('  No near-signal rejects logged for this date yet.')
        out.append('')
        return out
    by_reason = {}
    by_setup = {}
    for r in rows:
        by_reason[r.get('reason', 'unknown')] = by_reason.get(r.get('reason', 'unknown'), 0) + 1
        by_setup[r.get('setup_type', 'unknown')] = by_setup.get(r.get('setup_type', 'unknown'), 0) + 1
    out.append(f'  rows logged: {len(rows)}')
    out.append('  top reject reasons:')
    for reason, n in sorted(by_reason.items(), key=lambda kv: -kv[1])[:8]:
        out.append(f'    - {reason}: {n}')
    out.append('  top rejected setup types:')
    for setup, n in sorted(by_setup.items(), key=lambda kv: -kv[1])[:5]:
        out.append(f'    - {setup}: {n}')
    out.append('  Use this to tell whether the scalp engine is too strict before promoting looser rules.')
    out.append('')
    return out


def render_engine_scoreboard(day_iso):
    out = []
    out.append('Engine variant scoreboard')
    out.append('-' * 78)
    try:
        from engine_scoreboard import build_scoreboard
        board = build_scoreboard(day_iso)
    except Exception as e:
        out.append(f'  Engine scoreboard unavailable: {e}')
        out.append('')
        return out
    if board.get('summary'):
        for line in board['summary']:
            out.append(f'  - {line}')
    cf = board.get('counterfactual') or {}
    if cf.get('variant_scoreboard'):
        out.append('  Snapshot variants:')
        for row in cf['variant_scoreboard'][:5]:
            out.append(f"    - {row['variant']}: {row['trades']} trades, "
                       f"{row['wins']}W/{row['losses']}L, pnl=${row['pnl']:+.2f}")
    if (cf.get('actual') or {}).get('trades') and cf.get('exit_policy_scoreboard'):
        out.append('  Exit policies:')
        for row in cf['exit_policy_scoreboard'][:4]:
            out.append(f"    - {row['policy']}: triggered={row['triggered']}, "
                       f"est_pnl=${row['estimated_pnl']:+.2f}, "
                       f"delta=${row['estimated_delta_vs_actual']:+.2f}")
    rp = board.get('replay') or {}
    if rp.get('variant_scoreboard'):
        out.append('  Tick replay variants:')
        for row in rp['variant_scoreboard'][:5]:
            out.append(f"    - {row['variant']}: signals={row['signals']}, "
                       f"win={row['win_rate']}%, avg_end={row['avg_end_return_pct']}%")
    val = board.get('validation') or {}
    diag = val.get('loser_diagnostics') or {}
    if diag.get('tags'):
        out.append('  Losing-trade tags to track:')
        for tag, n in sorted(diag['tags'].items(), key=lambda kv: -kv[1])[:8]:
            out.append(f'    - {tag}: {n}')
    if val.get('gate_scoreboard'):
        out.append('  Decision gate variants (same-day, diagnostic only):')
        for row in val['gate_scoreboard'][:5]:
            out.append(f"    - {row['variant']}: kept={row['kept']} blocked={row['blocked']} "
                       f"kept_pnl=${row['kept_pnl']:+.2f} "
                       f"delta=${row['estimated_delta_vs_actual']:+.2f}")
    rolling = board.get('rolling_validation') or {}
    if rolling.get('gate_scoreboard'):
        out.append('  Rolling gate variants (promotion evidence):')
        for row in rolling['gate_scoreboard'][:5]:
            out.append(f"    - {row['variant']}: +days={row['days_positive']}/{row['days_seen']} "
                       f"blocked={row['blocked']} "
                       f"delta=${row['estimated_delta_vs_actual']:+.2f}")
    er = board.get('exit_policy_replay') or {}
    if er.get('scoreboard'):
        out.append(f"  Tick-capture exit replay: matched={er.get('matched_trades')}/{er.get('trades')} "
                   f"coverage={er.get('coverage_pct')}%")
        for row in er['scoreboard'][:5]:
            out.append(f"    - {row['policy']}: triggered={row['triggered']} "
                       f"est_pnl=${row['estimated_pnl']:+.2f} "
                       f"delta=${row['estimated_delta_vs_actual']:+.2f} "
                       f"hurt_winners={row.get('hurt_winners', 0)} "
                       f"saved_losers={row.get('improved_losers', 0)}")
    rer = board.get('rolling_exit_policy_replay') or {}
    if rer.get('scoreboard'):
        out.append('  Rolling exit-policy replay (promotion evidence):')
        for row in rer['scoreboard'][:5]:
            out.append(f"    - {row['policy']}: +days={row['days_positive']}/{row['days_seen']} "
                       f"matched={row['matched']} triggered={row['triggered']} "
                       f"delta=${row['estimated_delta_vs_actual']:+.2f} "
                       f"hurt_winners=${row.get('hurt_winner_delta', 0):+.2f}")
    ecand = board.get('exit_policy_candidate_config') or {}
    if ecand.get('candidates'):
        out.append('  Exit-policy candidates ready for human review:')
        for row in ecand['candidates'][:5]:
            ev = row.get('evidence') or {}
            out.append(f"    - {row.get('policy')}: +days={ev.get('days_positive')}/{ev.get('days_seen')} "
                       f"delta=${ev.get('estimated_delta_vs_actual', 0):+.2f}; staged={row.get('staged_config')}")
    cand = board.get('engine_candidate_config') or {}
    if cand.get('candidates'):
        out.append('  Engine gate candidates ready for human review:')
        for row in cand['candidates'][:5]:
            ev = row.get('evidence') or {}
            out.append(f"    - {row.get('variant')}: +days={ev.get('days_positive')}/{ev.get('days_seen')} "
                       f"blocked={ev.get('blocked')} delta=${ev.get('estimated_delta_vs_actual'):+.2f}")
    elif cand:
        out.append('  No engine gate candidate cleared the multi-day promotion threshold.')
    if (not board.get('summary') and not cf.get('variant_scoreboard')
            and not rp.get('variant_scoreboard') and not val.get('gate_scoreboard')):
        out.append('  Not enough replay/counterfactual data yet.')
    out.append('')
    return out


def render_candidate_config(day_iso):
    out = []
    out.append('Staged candidate config')
    out.append('-' * 78)
    try:
        from elite_control import build_candidate_config
        payload = build_candidate_config(day_iso)
    except Exception as e:
        out.append(f'  Candidate config unavailable: {e}')
        out.append('')
        return out
    changes = payload.get('changes') or []
    out.append('  These are staged only; live config is unchanged.')
    if not changes:
        out.append('  No candidate config changes cleared the evidence gate.')
        out.append('')
        return out
    for ch in changes[:8]:
        typ = ch.get('type')
        if typ == 'exit_policy_candidate':
            ev = ch.get('evidence') or {}
            out.append(f"  - Exit policy candidate: {ch.get('candidate')} "
                       f"delta=${ev.get('estimated_delta_vs_actual', 0):+.2f} "
                       f"triggered={ev.get('triggered')}")
        elif typ == 'setup_size_multiplier':
            ev = ch.get('evidence') or {}
            out.append(f"  - Setup size candidate: {ch.get('setup')} "
                       f"{ch.get('old')} -> {ch.get('new')} "
                       f"trades={ev.get('trades')} pnl=${ev.get('pnl'):+.2f}")
        else:
            out.append(f"  - {typ}: {ch}")
    out.append('  Promote only after review or repeated evidence; this section is a decision aid.')
    out.append('')
    return out


def render_ev_table(day_iso, tape):
    out = []
    out.append('Setup EV table')
    out.append('-' * 78)
    try:
        from ev_analytics import build_ev_table
        table = build_ev_table(day_iso, current_tape=tape, lookback_days=10)
    except Exception as e:
        out.append(f'  EV table unavailable: {e}')
        out.append('')
        return out
    summary = table.get('summary') or {}
    out.append('  Passive analytics only; live behavior is unchanged.')
    out.append(f"  Buckets={summary.get('buckets', 0)} trades={summary.get('trades', 0)} "
               f"size_candidates={summary.get('size_candidates', 0)} "
               f"avoid_candidates={summary.get('avoid_candidates', 0)} "
               f"collect_more={summary.get('collect_more', 0)}")
    rows = table.get('rows') or []
    avoid = [r for r in rows if r.get('verdict') in ('avoid_candidate', 'watch_negative')]
    size = [r for r in rows if r.get('verdict') == 'size_candidate']
    if avoid:
        out.append('  Weak buckets to watch:')
        for r in avoid[:5]:
            out.append(f"    - {r['ticker']} {r['side']} {r['setup']} "
                       f"{r['btc_bucket']} {r['opening_state']} {r['execution_bucket']} "
                       f"phase={r['session_phase']}: {r['trades']} trades, "
                       f"WR={r['win_rate']}%, EV=${r['expectancy']:+.2f}, "
                       f"pnl=${r['pnl']:+.2f}, verdict={r['verdict']}")
    if size:
        out.append('  Strong buckets to watch:')
        for r in sorted(size, key=lambda x: x.get('expectancy', 0), reverse=True)[:5]:
            out.append(f"    - {r['ticker']} {r['side']} {r['setup']} "
                       f"{r['btc_bucket']} {r['opening_state']} {r['execution_bucket']} "
                       f"phase={r['session_phase']}: {r['trades']} trades, "
                       f"WR={r['win_rate']}%, EV=${r['expectancy']:+.2f}, "
                       f"pnl=${r['pnl']:+.2f}")
    if not avoid and not size:
        out.append('  No bucket has enough repeated evidence yet. Keep collecting.')
    out.append('')
    return out


def render_guardrail_audit(day_iso, tape, state):
    out = []
    out.append('Engine guardrail audit')
    out.append('-' * 78)
    skipped = skipped_for_day(state or {}, day_iso)
    audits = audit_events_for_day(day_iso)
    pre_submit = [s for s in skipped if str(s.get('reason', '')).startswith('pre_submit_')]
    live_quality = [s for s in skipped if str(s.get('reason', '')).startswith('live_quality_gate:')]
    spread_blocks = [s for s in skipped if s.get('reason') in ('spread_too_wide', 'quote_stale')
                     or str(s.get('reason', '')).startswith('pre_submit_spread')]
    btc_impulse = [r for r in tape if str(r.get('reason', '')).startswith('btc_impulse_abort')]
    pre_submit_events = [a for a in audits if a.get('event') == 'entry_pre_submit_blocked']
    bad_slip = [
        r for r in tape
        if r.get('entry_slippage_pct') is not None and float(r.get('entry_slippage_pct') or 0) >= 0.12
    ]

    required = {
        'setup': lambda r: (r.get('forensics') or {}).get('setup_type'),
        'btc': lambda r: ((r.get('forensics') or {}).get('btc') or {}).get('regime'),
        'opening_range': lambda r: ((r.get('forensics') or {}).get('location') or {}).get('opening_15m_break_state'),
        'execution_quality': lambda r: ((r.get('forensics') or {}).get('execution_quality') or {}).get('score'),
        'session_phase': lambda r: (r.get('forensics') or {}).get('session_phase'),
    }
    completeness = {}
    for name, getter in required.items():
        have = sum(1 for r in tape if getter(r) not in (None, '', 'unknown'))
        completeness[name] = round(have / len(tape) * 100, 1) if tape else None

    out.append(f'  pre-submit blocks: {len(pre_submit)} skipped row(s), {len(pre_submit_events)} audit event(s)')
    if pre_submit[:5]:
        for s in pre_submit[:5]:
            out.append(f"    - {s.get('ticker')} {s.get('side')} reason={s.get('reason')} "
                       f"score={s.get('score')} price={fmt_num(s.get('price'), 4)}")
    out.append(f'  live-quality gate blocks: {len(live_quality)}')
    out.append(f'  spread/quote guardrail blocks: {len(spread_blocks)}')
    out.append(f'  BTC impulse abort exits: {len(btc_impulse)}')
    if btc_impulse[:5]:
        for r in btc_impulse[:5]:
            out.append(f"    - {r.get('time')} {r.get('ticker')} {r.get('side')} "
                       f"pnl=${r.get('pnl'):+.2f} reason={r.get('reason')}")
    out.append(f'  high entry-slippage fills: {len(bad_slip)}')
    if bad_slip[:5]:
        for r in bad_slip[:5]:
            out.append(f"    - {r.get('time')} {r.get('ticker')} {r.get('side')} "
                       f"slip={fmt_num(r.get('entry_slippage_pct'), 3, '%')} pnl=${r.get('pnl'):+.2f}")
    out.append('  EV data completeness:')
    if tape:
        out.append('    ' + ', '.join(
            f'{k}={v}%' for k, v in completeness.items()
        ))
    else:
        out.append('    no closed trades today')
    out.append('  Review question: did guardrails save bad fills, or block winners we should study?')
    out.append('')
    return out


def strong_considerations(day_iso, tape, state=None):
    recs = []

    try:
        hyp_path = os.path.join(OUT_DIR, 'hypotheses.json')
        with open(hyp_path, 'r', encoding='utf-8') as f:
            hyp_state = json.load(f)
        for h in hyp_state.get('active', []):
            if h.get('stale'):
                continue
            if h.get('evidence_days', 0) >= 3:
                recs.append(
                    f"{h['id']}: {h.get('monitor')} Evidence has appeared on "
                    f"{h.get('evidence_days')} separate day(s), first={h.get('created')}, "
                    f"last={h.get('last_seen')}."
                )
    except Exception:
        pass

    try:
        from engine_validation import candidate_config
        cand = candidate_config(day_iso, lookback=5)
        for row in cand.get('candidates') or []:
            ev = row.get('evidence') or {}
            recs.append(
                f"Review engine gate `{row.get('variant')}` for promotion: rolling validation "
                f"shows {ev.get('days_positive')}/{ev.get('days_seen')} positive day(s), "
                f"blocked={ev.get('blocked')}, net delta=${ev.get('estimated_delta_vs_actual'):+.2f}. "
                f"This cleared the multi-day evidence gate; still apply manually only after review."
            )
    except Exception:
        pass
    return recs


def render_review_artifact_summary(day_iso):
    out = []
    out.append('Compact review artifacts')
    out.append('-' * 78)
    try:
        from review_artifacts import (
            build_artifact_manifest,
            build_entry_retry_summary,
            build_execution_summary,
            build_exit_hierarchy_summary,
            build_per_ticker_learning,
            build_regime_scoring_review,
            build_review_index,
            build_risk_summary,
            build_shadow_exit_summary,
        )
        risk = build_risk_summary(day_iso)
        execution = build_execution_summary(day_iso)
        shadow = build_shadow_exit_summary(day_iso)
        retry = build_entry_retry_summary(day_iso)
        exits = build_exit_hierarchy_summary(day_iso)
        ticker_learning = build_per_ticker_learning(day_iso)
        regime_review = build_regime_scoring_review(day_iso)
        index = build_review_index(day_iso)
        manifest = build_artifact_manifest(day_iso)
    except Exception as e:
        out.append(f'  Compact review artifacts unavailable: {e}')
        out.append('')
        return out
    out.append('  Preferred future review entry point:')
    out.append(f"    - {os.path.join(OUT_DIR, f'review_index_{day_iso}.json')}")
    out.append(f"  Risk: pnl=${risk.get('pnl'):+.2f} win_rate={risk.get('win_rate')}% "
               f"PF={risk.get('profit_factor')} max_drawdown=${risk.get('max_intraday_drawdown'):+.2f}")
    spread = execution.get('spread') or {}
    slippage = execution.get('slippage') or {}
    stale = execution.get('stale_data_incidents') or {}
    out.append(f"  Execution friction: spread_avg={spread.get('avg_pct')}% "
               f"slippage_avg={slippage.get('avg_pct')}% stale_events={stale.get('count')}")
    by_policy = shadow.get('by_policy') or {}
    if by_policy:
        out.append('  Shadow-exit grading:')
        for policy, row in sorted(by_policy.items()):
            out.append(f"    - {policy}: events={row.get('events')} "
                       f"saved_loser={row.get('saved_loser')} cut_winner={row.get('cut_winner')} "
                       f"improved_winner={row.get('improved_winner')} worsened_loser={row.get('worsened_loser')}")
    else:
        out.append('  Shadow-exit grading: no rows yet.')
    if retry.get('total_candidates'):
        out.append(f"  Entry retry candidates: total={retry.get('total_candidates')} "
                   f"by_reason={retry.get('by_reason')}")
    exit_selected = exits.get('selected_counts') or {}
    if exit_selected:
        out.append(f'  Exit hierarchy captured: {dict(list(exit_selected.items())[:5])}')
    ticker_rows = ticker_learning.get('rows') or []
    if ticker_rows:
        weak = sorted(ticker_rows, key=lambda row: (row.get('pnl') or 0))[:3]
        out.append('  Per-ticker learning watch:')
        for row in weak:
            out.append(f"    - {row.get('ticker')} {row.get('side')} {row.get('setup_type')}: "
                       f"trades={row.get('trades')} win_rate={row.get('win_rate')}% "
                       f"pnl=${row.get('pnl'):+.2f}")
    regime_rows = regime_review.get('rows') or []
    if regime_rows:
        out.append('  Regime scoring review worst rows:')
        for row in regime_rows[:3]:
            out.append(f"    - {row.get('btc_regime')} {row.get('side')} {row.get('setup_type')}: "
                       f"trades={row.get('trades')} "
                       f"win_rate={row.get('win_rate')}% pnl=${row.get('pnl'):+.2f}")
    try:
        import weekend_readiness as wr
        split = wr.build_strategy_operations_split(day_iso)
        out.append('  Strategy vs operations split:')
        for key in ('strategy', 'data_or_execution', 'operations'):
            row = split.get(key) or {}
            out.append(f"    - {key}: trades={row.get('trades')} losses={row.get('losses')} "
                       f"pnl=${row.get('pnl'):+.2f}")
        why = wr.build_why_no_trade_summary(day_iso)
        recent = why.get('recent') or {}
        out.append(f"  Why-no-trade recent: skipped={recent.get('skipped_signals')} "
                   f"near={recent.get('near_signals')}")
    except Exception as e:
        out.append(f'  Strategy/operations compact artifacts unavailable: {e}')
    gate = (index.get('promotion_gate_status') or {}).get('recommendation')
    out.append(f'  Promotion gate: {gate}')
    try:
        from promotion_queue import render_queue_lines
        queue_lines = render_queue_lines(day_iso)
        if queue_lines:
            out.extend(queue_lines)
    except Exception as e:
        out.append(f'  Promotion queue unavailable: {e}')
    stats = manifest.get('corpus_size_stats') or {}
    out.append(f"  Corpus bytes tracked: {stats.get('total_bytes')} "
               f"large_files={len(stats.get('large_files') or [])}")
    out.append('')
    return out


def render_strong_considerations(day_iso, tape, state=None):
    out = []
    recs = strong_considerations(day_iso, tape, state=state)
    out.append('**STRONGLY CONSIDER THE BELOW CHANGES**')
    out.append('-' * 78)
    if not recs:
        out.append('  No strategy changes cleared the evidence threshold today.')
        out.append('  Continue collecting data; do not alter rules from isolated losses.')
    else:
        out.append('  These are recommendations for human review only. No automatic changes were made.')
        for rec in recs:
            out.append(f'  - {rec}')
    return out


def operational_incident_review(day_iso, tape, log_hits, state=None):
    """Separate broker/process safety incidents from strategy quality."""
    out = []
    out.append('Operational incident review')
    out.append('-' * 78)

    incident_reasons = {'manual_stop', 'external_broker_exit', 'session_end'}
    incident_trades = [r for r in tape if r.get('reason') in incident_reasons]
    exposure_lines = (
        log_hits.get('broker_block', [])
        + log_hits.get('close_failed', [])
        + log_hits.get('extended_hours', [])
    )

    if not incident_trades and not exposure_lines:
        out.append('  No broker-close or process-control incident detected in today\'s tape/logs.')
        out.append('')
        return out

    out.append('  What happened operationally:')
    if log_hits.get('close_failed'):
        out.append('    - Broker close attempts hit an order/position constraint; this is operational, not an entry-edge problem.')
    if log_hits.get('broker_block'):
        out.append('    - The bot raised broker_exposure_block, which correctly stops new entries until exposure is reconciled.')
    manual = [r for r in tape if r.get('reason') == 'manual_stop']
    if manual:
        loss = sum(r.get('pnl', 0) for r in manual)
        out.append(f"    - Manual stop closed {len(manual)} tracked trade(s), net=${loss:+.2f}.")
    external = [r for r in tape if r.get('reason') == 'external_broker_exit']
    if external:
        loss = sum(r.get('pnl', 0) for r in external)
        out.append(f"    - External broker reconciliation closed {len(external)} trade(s), net=${loss:+.2f}.")
    if log_hits.get('extended_hours'):
        out.append('    - Extended-hours flatten fallback was used after regular-hours close handling was insufficient.')
    if not any((log_hits.get('close_failed'), log_hits.get('broker_block'), manual, external, log_hits.get('extended_hours'))):
        out.append('    - Session-end/manual close reasons were present; inspect broker fills separately from strategy rules.')

    out.append('  Fixes now expected in live process:')
    out.append('    - Entry cutoff is separate from hard-flat, so the bot can stop opening trades before it must flatten.')
    out.append('    - Monitor flatten/retry continues even when the bot is stopped or broker_exposure_block is set.')
    out.append('    - Remaining exposure outside regular hours can use an extended-hours limit-close fallback.')
    out.append('    - Strategy review below should separate avoidable trade losses from broker/process cleanup losses.')

    positions = (state or {}).get('positions') or {}
    block = (state or {}).get('broker_exposure_block')
    if positions or block:
        out.append('  Current safety state:')
        out.append(f"    - local_positions={list(positions.keys())} broker_exposure_block={block}")
    out.append('')
    return out


def export_candidate_rules(day_iso, tape, state=None):
    rules = counterfactual_rules(tape)
    skipped_summary, _ = skipped_opportunity_summary(state or {}, day_iso)
    candidates = []
    for r in rules:
        if r.get('net_estimated_impact', 0) <= 0 and r.get('losers_avoided', 0) < 2:
            continue
        candidates.append({
            'source': 'taken_trade_counterfactual',
            **r,
            'backtest_status': 'pending',
            'created_from_day': day_iso,
        })
    for s in skipped_summary:
        avg5 = s.get('avg5')
        if s.get('n', 0) >= 5 and s.get('would_work_5m', 0) >= 4 and avg5 is not None and avg5 > 0:
            candidates.append({
                'source': 'skipped_signal_opportunity',
                'rule': f"review_skip_filter:{s.get('reason')}",
                'description': f"Review whether skip reason `{s.get('reason')}` is too strict.",
                'actionable_pre_entry': True,
                'confidence': 'low-medium' if s.get('n', 0) >= 8 else 'low',
                'skips': s.get('n'),
                'positive_fwd5': s.get('would_work_5m'),
                'avg5': avg5,
                'avg15': s.get('avg15'),
                'avg30': s.get('avg30'),
                'backtest_status': 'pending',
                'created_from_day': day_iso,
            })
    path = os.path.join(OUT_DIR, f'candidate_rules_{day_iso}.json')
    payload = {
        'date_iso': day_iso,
        'created_at': datetime.now(CT).isoformat(timespec='seconds'),
        'candidate_count': len(candidates),
        'candidates': candidates,
    }
    return path, payload


def render_do_not_change_yet(day_iso, tape, state=None):
    out = []
    out.append('Do not change yet')
    out.append('-' * 78)
    rules = counterfactual_rules(tape)
    roll = rolling_evidence(day_iso)
    notes = []
    if any(r.get('losers_avoided', 0) < 2 for r in rules):
        notes.append('Do not change live rules from a single isolated loser.')
    if not any(r.get('evidence_days', 0) >= 2 for r in roll.get('rules', [])):
        notes.append('Do not promote today-only counterfactuals without rolling evidence or a backtest.')
    if any(r.get('rule') in ('test_profit_protection', 'review_cond_time_stop') for r in rules):
        notes.append('Do not treat post-entry MFE/MAE evidence as a pre-entry filter.')
    if not notes:
        notes.append('No extra guardrails today beyond normal evidence thresholds.')
    for n in notes:
        out.append(f'  - {n}')
    out.append('')
    return out


def render_human_context(day_iso):
    notes = load_human_notes(day_iso)
    out = []
    out.append('Human context')
    out.append('-' * 78)
    if not any(notes.values()):
        out.append('  Trader notes: none recorded.')
        out.append('  Market feel: none recorded.')
        out.append('  Known catalysts: none recorded.')
    else:
        out.append(f"  Trader notes: {notes.get('trader_notes') or 'none recorded'}")
        out.append(f"  Market feel: {notes.get('market_feel') or 'none recorded'}")
        catalysts = notes.get('known_catalysts') or []
        if catalysts:
            out.append('  Known catalysts:')
            for c in catalysts[:8]:
                out.append(f'    - {c}')
        else:
            out.append('  Known catalysts: none recorded.')
        if notes.get('external_context'):
            out.append(f"  External context: {notes.get('external_context')}")
    out.append('  To add notes, create postmortem/notes_YYYY-MM-DD.json.')
    out.append('')
    return out


def hypothesis_preview(day_iso, tape):
    try:
        from postmortem_hypotheses import _load, _run_detectors
        hyp_state = _load()
        findings = _run_detectors(tape)
    except Exception:
        return {'added': [], 'updated': [], 'active': [], 'retired': []}
    active = hyp_state.get('active', []) or []
    by_key = {h.get('key'): h for h in active}
    added, updated = [], []
    for key, _ded, mon in findings:
        if key in by_key:
            updated.append({'id': by_key[key].get('id'), 'monitor': mon})
        else:
            added.append({'key': key, 'monitor': mon})
    retired = [
        h for h in hyp_state.get('retired', []) or []
        if h.get('retired_on') == day_iso
    ]
    return {'added': added, 'updated': updated, 'active': active, 'retired': retired}


def render_top_summary(day_iso, flags, tape, state):
    out = []
    winners = [r for r in tape if r.get('pnl', 0) > 0]
    losers = [r for r in tape if r.get('pnl', 0) < 0]
    total = sum(r.get('pnl', 0) for r in tape)
    out.append('Daily human summary')
    out.append('-' * 78)
    if not tape:
        out.append('  Day result: no closed trades yet; focus is data collection and guardrail monitoring.')
    else:
        wr = 100 * len(winners) / len(tape) if tape else 0
        out.append(f"  Day result: {len(winners)}W/{len(losers)}L, win rate={wr:.1f}%, P&L=${total:+.2f}.")

    try:
        from ev_analytics import build_ev_table
        ev_rows = build_ev_table(day_iso, current_tape=tape, lookback_days=10).get('rows') or []
    except Exception:
        ev_rows = []
    strong = [r for r in ev_rows if r.get('verdict') == 'size_candidate']
    weak = [r for r in ev_rows if r.get('verdict') in ('avoid_candidate', 'watch_negative')]

    out.append('  What worked:')
    if winners:
        by_setup = sorted(aggregate_by_setup(winners), key=lambda r: r.get('pnl', 0), reverse=True)
        for row in by_setup[:3]:
            out.append(f"    - {row['setup']}: {row['w']} winner(s), pnl=${row['pnl']:+.2f}")
    elif strong:
        for row in sorted(strong, key=lambda r: r.get('expectancy', 0), reverse=True)[:3]:
            out.append(f"    - Historical bucket to watch: {row['ticker']} {row['side']} "
                       f"{row['setup']} EV=${row['expectancy']:+.2f}")
    else:
        out.append('    - No same-day winners or proven bucket yet.')

    out.append("  What didn't work:")
    if losers:
        loser_review = build_loser_review(tape)
        themes = aggregate_loser_themes(loser_review)
        for tag, n in themes[:3]:
            out.append(f"    - {tag}: {n} losing trade(s)")
    elif weak:
        for row in weak[:3]:
            out.append(f"    - Historical weak bucket: {row['ticker']} {row['side']} "
                       f"{row['setup']} EV=${row['expectancy']:+.2f}")
    else:
        out.append('    - No same-day losers and no new weak bucket.')

    if flags:
        out.append('  Health/data notes: ' + '; '.join(flags[:4]))
    out.append('')
    return out


def render_tracking_hypotheses(hypothesis_lines):
    out = ['Tracking hypotheses', '-' * 78]
    body = list(hypothesis_lines or [])
    if len(body) >= 2 and body[0] == 'Watchlist & deductions':
        body = body[2:]
    if body:
        out.extend(body)
    else:
        out.append('  No active hypothesis data available.')
    out.append('')
    return out


def render_human_review_prompts(day_iso):
    return [
        'Human review prompts',
        '-' * 78,
        '  1. What would you change based on today, if anything?',
        '  2. What would you refuse to change from one day of data?',
        '  3. Did the bot lose because of strategy, execution, data, or operations?',
        '  4. Did any guardrail save us from a worse trade or worse day?',
        f'  Notes file: {os.path.join(OUT_DIR, f"notes_{day_iso}.json")}',
        '',
    ]


def build_daily_learning_brief(day_iso, flags, tape, state=None):
    state = state or {}
    winners = [r for r in tape if r.get('pnl', 0) > 0]
    losers = [r for r in tape if r.get('pnl', 0) < 0]
    loser_review = build_loser_review(tape)
    themes = aggregate_loser_themes(loser_review)
    winner_setups = aggregate_by_setup(winners)
    skipped_summary, skipped_rows = skipped_opportunity_summary(state, day_iso)
    near_rows = near_signals_for_day(day_iso)

    attribution = Counter()
    for item in loser_review:
        for bucket, weight in (item.get('attribution') or {}).items():
            attribution[bucket] += float(weight or 0)
    attr_total = sum(attribution.values()) or 1.0
    attribution_rows = [
        {'bucket': k, 'share': round(v / attr_total * 100, 1)}
        for k, v in attribution.most_common()
    ]

    if losers and themes:
        biggest_lesson = (
            f"Primary loser theme was {themes[0][0]} across {themes[0][1]} loser(s); "
            "watch for repeat evidence before promoting a rule."
        )
    elif winners and not losers:
        biggest_lesson = 'No closed losers; focus on whether winners had clean thesis confirmation and efficient exits.'
    elif not tape:
        biggest_lesson = 'No closed trades; review feed health, no-trade reasons, and near-signal quality.'
    else:
        biggest_lesson = 'Mixed or flat day; keep collecting evidence before changing live rules.'

    what_worked = []
    for row in sorted(winner_setups, key=lambda r: (r.get('pnl', 0), r.get('w', 0)), reverse=True)[:4]:
        what_worked.append(
            f"{row['setup']}: {row['w']} winner(s), pnl=${row['pnl']:+.2f}, win%={row['win_pct']:.1f}"
        )
    if not what_worked:
        what_worked.append('No same-day winning setup to promote.')

    what_failed = []
    for tag, count in themes[:5]:
        loss = sum(
            abs((item.get('row') or {}).get('pnl') or 0)
            for item in loser_review
            if tag in (item.get('tags') or [])
        )
        what_failed.append(f"{tag}: {count} loser(s), gross loss=${loss:.2f}")
    if not what_failed:
        what_failed.append('No same-day loser theme.')

    loser_rows = []
    for item in loser_review[:8]:
        row = item.get('row') or {}
        fx = row.get('forensics') or {}
        btc = row.get('btc') or {}
        thesis = item.get('thesis_failure') or {}
        preventers = item.get('preventers') or []
        loser_rows.append({
            'time': row.get('time'),
            'ticker': row.get('ticker'),
            'side': row.get('side'),
            'pnl': row.get('pnl'),
            'setup_type': fx.get('setup_type') or setup_type(row),
            'entry_score': row.get('score'),
            'entry_thesis': (
                f"setup={fx.get('setup_type') or setup_type(row)} "
                f"score={row.get('score')} conviction={row.get('conv')}"
            ),
            'btc_context': (
                f"regime={btc.get('regime')} conflict={btc.get('conflict')} "
                f"mom60={btc.get('mom_60s') or btc.get('mom_60')}"
            ),
            'failure_label': thesis.get('label'),
            'primary_problem': item.get('primary'),
            'warnings': (item.get('evidence') or [])[:4],
            'decision_grade': (item.get('decision_grade') or {}).get('label'),
            'preventers': [p.get('description') for p in preventers[:3]],
        })

    winner_rows = []
    for row in winners[:6]:
        fx = row.get('forensics') or {}
        winner_rows.append({
            'time': row.get('time'),
            'ticker': row.get('ticker'),
            'side': row.get('side'),
            'pnl': row.get('pnl'),
            'setup_type': fx.get('setup_type') or setup_type(row),
            'reason': row.get('reason'),
            'mfe_pct': row.get('mfe_pct'),
            'mae_pct': row.get('mae_pct'),
        })

    missed = [
        r for r in skipped_rows
        if r.get('opportunity_grade') in ('missed_winner', 'bad_skip')
    ][:5]
    good_skips = [
        r for r in skipped_rows
        if r.get('opportunity_grade') in ('avoided_loser', 'good_skip')
    ][:5]
    skipped_reason_rows = []
    for row in skipped_summary[:5]:
        skipped_reason_rows.append({
            'reason': row.get('reason'),
            'n': row.get('n'),
            'avg5': row.get('avg5'),
            'avg15': row.get('avg15'),
            'avg30': row.get('avg30'),
            'grade_counts': row.get('grade_counts'),
        })

    tomorrow_watch = []
    if themes:
        tomorrow_watch.append(f"Repeat loser theme check: {themes[0][0]}")
    if attribution_rows:
        tomorrow_watch.append(f"Primary loss bucket: {attribution_rows[0]['bucket']} ({attribution_rows[0]['share']}%)")
    if missed:
        tomorrow_watch.append('Review whether high-quality skipped trades were blocked by an overly strict gate.')
    if flags:
        tomorrow_watch.append('Resolve or explain health/data flags before trusting strategy conclusions.')
    if not tomorrow_watch:
        tomorrow_watch.append('Keep collecting clean data; no urgent rule-change candidate from this section.')

    return {
        'biggest_lesson': biggest_lesson,
        'market_regime': _market_regime_label(day_iso),
        'what_worked': what_worked,
        'what_failed': what_failed,
        'loss_attribution': attribution_rows,
        'loser_rows': loser_rows,
        'winner_rows': winner_rows,
        'skipped_reason_rows': skipped_reason_rows,
        'missed_skips': missed,
        'good_skips': good_skips,
        'near_signal_count': len(near_rows),
        'tomorrow_watch': tomorrow_watch,
    }


def render_daily_learning_brief(day_iso, flags, tape, state=None):
    brief = build_daily_learning_brief(day_iso, flags, tape, state=state)
    out = ['Daily learning desk review', '-' * 78]
    out.append(f"  Biggest lesson: {brief['biggest_lesson']}")
    out.append(f"  Regime read: {brief['market_regime']}")

    out.append('  What worked:')
    for item in brief['what_worked']:
        out.append(f'    - {item}')

    out.append("  What didn't work:")
    for item in brief['what_failed']:
        out.append(f'    - {item}')

    out.append('  Loss attribution:')
    if brief['loss_attribution']:
        for row in brief['loss_attribution']:
            out.append(f"    - {row['bucket']}: {row['share']}%")
    else:
        out.append('    - No losing trades to attribute.')

    out.append('  Loser review:')
    if brief['loser_rows']:
        for row in brief['loser_rows']:
            out.append(
                f"    - {row['time']} {row['ticker']} {row['side']} "
                f"pnl=${row['pnl']:+.2f}: {row['failure_label']} / {row['primary_problem']}"
            )
            out.append(f"      entry thesis: {row['entry_thesis']}; BTC {row['btc_context']}")
            out.append(f"      decision grade: {row['decision_grade']}")
            if row['warnings']:
                out.append('      warnings: ' + '; '.join(str(w) for w in row['warnings']))
            if row['preventers']:
                out.append('      possible preventers: ' + '; '.join(str(p) for p in row['preventers']))
    else:
        out.append('    - No losing trades.')

    out.append('  Winner review:')
    if brief['winner_rows']:
        for row in brief['winner_rows']:
            out.append(
                f"    - {row['time']} {row['ticker']} {row['side']} "
                f"{row['setup_type']} pnl=${row['pnl']:+.2f} exit={row['reason']} "
                f"mfe={row['mfe_pct']} mae={row['mae_pct']}"
            )
    else:
        out.append('    - No winning trades.')

    out.append('  Skipped and near-signal learning:')
    if brief['skipped_reason_rows']:
        for row in brief['skipped_reason_rows'][:4]:
            out.append(
                f"    - {row['reason']}: n={row['n']} "
                f"avg5={fmt_num(row['avg5'], 3)} avg15={fmt_num(row['avg15'], 3)} "
                f"avg30={fmt_num(row['avg30'], 3)} grades={row['grade_counts']}"
            )
    else:
        out.append('    - No skipped-signal forward data available.')
    out.append(f"    - near-signal rows reviewed: {brief['near_signal_count']}")
    if brief['missed_skips']:
        out.append('    - missed/bad skips to inspect:')
        for row in brief['missed_skips'][:3]:
            out.append(
                f"      {row.get('ticker')} {row.get('side')} reason={row.get('reason')} "
                f"fwd5={fmt_num(row.get('fwd5'), 3)} grade={row.get('opportunity_grade')}"
            )
    if brief['good_skips']:
        out.append('    - good avoided trades:')
        for row in brief['good_skips'][:3]:
            out.append(
                f"      {row.get('ticker')} {row.get('side')} reason={row.get('reason')} "
                f"fwd5={fmt_num(row.get('fwd5'), 3)} grade={row.get('opportunity_grade')}"
            )

    out.append('  Tomorrow watchlist:')
    for item in brief['tomorrow_watch']:
        out.append(f'    - {item}')
    out.append('')
    return out


def decision_confidence_meter(day_iso, tape, state=None):
    rules = counterfactual_rules(tape)
    roll = rolling_evidence(day_iso)
    lifecycle = build_learning_lifecycle(day_iso, tape, state=state)
    rows = []

    for item in build_loser_review(tape):
        tags = item.get('tags') or []
        for tag in tags:
            rows.append({
                'type': 'loser_theme',
                'name': tag,
                'stage': 'observation',
                'confidence': 'low',
                'evidence': f"today loser {item.get('row', {}).get('time')} {item.get('row', {}).get('ticker')}",
                'next_step': 'watch for repeated evidence before changing rules',
            })

    for h in (lifecycle.get('tracked_hypotheses') or lifecycle.get('hypotheses') or []):
        confidence = h.get('confidence') or 'low'
        days = int(h.get('evidence_days') or 0)
        stage = 'building_evidence' if days >= 2 else 'weak_hypothesis'
        if confidence in ('high', 'medium-high') and days >= 3:
            stage = 'strong_candidate'
        rows.append({
            'type': 'hypothesis',
            'name': h.get('id') or h.get('key') or 'hypothesis',
            'stage': stage,
            'confidence': confidence,
            'evidence_days': days,
            'evidence': h.get('monitor'),
            'next_step': 'promote only after clean multi-day evidence and human approval',
        })

    for r in rules:
        if r.get('net_estimated_impact', 0) > 0 and r.get('losers_avoided', 0) >= 2:
            stage = 'weak_hypothesis'
            if r.get('confidence') in ('medium', 'medium-high') and r.get('winners_blocked', 0) == 0:
                stage = 'building_evidence'
            rows.append({
                'type': 'candidate_rule',
                'name': r.get('rule'),
                'stage': stage,
                'confidence': r.get('confidence'),
                'losers_avoided': r.get('losers_avoided'),
                'winners_blocked': r.get('winners_blocked'),
                'net_estimated_impact': r.get('net_estimated_impact'),
                'next_step': 'backtest and require rolling evidence',
            })

    for r in roll.get('rules', []) or []:
        if r.get('evidence_days', 0) >= 2:
            stage = 'building_evidence'
            if r.get('evidence_days', 0) >= 3 and r.get('net_estimated_impact', 0) > 0:
                stage = 'strong_candidate'
            rows.append({
                'type': 'rolling_rule',
                'name': r.get('rule'),
                'stage': stage,
                'confidence': 'medium' if stage == 'building_evidence' else 'medium-high',
                'evidence_days': r.get('evidence_days'),
                'losers_avoided': r.get('losers_avoided'),
                'winners_blocked': r.get('winners_blocked'),
                'net_estimated_impact': r.get('net_estimated_impact'),
                'next_step': 'review winner damage before approval',
            })

    seen = set()
    unique = []
    priority = {
        'approved_change': 0,
        'strong_candidate': 1,
        'building_evidence': 2,
        'weak_hypothesis': 3,
        'observation': 4,
        'rejected': 5,
    }
    for row in rows:
        key = (row.get('type'), row.get('name'), row.get('stage'))
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    unique.sort(key=lambda r: (
        priority.get(r.get('stage'), 9),
        -(r.get('net_estimated_impact') or 0),
        -(r.get('evidence_days') or 0),
    ))
    return unique[:20]


def loser_saver_rankings(tape):
    counts = {}
    examples = {}
    for item in build_loser_review(tape):
        row = item.get('row') or {}
        tags = set(item.get('tags') or [])
        preventers = item.get('preventers') or []
        if any(t in tags for t in ('wide_spread', 'bad_fill', 'bad_execution_context')):
            bucket = 'execution_or_spread_block'
        elif any(t in tags for t in ('btc_conflict', 'btc_not_confirming', 'stock_lagged_btc', 'stock_outperformed_btc')):
            bucket = 'btc_or_miner_relationship_block'
        elif any(t in tags for t in ('profit_reversal', 'exit_rule_candidate', 'bad_exit_or_profit_protection')):
            bucket = 'faster_or_smarter_exit'
        elif any(t in tags for t in ('bad_entry_or_timing', 'never_green', 'short_into_positive_60s_mom', 'long_into_negative_60s_mom', 'bad_entry_context')):
            bucket = 'better_entry_filter'
        elif any((p.get('rule') or '') != 'no_change' for p in preventers):
            bucket = 'mixed_rule_candidate'
        else:
            bucket = 'no_realistic_preventer_yet'
        entry = counts.setdefault(bucket, {
            'bucket': bucket,
            'losers': 0,
            'gross_loss': 0.0,
            'examples': [],
        })
        entry['losers'] += 1
        entry['gross_loss'] += abs(float(row.get('pnl') or 0))
        if len(entry['examples']) < 4:
            entry['examples'].append(f"{row.get('time')} {row.get('ticker')} {row.get('side')} ${row.get('pnl'):+.2f}")
        examples[bucket] = entry['examples']
    out = []
    for row in counts.values():
        row['gross_loss'] = round(row['gross_loss'], 2)
        out.append(row)
    return sorted(out, key=lambda r: (-r['gross_loss'], -r['losers']))


def winner_damage_risk(tape):
    rows = []
    for r in counterfactual_rules(tape):
        loss_avoided = float(r.get('loss_avoided') or 0)
        winner_cost = float(r.get('winner_pnl_sacrificed') or 0)
        if loss_avoided <= 0 and winner_cost <= 0:
            continue
        ratio = winner_cost / loss_avoided if loss_avoided else None
        if winner_cost == 0:
            risk = 'low'
        elif ratio is not None and ratio <= 0.35:
            risk = 'acceptable'
        elif ratio is not None and ratio <= 0.8:
            risk = 'high'
        else:
            risk = 'very_high'
        rows.append({
            'rule': r.get('rule'),
            'description': r.get('description'),
            'losers_avoided': r.get('losers_avoided'),
            'loss_avoided': r.get('loss_avoided'),
            'winners_blocked': r.get('winners_blocked'),
            'winner_pnl_sacrificed': r.get('winner_pnl_sacrificed'),
            'net_estimated_impact': r.get('net_estimated_impact'),
            'winner_damage_ratio': round(ratio, 3) if ratio is not None else None,
            'risk': risk,
        })
    return sorted(rows, key=lambda r: (
        {'low': 0, 'acceptable': 1, 'high': 2, 'very_high': 3}.get(r['risk'], 9),
        -(r.get('net_estimated_impact') or 0),
    ))


def feedback_map(day_iso, tape):
    config_map = {
        'block_btc_conflict': ('ws_scalp.py', 'BTC context gates / scoring penalties'),
        'require_btc_confirmation': ('ws_scalp.py', 'BTC readiness and setup confirmation'),
        'block_short_positive_mom60': ('ws_scalp.py', 'side momentum guard'),
        'block_long_negative_mom60': ('ws_scalp.py', 'side momentum guard'),
        'block_wide_spread': ('mock_trader.py / ws_scalp.py', 'spread and execution quality gates'),
        'tighten_execution_slippage': ('mock_trader.py', 'pre-submit/fill quality checks'),
        'test_profit_protection': ('mock_trader.py', '_smart_exit_reason / profit protection'),
        'review_cond_time_stop': ('mock_trader.py', 'conditional time stop / conviction exits'),
        'block_stock_lagging_btc_long': ('ws_scalp.py', 'miner vs BTC relative strength'),
        'block_stock_outperforming_btc_short': ('ws_scalp.py', 'miner vs BTC relative strength'),
        'tighten_entry_timing': ('ws_scalp.py', 'entry confirmation / follow-through requirements'),
        'block_bad_entry_context': ('ws_scalp.py', 'composite entry score gates'),
        'block_bad_execution_context': ('mock_trader.py / ws_scalp.py', 'execution quality gates'),
    }
    rows = []
    for r in counterfactual_rules(tape):
        rule = r.get('rule')
        target = config_map.get(rule, ('trading_config.json', 'requires manual mapping before implementation'))
        rows.append({
            'rule': rule,
            'file_or_surface': target[0],
            'implementation_area': target[1],
            'actionable_pre_entry': r.get('actionable_pre_entry'),
            'promotion_status': 'candidate_only',
            'evidence_needed': 'multi-day clean evidence plus winner-damage review',
        })
    return rows


def next_session_watch_card(day_iso, flags, tape, state=None):
    brief = build_daily_learning_brief(day_iso, flags, tape, state=state)
    confidence = decision_confidence_meter(day_iso, tape, state=state)
    strong = [r for r in confidence if r.get('stage') == 'strong_candidate']
    building = [r for r in confidence if r.get('stage') == 'building_evidence']
    risks = []
    if flags:
        risks.extend(flags[:3])
    dq = data_quality_score(flags, tape, state or {}, day_iso)
    for issue in dq.get('issues') or []:
        if issue not in risks:
            risks.append(issue)
    return {
        'top_watch_items': brief.get('tomorrow_watch', [])[:5],
        'close_to_promotion': strong[:5],
        'building_evidence': building[:5],
        'hypotheses_to_kill_or_downgrade': [
            r for r in confidence
            if r.get('stage') == 'rejected'
        ][:5],
        'operational_risks': risks[:5],
    }


def postmortem_quality_score(day_iso, flags, tape, state=None):
    state = state or {}
    dq = data_quality_score(flags, tape, state, day_iso)
    score = int(dq.get('score') or 0)
    checks = []

    def add(name, ok, detail='', penalty=0):
        nonlocal score
        checks.append({'name': name, 'ok': bool(ok), 'detail': detail})
        if not ok:
            score -= penalty

    trade_count = len(tape or [])
    add('enough_closed_trades', trade_count >= 5, f'{trade_count} closed trade(s)', 8)
    fwd_issues = sum(1 for r in tape if r.get('fwd_status') not in (None, 'ok'))
    add('forward_returns_available', fwd_issues == 0, f'{fwd_issues} issue(s)', 12)
    missing_fills = sum(
        1 for r in tape
        if r.get('broker_close') and r.get('broker_exit_fill_price') is None
    )
    add('broker_fills_complete', missing_fills == 0, f'{missing_fills} missing loser/broker fill(s)', 10)
    clean = clean_day_score(day_iso, flags, tape, state)
    labels = clean.get('labels') or []
    add('not_ops_contaminated', 'ops_contaminated' not in labels, ', '.join(labels) or 'clean', 15)
    skipped_count = len(skipped_for_day(state, day_iso))
    near_count = len(near_signals_for_day(day_iso))
    add('comparison_corpus_present', skipped_count > 0 or near_count > 0,
        f'skipped={skipped_count} near={near_count}', 6)
    add('health_flags_clear', not flags, f'{len(flags or [])} flag(s)', 10)

    score = max(0, min(100, score))
    if score >= 85:
        label = 'high_trust'
    elif score >= 70:
        label = 'usable_with_caveats'
    elif score >= 50:
        label = 'review_only'
    else:
        label = 'do_not_learn_strategy'
    return {
        'score': score,
        'label': label,
        'checks': checks,
        'data_quality': dq,
        'clean_day_score': clean,
    }


def market_regime_fit(day_iso, tape, state=None):
    regime = _market_regime_label(day_iso)
    split = {}
    for row in tape or []:
        side = row.get('side') or 'UNKNOWN'
        bucket = split.setdefault(side, {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0})
        pnl = float(row.get('pnl') or 0)
        bucket['trades'] += 1
        bucket['wins'] += 1 if pnl > 0 else 0
        bucket['losses'] += 1 if pnl < 0 else 0
        bucket['pnl'] += pnl
    for row in split.values():
        row['pnl'] = round(row['pnl'], 2)
        row['win_rate'] = round(100 * row['wins'] / row['trades'], 1) if row['trades'] else None

    total = sum(float(r.get('pnl') or 0) for r in tape or [])
    losers = [r for r in tape or [] if r.get('pnl', 0) < 0]
    themes = aggregate_loser_themes(build_loser_review(tape))
    if regime == 'regime not classified yet':
        fit = 'no_read'
    elif total > 0 and len(losers) <= max(1, len(tape or []) * 0.35):
        fit = 'favorable'
    elif total < 0 and any(tag in {'btc_conflict', 'btc_not_confirming'} for tag, _ in themes[:4]):
        fit = 'hostile'
    elif total < 0:
        fit = 'mixed'
    else:
        fit = 'favorable'
    return {
        'regime': regime,
        'fit': fit,
        'side_performance': split,
        'deduction': (
            'Do not draw regime-specific conclusions until market regime is classified.'
            if fit == 'no_read'
            else f'Bot environment scored as {fit}; compare same setup behavior in similar regimes.'
        ),
    }


def hypothesis_aging(day_iso, tape):
    try:
        hp = hypothesis_preview(day_iso, tape)
    except Exception:
        hp = {'active': [], 'retired': []}
    today = datetime.fromisoformat(day_iso).date()
    rows = []
    for h in hp.get('active') or []:
        created = h.get('created') or h.get('first_seen') or day_iso
        last_seen = h.get('last_seen') or created
        try:
            created_day = datetime.fromisoformat(str(created)[:10]).date()
        except Exception:
            created_day = today
        try:
            last_seen_day = datetime.fromisoformat(str(last_seen)[:10]).date()
        except Exception:
            last_seen_day = created_day
        age_days = max(0, (today - created_day).days + 1)
        days_since_seen = max(0, (today - last_seen_day).days)
        evidence_days = int(h.get('evidence_days') or 0)
        if h.get('stale') or days_since_seen >= 3:
            trend = 'stale_or_fading'
            kill_review = day_iso
        elif evidence_days >= 3:
            trend = 'strengthening'
            kill_review = None
        elif evidence_days >= 2:
            trend = 'building'
            kill_review = None
        else:
            trend = 'new_or_unproven'
            kill_review = (today + timedelta(days=5)).isoformat()
        rows.append({
            'id': h.get('id'),
            'monitor': h.get('monitor'),
            'age_days': age_days,
            'evidence_days': evidence_days,
            'first_seen': created,
            'last_seen': last_seen,
            'days_since_seen': days_since_seen,
            'trend': trend,
            'kill_review_date': kill_review,
        })
    return sorted(rows, key=lambda r: (
        r['trend'] == 'stale_or_fading',
        -r['evidence_days'],
        -r['age_days'],
    ))


def rule_promotion_checklist(day_iso, flags, tape, state=None):
    quality = postmortem_quality_score(day_iso, flags, tape, state=state)
    rows = []
    rolling = rolling_evidence(day_iso)
    rolling_by_rule = {r.get('rule'): r for r in rolling.get('rules', []) or []}
    for risk in winner_damage_risk(tape):
        rule = risk.get('rule')
        roll = rolling_by_rule.get(rule, {})
        checks = [
            {'name': '2+ clean evidence days', 'ok': int(roll.get('evidence_days') or 0) >= 2},
            {'name': 'enough affected trades', 'ok': int(risk.get('losers_avoided') or 0) + int(risk.get('winners_blocked') or 0) >= 3},
            {'name': 'positive net impact', 'ok': float(risk.get('net_estimated_impact') or 0) > 0},
            {'name': 'acceptable winner damage', 'ok': risk.get('risk') in ('low', 'acceptable')},
            {'name': 'not ops/data contaminated', 'ok': quality.get('label') in ('high_trust', 'usable_with_caveats')},
            {'name': 'maps cleanly to engine rule', 'ok': any(row.get('rule') == rule for row in feedback_map(day_iso, tape))},
        ]
        passed = sum(1 for c in checks if c['ok'])
        status = 'promotion_ready' if passed == len(checks) else ('close' if passed >= 4 else 'not_ready')
        rows.append({
            'rule': rule,
            'status': status,
            'passed': passed,
            'total': len(checks),
            'checks': checks,
            'winner_damage_risk': risk,
        })
    return sorted(rows, key=lambda r: (
        r['status'] != 'promotion_ready',
        r['status'] != 'close',
        -r['passed'],
        -(r['winner_damage_risk'].get('net_estimated_impact') or 0),
    ))


def do_not_overfit_box(day_iso, flags, tape, state=None):
    notes = []
    quality = postmortem_quality_score(day_iso, flags, tape, state=state)
    if quality.get('label') not in ('high_trust',):
        notes.append(f"Postmortem quality is {quality.get('label')}; strategy promotion should wait.")
    for row in winner_damage_risk(tape)[:8]:
        if row.get('risk') in ('high', 'very_high'):
            notes.append(
                f"{row.get('rule')} looked interesting but has {row.get('risk')} winner-damage risk "
                f"({row.get('winners_blocked')} winner(s) blocked)."
            )
    for r in counterfactual_rules(tape):
        if r.get('losers_avoided', 0) < 2 and r.get('net_estimated_impact', 0) > 0:
            notes.append(f"{r.get('rule')} is a one-off saver today; do not promote from one loser.")
    if not notes:
        notes.append('No obvious overfit trap beyond normal multi-day evidence discipline.')
    return notes[:10]


def top_research_questions(day_iso, flags, tape, state=None):
    questions = []
    brief = build_daily_learning_brief(day_iso, flags, tape, state=state)
    savers = loser_saver_rankings(tape)
    if savers:
        questions.append(f"Does {savers[0]['bucket']} remain the top loser saver on the next clean day?")
    for row in decision_confidence_meter(day_iso, tape, state=state)[:4]:
        if row.get('stage') in ('weak_hypothesis', 'building_evidence', 'strong_candidate'):
            questions.append(f"Does {row.get('name')} improve losers without damaging winners?")
    if brief.get('tomorrow_watch'):
        questions.append(f"Does the top watch item repeat: {brief['tomorrow_watch'][0]}?")
    fit = market_regime_fit(day_iso, tape, state=state)
    questions.append(f"Was this strategy actually fit for the regime, or was the day {fit.get('fit')}?")
    unique = []
    for q in questions:
        if q not in unique:
            unique.append(q)
    return unique[:6]


def coach_verdict(day_iso, flags, tape, state=None):
    quality = postmortem_quality_score(day_iso, flags, tape, state=state)
    total = sum(float(r.get('pnl') or 0) for r in tape or [])
    winners = [r for r in tape or [] if r.get('pnl', 0) > 0]
    losers = [r for r in tape or [] if r.get('pnl', 0) < 0]
    savers = loser_saver_rankings(tape)
    fit = market_regime_fit(day_iso, tape, state=state)
    if quality.get('label') == 'do_not_learn_strategy':
        label = 'Data/ops contaminated the day; do not learn strategy from it.'
    elif not tape:
        label = 'No trades; evaluate readiness, feed health, and why-no-trade data.'
    elif total > 0 and len(winners) >= len(losers):
        label = 'Bot traded well; review whether exits captured enough of the move.'
    elif savers and savers[0]['bucket'] == 'faster_or_smarter_exit':
        label = 'Bot found entries, but exits need review.'
    elif savers and savers[0]['bucket'] == 'execution_or_spread_block':
        label = 'Bot had trade ideas, but execution/spread quality drove too much damage.'
    elif fit.get('fit') == 'hostile':
        label = 'Bot fought the day/regime; learn cautiously.'
    else:
        label = 'Bot traded okay but needs more evidence before rule changes.'
    return {
        'label': label,
        'pnl': round(total, 2),
        'record': f"{len(winners)}W/{len(losers)}L",
        'postmortem_quality': quality.get('label'),
        'market_regime_fit': fit.get('fit'),
        'primary_saver': savers[0] if savers else None,
        'instruction': 'Treat this as a coaching verdict, not an automatic config change.',
    }


def build_discipline_layer(day_iso, flags, tape, state=None):
    return {
        'coach_verdict': coach_verdict(day_iso, flags, tape, state=state),
        'do_not_overfit': do_not_overfit_box(day_iso, flags, tape, state=state),
        'hypothesis_aging': hypothesis_aging(day_iso, tape),
        'rule_promotion_checklist': rule_promotion_checklist(day_iso, flags, tape, state=state),
        'market_regime_fit': market_regime_fit(day_iso, tape, state=state),
        'top_research_questions': top_research_questions(day_iso, flags, tape, state=state),
        'postmortem_quality_score': postmortem_quality_score(day_iso, flags, tape, state=state),
    }


def trust_today_flag(day_iso, flags, tape, state=None):
    quality = postmortem_quality_score(day_iso, flags, tape, state=state)
    label = quality.get('label')
    if label == 'high_trust':
        trust = 'YES'
    elif label in ('usable_with_caveats', 'review_only'):
        trust = 'PARTIAL'
    else:
        trust = 'NO'
    return {
        'trust': trust,
        'quality_label': label,
        'quality_score': quality.get('score'),
        'reason': '; '.join(
            c.get('detail') for c in quality.get('checks', [])
            if not c.get('ok') and c.get('detail')
        ) or 'all core quality checks passed',
    }


def one_page_daily_scorecard(day_iso, flags, tape, state=None):
    winners = [r for r in tape or [] if r.get('pnl', 0) > 0]
    losers = [r for r in tape or [] if r.get('pnl', 0) < 0]
    total = round(sum(float(r.get('pnl') or 0) for r in tape or []), 2)
    gross_w = sum(float(r.get('pnl') or 0) for r in winners)
    gross_l = abs(sum(float(r.get('pnl') or 0) for r in losers))
    setups = aggregate_by_setup(tape or [])
    best_setup = max(setups, key=lambda r: r.get('pnl', 0), default=None)
    worst_setup = min(setups, key=lambda r: r.get('pnl', 0), default=None)
    largest_loser = min(losers, key=lambda r: r.get('pnl', 0), default=None)
    largest_winner = max(winners, key=lambda r: r.get('pnl', 0), default=None)
    lifecycle = build_learning_lifecycle(day_iso, tape, state=state)
    hyp_aging = hypothesis_aging(day_iso, tape)
    return {
        'date_iso': day_iso,
        'trust_today_for_learning': trust_today_flag(day_iso, flags, tape, state=state),
        'pnl': total,
        'record': f'{len(winners)}W/{len(losers)}L',
        'win_rate': round(100 * len(winners) / len(tape), 1) if tape else None,
        'profit_factor': round(gross_w / gross_l, 3) if gross_l else None,
        'largest_loser': slim_detail_row(largest_loser) if largest_loser else None,
        'largest_winner': slim_detail_row(largest_winner) if largest_winner else None,
        'best_setup': best_setup,
        'worst_setup': worst_setup,
        'coach_verdict': coach_verdict(day_iso, flags, tape, state=state),
        'postmortem_quality_score': postmortem_quality_score(day_iso, flags, tape, state=state),
        'hypotheses': {
            'building_or_promoted': [
                h for h in hyp_aging if h.get('trend') in ('building', 'strengthening')
            ][:6],
            'watch_or_kill': [
                h for h in hyp_aging if h.get('trend') in ('new_or_unproven', 'stale_or_fading')
            ][:6],
            'candidate_changes': lifecycle.get('candidate_changes', [])[:6],
        },
    }


def trade_quality_label(row):
    pnl = float(row.get('pnl') or 0)
    if pnl < 0:
        primary, tags, _ = loser_diagnosis(row)
        grade = decision_grade(row, tags)
        quality = 'good_trade' if grade.get('grade') in ('A', 'B') else 'bad_trade'
        return {
            'quality': quality,
            'outcome': 'lost',
            'label': grade.get('label'),
            'grade': grade.get('grade'),
            'primary': primary,
        }
    if pnl > 0:
        q = winner_quality_label(row)
        bad_labels = {'lucky_reversal', 'btc_conflict_winner', 'wide_spread_winner'}
        quality = 'bad_trade' if q.get('label') in bad_labels else 'good_trade'
        return {
            'quality': quality,
            'outcome': 'won',
            'label': q.get('label'),
            'grade': 'A' if quality == 'good_trade' else 'C',
            'primary': q.get('label'),
        }
    return {'quality': 'neutral_trade', 'outcome': 'flat', 'label': 'flat', 'grade': 'B', 'primary': 'flat'}


def trade_quality_outcome_split(tape):
    buckets = {
        'good_trade_won': {'trades': 0, 'pnl': 0.0, 'examples': []},
        'good_trade_lost': {'trades': 0, 'pnl': 0.0, 'examples': []},
        'bad_trade_won': {'trades': 0, 'pnl': 0.0, 'examples': []},
        'bad_trade_lost': {'trades': 0, 'pnl': 0.0, 'examples': []},
        'neutral_trade_flat': {'trades': 0, 'pnl': 0.0, 'examples': []},
    }
    rows = []
    for row in tape or []:
        q = trade_quality_label(row)
        key = f"{q['quality']}_{q['outcome']}"
        if key not in buckets:
            key = 'neutral_trade_flat'
        pnl = float(row.get('pnl') or 0)
        buckets[key]['trades'] += 1
        buckets[key]['pnl'] += pnl
        if len(buckets[key]['examples']) < 5:
            buckets[key]['examples'].append(f"{row.get('time')} {row.get('ticker')} {row.get('side')} ${pnl:+.2f} {q.get('label')}")
        rows.append({
            'time': row.get('time'),
            'ticker': row.get('ticker'),
            'side': row.get('side'),
            'pnl': row.get('pnl'),
            'setup_type': setup_type(row),
            **q,
        })
    for b in buckets.values():
        b['pnl'] = round(b['pnl'], 2)
    return {'buckets': buckets, 'rows': rows}


def best_loser_worst_winner(tape):
    losers = [r for r in tape or [] if r.get('pnl', 0) < 0]
    winners = [r for r in tape or [] if r.get('pnl', 0) > 0]
    loser_rows = []
    for r in losers:
        q = trade_quality_label(r)
        # Prefer valid losses with small damage.
        score = (0 if q.get('quality') == 'good_trade' else 1, abs(float(r.get('pnl') or 0)))
        loser_rows.append((score, r, q))
    winner_rows = []
    for r in winners:
        q = trade_quality_label(r)
        # Prefer questionable winners with larger P&L because they can hide weak logic.
        score = (0 if q.get('quality') == 'bad_trade' else 1, -float(r.get('pnl') or 0))
        winner_rows.append((score, r, q))
    best = min(loser_rows, key=lambda x: x[0], default=None)
    worst = min(winner_rows, key=lambda x: x[0], default=None)
    return {
        'best_loser': {
            'trade': slim_detail_row(best[1]),
            'quality': best[2],
            'why': 'Acceptable/valid loss or smallest questionable loss; do not overfix this pattern.',
        } if best else None,
        'worst_winner': {
            'trade': slim_detail_row(worst[1]),
            'quality': worst[2],
            'why': 'Winner with questionable context; inspect so lucky wins do not reinforce bad logic.',
        } if worst else None,
    }


def pattern_replay_request_list(day_iso, flags, tape, state=None):
    requests = []
    savers = loser_saver_rankings(tape)
    for row in savers[:3]:
        requests.append({
            'type': 'loser_pattern_replay',
            'request': f"Replay {row['bucket']} examples",
            'examples': row.get('examples', []),
            'reason': f"{row['losers']} loser(s), gross_loss=${row['gross_loss']:.2f}",
        })
    split = trade_quality_outcome_split(tape)
    bad_winners = [r for r in split['rows'] if r.get('quality') == 'bad_trade' and r.get('outcome') == 'won']
    if bad_winners:
        requests.append({
            'type': 'bad_winner_review',
            'request': 'Inspect bad winners before trusting their setup bucket',
            'examples': [
                f"{r.get('time')} {r.get('ticker')} {r.get('side')} ${r.get('pnl'):+.2f} {r.get('label')}"
                for r in bad_winners[:5]
            ],
            'reason': 'Winning P&L can hide weak entry logic.',
        })
    for row in decision_confidence_meter(day_iso, tape, state=state)[:3]:
        requests.append({
            'type': 'hypothesis_replay',
            'request': f"Compare recent trades against {row.get('name')}",
            'examples': [],
            'reason': row.get('next_step'),
        })
    return requests[:8]


def rolling_bot_personality(day_iso, lookback=10):
    payloads = prior_postmortem_payloads(day_iso, max_days=lookback)
    current_path = os.path.join(OUT_DIR, f'postmortem_{day_iso}.json')
    try:
        if os.path.exists(current_path):
            payloads.append(json.load(open(current_path, encoding='utf-8')))
    except Exception:
        pass
    rows = []
    for p in payloads:
        for r in p.get('tape', []) or []:
            rows.append(r)

    def aggregate(key_fn):
        groups = {}
        for r in rows:
            key = key_fn(r) or 'unknown'
            g = groups.setdefault(key, {'key': key, 'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0})
            pnl = float(r.get('pnl') or 0)
            g['trades'] += 1
            g['wins'] += 1 if pnl > 0 else 0
            g['losses'] += 1 if pnl < 0 else 0
            g['pnl'] += pnl
        out = []
        for g in groups.values():
            g['pnl'] = round(g['pnl'], 2)
            g['win_rate'] = round(100 * g['wins'] / g['trades'], 1) if g['trades'] else None
            out.append(g)
        return sorted(out, key=lambda x: (-x['pnl'], -x['trades']))

    def session_phase(r):
        try:
            hour = int(str(r.get('time', '00:00')).split(':')[0])
        except Exception:
            return 'unknown'
        if hour < 10:
            return 'open'
        if hour < 13:
            return 'midday'
        return 'afternoon'

    return {
        'lookback_days': lookback,
        'trades': len(rows),
        'by_side': aggregate(lambda r: r.get('side')),
        'by_ticker': aggregate(lambda r: r.get('ticker')),
        'by_setup': aggregate(setup_type),
        'by_session_phase': aggregate(session_phase),
        'deduction': 'Use this to describe where the bot naturally performs best/worst before changing rules.',
    }


def weekly_synthesis(day_iso, lookback=5):
    payloads = prior_postmortem_payloads(day_iso, max_days=lookback)
    current_path = os.path.join(OUT_DIR, f'postmortem_{day_iso}.json')
    try:
        if os.path.exists(current_path):
            payloads.append(json.load(open(current_path, encoding='utf-8')))
    except Exception:
        pass
    trades = [r for p in payloads for r in (p.get('tape', []) or [])]
    personality = rolling_bot_personality(day_iso, lookback=lookback)
    all_hyp = []
    promoted = []
    killed = []
    for p in payloads:
        dl = p.get('discipline_layer') or {}
        all_hyp.extend((dl.get('hypothesis_aging') or []))
        for c in (p.get('learning_lifecycle') or {}).get('candidate_changes', []) or []:
            if c.get('promotion_status') == 'eligible_for_human_review':
                promoted.append(c)
        for h in dl.get('hypothesis_aging') or []:
            if h.get('trend') == 'stale_or_fading':
                killed.append(h)
    setups = personality.get('by_setup') or []
    weakness = min(setups, key=lambda r: r.get('pnl', 0), default=None)
    edge = max(setups, key=lambda r: r.get('pnl', 0), default=None)
    return {
        'lookback_days': lookback,
        'days_loaded': [p.get('date_iso') for p in payloads if p.get('date_iso')],
        'trades': len(trades),
        'top_proven_or_observed_edge': edge,
        'top_weakness': weakness,
        'killed_or_stale_hypotheses': killed[:8],
        'promoted_or_eligible_candidates': promoted[:8],
        'next_week_watchlist': [
            h.get('monitor') or h.get('id')
            for h in sorted(all_hyp, key=lambda x: -int(x.get('evidence_days') or 0))[:8]
        ],
    }


def trade_archetype(row):
    side = row.get('side') or 'UNKNOWN'
    setup = setup_type(row)
    f = row.get('forensics') or {}
    btc = f.get('btc') or {}
    rel = f.get('relative_strength') or {}
    ind = row.get('ind') or {}
    flow_120 = ind.get('flow_120s')
    agreement = btc.get('agreement')
    conflict = btc.get('conflict')
    rs = rel.get('stock_minus_btc_implied_60s')
    try:
        rs = float(rs) if rs is not None else None
    except Exception:
        rs = None
    try:
        flow_120 = float(flow_120) if flow_120 is not None else None
    except Exception:
        flow_120 = None

    if conflict:
        return f'{side.lower()}_against_btc_conflict'
    if agreement is True:
        if side == 'LONG':
            return 'btc_aligned_momentum_long'
        if side == 'SHORT':
            return 'btc_aligned_continuation_short'
    if rs is not None:
        if side == 'LONG' and rs > 0.10:
            return 'miner_relative_strength_long'
        if side == 'SHORT' and rs < -0.10:
            return 'miner_relative_weakness_short'
        if side == 'SHORT' and rs > 0.10:
            return 'failed_short_into_resilient_miner'
        if side == 'LONG' and rs < -0.10:
            return 'failed_long_into_weak_miner'
    if side == 'SHORT' and flow_120 is not None and flow_120 >= 60:
        return 'flow_exhaustion_short_candidate'
    if side == 'LONG' and flow_120 is not None and flow_120 <= 40:
        return 'flow_reversal_long_candidate'
    if setup and setup != 'unknown':
        return f'{side.lower()}_{setup}'
    return f'{side.lower()}_chop_or_unclear_edge'


def trade_archetype_grades(tape):
    groups = {}
    for row in tape:
        key = trade_archetype(row)
        g = groups.setdefault(key, {
            'archetype': key,
            'trades': 0,
            'wins': 0,
            'losses': 0,
            'pnl': 0.0,
            'gross_win': 0.0,
            'gross_loss': 0.0,
            'mfe_values': [],
            'mae_values': [],
            'examples': [],
        })
        pnl = float(row.get('pnl') or 0)
        g['trades'] += 1
        g['wins'] += 1 if pnl > 0 else 0
        g['losses'] += 1 if pnl < 0 else 0
        g['pnl'] += pnl
        if pnl > 0:
            g['gross_win'] += pnl
        elif pnl < 0:
            g['gross_loss'] += abs(pnl)
        if row.get('mfe_pct') is not None:
            g['mfe_values'].append(float(row.get('mfe_pct') or 0))
        if row.get('mae_pct') is not None:
            g['mae_values'].append(float(row.get('mae_pct') or 0))
        if len(g['examples']) < 5:
            g['examples'].append(
                f"{row.get('time')} {row.get('ticker')} {row.get('side')} ${pnl:+.2f}"
            )

    rows = []
    for g in groups.values():
        trades = g['trades'] or 1
        avg_loss = g['gross_loss'] / g['losses'] if g['losses'] else 0.0
        avg_win = g['gross_win'] / g['wins'] if g['wins'] else 0.0
        row = {
            'archetype': g['archetype'],
            'trades': g['trades'],
            'wins': g['wins'],
            'losses': g['losses'],
            'win_rate': round(100 * g['wins'] / trades, 1),
            'pnl': round(g['pnl'], 2),
            'avg_win': round(avg_win, 2),
            'avg_loss': round(avg_loss, 2),
            'avg_mfe_pct': round(sum(g['mfe_values']) / len(g['mfe_values']), 3) if g['mfe_values'] else None,
            'avg_mae_pct': round(sum(g['mae_values']) / len(g['mae_values']), 3) if g['mae_values'] else None,
            'examples': g['examples'],
        }
        if row['trades'] < 3:
            row['grade'] = 'watch'
        elif row['pnl'] > 0 and row['win_rate'] >= 55:
            row['grade'] = 'working'
        elif row['pnl'] < 0 and row['win_rate'] <= 45:
            row['grade'] = 'weak'
        else:
            row['grade'] = 'mixed'
        rows.append(row)
    return sorted(rows, key=lambda r: (-r['pnl'], -r['trades']))


def btc_relationship_label(row):
    f = row.get('forensics') or {}
    btc = f.get('btc') or {}
    rel = f.get('relative_strength') or {}
    side = row.get('side')
    if btc.get('stale') or btc.get('ready') is False:
        return 'btc_context_stale_or_unready'
    if btc.get('conflict'):
        return 'btc_conflicted_against_trade'
    if btc.get('agreement') is True:
        return 'btc_confirmed_trade'
    if btc.get('agreement') is False:
        return 'btc_not_confirming'
    rs = rel.get('stock_minus_btc_implied_60s')
    try:
        rs = float(rs) if rs is not None else None
    except Exception:
        rs = None
    if rs is not None:
        if side == 'LONG' and rs > 0:
            return 'stock_led_btc_higher'
        if side == 'SHORT' and rs < 0:
            return 'stock_led_btc_lower'
        if side == 'LONG' and rs < 0:
            return 'stock_lagged_btc_for_long'
        if side == 'SHORT' and rs > 0:
            return 'stock_resisted_btc_for_short'
    return 'btc_relationship_unclear'


def btc_relationship_analysis(tape):
    groups = {}
    for row in tape:
        key = btc_relationship_label(row)
        g = groups.setdefault(key, {'label': key, 'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0, 'examples': []})
        pnl = float(row.get('pnl') or 0)
        g['trades'] += 1
        g['wins'] += 1 if pnl > 0 else 0
        g['losses'] += 1 if pnl < 0 else 0
        g['pnl'] += pnl
        if len(g['examples']) < 5:
            g['examples'].append(f"{row.get('time')} {row.get('ticker')} {row.get('side')} ${pnl:+.2f}")
    rows = []
    for g in groups.values():
        trades = g['trades'] or 1
        g['pnl'] = round(g['pnl'], 2)
        g['win_rate'] = round(100 * g['wins'] / trades, 1)
        rows.append(g)
    return sorted(rows, key=lambda r: (-abs(r['pnl']), -r['trades']))


def loser_root_cause_buckets(tape):
    buckets = {}
    for item in build_loser_review(tape):
        key = item.get('primary') or 'unclassified'
        row = item.get('row') or {}
        loss = abs(float(row.get('pnl') or 0))
        b = buckets.setdefault(key, {
            'root_cause': key,
            'losers': 0,
            'gross_loss': 0.0,
            'examples': [],
            'evidence': Counter(),
        })
        b['losers'] += 1
        b['gross_loss'] += loss
        if len(b['examples']) < 5:
            b['examples'].append(f"{row.get('time')} {row.get('ticker')} {row.get('side')} ${row.get('pnl'):+.2f}")
        for tag in item.get('tags') or []:
            b['evidence'][tag] += 1
    out = []
    for b in buckets.values():
        out.append({
            'root_cause': b['root_cause'],
            'losers': b['losers'],
            'gross_loss': round(b['gross_loss'], 2),
            'top_evidence_tags': [k for k, _ in b['evidence'].most_common(5)],
            'examples': b['examples'],
        })
    return sorted(out, key=lambda r: (-r['gross_loss'], -r['losers']))


def one_sentence_trade_lesson(row):
    pnl = float(row.get('pnl') or 0)
    if pnl < 0:
        primary, tags, evidence = loser_diagnosis(row)
        thesis = thesis_failure_label(row, tags)
        reason = evidence[0] if evidence else 'no single indicator explained the loss'
        return (
            f"{row.get('time')} {row.get('ticker')} {row.get('side')}: "
            f"loss lesson is {primary}; {reason}; classify as {thesis.get('label')}."
        )
    q = winner_quality_label(row)
    return (
        f"{row.get('time')} {row.get('ticker')} {row.get('side')}: "
        f"winner lesson is {q.get('label')}; repeat only when the same confirmation is present."
    )


def trade_lessons(tape):
    rows = []
    for row in sorted(tape, key=lambda r: (r.get('opened_at') or 0)):
        rows.append({
            'time': row.get('time'),
            'ticker': row.get('ticker'),
            'side': row.get('side'),
            'pnl': row.get('pnl'),
            'archetype': trade_archetype(row),
            'btc_relationship': btc_relationship_label(row),
            'lesson': one_sentence_trade_lesson(row),
        })
    return rows


def decision_replay(tape):
    rows = []
    for row in tape:
        timeline = trade_thesis_timeline(row)
        checkpoints = timeline.get('checkpoints') or []
        first_invalid = next((c for c in checkpoints if c.get('status') == 'invalidated'), None)
        first_confirm = next((c for c in checkpoints if c.get('status') == 'confirmed'), None)
        rows.append({
            'time': row.get('time'),
            'ticker': row.get('ticker'),
            'side': row.get('side'),
            'pnl': row.get('pnl'),
            'entry_quality': entry_quality_tier_from_row(row),
            'archetype': trade_archetype(row),
            'btc_relationship': btc_relationship_label(row),
            'verdict': timeline.get('verdict'),
            'first_confirmation': first_confirm,
            'first_invalidation': first_invalid,
            'deduction': (
                'Exit/entry invalidation showed up early; inspect for faster protection.'
                if first_invalid and (row.get('pnl') or 0) < 0
                else 'No early invalidation logged; treat outcome as normal variance until repeated.'
            ),
        })
    return rows


def rule_attribution_matrix(tape):
    rows = []
    exit_reasons = Counter(str(r.get('reason') or 'unknown') for r in tape)
    for reason, count in exit_reasons.most_common():
        reason_rows = [r for r in tape if str(r.get('reason') or 'unknown') == reason]
        pnl = round(sum(float(r.get('pnl') or 0) for r in reason_rows), 2)
        rows.append({
            'rule': f'exit:{reason}',
            'role': 'exit',
            'trades': count,
            'pnl': pnl,
            'deduction': 'helped' if pnl > 0 else ('hurt_or_needs_review' if pnl < 0 else 'neutral'),
            'examples': [f"{r.get('time')} {r.get('ticker')} {r.get('side')} ${r.get('pnl'):+.2f}" for r in reason_rows[:5]],
        })
    for rule in counterfactual_rules(tape):
        rows.append({
            'rule': rule.get('rule'),
            'role': 'entry_filter_candidate',
            'trades': rule.get('losers_avoided'),
            'pnl': rule.get('net_estimated_impact'),
            'deduction': (
                'candidate_helped_losers'
                if rule.get('net_estimated_impact', 0) > 0
                else 'candidate_may_damage_winners'
            ),
            'confidence': rule.get('confidence'),
            'examples': rule.get('evidence_trades') or [],
        })
    return sorted(rows, key=lambda r: (-abs(float(r.get('pnl') or 0)), str(r.get('rule'))))


def hypothesis_scoreboard(day_iso, tape, state=None):
    lifecycle = build_learning_lifecycle(day_iso, tape, state=state)
    aging = hypothesis_aging(day_iso, tape)
    rows = []
    for h in aging:
        evidence_days = int(h.get('evidence_days') or 0)
        contradicting = int(h.get('contradicting_days') or 0) if h.get('contradicting_days') is not None else 0
        confidence = _learning_confidence({'evidence_days': evidence_days, 'contradicting_days': contradicting})
        rows.append({
            'id': h.get('id'),
            'status': h.get('trend'),
            'evidence_days': evidence_days,
            'supporting_trades': h.get('supporting_trades') or h.get('support_count'),
            'contradicting_trades': h.get('contradicting_trades') or h.get('contradicting_count'),
            'confidence': confidence,
            'last_seen': h.get('last_seen'),
            'next_step': 'promote only after checklist passes' if evidence_days >= 2 else 'keep watching',
        })
    for c in lifecycle.get('candidate_changes') or []:
        rows.append({
            'id': c.get('name') or c.get('id'),
            'status': c.get('promotion_status'),
            'evidence_days': c.get('evidence_days'),
            'supporting_trades': c.get('supporting_trades'),
            'contradicting_trades': c.get('contradicting_trades'),
            'confidence': c.get('confidence'),
            'last_seen': c.get('last_seen'),
            'next_step': c.get('next_step') or 'human review required',
        })
    deduped = {}
    for row in rows:
        key = row.get('id') or 'unknown'
        existing = deduped.get(key)
        if not existing or int(row.get('evidence_days') or 0) > int(existing.get('evidence_days') or 0):
            deduped[key] = row
    return sorted(deduped.values(), key=lambda r: (-int(r.get('evidence_days') or 0), str(r.get('id'))))


def ultimate_learning_model(day_iso, flags, tape, state=None):
    return {
        'trade_archetype_grades': trade_archetype_grades(tape),
        'entry_quality_vs_outcome': trade_quality_outcome_split(tape),
        'decision_replay': decision_replay(tape),
        'missed_best_trades': skipped_opportunity_summary(state or {}, day_iso)[1][:10],
        'rule_attribution_matrix': rule_attribution_matrix(tape),
        'btc_relationship_analysis': btc_relationship_analysis(tape),
        'hypothesis_scoreboard': hypothesis_scoreboard(day_iso, tape, state=state),
        'loser_root_cause_buckets': loser_root_cause_buckets(tape),
        'one_sentence_trade_lessons': trade_lessons(tape),
        'monday_or_next_session_action_plan': next_session_plan(day_iso, flags, tape, state=state),
        'safety_rule': 'Use this to recommend changes, not to auto-modify live trading rules without human approval.',
    }


def render_ultimate_learning_model(day_iso, flags, tape, state=None):
    model = ultimate_learning_model(day_iso, flags, tape, state=state)
    out = ['Ultimate learning model', '-' * 78]

    out.append('  Trade archetype grading:')
    archetypes = model['trade_archetype_grades']
    if archetypes:
        for row in archetypes[:10]:
            out.append(
                f"    - {row['archetype']}: {row['grade']} trades={row['trades']} "
                f"win_rate={row['win_rate']}% pnl=${row['pnl']:+.2f} "
                f"avg_mfe={fmt_num(row.get('avg_mfe_pct'), 3, '%')} avg_mae={fmt_num(row.get('avg_mae_pct'), 3, '%')}"
            )
    else:
        out.append('    - No trades to grade.')

    out.append('  BTC leadership / lag analysis:')
    btc_rows = model['btc_relationship_analysis']
    if btc_rows:
        for row in btc_rows[:8]:
            out.append(
                f"    - {row['label']}: trades={row['trades']} win_rate={row['win_rate']}% "
                f"pnl=${row['pnl']:+.2f}"
            )
    else:
        out.append('    - No BTC relationship data available.')

    out.append('  Loser root-cause buckets:')
    roots = model['loser_root_cause_buckets']
    if roots:
        for row in roots[:8]:
            out.append(
                f"    - {row['root_cause']}: losers={row['losers']} "
                f"gross_loss=${row['gross_loss']:.2f} tags={', '.join(row['top_evidence_tags'])}"
            )
    else:
        out.append('    - No losing trades.')

    out.append('  Rule attribution:')
    for row in model['rule_attribution_matrix'][:10]:
        out.append(
            f"    - {row['rule']} ({row['role']}): {row['deduction']} "
            f"trades={row.get('trades')} net=${float(row.get('pnl') or 0):+.2f}"
        )

    out.append('  Decision replay callouts:')
    replay_rows = [r for r in model['decision_replay'] if (r.get('pnl') or 0) < 0]
    if replay_rows:
        for row in replay_rows[:8]:
            inv = row.get('first_invalidation') or {}
            inv_txt = f" first_invalid={inv.get('checkpoint')} {fmt_num(inv.get('signed_return_pct'), 3, '%')}" if inv else ''
            out.append(
                f"    - {row['time']} {row['ticker']} {row['side']}: "
                f"{row['verdict']} archetype={row['archetype']}{inv_txt}"
            )
    else:
        out.append('    - No losing trade replay callouts.')

    out.append('  Hypothesis scoreboard:')
    hrows = model['hypothesis_scoreboard']
    if hrows:
        for row in hrows[:10]:
            out.append(
                f"    - {row.get('id')}: status={row.get('status')} "
                f"evidence_days={row.get('evidence_days')} confidence={row.get('confidence')} "
                f"next={row.get('next_step')}"
            )
    else:
        out.append('    - No active hypotheses.')

    out.append('  One-sentence trade lessons:')
    lessons = model['one_sentence_trade_lessons']
    if lessons:
        for row in lessons[:12]:
            out.append(f"    - {row['lesson']}")
    else:
        out.append('    - No trades.')

    out.append('')
    return out


def red_yellow_green_status(day_iso, flags, tape, state=None):
    state = state or {}
    scorecard = one_page_daily_scorecard(day_iso, flags, tape, state=state)
    quality = postmortem_quality_score(day_iso, flags, tape, state=state)
    trust = trust_today_flag(day_iso, flags, tape, state=state)
    checklist = rule_promotion_checklist(day_iso, flags, tape, state=state)
    positions = (state or {}).get('positions') or {}
    broker_block = (state or {}).get('broker_exposure_block')

    def color(ok, warn=False):
        if ok:
            return 'green'
        if warn:
            return 'yellow'
        return 'red'

    pnl = float(scorecard.get('pnl') or 0)
    pf = scorecard.get('profit_factor')
    trading_color = color(pnl > 0 and (pf is None or pf >= 1.0), warn=len(tape or []) > 0)
    data_color = color(quality.get('label') == 'high_trust', warn=quality.get('label') in ('usable_with_caveats', 'review_only'))
    trust_color = {'YES': 'green', 'PARTIAL': 'yellow', 'NO': 'red'}.get(trust.get('trust'), 'red')
    promotion_color = 'yellow' if any(r.get('status') in ('promotion_ready', 'close') for r in checklist) else 'green'
    broker_color = color(not positions and not broker_block, warn=bool(positions) and not broker_block)
    return {
        'trading_performance': {
            'status': trading_color,
            'detail': f"pnl=${pnl:+.2f}, record={scorecard.get('record')}, PF={pf}",
        },
        'data_quality': {
            'status': data_color,
            'detail': f"{quality.get('score')}/100 {quality.get('label')}",
        },
        'learning_trust': {
            'status': trust_color,
            'detail': f"{trust.get('trust')} - {trust.get('reason')}",
        },
        'rule_promotion_readiness': {
            'status': promotion_color,
            'detail': (
                'candidate near promotion; review checklist'
                if promotion_color == 'yellow'
                else 'no action-ready rule changes'
            ),
        },
        'broker_ops_safety': {
            'status': broker_color,
            'detail': f"positions={list(positions.keys())}, broker_block={broker_block}",
        },
    }


def next_session_plan(day_iso, flags, tape, state=None):
    watch = next_session_watch_card(day_iso, flags, tape, state=state)
    quality = postmortem_quality_score(day_iso, flags, tape, state=state)
    checklist = rule_promotion_checklist(day_iso, flags, tape, state=state)
    close_candidates = [r for r in checklist if r.get('status') in ('promotion_ready', 'close')]
    allowed = [
        'Keep trading the current live config unless broker/ops safety blocks fire.',
        'Keep collecting skipped, near-signal, trade-quality, and winner-damage evidence.',
        'Treat same-day counterfactuals as research, not live-rule changes.',
    ]
    watch_items = list(watch.get('top_watch_items') or [])
    if close_candidates:
        watch_items.append(f"Promotion checklist watch: {close_candidates[0].get('rule')} ({close_candidates[0].get('status')})")
    change_triggers = [
        'A pattern repeats across multiple clean days with positive net impact.',
        'Winner damage remains low or acceptable after replay.',
        'The change maps cleanly to an engine/config rule and has a rollback plan.',
    ]
    if quality.get('label') not in ('high_trust', 'usable_with_caveats'):
        change_triggers.insert(0, 'First improve data/ops quality enough to trust strategy conclusions.')
    return {
        'allowed_to_keep_doing': allowed,
        'watch_carefully': watch_items[:6] or ['No special watch items beyond normal monitoring.'],
        'consider_change_after_close_if': change_triggers,
        'do_not_do': [
            'Do not promote a rule from one bad trade.',
            'Do not block a setup if winner damage is high.',
            'Do not learn strategy from an ops-contaminated day.',
        ],
    }


def human_approval_checklist(day_iso, flags, tape, state=None):
    rows = []
    recs = strong_considerations(day_iso, tape, state=state)
    promotion_rows = rule_promotion_checklist(day_iso, flags, tape, state=state)
    risk_by_rule = {r.get('rule'): r for r in winner_damage_risk(tape)}
    feedback_by_rule = {r.get('rule'): r for r in feedback_map(day_iso, tape)}
    for row in promotion_rows:
        if row.get('status') not in ('promotion_ready', 'close'):
            continue
        rule = row.get('rule')
        risk = risk_by_rule.get(rule, {})
        feedback = feedback_by_rule.get(rule, {})
        rows.append({
            'suggested_change': rule,
            'why_now': f"{row.get('passed')}/{row.get('total')} promotion checks passed",
            'evidence': risk,
            'winner_damage_risk': risk.get('risk'),
            'implementation_surface': feedback.get('file_or_surface'),
            'how_to_revert': 'Restore the prior trading_config/code value from config_diff/config_freeze and restart app.py.',
            'post_enable_watch': [
                'winner damage',
                'false positives blocked',
                'loser average loss',
                'broker/execution quality',
            ],
            'human_approval_required': True,
        })
    if recs and not rows:
        for rec in recs[:5]:
            rows.append({
                'suggested_change': rec,
                'why_now': 'Strong consideration text was generated; map to a concrete rule before implementation.',
                'evidence': {},
                'winner_damage_risk': 'unknown_until_mapped',
                'implementation_surface': 'manual_mapping_required',
                'how_to_revert': 'Document exact change before applying; revert that change if post-enable watch fails.',
                'post_enable_watch': ['net P&L delta', 'winner damage', 'loser recurrence'],
                'human_approval_required': True,
            })
    return rows


def render_next_session_plan(day_iso, flags, tape, state=None):
    plan = next_session_plan(day_iso, flags, tape, state=state)
    out = ['Next session plan', '-' * 78]
    out.append('  Allowed to keep doing:')
    for item in plan['allowed_to_keep_doing']:
        out.append(f'    - {item}')
    out.append('  Watch carefully:')
    for item in plan['watch_carefully']:
        out.append(f'    - {item}')
    out.append('  Consider a change after close if:')
    for item in plan['consider_change_after_close_if']:
        out.append(f'    - {item}')
    out.append('  Do not do:')
    for item in plan['do_not_do']:
        out.append(f'    - {item}')
    out.append('')
    return out


def render_ryg_status(day_iso, flags, tape, state=None):
    statuses = red_yellow_green_status(day_iso, flags, tape, state=state)
    out = ['Red / yellow / green status', '-' * 78]
    for name, row in statuses.items():
        out.append(f"  {name}: {row.get('status').upper()} - {row.get('detail')}")
    out.append('')
    return out


def render_human_approval_checklist(day_iso, flags, tape, state=None):
    rows = human_approval_checklist(day_iso, flags, tape, state=state)
    out = ['Human approval checklist', '-' * 78]
    if not rows:
        out.append('  No action candidate currently needs human approval.')
        out.append('  If a red suggested action appears later, this section will show evidence, winner damage, revert path, and post-enable watch items.')
    else:
        for row in rows:
            out.append(f"  - suggested change: {row.get('suggested_change')}")
            out.append(f"    why now: {row.get('why_now')}")
            out.append(f"    winner damage risk: {row.get('winner_damage_risk')}")
            out.append(f"    implementation surface: {row.get('implementation_surface')}")
            out.append(f"    how to revert: {row.get('how_to_revert')}")
            out.append(f"    post-enable watch: {', '.join(row.get('post_enable_watch') or [])}")
    out.append('')
    return out


def render_start_here_text(day_iso, flags, tape, state=None):
    trust = trust_today_flag(day_iso, flags, tape, state=state)
    score = one_page_daily_scorecard(day_iso, flags, tape, state=state)
    brief = build_daily_learning_brief(day_iso, flags, tape, state=state)
    discipline = build_discipline_layer(day_iso, flags, tape, state=state)
    statuses = red_yellow_green_status(day_iso, flags, tape, state=state)
    plan = next_session_plan(day_iso, flags, tape, state=state)
    approval = human_approval_checklist(day_iso, flags, tape, state=state)
    out = []
    out.append(f"POSTMORTEM START HERE {day_iso}")
    out.append('=' * 78)
    out.append(f"TRUST TODAY FOR LEARNING: {trust['trust']} ({trust['quality_score']}/100 {trust['quality_label']})")
    out.append(f"Reason: {trust['reason']}")
    out.append('')
    out.append(f"Coach verdict: {discipline['coach_verdict']['label']}")
    out.append(f"Scorecard: pnl=${score['pnl']:+.2f} record={score['record']} win_rate={score['win_rate']}% PF={score['profit_factor']}")
    out.append('')
    out.append('Red/yellow/green:')
    for name, row in statuses.items():
        out.append(f"  - {name}: {row['status'].upper()} - {row['detail']}")
    out.append('')
    out.append('What worked:')
    for item in brief.get('what_worked', [])[:4]:
        out.append(f'  - {item}')
    out.append("What didn't work:")
    for item in brief.get('what_failed', [])[:4]:
        out.append(f'  - {item}')
    out.append('')
    out.append('Watch next session:')
    for item in plan.get('watch_carefully', [])[:6]:
        out.append(f'  - {item}')
    out.append('')
    out.append('Do not overfit:')
    for item in discipline.get('do_not_overfit', [])[:6]:
        out.append(f'  - {item}')
    out.append('')
    out.append('Human approval needed:')
    if approval:
        for row in approval[:5]:
            out.append(f"  - {row.get('suggested_change')} ({row.get('winner_damage_risk')})")
    else:
        out.append('  - None.')
    out.append('')
    out.append('Fast learning callouts:')
    ultimate = ultimate_learning_model(day_iso, flags, tape, state=state)
    for row in (ultimate.get('loser_root_cause_buckets') or [])[:3]:
        out.append(f"  - Root cause: {row['root_cause']} losers={row['losers']} gross_loss=${row['gross_loss']:.2f}")
    for row in (ultimate.get('trade_archetype_grades') or [])[:3]:
        out.append(f"  - Archetype: {row['archetype']} {row['grade']} pnl=${row['pnl']:+.2f}")
    if not (ultimate.get('loser_root_cause_buckets') or ultimate.get('trade_archetype_grades')):
        out.append('  - No trade/archetype learning callouts yet.')
    out.append('')
    out.append(f"Full report: {os.path.join(OUT_DIR, f'postmortem_{day_iso}.txt')}")
    return '\n'.join(out) + '\n'


def write_weekly_learning_summary(day_iso):
    payload = weekly_synthesis(day_iso)
    json_path = os.path.join(OUT_DIR, f'WEEKLY_LEARNING_SUMMARY_{day_iso}.json')
    txt_path = os.path.join(OUT_DIR, f'WEEKLY_LEARNING_SUMMARY_{day_iso}.txt')
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, default=str)
    lines = [f'WEEKLY LEARNING SUMMARY {day_iso}', '=' * 78]
    lines.append(f"lookback_days={payload.get('lookback_days')} trades={payload.get('trades')}")
    edge = payload.get('top_proven_or_observed_edge') or {}
    weak = payload.get('top_weakness') or {}
    if edge:
        lines.append(f"top observed edge: {edge.get('key')} pnl=${edge.get('pnl'):+.2f} win_rate={edge.get('win_rate')}%")
    if weak:
        lines.append(f"top weakness: {weak.get('key')} pnl=${weak.get('pnl'):+.2f} win_rate={weak.get('win_rate')}%")
    lines.append('next week watchlist:')
    for item in payload.get('next_week_watchlist') or []:
        lines.append(f'  - {item}')
    lines.append('eligible/promoted candidates:')
    for item in payload.get('promoted_or_eligible_candidates') or []:
        lines.append(f"  - {item.get('key') or item.get('name')}: {item.get('promotion_status')}")
    with open(txt_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')
    return json_path, txt_path, payload


def build_advanced_learning_layer(day_iso, flags, tape, state=None):
    return {
        'trust_today_for_learning': trust_today_flag(day_iso, flags, tape, state=state),
        'red_yellow_green_status': red_yellow_green_status(day_iso, flags, tape, state=state),
        'next_session_plan': next_session_plan(day_iso, flags, tape, state=state),
        'human_approval_checklist': human_approval_checklist(day_iso, flags, tape, state=state),
        'ultimate_learning_model': ultimate_learning_model(day_iso, flags, tape, state=state),
        'one_page_daily_scorecard': one_page_daily_scorecard(day_iso, flags, tape, state=state),
        'pattern_replay_request_list': pattern_replay_request_list(day_iso, flags, tape, state=state),
        'trade_quality_outcome_split': trade_quality_outcome_split(tape),
        'best_loser_worst_winner': best_loser_worst_winner(tape),
        'rolling_bot_personality': rolling_bot_personality(day_iso),
        'weekly_synthesis': weekly_synthesis(day_iso),
    }


def render_advanced_learning_layer(day_iso, flags, tape, state=None):
    layer = build_advanced_learning_layer(day_iso, flags, tape, state=state)
    out = ['Advanced learning layer', '-' * 78]
    score = layer['one_page_daily_scorecard']
    trust = layer['trust_today_for_learning']
    out.append(f"  Trust today for learning: {trust['trust']} ({trust['quality_score']}/100 {trust['quality_label']})")
    out.append(f"    reason: {trust['reason']}")
    out.append(
        f"  One-page scorecard: pnl=${score['pnl']:+.2f} record={score['record']} "
        f"win_rate={score['win_rate']}% PF={score['profit_factor']}"
    )
    if score.get('best_setup'):
        out.append(f"    best setup: {score['best_setup'].get('setup')} pnl=${score['best_setup'].get('pnl'):+.2f}")
    if score.get('worst_setup'):
        out.append(f"    worst setup: {score['worst_setup'].get('setup')} pnl=${score['worst_setup'].get('pnl'):+.2f}")

    out.append('  Trade quality vs outcome:')
    for key, row in layer['trade_quality_outcome_split']['buckets'].items():
        if row['trades']:
            out.append(f"    - {key}: trades={row['trades']} pnl=${row['pnl']:+.2f} examples={'; '.join(row['examples'][:3])}")

    bw = layer['best_loser_worst_winner']
    if bw.get('best_loser'):
        t = bw['best_loser']['trade']
        q = bw['best_loser']['quality']
        out.append(f"  Best loser: {t.get('time')} {t.get('ticker')} {t.get('side')} pnl=${t.get('pnl'):+.2f} ({q.get('label')})")
    if bw.get('worst_winner'):
        t = bw['worst_winner']['trade']
        q = bw['worst_winner']['quality']
        out.append(f"  Worst winner: {t.get('time')} {t.get('ticker')} {t.get('side')} pnl=${t.get('pnl'):+.2f} ({q.get('label')})")

    out.append('  Pattern replay request list:')
    for req in layer['pattern_replay_request_list'][:6]:
        out.append(f"    - {req['request']}: {req['reason']}")
        if req.get('examples'):
            out.append(f"      examples: {'; '.join(req['examples'][:3])}")

    personality = layer['rolling_bot_personality']
    out.append(f"  Rolling bot personality ({personality['lookback_days']}d, {personality['trades']} trades):")
    for name, rows in (('side', personality['by_side']), ('ticker', personality['by_ticker']), ('setup', personality['by_setup']), ('phase', personality['by_session_phase'])):
        top = rows[0] if rows else None
        bottom = rows[-1] if rows else None
        if top:
            out.append(f"    - best {name}: {top['key']} pnl=${top['pnl']:+.2f} win_rate={top['win_rate']}%")
        if bottom and bottom != top:
            out.append(f"      weakest {name}: {bottom['key']} pnl=${bottom['pnl']:+.2f} win_rate={bottom['win_rate']}%")

    weekly = layer['weekly_synthesis']
    out.append(f"  Weekly synthesis ({weekly['lookback_days']}d): trades={weekly['trades']}")
    if weekly.get('top_proven_or_observed_edge'):
        e = weekly['top_proven_or_observed_edge']
        out.append(f"    - top observed edge: {e.get('key')} pnl=${e.get('pnl'):+.2f}")
    if weekly.get('top_weakness'):
        w = weekly['top_weakness']
        out.append(f"    - top weakness: {w.get('key')} pnl=${w.get('pnl'):+.2f}")
    if weekly.get('next_week_watchlist'):
        out.append('    - next week watchlist:')
        for item in weekly['next_week_watchlist'][:5]:
            out.append(f'      {item}')
    out.append('')
    return out


def render_discipline_layer(day_iso, flags, tape, state=None):
    layer = build_discipline_layer(day_iso, flags, tape, state=state)
    out = ['Discipline layer', '-' * 78]
    verdict = layer['coach_verdict']
    out.append(f"  Coach verdict: {verdict['label']}")
    out.append(
        f"    record={verdict['record']} pnl=${verdict['pnl']:+.2f} "
        f"quality={verdict['postmortem_quality']} regime_fit={verdict['market_regime_fit']}"
    )

    quality = layer['postmortem_quality_score']
    out.append(f"  Postmortem quality score: {quality['score']}/100 ({quality['label']})")
    for check in quality.get('checks', [])[:6]:
        out.append(f"    - {'OK' if check['ok'] else 'WATCH'} {check['name']}: {check['detail']}")

    fit = layer['market_regime_fit']
    out.append(f"  Market regime fit: {fit['fit']} ({fit['regime']})")
    out.append(f"    - {fit['deduction']}")

    out.append('  Do not overfit:')
    for note in layer['do_not_overfit']:
        out.append(f'    - {note}')

    out.append('  Hypothesis aging:')
    aging = layer['hypothesis_aging']
    if aging:
        for row in aging[:8]:
            kill = f" kill_review={row['kill_review_date']}" if row.get('kill_review_date') else ''
            out.append(
                f"    - {row.get('id')}: trend={row['trend']} age={row['age_days']}d "
                f"evidence_days={row['evidence_days']} last_seen={row['last_seen']}{kill}"
            )
    else:
        out.append('    - No active hypotheses.')

    out.append('  Rule promotion checklist:')
    checklist = layer['rule_promotion_checklist']
    if checklist:
        for row in checklist[:8]:
            out.append(f"    - {row['rule']}: {row['status']} ({row['passed']}/{row['total']} checks)")
    else:
        out.append('    - No rule candidates to checklist.')

    out.append('  Top research questions for next session:')
    for q in layer['top_research_questions']:
        out.append(f'    - {q}')
    out.append('')
    return out


def build_postmortem_start_here(day_iso, flags, tape, state=None):
    return {
        'date_iso': day_iso,
        'generated_at': datetime.now(CT).isoformat(timespec='seconds'),
        'trust_today_for_learning': trust_today_flag(day_iso, flags, tape, state=state),
        'red_yellow_green_status': red_yellow_green_status(day_iso, flags, tape, state=state),
        'next_session_plan': next_session_plan(day_iso, flags, tape, state=state),
        'human_approval_checklist': human_approval_checklist(day_iso, flags, tape, state=state),
        'ultimate_learning_model': ultimate_learning_model(day_iso, flags, tape, state=state),
        'advanced_learning_layer': build_advanced_learning_layer(day_iso, flags, tape, state=state),
        'daily_human_summary': build_daily_learning_brief(day_iso, flags, tape, state=state),
        'discipline_layer': build_discipline_layer(day_iso, flags, tape, state=state),
        'next_session_watch_card': next_session_watch_card(day_iso, flags, tape, state=state),
        'decision_confidence_meter': decision_confidence_meter(day_iso, tape, state=state),
        'loser_saver_rankings': loser_saver_rankings(tape),
        'winner_damage_risk': winner_damage_risk(tape),
        'feedback_map': feedback_map(day_iso, tape),
        'strong_considerations': strong_considerations(day_iso, tape, state=state),
        'review_rule': 'Read this file first; open raw postmortem only when a specific trade/candidate needs detail.',
    }


def write_postmortem_start_here(day_iso, flags, tape, state=None):
    payload = build_postmortem_start_here(day_iso, flags, tape, state=state)
    path = os.path.join(OUT_DIR, f'POSTMORTEM_START_HERE_{day_iso}.json')
    txt_path = os.path.join(OUT_DIR, f'POSTMORTEM_START_HERE_{day_iso}.txt')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, default=str)
    with open(txt_path, 'w', encoding='utf-8') as f:
        f.write(render_start_here_text(day_iso, flags, tape, state=state))
    return path, payload, txt_path


def render_elite_learning_controls(day_iso, flags, tape, state=None):
    out = ['Elite learning controls', '-' * 78]

    watch = next_session_watch_card(day_iso, flags, tape, state=state)
    out.append('  Next-session watch card:')
    for item in watch.get('top_watch_items') or []:
        out.append(f'    - {item}')
    if watch.get('close_to_promotion'):
        out.append('  Close to promotion:')
        for row in watch['close_to_promotion'][:3]:
            out.append(f"    - {row.get('name')}: confidence={row.get('confidence')} evidence_days={row.get('evidence_days')}")
    if watch.get('building_evidence'):
        out.append('  Building evidence:')
        for row in watch['building_evidence'][:3]:
            out.append(f"    - {row.get('name')}: confidence={row.get('confidence')} next={row.get('next_step')}")
    if watch.get('operational_risks'):
        out.append('  Operational/data risks:')
        for risk in watch['operational_risks'][:4]:
            out.append(f'    - {risk}')

    out.append('  Decision confidence meter:')
    meter = decision_confidence_meter(day_iso, tape, state=state)
    if meter:
        for row in meter[:8]:
            out.append(
                f"    - [{row.get('stage')}] {row.get('type')}:{row.get('name')} "
                f"confidence={row.get('confidence')} next={row.get('next_step')}"
            )
    else:
        out.append('    - No conclusions to score yet.')

    out.append('  What would have saved the losers:')
    savers = loser_saver_rankings(tape)
    if savers:
        for row in savers[:6]:
            out.append(
                f"    - {row['bucket']}: {row['losers']} loser(s), "
                f"gross_loss=${row['gross_loss']:.2f}; examples={'; '.join(row['examples'])}"
            )
    else:
        out.append('    - No losing trades.')

    out.append('  Winner damage risk for candidate rules:')
    risks = winner_damage_risk(tape)
    if risks:
        for row in risks[:8]:
            out.append(
                f"    - {row['rule']}: risk={row['risk']} "
                f"losers_saved={row['losers_avoided']} winners_blocked={row['winners_blocked']} "
                f"net=${row['net_estimated_impact']:+.2f}"
            )
    else:
        out.append('    - No candidate rules with measurable winner-damage risk.')

    out.append('  Engine/postmortem feedback map:')
    fmap = feedback_map(day_iso, tape)
    if fmap:
        for row in fmap[:8]:
            out.append(
                f"    - {row['rule']} -> {row['file_or_surface']} "
                f"({row['implementation_area']})"
            )
    else:
        out.append('    - No rule candidates to map.')
    out.append('')
    return out


# â”€â”€ Format report â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def render_report(day_iso, flags, notes, tape, log_hits, state=None,
                  mutate_hypotheses=False, detail_refresh=True):
    out = []
    try:
        from postmortem_hypotheses import update_and_render
        hypothesis_lines = update_and_render(day_iso, tape, mutate=mutate_hypotheses)
    except Exception as e:
        hypothesis_lines = [
            'Watchlist & deductions',
            '-' * 78,
            f'  ERROR computing deductions: {e}',
        ]

    trust = trust_today_flag(day_iso, flags, tape, state=state)
    out.append(f"TRUST TODAY FOR LEARNING: {trust['trust']} ({trust['quality_score']}/100 {trust['quality_label']})")
    out.append(f"TRUST REASON: {trust['reason']}")
    out.append('')
    out.append(f'POST-MORTEM {day_iso}')
    out.append('=' * 78)
    out.append('')

    gate_path = os.path.join(OUT_DIR, f'daily_review_gate_{day_iso}.json')
    try:
        with open(gate_path, 'r', encoding='utf-8') as f:
            gate = json.load(f)
    except Exception:
        gate = {}
    if gate:
        out.append(f"DAILY REVIEW GATE: {gate.get('verdict', 'unknown')}")
        out.append('-' * 78)
        out.append(
            f"  checks: {gate.get('checks_passed')}/{gate.get('checks_total')} | "
            "strategy conclusions require safe_to_learn plus human approval"
        )
        buckets = gate.get('conclusion_buckets') or {}
        out.append(
            "  buckets: "
            + ", ".join(f"{k}={len(v or [])}" for k, v in buckets.items())
        )
        out.append('')

    for line in render_top_summary(day_iso, flags, tape, state):
        out.append(line)

    out.append('')
    for line in render_strong_considerations(day_iso, tape, state=state):
        out.append(line)

    out.append('')
    for line in render_tracking_hypotheses(hypothesis_lines):
        out.append(line)

    for line in render_ryg_status(day_iso, flags, tape, state=state):
        out.append(line)

    for line in render_next_session_plan(day_iso, flags, tape, state=state):
        out.append(line)

    for line in render_human_approval_checklist(day_iso, flags, tape, state=state):
        out.append(line)

    for line in render_daily_learning_brief(day_iso, flags, tape, state=state):
        out.append(line)

    for line in render_ultimate_learning_model(day_iso, flags, tape, state=state):
        out.append(line)

    for line in render_elite_learning_controls(day_iso, flags, tape, state=state):
        out.append(line)

    for line in render_discipline_layer(day_iso, flags, tape, state=state):
        out.append(line)

    for line in render_advanced_learning_layer(day_iso, flags, tape, state=state):
        out.append(line)

    for line in render_learning_lifecycle_referee(day_iso, tape, state=state):
        out.append(line)

    out.append('')
    for line in render_human_review_prompts(day_iso):
        out.append(line)

    for line in render_executive_verdict(day_iso, flags, tape, state):
        out.append(line)

    for line in render_daily_verdict(day_iso, flags, tape, state):
        out.append(line)

    for line in operational_incident_review(day_iso, tape, log_hits, state):
        out.append(line)

    for line in render_trader_review(day_iso, tape, state):
        out.append(line)

    for line in render_decision_review(tape, state):
        out.append(line)

    for line in render_entry_quality_review(tape):
        out.append(line)

    for line in render_thesis_failure_review(tape):
        out.append(line)

    for line in render_trade_thesis_timeline_review(tape):
        out.append(line)

    for line in render_winner_quality_review(tape):
        out.append(line)

    for line in render_counterfactual_review(tape):
        out.append(line)

    for line in render_missed_vs_taken(tape, state, day_iso):
        out.append(line)

    for line in render_rolling_evidence(day_iso):
        out.append(line)

    for line in render_hypothesis_lifecycle(day_iso, tape):
        out.append(line)

    for line in render_do_not_change_yet(day_iso, tape, state):
        out.append(line)

    for line in render_human_context(day_iso):
        out.append(line)

    for line in render_loser_triage(tape):
        out.append(line)

    # Layer 1
    out.append('Health checks')
    out.append('-' * 78)
    if flags:
        for f in flags:
            out.append(f'  [!]  {f}')
    else:
        out.append('  no flags raised')
    for n in notes:
        out.append(f'  â€¢    {n}')
    out.append('')

    # Layer 2 â€” trade list
    out.append(f"Layer 2 â€” today's tape ({len(tape)} trades)")
    out.append('-' * 78)
    if not tape:
        out.append('  no trades today')
    else:
        wins   = sum(1 for r in tape if r['pnl']>0)
        losses = sum(1 for r in tape if r['pnl']<0)
        wr     = 100*wins/(wins+losses) if (wins+losses) else 0
        total  = sum(r['pnl'] for r in tape)
        avg_w  = (sum(r['pnl'] for r in tape if r['pnl']>0)/wins) if wins else 0
        avg_l  = (sum(r['pnl'] for r in tape if r['pnl']<0)/losses) if losses else 0
        out.append(f'  totals: {wins}W / {losses}L  win%={wr:.1f}  P&L=${total:+.2f}')
        out.append(f'  avg win=${avg_w:+.2f}  avg loss=${avg_l:+.2f}')
        fwd_errors = {}
        for r in tape:
            if r.get('fwd_status') not in (None, 'ok'):
                fwd_errors[r['fwd_status']] = fwd_errors.get(r['fwd_status'], 0) + 1
        if fwd_errors:
            out.append(f'  forward-return fetch issues: {fwd_errors}')
        out.append('')
        out.append(f"  {'time':<5} {'tkr':<5} {'side':<5} {'conv':<4} "
                   f"{'entry':>8} {'exit':>8} {'pnl':>8} {'dur':>5} "
                   f"{'fwd5':>6} {'fwd15':>6} {'fwd60':>6}  {'reason'}")
        for r in tape:
            f5  = f"{r['fwd5']:>+5.2f}%"  if r['fwd5']  is not None else '   n/a'
            f15 = f"{r['fwd15']:>+5.2f}%" if r['fwd15'] is not None else '   n/a'
            f60 = f"{r['fwd60']:>+5.2f}%" if r['fwd60'] is not None else '   n/a'
            out.append(f"  {r['time']:<5} {r['ticker']:<5} {r['side']:<5} "
                       f"{r['conv'][:3]:<4} {r['entry']:>8.4f} {r['exit']:>8.4f} "
                       f"{r['pnl']:>+8.2f} {r['duration_min']:>5.1f} "
                       f"{f5} {f15} {f60}  {r['reason']}")
        out.append('')

        # Raw indicator detail per losing trade
        losers = [r for r in tape if r['pnl']<0]
        if losers:
            out.append(f'  Raw indicator detail - losing trades only ({len(losers)})')
            for r in losers:
                ind = r['ind']; btc = r['btc']
                out.append(f"    {r['time']} {r['ticker']} {r['side']}/{r['conv']} "
                           f"pnl={r['pnl']:+.2f} score={r['score']}")
                fields = [
                    ('ema_stack', ind.get('ema_stack')),
                    ('vwap_dist', ind.get('vwap_dist')),
                    ('vwap_sigma', ind.get('vwap_dist_sigma')),
                    ('vol_z_60s', ind.get('vol_z_60s')),
                    ('tick_z_30s', ind.get('tick_z_30s')),
                    ('mom_5s', ind.get('mom_5s')),
                    ('mom_15s', ind.get('mom_15s')),
                    ('mom_60s', ind.get('mom_60s')),
                    ('flow30_buy', get_path(ind, 'flow_30s.buy_pct')),
                    ('flow120_buy', get_path(ind, 'flow_120s.buy_pct')),
                    ('btc.stack', (btc or {}).get('ema_stack') or (btc or {}).get('stack')),
                    ('btc.mom_60s', (btc or {}).get('mom_60s') or (btc or {}).get('mom_60')),
                ]
                parts = []
                for name, v in fields:
                    if v is None:
                        parts.append(f'{name}=None')
                    elif isinstance(v, float):
                        parts.append(f'{name}={v:.3f}')
                    else:
                        parts.append(f'{name}={v}')
                out.append('      ' + '  '.join(parts))
            out.append('')

    if detail_refresh:
        for line in render_skipped_review(state, day_iso):
            out.append(line)

        for line in render_shadow_decision_review(day_iso):
            out.append(line)

        for line in render_shadow_exit_review(day_iso):
            out.append(line)

        for line in render_near_signal_review(day_iso):
            out.append(line)
    else:
        out.append('Detail corpus refresh')
        out.append('-' * 78)
        out.append('  skipped/shadow/near-signal forward detail refresh skipped for this run.')
        out.append('  Full JSONL corpuses remain available under postmortem/* directories.')
        out.append('')

    for line in render_engine_scoreboard(day_iso):
        out.append(line)

    for line in render_guardrail_audit(day_iso, tape, state):
        out.append(line)

    for line in render_review_artifact_summary(day_iso):
        out.append(line)

    for line in render_ev_table(day_iso, tape):
        out.append(line)

    for line in render_candidate_config(day_iso):
        out.append(line)

    # Engine activity stats
    out.append('Engine activity')
    out.append('-' * 78)
    out.append('  active engine            : ws_scalp')
    out.append(f"  OPEN log lines           : {len(log_hits.get('open',[]))}")
    out.append(f"  CLOSE log lines          : {len(log_hits.get('close',[]))}")
    out.append('')

    return '\n'.join(out)


# â”€â”€ main â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def render_gdoc_summary(day_iso, flags, tape, state=None, mutate_hypotheses=False):
    """Short human-facing Google Doc body.

    Full forensic data remains in the local TXT/JSON artifacts so the bot can
    keep learning without turning the rolling doc into a data dump.
    """
    out = []
    trust = trust_today_flag(day_iso, flags, tape, state=state)
    winners = [r for r in tape if r.get('pnl', 0) > 0]
    losers = [r for r in tape if r.get('pnl', 0) < 0]
    total = sum(float(r.get('pnl') or 0) for r in tape)
    win_rate = 100 * len(winners) / len(tape) if tape else 0
    brief = build_daily_learning_brief(day_iso, flags, tape, state=state)
    roots = loser_root_cause_buckets(tape)
    hyp = hypothesis_preview(day_iso, tape)
    recs = strong_considerations(day_iso, tape, state=state)

    def worked_summary(items):
        if not winners:
            return 'No winning setup stood out today; treat the day as data collection.'
        text = ' '.join(items).lower()
        if 'btc_relative_strength' in text:
            return 'The best trades came when the miner move had a clear BTC-relative edge and follow-through.'
        if 'flow_exhaustion_fade' in text:
            return 'The bot did well when it waited for stretched flow to fade instead of chasing the first move.'
        return 'Winning trades shared cleaner confirmation and better follow-through than the losing group.'

    def failed_summary(items):
        if not losers:
            return 'No losing setup stood out today.'
        text = ' '.join(items).lower()
        if 'bad_execution_context' in text or 'wide_spread' in text:
            return 'Most damage came from execution-quality problems, especially trades entered with poor spread or fill context.'
        if 'exit_rule_candidate' in text:
            return 'The largest concern is whether exits reacted fast enough after the thesis weakened.'
        return 'The losing trades need more clean days before turning a pattern into a rule change.'

    out.append(f"Learning trust: {trust['trust']} ({trust['quality_score']}/100 {trust['quality_label']})")
    out.append(f"Why: {trust['reason']}")
    out.append('')

    out.append('Daily human summary')
    out.append('')
    out.append(f"Record: {len(winners)}W / {len(losers)}L | Win rate: {win_rate:.1f}% | P&L: ${total:+.2f}")
    out.append('')

    worked_items = (brief.get('what_worked') or [])[:3]
    out.append('What worked')
    out.append('')
    for item in worked_items:
        out.append(f"• {item}")
    out.append('')
    out.append(worked_summary(worked_items))
    out.append('')

    failed_items = (brief.get('what_failed') or [])[:3]
    out.append("What didn't work")
    out.append('')
    for item in failed_items:
        out.append(f"• {item}")
    out.append('')
    out.append(failed_summary(failed_items))
    if flags:
        out.append('')
        out.append('Data / ops notes')
        out.append('')
        for flag in flags[:3]:
            out.append(f"• {flag}")

    out.append('')
    out.append('Suggested action items')
    out.append('')
    if not recs:
        out.append('No strategy changes cleared the evidence threshold today.')
        out.append('Keep collecting data. Do not alter rules from isolated losses.')
    else:
        out.append('Human approval required. No automatic changes were made.')
        for rec in recs[:6]:
            out.append(f"• {rec}")

    out.append('')
    out.append('Tracking hypotheses')
    out.append('')
    if roots:
        out.append("Today's main evidence")
        out.append('')
        for row in roots[:4]:
            out.append(
                f"• {row['root_cause']}: {row['losers']} loser(s), "
                f"gross loss ${row['gross_loss']:.2f}"
            )
    active = sorted(
        hyp.get('active') or [],
        key=lambda h: (-int(h.get('evidence_days') or 0), h.get('id', '')),
    )
    if active:
        out.append('')
        out.append('Active watchlist')
        out.append('')
        for idx, h in enumerate(active[:6], start=1):
            stale = ' [STALE]' if h.get('stale') else ''
            monitor = str(h.get('monitor') or '')
            monitor = monitor.split(' If pattern holds')[0].strip()
            out.append(f"• Hypothesis {idx}{stale}: {monitor}")
            first_seen = h.get('first_seen') or h.get('created') or ((h.get('seen_dates') or [None])[0])
            out.append(
                f"  Seen {h.get('evidence_days', 0)} day(s), "
                f"first {first_seen}, last {h.get('last_seen')}"
            )
            out.append('')
    else:
        out.append('No active hypothesis data available.')

    out.append('')
    out.append('Local detail artifacts')
    out.append('')
    out.append(f"Full forensic TXT: {os.path.join(OUT_DIR, f'postmortem_{day_iso}.txt')}")
    out.append(f"Full machine JSON: {os.path.join(OUT_DIR, f'postmortem_{day_iso}.json')}")
    out.append(f"Start-here recap: {os.path.join(OUT_DIR, f'POSTMORTEM_START_HERE_{day_iso}.txt')}")
    out.append('The bot uses these local artifacts for learning, replay, hypotheses, and future reviews.')
    out.append('')
    return '\n'.join(out)


def run(day_iso=None, write_gdoc=True, mutate_hypotheses=None, write_files=True,
        detail_refresh=True, gdoc_mode='append', gdoc_full=False, quiet=False):
    day = today_iso(day_iso)
    if mutate_hypotheses is None:
        mutate_hypotheses = (day == today_iso(None))
    if not quiet:
        print(f'post-mortem for {day}')

    state = load_state()
    log_hits = parse_log_for_day(day, {
        'signal_source': 'signal_source=',
        'time_stop':     'TIME-STOP ',
        'open':          ' OPEN ',
        'close':         ' CLOSE ',
        'orphans':       'orphan',
        'broker_block':  'BROKER_EXPOSURE_BLOCK',
        'close_failed':  'held_for_orders',
        'extended_hours': 'extended-hours',
    })

    flags, notes = layer1_health(state, day, log_hits)
    tape = layer2_tape(state, day, log_hits)
    report = render_report(day, flags, notes, tape, log_hits, state=state,
                           mutate_hypotheses=mutate_hypotheses,
                           detail_refresh=detail_refresh)
    candidate_path, candidate_payload = export_candidate_rules(day, tape, state=state)
    try:
        from engine_scoreboard import write_scoreboard
        scoreboard_path, scoreboard_payload = write_scoreboard(day) if write_files else (None, None)
    except Exception as e:
        scoreboard_path, scoreboard_payload = None, {'error': str(e)}
    try:
        from engine_validation import (
            candidate_config as build_engine_candidate_config,
            exit_policy_candidate_config,
            exit_policy_replay,
            run_validation,
            rolling_exit_policy_replay,
            rolling_validation,
            write_candidate_config as write_engine_candidate_config,
            write_exit_policy_candidate_config,
            write_exit_policy_replay,
            write_rolling_exit_policy_replay,
            write_rolling_validation,
            write_validation,
            write_weekend_packet,
        )
        if write_files:
            engine_validation_path, decision_audit_path, engine_validation_payload = write_validation(
                day, include_replay=False
            )
            rolling_validation_path, rolling_validation_payload = write_rolling_validation(day, lookback=5)
            engine_candidate_path, engine_candidate_payload = write_engine_candidate_config(day, lookback=5)
            exit_replay_path, exit_replay_payload = write_exit_policy_replay(day)
            rolling_exit_replay_path, rolling_exit_replay_payload = write_rolling_exit_policy_replay(day, lookback=5)
            exit_candidate_path, exit_candidate_payload = write_exit_policy_candidate_config(day, lookback=5)
            weekend_packet_path, weekend_packet_payload = write_weekend_packet(day, lookback=5)
        else:
            engine_validation_path = decision_audit_path = rolling_validation_path = engine_candidate_path = None
            exit_replay_path = rolling_exit_replay_path = exit_candidate_path = weekend_packet_path = None
            engine_validation_payload = run_validation(day, include_replay=False)
            rolling_validation_payload = rolling_validation(day, lookback=5)
            engine_candidate_payload = build_engine_candidate_config(day, lookback=5)
            exit_replay_payload = exit_policy_replay(day)
            rolling_exit_replay_payload = rolling_exit_policy_replay(day, lookback=5)
            exit_candidate_payload = exit_policy_candidate_config(day, lookback=5)
            weekend_packet_payload = None
    except Exception as e:
        engine_validation_path = decision_audit_path = rolling_validation_path = engine_candidate_path = None
        exit_replay_path = rolling_exit_replay_path = exit_candidate_path = weekend_packet_path = None
        engine_validation_payload = rolling_validation_payload = engine_candidate_payload = {'error': str(e)}
        exit_replay_payload = rolling_exit_replay_payload = exit_candidate_payload = weekend_packet_payload = {'error': str(e)}
    try:
        from elite_control import write_candidate_config, build_candidate_config
        candidate_config_path, candidate_config_payload = (
            write_candidate_config(day) if write_files else (None, build_candidate_config(day))
        )
    except Exception as e:
        candidate_config_path, candidate_config_payload = None, {'error': str(e)}
    try:
        from ev_analytics import write_ev_table, build_ev_table
        ev_table_path, ev_table_payload = (
            write_ev_table(day, current_tape=tape) if write_files
            else (None, build_ev_table(day, current_tape=tape))
        )
    except Exception as e:
        ev_table_path, ev_table_payload = None, {'error': str(e)}
    review_artifact_paths = {}
    review_artifact_payloads = {}

    if detail_refresh:
        skipped_detail = skipped_with_bar_forwards(state, day)
        shadow_detail = shadow_decisions_with_bar_forwards(day)
        shadow_exit_detail = shadow_exits_for_day(day)
        near_detail = near_signals_for_day(day)
        audit_detail = audit_events_for_day(day)
        skipped_base = skipped_for_day(state, day)
        pre_submit_blocks = [
            s for s in skipped_base
            if str(s.get('reason', '')).startswith('pre_submit_')
        ]
        live_quality_blocks = [
            s for s in skipped_base
            if str(s.get('reason', '')).startswith('live_quality_gate:')
        ]
    else:
        skipped_detail = []
        shadow_detail = []
        shadow_exit_detail = []
        near_detail = []
        audit_detail = []
        pre_submit_blocks = []
        live_quality_blocks = []

    # Write JSON corpus
    json_path = os.path.join(OUT_DIR, f'postmortem_{day}.json')
    start_here_path = None
    start_here_txt_path = None
    weekly_json_path = None
    weekly_txt_path = None
    payload = {
        'date_iso': day,
        'flags':    flags,
        'notes':    notes,
        'daily_verdict': daily_verdict(day, flags, tape, state),
        'clean_day_score': clean_day_score(day, flags, tape, state),
        'data_quality': data_quality_score(flags, tape, state, day),
        'trust_today_for_learning': trust_today_flag(day, flags, tape, state=state),
        'red_yellow_green_status': red_yellow_green_status(day, flags, tape, state=state),
        'next_session_plan': next_session_plan(day, flags, tape, state=state),
        'human_approval_checklist': human_approval_checklist(day, flags, tape, state=state),
        'ultimate_learning_model': ultimate_learning_model(day, flags, tape, state=state),
        'trade_archetype_grades': trade_archetype_grades(tape),
        'btc_relationship_analysis': btc_relationship_analysis(tape),
        'loser_root_cause_buckets': loser_root_cause_buckets(tape),
        'rule_attribution_matrix': rule_attribution_matrix(tape),
        'one_sentence_trade_lessons': trade_lessons(tape),
        'advanced_learning_layer': build_advanced_learning_layer(day, flags, tape, state=state),
        'one_page_daily_scorecard': one_page_daily_scorecard(day, flags, tape, state=state),
        'pattern_replay_request_list': pattern_replay_request_list(day, flags, tape, state=state),
        'trade_quality_outcome_split': trade_quality_outcome_split(tape),
        'best_loser_worst_winner': best_loser_worst_winner(tape),
        'rolling_bot_personality': rolling_bot_personality(day),
        'weekly_synthesis': weekly_synthesis(day),
        'entry_quality_summary': entry_quality_summary(tape),
        'trade_thesis_timelines': build_trade_thesis_timelines(tape),
        'winner_quality_review': winner_quality_review(tape),
        'rolling_evidence': rolling_evidence(day),
        'learning_lifecycle': build_learning_lifecycle(day, tape, state=state),
        'daily_learning_brief': build_daily_learning_brief(day, flags, tape, state=state),
        'decision_confidence_meter': decision_confidence_meter(day, tape, state=state),
        'loser_saver_rankings': loser_saver_rankings(tape),
        'winner_damage_risk': winner_damage_risk(tape),
        'next_session_watch_card': next_session_watch_card(day, flags, tape, state=state),
        'discipline_layer': build_discipline_layer(day, flags, tape, state=state),
        'coach_verdict': coach_verdict(day, flags, tape, state=state),
        'do_not_overfit': do_not_overfit_box(day, flags, tape, state=state),
        'hypothesis_aging': hypothesis_aging(day, tape),
        'rule_promotion_checklist': rule_promotion_checklist(day, flags, tape, state=state),
        'market_regime_fit': market_regime_fit(day, tape, state=state),
        'top_research_questions': top_research_questions(day, flags, tape, state=state),
        'postmortem_quality_score': postmortem_quality_score(day, flags, tape, state=state),
        'feedback_map': feedback_map(day, tape),
        'human_notes': load_human_notes(day),
        'tape':     tape,
        'loser_decision_review': build_loser_review(tape),
        'counterfactual_rules': counterfactual_rules(tape),
        'candidate_rules_file': candidate_path,
        'candidate_rules': candidate_payload.get('candidates', []),
        'engine_scoreboard_file': scoreboard_path,
        'engine_scoreboard': scoreboard_payload,
        'engine_validation_file': engine_validation_path,
        'decision_audit_file': decision_audit_path,
        'engine_validation': engine_validation_payload,
        'rolling_engine_validation_file': rolling_validation_path,
        'rolling_engine_validation': rolling_validation_payload,
        'engine_candidate_config_file': engine_candidate_path,
        'engine_candidate_config': engine_candidate_payload,
        'exit_policy_replay_file': exit_replay_path,
        'exit_policy_replay': exit_replay_payload,
        'rolling_exit_policy_replay_file': rolling_exit_replay_path,
        'rolling_exit_policy_replay': rolling_exit_replay_payload,
        'exit_policy_candidate_config_file': exit_candidate_path,
        'exit_policy_candidate_config': exit_candidate_payload,
        'weekend_validation_packet_file': weekend_packet_path,
        'weekend_validation_packet': weekend_packet_payload,
        'candidate_config_file': candidate_config_path,
        'candidate_config': candidate_config_payload,
        'ev_table_file': ev_table_path,
        'ev_table': ev_table_payload,
        'operational_incident_review': operational_incident_review(day, tape, log_hits, state),
        'detail_corpus': {
            'json_detail_row_limit': JSON_DETAIL_ROW_LIMIT,
            'detail_refresh': detail_refresh,
            'skipped_signals_file': corpus_file_ref('skipped_signals', day),
            'shadow_decisions_file': corpus_file_ref('shadow_decisions', day),
            'shadow_exits_file': corpus_file_ref('shadow_exits', day),
            'near_signals_file': corpus_file_ref('near_signals', day),
            'rollups': {
                'skipped_signals': rollup_rows(skipped_detail),
                'shadow_decisions': rollup_rows(shadow_detail, fields=('ticker', 'side', 'decision', 'reason')),
                'shadow_exits': rollup_rows(shadow_exit_detail, fields=('ticker', 'side', 'policy', 'reason')),
                'near_signals': rollup_rows(near_detail),
                'audit_events': rollup_rows(audit_detail, fields=('event', 'symbol')),
            },
        },
        'skipped_signals': {'count': len(skipped_detail), 'file': corpus_file_ref('skipped_signals', day)},
        'shadow_decisions': {'count': len(shadow_detail), 'file': corpus_file_ref('shadow_decisions', day)},
        'near_signals': {'count': len(near_detail), 'file': corpus_file_ref('near_signals', day)},
        'guardrail_audit': {
            'audit_events': {'count': len(audit_detail), 'file': os.path.join(AUDIT_DIR, f'trade_lifecycle_{day}.jsonl')},
            'pre_submit_blocks': compact_rows(pre_submit_blocks),
            'live_quality_blocks': compact_rows(live_quality_blocks),
            'btc_impulse_abort_exits': [
                r for r in tape if str(r.get('reason', '')).startswith('btc_impulse_abort')
            ],
        },
        'strong_considerations': strong_considerations(day, tape, state=state),
        'skipped_opportunity_summary': skipped_opportunity_summary(state, day)[0],
        'forward_return_fetch_issues': {
            k: sum(1 for r in tape if r.get('fwd_status') == k)
            for k in sorted({r.get('fwd_status') for r in tape
                             if r.get('fwd_status') not in (None, 'ok')})
        },
        'log_counts': {k: len(v) for k, v in log_hits.items()},
    }
    if write_files:
        write_json_atomic(candidate_path, candidate_payload)
        try:
            from review_artifacts import write_all as write_review_artifacts
            review_artifact_paths, review_artifact_payloads = write_review_artifacts(day, postmortem_payload=payload)
            payload['review_artifacts'] = {
                'files': review_artifact_paths,
                'review_index': review_artifact_payloads.get('review_index'),
                'risk_summary': review_artifact_payloads.get('risk_summary'),
                'execution_summary': review_artifact_payloads.get('execution_summary'),
                'shadow_exit_summary': review_artifact_payloads.get('shadow_exit_summary'),
                'new_gate_attribution': review_artifact_payloads.get('new_gate_attribution'),
            }
        except Exception as e:
            payload['review_artifacts'] = {'error': str(e)}
        try:
            start_here_path, start_here_payload, start_here_txt_path = write_postmortem_start_here(day, flags, tape, state=state)
            write_json_atomic(os.path.join(OUT_DIR, 'POSTMORTEM_START_HERE_LATEST.json'), start_here_payload)
            payload['postmortem_start_here_file'] = start_here_path
            payload['postmortem_start_here_txt_file'] = start_here_txt_path
            payload['postmortem_start_here'] = {
                'daily_human_summary': start_here_payload.get('daily_human_summary'),
                'trust_today_for_learning': start_here_payload.get('trust_today_for_learning'),
                'red_yellow_green_status': start_here_payload.get('red_yellow_green_status'),
                'next_session_plan': start_here_payload.get('next_session_plan'),
                'next_session_watch_card': start_here_payload.get('next_session_watch_card'),
                'decision_confidence_count': len(start_here_payload.get('decision_confidence_meter') or []),
                'loser_saver_count': len(start_here_payload.get('loser_saver_rankings') or []),
                'winner_damage_risk_count': len(start_here_payload.get('winner_damage_risk') or []),
            }
        except Exception as e:
            payload['postmortem_start_here'] = {'error': str(e)}
        try:
            weekly_json_path, weekly_txt_path, weekly_payload = write_weekly_learning_summary(day)
            payload['weekly_learning_summary_file'] = weekly_json_path
            payload['weekly_learning_summary_txt_file'] = weekly_txt_path
            payload['weekly_learning_summary'] = weekly_payload
        except Exception as e:
            payload['weekly_learning_summary'] = {'error': str(e)}
        write_json_atomic(json_path, payload)

    # Write TXT
    txt_path = os.path.join(OUT_DIR, f'postmortem_{day}.txt')
    if write_files:
        with open(txt_path, 'w', encoding='utf-8') as f:
            f.write(report)

    if not quiet:
        print(report)
        print()
        if write_files:
            print(f'written: {json_path}')
            print(f'written: {txt_path}')
            print(f'written: {candidate_path}')
            for path in (
                    scoreboard_path, engine_validation_path, decision_audit_path,
                    rolling_validation_path, engine_candidate_path, exit_replay_path,
                    rolling_exit_replay_path, exit_candidate_path, weekend_packet_path,
                    candidate_config_path, ev_table_path, start_here_path,
                    start_here_txt_path, weekly_json_path, weekly_txt_path):
                if path:
                    print(f'written: {path}')
            for path in review_artifact_paths.values():
                print(f'written: {path}')
        else:
            print(f'dry-run: not written: {json_path}')
            print(f'dry-run: not written: {txt_path}')
            print(f'dry-run: not written: {candidate_path}')

    if write_gdoc:
        try:
            from gdocs_writer import append_postmortem
            gdoc_body = (
                report if gdoc_full
                else render_gdoc_summary(day, flags, tape, state=state, mutate_hypotheses=False)
            )
            res = append_postmortem(date_iso=day, body_text=gdoc_body, mode=gdoc_mode)
            if not quiet:
                print(f"wrote Google Doc ({gdoc_mode}): {res['inserted']} chars inserted")
        except Exception as e:
            if not quiet:
                print(f'WARNING: Google Doc append failed: {e}')


if __name__ == '__main__':
    # Optional CLI override: python daily_postmortem.py 2026-04-24
    skip_flags = {'--dry-run', '--summary-only', '--no-detail-refresh', '--gdoc-upsert', '--gdoc-full', '--quiet'}
    args = [a for a in sys.argv[1:] if a not in skip_flags]
    arg = args[0] if args else None
    dry_run = '--dry-run' in sys.argv[1:]
    detail_refresh = not (
        '--summary-only' in sys.argv[1:]
        or '--no-detail-refresh' in sys.argv[1:]
    )
    gdoc_mode = 'upsert' if '--gdoc-upsert' in sys.argv[1:] else 'append'
    gdoc_full = '--gdoc-full' in sys.argv[1:]
    quiet = '--quiet' in sys.argv[1:]
    run(day_iso=arg, write_gdoc=not dry_run, mutate_hypotheses=not dry_run,
        write_files=not dry_run, detail_refresh=detail_refresh, gdoc_mode=gdoc_mode,
        gdoc_full=gdoc_full, quiet=quiet)
