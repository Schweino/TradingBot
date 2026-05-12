"""Build reusable decision tapes from opportunity ledgers and prepared events.

The tape is meant for fast scoring-profile research. It preserves the detected
decision points, model features, and precomputed LONG/SHORT exit paths. It does
not modify the live engine.
"""
from __future__ import annotations

from output_paths import output_path

import argparse
import gzip
import json
import os
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date
from typing import Any

import backtest_30d_engine as replay
import opportunity_outcome_cache
import replay_state_checkpoints
import replay_artifacts
import scoring_profiles
import step2_artifact_identity
import step2_latency_model
import step2_execution_contract
import worker_policy
import ws_scalp


HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUT_DIR = output_path('postmortem', 'backtests', 'decision_tapes')


def _read_jsonl(path: str) -> list[dict]:
    rows = []
    if not os.path.exists(path):
        return rows
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _write_jsonl_gz(path: str, rows: list[dict]) -> dict[str, Any]:
    return step2_artifact_identity.write_canonical_jsonl_gz(path, rows, sort_rows=True)


def _opportunity_path(args, day: date) -> str:
    tickers = '-'.join(args.tickers)
    suffix = f'_{args.profile_name}' if args.profile_name else ''
    return os.path.join(
        args.opportunity_dir,
        f'engine_opportunities_{args.feed}_{args.quote_mode}_{args.btc_mode}_{tickers}{suffix}_{day.isoformat()}.jsonl',
    )


def _checkpoint_path(args, day: date) -> str:
    tickers = '-'.join(args.tickers)
    return os.path.join(
        replay.DEFAULT_OUT_DIR,
        'engine_replay_days',
        f'{args.feed}_{args.quote_mode}_{args.btc_mode}_{tickers}_100000_{day.isoformat()}.json',
    )


def _load_opportunities(args, day: date) -> tuple[list[dict], str]:
    opp_path = _opportunity_path(args, day)
    rows = _read_jsonl(opp_path)
    if rows:
        return rows, opp_path
    checkpoint = _checkpoint_path(args, day)
    payload = replay._read_json(checkpoint, {}) or {}
    rows = payload.get('opportunities') or []
    return rows, checkpoint


def _scan_signal_opportunities(day: date, args) -> tuple[list[dict], str, int]:
    tape_args = argparse.Namespace(
        tickers=args.tickers,
        feed=args.feed,
        quote_mode=args.quote_mode,
        btc_mode=args.btc_mode,
        cache_dir=args.cache_dir,
        prepared_cache_dir=args.prepared_cache_dir,
        use_prepared_events=True,
        refresh_prepared_events=False,
        refresh=False,
        max_pages=1000,
    )
    stats = replay.ReplayStats()
    events = replay._load_day_events(day, tape_args, stats)
    _start_iso, _end_iso, start_sec, end_sec = replay._session_bounds_utc(day)
    entry_cutoff = replay._entry_cutoff_ts(day)
    decision_start_sec = int(getattr(args, 'decision_start_ts', 0) or 0)
    decision_end_sec = int(getattr(args, 'decision_end_ts', 0) or 0)
    if decision_start_sec > 0:
        decision_start_sec = max(start_sec, decision_start_sec)
    else:
        decision_start_sec = start_sec
    if decision_end_sec > 0:
        decision_end_sec = min(end_sec, decision_end_sec)
    else:
        decision_end_sec = end_sec
    checkpoint_payload = None
    scan_start_sec = start_sec
    if getattr(args, 'use_state_checkpoints', False) and decision_start_sec > start_sec:
        checkpoint_payload = replay_state_checkpoints.load_best_before(
            getattr(args, 'checkpoint_dir', replay_state_checkpoints.DEFAULT_ROOT),
            args,
            day,
            decision_start_sec,
        )
    if checkpoint_payload:
        states = replay_state_checkpoints.restore_states(checkpoint_payload)
        scan_start_sec = int(checkpoint_payload.get('checkpoint_sec') or start_sec) + 1
    else:
        states = {sym: replay._state(sym) for sym in list(args.tickers) + [replay.BTC_SYMBOL]}
    by_sec: dict[int, list[dict]] = defaultdict(list)
    for event in events:
        sec = int(event['t']) // 1000
        if start_sec <= sec < end_sec:
            by_sec[sec].append(event)

    compute_indicators = (
        replay._replay_compute_indicators
        if getattr(args, 'indicator_mode', 'live') == 'fast'
        else ws_scalp.compute_indicators
    )
    rows = []
    seq_by_key: dict[str, int] = defaultdict(int)
    clock = replay.ReplayClock()
    original_active_profile = getattr(ws_scalp, 'ACTIVE_SCORING_PROFILE', None)
    original_time = ws_scalp.time.time
    original_near_signal_logger = getattr(ws_scalp, '_log_near_signal', None)
    if getattr(args, 'disable_active_scoring_profile', False):
        ws_scalp.ACTIVE_SCORING_PROFILE = {}
    ws_scalp.time.time = clock.time
    if original_near_signal_logger is not None:
        ws_scalp._log_near_signal = lambda *a, **k: None
    try:
        for sec in range(scan_start_sec, end_sec):
            clock.now = sec + 0.999
            for event in by_sec.get(sec, []):
                st = states.get(event['symbol'])
                if not st:
                    continue
                if event['kind'] == 'stock_trade':
                    replay._feed_stock_trade(st, event['symbol'], event['row'])
                elif event['kind'] == 'stock_quote':
                    replay._feed_stock_quote(st, event['row'])
                elif event['kind'] == 'btc_synth_trade':
                    replay._feed_btc_synth(st, event['row'])
            for st in states.values():
                replay._roll_bar(st, sec)
            if sec > entry_cutoff:
                continue
            if sec < decision_start_sec or sec > decision_end_sec:
                continue
            btc_ind = compute_indicators(states[replay.BTC_SYMBOL]) if args.btc_mode != 'off' else {
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
            for tkr, ind in stock_indicators.items():
                sig = ws_scalp.detect_signal(tkr, ind, btc_ind, miner_indicators=stock_indicators)
                if not sig:
                    continue
                sig = replay._json_clone(sig)
                sig['ts'] = sec
                key = f"{day.isoformat()}:{tkr}:{sec}:{sig.get('side')}:{sig.get('setup_type', 'unknown')}"
                seq = seq_by_key[key]
                seq_by_key[key] += 1
                opp_id = f'{key}:{seq}'
                sig['opportunity_id'] = opp_id
                rows.append({
                    'schema_version': 2,
                    'type': 'raw_signal_opportunity',
                    'indicator_mode': getattr(args, 'indicator_mode', 'live'),
                    'opportunity_id': opp_id,
                    'day': day.isoformat(),
                    'ts': sec,
                    'ts_ct': replay._iso_ct(sec),
                    'ticker': tkr,
                    'side': sig.get('side'),
                    'setup_type': sig.get('setup_type'),
                    'price': replay._float(sig.get('price')),
                    'score': sig.get('score'),
                    'conviction': sig.get('conviction'),
                    'session_phase': sig.get('session_phase'),
                    'btc_regime': sig.get('btc_regime'),
                    'decision': 'raw_signal',
                    'signal': sig,
                    'features': {
                        'ticker_indicators': ind,
                        'btc_indicators': btc_ind,
                        'miner_indicators': stock_indicators,
                    },
                })
    finally:
        ws_scalp.ACTIVE_SCORING_PROFILE = original_active_profile
        ws_scalp.time.time = original_time
        if original_near_signal_logger is not None:
            ws_scalp._log_near_signal = original_near_signal_logger
    source = 'raw_signal_scan_checkpoint' if checkpoint_payload else 'raw_signal_scan'
    return rows, source, len(events)


def _signal_scan_segment(day: date, args_dict: dict, start_sec: int, end_sec: int) -> dict:
    seg_args = argparse.Namespace(**args_dict)
    seg_args.workers = 1
    seg_args.decision_start_ts = int(start_sec)
    seg_args.decision_end_ts = int(end_sec)
    rows, source, prepared_events = _scan_signal_opportunities(day, seg_args)
    return {
        'rows': rows,
        'source': source,
        'prepared_events': prepared_events,
        'start_sec': int(start_sec),
        'end_sec': int(end_sec),
    }


def _scan_signal_opportunities_parallel(day: date, args) -> tuple[list[dict], str, int]:
    workers = worker_policy.clamp_workers(getattr(args, 'workers', 1))
    if workers <= 1:
        return _scan_signal_opportunities(day, args)

    _start_iso, _end_iso, start_sec, end_sec = replay._session_bounds_utc(day)
    entry_cutoff = replay._entry_cutoff_ts(day)
    decision_start_sec = int(getattr(args, 'decision_start_ts', 0) or 0)
    decision_end_sec = int(getattr(args, 'decision_end_ts', 0) or 0)
    scan_start = max(start_sec, decision_start_sec) if decision_start_sec > 0 else start_sec
    scan_end = min(end_sec, decision_end_sec) if decision_end_sec > 0 else end_sec
    scan_end = min(scan_end, entry_cutoff)
    if scan_start > scan_end:
        return _scan_signal_opportunities(day, args)
    if getattr(args, 'use_state_checkpoints', False):
        checkpoint = replay_state_checkpoints.ensure_day(
            getattr(args, 'checkpoint_dir', replay_state_checkpoints.DEFAULT_ROOT),
            args,
            day,
            bucket_sec=int(getattr(args, 'checkpoint_bucket_sec', 300) or 300),
        )
        print(
            f"[decision-tape] state checkpoints {day.isoformat()} "
            f"status={checkpoint.get('status')} count={checkpoint.get('checkpoint_count')}",
            flush=True,
        )

    span = scan_end - scan_start + 1
    workers = worker_policy.clamp_workers(workers, span)
    chunk = max(1, (span + workers - 1) // workers)
    ranges = []
    cur = scan_start
    while cur <= scan_end:
        end = min(scan_end, cur + chunk - 1)
        ranges.append((cur, end))
        cur = end + 1
    if len(ranges) <= 1:
        return _scan_signal_opportunities(day, args)

    args_dict = vars(args).copy()
    rows: list[dict] = []
    prepared_events = 0
    try:
        with ProcessPoolExecutor(max_workers=len(ranges)) as pool:
            futures = {
                pool.submit(_signal_scan_segment, day, args_dict, start, end): (start, end)
                for start, end in ranges
            }
            for fut in as_completed(futures):
                start, end = futures[fut]
                result = fut.result()
                segment_rows = result.get('rows') or []
                rows.extend(segment_rows)
                prepared_events = max(prepared_events, int(result.get('prepared_events') or 0))
                print(
                    f"[decision-tape] signal-scan segment {day.isoformat()} "
                    f"{start}-{end} rows={len(segment_rows)}",
                    flush=True,
                )
    except (PermissionError, OSError) as e:
        # Some Windows environments can block multiprocessing pipes/handles (WinError 5).
        # Fall back to the single-process scanner to keep Step 2 tape refresh functional.
        print(f"[decision-tape] ProcessPoolExecutor unavailable ({e}); falling back to single-process scan", flush=True)
        return _scan_signal_opportunities(day, args)
    rows.sort(key=lambda row: (int(row.get('ts') or 0), str(row.get('ticker') or ''), str(row.get('opportunity_id') or '')))
    return rows, 'raw_signal_scan_parallel', prepared_events


def _tape_path(args, day: date) -> str:
    tickers = '-'.join(args.tickers)
    suffix = f'_{args.profile_name}' if args.profile_name else ''
    indicator_mode = getattr(args, 'indicator_mode', 'live') or 'live'
    return os.path.join(
        args.out_dir,
        f'decision_tape_{args.feed}_{args.quote_mode}_{args.btc_mode}_{indicator_mode}_{tickers}{suffix}_{day.isoformat()}.jsonl.gz',
    )


def _merge_existing_rows(path: str, rows: list[dict]) -> tuple[list[dict], dict]:
    if not getattr(_merge_existing_rows, 'enabled', False) or not os.path.exists(path):
        return rows, {'enabled': False}
    existing = _read_jsonl_gz(path)
    by_key = {}
    for row in existing + rows:
        key = row.get('opportunity_id') or (
            row.get('day'),
            row.get('ticker'),
            row.get('ts'),
            row.get('original_side'),
            row.get('setup_type'),
        )
        by_key[key] = row
    merged = sorted(
        by_key.values(),
        key=lambda r: (str(r.get('day') or ''), int(r.get('ts') or 0), str(r.get('ticker') or '')),
    )
    return merged, {
        'enabled': True,
        'existing_rows': len(existing),
        'new_rows': len(rows),
        'merged_rows': len(merged),
    }


def _read_jsonl_gz(path: str) -> list[dict]:
    rows = []
    if not os.path.exists(path):
        return rows
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _price_series(events: list[dict], symbol: str, start_sec: int, end_sec: int) -> list[float | None]:
    by_sec: dict[int, float] = {}
    for event in events:
        if event.get('symbol') != symbol or event.get('kind') != 'stock_trade':
            continue
        if not replay_artifacts.is_executable_stock_trade(event.get('row') or {}):
            continue
        sec = int(event.get('t') or 0) // 1000
        if start_sec <= sec <= end_sec:
            price = replay._float((event.get('row') or {}).get('p'))
            if price is not None:
                by_sec[sec] = price
    series: list[float | None] = []
    last = None
    for sec in range(start_sec, end_sec + 1):
        if sec in by_sec:
            last = by_sec[sec]
        series.append(last)
    return series


def _outcome(side: str, ticker: str, entry_ts: int, entry_price: float,
             prices: list[Any], start_sec: int, flatten_ts: int, end_sec: int,
             args: argparse.Namespace | None = None) -> dict:
    cfg = replay.TICKER_CFG.get(ticker, {'sl': 0.05, 'tp': 0.01})
    sl_pct = float(cfg.get('sl') or 0.05)
    tp_pct = float(cfg.get('tp') or 0.01)
    bracket_price = entry_price
    active_entry_ts = entry_ts
    latency_meta = None
    latency_context = {'ticker': ticker, 'side': side, 'ts': entry_ts, 'price': entry_price}
    if args is not None and getattr(args, 'step2_latency_mode', 'off') != 'off':
        model = getattr(args, '_step2_latency_model', None) or step2_latency_model.load_model(
            getattr(args, 'step2_latency_model', None)
        )
        setattr(args, '_step2_latency_model', model)
        pct = getattr(args, 'step2_latency_percentile', 'p75')
        entry_delay_ms = step2_latency_model.latency_ms(model, ticker, side, 'entry_delay_ms', pct, context=latency_context)
        entry_slippage_bps = step2_latency_model.slippage_bps(model, ticker, side, 'entry_slippage_bps', 'p50', context=latency_context)
        active_entry_ts = entry_ts + int(round(entry_delay_ms / 1000.0))
        entry_price = float(step2_latency_model.apply_slippage(entry_price, side, entry_slippage_bps, 'entry'))
        latency_meta = {
            'mode': getattr(args, 'step2_latency_mode', 'off'),
            'percentile': pct,
            'entry_delay_ms': entry_delay_ms,
            'entry_slippage_bps': entry_slippage_bps,
            'entry_signal_price': round(float(bracket_price), 4),
            'entry_modeled_price': round(float(entry_price), 4),
            'entry_active_ts': active_entry_ts,
        }
    bracket = step2_execution_contract.exit_brackets(side, float(bracket_price), sl_pct, tp_pct)
    sl = bracket['sl']
    tp = bracket['tp']

    best = worst = entry_price
    exit_ts = end_sec
    exit_price = entry_price
    exit_evidence = None
    reason = 'end_of_data'
    for sec in range(max(active_entry_ts, start_sec), end_sec + 1):
        idx = sec - start_sec
        point = replay_artifacts._as_exit_point(prices[idx]) if 0 <= idx < len(prices) else replay_artifacts._legacy_exit_point(None)
        price = replay_artifacts._mark_price(point, side)
        if price is None:
            continue
        exit_price = float(price)
        if side == 'LONG':
            best = max(best, exit_price)
            worst = min(worst, exit_price)
            unrealized = exit_price - entry_price
        else:
            best = min(best, exit_price)
            worst = max(worst, exit_price)
            unrealized = entry_price - exit_price
        trigger = replay_artifacts._exit_trigger(side, point, sl, tp)
        if trigger:
            _unused, exit_price, reason, exit_evidence = trigger
            exit_ts = sec
            exit_evidence['model'] = replay_artifacts.EXIT_REPLAY_MODEL_VERSION
            break
        held_min = (sec - entry_ts) / 60
        if (
            replay.CONDITIONAL_TIME_STOP_ENABLED
            and held_min >= replay.COND_STOP_MIN
            and sec < flatten_ts
            and unrealized < 0
        ):
            exit_ts, reason = sec, 'cond_time_stop'
            break
        if sec >= flatten_ts:
            exit_ts, reason = sec, 'session_end'
            break

    if latency_meta and latency_meta.get('mode') == 'entry-exit':
        model = getattr(args, '_step2_latency_model', None) or step2_latency_model.load_model(
            getattr(args, 'step2_latency_model', None)
        )
        pct = getattr(args, 'step2_latency_percentile', 'p75')
        exit_delay_ms = step2_latency_model.latency_ms(model, ticker, side, 'exit_delay_ms', pct, context=latency_context)
        exit_slippage_bps = step2_latency_model.slippage_bps(model, ticker, side, 'exit_slippage_bps', 'p50', context=latency_context)
        exit_price = float(step2_latency_model.apply_slippage(float(exit_price), side, exit_slippage_bps, 'exit'))
        exit_ts += int(round(exit_delay_ms / 1000.0))
        latency_meta.update({
            'exit_delay_ms': exit_delay_ms,
            'exit_slippage_bps': exit_slippage_bps,
        })

    pnl_pct = (exit_price - entry_price) / entry_price * 100
    if side == 'SHORT':
        pnl_pct *= -1
    out = {
        'side': side,
        'entry': round(entry_price, 4),
        'exit': round(float(exit_price), 4),
        'exit_ts': int(exit_ts),
        'exit_ct': replay._iso_ct(int(exit_ts)),
        'held_sec': int(exit_ts - entry_ts),
        'reason': reason,
        'pnl_pct': round(pnl_pct, 6),
        'mfe_pct': round(abs(best - entry_price) / entry_price * 100, 6),
        'mae_pct': round(abs(worst - entry_price) / entry_price * 100, 6),
        'exit_evidence': exit_evidence,
    }
    if latency_meta:
        out['latency_model'] = latency_meta
    return out


def _outcome_from_trade(row: dict, entry_ts: int) -> dict | None:
    if not row:
        return None
    side = str(row.get('side') or '').upper()
    if side not in ('LONG', 'SHORT'):
        return None
    held_sec = int(row.get('held_sec') or 0)
    return {
        'side': side,
        'entry': round(float(row.get('entry') or 0.0), 4),
        'exit': round(float(row.get('exit') or 0.0), 4),
        'exit_ts': int(row.get('exit_ts') or (int(entry_ts) + held_sec)),
        'exit_ct': row.get('exit_ct'),
        'held_sec': held_sec,
        'reason': row.get('reason') or 'unknown',
        'pnl_pct': round(float(row.get('pnl_pct') or 0.0), 6),
        'mfe_pct': round(float(row.get('mfe_pct') or 0.0), 6),
        'mae_pct': round(float(row.get('mae_pct') or 0.0), 6),
        'source': 'accepted_live_trade',
    }


def _num(value: Any) -> float | None:
    try:
        if value in (None, ''):
            return None
        return float(value)
    except Exception:
        return None


def _compact_gate_features(sig: dict, ind: dict, btc: dict) -> dict:
    btc_ctx = sig.get('btc_context') or {}
    relative = sig.get('relative_strength') or {}
    flow_30s = ind.get('flow_30s') or {}
    miner = sig.get('miner_basket') or {}
    scoring = sig.get('scoring_profile') or {}
    scoring_features = scoring.get('features') or {}
    return {
        'schema_version': 1,
        'ticker_session_return_pct': _num(ind.get('session_return_pct')),
        'btc_session_return_pct': _num(btc.get('session_return_pct')),
        'vwap_dist': _num(ind.get('vwap_dist')),
        'vwap_dist_sigma': _num(ind.get('vwap_dist_sigma')),
        'session_range_pos': _num(ind.get('session_range_pos')),
        'mom_5s': _num(ind.get('mom_5s')),
        'mom_15s': _num(ind.get('mom_15s')),
        'mom_60s': _num(ind.get('mom_60s')),
        'btc_mom_60s': _num(btc_ctx.get('mom_60s') if btc_ctx else btc.get('mom_60s')),
        'flow_30s_buy_pct': _num(flow_30s.get('buy_pct')),
        'flow_30s_ratio': _num(flow_30s.get('ratio')),
        'flow_30s_delta': _num(ind.get('flow_30s_delta')),
        'stock_minus_btc_implied_60s': _num(
            relative.get('stock_minus_btc_implied_60s')
            if relative.get('stock_minus_btc_implied_60s') is not None
            else btc_ctx.get('stock_minus_btc_implied_60s')
        ),
        'btc_regime': sig.get('btc_regime') or btc_ctx.get('regime'),
        'btc_regime_detail': btc_ctx.get('regime_detail'),
        'ema_stack': ind.get('ema_stack'),
        'miner_basket_state': miner.get('state'),
        'miner_basket_score': _num(miner.get('score')),
        'execution_score': _num((sig.get('execution_quality') or {}).get('score')),
        'flow_fade_confirmed': (sig.get('signal_quality') or {}).get('flow_fade_confirmed'),
        'profile_original_side': sig.get('scoring_profile_original_side'),
        'profile_score': _num(scoring.get('score')),
        'profile_features': scoring_features,
        'entry_chop_metrics': {
            'range_180s_pct': _num(ind.get('chop_range_180s_pct')),
            'efficiency_180s': _num(ind.get('chop_efficiency_180s')),
            'flips_180s': _num(ind.get('chop_flips_180s')),
            'range_300s_pct': _num(ind.get('chop_range_300s_pct')),
            'efficiency_300s': _num(ind.get('chop_efficiency_300s')),
            'flips_300s': _num(ind.get('chop_flips_300s')),
        },
    }


def _compact_opportunity(opp: dict, outcomes: dict[str, dict]) -> dict:
    sig = opp.get('signal') or {}
    features = opp.get('features') or {}
    ind = features.get('ticker_indicators') or sig.get('indicators') or {}
    btc = features.get('btc_indicators') or sig.get('btc_indicators') or {}
    profile_snapshot = sig.get('scoring_profile') or {}
    preserved_features = profile_snapshot.get('features') or {}
    if preserved_features:
        model_features = preserved_features
    else:
        model_features = scoring_profiles.features_from_signal(sig, ind, btc)
    execution_quality = sig.get('execution_quality') or {}
    original_side = sig.get('scoring_profile_original_side') or sig.get('side') or opp.get('side')
    original_score = sig.get('scoring_profile_original_score')
    if original_score is None:
        original_score = sig.get('score') if sig.get('score') is not None else opp.get('score')
    return {
        'schema_version': 2,
        'type': 'decision_tape_row',
        'indicator_mode': opp.get('indicator_mode'),
        'opportunity_id': opp.get('opportunity_id'),
        'day': opp.get('day'),
        'ts': int(opp.get('ts') or 0),
        'ts_ct': opp.get('ts_ct'),
        'ticker': opp.get('ticker'),
        'original_side': original_side,
        'setup_type': sig.get('setup_type') or opp.get('setup_type'),
        'price': replay._float(sig.get('price') or opp.get('price')),
        'score': original_score,
        'conviction': sig.get('conviction') or opp.get('conviction'),
        'session_phase': sig.get('session_phase') or opp.get('session_phase'),
        'btc_regime': sig.get('btc_regime') or opp.get('btc_regime'),
        'source_decision': opp.get('decision'),
        'source_reject_reason': opp.get('reject_reason'),
        'exec_score': execution_quality.get('score'),
        'exec_reasons': execution_quality.get('reasons') or [],
        'reasons': sig.get('reasons') or [],
        'model_features': model_features,
        'gate_features': _compact_gate_features(sig, ind, btc),
        'outcomes': outcomes,
        'notes': [
            'Reusable scoring tape row. Valid for scoring/direction/entry-quality research against captured decision points.',
        ],
    }


def _refresh_existing_signal_rows(day: date, args, rows: list[dict], out_path: str) -> dict:
    tape_args = argparse.Namespace(
        tickers=args.tickers,
        feed=args.feed,
        quote_mode=args.quote_mode,
        btc_mode=args.btc_mode,
        cache_dir=args.cache_dir,
        prepared_cache_dir=args.prepared_cache_dir,
        use_prepared_events=True,
        refresh_prepared_events=False,
        refresh=False,
        max_pages=1000,
    )
    stats = replay.ReplayStats()
    events = replay._load_day_events(day, tape_args, stats)
    _start_iso, _end_iso, start_sec, end_sec = replay._session_bounds_utc(day)
    flatten_ts = replay._flatten_ts(day)
    artifact_key = replay_artifacts.key_from_args(args)
    outcome_shards = {}
    for ticker in args.tickers:
        if getattr(args, 'use_outcome_shards', True):
            if getattr(args, 'step2_latency_mode', 'off') != 'off':
                outcome_shards[ticker] = replay_artifacts.read_latency_outcome_shard(
                    args.artifact_dir,
                    artifact_key,
                    day,
                    ticker,
                    getattr(args, 'step2_latency_mode', 'off'),
                    getattr(args, 'step2_latency_percentile', 'p75'),
                    getattr(args, 'step2_latency_model', None),
                )
            else:
                outcome_shards[ticker] = replay_artifacts.read_outcome_shard(args.artifact_dir, artifact_key, day, ticker)
    prices = {}
    price_hashes = {}
    for ticker in args.tickers:
        _ts_values, prices[ticker] = replay_artifacts.exit_series_from_events(events, ticker, start_sec, end_sec)
        price_hashes[ticker] = opportunity_outcome_cache.price_series_hash(prices[ticker])

    outcome_sources = defaultdict(int)
    refreshed = []
    for row in rows:
        ticker = str(row.get('ticker') or '').upper()
        entry_price = replay._float(row.get('price'))
        entry_ts = int(row.get('ts') or 0)
        if ticker not in prices or not entry_price or entry_ts < start_sec:
            refreshed.append(row)
            outcome_sources['missing_price_context'] += 1
            continue
        shard_row = (outcome_shards.get(ticker) or {}).get(entry_ts)
        shard_price = replay._float((shard_row or {}).get('price'))
        entry_matches_shard = (
            shard_price is not None
            and abs(float(shard_price) - float(entry_price)) <= 0.0001
        )
        if shard_row and entry_matches_shard and shard_row.get('LONG') and shard_row.get('SHORT'):
            outcomes = {'LONG': shard_row['LONG'], 'SHORT': shard_row['SHORT']}
            outcome_sources['outcome_shard'] += 1
        else:
            outcomes, cache_meta = opportunity_outcome_cache.get_or_compute(
                day.isoformat(),
                ticker,
                entry_ts,
                float(entry_price),
                prices[ticker],
                start_sec,
                flatten_ts,
                end_sec,
                args,
                lambda: {
                    'LONG': _outcome('LONG', ticker, entry_ts, float(entry_price), prices[ticker], start_sec, flatten_ts, end_sec, args),
                    'SHORT': _outcome('SHORT', ticker, entry_ts, float(entry_price), prices[ticker], start_sec, flatten_ts, end_sec, args),
                },
                precomputed_price_hash=price_hashes.get(ticker),
            )
            if shard_row and not entry_matches_shard:
                outcome_sources['computed_inline_price_mismatch'] += 1
            else:
                outcome_sources[str(cache_meta.get('source') or 'computed_inline')] += 1
        new_row = dict(row)
        new_row['outcomes'] = outcomes
        notes = list(new_row.get('notes') or [])
        notes.append('Outcomes refreshed from existing signal/features tape without rescanning indicators.')
        new_row['notes'] = notes
        new_row['outcome_refresh'] = {
            'mode': 'reuse_existing_signals',
            'step2_latency_mode': getattr(args, 'step2_latency_mode', 'off'),
            'step2_latency_percentile': getattr(args, 'step2_latency_percentile', 'p75'),
        }
        refreshed.append(new_row)
    tape_identity = _write_jsonl_gz(out_path, refreshed)
    manifest = replay_artifacts.write_manifest(
        args.artifact_dir,
        args,
        day,
        {
            'decision_tape': out_path,
            'decision_tape_rows': len(refreshed),
            'decision_tape_schema_version': 2,
            'indicator_mode': getattr(args, 'indicator_mode', 'live'),
            'step2_latency_mode': getattr(args, 'step2_latency_mode', 'off'),
            'step2_latency_model': os.path.abspath(getattr(args, 'step2_latency_model', step2_latency_model.DEFAULT_MODEL_PATH)),
            'step2_latency_percentile': getattr(args, 'step2_latency_percentile', 'p75'),
            'exit_replay_model': replay_artifacts.EXIT_REPLAY_MODEL_VERSION,
            'outcome_sources': dict(outcome_sources),
            'signal_source': 'reused_existing_decision_tape',
            'decision_tape_identity': tape_identity,
        },
    )
    return {
        'day': day.isoformat(),
        'rows': len(refreshed),
        'status': 'ok',
        'opportunity_path': out_path,
        'out': out_path,
        'prepared_events': len(events),
        'artifact_manifest': replay_artifacts.manifest_path(args.artifact_dir, artifact_key, day),
        'fingerprint': manifest.get('fingerprint'),
        'decision_tape_identity': tape_identity,
        'outcome_sources': dict(outcome_sources),
        'merge_summary': {'enabled': False, 'mode': 'reuse_existing_signals'},
        'min_ts': min((int(row.get('ts') or 0) for row in refreshed), default=0),
        'max_ts': max((int(row.get('ts') or 0) for row in refreshed), default=0),
    }


def build_day(day: date, args) -> dict:
    out_path = _tape_path(args, day)
    if getattr(args, 'reuse_existing_signals', False) and os.path.exists(out_path):
        existing_rows = _read_jsonl_gz(out_path)
        if existing_rows:
            return _refresh_existing_signal_rows(day, args, existing_rows, out_path)
    if args.source == 'signal-scan':
        opportunities, opp_path, prepared_events = _scan_signal_opportunities_parallel(day, args)
    else:
        opportunities, opp_path = _load_opportunities(args, day)
        prepared_events = None
    if not opportunities:
        if args.source == 'signal-scan':
            if getattr(args, 'append_existing', False) and os.path.exists(out_path):
                existing_rows = _read_jsonl_gz(out_path)
                tape_identity = _write_jsonl_gz(out_path, existing_rows)
                row_count = len(existing_rows)
            else:
                tape_identity = _write_jsonl_gz(out_path, [])
                row_count = 0
            return {
                'day': day.isoformat(),
                'rows': row_count,
                'status': 'ok',
                'opportunity_path': opp_path,
                'out': out_path,
                'prepared_events': prepared_events,
                'empty_signal_scan': True,
                'decision_tape_identity': tape_identity,
                'min_ts': 0,
                'max_ts': 0,
            }
        return {'day': day.isoformat(), 'rows': 0, 'status': 'missing_opportunity_ledger', 'opportunity_path': opp_path}
    tape_args = argparse.Namespace(
        tickers=args.tickers,
        feed=args.feed,
        quote_mode=args.quote_mode,
        btc_mode=args.btc_mode,
        cache_dir=args.cache_dir,
        prepared_cache_dir=args.prepared_cache_dir,
        use_prepared_events=True,
        refresh_prepared_events=False,
        refresh=False,
        max_pages=1000,
    )
    stats = replay.ReplayStats()
    events = replay._load_day_events(day, tape_args, stats)
    if prepared_events is None:
        prepared_events = len(events)
    _start_iso, _end_iso, start_sec, end_sec = replay._session_bounds_utc(day)
    flatten_ts = replay._flatten_ts(day)
    artifact_key = replay_artifacts.key_from_args(args)
    outcome_shards = {}
    for ticker in args.tickers:
        if getattr(args, 'use_outcome_shards', True):
            if getattr(args, 'step2_latency_mode', 'off') != 'off':
                outcome_shards[ticker] = replay_artifacts.read_latency_outcome_shard(
                    args.artifact_dir,
                    artifact_key,
                    day,
                    ticker,
                    getattr(args, 'step2_latency_mode', 'off'),
                    getattr(args, 'step2_latency_percentile', 'p75'),
                    getattr(args, 'step2_latency_model', None),
                )
            else:
                outcome_shards[ticker] = replay_artifacts.read_outcome_shard(args.artifact_dir, artifact_key, day, ticker)
    prices = {}
    price_hashes = {}
    for ticker in args.tickers:
        _ts_values, prices[ticker] = replay_artifacts.exit_series_from_events(events, ticker, start_sec, end_sec)
        price_hashes[ticker] = opportunity_outcome_cache.price_series_hash(prices[ticker])
    rows = []
    outcome_sources = defaultdict(int)
    for opp in opportunities:
        ticker = opp.get('ticker')
        entry_price = replay._float((opp.get('signal') or {}).get('price') or opp.get('price'))
        entry_ts = int(opp.get('ts') or 0)
        if ticker not in prices or not entry_price or entry_ts < start_sec:
            continue
        shard_row = (outcome_shards.get(ticker) or {}).get(entry_ts)
        shard_price = replay._float((shard_row or {}).get('price'))
        entry_matches_shard = (
            shard_price is not None
            and abs(float(shard_price) - float(entry_price)) <= 0.0001
        )
        if shard_row and entry_matches_shard and shard_row.get('LONG') and shard_row.get('SHORT'):
            outcomes = {'LONG': shard_row['LONG'], 'SHORT': shard_row['SHORT']}
            outcome_sources['outcome_shard'] += 1
        else:
            outcomes, cache_meta = opportunity_outcome_cache.get_or_compute(
                day.isoformat(),
                ticker,
                entry_ts,
                float(entry_price),
                prices[ticker],
                start_sec,
                flatten_ts,
                end_sec,
                args,
                lambda: {
                    'LONG': _outcome('LONG', ticker, entry_ts, float(entry_price), prices[ticker], start_sec, flatten_ts, end_sec, args),
                    'SHORT': _outcome('SHORT', ticker, entry_ts, float(entry_price), prices[ticker], start_sec, flatten_ts, end_sec, args),
                },
                precomputed_price_hash=price_hashes.get(ticker),
            )
            if shard_row and not entry_matches_shard:
                outcome_sources['computed_inline_price_mismatch'] += 1
            else:
                outcome_sources[str(cache_meta.get('source') or 'computed_inline')] += 1
        live_outcome = _outcome_from_trade(opp.get('outcome') or {}, entry_ts)
        if live_outcome:
            outcomes[live_outcome['side']] = live_outcome
            outcome_sources['accepted_live_trade'] += 1
        rows.append(_compact_opportunity(opp, outcomes))
    _merge_existing_rows.enabled = bool(getattr(args, 'append_existing', False))
    rows, merge_summary = _merge_existing_rows(out_path, rows)
    tape_identity = _write_jsonl_gz(out_path, rows)
    manifest = replay_artifacts.write_manifest(
        args.artifact_dir,
        args,
        day,
        {
            'decision_tape': out_path,
            'decision_tape_rows': len(rows),
            'decision_tape_schema_version': 2,
            'indicator_mode': getattr(args, 'indicator_mode', 'live'),
            'step2_latency_mode': getattr(args, 'step2_latency_mode', 'off'),
            'step2_latency_model': os.path.abspath(getattr(args, 'step2_latency_model', step2_latency_model.DEFAULT_MODEL_PATH)),
            'step2_latency_percentile': getattr(args, 'step2_latency_percentile', 'p75'),
            'exit_replay_model': replay_artifacts.EXIT_REPLAY_MODEL_VERSION,
            'outcome_sources': dict(outcome_sources),
            'decision_tape_identity': tape_identity,
        },
    )
    return {
        'day': day.isoformat(),
        'rows': len(rows),
        'status': 'ok',
        'opportunity_path': opp_path,
        'out': out_path,
        'prepared_events': prepared_events,
        'artifact_manifest': replay_artifacts.manifest_path(args.artifact_dir, artifact_key, day),
        'fingerprint': manifest.get('fingerprint'),
        'decision_tape_identity': tape_identity,
        'outcome_sources': dict(outcome_sources),
        'merge_summary': merge_summary,
        'min_ts': min((int(row.get('ts') or 0) for row in rows), default=0),
        'max_ts': max((int(row.get('ts') or 0) for row in rows), default=0),
    }


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Build reusable decision tapes from opportunity ledgers.')
    ap.add_argument('--start', required=True)
    ap.add_argument('--end', required=True)
    ap.add_argument('--tickers', nargs='+', default=replay.TICKERS)
    ap.add_argument('--feed', default='sip')
    ap.add_argument('--quote-mode', default='per-second')
    ap.add_argument('--btc-mode', default='bars')
    ap.add_argument('--opportunity-dir', default=os.path.join(replay.DEFAULT_OUT_DIR, 'opportunities'))
    ap.add_argument('--prepared-cache-dir', default=replay.DEFAULT_PREPARED_DIR)
    ap.add_argument('--cache-dir', default=replay.DEFAULT_CACHE_DIR)
    ap.add_argument('--out-dir', default=DEFAULT_OUT_DIR)
    ap.add_argument('--artifact-dir', default=replay_artifacts.DEFAULT_ARTIFACT_DIR)
    ap.add_argument('--no-outcome-shards', action='store_true', help='Ignore prebuilt outcome shards and compute outcomes inline.')
    ap.add_argument('--profile-name', default='', help='Optional suffix for profile-specific opportunity ledgers.')
    ap.add_argument('--source', choices=['signal-scan', 'opportunity-ledger'], default='signal-scan',
                    help='Build from raw detect_signal stream or from existing opportunity ledgers.')
    ap.add_argument('--indicator-mode', choices=['live', 'fast'], default='live')
    ap.add_argument('--workers', type=int, default=1)
    ap.add_argument('--decision-start-ts', type=int, default=0,
                    help='Only emit decision rows at/after this epoch second; earlier events still warm indicators.')
    ap.add_argument('--decision-end-ts', type=int, default=0,
                    help='Only emit decision rows at/before this epoch second.')
    ap.add_argument('--append-existing', action='store_true',
                    help='Merge new decision rows into the existing day tape instead of replacing it.')
    ap.add_argument('--reuse-existing-signals', action='store_true',
                    help='Reuse existing decision-tape signal/features rows and refresh only outcomes.')
    ap.add_argument('--disable-active-scoring-profile', action='store_true',
                    help='Diagnostic only: scan raw signals without the live active scoring profile.')
    ap.add_argument('--use-state-checkpoints', action='store_true',
                    help='Use exact serialized replay state checkpoints to warm incremental signal scans.')
    ap.add_argument('--checkpoint-dir', default=replay_state_checkpoints.DEFAULT_ROOT)
    ap.add_argument('--checkpoint-bucket-sec', type=int, default=300)
    ap.add_argument('--step2-latency-mode', choices=['off', 'entry', 'entry-exit'], default='off',
                    help='Apply modeled live fill delay/slippage to generated Step 2 outcomes.')
    ap.add_argument('--step2-latency-model', default=step2_latency_model.DEFAULT_MODEL_PATH)
    ap.add_argument('--step2-latency-percentile', choices=['p50', 'p75', 'p95', 'default'], default='p75')
    args = ap.parse_args()
    args.use_outcome_shards = not args.no_outcome_shards
    return args


def main() -> int:
    args = parse_args()
    args.tickers = [t.upper() for t in args.tickers]
    start = replay._parse_day(args.start)
    end = replay._parse_day(args.end)
    days = replay._market_days(start, end)
    requested_workers = worker_policy.clamp_workers(args.workers)
    workers = worker_policy.clamp_workers(requested_workers, len(days) or 1)
    if workers > 1:
        args_dict = vars(args).copy()
        args_dict['workers'] = 1
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(build_day, day, argparse.Namespace(**args_dict)): day
                for day in days
            }
            results = []
            for fut in as_completed(futures):
                row = fut.result()
                results.append(row)
                print(f"[decision-tape] {row.get('day')} rows={row.get('rows')} status={row.get('status')}", flush=True)
        results.sort(key=lambda row: row.get('day') or '')
    else:
        args.workers = requested_workers
        results = [build_day(day, args) for day in days]
    print(json.dumps({
        'days': len(results),
        'rows': sum(int(r.get('rows') or 0) for r in results),
        'out_dir': os.path.abspath(args.out_dir),
        'results': results,
    }, indent=2))
    return 1 if any(r.get('status') != 'ok' for r in results) else 0


if __name__ == '__main__':
    raise SystemExit(main())
