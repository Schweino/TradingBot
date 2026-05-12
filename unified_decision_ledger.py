from __future__ import annotations

from output_paths import output_path

import gzip
import json
import os
import time
from collections import Counter
from datetime import datetime
from typing import Any

import numpy as np

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import backtest_30d_engine as replay
import decision_tape_compiled as compiled_tape
import routed_scoring_profile
import execution_kernel
import step2_parity_contract
import tournament_safety


HERE = os.path.dirname(os.path.abspath(__file__))
CT = ZoneInfo('America/Chicago')
DEFAULT_OUT_DIR = output_path('postmortem', 'unified_decision_ledger')
SCHEMA_VERSION = 1


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec='seconds')


def _write_json(path: str, payload: dict[str, Any]) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f'{path}.{os.getpid()}.{int(time.time() * 1000)}.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)
    os.replace(tmp, path)
    return os.path.abspath(path)


def _append_jsonl(path: str, rows: list[dict[str, Any]], replace: bool = True) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    mode = 'w' if replace else 'a'
    with open(path, mode, encoding='utf-8') as f:
        for row in rows:
            f.write(json.dumps(row, separators=(',', ':'), sort_keys=True, default=str) + '\n')
    return os.path.abspath(path)


def _read_jsonl_gz(path: str) -> list[dict[str, Any]]:
    if not path or not os.path.exists(path):
        return []
    rows: list[dict[str, Any]] = []
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except Exception:
                    pass
    return rows


def _reverse_map(mapping: dict[str, int]) -> dict[int, str]:
    return {int(v): str(k) for k, v in (mapping or {}).items()}


def _reason_name(code: int) -> str:
    for name, value in compiled_tape.REASON_CODE.items():
        if int(value) == int(code):
            return name or 'unknown'
    return 'unknown'


def _side_name(side: int) -> str:
    if int(side) == compiled_tape.SIDE_LONG:
        return 'LONG'
    if int(side) == compiled_tape.SIDE_SHORT:
        return 'SHORT'
    return 'SKIP'


def _compiled_source_rows(compiled: dict[str, Any]) -> list[dict[str, Any]]:
    manifest = compiled.get('manifest') or {}
    rows: list[dict[str, Any]] = []
    for path in manifest.get('source_paths') or []:
        rows.extend(_read_jsonl_gz(path))
    return rows


def _compiled_run_context(compiled: dict[str, Any], variant: Any,
                          starting_balance: float, sim_config: dict[str, Any] | None) -> dict[str, Any]:
    manifest = compiled.get('manifest') or {}
    routes = routed_scoring_profile.routes_to_dicts(variant) if routed_scoring_profile.is_routed_variant(variant) else []
    model_payload = {
        'name': getattr(variant, 'name', ''),
        'weights': getattr(variant, 'weights', {}) or {},
        'bias': float(getattr(variant, 'bias', 0.0) or 0.0),
    }
    if routes:
        model_payload['routes'] = routes
    model_id = tournament_safety.stable_json_hash(model_payload, length=20) if routes else tournament_safety.model_id(
        getattr(variant, 'name', ''),
        getattr(variant, 'weights', {}) or {},
        float(getattr(variant, 'bias', 0.0) or 0.0),
    )
    return {
        'schema_version': SCHEMA_VERSION,
        'engine': 'compiled_step2',
        'compiled_manifest': os.path.abspath(manifest.get('manifest_path') or ''),
        'compiled_tape_hash': manifest.get('compiled_tape_hash'),
        'compiled_rows': int(compiled.get('rows') or 0),
        'variant': getattr(variant, 'name', ''),
        'weights': dict(getattr(variant, 'weights', {}) or {}),
        'bias': float(getattr(variant, 'bias', 0.0) or 0.0),
        'routes': routes,
        'routed_scoring_profile': bool(routes),
        'model_id': model_id,
        'starting_balance': float(starting_balance),
        'sim_config': dict(sim_config or {}),
        'sim_config_hash': tournament_safety.stable_json_hash(dict(sim_config or {}), length=32),
        'execution_kernel_contract': execution_kernel.contract_from_config({'step2': dict(sim_config or {})}),
        'execution_kernel_hash': execution_kernel.contract_from_config({'step2': dict(sim_config or {})}).get('execution_kernel_hash'),
    }


def trace_step2_decisions(compiled: dict[str, Any], variant: Any, starting_balance: float,
                          gate: dict[str, Any] | None = None,
                          sim_config: dict[str, Any] | None = None,
                          include_rejected: bool = True) -> dict[str, Any]:
    """Trace one variant through the same state gates as the compiled Step 2 scorer."""
    sides = compiled_tape.side_matrix(compiled, [variant])[0]
    route_attrs = (
        routed_scoring_profile.route_attribution(compiled, variant)
        if routed_scoring_profile.is_routed_variant(variant)
        else None
    )
    mode = compiled_tape._gate_mode_code(gate)
    threshold = float((gate or {}).get('ticker_session_return_below_pct',
                                       (gate or {}).get('threshold_pct', -0.5)))
    if mode == 0 and int(compiled_tape._sim_config_value(sim_config, 'use_live_long_gate', 1.0)) > 0:
        mode = int(compiled.get('live_long_gate_mode') or 0)
        threshold = float(compiled.get('live_long_gate_threshold') or threshold)

    setup_map = compiled.get('setup_map') or {}
    setup_requires_state = np.zeros(int(compiled['setup_count']), dtype=np.int8)
    setup_state_trigger_kind = np.zeros(int(compiled['setup_count']), dtype=np.int8)
    for name, raw_idx in setup_map.items():
        idx = int(raw_idx)
        if name == 'flow_exhaustion_fade':
            setup_requires_state[idx] = 1
            setup_state_trigger_kind[idx] = 1
        elif name == 'vwap_reclaim_breakdown':
            setup_requires_state[idx] = 1
            setup_state_trigger_kind[idx] = 2

    ticker_names = _reverse_map(compiled.get('ticker_map') or {})
    setup_names = _reverse_map(compiled.get('setup_map') or {})
    day_names = _reverse_map(compiled.get('day_map') or {})
    source_rows = _compiled_source_rows(compiled)
    context = _compiled_run_context(compiled, variant, starting_balance, sim_config)

    ticker_count = int(compiled['ticker_count'])
    setup_count = int(compiled['setup_count'])
    day_count = int(compiled.get('day_count') or 1)
    balance = float(starting_balance)
    trade_size_pct = float(replay.TRADE_SIZE_PCT)
    open_until = np.zeros(ticker_count, dtype=np.int64)
    stop_cooldown_until = np.zeros(ticker_count, dtype=np.int64)
    loss_cluster_until = np.zeros((ticker_count, 2, setup_count), dtype=np.int64)
    loss_cluster_recent = np.zeros((ticker_count, 2, setup_count, 8), dtype=np.int64)
    loss_cluster_recent_count = np.zeros((ticker_count, 2, setup_count), dtype=np.int64)
    last_signal = np.zeros((ticker_count, 2, setup_count), dtype=np.float64)
    setup_armed_at = np.zeros((ticker_count, 2, setup_count), dtype=np.float64)
    day_trade_count = np.zeros(day_count, dtype=np.int64)
    ticker_day_trade_count = np.zeros((ticker_count, day_count), dtype=np.int64)

    rows: list[dict[str, Any]] = []
    pnl_total = 0.0
    wins = 0
    losses = 0
    accepted = 0
    rejected = 0
    reject_reasons: Counter[str] = Counter()

    use_short_recovery_guard = int(compiled_tape._sim_config_value(sim_config, 'use_live_short_guard', 0.0))
    min_exec_score = compiled_tape._sim_config_value(sim_config, 'min_exec_score', 0.0)
    admission_mode = int(compiled_tape._sim_config_value(sim_config, 'admission_mode', 0.0))
    require_conviction = int(compiled_tape._sim_config_value(sim_config, 'require_conviction', 1.0))
    setup_state_enabled = int(compiled_tape._sim_config_value(sim_config, 'setup_state_enabled', 1.0))
    state_min_confirm_sec = compiled_tape._sim_config_value(sim_config, 'state_min_confirm_sec', 5.0)
    state_max_confirm_sec = compiled_tape._sim_config_value(sim_config, 'state_max_confirm_sec', 45.0)
    stop_loss_cooldown_sec = int(compiled_tape._sim_config_value(sim_config, 'stop_loss_cooldown_sec', 0.0))
    loss_cluster_window_sec = int(compiled_tape._sim_config_value(sim_config, 'loss_cluster_window_sec', 0.0))
    loss_cluster_count = int(compiled_tape._sim_config_value(sim_config, 'loss_cluster_count', 0.0))
    loss_cluster_cooldown_sec = int(compiled_tape._sim_config_value(sim_config, 'loss_cluster_cooldown_sec', 0.0))
    use_step2_brs_long_guard = int(compiled_tape._sim_config_value(sim_config, 'use_step2_brs_long_guard', 1.0))
    brs_setup_code = int(setup_map.get('btc_relative_strength', -1))
    same_ticker_reentry_cooldown_sec = int(compiled_tape._sim_config_value(sim_config, 'same_ticker_reentry_cooldown_sec', 0.0))
    max_trades_per_day = int(compiled_tape._sim_config_value(sim_config, 'max_trades_per_day', 0.0))
    max_trades_per_ticker_day = int(compiled_tape._sim_config_value(sim_config, 'max_trades_per_ticker_day', 0.0))

    def reject(i: int, reason: str, side: int) -> None:
        nonlocal rejected
        rejected += 1
        reject_reasons[reason] += 1
        if not include_rejected:
            return
        src = source_rows[i] if i < len(source_rows) else {}
        tkr = int(compiled['ticker_code'][i])
        setup = int(compiled['setup_code'][i])
        day_idx = int(compiled['day_code'][i])
        rows.append({
            'schema_version': SCHEMA_VERSION,
            'source': 'step2_compiled_trace',
            'decision_status': 'rejected',
            'reject_reason': reason,
            'row_index': int(i),
            'day': day_names.get(day_idx, str(day_idx)),
            'ts': int(compiled['ts'][i]),
            'ts_ct': src.get('ts_ct'),
            'ticker': ticker_names.get(tkr, str(tkr)),
            'setup_type': setup_names.get(setup, str(setup)),
            'side': _side_name(side),
            'original_side': _side_name(int(compiled['original_side'][i])),
            'opportunity_id': src.get('opportunity_id'),
            'price': src.get('price'),
            'conviction': src.get('conviction'),
            'source_decision': src.get('source_decision'),
            'model_id': context['model_id'],
            'run_context': context,
            'route_name': str(route_attrs['route_name'][i]) if route_attrs else 'fallback',
            'route_action': str(route_attrs['route_action'][i]) if route_attrs else 'score',
            'route_matched': bool(route_attrs['route_matched'][i]) if route_attrs else False,
        })

    for i in range(int(compiled['rows'])):
        tkr = int(compiled['ticker_code'][i])
        t = int(compiled['ts'][i])
        d = int(compiled['day_code'][i])
        setup = int(compiled['setup_code'][i])
        side = int(sides[i])
        source_decision = int(compiled['source_decision_code'][i])
        if admission_mode == 1 and source_decision != 1:
            reject(i, 'admission_requires_live_accept', side)
            continue
        if admission_mode == 2 and source_decision == 2:
            reject(i, 'admission_blocks_live_reject', side)
            continue
        if t < open_until[tkr]:
            reject(i, 'open_position', side)
            continue
        if stop_loss_cooldown_sec > 0 and t < stop_cooldown_until[tkr]:
            reject(i, 'stop_loss_cooldown', side)
            continue
        if require_conviction > 0 and int(compiled['conviction_ok'][i]) == 0:
            reject(i, 'below_min_conviction', side)
            continue
        if float(compiled['exec_score'][i]) < min_exec_score:
            reject(i, 'execution_quality_low', side)
            continue
        if max_trades_per_day > 0 and day_trade_count[d] >= max_trades_per_day:
            reject(i, 'max_trades_per_day', side)
            continue
        if max_trades_per_ticker_day > 0 and ticker_day_trade_count[tkr, d] >= max_trades_per_ticker_day:
            reject(i, 'max_trades_per_ticker_day', side)
            continue
        if side == compiled_tape.SIDE_SKIP:
            reject(i, 'routed_profile_skip', side)
            continue
        if (use_step2_brs_long_guard > 0 and side == compiled_tape.SIDE_LONG
                and setup == brs_setup_code and int(compiled['conviction_high'][i]) == 0):
            mom60 = float(compiled['btc_mom_60'][i])
            if not np.isnan(mom60) and mom60 < 0.0:
                reject(i, 'step2_brs_long_btc_negative_mom60', side)
                continue
        if use_short_recovery_guard > 0 and side == compiled_tape.SIDE_SHORT and int(compiled['short_recovery_blocked'][i]) == 1:
            reject(i, 'short_recovery_guard', side)
            continue
        if side == compiled_tape.SIDE_LONG and mode > 0 and float(compiled['ticker_session_return_pct'][i]) < threshold:
            blocked = (
                mode == 1
                or (mode == 2 and float(compiled['vwap_dist'][i]) < 0.0)
                or (mode == 3 and int(compiled['btc_bullish'][i]) == 0)
                or (mode == 4 and float(compiled['vwap_dist'][i]) < 0.0 and int(compiled['btc_bullish'][i]) == 0)
            )
            if blocked:
                reject(i, 'entry_gate', side)
                continue
        side_idx = 0 if side == compiled_tape.SIDE_LONG else 1
        if loss_cluster_cooldown_sec > 0 and t < loss_cluster_until[tkr, side_idx, setup]:
            reject(i, 'loss_cluster_throttle', side)
            continue
        if setup_state_enabled > 0 and setup_requires_state[setup] == 1:
            trigger_kind = int(setup_state_trigger_kind[setup])
            trigger = int(compiled['flow_fade_confirmed'][i]) == 1 if trigger_kind == 1 else -1.0 <= float(compiled['vwap_dist_sigma'][i]) <= 1.0
            if not trigger:
                setup_armed_at[tkr, side_idx, setup] = 0.0
                reject(i, 'setup_state_not_triggered', side)
                continue
            armed = float(setup_armed_at[tkr, side_idx, setup])
            if armed <= 0.0:
                setup_armed_at[tkr, side_idx, setup] = float(t)
                reject(i, 'setup_state_arming', side)
                continue
            age = float(t) - armed
            if age > state_max_confirm_sec:
                setup_armed_at[tkr, side_idx, setup] = float(t)
                reject(i, 'setup_state_expired', side)
                continue
            if age < state_min_confirm_sec:
                reject(i, 'setup_state_pending', side)
                continue
            setup_armed_at[tkr, side_idx, setup] = 0.0
        if float(t) - float(last_signal[tkr, side_idx, setup]) < float(compiled['cooldown_by_setup'][setup]):
            reject(i, 'cooldown', side)
            continue

        pct = float(compiled['long_pnl_pct'][i] if side == compiled_tape.SIDE_LONG else compiled['short_pnl_pct'][i])
        held = int(compiled['long_held'][i] if side == compiled_tape.SIDE_LONG else compiled['short_held'][i])
        reason_code = int(compiled['long_reason_code'][i] if side == compiled_tape.SIDE_LONG else compiled['short_reason_code'][i])
        alloc = round((balance * trade_size_pct / max(1, ticker_count)) * 100.0) / 100.0
        row_pnl = alloc * pct / 100.0
        pnl_total += row_pnl
        balance += row_pnl
        accepted += 1
        wins += 1 if row_pnl > 0 else 0
        losses += 1 if row_pnl < 0 else 0
        day_trade_count[d] += 1
        ticker_day_trade_count[tkr, d] += 1
        open_until[tkr] = t + held + same_ticker_reentry_cooldown_sec
        last_signal[tkr, side_idx, setup] = float(t)
        if reason_code == compiled_tape.REASON_CODE.get('stop_loss'):
            if stop_loss_cooldown_sec > 0:
                stop_cooldown_until[tkr] = t + held + stop_loss_cooldown_sec
            if loss_cluster_cooldown_sec > 0 and loss_cluster_window_sec > 0 and loss_cluster_count > 0:
                exit_t = t + held
                cur_count = int(loss_cluster_recent_count[tkr, side_idx, setup])
                kept = 0
                for j in range(cur_count):
                    old_t = int(loss_cluster_recent[tkr, side_idx, setup, j])
                    if old_t >= exit_t - loss_cluster_window_sec:
                        loss_cluster_recent[tkr, side_idx, setup, kept] = old_t
                        kept += 1
                max_slots = loss_cluster_recent.shape[3]
                if kept < max_slots:
                    loss_cluster_recent[tkr, side_idx, setup, kept] = exit_t
                    kept += 1
                else:
                    for j in range(max_slots - 1):
                        loss_cluster_recent[tkr, side_idx, setup, j] = loss_cluster_recent[tkr, side_idx, setup, j + 1]
                    loss_cluster_recent[tkr, side_idx, setup, max_slots - 1] = exit_t
                    kept = max_slots
                loss_cluster_recent_count[tkr, side_idx, setup] = kept
                if kept >= loss_cluster_count:
                    loss_cluster_until[tkr, side_idx, setup] = exit_t + loss_cluster_cooldown_sec
                    loss_cluster_recent_count[tkr, side_idx, setup] = 0
        src = source_rows[i] if i < len(source_rows) else {}
        ticker = ticker_names.get(tkr, str(tkr))
        setup_name = setup_names.get(setup, str(setup))
        day = day_names.get(d, str(d))
        rows.append({
            'schema_version': SCHEMA_VERSION,
            'source': 'step2_compiled_trace',
            'decision_status': 'accepted',
            'reject_reason': None,
            'row_index': int(i),
            'day': day,
            'ts': t,
            'ts_ct': src.get('ts_ct'),
            'ticker': ticker,
            'setup_type': setup_name,
            'side': _side_name(side),
            'original_side': _side_name(int(compiled['original_side'][i])),
            'opportunity_id': src.get('opportunity_id'),
            'price': src.get('price'),
            'conviction': src.get('conviction'),
            'source_decision': src.get('source_decision'),
            'model_id': context['model_id'],
            'run_context': context,
            'route_name': str(route_attrs['route_name'][i]) if route_attrs else 'fallback',
            'route_action': str(route_attrs['route_action'][i]) if route_attrs else 'score',
            'route_matched': bool(route_attrs['route_matched'][i]) if route_attrs else False,
            'outcome': {
                'pnl': round(float(row_pnl), 4),
                'pnl_pct': round(float(pct), 6),
                'held_sec': held,
                'exit_ts': t + held,
                'exit_reason': _reason_name(reason_code),
                'ending_balance': round(float(balance), 4),
            },
        })

    summary = {
        'schema_version': SCHEMA_VERSION,
        'source': 'step2_compiled_trace',
        'created_at_ct': _now_ct(),
        'run_context': context,
        'rows': len(rows),
        'accepted': accepted,
        'rejected': rejected,
        'wins': wins,
        'losses': losses,
        'pnl': round(float(pnl_total), 2),
        'ending_balance': round(float(balance), 2),
        'reject_reasons': dict(sorted(reject_reasons.items())),
    }
    if route_attrs is not None:
        summary['route_audit'] = routed_scoring_profile.route_audit(compiled, variant, sides=sides, trace_rows=rows)
    return {'rows': rows, 'summary': summary}


def write_step2_trace(day: str, trace: dict[str, Any], out_dir: str = DEFAULT_OUT_DIR,
                      label: str = 'step2_compiled_trace') -> dict[str, Any]:
    safe_label = ''.join(ch if ch.isalnum() or ch in '._-' else '_' for ch in label).strip('._') or 'step2'
    root = os.path.join(out_dir, str(day))
    path = os.path.join(root, f'{safe_label}_{day}.jsonl')
    summary_path = os.path.join(root, f'{safe_label}_{day}.summary.json')
    rows = list(trace.get('rows') or [])
    summary = dict(trace.get('summary') or {})
    summary.update({
        'path': _append_jsonl(path, rows, replace=True),
        'summary_path': os.path.abspath(summary_path),
    })
    _write_json(summary_path, summary)
    return summary


def write_live_signal_parity(day: str, rows: list[dict[str, Any]],
                             out_dir: str = DEFAULT_OUT_DIR,
                             label: str = 'live_signal_parity') -> dict[str, Any]:
    normalized = []
    for row in rows:
        outcome = row.get('outcome_summary') or {}
        normalized.append({
            'schema_version': SCHEMA_VERSION,
            'source': 'live_signal_parity',
            'decision_status': 'accepted' if row.get('decision') == 'entered' else 'rejected',
            'reject_reason': row.get('reason') if row.get('decision') != 'entered' else None,
            'day': day,
            'ts': int(row.get('created_at') or 0),
            'ts_ct': row.get('created_at_ct'),
            'ticker': row.get('ticker'),
            'setup_type': row.get('setup_type'),
            'side': row.get('side'),
            'opportunity_id': row.get('opportunity_id'),
            'price': row.get('price'),
            'conviction': row.get('conviction'),
            'score': row.get('score'),
            'parity_key': row.get('parity_key'),
            'trade_id': row.get('trade_id'),
            'model_id': row.get('active_profile_hash') or row.get('active_profile_name'),
            'outcome': {
                'pnl': outcome.get('pnl'),
                'held_sec': outcome.get('held_sec'),
                'exit_reason': outcome.get('reason'),
                'entry': outcome.get('entry'),
                'exit': outcome.get('exit'),
                'exit_ct': outcome.get('exit_ct'),
            } if outcome else None,
        })
    root = os.path.join(out_dir, str(day))
    safe_label = ''.join(ch if ch.isalnum() or ch in '._-' else '_' for ch in label).strip('._') or 'live'
    path = os.path.join(root, f'{safe_label}_{day}.jsonl')
    summary_path = os.path.join(root, f'{safe_label}_{day}.summary.json')
    accepted = [row for row in normalized if row.get('decision_status') == 'accepted']
    summary = {
        'schema_version': SCHEMA_VERSION,
        'source': 'live_signal_parity',
        'created_at_ct': _now_ct(),
        'day': day,
        'rows': len(normalized),
        'accepted': len(accepted),
        'rejected': len(normalized) - len(accepted),
        'pnl': round(sum(float((row.get('outcome') or {}).get('pnl') or 0.0) for row in accepted), 4),
        'decisions': dict(Counter(str(row.get('decision_status') or 'unknown') for row in normalized)),
        'reject_reasons': dict(Counter(str(row.get('reject_reason') or 'none') for row in normalized if row.get('decision_status') != 'accepted')),
        'path': _append_jsonl(path, normalized, replace=True),
        'summary_path': os.path.abspath(summary_path),
    }
    _write_json(summary_path, summary)
    return summary
