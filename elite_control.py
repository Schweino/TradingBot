from __future__ import annotations

import copy
import json
import os
from datetime import datetime, timezone

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore

from engine_scoreboard import build_scoreboard


HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(HERE, 'trading_config.json')
POSTMORTEM_DIR = os.path.join(HERE, 'postmortem')
CT = ZoneInfo('America/Chicago')
WATCHED = ('CLSK', 'MARA', 'RIOT')


def _load_config() -> dict:
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)


def _postmortem_days(max_days=7):
    days = []
    if not os.path.isdir(POSTMORTEM_DIR):
        return days
    for name in sorted(os.listdir(POSTMORTEM_DIR), reverse=True):
        if name.startswith('postmortem_') and name.endswith('.json'):
            day = name[len('postmortem_'):-len('.json')]
            if len(day) == 10:
                days.append(day)
        if len(days) >= max_days:
            break
    return days


def rolling_setup_capital_table(max_days=7) -> dict:
    rows = {}
    for day in reversed(_postmortem_days(max_days=max_days)):
        path = os.path.join(POSTMORTEM_DIR, f'postmortem_{day}.json')
        try:
            payload = json.load(open(path, encoding='utf-8'))
        except Exception:
            continue
        for trade in payload.get('tape', []) or []:
            fx = trade.get('forensics') or {}
            setup = fx.get('setup_type') or trade.get('setup_type') or 'unknown'
            key = f"{trade.get('ticker')}:{trade.get('side')}:{setup}"
            row = rows.setdefault(key, {
                'key': key,
                'days': set(),
                'trades': 0,
                'wins': 0,
                'losses': 0,
                'pnl': 0.0,
                'gross_loss': 0.0,
                'avg_mfe_pct': [],
                'avg_mae_pct': [],
            })
            pnl = float(trade.get('pnl') or 0)
            row['days'].add(day)
            row['trades'] += 1
            row['wins'] += 1 if pnl > 0 else 0
            row['losses'] += 1 if pnl < 0 else 0
            row['pnl'] += pnl
            row['gross_loss'] += abs(pnl) if pnl < 0 else 0
            if trade.get('mfe_pct') is not None:
                row['avg_mfe_pct'].append(float(trade['mfe_pct']))
            if trade.get('mae_pct') is not None:
                row['avg_mae_pct'].append(float(trade['mae_pct']))
    out = []
    for row in rows.values():
        trades = row['trades']
        win_rate = row['wins'] / trades * 100 if trades else None
        pnl = round(row['pnl'], 2)
        if trades < 3:
            action = 'monitor'
            multiplier = 1.0
        elif pnl > 0 and win_rate is not None and win_rate >= 65:
            action = 'full_or_increase'
            multiplier = 1.0
        elif pnl < 0 or (win_rate is not None and win_rate < 45):
            action = 'reduce_or_pause'
            multiplier = 0.5
        else:
            action = 'reduced_monitor'
            multiplier = 0.75
        out.append({
            'key': row['key'],
            'days': sorted(row['days']),
            'evidence_days': len(row['days']),
            'trades': trades,
            'wins': row['wins'],
            'losses': row['losses'],
            'win_rate': round(win_rate, 1) if win_rate is not None else None,
            'pnl': pnl,
            'gross_loss': round(row['gross_loss'], 2),
            'avg_mfe_pct': round(sum(row['avg_mfe_pct']) / len(row['avg_mfe_pct']), 3) if row['avg_mfe_pct'] else None,
            'avg_mae_pct': round(sum(row['avg_mae_pct']) / len(row['avg_mae_pct']), 3) if row['avg_mae_pct'] else None,
            'recommendation': action,
            'suggested_size_multiplier': multiplier,
        })
    return {
        'generated_at': datetime.now(CT).isoformat(timespec='seconds'),
        'lookback_days': max_days,
        'rows': sorted(out, key=lambda r: (r['pnl'], r['win_rate'] or 0), reverse=True),
    }


def build_candidate_config(day: str, min_evidence_days: int = 2) -> dict:
    base = _load_config()
    candidate = copy.deepcopy(base)
    scoreboard = build_scoreboard(day)
    capital = rolling_setup_capital_table(max_days=7)
    changes = []

    # Stage exit-policy suggestions only as metadata; do not silently rewrite
    # live profit protection from a single session.
    exit_rows = (((scoreboard.get('counterfactual') or {}).get('exit_policy_scoreboard')) or [])
    if exit_rows:
        best = exit_rows[0]
        if best.get('triggered', 0) > 0 and best.get('estimated_delta_vs_actual', 0) > 0:
            changes.append({
                'type': 'exit_policy_candidate',
                'candidate': best['policy'],
                'evidence': best,
                'action': 'stage_only_review_before_live',
            })

    setup_mult = candidate.setdefault('adaptive_management', {}).setdefault('setup_size_multiplier', {})
    for row in capital['rows']:
        if row['evidence_days'] < min_evidence_days or row['trades'] < 3:
            continue
        parts = row['key'].split(':')
        setup = parts[2] if len(parts) >= 3 else 'unknown'
        if row['recommendation'] in ('reduce_or_pause', 'reduced_monitor') and setup in setup_mult:
            old = setup_mult.get(setup)
            new = min(float(old), float(row['suggested_size_multiplier']))
            if new != old:
                setup_mult[setup] = new
                changes.append({
                    'type': 'setup_size_multiplier',
                    'setup': setup,
                    'old': old,
                    'new': new,
                    'evidence': row,
                    'action': 'candidate_config_only',
                })

    return {
        'date_iso': day,
        'generated_at': datetime.now(CT).isoformat(timespec='seconds'),
        'base_config': CONFIG_PATH,
        'candidate_config': candidate,
        'changes': changes,
        'scoreboard_summary': scoreboard.get('summary', []),
        'capital_table': capital,
        'caveat': 'Staged candidate only. Review before copying into trading_config.json.',
    }


def write_candidate_config(day: str) -> tuple[str, dict]:
    payload = build_candidate_config(day)
    path = os.path.join(POSTMORTEM_DIR, f'candidate_config_{day}.json')
    os.makedirs(POSTMORTEM_DIR, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, default=str)
    return path, payload


def live_quality(mt) -> dict:
    status = mt.status()
    engine = mt.engine
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    feeds = {}
    for sym in WATCHED + ('BTC/USD',):
        st = getattr(engine, 'states', {}).get(sym)
        if not st:
            feeds[sym] = {'streamed': False}
            continue
        with st.lock:
            trade_age = round((now_ms - st.last_trade_ts_ms) / 1000, 2) if st.last_trade_ts_ms else None
            quote_age = round((now_ms - st.last_quote_ts_ms) / 1000, 2) if st.last_quote_ts_ms else None
            price = st.last_trade_price
        feeds[sym] = {
            'streamed': True,
            'price': price,
            'trade_age_sec': trade_age,
            'quote_age_sec': quote_age,
            'bars': len(st.bars_1s),
        }

    score = 100
    reasons = []
    if status.get('broker_exposure_block'):
        score -= 50
        reasons.append('broker_exposure_block')
    if status.get('broker_api_degraded'):
        score -= 30
        reasons.append('broker_api_degraded')
    if status.get('kill_switch', {}).get('enabled'):
        score = 0
        reasons.append('kill_switch')
    if not status.get('running'):
        score -= 25
        reasons.append('mock_not_running')
    for sym, row in feeds.items():
        if not row.get('streamed'):
            score -= 5 if sym != 'BTC/USD' else 15
            reasons.append(f'{sym}_not_streamed')
        elif sym == 'BTC/USD':
            trade_age = row.get('trade_age_sec')
            quote_age = row.get('quote_age_sec')
            if trade_age is not None and trade_age > 30 and (quote_age is None or quote_age > 10):
                score -= 20
                reasons.append(f'{sym}_stale_trade')
        elif row.get('trade_age_sec') is not None and row['trade_age_sec'] > 15:
            score -= 10
            reasons.append(f'{sym}_stale_trade')
    near_path = os.path.join(POSTMORTEM_DIR, 'near_signals',
                             f'near_signals_{datetime.now(CT).date().isoformat()}.jsonl')
    near_count = 0
    if os.path.exists(near_path):
        try:
            near_count = sum(1 for _ in open(near_path, encoding='utf-8'))
        except Exception:
            near_count = 0
    if near_count > 250:
        score -= 5
        reasons.append('many_near_signal_rejects')

    if score >= 80:
        posture = 'trade_allowed'
    elif score >= 60:
        posture = 'trade_cautiously'
    else:
        posture = 'risk_off_no_new_entries_preferred'
    return {
        'generated_at': datetime.now(CT).isoformat(timespec='seconds'),
        'score': max(0, min(100, score)),
        'posture': posture,
        'reasons': reasons,
        'feeds': feeds,
        'near_signal_rejects_today': near_count,
        'setup_pauses': status.get('setup_pauses', {}),
        'positions': status.get('positions', {}),
        'pending_entries': status.get('pending_entries', {}),
        'account': {
            'alpaca_equity': status.get('alpaca_equity'),
            'alpaca_cash': status.get('alpaca_cash'),
            'balance': status.get('balance'),
            'start_balance': status.get('start_balance'),
        },
    }


def live_entry_gate(mt, min_score: float = 60) -> dict:
    quality = live_quality(mt)
    score = float(quality.get('score') or 0)
    posture = quality.get('posture')
    blocked = score < min_score or posture == 'risk_off_no_new_entries_preferred'
    return {
        'allowed': not blocked,
        'score': score,
        'min_score': min_score,
        'posture': posture,
        'reasons': quality.get('reasons', []),
    }


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('day')
    ap.add_argument('--write', action='store_true')
    args = ap.parse_args()
    if args.write:
        path, payload = write_candidate_config(args.day)
        print(path)
    else:
        payload = build_candidate_config(args.day)
    print(json.dumps(payload, indent=2, default=str))


if __name__ == '__main__':
    main()
