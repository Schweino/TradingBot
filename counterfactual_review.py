from __future__ import annotations

import argparse
import json
import os
from collections import Counter

from ws_scalp import detect_signal


HERE = os.path.dirname(os.path.abspath(__file__))
POSTMORTEM_DIR = os.path.join(HERE, 'postmortem')

PROFIT_TRIGGER_PCT = 0.45
PROFIT_GIVEBACK_PCT = 0.55
SETUP_LOSS_PAUSE_COUNT = 2


def _qty_estimate(trade: dict) -> float:
    entry = float(trade.get('entry') or 0)
    exit_px = float(trade.get('exit') or 0)
    pnl = float(trade.get('pnl') or 0)
    side = trade.get('side')
    per_share = (exit_px - entry) if side == 'LONG' else (entry - exit_px)
    return abs(pnl / per_share) if per_share else 0.0


def _profit_protect_estimate(trade: dict):
    mfe = trade.get('mfe_pct')
    entry = float(trade.get('entry') or 0)
    exit_px = float(trade.get('exit') or 0)
    side = trade.get('side')
    if mfe is None or entry <= 0 or side not in ('LONG', 'SHORT'):
        return None
    exit_ret = ((exit_px - entry) / entry * 100) if side == 'LONG' else ((entry - exit_px) / entry * 100)
    if mfe < PROFIT_TRIGGER_PCT:
        return None
    giveback = 1.0 - (exit_ret / max(float(mfe), 0.0001))
    if giveback < PROFIT_GIVEBACK_PCT:
        return None
    kept_pct = float(mfe) * (1.0 - PROFIT_GIVEBACK_PCT)
    est_pnl = (kept_pct / 100.0) * entry * _qty_estimate(trade)
    return {
        'old_pnl': round(float(trade.get('pnl') or 0), 2),
        'estimated_pnl': round(est_pnl, 2),
        'estimated_delta': round(est_pnl - float(trade.get('pnl') or 0), 2),
        'mfe_pct': mfe,
        'exit_return_pct': round(exit_ret, 3),
    }


def _exit_policy_estimate(trade: dict, trigger_pct: float, giveback_pct: float):
    mfe = trade.get('mfe_pct')
    entry = float(trade.get('entry') or 0)
    if mfe is None or entry <= 0:
        return None
    if float(mfe) < trigger_pct:
        return None
    kept_pct = float(mfe) * (1.0 - giveback_pct)
    est_pnl = (kept_pct / 100.0) * entry * _qty_estimate(trade)
    return round(est_pnl, 2)


def _variant_flags(sig: dict) -> dict:
    if not sig:
        return {}
    if sig.get('shadow_variants'):
        return sig.get('shadow_variants') or {}
    setup = sig.get('setup_type')
    score = abs(sig.get('score') or 0)
    quality = sig.get('signal_quality') or {}
    lead_lag = sig.get('lead_lag') or {}
    execution = sig.get('execution_quality') or {}
    return {
        'live_engine': True,
        'momentum_only': setup in ('momentum_breakout', 'btc_relative_strength') and score >= 5,
        'strict_flow_fade': setup == 'flow_exhaustion_fade'
                            and quality.get('flow_fade_confirmed')
                            and score >= 6,
        'btc_lead_lag_only': (lead_lag.get('score') or 0) > 0,
        'high_quality_execution_only': execution.get('score', 100) >= 75,
    }


def analyze(day: str) -> dict:
    path = os.path.join(POSTMORTEM_DIR, f'postmortem_{day}.json')
    with open(path, 'r', encoding='utf-8') as f:
        payload = json.load(f)
    trades = payload.get('tape', [])
    base_pnl = round(sum(float(t.get('pnl') or 0) for t in trades), 2)

    rows = []
    for trade in trades:
        sig = detect_signal(
            trade.get('ticker'),
            dict(trade.get('ind') or {}),
            dict(trade.get('btc') or {}),
        )
        fired = bool(sig and sig.get('side') == trade.get('side'))
        rows.append({'trade': trade, 'signal': sig, 'fired': fired})

    ws_skipped = [r for r in rows if not r['fired']]
    ws_kept = [r for r in rows if r['fired']]

    setup_losses = Counter()
    setup_paused = []
    final_kept = []
    for row in ws_kept:
        trade = row['trade']
        sig = row['signal'] or {}
        setup = sig.get('setup_type') or 'unknown'
        key = (trade.get('ticker'), trade.get('side'), setup)
        if setup_losses[key] >= SETUP_LOSS_PAUSE_COUNT:
            setup_paused.append({**row, 'setup_key': key})
            continue
        final_kept.append({**row, 'setup_key': key})
        if float(trade.get('pnl') or 0) < 0:
            setup_losses[key] += 1

    profit_protect = []
    for row in final_kept:
        est = _profit_protect_estimate(row['trade'])
        if est:
            profit_protect.append({**row, 'profit_protect': est})

    variant_rows = {}
    for row in ws_kept:
        flags = _variant_flags(row.get('signal') or {})
        for name, active in flags.items():
            if active:
                variant_rows.setdefault(name, []).append(row['trade'])
    variant_scoreboard = []
    for name, items in sorted(variant_rows.items()):
        pnl = round(sum(float(t.get('pnl') or 0) for t in items), 2)
        wins = sum(1 for t in items if float(t.get('pnl') or 0) > 0)
        losses = sum(1 for t in items if float(t.get('pnl') or 0) < 0)
        variant_scoreboard.append({
            'variant': name,
            'trades': len(items),
            'wins': wins,
            'losses': losses,
            'win_rate': round(wins / len(items) * 100, 1) if items else None,
            'pnl': pnl,
            'avg_pnl': round(pnl / len(items), 2) if items else None,
        })

    exit_policies = []
    for name, trigger, giveback in (
        ('profit_protect_live', 0.45, 0.55),
        ('profit_protect_aggressive', 0.30, 0.45),
        ('profit_protect_conservative', 0.70, 0.60),
        ('trail_after_half_pct', 0.50, 0.35),
    ):
        est_total = 0.0
        triggered = 0
        for row in final_kept:
            trade = row['trade']
            est = _exit_policy_estimate(trade, trigger, giveback)
            if est is not None:
                est_total += est
                triggered += 1
            else:
                est_total += float(trade.get('pnl') or 0)
        exit_policies.append({
            'policy': name,
            'triggered': triggered,
            'estimated_pnl': round(est_total, 2),
            'estimated_delta_vs_actual': round(est_total - base_pnl, 2),
        })

    static_pnl = round(sum(float(r['trade'].get('pnl') or 0) for r in final_kept), 2)
    pp_delta = round(sum(r['profit_protect']['estimated_delta'] for r in profit_protect), 2)

    return {
        'day': day,
        'actual': {
            'trades': len(trades),
            'wins': sum(1 for t in trades if float(t.get('pnl') or 0) > 0),
            'losses': sum(1 for t in trades if float(t.get('pnl') or 0) < 0),
            'pnl': base_pnl,
        },
        'current_entry_engine': {
            'kept': len(ws_kept),
            'skipped': len(ws_skipped),
            'kept_pnl': round(sum(float(r['trade'].get('pnl') or 0) for r in ws_kept), 2),
            'skipped_pnl': round(sum(float(r['trade'].get('pnl') or 0) for r in ws_skipped), 2),
            'skipped_trades': [
                {
                    'time': r['trade'].get('time'),
                    'ticker': r['trade'].get('ticker'),
                    'side': r['trade'].get('side'),
                    'pnl': r['trade'].get('pnl'),
                    'score': r['trade'].get('score'),
                }
                for r in ws_skipped
            ],
        },
        'setup_pause': {
            'skipped': len(setup_paused),
            'skipped_pnl': round(sum(float(r['trade'].get('pnl') or 0) for r in setup_paused), 2),
            'skipped_trades': [
                {
                    'time': r['trade'].get('time'),
                    'ticker': r['trade'].get('ticker'),
                    'side': r['trade'].get('side'),
                    'pnl': r['trade'].get('pnl'),
                    'setup_key': ':'.join(str(x) for x in r['setup_key']),
                }
                for r in setup_paused
            ],
        },
        'profit_protection_estimate': {
            'triggered': len(profit_protect),
            'estimated_delta': pp_delta,
            'trades': [
                {
                    'time': r['trade'].get('time'),
                    'ticker': r['trade'].get('ticker'),
                    'side': r['trade'].get('side'),
                    **r['profit_protect'],
                }
                for r in profit_protect
            ],
        },
        'variant_scoreboard': sorted(variant_scoreboard, key=lambda r: r['pnl'], reverse=True),
        'exit_policy_scoreboard': sorted(exit_policies, key=lambda r: r['estimated_pnl'], reverse=True),
        'combined_estimate': {
            'kept_after_entry_and_pause': len(final_kept),
            'pnl_before_profit_protection': static_pnl,
            'estimated_pnl_after_profit_protection': round(static_pnl + pp_delta, 2),
            'estimated_delta_vs_actual': round(static_pnl + pp_delta - base_pnl, 2),
        },
        'caveats': [
            'Uses postmortem indicator snapshots, not full second-by-second replay.',
            'Profit-protection impact is estimated from MFE and final exit, so exact fill timing can differ.',
            'Yesterday has no shadow-decision corpus because that logger was added after the session.',
            'Market-regime filter cannot be fully replayed unless SPY/QQQ/IWM tape was captured for the entry.',
        ],
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('day')
    ap.add_argument('--json', action='store_true')
    args = ap.parse_args()
    result = analyze(args.day)
    if args.json:
        print(json.dumps(result, indent=2))
        return
    print(f"Counterfactual review {result['day']}")
    print(f"Actual: {result['actual']['trades']} trades, "
          f"{result['actual']['wins']}W/{result['actual']['losses']}L, "
          f"PnL=${result['actual']['pnl']:+.2f}")
    ce = result['current_entry_engine']
    print(f"Current ws_scalp: kept {ce['kept']}, skipped {ce['skipped']}, "
          f"kept PnL=${ce['kept_pnl']:+.2f}, skipped PnL=${ce['skipped_pnl']:+.2f}")
    sp = result['setup_pause']
    print(f"Setup pause: skipped {sp['skipped']} additional trade(s), "
          f"skipped PnL=${sp['skipped_pnl']:+.2f}")
    pp = result['profit_protection_estimate']
    print(f"Profit protection estimate: {pp['triggered']} trigger(s), "
          f"delta=${pp['estimated_delta']:+.2f}")
    combo = result['combined_estimate']
    print(f"Combined estimate: PnL=${combo['estimated_pnl_after_profit_protection']:+.2f}, "
          f"delta vs actual=${combo['estimated_delta_vs_actual']:+.2f}")
    if result.get('variant_scoreboard'):
        print("\nVariant scoreboard:")
        for row in result['variant_scoreboard']:
            print(f"  {row['variant']}: {row['trades']} trades "
                  f"{row['wins']}W/{row['losses']}L pnl=${row['pnl']:+.2f}")
    if result.get('exit_policy_scoreboard'):
        print("\nExit policy scoreboard:")
        for row in result['exit_policy_scoreboard']:
            print(f"  {row['policy']}: triggered={row['triggered']} "
                  f"est_pnl=${row['estimated_pnl']:+.2f} "
                  f"delta=${row['estimated_delta_vs_actual']:+.2f}")
    if ce['skipped_trades']:
        print("\nCurrent ws_scalp would skip:")
        for row in ce['skipped_trades']:
            print(f"  {row['time']} {row['ticker']} {row['side']} pnl=${row['pnl']:+.2f}")
    if sp['skipped_trades']:
        print("\nSetup pause would skip:")
        for row in sp['skipped_trades']:
            print(f"  {row['time']} {row['ticker']} {row['side']} pnl=${row['pnl']:+.2f} {row['setup_key']}")
    if pp['trades']:
        print("\nProfit protection candidates:")
        for row in pp['trades']:
            print(f"  {row['time']} {row['ticker']} {row['side']} "
                  f"old=${row['old_pnl']:+.2f} est=${row['estimated_pnl']:+.2f} "
                  f"delta=${row['estimated_delta']:+.2f} mfe={row['mfe_pct']}%")


if __name__ == '__main__':
    main()
