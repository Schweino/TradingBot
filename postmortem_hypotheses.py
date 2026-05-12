"""
Living watchlist + per-day deductions for daily_postmortem.py.

Three things this module produces (called from render_report):

  1. DEDUCTIONS — observations about *today's* losing trades. Stateless;
     re-derived each run from the tape. Plain English, templated from
     pattern detectors.

  2. ACTIVE HYPOTHESES — a persisted watchlist. Each entry is something
     to monitor going into the next trading day(s). Written to
     postmortem/hypotheses.json. Lifecycle:
        - New: detector fires today, no matching active hypothesis
        - Active: matched ≥1 day, last_seen within 5 trading days
        - Stale: not matched for >5 trading days  (still shown, marked)
        - Dropped: not matched for >10 trading days (removed from file)

  3. RECENTLY RETIRED — last 5 dropped hypotheses kept for transparency
     so it's easy to see what we stopped watching and why.

Detectors are intentionally narrow + deterministic. When in doubt, prefer
not firing — daily samples are tiny and false-positive hypotheses pollute
the watchlist faster than they help.
"""
from __future__ import annotations

from output_paths import output_path
import os, json
from datetime import datetime, timedelta
from typing import Optional

HERE = os.path.dirname(os.path.abspath(__file__))
HYP_PATH = output_path('postmortem', 'hypotheses.json')

STALE_AFTER_DAYS = 5
DROP_AFTER_DAYS  = 10


# ── Persistence ─────────────────────────────────────────────────────────
def _load() -> dict:
    if not os.path.exists(HYP_PATH):
        return {'next_id': 1, 'active': [], 'retired': []}
    with open(HYP_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)


def _save(data: dict) -> None:
    os.makedirs(os.path.dirname(HYP_PATH), exist_ok=True)
    with open(HYP_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, default=str)


def _days_between(a_iso: str, b_iso: str) -> int:
    """Calendar days (not trading days — close enough for staleness)."""
    a = datetime.strptime(a_iso, '%Y-%m-%d').date()
    b = datetime.strptime(b_iso, '%Y-%m-%d').date()
    if a == b:
        return 0
    if b < a:
        a, b = b, a
    cur = a + timedelta(days=1)
    days = 0
    while cur <= b:
        if cur.weekday() < 5:
            days += 1
        cur += timedelta(days=1)
    return days


# ── Pattern detectors ───────────────────────────────────────────────────
# Each returns (key, deduction_text, monitor_text) or None.
# `key` is the stable identifier — same key = same hypothesis across days.

def _det_side_skew(losers, all_trades):
    """≥80% of today's losers are on one side."""
    if len(losers) < 4: return None
    long_n  = sum(1 for l in losers if l['side'] == 'LONG')
    short_n = sum(1 for l in losers if l['side'] == 'SHORT')
    side, n = ('SHORT', short_n) if short_n >= long_n else ('LONG', long_n)
    if n / len(losers) < 0.80: return None
    return (
        'side_skew_losers',
        f"{n}/{len(losers)} losers were {side}. "
        f"{side}-side concentration well above chance.",
        f"Watch whether {side}-side trades continue underperforming. "
        f"If pattern holds 3+ days, consider biasing entries away from {side} "
        f"in this regime.",
    )


def _det_single_ticker(losers):
    """All today's losers on a single ticker."""
    if len(losers) < 3: return None
    tkrs = {l['ticker'] for l in losers}
    if len(tkrs) != 1: return None
    tkr = next(iter(tkrs))
    return (
        f'single_ticker_loser_{tkr}',
        f"All {len(losers)} losers were {tkr}.",
        f"Watch whether {tkr} continues losing. If pattern holds 3+ days, "
        f"may be ticker-specific regime issue (volatility shift, news, etc.).",
    )


def _det_high_vol_entries_failed(losers, all_trades):
    """Losers had higher entry vol_z than winners on average."""
    losers_vz  = [l['ind'].get('vol_z_60s') for l in losers
                  if isinstance(l.get('ind'), dict) and l['ind'].get('vol_z_60s') is not None]
    winners    = [t for t in all_trades if t['pnl'] > 0]
    winners_vz = [w['ind'].get('vol_z_60s') for w in winners
                  if isinstance(w.get('ind'), dict) and w['ind'].get('vol_z_60s') is not None]
    if len(losers_vz) < 3 or len(winners_vz) < 3: return None
    avg_l = sum(losers_vz) / len(losers_vz)
    avg_w = sum(winners_vz) / len(winners_vz)
    if avg_l - avg_w < 1.5: return None  # need a meaningful gap
    return (
        'losers_high_vol',
        f"Losers' average entry vol_z_60s = {avg_l:.2f} vs winners' {avg_w:.2f}. "
        f"High-volatility entries failed today.",
        f"Watch whether vol_z_60s>2 entries continue underperforming. May warrant "
        f"a vol_z ceiling on entries if pattern repeats.",
    )


def _det_short_positive_mom60(losers):
    """Losing SHORTs entered while 60s momentum was still positive."""
    eligible = [l for l in losers
                if l.get('side') == 'SHORT'
                and isinstance(l.get('ind'), dict)
                and l['ind'].get('mom_60s') is not None]
    if not eligible: return None
    flagged = [l for l in eligible if l['ind']['mom_60s'] > 0]
    if not flagged: return None
    detail = ', '.join(
        f"{l['ticker']} ({l['ind']['mom_60s']:+.3f}%)" for l in flagged
    )
    return (
        'short_positive_mom60',
        f"{len(flagged)}/{len(eligible)} losing SHORTs had positive mom_60s at entry "
        f"({detail}) — shorting a short-term dip within an upward 60s trend.",
        f"Watch whether SHORTs with positive mom_60s at entry continue losing. "
        f"If pattern holds 3+ days, consider blocking SHORTs when mom_60s > 0.",
    )


def _det_short_weak_ema_spread(losers):
    """Losing SHORTs where ema_15s and ema_60s were nearly flat (weak bear stack)."""
    EMA_SPREAD_PCT = 0.0002  # 0.02% of price
    eligible = [l for l in losers
                if l.get('side') == 'SHORT'
                and isinstance(l.get('ind'), dict)
                and l['ind'].get('ema_15s') is not None
                and l['ind'].get('ema_60s') is not None
                and l['ind'].get('price')]
    if not eligible: return None
    flagged = [l for l in eligible
               if abs(l['ind']['ema_15s'] - l['ind']['ema_60s']) / l['ind']['price']
               < EMA_SPREAD_PCT]
    if not flagged: return None
    detail = ', '.join(
        f"{l['ticker']} (spread={abs(l['ind']['ema_15s']-l['ind']['ema_60s']):.4f})"
        for l in flagged
    )
    return (
        'short_weak_ema_spread',
        f"{len(flagged)}/{len(eligible)} losing SHORTs had near-zero ema_15s/60s spread "
        f"({detail}) — bear stack valid but 15s and 60s EMAs nearly converged.",
        f"Watch whether weak-spread bear stacks continue losing on SHORTs. "
        f"If pattern holds 3+ days, consider requiring a minimum ema_15s/60s "
        f"spread for SHORT entries.",
    )


def _det_short_sustained_buying(losers):
    """Losing SHORTs where 120s flow showed sustained buying (≥65% buy_pct)."""
    eligible = [l for l in losers
                if l.get('side') == 'SHORT'
                and isinstance(l.get('ind'), dict)
                and isinstance(l['ind'].get('flow_120s'), dict)
                and l['ind']['flow_120s'].get('buy_pct') is not None]
    if not eligible: return None
    flagged = [l for l in eligible if l['ind']['flow_120s']['buy_pct'] >= 65]
    if not flagged: return None
    detail = ', '.join(
        f"{l['ticker']} ({l['ind']['flow_120s']['buy_pct']:.1f}%)" for l in flagged
    )
    return (
        'short_sustained_buying',
        f"{len(flagged)}/{len(eligible)} losing SHORTs had sustained buying in the "
        f"120s flow window ({detail}) — buyers dominant for 2+ minutes before entry.",
        f"Watch whether SHORTs with flow_120s buy_pct ≥ 65% continue losing. "
        f"If pattern holds 3+ days, the fade-SHORT setup may be unreliable when "
        f"buying pressure is sustained rather than a momentary spike.",
    )


def _det_short_conviction_decay_exits(losers):
    """New short conviction exit should be monitored for false positives."""
    flagged = [
        l for l in losers
        if str(l.get('reason', '')).startswith('short_conviction_decay_')
    ]
    if not flagged:
        return None
    total = sum(abs(l.get('pnl', 0) or 0) for l in flagged)
    return (
        'short_conviction_decay_rollout',
        f"{len(flagged)} losing SHORT(s) exited by short_conviction_decay, "
        f"gross loss ${total:.2f}.",
        "Watch whether short_conviction_decay reduces SHORT average loss without "
        "cutting high-quality SHORT winners. If it fires often and fwd5/fwd15 would "
        "have recovered, loosen it; if it prevents cond_time_stop-sized losses, keep it.",
    )


def _det_long_conviction_decay_exits(losers):
    """New long conviction exit should be monitored more gently."""
    flagged = [
        l for l in losers
        if str(l.get('reason', '')).startswith('long_conviction_decay_')
    ]
    if not flagged:
        return None
    total = sum(abs(l.get('pnl', 0) or 0) for l in flagged)
    return (
        'long_conviction_decay_rollout',
        f"{len(flagged)} losing LONG(s) exited by long_conviction_decay, "
        f"gross loss ${total:.2f}.",
        "Watch whether long_conviction_decay trims weak LONG follow-through without "
        "damaging the profitable LONG side. It should be conservative; loosen it if "
        "it exits trades that later show positive fwd5/fwd15.",
    )


def _det_short_flow_btc_not_bearish(losers):
    """Narrower version of sustained buying: only when BTC is not bearish."""
    eligible = [
        l for l in losers
        if l.get('side') == 'SHORT'
        and isinstance(l.get('ind'), dict)
        and isinstance(l['ind'].get('flow_120s'), dict)
        and l['ind']['flow_120s'].get('buy_pct') is not None
    ]
    if not eligible:
        return None
    flagged = []
    for l in eligible:
        f = l.get('forensics') or {}
        btc = f.get('btc') or {}
        if l['ind']['flow_120s']['buy_pct'] >= 60 and btc.get('regime') != 'bear':
            flagged.append(l)
    if not flagged:
        return None
    detail = ', '.join(
        f"{l['ticker']} ({l['ind']['flow_120s']['buy_pct']:.1f}%, "
        f"BTC={(l.get('forensics') or {}).get('btc', {}).get('regime')})"
        for l in flagged
    )
    return (
        'short_flow120_btc_not_bearish',
        f"{len(flagged)}/{len(eligible)} losing SHORTs had flow_120s buy_pct >= 60 "
        f"while BTC was not bearish ({detail}).",
        "Watch SHORTs with sustained buying plus non-bearish BTC. Do not block all "
        "high-buy-flow shorts; today-only replay showed broad flow blocks would also "
        "cut winners. If this narrow combo repeats, require extra confirmation or "
        "reduce SHORT conviction.",
    )


DETECTORS = [
    _det_side_skew,
    _det_single_ticker,
    _det_high_vol_entries_failed,
    _det_short_positive_mom60,
    _det_short_weak_ema_spread,
    _det_short_sustained_buying,
    _det_short_flow_btc_not_bearish,
    _det_short_conviction_decay_exits,
    _det_long_conviction_decay_exits,
]


def _run_detectors(tape):
    losers = [t for t in tape if t['pnl'] < 0]
    out = []
    if not losers: return out
    for det in DETECTORS:
        # Some detectors take (losers, all_trades), some take (losers,)
        try:
            n_args = det.__code__.co_argcount
            res = det(losers, tape) if n_args == 2 else det(losers)
        except Exception as e:
            res = None
        if res:
            out.append(res)
    return out


# ── Hypothesis lifecycle ────────────────────────────────────────────────
def update_and_render(day_iso: str, tape: list, mutate: bool = True) -> list[str]:
    """Run detectors and render text lines.

    mutate=True is for the official daily run. mutate=False is for historical
    reruns/reviews that should not change hypotheses.json.
    """
    state = _load()
    today_keys = set()
    today_findings = _run_detectors(tape)   # [(key, deduction, monitor), ...]

    if not mutate:
        return _render(state, today_findings, day_iso)

    # Index existing hypotheses by key
    by_key = {h['key']: h for h in state['active']}

    for key, ded, mon in today_findings:
        today_keys.add(key)
        if key in by_key:
            h = by_key[key]
            seen = set(h.get('seen_dates') or [])
            if not seen:
                # Backfill legacy records so rerunning the most recent report
                # cannot inflate evidence_days.
                if h.get('created'):
                    seen.add(h['created'])
                if h.get('last_seen'):
                    seen.add(h['last_seen'])
            seen.add(day_iso)
            h['seen_dates'] = sorted(seen)
            h['last_seen'] = day_iso
            h['evidence_days'] = len(seen)
            # Refresh wording in case a detector added richer numbers today
            h['deduction'] = ded
            h['monitor']   = mon
        else:
            new = {
                'id':            f"H{state['next_id']:03d}",
                'key':           key,
                'created':       day_iso,
                'last_seen':     day_iso,
                'seen_dates':    [day_iso],
                'evidence_days': 1,
                'deduction':     ded,
                'monitor':       mon,
            }
            state['active'].append(new)
            by_key[key] = new
            state['next_id'] = state['next_id'] + 1

    # Age out hypotheses not seen today: stale after 5 days, drop after 10.
    still_active = []
    for h in state['active']:
        days_since = _days_between(h['last_seen'], day_iso)
        if days_since > DROP_AFTER_DAYS:
            h['retired_on'] = day_iso
            h['retired_reason'] = f'no evidence for {days_since}d (drop threshold {DROP_AFTER_DAYS}d)'
            state['retired'].append(h)
            continue
        h['days_since_evidence'] = days_since
        h['stale'] = days_since > STALE_AFTER_DAYS
        still_active.append(h)
    state['active'] = still_active

    # Keep only last 5 retired entries
    state['retired'] = state['retired'][-5:]

    _save(state)
    return _render(state, today_findings, day_iso)


def _render(state, today_findings, day_iso) -> list[str]:
    out = []
    out.append('Watchlist & deductions')
    out.append('-' * 78)

    # Today's deductions
    if today_findings:
        out.append("  Deductions on today's losers:")
        for _, ded, _ in today_findings:
            out.append(f"    • {ded}")
    else:
        # Either no losers or no detectors fired — say which
        out.append("  No new deductions from today's losers (no patterns detected, "
                   "or no losers).")
    out.append('')

    # Active hypotheses (the watchlist)
    active = state.get('active', [])
    if active:
        out.append(f"  Watchlist — monitoring next trading day(s) ({len(active)} active):")
        for h in sorted(active, key=lambda x: (x.get('stale', False), -x['evidence_days'])):
            stale_marker = ' [STALE]' if h.get('stale') else ''
            out.append(f"    [{h['id']}{stale_marker}] {h['monitor']}")
            out.append(f"           "
                       f"seen on {h['evidence_days']} day(s), "
                       f"first {h['created']}, last {h['last_seen']}")
    else:
        out.append("  Watchlist is empty.")
    out.append('')

    # Recently retired
    retired = state.get('retired', [])
    if retired:
        out.append(f"  Recently retired ({len(retired)} most recent):")
        for h in retired[-5:]:
            out.append(f"    [{h['id']}] retired {h.get('retired_on','?')}: "
                       f"{h.get('retired_reason','?')}")
            out.append(f"           was monitoring: {h.get('monitor','?')[:90]}")
    return out


# ── CLI for inspection ──────────────────────────────────────────────────
if __name__ == '__main__':
    state = _load()
    print(f"hypotheses file: {HYP_PATH}")
    print(f"next_id  : {state.get('next_id', 1)}")
    print(f"active   : {len(state.get('active', []))}")
    print(f"retired  : {len(state.get('retired', []))}")
    for h in state.get('active', []):
        stale = ' [STALE]' if h.get('stale') else ''
        print(f"  [{h['id']}{stale}] (seen {h['evidence_days']}d) {h['monitor'][:80]}")
