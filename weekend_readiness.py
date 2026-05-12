from __future__ import annotations

from output_paths import output_path

import hashlib
import json
import os
from collections import Counter, defaultdict
from datetime import datetime, time, timedelta
from typing import Any, Optional
from urllib.request import urlopen

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo


HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = output_path('postmortem')
SNAPSHOT_DIR = os.path.join(OUT_DIR, 'broker_safety_snapshots')
CT = ZoneInfo('America/Chicago')
CURRENT_ERA_START = '2026-05-04'
CURRENT_ERA_LABEL = 'vNext_2026_05_04'
BASELINE_ERA_LABEL = 'pre_2026_05_04'


def _compiled_step2_paths(day: str) -> dict[str, str]:
    name = f'compiled_step2_live_mockparity_CLSK-MARA-RIOT_{day}_intraday'
    root = os.path.join(OUT_DIR, 'backtests', 'compiled_decision_tapes', name)
    return {
        'compiled_manifest': os.path.join(root, 'manifest.json'),
        'compiled_chunk_manifest': os.path.join(root, 'chunks', 'chunk_manifest.json'),
        'step2_score': os.path.join(OUT_DIR, 'backtests', 'step2_today_compiled', f'step2_today_compiled_{day}.json'),
        'step2_rebuild_plan': os.path.join(OUT_DIR, 'rebuild_plans', f'step2_rebuild_plan_{day}.json'),
        'unified_step2_current_trace': os.path.join(
            OUT_DIR, 'unified_decision_ledger', day, f'step2_current_trace_{day}.jsonl',
        ),
        'unified_step2_current_trace_summary': os.path.join(
            OUT_DIR, 'unified_decision_ledger', day, f'step2_current_trace_{day}.summary.json',
        ),
        'unified_live_signal_parity': os.path.join(
            OUT_DIR, 'unified_decision_ledger', day, f'live_signal_parity_{day}.jsonl',
        ),
        'unified_live_signal_parity_summary': os.path.join(
            OUT_DIR, 'unified_decision_ledger', day, f'live_signal_parity_{day}.summary.json',
        ),
        'golden_parity_suite': os.path.join(OUT_DIR, 'golden_parity', 'golden_parity_all.json'),
        'run_supervisor_cleanup': os.path.join(
            OUT_DIR, 'runtime_supervisor', f'run_supervisor_cleanup_post-close_{day}.json',
        ),
    }


def _now_ct() -> datetime:
    return datetime.now(CT)


def era_for_day(day: Optional[str]) -> str:
    if not day:
        return 'unknown'
    return CURRENT_ERA_LABEL if str(day) >= CURRENT_ERA_START else BASELINE_ERA_LABEL


def _read_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return default


def _write_json(path: str, payload: dict) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, default=str)
    return path


def _sha256(path: str) -> Optional[str]:
    try:
        h = hashlib.sha256()
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(65536), b''):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None


def _file_info(path: str) -> dict:
    try:
        st = os.stat(path)
        return {
            'path': path,
            'exists': True,
            'bytes': st.st_size,
            'modified_at_ct': datetime.fromtimestamp(st.st_mtime, CT).isoformat(timespec='seconds'),
        }
    except Exception:
        return {'path': path, 'exists': False}


def _iter_jsonl(path: str) -> list[dict]:
    rows = []
    if not os.path.exists(path):
        return rows
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    return rows


def _safe_pct(num: float, den: float) -> Optional[float]:
    return round(num / den * 100, 1) if den else None


def _status() -> tuple[dict, Optional[str]]:
    try:
        with urlopen('http://127.0.0.1:5000/mock/status', timeout=5) as resp:
            return json.loads(resp.read().decode('utf-8')), None
    except Exception as e:
        return {}, str(e)


def _trading_config() -> dict:
    return _read_json(os.path.join(HERE, 'trading_config.json'), {}) or {}


def _postmortem(day: str) -> dict:
    return _read_json(os.path.join(OUT_DIR, f'postmortem_{day}.json'), {}) or {}


def _tape(day: str) -> list[dict]:
    return list(_postmortem(day).get('tape') or [])


def _audit_rows(day: str) -> list[dict]:
    return _iter_jsonl(output_path('audit', f'trade_lifecycle_{day}.jsonl'))


def _skipped_rows(day: str) -> list[dict]:
    return _iter_jsonl(os.path.join(OUT_DIR, 'skipped_signals', f'skipped_signals_{day}.jsonl'))


def _near_signal_rows(day: str) -> list[dict]:
    return _iter_jsonl(os.path.join(OUT_DIR, 'near_signals', f'near_signals_{day}.jsonl'))


def _heartbeat_rows(day: str) -> list[dict]:
    return _iter_jsonl(os.path.join(OUT_DIR, 'health_heartbeats', f'health_heartbeats_{day}.jsonl'))


def _runtime_event_rows(day: str) -> list[dict]:
    return _iter_jsonl(os.path.join(OUT_DIR, 'runtime', f'runtime_events_{day}.jsonl'))


def _rule_dry_run_rows(day: str) -> list[dict]:
    return _iter_jsonl(os.path.join(OUT_DIR, 'rule_dry_run', f'rule_dry_run_{day}.jsonl'))


def _gate_timeline_rows(day: str) -> list[dict]:
    return _iter_jsonl(os.path.join(OUT_DIR, 'gate_timeline', f'gate_timeline_{day}.jsonl'))


def _no_signal_snapshot_rows(day: str) -> list[dict]:
    return _iter_jsonl(os.path.join(OUT_DIR, 'no_signal_snapshots', f'no_signal_snapshots_{day}.jsonl'))


def _ts(row: dict) -> Optional[float]:
    for key in ('created_at', 'ts', 'timestamp', 'time'):
        value = row.get(key)
        try:
            if isinstance(value, (int, float)):
                return float(value)
        except Exception:
            pass
    return None


def _since(rows: list[dict], minutes: int) -> list[dict]:
    cutoff = _now_ct().timestamp() - minutes * 60
    return [r for r in rows if (_ts(r) or 0) >= cutoff]


def _session_open_ts(day: str) -> float:
    dt = datetime.combine(datetime.fromisoformat(day).date(), time(8, 30), tzinfo=CT)
    return dt.timestamp()


def _broker_snapshot(watched: list[str]) -> dict:
    out = {
        'reachable': False,
        'account': None,
        'positions': [],
        'open_orders': [],
        'watched_positions': [],
        'watched_open_orders': [],
        'error': None,
    }
    try:
        from alpaca_trading import AlpacaTrader
        trader = AlpacaTrader.from_env(paper=True)
        account = trader.get_account()
        positions = trader.list_positions()
        orders = trader.list_orders(status='open', symbols=watched, nested=True)
        watched_set = set(watched)
        out.update({
            'reachable': True,
            'account': {
                'status': account.get('status'),
                'equity': account.get('equity'),
                'cash': account.get('cash'),
                'buying_power': account.get('buying_power'),
                'pattern_day_trader': account.get('pattern_day_trader'),
                'trading_blocked': account.get('trading_blocked'),
                'account_blocked': account.get('account_blocked'),
            },
            'positions': positions,
            'open_orders': orders,
            'watched_positions': [p for p in positions if p.get('symbol') in watched_set],
            'watched_open_orders': [o for o in orders if o.get('symbol') in watched_set],
        })
    except Exception as e:
        out['error'] = str(e)
    return out


def build_broker_safety_snapshot(day: Optional[str] = None, label: str = 'manual') -> dict:
    day = day or _now_ct().date().isoformat()
    cfg = _trading_config()
    watched = list(cfg.get('tickers') or ['CLSK', 'MARA', 'RIOT'])
    status, status_error = _status()
    broker = _broker_snapshot(watched)
    return {
        'day': day,
        'label': label,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'watched': watched,
        'status_reachable': status_error is None,
        'status_error': status_error,
        'local': {
            'running': status.get('running'),
            'in_window': status.get('in_window'),
            'positions': status.get('positions') or {},
            'pending_entries': status.get('pending_entries') or {},
            'broker_exposure_block': status.get('broker_exposure_block'),
            'broker_lifecycle_block': status.get('broker_lifecycle_block'),
            'broker_lifecycle_gate': status.get('broker_lifecycle_gate'),
            'broker_api_degraded': status.get('broker_api_degraded'),
            'kill_switch': status.get('kill_switch') or {},
            'strategy_config_hash': status.get('strategy_config_hash'),
        },
        'broker': broker,
        'verdict': {
            'flat_watched': not broker.get('watched_positions'),
            'no_watched_open_orders': not broker.get('watched_open_orders'),
            'no_local_positions': not (status.get('positions') or {}),
            'no_pending_entries': not (status.get('pending_entries') or {}),
            'no_exposure_block': not status.get('broker_exposure_block'),
            'no_broker_lifecycle_block': not status.get('broker_lifecycle_block'),
        },
    }


def write_broker_safety_snapshot(day: Optional[str] = None, label: str = 'manual') -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_broker_safety_snapshot(day, label=label)
    ts = _now_ct().strftime('%H%M%S')
    path = os.path.join(SNAPSHOT_DIR, f'broker_safety_{day}_{label}_{ts}.json')
    return _write_json(path, payload), payload


def build_premarket_checklist(day: Optional[str] = None) -> dict:
    day = day or _now_ct().date().isoformat()
    cfg = _trading_config()
    status, status_error = _status()
    safety = build_broker_safety_snapshot(day, label='pre_market_checklist')
    session = cfg.get('session') or {}
    checks = [
        {'name': 'app_status_reachable', 'ok': status_error is None, 'detail': status_error},
        {'name': 'config_hash_present', 'ok': bool(_sha256(os.path.join(HERE, 'trading_config.json')))},
        {'name': 'expected_tickers', 'ok': cfg.get('tickers') == ['CLSK', 'MARA', 'RIOT'], 'detail': cfg.get('tickers')},
        {'name': 'broker_flat_watched', 'ok': safety['verdict']['flat_watched']},
        {'name': 'no_watched_open_orders', 'ok': safety['verdict']['no_watched_open_orders']},
        {'name': 'no_local_positions', 'ok': safety['verdict']['no_local_positions']},
        {'name': 'no_pending_entries', 'ok': safety['verdict']['no_pending_entries']},
        {'name': 'no_exposure_block', 'ok': safety['verdict']['no_exposure_block']},
        {'name': 'no_broker_lifecycle_block', 'ok': safety['verdict']['no_broker_lifecycle_block']},
        {'name': 'kill_switch_disabled', 'ok': not (status.get('kill_switch') or {}).get('enabled')},
        {'name': 'alpaca_equity_visible', 'ok': status.get('alpaca_equity') is not None},
        {
            'name': 'flatten_cutoff_1455_ct',
            'ok': (session.get('flatten_hour'), session.get('flatten_minute')) == (14, 55),
            'detail': f"{session.get('flatten_hour')}:{session.get('flatten_minute')}",
        },
    ]
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'config_sha256': _sha256(os.path.join(HERE, 'trading_config.json')),
        'app_running': status.get('running'),
        'in_window': status.get('in_window'),
        'alpaca_equity': status.get('alpaca_equity'),
        'checks': checks,
        'ok': all(bool(c.get('ok')) for c in checks),
        'artifact_paths_ready': {
            'postmortem_json': os.path.join(OUT_DIR, f'postmortem_{day}.json'),
            'review_index': os.path.join(OUT_DIR, f'review_index_{day}.json'),
            'monday_review_packet': os.path.join(OUT_DIR, f'monday_review_packet_{day}.json'),
            'promotion_queue': os.path.join(OUT_DIR, 'promotion_queue.json'),
        },
        'safety_snapshot': safety,
    }


def write_premarket_checklist(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_premarket_checklist(day)
    path = os.path.join(OUT_DIR, f'pre_market_checklist_{day}.json')
    return _write_json(path, payload), payload


def build_config_change_note(day: Optional[str] = None, reason: str = 'weekend review hardening') -> dict:
    day = day or _now_ct().date().isoformat()
    current_hash = _sha256(os.path.join(HERE, 'trading_config.json'))
    previous_hash = None
    previous_day = None
    if os.path.isdir(OUT_DIR):
        for name in sorted(os.listdir(OUT_DIR), reverse=True):
            if name.startswith('artifacts_') and name.endswith('.json'):
                d = name[len('artifacts_'):-len('.json')]
                if d < day:
                    art = _read_json(os.path.join(OUT_DIR, name), {}) or {}
                    previous_hash = art.get('config_sha256')
                    previous_day = d
                    break
    intent_path = os.path.join(OUT_DIR, 'config_intent_ledger.json')
    intent = _read_json(intent_path, {}) or {}
    current_intent = (intent.get('items') or {}).get(current_hash, {})
    return {
        'day': day,
        'era': era_for_day(day),
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'reason': reason,
        'current_config_sha256': current_hash,
        'previous_artifact_day': previous_day,
        'previous_artifact_config_sha256': previous_hash,
        'changed_since_previous_artifact': None if previous_hash is None else previous_hash != current_hash,
        'intent': current_intent or {
            'status': 'missing_intent',
            'expected_impact': 'not documented yet',
            'prove_worked': 'not documented yet',
            'prove_failed': 'not documented yet',
        },
        'live_behavior_note': 'This note records config identity only; promotion queue still requires human approval.',
        'current_config_file': os.path.join(HERE, 'trading_config.json'),
    }


def write_config_change_note(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_config_change_note(day)
    path = os.path.join(OUT_DIR, f'config_changes_{day}.json')
    return _write_json(path, payload), payload


def build_config_intent_ledger(day: Optional[str] = None) -> dict:
    day = day or _now_ct().date().isoformat()
    path = os.path.join(OUT_DIR, 'config_intent_ledger.json')
    ledger = _read_json(path, {'items': {}}) or {'items': {}}
    items = ledger.setdefault('items', {})
    cfg_hash = _sha256(os.path.join(HERE, 'trading_config.json'))
    item = items.setdefault(cfg_hash, {
        'config_sha256': cfg_hash,
        'first_seen_day': day,
        'era': era_for_day(day),
        'what_changed': 'See config diff artifacts and recent change notes.',
        'why_changed': 'Weekend hardening before current era; no automatic rule promotion.',
        'expected_impact': [
            'Reduce operational contamination.',
            'Reduce largest-loss damage.',
            'Improve thesis invalidation visibility.',
            'Keep winner damage visible before future changes.',
        ],
        'prove_worked': [
            'No broker exposure blocks.',
            'Largest loss improves versus 2026-05-01 baseline.',
            'Clean-day score is high enough for strategy conclusions.',
            'Rule candidate quarantine does not show winner damage greater than loser damage avoided.',
        ],
        'prove_failed': [
            'Repeated large losers despite clean execution.',
            'Ops contamination persists.',
            'Winner quality shows many lucky/wide-spread winners carrying P&L.',
            'Strategy conclusion gate remains blocked on clean trading days.',
        ],
        'human_approval_required_for_live_rule_changes': True,
    })
    item['last_seen_day'] = day
    ledger['updated_at_ct'] = _now_ct().isoformat(timespec='seconds')
    ledger['current_config_sha256'] = cfg_hash
    ledger['current_era'] = era_for_day(day)
    return ledger


def write_config_intent_ledger(day: Optional[str] = None) -> tuple[str, dict]:
    payload = build_config_intent_ledger(day)
    path = os.path.join(OUT_DIR, 'config_intent_ledger.json')
    return _write_json(path, payload), payload


def _loser_tag(t: dict) -> list[str]:
    tags = []
    ind = t.get('indicators') or t.get('ind') or {}
    flow120 = ind.get('flow_120s') or {}
    ex = t.get('execution_quality') or t.get('entry_quality') or {}
    spread = ex.get('spread_pct')
    mom60 = ind.get('mom_60s')
    sigma = ind.get('vwap_dist_sigma')
    if spread is not None and float(spread) >= 0.08:
        tags.append('wide_spread')
    if t.get('side') == 'SHORT' and mom60 is not None and float(mom60) > 0:
        tags.append('short_positive_mom60')
    if t.get('side') == 'SHORT' and flow120.get('buy_pct') is not None and float(flow120.get('buy_pct')) >= 62:
        tags.append('short_sustained_buy_flow')
    if sigma is not None and abs(float(sigma)) >= 3:
        tags.append('vwap_extension')
    if (t.get('mfe_pct') or 0) <= 0.05:
        tags.append('low_mfe')
    return tags or ['unclassified']


def build_loser_clusters(day: str) -> dict:
    pm = _postmortem(day)
    losers = [t for t in pm.get('tape') or [] if (t.get('pnl') or 0) < 0]
    clusters = defaultdict(lambda: {'trades': 0, 'gross_loss': 0.0, 'examples': [], 'tags': Counter()})
    for t in losers:
        btc = t.get('btc') or t.get('btc_indicators') or {}
        setup = t.get('setup_type') or t.get('setup') or (t.get('entry_thesis') or {}).get('setup_type')
        score = t.get('entry_score') if t.get('entry_score') is not None else t.get('score')
        key = '|'.join([
            str(t.get('ticker') or 'unknown'),
            str(t.get('side') or 'unknown'),
            str(setup or 'unknown'),
            str(btc.get('regime_detail') or btc.get('regime') or 'unknown'),
            str(t.get('primary_loss_cause') or 'unknown'),
        ])
        row = clusters[key]
        row['trades'] += 1
        row['gross_loss'] += abs(float(t.get('pnl') or 0))
        row['tags'].update(_loser_tag(t))
        if len(row['examples']) < 5:
            row['examples'].append({
                'time': t.get('time'),
                'trade_id': t.get('trade_id'),
                'pnl': t.get('pnl'),
                'reason': t.get('reason'),
                'entry_score': score,
            })
    rows = []
    for key, row in clusters.items():
        ticker, side, setup, btc_regime, primary = key.split('|', 4)
        rows.append({
            'ticker': ticker,
            'side': side,
            'setup_type': setup,
            'btc_regime': btc_regime,
            'primary_loss_cause': primary,
            'trades': row['trades'],
            'gross_loss': round(row['gross_loss'], 2),
            'top_tags': [{'tag': k, 'count': v} for k, v in row['tags'].most_common(8)],
            'examples': row['examples'],
        })
    rows.sort(key=lambda r: (-r['gross_loss'], -r['trades']))
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'loser_count': len(losers),
        'clusters': rows,
        'promotion_note': 'Use clusters to form hypotheses; do not change live rules from one cluster without repeated evidence.',
    }


def write_loser_clusters(day: str) -> tuple[str, dict]:
    payload = build_loser_clusters(day)
    path = os.path.join(OUT_DIR, f'loser_clusters_{day}.json')
    return _write_json(path, payload), payload


def build_entry_quality_tiers(day: str) -> dict:
    tape = _tape(day)
    tiers = defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0, 'examples': []})
    for t in tape:
        try:
            from daily_postmortem import entry_quality_tier_from_row
            tier = entry_quality_tier_from_row(t)
        except Exception:
            tier = {
                'tier': t.get('entry_quality_tier') or (t.get('entry_thesis') or {}).get('entry_quality_tier') or 'unknown',
                'score': t.get('entry_quality_score') or (t.get('entry_thesis') or {}).get('entry_quality_score'),
                'tags': (t.get('entry_thesis') or {}).get('entry_quality_tags') or [],
            }
        key = tier.get('tier') or 'unknown'
        row = tiers[key]
        pnl = float(t.get('pnl') or 0)
        row['trades'] += 1
        row['wins'] += 1 if pnl > 0 else 0
        row['losses'] += 1 if pnl < 0 else 0
        row['pnl'] += pnl
        if len(row['examples']) < 8:
            row['examples'].append({
                'time': t.get('time'),
                'ticker': t.get('ticker'),
                'side': t.get('side'),
                'pnl': t.get('pnl'),
                'score': tier.get('score'),
                'tags': tier.get('tags') or [],
            })
    rows = []
    for key, row in tiers.items():
        trades = row['trades']
        rows.append({
            'tier': key,
            'trades': trades,
            'wins': row['wins'],
            'losses': row['losses'],
            'win_rate': _safe_pct(row['wins'], trades),
            'pnl': round(row['pnl'], 2),
            'avg_pnl': round(row['pnl'] / trades, 2) if trades else None,
            'examples': row['examples'],
        })
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'tiers': sorted(rows, key=lambda r: str(r.get('tier'))),
        'deduction': 'Passive review only. A/B/C tiers should prove separation across multiple clean days before becoming filters or size rules.',
    }


def write_entry_quality_tiers(day: str) -> tuple[str, dict]:
    payload = build_entry_quality_tiers(day)
    path = os.path.join(OUT_DIR, f'entry_quality_tiers_{day}.json')
    return _write_json(path, payload), payload


def build_loser_archetypes(day: str) -> dict:
    pm = _postmortem(day)
    review = pm.get('loser_decision_review') or []
    rows = defaultdict(lambda: {'trades': 0, 'gross_loss': 0.0, 'examples': []})
    for item in review:
        trade = item.get('row') or {}
        archetypes = item.get('archetypes') or []
        if not archetypes:
            try:
                from daily_postmortem import loser_archetypes
                archetypes = loser_archetypes(trade, item.get('tags') or [])
            except Exception:
                archetypes = ['unclassified']
        for name in archetypes:
            row = rows[name]
            row['trades'] += 1
            row['gross_loss'] += abs(float(trade.get('pnl') or 0))
            if len(row['examples']) < 8:
                row['examples'].append({
                    'time': trade.get('time'),
                    'ticker': trade.get('ticker'),
                    'side': trade.get('side'),
                    'pnl': trade.get('pnl'),
                    'reason': trade.get('reason'),
                    'tags': item.get('tags') or [],
                })
    out = [
        {
            'archetype': key,
            'trades': row['trades'],
            'gross_loss': round(row['gross_loss'], 2),
            'examples': row['examples'],
        }
        for key, row in rows.items()
    ]
    out.sort(key=lambda r: (-r['gross_loss'], -r['trades']))
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'archetypes': out,
        'deduction': 'Review the largest archetypes first. Do not change rules unless winner damage and rolling evidence agree.',
    }


def write_loser_archetypes(day: str) -> tuple[str, dict]:
    payload = build_loser_archetypes(day)
    path = os.path.join(OUT_DIR, f'loser_archetypes_{day}.json')
    return _write_json(path, payload), payload


def build_winner_damage_report(day: str) -> dict:
    pm = _postmortem(day)
    rules = pm.get('counterfactual_rules') or []
    rows = []
    for r in rules:
        rows.append({
            'rule': r.get('rule'),
            'losers_avoided': r.get('losers_avoided'),
            'loss_avoided': r.get('loss_avoided'),
            'winners_blocked': r.get('winners_blocked'),
            'winner_pnl_sacrificed': r.get('winner_pnl_sacrificed'),
            'net_estimated_impact': r.get('net_estimated_impact'),
            'confidence': r.get('confidence'),
            'pre_entry_actionable': r.get('actionable_pre_entry'),
            'promotion_readiness': (
                'needs_rolling_evidence'
                if (r.get('losers_avoided') or 0) < 3
                else 'candidate_for_backtest'
                if (r.get('net_estimated_impact') or 0) > 0 and (r.get('winners_blocked') or 0) <= 1
                else 'watch_winner_damage'
            ),
        })
    rows.sort(key=lambda r: (-(r.get('net_estimated_impact') or 0), r.get('winners_blocked') or 0))
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'rules': rows,
        'deduction': 'A rule is not attractive unless avoided loser damage clearly exceeds blocked winner damage across more than one clean day.',
    }


def write_winner_damage_report(day: str) -> tuple[str, dict]:
    payload = build_winner_damage_report(day)
    path = os.path.join(OUT_DIR, f'winner_damage_report_{day}.json')
    return _write_json(path, payload), payload


def build_clean_day_score_artifact(day: str) -> dict:
    pm = _postmortem(day)
    clean = pm.get('clean_day_score') or {}
    if not clean:
        try:
            from daily_postmortem import clean_day_score
            clean = clean_day_score(day, pm.get('flags') or [], pm.get('tape') or [], {})
        except Exception as e:
            clean = {'error': str(e)}
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'clean_day_score': clean,
        'use_for_learning': clean.get('verdict') != 'do_not_promote_rules_from_this_day',
        'deduction': 'Use clean days for strategy conclusions. Ops/data-contaminated days are still useful for process fixes.',
    }


def write_clean_day_score_artifact(day: str) -> tuple[str, dict]:
    payload = build_clean_day_score_artifact(day)
    path = os.path.join(OUT_DIR, f'clean_day_score_{day}.json')
    return _write_json(path, payload), payload


def build_no_trade_opportunity_grades(day: str) -> dict:
    pm = _postmortem(day)
    summary = pm.get('skipped_opportunity_summary') or []
    grades = Counter()
    reason_rows = []
    for row in summary:
        counts = row.get('grade_counts') or {}
        for grade, count in counts.items():
            grades[str(grade)] += int(count or 0)
        reason_rows.append({
            'reason': row.get('reason'),
            'signals': row.get('n'),
            'would_work_5m': row.get('would_work_5m'),
            'avg5': row.get('avg5'),
            'avg15': row.get('avg15'),
            'avg30': row.get('avg30'),
            'grade_counts': counts,
        })
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'grade_counts': dict(grades),
        'reasons': sorted(reason_rows, key=lambda r: (-(r.get('would_work_5m') or 0), -(r.get('signals') or 0)))[:50],
        'deduction': 'Missed_winner/bad_skip clusters tell us where gates may be too strict; avoided_loser/good_skip proves restraint is working.',
    }


def write_no_trade_opportunity_grades(day: str) -> tuple[str, dict]:
    payload = build_no_trade_opportunity_grades(day)
    path = os.path.join(OUT_DIR, f'no_trade_opportunity_grades_{day}.json')
    return _write_json(path, payload), payload


def build_thesis_failure_review(day: str) -> dict:
    pm = _postmortem(day)
    review = pm.get('loser_decision_review') or []
    rows = defaultdict(lambda: {'trades': 0, 'gross_loss': 0.0, 'examples': []})
    for item in review:
        trade = item.get('row') or {}
        tf = item.get('thesis_failure') or {}
        label = tf.get('label') or 'unclassified'
        row = rows[label]
        row['trades'] += 1
        row['gross_loss'] += abs(float(trade.get('pnl') or 0))
        if len(row['examples']) < 8:
            row['examples'].append({
                'time': trade.get('time'),
                'ticker': trade.get('ticker'),
                'side': trade.get('side'),
                'pnl': trade.get('pnl'),
                'reason': trade.get('reason'),
                'confidence': tf.get('confidence'),
            })
    out = [
        {'label': k, 'trades': v['trades'], 'gross_loss': round(v['gross_loss'], 2), 'examples': v['examples']}
        for k, v in rows.items()
    ]
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'failure_modes': sorted(out, key=lambda r: (-r['gross_loss'], -r['trades'])),
        'deduction': 'This answers what part of the original trade thesis failed: BTC, miner/BTC relationship, entry timing, exit policy, execution, or operations.',
    }


def write_thesis_failure_review(day: str) -> tuple[str, dict]:
    payload = build_thesis_failure_review(day)
    path = os.path.join(OUT_DIR, f'thesis_failure_review_{day}.json')
    return _write_json(path, payload), payload


def build_loser_replay_snapshots(day: str) -> dict:
    pm = _postmortem(day)
    review = pm.get('loser_decision_review') or []
    rows = []
    for item in review:
        snap = item.get('replay_snapshot') or {}
        if not snap:
            continue
        snap = dict(snap)
        snap['thesis_failure'] = (item.get('thesis_failure') or {}).get('label')
        snap['archetypes'] = item.get('archetypes') or []
        rows.append(snap)
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'loser_count': len(rows),
        'snapshots': rows,
        'deduction': 'Use 30s/1m/3m/5m movement to refine indicator-based exits without arbitrary timers.',
    }


def write_loser_replay_snapshots(day: str) -> tuple[str, dict]:
    payload = build_loser_replay_snapshots(day)
    path = os.path.join(OUT_DIR, f'loser_replay_snapshots_{day}.json')
    return _write_json(path, payload), payload


def build_per_symbol_personality(day: str, lookback: int = 10) -> dict:
    days = _available_report_days(day, lookback)
    rows = defaultdict(lambda: {
        'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0,
        'by_side': Counter(), 'loser_archetypes': Counter(), 'setups': Counter(),
        'spread_values': [], 'slippage_values': [],
    })
    for d in days:
        pm = _postmortem(d)
        loser_by_key = {}
        for item in pm.get('loser_decision_review') or []:
            tr = item.get('row') or {}
            key = (tr.get('time'), tr.get('ticker'), tr.get('side'), tr.get('pnl'))
            loser_by_key[key] = item
        for t in pm.get('tape') or []:
            sym = str(t.get('ticker') or 'unknown')
            r = rows[sym]
            pnl = float(t.get('pnl') or 0)
            r['trades'] += 1
            r['wins'] += 1 if pnl > 0 else 0
            r['losses'] += 1 if pnl < 0 else 0
            r['pnl'] += pnl
            r['by_side'][str(t.get('side') or 'unknown')] += 1
            r['setups'][str(t.get('setup_type') or t.get('setup') or (t.get('entry_thesis') or {}).get('setup_type') or 'unknown')] += 1
            spread = ((t.get('forensics') or {}).get('entry_quality') or {}).get('spread_pct')
            slip = t.get('entry_slippage_pct')
            if spread is not None:
                r['spread_values'].append(float(spread))
            if slip is not None:
                r['slippage_values'].append(float(slip))
            key = (t.get('time'), t.get('ticker'), t.get('side'), t.get('pnl'))
            item = loser_by_key.get(key)
            if item:
                r['loser_archetypes'].update(item.get('archetypes') or [])
    out = []
    for sym, r in rows.items():
        out.append({
            'ticker': sym,
            'trades': r['trades'],
            'wins': r['wins'],
            'losses': r['losses'],
            'win_rate': _safe_pct(r['wins'], r['trades']),
            'pnl': round(r['pnl'], 2),
            'avg_pnl': round(r['pnl'] / r['trades'], 2) if r['trades'] else None,
            'side_mix': dict(r['by_side']),
            'top_setups': [{'setup': k, 'count': v} for k, v in r['setups'].most_common(8)],
            'top_loser_archetypes': [{'archetype': k, 'count': v} for k, v in r['loser_archetypes'].most_common(8)],
            'avg_spread_pct': round(sum(r['spread_values']) / len(r['spread_values']), 4) if r['spread_values'] else None,
            'avg_entry_slippage_pct': round(sum(r['slippage_values']) / len(r['slippage_values']), 4) if r['slippage_values'] else None,
        })
    return {
        'day': day,
        'lookback': lookback,
        'days': days,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'symbols': sorted(out, key=lambda r: r['ticker']),
        'deduction': 'Per-symbol personality profile; use to learn CLSK/MARA/RIOT differences before symbol-specific live behavior changes.',
    }


def write_per_symbol_personality(day: str, lookback: int = 10) -> tuple[str, dict]:
    payload = build_per_symbol_personality(day, lookback=lookback)
    path = os.path.join(OUT_DIR, f'per_symbol_personality_{day}_{lookback}d.json')
    return _write_json(path, payload), payload


def build_out_of_sample_scoreboard(day: str, baseline_day: str = '2026-05-01') -> dict:
    current = _postmortem(day)
    baseline = _postmortem(baseline_day)
    def summary(pm: dict) -> dict:
        tape = pm.get('tape') or []
        pnl = round(sum(float(t.get('pnl') or 0) for t in tape), 2)
        wins = sum(1 for t in tape if (t.get('pnl') or 0) > 0)
        losses = sum(1 for t in tape if (t.get('pnl') or 0) < 0)
        archetypes = Counter()
        for item in pm.get('loser_decision_review') or []:
            archetypes.update(item.get('archetypes') or [])
        return {
            'trades': len(tape),
            'wins': wins,
            'losses': losses,
            'win_rate': _safe_pct(wins, len(tape)),
            'pnl': pnl,
            'largest_loss': min([float(t.get('pnl') or 0) for t in tape], default=0.0),
            'top_loser_archetypes': [{'archetype': k, 'count': v} for k, v in archetypes.most_common(6)],
            'clean_day_score': pm.get('clean_day_score'),
            'entry_quality_summary': pm.get('entry_quality_summary'),
        }
    cur = summary(current)
    base = summary(baseline)
    return {
        'day': day,
        'baseline_day': baseline_day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'current': cur,
        'baseline': base,
        'delta_vs_baseline': {
            'pnl': round((cur.get('pnl') or 0) - (base.get('pnl') or 0), 2),
            'win_rate': (
                round((cur.get('win_rate') or 0) - (base.get('win_rate') or 0), 1)
                if cur.get('win_rate') is not None and base.get('win_rate') is not None else None
            ),
            'largest_loss': round((cur.get('largest_loss') or 0) - (base.get('largest_loss') or 0), 2),
        },
        'deduction': 'Out-of-sample comparison against the pre-weekend failure day. Monday and later should reduce operational contamination and largest-loss damage.',
    }


def write_out_of_sample_scoreboard(day: str) -> tuple[str, dict]:
    payload = build_out_of_sample_scoreboard(day)
    path = os.path.join(OUT_DIR, f'out_of_sample_scoreboard_{day}.json')
    return _write_json(path, payload), payload


def _trades_between(day: str, start_hour: int, start_minute: int,
                    end_hour: int, end_minute: int) -> list[dict]:
    start_dt = datetime.combine(datetime.fromisoformat(day).date(), time(start_hour, start_minute), tzinfo=CT)
    end_dt = datetime.combine(datetime.fromisoformat(day).date(), time(end_hour, end_minute), tzinfo=CT)
    start_ts = start_dt.timestamp()
    end_ts = end_dt.timestamp()
    rows = []
    for t in _tape(day):
        ts = t.get('closed_at') or t.get('opened_at')
        if ts is not None and start_ts <= float(ts) <= end_ts:
            rows.append(t)
    return rows


def build_first_hour_review(day: Optional[str] = None) -> dict:
    day = day or _now_ct().date().isoformat()
    first_hour = _trades_between(day, 8, 30, 9, 30)
    full_tape = _tape(day)
    heartbeats = _heartbeat_rows(day)
    open_ts = _session_open_ts(day)
    first_end = open_ts + 60 * 60
    first_heartbeats = [r for r in heartbeats if open_ts <= (_ts(r) or 0) <= first_end]
    why = build_why_no_trade_summary(day, recent_minutes=60)
    alerts = build_trade_alerts(day)

    def summarize(items: list[dict]) -> dict:
        pnl = round(sum(float(t.get('pnl') or 0) for t in items), 2)
        wins = sum(1 for t in items if (t.get('pnl') or 0) > 0)
        losses = sum(1 for t in items if (t.get('pnl') or 0) < 0)
        return {
            'trades': len(items),
            'wins': wins,
            'losses': losses,
            'win_rate': _safe_pct(wins, len(items)),
            'pnl': pnl,
            'largest_loss': min([float(t.get('pnl') or 0) for t in items], default=0.0),
        }

    first_pm = {'tape': first_hour, 'loser_decision_review': []}
    try:
        from daily_postmortem import build_loser_review
        first_pm['loser_decision_review'] = build_loser_review(first_hour)
    except Exception:
        first_pm['loser_decision_review'] = []
    archetypes = Counter()
    thesis = Counter()
    for item in first_pm['loser_decision_review']:
        archetypes.update(item.get('archetypes') or [])
        label = (item.get('thesis_failure') or {}).get('label')
        if label:
            thesis[label] += 1
    freshness = (first_heartbeats[-1].get('ticker_freshness') if first_heartbeats else {}) or {}
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'window_ct': '08:30-09:30',
        'first_hour': summarize(first_hour),
        'full_day_so_far': summarize(full_tape),
        'entry_quality_tiers': build_entry_quality_tiers(day).get('tiers') or [],
        'top_loser_archetypes_first_hour': [{'archetype': k, 'count': v} for k, v in archetypes.most_common(8)],
        'top_thesis_failures_first_hour': [{'label': k, 'count': v} for k, v in thesis.most_common(8)],
        'latest_feed_freshness_first_hour': freshness,
        'why_no_trade_recent': why.get('recent'),
        'alerts': alerts.get('alerts') or [],
        'open_positions': (_status()[0].get('positions') or {}),
        'deduction': 'First-hour review is passive. Use it to spot bad damage early; do not change live rules from one hour alone unless it is an operational safety issue.',
    }


def write_first_hour_review(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_first_hour_review(day)
    path = os.path.join(OUT_DIR, f'first_hour_review_{day}.json')
    return _write_json(path, payload), payload


def build_strategy_conclusion_gate(day: Optional[str] = None) -> dict:
    day = day or _now_ct().date().isoformat()
    pm = _postmortem(day)
    tape = pm.get('tape') or []
    clean = (
        _read_json(os.path.join(OUT_DIR, f'clean_day_score_{day}.json'), {}) or {}
    ).get('clean_day_score') or pm.get('clean_day_score') or {}
    winner_damage = (
        _read_json(os.path.join(OUT_DIR, f'winner_damage_report_{day}.json'), {}) or {}
    ).get('rules') or pm.get('counterfactual_rules') or []
    replay = _read_json(os.path.join(OUT_DIR, f'loser_replay_snapshots_{day}.json'), {}) or {}
    split = _read_json(os.path.join(OUT_DIR, f'strategy_operations_split_{day}.json'), {}) or {}
    checks = []
    checks.append({
        'name': 'clean_day_score_high_enough',
        'ok': int(clean.get('score') or 0) >= 80,
        'detail': clean,
    })
    checks.append({
        'name': 'no_operations_contamination',
        'ok': (split.get('operations') or {}).get('trades', 0) == 0,
        'detail': split.get('operations'),
    })
    checks.append({
        'name': 'enough_trades_for_strategy_read',
        'ok': len(tape) >= 8,
        'detail': len(tape),
    })
    checks.append({
        'name': 'loser_replay_present',
        'ok': (replay.get('loser_count') or 0) >= sum(1 for t in tape if (t.get('pnl') or 0) < 0),
        'detail': {'replay_losers': replay.get('loser_count'), 'actual_losers': sum(1 for t in tape if (t.get('pnl') or 0) < 0)},
    })
    overfit = [
        r for r in winner_damage
        if (r.get('winner_pnl_sacrificed') or 0) > (r.get('loss_avoided') or 0)
    ]
    positive = [
        r for r in winner_damage
        if (r.get('net_estimated_impact') or 0) > 0 and not (
            (r.get('winner_pnl_sacrificed') or 0) > (r.get('loss_avoided') or 0)
        )
    ]
    checks.append({
        'name': 'winner_damage_acceptability_known',
        'ok': bool(winner_damage),
        'detail': {'rules': len(winner_damage), 'overfit_warnings': len(overfit), 'positive_candidates': len(positive)},
    })
    eligible = all(c.get('ok') for c in checks)
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'strategy_conclusions_allowed': eligible,
        'recommendation': (
            'Strategy conclusions allowed for human review; promotion still requires rolling evidence/backtest.'
            if eligible else
            'Do not promote strategy changes from this day. Use it for monitoring, process fixes, or hypothesis tracking only.'
        ),
        'checks': checks,
        'overfit_warning_rules': overfit[:10],
        'positive_candidate_rules': positive[:10],
    }


def write_strategy_conclusion_gate(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_strategy_conclusion_gate(day)
    path = os.path.join(OUT_DIR, f'strategy_conclusion_gate_{day}.json')
    return _write_json(path, payload), payload


def build_market_context_scoreboard(day: Optional[str] = None) -> dict:
    day = day or _now_ct().date().isoformat()
    tape = _tape(day)
    market = build_market_regime_day_label(day)
    split = _read_json(os.path.join(OUT_DIR, f'strategy_operations_split_{day}.json'), {}) or {}
    tiers = _read_json(os.path.join(OUT_DIR, f'entry_quality_tiers_{day}.json'), {}) or {}
    heartbeats = _heartbeat_rows(day)
    freshness = (heartbeats[-1].get('ticker_freshness') if heartbeats else {}) or {}
    by_side = defaultdict(lambda: {'trades': 0, 'pnl': 0.0, 'wins': 0})
    spread_vals = []
    rel_vals = []
    for t in tape:
        side = str(t.get('side') or 'unknown')
        pnl = float(t.get('pnl') or 0)
        by_side[side]['trades'] += 1
        by_side[side]['pnl'] += pnl
        by_side[side]['wins'] += 1 if pnl > 0 else 0
        spread = ((t.get('forensics') or {}).get('entry_quality') or {}).get('spread_pct')
        rel = ((t.get('forensics') or {}).get('relative_strength') or {}).get('stock_minus_btc_implied_60s')
        if spread is not None:
            spread_vals.append(float(spread))
        if rel is not None:
            rel_vals.append(float(rel))
    side_rows = []
    for side, row in by_side.items():
        side_rows.append({
            'side': side,
            'trades': row['trades'],
            'wins': row['wins'],
            'win_rate': _safe_pct(row['wins'], row['trades']),
            'pnl': round(row['pnl'], 2),
        })
    long_pnl = by_side['LONG']['pnl'] if 'LONG' in by_side else 0
    short_pnl = by_side['SHORT']['pnl'] if 'SHORT' in by_side else 0
    hindsight_bias = 'balanced_or_no_data'
    if long_pnl - short_pnl >= 100:
        hindsight_bias = 'longs_favored'
    elif short_pnl - long_pnl >= 100:
        hindsight_bias = 'shorts_favored'
    avg_spread = sum(spread_vals) / len(spread_vals) if spread_vals else None
    liquidity = 'unknown'
    if avg_spread is not None:
        liquidity = 'clean' if avg_spread <= 0.05 else ('mixed' if avg_spread <= 0.09 else 'wide_spread')
    avg_rel = sum(rel_vals) / len(rel_vals) if rel_vals else None
    miner_lead_lag = 'unknown'
    if avg_rel is not None:
        miner_lead_lag = 'miners_leading_btc' if avg_rel > 0.10 else ('miners_lagging_btc' if avg_rel < -0.10 else 'miners_inline_btc')
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'session_weather': {
            'market_regime_label': market.get('label'),
            'dominant_btc_regime': market.get('dominant_btc_regime'),
            'miner_lead_lag': miner_lead_lag,
            'liquidity_regime': liquidity,
            'hindsight_side_bias': hindsight_bias,
            'strategy_vs_operations': {
                'strategy_pnl': (split.get('strategy') or {}).get('pnl'),
                'operations_pnl': (split.get('operations') or {}).get('pnl'),
            },
        },
        'side_scoreboard': sorted(side_rows, key=lambda r: r.get('pnl') or 0, reverse=True),
        'entry_quality_tiers': tiers.get('tiers') or [],
        'latest_feed_freshness': freshness,
        'deduction': 'Session weather separates bad trade selection from a hostile/choppy environment. It is diagnostic only.',
    }


def write_market_context_scoreboard(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_market_context_scoreboard(day)
    path = os.path.join(OUT_DIR, f'market_context_scoreboard_{day}.json')
    return _write_json(path, payload), payload


def build_trade_thesis_timeline_artifact(day: Optional[str] = None) -> dict:
    day = day or _now_ct().date().isoformat()
    pm = _postmortem(day)
    rows = pm.get('trade_thesis_timelines') or []
    counts = Counter(str(r.get('verdict') or 'unknown') for r in rows)
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'counts': dict(counts),
        'timelines': rows[:200],
        'deduction': 'Timeline labels show whether entries confirmed quickly, weakened, invalidated, or won by late reversal.',
    }


def write_trade_thesis_timeline_artifact(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_trade_thesis_timeline_artifact(day)
    path = os.path.join(OUT_DIR, f'trade_thesis_timelines_{day}.json')
    return _write_json(path, payload), payload


def build_winner_quality_artifact(day: Optional[str] = None) -> dict:
    day = day or _now_ct().date().isoformat()
    pm = _postmortem(day)
    review = pm.get('winner_quality_review') or {}
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'counts': review.get('counts') or {},
        'rows': (review.get('rows') or [])[:200],
        'deduction': 'Clean winners deserve credit; lucky/wide-spread/BTC-conflict winners should not be used as proof that a setup is robust.',
    }


def write_winner_quality_artifact(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_winner_quality_artifact(day)
    path = os.path.join(OUT_DIR, f'winner_quality_{day}.json')
    return _write_json(path, payload), payload


def build_rule_candidate_quarantine(day: Optional[str] = None) -> dict:
    day = day or _now_ct().date().isoformat()
    pm = _postmortem(day)
    gate = _read_json(os.path.join(OUT_DIR, f'strategy_conclusion_gate_{day}.json'), {}) or build_strategy_conclusion_gate(day)
    clean = _read_json(os.path.join(OUT_DIR, f'clean_day_score_{day}.json'), {}) or {}
    split = _read_json(os.path.join(OUT_DIR, f'strategy_operations_split_{day}.json'), {}) or {}
    rows = []
    for r in pm.get('counterfactual_rules') or []:
        winner_damage_bad = (r.get('winner_pnl_sacrificed') or 0) > (r.get('loss_avoided') or 0)
        ops_contaminated = (split.get('operations') or {}).get('trades', 0) > 0
        eligible = (
            bool(gate.get('strategy_conclusions_allowed'))
            and not winner_damage_bad
            and not ops_contaminated
            and (r.get('net_estimated_impact') or 0) > 0
            and (r.get('losers_avoided') or 0) >= 2
        )
        rows.append({
            'rule': r.get('rule'),
            'status': 'eligible_for_backtest_only' if eligible else 'quarantined_collect_more',
            'losers_avoided': r.get('losers_avoided'),
            'loss_avoided': r.get('loss_avoided'),
            'winners_blocked': r.get('winners_blocked'),
            'winner_pnl_sacrificed': r.get('winner_pnl_sacrificed'),
            'net_estimated_impact': r.get('net_estimated_impact'),
            'pre_entry_actionable': r.get('actionable_pre_entry'),
            'clean_day_score': (clean.get('clean_day_score') or clean).get('score'),
            'strategy_conclusions_allowed': gate.get('strategy_conclusions_allowed'),
            'winner_damage_bad': winner_damage_bad,
            'ops_contaminated': ops_contaminated,
            'human_approval_required': True,
        })
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'items': sorted(rows, key=lambda r: (r.get('status') != 'eligible_for_backtest_only', -(r.get('net_estimated_impact') or 0))),
        'deduction': 'No rule leaves quarantine without clean-day eligibility, acceptable winner damage, backtest/rolling evidence, and human approval.',
    }


def write_rule_candidate_quarantine(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_rule_candidate_quarantine(day)
    path = os.path.join(OUT_DIR, f'rule_candidate_quarantine_{day}.json')
    return _write_json(path, payload), payload


def build_world_class_dashboard(day: Optional[str] = None) -> dict:
    day = day or _now_ct().date().isoformat()
    status, status_error = _status()
    alerts = _read_json(os.path.join(OUT_DIR, f'trade_alerts_{day}.json'), {}) or build_trade_alerts(day)
    market = _read_json(os.path.join(OUT_DIR, f'market_context_scoreboard_{day}.json'), {}) or build_market_context_scoreboard(day)
    first = _read_json(os.path.join(OUT_DIR, f'first_hour_review_{day}.json'), {}) or build_first_hour_review(day)
    gate = _read_json(os.path.join(OUT_DIR, f'strategy_conclusion_gate_{day}.json'), {}) or build_strategy_conclusion_gate(day)
    winner = _read_json(os.path.join(OUT_DIR, f'winner_quality_{day}.json'), {}) or build_winner_quality_artifact(day)
    loser = _read_json(os.path.join(OUT_DIR, f'loser_archetypes_{day}.json'), {}) or build_loser_archetypes(day)
    why = _read_json(os.path.join(OUT_DIR, f'why_no_trade_{day}.json'), {}) or build_why_no_trade_summary(day)
    feed = _read_json(os.path.join(OUT_DIR, f'feed_health_score_{day}.json'), {}) or build_feed_health_score(day)
    execution = _read_json(os.path.join(OUT_DIR, f'broker_execution_score_{day}.json'), {}) or build_broker_execution_score(day)
    era = _read_json(os.path.join(OUT_DIR, f'era_scorecard_{day}.json'), {}) or build_era_scorecard(day)
    monday = _read_json(os.path.join(OUT_DIR, f'monday_scorecard_{day}.json'), {}) or build_monday_scorecard(day)
    replay = _read_json(os.path.join(OUT_DIR, f'ws_scalp_replay_{day}.json'), {}) or build_ws_scalp_replay(day)
    regrade = _read_json(os.path.join(OUT_DIR, f'current_engine_regrade_{day}.json'), {}) or build_current_engine_regrade(day)
    near_miss = _read_json(os.path.join(OUT_DIR, f'near_miss_winners_{day}.json'), {}) or build_near_miss_winners(day)
    exit_quality = _read_json(os.path.join(OUT_DIR, f'exit_quality_score_{day}.json'), {}) or build_exit_quality_score(day)
    regime_specific = _read_json(os.path.join(OUT_DIR, f'regime_specific_scorecards_{day}.json'), {}) or build_regime_specific_scorecards(day)
    review_gate = _read_json(os.path.join(OUT_DIR, f'daily_review_gate_{day}.json'), {}) or build_daily_review_gate(day)
    lifecycle = _read_json(os.path.join(OUT_DIR, f'rule_lifecycle_dashboard_{day}.json'), {}) or build_rule_lifecycle_dashboard(day)
    replay_conf = _read_json(os.path.join(OUT_DIR, f'replay_confidence_{day}.json'), {}) or build_replay_confidence(day)
    fpfn = _read_json(os.path.join(OUT_DIR, f'false_positive_negative_{day}.json'), {}) or build_false_positive_negative_tables(day)
    return {
        'day': day,
        'era': era_for_day(day),
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'safe': {
            'status_reachable': status_error is None,
            'running': status.get('running'),
            'in_window': status.get('in_window'),
            'positions': status.get('positions') or {},
            'pending_entries': status.get('pending_entries') or {},
            'broker_exposure_block': status.get('broker_exposure_block'),
            'broker_lifecycle_block': status.get('broker_lifecycle_block'),
            'alerts': alerts.get('alerts') or [],
        },
        'trading_quality': {
            'market_weather': market.get('session_weather'),
            'first_hour': first.get('first_hour'),
            'winner_quality_counts': winner.get('counts'),
            'largest_loser_archetypes': (loser.get('archetypes') or [])[:5],
            'current_engine_regrade_counts': regrade.get('counts'),
            'near_miss_winner_count': near_miss.get('count'),
            'exit_quality_score': exit_quality.get('score'),
            'strategy_conclusions_allowed': gate.get('strategy_conclusions_allowed'),
            'strategy_gate_recommendation': gate.get('recommendation'),
            'daily_review_gate': review_gate.get('verdict'),
            'rule_lifecycle_counts': lifecycle.get('counts'),
            'false_positive_count': fpfn.get('false_positive_count'),
            'false_negative_count': fpfn.get('false_negative_count'),
        },
        'health': {
            'feed_score': feed.get('score'),
            'feed_verdict': feed.get('verdict'),
            'broker_execution_score': execution.get('score'),
            'broker_execution_verdict': execution.get('verdict'),
            'monday_scorecard_verdict': monday.get('verdict'),
            'era_summary': era.get('summary'),
            'ws_scalp_replay_counts': replay.get('counts'),
            'replay_confidence': {
                'verdict': replay_conf.get('verdict'),
                'usable_replay_count': replay_conf.get('usable_replay_count'),
                'usable_replay_pct': replay_conf.get('usable_replay_pct'),
            },
            'regime_specific_scorecards_file': os.path.join(OUT_DIR, f'regime_specific_scorecards_{day}.json'),
        },
        'what_to_look_at_next': [
            os.path.join(OUT_DIR, f'first_hour_review_{day}.json'),
            os.path.join(OUT_DIR, f'market_context_scoreboard_{day}.json'),
            os.path.join(OUT_DIR, f'trade_thesis_timelines_{day}.json'),
            os.path.join(OUT_DIR, f'winner_quality_{day}.json'),
            os.path.join(OUT_DIR, f'rule_candidate_quarantine_{day}.json'),
            os.path.join(OUT_DIR, f'strategy_conclusion_gate_{day}.json'),
            os.path.join(OUT_DIR, f'why_no_trade_{day}.json'),
            os.path.join(OUT_DIR, f'feed_health_score_{day}.json'),
            os.path.join(OUT_DIR, f'broker_execution_score_{day}.json'),
            os.path.join(OUT_DIR, f'era_scorecard_{day}.json'),
            os.path.join(OUT_DIR, f'monday_scorecard_{day}.json'),
            os.path.join(OUT_DIR, f'ws_scalp_replay_{day}.json'),
            os.path.join(OUT_DIR, f'decision_audit_summary_{day}.json'),
            os.path.join(OUT_DIR, f'current_engine_regrade_{day}.json'),
            os.path.join(OUT_DIR, f'regime_specific_scorecards_{day}.json'),
            os.path.join(OUT_DIR, f'near_miss_winners_{day}.json'),
            os.path.join(OUT_DIR, f'exit_quality_score_{day}.json'),
            os.path.join(OUT_DIR, f'replay_confidence_{day}.json'),
            os.path.join(OUT_DIR, f'false_positive_negative_{day}.json'),
            os.path.join(OUT_DIR, f'daily_review_gate_{day}.json'),
            os.path.join(OUT_DIR, f'rule_lifecycle_dashboard_{day}.json'),
            os.path.join(OUT_DIR, f'NOW_STATUS_{day}.json'),
        ],
        'why_no_trade_recent': why.get('recent'),
    }


def write_world_class_dashboard(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_world_class_dashboard(day)
    path = os.path.join(OUT_DIR, f'world_class_dashboard_{day}.json')
    return _write_json(path, payload), payload


def build_current_config_replay(day: str) -> dict:
    payload = {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'current_config_sha256': _sha256(os.path.join(HERE, 'trading_config.json')),
        'mode': 'diagnostic_replay_only',
        'scoreboard': None,
        'error': None,
    }
    try:
        from engine_scoreboard import build_scoreboard
        payload['scoreboard'] = build_scoreboard(day)
    except Exception as e:
        payload['error'] = str(e)
    return payload


def write_current_config_replay(day: str) -> tuple[str, dict]:
    payload = build_current_config_replay(day)
    path = os.path.join(OUT_DIR, f'current_config_replay_{day}.json')
    return _write_json(path, payload), payload


def build_ws_scalp_replay(day: Optional[str] = None) -> dict:
    day = day or _now_ct().date().isoformat()
    try:
        from engine_replay import build_replay
        return build_replay(day)
    except Exception as e:
        return {
            'day': day,
            'created_at_ct': _now_ct().isoformat(timespec='seconds'),
            'mode': 'diagnostic_replay_only_no_orders',
            'error': str(e),
        }


def write_ws_scalp_replay(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_ws_scalp_replay(day)
    path = os.path.join(OUT_DIR, f'ws_scalp_replay_{day}.json')
    return _write_json(path, payload), payload


def build_decision_audit_summary(day: Optional[str] = None) -> dict:
    day = day or _now_ct().date().isoformat()
    path = os.path.join(OUT_DIR, 'decision_audits', f'decision_audits_{day}.jsonl')
    rows = _iter_jsonl(path)
    by_tier = Counter()
    by_setup = Counter()
    by_side = Counter()
    weak_flags = Counter()
    for r in rows:
        tier = (r.get('entry_quality_tier') or {}).get('tier') or r.get('entry_quality_tier') or 'unknown'
        by_tier[str(tier)] += 1
        by_setup[str(r.get('setup_type') or 'unknown')] += 1
        by_side[str(r.get('side') or 'unknown')] += 1
        gates = r.get('gates') or {}
        for name, value in gates.items():
            if value:
                weak_flags[name] += 1
        eq = r.get('execution_quality') or {}
        if (eq.get('score') or 100) < 75:
            weak_flags['execution_quality_below_75'] += 1
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'audit_count': len(rows),
        'by_tier': dict(by_tier),
        'by_setup': dict(by_setup),
        'by_side': dict(by_side),
        'weak_flags': dict(weak_flags),
        'sample_rows': rows[:100],
        'deduction': 'Exact entry-time decision snapshots. This is the first artifact to open when a trade rationale looks confusing.',
    }


def write_decision_audit_summary(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_decision_audit_summary(day)
    path = os.path.join(OUT_DIR, f'decision_audit_summary_{day}.json')
    return _write_json(path, payload), payload


def _would_take_label(t: dict, replay_by_key: dict) -> str:
    key = (t.get('ticker'), t.get('side'), t.get('opened_at') or t.get('closed_at'), t.get('pnl'))
    replay = replay_by_key.get(key)
    if replay and replay.get('current_label') == 'would_skip_now':
        return 'would_skip_now'
    reason = str(t.get('reason') or '')
    if reason.startswith(('short_conviction_decay', 'long_conviction_decay', 'failed_followthrough', 'hard_adverse', 'btc_impulse_abort')):
        return 'would_exit_faster_or_safety_exit'
    dq = t.get('decision_quality') or {}
    label = dq.get('label')
    if label and label not in ('profitable_or_flat', 'valid_decision_bad_outcome'):
        return str(label)
    if (t.get('pnl') or 0) < 0:
        thesis = t.get('entry_thesis') or {}
        tier = t.get('entry_quality_tier') or thesis.get('entry_quality_tier')
        if tier in ('C', 'D', 'weak', 'poor'):
            return 'low_quality_entry'
        return 'still_valid_bad_outcome_or_unclear'
    return 'still_valid_winner'


def build_current_engine_regrade(day: Optional[str] = None) -> dict:
    day = day or _now_ct().date().isoformat()
    tape = _tape(day)
    replay = _read_json(os.path.join(OUT_DIR, f'ws_scalp_replay_{day}.json'), {}) or build_ws_scalp_replay(day)
    replay_by_key = {}
    for r in replay.get('rows') or []:
        if r.get('source') == 'trade':
            replay_by_key[(r.get('ticker'), r.get('historical_side'), r.get('created_at'), r.get('pnl'))] = r
    rows = []
    counts = Counter()
    for t in tape:
        label = _would_take_label(t, replay_by_key)
        counts[label] += 1
        rows.append({
            'ticker': t.get('ticker'),
            'side': t.get('side'),
            'pnl': t.get('pnl'),
            'reason': t.get('reason'),
            'entry_score': t.get('entry_score') if t.get('entry_score') is not None else t.get('score'),
            'setup_type': t.get('setup_type') or t.get('setup') or (t.get('entry_thesis') or {}).get('setup_type'),
            'current_engine_label': label,
            'decision_quality': t.get('decision_quality'),
            'entry_quality_tier': t.get('entry_quality_tier') or (t.get('entry_thesis') or {}).get('entry_quality_tier'),
        })
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'counts': dict(counts),
        'rows': rows,
        'deduction': 'Regrades completed trades under current review logic so we can see what the bot would refuse or exit faster now.',
    }


def write_current_engine_regrade(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_current_engine_regrade(day)
    path = os.path.join(OUT_DIR, f'current_engine_regrade_{day}.json')
    return _write_json(path, payload), payload


def build_replay_confidence(day: Optional[str] = None) -> dict:
    day = day or _now_ct().date().isoformat()
    replay = _read_json(os.path.join(OUT_DIR, f'ws_scalp_replay_{day}.json'), {}) or build_ws_scalp_replay(day)
    rows = replay.get('rows') or []
    counts = Counter()
    actionable = Counter()
    for r in rows:
        conf = (r.get('replay_confidence') or {}).get('label') or 'unknown'
        counts[conf] += 1
        if conf in ('high', 'medium'):
            actionable[r.get('current_label') or 'unknown'] += 1
    total = len(rows)
    usable = counts.get('high', 0) + counts.get('medium', 0)
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'processed_count': total,
        'confidence_counts': dict(counts),
        'usable_replay_count': usable,
        'usable_replay_pct': _safe_pct(usable, total),
        'actionable_current_label_counts': dict(actionable),
        'verdict': 'usable' if usable >= 25 or (total and usable / total >= 0.5) else 'low_confidence_replay',
        'deduction': 'Use high/medium confidence replay rows for rule research. Low-confidence rows are directional only.',
    }


def write_replay_confidence(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_replay_confidence(day)
    path = os.path.join(OUT_DIR, f'replay_confidence_{day}.json')
    return _write_json(path, payload), payload


def build_false_positive_negative_tables(day: Optional[str] = None) -> dict:
    day = day or _now_ct().date().isoformat()
    replay = _read_json(os.path.join(OUT_DIR, f'ws_scalp_replay_{day}.json'), {}) or build_ws_scalp_replay(day)
    false_positives = []
    false_negatives = []
    for r in replay.get('rows') or []:
        conf = (r.get('replay_confidence') or {}).get('label')
        if conf == 'low':
            continue
        label = r.get('current_label')
        if label == 'would_skip_now' and r.get('historical_decision') == 'enter':
            false_positives.append(r)
        elif label == 'would_enter_now' and r.get('historical_decision') in ('skip', 'near'):
            false_negatives.append(r)
    fp_reason_counts = Counter(str(r.get('reason') or 'unknown') for r in false_positives)
    fn_reason_counts = Counter(str(r.get('reason') or 'unknown') for r in false_negatives)
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'false_positive_count': len(false_positives),
        'false_negative_count': len(false_negatives),
        'false_positive_top_reasons': [{'reason': k, 'count': v} for k, v in fp_reason_counts.most_common(20)],
        'false_negative_top_reasons': [{'reason': k, 'count': v} for k, v in fn_reason_counts.most_common(20)],
        'false_positives': false_positives[:200],
        'false_negatives': false_negatives[:200],
        'deduction': 'False positives are entered trades current logic dislikes. False negatives are skipped/near signals current logic might take.',
    }


def write_false_positive_negative_tables(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_false_positive_negative_tables(day)
    path = os.path.join(OUT_DIR, f'false_positive_negative_{day}.json')
    return _write_json(path, payload), payload


def build_daily_review_gate(day: Optional[str] = None) -> dict:
    day = day or _now_ct().date().isoformat()
    tape = _tape(day)
    feed = _read_json(os.path.join(OUT_DIR, f'feed_health_score_{day}.json'), {}) or build_feed_health_score(day)
    execution = _read_json(os.path.join(OUT_DIR, f'broker_execution_score_{day}.json'), {}) or build_broker_execution_score(day)
    split = _read_json(os.path.join(OUT_DIR, f'strategy_operations_split_{day}.json'), {}) or build_strategy_operations_split(day)
    replay_conf = _read_json(os.path.join(OUT_DIR, f'replay_confidence_{day}.json'), {}) or build_replay_confidence(day)
    alerts = _read_json(os.path.join(OUT_DIR, f'trade_alerts_{day}.json'), {}) or build_trade_alerts(day)
    checks = []
    ops_trades = ((split.get('operations') or {}).get('trades') or 0)
    checks.append({'name': 'minimum_trade_sample', 'ok': len(tape) >= 4, 'detail': len(tape)})
    checks.append({'name': 'feed_usable_or_not_started', 'ok': feed.get('score') is None or (feed.get('score') or 0) >= 75, 'detail': feed.get('score')})
    checks.append({'name': 'broker_execution_usable', 'ok': (execution.get('score') or 0) >= 75, 'detail': execution.get('score')})
    checks.append({'name': 'no_operations_contamination', 'ok': ops_trades == 0, 'detail': ops_trades})
    checks.append({
        'name': 'replay_confidence_usable',
        'ok': (
            replay_conf.get('verdict') == 'usable'
            and (len(tape) == 0 or replay_conf.get('usable_replay_count', 0) >= min(10, len(tape)))
        ),
        'detail': {
            'verdict': replay_conf.get('verdict'),
            'usable_replay_count': replay_conf.get('usable_replay_count'),
        },
    })
    critical_alerts = [a for a in alerts.get('alerts') or [] if a.get('level') == 'critical']
    checks.append({'name': 'no_critical_alerts', 'ok': not critical_alerts, 'detail': critical_alerts})
    passed = sum(1 for c in checks if c.get('ok'))
    if not tape:
        verdict = 'insufficient_data'
    elif passed == len(checks):
        verdict = 'safe_to_learn'
    elif ops_trades or critical_alerts:
        verdict = 'unsafe_to_learn_strategy'
    else:
        verdict = 'watch_only'
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'verdict': verdict,
        'checks_passed': passed,
        'checks_total': len(checks),
        'checks': checks,
        'conclusion_buckets': {
            'proven': [],
            'promising': [],
            'watch_only': ['Review all patterns without live-rule changes.'] if verdict in ('watch_only', 'insufficient_data') else [],
            'rejected': [],
            'insufficient_data': ['No strategy promotion from this day.'] if verdict != 'safe_to_learn' else [],
        },
        'deduction': 'This is the top-level referee: safe_to_learn means strategy conclusions can be discussed, not automatically implemented.',
    }


def write_daily_review_gate(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_daily_review_gate(day)
    path = os.path.join(OUT_DIR, f'daily_review_gate_{day}.json')
    return _write_json(path, payload), payload


def build_rule_lifecycle_dashboard(day: Optional[str] = None) -> dict:
    day = day or _now_ct().date().isoformat()
    try:
        from promotion_queue import update_queue
        queue = update_queue(day)
    except Exception as e:
        return {'day': day, 'created_at_ct': _now_ct().isoformat(timespec='seconds'), 'error': str(e)}
    buckets = {k: [] for k in ('proven', 'promising', 'watch_only', 'rejected', 'insufficient_data')}
    for item in (queue.get('items') or {}).values():
        lifecycle = item.get('lifecycle') or 'insufficient_data'
        ev = item.get('evidence') or {}
        buckets.setdefault(lifecycle, []).append({
            'key': item.get('key'),
            'kind': item.get('kind'),
            'name': item.get('name'),
            'status': item.get('status'),
            'seen_days': item.get('seen_days') or [],
            'affected_trades': ev.get('affected_trades'),
            'estimated_delta': ev.get('estimated_delta_vs_actual') or ev.get('pnl'),
            'winner_damage': ev.get('winner_damage'),
        })
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'counts': {k: len(v) for k, v in buckets.items()},
        'buckets': buckets,
        'deduction': 'Every candidate must live in one explicit lifecycle bucket before humans consider promotion.',
    }


def write_rule_lifecycle_dashboard(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_rule_lifecycle_dashboard(day)
    path = os.path.join(OUT_DIR, f'rule_lifecycle_dashboard_{day}.json')
    return _write_json(path, payload), payload


def build_change_impact_ledger(day: str) -> dict:
    path = os.path.join(OUT_DIR, 'change_impact_ledger.json')
    ledger = _read_json(path, {'created_at_ct': _now_ct().isoformat(timespec='seconds'), 'items': {}}) or {}
    items = ledger.setdefault('items', {})
    queue = _read_json(os.path.join(OUT_DIR, 'promotion_queue.json'), {}) or {}
    risk = _read_json(os.path.join(OUT_DIR, f'risk_summary_{day}.json'), {}) or {}
    for key, item in (queue.get('items') or {}).items():
        rec = items.setdefault(key, {
            'key': key,
            'kind': item.get('kind'),
            'name': item.get('name'),
            'first_seen': day,
            'observations': [],
            'manual_status': 'not_promoted',
        })
        rec['last_seen'] = day
        obs = {
            'day': day,
            'candidate_status': item.get('status'),
            'candidate_evidence': item.get('evidence') or {},
            'day_pnl': risk.get('pnl'),
            'day_win_rate': risk.get('win_rate'),
            'config_sha256': _sha256(os.path.join(HERE, 'trading_config.json')),
        }
        rec['observations'] = [o for o in rec.get('observations', []) if o.get('day') != day]
        rec['observations'].append(obs)
        rec['observations'] = rec['observations'][-10:]
    ledger['updated_at_ct'] = _now_ct().isoformat(timespec='seconds')
    ledger['note'] = 'Tracks expected vs observed candidate impact. Manual_status must be changed by a human; no automatic config promotion.'
    return ledger


def write_change_impact_ledger(day: str) -> tuple[str, dict]:
    payload = build_change_impact_ledger(day)
    path = os.path.join(OUT_DIR, 'change_impact_ledger.json')
    return _write_json(path, payload), payload


def build_why_no_trade_summary(day: Optional[str] = None, recent_minutes: int = 30) -> dict:
    day = day or _now_ct().date().isoformat()
    skipped = _skipped_rows(day)
    near = _near_signal_rows(day)
    audit = _audit_rows(day)
    gate_timeline = _gate_timeline_rows(day)
    no_signal = _no_signal_snapshot_rows(day)
    recent_skipped = _since(skipped, recent_minutes)
    recent_near = _since(near, recent_minutes)
    recent_audit = _since(audit, recent_minutes)
    recent_gate = _since(gate_timeline, recent_minutes)
    recent_no_signal = _since(no_signal, recent_minutes)
    reason_counts = Counter(str(r.get('reason') or r.get('event') or 'unknown') for r in skipped)
    near_counts = Counter(str(r.get('reason') or 'unknown') for r in near)
    gate_blockers = Counter(
        str(blocker)
        for row in gate_timeline
        for blocker in (row.get('blocked_by') or [])
    )
    no_signal_blockers = Counter(
        str(blocker)
        for row in no_signal
        for blocker in (row.get('blocked_by') or [])
    )
    recent_reason_counts = Counter(str(r.get('reason') or r.get('event') or 'unknown') for r in recent_skipped)
    recent_gate_blockers = Counter(
        str(blocker)
        for row in recent_gate
        for blocker in (row.get('blocked_by') or [])
    )
    dual_side_counts = Counter(
        str((row.get('shadow_dual_side_score') or {}).get('chosen_side_by_shadow') or 'unknown')
        for row in gate_timeline + no_signal
        if row.get('shadow_dual_side_score')
    )
    thin_side_edge = [
        {
            'ticker': row.get('ticker'),
            'stage': row.get('stage'),
            'reason': row.get('reason'),
            'side_gap': (row.get('shadow_dual_side_score') or {}).get('side_gap'),
            'chosen_side': (row.get('shadow_dual_side_score') or {}).get('chosen_side_by_shadow'),
            'blocked_by': row.get('blocked_by') or [],
        }
        for row in gate_timeline + no_signal
        if (row.get('shadow_dual_side_score') or {}).get('side_gap') is not None
        and float((row.get('shadow_dual_side_score') or {}).get('side_gap') or 0) < 1.0
    ][:20]
    entry_events = Counter(str(r.get('event') or 'unknown') for r in audit if str(r.get('event') or '').startswith('entry_'))
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'recent_minutes': recent_minutes,
        'total': {
            'skipped_signals': len(skipped),
            'near_signals': len(near),
            'audit_entry_events': sum(entry_events.values()),
            'gate_timeline_rows': len(gate_timeline),
            'no_signal_snapshots': len(no_signal),
        },
        'recent': {
            'skipped_signals': len(recent_skipped),
            'near_signals': len(recent_near),
            'audit_events': len(recent_audit),
            'gate_timeline_rows': len(recent_gate),
            'no_signal_snapshots': len(recent_no_signal),
            'top_skipped_reasons': [{'reason': k, 'count': v} for k, v in recent_reason_counts.most_common(12)],
            'top_gate_blockers': [{'blocker': k, 'count': v} for k, v in recent_gate_blockers.most_common(12)],
        },
        'all_day_top_skipped_reasons': [{'reason': k, 'count': v} for k, v in reason_counts.most_common(20)],
        'all_day_top_near_signal_reasons': [{'reason': k, 'count': v} for k, v in near_counts.most_common(20)],
        'all_day_top_gate_blockers': [{'blocker': k, 'count': v} for k, v in gate_blockers.most_common(20)],
        'all_day_top_no_signal_blockers': [{'blocker': k, 'count': v} for k, v in no_signal_blockers.most_common(20)],
        'dual_side_shadow_chosen_counts': dict(dual_side_counts),
        'thin_side_edge_rows': thin_side_edge,
        'entry_event_counts': dict(entry_events),
        'deduction': (
            'If no trades are opening, check recent.top_gate_blockers first, then skipped and near-signal reasons. '
            'Gate timeline/no-signal snapshots explain quiet periods even when nothing was close to firing.'
        ),
    }


def write_why_no_trade_summary(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_why_no_trade_summary(day)
    path = os.path.join(OUT_DIR, f'why_no_trade_{day}.json')
    return _write_json(path, payload), payload


def build_market_open_monitor(day: Optional[str] = None) -> dict:
    day = day or _now_ct().date().isoformat()
    open_ts = _session_open_ts(day)
    first15_end = open_ts + 15 * 60
    status, status_error = _status()
    audits = _audit_rows(day)
    skipped = _skipped_rows(day)
    near = _near_signal_rows(day)
    heartbeats = _heartbeat_rows(day)
    first_audit = [r for r in audits if open_ts <= (_ts(r) or 0) <= first15_end]
    first_skipped = [r for r in skipped if open_ts <= (_ts(r) or 0) <= first15_end]
    first_near = [r for r in near if open_ts <= (_ts(r) or 0) <= first15_end]
    first_entries = [r for r in first_audit if r.get('event') in ('entry_committed', 'entry_submitted', 'entry_reserved')]
    last_hb = heartbeats[-1] if heartbeats else {}
    freshness = last_hb.get('ticker_freshness') or {}
    stale = {
        sym: row for sym, row in freshness.items()
        if row.get('age_sec') is not None and float(row.get('age_sec')) > (20 if sym == 'BTC/USD' else 10)
    }
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'window_ct': '08:30-08:45',
        'status_reachable': status_error is None,
        'status_error': status_error,
        'current': {
            'running': status.get('running'),
            'in_window': status.get('in_window'),
            'positions': status.get('positions') or {},
            'pending_entries': status.get('pending_entries') or {},
            'wins': status.get('wins'),
            'losses': status.get('losses'),
            'total_pnl': status.get('total_pnl'),
            'broker_exposure_block': status.get('broker_exposure_block'),
            'broker_lifecycle_block': status.get('broker_lifecycle_block'),
        },
        'feed_freshness': freshness,
        'stale_feeds': stale,
        'first_15_min': {
            'entry_events': Counter(str(r.get('event') or 'unknown') for r in first_entries),
            'skipped_reasons': Counter(str(r.get('reason') or 'unknown') for r in first_skipped),
            'near_signal_reasons': Counter(str(r.get('reason') or 'unknown') for r in first_near),
            'sample_entries': first_entries[:20],
            'sample_skips': first_skipped[:20],
            'sample_near_signals': first_near[:20],
        },
        'why_no_trade': build_why_no_trade_summary(day, recent_minutes=15),
        'review_question': 'At the open, verify feed freshness first, then whether no-trade is caused by score, BTC conflict, spread, stale quote, cooldown, or risk-off.',
    }


def write_market_open_monitor(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_market_open_monitor(day)
    path = os.path.join(OUT_DIR, f'market_open_monitor_{day}.json')
    return _write_json(path, payload), payload


def build_strategy_operations_split(day: str) -> dict:
    tape = _tape(day)
    op_reasons = {'manual_stop', 'session_end', 'external_broker_exit'}
    rows = {
        'strategy': [],
        'operations': [],
        'data_or_execution': [],
    }
    for t in tape:
        reason = str(t.get('reason') or '')
        primary = str(t.get('primary_loss_cause') or '')
        target = 'strategy'
        if reason in op_reasons or reason.startswith('external_'):
            target = 'operations'
        elif any(token in primary for token in ('execution', 'spread', 'stale', 'fill')):
            target = 'data_or_execution'
        rows[target].append(t)
    def summarize(items: list[dict]) -> dict:
        pnl = round(sum(float(t.get('pnl') or 0) for t in items), 2)
        wins = sum(1 for t in items if (t.get('pnl') or 0) > 0)
        losses = sum(1 for t in items if (t.get('pnl') or 0) < 0)
        return {
            'trades': len(items),
            'wins': wins,
            'losses': losses,
            'win_rate': _safe_pct(wins, len(items)),
            'pnl': pnl,
            'top_reasons': [{'reason': k, 'count': v} for k, v in Counter(str(t.get('reason') or 'unknown') for t in items).most_common(10)],
        }
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'strategy': summarize(rows['strategy']),
        'operations': summarize(rows['operations']),
        'data_or_execution': summarize(rows['data_or_execution']),
        'note': 'Use this split before changing strategy rules; operational and execution losses should not contaminate entry-edge decisions.',
    }


def _available_report_days(end_day: str, lookback: int) -> list[str]:
    days = []
    if os.path.isdir(OUT_DIR):
        for name in sorted(os.listdir(OUT_DIR)):
            if name.startswith('postmortem_') and name.endswith('.json'):
                d = name[len('postmortem_'):-len('.json')]
                if d <= end_day:
                    days.append(d)
    return days[-lookback:]


def build_multi_day_scorecard(day: str, lookback: int = 10) -> dict:
    days = _available_report_days(day, lookback)
    dims = {
        'ticker': defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0}),
        'side': defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0}),
        'setup': defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0}),
        'btc_regime': defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0}),
        'hour_ct': defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0}),
        'entry_score_bucket': defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0}),
        'exit_reason': defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0}),
    }

    def add(dim: str, key: str, pnl: float):
        row = dims[dim][key or 'unknown']
        row['trades'] += 1
        row['wins'] += 1 if pnl > 0 else 0
        row['losses'] += 1 if pnl < 0 else 0
        row['pnl'] += pnl

    for d in days:
        for t in _tape(d):
            pnl = float(t.get('pnl') or 0)
            btc = t.get('btc') or t.get('btc_indicators') or {}
            score = t.get('entry_score') if t.get('entry_score') is not None else t.get('score')
            try:
                score_bucket = f"{int(abs(float(score)) // 2 * 2)}-{int(abs(float(score)) // 2 * 2 + 1)}"
            except Exception:
                score_bucket = 'unknown'
            hour = str(t.get('time') or 'unknown')[:2]
            add('ticker', str(t.get('ticker') or 'unknown'), pnl)
            add('side', str(t.get('side') or 'unknown'), pnl)
            add('setup', str(t.get('setup_type') or t.get('setup') or 'unknown'), pnl)
            add('btc_regime', str(btc.get('regime_detail') or btc.get('regime') or 'unknown'), pnl)
            add('hour_ct', hour, pnl)
            add('entry_score_bucket', score_bucket, pnl)
            add('exit_reason', str(t.get('reason') or 'unknown'), pnl)

    def finalize(rows: dict) -> list[dict]:
        out = []
        for key, row in rows.items():
            trades = row['trades']
            out.append({
                'key': key,
                'trades': trades,
                'wins': row['wins'],
                'losses': row['losses'],
                'win_rate': _safe_pct(row['wins'], trades),
                'pnl': round(row['pnl'], 2),
                'avg_pnl': round(row['pnl'] / trades, 2) if trades else None,
            })
        return sorted(out, key=lambda r: (r['pnl'], -r['trades']))

    return {
        'day': day,
        'lookback': lookback,
        'days': days,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'scorecards': {name: finalize(rows) for name, rows in dims.items()},
        'note': 'Multi-day edge scorecard; use for review, not automatic promotion.',
    }


def write_multi_day_scorecard(day: str, lookback: int = 10) -> tuple[str, dict]:
    payload = build_multi_day_scorecard(day, lookback=lookback)
    path = os.path.join(OUT_DIR, f'multi_day_scorecard_{day}_{lookback}d.json')
    return _write_json(path, payload), payload


def build_market_regime_day_label(day: str) -> dict:
    tape = _tape(day)
    btc_counts = Counter()
    sides = Counter()
    rel_vals = []
    for t in tape:
        btc = t.get('btc') or t.get('btc_indicators') or {}
        btc_counts[str(btc.get('regime_detail') or btc.get('regime') or 'unknown')] += 1
        sides[str(t.get('side') or 'unknown')] += 1
        try:
            rel = t.get('rel_strength_60s')
            if rel is None:
                rel = (t.get('btc_context') or {}).get('stock_minus_btc_implied_60s')
            if rel is not None:
                rel_vals.append(float(rel))
        except Exception:
            pass
    dominant_btc = btc_counts.most_common(1)[0][0] if btc_counts else 'no_trades'
    avg_rel = round(sum(rel_vals) / len(rel_vals), 4) if rel_vals else None
    label = 'no_trade_day'
    if tape:
        if 'chop' in dominant_btc or dominant_btc in ('neutral', 'mixed', 'unknown'):
            label = 'btc_chop_day'
        elif 'bear' in dominant_btc or 'down' in dominant_btc:
            label = 'btc_trend_down_day'
        elif 'bull' in dominant_btc or 'up' in dominant_btc:
            label = 'btc_trend_up_day'
        if avg_rel is not None and abs(avg_rel) >= 0.20:
            label += '_miner_divergence'
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'label': label,
        'dominant_btc_regime': dominant_btc,
        'btc_regime_counts': dict(btc_counts),
        'side_counts': dict(sides),
        'avg_stock_minus_btc_implied_60s': avg_rel,
        'note': 'Approximate day label from captured trade snapshots; richer labels improve as live monitor data accumulates.',
    }


def write_market_regime_day_label(day: str) -> tuple[str, dict]:
    payload = build_market_regime_day_label(day)
    path = os.path.join(OUT_DIR, f'market_regime_day_{day}.json')
    return _write_json(path, payload), payload


def build_edge_quality_activity(day: str, lookback: int = 10) -> dict:
    scorecard = build_multi_day_scorecard(day, lookback=lookback)
    setup_rows = scorecard.get('scorecards', {}).get('setup') or []
    total_trades = sum(r.get('trades') or 0 for r in setup_rows)
    profitable = [r for r in setup_rows if (r.get('pnl') or 0) > 0]
    weak = [r for r in setup_rows if (r.get('pnl') or 0) < 0]
    return {
        'day': day,
        'lookback': lookback,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'activity': {
            'days': scorecard.get('days'),
            'total_trades': total_trades,
            'avg_trades_per_day': round(total_trades / max(1, len(scorecard.get('days') or [])), 2),
        },
        'quality': {
            'profitable_setup_count': len(profitable),
            'weak_setup_count': len(weak),
            'best_setups': sorted(profitable, key=lambda r: r.get('pnl') or 0, reverse=True)[:5],
            'worst_setups': weak[:5],
        },
        'deduction': 'If filters reduce activity, quality must improve enough to preserve or raise total P&L.',
    }


def write_edge_quality_activity(day: str, lookback: int = 10) -> tuple[str, dict]:
    payload = build_edge_quality_activity(day, lookback=lookback)
    path = os.path.join(OUT_DIR, f'edge_quality_activity_{day}_{lookback}d.json')
    return _write_json(path, payload), payload


def build_regime_specific_scorecards(day: Optional[str] = None, lookback: int = 10) -> dict:
    day = day or _now_ct().date().isoformat()
    days = _available_report_days(day, lookback)
    dims = {
        'btc_regime': defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0, 'largest_loss': 0.0}),
        'market_weather': defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0, 'largest_loss': 0.0}),
        'ticker_side': defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0, 'largest_loss': 0.0}),
        'time_window': defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0, 'largest_loss': 0.0}),
        'setup_regime': defaultdict(lambda: {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0, 'largest_loss': 0.0}),
    }

    def bucket_time(t: dict) -> str:
        ts = t.get('opened_at') or t.get('closed_at')
        try:
            dt = datetime.fromtimestamp(float(ts), CT)
            mins = dt.hour * 60 + dt.minute
            if mins < 9 * 60 + 30:
                return 'first_hour'
            if mins < 12 * 60:
                return 'late_morning'
            if mins < 14 * 60:
                return 'midday'
            return 'power_hour'
        except Exception:
            return 'unknown'

    def add(dim: str, key: str, pnl: float):
        row = dims[dim][key or 'unknown']
        row['trades'] += 1
        row['wins'] += 1 if pnl > 0 else 0
        row['losses'] += 1 if pnl < 0 else 0
        row['pnl'] += pnl
        row['largest_loss'] = min(row['largest_loss'], pnl)

    for d in days:
        market = _read_json(os.path.join(OUT_DIR, f'market_context_scoreboard_{d}.json'), {}) or {}
        weather = str(market.get('session_weather') or 'unknown')
        for t in _tape(d):
            pnl = float(t.get('pnl') or 0)
            btc = t.get('btc') or t.get('btc_indicators') or t.get('btc_context') or {}
            btc_key = str(btc.get('regime_detail') or btc.get('regime') or btc.get('stack') or 'unknown')
            setup = str(t.get('setup_type') or t.get('setup') or (t.get('entry_thesis') or {}).get('setup_type') or 'unknown')
            add('btc_regime', btc_key, pnl)
            add('market_weather', weather, pnl)
            add('ticker_side', f"{t.get('ticker') or 'unknown'}:{t.get('side') or 'unknown'}", pnl)
            add('time_window', bucket_time(t), pnl)
            add('setup_regime', f'{setup}:{btc_key}', pnl)

    def finalize(rows: dict) -> list[dict]:
        out = []
        for key, row in rows.items():
            trades = row['trades']
            out.append({
                'key': key,
                'trades': trades,
                'wins': row['wins'],
                'losses': row['losses'],
                'win_rate': _safe_pct(row['wins'], trades),
                'pnl': round(row['pnl'], 2),
                'avg_pnl': round(row['pnl'] / trades, 2) if trades else None,
                'largest_loss': round(row['largest_loss'], 2),
            })
        return sorted(out, key=lambda r: (r['pnl'], -r['trades']))

    return {
        'day': day,
        'lookback': lookback,
        'days': days,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'scorecards': {name: finalize(rows) for name, rows in dims.items()},
        'deduction': 'Shows where the bot is actually elite versus regime-dependent. Use this before broad rule changes.',
    }


def write_regime_specific_scorecards(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_regime_specific_scorecards(day)
    path = os.path.join(OUT_DIR, f'regime_specific_scorecards_{day}.json')
    return _write_json(path, payload), payload


def _best_forward_pct(row: dict) -> Optional[float]:
    fwd = row.get('fwd') or row.get('forward_returns') or {}
    vals = []
    if isinstance(fwd, dict):
        for key in ('5m', '15m', '30m', '+5m', '+15m', '+30m'):
            value = fwd.get(key)
            try:
                if value is not None:
                    vals.append(float(value))
            except Exception:
                pass
    for key in ('fwd_5m_pct', 'fwd_15m_pct', 'fwd_30m_pct', 'ret_5m', 'ret_15m', 'ret_30m'):
        try:
            if row.get(key) is not None:
                vals.append(float(row.get(key)))
        except Exception:
            pass
    return max(vals) if vals else None


def build_near_miss_winners(day: Optional[str] = None) -> dict:
    day = day or _now_ct().date().isoformat()
    skipped = _skipped_rows(day)
    near = _near_signal_rows(day)
    rows = []
    for source, items in (('skipped_signal', skipped), ('near_signal', near)):
        for r in items:
            best = _best_forward_pct(r)
            if best is None or best < 0.35:
                continue
            rows.append({
                'source': source,
                'ticker': r.get('ticker'),
                'side': r.get('side'),
                'reason': r.get('reason'),
                'score': r.get('score'),
                'setup_type': r.get('setup_type'),
                'created_at': r.get('created_at'),
                'best_forward_pct': round(best, 4),
                'btc_context': r.get('btc_context') or r.get('btc') or {},
            })
    reason_counts = Counter(str(r.get('reason') or 'unknown') for r in rows)
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'threshold_best_forward_pct': 0.35,
        'count': len(rows),
        'by_reason': [{'reason': k, 'count': v} for k, v in reason_counts.most_common(20)],
        'rows': sorted(rows, key=lambda r: r.get('best_forward_pct') or 0, reverse=True)[:200],
        'deduction': 'Skipped or near signals that later moved enough to matter. This helps detect over-filtering after safety hardening.',
    }


def write_near_miss_winners(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_near_miss_winners(day)
    path = os.path.join(OUT_DIR, f'near_miss_winners_{day}.json')
    return _write_json(path, payload), payload


def build_exit_quality_score(day: Optional[str] = None) -> dict:
    day = day or _now_ct().date().isoformat()
    rows = []
    counts = Counter()
    for t in _tape(day):
        pnl = float(t.get('pnl') or 0)
        path = t.get('path') or t.get('_path') or {}
        mfe = path.get('mfe_pct')
        mae = path.get('mae_pct')
        exit_ctx = t.get('exit_decision_context') or {}
        label = 'unknown'
        try:
            mfe_f = float(mfe) if mfe is not None else None
            mae_f = float(mae) if mae is not None else None
        except Exception:
            mfe_f = mae_f = None
        if pnl > 0:
            if mfe_f is not None and mfe_f > 0 and pnl >= 0:
                label = 'winner_exit_acceptable'
            else:
                label = 'winner_quality_unclear'
        else:
            if exit_ctx.get('kind') in ('short_conviction_decay', 'long_conviction_decay', 'hard_adverse_exit', 'failed_followthrough'):
                label = 'loss_cut_by_safety_logic'
            elif mfe_f is not None and mfe_f > 0.20:
                label = 'gave_back_positive_excursion'
            elif mae_f is not None and mae_f < -0.45:
                label = 'allowed_large_adverse_excursion'
            else:
                label = 'loser_exit_unclear'
        counts[label] += 1
        rows.append({
            'ticker': t.get('ticker'),
            'side': t.get('side'),
            'pnl': t.get('pnl'),
            'reason': t.get('reason'),
            'label': label,
            'mfe_pct': mfe,
            'mae_pct': mae,
            'duration_sec': t.get('duration_sec'),
            'exit_decision_context': exit_ctx,
        })
    score = 100
    score -= min(40, counts.get('allowed_large_adverse_excursion', 0) * 15)
    score -= min(25, counts.get('gave_back_positive_excursion', 0) * 10)
    score = max(0, score)
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'score': score,
        'counts': dict(counts),
        'rows': rows,
        'deduction': 'Exit quality separates bad entries from late exits. Large adverse excursion and MFE giveback are the first failure modes to inspect.',
    }


def write_exit_quality_score(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_exit_quality_score(day)
    path = os.path.join(OUT_DIR, f'exit_quality_score_{day}.json')
    return _write_json(path, payload), payload


def build_order_latency_summary(day: str) -> dict:
    audits = _audit_rows(day)
    by_symbol = defaultdict(list)
    for r in audits:
        if r.get('event') in ('entry_reserved', 'entry_submitted', 'entry_committed', 'entry_filled'):
            by_symbol[(r.get('symbol') or 'unknown')].append(r)
    rows = []
    for symbol, events in by_symbol.items():
        events = sorted(events, key=lambda r: r.get('ts') or 0)
        pending = {}
        for r in events:
            event = r.get('event')
            ts = r.get('ts')
            data = r.get('data') or {}
            if event == 'entry_reserved':
                pending = {'reserved_ts': ts, 'symbol': symbol, 'side': data.get('side')}
            elif event == 'entry_submitted' and pending:
                pending['submitted_ts'] = ts
                pending['order_id'] = data.get('order_id')
                pending['client_order_id'] = data.get('client_order_id')
                pending['submitted_status'] = data.get('status')
            elif event == 'entry_committed' and pending:
                pending['committed_ts'] = ts
                pending['committed_trade_id'] = data.get('trade_id') or (data.get('decision_audit') or {}).get('trade_id')
                rows.append(dict(pending))
                pending = {}
        fill_events = [
            r for r in events
            if r.get('event') == 'entry_filled'
        ]
        for fill in fill_events:
            data = fill.get('data') or {}
            oid = data.get('order_id')
            candidates = [
                row for row in rows
                if row.get('symbol') == symbol
                and (not oid or row.get('order_id') == oid)
                and (not row.get('filled_ts'))
                and (not row.get('committed_ts') or fill.get('ts') >= row.get('committed_ts'))
            ]
            if not candidates:
                candidates = [
                    row for row in rows
                    if row.get('symbol') == symbol
                    and (not row.get('filled_ts'))
                ]
            if not candidates:
                continue
            row = sorted(candidates, key=lambda x: abs((fill.get('ts') or 0) - (x.get('committed_ts') or x.get('reserved_ts') or 0)))[0]
            row['filled_ts'] = fill.get('ts')
            row['fill_price'] = data.get('fill_price')
            row['slippage_pct'] = data.get('slippage_pct')
            row['fill_status'] = data.get('status')
    for row in rows:
        row['reserve_to_submit_sec'] = (
            round(row['submitted_ts'] - row['reserved_ts'], 3)
            if row.get('submitted_ts') and row.get('reserved_ts') else None
        )
        row['submit_to_commit_sec'] = (
            round(row['committed_ts'] - row['submitted_ts'], 3)
            if row.get('committed_ts') and row.get('submitted_ts') else None
        )
        row['reserve_to_commit_sec'] = (
            round(row['committed_ts'] - row['reserved_ts'], 3)
            if row.get('committed_ts') and row.get('reserved_ts') else None
        )
        row['commit_to_fill_sec'] = (
            round(row['filled_ts'] - row['committed_ts'], 3)
            if row.get('filled_ts') and row.get('committed_ts') else None
        )
        row['reserve_to_fill_sec'] = (
            round(row['filled_ts'] - row['reserved_ts'], 3)
            if row.get('filled_ts') and row.get('reserved_ts') else None
        )
    vals = [r['reserve_to_commit_sec'] for r in rows if r.get('reserve_to_commit_sec') is not None]
    fill_vals = [r['reserve_to_fill_sec'] for r in rows if r.get('reserve_to_fill_sec') is not None]
    slippage_vals = [float(r.get('slippage_pct')) for r in rows if r.get('slippage_pct') is not None]
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'entries': len(rows),
        'filled_entries': len(fill_vals),
        'avg_reserve_to_commit_sec': round(sum(vals) / len(vals), 3) if vals else None,
        'max_reserve_to_commit_sec': max(vals, default=None),
        'avg_reserve_to_fill_sec': round(sum(fill_vals) / len(fill_vals), 3) if fill_vals else None,
        'max_reserve_to_fill_sec': max(fill_vals, default=None),
        'avg_entry_slippage_pct': round(sum(slippage_vals) / len(slippage_vals), 4) if slippage_vals else None,
        'max_entry_slippage_pct': max(slippage_vals, default=None),
        'rows': rows[:100],
        'note': 'Uses audit lifecycle events. Separates signal reserve, broker submit, local commit, and broker fill timing.',
    }


def write_order_latency_summary(day: str) -> tuple[str, dict]:
    payload = build_order_latency_summary(day)
    path = os.path.join(OUT_DIR, f'order_latency_{day}.json')
    return _write_json(path, payload), payload


def _score_verdict(score: int) -> str:
    if score >= 90:
        return 'excellent'
    if score >= 75:
        return 'good_watch_minor_friction'
    if score >= 60:
        return 'degraded_needs_review'
    return 'poor_block_strategy_conclusions'


def build_feed_health_score(day: Optional[str] = None) -> dict:
    day = day or _now_ct().date().isoformat()
    heartbeats = _heartbeat_rows(day)
    if not heartbeats:
        return {
            'day': day,
            'created_at_ct': _now_ct().isoformat(timespec='seconds'),
            'score': None,
            'verdict': 'not_started_no_heartbeats_yet',
            'heartbeat_count': 0,
            'missing_symbols': ['BTC/USD', 'CLSK', 'MARA', 'RIOT'],
            'symbols': [],
            'deduction': 'No feed samples exist yet for this session. Score starts once live heartbeats are collected.',
        }
    by_symbol = defaultdict(lambda: {
        'samples': 0,
        'fresh': 0,
        'stale': 0,
        'max_age_sec': None,
        'last_age_sec': None,
        'last_seen_ct': None,
    })
    for hb in heartbeats:
        ts = _ts(hb)
        freshness = hb.get('ticker_freshness') or {}
        for sym, row in freshness.items():
            age = row.get('age_sec')
            if age is None:
                continue
            try:
                age_f = float(age)
            except Exception:
                continue
            threshold = 20 if sym == 'BTC/USD' else 10
            rec = by_symbol[sym]
            rec['samples'] += 1
            rec['fresh'] += 1 if age_f <= threshold else 0
            rec['stale'] += 1 if age_f > threshold else 0
            rec['max_age_sec'] = max(rec['max_age_sec'] or 0, age_f)
            rec['last_age_sec'] = age_f
            rec['last_seen_ct'] = (
                datetime.fromtimestamp(ts, CT).isoformat(timespec='seconds')
                if ts else None
            )
    rows = []
    penalty = 0
    for sym, rec in sorted(by_symbol.items()):
        stale_pct = _safe_pct(rec['stale'], rec['samples']) or 0
        symbol_penalty = min(35, int(stale_pct // 2))
        if sym == 'BTC/USD':
            symbol_penalty = int(symbol_penalty * 1.5)
        penalty += symbol_penalty
        rows.append({
            'symbol': sym,
            'samples': rec['samples'],
            'fresh': rec['fresh'],
            'stale': rec['stale'],
            'fresh_pct': _safe_pct(rec['fresh'], rec['samples']),
            'stale_pct': stale_pct,
            'max_age_sec': round(rec['max_age_sec'], 3) if rec['max_age_sec'] is not None else None,
            'last_age_sec': round(rec['last_age_sec'], 3) if rec['last_age_sec'] is not None else None,
            'last_seen_ct': rec['last_seen_ct'],
            'threshold_sec': 20 if sym == 'BTC/USD' else 10,
            'score_penalty': symbol_penalty,
        })
    expected = {'BTC/USD', 'CLSK', 'MARA', 'RIOT'}
    missing = sorted(expected - set(by_symbol))
    penalty += len(missing) * 25
    score = max(0, min(100, 100 - penalty))
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'score': score,
        'verdict': _score_verdict(score),
        'heartbeat_count': len(heartbeats),
        'missing_symbols': missing,
        'symbols': rows,
        'deduction': (
            'Feed health gates review confidence. Stale BTC or miner feeds can explain wrong-side entries '
            'and should block strong strategy conclusions for that period.'
        ),
    }


def write_feed_health_score(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_feed_health_score(day)
    path = os.path.join(OUT_DIR, f'feed_health_score_{day}.json')
    return _write_json(path, payload), payload


def build_broker_execution_score(day: Optional[str] = None) -> dict:
    day = day or _now_ct().date().isoformat()
    latency = build_order_latency_summary(day)
    tape = _tape(day)
    vals = [r.get('reserve_to_commit_sec') for r in latency.get('rows') or [] if r.get('reserve_to_commit_sec') is not None]
    slippage = []
    missing_fills = 0
    by_symbol = defaultdict(lambda: {'trades': 0, 'slippage_values': []})
    for t in tape:
        sym = str(t.get('ticker') or 'unknown')
        by_symbol[sym]['trades'] += 1
        slip = t.get('entry_slippage_pct')
        if slip is not None:
            try:
                slippage.append(abs(float(slip)))
                by_symbol[sym]['slippage_values'].append(abs(float(slip)))
            except Exception:
                pass
        if not t.get('entry_fill') and not t.get('broker_entry_fill'):
            missing_fills += 1
        if (t.get('reason') or '').startswith('broker_') and not t.get('exit_fill'):
            missing_fills += 1
    avg_latency = round(sum(vals) / len(vals), 3) if vals else None
    max_latency = max(vals, default=None)
    avg_slip = round(sum(slippage) / len(slippage), 4) if slippage else None
    max_slip = round(max(slippage), 4) if slippage else None
    penalty = 0
    if avg_latency is not None and avg_latency > 2:
        penalty += min(25, int((avg_latency - 2) * 8))
    if max_latency is not None and max_latency > 5:
        penalty += min(20, int((max_latency - 5) * 4))
    if avg_slip is not None and avg_slip > 0.08:
        penalty += min(20, int((avg_slip - 0.08) * 100))
    penalty += min(30, missing_fills * 5)
    score = max(0, min(100, 100 - penalty))
    symbol_rows = []
    for sym, row in sorted(by_symbol.items()):
        slips = row['slippage_values']
        symbol_rows.append({
            'symbol': sym,
            'trades': row['trades'],
            'avg_abs_entry_slippage_pct': round(sum(slips) / len(slips), 4) if slips else None,
            'max_abs_entry_slippage_pct': round(max(slips), 4) if slips else None,
        })
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'score': score,
        'verdict': _score_verdict(score),
        'entries': latency.get('entries'),
        'avg_reserve_to_commit_sec': avg_latency,
        'max_reserve_to_commit_sec': max_latency,
        'avg_abs_entry_slippage_pct': avg_slip,
        'max_abs_entry_slippage_pct': max_slip,
        'missing_fill_evidence_count': missing_fills,
        'by_symbol': symbol_rows,
        'latency_file': os.path.join(OUT_DIR, f'order_latency_{day}.json'),
        'deduction': 'Execution score separates broker/API friction from strategy quality before changing rules.',
    }


def write_broker_execution_score(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_broker_execution_score(day)
    path = os.path.join(OUT_DIR, f'broker_execution_score_{day}.json')
    return _write_json(path, payload), payload


def build_era_scorecard(day: Optional[str] = None, lookback: int = 30) -> dict:
    day = day or _now_ct().date().isoformat()
    days = _available_report_days(day, lookback)
    rows = defaultdict(lambda: {'days': set(), 'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0, 'largest_loss': 0.0})
    for d in days:
        era = era_for_day(d)
        tape = _tape(d)
        rec = rows[era]
        rec['days'].add(d)
        for t in tape:
            pnl = float(t.get('pnl') or 0)
            rec['trades'] += 1
            rec['wins'] += 1 if pnl > 0 else 0
            rec['losses'] += 1 if pnl < 0 else 0
            rec['pnl'] += pnl
            rec['largest_loss'] = min(rec['largest_loss'], pnl)
    out = []
    for era, rec in rows.items():
        trades = rec['trades']
        out.append({
            'era': era,
            'days': sorted(rec['days']),
            'trades': trades,
            'wins': rec['wins'],
            'losses': rec['losses'],
            'win_rate': _safe_pct(rec['wins'], trades),
            'pnl': round(rec['pnl'], 2),
            'avg_pnl': round(rec['pnl'] / trades, 2) if trades else None,
            'largest_loss': round(rec['largest_loss'], 2),
        })
    return {
        'day': day,
        'current_era': era_for_day(day),
        'lookback': lookback,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'summary': sorted(out, key=lambda r: r['era']),
        'deduction': 'Evaluate post-2026-05-04 behavior separately from older bot behavior.',
    }


def write_era_scorecard(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_era_scorecard(day)
    path = os.path.join(OUT_DIR, f'era_scorecard_{day}.json')
    return _write_json(path, payload), payload


def build_monday_scorecard(day: Optional[str] = None, baseline_day: str = '2026-05-01') -> dict:
    day = day or _now_ct().date().isoformat()
    current = build_out_of_sample_scoreboard(day, baseline_day=baseline_day)
    feed = build_feed_health_score(day)
    execution = build_broker_execution_score(day)
    exit_quality = build_exit_quality_score(day)
    split = _read_json(os.path.join(OUT_DIR, f'strategy_operations_split_{day}.json'), {}) or build_strategy_operations_split(day)
    tape = _tape(day)
    exit_reasons = Counter(str(t.get('reason') or 'unknown') for t in tape)
    safety_exit_count = sum(
        count for reason, count in exit_reasons.items()
        if reason.startswith(('short_conviction_decay', 'long_conviction_decay', 'failed_followthrough', 'hard_adverse', 'btc_impulse_abort'))
    )
    delta = current.get('delta_vs_baseline') or {}
    ops_trades = ((split.get('operations') or {}).get('trades') or 0)
    checks = [
        {
            'name': 'largest_loss_improved_vs_baseline',
            'ok': (delta.get('largest_loss') is not None and delta.get('largest_loss') >= 0),
            'detail': delta.get('largest_loss'),
        },
        {
            'name': 'operations_contamination_absent',
            'ok': ops_trades == 0,
            'detail': ops_trades,
        },
        {
            'name': 'feed_health_good_enough',
            'ok': ((feed.get('score') or 0) >= 75) or (not tape and feed.get('score') is None),
            'detail': feed.get('score'),
        },
        {
            'name': 'broker_execution_good_enough',
            'ok': (execution.get('score') or 0) >= 75,
            'detail': execution.get('score'),
        },
        {
            'name': 'safety_exits_collected',
            'ok': safety_exit_count > 0 or len(tape) == 0,
            'detail': dict(exit_reasons),
        },
    ]
    passed = sum(1 for c in checks if c.get('ok'))
    verdict = 'too_early_no_trades' if not tape else ('monday_validated' if passed == len(checks) else 'watch_failures_before_changing_strategy')
    return {
        'day': day,
        'baseline_day': baseline_day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'verdict': verdict,
        'checks_passed': passed,
        'checks_total': len(checks),
        'checks': checks,
        'out_of_sample': current,
        'feed_health_score': feed.get('score'),
        'broker_execution_score': execution.get('score'),
        'deduction': 'This is the Monday go/no-go scorecard for whether the new bot behavior is actually better than 2026-05-01.',
    }


def write_monday_scorecard(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_monday_scorecard(day)
    path = os.path.join(OUT_DIR, f'monday_scorecard_{day}.json')
    return _write_json(path, payload), payload


def build_out_of_sample_boundary(day: str, label: str = 'vNext') -> dict:
    return {
        'day': day,
        'label': label,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'config_sha256': _sha256(os.path.join(HERE, 'trading_config.json')),
        'boundary_note': (
            f'{label} begins on {day}. Evaluate trades on/after this date separately '
            'from prior sessions to avoid mixing old behavior with new behavior.'
        ),
    }


def write_out_of_sample_boundary(day: str, label: str = 'vNext') -> tuple[str, dict]:
    payload = build_out_of_sample_boundary(day, label=label)
    path = os.path.join(OUT_DIR, f'out_of_sample_boundary_{day}_{label}.json')
    return _write_json(path, payload), payload


def write_strategy_operations_split(day: str) -> tuple[str, dict]:
    payload = build_strategy_operations_split(day)
    path = os.path.join(OUT_DIR, f'strategy_operations_split_{day}.json')
    return _write_json(path, payload), payload


def build_runtime_restart_alerts(day: Optional[str] = None) -> dict:
    day = day or _now_ct().date().isoformat()
    rows = _runtime_event_rows(day)
    boots = [r for r in rows if r.get('kind') == 'process_boot']
    alerts = []
    for row in boots:
        payload = row.get('payload') or {}
        downtime = int(payload.get('downtime_sec') or 0)
        if payload.get('unexpected_restart') or downtime >= 30:
            alerts.append({
                'created_at_ct': row.get('created_at_ct'),
                'previous_pid': payload.get('previous_pid'),
                'current_pid': payload.get('current_pid'),
                'downtime_sec': downtime,
                'previous_heartbeat_iso': payload.get('previous_heartbeat_iso'),
                'severity': 'critical' if downtime >= 180 else 'warning',
            })
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'boot_count': len(boots),
        'unexpected_restart_count': len(alerts),
        'alerts': alerts[-20:],
        'ok': len(alerts) == 0,
        'deduction': (
            'Unexpected app restarts are operational contamination. If alerts appear, '
            'review trade timing, feed gaps, broker reconciliation, and process logs before '
            'changing strategy rules.'
        ),
    }


def write_runtime_restart_alerts(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_runtime_restart_alerts(day)
    path = os.path.join(OUT_DIR, f'runtime_restart_alerts_{day}.json')
    return _write_json(path, payload), payload


def build_postmarket_artifact_validation(day: Optional[str] = None, mode: str = 'post_market') -> dict:
    day = day or _now_ct().date().isoformat()
    mode = mode or 'post_market'
    pre_market_expected = [
        os.path.join(OUT_DIR, f'feed_health_score_{day}.json'),
        os.path.join(OUT_DIR, f'broker_execution_score_{day}.json'),
        os.path.join(OUT_DIR, f'era_scorecard_{day}.json'),
        os.path.join(OUT_DIR, f'monday_scorecard_{day}.json'),
        os.path.join(OUT_DIR, f'ws_scalp_replay_{day}.json'),
        os.path.join(OUT_DIR, f'decision_audit_summary_{day}.json'),
        os.path.join(OUT_DIR, f'current_engine_regrade_{day}.json'),
        os.path.join(OUT_DIR, f'regime_specific_scorecards_{day}.json'),
        os.path.join(OUT_DIR, f'near_miss_winners_{day}.json'),
        os.path.join(OUT_DIR, f'exit_quality_score_{day}.json'),
        os.path.join(OUT_DIR, f'replay_confidence_{day}.json'),
        os.path.join(OUT_DIR, f'false_positive_negative_{day}.json'),
        os.path.join(OUT_DIR, f'daily_review_gate_{day}.json'),
        os.path.join(OUT_DIR, f'rule_lifecycle_dashboard_{day}.json'),
        os.path.join(OUT_DIR, f'NOW_STATUS_{day}.json'),
        os.path.join(OUT_DIR, 'NOW_STATUS.json'),
        os.path.join(OUT_DIR, f'why_no_trade_{day}.json'),
        os.path.join(OUT_DIR, f'market_open_monitor_{day}.json'),
        os.path.join(OUT_DIR, f'trade_alerts_{day}.json'),
        os.path.join(OUT_DIR, f'config_change_watch_{day}.json'),
        os.path.join(OUT_DIR, f'DAILY_REVIEW_START_HERE_{day}.json'),
        os.path.join(OUT_DIR, f'MONDAY_REVIEW_START_HERE_{day}.json'),
        os.path.join(OUT_DIR, f'multi_day_scorecard_{day}_10d.json'),
        os.path.join(OUT_DIR, f'market_regime_day_{day}.json'),
        os.path.join(OUT_DIR, f'edge_quality_activity_{day}_10d.json'),
        os.path.join(OUT_DIR, f'order_latency_{day}.json'),
        os.path.join(OUT_DIR, f'strategy_operations_split_{day}.json'),
        os.path.join(OUT_DIR, f'runtime_restart_alerts_{day}.json'),
        os.path.join(OUT_DIR, 'promotion_queue.json'),
        os.path.join(OUT_DIR, 'change_impact_ledger.json'),
        os.path.join(OUT_DIR, 'config_intent_ledger.json'),
    ]
    post_market_only = [
        os.path.join(OUT_DIR, f'postmortem_{day}.json'),
        os.path.join(OUT_DIR, f'postmortem_{day}.txt'),
        os.path.join(OUT_DIR, f'review_index_{day}.json'),
        os.path.join(OUT_DIR, f'loser_summary_{day}.json'),
        os.path.join(OUT_DIR, f'loser_clusters_{day}.json'),
        os.path.join(OUT_DIR, f'loser_archetypes_{day}.json'),
        os.path.join(OUT_DIR, f'entry_quality_tiers_{day}.json'),
        os.path.join(OUT_DIR, f'winner_damage_report_{day}.json'),
        os.path.join(OUT_DIR, f'clean_day_score_{day}.json'),
        os.path.join(OUT_DIR, f'no_trade_opportunity_grades_{day}.json'),
        os.path.join(OUT_DIR, f'thesis_failure_review_{day}.json'),
        os.path.join(OUT_DIR, f'loser_replay_snapshots_{day}.json'),
        os.path.join(OUT_DIR, f'per_symbol_personality_{day}_10d.json'),
        os.path.join(OUT_DIR, f'out_of_sample_scoreboard_{day}.json'),
        os.path.join(OUT_DIR, f'first_hour_review_{day}.json'),
        os.path.join(OUT_DIR, f'strategy_conclusion_gate_{day}.json'),
        os.path.join(OUT_DIR, f'market_context_scoreboard_{day}.json'),
        os.path.join(OUT_DIR, f'trade_thesis_timelines_{day}.json'),
        os.path.join(OUT_DIR, f'winner_quality_{day}.json'),
        os.path.join(OUT_DIR, f'rule_candidate_quarantine_{day}.json'),
        os.path.join(OUT_DIR, f'world_class_dashboard_{day}.json'),
        os.path.join(OUT_DIR, f'monday_review_packet_{day}.json'),
        os.path.join(OUT_DIR, 'live_step2_parity', f'live_step2_parity_{day}.json'),
        os.path.join(OUT_DIR, 'live_step2_parity', f'live_step2_parity_{day}.txt'),
        os.path.join(OUT_DIR, 'cache_layers', f'step2_cache_layers_{day}.json'),
        os.path.join(OUT_DIR, 'execution_replay_inputs', f'execution_replay_inputs_{day}.jsonl'),
        os.path.join(OUT_DIR, 'canonical_opportunities', f'canonical_opportunities_{day}.jsonl'),
        os.path.join(OUT_DIR, 'canonical_opportunities', f'canonical_opportunities_{day}.summary.json'),
        os.path.join(OUT_DIR, 'parity_diff_classifier', f'parity_diff_classifier_{day}.json'),
        os.path.join(OUT_DIR, 'market_data_integrity', f'market_data_integrity_{day}.json'),
        os.path.join(OUT_DIR, 'market_data_integrity', f'market_data_integrity_{day}.txt'),
        os.path.join(OUT_DIR, 'baseline_drift', f'baseline_drift_sentinel_{day}.json'),
        os.path.join(OUT_DIR, 'artifact_registry', f'artifact_registry_{day}.json'),
        os.path.join(OUT_DIR, 'parity_sentinel', f'parity_sentinel_{day}.json'),
        os.path.join(OUT_DIR, 'step2_decision_parity', f'step2_decision_parity_{day}.jsonl'),
        os.path.join(OUT_DIR, 'step2_decision_parity', f'step2_decision_parity_{day}.summary.json'),
        output_path('data_cache', 'incremental_market_store', day, 'manifest.json'),
        _compiled_step2_paths(day)['compiled_manifest'],
        _compiled_step2_paths(day)['compiled_chunk_manifest'],
        _compiled_step2_paths(day)['step2_score'],
        _compiled_step2_paths(day)['step2_rebuild_plan'],
        _compiled_step2_paths(day)['unified_step2_current_trace'],
        _compiled_step2_paths(day)['unified_step2_current_trace_summary'],
        _compiled_step2_paths(day)['unified_live_signal_parity'],
        _compiled_step2_paths(day)['unified_live_signal_parity_summary'],
        _compiled_step2_paths(day)['golden_parity_suite'],
        _compiled_step2_paths(day)['run_supervisor_cleanup'],
        output_path('audit', f'trade_lifecycle_{day}.jsonl'),
        os.path.join(OUT_DIR, 'health_heartbeats', f'health_heartbeats_{day}.jsonl'),
    ]
    intraday_optional = [
        output_path('audit', f'trade_lifecycle_{day}.jsonl'),
        os.path.join(OUT_DIR, 'health_heartbeats', f'health_heartbeats_{day}.jsonl'),
    ]
    expected = list(pre_market_expected)
    optional = []
    if mode == 'post_market':
        expected += post_market_only
    elif mode == 'intraday':
        optional = intraday_optional
    elif mode == 'pre_market':
        optional = post_market_only
    else:
        optional = post_market_only + intraday_optional
    safety_dir = os.path.join(OUT_DIR, 'broker_safety_snapshots')
    safety_hits = []
    if os.path.isdir(safety_dir):
        safety_hits = [
            os.path.join(safety_dir, name)
            for name in os.listdir(safety_dir)
            if name.startswith(f'broker_safety_{day}_')
        ]
    files = [_file_info(path) for path in expected]
    optional_files = [_file_info(path) for path in optional]
    missing = [f for f in files if not f.get('exists')]
    safety_required = mode in ('pre_market', 'post_market')
    return {
        'day': day,
        'mode': mode,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'ok': not missing and (bool(safety_hits) if safety_required else True),
        'files': files,
        'optional_files': optional_files,
        'missing': missing,
        'broker_safety_snapshots': [_file_info(path) for path in sorted(safety_hits)],
        'broker_safety_snapshot_present': bool(safety_hits),
        'deduction': 'Mode-aware validation: pre_market checks launch artifacts, intraday treats live logs as optional, post_market requires full review corpus.',
    }


def write_postmarket_artifact_validation(day: Optional[str] = None, mode: str = 'post_market') -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_postmarket_artifact_validation(day, mode=mode)
    suffix = '' if mode == 'post_market' else f'_{mode}'
    path = os.path.join(OUT_DIR, f'postmarket_artifact_validation_{day}{suffix}.json')
    return _write_json(path, payload), payload


def build_config_freeze(day: Optional[str] = None, label: str = 'pre_open') -> dict:
    day = day or _now_ct().date().isoformat()
    cfg_path = os.path.join(HERE, 'trading_config.json')
    cfg = _read_json(cfg_path, {}) or {}
    status, status_error = _status()
    return {
        'day': day,
        'label': label,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'config_sha256': _sha256(cfg_path),
        'strategy_config_hash_from_status': status.get('strategy_config_hash'),
        'status_error': status_error,
        'config': cfg,
        'note': 'Immutable evidence of the config intended to trade this session.',
    }


def write_config_freeze(day: Optional[str] = None, label: str = 'pre_open') -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_config_freeze(day, label=label)
    path = os.path.join(OUT_DIR, f'config_freeze_{day}_{label}.json')
    return _write_json(path, payload), payload


def build_config_change_watch(day: Optional[str] = None, freeze_label: str = 'pre_open') -> dict:
    day = day or _now_ct().date().isoformat()
    freeze_path = os.path.join(OUT_DIR, f'config_freeze_{day}_{freeze_label}.json')
    freeze = _read_json(freeze_path, {}) or {}
    current_hash = _sha256(os.path.join(HERE, 'trading_config.json'))
    frozen_hash = freeze.get('config_sha256')
    status, _err = _status()
    in_window = bool(status.get('in_window'))
    changed = bool(frozen_hash and current_hash and frozen_hash != current_hash)
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'freeze_file': freeze_path,
        'freeze_exists': bool(freeze),
        'frozen_config_sha256': frozen_hash,
        'current_config_sha256': current_hash,
        'changed_since_freeze': changed,
        'in_window': in_window,
        'warning': (
            'trading_config_changed_during_session'
            if changed and in_window else
            'trading_config_changed_since_freeze'
            if changed else None
        ),
        'mode': 'warning_only_no_block',
    }


def write_config_change_watch(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_config_change_watch(day)
    path = os.path.join(OUT_DIR, f'config_change_watch_{day}.json')
    return _write_json(path, payload), payload


def build_trade_alerts(day: Optional[str] = None) -> dict:
    day = day or _now_ct().date().isoformat()
    status, status_error = _status()
    tape = _tape(day)
    heartbeats = _heartbeat_rows(day)
    skipped = _skipped_rows(day)
    recent_skipped = _since(skipped, 15)
    alerts = []
    largest_loss = min([float(t.get('pnl') or 0) for t in tape], default=0.0)
    if largest_loss <= -150:
        alerts.append({'level': 'warning', 'kind': 'large_loss', 'detail': round(largest_loss, 2)})
    closed = sorted(tape, key=lambda t: t.get('closed_at') or 0)
    streak = 0
    for t in reversed(closed):
        if (t.get('pnl') or 0) < 0:
            streak += 1
        else:
            break
    if streak >= 2:
        alerts.append({'level': 'warning', 'kind': 'loss_streak', 'detail': streak})
    short_losses = [t for t in tape if str(t.get('side') or '').upper() == 'SHORT' and (t.get('pnl') or 0) < 0]
    long_losses = [t for t in tape if str(t.get('side') or '').upper() == 'LONG' and (t.get('pnl') or 0) < 0]
    if len(short_losses) >= 2 and sum(float(t.get('pnl') or 0) for t in short_losses) <= -150:
        alerts.append({
            'level': 'warning',
            'kind': 'friday_like_short_damage_drift',
            'loss_count': len(short_losses),
            'gross_loss': round(sum(float(t.get('pnl') or 0) for t in short_losses), 2),
            'action': 'inspect short conviction and buying-flow warnings before any rule changes',
        })
    if len(long_losses) >= 2 and sum(float(t.get('pnl') or 0) for t in long_losses) <= -150:
        alerts.append({
            'level': 'warning',
            'kind': 'long_followthrough_damage_drift',
            'loss_count': len(long_losses),
            'gross_loss': round(sum(float(t.get('pnl') or 0) for t in long_losses), 2),
            'action': 'inspect failed-follow-through and BTC divergence before any rule changes',
        })
    weak_entry_losses = [
        t for t in tape
        if (t.get('pnl') or 0) < 0
        and str(t.get('entry_quality_tier') or (t.get('entry_thesis') or {}).get('entry_quality_tier') or '').lower() in ('c', 'd', 'weak', 'poor')
    ]
    if len(weak_entry_losses) >= 2:
        alerts.append({
            'level': 'warning',
            'kind': 'weak_entry_loss_cluster',
            'detail': len(weak_entry_losses),
            'action': 'review current_engine_regrade and decision_audit_summary',
        })
    if status.get('broker_exposure_block'):
        alerts.append({'level': 'critical', 'kind': 'broker_exposure_block', 'detail': status.get('broker_exposure_block')})
    if status.get('broker_lifecycle_block'):
        alerts.append({'level': 'critical', 'kind': 'broker_lifecycle_block', 'detail': status.get('broker_lifecycle_block')})
    if status.get('broker_api_degraded'):
        alerts.append({'level': 'warning', 'kind': 'broker_api_degraded', 'detail': status.get('broker_api_degraded')})
    for sym, pos in (status.get('positions') or {}).items():
        try:
            entry = float(pos.get('entry') or pos.get('entry_price') or 0)
            current = float(pos.get('cur_price') or pos.get('current_price') or 0)
            side = str(pos.get('side') or '').lower()
            if entry <= 0 or current <= 0:
                continue
            signed_ret = (current - entry) / entry * 100.0
            if side == 'short':
                signed_ret *= -1
            unrealized = pos.get('unrealized')
            path = pos.get('_path') or {}
            mfe = path.get('mfe_pct')
            checks = path.get('checks') or []
            latest_check = checks[-1] if checks else {}
            if signed_ret <= -0.35 or (unrealized is not None and float(unrealized) <= -75):
                alerts.append({
                    'level': 'warning',
                    'kind': 'large_loss_developing',
                    'symbol': sym,
                    'side': side,
                    'signed_return_pct': round(signed_ret, 4),
                    'unrealized_pnl': unrealized,
                    'mfe_pct': mfe,
                    'latest_path_check': latest_check,
                    'action': 'review live thesis and protection; passive alert only',
                })
        except Exception:
            continue
    last_hb = heartbeats[-1] if heartbeats else {}
    freshness = last_hb.get('ticker_freshness') or {}
    for sym, row in freshness.items():
        age = row.get('age_sec')
        if age is None:
            continue
        threshold = 20 if sym == 'BTC/USD' else 10
        if float(age) > threshold:
            alerts.append({'level': 'warning', 'kind': 'stale_feed', 'symbol': sym, 'age_sec': age})
    recent_reasons = Counter(str(r.get('reason') or 'unknown') for r in recent_skipped)
    pre_submit_blocks = sum(
        count for reason, count in recent_reasons.items()
        if reason.startswith('pre_submit') or 'spread' in reason or 'quote_stale' in reason
    )
    if pre_submit_blocks >= 3:
        alerts.append({'level': 'warning', 'kind': 'execution_blocks_spike', 'detail': pre_submit_blocks})
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'status_reachable': status_error is None,
        'status_error': status_error,
        'alert_count': len(alerts),
        'alerts': alerts,
        'thresholds': {
            'large_loss_pnl': -150,
            'loss_streak': 2,
            'btc_stale_sec': 20,
            'stock_stale_sec': 10,
            'recent_execution_blocks_15m': 3,
            'developing_loss_signed_return_pct': -0.35,
            'developing_loss_unrealized_pnl': -75,
            'side_damage_cluster_pnl': -150,
            'weak_entry_loss_cluster_count': 2,
        },
        'mode': 'passive_warning_only',
    }


def write_trade_alerts(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_trade_alerts(day)
    path = os.path.join(OUT_DIR, f'trade_alerts_{day}.json')
    return _write_json(path, payload), payload


def build_now_status(day: Optional[str] = None) -> dict:
    day = day or _now_ct().date().isoformat()
    status, status_error = _status()
    alerts = _read_json(os.path.join(OUT_DIR, f'trade_alerts_{day}.json'), {}) or build_trade_alerts(day)
    feed = _read_json(os.path.join(OUT_DIR, f'feed_health_score_{day}.json'), {}) or build_feed_health_score(day)
    execution = _read_json(os.path.join(OUT_DIR, f'broker_execution_score_{day}.json'), {}) or build_broker_execution_score(day)
    why = _read_json(os.path.join(OUT_DIR, f'why_no_trade_{day}.json'), {}) or build_why_no_trade_summary(day)
    gate = _read_json(os.path.join(OUT_DIR, f'daily_review_gate_{day}.json'), {}) or build_daily_review_gate(day)
    positions = status.get('positions') or {}
    critical = [a for a in alerts.get('alerts') or [] if a.get('level') == 'critical']
    warnings = [a for a in alerts.get('alerts') or [] if a.get('level') == 'warning']
    state = 'critical' if critical else ('warning' if warnings else 'calm')
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'state': state,
        'safe': {
            'status_reachable': status_error is None,
            'running': status.get('running'),
            'in_window': status.get('in_window'),
            'broker_exposure_block': status.get('broker_exposure_block'),
            'broker_lifecycle_block': status.get('broker_lifecycle_block'),
            'broker_api_degraded': status.get('broker_api_degraded'),
            'pending_entries': status.get('pending_entries') or {},
            'open_position_count': len(positions),
        },
        'open_trade_quality': {
            sym: {
                'side': p.get('side'),
                'entry': p.get('entry'),
                'cur_price': p.get('cur_price'),
                'unrealized': p.get('unrealized'),
                'entry_quality_tier': p.get('entry_quality_tier') or (p.get('entry_thesis') or {}).get('entry_quality_tier'),
                'path': p.get('_path') or p.get('path'),
            }
            for sym, p in positions.items()
        },
        'feed': {'score': feed.get('score'), 'verdict': feed.get('verdict')},
        'broker_execution': {'score': execution.get('score'), 'verdict': execution.get('verdict')},
        'review_gate': {'verdict': gate.get('verdict'), 'checks_passed': gate.get('checks_passed'), 'checks_total': gate.get('checks_total')},
        'alerts': alerts.get('alerts') or [],
        'why_no_trade_recent': why.get('recent'),
        'files': {
            'review_start_here': os.path.join(OUT_DIR, f'DAILY_REVIEW_START_HERE_{day}.json'),
            'trade_alerts': os.path.join(OUT_DIR, f'trade_alerts_{day}.json'),
            'daily_review_gate': os.path.join(OUT_DIR, f'daily_review_gate_{day}.json'),
        },
    }


def write_now_status(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_now_status(day)
    dated_path = os.path.join(OUT_DIR, f'NOW_STATUS_{day}.json')
    _write_json(dated_path, payload)
    return _write_json(os.path.join(OUT_DIR, 'NOW_STATUS.json'), payload), payload


def build_monday_launch_checklist(day: Optional[str] = None) -> dict:
    day = day or _now_ct().date().isoformat()
    status, status_error = _status()
    pre = build_premarket_checklist(day)
    validation = build_postmarket_artifact_validation(day, mode='pre_market')
    now = build_now_status(day)
    config_watch = build_config_change_watch(day)
    checks = [
        {'name': 'app_reachable', 'ok': status_error is None, 'detail': status_error},
        {'name': 'broker_flat_and_ready', 'ok': pre.get('ok'), 'detail': [c for c in pre.get('checks', []) if not c.get('ok')]},
        {'name': 'config_frozen_or_freezable', 'ok': bool(_sha256(os.path.join(HERE, 'trading_config.json'))), 'detail': _sha256(os.path.join(HERE, 'trading_config.json'))},
        {'name': 'config_not_changed_since_freeze', 'ok': not config_watch.get('changed_since_freeze'), 'detail': config_watch.get('warning')},
        {'name': 'pre_market_artifacts_present', 'ok': validation.get('ok'), 'detail': [m.get('path') for m in validation.get('missing', [])]},
        {'name': 'now_status_calm_or_warning', 'ok': now.get('state') in ('calm', 'warning'), 'detail': now.get('state')},
        {'name': 'no_open_positions', 'ok': not status.get('positions'), 'detail': status.get('positions')},
        {'name': 'no_pending_entries', 'ok': not status.get('pending_entries'), 'detail': status.get('pending_entries')},
    ]
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'ok': all(bool(c.get('ok')) for c in checks),
        'checks': checks,
        'commands': [
            {'time_ct': '08:25', 'command': 'python smoke_check.py --mode no-surprises'},
            {'time_ct': '08:30', 'command': 'python live_monitor.py --loop --interval-sec 30 --parity-sentinel'},
            {'time_ct': '08:35', 'command': 'python smoke_check.py --mode post-open'},
            {'time_ct': '12:00', 'command': f'python live_monitor.py {day} --checkpoint --checkpoint-label noon'},
            {'time_ct': '14:55', 'command': 'hard flat should trigger automatically'},
            {'time_ct': '15:05', 'command': 'python monday_close_packet.py'},
        ],
        'files_to_open_first': [
            os.path.join(OUT_DIR, 'NOW_STATUS.json'),
            os.path.join(OUT_DIR, f'MONDAY_REVIEW_START_HERE_{day}.json'),
            os.path.join(OUT_DIR, f'monday_live_review_{day}.json'),
            os.path.join(OUT_DIR, f'trade_alerts_{day}.json'),
            os.path.join(OUT_DIR, f'daily_review_gate_{day}.json'),
        ],
        'deduction': 'Single launch checklist for Monday. It should be green before market open; feed score may remain not_started until live heartbeats begin.',
    }


def write_monday_launch_checklist(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_monday_launch_checklist(day)
    path = os.path.join(OUT_DIR, f'monday_launch_checklist_{day}.json')
    txt_path = os.path.join(HERE, 'MONDAY_LAUNCH_CHECKLIST.md')
    lines = [
        f'# Monday Launch Checklist - {day}',
        '',
        f"Overall: {'OK' if payload.get('ok') else 'NOT OK'}",
        '',
        '## Checks',
    ]
    for c in payload.get('checks') or []:
        lines.append(f"- [{'x' if c.get('ok') else ' '}] {c.get('name')}: {c.get('detail')}")
    lines.extend(['', '## Commands'])
    for row in payload.get('commands') or []:
        lines.append(f"- {row.get('time_ct')} CT: `{row.get('command')}`")
    lines.extend(['', '## Open First'])
    lines.extend(f"- `{p}`" for p in payload.get('files_to_open_first') or [])
    _write_json(path, payload)
    with open(txt_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')
    return path, payload


def build_trader_notes_template(day: Optional[str] = None) -> dict:
    day = day or _now_ct().date().isoformat()
    return {
        'day': day,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'market_feel': '',
        'known_catalysts': [],
        'manual_observations': [],
        'btc_context_notes': '',
        'ticker_notes': {'CLSK': '', 'MARA': '', 'RIOT': ''},
        'risk_notes': '',
        'what_i_would_change_if_any': '',
        'what_i_refuse_to_change_from_one_day': '',
    }


def write_trader_notes_template(day: Optional[str] = None, overwrite: bool = False) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    path = os.path.join(OUT_DIR, f'notes_{day}.json')
    if os.path.exists(path) and not overwrite:
        return path, _read_json(path, {}) or {}
    payload = build_trader_notes_template(day)
    return _write_json(path, payload), payload


def build_session_checkpoint(day: Optional[str] = None, label: str = 'manual') -> dict:
    day = day or _now_ct().date().isoformat()
    status, status_error = _status()
    tape = _tape(day)
    why = build_why_no_trade_summary(day, recent_minutes=30)
    alerts = build_trade_alerts(day)
    monitor = build_market_open_monitor(day)
    tiers = build_entry_quality_tiers(day)
    thesis = build_thesis_failure_review(day)
    personality = build_per_symbol_personality(day)
    feed = build_feed_health_score(day)
    execution = build_broker_execution_score(day)
    exit_quality = build_exit_quality_score(day)
    return {
        'day': day,
        'label': label,
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'status_reachable': status_error is None,
        'status_error': status_error,
        'status': {
            'running': status.get('running'),
            'in_window': status.get('in_window'),
            'positions': status.get('positions') or {},
            'pending_entries': status.get('pending_entries') or {},
            'trade_count': status.get('trade_count'),
            'wins': status.get('wins'),
            'losses': status.get('losses'),
            'total_pnl': status.get('total_pnl'),
            'broker_exposure_block': status.get('broker_exposure_block'),
            'broker_lifecycle_block': status.get('broker_lifecycle_block'),
        },
        'closed_trade_snapshot': {
            'count': len(tape),
            'latest': sorted(tape, key=lambda t: t.get('closed_at') or 0)[-5:],
        },
        'learning_snapshot': {
            'entry_quality_tiers': tiers.get('tiers') or [],
            'top_thesis_failures': (thesis.get('failure_modes') or [])[:5],
            'per_symbol_personality': personality.get('symbols') or [],
        },
        'why_no_trade_recent': why.get('recent'),
        'alerts': alerts.get('alerts'),
        'health_scores': {
            'feed': {'score': feed.get('score'), 'verdict': feed.get('verdict')},
            'broker_execution': {'score': execution.get('score'), 'verdict': execution.get('verdict')},
            'exit_quality': {'score': exit_quality.get('score'), 'counts': exit_quality.get('counts')},
        },
        'feed_freshness': monitor.get('feed_freshness'),
        'deduction': 'Checkpoint is passive. Use it to decide what to inspect, not to auto-change rules.',
    }


def write_session_checkpoint(day: Optional[str] = None, label: str = 'manual') -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    safe_label = ''.join(c if c.isalnum() or c in ('-', '_') else '_' for c in label)
    payload = build_session_checkpoint(day, safe_label)
    path = os.path.join(OUT_DIR, f'session_checkpoint_{day}_{safe_label}.json')
    return _write_json(path, payload), payload


def build_review_start_here(day: Optional[str] = None) -> dict:
    day = day or _now_ct().date().isoformat()
    risk = _read_json(os.path.join(OUT_DIR, f'risk_summary_{day}.json'), {}) or {}
    alerts = _read_json(os.path.join(OUT_DIR, f'trade_alerts_{day}.json'), {}) or build_trade_alerts(day)
    why = _read_json(os.path.join(OUT_DIR, f'why_no_trade_{day}.json'), {}) or build_why_no_trade_summary(day)
    split = _read_json(os.path.join(OUT_DIR, f'strategy_operations_split_{day}.json'), {}) or build_strategy_operations_split(day)
    clusters = _read_json(os.path.join(OUT_DIR, f'loser_clusters_{day}.json'), {}) or build_loser_clusters(day)
    archetypes = _read_json(os.path.join(OUT_DIR, f'loser_archetypes_{day}.json'), {}) or build_loser_archetypes(day)
    tiers = _read_json(os.path.join(OUT_DIR, f'entry_quality_tiers_{day}.json'), {}) or build_entry_quality_tiers(day)
    winner_damage = _read_json(os.path.join(OUT_DIR, f'winner_damage_report_{day}.json'), {}) or build_winner_damage_report(day)
    clean = _read_json(os.path.join(OUT_DIR, f'clean_day_score_{day}.json'), {}) or build_clean_day_score_artifact(day)
    no_trade = _read_json(os.path.join(OUT_DIR, f'no_trade_opportunity_grades_{day}.json'), {}) or build_no_trade_opportunity_grades(day)
    thesis = _read_json(os.path.join(OUT_DIR, f'thesis_failure_review_{day}.json'), {}) or build_thesis_failure_review(day)
    replay = _read_json(os.path.join(OUT_DIR, f'loser_replay_snapshots_{day}.json'), {}) or build_loser_replay_snapshots(day)
    personality = _read_json(os.path.join(OUT_DIR, f'per_symbol_personality_{day}_10d.json'), {}) or build_per_symbol_personality(day)
    oos = _read_json(os.path.join(OUT_DIR, f'out_of_sample_scoreboard_{day}.json'), {}) or build_out_of_sample_scoreboard(day)
    first_hour = _read_json(os.path.join(OUT_DIR, f'first_hour_review_{day}.json'), {}) or build_first_hour_review(day)
    gate = _read_json(os.path.join(OUT_DIR, f'strategy_conclusion_gate_{day}.json'), {}) or build_strategy_conclusion_gate(day)
    market_context = _read_json(os.path.join(OUT_DIR, f'market_context_scoreboard_{day}.json'), {}) or build_market_context_scoreboard(day)
    thesis_timeline = _read_json(os.path.join(OUT_DIR, f'trade_thesis_timelines_{day}.json'), {}) or build_trade_thesis_timeline_artifact(day)
    winner_quality = _read_json(os.path.join(OUT_DIR, f'winner_quality_{day}.json'), {}) or build_winner_quality_artifact(day)
    quarantine = _read_json(os.path.join(OUT_DIR, f'rule_candidate_quarantine_{day}.json'), {}) or build_rule_candidate_quarantine(day)
    dashboard = _read_json(os.path.join(OUT_DIR, f'world_class_dashboard_{day}.json'), {}) or build_world_class_dashboard(day)
    feed = _read_json(os.path.join(OUT_DIR, f'feed_health_score_{day}.json'), {}) or build_feed_health_score(day)
    execution = _read_json(os.path.join(OUT_DIR, f'broker_execution_score_{day}.json'), {}) or build_broker_execution_score(day)
    era = _read_json(os.path.join(OUT_DIR, f'era_scorecard_{day}.json'), {}) or build_era_scorecard(day)
    monday = _read_json(os.path.join(OUT_DIR, f'monday_scorecard_{day}.json'), {}) or build_monday_scorecard(day)
    replay = _read_json(os.path.join(OUT_DIR, f'ws_scalp_replay_{day}.json'), {}) or build_ws_scalp_replay(day)
    regrade = _read_json(os.path.join(OUT_DIR, f'current_engine_regrade_{day}.json'), {}) or build_current_engine_regrade(day)
    near_miss = _read_json(os.path.join(OUT_DIR, f'near_miss_winners_{day}.json'), {}) or build_near_miss_winners(day)
    exit_quality = _read_json(os.path.join(OUT_DIR, f'exit_quality_score_{day}.json'), {}) or build_exit_quality_score(day)
    review_gate = _read_json(os.path.join(OUT_DIR, f'daily_review_gate_{day}.json'), {}) or build_daily_review_gate(day)
    lifecycle = _read_json(os.path.join(OUT_DIR, f'rule_lifecycle_dashboard_{day}.json'), {}) or build_rule_lifecycle_dashboard(day)
    fpfn = _read_json(os.path.join(OUT_DIR, f'false_positive_negative_{day}.json'), {}) or build_false_positive_negative_tables(day)
    runtime_alerts = _read_json(os.path.join(OUT_DIR, f'runtime_restart_alerts_{day}.json'), {}) or build_runtime_restart_alerts(day)
    return {
        'day': day,
        'era': era_for_day(day),
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'purpose': 'Smallest token-efficient starting point for daily review.',
        'headline': {
            'pnl': risk.get('pnl'),
            'trades': risk.get('trades'),
            'wins': risk.get('wins'),
            'losses': risk.get('losses'),
            'win_rate': risk.get('win_rate'),
            'profit_factor': risk.get('profit_factor'),
            'largest_loss': risk.get('largest_loss'),
        },
        'clean_day_score': clean.get('clean_day_score'),
        'entry_quality_tiers': (tiers.get('tiers') or [])[:5],
        'alerts': alerts.get('alerts') or [],
        'runtime_restart_alerts': {
            'ok': runtime_alerts.get('ok'),
            'unexpected_restart_count': runtime_alerts.get('unexpected_restart_count'),
            'alerts': (runtime_alerts.get('alerts') or [])[:5],
        },
        'strategy_operations_split': {
            'strategy': split.get('strategy'),
            'data_or_execution': split.get('data_or_execution'),
            'operations': split.get('operations'),
        },
        'why_no_trade_top': {
            'recent': why.get('recent'),
            'all_day_top_skipped_reasons': (why.get('all_day_top_skipped_reasons') or [])[:8],
            'all_day_top_near_signal_reasons': (why.get('all_day_top_near_signal_reasons') or [])[:8],
        },
        'largest_loser_clusters': (clusters.get('clusters') or [])[:5],
        'largest_loser_archetypes': (archetypes.get('archetypes') or [])[:5],
        'thesis_failure_modes': (thesis.get('failure_modes') or [])[:5],
        'loser_replay_snapshot_count': replay.get('loser_count'),
        'per_symbol_personality': personality.get('symbols') or [],
        'out_of_sample_scoreboard': {
            'baseline_day': oos.get('baseline_day'),
            'delta_vs_baseline': oos.get('delta_vs_baseline'),
        },
        'first_hour_review': first_hour.get('first_hour'),
        'strategy_conclusion_gate': {
            'allowed': gate.get('strategy_conclusions_allowed'),
            'recommendation': gate.get('recommendation'),
        },
        'daily_review_gate': {
            'verdict': review_gate.get('verdict'),
            'checks_passed': review_gate.get('checks_passed'),
            'checks_total': review_gate.get('checks_total'),
        },
        'rule_lifecycle_counts': lifecycle.get('counts'),
        'false_positive_negative': {
            'false_positive_count': fpfn.get('false_positive_count'),
            'false_negative_count': fpfn.get('false_negative_count'),
        },
        'market_context': market_context.get('session_weather'),
        'trade_thesis_timeline_counts': thesis_timeline.get('counts'),
        'winner_quality_counts': winner_quality.get('counts'),
        'rule_candidate_quarantine_top': (quarantine.get('items') or [])[:5],
        'world_class_dashboard': {
            'safe': dashboard.get('safe'),
            'trading_quality': dashboard.get('trading_quality'),
            'health': dashboard.get('health'),
        },
        'health_scores': {
            'feed': {'score': feed.get('score'), 'verdict': feed.get('verdict')},
            'broker_execution': {'score': execution.get('score'), 'verdict': execution.get('verdict')},
            'monday_scorecard': {'verdict': monday.get('verdict'), 'checks_passed': monday.get('checks_passed'), 'checks_total': monday.get('checks_total')},
            'era_summary': era.get('summary'),
            'ws_scalp_replay_counts': replay.get('counts'),
            'current_engine_regrade_counts': regrade.get('counts'),
            'near_miss_winner_count': near_miss.get('count'),
            'exit_quality_score': exit_quality.get('score'),
        },
        'winner_damage_top_rules': (winner_damage.get('rules') or [])[:5],
        'no_trade_grade_counts': no_trade.get('grade_counts') or {},
        'files_to_open_next': [
            os.path.join(OUT_DIR, f'monday_live_review_{day}.json'),
            os.path.join(OUT_DIR, f'market_open_monitor_{day}.json'),
            os.path.join(OUT_DIR, f'why_no_trade_{day}.json'),
            os.path.join(OUT_DIR, f'loser_clusters_{day}.json'),
            os.path.join(OUT_DIR, f'loser_archetypes_{day}.json'),
            os.path.join(OUT_DIR, f'entry_quality_tiers_{day}.json'),
            os.path.join(OUT_DIR, f'winner_damage_report_{day}.json'),
            os.path.join(OUT_DIR, f'no_trade_opportunity_grades_{day}.json'),
            os.path.join(OUT_DIR, f'thesis_failure_review_{day}.json'),
            os.path.join(OUT_DIR, f'loser_replay_snapshots_{day}.json'),
            os.path.join(OUT_DIR, f'per_symbol_personality_{day}_10d.json'),
            os.path.join(OUT_DIR, f'out_of_sample_scoreboard_{day}.json'),
            os.path.join(OUT_DIR, f'first_hour_review_{day}.json'),
            os.path.join(OUT_DIR, f'strategy_conclusion_gate_{day}.json'),
            os.path.join(OUT_DIR, f'market_context_scoreboard_{day}.json'),
            os.path.join(OUT_DIR, f'trade_thesis_timelines_{day}.json'),
            os.path.join(OUT_DIR, f'winner_quality_{day}.json'),
            os.path.join(OUT_DIR, f'rule_candidate_quarantine_{day}.json'),
            os.path.join(OUT_DIR, f'world_class_dashboard_{day}.json'),
            os.path.join(OUT_DIR, f'feed_health_score_{day}.json'),
            os.path.join(OUT_DIR, f'broker_execution_score_{day}.json'),
            os.path.join(OUT_DIR, f'era_scorecard_{day}.json'),
            os.path.join(OUT_DIR, f'monday_scorecard_{day}.json'),
            os.path.join(OUT_DIR, f'ws_scalp_replay_{day}.json'),
            os.path.join(OUT_DIR, f'decision_audit_summary_{day}.json'),
            os.path.join(OUT_DIR, f'current_engine_regrade_{day}.json'),
            os.path.join(OUT_DIR, f'regime_specific_scorecards_{day}.json'),
            os.path.join(OUT_DIR, f'near_miss_winners_{day}.json'),
            os.path.join(OUT_DIR, f'exit_quality_score_{day}.json'),
            os.path.join(OUT_DIR, f'replay_confidence_{day}.json'),
            os.path.join(OUT_DIR, f'false_positive_negative_{day}.json'),
            os.path.join(OUT_DIR, f'daily_review_gate_{day}.json'),
            os.path.join(OUT_DIR, f'rule_lifecycle_dashboard_{day}.json'),
            os.path.join(OUT_DIR, f'NOW_STATUS_{day}.json'),
            os.path.join(OUT_DIR, f'multi_day_scorecard_{day}_10d.json'),
            os.path.join(OUT_DIR, f'market_regime_day_{day}.json'),
            os.path.join(OUT_DIR, f'edge_quality_activity_{day}_10d.json'),
            os.path.join(OUT_DIR, f'order_latency_{day}.json'),
            os.path.join(OUT_DIR, f'runtime_restart_alerts_{day}.json'),
            os.path.join(OUT_DIR, 'promotion_queue.json'),
        ],
    }


def write_review_start_here(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_review_start_here(day)
    daily_path = os.path.join(OUT_DIR, f'DAILY_REVIEW_START_HERE_{day}.json')
    monday_path = os.path.join(OUT_DIR, f'MONDAY_REVIEW_START_HERE_{day}.json')
    _write_json(daily_path, payload)
    _write_json(monday_path, payload)
    return daily_path, payload


def build_monday_live_review(day: Optional[str] = None) -> dict:
    day = day or _now_ct().date().isoformat()
    status, status_error = _status()
    risk = _read_json(os.path.join(OUT_DIR, f'risk_summary_{day}.json'), {}) or {}
    losers = _read_json(os.path.join(OUT_DIR, f'loser_clusters_{day}.json'), {}) or {}
    archetypes = _read_json(os.path.join(OUT_DIR, f'loser_archetypes_{day}.json'), {}) or {}
    tiers = _read_json(os.path.join(OUT_DIR, f'entry_quality_tiers_{day}.json'), {}) or {}
    clean = _read_json(os.path.join(OUT_DIR, f'clean_day_score_{day}.json'), {}) or {}
    first_hour = _read_json(os.path.join(OUT_DIR, f'first_hour_review_{day}.json'), {}) or {}
    gate = _read_json(os.path.join(OUT_DIR, f'strategy_conclusion_gate_{day}.json'), {}) or {}
    dashboard = _read_json(os.path.join(OUT_DIR, f'world_class_dashboard_{day}.json'), {}) or {}
    feed = _read_json(os.path.join(OUT_DIR, f'feed_health_score_{day}.json'), {}) or {}
    execution = _read_json(os.path.join(OUT_DIR, f'broker_execution_score_{day}.json'), {}) or {}
    era = _read_json(os.path.join(OUT_DIR, f'era_scorecard_{day}.json'), {}) or {}
    monday = _read_json(os.path.join(OUT_DIR, f'monday_scorecard_{day}.json'), {}) or {}
    replay = _read_json(os.path.join(OUT_DIR, f'ws_scalp_replay_{day}.json'), {}) or {}
    regrade = _read_json(os.path.join(OUT_DIR, f'current_engine_regrade_{day}.json'), {}) or {}
    exit_quality = _read_json(os.path.join(OUT_DIR, f'exit_quality_score_{day}.json'), {}) or {}
    return {
        'day': day,
        'era': era_for_day(day),
        'created_at_ct': _now_ct().isoformat(timespec='seconds'),
        'status_reachable': status_error is None,
        'status_error': status_error,
        'current': {
            'running': status.get('running'),
            'in_window': status.get('in_window'),
            'positions': status.get('positions') or {},
            'pending_entries': status.get('pending_entries') or {},
            'total_pnl': status.get('total_pnl'),
            'wins': status.get('wins'),
            'losses': status.get('losses'),
            'trade_count': status.get('trade_count'),
            'broker_exposure_block': status.get('broker_exposure_block'),
            'broker_lifecycle_block': status.get('broker_lifecycle_block'),
            'broker_api_degraded': status.get('broker_api_degraded'),
            'strategy_config_hash': status.get('strategy_config_hash'),
        },
        'latest_artifact_summary': {
            'risk': risk,
            'largest_loser_clusters': (losers.get('clusters') or [])[:5],
            'largest_loser_archetypes': (archetypes.get('archetypes') or [])[:5],
            'entry_quality_tiers': (tiers.get('tiers') or [])[:5],
            'clean_day_score': clean.get('clean_day_score'),
            'first_hour_review': first_hour.get('first_hour'),
            'strategy_conclusion_gate': {
                'allowed': gate.get('strategy_conclusions_allowed'),
                'recommendation': gate.get('recommendation'),
            },
            'world_class_dashboard': dashboard.get('trading_quality'),
            'health_scores': {
                'feed': {'score': feed.get('score'), 'verdict': feed.get('verdict')},
                'broker_execution': {'score': execution.get('score'), 'verdict': execution.get('verdict')},
                'monday_scorecard': {'verdict': monday.get('verdict'), 'checks_passed': monday.get('checks_passed'), 'checks_total': monday.get('checks_total')},
                'era_summary': era.get('summary'),
                'ws_scalp_replay_counts': replay.get('counts'),
                'current_engine_regrade_counts': regrade.get('counts'),
                'exit_quality_score': exit_quality.get('score'),
            },
        },
        'why_no_trade': build_why_no_trade_summary(day),
        'alerts': build_trade_alerts(day),
        'config_change_watch': build_config_change_watch(day),
        'market_regime_day': build_market_regime_day_label(day),
        'edge_quality_activity': build_edge_quality_activity(day),
        'order_latency': build_order_latency_summary(day),
        'market_context_scoreboard': build_market_context_scoreboard(day),
        'trade_thesis_timelines': build_trade_thesis_timeline_artifact(day),
        'winner_quality': build_winner_quality_artifact(day),
        'rule_candidate_quarantine': build_rule_candidate_quarantine(day),
        'feed_health_score': build_feed_health_score(day),
        'broker_execution_score': build_broker_execution_score(day),
        'era_scorecard': build_era_scorecard(day),
        'monday_scorecard': build_monday_scorecard(day),
        'ws_scalp_replay': build_ws_scalp_replay(day),
        'decision_audit_summary': build_decision_audit_summary(day),
        'current_engine_regrade': build_current_engine_regrade(day),
        'regime_specific_scorecards': build_regime_specific_scorecards(day),
        'near_miss_winners': build_near_miss_winners(day),
        'exit_quality_score': build_exit_quality_score(day),
        'replay_confidence': build_replay_confidence(day),
        'false_positive_negative': build_false_positive_negative_tables(day),
        'daily_review_gate': build_daily_review_gate(day),
        'rule_lifecycle_dashboard': build_rule_lifecycle_dashboard(day),
        'rule_dry_run_scoreboard': build_rule_dry_run_scoreboard(day),
        'execution_context_summary': build_execution_context_summary(day),
        'retention_plan_file': os.path.join(OUT_DIR, f'retention_plan_{day}.json'),
        'now_status': build_now_status(day),
        'config_intent_ledger': build_config_intent_ledger(day),
        'world_class_dashboard': build_world_class_dashboard(day),
        'market_open_monitor_file': os.path.join(OUT_DIR, f'market_open_monitor_{day}.json'),
        'review_start_here_file': os.path.join(OUT_DIR, f'DAILY_REVIEW_START_HERE_{day}.json'),
        'review_question': 'During the day, check this before raw logs: are losses clustering by side/setup/regime or by execution friction?',
    }


def write_monday_live_review(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_monday_live_review(day)
    path = os.path.join(OUT_DIR, f'monday_live_review_{day}.json')
    return _write_json(path, payload), payload


def _rule_dry_run_keys(row: dict) -> list[str]:
    keys = []
    for item in row.get('near_miss_rules') or []:
        if isinstance(item, dict):
            rule = item.get('rule') or item.get('name') or item.get('reason')
        else:
            rule = str(item)
        if rule:
            keys.append(f'near_miss:{rule}')
    fx = row.get('forensics') or {}
    micro = fx.get('market_microstructure') or {}
    if micro.get('halt_suspected'):
        keys.append('market_microstructure:halt_suspected')
    if micro.get('luld_like_risk'):
        keys.append('market_microstructure:luld_like_risk')
    if micro.get('ssr_approx_active'):
        keys.append('market_microstructure:ssr_approx_active')
    liq = fx.get('liquidity_impact') or {}
    if liq.get('impact_risk') in ('elevated', 'high'):
        keys.append(f'liquidity:{liq.get("impact_risk")}')
    catalyst = fx.get('catalyst_context') or {}
    if catalyst.get('has_catalyst'):
        keys.append('catalyst:flagged')
    proxy = fx.get('btc_proxy_basket') or {}
    if proxy.get('state') in ('proxy_risk_on', 'proxy_risk_off'):
        keys.append(f'btc_proxy:{proxy.get("state")}')
    if not keys and row.get('reason'):
        keys.append(f'skip_reason:{row.get("reason")}')
    return sorted(set(keys))


def build_rule_dry_run_scoreboard(day: Optional[str] = None) -> dict:
    day = day or _now_ct().date().isoformat()
    rows = _rule_dry_run_rows(day)
    by_rule = defaultdict(lambda: {
        'rule': None,
        'observed': 0,
        'entered': 0,
        'skipped': 0,
        'closed': 0,
        'wins': 0,
        'losses': 0,
        'pnl': 0.0,
        'net_pnl_after_estimated_costs': 0.0,
        'examples': [],
    })
    event_counts = Counter(row.get('event') for row in rows)
    for row in rows:
        keys = _rule_dry_run_keys(row) or ['uncategorized']
        for key in keys:
            rec = by_rule[key]
            rec['rule'] = key
            rec['observed'] += 1
            if row.get('decision') == 'entered':
                rec['entered'] += 1
            if row.get('decision') == 'skipped':
                rec['skipped'] += 1
            if row.get('decision') == 'closed':
                rec['closed'] += 1
                if row.get('result') == 'WIN':
                    rec['wins'] += 1
                elif row.get('result') == 'LOSS':
                    rec['losses'] += 1
                rec['pnl'] += float(row.get('pnl') or 0)
                rec['net_pnl_after_estimated_costs'] += float(
                    row.get('net_pnl_after_estimated_costs') or row.get('pnl') or 0
                )
            if len(rec['examples']) < 5:
                rec['examples'].append({
                    'event': row.get('event'),
                    'ticker': row.get('ticker'),
                    'side': row.get('side'),
                    'decision': row.get('decision'),
                    'reason': row.get('reason'),
                    'result': row.get('result'),
                    'pnl': row.get('pnl'),
                    'trade_id': row.get('trade_id'),
                })
    scoreboard = []
    for rec in by_rule.values():
        rec['pnl'] = round(rec['pnl'], 2)
        rec['net_pnl_after_estimated_costs'] = round(rec['net_pnl_after_estimated_costs'], 2)
        rec['win_rate'] = _safe_pct(rec['wins'], rec['wins'] + rec['losses'])
        rec['deduction'] = (
            'Use as a live-day dry-run scoreboard only. A rule must still pass multi-day '
            'evidence thresholds before it becomes a suggested action item.'
        )
        scoreboard.append(dict(rec))
    scoreboard.sort(
        key=lambda r: (r.get('losses') or 0, -(r.get('pnl') or 0), r.get('observed') or 0),
        reverse=True,
    )
    return {
        'day': day,
        'rows': len(rows),
        'event_counts': dict(event_counts),
        'scoreboard': scoreboard,
        'source_file': os.path.join(OUT_DIR, 'rule_dry_run', f'rule_dry_run_{day}.jsonl'),
        'purpose': (
            'Tracks how candidate rule/gate ideas behaved during the live day without automatically '
            'changing the bot.'
        ),
    }


def write_rule_dry_run_scoreboard(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_rule_dry_run_scoreboard(day)
    path = os.path.join(OUT_DIR, f'rule_dry_run_scoreboard_{day}.json')
    return _write_json(path, payload), payload


def build_execution_context_summary(day: Optional[str] = None) -> dict:
    day = day or _now_ct().date().isoformat()
    trades = _tape(day)
    skipped = _skipped_rows(day)
    rows = []
    total_cost = 0.0
    cost_count = 0
    micro_counts = Counter()
    liquidity_counts = Counter()
    proxy_counts = Counter()
    borrow_counts = Counter()
    catalyst_count = 0
    for source, records in (('trade', trades), ('skipped', skipped)):
        for rec in records:
            fx = rec.get('forensics') or {}
            if source == 'trade':
                costs = rec.get('estimated_transaction_costs') or {}
                if costs.get('estimated_total') is not None:
                    cost_count += 1
                    total_cost += float(costs.get('estimated_total') or 0)
                micro = rec.get('market_microstructure_at_entry') or fx.get('market_microstructure') or {}
                liq = rec.get('liquidity_impact_at_entry') or fx.get('liquidity_impact') or {}
                borrow = rec.get('short_borrow_snapshot') or fx.get('short_borrow_snapshot') or {}
                proxy = rec.get('btc_proxy_basket_at_entry') or fx.get('btc_proxy_basket') or {}
                catalyst = rec.get('catalyst_context_at_entry') or fx.get('catalyst_context') or {}
            else:
                micro = fx.get('market_microstructure') or {}
                liq = fx.get('liquidity_impact') or {}
                borrow = fx.get('short_borrow_snapshot') or {}
                proxy = fx.get('btc_proxy_basket') or {}
                catalyst = fx.get('catalyst_context') or {}
            if micro.get('halt_suspected'):
                micro_counts['halt_suspected'] += 1
            if micro.get('luld_like_risk'):
                micro_counts['luld_like_risk'] += 1
            if micro.get('ssr_approx_active'):
                micro_counts['ssr_approx_active'] += 1
            if liq.get('impact_risk'):
                liquidity_counts[str(liq.get('impact_risk'))] += 1
            if proxy.get('state'):
                proxy_counts[str(proxy.get('state'))] += 1
            if borrow:
                state = 'shortable' if borrow.get('shortable') else ('not_shortable' if borrow.get('available') else 'unknown')
                borrow_counts[state] += 1
            if catalyst.get('has_catalyst'):
                catalyst_count += 1
            if len(rows) < 25 and (micro or liq or borrow or proxy or catalyst):
                rows.append({
                    'source': source,
                    'ticker': rec.get('ticker'),
                    'side': rec.get('side'),
                    'result': rec.get('result'),
                    'pnl': rec.get('pnl'),
                    'micro': {
                        'halt_suspected': micro.get('halt_suspected'),
                        'luld_like_risk': micro.get('luld_like_risk'),
                        'ssr_approx_active': micro.get('ssr_approx_active'),
                        'spread_pct': micro.get('spread_pct'),
                    },
                    'liquidity_risk': liq.get('impact_risk'),
                    'borrow': {
                        'shortable': borrow.get('shortable'),
                        'easy_to_borrow': borrow.get('easy_to_borrow'),
                    },
                    'btc_proxy_state': proxy.get('state'),
                    'catalyst_flagged': catalyst.get('has_catalyst'),
                })
    return {
        'day': day,
        'trade_count': len(trades),
        'skipped_count': len(skipped),
        'estimated_transaction_costs': {
            'trades_with_estimate': cost_count,
            'estimated_total_cost': round(total_cost, 4),
            'avg_estimated_cost_per_trade': round(total_cost / cost_count, 4) if cost_count else None,
            'deduction': 'Cost estimates are configurable and used to rank friction; broker statements remain source of truth.',
        },
        'market_microstructure_counts': dict(micro_counts),
        'liquidity_impact_counts': dict(liquidity_counts),
        'btc_proxy_counts': dict(proxy_counts),
        'short_borrow_counts': dict(borrow_counts),
        'catalyst_flagged_rows': catalyst_count,
        'examples': rows,
        'purpose': 'Compact daily summary of the new market-structure, borrow, catalyst, BTC-proxy, liquidity, and cost fields.',
    }


def write_execution_context_summary(day: Optional[str] = None) -> tuple[str, dict]:
    day = day or _now_ct().date().isoformat()
    payload = build_execution_context_summary(day)
    path = os.path.join(OUT_DIR, f'execution_context_summary_{day}.json')
    return _write_json(path, payload), payload


def write_retention_plan_artifact(day: Optional[str] = None) -> tuple[str, dict]:
    from retention_policy import write_retention_plan
    return write_retention_plan(day or _now_ct().date().isoformat())


def write_weekend_artifacts(day: Optional[str] = None) -> tuple[dict, dict]:
    day = day or _now_ct().date().isoformat()
    paths = {}
    payloads = {}
    for key, writer in (
        ('pre_market_checklist', write_premarket_checklist),
        ('config_change_note', write_config_change_note),
        ('config_freeze', write_config_freeze),
        ('config_change_watch', write_config_change_watch),
        ('trader_notes_template', write_trader_notes_template),
        ('loser_clusters', write_loser_clusters),
        ('loser_archetypes', write_loser_archetypes),
        ('entry_quality_tiers', write_entry_quality_tiers),
        ('winner_damage_report', write_winner_damage_report),
        ('clean_day_score', write_clean_day_score_artifact),
        ('no_trade_opportunity_grades', write_no_trade_opportunity_grades),
        ('thesis_failure_review', write_thesis_failure_review),
        ('loser_replay_snapshots', write_loser_replay_snapshots),
        ('per_symbol_personality', write_per_symbol_personality),
        ('out_of_sample_scoreboard', write_out_of_sample_scoreboard),
        ('first_hour_review', write_first_hour_review),
        ('strategy_conclusion_gate', write_strategy_conclusion_gate),
        ('market_context_scoreboard', write_market_context_scoreboard),
        ('trade_thesis_timelines', write_trade_thesis_timeline_artifact),
        ('winner_quality', write_winner_quality_artifact),
        ('rule_candidate_quarantine', write_rule_candidate_quarantine),
        ('feed_health_score', write_feed_health_score),
        ('broker_execution_score', write_broker_execution_score),
        ('era_scorecard', write_era_scorecard),
        ('monday_scorecard', write_monday_scorecard),
        ('ws_scalp_replay', write_ws_scalp_replay),
        ('decision_audit_summary', write_decision_audit_summary),
        ('current_engine_regrade', write_current_engine_regrade),
        ('regime_specific_scorecards', write_regime_specific_scorecards),
        ('near_miss_winners', write_near_miss_winners),
        ('exit_quality_score', write_exit_quality_score),
        ('replay_confidence', write_replay_confidence),
        ('false_positive_negative', write_false_positive_negative_tables),
        ('daily_review_gate', write_daily_review_gate),
        ('rule_lifecycle_dashboard', write_rule_lifecycle_dashboard),
        ('now_status', write_now_status),
        ('monday_launch_checklist', write_monday_launch_checklist),
        ('config_intent_ledger', write_config_intent_ledger),
        ('world_class_dashboard', write_world_class_dashboard),
        ('strategy_operations_split', write_strategy_operations_split),
        ('runtime_restart_alerts', write_runtime_restart_alerts),
        ('multi_day_scorecard', write_multi_day_scorecard),
        ('market_regime_day', write_market_regime_day_label),
        ('edge_quality_activity', write_edge_quality_activity),
        ('order_latency', write_order_latency_summary),
        ('rule_dry_run_scoreboard', write_rule_dry_run_scoreboard),
        ('execution_context_summary', write_execution_context_summary),
        ('retention_plan', write_retention_plan_artifact),
        ('why_no_trade', write_why_no_trade_summary),
        ('market_open_monitor', write_market_open_monitor),
        ('trade_alerts', write_trade_alerts),
        ('session_checkpoint_manual', write_session_checkpoint),
        ('current_config_replay', write_current_config_replay),
        ('change_impact_ledger', write_change_impact_ledger),
        ('monday_live_review', write_monday_live_review),
        ('review_start_here', write_review_start_here),
        ('postmarket_artifact_validation', write_postmarket_artifact_validation),
    ):
        try:
            path, payload = writer(day)
            paths[key] = path
            payloads[key] = payload
        except Exception as e:
            paths[key] = None
            payloads[key] = {'error': str(e)}
    try:
        path, payload = write_broker_safety_snapshot(day, label='weekend_readiness')
        paths['broker_safety_snapshot'] = path
        payloads['broker_safety_snapshot'] = payload
    except Exception as e:
        paths['broker_safety_snapshot'] = None
        payloads['broker_safety_snapshot'] = {'error': str(e)}
    return paths, payloads


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser(description='Write weekend/pre-Monday readiness artifacts.')
    ap.add_argument('day', nargs='?', default=_now_ct().date().isoformat())
    ap.add_argument('--json', action='store_true')
    args = ap.parse_args()
    paths, payloads = write_weekend_artifacts(args.day)
    if args.json:
        print(json.dumps({'paths': paths, 'payloads': payloads}, indent=2, default=str))
    else:
        for path in paths.values():
            if path:
                print(path)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
