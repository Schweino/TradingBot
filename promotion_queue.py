from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo


HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, 'postmortem')
QUEUE_PATH = os.path.join(OUT_DIR, 'promotion_queue.json')
CT = ZoneInfo('America/Chicago')


def _load_json(path: str, default: Any) -> Any:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return default


def _save(payload: dict) -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(QUEUE_PATH, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, default=str)


def _candidate_key(kind: str, name: str) -> str:
    return f'{kind}:{name}'


def _affected_trades(evidence: dict, payload: dict | None = None) -> int:
    payload = payload or {}
    vals = [
        evidence.get('affected_trades'),
        evidence.get('trades'),
        evidence.get('matched'),
        evidence.get('triggered'),
        evidence.get('blocked'),
        payload.get('affected_trades'),
        payload.get('trades'),
        payload.get('matched'),
        payload.get('triggered'),
        payload.get('blocked'),
    ]
    for value in vals:
        try:
            if value is not None:
                return int(value)
        except Exception:
            continue
    return 0


def _winner_damage(evidence: dict) -> float:
    for key in ('hurt_winner_delta', 'winner_damage', 'winners_sacrificed_pnl'):
        try:
            if evidence.get(key) is not None:
                return float(evidence.get(key))
        except Exception:
            continue
    return 0.0


def _status_for_evidence(evidence: dict, payload: dict | None = None, contaminated: bool = False) -> str:
    days_seen = int(evidence.get('days_seen') or evidence.get('evidence_days') or 0)
    days_positive = int(evidence.get('days_positive') or 0)
    delta = float(evidence.get('estimated_delta_vs_actual') or evidence.get('pnl') or 0)
    hurt_winner_delta = _winner_damage(evidence)
    affected = _affected_trades(evidence, payload)
    if contaminated:
        return 'collecting_ops_contaminated'
    if days_seen >= 2 and affected < 8:
        return 'collecting_min_sample'
    if days_seen >= 2 and days_positive >= 2 and delta > 0 and hurt_winner_delta >= -100 and affected >= 8:
        return 'eligible_for_human_review'
    if days_seen >= 2 and days_positive >= 1 and delta > 0:
        return 'collecting_winner_damage'
    if days_seen >= 2 and delta <= 0:
        return 'rejected_no_edge_yet'
    return 'collecting'


def _lifecycle_for_status(status: str, evidence: dict, seen_days: list[str]) -> str:
    affected = _affected_trades(evidence or {})
    delta = float((evidence or {}).get('estimated_delta_vs_actual') or (evidence or {}).get('pnl') or 0)
    days = len(seen_days or [])
    if status == 'eligible_for_human_review':
        return 'proven'
    if status == 'rejected_no_edge_yet':
        return 'rejected'
    if status in ('collecting_ops_contaminated', 'collecting_min_sample'):
        return 'insufficient_data'
    if days >= 2 and affected >= 4 and delta > 0:
        return 'promising'
    if days >= 1:
        return 'watch_only'
    return 'insufficient_data'


def update_queue(day: str) -> dict:
    queue = _load_json(QUEUE_PATH, {'created_at': None, 'updated_at': None, 'items': {}, 'history': []})
    now = datetime.now(CT).isoformat(timespec='seconds')
    if not queue.get('created_at'):
        queue['created_at'] = now
    queue['updated_at'] = now
    items = queue.setdefault('items', {})

    sources = []
    pm = _load_json(os.path.join(OUT_DIR, f'postmortem_{day}.json'), {})
    engine = pm.get('engine_candidate_config') or _load_json(
        os.path.join(OUT_DIR, f'engine_candidate_config_{day}.json'), {}
    )
    exit_cfg = pm.get('exit_policy_candidate_config') or _load_json(
        os.path.join(OUT_DIR, f'exit_policy_candidate_config_{day}.json'), {}
    )
    staged = pm.get('candidate_config') or _load_json(
        os.path.join(OUT_DIR, f'candidate_config_{day}.json'), {}
    )
    split = _load_json(os.path.join(OUT_DIR, f'strategy_operations_split_{day}.json'), {}) or {}
    operations = split.get('operations') or {}
    contaminated = bool((operations.get('losses') or 0) or (operations.get('trades') or 0))

    for row in engine.get('candidates') or []:
        sources.append(('engine_gate', row.get('variant'), row, row.get('evidence') or {}))
    for row in engine.get('watch') or []:
        sources.append(('engine_gate', row.get('variant'), row, row.get('evidence') or {}))
    for row in exit_cfg.get('candidates') or []:
        sources.append(('exit_policy', row.get('policy'), row, row.get('evidence') or {}))
    for row in exit_cfg.get('watch') or []:
        sources.append(('exit_policy', row.get('policy'), row, row.get('evidence') or {}))
    for row in staged.get('changes') or []:
        name = row.get('candidate') or row.get('setup') or row.get('type')
        sources.append(('staged_config', name, row, row.get('evidence') or row))

    for kind, name, payload, evidence in sources:
        if not name:
            continue
        key = _candidate_key(kind, str(name))
        item = items.setdefault(key, {
            'key': key,
            'kind': kind,
            'name': name,
            'first_seen': day,
            'last_seen': day,
            'seen_days': [],
            'status': 'collecting',
            'promoted_manually': False,
            'killed_manually': False,
            'evidence': {},
            'latest_payload': {},
        })
        if day not in item['seen_days']:
            item['seen_days'].append(day)
        item['last_seen'] = day
        item['latest_payload'] = payload
        item['evidence'] = {
            **(evidence or {}),
            'affected_trades': _affected_trades(evidence or {}, payload or {}),
            'winner_damage': _winner_damage(evidence or {}),
            'ops_contaminated_day': contaminated,
        }
        if not item.get('promoted_manually') and not item.get('killed_manually'):
            item['status'] = _status_for_evidence(item['evidence'], payload, contaminated)
        item['lifecycle'] = _lifecycle_for_status(item.get('status'), item.get('evidence') or {}, item.get('seen_days') or [])

    queue['history'].append({
        'day': day,
        'updated_at': now,
        'item_count': len(items),
        'eligible': sum(1 for i in items.values() if i.get('status') == 'eligible_for_human_review'),
        'lifecycle_counts': {
            key: sum(1 for i in items.values() if i.get('lifecycle') == key)
            for key in ('proven', 'promising', 'watch_only', 'rejected', 'insufficient_data')
        },
    })
    queue['history'] = queue['history'][-50:]
    _save(queue)
    return queue


def render_queue_lines(day: str) -> list[str]:
    queue = update_queue(day)
    items = sorted(
        queue.get('items', {}).values(),
        key=lambda i: (i.get('status') != 'eligible_for_human_review', i.get('kind'), i.get('name')),
    )
    out = ['Promotion queue', '-' * 78]
    if not items:
        out.append('  No candidates are being tracked yet.')
        out.append('')
        return out
    for item in items[:12]:
        ev = item.get('evidence') or {}
        out.append(
            f"  - {item.get('key')}: status={item.get('status')} "
            f"lifecycle={item.get('lifecycle')} "
            f"seen_days={len(item.get('seen_days') or [])} "
            f"affected={int(ev.get('affected_trades') or 0)} "
            f"delta=${float(ev.get('estimated_delta_vs_actual') or ev.get('pnl') or 0):+.2f} "
            f"hurt_winners=${float(ev.get('winner_damage') or ev.get('hurt_winner_delta') or 0):+.2f}"
        )
    out.append('  Lifecycle: proven/promising/watch_only/rejected/insufficient_data. Promotion still requires eligibility and manual approval.')
    out.append('  Eligibility requires 2+ days, 8+ affected trades, positive delta, low winner damage, and no operations contamination.')
    out.append('  Manual approval is still required before any promoted item changes live config.')
    out.append('')
    return out


def write_queue(day: str) -> tuple[str, dict]:
    payload = update_queue(day)
    return QUEUE_PATH, payload
