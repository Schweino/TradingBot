from __future__ import annotations

from output_paths import output_path

import json
import os
from collections import Counter
from datetime import datetime
from typing import Any, Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo


HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = output_path('postmortem')
CT = ZoneInfo('America/Chicago')


def _read_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return default


def _read_jsonl(path: str) -> list[dict]:
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


def _day_from_ts(ts: Optional[float]) -> Optional[str]:
    try:
        return datetime.fromtimestamp(float(ts), CT).date().isoformat()
    except Exception:
        return None


def _rows_for_day(path: str, day: str, ts_field: str = 'created_at') -> list[dict]:
    return [r for r in _read_jsonl(path) if _day_from_ts(r.get(ts_field)) == day]


def _compact_signal(sig: Optional[dict]) -> dict:
    if not sig:
        return {'decision': 'skip'}
    quality = sig.get('signal_quality') or {}
    return {
        'decision': 'enter',
        'side': sig.get('side'),
        'score': sig.get('score'),
        'conviction': sig.get('conviction'),
        'setup_type': sig.get('setup_type'),
        'reasons': sig.get('reasons') or [],
        'btc_context': sig.get('btc_context') or {},
        'components': quality.get('score_components') or sig.get('components') or {},
        'setup_tags': quality.get('setup_tags') or [],
        'miner_basket': sig.get('miner_basket') or {},
    }


def _current_engine_decision(ticker: str, indicators: dict, btc: dict,
                             miner_indicators: Optional[dict] = None) -> tuple[dict, Optional[str]]:
    try:
        import ws_scalp
        sig = ws_scalp.detect_signal(
            ticker,
            indicators or {},
            btc or {},
            miner_indicators=miner_indicators or None,
        )
        return _compact_signal(sig), None
    except Exception as e:
        return {'decision': 'error'}, str(e)


def _sample_confidence(sample: dict) -> dict:
    indicators = sample.get('indicators') or {}
    btc = sample.get('btc') or {}
    required = ('price', 'ema_stack', 'mom_15s', 'mom_60s', 'vwap_dist_sigma')
    present = sum(1 for k in required if indicators.get(k) is not None)
    btc_present = sum(1 for k in ('ready', 'stack', 'regime', 'mom_60s') if btc.get(k) is not None)
    score = 20 + present * 10 + btc_present * 8
    if sample.get('ticker'):
        score += 8
    if sample.get('score') is not None:
        score += 8
    if sample.get('source') == 'trade':
        score += 8
    score = max(0, min(100, score))
    if score >= 80:
        label = 'high'
    elif score >= 55:
        label = 'medium'
    else:
        label = 'low'
    missing = [k for k in required if indicators.get(k) is None]
    return {
        'score': score,
        'label': label,
        'missing_indicator_keys': missing,
        'btc_keys_present': btc_present,
        'note': 'Replay confidence grades snapshot completeness, not whether the trade idea was good.',
    }


def _sample_from_trade(row: dict) -> dict:
    thesis = row.get('entry_thesis') or {}
    audit = row.get('decision_audit') or thesis.get('decision_audit') or {}
    indicators = (
        row.get('indicators')
        or thesis.get('indicators')
        or audit.get('indicators')
        or {}
    )
    btc = (
        row.get('btc_indicators')
        or thesis.get('btc_indicators')
        or audit.get('btc_indicators')
        or thesis.get('btc_context')
        or audit.get('btc_context')
        or row.get('btc')
        or row.get('btc_context')
        or {}
    )
    return {
        'source': 'trade',
        'historical_decision': 'enter',
        'ticker': row.get('ticker'),
        'side': row.get('side'),
        'created_at': row.get('opened_at') or row.get('closed_at'),
        'pnl': row.get('pnl'),
        'reason': row.get('reason'),
        'setup_type': row.get('setup_type') or row.get('setup') or (row.get('entry_thesis') or {}).get('setup_type'),
        'score': row.get('entry_score') if row.get('entry_score') is not None else row.get('score'),
        'indicators': indicators,
        'btc': btc,
        'components': (
            (thesis.get('signal_quality') or {}).get('score_components')
            or (audit.get('signal_quality') or {}).get('score_components')
            or row.get('components')
            or {}
        ),
    }


def _sample_from_row(row: dict, source: str, historical_decision: str) -> dict:
    return {
        'source': source,
        'historical_decision': historical_decision,
        'ticker': row.get('ticker'),
        'side': row.get('side'),
        'created_at': row.get('created_at'),
        'pnl': None,
        'reason': row.get('reason'),
        'setup_type': row.get('setup_type'),
        'score': row.get('score'),
        'indicators': row.get('indicators') or row.get('snapshot') or {},
        'btc': row.get('btc_context') or row.get('btc') or row.get('btc_indicators') or {},
        'components': row.get('components') or {},
    }


def _sample_ts(sample: dict) -> Optional[float]:
    try:
        ts = sample.get('created_at')
        return float(ts) if ts is not None else None
    except Exception:
        return None


def _miner_context_for_sample(sample: dict, samples: list[dict], max_age_sec: int = 5) -> dict:
    ticker = sample.get('ticker')
    created = _sample_ts(sample)
    out = {}
    if ticker and sample.get('indicators'):
        out[str(ticker)] = sample.get('indicators') or {}
    if created is None:
        return out
    for other in samples:
        sym = other.get('ticker')
        if not sym or sym == ticker or sym in out:
            continue
        ots = _sample_ts(other)
        if ots is None or abs(ots - created) > max_age_sec:
            continue
        ind = other.get('indicators') or {}
        if ind:
            out[str(sym)] = ind
    return out


def build_replay(day: str, limit: int = 2000) -> dict:
    pm = _read_json(os.path.join(OUT_DIR, f'postmortem_{day}.json'), {}) or {}
    skipped_path = os.path.join(OUT_DIR, 'skipped_signals', f'skipped_signals_{day}.jsonl')
    near_path = os.path.join(OUT_DIR, 'near_signals', f'near_signals_{day}.jsonl')
    samples = []
    samples.extend(_sample_from_trade(t) for t in pm.get('tape') or [])
    samples.extend(_sample_from_row(r, 'skipped_signal', 'skip') for r in _rows_for_day(skipped_path, day))
    samples.extend(_sample_from_row(r, 'near_signal', 'near') for r in _rows_for_day(near_path, day))
    rows = []
    counts = Counter()
    errors = Counter()
    for sample in samples[:limit]:
        ticker = sample.get('ticker')
        miner_context = _miner_context_for_sample(sample, samples)
        current, err = _current_engine_decision(
            str(ticker or ''),
            sample.get('indicators') or {},
            sample.get('btc') or {},
            miner_indicators=miner_context,
        )
        confidence = _sample_confidence(sample)
        if err:
            errors[err] += 1
        hist = sample.get('historical_decision')
        cur_decision = current.get('decision')
        if hist == 'enter' and cur_decision == 'enter':
            label = 'still_enters'
        elif hist == 'enter':
            label = 'would_skip_now'
        elif hist in ('skip', 'near') and cur_decision == 'enter':
            label = 'would_enter_now'
        else:
            label = 'still_skips_or_near'
        counts[label] += 1
        counts[f'confidence_{confidence["label"]}'] += 1
        rows.append({
            'source': sample.get('source'),
            'historical_decision': hist,
            'current_label': label,
            'ticker': ticker,
            'historical_side': sample.get('side'),
            'current_decision': current,
            'replay_confidence': confidence,
            'miner_context_symbols': sorted(miner_context),
            'historical_score': sample.get('score'),
            'historical_setup': sample.get('setup_type'),
            'historical_components': sample.get('components') or {},
            'pnl': sample.get('pnl'),
            'reason': sample.get('reason'),
            'created_at': sample.get('created_at'),
        })
    return {
        'day': day,
        'created_at_ct': datetime.now(CT).isoformat(timespec='seconds'),
        'mode': 'diagnostic_replay_only_no_orders',
        'sample_count': len(samples),
        'processed_count': len(rows),
        'counts': dict(counts),
        'confidence_summary': {
            'high': counts.get('confidence_high', 0),
            'medium': counts.get('confidence_medium', 0),
            'low': counts.get('confidence_low', 0),
        },
        'errors': [{'error': k, 'count': v} for k, v in errors.most_common(10)],
        'rows': rows,
        'deduction': 'Replays captured trade/skip/near-signal snapshots through the current ws_scalp detector. Use as a research comparator, not a fill-accurate backtest.',
    }


def write_replay(day: str) -> tuple[str, dict]:
    payload = build_replay(day)
    path = os.path.join(OUT_DIR, f'ws_scalp_replay_{day}.json')
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, default=str)
    return path, payload


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser(description='Replay captured snapshots through current ws_scalp logic.')
    ap.add_argument('day')
    ap.add_argument('--json', action='store_true')
    args = ap.parse_args()
    path, payload = write_replay(args.day)
    if args.json:
        print(json.dumps(payload, indent=2, default=str))
    else:
        print(path)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
