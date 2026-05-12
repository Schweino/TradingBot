"""Check reusable replay artifact coverage and staleness."""
from __future__ import annotations

import argparse
import json
import os

import backtest_30d_engine as replay
import replay_artifacts


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Report replay artifact coverage and fingerprint freshness.')
    ap.add_argument('--start', required=True)
    ap.add_argument('--end', required=True)
    ap.add_argument('--tickers', nargs='+', default=replay.TICKERS)
    ap.add_argument('--feed', default='sip')
    ap.add_argument('--quote-mode', default='per-second')
    ap.add_argument('--btc-mode', default='bars')
    ap.add_argument('--cache-dir', default=replay.DEFAULT_CACHE_DIR)
    ap.add_argument('--prepared-cache-dir', default=replay.DEFAULT_PREPARED_DIR)
    ap.add_argument('--artifact-dir', default=replay_artifacts.DEFAULT_ARTIFACT_DIR)
    return ap.parse_args()


def _read_json(path: str) -> dict | None:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def main() -> int:
    args = parse_args()
    args.tickers = [ticker.upper() for ticker in args.tickers]
    key = replay_artifacts.key_from_args(args)
    rows = []
    for day in replay._market_days(replay._parse_day(args.start), replay._parse_day(args.end)):
        manifest = _read_json(replay_artifacts.manifest_path(args.artifact_dir, key, day))
        current = replay_artifacts.artifact_fingerprint(args, day)
        price_missing = []
        outcome_missing = []
        for ticker in args.tickers:
            if not os.path.exists(replay_artifacts.price_shard_path(args.artifact_dir, key, day, ticker)):
                price_missing.append(ticker)
            if not os.path.exists(replay_artifacts.outcome_shard_path(args.artifact_dir, key, day, ticker)):
                outcome_missing.append(ticker)
        rows.append({
            'day': day.isoformat(),
            'manifest': bool(manifest),
            'fresh': bool(manifest and manifest.get('fingerprint') == current.get('fingerprint')),
            'fingerprint': (manifest or {}).get('fingerprint'),
            'current_fingerprint': current.get('fingerprint'),
            'missing_price_shards': price_missing,
            'missing_outcome_shards': outcome_missing,
        })
    summary = {
        'days': len(rows),
        'fresh_days': sum(1 for row in rows if row['fresh']),
        'missing_manifest_days': [row['day'] for row in rows if not row['manifest']],
        'stale_days': [row['day'] for row in rows if row['manifest'] and not row['fresh']],
        'days_missing_price_shards': [row['day'] for row in rows if row['missing_price_shards']],
        'days_missing_outcome_shards': [row['day'] for row in rows if row['missing_outcome_shards']],
    }
    print(json.dumps({'summary': summary, 'rows': rows}, indent=2, sort_keys=True))
    return 0 if summary['fresh_days'] == summary['days'] and not summary['days_missing_outcome_shards'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
