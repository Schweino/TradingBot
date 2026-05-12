"""Run Step 3 finalist audits.

Step 3 is intentionally thin: Step 2 is the primary promotion scorer, and this
script only audits finalists against the exact mock/live executed-opportunity
surface. The old synthetic full-replay engine is no longer a promotion gate.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
import os
import subprocess
import sys
import time

import scoring_variant_lab as slow_lab
import scoring_profiles
import replay_artifacts
import step3_replay_cache
import multi_profile_full_replay
import variant_tournament_runner as tournament
import worker_policy


HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_IN = os.path.join(
    HERE,
    'postmortem',
    'backtests',
    'decision_tape_validation_2026-04-06_2026-05-01_top10000.json',
)
DEFAULT_OUT_DIR = os.path.join(HERE, 'postmortem', 'backtests', 'full_replay_finalists')
STEP3_RUN_INDEX = os.path.join(HERE, 'postmortem', 'backtests', 'step3_run_index.jsonl')
FINGERPRINT_FILES = [
    'mock_replay.py',
    'ws_scalp.py',
    'scoring_profiles.py',
    'scoring_variant_lab.py',
    'variant_tournament_runner.py',
    'full_replay_finalists.py',
    'trading_config.json',
]


def _load_decision_results(path: str, limit: int) -> tuple[dict, list[dict]]:
    with open(path, 'r', encoding='utf-8') as f:
        payload = json.load(f)
    rows = payload.get('results') or []
    if limit:
        rows = rows[:limit]
    return payload, rows


def _variant_from_row(row: dict) -> slow_lab.Variant:
    return slow_lab.Variant(
        row['variant'],
        dict(row.get('weights') or {}),
        float(row.get('bias') or 0.0),
    )


def _load_checkpoint(path: str) -> dict:
    if not os.path.exists(path):
        return {'results': []}
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def _write_checkpoint(path: str, payload: dict) -> None:
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    os.replace(tmp, path)


def _file_sha256(path: str) -> str | None:
    if not os.path.exists(path):
        return None
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def _run_fingerprint(args: argparse.Namespace, rows: list[dict]) -> dict:
    source_hash = _file_sha256(os.path.abspath(args.input))
    files = {
        name: _file_sha256(os.path.join(HERE, name))
        for name in FINGERPRINT_FILES
    }
    variant_hash = hashlib.sha256()
    for row in rows:
        variant_hash.update(str(row.get('variant') or '').encode('utf-8'))
        variant_hash.update(b'\0')
        variant_hash.update(json.dumps(row.get('weights') or {}, sort_keys=True).encode('utf-8'))
        variant_hash.update(b'\0')
        variant_hash.update(str(float(row.get('bias') or 0.0)).encode('utf-8'))
        variant_hash.update(b'\n')
    replay_config = {
        'start': args.start,
        'end': args.end,
        'train_start': args.train_start,
        'train_end': args.train_end,
        'test_start': args.test_start,
        'test_end': args.test_end,
        'tickers': args.tickers,
        'feed': args.feed,
        'quote_mode': args.quote_mode,
        'btc_mode': args.btc_mode,
        'start_balance': args.start_balance,
        'entry_gate_mode': args.entry_gate_mode,
        'entry_gate_threshold_pct': args.entry_gate_threshold_pct,
        'brs_failed_followthrough_after_sec': args.brs_failed_followthrough_after_sec,
        'brs_failed_followthrough_min_mfe_pct': args.brs_failed_followthrough_min_mfe_pct,
        'brs_failed_followthrough_max_pnl_pct': args.brs_failed_followthrough_max_pnl_pct,
        'brs_tp_multiplier': args.brs_tp_multiplier,
        'momentum_tp_multiplier': args.momentum_tp_multiplier,
        'high_conviction_tp_multiplier': args.high_conviction_tp_multiplier,
        'entry_confirmation_delay_sec': args.entry_confirmation_delay_sec,
        'entry_confirmation_mode': args.entry_confirmation_mode,
        'entry_confirmation_setup': args.entry_confirmation_setup,
        'entry_confirmation_min_favorable_pct': args.entry_confirmation_min_favorable_pct,
        'entry_confirmation_adverse_pct': args.entry_confirmation_adverse_pct,
        'entry_confirmation_session_phase': args.entry_confirmation_session_phase,
        'entry_confirmation_edge_threshold_pct': args.entry_confirmation_edge_threshold_pct,
        'entry_confirmation_rel_threshold_pct': args.entry_confirmation_rel_threshold_pct,
        'path_failure_after_sec': args.path_failure_after_sec,
        'path_failure_edge_threshold_pct': args.path_failure_edge_threshold_pct,
        'path_failure_action': args.path_failure_action,
        'path_failure_setup': args.path_failure_setup,
        'path_failure_session_phase': args.path_failure_session_phase,
        'path_failure_rel_threshold_pct': args.path_failure_rel_threshold_pct,
        'path_failure_window_sec': args.path_failure_window_sec,
        'path_failure_reduce_fraction': args.path_failure_reduce_fraction,
    }
    data_fingerprints = {}
    for day in tournament.replay._market_days(tournament.replay._parse_day(args.start), tournament.replay._parse_day(args.end)):
        data_fingerprints[day.isoformat()] = replay_artifacts.artifact_fingerprint(args, day).get('fingerprint')
    data_hash = hashlib.sha256(
        json.dumps(data_fingerprints, sort_keys=True, separators=(',', ':')).encode('utf-8')
    ).hexdigest()
    config_hash = hashlib.sha256(json.dumps(replay_config, sort_keys=True).encode('utf-8')).hexdigest()
    payload = {
        'schema_version': 1,
        'source_decision_validation': os.path.abspath(args.input),
        'source_sha256': source_hash,
        'selected_variant_count': len(rows),
        'selected_variants_sha256': variant_hash.hexdigest(),
        'replay_config': replay_config,
        'replay_config_sha256': config_hash,
        'market_data_fingerprints': data_fingerprints,
        'market_data_fingerprints_sha256': data_hash,
        'code_and_config_sha256': files,
    }
    payload['fingerprint_sha256'] = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('utf-8')
    ).hexdigest()
    return payload


def _write_manifest(path: str, fingerprint: dict) -> None:
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(fingerprint, f, indent=2, sort_keys=True)
    os.replace(tmp, path)


def _manifest_mismatches(existing: dict, current: dict) -> list[str]:
    checks = [
        ('source_sha256', existing.get('source_sha256'), current.get('source_sha256')),
        ('selected_variants_sha256', existing.get('selected_variants_sha256'), current.get('selected_variants_sha256')),
        ('replay_config_sha256', existing.get('replay_config_sha256'), current.get('replay_config_sha256')),
        ('market_data_fingerprints_sha256', existing.get('market_data_fingerprints_sha256'), current.get('market_data_fingerprints_sha256')),
        ('code_and_config_sha256', existing.get('code_and_config_sha256'), current.get('code_and_config_sha256')),
    ]
    return [name for name, old, new in checks if old != new]


def _preflight_artifacts(args: argparse.Namespace, run_dir: str) -> dict:
    out_path = os.path.join(run_dir, 'replay_artifact_status_preflight.json')
    cmd = [
        sys.executable,
        os.path.join(HERE, 'replay_artifact_status.py'),
        '--start', args.start,
        '--end', args.end,
        '--feed', args.feed,
        '--quote-mode', args.quote_mode,
        '--btc-mode', args.btc_mode,
        '--tickers',
        *args.tickers,
    ]
    started = time.perf_counter()
    proc = subprocess.run(cmd, cwd=HERE, capture_output=True, text=True)
    result = {
        'command': cmd,
        'returncode': proc.returncode,
        'elapsed_seconds': round(time.perf_counter() - started, 3),
        'stdout_tail': proc.stdout[-4000:],
        'stderr_tail': proc.stderr[-4000:],
    }
    try:
        parsed = json.loads(proc.stdout)
        result['status'] = parsed
    except Exception:
        result['status_parse_error'] = True
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, sort_keys=True)
    result['path'] = out_path
    return result


def _summary_key(result: dict) -> float:
    summary = ((result.get('full_replay') or {}).get('summary') or {})
    return float(summary.get('pnl') if summary.get('pnl') is not None else -10**12)


def _load_active_full_baseline(path: str | None) -> dict | None:
    if not path:
        return None
    with open(path, 'r', encoding='utf-8') as f:
        payload = json.load(f)
    summary = payload.get('summary')
    if summary:
        return {'source': os.path.abspath(path), 'summary': summary}
    full = payload.get('full_replay') or {}
    if full.get('summary'):
        return {'source': os.path.abspath(path), 'summary': full.get('summary')}
    leaderboard = payload.get('leaderboard') or payload.get('results') or []
    if leaderboard:
        row = leaderboard[0]
        summary = ((row.get('full_replay') or {}).get('summary') or {})
        if summary:
            return {'source': os.path.abspath(path), 'summary': summary, 'variant': row.get('variant')}
    raise RuntimeError(f'could not find full replay summary in active baseline JSON: {path}')


def _annotate_full_vs_active(result: dict, active_full: dict | None) -> None:
    summary = ((result.get('full_replay') or {}).get('summary') or {})
    active_summary = ((active_full or {}).get('summary') or {})
    if not summary or not active_summary:
        return
    active_pnl = float(active_summary.get('pnl') or 0.0)
    pnl = float(summary.get('pnl') or 0.0)
    result['active_full_pnl'] = active_pnl
    result['full_pnl_delta_vs_active'] = round(pnl - active_pnl, 2)
    result['beats_active_full'] = pnl > active_pnl


def _run_one_finalist(args: argparse.Namespace, row: dict, idx: int, run_dir: str,
                      checkpoint_path: str) -> dict:
    variant = _variant_from_row(row)
    profile_path = tournament._write_profile(variant, os.path.join(run_dir, 'profiles'), checkpoint_path)
    audit_root = os.path.join(run_dir, 'mock_audit', tournament._safe_name(variant.name))
    os.makedirs(audit_root, exist_ok=True)
    cmd = [
        sys.executable,
        os.path.join(HERE, 'mock_replay.py'),
        '--start', args.start,
        '--end', args.end,
        '--start-balance', str(float(args.start_balance)),
        '--scoring-profile', profile_path,
        '--out-dir', audit_root,
    ]
    if args.tickers:
        cmd.extend(['--tickers', *args.tickers])
    if not args.allow_missing_mock_days:
        cmd.append('--require-complete-days')
    started = time.perf_counter()
    proc = subprocess.run(cmd, cwd=HERE, capture_output=True, text=True)
    elapsed = round(time.perf_counter() - started, 3)
    summary = {}
    summary_path = None
    trades_csv = None
    try:
        parsed = json.loads(proc.stdout)
        summary_path = parsed.get('summary')
        trades_csv = parsed.get('trades_csv')
    except Exception:
        parsed = {}
    if summary_path and os.path.exists(summary_path):
        with open(summary_path, 'r', encoding='utf-8') as f:
            summary = json.load(f)
    elif parsed:
        summary = {k: v for k, v in parsed.items() if k not in ('summary', 'trades_csv')}
    full = {
        'method': 'step3_mock_live_audit',
        'step3_role': 'finalist_audit_only',
        'summary': summary,
        'summary_path': summary_path,
        'csv_path': trades_csv,
        'promotion_eligible': proc.returncode == 0 and not (summary.get('missing_mock_trade_days') or []),
        'elapsed_seconds': elapsed,
        'returncode': proc.returncode,
        'command': cmd,
        'stdout_tail': proc.stdout[-4000:],
        'stderr_tail': proc.stderr[-4000:],
    }
    return {
        'decision_rank': row.get('decision_rank', idx),
        'lab_rank': row.get('lab_rank'),
        'variant': row['variant'],
        'weights': dict(row.get('weights') or {}),
        'bias': float(row.get('bias') or 0.0),
        'profile': profile_path,
        'decision_full': row.get('decision_full'),
        'lab_pnl': row.get('lab_pnl'),
        'active_decision_pnl': row.get('active_decision_pnl'),
        'decision_pnl_delta_vs_active': row.get('decision_pnl_delta_vs_active'),
        'beats_active_decision': row.get('beats_active_decision'),
        'full_replay': full,
        'train_replay': None,
        'test_replay': None,
    }


def _write_retry_queue(path: str, results: list[dict]) -> None:
    failed = []
    for row in results:
        full = row.get('full_replay') or {}
        train = row.get('train_replay') or {}
        test = row.get('test_replay') or {}
        if row.get('error') or full.get('error') or train.get('error') or test.get('error'):
            failed.append({
                'variant': row.get('variant'),
                'error': row.get('error') or full.get('error') or train.get('error') or test.get('error'),
                'full_returncode': full.get('returncode'),
                'train_returncode': train.get('returncode'),
                'test_returncode': test.get('returncode'),
            })
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump({'schema_version': 1, 'failed': failed}, f, indent=2, sort_keys=True)
    os.replace(tmp, path)


def _step3_health(payload: dict) -> dict:
    results = payload.get('results') or []
    failed = []
    promotion_eligible = 0
    shared_cache_hits = 0
    full_cache_hits = 0
    missing_summaries = []
    for row in results:
        full = row.get('full_replay') or {}
        if row.get('error') or full.get('error') or full.get('returncode') not in (None, 0):
            failed.append(row.get('variant'))
        if full.get('promotion_eligible'):
            promotion_eligible += 1
        if full.get('reused_multi_profile_shared_cache'):
            shared_cache_hits += 1
        if full.get('reused_global_cache') or full.get('reused_existing_summary'):
            full_cache_hits += 1
        if not full.get('summary'):
            missing_summaries.append(row.get('variant'))
    parity = payload.get('multi_profile_parity') or {}
    batches = payload.get('multi_profile_batches') or []
    parity_cache_hits = 0
    parity_checked = 0
    for item in ([parity] if parity else []) + [b.get('parity') for b in batches if b.get('parity')]:
        for check in item.get('checks') or []:
            parity_checked += 1
            if check.get('single_reused_parity_cache'):
                parity_cache_hits += 1
    return {
        'schema_version': 1,
        'completed': len(results),
        'failed_count': len(failed),
        'failed_variants': failed,
        'missing_summary_count': len(missing_summaries),
        'missing_summary_variants': missing_summaries,
        'promotion_eligible_count': promotion_eligible,
        'shared_cache_hits': shared_cache_hits,
        'full_cache_hits': full_cache_hits,
        'parity_checked': parity_checked,
        'parity_cache_hits': parity_cache_hits,
        'multi_profile_parity_passed': parity.get('passed') if parity else None,
    }


def _write_promotion_packet(path: str, payload: dict) -> dict:
    leaderboard = payload.get('leaderboard') or []
    leader = leaderboard[0] if leaderboard else None
    packet = {
        'schema_version': 1,
        'source_decision_validation': payload.get('source_decision_validation'),
        'run_manifest': payload.get('run_manifest'),
        'fingerprint_sha256': (payload.get('fingerprint') or {}).get('fingerprint_sha256'),
        'config': payload.get('config'),
        'step3_health': payload.get('step3_health') or _step3_health(payload),
        'active_engine_baseline': payload.get('active_engine_baseline'),
        'active_full_baseline': payload.get('active_full_baseline'),
        'multi_profile_parity': payload.get('multi_profile_parity'),
        'leader': leader,
        'top10': leaderboard[:10],
    }
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(packet, f, indent=2, sort_keys=True)
    os.replace(tmp, path)
    return packet


def _append_run_index(index_path: str, payload: dict, summary_path: str, promotion_packet_path: str) -> None:
    os.makedirs(os.path.dirname(index_path), exist_ok=True)
    leaderboard = payload.get('leaderboard') or []
    leader = leaderboard[0] if leaderboard else {}
    full = (leader.get('full_replay') or {}) if isinstance(leader, dict) else {}
    summary = full.get('summary') or {}
    row = {
        'schema_version': 1,
        'name': (payload.get('config') or {}).get('name'),
        'summary_path': summary_path,
        'promotion_packet_path': promotion_packet_path,
        'completed': payload.get('completed'),
        'elapsed_seconds': payload.get('elapsed_seconds'),
        'leader_variant': leader.get('variant') if isinstance(leader, dict) else None,
        'leader_pnl': summary.get('pnl'),
        'leader_trades': summary.get('trades'),
        'promotion_eligible': full.get('promotion_eligible'),
        'parity_passed': (payload.get('multi_profile_parity') or {}).get('passed'),
        'fingerprint_sha256': (payload.get('fingerprint') or {}).get('fingerprint_sha256'),
    }
    with open(index_path, 'a', encoding='utf-8') as f:
        f.write(json.dumps(row, sort_keys=True) + '\n')


def _run_multi_profile_finalists(args: argparse.Namespace, pending: list[tuple[int, dict]],
                                 run_dir: str) -> tuple[list[dict], dict | None]:
    raise RuntimeError(
        'Legacy Step 3 multi-profile/synthetic replay has been removed from the promotion funnel. '
        'Run Step 2 for scoring, then Step 3 mock/live audit for only the finalists.'
    )
    if args.train_start or args.train_end or args.test_start or args.test_end:
        raise RuntimeError('--multi-profile-shared-pass currently supports the full Step 3 window only, not train/test splits.')
    if args.multi_profile_day_shards and not (args.multi_profile_parity_check and args.multi_profile_require_parity):
        raise RuntimeError(
            '--multi-profile-day-shards must be run with --multi-profile-parity-check N '
            'and --multi-profile-require-parity. Day sharding is only accepted when it proves exact parity.'
        )
    if args.multi_profile_day_shards:
        days = tournament.replay._market_days(tournament.replay._parse_day(args.start), tournament.replay._parse_day(args.end))
        if len(days) > 1 and not args.force_multi_day_day_shards:
            raise RuntimeError(
                '--multi-profile-day-shards is limited to one market day unless --force-multi-day-day-shards is set. '
                'The non-day-sharded shared pass is the exact-safe multi-day fast path.'
            )
    multi_args = argparse.Namespace(**vars(args))
    multi_args.out_dir = os.path.join(run_dir, 'multi_profile_shared_pass')
    multi_args.validation_mode = 'finalist'
    multi_args.indicator_mode = 'fast'
    multi_args.use_prepared_events = True
    multi_args.write_prepared_events = False
    multi_args.refresh_prepared_events = False
    multi_args.refresh = False
    multi_args.max_pages = 1000
    multi_args.write_opportunity_ledger = False
    multi_args.opportunity_dir = None
    multi_args.resume_days = False
    multi_args.rebuild_missing_opportunities = False
    multi_args.replay_regime_long_throttle = args.entry_gate_mode or 'off'
    multi_args.replay_regime_threshold_pct = args.entry_gate_threshold_pct or 0.0
    multi_args.profile_replay_timing = False
    os.makedirs(multi_args.out_dir, exist_ok=True)
    profile_dir = os.path.join(multi_args.out_dir, 'generated_profiles')
    states = []
    specs = []
    row_by_variant = {}
    idx_by_variant = {}
    source_rows = []
    for idx, row in pending:
        variant = _variant_from_row(row)
        profile_path = tournament._write_profile(variant, profile_dir, os.path.abspath(args.input))
        profile = scoring_profiles.load_profile(profile_path)
        states.append(multi_profile_full_replay.ProfileState(
            variant=variant.name,
            profile_path=profile_path,
            profile=profile,
            balance=float(args.start_balance),
        ))
        specs.append({
            'variant': variant.name,
            'profile_path': profile_path,
            'profile': profile,
        })
        row_by_variant[variant.name] = row
        idx_by_variant[variant.name] = idx
        source_rows.append(row)
    multi_args.multi_profile_day_shards = bool(args.multi_profile_day_shards)
    multi_args.multi_profile_day_workers = int(args.multi_profile_day_workers or 1)
    multi_args.multi_profile_resume_days = bool(args.multi_profile_resume_days)
    multi_args.force_multi_day_day_shards = bool(args.force_multi_day_day_shards)
    multi_args.multi_profile_shared_cache = bool(args.multi_profile_shared_cache)
    multi_args.multi_profile_shared_cache_dir = args.multi_profile_shared_cache_dir
    multi_args.multi_profile_parity_cache = bool(args.multi_profile_parity_cache)
    multi_args.multi_profile_parity_cache_dir = args.multi_profile_parity_cache_dir
    multi_args.brs_tp_multiplier = float(getattr(args, 'brs_tp_multiplier', 1.0) or 1.0)
    multi_args.momentum_tp_multiplier = float(getattr(args, 'momentum_tp_multiplier', 1.0) or 1.0)
    multi_args.high_conviction_tp_multiplier = float(getattr(args, 'high_conviction_tp_multiplier', 1.0) or 1.0)
    multi_args.entry_confirmation_delay_sec = int(float(getattr(args, 'entry_confirmation_delay_sec', 0.0) or 0.0))
    multi_args.entry_confirmation_mode = getattr(args, 'entry_confirmation_mode', 'off') or 'off'
    multi_args.entry_confirmation_setup = getattr(args, 'entry_confirmation_setup', 'all') or 'all'
    multi_args.entry_confirmation_min_favorable_pct = float(getattr(args, 'entry_confirmation_min_favorable_pct', 0.0) or 0.0)
    multi_args.entry_confirmation_adverse_pct = float(getattr(args, 'entry_confirmation_adverse_pct', 0.0) or 0.0)
    multi_args.entry_confirmation_session_phase = getattr(args, 'entry_confirmation_session_phase', 'all') or 'all'
    multi_args.entry_confirmation_edge_threshold_pct = getattr(args, 'entry_confirmation_edge_threshold_pct', None)
    multi_args.entry_confirmation_rel_threshold_pct = getattr(args, 'entry_confirmation_rel_threshold_pct', None)
    multi_args.path_failure_after_sec = int(float(getattr(args, 'path_failure_after_sec', 0.0) or 0.0))
    multi_args.path_failure_edge_threshold_pct = float(getattr(args, 'path_failure_edge_threshold_pct', 0.0) or 0.0)
    multi_args.path_failure_action = getattr(args, 'path_failure_action', 'off') or 'off'
    multi_args.path_failure_setup = getattr(args, 'path_failure_setup', 'all') or 'all'
    multi_args.path_failure_session_phase = getattr(args, 'path_failure_session_phase', 'all') or 'all'
    multi_args.path_failure_rel_threshold_pct = getattr(args, 'path_failure_rel_threshold_pct', None)
    multi_args.path_failure_window_sec = int(float(getattr(args, 'path_failure_window_sec', 0.0) or 0.0))
    multi_args.path_failure_reduce_fraction = float(getattr(args, 'path_failure_reduce_fraction', 0.0) or 0.0)
    method = (
        'multi_profile_day_sharded_shared_market_indicator_pass'
        if multi_args.multi_profile_day_shards
        else 'multi_profile_shared_market_indicator_pass'
    )
    cached = multi_profile_full_replay._restore_shared_cache(multi_args, specs, method)
    if cached is not None:
        multi_results = cached
    elif multi_args.multi_profile_day_shards:
        multi_results = multi_profile_full_replay._run_multi_day_sharded(multi_args, specs)
        multi_profile_full_replay._store_shared_cache(multi_args, specs, method, multi_results)
    else:
        multi_results = multi_profile_full_replay._run_multi(multi_args, states)
        multi_profile_full_replay._store_shared_cache(multi_args, specs, method, multi_results)
    parity = None
    if args.multi_profile_parity_check:
        multi_args.parity_check = min(int(args.multi_profile_parity_check), len(source_rows))
        parity = multi_profile_full_replay._parity_check(multi_args, source_rows, multi_results)
        if args.multi_profile_require_parity and not parity.get('passed'):
            raise RuntimeError('Multi-profile parity check failed; refusing to use shared-pass results.')
    promotion_eligible = bool(parity and parity.get('passed'))
    converted = []
    for multi_row in multi_results:
        variant_name = multi_row['variant']
        source_row = row_by_variant[variant_name]
        converted.append({
            'decision_rank': source_row.get('decision_rank', idx_by_variant.get(variant_name)),
            'lab_rank': source_row.get('lab_rank'),
            'variant': variant_name,
            'weights': dict(source_row.get('weights') or {}),
            'bias': float(source_row.get('bias') or 0.0),
            'profile': multi_row.get('profile'),
            'decision_full': source_row.get('decision_full'),
            'lab_pnl': source_row.get('lab_pnl'),
            'active_decision_pnl': source_row.get('active_decision_pnl'),
            'decision_pnl_delta_vs_active': source_row.get('decision_pnl_delta_vs_active'),
            'beats_active_decision': source_row.get('beats_active_decision'),
            'full_replay': {
                'label': 'full',
                'start': args.start,
                'end': args.end,
                'variant': variant_name,
                'profile': multi_row.get('profile'),
                'out_dir': os.path.dirname(multi_row.get('summary_path') or ''),
                'elapsed_seconds': None,
                'returncode': 0,
                'multi_profile_shared_pass': True,
                'multi_profile_day_shards': bool(args.multi_profile_day_shards),
                'promotion_eligible': promotion_eligible,
                'summary': multi_row.get('summary'),
                'summary_path': multi_row.get('summary_path'),
                'csv_path': multi_row.get('csv'),
                'hashes': multi_row.get('hashes'),
                'reused_multi_profile_shared_cache': bool(multi_row.get('reused_multi_profile_shared_cache')),
            },
            'train_replay': None,
            'test_replay': None,
        })
    return converted, parity


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Step 3 mock/live finalist audit.')
    ap.add_argument('--input', default=DEFAULT_IN)
    ap.add_argument('--out-dir', default=DEFAULT_OUT_DIR)
    ap.add_argument('--name', default='top500_2026-04-06_2026-05-01')
    ap.add_argument('--start', default='2026-04-06')
    ap.add_argument('--end', default='2026-05-01')
    ap.add_argument('--train-start', default=None)
    ap.add_argument('--train-end', default=None)
    ap.add_argument('--test-start', default=None)
    ap.add_argument('--test-end', default=None)
    ap.add_argument('--tickers', nargs='+', default=tournament.replay.TICKERS)
    ap.add_argument('--feed', default='sip')
    ap.add_argument('--quote-mode', default='per-second')
    ap.add_argument('--btc-mode', default='bars')
    ap.add_argument('--start-balance', type=float, default=100000.0)
    ap.add_argument('--limit', type=int, default=500)
    ap.add_argument('--max-new', type=int, default=0, help='Run at most this many new finalists this invocation. 0 means no cap.')
    ap.add_argument('--full-workers', type=int, default=worker_policy.DEFAULT_MAX_WORKERS)
    ap.add_argument('--model-workers', type=int, default=1,
                    help='Run this many finalist models concurrently. Results are unchanged; this only schedules independent full replays in parallel.')
    ap.add_argument('--auto-worker-budget', action='store_true',
                    help='Automatically cap nested model/day workers to avoid CPU oversubscription. Results are unchanged.')
    ap.add_argument('--target-total-workers', type=int, default=worker_policy.DEFAULT_MAX_WORKERS,
                    help='Total concurrent replay worker budget used with --auto-worker-budget.')
    ap.add_argument('--low-priority-replays', action='store_true',
                    help='Launch replay subprocesses below normal priority on Windows so the desktop remains responsive.')
    ap.add_argument('--global-full-replay-cache', action='store_true',
                    help='Use the global exact Step 3 replay cache keyed by variant/config/code/data fingerprints.')
    ap.add_argument('--full-replay-cache-dir', default=step3_replay_cache.DEFAULT_CACHE_DIR)
    ap.add_argument('--reuse-existing-summaries', action='store_true',
                    help='Reuse an exact existing per-finalist full replay summary in the run directory instead of rerunning it.')
    ap.add_argument('--resume-day-checkpoints', action='store_true',
                    help='Pass --resume-days into each full replay. Use with a stable run directory/fingerprint to resume interrupted exact day replays.')
    ap.add_argument('--skip-preflight-artifacts', action='store_true',
                    help='Skip prepared replay artifact status preflight.')
    ap.add_argument('--require-fresh-artifacts', action='store_true',
                    help='Stop if replay artifact preflight reports missing/stale artifacts. Default records the status but allows full replay build-on-miss behavior.')
    ap.add_argument('--allow-manifest-mismatch', action='store_true',
                    help='Allow --resume with a changed source/config/code fingerprint. Default is to stop so stale Step 3 results are not mixed.')
    ap.add_argument('--progress-batch-size', type=int, default=10,
                    help='Write a compact batch readout every N newly completed finalists. 0 disables batch readouts.')
    ap.add_argument('--dry-run', action='store_true',
                    help='Validate inputs, fingerprints, manifests, preflight, and scheduling without launching finalist replays.')
    ap.add_argument('--allow-missing-mock-days', action='store_true',
                    help='Allow audit summaries when postmortem/trades corpora are missing. Promotion audits should leave this off.')
    ap.add_argument('--multi-profile-shared-pass', action='store_true',
                    help='Use the opt-in shared market/indicator pass for all pending finalists. Requires parity validation before promotion use.')
    ap.add_argument('--multi-profile-parity-check', type=int, default=0,
                    help='With --multi-profile-shared-pass, compare this many profiles against the existing single-profile replay.')
    ap.add_argument('--multi-profile-require-parity', action='store_true',
                    help='With --multi-profile-shared-pass, fail if the parity check does not pass.')
    ap.add_argument('--multi-profile-day-shards', action='store_true',
                    help='With --multi-profile-shared-pass, run independent shared-pass day shards and compound results in date order.')
    ap.add_argument('--multi-profile-day-workers', type=int, default=1,
                    help='With --multi-profile-day-shards, run this many market days concurrently.')
    ap.add_argument('--multi-profile-resume-days', action='store_true',
                    help='With --multi-profile-day-shards, write/reuse exact day-shard checkpoints.')
    ap.add_argument('--force-multi-day-day-shards', action='store_true',
                    help='Allow multi-day day-sharded runs. Without this, day shards are limited to one market day because exact multi-day parity can fail on compounding details.')
    ap.add_argument('--multi-profile-shared-cache', action='store_true',
                    help='With --multi-profile-shared-pass, reuse/store exact shared-pass outputs keyed by profile/config/code/data fingerprints.')
    ap.add_argument('--multi-profile-shared-cache-dir', default=multi_profile_full_replay.DEFAULT_SHARED_CACHE_DIR)
    ap.add_argument('--multi-profile-parity-cache', action='store_true',
                    help='With --multi-profile-shared-pass, reuse/store exact single-profile parity replay outputs.')
    ap.add_argument('--multi-profile-parity-cache-dir', default=multi_profile_full_replay.DEFAULT_PARITY_CACHE_DIR)
    ap.add_argument('--multi-profile-batch-size', type=int, default=0,
                    help='With --multi-profile-shared-pass, run pending finalists in batches of this size. 0 means one batch.')
    ap.add_argument('--strict-safe-mode', action='store_true',
                    help='Enable the safest Step 3 settings: parity required, shared/parity caches, preflight, promotion packet, and no forced day shards.')
    ap.add_argument('--write-promotion-packet', action='store_true',
                    help='Write promotion_packet.json with leader, parity, hashes, and run fingerprints.')
    ap.add_argument('--write-run-index', action='store_true',
                    help='Append a compact row to the Step 3 run index JSONL.')
    ap.add_argument('--run-index-path', default=STEP3_RUN_INDEX)
    ap.add_argument('--write-run-complete-marker', action='store_true',
                    help='Write RUN_COMPLETE.json only after final checkpoint/promotion packet writes succeed.')
    ap.add_argument('--entry-gate-mode', default=None)
    ap.add_argument('--entry-gate-threshold-pct', type=float, default=None)
    ap.add_argument('--brs-failed-followthrough-after-sec', type=float, default=0.0)
    ap.add_argument('--brs-failed-followthrough-min-mfe-pct', type=float, default=0.12)
    ap.add_argument('--brs-failed-followthrough-max-pnl-pct', type=float, default=0.0)
    ap.add_argument('--brs-tp-multiplier', type=float, default=1.0)
    ap.add_argument('--momentum-tp-multiplier', type=float, default=1.0)
    ap.add_argument('--high-conviction-tp-multiplier', type=float, default=1.0)
    ap.add_argument('--entry-confirmation-delay-sec', type=int, default=0)
    ap.add_argument('--entry-confirmation-mode', choices=['off', 'confirm', 'skip_adverse', 'flip_adverse', 'skip_path_failure'], default='off')
    ap.add_argument('--entry-confirmation-setup', default='all')
    ap.add_argument('--entry-confirmation-min-favorable-pct', type=float, default=0.0)
    ap.add_argument('--entry-confirmation-adverse-pct', type=float, default=0.0)
    ap.add_argument('--entry-confirmation-session-phase', default='all')
    ap.add_argument('--entry-confirmation-edge-threshold-pct', type=float, default=None)
    ap.add_argument('--entry-confirmation-rel-threshold-pct', type=float, default=None)
    ap.add_argument('--path-failure-after-sec', type=int, default=0)
    ap.add_argument('--path-failure-edge-threshold-pct', type=float, default=0.0)
    ap.add_argument('--path-failure-action', choices=['off', 'exit', 'flip', 'reduce'], default='off')
    ap.add_argument('--path-failure-setup', default='all')
    ap.add_argument('--path-failure-session-phase', default='all')
    ap.add_argument('--path-failure-rel-threshold-pct', type=float, default=None)
    ap.add_argument('--path-failure-window-sec', type=int, default=0)
    ap.add_argument('--path-failure-reduce-fraction', type=float, default=0.0)
    ap.add_argument('--active-full-baseline-json', default=None,
                    help='Optional existing active-engine full replay summary JSON for full P/L deltas.')
    ap.add_argument('--no-active-baseline', action='store_true',
                    help='Do not copy Step 1/2 active baseline metadata into this Step 3 summary.')
    ap.add_argument('--resume', action='store_true')
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    args.tickers = [t.upper() for t in args.tickers]
    args.full_workers = worker_policy.clamp_workers(args.full_workers)
    args.model_workers = worker_policy.clamp_workers(args.model_workers)
    args.target_total_workers = worker_policy.clamp_workers(args.target_total_workers)
    args.multi_profile_day_workers = worker_policy.clamp_workers(args.multi_profile_day_workers)
    args.cache_dir = getattr(args, 'cache_dir', tournament.replay.DEFAULT_CACHE_DIR)
    args.prepared_cache_dir = getattr(args, 'prepared_cache_dir', tournament.replay.DEFAULT_PREPARED_DIR)
    args.artifact_dir = getattr(args, 'artifact_dir', replay_artifacts.DEFAULT_ARTIFACT_DIR)
    args.reuse_full_replay_summary = bool(args.reuse_existing_summaries)
    args.skip_preflight_artifacts = True
    if args.strict_safe_mode:
        args.multi_profile_require_parity = True
        if args.multi_profile_shared_pass and not args.multi_profile_parity_check:
            args.multi_profile_parity_check = min(5, int(args.limit or 5))
        args.multi_profile_shared_cache = True
        args.multi_profile_parity_cache = True
        args.write_promotion_packet = True
        args.write_run_index = True
        args.write_run_complete_marker = True
        args.force_multi_day_day_shards = False
    if args.auto_worker_budget:
        args.model_workers = max(1, int(args.model_workers or 1))
        args.full_workers = max(1, min(int(args.full_workers or 1), int(args.target_total_workers) // args.model_workers))
    source_payload, rows = _load_decision_results(args.input, args.limit)
    run_dir = os.path.join(args.out_dir, tournament._safe_name(args.name))
    os.makedirs(run_dir, exist_ok=True)
    checkpoint_path = os.path.join(run_dir, 'full_replay_finalists_summary.json')
    manifest_path = os.path.join(run_dir, 'run_manifest.json')
    retry_queue_path = os.path.join(run_dir, 'retry_queue.json')
    fingerprint = _run_fingerprint(args, rows)
    args.step3_run_fingerprint_sha256 = fingerprint.get('fingerprint_sha256')
    args.step3_code_config_sha256 = hashlib.sha256(
        json.dumps(fingerprint.get('code_and_config_sha256') or {}, sort_keys=True, separators=(',', ':')).encode('utf-8')
    ).hexdigest()
    args.step3_data_fingerprint_sha256 = fingerprint.get('market_data_fingerprints_sha256')
    if (args.resume or args.reuse_existing_summaries) and not os.path.exists(manifest_path) and not args.allow_manifest_mismatch:
        raise RuntimeError(
            'Refusing to resume/reuse Step 3 artifacts without a run manifest. '
            'Use a new --name, rerun without --resume/--reuse-existing-summaries, '
            'or pass --allow-manifest-mismatch if you intentionally accept older artifacts.'
        )
    if (args.resume or args.reuse_existing_summaries) and os.path.exists(manifest_path):
        with open(manifest_path, 'r', encoding='utf-8') as f:
            existing_manifest = json.load(f)
        mismatches = _manifest_mismatches(existing_manifest, fingerprint)
        if mismatches and not args.allow_manifest_mismatch:
            raise RuntimeError(
                'Refusing to resume Step 3 with a changed fingerprint. '
                f'Changed fields: {", ".join(mismatches)}. '
                'Use a new --name or pass --allow-manifest-mismatch if you intentionally want to mix artifacts.'
            )
    _write_manifest(manifest_path, fingerprint)
    checkpoint = _load_checkpoint(checkpoint_path) if args.resume else {'results': []}
    done = {row.get('variant') for row in checkpoint.get('results', []) if row.get('variant')}
    active_baseline = None if args.no_active_baseline else source_payload.get('active_engine_baseline')
    active_full = _load_active_full_baseline(args.active_full_baseline_json)
    if active_full:
        for existing in checkpoint.get('results', []):
            _annotate_full_vs_active(existing, active_full)

    payload = {
        'schema_version': 1,
        'source_decision_validation': args.input,
        'source_variants': len(source_payload.get('results') or []),
        'requested_limit': args.limit,
        'config': vars(args),
        'run_manifest': manifest_path,
        'fingerprint': fingerprint,
        'active_engine_baseline': active_baseline,
        'active_full_baseline': active_full,
        'results': checkpoint.get('results', []),
        'method_note': 'Full engine replay with prepared events, finalist mode, and fast indicators. This is the backtest verdict stage.',
    }
    if not args.skip_preflight_artifacts:
        payload['preflight_artifacts'] = _preflight_artifacts(args, run_dir)
        _write_checkpoint(checkpoint_path, payload)
        if payload['preflight_artifacts'].get('returncode') != 0 and args.require_fresh_artifacts:
            raise RuntimeError(
                'Replay artifact preflight reported missing or stale artifacts. '
                f"Details: {payload['preflight_artifacts'].get('path')}. "
                'Run the artifact fill pipeline first, or omit --require-fresh-artifacts to allow full replay build-on-miss behavior.'
            )

    started = time.perf_counter()
    ran_new = 0
    total_target = len(rows)
    pending = [(idx, row) for idx, row in enumerate(rows, 1) if row['variant'] not in done]
    if args.max_new:
        pending = pending[:args.max_new]
    if args.dry_run:
        pending = []

    def record_result(result: dict) -> None:
        nonlocal ran_new
        _annotate_full_vs_active(result, active_full)
        payload['results'].append(result)
        ran_new += 1
        done.add(result['variant'])
        payload['elapsed_seconds'] = round(time.perf_counter() - started, 3)
        payload['completed'] = len(payload['results'])
        payload['remaining_in_limit'] = total_target - len(done)
        payload['leaderboard'] = sorted(payload['results'], key=_summary_key, reverse=True)[:50]
        _write_checkpoint(checkpoint_path, payload)
        _write_retry_queue(retry_queue_path, payload['results'])
        elapsed = time.perf_counter() - started
        avg = elapsed / ran_new if ran_new else None
        eta = avg * (total_target - len(done)) if avg else None
        full = result.get('full_replay') or {}
        summary = (full.get('summary') or {})
        print(json.dumps({
            'event': 'progress',
            'ran_new': ran_new,
            'completed_total': len(payload['results']),
            'target': total_target,
            'variant': result['variant'],
            'full_pnl': summary.get('pnl'),
            'full_pnl_delta_vs_active': result.get('full_pnl_delta_vs_active'),
            'beats_active_full': result.get('beats_active_full'),
            'full_trades': summary.get('trades'),
            'reused_existing_summary': bool(full.get('reused_existing_summary')),
            'reused_global_cache': bool(full.get('reused_global_cache')),
            'reused_multi_profile_shared_cache': bool(full.get('reused_multi_profile_shared_cache')),
            'promotion_eligible': full.get('promotion_eligible'),
            'elapsed_seconds': round(elapsed, 2),
            'avg_seconds_per_model': round(avg, 2) if avg else None,
            'eta_seconds_for_remaining_limit': round(eta, 2) if eta else None,
        }, sort_keys=True), flush=True)
        if args.progress_batch_size and ran_new % args.progress_batch_size == 0:
            print(json.dumps({
                'event': 'batch_readout',
                'batch_size': args.progress_batch_size,
                'ran_new': ran_new,
                'leader': {
                    'variant': (payload['leaderboard'][0] or {}).get('variant') if payload['leaderboard'] else None,
                    'full_pnl': (((payload['leaderboard'][0] or {}).get('full_replay') or {}).get('summary') or {}).get('pnl') if payload['leaderboard'] else None,
                    'full_trades': (((payload['leaderboard'][0] or {}).get('full_replay') or {}).get('summary') or {}).get('trades') if payload['leaderboard'] else None,
                },
                'top10': [
                    {
                        'rank': i + 1,
                        'variant': row.get('variant'),
                        'full_pnl': ((row.get('full_replay') or {}).get('summary') or {}).get('pnl'),
                        'full_trades': ((row.get('full_replay') or {}).get('summary') or {}).get('trades'),
                    }
                    for i, row in enumerate(payload['leaderboard'][:10])
                ],
            }, sort_keys=True), flush=True)

    if args.multi_profile_shared_pass and pending:
        batch_size = int(args.multi_profile_batch_size or 0)
        batches = [pending] if batch_size <= 0 else [pending[i:i + batch_size] for i in range(0, len(pending), batch_size)]
        payload['multi_profile_batches'] = []
        for batch_idx, batch in enumerate(batches, 1):
            multi_results, parity = _run_multi_profile_finalists(args, batch, run_dir)
            batch_payload = {
                'batch': batch_idx,
                'size': len(batch),
                'variants': [row.get('variant') for _idx, row in batch],
                'parity': parity,
            }
            payload['multi_profile_batches'].append(batch_payload)
            if parity is not None:
                payload['multi_profile_parity'] = parity
                _write_checkpoint(checkpoint_path, payload)
            for result in multi_results:
                result['multi_profile_batch'] = batch_idx
                record_result(result)
    elif args.model_workers <= 1:
        for idx, row in pending:
            record_result(_run_one_finalist(args, row, idx, run_dir, checkpoint_path))
    else:
        with ThreadPoolExecutor(max_workers=args.model_workers) as executor:
            futures = {
                executor.submit(_run_one_finalist, args, row, idx, run_dir, checkpoint_path): row.get('variant')
                for idx, row in pending
            }
            for future in as_completed(futures):
                try:
                    record_result(future.result())
                except Exception as exc:
                    result = {
                        'variant': futures[future],
                        'error': 'full replay finalist worker failed',
                        'exception': repr(exc),
                    }
                    payload['results'].append(result)
                    ran_new += 1
                    payload['elapsed_seconds'] = round(time.perf_counter() - started, 3)
                    payload['completed'] = len(payload['results'])
                    payload['leaderboard'] = sorted(payload['results'], key=_summary_key, reverse=True)[:50]
                    _write_checkpoint(checkpoint_path, payload)
                    print(json.dumps(result, sort_keys=True), flush=True)

    payload['elapsed_seconds'] = round(time.perf_counter() - started, 3)
    payload['completed'] = len(payload['results'])
    payload['leaderboard'] = sorted(payload['results'], key=_summary_key, reverse=True)[:50]
    payload['step3_health'] = _step3_health(payload)
    promotion_packet_path = os.path.join(run_dir, 'promotion_packet.json')
    if args.write_promotion_packet:
        _write_promotion_packet(promotion_packet_path, payload)
    if args.write_run_index:
        _append_run_index(args.run_index_path, payload, checkpoint_path, promotion_packet_path if args.write_promotion_packet else '')
    if args.write_run_complete_marker:
        complete_path = os.path.join(run_dir, 'RUN_COMPLETE.json')
        tmp = complete_path + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump({
                'schema_version': 1,
                'summary_path': checkpoint_path,
                'promotion_packet_path': promotion_packet_path if args.write_promotion_packet else None,
                'completed': payload['completed'],
                'elapsed_seconds': payload['elapsed_seconds'],
                'step3_health': payload['step3_health'],
            }, f, indent=2, sort_keys=True)
        os.replace(tmp, complete_path)
    _write_retry_queue(retry_queue_path, payload['results'])
    _write_checkpoint(checkpoint_path, payload)
    print(json.dumps({
        'event': 'done',
        'out': checkpoint_path,
        'ran_new': ran_new,
        'completed': len(payload['results']),
        'target': total_target,
        'dry_run': bool(args.dry_run),
        'top10': [
            {
                'full_rank': i + 1,
                'decision_rank': row.get('decision_rank'),
                'lab_rank': row.get('lab_rank'),
                'variant': row.get('variant'),
                'full_pnl': ((row.get('full_replay') or {}).get('summary') or {}).get('pnl'),
                'full_pnl_delta_vs_active': row.get('full_pnl_delta_vs_active'),
                'beats_active_full': row.get('beats_active_full'),
                'full_trades': ((row.get('full_replay') or {}).get('summary') or {}).get('trades'),
                'full_win_rate_pct': ((row.get('full_replay') or {}).get('summary') or {}).get('win_rate_pct'),
                'decision_pnl': ((row.get('decision_full') or {}).get('pnl')),
                'lab_pnl': row.get('lab_pnl'),
            }
            for i, row in enumerate(payload['leaderboard'][:10])
        ],
    }, indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == '__main__':
    import canonical_command_registry as _canonical_commands
    _canonical_commands.enforce_direct_script_allowed(__file__)
    raise SystemExit(main())
