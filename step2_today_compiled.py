"""Refresh and score the current live Step 2 variant from a same-day compiled tape.

This is the fast on-demand path for intraday parity checks:

1. materialize/refresh the same-day live event tape;
2. build or incrementally update the same-day decision tape and compiled arrays;
3. score the active live scoring profile against the compiled tape.

The expensive part is building the decision tape. Once the compiled manifest is
fresh, repeated scoring is normally seconds instead of a full event replay.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from types import SimpleNamespace

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

from datetime import datetime

import active_engine_baseline
import artifact_version_registry
import compiled_tape_lineage
import decision_tape_compiled
import execution_kernel
import refresh_intraday_step2
import step2_cache_layers
import step2_latency_model
import step2_parity_contract
import step2_warm_scorer
import unified_decision_ledger
import worker_policy


HERE = Path(__file__).resolve().parent
CT = ZoneInfo('America/Chicago')
DEFAULT_OUT_DIR = HERE / 'postmortem' / 'backtests' / 'step2_today_compiled'


def _read_json(path: Path) -> dict:
    with path.open('r', encoding='utf-8') as f:
        return json.load(f)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding='utf-8')
    os.replace(tmp, path)


def _compiled_name(args: argparse.Namespace, tickers: list[str]) -> str:
    if args.name:
        return args.name
    return f"compiled_step2_live_mockparity_{'-'.join(tickers)}_{args.day}_intraday"


def _manifest_path(args: argparse.Namespace, name: str) -> Path:
    if args.compiled_manifest:
        return Path(args.compiled_manifest).resolve()
    return (
        HERE
        / 'postmortem'
        / 'backtests'
        / 'compiled_decision_tapes'
        / name
        / 'manifest.json'
    )


def _manifest_code_stale(manifest_path: Path) -> dict:
    if not manifest_path.exists():
        return {
            'stale': True,
            'reason': 'compiled_manifest_missing',
            'path': str(manifest_path),
            'rebuild_strategy': 'full_signal_rebuild',
        }
    try:
        manifest = _read_json(manifest_path)
    except Exception as exc:
        return {
            'stale': True,
            'reason': 'compiled_manifest_unreadable',
            'error': repr(exc),
            'path': str(manifest_path),
            'rebuild_strategy': 'full_signal_rebuild',
        }
    manifest['manifest_path'] = str(manifest_path)
    lineage = compiled_tape_lineage.evaluate_manifest(
        manifest,
        code_hash_inputs=decision_tape_compiled.CODE_HASH_INPUTS,
    )
    mismatches = lineage.get('all_mismatches') or []
    rebuild_strategy = 'fresh' if not mismatches else str(lineage.get('recommended_action') or 'full_signal_rebuild')
    return {
        'stale': bool(mismatches),
        'reason': lineage.get('status') if mismatches else 'fresh',
        'path': str(manifest_path),
        'mismatches': mismatches[:10],
        'lineage': lineage,
        'rebuild_strategy': rebuild_strategy,
    }


def _cache_layer_report(args: argparse.Namespace, tickers: list[str], name: str) -> dict:
    try:
        return step2_cache_layers.report(
            args.day,
            tickers,
            feed=args.feed,
            quote_mode=args.quote_mode,
            btc_mode=args.btc_mode,
            indicator_mode=args.indicator_mode,
            compiled_name=name,
            trading_config=_load_trading_config(),
        )
    except Exception as exc:
        return {'error': repr(exc)}


def _write_cache_layer_report(args: argparse.Namespace, tickers: list[str], name: str) -> str | None:
    try:
        return step2_cache_layers.write_report(
            args.day,
            tickers,
            feed=args.feed,
            quote_mode=args.quote_mode,
            btc_mode=args.btc_mode,
            indicator_mode=args.indicator_mode,
            compiled_name=name,
            trading_config=_load_trading_config(),
        )
    except Exception:
        return None


def _refresh(args: argparse.Namespace, tickers: list[str], name: str, force_full: bool) -> dict:
    reuse_existing_signals = bool(
        getattr(args, 'reuse_existing_signals', False)
        or (
            not args.full_rebuild
            and getattr(args, 'rebuild_strategy', '') in (
                'reuse_existing_signals',
                'reuse_existing_signals_refresh_outcomes_compile',
                'refresh_outcomes_compile',
            )
        )
    )
    refresh_args = SimpleNamespace(
        day=args.day,
        tickers=tickers,
        feed=args.feed,
        quote_mode=args.quote_mode,
        btc_mode=args.btc_mode,
        indicator_mode=args.indicator_mode,
        workers=worker_policy.clamp_workers(args.workers),
        name=name,
        no_existing=args.no_existing,
        materialize_only=False,
        full_rebuild=bool(args.full_rebuild or (force_full and not reuse_existing_signals)),
        reuse_existing_signals=reuse_existing_signals,
        overlap_sec=args.overlap_sec,
        step2_latency_mode=args.step2_latency_mode,
        step2_latency_model=args.step2_latency_model,
        step2_latency_percentile=args.step2_latency_percentile,
        use_state_checkpoints=bool(getattr(args, 'use_state_checkpoints', True)),
    )
    return refresh_intraday_step2.refresh_once(refresh_args)


def _load_trading_config() -> dict:
    path = HERE / 'trading_config.json'
    try:
        return _read_json(path)
    except Exception:
        return {}


def _score_active(manifest_path: Path, start_balance: float, args: argparse.Namespace) -> dict:
    if not getattr(args, 'no_warm_scorer', False):
        warm_timeout = float(getattr(args, 'warm_scorer_timeout', 0.25))
        if getattr(args, 'write_unified_ledger', False):
            warm_timeout = max(warm_timeout, 30.0)
        warm = step2_warm_scorer.try_score_current_via_service(
            manifest_path,
            start_balance,
            host=getattr(args, 'warm_scorer_host', step2_warm_scorer.DEFAULT_HOST),
            port=int(getattr(args, 'warm_scorer_port', step2_warm_scorer.DEFAULT_PORT)),
            timeout=warm_timeout,
            write_ledger=bool(getattr(args, 'write_unified_ledger', False)),
        )
        if warm:
            return warm
    compiled = decision_tape_compiled.load_compiled(str(manifest_path), mmap=True)
    variant = active_engine_baseline.active_variant()
    config = _load_trading_config()
    sim_config = step2_parity_contract.sim_config(config)
    started = time.perf_counter()
    rows = decision_tape_compiled.simulate_variants(
        compiled,
        [variant],
        float(start_balance),
        gate=None,
        sim_config=sim_config,
    )
    elapsed = time.perf_counter() - started
    if not rows:
        raise RuntimeError('compiled Step 2 simulator unavailable')
    row = rows[0]
    full = row.get('decision_full') or {}
    active = active_engine_baseline.active_profile_payload()
    parity = step2_parity_contract.contract(config)
    execution_contract = execution_kernel.contract_from_config(config)
    payload = {
        'active_profile': active.get('profile'),
        'long_entry_quality_gate_used': False,
        'execution_kernel_contract': execution_contract,
        'execution_kernel_hash': execution_contract.get('execution_kernel_hash'),
        'step2_parity_contract': parity,
        'step2_parity_contract_hash': step2_parity_contract.contract_hash(parity),
        'compiled_manifest': str(manifest_path),
        'score_elapsed_sec': round(elapsed, 4),
        'result': full,
        'served_by': 'local_step2_today_compiled',
    }
    if getattr(args, 'write_unified_ledger', False):
        trace_started = time.perf_counter()
        trace = unified_decision_ledger.trace_step2_decisions(
            compiled,
            variant,
            float(start_balance),
            gate=None,
            sim_config=sim_config,
            include_rejected=True,
        )
        days = list((full.get('by_day') or {}).keys())
        day = days[0] if len(days) == 1 else args.day
        ledger_summary = unified_decision_ledger.write_step2_trace(day, trace, label='step2_current_trace')
        ledger_summary['trace_elapsed_sec'] = round(time.perf_counter() - trace_started, 4)
        payload['ledger_summary'] = ledger_summary
    return payload


def _selected_strategy(args: argparse.Namespace, refresh_payload: dict | None) -> str:
    if args.skip_refresh:
        return 'skip_refresh_score_only'
    refresh_payload = refresh_payload or {}
    rebuild_plan = refresh_payload.get('rebuild_plan') or {}
    if refresh_payload.get('event') == 'intraday_step2_refresh_skipped_score_only' or rebuild_plan.get('score_only'):
        return 'score_only_uncertified' if rebuild_plan.get('uncertified_score_only') else 'score_only'
    incremental = refresh_payload.get('incremental') or {}
    if incremental.get('compile_only') or rebuild_plan.get('recompile_only'):
        return 'recompile_only'
    if incremental.get('reuse_existing_signals'):
        return 'refresh_outcomes_compile'
    if incremental.get('enabled'):
        return 'incremental_signal_append_compile'
    return 'refresh_signal_tape_and_compile'


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Fast same-day compiled Step 2 active-variant scorer.')
    ap.add_argument('--day', default=datetime.now(CT).date().isoformat())
    ap.add_argument('--tickers', nargs='+', default=['CLSK', 'MARA', 'RIOT'])
    ap.add_argument('--feed', default='sip')
    ap.add_argument('--quote-mode', default='per-second')
    ap.add_argument('--btc-mode', default='bars')
    ap.add_argument('--indicator-mode', choices=['live', 'fast'], default='live')
    ap.add_argument('--workers', type=int, default=worker_policy.DEFAULT_MAX_WORKERS)
    ap.add_argument('--start-balance', type=float, default=100000.0)
    ap.add_argument('--name', default='')
    ap.add_argument('--compiled-manifest', default='')
    ap.add_argument('--skip-refresh', action='store_true',
                    help='Only score an existing compiled manifest. This is the seconds-fast rerun path.')
    ap.add_argument('--no-warm-scorer', action='store_true',
                    help='Do not attempt to use the persistent warm Step 2 scorer service.')
    ap.add_argument('--warm-scorer-host', default=step2_warm_scorer.DEFAULT_HOST)
    ap.add_argument('--warm-scorer-port', type=int, default=step2_warm_scorer.DEFAULT_PORT)
    ap.add_argument('--warm-scorer-timeout', type=float, default=0.25)
    ap.add_argument('--write-unified-ledger', action='store_true',
                    help='Write row-level Step 2 decisions to the unified decision ledger.')
    ap.add_argument('--full-rebuild', action='store_true',
                    help='Force a full same-day decision-tape rebuild before scoring.')
    ap.add_argument('--reuse-existing-signals', action='store_true',
                    help='Reuse existing decision-tape signal/features rows and refresh only outcomes.')
    ap.add_argument('--no-existing', action='store_true')
    ap.add_argument('--overlap-sec', type=int, default=120)
    ap.add_argument('--no-state-checkpoints', dest='use_state_checkpoints', action='store_false',
                    help='Disable exact replay state checkpoints for incremental signal scans.')
    ap.set_defaults(use_state_checkpoints=True)
    ap.add_argument('--step2-latency-mode', choices=['off', 'entry', 'entry-exit'], default='entry-exit')
    ap.add_argument('--step2-latency-model', default=step2_latency_model.DEFAULT_MODEL_PATH)
    ap.add_argument('--step2-latency-percentile', choices=['p50', 'p75', 'p95', 'default'], default='p75')
    ap.add_argument('--out-dir', default=str(DEFAULT_OUT_DIR))
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    tickers = [ticker.upper() for ticker in args.tickers]
    name = _compiled_name(args, tickers)
    manifest_path = _manifest_path(args, name)
    stale_before = _manifest_code_stale(manifest_path)
    cache_layers_before = _cache_layer_report(args, tickers, name)
    stale_after = stale_before
    cache_layers_after = cache_layers_before
    refresh_payload = None
    started = time.perf_counter()
    if not args.skip_refresh:
        args.rebuild_strategy = stale_before.get('rebuild_strategy')
        force_full = stale_before.get('rebuild_strategy') in ('full_signal_rebuild', 'full_rebuild')
        refresh_payload = _refresh(args, tickers, name, bool(force_full))
        manifest_path = Path(refresh_payload.get('compiled_manifest') or manifest_path).resolve()
        stale_after = _manifest_code_stale(manifest_path)
        cache_layers_after = _cache_layer_report(args, tickers, name)
    elif stale_before.get('stale'):
        raise SystemExit(
            'compiled manifest is missing/stale; rerun without --skip-refresh '
            'or pass --full-rebuild to rebuild it'
        )

    score = _score_active(manifest_path, args.start_balance, args)
    payload = {
        'event': 'step2_today_compiled_scored',
        'day': args.day,
        'tickers': tickers,
        'worker_policy': worker_policy.describe_policy(),
        'workers_requested': args.workers,
        'workers_effective': worker_policy.clamp_workers(args.workers),
        'started_at_ct': datetime.now(CT).isoformat(),
        'elapsed_sec': round(time.perf_counter() - started, 3),
        'cache_status_before_refresh': stale_before,
        'cache_status_after_refresh': stale_after,
        'cache_layers_before_refresh': cache_layers_before,
        'cache_layers_after_refresh': cache_layers_after,
        'cache_layer_report_path': _write_cache_layer_report(args, tickers, name),
        'why_rebuild': {
            'before_action': cache_layers_before.get('recommended_action'),
            'after_action': cache_layers_after.get('recommended_action'),
            'before_stale_layers': cache_layers_before.get('stale_layers'),
            'after_stale_layers': cache_layers_after.get('stale_layers'),
            'selected_strategy': _selected_strategy(args, refresh_payload),
        },
        'refresh': refresh_payload,
        'score': score,
    }
    out_path = Path(args.out_dir) / f'step2_today_compiled_{args.day}.json'
    _write_json(out_path, payload)
    payload['out_path'] = str(out_path.resolve())
    try:
        extra_artifacts = {
            'step2_today_compiled': str(out_path.resolve()),
            'compiled_manifest': str(manifest_path.resolve()),
            'compiled_chunk_manifest': str(manifest_path.resolve().parent / 'chunks' / 'chunk_manifest.json'),
        }
        if payload.get('cache_layer_report_path'):
            extra_artifacts['step2_cache_layer_report'] = str(payload['cache_layer_report_path'])
        if refresh_payload:
            rebuild_plan = refresh_payload.get('rebuild_plan') or {}
            if rebuild_plan.get('path'):
                extra_artifacts['step2_rebuild_plan'] = str(rebuild_plan['path'])
        registry_path = artifact_version_registry.write(args.day, tickers, extra_artifacts=extra_artifacts)
        payload['artifact_registry_path'] = registry_path
        payload['artifact_registry'] = _read_json(Path(registry_path))
        _write_json(out_path, payload)
    except Exception as exc:
        payload['artifact_registry_error'] = repr(exc)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
