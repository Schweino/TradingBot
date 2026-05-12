"""Adaptive Step 2 variant hunter.

This skips Step 1 and searches directly on the compiled Step 2 surface in small
batches. Each round scores a 500-variant batch, keeps elites, mutates around the
best shapes, and stops once enough variants beat the active Step 2 baseline by
the requested margin.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import random
import time
from pathlib import Path

import active_engine_baseline
import baseline_drift_sentinel
import candidate_decision_brief
import candidate_profile_schema
import candidate_robustness_report
import certify_step2_cache
import compiled_tape_lineage
import decision_tape_compiled
import market_data_integrity_gate
import scoring_variant_lab as lab
import step2_cache_catalog
import step2_evaluation_envelope
import step2_latency_model
import step2_manifest_resolver
import step2_parity_contract
import step2_quote_aware_guard
import step2_score_cache
import tournament_safety


HERE = Path(__file__).resolve().parent
DEFAULT_CACHE_TICKERS = ['CLSK', 'MARA', 'RIOT']
DEFAULT_OUT = HERE / 'postmortem' / 'backtests' / 'step2_adaptive_hunter'
FEATURES = [
    'ema', 'vwap', 'momentum', 'btc', 'relative', 'miner', 'burst', 'flow_contra',
    'btc_chop', 'exec_penalty', 'open_phase', 'midday_phase', 'riot_short_penalty',
    'riot_long_penalty', 'vwap_sigma_ext', 'btc_mom_abs', 'flow_pressure_abs',
    'setup_btc_relative_strength', 'setup_momentum_breakout', 'setup_trend_pullback',
    'setup_flow_exhaustion_fade', 'setup_vwap_reclaim_breakdown',
    'brs_weak_followthrough', 'brs_open_risk', 'brs_bear_normal_risk',
    'brs_open_weak_followthrough', 'timeout_decay_risk',
    'brs_open_continuation_quality', 'brs_normal_continuation_quality',
    'medium_brs_penalty', 'brs_open_medium_risk', 'brs_open_inversion_risk',
    'brs_open_non_riot_inversion_risk', 'brs_clsk_mara_inversion_risk',
    'brs_open_btc_neutral_risk', 'brs_open_low_range_risk',
    'brs_open_low_range_020', 'brs_open_low_range_025', 'brs_open_low_range_035',
    'brs_open_low_range_040', 'brs_open_low_range_045',
    'brs_open_side_bad_range_025', 'brs_open_side_bad_range_031',
    'brs_open_side_bad_range_040', 'brs_wide_spread_risk',
    'brs_open_book_worsening_risk', 'brs_open_flow_quote_failure',
    'brs_open_flow_book_failure', 'brs_open_flow_book_failure_loose',
    'brs_open_session_neutral_failure', 'brs_open_range_neutral_mom_failure',
    'brs_open_three_tape_failure', 'brs_alignment_failure',
    'brs_open_alignment_failure', 'brs_normal_alignment_failure',
]


def _load_trading_config() -> dict:
    path = HERE / 'trading_config.json'
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return {}


def _sim_config(args: argparse.Namespace) -> dict:
    sim = step2_parity_contract.sim_config(_load_trading_config())
    sim['max_trades_per_day'] = int(args.max_trades_per_day)
    sim['max_trades_per_ticker_day'] = int(args.max_trades_per_ticker_day)
    return sim


def _target_pnl(active_pnl: float, beat_pct: float) -> float:
    improvement = abs(float(active_pnl or 0.0)) * float(beat_pct or 0.0) / 100.0
    return round(float(active_pnl or 0.0) + improvement, 6)


def _target_config(args: argparse.Namespace, active_pnl: float) -> tuple[float, str]:
    if getattr(args, 'target_pnl', None) is not None:
        return round(float(args.target_pnl), 6), 'absolute_target_pnl'
    return _target_pnl(active_pnl, float(args.beat_pct)), 'active_pnl + abs(active_pnl) * beat_pct / 100'


def _delta_pct(candidate_pnl: float, active_pnl: float) -> float | None:
    if abs(float(active_pnl or 0.0)) <= 1e-12:
        return None
    return round((float(candidate_pnl) - float(active_pnl)) / abs(float(active_pnl)) * 100.0, 4)


def _promotion_surface_ok(data_integrity: dict, cache_certification: dict) -> bool:
    return bool(data_integrity.get('promotion_safe') and cache_certification.get('ok'))


def _non_promotable_reason(surface_ok: bool, data_integrity: dict, cache_certification: dict,
                           recommended: list[dict] | None = None) -> str | None:
    if not surface_ok:
        return (
            'market_data_integrity_failed'
            if not data_integrity.get('promotion_safe')
            else 'step2_cache_certification_failed'
        )
    if recommended is not None and not recommended:
        return 'no_recommended_candidate'
    return None


def _variant(name: str, weights: dict[str, float], bias: float = 0.0) -> lab.Variant:
    clean = {k: round(float(v), 6) for k, v in weights.items() if abs(float(v)) > 1e-9}
    return lab.Variant(name, clean, round(float(bias), 6))


def _active_seed() -> lab.Variant:
    active = active_engine_baseline.reference_variant()
    return _variant(active.name, dict(active.weights), active.bias)


def _load_seed_rows(paths: list[str], limit: int) -> list[lab.Variant]:
    seeds: list[lab.Variant] = []
    seen = set()
    for raw in paths:
        path = Path(raw)
        if not path.exists():
            continue
        payload = json.loads(path.read_text(encoding='utf-8'))
        rows = payload.get('candidates') or payload.get('results') or payload.get('leaderboard') or []
        rows = sorted(rows, key=lambda r: float(r.get('step2_pnl') or r.get('pnl') or -1e18), reverse=True)
        for row in rows[:limit]:
            if not row.get('weights'):
                continue
            name = str(row.get('variant') or row.get('name') or f'seed_{len(seeds)}')
            key = tournament_safety.model_id(name, row.get('weights') or {}, float(row.get('bias') or 0.0))
            if key in seen:
                continue
            seen.add(key)
            seeds.append(_variant(name, dict(row.get('weights') or {}), float(row.get('bias') or 0.0)))
    return seeds


def _random_sparse(rng: random.Random, idx: int) -> lab.Variant:
    weights = {}
    core = ['vwap', 'relative', 'btc_chop', 'exec_penalty', 'ema', 'momentum', 'btc']
    for key in core:
        weights[key] = rng.choice([-2.5, -2.0, -1.5, -1.0, -0.5, 0.0, 0.25, 0.5, 0.75, 1.0])
    for key in rng.sample([f for f in FEATURES if f not in core], rng.randint(2, 9)):
        weights[key] = rng.choice([-2.0, -1.25, -0.75, -0.35, 0.35, 0.75, 1.25, 2.0])
    return _variant(f'adapt_random_{idx:09d}', weights, rng.choice([0.0, -0.25, 0.25, -0.5, 0.5]))


def _mutate(seed: lab.Variant, rng: random.Random, idx: int, scale: float,
            weight_limit: float = 4.0) -> lab.Variant:
    weights = dict(seed.weights)
    keys = list(set(weights) | set(rng.sample(FEATURES, rng.randint(1, 5))))
    for key in keys:
        if rng.random() < 0.78:
            cur = float(weights.get(key, 0.0))
            cur += rng.gauss(0.0, scale)
            if rng.random() < 0.08:
                cur *= rng.choice([-1.0, 0.0, 1.5])
            cur = max(-float(weight_limit), min(float(weight_limit), cur))
            weights[key] = cur
    if rng.random() < 0.25:
        weights[rng.choice(FEATURES)] = rng.choice([-3.0, -2.0, -1.0, 1.0, 2.0, 3.0])
    bias = float(seed.bias or 0.0) + (rng.gauss(0.0, scale / 4.0) if rng.random() < 0.25 else 0.0)
    return _variant(f'adapt_mut_{idx:09d}_{seed.name[:32]}', weights, bias)


def _score(compiled: dict, variants: list[lab.Variant], args: argparse.Namespace,
           lineage_by_name: dict[str, dict] | None = None) -> list[dict]:
    scorer = step2_score_cache.score_variants_cached if getattr(args, 'use_score_cache', True) else decision_tape_compiled.simulate_variants
    kwargs = {
        'gate': None,
        'sim_config': _sim_config(args),
    }
    if getattr(args, 'use_score_cache', True):
        kwargs['cache_db'] = getattr(args, 'score_cache_db', str(step2_score_cache.DEFAULT_CACHE_DB))
    rows = scorer(
        compiled,
        variants,
        float(args.start_balance),
        **kwargs,
    )
    if rows is None:
        raise RuntimeError('compiled Step 2 simulation unavailable')
    lineage_by_name = lineage_by_name or {}
    out = []
    for row in rows:
        full = row.get('decision_full') or {}
        lineage = lineage_by_name.get(str(row.get('variant') or ''), {})
        out.append({
            'variant': row.get('variant'),
            'weights': row.get('weights') or {},
            'bias': float(row.get('bias') or 0.0),
            'lineage': lineage,
            'parent_variant': lineage.get('parent_variant') if isinstance(lineage, dict) else '',
            'parent_behavior_key': lineage.get('parent_behavior_key') if isinstance(lineage, dict) else '',
            'mutation_lane': lineage.get('mutation_lane') if isinstance(lineage, dict) else '',
            'mutation_reason': lineage.get('mutation_reason') if isinstance(lineage, dict) else '',
            'worker_role': lineage.get('worker_role') if isinstance(lineage, dict) else '',
            'generation': lineage.get('generation') if isinstance(lineage, dict) else 0,
            'mutation_scale': lineage.get('mutation_scale') if isinstance(lineage, dict) else 0.0,
            'step2_pnl': float(full.get('pnl') or 0.0),
            'step2_trades': int(full.get('trades') or 0),
            'step2_win_rate_pct': full.get('win_rate_pct'),
            'by_ticker': full.get('by_ticker'),
            'by_day': full.get('by_day'),
            'by_side': full.get('by_side'),
            'skipped': full.get('skipped'),
            'exit_replay_model': row.get('exit_replay_model') or full.get('exit_replay_model'),
            'required_exit_replay_model': row.get('required_exit_replay_model'),
            'score_cache': row.get('score_cache') or {},
        })
    return out


def _behavior_key_from_row(row: dict) -> str:
    return tournament_safety.stable_json_hash(
        {
            'pnl': round(float(row.get('step2_pnl') or row.get('pnl') or 0.0), 2),
            'trades': int(row.get('step2_trades') or 0),
            'by_day': row.get('by_day') or {},
            'by_ticker': row.get('by_ticker') or {},
        },
        length=32,
    )


def _attach_lineage(variant: lab.Variant, lineage: dict) -> lab.Variant:
    return variant


def _lineage_for_seed(
    *,
    source_row: dict | None,
    lane: str,
    reason: str,
    batch_idx: int,
    scale: float,
    worker_role: str = 'adaptive',
) -> dict:
    source_row = source_row or {}
    parent_variant = str(source_row.get('variant') or source_row.get('name') or 'active_seed')
    parent_behavior = str(source_row.get('behavior_key') or _behavior_key_from_row(source_row) if source_row else '')
    parent_lineage = source_row.get('lineage') if isinstance(source_row.get('lineage'), dict) else {}
    ancestor_path = list(parent_lineage.get('ancestor_path') or [])
    if parent_variant:
        ancestor_path.append(parent_variant)
    return {
        'parent_variant': parent_variant,
        'parent_behavior_key': parent_behavior,
        'mutation_lane': lane,
        'mutation_reason': reason,
        'worker_role': worker_role,
        'generation': int(parent_lineage.get('generation') or 0) + 1,
        'route_seed': source_row.get('route_key') or source_row.get('route_seed') or 'unrouted',
        'mutation_scale': round(float(scale), 6),
        'ancestor_path': ancestor_path[-12:],
        'cycle': batch_idx,
    }


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding='utf-8')
    os.replace(tmp, path)


def _read_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding='utf-8-sig'))
    except Exception:
        return {}


def _lineage_validation(compiled: dict) -> dict:
    manifest = compiled.get('manifest') if isinstance(compiled.get('manifest'), dict) else {}
    lineage = manifest.get('lineage_validation') if isinstance(manifest.get('lineage_validation'), dict) else {}
    return lineage


def _cache_certification_ok(compiled: dict) -> bool:
    lineage = _lineage_validation(compiled)
    return bool(lineage.get('status') == 'CERTIFIED_MATCH' and lineage.get('certified') is True)


def _source_days_from_manifest(manifest: dict) -> list[str]:
    day_map = manifest.get('day_map')
    if isinstance(day_map, dict):
        return sorted(str(day) for day in day_map)
    days = manifest.get('source_days')
    if isinstance(days, list):
        return sorted(str(day) for day in days)
    return []


def _infer_certification_day(args: argparse.Namespace, manifest: dict) -> str:
    if getattr(args, 'certify_day', ''):
        return str(args.certify_day)
    days = _source_days_from_manifest(manifest)
    return days[0] if len(days) == 1 else ''


def _compact_certification(payload: dict) -> dict:
    if not isinstance(payload, dict):
        return {}
    return {
        'ok': payload.get('ok'),
        'source': payload.get('source') or 'step2_adaptive_hunter',
        'day': payload.get('day'),
        'tickers': payload.get('tickers'),
        'selected_action': payload.get('selected_action'),
        'initial_action': payload.get('initial_action'),
        'reason': payload.get('reason'),
        'failure_reasons': payload.get('failure_reasons') or [],
        'certification_hash': payload.get('certification_hash'),
        'output': payload.get('output') or {},
        'compiled_manifest': payload.get('compiled_manifest') or {},
        'quote_aware_outcome_gate': payload.get('quote_aware_outcome_gate') or {},
        'before': payload.get('before') or {},
        'after': payload.get('after') or {},
    }


def _lineage_only_certification(manifest_path: Path, manifest: dict) -> dict:
    try:
        manifest_for_lineage = dict(manifest)
        manifest_for_lineage['manifest_path'] = str(manifest_path)
        lineage = compiled_tape_lineage.evaluate_manifest(
            manifest_for_lineage,
            code_hash_inputs=decision_tape_compiled.CODE_HASH_INPUTS,
        )
    except Exception as exc:
        lineage = {
            'status': 'LINEAGE_EVALUATION_FAILED',
            'certified': False,
            'quick_score_allowed': False,
            'rebuild_required': True,
            'recommended_action': 'full_signal_rebuild',
            'error': repr(exc),
        }
    return {
        'ok': bool(lineage.get('status') == 'CERTIFIED_MATCH' and lineage.get('certified') is True),
        'source': 'step2_adaptive_hunter_lineage_only_certification',
        'day': None,
        'tickers': [],
        'selected_action': 'lineage_only_check',
        'initial_action': lineage.get('recommended_action'),
        'reason': 'multi_day_or_unidentified_cache_not_auto_rebuildable',
        'failure_reasons': [] if lineage.get('certified') else ['compiled_tape_not_certified'],
        'compiled_manifest': {'path': str(manifest_path), 'exists': manifest_path.exists()},
        'before': {
            'lineage_status': lineage.get('status'),
            'lineage_certified': lineage.get('certified'),
            'lineage_rebuild_required': lineage.get('rebuild_required'),
            'lineage_quick_score_allowed': lineage.get('quick_score_allowed'),
            'recommended_action': lineage.get('recommended_action'),
        },
        'after': {
            'lineage_status': lineage.get('status'),
            'lineage_certified': lineage.get('certified'),
            'lineage_rebuild_required': lineage.get('rebuild_required'),
            'lineage_quick_score_allowed': lineage.get('quick_score_allowed'),
            'recommended_action': lineage.get('recommended_action'),
        },
    }


def _resolve_compiled_cache_for_hunter(args: argparse.Namespace, out_dir: Path) -> dict:
    explicit = str(getattr(args, 'compiled_decision_tape', '') or '').strip()
    if explicit and (not getattr(args, 'cache_resolver', True) or getattr(args, 'allow_uncertified_cache', False)):
        manifest_path = Path(explicit).resolve()
        args.compiled_decision_tape = str(manifest_path)
        return {
            'ok': True,
            'source': 'explicit_compiled_decision_tape',
            'manifest_path': str(manifest_path),
            'requested': {
                'tickers': getattr(args, 'cache_tickers', None) or [],
                'day': getattr(args, 'cache_day', ''),
                'start': getattr(args, 'cache_start', ''),
                'end': getattr(args, 'cache_end', ''),
                'require_certified': not bool(getattr(args, 'allow_uncertified_cache', False)),
            },
            'blockers': [],
        }
    if not getattr(args, 'cache_resolver', True):
        return {
            'ok': False,
            'source': 'step2_manifest_resolver',
            'manifest_path': '',
            'blockers': ['compiled_decision_tape_missing', 'cache_resolver_disabled'],
            'certification_hint': {
                'supported': False,
                'reason': 'pass --compiled-decision-tape or enable cache resolver',
            },
        }

    tickers = [
        str(ticker).upper()
        for ticker in (getattr(args, 'cache_tickers', None) or getattr(args, 'certify_tickers', None) or DEFAULT_CACHE_TICKERS)
        if str(ticker).strip()
    ] or DEFAULT_CACHE_TICKERS
    cache_day = str(getattr(args, 'cache_day', '') or getattr(args, 'certify_day', '') or '')
    cache_start = str(getattr(args, 'cache_start', '') or '') or cache_day
    cache_end = str(getattr(args, 'cache_end', '') or '') or cache_day
    catalog_payload = step2_cache_catalog.resolve(
        tickers=tickers,
        day=cache_day,
        start='' if cache_day else cache_start,
        end='' if cache_day else cache_end,
        require_certified=not bool(getattr(args, 'allow_uncertified_cache', False)),
    )
    if catalog_payload.get('ok') and catalog_payload.get('manifest_path'):
        receipt_path = out_dir / 'cache_manifest_resolution.json'
        out_dir.mkdir(parents=True, exist_ok=True)
        _write_json(receipt_path, catalog_payload)
        args.compiled_decision_tape = str(catalog_payload['manifest_path'])
        compact = step2_cache_catalog._compact_resolution(catalog_payload)
        compact['hunter_cache_manifest_resolution_path'] = str(receipt_path.resolve())
        return compact
    if catalog_payload.get('blockers') and cache_day and not (getattr(args, 'cache_start', '') or getattr(args, 'cache_end', '')):
        receipt_path = out_dir / 'cache_manifest_resolution.json'
        out_dir.mkdir(parents=True, exist_ok=True)
        _write_json(receipt_path, catalog_payload)
        compact = step2_cache_catalog._compact_resolution(catalog_payload)
        compact['hunter_cache_manifest_resolution_path'] = str(receipt_path.resolve())
        return compact
    payload = step2_manifest_resolver.resolve_best_manifest(
        compiled_manifest=explicit or None,
        tickers=tickers,
        start=str(getattr(args, 'cache_start', '') or '') or cache_day or step2_manifest_resolver.DEFAULT_START,
        end=str(getattr(args, 'cache_end', '') or '') or cache_day or step2_manifest_resolver.DEFAULT_END,
        require_certified=not bool(getattr(args, 'allow_uncertified_cache', False)),
        write_certification=True,
        write_receipt=True,
        label=str(getattr(args, 'name', '') or 'adaptive_step2_hunt'),
        feed=getattr(args, 'certify_feed', 'sip'),
        quote_mode=getattr(args, 'certify_quote_mode', 'per-second'),
        btc_mode=getattr(args, 'certify_btc_mode', 'bars'),
        indicator_mode=getattr(args, 'certify_indicator_mode', 'live'),
        workers=int(getattr(args, 'certify_workers', 6) or 6),
        overlap_sec=int(getattr(args, 'certify_overlap_sec', 120) or 120),
        step2_latency_mode=getattr(args, 'certify_step2_latency_mode', 'entry-exit'),
        step2_latency_model_path=getattr(args, 'certify_step2_latency_model', str(step2_latency_model.DEFAULT_MODEL_PATH)),
        step2_latency_percentile=getattr(args, 'certify_step2_latency_percentile', 'p75'),
    )
    compact = step2_manifest_resolver.compact_resolution(payload)
    receipt_path = out_dir / 'cache_manifest_resolution.json'
    out_dir.mkdir(parents=True, exist_ok=True)
    _write_json(receipt_path, payload)
    compact['hunter_cache_manifest_resolution_path'] = str(receipt_path.resolve())
    if payload.get('ok') and payload.get('manifest_path'):
        args.compiled_decision_tape = str(payload['manifest_path'])
    return compact


def _certify_cache_for_hunter(args: argparse.Namespace, out_dir: Path) -> dict:
    if not getattr(args, 'certify_cache', True):
        return {
            'ok': False,
            'source': 'step2_adaptive_hunter',
            'selected_action': 'certification_disabled',
            'failure_reasons': ['cache_certification_disabled'],
        }
    manifest_path = Path(args.compiled_decision_tape).resolve()
    manifest = _read_json(manifest_path)
    day = _infer_certification_day(args, manifest)
    source_days = _source_days_from_manifest(manifest)
    range_start = source_days[0] if source_days else ''
    range_end = source_days[-1] if source_days else ''
    if not day and not (range_start and range_end):
        return _lineage_only_certification(manifest_path, manifest)
    tickers = [
        str(ticker).upper()
        for ticker in (getattr(args, 'certify_tickers', None) or [])
        if str(ticker).strip()
    ]
    if not tickers:
        ticker_map = manifest.get('ticker_map') if isinstance(manifest.get('ticker_map'), dict) else {}
        tickers = sorted(str(ticker).upper() for ticker in ticker_map) or ['CLSK', 'MARA', 'RIOT']
    cert_args = argparse.Namespace(
        day=day,
        start='' if day else range_start,
        end='' if day else range_end,
        tickers=tickers,
        feed=getattr(args, 'certify_feed', 'sip'),
        quote_mode=getattr(args, 'certify_quote_mode', 'per-second'),
        btc_mode=getattr(args, 'certify_btc_mode', 'bars'),
        indicator_mode=getattr(args, 'certify_indicator_mode', 'live'),
        name=manifest_path.parent.name,
        compiled_manifest=str(manifest_path),
        workers=int(getattr(args, 'certify_workers', 6) or 6),
        no_existing=False,
        overlap_sec=int(getattr(args, 'certify_overlap_sec', 120) or 120),
        step2_latency_mode=getattr(args, 'certify_step2_latency_mode', 'entry-exit'),
        step2_latency_model=getattr(args, 'certify_step2_latency_model', ''),
        step2_latency_percentile=float(getattr(args, 'certify_step2_latency_percentile', 0.75) or 0.75),
        use_state_checkpoints=bool(getattr(args, 'certify_use_state_checkpoints', True)),
        check_only=False,
        no_recertify_non_score=False,
    )
    try:
        receipt = certify_step2_cache.certify(cert_args, write=True)
    except Exception as exc:
        receipt = {
            'ok': False,
            'source': 'certify_step2_cache',
            'day': day,
            'tickers': tickers,
            'selected_action': 'certification_error',
            'failure_reasons': ['certification_exception'],
            'error': repr(exc),
        }
    receipt_path = out_dir / 'cache_certification.json'
    out_dir.mkdir(parents=True, exist_ok=True)
    _write_json(receipt_path, receipt)
    compact = _compact_certification(receipt)
    compact['hunter_cache_certification_path'] = str(receipt_path.resolve())
    compiled_meta = compact.get('compiled_manifest') if isinstance(compact.get('compiled_manifest'), dict) else {}
    certified_manifest = compiled_meta.get('path')
    if receipt.get('ok') and certified_manifest:
        args.compiled_decision_tape = str(certified_manifest)
    return compact


def _compiled_days(compiled: dict) -> list[str]:
    return sorted(str(day) for day in (compiled.get('day_map') or {}).keys())


def _data_integrity_preflight(days: list[str], args: argparse.Namespace) -> dict:
    if args.skip_data_integrity_gate or not days:
        return {
            'skipped': True,
            'reason': 'disabled' if args.skip_data_integrity_gate else 'no_compiled_days',
            'days': days,
            'ok': True,
            'promotion_safe': True,
        }
    payload = market_data_integrity_gate.build_many(
        days,
        source='canonical',
        write=True,
        use_cached=not args.refresh_data_integrity,
    )
    payload['skipped'] = False
    return payload


def _attach_evaluation_guardrails(payload: dict, envelope: dict) -> dict:
    step2_evaluation_envelope.attach(payload, envelope)
    baseline_drift_sentinel.annotate_payload(payload)
    return payload


def _row_id(row: dict) -> str:
    return tournament_safety.model_id(
        str(row.get('variant') or row.get('name') or 'candidate'),
        row.get('weights') or {},
        float(row.get('bias') or 0.0),
    )


def _robustness(row: dict) -> dict:
    value = row.get('robustness')
    return value if isinstance(value, dict) else {}


def _adjusted_score(row: dict) -> float:
    value = _robustness(row).get('adjusted_score')
    if value is None:
        value = _robustness(row).get('score')
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


def _raw_pnl(row: dict) -> float:
    try:
        return float(row.get('step2_pnl') or row.get('pnl') or 0.0)
    except Exception:
        return 0.0


def _sort_raw(rows: list[dict]) -> list[dict]:
    return sorted(rows, key=lambda row: _raw_pnl(row), reverse=True)


def _sort_adjusted(rows: list[dict]) -> list[dict]:
    return sorted(rows, key=lambda row: (_adjusted_score(row), _raw_pnl(row)), reverse=True)


def _dedupe_rows(rows: list[dict]) -> list[dict]:
    best: dict[str, dict] = {}
    for row in rows:
        if not isinstance(row, dict) or not row.get('weights'):
            continue
        key = _row_id(row)
        old = best.get(key)
        if old is None or (_adjusted_score(row), _raw_pnl(row)) > (_adjusted_score(old), _raw_pnl(old)):
            best[key] = row
    return list(best.values())


def _recommended_rows(rows: list[dict]) -> list[dict]:
    return _sort_adjusted([
        row for row in _dedupe_rows(rows)
        if bool(_robustness(row).get('recommended'))
    ])


def _review(row: dict | None) -> dict:
    if not row:
        return {'exists': False}
    robustness = _robustness(row)
    return {
        'exists': True,
        'variant': row.get('variant') or row.get('name'),
        'step2_pnl': row.get('step2_pnl') or row.get('pnl'),
        'step2_delta_pct_vs_active': row.get('step2_delta_pct_vs_active') or robustness.get('delta_pct_vs_active'),
        'robustness_score': robustness.get('score'),
        'robustness_adjusted_score': robustness.get('adjusted_score'),
        'recommended': bool(robustness.get('recommended')),
        'recommendation_tier': robustness.get('recommendation_tier'),
        'recommendation_blockers': robustness.get('recommendation_blockers') or [],
        'red_flags': robustness.get('red_flags') or [],
    }


def _leaderboard_union(*groups: list[dict]) -> list[dict]:
    rows: list[dict] = []
    for group in groups:
        rows.extend(group or [])
    return _dedupe_rows(rows)


def main() -> int:
    ap = argparse.ArgumentParser(description='Adaptive direct Step 2 variant hunter.')
    ap.add_argument('--compiled-decision-tape', default='',
                    help='Explicit compiled tape manifest. Defaults to resolving the newest certified Step 2 cache.')
    ap.add_argument('--no-cache-resolver', dest='cache_resolver', action='store_false',
                    help='Diagnostics only: require --compiled-decision-tape instead of resolving a certified cache.')
    ap.set_defaults(cache_resolver=True)
    ap.add_argument('--cache-day', default='',
                    help='Resolve a certified cache covering this market day.')
    ap.add_argument('--cache-start', default='',
                    help='Resolve a certified cache covering this start day.')
    ap.add_argument('--cache-end', default='',
                    help='Resolve a certified cache covering this end day.')
    ap.add_argument('--cache-tickers', nargs='*', default=DEFAULT_CACHE_TICKERS,
                    help='Ticker set used by the certified cache resolver.')
    ap.add_argument('--out-dir', default=str(DEFAULT_OUT))
    ap.add_argument('--name', default='adaptive_step2_hunt')
    ap.add_argument('--batch-size', type=int, default=500)
    ap.add_argument('--target-count', type=int, default=5)
    ap.add_argument('--target-pnl', type=float, default=None,
                    help='Absolute P/L target for winners. Use 0 to hunt only profitable variants.')
    ap.add_argument('--beat-pct', type=float, default=10.0)
    ap.add_argument('--max-batches', type=int, default=200)
    ap.add_argument('--start-balance', type=float, default=100000.0)
    ap.add_argument('--max-trades-per-day', type=int, default=0)
    ap.add_argument('--max-trades-per-ticker-day', type=int, default=0)
    ap.add_argument('--seed', type=int, default=20260507)
    ap.add_argument('--seed-json', action='append', default=[])
    ap.add_argument('--seed-limit', type=int, default=250)
    ap.add_argument('--weight-limit', type=float, default=4.0)
    ap.add_argument('--score-cache-db', default=str(step2_score_cache.DEFAULT_CACHE_DB))
    ap.add_argument('--no-score-cache', dest='use_score_cache', action='store_false')
    ap.set_defaults(use_score_cache=True)
    ap.add_argument('--skip-data-integrity-gate', action='store_true',
                    help='Diagnostics only: do not preflight the compiled tape market data.')
    ap.add_argument('--refresh-data-integrity', action='store_true',
                    help='Rebuild market-data integrity reports instead of using cached day reports.')
    ap.add_argument('--allow-data-integrity-fail', action='store_true',
                    help='Run and write non-promotable results even when the tape integrity gate fails.')
    ap.add_argument('--allow-uncertified-cache', action='store_true',
                    help='Diagnostics only: allow hunting on a non-certified compiled cache and mark output non-promotable.')
    ap.add_argument('--no-certify-cache', dest='certify_cache', action='store_false',
                    help='Diagnostics only: skip automatic compiled-cache certification before hunting.')
    ap.set_defaults(certify_cache=True)
    ap.add_argument('--certify-day', default='',
                    help='Override the day used for cache certification. By default, infer it from one-day manifests.')
    ap.add_argument('--certify-tickers', nargs='*', default=[],
                    help='Override tickers used for cache certification. Defaults to manifest ticker_map.')
    ap.add_argument('--certify-workers', type=int, default=6)
    ap.add_argument('--certify-feed', default='sip')
    ap.add_argument('--certify-quote-mode', default='per-second')
    ap.add_argument('--certify-btc-mode', default='bars')
    ap.add_argument('--certify-indicator-mode', default='live')
    ap.add_argument('--certify-overlap-sec', type=int, default=120)
    ap.add_argument('--certify-step2-latency-mode', choices=['off', 'entry', 'entry-exit'], default='entry-exit')
    ap.add_argument('--certify-step2-latency-model', default=str(step2_latency_model.DEFAULT_MODEL_PATH))
    ap.add_argument('--certify-step2-latency-percentile', type=float, default=0.75)
    ap.add_argument('--no-certify-state-checkpoints', dest='certify_use_state_checkpoints', action='store_false')
    ap.set_defaults(certify_use_state_checkpoints=True)
    ap.add_argument('--robustness-top-n', type=int, default=50,
                    help='Number of top rows to include in the candidate robustness report.')
    ap.add_argument('--skip-robustness-report', action='store_true',
                    help='Diagnostics only: do not attach candidate robustness scoring to output rows.')
    args = ap.parse_args()

    rng = random.Random(args.seed)
    parity = step2_parity_contract.contract(_load_trading_config())
    parity_hash = step2_parity_contract.contract_hash(parity)
    out_dir = Path(args.out_dir) / args.name
    checkpoint = out_dir / 'checkpoint.json'
    rows_path = out_dir / 'rounds.jsonl'
    cache_catalog_resolution = _resolve_compiled_cache_for_hunter(args, out_dir)
    if not cache_catalog_resolution.get('ok'):
        out_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            'schema_version': 1,
            'script': 'step2_adaptive_hunter.py',
            'completed': False,
            'promotable': False,
            'non_promotable_reason': 'step2_cache_resolution_failed',
            'non_promotable_reasons': ['step2_cache_resolution_failed'],
            'compiled_decision_tape': args.compiled_decision_tape,
            'cache_catalog_resolution': cache_catalog_resolution,
        }
        _write_json(out_dir / 'summary.json', payload)
        print(json.dumps({
            'out': str(out_dir / 'summary.json'),
            'completed': False,
            'promotable': False,
            'non_promotable_reason': 'step2_cache_resolution_failed',
            'cache_catalog_resolution': cache_catalog_resolution,
        }, indent=2, sort_keys=True))
        return 4
    cache_certification = _certify_cache_for_hunter(args, out_dir)
    if not cache_certification.get('ok') and not args.allow_uncertified_cache:
        out_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            'schema_version': 1,
            'script': 'step2_adaptive_hunter.py',
            'completed': False,
            'promotable': False,
            'non_promotable_reason': 'step2_cache_certification_failed',
            'non_promotable_reasons': ['step2_cache_certification_failed'],
            'compiled_decision_tape': args.compiled_decision_tape,
            'cache_catalog_resolution': cache_catalog_resolution,
            'cache_certification': cache_certification,
        }
        _write_json(out_dir / 'summary.json', payload)
        print(json.dumps({
            'out': str(out_dir / 'summary.json'),
            'completed': False,
            'promotable': False,
            'non_promotable_reason': 'step2_cache_certification_failed',
            'cache_catalog_resolution': cache_catalog_resolution,
            'certification': cache_certification,
        }, indent=2, sort_keys=True))
        return 4
    try:
        compiled = decision_tape_compiled.load_compiled(args.compiled_decision_tape, mmap=True)
    except compiled_tape_lineage.CompiledTapeLineageError as exc:
        out_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            'schema_version': 1,
            'script': 'step2_adaptive_hunter.py',
            'completed': False,
            'promotable': False,
            'non_promotable_reason': 'compiled_tape_unsafe_score_drift',
            'non_promotable_reasons': ['compiled_tape_unsafe_score_drift', 'compiled_tape_not_certified'],
            'compiled_decision_tape': args.compiled_decision_tape,
            'cache_catalog_resolution': cache_catalog_resolution,
            'lineage_validation': exc.lineage,
        }
        _write_json(out_dir / 'summary.json', payload)
        print(json.dumps({
            'out': str(out_dir / 'summary.json'),
            'completed': False,
            'promotable': False,
            'non_promotable_reason': 'compiled_tape_unsafe_score_drift',
            'lineage_status': exc.lineage.get('status'),
            'recommended_action': exc.lineage.get('recommended_action'),
        }, indent=2, sort_keys=True))
        return 4
    quote_aware_gate = step2_quote_aware_guard.manifest_gate(compiled.get('manifest') if isinstance(compiled.get('manifest'), dict) else {})
    if not quote_aware_gate.get('ok'):
        out_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            'schema_version': 1,
            'script': 'step2_adaptive_hunter.py',
            'completed': False,
            'promotable': False,
            'non_promotable_reason': 'compiled_tape_exit_replay_model_mismatch',
            'non_promotable_reasons': ['compiled_tape_exit_replay_model_mismatch'],
            'compiled_decision_tape': args.compiled_decision_tape,
            'cache_catalog_resolution': cache_catalog_resolution,
            'cache_certification': cache_certification,
            'quote_aware_outcome_gate': quote_aware_gate,
        }
        _write_json(out_dir / 'summary.json', payload)
        print(json.dumps({
            'out': str(out_dir / 'summary.json'),
            'completed': False,
            'promotable': False,
            'non_promotable_reason': 'compiled_tape_exit_replay_model_mismatch',
            'quote_aware_outcome_gate': quote_aware_gate,
        }, indent=2, sort_keys=True))
        return 4
    if not _cache_certification_ok(compiled) and not args.allow_uncertified_cache:
        lineage = _lineage_validation(compiled)
        out_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            'schema_version': 1,
            'script': 'step2_adaptive_hunter.py',
            'completed': False,
            'promotable': False,
            'non_promotable_reason': 'compiled_tape_not_certified',
            'non_promotable_reasons': ['compiled_tape_not_certified'],
            'compiled_decision_tape': args.compiled_decision_tape,
            'cache_catalog_resolution': cache_catalog_resolution,
            'lineage_validation': lineage,
        }
        _write_json(out_dir / 'summary.json', payload)
        print(json.dumps({
            'out': str(out_dir / 'summary.json'),
            'completed': False,
            'promotable': False,
            'non_promotable_reason': 'compiled_tape_not_certified',
            'lineage_status': lineage.get('status'),
            'recommended_action': lineage.get('recommended_action'),
        }, indent=2, sort_keys=True))
        return 4
    data_integrity = _data_integrity_preflight(_compiled_days(compiled), args)
    if not data_integrity.get('promotion_safe') and not args.allow_data_integrity_fail:
        envelope = step2_evaluation_envelope.build(
            compiled=compiled,
            compiled_manifest_path=args.compiled_decision_tape,
            args=args,
            data_integrity=data_integrity,
            sim_config=_sim_config(args),
            run_context={'mode': 'pre_score_abort', 'cache_catalog_resolution': cache_catalog_resolution},
            cache_certification=cache_certification,
            name=args.name,
        )
        out_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            'schema_version': 1,
            'script': 'step2_adaptive_hunter.py',
            'completed': False,
            'promotable': False,
            'non_promotable_reason': 'market_data_integrity_failed',
            'compiled_decision_tape': args.compiled_decision_tape,
            'market_data_integrity': data_integrity,
            'cache_catalog_resolution': cache_catalog_resolution,
            'cache_certification': cache_certification,
        }
        _attach_evaluation_guardrails(payload, envelope)
        _write_json(out_dir / 'summary.json', payload)
        print(json.dumps({
            'out': str(out_dir / 'summary.json'),
            'completed': False,
            'promotable': False,
            'non_promotable_reason': 'market_data_integrity_failed',
            'critical_count': data_integrity.get('critical_count'),
            'warning_count': data_integrity.get('warning_count'),
        }, indent=2, sort_keys=True))
        return 3
    active = _active_seed()
    active_row = _score(compiled, [active], args)[0]
    evaluation_envelope = step2_evaluation_envelope.build(
        compiled=compiled,
        compiled_manifest_path=args.compiled_decision_tape,
        args=args,
        active_row=active_row,
        data_integrity=data_integrity,
        sim_config=_sim_config(args),
        run_context={'mode': 'adaptive_step2_hunt', 'cache_catalog_resolution': cache_catalog_resolution},
        cache_certification=cache_certification,
        name=args.name,
    )
    active_pnl = float(active_row['step2_pnl'])
    target_pnl, target_formula = _target_config(args, active_pnl)

    seeds = [active]
    seeds.extend(_load_seed_rows(args.seed_json, args.seed_limit))
    elites: list[dict] = []
    adjusted_elites: list[dict] = []
    recommended: list[dict] = []
    winners: list[dict] = []
    seen_models = set()
    started = time.perf_counter()
    out_dir.mkdir(parents=True, exist_ok=True)

    with rows_path.open('a', encoding='utf-8') as round_log:
        for batch_idx in range(1, int(args.max_batches) + 1):
            scale = max(0.08, 0.9 * (0.985 ** batch_idx))
            variants: list[lab.Variant] = []
            lineage_by_name: dict[str, dict] = {}
            while len(variants) < int(args.batch_size):
                src_rows: list[dict] = []
                roll = rng.random()
                if recommended and roll < 0.55:
                    src_rows = recommended[:min(len(recommended), 120)]
                elif adjusted_elites and roll < 0.75:
                    src_rows = adjusted_elites[:min(len(adjusted_elites), 120)]
                elif winners and roll < 0.88:
                    src_rows = winners[:min(len(winners), 120)]
                elif elites and roll < 0.95:
                    src_rows = elites[:min(len(elites), 120)]
                if src_rows:
                    src = rng.choice(src_rows[:min(len(src_rows), 80)])
                    seed_variant = _variant(str(src['variant']), dict(src['weights']), float(src.get('bias') or 0.0))
                    v = _mutate(
                        seed_variant,
                        rng,
                        batch_idx * 1_000_000 + len(variants),
                        scale,
                        float(args.weight_limit),
                    )
                    if recommended and src in recommended:
                        lane = 'robustness_first'
                    elif adjusted_elites and src in adjusted_elites:
                        lane = 'promotion_readiness'
                    elif winners and src in winners:
                        lane = 'exploitation'
                    else:
                        lane = 'raw_pnl_elite'
                    lineage_by_name[v.name] = _lineage_for_seed(
                        source_row=src,
                        lane=lane,
                        reason='mutate_scored_parent',
                        batch_idx=batch_idx,
                        scale=scale,
                    )
                elif seeds and rng.random() < 0.88:
                    seed = rng.choice(seeds)
                    v = _mutate(
                        seed,
                        rng,
                        batch_idx * 1_000_000 + len(variants),
                        scale,
                        float(args.weight_limit),
                    )
                    lineage_by_name[v.name] = _lineage_for_seed(
                        source_row={'variant': seed.name, 'weights': dict(seed.weights), 'bias': float(seed.bias or 0.0)},
                        lane='seed_mutation',
                        reason='mutate_initial_or_seed_json',
                        batch_idx=batch_idx,
                        scale=scale,
                    )
                else:
                    v = _random_sparse(rng, batch_idx * 1_000_000 + len(variants))
                    lineage_by_name[v.name] = _lineage_for_seed(
                        source_row={},
                        lane='wild_shuffle',
                        reason='random_sparse_indicator_shuffle',
                        batch_idx=batch_idx,
                        scale=scale,
                    )
                key = tournament_safety.model_id(v.name, v.weights, v.bias)
                if key in seen_models:
                    continue
                seen_models.add(key)
                variants.append(v)

            scored = _score(compiled, variants, args, lineage_by_name=lineage_by_name)
            if not args.skip_robustness_report:
                candidate_robustness_report.annotate_rows(
                    scored,
                    active_row=active_row,
                    start_balance=float(args.start_balance),
                )
            for row in scored:
                row['step2_delta_vs_active'] = round(row['step2_pnl'] - active_pnl, 4)
                row['step2_delta_pct_vs_active'] = _delta_pct(row['step2_pnl'], active_pnl)
                row['beats_target'] = row['step2_pnl'] >= target_pnl
            scored.sort(key=lambda r: r['step2_pnl'], reverse=True)
            elites = _sort_raw(_dedupe_rows(elites + scored))[:2000]
            winner_by_id = {
                tournament_safety.model_id(r['variant'], r['weights'], r['bias']): r
                for r in winners
            }
            for row in scored:
                if row['beats_target']:
                    winner_by_id[tournament_safety.model_id(row['variant'], row['weights'], row['bias'])] = row
            winners = sorted(winner_by_id.values(), key=lambda r: r['step2_pnl'], reverse=True)
            robustness_payload = {}
            if not args.skip_robustness_report:
                candidate_robustness_report.annotate_rows(
                    elites,
                    active_row=active_row,
                    start_balance=float(args.start_balance),
                )
                candidate_robustness_report.annotate_rows(
                    winners,
                    active_row=active_row,
                    start_balance=float(args.start_balance),
                )
                adjusted_elites = _sort_adjusted(_dedupe_rows(elites))[:2000]
                recommended = _recommended_rows(elites)[:500]
                robustness_payload = candidate_robustness_report.build(
                    rows=_leaderboard_union(elites[:200], adjusted_elites[:200], recommended[:200]),
                    active_row=active_row,
                    start_balance=float(args.start_balance),
                    top_n=int(args.robustness_top_n),
                    source={
                        'script': 'step2_adaptive_hunter.py',
                        'name': args.name,
                        'compiled_decision_tape': args.compiled_decision_tape,
                        'batch': batch_idx,
                    },
                )

            event = {
                'batch': batch_idx,
                'elapsed_sec': round(time.perf_counter() - started, 3),
                'scored_total': batch_idx * int(args.batch_size),
                'active_step2_pnl': active_pnl,
                'target_pnl': target_pnl,
                'target_formula': target_formula,
                'winners': len(winners),
                'recommended': len(recommended),
                'batch_best': scored[0],
                'global_best': elites[0],
                'best_adjusted': adjusted_elites[0] if adjusted_elites else None,
                'best_recommended': recommended[0] if recommended else None,
            }
            round_log.write(json.dumps(event, sort_keys=True) + '\n')
            round_log.flush()
            surface_ok = _promotion_surface_ok(data_integrity, cache_certification)
            payload = {
                'schema_version': 1,
                'script': 'step2_adaptive_hunter.py',
                'step2_parity_contract': parity,
                'step2_parity_contract_hash': parity_hash,
                'step2_execution_contract_hash': (compiled.get('manifest') or {}).get('step2_execution_contract_hash'),
                'compiled_decision_tape': args.compiled_decision_tape,
                'exit_replay_model': step2_quote_aware_guard.manifest_exit_replay_model(compiled.get('manifest') if isinstance(compiled.get('manifest'), dict) else {}),
                'required_exit_replay_model': step2_quote_aware_guard.REQUIRED_EXIT_REPLAY_MODEL,
                'cache_catalog_resolution': cache_catalog_resolution,
                'start_balance': args.start_balance,
                'active': active_row,
                'active_step2_pnl': active_pnl,
                'target_pnl': target_pnl,
                'target_formula': target_formula,
                'beat_pct': args.beat_pct,
                'absolute_target_pnl': args.target_pnl,
                'batch_size': args.batch_size,
                'completed_batches': batch_idx,
                'scored_total': batch_idx * int(args.batch_size),
                'promotion_surface_ok': surface_ok,
                'promotable': bool(surface_ok and recommended),
                'non_promotable_reason': _non_promotable_reason(surface_ok, data_integrity, cache_certification, recommended),
                'market_data_integrity': data_integrity,
                'cache_certification': cache_certification,
                'elapsed_sec': round(time.perf_counter() - started, 3),
                'candidate_robustness': robustness_payload.get('summary') if robustness_payload else {'skipped': True},
                'recommendation_policy': {
                    'report_only': True,
                    'mutation_bias': {
                        'recommended_pool': 0.55,
                        'robustness_adjusted_pool': 0.20,
                        'raw_target_winners_pool': 0.13,
                        'raw_pnl_elites_pool': 0.07,
                        'seed_or_random_pool': 0.05,
                    },
                    'deduction': (
                        'The hunter still records raw P/L winners, but future batches preferentially mutate '
                        'from candidates that beat Live with cleaner robustness characteristics.'
                    ),
                },
                'raw_rank1_review': _review(elites[0] if elites else None),
                'best_recommended': _review(recommended[0] if recommended else None),
                'winners': winners[:max(args.target_count, 25)],
                'leaderboard': elites[:50],
                'robustness_adjusted_leaderboard': adjusted_elites[:50],
                'recommended_leaderboard': recommended[:50],
            }
            _attach_evaluation_guardrails(payload, evaluation_envelope)
            candidate_profile_schema.decorate_payload(payload, context={
                'script': 'step2_adaptive_hunter.py',
                'start_balance': args.start_balance,
                'compiled_decision_tape': args.compiled_decision_tape,
                'cache_catalog_resolution': cache_catalog_resolution,
                'exit_replay_model': step2_quote_aware_guard.manifest_exit_replay_model(compiled.get('manifest') if isinstance(compiled.get('manifest'), dict) else {}),
                'active_step2_pnl': active_pnl,
                'target_pnl': target_pnl,
                'scored_total': payload.get('scored_total'),
            })
            _write_json(checkpoint, payload)
            print(json.dumps({
                'event': 'batch_done',
                'batch': batch_idx,
                'scored_total': payload['scored_total'],
                'winners': len(winners),
                'batch_best_pnl': scored[0]['step2_pnl'],
                'global_best_pnl': elites[0]['step2_pnl'],
                'target_pnl': round(target_pnl, 2),
                'recommended': len(recommended),
                'raw_rank1_recommended': _review(elites[0] if elites else None).get('recommended'),
                'best_recommended_pnl': (recommended[0].get('step2_pnl') if recommended else None),
                'robustness_leader': (
                    (robustness_payload.get('top_by_adjusted') or [{}])[0].get('variant')
                    if robustness_payload else None
                ),
                'robustness_score': (
                    (robustness_payload.get('top_by_adjusted') or [{}])[0].get('robustness_adjusted_score')
                    if robustness_payload else None
                ),
            }), flush=True)
            if len(winners) >= int(args.target_count):
                break

    final = json.loads(checkpoint.read_text(encoding='utf-8'))
    final['target_reached'] = len(final.get('winners') or []) >= int(args.target_count)
    final['completed_batches'] = int(final.get('completed_batches') or args.max_batches)
    final['completed'] = bool(final['target_reached'] or final['completed_batches'] >= int(args.max_batches))
    final['stop_reason'] = 'target_count_reached' if final['target_reached'] else 'max_batches_exhausted'
    final_certification = final.get('cache_certification') if isinstance(final.get('cache_certification'), dict) else cache_certification
    final['cache_certification'] = final_certification
    surface_ok = _promotion_surface_ok(final.get('market_data_integrity') or {}, final_certification)
    final['promotion_surface_ok'] = surface_ok
    final['promotable'] = bool(surface_ok and (final.get('recommended_leaderboard') or []))
    final['non_promotable_reason'] = _non_promotable_reason(
        surface_ok,
        final.get('market_data_integrity') or {},
        final_certification,
        final.get('recommended_leaderboard') or [],
    )
    final['step2_parity_contract'] = parity
    final['step2_parity_contract_hash'] = parity_hash
    if not args.skip_robustness_report:
        report_rows = _leaderboard_union(
            final.get('leaderboard') or [],
            final.get('robustness_adjusted_leaderboard') or [],
            final.get('recommended_leaderboard') or [],
        )
        robustness_payload = candidate_robustness_report.build(
            rows=report_rows,
            active_row=final.get('active') or active_row,
            start_balance=float(args.start_balance),
            top_n=int(args.robustness_top_n),
            source={
                'script': 'step2_adaptive_hunter.py',
                'name': args.name,
                'compiled_decision_tape': args.compiled_decision_tape,
                'summary': str((out_dir / 'summary.json').resolve()),
                'exit_replay_model': step2_quote_aware_guard.manifest_exit_replay_model(compiled.get('manifest') if isinstance(compiled.get('manifest'), dict) else {}),
            },
        )
        robustness_path = out_dir / 'candidate_robustness_report.json'
        candidate_robustness_report.write_report(robustness_payload, robustness_path)
        final['candidate_robustness'] = robustness_payload.get('summary')
        final['candidate_robustness_report_path'] = str(robustness_path.resolve())
        final['raw_rank1_review'] = _review((final.get('leaderboard') or [None])[0])
        final['best_recommended'] = _review((final.get('recommended_leaderboard') or [None])[0])
    candidate_profile_schema.decorate_payload(final, context={
        'script': 'step2_adaptive_hunter.py',
        'start_balance': args.start_balance,
        'compiled_decision_tape': args.compiled_decision_tape,
        'cache_catalog_resolution': cache_catalog_resolution,
        'exit_replay_model': step2_quote_aware_guard.manifest_exit_replay_model(compiled.get('manifest') if isinstance(compiled.get('manifest'), dict) else {}),
        'active_step2_pnl': final.get('active_step2_pnl'),
        'target_pnl': final.get('target_pnl'),
        'scored_total': final.get('scored_total'),
    })
    _attach_evaluation_guardrails(final, evaluation_envelope)
    summary_path = out_dir / 'summary.json'
    _write_json(summary_path, final)
    try:
        brief_variant = ((final.get('best_recommended') or {}).get('variant') or
                         ((final.get('leaderboard') or [{}])[0].get('variant')))
        brief = candidate_decision_brief.build_from_file(
            str(summary_path),
            rank=1,
            variant=str(brief_variant or ''),
            run_reproducibility=True,
            run_quarantine=True,
        )
        brief_path = out_dir / 'candidate_decision_brief.json'
        candidate_decision_brief.write_report(brief, brief_path)
        final['candidate_decision_brief'] = {
            'path': str(brief_path.resolve()),
            'decision': (brief.get('decision') or {}).get('label'),
            'variant': (brief.get('selection') or {}).get('variant'),
            'plain_english': brief.get('plain_english'),
            'promotion_blockers': (brief.get('decision') or {}).get('promotion_blockers') or [],
            'reject_reasons': (brief.get('decision') or {}).get('reject_reasons') or [],
        }
        if final['candidate_decision_brief']['decision'] != 'PROMOTE':
            final['promotable'] = False
            final['non_promotable_reason'] = (
                final['candidate_decision_brief']['reject_reasons'][0]
                if final['candidate_decision_brief']['reject_reasons']
                else final['candidate_decision_brief']['decision'] or 'candidate_not_promotable'
            )
        _write_json(summary_path, final)
    except Exception as exc:
        final['candidate_decision_brief'] = {
            'path': None,
            'decision': 'ERROR',
            'error': repr(exc),
        }
        _write_json(summary_path, final)
    print(json.dumps({
        'out': str(summary_path),
        'completed': final['completed'],
        'winners': len(final.get('winners') or []),
        'scored_total': final.get('scored_total'),
        'leader': (final.get('leaderboard') or [{}])[0],
        'best_recommended': final.get('best_recommended'),
        'raw_rank1_review': final.get('raw_rank1_review'),
        'candidate_robustness': final.get('candidate_robustness'),
        'candidate_decision_brief': final.get('candidate_decision_brief'),
        'cache_catalog_resolution': final.get('cache_catalog_resolution'),
        'cache_certification': final.get('cache_certification'),
    }, indent=2, sort_keys=True))
    return 0 if final['completed'] else 2


if __name__ == '__main__':
    raise SystemExit(main())
