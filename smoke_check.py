from __future__ import annotations

import argparse
import json
import os
import py_compile
import sys
from pathlib import Path
from urllib.request import urlopen

HERE = Path(__file__).resolve().parent


def check(name: str, ok: bool, detail: str = '') -> bool:
    status = 'OK' if ok else 'FAIL'
    suffix = f' - {detail}' if detail else ''
    print(f'{status}: {name}{suffix}')
    return ok


def main() -> int:
    parser = argparse.ArgumentParser(description='Trading app smoke/pre-market check.')
    parser.add_argument(
        '--pre-market',
        action='store_true',
        help='Require the broker account to be flat even if the app reports in_window=true.',
    )
    parser.add_argument(
        '--mode',
        choices=('auto', 'pre-market', 'market-open', 'post-market', 'no-surprises', 'post-open'),
        default='auto',
        help='Readiness mode. no-surprises adds compile checks and readiness artifacts.',
    )
    args = parser.parse_args()
    ok = True
    os.chdir(HERE)

    import mock_trader  # noqa: F401
    import ws_scalp  # noqa: F401
    import daily_postmortem  # noqa: F401
    import postmortem_hypotheses  # noqa: F401

    cfg = json.loads((HERE / 'trading_config.json').read_text(encoding='utf-8'))
    ok &= check('config tickers', cfg.get('tickers') == ['CLSK', 'MARA', 'RIOT'])
    ok &= check('config has ticker_cfg', all(t in cfg.get('ticker_cfg', {}) for t in cfg.get('tickers', [])))
    session = cfg.get('session', {})
    cutoff = f"{int(session.get('flatten_hour', -1)):02d}:{int(session.get('flatten_minute', -1)):02d}"
    ok &= check('flatten cutoff configured', cutoff == '14:55', cutoff)

    live_files = ['mock_trader.py', 'daily_postmortem.py']
    forbidden = ['bar_engine', 'BAR_ENGINE', 'SIGNAL_SOURCE', 'shadow:ws_scalp']
    for file_name in live_files:
        text = (HERE / file_name).read_text(encoding='utf-8', errors='replace')
        hits = [token for token in forbidden if token in text]
        ok &= check(f'no stale engine tokens in {file_name}', not hits, ', '.join(hits))

    data = {}
    try:
        with urlopen('http://127.0.0.1:5000/mock/status', timeout=3) as resp:
            data = json.loads(resp.read().decode('utf-8'))
        ok &= check('mock status reachable', True)
        if args.mode == 'market-open' or (args.mode == 'post-open' and data.get('in_window')) \
                or (args.mode == 'auto' and data.get('in_window')):
            ok &= check('mock trader running', data.get('running') is True)
        else:
            ok &= check('mock trader inactive outside market window is acceptable',
                        data.get('running') in (True, False),
                        f"running={data.get('running')} in_window={data.get('in_window')}")
        require_local_flat = args.pre_market or args.mode in ('pre-market', 'post-market', 'no-surprises') \
            or (args.mode == 'auto' and not data.get('in_window'))
        if require_local_flat:
            ok &= check('no internal open positions', not data.get('positions'))
        else:
            positions = data.get('positions') or {}
            unprotected = [
                sym for sym, pos in positions.items()
                if not pos.get('alpaca_order_id') and not pos.get('broker_close')
            ]
            ok &= check('market-open positions are tracked/protected',
                        not unprotected,
                        ', '.join(unprotected) if unprotected else f"{len(positions)} open")
        ok &= check('no broker exposure block', not data.get('broker_exposure_block'),
                    json.dumps(data.get('broker_exposure_block')))
        ok &= check('broker api not degraded', not data.get('broker_api_degraded'),
                    json.dumps(data.get('broker_api_degraded')))
        kill_switch = data.get('kill_switch') or {}
        ok &= check('kill switch disabled', not kill_switch.get('enabled'),
                    json.dumps(kill_switch))
        ok &= check('status flatten cutoff', data.get('flatten_cutoff_ct') == '14:55',
                    str(data.get('flatten_cutoff_ct')))
        ok &= check('alpaca equity present', data.get('alpaca_equity') is not None,
                    str(data.get('alpaca_equity')))
    except Exception as e:
        ok &= check('mock status reachable', False, str(e))

    state_path = HERE / 'mock_trader_state.json'
    if state_path.exists():
        state = json.loads(state_path.read_text(encoding='utf-8'))
        ok &= check('pending_entries empty', not state.get('pending_entries'))
        ok &= check('state broker exposure clear', not state.get('broker_exposure_block'),
                    json.dumps(state.get('broker_exposure_block')))

    require_broker_flat = (
        args.pre_market
        or args.mode in ('pre-market', 'post-market', 'no-surprises')
        or (args.mode == 'auto' and not data.get('in_window'))
    )
    try:
        from alpaca_trading import AlpacaTrader
        trader = AlpacaTrader.from_env(paper=True)
        positions = trader.list_positions()
        watched = set(cfg.get('tickers', []))
        live = [p.get('symbol') for p in positions if p.get('symbol') in watched]
        if require_broker_flat:
            ok &= check('broker flat for watched tickers', not live, ', '.join(live))
        else:
            ok &= check('broker flat check skipped during trading window', True)
            local_positions = set((data.get('positions') or {}).keys())
            untracked = sorted(set(live) - local_positions)
            ok &= check('broker positions tracked locally', not untracked, ', '.join(untracked))
            orders = trader.list_orders(status='open', symbols=list(watched), nested=True)
            protected = set()
            for order in orders or []:
                sym = order.get('symbol')
                if sym in watched and (order.get('order_class') == 'bracket' or order.get('legs')):
                    protected.add(sym)
                for leg in order.get('legs') or []:
                    if sym in watched and leg.get('status') in ('new', 'accepted', 'held', 'partially_filled'):
                        protected.add(sym)
            unprotected = sorted(sym for sym in live if sym not in protected)
            ok &= check('broker positions have protective orders',
                        not unprotected,
                        ', '.join(unprotected) if unprotected else f'{len(protected)} protected')
    except Exception as e:
        if require_broker_flat:
            ok &= check('broker flat check reachable', False, str(e))
        else:
            degraded = data.get('broker_api_degraded') if isinstance(data, dict) else None
            ok &= check(
                'direct broker check degraded to app status',
                not degraded,
                f'direct Alpaca unavailable: {e}; app broker_api_degraded={degraded}',
            )

    if args.mode == 'no-surprises':
        compile_targets = [
            'mock_trader.py',
            'ws_scalp.py',
            'daily_postmortem.py',
            'review_artifacts.py',
            'weekend_readiness.py',
            'live_monitor.py',
            'monday_close_packet.py',
            'daily_close_packet.py',
            'automation_ops.py',
            'ops.py',
            'state_compactor.py',
            'engine_replay.py',
            'review_packet.py',
            'monday_ops.py',
            'daily_ops.py',
            'promotion_review.py',
            'schedule_helpers.py',
            'promotion_queue.py',
            'engine_validation.py',
            'engine_scoreboard.py',
            'eod_integrity_check.py',
        ]
        for name in compile_targets:
            try:
                py_compile.compile(str(HERE / name), doraise=True)
                ok &= check(f'compile {name}', True)
            except Exception as e:
                ok &= check(f'compile {name}', False, str(e))
        try:
            from weekend_readiness import write_weekend_artifacts
            paths, payloads = write_weekend_artifacts()
            checklist = payloads.get('pre_market_checklist') or {}
            ok &= check('weekend readiness artifacts written', True, ', '.join(k for k, v in paths.items() if v))
            ok &= check('pre-market checklist passes', bool(checklist.get('ok')),
                        json.dumps([c for c in checklist.get('checks', []) if not c.get('ok')])[:500])
        except Exception as e:
            ok &= check('weekend readiness artifacts written', False, str(e))

    if args.mode == 'post-open':
        try:
            from weekend_readiness import (
                write_config_change_watch,
                write_market_open_monitor,
                write_session_checkpoint,
                write_trade_alerts,
                write_why_no_trade_summary,
            )
            paths = {}
            for key, writer in (
                ('market_open_monitor', write_market_open_monitor),
                ('why_no_trade', write_why_no_trade_summary),
                ('trade_alerts', write_trade_alerts),
                ('config_change_watch', write_config_change_watch),
            ):
                path, _ = writer()
                paths[key] = path
            checkpoint, payload = write_session_checkpoint(label='post_open_sanity')
            paths['session_checkpoint'] = checkpoint
            alerts = payload.get('alerts') or []
            ok &= check('post-open artifacts written', True, ', '.join(paths))
            ok &= check('no critical passive alerts', not any(a.get('level') == 'critical' for a in alerts),
                        json.dumps(alerts)[:500])
        except Exception as e:
            ok &= check('post-open artifacts written', False, str(e))

    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())
