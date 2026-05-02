from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from urllib.request import urlopen

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo


HERE = Path(__file__).resolve().parent
CT = ZoneInfo('America/Chicago')


def check(name: str, ok: bool, detail: str = '') -> bool:
    print(f"{'OK' if ok else 'FAIL'}: {name}" + (f' - {detail}' if detail else ''))
    return ok


def main(day: str | None = None) -> int:
    os.chdir(HERE)
    day = day or datetime.now(CT).date().isoformat()
    ok = True
    cfg = json.loads((HERE / 'trading_config.json').read_text(encoding='utf-8'))
    watched = set(cfg.get('tickers', []))

    status = {}
    try:
        with urlopen('http://127.0.0.1:5000/mock/status', timeout=5) as resp:
            status = json.loads(resp.read().decode('utf-8'))
        ok &= check('mock status reachable', True)
    except Exception as e:
        ok &= check('mock status reachable', False, str(e))

    ok &= check('local positions empty', not status.get('positions'), json.dumps(status.get('positions')))
    ok &= check('pending entries empty', not status.get('pending_entries'), json.dumps(status.get('pending_entries')))
    ok &= check('broker exposure block clear', not status.get('broker_exposure_block'),
                json.dumps(status.get('broker_exposure_block')))

    try:
        from alpaca_trading import AlpacaTrader
        trader = AlpacaTrader.from_env(paper=True)
        positions = trader.list_positions()
        live = [p.get('symbol') for p in positions if p.get('symbol') in watched]
        ok &= check('Alpaca flat for watched tickers', not live, ', '.join(live))
        orders = trader.list_orders(status='open', symbols=list(watched), nested=True)
        ok &= check('Alpaca open orders clear', not orders, str(len(orders)))
    except Exception as e:
        ok &= check('Alpaca checks reachable', False, str(e))

    pm_json = HERE / 'postmortem' / f'postmortem_{day}.json'
    pm_txt = HERE / 'postmortem' / f'postmortem_{day}.txt'
    ok &= check('postmortem json exists', pm_json.exists(), str(pm_json))
    ok &= check('postmortem txt exists', pm_txt.exists(), str(pm_txt))
    tape_count = None
    if pm_json.exists():
        try:
            pm = json.loads(pm_json.read_text(encoding='utf-8'))
            tape_count = len(pm.get('tape') or [])
        except Exception:
            tape_count = None

    audit = HERE / 'audit' / f'trade_lifecycle_{day}.jsonl'
    ok &= check('audit path initialized or no-trade day acceptable',
                audit.exists() or tape_count == 0 or status.get('trade_count') == 0,
                str(audit))
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else None))
