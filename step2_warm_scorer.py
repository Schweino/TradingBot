from __future__ import annotations

import argparse
import json
import os
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

import active_engine_baseline
import candidate_profile_schema
import decision_tape_compiled
import execution_kernel
import routed_scoring_profile
import scoring_variant_lab as lab
import step2_parity_contract
import tournament_safety
import unified_decision_ledger


HERE = Path(__file__).resolve().parent
CT = ZoneInfo('America/Chicago')
DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 8765
DEFAULT_TICKERS = ['CLSK', 'MARA', 'RIOT']
DEFAULT_OUT_DIR = HERE / 'postmortem' / 'warm_scorer'


def _now_ct() -> str:
    return datetime.now(CT).isoformat(timespec='seconds')


def _read_json(path: Path, default: Any = None) -> Any:
    try:
        with path.open('r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return default


def _write_json(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f'.{os.getpid()}.{int(time.time() * 1000)}.tmp')
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding='utf-8')
    os.replace(tmp, path)
    return str(path.resolve())


def default_compiled_name(day: str, tickers: list[str] | None = None) -> str:
    tickers = [str(t).upper() for t in (tickers or DEFAULT_TICKERS)]
    return f"compiled_step2_live_mockparity_{'-'.join(tickers)}_{day}_intraday"


def default_manifest_path(day: str, tickers: list[str] | None = None, name: str = '') -> Path:
    compiled_name = name or default_compiled_name(day, tickers)
    return HERE / 'postmortem' / 'backtests' / 'compiled_decision_tapes' / compiled_name / 'manifest.json'


def _config() -> dict[str, Any]:
    return _read_json(HERE / 'trading_config.json', {}) or {}


def _variant_from_payload(payload: dict[str, Any]):
    weights = payload.get('weights') or {}
    if not isinstance(weights, dict):
        raise ValueError('variant weights must be a JSON object')
    routes = payload.get('routes') if isinstance(payload.get('routes'), list) else []
    if routes:
        return routed_scoring_profile.routed_variant(
            str(payload.get('name') or payload.get('variant') or 'candidate'),
            {str(k): float(v or 0.0) for k, v in weights.items()},
            float(payload.get('bias') or 0.0),
            [routed_scoring_profile.route_from_dict(route) for route in routes if isinstance(route, dict)],
        )
    return lab.Variant(
        str(payload.get('name') or payload.get('variant') or 'candidate'),
        {str(k): float(v or 0.0) for k, v in weights.items()},
        float(payload.get('bias') or 0.0),
    )


def _variants_from_payload(payload: Any) -> list:
    if isinstance(payload, dict):
        rows = payload.get('variants') or payload.get('rows') or []
    else:
        rows = payload
    if not isinstance(rows, list):
        raise ValueError('variants payload must be a list or an object with variants')
    return [_variant_from_payload(row) for row in rows if isinstance(row, dict)]


def _manifest_signature(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {'path': str(path), 'exists': False}
    st = path.stat()
    return {
        'path': str(path.resolve()),
        'exists': True,
        'size': int(st.st_size),
        'mtime_ns': int(getattr(st, 'st_mtime_ns', int(st.st_mtime * 1_000_000_000))),
        'sha256': tournament_safety._file_sha256(str(path)),
    }


class WarmScorer:
    def __init__(self) -> None:
        self.compiled: dict[str, Any] | None = None
        self.manifest_path: Path | None = None
        self.manifest_signature: dict[str, Any] = {}
        self.loaded_at_ct: str | None = None
        self.load_elapsed_sec: float | None = None
        self.score_count = 0
        self.last_score: dict[str, Any] | None = None

    def load(self, manifest_path: str | os.PathLike[str], force: bool = False) -> dict[str, Any]:
        path = Path(manifest_path).resolve()
        sig = _manifest_signature(path)
        if not force and self.compiled is not None and self.manifest_signature == sig:
            return self.status()
        started = time.perf_counter()
        compiled = decision_tape_compiled.load_compiled(str(path), mmap=True)
        self.compiled = compiled
        self.manifest_path = path
        self.manifest_signature = sig
        self.loaded_at_ct = _now_ct()
        self.load_elapsed_sec = round(time.perf_counter() - started, 4)
        return self.status()

    def ensure_loaded(self, manifest_path: str | os.PathLike[str]) -> dict[str, Any]:
        return self.load(manifest_path, force=False)

    def status(self) -> dict[str, Any]:
        manifest = (self.compiled or {}).get('manifest') or {}
        return {
            'event': 'step2_warm_scorer_status',
            'loaded': self.compiled is not None,
            'manifest_path': str(self.manifest_path) if self.manifest_path else None,
            'manifest_signature': self.manifest_signature,
            'compiled_rows': int((self.compiled or {}).get('rows') or 0),
            'compiled_tape_hash': manifest.get('compiled_tape_hash'),
            'loaded_at_ct': self.loaded_at_ct,
            'load_elapsed_sec': self.load_elapsed_sec,
            'score_count': self.score_count,
            'last_score': self.last_score,
        }

    def score_variants(self, manifest_path: str | os.PathLike[str], variants: list,
                       start_balance: float = 100000.0,
                       write_ledger: bool = False,
                       include_rejected: bool = True,
                       ledger_label: str = 'step2_warm_scorer') -> dict[str, Any]:
        self.ensure_loaded(manifest_path)
        if self.compiled is None:
            raise RuntimeError('compiled tape not loaded')
        cfg = _config()
        sim_config = step2_parity_contract.sim_config(cfg)
        parity = step2_parity_contract.contract(cfg)
        execution_contract = execution_kernel.contract_from_config(cfg)
        started = time.perf_counter()
        rows = decision_tape_compiled.simulate_variants(
            self.compiled,
            variants,
            float(start_balance),
            gate=None,
            sim_config=sim_config,
        )
        elapsed = time.perf_counter() - started
        if rows is None:
            raise RuntimeError('compiled Step 2 simulator unavailable')
        ledger_summary = None
        if write_ledger and len(variants) == 1:
            trace_started = time.perf_counter()
            trace = unified_decision_ledger.trace_step2_decisions(
                self.compiled,
                variants[0],
                float(start_balance),
                gate=None,
                sim_config=sim_config,
                include_rejected=include_rejected,
            )
            days = list(((rows[0].get('decision_full') or {}).get('by_day') or {}).keys())
            day = days[0] if len(days) == 1 else datetime.now(CT).date().isoformat()
            ledger_summary = unified_decision_ledger.write_step2_trace(day, trace, label=ledger_label)
            ledger_summary['trace_elapsed_sec'] = round(time.perf_counter() - trace_started, 4)
            if routed_scoring_profile.is_routed_variant(variants[0]):
                audit = routed_scoring_profile.route_audit(
                    self.compiled,
                    variants[0],
                    trace_rows=trace.get('rows') or [],
                )
                rows[0]['route_audit'] = audit
                rows[0]['routed_profile_audit'] = audit
        self.score_count += 1
        self.last_score = {
            'created_at_ct': _now_ct(),
            'variant_count': len(variants),
            'score_elapsed_sec': round(elapsed, 4),
            'manifest_path': str(Path(manifest_path).resolve()),
        }
        payload = {
            'event': 'step2_warm_scorer_scored',
            'created_at_ct': self.last_score['created_at_ct'],
            'script': 'step2_warm_scorer.py',
            'manifest_path': str(Path(manifest_path).resolve()),
            'compiled_tape_hash': ((self.compiled or {}).get('manifest') or {}).get('compiled_tape_hash'),
            'variant_count': len(variants),
            'start_balance': float(start_balance),
            'score_elapsed_sec': round(elapsed, 4),
            'execution_kernel_contract': execution_contract,
            'execution_kernel_hash': execution_contract.get('execution_kernel_hash'),
            'step2_parity_contract': parity,
            'step2_parity_contract_hash': step2_parity_contract.contract_hash(parity),
            'results': rows,
            'ledger_summary': ledger_summary,
        }
        candidate_profile_schema.decorate_payload(payload, context={
            'script': 'step2_warm_scorer.py',
            'start_balance': float(start_balance),
            'compiled_decision_tape': str(Path(manifest_path).resolve()),
            'scored_total': len(rows),
        })
        return payload

    def score_current(self, manifest_path: str | os.PathLike[str],
                      start_balance: float = 100000.0,
                      write_ledger: bool = False,
                      include_rejected: bool = True) -> dict[str, Any]:
        variant = active_engine_baseline.active_variant()
        payload = self.score_variants(
            manifest_path,
            [variant],
            start_balance=start_balance,
            write_ledger=write_ledger,
            include_rejected=include_rejected,
            ledger_label='step2_current_trace',
        )
        row = payload['results'][0]
        active = active_engine_baseline.active_profile_payload()
        return {
            'event': 'step2_warm_scorer_current_scored',
            'active_profile': active.get('profile'),
            'long_entry_quality_gate_used': False,
            'execution_kernel_contract': payload.get('execution_kernel_contract'),
            'execution_kernel_hash': payload.get('execution_kernel_hash'),
            'step2_parity_contract': payload.get('step2_parity_contract'),
            'step2_parity_contract_hash': payload.get('step2_parity_contract_hash'),
            'compiled_manifest': str(Path(manifest_path).resolve()),
            'compiled_tape_hash': payload.get('compiled_tape_hash'),
            'score_elapsed_sec': payload.get('score_elapsed_sec'),
            'result': row.get('decision_full') or {},
            'ledger_summary': payload.get('ledger_summary'),
            'served_by': 'step2_warm_scorer',
        }


SCORER = WarmScorer()


def _client_url(path: str, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> str:
    return f'http://{host}:{int(port)}{path}'


def client_post(path: str, payload: dict[str, Any], host: str = DEFAULT_HOST,
                port: int = DEFAULT_PORT, timeout: float = 2.0) -> dict[str, Any]:
    req = Request(
        _client_url(path, host, port),
        data=json.dumps(payload).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST',
    )
    with urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode('utf-8'))


def client_get(path: str, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT,
               timeout: float = 2.0) -> dict[str, Any]:
    with urlopen(_client_url(path, host, port), timeout=timeout) as resp:
        return json.loads(resp.read().decode('utf-8'))


def try_score_current_via_service(manifest_path: str | os.PathLike[str],
                                  start_balance: float,
                                  host: str = DEFAULT_HOST,
                                  port: int = DEFAULT_PORT,
                                  timeout: float = 0.75,
                                  write_ledger: bool = False) -> dict[str, Any] | None:
    try:
        payload = client_post('/score-current', {
            'manifest_path': str(Path(manifest_path).resolve()),
            'start_balance': float(start_balance),
            'write_ledger': bool(write_ledger),
        }, host=host, port=port, timeout=timeout)
        if payload.get('ok') and isinstance(payload.get('score'), dict):
            return payload['score']
    except (OSError, URLError, TimeoutError, json.JSONDecodeError):
        return None
    except Exception:
        return None
    return None


class Handler(BaseHTTPRequestHandler):
    server_version = 'Step2WarmScorer/1.0'

    def log_message(self, fmt: str, *args: Any) -> None:  # keep service log clean
        return

    def _read_payload(self) -> dict[str, Any]:
        length = int(self.headers.get('Content-Length') or 0)
        if length <= 0:
            return {}
        raw = self.rfile.read(length).decode('utf-8')
        payload = json.loads(raw) if raw else {}
        return payload if isinstance(payload, dict) else {}

    def _send(self, payload: dict[str, Any], status: int = 200) -> None:
        body = json.dumps(payload, indent=2, sort_keys=True, default=str).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == '/status':
            self._send({'ok': True, **SCORER.status()})
            return
        self._send({'ok': False, 'error': 'not_found', 'path': self.path}, status=404)

    def do_POST(self) -> None:
        try:
            payload = self._read_payload()
            if self.path == '/reload':
                status = SCORER.load(payload.get('manifest_path') or '', force=True)
                self._send({'ok': True, **status})
                return
            if self.path == '/score-current':
                score = SCORER.score_current(
                    payload.get('manifest_path') or '',
                    float(payload.get('start_balance') or 100000.0),
                    write_ledger=bool(payload.get('write_ledger')),
                    include_rejected=bool(payload.get('include_rejected', True)),
                )
                self._send({'ok': True, 'score': score, 'status': SCORER.status()})
                return
            if self.path == '/score-batch':
                variants = _variants_from_payload(payload)
                scored = SCORER.score_variants(
                    payload.get('manifest_path') or '',
                    variants,
                    start_balance=float(payload.get('start_balance') or 100000.0),
                    write_ledger=bool(payload.get('write_ledger')),
                    include_rejected=bool(payload.get('include_rejected', True)),
                    ledger_label=str(payload.get('ledger_label') or 'step2_batch_trace'),
                )
                self._send({'ok': True, 'score': scored, 'status': SCORER.status()})
                return
            if self.path == '/shutdown':
                self._send({'ok': True, 'event': 'step2_warm_scorer_shutdown'})
                threading.Thread(target=self.server.shutdown, daemon=True).start()
                return
            self._send({'ok': False, 'error': 'not_found', 'path': self.path}, status=404)
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            self._send({'ok': False, 'error': repr(exc)}, status=500)


def serve(args: argparse.Namespace) -> int:
    if args.manifest_path:
        SCORER.load(args.manifest_path, force=True)
    server = ThreadingHTTPServer((args.host, int(args.port)), Handler)
    print(json.dumps({
        'event': 'step2_warm_scorer_started',
        'host': args.host,
        'port': int(args.port),
        'status': SCORER.status(),
    }, indent=2, sort_keys=True), flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Persistent warm Step 2 compiled scorer.')
    sub = ap.add_subparsers(dest='command', required=True)
    serve_ap = sub.add_parser('serve')
    serve_ap.add_argument('--host', default=DEFAULT_HOST)
    serve_ap.add_argument('--port', type=int, default=DEFAULT_PORT)
    serve_ap.add_argument('--manifest-path', default='')

    for name in ('status', 'reload', 'score-current', 'score-batch', 'stop'):
        p = sub.add_parser(name)
        p.add_argument('--host', default=DEFAULT_HOST)
        p.add_argument('--port', type=int, default=DEFAULT_PORT)
        p.add_argument('--timeout', type=float, default=2.0)
        if name in ('reload', 'score-current', 'score-batch'):
            p.add_argument('--day', default=datetime.now(CT).date().isoformat())
            p.add_argument('--tickers', nargs='+', default=DEFAULT_TICKERS)
            p.add_argument('--name', default='')
            p.add_argument('--manifest-path', default='')
        if name in ('score-current', 'score-batch'):
            p.add_argument('--start-balance', type=float, default=100000.0)
            p.add_argument('--write-ledger', action='store_true')
        if name == 'score-batch':
            p.add_argument('--variants-json', required=True)
            p.add_argument('--out', default='')
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == 'serve':
        return serve(args)
    if args.command == 'status':
        print(json.dumps(client_get('/status', args.host, args.port, args.timeout), indent=2, sort_keys=True))
        return 0
    if args.command == 'stop':
        try:
            print(json.dumps(client_post('/shutdown', {}, args.host, args.port, args.timeout), indent=2, sort_keys=True))
        except Exception as exc:
            print(json.dumps({'ok': False, 'error': repr(exc)}, indent=2, sort_keys=True))
        return 0
    manifest_path = Path(args.manifest_path).resolve() if args.manifest_path else default_manifest_path(args.day, args.tickers, args.name)
    if args.command == 'reload':
        print(json.dumps(client_post('/reload', {'manifest_path': str(manifest_path)}, args.host, args.port, args.timeout), indent=2, sort_keys=True))
        return 0
    if args.command == 'score-current':
        payload = client_post('/score-current', {
            'manifest_path': str(manifest_path),
            'start_balance': float(args.start_balance),
            'write_ledger': bool(args.write_ledger),
        }, args.host, args.port, args.timeout)
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0 if payload.get('ok') else 1
    if args.command == 'score-batch':
        variants = _read_json(Path(args.variants_json), [])
        payload = client_post('/score-batch', {
            'manifest_path': str(manifest_path),
            'start_balance': float(args.start_balance),
            'variants': variants.get('variants') if isinstance(variants, dict) else variants,
            'write_ledger': bool(args.write_ledger),
        }, args.host, args.port, args.timeout)
        if args.out:
            _write_json(Path(args.out), payload)
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0 if payload.get('ok') else 1
    raise ValueError(f'unknown command {args.command}')


if __name__ == '__main__':
    raise SystemExit(main())
