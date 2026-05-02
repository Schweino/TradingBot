from __future__ import annotations

import json
import sys
from urllib.error import HTTPError
from urllib.request import Request, urlopen


BASE = 'http://127.0.0.1:5000'


def check(path: str, method: str = 'GET', body: dict | None = None,
          expect_status: tuple[int, ...] = (200,)) -> bool:
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode('utf-8')
        headers['Content-Type'] = 'application/json'
    req = Request(BASE + path, data=data, headers=headers, method=method)
    try:
        with urlopen(req, timeout=5) as resp:
            payload = resp.read()
            ok = resp.status in expect_status
            print(f'{"OK" if ok else "FAIL"}: {method} {path} -> {resp.status} ({len(payload)} bytes)')
            return ok
    except HTTPError as e:
        ok = e.code in expect_status
        print(f'{"OK" if ok else "FAIL"}: {method} {path} -> {e.code}')
        return ok
    except Exception as e:
        print(f'FAIL: {method} {path} -> {e}')
        return False


def main() -> int:
    ok = True
    ok &= check('/')
    ok &= check('/mock/status')
    ok &= check('/mock/status?full=1')
    ok &= check('/mock/trades?limit=5')
    ok &= check('/mock/quality')
    ok &= check('/mock/setup-capital')
    ok &= check('/mock/ev-table/2026-05-01')
    ok &= check('/scalp/status')
    ok &= check('/watcher/status')
    # POST endpoints with validation errors prove route wiring without mutating state.
    ok &= check('/scalp/start', method='POST', body={}, expect_status=(400,))
    ok &= check('/watcher/start', method='POST', body={}, expect_status=(400,))
    ok &= check('/watcher/enter', method='POST', body={}, expect_status=(400,))
    ok &= check('/watcher/exit', method='POST', body={}, expect_status=(400,))
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())
