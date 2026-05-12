from __future__ import annotations

import os
import sys

from runtime_guard import SingleInstanceLock, rotate_runtime_logs, status_reachable


HOST = os.getenv('CLAUDE_APP_HOST', '127.0.0.1')
PORT = int(os.getenv('CLAUDE_APP_PORT', '5000'))


def main() -> int:
    lock = SingleInstanceLock()
    if not lock.acquire():
        if status_reachable(f'http://{HOST}:{PORT}/mock/status', timeout=3):
            print(f'[server] existing app is healthy at http://{HOST}:{PORT}', flush=True)
            return 0
        print('[server] refused to start: app lock is held but status is unreachable', flush=True)
        return 2
    try:
        rotate_runtime_logs()
        from app import app, boot_app

        boot_app()
        try:
            from waitress import serve  # type: ignore
        except Exception:
            print('[server] waitress unavailable; using Flask local server with debug disabled', flush=True)
            app.run(host=HOST, port=PORT, debug=False, use_reloader=False, threaded=True)
        else:
            threads = int(os.getenv('CLAUDE_APP_THREADS', '8'))
            print(f'[server] serving with waitress on http://{HOST}:{PORT} threads={threads}', flush=True)
            serve(app, host=HOST, port=PORT, threads=threads)
        return 0
    finally:
        lock.release()


if __name__ == '__main__':
    sys.exit(main())
