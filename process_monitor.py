"""
process_monitor.py — crash / shutdown / death diagnostics for app.py.

Installs four complementary loggers so we can always answer "what
happened to the Flask process?" after the fact:

  1. Heartbeat file  — thread that rewrites a small JSON file every 10s
                       with (pid, iso_time, monotonic). Survives any
                       form of death. Mtime of the file tells you
                       exactly when the process stopped writing.

  2. faulthandler    — Python std-lib. Catches segfaults / C-level
                       aborts / other hard crashes that bypass the
                       normal exception machinery; dumps a native
                       traceback to a file descriptor we keep open.

  3. excepthooks     — sys.excepthook + threading.excepthook. Logs
                       any exception that propagates all the way up
                       (including from background threads) before
                       the interpreter exits.

  4. signal + atexit — SIGTERM / SIGINT / SIGBREAK on Windows, plus
                       atexit. Distinguishes "the OS / a user sent a
                       signal" from "I just vanished without warning."

On startup, we also read the previous heartbeat file (if any) and log
`PROC BOOT: previous heartbeat was at T, downtime=X seconds` so every
boot tells you how long the last outage lasted.
"""

import atexit
import faulthandler
import json
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone

log = logging.getLogger('proc_monitor')
log.setLevel(logging.INFO)
if not log.handlers:
    _h = logging.FileHandler(
        os.path.join(os.path.dirname(os.path.abspath(__file__)),
                     'proc_monitor.log'),
        encoding='utf-8',
    )
    _h.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    log.addHandler(_h)

HEARTBEAT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              'proc_heartbeat.json')
CRASH_LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              'proc_crash.log')
HEARTBEAT_INTERVAL = 10  # seconds


def _read_prev_heartbeat():
    try:
        with open(HEARTBEAT_PATH) as f:
            return json.load(f)
    except Exception:
        return None


def _write_heartbeat():
    try:
        tmp = HEARTBEAT_PATH + '.tmp'
        with open(tmp, 'w') as f:
            json.dump({
                'pid':  os.getpid(),
                'iso':  datetime.now(timezone.utc).astimezone().isoformat(timespec='seconds'),
                'mono': time.monotonic(),
                'unix': int(time.time()),
            }, f)
        os.replace(tmp, HEARTBEAT_PATH)
    except Exception as e:
        log.warning(f'heartbeat write failed: {e}')


def _heartbeat_loop():
    while True:
        _write_heartbeat()
        time.sleep(HEARTBEAT_INTERVAL)


def _signal_handler(signum, frame):
    # Resolve signal name for clarity (Windows has limited signal set).
    try:
        name = signal.Signals(signum).name
    except Exception:
        name = f'signal_{signum}'
    log.warning(f'PROC SIGNAL: received {name} (signum={signum}) — exiting')
    # Re-raise default behavior so Python exits cleanly and atexit runs.
    sys.exit(128 + signum)


def _atexit_handler():
    log.info(f'PROC EXIT: pid={os.getpid()} clean-exit atexit fired '
             f'at {datetime.now().isoformat(timespec="seconds")}')


def _excepthook(exc_type, exc_value, exc_tb):
    log.error('PROC UNHANDLED EXCEPTION (main thread):',
              exc_info=(exc_type, exc_value, exc_tb))
    # Also chain to default hook so repr goes to stderr as usual.
    sys.__excepthook__(exc_type, exc_value, exc_tb)


def _thread_excepthook(args):
    log.error(f'PROC UNHANDLED EXCEPTION (thread={args.thread.name}):',
              exc_info=(args.exc_type, args.exc_value, args.exc_traceback))


def install():
    """Install all diagnostics. Call once from app.py at startup."""

    # ── 1. Heartbeat: check previous, then start rewriter thread ──
    prev = _read_prev_heartbeat()
    if prev:
        try:
            prev_iso = prev.get('iso', '?')
            prev_unix = int(prev.get('unix', 0) or 0)
            now_unix = int(time.time())
            downtime = max(0, now_unix - prev_unix)
            log.info(f'PROC BOOT: previous heartbeat pid={prev.get("pid")} '
                     f'at {prev_iso} — downtime ~{downtime}s '
                     f'({downtime//60}m{downtime%60}s)')
        except Exception as e:
            log.warning(f'heartbeat-prev-read: {e}')
    else:
        log.info('PROC BOOT: no previous heartbeat (fresh install or cleared)')

    _write_heartbeat()
    t = threading.Thread(target=_heartbeat_loop, name='heartbeat', daemon=True)
    t.start()

    # ── 2. faulthandler: C-level crashes go here ──
    try:
        crash_fd = open(CRASH_LOG_PATH, 'a', buffering=1)
        crash_fd.write(f'\n=== faulthandler attached pid={os.getpid()} at {datetime.now().isoformat()} ===\n')
        faulthandler.enable(file=crash_fd, all_threads=True)
    except Exception as e:
        log.warning(f'faulthandler enable failed: {e}')

    # ── 3. Exception hooks ──
    sys.excepthook = _excepthook
    try:
        threading.excepthook = _thread_excepthook
    except Exception:
        pass  # Python <3.8

    # ── 4. Signals + atexit ──
    for sig_name in ('SIGTERM', 'SIGINT', 'SIGBREAK'):
        sig = getattr(signal, sig_name, None)
        if sig is not None:
            try:
                signal.signal(sig, _signal_handler)
            except Exception as e:
                log.warning(f'signal handler install failed for {sig_name}: {e}')
    atexit.register(_atexit_handler)

    log.info(f'PROC MONITOR installed: heartbeat={HEARTBEAT_PATH} '
             f'crashlog={CRASH_LOG_PATH} pid={os.getpid()}')
