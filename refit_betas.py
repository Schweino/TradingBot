"""
Daily beta refit job. Runs locally — no external API costs beyond Alpaca.
- Pulls 180 days of backtest rows from local Flask /backtest_report
- Fits through-origin β, β_up, β_down for each ticker
- If any value drifted > THRESHOLD from what's hardcoded, rewrites
  TICKER_BTC_BETAS in app.py, logs the change, and restarts Flask.
- Guards with a daily date file so logging in twice doesn't double-run.

Schedule via Windows Task Scheduler (see below).
"""
import json, os, re, sys, time, subprocess, logging, urllib.request, urllib.error
from datetime import date
from pathlib import Path

# ---------- config ----------
APP_DIR   = Path(r'C:\xampp\htdocs\Claude')
APP_PY    = APP_DIR / 'app.py'
LOG_FILE  = APP_DIR / 'beta_refit.log'
GUARD     = APP_DIR / '.last_refit.txt'
FLASK_URL = 'http://127.0.0.1:5000'
TICKERS   = ['CLSK', 'MARA']            # add more when you have enough history
DAYS      = 180
THRESHOLD = 0.05                        # only rewrite when |Δβ| > this
PYTHON    = r'C:\Windows\py.exe'        # launcher; swap for python.exe if preferred
# ----------------------------

logging.basicConfig(
    filename=LOG_FILE, level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
)
log = logging.getLogger(__name__)


def already_ran_today() -> bool:
    if GUARD.exists() and GUARD.read_text().strip() == date.today().isoformat():
        return True
    return False


def stamp_today():
    GUARD.write_text(date.today().isoformat())


def flask_up() -> bool:
    try:
        urllib.request.urlopen(FLASK_URL + '/', timeout=3)
        return True
    except Exception:
        return False


def start_flask():
    log.info('Starting local server...')
    subprocess.Popen(
        [PYTHON, str(APP_DIR / 'local_server.py')],
        cwd=str(APP_DIR),
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | 0x00000008,  # DETACHED
    )
    for _ in range(20):
        time.sleep(1)
        if flask_up():
            log.info('Flask is up.')
            return
    raise RuntimeError('Local server did not start within 20s')


def restart_flask():
    """
    Spawn a detached batch that waits a few seconds (so this script can exit
    cleanly) then kills+restarts Flask. We can't taskkill python.exe from
    inside a python script — it would kill ourselves.
    Skipped when invoked from inside Flask (CLAUDE_REFIT_NO_RESTART=1) — the
    new betas will be picked up on the next natural Flask restart (wake/reboot).
    """
    if os.environ.get('CLAUDE_REFIT_NO_RESTART') == '1':
        log.info('Skipping restart (in-process invocation). '
                 'New betas will load on next Flask start.')
        return
    bat = APP_DIR / 'restart_flask.bat'
    log.info(f'Spawning detached restarter: {bat.name}')
    subprocess.Popen(
        ['cmd', '/c', 'start', '""', '/B', str(bat)],
        cwd=str(APP_DIR),
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | 0x00000008,  # DETACHED
    )


def fetch_rows(ticker: str):
    req = urllib.request.Request(
        FLASK_URL + '/backtest_report',
        data=json.dumps({'ticker': ticker, 'days': DAYS}).encode(),
        headers={'Content-Type': 'application/json'},
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        data = json.loads(r.read())
    if 'error' in data:
        raise RuntimeError(f'{ticker}: {data["error"]}')
    return data['rows']


def fit_betas(rows):
    """Through-origin β overall + per-direction from (btc_move_pct, actual_move_pct)."""
    pairs = [(r['btc_move_pct'], r['actual_move_pct']) for r in rows
             if r.get('actual_move_pct') is not None]
    if len(pairs) < 20:
        return None

    def beta(subset):
        if len(subset) < 5:
            return None
        sx2 = sum(x*x for x, _ in subset)
        sxy = sum(x*y for x, y in subset)
        return round(sxy / sx2, 3) if sx2 > 0 else None

    # R² on overall through-origin fit
    b = beta(pairs)
    y_mean = sum(y for _, y in pairs) / len(pairs)
    ss_tot = sum((y - y_mean) ** 2 for _, y in pairs)
    ss_res = sum((y - b * x) ** 2 for x, y in pairs)
    r2 = round(1 - ss_res / ss_tot, 3) if ss_tot > 0 else 0

    up   = [(x, y) for x, y in pairs if x >=  0.3]
    down = [(x, y) for x, y in pairs if x <= -0.3]
    return {
        'beta':       b,
        'beta_up':    beta(up),
        'beta_down':  beta(down),
        'r2':         r2,
        'n':          len(pairs),
    }


def read_current_betas() -> dict:
    """Parse TICKER_BTC_BETAS from app.py source."""
    src = APP_PY.read_text(encoding='utf-8')
    m = re.search(r'TICKER_BTC_BETAS\s*=\s*\{(.*?)\n\}', src, re.DOTALL)
    if not m:
        raise RuntimeError('Could not locate TICKER_BTC_BETAS in app.py')
    block = m.group(1)
    out = {}
    for line in block.splitlines():
        tm = re.search(r"'([A-Z]+)':\s*\{([^}]*)\}", line)
        if not tm:
            continue
        vals = dict(re.findall(r"'(\w+)':\s*([-\d.]+)", tm.group(2)))
        out[tm.group(1)] = {k: float(v) for k, v in vals.items()}
    return out


def significant_change(new, old) -> bool:
    if not old:
        return True
    for k in ('beta', 'beta_up', 'beta_down'):
        if k in new and new[k] is not None:
            if abs(new[k] - old.get(k, new[k])) > THRESHOLD:
                return True
    return False


def rewrite_betas(updated: dict):
    """Rewrite the TICKER_BTC_BETAS dict block in app.py."""
    src = APP_PY.read_text(encoding='utf-8')
    lines = ["TICKER_BTC_BETAS = {"]
    lines.append(
        "    # Auto-refitted daily by refit_betas.py. "
        "Asymmetric β — miners react differently to BTC rallies vs sells."
    )
    for tkr in sorted(updated.keys()):
        v = updated[tkr]
        lines.append(
            f"    '{tkr}': {{'beta': {v['beta']}, 'beta_up': {v['beta_up']}, "
            f"'beta_down': {v['beta_down']}, 'r2': {v['r2']}}},  # n={v['n']}, refit {date.today().isoformat()}"
        )
    lines.append("}")
    new_block = '\n'.join(lines)
    new_src, n = re.subn(
        r'TICKER_BTC_BETAS\s*=\s*\{.*?\n\}',
        new_block, src, count=1, flags=re.DOTALL,
    )
    if n != 1:
        raise RuntimeError('Rewrite failed — regex did not match exactly one block')
    # Backup before overwriting
    backup = APP_DIR / f'app.py.bak-{date.today().isoformat()}'
    if not backup.exists():
        backup.write_text(src, encoding='utf-8')
    APP_PY.write_text(new_src, encoding='utf-8')
    log.info(f'Rewrote TICKER_BTC_BETAS (backup: {backup.name})')


def main():
    log.info('=== Daily beta refit start ===')
    if already_ran_today():
        log.info('Already ran today — skipping.')
        return

    if not flask_up():
        start_flask()

    current = read_current_betas()
    new_betas = {}
    for tkr in TICKERS:
        try:
            rows = fetch_rows(tkr)
            fitted = fit_betas(rows)
        except Exception as e:
            log.error(f'{tkr}: fetch/fit failed — {e}')
            continue
        if fitted is None:
            log.warning(f'{tkr}: not enough data')
            continue
        old = current.get(tkr, {})
        log.info(
            f'{tkr}: new β={fitted["beta"]} up={fitted["beta_up"]} down={fitted["beta_down"]} '
            f'r2={fitted["r2"]} n={fitted["n"]} | '
            f'old β={old.get("beta","?")} up={old.get("beta_up","?")} down={old.get("beta_down","?")}'
        )
        new_betas[tkr] = fitted

    if not new_betas:
        log.error('No tickers successfully fitted.')
        return

    # Merge with current so we don't lose tickers not in TICKERS
    merged = dict(current)
    changed = False
    for tkr, v in new_betas.items():
        if significant_change(v, current.get(tkr)):
            merged[tkr] = v
            log.info(f'{tkr}: SIGNIFICANT CHANGE — will rewrite')
            changed = True
        else:
            log.info(f'{tkr}: within threshold, keeping existing')

    if changed:
        rewrite_betas(merged)
        restart_flask()
    else:
        log.info('No significant drift. app.py unchanged.')

    stamp_today()
    log.info('=== Done ===')


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        log.exception(f'Fatal: {e}')
        sys.exit(1)
