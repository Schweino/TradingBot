from __future__ import annotations

from output_paths import output_path

import json
import os

from counterfactual_review import analyze as counterfactual_analyze
from engine_validation import (
    candidate_config,
    exit_policy_candidate_config,
    exit_policy_replay,
    rolling_exit_policy_replay,
    rolling_validation,
    run_validation,
)
from scalp_replay import replay_day


HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = output_path('postmortem')


def build_scoreboard(day: str) -> dict:
    out = {
        'day': day,
        'counterfactual': None,
        'replay': None,
        'validation': None,
        'rolling_validation': None,
        'engine_candidate_config': None,
        'exit_policy_replay': None,
        'rolling_exit_policy_replay': None,
        'exit_policy_candidate_config': None,
        'summary': [],
    }
    try:
        out['counterfactual'] = counterfactual_analyze(day)
    except Exception as e:
        out['counterfactual_error'] = str(e)
    try:
        replay = replay_day(day)
        replay.pop('files_detail', None)
        out['replay'] = replay
    except Exception as e:
        out['replay_error'] = str(e)
    try:
        out['validation'] = run_validation(day, include_replay=False)
    except Exception as e:
        out['validation_error'] = str(e)
    try:
        out['rolling_validation'] = rolling_validation(day, lookback=5)
    except Exception as e:
        out['rolling_validation_error'] = str(e)
    try:
        out['engine_candidate_config'] = candidate_config(day, lookback=5)
    except Exception as e:
        out['engine_candidate_config_error'] = str(e)
    try:
        er = exit_policy_replay(day)
        er.pop('rows', None)
        out['exit_policy_replay'] = er
    except Exception as e:
        out['exit_policy_replay_error'] = str(e)
    try:
        out['rolling_exit_policy_replay'] = rolling_exit_policy_replay(day, lookback=5)
    except Exception as e:
        out['rolling_exit_policy_replay_error'] = str(e)
    try:
        out['exit_policy_candidate_config'] = exit_policy_candidate_config(day, lookback=5)
    except Exception as e:
        out['exit_policy_candidate_config_error'] = str(e)

    cf = out.get('counterfactual') or {}
    actual_trades = (cf.get('actual') or {}).get('trades', 0)
    if cf.get('variant_scoreboard'):
        best = cf['variant_scoreboard'][0]
        out['summary'].append(
            f"Best snapshot variant: {best['variant']} "
            f"({best['trades']} trades, pnl={best['pnl']:+.2f})"
        )
    if actual_trades and cf.get('exit_policy_scoreboard'):
        best = cf['exit_policy_scoreboard'][0]
        out['summary'].append(
            f"Best exit policy estimate: {best['policy']} "
            f"(est pnl={best['estimated_pnl']:+.2f}, "
            f"delta={best['estimated_delta_vs_actual']:+.2f})"
        )
    rp = out.get('replay') or {}
    if rp.get('variant_scoreboard'):
        best = rp['variant_scoreboard'][0]
        out['summary'].append(
            f"Best tick-replay variant: {best['variant']} "
            f"(signals={best['signals']}, avg_end={best['avg_end_return_pct']:+.3f}%)"
        )
    val = out.get('validation') or {}
    gates = val.get('gate_scoreboard') or []
    if gates:
        best = gates[0]
        out['summary'].append(
            f"Best decision-gate variant: {best['variant']} "
            f"(kept={best['kept']}, blocked={best['blocked']}, "
            f"kept_pnl={best['kept_pnl']:+.2f})"
        )
    rolling = out.get('rolling_validation') or {}
    rgates = rolling.get('gate_scoreboard') or []
    if rgates:
        best = rgates[0]
        out['summary'].append(
            f"Best rolling gate variant: {best['variant']} "
            f"({best['days_positive']}/{best['days_seen']} positive days, "
            f"blocked={best['blocked']}, delta={best['estimated_delta_vs_actual']:+.2f})"
        )
    cand = out.get('engine_candidate_config') or {}
    if cand.get('candidates'):
        out['summary'].append(
            f"Engine gate candidates ready for human review: {len(cand['candidates'])}"
        )
    er = out.get('exit_policy_replay') or {}
    if er.get('scoreboard'):
        best = er['scoreboard'][0]
        out['summary'].append(
            f"Best tick-capture exit replay: {best['policy']} "
            f"(matched={best['matched']}, triggered={best['triggered']}, "
            f"delta={best['estimated_delta_vs_actual']:+.2f})"
        )
    rer = out.get('rolling_exit_policy_replay') or {}
    if rer.get('scoreboard'):
        best = rer['scoreboard'][0]
        out['summary'].append(
            f"Best rolling exit policy: {best['policy']} "
            f"({best['days_positive']}/{best['days_seen']} positive days, "
            f"matched={best['matched']}, delta={best['estimated_delta_vs_actual']:+.2f}, "
            f"hurt_winners={best['hurt_winners']})"
        )
    ecand = out.get('exit_policy_candidate_config') or {}
    if ecand.get('candidates'):
        out['summary'].append(
            f"Exit-policy candidates ready for human review: {len(ecand['candidates'])}"
        )
    return out


def write_scoreboard(day: str) -> tuple[str, dict]:
    payload = build_scoreboard(day)
    path = os.path.join(OUT_DIR, f'engine_scoreboard_{day}.json')
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, default=str)
    return path, payload


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('day')
    ap.add_argument('--write', action='store_true')
    args = ap.parse_args()
    if args.write:
        path, payload = write_scoreboard(args.day)
        print(path)
    else:
        payload = build_scoreboard(args.day)
    print(json.dumps(payload, indent=2, default=str))


if __name__ == '__main__':
    main()
