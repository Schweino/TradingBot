"""Audit that live mock-engine config matches the Step 2 parity contract."""
from __future__ import annotations

import json
from pathlib import Path

import step2_parity_contract


HERE = Path(__file__).resolve().parent
CONFIG_PATH = HERE / 'trading_config.json'
MOCK_TRADER_PATH = HERE / 'mock_trader.py'
WS_SCALP_PATH = HERE / 'ws_scalp.py'


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding='utf-8'))


def _contains(path: Path, text: str) -> bool:
    return text in path.read_text(encoding='utf-8')


def _check(name: str, ok: bool, detail: str = '') -> dict:
    return {'name': name, 'ok': bool(ok), 'detail': detail}


def main() -> int:
    cfg = _read_json(CONFIG_PATH)
    smart = cfg.get('smart_entry') or {}
    adaptive = cfg.get('adaptive_management') or {}
    contract = step2_parity_contract.contract(cfg)
    sim = step2_parity_contract.sim_config(cfg)

    checks = [
        _check('execution_mode_step2_signal_scan_live',
               cfg.get('execution_mode') == 'step2_signal_scan_live',
               str(cfg.get('execution_mode'))),
        _check('active_scoring_profile_enabled',
               bool((smart.get('active_scoring_profile') or {}).get('enabled'))),
        _check('step2_requires_no_conviction_gate',
               contract.get('require_conviction') is False and sim.get('require_conviction') == 0.0),
        _check('step2_no_exec_score_gate',
               float(contract.get('min_exec_score') or 0.0) == 0.0 and float(smart.get('execution_quality_min') or 0.0) == 0.0),
        _check('step2_no_density_caps',
               int(contract.get('max_trades_per_day') or 0) == 0
               and int(contract.get('max_trades_per_ticker_day') or 0) == 0),
        _check('step2_close_based_reentry_cooldown',
               int(contract.get('same_ticker_reentry_cooldown_sec') or 0) == 5
               and _contains(MOCK_TRADER_PATH, 'step2_execution_contract.reentry_cooldown_reason(')
               and _contains(HERE / 'step2_execution_contract.py', 'until = closed_at + cooldown')),
        _check('step2_conditional_time_stop_disabled',
               bool(contract.get('conditional_time_stop_enabled')) is False
               and bool(sim.get('conditional_time_stop_enabled')) is False
               and bool(cfg.get('conditional_time_stop_enabled', False)) is False),
        _check('step2_internal_compounded_sizing',
               contract.get('sizing_mode') == 'compounded_internal_balance'
               and _contains(MOCK_TRADER_PATH, 'if STEP2_SIGNAL_SCAN_LIVE_MODE:')
               and _contains(MOCK_TRADER_PATH, "return float(self.state.get('balance', 0) or 0) / max(1, len(TICKERS))")),
        _check('smart_entry_state_machine_disabled',
               smart.get('state_machine_enabled') is False),
        _check('smart_entry_live_gates_disabled',
               smart.get('live_quality_gate_enabled') is False
               and smart.get('pre_submit_check_enabled') is False
               and smart.get('spread_quality_gate_enabled') is False),
        _check('adaptive_risk_gates_disabled',
               adaptive.get('profit_protect_enabled') is False
               and adaptive.get('short_conviction_decay_enabled') is False
               and adaptive.get('long_conviction_decay_enabled') is False
               and adaptive.get('execution_risk_pause_enabled') is False
               and adaptive.get('shadow_exit_policies_enabled') is False
               and int(adaptive.get('setup_loss_pause_count') or 0) == 0
               and int(adaptive.get('max_consecutive_losses') or 0) == 0
               and int(adaptive.get('risk_off_cooldown_min') or 0) == 0),
        _check('ws_scalp_setup_side_cooldown_present',
               _contains(WS_SCALP_PATH, "key = f\"{tkr}:{sig['side']}:{sig.get('setup_type', 'unknown')}\"")
               and _contains(WS_SCALP_PATH, "SETUP_COOLDOWN_SEC.get(sig.get('setup_type'), 30)")),
        _check('mock_one_open_or_pending_per_ticker',
               _contains(MOCK_TRADER_PATH, "if tkr in self.state['positions']")
               and _contains(MOCK_TRADER_PATH, 'if tkr in pending:')),
    ]

    failed = [row for row in checks if not row['ok']]
    payload = {
        'ok': not failed,
        'step2_parity_contract_hash': step2_parity_contract.contract_hash(contract),
        'step2_parity_contract': contract,
        'checks': checks,
        'failed': failed,
        'broker_or_feed_only_live_differences': [
            'kill_switch/running/broker_api_degraded can stop live but are operational, not strategy logic',
            'Alpaca buying power, shortability, order submit/fill errors can block live entries',
            'pending entry state can hold a ticker while an order is in flight',
            'feed/data gaps can prevent live from seeing a Step 2 historical signal',
        ],
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload['ok'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
