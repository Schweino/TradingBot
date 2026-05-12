# Monday Launch Checklist - 2026-05-11

Overall: NOT OK

## Checks
- [ ] app_reachable: <urlopen error [WinError 10061] No connection could be made because the target machine actively refused it>
- [ ] broker_flat_and_ready: [{'name': 'app_status_reachable', 'ok': False, 'detail': '<urlopen error [WinError 10061] No connection could be made because the target machine actively refused it>'}, {'name': 'alpaca_equity_visible', 'ok': False}]
- [x] config_frozen_or_freezable: 8c43482868d741d226d5880f6c338a0ca23058d4c8aab5766bd15f0d17bfe07c
- [ ] config_not_changed_since_freeze: trading_config_changed_since_freeze
- [x] pre_market_artifacts_present: []
- [x] now_status_calm_or_warning: calm
- [x] no_open_positions: None
- [x] no_pending_entries: None

## Commands
- 08:25 CT: `python smoke_check.py --mode no-surprises`
- 08:30 CT: `python live_monitor.py --loop --interval-sec 30 --parity-sentinel`
- 08:35 CT: `python smoke_check.py --mode post-open`
- 12:00 CT: `python live_monitor.py 2026-05-11 --checkpoint --checkpoint-label noon`
- 14:55 CT: `hard flat should trigger automatically`
- 15:05 CT: `python monday_close_packet.py`

## Open First
- `C:\xampp\htdocs\Claude\postmortem\NOW_STATUS.json`
- `C:\xampp\htdocs\Claude\postmortem\MONDAY_REVIEW_START_HERE_2026-05-11.json`
- `C:\xampp\htdocs\Claude\postmortem\monday_live_review_2026-05-11.json`
- `C:\xampp\htdocs\Claude\postmortem\trade_alerts_2026-05-11.json`
- `C:\xampp\htdocs\Claude\postmortem\daily_review_gate_2026-05-11.json`
