# Monday Launch Checklist - 2026-05-04

Overall: NOT OK

## Checks
- [x] app_reachable: None
- [x] broker_flat_and_ready: []
- [x] config_frozen_or_freezable: 19ff5d260cc84c9fc57d2c2fb0816217096eb11a82324c65a9e9f8148ced91a1
- [ ] config_not_changed_since_freeze: trading_config_changed_since_freeze
- [x] pre_market_artifacts_present: []
- [x] now_status_calm_or_warning: calm
- [x] no_open_positions: {}
- [x] no_pending_entries: {}

## Commands
- 08:25 CT: `python smoke_check.py --mode no-surprises`
- 08:30 CT: `python live_monitor.py --loop --interval-sec 30`
- 08:35 CT: `python smoke_check.py --mode post-open`
- 12:00 CT: `python live_monitor.py 2026-05-04 --checkpoint --checkpoint-label noon`
- 14:55 CT: `hard flat should trigger automatically`
- 15:05 CT: `python monday_close_packet.py`

## Open First
- `C:\xampp\htdocs\Claude\postmortem\NOW_STATUS.json`
- `C:\xampp\htdocs\Claude\postmortem\MONDAY_REVIEW_START_HERE_2026-05-04.json`
- `C:\xampp\htdocs\Claude\postmortem\monday_live_review_2026-05-04.json`
- `C:\xampp\htdocs\Claude\postmortem\trade_alerts_2026-05-04.json`
- `C:\xampp\htdocs\Claude\postmortem\daily_review_gate_2026-05-04.json`
