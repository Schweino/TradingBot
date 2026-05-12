@echo off
cd /d C:\xampp\htdocs\Claude
C:\xampp\htdocs\python.exe run_step2_three_hour_hunt.py --name codex_step2_live_only_3h --hours 3 --hunters adaptive local router --batch-size 1000 --max-batches 12 --target-count 100 --cycle-timeout-sec 2400 1>> C:\xampp\htdocs\Claude\logs\codex_step2_live_only_3h.out.log 2>> C:\xampp\htdocs\Claude\logs\codex_step2_live_only_3h.err.log
