@echo off
cd /d C:\xampp\htdocs\Claude
python step2_adaptive_hunter.py ^
  --compiled-decision-tape C:\xampp\htdocs\Claude\postmortem\backtests\compiled_decision_tapes\compiled_step2_current_live_CLSK-MARA-RIOT_2026-04-06_2026-05-08\manifest.json ^
  --name hunt_100k_live_chain_20260509 ^
  --batch-size 1000 ^
  --target-count 1 ^
  --beat-pct 51.1 ^
  --max-batches 500 ^
  --seed 2026050915 ^
  --seed-json C:\xampp\htdocs\Claude\postmortem\backtests\step2_adaptive_hunter\current_cache_rescore_top250_20260509.json ^
  --skip-robustness-report ^
  1>> C:\xampp\htdocs\Claude\logs\hunt_100k_live_chain_20260509.out.log ^
  2>> C:\xampp\htdocs\Claude\logs\hunt_100k_live_chain_20260509.err.log
