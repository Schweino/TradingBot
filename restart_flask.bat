@echo off
REM Detached Flask restarter. Called by refit_betas.py after rewriting TICKER_BTC_BETAS.
REM Waits briefly so the parent python (refit script) can exit first, then kills
REM any running Flask and relaunches in the background via pythonw.
timeout /t 3 /nobreak >nul
taskkill /F /IM python.exe /T >nul 2>&1
taskkill /F /IM pythonw.exe /T >nul 2>&1
timeout /t 1 /nobreak >nul
start "" /B pythonw "C:\xampp\htdocs\Claude\app.py"
