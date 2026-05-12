@echo off
REM Detached Flask restarter. Called by refit_betas.py after rewriting TICKER_BTC_BETAS.
REM Waits briefly so the parent python (refit script) can exit first, then kills
REM this app's local server and relaunches it in the background.
timeout /t 3 /nobreak >nul
powershell -NoProfile -ExecutionPolicy Bypass -Command "Get-CimInstance Win32_Process | Where-Object { ($_.CommandLine -like '*C:\xampp\htdocs\Claude\local_server.py*') -or ($_.CommandLine -like '*C:\xampp\htdocs\Claude\app.py*') -or ($_.CommandLine -like '*C:\xampp\htdocs\Claude\refresh_intraday_step2.py*') -or (($_.CommandLine -like '*pythonw.exe*') -and ($_.CommandLine -like '*C:\xampp\htdocs\Claude*')) } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }" >nul 2>&1
timeout /t 1 /nobreak >nul
start "" /MIN cmd /c "C:\xampp\htdocs\Claude\start_flask.bat"
