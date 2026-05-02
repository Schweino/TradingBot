@echo off
REM Auto-launches Flask (ws_scalp + mock_trader autostart on boot).
REM Registered as Windows Scheduled Task trigger=AtLogon.
REM Logs to flask_boot.log next to app.py.
cd /d C:\xampp\htdocs\Claude
C:\xampp\htdocs\python.exe app.py >> flask_boot.log 2>&1
