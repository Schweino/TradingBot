@echo off
REM Auto-launches Flask (ws_scalp + mock_trader autostart on boot).
REM Registered as Windows Scheduled Task trigger=AtLogon.
REM Logs to flask_boot.log next to local_server.py.
cd /d C:\xampp\htdocs\Claude
C:\xampp\htdocs\python.exe -c "from runtime_guard import rotate_runtime_logs; rotate_runtime_logs()"
C:\xampp\htdocs\python.exe local_server.py >> flask_boot.log 2>&1
