#
# Health-check: launch Flask (app.py) if nothing is listening on port 5000.
# Idempotent — safe to call at login, on wake, and every 5 minutes during
# market hours. Invoked by Windows Scheduled Task "Stock Analyzer".
#
$ErrorActionPreference = 'SilentlyContinue'
$logPath = 'C:\xampp\htdocs\Claude\flask_boot.log'
$ts = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'

$conn = Get-NetTCPConnection -LocalPort 5000 -State Listen -ErrorAction SilentlyContinue
if ($conn) {
    # Healthy — nothing to do. Don't log (would spam the file every 5 min).
    exit 0
}

# Port 5000 not listening — relaunch.
try {
    Start-Process -WindowStyle Hidden `
                  -FilePath "$env:ComSpec" `
                  -ArgumentList '/c "cd /d C:\xampp\htdocs\Claude && C:\xampp\htdocs\python.exe app.py >> flask_boot.log 2>&1"' `
                  -WorkingDirectory 'C:\xampp\htdocs\Claude'
    "[$ts] health_check: port 5000 dead, relaunched python.exe app.py with boot-log redirection" |
        Out-File -Append -FilePath $logPath -Encoding utf8
} catch {
    "[$ts] health_check: launch FAILED: $($_.Exception.Message)" |
        Out-File -Append -FilePath $logPath -Encoding utf8
}
