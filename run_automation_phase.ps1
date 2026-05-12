param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('pre-open', 'start-monitor', 'post-open', 'intraday', 'parity-sentinel', 'pre-flat', 'post-close', 'verify-day')]
    [string]$Phase,

    [string]$Day,

    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ExtraArgs
)

$ErrorActionPreference = 'Continue'
$python = 'C:\xampp\htdocs\python.exe'
$workdir = 'C:\xampp\htdocs\Claude'
$logDir = Join-Path $workdir 'postmortem\automation_logs'
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$safePhase = $Phase -replace '[^A-Za-z0-9_-]', '_'
$logPath = Join-Path $logDir "automation_${safePhase}_${stamp}.log"

$cmdArgs = @('ops.py', $Phase)
if ($Day) {
    $cmdArgs += $Day
}
if ($ExtraArgs) {
    $cmdArgs += $ExtraArgs
}

"[$(Get-Date -Format o)] START $python $($cmdArgs -join ' ')" | Out-File -FilePath $logPath -Encoding utf8
Push-Location $workdir
try {
    $output = & $python @cmdArgs 2>&1
    $code = $LASTEXITCODE
    if ($output) {
        $output | Out-File -FilePath $logPath -Append -Encoding utf8
    }
}
catch {
    $code = 1
    "[$(Get-Date -Format o)] WRAPPER ERROR $($_.Exception.Message)" | Add-Content -Path $logPath -Encoding utf8
}
finally {
    if ($Phase -eq 'post-close') {
        $cleanupDay = $Day
        if (-not $cleanupDay) {
            $cleanupDay = Get-Date -Format 'yyyy-MM-dd'
        }
        $cleanupOutput = & $python -c "import automation_ops, json; print(json.dumps(automation_ops.stop_live_monitor('$cleanupDay'), indent=2))" 2>&1
        if ($cleanupOutput) {
            "[POST-CLOSE CLEANUP]" | Add-Content -Path $logPath -Encoding utf8
            $cleanupOutput | Add-Content -Path $logPath -Encoding utf8
        }
    }
    Pop-Location
}
"[$(Get-Date -Format o)] END exit=$code" | Add-Content -Path $logPath -Encoding utf8
exit $code
