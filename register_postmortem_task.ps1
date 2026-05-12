# Register Windows Scheduled Tasks for the mock_trader trading day.
# Safe to re-run. Removes the old direct postmortem task and installs the
# fuller automation_ops phases so readiness, monitoring, close packet, Google
# Doc, and post-market smoke checks stay in one workflow.

$powershell = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
$workdir = 'C:\xampp\htdocs\Claude'
$runner = Join-Path $workdir 'run_automation_phase.ps1'
$days = @('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday')
$legacyTasks = @('ClaudeMockTraderPostmortem')

foreach ($legacy in $legacyTasks) {
    $existing = Get-ScheduledTask -TaskName $legacy -ErrorAction SilentlyContinue
    if ($existing) {
        Write-Output "Removing legacy task '$legacy'..."
        Unregister-ScheduledTask -TaskName $legacy -Confirm:$false
    }
}

function Register-ClaudeAutomationTask {
    param(
        [Parameter(Mandatory = $true)][string]$TaskName,
        [Parameter(Mandatory = $true)][string]$Phase,
        [Parameter(Mandatory = $true)][string]$At,
        [Parameter(Mandatory = $true)][int]$LimitMinutes,
        [Parameter(Mandatory = $true)][string]$Description
    )

    $existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($existing) {
        Write-Output "Removing existing task '$TaskName'..."
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    }

    $action = New-ScheduledTaskAction `
        -Execute $powershell `
        -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$runner`" $Phase" `
        -WorkingDirectory $workdir

    $trigger = New-ScheduledTaskTrigger `
        -Weekly `
        -DaysOfWeek $days `
        -At $At

    $settings = New-ScheduledTaskSettingsSet `
        -StartWhenAvailable `
        -DontStopOnIdleEnd `
        -ExecutionTimeLimit (New-TimeSpan -Minutes $LimitMinutes) `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -WakeToRun `
        -RestartCount 2 `
        -RestartInterval (New-TimeSpan -Minutes 2) `
        -MultipleInstances IgnoreNew

    $principal = New-ScheduledTaskPrincipal `
        -UserId $env:USERNAME `
        -LogonType Interactive `
        -RunLevel Limited

    Register-ScheduledTask `
        -TaskName $TaskName `
        -Action $action `
        -Trigger $trigger `
        -Settings $settings `
        -Principal $principal `
        -Description $Description `
        | Out-Null

    $info = Get-ScheduledTaskInfo -TaskName $TaskName
    $task = Get-ScheduledTask -TaskName $TaskName
    Write-Output "Registered '$TaskName'."
    Write-Output "  State          : $($task.State)"
    Write-Output "  Next run time  : $($info.NextRunTime)"
    Write-Output "  Action         : $($task.Actions[0].Execute) $($task.Actions[0].Arguments)"
    Write-Output "  Working dir    : $($task.Actions[0].WorkingDirectory)"
    Write-Output "  Days           : $($task.Triggers[0].DaysOfWeek)"
}

Register-ClaudeAutomationTask `
    -TaskName 'ClaudeMockTraderPreOpen' `
    -Phase 'pre-open' `
    -At '8:15AM' `
    -LimitMinutes 25 `
    -Description 'Pre-open readiness: ensure app, no-surprises smoke, refresh readiness artifacts, and start live monitor plus parity sentinel. Mon-Fri 08:15 CT.'

Register-ClaudeAutomationTask `
    -TaskName 'ClaudeMockTraderPostOpen' `
    -Phase 'post-open' `
    -At '8:35AM' `
    -LimitMinutes 15 `
    -Description 'Post-open sanity: broker/app/feed smoke plus post-open checkpoint artifacts. Mon-Fri 08:35 CT.'

Register-ClaudeAutomationTask `
    -TaskName 'ClaudeMockTraderIntradayStep2Warm0930' `
    -Phase 'intraday' `
    -At '9:30AM' `
    -LimitMinutes 45 `
    -Description 'Intraday Step 2 parity refresh: live tape, incremental market partitions, compiled score, parity sentinel, canonical opportunity ledger, diff classifier, cache layers, and artifact registry. Mon-Fri 09:30 CT.'

Register-ClaudeAutomationTask `
    -TaskName 'ClaudeMockTraderIntradayStep2Warm1100' `
    -Phase 'intraday' `
    -At '11:00AM' `
    -LimitMinutes 45 `
    -Description 'Intraday Step 2 parity refresh: live tape, incremental market partitions, compiled score, parity sentinel, canonical opportunity ledger, diff classifier, cache layers, and artifact registry. Mon-Fri 11:00 CT.'

Register-ClaudeAutomationTask `
    -TaskName 'ClaudeMockTraderIntradayStep2Warm1230' `
    -Phase 'intraday' `
    -At '12:30PM' `
    -LimitMinutes 45 `
    -Description 'Intraday Step 2 parity refresh: live tape, incremental market partitions, compiled score, parity sentinel, canonical opportunity ledger, diff classifier, cache layers, and artifact registry. Mon-Fri 12:30 CT.'

Register-ClaudeAutomationTask `
    -TaskName 'ClaudeMockTraderIntradayStep2Warm1400' `
    -Phase 'intraday' `
    -At '2:00PM' `
    -LimitMinutes 45 `
    -Description 'Intraday Step 2 parity refresh: live tape, incremental market partitions, compiled score, parity sentinel, canonical opportunity ledger, diff classifier, cache layers, and artifact registry. Mon-Fri 14:00 CT.'

Register-ClaudeAutomationTask `
    -TaskName 'ClaudeMockTraderPreFlat' `
    -Phase 'pre-flat' `
    -At '2:50PM' `
    -LimitMinutes 10 `
    -Description 'Pre-flat checkpoint shortly before the 14:55 CT hard-flat cutoff. Mon-Fri 14:50 CT.'

Register-ClaudeAutomationTask `
    -TaskName 'ClaudeMockTraderPostClose' `
    -Phase 'post-close' `
    -At '3:10PM' `
    -LimitMinutes 120 `
    -Description 'Full post-close workflow: market replay cache, latency shards, compiled Step 2, canonical opportunity ledger, diff classifier, parity report, cache layers, artifact registry, Google Doc summary, context packet, and post-market smoke. Mon-Fri 15:10 CT.'

Register-ClaudeAutomationTask `
    -TaskName 'ClaudeMockTraderVerifyDay' `
    -Phase 'verify-day' `
    -At '4:30PM' `
    -LimitMinutes 15 `
    -Description 'End-of-day watchdog: verifies all scheduled phase artifacts plus Step 2/Live architecture outputs were written successfully. Mon-Fri 16:30 CT.'
