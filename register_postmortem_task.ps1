# Register Windows Scheduled Task for daily mock_trader post-mortem.
# Runs Mon-Fri at 15:05 local (CT, since the box is configured to CT).

$taskName = 'ClaudeMockTraderPostmortem'

# Drop any pre-existing task with this name (safe re-runs)
$existing = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if ($existing) {
    Write-Output "Removing existing task '$taskName'..."
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
}

$action = New-ScheduledTaskAction `
    -Execute 'C:\xampp\htdocs\python.exe' `
    -Argument 'daily_postmortem.py' `
    -WorkingDirectory 'C:\xampp\htdocs\Claude'

$trigger = New-ScheduledTaskTrigger `
    -Weekly `
    -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday `
    -At '3:05PM'

$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -DontStopOnIdleEnd `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 30) `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries

$principal = New-ScheduledTaskPrincipal `
    -UserId $env:USERNAME `
    -LogonType Interactive `
    -RunLevel Limited

Register-ScheduledTask `
    -TaskName $taskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal `
    -Description 'Daily mock_trader post-mortem at 15:05 CT, Mon-Fri. Writes JSON+TXT to postmortem/ and appends to Google Doc.' `
    | Out-Null

Write-Output "Registered '$taskName'."
$info = Get-ScheduledTaskInfo -TaskName $taskName
$task = Get-ScheduledTask -TaskName $taskName
Write-Output "  State          : $($task.State)"
Write-Output "  Next run time  : $($info.NextRunTime)"
Write-Output "  Action         : $($task.Actions[0].Execute) $($task.Actions[0].Arguments)"
Write-Output "  Working dir    : $($task.Actions[0].WorkingDirectory)"
Write-Output "  Days           : $($task.Triggers[0].DaysOfWeek)"
