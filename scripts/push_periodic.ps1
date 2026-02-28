param(
    [int]$IntervalMinutes = 15,
    [string]$Remote = "origin",
    [string]$Branch = "main",
    [string]$MessagePrefix = "Checkpoint update"
)

if ($IntervalMinutes -lt 1) {
    Write-Error "IntervalMinutes must be >= 1"
    exit 1
}

Write-Host "Starting periodic push loop every $IntervalMinutes minute(s)..."
Write-Host "Remote: $Remote | Branch: $Branch"

while ($true) {
    git add -A
    $status = git status --porcelain

    if (-not [string]::IsNullOrWhiteSpace($status)) {
        $stamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        $msg = "$MessagePrefix - $stamp"
        git commit -m $msg | Out-Host
        if ($LASTEXITCODE -eq 0) {
            git push $Remote $Branch | Out-Host
        }
    } else {
        Write-Host "$(Get-Date -Format 'HH:mm:ss') No changes to push."
    }

    Start-Sleep -Seconds ($IntervalMinutes * 60)
}
