param(
    [string]$Uri = ""
)

$ErrorActionPreference = "SilentlyContinue"

$projectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$startBat = Join-Path $projectRoot "start.bat"

if (-not (Test-Path $startBat)) {
    exit 1
}

# Backend already alive: no-op.
try {
    $health = Invoke-RestMethod -Uri "http://127.0.0.1:8765/health" -Method GET -TimeoutSec 2
    if ($health -and $health.ok) {
        exit 0
    }
} catch {
    # continue
}

# Launch backend without showing cmd window.
Start-Process -FilePath "cmd.exe" -ArgumentList "/c `"$startBat`"" -WorkingDirectory $projectRoot -WindowStyle Hidden
exit 0
