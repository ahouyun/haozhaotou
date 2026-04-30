$ErrorActionPreference = "Stop"

$root = Resolve-Path $PSScriptRoot
$launcher = Join-Path $root "backend\launch_backend.ps1"

if (-not (Test-Path $launcher)) {
    Write-Host "[ERROR] Missing launcher script: $launcher"
    exit 1
}

$base = "HKCU:\Software\Classes\vaultpro"
$cmdKey = Join-Path $base "shell\open\command"

New-Item -Path $base -Force | Out-Null
Set-ItemProperty -Path $base -Name "(default)" -Value "URL:Vault PRO Launcher Protocol"
Set-ItemProperty -Path $base -Name "URL Protocol" -Value ""

New-Item -Path $cmdKey -Force | Out-Null
$command = "powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$launcher`" `"%1`""
Set-ItemProperty -Path $cmdKey -Name "(default)" -Value $command

Write-Host "[OK] vaultpro:// protocol registered."
Write-Host "     You can now click '连接后端' for one-click backend launch."
