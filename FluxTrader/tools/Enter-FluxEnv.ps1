Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$activateScript = Join-Path $repoRoot ".venv\Scripts\Activate.ps1"

if (-not (Test-Path $activateScript)) {
    Write-Host "[flux-env] Missing .venv. Run: .\tools\Setup-FluxEnv.ps1"
    exit 1
}

Set-Location $repoRoot
. $activateScript

Write-Host "[flux-env] Activated in $repoRoot"
Write-Host "[flux-env] Python: $((Get-Command python).Source)"
Write-Host "[flux-env] Pip:    $((Get-Command pip).Source)"
