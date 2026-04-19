param(
    [string]$PythonVersion = "3.13",
    [string]$VenvDir = ".venv",
    [string]$Extras = "alpaca,live,backtest,dev",
    [string]$ExtraPipPackages = "",
    [switch]$InstallMlTestDeps,
    [switch]$Recreate
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repoRoot

try {
    if ($Recreate -and (Test-Path $VenvDir)) {
        Write-Host "[flux-env] Removing existing virtual environment: $VenvDir"
        Remove-Item $VenvDir -Recurse -Force
    }

    if (-not (Test-Path $VenvDir)) {
        $pyLauncher = Get-Command py -ErrorAction SilentlyContinue
        if ($pyLauncher) {
            Write-Host "[flux-env] Creating venv with py -$PythonVersion ..."
            & py "-$PythonVersion" -m venv $VenvDir
        }
        else {
            Write-Host "[flux-env] 'py' launcher not found, fallback to 'python -m venv' ..."
            python -m venv $VenvDir
        }
    }

    $activateScript = Join-Path $repoRoot "$VenvDir\Scripts\Activate.ps1"
    if (-not (Test-Path $activateScript)) {
        throw "Activation script not found: $activateScript"
    }

    . $activateScript

    $venvVersion = python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
    if ($venvVersion -ne $PythonVersion) {
        Write-Warning "[flux-env] Venv uses Python $venvVersion (requested $PythonVersion). Continuing anyway."
    }

    python -m pip install --upgrade pip setuptools wheel
    pip install -e ".[${Extras}]"

    if ($InstallMlTestDeps) {
        pip install scikit-learn joblib
    }

    if ($ExtraPipPackages.Trim()) {
        pip install $ExtraPipPackages
    }

    Write-Host ""
    Write-Host "[flux-env] Environment is ready and activated."
    Write-Host "[flux-env] Python executable: $((Get-Command python).Source)"
    Write-Host "[flux-env] Pip executable:    $((Get-Command pip).Source)"
    python -c "import sys; print('[flux-env] Python version:   ' + sys.version.replace('\n',' '))"
}
finally {
    Pop-Location
}
