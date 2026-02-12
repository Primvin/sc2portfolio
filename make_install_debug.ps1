$ErrorActionPreference = 'Stop'

Write-Host "[1/5] Creating virtual environment..."
if (-Not (Test-Path .venv)) {
    python -m venv .venv
}

$py = Join-Path (Get-Location) ".venv\Scripts\python.exe"
if (-Not (Test-Path $py)) {
    throw "Virtual environment python not found at $py"
}

Write-Host "[2/5] Upgrading pip..."
& $py -m pip install --upgrade pip

Write-Host "[3/5] Installing dependencies..."
& $py -m pip install -r requirements.txt pyinstaller

Write-Host "[4/5] Building debug exe (console enabled)..."
& $py -m PyInstaller --noconfirm --clean --onefile -n SC2ReplayAnalyzer --icon sc2replaytool/ico/SC2RA_multi_sizes.ico --collect-data sc2reader --hidden-import mpyq --debug all run_app.py

Write-Host "[5/5] Launching exe for debug output..."
& .\dist\SC2ReplayAnalyzer.exe
