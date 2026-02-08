$ErrorActionPreference = 'Stop'

Write-Host "[1/4] Creating virtual environment..."
if (-Not (Test-Path .venv)) {
    python -m venv .venv
}

Write-Host "[2/4] Activating virtual environment and installing deps..."
& .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install pyinstaller

Write-Host "[3/4] Building debug exe (console enabled)..."
pyinstaller --onefile -n SC2ReplayAnalyzer --icon sc2replaytool/ico/SC2RA_multi_sizes.ico --collect-data sc2reader --debug all run_app.py

Write-Host "[4/4] Launching exe for debug output..."
& .\dist\SC2ReplayAnalyzer.exe
