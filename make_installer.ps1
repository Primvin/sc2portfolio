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

Write-Host "[4/5] Building exe..."
& $py -m PyInstaller --noconfirm --clean --onefile --windowed -n SC2ReplayAnalyzer --icon sc2replaytool/ico/SC2RA_multi_sizes.ico --collect-data sc2reader --hidden-import mpyq run_app.py

Write-Host "[5/5] Preparing Inno Setup script..."
$iss = @'
[Setup]
AppName=SC2ReplayAnalyzer
AppVersion=0.1.0
DefaultDirName={pf}\SC2ReplayAnalyzer
DefaultGroupName=SC2ReplayAnalyzer
OutputDir=dist
OutputBaseFilename=SC2ReplayAnalyzer-Setup
Compression=lzma
SolidCompression=yes
SetupIconFile=sc2replaytool\\ico\\SC2RA_multi_sizes.ico

[Files]
Source: "dist\\SC2ReplayAnalyzer.exe"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\\SC2ReplayAnalyzer"; Filename: "{app}\\SC2ReplayAnalyzer.exe"; IconFilename: "{app}\\SC2ReplayAnalyzer.exe"
Name: "{commondesktop}\\SC2ReplayAnalyzer"; Filename: "{app}\\SC2ReplayAnalyzer.exe"; IconFilename: "{app}\\SC2ReplayAnalyzer.exe"; Tasks: desktopicon

[Tasks]
Name: "desktopicon"; Description: "Create a &desktop icon"; GroupDescription: "Additional icons:"; Flags: unchecked
'@

$issPath = Join-Path (Get-Location) 'installer.iss'
$iss | Set-Content -Path $issPath -Encoding UTF8

Write-Host "Installer script written to installer.iss"
Write-Host "Now open installer.iss with Inno Setup Compiler and build the installer."
