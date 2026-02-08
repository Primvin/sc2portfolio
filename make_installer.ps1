$ErrorActionPreference = 'Stop'

Write-Host "[1/4] Creating virtual environment..."
if (-Not (Test-Path .venv)) {
    python -m venv .venv
}

Write-Host "[2/4] Activating virtual environment and installing deps..."
& .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install pyinstaller

Write-Host "[3/4] Building exe..."
pyinstaller --onefile --windowed -n SC2ReplayAnalyzer run_app.py

Write-Host "[4/4] Preparing Inno Setup script..."
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

[Files]
Source: "dist\\SC2ReplayAnalyzer.exe"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\\SC2ReplayAnalyzer"; Filename: "{app}\\SC2ReplayAnalyzer.exe"
Name: "{commondesktop}\\SC2ReplayAnalyzer"; Filename: "{app}\\SC2ReplayAnalyzer.exe"; Tasks: desktopicon

[Tasks]
Name: "desktopicon"; Description: "Create a &desktop icon"; GroupDescription: "Additional icons:"; Flags: unchecked
'@

$issPath = Join-Path (Get-Location) 'installer.iss'
$iss | Set-Content -Path $issPath -Encoding UTF8

Write-Host "Installer script written to installer.iss"
Write-Host "Now open installer.iss with Inno Setup Compiler and build the installer."
