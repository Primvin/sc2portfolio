# sc2portfolio
Handle your SC2 replays with ease.

## SC2 Replay Analyzer (Python)
This project includes a simple replay organizer built on `sc2reader` with both GUI (Tkinter) and CLI.

### Features
- Scan a folder of `.SC2Replay` files
- Filter by matchup
- Search by player name and map
- Tag favorites
- Tag build orders (manual override) and auto-detect simple openings
- Step-by-step build order filter (Tech or General)
- Proxy detection (distance from main to first building)
- Export filtered results to CSV
- Works on Windows and Linux

### Run (GUI)
From the repo root:
```bash
python -m sc2replaytool.app
```

### Run (CLI)
```bash
python -m sc2replaytool.cli --replays /path/to/replays --scan
python -m sc2replaytool.cli --list --matchup PvT
python -m sc2replaytool.cli --list --player Maru --map \"Cosmic Sapphire\"
python -m sc2replaytool.cli --list --proxy --export-csv /tmp/replays.csv
python -m sc2replaytool.cli --replays /path/to/replays --scan --proxy-threshold 35
```

### Data Files
The app creates a `data/` folder in the current working directory:
- `data/replay_index.json`
- `data/replay_tags.json`
- `data/settings.json`

### Build Windows Executable (PyInstaller)
From the repo root:
```bash
pip install pyinstaller
pyinstaller --onefile --windowed -n SC2ReplayAnalyzer run_app.py
```
The executable will be in `dist/SC2ReplayAnalyzer.exe`.

If you want the local `sc2reader` included, add:
```bash
pyinstaller --onefile --windowed -n SC2ReplayAnalyzer --add-data "sc2reader;sc2reader" run_app.py
```
On Linux, use `:` instead of `;` in `--add-data`:
```bash
pyinstaller --onefile --windowed -n SC2ReplayAnalyzer --add-data "sc2reader:sc2reader" run_app.py
```

### Windows Installer (PyInstaller + Inno Setup)
1. Install dependencies:
```bash
pip install -r requirements.txt
pip install pyinstaller
```
2. Build the exe:
```bash
pyinstaller --onefile --windowed -n SC2ReplayAnalyzer sc2replaytool/app.py
```
3. Install Inno Setup (then use the Inno Setup Compiler GUI or CLI).
4. Create an installer script `installer.iss`:
```ini
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
```
5. Compile `installer.iss` to generate the installer in `dist/`.

### Data Folder Location
On Windows, app data is stored in:
`%APPDATA%\\SC2ReplayAnalyzer\\data\\`

### Notes
- Place your replays in a single folder (the app can scan subfolders).
- Build order tagging is manual for now; we can add detection later.
- Proxy detection uses a default distance threshold of 35. You can change it in the GUI and then re-scan.
- Team games (2v2/3v3/4v4) are supported; detection is done per player.
