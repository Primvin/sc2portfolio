from __future__ import annotations

import sys
from pathlib import Path
import os


def get_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path.cwd()


def get_data_dir() -> Path:
    if os.name == "nt":
        base = Path(os.getenv("APPDATA", get_base_dir()))
        data_dir = base / "SC2ReplayAnalyzer" / "data"
    else:
        base = Path(os.getenv("XDG_DATA_HOME", Path.home() / ".local" / "share"))
        data_dir = base / "sc2replayanalyzer"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir
