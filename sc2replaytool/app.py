from __future__ import annotations

if __package__ is None or __package__ == "":
    import sys
    from pathlib import Path

    sys.path.append(str(Path(__file__).resolve().parents[1]))

import json
import csv
import threading
import subprocess
import os
import sys
from datetime import datetime
from pathlib import Path
from queue import Queue
from typing import Dict, Any, List

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

from .core.indexer import (
    scan_replays,
    scan_replays_multi,
    scan_replays_delta,
    scan_replays_multi_delta,
    load_index,
)
from .core.tags import load_tags, save_tags, set_favorite, set_build_order, set_tags
from .core.paths import get_data_dir


SETTINGS_FILENAME = "settings.json"


def _icon_path() -> Path:
    # When frozen by PyInstaller, data files are extracted under _MEIPASS.
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        base = Path(getattr(sys, "_MEIPASS"))
        return base / "sc2replaytool" / "ico" / "sc2ra.ico"
    return Path(__file__).resolve().parent / "ico" / "sc2ra.ico"


def settings_path() -> Path:
    return get_data_dir() / SETTINGS_FILENAME


def load_settings() -> Dict[str, Any]:
    path = settings_path()
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def save_settings(settings: Dict[str, Any]) -> None:
    path = settings_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(settings, ensure_ascii=False, indent=2), encoding="utf-8")


def format_date(value: str) -> str:
    if not value:
        return ""
    try:
        dt = datetime.fromisoformat(value)
        return dt.strftime("%Y-%m-%d %H:%M")
    except ValueError:
        return value


def format_length(value: str) -> str:
    return value.replace("0:0", "0:") if value else ""


def parse_length_seconds(value: str) -> int:
    if not value:
        return 0
    raw = value.strip()
    if ":" in raw:
        parts = raw.split(":")
        try:
            nums = [int(p) for p in parts]
        except ValueError:
            return 0
        if len(nums) == 3:
            return nums[0] * 3600 + nums[1] * 60 + nums[2]
        if len(nums) == 2:
            return nums[0] * 60 + nums[1]
        return 0
    if "." in raw:
        left, right = raw.split(".", 1)
        try:
            minutes = int(left)
            seconds = int(right)
        except ValueError:
            return 0
        return minutes * 60 + seconds
    try:
        minutes = float(raw)
    except ValueError:
        return 0
    return int(minutes * 60)


def format_total_seconds(seconds: int) -> str:
    if seconds <= 0:
        return "0:00:00"
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours}:{minutes:02d}:{secs:02d}"


class App:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("SC2 Replay Analyzer")
        self.root.geometry("1200x980")
        self.root.minsize(1100, 820)
        self.root.configure(bg="#f3f5f7")
        self._apply_window_icon(self.root)

        self.index: Dict[str, Any] = load_index()
        self.tags: Dict[str, Any] = load_tags()
        self.settings: Dict[str, Any] = load_settings()

        replay_folders = list(self.settings.get("replay_folders", []))
        self.replay_folder_labels: Dict[str, str] = dict(self.settings.get("replay_folder_labels", {}))
        legacy_folder = self.settings.get("replay_folder", "")
        if legacy_folder and legacy_folder not in replay_folders:
            replay_folders.append(legacy_folder)
        self.replay_folders = replay_folders
        default_folder = legacy_folder if legacy_folder in replay_folders else (replay_folders[0] if replay_folders else "")
        self.replay_folder = tk.StringVar(value=default_folder)
        self.folder_filter = tk.StringVar(value="All")
        self.folder_label_entry = tk.StringVar(value="")
        self.matchup_filter = tk.StringVar(value="All")
        self.tag_filter = tk.StringVar(value="All")
        self.player_count_filter = tk.StringVar(value="All")
        self.race_filter = tk.StringVar(value="All")
        self.player_filter = tk.StringVar(value="")
        self.map_filter = tk.StringVar(value="")
        self.proxy_only = tk.BooleanVar(value=False)
        self.favorite_only = tk.BooleanVar(value=False)
        self.proxy_threshold = tk.StringVar(value=str(self.settings.get("proxy_threshold", 35.0)))
        self.watch_enabled = tk.BooleanVar(value=bool(self.settings.get("watch_enabled", False)))
        self.watch_interval_seconds = tk.StringVar(value=str(self.settings.get("watch_interval_seconds", 15)))
        self.bo_step_vars = [tk.StringVar(value="Any") for _ in range(8)]
        self.build_order_entry = tk.StringVar(value="")
        self.tags_entry = tk.StringVar(value="")
        self.tags_search = tk.StringVar(value="")
        self.new_tag_entry = tk.StringVar(value="")
        self.edit_tag_select = tk.StringVar(value="")
        self.status = tk.StringVar(value="Ready")
        self.progress_var = tk.DoubleVar(value=0.0)
        self.scan_hint = tk.StringVar(value="")
        self.selected_replay_path = tk.StringVar(value="")
        self.new_replays_header = tk.StringVar(value="")
        self.new_replays_selected_info = tk.StringVar(value="Selectionne une game pour editer tags/favorite.")
        self.new_replays_tags = tk.StringVar(value="")
        self.new_replays_fav = tk.BooleanVar(value=False)

        self.scan_queue: Queue[Any] = Queue()
        self._scan_log_path = get_data_dir() / "scan_debug.log"
        self._scan_in_progress = False
        self._scan_context = "manual"
        self._startup_known_paths: set[str] = set()
        self._scan_notify_new = False
        self._scan_baseline_paths: set[str] = set()
        self._scan_update_ui = True
        self._scan_delta_only = False
        self._watch_enabled = bool(self.watch_enabled.get())
        initial_watch_ms = self._get_watch_interval_ms_silent(default_ms=15000)
        self._watch_interval_ms = initial_watch_ms if initial_watch_ms > 0 else 15000
        self._new_replays_window: tk.Toplevel | None = None
        self._new_replays_tree: ttk.Treeview | None = None
        self._new_replays_by_path: Dict[str, Dict[str, Any]] = {}
        self._folder_display_to_path: Dict[str, str] = {}
        self._folder_path_to_display: Dict[str, str] = {}

        self._setup_styles()
        self._build_ui()
        self._sync_folder_controls()
        self._sort_state["date"] = True
        self.last_sort_column = "date"
        self._refresh_filters()
        self._refresh_list()
        self.root.after(300, self._auto_scan_on_startup)
        self.root.after(5000, self._watch_loop)

    def _build_ui(self) -> None:
        frame = ttk.Frame(self.root, padding=12, style="App.TFrame")
        frame.pack(fill=tk.BOTH, expand=True)

        header = ttk.Frame(frame, style="Header.TFrame")
        header.pack(fill=tk.X, pady=(0, 6))
        ttk.Label(header, text="SC2 Replay Analyzer", style="HeaderTitle.TLabel").pack(side=tk.LEFT)
        ttk.Label(
            header,
            text="Replay management, build-order analysis, tags and stats",
            style="HeaderSub.TLabel",
        ).pack(side=tk.LEFT, padx=12)

        tabs = ttk.Notebook(frame)
        tabs.pack(fill=tk.X, pady=(0, 6))

        folder_tab = ttk.Frame(tabs, padding=8)
        build_order_tab = ttk.Frame(tabs, padding=8)
        search_tab = ttk.Frame(tabs, padding=8)
        tabs.add(folder_tab, text="Folder")
        tabs.add(build_order_tab, text="Build Order")
        tabs.add(search_tab, text="Search")

        folder_row = ttk.Frame(folder_tab)
        folder_row.pack(fill=tk.X)

        ttk.Label(folder_row, text="Replay Folders:").pack(side=tk.LEFT)
        self.replay_folder_combo = ttk.Combobox(folder_row, state="readonly", width=70)
        self.replay_folder_combo.pack(side=tk.LEFT, padx=6)
        self.replay_folder_combo.bind("<<ComboboxSelected>>", self._on_replay_folder_selected)
        ttk.Label(folder_row, text="Name:").pack(side=tk.LEFT, padx=(6, 0))
        ttk.Entry(folder_row, textvariable=self.folder_label_entry, width=24).pack(side=tk.LEFT, padx=6)
        ttk.Button(folder_row, text="Add Folder", command=self._browse_folder, style="Accent.TButton").pack(side=tk.LEFT)
        ttk.Button(folder_row, text="Save Name", command=self._save_folder_name).pack(side=tk.LEFT, padx=4)
        ttk.Button(folder_row, text="Remove", command=self._remove_folder).pack(side=tk.LEFT, padx=6)
        ttk.Button(folder_row, text="Scan Selected", command=self._start_scan, style="Accent.TButton").pack(side=tk.LEFT)
        ttk.Button(folder_row, text="Scan All", command=self._start_scan_all, style="Accent.TButton").pack(side=tk.LEFT, padx=6)
        ttk.Label(folder_row, textvariable=self.scan_hint).pack(side=tk.LEFT, padx=6)
        ttk.Button(
            folder_row,
            text="Supprimer l'historique",
            command=self._clear_history,
            style="Danger.TButton",
        ).pack(side=tk.RIGHT)

        action_row = ttk.Frame(folder_tab)
        action_row.pack(fill=tk.X, pady=(8, 0))
        ttk.Button(action_row, text="Export Full CSV", command=self._export_full_csv).pack(side=tk.LEFT, padx=6)
        ttk.Button(action_row, text="Import CSV", command=self._import_csv).pack(side=tk.LEFT, padx=6)
        ttk.Checkbutton(action_row, text="Auto Watch", variable=self.watch_enabled, command=self._on_watch_toggle).pack(side=tk.LEFT, padx=10)
        ttk.Label(action_row, text="Watch (s):").pack(side=tk.LEFT)
        ttk.Entry(action_row, textvariable=self.watch_interval_seconds, width=6).pack(side=tk.LEFT, padx=4)
        ttk.Button(action_row, text="Set Watch", command=self._set_watch_settings).pack(side=tk.LEFT, padx=6)

        filter_row = ttk.Frame(search_tab)
        filter_row.pack(fill=tk.X, pady=8)

        ttk.Label(filter_row, text="Matchup:").pack(side=tk.LEFT)
        self.matchup_combo = ttk.Combobox(filter_row, textvariable=self.matchup_filter, state="readonly", width=12)
        self.matchup_combo.pack(side=tk.LEFT, padx=6)
        self.matchup_combo.bind("<<ComboboxSelected>>", lambda _e: self._refresh_list())

        ttk.Label(filter_row, text="Folder:").pack(side=tk.LEFT, padx=6)
        self.folder_combo = ttk.Combobox(filter_row, textvariable=self.folder_filter, state="readonly", width=28)
        self.folder_combo.pack(side=tk.LEFT, padx=6)
        self.folder_combo.bind(
            "<<ComboboxSelected>>",
            lambda _e: (self._refresh_list(), self._scroll_combo_to_end(self.folder_combo)),
        )
        self.folder_combo.bind("<FocusIn>", lambda _e: self._scroll_combo_to_end(self.folder_combo))

        ttk.Label(filter_row, text="Race:").pack(side=tk.LEFT)
        self.race_combo = ttk.Combobox(filter_row, textvariable=self.race_filter, state="readonly", width=6)
        self.race_combo["values"] = ["All", "T", "P", "Z"]
        self.race_combo.pack(side=tk.LEFT, padx=6)
        self.race_combo.bind("<<ComboboxSelected>>", lambda _e: self._refresh_filters())

        ttk.Label(filter_row, text="Nombre de joueurs:").pack(side=tk.LEFT)
        self.player_count_combo = ttk.Combobox(filter_row, textvariable=self.player_count_filter, state="readonly", width=6)
        self.player_count_combo.pack(side=tk.LEFT, padx=6)
        self.player_count_combo.bind("<<ComboboxSelected>>", lambda _e: self._refresh_list())

        ttk.Checkbutton(filter_row, text="Favorites only", variable=self.favorite_only, command=self._refresh_list).pack(side=tk.LEFT, padx=6)
        ttk.Label(filter_row, text="Tag:").pack(side=tk.LEFT, padx=6)
        self.tag_combo = ttk.Combobox(filter_row, textvariable=self.tag_filter, state="normal", width=14)
        self.tag_combo.pack(side=tk.LEFT, padx=6)
        self.tag_combo.bind("<<ComboboxSelected>>", lambda _e: self._refresh_list())
        self.tag_combo.bind("<KeyRelease>", lambda _e: self._refresh_list())

        bo_steps_row1 = ttk.Frame(build_order_tab)
        bo_steps_row1.pack(fill=tk.X, pady=2)
        bo_steps_row2 = ttk.Frame(build_order_tab)
        bo_steps_row2.pack(fill=tk.X, pady=2)
        self.bo_step_combos: List[ttk.Combobox] = []
        for i in range(8):
            parent = bo_steps_row1 if i < 4 else bo_steps_row2
            ttk.Label(parent, text=f"Step {i+1}:").pack(side=tk.LEFT)
            combo = ttk.Combobox(parent, textvariable=self.bo_step_vars[i], state="readonly", width=14)
            combo["values"] = ["Any"]
            combo.current(0)
            combo.pack(side=tk.LEFT, padx=6)
            combo.bind("<<ComboboxSelected>>", lambda _e, idx=i: self._on_build_order_step_change(idx))
            self.bo_step_combos.append(combo)

        bo_manual_row = ttk.Frame(build_order_tab)
        bo_manual_row.pack(fill=tk.X, pady=(8, 0))
        ttk.Label(bo_manual_row, text="Selected Build Order (manual):").pack(side=tk.LEFT)
        ttk.Entry(bo_manual_row, textvariable=self.build_order_entry, width=36).pack(side=tk.LEFT, padx=6)
        ttk.Button(bo_manual_row, text="Set", command=self._set_selected_build_order).pack(side=tk.LEFT)

        search_row = ttk.Frame(search_tab)
        search_row.pack(fill=tk.X, pady=4)
        ttk.Label(search_row, text="Player:").pack(side=tk.LEFT)
        player_entry = ttk.Entry(search_row, textvariable=self.player_filter, width=20)
        player_entry.pack(side=tk.LEFT, padx=6)
        player_entry.bind("<KeyRelease>", lambda _e: self._refresh_list())

        ttk.Label(search_row, text="Map:").pack(side=tk.LEFT)
        map_entry = ttk.Entry(search_row, textvariable=self.map_filter, width=24)
        map_entry.pack(side=tk.LEFT, padx=6)
        map_entry.bind("<KeyRelease>", lambda _e: self._refresh_list())
        ttk.Label(search_row, text="Tag Search:").pack(side=tk.LEFT, padx=(12, 6))
        self.tag_search_combo = ttk.Combobox(search_row, textvariable=self.tags_search, state="normal", width=24)
        self.tag_search_combo.pack(side=tk.LEFT, padx=6)
        self.tag_search_combo.configure(postcommand=self._refresh_tag_combo_values)
        self.tag_search_combo.bind("<KeyRelease>", lambda _e: self._on_tag_search_change())
        self.tag_search_combo.bind("<<ComboboxSelected>>", lambda _e: self._refresh_list())
        ttk.Button(search_row, text="Stats", command=self._open_stats_window).pack(side=tk.LEFT, padx=6)

        search_tools_row = ttk.Frame(search_tab)
        search_tools_row.pack(fill=tk.X, pady=4)
        ttk.Checkbutton(search_tools_row, text="Proxy only", variable=self.proxy_only, command=self._refresh_list).pack(side=tk.LEFT, padx=6)
        ttk.Label(search_tools_row, text="Proxy Threshold:").pack(side=tk.LEFT, padx=6)
        ttk.Entry(search_tools_row, textvariable=self.proxy_threshold, width=6).pack(side=tk.LEFT)
        ttk.Button(search_tools_row, text="Set Threshold", command=self._set_proxy_threshold).pack(side=tk.LEFT, padx=6)

        columns = ("fav", "filename", "players", "winner", "matchup", "map", "date", "length", "tags", "build_order", "proxy_dist")
        tree_frame = ttk.Frame(frame)
        tree_frame.pack(fill=tk.BOTH, expand=True, pady=6)
        self.tree = ttk.Treeview(tree_frame, columns=columns, show="headings", height=12, selectmode="extended")
        self._sort_state = {}
        self.tree.heading("fav", text="Fav", command=lambda: self._sort_by("fav"))
        self.tree.heading("filename", text="Filename", command=lambda: self._sort_by("filename"))
        self.tree.heading("players", text="Players", command=lambda: self._sort_by("players"))
        self.tree.heading("winner", text="Winner", command=lambda: self._sort_by("winner"))
        self.tree.heading("matchup", text="Matchup", command=lambda: self._sort_by("matchup"))
        self.tree.heading("map", text="Map", command=lambda: self._sort_by("map"))
        self.tree.heading("date", text="Date", command=lambda: self._sort_by("date"))
        self.tree.heading("length", text="Length", command=lambda: self._sort_by("length"))
        self.tree.heading("tags", text="Tags", command=lambda: self._sort_by("tags"))
        self.tree.heading("build_order", text="Build Order", command=lambda: self._sort_by("build_order"))
        self.tree.heading("proxy_dist", text="Proxy Dist", command=lambda: self._sort_by("proxy_dist"))

        self.tree.column("fav", width=40, minwidth=40, anchor=tk.CENTER, stretch=False)
        self.tree.column("filename", width=220, minwidth=120, stretch=False)
        self.tree.column("players", width=220, minwidth=120, stretch=False)
        self.tree.column("winner", width=160, minwidth=120, stretch=False)
        self.tree.column("matchup", width=80, minwidth=60, anchor=tk.CENTER, stretch=False)
        self.tree.column("map", width=180, minwidth=120, stretch=False)
        self.tree.column("date", width=140, minwidth=100, stretch=False)
        self.tree.column("length", width=80, minwidth=60, anchor=tk.CENTER, stretch=False)
        self.tree.column("tags", width=180, minwidth=120, stretch=False)
        self.tree.column("build_order", width=180, minwidth=120, stretch=False)
        self.tree.column("proxy_dist", width=320, minwidth=200, anchor=tk.CENTER, stretch=False)

        y_scroll = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.tree.yview)
        x_scroll = ttk.Scrollbar(tree_frame, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll.grid(row=1, column=0, sticky="ew")
        tree_frame.rowconfigure(0, weight=1)
        tree_frame.columnconfigure(0, weight=1)
        self.tree.bind("<<TreeviewSelect>>", self._on_select)

        details_frame = ttk.Frame(frame)
        details_frame.pack(fill=tk.BOTH, expand=False, pady=6)
        ttk.Label(details_frame, text="Selected Replay Details:").pack(side=tk.LEFT, padx=4)

        details_box = ttk.Frame(frame)
        details_box.pack(fill=tk.BOTH, expand=False)
        self.details_text = tk.Text(
            details_box,
            height=5,
            wrap=tk.NONE,
            bg="#ffffff",
            fg="#1f2933",
            relief=tk.FLAT,
            borderwidth=0,
            highlightthickness=1,
            highlightbackground="#d5dde6",
            padx=8,
            pady=8,
        )
        details_y = ttk.Scrollbar(details_box, orient=tk.VERTICAL, command=self.details_text.yview)
        details_x = ttk.Scrollbar(details_box, orient=tk.HORIZONTAL, command=self.details_text.xview)
        self.details_text.configure(yscrollcommand=details_y.set, xscrollcommand=details_x.set, state=tk.DISABLED)
        self.details_text.grid(row=0, column=0, sticky="nsew")
        details_y.grid(row=0, column=1, sticky="ns")
        details_x.grid(row=1, column=0, sticky="ew")
        details_box.rowconfigure(0, weight=1)
        details_box.columnconfigure(0, weight=1)

        tags_section = ttk.LabelFrame(frame, text="Tags & Favorites", padding=8)
        tags_section.pack(fill=tk.X, pady=4)
        tags_row_top = ttk.Frame(tags_section)
        tags_row_top.pack(fill=tk.X, pady=(0, 4))
        ttk.Button(tags_row_top, text="Toggle Favorite", command=self._toggle_favorite).pack(side=tk.LEFT, padx=6)
        ttk.Label(tags_row_top, text="New Tag:").pack(side=tk.LEFT)
        ttk.Entry(tags_row_top, textvariable=self.new_tag_entry, width=18).pack(side=tk.LEFT, padx=6)
        ttk.Button(tags_row_top, text="Add To Selected", command=self._add_new_tag_to_selected).pack(side=tk.LEFT, padx=6)
        ttk.Separator(tags_row_top, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=10)
        ttk.Label(tags_row_top, text="Add Existing:").pack(side=tk.LEFT, padx=6)
        self.edit_tag_combo = ttk.Combobox(tags_row_top, textvariable=self.edit_tag_select, state="readonly", width=22)
        self.edit_tag_combo.pack(side=tk.LEFT, padx=6)
        self.edit_tag_combo.configure(postcommand=self._refresh_tag_combo_values)
        ttk.Button(tags_row_top, text="Add Tag", command=self._add_existing_tag_to_selected).pack(side=tk.LEFT, padx=6)

        tags_row_bottom = ttk.Frame(tags_section)
        tags_row_bottom.pack(fill=tk.X)
        ttk.Label(tags_row_bottom, text="Edit Tags (comma):").pack(side=tk.LEFT, padx=6)
        ttk.Entry(tags_row_bottom, textvariable=self.tags_entry, width=56).pack(side=tk.LEFT, padx=6)
        ttk.Button(tags_row_bottom, text="Update Tags", command=self._set_selected_tags).pack(side=tk.LEFT, padx=6)

        open_row = ttk.Frame(frame)
        open_row.pack(fill=tk.X, pady=4)
        ttk.Button(open_row, text="Open In Folder", command=self._open_in_folder).pack(side=tk.LEFT, padx=6)

        status_row = ttk.Frame(frame)
        status_row.pack(fill=tk.X)
        ttk.Label(status_row, textvariable=self.status).pack(side=tk.LEFT)
        self.progress = ttk.Progressbar(status_row, orient=tk.HORIZONTAL, length=260, mode="determinate", variable=self.progress_var)
        self.progress.pack(side=tk.RIGHT)

    def _apply_window_icon(self, window: tk.Toplevel | tk.Tk) -> None:
        try:
            icon = _icon_path()
            if icon.exists():
                window.iconbitmap(str(icon))
        except Exception:
            pass

    def _setup_styles(self) -> None:
        style = ttk.Style(self.root)
        try:
            style.theme_use("clam")
        except Exception:
            pass

        bg = "#eef3f8"
        card = "#f8fbff"
        text = "#0f172a"
        subtext = "#425466"
        border = "#c9d5e3"
        accent = "#0ea5e9"
        accent_active = "#0284c7"
        danger = "#b42318"

        style.configure("App.TFrame", background=bg)
        style.configure("Header.TFrame", background=bg)
        style.configure("HeaderTitle.TLabel", background=bg, foreground=text, font=("Segoe UI", 17, "bold"))
        style.configure("HeaderSub.TLabel", background=bg, foreground=subtext, font=("Segoe UI", 10))

        style.configure("TFrame", background=card)
        style.configure("TLabel", background=card, foreground=text, font=("Segoe UI", 10))
        style.configure("TNotebook", background=bg, borderwidth=0)
        style.configure("TNotebook.Tab", padding=(16, 8), font=("Segoe UI", 10), width=15)
        style.map(
            "TNotebook.Tab",
            background=[("selected", card), ("active", "#e8eef5"), ("!selected", "#e9edf2")],
            foreground=[("selected", text), ("!selected", text)],
            expand=[("selected", [0, 0, 0, 0]), ("!selected", [0, 0, 0, 0])],
            padding=[("selected", [16, 8]), ("!selected", [16, 8])],
        )

        style.configure("TButton", padding=(8, 4), font=("Segoe UI", 9))
        style.configure("Accent.TButton", background=accent, foreground="white", borderwidth=0)
        style.map("Accent.TButton", background=[("active", accent_active)], foreground=[("active", "white")])
        style.configure("Danger.TButton", background=danger, foreground="white", borderwidth=0)
        style.map("Danger.TButton", background=[("active", "#912018")], foreground=[("active", "white")])

        style.configure("TEntry", fieldbackground="white", bordercolor=border, lightcolor=border, darkcolor=border, padding=4)
        style.configure("TCombobox", fieldbackground="white", background="white", bordercolor=border, lightcolor=border, darkcolor=border)
        style.configure("TCheckbutton", background=card, foreground=text, font=("Segoe UI", 10))

        style.configure(
            "Treeview",
            background="#ffffff",
            fieldbackground="#d7deea",
            foreground=text,
            rowheight=27,
            bordercolor=border,
            padding=(0, 0, 0, 1),
        )
        style.configure("Treeview.Heading", background="#1f2937", foreground="#e6edf7", font=("Segoe UI", 10, "bold"))
        style.map("Treeview", background=[("selected", "#123050")], foreground=[("selected", "#eaf2ff")])

        style.configure("Horizontal.TProgressbar", troughcolor="#e6edf5", background=accent, bordercolor="#e6edf5", lightcolor=accent, darkcolor=accent)

    def _browse_folder(self) -> None:
        folder = filedialog.askdirectory(title="Select Replay Folder")
        if folder:
            if folder not in self.replay_folders:
                self.replay_folders.append(folder)
            self.replay_folder.set(folder)
            label = self.folder_label_entry.get().strip()
            if label:
                self.replay_folder_labels[folder] = label
            else:
                self.replay_folder_labels.pop(folder, None)
            self.settings["replay_folders"] = list(self.replay_folders)
            self.settings["replay_folder"] = folder
            self.settings["replay_folder_labels"] = dict(self.replay_folder_labels)
            save_settings(self.settings)
            self._sync_folder_controls()

    def _remove_folder(self) -> None:
        folder = self.replay_folder.get().strip()
        if not folder:
            return
        if folder in self.replay_folders:
            self.replay_folders = [f for f in self.replay_folders if f != folder]
            self.replay_folder_labels.pop(folder, None)
            self.settings["replay_folders"] = list(self.replay_folders)
            self.settings["replay_folder_labels"] = dict(self.replay_folder_labels)
            if self.replay_folders:
                self.replay_folder.set(self.replay_folders[0])
                self.settings["replay_folder"] = self.replay_folders[0]
            else:
                self.replay_folder.set("")
                self.settings["replay_folder"] = ""
            save_settings(self.settings)
            self._sync_folder_controls()
            self._refresh_list()

    def _save_folder_name(self) -> None:
        folder = self.replay_folder.get().strip()
        if not folder:
            messagebox.showinfo("No Folder", "Select a folder first.")
            return
        label = self.folder_label_entry.get().strip()
        if label:
            self.replay_folder_labels[folder] = label
        else:
            self.replay_folder_labels.pop(folder, None)
        self.settings["replay_folder_labels"] = dict(self.replay_folder_labels)
        save_settings(self.settings)
        self._sync_folder_controls()

    def _start_scan(self) -> None:
        self._log_scan("Scan button clicked")
        folder = self.replay_folder.get().strip()
        if not folder:
            messagebox.showwarning("Missing Folder", "Please select a replay folder first.")
            self._log_scan("Missing folder")
            return
        self._log_scan(f"Folder: {folder}")
        self.status.set("Scanning...")
        self.scan_hint.set("Scanning...")
        self.progress_var.set(0.0)
        self.progress["value"] = 0
        threshold = self._get_proxy_threshold()
        if threshold is None:
            self._log_scan("Invalid proxy threshold")
            self.scan_hint.set("")
            return
        self._log_scan(f"Proxy threshold: {threshold}")
        self._start_scan_thread([Path(folder)], threshold, context="manual")

    def _start_scan_all(self) -> None:
        self._log_scan("Scan all button clicked")
        if not self.replay_folders:
            messagebox.showwarning("Missing Folder", "Please add at least one replay folder first.")
            self._log_scan("Missing folders")
            return
        self.status.set("Scanning...")
        self.scan_hint.set("Scanning all...")
        self.progress_var.set(0.0)
        self.progress["value"] = 0
        threshold = self._get_proxy_threshold()
        if threshold is None:
            self._log_scan("Invalid proxy threshold")
            self.scan_hint.set("")
            return
        self._log_scan(f"Proxy threshold: {threshold}")
        self._start_scan_thread([Path(folder) for folder in self.replay_folders], threshold, context="manual")

    def _auto_scan_on_startup(self) -> None:
        if not self.replay_folders:
            return
        threshold = self._get_proxy_threshold_silent(default=35.0)
        self._startup_known_paths = {str(item.get("path", "")) for item in self.index.get("replays", []) if item.get("path")}
        self.status.set("Checking for new replays...")
        self.scan_hint.set("Startup check...")
        self.progress_var.set(0.0)
        self.progress["value"] = 0
        self._log_scan("Auto-scan on startup")
        self._start_scan_thread(
            [Path(folder) for folder in self.replay_folders],
            threshold,
            context="startup",
            notify_new=True,
            baseline_paths=set(self._startup_known_paths),
            update_ui=True,
            delta_only=True,
        )

    def _watch_loop(self) -> None:
        try:
            if self._watch_enabled and self.replay_folders and not self._scan_in_progress:
                threshold = self._get_proxy_threshold_silent(default=35.0)
                known_paths = {str(item.get("path", "")) for item in self.index.get("replays", []) if item.get("path")}
                self._start_scan_thread(
                    [Path(folder) for folder in self.replay_folders],
                    threshold,
                    context="watch",
                    notify_new=True,
                    baseline_paths=known_paths,
                    update_ui=False,
                    delta_only=True,
                )
        finally:
            self.root.after(self._watch_interval_ms, self._watch_loop)

    def _start_scan_thread(
        self,
        folders: List[Path],
        threshold: float,
        *,
        context: str,
        notify_new: bool = False,
        baseline_paths: set[str] | None = None,
        update_ui: bool = True,
        delta_only: bool = False,
    ) -> bool:
        if self._scan_in_progress:
            self.status.set("Scan already running...")
            return False
        self._scan_in_progress = True
        self._scan_context = context
        self._scan_notify_new = notify_new
        self._scan_baseline_paths = set(baseline_paths or set())
        self._scan_update_ui = update_ui
        self._scan_delta_only = delta_only
        thread = threading.Thread(target=self._scan_worker, args=(folders, threshold), daemon=True)
        thread.start()
        self.root.after(100, self._poll_scan)
        return True

    def _reload_index(self) -> None:
        self.index = load_index()
        self.tags = load_tags()
        self._refresh_filters()
        self._refresh_list()

    def _scan_worker(self, folders: List[Path], threshold: float) -> None:
        def progress_cb(current: int, total: int) -> None:
            self.scan_queue.put(("progress", current, total))

        try:
            if len(folders) == 1 and self._scan_delta_only:
                index = scan_replays_delta(folders[0], proxy_threshold=threshold, progress_cb=progress_cb)
            elif len(folders) == 1:
                index = scan_replays(folders[0], proxy_threshold=threshold, progress_cb=progress_cb)
            elif self._scan_delta_only:
                index = scan_replays_multi_delta(folders, proxy_threshold=threshold, progress_cb=progress_cb)
            else:
                index = scan_replays_multi(folders, proxy_threshold=threshold, progress_cb=progress_cb)
            self.scan_queue.put(("done", index))
        except Exception as exc:  # noqa: BLE001
            self.scan_queue.put(("error", str(exc)))

    def _poll_scan(self) -> None:
        if self.scan_queue.empty():
            self.root.after(100, self._poll_scan)
            return
        item = self.scan_queue.get()
        if isinstance(item, tuple) and item[0] == "progress":
            _tag, current, total = item
            if total and self._scan_update_ui:
                percent = (current / total) * 100.0
                self.progress_var.set(percent)
                self.status.set(f"Scanning... {current}/{total}")
            self.root.after(50, self._poll_scan)
            return
        if isinstance(item, tuple) and item[0] == "done":
            context = self._scan_context
            notify_new = self._scan_notify_new
            baseline_paths = set(self._scan_baseline_paths)
            self.index = item[1]
            self.tags = load_tags()
            new_items: List[Dict[str, Any]] = []
            if notify_new:
                new_items = [r for r in self.index.get("replays", []) if r.get("path") not in baseline_paths]
            if self._scan_update_ui:
                self.progress_var.set(100.0)
                self.status.set("Scan complete")
                self.scan_hint.set("")
            self._scan_in_progress = False
            should_refresh_ui = self._scan_update_ui or bool(new_items)
            if should_refresh_ui:
                self._refresh_filters()
                self._refresh_list()
            self._log_scan("Scan complete")
            if notify_new:
                if new_items:
                    self._show_new_replays_window(new_items, source_context=context)
                elif context == "startup":
                    self.status.set("No new replays found on startup")
            self._scan_context = "manual"
            self._scan_notify_new = False
            self._scan_baseline_paths = set()
            self._scan_update_ui = True
            self._scan_delta_only = False
            return
        if isinstance(item, tuple) and item[0] == "error":
            _tag, message = item
            context = self._scan_context
            if self._scan_update_ui:
                self.status.set("Scan failed")
            self._log_scan(f"Scan failed: {message}")
            if self._scan_update_ui:
                self.scan_hint.set("")
            self._scan_in_progress = False
            self._scan_context = "manual"
            self._scan_notify_new = False
            self._scan_baseline_paths = set()
            self._scan_update_ui = True
            self._scan_delta_only = False
            if context != "watch":
                messagebox.showerror("Scan failed", message)
            return
        self._scan_in_progress = False
        self._scan_context = "manual"
        self._scan_notify_new = False
        self._scan_baseline_paths = set()
        self._scan_update_ui = True
        self._scan_delta_only = False

    def _refresh_filters(self) -> None:
        self._sync_folder_controls()
        matchups = sorted(
            {item.get("matchup", "Unknown") for item in self.index.get("replays", [])},
            key=lambda value: (len(str(value)), str(value).lower()),
        )
        matchups = ["All"] + matchups
        self.matchup_combo["values"] = matchups
        if self.matchup_filter.get() not in matchups:
            self.matchup_filter.set("All")
        if self.race_filter.get() not in {"All", "T", "P", "Z"}:
            self.race_filter.set("All")
        counts = ["All", "1", "2", "4", "6", "8"]
        self.player_count_combo["values"] = counts
        if self.player_count_filter.get() not in counts:
            self.player_count_filter.set("All")
        tag_values = set()
        for tag_list in self.tags.get("tags", {}).values():
            for tag in tag_list:
                tag_values.add(tag)
        tag_values = ["All"] + sorted(tag_values)
        self.tag_combo["values"] = tag_values
        if self.tag_filter.get() == "":
            self.tag_filter.set("All")
        self.available_tags = sorted(set(tag_values[1:]))
        self._refresh_tag_combo_values()
        self._refresh_build_order_options()
        self._refresh_list()

    def _refresh_tag_combo_values(self) -> None:
        tags_set = set()
        for tag_list in self.tags.get("tags", {}).values():
            for tag in tag_list:
                tags_set.add(tag)
        self.available_tags = sorted(tags_set)
        if hasattr(self, "tag_search_combo"):
            self.tag_search_combo["values"] = self.available_tags
        if hasattr(self, "edit_tag_combo"):
            self.edit_tag_combo["values"] = self.available_tags

    def _sync_folder_controls(self) -> None:
        values = list(self.replay_folders)
        if self.replay_folder.get() not in values:
            self.replay_folder.set(values[0] if values else "")

        display_to_path: Dict[str, str] = {}
        path_to_display: Dict[str, str] = {}
        for path in values:
            label = self.replay_folder_labels.get(path, "").strip()
            base = label if label else path
            display = base
            if display in display_to_path:
                display = f"{base} [{Path(path).name}]"
            if display in display_to_path:
                display = f"{base} [{path}]"
            display_to_path[display] = path
            path_to_display[path] = display
        self._folder_display_to_path = display_to_path
        self._folder_path_to_display = path_to_display

        if hasattr(self, "replay_folder_combo"):
            display_values = [path_to_display[path] for path in values if path in path_to_display]
            self.replay_folder_combo["values"] = display_values
            selected_path = self.replay_folder.get()
            self.replay_folder_combo.set(path_to_display.get(selected_path, ""))

        selected_label = self.replay_folder_labels.get(self.replay_folder.get(), "")
        self.folder_label_entry.set(selected_label)

        folder_values = ["All"] + [path_to_display[path] for path in values if path in path_to_display]
        if hasattr(self, "folder_combo"):
            self.folder_combo["values"] = folder_values
        if self.folder_filter.get() not in folder_values:
            self.folder_filter.set("All")
        if hasattr(self, "folder_combo"):
            self._scroll_combo_to_end(self.folder_combo)

    def _on_replay_folder_selected(self, _event: Any | None = None) -> None:
        display_value = self.replay_folder_combo.get().strip()
        selected_path = self._folder_display_to_path.get(display_value, "")
        if not selected_path:
            return
        self.replay_folder.set(selected_path)
        self.settings["replay_folder"] = selected_path
        self.settings["replay_folders"] = list(self.replay_folders)
        save_settings(self.settings)
        self.folder_label_entry.set(self.replay_folder_labels.get(selected_path, ""))
        self._sync_folder_controls()

    def _scroll_combo_to_end(self, combo: ttk.Combobox) -> None:
        try:
            combo.xview_moveto(1.0)
        except Exception:
            pass

    def _item_source_folder(self, item: Dict[str, Any]) -> str:
        folder = item.get("source_folder", "") or ""
        if not folder:
            index_folder = str(self.index.get("folder", ""))
            path = str(item.get("path", ""))
            if index_folder and path.startswith(index_folder):
                folder = index_folder
            else:
                try:
                    folder = str(Path(path).parent)
                except Exception:
                    folder = ""
        return folder

    def _normalize_path(self, value: str) -> str:
        try:
            return os.path.normcase(os.path.normpath(value))
        except Exception:
            return value

    def _folder_matches(self, item: Dict[str, Any], folder_filter: str) -> bool:
        if folder_filter == "All":
            return True
        folder_filter_path = self._folder_display_to_path.get(folder_filter, folder_filter)
        item_folder = self._item_source_folder(item)
        return self._normalize_path(item_folder) == self._normalize_path(folder_filter_path)

    def _refresh_list(self) -> None:
        selected_before = self._get_selected_path()
        self.tree.delete(*self.tree.get_children())

        matchup_filter = self.matchup_filter.get()
        folder_filter = self.folder_filter.get()
        favorites = set(self.tags.get("favorites", []))
        build_orders = self.tags.get("build_orders", {})
        tags_map = self.tags.get("tags", {})
        search_tags = [t.strip() for t in self.tags_search.get().split(",") if t.strip()]
        player_count_filter = self.player_count_filter.get().strip()
        race_filter = self.race_filter.get()
        player_query = self.player_filter.get().strip().lower()
        map_query = self.map_filter.get().strip().lower()
        self.filtered_items: List[Dict[str, Any]] = []
        selected_steps = self._normalized_bo_steps()

        inserted_by_path: Dict[str, str] = {}
        for item in self.index.get("replays", []):
            if not self._folder_matches(item, folder_filter):
                continue
            if matchup_filter != "All" and item.get("matchup") != matchup_filter:
                continue
            if self.favorite_only.get() and item.get("path") not in favorites:
                continue
            if self.proxy_only.get() and not item.get("proxy_flag"):
                continue
            if player_count_filter != "All":
                try:
                    if len(item.get("players", [])) != int(player_count_filter):
                        continue
                except ValueError:
                    pass
            if race_filter != "All":
                if not any(p.get("race") == race_filter for p in item.get("players", [])):
                    continue
            manual_bo = build_orders.get(item.get("path", ""), "")
            auto_bo = item.get("build_order_auto", "")
            display_bo = manual_bo or auto_bo
            tag_list = tags_map.get(item.get("path", ""), [])
            tag_filter = self.tag_filter.get().strip()
            if tag_filter and tag_filter != "All" and tag_filter not in tag_list:
                continue
            if search_tags and not all(t in tag_list for t in search_tags):
                continue
            if player_query:
                players = item.get("players", [])
                if not any(player_query in str(p.get("name", "")).lower() for p in players):
                    continue
            if map_query and map_query not in str(item.get("map", "")).lower():
                continue
            if selected_steps and not self._match_build_order_steps(item, selected_steps):
                continue
            self.filtered_items.append(item)

            node_id = self.tree.insert(
                "",
                tk.END,
                values=(
                    "Y" if item.get("path") in favorites else "",
                    item.get("filename"),
                    self._format_players(item.get("players", [])),
                    self._format_winner(item.get("players", [])),
                    item.get("matchup"),
                    item.get("map"),
                    format_date(item.get("start_time", "")),
                    format_length(item.get("length", "")),
                    ", ".join(tag_list),
                    display_bo,
                    self._format_proxy_by_player(item),
                ),
                tags=(item.get("path"),),
            )
            item_path = str(item.get("path", ""))
            if item_path:
                inserted_by_path[item_path] = node_id

        self.status.set(f"Loaded {len(self.tree.get_children())} replays")
        if selected_before and selected_before in inserted_by_path:
            node_id = inserted_by_path[selected_before]
            self.tree.selection_set(node_id)
            self.tree.focus(node_id)
        if hasattr(self, "last_sort_column"):
            self._sort_by(self.last_sort_column, toggle=False)

    def _get_selected_path(self) -> str | None:
        selection = self.tree.selection()
        if not selection:
            value = self.selected_replay_path.get().strip()
            return value if value else None
        item = self.tree.item(selection[0])
        tags = item.get("tags") or []
        path = tags[0] if tags else None
        if path:
            self.selected_replay_path.set(str(path))
        return path

    def _get_selected_paths(self) -> List[str]:
        selection = self.tree.selection()
        paths: List[str] = []
        for selected in selection:
            item = self.tree.item(selected)
            tags = item.get("tags") or []
            if tags and tags[0]:
                paths.append(str(tags[0]))
        if paths:
            self.selected_replay_path.set(paths[0])
            return paths
        fallback = self.selected_replay_path.get().strip()
        return [fallback] if fallback else []

    def _on_select(self, _event: Any) -> None:
        path = self._get_selected_path()
        if not path:
            self.build_order_entry.set("")
            self._set_details("")
            return
        self.selected_replay_path.set(path)
        current_bo = self.tags.get("build_orders", {}).get(path, "")
        self.build_order_entry.set(current_bo)
        current_tags = self.tags.get("tags", {}).get(path, [])
        self.tags_entry.set(", ".join(current_tags))
        self._update_details_for_path(path)

    def _set_details(self, text: str) -> None:
        self.details_text.configure(state=tk.NORMAL)
        self.details_text.delete("1.0", tk.END)
        self.details_text.insert(tk.END, text)
        self.details_text.configure(state=tk.DISABLED)

    def _update_details_for_path(self, path: str) -> None:
        item = next((r for r in self.index.get("replays", []) if r.get("path") == path), None)
        if not item:
            self._set_details("No details available.")
            return

        players = self._format_players(item.get("players", []))
        winner = self._format_winner(item.get("players", []))
        manual_bo = self.tags.get("build_orders", {}).get(path, "")
        auto_bo = item.get("build_order_auto", "")
        proxy_by_player = self._format_proxy_by_player(item)
        details = [
            f"Filename: {item.get('filename', '')}",
            f"Path: {item.get('path', '')}",
            f"Map: {item.get('map', '')}",
            f"Matchup: {item.get('matchup', '')}",
            f"Date: {format_date(item.get('start_time', ''))}",
            f"Length: {format_length(item.get('length', ''))}",
            f"Players: {players}",
            f"Winner: {winner}",
            f"Player Count: {len(item.get('players', []))}",
            f"Tags: {', '.join(self.tags.get('tags', {}).get(path, []))}",
            f"Build Order (manual): {manual_bo}",
            f"Build Order (auto): {auto_bo}",
            f"Proxy Distances: {proxy_by_player}",
            f"Proxy Threshold: {item.get('proxy_threshold', '')}",
        ]
        self._set_details("\n".join(details))

    def _set_selected_build_order(self) -> None:
        paths = self._get_selected_paths()
        if not paths:
            messagebox.showinfo("No Selection", "Select a replay first.")
            return
        value = self.build_order_entry.get().strip()
        for path in paths:
            set_build_order(self.tags, path, value)
        save_tags(self.tags)
        self._refresh_filters()
        self._refresh_list()
        self.status.set(f"Build order updated for {len(paths)} replay(s)")

    def _set_selected_tags(self) -> None:
        paths = self._get_selected_paths()
        if not paths:
            messagebox.showinfo("No Selection", "Select a replay first.")
            return
        raw = self.tags_entry.get().strip()
        tag_list = [t.strip() for t in raw.split(",")] if raw else []
        for path in paths:
            set_tags(self.tags, path, tag_list)
        save_tags(self.tags)
        self._refresh_filters()
        self._refresh_list()
        self.status.set(f"Tags updated for {len(paths)} replay(s)")

    def _toggle_favorite(self) -> None:
        paths = self._get_selected_paths()
        if not paths:
            messagebox.showinfo("No Selection", "Select a replay first.")
            return
        favorites = set(self.tags.get("favorites", []))
        for path in paths:
            is_fav = path in favorites
            set_favorite(self.tags, path, not is_fav)
        save_tags(self.tags)
        self._refresh_list()
        self.status.set(f"Favorite toggled for {len(paths)} replay(s)")

    def _on_build_order_step_change(self, idx: int) -> None:
        self._reset_bo_steps_from(idx + 1)
        self._refresh_build_order_options()
        self._refresh_list()

    def _reset_bo_steps_from(self, idx: int) -> None:
        for i in range(idx, len(self.bo_step_vars)):
            self.bo_step_vars[i].set("Any")

    def _normalized_bo_steps(self) -> List[str]:
        steps = [var.get() for var in self.bo_step_vars]
        normalized: List[str] = []
        for step in steps:
            if step == "Any":
                break
            normalized.append(step)
        return normalized

    def _iter_sequences(self, item: Dict[str, Any]) -> List[List[str]]:
        sequences = item.get("bo_sequences", [])
        key = "seq_tech"
        race_filter = self.race_filter.get()
        result: List[List[str]] = []
        for entry in sequences:
            if race_filter != "All" and entry.get("race") != race_filter:
                continue
            seq = entry.get(key, [])
            if seq:
                result.append(seq)
        return result

    def _match_build_order_steps(self, item: Dict[str, Any], steps: List[str]) -> bool:
        if not steps:
            return True
        for seq in self._iter_sequences(item):
            if len(seq) < len(steps):
                continue
            if all(seq[i] == steps[i] for i in range(len(steps))):
                return True
        return False

    def _refresh_build_order_options(self) -> None:
        candidates = []
        matchup_filter = self.matchup_filter.get()
        folder_filter = self.folder_filter.get()
        favorites = set(self.tags.get("favorites", []))
        player_query = self.player_filter.get().strip().lower()
        map_query = self.map_filter.get().strip().lower()
        tags_map = self.tags.get("tags", {})

        for item in self.index.get("replays", []):
            if not self._folder_matches(item, folder_filter):
                continue
            if matchup_filter != "All" and item.get("matchup") != matchup_filter:
                continue
            if self.favorite_only.get() and item.get("path") not in favorites:
                continue
            if self.proxy_only.get() and not item.get("proxy_flag"):
                continue
            tag_list = tags_map.get(item.get("path", ""), [])
            if self.tag_filter.get() != "All" and self.tag_filter.get() not in tag_list:
                continue
            if player_query:
                players = item.get("players", [])
                if not any(player_query in str(p.get("name", "")).lower() for p in players):
                    continue
            if map_query and map_query not in str(item.get("map", "")).lower():
                continue
            candidates.append(item)

        selected = [var.get() for var in self.bo_step_vars]
        for idx in range(len(self.bo_step_vars)):
            prefix = []
            for step in selected[:idx]:
                if step == "Any":
                    break
                prefix.append(step)

            options = set()
            for item in candidates:
                for seq in self._iter_sequences(item):
                    if len(seq) <= idx:
                        continue
                    if prefix and not all(seq[i] == prefix[i] for i in range(len(prefix))):
                        continue
                    options.add(seq[idx])

            values = ["Any"] + sorted(options)
            combo = self.bo_step_combos[idx]
            combo["values"] = values
            current = self.bo_step_vars[idx].get()
            if current not in values:
                self.bo_step_vars[idx].set("Any")

    def _get_proxy_threshold(self) -> float | None:
        try:
            value = float(self.proxy_threshold.get().strip())
        except ValueError:
            messagebox.showwarning("Invalid Threshold", "Proxy threshold must be a number.")
            return None
        if value <= 0:
            messagebox.showwarning("Invalid Threshold", "Proxy threshold must be > 0.")
            return None
        return value

    def _get_proxy_threshold_silent(self, default: float = 35.0) -> float:
        try:
            value = float(self.proxy_threshold.get().strip())
        except (TypeError, ValueError):
            return default
        if value <= 0:
            return default
        return value

    def _get_watch_interval_ms(self) -> int | None:
        try:
            seconds = float(self.watch_interval_seconds.get().strip())
        except ValueError:
            messagebox.showwarning("Invalid Watch", "Watch interval must be a number (seconds).")
            return None
        if seconds < 3:
            messagebox.showwarning("Invalid Watch", "Watch interval must be >= 3 seconds.")
            return None
        return int(seconds * 1000)

    def _get_watch_interval_ms_silent(self, default_ms: int = 15000) -> int:
        try:
            seconds = float(self.watch_interval_seconds.get().strip())
        except (TypeError, ValueError):
            return default_ms
        if seconds < 3:
            return default_ms
        return int(seconds * 1000)

    def _on_watch_toggle(self) -> None:
        self._watch_enabled = bool(self.watch_enabled.get())
        self.settings["watch_enabled"] = self._watch_enabled
        save_settings(self.settings)
        self.status.set("Auto watch enabled" if self._watch_enabled else "Auto watch disabled")

    def _set_watch_settings(self) -> None:
        interval_ms = self._get_watch_interval_ms()
        if interval_ms is None:
            return
        self._watch_interval_ms = interval_ms
        self._watch_enabled = bool(self.watch_enabled.get())
        self.settings["watch_enabled"] = self._watch_enabled
        self.settings["watch_interval_seconds"] = round(interval_ms / 1000, 2)
        save_settings(self.settings)
        self.status.set(f"Watch settings saved ({self.settings['watch_interval_seconds']}s)")

    def _show_new_replays_window(self, replays: List[Dict[str, Any]], *, source_context: str = "startup") -> None:
        sorted_replays = sorted(replays, key=lambda r: str(r.get("start_time", "")), reverse=True)
        for replay in sorted_replays:
            path = str(replay.get("path", ""))
            if not path:
                continue
            self._new_replays_by_path[path] = replay

        if not self._new_replays_window or not self._new_replays_window.winfo_exists():
            top = tk.Toplevel(self.root)
            self._new_replays_window = top
            top.title("Nouvelles parties detectees")
            top.geometry("1120x560")
            top.minsize(900, 420)
            self._apply_window_icon(top)
            top.transient(self.root)
            top.grab_set()
            top.protocol("WM_DELETE_WINDOW", self._close_new_replays_window)

            header = ttk.Frame(top, padding=10)
            header.pack(fill=tk.X)
            ttk.Label(header, textvariable=self.new_replays_header).pack(side=tk.LEFT)

            list_frame = ttk.Frame(top, padding=(10, 0, 10, 6))
            list_frame.pack(fill=tk.BOTH, expand=True)
            columns = ("date", "filename", "winner", "matchup", "map", "players")
            tree = ttk.Treeview(list_frame, columns=columns, show="headings", height=12)
            self._new_replays_tree = tree
            tree.heading("date", text="Date")
            tree.heading("filename", text="Fichier")
            tree.heading("winner", text="Winner")
            tree.heading("matchup", text="Matchup")
            tree.heading("map", text="Map")
            tree.heading("players", text="Players")
            tree.column("date", width=130, stretch=False)
            tree.column("filename", width=220, stretch=False)
            tree.column("winner", width=160, stretch=False)
            tree.column("matchup", width=80, stretch=False, anchor=tk.CENTER)
            tree.column("map", width=180, stretch=False)
            tree.column("players", width=360, stretch=True)

            y_scroll = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=tree.yview)
            tree.configure(yscrollcommand=y_scroll.set)
            tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            y_scroll.pack(side=tk.RIGHT, fill=tk.Y)
            tree.bind("<<TreeviewSelect>>", self._reload_new_replay_editor_for_selection)

            details_frame = ttk.Frame(top, padding=(10, 4, 10, 6))
            details_frame.pack(fill=tk.X)
            ttk.Label(details_frame, textvariable=self.new_replays_selected_info).pack(side=tk.LEFT)

            editor_frame = ttk.Frame(top, padding=(10, 2, 10, 10))
            editor_frame.pack(fill=tk.X)
            ttk.Label(editor_frame, text="Tags (comma):").pack(side=tk.LEFT)
            ttk.Entry(editor_frame, textvariable=self.new_replays_tags, width=48).pack(side=tk.LEFT, padx=6)
            ttk.Checkbutton(editor_frame, text="Favorite", variable=self.new_replays_fav).pack(side=tk.LEFT, padx=10)

            footer = ttk.Frame(top, padding=10)
            footer.pack(fill=tk.X)
            ttk.Button(footer, text="Apply", command=self._apply_selected_new_replay).pack(side=tk.RIGHT, padx=6)
            ttk.Button(footer, text="Fermer", command=self._close_new_replays_window).pack(side=tk.RIGHT)

        tree = self._new_replays_tree
        if tree and tree.winfo_exists():
            existing_paths = set()
            for child in tree.get_children():
                item = tree.item(child)
                tags = item.get("tags") or []
                if tags:
                    existing_paths.add(str(tags[0]))
            for replay in sorted_replays:
                path = str(replay.get("path", ""))
                if not path or path in existing_paths:
                    continue
                tree.insert(
                    "",
                    0,
                    values=(
                        format_date(replay.get("start_time", "")),
                        replay.get("filename", ""),
                        self._format_winner(replay.get("players", [])),
                        replay.get("matchup", ""),
                        replay.get("map", ""),
                        self._format_players(replay.get("players", [])),
                    ),
                    tags=(path,),
                )

            if not tree.selection():
                children = tree.get_children()
                if children:
                    tree.selection_set(children[0])
            self._reload_new_replay_editor_for_selection()

        context_text = "au lancement" if source_context == "startup" else "en temps reel"
        self.new_replays_header.set(
            f"{len(self._new_replays_by_path)} nouvelle(s) partie(s) detectee(s) {context_text}."
        )
        if self._new_replays_window and self._new_replays_window.winfo_exists():
            self._new_replays_window.lift()
            self._new_replays_window.focus_force()

    def _selected_new_replay_path(self) -> str | None:
        tree = self._new_replays_tree
        if not tree or not tree.winfo_exists():
            return None
        selection = tree.selection()
        if not selection:
            return None
        item = tree.item(selection[0])
        tags = item.get("tags") or []
        return str(tags[0]) if tags else None

    def _reload_new_replay_editor_for_selection(self, _event: Any | None = None) -> None:
        path = self._selected_new_replay_path()
        if not path:
            self.new_replays_tags.set("")
            self.new_replays_fav.set(False)
            self.new_replays_selected_info.set("Selectionne une game pour editer tags/favorite.")
            return
        replay = self._new_replays_by_path.get(path, {})
        winner = self._format_winner(replay.get("players", []))
        self.new_replays_selected_info.set(
            f"{replay.get('filename', '')} | Winner: {winner} | Map: {replay.get('map', '')}"
        )
        self.new_replays_tags.set(", ".join(self.tags.get("tags", {}).get(path, [])))
        self.new_replays_fav.set(path in set(self.tags.get("favorites", [])))

    def _apply_selected_new_replay(self) -> None:
        path = self._selected_new_replay_path()
        if not path:
            messagebox.showinfo("No Selection", "Select a replay first.")
            return
        raw_tags = self.new_replays_tags.get().strip()
        tag_list = [t.strip() for t in raw_tags.split(",")] if raw_tags else []
        set_tags(self.tags, path, tag_list)
        set_favorite(self.tags, path, bool(self.new_replays_fav.get()))
        save_tags(self.tags)
        self.tags = load_tags()
        self._refresh_filters()
        self._refresh_list()
        self.status.set("Replay metadata updated")

    def _close_new_replays_window(self) -> None:
        if self._new_replays_window and self._new_replays_window.winfo_exists():
            self._new_replays_window.destroy()
        self._new_replays_window = None
        self._new_replays_tree = None
        self._new_replays_by_path = {}
        self.new_replays_header.set("")
        self.new_replays_selected_info.set("Selectionne une game pour editer tags/favorite.")
        self.new_replays_tags.set("")
        self.new_replays_fav.set(False)

    def _set_proxy_threshold(self) -> None:
        value = self._get_proxy_threshold()
        if value is None:
            return
        self.settings["proxy_threshold"] = value
        save_settings(self.settings)
        self.status.set("Proxy threshold saved. Re-scan to apply.")

    def _format_proxy_distance(self, value: Any) -> str:
        if value is None:
            return ""
        try:
            return f"{float(value):.1f}"
        except (TypeError, ValueError):
            return ""

    def _format_players(self, players: List[Dict[str, Any]]) -> str:
        parts = []
        for p in players:
            name = p.get("name", "")
            race = p.get("race", "")
            if race:
                parts.append(f"{name}({race})")
            else:
                parts.append(name)
        return " | ".join(parts)

    def _format_winner(self, players: List[Dict[str, Any]]) -> str:
        winners = []
        for p in players:
            result = str(p.get("result", "")).lower()
            if result in {"win", "winner", "victory"}:
                winners.append(p.get("name", ""))
        winners = [w for w in winners if w]
        if not winners:
            return "Unknown"
        return " | ".join(winners)

    def _open_stats_window(self) -> None:
        if not hasattr(self, "filtered_items"):
            self._refresh_list()
        if not self.filtered_items:
            messagebox.showinfo("No Data", "No replays in current filters.")
            return

        window = tk.Toplevel(self.root)
        window.title("Stats")
        window.geometry("760x520")
        window.minsize(720, 480)
        self._apply_window_icon(window)

        top = ttk.Frame(window, padding=10)
        top.pack(fill=tk.X)

        ttk.Label(top, text="Player 1:").pack(side=tk.LEFT)
        player_var = tk.StringVar(value="")
        player_entry = ttk.Entry(top, textvariable=player_var, width=28)
        player_entry.pack(side=tk.LEFT, padx=6)

        ttk.Label(top, text="Player 2:").pack(side=tk.LEFT)
        player2_var = tk.StringVar(value="")
        player2_entry = ttk.Entry(top, textvariable=player2_var, width=28)
        player2_entry.pack(side=tk.LEFT, padx=6)

        ttk.Button(top, text="Refresh", command=lambda: refresh_stats()).pack(side=tk.LEFT, padx=6)

        stats_text = tk.Text(window, height=20, wrap=tk.NONE)
        stats_text.pack(fill=tk.BOTH, expand=True, padx=10, pady=6)
        stats_text.configure(state=tk.DISABLED)

        players = sorted({p.get("name", "") for item in self.filtered_items for p in item.get("players", []) if p.get("name")})

        def set_text(text: str) -> None:
            stats_text.configure(state=tk.NORMAL)
            stats_text.delete("1.0", tk.END)
            stats_text.insert(tk.END, text)
            stats_text.configure(state=tk.DISABLED)

        def is_win(p: Dict[str, Any]) -> bool:
            return str(p.get("result", "")).lower() in {"win", "winner", "victory"}

        def is_proxy_against(item: Dict[str, Any], player_pid: int | None, player_team: Any) -> bool:
            if player_pid is None:
                return False
            distances = item.get("proxy_distances", {}) or {}
            threshold = item.get("proxy_threshold", 35.0)
            try:
                threshold_val = float(threshold)
            except (TypeError, ValueError):
                threshold_val = 35.0
            for p in item.get("players", []):
                pid = p.get("pid")
                if pid is None or pid == player_pid:
                    continue
                if player_team is not None:
                    team_id = p.get("team_id")
                    if team_id is not None and team_id == player_team:
                        continue
                dist = distances.get(str(pid))
                if dist is None:
                    continue
                try:
                    if float(dist) > threshold_val:
                        return True
                except (TypeError, ValueError):
                    continue
            return False

        def opponent_race_key(item: Dict[str, Any], player_pid: int | None) -> str:
            races = []
            for p in item.get("players", []):
                pid = p.get("pid")
                if pid is None or pid == player_pid:
                    continue
                race = p.get("race", "")
                if race:
                    races.append(race)
            if not races:
                return "Unknown"
            return "+".join(sorted(races))

        def compute_stats() -> str:
            player_name = player_var.get().strip()
            player2_name = player2_var.get().strip()
            if not player_name:
                return "No player selected."

            total_games = 0
            total_wins = 0
            total_seconds = 0
            proxy_games = 0
            proxy_wins = 0

            by_matchup: Dict[str, Dict[str, int]] = {}
            by_opponent_race: Dict[str, Dict[str, int]] = {}
            head_to_head_games = 0
            head_to_head_wins = 0

            for item in self.filtered_items:
                player_entry = None
                player2_entry = None
                for p in item.get("players", []):
                    if p.get("name") == player_name:
                        player_entry = p
                    if player2_name and p.get("name") == player2_name:
                        player2_entry = p
                if not player_entry:
                    continue

                total_games += 1
                won = is_win(player_entry)
                if won:
                    total_wins += 1
                total_seconds += parse_length_seconds(str(item.get("length", "")))

                matchup = item.get("matchup", "Unknown") or "Unknown"
                by_matchup.setdefault(matchup, {"games": 0, "wins": 0})
                by_matchup[matchup]["games"] += 1
                if won:
                    by_matchup[matchup]["wins"] += 1

                race_key = opponent_race_key(item, player_entry.get("pid"))
                by_opponent_race.setdefault(race_key, {"games": 0, "wins": 0})
                by_opponent_race[race_key]["games"] += 1
                if won:
                    by_opponent_race[race_key]["wins"] += 1

                if is_proxy_against(item, player_entry.get("pid"), player_entry.get("team_id")):
                    proxy_games += 1
                    if won:
                        proxy_wins += 1
                if player2_name and player2_entry:
                    head_to_head_games += 1
                    if won:
                        head_to_head_wins += 1

            if total_games == 0:
                return "No games found for selected player in current filters."

            lines = [
                f"Player: {player_name}",
                f"Games: {total_games}",
                f"Win%: {round((total_wins / total_games) * 100, 1)}",
                f"Total Time: {format_total_seconds(total_seconds)}",
                f"Proxy Against%: {round((proxy_games / total_games) * 100, 1)}",
                f"Win% vs Proxy: {round((proxy_wins / proxy_games) * 100, 1) if proxy_games else 0.0}",
            ]
            if player2_name:
                matchup_win_pct = round((head_to_head_wins / head_to_head_games) * 100, 1) if head_to_head_games else 0.0
                lines.append(f"Matchup vs {player2_name}: {matchup_win_pct}% ({head_to_head_wins}/{head_to_head_games})")
            lines.append("")
            lines.append("Win% by Opponent Race:")

            for race_key in sorted(by_opponent_race.keys()):
                data = by_opponent_race[race_key]
                win_pct = round((data["wins"] / data["games"]) * 100, 1) if data["games"] else 0.0
                lines.append(f"- {race_key}: {win_pct}% ({data['wins']}/{data['games']})")

            lines.append("")
            lines.append("Win% by Matchup:")
            for matchup in sorted(by_matchup.keys()):
                data = by_matchup[matchup]
                win_pct = round((data["wins"] / data["games"]) * 100, 1) if data["games"] else 0.0
                lines.append(f"- {matchup}: {win_pct}% ({data['wins']}/{data['games']})")

            return "\n".join(lines)

        def refresh_stats(_event: Any | None = None) -> None:
            set_text(compute_stats())

        def make_popup(entry: ttk.Entry) -> tuple[tk.Toplevel, tk.Listbox]:
            popup = tk.Toplevel(window)
            popup.withdraw()
            popup.overrideredirect(True)
            popup.attributes("-topmost", True)
            listbox = tk.Listbox(popup, height=6, activestyle="dotbox")
            listbox.pack(fill=tk.BOTH, expand=True)
            return popup, listbox

        popup1, listbox1 = make_popup(player_entry)
        popup2, listbox2 = make_popup(player2_entry)

        def show_popup(entry: ttk.Entry, popup: tk.Toplevel, listbox: tk.Listbox, values: list[str]) -> None:
            listbox.delete(0, tk.END)
            for value in values:
                listbox.insert(tk.END, value)
            if not values:
                popup.withdraw()
                return
            x = entry.winfo_rootx()
            y = entry.winfo_rooty() + entry.winfo_height()
            popup.geometry(f"{entry.winfo_width()}x{min(150, 20 * len(values))}+{x}+{y}")
            popup.deiconify()

        def hide_popup(popup: tk.Toplevel) -> None:
            popup.withdraw()

        def filter_players(query: str) -> list[str]:
            if not query:
                return players
            q = query.lower()
            return [p for p in players if q in p.lower()]

        def bind_entry(entry: ttk.Entry, popup: tk.Toplevel, listbox: tk.Listbox, var: tk.StringVar) -> None:
            def on_key_release(event: Any) -> None:
                if getattr(event, "keysym", "") in {"Up", "Down", "Return", "Escape"}:
                    return
                show_popup(entry, popup, listbox, filter_players(var.get().strip()))

            def on_focus_out(_event: Any) -> None:
                self.root.after(100, lambda: hide_popup(popup))

            def on_select(_event: Any | None = None) -> None:
                selection = listbox.curselection()
                if not selection:
                    return
                value = listbox.get(selection[0])
                var.set(value)
                hide_popup(popup)
                refresh_stats()
                entry.focus_set()
                entry.icursor(tk.END)

            def move_selection(delta: int) -> None:
                size = listbox.size()
                if size == 0:
                    return
                selection = listbox.curselection()
                if not selection:
                    index = 0 if delta > 0 else size - 1
                else:
                    index = max(0, min(size - 1, selection[0] + delta))
                listbox.selection_clear(0, tk.END)
                listbox.selection_set(index)
                listbox.activate(index)
                listbox.see(index)

            def on_down(_event: Any) -> str:
                if not popup.winfo_viewable():
                    show_popup(entry, popup, listbox, filter_players(var.get().strip()))
                move_selection(1)
                return "break"

            def on_up(_event: Any) -> str:
                if not popup.winfo_viewable():
                    show_popup(entry, popup, listbox, filter_players(var.get().strip()))
                move_selection(-1)
                return "break"

            def on_enter(_event: Any) -> str:
                on_select()
                return "break"

            entry.bind("<KeyRelease>", on_key_release)
            entry.bind("<FocusOut>", on_focus_out)
            entry.bind("<Down>", on_down)
            entry.bind("<Up>", on_up)
            entry.bind("<Return>", on_enter)
            listbox.bind("<<ListboxSelect>>", on_select)
            listbox.bind("<ButtonRelease-1>", on_select)

        bind_entry(player_entry, popup1, listbox1, player_var)
        bind_entry(player2_entry, popup2, listbox2, player2_var)

        player_entry.bind("<Return>", refresh_stats)
        player2_entry.bind("<Return>", refresh_stats)
        refresh_stats()

    def _format_proxy_by_player(self, item: Dict[str, Any]) -> str:
        distances = item.get("proxy_distances", {}) or {}
        players = item.get("players", [])
        parts = []
        for p in players:
            pid = p.get("pid")
            if pid is None:
                continue
            value = distances.get(str(pid))
            if value is None:
                continue
            name = p.get("name", "")
            race = p.get("race", "")
            dist = self._format_proxy_distance(value)
            parts.append(f"{name}({race}):{dist}")
        return " | ".join(parts) if parts else self._format_proxy_distance(item.get("proxy_distance_max"))

    def _sort_by(self, column: str, toggle: bool = True) -> None:
        reverse = self._sort_state.get(column, False)
        if toggle:
            reverse = not reverse
        self._sort_state[column] = reverse
        self.last_sort_column = column

        data = []
        for child in self.tree.get_children(""):
            values = self.tree.item(child, "values")
            data.append((values, child))

        idx_map = {
            "fav": 0,
            "filename": 1,
            "players": 2,
            "winner": 3,
            "matchup": 4,
            "map": 5,
            "date": 6,
            "length": 7,
            "tags": 8,
            "build_order": 9,
            "proxy_dist": 10,
        }
        idx = idx_map[column]

        def to_number(value: str) -> float:
            try:
                return float(value)
            except ValueError:
                return -1.0

        def sort_key(item: tuple[Any, str]) -> Any:
            val = item[0][idx]
            if column in {"length"}:
                return val
            if column in {"proxy_dist"}:
                first = str(val).split("|")[0].strip()
                return to_number(first.split(":")[-1].strip()) if first else -1.0
            if column in {"fav"}:
                return 0 if val == "Y" else 1
            return str(val).lower()

        data.sort(key=sort_key, reverse=reverse)
        for index, (_values, child) in enumerate(data):
            self.tree.move(child, "", index)

    def _on_tag_search_change(self) -> None:
        text = self.tags_search.get().strip().lower()
        if not hasattr(self, "available_tags"):
            self.available_tags = []
        if not text:
            self.tag_search_combo["values"] = self.available_tags
        else:
            filtered = [t for t in self.available_tags if text in t.lower()]
            self.tag_search_combo["values"] = filtered
        self._refresh_list()

    def _add_existing_tag_to_selected(self) -> None:
        tag = self.edit_tag_select.get().strip()
        if not tag:
            return
        self._append_tag_to_selected(tag)

    def _add_new_tag_to_selected(self) -> None:
        tag = self.new_tag_entry.get().strip()
        if not tag:
            return
        self._append_tag_to_selected(tag)
        self.new_tag_entry.set("")

    def _append_tag_to_selected(self, tag: str) -> None:
        paths = self._get_selected_paths()
        if not paths:
            messagebox.showinfo("No Selection", "Select a replay first.")
            return
        for path in paths:
            current_tags = list(self.tags.get("tags", {}).get(path, []))
            if tag not in current_tags:
                current_tags.append(tag)
            set_tags(self.tags, path, current_tags)
        save_tags(self.tags)
        first_path = paths[0]
        self.tags_entry.set(", ".join(self.tags.get("tags", {}).get(first_path, [])))
        self._refresh_filters()
        self._refresh_list()
        self.status.set(f"Tag '{tag}' added to {len(paths)} replay(s)")

    def _open_in_folder(self) -> None:
        path = self._get_selected_path()
        if not path:
            messagebox.showinfo("No Selection", "Select a replay first.")
            return
        file_path = Path(path)
        if not file_path.exists():
            messagebox.showwarning("Missing File", "Replay file not found.")
            return
        try:
            if os.name == "nt":
                subprocess.run(["explorer", "/select,", str(file_path)], check=False)
            elif sys.platform == "darwin":
                subprocess.run(["open", "-R", str(file_path)], check=False)
            else:
                subprocess.run(["xdg-open", str(file_path.parent)], check=False)
        except Exception:
            messagebox.showwarning("Open Failed", "Could not open file explorer.")

    def _export_full_csv(self) -> None:
        if not self.index.get("replays", []):
            messagebox.showinfo("No Data", "No replays to export. Please scan first.")
            return
        filename = filedialog.asksaveasfilename(
            title="Export Full CSV",
            defaultextension=".csv",
            filetypes=[("CSV Files", "*.csv")],
        )
        if not filename:
            return

        tags_map = self.tags.get("tags", {})
        build_orders = self.tags.get("build_orders", {})
        favorites = set(self.tags.get("favorites", []))

        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "filename",
                    "path",
                    "source_folder",
                    "map",
                    "start_time",
                    "length",
                    "game_type",
                    "speed",
                    "matchup",
                    "players",
                    "build_order_auto",
                    "bo_sequences",
                    "proxy_flag",
                    "proxy_distance_max",
                    "proxy_distances",
                    "proxy_threshold",
                    "mtime",
                    "size",
                    "tags",
                    "build_order_manual",
                    "favorite",
                ],
            )
            writer.writeheader()
            for item in self.index.get("replays", []):
                path = item.get("path", "")
                writer.writerow(
                    {
                        "filename": item.get("filename", ""),
                        "path": path,
                        "source_folder": item.get("source_folder", ""),
                        "map": item.get("map", ""),
                        "start_time": item.get("start_time", ""),
                        "length": item.get("length", ""),
                        "game_type": item.get("game_type", ""),
                        "speed": item.get("speed", ""),
                        "matchup": item.get("matchup", ""),
                        "players": json.dumps(item.get("players", []), ensure_ascii=False),
                        "build_order_auto": item.get("build_order_auto", ""),
                        "bo_sequences": json.dumps(item.get("bo_sequences", []), ensure_ascii=False),
                        "proxy_flag": bool(item.get("proxy_flag")),
                        "proxy_distance_max": item.get("proxy_distance_max", ""),
                        "proxy_distances": json.dumps(item.get("proxy_distances", {}), ensure_ascii=False),
                        "proxy_threshold": item.get("proxy_threshold", ""),
                        "mtime": item.get("mtime", ""),
                        "size": item.get("size", ""),
                        "tags": ", ".join(tags_map.get(path, [])),
                        "build_order_manual": build_orders.get(path, ""),
                        "favorite": path in favorites,
                    }
                )

    def _import_csv(self) -> None:
        filename = filedialog.askopenfilename(
            title="Import CSV",
            filetypes=[("CSV Files", "*.csv")],
        )
        if not filename:
            return
        csv_path = Path(filename)
        base_folder = csv_path.parent

        try:
            with open(filename, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Import Failed", f"Could not read CSV: {exc}")
            return

        if not rows:
            messagebox.showinfo("No Data", "CSV is empty.")
            return

        def item_source_folder(item: Dict[str, Any]) -> str:
            folder = item.get("source_folder", "") or ""
            if folder:
                return folder
            index_folder = str(self.index.get("folder", ""))
            path_value = str(item.get("path", ""))
            if index_folder and path_value.startswith(index_folder):
                return index_folder
            try:
                return str(Path(path_value).parent)
            except Exception:
                return ""

        existing: Dict[str, Dict[str, Any]] = {}
        for item in self.index.get("replays", []):
            key = f"{item_source_folder(item)}|{item.get('filename', '')}"
            existing[key] = item
        tags_map = self.tags.get("tags", {})
        build_orders = self.tags.get("build_orders", {})
        favorites = set(self.tags.get("favorites", []))

        def parse_json(value: str, fallback: Any) -> Any:
            if value is None or value == "":
                return fallback
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return fallback

        imported = 0
        for row in rows:
            filename_value = (row.get("filename") or "").strip()
            if not filename_value:
                continue
            local_path = (base_folder / filename_value).resolve()
            row_path = (row.get("path") or "").strip()
            if local_path.exists():
                path = str(local_path)
            elif row_path:
                path = row_path
            else:
                path = str(local_path)

            item = {
                "path": path,
                "filename": filename_value,
                "source_folder": row.get("source_folder") or str(base_folder),
                "map": row.get("map", ""),
                "start_time": row.get("start_time", ""),
                "length": row.get("length", ""),
                "game_type": row.get("game_type", ""),
                "speed": row.get("speed", ""),
                "matchup": row.get("matchup", ""),
                "players": parse_json(row.get("players", ""), []),
                "build_order_auto": row.get("build_order_auto", ""),
                "bo_sequences": parse_json(row.get("bo_sequences", ""), []),
                "proxy_flag": str(row.get("proxy_flag", "")).lower() in {"true", "1", "yes"},
                "proxy_distance_max": row.get("proxy_distance_max", ""),
                "proxy_distances": parse_json(row.get("proxy_distances", ""), {}),
                "proxy_threshold": row.get("proxy_threshold", ""),
                "mtime": row.get("mtime", ""),
                "size": row.get("size", ""),
            }

            key = f"{item.get('source_folder', '')}|{filename_value}"
            existing[key] = item
            imported += 1

            tag_str = row.get("tags", "")
            if tag_str:
                incoming_tags = [t.strip() for t in tag_str.split(",") if t.strip()]
                if incoming_tags:
                    current_tags = set(tags_map.get(path, []))
                    current_tags.update(incoming_tags)
                    tags_map[path] = sorted(current_tags)

            manual_bo = row.get("build_order_manual", "")
            if manual_bo:
                build_orders[path] = manual_bo

            favorite_raw = str(row.get("favorite", "")).lower()
            if favorite_raw in {"true", "1", "yes"}:
                favorites.add(path)

        self.index["replays"] = list(existing.values())
        self.index["folders"] = sorted(
            {item.get("source_folder", "") for item in self.index.get("replays", []) if item.get("source_folder")}
        )
        save_index(self.index)

        self.tags["tags"] = tags_map
        self.tags["build_orders"] = build_orders
        self.tags["favorites"] = sorted(favorites)
        save_tags(self.tags)

        if str(base_folder) not in self.replay_folders:
            self.replay_folders.append(str(base_folder))
            self.settings["replay_folders"] = list(self.replay_folders)
            self.settings["replay_folder"] = str(base_folder)
            save_settings(self.settings)

        self._sync_folder_controls()
        self._refresh_filters()
        self._refresh_list()
        messagebox.showinfo("Import Complete", f"Imported {imported} replays.")

    def _clear_history(self) -> None:
        confirm = messagebox.askyesno(
            "Supprimer l'historique",
            "Supprimer tout l'historique de scan et les tags ?",
        )
        if not confirm:
            return
        for filename in ("replay_index.json", "replay_tags.json"):
            path = get_data_dir() / filename
            try:
                if path.exists():
                    path.unlink()
            except Exception:
                pass
        self.index = {"replays": []}
        self.tags = {"favorites": [], "tags": {}, "build_orders": {}}
        self._refresh_filters()
        self._refresh_list()
        self.status.set("History cleared")

    def _log_scan(self, message: str) -> None:
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._scan_log_path.parent.mkdir(parents=True, exist_ok=True)
            with self._scan_log_path.open("a", encoding="utf-8") as f:
                f.write(f"[{timestamp}] {message}\n")
        except Exception:
            pass


def main() -> None:
    root = tk.Tk()
    App(root)
    root.mainloop()


if __name__ == "__main__":
    main()
