from __future__ import annotations

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

from .core.indexer import scan_replays, load_index
from .core.tags import load_tags, save_tags, set_favorite, set_build_order, set_tags
from .core.paths import get_data_dir


SETTINGS_FILENAME = "settings.json"


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


class App:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("SC2 Replay Analyzer")
        self.root.geometry("1100x700")

        self.index: Dict[str, Any] = load_index()
        self.tags: Dict[str, Any] = load_tags()
        self.settings: Dict[str, Any] = load_settings()

        self.replay_folder = tk.StringVar(value=self.settings.get("replay_folder", ""))
        self.matchup_filter = tk.StringVar(value="All")
        self.build_order_mode = tk.StringVar(value="Tech")
        self.tag_filter = tk.StringVar(value="All")
        self.player_count_filter = tk.StringVar(value="All")
        self.race_filter = tk.StringVar(value="All")
        self.player_filter = tk.StringVar(value="")
        self.map_filter = tk.StringVar(value="")
        self.proxy_only = tk.BooleanVar(value=False)
        self.favorite_only = tk.BooleanVar(value=False)
        self.proxy_threshold = tk.StringVar(value=str(self.settings.get("proxy_threshold", 35.0)))
        self.bo_step_vars = [tk.StringVar(value="Any") for _ in range(8)]
        self.build_order_entry = tk.StringVar(value="")
        self.tags_entry = tk.StringVar(value="")
        self.tags_search = tk.StringVar(value="")
        self.new_tag_entry = tk.StringVar(value="")
        self.edit_tag_select = tk.StringVar(value="")
        self.status = tk.StringVar(value="Ready")
        self.progress_var = tk.DoubleVar(value=0.0)

        self.scan_queue: Queue[Any] = Queue()

        self._build_ui()
        self._refresh_filters()
        self._refresh_list()

    def _build_ui(self) -> None:
        frame = ttk.Frame(self.root, padding=10)
        frame.pack(fill=tk.BOTH, expand=True)

        folder_row = ttk.Frame(frame)
        folder_row.pack(fill=tk.X)

        ttk.Label(folder_row, text="Replay Folder:").pack(side=tk.LEFT)
        ttk.Entry(folder_row, textvariable=self.replay_folder, width=70).pack(side=tk.LEFT, padx=6)
        ttk.Button(folder_row, text="Browse", command=self._browse_folder).pack(side=tk.LEFT)
        ttk.Button(folder_row, text="Scan", command=self._start_scan).pack(side=tk.LEFT, padx=6)
        ttk.Button(folder_row, text="Reload", command=self._reload_index).pack(side=tk.LEFT)

        filter_row = ttk.Frame(frame)
        filter_row.pack(fill=tk.X, pady=8)

        ttk.Label(filter_row, text="Matchup:").pack(side=tk.LEFT)
        self.matchup_combo = ttk.Combobox(filter_row, textvariable=self.matchup_filter, state="readonly", width=12)
        self.matchup_combo.pack(side=tk.LEFT, padx=6)
        self.matchup_combo.bind("<<ComboboxSelected>>", lambda _e: self._refresh_list())

        ttk.Label(filter_row, text="Race:").pack(side=tk.LEFT)
        self.race_combo = ttk.Combobox(filter_row, textvariable=self.race_filter, state="readonly", width=6)
        self.race_combo["values"] = ["All", "T", "P", "Z", "R"]
        self.race_combo.pack(side=tk.LEFT, padx=6)
        self.race_combo.bind("<<ComboboxSelected>>", lambda _e: self._refresh_filters())

        ttk.Label(filter_row, text="Nombre de joueurs:").pack(side=tk.LEFT)
        self.player_count_combo = ttk.Combobox(filter_row, textvariable=self.player_count_filter, state="readonly", width=6)
        self.player_count_combo.pack(side=tk.LEFT, padx=6)
        self.player_count_combo.bind("<<ComboboxSelected>>", lambda _e: self._refresh_list())

        ttk.Checkbutton(filter_row, text="Favorites only", variable=self.favorite_only, command=self._refresh_list).pack(side=tk.LEFT, padx=6)
        ttk.Checkbutton(filter_row, text="Proxy only", variable=self.proxy_only, command=self._refresh_list).pack(side=tk.LEFT, padx=6)
        ttk.Label(filter_row, text="Tag:").pack(side=tk.LEFT, padx=6)
        self.tag_combo = ttk.Combobox(filter_row, textvariable=self.tag_filter, state="normal", width=14)
        self.tag_combo.pack(side=tk.LEFT, padx=6)
        self.tag_combo.bind("<<ComboboxSelected>>", lambda _e: self._refresh_list())
        self.tag_combo.bind("<KeyRelease>", lambda _e: self._refresh_list())

        bo_mode_row = ttk.Frame(frame)
        bo_mode_row.pack(fill=tk.X, pady=4)
        ttk.Label(bo_mode_row, text="Build Order Mode:").pack(side=tk.LEFT)
        self.build_order_mode_combo = ttk.Combobox(
            bo_mode_row,
            textvariable=self.build_order_mode,
            state="readonly",
            width=12,
            values=["Tech", "General"],
        )
        self.build_order_mode_combo.pack(side=tk.LEFT, padx=6)
        self.build_order_mode_combo.bind("<<ComboboxSelected>>", lambda _e: self._on_build_order_mode_change())

        bo_steps_row1 = ttk.Frame(frame)
        bo_steps_row1.pack(fill=tk.X, pady=2)
        bo_steps_row2 = ttk.Frame(frame)
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

        search_row = ttk.Frame(frame)
        search_row.pack(fill=tk.X, pady=4)
        ttk.Label(search_row, text="Player:").pack(side=tk.LEFT)
        player_entry = ttk.Entry(search_row, textvariable=self.player_filter, width=20)
        player_entry.pack(side=tk.LEFT, padx=6)
        player_entry.bind("<KeyRelease>", lambda _e: self._refresh_list())

        ttk.Label(search_row, text="Map:").pack(side=tk.LEFT)
        map_entry = ttk.Entry(search_row, textvariable=self.map_filter, width=24)
        map_entry.pack(side=tk.LEFT, padx=6)
        map_entry.bind("<KeyRelease>", lambda _e: self._refresh_list())

        action_row = ttk.Frame(frame)
        action_row.pack(fill=tk.X)

        ttk.Label(action_row, text="Selected Build Order (manual):").pack(side=tk.LEFT)
        ttk.Entry(action_row, textvariable=self.build_order_entry, width=30).pack(side=tk.LEFT, padx=6)
        ttk.Button(action_row, text="Set", command=self._set_selected_build_order).pack(side=tk.LEFT)
        ttk.Button(action_row, text="Export CSV", command=self._export_csv).pack(side=tk.LEFT, padx=6)

        ttk.Label(action_row, text="Proxy Threshold:").pack(side=tk.LEFT, padx=6)
        ttk.Entry(action_row, textvariable=self.proxy_threshold, width=6).pack(side=tk.LEFT)
        ttk.Button(action_row, text="Set Threshold", command=self._set_proxy_threshold).pack(side=tk.LEFT, padx=6)

        tags_row = ttk.Frame(frame)
        tags_row.pack(fill=tk.X, pady=4)
        ttk.Button(tags_row, text="Toggle Favorite", command=self._toggle_favorite).pack(side=tk.LEFT, padx=6)
        ttk.Label(tags_row, text="New Tag:").pack(side=tk.LEFT)
        ttk.Entry(tags_row, textvariable=self.new_tag_entry, width=16).pack(side=tk.LEFT, padx=6)
        ttk.Button(tags_row, text="Add To Selected", command=self._add_new_tag_to_selected).pack(side=tk.LEFT, padx=6)
        ttk.Label(tags_row, text="Tag Search:").pack(side=tk.LEFT, padx=6)
        self.tag_search_combo = ttk.Combobox(tags_row, textvariable=self.tags_search, state="normal", width=24)
        self.tag_search_combo.pack(side=tk.LEFT, padx=6)
        self.tag_search_combo.bind("<KeyRelease>", lambda _e: self._on_tag_search_change())
        self.tag_search_combo.bind("<<ComboboxSelected>>", lambda _e: self._refresh_list())

        columns = ("fav", "filename", "players", "matchup", "map", "date", "length", "tags", "build_order", "proxy_dist")
        tree_frame = ttk.Frame(frame)
        tree_frame.pack(fill=tk.BOTH, expand=True, pady=6)
        self.tree = ttk.Treeview(tree_frame, columns=columns, show="headings", height=18)
        self._sort_state = {}
        self.tree.heading("fav", text="Fav", command=lambda: self._sort_by("fav"))
        self.tree.heading("filename", text="Filename", command=lambda: self._sort_by("filename"))
        self.tree.heading("players", text="Players", command=lambda: self._sort_by("players"))
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
        self.details_text = tk.Text(details_box, height=8, wrap=tk.NONE)
        details_y = ttk.Scrollbar(details_box, orient=tk.VERTICAL, command=self.details_text.yview)
        details_x = ttk.Scrollbar(details_box, orient=tk.HORIZONTAL, command=self.details_text.xview)
        self.details_text.configure(yscrollcommand=details_y.set, xscrollcommand=details_x.set, state=tk.DISABLED)
        self.details_text.grid(row=0, column=0, sticky="nsew")
        details_y.grid(row=0, column=1, sticky="ns")
        details_x.grid(row=1, column=0, sticky="ew")
        details_box.rowconfigure(0, weight=1)
        details_box.columnconfigure(0, weight=1)

        details_tags_row = ttk.Frame(frame)
        details_tags_row.pack(fill=tk.X, pady=4)
        ttk.Label(details_tags_row, text="Edit Tags (comma):").pack(side=tk.LEFT)
        ttk.Entry(details_tags_row, textvariable=self.tags_entry, width=32).pack(side=tk.LEFT, padx=6)
        ttk.Label(details_tags_row, text="Add Existing:").pack(side=tk.LEFT, padx=6)
        self.edit_tag_combo = ttk.Combobox(details_tags_row, textvariable=self.edit_tag_select, state="readonly", width=18)
        self.edit_tag_combo.pack(side=tk.LEFT, padx=6)
        ttk.Button(details_tags_row, text="Add Tag", command=self._add_existing_tag_to_selected).pack(side=tk.LEFT, padx=6)
        ttk.Button(details_tags_row, text="Update Tags", command=self._set_selected_tags).pack(side=tk.LEFT)

        open_row = ttk.Frame(frame)
        open_row.pack(fill=tk.X, pady=4)
        ttk.Button(open_row, text="Open In Folder", command=self._open_in_folder).pack(side=tk.LEFT, padx=6)

        status_row = ttk.Frame(frame)
        status_row.pack(fill=tk.X)
        ttk.Label(status_row, textvariable=self.status).pack(side=tk.LEFT)
        self.progress = ttk.Progressbar(status_row, orient=tk.HORIZONTAL, length=260, mode="determinate", variable=self.progress_var)
        self.progress.pack(side=tk.RIGHT)

    def _browse_folder(self) -> None:
        folder = filedialog.askdirectory(title="Select Replay Folder")
        if folder:
            self.replay_folder.set(folder)
            self.settings["replay_folder"] = folder
            save_settings(self.settings)

    def _start_scan(self) -> None:
        folder = self.replay_folder.get().strip()
        if not folder:
            messagebox.showwarning("Missing Folder", "Please select a replay folder first.")
            return
        self.status.set("Scanning...")
        self.progress_var.set(0.0)
        self.progress["value"] = 0
        threshold = self._get_proxy_threshold()
        if threshold is None:
            return
        thread = threading.Thread(target=self._scan_worker, args=(Path(folder), threshold), daemon=True)
        thread.start()
        self.root.after(100, self._poll_scan)

    def _reload_index(self) -> None:
        self.index = load_index()
        self.tags = load_tags()
        self._refresh_filters()
        self._refresh_list()

    def _scan_worker(self, folder: Path, threshold: float) -> None:
        def progress_cb(current: int, total: int) -> None:
            self.scan_queue.put(("progress", current, total))

        index = scan_replays(folder, proxy_threshold=threshold, progress_cb=progress_cb)
        self.scan_queue.put(("done", index))

    def _poll_scan(self) -> None:
        if self.scan_queue.empty():
            self.root.after(100, self._poll_scan)
            return
        item = self.scan_queue.get()
        if isinstance(item, tuple) and item[0] == "progress":
            _tag, current, total = item
            if total:
                percent = (current / total) * 100.0
                self.progress_var.set(percent)
                self.status.set(f"Scanning... {current}/{total}")
            self.root.after(50, self._poll_scan)
            return
        if isinstance(item, tuple) and item[0] == "done":
            self.index = item[1]
            self.tags = load_tags()
            self.progress_var.set(100.0)
            self.status.set("Scan complete")
            self._refresh_filters()
            self._refresh_list()

    def _refresh_filters(self) -> None:
        matchups = sorted({item.get("matchup", "Unknown") for item in self.index.get("replays", [])})
        matchups = ["All"] + matchups
        self.matchup_combo["values"] = matchups
        if self.matchup_filter.get() not in matchups:
            self.matchup_filter.set("All")
        if self.race_filter.get() not in {"All", "T", "P", "Z", "R"}:
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
        self.tag_search_combo["values"] = self.available_tags
        self.edit_tag_combo["values"] = self.available_tags
        self._refresh_build_order_options()
        self._refresh_list()

    def _refresh_list(self) -> None:
        self.tree.delete(*self.tree.get_children())

        matchup_filter = self.matchup_filter.get()
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

        for item in self.index.get("replays", []):
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

            self.tree.insert(
                "",
                tk.END,
                values=(
                    "Y" if item.get("path") in favorites else "",
                    item.get("filename"),
                    self._format_players(item.get("players", [])),
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

        self.status.set(f"Loaded {len(self.tree.get_children())} replays")
        if hasattr(self, "last_sort_column"):
            self._sort_by(self.last_sort_column, toggle=False)

    def _get_selected_path(self) -> str | None:
        selection = self.tree.selection()
        if not selection:
            return None
        item = self.tree.item(selection[0])
        tags = item.get("tags") or []
        return tags[0] if tags else None

    def _on_select(self, _event: Any) -> None:
        path = self._get_selected_path()
        if not path:
            self.build_order_entry.set("")
            self._set_details("")
            return
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
            f"Player Count: {len(item.get('players', []))}",
            f"Tags: {', '.join(self.tags.get('tags', {}).get(path, []))}",
            f"Build Order (manual): {manual_bo}",
            f"Build Order (auto): {auto_bo}",
            f"Proxy Distances: {proxy_by_player}",
            f"Proxy Threshold: {item.get('proxy_threshold', '')}",
        ]
        self._set_details("\n".join(details))

    def _set_selected_build_order(self) -> None:
        path = self._get_selected_path()
        if not path:
            messagebox.showinfo("No Selection", "Select a replay first.")
            return
        value = self.build_order_entry.get().strip()
        set_build_order(self.tags, path, value)
        save_tags(self.tags)
        self._refresh_filters()
        self._refresh_list()

    def _set_selected_tags(self) -> None:
        path = self._get_selected_path()
        if not path:
            messagebox.showinfo("No Selection", "Select a replay first.")
            return
        raw = self.tags_entry.get().strip()
        tag_list = [t.strip() for t in raw.split(",")] if raw else []
        set_tags(self.tags, path, tag_list)
        save_tags(self.tags)
        self._refresh_filters()
        self._refresh_list()

    def _toggle_favorite(self) -> None:
        path = self._get_selected_path()
        if not path:
            messagebox.showinfo("No Selection", "Select a replay first.")
            return
        is_fav = path in set(self.tags.get("favorites", []))
        set_favorite(self.tags, path, not is_fav)
        save_tags(self.tags)
        self._refresh_list()

    def _on_build_order_mode_change(self) -> None:
        self._reset_bo_steps_from(0)
        self._refresh_build_order_options()
        self._refresh_list()

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
        mode = self.build_order_mode.get()
        key = "seq_tech" if mode == "Tech" else "seq_general"
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
        favorites = set(self.tags.get("favorites", []))
        player_query = self.player_filter.get().strip().lower()
        map_query = self.map_filter.get().strip().lower()
        tags_map = self.tags.get("tags", {})

        for item in self.index.get("replays", []):
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
            "matchup": 3,
            "map": 4,
            "date": 5,
            "length": 6,
            "tags": 7,
            "build_order": 8,
            "proxy_dist": 9,
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
        current = self.tags_entry.get()
        parts = [t.strip() for t in current.split(",") if t.strip()]
        if tag not in parts:
            parts.append(tag)
        self.tags_entry.set(", ".join(parts))

    def _add_new_tag_to_selected(self) -> None:
        tag = self.new_tag_entry.get().strip()
        if not tag:
            return
        current = self.tags_entry.get()
        parts = [t.strip() for t in current.split(",") if t.strip()]
        if tag not in parts:
            parts.append(tag)
        self.tags_entry.set(", ".join(parts))
        self.new_tag_entry.set("")

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

    def _export_csv(self) -> None:
        if not hasattr(self, "filtered_items"):
            self._refresh_list()
        if not self.filtered_items:
            messagebox.showinfo("No Data", "No replays to export with current filters.")
            return
        filename = filedialog.asksaveasfilename(
            title="Export CSV",
            defaultextension=".csv",
            filetypes=[("CSV Files", "*.csv")],
        )
        if not filename:
            return

        favorites = set(self.tags.get("favorites", []))
        build_orders = self.tags.get("build_orders", {})

        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "filename",
                    "path",
                    "map",
                    "matchup",
                    "date",
                    "length",
                    "players",
                    "favorite",
                        "tags",
                    "build_order_manual",
                    "build_order_auto",
                    "proxy_distance",
                    "proxy_by_player",
                    "proxy_flag",
                ],
            )
            writer.writeheader()
            for item in self.filtered_items:
                manual_bo = build_orders.get(item.get("path", ""), "")
                auto_bo = item.get("build_order_auto", "")
                writer.writerow(
                    {
                        "filename": item.get("filename", ""),
                        "path": item.get("path", ""),
                        "map": item.get("map", ""),
                        "matchup": item.get("matchup", ""),
                        "date": format_date(item.get("start_time", "")),
                        "length": format_length(item.get("length", "")),
                        "players": self._format_players(item.get("players", [])),
                        "favorite": item.get("path") in favorites,
                        "tags": ", ".join(self.tags.get("tags", {}).get(item.get("path", ""), [])),
                        "build_order_manual": manual_bo,
                        "build_order_auto": auto_bo,
                        "proxy_distance": self._format_proxy_distance(item.get("proxy_distance_max")),
                        "proxy_by_player": self._format_proxy_by_player(item),
                        "proxy_flag": bool(item.get("proxy_flag")),
                    }
                )


def main() -> None:
    root = tk.Tk()
    App(root)
    root.mainloop()


if __name__ == "__main__":
    main()
