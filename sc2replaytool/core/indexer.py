from __future__ import annotations

import os
from pathlib import Path
import sys
import importlib
import importlib.util
import logging
from typing import Dict, List, Any, Iterable, Optional, Tuple

from .storage import load_json, save_json
from .paths import get_data_dir


INDEX_FILENAME = "replay_index.json"


def _canonical_unit_name(name: str) -> str:
    return name.replace(" ", "").replace("_", "").lower()


def _build_unit_name_map(names: Iterable[str]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for name in names:
        mapping.setdefault(_canonical_unit_name(name), name)
    return mapping


def _normalize_unit_name(name: str, mapping: Dict[str, str]) -> Optional[str]:
    return mapping.get(_canonical_unit_name(name))


def index_path() -> Path:
    return get_data_dir() / INDEX_FILENAME


def _iter_replay_files(folder: Path) -> Iterable[Path]:
    for root, _dirs, files in os.walk(folder):
        for name in files:
            if name.lower().endswith(".sc2replay"):
                yield Path(root) / name


def _safe_race(player: Any) -> str:
    race = getattr(player, "play_race", None) or getattr(player, "race", None) or "Unknown"
    race = race[:1].upper()
    if race not in {"P", "T", "Z", "R"}:
        return "U"
    return race


def _matchup_from_players(players: List[Any]) -> str:
    if not players:
        return "Unknown"

    if len(players) == 2:
        races = sorted([_safe_race(p) for p in players])
        return f"{races[0]}v{races[1]}"

    team_map: Dict[Any, List[str]] = {}
    for p in players:
        team_id = getattr(p, "team_id", None)
        if team_id is None:
            team = getattr(p, "team", None)
            team_id = getattr(team, "number", None)
        team_map.setdefault(team_id, []).append(_safe_race(p))

    team_strings = []
    for _team_id, races in team_map.items():
        team_strings.append("+".join(sorted(races)))

    return " vs ".join(team_strings)


def _event_player_id(event: Any) -> Optional[int]:
    for attr in ("player_id", "pid", "control_pid", "unit_owner_id", "player"):
        value = getattr(event, attr, None)
        if isinstance(value, int):
            return value
        if hasattr(value, "pid"):
            return getattr(value, "pid")
    return None


def _extract_position(event: Any) -> Optional[Tuple[float, float]]:
    loc = getattr(event, "location", None)
    if isinstance(loc, (tuple, list)) and len(loc) == 2:
        return float(loc[0]), float(loc[1])
    x = getattr(event, "x", None)
    y = getattr(event, "y", None)
    if x is None or y is None:
        return None
    return float(x), float(y)


def _distance(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    dx = a[0] - b[0]
    dy = a[1] - b[1]
    return (dx * dx + dy * dy) ** 0.5


def _proxy_info(replay: Any, threshold: float = 35.0) -> Dict[str, Any]:
    events = getattr(replay, "tracker_events", None)
    if not events:
        logging.debug("No tracker events for replay")
        return {"proxy_flag": False, "proxy_distance_max": None, "proxy_distances": {}}

    players = {}
    for p in getattr(replay, "players", []):
        pid = getattr(p, "pid", None) or getattr(p, "player_id", None) or getattr(p, "id", None)
        if pid is not None:
            players[pid] = p

    townhalls = [
        "Command Center",
        "Orbital Command",
        "Planetary Fortress",
        "Nexus",
        "Hatchery",
        "Lair",
        "Hive",
    ]
    buildings = [
        "Supply Depot",
        "Barracks",
        "Refinery",
        "Factory",
        "Starport",
        "Engineering Bay",
        "Bunker",
        "Missile Turret",
        "Armory",
        "Fusion Core",
        "Command Center",
        "Orbital Command",
        "Planetary Fortress",
        "Pylon",
        "Gateway",
        "Assimilator",
        "Cybernetics Core",
        "Robotics Facility",
        "Stargate",
        "Twilight Council",
        "Templar Archives",
        "Dark Shrine",
        "Forge",
        "Photon Cannon",
        "Nexus",
        "Spawning Pool",
        "Extractor",
        "Roach Warren",
        "Baneling Nest",
        "Lair",
        "Hydralisk Den",
        "Spire",
        "Hive",
        "Infestation Pit",
        "Evolution Chamber",
        "Spine Crawler",
        "Spore Crawler",
        "Ultralisk Cavern",
    ]
    townhall_set = set(townhalls)
    building_map = _build_unit_name_map(buildings)

    start_pos: Dict[int, Tuple[float, float]] = {}
    building_events: Dict[int, List[Tuple[int, Tuple[float, float], str, bool]]] = {pid: [] for pid in players}

    for event in events:
        unit_type = getattr(event, "unit_type_name", None)
        if not unit_type:
            continue
        unit_name = _normalize_unit_name(unit_type, building_map)
        if not unit_name:
            continue
        pid = _event_player_id(event)
        if pid is None or pid not in players:
            continue
        pos = _extract_position(event)
        if pos is None:
            continue
        frame = getattr(event, "frame", None)
        if frame is None:
            frame = getattr(event, "gameloop", 0)

        is_townhall = unit_name in townhall_set
        building_events[pid].append((frame, pos, unit_name, is_townhall))

        if is_townhall and pid not in start_pos:
            start_pos[pid] = pos
            continue

    for pid, events_list in building_events.items():
        if not events_list:
            logging.debug("No building events for pid=%s", pid)
            continue
        events_list.sort(key=lambda e: e[0])
        if pid not in start_pos:
            start_pos[pid] = events_list[0][1]
    distances: Dict[str, float] = {}
    max_dist: Optional[float] = None
    for pid, start in start_pos.items():
        events_list = building_events.get(pid, [])
        if not events_list:
            logging.debug("No building events for pid=%s", pid)
            continue
        first_four: List[float] = []
        for frame, pos, unit_type, is_townhall in events_list:
            if is_townhall:
                continue
            first_four.append(_distance(start, pos))
            if len(first_four) >= 4:
                break
        if not first_four:
            logging.debug("No non-townhall building found for pid=%s", pid)
            continue
        dist = max(first_four)
        distances[str(pid)] = dist
        if max_dist is None or dist > max_dist:
            max_dist = dist

    proxy_flag = max_dist is not None and max_dist > threshold
    return {
        "proxy_flag": proxy_flag,
        "proxy_distance_max": max_dist,
        "proxy_distances": distances,
        "proxy_threshold": threshold,
    }


def _collect_sequences(replay: Any) -> List[Dict[str, Any]]:
    events = getattr(replay, "tracker_events", None)
    if not events:
        return []

    players: Dict[int, Any] = {}
    for p in getattr(replay, "players", []):
        pid = getattr(p, "pid", None) or getattr(p, "player_id", None) or getattr(p, "id", None)
        if pid is not None:
            players[pid] = p

    tech_buildings = [
        "Barracks",
        "Factory",
        "Starport",
        "Command Center",
        "Orbital Command",
        "Planetary Fortress",
        "Gateway",
        "Cybernetics Core",
        "Robotics Facility",
        "Stargate",
        "Twilight Council",
        "Templar Archives",
        "Dark Shrine",
        "Nexus",
        "Spawning Pool",
        "Roach Warren",
        "Baneling Nest",
        "Lair",
        "Hydralisk Den",
        "Spire",
        "Hive",
        "Infestation Pit",
    ]

    townhalls = [
        "Command Center",
        "Orbital Command",
        "Planetary Fortress",
        "Nexus",
        "Hatchery",
        "Lair",
        "Hive",
    ]

    tech_map = _build_unit_name_map(tech_buildings)
    townhall_map = _build_unit_name_map(townhalls)
    townhall_set = set(townhalls)

    workers = {"SCV", "Probe", "Drone"}

    seq_tech: Dict[int, List[str]] = {pid: [] for pid in players}
    seq_general: Dict[int, List[str]] = {pid: [] for pid in players}
    skipped_start_townhall: Dict[int, bool] = {pid: False for pid in players}

    events = sorted(events, key=lambda e: getattr(e, "frame", getattr(e, "gameloop", 0)))
    max_steps = 8
    for event in events:
        class_name = event.__class__.__name__
        if class_name not in {"UnitInitEvent", "UnitBornEvent"}:
            continue
        unit_type = getattr(event, "unit_type_name", None)
        if not unit_type:
            continue
        pid = _event_player_id(event)
        if pid is None or pid not in players:
            continue

        unit_name_tech = _normalize_unit_name(unit_type, tech_map)
        if unit_name_tech and len(seq_tech[pid]) < max_steps:
            seq_tech[pid].append(unit_name_tech)

        unit_name_townhall = _normalize_unit_name(unit_type, townhall_map)
        unit_name_general = unit_name_tech or unit_name_townhall or unit_type
        if unit_type not in workers:
            if unit_name_townhall in townhall_set and not skipped_start_townhall[pid]:
                skipped_start_townhall[pid] = True
            else:
                if len(seq_general[pid]) < max_steps:
                    seq_general[pid].append(unit_name_general)

    sequences: List[Dict[str, Any]] = []
    for pid, p in players.items():
        sequences.append(
            {
                "pid": pid,
                "race": _safe_race(p),
                "name": getattr(p, "name", "Unknown"),
                "seq_tech": seq_tech.get(pid, []),
                "seq_general": seq_general.get(pid, []),
            }
        )

    return sequences


def _build_order_auto_from_sequences(sequences: List[Dict[str, Any]]) -> str:
    parts: List[str] = []
    for entry in sequences:
        sequence = entry.get("seq_tech", [])
        if not sequence:
            continue
        race = entry.get("race", "U")
        parts.append(f"{race}: {' > '.join(sequence[:3])}")
    return " | ".join(parts)


def _player_summary(players: List[Any]) -> List[Dict[str, Any]]:
    summary = []
    for p in players:
        pid = getattr(p, "pid", None) or getattr(p, "player_id", None) or getattr(p, "id", None)
        summary.append(
            {
                "name": getattr(p, "name", "Unknown"),
                "race": _safe_race(p),
                "result": getattr(p, "result", "Unknown"),
                "team_id": getattr(p, "team_id", None),
                "pid": pid,
            }
        )
    return summary


def _serialize_replay(
    replay: Any,
    path: Path,
    *,
    proxy_threshold: float = 35.0,
    source_folder: Optional[Path] = None,
) -> Dict[str, Any]:
    start_time = getattr(replay, "start_time", None) or getattr(replay, "date", None)
    length = getattr(replay, "length", None)

    sequences = _collect_sequences(replay)
    proxy_info = _proxy_info(replay, threshold=proxy_threshold)
    return {
        "path": str(path.resolve()),
        "filename": path.name,
        "source_folder": str(source_folder) if source_folder else "",
        "map": getattr(replay, "map_name", None) or getattr(replay, "map", None) or "Unknown",
        "start_time": start_time.isoformat() if start_time else "",
        "length": str(length) if length else "",
        "game_type": getattr(replay, "game_type", ""),
        "speed": getattr(replay, "speed", ""),
        "matchup": _matchup_from_players(getattr(replay, "players", [])),
        "players": _player_summary(getattr(replay, "players", [])),
        "build_order_auto": _build_order_auto_from_sequences(sequences),
        "bo_sequences": sequences,
        "proxy_flag": proxy_info.get("proxy_flag", False),
        "proxy_distance_max": proxy_info.get("proxy_distance_max"),
        "proxy_distances": proxy_info.get("proxy_distances", {}),
        "proxy_threshold": proxy_info.get("proxy_threshold", 35.0),
        "mtime": path.stat().st_mtime,
        "size": path.stat().st_size,
    }


def load_index() -> Dict[str, Any]:
    return load_json(index_path(), {"replays": []})


def save_index(index: Dict[str, Any]) -> None:
    save_json(index_path(), index)


def scan_replays(
    folder: Path,
    *,
    use_cache: bool = True,
    proxy_threshold: float = 35.0,
    progress_cb: Optional[callable] = None,
) -> Dict[str, Any]:
    _ensure_sc2reader()
    from sc2reader import load_replay

    folder = folder.resolve()
    existing = load_index() if use_cache else {"replays": []}
    by_path = {item["path"]: item for item in existing.get("replays", [])}

    updated: List[Dict[str, Any]] = []
    errors: List[str] = []

    replay_files = list(_iter_replay_files(folder))
    total = len(replay_files)
    for idx, replay_file in enumerate(replay_files, start=1):
        resolved = str(replay_file.resolve())
        stat = replay_file.stat()
        cached = by_path.get(resolved)
        if (
            cached
            and cached.get("mtime") == stat.st_mtime
            and cached.get("size") == stat.st_size
            and cached.get("proxy_threshold") == proxy_threshold
        ):
            updated.append(cached)
            if progress_cb:
                progress_cb(idx, total)
            continue

        try:
            replay = load_replay(str(replay_file), load_level=3)
            updated.append(_serialize_replay(replay, replay_file, proxy_threshold=proxy_threshold, source_folder=folder))
            if progress_cb:
                progress_cb(idx, total)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{replay_file}: {exc}")
            if progress_cb:
                progress_cb(idx, total)

    index = {
        "replays": updated,
        "errors": errors,
        "folder": str(folder),
        "folders": [str(folder)],
        "proxy_threshold": proxy_threshold,
    }
    save_index(index)
    return index


def scan_replays_multi(
    folders: Iterable[Path],
    *,
    use_cache: bool = True,
    proxy_threshold: float = 35.0,
    progress_cb: Optional[callable] = None,
) -> Dict[str, Any]:
    _ensure_sc2reader()
    from sc2reader import load_replay

    folder_list = [Path(folder).resolve() for folder in folders if folder]
    existing = load_index() if use_cache else {"replays": []}
    by_path = {item["path"]: item for item in existing.get("replays", [])}

    replay_files: List[Tuple[Path, Path]] = []
    for folder in folder_list:
        for replay_file in _iter_replay_files(folder):
            replay_files.append((replay_file, folder))

    updated: List[Dict[str, Any]] = []
    errors: List[str] = []
    total = len(replay_files)
    for idx, (replay_file, source_folder) in enumerate(replay_files, start=1):
        resolved = str(replay_file.resolve())
        stat = replay_file.stat()
        cached = by_path.get(resolved)
        if (
            cached
            and cached.get("mtime") == stat.st_mtime
            and cached.get("size") == stat.st_size
            and cached.get("proxy_threshold") == proxy_threshold
            and cached.get("source_folder") == str(source_folder)
        ):
            updated.append(cached)
            if progress_cb:
                progress_cb(idx, total)
            continue

        try:
            replay = load_replay(str(replay_file), load_level=3)
            updated.append(
                _serialize_replay(
                    replay,
                    replay_file,
                    proxy_threshold=proxy_threshold,
                    source_folder=source_folder,
                )
            )
            if progress_cb:
                progress_cb(idx, total)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{replay_file}: {exc}")
            if progress_cb:
                progress_cb(idx, total)

    index = {
        "replays": updated,
        "errors": errors,
        "folders": [str(folder) for folder in folder_list],
        "proxy_threshold": proxy_threshold,
    }
    save_index(index)
    return index


def _ensure_sc2reader() -> None:
    try:
        mod = importlib.import_module("sc2reader")
        if hasattr(mod, "load_replay") and getattr(mod, "__file__", None):
            return
    except Exception:
        pass

    repo_root = Path(__file__).resolve().parents[2]
    package_root = repo_root / "sc2reader" / "sc2reader"
    init_file = package_root / "__init__.py"
    if init_file.exists():
        spec = importlib.util.spec_from_file_location("sc2reader", init_file)
        if spec and spec.loader:
            module = importlib.util.module_from_spec(spec)
            sys.modules["sc2reader"] = module
            spec.loader.exec_module(module)
            return

    candidate = repo_root / "sc2reader"
    if candidate.exists():
        sys.path.insert(0, str(candidate))
        importlib.invalidate_caches()


def _setup_logging() -> None:
    log_path = get_data_dir() / "proxy_debug.log"
    logging.basicConfig(
        filename=str(log_path),
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(message)s",
    )


_setup_logging()
