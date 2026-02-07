from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

from .storage import load_json, save_json
from .paths import get_data_dir


TAGS_FILENAME = "replay_tags.json"


def tags_path() -> Path:
    return get_data_dir() / TAGS_FILENAME


def load_tags() -> Dict[str, Any]:
    return load_json(tags_path(), {"favorites": [], "build_orders": {}, "tags": {}})


def save_tags(tags: Dict[str, Any]) -> None:
    save_json(tags_path(), tags)


def is_favorite(tags: Dict[str, Any], replay_path: str) -> bool:
    return replay_path in set(tags.get("favorites", []))


def set_favorite(tags: Dict[str, Any], replay_path: str, value: bool) -> None:
    favorites = set(tags.get("favorites", []))
    if value:
        favorites.add(replay_path)
    else:
        favorites.discard(replay_path)
    tags["favorites"] = sorted(favorites)


def get_build_order(tags: Dict[str, Any], replay_path: str) -> str:
    return tags.get("build_orders", {}).get(replay_path, "")


def set_build_order(tags: Dict[str, Any], replay_path: str, build_order: str) -> None:
    build_orders = tags.get("build_orders", {})
    if build_order:
        build_orders[replay_path] = build_order
    else:
        build_orders.pop(replay_path, None)
    tags["build_orders"] = build_orders


def get_tags(tags: Dict[str, Any], replay_path: str) -> List[str]:
    return list(tags.get("tags", {}).get(replay_path, []))


def set_tags(tags: Dict[str, Any], replay_path: str, tag_list: List[str]) -> None:
    tags_map = tags.get("tags", {})
    cleaned = [t for t in (t.strip() for t in tag_list) if t]
    if cleaned:
        tags_map[replay_path] = sorted(set(cleaned))
    else:
        tags_map.pop(replay_path, None)
    tags["tags"] = tags_map
