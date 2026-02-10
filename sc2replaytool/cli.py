from __future__ import annotations

import argparse
from pathlib import Path

from .core.indexer import scan_replays, load_index
from .core.tags import load_tags, save_tags, set_favorite, set_build_order


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SC2 Replay Analyzer (CLI)")
    parser.add_argument("--replays", type=Path, help="Folder containing .SC2Replay files")
    parser.add_argument("--scan", action="store_true", help="Scan and index replays")
    parser.add_argument("--list", action="store_true", help="List indexed replays")
    parser.add_argument("--matchup", type=str, default="", help="Filter by matchup (e.g. PvT)")
    parser.add_argument("--favorite", action="store_true", help="Filter favorites only")
    parser.add_argument("--build-order", type=str, default="", help="Filter by build order")
    parser.add_argument("--player", type=str, default="", help="Filter by player name (substring)")
    parser.add_argument("--map", type=str, default="", help="Filter by map name (substring)")
    parser.add_argument("--proxy", action="store_true", help="Filter proxy-only replays")
    parser.add_argument("--export-csv", type=Path, help="Export filtered list to CSV")
    parser.add_argument("--proxy-threshold", type=float, default=35.0, help="Proxy distance threshold")
    parser.add_argument("--tag", type=str, default="", help="Filter by tag")
    parser.add_argument("--set-tags", type=Path, help="Set tags for a replay path")
    parser.add_argument("--tags-value", type=str, default="", help="Comma separated tags")
    parser.add_argument("--set-favorite", type=Path, help="Toggle favorite for a replay path")
    parser.add_argument("--favorite-value", action="store_true", help="Set favorite to true (default false)")
    parser.add_argument("--set-build-order", type=Path, help="Set build order for a replay path")
    parser.add_argument("--build-order-value", type=str, default="", help="Build order value")
    return parser.parse_args()


def _format_winner(players: list[dict[str, object]]) -> str:
    winners = []
    for p in players:
        result = str(p.get("result", "")).lower()
        if result in {"win", "winner", "victory"}:
            winners.append(str(p.get("name", "")))
    winners = [w for w in winners if w]
    if not winners:
        return "Unknown"
    return " | ".join(winners)


def main() -> None:
    args = parse_args()

    if args.scan:
        if not args.replays:
            raise SystemExit("--replays is required for --scan")
        scan_replays(args.replays, proxy_threshold=args.proxy_threshold)

    if args.set_favorite:
        tags = load_tags()
        set_favorite(tags, str(args.set_favorite.resolve()), args.favorite_value)
        save_tags(tags)

    if args.set_build_order:
        tags = load_tags()
        set_build_order(tags, str(args.set_build_order.resolve()), args.build_order_value)
        save_tags(tags)
    if args.set_tags:
        tags = load_tags()
        from .core.tags import set_tags as _set_tags

        tag_list = [t.strip() for t in args.tags_value.split(",")] if args.tags_value else []
        _set_tags(tags, str(args.set_tags.resolve()), tag_list)
        save_tags(tags)

    if args.list:
        index = load_index()
        tags = load_tags()
        rows = []
        for item in index.get("replays", []):
            if args.matchup and item.get("matchup") != args.matchup:
                continue
            if args.favorite and item.get("path") not in tags.get("favorites", []):
                continue
            if args.proxy and not item.get("proxy_flag"):
                continue
            if args.tag and args.tag not in tags.get("tags", {}).get(item.get("path", ""), []):
                continue
            if args.build_order:
                manual_bo = tags.get("build_orders", {}).get(item.get("path", ""), "")
                auto_bo = item.get("build_order_auto", "")
                display_bo = manual_bo or auto_bo
                if display_bo != args.build_order:
                    continue
            if args.player:
                players = item.get("players", [])
                if not any(args.player.lower() in str(p.get("name", "")).lower() for p in players):
                    continue
            if args.map and args.map.lower() not in str(item.get("map", "")).lower():
                continue
            bo_display = tags.get("build_orders", {}).get(item.get("path", ""), "") or item.get("build_order_auto", "")
            players = "; ".join(p.get("name", "") for p in item.get("players", []))
            tag_list = tags.get("tags", {}).get(item.get("path", ""), [])
            tags_str = ", ".join(tag_list)
            winner = _format_winner(item.get("players", []))
            line = f"{item.get('filename')} | {players} | {winner} | {item.get('matchup')} | {item.get('map')} | {tags_str} | {bo_display}"
            print(line)
            rows.append(
                {
                    "filename": item.get("filename", ""),
                    "path": item.get("path", ""),
                    "map": item.get("map", ""),
                    "winner": _format_winner(item.get("players", [])),
                    "matchup": item.get("matchup", ""),
                    "date": item.get("start_time", ""),
                    "length": item.get("length", ""),
                    "players": players,
                    "favorite": item.get("path") in tags.get("favorites", []),
                    "tags": ", ".join(tags.get("tags", {}).get(item.get("path", ""), [])),
                    "build_order_manual": tags.get("build_orders", {}).get(item.get("path", ""), ""),
                    "build_order_auto": item.get("build_order_auto", ""),
                    "proxy_distance": item.get("proxy_distance_max", ""),
                    "proxy_by_player": item.get("proxy_distances", {}),
                    "proxy_flag": bool(item.get("proxy_flag")),
                }
            )

        if args.export_csv:
            import csv

            with args.export_csv.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=[
                        "filename",
                        "path",
                        "map",
                        "winner",
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
                writer.writerows(rows)


if __name__ == "__main__":
    main()
