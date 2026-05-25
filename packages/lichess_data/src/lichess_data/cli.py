"""CLI entrypoints for ``lichess-data``."""

from __future__ import annotations

import argparse
from pathlib import Path

from libs.shared import load_config

from lichess_data.extract import lichess_downloader as ld


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="lichess-data",
        description="Lichess open database pipeline tools",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    dl = sub.add_parser("download", help="Download a monthly standard-rated .pgn.zst shard")
    dl.add_argument(
        "--list",
        action="store_true",
        dest="list_shards",
        help="Print available months from the live index",
    )
    g = dl.add_mutually_exclusive_group(required=False)
    g.add_argument("--month", metavar="YYYY-MM", help="Shard month to fetch")
    g.add_argument(
        "--previous-month",
        action="store_true",
        help="Download the prior calendar month",
    )
    dl.add_argument(
        "--no-verify",
        action="store_true",
        help="Skip SHA256 verification (not recommended)",
    )
    dl.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Always re-download even if the file exists",
    )
    dl.add_argument(
        "--dest",
        type=Path,
        default=None,
        help="Directory for the .pgn.zst (default: artifact path from config)",
    )
    dl.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable tqdm progress bar",
    )
    dl.add_argument(
        "--base-url",
        default=None,
        metavar="URL",
        help=f"Override Lichess database base URL (default: {ld.DEFAULT_BASE_URL})",
    )

    args = parser.parse_args(argv)
    cfg = load_config("lichess_data")

    if args.cmd == "download":
        return _cmd_download(args, cfg)

    return 2


def _cmd_download(args: argparse.Namespace, cfg: dict) -> int:
    if args.list_shards:
        shards = ld.fetch_monthly_index(config=cfg)
        for s in shards:
            print(
                f"{s.year_month}\t{s.month_label}\t{s.size}\t{s.game_count}\t{s.filename}"
            )
        return 0

    verify = not args.no_verify
    skip_existing = not args.no_skip_existing
    progress = not args.no_progress
    kw: dict = {
        "verify": verify,
        "skip_existing": skip_existing,
        "progress": progress,
        "config": cfg,
    }
    if args.dest is not None:
        kw["dest_dir"] = args.dest
    if args.base_url is not None:
        kw["base_url"] = args.base_url

    if args.previous_month:
        path = ld.download_previous_month(**kw)
    elif args.month:
        path = ld.download_month(args.month, **kw)
    else:
        print(
            "error: specify --month YYYY-MM, --previous-month, or --list",
            flush=True,
        )
        return 2

    print(path.resolve())
    return 0
