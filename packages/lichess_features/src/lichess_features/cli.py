"""CLI entrypoints for ``lichess-features``."""

from __future__ import annotations

import argparse

from lichess_data.extract import lichess_downloader as ld
from lichess_features.split import run_split


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="lichess-features",
        description="Lichess feature store tools",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser(
        "split",
        help="Chronological train/test split via Feast historical features",
    )
    month_g = sp.add_mutually_exclusive_group(required=True)
    month_g.add_argument("--month", metavar="YYYY-MM", help="Shard month to split")
    month_g.add_argument(
        "--previous-month",
        action="store_true",
        help="Split the prior calendar month",
    )
    sp.add_argument(
        "--test-size",
        type=float,
        default=None,
        help="Fraction of rows for chronological test split (default: from config)",
    )
    sp.add_argument(
        "--use-sample",
        action="store_true",
        help="Cap combined games before train/test split (OOM-safe dev runs)",
    )
    sp.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Max games to keep when --use-sample (default: from config)",
    )

    args = parser.parse_args(argv)

    if args.cmd == "split":
        return _cmd_split(args)

    return 2


def _cmd_split(args: argparse.Namespace) -> int:
    month = ld.resolve_previous_month() if args.previous_month else args.month
    train_path, test_path = run_split(
        month,
        test_size=args.test_size,
        use_sample=args.use_sample or None,
        max_rows=args.max_rows,
    )
    print(train_path)
    print(test_path)
    return 0
