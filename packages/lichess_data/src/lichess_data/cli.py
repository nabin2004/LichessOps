"""CLI entrypoints for ``lichess-data``."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import chess.pgn
from compression import zstd

from libs.shared import get_artifact_path, load_config

from lichess_data.extract import lichess_downloader as ld
from lichess_data.extract.parquet_stream_writer import ParquetStreamWriter
from lichess_data.extract.pgn_parser import PGNParser
from lichess_data.preprocessing.pipeline import run_pipeline


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

    pp = sub.add_parser(
        "preprocess",
        help="Transform raw Parquet into model-ready train/test splits",
    )
    pp.add_argument("path", type=Path, help="Path to raw .parquet file")
    pp.add_argument(
        "--test-size",
        type=float,
        default=0.2,
        help="Fraction of rows for chronological test split (default: 0.2)",
    )
    pp.add_argument(
        "--save-dir",
        type=Path,
        default=None,
        help="Directory for train.parquet and test.parquet (optional)",
    )

    ex = sub.add_parser(
        "extract",
        help="Convert a monthly .pgn.zst shard to Parquet",
    )
    ex.add_argument(
        "input",
        type=Path,
        help="Path to input .pgn.zst file",
    )
    ex.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output .parquet path (default: derived from input under artifacts/)",
    )
    ex.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Games per Parquet write batch (default: from config)",
    )

    args = parser.parse_args(argv)
    cfg = load_config("lichess_data")

    if args.cmd == "download":
        return _cmd_download(args, cfg)

    if args.cmd == "preprocess":
        return _cmd_preprocess(args)

    if args.cmd == "extract":
        return _cmd_extract(args, cfg)

    return 2


def _cmd_preprocess(args: argparse.Namespace) -> int:
    run_pipeline(
        path=args.path,
        test_size=args.test_size,
        save_dir=args.save_dir,
    )
    return 0


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


def _month_from_shard_name(filename: str) -> str | None:
    match = re.search(r"(\d{4}-\d{2})", filename)
    return match.group(1) if match else None


def _resolve_extract_output(input_path: Path, cfg: dict) -> Path:
    extract_cfg = cfg.get("extract") or {}
    subpath = extract_cfg.get("output_subpath", "processed")
    base = get_artifact_path("lichess_data", subpath, create=True)

    if base.suffix == ".parquet":
        return base

    month = _month_from_shard_name(input_path.name)
    name = f"{month}.parquet" if month else f"{input_path.stem}.parquet"
    return base / name


def _cmd_extract(args: argparse.Namespace, cfg: dict) -> int:
    extract_cfg = cfg.get("extract") or {}
    batch_size = (
        args.batch_size
        if args.batch_size is not None
        else int(extract_cfg.get("batch_size", 5000))
    )

    input_path = args.input.resolve()
    if not input_path.is_file():
        print(f"error: input file not found: {input_path}", flush=True)
        return 2

    output_path = (
        args.output.resolve()
        if args.output is not None
        else _resolve_extract_output(input_path, cfg)
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    run_extraction(str(input_path), str(output_path), batch_size)
    print(output_path)
    return 0


def run_extraction(input_path: str, output_path: str, batch_size: int) -> None:
    pgn_parser = PGNParser()

    with (
        zstd.open(input_path, "rt", encoding="utf-8") as f,
        ParquetStreamWriter(output_path, batch_size) as writer,
    ):
        while True:
            game = chess.pgn.read_game(f)
            if game is None:
                break
            writer.add(pgn_parser.parse(game))

    print(f"Done. Games processed: {writer.total:,}")
    print(f"Saved to: {output_path}")
