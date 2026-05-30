"""CLI entrypoints for ``lichess-data``."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import chess.pgn
from compression import zstd

from libs.shared import get_artifact_path, load_config

from lichess_data.extract import lichess_downloader as ld
from lichess_data.extract.parquet_stream_writer import ParquetStreamWriter
from lichess_data.extract.pgn_parser import PGNParser
from lichess_data.preprocessing.pipeline import run_pipeline
from lichess_data.validate import (
    validate_checksum_result,
    validate_ge_features_parquet,
    validate_ge_preprocessed_dir,
    validate_ge_processed_parquet,
)


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
        help="Transform raw Parquet into model-ready features (no split)",
    )
    pp_path = pp.add_mutually_exclusive_group(required=False)
    pp_path.add_argument("path", nargs="?", type=Path, help="Path to raw .parquet file")
    pp_path.add_argument("--month", metavar="YYYY-MM", help="Shard month to preprocess")
    pp_path.add_argument(
        "--previous-month",
        action="store_true",
        help="Preprocess the prior calendar month",
    )
    pp.add_argument(
        "--save-dir",
        type=Path,
        default=None,
        help="Directory for features.parquet (optional)",
    )

    ex = sub.add_parser(
        "extract",
        help="Convert a monthly .pgn.zst shard to Parquet",
    )
    ex_path = ex.add_mutually_exclusive_group(required=False)
    ex_path.add_argument("input", nargs="?", type=Path, help="Path to input .pgn.zst file")
    ex_path.add_argument("--month", metavar="YYYY-MM", help="Shard month to extract")
    ex_path.add_argument(
        "--previous-month",
        action="store_true",
        help="Extract the prior calendar month",
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

    val = sub.add_parser("validate", help="Validate a downloaded shard checksum")
    val_g = val.add_mutually_exclusive_group(required=True)
    val_g.add_argument("--month", metavar="YYYY-MM", help="Shard month to validate")
    val_g.add_argument(
        "--previous-month",
        action="store_true",
        help="Validate the prior calendar month",
    )
    val.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero when validation fails",
    )

    vge = sub.add_parser("validate-ge", help="Run Great Expectations validation")
    vge_g = vge.add_mutually_exclusive_group(required=False)
    vge_g.add_argument("--month", metavar="YYYY-MM", help="Shard month to validate")
    vge_g.add_argument(
        "--previous-month",
        action="store_true",
        help="Validate the prior calendar month",
    )
    vge.add_argument(
        "--stage",
        choices=("processed", "features", "preprocessed", "all"),
        default="all",
        help="Which artifact stage to validate (default: all)",
    )
    vge.add_argument(
        "--input",
        type=Path,
        default=None,
        help="Processed parquet path (overrides --month for processed stage)",
    )
    vge.add_argument(
        "--input-dir",
        type=Path,
        default=None,
        help="Preprocessed directory (overrides --month for features/preprocessed)",
    )
    vge.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero when validation fails",
    )

    args = parser.parse_args(argv)
    cfg = load_config("lichess_data")

    if args.cmd == "download":
        return _cmd_download(args, cfg)

    if args.cmd == "preprocess":
        return _cmd_preprocess(args, cfg)

    if args.cmd == "extract":
        return _cmd_extract(args, cfg)

    if args.cmd == "validate":
        return _cmd_validate(args, cfg)

    if args.cmd == "validate-ge":
        return _cmd_validate_ge(args, cfg)

    return 2


def _resolve_month(args: argparse.Namespace) -> str | None:
    if getattr(args, "month", None):
        return args.month
    if getattr(args, "previous_month", False):
        return ld.resolve_previous_month()
    return None


def _resolve_raw_shard_path(month: str, cfg: dict) -> Path:
    dl_cfg = cfg.get("download") or {}
    subpath = dl_cfg.get("output_subpath", "raw/pgn")
    base = get_artifact_path("lichess_data", subpath, create=False)
    return base / ld.shard_filename(month)


def _resolve_processed_path(month: str, cfg: dict) -> Path:
    extract_cfg = cfg.get("extract") or {}
    subpath = extract_cfg.get("output_subpath", "processed")
    base = get_artifact_path("lichess_data", subpath, create=False)
    return base / f"{month}.parquet"


def _resolve_preprocessed_dir(month: str, cfg: dict, *, create: bool = False) -> Path:
    pp_cfg = cfg.get("preprocessing") or {}
    subpath = pp_cfg.get("output_subpath", "preprocessed")
    return get_artifact_path("lichess_data", f"{subpath}/{month}", create=create)


def _cmd_preprocess(args: argparse.Namespace, cfg: dict) -> int:
    month = _resolve_month(args)
    if month is not None:
        path = _resolve_processed_path(month, cfg)
        save_dir = args.save_dir or _resolve_preprocessed_dir(month, cfg, create=True)
    elif args.path is not None:
        path = args.path
        save_dir = args.save_dir
    else:
        print(
            "error: specify path, --month YYYY-MM, or --previous-month",
            flush=True,
        )
        return 2

    if not Path(path).is_file():
        print(f"error: input file not found: {path}", flush=True)
        return 2

    run_pipeline(path=path, save_dir=save_dir)
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

    month = _resolve_month(args)
    if month is not None:
        input_path = _resolve_raw_shard_path(month, cfg)
        output_path = args.output or _resolve_processed_path(month, cfg)
    elif args.input is not None:
        input_path = args.input.resolve()
        output_path = (
            args.output.resolve()
            if args.output is not None
            else _resolve_extract_output(input_path, cfg)
        )
    else:
        print(
            "error: specify input path, --month YYYY-MM, or --previous-month",
            flush=True,
        )
        return 2

    if not input_path.is_file():
        print(f"error: input file not found: {input_path}", flush=True)
        return 2

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    run_extraction(str(input_path), str(output_path), batch_size)
    print(output_path)
    return 0


def _cmd_validate(args: argparse.Namespace, cfg: dict) -> int:
    month = _resolve_month(args)
    if month is None:
        print("error: specify --month YYYY-MM or --previous-month", flush=True)
        return 2

    shard_path = _resolve_raw_shard_path(month, cfg)
    result = validate_checksum_result(shard_path, config=cfg)
    if result.ok:
        print(f"checksum OK: {shard_path}", flush=True)
        return 0

    print(f"checksum FAILED: {shard_path} ({result.reason})", flush=True)
    return 1 if args.strict else 0


def _cmd_validate_ge(args: argparse.Namespace, cfg: dict) -> int:
    month = _resolve_month(args)
    failures: list[str] = []

    if args.stage in ("processed", "all"):
        processed_path = args.input or (
            _resolve_processed_path(month, cfg) if month else None
        )
        if processed_path is None:
            print("error: processed stage requires --month or --input", flush=True)
            return 2
        result = validate_ge_processed_parquet(processed_path, config=cfg)
        if result.ok:
            print(f"GE processed OK: {processed_path}", flush=True)
        else:
            failures.append(f"processed: {processed_path}")
            print(f"GE processed FAILED: {processed_path}", flush=True)

    if args.stage in ("features", "all"):
        features_dir = args.input_dir or (
            _resolve_preprocessed_dir(month, cfg) if month else None
        )
        if features_dir is None:
            print("error: features stage requires --month or --input-dir", flush=True)
            return 2
        features_path = Path(features_dir) / "features.parquet"
        result = validate_ge_features_parquet(features_path, config=cfg)
        if result.ok:
            print(f"GE features OK: {features_path}", flush=True)
        else:
            failures.append(f"features: {features_path}")
            print(f"GE features FAILED: {features_path}", flush=True)

    if args.stage in ("preprocessed", "all"):
        preprocessed_dir = args.input_dir or (
            _resolve_preprocessed_dir(month, cfg) if month else None
        )
        if preprocessed_dir is None:
            print("error: preprocessed stage requires --month or --input-dir", flush=True)
            return 2
        result = validate_ge_preprocessed_dir(preprocessed_dir, config=cfg)
        if result.ok:
            print(f"GE preprocessed OK: {preprocessed_dir}", flush=True)
        else:
            failures.append(f"preprocessed: {preprocessed_dir}")
            print(f"GE preprocessed FAILED: {preprocessed_dir}", flush=True)

    if failures and args.strict:
        return 1
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
