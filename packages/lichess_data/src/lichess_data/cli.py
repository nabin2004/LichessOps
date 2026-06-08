"""CLI entrypoints for ``lichess-data``."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import chess.pgn
import zstandard as zstd

from lichess_libs.shared import get_artifact_path, load_config

from lichess_data.extract import lichess_downloader as ld
from lichess_data.extract.parquet_stream_writer import ParquetStreamWriter
from lichess_data.extract.pgn_parser import PGNParser
from lichess_data.load.columnstore_sync import sync_month
from lichess_data.load.upload import upload_raw_shard
from lichess_data.preprocessing import run_pipeline
from lichess_data.spark.run import run_transform
from lichess_data.validate import (
    validate_checksum_file,
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

    ex = sub.add_parser("extract", help="Parse a .pgn.zst shard into Parquet")
    _add_month_args(ex)
    ex.add_argument(
        "--input",
        type=Path,
        default=None,
        help="Input .pgn.zst path (default: resolved from --month)",
    )
    ex.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output Parquet path (default: artifacts/lichess_data/processed/<month>.parquet)",
    )
    ex.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Batch size for Parquet writing (default: config extract.batch_size)",
    )

    pp = sub.add_parser("preprocess", help="Run preprocessing pipeline and split train/test")
    _add_month_args(pp)
    pp.add_argument(
        "--input",
        type=Path,
        default=None,
        help="Input Parquet path (default: artifacts/lichess_data/processed/<month>.parquet)",
    )
    pp.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for train/test Parquet outputs (default: artifacts/lichess_data/preprocessed/<month>/)",
    )
    pp.add_argument(
        "--test-size",
        type=float,
        default=None,
        help="Fraction of data reserved for test (default: config preprocessing.test_size)",
    )

    val = sub.add_parser("validate", help="Validate a downloaded shard checksum")
    _add_month_args(val)
    val.add_argument(
        "--input",
        type=Path,
        default=None,
        help="Input .pgn.zst path (default: resolved from --month)",
    )
    val.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero if checksum validation fails",
    )

    val_ge = sub.add_parser(
        "validate-ge",
        help="Validate processed/preprocessed data with Great Expectations",
    )
    _add_month_args(val_ge)
    val_ge.add_argument(
        "--stage",
        choices=["processed", "preprocessed", "all"],
        default="all",
        help="Which stage to validate",
    )
    val_ge.add_argument(
        "--input",
        type=Path,
        default=None,
        help="Processed parquet path (default: resolved from --month)",
    )
    val_ge.add_argument(
        "--input-dir",
        type=Path,
        default=None,
        help="Preprocessed directory with train/test parquet",
    )
    val_ge.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero if Great Expectations validation fails",
    )

    up = sub.add_parser("upload", help="Upload a downloaded shard to object storage")
    _add_month_args(up)

    sp = sub.add_parser(
        "spark-transform",
        help="Build star schema and wide tables (writes to MinIO by default)",
    )
    _add_month_args(sp)
    sp.add_argument(
        "--local",
        action="store_true",
        help="Write outputs under artifacts/ instead of object storage",
    )

    cs = sub.add_parser(
        "columnstore-sync",
        help="Load processed Parquet into MariaDB ColumnStore and export ML-ready wide parquet",
    )
    _add_month_args(cs)

    args = parser.parse_args(argv)
    cfg = load_config("lichess_data")

    if args.cmd == "download":
        return _cmd_download(args, cfg)
    if args.cmd == "extract":
        return _cmd_extract(args, cfg)
    if args.cmd == "preprocess":
        return _cmd_preprocess(args, cfg)
    if args.cmd == "validate":
        return _cmd_validate(args, cfg)
    if args.cmd == "validate-ge":
        return _cmd_validate_ge(args, cfg)
    if args.cmd == "upload":
        return _cmd_upload(args, cfg)
    if args.cmd == "spark-transform":
        return _cmd_spark_transform(args, cfg)
    if args.cmd == "columnstore-sync":
        return _cmd_columnstore_sync(args, cfg)

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


def _add_month_args(subparser: argparse.ArgumentParser) -> None:
    g = subparser.add_mutually_exclusive_group(required=False)
    g.add_argument("--month", metavar="YYYY-MM", help="Shard month to process")
    g.add_argument(
        "--previous-month",
        action="store_true",
        help="Use the prior calendar month",
    )


def _resolve_month(args: argparse.Namespace) -> str | None:
    if getattr(args, "month", None):
        return args.month
    if getattr(args, "previous_month", False):
        return ld.resolve_previous_month()
    return None


def _parse_month_from_filename(path: Path) -> str | None:
    match = re.search(r"(\d{4}-\d{2})", path.name)
    return match.group(1) if match else None


def _resolve_raw_shard_path(cfg: dict, month: str | None, input_path: Path | None) -> Path:
    if input_path is not None:
        return input_path.expanduser().resolve()
    if month is None:
        raise ValueError("Specify --month/--previous-month or --input")
    dl = cfg.get("download") or {}
    subpath = dl.get("output_subpath", "raw/pgn")
    base = get_artifact_path("lichess_data", subpath, create=True)
    return (base / ld.shard_filename(month)).resolve()


def _resolve_extract_output_path(
    cfg: dict, month: str | None, input_path: Path, output_path: Path | None
) -> Path:
    if output_path is not None:
        return output_path.expanduser().resolve()
    extract_cfg = cfg.get("extract") or {}
    subpath = extract_cfg.get("output_subpath", "processed")
    base = get_artifact_path("lichess_data", subpath, create=True)
    resolved_month = month or _parse_month_from_filename(input_path)
    filename = f"{resolved_month}.parquet" if resolved_month else "extracted.parquet"
    return (base / filename).resolve()


def _resolve_preprocess_output_dir(
    cfg: dict, month: str | None, output_dir: Path | None
) -> Path:
    if output_dir is not None:
        return output_dir.expanduser().resolve()
    preprocess_cfg = cfg.get("preprocessing") or {}
    subpath = preprocess_cfg.get("output_subpath", "preprocessed")
    suffix = f"/{month}" if month else ""
    return get_artifact_path("lichess_data", f"{subpath}{suffix}", create=True).resolve()


def _resolve_processed_path(
    cfg: dict, month: str | None, input_path: Path | None
) -> Path:
    if input_path is not None:
        return input_path.expanduser().resolve()
    if month is None:
        raise ValueError("Specify --month/--previous-month or --input")
    subpath = (cfg.get("extract") or {}).get("output_subpath", "processed")
    base = get_artifact_path("lichess_data", subpath, create=False)
    return (base / f"{month}.parquet").resolve()


def _resolve_preprocessed_dir(
    cfg: dict, month: str | None, input_dir: Path | None
) -> Path:
    if input_dir is not None:
        return input_dir.expanduser().resolve()
    if month is None:
        raise ValueError("Specify --month/--previous-month or --input-dir")
    subpath = (cfg.get("preprocessing") or {}).get("output_subpath", "preprocessed")
    base = get_artifact_path("lichess_data", subpath, create=False)
    return (base / month).resolve()


def _cmd_extract(args: argparse.Namespace, cfg: dict) -> int:
    month = _resolve_month(args)
    input_path = _resolve_raw_shard_path(cfg, month, args.input)
    if not input_path.exists():
        print(f"error: input file not found: {input_path}", flush=True)
        return 2

    batch_size = args.batch_size
    if batch_size is None:
        batch_size = int((cfg.get("extract") or {}).get("batch_size", 5000))

    output_path = _resolve_extract_output_path(cfg, month, input_path, args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _run_extraction(input_path, output_path, batch_size)
    print(output_path)
    return 0


def _cmd_preprocess(args: argparse.Namespace, cfg: dict) -> int:
    month = _resolve_month(args)
    input_path = args.input
    if input_path is None:
        if month is None:
            print("error: specify --month/--previous-month or --input", flush=True)
            return 2
        subpath = (cfg.get("extract") or {}).get("output_subpath", "processed")
        base = get_artifact_path("lichess_data", subpath, create=False)
        input_path = base / f"{month}.parquet"

    input_path = input_path.expanduser().resolve()
    if not input_path.exists():
        print(f"error: input file not found: {input_path}", flush=True)
        return 2

    test_size = args.test_size
    if test_size is None:
        test_size = float((cfg.get("preprocessing") or {}).get("test_size", 0.2))

    output_dir = _resolve_preprocess_output_dir(cfg, month, args.output_dir)
    run_pipeline(input_path, test_size=test_size, save_dir=output_dir)
    print(output_dir)
    return 0


def _cmd_validate(args: argparse.Namespace, cfg: dict) -> int:
    month = _resolve_month(args)
    input_path = _resolve_raw_shard_path(cfg, month, args.input)
    ok = validate_checksum_file(input_path, config=cfg)
    if ok or not args.strict:
        return 0
    return 1


def _cmd_validate_ge(args: argparse.Namespace, cfg: dict) -> int:
    month = _resolve_month(args)
    ok = True

    if args.stage in {"processed", "all"}:
        try:
            input_path = _resolve_processed_path(cfg, month, args.input)
        except ValueError as exc:
            print(f"error: {exc}", flush=True)
            return 2
        result = validate_ge_processed_parquet(input_path, config=cfg)
        status = "OK" if result.ok else "FAIL"
        print(f"GE processed: {status}")
        ok = ok and result.ok

    if args.stage in {"preprocessed", "all"}:
        try:
            input_dir = _resolve_preprocessed_dir(cfg, month, args.input_dir)
        except ValueError as exc:
            print(f"error: {exc}", flush=True)
            return 2
        result = validate_ge_preprocessed_dir(input_dir, config=cfg)
        status = "OK" if result.ok else "FAIL"
        print(f"GE preprocessed: {status}")
        ok = ok and result.ok

    if ok or not args.strict:
        return 0
    return 1


def _cmd_upload(args: argparse.Namespace, cfg: dict) -> int:
    month = _resolve_month(args)
    if month is None:
        print("error: specify --month/--previous-month", flush=True)
        return 2
    uri = upload_raw_shard(month, config=cfg)
    if uri:
        print(uri)
    return 0


def _cmd_spark_transform(args: argparse.Namespace, cfg: dict) -> int:
    month = _resolve_month(args)
    if month is None:
        print("error: specify --month/--previous-month", flush=True)
        return 2
    outputs = run_transform(month, config=cfg, local=bool(args.local))
    for _, out in outputs.items():
        print(out)
    return 0


def _cmd_columnstore_sync(args: argparse.Namespace, cfg: dict) -> int:
    month = _resolve_month(args)
    if month is None:
        print("error: specify --month/--previous-month", flush=True)
        return 2
    out_path = sync_month(month, config=cfg)
    print(out_path)
    return 0


def _run_extraction(input_path: Path, output_path: Path, batch_size: int) -> None:
    parser = PGNParser()

    with (
        zstd.open(input_path, "rt", encoding="utf-8") as f,
        ParquetStreamWriter(str(output_path), batch_size) as writer,
    ):
        while True:
            game = chess.pgn.read_game(f)
            if game is None:
                break
            writer.add(parser.parse(game))

    print(f"Done. Games processed: {writer.total:,}")
    print(f"Saved to: {output_path}")




def run_extraction(input_path: str, output_path: str, batch_size: int) -> None:
    parser = PGNParser()
 
    with (
        zstd.open(input_path, "rt", encoding="utf-8") as f,
        ParquetStreamWriter(output_path, batch_size) as writer,
    ):
        while True:
            game = chess.pgn.read_game(f)
            if game is None:
                break
            writer.add(parser.parse(game))
 
    print(f"Done. Games processed: {writer.total:,}")
    print(f"Saved to: {output_path}")
