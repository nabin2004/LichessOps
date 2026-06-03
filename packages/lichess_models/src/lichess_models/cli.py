"""CLI entrypoints for ``lichess-models``."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from lichess_data.extract import lichess_downloader as ld

from lichess_models.analyze import run_analyze
from lichess_models.evaluate import run_evaluate
from lichess_models.register import log_training_run, run_register, start_training_run
from lichess_models.train import run_train


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="lichess-models",
        description="Train and evaluate Lichess game outcome prediction models",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    train_p = sub.add_parser("train", help="Train outcome prediction model")
    _add_month_arg(train_p)
    train_p.add_argument("--run-id", default=None, help="Optional run directory name")
    train_p.add_argument(
        "--cv",
        action="store_true",
        help="Enable cross-validation and hyperparameter search (slow on large data)",
    )
    train_p.add_argument(
        "--no-mlflow",
        action="store_true",
        help="Skip MLflow logging and registration",
    )

    eval_p = sub.add_parser("evaluate", help="Evaluate a trained model on the test split")
    _add_month_arg(eval_p)
    eval_p.add_argument("--run-dir", required=True, help="Path to training run directory")

    analyze_p = sub.add_parser("analyze", help="Generate opening weakness report")
    _add_month_arg(analyze_p)
    analyze_p.add_argument("--run-dir", required=True, help="Path to training run directory")

    reg_p = sub.add_parser("register", help="Register an existing run with MLflow")
    _add_month_arg(reg_p)
    reg_p.add_argument("--run-dir", required=True, help="Path to training run directory")

    args = parser.parse_args(argv)

    if args.cmd == "train":
        return _cmd_train(args)
    if args.cmd == "evaluate":
        return _cmd_evaluate(args)
    if args.cmd == "analyze":
        return _cmd_analyze(args)
    if args.cmd == "register":
        return _cmd_register(args)
    return 2


def _add_month_arg(parser: argparse.ArgumentParser) -> None:
    month_g = parser.add_mutually_exclusive_group(required=True)
    month_g.add_argument("--month", metavar="YYYY-MM", help="Shard month to train on")
    month_g.add_argument(
        "--previous-month",
        action="store_true",
        help="Use the prior calendar month",
    )


def _resolve_month(args: argparse.Namespace) -> str:
    return ld.resolve_previous_month() if args.previous_month else args.month


def _cmd_train(args: argparse.Namespace) -> int:
    month = _resolve_month(args)
    use_cv = True if args.cv else None
    result = run_train(month, run_id=args.run_id, use_cv=use_cv)
    eval_result = run_evaluate(month, result.run_dir)
    run_analyze(month, result.run_dir)

    print(result.run_dir)
    print(json.dumps(eval_result.metrics, indent=2))

    if not args.no_mlflow:
        try:
            with start_training_run(month):
                metadata = json.loads((result.run_dir / "train_metadata.json").read_text())
                log_training_run(
                    result.run_dir,
                    month,
                    train_metadata=metadata,
                    metrics=eval_result.metrics,
                    register=True,
                )
        except Exception as exc:
            print(f"MLflow logging skipped: {exc}")

    return 0


def _cmd_evaluate(args: argparse.Namespace) -> int:
    month = _resolve_month(args)
    run_dir = Path(args.run_dir)
    result = run_evaluate(month, run_dir)
    print(json.dumps(result.metrics, indent=2))
    return 0


def _cmd_analyze(args: argparse.Namespace) -> int:
    month = _resolve_month(args)
    run_dir = Path(args.run_dir)
    path = run_analyze(month, run_dir)
    print(path)
    return 0


def _cmd_register(args: argparse.Namespace) -> int:
    month = _resolve_month(args)
    run_id = run_register(Path(args.run_dir), month)
    print(run_id)
    return 0
