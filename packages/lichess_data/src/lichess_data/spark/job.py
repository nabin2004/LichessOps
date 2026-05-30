"""Spark driver script for cluster execution."""

from __future__ import annotations

import argparse
from pathlib import Path

from libs.shared import load_config

from lichess_data.spark.submit import submit_transform


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", required=True)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--local", action="store_true")
    args = parser.parse_args()

    cfg = load_config("lichess_data")
    submit_transform(
        args.month,
        input_path=args.input,
        local=args.local or True,
        config_path=Path("packages/lichess_data/configs/default.yaml"),
    )
    _ = cfg
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
