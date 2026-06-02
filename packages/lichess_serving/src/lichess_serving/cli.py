"""CLI entrypoint for the serving package."""

from __future__ import annotations

import argparse

import uvicorn

from lichess_libs.shared import load_config


def main(argv: list[str] | None = None) -> int:
    cfg = load_config("lichess_serving")
    server_cfg = cfg.get("server") or {}

    parser = argparse.ArgumentParser(prog="lichess-serving")
    parser.add_argument(
        "--host",
        default=server_cfg.get("host", "0.0.0.0"),
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(server_cfg.get("port", 8080)),
    )
    args = parser.parse_args(argv)

    uvicorn.run(
        "lichess_serving.app:app",
        host=args.host,
        port=args.port,
        reload=False,
    )
    return 0
