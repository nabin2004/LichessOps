def main() -> None:
    raise SystemExit(_cli_exit_code())


def _cli_exit_code() -> int:
    from lichess_features.cli import main as cli_main

    return cli_main()
