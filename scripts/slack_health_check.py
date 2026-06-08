#!/usr/bin/env python3
"""Probe Lichess service health endpoints and alert Slack on failure."""

from __future__ import annotations

import argparse
import os
import sys
import urllib.error
import urllib.request

from lichess_libs.shared.slack import is_slack_configured, send_slack_alert

DEFAULT_CHECKS: dict[str, str] = {
    "minio": os.getenv("MINIO_HEALTH_URL", "http://localhost:9000/minio/health/live"),
    "mlflow": os.getenv("MLFLOW_HEALTH_URL", "http://localhost:5000/health"),
    "serving": os.getenv("SERVING_HEALTH_URL", "http://localhost:8082/health"),
}


def _probe(name: str, url: str, timeout: float) -> str | None:
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            if 200 <= response.status < 300:
                return None
            return f"{name} unhealthy: HTTP {response.status} from {url}"
    except urllib.error.URLError as exc:
        return f"{name} unreachable: {exc.reason} ({url})"


def run_checks(checks: dict[str, str], timeout: float) -> list[str]:
    failures: list[str] = []
    for name, url in checks.items():
        error = _probe(name, url, timeout)
        if error:
            failures.append(error)
    return failures


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help="HTTP timeout in seconds for each probe (default: 5)",
    )
    parser.add_argument(
        "--no-alert",
        action="store_true",
        help="Print failures only; do not send Slack messages",
    )
    args = parser.parse_args(argv)

    failures = run_checks(DEFAULT_CHECKS, args.timeout)
    if not failures:
        print("All health checks passed.")
        return 0

    for failure in failures:
        print(failure, file=sys.stderr)

    if not args.no_alert:
        if not is_slack_configured():
            print("Slack is not configured; skipping alerts.", file=sys.stderr)
        else:
            for failure in failures:
                send_slack_alert("health_check", failure)

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
