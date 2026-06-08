#!/usr/bin/env bash
# Run the full MLOps pipeline for January 2013.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

exec uv run python scripts/run_pipeline.py --month 2013-01 "$@"
