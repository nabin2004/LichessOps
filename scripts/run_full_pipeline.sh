#!/usr/bin/env bash
# Run the full MLOps stack end-to-end: ingestion, serving, monitoring, portal.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

exec uv run python scripts/run_pipeline.py --full --month 2013-01 "$@"
