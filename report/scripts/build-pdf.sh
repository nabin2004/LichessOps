#!/usr/bin/env bash
# Build a LaTeX root file to build/<name>.pdf (pdflatex + biber).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MAIN="${1:-main.tex}"
BASE="${MAIN%.tex}"

cd "$ROOT"
./scripts/gen-body-wordcount.sh "$MAIN"

pdflatex -interaction=nonstopmode -output-directory=build "$MAIN" >/dev/null
biber "build/$BASE"
pdflatex -interaction=nonstopmode -output-directory=build "$MAIN" >/dev/null
pdflatex -interaction=nonstopmode -output-directory=build "$MAIN" >/dev/null

echo "Built build/${BASE}.pdf"
