#!/usr/bin/env bash
# Count words in the report main body only (see %TC:ignore regions in the TeX root).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MAIN="${1:-main.tex}"
OUT="$ROOT/build/bodywordcount.tex"

mkdir -p "$ROOT/build"
COUNT="$(texcount -1 -sum -merge "$ROOT/$MAIN")"

cat > "$OUT" <<EOF
\\providecommand{\\BodyWordCount}{${COUNT}}
EOF

echo "Main body word count (${MAIN}): ${COUNT}"
