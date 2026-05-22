#!/usr/bin/env bash
# Count words in the report main body only (see %TC:ignore regions in main.tex).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$ROOT/build/bodywordcount.tex"

mkdir -p "$ROOT/build"
COUNT="$(texcount -1 -sum -merge "$ROOT/main.tex")"

cat > "$OUT" <<EOF
\\providecommand{\\BodyWordCount}{${COUNT}}
EOF
