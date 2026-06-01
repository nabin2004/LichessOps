#!/usr/bin/env bash
# Count words in Report 2 main body only (see %TC:ignore regions in report2.tex).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$ROOT/build/bodywordcount.tex"

mkdir -p "$ROOT/build"
# Placeholder so texcount does not error on \input{build/bodywordcount.tex}
echo '\providecommand{\BodyWordCount}{0}' > "$OUT"
COUNT="$(texcount -1 -sum -merge "$ROOT/report2.tex" 2>/dev/null | tail -1 | tr -d ' ')"

cat > "$OUT" <<EOF
\\providecommand{\\BodyWordCount}{${COUNT}}
EOF

echo "Report 2 main body word count: ${COUNT}"
