#!/usr/bin/env bash
# Re-encode diagram PDFs so pdfLaTeX can embed them (avoids
# "TeX capacity exceeded [PDF object stream buffer=5000000]" on dense Draw.io exports).
# Requires: ghostscript (gs). Run from report/:  bash scripts/normalize-figure-pdfs.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/figures"

for f in arch_diag.pdf dataIngestion.pdf dataValidation.pdf deployment.pdf \
         etl.pdf modelTrainer.pdf monitoringObserv.pdf; do
  if [[ -f "$f" ]]; then
    echo "Normalizing $f ..."
    tmp="$(mktemp)"
    gs -q -sDEVICE=pdfwrite -dCompatibilityLevel=1.5 -dNOPAUSE -dBATCH \
      -sOutputFile="$tmp" -f "$f"
    mv "$tmp" "$f"
  fi
done
echo "Done. Re-run: make pdf"
