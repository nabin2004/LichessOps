# Appendix figures (optional artwork)

This directory holds static images referenced from [`content/app-lichess-open-database.tex`](../content/app-lichess-open-database.tex).

`\graphicspath` in [`preamble.tex`](../preamble.tex) already includes `figures/appendix/`, so `\includegraphics{lichess-logo.pdf}` resolves here.

## Expected files

| Filename | Role | Notes |
|----------|------|--------|
| `lichess-logo.pdf` | Wordmark | Prefer **vector PDF** or **SVG → PDF** from official brand or press assets. Keep background transparent if the report uses white paper. |
| `lichess-database-index.png` **or** `lichess-database-index.pdf` | Screenshot | Crop tightly to the **Standard (rated) chess games** table (or full browser chrome if your style guide demands it). Take a fresh capture when you finalise the submission date. |

If a file is missing, the report still builds: grey framed placeholders appear in the PDF.

## Attribution and rights

- **Screenshot:** cite \texttt{database.lichess.org} in the figure caption (handled in the LaTeX source via `\cite{lichess_open_database}`). You normally do **not** need separate permission for factual screenshots of a public catalogue; follow your institution’s media policy if unsure.
- **Logo:** trademarks belong to their owners; use official artwork, do not distort colours, and keep the caption scholarly (source + retrieval date via bibliography).
- **CC0 data:** the database *contents* are CC0; **site artwork** is not automatically CC0—treat logos/screenshots as distinct from the PGN files.

## Regenerating the monthly table fragment (optional)

[`content/lichess-monthly-inventory.tex`](../content/lichess-monthly-inventory.tex) can be refreshed from the live HTML table with:

```bash
python3 tools/gen_lichess_monthly_inventory.py
```

Run from the `report/` directory when you need a newer snapshot (update the access date in the appendix prose and `urldate` in `references.bib` accordingly).
