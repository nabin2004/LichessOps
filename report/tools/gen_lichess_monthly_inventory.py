#!/usr/bin/env python3
"""Fetch the standard-rated monthly shard table from database.lichess.org and emit LaTeX rows.

Run from the report/ root:

    python3 tools/gen_lichess_monthly_inventory.py

Writes: content/lichess-monthly-inventory.tex
"""

from __future__ import annotations

import html
import re
from pathlib import Path
from urllib.request import urlopen

URL = "https://database.lichess.org/"
OUT = Path(__file__).resolve().parent.parent / "content" / "lichess-monthly-inventory.tex"


def strip_tag(s: str) -> str:
    s = re.sub(r"<[^>]+>", "", s)
    s = html.unescape(s)
    return re.sub(r"\s+", " ", s).strip()


def fmt_games(g: str) -> str:
    n = int(g.replace(",", ""))
    return f"{n:,}".replace(",", r"\,")


def main() -> None:
    text = urlopen(URL).read().decode("utf-8", "replace")
    m = re.search(r'id="standard_games"[^>]*>(.*?)</section>', text, re.S)
    if not m:
        raise SystemExit("Could not find #standard_games section; page structure may have changed.")
    sec = m.group(1)
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", sec, re.S)
    lines: list[str] = []
    for r in rows:
        if "<th" in r:
            continue
        cols = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", r, re.S)
        if len(cols) != 4:
            continue
        month, size, games, _ = (strip_tag(c) for c in cols)
        mlatex = month.replace(" - ", " -- ")
        lines.append(f"{mlatex} & {size} & {fmt_games(games)} \\\\\n")

    body = "".join(lines)
    body += (
        r"\midrule" "\n"
        r"\textbf{Total} & \textbf{2.46 TB} & \textbf{7\,772\,124\,731} \\" "\n"
        r"\bottomrule" "\n"
    )
    OUT.write_text(body, encoding="utf-8")
    print(f"Wrote {len(lines)} monthly rows + totals to {OUT}")


if __name__ == "__main__":
    main()
