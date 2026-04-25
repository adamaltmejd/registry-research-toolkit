#!/usr/bin/env python3
"""Extract per-year LKF (län/kommun/församling) canonical codes from SCB.

SCB publishes one LKF snapshot per year as either lkf{YEAR}.xls (older) or
lkf{YEAR}.xlsx (newer) under
``contentassets/13ec5841d80045498d960d456e87ea78/``. Each file is a flat
table at row 12+:

    Län | Länsnamn | Kommun | Kommunnamn | Församling | Församlingsnamn

Each data row contributes three LKF codes — the 2-digit län, the 4-digit
kommun, and the 6-digit församling — all of which appear together in
register data. The output is one canonical CSV per year, ready to wire
into ``classifications.toml`` as ``valid_codes_file = "lkf{YEAR}.csv"``.

Status of SCB downloads (as of probing in 2026-04):

    1980–2014  no XLS/XLSX found at standard URL — likely paper/archived
    2015       PDF only (lkf2015.pdf), needs separate OCR pipeline
    2016–2019  XLS available
    2020       missing (skipped or different URL pattern)
    2021–2025  XLS available
    2026       XLSX (different subpath: 2025-06-19/lkf2026.xlsx)

This script therefore covers ~10 of the ~47 yearly vardemangdsversion
strings the regmeta DB carries. Years without a published file fall back
to is_valid=NULL until someone tracks down or hand-builds the missing list.

Usage:
    uv run --with openpyxl --with xlrd python scripts/extract_lkf.py \\
        --out regmeta/input_data/classifications/

Add --emit-toml to print seed entries for classifications.toml on stderr.
"""

from __future__ import annotations

import argparse
import csv
import sys
import urllib.request
from pathlib import Path

# Years we know are downloadable (probed empirically).
YEARS_AVAILABLE = list(range(2016, 2020)) + list(range(2021, 2027))

BASE_URL = "https://www.scb.se/contentassets/13ec5841d80045498d960d456e87ea78"
URL_OVERRIDES = {
    2026: f"{BASE_URL}/2025-06-19/lkf2026.xlsx",
}


def url_for(year: int) -> str:
    if year in URL_OVERRIDES:
        return URL_OVERRIDES[year]
    ext = "xlsx" if year >= 2026 else "xls"
    return f"{BASE_URL}/lkf{year}.{ext}"


def download(year: int, dst: Path) -> Path:
    """Download lkf{year} into dst (skipped if already present)."""
    url = url_for(year)
    fname = url.rsplit("/", 1)[-1]
    out = dst / fname
    if out.exists():
        return out
    print(f"  downloading {url}", file=sys.stderr)
    urllib.request.urlretrieve(url, out)
    return out


def extract(year: int, src: Path) -> dict[str, str]:
    """Read one LKF snapshot file, return {vardekod: vardebenamning}.

    SCB has used at least three different layouts over the years (2018–19
    use a 2-column layout, 2021–22 a sparse 5-column layout, 2023+ a
    6-column table). To stay robust, the extractor doesn't try to anchor
    on any specific header — it walks every row and takes the first
    code-like cell (2/4/6-digit numeric string) paired with the next
    non-empty cell as the label. Works across all observed layouts.
    """
    if src.suffix == ".xlsx":
        import openpyxl

        wb = openpyxl.load_workbook(src, read_only=True, data_only=True)
        rows = list(wb[wb.sheetnames[0]].iter_rows(values_only=True))
    else:
        import xlrd

        wb = xlrd.open_workbook(src)
        sh = wb.sheet_by_index(0)
        rows = [tuple(sh.row_values(i)) for i in range(sh.nrows)]

    out: dict[str, str] = {}
    for row in rows:
        i = 0
        while i < len(row):
            cell = row[i]
            if cell is None:
                i += 1
                continue
            code = str(cell).strip()
            if not (code.isdigit() and len(code) in (2, 4, 6)):
                i += 1
                continue
            # Found a code. Take the next non-empty cell as its label, then
            # advance past it. The 2023+ table layout has multiple codes per
            # row; vertical layouts have one but we still walk uniformly.
            label = ""
            label_idx = i + 1
            for j in range(i + 1, len(row)):
                if row[j] is None:
                    continue
                s = str(row[j]).strip()
                if s:
                    label = s
                    label_idx = j
                    break
            if label:
                out.setdefault(code, label)
            i = label_idx + 1
    return out


def write_csv(year: int, codes: dict[str, str], out_dir: Path) -> Path:
    out = out_dir / f"lkf{year}.csv"
    with out.open("w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["vardekod", "vardebenamning"])
        for code, label in sorted(codes.items()):
            w.writerow([code, label])
    return out


def emit_toml_entry(year: int, prev_year: int | None) -> str:
    """Emit a [[classification]] block for LKF{year}."""
    # The vardemangdsversion strings are inconsistent across years — see
    # the existing LKF entry in classifications.toml for the variants.
    # The script prints a starter; reconcile against actual observed
    # strings before committing.
    sup = f'\nsupersedes = "LKF{prev_year}"' if prev_year else ""
    return f"""\
[[classification]]
short_name = "LKF{year}"
name = "Län, kommuner och församlingar {year}"
name_en = "Counties, municipalities and parishes {year}"
publisher = "SCB"
version = "{year}"
valid_from = {year}
valid_to = {year}
url = "https://www.scb.se/hitta-statistik/regional-statistik-och-kartor/regionala-indelningar/lan-och-kommuner/"{sup}
valid_codes_file = "lkf{year}.csv"
vardemangdsversion = [
  # TODO: add the year's vardemangdsversion variants here. Sample patterns:
  #   "LKF {year}-01-01/ Län, kommuner och församlingar"
  #   "LKF {year}-01-01/ Län, kommuner och församlingar "  (trailing space)
  #   "{year}-01-01/ Län, kommuner och församlingar"       (no LKF prefix, 2018+)
  #   "{year}-01-01 /Län, kommuner och församlingar"       (space before /, 2023+)
]
"""


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--out",
        type=Path,
        default=Path("regmeta/input_data/classifications"),
        help="Where to write per-year CSVs.",
    )
    p.add_argument(
        "--cache",
        type=Path,
        default=Path("/tmp/scb_lkf"),
        help="Where to cache downloaded XLS/XLSX files.",
    )
    p.add_argument(
        "--years",
        type=int,
        nargs="*",
        default=YEARS_AVAILABLE,
        help="Specific years to extract (default: all known available).",
    )
    p.add_argument(
        "--emit-toml",
        action="store_true",
        help="Print starter classifications.toml entries on stderr.",
    )
    args = p.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    args.cache.mkdir(parents=True, exist_ok=True)

    prev_year: int | None = None
    for year in sorted(args.years):
        print(f"\n=== LKF {year} ===", file=sys.stderr)
        try:
            src = download(year, args.cache)
            codes = extract(year, src)
            out = write_csv(year, codes, args.out)
            print(f"  wrote {out} ({len(codes)} codes)", file=sys.stderr)
            if args.emit_toml:
                sys.stdout.write(emit_toml_entry(year, prev_year))
                sys.stdout.write("\n")
            prev_year = year
        except Exception as exc:
            print(f"  FAILED: {exc}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
