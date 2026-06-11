"""Measure sub-annual coding divergence in the SCB source corpus (#271).

The before/after instrument for the interval-native co-delivery resolver
(DESIGN.md → Interval-native co-delivery resolver): counts the
`(register_variant, var, column, year)` groups where editions of the same year
carry DIFFERENT value-code key sets depending on their sub-annual window —
the population the year-bucketed resolver collapses to a single winner and an
interval-native one keeps as non-overlapping term states.

Methodology (deliberately reuses the resolver's own helpers so the
classification can never drift from build behavior):

- Every `Registerinformation.csv` row is an edition observation. Its window
  class comes from `_edition_bounds(registerversionnamn, extract_year(...))`:
  `VT` (Jan–Jun), `HT` (Jul–Dec), `PART` (any other strict sub-year window —
  quarters/halves), `FY` (full-year — bare years, dated annuals, and the
  deliberately-unparsed sub-annual forms: months, seasons, läsår).
- Group key is `(RegVarID, VarId, ascii-folded Kolumnnamn, year)` — the
  resolver's per-column contested-year unit.
- Per class, the code-key set is the union of `Värdekod` over the class's
  CVIDs in `Vardemangder.csv`, with the build's sentinel rows (`Tal`,
  `Beskrivande text`) dropped. No year-projection is applied: the comparison
  targets what the editions DELIVERED, pre-projection.
- A group counts as divergent when two classes both delivered codes and the
  key sets differ; `symdiff <= _COSMETIC_MAX_SYM` mirrors the resolver's
  cosmetic-drift threshold (the label-relabel refinement is resolver-only and
  not reproduced here).

Reference baselines. The 2026-06-09 #271 investigation (script not retained)
measured ~257 VT-vs-HT divergent groups, ~282 term-vs-full-year, ~194
above-cosmetic group-years. This script, on the same corpus (2026-06-11):
488,972 (variant,var,col,year) groups, 6,406 with >=2 window classes;
VT-vs-HT 239 divergent (94 cosmetic / 145 substantive across 51 events);
term-vs-full-year 244 (102 / 142 across 43 events) — same populations, same
dominant registers (Komvux, sfi, SSV/CFL); the deltas trace to this script
comparing raw delivered code keys where the spike read built-DB value sets.
Before/after gates for the #271 PRs run THIS script on both sides, so
internal consistency is what matters, not calibration to the lost spike.

Usage:
    uv run python scripts/measure_subannual_codings.py [INPUT_DIR] [--json OUT]

INPUT_DIR defaults to the real seed at reg_meta_build/input_data/ (main
checkout only — pass the absolute path when running from a worktree).
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

from reg_meta.queries import extract_year
from reg_meta_build.db import _VARDEMANGDER_SENTINELS, _open_scb_csv
from reg_meta_build.sources.scb import (
    _COSMETIC_MAX_SYM,
    _ascii_fold_lower,
    _edition_bounds,
)

# Group key: (RegVarID, VarId, folded column, year).
GroupKey = tuple[int, int, str, int]


def _window_class(versionname: str, year: int) -> str:
    lo, hi = _edition_bounds(versionname, year) or ("", "")
    ystr = f"{year:04d}"
    if (lo, hi) == (f"{ystr}-01-01", f"{ystr}-06-30"):
        return "VT"
    if (lo, hi) == (f"{ystr}-07-01", f"{ystr}-12-31"):
        return "HT"
    if (lo, hi) == (f"{ystr}-01-01", f"{ystr}-12-31"):
        return "FY"
    return "PART"


def _collect_editions(
    reginfo: Path,
) -> tuple[dict[GroupKey, dict[str, set[int]]], dict[int, str]]:
    """Pass 1: group every edition row by (variant, var, column, year) and
    window class. Returns (groups → class → cvids, register-variant labels)."""
    groups: dict[GroupKey, dict[str, set[int]]] = defaultdict(lambda: defaultdict(set))
    variant_label: dict[int, str] = {}
    with _open_scb_csv(reginfo) as (_, rows):
        for _, row in rows:
            versionname = row["Registerversionnamn"] or ""
            year = extract_year(versionname)
            if year is None:
                continue  # yearless editions have no per-year placement
            rvid = int(row["RegVarID"])
            key: GroupKey = (
                rvid,
                int(row["VarId"]),
                _ascii_fold_lower(row["Kolumnnamn"]),
                year,
            )
            groups[key][_window_class(versionname, year)].add(int(row["CVID"]))
            variant_label.setdefault(
                rvid, f"{row['Registernamn']} / {row['Registervariantnamn']}"
            )
    return groups, variant_label


def _collect_codes(vardemangder: Path, needed: set[int]) -> dict[int, frozenset[str]]:
    """Pass 2: per needed CVID, the delivered code-key set (sentinels dropped)."""
    codes: dict[int, set[str]] = defaultdict(set)
    with _open_scb_csv(vardemangder) as (_, rows):
        for _, row in rows:
            cvid = int(row["CVID"])
            if cvid not in needed:
                continue
            kod = row["Värdekod"]
            if not kod or (
                kod == row["Värdemängdsversion"] and kod in _VARDEMANGDER_SENTINELS
            ):
                continue
            codes[cvid].add(kod)
    return {cvid: frozenset(kods) for cvid, kods in codes.items()}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Measure sub-annual coding divergence in the SCB source corpus (#271)."
    )
    parser.add_argument(
        "input_dir",
        nargs="?",
        default="reg_meta_build/input_data",
        help="Directory containing SCB/ (default: %(default)s)",
    )
    parser.add_argument("--json", type=Path, help="Also write the report as JSON.")
    args = parser.parse_args()

    scb_dir = Path(args.input_dir) / "SCB"
    if not scb_dir.is_dir():
        sys.exit(f"no such directory: {scb_dir} (pass the seed's absolute path)")

    print("pass 1: classifying editions from Registerinformation.csv ...")
    groups, variant_label = _collect_editions(scb_dir / "Registerinformation.csv")

    # Only groups with >= 2 window classes can diverge; only their cvids are
    # needed for the (heavy) Vardemangder pass.
    contested = {k: v for k, v in groups.items() if len(v) >= 2}
    needed = {
        cvid for v in contested.values() for cvids in v.values() for cvid in cvids
    }
    print(
        f"  {len(groups):,} (variant,var,col,year) groups; "
        f"{len(contested):,} carry >=2 window classes ({len(needed):,} cvids)"
    )

    print("pass 2: streaming code-key sets from Vardemangder.csv ...")
    codes = _collect_codes(scb_dir / "Vardemangder.csv", needed)

    def class_codes(cvids: set[int]) -> frozenset[str]:
        out: set[str] = set()
        for cvid in cvids:
            out |= codes.get(cvid, frozenset())
        return frozenset(out)

    vt_ht: list[tuple[GroupKey, int]] = []  # (key, symdiff)
    term_fy: list[tuple[GroupKey, int]] = []
    for key, by_class in sorted(contested.items()):
        sets = {cls: class_codes(cvids) for cls, cvids in by_class.items()}
        vt, ht, fy = sets.get("VT"), sets.get("HT"), sets.get("FY")
        if vt and ht and vt != ht:
            vt_ht.append((key, len(vt ^ ht)))
        for term in (vt, ht):
            if term and fy and term != fy:
                term_fy.append((key, len(term ^ fy)))
                break  # count the group once

    def report(name: str, rows: list[tuple[GroupKey, int]]) -> dict:
        substantive = [(k, d) for k, d in rows if d > _COSMETIC_MAX_SYM]
        events = sorted({k[:3] for k, _ in substantive})
        print(f"\n{name}: {len(rows)} divergent (variant,var,col,year) groups")
        print(
            f"  cosmetic (symdiff <= {_COSMETIC_MAX_SYM}): "
            f"{len(rows) - len(substantive)}; substantive: {len(substantive)} "
            f"across {len(events)} distinct (variant,var,col) coding events"
        )
        for key, sym in sorted(substantive, key=lambda t: -t[1])[:15]:
            rvid, var_id, col, year = key
            print(
                f"    symdiff={sym:>3}  {variant_label.get(rvid, rvid)}  "
                f"var={var_id} col={col} year={year}"
            )
        return {
            "divergent_groups": len(rows),
            "substantive_groups": len(substantive),
            "substantive_events": len(events),
            "substantive": [
                {"variant": k[0], "var": k[1], "col": k[2], "year": k[3], "symdiff": d}
                for k, d in sorted(substantive)
            ],
        }

    out = {
        "vt_vs_ht": report("VT vs HT", vt_ht),
        "term_vs_full_year": report("term vs full-year", term_fy),
    }
    if args.json:
        args.json.write_text(json.dumps(out, indent=2, sort_keys=True))
        print(f"\nwrote {args.json}")


if __name__ == "__main__":
    main()
