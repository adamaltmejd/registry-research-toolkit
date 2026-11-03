#!/usr/bin/env python3
"""holdings_scope_check — a holdings DB carries only what the catalog serves.

    uv run --no-project scripts/holdings_scope_check.py <reg_meta_swecov.db> <base reg_meta.db> <toolkit checkout>

The SWECOV catalog is public (maintainer 2026-11-03), so the paired holdings asset has no
disclosure review. What a release must not carry is background data: steward-repo notes,
raw census or storage exports, inventory working files. This check enforces that
mechanically. Every table, column and meta key of the holdings DB must be

- served: a word in crates/reg-catalog/src or crates/reg-meta/src, an FTS shadow table of
  a served table, or part of the base catalog's schema (the base release is judged on its
  own); or
- designated: listed below, the holdings physical topology the order materializer reads
  (crates/DESIGN.md, Holdings, "MONA order file") and the provenance meta keys.

Holdings-specific text values must also stay under a size bound; a raw payload is larger.
Both DBs are opened read-only. Output is one summary line and the failing items' names,
never row contents. Exit 1 on any failure.
"""

from __future__ import annotations

import re
import sqlite3
import sys
from pathlib import Path

# Tables the order materializer reads, not yet read by crates/ (it is built later).
DESIGNATED_TABLES = {
    "holding_period",  # the periods a physical table covers: row-filter ranges
    "holding_route",  # physical table -> catalog table: which tables an order may select
    "holding_notice",  # coverage and delivery findings the order carries as `findings`
}
DESIGNATED_COLUMNS = {
    # file naming, schema and partitioning of a physical table
    "holding_table.file_name",  # order file name (physical-table filenames)
    "holding_table.schema_name",  # exact physical schema in the order file
    "holding_table.table_type",  # table vs view, for the runner's query
    "holding_table.partition",  # partition selector of a split table
    "holding_table.projection_of",  # source table of a projection
    "holding_table.projection_token",  # projection selector
    "holding_table.period_column_id",  # column carrying the period
    "holding_table.period_encoding",  # how that column encodes the period (row_filter)
    # per-column storage metadata and steward slot topology
    "holding_column.storage_type",  # expected storage metadata
    "holding_column.precision",  # expected storage metadata
    "holding_column.scale",  # expected storage metadata
    "holding_column.slot_family",  # steward slot topology
    "holding_column.slot_kind",  # steward slot topology
    "holding_column.slot_index",  # steward slot topology
    "holding_column.follow_up",  # steward follow-up note shown on the column
}
DESIGNATED_META = {
    "input_pins",  # revision and digest of every pinned input
    "steward_repo",  # provenance: steward repository
    "steward_commit",  # provenance: steward commit
    "compiler_digest",  # provenance: compiler build
    "semantic_generation",  # provenance: semantic generation
    "semantic_sha256",  # provenance: semantic digest
    "census_tables",  # aggregate count
    "census_columns",  # aggregate count
    "mapped_columns",  # aggregate count
    "unmatched_columns",  # aggregate count
    "storage_extra_columns",  # aggregate count
}

# Size bounds in bytes, from the 0.71.0 preview DB's maxima (text columns 630, other meta
# values 1755 for `logo`) with margin. A raw export is larger than any of these.
TEXT_BOUND = 2048  # holdings-specific text columns
META_BOUND = 4096  # holdings-specific meta values
META_BOUNDS = {
    "input_pins": 400_000,  # one entry per pinned input file (preview 291 474)
    "scope": 120_000,  # the steward's public scope document (preview 81 974)
}

SHADOW_SUFFIXES = ("_data", "_idx", "_docsize", "_config", "_content")
SERVED_SRC = ("crates/reg-catalog/src", "crates/reg-meta/src")


def connect_ro(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)


def tables_of(con: sqlite3.Connection) -> dict[str, list[tuple[str, str]]]:
    names = [
        r[0]
        for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    ]
    return {
        t: [(c[1], c[2].upper()) for c in con.execute(f'PRAGMA table_info("{t}")')]
        for t in names
    }


def main() -> int:
    if len(sys.argv) != 4:
        print(__doc__, file=sys.stderr)
        return 2
    holdings, base, checkout = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
    text = "\n".join(
        p.read_text() for d in SERVED_SRC for p in (checkout / d).rglob("*.rs")
    )
    words = set(re.findall(r"[A-Za-z_][A-Za-z0-9_]*", text))
    hcon, bcon = connect_ro(holdings), connect_ro(base)
    h_schema, b_schema = tables_of(hcon), tables_of(bcon)
    b_meta = {r[0] for r in bcon.execute("SELECT key FROM meta")}
    h_meta = {
        r[0]: r[1]
        for r in hcon.execute("SELECT key, length(CAST(value AS BLOB)) FROM meta")
    }

    def shadow_of_served(t: str) -> bool:
        stem = next((t[: -len(s)] for s in SHADOW_SUFFIXES if t.endswith(s)), None)
        return stem in words and stem in h_schema

    bad_t, bad_c, bad_m, too_big = [], [], [], []
    for t, cols in h_schema.items():
        if shadow_of_served(t):
            continue
        if t not in b_schema and t not in words and t not in DESIGNATED_TABLES:
            bad_t.append(t)
            continue
        inherited = {c for c, _ in b_schema.get(t, [])}
        for c, ty in cols:
            if c in inherited:
                continue
            if c not in words and f"{t}.{c}" not in DESIGNATED_COLUMNS:
                bad_c.append(f"{t}.{c}")
            if ty == "TEXT":
                n = (
                    hcon.execute(
                        f'SELECT max(length(CAST("{c}" AS BLOB))) FROM "{t}"'
                    ).fetchone()[0]
                    or 0
                )
                if n > TEXT_BOUND:
                    too_big.append(f"{t}.{c} ({n} > {TEXT_BOUND} bytes)")
    for k, n in h_meta.items():
        if (
            k not in b_meta
            and k not in words
            and f'"{k}"' not in text
            and k not in DESIGNATED_META
        ):
            bad_m.append(k)
        bound = META_BOUNDS.get(k, META_BOUND)
        if k not in b_meta and n > bound:
            too_big.append(f"meta.{k} ({n} > {bound} bytes)")

    items = [
        ("unserved table", bad_t),
        ("unserved column", bad_c),
        ("unserved meta key", bad_m),
        ("oversize value", too_big),
    ]
    failing = sum(len(v) for _, v in items)
    n_cols = sum(len(c) for c in h_schema.values())
    print(
        f"scope check {'FAIL' if failing else 'ok'}: tables={len(h_schema)} columns={n_cols} "
        f"meta_keys={len(h_meta)} unserved_tables={len(bad_t)} unserved_columns={len(bad_c)} "
        f"unserved_meta_keys={len(bad_m)} oversize_values={len(too_big)}"
    )
    for label, names in items:
        for name in sorted(names):
            print(f"  {label}: {name}")
    return 1 if failing else 0


if __name__ == "__main__":
    sys.exit(main())
