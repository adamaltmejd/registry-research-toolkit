#!/usr/bin/env python3
"""holdings_scope_check — a holdings DB carries only what the builder declares.

    uv run --no-project scripts/holdings_scope_check.py <reg_meta_swecov.db> <toolkit checkout>

The SWECOV catalog is public (maintainer 2026-11-03), so the paired holdings asset has no
disclosure review. What a release must not carry is background data: steward-repo notes,
raw census or storage exports, inventory working files. This check enforces that
mechanically against the builder's own DDL, which the checkout supplies:

- Schema: every table (FTS shadow tables included) and column must equal what executing
  crates/reg-build/src/schema.sql, holdings/relations.sql, holdings/schema.sql and the
  inline holding_reference_occurrence DDL (holdings/references.rs) yields in SQLite. An
  extra or missing table or column fails. holding_relation is optional: the builder
  emits it only when the steward declares external relations.
- Meta: every key must be in META_KEYS below.
- Rows: tables the steward build must leave empty (EMPTY_TABLES) have no rows. The
  semantic part keeps the catalog schema, but its source locators point into steward
  working files.
- Size: every TEXT, BLOB or untyped column of every table stays under its bound, so a raw
  payload in an allowed column is caught.
- Text: no `text` body names a steward working file (`*.json`, `*.jsonl`, `*.toml`) or a
  census row (`census:row=`). The reader serves these bodies as labels, definitions and
  notes, so a provenance suffix in a transcription cell reaches the public site
  (0.72.0: "Evidence: reference-occurrence-evidence.jsonl; census:row=489; ...").

The DB is opened read-only. Output is one summary line and the failing items' names,
never row contents. Exit 1 on any failure.
"""

from __future__ import annotations

import re
import sqlite3
import sys
from pathlib import Path

BUILD_SRC = "crates/reg-build/src"
OPTIONAL_TABLES = {"holding_relation"}

# Every meta key a holdings DB carries: the base catalog's keys plus the steward's.
META_KEYS = {
    "schema_version",  # base: schema version
    "builder_version",  # base: builder provenance
    "catalog_version",  # base: release label
    "generation_id",  # base: content digest
    "curation_digest",  # base: curation digest
    "sqlite_version",  # base: SQLite that built the file
    "period_grammar",  # base: period grammar the reader parses
    "delivery_name_grammar",  # base: delivery-name grammar
    "open_start",  # base: open-interval sentinel
    "open_end",  # base: open-interval sentinel
    "semantic_schema_version",  # holdings: schema of the semantic build
    "semantic_generation",  # holdings: provenance of the semantic build
    "semantic_sha256",  # holdings: digest of the semantic build
    "catalog_generation",  # holdings: the paired base catalog
    "catalog_sha256",  # holdings: the paired base catalog
    "owner_kind",  # holdings: owner of the file
    "owner_id",  # holdings: owner of the file
    "steward_repo",  # holdings: provenance, steward repository
    "steward_commit",  # holdings: provenance, steward commit
    "steward_config",  # holdings: provenance, steward config path
    "compiler_digest",  # holdings: provenance, compiler build
    "input_manifest_sha256",  # holdings: provenance, digest of the pinned input list
    "steward",  # holdings: steward presentation, shown in the SPA
    "scope",  # holdings: the steward's public scope document
    "logo",  # holdings: steward logo, shown in the SPA
    "census_tables",  # holdings: aggregate count
    "census_columns",  # holdings: aggregate count
    "mapped_columns",  # holdings: aggregate count
    "unmatched_columns",  # holdings: aggregate count
    "storage_extra_columns",  # holdings: aggregate count
}

# Size bounds in bytes, set from the 0.71.0 preview DB's maxima with margin: no ordinary
# TEXT column exceeds 918; a raw export is larger than any of these.
DEFAULT_BOUND = 2048
BOUNDS = {
    # FTS5 index pages (preview max 4 053), page size 4 096 by default.
    "doc_fts_data.block": 16_384,
    "search_data.block": 16_384,
    "search_description_data.block": 16_384,
    "search_exact_data.block": 16_384,  # exact-match ranking index (#2167)
}
PER_KEY = "meta.value"  # bounded per key, below

# A steward working-file name or a census row locator inside published text.
WORKING_REFERENCE = re.compile(r"[\w./-]+\.(?:jsonl?|toml)\b|census:row=")

META_BOUND = 4096  # preview max 1 755 (`logo`)
META_BOUNDS = {
    "scope": 120_000,  # the steward's public scope document (preview 81 974)
}

# Catalog-schema tables a steward build emits empty: rows would name working files.
EMPTY_TABLES = ("occurrence_locator",)


def schema_of(
    con: sqlite3.Connection,
) -> tuple[dict[str, set[tuple[str, str]]], set[str]]:
    """Tables with their (column, type) sets, and the names of the virtual tables."""
    rows = con.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT GLOB 'sqlite_*'"
    ).fetchall()
    cols = {
        t: {(c[1], c[2].upper()) for c in con.execute(f'PRAGMA table_xinfo("{t}")')}
        for t, _ in rows
    }
    return cols, {t for t, sql in rows if sql.startswith("CREATE VIRTUAL")}


def expected_schema(checkout: Path) -> dict[str, set[tuple[str, str]]]:
    src = checkout / BUILD_SRC
    # holding_reference_occurrence is created from a Rust string, not a .sql file.
    inline = re.search(
        r'"(CREATE TABLE holding_reference_occurrence .*?;)"',
        (src / "holdings/references.rs").read_text(),
        re.DOTALL,
    )
    if inline is None:
        sys.exit("holding_reference_occurrence DDL not found in holdings/references.rs")
    ddl = [
        (src / "schema.sql").read_text(),
        (src / "holdings/relations.sql").read_text(),
        (src / "holdings/schema.sql").read_text(),
        inline.group(1),
    ]
    con = sqlite3.connect(":memory:")
    con.executescript("\n".join(ddl))
    return schema_of(con)[0]


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    holdings, checkout = Path(sys.argv[1]), Path(sys.argv[2])
    con = sqlite3.connect(holdings.resolve().as_uri() + "?mode=ro", uri=True)
    want = expected_schema(checkout)
    have, virtual = schema_of(con)

    extra_t = sorted(set(have) - set(want))
    missing_t = sorted(set(want) - set(have) - OPTIONAL_TABLES)
    extra_c, missing_c, too_big = [], [], []
    for t in sorted(set(have) & set(want)):
        extra_c += [f"{t}.{c} {ty}" for c, ty in sorted(have[t] - want[t])]
        missing_c += [f"{t}.{c} {ty}" for c, ty in sorted(want[t] - have[t])]
    for t, cols in sorted(have.items()):
        if t in virtual:  # their rows live in the shadow tables, checked below
            continue
        for c, ty in sorted(cols):
            if ty not in ("TEXT", "BLOB", "") or f"{t}.{c}" == PER_KEY:
                continue
            bound = BOUNDS.get(f"{t}.{c}", DEFAULT_BOUND)
            n = con.execute(
                f'SELECT max(length(CAST("{c}" AS BLOB))) FROM "{t}"'
            ).fetchone()[0]
            if (n or 0) > bound:
                too_big.append(f"{t}.{c} ({n} > {bound} bytes)")

    meta = (
        con.execute("SELECT key, length(CAST(value AS BLOB)) FROM meta").fetchall()
        if "meta" in have
        else []
    )
    bad_m = sorted(k for k, _ in meta if k not in META_KEYS)
    for k, n in meta:
        bound = META_BOUNDS.get(k, META_BOUND)
        if n > bound:
            too_big.append(f"meta.{k} ({n} > {bound} bytes)")

    filled = [
        t
        for t in EMPTY_TABLES
        if t in have
        and con.execute(f'SELECT EXISTS(SELECT 1 FROM "{t}")').fetchone()[0]
    ]

    # Reported by text id and the reference's file extension, never the body.
    leaked = (
        sorted(
            f"text:{tid} ({m.group(0).rsplit('.', 1)[-1].rstrip('=')})"
            for tid, body in con.execute("SELECT id, body FROM text")
            if (m := WORKING_REFERENCE.search(body))
        )
        if "text" in have
        else []
    )

    items = [
        ("extra table", extra_t),
        ("missing table", missing_t),
        ("extra column", extra_c),
        ("missing column", missing_c),
        ("unknown meta key", bad_m),
        ("oversize value", too_big),
        ("non-empty table", filled),
        ("working-file reference", leaked),
    ]
    failing = sum(len(v) for _, v in items)
    print(
        f"scope check {'FAIL' if failing else 'ok'}: tables={len(have)} "
        f"columns={sum(len(c) for c in have.values())} meta_keys={len(meta)} "
        + " ".join(f"{label.replace(' ', '_')}s={len(v)}" for label, v in items)
    )
    for label, names in items:
        for name in sorted(names):
            print(f"  {label}: {name}")
    return 1 if failing else 0


if __name__ == "__main__":
    sys.exit(main())
